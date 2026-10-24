//go:build linux

package infrahost

// Real private filesystem trees, X.509 renewals, supervised writer processes,
// pidfds and flock OFDs run here. Policy/domain authorization is P04's boundary.
// Stock Caddy, real ACME protocol and systemd restart are P29/P31/86.I evidence.

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

const acmeWriterEnv = "_TETHER_TEST_ACME_WRITER"
const acmeRenewalEnv = "_TETHER_TEST_ACME_RENEWAL"
const acmeCanary = "private-account-key-ACME-canary"

type acmeFixture struct {
	storage       *ACMEStorage
	config        ACMEConfig
	root, initial string
	request       infracontract.HostRequest
	writer        *acmeTestWriter
	fenceRevision uint64
}

type acmeTestWriter struct {
	process infracontract.Process
	pidfd   *os.File
	input   io.WriteCloser
	output  *bufio.Reader
	close   func()
}

func acmeDeadline(t *testing.T, duration time.Duration) infracontract.Deadline {
	t.Helper()
	d, err := controlbudget.After(duration)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func acmeRequestFixture(process infracontract.Process) infracontract.HostRequest {
	return infracontract.HostRequest{Version: 1, Op: infracontract.FinalizeStorage, Component: infracontract.Caddy,
		Tx: infracontract.ID{1}, Domain: infracontract.ID{2}, Step: 1, Sequence: 1, PolicyRevision: 1,
		KeyID: "test-policy", HostBootID: process.BootID, Nonce: infracontract.ID{3}, Expected: process,
		ExpectedJournal: infracontract.Digest{4}, BeforeCensus: infracontract.Digest{5}, Payload: infracontract.Digest{6}, RemainingNS: uint64(10 * time.Second)}
}

func newACMEFixture(t *testing.T) *acmeFixture {
	t.Helper()
	if runtime.GOARCH != "amd64" {
		t.Skip("ACME hot migration requires linux/amd64")
	}
	f := &acmeFixture{root: t.TempDir(), initial: t.TempDir()}
	for _, dir := range []string{f.root, f.initial} {
		if err := os.Chmod(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	writeACMERevision(t, f.initial, "A")
	if err := os.Mkdir(filepath.Join(f.initial, "locks"), 0700); err != nil {
		t.Fatal(err)
	}
	acmeWrite(t, filepath.Join(f.initial, "locks", "renew.lock"), []byte("old-writer-lock"))
	renewal := t.TempDir()
	writeACMERevision(t, renewal, "B")
	f.writer = startACMEWriter(t, f.initial, renewal, nil)
	root, err := os.Open(f.root)
	if err != nil {
		t.Fatal(err)
	}
	// NewACMEStorage duplicates both policy descriptors, so the loader's copies
	// are released here; their read-only close has nothing to report.
	defer func() { _ = root.Close() }()
	initial, err := os.Open(f.initial)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = initial.Close() }()
	f.config = ACMEConfig{Root: root, Initial: initial, UID: os.Geteuid(), GID: os.Getegid(), Exit: f.writer.exitCensus}
	f.storage, err = NewACMEStorage(f.config)
	if err != nil {
		t.Fatal(err)
	}
	// Reopen with the owned descriptors when exercising a replacement worker.
	f.config.Root, f.config.Initial = f.storage.root, f.storage.initial
	t.Cleanup(func() { _ = f.storage.Close() })
	f.request = acmeRequestFixture(f.writer.process)
	f.fenceRevision = fenceACMEFixture(t, f.storage, f.request)
	return f
}

func fenceACMEFixture(t *testing.T, storage *ACMEStorage, request infracontract.HostRequest) uint64 {
	t.Helper()
	request.Op = infracontract.FenceStart
	revision, err := FenceACME(storage, request, acmeDeadline(t, 5*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func startACMEWriter(t *testing.T, current, renewal string, lease *os.File) *acmeTestWriter {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(path, "-test.run=^TestACMEFinalDeltaFollowsOldExit$", "-test.count=1")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, acmeWriterEnv+"=") && !strings.HasPrefix(entry, acmeRenewalEnv+"=") && !strings.HasPrefix(entry, "_TETHER_HANDOFF=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, acmeWriterEnv+"="+current, acmeRenewalEnv+"="+renewal, "GORACE=atexit_sleep_ms=0")
	if lease != nil {
		cmd.ExtraFiles = []*os.File{lease}
	}
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		_ = input.Close()
		t.Fatal(err)
	}
	wait, err := testharness.StartProcess(cmd)
	if err != nil {
		_ = input.Close()
		_ = output.Close()
		t.Fatal(err)
	}
	var once sync.Once
	w := &acmeTestWriter{input: input, output: bufio.NewReader(output)}
	w.close = func() {
		once.Do(func() {
			_ = input.Close()
			if err := wait(); err != nil {
				t.Errorf("ACME writer did not exit cleanly: %v", err)
			}
			_ = output.Close()
		})
	}
	t.Cleanup(w.close)
	if line, err := w.output.ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatal("supervised ACME writer did not become ready")
	}
	fd, err := unix.PidfdOpen(cmd.Process.Pid, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.pidfd = os.NewFile(uintptr(fd), "test-acme-writer-pidfd")
	t.Cleanup(func() { _ = w.pidfd.Close() })
	statBody, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", cmd.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(statBody)[strings.LastIndexByte(string(statBody), ')')+1:])
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	exe := fmt.Sprintf("/proc/%d/exe", cmd.Process.Pid)
	var stat unix.Stat_t
	if err := unix.Stat(exe, &stat); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	w.process = infracontract.Process{BootID: strings.TrimSpace(string(boot)), PID: int32(cmd.Process.Pid), StartTicks: ticks,
		InvocationID: "private-test-writer", ExeDevice: stat.Dev, ExeInode: stat.Ino, ExeSHA256: sha256.Sum256(body), Cgroup: "/private-test-writer"}
	return w
}

func (w *acmeTestWriter) exitCensus(expected infracontract.Process, d infracontract.Deadline, stop infracontract.Stop) (infracontract.Census, error) {
	if err := acmeCheck(d, stop); err != nil || expected != w.process {
		return infracontract.Census{}, infracontract.ErrIncomplete
	}
	poll := []unix.PollFd{{Fd: int32(w.pidfd.Fd()), Events: unix.POLLIN}}
	if _, err := unix.Poll(poll, 0); err != nil {
		return infracontract.Census{}, err
	}
	c := infracontract.Census{Complete: true, Generation: 1, Digest: sha256.Sum256([]byte(fmt.Sprintf("pidfd:%d:%d:%d", expected.PID, expected.StartTicks, poll[0].Revents)))}
	if poll[0].Revents&unix.POLLIN == 0 {
		c.Processes, c.OldOwnerPending = []infracontract.Process{expected}, 1
	}
	return c, nil
}

func acmeWriterChild(t *testing.T, path string) {
	if _, err := io.WriteString(os.Stdout, "ready\n"); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if scanner.Text() != "renew" {
			t.Fatal("unexpected writer command")
		}
		stage := os.Getenv(acmeRenewalEnv)
		for _, name := range []string{"account.key", "certificate.pem", "private.key", "metadata.json"} {
			body, err := os.ReadFile(filepath.Join(stage, name))
			if err != nil {
				t.Fatal("missing renewal input")
			}
			acmeWrite(t, filepath.Join(path, name), body)
		}
		if err := os.Remove(filepath.Join(path, "obsolete")); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal("cannot retire old metadata")
		}
		if _, err := io.WriteString(os.Stdout, "renewed\n"); err != nil {
			t.Fatal(err)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

func (w *acmeTestWriter) renew(t *testing.T) {
	t.Helper()
	if _, err := io.WriteString(w.input, "renew\n"); err != nil {
		t.Fatal(err)
	}
	if line, err := w.output.ReadString('\n'); err != nil || line != "renewed\n" {
		t.Fatal("current writer did not commit renewal")
	}
}

func writeACMERevision(t *testing.T, dir, revision string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Private test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(int64(revision[0])), DNSNames: []string{"localhost"},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca, &serverKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil || parsed.CheckSignatureFrom(ca) != nil {
		t.Fatal("test CA did not issue a valid renewal")
	}
	keyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		t.Fatal(err)
	}
	acmeWrite(t, filepath.Join(dir, "certificate.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	acmeWrite(t, filepath.Join(dir, "private.key"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	acmeWrite(t, filepath.Join(dir, "account.key"), []byte(acmeCanary+revision))
	acmeWrite(t, filepath.Join(dir, "metadata.json"), []byte(`{"revision":"`+revision+`"}`))
	if revision == "A" {
		acmeWrite(t, filepath.Join(dir, "obsolete"), []byte("removed-after-warm"))
	}
	if err := os.MkdirAll(filepath.Join(dir, "empty", "nested"), 0700); err != nil {
		t.Fatal(err)
	}
}

func acmeWrite(t *testing.T, path string, body []byte) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal("cannot open private test record")
	}
	if _, err := file.Write(body); err != nil || file.Sync() != nil {
		_ = file.Close()
		t.Fatal("cannot persist private test record")
	}
	if err := file.Close(); err != nil {
		t.Fatal("cannot close private test record")
	}
}

func acmeStateFixture(t *testing.T, s *ACMEStorage) acmeState {
	t.Helper()
	state, err := s.readState(false)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func assertACMERevision(t *testing.T, dir *os.File, revision string, warm bool) {
	t.Helper()
	mode := os.FileMode(0600)
	dirMode := os.FileMode(0700)
	if warm {
		mode, dirMode = 0400, 0500
	}
	rootStat, err := dir.Stat()
	if err != nil || rootStat.Mode().Perm() != dirMode {
		t.Fatal("storage directory permissions changed")
	}
	for _, name := range []string{"account.key", "certificate.pem", "private.key", "metadata.json"} {
		file, err := acmeOpen(dir, name, unix.O_RDONLY, 0)
		if err != nil {
			t.Fatal("current tree lost renewal material")
		}
		body, err := io.ReadAll(file)
		var stat unix.Stat_t
		statErr := unix.Fstat(int(file.Fd()), &stat)
		_ = file.Close()
		if err != nil || statErr != nil || stat.Mode&0777 != uint32(mode) || stat.Uid != uint32(os.Geteuid()) || stat.Gid != uint32(os.Getegid()) {
			t.Fatal("renewal record owner/mode changed")
		}
		if name == "metadata.json" && string(body) != `{"revision":"`+revision+`"}` {
			t.Fatal("selected stale renewal metadata")
		}
		if name == "account.key" && !bytes.Equal(body, []byte(acmeCanary+revision)) {
			t.Fatal("selected stale account key")
		}
		if name == "certificate.pem" {
			block, _ := pem.Decode(body)
			if block == nil {
				t.Fatal("selected invalid certificate")
			}
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil || cert.SerialNumber.Int64() != int64(revision[0]) {
				t.Fatal("selected stale certificate")
			}
		}
	}
	if nested, err := acmeOpenDirectory(dir, "empty/nested"); err != nil {
		t.Fatal("delta omitted empty directories")
	} else {
		_ = nested.Close()
	}
	var stat unix.Stat_t
	if err := unix.Fstatat(int(dir.Fd()), "locks", &stat, unix.AT_SYMLINK_NOFOLLOW); !errors.Is(err, unix.ENOENT) {
		t.Fatal("copied a writer's advisory lock")
	}
	if revision != "A" {
		if err := unix.Fstatat(int(dir.Fd()), "obsolete", &stat, unix.AT_SYMLINK_NOFOLLOW); !errors.Is(err, unix.ENOENT) {
			t.Fatal("delta kept deleted metadata")
		}
	}
	certFile, err := acmeOpen(dir, "certificate.pem", unix.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	certBytes, certErr := io.ReadAll(certFile)
	_ = certFile.Close()
	keyFile, err := acmeOpen(dir, "private.key", unix.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, keyErr := io.ReadAll(keyFile)
	_ = keyFile.Close()
	if _, err := tls.X509KeyPair(certBytes, keyBytes); certErr != nil || keyErr != nil || err != nil {
		t.Fatal("selected certificate and key are different renewals")
	}
}

func assertACMENoSecret(t *testing.T, values ...any) {
	t.Helper()
	for _, value := range values {
		text := fmt.Sprintf("%+v", value)
		for _, secret := range []string{acmeCanary, hex.EncodeToString([]byte(acmeCanary)), base64.StdEncoding.EncodeToString([]byte(acmeCanary))} {
			if strings.Contains(text, secret) {
				t.Fatal("ACME secret escaped a private filesystem reference")
			}
		}
	}
}

func acmeStateFixtureBeforeOwnerChange(t *testing.T, s *ACMEStorage) acmeState {
	t.Helper()
	uid := s.config.UID
	s.config.UID = os.Geteuid()
	state := acmeStateFixture(t, s)
	s.config.UID = uid
	return state
}
