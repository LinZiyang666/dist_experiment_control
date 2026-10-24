//go:build linux

// Package hotupdate_test supplies real artifacts and external business oracles.
// These fixtures never reconnect business sockets or replace product recovery.
// origin: hot-update S5 plan §8 / §12 P19
package hotupdate_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/test/hotupdate/testsupervisor"
	"golang.org/x/sys/unix"
)

type hotUpdateArtifact struct {
	Path, SHA256, SourceCommit, SourceDigest string
	BuildArgs                                []string
	Race                                     bool
	Tarball                                  []byte
	TarSHA256                                string
}

// A build/helper may exit before a descendant. Own the whole private group
// on success as well as cancellation; Wait alone only reaps the direct child.
// origin: hot-update S5 internal review HOTUPDATE process cleanup
func hotUpdateOwnProcessGroup(cmd *exec.Cmd) func() {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	if cmd.Cancel != nil {
		cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	}
	cmd.WaitDelay = time.Second
	return func() {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}
}

func hotUpdateRaceEnabled() bool {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		panic("hotupdate: missing harness build information")
	}
	for _, setting := range info.Settings {
		if setting.Key == "-race" {
			return setting.Value == "true"
		}
	}
	return false
}

func debugBuildArtifact(path string) (bool, error) {
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return false, err
	}
	for _, setting := range info.Settings {
		if setting.Key == "-race" {
			return setting.Value == "true", nil
		}
	}
	return false, nil
}

// Build the actual CLI, forwarding GOFLAGS so a nested production mutation is
// present in the executable under observation, as well as in the harness.
// origin: hot-update S5 plan §8 / testing-standards §0.4–§0.6
func buildHotUpdateArtifact(t *testing.T, release string) hotUpdateArtifact {
	t.Helper()
	if release == "" || strings.ContainsAny(release, " \t\r\n") {
		t.Fatal("invalid fixture release label")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	a := hotUpdateArtifact{Path: filepath.Join(t.TempDir(), "tether"), Race: hotUpdateRaceEnabled()}
	a.BuildArgs = []string{"build"}
	if a.Race {
		a.BuildArgs = append(a.BuildArgs, "-race")
	}
	a.BuildArgs = append(a.BuildArgs, "-o", a.Path, "-ldflags",
		"-X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion="+release, "./cmd/tether")
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", a.BuildArgs...)
	cmd.Dir = root
	cleanup := hotUpdateOwnProcessGroup(cmd)
	defer cleanup()
	if out, err := testsupervisor.CombinedOutput(cmd); err != nil {
		t.Fatalf("build real artifact: %v\n%s", err, out)
	}
	a.SHA256 = hotUpdateFileSHA(t, a.Path)
	commit := exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	commit.Dir = root
	body, err := testsupervisor.Output(commit)
	if err != nil {
		t.Fatal(err)
	}
	a.SourceCommit = strings.TrimSpace(string(body))
	a.SourceDigest = hotUpdateSourceDigest(t, root)
	bin, err := os.ReadFile(a.Path)
	if err != nil {
		t.Fatal(err)
	}
	var packed bytes.Buffer
	gz, err := gzip.NewWriterLevel(&packed, gzip.NoCompression)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "tether", Mode: 0o755, Size: int64(len(bin))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(bin); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	a.Tarball = packed.Bytes()
	a.TarSHA256 = fmt.Sprintf("%x", sha256.Sum256(a.Tarball))
	return a
}

func hotUpdateFileSHA(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// A commit alone would label a dirty or untracked production source as clean.
// Hash names and content with explicit lengths, including the dependency pins.
func hotUpdateSourceDigest(t *testing.T, root string) string {
	t.Helper()
	paths := []string{"go.mod", "go.sum"}
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() {
				rel, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				paths = append(paths, rel)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, path := range paths {
		body, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = fmt.Fprintf(h, "%d:%s:%d:", len(path), path, len(body))
		_, _ = h.Write(body)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Strip both directions of inherited dispatch. Each real role receives its own
// private home; a business child must also use this environment before exec.
func hotUpdateEnvironment(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		if name == "HOME" || strings.HasPrefix(name, "TETHER_") || strings.HasPrefix(name, "_TETHER_") {
			continue
		}
		out = append(out, item)
	}
	return append(out, "HOME="+t.TempDir(), "TETHER_HOME="+t.TempDir())
}

type hotUpdateOutput struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (o *hotUpdateOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.b.Write(p)
}

func (o *hotUpdateOutput) Bytes() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]byte(nil), o.b.Bytes()...)
}

type hotUpdateProcess struct {
	Cmd    *exec.Cmd
	Output *hotUpdateOutput
	Done   <-chan struct{}
	err    error // read only after Done closes
}

// The executable must be a private artifact. Cleanup joins the original PID,
// including on failed readiness or a blocked output pipe. The private reaper
// owns fatal-exit cleanup without replacing the product PID or its recovery.
// origin: hot-update S8 plan §9.1 / P21 / L21
func startHotUpdateProcess(t *testing.T, path string, env []string, args ...string) *hotUpdateProcess {
	t.Helper()
	if !filepath.IsAbs(path) || strings.HasPrefix(path, "/home/weiland/.local/bin/") {
		t.Fatal("real process requires an absolute private artifact path")
	}
	p := &hotUpdateProcess{Cmd: exec.CommandContext(t.Context(), path, args...), Output: &hotUpdateOutput{}}
	p.Cmd.Dir = t.TempDir()
	p.Cmd.Env = env
	p.Cmd.Stdout, p.Cmd.Stderr = p.Output, p.Output
	p.Cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	waitProcess, err := testsupervisor.StartProcess(p.Cmd)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	p.Done = done
	go func() { p.err = waitProcess(); close(done) }()
	t.Cleanup(func() {
		select {
		case <-p.Done:
			// A leader can exit before its descendants. Kill the owned group too.
			_ = syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGKILL)
			return
		default:
		}
		_ = syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGTERM)
		select {
		case <-p.Done:
		case <-time.After(3 * time.Second):
			_ = syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGKILL)
			select {
			case <-p.Done:
			case <-time.After(3 * time.Second):
				t.Error("real process group did not join")
			}
		}
		_ = syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGKILL)
	})
	return p
}

type hotUpdateSocketIdentity struct {
	Dev, Ino      uint64
	Type          uint32
	Local, Remote string // private fixture evidence; never log or serialize
}

// RawConn.Control observes the original FD without File()/File.Fd(), whose
// wrapper can change nonblocking state or invent another descriptor owner.
// origin: hot-update S5 plan §3.2 / §5.1 / T08
func hotUpdateTCPIdentity(c *net.TCPConn) (hotUpdateSocketIdentity, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return hotUpdateSocketIdentity{}, err
	}
	var stat unix.Stat_t
	var statErr error
	if err := raw.Control(func(fd uintptr) { statErr = unix.Fstat(int(fd), &stat) }); err != nil {
		return hotUpdateSocketIdentity{}, err
	}
	if statErr != nil {
		return hotUpdateSocketIdentity{}, statErr
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFSOCK {
		return hotUpdateSocketIdentity{}, fmt.Errorf("business descriptor is not a socket")
	}
	return hotUpdateSocketIdentity{Dev: uint64(stat.Dev), Ino: stat.Ino, Type: stat.Mode & unix.S_IFMT,
		Local: c.LocalAddr().String(), Remote: c.RemoteAddr().String()}, nil
}

func hotUpdateTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	if err := ln.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	client, err := net.DialTCP("tcp4", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	server, err := ln.AcceptTCP()
	if err != nil {
		_ = client.Close()
		t.Fatal(err)
	}
	for _, conn := range []*net.TCPConn{client, server} {
		t.Cleanup(func() { _ = conn.Close() })
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	return client, server
}

// A stopped peer and small kernel send buffer cause a genuine n+timeout,
// never a short-write wrapper. The caller retains both sockets for recovery.
func hotUpdateBlockedWrite(t *testing.T, c *net.TCPConn, body []byte) (int, error) {
	t.Helper()
	if err := c.SetWriteBuffer(4096); err != nil {
		t.Fatal(err)
	}
	if err := c.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	n, err := c.Write(body)
	if n <= 0 || n >= len(body) || err == nil {
		t.Fatal("blocked peer did not produce a genuine partial write")
	}
	if err := c.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return n, err
}

// Product interop stays outside ss_reference_test.go: the reference primitives
// must never import or invoke ssproxy's reader/writer to produce expectations.
func exerciseSSProductInterop(t *testing.T) {
	t.Helper()
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	if err := ln.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	password := "reference-interop-fixture"
	s := ssproxy.New(nil)
	port, err := s.Start(0, []ssproxy.Key{{KeyID: "reference", Secret: password}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	client, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	request := bytes.Repeat([]byte("independent client request\x00"), 32)
	response := bytes.Repeat([]byte("independent target response\xff"), 40)
	targetPort := ln.Addr().(*net.TCPAddr).Port
	header := []byte{1, 127, 0, 0, 1, byte(targetPort >> 8), byte(targetPort)}
	salt := [32]byte{1, 3, 5, 7}
	wire, err := ssReferenceEncode(password, salt, [][]byte{append(header, request...)}, &ssSealLedger{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(client, bytes.NewReader(wire)); err != nil {
		t.Fatal(err)
	}
	// Accept once and keep that original target alive while verifying both
	// directions; the external target never reconnects or supplies a fallback.
	target, err := ln.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = target.Close() }()
	if err := target.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(request))
	if _, err := io.ReadFull(target, got); err != nil || !bytes.Equal(got, request) {
		t.Fatal("product did not deliver independent request to original target")
	}
	if _, err := io.Copy(target, bytes.NewReader(response)); err != nil {
		t.Fatal(err)
	}
	reader := &ssReferenceReader{Reader: client, Password: password}
	var received []byte
	for len(received) < len(response) {
		chunk, err := reader.ReadChunk()
		if err != nil {
			t.Fatal("independent reference rejected product ciphertext")
		}
		received = append(received, chunk...)
	}
	if !bytes.Equal(received, response) {
		t.Fatal("product ciphertext plaintext differs from target ledger")
	}
	if err := ln.SetDeadline(time.Now()); err != nil {
		t.Fatal(err)
	}
	extra, err := ln.AcceptTCP()
	if err == nil {
		_ = extra.Close()
		t.Fatal("SS product dialed target twice")
	}
	if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatal("target Accept census failed")
	}
}
