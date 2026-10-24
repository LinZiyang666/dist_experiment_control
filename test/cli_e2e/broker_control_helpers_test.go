//go:build linux

// broker_control_helpers_test.go keeps independent command, attempt and target
// witnesses. Store/process cut fixtures remain owned by clusterharness.
// origin: hot-update S6 plan §8.2–§8.3 / §12.1 P27
package cli_e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/testharness"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	natstest "github.com/nats-io/nats-server/v2/test"
)

type brokerControlResult struct {
	ctlResult
	startedMonoNS int64
	attempts      []controlbudget.Attempt
	err           error
}

// Each invocation owns a separate inherited receipt FD. Reading stdout or
// stderr cannot compensate for a missing production attempt record.
func runBrokerControl(ctx context.Context, s *realStack, home, url, receiptPath string, extraEnv []string, args ...string) brokerControlResult {
	ctx, cancel := context.WithTimeout(ctx, 2*upgradebudget.RequestLatency)
	defer cancel()
	file, err := os.OpenFile(receiptPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return brokerControlResult{err: err}
	}
	defer func() { _ = file.Close() }()
	full := append([]string{args[0], "--home", home, "--nats-url", url}, args[1:]...)
	cmd := exec.CommandContext(ctx, s.bin, full...)
	cmd.Env = append(ctlEnv(), "HTTP_PROXY=", "HTTPS_PROXY=", "ALL_PROXY=", "NO_PROXY=", "http_proxy=", "https_proxy=", "all_proxy=", "no_proxy=", cli.AttemptReceiptFDEnv+"=3")
	cmd.Env = append(cmd.Env, extraEnv...)
	cmd.ExtraFiles = []*os.File{file}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	startedMonoNS, err := controlbudget.Now()
	if err != nil {
		return brokerControlResult{err: err}
	}
	start := time.Now()
	err = testharness.RunProcess(cmd)
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	result := brokerControlResult{startedMonoNS: startedMonoNS, ctlResult: ctlResult{stdout: out.String(), stderr: stderr.String(), elapsed: time.Since(start)}}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		result.rc = exit.ExitCode()
	} else if err != nil {
		result.err = err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		result.err = errors.Join(result.err, err)
		return result
	}
	result.attempts, err = decodeBrokerControlAttempts(io.LimitReader(file, cli.AttemptByteLimit+1))
	result.err = errors.Join(result.err, err)
	return result
}

// The production close envelope is distinct from the frozen Attempt shape.
// EOF joins the CLI process; its final census must account for every ordinal.
// origin: hot-update S6 internal review rv-ctl-2 / T35
func decodeBrokerControlAttempts(reader io.Reader) ([]controlbudget.Attempt, error) {
	decoder := json.NewDecoder(reader)
	var rows []controlbudget.Attempt
	var closure *struct {
		Kind         string `json:"kind"`
		LogicalID    string `json:"logical_id"`
		AttemptCount int    `json:"attempt_count"`
		Overflow     bool   `json:"overflow"`
	}
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return rows, err
		}
		if closure != nil {
			return rows, errors.New("record after observer close")
		}
		var kind struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &kind); err != nil {
			return rows, err
		}
		typed := json.NewDecoder(bytes.NewReader(raw))
		typed.DisallowUnknownFields()
		if kind.Kind != "" {
			if kind.Kind != "observer_close" {
				return rows, errors.New("unknown attempt envelope")
			}
			if err := typed.Decode(&closure); err != nil {
				return rows, err
			}
			continue
		}
		var attempt controlbudget.Attempt
		if err := typed.Decode(&attempt); err != nil {
			return rows, err
		}
		rows = append(rows, attempt)
	}
	if closure == nil {
		return rows, errors.New("missing observer close receipt")
	}
	if len(rows) == 0 || closure.LogicalID != rows[0].LogicalID || closure.AttemptCount != len(rows) || closure.Overflow {
		return rows, errors.New("observer close does not account for all attempts")
	}
	return rows, nil
}

// Ordinals are allocated before work; completion order is allowed to differ.
// Missing failures, including CONNECT failures before eventual success, leave
// a gap. A final result and real transport work must both be present.
func validateBrokerAttempts(rows []controlbudget.Attempt) error {
	if len(rows) == 0 || len(rows) > cli.AttemptLimit {
		return errors.New("missing or overflowing attempts")
	}
	rows = append([]controlbudget.Attempt(nil), rows...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Ordinal < rows[j].Ordinal })
	id, first, last, connects, results := rows[0].LogicalID, rows[0].StartedMonoNS, int64(0), 0, 0
	for i, row := range rows {
		if id == "" || row.LogicalID != id || row.Ordinal != uint32(i+1) || row.StartedMonoNS <= 0 || row.FinishedMonoNS < row.StartedMonoNS || row.Class == "" {
			return errors.New("attempt identity, ordinal or interval changed")
		}
		if row.StartedMonoNS < first {
			first = row.StartedMonoNS
		}
		if row.FinishedMonoNS > last {
			last = row.FinishedMonoNS
		}
		if row.Phase == controlbudget.PhaseConnect {
			connects++
		}
		if row.Phase == controlbudget.PhaseResult {
			results++
		}
	}
	var problems []error
	if connects == 0 {
		problems = append(problems, errors.New("missing CONNECT"))
	}
	if results != 1 || rows[len(rows)-1].Phase != controlbudget.PhaseResult {
		problems = append(problems, fmt.Errorf("terminal result count is %d, want one final result", results))
	}
	if time.Duration(last-first) > upgradebudget.RequestLatency {
		problems = append(problems, errors.New("RunE exceeded its absolute admission budget"))
	}
	return errors.Join(problems...)
}

func brokerAttemptCount(rows []controlbudget.Attempt, phase controlbudget.AttemptPhase, class string) int {
	count := 0
	for _, row := range rows {
		if row.Phase == phase && (class == "" || row.Class == class) {
			count++
		}
	}
	return count
}

type brokerTargetWitness struct {
	LogicalID  string `json:"logical_id"`
	PID        int    `json:"pid"`
	StartTicks uint64 `json:"start_ticks"`
	BootID     string `json:"boot_id"`
	AtNS       int64  `json:"at_ns"`
}

// The actual target reads /proc itself and appends once with one O_APPEND
// write. Neither reliable acceptance nor the CLI's stdout can create this row.
const brokerTargetProgram = `import json,os,pathlib,sys,time
pid=os.getpid()
ticks=int(pathlib.Path("/proc/self/stat").read_text().rsplit(")",1)[1].split()[19])
row=dict(logical_id=sys.argv[1],pid=pid,start_ticks=ticks,boot_id=pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip(),at_ns=time.clock_gettime_ns(time.CLOCK_BOOTTIME))
if len(sys.argv)>3 and sys.argv[3]=="write":
 stable=os.open(sys.argv[2]+".stable-writes",os.O_CREAT|os.O_WRONLY|os.O_APPEND,0o600)
 os.write(stable,(sys.argv[1]+"\n").encode());os.fsync(stable);os.close(stable)
fd=os.open(sys.argv[2],os.O_CREAT|os.O_WRONLY|os.O_APPEND,0o600)
os.write(fd,(json.dumps(row)+"\n").encode()); os.close(fd)
print(json.dumps(dict(row,kind="broker-probe-started-v1")),flush=True)
`

func validateBrokerTarget(stdout string, effects []byte, id string) error {
	var received struct {
		brokerTargetWitness
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal([]byte(stdout), &received); err != nil {
		return err
	}
	if received.Kind != "broker-probe-started-v1" || received.LogicalID != id || received.PID <= 0 || received.StartTicks == 0 || received.BootID == "" || received.AtNS <= 0 {
		return errors.New("accepted is not a true process start")
	}
	decoder := json.NewDecoder(bytes.NewReader(effects))
	var target brokerTargetWitness
	if err := decoder.Decode(&target); err != nil {
		return err
	}
	if target != received.brokerTargetWitness {
		return errors.New("stdout and independent target identity differ")
	}
	var extra brokerTargetWitness
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("duplicate or malformed target effect")
	}
	return nil
}

// HTTP CONNECT and server INFO delays consume successive parts of the same
// admission. All forwarded NATS bytes come from the real embedded server.
// Every accepted connection, including cancellation during a delay, is joined.
func brokerBudgetProxy(t *testing.T, upstream string, proxyDelay, infoDelay time.Duration) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	var workers sync.WaitGroup
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	track := func(c net.Conn) { mu.Lock(); connections[c] = struct{}{}; mu.Unlock() }
	untrack := func(c net.Conn) { _ = c.Close(); mu.Lock(); delete(connections, c); mu.Unlock() }
	delay := func(d time.Duration) bool {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			track(client)
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer untrack(client)
				_ = client.SetDeadline(time.Now().Add(2 * upgradebudget.RequestLatency))
				reader := bufio.NewReader(client)
				request, err := http.ReadRequest(reader)
				if err != nil {
					return
				}
				_ = request.Body.Close()
				if request.Method != http.MethodConnect || !delay(proxyDelay) {
					return
				}
				server, err := (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(upstream, "nats://"))
				if err != nil {
					return
				}
				track(server)
				defer untrack(server)
				if _, err := io.WriteString(client, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
					return
				}
				copied := make(chan struct{})
				go func() { _, _ = io.Copy(server, reader); _ = server.Close(); close(copied) }()
				if delay(infoDelay) {
					_, _ = io.Copy(client, server)
				}
				_ = client.Close()
				_ = server.Close()
				<-copied
			}()
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		<-done
		mu.Lock()
		for c := range connections {
			_ = c.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return "http://" + strings.Replace(listener.Addr().String(), "127.0.0.1", "localhost", 1)
}

func requireBrokerControl(t *testing.T, result brokerControlResult) {
	t.Helper()
	if result.err != nil {
		t.Fatal(result.err)
	}
	if err := validateBrokerAttempts(result.attempts); err != nil {
		t.Errorf("%v: rc=%d elapsed=%s stderr=%q attempts=%+v", err, result.rc, result.elapsed, result.stderr, result.attempts)
	}
}

func brokerReadResult(stdout string, nid string) error {
	var response struct {
		Schema string `json:"schema"`
		Nodes  []struct {
			NID    string `json:"nid"`
			Status string `json:"status"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(stdout), &response); err != nil {
		return err
	}
	if response.Schema != "node_ls" {
		return fmt.Errorf("unvalidated node list schema")
	}
	for _, node := range response.Nodes {
		if node.NID == nid && node.Status == "ONLINE" {
			return nil
		}
	}
	return errors.New("expected online node is missing")
}

func brokerReceiptPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "attempts.jsonl")
}

// A real TLS/WSS server with a private trusted CA exercises the CLI's deep
// transport. There is deliberately no business responder: a successful
// handshake must still share its remaining budget with a failing request.
func exerciseBrokerSecureAdmission(t *testing.T, binary string, websocket bool) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "broker-budget"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	root := t.TempDir()
	caPath := filepath.Join(root, "ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	options := natstest.DefaultTestOptions
	options.Host, options.Port, options.AlwaysEnableNonce = "127.0.0.1", -1, true
	config := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if websocket {
		options.Websocket.Host, options.Websocket.Port, options.Websocket.TLSConfig = "127.0.0.1", -1, config
	} else {
		options.TLSConfig = config
	}
	server := natstest.RunServer(&options)
	defer func() { server.Shutdown(); server.WaitForShutdown() }()
	url := server.ClientURL()
	if websocket {
		url = server.WebsocketURL()
	}
	home := filepath.Join(root, "home")
	if _, err := cli.EnsureIdentity(home); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "current_session"), []byte("tls-lab\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := &realStack{bin: binary}
	result := runBrokerControl(t.Context(), s, home, url, filepath.Join(root, "attempts.jsonl"), []string{"SSL_CERT_FILE=" + caPath}, "node", "ls", "--json")
	requireBrokerControl(t, result)
	if result.rc == 0 {
		t.Fatal("TLS handshake substituted for a business result")
	}
	for _, phase := range []controlbudget.AttemptPhase{controlbudget.PhaseTLS, controlbudget.PhaseInboxProbe, controlbudget.PhaseRequest} {
		if brokerAttemptCount(result.attempts, phase, "") == 0 {
			t.Fatalf("missing actual secure %s phase", phase)
		}
	}
	if websocket && brokerAttemptCount(result.attempts, controlbudget.PhaseWS, "") == 0 {
		t.Fatal("WSS upgrade was missing from production attempts")
	}
}
