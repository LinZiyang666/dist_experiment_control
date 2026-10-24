//go:build linux

package clusterharness

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"encoding/base64"
	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/agentstate"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/crypto/ssh"
)

// This is another evidence consumer of the unique product/store fixture, not a
// second cut implementation. The agent is a private real binary. SSH uses the
// wire protocol directly, so no installed SSH/tether or production state is used.
// origin: hot-update S6 plan §6.3 / §10.1 / T48
func StartBrokerCohortFixture(t *testing.T, cfg BrokerImageFixtureConfig) (*BrokerImageFixture, error) {
	return startBrokerProductFixture(t, cfg, false, false, true)
}

// Control admission has its own two-agent supply. Sharing T48's eleven raw
// flows would consume replay needed by the post-blackout control burst, turning
// the normal profile into an unintended capacity-saturation test.
// origin: hot-update S6 plan §10.1 / T50 / EXITS-2-3
func StartBrokerControlFixture(t *testing.T, cfg BrokerImageFixtureConfig) (*BrokerImageFixture, error) {
	return startBrokerProductFixture(t, cfg, true, false, true)
}

type cohortSample struct {
	ID         string `json:"id"`
	Direction  uint8  `json:"direction"`
	Sequence   uint64 `json:"sequence"`
	PayloadSHA string `json:"payload_sha256"`
	Started    int64  `json:"started_boot_ns"`
	Finished   int64  `json:"finished_boot_ns"`
	EOF        bool   `json:"eof"`
}
type cohortStream struct {
	ID             string `json:"id"`
	Protocol       string `json:"protocol"`
	PublicPID      int    `json:"public_pid"`
	PublicFD       int    `json:"public_fd"`
	TargetPID      int    `json:"target_pid"`
	TargetFD       int    `json:"target_fd"`
	AppPID         int    `json:"app_pid"`
	AppTicks       uint64 `json:"app_startticks"`
	AppSHA         string `json:"app_running_sha256"`
	TargetAccepts  uint32 `json:"target_accepts"`
	SSHeaders      uint32 `json:"ss_headers"`
	SSAuthFailures uint32 `json:"ss_auth_failures"`
	HalfClosed     bool   `json:"half_closed"`
	HalfSequence   uint64 `json:"half_close_sequence"`
	FinalSequence  uint64 `json:"final_sequence"`
}
type cohortConnect struct {
	Slot     uint64 `json:"slot"`
	Started  int64  `json:"started_boot_ns"`
	Finished int64  `json:"finished_boot_ns"`
	RC       int    `json:"native_rc"`
}
type cohortEndpoint struct {
	evidence   cohortStream
	password   string
	listener   net.Listener
	target     net.Conn
	peer       net.Conn
	ss         *productSSPeer
	sshClient  *ssh.Client
	sshSession *ssh.Session
	stdin      io.WriteCloser
	stdout     io.Reader
	child      *exec.Cmd
	childDone  chan error
	ready      chan error
	reads      chan error
	writes     chan error
	allocation port.Allocation
}
type productCohort struct {
	f                           *brokerProductFixture
	observerOnly                bool
	opening                     *cohortOpening
	endpoints                   []*cohortEndpoint
	agent                       *exec.Cmd
	agentDone                   chan error
	controlAgent                *exec.Cmd
	controlDone                 chan error
	controlLogs                 productLogs
	agentLogs                   productLogs
	agentHome, ctlHome          string
	ledger                      *os.File
	ledgerMu                    sync.Mutex
	traffic, post, fin, cleanup chan struct{}
	postOnce, cleanupOnce       sync.Once
	workers                     sync.WaitGroup
	before, finished            bool
	streams                     []cohortStream
	listeners                   []listenerimage.Descriptor
	brokerSockets               map[string]cohortSocketIdentity
	cut, ready                  int64
	connects                    []cohortConnect
}

func newProductCohort(f *brokerProductFixture) *productCohort {
	return &productCohort{f: f, traffic: make(chan struct{}), post: make(chan struct{}), fin: make(chan struct{}), cleanup: make(chan struct{}), agentHome: filepath.Join(f.root, "agent-home"), ctlHome: filepath.Join(f.root, "ctl-home")}
}
func (c *productCohort) setup(db *sql.DB) error {
	id, err := cli.EnsureAgentIdentity(c.agentHome, "product")
	if err != nil {
		return err
	}
	if err = agentprov.Provision(db, "product", "agent", id.Fingerprint, time.Now()); err != nil {
		return err
	}
	ctl, err := cli.EnsureIdentity(c.ctlHome)
	if err != nil {
		return err
	}
	if err = session.AddMember(db, "product", ctl.Fingerprint, session.RoleOwner, session.ViaPin, time.Now()); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(c.ctlHome, "current_session"), []byte("product\n"), 0600); err != nil {
		return err
	}
	if c.f.storeOnly || c.observerOnly {
		return setupStoreControlAgents(c, db)
	}
	c.ledger, err = os.OpenFile(filepath.Join(c.f.root, "cohort-bytes.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	for _, spec := range []struct {
		id, protocol string
		half         bool
	}{{"ssh", "ssh", false}, {"tcp-half", "tcp", true}, {"tcp-idle", "tcp", false}, {"tcp-active", "tcp", false}, {"tcp-slow", "tcp", false}, {"ss-bob", "ss", false}, {"ss-alice", "ss", false}} {
		// Keep the predeclared source live through the independent N3 census;
		// exhausting it early turns the fixture's FIN gate into business silence.
		// origin: S10 internal review FINAL-REAL-FAILS F6 / S6 plan §6.3 / T48
		e := &cohortEndpoint{evidence: cohortStream{ID: spec.id, Protocol: spec.protocol, HalfClosed: spec.half, FinalSequence: 2048}, ready: make(chan error, 1), reads: make(chan error, 1), writes: make(chan error, 2)}
		e.password = c.f.password
		if spec.id == "ss-alice" {
			secret := make([]byte, 32)
			if _, err := rand.Read(secret); err != nil {
				return err
			}
			e.password = base64.RawStdEncoding.EncodeToString(secret)
			if err := c.f.ss.SetKeys([]ssproxy.Key{{KeyID: "product", Secret: c.f.password}, {KeyID: "alice", Secret: e.password}}); err != nil {
				return err
			}
		}
		if spec.half {
			e.evidence.HalfSequence = 20
			e.evidence.FinalSequence = 2048
		}
		e.listener, err = net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			return err
		}
		c.endpoints = append(c.endpoints, e)
		public, err := strconv.Atoi(strings.Split(productFixtureAddress(c.f), ":")[1])
		if err != nil {
			return err
		}
		local, err := brokerFixtureTCPPort(e.listener.Addr())
		if err != nil {
			return err
		}
		if spec.protocol == "ss" {
			local = c.f.ssPeerPort()
		}
		alloc, err := port.Allocate(db, "product", "agent", "cohort-"+spec.id, local, public, "owner", false, &port.Config{BandLow: public, BandHigh: public})
		if err != nil {
			return err
		}
		e.allocation = *alloc
		c.f.allocations = append(c.f.allocations, *alloc)
		c.workers.Add(1)
		go func() { defer c.workers.Done(); c.serve(e) }()
	}
	if err := c.setupOpening(db); err != nil {
		return err
	}
	state := agentstate.StateFile{}
	for _, a := range c.f.allocations {
		p := agentstate.PortToken{Name: a.Name, Port: a.Port, LocalPort: a.LocalPort, Token: a.Token, Epoch: a.Epoch}
		if c.f.topology != "single" {
			p.HomeBrokerAddr = c.f.tunnel
		}
		state.PortTokens = append(state.PortTokens, p)
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return setupCohortMaintenanceAgent(c, data)
}
func (f *brokerProductFixture) ssPeerPort() int { return f.allocations[1].LocalPort }

func (c *productCohort) startAgent() error {
	// The broker executable is staged later; agent and ctl borrow an immutable
	// private copy, so broker-only replacement cannot change their mapped image.
	binary := filepath.Join(c.f.root, "broker-product.prev")
	c.agentLogs.path = filepath.Join(c.f.root, "agent.stderr")
	c.agent = exec.Command(binary, "agent", "--session", "product", "--nid", "agent", "--nats-url", c.f.nats.ClientURL(), "--tunnel-addr", c.f.tunnel, "--log-file", "-", "--log-json")
	c.agent.Dir = c.agentHome
	c.agent.Env = cohortEnv(c.agentHome)
	c.agent.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	c.agent.Stdout, c.agent.Stderr = &c.agentLogs, &c.agentLogs
	waitProcess, err := testharness.StartProcess(c.agent)
	if err != nil {
		return err
	}
	c.agentDone = make(chan error, 1)
	go func() { c.agentDone <- waitProcess() }()
	if c.f.storeOnly || c.observerOnly {
		return c.startControlAgent(binary)
	}
	until := time.Now().Add(15 * time.Second)
	for time.Now().Before(until) {
		if strings.Contains(c.agentLogs.String(), `"msg":"agent: registered"`) {
			// Register can apply home directives before replay; log counts are
			// not the installed port census. Sample the actual listening OFDs.
			if _, err := (&adminsock.Client{Path: filepath.Join(c.f.root, "admin.sock"), Timeout: time.Second}).Call(adminsock.Request{Op: adminsock.OpRuntime}); err == nil {
				data, err := os.ReadFile(filepath.Join(c.f.root, "cohort-listeners.json"))
				var listeners []listenerimage.Descriptor
				if err == nil && json.Unmarshal(data, &listeners) == nil {
					ports := map[string]bool{}
					for _, d := range listeners {
						ports[d.ID] = true
					}
					ready := true
					for _, a := range c.f.allocations {
						ready = ready && ports[fmt.Sprintf("public-%d", a.Port)]
					}
					if ready {
						return awaitCohortMaintenanceAgent(c)
					}
				}
			}
		}
		select {
		case err := <-c.agentDone:
			c.agentDone <- err
			return fmt.Errorf("real agent exited: %w %s", err, c.agentLogs.String())
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("real agent did not register: %s", c.agentLogs.String())
}
func cohortEnv(home string) []string {
	return append(handoff.CleanEnvironment(os.Environ()), "HOME="+home, "TETHER_HOME="+home, "TETHER_DEV_NO_AUTH=", "TETHER_SESSION=", "TETHER_NATS_URL=", "GORACE=atexit_sleep_ms=0")
}

func (c *productCohort) sample(e *cohortEndpoint, d uint8, seq uint64, start int64, b []byte, eof bool) error {
	end, err := controlbudget.Now()
	if err != nil {
		return err
	}
	row := cohortSample{ID: e.evidence.ID, Direction: d, Sequence: seq, Started: start, Finished: end, EOF: eof}
	if !eof {
		row.PayloadSHA = fmt.Sprintf("%x", sha256.Sum256(b))
		if !bytes.Equal(b, cohortPayload(e.evidence.ID, d, seq)) {
			return errors.New("cohort received incorrect application bytes")
		}
	}
	c.ledgerMu.Lock()
	defer c.ledgerMu.Unlock()
	return json.NewEncoder(c.ledger).Encode(row)
}
func (c *productCohort) awaitFIN() error {
	select {
	case <-c.fin:
		return nil
	case <-c.cleanup:
		return errors.New("cohort stopped")
	}
}
func (c *productCohort) awaitTraffic() error {
	select {
	case <-c.traffic:
		return nil
	case <-c.cleanup:
		return errors.New("cohort stopped")
	}
}
func (c *productCohort) awaitPost() error {
	select {
	case <-c.post:
		return nil
	case <-c.cleanup:
		return errors.New("cohort stopped")
	}
}
func (c *productCohort) receive(e *cohortEndpoint, r io.Reader, d uint8, end uint64, forward io.Writer) error {
	for seq := uint64(0); seq < end; seq++ {
		if seq == 20 {
			if err := c.awaitTraffic(); err != nil {
				return err
			}
		}
		if end > 20 && (seq == end-20) {
			if err := c.awaitPost(); err != nil {
				return err
			}
		}
		start, err := controlbudget.Now()
		if err != nil {
			return err
		}
		b := make([]byte, cohortRecordBytes)
		// The fixture deliberately idles between sides. Only an active record
		// consumes a business deadline; the initial SS handshake deadline is
		// never reused across the cut's observation/hash work.
		socket := e.target
		if d == 1 {
			socket = e.peer
		}
		_ = socket.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, readErr := io.ReadFull(r, b)
		_ = socket.SetReadDeadline(time.Time{})
		if err = readErr; err != nil {
			return fmt.Errorf("%s direction %d record %d: %w", e.evidence.ID, d, seq, err)
		}
		if err = c.sample(e, d, seq, start, b, false); err != nil {
			return err
		}
		if forward != nil {
			if _, err = forward.Write(b); err != nil {
				return err
			}
		}
	}
	if !e.evidence.HalfClosed || d != 0 {
		if err := c.awaitFIN(); err != nil {
			return err
		}
	}
	start, err := controlbudget.Now()
	if err != nil {
		return err
	}
	var tail [1]byte
	n, err := r.Read(tail[:])
	if n != 0 || !errors.Is(err, io.EOF) {
		return fmt.Errorf("%s direction %d has no exact EOF: %d %v", e.evidence.ID, d, n, err)
	}
	if forward != nil {
		if closer, ok := forward.(io.Closer); ok {
			_ = closer.Close()
		}
	}
	return c.sample(e, d, end, start, nil, true)
}

// Keep traffic running through the post-ready independent census. Prematurely
// completing bytes and gating only FIN makes archive/maintenance observation
// appear as trailing business silence. The longer bounded half-close tail also
// leaves more than a replay ring to deliver after the actual cut.
// origin: hot-update S6 plan §6.3 / T48 / EXITS-2-3
func cohortRecordPace(e *cohortEndpoint, d uint8, seq uint64) time.Duration {
	return 30 * time.Millisecond
}
func (c *productCohort) send(e *cohortEndpoint, w io.Writer, d uint8, end uint64) error {
	for seq := uint64(0); seq < end; seq++ {
		if seq == 20 {
			if err := c.awaitTraffic(); err != nil {
				return err
			}
		}
		if end > 20 && (seq == end-20) {
			if err := c.awaitPost(); err != nil {
				return err
			}
		}
		if seq >= 20 && seq < end-20 {
			select {
			case <-time.After(cohortRecordPace(e, d, seq)):
			case <-c.fin:
				// The post-cut census is complete. Drain every remaining record
				// and exact EOF without adding fixture idle time to each round.
				// origin: hot-update S6 plan §6.3 / T48
			case <-c.cleanup:
				return errors.New("cohort stopped")
			}
		}
		b := cohortPayload(e.evidence.ID, d, seq)
		for len(b) > 0 {
			n, err := w.Write(b)
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
			b = b[n:]
		}
	}
	// Keep original sockets observable through the after-cut census. Only
	// terminal FIN is gated; bytes resume immediately on product readiness.
	if !e.evidence.HalfClosed || d != 0 {
		if err := c.awaitFIN(); err != nil {
			return err
		}
	}
	if cw, ok := w.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	// The SSH stdin witness exposes Close rather than TCP CloseWrite.
	// origin: hot-update S6 plan §6.3 / T48
	closer, ok := w.(io.Closer)
	if !ok {
		return errors.New("cohort writer has no FIN operation")
	}
	return closer.Close()
}
func (c *productCohort) serve(e *cohortEndpoint) {
	conn, err := e.listener.Accept()
	if err != nil {
		e.ready <- err
		return
	}
	e.target = conn
	e.evidence.TargetPID = os.Getpid()
	e.evidence.TargetFD, err = cohortFD(conn)
	e.evidence.TargetAccepts = 1
	if err != nil {
		e.ready <- err
		return
	}
	if e.evidence.Protocol == "ssh" {
		c.serveSSH(e, conn)
		return
	}
	e.ready <- nil
	end := e.evidence.FinalSequence
	if e.evidence.HalfClosed {
		end = e.evidence.HalfSequence
	}
	received := make(chan error, 1)
	go func() { received <- c.receive(e, conn, 0, end, nil) }()
	sent := c.send(e, conn, 1, e.evidence.FinalSequence)
	readErr := <-received
	e.reads <- errors.Join(sent, readErr)
	<-c.cleanup
}

// A foreground process runs behind the actual SSH channel. It independently
// parses stdin and writes the other direction's records; its original PID and
// start ticks stay observable until the consumer has checked the final FIN.
// origin: hot-update S6 plan §6.3 / T48
const cohortSSHProgram = `import os,sys,time
id=sys.argv[1]
for seq in range(int(sys.argv[3])):
 b=sys.stdin.buffer.read(16384)
 if len(b)!=16384: sys.exit(12)
 prefix=("tether-broker-cohort-v1/%s/1/%d\n"%(id,seq)).encode()
 period=bytes(((i*31+seq*17+1)&255)^len(id) for i in range(256))
 out=prefix+(period*64)[len(prefix):]
 sys.stdout.buffer.write(out);sys.stdout.buffer.flush()
os.close(1)
while not os.path.exists(sys.argv[2]): time.sleep(.01)
`

func (c *productCohort) serveSSH(e *cohortEndpoint, conn net.Conn) {
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		e.ready <- err
		return
	}
	signer, err := ssh.NewSignerFromKey(private)
	if err != nil {
		e.ready <- err
		return
	}
	config := &ssh.ServerConfig{NoClientAuth: true}
	config.AddHostKey(signer)
	server, channels, requests, err := ssh.NewServerConn(conn, config)
	if err != nil {
		e.ready <- err
		return
	}
	defer closeBrokerFixtureWitness(c.f.test, server)
	go ssh.DiscardRequests(requests)
	opening, ok := <-channels
	if !ok {
		e.ready <- errors.New("SSH channel missing")
		return
	}
	ch, reqs, err := opening.Accept()
	if err != nil {
		e.ready <- err
		return
	}
	defer closeBrokerFixtureWitness(c.f.test, ch)
	req, ok := <-reqs
	if !ok || req.Type != "exec" {
		e.ready <- errors.New("SSH foreground exec missing")
		return
	}
	e.child = exec.Command("python3", "-I", "-B", "-c", cohortSSHProgram, e.evidence.ID, filepath.Join(c.f.root, "cohort-child-release"), strconv.FormatUint(e.evidence.FinalSequence, 10))
	e.child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	stdin, err := e.child.StdinPipe()
	if err != nil {
		e.ready <- err
		return
	}
	stdout, err := e.child.StdoutPipe()
	if err != nil {
		e.ready <- err
		return
	}
	waitProcess, err := testharness.StartProcess(e.child)
	if err != nil {
		e.ready <- err
		return
	}
	e.childDone = make(chan error, 1)
	go func() { e.childDone <- waitProcess() }()
	e.evidence.AppPID = e.child.Process.Pid
	e.evidence.AppTicks, err = storeFixtureStartTicks(e.evidence.AppPID)
	if err == nil {
		e.evidence.AppSHA, err = storeFixtureHash(fmt.Sprintf("/proc/%d/exe", e.evidence.AppPID))
	}
	if err != nil {
		e.ready <- err
		return
	}
	_ = req.Reply(true, nil)
	go ssh.DiscardRequests(reqs)
	e.ready <- nil
	received := make(chan error, 1)
	go func() { received <- c.receive(e, ch, 0, e.evidence.FinalSequence, stdin) }()
	_, writeErr := io.Copy(ch, stdout)
	_ = ch.CloseWrite()
	e.reads <- errors.Join(writeErr, <-received)
	<-c.cleanup
}

type cohortSSWriter struct{ p *productSSPeer }

func (w cohortSSWriter) Write(b []byte) (int, error) {
	n := min(len(b), 16383)
	err := w.p.write(b[:n])
	if err != nil {
		return 0, err
	}
	return n, nil
}
func (w cohortSSWriter) CloseWrite() error {
	// The reference peer must half-close the original TCP socket, never a replacement.
	// origin: hot-update S6 plan §6.3 / T48
	conn, ok := w.p.conn.(*net.TCPConn)
	if !ok || conn == nil {
		return errors.New("cohort SS peer has no TCP half-close operation")
	}
	return conn.CloseWrite()
}

type cohortSSReader struct {
	p       *productSSPeer
	pending []byte
}

func (r *cohortSSReader) Read(b []byte) (int, error) {
	if len(r.pending) == 0 {
		v, err := r.p.read()
		if err != nil {
			return 0, err
		}
		r.pending = v
	}
	n := copy(b, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}
func (c *productCohort) open() error {
	for _, e := range c.endpoints {
		var err error
		e.peer, err = net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(e.allocation.Port)), time.Second)
		if err != nil {
			return err
		}
		_ = e.peer.SetDeadline(time.Now().Add(5 * time.Second))
		var writer io.Writer = e.peer
		var reader io.Reader = e.peer
		if e.evidence.Protocol == "ss" {
			localPort, err := brokerFixtureTCPPort(e.listener.Addr())
			if err != nil {
				return err
			}
			e.ss, err = newProductSSPeer(e.peer, e.password, localPort)
			if err != nil {
				return err
			}
			_ = e.peer.SetDeadline(time.Time{})
			writer = cohortSSWriter{e.ss}
			reader = &cohortSSReader{p: e.ss}
			e.evidence.SSHeaders = 1
		}
		if e.evidence.Protocol == "ssh" {
			config := &ssh.ClientConfig{User: "cohort", HostKeyCallback: ssh.InsecureIgnoreHostKey(), Timeout: 5 * time.Second}
			conn, ch, reqs, err := ssh.NewClientConn(e.peer, e.peer.RemoteAddr().String(), config)
			if err != nil {
				return err
			}
			e.sshClient = ssh.NewClient(conn, ch, reqs)
			e.sshSession, err = e.sshClient.NewSession()
			if err != nil {
				return err
			}
			e.stdin, err = e.sshSession.StdinPipe()
			if err != nil {
				return err
			}
			e.stdout, err = e.sshSession.StdoutPipe()
			if err != nil {
				return err
			}
			if err = e.sshSession.Start("cohort-duplex"); err != nil {
				return err
			}
			writer = e.stdin
			reader = e.stdout
		}
		// Accepted-target nonemptiness is bounded even if a transport admission
		// failed before reaching this listener; cleanup must still join its worker.
		timer := time.NewTimer(5 * time.Second)
		select {
		case err := <-e.ready:
			timer.Stop()
			if err != nil {
				return err
			}
		case <-timer.C:
			return errors.New("original cohort target admission did not settle")
		}
		_ = e.peer.SetDeadline(time.Time{})
		end := e.evidence.FinalSequence
		if e.evidence.HalfClosed {
			end = e.evidence.HalfSequence
		}
		c.workers.Add(2)
		go func(e *cohortEndpoint, w io.Writer, end uint64) {
			defer c.workers.Done()
			e.writes <- c.send(e, w, 0, end)
		}(e, writer, end)
		go func(e *cohortEndpoint, r io.Reader) {
			defer c.workers.Done()
			err := c.receive(e, r, 1, e.evidence.FinalSequence, nil)
			if err != nil && e.evidence.Protocol == "ss" {
				e.evidence.SSAuthFailures++
			}
			e.writes <- err
		}(e, reader)
	}
	// The ledger, written by independent target/client readers, is the barrier.
	// All six streams have twenty exact records in both directions before cut.
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		if c.preCount() >= len(c.endpoints)*40+1 {
			return c.capture()
		}
		time.Sleep(time.Millisecond)
	}
	return errors.New("original cohort did not reach twenty pre-cut records and half-close EOF")
}
func (c *productCohort) preCount() int {
	c.ledgerMu.Lock()
	defer c.ledgerMu.Unlock()
	data, _ := os.ReadFile(c.ledger.Name())
	return bytes.Count(data, []byte{'\n'})
}

type cohortSocketIdentity struct {
	Device, Inode uint64
	Mode          uint32
}

func cohortSocketAt(pid, fd int) (cohortSocketIdentity, error) {
	var stat syscall.Stat_t
	err := syscall.Stat(fmt.Sprintf("/proc/%d/fd/%d", pid, fd), &stat)
	if err != nil || stat.Mode&syscall.S_IFMT != syscall.S_IFSOCK {
		return cohortSocketIdentity{}, errors.New("original raw socket missing")
	}
	return cohortSocketIdentity{uint64(stat.Dev), stat.Ino, stat.Mode & syscall.S_IFMT}, nil
}
func (c *productCohort) rawCensus() (map[string]cohortSocketIdentity, error) {
	out := map[string]cohortSocketIdentity{}
	for _, e := range c.endpoints {
		clientPort, err := brokerFixtureTCPPort(e.peer.LocalAddr())
		if err != nil {
			return nil, err
		}
		fd, err := cohortRemoteFD(c.f.cmd.Process.Pid, clientPort, e.allocation.Port)
		if err != nil {
			return nil, err
		}
		id, err := cohortSocketAt(c.f.cmd.Process.Pid, fd)
		if err != nil {
			return nil, err
		}
		out[e.evidence.ID] = id
	}
	return out, nil
}
func (c *productCohort) capture() error {
	if _, err := (&adminsock.Client{Path: filepath.Join(c.f.root, "admin.sock"), Timeout: 5 * time.Second}).Call(adminsock.Request{Op: adminsock.OpRuntime}); err != nil {
		return err
	}
	for _, e := range c.endpoints {
		fd, err := cohortFD(e.peer)
		if err != nil {
			return err
		}
		e.evidence.PublicPID = os.Getpid()
		e.evidence.PublicFD = fd
		c.streams = append(c.streams, e.evidence)
	}
	listeners, err := cohortListenerCensus(c.f.cmd.Process.Pid, c.endpoints)
	if err != nil {
		return err
	}
	c.listeners = listeners
	c.brokerSockets, err = c.rawCensus()
	return err
}

// Product readiness is an emitted BOOTTIME event. An admin census can spend
// hundreds of milliseconds hashing/querying after that event; it must not hold
// application traffic. The lexical cut owner always stops and joins this watcher.
// origin: hot-update S6 plan §8.1 / T48 / rv-ctl-3
func (c *productCohort) releaseOnReady() func() error {
	stop, done := make(chan struct{}), make(chan struct{})
	var result error
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			for _, line := range strings.Split(c.f.logs.String(), "\n") {
				var row struct {
					Event string `json:"event"`
					Cut   string `json:"cut_id"`
					Boot  int64  `json:"boot_ns"`
				}
				if json.Unmarshal([]byte(line), &row) == nil && row.Event == "control_ready" && row.Cut != "" && row.Cut != "boot" && row.Boot > 0 {
					result = c.opening.release()
					if result == nil {
						c.postOnce.Do(func() { close(c.post) })
					}
					return
				}
			}
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	}()
	return func() error { close(stop); <-done; return result }
}

func (c *productCohort) checkpoint(label string) (BrokerImageReceipt, error) {
	if label == "control-probe-config" {
		return cohortReceipt(label, map[string]any{"binary": filepath.Join(c.f.root, "broker-product.prev"), "home": c.ctlHome, "url": c.f.nats.ClientURL(), "nid": "agent", "short_nid": "short-agent", "root": c.f.root})
	}
	if label == "flow-cohort-before-reload" {
		if c.before {
			return BrokerImageReceipt{}, errors.New("cohort already started")
		}
		if err := c.open(); err != nil {
			return BrokerImageReceipt{}, err
		}
		c.before = true
	}
	if label == "flow-cohort-after-reload" {
		if !c.before {
			return BrokerImageReceipt{}, errors.New("cohort not established")
		}
		close(c.traffic)
		// A fresh connect is issued while the old store is actually closed by the
		// shared fixture. Its existing witness is the independently received echo.
		if _, err := c.f.productCheckpoint("reload-control-ready"); err != nil {
			return BrokerImageReceipt{}, err
		}

		for _, line := range strings.Split(c.f.logs.String(), "\n") {
			var row struct {
				Event string `json:"event"`
				Boot  int64  `json:"boot_ns"`
				Cut   string `json:"cut_id"`
			}
			if json.Unmarshal([]byte(line), &row) == nil && row.Cut != "" {
				if row.Event == "freeze_begin" {
					c.cut = row.Boot
				}
				if row.Event == "control_ready" {
					c.ready = row.Boot
				}
			}
		}
		if err := c.opening.after(); err != nil {
			return BrokerImageReceipt{}, err
		}
		census, err := c.rawCensus()
		if err != nil {
			return BrokerImageReceipt{}, err
		}
		if !reflect.DeepEqual(census, c.brokerSockets) {
			return BrokerImageReceipt{}, errors.New("broker exec replaced an original public raw socket")
		}
		if c.cut <= 0 || c.ready <= c.cut {
			return BrokerImageReceipt{}, errors.New("real product freeze/ready stamps missing")
		}
	}
	if label == "flow-cohort-finished" {
		close(c.fin)
		c.postOnce.Do(func() { close(c.post) })
		for _, e := range c.endpoints {
			for n := 0; n < 2; n++ {
				if err := <-e.writes; err != nil {
					return BrokerImageReceipt{}, err
				}
			}
			if err := <-e.reads; err != nil {
				return BrokerImageReceipt{}, err
			}
		}
		c.finished = true
	}
	base, err := c.f.productCheckpoint("before-reload")
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	var value map[string]any
	if err = json.Unmarshal(base.JSON, &value); err != nil {
		return BrokerImageReceipt{}, err
	}
	pid := c.agent.Process.Pid
	ticks, err := storeFixtureStartTicks(pid)
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	sha, err := storeFixtureHash(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	value["claim"], value["test_or_arm"], value["verdict"] = label, "T48", "GREEN"
	value["phase_intersections"] = []map[string]int64{{"freeze_begin": c.cut, "control_ready": c.ready}}
	value["oracle_inputs"] = map[string]string{"flow_ledger": filepath.Base(c.f.root) + "/cohort-bytes.jsonl"}
	value["stdout_path"], value["stderr_path"] = "product.stdout", "product.stderr"
	if label == "flow-cohort-before-reload" {
		if err := c.opening.start(); err != nil {
			return BrokerImageReceipt{}, err
		}
	}
	value["counters"] = map[string]any{"generation": value["generation"], "running_sha256": value["running_sha256"], "agent_pid": pid, "agent_startticks": ticks, "agent_running_sha256": sha, "cut_boot_ns": c.cut, "ready_boot_ns": c.ready, "streams": c.streams, "listeners": c.listeners, "expected_connect_slots": len(c.connects), "connects": c.connects, "pending_open": c.opening.receipt}
	if err = os.WriteFile(filepath.Join(c.f.root, "product.stderr"), []byte(c.f.logs.String()), 0600); err != nil {
		return BrokerImageReceipt{}, err
	}
	return cohortReceipt(label, value)
}
func cohortReceipt(label string, value any) (BrokerImageReceipt, error) {
	data, err := json.Marshal(value)
	return BrokerImageReceipt{Label: label, JSON: data, Digest: storeFixtureDigest(data)}, err
}
func cohortFD(conn net.Conn) (int, error) {
	// A non-kernel connection cannot certify original socket ownership.
	// origin: hot-update S6 plan §6.3 / T48
	socket, ok := conn.(syscall.Conn)
	if !ok {
		return 0, errors.New("cohort connection has no kernel socket")
	}
	raw, err := socket.SyscallConn()
	if err != nil {
		return 0, err
	}
	fd := -1
	err = raw.Control(func(n uintptr) { fd = int(n) })
	return fd, err
}
func cohortRemoteFD(pid, clientPort, publicPort int) (int, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/tcp", pid))
	if err != nil {
		return 0, err
	}
	data6, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/tcp6", pid))
	if err != nil {
		return 0, err
	}
	data = append(append(data, byte(10)), data6...)
	inodes := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}
		_, p, _ := strings.Cut(fields[2], ":")
		_, local, _ := strings.Cut(fields[1], ":")
		if p == fmt.Sprintf("%04X", clientPort) && local == fmt.Sprintf("%04X", publicPort) && fields[9] != "0" {
			inodes[fields[9]] = true
		}
	}
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return 0, err
	}
	for _, entry := range entries {
		link, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, entry.Name()))
		if inodes[strings.TrimSuffix(strings.TrimPrefix(link, "socket:["), "]")] {
			return strconv.Atoi(entry.Name())
		}
	}
	return 0, errors.New("original broker public socket absent")
}
func cohortListenerCensus(pid int, endpoints []*cohortEndpoint) ([]listenerimage.Descriptor, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/tcp", pid))
	if err != nil {
		return nil, err
	}
	data6, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/tcp6", pid))
	if err != nil {
		return nil, err
	}
	data = append(append(data, byte(10)), data6...)
	wanted := map[int]string{}
	for _, e := range endpoints {
		wanted[e.allocation.Port] = e.evidence.ID
	}
	var result []listenerimage.Descriptor
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 || fields[3] != "0A" {
			continue
		}
		_, p, _ := strings.Cut(fields[1], ":")
		n, _ := strconv.ParseUint(p, 16, 16)
		if wanted[int(n)] == "" {
			continue
		}
		inode, _ := strconv.ParseUint(fields[9], 10, 64)
		// Socket cookie and flags are observed inside the live product by the
		// shared observation overlay, rather than guessed from /proc FD counts.
		var descriptors []listenerimage.Descriptor
		observed, err := os.ReadFile(fmt.Sprintf("/proc/%d/cwd/cohort-listeners.json", pid))
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(observed, &descriptors); err != nil {
			return nil, err
		}
		for _, d := range descriptors {
			if d.Inode == inode {
				result = append(result, d)
			}
		}
	}
	if len(result) != len(endpoints) {
		return nil, errors.New("cohort listener cookie census incomplete")
	}
	return result, nil
}
func (c *productCohort) close() error {
	c.cleanupOnce.Do(func() { close(c.cleanup) })
	if c.opening != nil {
		c.opening.close()
	}
	_ = os.WriteFile(filepath.Join(c.f.root, "cohort-child-release"), []byte("release"), 0600)
	for _, e := range c.endpoints {
		if e.listener != nil {
			_ = e.listener.Close()
		}
		if e.peer != nil {
			_ = e.peer.Close()
		}
		if e.target != nil {
			_ = e.target.Close()
		}
		if e.sshClient != nil {
			_ = e.sshClient.Close()
		}
		if e.child != nil && e.child.Process != nil {
			_ = syscall.Kill(-e.child.Process.Pid, syscall.SIGKILL)
			if e.childDone != nil {
				<-e.childDone
			}
		}
	}
	if c.agent != nil && c.agent.Process != nil {
		_ = c.agent.Process.Signal(os.Interrupt)
		select {
		case <-c.agentDone:
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-c.agent.Process.Pid, syscall.SIGKILL)
			<-c.agentDone
		}
		_ = syscall.Kill(-c.agent.Process.Pid, syscall.SIGKILL)
	}
	if c.controlAgent != nil && c.controlDone != nil {
		_ = c.controlAgent.Process.Signal(os.Interrupt)
		select {
		case <-c.controlDone:
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-c.controlAgent.Process.Pid, syscall.SIGKILL)
			<-c.controlDone
		}
	}
	c.workers.Wait()
	if c.ledger != nil {
		return c.ledger.Close()
	}
	return nil
}

// Separate normal control traffic from the original-socket cohort; each target
// keeps the same production 256 MiB reservation owner and 8 MiB output logs.
// origin: hot-update S6 plan §10.1 / T50 / EXITS-2-3
func (c *productCohort) setupControlAgent(db *sql.DB) error {
	home := filepath.Join(c.f.root, "short-agent-home")
	id, err := cli.EnsureAgentIdentity(home, "product")
	if err != nil {
		return err
	}
	if err = agentprov.Provision(db, "product", "short-agent", id.Fingerprint, time.Now()); err != nil {
		return err
	}
	state := agentstate.StateFile{}
	for _, a := range c.f.allocations {
		state.PortTokens = append(state.PortTokens, agentstate.PortToken{Name: a.Name, Port: a.Port, LocalPort: a.LocalPort, Token: a.Token, Epoch: a.Epoch})
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(c.agentHome, "agent", "product", "state.json"), data, 0600)
}
func (c *productCohort) startControlAgent(binary string) error {
	home := filepath.Join(c.f.root, "short-agent-home")
	c.controlAgent = exec.Command(binary, "agent", "--session", "product", "--nid", "short-agent", "--nats-url", c.f.nats.ClientURL(), "--log-file", "-", "--log-json")
	c.controlAgent.Dir = home
	c.controlAgent.Env = cohortEnv(home)
	c.controlAgent.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	c.controlLogs.path = filepath.Join(c.f.root, "short-agent.stderr")
	c.controlAgent.Stdout, c.controlAgent.Stderr = &c.controlLogs, &c.controlLogs
	waitProcess, err := testharness.StartProcess(c.controlAgent)
	if err != nil {
		return err
	}
	c.controlDone = make(chan error, 1)
	go func() { c.controlDone <- waitProcess() }()
	until := time.Now().Add(15 * time.Second)
	for time.Now().Before(until) {
		if strings.Contains(c.agentLogs.String(), `"msg":"agent: registered"`) && strings.Contains(c.controlLogs.String(), `"msg":"agent: registered"`) {
			if _, err := (&adminsock.Client{Path: filepath.Join(c.f.root, "admin.sock"), Timeout: time.Second}).Call(adminsock.Request{Op: adminsock.OpRuntime}); err != nil {
				return err
			}
			data, err := os.ReadFile(filepath.Join(c.f.root, "cohort-listeners.json"))
			var listeners []listenerimage.Descriptor
			ready := err == nil && json.Unmarshal(data, &listeners) == nil
			for _, a := range c.f.allocations {
				found := false
				for _, d := range listeners {
					found = found || d.ID == fmt.Sprintf("public-%d", a.Port)
				}
				ready = ready && found
			}
			if ready {
				return awaitProductMaintenanceAgents(c, 2)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("control fixture: two real agents did not register")
}

// TCP fixtures need the original numeric port for allocation and /proc tuple
// matching. A non-TCP address cannot provide that evidence.
// origin: hot-update S6 plan §6.3 / T48
func brokerFixtureTCPPort(address net.Addr) (int, error) {
	tcp, ok := address.(*net.TCPAddr)
	if !ok || tcp == nil {
		return 0, errors.New("broker fixture: witness address is not TCP")
	}
	return tcp.Port, nil
}
