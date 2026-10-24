//go:build linux && hotupdate_integration

package hotupdate_test

// Real: workspace-built agent/broker/ctl, authenticated process-resume, kernel
// terminals and sealed runtime samples. The loopback NATS host is a hermetic
// supply; it cannot certify the privileged S9 deployment path.
// origin: hot-update S10 plan §6 / §10.2 P21
import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/fullstackharness"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
	"github.com/LinZiyang666/tether/test/transferharness"
	server "github.com/nats-io/nats-server/v2/server"
	"golang.org/x/sys/unix"
)

type fullstackTaskStack struct {
	*transferStack
	observer  *fullstackTaskObserver
	tx        map[string]string
	tracePath string
	tracer    *fullstackWriteTrace
}

// Tracing is a fixture resource, including when an assertion aborts before the
// partial-write checkpoint. Detach it while its original tracee is still live.
// origin: S10 internal review TAGGED-SUITE-SWEEP-B F5
func (s *fullstackTaskStack) close() {
	defer s.transferStack.close()
	if s.tracer != nil {
		s.tracer.stop()
	}
}

type fullstackTaskPipe struct {
	link    string
	witness *fullstackharness.OFDWitness
}

// origin: hot-update S10 plan §6 / M11P. Concurrent Go threads make strace
// split syscalls; associate each completion with its own thread's attempt.
func fullstackShortWrite(body string, fd int) (uint64, uint64) {
	start := regexp.MustCompile(`^(\d+)\s+write\((0x[0-9a-f]+|[0-9]+), [^,]+, (0x[0-9a-f]+|[0-9]+)(.*)$`)
	resume := regexp.MustCompile(`^(\d+)\s+<\.\.\. write resumed>(.*)$`)
	result := regexp.MustCompile(`\)\s+=\s+(0x[0-9a-f]+|[0-9]+)(?:\s|$)`)
	type attempt struct{ fd, size uint64 }
	pending := make(map[string]attempt)
	for _, line := range strings.Split(body, "\n") {
		var call attempt
		var suffix string
		if match := start.FindStringSubmatch(line); match != nil {
			call.fd, _ = strconv.ParseUint(match[2], 0, 64)
			call.size, _ = strconv.ParseUint(match[3], 0, 64)
			suffix = match[4]
			delete(pending, match[1])
			if strings.Contains(suffix, "<unfinished ...>") {
				pending[match[1]] = call
				continue
			}
		} else if match := resume.FindStringSubmatch(line); match != nil {
			call, suffix = pending[match[1]], match[2]
			delete(pending, match[1])
		} else {
			continue
		}
		if match := result.FindStringSubmatch(suffix); match != nil {
			n, _ := strconv.ParseUint(match[1], 0, 64)
			if call.fd == uint64(fd) && n > 0 && n < call.size {
				return call.size, n
			}
		}
	}
	return 0, 0
}

// Each original pipe direction is bound to an actual KCMP_FILE witness. A
// matching fd count cannot hide closing one task's source and leaking another.
// origin: hot-update S10 plan §4.2 / §6 / M02E/M11I/M11O
func fullstackTaskPipes(t *testing.T, s *fullstackTaskStack, child fullstackChildIdentity) []fullstackTaskPipe {
	t.Helper()
	stamp := fullstackTaskStamp(t, s)
	var pipes []fullstackTaskPipe
	for fd := 0; fd < 3; fd++ {
		link, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", child.PID, fd))
		transferMust(t, err)
		if !strings.HasPrefix(link, "pipe:[") {
			t.Fatal("exec business direction is not its original pipe")
		}
		witness, err := fullstackharness.NewOFDWitness(stamp, fullstackFindPipe(t, stamp.Process.PID, link))
		transferMust(t, err)
		t.Cleanup(func() {
			if err := witness.Close(); err != nil {
				t.Error(err)
			}
		})
		pipes = append(pipes, fullstackTaskPipe{link, witness})
	}
	return pipes
}

func fullstackFindPipe(t *testing.T, pid int, link string) int {
	t.Helper()
	root := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(root)
	transferMust(t, err)
	var matches []int
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join(root, entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		transferMust(t, err)
		if target == link {
			fd, err := strconv.Atoi(entry.Name())
			transferMust(t, err)
			matches = append(matches, fd)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("exec direction has %d live product descriptors", len(matches))
	}
	return matches[0]
}

func fullstackCheckPipes(t *testing.T, s *fullstackTaskStack, pipes []fullstackTaskPipe) {
	t.Helper()
	stamp := fullstackTaskStamp(t, s)
	for _, pipe := range pipes {
		_, err := pipe.witness.Check(t.Context(), stamp, fullstackFindPipe(t, stamp.Process.PID, pipe.link))
		transferMust(t, err)
	}
}

// Reuse the shared supervised artifact/process owners, but keep the task
// observer separate: the S7 collector intentionally discards runtime samples.
func newFullstackTaskStack(t *testing.T, a transferArtifacts, traceWrites ...bool) *fullstackTaskStack {
	t.Helper()
	transferFixtureSlots <- struct{}{}
	s := &transferStack{t: t, h: transferharness.New(t, transferSupervisor{}), a: a,
		sid: "fullstack-tasks", nid: "task-agent", collectors: make(map[string]*transferCollector)}
	task := &fullstackTaskStack{transferStack: s, tx: make(map[string]string)}
	t.Cleanup(task.close)
	s.home, s.agentHome = filepath.Join(s.h.Root, "ctl-home"), filepath.Join(s.h.Root, "agent-home")
	for _, home := range []string{s.home, s.agentHome} {
		transferMust(t, os.MkdirAll(home, 0700))
	}
	s.brokerPath, s.agentPath = filepath.Join(s.h.Root, "broker"), filepath.Join(s.h.Root, "agent")
	for _, path := range []string{s.brokerPath, s.agentPath, filepath.Join(s.h.Root, "ctl")} {
		transferCopy(t, a.a, path)
	}
	id, err := cli.EnsureIdentity(s.home)
	transferMust(t, err)
	transferMust(t, os.WriteFile(filepath.Join(s.home, "current_session"), []byte(s.sid+"\n"), 0600))
	dbPath := filepath.Join(s.h.Root, "broker.db")
	db, err := storage.Open(dbPath)
	transferMust(t, err)
	// Session incarnation is derived from the real PIN salt. A placeholder
	// leaves the registered runtime unable to prove its authority at a cut.
	// origin: S10 internal review TAGGED-SUITE-SWEEP-B F3
	pinHash, err := auth.HashPIN("private-task-fixture")
	transferMust(t, err)
	_, createErr := session.Create(db, s.sid, s.sid, id.Fingerprint, pinHash, time.Now().UTC())
	transferMust(t, errors.Join(createErr, db.Close()))
	// Retrofit registration is immutable fixture supply before the first boot,
	// just as in the landed S8 fixture. Product CLI owns every later mutation.
	// origin: S10 internal review TAGGED-SUITE-SWEEP-B F3
	registerTransferAgent(t, s)
	configPath := filepath.Join(s.h.Root, "broker.yaml")
	transferMust(t, os.WriteFile(configPath, []byte(fmt.Sprintf("broker:\n  storage:\n    db: %q\n", dbPath)), 0600))
	role := upgradecli.BrokerBootRole(s.h.Root, configPath, "", uint32(os.Geteuid()))
	transferMust(t, os.MkdirAll(filepath.Join(role.ControlDir, "artifacts"), 0700))
	_, err = upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: s.brokerPath, ControlDir: role.ControlDir,
		DeploymentID: "fullstack-tasks", Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "fullstack-broker"}}})
	transferMust(t, err)
	s.options = server.Options{Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true,
		AlwaysEnableNonce: true, JetStream: true, StoreDir: filepath.Join(s.h.Root, "jetstream")}
	s.startJS()
	s.url = s.ns.ClientURL()
	s.artifact = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(a.archiveB)
	}))
	s.admin = filepath.Join(s.h.Root, "admin.sock")
	tunnel := transferAddress(t)
	s.broker = s.launch("broker", s.brokerPath, s.h.Root, []string{"serve", "--config", configPath, "--nats-url", s.url,
		"--db", dbPath, "--admin-socket", s.admin, "--tunnel-addr", tunnel,
		"--tunnel-public-host", "127.0.0.1", "--upgrade-url-allow", s.artifact.URL + "/", "--log-file", "-"}, false)
	if !testharness.WaitFor(t, 15*time.Second, 20*time.Millisecond, func() bool {
		r, err := (&adminsock.Client{Path: s.admin, Timeout: 100 * time.Millisecond}).Call(adminsock.Request{Op: adminsock.OpRuntime})
		return err == nil && r.OK
	}) {
		t.Fatalf("SETUP-RED: private broker readiness: %s", s.output(s.broker))
	}
	c := newTransferCollector(t)
	s.collectors["agent"] = c
	o := &fullstackTaskObserver{collector: c, samples: make(map[[12]byte]drillobserve.RuntimeCounters), acks: make(map[[12]byte][]byte), sequences: make(map[[12]byte]uint64)}
	env := append(flowSandboxEnvironment(s.agentHome), "GORACE=atexit_sleep_ms=0")
	// The shared reaper is this test binary and imports drillobserve itself.
	// Set custody variables only AFTER that reaper's init, using env's same-PID
	// exec. Otherwise it consumes the target's sealed configuration first.
	launcher := filepath.Join(s.h.Root, "agent-launcher")
	transferCopy(t, "/usr/bin/env", launcher)
	// These binaries have no S7 observation overlay. Use the product's sealed
	// startup protocol after the supervisor init; private S7 enablers are inert.
	// origin: S10 internal review TAGGED-SUITE-SWEEP-B F1
	args := append([]string{drillobserve.EnableEnv + "=v1", drillobserve.CustodyEnv + "=4"}, s.agentPath, "agent", "--session", s.sid, "--nid", s.nid, "--nats-url", s.url,
		"--tunnel-addr", tunnel, "--upgrade-url-allow", s.artifact.URL+"/", "--log-file", "-")
	tracePath := ""
	if len(traceWrites) != 0 && traceWrites[0] {
		// Yama forbids a sibling observer attaching later. strace -D starts the
		// tracer at birth and then execs the tracee at this same supervised PID.
		// Its grandchild remains inside the shared supervisor's descendant lease.
		tracePath = filepath.Join(s.h.Root, "write-syscalls.txt")
		tracer := filepath.Join(s.h.Root, "syscall-observer")
		transferCopy(t, "/usr/bin/strace", tracer)
		// With an output file, strace otherwise blocks fatal signals. The
		// fixture's SIGINT detach must remain effective on every exit path.
		// origin: S10 internal review TAGGED-SUITE-SWEEP-B F5
		args = append([]string{"-D", "-I", "1", "-f", "-qq", "-e", "trace=write", "-e", "raw=write", "-o", tracePath, launcher}, args...)
		launcher = tracer
	}
	s.agent, err = s.h.Start(t.Context(), transferharness.Spec{Binary: launcher, ArtifactSHA: hotUpdateFileSHA(t, launcher), Env: env,
		Args: args, ExtraFiles: c.files()})
	transferMust(t, err)
	c.process, c.running = s.agent, true
	_ = c.child.Close()
	_ = c.config.Close()
	go o.read()
	if !testharness.WaitFor(t, 15*time.Second, 20*time.Millisecond, func() bool {
		connections, err := s.ns.Connz(&server.ConnzOptions{Subscriptions: true})
		if err != nil {
			return false
		}
		for _, connection := range connections.Conns {
			for _, subscription := range connection.Subs {
				if subscription == proto.SubjCmdForwarded(s.sid, s.nid, "*") {
					return true
				}
			}
		}
		return false
	}) {
		t.Fatalf("SETUP-RED: private agent readiness: %s", s.output(s.agent))
	}
	task.observer, task.tracePath = o, tracePath
	if tracePath != "" {
		fullstackTraceWrites(t, task)
	}
	return task
}

type fullstackTaskObserver struct {
	collector *transferCollector
	mu        sync.Mutex
	samples   map[[12]byte]drillobserve.RuntimeCounters
	acks      map[[12]byte][]byte
	sequences map[[12]byte]uint64
	err       error
}

// Custody is consumed continuously, so a long command cannot fill the socket
// and silently lose samples. Every received descriptor is closed on all paths.
func (o *fullstackTaskObserver) read() {
	c := o.collector
	defer close(c.done)
	for {
		select {
		case <-c.stop:
			return
		default:
		}
		buf, ancillary := make([]byte, 128), make([]byte, unix.CmsgSpace(16))
		n, on, flags, _, err := unix.Recvmsg(c.fd, buf, ancillary, unix.MSG_DONTWAIT|unix.MSG_CMSG_CLOEXEC)
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
			_, _ = unix.Poll([]unix.PollFd{{Fd: int32(c.fd), Events: unix.POLLIN}}, 20)
			continue
		}
		if n == 0 && err == nil {
			return
		}
		messages, parseErr := unix.ParseSocketControlMessage(ancillary[:on])
		var fds []int
		for _, message := range messages {
			rights, e := unix.ParseUnixRights(&message)
			parseErr = errors.Join(parseErr, e)
			fds = append(fds, rights...)
		}
		o.mu.Lock()
		if err != nil || parseErr != nil || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
			o.err = errors.Join(o.err, drillobserve.ErrRuntimeSample)
		} else if n == 72 && len(fds) == 0 {
			var nonce [12]byte
			copy(nonce[:], buf[4:16])
			o.acks[nonce] = bytes.Clone(buf[:n])
		} else if n == 4 && string(buf[:4]) == "TDC1" && len(fds) == 1 {
			o.accept(fds[0])
		} else {
			o.err = errors.Join(o.err, drillobserve.ErrRuntimeSample)
		}
		o.mu.Unlock()
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
		if err != nil {
			return
		}
	}
}

func (o *fullstackTaskObserver) accept(fd int) {
	c := o.collector
	seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	const required = unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	var stat unix.Stat_t
	if err != nil || seals&required != required || unix.Fstat(fd, &stat) != nil || stat.Size < 76 || stat.Size > 1<<20 {
		o.err = drillobserve.ErrRuntimeSample
		return
	}
	body := make([]byte, stat.Size)
	if n, err := unix.Pread(fd, body, 0); err != nil || n != len(body) {
		o.err = drillobserve.ErrRuntimeSample
		return
	}
	if !bytes.Equal(c.incarnation, body[40:56]) {
		identity, err := c.process.SampleIdentity()
		if err != nil || len(c.incarnation) != 0 && identity.RunningSHA == c.lastSHA {
			o.err = drillobserve.ErrRuntimeSample
			return
		}
		c.lastSHA, c.incarnation = identity.RunningSHA, bytes.Clone(body[40:56])
		c.reader = drillobserve.TransferCustodyReader{Identity: c.identity}
	}
	events, err := c.reader.Accept(body)
	if err != nil {
		o.err = err
		return
	}
	for _, event := range events {
		if event.Kind == drillobserve.RuntimeSampleKind {
			sample, err := drillobserve.DecodeRuntimeSample(event)
			if err != nil {
				o.err = err
				return
			}
			o.samples[sample.Nonce] = sample
			o.sequences[sample.Nonce] = binary.LittleEndian.Uint64(body[56:64])
		}
	}
	// The shared decoder verifies the contiguous envelope sequence. ACK binds
	// the challenged sample to the corresponding sealed batch, independently.
}

func fullstackRuntimeSample(t *testing.T, s *fullstackTaskStack) drillobserve.RuntimeCounters {
	t.Helper()
	var nonce [12]byte
	_, err := rand.Read(nonce[:])
	transferMust(t, err)
	scope := sha256.Sum256([]byte(t.Name()))
	request, err := drillobserve.RuntimeSampleRequest(nonce, scope)
	transferMust(t, err)
	transferMust(t, unix.Sendmsg(s.observer.collector.fd, request, nil, nil, unix.MSG_NOSIGNAL))
	var sample drillobserve.RuntimeCounters
	if !testharness.WaitFor(t, 5*time.Second, 10*time.Millisecond, func() bool {
		o := s.observer
		o.mu.Lock()
		defer o.mu.Unlock()
		transferMust(t, o.err)
		var ok bool
		sample, ok = o.samples[nonce]
		if !ok || o.acks[nonce] == nil {
			return false
		}
		transferMust(t, drillobserve.ValidateRuntimeSampleACK(o.acks[nonce], sample, o.sequences[nonce]))
		return true
	}) {
		t.Fatal("live product did not answer the fresh sealed runtime sample/ACK challenge")
	}
	actual, err := s.agent.SampleIdentity()
	transferMust(t, err)
	if sample.PID != uint64(actual.PID) || sample.StartTicks != actual.ProcessStart || fmt.Sprintf("%x", sample.ExeSHA) != actual.RunningSHA {
		t.Fatal("runtime sample did not belong to the still-live product image")
	}
	return sample
}

// Five settled cycles occur inside ONE live child image. Parent leak checks or
// a goroutine reset caused by exec cannot discharge this obligation.
// origin: hot-update S10 plan §4.1 / §10.2 P21 / M08E/M08P
func fullstackTaskCycles(t *testing.T, s *fullstackTaskStack, kind string) {
	t.Helper()
	before := fullstackRuntimeSample(t, s)
	for cycle := 0; cycle < 5; cycle++ {
		w := newFullstackTaskPeer(t, s, kind)
		fullstackCreate(t, w, []string{"/usr/bin/python3", "-I", "-c", "import os; os.write(1,b'cycle'); raise SystemExit(23)"})
		fullstackFinish(t, w, []byte("cycle"), nil, 23)
		w.close()
	}
	var after drillobserve.RuntimeCounters
	if !testharness.WaitFor(t, 5*time.Second, 20*time.Millisecond, func() bool {
		after = fullstackRuntimeSample(t, s)
		return after.Goroutines <= before.Goroutines+2 && after.FDs <= before.FDs
	}) {
		t.Fatalf("same-image task leak: G %d -> %d, FD %d -> %d", before.Goroutines, after.Goroutines, before.FDs, after.FDs)
	}
	if before.PID != after.PID || before.StartTicks != after.StartTicks || before.ExeSHA != after.ExeSHA || before.Binding != after.Binding {
		t.Fatal("five-cycle child resource check crossed an image boundary")
	}
	t.Logf("child-five-cycle kind=%s pid=%d G=%d/%d FD=%d/%d", kind, after.PID, before.Goroutines, after.Goroutines, before.FDs, after.FDs)
}

// Start may lose its result to the accepted cut. Only a mutation on an
// existing handle may proceed to wait; prepare, query and wait must succeed.
// origin: S10 internal review TAGGED-SUITE-SWEEP-B F4
func fullstackTaskStartUnsettled(args []string, rc int) bool {
	if rc != 74 || len(args) < 3 {
		return false
	}
	for _, arg := range args {
		if arg == "--tx-id" {
			return args[0] == "node" && (args[1] == "upgrade" || args[1] == "upgrade-rollback") ||
				args[0] == "broker" && args[1] == "upgrade" && (args[2] == "reload" || args[2] == "rollback")
		}
	}
	return false
}

// Use the frozen product argv and S6 candidate supply. Never supply .prev or
// treat a help/preflight rejection as an accepted continuity mutation.
// P18's current native View projection is pending; retaining the full S8 View
// here avoids manufacturing a boot-clock Stamp from its Unix timestamps.
// origin: hot-update S10 plan §2.1 / §5.1 / §6 / M01A/M05R
func fullstackTaskCut(t *testing.T, s *fullstackTaskStack, role string, reverse bool) {
	t.Helper()
	process := s.agent
	if role == "broker" {
		process = s.broker
	}
	before, err := process.SampleIdentity()
	transferMust(t, err)
	command := func(args ...string) transferharness.Receipt {
		// Only remote node verbs take these flags. Local broker/stage commands
		// use their explicit socket/grant and the sandbox environment instead.
		if len(args) > 0 && args[0] == "node" {
			args = append(args, "--home", s.home, "--nats-url", s.url)
		}
		home := s.home
		if len(args) > 0 && args[0] == "upgrade" && role == "agent" {
			home = s.agentHome // Root stage resolves registration from the component's own home.
		}
		p := s.launch("transaction-command", filepath.Join(s.h.Root, "ctl"), home, args, false)
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		r, err := p.Wait(ctx)
		if err != nil && !fullstackTaskStartUnsettled(args, r.NativeRC) {
			stdout, _ := os.ReadFile(r.StdoutPath)
			t.Fatalf("%s product transaction CLI failed: native_rc=%d error=%v stdout=%s stderr=%s", role, r.NativeRC, err, stdout, s.output(p))
		}
		return r
	}
	if !reverse {
		grantPath, receiptPath := filepath.Join(s.h.Root, role+"-grant.json"), filepath.Join(s.h.Root, role+"-stage.json")
		if role == "agent" {
			command("node", "upgrade", "--url", s.artifact.URL+"/candidate.tar.gz", "--sha256", transferDigest(s.a.archiveB),
				"--prepare-only", "--grant-out", grantPath, s.nid)
		} else {
			// S6 describe probes the candidate at the registered canonical
			// entry. Supply only that inert candidate; root stage still pins
			// the actual /proc source and Start owns activation.
			// origin: S8 internal review activation-and-route-migration F3
			staged := filepath.Join(s.h.Root, "broker-candidate")
			transferCopy(t, s.a.b, staged)
			transferMust(t, os.Rename(staged, s.brokerPath))
			command("broker", "upgrade", "reload", "--socket", s.admin, "--to-version", transferArtifactRelease(s.transferStack, s.a.b),
				"--expect-sha256", s.a.shaB, "--prepare-only", "--grant-out", grantPath)
		}
		var grant upgradetx.Grant
		fullstackReadJSON(t, grantPath, &grant)
		if grant.Handle.TxID == "" || grant.SourceSHA != before.RunningSHA || grant.TargetSHA != s.a.shaB {
			t.Fatal("target-issued grant is missing or names different images")
		}
		s.tx[role] = grant.Handle.TxID
		command("upgrade", "stage", "--grant", grantPath, "--source", s.a.a, "--candidate", s.a.b, "--receipt-out", receiptPath)
		if role == "agent" {
			command("node", "upgrade", "--tx-id", s.tx[role], "--artifact-receipt", receiptPath, s.nid)
		} else {
			command("broker", "upgrade", "reload", "--socket", s.admin, "--tx-id", s.tx[role], "--artifact-receipt", receiptPath)
		}
	} else {
		var result transferharness.Receipt
		if role == "agent" {
			result = command("node", "upgrade-rollback", "--tx-id", s.tx[role], "--json", s.nid)
		} else {
			result = command("broker", "upgrade", "rollback", "--socket", s.admin, "--tx-id", s.tx[role], "--json")
		}
		// A manual reverse of completed execution has a newly issued ID.
		// Following the old ID waits on forward history and loses the reverse;
		// a native 74 authorizes nothing without this accepted reverse record.
		// origin: S8 internal review activation-and-route-migration F6
		var reply upgradetx.View
		if result.NativeRC == 74 {
			// An early accepted notice intentionally has no current observation.
			// Strictly decode its DTO and bind its execution identity below;
			// full current-proof/schema validation remains after Wait/Status.
			body, err := os.ReadFile(result.StdoutPath)
			transferMust(t, err)
			decoder := json.NewDecoder(bytes.NewReader(body))
			decoder.DisallowUnknownFields()
			transferMust(t, decoder.Decode(&reply))
		} else {
			fullstackReadJSON(t, result.StdoutPath, &reply)
		}
		attempt := reply.Historical.Forward
		if reply.Historical.Reverse.Handle.TxID != "" {
			attempt = reply.Historical.Reverse
		}
		if attempt.Direction != upgradetx.DirectionReverse || attempt.SourceSHA != before.RunningSHA || attempt.TargetSHA != s.a.shaA || attempt.Handle.TxID != reply.Historical.Grant.Handle.TxID {
			t.Fatalf("rollback did not announce an accepted current reverse: tx_id=%s direction=%s phase=%s code=%s detail=%s", attempt.Handle.TxID, attempt.Direction, attempt.Phase, reply.Code, reply.Detail)
		}
		s.tx[role] = attempt.Handle.TxID
	}
	// A native 74 from Start is never completion. Wait challenges the same
	// original handle, and the full current View below must still complete.
	// origin: S10 internal review TAGGED-SUITE-SWEEP-B F4
	if role == "agent" {
		command("node", "upgrade-wait", "--tx-id", s.tx[role], "--timeout", "120s", s.nid)
	} else {
		command("broker", "upgrade", "wait", "--socket", s.admin, "--tx-id", s.tx[role], "--timeout", "120s")
	}
	want := s.a.shaB
	if reverse {
		want = s.a.shaA
	}
	if !testharness.WaitFor(t, 20*time.Second, 20*time.Millisecond, func() bool {
		after, err := process.SampleIdentity()
		return err == nil && after.RunningSHA == want
	}) {
		t.Fatal("accepted transaction did not change the original process image")
	}
	after, err := process.SampleIdentity()
	transferMust(t, err)
	transferMust(t, validateTransferProcess(before, after, want))
	var result transferharness.Receipt
	if role == "agent" {
		result = command("node", "upgrade-status", "--tx-id", s.tx[role], "--json", s.nid)
	} else {
		result = command("broker", "upgrade", "status", "--socket", s.admin, "--tx-id", s.tx[role], "--json")
	}
	var view upgradetx.View
	fullstackReadJSON(t, result.StdoutPath, &view)
	transferMust(t, upgradetx.Completed(view.Observation.Current, view.Observation))
	if view.Observation.RunningSHA != want || view.Observation.Current.Handle.TxID != s.tx[role] {
		t.Fatal("completed View is not the original current transaction")
	}
	if role == "agent" {
		// The independent private sample must name the cut whose three
		// receipts just completed, not an invented per-image observer token.
		// origin: S8 internal review activation-and-route-migration F4
		sample := fullstackRuntimeSample(t, s)
		if sample.CutID != view.Observation.Current.Handle.CutID || sample.Binding.Generation != view.Observation.Current.Handle.Generation {
			t.Fatal("runtime sample does not identify the completed transaction cut")
		}
	}
}

func fullstackReadJSON(t *testing.T, path string, value any) {
	t.Helper()
	body, err := os.ReadFile(path)
	transferMust(t, err)
	if _, native := value.(*upgradetx.View); native {
		transferMust(t, contract.ValidateJSON("s8_view", body))
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	transferMust(t, decoder.Decode(value))
	if err := decoder.Decode(new(any)); err != io.EOF {
		t.Fatal("receipt contains trailing JSON")
	}
}

func fullstackWaitFile(t *testing.T, path string) []byte {
	t.Helper()
	var body []byte
	if !testharness.WaitFor(t, 10*time.Second, 10*time.Millisecond, func() bool {
		var err error
		body, err = os.ReadFile(path)
		return err == nil && len(body) != 0
	}) {
		t.Fatalf("original business child did not publish %s", filepath.Base(path))
	}
	return body
}

type fullstackChildIdentity struct {
	PID, Session, PGID, TTY int
	Ticks                   uint64
}

func fullstackChild(t *testing.T, root string) fullstackChildIdentity {
	t.Helper()
	fullstackWaitFile(t, filepath.Join(root, "identity.json"))
	var id fullstackChildIdentity
	fullstackReadJSON(t, filepath.Join(root, "identity.json"), &id)
	if id.PID <= 0 || id.Ticks == 0 {
		t.Fatal("business child identity is incomplete")
	}
	return id
}

func fullstackChildState(id fullstackChildIdentity) (string, bool) {
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", id.PID))
	if err != nil {
		return "", false
	}
	_, fields, ok := strings.Cut(string(body), ") ")
	parts := strings.Fields(fields)
	if !ok || len(parts) < 20 {
		return "", false
	}
	ticks, err := strconv.ParseUint(parts[19], 10, 64)
	return parts[0], err == nil && ticks == id.Ticks
}

func fullstackGate(t *testing.T, root, name string) {
	t.Helper()
	transferMust(t, os.WriteFile(filepath.Join(root, name), []byte("continue"), 0600))
}

func fullstackNativeWait(t *testing.T, p *transferharness.Process, code int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	r, err := p.Wait(ctx)
	if r.NativeRC != code || fullstackharness.ProcessExitCode(err) != code {
		t.Fatalf("original ctl wait status=%d, want=%d, error=%v", r.NativeRC, code, err)
	}
}
