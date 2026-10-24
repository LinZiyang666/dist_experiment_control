//go:build linux

package cli_e2e_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/test/transferharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// This adapter delegates fork, descendant custody and Wait to FIX-ORPHAN.
// The local goroutine only translates the original native status for P29.
// origin: hot-update S7 plan §11.3 / lead L16 / P33
type transferSupervisor struct{}

func (transferSupervisor) Launch(cmd *exec.Cmd) (transferharness.Child, error) {
	wait, err := testharness.StartProcess(cmd)
	if err != nil {
		return transferharness.Child{}, err
	}
	exits, joined := make(chan transferharness.Exit, 1), make(chan struct{})
	go func() {
		err := wait()
		rc := 0
		if err != nil {
			rc = -1
			var native *exec.ExitError
			if errors.As(err, &native) {
				rc = native.ExitCode()
			}
		}
		exits <- transferharness.Exit{NativeRC: rc, Err: err}
		close(exits)
		close(joined)
	}()
	stop := func() error {
		select {
		case <-joined:
			return nil
		default:
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-joined:
			return nil
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
		}
		select {
		case <-joined:
			return nil
		case <-time.After(5 * time.Second):
			return errors.New("transfer supervisor did not join descendants")
		}
	}
	return transferharness.Child{PID: cmd.Process.Pid, Done: exits, Stop: stop}, nil
}

type transferPeer struct {
	sid, nid, home, agentHome, agentBinary string
	id                                     *cli.Identity
	agent                                  *transferharness.Process
}

type transferWireRow struct {
	Subject string
	Data    []byte
}

type transferStack struct {
	t      *testing.T
	h      *transferharness.Harness
	ctx    context.Context
	bin    string
	url    string
	db     *sql.DB
	ns     *natsserver.Server
	nc     *nats.Conn
	js     jetstream.JetStream
	broker *transferharness.Process
	peers  []*transferPeer
	mu     sync.Mutex
	wire   []transferWireRow
}

// Real serve/agent/ctl artifacts share only a private embedded file-store NATS.
// SQL is seeded before serve opens it; fault injection never changes product rows.
// origin: hot-update S7 plan §9 / T40 / T45
func newTransferStack(t *testing.T, brokerBinary, agentBinary string, callout bool, sids ...string) *transferStack {
	t.Helper()
	h := transferharness.New(t, transferSupervisor{})
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	t.Cleanup(cancel)
	f := &transferStack{t: t, h: h, ctx: ctx}
	f.bin = copyTransferArtifact(t, h.Root, "ctl", buildTether(t))
	brokerBinary = copyTransferArtifact(t, h.Root, "broker", brokerBinary)
	var err error
	f.db, err = storage.Open(filepath.Join(h.Root, "broker.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.db.Close() })
	realStackPINOnce.Do(func() { realStackPINHash, realStackPINErr = auth.HashPIN(realStackPIN) })
	if realStackPINErr != nil {
		t.Fatal(realStackPINErr)
	}
	for _, sid := range sids {
		p := &transferPeer{sid: sid, nid: "agt1", home: filepath.Join(h.Root, sid, "ctl"), agentHome: filepath.Join(h.Root, sid, "agent")}
		for _, dir := range []string{p.home, p.agentHome} {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		p.id, err = cli.EnsureIdentity(p.home)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(p.home, "current_session"), []byte(sid+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := session.Create(f.db, sid, sid, p.id.Fingerprint, realStackPINHash, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		p.agentBinary = copyTransferArtifact(t, h.Root, sid+"-agent", agentBinary)
		f.peers = append(f.peers, p)
	}
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true,
		StoreDir: filepath.Join(h.Root, "js"), MaxPayload: 16 << 20, AlwaysEnableNonce: true, NoLog: true, NoSigs: true}
	// Both broker admission and agent download enforce the artifact URL policy.
	// origin: S7 internal review MAKETEST-TRANSFER F6; S7 plan §9 / T35
	brokerArgs := []string{"serve", "--db", filepath.Join(h.Root, "broker.db"), "--tunnel-addr", "", "--admin-socket", "", "--public-host", "test.local", "--upgrade-url-allow", "http://127.0.0.1:"}
	var adminOpts []nats.Option
	if callout {
		brokerSeed, accountSeed := mintRealStackSeeds(t)
		bk, err := nkeys.FromSeed(brokerSeed)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(bk.Wipe)
		ak, err := nkeys.FromSeed(accountSeed)
		if err != nil {
			t.Fatal(err)
		}
		bp, _ := bk.PublicKey()
		ap, _ := ak.PublicKey()
		ak.Wipe()
		// The independent collector needs broad read access; borrowing a product
		// broker key would make its wildcard SUB fail the broker's real ACL.
		// Product broker/agent/ctl grants remain entirely under auth_callout.
		observer, err := nkeys.CreateUser()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(observer.Wipe)
		op, _ := observer.PublicKey()
		opts.Nkeys = []*natsserver.NkeyUser{{Nkey: bp, Permissions: realStackServerPerms(auth.PermissionsForBroker())}, {Nkey: op}}
		opts.AuthCallout = &natsserver.AuthCallout{Issuer: ap, Account: "$G", AuthUsers: []string{bp, op}}
		adminOpts = append(adminOpts, nats.Nkey(op, observer.Sign))
		seeds := filepath.Join(h.Root, "seeds")
		if err := os.Mkdir(seeds, 0o700); err != nil {
			t.Fatal(err)
		}
		for name, seed := range map[string][]byte{"broker.nk": brokerSeed, "account.nk": accountSeed} {
			if err := os.WriteFile(filepath.Join(seeds, name), seed, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		brokerArgs = append(brokerArgs, "--auth-callout-seeds-dir", seeds)
	}
	f.ns, err = natsserver.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	f.ns.Start()
	t.Cleanup(func() { f.ns.Shutdown(); f.ns.WaitForShutdown() })
	if !f.ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("transfer NATS did not start")
	}
	f.url = f.ns.ClientURL()
	adminOpts = append(adminOpts, nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		t.Errorf("transfer collector asynchronous failure: %v", err)
	}))
	f.nc, err = nats.Connect(f.url, adminOpts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.nc.Close)
	f.js, err = jetstream.New(f.nc)
	if err != nil {
		t.Fatal(err)
	}
	// Register this before the first launch, including startup-failure paths.
	// P29's original Close/census remain the final idempotent cleanup owners.
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	})
	// Observe actual requests/events before starting producers, including records
	// that session removal will later delete. No fixture response is synthesized.
	_, err = f.nc.Subscribe(proto.SubjectPrefix+".>", func(m *nats.Msg) {
		f.mu.Lock()
		f.wire = append(f.wire, transferWireRow{Subject: m.Subject, Data: bytes.Clone(m.Data)})
		f.mu.Unlock()
	})
	if err != nil || f.nc.FlushTimeout(time.Second) != nil {
		t.Fatal("transfer observer subscription was not installed", err)
	}
	f.broker = f.start(brokerBinary, filepath.Join(h.Root, "broker-home"), append(brokerArgs, "--nats-url", f.url)...)
	f.waitSubscription(".push.req")
	for _, p := range f.peers {
		args := []string{"agent", "--session", p.sid, "--nid", p.nid, "--nats-url", f.url,
			"--tunnel-addr", "", "--log-level", "info"}
		if agentBinary == buildTether(t) {
			// Current artifacts use S8's registered transaction route; S7 §9
			// still requires those actual readers for the same-PID transfer cut.
			// origin: S7 internal review TRANSFER-MAKETEST F6 / S8 plan §5
			registerTransactionDomain(t, upgradecli.AgentBootRole(p.agentHome, p.sid, p.nid, uint32(os.Geteuid())), p.agentBinary, "tether-agent@"+p.sid)
			args = append(args, "--upgrade-url-allow", "http://127.0.0.1:")
		}
		if callout {
			args = append(args, "--pin", realStackPIN)
		}
		p.agent = f.start(p.agentBinary, p.agentHome, args...)
		testharness.WaitNodeOnline(t, f.db, p.sid, p.nid, 15*time.Second)
		f.waitSubscription(proto.SubjCmdForwarded(p.sid, p.nid, "*"))
	}
	return f
}

func copyTransferArtifact(t *testing.T, root, name, src string) string {
	t.Helper()
	body, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, body, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func (f *transferStack) start(binary, home string, args ...string) *transferharness.Process {
	f.t.Helper()
	sha, err := transferharness.FileSHA(binary)
	if err != nil {
		f.t.Fatal(err)
	}
	// The test and supervisor own process lifetime. A provisioning/query wait
	// cannot kill an original invocation before its own product deadline.
	// origin: S7 internal review CONTINUATION-VICTIM F3
	p, err := f.h.Start(f.t.Context(), transferharness.Spec{Binary: binary, ArtifactSHA: sha, Args: args,
		Env: []string{"HOME=" + f.h.Root, "TETHER_HOME=" + home, "TETHER_SESSION=", "TETHER_NATS_URL=", "TETHER_DEV_NO_AUTH="}})
	if err != nil {
		f.t.Fatal(err)
	}
	return p
}

func (f *transferStack) waitSubscription(suffix string) {
	f.t.Helper()
	if !testharnessWait(10*time.Second, func() bool {
		conns, err := f.ns.Connz(&natsserver.ConnzOptions{Subscriptions: true})
		if err != nil {
			return false
		}
		for _, c := range conns.Conns {
			for _, sub := range c.Subs {
				if strings.HasSuffix(sub, suffix) {
					return true
				}
			}
		}
		return false
	}) {
		r := f.broker.Receipt()
		body, _ := os.ReadFile(r.StderrPath)
		f.t.Fatalf("binary subscription %s absent: %s", suffix, body)
	}
}

func (f *transferStack) command(p *transferPeer, args ...string) *transferharness.Process {
	return f.start(f.bin, p.home, append(args, "--home", p.home, "--nats-url", f.url)...)
}

func transferNative(ctx context.Context, t *testing.T, p *transferharness.Process) (transferharness.Receipt, string, string) {
	t.Helper()
	r, err := p.Wait(ctx)
	var native *exec.ExitError
	if err != nil && !errors.As(err, &native) {
		t.Fatalf("native receipt unavailable: %v (pid=%d)", err, r.PID)
	}
	out, outErr := os.ReadFile(r.StdoutPath)
	errout, errErr := os.ReadFile(r.StderrPath)
	if outErr != nil || errErr != nil || r.NativeRC < 0 || r.PID <= 0 {
		t.Fatalf("invalid native receipt: rc=%d stdout=%v stderr=%v", r.NativeRC, outErr, errErr)
	}
	t.Logf("native pid=%d start_ticks=%d boot_id=%s artifact=%s running=%s rc=%d wall=%s stdout=%q stderr=%q",
		r.PID, r.ProcessStart, r.BootID, r.ArtifactSHA, r.RunningSHA, r.NativeRC, time.Duration(r.WallNS), out, errout)
	return r, string(out), string(errout)
}

func (f *transferStack) rows() []transferWireRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]transferWireRow(nil), f.wire...)
}

func (f *transferStack) history(p *transferPeer) []transferharness.RawMessage {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	stream, err := f.js.Stream(ctx, jsstream.HistoryStreamName(p.sid))
	if err != nil {
		f.t.Fatal("real history stream unavailable", err)
	}
	rows, _, err := transferharness.ScanStream(ctx, stream, 0)
	if err != nil {
		f.t.Fatal("raw physical history scan", err)
	}
	for _, row := range rows {
		if row.Absent || strings.HasSuffix(row.Subject, ".audit.transfer") {
			f.t.Logf("physical history sid=%s seq=%d absent=%t subject=%s data=%q", p.sid, row.Sequence, row.Absent, row.Subject, row.Data)
		}
	}
	return rows
}

func transferRecords(rows []transferharness.RawMessage, tid string) ([]schema.AuditTransfer, error) {
	var records []schema.AuditTransfer
	for _, row := range rows {
		if row.Absent || !strings.HasSuffix(row.Subject, ".audit.transfer") {
			continue
		}
		var rec schema.AuditTransfer
		if err := json.Unmarshal(row.Data, &rec); err != nil {
			return nil, err
		}
		if rec.TransferID == tid {
			records = append(records, rec)
		}
	}
	return records, nil
}

// Match the full canonical terminal bytes, not the existence of some terminal.
// origin: hot-update S7 plan §5.2 / §7 / P33
func checkTransferHistory(rows []transferharness.RawMessage, status proto.TransferStatusResp) error {
	records, err := transferRecords(rows, status.Identity.TransferID)
	if err != nil {
		return err
	}
	if len(records) != 2 || records[0].Kind != "start" || records[1].Kind != "complete" || status.Terminal == nil {
		return errors.New("history must contain exactly one START and one complete TERMINAL")
	}
	terminal := status.Terminal
	if terminal.Kind != "complete" || terminal.OutcomeUnknown || terminal.HistoryStream != jsstream.HistoryStreamName(status.Identity.SID) {
		return errors.New("terminal receipt claims a different outcome or history stream")
	}
	for _, record := range records {
		canonical, err := schema.CanonicalTransferAudit(record)
		if err != nil || proto.Identity(canonical.Identity) != status.Identity {
			return errors.New("canonical history changed original transfer scope")
		}
	}
	for _, row := range rows {
		if row.Sequence != terminal.HistorySeq {
			continue
		}
		if row.Absent || hexSHA256ForTest(row.Data) != terminal.CanonicalDigest {
			return errors.New("terminal receipt does not match canonical history bytes")
		}
		if records[1].TerminalID != terminal.TerminalID || records[1].EntryNonce != status.Identity.EntryNonce ||
			records[1].ObjectVersion != status.Identity.ObjectVersion || records[1].OutcomeUnknown ||
			records[1].Bytes != terminal.Bytes || records[1].SHA256 != terminal.SHA256 {
			return errors.New("history changed the original outcome or identity")
		}
		return nil
	}
	return errors.New("terminal history sequence is missing")
}

func requireTransferIdentity(t *testing.T, before, after transferharness.Receipt, sha string) {
	t.Helper()
	if err := checkTransferProcessIdentity(before, after, sha); err != nil {
		t.Fatalf("%v: before=%+v after=%+v expected_sha=%s", err, before, after, sha)
	}
}

func checkTransferProcessIdentity(before, after transferharness.Receipt, sha string) error {
	if before.PID <= 0 || before.ProcessStart == 0 || before.BootID == "" || before.RunningSHA == "" || sha == "" || after.PID != before.PID ||
		after.ProcessStart != before.ProcessStart || after.BootID != before.BootID || after.RunningSHA != sha {
		return errors.New("original process changed")
	}
	return nil
}

// These adversarial rows exercise the readers used by the actual binary tests.
// They establish oracle sensitivity only; production cuts still require the
// live receipt above and cannot be signed by a synthetic positive sample.
// origin: hot-update S7 plan §7 / §12.2 / P33
func TestTransferBinaryContinuationEvidenceControls(t *testing.T) {
	start := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "start", Verb: "push", Tier: "b",
		Ts: time.Unix(100, 0).UTC(), Session: "session", Node: "node", ActorFp: "actor", TransferID: "tid",
		AgentInstance: "agent", EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "object",
		Size: 1024, SHA256: strings.Repeat("a", 64)}
	complete := start
	complete.Kind, complete.Bytes, complete.Ts = "complete", start.Size, start.Ts.Add(time.Second)
	complete.TerminalID = schema.TransferTerminalID(start.Session, start.TransferID, start.EntryNonce)
	canonical, err := schema.CanonicalTransferAudit(complete)
	if err != nil {
		t.Fatal(err)
	}
	marshal := func(rec schema.AuditTransfer) []byte {
		body, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	rows := []transferharness.RawMessage{
		{Sequence: 1, Subject: proto.SubjAuditTransfer(start.Session), Data: marshal(start)},
		{Sequence: 2, Subject: proto.SubjAuditTransfer(start.Session), Data: canonical.Bytes},
	}
	status := proto.TransferStatusResp{State: proto.StatusTerminal, Identity: proto.Identity(canonical.Identity), Terminal: &proto.TerminalReceipt{
		Kind: "complete", Bytes: complete.Bytes, SHA256: complete.SHA256, TerminalID: complete.TerminalID,
		HistoryStream: jsstream.HistoryStreamName(start.Session), HistorySeq: 2, CanonicalDigest: canonical.Digest}}
	if err := checkTransferHistory(rows, status); err != nil {
		t.Fatal("positive physical history control", err)
	}
	for _, name := range []string{"missing-start", "duplicate-terminal", "opposite-terminal", "foreign-start", "wrong-digest", "wrong-sequence", "wrong-stream", "unknown-outcome", "failed-receipt"} {
		t.Run(name, func(t *testing.T) {
			badRows := append([]transferharness.RawMessage(nil), rows...)
			badStatus, receipt := status, *status.Terminal
			badStatus.Terminal = &receipt
			switch name {
			case "missing-start":
				badRows = badRows[1:]
			case "duplicate-terminal":
				duplicate := badRows[1]
				duplicate.Sequence = 3
				badRows = append(badRows, duplicate)
			case "opposite-terminal":
				rec := complete
				rec.Kind = "failed"
				badRows[1].Data = marshal(rec)
			case "foreign-start":
				rec := start
				rec.ActorFp = "another-actor"
				badRows[0].Data = marshal(rec)
			case "wrong-digest":
				receipt.CanonicalDigest = strings.Repeat("b", 64)
			case "wrong-sequence":
				receipt.HistorySeq = 3
			case "wrong-stream":
				receipt.HistoryStream = "history-another-session"
			case "unknown-outcome":
				receipt.OutcomeUnknown = true
			case "failed-receipt":
				receipt.Kind = "failed"
			}
			if err := checkTransferHistory(badRows, badStatus); err == nil {
				t.Fatal("history oracle accepted", name)
			}
		})
	}
	p := &transferPeer{sid: "session", nid: "node", id: &cli.Identity{PublicKey: "actor"}}
	request := transferWireRow{Subject: proto.SubjCmdBy(p.sid, p.id.PublicKey, p.nid, "push"), Data: []byte(`{"transfer_id":"tid","call_id":"original"}`)}
	if _, _, err := transferInvocation([]transferWireRow{request, request}, p, "push"); err != nil {
		t.Fatal("same invocation replay refused", err)
	}
	for _, replacement := range []string{`{"transfer_id":"replacement","call_id":"original"}`, `{"transfer_id":"tid","call_id":"replacement"}`} {
		if _, _, err := transferInvocation([]transferWireRow{request, {Subject: request.Subject, Data: []byte(replacement)}}, p, "push"); err == nil {
			t.Fatal("replacement invocation accepted")
		}
	}
	before := transferharness.Receipt{PID: 123, ProcessStart: 456, BootID: "boot", RunningSHA: "original"}
	after := before
	after.RunningSHA = "next"
	if err := checkTransferProcessIdentity(before, after, "next"); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"pid", "start", "boot", "sha"} {
		bad := after
		switch field {
		case "pid":
			bad.PID++
		case "start":
			bad.ProcessStart++
		case "boot":
			bad.BootID = "other"
		case "sha":
			bad.RunningSHA = "original"
		}
		if err := checkTransferProcessIdentity(before, bad, "next"); err == nil {
			t.Fatal("process replacement accepted", field)
		}
	}
}

func sampleTransferIdentity(t *testing.T, p *transferharness.Process) transferharness.Receipt {
	t.Helper()
	r, err := p.SampleIdentity()
	if err != nil {
		t.Fatal("live process identity unavailable", err)
	}
	return r
}

func transferPayload(t *testing.T, root, name string, size int) (string, string) {
	t.Helper()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*31 + i/251) % 251)
	}
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, hexSHA256ForTest(data)
}

func (f *transferStack) transfer(p *transferPeer, binary, url, verb, src, dst string, extra ...string) *transferharness.Process {
	args := []string{verb, src, p.nid + ":" + dst}
	if verb == "pull" {
		args = []string{verb, p.nid + ":" + src, dst}
	}
	args = append(args, "--home", p.home, "--nats-url", url, "--timeout", "60s")
	return f.start(binary, p.home, append(args, extra...)...)
}

// The relay holds a real prepare reply, so even inline transfers own nonempty
// responsibility during both hot cuts. No worker is replaced by a test double.
// origin: hot-update S7 plan §7 / §10.2 / P33 (P32 owns the full boundary matrix)
// origin: S7 internal review MAKETEST-TRANSFER F6
// origin: S7 internal review CONTINUATION-VICTIM F1
func TestTransferBinaryContinuationKeepsOriginalInvocation(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	buildTether(t)
	next := buildUpgradeArtifact(t, "v0.0.0-transfer-next")
	original, err := os.ReadFile(buildTether(t))
	if err != nil {
		t.Fatal(err)
	}
	prev := packUpgradeArtifact(t, original)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	// Independent rounds share only immutable artifacts. Two lanes measured
	// above the frozen 420-second split threshold; join all three lanes before
	// measuring resources.
	// origin: S7 internal review CONTINUATION-VICTIM F1 / lead L17
	lanes := make(chan struct{}, 3)
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprint(round), func(t *testing.T) {
				t.Parallel()
				lanes <- struct{}{}
				defer func() { <-lanes }()
				exerciseTransferBinaryContinuation(t, prev, next)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "transfer continuation", g)
	testharness.AssertNoFDLeak(t, "transfer continuation", fd, 0)
}

func exerciseTransferBinaryContinuation(t *testing.T, prev, next upgradeArtifact) {
	for _, tier := range []string{"a", "b"} {
		for _, verb := range []string{"push", "pull"} {
			t.Run(tier+"-"+verb, func(t *testing.T) {
				f := newTransferStack(t, buildTether(t), buildTether(t), false, "continuation")
				p := f.peers[0]
				f.requireCapable(p)
				srv := serveUpgradeArtifacts(t, next, prev)
				candidate := f.agentTransactionCandidate(t, p, next)
				size := 64 << 10
				if tier == "b" {
					size = 9 << 20
				}
				src, digest := transferPayload(t, f.h.Root, "source", size)
				dst := filepath.Join(f.h.Root, "destination")
				if err := os.WriteFile(dst, []byte("old-complete-target"), 0o600); err != nil {
					t.Fatal(err)
				}
				release := make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				relay, url := startNATSReplyFence(t, f.url, func(subject string, _ []byte) bool {
					return subject == proto.SubjCmdBy(p.sid, p.id.PublicKey, p.nid, verb)
				}, release, false)
				baseWire := len(f.rows())
				ctl := f.transfer(p, f.bin, url, verb, src, dst, "--force")
				before := sampleTransferIdentity(t, ctl)
				if !testharnessWait(5*time.Second, func() bool { return len(relay.receipts()) > 0 }) {
					t.Fatal("prepare reply was not captured; no live continuation window")
				}
				loss := relay.requireLoss(t)
				var req struct {
					CallID       string                      `json:"call_id"`
					TransferID   string                      `json:"transfer_id"`
					Continuation *proto.TransferContinuation `json:"continuation"`
				}
				if json.Unmarshal(loss.Request, &req) != nil || req.CallID == "" || req.Continuation == nil {
					t.Fatal("prepare has no capable invocation identity")
				}
				t.Logf("original invocation pid=%d transfer_id=%s call_id=%s", before.PID, req.TransferID, req.CallID)
				live := f.status(p, req.TransferID)
				if live.State != proto.StatusLive || live.Identity != req.Continuation.Identity {
					t.Fatalf("prepare did not retain original live responsibility: %+v", live)
				}
				if (tier == "b") != (live.Identity.ObjectVersion != "") {
					t.Fatalf("requested tier %s was not exercised: object_version=%q", tier, live.Identity.ObjectVersion)
				}
				agentBefore := sampleTransferIdentity(t, p.agent)
				var cut transferAgentCut
				for i, artifact := range []upgradeArtifact{next, prev} {
					// S7 §8.2/§9 requires the current product's real cut. S8 §5
					// replaced the old dispatch with prepare → stage → start → wait;
					// preserve the original transfer and every identity/budget oracle.
					// origin: S7 internal review TRANSFER-MAKETEST F6
					var err error
					if i == 0 {
						cut = f.prepareAgentTransactionCut(t, p, srv.URL+"/0.tar.gz", next, agentBefore.PID, candidate)
						err = cut.start(t)
					} else {
						err = cut.rollback(t)
					}
					if err != nil {
						t.Fatalf("production transfer transaction cut: %s%v", transactionDependency(err), err)
					}
					requireTransferIdentity(t, agentBefore, sampleTransferIdentity(t, p.agent), artifact.binSHA)
					requireTransferIdentity(t, before, sampleTransferIdentity(t, ctl), before.RunningSHA)
					current := f.status(p, req.TransferID)
					if current.Identity != live.Identity || current.Budget.StartedAt != live.Budget.StartedAt || current.Budget.EffectiveBudgetNS != live.Budget.EffectiveBudgetNS {
						t.Fatal("cut replaced original transfer identity or budget")
					}
				}
				unblock()
				r, stdout, stderr := transferNative(f.ctx, t, ctl)
				if r.NativeRC != 0 || strings.Count(stdout, "kind=complete") != 1 || !strings.Contains(stdout, "transfer_id="+req.TransferID) {
					t.Fatalf("original ctl did not complete exactly once: rc=%d %s %s", r.NativeRC, stdout, stderr)
				}
				if got := upgradeFileSHA(t, dst); got != digest {
					t.Fatalf("destination digest=%s want=%s", got, digest)
				}
				tid, callID, err := transferInvocation(f.rows()[baseWire:], p, verb)
				if err != nil || tid != req.TransferID || callID != req.CallID {
					t.Fatalf("reconciliation replaced original invocation: id=%s call=%s err=%v", tid, callID, err)
				}
				terminal := f.status(p, req.TransferID)
				if terminal.State != proto.StatusTerminal || terminal.Identity != live.Identity || terminal.Terminal == nil || terminal.Terminal.Bytes != int64(size) || terminal.Terminal.SHA256 != digest {
					t.Fatalf("original outcome was not retained: %+v", terminal)
				}
				if err := checkTransferHistory(f.history(p), terminal); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
