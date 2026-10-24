//go:build linux

package cli_e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/transferharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

type transferVictim struct {
	peer            *transferPeer
	ctl             *transferharness.Process
	id, dst, oldSHA string
	release         func()
}

// Independent SIDs imply independent per-session buckets. Check actual fixture
// assignments before launch so a reused victim cannot quietly kill a survivor.
// origin: hot-update S7 plan §10.3 / T45
func checkTransferCohorts(peers []*transferPeer) error {
	if len(peers) != 6 {
		return errors.New("four survivors and two victims are required")
	}
	seen := make(map[string]bool)
	for _, p := range peers {
		bucket := proto.XferBucketName(p.sid)
		if p.sid == "" || seen[bucket] {
			return errors.New("victim and survivor buckets are not independent")
		}
		seen[bucket] = true
	}
	return nil
}

func (f *transferStack) heldTransfer(p *transferPeer, tier, verb, operation string) transferVictim {
	f.t.Helper()
	size := 32 << 10
	if tier == "b" {
		size = 9 << 20
	}
	src, _ := transferPayload(f.t, f.h.Root, p.sid+"-source", size)
	dst, oldSHA := transferPayload(f.t, f.h.Root, p.sid+"-destination", 4096)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	f.t.Cleanup(unblock)
	relay, url := startNATSReplyFence(f.t, f.url, func(subject string, _ []byte) bool {
		if operation == "prepare" {
			return subject == proto.SubjCmdBy(p.sid, p.id.PublicKey, p.nid, verb)
		}
		if operation == "chunk" {
			return strings.HasPrefix(subject, "$O."+proto.XferBucketName(p.sid)+".C.")
		}
		return strings.Contains(subject, ".s."+p.sid+".") && strings.HasSuffix(subject, "."+operation+".req")
	}, release, false)
	base := len(f.rows())
	ctl := f.transfer(p, f.bin, url, verb, src, dst, "--force")
	if !testharnessWait(15*time.Second, func() bool { return len(relay.receipts()) > 0 }) {
		r := ctl.Receipt()
		errout, _ := os.ReadFile(r.StderrPath)
		f.t.Fatalf("no real %s reply at victim window: rc=%d %s", operation, r.NativeRC, errout)
	}
	loss := relay.requireLoss(f.t)
	var request struct {
		TransferID   string                      `json:"transfer_id"`
		Identity     proto.Identity              `json:"identity"`
		Continuation *proto.TransferContinuation `json:"continuation"`
	}
	var tid string
	if operation == "chunk" {
		var err error
		tid, _, err = transferInvocation(f.rows()[base:], p, verb)
		if err != nil {
			f.t.Fatal(err)
		}
	} else {
		if err := json.Unmarshal(loss.Request, &request); err != nil {
			f.t.Fatal(err)
		}
		tid = request.TransferID
		if operation != "prepare" {
			tid = request.Identity.TransferID
		}
	}
	if tid == "" {
		f.t.Fatal("held victim has no original transfer ID")
	}
	status := f.status(p, tid)
	if status.State != proto.StatusLive || status.Identity.EntryNonce == "" || status.Terminal != nil {
		f.t.Fatalf("victim has no live responsibility: %+v", status)
	}
	if operation == "reserve" && status.Phase != proto.PhaseCommitReserved {
		f.t.Fatalf("lost grant was not durably reserved: %+v", status)
	}
	if operation != "reserve" && (status.Phase == proto.PhaseCommitReserved || status.Phase == proto.PhaseDecided ||
		(operation == "prepare" && request.Continuation == nil)) {
		f.t.Fatal("cancel/revoke victim passed the pre-reserve boundary")
	}
	if operation == "chunk" {
		// A real accepted chunk with its reply withheld proves the victim is in
		// the data window. Merely preparing a tier-B sender would be vacuous.
		var ack struct {
			Stream   string `json:"stream"`
			Sequence uint64 `json:"seq"`
		}
		if json.Unmarshal(loss.Reply, &ack) != nil || ack.Sequence == 0 || ack.Stream != "OBJ_"+proto.XferBucketName(p.sid) ||
			status.Identity.ObjectVersion == "" || loss.Subject != "$O."+proto.XferBucketName(p.sid)+".C."+status.Identity.ObjectVersion {
			f.t.Fatal("victim has no original object chunk receipt")
		}
		ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
		defer cancel()
		stream, err := f.js.Stream(ctx, ack.Stream)
		if err != nil {
			f.t.Fatal(err)
		}
		chunk, err := stream.GetMsg(ctx, ack.Sequence)
		if err != nil || chunk.Subject != loss.Subject || len(chunk.Data) == 0 || !bytes.Equal(chunk.Data, loss.Request) {
			f.t.Fatal("victim data window lacks physical accepted bytes", err)
		}
	}
	if upgradeFileSHA(f.t, dst) != oldSHA {
		f.t.Fatal("victim target changed before release")
	}
	return transferVictim{peer: p, ctl: ctl, id: tid, dst: dst, oldSHA: oldSHA, release: unblock}
}

// Ctrl-C and real session rm operate on two different sessions, while four
// genuine push/pull commands retain their original processes and histories.
// Small hermetic files bound this unit; drill63 owns the 66MB/1GiB live profile.
// origin: hot-update S7 plan §10.3 / T45
// origin: S7 internal review CONTINUATION-VICTIM F1
func TestTransferRevokeAndCancelVictimsAreIndependent(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	buildTether(t)
	next := buildUpgradeArtifact(t, "v0.0.0-transfer-revoke")
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	// Join isolated lanes before the leak census, keeping all five rounds and
	// their original byte/command deadlines inside the 600-second unit.
	// origin: S7 internal review CONTINUATION-VICTIM F2 / lead L17
	lanes := make(chan struct{}, 3)
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprint(round), func(t *testing.T) {
				t.Parallel()
				lanes <- struct{}{}
				defer func() { <-lanes }()
				exerciseTransferVictims(t, next)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "transfer revoke and cancel", g)
	testharness.AssertNoFDLeak(t, "transfer revoke and cancel", fd, 0)
}

func exerciseTransferVictims(t *testing.T, next upgradeArtifact) {
	for _, tier := range []string{"a", "b"} {
		t.Run(tier, func(t *testing.T) {
			f := newTransferStack(t, buildTether(t), buildTether(t), true,
				"small-push", "small-pull", "large-push", "large-pull", "cancel-victim", "revoke-victim")
			if err := checkTransferCohorts(f.peers); err != nil {
				t.Fatal(err)
			}
			for _, p := range f.peers {
				f.requireCapable(p)
			}
			// Source generation is fixture setup, not transfer downtime. Freeze
			// all survivor bytes before starting either 30-second inline victim.
			// origin: S7 internal review CONTINUATION-VICTIM F2 / S7 plan §10.2
			type survivor struct {
				peer                        *transferPeer
				ctl                         *transferharness.Process
				before                      transferharness.Receipt
				verb, src, tid, dst, digest string
			}
			var survivors []survivor
			for i, p := range f.peers[:4] {
				verb := "push"
				if i%2 != 0 {
					verb = "pull"
				}
				size := 9 << 20
				if i >= 2 {
					size = 12 << 20
				}
				src, digest := transferPayload(t, f.h.Root, p.sid+"-source", size)
				survivors = append(survivors, survivor{peer: p, verb: verb, src: src,
					dst: filepath.Join(f.h.Root, p.sid+"-destination"), digest: digest})
			}
			// Open the consumer before deletion, when history is still readable.
			history := captureTransferHistory(t, f, f.peers[5])
			window := "prepare"
			if tier == "b" {
				window = "chunk"
			}
			srv := serveUpgradeArtifacts(t, next)
			candidates := make([]string, 2)
			for i, p := range f.peers[4:] {
				candidates[i] = f.agentTransactionCandidate(t, p, next)
			}
			beginTransferCohort(t, f)
			cancelVictim := f.heldTransfer(f.peers[4], tier, "push", window)
			revokeVictim := f.heldTransfer(f.peers[5], tier, "push", window)
			runTransferVictimCuts(t, []transferVictim{cancelVictim, revokeVictim}, func(t *testing.T, victim transferVictim) {
				before := sampleTransferIdentity(t, victim.peer.agent)
				ctlBefore := sampleTransferIdentity(t, victim.ctl)
				// S7 §8.2/§9/T45 keeps the original victims across the current
				// product cut; S8 §5 now requires the explicit transaction route.
				// origin: S7 internal review TRANSFER-MAKETEST F6
				candidate := candidates[0]
				if victim.peer == revokeVictim.peer {
					candidate = candidates[1]
				}
				cut := f.prepareAgentTransactionCut(t, victim.peer, srv.URL+"/0.tar.gz", next, before.PID, candidate)
				if err := cut.start(t); err != nil {
					t.Fatalf("victim transaction cut refused: %s%v", transactionDependency(err), err)
				}
				requireTransferIdentity(t, before, sampleTransferIdentity(t, victim.peer.agent), next.binSHA)
				requireTransferIdentity(t, ctlBefore, sampleTransferIdentity(t, victim.ctl), ctlBefore.RunningSHA)
			})
			for i := range survivors {
				s := &survivors[i]
				p := s.peer
				base := len(f.rows())
				s.ctl = f.transfer(p, f.bin, f.url, s.verb, s.src, s.dst, "--rate-limit", "1048576")
				s.before = sampleTransferIdentity(t, s.ctl)
				if !testharnessWait(5*time.Second, func() bool {
					var err error
					s.tid, _, err = transferInvocation(f.rows()[base:], p, s.verb)
					return err == nil
				}) {
					t.Fatal("survivor did not prepare")
				}
				if f.status(p, s.tid).State != proto.StatusLive {
					t.Fatal("survivor finished before victim action")
				}
			}
			// Signal the original ctl; killing the process is not a cancellation oracle.
			process, err := os.FindProcess(cancelVictim.ctl.Receipt().PID)
			if err != nil {
				t.Fatal(err)
			}
			err = process.Signal(os.Interrupt)
			_ = process.Release()
			if err != nil {
				t.Fatal(err)
			}
			cancelVictim.release()
			rm, _, stderr := transferNative(f.ctx, t, f.command(revokeVictim.peer, "session", "rm", revokeVictim.peer.sid))
			if rm.NativeRC != 0 && rm.NativeRC != 75 {
				t.Fatalf("session rm rc=%d %s", rm.NativeRC, stderr)
			}
			// Keep the prepare reply fenced until the actual removal request has
			// revoked authority. Releasing it first races the receiver's reserve.
			revokeVictim.release()
			for _, s := range survivors {
				requireTransferIdentity(t, s.before, sampleTransferIdentity(t, s.ctl), s.before.RunningSHA)
			}
			for _, victim := range []transferVictim{cancelVictim, revokeVictim} {
				r, stdout, stderr := transferNative(f.ctx, t, victim.ctl)
				if r.NativeRC == 0 || strings.Contains(stdout, "kind=complete") {
					t.Fatalf("victim reported success: %s %s", stdout, stderr)
				}
				if upgradeFileSHA(t, victim.dst) != victim.oldSHA {
					t.Fatal("pre-reserve victim changed destination")
				}
			}
			requireFailedTransferHistory(t, f.history(cancelVictim.peer), cancelVictim.id, false)
			requireFailedTransferHistory(t, history(), revokeVictim.id, false)
			requireTransferConnectRevoked(t, f, revokeVictim.peer)
			for _, s := range survivors {
				r, stdout, stderr := transferNative(f.ctx, t, s.ctl)
				if err := checkTransferNativeResult(r.NativeRC, stdout, 0); err != nil {
					t.Fatalf("survivor was interrupted: %v %s", err, stderr)
				}
				if upgradeFileSHA(t, s.dst) != s.digest {
					t.Fatal("survivor destination is incomplete")
				}
				if err := checkTransferHistory(f.history(s.peer), f.status(s.peer, s.tid)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// Losing a real pull receiver's grant reply leaves a settlement responsibility.
// Repeated ordinary rm must retain it; only the explicit LQ1 flag can abandon.
// origin: hot-update S7 plan §6.4 / lead LQ1 / T45
func TestTransferSessionRemovalRequiresExplicitAbandon(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	buildTether(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferExplicitAbandon(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer explicit abandonment", g)
	testharness.AssertNoFDLeak(t, "transfer explicit abandonment", fd, 0)
}

func exerciseTransferExplicitAbandon(t *testing.T) {
	f := newTransferStack(t, buildTether(t), buildTether(t), true, "abandon")
	p := f.peers[0]
	f.requireCapable(p)
	history := captureTransferHistory(t, f, p)
	victim := f.heldTransfer(p, "b", "pull", "reserve")
	before := f.history(p)
	records, err := transferRecords(before, victim.id)
	if err != nil || len(records) != 1 || records[0].Kind != "start" {
		t.Fatal("unsettled transfer has no START history")
	}
	for attempt := 0; attempt < 2; attempt++ {
		r, stdout, stderr := transferNative(f.ctx, t, f.command(p, "session", "rm", p.sid))
		if r.NativeRC != 75 || !strings.Contains(stderr, victim.id) || !strings.Contains(stderr, "pending") || strings.Contains(stdout, "removed") {
			t.Fatalf("no-flag settlement barrier lost pending ID/reason: rc=%d %s %s", r.NativeRC, stdout, stderr)
		}
		var state string
		if err := f.db.QueryRowContext(f.ctx, "SELECT state FROM sessions WHERE sid=?", p.sid).Scan(&state); err != nil || state != "DELETING" {
			t.Fatalf("unsettled session was deleted: state=%s err=%v", state, err)
		}
		if _, err := f.js.ObjectStore(f.ctx, proto.XferBucketName(p.sid)); err != nil {
			t.Fatal("unsettled OBJ was deleted", err)
		}
		after := f.history(p)
		for _, original := range before {
			found := false
			for _, row := range after {
				if row.Sequence == original.Sequence && row.Absent == original.Absent && bytes.Equal(row.Data, original.Data) {
					found = true
				}
			}
			if !found {
				t.Fatal("unsettled history was deleted or rewritten")
			}
		}
		if upgradeFileSHA(t, victim.dst) != victim.oldSHA {
			t.Fatal("lost grant permitted a target commit")
		}
	}
	r, stdout, stderr := transferNative(f.ctx, t, f.command(p, "session", "rm", p.sid, "--abandon-unsettled-transfers"))
	if r.NativeRC != 0 {
		t.Fatalf("explicit abandon did not finish removal: rc=%d %s %s", r.NativeRC, stdout, stderr)
	}
	requireFailedTransferHistory(t, history(), victim.id, true)
	var count int
	if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM sessions WHERE sid=?", p.sid).Scan(&count); err != nil || count != 0 {
		t.Fatalf("explicit abandonment did not delete SQL last: count=%d err=%v", count, err)
	}
	if _, err := f.js.Stream(f.ctx, jsstream.HistoryStreamName(p.sid)); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatal("history not deleted after settlement", err)
	}
	if _, err := f.js.Stream(f.ctx, "OBJ_"+proto.XferBucketName(p.sid)); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatal("OBJ not deleted after settlement", err)
	}
	victim.release()
	r, stdout, stderr = transferNative(f.ctx, t, victim.ctl)
	if r.NativeRC == 0 || strings.Contains(stdout, "kind=complete") {
		t.Fatalf("abandoned original ctl reported success: %s %s", stdout, stderr)
	}
	if upgradeFileSHA(t, victim.dst) != victim.oldSHA {
		t.Fatal("late grant committed after abandonment")
	}
}

// A JS consumer carries physical stream sequence metadata across deletion.
// Capturing ordinary ev messages alone would mistake a retried publish for a
// second physical history row, or lose the terminal when the stream disappears.
// origin: hot-update S7 plan §6.4 / T45
func captureTransferHistory(t *testing.T, f *transferStack, p *transferPeer) func() []transferharness.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	stream, err := f.js.Stream(ctx, jsstream.HistoryStreamName(p.sid))
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{FilterSubjects: []string{proto.SubjAuditTransfer(p.sid)}})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var rows []transferharness.RawMessage
	var captureErr error
	reader, err := consumer.Consume(func(msg jetstream.Msg) {
		meta, err := msg.Metadata()
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			captureErr = err
			return
		}
		row := transferharness.RawMessage{Sequence: meta.Sequence.Stream, Subject: msg.Subject(), Data: bytes.Clone(msg.Data())}
		for _, prior := range rows {
			if prior.Sequence == row.Sequence {
				if !bytes.Equal(prior.Data, row.Data) {
					captureErr = errors.New("physical history sequence changed")
				}
				return // consumer replay of the same physical row is not another publish
			}
		}
		rows = append(rows, row)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		reader.Stop()
		select {
		case <-reader.Closed():
		case <-time.After(5 * time.Second):
			t.Error("physical history consumer did not join")
		}
	})
	return func() []transferharness.RawMessage {
		// Await the consumer's already-published terminal, never recreate a stream.
		if !testharnessWait(5*time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			for _, row := range rows {
				var rec schema.AuditTransfer
				if json.Unmarshal(row.Data, &rec) == nil && (rec.Kind == "complete" || rec.Kind == "failed") {
					return true
				}
			}
			return captureErr != nil
		}) {
			t.Fatal("terminal physical history was not captured before deletion")
		}
		mu.Lock()
		defer mu.Unlock()
		if captureErr != nil {
			t.Fatal(captureErr)
		}
		out := append([]transferharness.RawMessage(nil), rows...)
		sort.Slice(out, func(i, j int) bool { return out[i].Sequence < out[j].Sequence })
		for _, row := range out {
			t.Logf("deletion history sid=%s seq=%d subject=%s data=%q", p.sid, row.Sequence, row.Subject, row.Data)
		}
		return out
	}
}

func requireFailedTransferHistory(t *testing.T, rows []transferharness.RawMessage, tid string, abandoned bool) {
	t.Helper()
	records, err := transferRecords(rows, tid)
	if err != nil || len(records) != 2 || records[0].Kind != "start" || records[1].Kind != "failed" {
		t.Fatalf("victim history must be one START and one failed TERMINAL: %+v %v", records, err)
	}
	rec := records[1]
	if _, err := schema.CanonicalTransferAudit(rec); err != nil {
		t.Fatal("victim canonical terminal", err)
	}
	if rec.OutcomeUnknown != abandoned || (rec.Code == "settlement_abandoned") != abandoned {
		t.Fatalf("abandonment outcome was fabricated or omitted: %+v", rec)
	}
}

func requireTransferConnectRevoked(t *testing.T, f *transferStack, p *transferPeer) {
	t.Helper()
	key, err := nkeys.FromSeed(p.id.Seed)
	if err != nil {
		t.Fatal(err)
	}
	defer key.Wipe()
	nc, err := nats.Connect(f.url, nats.Nkey(p.id.PublicKey, key.Sign), nats.Name(cli.CtlNameForSession(p.sid)), nats.Timeout(2*time.Second), nats.NoReconnect())
	if nc != nil {
		nc.Close()
	}
	if !errors.Is(err, nats.ErrAuthorization) {
		t.Fatalf("ordinary revoked CONNECT not denied by callout: %v", err)
	}
}

// The same predicate gates the live T45 fixture. Reuse each victim against
// every survivor as well as against the other victim; all must turn red.
// origin: hot-update S7 plan §12.2 / T45 mandatory victim-bucket-reuse control
func TestTransferVictimBucketIsolationControls(t *testing.T) {
	peers := make([]*transferPeer, 6)
	for i := range peers {
		peers[i] = &transferPeer{sid: fmt.Sprintf("session-%d", i)}
	}
	if err := checkTransferCohorts(peers); err != nil {
		t.Fatal(err)
	}
	for _, victim := range []int{4, 5} {
		for other := 0; other < 6; other++ {
			if victim == other {
				continue
			}
			mutant := append([]*transferPeer(nil), peers...)
			mutant[victim] = &transferPeer{sid: peers[other].sid}
			if err := checkTransferCohorts(mutant); err == nil {
				t.Fatalf("victim %d reused bucket %d", victim, other)
			}
		}
	}
	if err := checkTransferCohorts(peers[:5]); err == nil {
		t.Fatal("missing independent victim accepted")
	}
}
