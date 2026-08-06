package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/serveconf"
	"github.com/LinZiyang666/tether/internal/xferaudit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// This file consolidates the G67 internal review's CONFIRMED findings into permanent pins. The
// reviewers demonstrated each of these with a throwaway test; the main process adopted the FINDING
// (not always the reviewer's assertion — see budget below) and owns the pin from here.

// blockingJS makes a create attempt cost its FULL timeout, which is the regime #67 exists for:
// nats-server returns without a reply at all when it holds no JetStream leadership, so the client
// learns nothing until its own deadline. countingJS returns instantly and therefore leaves the entire
// wall-clock budget dead under the suite — internal review B2.
type blockingJS struct {
	jetstream.JetStream
	mu       sync.Mutex
	budgets  []time.Duration // remaining budget observed at the start of each attempt
	maxStore int64
}

func (b *blockingJS) AccountInfo(context.Context) (*jetstream.AccountInfo, error) {
	return &jetstream.AccountInfo{Tier: jetstream.Tier{Limits: jetstream.AccountLimits{MaxStore: b.maxStore}}}, nil
}

func (b *blockingJS) CreateObjectStore(ctx context.Context, _ jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	if dl, ok := ctx.Deadline(); ok {
		b.mu.Lock()
		b.budgets = append(b.budgets, time.Until(dl))
		b.mu.Unlock()
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *blockingJS) observed() []time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]time.Duration, len(b.budgets))
	copy(out, b.budgets)
	return out
}

func newProvisionBroker(js jetstream.JetStream) *Broker {
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{Logger: silentLogger(), Now: time.Now}
	b.js = js
	return b
}

// TestG67EveryStartedAttemptGetsAFullBudget is the ADOPTED form of the review's "attempt budget
// starvation" finding. The reviewer's own assertion ("there must be 3 attempts") encoded the OLD
// behaviour and is NOT what was wrong; the defect was that xferProvisionMinSlack was SMALLER than one
// attempt and was only checked before the backoff sleep, so a third attempt could start with 1.68s of
// an attempt that needs 2.5s. Its DeadlineExceeded was then caused by OUR wall clock, and the operator
// was told "JetStream did not accept the request" about a request JetStream was never given time to
// answer — a fresh instance of exactly the dishonesty #67 exists to remove.
//
// The invariant is therefore per-attempt, not per-count: whatever the budget allows, every attempt
// that IS started gets a full one.
func TestG67EveryStartedAttemptGetsAFullBudget(t *testing.T) {
	js := &blockingJS{}
	b := newProvisionBroker(js)

	start := time.Now()
	_, _, perr := b.provisionXferBucket(context.Background(), "sess", 0)
	elapsed := time.Since(start)

	if perr == nil || !perr.Transient {
		t.Fatalf("a stalled create must be classified transient, got %+v", perr)
	}
	budgets := js.observed()
	if len(budgets) == 0 {
		t.Fatal("no attempt was made")
	}
	for i, got := range budgets {
		if got < xferCreateAttemptTO-100*time.Millisecond {
			t.Fatalf("attempt %d started with only %v of budget (a full attempt is %v) — an attempt "+
				"that cannot run to completion fails for OUR wall-clock reason and is then reported as "+
				"a JetStream verdict. budgets=%v", i+1, got, xferCreateAttemptTO, budgets)
		}
	}
	if len(budgets) > xferProvisionMaxTries {
		t.Fatalf("more attempts (%d) than the cap (%d)", len(budgets), xferProvisionMaxTries)
	}
	if elapsed > xferSizingTimeout+xferProvisionBudget+time.Second {
		t.Fatalf("provisioning ran %v, past its wall-clock ceiling", elapsed)
	}
	if perr.Attempts != len(budgets) {
		t.Fatalf("the refusal quotes %d attempts to the operator but %d were made", perr.Attempts, len(budgets))
	}
}

// TestG67ProvisionConstantsAreSelfConsistent is plan T10, promised and never written (review B2).
// These relations are the whole safety argument for blocking inside a handler that nats.go delivers
// SERIALLY (.push.req is a plain Subscribe, so every push on this broker queues behind this call).
func TestG67ProvisionConstantsAreSelfConsistent(t *testing.T) {
	if xferProvisionMinSlack < xferCreateAttemptTO {
		t.Fatalf("minSlack (%v) must be >= one full attempt (%v), or an attempt can start with a sliver "+
			"of budget and fail for a non-JetStream reason", xferProvisionMinSlack, xferCreateAttemptTO)
	}
	worst := xferSizingTimeout + xferProvisionBudget
	if worst >= transferTimeoutTierA {
		t.Fatalf("worst-case in-handler time %v must stay well under transferTimeoutTierA (%v): this "+
			"blocks the SERIALISED .push.req callback, so it is head-of-line latency for every other "+
			"transfer on this broker", worst, transferTimeoutTierA)
	}
	if xferProvisionBudget < xferCreateAttemptTO {
		t.Fatal("the wall-clock budget cannot fit even one attempt")
	}
	if xferSizingTimeout >= xferProvisionBudget {
		t.Fatal("the best-effort sizing probe must not dominate the load-bearing create budget")
	}
}

// TestG67HeadOfLineLatencyIsBounded pins the review's head-of-line amplification measurement. It does
// not assert that serialisation is absent (it is inherent: .push.req is a plain nc.Subscribe and the
// handler is the nats.go callback) — it asserts the AMPLIFIER stays bounded, which is what makes
// widening the budget a decision someone has to justify rather than a one-constant edit.
func TestG67HeadOfLineLatencyIsBounded(t *testing.T) {
	js := &blockingJS{}
	b := newProvisionBroker(js)

	const concurrent = 2
	var wg sync.WaitGroup
	lat := make([]time.Duration, concurrent)
	var mu sync.Mutex // serialise exactly as the nats.go callback does
	for i := range concurrent {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := time.Now()
			mu.Lock()
			_, _, _ = b.provisionXferBucket(context.Background(), "sess", 0)
			mu.Unlock()
			lat[i] = time.Since(s)
		}(i)
	}
	wg.Wait()

	worstOne := xferSizingTimeout + xferProvisionBudget
	for i, d := range lat {
		if d > time.Duration(concurrent)*worstOne+2*time.Second {
			t.Fatalf("queued prepare %d waited %v; with %d serialised prepares the ceiling is %v. "+
				"Raising xferProvisionBudget multiplies by the queue depth — that is the review's R1 risk",
				i, d, concurrent, time.Duration(concurrent)*worstOne)
		}
	}
	t.Logf("serialised prepare latencies: %v (single-call ceiling %v)", lat, worstOne)
}

// TestG67AbortedProvisionIsRetriableAndNotAJetStreamVerdict adopts two review findings at once:
// a parent that ended by DEADLINE (not Canceled) used to fall through to the PERMANENT branch, and a
// broker that is shutting down — the most retriable condition there is — was reported under
// bucket_create_failed, which cmd/tether leaves unmapped => exit 70 ("a tether fault you should
// report"). Both told the operator a JetStream verdict about a JetStream that was never asked.
func TestG67AbortedProvisionIsRetriableAndNotAJetStreamVerdict(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mkParent func() (context.Context, context.CancelFunc)
		wantWord string
	}{
		{"shutdown (parent cancelled)", func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			go func() { time.Sleep(300 * time.Millisecond); cancel() }()
			return ctx, cancel
		}, "shutting down"},
		{"parent carried a deadline", func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 300*time.Millisecond)
		}, "run context expired"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newProvisionBroker(&blockingJS{})
			parent, cancel := tc.mkParent()
			defer cancel()

			_, _, perr := b.provisionXferBucket(parent, "sess", 0)
			if perr == nil {
				t.Fatal("want a refusal")
			}
			if !perr.Aborted {
				t.Fatalf("a parent whose lifetime ended must be reported as ABORTED, not as a JetStream "+
					"verdict; got %+v", perr)
			}
			code, text := xferProvisionRefusal(perr)
			if code != codeXferBrokerRestarting {
				t.Fatalf("code=%q — a restarting broker must carry a RETRIABLE code, not the unclassified "+
					"bucket_create_failed (exit 70 = 'a tether fault you should report')", code)
			}
			if !strings.Contains(text, tc.wantWord) {
				t.Fatalf("the text must name the real cause %q; got %q", tc.wantWord, text)
			}
			if !strings.Contains(text, "not a JetStream verdict") {
				t.Fatalf("the text must say this is NOT a JetStream verdict; got %q", text)
			}
		})
	}
}

// TestG67SizingTimeoutCannotMoveTheAdmissionDecision is the REJECTION of the review's
// "the 1.5s sizing cut silently disables the disk-aware ceiling" finding, made durable.
//
// The finding was refuted on this fact, and the fact HOLDS — it was re-verified against the live
// broker during the smalldisk increment: tether renders neither an account JWT limit nor
// max_file_store, so AccountInfo reports MaxStore = -1 (UNLIMITED) and jsStoreCeiling falls through
// to the statfs estimate, a local syscall no network deadline can starve.
//
//	live AccountInfo, racknerd:  "storage":49485651  "reserved_storage":10737418240  "max_storage":-1
//
// A DIGRESSION WORTH KEEPING, because it cost this increment a wrong turn. /jsz reports
// config.max_storage = 10.33 GiB on that same broker, which looks like a contradiction and is not:
// that figure is the SERVER's limit (nats derives it from diskAvailable when max_file_store is
// unset), while AccountInfo.Limits.MaxStore is the ACCOUNT's. The smalldisk increment briefly
// "corrected" this comment on the strength of the /jsz number, and in doing so built a convergence
// trigger gated on a finite account limit — i.e. dead code on every real broker. Three independent
// reviewers caught it; `nats account info` ("Storage: 47 MiB of Unlimited") settled it. If you are
// about to conclude that MaxStore is finite here, check WHICH MaxStore you are reading.
//
// Rejecting a finding is only safe if the reason is pinned: if someone ever makes the ACCOUNT limit
// finite (or makes the ceiling depend on a network round trip again), this test goes red and the
// rejection has to be re-argued rather than silently inherited. The fixture below therefore keeps
// MaxStore = -1 deliberately — it is reproducing production, not asserting its own premise.

func TestG67SizingTimeoutCannotMoveTheAdmissionDecision(t *testing.T) {
	// PRODUCTION SHAPE: the ACCOUNT limit is unlimited, so the ceiling comes from statfs.
	slow := &slowAccountInfoJS{delay: xferSizingTimeout * 3, maxStore: -1}
	b := newProvisionBroker(slow)
	b.cfg.StoreDir = t.TempDir()

	fast := b.jsStoreCeiling(context.Background())

	sctx, cancel := context.WithTimeout(context.Background(), xferSizingTimeout)
	starved := b.jsStoreCeiling(sctx)
	cancel()

	// Both calls must yield a usable statfs-derived ceiling. A zero would mean the fallback path
	// changed and the rejected finding may have become real.
	if fast <= 0 || starved <= 0 {
		t.Fatalf("expected a statfs-derived ceiling from BOTH calls, got fast=%d starved=%d", fast, starved)
	}
	// Free space drifts under a concurrent suite, so compare within a tolerance; a REGRESSION would
	// be a different ORDER of magnitude (the cap, or 0), never a fraction of a percent.
	const tolerance = 0.02
	if delta := math.Abs(float64(fast-starved)) / float64(fast); delta > tolerance {
		t.Fatalf("the ceiling moved by %.2f%% when AccountInfo was starved (%d vs %d). On a "+
			"tether-rendered conf the ACCOUNT limit is -1, so the ceiling must come from statfs and be "+
			"insensitive to the sizing deadline. If this fails, the review's rejected 'sizing cut "+
			"degrades the admission gate' finding has become REAL and must be re-adjudicated", delta*100, fast, starved)
	}
	// The decisive form: the same push size must get the same verdict either way.
	probe := fast / 2
	fastMax, fastErr := xferMaxBytesForCeiling(fast, 1, 1)
	starvedMax, starvedErr := xferMaxBytesForCeiling(starved, 1, 1)
	if (fastErr == nil) != (starvedErr == nil) {
		t.Fatalf("the sizing deadline changed whether the store is judged too small: fast=%v starved=%v",
			fastErr, starvedErr)
	}
	if (probe > fastMax) != (probe > starvedMax) {
		t.Fatalf("the sizing deadline flipped the ADMISSION verdict for a %d-byte push (ceilings %d vs %d)",
			probe, fastMax, starvedMax)
	}
}

type slowAccountInfoJS struct {
	jetstream.JetStream
	delay    time.Duration
	maxStore int64
}

func (s *slowAccountInfoJS) AccountInfo(ctx context.Context) (*jetstream.AccountInfo, error) {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &jetstream.AccountInfo{Tier: jetstream.Tier{Limits: jetstream.AccountLimits{MaxStore: s.maxStore}}}, nil
}

// TestG67AdmissionGateStillRefusesOversizeWithItsOwnCode is review M3: the typed *xferTooLarge return
// and the relocated G6 #21 admission gate had ZERO pins at any tier — deleting the gate outright left
// every Go package and drills 21/61 green. The only shipped assertion was an anti-assertion (size 0,
// expect nil).
func TestG67AdmissionGateStillRefusesOversizeWithItsOwnCode(t *testing.T) {
	const ceiling = 4 << 30 // finite MaxStore => a finite, computable per-session ceiling
	js := &countingJS{maxStore: ceiling}
	b := newProvisionBroker(js)

	maxBytes, err := xferMaxBytesForCeiling(ceiling, 1, 1)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	payloadMax := xferPayloadLimit(maxBytes, 0)
	bucket, tooLarge, perr := b.provisionXferBucket(context.Background(), "sess", payloadMax+1)

	if tooLarge == nil {
		t.Fatal("a size above the per-session ceiling must be refused by the ADMISSION gate — without " +
			"this the G6 #21 refusal degrades into bucket_create_failed (or vanishes) and a small-disk " +
			"broker accepts a transfer it cannot store, which is the racknerd fill shape")
	}
	if tooLarge.MaxBytes != payloadMax {
		t.Fatalf("the refusal must quote the real payload ceiling: got %d want %d", tooLarge.MaxBytes, payloadMax)
	}
	if perr != nil || bucket != "" {
		t.Fatalf("admission refusal must not also report a provisioning failure: perr=%v bucket=%q", perr, bucket)
	}
	if js.creates != 0 {
		t.Fatalf("an admission refusal must never touch JetStream, got %d creates", js.creates)
	}
}

// TestG67PullHandlerRepliesTransientCodeOnStall is review M1: the pull half was hand-edited in a
// second place with NO oracle — reverting handlePullReq to its pre-#67 single-shot form left
// ./internal/broker/ and ./test/cli_e2e/ green while the push twin was correctly caught.
func TestG67PullHandlerRepliesTransientCodeOnStall(t *testing.T) {
	nc, actor, b := pushWiringBroker(t, &countingJS{err: context.DeadlineExceeded})

	subj := proto.SubjCmdBy("lab", actor, "lab-1", "pull")
	sub, err := nc.Subscribe(subj, func(msg *nats.Msg) { b.handlePullReq(nc, msg) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(proto.PullPrepareReq{TransferID: "tid-pull-g67", Path: "big.bin"})
	reply, err := nc.Request(subj, body, 30*time.Second)
	if err != nil {
		t.Fatalf("pull prepare: %v", err)
	}
	var resp proto.PullPrepareResp
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.OK {
		t.Fatal("the stubbed create always fails, so prepare must be refused")
	}
	if resp.Code != codeXferJSNotReady {
		t.Fatalf("PULL must reply with the transient code too, got %q (%s) — the pull block is a second, "+
			"separately hand-edited call site and needs its own oracle", resp.Code, resp.Error)
	}
	if !strings.Contains(strings.ToLower(resp.Error), "retry") {
		t.Fatalf("the operator-visible pull text must say it is worth retrying; got %q", resp.Error)
	}
}

// TestG67HandlersPassRunCtxNotBackground is review M2: swapping both call sites to context.Background()
// kept everything green, because pushWiringBroker never set runCtx and provisionXferBucket maps nil to
// Background(). With Background() a broker shutdown no longer aborts provisioning — every in-flight
// push adds up to the full budget to `systemctl restart` — and the whole Aborted branch becomes dead.
func TestG67HandlersPassRunCtxNotBackground(t *testing.T) {
	nc, actor, b := pushWiringBroker(t, &blockingJS{})
	ctx, cancel := context.WithCancel(context.Background())
	b.runCtx = ctx

	subj := proto.SubjCmdBy("lab", actor, "lab-1", "push")
	sub, err := nc.Subscribe(subj, func(msg *nats.Msg) { b.handlePushReq(nc, msg) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	go func() { time.Sleep(400 * time.Millisecond); cancel() }()

	body, _ := json.Marshal(proto.PushPrepareReq{
		TransferID: "tid-runctx", Path: "big.bin", Tier: "b", Size: 12_000_000, SHA256: "deadbeef",
	})
	start := time.Now()
	reply, err := nc.Request(subj, body, 30*time.Second)
	if err != nil {
		t.Fatalf("push prepare: %v", err)
	}
	elapsed := time.Since(start)

	var resp proto.PushPrepareResp
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code != codeXferBrokerRestarting {
		t.Fatalf("cancelling the broker's runCtx must abort provisioning and reply %q; got %q (%s). "+
			"If the handler passes context.Background() instead, the cancel is invisible and this fails",
			codeXferBrokerRestarting, resp.Code, resp.Error)
	}
	if elapsed > xferSizingTimeout+xferCreateAttemptTO+2*time.Second {
		t.Fatalf("shutdown took %v to abort provisioning — it must not wait out the whole budget", elapsed)
	}
	if !errors.Is(b.runCtx.Err(), context.Canceled) {
		t.Fatal("fixture: runCtx should be cancelled by now")
	}
}

// TestCrossHomeReapSafeFloorMatchesDerivedDefault pins serveconf's duplicated safe floor against the
// broker's derived value (external review F2). internal/serveconf must not import internal/broker, so
// the constant is duplicated; without this pin the two could drift and the production schema would
// start accepting a floor below the tier-B watchdog again.
func TestCrossHomeReapSafeFloorMatchesDerivedDefault(t *testing.T) {
	if serveconf.MinXferCrossHomeReapAge != xferCrossHomeReapAge {
		t.Fatalf("serveconf safe floor %v != broker derived default %v — the production schema would "+
			"then permit a cross-home GC floor the broker itself considers unsafe",
			serveconf.MinXferCrossHomeReapAge, xferCrossHomeReapAge)
	}
	if xferCrossHomeReapAge <= transferTimeoutTierB {
		t.Fatalf("the cross-home floor (%v) must exceed one tier-B watchdog (%v): the leader cannot see "+
			"a transfer live on another home", xferCrossHomeReapAge, transferTimeoutTierB)
	}
}

// TestStagedTerminalReplayDetectsPriorCommitOnARealLedger is the POSITIVE evidence for the re-review's
// F1 fix, using a REAL raft node instead of a stubbed forward.
//
// The reviewer's counter-example stubs transferAuditForwardSync, so it can only observe emissions, not
// what the replicated dedup ledger does with them. This drives the actual path: commit the terminal
// through raft (which records its content-addressed ReqID in cluster_reqid_ledger), then simulate the
// crash window — ledger still on disk with the staged terminal — and assert recovery DETECTS the prior
// commit and re-emits nothing.
func TestStagedTerminalReplayDetectsPriorCommitOnARealLedger(t *testing.T) {
	n, _ := d7SingleNode(t, "brk-a")
	dir := t.TempDir()
	now := time.Now().UTC()

	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: dir, Logger: silentLogger(), Now: func() time.Time { return now }}
	b.cl = &clusterRuntime{node: n}

	terminal := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push",
		Ts: now.Add(-time.Hour), Session: "sess", Node: "n1",
		TransferID: "tid-real-ledger", Path: "/dst", Tier: "a", Bucket: "OBJ_xfer-sess",
	}
	// 1. The real terminal is COMMITTED through raft, exactly as a live forward would.
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return xferaudit.PlanTransferAudit(terminal)
	}); err != nil {
		t.Fatalf("commit the real terminal: %v", err)
	}
	// 2. The crash window: the ledger still carries the staged terminal because the unlink never ran.
	e := &transferEntry{transferID: "tid-real-ledger", sid: "sess", nid: "n1", verb: "push", tier: "a",
		bucket: "OBJ_xfer-sess", path: "/dst", startedAt: now.Add(-transferTimeoutTierA - xferStrandedSlack - time.Minute)}
	b.writeXferInflight(e)
	if staged, _ := b.stageXferInflightTerminal(e.transferID, terminal); !staged {
		t.Fatal("staging the terminal must succeed when a ledger dir exists")
	}

	// 3. Recovery must DETECT the prior commit and emit nothing.
	emitted := 0
	b.transferAuditForwardSync = func([]byte) error { emitted++; return nil }
	if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if emitted != 0 {
		t.Fatalf("recovery re-emitted %d terminal(s) for a transfer whose terminal was ALREADY committed "+
			"— the audit would then carry two rows for one transfer, which is the contradiction the "+
			"re-review's F1 counter-example pins", emitted)
	}
	if _, err := os.Stat(filepath.Join(dir, "xfer-inflight", xferInflightFilename(e.transferID))); !os.IsNotExist(err) {
		t.Fatal("once the prior commit is detected the ledger must be dropped, or every pass re-checks it forever")
	}
}

// Stream: same reason as the create-path fakes in xfer_provision_test.go — this fixture measures the
// CREATE attempt budget, so the resolve lookup must report "not found" and fall through.
func (b *blockingJS) Stream(context.Context, string) (jetstream.Stream, error) {
	return nil, jetstream.ErrStreamNotFound
}

func (s *slowAccountInfoJS) Stream(context.Context, string) (jetstream.Stream, error) {
	return nil, jetstream.ErrStreamNotFound
}
