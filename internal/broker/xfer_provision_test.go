package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// stallJS is a jetstream.JetStream stub that reproduces the #67 face-A window: the JS meta is
// re-assigning and has no leader yet, so the server accepts the API request and NEVER REPLIES
// (nats-server returns without a reply when !JetStreamIsLeader()). The client therefore learns
// nothing until its own deadline expires.
//
// The embedded nil interface deliberately supplies the ~20 methods we do not override: any call to
// one of them panics, which is the guard we want — a test that silently exercised an unstubbed JS
// method would be proving something other than what it claims.
type stallJS struct {
	jetstream.JetStream
	// accountInfoBlocks makes AccountInfo consume the caller's whole deadline instead of answering.
	accountInfoBlocks bool
	// createBudget receives the time remaining on the context that CreateObjectStore was handed.
	// A negative value means the deadline had already expired when the create was attempted.
	createBudget chan time.Duration
}

func (s *stallJS) AccountInfo(ctx context.Context) (*jetstream.AccountInfo, error) {
	if s.accountInfoBlocks {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &jetstream.AccountInfo{}, nil
}

func (s *stallJS) CreateObjectStore(ctx context.Context, _ jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	remaining := time.Duration(-1)
	if dl, ok := ctx.Deadline(); ok {
		remaining = time.Until(dl)
	}
	select {
	case s.createBudget <- remaining:
	default:
	}
	return nil, context.DeadlineExceeded
}

// TestSizingStallDoesNotConsumeCreateBudget is the #67 face-A ROOT-CAUSE ADJUDICATION (plan G67
// step 0, written RED-first against the pre-fix code).
//
// handlePushReq puts TWO independent JetStream round trips under ONE 5s context: the best-effort
// sizing probe (xferBucketMaxBytes -> jsStoreCeiling -> AccountInfo, whose error is SWALLOWED at
// transfer.go:207-215 in favour of statfs) and the load-bearing CreateObjectStore. A sizing probe
// that stalls therefore reports no failure at all — it silently eats the create's budget, and the
// operator is told `create_bucket: context deadline exceeded` for a create that a probe measured at
// ~57ms on a healthy cluster.
//
// The invariant: however long the best-effort sizing probe stalls, the LOAD-BEARING create must
// still get a usable budget of its own.
func TestSizingStallDoesNotConsumeCreateBudget(t *testing.T) {
	js := &stallJS{accountInfoBlocks: true, createBudget: make(chan time.Duration, 1)}
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{Logger: silentLogger(), Now: time.Now}
	b.js = js

	// Written RED-first against the pre-fix sequence (one shared 5s ctx across both round trips),
	// where the create was handed an ALREADY-EXPIRED context (-4ms observed). It now drives the
	// extracted seam; the ASSERTION below is the invariant and did not move.
	_, _, perr := b.provisionXferBucket(context.Background(), "sess", 0)
	if perr == nil {
		t.Fatal("the stubbed create always fails, so provisioning must report an error")
	}

	var got time.Duration
	select {
	case got = <-js.createBudget:
	default:
		t.Fatal("CreateObjectStore was never attempted — the test is not exercising the create leg")
	}

	const wantAtLeast = 2 * time.Second
	if got < wantAtLeast {
		t.Fatalf("#67 face A: the load-bearing CreateObjectStore was handed only %v of budget "+
			"(want >= %v) because the best-effort sizing probe consumed the shared deadline. "+
			"A stalled AccountInfo must not starve the create: give the sizing probe its own "+
			"short deadline and the create a fresh one.", got, wantAtLeast)
	}
}

// TestXferProvisionRefusalDistinguishesTransientFromPermanent is T5: the #67 headline invariant. A
// transient stall and a permanent misconfiguration must NOT arrive at the operator as the same
// terminal message. Asserted BOTH ways, because the likeliest dishonest version of this fix is to
// paste "retry shortly" onto every refusal until the word means nothing.
func TestXferProvisionRefusalDistinguishesTransientFromPermanent(t *testing.T) {
	retryWords := regexp.MustCompile(`(?i)retry|transient|temporar|try again`)

	transientCode, transientText := xferProvisionRefusal(&xferProvisionErr{
		Err:       fmt.Errorf("create_bucket: %w", context.DeadlineExceeded),
		Transient: true, Attempts: 3, Elapsed: 7 * time.Second,
	})
	if transientCode != codeXferJSNotReady {
		t.Fatalf("a transient stall must get its own code, got %q", transientCode)
	}
	if !retryWords.MatchString(transientText) {
		t.Fatalf("the transient refusal must tell the operator it is worth retrying; got %q", transientText)
	}
	if !strings.Contains(transientText, "context deadline exceeded") {
		t.Fatalf("the raw cause must survive verbatim for grepping; got %q", transientText)
	}
	if !strings.Contains(transientText, "cluster status") {
		t.Fatalf("the transient refusal must carry an escalation path for the case where it does NOT "+
			"clear (a permanently departed peer produces the identical DeadlineExceeded); got %q", transientText)
	}
	if strings.Contains(strings.ToLower(transientText), "quorum") {
		t.Fatalf("the broker cannot establish quorum loss as fact here and must not assert it; got %q", transientText)
	}

	// Internal review M5: the sizing refusal now carries its OWN code so the exit taxonomy matches the
	// prose — docs/usage.md §9.13 tells automation 70 is RETRYABLE, so a "permanent" refusal mapped to
	// 70 would be retried forever, each attempt paying a full-file SHA-256.
	permCode, permText := xferProvisionRefusal(&xferProvisionErr{
		Err:       fmt.Errorf("xfer bucket sizing: %w", errXferStoreTooSmall),
		Transient: false, Attempts: 0,
	})
	if permCode != codeXferStoreTooSmall {
		t.Fatalf("an operator-actionable disk refusal must carry its own TERMINAL code, got %q", permCode)
	}
	unclassCode, _ := xferProvisionRefusal(&xferProvisionErr{
		Err: errors.New("something nobody classified"), Transient: false, Attempts: 1,
	})
	if unclassCode != "bucket_create_failed" {
		t.Fatalf("the genuinely UNCLASSIFIED remainder must keep bucket_create_failed, got %q", unclassCode)
	}
	if retryWords.MatchString(permText) {
		t.Fatalf("the PERMANENT refusal must contain no retry vocabulary — retrying a too-small disk "+
			"never succeeds and the word would stop meaning anything; got %q", permText)
	}
	if !errors.Is(fmt.Errorf("xfer bucket sizing: %w", errXferStoreTooSmall), errXferStoreTooSmall) {
		t.Fatal("the sizing sentinel must remain structurally detectable through wrapping")
	}

	// Internal review (adopted): a restarting broker is the most retriable condition there is, so the
	// aborted refusal carries a RETRIABLE code — it used to land on the unclassified exit 70.
	abortCode, abortText := xferProvisionRefusal(&xferProvisionErr{
		Err: context.Canceled, Aborted: true, AbortCause: context.Canceled, Attempts: 1,
	})
	if abortCode != codeXferBrokerRestarting || !strings.Contains(abortText, "shutting down") {
		t.Fatalf("a broker shutdown must be reported as such, not as a JetStream verdict; got %q/%q", abortCode, abortText)
	}
}

// TestXferProvisionPermanentMakesExactlyOneAttempt pins the head-of-line-blocking guard: push.req is
// a plain nc.Subscribe whose handler is registered directly as the nats.go callback, so every push on
// this broker serialises behind this call. A misconfigured broker must therefore NEVER pay the retry
// budget per request.
func TestXferProvisionPermanentMakesExactlyOneAttempt(t *testing.T) {
	js := &countingJS{err: errors.New("replicas > 1 not supported in non-clustered mode")}
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{Logger: silentLogger(), Now: time.Now}
	b.js = js

	start := time.Now()
	_, _, perr := b.provisionXferBucket(context.Background(), "sess", 0)
	elapsed := time.Since(start)

	if perr == nil {
		t.Fatal("want a provisioning error")
	}
	if perr.Transient {
		t.Fatal("a non-clustered misconfiguration is PERMANENT — classifying it transient would spin")
	}
	if js.creates != 1 {
		t.Fatalf("a permanent condition must cost exactly ONE create attempt, got %d — every extra "+
			"attempt is head-of-line blocking for every other push on this broker", js.creates)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("a permanent refusal must return fast, took %v", elapsed)
	}
}

// TestXferProvisionRetriesTransientWithinBudget pins that a transient failure IS retried, and that the
// whole loop stays inside the wall-clock budget.
func TestXferProvisionRetriesTransientWithinBudget(t *testing.T) {
	js := &countingJS{err: context.DeadlineExceeded}
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{Logger: silentLogger(), Now: time.Now}
	b.js = js

	start := time.Now()
	_, _, perr := b.provisionXferBucket(context.Background(), "sess", 0)
	elapsed := time.Since(start)

	if perr == nil || !perr.Transient {
		t.Fatalf("a stalled create must be classified transient, got %+v", perr)
	}
	if js.creates != xferProvisionMaxTries {
		t.Fatalf("want %d attempts, got %d — deleting the retry loop must redden this",
			xferProvisionMaxTries, js.creates)
	}
	if perr.Attempts != xferProvisionMaxTries {
		t.Fatalf("the refusal text quotes the attempt count to the operator; got %d", perr.Attempts)
	}
	if elapsed > xferSizingTimeout+xferProvisionBudget+time.Second {
		t.Fatalf("provisioning ran %v, past its wall-clock ceiling — this is serialised in front of "+
			"every other push on the broker", elapsed)
	}
}

// TestXferProvisionSucceedsOnRetry is the deploy-tier headline in hermetic form: the FIRST attempt
// fails transiently and the second succeeds, which is exactly what a plain manual retry did on a
// freshly grown N=2 cluster.
func TestXferProvisionSucceedsOnRetry(t *testing.T) {
	js := &countingJS{err: context.DeadlineExceeded, succeedFrom: 2}
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{Logger: silentLogger(), Now: time.Now}
	b.js = js

	_, _, perr := b.provisionXferBucket(context.Background(), "sess", 0)
	if perr != nil {
		t.Fatalf("the second attempt succeeds, so provisioning must succeed: %+v", perr)
	}
	if js.creates != 2 {
		t.Fatalf("want exactly 2 attempts, got %d", js.creates)
	}
}

// TestXferProvisionSizingRefusalMakesZeroAttempts: a disk too small for the tier-B floor is a
// PERMANENT sizing verdict, so JetStream must not be touched at all.
func TestXferProvisionSizingRefusalMakesZeroAttempts(t *testing.T) {
	js := &countingJS{maxStore: 1024} // far below the events/history reserve + floor
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{Logger: silentLogger(), Now: time.Now}
	b.js = js

	_, tooLarge, perr := b.provisionXferBucket(context.Background(), "sess", 0)
	if tooLarge != nil {
		t.Fatal("size 0 can never exceed the ceiling")
	}
	if perr == nil || perr.Transient {
		t.Fatalf("a too-small store is a PERMANENT refusal, got %+v", perr)
	}
	if js.creates != 0 {
		t.Fatalf("a sizing refusal must make ZERO create attempts, got %d", js.creates)
	}
	if !errors.Is(perr.Err, errXferStoreTooSmall) {
		t.Fatalf("the sizing sentinel must survive wrapping so the classifier is structural: %v", perr.Err)
	}
}

// countingJS counts create attempts and can be told to start succeeding, so the retry loop's
// behaviour is observable rather than inferred from wall-clock.
type countingJS struct {
	jetstream.JetStream
	err         error // returned by CreateObjectStore until succeedFrom
	succeedFrom int   // 1-based attempt from which the create succeeds; 0 = never
	maxStore    int64 // AccountInfo.Limits.MaxStore; 0 => unlimited => statfs fallback
	creates     int
}

func (c *countingJS) AccountInfo(context.Context) (*jetstream.AccountInfo, error) {
	return &jetstream.AccountInfo{Tier: jetstream.Tier{Limits: jetstream.AccountLimits{MaxStore: c.maxStore}}}, nil
}

func (c *countingJS) CreateObjectStore(_ context.Context, _ jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	c.creates++
	if c.succeedFrom > 0 && c.creates >= c.succeedFrom {
		return nil, nil
	}
	return nil, c.err
}

// pushWiringBroker builds the minimum real state handlePushReq needs to REACH the tier-B
// provisioning block: an ACTIVE session, the actor as a member, and an ONLINE node. selfID is left
// empty so transferHomeGate is inert (production single-broker shape).
func pushWiringBroker(t *testing.T, js jetstream.JetStream) (*nats.Conn, string, *Broker) {
	t.Helper()
	url := startNATS(t)
	db := openDB(t)
	pub, fp := testharness.FreshUserPub(t)
	if _, err := session.Create(db, "lab", "lab", fp, "pinhash", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := session.AddMember(db, "lab", fp, session.RoleOwner, session.ViaPin, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := node.Register(db, node.RegisterInput{
		SID: "lab", NID: "lab-1", ProtoVersion: proto.ProtoVersion, ReleaseVersion: "v0.0.0",
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger(), Now: time.Now}, transfers: newTransferTracker()}
	b.js = js
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc, pub, b
}

// TestPushHandlerRepliesTransientCodeOnStall is the WIRING pin (R16 internal-review M7 lesson: a test
// that only exercises the mapping function passes with the handler still hard-coding the old code).
// It drives the real handlePushReq over real NATS and asserts the reply an operator would receive.
// Reverting the handler's tier-B block to the pre-#67 single-attempt form reddens THIS test.
func TestPushHandlerRepliesTransientCodeOnStall(t *testing.T) {
	nc, actor, b := pushWiringBroker(t, &countingJS{err: context.DeadlineExceeded})

	subj := proto.SubjCmdBy("lab", actor, "lab-1", "push")
	sub, err := nc.Subscribe(subj, func(msg *nats.Msg) { b.handlePushReq(nc, msg) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(proto.PushPrepareReq{
		TransferID: "tid-g67", Path: "big.bin", Tier: "b", Size: 12_000_000, SHA256: "deadbeef",
	})
	reply, err := nc.Request(subj, body, 30*time.Second)
	if err != nil {
		t.Fatalf("push prepare: %v", err)
	}
	var resp proto.PushPrepareResp
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.OK {
		t.Fatal("the stubbed create always fails, so prepare must be refused")
	}
	if resp.Code != codeXferJSNotReady {
		t.Fatalf("the HANDLER must reply with the transient code, got %q (%s) — this is the wiring, "+
			"not the mapping: a handler still hard-coding bucket_create_failed passes every unit test "+
			"of xferProvisionRefusal", resp.Code, resp.Error)
	}
	if !strings.Contains(strings.ToLower(resp.Error), "retry") {
		t.Fatalf("the operator-visible text must say it is worth retrying; got %q", resp.Error)
	}
	if b.transfers.get("tid-g67") != nil {
		t.Fatal("a refused prepare must leave NO tracker entry — provisioning runs before transfers.put()")
	}
}

// TestPushHandlerNilJetStreamHasRealProse: the nil-JS refusal used to carry an EMPTY Error string,
// which is what let the CLI invent "broker has no JetStream ... bump nats max_payload" out of a zero
// value. The optimistic client path now lands here, so this message must be real.
func TestPushHandlerNilJetStreamHasRealProse(t *testing.T) {
	nc, actor, b := pushWiringBroker(t, nil)
	b.js = nil

	subj := proto.SubjCmdBy("lab", actor, "lab-1", "push")
	sub, err := nc.Subscribe(subj, func(msg *nats.Msg) { b.handlePushReq(nc, msg) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(proto.PushPrepareReq{
		TransferID: "tid-nojs", Path: "big.bin", Tier: "b", Size: 12_000_000, SHA256: "deadbeef",
	})
	reply, err := nc.Request(subj, body, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.PushPrepareResp
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code != "jetstream_unavailable" {
		t.Fatalf("code=%q want jetstream_unavailable", resp.Code)
	}
	if resp.Error == "" {
		t.Fatal("the nil-JS refusal must not be an empty string — an empty broker answer is exactly " +
			"what the client used to paper over with an invented, wrong explanation")
	}
	if !strings.Contains(resp.Error, "probe") {
		t.Fatalf("the message must name BOTH causes (disabled in nats.conf OR the one-shot boot probe "+
			"failed), because the broker never re-probes and cannot claim it is disabled; got %q", resp.Error)
	}
}

// Stream is what the resolve-before-create lookup calls. These fixtures exist to exercise the CREATE
// path, so they report "no such stream" and the lookup falls through exactly as it did before that
// step existed. (Without this the embedded nil interface panics — which is the fixture pattern doing
// its job: it announced that the code under test began calling something new.)
func (c *countingJS) Stream(context.Context, string) (jetstream.Stream, error) {
	return nil, jetstream.ErrStreamNotFound
}

// Stream is what the resolve-before-create lookup calls. These fixtures exist to exercise the CREATE
// path, so they report "no such stream" and the lookup falls through exactly as it did before that
// step existed. (Without this the embedded nil interface panics — which is the fixture pattern doing
// its job: it announced that the code under test began calling something new.)
func (s *stallJS) Stream(context.Context, string) (jetstream.Stream, error) {
	return nil, jetstream.ErrStreamNotFound
}
