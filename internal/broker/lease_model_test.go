package broker

// lease_model_test.go — the lease adjudicator driven by SEEDED RANDOM EVENT SEQUENCES under the
// injected clock, with the invariants stated once and checked after every register.
//
// WHY A MODEL AND NOT MORE TABLE TESTS
// ------------------------------------
// Every lease test in this package is one hand-written interleaving: register, subscribe, sleep a
// number, register again. The defect class this adjudicator exists for — two live processes on one
// forwarded subject — is a property of INTERLEAVINGS, and the ones that bit were the ones nobody
// wrote down (drill 83: a clone adjudicated two seconds after the incumbent's grant, inside the
// incumbent's register→subscribe turnaround; review round 2 B5: a cached "nobody" observation
// replayed after the grant window). A generator writes the interleavings nobody thought of; the
// invariants say what must hold in all of them.
//
// WHAT IS REAL AND WHAT IS MODELLED
// ---------------------------------
//   - The broker, its SQLite, the embedded NATS bus and the claim-probe request/reply are REAL
//     (leaseBrokerWithBus). "Subscribed" means a real subscription answering claim-probe;
//     "crashed" means that connection closed, exactly as a killed process looks to the server.
//   - Time is the INJECTED clock (Config.Now), which B3 made the lease path honour end to end;
//     heartbeats are written at model time, so "the predecessor has been silent for 11s" is one
//     Advance(11s), not an 11-second test. The probe budgets are real wall-clock waits inside NATS
//     Request. The BACKGROUND budget (3s) is replaced through the backgroundProbeBudgetOverride seam
//     in the random walk only — the two B5 scenarios run at the real budget because their timing is
//     the point. With the seam a "silent subscriber" step (interest on the name that never answers:
//     a dying process the server has not reaped) costs ~70ms, so the walk can afford the one event
//     that forces the background probe, the verdict cache and the silence rule. The first version
//     excluded it and ran the background probe zero times in 480 steps (internal review L2-F1).
//   - registerNode's side effect (the nodes row / heartbeat the real handler writes after a grant)
//     is replayed by the model after EVERY grant, because adjudicateLease alone reads that row and
//     never writes it. For a suffixed grant the contested register itself writes nothing (broker.go:
//     "a contested register writes NOTHING"; internal review, properties verifier) — the row the
//     model writes stands in for the agent's SECOND register under its new name, which follows
//     within the turnaround and is not modelled as a separate event (every register here presents
//     the bare name). Omitting it produced a false I1 on the first run: an instance live on gpu1-03
//     with no row claiming gpu1-03, and the next clone offered the same suffix.
//   - node.ReconcileStates is not run: nodes rows stay ONLINE however old their beat. Adjudication
//     reads heartbeat AGE, not status, so I1/I2 are unaffected; only the suffix CHOSEN for a clone
//     (claimedLeaseNames skips names whose row is not OFFLINE) differs from production. Recorded,
//     not modelled.
//   - Not modelled: the farewell (ReleasingName) path, which lives in replyLeaseVerdict rather than
//     adjudicateLease; it is covered by lease_ops_recovery_test.go and the p2 restart tests.
//   - Not modelled, DELIBERATELY: a zombie that subscribes on its name long after its grant. The
//     agent's order is connect → register → subscribe on one goroutine, and the product's whole
//     settle-window argument is that past the window, silence IS death — a process that only
//     subscribes after that has, by the broker's definition, already lost the name to whoever asked
//     in between. The first random walk (seed 1) produced exactly that: A granted at t0, never
//     subscribed or beat, C legitimately took the name at t0+11s, then A subscribed and the
//     invariant reported two live processes on one subject. That is not an adjudication bug —
//     there is no broker-side defence against a unilateral late subscription on a shared
//     credential — but it is a REAL residual (gotcha #72's shape: subscriptions outliving the
//     process's ability to speak) and is recorded here for review. The generator therefore only
//     subscribes an instance inside modelSubscribeTurnaround of its last register — a FIXED 3s, on
//     purpose NOT leaseGrantWindow: the first version used the product constant, so shrinking the
//     window to the old 1s (plan §4 B3 ①) also stopped the generator subscribing and the walk went
//     blind to the very mutation it was meant to catch (internal review L6-F10).
//   - adjudicateLease exposes no REASON for a successful grant (code is "" for bare and suffixed
//     alike; the only codes are the three error classes). plan B3's "assert the same reason branch"
//     is therefore not expressible without a product seam and the scenarios assert NAMES; recorded
//     (internal review L6-F7).
//
// INVARIANTS
//   I1  no two LIVE instances are subscribed on the same name (compared on the subject each one
//       actually subscribed to — liveName — not on the name the model last saw granted);
//   I2  with no other instance live and the holder shield expired, a register keeps the bare name
//       (a restart is never renamed for restarting). The shield is read from the broker's own
//       leaseHolder record, not a model copy (the first version kept its own lastBareGrant and moved
//       it on the same-instance fast path, where the product does not — internal review L2-F8), and
//       "expired" is strictly past leaseSubscribeSettle: at exactly the boundary the product's
//       silence rule still suffixes, conservatively, and I2 says the same;
//   I3  (plan B3 wrote "a suffixed name never reaches the DB") — NOT an invariant of this product:
//       the suffixed instance registers under its new name and its row is written like any other's.
//       The property the plan was reaching for is that the CONTESTED register writes nothing, which
//       is a broker.go ordering comment, not a DB predicate this model can state. Dropped, with this
//       note, rather than silently skipped (internal review L6-F7);
//   I4  a register never stays lease_probe_pending beyond the retry budget (bounded liveness).
//
// B5 IN THE WALK. With silent events the walk can reach review round 2's B5 (a cached "nobody"
// verdict consumed inside probeTTL while the incumbent is live). Under plan §-1 F5 a reproduction
// is recorded, never relaxed: an I1 violation inside probeTTL of a silent event, while
// b5ReplayWindowOpen is true, skips THAT seed with the trace and is counted; any other I1 violation
// fails. When the product increment closes B5, flip b5ReplayWindowOpen and every reproduction
// becomes a failure (internal review L2-F4).
//
// origin: docs/reviews/test-system-overhaul-plan.md B3 (distributed D1); pre-ruling §-1 F5 for the
// round-2 B5 replay window.

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// b5ReplayWindowOpen is flipped to false by the product increment that closes review round 2's B5.
// While true, a reproduction is recorded as a Skip (plan §-1 F5) and a NON-reproduction of the
// deterministic late-subscribe scenario is a failure (the Skip has gone stale); once false, a
// reproduction is a regression and fails.
// origin: docs/reviews/cloned-credential-instances-review-round2.md B5
const b5ReplayWindowOpen = true

// modelSubscribeTurnaround bounds how long after its register the generator lets an instance
// subscribe — the agent's real turnaround (drill 83 measured ~2s on a loaded container host).
// Deliberately NOT leaseGrantWindow; see the header.
const modelSubscribeTurnaround = 3 * time.Second

type leaseClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *leaseClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *leaseClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

const leaseModelBase = "gpu1"

var leaseModelInstances = []string{testInstanceA, testInstanceB, testInstanceC}

type leaseModel struct {
	t        *testing.T
	b        *Broker
	url      string
	clock    *leaseClock
	live     map[string]*nats.Conn // instance -> answering claim-probe subscription
	liveName map[string]string     // instance -> the subject it is actually subscribed on
	name     map[string]string     // instance -> name the broker last granted it
	// lastRegister is when each instance last registered; subscribe is only legal inside
	// modelSubscribeTurnaround of it (see the header: the agent's register→subscribe turnaround).
	lastRegister map[string]time.Time
	// lastSilent is the model time of the last silent-subscriber step (B5 classification).
	lastSilent time.Time
	seed       int64
	logMu      sync.Mutex // trace is called from the silent-subscriber closer goroutine too
	log        []string
}

func newLeaseModel(t *testing.T) *leaseModel {
	t.Helper()
	b, _, url := leaseBrokerWithBus(t)
	clock := &leaseClock{t: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)}
	b.cfg.Now = clock.Now
	return &leaseModel{t: t, b: b, url: url, clock: clock,
		live: map[string]*nats.Conn{}, liveName: map[string]string{}, name: map[string]string{},
		lastRegister: map[string]time.Time{}}
}

// newFastLeaseModel is newLeaseModel with the background probe budget shortened through the seam
// (restored on Cleanup). Used by the random walk only; the B5 scenarios run at the real budget.
func newFastLeaseModel(t *testing.T) *leaseModel {
	t.Helper()
	backgroundProbeBudgetOverride.Store(int64(inlineProbeGrace * 3))
	t.Cleanup(func() { backgroundProbeBudgetOverride.Store(0) })
	return newLeaseModel(t)
}

func (m *leaseModel) trace(format string, args ...any) {
	m.logMu.Lock()
	defer m.logMu.Unlock()
	m.log = append(m.log, fmt.Sprintf("%s  ", m.clock.Now().Format("15:04:05.000"))+fmt.Sprintf(format, args...))
}

func (m *leaseModel) traceText() string {
	m.logMu.Lock()
	defer m.logMu.Unlock()
	return strings.Join(m.log, "\n  ")
}

// beatRow replays registerNode's / the heartbeat handler's effect on the nodes row, at MODEL time.
func (m *leaseModel) beatRow(nid string) {
	m.t.Helper()
	res, err := m.b.cfg.DB.Exec(`UPDATE nodes SET last_heartbeat_at = ?, status = 'ONLINE' WHERE sid = 'lab' AND nid = ?`,
		m.clock.Now(), nid)
	if err != nil {
		m.t.Fatalf("beat %s: %v", nid, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		if _, err := m.b.cfg.DB.Exec(`INSERT INTO nodes(sid, nid, status, last_heartbeat_at) VALUES ('lab', ?, 'ONLINE', ?)`,
			nid, m.clock.Now()); err != nil {
			m.t.Fatalf("insert node %s: %v", nid, err)
		}
	}
}

func short(inst string) string { return inst[:1] }

// register adjudicates `inst` presenting the bare name, retrying through lease_probe_pending the
// way the agent does (I4 is the retry budget), and replays the register side effect of a BARE
// grant (a contested register writes nothing).
func (m *leaseModel) register(inst string) string {
	m.t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		lease, code, err := adjudicateLease(m.b, "lab", leaseModelBase, &proto.NodeRegisterReq{InstanceID: inst}, m.clock.Now())
		if code == leaseReasonProbePending {
			if time.Now().After(deadline) {
				m.fail("I4: register(%s) stayed %q for 8s", short(inst), leaseReasonProbePending)
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil || code != "" {
			m.fail("register(%s): code=%q err=%v", short(inst), code, err)
		}
		name := leaseModelBase
		if lease != nil {
			name = lease.AssignedNID
		}
		m.name[inst] = name
		m.lastRegister[inst] = m.clock.Now()
		// The nodes row. For a bare grant this is registerNode's write. For a SUFFIXED grant the
		// contested register itself writes nothing (broker.go), but the agent then adopts the name
		// and registers again under it, and THAT register writes the row — within the same
		// turnaround. The model replays that second register here; without it a suffixed instance
		// could be live on gpu1-03 with no row claiming gpu1-03, and the next clone would be offered
		// the same suffix once the in-memory "already offered" record aged out — a false I1 the
		// first run of this walk produced (seed 14).
		m.beatRow(name)
		m.trace("register(%s) -> %s", short(inst), name)
		return name
	}
}

// shieldExpired reads the holder shield from the broker's own record: true when nobody was ever
// granted the bare name, the holder released it, or the grant is strictly older than
// leaseSubscribeSettle. Read BEFORE a register — a bare grant overwrites grantedAt.
func (m *leaseModel) shieldExpired() bool {
	v, ok := m.b.leaseHolder.Load(leaseKey("lab", leaseModelBase))
	if !ok {
		return true
	}
	g, _ := v.(leaseGrant)
	return g.released || m.clock.Now().Sub(g.grantedAt) > leaseSubscribeSettle
}

// subscribe installs the instance's answering claim-probe subscription — only inside
// modelSubscribeTurnaround of its last register (see the header for the zombie shape this excludes
// and why). Reports whether it took effect.
func (m *leaseModel) subscribe(inst string) bool {
	m.t.Helper()
	n, ok := m.name[inst]
	if !ok || m.live[inst] != nil {
		return false
	}
	if m.clock.Now().Sub(m.lastRegister[inst]) >= modelSubscribeTurnaround {
		m.trace("subscribe(%s) skipped: %v since register, past the turnaround (zombie shape, not modelled)",
			short(inst), m.clock.Now().Sub(m.lastRegister[inst]))
		return false
	}
	m.live[inst] = subscribeClaimProbeAs(m.t, m.url, "lab", n, inst)
	m.liveName[inst] = n
	m.trace("subscribe(%s) on %s", short(inst), n)
	return true
}

// crash closes the instance's connection: no farewell, interest reaped by the server. Reports
// whether a live connection was actually closed.
func (m *leaseModel) crash(inst string) bool {
	c := m.live[inst]
	if c == nil {
		return false
	}
	c.Close()
	delete(m.live, inst)
	delete(m.liveName, inst)
	m.trace("crash(%s)", short(inst))
	return true
}

// beat writes a heartbeat for a LIVE instance's name at model time.
func (m *leaseModel) beat(inst string) bool {
	n, ok := m.name[inst]
	if !ok || m.live[inst] == nil {
		return false
	}
	m.beatRow(n)
	m.trace("beat(%s) %s", short(inst), n)
	return true
}

func (m *leaseModel) advance(d time.Duration) {
	m.clock.Advance(d)
	m.trace("advance(%v)", d)
}

// silentSubscriber installs interest on the bare name that never answers — a dying process whose
// subscription the server has not reaped. It is what forces the inline probe to time out and the
// background probe to run (and write the cache). Returns the closer.
func (m *leaseModel) silentSubscriber() func() {
	m.t.Helper()
	nc, err := nats.Connect(m.url)
	if err != nil {
		m.t.Fatal(err)
	}
	if _, err := nc.Subscribe(proto.SubjCmdForwarded("lab", leaseModelBase, "*"), func(*nats.Msg) {}); err != nil {
		m.t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		m.t.Fatal(err)
	}
	m.trace("silent interest installed")
	return func() { nc.Close(); m.trace("silent interest gone") }
}

// silentThenRegister is the walk's B5-shaped step: silent interest appears, `inst` registers into
// it (inline probe times out → background probe under the seam's budget → verdict cached), and the
// interest is removed while that probe is in flight. Returns the granted name.
func (m *leaseModel) silentThenRegister(inst string) string {
	m.t.Helper()
	stop := m.silentSubscriber()
	m.lastSilent = m.clock.Now()
	go func() {
		time.Sleep(inlineProbeGrace * 4)
		stop()
	}()
	return m.register(inst)
}

// waitBackgroundProbe blocks (wall clock) until a background probe launched just now has certainly
// finished and stored its verdict, whatever budget is in force.
func (m *leaseModel) waitBackgroundProbe() {
	time.Sleep(effectiveBackgroundProbeBudget() + 2*inlineProbeGrace + 300*time.Millisecond)
}

// deadPredecessor seeds a nodes row whose last beat is older than the grace: "the previous holder
// died a while ago". This is the ordinary restart precondition.
func (m *leaseModel) deadPredecessor() {
	m.t.Helper()
	at := m.clock.Now().Add(-2 * DefaultLeaseGrace)
	if _, err := m.b.cfg.DB.Exec(`INSERT INTO nodes(sid, nid, status, last_heartbeat_at) VALUES ('lab', ?, 'ONLINE', ?)`,
		leaseModelBase, at); err != nil {
		m.t.Fatal(err)
	}
	m.trace("dead predecessor row (beat %v ago)", 2*DefaultLeaseGrace)
}

func (m *leaseModel) fail(format string, args ...any) {
	m.t.Helper()
	m.t.Fatalf(format+"\n\nevent trace:\n  %s", append(args, m.traceText())...)
}

// cachedVerdictIsSilence reports whether the broker's probe cache holds, for `name`, a verdict that
// is NOT an answer (nobody / timeout) and is still inside probeTTL at model time — the exact state
// review round 2's B5 describes. Read straight from the product's cache, not inferred from timing.
func (m *leaseModel) cachedVerdictIsSilence(name string) bool {
	v, ok := m.b.probeCache.Load(leaseKey("lab", name))
	if !ok {
		return false
	}
	pv, _ := v.(probeVerdict)
	return !pv.answer.answered && m.clock.Now().Sub(pv.at) < probeTTL
}

// cachedVerdictText renders the cache entry for a failure message.
func (m *leaseModel) cachedVerdictText(name string) string {
	v, ok := m.b.probeCache.Load(leaseKey("lab", name))
	if !ok {
		return "none"
	}
	pv, _ := v.(probeVerdict)
	return fmt.Sprintf("answered=%v responder=%q definitive=%v age=%v (last silent step %v ago)",
		pv.answer.answered, pv.answer.responder, pv.answer.definitive, m.clock.Now().Sub(pv.at), m.clock.Now().Sub(m.lastSilent))
}

// expectB5 is the F5 pre-ruling as code, for the two deterministic B5 scenarios: reproduced while
// the window is open ⇒ Skip with the trace; reproduced after it was declared closed ⇒ regression;
// not reproduced while it is declared open ⇒ the Skip is stale, flip the constant.
func (m *leaseModel) expectB5(shape, granted string) {
	m.t.Helper()
	reproduced := granted == leaseModelBase
	switch {
	case reproduced && b5ReplayWindowOpen:
		m.t.Skipf("B5 REPRODUCED (%s; window [%v, %v)): clone was granted the bare name from a cached background "+
			"verdict while the incumbent is live and answering. Skipped under docs/reviews/test-system-overhaul-plan.md "+
			"§-1 F5 — a product increment owns the fix; flip b5ReplayWindowOpen when it lands.\n\n%s",
			shape, leaseGrantWindow, probeTTL, m.traceText())
	case reproduced:
		m.fail("B5 REGRESSED (%s): b5ReplayWindowOpen is false but the clone was granted the bare name", shape)
	case b5ReplayWindowOpen:
		m.fail("B5 no longer reproduces (%s): flip b5ReplayWindowOpen=false and delete this Skip (plan §-1 F5)", shape)
	}
}

// registerAndCheck reads the shield BEFORE the register (a bare grant overwrites grantedAt), then
// applies I1 and I2 to the outcome. Returns the granted name and, for the walk, whether the outcome
// was skipped as a B5 reproduction.
func (m *leaseModel) registerAndCheck(inst string) (name string, b5 bool) {
	m.t.Helper()
	expired := m.shieldExpired()
	othersLive := false
	for other, conn := range m.live {
		if other != inst && conn != nil {
			othersLive = true
		}
	}
	name = m.register(inst)
	for other, conn := range m.live {
		if other == inst || conn == nil {
			continue
		}
		if m.liveName[other] == name {
			// B5 is a specific mechanism — a CACHED background verdict that says "nobody / unknown"
			// consumed inside probeTTL while the incumbent is live — not "any collision after a
			// silent step". The classification therefore reads the broker's cache for this name:
			// only a live entry whose observation is NOT an answer, stamped inside probeTTL, is B5;
			// every other collision is a defect this walk has found (external review suggestion 4).
			if b5ReplayWindowOpen && m.cachedVerdictIsSilence(name) {
				m.t.Skipf("B5 REPRODUCED by the random walk (seed %d): %s was granted %q from a cached silent verdict "+
					"while %s is LIVE and subscribed on it. Skipped under plan §-1 F5.\n\nevent trace:\n  %s",
					m.seed, short(inst), name, short(other), m.traceText())
				return name, true
			}
			m.fail("I1 violated: %s was granted %q while %s is LIVE and subscribed on it — two processes on one forwarded subject "+
				"(cached verdict for %q: %s)", short(inst), name, short(other), name, m.cachedVerdictText(name))
		}
	}
	if !othersLive && expired && name != leaseModelBase {
		m.fail("I2 violated: %s was renamed to %q with nobody else live and the holder shield expired — a restart renamed for restarting",
			short(inst), name)
	}
	return name, false
}

// ---------------------------------------------------------------------------------------------
// Known scenarios: the interleavings this package's other tests and the two reviews pinned by hand,
// each written as a fixed sequence so the same machinery checks them.

func TestLeaseModelSameInstanceRegisteringTwiceKeepsTheBareName(t *testing.T) {
	m := newLeaseModel(t)
	if n := m.register(testInstanceA); n != leaseModelBase {
		m.fail("first register got %q", n)
	}
	m.subscribe(testInstanceA)
	m.advance(2 * time.Second)
	if n, _ := m.registerAndCheck(testInstanceA); n != leaseModelBase {
		m.fail("re-register by the same instance got %q", n)
	}
}

func TestLeaseModelSimultaneousLaunchSuffixesTheSecond(t *testing.T) {
	m := newLeaseModel(t)
	m.register(testInstanceA) // granted, not yet subscribed: the register->subscribe turnaround
	if n, _ := m.registerAndCheck(testInstanceB); n == leaseModelBase {
		m.fail("second clone inside the grant window got the bare name — the drill-83 fan-out")
	}
}

// TestLeaseModelCloneInsideTheSettleWindowIsSuffixed is drill 83 as a sequence: the incumbent was
// granted the name and is still between its register and its subscribe when a clone arrives two
// seconds later. Silence here must be read as "not subscribed yet", never as "dead" — the holder
// shield (leaseGrantWindow) is what makes that true, and shrinking it to the old 1s is the exact
// mutation that reintroduced the fan-out on real hosts.
func TestLeaseModelCloneInsideTheSettleWindowIsSuffixed(t *testing.T) {
	m := newLeaseModel(t)
	m.register(testInstanceA)
	m.advance(2 * time.Second) // inside the window, past the old 1s
	if n, _ := m.registerAndCheck(testInstanceB); n == leaseModelBase {
		m.fail("clone inside the settle window got the bare name (drill 83 fan-out)")
	}
	m.subscribe(testInstanceA) // the incumbent finally subscribes: still inside the window, still alone on the name
	m.registerAndCheck(testInstanceB)
}

func TestLeaseModelRestartPastTheWindowReclaimsTheBareName(t *testing.T) {
	m := newLeaseModel(t)
	m.register(testInstanceA)
	m.subscribe(testInstanceA)
	m.beat(testInstanceA)
	m.crash(testInstanceA)
	m.advance(leaseGrantWindow + DefaultLeaseGrace + time.Second)
	if n, _ := m.registerAndCheck(testInstanceB); n != leaseModelBase { // the restarted process: a NEW instance id
		m.fail("restart past the window and the grace was renamed to %q", n)
	}
}

func TestLeaseModelLiveHolderIsSeenPastProbeTTL(t *testing.T) {
	m := newLeaseModel(t)
	m.register(testInstanceA)
	m.subscribe(testInstanceA)
	m.advance(probeTTL + time.Second)
	m.beat(testInstanceA)
	if n, _ := m.registerAndCheck(testInstanceB); n == leaseModelBase {
		m.fail("a clone got the bare name while the incumbent is live, beating and answering")
	}
}

// TestLeaseModelReplayWindowAfterABackgroundProbe is review round 2's B5 shape that the product
// DOES handle: a dying predecessor's unreaped subscription makes the incumbent's register go through
// the background probe; the incumbent subscribes BEFORE that probe's recheck runs, so the recheck
// sees it and the cache holds "held by A"; a clone arriving past leaseGrantWindow but inside
// probeTTL must be suffixed. This scenario is expected to PASS, so its precondition — that A's
// subscribe landed inside the background budget — is proven, and a slow host that breaks it is
// reported as "precondition not met", never as a B5 reproduction (internal review L2-F4).
func TestLeaseModelReplayWindowAfterABackgroundProbe(t *testing.T) {
	m := newLeaseModel(t)
	m.deadPredecessor()
	stopSilent := m.silentSubscriber()
	// The incumbent registers: the inline probe times out on the silent interest, the background
	// probe starts (real ~3s). The silent interest is removed WHILE it is in flight, so the
	// incumbent's next inline retry sees ErrNoResponders and is granted at once.
	go func() {
		time.Sleep(inlineProbeGrace * 4)
		stopSilent()
	}()
	n := m.register(testInstanceA)
	grantedAt := time.Now()
	if n != leaseModelBase {
		m.fail("incumbent's first register got %q", n)
	}
	m.subscribe(testInstanceA)
	m.beat(testInstanceA)
	if since := time.Since(grantedAt); since >= backgroundProbeBudget {
		t.Skipf("timing precondition not met: subscribe landed %v after the grant (>= backgroundProbeBudget); this is load, not B5", since)
	}
	// Let the background probe finish writing (its recheck sees A) before the clock moves.
	m.waitBackgroundProbe()
	m.advance(leaseGrantWindow + time.Second) // past the shield, inside probeTTL
	m.beat(testInstanceA)
	if n, _ := m.registerAndCheck(testInstanceB); n == leaseModelBase {
		m.fail("cached background verdict overrode a live, answering incumbent: the recheck should have seen A")
	}
}

// TestLeaseModelReplayWindowWhenTheIncumbentSubscribesLate is the B5 shape the scenario above cannot
// reach: the incumbent's register→subscribe turnaround is longer than backgroundProbeBudget (still
// inside leaseGrantWindow — drill 83 measured ~2s on a loaded container host; 3.5s is the same shape
// under more load), so the recheck sees NOBODY and that is what the cache keeps for probeTTL. A
// clone past the window then reads "free".
//
// Pre-ruling §-1 F5: reproduce ⇒ record and skip, never relax (expectB5 makes the Skip expire in
// both directions).
func TestLeaseModelReplayWindowWhenTheIncumbentSubscribesLate(t *testing.T) {
	m := newLeaseModel(t)
	m.deadPredecessor()
	stopSilent := m.silentSubscriber()
	go func() {
		time.Sleep(inlineProbeGrace * 4)
		stopSilent()
	}()
	n := m.register(testInstanceA)
	if n != leaseModelBase {
		m.fail("incumbent's first register got %q", n)
	}
	// The incumbent is slow to subscribe: the background probe (real ~3s) completes first and its
	// recheck finds nobody. Wall-clock wait, clock NOT advanced — the whole point is that the
	// verdict is stamped at the grant instant.
	m.waitBackgroundProbe()
	m.subscribe(testInstanceA)
	m.beat(testInstanceA)
	m.advance(leaseGrantWindow + time.Second) // past the shield, inside probeTTL
	m.beat(testInstanceA)
	m.expectB5("late-subscribe", m.register(testInstanceB))
}

// ---------------------------------------------------------------------------------------------
// Random walk.

func TestLeaseModelRandomSequencesHoldTheInvariants(t *testing.T) {
	// plan B3's 60×20. Each silent step costs ~70ms of wall clock at the seam's budget (at most one
	// per sequence); every other step is milliseconds; a fresh broker + embedded NATS per sequence
	// is the rest. ~8s on the maintainer's box.
	const sequences, steps = 60, 20
	advances := []time.Duration{100 * time.Millisecond, time.Second, 3 * time.Second,
		leaseGrantWindow + time.Second, probeTTL + time.Second}
	// effects is the G2 self-check, counted by the REAL walk: not emissions of the generator but
	// events that took effect (a subscribe that installed interest, a crash that closed a live
	// connection) — a shadow replay of the RNG counted the former and would silently fall out of
	// step the day someone adds an Intn (internal review L2-F7). The first real count showed why
	// emissions are the wrong measure: 480 steps emitted crash ~70 times and closed a live
	// connection 5 times, because a random instance is rarely eligible. The generator therefore
	// offers each state-dependent event to the instances in random order and takes the first that
	// is eligible. Subtests run sequentially.
	effects := map[string]int{}
	for seed := int64(1); seed <= sequences; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			m := newFastLeaseModel(t)
			m.seed = seed
			r := rand.New(rand.NewSource(seed))
			usedSilent := false
			defer func() {
				for inst, c := range m.live {
					if c != nil {
						c.Close()
						delete(m.live, inst)
					}
				}
			}()
			// candidates returns the instances in a seeded random order.
			candidates := func() []string {
				out := append([]string(nil), leaseModelInstances...)
				r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
				return out
			}
			firstEligible := func(kind string, try func(string) bool) {
				for _, inst := range candidates() {
					if try(inst) {
						effects[kind]++
						return
					}
				}
				effects[kind+"-ineligible"]++
			}
			for i := 0; i < steps; i++ {
				switch p := r.Intn(100); {
				case p < 30:
					inst := leaseModelInstances[r.Intn(len(leaseModelInstances))]
					if _, b5 := m.registerAndCheck(inst); b5 {
						effects["b5-skip"]++
						return
					}
					effects["register"]++
				case p < 50:
					firstEligible("subscribe", m.subscribe)
				case p < 62:
					firstEligible("crash", m.crash)
				case p < 77:
					firstEligible("beat", m.beat)
				case p < 85 && !usedSilent:
					// At most one per sequence: the step is ~70ms of wall clock, and one is enough
					// to open the cache / silence-rule paths for the rest of the sequence.
					usedSilent = true
					m.silentThenRegister(leaseModelInstances[r.Intn(len(leaseModelInstances))])
					effects["silent"]++
				default:
					m.advance(advances[r.Intn(len(advances))])
					effects["advance"]++
				}
			}
		})
	}
	for _, k := range []string{"register", "subscribe", "crash", "beat", "silent", "advance"} {
		if effects[k] < 20 {
			t.Fatalf("event %q took effect only %d times across %dx%d steps — the invariants were checked against "+
				"sequences that do not exercise it: %v", k, effects[k], sequences, steps, effects)
		}
	}
	t.Logf("walk effects: %v", effects)
}
