package broker

// session_rm_contract_test.go — the `session rm` reply contract around the S7
// transfer settlement barrier. LQ1 keeps a session DELETING with rc75 ONLY while
// a continuation transfer is unsettled (or a census cannot prove there is none);
// every other outcome keeps the contract the command had before S7:
//
//   - phase ②③ failures after the barrier released the session are non-fatal:
//     OK, the session stays DELETING, and the boot resumer finishes the job;
//   - an ungoverned removal of an ALREADY DELETING session answers already_deleting;
//     a retry that passes settlement and drives the cascade answers OK;
//   - a follower acts on the tombstone result the leader returned to it, never
//     on its own replica, which may not have applied that tombstone yet.
//
// Every test drives the real handler over real NATS against real SQL (and, for
// the follower, a real raft leader behind the real forwarder).

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// A fixture with an invalid stored PIN hash cannot exercise the S7 barrier:
// S8 must refuse before the tombstone, settlement or external deletion. Keep
// that refusal explicit so valid creation fixtures never conceal a bypass.
// origin: S7 internal review BROKER-REDS F3; hot-update S8 plan §6.2
func TestSessionRmRejectsUnavailableCreationAuthority(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSessionRmUnavailableAuthority(t)
	}
	testharness.AssertNoGoroutineLeak(t, "session removal creation refusal", g)
	testharness.AssertNoFDLeak(t, "session removal creation refusal", fd, 0)
}

func exerciseSessionRmUnavailableAuthority(t *testing.T) {
	t.Helper()
	for _, state := range []string{"ACTIVE", "DELETING"} {
		t.Run(state, func(t *testing.T) {
			barrier := &recordingBarrier{decide: func(bool) error { return nil }}
			f := newRmFixture(t, barrier)
			const unavailable = "invalid-private-creation-material"
			if _, err := f.db.Exec(`UPDATE sessions SET state=?, pin_hash=? WHERE sid='lab'`, state, unavailable); err != nil {
				t.Fatal(err)
			}
			for _, body := range []string{`{}`, `{"abandon_unsettled_transfers":true}`} {
				r := requestSessionRm(t, f.nc, f.actor, body)
				if r.OK || r.Code != "store_error" || !strings.Contains(r.Error, "upgrade_deferred") || strings.Contains(r.Error, unavailable) {
					t.Fatalf("unavailable creation authority did not fail closed: %+v", r)
				}
				if got := f.state(t); got != state {
					t.Fatalf("creation refusal changed session state: got %q, want %q", got, state)
				}
				for _, stream := range []string{"history-lab", "OBJ_xfer-lab"} {
					if !f.streamExists(t, stream) {
						t.Fatalf("creation refusal deleted %s", stream)
					}
				}
				if len(barrier.calls()) != 0 {
					t.Fatal("unavailable authority reached transfer settlement")
				}
			}
		})
	}
}

// cascadeFaultJS fails the phase ② history delete on demand. Every other call,
// including the audit publishes and the boot resumer's later delete, reaches
// the real embedded JetStream.
type cascadeFaultJS struct {
	jetstream.JetStream
	fail atomic.Bool
}

func (j *cascadeFaultJS) DeleteStream(ctx context.Context, name string) error {
	if j.fail.Load() {
		return errors.New("injected: jetstream stream delete unavailable")
	}
	return j.JetStream.DeleteStream(ctx, name)
}

// recordingBarrier is a settlement port that answers from a fixed decision
// function and records the abandonment flag of every call it receives.
type recordingBarrier struct {
	mu     sync.Mutex
	seen   []bool
	decide func(abandon bool) error
}

func (r *recordingBarrier) Settle(sid string, abandon bool, d controlbudget.Deadline) error {
	if _, err := controlbudget.Remaining(d); err != nil {
		return err
	}
	r.mu.Lock()
	r.seen = append(r.seen, abandon)
	r.mu.Unlock()
	if sid != "lab" {
		return errors.New("settlement asked about the wrong session")
	}
	return r.decide(abandon)
}

func (r *recordingBarrier) calls() []bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.seen)
}

type rmFixture struct {
	js    *cascadeFaultJS
	nc    *nats.Conn
	db    *sql.DB
	b     *Broker
	actor string
}

// newRmFixture builds a single broker with session "lab", its history stream
// and its OBJ_xfer bucket. A nil barrier is the broker with no continuation
// capability at all (no factory, no graph, no injected port).
func newRmFixture(t *testing.T, barrier xferauth.SessionBarrier) *rmFixture {
	t.Helper()
	js, nc := newXferTestJS(t)
	f := &rmFixture{js: &cascadeFaultJS{JetStream: js}, nc: nc, db: openDB(t), actor: freshUserActor(t)}
	fp, err := auth.FingerprintFromActor(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(f.db, "lab", "lab", fp, brokerAuthorityPIN(1), time.Now()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := jsstream.EnsureHistoryStream(ctx, js, "lab", jsstream.ReplicasSingle); err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-lab"}); err != nil {
		t.Fatal(err)
	}
	cfg := Config{NATSURL: nc.ConnectedUrl(), DB: f.db, Logger: silentLogger()}
	if barrier != nil {
		cfg.TransferSettlement = barrier
	}
	if f.b, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	f.b.nc.Store(nc)
	setBrokerJS(f.b, f.js)
	t.Cleanup(func() { setBrokerJS(f.b, nil) })
	sub, err := nc.Subscribe(proto.SubjCtrlSessionRm(f.actor, "lab"), f.b.handleSessionRm)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return f
}

func requestSessionRm(t *testing.T, nc *nats.Conn, actor, body string) proto.SessionRmResp {
	t.Helper()
	msg, err := nc.Request(proto.SubjCtrlSessionRm(actor, "lab"), []byte(body), 8*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var r proto.SessionRmResp
	if err := json.Unmarshal(msg.Data, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

// state is the session row's state, or "" once phase ③ removed the row.
func (f *rmFixture) state(t *testing.T) string {
	t.Helper()
	var state string
	err := f.db.QueryRow(`SELECT state FROM sessions WHERE sid='lab'`).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func (f *rmFixture) streamExists(t *testing.T, name string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := f.js.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

func (f *rmFixture) assertGone(t *testing.T) {
	t.Helper()
	for _, name := range []string{"history-lab", "OBJ_xfer-lab"} {
		if f.streamExists(t, name) {
			t.Fatalf("removal retained %s", name)
		}
	}
	if got := f.state(t); got != "" {
		t.Fatalf("removal retained the session row in state %q", got)
	}
}

func (f *rmFixture) assertRetained(t *testing.T) {
	t.Helper()
	for _, name := range []string{"history-lab", "OBJ_xfer-lab"} {
		if !f.streamExists(t, name) {
			t.Fatalf("%s was deleted although the removal did not complete", name)
		}
	}
	if got := f.state(t); got != string(session.StateDeleting) {
		t.Fatalf("session left state %q, want the DELETING tombstone", got)
	}
}

// Before S7, a removal whose phase ②③ failed still answered OK: the tombstone
// already revokes every ingress, and the boot DELETING resumer finishes the
// cascade. P19 mapped EVERY finalize error to rc75 "retry after settlement",
// so a JetStream or SQL hiccup told the operator to wait for a settlement that
// either had already happened or does not exist on this broker.
// origin: hot-update S7 plan §6.4 / lead LQ1; S7 lead integration decision 1
// origin: S7 internal review BROKER-REDS F3; hot-update S8 plan §6.2 (valid creation material)
func TestSessionRmAfterSettlementSucceedsDespiteCascadeFailure(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	barriers := []struct {
		name    string
		barrier func() *recordingBarrier
	}{
		{"no_continuation_barrier", func() *recordingBarrier { return nil }},
		{"barrier_released", func() *recordingBarrier {
			return &recordingBarrier{decide: func(bool) error { return nil }}
		}},
	}
	faults := []struct {
		name          string
		inject, clear func(*testing.T, *rmFixture)
		historyKept   bool
	}{
		{name: "jetstream_history_delete_fails", historyKept: true,
			inject: func(_ *testing.T, f *rmFixture) { f.js.fail.Store(true) },
			clear:  func(_ *testing.T, f *rmFixture) { f.js.fail.Store(false) }},
		{name: "sql_row_delete_fails",
			inject: func(t *testing.T, f *rmFixture) {
				if _, err := f.db.Exec(`CREATE TRIGGER rm_fault BEFORE DELETE ON sessions BEGIN SELECT RAISE(ABORT, 'injected: session row delete unavailable'); END`); err != nil {
					t.Fatal(err)
				}
			},
			clear: func(t *testing.T, f *rmFixture) {
				if _, err := f.db.Exec(`DROP TRIGGER rm_fault`); err != nil {
					t.Fatal(err)
				}
			}},
	}
	for _, bc := range barriers {
		for _, fc := range faults {
			t.Run(bc.name+"/"+fc.name, func(t *testing.T) {
				barrier := bc.barrier()
				var port xferauth.SessionBarrier
				if barrier != nil {
					port = barrier
				}
				f := newRmFixture(t, port)
				fc.inject(t, f)
				r := requestSessionRm(t, f.nc, f.actor, `{}`)
				if !r.OK || r.Code != "" {
					t.Fatalf("a phase ②③ failure after settlement was reported as %+v; before S7 it was OK "+
						"and the boot DELETING resumer finished the job", r)
				}
				if barrier != nil && !slices.Equal(barrier.calls(), []bool{false}) {
					t.Fatalf("the barrier was not consulted exactly once before the cascade: %v", barrier.calls())
				}
				if got := f.state(t); got != string(session.StateDeleting) {
					t.Fatalf("the failed cascade left state %q, want the DELETING tombstone for the resumer", got)
				}
				// The fault really fired at the phase it names: otherwise OK proves nothing.
				if got := f.streamExists(t, "history-lab"); got != fc.historyKept {
					t.Fatalf("history-lab exists=%v after the injected fault, want %v", got, fc.historyKept)
				}
				fc.clear(t, f)
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				if err := f.b.reconcileHistoryStreamsOnBoot(ctx); err != nil {
					t.Fatal(err)
				}
				f.assertGone(t)
			})
		}
	}
}

// A removal of a session that is already DELETING answered already_deleting
// before S7 and did nothing else. P19 re-drove the whole finalize instead, so
// a broker with no continuation at all replied OK (or rc75) and a retry could
// never again tell the operator that the tombstone was already there. LQ1
// keeps that answer for ungoverned retries. The documented LQ1 flow (repeat
// the removal, optionally with explicit abandonment) must return OK once the
// barrier releases the session and the retry drives the cascade.
// origin: hot-update S7 plan §6.4 / lead LQ1; S7 lead integration decision 2
// origin: S7 internal review BROKER-REDS F3; hot-update S8 plan §6.2 (valid creation material)
func TestSessionRmOnADeletingSessionKeepsTheAlreadyDeletingContract(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	held := &xferauth.PendingError{Transfers: []xferauth.PendingTransfer{{TransferID: "held-transfer", Reason: "receiver_commit_unsettled"}}}
	cases := []struct {
		name     string
		decide   func(abandon bool) error // nil: no continuation barrier at all
		body     string
		wantOK   bool
		wantCode string
		detail   []string
		deleted  bool
		seen     []bool
	}{
		{name: "no_continuation_barrier", body: `{}`, wantCode: "already_deleting"},
		{name: "no_continuation_barrier_with_flag", body: `{"abandon_unsettled_transfers":true}`, wantCode: "already_deleting"},
		{name: "unsettled_transfer", body: `{}`, decide: func(bool) error { return held },
			wantCode: xfercontract.CodeOwnerRecovering, detail: []string{"held-transfer", "receiver_commit_unsettled"}, seen: []bool{false}},
		{name: "unsettled_census", body: `{}`, decide: func(bool) error { return errors.New("census transport down") },
			wantCode: xfercontract.CodeOwnerRecovering, detail: []string{"retry after settlement"}, seen: []bool{false}},
		{name: "settled_since_the_tombstone", body: `{}`, decide: func(bool) error { return nil },
			// origin: hot-update S7 plan §6.4 / lead LQ1; S7 RM-RETRY lead decision
			wantOK: true, deleted: true, seen: []bool{false}},
		{name: "explicit_abandonment", body: `{"abandon_unsettled_transfers":true}`,
			decide: func(abandon bool) error {
				if abandon {
					return nil
				}
				return held
			},
			wantOK: true, deleted: true, seen: []bool{true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var barrier *recordingBarrier
			var port xferauth.SessionBarrier
			if tc.decide != nil {
				barrier = &recordingBarrier{decide: tc.decide}
				port = barrier
			}
			f := newRmFixture(t, port)
			// An earlier removal (an rc75, or a crash before phase ②) left the tombstone.
			if err := session.Tombstone(f.db, "lab", time.Now()); err != nil {
				t.Fatal(err)
			}
			r := requestSessionRm(t, f.nc, f.actor, tc.body)
			if r.OK != tc.wantOK || r.Code != tc.wantCode {
				t.Fatalf("removal of a DELETING session answered %+v, want ok=%v code=%q", r, tc.wantOK, tc.wantCode)
			}
			for _, want := range tc.detail {
				if !strings.Contains(r.Error, want) {
					t.Fatalf("rc75 detail lost %q: %q", want, r.Error)
				}
			}
			if tc.deleted {
				f.assertGone(t)
			} else {
				f.assertRetained(t)
			}
			if barrier != nil && !slices.Equal(barrier.calls(), tc.seen) {
				t.Fatalf("barrier saw abandonment flags %v, want %v", barrier.calls(), tc.seen)
			}
		})
	}
}

// In cluster mode a follower forwards the tombstone to the leader. The leader's
// answer is the authoritative state; the follower's own replica may not have
// applied it yet. P19 re-read `SELECT state` from that replica before settling,
// found ACTIVE, and reported a spurious rc75 for a tombstone the cluster had
// already committed. Here the follower's database never receives any write
// after the session was created, which is the extreme of that lag.
// origin: hot-update S7 plan §6.4 / lead LQ1; S7 lead integration decision 3
// origin: S7 internal review BROKER-REDS F3; hot-update S8 plan §6.2 (valid creation material)
func TestSessionRmOnAFollowerActsOnTheForwardedTombstone(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	for _, tc := range []struct {
		name    string
		barrier xferauth.SessionBarrier
	}{
		{"no_continuation_barrier", nil},
		{"barrier_released", &recordingBarrier{decide: func(bool) error { return nil }}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader, _ := d7SingleNode(t, "leader-1")
			actor := freshUserActor(t)
			fp, err := auth.FingerprintFromActor(actor)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			if err := leader.Propose(func(db *sql.DB) (*cluster.Command, error) {
				return session.PlanCreate(db, "lab", "lab", fp, brokerAuthorityPIN(1), now)
			}); err != nil {
				t.Fatalf("seed session on the leader: %v", err)
			}
			url := testharness.StartNATS(t)
			lnc, err := nats.Connect(url)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(lnc.Close)
			apply, err := SubscribeClusterApply(lnc, leader, time.Now, silentLogger(),
				(&authcallout.Handler{Now: time.Now}).VerifyPINWithBudget)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = apply.Unsubscribe() })
			if err := lnc.Flush(); err != nil {
				t.Fatal(err)
			}
			fnc, err := nats.Connect(url)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(fnc.Close)
			// The follower's replica applied the create and nothing after it.
			replica := openDB(t)
			if _, err := session.Create(replica, "lab", "lab", fp, brokerAuthorityPIN(1), now); err != nil {
				t.Fatal(err)
			}
			b := &Broker{cfg: Config{Logger: silentLogger(), Now: time.Now, DB: replica, TransferSettlement: tc.barrier}}
			b.clusterMode = true
			b.cl = &clusterRuntime{node: nonLeaderNode(t, "follower-1"), forwarder: NewForwarder(fnc, 3*time.Second)}
			sub, err := fnc.Subscribe(proto.SubjCtrlSessionRm(actor, "lab"), b.handleSessionRm)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sub.Unsubscribe() })
			if err := fnc.Flush(); err != nil {
				t.Fatal(err)
			}
			r := requestSessionRm(t, fnc, actor, `{}`)
			if !r.OK || r.Code != "" {
				t.Fatalf("follower answered %+v for a tombstone the leader committed; its own replica is "+
					"not the authority for a write it forwarded", r)
			}
			var count int
			if err := leader.RODB().QueryRow(`SELECT COUNT(*) FROM sessions WHERE sid='lab'`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatal("the forwarded removal did not drop the session row on the leader")
			}
			// THE PREMISE: the follower acted without its replica ever seeing the tombstone.
			var state string
			if err := replica.QueryRow(`SELECT state FROM sessions WHERE sid='lab'`).Scan(&state); err != nil || state != string(session.StateActive) {
				t.Fatalf("premise broken: the follower replica is %q (%v), not the lagging ACTIVE row", state, err)
			}
		})
	}
}
