// proc_delivery.go (h1 C) — reliable delivery of proc lifecycle events.
//
// THE DEFECT THIS FILE REMOVES (zombie class a, 2026-08-04 incident /
// docs/reviews/h1-plan.md workstream C): handleRunForwarded and
// handleExecForwarded captured the SPAWN-TIME *nats.Conn for the child's whole
// lifetime and fire-and-forget published proc.exit on it. After the agent
// rebuilt its NATS conn (any reconnect), every long-running child's eventual
// exit publish failed `nats: connection closed`, was WARN-logged, and was
// GONE — the broker row stayed RUNNING until an agent re-register happened to
// run G.1, which could be days later or never. Operators watched `tether ps`
// fill with 30+ zombie `tmux attach` rows.
//
// THE SHAPE OF THE FIX — a courier, not a journal:
//
//   - proc started/exit events are ENQUEUED in memory and delivered by ONE
//     courier goroutine that loads the CURRENT conn (a.ncBox) per attempt.
//   - delivery is an ACKed request (same ev.proc subjects — zero new
//     subjects/ACL; the broker responds proto.ProcEventAck when a Reply inbox
//     is present, and an old agent that plain-publishes sees byte-identical
//     behavior). Entries are removed only on a positive ack.
//   - the register snapshot is the replay channel: pending EXITS ride
//     NodeRegisterReq.LocalProcesses as State:"exited" rows, so one register
//     round-trip settles everything a broker outage dropped. Clearance is
//     driven by the reply (see onRegisterSuccess).
//   - G.1's missed-exit(-1) stays as the last-resort backstop (agent-process
//     crash loses the in-memory queue; the plan's adjudication C-6 cut the
//     on-disk journal — an rc lost to a full agent crash degrades to the
//     documented G.1 semantics, which is today's behavior).
//
// OLD-BROKER DEMOTION (plan critique-2 BLOCKER): against a pre-h1 broker the
// ACK request is, on the wire, just a publish with an ignored reply inbox —
// the broker PROCESSES the event but never answers. Retrying would deliver
// (and audit) the same exit every backoff interval forever. After
// courierParkAfter consecutive request timeouts on a CONNECTED conn the entry
// is parked as register-replay-only. The rollout runbook orders broker before
// agents, so this belt exists for the skew window, not the steady state.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// courierRequestTimeout is a var (not const) ONLY so the package tests can
// shrink it — the park-after-3-silent-timeouts belt would otherwise cost 15s
// of wall clock per test. Production never mutates it.
var courierRequestTimeout = 5 * time.Second

const (
	courierRoundBudget = 8 // max requests per round (plan critique-2: >12 stuck entries must not starve fresh exits)
	courierBackoffBase = 2 * time.Second
	courierBackoffCap  = 60 * time.Second
	courierParkAfter   = 3 // consecutive connected-timeouts → register-replay-only
	courierMaxPending  = 4096
	courierMaxLifetime = 24 * time.Hour // belt: nothing waits forever
)

type procEventKind string

const (
	procEventStarted procEventKind = "started"
	procEventExit    procEventKind = "exit"
)

type pendingProcEvent struct {
	pid        string
	kind       procEventKind
	payload    []byte // marshaled ProcStartedEvent / ProcExitEvent
	enqueuedAt time.Time
	next       time.Time // earliest next attempt (backoff)
	fails      int
	silent     int  // consecutive connected-timeouts (old-broker signature)
	parked     bool // register-replay-only
	rc         int  // exit only — for the register snapshot row
	endedAt    time.Time
}

// procCourier owns the pending set + the delivery goroutine. All methods hang
// HERE, not on Agent (the type-methods ledger pins Agent's count; h1 plan X-2).
type procCourier struct {
	a  *Agent
	mu sync.Mutex
	// pending is keyed pid+"/"+kind: a pid can owe both a started and an exit.
	pending map[string]*pendingProcEvent
	nudge   chan struct{}
	// lastOverflowWarn rate-limits the drop warning to 1/min.
	lastOverflowWarn time.Time
}

func newProcCourier(a *Agent) *procCourier {
	return &procCourier{
		a:       a,
		pending: make(map[string]*pendingProcEvent),
		nudge:   make(chan struct{}, 1),
	}
}

func courierKey(pid string, kind procEventKind) string { return pid + "/" + string(kind) }

// enqueueStarted queues a proc.started event. Payload fields are captured at
// enqueue time (the event describes the spawn instant, not the send instant).
func (c *procCourier) enqueueStarted(pid string, argv []string, actorFP, bootID string, startTimeTicks int64, startedAt time.Time) {
	payload, err := json.Marshal(proto.ProcStartedEvent{
		PID: pid, Argv: argv, StartedAt: startedAt, StartedByFP: actorFP,
		BootID: bootID, StartTimeTicks: startTimeTicks,
	})
	if err != nil {
		return
	}
	c.add(&pendingProcEvent{pid: pid, kind: procEventStarted, payload: payload, enqueuedAt: time.Now()})
}

// enqueueExit queues a proc.exit event. NOTE the documented gap: run.go calls
// unregisterProc BEFORE this enqueue, so a register snapshot built between
// the two omits the pid entirely and G.1 marks -1 — a pre-existing window,
// deliberately NOT "fixed" here (closing it by enqueueing first would let
// the same exit be reported twice: snapshot row + live courier request).
func (c *procCourier) enqueueExit(pid string, exitCode int) {
	endedAt := time.Now().UTC()
	payload, err := json.Marshal(proto.ProcExitEvent{PID: pid, ExitCode: exitCode, EndedAt: endedAt})
	if err != nil {
		return
	}
	c.add(&pendingProcEvent{
		pid: pid, kind: procEventExit, payload: payload,
		enqueuedAt: time.Now(), rc: exitCode, endedAt: endedAt,
	})
}

func (c *procCourier) add(ev *pendingProcEvent) {
	c.mu.Lock()
	if len(c.pending) >= courierMaxPending {
		c.evictOneLocked()
	}
	c.pending[courierKey(ev.pid, ev.kind)] = ev
	c.mu.Unlock()
	select {
	case c.nudge <- struct{}{}:
	default:
	}
}

// evictOneLocked drops the oldest STARTED entry first, and only if none
// exist the oldest exit — exits carry the zombie-row cure, started rows are
// recoverable from the live snapshot (drop-oldest-exit-last).
func (c *procCourier) evictOneLocked() {
	var victim string
	var victimAt time.Time
	pick := func(kind procEventKind) bool {
		found := false
		for k, ev := range c.pending {
			if ev.kind != kind {
				continue
			}
			if !found || ev.enqueuedAt.Before(victimAt) {
				victim, victimAt, found = k, ev.enqueuedAt, true
			}
		}
		return found
	}
	if !pick(procEventStarted) {
		if !pick(procEventExit) {
			return
		}
	}
	delete(c.pending, victim)
	if now := time.Now(); now.Sub(c.lastOverflowWarn) > time.Minute {
		c.lastOverflowWarn = now
		c.a.cfg.Logger.Warn("agent: proc-event queue overflow — dropping oldest entry",
			"dropped", victim, "cap", courierMaxPending)
	}
}

// run is the courier goroutine: one per agent lifetime, ctx-bound (leak-gate
// covered). It wakes on enqueue nudges and on a 1s tick (backoff expiries).
// ctx-root: agent Run's background delivery loop.
func (c *procCourier) run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.nudge:
		case <-t.C:
		}
		c.deliverRound(ctx)
	}
}

// deliverRound attempts up to courierRoundBudget due entries: every due
// STARTED first, then exits whose pid has no pending started (per-PID
// ordering — a broker must never see an exit for a process it was never told
// started, or the insert-side dedup would misfire).
func (c *procCourier) deliverRound(ctx context.Context) {
	nc := c.a.ncBox.Load()
	if nc == nil || !nc.IsConnected() {
		return
	}
	now := time.Now()

	c.mu.Lock()
	// Only a LIVE (un-parked) started holds its pid's exit back. A PARKED
	// started is never attempted again — counting it here would block that
	// pid's exit forever, which against a pre-h1 broker is strictly WORSE
	// than the fire-and-forget it replaced: the exit would never be
	// couriered at all (internal review, wire + failure lenses). The parked
	// started still rides the register snapshot, and the exit's own snapshot
	// row settles the ps state, so ordering is preserved where it can be and
	// abandoned where holding it would lose the event entirely.
	startedPending := map[string]bool{}
	for _, ev := range c.pending {
		if ev.kind == procEventStarted && !ev.parked {
			startedPending[ev.pid] = true
		}
	}
	var due []*pendingProcEvent
	for _, ev := range c.pending {
		// The max-lifetime belt is checked BEFORE the parked/backoff skips
		// (internal review): parked entries are exactly the ones that can sit
		// for days, so exempting them from the "nothing waits forever" bound
		// would make the belt unreachable for its most likely subject.
		if now.Sub(ev.enqueuedAt) > courierMaxLifetime {
			delete(c.pending, courierKey(ev.pid, ev.kind))
			continue
		}
		if ev.parked || now.Before(ev.next) {
			continue
		}
		if ev.kind == procEventExit && startedPending[ev.pid] {
			continue // started strictly first for the same pid
		}
		due = append(due, ev)
	}
	// started before exit inside the budget.
	for i, j := 0, 0; i < len(due); i++ {
		if due[i].kind == procEventStarted {
			due[i], due[j] = due[j], due[i]
			j++
		}
	}
	if len(due) > courierRoundBudget {
		due = due[:courierRoundBudget]
	}
	c.mu.Unlock()

	for _, ev := range due {
		if ctx.Err() != nil {
			return
		}
		c.attempt(ctx, nc, ev)
	}
}

// attempt sends one ACKed request and settles the entry's fate.
func (c *procCourier) attempt(ctx context.Context, nc *nats.Conn, ev *pendingProcEvent) {
	subj := proto.SubjEvProc(c.a.cfg.SID, c.a.cfg.NID, ev.pid, string(ev.kind))
	rctx, cancel := context.WithTimeout(ctx, courierRequestTimeout)
	msg, err := nc.RequestWithContext(rctx, subj, ev.payload)
	cancel()

	c.mu.Lock()
	defer c.mu.Unlock()
	key := courierKey(ev.pid, ev.kind)
	if _, still := c.pending[key]; !still {
		return // cleared concurrently (register clearance)
	}
	if err == nil {
		var ack proto.ProcEventAck
		if jerr := json.Unmarshal(msg.Data, &ack); jerr == nil && ack.OK {
			delete(c.pending, key)
			return
		}
		// A decodable negative ack (store_error / leader_unavailable) or
		// garbage: transient — back off and retry. A negative ack proves the
		// broker is NEW (it answered), so the parked counter resets.
		ev.silent = 0
		c.backoffLocked(ev)
		return
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded) && nc.IsConnected():
		// The old-broker signature: the event was almost certainly PROCESSED
		// (the subject is the plain ev.proc subject an old broker consumes)
		// but nothing will ever answer. Hammering on would re-publish the
		// same audit.proc record every backoff interval forever.
		//
		// EXITS ONLY (internal review, wire lens). A parked STARTED would be
		// catastrophic against a NEW broker that merely timed out three times
		// (a 1-vCPU leader under the GC backlog drain, a leadership blip):
		// the started never lands, the broker has no row, and the very next
		// register makes G.1's orphan pass KILL the user's live process —
		// the snapshot lists a pid the broker cannot account for. A retried
		// started, by contrast, is harmless in both directions: the insert is
		// INSERT OR IGNORE and the broker's own dedup pre-read suppresses the
		// duplicate audit. So a started keeps retrying until it lands or the
		// register accepts it.
		ev.silent++
		if ev.kind == procEventExit && ev.silent >= courierParkAfter {
			ev.parked = true
			c.a.cfg.Logger.Warn("agent: proc-event ack never arrived — parking exit for register-replay "+
				"(pre-h1 broker, or a wedged responder)", "pid", ev.pid)
			return
		}
		c.backoffLocked(ev)
	default:
		// No responders / conn churn / ctx cancel: plain transient.
		c.backoffLocked(ev)
	}
}

func (c *procCourier) backoffLocked(ev *pendingProcEvent) {
	delay := courierBackoffBase << uint(min(ev.fails, 10))
	if delay > courierBackoffCap {
		delay = courierBackoffCap
	}
	ev.fails++
	ev.next = time.Now().Add(delay)
}

// drainStarted makes one bounded pass over pending STARTED entries before a
// register (h1 plan C4): a started event that never landed makes the next
// register's G.1 orphan pass KILL the live process, so we spend up to ONE
// request timeout getting them in ahead of the snapshot. Best-effort — a
// failure falls through to register (whose AcceptedProcesses clearance and
// orphan semantics are today's).
func (c *procCourier) drainStarted(ctx context.Context, nc *nats.Conn) {
	if nc == nil || !nc.IsConnected() {
		return
	}
	c.mu.Lock()
	var started []*pendingProcEvent
	for _, ev := range c.pending {
		if ev.kind == procEventStarted && !ev.parked {
			started = append(started, ev)
		}
	}
	c.mu.Unlock()
	if len(started) == 0 {
		return
	}
	dctx, cancel := context.WithTimeout(ctx, courierRequestTimeout)
	defer cancel()
	for _, ev := range started {
		if dctx.Err() != nil {
			return
		}
		c.attempt(dctx, nc, ev)
	}
}

// onRegisterSuccess is the replay-channel settlement (h1 plan C4). The
// snapshot carried every pending exit as a State:"exited" row, so after a
// successful register:
//
//   - an EXIT entry whose pid is ABSENT from resp.AcceptedProcesses is done:
//     the broker no longer believes the pid is RUNNING — it either reconciled
//     the snapshot row to EXITED(rc) or already had it terminal. (Membership
//     in ReconciledProcesses would be the WRONG rule — plan critique-2: an
//     old broker that processed a couriered request via the plain subject
//     marks the row EXITED and reports the pid in NEITHER list, which would
//     strand the entry forever.)
//   - a STARTED entry whose pid IS in AcceptedProcesses is done (the broker
//     acknowledged the row as live).
//
// Parked entries settle here too — this is exactly the register-replay they
// were parked for.
func (c *procCourier) onRegisterSuccess(resp proto.NodeRegisterResp) {
	accepted := make(map[string]bool, len(resp.AcceptedProcesses))
	for _, pid := range resp.AcceptedProcesses {
		accepted[pid] = true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, ev := range c.pending {
		switch ev.kind {
		case procEventExit:
			if !accepted[ev.pid] {
				delete(c.pending, key)
			}
		case procEventStarted:
			if accepted[ev.pid] {
				delete(c.pending, key)
			}
		}
	}
}

// pendingExitSnapshot returns the State:"exited" rows the register snapshot
// carries (the replay channel's send half).
func (c *procCourier) pendingExitSnapshot() []proto.LocalProcess {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]proto.LocalProcess, 0)
	for _, ev := range c.pending {
		if ev.kind != procEventExit {
			continue
		}
		rc := ev.rc
		ended := ev.endedAt
		out = append(out, proto.LocalProcess{
			PID: ev.pid, State: "exited", RC: &rc, EndedAt: &ended,
		})
	}
	return out
}
