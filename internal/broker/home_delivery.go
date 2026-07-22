package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// home_delivery.go (R8a) — the ACTIVE home-directive delivery channel.
//
// THE BUG THIS EXISTS TO KILL
// ---------------------------
// Before R8 there was EXACTLY ONE way a home directive reached an agent: the
// NodeRegisterResp.Home field built by homeForRegister (home.go). Its own comment
// said it "drives a rehome on the next reconnect", and the agent registers ONCE
// per NATS connection (agent.go: "registers ONCE and heartbeat is fire-and-forget"),
// so the delivery was gated on the agent producing a reconnect event.
//
// `cluster drain` writes port_allocations.home_broker through raft and returns
// rc=0 — it does not stop nats-server, does not disconnect the agent, and does not
// notify anyone. A silent agent therefore NEVER learns its expose moved, and the
// data plane stays pinned to the drained broker indefinitely. Control plane says
// done; data plane never follows.
//
// THE ONE-VOTE-VETO INVARIANT OF THIS BATCH
// -----------------------------------------
//	Delivery of a home/roster change must NOT be conditional on the peer
//	producing any event.
//
// Operationally: with the agent COMPLETELY silent (no reconnect, no restart, no
// command), the data plane must follow a drain within a BOUNDED time. Every design
// choice below is downstream of that sentence.
//
// SHAPE: A RECONCILE PASS, NOT AN EDGE
// ------------------------------------
// The delivery is registered as an R7a reconciliation pass ("home-delivery"), so
// it is level-triggered and re-runs forever on a fixed cadence. An edge fired from
// inside DrainNode would have exactly the failure mode we are fixing: one publish,
// dropped on the floor if the agent was momentarily unreachable, never retried.
//
// The pass obeys the R7a one-vote-veto invariant literally:
//
//	(1) EXPECTED state  = homeForRegister(sid, nid) — the SAME builder the register
//	    reply uses. This pass invents no directive of its own; it re-delivers what
//	    the product already computes. That is why it cannot drift from the register
//	    path: there is one builder.
//	(2) ACTUAL state    = the highest home epoch the AGENT has confirmed APPLIED for
//	    that public port (see the ack channel below).
//	(3) idempotent path = agent.applyHomeDirectives, the very function the register
//	    reply drives. It is epoch-monotone and same-epoch-non-tearing by contract.
//
// Converged (actual == expected for every directive) ⇒ the pass publishes NOTHING.
//
// THE ACK IS AN *APPLIED* ACK, NOT A DELIVERY ACK
// -----------------------------------------------
// A "I received your message" ack would let the pass go quiet while the rehome was
// still failing — i.e. it would report convergence that does not exist, which is
// the same class of lie as rc=0 on an unconverged drain. So the agent acks from the
// SUCCESS path of applyOneHome, after ApplyHome returned nil AND the tunnel session
// exists AND the new home was persisted. No success, no ack, and the next pass tick
// re-delivers.
//
// Transport (SR-8): the broker sends ONE push PER DIRECTIVE to the agent's per-node
// forwarded-command subject (verb "home"), each carrying a fresh SINGLE-USE ack token as
// its Reply (a child of the broker's _INBOX wildcard subscription). Agents are already
// granted Sub on `cmd.node.<nid>.*.req.forwarded` and Pub on `_INBOX.>`
// (internal/auth/permissions.go), so this needs no ACL change and no new subject builder.
// The per-directive token is what keeps the ack UNFORGEABLE across sessions even though it
// rides the shared _INBOX bus that every agent may Sub: a token is disclosed only when its
// OWN port is acked, and is consumed at that instant, so a disclosed token names only an
// already-converged port and authorizes nothing. Publishing (not Request-ing) keeps the
// pass non-blocking: a slow or islanded agent can never stall the broker's single ticker.

// homeDeliveryVerb is the forwarded-command verb carrying a proto.HomeAssignment
// push. It must match the agent's dispatchForwarded case; TestHomeDeliveryVerbIsWireStable
// pins both sides.
const homeDeliveryVerb = "home"

// homeDeliveryMaxBackoff caps the per-node re-delivery backoff. A node that never
// acks (a pre-R8 agent, or one islanded on a retired broker's nats-server) must not
// be re-pushed every tick forever, but must also never be abandoned — 5 min mirrors
// reconcileMaxBackoff. The backoff is RESET the moment the EXPECTED state changes
// (a new drain mints a new epoch), so a fresh drain never inherits a stale backoff:
// that is what keeps "bounded time" bounded by the pass interval, not by the cap.
const homeDeliveryMaxBackoff = 5 * time.Minute

// homeAckTokenTTL bounds how long an issued applied-ack token stays valid. It must
// outlast the agent's apply+ack round trip (which includes a tunnel re-dial), and a
// token is superseded by the next re-delivery's fresh token anyway, so a generous cap
// mirroring the delivery backoff cap keeps the outstanding map bounded without racing
// a slow apply.
const homeAckTokenTTL = homeDeliveryMaxBackoff

// homeDeliveryState is the broker-local delivery bookkeeping. It is NOT replicated:
// it is a leader-local convergence cache, exactly like proxyDwell. A leadership
// change or a restart empties it, which makes the new leader re-deliver everything
// once and re-learn the applied set from the acks — level-triggered, self-healing,
// and cheap (a same-epoch ApplyHome is a documented non-tearing no-op).
type homeDeliveryState struct {
	mu sync.Mutex

	// applied maps public port → the highest home epoch the agent has CONFIRMED
	// applied. Bounded by the number of ALLOCATED homed exposes; pruned by the pass.
	applied map[int]int64

	// attempts maps sid|nid → the re-delivery backoff for that node.
	attempts map[string]*homeDeliveryAttempt

	// outstanding maps a single-use ack token → the directives THIS broker actually
	// issued under it (public port → the epoch we asked the agent to reach), plus an
	// expiry. handleHomeAck advances the applied epoch for a port ONLY when the ack
	// arrives on a token in this map and names a port the token carried — the token was
	// published solely to the OWNING agent's private forwarded subject, so a different
	// authenticated agent (which can Pub to _INBOX.> but never SAW the token) cannot
	// forge another session's convergence, and a tampered high epoch in the ack body is
	// ignored in favour of the epoch we recorded (external review C1/M-1). Consumed on
	// use, expired by TTL ⇒ bounded.
	outstanding map[string]*homeOutstanding

	// inbox is the broker-owned _INBOX base subject; ack tokens are its children
	// (inbox.<token>). Empty until subscribeHomeAcks runs (single mode never runs it).
	inbox string

	// pushes counts the per-directive pushes actually published (SR-8: one push per
	// directive, so a multi-expose agent's delivery increments this by N). It is the
	// observability counter AND the mutation-test hook: a test that disables the
	// delivery must see this stay at zero.
	pushes uint64

	// acks counts applied-acks accepted from agents.
	acks uint64
}

type homeDeliveryAttempt struct {
	want    string        // fingerprint of the EXPECTED assignment this backoff belongs to
	nextAt  time.Time     // earliest next publish
	backoff time.Duration // current backoff step
}

// homeOutstanding records what a single push asked one agent to reach, keyed by the
// single-use token the push carried in its Reply subject.
type homeOutstanding struct {
	expires time.Time
	dirs    map[int]int64 // public port → the epoch this broker asked the agent to reach
}

func newHomeDeliveryState() *homeDeliveryState {
	return &homeDeliveryState{
		applied:     map[int]int64{},
		attempts:    map[string]*homeDeliveryAttempt{},
		outstanding: map[string]*homeOutstanding{},
	}
}

// homeDelivery returns the broker's delivery state, creating it on first use so a
// zero-value Broker (the unit-test shape used all over this package) works without
// a constructor change.
func (b *Broker) homeDelivery() *homeDeliveryState {
	b.homeDeliveryOnce.Do(func() { b.homeDeliveryState = newHomeDeliveryState() })
	return b.homeDeliveryState
}

// now is the broker clock, defaulting to time.Now for a zero-value Broker (the
// unit-test shape) whose cfg.Now was never wired.
func (b *Broker) now() time.Time {
	if b.cfg.Now != nil {
		return b.cfg.Now()
	}
	return time.Now()
}

// --- the ack channel -------------------------------------------------------

// subscribeHomeAcks installs the broker-owned _INBOX subscription agents publish
// their APPLIED acks to, and records its subject so pushHomeAssignment can set it
// as the Reply. Cluster mode only (single mode emits no home directives at all).
//
// One long-lived subscription, no goroutine of our own (nats.go's own reader
// dispatches the handler), so the repo's NumGoroutine/fd leak gate sees a constant.
func (b *Broker) subscribeHomeAcks(nc *nats.Conn) (*nats.Subscription, error) {
	hd := b.homeDelivery()
	inbox := nats.NewInbox()
	// Wildcard on the token children (inbox.<token>): one long-lived subscription, no
	// goroutine of our own, so the leak gate still sees a constant — while the reply
	// subject a push carries is a fresh single-use child that only the OWNING agent is
	// told about (external review C1/M-1).
	sub, err := nc.Subscribe(inbox+".>", b.handleHomeAck)
	if err != nil {
		return nil, fmt.Errorf("broker: subscribe home acks: %w", err)
	}
	hd.mu.Lock()
	hd.inbox = inbox
	hd.mu.Unlock()
	return sub, nil
}

// newAckToken mints an unguessable single-use ack token. crypto/rand so a peer that
// never saw the token cannot predict it; an empty return (rand failure, ~never)
// degrades the push to an un-acked re-delivery loop — fail-closed, not fail-open.
func newAckToken() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(buf[:])
}

// recordHomeOutstanding mints a token, records the directives THIS push issued under
// it (so an ack can be correlated to something this broker actually asked for), and
// opportunistically prunes expired tokens. Returns "" if a token could not be minted
// (the caller then delivers without an ack channel).
func (b *Broker) recordHomeOutstanding(dirs []proto.HomeDirective, now time.Time) string {
	token := newAckToken()
	if token == "" {
		return ""
	}
	m := make(map[int]int64, len(dirs))
	for _, d := range dirs {
		if d.PublicPort > 0 {
			m[d.PublicPort] = d.Epoch
		}
	}
	hd := b.homeDelivery()
	hd.mu.Lock()
	if hd.outstanding == nil {
		hd.outstanding = map[string]*homeOutstanding{}
	}
	hd.outstanding[token] = &homeOutstanding{expires: now.Add(homeAckTokenTTL), dirs: m}
	for k, o := range hd.outstanding {
		if now.After(o.expires) {
			delete(hd.outstanding, k)
		}
	}
	hd.mu.Unlock()
	return token
}

// pruneHomeOutstanding drops expired ack tokens so a never-acking (islanded) agent's
// re-deliveries cannot pin unbounded token state. Called from the pass.
func (b *Broker) pruneHomeOutstanding(now time.Time) {
	hd := b.homeDelivery()
	hd.mu.Lock()
	defer hd.mu.Unlock()
	for k, o := range hd.outstanding {
		if now.After(o.expires) {
			delete(hd.outstanding, k)
		}
	}
}

// handleHomeAck records an agent's APPLIED ack. The payload is a
// proto.HomeAssignment echoing ONLY the directives the agent actually installed
// (agent side: the applyOneHome success path), so decoding needs no new wire type.
//
// AUTHENTICITY (external review C1/M-1, SR-8): the ack is honored ONLY when it arrives
// on a token THIS broker issued (the reply subject's final segment) and names the ONE
// port that token carried. Each token is minted PER DIRECTIVE and consumed on its first
// ack (single-use). The security rests on that, NOT on the token staying secret: the
// ack rides the broker _INBOX, which every agent may `Sub _INBOX.>`, so a token IS
// disclosed on the shared bus the instant its port is acked — but by then it is already
// consumed and named only that one (now-converged) port, so a disclosed token authorizes
// nothing (a forged ack for any OTHER port carries a DIFFERENT, still-secret token this
// attacker never saw). The applied epoch is advanced to min(ISSUED, acked) — capped at
// issued so a tampered high epoch cannot inflate it, and no higher than the agent's
// actual ack so a stale in-flight ack cannot over-credit. Monotone; a replay is a no-op.
func (b *Broker) handleHomeAck(msg *nats.Msg) {
	var ha proto.HomeAssignment
	if err := json.Unmarshal(msg.Data, &ha); err != nil {
		b.cfg.Logger.Warn("broker: home ack parse", "err", err)
		return
	}
	if len(ha.Directives) == 0 {
		return
	}
	hd := b.homeDelivery()
	hd.mu.Lock()
	defer hd.mu.Unlock()
	token := strings.TrimPrefix(msg.Subject, hd.inbox+".")
	o := hd.outstanding[token]
	if o == nil || b.now().After(o.expires) {
		delete(hd.outstanding, token) // an absent/expired token is not evidence of anything
		return
	}
	for _, d := range ha.Directives {
		if d.PublicPort <= 0 {
			continue
		}
		issued, ok := o.dirs[d.PublicPort]
		if !ok {
			continue // this port was not part of THIS token's issued directive set
		}
		// The agent acks PER PORT — one message per directive, ALL on this push's single reply
		// subject (home_push.go: rehomeAckTo[port]=msg.Reply for every port). So a multi-expose
		// push produces N acks on the SAME token; consuming the token on the FIRST would drop the
		// rest and strand every-but-one port's convergence. Consume PER PORT instead (drop the port
		// from this token's set; delete the token only once every port it carried is confirmed).
		delete(o.dirs, d.PublicPort)
		// Advance to min(ISSUED, ACKED): capping at issued blocks epoch INFLATION (a tampered ack
		// body can never push applied past what we asked for); taking the acked value blocks
		// OVER-CREDITING a stale in-flight ack that landed on a newer token (the agent only truly
		// reached the epoch it acked). Monotone: a re-ordered/duplicate ack can't walk it backwards.
		confirmed := issued
		if d.Epoch < confirmed {
			confirmed = d.Epoch
		}
		if confirmed <= 0 {
			continue
		}
		if cur, seen := hd.applied[d.PublicPort]; !seen || confirmed > cur {
			hd.applied[d.PublicPort] = confirmed
		}
		hd.acks++
	}
	if len(o.dirs) == 0 {
		delete(hd.outstanding, token) // fully confirmed — free it now (else TTL prunes a partial one)
	}
}

// homeAppliedEpoch reports the highest home epoch the agent confirmed applied for
// publicPort (0 == nothing confirmed). It is the ACTUAL half of the pass's compare
// and the oracle behind the drain/retire/upgrade rc semantics.
func (b *Broker) homeAppliedEpoch(publicPort int) int64 {
	hd := b.homeDelivery()
	hd.mu.Lock()
	defer hd.mu.Unlock()
	return hd.applied[publicPort]
}

// homeDeliveryStats snapshots the counters (tests + R13 status).
func (b *Broker) homeDeliveryStats() (pushes, acks uint64) {
	hd := b.homeDelivery()
	hd.mu.Lock()
	defer hd.mu.Unlock()
	return hd.pushes, hd.acks
}

// --- publishing ------------------------------------------------------------

// pushHomeAssignment publishes a home assignment to an agent's forwarded-command
// subject with a per-directive ack token as Reply. Fire-and-forget by design: a
// blocking Request here would let one unreachable agent stall the broker's single
// reconcile ticker, and the pass's own re-delivery already covers a dropped publish.
//
// SR-8 (external re-review): ONE single-use token PER DIRECTIVE, delivered as a
// SEPARATE single-directive push — NOT one token for the whole assignment. The ack
// travels back on the broker's _INBOX, and EVERY agent (any session) is granted
// `Sub _INBOX.>` (auth/permissions.go), so a token is DISCLOSED on the shared bus the
// moment its port is acked. A token shared across a multi-port push would then let a
// disclosed (already-acked) token authorize a forged ack for a still-un-acked SIBLING
// port — cross-session false convergence that bypasses the drain/retire/upgrade rc
// gate. A per-directive token is consumed by its own single ack, so a token is only
// ever disclosed AFTER it is spent, and it names exactly one port: a disclosed token
// authorizes nothing.
func (b *Broker) pushHomeAssignment(nc *nats.Conn, sid, nid string, ha *proto.HomeAssignment) {
	if nc == nil || ha == nil || len(ha.Directives) == 0 {
		return
	}
	hd := b.homeDelivery()
	hd.mu.Lock()
	inbox := hd.inbox
	hd.mu.Unlock()
	subj := proto.SubjCmdForwarded(sid, nid, homeDeliveryVerb)
	now := b.now()
	pushed := 0
	for i := range ha.Directives {
		single := &proto.HomeAssignment{Directives: ha.Directives[i : i+1]}
		body, err := json.Marshal(single)
		if err != nil {
			continue
		}
		reply := ""
		if inbox != "" {
			// A fresh single-use token for THIS one directive (SR-8), recorded so the ack can be
			// correlated to something we issued and consumed on first use (C1/M-1).
			if token := b.recordHomeOutstanding(single.Directives, now); token != "" {
				reply = inbox + "." + token
			}
		}
		if reply == "" {
			// No ack channel wired (single mode) or token mint failed: still DELIVER — a missing
			// ack degrades this to an un-acked re-delivery loop, strictly better than not
			// delivering. Correctness still rests on the pass re-delivering.
			err = nc.Publish(subj, body)
		} else {
			err = nc.PublishRequest(subj, reply, body)
		}
		if err != nil {
			b.cfg.Logger.Warn("broker: push home assignment", "sid", sid, "nid", nid, "port", single.Directives[0].PublicPort, "err", err)
			continue
		}
		pushed++
	}
	if pushed > 0 {
		hd.mu.Lock()
		hd.pushes += uint64(pushed)
		hd.mu.Unlock()
	}
}

// --- the reconcile pass ----------------------------------------------------

// homeDeliveryTarget is one (sid,nid) that owns at least one ALLOCATED homed expose.
type homeDeliveryTarget struct{ sid, nid string }

// homeDeliveryTargets lists the agents that own ALLOCATED homed exposes, plus the
// live set of homed ports (used to prune the applied cache). Read-only.
//
// It deliberately does NOT filter on nodes.status: an OFFLINE-marked node may still
// hold a live tunnel (that is precisely the drained-and-silent case this batch
// exists for), and a publish to a truly absent node is a no-op on the wire. Filtering
// on a liveness column would re-introduce a peer-event precondition through the back
// door.
// It also returns the live set of (sid|nid) KEYS (== the dedup keys), so the pass can prune the
// attempts map for nodes that no longer own any homed expose (N-9). Returning the SAME `seen` map the
// dedup uses keeps the prune key definitionally identical to the enumeration key — no drift.
func (b *Broker) homeDeliveryTargets() ([]homeDeliveryTarget, map[int]struct{}, map[string]struct{}, error) {
	rows, err := b.cfg.DB.Query(
		`SELECT DISTINCT sid, nid, port FROM port_allocations
		  WHERE state='ALLOCATED' AND home_broker != ''
		  ORDER BY sid, nid, port`)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() { _ = rows.Close() }()
	var targets []homeDeliveryTarget
	live := map[int]struct{}{}
	seen := map[string]struct{}{}
	for rows.Next() {
		var sid, nid string
		var p int
		if err := rows.Scan(&sid, &nid, &p); err != nil {
			return nil, nil, nil, err
		}
		live[p] = struct{}{}
		k := sid + "|" + nid
		if _, dup := seen[k]; !dup {
			seen[k] = struct{}{}
			targets = append(targets, homeDeliveryTarget{sid: sid, nid: nid})
		}
	}
	return targets, live, seen, rows.Err()
}

// homeAssignmentFingerprint renders the EXPECTED assignment into a stable string.
// A change here means "the operator changed something" and RESETS the per-node
// backoff — that is what makes a fresh drain converge on the pass cadence instead
// of inheriting a 5-minute penalty earned by an earlier, unrelated failure.
func homeAssignmentFingerprint(ha *proto.HomeAssignment) string {
	if ha == nil {
		return ""
	}
	parts := make([]string, 0, len(ha.Directives))
	for _, d := range ha.Directives {
		parts = append(parts, strconv.Itoa(d.PublicPort)+":"+strconv.FormatInt(d.Epoch, 10)+":"+d.NodeID+":"+d.BrokerAddr)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// homeAssignmentApplied reports whether the agent has confirmed EVERY directive in
// the assignment. This is the compare step; true ⇒ the pass does nothing at all.
func (b *Broker) homeAssignmentApplied(ha *proto.HomeAssignment) bool {
	if ha == nil || len(ha.Directives) == 0 {
		return true
	}
	hd := b.homeDelivery()
	hd.mu.Lock()
	defer hd.mu.Unlock()
	for _, d := range ha.Directives {
		if hd.applied[d.PublicPort] < d.Epoch {
			return false
		}
	}
	return true
}

// homeDeliveryDue applies the per-node backoff. want is the EXPECTED fingerprint:
// when it differs from the fingerprint the backoff was earned against, the backoff
// is discarded and the push happens immediately.
func (b *Broker) homeDeliveryDue(sid, nid, want string, now time.Time) bool {
	hd := b.homeDelivery()
	hd.mu.Lock()
	defer hd.mu.Unlock()
	k := sid + "|" + nid
	a := hd.attempts[k]
	if a == nil || a.want != want {
		hd.attempts[k] = &homeDeliveryAttempt{want: want}
		return true
	}
	if now.Before(a.nextAt) {
		return false
	}
	return true
}

// homeDeliveryPushed records that a push went out and advances the backoff.
func (b *Broker) homeDeliveryPushed(sid, nid, want string, interval time.Duration, now time.Time) {
	hd := b.homeDelivery()
	hd.mu.Lock()
	defer hd.mu.Unlock()
	k := sid + "|" + nid
	a := hd.attempts[k]
	if a == nil || a.want != want {
		a = &homeDeliveryAttempt{want: want}
		hd.attempts[k] = a
	}
	if a.backoff <= 0 {
		a.backoff = interval
	} else if a.backoff < homeDeliveryMaxBackoff {
		a.backoff *= 2
	}
	if a.backoff > homeDeliveryMaxBackoff {
		a.backoff = homeDeliveryMaxBackoff
	}
	a.nextAt = now.Add(a.backoff)
}

// homeDeliveryReset drops the backoff for a node so the next pass tick (or an
// immediate deliverHomeNow) publishes without waiting. Called when a drain has just
// re-pointed this node's homes: the operator is standing there, and the new
// assignment must go out on the next tick, not on the tail of an old backoff.
func (b *Broker) homeDeliveryReset(sid, nid string) {
	hd := b.homeDelivery()
	hd.mu.Lock()
	delete(hd.attempts, sid+"|"+nid)
	hd.mu.Unlock()
}

// reconcileHomeDelivery is the registered pass. See the file header for why each
// step is what it is.
//
// Idempotent path called: agent.applyHomeDirectives (via the "home" forwarded verb)
// — the exact function the register reply already drives, epoch-monotone and
// documented non-tearing at equal epoch.
func (b *Broker) reconcileHomeDelivery(_ context.Context, now time.Time) error {
	// Single mode emits no home directives at all (selfID==""), so the pass is
	// structurally inert there and the register reply stays byte-identical.
	if !b.clusterMode || b.selfID == "" {
		return nil
	}
	nc := b.nc.Load()
	if nc == nil {
		return nil
	}
	targets, live, liveKeys, err := b.homeDeliveryTargets()
	if err != nil {
		return fmt.Errorf("home-delivery: enumerate targets: %w", err)
	}
	b.pruneHomeApplied(live)
	b.pruneHomeOutstanding(now)
	b.pruneHomeAttempts(liveKeys)
	for _, t := range targets {
		ha := b.homeForRegister(t.sid, t.nid, proto.NodeRegisterReq{})
		if ha == nil || len(ha.Directives) == 0 {
			continue
		}
		if b.homeAssignmentApplied(ha) {
			// CONVERGED: zero side effects. This is the branch the registry's
			// "converged ⇒ nothing happens across consecutive ticks" test pins.
			b.homeDeliveryReset(t.sid, t.nid)
			continue
		}
		fp := homeAssignmentFingerprint(ha)
		if !b.homeDeliveryDue(t.sid, t.nid, fp, now) {
			continue
		}
		b.pushHomeAssignment(nc, t.sid, t.nid, ha)
		b.homeDeliveryPushed(t.sid, t.nid, fp, b.cfg.HomeDeliverInterval, now)
	}
	return nil
}

// pruneHomeApplied drops applied entries for ports that no longer have an ALLOCATED
// homed allocation, so the cache cannot grow without bound across expose churn.
func (b *Broker) pruneHomeApplied(live map[int]struct{}) {
	hd := b.homeDelivery()
	hd.mu.Lock()
	defer hd.mu.Unlock()
	for p := range hd.applied {
		if _, ok := live[p]; !ok {
			delete(hd.applied, p)
		}
	}
}

// pruneHomeAttempts (N-9) drops backoff entries for (sid,nid) pairs that no longer own any ALLOCATED
// homed expose. Without it, a node whose expose is released / whose node is removed while it was NOT
// yet converged leaks its attempts entry forever: homeDeliveryReset (the only delete) fires only for a
// still-ENUMERATED, converged target, and a departed node is never enumerated again. That is a slow map
// leak the NumGoroutine/fd gate structurally cannot see. Keyed by the SAME sid+"|"+nid the targets
// enumeration dedups on (liveKeys == the pass's `seen` set), so a still-live target — including one
// merely sitting in backoff this tick — is always in the set and keeps its earned backoff untouched. A
// live-set prune (not a TTL on nextAt) is exact: a TTL would evict a live node parked in max backoff
// during a long agent outage and reset the backoff the storm-control depends on.
func (b *Broker) pruneHomeAttempts(liveKeys map[string]struct{}) {
	hd := b.homeDelivery()
	hd.mu.Lock()
	defer hd.mu.Unlock()
	for k := range hd.attempts {
		if _, ok := liveKeys[k]; !ok {
			delete(hd.attempts, k)
		}
	}
}

// --- the upgrade verb's rc-semantics oracle ---------------------------------

// homesUnconverged returns every ALLOCATED homed expose whose agent has NOT
// confirmed the row's current epoch, as "sid/nid port=P want=E" strings.
//
// This is the fleet-wide form of the drain gate, and it is what gives the third
// verb — `cluster upgrade` — its rc semantics. A rolling upgrade restarts brokers
// underneath live tunnels; "every host reports the target version" is a
// CONTROL-plane fact, exactly like "the drain's raft write committed". A roll that
// left an expose's tunnel unrecovered has not finished, and (mirroring the existing
// keeper.Lost() precedent in driveUpgrade, which already refuses rc=0 for a roll
// that lost its lock) must not report success.
//
// Meaningful ONLY on the leader: the home-delivery pass is leaderOnly, so only the
// leader accumulates applied acks. The caller targets the leader; a follower would
// report everything unconverged, which is why handleUpgradeTrigger answers
// not_leader instead of a verdict.
func (b *Broker) homesUnconverged() ([]string, error) {
	rows, err := b.cfg.DB.Query(
		`SELECT sid, nid, port, epoch FROM port_allocations
		  WHERE state='ALLOCATED' AND home_broker != ''
		  ORDER BY port`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var sid, nid string
		var p int
		var epoch int64
		if err := rows.Scan(&sid, &nid, &p, &epoch); err != nil {
			return nil, err
		}
		if b.homeAppliedEpoch(p) < epoch {
			out = append(out, fmt.Sprintf("%s/%s port=%d want_epoch=%d", sid, nid, p, epoch))
		}
	}
	return out, rows.Err()
}

// deliverHomeNow forces one immediate delivery round for a single agent, bypassing
// the backoff. It is the "kick" DrainNode issues right after its raft write so the
// data plane starts converging in milliseconds rather than on the next pass tick —
// but it is NOT the mechanism the invariant rests on: if this publish is lost, the
// pass still re-delivers. Correctness comes from the pass; this is latency only.
func (b *Broker) deliverHomeNow(sid, nid string) {
	if !b.clusterMode || b.selfID == "" {
		return
	}
	nc := b.nc.Load()
	if nc == nil {
		return
	}
	ha := b.homeForRegister(sid, nid, proto.NodeRegisterReq{})
	if ha == nil || len(ha.Directives) == 0 {
		return
	}
	b.homeDeliveryReset(sid, nid)
	b.pushHomeAssignment(nc, sid, nid, ha)
}
