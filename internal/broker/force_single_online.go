package broker

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
)

// force_single_online.go — the ONLINE force-single escape hatch (the quorum-loss recovery WITHOUT a broker
// stop). A two-step arm->commit flow over the LOCAL root-only admin socket, gated by: a sustained
// quorum-loss dwell, the exact offline peer-liveness HARD-REFUSE (re-checked at commit), a single-shot
// broker-minted arm token, and (CLI-side) the unchanged TTY-typed node_id confirm. Commit does the
// in-process raft swap (cluster.Node.RecoverToSelfOnline) — the process never stops. The offline path
// stays the floor for a broker that won't start.

const (
	// forceSingleDwell is how long this node must be CONTINUOUSLY quorum-lost (no leader contact) before
	// an online force-single is allowed — the primary transient-partition defense (>> a normal election,
	// so a momentary re-election never trips it). = max(15s, 10x MultinodeElectionTimeout(1s)).
	forceSingleDwell = 15 * time.Second
	// forceSingleArmTTL bounds how long an arm token is valid before commit must re-arm — forbids
	// pre-staging ("armed for 24h") and forces the operator's TTY confirm to sit between arm and commit.
	forceSingleArmTTL = 60 * time.Second
	// forceSingleLeaderWait bounds how long Commit waits for the freshly-recovered {self} raft to elect
	// itself leader before it can persist the marker. A single-voter raft elects in well under a second.
	forceSingleLeaderWait = 5 * time.Second
)

// forceSingleArm tracks the sustained-quorum-loss dwell + the single-shot arm token for the online
// force-single. observeLeadership is fed by the (non-leader-gated) observe tick; the handlers read it.
type forceSingleArm struct {
	mu              sync.Mutex
	dwell           time.Duration
	leaderlessSince time.Time // zero = not currently observed leaderless (has leader contact, or unobserved)
	token           string
	tokenExpiry     time.Time
}

func newForceSingleArm() *forceSingleArm { return &forceSingleArm{dwell: forceSingleDwell} }

// observeLeadership records the leader-contact state each tick: set leaderlessSince on the first stale
// observation, reset it the instant contact returns. Called every observe tick regardless of leadership.
func (a *forceSingleArm) observeLeadership(stale bool, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if stale {
		if a.leaderlessSince.IsZero() {
			a.leaderlessSince = now
		}
		return
	}
	a.leaderlessSince = time.Time{}
}

// dwellState returns (remaining, quorumLost). quorumLost=false means the node currently HAS leader
// contact (or has not yet been observed leaderless) — not eligible at all. remaining>0 means it is
// quorum-lost but not yet for the full dwell.
func (a *forceSingleArm) dwellState(now time.Time) (remaining time.Duration, quorumLost bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.leaderlessSince.IsZero() {
		return a.dwell, false
	}
	elapsed := now.Sub(a.leaderlessSince)
	if elapsed >= a.dwell {
		return 0, true
	}
	return a.dwell - elapsed, true
}

// mint generates a fresh single-shot arm token. It FAILS CLOSED: if the system CSPRNG is
// unavailable it returns an error and no token is set (a commit then has nothing to consume) —
// an emergency safety interlock must never fall back to a predictable/empty token.
func (a *forceSingleArm) mint(now time.Time, ttl time.Duration) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	tok := hex.EncodeToString(b[:])
	a.mu.Lock()
	defer a.mu.Unlock()
	a.token = tok
	a.tokenExpiry = now.Add(ttl)
	return tok, nil
}

// consume validates + single-shot-clears the arm token (a stray lone commit, an expired/wrong token, or
// a replay all fail). Empty token never matches.
func (a *forceSingleArm) consume(token string, now time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if token == "" || a.token == "" || token != a.token || now.After(a.tokenExpiry) {
		a.token = "" // clear on any mismatch/expiry so a captured-but-stale token can't linger
		return false
	}
	a.token = ""
	return true
}

func fsRefuse(op, code, format string, args ...any) adminsock.Response {
	return adminsock.Response{Op: op, Error: fmt.Sprintf(format, args...), Code: code}
}

// fsSelfIDMismatch returns a non-empty message when the operator-confirmed --self-id (reqNodeID) does
// NOT match THIS broker (F2): the typed node_id only proves intent if the broker actually verifies it.
// Without this, an operator who runs the online recovery against broker brk-a but mistypes
// `--self-id brk-b` would be prompted to type brk-b, and the brk-a socket would still force-single
// brk-a. Empty is refused as well: the broker-side check is part of the destructive operation's
// safety boundary, so old or hand-written local socket clients must not bypass the typed self-id.
func fsSelfIDMismatch(reqNodeID, selfID string) string {
	if reqNodeID == "" {
		return fmt.Sprintf("self-id is required for online force-single; re-run with --self-id %q", selfID)
	}
	if reqNodeID != selfID {
		return fmt.Sprintf("self-id mismatch: this admin socket is broker %q but --self-id is %q; "+
			"you would force-single the WRONG node — re-run on %q", selfID, reqNodeID, selfID)
	}
	return ""
}

// fsBuildReport assembles the per-peer probe verdict for the report (does NOT gate; CheckPeersDead
// remains the authoritative HARD-REFUSE). probes is the display-only liveness from ProbePeers.
func fsBuildReport(selfID string, roster []clusteroffline.Peer, confirmed []string, probes []clusteroffline.PeerLiveness) adminsock.ForceSingleReport {
	confSet := map[string]bool{}
	for _, id := range confirmed {
		confSet[id] = true
	}
	liveBy := make(map[string]clusteroffline.PeerLiveness, len(probes))
	for _, pl := range probes {
		liveBy[pl.NodeID] = pl
	}
	rep := adminsock.ForceSingleReport{BrokerSelfID: selfID}
	for _, p := range roster {
		pl := liveBy[p.NodeID]
		rep.Peers = append(rep.Peers, adminsock.ForceSinglePeerProbe{
			NodeID: p.NodeID, Confirmed: confSet[p.NodeID], Alive: pl.Alive, OnPort: pl.OnPort,
		})
	}
	return rep
}

// selfRaftAddr reads this node's advertised raft address from the committed roster (cluster_nodes). For a
// resulting N=1 cluster the address never needs to be dialed, but it is preserved in the new {self} config.
func (b *clusterAdminBackend) selfRaftAddr(selfID string) (string, error) {
	var addr string
	err := b.admin.node.RODB().QueryRow(`SELECT raft_addr FROM cluster_nodes WHERE node_id = ?`, selfID).Scan(&addr)
	if err != nil {
		return "", err
	}
	if addr == "" {
		return "", fmt.Errorf("cluster_nodes.raft_addr is empty for self %q", selfID)
	}
	return addr, nil
}

// fsArmVerdict evaluates the online force-single gates IN ORDER (dwell → peer-liveness HARD-REFUSE) and
// returns the FIRST failing (code, reason); ("", "") means every gate passes. It records
// rep.DwellRemaining when the dwell is not yet satisfied. The peer-liveness gate is the SHARED
// CheckPeersDead — online and offline refuse identically.
func (b *clusterAdminBackend) fsArmVerdict(roster []clusteroffline.Peer, confirmed []string, now time.Time, rep *adminsock.ForceSingleReport) (code, reason string) {
	remaining, lost := b.fsArm.dwellState(now)
	if !lost {
		return adminsock.CodeQuorumNotLost, "node still has leader contact; force-single is a quorum-loss escape only"
	}
	if remaining > 0 {
		rep.DwellRemaining = remaining.Round(time.Second).String()
		return adminsock.CodeQuorumNotLost, fmt.Sprintf("not yet quorum-lost for the required dwell; wait %s", rep.DwellRemaining)
	}
	if err := clusteroffline.CheckPeersDead(roster, confirmed); err != nil {
		return adminsock.CodePeerAlive, err.Error()
	}
	return "", ""
}

// handleForceSingleArm runs the online force-single gates and, unless DryRun, mints an arm token. It is
// dispatched BEFORE the leader gate (a quorum-lost survivor is never leader). DryRun = a zero-mutation
// drill (no token, no state change) runnable on a HEALTHY cluster: it ALWAYS returns OK with the gate
// verdict (WouldProceed + Reason), so an operator can rehearse the command + verify the peer list
// without manufacturing an outage. A real (non-dry-run) arm REFUSES on any failing gate.
func (b *clusterAdminBackend) handleForceSingleArm(req adminsock.Request) adminsock.Response {
	op := req.Op
	if b.admin == nil || b.admin.node == nil || b.fsArm == nil {
		return fsRefuse(op, adminsock.CodeClusterNotEnabled, "online force-single unavailable (cluster mode not enabled)")
	}
	selfID := b.admin.node.SelfID()
	if mismatch := fsSelfIDMismatch(req.NodeID, selfID); mismatch != "" {
		return fsRefuse(op, adminsock.CodeBadRequest, "%s", mismatch)
	}
	roster, err := clusteroffline.ReadRoster(b.admin.node.RODB(), selfID)
	if err != nil { // fail-closed: cannot read the roster ⇒ cannot prove peers dead
		return fsRefuse(op, adminsock.CodeForceSingleRefused, "read roster: %v", err)
	}
	now := time.Now()
	rep := fsBuildReport(selfID, roster, req.ConfirmPeersDead, clusteroffline.ProbePeers(roster))
	code, reason := b.fsArmVerdict(roster, req.ConfirmPeersDead, now, &rep)
	if code == "" {
		rep.WouldProceed = true
	} else {
		rep.Reason = reason
	}
	if req.DryRun { // drill: socket round-trip + gates evaluated, zero mutation, no token — always OK
		return adminsock.Response{Op: op, OK: true, ForceSingle: &rep}
	}
	if code != "" { // a real arm refuses on any failing gate
		return adminsock.Response{Op: op, Error: reason, Code: code, ForceSingle: &rep}
	}
	tok, err := b.fsArm.mint(now, forceSingleArmTTL)
	if err != nil { // fail-closed: no token ⇒ no commit
		return fsRefuse(op, adminsock.CodeStoreError, "mint arm token (entropy unavailable): %v", err)
	}
	rep.ArmToken = tok
	return adminsock.Response{Op: op, OK: true, ForceSingle: &rep}
}

// handleForceSingleCommit re-checks every gate (self-id + token + dwell + peer-liveness RE-PROBE), does
// the in-process raft RecoverToSelfOnline, then persists the force_single_active marker + recovery epoch
// as PART OF THE SUCCESS BOUNDARY (F1): a missing marker = a writable emergency broker with no visible
// emergency (status exit-3 + the ctl destructive gate read ONLY that marker), so a leader-wait or
// marker-propose failure returns a LOUD failure naming the repair rather than a false OK.
func (b *clusterAdminBackend) handleForceSingleCommit(req adminsock.Request) adminsock.Response {
	op := req.Op
	if b.admin == nil || b.admin.node == nil || b.fsArm == nil {
		return fsRefuse(op, adminsock.CodeClusterNotEnabled, "online force-single unavailable (cluster mode not enabled)")
	}
	selfID := b.admin.node.SelfID()
	if mismatch := fsSelfIDMismatch(req.NodeID, selfID); mismatch != "" {
		return fsRefuse(op, adminsock.CodeBadRequest, "%s", mismatch)
	}
	now := time.Now()
	if !b.fsArm.consume(req.ArmToken, now) {
		return fsRefuse(op, adminsock.CodeArmExpired, "no fresh arm token (missing/expired/already-used); re-run `cluster recovery force-single --online` to re-arm")
	}
	roster, err := clusteroffline.ReadRoster(b.admin.node.RODB(), selfID)
	if err != nil {
		return fsRefuse(op, adminsock.CodeForceSingleRefused, "read roster: %v", err)
	}
	if remaining, lost := b.fsArm.dwellState(now); !lost || remaining > 0 {
		return fsRefuse(op, adminsock.CodeQuorumNotLost, "no longer continuously quorum-lost; refusing (a transient partition may have healed)")
	}
	// RE-PROBE peer liveness at the last moment before the irreversible rewrite (catches a peer that
	// revived in the arm->confirm window).
	if err := clusteroffline.CheckPeersDead(roster, req.ConfirmPeersDead); err != nil {
		return fsRefuse(op, adminsock.CodePeerAlive, "%s", err.Error())
	}
	selfAddr, err := b.selfRaftAddr(selfID)
	if err != nil {
		return fsRefuse(op, adminsock.CodeForceSingleRefused, "resolve self raft addr: %v", err)
	}
	// origin: batch-c EXTERNAL review B1. The DURABLE INTENT is written and fsync'd BEFORE the
	// irreversible rewrite, and it is what makes every window past this point recoverable.
	//
	// Before this, the only record that a force-single had happened was the force_single_active marker
	// — written AFTER the rewrite. So the very failure that mattered most (the marker not landing) also
	// erased the evidence needed to notice it: `cluster status` showed a merely-DEGRADED cluster, the
	// destructive gate (QuorumLost || ForceSingleActive) went FULLY OPEN because the rewrite had just
	// made QuorumLost false, the error text told the operator to re-run a command the dwell gate now
	// refuses (the node has leader contact — its own), and the leadership-edge resume was keyed on that
	// same missing marker.
	//
	// It is a local file, so it needs no quorum — which is the point, since this runs on a quorum-lost
	// node. The epoch is minted HERE and carried in the intent so every resume proposes the SAME value:
	// minting it after the rewrite is what made "did the epoch I promised actually land?" unanswerable
	// when the epoch write was the step that failed.
	abandoned := make([]string, 0, len(roster))
	for _, p := range roster {
		abandoned = append(abandoned, p.NodeID)
	}
	epoch, err := newRecoveryEpoch()
	if err != nil {
		// Fail BEFORE the rewrite: nothing irreversible has happened yet, so a plain refusal is honest
		// and the operator can simply re-run.
		return fsRefuse(op, adminsock.CodeStoreError, "generate the recovery epoch: %v", err)
	}
	intent := clusteroffline.OnlineIntent{
		SelfID: selfID, Abandoned: abandoned, Epoch: epoch,
		MarkedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	// A leadership edge can be observed as soon as the replacement raft elects itself. Keep that
	// recovery callback out until this handler has either completed its synchronous prune or returned:
	// otherwise the callback can create a retry op for the same ghosts while the happy path is still
	// running, violating the "clean recovery creates no op" contract.
	b.admin.forceSingleIntentMu.Lock()
	defer b.admin.forceSingleIntentMu.Unlock()
	if err := clusteroffline.WriteOnlineIntent(b.admin.dataDir, intent); err != nil {
		return fsRefuse(op, adminsock.CodeStoreError,
			"could not record the force-single intent on disk (%v); refusing to start an irreversible "+
				"raft rewrite that nothing would be able to finish if this process died", err)
	}

	// THE in-process swap. On failure the node stays read-only (never bricked); offline is the floor.
	if err := b.admin.node.RecoverToSelfOnline(selfAddr); err != nil {
		// RecoverToSelfOnline can fail either BEFORE RecoverCluster changes disk, or AFTER disk is
		// already {self} while rebuilding the transport. The caller cannot distinguish those phases
		// from the error, so deleting the intent here reopens the exact post-rewrite crash window it
		// exists to close. Retain it. On restart the leadership path inspects the committed raft
		// configuration: old configuration => safely discard; exact {self} => forward-complete.
		return fsRefuse(op, adminsock.CodeStoreError,
			"online recover did not finish (%v); the recovery intent remains on disk because the raft "+
				"rewrite may already have landed. Restart this broker: it will inspect the committed "+
				"configuration and either discard a pre-rewrite intent or forward-complete a {self} recovery",
			err)
	}
	// From here the rewrite has LANDED. Every remaining step is idempotent and re-derivable from the
	// intent, so a failure returns a loud error AND leaves the intent in place for
	// resumeForceSingleFinalizeOnLeadership to forward-complete — the repair no longer depends on an
	// operator re-running a command the arm gate would refuse.
	if err := b.admin.node.WaitForLeader(forceSingleLeaderWait); err != nil {
		return fsRefuse(op, adminsock.CodeStoreError,
			"online recover rewrote raft to {self} but leadership did not settle in %s (%v); the recovery "+
				"intent is recorded on disk and this broker will finish it automatically as soon as it "+
				"becomes leader — watch `tether cluster status` and `tether cluster ops ls`",
			forceSingleLeaderWait, err)
	}
	if err := b.admin.applyForceSingleIntent(intent); err != nil {
		return fsRefuse(op, adminsock.CodeStoreError,
			"online recover succeeded (node is a writable single voter) but persisting the emergency "+
				"state FAILED (%v); the recovery intent is recorded on disk and will be completed "+
				"automatically on the next leader tick (seconds) — until then `cluster status` may not "+
				"show FORCE_SINGLE, so do NOT run destructive commands", err)
	}
	// G2 #12: prune the abandoned peers from cluster_nodes so the signed roster converges to {self} and
	// clients stop dialing (and failover-preferring) the dead endpoints. The raft config is ALREADY {self}
	// (RecoverToSelfOnline moved the abandoned out above), so deleting their roster rows cannot fork quorum.
	//
	// batch C: the SYNCHRONOUS attempt below is unchanged, because the operator's very next documented
	// step depends on it — a ghost roster row has no raft role, and `reconcile nats --to-standalone`
	// refuses on an unrecognized role, so an un-pruned survivor cannot de-cluster and its JetStream
	// stays 503. What changed is the FAILURE branch. It used to be a dead end and said so: a re-run is
	// refused by the dwell gate (the node now HAS leader contact → CodeQuorumNotLost), so the prune had
	// exactly one attempt and a permanent ghost on failure. Now a failure starts an
	// OpKindForceSingleFinalize that the controller retries to a terminal state.
	report := adminsock.ForceSingleReport{BrokerSelfID: selfID, Abandoned: abandoned}
	if len(abandoned) > 0 {
		if err := b.admin.node.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanClusterNodePrune(abandoned, time.Now())
		}); err != nil {
			// The rows are still there on BOTH sub-branches below; say so explicitly rather than
			// leaving the CLI to infer it from whether an op id came back.
			report.PruneIncomplete = true
			opID, serr := b.admin.startForceSingleFinalize(selfID, abandoned)
			if serr != nil {
				// Fall back to EXACTLY the pre-batch-C behaviour. Creating the op is an improvement, not
				// a precondition: turning a survivable recovery into a hard failure at this moment would
				// be the "one more failure mode during an emergency" this change exists to avoid.
				b.admin.logger.Warn("online force-single: roster prune of abandoned peers failed AND a retry operation could not be started; they linger as ghosts until `cluster recovery node remove` (raft is already {self}, so this is not a quorum risk — but `reconcile nats --to-standalone` will REFUSE until they are gone)",
					"abandoned", abandoned, "prune_err", err, "op_err", serr)
			} else {
				report.FinalizeOpID = opID
				b.admin.logger.Warn("online force-single: roster prune failed; started a finalize operation to retry it",
					"abandoned", abandoned, "err", err, "op", opID)
			}
		} else if serr := b.admin.deriveAndConvergeSeedsFromRoster(); serr != nil {
			// G3 #1: prune succeeded → roster is {self}; converge the published seeds so clients stop
			// dialing (and failover-preferring) the abandoned endpoints. GATED on prune success (INV-4):
			// deriving before the prune commits would re-derive the dead endpoints straight back into seeds.
			b.admin.logger.Warn("online force-single: seed auto-converge failed (abandoned endpoints linger in published seeds until a later op / leadership backstop)",
				"err", serr)
		}
	}
	// The intent is cleared ONLY when every promised fact is confirmed landed: marker + epoch (above)
	// and the roster prune. If a finalize op is still retrying the prune, the intent stays so that a
	// crash before that op terminates is still recoverable from disk.
	if !report.PruneIncomplete {
		if err := clusteroffline.ClearOnlineIntent(b.admin.dataDir); err != nil {
			b.admin.logger.Warn("online force-single: completed, but the on-disk recovery intent could "+
				"not be cleared; a later leadership tick will find it, re-verify the same facts and "+
				"clear it then (the operations are idempotent)", "err", err)
		}
	}
	return adminsock.Response{Op: op, OK: true, ForceSingle: &report}
}

// newRecoveryEpoch mints the per-recovery epoch tag (the durable input to a future cross-node
// split-brain detector). It FAILS CLOSED on a CSPRNG error so a recovery never persists an empty epoch.
func newRecoveryEpoch() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
