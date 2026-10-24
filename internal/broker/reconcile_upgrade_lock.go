package broker

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/brokerruntime"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/nats-io/nats.go"
)

// reconcile_upgrade_lock.go (R7b, defect P3) — release a cluster_upgrade_active
// marker whose holder has stopped renewing.
//
// THE DEFECT
// ----------
// `tether cluster upgrade` acquires a cluster-scoped roll lock before touching a
// single host, and releases it ONLY on clean completion. A HALT — any refusal at
// any host — deliberately leaves it held, on the theory that membership should
// stay frozen while the cluster sits in a partial-roll state, and that the
// operator will "fix and re-run", whose re-run re-acquires and eventually
// releases.
//
// That theory has one reachable branch and one unreachable one. The re-run path
// works while there is still something to roll. But once every broker host is at
// the target, `cluster upgrade` short-circuits at `plan.Upgrades() == 0`, and the
// stale-lock self-heal lives INSIDE that branch — reachable only if the planner
// agrees every host is AtTarget. On an agentless host AtTarget is structurally
// false, so `plan.Upgrades()` never reaches 0, the self-heal branch never runs,
// and the roll loop that would release the lock has nothing left to do either.
// The marker is then held FOREVER: `cluster join` and `cluster retire` are
// refused, cluster-wide, until somebody hand-edits a replicated SQLite row.
//
// WHY A LEASE AND NOT A DEADLINE
// ------------------------------
// A plain "locks older than X are stale" rule is a POLICY, and a wrong one: rolls
// legitimately run long, so any X large enough to be safe is too large to be
// useful, and any X small enough to be useful drops the mutex under a live roll —
// which is strictly worse than the leak, because it re-admits the concurrent
// join/retire the lock exists to fence. The lease inverts the question from "has
// this taken too long?" (unanswerable) to "is anybody still holding it?"
// (answered by the holder, continuously, by renewing).
//
// THE ONE-VOTE-VETO INVARIANT, DISCHARGED
// ---------------------------------------
// Expected state: no roll lock, once nobody is renewing its lease.
// Actual state:   upgradeActive(ro) + cluster_upgrade_lease.
// Existing legacy command: cluster.PlanClearUpgradeActive(). S6 uses the same
// operation with a current marker/lease CAS, so an old expiry hint cannot clear
// a replacement owner or a renewed lease.
//
// The pass invents no policy beyond the lease's own expiry, which is the single
// new mechanism R7b is authorized to add. It does not decide a roll has failed,
// does not touch any host, does not abort anything: it removes a mutex whose
// holder is provably gone.
//
// FAIL-CLOSED, TWICE
// ------------------
//   - No lease row ⇒ never expire. That is what a lock acquired by a pre-R7b
//     broker looks like, and mid-rolling-upgrade is exactly when a mixed-version
//     cluster exists. `tether cluster unlock` is the operator's out for those.
//   - Unparseable/unreadable lease ⇒ never expire, and surface the error so the
//     pass earns the registry's exponential backoff instead of silently declining
//     forever.

// upgradeLockDecision is the whole decision, isolated from raft so it can be
// tested exhaustively against a plain SQLite fixture. It performs NO writes
// (brokerruntime.UpgradeLockDecision; S8 §12.1 P15 net-zero move).
func upgradeLockDecision(ro *sql.DB, now time.Time) (clear bool, err error) {
	return brokerruntime.UpgradeLockDecision(ro, now)
}

// reconcileUpgradeLock is the registry pass. Leader-only (see registerCoreReconcilePasses).
func (b *Broker) reconcileUpgradeLock(_ context.Context, now time.Time) error {
	if !b.clusterMode || b.cl == nil || b.cl.node == nil {
		return nil
	}
	ro := b.cl.node.RODB()
	if ro == nil {
		return nil
	}
	// External review M-2: reap only when this leader has APPLIED everything committed. A leader that
	// just won an election (e.g. mid rolling-upgrade) can trail the log by MANY entries; deciding to
	// reap off that stale view could read an EXPIRED lease that a renewal has already refreshed on the
	// committed-but-not-yet-applied tail, and tear out a LIVE roll lock — re-admitting the concurrent
	// join/retire the lock exists to fence. reaperCaughtUp() closes the large-replay case: a just-elected
	// leader replaying a long committed tail has RaftAppliedIndex < CommitIndex (the FSM dispatch channel
	// backpressures), so the gate holds off until the bulk of the tail has drained.
	//
	// origin: hot-update S6 plan §7.1 / T40
	// RaftAppliedIndex tracks dispatch rather than SQLite completion. That old
	// residual is closed by re-reading under applyMu and comparing the exact
	// marker/lease pair during apply; a queued renewal makes the clear a no-op.
	if !b.reaperCaughtUp() {
		return nil
	}

	if err := reapBrokerMaintenance(b); err != nil {
		return err
	}
	clear, err := upgradeLockDecision(ro, now)
	if err != nil {
		return fmt.Errorf("upgrade-lock: %w", err)
	}
	if !clear {
		return nil
	}

	// An expired RO observation is only a hint; writer planning decides anew.
	if err := b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return cluster.PlanReapUpgradeLease(db, now)
	}); err != nil {
		// Propose fails on leadership loss; returning the error earns the registry's
		// exponential backoff rather than re-proposing into a running election.
		return fmt.Errorf("upgrade-lock: release expired cluster_upgrade_active: %w", err)
	}
	// Commit/readback is the release receipt; a renewed pair can make a hint no-op.
	// origin: hot-update S6 plan §7.1 / T40
	state, err := cluster.ReadUpgradeLock(ro)
	if err != nil {
		return fmt.Errorf("upgrade-lock: readback: %w", err)
	}
	if state.Marker != "" || state.Lease != "" {
		return nil
	}
	b.cfg.Logger.Warn("broker: released an expired `cluster upgrade` roll lock — its orchestrator stopped renewing the lease. " +
		"If a roll IS still running, it HALTED; re-run `tether cluster upgrade` before starting any membership change.")
	return nil
}

// Public registration evidence is replicated separately from the short proof
// cache. A new leader can verify agent ACKs without inheriting another machine's
// clock, and ordinary refreshes make no writes when this evidence is unchanged.
// origin: hot-update S6 internal review rv-cluster-3
const maintenanceAgentPrefix = "broker_image_observer."

type maintenanceAgent = brokercontrol.MaintenanceAgent
type maintenanceAdmission struct {
	Record    brokermaint.Record
	Deadline  int64
	TargetAck proto.BrokerMaintenanceAck
}
type maintenancePeer = brokercontrol.MaintenancePeer

func acceptedMaintenanceCapability(req proto.NodeRegisterReq) string {
	if req.BrokerImageMaintenance == brokermaint.Capability {
		return brokermaint.Capability
	}
	return ""
}

func rememberAgentMaintenance(b *Broker, sid, nid string, req proto.NodeRegisterReq) {
	if b.cl != nil && (b.cl.node == nil || !b.cl.node.IsLeader()) || req.BrokerImageMaintenance == "" {
		return
	}
	// The register subject is provisioned by auth_callout. Bind its public key
	// to that same identity before making it a signature-verification oracle.
	fp, err := auth.FingerprintFromActor(req.BrokerImageMaintenanceKey)
	if err != nil {
		return
	}
	var provisioned string
	if b.read().QueryRow(`SELECT agent_fp FROM agent_provisioning WHERE sid=? AND nid=?`, sid, nid).Scan(&provisioned) != nil || fp != provisioned {
		return
	}
	observer := maintenanceAgent{SID: sid, NID: nid, Capability: acceptedMaintenanceCapability(req), PublicKey: req.BrokerImageMaintenanceKey}
	raw, _ := json.Marshal(observer)
	key := maintenanceAgentPrefix + sid + "/" + nid
	var previous string
	_ = b.read().QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, key).Scan(&previous)
	if previous == string(raw) {
		return
	}
	err = maintenanceWrite(b, func(db *sql.DB) (*cluster.Command, error) {
		// Recheck provisioning under the writer lock; a revoked agent cannot leave
		// valid observer authority behind a concurrent register/revoke race.
		var actual string
		if err := db.QueryRow(`SELECT agent_fp FROM agent_provisioning WHERE sid=? AND nid=?`, sid, nid).Scan(&actual); err != nil || actual != fp {
			return nil, brokermaint.ErrIdentity
		}
		return cluster.NewCommand(cluster.OpClusterMetaSet, cluster.Stmt(`INSERT INTO cluster_meta(key,value) VALUES(`+cluster.MustLitText(key)+`,`+cluster.MustLitText(string(raw))+`) ON CONFLICT(key) DO UPDATE SET value=excluded.value`)), nil
	})
	if err != nil {
		b.cfg.Logger.Warn("broker: maintenance observer registration failed")
	}
}

// registerAuthority runs before any lease or row write: an image carried across
// exec names the session creation it belongs to, and a recreated SID must not
// lend it a lease or a row. A proven mismatch revokes; an unreadable creation
// is transient and answered with the code the register loop already retries.
// It stays outside handleRegister, which sits on the maintidx boundary.
// origin: hot-update S8 plan §6.2 / §11.2 P15
func registerAuthority(b *Broker, msg *nats.Msg, sid string, req *proto.NodeRegisterReq) (string, bool) {
	incarnation, err := brokerupgrade.RegisterAuthority(b.read().SQL(), sid, req.Incarnation)
	if brokerupgrade.IncarnationMismatch(err) {
		b.replyErr(msg, proto.CodeSessionIncarnationMismatch, "session incarnation does not match the inherited image")
	} else if err != nil {
		b.replyErr(msg, proto.CodeLeaderUnavailable, "session creation identity is unavailable; retrying")
	}
	return incarnation, err == nil
}

// noteRegisterUpgrade records register-time upgrade facts after the row write.
//
// upgrade-safety plan §4: an agent's first register after a `node upgrade`
// carries the outcome — without this line a rollback is indistinguishable
// from a plain reconnect on the broker side. Log-only by design: nodes rows
// travel through the raft-frozen RegisterInput payload, where a new field
// would silently drop in a mixed-version cluster (wire_freeze_test.go).
// "agent-reported" wording is deliberate (internal review S23): an
// in-flight register racing the agent's own rollback can carry a state
// the marker no longer holds; a later register delivers the correction.
// The S8 summary is the same log-only projection (public fields only), and
// the replicated capability record lets any queue-group member route
// upgrade-tx from the same fact. origin: hot-update S8 plan §7.2 / P15
func noteRegisterUpgrade(b *Broker, sid, nid string, req proto.NodeRegisterReq) {
	if req.UpgradeState != "" || req.Upgrade != nil {
		b.cfg.Logger.Info("broker: agent-reported upgrade outcome",
			"sid", sid, "nid", nid, "upgrade_state", req.UpgradeState,
			"detail", req.UpgradeDetail, "release", req.ReleaseVersion, "transaction", req.Upgrade)
	}
	if err := brokerupgrade.RememberAgentTransaction(b.read().SQL(), func(plan func(*sql.DB) (*cluster.Command, error)) error {
		return maintenanceWrite(b, plan)
	}, sid, nid, req.Capabilities); err != nil {
		b.cfg.Logger.Warn("broker: upgrade transaction capability record failed", "sid", sid, "nid", nid)
	}
}

func maintenanceSign(b *Broker) brokermaint.SignProof {
	return func(body []byte) (string, error) {
		if b.cfg.AuthCallout == nil {
			return "", brokermaint.ErrIdentity
		}
		sig, err := auth.SignWithSeed(b.cfg.AuthCallout.AccountSeed, body)
		return base64.RawStdEncoding.EncodeToString(sig), err
	}
}
func maintenanceVerify(pub string) brokermaint.VerifySignature {
	return func(body []byte, encoded string) error {
		sig, err := base64.RawStdEncoding.DecodeString(encoded)
		if err != nil {
			return brokermaint.ErrIdentity
		}
		return auth.VerifySignature(pub, body, sig)
	}
}
func maintenanceNonce() (string, error) { return brokercontrol.MaintenanceNonce() }
func maintenanceSend(b *Broker, peer maintenancePeer, req proto.BrokerMaintenanceReq, verify func(proto.BrokerMaintenanceAck) error) (proto.BrokerMaintenanceAck, error) {
	var ack proto.BrokerMaintenanceAck
	body, err := brokermaint.CanonicalRequestBytes(req.Record, req.Proof, req.NID, req.ConnectedBrokerID, req.IngressServerID, req.RouteDigest)
	if err != nil {
		return ack, err
	}
	req.Sig, err = maintenanceSign(b)(body)
	if err != nil {
		return ack, err
	}
	if req.Proof != (brokermaint.Proof{}) && peer.SID != "" {
		return deliverMaintenanceChallenge(b.nc.Load(), peer.SID, req, time.Second, verify)
	}
	raw, _ := json.Marshal(req)
	reply, err := b.nc.Load().Request(peer.Subject, raw, time.Second)
	if err != nil {
		return ack, err
	}
	if json.Unmarshal(reply.Data, &ack) != nil {
		return ack, brokermaint.ErrInvalid
	}
	return ack, verify(ack)
}

// The cohort snapshot and the replicated record commit live in the broker runtime leaf.
// origin: hot-update S6 plan §7.1 / rv-cluster-3; S8 §12.1 (P15 net-zero move)
func maintenancePeers(b *Broker, home, accountPub string) ([]maintenancePeer, error) {
	return brokerruntime.MaintenancePeers(b.cl.node, b.read().SQL(), home, accountPub)
}
func commitMaintenanceRecord(b *Broker, record brokermaint.Record, baseline brokerimage.MaintenanceBaseline) error {
	return brokerruntime.CommitMaintenanceRecord(b.cl.node, record, baseline, b.cfg.Now)
}

// The leader owns a single preparation at a time. Network callbacks run in
// parallel and are all joined before the phase is committed or refused, so one
// slow observer cannot consume a separate second TTL or leak a pending worker.
// origin: hot-update S6 internal review rv-cluster-3
func establishBrokerMaintenance(b *Broker, req *proto.ClusterUpgradeReq) *proto.ClusterUpgradeResp {
	refuse := func(err error) *proto.ClusterUpgradeResp {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: "maintenance_observer_unsupported: " + err.Error()}
	}
	if b.cl == nil || b.cl.node == nil || !b.cl.node.IsLeader() || req.Holder == "" || req.CutID == "" || req.TransferTo == "" {
		return refuse(brokermaint.ErrInvalid)
	}
	if err := installHomeMaintenance(b); err != nil {
		return refuse(err)
	}
	m := &b.cl.homeMaintenance
	m.mu.Lock()
	// Completed cut IDs cannot accumulate forever in a long-lived issuer.
	pruneNow, clockErr := controlbudget.Now()
	if clockErr != nil {
		m.mu.Unlock()
		return refuse(clockErr)
	}
	for id, admission := range m.admissions {
		if admission.Deadline <= pruneNow {
			delete(m.admissions, id)
		}
	}
	for id, challenge := range m.challenges {
		if pruneNow-challenge.SentBootNS >= int64(brokermaint.MaxLifetime) {
			delete(m.challenges, id)
		}
	}
	busy := m.coordinating
	if !busy {
		m.coordinating = true
	}
	m.mu.Unlock()
	if busy {
		return refuse(brokermaint.ErrConflict)
	}
	defer func() { m.mu.Lock(); m.coordinating = false; m.mu.Unlock() }()
	if b.cfg.AuthCallout == nil {
		return refuse(brokermaint.ErrIdentity)
	}
	pub, err := auth.PublicKeyFromSeed(b.cfg.AuthCallout.AccountSeed)
	if err != nil {
		return refuse(err)
	}
	peers, err := maintenancePeers(b, req.TransferTo, pub)
	if err != nil {
		return refuse(err)
	}
	role, err := cluster.ReadImageRole(b.cl.node)
	if err != nil {
		return refuse(err)
	}
	// Positive target health comes from its signed nonce reply below. Its
	// generation is checked again when that observer installs the proof.
	target, err := sendMaintenanceTrigger(b, &proto.ClusterUpgradeReq{Op: "maintenance-info", TargetNode: req.TransferTo})
	if err != nil || target.BrokerImage == nil || target.BrokerImage.Generation == 0 {
		return refuse(brokerimage.ErrMaintenanceObserverUnsupported)
	}
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		return refuse(err)
	}
	now, err := procrequest.BootNow()
	if err != nil {
		return refuse(err)
	}
	baseline := brokerimage.MaintenanceBaseline{Home: req.TransferTo, InstanceID: req.TransferTo}
	history, err := ExportHomeMaintenanceHistory(b)
	if err != nil {
		return refuse(err)
	}
	for _, dwell := range history.Dwells {
		if dwell.Home == req.TransferTo {
			baseline.DwellNS = max(baseline.DwellNS, int64(dwell.Dwell)*int64(observeTickInterval))
		}
	}
	digest, err := brokerimage.MaintenanceBaselineDigest(baseline)
	if err != nil {
		return refuse(err)
	}
	record := brokermaint.Record{CutID: req.CutID, Holder: req.Holder, Home: req.TransferTo, InstanceID: req.TransferTo, IssuerBootID: boot, Generation: target.BrokerImage.Generation, Mode: target.BrokerImage.Mode, Phase: brokermaint.MaintPrepared, IssuedBootNS: now, ExpiresBootNS: now + int64(brokermaint.MaxLifetime), Term: role.Term, ConfigIndex: role.ConfigIndex, VoterDigest: role.VoterDigest, BaselineDigest: digest, RequiredCapability: brokermaint.Capability}

	if err := commitMaintenanceRecord(b, record, baseline); err != nil {
		return refuse(err)
	}
	success := false
	defer func() {
		if !success {
			ended := record
			ended.Phase = brokermaint.MaintEnded
			_ = commitMaintenanceRecord(b, ended, baseline)
		}
	}()
	required := make([]brokerimage.MaintenanceObserver, len(peers))
	acks := make([]brokerimage.MaintenanceAck, len(peers))
	failures := make([]error, len(peers))
	var workers sync.WaitGroup
	for i, peer := range peers {
		workers.Add(1)
		go func(i int, peer maintenancePeer) {
			defer workers.Done()
			required[i], acks[i], failures[i] = collectMaintenanceObserver(b, record, peer)
		}(i, peer)
	}
	workers.Wait()
	for _, err := range failures {
		if err != nil {
			return refuse(err)
		}
	}
	record.Phase = brokermaint.MaintActive
	now, err = procrequest.BootNow()
	if err != nil {
		return refuse(err)
	}
	deadline, err := brokerimage.MaintenanceCutDeadline(record, record.IssuerBootID, now, required, acks, func(observer brokerimage.MaintenanceObserver, body []byte, sig string) error {
		for _, peer := range peers {
			if peer.ID == observer.ID {
				return maintenanceVerify(peer.PublicKey)(body, sig)
			}
		}
		return brokermaint.ErrIdentity
	})
	if err != nil {
		return refuse(err)
	}
	if err := cluster.ValidateImageRole(b.cl.node, role); err != nil {
		return refuse(err)
	}
	if err := commitMaintenanceRecord(b, record, baseline); err != nil {
		return refuse(err)
	}
	var targetAck proto.BrokerMaintenanceAck
	for i, peer := range peers {
		if peer.ID == "broker-"+record.Home {
			targetAck = acks[i].Reply
		}
	}
	if targetAck.Nonce == "" {
		return refuse(brokermaint.ErrIdentity)
	}
	m.mu.Lock()
	if m.admissions == nil {
		m.admissions = map[string]maintenanceAdmission{}
	}
	m.admissions[record.CutID] = maintenanceAdmission{record, deadline, targetAck}
	m.mu.Unlock()
	success = true
	return &proto.ClusterUpgradeResp{OK: true, Maintenance: &record}
}

func collectMaintenanceObserver(b *Broker, record brokermaint.Record, peer maintenancePeer) (brokerimage.MaintenanceObserver, brokerimage.MaintenanceAck, error) {
	return brokercontrol.CollectMaintenanceObserver(record, peer, func(req proto.BrokerMaintenanceReq, verify func(proto.BrokerMaintenanceAck) error) (proto.BrokerMaintenanceAck, error) {
		return maintenanceSend(b, peer, req, verify)
	}, func() (uint64, error) {
		if b.cl == nil {
			return 0, nil
		}
		return b.cl.node.AppliedIndex()
	}, maintenanceSign(b), maintenanceVerify(peer.PublicKey))
}

func sendMaintenanceTrigger(b *Broker, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
	req.IssuedAt = b.cfg.Now().UTC().Format(time.RFC3339)
	sig, err := auth.SignWithSeed(b.cfg.AuthCallout.AccountSeed, proto.CanonicalUpgradeReqBytes(req))
	if err != nil {
		return nil, err
	}
	req.Sig = hex.EncodeToString(sig)
	raw, _ := json.Marshal(req)
	reply, err := b.nc.Load().Request(proto.SubjCtrlClusterUpgrade(b.selfID), raw, time.Second)
	if err != nil {
		return nil, err
	}
	var response proto.ClusterUpgradeResp
	if json.Unmarshal(reply.Data, &response) != nil {
		return nil, brokermaint.ErrInvalid
	}
	if !response.OK {
		return nil, brokermaint.ErrIdentity
	}
	return &response, nil
}

// origin: hot-update S6 internal review rv-cluster-3
func admitBrokerMaintenance(b *Broker, req adminsock.Request, deadline controlbudget.Deadline) error {
	if b.cfg.AuthCallout == nil {
		return brokerimage.ErrMaintenanceObserverUnsupported
	}
	record, bound, err := brokercontrol.AdmitRemoteMaintenance(b.cl.node, b.selfID, b.cfg.AuthCallout.AccountSeed, brokerImageSnapshot(b).Generation, req, deadline,
		func(request *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
			return sendMaintenanceTrigger(b, request)
		},
		func() (map[string]brokermaint.LocalLease, int64, error) {
			_, proofs, now, err := homeMaintenanceSnapshot(b)
			return proofs, now, err
		},
		func(boot string, role storage.RoleReceipt) string {
			return brokerMaintenanceRouteDigest(b.selfID, boot, role, brokerImageSnapshot(b).Generation)
		}, maintenanceVerify)
	if err != nil {
		return err
	}
	m := &b.cl.homeMaintenance
	m.mu.Lock()
	m.cutDeadline, m.cutRecord = bound, record
	m.mu.Unlock()
	return nil
}

// A new leader never rebases foreign BOOTTIME. Eight seconds after its own
// first observation the maximum lifetime is provably over, even without cache.
// Losing the original holder is another positive expiry proof.
// origin: hot-update S6 internal review rv-cluster-6
func reapBrokerMaintenance(b *Broker) error {
	now, err := controlbudget.Now()
	if err != nil {
		return err
	}
	rows, err := b.cl.node.RODB().Query(`SELECT value FROM cluster_meta WHERE key LIKE 'broker_image_maintenance.%'`)
	if err != nil {
		return err
	}
	var records []brokermaint.Record
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return err
		}
		var record brokermaint.Record
		if json.Unmarshal([]byte(raw), &record) == nil && brokermaint.ValidateRecord(record) == nil && record.Phase != brokermaint.MaintEnded {
			records = append(records, record)
		}
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return err
	}
	live := map[string]bool{}
	for _, record := range records {
		live[record.Home+"/"+record.CutID] = true
	}
	maintenance := &b.cl.homeMaintenance
	maintenance.mu.Lock()
	for key := range maintenance.firstSeen {
		if !live[key] {
			delete(maintenance.firstSeen, key)
		}
	}
	maintenance.mu.Unlock()
	for _, record := range records {
		m := &b.cl.homeMaintenance
		key := record.Home + "/" + record.CutID
		m.mu.Lock()
		if m.firstSeen == nil {
			m.firstSeen = map[string]int64{}
		}
		first, seen := m.firstSeen[key]
		if !seen {
			first = now
			m.firstSeen[key] = now
		}
		m.mu.Unlock()
		err := b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			lock, err := cluster.ReadUpgradeLock(db)
			if err != nil {
				return nil, err
			}
			marker, err := cluster.UpgradeHolderMarker(record.Holder)
			if err != nil {
				return nil, err
			}
			if lock.Marker == marker && now-first < int64(brokermaint.MaxLifetime) {
				return nil, nil
			}
			return cluster.PlanExpireBrokerMaintenance(db, record, lock)
		})
		if err != nil && !errors.Is(err, brokermaint.ErrConflict) {
			return err
		}
	}
	return nil
}

func maintenanceWrite(b *Broker, plan func(*sql.DB) (*cluster.Command, error)) error {
	var node *cluster.Node
	if b.cl != nil {
		node = b.cl.node
	}
	db, err := brokerMaintenanceDB(b, node)
	if err != nil {
		return err
	}
	return brokercontrol.MaintenanceWrite(db, node, plan)
}

// freshBrokerRole is the S8 cut role, read from the CURRENT cluster runtime on
// every gated step; single mode has no cluster role that can change under it.
// origin: hot-update S8 plan §6.3 / §8.2 F8 / §11.2 P15
func freshBrokerRole(b *Broker) error {
	if b.cl == nil {
		return nil
	}
	return brokerupgrade.FreshRole(b.cl.node)
}

func validateBrokerReloadRole(b *Broker, stopped bool) error {
	var lease brokermaint.LocalLease
	if b.imageControl != nil {
		lease = b.imageControl.localMaintenance
	}
	if err := brokerruntime.LocalReloadLease(lease, b.cl == nil, stopped, func() *sql.DB { return b.read().SQL() }, func(holder string) error {
		return checkUpgradeHolder(b, &proto.ClusterUpgradeReq{Holder: holder}, b.cfg.Now())
	}); err != nil {
		return err
	}
	if b.cl == nil || b.cl.node == nil {
		return nil
	}
	m := &b.cl.homeMaintenance
	m.mu.Lock()
	expected, admitted := m.admission, m.admitted
	bound, record := m.cutDeadline, m.cutRecord
	m.mu.Unlock()
	if !admitted {
		return errors.New("broker: image role admission missing")
	}
	return brokerruntime.ReloadRole(b.cl.node, b.selfID, expected, bound, record, stopped, func(holder string) error {
		return checkUpgradeHolder(b, &proto.ClusterUpgradeReq{Holder: holder}, b.cfg.Now())
	})
}

// The replicated planner gets its writer from Node.Propose. Single mode must
// positively select the writable role; a missing cluster node must never fall
// through to a transaction on the runtime's read-only pool.
// origin: S7 internal review GATE-CONVERGE-contracts F6
func brokerMaintenanceDB(b *Broker, node *cluster.Node) (*sql.DB, error) {
	if node != nil {
		return b.read().SQL(), nil
	}
	db, writable := b.singleWriter()
	if !writable {
		return nil, singleWriteRefusal()
	}
	return db, nil
}

// Finish borrows the current role just as the maintenance planner does. The
// local record read uses the read pool in cluster mode; its writes use raft.
// origin: S7 internal review GATE-CONVERGE-contracts F6
func finishBrokerLocalMaintenance(b *Broker, node *cluster.Node, lease brokermaint.LocalLease) error {
	db, err := brokerMaintenanceDB(b, node)
	if err != nil {
		return err
	}
	return brokercontrol.FinishLocalMaintenance(db, node, lease, b.cfg.Now())
}

// Imported activation retains its original signed hold until the remaining
// fallible work has committed; a returned-exec thaw can finish immediately.
// origin: S6 internal review SINGLEACK F4 / lead decision D-C / plan §4.2 / T49
func finishBrokerImageMaintenance(b *Broker, pending bool) error {
	c := b.imageControl
	return brokerruntime.FinishLocalActivation(&c.localMaintenance, pending, c.store, func(node *cluster.Node, lease brokermaint.LocalLease) error {
		return finishBrokerLocalMaintenance(b, node, lease)
	})
}
