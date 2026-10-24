package broker

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/brokerruntime"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/nats-io/nats.go"
)

// cluster_upgrade_trigger.go (G5 #13 W2b) — the account-signed remote reload/transfer-leader responder.
// Security anchor = the account signature (the operator's root authority, same trust as the signed
// roster/seeds): a broker acts ONLY if the request targets its own node_id AND the signature verifies
// against its pinned account_pub. Dispatch routes through the local admin backend, so it reuses EVERY
// gate the socket path has (reload leader-refuse/sha, transfer leader-gate) — no duplicated logic.

// upgradeTriggerCodeUnauthorized is the ClusterUpgradeResp code for a request that does not carry a valid
// account signature (the socket-only adminsock vocabulary has no auth code — the socket is root-gated).
const upgradeTriggerCodeUnauthorized = "unauthorized"

// upgradeTriggerCodeNotReady (External-review M1) is a RETRIABLE code returned when the admin backend is
// not wired yet (a broker that just re-exec'd has its health responder up before clusterAdminHandle is
// set). The orchestrator retries it, so a startup window is a brief wait rather than a roll HALT.
const upgradeTriggerCodeNotReady = "cluster_not_ready"

// upgradeTriggerCodeLockNotHeld (R7b) tells a lease renewer the roll lock is gone. TERMINAL for the
// renewer: a renewal is structurally incapable of re-creating a released lock. The emitter moved with
// the lock mapper; this name keeps the wire-stability pin bound to the same value.
const upgradeTriggerCodeLockNotHeld = brokerupgrade.CodeLockNotHeld

// upgradeOpHomesConverged (R8a P1) is the roll-tail data-plane verdict op. The literal is
// shared with cmd/tether/cluster_upgrade_drive.go (both ends author it independently — the
// ClusterUpgradeReq.Op field is a free-form string); TestUpgradeHomesConvergedOpIsWireStable
// pins the pair so a rename on one side cannot silently turn the gate into a no-op.
const upgradeOpHomesConverged = "homes-converged"

// setClusterAdminHandle stores the admin-dispatch func under the lock (the trigger responder reads it from
// another goroutine). External-review M1.
func (b *Broker) setClusterAdminHandle(fn func(adminsock.Request) adminsock.Response) {
	b.clusterAdminMu.Lock()
	b.clusterAdminHandle = fn
	b.clusterAdminMu.Unlock()
}

func (b *Broker) clusterAdminHandleFn() func(adminsock.Request) adminsock.Response {
	b.clusterAdminMu.RLock()
	defer b.clusterAdminMu.RUnlock()
	return b.clusterAdminHandle
}

// SubscribeClusterUpgradeTrigger wires the broadcast responder. Every broker subscribes, but each request
// names exactly one TargetNode, so only that broker replies — a wrong target stays SILENT so the intended
// broker's reply is the one the orchestrator sees.
func (b *Broker) SubscribeClusterUpgradeTrigger(nc *nats.Conn) (*nats.Subscription, error) {
	return nc.Subscribe(proto.SubjCtrlClusterUpgradeWildcard, func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		var request proto.BrokerMaintenanceReq
		if json.Unmarshal(msg.Data, &request) == nil && request.Record.CutID != "" {
			if request.NID != b.selfID {
				return
			}
			ack, err := acceptBrokerMaintenance(b, request)
			if err != nil {
				return
			}
			raw, _ := json.Marshal(ack)
			b.respondBytes(msg, raw)
			return
		}
		var upgrade proto.ClusterUpgradeReq
		if json.Unmarshal(msg.Data, &upgrade) == nil && upgrade.Op == "maintenance" && upgrade.Maintenance == nil && upgrade.TargetNode == b.selfID {
			// NATS serializes one subscription's callbacks. The issuer must not
			// wait for its own voter challenge on that same callback: it would
			// deadlock the whole observer cohort. Track the asynchronous issuer
			// as a real store borrower so Freeze still joins its DB/ACK work.
			// origin: hot-update S6 plan §7.1 / internal review rv-cluster-3
			control := b.imageControl
			if control == nil || !control.users.Begin() {
				body, _ := json.Marshal(proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: brokerimage.DetailMaintenanceObserverUnsupported})
				b.respondBytes(msg, body)
				return
			}
			go func() {
				defer control.users.End()
				if resp := b.handleUpgradeTrigger(msg.Data, b.cfg.Now()); resp != nil {
					body, _ := json.Marshal(resp)
					b.respondBytes(msg, body)
				}
			}()
			return
		}
		// The transaction action binds the subject's authenticated actor, so it
		// needs the route the signature-only verbs never read. Like the S6
		// initiating reload it is served inline and DB-free after admission; an
		// orchestrator establishes any maintenance hold with its own action first.
		var resp *proto.ClusterUpgradeResp
		if upgrade.Op == brokerupgrade.SignedTransactionOp {
			resp = handleUpgradeTxTrigger(b, msg.Subject, msg.Data, b.cfg.Now())
		} else {
			resp = b.handleUpgradeTrigger(msg.Data, b.cfg.Now())
		}
		if resp == nil {
			return // not for this broker → stay silent
		}
		body, _ := json.Marshal(resp)
		b.respondBytes(msg, body)
	})
}

// handleUpgradeTxTrigger serves the additive signed transaction action after the same signature,
// target and replay verification as every other op, binding the subject's authenticated actor and
// this broker's registered key. A wrong target stays silent. origin: hot-update S8 plan §2.2 / §7.1 / P15
func handleUpgradeTxTrigger(b *Broker, subject string, data []byte, now time.Time) *proto.ClusterUpgradeResp {
	req, refused := verifyUpgradeTrigger(b, data, now)
	if req == nil {
		return refused
	}
	actor, _, _ := proto.ParseCtrlBy(subject)
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		fp = ""
	}
	var node *cluster.Node
	if b.cl != nil {
		node = b.cl.node
	}
	return brokerupgrade.HandleSignedTransaction(brokerUpgradeContext(b), brokerupgrade.WithRoll(b.upgradeEndpoint, b.cfg.Upgrade, node, b.cfg.AuthCallout.AccountSeed, func() error { return brokerupgrade.CheckHomes(b.homesUnconverged()) }), b.upgradeKey, req, b.selfID, fp,
		func() error { return checkUpgradeHolder(b, req, now) })
}

// handleUpgradeTrigger verifies + dispatches one signed request. Returns nil (stay silent) when the
// request targets a DIFFERENT broker; a *ClusterUpgradeResp otherwise (including every refusal).
func (b *Broker) handleUpgradeTrigger(data []byte, now time.Time) *proto.ClusterUpgradeResp {
	verified, refused := verifyUpgradeTrigger(b, data, now)
	if verified == nil {
		return refused
	}
	req := *verified
	// origin: hot-update S6 plan §7.1 / T40
	if req.Op == "reload" || req.Op == "transfer-leader" || req.Op == "reexec-agent" {
		if err := checkUpgradeHolder(b, &req, now); err != nil {
			return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: "upgrade holder is not current"}
		}
	}
	handle := b.clusterAdminHandleFn()
	if handle == nil && (req.Op == "reload" || req.Op == "transfer-leader") {
		// External-review M1: not a terminal failure — the admin backend wires a moment after the health
		// responder comes up (esp. just after a re-exec). Retriable so the orchestrator waits, not HALTs.
		return &proto.ClusterUpgradeResp{Code: upgradeTriggerCodeNotReady, Error: "cluster admin backend not wired yet — retry"}
	}
	// Route through the local admin backend so EVERY existing gate applies (no duplicated logic).
	switch req.Op {
	case "reload":
		// origin: S8 internal review rv3-leaves F2
		ar := brokerupgrade.LegacyReloadResponse(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload}, b.read().SQL())
		return &proto.ClusterUpgradeResp{OK: ar.OK, Code: ar.Code, Error: ar.Error, Reloading: ar.Reloading, AlreadyAtVersion: ar.AlreadyAtVersion, BrokerImage: ar.BrokerImage, NextGeneration: ar.NextGeneration}
	case "transfer-leader":
		ar := handle(adminsock.Request{Op: adminsock.OpClusterTransfer, NodeID: req.TransferTo})
		out := &proto.ClusterUpgradeResp{OK: ar.OK, Code: ar.Code, Error: ar.Error}
		if ar.OK {
			out.NewLeader = req.TransferTo
		}
		return out
	case "reexec-agent":
		return brokerupgrade.LegacyReexecResponse(b.read().SQL(), req, b.selfID, b.cfg.ColocatedAgentNID, b.nc.Load() != nil)

	case upgradeOpHomesConverged:
		// R8a P1: the `cluster upgrade` verb's rc-semantics probe. The orchestrator asks
		// the LEADER, at the tail of a roll, whether every homed expose's agent has
		// CONFIRMED its home — i.e. whether the data plane actually came back after the
		// brokers were restarted underneath it. Not-converged is a NONZERO outcome for
		// the roll, exactly as keeper.Lost() already is.
		if b.cl == nil || b.cl.node == nil || !b.cl.node.IsLeader() {
			// Only the leader accumulates applied acks (the delivery pass is leaderOnly),
			// so a follower must not render a verdict — it would always say "unconverged".
			return &proto.ClusterUpgradeResp{Code: adminsock.CodeNotLeader, Error: "not leader; ask the leader for the data-plane verdict"}
		}
		pending, err := b.homesUnconverged()
		if err != nil {
			return &proto.ClusterUpgradeResp{Code: adminsock.CodeStoreError, Error: err.Error()}
		}
		if len(pending) > 0 {
			return &proto.ClusterUpgradeResp{Code: codeDataplaneNotConverged,
				Error: "the roll finished on every host but " + strconv.Itoa(len(pending)) +
					" expose(s) have not re-confirmed their home: " + strings.Join(pending, ", ")}
		}
		return &proto.ClusterUpgradeResp{OK: true}
	case "acquire-lock", "renew-lock", "expire-lock-lease", "release-lock":
		return handleUpgradeLock(b, &req, now)
	case "maintenance-info":
		if b.cl == nil || b.cl.node == nil || b.cl.node.LeaderContactStale(now) {
			return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: brokerimage.DetailMaintenanceObserverUnsupported}
		}
		status := brokerImageSnapshot(b)
		if status == nil || status.Phase != proto.BrokerImagePhaseServing {
			return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: brokerimage.DetailMaintenanceObserverUnsupported}
		}
		return &proto.ClusterUpgradeResp{OK: true, BrokerImage: status}
	case "maintenance-admission":
		return brokerMaintenanceAdmission(b, &req)
	case "maintenance":
		if req.Maintenance == nil {
			return establishBrokerMaintenance(b, &req)
		}
		return handleUpgradeMaintenance(b, &req, now)
	default:
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: "unknown op: " + req.Op}
	}
}

// origin: hot-update S7 plan §11.4 / S7 BROKER-SLIM-2
func verifyUpgradeTrigger(b *Broker, data []byte, now time.Time) (*proto.ClusterUpgradeReq, *proto.ClusterUpgradeResp) {
	return brokerruntime.VerifyUpgradeTrigger(b.selfID, func() []byte {
		if b.cfg.AuthCallout == nil {
			return nil
		}
		return b.cfg.AuthCallout.AccountSeed
	}, data, now)
}

// legacyUpgradeRefusal is the compatibility adapter to the extracted leaf's
// shared legacy admission (moved here from upgrade.go beside its caller).
// origin: hot-update S8 plan §12.1 / §11.2 P15
func legacyUpgradeRefusal(b *Broker, sid, nid string) (string, string) {
	return brokerupgrade.LegacyAgentRefusal(b.read().SQL(), sid, nid)
}

// brokerUpgradeContext is Run's lifetime for transaction calls from NATS callbacks.
func brokerUpgradeContext(b *Broker) context.Context {
	if b.runCtx != nil {
		return b.runCtx
	}
	return context.Background() // ctx-none: nats.go MsgHandler has no ctx; a unit broker never ran.
}

// origin: hot-update S6 plan §7.1 / T40
func handleUpgradeLock(b *Broker, req *proto.ClusterUpgradeReq, now time.Time) *proto.ClusterUpgradeResp {
	if b.cl == nil || b.cl.node == nil {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeClusterNotEnabled, Error: "cluster not enabled"}
	}
	// The expected-holder CAS is the S8 full-fleet release reservation (brokerupgrade/reservation.go).
	return brokerupgrade.UpgradeLock(b.cl.node, req, now)
}

// The maintenance-protocol helpers live in the broker runtime leaf; these adapters bind the current
// cluster runtime. origin: hot-update S6 plan §7.1 / §9.1; S8 §12.1 (P15 trigger net-zero move)
func handleUpgradeMaintenance(b *Broker, req *proto.ClusterUpgradeReq, now time.Time) *proto.ClusterUpgradeResp {
	if b.cl == nil || b.cl.node == nil {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeClusterNotEnabled, Error: "cluster not enabled"}
	}
	return brokerruntime.MaintenanceRecord(b.cl.node, req, now)
}
func deliverMaintenanceChallenge(nc *nats.Conn, sid string, req proto.BrokerMaintenanceReq, timeout time.Duration, verify func(proto.BrokerMaintenanceAck) error) (proto.BrokerMaintenanceAck, error) {
	return brokerruntime.DeliverMaintenanceChallenge(nc, sid, req, timeout, verify)
}

// checkUpgradeHolder reads the CURRENT cluster runtime's roll lock (brokerupgrade.CheckHolder).
// origin: hot-update S6 plan §7.1 / T40; S8 §12.1 (P15 trigger net-zero move)
func checkUpgradeHolder(b *Broker, req *proto.ClusterUpgradeReq, now time.Time) error {
	if b.cl == nil {
		return brokerupgrade.CheckHolder(nil, req.Holder, now)
	}
	return brokerupgrade.CheckHolder(b.cl.node, req.Holder, now)
}

// Voters install the same signed proof as agents, using their own nonce/t0 and
// applied cursor. The existing account-authenticated trigger subject carries
// this pure protocol; no new subject grant or alternate signature codec exists.
// origin: hot-update S6 internal review rv-cluster-3 / rv-cluster-4
func acceptBrokerMaintenance(b *Broker, req proto.BrokerMaintenanceReq) (proto.BrokerMaintenanceAck, error) {
	var ack proto.BrokerMaintenanceAck
	if b.cl == nil || b.cl.node == nil || req.NID != b.selfID || req.Record.Home != req.ConnectedBrokerID || req.IngressServerID != "" || b.cfg.AuthCallout == nil {
		return ack, brokermaint.ErrIdentity
	}
	pub, err := auth.PublicKeyFromSeed(b.cfg.AuthCallout.AccountSeed)
	if err != nil {
		return ack, err
	}
	body, err := brokermaint.CanonicalRequestBytes(req.Record, req.Proof, req.NID, req.ConnectedBrokerID, req.IngressServerID, req.RouteDigest)
	if err != nil {
		return ack, err
	}
	if err := maintenanceVerify(pub)(body, req.Sig); err != nil {
		return ack, err
	}
	// Leader commit precedes local follower Apply. A challenge can arrive over
	// NATS before the corresponding Raft entry, so wait for the exact record
	// within this RPC instead of classifying propagation lag as no capability.
	// The nonce/proof still consume their original fixed lifetime.
	// origin: hot-update S6 plan §7.1 / internal review rv-cluster-3
	if err := awaitBrokerMaintenanceRecord(b, req.Record); err != nil {
		return ack, err
	}
	// The issuer may include a command committed after PREPARED. Observe that
	// later SQL cursor before sampling role and acknowledging the proof. Both
	// waits consume this RPC and the original challenge's fixed lifetime.
	// origin: hot-update S6 plan §7.1 / T39 / N3 maintenance integration
	if req.Proof != (brokermaint.Proof{}) {
		if err := awaitBrokerMaintenanceCursor(b, req.Proof.AppliedIndex); err != nil {
			return ack, err
		}
	}
	role, err := cluster.ReadImageRole(b.cl.node)
	if err != nil || role.Term != req.Record.Term || role.ConfigIndex != req.Record.ConfigIndex || role.VoterDigest != req.Record.VoterDigest {
		return ack, brokermaint.ErrIdentity
	}
	status := brokerImageSnapshot(b)
	if status == nil || status.Generation == 0 || status.Phase != proto.BrokerImagePhaseServing {
		return ack, brokerimage.ErrMaintenanceObserverUnsupported
	}
	if req.Record.Home == b.selfID && req.Record.Generation != status.Generation {
		return ack, brokermaint.ErrIdentity
	}
	if err := installHomeMaintenance(b); err != nil {
		return ack, err
	}
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		return ack, err
	}
	now, err := procrequest.BootNow()
	if err != nil {
		return ack, err
	}
	route := brokerMaintenanceRouteDigest(b.selfID, boot, role, status.Generation)
	m := &b.cl.homeMaintenance
	m.mu.Lock()
	if m.challenges == nil {
		m.challenges = map[string]brokermaint.Challenge{}
	}
	challenge, exists := m.challenges[req.Record.CutID]
	if !exists {
		if req.Proof != (brokermaint.Proof{}) || len(m.challenges) >= brokerimage.MaxMaintenanceHomes {
			m.mu.Unlock()
			return ack, brokermaint.ErrInvalid
		}
		nonce, e := maintenanceNonce()
		if e != nil {
			m.mu.Unlock()
			return ack, e
		}
		challenge = brokermaint.Challenge{Nonce: nonce, ObserverBootID: boot, IssuerBootID: req.Record.IssuerBootID, SentBootNS: now}
		m.challenges[req.Record.CutID] = challenge
	}
	cache := m.cache
	m.mu.Unlock()
	ack = proto.BrokerMaintenanceAck{CutID: req.Record.CutID, Nonce: challenge.Nonce, NID: b.selfID, ConnectedBrokerID: req.Record.Home, InstanceID: req.Record.InstanceID, RouteDigest: route}
	if req.Proof != (brokermaint.Proof{}) {
		if req.RouteDigest != route {
			return ack, brokermaint.ErrIdentity
		}
		baseline, e := brokerimage.ReadMaintenanceBaseline(b.cl.node.RODB(), req.Record)
		if e != nil {
			return ack, e
		}
		if e := cache.Observe(req.Record, req.Proof, challenge, baseline, now, req.Proof.AppliedIndex, maintenanceVerify(pub)); e != nil {
			return ack, e
		}
		for _, o := range cache.Snapshot().Observations {
			if o.Lease.Record.CutID == req.Record.CutID {
				installed, e := procrequest.BootNow()
				if e != nil || installed >= o.Lease.ExpiresLocalBootNS {
					return ack, brokermaint.ErrExpired
				}
				ack.Accepted = true
				ack.RemainingNS = o.Lease.ExpiresLocalBootNS - installed
			}
		}
		ack.ProofDigest, err = brokermaint.ProofDigest(req.Proof)
		if err != nil {
			return ack, err
		}
	}
	body, err = brokermaint.CanonicalAckBytes(ack.CutID, ack.Nonce, ack.NID, ack.ConnectedBrokerID, ack.IngressServerID, ack.InstanceID, ack.RouteDigest, ack.ProofDigest, ack.Accepted, ack.RemainingNS)
	if err != nil {
		return ack, err
	}
	ack.Sig, err = maintenanceSign(b)(body)
	return ack, err
}

// Both PREPARED challenges and ACTIVE reloads can overtake their committed Raft record on the NATS
// transport (brokerruntime.AwaitMaintenanceRecord / AwaitMaintenanceCursor).
// origin: hot-update S6 plan §7.1 / internal review rv-cluster-3
func awaitBrokerMaintenanceRecord(b *Broker, expected brokermaint.Record) error {
	return brokerruntime.AwaitMaintenanceRecord(b.cl.node, expected)
}
func awaitBrokerMaintenanceCursor(b *Broker, minimum uint64) error {
	return brokerruntime.AwaitMaintenanceCursor(b.cl.node, minimum)
}

// Reissue a duration from the original aggregate deadline, never a new TTL.
// The target consumes network time using its request t0 before arming reload.
// origin: hot-update S6 plan §7.1 / rv-cluster-3
func brokerMaintenanceAdmission(b *Broker, req *proto.ClusterUpgradeReq) *proto.ClusterUpgradeResp {
	if b.cl == nil || b.cl.node == nil || !b.cl.node.IsLeader() {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeNotLeader, Error: "maintenance issuer is not leader"}
	}
	m := &b.cl.homeMaintenance
	m.mu.Lock()
	admission, ok := m.admissions[req.CutID]
	m.mu.Unlock()
	if !ok || admission.Record.Holder != req.Holder || admission.Record.Home != req.TransferTo {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: brokerimage.DetailMaintenanceExpired}
	}
	current, err := cluster.ReadBrokerMaintenance(b.cl.node.RODB(), admission.Record.Home)
	if err != nil || current != admission.Record || current.Phase != brokermaint.MaintActive {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: brokerimage.DetailMaintenanceExpired}
	}
	now, err := procrequest.BootNow()
	if err != nil || admission.Deadline <= now {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: brokerimage.DetailMaintenanceExpired}
	}
	ack := admission.TargetAck
	ack.RemainingNS = admission.Deadline - now
	body, err := brokermaint.CanonicalAckBytes(ack.CutID, ack.Nonce, ack.NID, ack.ConnectedBrokerID, ack.IngressServerID, ack.InstanceID, ack.RouteDigest, ack.ProofDigest, ack.Accepted, ack.RemainingNS)
	if err != nil {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: brokerimage.DetailMaintenanceExpired}
	}
	ack.Sig, err = maintenanceSign(b)(body)
	if err != nil {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: brokerimage.DetailMaintenanceExpired}
	}
	return &proto.ClusterUpgradeResp{OK: true, Maintenance: &current, MaintenanceAck: &ack}
}

func brokerMaintenanceRouteDigest(nodeID, boot string, role storage.RoleReceipt, generation uint64) string {
	return brokerruntime.MaintenanceRouteDigest(nodeID, boot, role, generation)
}
