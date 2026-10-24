package maintenance

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/nats-io/nats.go"
)

// Maintenance evidence is shared by local and rolling admission.
type MaintenanceAgent struct {
	SID, NID, Capability, PublicKey string
}

type MaintenancePeer struct {
	ID, SID, NID, Subject, PublicKey string
}

func MaintenanceAgents(db *sql.DB, home string, local bool) ([]MaintenancePeer, error) {
	var peers []MaintenancePeer
	// Only affected current ingress routes need the silence hold. Ordinary home
	// allocations still have their independently guarded from/to SQL decisions.
	// Local reload has no peer home to cover unresolved registrations. Include
	// every active-session agent, including legacy rows without ingress evidence.
	// origin: S6 internal review rv-final F4
	query := `SELECT n.sid,n.nid,COALESCE(m.value,''),n.proxy_capable,n.proxy_ready,p.agent_fp FROM nodes n JOIN sessions s ON s.sid=n.sid LEFT JOIN cluster_meta m ON m.key=?||n.sid||'/'||n.nid LEFT JOIN agent_provisioning p ON p.sid=n.sid AND p.nid=n.nid WHERE s.state='ACTIVE'`
	args := []any{"broker_image_observer."}
	if !local {
		query += ` AND EXISTS(SELECT 1 FROM cluster_nodes c WHERE c.nats_server_id=n.nats_server AND c.node_id=?)`
		args = append(args, home)
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var sid, nid, raw string
		var fp sql.NullString
		var capable, ready bool
		if err := rows.Scan(&sid, &nid, &raw, &capable, &ready, &fp); err != nil {
			return nil, err
		}
		var observer MaintenanceAgent
		if json.Unmarshal([]byte(raw), &observer) != nil || observer.SID != sid || observer.NID != nid || observer.Capability != brokermaint.Capability || capable && !ready {
			return nil, ErrMaintenanceObserverUnsupported
		}
		actual, err := auth.FingerprintFromActor(observer.PublicKey)
		if err != nil || !fp.Valid || actual != fp.String {
			return nil, brokermaint.ErrIdentity
		}
		peers = append(peers, MaintenancePeer{ID: "agent-" + sid + "-" + nid, SID: sid, NID: nid, Subject: proto.SubjCmdForwarded(sid, nid, proto.BrokerMaintenanceVerb), PublicKey: observer.PublicKey})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(peers) > MaxMaintenanceHomes {
		return nil, brokermaint.ErrInvalid
	}
	return peers, nil
}

// Challenge and installation are separate authenticated legs. An observer's
// route, nonce and proof digest must remain bound throughout both exchanges.
// origin: hot-update S6 plan §7.1 / T39 / T42
func CollectMaintenanceObserver(record brokermaint.Record, peer MaintenancePeer, send func(proto.BrokerMaintenanceReq, func(proto.BrokerMaintenanceAck) error) (proto.BrokerMaintenanceAck, error), appliedIndex func() (uint64, error), sign brokermaint.SignProof, verifySignature brokermaint.VerifySignature) (MaintenanceObserver, MaintenanceAck, error) {
	sent, e := procrequest.BootNow()
	if e != nil {
		return MaintenanceObserver{}, MaintenanceAck{}, e
	}
	challengeReq := proto.BrokerMaintenanceReq{Record: record, NID: peer.NID, ConnectedBrokerID: record.Home, IngressServerID: record.IngressServerID}
	verify := func(ack proto.BrokerMaintenanceAck) error {
		body, e := brokermaint.CanonicalAckBytes(ack.CutID, ack.Nonce, ack.NID, ack.ConnectedBrokerID, ack.IngressServerID, ack.InstanceID, ack.RouteDigest, ack.ProofDigest, ack.Accepted, ack.RemainingNS)
		if e != nil {
			return e
		}
		if ack.CutID != record.CutID || ack.NID != peer.NID || ack.ConnectedBrokerID != record.Home || ack.IngressServerID != record.IngressServerID || ack.InstanceID != record.InstanceID {
			return brokermaint.ErrIdentity
		}
		return verifySignature(body, ack.Sig)
	}
	challenge, e := send(challengeReq, verify)
	if e != nil || challenge.Accepted {
		return MaintenanceObserver{}, MaintenanceAck{}, fmt.Errorf("observer %s challenge: %w", peer.ID, errors.Join(brokermaint.ErrIdentity, e))
	}
	response, e := procrequest.BootNow()
	if e != nil {
		return MaintenanceObserver{}, MaintenanceAck{}, e
	}
	applied, e := appliedIndex()
	if e != nil {
		return MaintenanceObserver{}, MaintenanceAck{}, e
	}
	proof, e := brokermaint.MakeProof(record, challenge.Nonce, record.IssuerBootID, response, applied, sign)
	if e != nil {
		return MaintenanceObserver{}, MaintenanceAck{}, e
	}
	challengeReq.Proof, challengeReq.RouteDigest = proof, challenge.RouteDigest
	ack, e := send(challengeReq, verify)
	if e != nil || !ack.Accepted || ack.Nonce != proof.Nonce || ack.RouteDigest != challenge.RouteDigest {
		return MaintenanceObserver{}, MaintenanceAck{}, fmt.Errorf("observer %s installation: %w", peer.ID, errors.Join(brokermaint.ErrIdentity, e))
	}
	proofDigest, _ := brokermaint.ProofDigest(proof)
	return MaintenanceObserver{ID: peer.ID, NID: peer.NID, ConnectedBrokerID: record.Home, IngressServerID: record.IngressServerID, RouteDigest: ack.RouteDigest, Nonce: proof.Nonce, ProofDigest: proofDigest, Capability: brokermaint.Capability, SentBootNS: sent}, MaintenanceAck{ObserverID: peer.ID, Reply: ack}, nil
}

// A local cut has the same fixed issuer lifetime and installed observer receipts
// as a rolling cut. Empty fleets have no remote hold to install. The caller owns
// current-role/holder checks and the serialized record writer.
// origin: S6 internal review rv-final F4
func EstablishLocalMaintenance(record brokermaint.Record, peers []MaintenancePeer, commit func(brokermaint.Record) error, collect func(brokermaint.Record, MaintenancePeer) (MaintenanceObserver, MaintenanceAck, error), sign brokermaint.SignProof, verify func(MaintenancePeer) brokermaint.VerifySignature) (brokermaint.LocalLease, error) {
	if err := commit(record); err != nil {
		return brokermaint.LocalLease{}, err
	}
	success := false
	defer func() {
		if !success {
			ended := record
			ended.Phase = brokermaint.MaintEnded
			_ = commit(ended)
		}
	}()
	required := make([]MaintenanceObserver, len(peers))
	acks := make([]MaintenanceAck, len(peers))
	for i, peer := range peers {
		var err error
		required[i], acks[i], err = collect(record, peer)
		if err != nil {
			return brokermaint.LocalLease{}, err
		}
	}
	proof, err := brokermaint.MakeProof(record, record.CutID, record.IssuerBootID, record.IssuedBootNS, 0, sign)
	if err != nil {
		return brokermaint.LocalLease{}, err
	}
	record.Phase = brokermaint.MaintActive
	now, err := procrequest.BootNow()
	if err != nil {
		return brokermaint.LocalLease{}, err
	}
	bound := record.ExpiresBootNS
	if len(peers) != 0 {
		bound, err = MaintenanceCutDeadline(record, record.IssuerBootID, now, required, acks, func(observer MaintenanceObserver, body []byte, sig string) error {
			for _, peer := range peers {
				if peer.ID == observer.ID {
					return verify(peer)(body, sig)
				}
			}
			return brokermaint.ErrIdentity
		})
		if err != nil {
			return brokermaint.LocalLease{}, err
		}
	}
	if err := commit(record); err != nil {
		return brokermaint.LocalLease{}, err
	}
	success = true
	return brokermaint.LocalLease{Record: record, Proof: proof, ExpiresLocalBootNS: bound}, nil
}

// Single uses the same canonical record transaction as the replicated planner.
// The single SQL pool has one connection; all record/readback work finishes
// before the owner sends challenges or allows Run to quiesce that pool.
// origin: S6 internal review rv-final F4
func MaintenanceWrite(db *sql.DB, node *cluster.Node, plan func(*sql.DB) (*cluster.Command, error)) error {
	if node != nil {
		return node.Propose(plan)
	}
	command, err := plan(db)
	if err != nil || command == nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, statement := range command.Body {
		if _, err := tx.Exec(statement.SQL, statement.Args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// LocalMaintenanceConfig borrows the current authority; it never opens a store.
type LocalMaintenanceConfig struct {
	DB         *sql.DB
	Node       *cluster.Node
	NC         *nats.Conn
	Instance   string
	Generation uint64
	Now        func() time.Time
	Sign       brokermaint.SignProof
	Verify     func(string) brokermaint.VerifySignature
	Collect    func(brokermaint.Record, MaintenancePeer) (MaintenanceObserver, MaintenanceAck, error)
}

func AdmitLocalMaintenance(c LocalMaintenanceConfig, deadline controlbudget.Deadline) (brokermaint.LocalLease, error) {
	peers, err := MaintenanceAgents(c.DB, "", true)
	if err != nil {
		return brokermaint.LocalLease{}, fmt.Errorf("%w: %w", ErrMaintenanceObserverUnsupported, err)
	}
	if len(peers) == 0 {
		return brokermaint.LocalLease{}, nil
	}
	if c.NC == nil {
		return brokermaint.LocalLease{}, ErrMaintenanceObserverUnsupported
	}
	cut, err := MaintenanceNonce()
	if err != nil {
		return brokermaint.LocalLease{}, err
	}
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		return brokermaint.LocalLease{}, err
	}
	now, err := procrequest.BootNow()
	if err != nil {
		return brokermaint.LocalLease{}, err
	}
	mode, home, ingress, instance := proto.BrokerImageModeSingle, "", c.NC.ConnectedServerId(), c.Instance
	if instance == "" {
		instance = cut
	}
	var role storage.RoleReceipt
	if c.Node != nil {
		mode, home, ingress, instance = proto.BrokerImageModeSoleVoter, c.Node.SelfID(), "", c.Node.SelfID()
		role, err = cluster.ReadImageRole(c.Node)
		if err != nil {
			return brokermaint.LocalLease{}, err
		}
	}
	// A single owner has no leader reaper. Same-boot expiry is a positive proof
	// that an old local cut cannot protect routes or block the next admission.
	// origin: S6 internal review rv-final F4
	if old, e := ReadLocalMaintenance(c.DB, home); e == nil && old.Phase != brokermaint.MaintEnded {
		owned, e := BindLocalMaintenance(c.DB, brokermaint.LocalLease{Record: old})
		if e != nil || owned.Record.CutID == "" {
			return brokermaint.LocalLease{}, brokermaint.ErrConflict
		}
		if old.IssuerBootID != boot || now < old.ExpiresBootNS {
			return brokermaint.LocalLease{}, brokermaint.ErrConflict
		}
		if e := FinishLocalMaintenance(c.DB, c.Node, brokermaint.LocalLease{Record: old}, c.Now()); e != nil {
			return brokermaint.LocalLease{}, e
		}
	}
	baseline := MaintenanceBaseline{Home: home, InstanceID: instance}
	digest, _ := MaintenanceBaselineDigest(baseline)
	record := brokermaint.Record{CutID: cut, Holder: "local-" + cut, Home: home, InstanceID: instance, IssuerBootID: boot, Generation: c.Generation, Mode: mode, IngressServerID: ingress, Phase: brokermaint.MaintPrepared, IssuedBootNS: now, ExpiresBootNS: now + int64(brokermaint.MaxLifetime), Term: role.Term, ConfigIndex: role.ConfigIndex, VoterDigest: role.VoterDigest, BaselineDigest: digest, RequiredCapability: brokermaint.Capability}
	if c.Node != nil {
		if err := MaintenanceWrite(c.DB, c.Node, func(db *sql.DB) (*cluster.Command, error) {
			return cluster.PlanSetUpgradeActiveExpected(db, record.Holder, c.Now())
		}); err != nil {
			return brokermaint.LocalLease{}, err
		}
	}
	success := false
	defer func() {
		if !success {
			_ = FinishLocalMaintenance(c.DB, c.Node, brokermaint.LocalLease{Record: record}, c.Now())
		}
	}()
	commit := func(next brokermaint.Record) error { return commitLocalMaintenance(c, next, baseline) }
	lease, err := EstablishLocalMaintenance(record, peers, commit, c.Collect, c.Sign, func(p MaintenancePeer) brokermaint.VerifySignature { return c.Verify(p.PublicKey) })
	if err != nil {
		return brokermaint.LocalLease{}, fmt.Errorf("%w: %w", ErrMaintenanceObserverUnsupported, err)
	}
	if lease.ExpiresLocalBootNS < int64(deadline) {
		return brokermaint.LocalLease{}, brokermaint.ErrExpired
	}
	success = true
	return lease, nil
}

func commitLocalMaintenance(c LocalMaintenanceConfig, record brokermaint.Record, baseline MaintenanceBaseline) error {
	err := MaintenanceWrite(c.DB, c.Node, func(db *sql.DB) (*cluster.Command, error) {
		old, err := ReadLocalMaintenance(db, record.Home)
		var expected *brokermaint.Record
		if err == nil {
			expected = &old
		} else if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		command, err := PlanMaintenanceRecord(db, expected, record, baseline)
		if err == nil {
			raw, _ := brokermaint.CanonicalRecordBytes(record)
			// A stale zero-row record CAS cannot relabel a remote or newer owner.
			command.Body = append(command.Body, cluster.Stmt(`INSERT INTO cluster_meta(key,value) SELECT `+cluster.MustLitText("broker_image_local."+record.Home)+`,`+cluster.MustLitText(record.CutID)+` WHERE EXISTS(SELECT 1 FROM cluster_meta WHERE key=`+cluster.MustLitText(brokermaint.MetaKeyPrefix+record.Home)+` AND value=`+cluster.MustLitText(string(raw))+`) ON CONFLICT(key) DO UPDATE SET value=excluded.value`))
		}
		return command, err
	})
	if err != nil {
		return err
	}
	actual, err := ReadLocalMaintenance(c.DB, record.Home)
	if err != nil || actual != record {
		return brokermaint.ErrConflict
	}
	return nil
}

func MaintenanceNonce() (string, error) {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(nonce[:]), nil
}

// Keep only a locally mapped conservative bound in reload state. This request
// asks the issuer for its aggregate; comparing its absolute clock would be unsafe.
// origin: hot-update S6 internal review rv-cluster-3
func AdmitRemoteMaintenance(node *cluster.Node, self string, seed []byte, generation uint64, req adminsock.Request, deadline controlbudget.Deadline, send func(*proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error), snapshot func() (map[string]brokermaint.LocalLease, int64, error), route func(string, storage.RoleReceipt) string, verify func(string) brokermaint.VerifySignature) (brokermaint.Record, int64, error) {
	if req.CutID == "" || req.Holder == "" || len(seed) == 0 {
		return brokermaint.Record{}, 0, ErrMaintenanceObserverUnsupported
	}
	_, leader := node.LeaderWithID()
	if leader == "" {
		return brokermaint.Record{}, 0, ErrMaintenanceObserverUnsupported
	}
	sent, err := procrequest.BootNow()
	if err != nil {
		return brokermaint.Record{}, 0, err
	}
	response, err := send(&proto.ClusterUpgradeReq{Op: "maintenance-admission", TargetNode: leader, TransferTo: self, Holder: req.Holder, CutID: req.CutID})
	if err != nil || response.Maintenance == nil || response.MaintenanceAck == nil {
		return brokermaint.Record{}, 0, ErrMaintenanceObserverUnsupported
	}
	record, ack := *response.Maintenance, *response.MaintenanceAck
	current, err := cluster.ReadBrokerMaintenance(node.RODB(), self)
	if err != nil || record != current || record.Holder != req.Holder || record.CutID != req.CutID || record.Phase != brokermaint.MaintActive {
		return brokermaint.Record{}, 0, brokermaint.ErrIdentity
	}
	role, err := cluster.ReadImageRole(node)
	if err != nil || role.Term != record.Term || role.ConfigIndex != record.ConfigIndex || role.VoterDigest != record.VoterDigest {
		return brokermaint.Record{}, 0, brokermaint.ErrIdentity
	}
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		return brokermaint.Record{}, 0, err
	}

	if generation != record.Generation || ack.RouteDigest != route(boot, role) {
		return brokermaint.Record{}, 0, brokermaint.ErrIdentity
	}
	proofs, now, err := snapshot()
	if err != nil {
		return brokermaint.Record{}, 0, err
	}
	lease := proofs[self]
	proofDigest, err := brokermaint.ProofDigest(lease.Proof)
	if err != nil || !brokermaint.Protects(lease, record, now) || !ack.Accepted || ack.NID != self || ack.ConnectedBrokerID != self || ack.IngressServerID != "" || ack.CutID != req.CutID || ack.InstanceID != record.InstanceID || ack.Nonce != lease.Proof.Nonce || ack.ProofDigest != proofDigest {
		return brokermaint.Record{}, 0, brokermaint.ErrIdentity
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		return brokermaint.Record{}, 0, err
	}
	body, err := brokermaint.CanonicalAckBytes(ack.CutID, ack.Nonce, ack.NID, ack.ConnectedBrokerID, ack.IngressServerID, ack.InstanceID, ack.RouteDigest, ack.ProofDigest, ack.Accepted, ack.RemainingNS)
	if err != nil || verify(pub)(body, ack.Sig) != nil {
		return brokermaint.Record{}, 0, brokermaint.ErrIdentity
	}
	bound := min(sent+ack.RemainingNS, lease.ExpiresLocalBootNS)
	if bound < int64(deadline) || bound <= now {
		return brokermaint.Record{}, 0, brokermaint.ErrExpired
	}
	return record, bound, nil
}

// End only the exact local owner. A failed admission never leaves the long
// rolling holder blocking membership, and a returned cut cannot release a peer.
// origin: S6 internal review rv-final F4
func FinishLocalMaintenance(db *sql.DB, node *cluster.Node, lease brokermaint.LocalLease, now time.Time) error {
	record := lease.Record
	current, err := ReadLocalMaintenance(db, record.Home)
	if err == nil && current.CutID == record.CutID && current.Phase != brokermaint.MaintEnded {
		current.Phase = brokermaint.MaintEnded
		err = commitLocalMaintenance(LocalMaintenanceConfig{DB: db, Node: node}, current, MaintenanceBaseline{Home: record.Home, InstanceID: record.InstanceID})
	}
	if node != nil {
		err = errors.Join(err, MaintenanceWrite(db, node, func(writer *sql.DB) (*cluster.Command, error) {
			return cluster.PlanClearUpgradeActiveExpected(writer, record.Holder, now)
		}))
	}
	return err
}

// Single's empty SQL home is intentional; the signed mode/ingress carries its
// route identity. Cluster's home-key validator cannot read this local record.
// origin: S6 internal review rv-final F4
func ReadLocalMaintenance(db *sql.DB, home string) (brokermaint.Record, error) {
	var raw string
	if err := db.QueryRow("SELECT value FROM cluster_meta WHERE key=?", brokermaint.MetaKeyPrefix+home).Scan(&raw); err != nil {
		return brokermaint.Record{}, err
	}
	return brokermaint.DecodeRecord([]byte(raw))
}

// Holder spelling is not ownership: a remote roll may choose the same prefix.
// Reopened authority binds the imported lease to the durable local admission.
// origin: S6 internal review rv-final F4
func BindLocalMaintenance(db *sql.DB, lease brokermaint.LocalLease) (brokermaint.LocalLease, error) {
	if lease.Record.CutID == "" {
		return lease, nil
	}
	var cut string
	err := db.QueryRow("SELECT value FROM cluster_meta WHERE key=?", "broker_image_local."+lease.Record.Home).Scan(&cut)
	if errors.Is(err, sql.ErrNoRows) || err == nil && cut != lease.Record.CutID {
		return brokermaint.LocalLease{}, nil
	}
	return lease, err
}
