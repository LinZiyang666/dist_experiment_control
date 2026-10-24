// home.go is the D6 (distributed data-plane, §6.5/§7.x) home-assignment + cert-pinning seam.
//
// Post-D9 cutover it is LIVE in CLUSTER mode: wireClusterEarly calls AttachClusterSeam to set
// b.selfID (the broker's cluster_nodes node_id) and the stable tunnel cert, so the
// home-assignment + cert-pinning paths activate. In SINGLE mode b.selfID=="" keeps
// homeForRegister / homeForExpose / selfNodeID returning the zero value and the
// tunnelTokenLookup home ladder skipped — every register/expose response stays byte-identical
// to pre-D6. (The per-phase TestD6ProductionWiresNoClusterNode guard was a build-and-prove
// scaffold removed at the D9 cutover — see test/d9.)
package broker

import (
	"crypto/tls"
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"sync"

	"log/slog"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/storage"

	"github.com/LinZiyang666/tether/internal/clusternodes"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// AttachClusterSeam wires the D6 home/cert seam. It sets the broker's cluster identity
// (selfID == its cluster_nodes.node_id, the "self" for the home_broker==self filter) and an
// optional stable tunnel cert so the home-assignment + cert-pinning paths activate. Post-D9 it
// is called by wireClusterEarly in CLUSTER mode; in SINGLE mode it is never called and
// selfID=="" keeps every D6 path inert and byte-identical. Call before Run.
func (b *Broker) AttachClusterSeam(selfID string, tunnelCert *tls.Certificate) {
	b.selfID = selfID
	b.tunnelCert = tunnelCert
}

// selfNodeID returns this broker's cluster_nodes.node_id ("" in production). It
// is the "self" the tunnelTokenLookup home_broker==self filter compares against;
// "" short-circuits the ladder before self is consulted (R-10).
func (b *Broker) selfNodeID() string { return b.selfID }

// connectedBrokerID resolves the actual NATS ingress, not the replying leader.
// Server names are operator-configurable and must never be parsed as node IDs.
func connectedBrokerID(b *Broker, serverID string) string {
	if b.selfID == "" || serverID == "" {
		return ""
	}
	var id string
	if err := b.read().QueryRow(`SELECT node_id FROM cluster_nodes WHERE nats_server_id = ?`, serverID).Scan(&id); err != nil {
		return ""
	}
	return id
}

// TunnelTokenLookupForTest exposes the broker's REGISTER authorizer (with the D6
// home/epoch ladder) so the d6_integration harness can wire it into a
// stand-alone tunnel.Server without spinning the full broker.Run / NATS stack.
// TEST-ONLY: the d6_integration harness calls it; production never does (it wires the
// authorizer into the real tunnel server inside Run instead).
func (b *Broker) TunnelTokenLookupForTest() tunnel.TokenLookup { return b.tunnelTokenLookup }

// newTunnelServer is the D6 cert seam: a STABLE-cert tunnel server when the
// harness attached one, else the production ephemeral self-signed path
// (byte-equivalent to the prior tunnel.NewServer call). Keeps the
// NewServerWithCert( token out of the scanned broker.go.
func (b *Broker) newTunnelServer(addr, host string, lookup tunnel.TokenLookup, logger *slog.Logger) *tunnel.Server {
	if b.tunnelCert != nil {
		return tunnel.NewServerWithCert(addr, host, lookup, b.tunnelCert, logger)
	}
	return tunnel.NewServer(addr, host, lookup, logger)
}

// resolveHomeForAgent maps an agent (sid,nid) to its home cluster node via the
// server-id bridge (§6.5): nodes.nats_server (the deterministic server_name the
// agent last reported) → cluster_nodes by nats_server_id → an ELIGIBLE (VOTER)
// node. Returns nil when there is no binding / no row / the node is not eligible
// (the expose stays un-homed; §7.4 reconvergence retries next reconnect).
func (b *Broker) resolveHomeForAgent(sid, nid string) *clusternodes.HomeNode {
	var natsServer string
	if err := b.read().QueryRow(
		`SELECT nats_server FROM nodes WHERE sid=? AND nid=?`, sid, nid,
	).Scan(&natsServer); err != nil {
		return nil
	}
	home, err := clusternodes.LookupByNatsServer(b.read().SQL(), natsServer)
	if err != nil || !home.Eligible() || home.CertFP == "" {
		// An empty cert_fp would yield CertPins{Current:""} → the agent permanently
		// rejects every dial (ErrHomePinsRequired). Treat it as ineligible (no
		// directive → un-homed, retried next reconnect) rather than emitting a
		// directive that can never be satisfied (review A5 M4).
		return nil
	}
	return home
}

// certPinsFor projects a cluster node's tunnel-cert fingerprints into the wire
// CertPins the agent verifies the home with (§7.7/§15).
func certPinsFor(home *clusternodes.HomeNode) proto.CertPins {
	return proto.CertPins{Current: home.CertFP, Previous: home.CertFPPrev, ValidUntil: home.CertValid}
}

// homeForExpose is the C1 INITIAL-home delivery (DA-12): it returns the HomeDirective the
// broker rides in ExposeForwardedReq.Home for a just-allocated expose. It takes the COMMITTED
// allocation that allocatePort captured (home_broker + epoch were baked into the row by the
// leader's PlanAllocate), so it needs no DB read of its own. Returns nil in single mode
// (selfID=="") or for an un-homed allocation (HomeBroker=="") so the forward stays
// byte-identical.
//
// audit dataplane F1/F3/F7: it uses the authoritative committed home_broker + epoch from the
// allocation — NOT a re-resolution via nodes.nats_server (which could DIVERGE from the row and
// emit a directive tunnelTokenLookup would terminal-deny) and NOT a re-query of the row (whose
// transient read error would silently birth a dead clustered expose). It only resolves the
// home node's CURRENT tunnel_addr + cert pins by node_id.
func (b *Broker) homeForExpose(alloc *port.Allocation) *proto.HomeDirective {
	if b.selfID == "" || alloc == nil || alloc.HomeBroker == "" {
		return nil
	}
	home, err := clusternodes.LookupByNodeID(b.read().SQL(), alloc.HomeBroker)
	if err != nil || !home.Eligible() || home.CertFP == "" {
		// Home not (yet) resolvable / not a VOTER / no usable cert pin → un-homed; §7.4
		// reconvergence retries on the next reconnect (emitting a directive that can never be
		// satisfied would permanently brick the expose).
		b.cfg.Logger.Warn("broker: home not deliverable on expose; leaving un-homed (retried next reconnect)",
			"home_broker", alloc.HomeBroker, "sid", alloc.SID, "nid", alloc.NID, "port", alloc.Port, "err", err)
		return nil
	}
	return &proto.HomeDirective{
		Name: alloc.Name, PublicPort: alloc.Port, NodeID: alloc.HomeBroker,
		BrokerAddr: home.TunnelAddr, Epoch: alloc.Epoch, CertPins: certPinsFor(home),
	}
}

// homeForRegister is the §7.4 rehome-delivery path: on (re)register it reads the
// agent's ALLOCATED, HOMED exposes and returns one epoch-stamped HomeDirective
// per expose (with the home's current tunnel_addr + cert pins resolved by
// node_id). The agent applies each epoch-ordered, so a row whose home_broker was
// re-pointed by the leader drives a rehome on the next reconnect. Returns nil in
// production (selfID=="") so NodeRegisterResp stays byte-identical.
func (b *Broker) homeForRegister(sid, nid string, req proto.NodeRegisterReq) *proto.HomeAssignment {
	rememberAgentMaintenance(b, sid, nid, req)
	if b.selfID == "" {
		return nil
	}
	// Collect the homed exposes FIRST, then close the rows BEFORE resolving each
	// home via a nested query. The cluster DB is opened with SetMaxOpenConns(1)
	// (storage.go), so a LookupByNodeID issued while `rows` still holds the single
	// connection would DEADLOCK. (Drain-then-resolve.)
	type homed struct {
		port       int
		name       string
		homeBroker string
		epoch      int64
	}
	var exposes []homed
	rows, err := b.read().Query(
		`SELECT port, name, home_broker, epoch FROM port_allocations
		  WHERE sid=? AND nid=? AND state='ALLOCATED' AND home_broker != ''
		  ORDER BY port`,
		sid, nid,
	)
	if err != nil {
		b.cfg.Logger.Warn("broker: home-for-register query", "err", err, "sid", sid, "nid", nid)
		return nil
	}
	for rows.Next() {
		var h homed
		if err := rows.Scan(&h.port, &h.name, &h.homeBroker, &h.epoch); err != nil {
			_ = rows.Close()
			b.cfg.Logger.Warn("broker: home-for-register scan", "err", err, "sid", sid, "nid", nid)
			return nil
		}
		exposes = append(exposes, h)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil
	}
	_ = rows.Close()

	var dirs []proto.HomeDirective
	for _, h := range exposes {
		home, err := clusternodes.LookupByNodeID(b.read().SQL(), h.homeBroker)
		if err != nil || !home.Eligible() || home.CertFP == "" {
			// The home node is not (yet) resolvable, is NOT a VOTER (audit dataplane F4: a
			// draining/retiring/non-VOTER home must not receive a rehome directive — it mirrors
			// resolveHomeForAgent + homeForExpose), or has no usable cert pin (review A5 M4); skip
			// — the agent keeps its current home and §7.4 retries on the next reconnect.
			continue
		}
		dirs = append(dirs, proto.HomeDirective{
			Name: h.name, PublicPort: h.port, NodeID: h.homeBroker,
			BrokerAddr: home.TunnelAddr, Epoch: h.epoch, CertPins: certPinsFor(home),
		})
	}
	if len(dirs) == 0 {
		return nil
	}
	return &proto.HomeAssignment{Directives: dirs}
}

type HomeMutationPlanners = brokercontrol.HomeMutationPlanners

// homeMaintenanceRuntime belongs to the current cluster runtime, never a global
// map keyed by Broker pointers. Proofs are supplied by P15 after signature/nonce
// validation. The local clock and proof expiry must survive image import unchanged.
type homeMaintenanceRuntime struct {
	mu           sync.Mutex
	plans        HomeMutationPlanners
	proofs       func() map[string]brokermaint.LocalLease
	now          func() (int64, error)
	returns      map[string]HomeReturnBaseline
	cache        *brokerimage.MaintenanceCache
	challenges   map[string]brokermaint.Challenge
	admissions   map[string]maintenanceAdmission
	firstSeen    map[string]int64
	coordinating bool
	admission    storage.RoleReceipt
	admitted     bool
	cutDeadline  int64
	cutRecord    brokermaint.Record
}
type HomeReturnBaseline struct {
	CutID              string             `json:"cut_id"`
	Record             brokermaint.Record `json:"record"`
	ExpiresLocalBootNS int64              `json:"expires_local_boot_ns"`
	SawDown            bool               `json:"saw_down"`
}

// ConfigureHomeMaintenance is called before starting the current owner's loops.
// The proof source returns a snapshot of P15's validated cache, including ended
// records; it must never mint fresh deadlines while reading a replicated record.
// origin: hot-update S6 plan §7.1–§7.2 / T41 / T43
func ConfigureHomeMaintenance(b *Broker, plans HomeMutationPlanners,
	proofs func() map[string]brokermaint.LocalLease, now func() (int64, error)) error {
	if b.cl == nil || proofs == nil || now == nil || plans.Reassign == nil ||
		plans.Allocate == nil || plans.Rotate == nil || plans.Free == nil {
		return errors.New("broker: incomplete home maintenance wiring")
	}
	m := &b.cl.homeMaintenance
	m.mu.Lock()
	defer m.mu.Unlock()
	m.plans, m.proofs, m.now = plans, proofs, now
	return nil
}

// Install the concrete planners before any cluster home write. Lazy installation
// also covers embedded brokers that attach an already running Node without Run.
// No clock proof is fabricated: a new cache contains no protected homes.
// origin: hot-update S6 internal review rv-cluster-1
func installHomeMaintenance(b *Broker) error {
	m := &b.cl.homeMaintenance
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.plans.Reassign != nil {
		return nil
	}
	boot, err := procrequest.CurrentBootID()
	var cache *brokerimage.MaintenanceCache
	// Portable cold brokers still need concrete home writers. An unsupported
	// image clock yields an empty cache, never a fabricated maintenance lease.
	// origin: hot-update S6 internal review rv-cluster-1
	if errors.Is(err, handoff.ErrUnsupported) {
		err = nil
	} else if err == nil {
		cache, err = brokerimage.NewMaintenanceCache(boot)
	}
	if err != nil {
		return err
	}
	m.cache = cache
	m.plans = HomeMutationPlanners{Reassign: port.PlanReassignHomeGuarded, Allocate: port.PlanAllocateGuarded,
		Rotate: port.PlanRotateAllocationGuarded, Free: port.PlanFreeAllocationGuarded}
	m.proofs = func() map[string]brokermaint.LocalLease {
		out := map[string]brokermaint.LocalLease{}
		if cache == nil {
			return out
		}
		for _, observation := range cache.Snapshot().Observations {
			out[observation.Lease.Record.Home] = observation.Lease
		}
		return out
	}
	m.now = controlbudget.Now
	return nil
}

func homeMaintenanceSnapshot(b *Broker) (HomeMutationPlanners, map[string]brokermaint.LocalLease, int64, error) {
	if b.cl == nil {
		return HomeMutationPlanners{}, nil, 0, errors.New("broker: home mutation requires current cluster runtime")
	}
	if b.cl.node != nil {
		if err := installHomeMaintenance(b); err != nil {
			return HomeMutationPlanners{}, nil, 0, err
		}
	}
	m := &b.cl.homeMaintenance
	m.mu.Lock()
	plans, source, clock := m.plans, m.proofs, m.now
	m.mu.Unlock()
	if clock == nil {
		clock = controlbudget.Now
	}
	now, err := clock()
	if err != nil || now <= 0 {
		return plans, nil, 0, errors.New("broker: home maintenance clock unavailable")
	}
	proofs := map[string]brokermaint.LocalLease{}
	if source != nil {
		for home, lease := range source() {
			proofs[home] = lease
		}
	}
	return plans, proofs, now, nil
}

type HomeMaintenanceDeferredError = brokercontrol.HomeMaintenanceDeferredError

const homeMaintenancePrefix = brokercontrol.HomeMaintenancePrefix

func readHomeMaintenance(db *sql.DB, home string) (string, brokermaint.Record, error) {
	return brokercontrol.ReadHomeMaintenance(db, home)
}

func maintainedHome(record brokermaint.Record, lease brokermaint.LocalLease, now int64) bool {
	return brokercontrol.MaintainedHome(record, lease, now)
}
func matchingHomeLease(record brokermaint.Record, lease brokermaint.LocalLease) bool {
	return brokercontrol.MatchingHomeLease(record, lease)
}

func guardHomeMutation(b *Broker, db *sql.DB, expected *port.Allocation, from, to string, absent bool) (port.MutationGuard, HomeMutationPlanners, error) {
	return brokercontrol.GuardHomeMutation(func() (HomeMutationPlanners, map[string]brokermaint.LocalLease, int64, error) {
		return homeMaintenanceSnapshot(b)
	}, db, expected, from, to, absent)
}
func sameHomeAllocation(a, expected *port.Allocation) bool {
	return brokercontrol.SameHomeAllocation(a, expected)
}

func planHomeReassign(b *Broker, db *sql.DB, expected *port.Allocation, to string) (int64, *cluster.Command, error) {
	guard, plans, err := guardHomeMutation(b, db, expected, expected.HomeBroker, to, false)
	if err != nil {
		return 0, nil, err
	}
	if plans.Reassign == nil {
		return 0, nil, errors.New("broker: guarded home reassignment unavailable")
	}
	return plans.Reassign(db, guard, to, b.cfg.Now())
}
func planHomeAllocate(b *Broker, db *sql.DB, expected *port.Allocation, intent port.AllocationIntent) (*port.Allocation, *cluster.Command, error) {
	from := ""
	if expected != nil {
		from = expected.HomeBroker
	}
	guard, plans, err := guardHomeMutation(b, db, expected, from, intent.HomeBroker, expected == nil)
	if err != nil {
		return nil, nil, err
	}
	if expected != nil {
		if plans.Rotate == nil {
			return nil, nil, errors.New("broker: guarded allocation rotation unavailable")
		}
		return plans.Rotate(db, guard, intent, b.cfg.Now())
	}
	if plans.Allocate == nil {
		return nil, nil, errors.New("broker: guarded home allocation unavailable")
	}
	return plans.Allocate(db, guard, intent)
}

// committedHomeMove returns authority only after Apply and readback. A nil
// command / zero-row CAS is not a successful move and emits no started event.
// origin: hot-update S6 plan §7.2 / T41
func committedHomeMove(b *Broker, existing *port.Allocation, target string) (*port.Allocation, error) {
	var newEpoch int64
	var proposed bool
	err := b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		ne, cmd, err := planHomeReassign(b, db, existing, target)
		newEpoch, proposed = ne, cmd != nil
		return cmd, err
	})
	if err != nil {
		return nil, err
	}
	if !proposed || newEpoch <= existing.Epoch {
		return nil, port.ErrNotFound
	}
	upd, err := port.LookupByTokenHash(b.read().SQL(), existing.TokenHash)
	want := *existing
	want.HomeBroker, want.Epoch = target, newEpoch
	if err != nil {
		return nil, err
	}
	if !sameHomeAllocation(upd, &want) {
		return nil, port.ErrNotFound
	}
	return upd, nil
}

// A reaper maintenance decision observes the live row and pauses only this
// allocation. Neither historical dwell nor the rotation damper is cleared.
// origin: hot-update S6 plan §7.2 / T43
func proxyHomeMaintained(b *Broker, home string) bool {
	// Without a cluster runtime there is no installed proof owner. Selector-only
	// fixtures and single mode must not fabricate protection from a missing owner.
	if b.cl == nil {
		return false
	}
	_, proofs, now, err := homeMaintenanceSnapshot(b)
	if err != nil {
		return true
	} // Unknown clock/read state cannot authorize a rotation.
	raw, record, err := readHomeMaintenance(b.read().SQL(), home)
	if err != nil {
		return true
	}
	return raw != "" && maintainedHome(record, proofs[home], now)
}

// suppressPlannedHomeReturn remembers a down edge that was actually covered by
// a validated proof. Ended records still classify the return; expiry does not.
// It never touches another home's pending auto-rebalance work.
// origin: hot-update S6 plan §7.2 / T43
func suppressPlannedHomeReturn(b *Broker, home string, down, returned bool) bool {
	_, proofs, now, err := homeMaintenanceSnapshot(b)
	if err != nil {
		return false
	}
	raw, record, err := readHomeMaintenance(b.read().SQL(), home)
	if err != nil || raw == "" {
		return false
	}
	lease := proofs[home]
	m := &b.cl.homeMaintenance
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.returns == nil {
		m.returns = map[string]HomeReturnBaseline{}
	}
	baseline, known := m.returns[home]
	if down && maintainedHome(record, lease, now) {
		baseline = HomeReturnBaseline{CutID: record.CutID, Record: record,
			ExpiresLocalBootNS: lease.ExpiresLocalBootNS, SawDown: true}
		m.returns[home] = baseline
		return true
	}
	if returned && known {
		delete(m.returns, home)
		// Phase may have advanced to ended, but replacement holders/cuts may not
		// consume a previous cut's down edge. The original deadline is never renewed.
		prior := baseline.Record
		prior.Phase = record.Phase
		return baseline.SawDown && baseline.CutID == record.CutID && reflect.DeepEqual(prior, record) && (record.Phase == brokermaint.MaintEnded || now < baseline.ExpiresLocalBootNS)
	}
	return false
}

// HomeMaintenanceHistory is private control state for P02's sealed codec. It
// carries the actual pre-cut dwell and return history, not a new healthy baseline.
// Export requires the control loops to have joined; restore precedes their start.
// origin: hot-update S6 plan §2.2 / §7.2 / T43
type HomeMaintenanceHistory struct {
	Dwells       []HomeDwellBaseline              `json:"dwells"`
	Returns      []HomeReturnBaseline             `json:"returns"`
	AutoPending  map[string]int                   `json:"auto_pending"`
	AutoCooldown int                              `json:"auto_cooldown"`
	Cache        *brokerimage.MaintenanceSnapshot `json:"maintenance_cache,omitempty"`
	CutRecord    brokermaint.Record               `json:"cut_record"`
	CutDeadline  int64                            `json:"cut_deadline"`
}
type HomeDwellBaseline struct {
	Port      int    `json:"port"`
	SID       string `json:"sid"`
	NID       string `json:"nid"`
	TokenHash string `json:"token_hash"`
	Home      string `json:"home"`
	Epoch     int64  `json:"epoch"`
	Dwell     int    `json:"dwell"`
}

func ExportHomeMaintenanceHistory(b *Broker) (HomeMaintenanceHistory, error) {
	out := HomeMaintenanceHistory{Dwells: []HomeDwellBaseline{}, Returns: []HomeReturnBaseline{}, AutoPending: map[string]int{}}
	if b.cl == nil {
		return out, nil
	}
	var exportErr error
	b.proxyDwell.Range(func(key, value any) bool {
		p, ok := key.(int)
		dwell, valid := value.(int)
		if !ok || !valid || dwell < 0 {
			exportErr = errors.New("broker: invalid home dwell state")
			return false
		}
		var row HomeDwellBaseline
		row.Port, row.Dwell = p, dwell
		err := b.read().QueryRow(`SELECT sid,nid,token_hash,home_broker,epoch FROM port_allocations WHERE port=? AND state='ALLOCATED'`, p).
			Scan(&row.SID, &row.NID, &row.TokenHash, &row.Home, &row.Epoch)
		if errors.Is(err, sql.ErrNoRows) {
			return true
		} // A committed revoke retired this dwell owner.
		if err != nil {
			exportErr = err
			return false
		}
		out.Dwells = append(out.Dwells, row)
		return true
	})
	if exportErr != nil {
		return HomeMaintenanceHistory{}, exportErr
	}
	m := &b.cl.homeMaintenance
	m.mu.Lock()
	out.CutRecord, out.CutDeadline = m.cutRecord, m.cutDeadline
	if m.cache != nil {
		snapshot := m.cache.Snapshot()
		out.Cache = &snapshot
	}
	for _, baseline := range m.returns {
		out.Returns = append(out.Returns, baseline)
	}
	m.mu.Unlock()
	for home, left := range b.autoRebalanceArm.pending {
		out.AutoPending[home] = left
	}
	out.AutoCooldown = b.autoRebalanceArm.cooldown
	sort.Slice(out.Dwells, func(i, j int) bool { return out.Dwells[i].Port < out.Dwells[j].Port })
	sort.Slice(out.Returns, func(i, j int) bool { return out.Returns[i].Record.Home < out.Returns[j].Record.Home })
	return out, nil
}

// All checks precede installation. A reused public port cannot inherit A's
// failure dwell, and a forged baseline cannot extend the P15 proof's expiry.
// origin: hot-update S6 plan §7.2 / T43
func RestoreHomeMaintenanceHistory(b *Broker, history HomeMaintenanceHistory) error {
	if b.cl == nil || len(history.Dwells) > 16384 || len(history.Returns) > 16384 || len(history.AutoPending) > 16384 ||
		history.AutoCooldown < 0 || history.AutoCooldown > autoRebalanceCooldownTicks {
		return errors.New("broker: invalid home maintenance history")
	}
	_, proofs, _, err := homeMaintenanceSnapshot(b)
	if err != nil {
		return err
	}
	var restoredCache *brokerimage.MaintenanceCache
	if history.Cache != nil {
		if b.cfg.AuthCallout == nil {
			return brokermaint.ErrIdentity
		}
		boot, err := procrequest.CurrentBootID()
		if err != nil {
			return err
		}
		pub, err := auth.PublicKeyFromSeed(b.cfg.AuthCallout.AccountSeed)
		if err != nil {
			return err
		}
		restoredCache, err = brokerimage.RestoreMaintenanceCache(*history.Cache, boot, maintenanceVerify(pub))
		if err != nil {
			return err
		}
		proofs = map[string]brokermaint.LocalLease{}
		for _, o := range restoredCache.Snapshot().Observations {
			proofs[o.Lease.Record.Home] = o.Lease
		}
	}
	if history.CutRecord.CutID == "" && history.CutDeadline != 0 {
		return brokermaint.ErrIdentity
	}
	if history.CutRecord.CutID != "" {
		lease := proofs[history.CutRecord.Home]
		if history.CutRecord.Home != b.selfID || history.CutRecord.Phase != brokermaint.MaintActive ||
			!matchingHomeLease(history.CutRecord, lease) || history.CutDeadline <= 0 || history.CutDeadline > lease.ExpiresLocalBootNS {
			return brokermaint.ErrIdentity
		}
	}
	seenPorts, seenHomes := map[int]bool{}, map[string]bool{}
	for _, row := range history.Dwells {
		a, err := port.LookupByTokenHash(b.read().SQL(), row.TokenHash)
		if err != nil {
			return err
		}
		if seenPorts[row.Port] || row.Dwell < 0 || a == nil || a.Port != row.Port || a.SID != row.SID || a.NID != row.NID ||
			a.HomeBroker != row.Home || a.Epoch != row.Epoch || a.State != port.StateAllocated {
			return errors.New("broker: home dwell allocation identity mismatch")
		}
		seenPorts[row.Port] = true
	}
	for _, baseline := range history.Returns {
		home := baseline.Record.Home
		_, record, err := readHomeMaintenance(b.read().SQL(), home)
		if err != nil {
			return err
		}
		lease := proofs[home]
		prior := baseline.Record
		prior.Phase = record.Phase
		if home == "" || seenHomes[home] || !baseline.SawDown || baseline.CutID != record.CutID ||
			!reflect.DeepEqual(prior, record) || !matchingHomeLease(record, lease) || baseline.ExpiresLocalBootNS <= 0 ||
			baseline.ExpiresLocalBootNS > lease.ExpiresLocalBootNS {
			return errors.New("broker: home return baseline identity mismatch")
		}
		seenHomes[home] = true
	}
	for home, left := range history.AutoPending {
		if home == "" || left < 0 || left > autoRebalanceReturnDwellTicks {
			return errors.New("broker: invalid home return pending state")
		}
	}
	m := &b.cl.homeMaintenance
	m.mu.Lock()
	defer m.mu.Unlock()
	occupied := false
	b.proxyDwell.Range(func(_, _ any) bool { occupied = true; return false })
	if occupied || len(m.returns) > 0 || len(b.autoRebalanceArm.pending) > 0 || b.autoRebalanceArm.cooldown != 0 {
		return errors.New("broker: home maintenance history already installed")
	}
	m.cutRecord, m.cutDeadline = history.CutRecord, history.CutDeadline
	for _, row := range history.Dwells {
		b.proxyDwell.Store(row.Port, row.Dwell)
	}
	if restoredCache != nil {
		m.cache = restoredCache
		m.proofs = func() map[string]brokermaint.LocalLease {
			out := map[string]brokermaint.LocalLease{}
			for _, o := range restoredCache.Snapshot().Observations {
				out[o.Lease.Record.Home] = o.Lease
			}
			return out
		}
	}
	m.returns = map[string]HomeReturnBaseline{}
	for _, baseline := range history.Returns {
		m.returns[baseline.Record.Home] = baseline
	}
	b.autoRebalanceArm.pending = map[string]int{}
	for home, left := range history.AutoPending {
		b.autoRebalanceArm.pending[home] = left
	}
	b.autoRebalanceArm.cooldown = history.AutoCooldown
	return nil
}
