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
	"log/slog"

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
	if err := b.cfg.DB.QueryRow(
		`SELECT nats_server FROM nodes WHERE sid=? AND nid=?`, sid, nid,
	).Scan(&natsServer); err != nil {
		return nil
	}
	home, err := clusternodes.LookupByNatsServer(b.cfg.DB, natsServer)
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
	home, err := clusternodes.LookupByNodeID(b.cfg.DB, alloc.HomeBroker)
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
func (b *Broker) homeForRegister(sid, nid string, _ proto.NodeRegisterReq) *proto.HomeAssignment {
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
	rows, err := b.cfg.DB.Query(
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
		home, err := clusternodes.LookupByNodeID(b.cfg.DB, h.homeBroker)
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
