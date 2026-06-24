// home.go is the D6 (distributed data-plane, §6.5/§7.x) BUILD-AND-PROVE file.
//
// Everything here is INERT in production: it activates only when the test harness
// has called AttachClusterSeam to set b.selfID (the broker's cluster_nodes
// node_id) and, optionally, a stable tunnel cert. serve.go never calls it, so in
// production b.selfID=="" makes homeForRegister / homeForExpose / selfNodeID
// return the zero value and the tunnelTokenLookup home ladder is skipped — every
// register/expose response stays byte-identical (cutover = D9).
//
// This file is EXCLUDED from the TestD6ProductionWiresNoClusterNode guard scan
// (the build-and-prove mechanism file), like audit_publisher.go for D5. The
// guard bans AttachClusterSeam( / the stable-cert constructors from the SCANNED
// production files so the seam can never be wired there.
package broker

import (
	"crypto/tls"
	"log/slog"

	"github.com/LinZiyang666/tether/internal/clusternodes"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// AttachClusterSeam wires the D6 build-and-prove seam (TEST/HARNESS ONLY). It
// sets the broker's cluster identity (selfID == its cluster_nodes.node_id, the
// "self" for the home_broker==self filter) and an optional stable tunnel cert so
// the home-assignment + cert-pinning paths activate. Production NEVER calls it
// (the guard bans the symbol from scanned files); without it selfID=="" keeps
// every D6 path inert and byte-identical. Call before Run.
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
// TEST-ONLY (like AttachClusterSeam); not called in production.
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

// homeForExpose is the C1 INITIAL-home delivery (DA-12): at expose time it
// resolves the agent's home, STAMPS home_broker onto the just-allocated row (a
// direct mutator grandfathered to the D9 cutover — the FSM PlanAllocate(home)
// form is unit-proven; in real raft this write would be leader-local, so the
// d6_integration harness shares one DB to simulate replication, review L-2), and
// returns the HomeDirective the broker rides in ExposeForwardedReq.Home. Returns
// nil in production (selfID=="") so the forward stays byte-identical.
//
// PRECONDITION: handleExposeReq calls this only after port.Allocate (which is
// INSERT-only, port.go) created a FRESH row, so the row's epoch is the DA-3
// baseline; a same-name re-expose is rejected with ErrNameTaken before reaching
// here. The directive epoch is READ FROM THE ROW (review L-1) — not hardcoded —
// so it stays correct even if a future path presents a non-zero-epoch row.
func (b *Broker) homeForExpose(sid, nid, name string, publicPort int) *proto.HomeDirective {
	if b.selfID == "" {
		return nil
	}
	home := b.resolveHomeForAgent(sid, nid)
	if home == nil {
		return nil
	}
	// D9 §3 (audit #10): home_broker is stamped AT allocate (PlanAllocate's homeBroker arg,
	// baked by the leader via Propose), so here we only READ the epoch — b.cfg.DB is the FSM
	// read-only handle in cluster mode and must not be written directly. (Pre-D9 the seam ran
	// only under the harness, which DID write here; the live cutover moves the stamp to Apply.)
	var epoch int64
	if err := b.cfg.DB.QueryRow(
		`SELECT epoch FROM port_allocations WHERE sid=? AND nid=? AND port=? AND state='ALLOCATED'`,
		sid, nid, publicPort,
	).Scan(&epoch); err != nil {
		b.cfg.Logger.Warn("broker: home epoch read on expose failed", "err", err, "sid", sid, "nid", nid, "port", publicPort)
		return nil
	}
	return &proto.HomeDirective{
		Name: name, PublicPort: publicPort, NodeID: home.NodeID,
		BrokerAddr: home.TunnelAddr, Epoch: epoch, CertPins: certPinsFor(home),
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
		if err != nil || home.CertFP == "" {
			// The home node is not (yet) resolvable, or has no usable cert pin
			// (review A5 M4); skip — the agent keeps its current home and §7.4
			// retries on the next reconnect.
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
