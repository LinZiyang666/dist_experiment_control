// Package clusternodes is the read side of the cluster_nodes roster (migration
// 0008), used by the broker's D6 server-id bridge to resolve an agent's home
// broker from the deterministic nats server_name it reports.
//
// It is deliberately a PURE-SQL leaf (architecture L-2): it imports NEITHER
// nats.go NOR internal/cluster (raft), so it can live below the broker without
// dragging the consensus or messaging layers into the home-resolution path.
// D7's ClusterNodeUpsert writer will co-locate here later; D6 only reads (rows
// are seeded by the test harness in build-and-prove, written by D7 in
// production).
package clusternodes

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrNotFound is returned by LookupByNatsServer when no cluster_nodes row maps
// the given nats server_name.
var ErrNotFound = errors.New("clusternodes: no node for nats server")

// PhaseVoter is the only home-eligible phase (D6 §6.5/§18.3): a node serves as a
// home broker iff it is a healthy voter. All other phases (joining, catching up,
// draining, retiring, add-failed) are NOT eligible; the caller skips them.
const PhaseVoter = "VOTER"

// HomeNode is the projection of one cluster_nodes row needed for home
// assignment + tunnel cert pinning (D6 §6.5/§7.7).
type HomeNode struct {
	NodeID     string // raft ServerID == the home broker identity written into port_allocations.home_broker
	NatsServer string // cluster_nodes.nats_server_id (the deterministic server_name the agent reports)
	TunnelAddr string // host:port the agent dials for the reverse tunnel
	PublicHost string
	CertFP     string     // current tunnel-cert fingerprint ("sha256:"+hex), the pin
	CertFPPrev string     // previous fingerprint, non-empty only during a rotation window
	CertValid  *time.Time // rotation window end; nil outside a rotation
	Phase      string     // raw phase; eligibility (== PhaseVoter) is decided by the caller
}

// Eligible reports whether this node may be assigned as a home broker (phase is
// VOTER). The caller (broker home resolution) skips ineligible nodes and emits
// no directive, letting §7.4 reconvergence pick it up on the next reconnect.
func (n *HomeNode) Eligible() bool { return n.Phase == PhaseVoter }

const selectHomeNode = `SELECT node_id, nats_server_id, tunnel_addr, public_host, cert_fp,
        cert_fp_prev, cert_fp_valid_until, phase FROM cluster_nodes WHERE `

// LookupByNatsServer resolves a cluster_nodes row by the nats server_name an
// agent reported (NodeRegisterReq.ServerID == nc.ConnectedServerName()). Used for
// the INITIAL home resolution (the agent's current connected broker, §6.5). An
// empty server yields ErrNotFound without a query (no binding ⇒ no home). The
// caller checks HomeNode.Eligible() before assigning the node as a home.
func LookupByNatsServer(db *sql.DB, server string) (*HomeNode, error) {
	if server == "" {
		return nil, ErrNotFound
	}
	return scanHomeNode(db.QueryRow(selectHomeNode+`nats_server_id = ?`, server))
}

// LookupByNodeID resolves a cluster_nodes row by its raft ServerID (==
// cluster_nodes.node_id == port_allocations.home_broker). Used for REHOME
// DELIVERY (§7.4): each of the agent's ALLOCATED exposes already carries a
// home_broker node_id, and the register-reply directive needs that home's
// tunnel_addr + cert pins. An empty node yields ErrNotFound without a query.
func LookupByNodeID(db *sql.DB, nodeID string) (*HomeNode, error) {
	if nodeID == "" {
		return nil, ErrNotFound
	}
	return scanHomeNode(db.QueryRow(selectHomeNode+`node_id = ?`, nodeID))
}

func scanHomeNode(row *sql.Row) (*HomeNode, error) {
	var (
		n        HomeNode
		certPrev sql.NullString
		validU   sql.NullTime
	)
	err := row.Scan(&n.NodeID, &n.NatsServer, &n.TunnelAddr, &n.PublicHost, &n.CertFP,
		&certPrev, &validU, &n.Phase)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrNotFound
	case err != nil:
		return nil, fmt.Errorf("clusternodes: scan home node: %w", err)
	}
	if certPrev.Valid {
		n.CertFPPrev = certPrev.String
	}
	if validU.Valid {
		t := validU.Time
		n.CertValid = &t
	}
	return &n, nil
}
