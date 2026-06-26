package broker

import (
	"fmt"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// homes.go (C6 建议5) — the `cluster status --homes` aggregate: every ALLOCATED expose + __proxy__
// allocation with its home broker / epoch / readiness, materialize-then-enrich off the RODB. Leader-
// agnostic; reachability (the leader's §17 lastObserve) is honest only on the leader (is_leader_view).
// Secret-free (no token/psk column is read).

// buildHomesReport is injected as ClusterAdmin.homesReport in wireClusterLate.
func (b *Broker) buildHomesReport() (*adminsock.ClusterHomesReport, error) {
	rep := &adminsock.ClusterHomesReport{
		SchemaVersion: 1,
		IsLeaderView:  b.cl != nil && b.cl.node != nil && b.cl.node.IsLeader(),
		Homes:         []adminsock.HomeEntry{},
		Errors:        []string{},
	}
	// proxy-first, then by port. LEFT JOIN so a row whose home is unknown still lists (PublicURL="").
	rows, err := b.cfg.DB.Query(`
		SELECT pa.name, pa.sid, pa.nid, pa.port, pa.home_broker, pa.epoch, pa.last_rehome_at,
		       COALESCE(cn.public_host,''), COALESCE(cn.phase,''), COALESCE(cn.cert_fp,'')
		FROM port_allocations pa
		LEFT JOIN cluster_nodes cn ON cn.node_id = pa.home_broker
		WHERE pa.state = 'ALLOCATED'
		ORDER BY (pa.name = '__proxy__') DESC, pa.port`)
	if err != nil {
		return nil, fmt.Errorf("homes report: query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type raw struct {
		name, sid, nid, homeBroker, lastRehome, publicHost, phase, certFP string
		port                                                              int
		epoch                                                             int64
	}
	var rs []raw
	for rows.Next() {
		var r raw
		if err := rows.Scan(&r.name, &r.sid, &r.nid, &r.port, &r.homeBroker, &r.epoch, &r.lastRehome,
			&r.publicHost, &r.phase, &r.certFP); err != nil {
			rep.Errors = append(rep.Errors, "scan: "+err.Error())
			rep.Partial = true
			return rep, nil
		}
		rs = append(rs, r)
	}
	if err := rows.Err(); err != nil {
		rep.Errors = append(rep.Errors, "rows: "+err.Error())
		rep.Partial = true
		return rep, nil
	}
	for _, r := range rs {
		e := adminsock.HomeEntry{
			SID: r.sid, NID: r.nid, Name: r.name, Port: r.port, HomeBroker: r.homeBroker,
			Epoch: r.epoch, Reconnects: r.epoch, LastRehomeAt: r.lastRehome,
		}
		if r.name == "__proxy__" {
			e.Kind = "proxy"
			e.ReadyReason = b.proxyReadyFor(r.sid, r.nid, r.homeBroker)
		} else {
			e.Kind = "expose"
			reachable := r.homeBroker == "" || r.homeBroker == b.selfID || b.homeReachable(r.homeBroker)
			e.ReadyReason = exposeReadyReason(r.homeBroker, r.phase, r.certFP, r.publicHost, reachable)
		}
		e.Ready = proxyInSubRender(e.ReadyReason)
		// HR4: PublicURL is where /sub ACTUALLY sends subscribers — only a READY (vended) home has a
		// live one. A not-ready/unreachable home shows "" (matching proxy status, which only projects
		// PublicHost/PublicPort when proxyHomeHealthy), so the URL is never a dead pointer.
		if e.Ready && r.publicHost != "" {
			e.PublicURL = fmt.Sprintf("%s:%d", r.publicHost, r.port)
		}
		rep.Homes = append(rep.Homes, e)
	}
	return rep, nil
}

// proxyReadyFor (BD12, shared) builds the proxy readiness facts for one agent + returns the reason —
// the SAME logic proxyStatusNodesCluster uses, so `--homes` and `proxy status --cluster` agree.
func (b *Broker) proxyReadyFor(sid, nid, homeBroker string) string {
	facts := proxyAgentFacts{
		NodeOnline: b.nodeStatusOnline(sid, nid),
		ProxyReady: b.nodeProxyReady(sid, nid),
	}
	// keyset epoch is not tracked per-node here → leave AgentEpoch==SessionEpoch (never keyset_stale).
	if homeBroker != "" {
		if b.proxyHomeHealthy(homeBroker) {
			facts.HasHome = true
		} else {
			facts.HasHome, facts.HomeCatchingUp = true, true // row has a home but it is rehoming
		}
	}
	return proxyReadyReason(facts)
}

// exposeReadyReason mirrors proxyReadyReason's vocab for a regular expose's home (BD12). A deliverable
// home requires phase==VOTER + cert + public_host (render-equivalence with /sub + the reaper).
func exposeReadyReason(homeBroker, phase, certFP, publicHost string, reachable bool) string {
	switch {
	case homeBroker == "":
		return proxyReasonNoHome
	case phase != "VOTER" || certFP == "" || publicHost == "":
		return proxyReasonCatchingUp
	case !reachable:
		return proxyReasonTunnelDown
	default:
		return proxyReasonReady
	}
}

// nodeStatusOnline reports whether the node row is ONLINE.
func (b *Broker) nodeStatusOnline(sid, nid string) bool {
	var status string
	if err := b.cfg.DB.QueryRow(`SELECT status FROM nodes WHERE sid=? AND nid=?`, sid, nid).Scan(&status); err != nil {
		return false
	}
	return status == "ONLINE"
}
