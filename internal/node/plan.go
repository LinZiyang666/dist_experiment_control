package node

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// Plan* are the D2 leader-side renderers for the node mutators (architecture
// §3.3/§3.5 + docs/reviews/d2-plan.md §3). node.Register binds now.UTC(), so
// PlanRegister passes now.UTC() to LitTime.

// PlanRegister renders OpNodeRegister — the IDENTITY half of node.Register only
// (architecture §3.5). The Apply writes nid/sid/boot_id/release_version/
// proto_version/registered_at/proxy_capable and on conflict updates the mutable
// identity columns. On INSERT it explicitly sets status='OFFLINE' because the
// replicated identity op does not carry a heartbeat; liveness paths are the only
// writers allowed to force ONLINE. On conflict it leaves status/last_heartbeat_at
// untouched. registered_at is set on INSERT and preserved on conflict (matching
// today's UPSERT, which omits it from DO UPDATE). The session FK is checked on the
// leader DB (ErrSessionMissing before proposing).
//
// NOTE (ops-only, §5/§19-D2): the LIVE node.Register stays a single atomic UPSERT
// (identity + liveness) unchanged; this identity-only op is exercised by the lint
// and the equivalence/differential harnesses, where liveness columns are excluded
// from the comparison.
func PlanRegister(db *sql.DB, in RegisterInput, now time.Time) (*cluster.Command, error) {
	var state string
	switch err := db.QueryRow(`SELECT state FROM sessions WHERE sid=?`, in.SID).Scan(&state); err {
	case nil:
		if state != "ACTIVE" {
			return nil, ErrSessionNotActive
		}
	case sql.ErrNoRows:
		return nil, ErrSessionMissing
	default:
		return nil, fmt.Errorf("node: plan register session lookup: %w", err)
	}
	capable := int64(0)
	if in.ProxyCapable {
		capable = 1
	}
	lits, err := cluster.LitTextAll(in.NID, in.SID, in.BootID, in.ReleaseVersion, in.NatsServer)
	if err != nil {
		return nil, fmt.Errorf("node: plan register literal: %w", err)
	}
	nidL, sidL, bootL, relL, natsL := lits[0], lits[1], lits[2], lits[3], lits[4]
	// nats_server (D6 §6.5) is an IDENTITY column written by BOTH register paths
	// so the DIFF-1 equivalence stays consistent. Set on INSERT and refreshed on
	// conflict (matching the live mutator's `nats_server = excluded.nats_server`).
	sql := `INSERT INTO nodes(nid, sid, boot_id, release_version, proto_version, registered_at, proxy_capable, nats_server, status) ` +
		`VALUES (` + nidL + `, ` + sidL + `, ` + bootL + `, ` + relL + `, ` +
		cluster.LitInt(int64(in.ProtoVersion)) + `, ` + cluster.LitTime(now.UTC()) + `, ` + cluster.LitInt(capable) + `, ` + natsL + `, 'OFFLINE') ` +
		`ON CONFLICT(sid, nid) DO UPDATE SET ` +
		`boot_id = excluded.boot_id, release_version = excluded.release_version, ` +
		`proto_version = excluded.proto_version, proxy_capable = excluded.proxy_capable, ` +
		`nats_server = excluded.nats_server`
	return cluster.NewCommand(cluster.OpNodeRegister, cluster.Stmt(sql)), nil
}

// PlanEvict renders OpNodeEvict — the adminsock evict write (DELETE
// agent_provisioning then DELETE nodes for one (sid,nid), matching
// adminsock/server.go:337/346 order). Deleting the node cascades to its
// processes/port_allocations (FK ON DELETE CASCADE). No timestamps.
func PlanEvict(sid, nid string) (*cluster.Command, error) {
	lits, err := cluster.LitTextAll(sid, nid)
	if err != nil {
		return nil, fmt.Errorf("node: plan evict literal: %w", err)
	}
	sidL, nidL := lits[0], lits[1]
	return cluster.NewCommand(cluster.OpNodeEvict,
		cluster.Stmt(`DELETE FROM agent_provisioning WHERE sid=`+sidL+` AND nid=`+nidL),
		cluster.Stmt(`DELETE FROM nodes WHERE sid=`+sidL+` AND nid=`+nidL),
	), nil
}
