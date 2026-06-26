package broker

import (
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/storage"
)

// roster_gen_floor_test.go (C1 Stage-C BLOCKER-1 guard) — buildSignedRoster must FLOOR the wire
// generation with the derived-max so a mixed-version (v0.4.0→C1) rollout never serves a gen LOWER
// than what v0.4.0 stamped (which would make a strict-monotone agent reject every C1 roster).

func TestRosterGenRollingUpgradeFloor(t *testing.T) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	db, err := storage.OpenWAL("file:" + filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// One VOTER with a high added_at → the derived-max is a big UnixNano; cluster_meta
	// roster_generation is ABSENT (the counter reads 0, as on a fresh D9→C1 upgrade before any op).
	if _, err := db.Exec(`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES('self','self','p','self','10.0.0.1:7400','nats://10.0.0.1:6222','10.0.0.1:7443','h','sha256:s','VOTER','2026-06-24 00:00:00 +0000 UTC')`); err != nil {
		t.Fatal(err)
	}
	b := &Broker{selfID: "self"}
	b.cfg.AuthCallout = &AuthCalloutConfig{AccountSeed: seed}
	b.cfg.DB = db
	b.cfg.Now = func() time.Time { return time.Unix(1700000000, 0).UTC() }

	r := b.buildSignedRoster()
	if r == nil || r.Generation == 0 {
		t.Fatalf("wire gen must be FLOORED to the derived-max (non-zero) when the counter is unseeded; got %+v", r)
	}
	floored := r.Generation

	// Once the counter exceeds the derived-max (the first real membership op bumps it past now),
	// the counter governs.
	higher := floored + 1_000_000
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('roster_generation', ?)`, strconv.FormatUint(higher, 10)); err != nil {
		t.Fatal(err)
	}
	r2 := b.buildSignedRoster()
	if r2 == nil || r2.Generation != higher {
		t.Fatalf("once the counter exceeds the derived-max it must govern the wire gen: got %d want %d", r2.Generation, higher)
	}
}
