package broker

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
)

// roster_test.go (formerly b7_roster_test.go) — B7 DOC#3 byte-equivalence guards (the plan's TWO required tests):
// (1) construction-site inertness — rosterForRegister returns nil when selfID=="" (single broker),
// (2) marshal-omits-key — a nil Roster pointer omits the "roster" key entirely.

// origin: b7_roster_test.go (renamed in B6) — docs/reviews/b7-plan.md, docs/reviews/b7-review.md
func TestRosterForRegisterInertWhenSingleBroker(t *testing.T) {
	b := &Broker{} // selfID == "" (single broker / unwired)
	if r := b.rosterForRegister(proto.NodeRegisterReq{}); r != nil {
		t.Fatalf("a single broker (selfID=='') must attach NO roster, got %+v", r)
	}
}

func TestRosterRespMarshalOmitsKeyWhenNil(t *testing.T) {
	blob, err := json.Marshal(proto.NodeRegisterResp{OK: true}) // Roster nil
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(blob), "roster") {
		t.Fatalf("a nil Roster must omit the key (byte-identical to pre-B7): %s", blob)
	}
	// With a roster set, the key IS present.
	blob2, _ := json.Marshal(proto.NodeRegisterResp{OK: true, Roster: &proto.ClusterRoster{Generation: 5}})
	if !strings.Contains(string(blob2), "\"roster\"") {
		t.Fatalf("a non-nil Roster must serialize: %s", blob2)
	}
}

// TestRosterGenerationNeverSuppressesAfterRecover — Stage-C M1: rosterForRegister must NOT gate on
// req.RosterGen (the derived generation can regress on recover/retire). Even a huge cached gen
// must still receive the current roster.
func TestRosterGenerationNeverSuppressesAfterRecover(t *testing.T) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(t.TempDir(), "tether.db")
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES('self','self','p','self','10.0.0.1:7400','nats://10.0.0.1:6222','10.0.0.1:7443','h','sha256:s','VOTER','2026-06-24 00:00:00 +0000 UTC')`); err != nil {
		t.Fatal(err)
	}
	b := &Broker{selfID: "self"}
	b.cfg.AuthCallout = &AuthCalloutConfig{AccountSeed: seed}
	b.cfg.DB = db
	b.cfg.Now = func() time.Time { return time.Unix(1700000000, 0).UTC() }

	// A huge cached generation must NOT suppress delivery (no nil).
	r := b.rosterForRegister(proto.NodeRegisterReq{RosterGen: 1 << 62})
	if r == nil {
		t.Fatal("rosterForRegister must ALWAYS emit (generation is advisory, never a suppression gate)")
	}
	if len(r.Brokers) != 1 || r.Brokers[0].NodeID != "self" {
		t.Fatalf("roster content wrong: %+v", r.Brokers)
	}
}

// TestBuildSignedRosterStampsTTL — External-review re-review: the broker now stamps expires_at so
// the anti-replay seam (clusterroster.VerifyAt) is armed. A roster captured + replayed after the TTL
// is rejected.
func TestBuildSignedRosterStampsTTL(t *testing.T) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	accountPub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(t.TempDir(), "tether.db")
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES('self','self','p','self','10.0.0.1:7400','nats://10.0.0.1:6222','10.0.0.1:7443','h','sha256:s','VOTER','2026-06-24 00:00:00 +0000 UTC')`); err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1700000000, 0).UTC()
	b := &Broker{selfID: "self"}
	b.cfg.AuthCallout = &AuthCalloutConfig{AccountSeed: seed}
	b.cfg.DB = db
	b.cfg.Now = func() time.Time { return base }

	r := b.buildSignedRoster()
	if r == nil || r.ExpiresAt == "" {
		t.Fatalf("broker must stamp a non-empty expires_at, got %+v", r)
	}
	// VerifyAt accepts within the TTL window...
	if err := clusterroster.VerifyAt(r, accountPub, base.Add(time.Hour)); err != nil {
		t.Fatalf("VerifyAt must accept a fresh roster: %v", err)
	}
	// ...and rejects a replay past it.
	if err := clusterroster.VerifyAt(r, accountPub, base.Add(rosterTTL+time.Minute)); err == nil {
		t.Fatal("VerifyAt must reject a roster replayed after its TTL")
	}
}
