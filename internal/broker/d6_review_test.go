package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

func seedFK(t *testing.T, b *Broker, sid, nid string) {
	t.Helper()
	_, _ = b.cfg.DB.Exec(`INSERT OR IGNORE INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`, sid, sid, "SHA256:o", "h")
	_, _ = b.cfg.DB.Exec(`INSERT OR IGNORE INTO nodes(sid, nid, last_heartbeat_at, status) VALUES (?,?,?,?)`, sid, nid, time.Now().UTC(), "ONLINE")
}

// TestD6InitialHomeReplicatedLadder (review L-2): the build-and-prove harness
// shares one DB, masking that homeForExpose's write is leader-local. This proves
// the REPLICATED initial chain the shared-DB harness skips: a leader bakes a
// homed PlanAllocate, the baked command is applied on a SEPARATE (replica) DB,
// and the ladder on that replica ALLOWs for selfID==home and terminal-denies for
// selfID!=home — i.e. the producer→replica→ladder chain is correct.
func TestD6InitialHomeReplicatedLadder(t *testing.T) {
	leaderDB := openDB(t)
	bLeader := &Broker{cfg: Config{DB: leaderDB, Logger: silentLogger()}}
	seedFK(t, bLeader, "lab", "lab-1")
	alloc, cmd, err := port.PlanAllocate(leaderDB, "lab", "lab-1", "svc", 9000, 0, "fp", "node-B", &port.Config{BandLow: 14000, BandHigh: 14999})
	if err != nil {
		t.Fatalf("plan allocate(home): %v", err)
	}

	// Apply the baked command on a SEPARATE replica DB (simulating raft replication).
	replica := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger()}, selfID: "node-B"}
	seedFK(t, replica, "lab", "lab-1")
	if _, err := replica.cfg.DB.Exec(cmd.Body[0].SQL); err != nil {
		t.Fatalf("apply baked homed INSERT on replica: %v", err)
	}

	// The replica IS the home (node-B) → allow at the baked epoch 0.
	if err := replica.tunnelTokenLookup("lab", "lab-1", alloc.Port, alloc.TokenHash, 0); err != nil {
		t.Fatalf("home replica must ALLOW the replicated homed row, got %v", err)
	}
	// A different broker reading the SAME replicated row is NOT the home → terminal.
	other := &Broker{cfg: Config{DB: replica.cfg.DB, Logger: silentLogger()}, selfID: "node-A"}
	if err := other.tunnelTokenLookup("lab", "lab-1", alloc.Port, alloc.TokenHash, 0); err == nil || err.Error() != "token_unknown_or_revoked" {
		t.Fatalf("non-home broker must terminal-deny the replicated row, got %v", err)
	}
}

// TestD6EmptyCertFPYieldsNoDirective (review A5 M4): a VOTER home with an empty
// cert_fp is treated as ineligible — homeForExpose / homeForRegister emit NO
// directive (un-homed, retried) rather than a permanently un-dialable one.
func TestD6EmptyCertFPYieldsNoDirective(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: "node-self"}
	seedClusterNode(t, b, "node-home", "tether-2", "10.0.0.2:7000", "" /* empty cert_fp */, "VOTER")

	if hd := b.homeForExpose(&port.Allocation{SID: "lab", NID: "lab-1", Name: "svc", Port: 14000, HomeBroker: "node-home", Epoch: 0}); hd != nil {
		t.Fatalf("empty cert_fp home must yield no expose directive, got %+v", hd)
	}
	// homeForRegister against a row already homed to the empty-fp node → no directive.
	seedHomedExpose(t, b, "lab", "lab-1", "svc2", 14001, "th2", "node-home", 1)
	if ha := b.homeForRegister("lab", "lab-1", proto.NodeRegisterReq{}); ha != nil {
		t.Fatalf("empty cert_fp home must yield no register directive, got %+v", ha)
	}
}

// TestD6LadderNegativeStoredEpoch (review A5 M6): a corrupted negative stored
// epoch is a deterministic TERMINAL deny, never an infinite home_catching_up.
func TestD6LadderNegativeStoredEpoch(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: "node-self"}
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "node-self", -1)
	if err := b.tunnelTokenLookup("lab", "lab-1", 14000, "th", 0); err == nil || err.Error() != "token_unknown_or_revoked" {
		t.Fatalf("negative stored epoch must be a deterministic terminal deny, got %v", err)
	}
}

// TestD6ReviewHandleRegisterPersistsServerID covers the broker-level register
// path, not just node.Register. D6 home assignment depends on nodes.nats_server
// being populated from NodeRegisterReq.ServerID; if handleRegister forgets to
// copy it into node.RegisterInput, homeForExpose cannot resolve an initial home.
func TestD6ReviewHandleRegisterPersistsServerID(t *testing.T) {
	db := openDBWithSession(t, "lab")
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger(), Now: func() time.Time {
		return time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC)
	}}}
	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            "lab-1", OS: "linux", Arch: "amd64",
		ServerID: "tether-2",
	})

	b.handleRegister(&nats.Msg{Subject: proto.SubjNodeRegister("lab", "lab-1"), Data: body})

	var got string
	if err := db.QueryRow(`SELECT nats_server FROM nodes WHERE sid='lab' AND nid='lab-1'`).Scan(&got); err != nil {
		t.Fatalf("read registered node: %v", err)
	}
	if got != "tether-2" {
		t.Fatalf("handleRegister did not persist req.ServerID into nodes.nats_server: got %q", got)
	}
}
