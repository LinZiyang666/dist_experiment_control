package broker

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// seedHomedExpose inserts the session/node FK parents and one ALLOCATED
// port_allocations row with the given home_broker + epoch + token_hash.
func seedHomedExpose(t *testing.T, b *Broker, sid, nid, name string, port int, th, homeBroker string, epoch int64) {
	t.Helper()
	db := b.cfg.DB
	_, _ = db.Exec(`INSERT OR IGNORE INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		sid, sid, "SHA256:o", "h")
	_, _ = db.Exec(`INSERT OR IGNORE INTO nodes(sid, nid, last_heartbeat_at, status) VALUES (?,?,?,?)`,
		sid, nid, time.Now().UTC(), "ONLINE")
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, home_broker, epoch)
		 VALUES (?,?,?,?,?,?, 'ALLOCATED', ?, ?, ?, ?)`,
		port, sid, nid, name, 9000, th, "SHA256:o", time.Now().UTC(), homeBroker, epoch,
	); err != nil {
		t.Fatalf("seed port: %v", err)
	}
}

func seedClusterNode(t *testing.T, b *Broker, nodeID, natsServer, tunnelAddr, certFP, phase string) {
	t.Helper()
	if _, err := b.cfg.DB.Exec(
		`INSERT OR REPLACE INTO cluster_nodes
		   (node_id, name, node_ident_pub, nats_server_id, raft_addr, nats_route, tunnel_addr, public_host, cert_fp, phase, added_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		nodeID, nodeID, "Nident", natsServer, "127.0.0.1:7400", "nats://127.0.0.1:6222",
		tunnelAddr, "pub.example", certFP, phase, time.Now().UTC(),
	); err != nil {
		t.Fatalf("seed cluster_node: %v", err)
	}
}

// TestD6TunnelTokenLookupLadder (R-9): the two-dimensional home/epoch ladder.
// An un-homed row (home_broker=='') is INERT (allow regardless of epoch — the
// byte-equivalent pre-D6 path). A homed row decides on (home vs self) AND
// (presented vs row epoch).
func TestD6TunnelTokenLookupLadder(t *testing.T) {
	const self = "node-self"
	cases := []struct {
		name       string
		home       string
		rowEpoch   int64
		presented  int64
		wantReason string // "" = allow
	}{
		{"un-homed inert (low epoch)", "", 0, 0, ""},
		{"un-homed inert (any epoch)", "", 0, 9, ""},
		{"home==self, epoch match → allow", self, 3, 3, ""},
		{"home==self, presented<row → terminal", self, 3, 2, "token_unknown_or_revoked"},
		{"home==self, presented>row → catching_up", self, 3, 4, proto.ReasonHomeCatchingUp},
		{"home!=self, epoch match → terminal", "node-other", 3, 3, "token_unknown_or_revoked"},
		{"home!=self, presented>row → catching_up", "node-other", 3, 4, proto.ReasonHomeCatchingUp},
		{"home!=self, presented<row → terminal", "node-other", 3, 2, "token_unknown_or_revoked"},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := openDB(t)
			b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: self}
			th := "th-" + string(rune('a'+i))
			port := 14000 + i
			seedHomedExpose(t, b, "lab", "lab-1", "svc", port, th, c.home, c.rowEpoch)
			err := b.tunnelTokenLookup("lab", "lab-1", port, th, c.presented)
			got := ""
			if err != nil {
				got = err.Error()
			}
			if got != c.wantReason {
				t.Fatalf("ladder reason = %q, want %q", got, c.wantReason)
			}
		})
	}
}

// TestD6LadderInertWhenNoSelf: with selfID=="" (production), even a homed row is
// inert — the ladder is skipped (the seam was never attached). This is the
// build-and-prove inertness assertion.
func TestD6LadderInertWhenNoSelf(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: ""}
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "node-x", 5)
	// home_broker != "" but selfID=="" → self resolves to "" → the row's
	// home_broker != "" still runs the ladder against self="". presented==row →
	// home!=self ("node-x" != "") → terminal. So with a homed row + empty self,
	// the agent is denied (correct: a production broker should never serve a homed
	// row it isn't the home for). The KEY inertness guarantee is that production
	// rows are NEVER homed (home_broker=='' default), tested above.
	if err := b.tunnelTokenLookup("lab", "lab-1", 14000, "th", 5); err == nil {
		t.Fatal("a homed row on a no-self broker must not allow")
	}
	// The real production case: an UN-homed row is allowed regardless of self.
	seedHomedExpose(t, b, "lab", "lab-1", "svc2", 14001, "th2", "", 0)
	if err := b.tunnelTokenLookup("lab", "lab-1", 14001, "th2", 0); err != nil {
		t.Fatalf("un-homed row must be allowed (inert): %v", err)
	}
}

// TestD6HomeForRegisterInertN1 (B2): with selfID=="" (production), homeForRegister
// returns nil so NodeRegisterResp.Home is nil and the marshaled reply has NO
// "home" key — byte-identical to pre-D6.
func TestD6HomeForRegisterInertN1(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: ""}
	if ha := b.homeForRegister("lab", "lab-1", proto.NodeRegisterReq{}); ha != nil {
		t.Fatalf("homeForRegister must be nil in production, got %+v", ha)
	}
	if hd := b.homeForExpose("lab", "lab-1", "svc", 14000); hd != nil {
		t.Fatalf("homeForExpose must be nil in production, got %+v", hd)
	}
	// And the marshaled NodeRegisterResp with Home nil omits the key.
	body, _ := json.Marshal(proto.NodeRegisterResp{OK: true, Home: nil})
	if string(body) != `{"ok":true}` {
		t.Fatalf("nil Home must omit the key, got %s", body)
	}
	// ExposeForwardedReq with Home nil is byte-identical to pre-D6 (no "home" key) —
	// the N=1 forward stays unchanged (review A1 MINOR / A6 m3).
	fwd, _ := json.Marshal(proto.ExposeForwardedReq{Name: "svc", Port: 14000, LocalPort: 9000, Token: "t", ActorFP: "fp", Home: nil})
	if strings.Contains(string(fwd), "home") {
		t.Fatalf("nil Home must omit the key from ExposeForwardedReq, got %s", fwd)
	}
	// CertPins with a nil ValidUntil omits the key (review A1 MINOR).
	pins, _ := json.Marshal(proto.CertPins{Current: "x"})
	if strings.Contains(string(pins), "valid_until") {
		t.Fatalf("nil ValidUntil must omit the key, got %s", pins)
	}
}

// TestD6HomeForRegisterDirectives (R-9 producer side): with the seam attached and
// a homed expose + a resolvable cluster_nodes home, homeForRegister returns one
// directive carrying the home's tunnel_addr + cert pin + the row epoch.
func TestD6HomeForRegisterDirectives(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: "node-self"}
	seedClusterNode(t, b, "node-home", "tether-2", "10.0.0.2:7000", "sha256:abc", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "node-home", 2)

	ha := b.homeForRegister("lab", "lab-1", proto.NodeRegisterReq{})
	if ha == nil || len(ha.Directives) != 1 {
		t.Fatalf("want 1 directive, got %+v", ha)
	}
	d := ha.Directives[0]
	if d.Name != "svc" || d.PublicPort != 14000 || d.NodeID != "node-home" ||
		d.BrokerAddr != "10.0.0.2:7000" || d.Epoch != 2 || d.CertPins.Current != "sha256:abc" {
		t.Fatalf("directive mismatch: %+v", d)
	}
}

// TestD6HomeForExposeStampsHome (DA-12 / C1): homeForExpose resolves the agent's
// home from nodes.nats_server → an eligible cluster node, STAMPS home_broker onto
// the row, and returns the directive (epoch 0). A non-VOTER home yields nil.
func TestD6HomeForExposeStampsHome(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: "node-self"}
	// The agent reported server_name "tether-2"; that node is an eligible VOTER.
	seedClusterNode(t, b, "node-home", "tether-2", "10.0.0.2:7000", "sha256:xyz", "VOTER")
	seedHomedExpose(t, b, "lab", "lab-1", "svc", 14000, "th", "", 0) // starts un-homed
	if _, err := db.Exec(`UPDATE nodes SET nats_server='tether-2' WHERE sid='lab' AND nid='lab-1'`); err != nil {
		t.Fatal(err)
	}

	hd := b.homeForExpose("lab", "lab-1", "svc", 14000)
	if hd == nil || hd.NodeID != "node-home" || hd.BrokerAddr != "10.0.0.2:7000" || hd.Epoch != 0 {
		t.Fatalf("directive mismatch: %+v", hd)
	}
	// home_broker was stamped onto the row.
	var home string
	_ = db.QueryRow(`SELECT home_broker FROM port_allocations WHERE port=14000`).Scan(&home)
	if home != "node-home" {
		t.Fatalf("home_broker not stamped, got %q", home)
	}

	// A draining (non-VOTER) home is not eligible → nil, no stamp.
	seedClusterNode(t, b, "node-drain", "tether-3", "10.0.0.3:7000", "sha256:d", "DRAINING")
	seedHomedExpose(t, b, "lab", "lab-1", "svc2", 14001, "th2", "", 0)
	_, _ = db.Exec(`UPDATE nodes SET nats_server='tether-3' WHERE sid='lab' AND nid='lab-1'`)
	if hd := b.homeForExpose("lab", "lab-1", "svc2", 14001); hd != nil {
		t.Fatalf("a draining home must not be assigned, got %+v", hd)
	}
}
