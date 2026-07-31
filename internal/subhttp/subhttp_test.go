package subhttp

import (
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proxysub"
	"github.com/LinZiyang666/tether/internal/storage"
)

func newDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES('lab','lab','SHA256:o','x')`); err != nil {
		t.Fatal(err)
	}
	return db
}

// seedNode inserts a node + its __proxy__ allocation. ready/online toggle the
// two render gates so we can prove exactly which nodes appear.
func seedNode(t *testing.T, db *sql.DB, nid string, port int, status string, ready int) {
	t.Helper()
	// A renderable exit node is proxy-capable (round-6 F9 render gate).
	if _, err := db.Exec(
		`INSERT INTO nodes(sid,nid,last_heartbeat_at,status,proxy_ready,proxy_capable) VALUES('lab',?,?,?,?,1)`,
		nid, time.Now(), status, ready,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port,sid,nid,name,local_port,token_hash,state,created_by_fp,created_at)
		 VALUES(?,'lab',?,'__proxy__',0,?,'ALLOCATED','',?)`,
		port, nid, "h"+nid, time.Now(),
	); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, db *sql.DB, path string) *http.Response {
	t.Helper()
	h := Handler(Config{DB: db, PublicHost: "broker.example.com"})
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w.Result()
}

func TestSubRendersOnlyLiveNodes(t *testing.T) {
	db := newDB(t)
	s, err := proxysub.Create(db, "lab", "alice", "SHA256:o", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// Proxy ON (round-6 F9: render requires the authoritative master switch).
	if _, err := db.Exec(`UPDATE sessions SET proxy_enabled=1 WHERE sid='lab'`); err != nil {
		t.Fatal(err)
	}
	// online+ready → rendered; online+not-ready → excluded; offline+ready → excluded.
	seedNode(t, db, "n-online-ready", 14000, "ONLINE", 1)
	seedNode(t, db, "n-online-unready", 14001, "ONLINE", 0)
	seedNode(t, db, "n-offline-ready", 14002, "OFFLINE", 1)

	resp := get(t, db, "/sub/"+s.Token)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/yaml") {
		t.Errorf("content-type=%q", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("cache-control=%q (token must not be cached)", cc)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, "n-online-ready") {
		t.Errorf("live node must be rendered:\n%s", body)
	}
	if strings.Contains(body, "n-online-unready") || strings.Contains(body, "n-offline-ready") {
		t.Errorf("non-live node leaked into render:\n%s", body)
	}
	// The subscriber's PSK is vended; the cipher is the locked one.
	if !strings.Contains(body, s.PSK) || !strings.Contains(body, "chacha20-ietf-poly1305") {
		t.Errorf("render missing psk/cipher:\n%s", body)
	}
	// 14000 is the live node's port; the excluded ports must NOT appear.
	if !strings.Contains(body, "14000") || strings.Contains(body, "14001") || strings.Contains(body, "14002") {
		t.Errorf("wrong ports rendered:\n%s", body)
	}
}

// Unknown, revoked, and DELETING-session tokens must ALL return the identical
// 404 — no existence oracle (a 410-on-revoked would leak that the token was
// once valid).
func TestSubNoExistenceOracle(t *testing.T) {
	db := newDB(t)
	s, _ := proxysub.Create(db, "lab", "alice", "SHA256:o", time.Now())
	seedNode(t, db, "n1", 14000, "ONLINE", 1)

	unknown := get(t, db, "/sub/deadbeefdeadbeefdeadbeef")
	if unknown.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown token: status=%d want 404", unknown.StatusCode)
	}
	unknownBody := readBody(t, unknown)

	if err := proxysub.Revoke(db, "lab", "alice", time.Now()); err != nil {
		t.Fatal(err)
	}
	revoked := get(t, db, "/sub/"+s.Token)
	if revoked.StatusCode != http.StatusNotFound {
		t.Fatalf("revoked token: status=%d want 404", revoked.StatusCode)
	}
	if got := readBody(t, revoked); got != unknownBody {
		t.Errorf("revoked 404 body differs from unknown 404 body (oracle!):\n revoked=%q\n unknown=%q", got, unknownBody)
	}

	// A DELETING session also 404s identically.
	s2, _ := proxysub.Create(db, "lab", "bob", "SHA256:o", time.Now())
	if _, err := db.Exec(`UPDATE sessions SET state='DELETING' WHERE sid='lab'`); err != nil {
		t.Fatal(err)
	}
	del := get(t, db, "/sub/"+s2.Token)
	if del.StatusCode != http.StatusNotFound {
		t.Fatalf("DELETING session token: status=%d want 404", del.StatusCode)
	}
}

// Method + path hostility: non-GET → 405; nested path / oversized token → 404
// (and never a DB query that could panic).
func TestSubHostileInputs(t *testing.T) {
	db := newDB(t)
	h := Handler(Config{DB: db, PublicHost: "broker.example.com"})

	post := httptest.NewRequest(http.MethodPost, "/sub/whatever", nil)
	wp := httptest.NewRecorder()
	h.ServeHTTP(wp, post)
	if wp.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST: got %d want 405", wp.Code)
	}

	for _, p := range []string{
		"/sub/",                          // empty token
		"/sub/a/b",                       // nested
		"/sub/" + strings.Repeat("x", 9), // unknown short
		"/sub/" + strings.Repeat("x", 4096),
	} {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusNotFound {
			t.Errorf("GET %q: got %d want 404", p, w.Code)
		}
	}
}

// An empty live-node set must still produce a valid (importable) Clash doc.
func TestSubEmptyNodeSetValid(t *testing.T) {
	db := newDB(t)
	s, _ := proxysub.Create(db, "lab", "alice", "SHA256:o", time.Now())
	resp := get(t, db, "/sub/"+s.Token)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, "proxy-groups") || !strings.Contains(body, "rules") {
		t.Errorf("empty render not a valid clash doc:\n%s", body)
	}
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}
