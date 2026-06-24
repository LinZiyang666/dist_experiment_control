package broker

import (
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proxysub"
	"github.com/LinZiyang666/tether/internal/session"
)

// B4: session rm must cascade-delete proxy_subscribers (via the real
// dropSessionRows SQL atom), so a rebuilt same-sid session never resolves an
// old subscription token.
func TestSessionRmCascadesProxySubscribers(t *testing.T) {
	db := openDB(t)
	if _, err := session.Create(db, "lab", "lab", "SHA256:o", "ph", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(sid,nid,last_heartbeat_at,status) VALUES('lab','lab-1',?, 'ONLINE')`, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := port.AllocateProxy(db, "lab", "lab-1", nil); err != nil {
		t.Fatal(err)
	}
	s, err := proxysub.Create(db, "lab", "alice", "SHA256:o", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h := proxysub.HashToken(s.Token)

	if err := session.Tombstone(db, "lab", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := dropSessionRows(db, "lab"); err != nil {
		t.Fatalf("dropSessionRows: %v", err)
	}

	var subs, ports int
	_ = db.QueryRow(`SELECT COUNT(*) FROM proxy_subscribers WHERE sid='lab'`).Scan(&subs)
	_ = db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE sid='lab'`).Scan(&ports)
	if subs != 0 || ports != 0 {
		t.Fatalf("cascade leak: proxy_subscribers=%d port_allocations=%d", subs, ports)
	}
	if _, err := proxysub.LookupSIDByTokenHash(db, h); !errors.Is(err, proxysub.ErrNotFound) {
		t.Fatalf("old token still resolves after session rm: %v", err)
	}

	// Rebuild a same-sid session + a NEW subscriber: the old token must not resolve.
	if _, err := session.Create(db, "lab", "lab", "SHA256:o", "ph", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := proxysub.Create(db, "lab", "bob", "SHA256:o", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := proxysub.LookupSIDByTokenHash(db, h); !errors.Is(err, proxysub.ErrNotFound) {
		t.Fatal("old token resolved against a rebuilt same-sid session")
	}
}

func TestSessionHardDeleteRequiresDeletingState(t *testing.T) {
	db := openDB(t)
	if _, err := session.Create(db, "lab", "lab", "SHA256:o", "ph", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := dropSessionRows(db, "lab"); !errors.Is(err, session.ErrDeleting) {
		t.Fatalf("active session hard delete err=%v, want ErrDeleting", err)
	}
	var state string
	if err := db.QueryRow(`SELECT state FROM sessions WHERE sid='lab'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != string(session.StateActive) {
		t.Fatalf("active session was modified by stale hard delete, state=%q", state)
	}
}
