package session

import (
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
)

func TestProxyEnabledSwitch(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	now := time.Now()
	if _, err := Create(db, "lab", "lab", "SHA256:owner", "pinhash", now); err != nil {
		t.Fatal(err)
	}

	on, err := GetProxyEnabled(db, "lab")
	if err != nil || on {
		t.Fatalf("default must be off: on=%v err=%v", on, err)
	}
	changed, err := SetProxyEnabled(db, "lab", true)
	if err != nil || !changed {
		t.Fatalf("enable: changed=%v err=%v", changed, err)
	}
	changed, err = SetProxyEnabled(db, "lab", true) // idempotent
	if err != nil || changed {
		t.Fatalf("re-enable should be no-change: changed=%v err=%v", changed, err)
	}
	on, _ = GetProxyEnabled(db, "lab")
	if !on {
		t.Fatal("want enabled")
	}
	if _, err := SetProxyEnabled(db, "ghost", true); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing session: want ErrNotFound, got %v", err)
	}

	// A DELETING (tombstoned) session rejects the switch.
	if err := Tombstone(db, "lab", now); err != nil {
		t.Fatal(err)
	}
	if _, err := SetProxyEnabled(db, "lab", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DELETING session: want ErrNotFound, got %v", err)
	}
}
