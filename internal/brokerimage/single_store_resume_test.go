//go:build linux

package brokerimage

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// A failed single-store quiesce can already have fenced its adapter. The paired
// resume must restore the same pool before auth is allowed to use it again.
// origin: hot-update S6 plan §4.2; internal review rv-writer-4
func TestSingleStoreRefusedFreezeRestoresWriteAdmission(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("partial-quiesce-%d", round), func(t *testing.T) { exerciseSingleStoreRefusal(t, true) })
		t.Run(fmt.Sprintf("server-refusal-%d", round), func(t *testing.T) { exerciseSingleStoreRefusal(t, false) })
	}
	testharness.AssertNoGoroutineLeak(t, "single refused cut admission", beforeG)
	testharness.AssertNoFDLeak(t, "single refused cut admission", beforeFD, 0)
}

func exerciseSingleStoreRefusal(t *testing.T, partialQuiesce bool) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	identity, err := storage.CanonicalStoreIdentity(path, "")
	if err != nil {
		t.Fatal(err)
	}
	fenced := false
	refusal := errors.New("injected reversible cut refusal")
	support := &SingleStoreSupport{
		ReadStamp: storage.ReadStoreStamp,
		Quiesce: func(pool *sql.DB, _ controlbudget.Deadline) error {
			if pool != db {
				t.Fatal("quiesce used a replacement pool")
			}
			fenced = true
			if partialQuiesce {
				return refusal
			}
			return nil
		},
		Resume: func(pool *sql.DB) error {
			if pool != db || !fenced {
				t.Fatal("resume did not own the original pool fence")
			}
			fenced = false
			return nil
		},
	}
	store := &singleStoreOwner{support: support, db: db, stamp: storage.StoreStamp{Identity: identity}}
	f := newCutFixture(t)
	f.cfg.Store = store
	f.control.authHook = func() error {
		if fenced {
			return errors.New("auth restarted before store admission")
		}
		_, err := db.Exec("INSERT INTO cluster_meta(key, value) VALUES('t:after-refusal', 'ok')")
		return err
	}
	f.cfg.FreezeServer = func(*tunnel.Server, controlbudget.Deadline) (tunnel.ServerFrozen, error) {
		return nil, refusal
	}
	cut, err := Freeze(f.cfg)
	if cut != nil || !errors.Is(err, refusal) || fenced {
		t.Fatalf("single refusal did not restore original admission: cut=%v err=%v fenced=%v", cut != nil, err, fenced)
	}
	store.closeAttempted = true
	if err := resumeSingleStore(store); err == nil {
		t.Fatal("single admission resumed after an unknown close")
	}
}

// A refused resume is still a fenced cut, and retry completes the original
// graph without repeating a close/open or restarting auth ahead of the store.
// origin: hot-update S6 plan §4.2; internal review rv-writer-4
func TestCutStoreResumeFailureKeepsAdmissionFenced(t *testing.T) {
	f := newCutFixture(t)
	refusal := errors.New("legacy_session_busy")
	f.cfg.FreezeServer = func(*tunnel.Server, controlbudget.Deadline) (tunnel.ServerFrozen, error) {
		f.events.fail = "store.resume"
		return nil, refusal
	}
	cut, err := Freeze(f.cfg)
	if cut == nil || !errors.Is(err, refusal) {
		t.Fatalf("failed resume lost its recovery owner: cut=%v err=%v", cut, err)
	}
	for _, event := range f.events.snapshot() {
		if strings.HasSuffix(event, ".auth") || strings.HasSuffix(event, ".business") {
			t.Fatalf("failed resume published control ready: %v", f.events.snapshot())
		}
	}
	f.events.fail = ""
	if err := cut.Thaw(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
	if f.store.closed || f.store.openN != 0 {
		t.Fatal("reversible refusal replaced the original store")
	}
}
