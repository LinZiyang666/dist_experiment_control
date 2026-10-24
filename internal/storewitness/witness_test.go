//go:build linux

// Real sealed memfds, SQLite RO transactions and independent flock OFDs are
// exercised here. No product service is stubbed; product replay is tested at
// the actual OpenCurrent boundary by the hotupdate integration test.
package storewitness_test

import (
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storewitness"
	"github.com/LinZiyang666/tether/internal/testharness"
	_ "modernc.org/sqlite"
)

type witnessSource struct{ file *os.File }

func (s *witnessSource) TakeFile(_, _ string) (*os.File, error) {
	f := s.file
	s.file = nil
	return f, nil
}

// origin: hot-update S10 plan §5.3 / P08
func TestStoreWitnessRejectsInvalidCustody(t *testing.T) {
	exerciseStoreWitnessCustody(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseStoreWitnessCustody(t)
	}
	testharness.AssertNoGoroutineLeak(t, "store witness RO/custody", beforeG)
	testharness.AssertNoFDLeak(t, "store witness RO/custody", beforeFD, 0)
}

func exerciseStoreWitnessCustody(t *testing.T) {
	t.Helper()
	r := storewitness.Registration{Device: 1, Inode: 2, Nonce: [12]byte{1}, ScopeDigest: [32]byte{2}, Keys: []string{"ack", "absent", "null"}}
	b, err := storewitness.EncodeRegistration(r)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(storewitness.Registration) storewitness.Registration{
		"empty keys":    func(r storewitness.Registration) storewitness.Registration { r.Keys = nil; return r },
		"duplicate key": func(r storewitness.Registration) storewitness.Registration { r.Keys = []string{"ack", "ack"}; return r },
		"oversize key": func(r storewitness.Registration) storewitness.Registration {
			r.Keys = []string{strings.Repeat("x", 257)}
			return r
		},
		"NUL key":       func(r storewitness.Registration) storewitness.Registration { r.Keys = []string{"ack\x00"}; return r },
		"zero nonce":    func(r storewitness.Registration) storewitness.Registration { r.Nonce = [12]byte{}; return r },
		"zero scope":    func(r storewitness.Registration) storewitness.Registration { r.ScopeDigest = [32]byte{}; return r },
		"missing inode": func(r storewitness.Registration) storewitness.Registration { r.Inode = 0; return r },
	} {
		if _, err := storewitness.EncodeRegistration(mutate(r)); !errors.Is(err, storewitness.ErrWitness) {
			t.Fatalf("%s was accepted", name)
		}
	}
	for _, malformed := range [][]byte{nil, b[:65], append(append([]byte(nil), b...), 0), append([]byte("BAD1"), b[4:]...)} {
		snapshot, err := sealedstate.New("store-witness", malformed)
		if err != nil {
			t.Fatal(err)
		}
		w, err := storewitness.Register(&witnessSource{snapshot.Owned[0]}, "store-witness", "state")
		if w != nil || !errors.Is(err, storewitness.ErrWitness) {
			t.Fatal("malformed sealed registration was accepted")
		}
		// storewitness.Register owns the taken file, including rejection paths.
		if _, err := snapshot.Owned[0].Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("registration left its descriptor open")
		}
	}
	ordinary, err := os.CreateTemp(t.TempDir(), "unsealed")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ordinary.Write(b); err != nil {
		t.Fatal(err)
	}
	if w, err := storewitness.Register(&witnessSource{ordinary}, "store-witness", "state"); w != nil || !errors.Is(err, storewitness.ErrWitness) {
		t.Fatal("ordinary file became sealed configuration")
	}
	if _, err := ordinary.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("rejected ordinary file was not closed")
	}
	snapshot, err := sealedstate.New("store-witness", b)
	if err != nil {
		t.Fatal(err)
	}
	w, err := storewitness.Register(&witnessSource{snapshot.Owned[0]}, "store-witness", "state")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	second, err := sealedstate.New("store-witness", b)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := storewitness.Register(&witnessSource{second.Owned[0]}, "store-witness", "state"); other != nil || !errors.Is(err, storewitness.ErrWitness) {
		t.Fatal("active witness registration was replaced")
	}
	path := filepath.Join(t.TempDir(), "ack.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE session_creators(fp TEXT PRIMARY KEY,note TEXT); INSERT INTO session_creators VALUES('ack','business-nonce'),('null',NULL)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ro.Close(); err != nil {
			t.Error(err)
		}
	}()
	tx, err := ro.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Error(err)
		}
	}()
	stamp := storewitness.Stamp{Device: 1, Inode: 2, SchemaDigest: [32]byte{3}, Journal: "delete", Synchronous: 2, Readers: 1}
	var events []drillobserve.Event
	old := drillobserve.Hook
	drillobserve.Hook = func(e drillobserve.Event) { events = append(events, e) }
	defer func() { drillobserve.Hook = old }()
	storewitness.Capture(tx, stamp)
	if len(events) != 1 {
		t.Fatal("missing RO receipt")
	}
	receipt, err := storewitness.Decode(events[0])
	if err != nil {
		t.Fatal(err)
	}
	expected := []storewitness.Row{{Key: "ack", Present: true, Digest: sha256.Sum256([]byte("business-nonce"))}, {Key: "absent"}, {Key: "null", Present: true, Null: true}}
	if err := storewitness.Verify(receipt, r, expected); err != nil {
		t.Fatal("RO rows, NULL or affirmative absence mismatched the independent ledger")
	}
	if _, err := tx.Exec("DELETE FROM session_creators"); err == nil {
		t.Fatal("witness transaction was writable")
	}
	for name, mutate := range map[string]func(*drillobserve.Event){
		"wrong kind":       func(e *drillobserve.Event) { e.Kind = "store-after-replay" },
		"wrong domain":     func(e *drillobserve.Event) { e.Domain = "flow" },
		"unknown version":  func(e *drillobserve.Event) { e.Values[9]++ },
		"SQL failure":      func(e *drillobserve.Event) { e.Values[10] = 1 },
		"reserved slot":    func(e *drillobserve.Event) { e.Values[15] = 1 },
		"writer present":   func(e *drillobserve.Event) { e.Values[6] = 1 },
		"missing RO":       func(e *drillobserve.Event) { e.Values[7] = 0 },
		"missing schema":   func(e *drillobserve.Event) { e.Digest = [32]byte{} },
		"unknown journal":  func(e *drillobserve.Event) { e.Values[5] = 3 },
		"weak synchronous": func(e *drillobserve.Event) { e.Values[4] = 1 },
		"partial cursor":   func(e *drillobserve.Event) { e.Values[2] = 1 },
		"truncated data":   func(e *drillobserve.Event) { e.Data = e.Data[:len(e.Data)-1] },
		"trailing data":    func(e *drillobserve.Event) { e.Data = append(e.Data, 0) },
	} {
		e := events[0]
		e.Data = append([]byte(nil), e.Data...)
		mutate(&e)
		if _, err := storewitness.Decode(e); !errors.Is(err, storewitness.ErrWitness) {
			t.Fatalf("%s receipt was accepted", name)
		}
	}
	stale := receipt
	stale.Nonce[0]++
	if err := storewitness.Verify(stale, r, expected); !errors.Is(err, storewitness.ErrWitness) {
		t.Fatal("stale nonce was accepted")
	}
	stale = receipt
	stale.ScopeDigest[0]++
	if err := storewitness.Verify(stale, r, expected); !errors.Is(err, storewitness.ErrWitness) {
		t.Fatal("wrong cut scope was accepted")
	}
	storewitness.Capture(nil, stamp)
	if len(events) != 2 {
		t.Fatal("failed query did not emit invalid evidence")
	}
	if _, err := storewitness.Decode(events[1]); !errors.Is(err, storewitness.ErrWitness) {
		t.Fatal("failed reader fabricated absence")
	}
}

// origin: hot-update S10 plan §5.4 / P08
func TestStoreWitnessContenderRecordsTransientUnlock(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseStoreWitnessContender(t)
	}
	testharness.AssertNoGoroutineLeak(t, "store lock contender", beforeG)
	testharness.AssertNoFDLeak(t, "store lock contender", beforeFD, 0)
}

func exerciseStoreWitnessContender(t *testing.T) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tether.lock")
	if c, err := storewitness.StartContender(path); c != nil || !errors.Is(err, storewitness.ErrLock) {
		t.Fatal("contender created a missing product lock")
	}
	owner, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := syscall.Flock(int(owner.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	c, err := storewitness.StartContender(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = c.Join() }()
	if err := c.Probe(); err != nil {
		t.Fatal("independent OFD did not observe owner contention")
	}
	if err := syscall.Flock(int(owner.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := c.Probe(); !errors.Is(err, storewitness.ErrLock) {
		t.Fatal("transient unlock was missed")
	}
	// Immediate owner reacquisition proves the contender released its hit.
	if err := syscall.Flock(int(owner.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal("observer retained a successful lock")
	}
	if err := c.Probe(); !errors.Is(err, storewitness.ErrLock) {
		t.Fatal("relock erased the earlier acquisition")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Join(); !errors.Is(err, storewitness.ErrLock) {
		t.Fatal("Join erased the acquisition failure")
	}
	r, err := c.Result()
	if !errors.Is(err, storewitness.ErrLock) || r.Attempts < 3 || r.Acquired == 0 || r.FirstAcquiredAttempt == 0 {
		t.Fatal("missing retained acquisition receipt")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Join(); !errors.Is(err, storewitness.ErrLock) {
		t.Fatal("second Join lost failure")
	}
}
