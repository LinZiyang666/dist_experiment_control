//go:build linux

package brokerimage

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// A real single-mode store keeps its DELETE journal (plan §3.2 step 5). The
// actual close receipt from storage.CheckpointForImage is (-1,-1,-1); every
// validator on the cut path must accept that exact receipt, otherwise a cut
// reaches Export only AFTER the sole writer closed and then blocks forever.
// origin: hot-update S6 internal review rv-writer
func TestSingleDeleteJournalSealAcceptedByCut(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tether.db")
	db, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	identity, err := storage.CanonicalStoreIdentity(path, "")
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := storage.ReadStoreStamp(db, identity)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if stamp.Journal != "delete" {
		_ = db.Close()
		t.Fatalf("single store journal = %q, want delete", stamp.Journal)
	}
	seal, err := closeColdSingleStore(db, stamp, storage.DefaultDriverRegistry(), futureCutDeadline(t))
	if err != nil {
		t.Fatalf("real close failed: %v", err)
	}
	if !StoreSealClosed(seal) {
		t.Fatalf("StoreSealClosed rejected a real DELETE close receipt: %+v", seal)
	}
	if err := validateSeal(seal); err != nil {
		t.Fatalf("codec rejected a real DELETE close receipt: %v", err)
	}
	if err := validateCutSeal(ModeSingle, stamp, seal); err != nil {
		t.Fatalf("cut rejected a real DELETE close receipt (busy=%d log=%d done=%d): %v",
			seal.CheckpointBusy, seal.CheckpointLog, seal.CheckpointDone, err)
	}
}

// Drive the same receipt through Freeze→Export→Thaw. The store owner reports
// a genuine closed seal; the cut must either export it or reopen the current
// store. Refusing both leaves the only writer closed and the broker fenced.
// origin: hot-update S6 internal review rv-writer
func TestSingleDeleteJournalCutNotStrandedAfterClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tether.db")
	db, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	identity, err := storage.CanonicalStoreIdentity(path, "")
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := storage.ReadStoreStamp(db, identity)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	seal, err := closeColdSingleStore(db, stamp, storage.DefaultDriverRegistry(), futureCutDeadline(t))
	if err != nil || !StoreSealClosed(seal) {
		t.Fatalf("real close: %v %+v", err, seal)
	}
	f := newCutFixture(t)
	f.store.stamp, f.store.seal = cloneStamp(stamp), cloneSeal(seal)
	cut := freezeFixture(t, f)
	if _, err := f.store.CloseForImage(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
	_, _, exportErr := cut.Export(seal)
	if exportErr == nil {
		return
	}
	thawErr := cut.Thaw(f.cfg.Deadline)
	if errors.Is(thawErr, ErrStoreClose) {
		t.Fatalf("closed DELETE store stranded: Export=%v, Thaw=%v (no reopen path remains)", exportErr, thawErr)
	}
	t.Fatalf("closed DELETE store not exported: %v (thaw=%v)", exportErr, thawErr)
}

// Both real checkpoint journals pass every consumer of the close receipt, and
// every altered triple fails all three. Synthetic WAL-only cuts missed DELETE.
// origin: hot-update S6 plan §3.2 / T11; internal review rv-writer-3
func TestStoreSealValidatorsShareRealJournalReceipts(t *testing.T) {
	for _, journal := range []string{"delete", "wal"} {
		t.Run(journal, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			open := storage.Open
			if journal == "wal" {
				open = storage.OpenWAL
			}
			db, err := open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			identity, err := storage.CanonicalStoreIdentity(path, "")
			if err != nil {
				t.Fatal(err)
			}
			stamp, err := storage.ReadStoreStamp(db, identity)
			if err != nil {
				t.Fatal(err)
			}
			if stamp.Journal != journal {
				t.Fatalf("journal=%q, want %q", stamp.Journal, journal)
			}
			seal, err := closeColdSingleStore(db, stamp, storage.DefaultDriverRegistry(), futureCutDeadline(t))
			if err != nil {
				t.Fatal(err)
			}
			check := func(seal storage.StoreSeal, want bool) {
				t.Helper()
				if StoreSealClosed(seal) != want || (validateSeal(seal) == nil) != want || (validateCutSeal(ModeSingle, stamp, seal) == nil) != want {
					t.Fatalf("receipt consumers disagree (busy=%d log=%d done=%d), want closed=%v", seal.CheckpointBusy, seal.CheckpointLog, seal.CheckpointDone, want)
				}
			}
			check(seal, true)
			for _, field := range []string{"busy", "log", "done", "journal"} {
				bad := cloneSeal(seal)
				switch field {
				case "busy":
					bad.CheckpointBusy++
				case "log":
					bad.CheckpointLog++
				case "done":
					bad.CheckpointDone++
				case "journal":
					bad.Stamp.Journal = "persist"
				}
				check(bad, false)
			}
		})
	}
}
