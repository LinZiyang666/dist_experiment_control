package cluster

import (
	"database/sql"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
)

// P03's current opener, stamp reader and driver registry have landed. Capture
// their actual shutdown seal from the seed, including its Raft frontier, rather
// than guessing a stamp or leaving the dependent cut test behind a nil adapter.
// origin: S10 internal review NO-SKIP-AS-PASS F3
func closeCurrentStoreSeed(t *testing.T, seed *Node) (Config, storage.StoreStamp) {
	t.Helper()
	// Ensure an assertion during preparation still joins the real seed owners.
	t.Cleanup(func() { checkImageCleanup(t, "seed.Shutdown", seed.Shutdown) })
	cfg := seed.cfg
	cfg.ImageOpenCurrent, cfg.ImageReadOnly, cfg.ImageRegistry = storage.OpenCurrent, storage.OpenReadOnly, storage.DefaultDriverRegistry()
	cfg.ImageStamp = func(db *sql.DB) (storage.StoreStamp, error) {
		identity, err := storage.CanonicalStoreIdentity(cfg.DBPath, cfg.DataDir)
		if err != nil {
			return storage.StoreStamp{}, err
		}
		return storage.ReadStoreStamp(db, identity)
	}
	seed.cfg = cfg
	if err := PrepareImageShutdown(seed, imageDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := seed.Shutdown(); err != nil {
		t.Fatal(err)
	}
	seal, err := ClosedImageStore(seed)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, seal.Stamp
}
