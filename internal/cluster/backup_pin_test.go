package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// TestBackupAPIPin pins the modernc.org/sqlite online-backup API D1 depends on
// (architecture §3.8 D1 amendment): the Raw driver conn implements NewBackup /
// NewRestore, Step's polarity is true=MORE / false=DONE, and Finish closes the
// destination cleanly. A future modernc rename turns THIS red (a clear typed
// failure) instead of a panic deep in Persist.
func TestBackupAPIPin(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	src, err := storage.OpenWAL("file:" + srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	if _, err := src.Exec(`INSERT INTO cluster_meta(key,value) VALUES('t:pin','1')`); err != nil {
		t.Fatal(err)
	}

	ro, err := storage.OpenReadOnly("file:" + srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()

	dstPath := filepath.Join(dir, "dst.db")
	conn, err := ro.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	sawDone := false
	err = conn.Raw(func(dc any) error {
		bc, ok := dc.(backupConn)
		if !ok {
			t.Fatal("Raw driver conn must implement NewBackup(string) (modernc API pin)")
		}
		if _, ok := dc.(restoreConn); !ok {
			t.Fatal("Raw driver conn must implement NewRestore(string) (modernc API pin)")
		}
		b, err := bc.NewBackup(dstPath)
		if err != nil {
			return err
		}
		// Copy one page at a time; the loop terminates ONLY because Step returns
		// false=DONE (if polarity were inverted this would copy nothing or spin).
		for {
			more, err := b.Step(1)
			if err != nil {
				_ = b.Finish()
				return err
			}
			if !more {
				sawDone = true
				break
			}
		}
		return b.Finish()
	})
	if err != nil {
		t.Fatalf("backup round-trip: %v", err)
	}
	if !sawDone {
		t.Fatal("Step never returned false=DONE — polarity pin failed")
	}
	// The destination is a valid, integrity-clean DB carrying the row.
	if err := verifyIntegrity(dstPath); err != nil {
		t.Fatalf("backup destination not integrity-clean: %v", err)
	}
}
