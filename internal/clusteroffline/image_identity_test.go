// These tests use real temporary databases and the existing flock owner.
// No store or lock implementation is mocked; inherited OFDs, broker Run and
// same-PID exec remain P04/P26 integration.
package clusteroffline

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
)

// origin: hot-update S6 plan §3.1 / T05
// Every offline Acquire must reject an unrelated DB before creating its lock.
// Checking only the eventual error would let a wrong-domain lock or even disk
// surgery happen first and then look like a harmless setup failure.
func TestOfflineStoreLockBindingBeforeMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	operations := []struct {
		name string
		call func(string, string) error
	}{
		{"init", func(path, dir string) error {
			return InitFromExisting(InitFromExistingOptions{
				DBPath: path, DataDir: dir, SelfID: "node-a", Name: "node-a", NodeIdentPub: "identity", RaftAddr: "127.0.0.1:9001", NatsRoute: "127.0.0.1:9002", TunnelAddr: "127.0.0.1:9003", PublicHost: "localhost",
			})
		}},
		{"force_single", func(path, dir string) error {
			_, err := ForceSingle(ForceSingleOptions{DBPath: path, DataDir: dir, SelfID: "node-a", SelfRaftAddr: "127.0.0.1:9001"})
			return err
		}},
		{"resnapshot", func(path, dir string) error {
			return Resnapshot(ResnapshotOptions{DBPath: path, DataDir: dir, SelfID: "node-a", SelfRaftAddr: "127.0.0.1:9001"})
		}},
		{"recover", func(path, dir string) error {
			_, err := Recover(RecoverOptions{DBPath: path, DataDir: dir, DumpPath: filepath.Join(dir, "dump.json")})
			return err
		}},
		{"restore", func(path, dir string) error {
			_, err := RestoreFromBackup(RestoreOptions{DBPath: path, DataDir: dir, BundleDir: filepath.Join(dir, "bundle"), SecretsDir: filepath.Join(dir, "secrets"), ConfirmNodeID: "node-a"})
			return err
		}},
		{"backup", func(path, dir string) error {
			_, err := OfflineBackup(BackupOptions{DBPath: path, DataDir: dir, OutDir: filepath.Join(dir, "bundle")})
			return err
		}},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			wrongDir := t.TempDir()
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			err = operation.call(path, wrongDir)
			if err == nil || !strings.Contains(err.Error(), "lock domain") {
				t.Fatalf("wrong-domain refusal missing: %v", err)
			}
			entries, err := os.ReadDir(wrongDir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("wrong-domain operation created files: %v, %v", entries, err)
			}
			after, err := os.ReadFile(path)
			if err != nil || string(before) != string(after) {
				t.Fatal("wrong-domain operation mutated DB", err)
			}
		})
	}
}

// origin: hot-update S6 plan §3.1 / T05
// The legacy lock already excludes an offline operation on the same canonical
// DB. Hot OFD adoption and the all-window contender remain P04/P26 seams.
func TestOfflineCanonicalStoreUsesHeldLock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.db")
	db, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	release, err := cluster.AcquireDataDirLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	_, err = ForceSingle(ForceSingleOptions{DBPath: "file:" + path, DataDir: dir})
	if !errors.Is(err, cluster.ErrDataDirLockHeld) {
		t.Fatalf("same DB did not contend for held lock: %v", err)
	}
	if _, err := os.Stat(path + ".bak"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("operation crossed lock refusal")
	}
}
