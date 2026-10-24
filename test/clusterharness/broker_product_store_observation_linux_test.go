package clusterharness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// A moved constructor must be instrumented at its actual source before the
// product is built. A stale replacement silently drops the pre-replay oracle.
// origin: hot-update S8 plan §12.1; S8 internal review upgrade-maketest F10
func TestProductStoreObserverFollowsTheCurrentConstructor(t *testing.T) {
	_, source, _, _ := runtime.Caller(0)
	repo := filepath.Dir(filepath.Dir(filepath.Dir(source)))
	manifest := productObservationOverlay(t, repo, t.TempDir(), 12345)
	b, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var overlay struct{ Replace map[string]string }
	if err := json.Unmarshal(b, &overlay); err != nil {
		t.Fatal(err)
	}
	path := overlay.Replace[filepath.Join(repo, "internal/brokerupgrade/brokerruntime/cut.go")]
	if path == "" {
		t.Fatal("current-store constructor was omitted from the private artifact overlay")
	}
	b, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "ImageOpenCurrent: func(cfg storage.OpenImageConfig)") || strings.Count(string(b), "func s6ProductBeforeReplay(db *sql.DB)") != 1 {
		t.Fatal("private artifact did not observe the actual current store before replay")
	}
	// origin: S6 lead decision D-C / plan §3.2 / T01
	if !strings.Contains(string(b), "err:=n.Shutdown();if err==nil {s6ProductNodeClosed()};return err") || strings.Count(string(b), "func s6ProductNodeClosed()") != 1 {
		t.Fatal("private artifact did not observe the actual Node shutdown before exec")
	}
}
