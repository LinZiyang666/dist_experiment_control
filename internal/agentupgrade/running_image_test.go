//go:build linux

package agentupgrade

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentimage"
)

// origin: S8 internal review FREEZE-WINDOW R6
// A configured test/alternate image is mutable and never borrows the Linux
// executing-inode cache, even after a default-path read populated that cache.
func TestRunningImageDigestKeepsExplicitPathsFresh(t *testing.T) {
	if _, err := RunningImageSHA("", func() (string, error) { t.Fatal("Linux running image followed a pathname"); return "", nil }); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "image")
	for _, body := range []string{"source", "target"} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		want, err := agentimage.SHA256OfFile(path)
		if err != nil {
			t.Fatal(err)
		}
		got, err := RunningImageSHA(path, func() (string, error) { t.Fatal("explicit image resolved through canonical path"); return "", nil })
		if err != nil || got != want {
			t.Fatalf("explicit path reused stale bytes: %s %v", got, err)
		}
	}
}
