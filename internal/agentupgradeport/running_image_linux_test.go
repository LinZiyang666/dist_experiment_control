package agentupgradeport

import (
	"errors"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"testing"
)

// origin: S8 internal review FREEZE-WINDOW R6
// Failures are not evidence. Each new process image starts with its own cache.
func TestRunningImageDigestKeepsOnlySuccessfulProcessEvidence(t *testing.T) {
	var cache imageDigestCache
	reads := 0
	unavailable := errors.New("unavailable image")
	read := func() (string, error) {
		reads++
		if reads == 1 {
			return "unverified", unavailable
		}
		return "verified image", nil
	}
	if _, err := cachedImageDigest(&cache, read); !errors.Is(err, unavailable) {
		t.Fatal(err)
	}
	for gate := 0; gate < 3; gate++ {
		if got, err := cachedImageDigest(&cache, read); err != nil || got != "verified image" {
			t.Fatalf("cached digest: %s %v", got, err)
		}
	}
	if reads != 2 {
		t.Fatalf("running image hashed %d times, want 2 (failed read and first successful read)", reads)
	}
	var next imageDigestCache
	if got, err := cachedImageDigest(&next, func() (string, error) { return "next image", nil }); err != nil || got != "next image" {
		t.Fatalf("new image inherited a digest: %s %v", got, err)
	}
}

// origin: S8 internal review FREEZE-WINDOW R6
func TestRunningImageDigestIsBoundToTheExecutingInode(t *testing.T) {
	want, err := agentimage.SHA256OfFile("/proc/self/exe")
	if err != nil {
		t.Fatal(err)
	}
	got, err := ProcessImageDigest()
	executingImageSHA.Lock()
	cached := executingImageSHA.sha
	executingImageSHA.Unlock()
	if err != nil || got != want || cached != want {
		t.Fatalf("executing inode not retained: got=%s cached=%s want=%s err=%v", got, cached, want, err)
	}
}
