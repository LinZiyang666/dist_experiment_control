package agentupgradeport

import (
	"sync"

	"github.com/LinZiyang666/tether/internal/agentimage"
)

// origin: hot-update S8 plan §6.1 / §10; S8 internal review FREEZE-WINDOW R6
// Linux forbids writes to the executing inode (ETXTBSY), and exec replaces
// this cache along with the address space. Rehashing it inside the final
// gate adds no evidence. Explicit test paths and non-Linux paths stay fresh.
type imageDigestCache struct {
	sync.Mutex
	sha string
}

var executingImageSHA imageDigestCache

func cachedImageDigest(cache *imageDigestCache, read func() (string, error)) (string, error) {
	cache.Lock()
	defer cache.Unlock()
	if cache.sha != "" {
		return cache.sha, nil
	}
	sha, err := read()
	if err == nil {
		cache.sha = sha
	}
	return sha, err
}

// ProcessImageDigest hashes this Linux process once per address space.
// Failed reads never become reusable evidence.
func ProcessImageDigest() (string, error) {
	return cachedImageDigest(&executingImageSHA, func() (string, error) { return agentimage.SHA256OfFile("/proc/self/exe") })
}
