package spawnsafe

import "syscall"

// realProbe is the production ProbeFn: a single statfs of the mountpoint. On a
// healthy mount it returns true within microseconds; on a wedged hard-mount it
// blocks in D-state until the server returns (possibly forever) — which is
// exactly why the Policy runs it inside a bounded goroutine and abandons it on
// timeout. syscall.Statfs exists on linux and darwin; we only inspect the error,
// never the (platform-specific) Statfs_t fields, so no build tag is needed.
func realProbe(mountpoint string) bool {
	var buf syscall.Statfs_t
	return syscall.Statfs(mountpoint, &buf) == nil
}
