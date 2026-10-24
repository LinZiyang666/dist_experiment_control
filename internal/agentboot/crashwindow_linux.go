//go:build linux

// crashwindow_linux.go — the W1 crash window's one action: when the
// dispatch cannot be read, close every inheritable descriptor above stderr
// to give the previous image's children their EOF/EPIPE now and keep the
// registry key's memfd out of every child this process will ever spawn.
// Prepared.Exec cleared those CLOEXEC bits; Restore restores them after
// validating the manifest.
//
// W1 assumes startup exclusivity: at main's first
// statement, inherited fds >=3 belong only to the dispatch. Socket activation
// is unsupported. CLOEXEC is selection under that premise, not provenance.
// Restore may protect a manifest before refusing it, so snapshot before it runs.
package agentboot

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

type foreignDescriptor struct {
	fd       int
	dev, ino uint64
}

// foreignDescriptors snapshots inheritable descriptors before Restore changes
// any flags. A bad nonce can be rejected AFTER the manifest gains CLOEXEC;
// scanning only after that refusal would leak the secret-bearing manifest.
// origin: hot-update S3 plan §1.5 W1 / T13.
func foreignDescriptors() ([]foreignDescriptor, error) {
	var fds []foreignDescriptor
	var censusErr error
	probe := func(fd int) {
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if errors.Is(err, unix.EBADF) {
			return
		}
		if err != nil {
			censusErr = err
			return
		}
		if flags&unix.FD_CLOEXEC != 0 {
			return
		}
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			censusErr = err
			return
		}
		fds = append(fds, foreignDescriptor{fd, uint64(st.Dev), st.Ino})
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err == nil {
		for _, entry := range entries {
			fd, err := strconv.Atoi(entry.Name())
			if err == nil && fd >= 3 {
				probe(fd)
			}
		}
		return fds, censusErr
	}
	// EMFILE must not disable cleanup. F_GETFD needs no new descriptor.
	// Include the hard limit too: lowering the soft limit does not close
	// already inherited descriptors above it (including the dispatch itself).
	var limit unix.Rlimit
	if limitErr := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); limitErr != nil {
		return nil, fmt.Errorf("descriptor census and limit lookup failed: %w", errors.Join(err, limitErr))
	}
	upper := max(limit.Cur, limit.Max)
	if upper <= 3 || upper > 1<<31-1 {
		return nil, errors.New("descriptor census failed with unusable descriptor limit")
	}
	for fd := 3; uint64(fd) < upper; fd++ {
		probe(fd)
	}
	return fds, censusErr
}

func closeForeignDescriptors(fds []foreignDescriptor) int {
	closed := 0
	for _, saved := range fds {
		// origin: hot-update S3 plan §1.5 / RB-5. Do not close a different
		// file that reused this number during failed validation. This check
		// still relies on startup exclusivity, not concurrent fd mutation.
		var st unix.Stat_t
		if unix.Fstat(saved.fd, &st) != nil || uint64(st.Dev) != saved.dev || st.Ino != saved.ino {
			continue
		}
		if unix.Close(saved.fd) == nil {
			closed++
		}
	}
	return closed
}
