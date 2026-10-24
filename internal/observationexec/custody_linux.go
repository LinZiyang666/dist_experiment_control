//go:build linux

package observationexec

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

var observationCustody struct {
	sync.Mutex
	fd, channel int
	identity    [32]byte
	bound       bool
	poison      *atomic.Bool
}

func Register(fd, channel int, identity [32]byte, poison *atomic.Bool) error {
	observationCustody.Lock()
	defer observationCustody.Unlock()
	if observationCustody.bound {
		return errors.New("handoff: observation custody already registered")
	}
	if poison == nil {
		return errors.New("handoff: missing observation poison owner")
	}
	if err := Validate(fd, channel, identity); err != nil {
		return err
	}
	observationCustody.fd, observationCustody.channel, observationCustody.identity, observationCustody.bound = fd, channel, identity, true
	observationCustody.poison = poison
	return nil
}

// Recheck the immutable authority on every exec: a closed/reused descriptor
// number cannot expand the inheritance whitelist. Registration owns no FD.
// origin: S8 internal review activation-and-route-migration F1
func Validate(fd, channel int, identity [32]byte) error {
	if fd < 3 || channel < 3 || fd == channel {
		return errors.New("handoff: invalid observation custody")
	}
	const seals = unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	var stat unix.Stat_t
	data := make([]byte, 32)
	n, readErr := unix.Pread(fd, data, 0)
	family, familyErr := unix.GetsockoptInt(channel, unix.SOL_SOCKET, unix.SO_DOMAIN)
	typ, typeErr := unix.GetsockoptInt(channel, unix.SOL_SOCKET, unix.SO_TYPE)
	_, peerErr := unix.Getpeername(channel)
	if err != nil || flags&seals != seals || unix.Fstat(fd, &stat) != nil || stat.Size != 32 || readErr != nil || n != 32 ||
		(string(data[:8]) != "TDCFG1\x00\x00" && string(data[:8]) != "TDCFG2\x00\x00") || sha256.Sum256(data) != identity || binary.LittleEndian.Uint64(data[8:16]) != uint64(channel) ||
		familyErr != nil || family != unix.AF_UNIX || typeErr != nil || typ != unix.SOCK_SEQPACKET || peerErr != nil {
		return errors.New("handoff: invalid observation custody")
	}
	return nil
}

func Descriptors() ([]int, error) {
	observationCustody.Lock()
	defer observationCustody.Unlock()
	if !observationCustody.bound {
		return nil, nil
	}
	// Poison is monotonic in the observer and must survive this handover;
	// exec cannot reset a failed evidence producer into a fresh valid one.
	// origin: S8 internal review activation-and-route-migration F1
	if observationCustody.poison.Load() {
		return nil, errors.New("handoff: observation custody poisoned")
	}
	if err := Validate(observationCustody.fd, observationCustody.channel, observationCustody.identity); err != nil {
		return nil, err
	}
	return []int{observationCustody.fd, observationCustody.channel}, nil
}

func Environment(env []string) []string {
	observationCustody.Lock()
	defer observationCustody.Unlock()
	if !observationCustody.bound {
		return env
	}
	out := make([]string, 0, len(env)+2)
	for _, entry := range env {
		if !strings.HasPrefix(entry, "TETHER_TEST_DRILL_OBSERVER=") && !strings.HasPrefix(entry, "TETHER_TEST_DRILL_CUSTODY_FD=") {
			out = append(out, entry)
		}
	}
	if observationCustody.poison.Load() {
		return out
	}
	return append(out, "TETHER_TEST_DRILL_OBSERVER=v1", "TETHER_TEST_DRILL_CUSTODY_FD="+strconv.Itoa(observationCustody.fd))
}
