package upgradecut

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"golang.org/x/sys/unix"
)

// ResourceID names the one handoff resource carrying the required adjunct. Its
// Kind is Version, its Data the canonical adjunct bytes and its files the
// auxiliary custody entries named by index. Exporters (the S8 runtime ports)
// and importers (agentboot, brokerimage) share this convention, so neither can
// smuggle the cut through a domain's closed private codec or a second resource.
// origin: hot-update S8 plan §5.1 / §6.1 / §11.2 P17
const ResourceID = "upgrade-cut"

// AuxiliaryName binds an adjunct's FD index to its handoff file name.
func AuxiliaryName(index uint32) string { return "aux-" + strconv.FormatUint(uint64(index), 10) }

// Pack builds the exporter's resource. The lease file is borrowed: Prepare
// duplicates it, so the exporting reservation stays held until exec succeeds.
// The lock is the only auxiliary FD this version defines; any other entry is a
// descriptor the importer has no owner for, so it is refused here.
func Pack(state State, auxiliary map[uint32]*os.File) (handoff.Resource, error) {
	data, err := Encode(state)
	if err != nil {
		return handoff.Resource{}, err
	}
	if len(auxiliary) != 1 || auxiliary[state.LockFD] == nil {
		return handoff.Resource{}, invalidAdjunct()
	}
	files := map[string]*os.File{AuxiliaryName(state.LockFD): auxiliary[state.LockFD]}
	return handoff.Resource{ID: ResourceID, Kind: Version, Data: data, Files: files}, nil
}

// Find validates the adjunct among an importer's borrowed resources before any
// file is taken. Absence is a legacy (non-S8) dispatch; a second adjunct, an ID
// and Kind that disagree, or a file set that is not exactly the lock reference
// refuses the whole import rather than letting a reader skip the required cut.
// origin: hot-update S8 plan §6.1 / §11.2 P17
func Find(resources []handoff.Resource) (State, bool, error) {
	var found *handoff.Resource
	for index := range resources {
		resource := &resources[index]
		if resource.ID != ResourceID && resource.Kind != Version {
			continue
		}
		if found != nil || resource.ID != ResourceID || resource.Kind != Version {
			return State{}, true, invalidAdjunct()
		}
		found = resource
	}
	if found == nil {
		return State{}, false, nil
	}
	state, err := Decode(found.Data)
	if err != nil {
		return State{}, true, err
	}
	if len(found.Files) != 1 || found.Files[AuxiliaryName(state.LockFD)] == nil {
		return State{}, true, invalidAdjunct()
	}
	return state, true, nil
}

// TakeLease moves the inherited lease out of the handoff source through Take.
// The taken file's raw descriptor is duplicated and its os.File wrapper closed,
// so no finalizer can later close the adopted custody; the duplicate refers to
// the same OFD and therefore the same kernel reservation. If the duplicate
// cannot be made, the taken file itself is retained as refused custody for
// salvage: closing it here could be the early unlock M08 forbids.
// origin: hot-update S8 plan §5.1 / M08 / §11.2 P17
func TakeLease(state State, take func(id, name string) (*os.File, error)) (upgradelock.Lease, error) {
	if take == nil {
		return nil, invalidAdjunct()
	}
	var held *os.File
	lease, err := Take(state, func(index uint32) (uintptr, error) {
		file, takeErr := take(ResourceID, AuxiliaryName(index))
		if file == nil {
			return ^uintptr(0), errors.Join(takeErr, errors.New("auxiliary lease unavailable"))
		}
		raw, rawErr := file.SyscallConn()
		duplicate, dupErr := -1, rawErr
		if rawErr == nil {
			if controlErr := raw.Control(func(fd uintptr) { duplicate, dupErr = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3) }); controlErr != nil {
				dupErr = controlErr
			}
		}
		if dupErr != nil || duplicate < 3 {
			held = file
			return ^uintptr(0), errors.New("auxiliary lease duplicate unavailable")
		}
		return uintptr(duplicate), errors.Join(takeErr, file.Close())
	})
	if held != nil {
		if lease != nil {
			_ = lease.Close()
		}
		return &takenFD{file: held, domainID: state.Attempt.Key.DomainID}, custodyError()
	}
	return lease, err
}

// Custody is an importer's hold on the verified adjunct and its lease from
// admission until activation commits or terminal cleanup. Both importers
// (agentboot, brokerimage) use it, so the Take-once and repack rules cannot
// drift between roles. A nil Custody is a legacy dispatch: every method is a
// no-op. A refused Take keeps the taken FD as custody and stays final.
// origin: hot-update S8 plan §5.1 / §6.1 / M08 / §11.2 P17
type Custody struct {
	mu    sync.Mutex
	state State
	lease upgradelock.Lease
	err   error
	taken bool // the FD left the source; a partial-activation retry after Release must not take again
}

// Admit verifies the adjunct among the importer's borrowed resources before
// any file is taken: the closed resource shape, the importer's component, the
// same-PID origin, then expect, the caller's own facts (running image SHA,
// configured Key, accepted journal cut). (nil, nil) is a legacy dispatch.
// expect receives a copy and must not call back into the importer.
func Admit(resources []handoff.Resource, component upgradetx.Component, expect func(State) error) (*Custody, error) {
	state, present, err := Find(resources)
	if err != nil || !present {
		return nil, err
	}
	if state.Attempt.Key.Component != component || expect == nil {
		return nil, invalidAdjunct()
	}
	if err := SameProcess(state.Owner); err != nil {
		return nil, err
	}
	custody := &Custody{state: state}
	if err := expect(*custody.State()); err != nil {
		return nil, err
	}
	return custody, nil
}

// State returns a copy sharing no slice with the custody. The state was
// decoded canonically, so its re-encoding cannot fail.
func (c *Custody) State() *State {
	if c == nil {
		return nil
	}
	data, _ := Encode(c.state)
	state, _ := Decode(data)
	return &state
}

// Take adopts the inherited lease exactly once, through TakeLease.
func (c *Custody) Take(take func(id, name string) (*os.File, error)) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.taken {
		c.taken = true
		c.lease, c.err = TakeLease(c.state, take)
	}
	return c.err
}

// Release transfers an adopted lease to the commit owner once the importer
// holds ACTIVE responsibility. Before that the importer keeps it for pristine
// salvage, and refused custody is never released as if it were a reservation.
func (c *Custody) Release(active bool) upgradelock.Lease {
	if c == nil || !active {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil
	}
	lease := c.lease
	c.lease = nil
	return lease
}

// Resources re-packs a lease that already left the handoff source, for a
// pristine redispatch. Prepare receives a duplicate (the caller closes it
// after Prepare duplicated it again), never the lease's own descriptor; the
// reservation is therefore never unlocked by a redispatch.
func (c *Custody) Resources() ([]handoff.Resource, error) {
	if c == nil {
		return nil, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lease == nil {
		return nil, nil
	}
	duplicate, err := unix.FcntlInt(c.lease.FD(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		return nil, custodyError()
	}
	file := os.NewFile(uintptr(duplicate), "upgrade-lease-redispatch")
	resource, err := Pack(c.state, map[uint32]*os.File{c.state.LockFD: file})
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return []handoff.Resource{resource}, nil
}

// Close is terminal cleanup by the lease's final owner. It never issues
// LOCK_UN; the kernel releases the reservation at the last holder.
func (c *Custody) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lease == nil {
		return nil
	}
	err := c.lease.Close()
	c.lease = nil
	return err
}

// Files is the number of handoff files an admitted adjunct occupies outside
// the importer's own closed resource graph: Find admits exactly its lock.
func Files(c *Custody) int {
	if c == nil {
		return 0
	}
	return 1
}

// ExpectImage is the importer check when the caller's only facts are the
// exporting and running image SHAs (the broker bundle seals both). A
// refusal never echoes either value.
func ExpectImage(sourceSHA, runningSHA string) func(State) error {
	return func(state State) error {
		if sourceSHA == "" || runningSHA == "" || state.SourceSHA != sourceSHA || state.TargetSHA != runningSHA {
			return invalidAdjunct()
		}
		return nil
	}
}

// CloseResources closes the duplicates a Custody handed to a repack after
// Prepare has made its own; the custody itself is untouched.
func CloseResources(resources []handoff.Resource) {
	for _, resource := range resources {
		for _, file := range resource.Files {
			_ = file.Close()
		}
	}
}

// MatchAuthorities compares the adjunct's complete authority map with the
// current authority rows read by the real store after OpenCurrent. It is an
// exact set comparison: a missing, extra or recreated session is a mismatch,
// never a partial success. Callers revoke and clean up on mismatch; they do not
// bind the recreated session first.
// origin: hot-update S8 plan §6.1 / §6.2 / §11.2 P17
func MatchAuthorities(expected, actual []Authority) error {
	mismatch := &upgradetx.Error{Code: upgradetx.CodeSessionIncarnationMismatch, Detail: "inherited authority differs from the current store"}
	if len(expected) != len(actual) {
		return mismatch
	}
	current := make(map[string]string, len(actual))
	for _, authority := range actual {
		if _, duplicate := current[authority.SID]; duplicate || !hexWidth(authority.Incarnation, 64) {
			return mismatch
		}
		current[authority.SID] = authority.Incarnation
	}
	for _, authority := range expected {
		if incarnation, ok := current[authority.SID]; !ok || incarnation != authority.Incarnation {
			return mismatch
		}
	}
	return nil
}

// SameProcess proves the cut was produced by this very process before its
// same-PID exec: PID, kernel start tick and boot all survive exec unchanged,
// while any other process (or this PID after reuse) differs. A non-Linux host
// has no such witness and refuses, matching the adoption contract.
// origin: hot-update S8 plan §2.1 / §5.1 / §11.2 P17
func SameProcess(owner upgradetx.Owner) error {
	refused := &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: "upgrade cut belongs to another process"}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.ReplaceAll(strings.TrimSpace(string(boot)), "-", "") != strings.ReplaceAll(owner.BootID, "-", "") {
		return refused
	}
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil || owner.PID != os.Getpid() {
		return refused
	}
	line := string(stat)
	fields := strings.Fields(line[strings.LastIndexByte(line, ')')+1:])
	// Field 22 (starttime) is the 20th after the parenthesised comm.
	if len(fields) < 20 || fields[19] != strconv.FormatUint(owner.StartTicks, 10) {
		return refused
	}
	return nil
}
