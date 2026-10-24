package infraupgrade

import (
	"context"
	"encoding/hex"
	"errors"
	"runtime"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// SupportState is a projection of root's immutable slots and durable selector.
// Root remains the only writer/effect owner. Previous is retained even after
// its worker exits; retirement is not permission to discard rollback bytes.
// origin: hot-update S9 plan §6.5 / T45
type SupportState struct {
	Domain         c.ID
	Revision       uint64
	Kind           c.ArtifactKind
	Active         c.Artifact
	Previous       c.Artifact
	Pending        c.Artifact
	Selector       c.Digest
	Journal        c.Digest
	Before         []c.Process
	Retiring       bool
	RollbackClosed bool
	RetainedOwners bool
	Selecting      c.Process
	StageOnly      bool
}

// The new helper worker obtains this census independently. For launcher
// retirement, LaunchTarget and InheritedLease come from root's post-exec
// pidfd/FD census, not the disappearing launcher's pre-exec ACK.
type SupportCensus struct {
	Domain         c.ID
	Generation     uint64
	Digest         c.Digest
	Complete       bool
	Observer       c.Process
	Processes      []c.Process
	LaunchTarget   c.Digest
	InheritedLease bool
}

func validSupportArtifact(a c.Artifact, kind c.ArtifactKind) bool {
	return (kind == c.HelperBinary || kind == c.LauncherBinary) && a.Kind == kind &&
		a.Version != "" && a.GOOS == "linux" && a.GOARCH == "amd64" &&
		a.ArchiveSHA256 != (c.Digest{}) && a.BinarySHA256 != (c.Digest{}) && a.CatalogSHA256 != (c.Digest{})
}

func supportStateValid(s SupportState) bool {
	if s.Domain == (c.ID{}) || s.Revision == 0 || s.Journal == (c.Digest{}) || s.Selector == (c.Digest{}) ||
		!validSupportArtifact(s.Active, s.Kind) || (s.Kind == c.HelperBinary && len(s.Before) == 0) {
		return false
	}
	if s.Previous != (c.Artifact{}) && (!validSupportArtifact(s.Previous, s.Kind) || s.Previous.BinarySHA256 == s.Active.BinarySHA256) {
		return false
	}
	if s.Pending != (c.Artifact{}) && !validSupportArtifact(s.Pending, s.Kind) {
		return false
	}
	seen := make(map[c.Process]bool)
	for _, p := range s.Before {
		if !caddyProcess(p) || seen[p] || p.ExeSHA256 != s.Active.BinarySHA256 {
			return false
		}
		seen[p] = true
	}
	return true
}

// PrepareSupport stages only. Even a busy host may not evict the recorded
// rollback slot with a third distinct artifact. A closed rollback window and
// independently released owners are required before reusing that slot.
// origin: hot-update S9 plan §6.5 / T45 / M22
func PrepareSupport(s SupportState, candidate c.Artifact, busy bool) (SupportState, error) {
	if !supportStateValid(s) || !validSupportArtifact(candidate, s.Kind) {
		return s, c.ErrIncomplete
	}
	// Admission seals the original worker roster. A caller retaining its input
	// slice must not be able to shrink or rewrite the staged retirement set.
	s.Before = append([]c.Process(nil), s.Before...)
	if s.Retiring || s.Pending != (c.Artifact{}) && s.Pending != candidate {
		return s, errors.New("infra_busy")
	}
	if candidate.BinarySHA256 == s.Active.BinarySHA256 {
		if candidate != s.Active {
			return s, errors.New("infra_artifact_invalid")
		}
		return s, nil
	}
	if s.Previous != (c.Artifact{}) && candidate.BinarySHA256 == s.Previous.BinarySHA256 && candidate != s.Previous {
		return s, errors.New("infra_artifact_invalid")
	}
	if s.Previous != (c.Artifact{}) && candidate != s.Previous {
		if busy || !s.RollbackClosed || s.RetainedOwners {
			return s, errors.New("infra_busy")
		}
		s.Previous = c.Artifact{}
	}
	s.Pending = candidate
	s.StageOnly = busy
	return s, nil
}

// AcquireSupport uses the same registered host domain as image and component
// cuts. HostSupport is a root responsibility, never an invented public S8
// component. Root supplies its registered broker key and persists the returned
// stage under its own journal CAS; this function cannot select or start bytes.
// origin: hot-update S9 plan §6.5 / §7.3 / T45
func AcquireSupport(ctx context.Context, key upgradetx.Key, s SupportState, candidate c.Artifact, busy bool, deadline c.Deadline) (SupportState, upgradelock.Lease, error) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" || ctx == nil || key.Component != upgradetx.ComponentBroker || key.DomainID != hexID(s.Domain) {
		return s, nil, c.ErrUnsupported
	}
	if err := serviceBudget(deadline, nil); err != nil {
		return s, nil, err
	}
	next, err := PrepareSupport(s, candidate, busy)
	if err != nil {
		return s, nil, err
	}
	lease, err := upgradelock.Acquire(ctx, key, "")
	if err != nil {
		return s, nil, transactionError(err)
	}
	return next, lease, nil
}

func validSupportCensus(s SupportState, x SupportCensus) bool {
	if x.Domain != s.Domain || !x.Complete || x.Generation == 0 || x.Digest == (c.Digest{}) || !caddyProcess(x.Observer) || len(x.Processes) == 0 {
		return false
	}
	seen := make(map[c.Process]bool)
	for _, p := range x.Processes {
		if !caddyProcess(p) || seen[p] || p.BootID != x.Observer.BootID {
			return false
		}
		seen[p] = true
	}
	return seen[x.Observer]
}

// SupportSelection records a selector CAS after all other old helper workers
// joined. The selecting worker is still alive; it cannot sign its own exit.
// No method waits for that worker or stops the stock process it may have execed.
// origin: hot-update S9 plan §6.5 / T45 / M22
func SupportSelection(s SupportState, caller c.Process, census SupportCensus, selector, journal c.Digest) (SupportState, error) {
	if !supportStateValid(s) || s.StageOnly || s.Retiring || s.Pending == (c.Artifact{}) || !validSupportCensus(s, census) ||
		selector == (c.Digest{}) || selector == s.Selector || journal == (c.Digest{}) || journal == s.Journal || census.Observer != caller {
		return s, c.ErrIncomplete
	}
	found := false
	for _, before := range s.Before {
		if before == caller {
			found = true
		}
		for _, live := range census.Processes {
			if before != caller && natsSameIncarnation(before, live) {
				return s, errors.New("infra_busy")
			}
		}
	}
	if s.Kind == c.HelperBinary && !found {
		return s, c.ErrIncomplete
	}
	// A worker admitted after the initial roster is also an old responsibility.
	// Joining only Before would allow a late old-selector worker past this CAS.
	for _, live := range census.Processes {
		if s.Kind == c.HelperBinary && live != caller && live.ExeSHA256 == s.Active.BinarySHA256 {
			return s, errors.New("infra_busy")
		}
	}
	s.Selector, s.Journal, s.Retiring = selector, journal, true
	s.Selecting = caller
	return s, nil
}

// CompleteSupport is a fresh post-selection check by a new helper worker. A
// selector flip, a zero count or a pre-exec launcher ACK never retires an image.
// A rollback may run the original SHA; before incarnations remain the keys.
// origin: hot-update S9 plan §2.3 / §6.5 / T45 / M22
func CompleteSupport(s SupportState, census SupportCensus, journal c.Digest) (SupportState, error) {
	if !supportStateValid(s) || !s.Retiring || s.Pending == (c.Artifact{}) || !validSupportCensus(s, census) ||
		journal == (c.Digest{}) || journal == s.Journal {
		return s, c.ErrIncomplete
	}
	if s.Kind == c.HelperBinary && census.Observer.ExeSHA256 != s.Pending.BinarySHA256 {
		return s, c.ErrIncomplete
	}
	if !caddyProcess(s.Selecting) || natsSameIncarnation(s.Selecting, census.Observer) {
		return s, c.ErrIncomplete
	}
	for _, live := range census.Processes {
		if s.Kind == c.HelperBinary && live.ExeSHA256 == s.Active.BinarySHA256 {
			return s, c.ErrIncomplete
		}
	}
	for _, before := range s.Before {
		// Observing one's own process cannot prove exit, even after an exec.
		if natsSameIncarnation(before, census.Observer) {
			return s, c.ErrIncomplete
		}
		for _, live := range census.Processes {
			if !natsSameIncarnation(before, live) {
				continue
			}
			if s.Kind != c.LauncherBinary || !census.InheritedLease || census.LaunchTarget == (c.Digest{}) ||
				live.ExeSHA256 != census.LaunchTarget || live.ExeSHA256 == before.ExeSHA256 ||
				live.InvocationID != before.InvocationID || live.Cgroup != before.Cgroup {
				return s, c.ErrIncomplete
			}
		}
	}
	s.Previous, s.Active, s.Pending = s.Active, s.Pending, c.Artifact{}
	s.Journal, s.Retiring, s.RollbackClosed = journal, false, false
	s.Revision++
	if s.Revision == 0 {
		return s, c.ErrIncomplete
	}
	// Before is the next current generation's roster, not the old witness set.
	s.Before = nil
	for _, p := range census.Processes {
		if p.ExeSHA256 == s.Active.BinarySHA256 {
			s.Before = append(s.Before, p)
		}
	}
	return s, nil
}

func hexID(id c.ID) string { return hex.EncodeToString(id[:]) }
