// Package contract defines the stdlib-only S10 evidence boundary.
// Values preserve observations; constructing them cannot establish continuity.
// Disk names and validation belong to schema.json, not Go's default field names.
// origin: hot-update S10 plan §5.1 / P00
package contract

import "encoding/json"

// Digest identifies bytes, not a producer's claim about the executing image.
type Digest [32]byte

// Key includes every runtime identity axis; sharing a host does not merge units.
type Key struct {
	Role, Host, Unit, SID, NID, Domain string
	UID                                uint32
}

// PID reuse and same-PID exec are separate events. Stamp adds the latter's cut
// and generation; neither a PID nor an executable digest alone identifies it.
type Process struct {
	Key                       Key
	BootID, PIDNS, InstanceID string
	PID                       int
	StartTicks                uint64
}
type Stamp struct {
	Process                                         Process
	TxID, CutID, Direction, QueryNonce, ScopeDigest string
	Generation, Revision, Sequence                  uint64
	AtBootNS                                        int64
}

// Supply-chain digests remain separate so staged bytes cannot impersonate the
// executing binary. Mechanism stamps do not certify release compatibility.
type Artifact struct {
	ID, Role, Release, SourceCommit, Toolchain, ImageID       string
	Tree, Dirty, Flags, Archive, Binary, Vendor, CopyManifest Digest
	Epoch                                                     uint64
	ReadEpochs                                                []uint64
	Domains                                                   []string
}

// Mixed stack vectors consist of separately directed component steps. A reverse
// receipt cannot satisfy a later forward step merely because its SHA matches.
type Step struct {
	ID, ParentID, Action, Direction, Cohort, Leg string
	Target                                       Key
	From, To                                     string
	DependsOn                                    []string
	DeadlineNS                                   int64
}

// Required is the sealed population, not a roster recomputed after a failure.
// Tier distinguishes hermetic mechanism checks from actual deployment evidence.
type Profile struct {
	ID, Topology, Regime, Tier, EvidenceRoot string
	CommandBindings                          RawRef
	ProbeRate                                int
	StartNS, EndNS                           int64
	BudgetDigest, WorkloadDigest             Digest
	Required                                 []Key
	Steps                                    []Step
	Artifacts                                []Artifact
}

// RawRef refers to immutable, complete bytes. Validation never opens Path;
// collectors must establish its digest and preserve the original source schema.
type RawRef struct {
	Path   string
	SHA    Digest
	Bytes  int64
	Schema string
}

// Known and Complete are affirmative observations, including for an empty set.
// Missing proof must not become an invented not-applicable or zero-owner result.
type Receipt struct {
	Kind            string
	Stamp           Stamp
	Known, Complete bool
	RunningSHA      Digest
	ResourceIDs     []string
	Evidence        RawRef
}

// Historical contains the full S8 record and metadata; Current contains its
// full observation. Raw retains the original View bytes, including diagnostics.
type View struct {
	Stamp                        Stamp
	Historical, Current          json.RawMessage
	Phase, Outcome, Code, Detail string
	Deferred                     []string
	Control, Resources, Released Receipt
	Raw                          RawRef
}

// Equal FD counts or inode numbers cannot replace the original OFD witness.
// ReleaseAfterNS preserves legitimate retention instead of labeling it a leak.
type Resource struct {
	ID, Kind, Domain, OFD, State string
	Owner                        Stamp
	FD                           int
	Flags                        uint64
	Bytes                        int64
	ReleaseAfterNS               int64
}

// These counters belong to Stamp.Process, never to the collecting test process.
// An incomplete sample stays incomplete even when all observed counts are zero.
type Sample struct {
	Stamp                              Stamp
	ExeSHA                             Digest
	Goroutines, FDs                    int64
	Resources                          []Resource
	Consumers, Temps, Locks, OldImages []string
	Complete                           bool
	Raw                                RawRef
}

// Slots exist before launch; attempts cannot shrink the scheduled denominator.
type Slot struct {
	ID, Class               string
	Target                  Key
	ScheduledNS, DeadlineNS int64
}
type Attempt struct {
	SlotID, CallID, Phase string
	Index                 uint64
	StartedNS, FinishedNS int64
	Sent, Completed       bool
	RC                    int
	Code                  string
	Raw                   RawRef
}

// Full activation and retirement intervals expose owners missed by polling.
type Interval struct {
	Stamp             Stamp
	ResourceID, Event string
	AtNS              int64
	Raw               RawRef
}

// RC is the native drill vocabulary, independent of CLI business exit classes.
type Result struct {
	Verdict           string
	RC                int
	Failures, Missing []string
	Inputs            []RawRef
}

// Failed, censored and missing observations cannot disappear from quantiles.
type Distribution struct {
	Count, Failed, Censored, Missing  uint64
	MinNS, P50NS, P95NS, P99NS, MaxNS int64
}
type Evidence struct {
	Profile   Profile
	Views     []View
	Samples   []Sample
	Slots     []Slot
	Attempts  []Attempt
	Intervals []Interval
	Raw       []RawRef
}
