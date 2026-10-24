// Package infracontract defines the neutral S9 infrastructure boundary.
// Only standard-library and controlbudget dependencies belong here.
// origin: hot-update S9 plan §2.1 / T01
package infracontract

import "github.com/LinZiyang666/tether/internal/controlbudget"

// Deadline is local absolute BOOTTIME, never a timestamp to compare across hosts.
// Stop cancels transport work, not the lifetime of any business resource.
type Deadline = controlbudget.Deadline
type Stop <-chan struct{}
type Digest [32]byte
type ID [16]byte
type Component uint8

const (
	NATS        Component = 1
	Caddy       Component = 2
	HostSupport Component = 3
)

type Phase uint8

const (
	Prepared  Phase = 1
	Staged    Phase = 2
	Ready     Phase = 3
	Moving    Phase = 4
	Cutting   Phase = 5
	Verifying Phase = 6
	Returning Phase = 7
	Retiring  Phase = 8
	Completed Phase = 9
	Deferred  Phase = 10
	Halted    Phase = 11
)

type Action uint8

const (
	Upgrade  Action = 1
	Rollback Action = 2
	Reload   Action = 3
	Recover  Action = 4
)

// Process identifies an incarnation. Unknown identity never means exited;
// rollback may reuse a SHA, and launcher exec may retain PID and start ticks.
// origin: hot-update S9 plan §2.3 / T01
type Process struct {
	BootID       string
	PID          int32
	StartTicks   uint64
	InvocationID string
	ExeDevice    uint64
	ExeInode     uint64
	ExeSHA256    Digest
	Cgroup       string
}

// The raw socket and its accepted ingress owner must both be observed; changing
// a URL, certificate or server-pool label cannot establish a physical move.
type Socket struct {
	NetNS  uint64
	Device uint64
	Inode  uint64
	Cookie uint64
	Local  string
	Remote string
	Owner  Process
}

// Generation identifies the actual transport attachment. CID/ServerID alone
// cannot distinguish two Caddy processes forwarding to the same NATS server.
type Attachment struct {
	Participant      ID
	Generation       uint64
	ServerID         string
	CID              uint64
	URL              string
	Raw              Socket
	Ingress          Process
	HealthGeneration uint64
}

// Store identifies the actual directory and permanent lock inode, including
// aliases and bind mounts. A pathname is not a lifetime-lock ownership proof.
type Store struct {
	Device     uint64
	Inode      uint64
	MountID    uint64
	LockDevice uint64
	LockInode  uint64
}
type ArtifactKind uint8

const (
	NATSBinary     ArtifactKind = 1
	CaddyBinary    ArtifactKind = 2
	HelperBinary   ArtifactKind = 3
	LauncherBinary ArtifactKind = 4
)

// Archive identity and running binary identity are distinct. CatalogSHA256
// binds the allowlist; these bytes alone do not prove a process is running them.
type Artifact struct {
	Kind          ArtifactKind
	Version       string
	GOOS          string
	GOARCH        string
	ArchiveSHA256 Digest
	BinarySHA256  Digest
	CatalogSHA256 Digest
}

// Only NATS and Caddy are public components. HostSupport is reserved for root
// policy/support operations and must never become a whole-host CLI shortcut.
type Plan struct {
	Tx             ID
	Step           uint32
	Domain         ID
	Component      Component
	Action         Action
	Candidate      Artifact
	Previous       Artifact
	Before         Process
	ConfigSHA256   Digest
	Store          Store
	PolicyRevision uint64
}

// Complete is a positive inventory assertion, never the default for an empty
// or inaccessible enumeration. OldOwnerPending counts responsibilities, whereas
// LiveBusiness may remain nonzero after those responsibilities have moved.
type Census struct {
	Complete        bool
	Generation      uint64
	Processes       []Process
	Sockets         []Socket
	LiveBusiness    uint32
	OldOwnerPending uint32
	Digest          Digest
}

// Completed requires this step's old and temporary owners to retire, a verified
// return, and root/S8 journal agreement. Files installed and runtime ready are
// earlier completion points. LiveBusiness may remain positive while
// OldOwnerPending must be zero; long tasks need not finish. A Status projection
// cannot supply its own independent observation or retirement proof.
// origin: hot-update S9 plan §2.3 / T01
type Status struct {
	Tx       ID
	Step     uint32
	Phase    Phase
	Revision uint64
	Code     string
	Detail   string
	Before   Process
	Current  Process
	Census   Census
	Proof    Digest
}

// A seal comes from the actual mutation admission owner after joining pending
// writes; two coincidentally identical LIST responses cannot produce this fact.
type AssetSeal struct {
	Revision     uint64
	Accounts     Digest
	Assets       Digest
	Participants Digest
	Complete     bool
}
type Readiness struct {
	Topology   Digest
	Seal       AssetSeal
	Auth       Digest
	WSS        Digest
	Running    Process
	MetaLeader string
	Groups     Digest
	Complete   bool
}
type RoutePurpose uint8

const (
	RoutePrepare RoutePurpose = 1
	RouteBind    RoutePurpose = 2
)

type RouteDirection uint8

const (
	RouteOutbound RouteDirection = 1
	RouteReturn   RouteDirection = 2
)

type RouteTarget struct {
	URL       string
	NodeID    string
	Component Component
	Before    Process
	Candidate Artifact
	Ingress   Process
}
type MoveResult struct {
	Census      Census
	Attachments []Attachment
}

// Prepare has no New attachment. Bind supplies physical evidence after the
// move, preserving the original nonce and remaining budget rather than renewing
// the eight-second protection. The route owner signs a separate purpose domain.
type RouteProof struct {
	Purpose           RoutePurpose
	Direction         RouteDirection
	Target            RouteTarget
	Version           uint16
	Tx                ID
	Participant       ID
	Old               Attachment
	New               Attachment
	HomeDigest        Digest
	AuthorityRevision uint64
	PolicyRevision    uint64
	RemainingNS       uint64
	Nonce             ID
	KeyID             string
	Signature         []byte
}

// S7's real source/schema/receipt identity is required. A compile adapter cannot
// advertise continuity by setting Complete on a fabricated checkpoint.
type TransferSeal struct {
	Version       uint16
	SourceSHA256  Digest
	SchemaSHA256  Digest
	InvocationSet Digest
	ReceiptSet    Digest
	ConsumerSet   Digest
	Complete      bool
}
type Checkpoint struct {
	Cohort    ID
	Assets    AssetSeal
	Transfers TransferSeal
	Proof     Digest
}

// Subject and Evidence identify a real producer and its independent evidence.
// Sanitized public projections do not include raw socket tuples or private keys.
type Observation struct {
	Version    uint16
	Tx         ID
	Step       uint32
	Kind       string
	BootID     string
	BootNS     int64
	Subject    ID
	Generation uint64
	Evidence   Digest
}

// ObservationKinds returns independent storage for the closed vocabulary.
// Actual emitters and independent evidence belong to implementation owners.
// origin: hot-update S9 plan §2.3 / T01
func ObservationKinds() []string {
	return []string{
		"artifact_verified", "asset_sealed", "successor_installed",
		"attachment_changed", "auth_usable", "business_ready", "signal_applied",
		"old_exit", "storage_finalized", "startup_released", "return_verified",
		"owner_retired", "completed", "halted",
	}
}
