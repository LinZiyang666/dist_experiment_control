// Package brokerimage defines broker cuts without importing broker or command wiring.
// origin: hot-update S6 plan §2.1 / §12.1 P00
package brokerimage

import (
	"database/sql"
	"os"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfertrack"
)

const (
	Domain         = "broker-image-v1"
	BundleVersion  = 1
	ImageRole      = "broker"
	MaxBundleBytes = 16 << 20
)

// Mode zero is invalid; a single broker must not claim a running Raft owner.
type Mode uint8

const (
	ModeSingle Mode = iota + 1
	ModeSoleVoter
	ModeQuorum
)

type Bundle struct {
	Schema          string                     `json:"schema"`
	SchemaVersion   uint32                     `json:"schema_version"`
	Role            string                     `json:"role"`
	Mode            Mode                       `json:"mode"`
	Generation      uint64                     `json:"generation"`
	ExporterSHA256  string                     `json:"exporter_sha256"`
	CandidateSHA256 string                     `json:"candidate_sha256"`
	PreviousSHA256  string                     `json:"previous_sha256"`
	InstanceID      string                     `json:"instance_id"`
	BootID          string                     `json:"boot_id"`
	Args            []string                   `json:"args"`
	ConfigDigest    string                     `json:"config_digest"`
	RequiredDomains []string                   `json:"required_domains"`
	StateResource   string                     `json:"state_resource"`
	Store           storage.StoreStamp         `json:"store"`
	Seal            storage.StoreSeal          `json:"seal"`
	LockResource    string                     `json:"lock_resource"`
	Listeners       []listenerimage.Descriptor `json:"listeners"`
	Tunnel          tunnel.ServerImage         `json:"tunnel"`
	Maintenance     []brokermaint.LocalLease   `json:"maintenance"`
	Transfers       *xfercontract.ImageRef     `json:"transfers,omitempty"`
}

// TransferOwner keeps the tracker and its sealed export under one terminal
// owner. Factories are injected by outer wiring; no compile stub is reachable.
// The S6 slot and CurrentOwners signatures remain unchanged.
// origin: hot-update S7 plan §8.2 / P24
type TransferOwner interface {
	xfertrack.Manager
	FreezeImage(controlbudget.Deadline) error
	ExportImage() (xfercontract.ImageRef, []handoff.Resource, error)
	ImportImage(xfercontract.ImageRef, tunnel.FileSource, controlbudget.Deadline) (TransferOwner, tunnel.Salvage, error)
	DetachImage() (xfercontract.Salvage, error)
	Rebind(controlbudget.Deadline) error
}

// LockResource borrows the original OFD; Prepare owns only its duplicates.
type StoreOwner interface {
	Quiesce(controlbudget.Deadline) error
	CloseForImage(controlbudget.Deadline) (storage.StoreSeal, error)
	ReopenCurrent(storage.StoreStamp, controlbudget.Deadline) (StoreOwner, error)
	Stamp() (storage.StoreStamp, error)
	LockResource() (handoff.Resource, error)
}
type ControlOwner interface {
	FenceTransfers(controlbudget.Deadline) (uint32, error)
	ResumeTransfers() error
	Fence(controlbudget.Deadline) error
	JoinStoreUsers(controlbudget.Deadline) error
	StopTransport(controlbudget.Deadline) error
	StartAuth(controlbudget.Deadline) error
	StartBusiness(controlbudget.Deadline) error
	StopCurrent(controlbudget.Deadline) error
}
type Config struct {
	Mode         Mode
	Store        StoreOwner
	Control      ControlOwner
	Server       *tunnel.Server
	Listeners    []listenerimage.Keeper
	FreezeServer func(*tunnel.Server, controlbudget.Deadline) (tunnel.ServerFrozen, error)
	ImportServer func(tunnel.ServerImportConfig, tunnel.ServerImage, tunnel.FileSource) (tunnel.ServerImported, tunnel.Salvage, error)
	Observe      controlbudget.Observer
	Deadline     controlbudget.Deadline
	NewStore     func(storage.StoreStamp, *os.File, controlbudget.Deadline) (StoreOwner, error)
	NewControl   func(StoreOwner, controlbudget.Deadline) (ControlOwner, error)
	TunnelConfig tunnel.ServerImportConfig
	Transfers    TransferOwner
}
type Cut interface {
	Export(storage.StoreSeal) (Bundle, []handoff.Resource, error)
	Prepare(Bundle, []handoff.Resource) (*handoff.Prepared, error)
	Thaw(controlbudget.Deadline) error
	Discard() error
}
type Imported interface {
	Activate(RuntimeSlot, controlbudget.Deadline) error
	DetachPristine() (tunnel.Salvage, error)
}

// Install and StopCurrent serialize replacement; an old generation cannot return.
type RuntimeSlot interface {
	Install(StoreOwner, ControlOwner, tunnel.ServerImported) error
	StopCurrent(controlbudget.Deadline) error
}
type SingleStoreConfig struct {
	DBPath   string
	DataDir  string
	Registry storage.DriverRegistry
}
type PreflightRequest struct {
	CandidatePath   string
	PreviousPath    string
	Config          []byte
	AllowedArgs     []string
	Stamp           storage.StoreStamp
	RequiredDomains []string
	Deadline        controlbudget.Deadline
}
type ReaderProof struct {
	SHA256          string   `json:"sha256"`
	ABI             uint32   `json:"abi"`
	Domains         []string `json:"domains"`
	MigrationDigest string   `json:"migration_digest"`
}

// Function aliases keep implementation bodies in their unique owner files.
type FreezeFunc = func(Config) (Cut, error)
type ImportFunc = func(Config, Bundle, tunnel.FileSource) (Imported, tunnel.Salvage, error)
type OpenSingleStoreFunc = func(SingleStoreConfig) (StoreOwner, *sql.DB, error)
type PreflightFunc = func(PreflightRequest) (candidate ReaderProof, previous ReaderProof, err error)
type NewForImageFunc = func(cluster.Config, storage.StoreStamp) (*cluster.Node, error)
type NewProductionForImageFunc = func(cluster.ProductionConfig, storage.StoreStamp) (*cluster.Node, error)
type PrepareImageShutdownFunc = func(*cluster.Node, controlbudget.Deadline) error
type ClosedImageStoreFunc = func(*cluster.Node) (storage.StoreSeal, error)
type ReadImageRoleFunc = func(*cluster.Node) (storage.RoleReceipt, error)

// P04 owns cluster.DataDirLock, absent from S5. Instantiating Lock with that
// concrete type preserves the exact pointer signature without a second lock type.
type AcquireImageDataDirLockFunc[Lock any] = func(string) (*Lock, error)
type AdoptDataDirLockFunc[Lock any] = func(string, *os.File) (*Lock, error)
type DataDirLockExportFunc[Lock any] = func(*Lock) (handoff.Resource, error)
type DataDirLockDetachFunc[Lock any] = func(*Lock) (*os.File, error)
type DataDirLockReleaseFunc[Lock any] = func(*Lock) error
type TakeRoleFunc = func(role string) (*handoff.Restored, error)

// Stable details keep the existing outer code namespaces and CLI exit meanings.
// origin: hot-update S6 plan §9.1
const (
	DetailMaintenanceObserverUnsupported = "maintenance_observer_unsupported"
	DetailMaintenanceExpired             = "maintenance_expired"
	DetailStoreCloseUnknown              = "store_close_unknown"
	DetailControlResultUnknown           = "control_result_unknown"
)
