// origin: hot-update S6 plan §2.1 / §12.1 P00
package storage

import (
	"database/sql"
)

type DBIdentity struct {
	Path     string `json:"path"`
	DataDir  string `json:"data_dir"`
	LockPath string `json:"lock_path"`
	Device   uint64 `json:"device"`
	Inode    uint64 `json:"inode"`
}

type SchemaStamp struct {
	Applied      []MigrationDigest `json:"applied"`
	Supported    []MigrationDigest `json:"supported"`
	Digest       string            `json:"digest"`
	ObjectDigest string            `json:"object_digest"`
}

type MigrationDigest struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
}

type StoreStamp struct {
	Identity         DBIdentity  `json:"identity"`
	Schema           SchemaStamp `json:"schema"`
	Journal          string      `json:"journal"`
	Synchronous      int         `json:"synchronous"`
	AppliedIndex     uint64      `json:"applied_index"`
	AppliedTerm      uint64      `json:"applied_term"`
	RaftLastIndex    uint64      `json:"raft_last_index"`
	RaftConfigIndex  uint64      `json:"raft_config_index"`
	RaftConfigDigest string      `json:"raft_config_digest"`
}

// Stopped is not closed: an unknown close forbids a second writer.
type ClosePhase uint8

const (
	CloseOpen ClosePhase = iota
	CloseStopped
	CloseClosed
	CloseUnknown
)

type StoreSeal struct {
	Stamp          StoreStamp  `json:"stamp"`
	Writer         ClosePhase  `json:"writer"`
	ReadOnly       ClosePhase  `json:"read_only"`
	Bolt           ClosePhase  `json:"bolt"`
	Transport      ClosePhase  `json:"transport"`
	Raft           ClosePhase  `json:"raft"`
	Role           RoleReceipt `json:"role"`
	DriverWriters  uint32      `json:"driver_writers"`
	DriverReaders  uint32      `json:"driver_readers"`
	ActiveRPCs     uint32      `json:"active_rpcs"`
	ActiveBackups  uint32      `json:"active_backups"`
	CheckpointBusy int         `json:"checkpoint_busy"`
	CheckpointLog  int         `json:"checkpoint_log"`
	CheckpointDone int         `json:"checkpoint_done"`
}

type RoleReceipt struct {
	EdgeSeq             uint64 `json:"edge_seq"`
	Term                uint64 `json:"term"`
	ConfigIndex         uint64 `json:"config_index"`
	VoterDigest         string `json:"voter_digest"`
	LastRunningIsLeader bool   `json:"last_running_is_leader"`
	ObserverClosed      bool   `json:"observer_closed"`
}

type OpenImageConfig struct {
	Stamp    StoreStamp
	Registry DriverRegistry
}

// Connections counts real driver connections, including lazy readers and backups.
type DriverRegistry interface {
	Connections(DBIdentity) (writers uint32, readers uint32)
	AssertExclusive(DBIdentity) error
}

// OpenCurrentFunc preserves the pure current-store opener seam implemented by P03.
type OpenCurrentFunc = func(OpenImageConfig) (*sql.DB, error)
