// Package xferport owns the filesystem transfer ports and their value closure.
// It depends only on the standard library so importing a filesystem contract
// cannot pull a transport implementation into transferpath.
// origin: hot-update S7 plan §8.1 / §11.1 / P00
package xferport

import (
	"encoding/json"
	"os"
)

// HandoffResource binds the existing handoff.Resource without importing its
// owner. D similarly binds controlbudget.Deadline at the alias boundary. These
// parameters retain the exact named types in the frozen consumer signatures;
// copying either type into this package would break factory assignability.
type HandoffResource interface {
	~struct {
		ID    string
		Kind  string
		Data  json.RawMessage
		Files map[string]*os.File
	}
}

// Salvage retains the entire original descriptor closure, never just FD numbers.
type Salvage[R HandoffResource] struct{ Resources []R }

// Phase records retained responsibility, not a success bit. A decided transfer
// can still own history publication, cleanup and capacity after byte work ends.
type Phase string

const (
	PhaseOpen             Phase = "open"
	PhasePrepared         Phase = "prepared"
	PhaseSending          Phase = "sending"
	PhaseReceiving        Phase = "receiving"
	PhaseCommitReserved   Phase = "commit_reserved"
	PhaseDecided          Phase = "decided"
	PhaseTerminalPending  Phase = "terminal_pending"
	PhaseHistoryConfirmed Phase = "history_confirmed"
	PhaseCleanupPending   Phase = "cleanup_pending"
	PhaseSettled          Phase = "settled"
)

// The grant authorizes one of these filesystem effects. Link cleanup and
// directory Sync are separate responsibilities and cannot repeat the effect.
type CommitPrimitive string

const (
	PrimitiveRename CommitPrimitive = "rename"
	PrimitiveLink   CommitPrimitive = "link"
)

// NotEntered needs an irreversible closed gate; Inflight cannot be guessed
// failed by a watchdog. Landed does not imply directory durability.
type PrimitiveOutcome string

const (
	OutcomeNotEntered PrimitiveOutcome = "not_entered"
	OutcomeInflight   PrimitiveOutcome = "inflight"
	OutcomeLanded     PrimitiveOutcome = "landed"
)

// Identity is shared by every receipt and mutation. EntryNonce prevents a late
// callback from retiring a replacement slot with the same business key.
type Identity struct {
	SID           string `json:"sid"`
	NID           string `json:"nid"`
	ActorFP       string `json:"actor_fp"`
	AgentInstance string `json:"agent_instance"`
	TransferID    string `json:"transfer_id"`
	EntryNonce    string `json:"entry_nonce"`
	IntentDigest  string `json:"intent_digest"`
	ObjectVersion string `json:"object_version"`
}

// Reserve persists this grant before a reply can escape. Repeated Reserve
// returns the same nonce/terminal slot, including after an unknown reply.
type CommitGrant struct {
	Identity         Identity `json:"identity"`
	GrantNonce       string   `json:"grant_nonce"`
	ReceiverInstance string   `json:"receiver_instance"`
	TerminalID       string   `json:"terminal_id"`
}

// An empty outcome is unresolved. In particular it is not a closed-gate proof.
// Landed plus an unsuccessful directory Sync retains the original primitive.
type ReceiverOutcome struct {
	Grant             CommitGrant      `json:"grant"`
	Primitive         CommitPrimitive  `json:"primitive"`
	Outcome           PrimitiveOutcome `json:"outcome"`
	Bytes             int64            `json:"bytes"`
	SHA256            string           `json:"sha256"`
	Code              string           `json:"code"`
	Error             string           `json:"error"`
	ParentSynced      bool             `json:"parent_synced"`
	TmpCleanupPending bool             `json:"tmp_cleanup_pending"`
}

// Both ID and name participate in TakeFile. Import must not guess a singleton
// name or reduce the handoff closure to descriptor integers.
type ResourceRef struct{ ResourceID, Name, Kind string }

// These are read-only checks on an inherited OFD. Matching device/inode alone
// is not proof that a pathname reopen preserved the original description.
type FileIdentity struct {
	Dev, Ino uint64
	Mode     uint32
	Flags    int32
}

// Import validates fingerprints and exact size without repairing the shared
// OFD. Names below are private relative basenames for the original parent FD;
// reopening a pathname is not a substitute for taking its ResourceRef.
type AtomicState struct {
	Phase                                       Phase
	Tmp, Parent, Source                         ResourceRef
	TmpIdentity, ParentIdentity, SourceIdentity FileIdentity
	TmpName, Destination                        string
	Force                                       bool
	DurableOffset                               int64
	CommitGrant                                 CommitGrant
	PrimitiveOutcome                            ReceiverOutcome
	DirSyncPending                              bool
	UnlinkPending                               bool
}

type FileSource interface {
	TakeFile(string, string) (*os.File, error)
}

// Commit is indivisible with respect to Freeze. The outcome records a landed
// primitive even if directory Sync or subsequent unlink still needs cleanup.
type AtomicPort[D ~int64, R HandoffResource] interface {
	WriteAt([]byte, int64) (int, error)
	Sync() error
	Commit(CommitGrant, D) ReceiverOutcome
	Freeze(D) (AtomicState, []R, error)
	Stop(D) error
}

type AtomicImportFactory[D ~int64, R HandoffResource] func(AtomicState, FileSource) (AtomicPort[D, R], Salvage[R], error)
