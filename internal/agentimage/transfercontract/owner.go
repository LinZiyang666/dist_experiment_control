// Package transfercontract defines the NATS-free ports shared by the agent
// image composition and its transfer adapter. Importing a port must not pull
// the image's unrelated runtime owners into the adapter's dependency closure.
// origin: S7 internal review IMAGE-LEAF F1
package transfercontract

import (
	"os"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

// TransferWorker is the agent's continuation worker as the image composition
// sees it. The agent-side adapter owns the NATS-bearing worker, its current
// bindings and the sealed worker codec; the image handles only the encoded
// state, the custody facts it validates and the borrowed resources. That is
// what keeps this package (and the bootstrap that restores it before any
// control connection exists) NATS-free.
// origin: hot-update S7 plan §8.1–§8.3 / P21 — R9 NATS-free image ports
type TransferWorker interface {
	Freeze(controlbudget.Deadline) (TransferFrozen, error)
	// Thaw binds the backend current at the call (the adapter resolves it):
	// a returning exec or a rebuilt session must never revive a stale one.
	Thaw(controlbudget.Deadline) error
}

// TransferFrozen is a joined worker. Export encodes its whole private state;
// DetachPristine retires a sleeping import and hands back what it took.
type TransferFrozen interface {
	Export() (TransferState, []handoff.Resource, error)
	DetachPristine() (xfervalue.Salvage, error)
}

// TransferImporter decodes the sealed bytes and imports a sleeping worker.
// Import takes descriptors only through the census-checked FileSource.
type TransferImporter interface {
	Decode([]byte) (TransferState, error)
	Import(TransferState, TransferFileSource) (TransferWorker, xfervalue.Salvage, error)
}

// TransferFileSource is the frozen worker FileSource shape (xferport).
type TransferFileSource interface {
	TakeFile(id, name string) (*os.File, error)
}

// TransferState is one encoded worker state and the facts composition checks
// against this bundle: lineage, joined admission, entry count, capacity and
// every resource reference the state names. The codec still owns field and
// phase validation of Encoded; these facts are projected by the same adapter
// from the same decoded value, never supplied separately by a caller.
type TransferState struct {
	Encoded         []byte
	Schema          string
	BootID          string
	InstanceID      string
	Generation      uint64
	AdmissionClosed bool
	Inflight        uint32
	Entries         uint32
	Reservations    []xfervalue.Reservation
	Refs            []xfervalue.ResourceRef
}
