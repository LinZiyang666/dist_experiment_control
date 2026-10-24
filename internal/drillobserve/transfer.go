package drillobserve

import (
	"encoding/json"
	"reflect"
	"sync"

	"github.com/LinZiyang666/tether/internal/drillobserve/receipt"
)

// Receipt encoding is pure; startup custody, kernel IO and process generations
// remain owned here. Aliases keep producers and independent readers on one ABI.
// origin: hot-update S7 plan §11.1 / T51
const TransferSchemaVersion = receipt.TransferSchemaVersion
const TransferEventKind = receipt.TransferEventKind
const TransferPoisonKind = receipt.TransferPoisonKind
const TransferSetupRC = receipt.TransferSetupRC

var ErrTransferCustody = receipt.ErrTransferCustody

type TransferProcess = receipt.TransferProcess
type TransferSequence = receipt.TransferSequence
type TransferOFDReceipt = receipt.TransferOFDReceipt
type TransferCustodyReader = receipt.TransferCustodyReader
type transferObservation = receipt.TransferObservation
type transferResource = receipt.TransferResource
type transferDeadline = receipt.TransferDeadline

func EncodeTransfer(value any) (Event, error)             { return receipt.EncodeTransfer(value) }
func DecodeTransfer(e Event) (transferObservation, error) { return receipt.DecodeTransfer(e) }
func TransferPendingBoundary(window string, value any) bool {
	return receipt.TransferPendingBoundary(window, value)
}
func TransferDrainRequest(nonce [16]byte) ([]byte, error) { return receipt.TransferDrainRequest(nonce) }
func ValidateTransferDrainACK(data []byte, nonce [16]byte, sealedSequence uint64) error {
	return receipt.ValidateTransferDrainACK(data, nonce, sealedSequence)
}
func copyTransferValue(dst, src reflect.Value) error { return receipt.CopyTransferValue(dst, src) }
func transferToken(s string, required bool) bool     { return receipt.TransferToken(s, required) }
func transferDigest(s string) bool                   { return receipt.TransferDigest(s) }
func validTransferResource(r transferResource, required bool) bool {
	return receipt.ValidTransferResource(r, required)
}
func transferObject(v reflect.Value) (map[string]any, error) { return receipt.TransferObject(v) }
func transferRawObject(data []byte) (map[string]json.RawMessage, error) {
	return receipt.TransferRawObject(data)
}
func decodeTransferObject(fields map[string]json.RawMessage, v reflect.Value) error {
	return receipt.DecodeTransferObject(fields, v)
}

// NewTransferObserver is inert until immutable startup custody is installed.
func NewTransferObserver[T any](process TransferProcess) func(T) {
	return receipt.NewTransferObserver[T](process, Hook, transferNow, poisonTransferCustody)
}

var transferProcessObservers = struct {
	sync.Mutex
	byGeneration map[uint64]func(transferObservation)
}{byGeneration: make(map[uint64]func(transferObservation))}

// ProcessTransferObserver joins separately constructed production leaves to
// one ordinal stream for their actual image generation. A new Base must not
// restart the sequence while another leaf still emits into the same custody.
// The caller supplies the owner generation and responsibility, not progress;
// all byte, deadline, outcome and resource evidence remains producer-owned.
// origin: S7 internal review INTEG-M F1 / hot-update S7 plan §10.4
func ProcessTransferObserver[T any](generation uint64, leg string) func(T) {
	if Hook == nil {
		return nil
	}
	if generation == 0 || !transferToken(leg, true) {
		transferPoison(Hook)
		return nil
	}
	transferProcessObservers.Lock()
	observe := transferProcessObservers.byGeneration[generation]
	if observe == nil {
		process, err := TransferProcessIdentity(generation, "")
		if err != nil {
			transferProcessObservers.Unlock()
			transferPoison(Hook)
			return nil
		}
		observe = NewTransferObserver[transferObservation](process)
		transferProcessObservers.byGeneration[generation] = observe
	}
	transferProcessObservers.Unlock()
	return func(value T) {
		var o transferObservation
		if copyTransferValue(reflect.ValueOf(&o).Elem(), reflect.ValueOf(value)) != nil {
			transferPoison(Hook)
			return
		}
		if o.Leg == "" {
			o.Leg = leg
		}
		if o.ReaderSHA == "" && (o.Kind == "import" || o.Kind == "install" || o.Kind == "rollback") {
			var err error
			o.ReaderSHA, err = transferRunningReader()
			if err != nil {
				transferPoison(Hook)
				return
			}
		}
		observe(o)
	}
}

// Preserve both a semantic poison marker and the channel's sticky failure.
// A later fresh drain must not return a clean ACK after invalid typed evidence.
// origin: hot-update S7 plan §10.4 / T41
func transferPoison(hook func(Event)) {
	hook(Event{Kind: TransferPoisonKind, Domain: "transfer", Values: [16]uint64{TransferSetupRC}})
	poisonTransferCustody()
}
