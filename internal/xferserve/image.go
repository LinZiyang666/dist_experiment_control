package xferserve

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"sync"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/xferbridge"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/LinZiyang666/tether/internal/xfertrack"
)

// ImageConfig resolves the current graph after the image slot installs its
// current SQL/NATS backends. Rebuild must consume State before subscriptions.
// The managed slot owns Stop, including failures before business starts.
// origin: hot-update S7 plan §8.2 / T36
type ImageConfig struct {
	Capacity func() x.CapacityAccount
	Current  func() (x.TrackerManager, x.BucketPort)
	Rebuild  func(*x.TrackerState, x.Deadline) (x.TrackerManager, error)
	Stop     func(x.Deadline) error
	Resume   func(x.Deadline) error
}

// ImageOwner keeps the full tracker projection while its old backend borrowers
// retire. Import is sleeping and network-free; Rebind constructs new borrowers
// around that projection after StartAuth, before any continuation SUB.
type ImageOwner struct {
	x.TrackerManager
	mu                 sync.Mutex
	cfg                ImageConfig
	state              *x.TrackerState
	snapshot           *sealedstate.Snapshot
	account            x.CapacityAccount
	reservation        x.ReservationID
	pristine, detached bool
}

func NewImageOwner(cfg ImageConfig) *ImageOwner { return &ImageOwner{cfg: cfg} }

// Census names every entry of the tracker the current graph resolves, for the
// S8 broker census, through the tracker's own serial token bounded by d. A
// graph with no current tracker (a sleeping import before Rebind) cannot say
// what it holds and refuses instead of reporting none.
// origin: hot-update S8 plan §4.2 / M06 / M14; S8 BROKER-EXPORTER R-11
func (o *ImageOwner) Census(d x.Deadline) ([]xfertrack.CensusEntry, error) {
	if o.cfg.Current == nil {
		return nil, brokerimage.ErrImage
	}
	tracker, _ := o.cfg.Current()
	if tracker == nil {
		return nil, brokerimage.ErrImage
	}
	return xfertrack.Census(tracker, d)
}

func (o *ImageOwner) FreezeImage(d x.Deadline) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.detached {
		return brokerimage.ErrImage
	}
	if o.state != nil {
		return nil
	}
	tracker, buckets := o.cfg.Current()
	if tracker == nil || buckets == nil || o.cfg.Capacity == nil {
		return brokerimage.ErrImage
	}
	// Resolve the shared account while the old graph still exists. Its store
	// users are joined before ExportImage, which must not query a cleared slot.
	o.account = o.cfg.Capacity()
	if o.account == nil {
		return brokerimage.ErrImage
	}
	o.TrackerManager = tracker
	frozen, err := tracker.Freeze(d)
	if err != nil {
		return err
	}
	state, resources, err := frozen.Export()
	if err != nil {
		return o.resumeRefusedFreeze(d, err)
	}
	// The tracker has no business FDs. A future resource-bearing version must
	// extend this owner rather than silently dropping its custody.
	if len(resources) != 0 {
		return o.resumeRefusedFreeze(d, brokerimage.ErrImage)
	}
	writer, err := buckets.Freeze(d)
	if err != nil {
		return o.resumeRefusedFreeze(d, err)
	}
	state.ConfigAttempts = writer.Attempts
	o.state = &state
	return nil
}

// A refused coordinator cut leaves its original gate usable. The tracker
// was already frozen, so resume that same graph instead of stranding it while
// trying to stop a sent-unknown config writer.
// origin: hot-update S7 plan §2.1 / §8.2 / T36
func (o *ImageOwner) resumeRefusedFreeze(d x.Deadline, cause error) error {
	if o.cfg.Resume == nil {
		return cause
	}
	return errors.Join(cause, o.cfg.Resume(d))
}

func (o *ImageOwner) ExportImage() (x.ImageRef, []handoff.Resource, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state == nil || o.detached {
		return x.ImageRef{}, nil, brokerimage.ErrImage
	}
	data, err := xferimage.EncodeTracker(*o.state)
	if err != nil {
		return x.ImageRef{}, nil, err
	}
	if o.snapshot == nil {
		if o.account == nil {
			return x.ImageRef{}, nil, brokerimage.ErrImage
		}
		// Charge the original, prepared and witness descriptor peak against the
		// same account as retained tracker entries before allocating the memfd.
		// origin: hot-update S7 plan §8.3 / T36
		o.reservation, err = o.account.Reserve(x.Reservation{SealedBytes: int64(len(data) + 16), NonReplayBytes: int64(len(data)), FDs: 3, Resources: 1})
		if err != nil {
			return x.ImageRef{}, nil, err
		}
		o.snapshot, err = sealedstate.New("transfer.broker.state", data)
		if err != nil {
			return x.ImageRef{}, nil, errors.Join(err, o.releaseSnapshot())
		}
		o.snapshot.Resources[0].Kind = xferbridge.StateKind
	}
	sum := sha256.Sum256(data)
	return x.ImageRef{Schema: xferbridge.Domain, ResourceID: "transfer.broker.state", Digest: hex.EncodeToString(sum[:]),
		Count: uint32(len(o.state.Entries)), EncodedBytes: uint64(len(data))}, o.snapshot.Resources, nil
}

func (o *ImageOwner) ImportImage(ref x.ImageRef, files tunnel.FileSource, _ x.Deadline) (brokerimage.TransferOwner, tunnel.Salvage, error) {
	if xferbridge.ValidateRef(ref) != nil || files == nil {
		return nil, tunnel.Salvage{}, brokerimage.ErrImage
	}
	data, file, err := sealedstate.Read(imageFiles{files}, ref.ResourceID, "state", int64(ref.EncodedBytes))
	var salvage tunnel.Salvage
	if file != nil {
		salvage.Resources = []handoff.Resource{{ID: ref.ResourceID, Kind: xferbridge.StateKind, Files: map[string]*os.File{"state": file}}}
	}
	sum := sha256.Sum256(data)
	if err != nil || uint64(len(data)) != ref.EncodedBytes || hex.EncodeToString(sum[:]) != ref.Digest {
		return nil, salvage, brokerimage.ErrImage
	}
	state, err := xferimage.DecodeTracker(data)
	if err != nil || uint32(len(state.Entries)) != ref.Count {
		return nil, salvage, errors.Join(err, brokerimage.ErrImage)
	}
	return &ImageOwner{cfg: o.cfg, state: &state, pristine: true}, salvage, nil
}

func (o *ImageOwner) DetachImage() (x.Salvage, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.pristine || o.detached {
		return x.Salvage{}, brokerimage.ErrImage
	}
	o.detached = true
	return x.Salvage{}, nil // The outer image retains the decode-only sealed FD.
}

func (o *ImageOwner) Rebind(d x.Deadline) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.detached {
		return brokerimage.ErrImage
	}
	if o.state == nil {
		return brokerimage.ErrImage
	}
	// A refused store cut may still have the old graph. Join it before the
	// replacement can obtain config-writer authority or touch the same ledger.
	if err := o.cfg.Stop(d); err != nil {
		return err
	}
	manager, err := o.cfg.Rebuild(o.state, d)
	if manager != nil {
		o.TrackerManager = manager
	}
	if err != nil {
		return err
	}
	if manager == nil {
		return brokerimage.ErrImage
	}
	o.state, o.pristine = nil, false
	return o.releaseSnapshot()
}

func (o *ImageOwner) Stop(d x.Deadline) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.detached {
		return nil
	}
	if err := o.cfg.Stop(d); err != nil {
		return err
	}
	return o.releaseSnapshot()
}

func (o *ImageOwner) releaseSnapshot() error {
	if err := o.snapshot.Close(); err != nil {
		return err
	}
	o.snapshot = nil
	if o.reservation != "" {
		if err := o.account.Release(o.reservation); err != nil {
			return err
		}
		o.reservation = ""
	}
	return nil
}

type imageFiles struct{ tunnel.FileSource }

func (f imageFiles) TakeFile(id, name string) (*os.File, error) {
	return f.FileSource.TakeFile(id + "/" + name)
}
