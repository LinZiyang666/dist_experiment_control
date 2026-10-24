package xferserve

import (
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// The durable head names ONE coordinator, rather than one coordinator per
// leader. The original owner alone can add its closed-and-joined state. A new
// leader CASes a successor only after that proof, never from an equal INFO.
// origin: hot-update S7 plan §2.1 / §8.2 / T05; S7 internal review rv-jsnats F1
type configHead struct {
	Origin     x.Origin             `json:"origin"`
	Generation uint64               `json:"generation"`
	Nonce      string               `json:"nonce"`
	Retired    *x.ConfigWriterState `json:"retired,omitempty"`
}

type clusterWriter struct {
	directory *Directory
	origin    x.Origin
	conn      func() *nats.Conn
	leader    func() bool
	make      func(x.ConfigWriterProof, *x.ConfigWriterState) (x.BucketPort, error)
	gate      chan struct{}
	local     x.BucketPort
	head      configHead
	closed    bool
}

func (w *clusterWriter) lock(d x.Deadline) error {
	left, err := controlbudget.Remaining(d)
	if err != nil {
		return err
	}
	timer := time.NewTimer(left)
	defer timer.Stop()
	select {
	case w.gate <- struct{}{}:
		return nil
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}
func (w *clusterWriter) unlock() { <-w.gate }

func (w *clusterWriter) readHead(d x.Deadline) (configHead, uint64, error) {
	data, seq, err := w.directory.read(authorityPrefix+"config.writer", d)
	var h configHead
	if err != nil || data == nil {
		return h, seq, err
	}
	if decodeClosed(data, &h) != nil || h.Origin.BrokerID == "" || h.Origin.InstanceID == "" || h.Origin.BootID == "" || h.Generation == 0 || h.Nonce == "" {
		return h, 0, xferauth.ControlRefusal(x.CodeResultUnknown)
	}
	return h, seq, nil
}

func sameHead(a, b configHead) bool {
	return a.Origin == b.Origin && a.Generation == b.Generation && a.Nonce == b.Nonce
}

func (w *clusterWriter) acquire(d x.Deadline) error {
	if w.closed || w.leader == nil || !w.leader() {
		return xferauth.ControlRefusal(x.CodeOwnerRecovering)
	}
	h, seq, err := w.readHead(d)
	if err != nil {
		return err
	}
	if seq != 0 && sameHead(h, w.head) && h.Retired == nil && w.local != nil {
		return nil
	}
	if seq != 0 && h.Retired == nil {
		// This call names the exact previous epoch. A removed/unreachable owner
		// cannot be replaced, even when the current leader is healthy.
		// origin: hot-update S7 plan §2.1 / T05
		_, err := authorityRPC(w.conn(), authorityRequest{Operation: "retire", Origin: h.Origin, Head: &h}, d)
		if err != nil {
			return err
		}
		h, seq, err = w.readHead(d)
		if err != nil || h.Retired == nil {
			return xferauth.ControlRefusal(x.CodeResultUnknown)
		}
	}
	generation := uint64(1)
	if seq != 0 {
		generation = max(h.Generation, h.Retired.Admission.Generation) + 1
	}
	if generation == 0 {
		return xferauth.ControlRefusal(x.CodeResultUnknown)
	}
	next := configHead{Origin: w.origin, Generation: generation, Nonce: nuid.Next()}
	proof := x.ConfigWriterProof{BrokerID: w.origin.BrokerID, InstanceID: w.origin.InstanceID, BootID: w.origin.BootID, Generation: generation, Nonce: next.Nonce}
	local, err := w.make(proof, h.Retired)
	if err != nil {
		return err
	}
	// Retain this candidate before sending its durable claim. A lost claim ACK
	// does not permit config IO, but its origin must still be able to close and
	// prove retirement of the candidate if the claim lands after this call.
	// origin: hot-update S7 plan §2.1 / §8.2 / T05
	w.head, w.local = next, local
	if err = w.directory.publish(authorityPrefix+"config.writer", next, seq, d); err != nil {
		read, _, readErr := w.readHead(d)
		if readErr != nil || !sameHead(read, next) || read.Retired != nil {
			return xferauth.ControlRefusal(x.CodeResultUnknown)
		}
	}
	if !w.leader() {
		return xferauth.ControlRefusal(x.CodeOwnerRecovering)
	}
	return nil
}

func (w *clusterWriter) Apply(p x.ConfigPatch, d x.Deadline) (x.BucketSeal, error) {
	if err := w.lock(d); err != nil {
		return x.BucketSeal{}, err
	}
	defer w.unlock()
	if err := w.acquire(d); err != nil {
		return x.BucketSeal{}, err
	}
	return w.local.Apply(p, d)
}

func (w *clusterWriter) Shrink(p x.ConfigPatch, d x.Deadline) (x.BucketSeal, error) {
	if err := w.lock(d); err != nil {
		return x.BucketSeal{}, err
	}
	defer w.unlock()
	if err := w.acquire(d); err != nil {
		return x.BucketSeal{}, err
	}
	shrinker, ok := w.local.(xferbucket.Shrinker)
	if !ok {
		return x.BucketSeal{}, xferauth.ControlRefusal(x.CodeOwnerRecovering)
	}
	return shrinker.Shrink(p, d)
}

func (w *clusterWriter) retire(expected *configHead, d x.Deadline) (state x.ConfigWriterState, result error) {
	if err := w.lock(d); err != nil {
		return x.ConfigWriterState{}, err
	}
	defer w.unlock()
	wasClosed := w.closed
	if expected == nil {
		// Freeze closes the wrapper's future gate under the SAME serializer as
		// retirement. Unlocking between the proof and this fence would allow a
		// same-origin successor to enter before the image takes its snapshot.
		// origin: hot-update S7 plan §2.1 / §8.2 / T05
		w.closed = true
		defer func() {
			if result != nil {
				w.closed = wasClosed
			}
		}()
	}
	if expected != nil && (!sameHead(*expected, w.head) || expected.Origin != w.origin) {
		return x.ConfigWriterState{}, xferauth.ControlRefusal(x.CodeResultUnknown)
	}
	if w.local == nil {
		return x.ConfigWriterState{Admission: x.AdmissionState{Closed: true}}, nil
	}
	state, err := w.local.Freeze(d)
	if err != nil {
		return state, err
	}
	state.Admission.Generation = max(state.Admission.Generation, w.head.Generation)
	h, seq, err := w.readHead(d)
	if err != nil {
		return state, err
	}
	if !sameHead(h, w.head) {
		// A successor already consumes this exact proof; the old gate remains
		// closed, and repeated image/Stop retirement is a read-only operation.
		return state, nil
	}
	if h.Retired != nil {
		return *h.Retired, nil
	}
	h.Retired = &state
	if err = w.directory.publish(authorityPrefix+"config.writer", h, seq, d); err != nil {
		actual, _, readErr := w.readHead(d)
		if readErr != nil || !sameHead(actual, h) || actual.Retired == nil {
			return state, xferauth.ControlRefusal(x.CodeResultUnknown)
		}
	}
	return state, nil
}

func (w *clusterWriter) Freeze(d x.Deadline) (x.ConfigWriterState, error) {
	return w.retire(nil, d)
}
func (w *clusterWriter) Stop(d x.Deadline) error { _, err := w.Freeze(d); return err }

func clusterCoordinator(h Host, a *Directory, origin x.Origin, base x.Base, jsConfig xferbucket.Config) x.BucketPort {
	return &clusterWriter{directory: a, origin: origin, conn: h.Conn, leader: h.Leader, gate: make(chan struct{}, 1), make: func(owner x.ConfigWriterProof, previous *x.ConfigWriterState) (x.BucketPort, error) {
		authority, err := xferbucket.NewAuthority(owner, previous)
		if err != nil {
			return nil, err
		}
		cfg := jsConfig
		cfg.Base, cfg.Authority = base, authority
		return xferbucket.NewWithCensus(cfg, h.Census)
	}}
}
