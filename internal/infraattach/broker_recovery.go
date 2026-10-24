package infraattach

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrarepair"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// broker_recovery.go binds P18's producer gate to the broker composition: the
// read-only management NC, every legacy producer root with its real retained
// obligation, the original-ACK ledger and the pre-business verification that
// P17's auth-first admission runs before any business SUB can replay.
// origin: hot-update S9 plan §3.2 / §4.3 / P22 / T49; S9 P18 requests

// The read-only management NC is the only client P18 reads through. It owns
// no business SUB: its reconnect replays only its own reply inbox, so it can
// read the original store while the business NC is still refused.
// origin: hot-update S9 plan §4.3 / T40 / T49
func (o *Broker) bindRecovery() error {
	r := o.cfg.Infra.Recovery
	if gate, _ := o.recovery(); r == nil || gate != nil {
		return nil
	}
	mgmt, err := nats.Connect(o.cfg.URL, o.local("tetherd-recovery", nil)...)
	if err != nil {
		return err
	}
	var gate *infrarepair.Gate
	js, err := jetstream.New(mgmt)
	if err == nil {
		gate, err = infrarepair.New(infrarepair.Config{Domain: r.Domain, Required: infrarepair.LegacyRoots(),
			Accounts: map[string]jetstream.JetStream{"$G": js}, Assets: r.Assets, Exclusive: r.Exclusive, Transfers: r.Transfers})
	}
	for _, name := range infrarepair.LegacyRoots() {
		if err != nil {
			break
		}
		err = gate.Register(o.producer(name))
	}
	if err != nil {
		mgmt.Close()
		return err
	}
	o.mu.Lock()
	o.gate, o.mgmt, o.mgmtJS = gate, mgmt, js
	o.mu.Unlock()
	return nil
}

func (o *Broker) recovery() (*infrarepair.Gate, jetstream.JetStream) {
	if o == nil {
		return nil, nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.gate, o.mgmtJS
}

// producer registers every legacy root with its real retained obligation.
// Every root shares the business NC, so Retire is that transport's retirement:
// without it one past sent-unknown request (any JS timeout or "no response")
// would fail every later Seal, and a failed Seal keeps the gate closed — every
// broker producer would then stay fenced until the process restarts.
// origin: hot-update S9 plan §3.2 / §4.3 / T41 / T49; S9 P18 request "transport retirement"
func (o *Broker) producer(name string) infrarepair.Producer {
	p := infrarepair.Producer{Name: name, Retire: o.retireProducer,
		Capture: func(c.Deadline) (infrarepair.Responsibilities, error) { return infrarepair.Responsibilities{}, nil },
		Check:   func(infrarepair.Responsibilities, c.Deadline, c.Stop) error { return nil }}
	switch name {
	case infrarepair.AuditPublish:
		p.Acknowledged = o.acknowledged
		p.Capture = o.captureACKs
	case infrarepair.AuditReplay:
		identity := c.Digest(sha256.Sum256([]byte("tether-broker-audit-replay-cursor")))
		p.Capture = func(c.Deadline) (infrarepair.Responsibilities, error) {
			cursor, ok, err := o.auditCursor()
			if err != nil || !ok {
				return infrarepair.Responsibilities{}, err
			}
			return infrarepair.Responsibilities{Local: []infrarepair.LocalReceipt{{Identity: identity, PublisherCursor: cursor}}}, nil
		}
		// A replay that advanced before the original prefix was read is the
		// repair-before-read failure itself; it is a receipt change, not lag.
		p.Check = func(r infrarepair.Responsibilities, _ c.Deadline, _ c.Stop) error {
			cursor, ok, err := o.auditCursor()
			if err != nil {
				return err
			}
			for _, local := range r.Local {
				if !ok || local.PublisherCursor != cursor {
					return infrarepair.ErrReceipt
				}
			}
			return nil
		}
	}
	return p
}

// retireUnknowns retires the business transport ONCE after a Seal join found a
// sent-unknown request, then clears each root through P18's RetireUnknown,
// whose Retire hook (retireProducer) is already satisfied by that retirement.
// A root that is not unknown answers ErrUnknown and is left to the re-Seal.
func (o *Broker) retireUnknowns(gate *infrarepair.Gate, d c.Deadline) error {
	if err := o.retireBusiness(d, nil); err != nil {
		return err
	}
	o.mu.Lock()
	o.retiredPass = true
	o.mu.Unlock()
	defer func() {
		o.mu.Lock()
		o.retiredPass = false
		o.mu.Unlock()
	}()
	// Include the original data owners, whose transports are independent of business.
	// origin: hot-update S9 plan §4.3 / T40; NC-EXPORTS ACK registration
	return infrarepair.RetireUnknownProducers(gate, d, nil)
}

// retireProducer is every root's Retire. Outside a retireUnknowns pass it
// performs the shared retirement itself; inside one it was already performed
// after the gate closed, after every unknown request had been sent.
func (o *Broker) retireProducer(d c.Deadline, stop c.Stop) error {
	o.mu.Lock()
	pass := o.retiredPass
	o.mu.Unlock()
	if pass {
		return nil
	}
	return o.retireBusiness(d, stop)
}

func (o *Broker) auditCursor() (uint64, bool, error) {
	if o.cfg.AuditCursor == nil {
		return 0, false, nil
	}
	return o.cfg.AuditCursor()
}

// acknowledged keeps a bounded window of original server ACKs, never a
// post-restart LastSeq. Producer admission is joined before Capture reads it.
func (o *Broker) acknowledged(ack infrarepair.ACK) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ledger = append(o.ledger, ack)
	if len(o.ledger) > brokerACKWindow {
		o.ledger = append([]infrarepair.ACK(nil), o.ledger[len(o.ledger)-brokerACKWindow:]...)
	}
	return nil
}

// captureACKs freezes the receipts this cut must preserve. A stream already
// deleted, or a sequence already below its first retained sequence, left
// before the seal through an admitted producer or a stream limit; neither is
// this cut's responsibility. Everything kept is read back before replay.
func (o *Broker) captureACKs(d c.Deadline) (infrarepair.Responsibilities, error) {
	o.mu.Lock()
	acks := append([]infrarepair.ACK(nil), o.ledger...)
	o.mu.Unlock()
	_, js := o.recovery()
	if len(acks) == 0 {
		return infrarepair.Responsibilities{}, nil
	}
	left, err := controlbudget.Remaining(d)
	if err != nil {
		return infrarepair.Responsibilities{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), left) // ctx-none: P18 Capture has only an absolute Deadline.
	defer cancel()
	first := map[string]uint64{}
	var kept []infrarepair.ACK
	for _, ack := range acks {
		floor, seen := first[ack.Stream]
		if !seen {
			stream, err := js.Stream(ctx, ack.Stream)
			switch {
			case errors.Is(err, jetstream.ErrStreamNotFound):
				floor = ^uint64(0)
			case err != nil:
				return infrarepair.Responsibilities{}, err
			default:
				floor = stream.CachedInfo().State.FirstSeq
			}
			first[ack.Stream] = floor
		}
		if ack.Sequence >= floor {
			kept = append(kept, ack)
		}
	}
	return infrarepair.Responsibilities{ACKs: kept}, nil
}

// beforeBusiness is P17's Recovery step: after auth_usable, before the auth
// owner's business NC and before any broker business dial. Every generation
// re-reads the sealed original prefix; a confirmed loss keeps admission shut.
// origin: hot-update S9 plan §4.3 / T40 / T49; rv-business F2
func (o *Broker) beforeBusiness(d c.Deadline, stop c.Stop) error {
	o.mu.Lock()
	cp, gate := o.pending, o.gate
	o.mu.Unlock()
	if cp == (c.Checkpoint{}) || gate == nil {
		return nil
	}
	return gate.VerifyBeforeReplay(cp, d, stop)
}

// Recovery is the c.Recovery the NATS driver seals and releases. It remembers
// the sealed checkpoint so ordinary admission verifies the same prefix.
func (o *Broker) Recovery() c.Recovery {
	if gate, _ := o.recovery(); gate != nil {
		return brokerRecovery{o, gate}
	}
	return nil
}

type brokerRecovery struct {
	o    *Broker
	gate *infrarepair.Gate
}

func (r brokerRecovery) Seal(p c.Plan, d c.Deadline) (c.Checkpoint, error) {
	cp, err := r.gate.Seal(p, d)
	if errors.Is(err, infrarepair.ErrUnknown) {
		// The failed join kept the gate closed, so every unknown request was sent
		// before it. One shared-transport retirement covers them all; the same
		// plan is then sealed again (P18: a retry names the same plan).
		if err = r.o.retireUnknowns(r.gate, d); err == nil {
			cp, err = r.gate.Seal(p, d)
		}
	}
	if err == nil {
		r.o.mu.Lock()
		r.o.pending = cp
		r.o.mu.Unlock()
	}
	return cp, err
}

func (r brokerRecovery) VerifyBeforeReplay(cp c.Checkpoint, d c.Deadline, stop c.Stop) error {
	return r.gate.VerifyBeforeReplay(cp, d, stop)
}

func (r brokerRecovery) Release(cp c.Checkpoint, d c.Deadline) error {
	err := r.gate.Release(cp, d)
	if err == nil {
		r.o.mu.Lock()
		if r.o.pending == cp {
			r.o.pending = c.Checkpoint{}
		}
		r.o.mu.Unlock()
	}
	return err
}

// BindJetStream routes every broker JetStream producer through P18's permits.
// A cold deployment without a gate keeps the unwrapped client. It accepts
// jetstream.New's result pair so a constructor error is never bound.
func (o *Broker) BindJetStream(js jetstream.JetStream, err error) (jetstream.JetStream, error) {
	if gate, _ := o.recovery(); err == nil && gate != nil && js != nil {
		return infrarepair.BindLegacy(js, gate, "$G")
	}
	return js, err
}

// PublishAudit is the broker's single JetStream audit/event publish producer.
// It keeps the explicit-header PublishMsg form (external review M-3), takes
// the AuditPublish permit and records the original ACK before returning.
// origin: hot-update S9 plan §4.3 / T40 / T49
func PublishAudit(o *Broker, js jetstream.JetStream, msg *nats.Msg, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout) // ctx-none: broker audit producers run in NATS callbacks and ledger scans without a caller context.
	defer cancel()
	return infrarepair.RunLegacy(ctx, js, infrarepair.AuditPublish, func(ctx context.Context) error {
		ack, err := js.PublishMsg(ctx, msg)
		if err != nil || o.Recovery() == nil {
			return err
		}
		return o.acknowledged(infrarepair.ACK{Account: "$G", Stream: ack.Stream, Subject: msg.Subject, Sequence: ack.Sequence,
			Message: infrarepair.MessageDigest(msg.Header, msg.Data)})
	})
}

// DeleteObject is the broker's per-transfer object delete (the body of
// broker.deleteXferObject, moved verbatim): it removes one transfer's object
// from the per-session bucket. Missing-object is OK (idempotent). The bucket
// itself survives — it's per-session, not per-transfer. The whole lookup and
// delete hold one ObjectDelete permit, so a sealed recovery refuses it before
// the store is touched; the GC reaper retries a refused terminal cleanup.
// origin: hot-update S9 plan §3.2 / §4.3 / T49 (every broker JS producer)
func DeleteObject(ctx context.Context, js jetstream.JetStream, bucket, name string) error {
	return infrarepair.RunLegacy(ctx, js, infrarepair.ObjectDelete, func(ctx context.Context) error {
		store, err := js.ObjectStore(ctx, bucket)
		if err != nil {
			if errors.Is(err, jetstream.ErrBucketNotFound) ||
				errors.Is(err, jetstream.ErrStreamNotFound) {
				return nil
			}
			return err
		}
		if err := store.Delete(ctx, name); err != nil &&
			!errors.Is(err, jetstream.ErrObjectNotFound) {
			return err
		}
		return nil
	})
}

// ImageExclusive refuses an S6 image cut while this domain has a sealed
// recovery or a pending S9 transaction. No live S9 owner enters an image.
// origin: hot-update S9 plan §7.2 / M24 (image and infra overlap)
func ImageExclusive(o *Broker) error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	sealed := o.pending != (c.Checkpoint{})
	o.mu.Unlock()
	if sealed || servicePending(o) || (o.cfg.Infra != nil && o.cfg.Infra.Pending != nil && o.cfg.Infra.Pending()) {
		return ErrInfraPending
	}
	return nil
}

// ExclusiveRole prefixes an S6 cut-role sample with ImageExclusive, so the
// preparation, final pre-close and exec-latch samples all refuse an overlap.
func ExclusiveRole(o *Broker, role func(bool) error) func(bool) error {
	return func(stopped bool) error {
		if err := ImageExclusive(o); err != nil {
			return err
		}
		return role(stopped)
	}
}
