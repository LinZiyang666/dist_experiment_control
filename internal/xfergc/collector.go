package xfergc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// A collector owns enumeration, never retirement. The finalizer rechecks the
// nonce/revision and obtains writer/consumer/fence receipts before any purge.
// origin: hot-update S7 plan §5.3 / P17
type collector struct {
	cfg     Config
	boot    string
	born    time.Time
	root    context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	stopped bool
	active  chan struct{}
}

var _ Factory = New

// New creates an idle owner. Its context belongs to Stop, rather than a broker
// connection or an individual reconcile request; an expired pass is reusable.
func New(cfg Config) (Collector, error) {
	if cfg.Clock == nil || cfg.UTC == nil || cfg.JS == nil || cfg.Owners == nil || cfg.Finalizer == nil {
		return nil, errors.New("xfergc: incomplete configuration")
	}
	boot, now, err := cfg.Clock()
	if err != nil {
		return nil, err
	}
	if boot == "" || now <= 0 {
		return nil, controlbudget.ErrInvalidDeadline
	}
	root, cancel := context.WithCancel(context.Background()) // ctx-root: collector lifetime, explicitly canceled by Stop.
	return &collector{cfg: cfg, boot: boot, born: cfg.UTC(), root: root, cancel: cancel}, nil
}

// NewCollector preserves the constructor spelling used by P00's compile
// witness. Both names have the same frozen Factory signature.
func NewCollector(cfg Config) (Collector, error) { return New(cfg) }

// Pass serializes enumeration and carries one absolute budget through local
// census, remote proofs, both object lists, and finalizer scheduling. Empty or
// incomplete census evidence must never become permission to delete.
// origin: hot-update S7 plan §5.3 / T29 / T30
func (c *collector) Pass(deadline xfercontract.Deadline) (xfercontract.LedgerCensus, error) {
	d, err := c.boundedDeadline(deadline)
	if err != nil {
		return xfercontract.LedgerCensus{}, err
	}
	c.mu.Lock()
	if c.stopped || c.active != nil {
		c.mu.Unlock()
		return xfercontract.LedgerCensus{Code: xfercontract.CodeOwnerRecovering}, errors.New("xfergc: collector stopped or pass already active")
	}
	done := make(chan struct{})
	c.active = done
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.active = nil
		close(done)
		c.mu.Unlock()
	}()
	ctx, cancel, err := c.operationContext(c.root, d)
	if err != nil {
		return xfercontract.LedgerCensus{}, err
	}
	defer cancel()
	// The production adapter owns the same journal as Pin/Checkpoint. This
	// synchronous bounded pass stays inside active admission, including when
	// an unrelated remote census prevents object collection later in the pass.
	// origin: hot-update S7 plan §5.3 / §8.2 / FIX-TERM request 4
	var checkpointErr error
	if journal, ok := c.cfg.Owners.(interface {
		CollectRetirementCheckpoints(xfercontract.Deadline) error
	}); ok {
		checkpointErr = journal.CollectRetirementCheckpoints(d)
	}

	// The outer adapter supplies the installed (including sleeping) tracker
	// plus both durable directories for the zero-origin local request. It must
	// include known remote origins and mark directory/census failures incomplete.
	census, err := c.cfg.Owners.Census(xfercontract.Origin{}, d)
	if err != nil || !census.Ready || !census.Complete || census.Code != "" {
		census.Complete = false
		census.Code = xfercontract.CodeResultUnknown
		return census, errors.Join(checkpointErr, err, errors.New("xfergc: local owner census incomplete"))
	}
	view, evidenceErr := c.resolveOwners(ctx, census, d)
	if evidenceErr != nil {
		census.Complete = false
		census.Code = xfercontract.CodeResultUnknown
	}

	// Live protection is not a replay filter. In particular cleanup-only and
	// history-pending rows keep progressing while protecting both directories.
	advanceErr := c.advanceRows(ctx, view, d)
	streams := c.cfg.JS.ListStreams(ctx)
	var scanErr error
	for info := range streams.Info() {
		if !isTransferStream(info.Config.Name) {
			continue
		}
		store, openErr := c.cfg.JS.ObjectStore(ctx, transferBucket(info.Config.Name))
		if openErr != nil {
			scanErr = errors.Join(scanErr, openErr)
			continue
		}
		// These are the two former reaper List responsibilities: candidate
		// enumeration and the post-reap aged-object observation. Neither may
		// hold the reconcile owner forever on an expired, non-pristine stream.
		objects, listErr := c.collectBucketObjects(ctx, store, d)
		if listErr != nil {
			scanErr = errors.Join(scanErr, listErr)
			continue
		}
		scanErr = errors.Join(scanErr, c.advanceOrphans(ctx, info.Config.Name, objects, view, census.Complete, d))
		scanErr = errors.Join(scanErr, c.inspectUnreferencedChunks(ctx, info.Config.Name, objects, view))
		scanErr = errors.Join(scanErr, c.observeAgedObjects(ctx, store, d))
	}
	resultErr := errors.Join(checkpointErr, evidenceErr, advanceErr, scanErr, streams.Err(), ctx.Err())
	if resultErr != nil {
		census.Complete = false
		census.Code = xfercontract.CodeResultUnknown
	}
	return census, resultErr
}

// Stop closes admission before canceling the active pass, then joins it. The
// collector does not Stop the shared finalizer or destroy retained evidence.
// origin: hot-update S7 plan §8.2 / T30
func (c *collector) Stop(deadline xfercontract.Deadline) error {
	c.mu.Lock()
	c.stopped = true
	c.cancel()
	done := c.active
	c.mu.Unlock()
	if done == nil {
		return nil
	}
	duration, err := c.remaining(deadline)
	if err != nil {
		return err
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}

func (c *collector) remaining(deadline xfercontract.Deadline) (time.Duration, error) {
	if deadline <= 0 {
		return 0, controlbudget.ErrInvalidDeadline
	}
	boot, now, err := c.cfg.Clock()
	if err != nil {
		return 0, err
	}
	if boot != c.boot || now <= 0 {
		return 0, errors.New("xfergc: operation clock identity changed")
	}
	if int64(deadline) <= now {
		return 0, controlbudget.ErrExpired
	}
	return time.Duration(int64(deadline) - now), nil
}

func (c *collector) boundedDeadline(deadline xfercontract.Deadline) (xfercontract.Deadline, error) {
	remaining, err := c.remaining(deadline)
	if err != nil {
		return 0, err
	}
	if remaining > 5*time.Second {
		return deadline - xfercontract.Deadline(remaining-5*time.Second), nil
	}
	return deadline, nil
}

func (c *collector) operationContext(parent context.Context, deadline xfercontract.Deadline) (context.Context, context.CancelFunc, error) {
	duration, err := c.remaining(deadline)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(parent, duration)
	return ctx, cancel, nil
}

// List runs synchronously: returning from it includes the SDK watcher.Stop,
// and returning from Pass includes this join before releasing single-flight.
// No detached timeout goroutine can keep an old reconcile pass alive.
// origin: hot-update S7 plan lead LQ3 / §3 / T30
func (c *collector) collectBucketObjects(parent context.Context, store jetstream.ObjectStore, deadline xfercontract.Deadline) ([]*jetstream.ObjectInfo, error) {
	ctx, cancel, err := c.operationContext(parent, deadline)
	if err != nil {
		return nil, err
	}
	defer cancel()
	objects, err := store.List(ctx, jetstream.ListObjectsShowDeleted())
	if errors.Is(err, jetstream.ErrNoObjectsFound) {
		return nil, nil
	}
	return objects, err
}

// This separate list keeps the former unreapable-object observation bounded
// too. A list failure is unknown, rather than an invented empty observation.
// The collector has no consumer for an aged-object verdict (the unreapable
// gauge still belongs to the broker's legacy reaper), so only the bounded List
// and its failure evidence remain; a future gauge must classify what it lists.
// origin: hot-update S7 plan lead LQ3 / §5.3 / T30
func (c *collector) observeAgedObjects(parent context.Context, store jetstream.ObjectStore, deadline xfercontract.Deadline) error {
	ctx, cancel, err := c.operationContext(parent, deadline)
	if err != nil {
		return err
	}
	defer cancel()
	_, err = store.List(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoObjectsFound) {
		return fmt.Errorf("xfergc: aged object census: %w", err)
	}
	return nil
}
