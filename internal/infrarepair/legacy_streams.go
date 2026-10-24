package infrarepair

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// These are actual emitter roots, not a claim that all broker/S7 producers are
// wired. P22 supplies the complete required set; P19 adds S7 owners before Seal.
const (
	StreamEnsure     = "stream-ensure"
	StreamDelete     = "stream-delete"
	ObjectDelete     = "object-delete"
	PlacementProbe   = "placement-probe"
	AuditPublish     = "audit-publish"
	AuditReplay      = "audit-replay"
	ReplicaReconcile = "replica-reconcile"
)

func LegacyRoots() []string {
	return []string{StreamEnsure, StreamDelete, ObjectDelete, PlacementProbe, AuditPublish, AuditReplay, ReplicaReconcile}
}

type legacyClient struct {
	account string
	jetstream.JetStream
	gate *Gate
}

// BindLegacy preserves the existing cold-path API. It binds the management
// helpers and audit entry points below to the same domain as the S7 owners.
// The raw JS interface remains for existing readers; callers emitting directly
// must take a Permit (P19/P22). Wrapping alone does not certify their coverage.
func BindLegacy(js jetstream.JetStream, gate *Gate, account string) (jetstream.JetStream, error) {
	if js == nil || gate == nil || gate.cfg.Accounts[account] == nil {
		return nil, c.ErrIncomplete
	}
	if _, ok := js.(*legacyClient); ok {
		return nil, c.ErrIncomplete
	}
	return &legacyClient{JetStream: js, gate: gate, account: account}, nil
}

type permitKey struct{ gate *Gate }

// BeginLegacy carries an admitted parent through nested ensure/reconcile calls.
// Children still need a registered root; otherwise a missed producer hidden
// inside an admitted audit pass would evade the required-set check.
// origin: hot-update S9 plan §3.2 / §4.3 / T40 / T41
func BeginLegacy(ctx context.Context, js jetstream.JetStream, name string) (context.Context, func(error), error) {
	bound, ok := js.(*legacyClient)
	if !ok {
		return ctx, func(error) {}, nil
	} // Ordinary cold deployment has no infra transaction.
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	g := bound.gate
	if parent, ok := ctx.Value(permitKey{g}).(*Permit); ok {
		g.mu.Lock()
		if parent.finished || g.producers[name].Capture == nil {
			g.mu.Unlock()
			return nil, nil, ErrFenced
		}
		// Count children independently: even an incorrectly early parent Finish
		// cannot let Seal overtake an already-admitted child's actual request.
		g.active++
		if g.producers[name].AssetWriter {
			g.revision++
		}
		child := &Permit{gate: g, name: name}
		g.mu.Unlock()
		return context.WithValue(ctx, permitKey{g}, child), child.Finish, nil
	}

	permit, err := g.Enter(name)
	if err != nil {
		return nil, nil, err
	}
	return context.WithValue(ctx, permitKey{g}, permit), permit.Finish, nil
}

// RunLegacy joins the complete synchronous operation before releasing its
// permit. An unknown result poisons the generation until transport retirement.
func RunLegacy(ctx context.Context, js jetstream.JetStream, name string, work func(context.Context) error) error {
	_, err := RunLegacyValue(ctx, js, name, func(ctx context.Context) (struct{}, error) { return struct{}{}, work(ctx) })
	return err
}

func RunLegacyValue[T any](ctx context.Context, js jetstream.JetStream, name string, work func(context.Context) (T, error)) (result T, err error) {
	ctx, finish, err := BeginLegacy(ctx, js, name)
	if err != nil {
		return result, err
	}
	// A panic is also unproven retirement. Preserve the fence while unwinding.
	retired := false
	defer func() {
		if !retired {
			finish(ErrUnknown)
		} else {
			finish(err)
		}
	}()
	result, err = work(ctx)
	retired = true
	return result, err
}

// ParallelSessions bounds and joins replica workers, including after the first
// hard failure. It never leaves children using a permit after its parent exits.
func ParallelSessions(ctx context.Context, sids []string, parallel int, visit func(context.Context, string) error) error {
	if parallel < 1 {
		return c.ErrIncomplete
	}
	var mu sync.Mutex
	var first error
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for _, sid := range sids {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := visit(ctx, sid); err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return first
}

// PublishLegacy hands the original ACK to the durable owner before returning
// success. Reading it later during Seal could turn pre-seal loss into evidence.
// origin: hot-update S9 plan §4.3 / T40
func PublishLegacy(ctx context.Context, js jetstream.JetStream, subject string, payload []byte, msgID string) error {
	return RunLegacy(ctx, js, AuditPublish, func(ctx context.Context) error {
		var remember func(ACK) error
		bound, managed := js.(*legacyClient)
		if managed {
			bound.gate.mu.Lock()
			remember = bound.gate.producers[AuditPublish].Acknowledged
			bound.gate.mu.Unlock()
			if remember == nil {
				return c.ErrIncomplete
			}
		}
		ack, err := js.Publish(ctx, subject, payload, jetstream.WithMsgID(msgID))
		if err != nil {
			return fmt.Errorf("d5: publish %s id=%s: %w", subject, msgID, err)
		}
		if managed {
			header := nats.Header{"Nats-Msg-Id": []string{msgID}}
			if msgID == "" {
				header = nil
			}
			return remember(ACK{Account: bound.account, Stream: ack.Stream, Subject: subject, Sequence: ack.Sequence, Message: MessageDigest(header, payload)})
		}
		return nil
	})
}

// DeleteLegacyObjectStore joins the complete session-rm bucket deletion.
// origin: hot-update S9 plan §3.2 / §4.3 / T40 / T41
func DeleteLegacyObjectStore(ctx context.Context, js jetstream.JetStream, bucket, sid string) error {
	return RunLegacy(ctx, js, ObjectDelete, func(ctx context.Context) error {
		err := js.DeleteObjectStore(ctx, bucket)
		if err == nil {
			return nil
		}
		// DeleteObjectStore joins ErrBucketNotFound with the underlying ErrStreamNotFound; accept
		// either as "already gone".
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil
		}
		return fmt.Errorf("jsstream: delete xfer bucket %s: %w", sid, err)
	})
}

// ErrMetaGroupNotReady is a retired API rejection, not a sent-unknown mutation.
var ErrMetaGroupNotReady = errors.New("jsstream: JS meta-group cannot yet host target replicas (retriable)")

// EnsureLegacyStream is the create-OR-RAISE helper (D5 §6.4 / R-18). On first call it
// creates the stream at cfg.Replicas. When the stream already exists it RECONCILES the
// replica factor toward cfg.Replicas — RAISE-ONLY: it never shrinks (shrink is the D7
// retire path, gated on AllAtTarget). Retention/limits are NOT auto-mutated (operators
// pin those via `nats stream edit`); only Replicas is reconciled, because the HA
// replica factor is owned by the cluster (replicasFor(nVoters)), not the operator.
// origin: hot-update S9 plan §3.2 / §4.3 / T40 / T41
func EnsureLegacyStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig) error {
	return RunLegacy(ctx, js, StreamEnsure, func(ctx context.Context) error {
		_, err := js.CreateStream(ctx, cfg)
		if err == nil {
			return nil
		}
		if !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return fmt.Errorf("jsstream: create %s: %w", cfg.Name, err)
		}
		return reconcileReplicas(ctx, js, cfg)

	})
}

// reconcileReplicas raises an existing stream's replica factor toward cfg.Replicas
// (D5 §6.4 / R-17/R-18). It compares the stream's CONFIGURED replicas (the current
// target) to cfg.Replicas: at-or-above => no-op (raise-only, never shrink). Below =>
// UpdateStream toward the target, with the meta-group readiness GATE being the
// UpdateStream rejection itself (classified to ErrMetaGroupNotReady) — an R1 stream's
// StreamInfo.Cluster cannot reveal the meta-group size, so a Cluster-based pre-check
// would falsely block the expansion (R-17).
func reconcileReplicas(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig) error {
	s, err := js.Stream(ctx, cfg.Name)
	if err != nil {
		return fmt.Errorf("jsstream: lookup %s: %w", cfg.Name, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		return fmt.Errorf("jsstream: info %s: %w", cfg.Name, err)
	}
	current := info.Config.Replicas
	if current < 1 {
		current = 1 // JS normalizes 0 -> 1 at create; defensive
	}
	if current >= cfg.Replicas {
		return nil // raise-only (R-18): already at/above target
	}
	// audit jsstream F2: raise ONLY the replica factor — start from the LIVE config and change
	// just Replicas, so an operator's `nats stream edit` limits (MaxBytes/retention/…) survive.
	// UpdateStream(cfg) with the full tether-default cfg would silently clobber those edits,
	// contradicting this function's "only Replicas is reconciled" contract (the HA replica factor
	// is cluster-owned; retention/limits are operator-owned).
	target := info.Config
	target.Replicas = cfg.Replicas
	if _, err := js.UpdateStream(ctx, target); err != nil {
		if IsMetaGroupNotReady(err) {
			return ErrMetaGroupNotReady // R-17: retriable; meta-group not ready
		}
		return fmt.Errorf("jsstream: update %s replicas->%d: %w", cfg.Name, cfg.Replicas, err)
	}
	return nil
}

// IsMetaGroupNotReady classifies a JetStream UpdateStream/UpdateObjectStore error as a
// TRANSIENT "the meta-group cannot host this replica count YET" condition (R-17) — i.e.
// retry next tick. It must NOT match PERMANENT misconfigurations that merely mention
// peers/replicas: JS code 10074 "replicas > 1 not supported in non-clustered mode" means
// the node's JetStream never formed a cluster, so retrying spins the reconcile loop
// forever masked by Degraded() (internal review M1). A bare "peers" substring is likewise
// too broad (e.g. "lost connection to peers" is a real failure). Any non-matching error
// propagates as a hard failure (E-B3 pins the live classification). Exported so the
// broker object-store reconcile (transfer.go) shares the one classifier.
func IsMetaGroupNotReady(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	// Permanent dead-ends that mention replicas/peers but are NOT retriable. Checked BEFORE the
	// transient signals so an overlapping substring resolves to permanent. "insufficient storage"
	// (a disk-full peer; JS "insufficient storage resources available") is PERMANENT — retrying
	// the same placement spins the reconcile loop forever, masked by Degraded() (audit jsstream
	// F3); it must propagate as a hard failure so the operator sees it, distinct from the
	// transient "insufficient resources"/"no suitable peer" placement-capacity signals below.
	for _, perm := range []string{"non-clustered", "not supported", "insufficient storage"} {
		if strings.Contains(msg, perm) {
			return false
		}
	}
	// Genuinely transient placement-capacity signals (a newly-joined voter not yet in the
	// JS meta-group, or resources momentarily unavailable).
	for _, s := range []string{
		"no suitable peer",
		"no peers",
		"insufficient",
		"not enough",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
