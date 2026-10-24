package infrarepair

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S9 plan §3.2 / T41 / M20
func TestLegacyMutationScopeJoinsChildrenAndRejectsEscapedPermit(t *testing.T) {
	f := newRecovery(t)
	defer f.close()
	p := Producer{Name: StreamEnsure, Capture: func(c.Deadline) (Responsibilities, error) { return Responsibilities{}, nil }, Check: func(Responsibilities, c.Deadline, c.Stop) error { return nil }}
	if err := f.gate.Register(p); err != nil {
		t.Fatal(err)
	}
	bound, err := BindLegacy(f.js, f.gate, "business")
	if err != nil {
		t.Fatal(err)
	}
	ctx, finish, err := BeginLegacy(t.Context(), bound, "transfer")
	if err != nil {
		t.Fatal(err)
	}
	if err = RunLegacy(ctx, bound, "unregistered", func(context.Context) error { t.Fatal("unregistered child ran"); return nil }); err == nil {
		t.Fatal("missing nested producer accepted")
	}
	deadline := recoveryDeadline(t)
	type result struct {
		cp  c.Checkpoint
		err error
	}
	done := make(chan result, 1)
	go func() { cp, err := f.gate.Seal(f.plan, deadline); done <- result{cp, err} }()
	awaitFence(t, f.gate)
	// A parent admitted before Seal must finish its already-owned child work;
	// rejecting this child would strand the parent half way through its mutation.
	if err = RunLegacy(ctx, bound, StreamEnsure, func(ctx context.Context) error {
		_, err := bound.CreateStream(ctx, jetstream.StreamConfig{Name: "admitted-child", Subjects: []string{"admitted-child"}, Storage: jetstream.FileStorage})
		return err
	}); err != nil {
		finish(err)
		<-done
		t.Fatal(err)
	}
	finish(nil)
	sealed := <-done
	if sealed.err != nil {
		t.Fatal(sealed.err)
	}
	if err = RunLegacy(ctx, bound, StreamEnsure, func(context.Context) error { t.Error("escaped permit ran"); return nil }); !errors.Is(err, ErrFenced) {
		t.Fatalf("escaped context admitted: %v", err)
	}
	if err = f.gate.VerifyBeforeReplay(sealed.cp, recoveryDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	if err = f.gate.Release(sealed.cp, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §4.3 / T40
func TestLegacyPublishRequiresOriginalACKOwner(t *testing.T) {
	f := newRecovery(t)
	defer f.close()
	if err := f.gate.Register(Producer{Name: AuditPublish, Capture: func(c.Deadline) (Responsibilities, error) { return Responsibilities{}, nil }, Check: func(Responsibilities, c.Deadline, c.Stop) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	bound, err := BindLegacy(f.js, f.gate, "business")
	if err != nil {
		t.Fatal(err)
	}
	stream, err := f.js.Stream(t.Context(), "audit")
	if err != nil {
		t.Fatal(err)
	}
	before, err := stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = PublishLegacy(t.Context(), bound, "audit.untracked", []byte("must not publish"), "untracked"); !errors.Is(err, c.ErrIncomplete) {
		t.Fatalf("missing original ACK owner: %v", err)
	}
	after, err := stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if before.State.LastSeq != after.State.LastSeq {
		t.Fatal("missing ACK owner published before rejecting")
	}
}

// origin: hot-update S9 plan §3.2 / T41
func TestRecoveryOperationQueueConsumesCallerDeadline(t *testing.T) {
	f := newRecovery(t)
	defer f.close()
	if err := f.gate.beginOperation(recoveryDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	defer func() {
		select {
		case f.gate.operation <- struct{}{}:
		default:
		}
	}()
	d, err := controlbudget.After(10 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.gate.ReadSeal(f.plan, d, nil); !controlbudget.IsExpired(err) {
		t.Fatalf("queued read ignored caller deadline: %v", err)
	}
	if len(f.gate.operation) != 0 {
		t.Fatal("expired waiter released an operation it never acquired")
	}
}
