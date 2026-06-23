package broker

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/schema"
)

func d8AuditRec(kind string) schema.AuditTransfer {
	return schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: kind, Verb: "push", Session: "lab", Node: "lab-1", TransferID: "tid-x", Tier: "b"}
}

// TestD8TransferAuditForwardNonBlocking (review M4 / OQ9-A): emitTransferAudit must NOT block
// the caller (the NATS handler) on the forward — the forward runs in a tracked goroutine.
func TestD8TransferAuditForwardNonBlocking(t *testing.T) {
	b := &Broker{cfg: Config{Logger: silentLogger()}}
	release := make(chan struct{})
	b.attachTransferAuditSinkWith(func([]byte) error {
		<-release // block the forward
		return nil
	})
	start := time.Now()
	b.emitTransferAudit(d8AuditRec("start"))
	if elapsed := time.Since(start); elapsed > 80*time.Millisecond {
		t.Fatalf("emitTransferAudit blocked %v on the forward — must be async", elapsed)
	}
	close(release)
	b.WaitTransferAudit()
}

// TestD8TransferAuditForwardRetry (review M4): a transient ErrForwardNotLeader is retried (the
// derived reqID dedups a double-commit); a permanent error returns at once.
func TestD8TransferAuditForwardRetry(t *testing.T) {
	t.Run("retries-then-ok", func(t *testing.T) {
		b := &Broker{cfg: Config{Logger: silentLogger()}}
		var calls int32
		b.attachTransferAuditSinkWith(func([]byte) error {
			if atomic.AddInt32(&calls, 1) < 3 {
				return cluster.ErrForwardNotLeader
			}
			return nil
		})
		b.emitTransferAudit(d8AuditRec("complete"))
		b.WaitTransferAudit()
		if got := atomic.LoadInt32(&calls); got != 3 {
			t.Fatalf("transient retry: %d forward calls, want 3 (2 not_leader + 1 ok)", got)
		}
	})
	t.Run("permanent-single-shot", func(t *testing.T) {
		b := &Broker{cfg: Config{Logger: silentLogger()}}
		var calls int32
		b.attachTransferAuditSinkWith(func([]byte) error {
			atomic.AddInt32(&calls, 1)
			return errors.New("permanent business error")
		})
		b.emitTransferAudit(d8AuditRec("failed"))
		b.WaitTransferAudit()
		if got := atomic.LoadInt32(&calls); got != 1 {
			t.Fatalf("permanent error: %d forward calls, want 1 (no retry)", got)
		}
	})
}

// TestD8TransferAuditForwardLeakGate (review M4 / CLAUDE.md §5): the async forward goroutine
// must drain — after WaitTransferAudit the goroutine count returns to baseline.
func TestD8TransferAuditForwardLeakGate(t *testing.T) {
	b := &Broker{cfg: Config{Logger: silentLogger()}}
	b.attachTransferAuditSinkWith(func([]byte) error { return nil })
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		b.emitTransferAudit(d8AuditRec("start"))
	}
	b.WaitTransferAudit()
	for i := 0; i < 50; i++ {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("transfer-audit forward goroutines leaked: before=%d after=%d", before, runtime.NumGoroutine())
}
