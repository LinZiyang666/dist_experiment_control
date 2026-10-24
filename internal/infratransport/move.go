package infratransport

import (
	"context"
	"net"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// InterruptRaw forces synchronous socket readers/writers to return. The caller
// owns the registry fence and joins workers after this operation, never before.
func InterruptRaw(conns []net.Conn) int {
	for _, c := range conns {
		_ = c.SetDeadline(time.Unix(1, 0))
		_ = c.Close()
	}
	return len(conns)
}

// RecoveryIntentName keeps legacy diagnostics beside raw retirement policy.
func RecoveryIntentName(shutdown bool) string {
	if shutdown {
		return "shutdown"
	}
	return "rebuild"
}

func (t *Transport) interrupt() {
	t.mu.Lock()
	t.stopped = true
	t.cancel()
	conns := make([]*Connection, 0, len(t.conns))
	for c := range t.conns {
		conns = append(conns, c)
	}
	t.mu.Unlock()
	// origin: hot-update S9 plan §4.5 / T32–T33 — break raw I/O BEFORE any nc.mu-taking operation.
	for _, c := range conns {
		_ = c.Conn.SetDeadline(time.Unix(1, 0))
		_ = c.Close()
	}
}

// Interrupt retires this raw generation and joins every owned dial/CONNECT
// worker. No timeout goroutine, business cleanup or self-exec is involved.
func (t *Transport) Interrupt() {
	t.interrupt()
	t.workers.Wait()
	<-t.watchDone
}

// Close is the only SDK operation needed by retirement; passing the entire
// connection would expose its public configuration and caller context hooks.
// origin: S7 internal review INTEG-M F4
// Move retires only the old NC. The cohort owner creates and authenticates its
// successor under the SAME deadline, then proves the new physical attachment.
// A failed/expired join never becomes success; even then all workers are joined.
func Move(t *Transport, nc interface{ Close() }, deadline infracontract.Deadline, stop infracontract.Stop) error {
	if t == nil {
		return infracontract.ErrUnsupported
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return err
	}
	started := time.Now()
	if remaining > upgradebudget.RequestLatency {
		remaining = upgradebudget.RequestLatency
	}
	t.Interrupt()
	if nc != nil {
		nc.Close()
	}
	if time.Since(started) >= remaining {
		return context.DeadlineExceeded
	}
	select {
	case <-stop:
		return context.Canceled
	default:
		return nil
	}
}
