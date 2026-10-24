package xferworker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfersend"
	"github.com/nats-io/nats.go"
)

type reconnectingSenderWriter struct {
	*rateWriter
	first    chan time.Duration
	original []byte
	index    uint32
	attempts int
	changed  bool
}

func (w *reconnectingSenderWriter) Append(index uint32, data []byte, d x.Deadline) (x.ChunkReceipt, error) {
	w.attempts++
	if w.attempts == 1 {
		w.original, w.index = bytes.Clone(data), index
		remaining, _ := controlbudget.Remaining(d)
		w.first <- remaining
		return x.ChunkReceipt{}, nats.ErrDisconnected
	}
	w.changed = w.changed || index != w.index || !bytes.Equal(data, w.original)
	return w.rateWriter.Append(index, data, d)
}

// Real: the sender's pinned source, sealed budget and pending chunk, driven by
// the registry runner. Injected: one disconnected writer attempt without ACK.
// origin: S7 internal review PULL-B F9 / hot-update S7 plan §1.2 / §2.2 / §4 / T46
func TestTransferWorkerPullSenderRetriesTransportFailure(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePullSenderTransportRecovery(t)
	}
	testharness.AssertNoGoroutineLeak(t, "pull sender transport recovery", g)
	testharness.AssertNoFDLeak(t, "pull sender transport recovery", fd, 0)
}

func exercisePullSenderTransportRecovery(t *testing.T) {
	t.Helper()
	f := newWorkerFixture()
	f.utc = time.Now().UTC()
	p := f.prepare("b", "reconnect-sender")
	p.Intent.Verb, p.Intent.Size = "pull", 32<<10
	body := bytes.Repeat([]byte("s"), int(p.Intent.Size))
	sum := sha256.Sum256(body)
	p.Intent.SHA256 = hex.EncodeToString(sum[:])
	p.Budget.Size, p.Budget.EffectiveBudgetNS, p.Budget.RemainingNS = p.Intent.Size, int64(300*time.Second), int64(300*time.Second)
	source, err := os.CreateTemp(t.TempDir(), "pinned-source")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	if _, err := source.Write(body); err != nil {
		t.Fatal(err)
	}
	writer := &reconnectingSenderWriter{first: make(chan time.Duration, 1), rateWriter: &rateWriter{appended: make(chan struct{}), spec: x.ObjectSpec{Identity: p.Identity, Size: p.Intent.Size, ChunkSize: p.Intent.ChunkSize, Chunks: 1, SHA256: p.Intent.SHA256}}}
	f.cfg.MakeSender = func(cfg x.SendConfig) (x.Sender, error) {
		cfg.Source, cfg.Writer, cfg.Transport = source, writer, rateTransport{p.Budget}
		return xfersend.New(cfg)
	}
	o := newOwner(t, f)
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	original := o.entries[key(p.Identity)].byteDeadline
	if err := o.Start(p.Identity); err != nil {
		t.Fatal(err)
	}
	select {
	case remaining := <-writer.first:
		if remaining <= 0 || remaining > 5*time.Second {
			t.Fatalf("one sender receipt attempt consumed the whole byte budget: %s", remaining)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original sender did not reach its first writer attempt")
	}
	await(t, writer.appended)
	if err := o.Stop(deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if writer.attempts != 2 || writer.changed || o.entries[key(p.Identity)].byteDeadline != original {
		t.Fatal("transport recovery changed the pending bytes, source cursor or original deadline")
	}
}
