package xferclient

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §7 / §9 / T33
func TestTransferLegacyInvocationIsOneShotAfterUnknown(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		calls := 0
		invocation, err := NewLegacy(LegacyConfig{TransferID: "original", PutOnce: func(x.Deadline) (x.LegacyResult, error) {
			calls++
			if unknown {
				return x.LegacyResult{}, errors.New("sent without a result")
			}
			return x.LegacyResult{Kind: "complete", Bytes: 12, SHA256: digestOf([]byte("legacy bytes"))}, nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		// Separate interfaces prevent a cap0 result from reaching the capable
		// engine's success sink, even through an interface/type-alias assignment.
		if _, capable := any(invocation).(Invocation); capable {
			t.Fatal("legacy implements the capable receipt interface")
		}
		var wg sync.WaitGroup
		for range 12 {
			wg.Go(func() {
				result, err := invocation.Run(testDeadline(t, time.Second))
				if !result.BestEffort {
					t.Error("legacy result hid its best-effort scope")
				}
				if unknown && (err == nil || ExitCode(err) != 74) {
					t.Errorf("legacy sent-unknown claimed success: %v", err)
				}
				if !unknown && (err != nil || result.Kind != "complete") {
					t.Errorf("one-shot completion lost its result: %v", err)
				}
			})
		}
		wg.Wait()
		if calls != 1 {
			t.Fatalf("legacy operation retried SDK Put: calls=%d", calls)
		}
		if err := invocation.Stop(testDeadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
	}
}

// origin: hot-update S7 plan §7 / §11.3 / T33
func TestTransferLegacyStopHasBoundedJoin(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	invocation, err := NewLegacy(LegacyConfig{TransferID: "original", PutOnce: func(x.Deadline) (x.LegacyResult, error) {
		close(entered)
		<-release
		return x.LegacyResult{Kind: "complete"}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	joined := make(chan struct{})
	go func() { defer close(joined); _, _ = invocation.Run(testDeadline(t, time.Second)) }()
	<-entered
	start := time.Now()
	err = invocation.Stop(testDeadline(t, 30*time.Millisecond))
	close(release)
	<-joined
	if !errors.Is(err, controlbudget.ErrExpired) || time.Since(start) > 250*time.Millisecond {
		t.Fatalf("Stop did not honor its deadline: %v", err)
	}
	if err := invocation.Stop(testDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §7 / §9 / T33
func TestTransferLegacyFailedResultCannotExitZero(t *testing.T) {
	invocation, err := NewLegacy(LegacyConfig{TransferID: "original", PutOnce: func(x.Deadline) (x.LegacyResult, error) {
		return x.LegacyResult{Kind: "failed", Code: x.CodeSourceChanged, Error: "source changed"}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, err := invocation.Run(testDeadline(t, time.Second))
		if result.Kind != "failed" || !result.BestEffort || ExitCode(err) != 64 {
			t.Fatalf("legacy failure became success: %+v %v", result, err)
		}
	}
}

// origin: hot-update S7 plan §7 / §9 / T33
func TestTransferLegacyPanicRetainsUnknownWithoutReinvoking(t *testing.T) {
	calls := 0
	invocation, err := NewLegacy(LegacyConfig{TransferID: "original", PutOnce: func(x.Deadline) (x.LegacyResult, error) { calls++; panic("one-shot operation panicked") }})
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic exercise did not enter the one-shot operation")
			}
		}()
		_, _ = invocation.Run(testDeadline(t, time.Second))
	}()
	result, err := invocation.Run(testDeadline(t, time.Second))
	if calls != 1 || !result.BestEffort || ExitCode(err) != 74 {
		t.Fatalf("panic lost one-shot uncertainty: %+v %v calls=%d", result, err, calls)
	}
}

// origin: hot-update S7 plan §7 / §9 / M01
func TestTransferLegacyObjectAdapterCannotFabricateMetadata(t *testing.T) {
	for _, defect := range []string{"valid", "nil", "foreign", "digest", "size"} {
		t.Run(defect, func(t *testing.T) {
			f := newClientFixture(t, "push")
			calls := 0
			hash := sha256.New()
			_, _ = hash.Write(f.data)
			operation := func(_ jetstream.JetStream, bucket, key string, _ io.Reader, _ x.Deadline) (*jetstream.ObjectInfo, error) {
				calls++
				if defect == "nil" {
					return nil, nil
				}
				info := &jetstream.ObjectInfo{ObjectMeta: jetstream.ObjectMeta{Name: key}, Bucket: bucket, NUID: "legacy-version", Size: uint64(len(f.data)), Digest: jetstream.GetObjectDigestValue(hash)}
				if defect == "foreign" {
					info.Name = "another operation"
				}
				if defect == "digest" {
					info.Digest = "invalid"
				}
				if defect == "size" {
					info.Size = proto.XferMaxBytes + 1
				}
				return info, nil
			}
			invocation, err := newLegacyObject(LegacyConfig{TransferID: "original"}, operation, f.js, "bucket", "original", bytes.NewReader(f.data))
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				result, err := invocation.Run(testDeadline(t, time.Second))
				if defect == "valid" {
					if err != nil || !result.BestEffort || result.Kind != "complete" || result.SHA256 != digestOf(f.data) {
						t.Fatal("legacy metadata was not mapped to a best-effort result")
					}
				} else if err == nil || ExitCode(err) != 74 {
					t.Fatal("missing/foreign metadata became a successful legacy result")
				}
			}
			if calls != 1 {
				t.Fatal("legacy adapter repeated its SDK operation")
			}
		})
	}
}
