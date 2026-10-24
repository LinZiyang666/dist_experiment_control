package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/testharness"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Copy into internal/cli. Real embedded NATS and production transport/selector;
// the peer implements the v0.6.0 caps surface, not a v0.6.0 binary.
// origin: S7 adversarial review rv-n1compat
// origin: S7 internal review rv-n1compat F2
func TestTransferOldBrokerNegotiation(t *testing.T) {
	s, nc, id, stop := transferConnectionFixture(t)
	defer stop()
	var capsCalls atomic.Int32
	sub, err := nc.Subscribe(proto.SubjCtrlCaps(id.PublicKey, "lab"), func(m *nats.Msg) {
		capsCalls.Add(1)
		_ = m.Respond([]byte(`{"ok":true,"jetstream_ready":true,"max_payload":1048576,"broker_release":"v0.6.0","broker_proto":2}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	cfg, closeOwner, err := TransferClientConfig(nc, s.ClientURL(), id, "lab")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeOwner(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cfg.Identity.NID, cfg.Identity.TransferID = "node", "original"
	cfg.Intent = x.Intent{Verb: "push", Path: "/tmp/target", Size: 1}
	d, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := SelectTransferClient(&cfg, d)
	if !legacy || err != nil {
		t.Fatalf("v0.6.0 negotiation unavailable: legacy=%v err=%v old_caps_calls=%d", legacy, err, capsCalls.Load())
	}
}

// A v0.6.0 agent ACKs commit before Get/SHA/rename. Only ev.transfer carries
// its eventual success/failure. Use real object upload and an independent event
// witness so the negative result cannot disappear as an unobserved fixture.
// origin: S7 internal review rv-n1compat F1
func TestTransferLegacyPushWaitsForReceiverOutcome(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTransferLegacyPushWaitsForReceiverOutcome(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestTransferLegacyPushWaitsForReceiverOutcome", g)
	testharness.AssertNoFDLeak(t, "TestTransferLegacyPushWaitsForReceiverOutcome", fd, 0)
}
func exerciseTransferLegacyPushWaitsForReceiverOutcome(t *testing.T) {
	s, nc, id, stop := transferConnectionFixture(t)
	defer stop()
	// The merged selector first requires the existing authenticated caps response.
	caps := answerTransferCaps(t, nc, id.PublicKey, "lab", x.Version, "")
	defer func() {
		if err := caps.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cfg, closeOwner, err := TransferClientConfig(nc, s.ClientURL(), id, "lab")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeOwner(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cfg.Identity.NID, cfg.Identity.TransferID = "node", "original"
	cfg.Intent = x.Intent{Verb: "push", Path: "/tmp/target", Size: 600 * 1024}
	path := filepath.Join(t.TempDir(), "source")
	if err = os.WriteFile(path, make([]byte, cfg.Intent.Size), 0600); err != nil {
		t.Fatal(err)
	}
	cfg.Source, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cfg.Source.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if _, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: proto.XferBucketName("lab")}); err != nil {
		t.Fatal(err)
	}
	probe, _ := proto.SubjCtrlTransferRequest(id.PublicKey, "lab", "node", "original", x.OpProbe)
	ps, err := nc.Subscribe(probe, func(m *nats.Msg) {
		b, _ := json.Marshal(proto.TransferProbeResp{InstanceID: "oldagent", MaxPayload: 1 << 20, MaxMsgSize: 1 << 20})
		_ = m.Respond(b)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ps.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	prep, err := nc.Subscribe(proto.SubjCmdBy("lab", id.PublicKey, "node", "push"), func(m *nats.Msg) { _ = m.Respond([]byte(`{"ok":true,"tier":"b"}`)) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := prep.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	evSubject := proto.SubjEvTransfer("lab", "node", "original", "failed")
	witness, err := nc.SubscribeSync(evSubject)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := witness.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	commit, err := nc.Subscribe(proto.SubjCmdBy("lab", id.PublicKey, "node", "push-commit"), func(m *nats.Msg) {
		_ = m.Respond([]byte(`{"ok":true}`))
		b, _ := json.Marshal(proto.TransferEvent{Kind: "failed", Verb: "push", TransferID: "original", Tier: "b", Code: "dst_exists", Error: "receiver could not commit destination"})
		_ = nc.Publish(evSubject, b)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := commit.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err = nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	d, _ := controlbudget.After(10 * time.Second)
	legacy, err := SelectTransferClient(&cfg, d)
	if err != nil || !legacy {
		t.Fatalf("cap0 fixture: %v %v", legacy, err)
	}
	owner, err := NewLegacyTransfer(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := StopLegacyTransfer(owner); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	result, runErr := owner.Run(d)
	event, err := witness.NextMsg(time.Second)
	if err != nil {
		t.Fatal("receiver failure was not published", err)
	}
	if runErr == nil || result.Kind == "complete" {
		t.Fatalf("false success: result=%+v exit=%d receiver_event=%s", result, xferclient.ExitCode(runErr), event.Data)
	}
}

// The old prepare reply includes byte IO. It is not the new five-second
// control-only prepare: v0.6.0 waits for the full caller transfer timeout.
// origin: S7 internal review rv-n1compat F5
func TestTransferLegacyReplyBeyondControlWindow(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTransferLegacyReplyBeyondControlWindow(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestTransferLegacyReplyBeyondControlWindow", g)
	testharness.AssertNoFDLeak(t, "TestTransferLegacyReplyBeyondControlWindow", fd, 0)
}
func exerciseTransferLegacyReplyBeyondControlWindow(t *testing.T) {
	s, nc, id, stop := transferConnectionFixture(t)
	defer stop()
	// The merged selector first requires the existing authenticated caps response.
	caps := answerTransferCaps(t, nc, id.PublicKey, "lab", x.Version, "")
	defer func() {
		if err := caps.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cfg, closeOwner, err := TransferClientConfig(nc, s.ClientURL(), id, "lab")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeOwner(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cfg.Identity.NID, cfg.Identity.TransferID = "node", "original"
	cfg.Intent = x.Intent{Verb: "push", Path: "/tmp/target", Size: 1}
	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg.Source, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cfg.Source.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	probe, _ := proto.SubjCtrlTransferRequest(id.PublicKey, "lab", "node", "original", x.OpProbe)
	ps, err := nc.Subscribe(probe, func(m *nats.Msg) {
		b, _ := json.Marshal(proto.TransferProbeResp{InstanceID: "old-agent", MaxPayload: 1 << 20, MaxMsgSize: 1 << 20})
		_ = m.Respond(b)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ps.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	done := make(chan struct{})
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	bs, err := nc.Subscribe(proto.SubjCmdBy("lab", id.PublicKey, "node", "push"), func(m *nats.Msg) {
		calls.Add(1)
		defer close(done)
		// Intentional byte-work delay, larger than the real control window.
		timer := time.NewTimer(5500 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			_ = m.Respond([]byte(`{"ok":true,"tier":"a"}`))
		case <-ctx.Done():
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := bs.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err = nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	d, _ := controlbudget.After(9 * time.Second)
	legacy, err := SelectTransferClient(&cfg, d)
	if err != nil || !legacy {
		t.Fatalf("fixture cap0: %v %v", legacy, err)
	}
	owner, err := NewLegacyTransfer(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := StopLegacyTransfer(owner); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	started := time.Now()
	result, runErr := owner.Run(d)
	elapsed := time.Since(started)
	if calls.Load() != 1 {
		t.Fatalf("legacy request repeated: %d", calls.Load())
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("peer did not join")
	}
	if runErr != nil || result.Kind != "complete" {
		t.Fatalf("legacy byte work cut at %s despite 9s caller budget: calls=%d result=%+v err=%v", elapsed, calls.Load(), result, runErr)
	}
}

// origin: S7 internal review rv-n1compat F6
func TestTransferLegacyRefusalExitClasses(t *testing.T) {
	for code, want := range map[string]int{
		"try_again": 75, "forward_failed": 75, "broker_forward_failed": 75,
		"too_many_in_flight": 75, "path_outside_roots": 77, "path_not_absolute": 64,
		"too_large": 64, "transfer_unknown": 64,
		x.CodeStorageHorizon: 75, x.CodeOwnerRecovering: 75, x.CodeNotMember: 77,
	} {
		t.Run(code, func(t *testing.T) {
			d, _ := controlbudget.After(time.Second)
			owner, err := xferclient.NewLegacy(xferclient.LegacyConfig{TransferID: "original", PutOnce: func(x.Deadline) (x.LegacyResult, error) {
				return legacyRefusal(code, "peer refused before byte work")
			}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = owner.Run(d)
			defer func() {
				if err := owner.Stop(d); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			gotCode, got := ClassifyTransferError(err)
			if gotCode != code || got != want {
				t.Fatalf("%s: got exit=%d, want=%d, err=%v", code, got, want, err)
			}
		})
	}
}

// A local destination that already exists is known before sending a prepare.
// It must not acquire remote work and then turn a usage refusal into rc74.
// origin: S7 internal review MAKETEST-TRANSFER F9; S7 plan §7 / §9 / T40
func TestTransferLegacyPullRejectsExistingDestinationBeforePrepare(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLegacyExistingDestination(t)
	}
	testharness.AssertNoGoroutineLeak(t, "legacy existing destination", g)
	testharness.AssertNoFDLeak(t, "legacy existing destination", fd, 0)
}

func exerciseLegacyExistingDestination(t *testing.T) {
	t.Helper()
	s, nc, id, stop := transferConnectionFixture(t)
	defer stop()
	cfg, closeOwner, err := TransferClientConfig(nc, s.ClientURL(), id, "lab")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeOwner(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cfg.Identity.NID, cfg.Identity.TransferID = "node", "original"
	cfg.Intent = x.Intent{Verb: "pull", Path: "/source"}
	destination := filepath.Join(t.TempDir(), "existing")
	if err := os.WriteFile(destination, []byte("original bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	owner, err := NewLegacyTransfer(cfg, destination)
	if owner != nil {
		defer func() {
			if err := owner.Stop(retryDeadline(t, time.Second)); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		}()
	}
	var classified *xferclient.Error
	if owner != nil || !errors.As(err, &classified) || classified.Code != "dst_exists" || classified.Unresolved || xferclient.ExitCode(err) != 64 {
		t.Fatalf("existing destination was admitted as a potentially sent legacy pull: owner=%T err=%v", owner, err)
	}
	cfg.Intent.Force = true
	forced, err := NewLegacyTransfer(cfg, destination)
	if err != nil || forced == nil {
		t.Fatalf("explicit forced overwrite was refused before the atomic primitive: %v", err)
	}
	defer func() {
		if err := forced.Stop(retryDeadline(t, time.Second)); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	contents, err := os.ReadFile(destination)
	if err != nil || string(contents) != "original bytes" {
		t.Fatalf("local refusal changed destination: %q %v", contents, err)
	}
}

// origin: S7 internal review rv-n1compat F6
func TestTransferRateAdmissionUsesUsageExit(t *testing.T) {
	s, nc, id, stop := transferConnectionFixture(t)
	defer stop()
	// The merged selector first requires the existing authenticated caps response.
	caps := answerTransferCaps(t, nc, id.PublicKey, "lab", x.Version, "")
	defer func() {
		if err := caps.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cfg, closeOwner, err := TransferClientConfig(nc, s.ClientURL(), id, "lab")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeOwner(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cfg.Identity.NID, cfg.Identity.TransferID = "node", "original"
	cfg.Intent = x.Intent{Verb: "push", Path: "/tmp/target", Size: 1 << 20, RateLimitBPS: 1}
	subject, _ := proto.SubjCtrlTransferRequest(id.PublicKey, "lab", "node", "original", x.OpProbe)
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		body, _ := json.Marshal(proto.TransferProbeResp{Version: x.Version, HistoryGuardVersion: x.Version, BucketConfigVersion: x.Version, InstanceID: "agent", MaxPayload: 1 << 20, MaxMsgSize: 1 << 20})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err = nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	d, _ := controlbudget.After(time.Second)
	legacy, err := SelectTransferClient(&cfg, d)
	if legacy || xferclient.ErrorCode(err) != x.CodeBudgetExceeded {
		t.Fatalf("wrong rate refusal: legacy=%v err=%v", legacy, err)
	}
	wrapped := TransferLeafError(TransferCommandPorts{Classify: ClassifyTransferError}, err)
	var ce *TransferCommandError
	if !errors.As(wrapped, &ce) || ce.Class != 64 {
		t.Fatalf("deterministic rate admission must use 64: class=%d err=%v", ce.Class, wrapped)
	}
}

// origin: S7 internal review rv-n1compat F5
func TestTransferLegacyPullPrepareIncludesByteBudget(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTransferLegacyPullPrepareIncludesByteBudget(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestTransferLegacyPullPrepareIncludesByteBudget", g)
	testharness.AssertNoFDLeak(t, "TestTransferLegacyPullPrepareIncludesByteBudget", fd, 0)
}
func exerciseTransferLegacyPullPrepareIncludesByteBudget(t *testing.T) {
	for _, budget := range []time.Duration{9 * time.Second, time.Second} {
		t.Run(budget.String(), func(t *testing.T) {
			s, nc, id, stop := transferConnectionFixture(t)
			defer stop()
			cfg, closeOwner, err := TransferClientConfig(nc, s.ClientURL(), id, "lab")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := closeOwner(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			cfg.Identity.NID, cfg.Identity.TransferID = "node", "original"
			cfg.Intent = x.Intent{Verb: "pull", Path: "/tmp/source"}
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			store, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: proto.XferBucketName("lab")})
			if err != nil {
				t.Fatal(err)
			}
			caps := answerTransferCaps(t, nc, id.PublicKey, "lab", "", "")
			defer func() {
				if err := caps.Unsubscribe(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			var calls, puts atomic.Int32
			done := make(chan struct{})
			peer, err := nc.Subscribe(proto.SubjCmdBy("lab", id.PublicKey, "node", "pull"), func(m *nats.Msg) {
				defer close(done)
				calls.Add(1)
				// Old prepare does the entire SDK upload before sending its reply.
				timer := time.NewTimer(5500 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-t.Context().Done():
					return
				}
				puts.Add(1)
				info, err := store.PutBytes(t.Context(), "original", []byte("old peer bytes"))
				if err != nil {
					t.Error(err)
					return
				}
				digest, err := jetstream.DecodeObjectDigest(info.Digest)
				if err != nil {
					t.Error(err)
					return
				}
				body, _ := json.Marshal(proto.PullPrepareResp{OK: true, Tier: "b", Size: int64(info.Size), SHA256: fmt.Sprintf("%x", digest), Bucket: proto.XferBucketName("lab"), ObjectKey: "original"})
				_ = m.Respond(body)
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := peer.Unsubscribe(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			final, err := nc.Subscribe(proto.SubjCtrlTransferFinalize(id.PublicKey, "lab", "original"), func(m *nats.Msg) { _ = m.Respond([]byte(`{"ok":true}`)) })
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := final.Unsubscribe(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			if err = nc.Flush(); err != nil {
				t.Fatal(err)
			}
			d, _ := controlbudget.After(budget)
			legacy, err := SelectTransferClient(&cfg, d)
			if err != nil || !legacy {
				t.Fatalf("cap0: %v %v", legacy, err)
			}
			destination := filepath.Join(t.TempDir(), "destination")
			owner, err := NewLegacyTransfer(cfg, destination)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := StopLegacyTransfer(owner); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			result, runErr := owner.Run(d)
			<-done
			if calls.Load() != 1 || puts.Load() != 1 {
				t.Fatalf("legacy byte work repeated: calls=%d puts=%d", calls.Load(), puts.Load())
			}
			if budget == time.Second {
				if xferclient.ExitCode(runErr) != 74 || result.Kind == "complete" {
					t.Fatalf("expired byte budget claimed success: %+v %v", result, runErr)
				}
			} else {
				data, err := os.ReadFile(destination)
				if runErr != nil || result.Kind != "complete" || err != nil || string(data) != "old peer bytes" {
					t.Fatalf("byte work cut before business deadline: %+v %v %v", result, runErr, err)
				}
			}
		})
	}
}
