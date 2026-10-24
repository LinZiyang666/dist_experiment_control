package cli

import (
	"encoding/json"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
)

// A source-size answer belongs to precreation selection. The original pull
// still carries unknown source facts until its pinned sender seals them.
// origin: S7 internal review INTEG-O F4
func TestTransferSelectionPinsSmallPullToInlineBeforeCreation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePullSelection(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

// A rejected inspection must leave selection unset. Retrying the same config
// must inspect again, rather than treating an error's temporary default as an
// explicit Tier B instruction.
// origin: S7 internal review INTEG-O F4
func TestTransferPullSelectionRefusesUnprovenSizeBeforeCreation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUnprovenPullSize(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseUnprovenPullSize(t *testing.T) {
	t.Helper()
	s, nc, actor, stop := transferConnectionFixture(t)
	defer stop()
	caps := answerTransferCaps(t, nc, actor.PublicKey, "lab", x.Version, "")
	defer func() {
		if err := caps.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	for _, tc := range []struct {
		name string
		size any
	}{
		{"missing", nil}, {"negative", int64(-1)}, {"oversize", proto.XferMaxBytes + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, closeOwner, err := TransferClientConfig(nc, s.ClientURL(), actor, "lab")
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
			var probes atomic.Int32
			subject, _ := proto.SubjCtrlTransferRequest(actor.PublicKey, "lab", "node", "original", x.OpProbe)
			sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
				probes.Add(1)
				response := map[string]any{"version": x.Version, "history_guard_version": x.Version,
					"bucket_config_version": x.Version, "instance_id": "agent", "max_payload": nc.MaxPayload(), "max_msg_size": nc.MaxPayload(), "code": "", "error": ""}
				if tc.size != nil {
					response["source_size"] = tc.size
				}
				body, _ := json.Marshal(response)
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
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			for attempt := 0; attempt < 2; attempt++ {
				legacy, err := SelectTransferClient(&cfg, retryDeadline(t, time.Second))
				if legacy || xferclient.ErrorCode(err) != x.CodePayloadUnsupported || cfg.Intent.Tier != "" || cfg.Identity.EntryNonce != "" || cfg.Identity.ObjectVersion != "" {
					t.Fatalf("unproven source became a created/default engine: tier=%q legacy=%v err=%v", cfg.Intent.Tier, legacy, err)
				}
			}
			if probes.Load() != 2 {
				t.Fatal("failed selection did not repeat its read-only inspection")
			}
		})
	}
	wire := &CanonicalTransferWire{Actor: actor.PublicKey}
	query := x.Request{Op: x.OpProbe, CallID: "select", Identity: x.Identity{SID: "lab", NID: "node", TransferID: "original"}, Intent: &x.Intent{Verb: "pull", Path: "/source"}}
	if _, _, err := wire.Encode(query); err != nil {
		t.Fatalf("valid read-only query refused: %v", err)
	}
	for _, mutate := range []func(*x.Request){
		func(r *x.Request) { r.Permit = &x.CreationPermit{} },
		func(r *x.Request) { r.Source = &x.SourceSeal{} },
		func(r *x.Request) { r.Reserve = &x.ReserveRequest{} },
		func(r *x.Request) { r.Outcome = &x.ReceiverOutcome{} },
		func(r *x.Request) { r.Cancel = &x.CancelRequest{} },
		func(r *x.Request) { r.Inline = []byte("hidden bytes") },
		func(r *x.Request) { r.Intent = &x.Intent{Verb: "pull", Path: "/source", Tier: "b"} },
	} {
		invalid := query
		mutate(&invalid)
		if _, _, err := wire.Encode(invalid); err == nil {
			t.Fatal("source inspection silently discarded creation or settlement fields")
		}
	}
}

func exercisePullSelection(t *testing.T) {
	t.Helper()
	s, nc, actor, stop := transferConnectionFixture(t)
	defer stop()
	caps := answerTransferCaps(t, nc, actor.PublicKey, "lab", x.Version, "")
	defer func() {
		if err := caps.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	ceiling := min(int64(proto.XferTierAMaxBytes), nc.MaxPayload()/2-4096)
	for _, tc := range []struct {
		name string
		size int64
		tier string
		want string
	}{
		{"empty", 0, "", "a"},
		{"small", 4096, "", "a"},
		{"below-boundary", ceiling - 1, "", "a"},
		{"at-boundary", ceiling, "", "b"},
		{"above-boundary", ceiling + 1, "", "b"},
		{"object-required", proto.XferTierAMaxBytes, "", "b"},
		{"explicit-object", 4096, "b", "b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, closeOwner, err := TransferClientConfig(nc, s.ClientURL(), actor, "lab")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := closeOwner(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			cfg.Identity.NID, cfg.Identity.TransferID = "node", "original"
			cfg.Intent = x.Intent{Verb: "pull", Tier: tc.tier, Path: "/source", RateLimitBPS: 0}
			var probes atomic.Int32
			var creates atomic.Int32
			subject, err := proto.SubjCtrlTransferRequest(actor.PublicKey, "lab", "node", "original", x.OpProbe)
			if err != nil {
				t.Fatal(err)
			}
			sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
				probes.Add(1)
				var request struct {
					SourcePath string `json:"source_path"`
				}
				if err := json.Unmarshal(m.Data, &request); err != nil {
					t.Error(err)
					return
				}
				response := map[string]any{"version": x.Version, "history_guard_version": x.Version,
					"bucket_config_version": x.Version, "instance_id": "agent", "max_payload": nc.MaxPayload(), "max_msg_size": nc.MaxPayload(), "code": "", "error": ""}
				if request.SourcePath != "" {
					if request.SourcePath != cfg.Intent.Path || tc.tier != "" {
						t.Error("source inspection replaced an explicitly selected engine or path")
					}
					response["source_size"] = tc.size
				}
				body, _ := json.Marshal(response)
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
			permit, _ := proto.SubjCtrlTransferRequest(actor.PublicKey, "lab", "node", "original", x.OpPermit)
			guard, err := nc.Subscribe(permit, func(m *nats.Msg) { creates.Add(1) })
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := guard.Unsubscribe(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			legacy, err := SelectTransferClient(&cfg, retryDeadline(t, time.Second))
			if legacy || err != nil || cfg.Intent.Tier != tc.want {
				t.Fatalf("precreation pull size=%d: tier=%q want=%q legacy=%v err=%v", tc.size, cfg.Intent.Tier, tc.want, legacy, err)
			}
			if probes.Load() != 1 || creates.Load() != 0 || cfg.Identity.EntryNonce != "" || cfg.Identity.ObjectVersion != "" || cfg.Intent.Size != 0 || cfg.Intent.SHA256 != "" || cfg.Intent.RateLimitBPS != 0 {
				t.Fatal("selection created authority or rewrote unknown source/rate facts")
			}
			if (cfg.Intent.ChunkSize != 0) != (tc.want == "b") {
				t.Fatal("inline selection retained OBJ geometry")
			}
		})
	}
}
