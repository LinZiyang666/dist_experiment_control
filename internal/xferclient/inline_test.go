package xferclient

import (
	"bytes"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// The peer remains a control-port witness; the inline sink below is the real
// atomic path. This test certifies client ownership, not broker registration.
// origin: hot-update S7 plan §3 / §7 / T33 / INTEG-D
func TestTransferClientInlinePreservesOriginalInvocation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInlineInvocation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "inline client", g)
	testharness.AssertNoFDLeak(t, "inline client", fd, 0)
}
func exerciseInlineInvocation(t *testing.T) {
	t.Helper()
	for _, verb := range []string{"push", "pull"} {
		f := newClientFixture(t, verb, func(c *Config) { c.Intent.Tier = "a"; c.Intent.ChunkSize = 0; c.Intent.Force = true })
		func() {
			defer f.close()
			f.transport.identity.ObjectVersion = ""
			f.transport.lost = x.OpPrepare
			if verb == "pull" {
				if err := f.atomic.Stop(testDeadline(t, time.Second)); err != nil {
					t.Fatal(err)
				}
				vp, err := transferpath.ValidateForWrite(f.atomic.target, nil, transferpath.ModeOpen)
				if err != nil {
					t.Fatal(err)
				}
				receiver, err := transferpath.NewAtomicReceiver(vp, "inline-sink", true)
				if err != nil {
					t.Fatal(err)
				}
				f.client.cfg.Destination = receiver
			}
			var first []byte
			f.transport.onRequest = func(r x.Request) (x.Response, x.Attempt, error, bool) {
				if strings.Contains(r.CallID, ":") {
					t.Error("logical CallID is rejected by the canonical wire")
				}
				if r.Op == x.OpPrepare && verb == "push" {
					if !bytes.Equal(r.Inline, f.data) {
						t.Error("prepare did not retain exact source bytes")
					}
					if first == nil {
						first = bytes.Clone(r.Inline)
						if err := os.WriteFile(f.client.cfg.Source.Name(), []byte("changed pathname"), 0600); err != nil {
							t.Fatal(err)
						}
					} else if !bytes.Equal(first, r.Inline) {
						t.Error("replay reopened changed source")
					}
				}
				return x.Response{}, x.Attempt{}, nil, false
			}
			receipt, err := f.client.Run(testDeadline(t, 3*time.Second))
			if err != nil || receipt.Kind != "complete" {
				t.Fatalf("inline %s failed: %+v %v", verb, receipt, err)
			}
			if f.transport.dataOpened != 0 || f.writers != 0 || f.readers != 0 || f.transport.counts[x.OpPermit] != 1 {
				t.Fatal("inline transfer opened OBJ or minted another permit")
			}
			if f.transport.counts[x.OpPrepare] != 2 {
				t.Fatal("lost prepare did not replay original request")
			}
			trace := f.transport.trace
			for i, op := range trace {
				if i > 0 && op == x.OpPrepare && f.transport.counts[x.OpPrepare] > 1 {
					if i > 2 && trace[i-1] != x.OpStatus {
						t.Fatal("inline replay skipped status")
					}
				}
			}
			if verb == "pull" {
				data, err := os.ReadFile(f.atomic.target)
				if err != nil || !bytes.Equal(data, f.data) {
					t.Fatal("real atomic destination did not contain original inline source")
				}
			}
		}()
	}
}

// origin: hot-update S7 plan §3 / §6.4 / T33 / INTEG-D
func TestTransferClientPreparedEvidenceCannotBeReplaced(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePreparedEvidence(t)
	}
	testharness.AssertNoGoroutineLeak(t, "prepared source evidence", g)
	testharness.AssertNoFDLeak(t, "prepared source evidence", fd, 0)
}

func exercisePreparedEvidence(t *testing.T) {
	t.Helper()
	for _, defect := range []string{"missing_source", "grant_object_hash", "fingerprint", "size", "rate", "geometry", "capsule_identity", "capsule_origin", "capsule_signature", "object_hash"} {
		t.Run(defect, func(t *testing.T) {
			f := newClientFixture(t, "pull")
			defer f.close()
			// A Tier B pull's source may be sealed after prepare. Then the Grant
			// must name it, and a missing or different seal closes the gate
			// before the primitive (origin: hot-update S7 plan §2.2 / §4 / INTEG-D).
			sealedLate := defect == "missing_source" || defect == "grant_object_hash"
			f.transport.onRequest = func(r x.Request) (x.Response, x.Attempt, error, bool) {
				if r.Op == x.OpReserve && sealedLate {
					grant := &x.CommitGrant{Identity: f.transport.identity, GrantNonce: "original-grant", TerminalID: terminalSlot(f.transport.identity), ReceiverInstance: r.Reserve.ReceiverInstance}
					var source *x.SourceSeal
					if defect == "grant_object_hash" {
						source = &x.SourceSeal{Size: int64(len(f.data)), SHA256: digestOf([]byte("different authenticated source")), ChunkSize: 32768, Chunks: 3, SourceFingerprint: "stat"}
					}
					return x.Response{Grant: grant, Source: source}, x.Attempt{Sent: true}, nil, true
				}
				if r.Op != x.OpPrepare {
					return x.Response{}, x.Attempt{}, nil, false
				}
				s := f.transport.status()
				source := &x.SourceSeal{Size: int64(len(f.data)), SHA256: digestOf(f.data), ChunkSize: 32768, Chunks: 3, SourceFingerprint: "stat"}
				cap := &x.SettlementCapsule{Identity: f.transport.identity, Origin: f.transport.origin, ReceiverNkey: "receiver", Issuer: "issuer", Nonce: "nonce", Signature: "signature"}
				switch defect {
				case "missing_source", "grant_object_hash":
					source = nil
				case "fingerprint":
					source.SourceFingerprint = ""
				case "size":
					source.Size++
				case "rate":
					source.RateLimitBPS++
				case "geometry":
					source.Chunks++
				case "capsule_identity":
					cap.Identity.EntryNonce = "foreign"
				case "capsule_origin":
					cap.Origin.Generation++
				case "capsule_signature":
					cap.Signature = ""
				case "object_hash":
					source.SHA256 = digestOf([]byte("different authenticated source"))
				}
				return x.Response{Status: &s, Source: source, Settlement: cap}, x.Attempt{Sent: true}, nil, true
			}
			receipt, err := f.client.Run(testDeadline(t, time.Second))
			if sealedLate {
				// The durable reservation is settled as a closed gate, never landed.
				if err == nil || receipt.Kind == "complete" || f.atomic.commits != 0 || f.transport.finalizedOutcome == nil ||
					f.transport.finalizedOutcome.Outcome != x.OutcomeNotEntered || f.transport.finalizedOutcome.Code != x.CodeSourceChanged {
					t.Fatalf("unsealed or forged grant source reached the primitive: %v %+v", err, f.transport.finalizedOutcome)
				}
				return
			}
			if err == nil || receipt.Kind != "" || f.transport.counts[x.OpReserve] != 0 || f.atomic.commits != 0 {
				t.Fatal("forged prepare evidence reached commit")
			}
			if defect != "object_hash" && (f.client.pending == nil || f.client.pending.Op != x.OpPrepare) {
				t.Fatal("malformed sent prepare lost reconciliation duty")
			}
			if defect == "object_hash" && ErrorCode(err) != x.CodeSourceChanged {
				t.Fatalf("object metadata replaced authenticated source: %v", err)
			}
		})
	}
}
