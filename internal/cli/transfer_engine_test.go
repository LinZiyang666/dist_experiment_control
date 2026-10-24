package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The control peer supplies adversarial replies while the invocation, wire,
// dedicated connection and receipt writer are production implementations. This
// certifies the client/transport seam; it does not certify broker admission.
// origin: hot-update S7 plan §7 / T32–T34 / M01 / INTEG-D
func TestTransferNativeEngineReconcilesLostCommitWithoutReupload(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseNativeEngine(t)
	}
	testharness.AssertNoGoroutineLeak(t, "native transfer engine", g)
	testharness.AssertNoFDLeak(t, "native transfer engine", fd, 0)
}

func exerciseNativeEngine(t *testing.T) {
	t.Helper()
	s, control, actor, stop := transferConnectionFixture(t)
	defer stop()
	cfg, closeOwner, err := TransferClientConfig(control, s.ClientURL(), actor, "session")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeOwner(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	data := bytes.Repeat([]byte("original"), 256)
	source := filepath.Join(t.TempDir(), "source")
	if err = os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	cfg.Source, err = os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture owns this FD only until NewClient takes byte custody.
	// origin: hot-update S10 plan §0 / LINT-L2 errcheck
	sourceTransferred := false
	defer func() {
		if !sourceTransferred {
			if err := cfg.Source.Close(); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		}
	}()
	cfg.Identity.NID, cfg.Identity.TransferID = "node", "original"
	cfg.Intent = x.Intent{Verb: "push", Tier: "b", Path: "/remote/target", Size: int64(len(data)), RateLimitBPS: int64(len(data))}
	js, err := jetstream.New(control)
	if err != nil {
		t.Fatal(err)
	}
	bucket := proto.XferBucketName("session")
	store, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), "OBJ_"+bucket)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	policy := info.Config
	policy.Duplicates = proto.ObjectRecoveryHorizon
	if _, err = js.UpdateStream(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var identity x.Identity
	origin := x.Origin{BrokerID: "original-broker", InstanceID: "original-process", BootID: "original-boot", Generation: 2}
	var intent x.Intent
	var budget x.BudgetSeal
	var trace []x.Operation
	var committed bool
	var terminal *x.TerminalReceipt
	var physicalAtCommit uint64
	probe := x.ProbeResult{Version: x.Version, HistoryGuardVersion: x.Version, BucketConfigVersion: x.Version, InstanceID: "agent", MaxPayload: control.MaxPayload(), MaxMsgSize: control.MaxPayload()}
	handler := func(op x.Operation) nats.MsgHandler {
		return func(m *nats.Msg) {
			mu.Lock()
			defer mu.Unlock()
			r, err := proto.DecodeTransferRequest(actor.Fingerprint, "session", "node", "original", op, "push", m.Data)
			if err != nil {
				t.Errorf("production carrier refused original %s: %v", op, err)
				_ = m.Respond([]byte(`{"code":"transfer_intent_conflict"}`))
				return
			}
			trace = append(trace, op)
			if identity.EntryNonce != "" && op != x.OpProbe && op != x.OpPermit && (r.Identity != identity || r.Origin != origin) {
				t.Error("native engine replaced original identity/origin")
			}
			response := x.Response{}
			switch op {
			case x.OpProbe:
				response.Probe = &probe
			case x.OpPermit:
				identity, intent = r.Identity, *r.Intent
				identity.EntryNonce, identity.ObjectVersion = "entry", "fixedOriginalNUID"
				response.Permit = &x.CreationPermit{Identity: identity, Origin: origin, IssuedAt: time.Now().UTC(), CreateUntil: time.Now().UTC().Add(time.Minute), PermitNonce: "permit", Signature: "signed"}
				duration, err := proto.TransferBudget(intent.Tier, intent.Size, intent.RateLimitBPS)
				if err != nil {
					t.Error(err)
				}
				budget = x.BudgetSeal{StartedAt: time.Now().UTC(), EffectiveBudgetNS: int64(duration), RemainingNS: int64(duration), Size: intent.Size, RateLimitBPS: intent.RateLimitBPS, SourceSealed: true}
			case x.OpPrepare:
				if *r.Intent != intent || intent.RateLimitBPS != int64(len(data)) {
					t.Error("prepare lost its original rate-bearing intent")
				}
			case x.OpSource:
				if r.Source.RateLimitBPS != intent.RateLimitBPS || r.Source.SHA256 != intent.SHA256 {
					t.Error("source publication lost the sealed rate/hash")
				}
			case x.OpCommit:
				metadata, err := store.GetInfo(t.Context(), "original")
				if err != nil || metadata.NUID != identity.ObjectVersion || metadata.Size != uint64(len(data)) {
					t.Errorf("commit has no original physical object: %v", err)
				}
				current, err := stream.Info(t.Context())
				if err != nil {
					t.Error(err)
				} else {
					physicalAtCommit = current.State.LastSeq
				}
				slot := sha256.Sum256([]byte("xfer-terminal-v1\nsession\noriginal\nentry"))
				terminal = &x.TerminalReceipt{TerminalID: hex.EncodeToString(slot[:]), Kind: "complete", CanonicalDigest: strings.Repeat("b", 64), ReqID: "exact", HistoryStream: "history-session", HistorySeq: 1, Bytes: int64(len(data)), SHA256: intent.SHA256}
				committed = true
				// Accepted commit with an unusable reply retains the native Sent
				// fact and must cause status before any second mutation.
				_ = m.Respond([]byte(`{}`))
				return
			case x.OpStatus:
				if !committed || len(trace) < 2 || trace[len(trace)-2] != x.OpCommit {
					t.Error("commit loss did not reconcile before another operation")
				}
			}
			if op == x.OpPrepare || op == x.OpSource || op == x.OpStatus {
				response.Status = &x.Status{State: x.StatusLive, Phase: x.PhasePrepared, Identity: identity, Origin: origin, Budget: budget}
				if committed {
					response.Status.State, response.Status.Phase, response.Status.Terminal = x.StatusTerminal, x.PhaseHistoryConfirmed, terminal
				}
			}
			body, err := proto.EncodeTransferResponse(r, response)
			if err != nil {
				t.Error(err)
				return
			}
			_ = m.Respond(body)
		}
	}
	for _, op := range []x.Operation{x.OpProbe, x.OpPermit, x.OpPrepare, x.OpSource, x.OpCommit, x.OpStatus} {
		var subject string
		if op == x.OpPrepare || op == x.OpCommit {
			verb := "push"
			if op == x.OpCommit {
				verb = "push-commit"
			}
			subject = proto.SubjCmdBy("session", actor.PublicKey, "node", verb)
		} else {
			subject, err = proto.SubjCtrlTransferRequest(actor.PublicKey, "session", "node", "original", op)
			if err != nil {
				t.Fatal(err)
			}
		}
		sub, err := control.Subscribe(subject, handler(op))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := sub.Unsubscribe(); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		}()
	}
	if err = control.Flush(); err != nil {
		t.Fatal(err)
	}
	// A transfer-v1 broker answers the caps verb with its version before the
	// transfer probe is consulted (§9).
	caps := answerTransferCaps(t, control, actor.PublicKey, "session", x.Version, "")
	defer func() {
		if err := caps.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	deadline := retryDeadline(t, 5*time.Second)
	if legacy, err := SelectTransferClient(&cfg, deadline); legacy || err != nil {
		t.Fatalf("capable selection failed: %v", err)
	}
	invocation, err := xferclient.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sourceTransferred = true
	defer func() {
		if err := invocation.Stop(retryDeadline(t, 5*time.Second)); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	started := time.Now()
	receipt, err := invocation.Run(deadline)
	// Inspect raw metadata even when the client refuses: an SDK upload can
	// replace the physical NUID before the receipt owner notices the conflict.
	physical, physicalErr := store.GetInfo(t.Context(), "original")
	if physicalErr == nil && physical.NUID != "fixedOriginalNUID" {
		t.Fatalf("capable path allocated a second physical NUID: %s", physical.NUID)
	}
	if err != nil || receipt.Kind != "complete" {
		t.Fatalf("native invocation failed: %v", err)
	}
	if time.Since(started) < 950*time.Millisecond {
		t.Fatal("native bytes bypassed the original sender rate cap")
	}
	mu.Lock()
	counts := map[x.Operation]int{}
	for _, op := range trace {
		counts[op]++
	}
	last := physicalAtCommit
	mu.Unlock()
	if counts[x.OpPermit] != 1 || counts[x.OpPrepare] != 1 || counts[x.OpCommit] != 1 || counts[x.OpStatus] != 1 {
		b, _ := json.Marshal(counts)
		t.Fatalf("lost commit created another invocation or replay: %s", b)
	}
	after, err := stream.Info(t.Context())
	if err != nil || after.State.LastSeq != last || last == 0 {
		t.Fatal("commit reconciliation republished physical OBJ records")
	}
	metadata, err := store.GetInfo(t.Context(), "original")
	if err != nil || metadata.NUID != "fixedOriginalNUID" {
		t.Fatal("capable path allocated a second physical NUID")
	}
}
