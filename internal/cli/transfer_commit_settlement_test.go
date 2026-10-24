package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferfinal"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xferserve"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// lostCommitWire discards one decoded reply at the native ctl receive edge.
// The FIX-CUT receive overlay can instead drop it before Decode and wait out
// the real attempt budget; its marker prevents this fallback dropping twice.
type lostCommitWire struct {
	CanonicalTransferWire
	root    string
	dropped bool
}

func (w *lostCommitWire) Decode(op x.Operation, data []byte) (x.Response, error) {
	response, err := w.CanonicalTransferWire.Decode(op, data)
	if op == x.OpCommit && !w.dropped {
		w.dropped = true
		if _, markerErr := os.Stat(filepath.Join(w.root, "fault-commit-reply.entered")); errors.Is(markerErr, os.ErrNotExist) && err == nil {
			if err := os.Rename(filepath.Join(w.root, "fault-commit-reply"), filepath.Join(w.root, "fault-commit-reply.entered")); err != nil {
				return x.Response{}, err
			}
			return x.Response{}, errors.New("commit reply dropped at ctl receive edge")
		}
	}
	return response, err
}

type landedCommitResolver struct{ outcome *x.ReceiverOutcome }

func (r landedCommitResolver) Resolve(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
	if r.outcome == nil {
		return x.ReceiverOutcome{}, errors.New(x.CodeCommitInflight)
	}
	return *r.outcome, nil
}

// This is a native transport/settlement seam, with real atomic landing,
// tracker, two-directory ledger, exact JS history and finalizer retirement.
// Its peer deliberately orders publication after the closed-gate replay.
// origin: S7 internal review FIX-CUT F1 / hot-update S7 plan §3 / §4 / §7
func TestTransferNativeLostCommitUsesRetainedTerminal(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseNativeCommitSettlement(t)
	}
	testharness.AssertNoGoroutineLeak(t, "native lost commit settlement", g)
	testharness.AssertNoFDLeak(t, "native lost commit settlement", fd, 0)
}

func exerciseNativeCommitSettlement(t *testing.T) {
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
	root := t.TempDir()
	if os.Getenv("S7_COMMIT_RECEIVE_OVERLAY") != "" {
		// The overlay uses the same one-shot rename marker as FIX-CUT.
		t.Setenv("S7_TRANSFER_FAULT_ROOT", root)
	}
	if err = os.WriteFile(filepath.Join(root, "fault-commit-reply"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	wire := &lostCommitWire{CanonicalTransferWire: CanonicalTransferWire{Actor: actor.PublicKey}, root: root}
	cfg.Transport.(*TransferTransport).wire = wire
	data := []byte("the original complete transfer\n")
	source, target := filepath.Join(root, "source"), filepath.Join(root, "target")
	if err = os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(target, []byte("old complete contents"), 0600); err != nil {
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
	digest := sha256.Sum256(data)
	cfg.Intent = x.Intent{Verb: "push", Tier: "a", Path: target, Force: true, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
	receiver, err := NewTransferReceiver(target, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = receiver.Stop(retryDeadline(t, 5*time.Second)) }()
	if err = receiver.(interface{ BindObjectVersion(string) error }).BindObjectVersion(""); err != nil {
		t.Fatal(err)
	}
	js, err := jetstream.New(control)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: "HISTORY_session",
		Subjects: []string{proto.SubjAuditTransfer("session")}, Discard: jetstream.DiscardNew})
	if err != nil {
		t.Fatal(err)
	}
	base := cfg.Base
	boot, _, err := base.Clock()
	if err != nil {
		t.Fatal(err)
	}
	origin := x.Origin{BrokerID: "original-broker", InstanceID: "original-process", BootID: boot, Generation: 1}
	ledger, err := xferledger.New(x.LedgerConfig{Base: base, Root: filepath.Join(root, "primary"), FallbackRoot: filepath.Join(root, "outbox")})
	if err != nil {
		t.Fatal(err)
	}
	history, err := xferhistory.New(x.HistoryConfig{Base: base, JS: js, SID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	tracker, err := xfertrack.New(x.TrackerConfig{Base: base, Origin: origin, Ledger: ledger, History: history})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tracker.Stop(retryDeadline(t, 5*time.Second)) }()
	view, err := xfertrack.Decisions(tracker)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var identity x.Identity
	var permit x.CreationPermit
	var outcome x.ReceiverOutcome
	var exact x.TerminalReceipt
	var trace []x.Operation
	commits := 0
	finalizer, err := xferfinal.New(x.FinalConfig{Base: base, Ledger: ledger, History: history, Decisions: view,
		ResolveReceiver: landedCommitResolver{outcome: &outcome}, Objects: xferserve.Objects{UTC: base.UTC,
			Rows: func(id x.Identity, d x.Deadline) (x.LedgerRow, error) {
				return view.Read(x.Key{SID: id.SID, TransferID: id.TransferID}, d)
			}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = finalizer.Stop(retryDeadline(t, 5*time.Second)) }()
	for _, op := range []x.Operation{x.OpProbe, x.OpPermit, x.OpPrepare, x.OpSource, x.OpCommit, x.OpStatus} {
		route := op
		var subject string
		// Routing uses the public subject builders; encoding the real union
		// happens inside the client.
		if route == x.OpPrepare || route == x.OpCommit {
			verb := "push"
			if route == x.OpCommit {
				verb = "push-commit"
			}
			subject = proto.SubjCmdBy("session", actor.PublicKey, "node", verb)
		} else {
			subject, err = proto.SubjCtrlTransferRequest(actor.PublicKey, "session", "node", "original", route)
			if err != nil {
				t.Fatal(err)
			}
		}
		sub, err := control.Subscribe(subject, func(m *nats.Msg) {
			mu.Lock()
			defer mu.Unlock()
			r, requestErr := proto.DecodeTransferRequest(actor.Fingerprint, "session", "node", "original", route, "push", m.Data)
			if requestErr != nil {
				t.Error(requestErr)
				return
			}
			trace = append(trace, route)
			d := retryDeadline(t, 5*time.Second)
			response := x.Response{}
			switch route {
			case x.OpProbe:
				response.Probe = &x.ProbeResult{Version: x.Version, HistoryGuardVersion: x.Version, BucketConfigVersion: x.Version,
					InstanceID: "agent", MaxPayload: control.MaxPayload(), MaxMsgSize: control.MaxPayload()}
			case x.OpPermit:
				identity = r.Identity
				identity.EntryNonce = "entry"
				permit = x.CreationPermit{Identity: identity, Origin: origin, IssuedAt: base.UTC(), CreateUntil: base.UTC().Add(time.Minute), PermitNonce: "permit", Signature: "fixture-authority"}
				response.Permit = &permit
			case x.OpPrepare:
				_, requestErr = tracker.Admit(permit, *r.Intent, d)
				if requestErr == nil {
					var n int
					n, requestErr = receiver.WriteAt(r.Inline, 0)
					if requestErr == nil && n != len(data) {
						requestErr = errors.New("short receiver write")
					}
				}
				if requestErr == nil {
					requestErr = receiver.Sync()
				}
			case x.OpSource:
				_, requestErr = tracker.SourceSeal(identity, *r.Source, d)
			case x.OpCommit:
				commits++
				if commits == 1 {
					var grant x.CommitGrant
					grant, requestErr = tracker.Reserve(identity, x.ReserveRequest{Origin: origin, ReceiverInstance: identity.AgentInstance}, d)
					if requestErr == nil {
						outcome = receiver.Commit(grant, d)
						if outcome.Outcome != x.OutcomeLanded || !outcome.ParentSynced {
							requestErr = errors.New("original primitive did not land durably")
						} else {
							// The atomic path owns the primitive; the receiver actor
							// adds the verified byte/hash seal to its courier outcome.
							outcome.Bytes, outcome.SHA256 = int64(len(data)), cfg.Intent.SHA256
						}
					}
				} else {
					// A stale commit reaches the receiver just before its courier's
					// outcome becomes visible. Only that outcome may decide the ctl.
					exact, requestErr = tracker.Finish(identity, outcome, d)
					if requestErr == nil {
						var settled x.Status
						settled, requestErr = finalizer.Advance(identity, d)
						if requestErr == nil && settled.Phase != x.PhaseSettled {
							requestErr = errors.New("landed transfer did not retire")
						}
					}
					response.Code, response.Error = x.CodeCanceled, "receiver gate closed"
				}
			case x.OpStatus:
			}
			if requestErr == nil && (route == x.OpPrepare || route == x.OpSource || route == x.OpStatus || route == x.OpCommit && commits == 1) {
				var status x.Status
				status, requestErr = tracker.Status(x.StatusRequest{Identity: identity, Origin: origin}, d)
				response.Status = &status
			}
			if requestErr != nil {
				t.Error(requestErr)
				response = x.Response{Code: x.CodeResultUnknown, Error: requestErr.Error()}
			}
			body, encodeErr := proto.EncodeTransferResponse(r, response)
			if encodeErr != nil {
				t.Error(encodeErr)
				return
			}
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
	}
	if err = control.Flush(); err != nil {
		t.Fatal(err)
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
	receipt, runErr := invocation.Run(retryDeadline(t, 30*time.Second))
	mu.Lock()
	defer mu.Unlock()
	landed, err := os.ReadFile(target)
	if err != nil || !bytes.Equal(landed, data) {
		t.Fatalf("destination did not land: %q %v", landed, err)
	}
	census, err := ledger.Scan(retryDeadline(t, 5*time.Second))
	if err != nil || !census.Ready || !census.Complete || len(census.Rows) != 0 || len(census.Entries) != 0 {
		t.Fatalf("fixture did not consume the durable responsibility: %+v %v", census, err)
	}
	if runErr != nil || receipt != exact || receipt.Kind != "complete" || xferclient.ExitCode(runErr) != 0 {
		t.Fatalf("landed/retired transfer returned fabricated cancellation: receipt=%+v native_rc=%d err=%v", receipt, xferclient.ExitCode(runErr), runErr)
	}
	info, err := stream.Info(t.Context())
	if err != nil || info.State.Msgs != 2 {
		t.Fatalf("physical START/TERMINAL count differs: %+v %v", info, err)
	}
	counts := make(map[x.Operation]int)
	for i, op := range trace {
		counts[op]++
		if op == x.OpCommit && counts[op] > 1 && trace[i-1] != x.OpStatus {
			t.Fatalf("replay skipped status: %v", trace)
		}
	}
	if counts[x.OpPermit] != 1 || counts[x.OpPrepare] != 1 || commits != 2 || counts[x.OpStatus] != 2 || !wire.dropped {
		t.Fatalf("lost commit missed original-ID recovery: %v", trace)
	}
}
