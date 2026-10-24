package filetransfer

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// Copy into internal/filetransfer. Production encoder and forwarded decoder
// communicate over real NATS; the worker records admission, without byte IO.
// origin: S7 adversarial review rv-n1compat
// origin: S7 internal review rv-n1compat F3
func TestTransferForwardedCanonicalCarriers(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferForwardedCanonicalCarriers(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}
func exerciseTransferForwardedCanonicalCarriers(t *testing.T) {
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	if !s.ReadyForConnections(time.Second) {
		t.Fatal("NATS not ready")
	}
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	kp, _ := nkeys.CreateUser()
	actor, _ := kp.PublicKey()
	sum := sha256.Sum256([]byte("abc"))
	for _, verb := range []string{"push", "pull", "push-commit"} {
		t.Run(verb, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data")
			if err := os.WriteFile(path, []byte("abc"), 0600); err != nil {
				t.Fatal(err)
			}
			id := x.Identity{SID: "lab", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", ObjectVersion: "fixedNUID"}
			intent := x.Intent{Verb: verb, Tier: "b", Path: path, Size: 3, SHA256: hex.EncodeToString(sum[:]), RateLimitBPS: 65536, ChunkSize: 32768}
			if verb == "push-commit" {
				intent.Verb = "push"
			}
			id.IntentDigest, err = proto.TransferIntentDigest(proto.Intent(intent))
			if err != nil {
				t.Fatal(err)
			}
			origin := x.Origin{BrokerID: "broker", InstanceID: "brokerinstance", BootID: "boot", Generation: 1}
			permit := x.CreationPermit{Identity: id, Origin: origin, IssuedAt: time.Now().UTC(), CreateUntil: time.Now().UTC().Add(time.Minute), PermitNonce: "permit", Signature: "signature"}
			request := x.Request{Op: x.OpPrepare, CallID: "originalcall", Identity: id, Origin: origin, Intent: &intent, Permit: &permit}
			if verb == "push-commit" {
				request.Op = x.OpCommit
				request.Intent = nil
				request.Permit = nil
			}
			_, body, err := proto.EncodeTransferRequest(actor, request)
			if err != nil {
				t.Fatal(err)
			}
			// Current production forwarding decodes the public ctl carrier,
			// then stamps its admitted budget and durable receiver grant.
			// origin: S7 internal review rv-n1compat F3 / INTEG-D carrier contract
			decoded, err := proto.DecodeTransferRequest(id.ActorFP, id.SID, id.NID, id.TransferID, request.Op, intent.Verb, body)
			if err != nil {
				t.Fatal(err)
			}
			budget := x.BudgetSeal{StartedAt: time.Now().UTC(), Size: intent.Size, RateLimitBPS: intent.RateLimitBPS, EffectiveBudgetNS: int64(300 * time.Second), RemainingNS: int64(299 * time.Second), SourceSealed: true}
			var grant *x.CommitGrant
			if request.Op == x.OpCommit {
				grant = &x.CommitGrant{Identity: id, GrantNonce: "grant", ReceiverInstance: id.AgentInstance, TerminalID: "terminal"}
			}
			_, body, err = proto.EncodeAgentTransferCarrier(decoded, budget, nil, grant)
			if err != nil {
				t.Fatal(err)
			}
			worker := &lifecycleWorker{}
			if verb == "push-commit" {
				worker.prepares = append(worker.prepares, x.Prepare{Identity: id, Origin: origin, Intent: intent, Budget: budget})
			}
			h := &Handler{Worker: worker, Logger: testharness.SilentLog(), SID: id.SID, NID: func() string { return id.NID }, Instance: func() string { return id.AgentInstance }}
			h.ports = newWorkerPorts(h)
			if request.Op == x.OpCommit {
				if err := h.ports.record(x.Request{Op: x.OpPrepare, Identity: id, Origin: origin, Intent: &intent}, budget, nil); err != nil {
					t.Fatal(err)
				}
			}
			h.Admit = func() (func(), bool) { t.Error("capable carrier entered legacy"); return func() {}, false }
			inbox := nats.NewInbox()
			sub, err := nc.SubscribeSync(inbox)
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
			msg := &nats.Msg{Reply: inbox, Data: body}
			switch verb {
			case "push":
				h.HandlePushForwarded(nc, msg)
			case "pull":
				h.HandlePullForwarded(nc, msg)
			case "push-commit":
				h.HandlePushCommitForwarded(nc, msg)
			}
			reply, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if verb != "push-commit" {
				if len(worker.prepares) != 1 {
					t.Errorf("canonical prepare did not reach worker: %s", reply.Data)
				} else if worker.prepares[0].Intent != intent {
					t.Errorf("signed intent changed: want=%+v got=%+v", intent, worker.prepares[0].Intent)
				}
			}
			response, decodeErr := proto.DecodeTransferResponse(request.Op, intent.Verb, reply.Data)
			if decodeErr != nil || response.Code != "" {
				t.Errorf("canonical caller cannot accept forwarded result: decode=%v code=%s wire=%s", decodeErr, response.Code, reply.Data)
			}
		})
	}
}
