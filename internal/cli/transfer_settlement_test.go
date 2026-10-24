package cli

import (
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
)

// The embedded server observes the native restricted CONNECT and scoped wire;
// issuer verification and durable census remain the broker's independent gate.
// origin: hot-update S7 plan §6.4 / T31–T34 / INTEG-D
func TestTransferSettlementRetainsReceiverAndOriginalScope(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferSettlement(t)
	}
	testharness.AssertNoGoroutineLeak(t, "settlement transport", g)
	testharness.AssertNoFDLeak(t, "settlement transport", fd, 0)
}

func exerciseTransferSettlement(t *testing.T) {
	t.Helper()
	s, control, receiver, stop := transferConnectionFixture(t)
	defer stop()
	cfg, closeOwner, err := TransferClientConfig(control, s.ClientURL(), receiver, "session")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeOwner(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	transport := cfg.Transport.(*TransferTransport)
	id := x.Identity{SID: "session", NID: "node", ActorFP: receiver.Fingerprint, AgentInstance: "agent", TransferID: "original", EntryNonce: "entry", IntentDigest: strings.Repeat("a", 64)}
	origin := x.Origin{BrokerID: "original-broker", InstanceID: "broker-instance", BootID: "broker-boot", Generation: 3}
	capsule := x.SettlementCapsule{Identity: id, Origin: origin, ReceiverNkey: receiver.PublicKey, Issuer: "issuer", Nonce: "nonce", Signature: "signed-by-issuer"}
	for _, change := range []func(*x.SettlementCapsule){
		func(c *x.SettlementCapsule) { c.ReceiverNkey = "foreign" },
		func(c *x.SettlementCapsule) { c.Identity.ActorFP = "foreign" },
		func(c *x.SettlementCapsule) { c.Signature = "" },
		func(c *x.SettlementCapsule) { c.Identity.EntryNonce = "" },
		func(c *x.SettlementCapsule) { c.Origin.BootID = "" },
	} {
		bad := capsule
		change(&bad)
		if err := transport.BindSettlement(bad); err == nil {
			t.Fatal("accepted incomplete or foreign settlement authority")
		}
	}
	if err = transport.BindSettlement(capsule); err != nil {
		t.Fatal(err)
	}
	changed := capsule
	changed.Identity.TransferID = "replacement"
	if err = transport.BindSettlement(changed); err == nil {
		t.Fatal("settlement capsule replaced the original binding")
	}
	request := x.Request{Op: x.OpStatus, CallID: "original-status", Identity: id, Origin: origin}
	deadline := retryDeadline(t, time.Second)
	settlementRequest := request
	settlementRequest.Op = x.OpTerminal
	nc, err := transport.requestConnection(settlementRequest, deadline)
	if err != nil {
		t.Fatal(err)
	}
	var connected x.SettlementCapsule
	if err = json.Unmarshal([]byte(nc.Opts.Token), &connected); err != nil || connected != capsule || nc == control || nc.Opts.Name != "transfer-settlement:session:node" || nc.Opts.ReconnectBufSize != -1 || nc.Opts.FlusherTimeout != 5*time.Second || nc.Opts.Nkey != receiver.PublicKey || nc.Opts.SignatureCB == nil {
		t.Fatal("restricted CONNECT lost original capsule, NKey proof, role or bounds")
	}
	if reused, err := transport.requestConnection(settlementRequest, deadline); err != nil || reused != nc {
		t.Fatal("settlement retry replaced its connection")
	}
	foreign := request
	foreign.Origin.Generation++
	if _, err = transport.requestConnection(foreign, deadline); err == nil {
		t.Fatal("another origin borrowed restricted settlement authority")
	}
	bytes := request
	bytes.Op = x.OpPrepare
	if ordinary, err := transport.requestConnection(bytes, deadline); err != nil || ordinary != control {
		t.Fatal("settlement authority became a byte-work credential")
	}
	// Full native decoding validates every supplied binding even for status,
	// where no invocation exists to perform an additional validation pass.
	subject, _, err := proto.EncodeTransferRequest(receiver.PublicKey, request)
	if err != nil {
		t.Fatal(err)
	}
	for _, defect := range []string{"valid", "actor", "origin", "terminal_slot"} {
		status := x.Status{State: x.StatusLive, Phase: x.PhasePrepared, Identity: id, Origin: origin}
		switch defect {
		case "actor":
			status.Identity.ActorFP = "foreign"
		case "origin":
			status.Origin.Generation++
		case "terminal_slot":
			status.State = x.StatusTerminal
			status.Terminal = &x.TerminalReceipt{TerminalID: "wrong-slot", Kind: "failed", CanonicalDigest: strings.Repeat("b", 64), ReqID: "req", HistoryStream: "history-session", HistorySeq: 1}
		}
		body, err := proto.EncodeTransferResponse(request, x.Response{Status: &status})
		if err != nil {
			t.Fatal(err)
		}
		sub, err := control.Subscribe(subject, func(m *nats.Msg) { _ = m.Respond(body) })
		if err != nil {
			t.Fatal(err)
		}
		if err = control.Flush(); err != nil {
			t.Fatal(err)
		}
		_, attempt, requestErr := transport.Request(request, retryDeadline(t, time.Second))
		_ = sub.Unsubscribe()
		if defect == "valid" && requestErr != nil || defect != "valid" && (requestErr == nil || !attempt.Sent || !errors.Is(requestErr, ErrTransferReconcileRequired)) {
			t.Fatalf("native status %s lost exact scope or sent uncertainty: %v", defect, requestErr)
		}
	}
	if err = closeOwner(); err != nil || !nc.IsClosed() || control.IsClosed() {
		t.Fatal("settlement close leaked its connection or closed borrowed control")
	}
	if err = transport.BindSettlement(capsule); err == nil {
		t.Fatal("retired settlement owner reopened")
	}
}

// This covers the actual session-removal control adapter over NATS. The
// controlled finalizer refuses even explicit abandonment: transmitting a flag
// is not evidence that an exact abandoned terminal has reached history.
// origin: hot-update S7 plan lead LQ1 / T34 / INTEG-D
func TestTransferSessionRemovalPreservesExplicitAuthority(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferSessionRemoval(t)
	}
	testharness.AssertNoGoroutineLeak(t, "session removal carrier", g)
	testharness.AssertNoFDLeak(t, "session removal carrier", fd, 0)
}

func exerciseTransferSessionRemoval(t *testing.T) {
	t.Helper()
	_, nc, id, stop := transferConnectionFixture(t)
	defer stop()
	seen := make(chan bool, 2)
	sub, err := nc.Subscribe(proto.SubjCtrlSessionRm(id.PublicKey, "session"), func(m *nats.Msg) {
		tombstoned := false
		response := xferauth.RemoveSession(m.Data, func() error { tombstoned = true; return nil }, true, func(abandon bool) error {
			if !tombstoned {
				t.Error("session barrier preceded tombstone")
			}
			seen <- abandon
			return &xferauth.PendingError{Transfers: []xferauth.PendingTransfer{{TransferID: "original", Reason: "receiver_commit_unsettled"}}}
		}, func() { t.Error("unsettled removal reached cascade") })
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
	if err = nc.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, abandon := range []bool{false, true} {
		response, err := RequestSessionRemoval(t.Context(), nc, id.PublicKey, "session", abandon, func(code, detail string) error {
			if code != x.CodeOwnerRecovering || !strings.Contains(detail, "original") || !strings.Contains(detail, "receiver_commit_unsettled") {
				t.Error("session refusal lost pending identity or reason")
			}
			return errors.New(detail)
		})
		if err == nil || response.OK {
			t.Fatal("unsettled session deletion returned success")
		}
		if got := <-seen; got != abandon {
			t.Fatal("session removal changed explicit abandonment authority")
		}
	}
}
