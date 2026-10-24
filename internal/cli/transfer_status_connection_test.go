package cli

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
)

// The member connection remains usable after the settlement role is retired.
// A closed restricted connection models its failed CONNECT after a JS restart;
// the canonical terminal is served over the real, original member connection.
// origin: S7 internal review FIX-TIERA2 F8 / hot-update S7 plan §6.1 / §6.4 / §7 / T46
func TestTransferSettledStatusUsesRetainedMemberConnection(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSettledStatusConnection(t)
	}
	testharness.AssertNoGoroutineLeak(t, "settled status connection", g)
	testharness.AssertNoFDLeak(t, "settled status connection", fd, 0)
}

func exerciseSettledStatusConnection(t *testing.T) {
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
	origin := x.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}
	if err = transport.BindSettlement(x.SettlementCapsule{Identity: id, Origin: origin, ReceiverNkey: receiver.PublicKey, Issuer: "issuer", Nonce: "nonce", Signature: "signed"}); err != nil {
		t.Fatal(err)
	}
	// Retire only the restricted attachment; ordinary membership still authorizes
	// this read. No replacement ID, capsule or settlement authority is minted.
	retired, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	retired.Close()
	transport.settlement.nc = retired
	request := x.Request{Op: x.OpStatus, CallID: "original-status", Identity: id, Origin: origin}
	receipt := x.TerminalReceipt{TerminalID: schema.TransferTerminalID(id.SID, id.TransferID, id.EntryNonce), Kind: "complete", CanonicalDigest: strings.Repeat("b", 64), ReqID: "canonical-request", HistoryStream: "history-session", HistorySeq: 2, Bytes: 1, SHA256: strings.Repeat("c", 64)}
	status := x.Status{State: x.StatusTerminal, Phase: x.PhaseSettled, Identity: id, Origin: origin, Terminal: &receipt}
	body, err := proto.EncodeTransferResponse(request, x.Response{Status: &status})
	if err != nil {
		t.Fatal(err)
	}
	subject, _, err := proto.EncodeTransferRequest(receiver.PublicKey, request)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := control.Subscribe(subject, func(m *nats.Msg) { _ = m.Respond(body) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err := control.Flush(); err != nil {
		t.Fatal(err)
	}
	response, _, err := transport.Request(request, retryDeadline(t, time.Second))
	if err != nil || response.Status == nil || response.Status.Terminal == nil || *response.Status.Terminal != receipt {
		t.Fatalf("settled status remained trapped behind retired settlement CONNECT: %+v %v", response, err)
	}
}
