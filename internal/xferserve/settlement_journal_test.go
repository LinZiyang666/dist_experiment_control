package xferserve

import (
	"encoding/json"
	"os"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nkeys"
)

// origin: S7 internal review s7-p9m-open F2 / hot-update S7 plan §6.4 / T31
func TestTransferCapsuleJournalRejectsRetainedForgery(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCapsuleJournalForgery(t)
	}
	testharness.AssertNoGoroutineLeak(t, "capsule journal forgery", g)
	testharness.AssertNoFDLeak(t, "capsule journal forgery", fd, 0)
}

func exerciseCapsuleJournalForgery(t *testing.T) {
	issuer, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer issuer.Wipe()
	receiver, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Wipe()
	pub, _ := receiver.PublicKey()
	s := &CapsuleStore{Root: t.TempDir(), issuer: issuer}
	l := &Link{Issuer: issuer, Capsules: s}
	id := x.Identity{SID: "lab", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: "digest"}
	origin := x.Origin{BrokerID: "broker", InstanceID: "original", BootID: "boot", Generation: 1}
	c, err := l.ReceiverCapsule(id, origin, pub, assemblyDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := l.ReceiverCapsule(id, origin, pub, assemblyDeadline(t))
	if err != nil || repeated != c {
		t.Fatal("pull preparation replaced the original receiver capsule")
	}
	row := x.LedgerRow{Identity: id, Origin: origin, EntryNonce: id.EntryNonce, Revision: 1, Phase: x.PhaseOpen}
	if retained, err := s.row(row); err != nil || retained.Settlement == nil || *retained.Settlement != c {
		t.Fatal("original durable capsule did not verify")
	}
	other, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer other.Wipe()
	for _, change := range []func(*x.SettlementCapsule){
		func(c *x.SettlementCapsule) { c.ReceiverNkey, _ = other.PublicKey() },
		func(c *x.SettlementCapsule) { c.Nonce += "changed" },
		func(c *x.SettlementCapsule) { c.Signature = "forged" },
		func(c *x.SettlementCapsule) { c.Origin.InstanceID = "another" },
	} {
		bad := c
		change(&bad)
		data, _ := json.Marshal(bad)
		if err := os.WriteFile(s.path(id), data, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := s.row(row); err == nil {
			t.Fatal("valid JSON replaced retained settlement authority")
		}
	}
}
