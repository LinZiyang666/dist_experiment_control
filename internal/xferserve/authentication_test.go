package xferserve

import (
	"testing"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferproof"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// A payload's claimed instance and an actor-shaped inbox are not principals.
// origin: hot-update S7 plan §6.3 / T39
func TestTransferAgentPrincipalRequiresTheBoundInstanceSignature(t *testing.T) {
	key, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer key.Wipe()
	pub, _ := key.PublicKey()
	id := x.Identity{SID: "session", NID: "node", AgentInstance: "original", ActorFP: "owner"}
	l := &Link{keys: map[string]string{"session/node/original": pub}}
	msg := &nats.Msg{Subject: "subject", Reply: "reply", Data: []byte("outcome"), Header: nats.Header{}}
	sig, err := xferproof.Sign(key.Sign, xferproof.Message(msg.Subject, msg.Reply, msg.Data))
	if err != nil {
		t.Fatal(err)
	}
	msg.Header.Set(xferproof.KeyHeader, pub)
	msg.Header.Set(xferproof.SignatureHeader, sig)
	who, err := l.AgentPrincipal(id, msg, assemblyDeadline(t))
	if err != nil || who.Role != x.RoleAgent || who.InstanceID != id.AgentInstance || who.NKey != pub {
		t.Fatalf("bound instance refused: %+v %v", who, err)
	}
	for _, change := range []func(*nats.Msg){
		func(m *nats.Msg) { m.Header.Del(xferproof.SignatureHeader) },
		func(m *nats.Msg) { m.Header.Set(xferproof.KeyHeader, "forged") },
		func(m *nats.Msg) { m.Data = []byte("different outcome") },
		func(m *nats.Msg) { m.Subject = "other subject" },
		func(m *nats.Msg) { m.Reply = "other inbox" },
	} {
		bad := &nats.Msg{Subject: msg.Subject, Reply: msg.Reply, Data: msg.Data, Header: nats.Header{}}
		bad.Header.Set(xferproof.KeyHeader, pub)
		bad.Header.Set(xferproof.SignatureHeader, sig)
		change(bad)
		if _, err := l.AgentPrincipal(id, bad, assemblyDeadline(t)); err == nil {
			t.Fatal("forged request acquired agent authority")
		}
	}
}
