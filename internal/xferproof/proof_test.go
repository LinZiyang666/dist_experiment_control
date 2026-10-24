package xferproof

import (
	"testing"

	"github.com/nats-io/nkeys"
)

// The same private signer and lineage survive exec, while another signer,
// lineage, subject, inbox or payload cannot reuse the proof.
// origin: hot-update S7 plan §6.3 / T39
func TestTransferInstanceProofBindsLineageAndEntireMessage(t *testing.T) {
	agent, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Wipe()
	first, err := InstanceKey("original", agent.Sign)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Wipe()
	second, err := InstanceKey("original", agent.Sign)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Wipe()
	pub, _ := first.PublicKey()
	same, _ := second.PublicKey()
	if pub != same {
		t.Fatal("exec changed the receiver identity")
	}
	other, err := InstanceKey("replacement", agent.Sign)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Wipe()
	foreign, _ := other.PublicKey()
	if foreign == pub {
		t.Fatal("different lineage reused the original receiver key")
	}
	body := Message("subject.original", "inbox.original", []byte("original"))
	signature, err := Sign(first.Sign, body)
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(pub, signature, body); err != nil {
		t.Fatal(err)
	}
	for _, changed := range [][]byte{
		Message("subject.replacement", "inbox.original", []byte("original")),
		Message("subject.original", "inbox.replacement", []byte("original")),
		Message("subject.original", "inbox.original", []byte("replacement")),
	} {
		if Verify(pub, signature, changed) == nil {
			t.Fatal("a changed request reused the original proof")
		}
	}
	if Verify(foreign, signature, body) == nil {
		t.Fatal("replacement key authenticated an original request")
	}
	binding := Binding("call", "session", "node", "original", pub)
	bound, err := Sign(agent.Sign, binding)
	if err != nil {
		t.Fatal(err)
	}
	agentPub, _ := agent.PublicKey()
	if Verify(agentPub, bound, binding) != nil {
		t.Fatal("original node could not bind its instance")
	}
	for _, changed := range [][]byte{
		Binding("other", "session", "node", "original", pub),
		Binding("call", "other", "node", "original", pub),
		Binding("call", "session", "other", "original", pub),
		Binding("call", "session", "node", "replacement", pub),
		Binding("call", "session", "node", "original", foreign),
	} {
		if Verify(agentPub, bound, changed) == nil {
			t.Fatal("a changed node binding reused the original proof")
		}
	}
	if _, err := InstanceKey("", agent.Sign); err == nil {
		t.Fatal("empty lineage admitted")
	}
	if _, err := InstanceKey("original", nil); err == nil {
		t.Fatal("missing private signer admitted")
	}
}
