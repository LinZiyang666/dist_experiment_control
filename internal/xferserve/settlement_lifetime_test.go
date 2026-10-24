package xferserve

import (
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// origin: S7 internal review s7-p9m-open F2 / hot-update S7 plan §8.2
func TestTransferSettlementReconnectJoinsRemoteLookup(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseRemoteSettlementJoin(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "remote settlement join", g)
	testharness.AssertNoFDLeak(t, "remote settlement join", fd, 0)
}

func exerciseRemoteSettlementJoin(t *testing.T) {
	_, js := assemblyHost(t, t.TempDir())
	directory, err := openDirectory(js, 1, assemblyDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer issuer.Wipe()
	pub, _ := issuer.PublicKey()
	original := x.Origin{BrokerID: "foreign", InstanceID: "original", BootID: "boot", Generation: 1}
	if err := directory.register(original, pub, assemblyDeadline(t)); err != nil {
		t.Fatal(err)
	}
	receiver, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Wipe()
	receiverPub, _ := receiver.PublicKey()
	capsule, err := xferauth.SignCapsule(issuer, x.SettlementCapsule{Identity: x.Identity{SID: "lab", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "entry", EntryNonce: "nonce", IntentDigest: "digest"}, Origin: original, ReceiverNkey: receiverPub, Nonce: "capsule"})
	if err != nil {
		t.Fatal(err)
	}
	trusted, err := xferauth.NewIssuerVerifier([]string{pub})
	if err != nil {
		t.Fatal(err)
	}
	local, err := xferauth.New(xferauth.Config{Issuers: trusted, Owners: originCensus{}})
	if err != nil {
		t.Fatal(err)
	}
	v := &settlementReconnect{local: local, owners: Owners{Origin: x.Origin{BrokerID: "local"}}, directory: directory, issuer: pub, conn: func() *nats.Conn { return js.Conn() }}
	entered, release := make(chan struct{}), make(chan struct{})
	sub, err := js.Conn().Subscribe(continuationAuthoritySubject, func(m *nats.Msg) {
		close(entered)
		<-release
		_ = m.Respond(marshal(authorityReply{Code: x.CodeOwnerRecovering}))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := js.Conn().FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	nonce := []byte("server-challenge")
	sig, _ := receiver.Sign(nonce)
	result := make(chan error, 1)
	d := assemblyDeadline(t)
	go func() {
		_, err := v.Verify(capsule, x.NKeyProof{NKey: receiverPub, Nonce: nonce, Signature: sig}, d)
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("remote lookup did not enter")
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		<-result
	}()
	short, err := controlbudget.After(30 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Stop(short); err == nil {
		t.Fatal("settlement Stop retired a verifier with an in-flight remote lookup")
	}
	close(release)
	err = <-result
	result <- err
	if err == nil {
		t.Fatal("refused remote lookup granted authority")
	}
	if err := v.Stop(assemblyDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(capsule, x.NKeyProof{NKey: receiverPub, Nonce: nonce, Signature: sig}, assemblyDeadline(t)); err == nil {
		t.Fatal("stopped verifier reopened remote authentication")
	}
}
