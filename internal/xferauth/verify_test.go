package xferauth

import (
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nkeys"
)

type censusFunc func(xfercontract.Origin, controlbudget.Deadline) (xfercontract.LedgerCensus, error)

func (f censusFunc) Census(o xfercontract.Origin, d controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
	return f(o, d)
}

func authFixture(t *testing.T) (xfercontract.SettlementCapsule, xfercontract.NKeyProof, xfercontract.LedgerCensus, xfercontract.IssuerVerifier) {
	t.Helper()
	issuer, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(issuer.Wipe)
	receiver, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(receiver.Wipe)
	pub, _ := receiver.PublicKey()
	c, err := SignCapsule(issuer, xfercontract.SettlementCapsule{
		Identity: xfercontract.Identity{SID: "lab", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "object"},
		Origin:   xfercontract.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 0}, ReceiverNkey: pub, Nonce: "capsule"})
	if err != nil {
		t.Fatal(err)
	}
	nonce := []byte("fresh-server-challenge")
	sig, err := receiver.Sign(nonce)
	if err != nil {
		t.Fatal(err)
	}
	proof := xfercontract.NKeyProof{NKey: pub, Nonce: nonce, Signature: sig}
	row := xfercontract.LedgerRow{Identity: c.Identity, Origin: c.Origin, EntryNonce: c.Identity.EntryNonce, Revision: 1, Phase: xfercontract.PhaseCommitReserved, Settlement: &c}
	census := xfercontract.LedgerCensus{Origin: c.Origin, Ready: true, Complete: true, Rows: []xfercontract.LedgerRow{row}}
	iv, err := NewIssuerVerifier([]string{c.Issuer})
	if err != nil {
		t.Fatal(err)
	}
	return c, proof, census, iv
}
func authDeadline(t *testing.T) controlbudget.Deadline {
	t.Helper()
	d, e := controlbudget.After(time.Second)
	if e != nil {
		t.Fatal(e)
	}
	return d
}

// origin: hot-update S7 plan §6.4 / T31
func TestSettlementCapsuleRequiresReceiverAndDurableEntry(t *testing.T) {
	c, p, census, iv := authFixture(t)
	v, err := New(Config{Issuers: iv, Owners: censusFunc(func(o xfercontract.Origin, d controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
		return census, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	got, err := v.Verify(c, p, authDeadline(t))
	if err != nil || got.Identity != c.Identity || got.ReceiverNkey != c.ReceiverNkey {
		t.Fatalf("valid retained receiver denied: %v", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*xfercontract.SettlementCapsule, *xfercontract.NKeyProof)
	}{
		{"actor", func(c *xfercontract.SettlementCapsule, _ *xfercontract.NKeyProof) { c.Identity.ActorFP = "thief" }},
		{"transfer", func(c *xfercontract.SettlementCapsule, _ *xfercontract.NKeyProof) { c.Identity.TransferID = "other" }},
		{"nonce", func(c *xfercontract.SettlementCapsule, _ *xfercontract.NKeyProof) { c.Identity.EntryNonce = "new" }},
		{"origin", func(c *xfercontract.SettlementCapsule, _ *xfercontract.NKeyProof) { c.Origin.Generation++ }},
		{"signature", func(c *xfercontract.SettlementCapsule, _ *xfercontract.NKeyProof) { c.Signature = "bogus" }},
		{"receiver", func(_ *xfercontract.SettlementCapsule, p *xfercontract.NKeyProof) {
			kp, _ := nkeys.CreateUser()
			defer kp.Wipe()
			p.NKey, _ = kp.PublicKey()
			p.Signature, _ = kp.Sign(p.Nonce)
		}},
		{"proof", func(_ *xfercontract.SettlementCapsule, p *xfercontract.NKeyProof) { p.Signature = []byte("bogus") }},
		{"challenge", func(_ *xfercontract.SettlementCapsule, p *xfercontract.NKeyProof) {
			p.Nonce = []byte("old-server-challenge")
		}},
		{"subject_injection", func(c *xfercontract.SettlementCapsule, _ *xfercontract.NKeyProof) { c.Identity.TransferID = "*" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc, pp := c, p
			tc.change(&cc, &pp)
			if _, err := v.Verify(cc, pp, authDeadline(t)); !errors.Is(err, ErrDenied) {
				t.Fatalf("forged capsule/proof accepted: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*xfercontract.LedgerCensus)
	}{
		{"unready", func(c *xfercontract.LedgerCensus) { c.Ready = false }},
		{"incomplete", func(c *xfercontract.LedgerCensus) { c.Complete = false }},
		{"wrong_origin", func(c *xfercontract.LedgerCensus) { c.Origin.Generation++ }},
		{"retired", func(c *xfercontract.LedgerCensus) { c.Rows[0].Phase = xfercontract.PhaseSettled }},
		{"absent", func(c *xfercontract.LedgerCensus) { c.Rows = nil }},
		{"wrong_actor", func(c *xfercontract.LedgerCensus) { c.Rows[0].Identity.ActorFP = "other" }},
		{"wrong_nonce", func(c *xfercontract.LedgerCensus) { c.Rows[0].EntryNonce = "other" }},
		{"no_capsule", func(c *xfercontract.LedgerCensus) { c.Rows[0].Settlement = nil }},
		{"duplicate", func(c *xfercontract.LedgerCensus) { c.Rows = append(c.Rows, c.Rows[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saved := census
			census.Rows = append([]xfercontract.LedgerRow(nil), saved.Rows...)
			defer func() { census = saved }()
			tc.change(&census)
			if _, err := v.Verify(c, p, authDeadline(t)); !errors.Is(err, ErrDenied) {
				t.Fatalf("unproven responsibility accepted: %v", err)
			}
		})
	}
	if _, err := v.Verify(c, p, 0); err == nil {
		t.Fatal("zero deadline accepted")
	}
	if err := v.Stop(authDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(c, p, authDeadline(t)); !errors.Is(err, ErrStopped) {
		t.Fatal("stopped verifier accepted reconnect")
	}
}

// origin: hot-update S7 plan §6.4 / T31
func TestSettlementIssuerTrustDoesNotComeFromCapsule(t *testing.T) {
	c, p, _, _ := authFixture(t)
	untrusted, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer untrusted.Wipe()
	pub, _ := untrusted.PublicKey()
	iv, err := NewIssuerVerifier([]string{pub})
	if err != nil {
		t.Fatal(err)
	}
	if err := iv.Verify(c, p); !errors.Is(err, ErrDenied) {
		t.Fatal("untrusted issuer accepted")
	}
	for _, keys := range [][]string{nil, {"not-a-key"}, {p.NKey + "*"}} {
		if _, err := NewIssuerVerifier(keys); err == nil {
			t.Fatal("invalid issuer config accepted")
		}
	}
	if _, err := New(Config{}); err == nil {
		t.Fatal("missing authority accepted")
	}
}

// Administrative abandonment retires the settlement capsule as well as the
// terminal slot; a revoked receiver cannot regain authority through CONNECT.
// origin: S7 internal review rv-wiring F5 / hot-update S7 plan lead LQ1
func TestSettlementAbandonmentClosesRestrictedReconnect(t *testing.T) {
	c, proof, census, issuer := authFixture(t)
	census.Rows[0].OutcomeUnknown = true
	v, err := New(Config{Issuers: issuer, Owners: censusFunc(func(xfercontract.Origin, controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
		return census, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Stop(authDeadline(t)); err != nil {
			t.Error(err)
		}
	}()
	if _, err := v.Verify(c, proof, authDeadline(t)); !errors.Is(err, ErrDenied) {
		t.Fatalf("abandoned receiver regained settlement authority: %v", err)
	}
}
