package xferauth

import (
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nkeys"
)

// origin: hot-update S7 plan §6.1 / T26
func TestTransferPermitPersistsOriginalAuthorityAcrossSigners(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(time.Duration(round).String(), func(t *testing.T) { exercisePermitAuthority(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "durable permit authority", g)
	testharness.AssertNoFDLeak(t, "durable permit authority", fd, 0)
}
func exercisePermitAuthority(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	issuer, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer issuer.Wipe()
	origin := x.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}
	now := time.Now().UTC()
	a, err := NewPermitAuthority(root, origin, issuer, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewPermitAuthority(root, origin, issuer, func() time.Time { return now.Add(time.Hour) })
	if err != nil {
		t.Fatal(err)
	}
	id := x.Identity{SID: "lab", NID: "node", ActorFP: "fp", AgentInstance: "agent", TransferID: "transfer"}
	intent := x.Intent{Tier: "b", Verb: "pull", Path: "target", ChunkSize: 32768}
	if _, err := a.Issue(intent, id.TransferID, id.AgentInstance, authDeadline(t)); err == nil {
		t.Fatal("unscoped signer minted authority")
	}
	var got [2]x.CreationPermit
	var errs [2]error
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	deadline := authDeadline(t)
	for i, p := range []*PermitAuthority{a, b} {
		go func() { defer wg.Done(); <-start; got[i], errs[i] = p.IssueFor(id, intent, deadline) }()
	}
	close(start)
	wg.Wait()
	if errs[0] != nil || errs[1] != nil || !reflect.DeepEqual(got[0], got[1]) {
		t.Fatalf("concurrent issuers changed authority: %v %v equal=%v", errs[0], errs[1], reflect.DeepEqual(got[0], got[1]))
	}
	again, err := b.IssueFor(id, intent, authDeadline(t))
	if err != nil || again != got[0] {
		t.Fatalf("replay renewed permit: %v", err)
	}
	changed := id
	changed.ActorFP = "foreign"
	if _, err := a.IssueFor(changed, intent, authDeadline(t)); err == nil {
		t.Fatal("foreign actor reused permit")
	}
	intent.Force = true
	if _, err := a.IssueFor(id, intent, authDeadline(t)); err == nil {
		t.Fatal("changed intent reused permit")
	}
	forged := got[0]
	forged.Identity.ObjectVersion = "replacement"
	if err := a.Verify(forged); err == nil {
		t.Fatal("forged permit verified")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 {
		t.Fatalf("permit files=%d err=%v", len(entries), err)
	}
	if err := os.WriteFile(filepath.Join(root, entries[0].Name()), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	intent.Force = false
	if _, err := a.IssueFor(id, intent, authDeadline(t)); err == nil {
		t.Fatal("unreadable retained permit recreated")
	}
}

type controlLedger struct {
	x.LedgerPort
	census x.LedgerCensus
}

func (l controlLedger) Scan(x.Deadline) (x.LedgerCensus, error) { return l.census, nil }

// The retained-row witness tests authentication only. T39 uses the real ledger.
// origin: hot-update S7 plan §6.4 / T31 / T39
func TestTransferControlAuthorityRechecksMembershipBeforeStale(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	actorKey, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer actorKey.Wipe()
	actor, err := actorKey.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES('lab','lab',?,'pin')", fp); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("INSERT INTO members(sid,pubkey_fp,role,via) VALUES('lab',?,'owner','pin')", fp); err != nil {
		t.Fatal(err)
	}
	id := x.Identity{SID: "lab", NID: "node", ActorFP: fp, AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry"}
	calls := 0
	a := ControlAuthority{SQL: func() *sql.DB { return db }, NodeGate: func(_, _ string, accepted bool) string {
		calls++
		if !accepted {
			return "node_offline"
		}
		return ""
	}}
	if err := a.Check(id, x.AccessContinue, authDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := a.Check(id, x.AccessCreate, authDeadline(t)); err == nil {
		t.Fatal("STALE admitted creation")
	}
	if _, err := a.Principal(actor, id, authDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("DELETE FROM members"); err != nil {
		t.Fatal(err)
	}
	before := calls
	if err := a.Check(id, x.AccessContinue, authDeadline(t)); err == nil || calls != before {
		t.Fatal("revoked member reached STALE exception")
	}
	if _, err := a.Principal(actor, id, authDeadline(t)); err == nil {
		t.Fatal("cached NATS permission replaced current membership")
	}
	receiver, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Wipe()
	pub, err := receiver.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	origin := x.Origin{BrokerID: "b", InstanceID: "i", BootID: "boot", Generation: 1}
	row := x.LedgerRow{Identity: id, Origin: origin, EntryNonce: id.EntryNonce, Revision: 1, Phase: x.PhaseCommitReserved, Settlement: &x.SettlementCapsule{Identity: id, Origin: origin, ReceiverNkey: pub}, Grant: &x.CommitGrant{Identity: id, ReceiverInstance: "original-receiver"}}
	census := x.LedgerCensus{Ready: true, Complete: true, Rows: []x.LedgerRow{row}}
	a.Ledger = func() x.LedgerPort { return controlLedger{census: census} }
	who, err := a.Principal(pub, id, authDeadline(t))
	if err != nil || who.Role != x.RoleSettlement || who.InstanceID != "original-receiver" {
		t.Fatalf("retained settlement principal=%+v %v", who, err)
	}
	if err := a.Check(id, x.AccessSettle, authDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := a.Check(id, x.AccessContinue, authDeadline(t)); err == nil {
		t.Fatal("settlement reopened data admission")
	}
	if _, err := a.Principal(actor, id, authDeadline(t)); err == nil {
		t.Fatal("foreign receiver borrowed capsule")
	}

	// Administrative release must invalidate permissions cached at CONNECT too.
	// origin: S7 internal review rv-wiring F5
	census.Rows[0].OutcomeUnknown = true
	if _, err := a.Principal(pub, id, authDeadline(t)); err == nil {
		t.Fatal("abandoned capsule still authorized a receiver")
	}
	if err := a.Check(id, x.AccessSettle, authDeadline(t)); err == nil {
		t.Fatal("abandonment left cached settlement permission active")
	}
	census.Rows[0].OutcomeUnknown = false
	census.Complete = false
	if _, err := a.Principal(pub, id, authDeadline(t)); err == nil {
		t.Fatal("incomplete census renewed settlement")
	}
}
