package xferserve

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nkeys"
)

// A result can outlive its active ledger row. Its original signed permit is
// still read authority; resolving it must neither mint a replacement nor lose
// a binding supplied by the query.
// origin: S7 internal review MAKETEST-TRANSFER F7; S7 plan §6.1 / T28
func TestTransferStatusResolvesRetainedLocalPermit(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRetainedLocalStatus(t)
	}
	testharness.AssertNoGoroutineLeak(t, "local status authority", g)
	testharness.AssertNoFDLeak(t, "local status authority", fd, 0)
}

func exerciseRetainedLocalStatus(t *testing.T) {
	t.Helper()
	issuer, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer issuer.Wipe()
	root := t.TempDir()
	origin := x.Origin{BrokerID: "single", InstanceID: "instance", BootID: "boot", Generation: 1}
	permits, err := xferauth.NewPermitAuthority(root, origin, issuer, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	id := x.Identity{SID: "lab", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "retained"}
	permit, err := permits.IssueFor(id, x.Intent{Verb: "push", Tier: "a", Path: "destination"}, assemblyDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	owners := Owners{Origin: origin, Permits: permits, PermitRoot: root}
	query := x.Request{Op: x.OpStatus, CallID: "status", Identity: x.Identity{SID: id.SID, NID: id.NID, ActorFP: id.ActorFP, TransferID: id.TransferID}}
	resolved, ok := ResolveOrigin(owners, query, assemblyDeadline(t))
	if !ok || resolved.Identity != permit.Identity || resolved.Origin != origin {
		t.Fatalf("retained signed creation was lost after ledger retirement: ok=%v identity=%+v", ok, resolved.Identity)
	}
	foreign := owners
	foreign.Origin.Generation++
	if _, ok := ResolveOrigin(foreign, query, assemblyDeadline(t)); ok {
		t.Fatal("local status borrowed another original origin")
	}
	for _, field := range []string{"node", "actor", "agent", "nonce", "intent", "object"} {
		bad := query
		switch field {
		case "node":
			bad.Identity.NID = "other"
		case "actor":
			bad.Identity.ActorFP = "other"
		case "agent":
			bad.Identity.AgentInstance = "other"
		case "nonce":
			bad.Identity.EntryNonce = "other"
		case "intent":
			bad.Identity.IntentDigest = "other"
		case "object":
			bad.Identity.ObjectVersion = "other"
		}
		if _, ok := ResolveOrigin(owners, bad, assemblyDeadline(t)); ok {
			t.Fatalf("status replaced supplied %s authority", field)
		}
	}
	sum := sha256.Sum256([]byte(id.SID + "\n" + id.TransferID))
	path := filepath.Join(root, hex.EncodeToString(sum[:])+".json")
	forged := permit
	forged.Signature = "forged-signature"
	data, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := ResolveOrigin(owners, query, assemblyDeadline(t)); ok {
		t.Fatal("local status trusted an unauthenticated retained permit")
	}
	if err := os.WriteFile(path, []byte(`{"identity":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := ResolveOrigin(owners, query, assemblyDeadline(t)); ok {
		t.Fatal("corrupt creation authority became a status answer")
	}
}
