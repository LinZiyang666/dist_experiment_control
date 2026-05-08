// Package p3_test exercises the P3 session/auth surface end-to-end with
// NATS auth_callout enabled — every CLI CONNECT goes through the broker's
// auth handler, JWT permissions are issued per-connection, and NATS pins
// the `by.<actor>` segment.
package p3_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
)

// ---- Happy paths -----------------------------------------------------------

func TestSessionCreateAndOwnerCanList(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)
	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	id := freshIdentity(t)
	nc := ctlConnUnactivated(t, url, id)

	body, _ := json.Marshal(proto.SessionCreateReq{Name: "lab", PIN: "123456"})
	msg, err := nc.Request(proto.SubjCtrlSessionCreate(id.PublicKey), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.SessionCreateResp
	_ = json.Unmarshal(msg.Data, &resp)
	if resp.Error != "" {
		t.Fatalf("create rejected: %s", resp.Error)
	}
	if resp.SID != "lab" || resp.OwnerFP != id.Fingerprint {
		t.Fatalf("unexpected create resp: %+v (fp=%s)", resp, id.Fingerprint)
	}

	msg, err = nc.Request(proto.SubjCtrlSessionList(id.PublicKey), []byte("{}"), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var lresp proto.SessionListResp
	_ = json.Unmarshal(msg.Data, &lresp)
	if len(lresp.Sessions) != 1 || lresp.Sessions[0].SID != "lab" || !lresp.Sessions[0].IsOwner {
		t.Fatalf("expected sole ownership of 'lab', got %+v", lresp.Sessions)
	}
}

// First-time PIN join now happens at NATS CONNECT (Token=PIN). Successful
// CONNECT means auth_callout verified the PIN AND added the user as a member.
func TestPINJoinAcceptsCorrectPIN(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)
	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	owner := freshIdentity(t)
	mustCreate(t, url, owner, "lab", "secret")

	member := freshIdentity(t)
	nc, err := ctlConnActivated(t, url, member, "lab", "secret")
	if err != nil {
		t.Fatalf("PIN join CONNECT failed: %v", err)
	}

	// member can now session.list and see lab as non-owner.
	msg, err := nc.Request(proto.SubjCtrlSessionList(member.PublicKey), []byte("{}"), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var lresp proto.SessionListResp
	_ = json.Unmarshal(msg.Data, &lresp)
	if len(lresp.Sessions) != 1 || lresp.Sessions[0].SID != "lab" || lresp.Sessions[0].IsOwner {
		t.Fatalf("expected member visibility of lab, got %+v", lresp.Sessions)
	}
}

// Wrong PIN: auth_callout rejects, nats.Connect returns an error.
func TestPINJoinRejectsWrongPIN(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)
	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	owner := freshIdentity(t)
	mustCreate(t, url, owner, "lab", "right-pin")

	intruder := freshIdentity(t)
	if _, err := ctlConnActivated(t, url, intruder, "lab", "wrong-pin"); err == nil {
		t.Fatal("CONNECT must fail for wrong PIN")
	}

	// And intruder also can't list the session (not a member, no PIN).
	if _, err := ctlConnActivated(t, url, intruder, "lab", ""); err == nil {
		t.Fatal("CONNECT must fail for non-member without PIN")
	}
}

func TestSessionRmOwnerOnlyViaConnect(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)
	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	owner := freshIdentity(t)
	mustCreate(t, url, owner, "lab", "p")

	member := freshIdentity(t)
	memberNC, err := ctlConnActivated(t, url, member, "lab", "p")
	if err != nil {
		t.Fatal(err)
	}

	// Member tries rm — broker app-layer reject (not_owner).
	msg, err := memberNC.Request(proto.SubjCtrlSessionRm(member.PublicKey, "lab"),
		[]byte("{}"), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var rresp proto.SessionRmResp
	_ = json.Unmarshal(msg.Data, &rresp)
	if rresp.OK || rresp.Code != "not_owner" {
		t.Fatalf("non-owner rm: got %+v want not_owner", rresp)
	}

	// Owner activates lab, then rm.
	ownerNC, err := ctlConnActivated(t, url, owner, "lab", "")
	if err != nil {
		t.Fatal(err)
	}
	msg, err = ownerNC.Request(proto.SubjCtrlSessionRm(owner.PublicKey, "lab"),
		[]byte("{}"), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rresp = proto.SessionRmResp{}
	_ = json.Unmarshal(msg.Data, &rresp)
	if !rresp.OK {
		t.Fatalf("owner rm: %+v", rresp)
	}
}

// CONNECT to a tombstoned session is rejected by auth_callout.
func TestPINJoinRejectedAfterTombstone(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)
	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	owner := freshIdentity(t)
	mustCreate(t, url, owner, "lab", "p")
	mustTombstone(t, url, owner, "lab")

	newcomer := freshIdentity(t)
	if _, err := ctlConnActivated(t, url, newcomer, "lab", "p"); err == nil {
		t.Fatal("CONNECT must fail for join into tombstoned session")
	}
}

// Multi-shell isolation: TETHER_SESSION env per-shell separate from file.
func TestMultiShellSessionIsolation(t *testing.T) {
	homeA := t.TempDir()
	homeB := t.TempDir()
	if err := cli.WriteCurrentSession(homeA, "lab"); err != nil {
		t.Fatal(err)
	}
	if err := cli.WriteCurrentSession(homeB, "prod"); err != nil {
		t.Fatal(err)
	}
	if got := readWithEnv(t, "TETHER_SESSION", "lab", homeA); got != "lab" {
		t.Errorf("shell A: got %q want lab", got)
	}
	if got := readWithEnv(t, "TETHER_SESSION", "prod", homeB); got != "prod" {
		t.Errorf("shell B: got %q want prod", got)
	}
	if got := readWithEnv(t, "TETHER_SESSION", "", homeA); got != "lab" {
		t.Errorf("shell A no env: should fall back to file 'lab', got %q", got)
	}
}

// Sanity: PIN hashing + verify roundtrip works end-to-end.
func TestArgonRoundtripUsedByBroker(t *testing.T) {
	hash, err := auth.HashPIN("hello")
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.VerifyPIN("hello", hash); err != nil {
		t.Fatal(err)
	}
	if err := auth.VerifyPIN("nope", hash); err == nil {
		t.Fatal("VerifyPIN must reject wrong pin")
	}
}

// ---- helpers --------------------------------------------------------------

func mustCreate(t *testing.T, url string, id *cli.Identity, sid, pin string) {
	t.Helper()
	nc := ctlConnUnactivated(t, url, id)
	body, _ := json.Marshal(proto.SessionCreateReq{Name: sid, PIN: pin})
	msg, err := nc.Request(proto.SubjCtrlSessionCreate(id.PublicKey), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.SessionCreateResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" {
		t.Fatalf("create %s: %s", sid, resp.Error)
	}
}

func mustTombstone(t *testing.T, url string, owner *cli.Identity, sid string) {
	t.Helper()
	nc, err := ctlConnActivated(t, url, owner, sid, "")
	if err != nil {
		t.Fatal(err)
	}
	msg, err := nc.Request(proto.SubjCtrlSessionRm(owner.PublicKey, sid),
		[]byte("{}"), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var rresp proto.SessionRmResp
	_ = json.Unmarshal(msg.Data, &rresp)
	if !rresp.OK {
		t.Fatalf("tombstone %s: %+v", sid, rresp)
	}
}

func readWithEnv(t *testing.T, key, val, home string) string {
	t.Helper()
	prev, hadPrev := os.LookupEnv(key)
	if val == "" {
		_ = os.Unsetenv(key)
	} else {
		_ = os.Setenv(key, val)
	}
	defer func() {
		if hadPrev {
			_ = os.Setenv(key, prev)
		} else {
			_ = os.Unsetenv(key)
		}
	}()
	return cli.ReadCurrentSession(home)
}

