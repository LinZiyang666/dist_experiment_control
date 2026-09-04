package cli_e2e_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// callSessionCreate fires session.create.req and returns the parsed
// response. A successful response yields resp.SID == name (broker
// uses Name as both SID and display name in the current proto).
func callSessionCreate(t *testing.T, nc *nats.Conn, actor, name, pin string) proto.SessionCreateResp {
	t.Helper()
	body, _ := json.Marshal(proto.SessionCreateReq{Name: name, PIN: pin})
	respMsg, err := nc.Request(proto.SubjCtrlSessionCreate(actor), body, 3*time.Second)
	if err != nil {
		t.Fatalf("session.create.req: %v", err)
	}
	var resp proto.SessionCreateResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatalf("session.create decode: %v", err)
	}
	return resp
}

func callSessionList(t *testing.T, nc *nats.Conn, actor string) proto.SessionListResp {
	t.Helper()
	respMsg, err := nc.Request(proto.SubjCtrlSessionList(actor), []byte("{}"), 3*time.Second)
	if err != nil {
		t.Fatalf("session.list.req: %v", err)
	}
	var resp proto.SessionListResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatalf("session.list decode: %v", err)
	}
	return resp
}

func callSessionRm(t *testing.T, nc *nats.Conn, actor, sid string) proto.SessionRmResp {
	t.Helper()
	respMsg, err := nc.Request(proto.SubjCtrlSessionRm(actor, sid), []byte("{}"), 3*time.Second)
	if err != nil {
		t.Fatalf("session.rm.req: %v", err)
	}
	var resp proto.SessionRmResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatalf("session.rm decode: %v", err)
	}
	return resp
}

// A.1 — full lifecycle: create → ls → rm → ls.
func TestSessionFullLifecycle(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	admitCreator(t, db, fp)

	defer startBroker(t, url, db)()

	nc := connect(t, url)
	defer nc.Close()

	// Step 1: create.
	cr := callSessionCreate(t, nc, pub, "alpha", "test-pin-1234")
	if cr.Error != "" {
		t.Fatalf("create: %s", cr.Error)
	}
	if cr.SID != "alpha" {
		t.Errorf("created SID: got %q want alpha", cr.SID)
	}

	// Step 2: ls — owner sees their own session.
	ls := callSessionList(t, nc, pub)
	if ls.Error != "" {
		t.Fatalf("list: %s", ls.Error)
	}
	found := false
	for _, s := range ls.Sessions {
		if s.SID == "alpha" {
			found = true
			if !s.IsOwner {
				t.Errorf("creator must be owner; got is_owner=false")
			}
			if s.State != "ACTIVE" {
				t.Errorf("fresh session state: got %q want ACTIVE", s.State)
			}
		}
	}
	if !found {
		t.Errorf("ls did not include just-created session: %+v", ls.Sessions)
	}

	// Step 3: rm.
	rm := callSessionRm(t, nc, pub, "alpha")
	if !rm.OK {
		t.Fatalf("rm rejected: %s %s", rm.Code, rm.Error)
	}

	// Step 4: ls — session must no longer be ACTIVE. The rm finalize
	// runs synchronously inside the rm handler, so by the time the OK
	// reply lands the row is gone (audit shard 01: H.3 phases ②③④).
	if !testharness.WaitFor(t, 2*time.Second, 50*time.Millisecond, func() bool {
		ls := callSessionList(t, nc, pub)
		for _, s := range ls.Sessions {
			if s.SID == "alpha" {
				return false
			}
		}
		return true
	}) {
		t.Errorf("session 'alpha' still in ls output 2s after rm OK")
	}
}

// A.2 — create + same-name re-create. Because the broker uses Name
// as the SID and the SID is UNIQUE, the second create MUST fail with
// a clear error. (We pin this as the contract — if it ever started
// silently allocating a fresh SID the call sites that use ReadCurrent
// Session would silently target a stale sid.)
func TestSessionCreateDuplicateNameFails(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	admitCreator(t, db, fp)

	defer startBroker(t, url, db)()

	nc := connect(t, url)
	defer nc.Close()

	first := callSessionCreate(t, nc, pub, "dup", "pin-1234")
	if first.Error != "" {
		t.Fatalf("first create: %s", first.Error)
	}
	second := callSessionCreate(t, nc, pub, "dup", "pin-1234")
	if second.Error == "" {
		t.Errorf("second create with same name MUST return an error; got resp=%+v", second)
	}
	if !strings.Contains(strings.ToLower(second.Error), "already_exists") {
		t.Errorf("error code: got %q want already_exists", second.Error)
	}
}

// A.3 — rm then create same SID with a NEW owner. After the broker's
// finalize cleans the row out, a brand-new owner can claim that SID.
func TestSessionRmThenRecreateSameSID(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	ownerA, fpA := freshUserPub(t)
	ownerB, fpB := freshUserPub(t)
	admitCreator(t, db, fpA)
	admitCreator(t, db, fpB)

	defer startBroker(t, url, db)()
	nc := connect(t, url)
	defer nc.Close()

	if r := callSessionCreate(t, nc, ownerA, "lab", "pinA"); r.Error != "" {
		t.Fatalf("first create: %s", r.Error)
	}
	if r := callSessionRm(t, nc, ownerA, "lab"); !r.OK {
		t.Fatalf("rm by ownerA: %s %s", r.Code, r.Error)
	}

	// Wait until the SQLite row is fully gone; finalize runs sync but
	// poll for resilience against CI scheduler jitter.
	if !testharness.WaitFor(t, 2*time.Second, 50*time.Millisecond, func() bool {
		_, err := session.Get(db, "lab")
		return err == session.ErrNotFound
	}) {
		t.Fatal("sessions row still present 2s after rm")
	}

	r := callSessionCreate(t, nc, ownerB, "lab", "pinB")
	if r.Error != "" {
		t.Fatalf("re-create same SID by new owner: %s", r.Error)
	}
}

// A.4 — a member who joined someone else's session must not be able
// to rm it. Server-side check is session.IsOwner; the protocol code
// is "not_owner".
func TestSessionRmNonOwnerRejected(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	owner, ownerFP := freshUserPub(t)
	memberPub, memberFP := freshUserPub(t)

	// Seed both rows directly so we don't care about the join flow.
	seedSession(t, db, "lab", ownerFP)
	if err := session.AddMember(db, "lab", memberFP, session.RoleMember, session.ViaPin, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	_ = owner

	defer startBroker(t, url, db)()
	nc := connect(t, url)
	defer nc.Close()

	r := callSessionRm(t, nc, memberPub, "lab")
	if r.OK {
		t.Errorf("member rm must be rejected; got OK")
	}
	if r.Code != "not_owner" {
		t.Errorf("rm code: got %q want not_owner", r.Code)
	}
}

// A.5 — owner rm followed by member ps must return
// session_not_found_or_deleting (same gate handleExecReq applies).
func TestPsAfterRmRejectsMember(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	owner, ownerFP := freshUserPub(t)
	memberPub, memberFP := freshUserPub(t)
	seedSession(t, db, "lab", ownerFP)
	if err := session.AddMember(db, "lab", memberFP, session.RoleMember, session.ViaPin, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()
	nc := connect(t, url)
	defer nc.Close()

	r := callSessionRm(t, nc, owner, "lab")
	if !r.OK {
		t.Fatalf("rm by owner: %s %s", r.Code, r.Error)
	}

	// Issue ps as the member. The session row may already be gone
	// (finalize is sync), in which case the gate path is the IsActive
	// false branch returning session_not_found_or_deleting. Either
	// shape is fine; assert the code.
	body, _ := json.Marshal(proto.PsReq{})
	resp, err := nc.Request(proto.SubjCtrlPs(memberPub, "lab"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var ps proto.PsResp
	_ = json.Unmarshal(resp.Data, &ps)
	if ps.Code != "session_not_found_or_deleting" {
		t.Errorf("ps after rm: got %q want session_not_found_or_deleting", ps.Code)
	}
}

// A.6 — ls must show both ACTIVE and DELETING sessions (the operator
// needs visibility into what's mid-tear-down). We force the DELETING
// state by direct Tombstone (handleSessionRm's finalize would
// immediately drop the row).
func TestSessionListIncludesDeletingState(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	admitCreator(t, db, fp)

	seedSession(t, db, "alive", fp)
	seedSession(t, db, "doomed", fp)
	if err := session.Tombstone(db, "doomed", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()
	nc := connect(t, url)
	defer nc.Close()

	ls := callSessionList(t, nc, pub)
	if ls.Error != "" {
		t.Fatalf("list: %s", ls.Error)
	}
	got := map[string]string{}
	for _, s := range ls.Sessions {
		got[s.SID] = s.State
	}
	if got["alive"] != "ACTIVE" {
		t.Errorf("alive state: got %q want ACTIVE", got["alive"])
	}
	if got["doomed"] != "DELETING" {
		t.Errorf("doomed state: got %q want DELETING", got["doomed"])
	}
}
