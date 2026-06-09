package p6_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// subAllocatedEvent subscribes (sync) to the ev.port "allocated" subject
// for one port; the returned assert() fails the test if any such event
// arrived. Used to pin that a REJECTED expose emits NO "allocated"
// lifecycle event (the other half of the plan's reject-path invariant,
// alongside "does not forward to the agent").
func subAllocatedEvent(t *testing.T, nc *nats.Conn, sid string, port int) (assert func()) {
	t.Helper()
	sub, err := nc.SubscribeSync(proto.SubjEvPort(sid, port, "allocated"))
	if err != nil {
		t.Fatalf("subscribe ev.port allocated: %v", err)
	}
	return func() {
		t.Helper()
		_ = nc.Flush()
		if msg, err := sub.NextMsg(200 * time.Millisecond); err == nil {
			t.Errorf("reject path published an 'allocated' ev.port for %d: %s", port, string(msg.Data))
		}
		_ = sub.Unsubscribe()
	}
}

// runExposeRemote is runExpose with an explicit --remote-port
// (RemotePort). remotePort==0 reproduces the auto path.
func runExposeRemote(t *testing.T, nc *nats.Conn, sid, actor, nid, name string, local, remotePort int) proto.ExposeResp {
	t.Helper()
	body, _ := json.Marshal(proto.ExposeReq{Name: name, LocalPort: local, RemotePort: remotePort})
	subj := proto.SubjCmdBy(sid, actor, nid, "expose")
	respMsg, err := nc.Request(subj, body, 3*time.Second)
	if err != nil {
		t.Fatalf("expose request: %v", err)
	}
	var resp proto.ExposeResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatalf("expose response parse: %v", err)
	}
	return resp
}

// TestExposeHonorsRequestedRemotePort: --remote-port 14002 is honored
// even though 14000 is the lowest free in the test band 14000-14002, so
// a pass can only mean the requested port was used, not auto-picked.
func TestExposeHonorsRequestedRemotePort(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1", t.TempDir(), &recordingAdapter{})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	resp := runExposeRemote(t, nc, "lab", pub, "lab-1", "jupyter", 8888, 14002)
	if resp.Code != "" {
		t.Fatalf("expose rejected: %s %s", resp.Code, resp.Error)
	}
	if resp.Port != 14002 {
		t.Errorf("requested 14002, got %d", resp.Port)
	}
	a, err := port.LookupByName(db, "lab", "jupyter")
	if err != nil {
		t.Fatalf("lookup by name: %v", err)
	}
	if a.Port != 14002 {
		t.Errorf("DB row port: got %d want 14002", a.Port)
	}
}

// TestExposeRequestedPortTakenHardFails: requesting an already-ALLOCATED
// port hard-fails with port_taken — no fallback to another free port,
// no clobber of the original, and the reject path does NOT forward to
// the agent (adapter saw exactly one AddProxy).
func TestExposeRequestedPortTakenHardFails(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	adapter := &recordingAdapter{}
	defer startAgent(t, url, "lab", "lab-1", t.TempDir(), adapter)()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	first := runExpose(t, nc, "lab", pub, "lab-1", "a", 8000)
	if first.Code != "" || first.Port != 14000 {
		t.Fatalf("first expose: code=%q port=%d (want code='' port=14000)", first.Code, first.Port)
	}

	// Watch for any (wrongful) "allocated" ev.port on the requested port.
	// Subscribe AFTER the first expose (whose own allocated event already
	// fired) so only a NEW event from the rejected request would be seen.
	noAlloc := subAllocatedEvent(t, nc, "lab", 14000)

	second := runExposeRemote(t, nc, "lab", pub, "lab-1", "b", 8001, 14000)
	if second.Code != "port_taken" {
		t.Errorf("want port_taken, got code=%q port=%d", second.Code, second.Port)
	}
	if second.Port != 0 {
		t.Errorf("rejected expose must not carry a port, got %d", second.Port)
	}

	// Reject path must short-circuit before the agent forward AND before
	// the "allocated" audit/broadcast.
	added, _ := adapter.snapshot()
	if len(added) != 1 {
		t.Errorf("reject must not forward to agent: AddProxy calls=%d want 1", len(added))
	}
	noAlloc()

	// Original allocation untouched, still on 14000.
	a, err := port.LookupByName(db, "lab", "a")
	if err != nil {
		t.Fatalf("original lookup: %v", err)
	}
	if a.Port != 14000 {
		t.Errorf("original allocation clobbered: port=%d want 14000", a.Port)
	}
}

// TestExposeRequestedPortOutOfBand: a port below or above the broker's
// band is rejected with port_out_of_band. 15000 is a valid TCP port
// (passes any 1..65535 client guard) but out of the 14000-14002 band,
// so the broker is the authority here.
func TestExposeRequestedPortOutOfBand(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	adapter := &recordingAdapter{}
	defer startAgent(t, url, "lab", "lab-1", t.TempDir(), adapter)()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	for _, p := range []int{13999, 15000} {
		noAlloc := subAllocatedEvent(t, nc, "lab", p)
		resp := runExposeRemote(t, nc, "lab", pub, "lab-1", fmt.Sprintf("n-%d", p), 8000, p)
		if resp.Code != "port_out_of_band" {
			t.Errorf("remote-port %d: want port_out_of_band, got code=%q", p, resp.Code)
		}
		noAlloc()
	}

	// Reject path must never forward to the agent (no AddProxy at all).
	added, _ := adapter.snapshot()
	if len(added) != 0 {
		t.Errorf("out-of-band reject must not forward to agent: AddProxy calls=%d want 0", len(added))
	}
}

// TestExposeRequestedPortReusableAfterRm: a port whose only history is
// FREED (via expose rm) is grantable again by --remote-port — proving
// REVOKED/FREED rows do not block.
func TestExposeRequestedPortReusableAfterRm(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1", t.TempDir(), &recordingAdapter{})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	a := runExposeRemote(t, nc, "lab", pub, "lab-1", "a", 8000, 14001)
	if a.Code != "" || a.Port != 14001 {
		t.Fatalf("first expose: code=%q port=%d", a.Code, a.Port)
	}
	rm := runExposeRm(t, nc, "lab", pub, "lab-1", "a")
	if !rm.OK {
		t.Fatalf("expose rm failed: %s", rm.Code)
	}
	b := runExposeRemote(t, nc, "lab", pub, "lab-1", "b", 8001, 14001)
	if b.Code != "" || b.Port != 14001 {
		t.Errorf("reuse after rm: code=%q port=%d (want code='' port=14001)", b.Code, b.Port)
	}
}

// TestExposeOmittedRemotePortUnchanged: RemotePort==0 (omitted flag)
// still allocates the lowest-free port — the additive field is inert
// when zero.
func TestExposeOmittedRemotePortUnchanged(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1", t.TempDir(), &recordingAdapter{})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	resp := runExposeRemote(t, nc, "lab", pub, "lab-1", "j", 8888, 0)
	if resp.Code != "" || resp.Port != 14000 {
		t.Errorf("omitted remote-port: code=%q port=%d (want code='' port=14000)", resp.Code, resp.Port)
	}
}
