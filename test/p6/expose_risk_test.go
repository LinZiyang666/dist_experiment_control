package p6_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

func TestExposeRmRejectsNonCreatorMember(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)

	ownerPub, ownerFP := freshUserPub(t)
	memberPub, memberFP := freshUserPub(t)
	seed(t, db, "lab", ownerFP)
	if err := session.AddMember(db, "lab", memberFP, session.RoleMember, session.ViaPin, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1", t.TempDir(), &recordingAdapter{})()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	exposed := runExpose(t, nc, "lab", ownerPub, "lab-1", "web", 18080)
	if exposed.Code != "" {
		t.Fatalf("owner expose rejected: %s %s", exposed.Code, exposed.Error)
	}

	rm := runExposeRm(t, nc, "lab", memberPub, "lab-1", "web")
	if rm.OK {
		t.Fatal("non-creator member must not be allowed to remove another member's expose")
	}
	if rm.Code == "" {
		t.Fatal("expected a stable denial code for non-creator expose-rm")
	}
	if _, err := port.LookupByName(db, "lab", "web"); err != nil {
		t.Fatalf("expose row should remain allocated after denied rm: %v", err)
	}
}

// TestExposeResponseDoesNotLeakTunnelTokenToCtl pins the architecture
// F.4 storage rule: only the agent ever sees the raw tunnel token; the
// ctl-facing reply must not carry it. Originally the field existed on
// proto.ExposeResp and was set by the broker — this test failed against
// that. Fix removed the field entirely; the assertion below now both
// (a) compiles only because the field is gone (proto.ExposeResp{}.Token
// would not type-check), and (b) re-checks the wire JSON so a future
// "let's just add it back" regression is caught even if the Go type
// gains a different-name token field.
func TestExposeResponseDoesNotLeakTunnelTokenToCtl(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	adapter := &recordingAdapter{}
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1", t.TempDir(), adapter)()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	body, _ := json.Marshal(proto.ExposeReq{Name: "web", LocalPort: 18080})
	respMsg, err := nc.Request(proto.SubjCmdBy("lab", pub, "lab-1", "expose"), body, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Wire-level check: the JSON response must not contain a token key
	// nor the raw token value the agent received.
	wire := string(respMsg.Data)
	if strings.Contains(wire, `"token"`) {
		t.Fatalf("ctl response leaks a token field: %s", wire)
	}
	added, _ := adapter.snapshot()
	if len(added) != 1 || added[0].Token == "" {
		t.Fatalf("agent adapter must receive the raw token; got %+v", added)
	}
	if strings.Contains(wire, added[0].Token) {
		t.Fatalf("ctl response leaks the agent's raw token verbatim: %s", wire)
	}
}
