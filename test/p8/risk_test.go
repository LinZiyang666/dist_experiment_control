package p8_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// origin: p8_review_risk_test.go (renamed in B6) — docs/reviews/p8-review.md
func TestReviewRegisterRejectsDeletingSession(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	if err := session.Tombstone(db, "lab", time.Now().UTC()); err != nil {
		t.Fatalf("tombstone: %v", err)
	}

	defer startBroker(t, url, db)()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		NID:            "lab-1",
	})
	msg, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.OK || resp.Code != "session_not_found_or_deleting" {
		t.Fatalf("register into DELETING session should be rejected; got OK=%v code=%q err=%q",
			resp.OK, resp.Code, resp.Error)
	}

	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM nodes WHERE sid='lab' AND nid='lab-1'`,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("rejected register must not create/update node row; got %d rows", n)
	}
}

func TestReviewLocalProcessCarriesPIDReuseFields(t *testing.T) {
	typ := reflect.TypeOf(proto.LocalProcess{})
	for _, name := range []string{"StartedAt", "StartTimeTicks"} {
		if _, ok := typ.FieldByName(name); !ok {
			t.Fatalf("proto.LocalProcess missing %s; G.1 cannot verify (boot_id,pid,start_time_ticks)", name)
		}
	}
}
