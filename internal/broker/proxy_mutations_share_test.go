package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/proxysub"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// origin: p13_external_review_round8_test.go (renamed in B6) — docs/reviews/p13-external-review-round8.md
func TestExternalReviewProxyMutationsShareSerializationLock(t *testing.T) {
	nc, owner, sid, b := proxyTestBroker(t)

	b.proxyOpMu.Lock()
	locked := true
	defer func() {
		if locked {
			b.proxyOpMu.Unlock()
		}
	}()

	type response struct {
		msg *nats.Msg
		err error
	}
	offCh := make(chan response, 1)
	subCh := make(chan response, 1)

	go func() {
		body, _ := json.Marshal(proto.ProxySetReq{Enabled: false})
		msg, err := nc.Request(proto.SubjCtrlProxySet(owner, sid), body, 2*time.Second)
		offCh <- response{msg: msg, err: err}
	}()
	go func() {
		body, _ := json.Marshal(proto.ProxySubCreateReq{Name: "serialized"})
		msg, err := nc.Request(proto.SubjCtrlProxySubCreate(owner, sid), body, 2*time.Second)
		subCh <- response{msg: msg, err: err}
	}()

	time.Sleep(100 * time.Millisecond)
	select {
	case r := <-offCh:
		t.Fatalf("proxy off bypassed mutation serialization: err=%v msg=%v", r.err, r.msg)
	default:
	}
	select {
	case r := <-subCh:
		t.Fatalf("subscriber create bypassed mutation serialization: err=%v msg=%v", r.err, r.msg)
	default:
	}
	if subs, err := proxysub.ListBySession(b.cfg.DB, sid); err != nil {
		t.Fatal(err)
	} else if len(subs) != 0 {
		t.Fatalf("subscriber mutation committed while serialization lock held: %+v", subs)
	}

	b.proxyOpMu.Unlock()
	locked = false

	for name, ch := range map[string]<-chan response{"off": offCh, "sub.create": subCh} {
		select {
		case r := <-ch:
			if r.err != nil {
				t.Fatalf("%s request failed after unlock: %v", name, r.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s request remained blocked after unlock", name)
		}
	}

	enabled, err := session.GetProxyEnabled(b.cfg.DB, sid)
	if err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Fatal("proxy off did not commit after serialization lock was released")
	}
}
