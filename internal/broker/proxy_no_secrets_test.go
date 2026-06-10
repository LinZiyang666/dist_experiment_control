package broker

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/proxysub"
)

// B3 keystone: no raw subscription token or SS PSK may ever appear on the
// member-readable channels (sys.events, audit.*) or in any control reply.
func TestProxyNoSecretsInMemberChannels(t *testing.T) {
	nc, owner, sid, b := proxyTestBroker(t)

	var mu sync.Mutex
	var captured []string
	collect := func(m *nats.Msg) { mu.Lock(); captured = append(captured, string(m.Data)); mu.Unlock() }
	s1, _ := nc.Subscribe(proto.SubjSysEvents, collect)
	s2, _ := nc.Subscribe("tether.v1.s."+sid+".audit.>", collect)
	defer func() { _ = s1.Unsubscribe(); _ = s2.Unsubscribe() }()

	var setR proto.ProxySetResp
	req(t, nc, proto.SubjCtrlProxySet(owner, sid), proto.ProxySetReq{Enabled: true}, &setR)
	var cR proto.ProxySubCreateResp
	req(t, nc, proto.SubjCtrlProxySubCreate(owner, sid), proto.ProxySubCreateReq{Name: "alice"}, &cR)
	token := cR.SubURL[strings.LastIndex(cR.SubURL, "/sub/")+len("/sub/"):]

	active, _ := proxysub.ListActive(b.cfg.DB, sid)
	if len(active) != 1 {
		t.Fatalf("want 1 active subscriber, got %d", len(active))
	}
	psk := active[0].PSK

	var rR proto.ProxySubRevokeResp
	req(t, nc, proto.SubjCtrlProxySubRevoke(owner, sid), proto.ProxySubRevokeReq{Name: "alice"}, &rR)
	_ = nc.Flush()
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	all := strings.Join(captured, "\n")
	mu.Unlock()
	if len(captured) == 0 {
		t.Fatal("expected to capture some sys.events/audit messages")
	}
	for label, secret := range map[string]string{"sub token": token, "ss psk": psk} {
		if secret == "" {
			t.Fatalf("%s was empty — test can't prove absence", label)
		}
		if strings.Contains(all, secret) {
			t.Fatalf("%s leaked into a member-readable channel:\n%s", label, all)
		}
	}
	// The create reply carries the one-time SubURL (token) but must NOT carry the PSK.
	if strings.Contains(cR.SubURL, psk) {
		t.Fatal("create reply leaked the PSK")
	}
}
