package proto

import (
	"encoding/json"
	"strings"
	"testing"
)

// P13 was additive (proto stayed v1 through the entire v0.3.x line). The
// distributed-broker HA epic is the first DELIBERATE proto bump: v1→v2 hard
// upgrade (D0, §16.2). This pins the current wire version so an accidental
// bump still trips; intentional bumps update this one line + the version-
// agnostic TestProtoVersionStillPositive.
func TestProtoVersionIsV2(t *testing.T) {
	if ProtoVersion != 2 {
		t.Fatalf("expected proto v2 (distributed-broker D0 hard upgrade), got ProtoVersion=%d", ProtoVersion)
	}
}

// A proxy-disabled NodeRegisterResp must marshal with NO "proxy" key so it is
// byte-identical to a pre-P13 broker reply (the *ProxyDirective pointer is what
// guarantees this — a value type would always emit "proxy":{...}).
func TestNodeRegisterRespProxyOffByteIdentical(t *testing.T) {
	off := NodeRegisterResp{OK: true, KeepPorts: []int{14022}}
	b, err := json.Marshal(off)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "proxy") {
		t.Fatalf("proxy-off NodeRegisterResp must not contain a proxy key: %s", b)
	}

	on := NodeRegisterResp{OK: true, Proxy: &ProxyDirective{Enabled: true, PublicPort: 14000}}
	b2, _ := json.Marshal(on)
	if !strings.Contains(string(b2), `"proxy"`) {
		t.Fatalf("proxy-on NodeRegisterResp must contain a proxy key: %s", b2)
	}
}

func TestProxySubjectBuilders(t *testing.T) {
	const a, s = "UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "lab"
	cases := []struct{ got, want string }{
		{SubjCtrlProxySet(a, s), "tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.set.req"},
		{SubjCtrlProxyStatus(a, s), "tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.status.req"},
		{SubjCtrlProxySubCreate(a, s), "tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.sub.create.req"},
		{SubjCtrlProxySubList(a, s), "tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.sub.list.req"},
		{SubjCtrlProxySubRevoke(a, s), "tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.sub.revoke.req"},
		{SubjEvNodeProxyReady(s, "node1", "ready"), "tether.v2.s.lab.ev.node.node1.proxy.ready"},
		{SubjCmdForwarded(s, "node1", "proxy-keys"), "tether.v2.s.lab.cmd.node.node1.proxy-keys.req.forwarded"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("subject mismatch:\n got  %q\n want %q", c.got, c.want)
		}
	}
}

func TestParseCtrlProxy(t *testing.T) {
	good := []struct {
		subj, action string
	}{
		{"tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.set.req", "set"},
		{"tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.status.req", "status"},
		{"tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.sub.create.req", "sub.create"},
		{"tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.sub.list.req", "sub.list"},
		{"tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.sub.revoke.req", "sub.revoke"},
	}
	for _, g := range good {
		actor, sid, action, ok := ParseCtrlProxy(g.subj)
		if !ok || actor != "UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" || sid != "lab" || action != g.action {
			t.Errorf("ParseCtrlProxy(%q) = (%q,%q,%q,%v), want (UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA,lab,%q,true)",
				g.subj, actor, sid, action, ok, g.action)
		}
	}

	bad := []string{
		"tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.set",           // missing req (too few)
		"tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.set.req.extra", // extra token
		"tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.bogus.req",     // unknown action
		"tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.lab.proxy.sub.bogus.req", // unknown sub action
		"tether.v2.ctrl.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.s.BAD!SID.proxy.set.req",   // bad sid
		"tether.v2.ctrl.by..s.lab.proxy.set.req",                                                               // empty actor
		"tether.v2.s.lab.cmd.node.n.proxy-keys.req.forwarded",                                                  // wrong tree
		// Wrong VERSION: structurally valid but the old v1 prefix — the v2 parser
		// must reject at parts[1] (D0 hard-upgrade, §16.2). Without this the
		// proxy parser's parts[1]!=SubjectVersionToken branch is never tested.
		"tether.v1.ctrl.by." + "UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" + ".s.lab.proxy.set.req",
	}
	for _, b := range bad {
		if _, _, _, ok := ParseCtrlProxy(b); ok {
			t.Errorf("ParseCtrlProxy(%q) should be rejected", b)
		}
	}
}

func TestProxyStructsRoundTrip(t *testing.T) {
	d := ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok", Cipher: "chacha20-ietf-poly1305",
		Keys: []ProxyKey{{SubID: "01H", Secret: "psk"}}, Epoch: 7,
	}
	b, _ := json.Marshal(d)
	var got ProxyDirective
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Epoch != 7 || len(got.Keys) != 1 || got.Keys[0].SubID != "01H" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

// TestProxyOptOutFieldsZeroValueByteEquivalent is the mechanical half of the
// #78 N-1 story: the two new wire fields must be INVISIBLE at their zero value
// (omitempty), so every pre-#78 peer sees byte-identical messages.
// NodeRegisterReq's half is also pinned by the frozen golden fixture; this
// covers ProxyNodeEntry (not in the golden set) and states the property
// explicitly for both.
// origin: docs/deploy-tier-gotchas.md #78
func TestProxyOptOutFieldsZeroValueByteEquivalent(t *testing.T) {
	if b, _ := json.Marshal(NodeRegisterReq{NID: "lab-1"}); strings.Contains(string(b), "proxy_opt_out") {
		t.Fatalf("zero-value ProxyOptOut must be omitted: %s", b)
	}
	if b, _ := json.Marshal(ProxyNodeEntry{NID: "lab-1"}); strings.Contains(string(b), "opted_out") {
		t.Fatalf("zero-value OptedOut must be omitted: %s", b)
	}
	// And the true values round-trip.
	b, _ := json.Marshal(ProxyNodeEntry{NID: "lab-1", OptedOut: true})
	var e ProxyNodeEntry
	if err := json.Unmarshal(b, &e); err != nil || !e.OptedOut {
		t.Fatalf("OptedOut=true must round-trip, got %+v err=%v", e, err)
	}
	b, _ = json.Marshal(NodeRegisterReq{NID: "lab-1", ProxyOptOut: true})
	var r NodeRegisterReq
	if err := json.Unmarshal(b, &r); err != nil || !r.ProxyOptOut {
		t.Fatalf("ProxyOptOut=true must round-trip, got %+v err=%v", r, err)
	}
}
