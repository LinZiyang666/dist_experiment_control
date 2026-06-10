package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// Round-2 F6: prove the real `tether proxy ...` cobra commands publish real
// NATS requests on the right subjects with the right bodies (an embedded
// responder captures them) — not just local validation. Mirrors the P12
// expose WireThrough pattern.
func TestProxyCLIWiresToNATS(t *testing.T) {
	t.Setenv(cli.DevNoAuthEnv, "1")
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	capture := func(leaf string, reply any) <-chan []byte {
		ch := make(chan []byte, 1)
		sub, err := nc.Subscribe(proto.SubjectPrefix+".ctrl.by.*.s.lab."+leaf, func(m *nats.Msg) {
			select {
			case ch <- append([]byte(nil), m.Data...):
			default:
			}
			b, _ := json.Marshal(reply)
			_ = m.Respond(b)
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
		return ch
	}

	onCh := capture("proxy.set.req", proto.ProxySetResp{OK: true, Enabled: true, AffectedNodes: 2})
	subCh := capture("proxy.sub.create.req", proto.ProxySubCreateResp{OK: true, Name: "alice", SubURL: "https://b/sub/tok"})
	revCh := capture("proxy.sub.revoke.req", proto.ProxySubRevokeResp{OK: true, Name: "alice"})
	statusCh := capture("proxy.status.req", proto.ProxyStatusResp{Enabled: true})
	lsCh := capture("proxy.sub.list.req", proto.ProxySubListResp{})
	_ = nc.Flush()

	run := func(t *testing.T, args ...string) {
		t.Helper()
		home := t.TempDir()
		if err := cli.WriteCurrentSession(home, "lab"); err != nil {
			t.Fatal(err)
		}
		root := newProxyCmd()
		root.SetOut(&bytes.Buffer{})
		root.SetErr(&bytes.Buffer{})
		root.SetArgs(append(args, "--nats-url", url, "--home", home))
		if err := root.Execute(); err != nil {
			t.Fatalf("proxy %v: %v", args, err)
		}
	}

	// `proxy on --yes` publishes ProxySetReq{Enabled:true}.
	run(t, "on", "--yes")
	assertBody(t, onCh, func(b []byte) bool {
		var r proto.ProxySetReq
		return json.Unmarshal(b, &r) == nil && r.Enabled
	}, "proxy on should wire Enabled:true")

	// `proxy off` publishes ProxySetReq{Enabled:false}.
	run(t, "off")
	assertBody(t, onCh, func(b []byte) bool {
		var r proto.ProxySetReq
		return json.Unmarshal(b, &r) == nil && !r.Enabled
	}, "proxy off should wire Enabled:false")

	// `proxy sub create --name alice` wires the name.
	run(t, "sub", "create", "--name", "alice")
	assertBody(t, subCh, func(b []byte) bool {
		var r proto.ProxySubCreateReq
		return json.Unmarshal(b, &r) == nil && r.Name == "alice"
	}, "sub create should wire Name")

	// `proxy sub revoke alice` (positional) wires the name.
	run(t, "sub", "revoke", "alice")
	assertBody(t, revCh, func(b []byte) bool {
		var r proto.ProxySubRevokeReq
		return json.Unmarshal(b, &r) == nil && r.Name == "alice"
	}, "sub revoke should wire Name")

	// `proxy status` and `proxy sub ls` publish their requests too (round-3 F6).
	run(t, "status")
	assertBody(t, statusCh, func(b []byte) bool { return true }, "proxy status should reach the broker")
	run(t, "sub", "ls")
	assertBody(t, lsCh, func(b []byte) bool { return true }, "proxy sub ls should reach the broker")
}

func assertBody(t *testing.T, ch <-chan []byte, ok func([]byte) bool, msg string) {
	t.Helper()
	select {
	case b := <-ch:
		if !ok(b) {
			t.Fatalf("%s; body=%s", msg, b)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("%s; no request captured", msg)
	}
}
