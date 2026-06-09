package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// TestExposeRemotePortClientSideGuard pins the soft floor/ceiling guard
// on --remote-port: a value that can't be a port at all (negative, or
// >65535) is rejected locally with a plain message, BEFORE any NATS
// dial. The band (e.g. 14000-14999) is deliberately NOT checked here —
// it is broker config and an in-65535-range but out-of-band value
// round-trips to the broker as port_out_of_band. We set an active
// session so the RunE reaches the guard (the active-session check is
// the first guard); the guard fires before EnsureIdentity/connect.
func TestExposeRemotePortClientSideGuard(t *testing.T) {
	for _, val := range []string{"-1", "70000"} {
		t.Run(val, func(t *testing.T) {
			home := t.TempDir()
			if err := cli.WriteCurrentSession(home, "lab"); err != nil {
				t.Fatal(err)
			}
			cmd := newExposeCmd()
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			cmd.SetArgs([]string{
				"lab-1", "--local", "8888", "--name", "j",
				"--remote-port", val, "--home", home,
			})
			err := cmd.Execute()
			if err == nil {
				t.Fatalf("--remote-port %s should be rejected client-side", val)
			}
			if !strings.Contains(err.Error(), "--remote-port must be") {
				t.Errorf("want client-side guard message, got %q", err.Error())
			}
		})
	}
}

// TestExposeRemotePortValidValueReachesConnect: an in-65535-range value
// is NOT rejected by the local guard — it must pass the guard and only
// then fail at the NATS dial (band membership is the broker's call, not
// the CLI's). We assert the error is a connect/transport failure, not
// the local "--remote-port must be" guard message.
func TestExposeRemotePortValidValueReachesConnect(t *testing.T) {
	home := t.TempDir()
	if err := cli.WriteCurrentSession(home, "lab"); err != nil {
		t.Fatal(err)
	}
	cmd := newExposeCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	// 13000 is in 1..65535 but (for a default broker) out of band; the
	// CLI must NOT reject it locally — it should reach the broker.
	cmd.SetArgs([]string{
		"lab-1", "--local", "8888", "--name", "j",
		"--remote-port", "13000", "--home", home,
		"--nats-url", "nats://127.0.0.1:1", // unreachable: forces a connect error
	})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected a connect error (no broker), got nil")
	}
	if strings.Contains(err.Error(), "--remote-port must be") {
		t.Errorf("in-range --remote-port must NOT be rejected by the local guard; got %q", err.Error())
	}
}

// TestExposeRemotePortWireThrough runs the real `newExposeCmd` against an
// embedded NATS responder and captures the published ExposeReq, pinning
// the CLI->wire threading at cmd/tether/expose.go (RemotePort: remotePort
// in the json.Marshal). Without this, deleting that field would leave
// every other P12 test green while silently breaking the flag — the
// proto tests only marshal a pre-populated struct, and the p6 e2e tests
// build ExposeReq directly, neither of which exercises the cobra command.
func TestExposeRemotePortWireThrough(t *testing.T) {
	t.Setenv(cli.DevNoAuthEnv, "1") // connect anonymously against the plain test server
	url := testharness.StartNATS(t)

	nc, err := nats.Connect(url, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// Responder: capture the expose request body, reply success so the
	// command returns cleanly. The actor segment (ctl pubkey) is unknown
	// ahead of time, so match it with a wildcard.
	bodyCh := make(chan []byte, 1)
	sub, err := nc.Subscribe(proto.SubjectPrefix+".s.lab.cmd.by.*.node.lab-1.expose.req", func(m *nats.Msg) {
		select {
		case bodyCh <- append([]byte(nil), m.Data...):
		default:
		}
		resp, _ := json.Marshal(proto.ExposeResp{Port: 14005, PublicHost: "test.local", Name: "j"})
		_ = m.Respond(resp)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	_ = nc.Flush()

	runExpose := func(t *testing.T, extraArgs ...string) []byte {
		t.Helper()
		home := t.TempDir()
		if err := cli.WriteCurrentSession(home, "lab"); err != nil {
			t.Fatal(err)
		}
		cmd := newExposeCmd()
		cmd.SetOut(&bytes.Buffer{})
		cmd.SetErr(&bytes.Buffer{})
		args := append([]string{"lab-1", "--local", "8888", "--name", "j", "--nats-url", url, "--home", home}, extraArgs...)
		cmd.SetArgs(args)
		if err := cmd.Execute(); err != nil {
			t.Fatalf("expose execute: %v", err)
		}
		select {
		case b := <-bodyCh:
			return b
		case <-time.After(3 * time.Second):
			t.Fatal("no expose request captured")
			return nil
		}
	}

	// --remote-port 14005 MUST appear in the published body.
	withFlag := runExpose(t, "--remote-port", "14005")
	if !strings.Contains(string(withFlag), `"remote_port":14005`) {
		t.Errorf("--remote-port not threaded to the wire; body=%s", withFlag)
	}
	var got proto.ExposeReq
	if err := json.Unmarshal(withFlag, &got); err != nil {
		t.Fatal(err)
	}
	if got.RemotePort != 14005 {
		t.Errorf("wire RemotePort: got %d want 14005", got.RemotePort)
	}

	// Omitting the flag MUST send no remote_port key at all (omitempty).
	noFlag := runExpose(t)
	if strings.Contains(string(noFlag), "remote_port") {
		t.Errorf("omitted --remote-port must not send a remote_port key; body=%s", noFlag)
	}
}
