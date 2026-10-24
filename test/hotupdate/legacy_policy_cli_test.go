//go:build linux

package hotupdate_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/hotupdate/testsupervisor"
	"github.com/nats-io/nats.go"
)

// The actual previous ctl must translate the product's projected codes. A
// synthetic error responder would only prove the CLI table, leaving the unsafe
// old request -> new broker -> legacy agent forwarding seam untested.
// origin: hot-update S8 plan §7.1 / §7.2 / P24
// origin: S8 internal review rv-n1 F1
// origin: S8 internal review upgrade-maketest-2 F1
func TestLegacyUpgradePolicyWithOldCLI(t *testing.T) {
	previous := previousUpgradeArtifact(t)
	candidate := buildHotUpdateArtifact(t, "v0.0.0-s8-policy")
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, arm := range []string{"busy", "idle-unfenced"} {
			t.Run(fmt.Sprintf("%d/%s", round, arm), func(t *testing.T) { exerciseLegacyPolicyCLI(t, previous, candidate, arm) })
		}
	}
	testharness.AssertNoGoroutineLeak(t, "real old ctl policy x5", g)
	testharness.AssertNoFDLeak(t, "real old ctl policy x5", fd, 0)
}

func exerciseLegacyPolicyCLI(t *testing.T, previous string, candidate hotUpdateArtifact, arm string) {
	t.Helper()
	var downloads atomic.Int64
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { downloads.Add(1); _, _ = w.Write(candidate.Tarball) }))
	t.Cleanup(httpServer.Close)
	db := testharness.OpenDB(t)
	home := t.TempDir()
	id, err := cli.EnsureIdentity(home)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.WriteCurrentSession(home, "lab"); err != nil {
		t.Fatal(err)
	}
	authorityCreate(t, db, id.Fingerprint, 1)
	url, nc := authorityBroker(t, db, httpServer.URL)
	copyPath := filepath.Join(t.TempDir(), "tether-prev")
	data, err := os.ReadFile(previous)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(copyPath, data, 0o755); err != nil {
		t.Fatal(err)
	}
	agentHome := t.TempDir()
	agent := startHotUpdateProcess(t, copyPath, append(hotUpdateEnvironment(t), "TETHER_HOME="+agentHome), "agent", "--session", "lab", "--nid", "node", "--nats-url", url, "--log-file", "-", "--upgrade-url-allow", httpServer.URL)
	authorityAwait(t, func() bool {
		select {
		case <-agent.Done:
			t.Fatalf("previous agent exited before register: %s", agent.Output.Bytes())
		default:
		}
		var count int
		return db.QueryRow(`SELECT COUNT(*) FROM nodes WHERE sid='lab' AND nid='node' AND status='ONLINE'`).Scan(&count) == nil && count == 1
	})
	if hotUpdateFileSHA(t, fmt.Sprintf("/proc/%d/exe", agent.Cmd.Process.Pid)) != hotUpdateFileSHA(t, previous) {
		t.Fatal("previous agent was not the supplied executable")
	}
	var forwarded atomic.Int64
	sub, err := nc.Subscribe(proto.SubjCmdForwarded("lab", "node", "upgrade"), func(*nats.Msg) { forwarded.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	// v0.6.0 renders its static try_again hint instead of the broker's detail.
	// Observe the exact reply delivered to that ctl; do not substitute a second
	// request, a fake responder or modified historical executable for this arm.
	replies := make(chan []byte, 1)
	replySubject := ""
	wire, err := nc.Subscribe(">", func(m *nats.Msg) {
		if m.Subject == proto.SubjCmdBy("lab", id.PublicKey, "node", "upgrade") {
			replySubject = m.Reply
		} else if replySubject != "" && m.Subject == replySubject {
			select {
			case replies <- bytes.Clone(m.Data):
			default:
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wire.Unsubscribe() })
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	var original *hotUpdateProcess
	progressGate := filepath.Join(t.TempDir(), "continue-original")
	if arm == "busy" {
		original = startHotUpdateProcess(t, copyPath, hotUpdateEnvironment(t), "run", "--home", home, "--nats-url", url, "node", "--", "/bin/sh", "-c", `printf policy-ready; while [ ! -e "$1" ]; do sleep 0.01; done; printf policy-after; exec sleep 60`, "sh", progressGate)
		authorityAwait(t, func() bool { return bytes.Contains(original.Output.Bytes(), []byte("policy-ready")) })
		// Output can overtake proc.started. Require the real broker's positive
		// busy witness before asking it to distinguish rejection from deferral.
		authorityAwait(t, func() bool {
			var count int
			return db.QueryRow(`SELECT COUNT(*) FROM processes WHERE sid='lab' AND nid='node' AND status='RUNNING'`).Scan(&count) == nil && count > 0
		})
	}
	cmd := exec.CommandContext(t.Context(), copyPath, "node", "upgrade", "--home", home, "--nats-url", url, "--url", httpServer.URL+"/candidate.tar.gz", "--sha256", candidate.TarSHA256, "node")
	cmd.Env = hotUpdateEnvironment(t)
	out, err := testsupervisor.CombinedOutput(cmd)
	wantRC, wantCode, wantReason := 75, "try_again", "legacy_quiescence_unproved"
	if arm == "busy" {
		wantRC, wantCode, wantReason = 64, "bad_request", "legacy_busy"
	}
	if err == nil || cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != wantRC || !bytes.Contains(out, []byte(wantCode)) {
		t.Fatalf("old ctl %s projection: require rc=%d %s/%s; got %v %s", arm, wantRC, wantCode, wantReason, err, out)
	}
	select {
	case raw := <-replies:
		var response proto.UpgradeResp
		if err := json.Unmarshal(raw, &response); err != nil || response.OK || response.Code != wantCode || response.Error != wantReason {
			t.Fatalf("old ctl %s received wrong policy detail: %s (%v)", arm, raw, err)
		}
	case <-time.After(time.Second):
		t.Fatal("old ctl policy reply was not observed")
	}
	if downloads.Load() != 0 || forwarded.Load() != 0 {
		t.Fatal("legacy policy refused after dispatch/download")
	}
	if hotUpdateFileSHA(t, copyPath) != hotUpdateFileSHA(t, previous) || hotUpdateFileSHA(t, fmt.Sprintf("/proc/%d/exe", agent.Cmd.Process.Pid)) != hotUpdateFileSHA(t, previous) {
		t.Fatal("rejection changed legacy disk or running image")
	}
	// Ordinary business remains admitted in both arms after the refusal.
	business := exec.CommandContext(t.Context(), copyPath, "exec", "--home", home, "--nats-url", url, "--timeout", "5s", "node", "--", "/bin/sh", "-c", "printf policy-ordinary")
	business.Env = hotUpdateEnvironment(t)
	if out, err := testsupervisor.CombinedOutput(business); err != nil || !bytes.Contains(out, []byte("policy-ordinary")) {
		t.Fatalf("ordinary legacy business stopped after refusal: %v %s", err, out)
	}
	if original != nil {
		if err := os.WriteFile(progressGate, []byte("continue"), 0o600); err != nil {
			t.Fatal(err)
		}
		authorityAwait(t, func() bool { return bytes.Contains(original.Output.Bytes(), []byte("policy-after")) })
		select {
		case <-original.Done:
			t.Fatal("policy refusal interrupted original legacy task")
		default:
		}
		// Exercise the previous sender's actual non-TTY SIGINT cancellation too.
		if err := original.Cmd.Process.Signal(syscall.SIGINT); err != nil {
			t.Fatal(err)
		}
		select {
		case <-original.Done:
		case <-time.After(5 * time.Second):
			t.Fatal("old ctl cancellation did not join")
		}
	}
}
