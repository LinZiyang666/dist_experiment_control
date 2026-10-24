package chaos_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// This exercises the agent's own refusal, independently of broker policy.
// origin: hot-update S8 plan §5.2 / §7.2
func exerciseAgentUpgradeUnfencedURL(t *testing.T) {
	t.Helper()
	ctx, cancel := withTestDeadline(t, 10*time.Second)
	defer cancel()

	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	_ = startBrokerExplicit(t, url, db)
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte("unauthorized archive"))
	}))
	defer srv.Close()
	executable := filepath.Join(t.TempDir(), "tether")
	const original = "original sandbox executable"
	if err := os.WriteFile(executable, []byte(original), 0o755); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(executable)
	if err != nil {
		t.Fatal(err)
	}

	// Boot agent with a tightly-scoped allowlist that won't match the
	// URL we send.
	a, err := agent.New(agent.Config{
		NATSURL:               url,
		SID:                   "lab",
		NID:                   "n1",
		Logger:                silentLog(),
		HeartbeatInterval:     100 * time.Millisecond,
		RegisterTimeout:       1500 * time.Millisecond,
		UpgradeURLAllowlist:   []string{"https://only-this-host.invalid/"},
		UpgradeNoExit:         true,
		UpgradeExecutablePath: executable,
	})
	if err != nil {
		t.Fatal(err)
	}
	agentCtx, cancelAgent := context.WithCancel(ctx)
	agentDone := make(chan error, 1)
	go func() { agentDone <- a.Run(agentCtx) }()
	defer func() {
		cancelAgent()
		select {
		case err := <-agentDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("agent did not stop")
		}
	}()

	// Wait for register.
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// Hit the agent's forwarded subject directly with a known-bad URL.
	// We're not going through the broker's owner-only gate for this
	// test — the focus is the agent's local defense.
	body, err := json.Marshal(proto.UpgradeReq{URL: srv.URL + "/x.tar.gz", SHA256: strings.Repeat("0", 64), ActorFP: "x", ProtoVersion: proto.ProtoVersion})
	if err != nil {
		t.Fatal(err)
	}
	subj := proto.SubjCmdForwarded("lab", "n1", "upgrade")

	// An actual reply proves subscription readiness; a startup race cannot Skip
	// the local guard, and broker admission cannot conceal its removal.
	var respMsg *nats.Msg
	testharness.WaitFor(t, 3*time.Second, 10*time.Millisecond, func() bool {
		respMsg, err = nc.Request(subj, body, 100*time.Millisecond)
		return err == nil
	})
	if err != nil || respMsg == nil {
		t.Fatalf("agent forwarded subscriber not ready: %v", err)
	}
	var response struct {
		OK             bool   `json:"ok"`
		Code           string `json:"code"`
		Error          string `json:"error"`
		BinaryReplaced bool   `json:"binary_replaced"`
	}
	if err := json.Unmarshal(respMsg.Data, &response); err != nil {
		t.Fatal(err)
	}
	if response.OK || response.BinaryReplaced || response.Code != "upgrade_deferred" || !strings.Contains(response.Error, "target-issued upgrade transaction is required") {
		t.Fatalf("unfenced URL did not refuse before download: %s", respMsg.Data)
	}
	if requests.Load() != 0 {
		t.Fatalf("unauthorized URL was fetched %d times", requests.Load())
	}
	after, err := os.Stat(executable)
	data, readErr := os.ReadFile(executable)
	if err != nil || readErr != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() || string(data) != original {
		t.Fatalf("refusal changed executable: stat=%v read=%v", err, readErr)
	}
	for _, path := range []string{executable + ".prev", filepath.Join(filepath.Dir(executable), ".tether-upgrade.json")} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("refusal created recovery evidence %s: %v", path, err)
		}
	}
}
