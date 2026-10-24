package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: serve_review_test.go (renamed in B6)
func TestServeInvalidProcRetentionDoesNotOpenDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tether.db")
	configPath := filepath.Join(dir, "broker.yaml")
	body := []byte("broker:\n" +
		"  nats:\n" +
		"    url: nats://127.0.0.1:1\n" +
		"  storage:\n" +
		"    db: " + dbPath + "\n" +
		"    proc_retention: -1h\n")
	if err := os.WriteFile(configPath, body, 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := newServeCmd()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{"--config", configPath})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected invalid proc_retention to fail")
	}
	if !strings.Contains(err.Error(), "proc_retention") {
		t.Fatalf("expected proc_retention error, got %v", err)
	}
	if _, statErr := os.Stat(dbPath); !os.IsNotExist(statErr) {
		t.Fatalf("invalid config should fail before opening DB; stat err=%v", statErr)
	}
}

// origin: hot-update S6 internal review rv-cluster-2 / rv-fd-7 / D2
func TestServeInstallsBrokerImageOwners(t *testing.T) {
	source, err := os.ReadFile("serve.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(source, []byte("broker.NewImageRun(")) || !bytes.Contains(source, []byte("cfg.ImageRun, err =")) {
		t.Fatal("serve does not install the production broker image owners")
	}
	if bytes.Contains(source, []byte("restored runtime adapters are not installed")) {
		t.Fatal("serve still refuses every validated broker image")
	}
}

// origin: hot-update S6 plan §4.1 / T03
func TestServeLostDispatchNeverOpensDB(t *testing.T) {
	// No parallel subtest may replace the process's single bootstrap owner.
	agentboot.Reset()
	t.Cleanup(agentboot.Reset)
	t.Setenv("_TETHER_HANDOFF", "malformed")
	img := agentboot.Begin(nil)
	if img == nil || img.Phase() != agentboot.PhaseLost {
		t.Fatal("fixture did not exercise W1 dispatch cleanup")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tether.db")
	cmd := newServeCmd()
	cmd.SetArgs([]string{"--db", dbPath, "--nats-url=nats://127.0.0.1:1", "--admin-socket=", "--tunnel-addr="})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	err := cmd.ExecuteContext(t.Context())
	if err == nil || !strings.Contains(err.Error(), "refusing cold start after unreadable dispatch") {
		t.Fatalf("lost dispatch reached a fresh broker: %v", err)
	}
	for _, path := range []string{dbPath, filepath.Join(dir, "tether.lock")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("lost dispatch created store or lock %s: %v", filepath.Base(path), err)
		}
	}
}

// origin: hot-update S6 plan §2.1 / §4.1 / T03
func TestServeInvalidLoggerDoesNotAcquireStore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tether.db")
	cmd := newServeCmd()
	cmd.SetArgs([]string{"--db", dbPath, "--log-level=invalid", "--admin-socket=", "--tunnel-addr="})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.ExecuteContext(t.Context()); err == nil || !strings.Contains(err.Error(), "--log-level") {
		t.Fatalf("invalid logger should fail before acquiring a store: %v", err)
	}
	for _, path := range []string{dbPath, filepath.Join(dir, "tether.lock")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("invalid logger created store or lock %s: %v", filepath.Base(path), err)
		}
	}
}

// origin: hot-update S6 plan §3.1 / §4.2 / T02
func TestServeSingleStoreCleanupOnRunFailure(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseServeSingleStoreFailure(t)
	}
	testharness.AssertNoGoroutineLeak(t, "single serve startup failure", beforeG)
	testharness.AssertNoFDLeak(t, "single serve startup failure", beforeFD, 0)
}

func exerciseServeSingleStoreFailure(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	cmd := newServeCmd()
	cmd.SetArgs([]string{"--db", dbPath, "--nats-url=nats://127.0.0.1:1", "--admin-socket=", "--tunnel-addr="})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	if err := cmd.ExecuteContext(t.Context()); err == nil || !strings.Contains(err.Error(), "broker: NATS connect:") {
		t.Fatalf("fixture did not reach the actual Run connect failure: %v", err)
	}
	identity, err := storage.CanonicalStoreIdentity(dbPath, "")
	if err != nil {
		t.Fatal(err)
	}
	if identity.Inode == 0 {
		t.Fatal("fixture never opened its real cold store")
	}
	if writers, readers := storage.DefaultDriverRegistry().Connections(identity); writers != 0 || readers != 0 {
		t.Fatalf("Run failure retained SQL drivers: writers=%d readers=%d", writers, readers)
	}
	release, err := cluster.AcquireDataDirLock(dir)
	if err != nil {
		t.Fatalf("Run failure retained the single-store lock: %v", err)
	}
	release()
}

// origin: hot-update S6 plan §3.1 / T06
func TestBrokerPreflightCommandUsesPureServeValidation(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker reader probes require Linux image support")
	}
	dir := t.TempDir()
	dbPath, configPath := filepath.Join(dir, "absent.db"), filepath.Join(dir, "broker.yaml")
	config := []byte("broker:\n  storage:\n    db: " + dbPath + "\n")
	if err := os.WriteFile(configPath, config, 0600); err != nil {
		t.Fatal(err)
	}
	// Obtain a real schema receipt from a separate fixture, then keep the
	// production destination absent. The reader must never migrate/open it.
	fixturePath := filepath.Join(dir, "fixture.db")
	db, err := storage.Open(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := storage.CanonicalStoreIdentity(fixturePath, "")
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := storage.ReadStoreStamp(db, identity)
	if closeErr := db.Close(); err != nil || closeErr != nil {
		t.Fatalf("schema fixture: %v / %v", err, closeErr)
	}
	request := brokerimage.ProbeRequest{Config: config, AllowedArgs: []string{"tether", "serve", "--config", configPath}, Schema: stamp.Schema, RequiredDomains: []string{brokerimage.Domain}}
	for _, variant := range []string{"valid", "wrong-config", "invalid-log-level", "invalid-argv", "wrong-migrations", "duplicate-config"} {
		t.Run(variant, func(t *testing.T) {
			probe := request
			probe.AllowedArgs = append([]string(nil), request.AllowedArgs...)
			switch variant {
			case "valid":
			case "wrong-config":
				probe.Config = []byte("broker: {}\n")
			case "invalid-log-level":
				probe.AllowedArgs = append(probe.AllowedArgs, "--log-level=invalid")
			case "invalid-argv":
				probe.AllowedArgs[1] = "version"
			case "duplicate-config":
				probe.AllowedArgs = append(probe.AllowedArgs, "--config", configPath)
			case "wrong-migrations":
				probe.Schema.Supported = probe.Schema.Supported[1:]
			default:
				t.Fatal("unknown probe control")
			}
			input, err := json.Marshal(probe)
			if err != nil {
				t.Fatal(err)
			}
			cmd := newBrokerImagePreflightCmd()
			cmd.SetArgs(nil)
			cmd.SetIn(bytes.NewReader(input))
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(io.Discard)
			err = cmd.ExecuteContext(t.Context())
			if (variant == "valid") != (err == nil) {
				t.Fatalf("reader control %s: %v", variant, err)
			}
			if err == nil {
				var reply brokerimage.ProbeReply
				if err := json.Unmarshal(output.Bytes(), &reply); err != nil || reply.Proof.SHA256 == "" || reply.ConfigDigest == "" {
					t.Fatalf("reader did not emit a bound proof: %v", err)
				}
			}
			for _, path := range []string{dbPath, filepath.Join(dir, storage.StoreLockFile)} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("pure reader created %s: %v", filepath.Base(path), err)
				}
			}
		})
	}
}

// The `:memory:` cold-start regression lives in serve_cold_store_compat_test.go
// (TestServeColdSingleMemoryStoreReachesRun); FIX-RUN wrote the same test here.
