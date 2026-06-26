package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nkeys"
)

// agent_cli_test.go (C2 Stage-C T1) — behavioral tests of the join/doctor/config-refresh acceptance
// surface (the CLI IS the acceptance surface). They pin the security-load-bearing invariants:
// authenticated-first-dial (fail-loud, persist-nothing on a forged/missing seed), no-TOFU-from-HTTP,
// PIN-never-persisted, and the doctor read-only / forged-manifest-FATAL guarantees.

func accountKeypair(t *testing.T) (seed []byte, pub string) {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	seed, _ = kp.Seed()
	pub, _ = kp.PublicKey()
	return seed, pub
}

// signedManifestServer serves a manifest whose roster + seeds claim AccountPub=claimedPub but are
// SIGNED by signSeed. For a valid manifest signSeed's pub == claimedPub; for a forged one they differ.
func signedManifestServer(t *testing.T, signSeed []byte, claimedPub string, rosterGen, seedGen uint64, endpoints []string) *httptest.Server {
	t.Helper()
	now := time.Now().UTC()
	r, err := clusterroster.Build(signSeed, claimedPub, rosterGen,
		[]proto.RosterBroker{{NodeID: "b1", PublicHost: "b1.example.com", Phase: proto.RosterPhaseVoter}},
		now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	s, err := clusterroster.BuildSeeds(signSeed, claimedPub, seedGen, endpoints, "https://c/c.json",
		now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(proto.ClusterManifest{SchemaVersion: 1, Roster: r, Seeds: s})
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
}

func TestAgentJoinFailsLoudWithoutDialableURL(t *testing.T) {
	home := t.TempDir()
	t.Setenv("TETHER_HOME", home)
	t.Setenv("TETHER_DEV_NO_AUTH", "1") // allow http(s) httptest bootstrap in tests
	seed, pub := accountKeypair(t)
	_ = seed
	foreign, _ := accountKeypair(t)
	srv := signedManifestServer(t, foreign, pub, 1, 1, []string{"wss://b1:443"}) // FORGED (foreign-signed)
	defer srv.Close()
	inv, err := clusterroster.MintInvite(clusterroster.Invite{Pin: pub, BootstrapURL: srv.URL, SID: "lab"}) // no Seed=
	if err != nil {
		t.Fatal(err)
	}
	cmd := newAgentJoinCmd()
	cmd.SetArgs([]string{inv, "--nid", "lab-1"})
	cmd.SetContext(context.Background())
	if cmd.Execute() == nil {
		t.Fatal("join must FAIL LOUD when no authenticated dialable URL can be derived")
	}
	if _, serr := os.Stat(filepath.Join(home, "agent", "lab", "agent.yaml")); !os.IsNotExist(serr) {
		t.Fatal("join must persist NOTHING on failure")
	}
}

func TestAgentJoinValidWritesConfigAndCache(t *testing.T) {
	home := t.TempDir()
	t.Setenv("TETHER_HOME", home)
	t.Setenv("TETHER_DEV_NO_AUTH", "1") // allow http(s) httptest bootstrap in tests
	seed, pub := accountKeypair(t)
	srv := signedManifestServer(t, seed, pub, 5, 6, []string{"wss://b1.example.com:443"})
	defer srv.Close()
	inv, _ := clusterroster.MintInvite(clusterroster.Invite{Pin: pub, BootstrapURL: srv.URL, SID: "lab"}) // no Seed= → uses verified endpoints
	cmd := newAgentJoinCmd()
	cmd.SetArgs([]string{inv, "--nid", "lab-1"})
	cmd.SetContext(context.Background())
	if err := cmd.Execute(); err != nil {
		t.Fatalf("valid join must succeed: %v", err)
	}
	ay, err := loadAgentYAML(home, "lab")
	if err != nil {
		t.Fatal(err)
	}
	if ay.BrokerURL != "wss://b1.example.com:443" {
		t.Fatalf("broker_url must be the VERIFIED endpoint: %q", ay.BrokerURL)
	}
	if ay.AccountPub != pub {
		t.Fatalf("account_pub must be pinned: %q", ay.AccountPub)
	}
	rc, _ := agent.ReadRosterCache(home, "lab")
	if rc == nil || rc.Generation != 5 || rc.SeedGen != 6 {
		t.Fatalf("roster_cache.json must be pre-warmed: %+v", rc)
	}
}

func TestAgentJoinNeverPersistsPIN(t *testing.T) {
	home := t.TempDir()
	t.Setenv("TETHER_HOME", home)
	t.Setenv("TETHER_DEV_NO_AUTH", "1") // allow http(s) httptest bootstrap in tests
	seed, pub := accountKeypair(t)
	srv := signedManifestServer(t, seed, pub, 1, 1, []string{"wss://b1:443"})
	defer srv.Close()
	inv, _ := clusterroster.MintInvite(clusterroster.Invite{Pin: pub, BootstrapURL: srv.URL, SID: "lab", Seed: "wss://b1:443"})
	cmd := newAgentJoinCmd()
	cmd.SetArgs([]string{inv, "--nid", "lab-1", "--pin", "SUPERSECRETPIN"})
	cmd.SetContext(context.Background())
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	body, _ := os.ReadFile(filepath.Join(home, "agent", "lab", "agent.yaml"))
	if strings.Contains(string(body), "SUPERSECRETPIN") {
		t.Fatal("agent.yaml must NEVER contain the session PIN")
	}
}

func TestAgentConfigRefreshRefusesFirstPinFromHTTP(t *testing.T) {
	home := t.TempDir()
	t.Setenv("TETHER_HOME", home)
	t.Setenv("TETHER_DEV_NO_AUTH", "1") // allow http(s) httptest bootstrap in tests
	seed, pub := accountKeypair(t)
	srv := signedManifestServer(t, seed, pub, 1, 1, []string{"wss://b1:443"})
	defer srv.Close()
	if err := writeAgentConfig(home, "lab", "lab-1", "wss://b1:443", "", srv.URL); err != nil { // NO account_pub
		t.Fatal(err)
	}
	cmd := newAgentConfigRefreshCmd()
	cmd.SetArgs([]string{"--session", "lab", "--once"})
	cmd.SetContext(context.Background())
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "refusing to TOFU-pin") {
		t.Fatalf("refresh must refuse a first pin from HTTP, got: %v", err)
	}
}

func TestAgentConfigRefreshHappyPath(t *testing.T) {
	home := t.TempDir()
	t.Setenv("TETHER_HOME", home)
	t.Setenv("TETHER_DEV_NO_AUTH", "1") // allow http(s) httptest bootstrap in tests
	seed, pub := accountKeypair(t)
	srv := signedManifestServer(t, seed, pub, 9, 9, []string{"wss://b1:443"})
	defer srv.Close()
	if err := writeAgentConfig(home, "lab", "lab-1", "wss://b1:443", pub, srv.URL); err != nil {
		t.Fatal(err)
	}
	cmd := newAgentConfigRefreshCmd()
	cmd.SetArgs([]string{"--session", "lab", "--once"})
	cmd.SetContext(context.Background())
	if err := cmd.Execute(); err != nil {
		t.Fatalf("refresh must succeed with a pin: %v", err)
	}
	rc, _ := agent.ReadRosterCache(home, "lab")
	if rc == nil || rc.Generation != 9 {
		t.Fatalf("cache not updated: %+v", rc)
	}
}

func TestAgentDoctorForgedManifestFATAL(t *testing.T) {
	home := t.TempDir()
	t.Setenv("TETHER_HOME", home)
	t.Setenv("TETHER_DEV_NO_AUTH", "1") // allow http(s) httptest bootstrap in tests
	_, pub := accountKeypair(t)
	foreign, _ := accountKeypair(t)
	srv := signedManifestServer(t, foreign, pub, 1, 1, []string{"wss://b1:443"}) // forged
	defer srv.Close()
	if err := writeAgentConfig(home, "lab", "lab-1", "nats://127.0.0.1:14222", pub, srv.URL); err != nil {
		t.Fatal(err)
	}
	_ = os.MkdirAll(filepath.Join(home, "agent", "lab", "keys"), 0o700)
	_ = os.WriteFile(filepath.Join(home, "agent", "lab", "keys", "agent.nk"), []byte("x"), 0o600)
	checks := runAgentDoctor(context.Background(), home, "lab")
	fatal := false
	for _, c := range checks {
		if c.name == "manifest" && c.status == "FATAL" {
			fatal = true
		}
	}
	if !fatal {
		t.Fatalf("doctor must FATAL on a forged manifest: %+v", checks)
	}
}

func TestAgentDoctorReadOnlyNeverMintsKey(t *testing.T) {
	home := t.TempDir()
	t.Setenv("TETHER_HOME", home)
	t.Setenv("TETHER_DEV_NO_AUTH", "1") // allow http(s) httptest bootstrap in tests
	if err := writeAgentConfig(home, "lab", "lab-1", "nats://127.0.0.1:14222", "", ""); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(home, "agent", "lab", "keys", "agent.nk")
	_ = runAgentDoctor(context.Background(), home, "lab")
	if _, err := os.Stat(keyPath); !os.IsNotExist(err) {
		t.Fatal("doctor must NEVER mint an identity key (read-only)")
	}
}
