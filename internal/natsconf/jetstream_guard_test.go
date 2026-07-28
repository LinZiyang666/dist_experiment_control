package natsconf

import (
	"path/filepath"
	"strings"
	"testing"
)

// jetstream_guard_test.go — BuildMergedConf must refuse to render JetStream AWAY (B5, BUG-1).
//
// THE MECHANISM
// -------------
// Render omits the entire jetstream{} block when JSStoreDir is empty, and what comes out is
// SYNTACTICALLY VALID: `nats-server -t` accepts it, so the DryRun gate passes it and the atomic swap
// proceeds. The broker restarts with no JetStream at all — which is where history, audit and every
// in-flight transfer object live.
//
// Three of the five callers guarded this themselves (cluster_natsconf.go's F2 and F3 paths,
// cluster_offline.go's force-single). The two that did NOT were the AUTOMATED ones: the topology
// reconciler's 5-second loop and the grow cutover. That is the worst possible split — the paths with no
// operator watching were the paths without the guard — and it is the "the check exists in the copies
// someone remembered" shape. The guard now lives at the single point all five funnel through.
//
// REACHABILITY, stated honestly: it needs a conf whose jetstream block has no store_dir. install.sh
// always writes one, so this is LATENT, not live. It is worth closing anyway because Preflight ACCEPTS
// that shape (jetstream is InstallSafe at the top level and unrecognizedSubkeys returns nil for an empty
// map), the fleet's only broker runs a hand-edited conf, and the failure is silent and total.
func TestBuildMergedConfRefusesToRenderJetStreamAway(t *testing.T) {
	// A conf that RUNS JetStream but from which no store_dir can be harvested.
	const jsWithoutStoreDir = `server_name: "brk-a"
host: "0.0.0.0"
port: 4222
jetstream {
}
`
	own, err := Preflight(writeConf(t, jsWithoutStoreDir))
	if err != nil {
		t.Fatalf("premise check: Preflight must ACCEPT a bare jetstream{} (that is what makes the defect "+
			"reachable). Got: %v", err)
	}
	if !own.HasJetStream() {
		t.Fatal("premise check: the fixture conf must read as JetStream-enabled")
	}
	if own.JSStoreDir() != "" {
		t.Fatalf("premise check: no store_dir should be harvestable, got %q", own.JSStoreDir())
	}

	cfg := sampleClusterConfig()
	cfg.JSStoreDir = own.JSStoreDir() // exactly what every caller does: harvest, then render

	out, err := BuildMergedConf(own, cfg)
	if err == nil {
		hasJS := strings.Contains(out, "jetstream")
		t.Fatalf("BuildMergedConf rendered a conf for a JetStream-enabled broker with no store_dir "+
			"(jetstream block present in output: %v). `nats-server -t` accepts that file, so the swap "+
			"proceeds and the broker comes back with history/audit/transfers gone.\n%s", hasJS, out)
	}
	if !strings.Contains(err.Error(), "store_dir") {
		t.Errorf("the refusal must name what is missing so an operator can fix it; got: %v", err)
	}
}

// TestBuildMergedConfRendersWhenTheStoreDirIsPresent is the other half: the guard must not refuse the
// normal case. A fail-closed check that also fails the healthy path would take the whole fleet's
// reconciler down, which is a strictly worse outcome than the latent defect it closes.
func TestBuildMergedConfRendersWhenTheStoreDirIsPresent(t *testing.T) {
	own, err := Preflight(writeConf(t, installSHConf))
	if err != nil {
		t.Fatalf("install.sh shape must preflight clean: %v", err)
	}
	if !own.HasJetStream() || own.JSStoreDir() == "" {
		t.Fatalf("premise: the install shape must carry JetStream WITH a store_dir; hasJS=%v store_dir=%q",
			own.HasJetStream(), own.JSStoreDir())
	}
	cfg := sampleClusterConfig()
	cfg.JSStoreDir = own.JSStoreDir()

	out, err := BuildMergedConf(own, cfg)
	if err != nil {
		t.Fatalf("the guard must not refuse the healthy path: %v", err)
	}
	if !strings.Contains(out, "store_dir") {
		t.Errorf("rendered conf lost its store_dir:\n%s", out)
	}
}

// TestBuildMergedConfStillRendersForAJetStreamLessConf: a conf with no jetstream block at all is not
// the defect — nothing is being taken away — and some tests render only the routes/callout surface.
// The guard keys on "the live conf HAS JetStream", not on "the config lacks a store dir".
func TestBuildMergedConfStillRendersForAJetStreamLessConf(t *testing.T) {
	const noJS = `server_name: "brk-a"
host: "0.0.0.0"
port: 4222
`
	own, err := Preflight(writeConf(t, noJS))
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if own.HasJetStream() {
		t.Fatal("premise: this fixture must have no jetstream block")
	}
	cfg := sampleClusterConfig()
	cfg.JSStoreDir = ""
	if _, err := BuildMergedConf(own, cfg); err != nil {
		t.Errorf("a conf that never had JetStream must still render; the guard is about not REMOVING it: %v", err)
	}
}

// TestJetStreamGuardCoversTheAutomatedPaths pins WHY the guard is here and not in the callers. Both
// automated assemblers pass a harvested store dir straight through, so the only place a single check can
// cover them is the shared render entry point. This is a structural assertion and complements the
// behavioural ones above rather than replacing them (testing-standards S1).
func TestJetStreamGuardCoversTheAutomatedPaths(t *testing.T) {
	// Simulate what the reconciler and the cutover both do: harvest, then render.
	const jsNoStore = "server_name: \"brk-a\"\nhost: \"0.0.0.0\"\nport: 4222\njetstream {\n}\n"
	own, err := Preflight(writeConf(t, jsNoStore))
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	dir := t.TempDir()

	// The reconciler's shape (natsreconcile: JSStoreDir: own.JSStoreDir()).
	reconcilerCfg := sampleClusterConfig()
	reconcilerCfg.JSStoreDir = own.JSStoreDir()
	if _, err := BuildMergedConf(own, reconcilerCfg); err == nil {
		t.Error("the RECONCILER's assembly (harvest store_dir, render) must be refused — it is the 5-second " +
			"autonomous loop, so this is the path with nobody watching")
	}

	// The grow cutover's shape (own.JSStoreDir() as well, plus a forced monitor).
	cutoverCfg := sampleClusterConfig()
	cutoverCfg.JSStoreDir = own.JSStoreDir()
	cutoverCfg.MonitorListen = "127.0.0.1:8223"
	if _, err := BuildMergedConf(own, cutoverCfg); err == nil {
		t.Error("the grow CUTOVER's assembly must be refused too — it swaps a conf and then hard-restarts, " +
			"so a JetStream-less render there is immediately load-bearing")
	}

	// And the same assembly with a real store dir must pass, so the two assertions above are about the
	// missing store_dir and not about the assembly shape.
	ok := sampleClusterConfig()
	ok.JSStoreDir = filepath.Join(dir, "js")
	if _, err := BuildMergedConf(own, ok); err != nil {
		t.Errorf("with a real store dir the same assembly must render: %v", err)
	}
}
