package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// cluster_c8_test.go — C8 CLI-consolidation invariants: the deletions are gone, no live new+old dual
// surface, recovery is the primary tree, the top-level escapes are hidden one-cycle deprecated aliases
// (warning to stderr, gates byte-preserved), drain --retire redirects, and 2nd-node bootstrap survives
// without add/sign-join.

func c8Child(root *cobra.Command, name string) *cobra.Command {
	for _, c := range root.Commands() {
		if c.Name() == name {
			return c
		}
	}
	return nil
}

// TestC8DeletedCommandsGone: add / sign-join / wait are absent from the cluster command set.
func TestC8DeletedCommandsGone(t *testing.T) {
	root := newClusterCmd()
	for _, gone := range []string{"add", "sign-join", "wait"} {
		if c := c8Child(root, gone); c != nil {
			t.Errorf("`cluster %s` must be DELETED, but it is still registered (hidden=%v)", gone, c.Hidden)
		}
	}
}

// TestC8ClusterGroupsThree: 4 groups (online/client/migrate/escape); `local` is gone, `client` added by
// the cli-failover increment (cluster pin/invite).
func TestC8ClusterGroupsThree(t *testing.T) {
	root := newClusterCmd()
	if g := root.Groups(); len(g) != 4 {
		t.Fatalf("want 4 groups, got %d", len(g))
	}
	for _, g := range root.Groups() {
		if g.ID == "local" {
			t.Error("the `local` group must be removed (sign-join deleted; node-pub/keygen hidden)")
		}
	}
	// Running --help must not panic on a command carrying a GroupID without a registered group.
	root.SetArgs([]string{"--help"})
	root.SetOut(&strings.Builder{})
	root.SetErr(&strings.Builder{})
	if err := root.Execute(); err != nil {
		t.Fatalf("cluster --help must not error (checkCommandGroups): %v", err)
	}
}

// TestC8NoLiveDualSurface: exactly one VISIBLE mutating join + one visible retire; the migrated escape
// verbs have a HIDDEN top-level instance + a VISIBLE recovery child — never two visible parents.
func TestC8NoLiveDualSurface(t *testing.T) {
	root := newClusterCmd()
	visible := map[string]bool{}
	for _, c := range root.Commands() {
		if !c.Hidden {
			visible[c.Name()] = true
		}
	}
	if !visible["join"] || !visible["retire"] {
		t.Error("join + retire must be the visible mutating membership verbs")
	}
	// The migrated escapes must NOT be visible at top level (they live under recovery now).
	for _, moved := range []string{"force-single", "recover", "restore", "export-incident", "remove"} {
		if visible[moved] {
			t.Errorf("`cluster %s` must not be a VISIBLE top-level command after C8 (moved under recovery)", moved)
		}
		if c := c8Child(root, moved); c == nil || !c.Hidden {
			t.Errorf("`cluster %s` must survive as a HIDDEN deprecated alias", moved)
		}
	}
}

// TestC8RecoveryIsPrimaryTree: recovery is visible + hosts the escape verbs as visible children.
func TestC8RecoveryIsPrimaryTree(t *testing.T) {
	root := newClusterCmd()
	rec := c8Child(root, "recovery")
	if rec == nil || rec.Hidden {
		t.Fatal("`cluster recovery` must be a VISIBLE primary namespace")
	}
	want := map[string]bool{"diagnose": false, "force-single": false, "rejoin": false, "restore": false, "incident": false, "node": false}
	for _, c := range rec.Commands() {
		if _, ok := want[c.Name()]; ok {
			want[c.Name()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("recovery is missing the primary child %q", name)
		}
	}
	// rejoin->prepare, incident->export, node->remove are the nested leaves.
	if p := c8Child(c8Child(rec, "rejoin"), "prepare"); p == nil {
		t.Error("recovery rejoin prepare missing")
	}
	if e := c8Child(c8Child(rec, "incident"), "export"); e == nil {
		t.Error("recovery incident export missing")
	}
	if n := c8Child(c8Child(rec, "node"), "remove"); n == nil {
		t.Error("recovery node remove missing")
	}
}

// TestC8DeprecationNoteStderrNotStdout: a hidden alias warns to STDERR (never stdout) — the D2 guard
// against cobra's .Deprecated stdout pollution. Uses `restore` (typed-confirm refuses non-TTY, so it
// never reaches the admin socket) and asserts the deprecation note is on stderr, absent from stdout.
func TestC8DeprecationNoteStderrNotStdout(t *testing.T) {
	root := newClusterCmd()
	var out, errb strings.Builder
	root.SetOut(&out)
	root.SetErr(&errb)
	root.SetArgs([]string{"restore", "/nonexistent-bundle", "--confirm-node-id", "brk-a"})
	_ = root.Execute() // expected to fail at the typed-confirm; we only inspect the streams
	if !strings.Contains(errb.String(), "deprecated:") || !strings.Contains(errb.String(), "recovery restore") {
		t.Errorf("the deprecation note must be on STDERR: %q", errb.String())
	}
	if strings.Contains(out.String(), "deprecated:") {
		t.Errorf("the deprecation note must NOT ride STDOUT (D2): %q", out.String())
	}
}

// TestC8DrainRetireRedirects: `drain --retire` returns a usage error naming `cluster retire` (the flag
// is removed, not honored); `drain` alone still parses.
func TestC8DrainRetireRedirects(t *testing.T) {
	root := newClusterCmd()
	var out strings.Builder
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"drain", "brk-b", "--retire"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "cluster retire") || !strings.Contains(err.Error(), "removed") {
		t.Fatalf("`drain --retire` must redirect to `cluster retire`, got %v", err)
	}
}

// TestC8RecoveryNodeRemoveRequiresManual: raw remove without --manual fails closed naming retire; the
// `--yes` path still hits "cannot run unattended" first (gate order).
func TestC8RecoveryNodeRemoveRequiresManual(t *testing.T) {
	sp := "/nonexistent.sock"
	run := func(args ...string) error {
		cmd := newClusterRemoveCmd(&sp)
		cmd.SetOut(&strings.Builder{})
		cmd.SetErr(&strings.Builder{})
		cmd.SetArgs(args)
		return cmd.Execute()
	}
	if err := run("brk-b"); err == nil || !strings.Contains(err.Error(), "requires --manual") || !strings.Contains(err.Error(), "cluster retire") {
		t.Errorf("remove without --manual must fail-closed naming `cluster retire`, got %v", err)
	}
	// --yes must STILL be refused before the --manual gate (unattended rejection is first).
	if err := run("brk-b", "--yes"); err == nil || strings.Contains(err.Error(), "requires --manual") {
		t.Errorf("`--yes` must be rejected as unattended BEFORE the --manual gate, got %v", err)
	}
}

// TestC8KeygenJoinPrepareRoundTrip: the hidden `keygen` still mints a seed, and `join prepare` derives
// a pub from it — 2nd-node bootstrap survives the deletion of add/sign-join.
func TestC8KeygenJoinPrepareRoundTrip(t *testing.T) {
	dir := t.TempDir()
	seedPath := filepath.Join(dir, "node-ident.nk")
	kg := newClusterKeygenCmd()
	kg.SetOut(&strings.Builder{})
	kg.SetErr(&strings.Builder{})
	kg.SetArgs([]string{"--out", seedPath})
	if err := kg.Execute(); err != nil {
		t.Fatalf("hidden keygen must still mint a seed: %v", err)
	}
	if _, err := os.Stat(seedPath); err != nil {
		t.Fatalf("keygen did not write the seed: %v", err)
	}
	// broker.nk in the same dir so bus_nkey derives (audit A fail-closed); --cert-fp is the escape hatch.
	bnk := newClusterKeygenCmd()
	bnk.SetOut(&strings.Builder{})
	bnk.SetErr(&strings.Builder{})
	bnk.SetArgs([]string{"--out", filepath.Join(dir, "broker.nk")})
	if err := bnk.Execute(); err != nil {
		t.Fatal(err)
	}
	jp := newClusterJoinPrepareCmd()
	var jout strings.Builder
	jp.SetOut(&jout)
	jp.SetErr(&strings.Builder{})
	jp.SetArgs([]string{"--node-id", "brk-b", "--seed", seedPath, "--raft-addr", "10.0.0.2:7400",
		"--nats-route", "nats://10.0.0.2:6222", "--tunnel-addr", "brk-b:7000",
		"--secrets-dir", dir, "--cert-fp", "ABCDEF1234"})
	if err := jp.Execute(); err != nil {
		t.Fatalf("join prepare must derive a bundle from a keygen seed: %v", err)
	}
	if !strings.Contains(jout.String(), "tether-join:") {
		t.Errorf("join prepare must emit a join bundle: %q", jout.String())
	}
}

// TestC8JoinPrepareNewlineSeed (D12): a trailing-newline seed derives the same bundle as a raw one.
func TestC8JoinPrepareNewlineSeed(t *testing.T) {
	dir := t.TempDir()
	raw := filepath.Join(dir, "raw.nk")
	nl := filepath.Join(dir, "nl.nk")
	kg := newClusterKeygenCmd()
	kg.SetOut(&strings.Builder{})
	kg.SetErr(&strings.Builder{})
	kg.SetArgs([]string{"--out", raw})
	if err := kg.Execute(); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(raw)
	if err := os.WriteFile(nl, append(append([]byte{}, b...), '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	// secrets dir with a broker.nk so bus_nkey derives (audit A fail-closed); --cert-fp is the explicit
	// escape hatch so no generated tunnel-cert.pem is needed. This test is about --seed newline tolerance.
	secrets := filepath.Join(dir, "secrets")
	if err := os.MkdirAll(secrets, 0o700); err != nil {
		t.Fatal(err)
	}
	bnk := newClusterKeygenCmd()
	bnk.SetOut(&strings.Builder{})
	bnk.SetErr(&strings.Builder{})
	bnk.SetArgs([]string{"--out", filepath.Join(secrets, "broker.nk")})
	if err := bnk.Execute(); err != nil {
		t.Fatal(err)
	}
	prep := func(seed string) string {
		jp := newClusterJoinPrepareCmd()
		var out strings.Builder
		jp.SetOut(&out)
		jp.SetErr(&strings.Builder{})
		jp.SetArgs([]string{"--node-id", "brk-b", "--seed", seed, "--raft-addr", "10.0.0.2:7400",
			"--nats-route", "nats://10.0.0.2:6222", "--tunnel-addr", "brk-b:7000",
			"--secrets-dir", secrets, "--cert-fp", "ABCDEF1234"})
		if err := jp.Execute(); err != nil {
			t.Fatalf("join prepare(%s): %v", seed, err)
		}
		return out.String()
	}
	// The bundle carries a random nonce so the full string differs; assert BOTH succeed + emit a bundle
	// (the newline must NOT make PublicKeyFromSeed fail).
	if !strings.Contains(prep(raw), "tether-join:") || !strings.Contains(prep(nl), "tether-join:") {
		t.Error("a newline-terminated seed must still produce a valid join bundle (D12)")
	}
}

// TestC8JoinPrepareRequiresHomeIdentity (D10): join prepare without --tunnel-addr / --nats-route
// refuses — the deleted `add`'s expose-home/NATS-peer admission gate is preserved.
func TestC8JoinPrepareRequiresHomeIdentity(t *testing.T) {
	dir := t.TempDir()
	seed := filepath.Join(dir, "s.nk")
	kg := newClusterKeygenCmd()
	kg.SetOut(&strings.Builder{})
	kg.SetErr(&strings.Builder{})
	kg.SetArgs([]string{"--out", seed})
	if err := kg.Execute(); err != nil {
		t.Fatal(err)
	}
	run := func(extra ...string) error {
		jp := newClusterJoinPrepareCmd()
		jp.SetOut(&strings.Builder{})
		jp.SetErr(&strings.Builder{})
		args := append([]string{"--node-id", "brk-b", "--seed", seed, "--raft-addr", "10.0.0.2:7400"}, extra...)
		jp.SetArgs(args)
		return jp.Execute()
	}
	if err := run("--nats-route", "nats://10.0.0.2:6222"); err == nil || !strings.Contains(err.Error(), "tunnel-addr") {
		t.Errorf("missing --tunnel-addr must refuse, got %v", err)
	}
	if err := run("--tunnel-addr", "brk-b:7000"); err == nil || !strings.Contains(err.Error(), "nats-route") {
		t.Errorf("missing --nats-route must refuse, got %v", err)
	}
	home := []string{"--nats-route", "nats://10.0.0.2:6222", "--tunnel-addr", "brk-b:7000"}
	// v0.4.4 review F1: join prepare now FAILS CLOSED if it cannot derive bus_nkey/cert_fp — an empty
	// bundle re-arms the learner self-backfill DEADLOCK (bus_nkey) + crash-loops the joiner (cert_fp),
	// the exact failures audit A removes. A missing/wrong secrets dir (no broker.nk) must REFUSE, not
	// WARN-and-emit a poisoned bundle.
	if err := run(append(home, "--secrets-dir", filepath.Join(dir, "nonexistent"))...); err == nil || !strings.Contains(err.Error(), "broker.nk") {
		t.Errorf("missing broker.nk must fail closed (no poisoned empty-bus_nkey bundle), got %v", err)
	}
	// With broker.nk present (bus_nkey derives) + --cert-fp as the explicit escape hatch (so no generated
	// tunnel-cert.pem is needed here) it succeeds.
	secrets := t.TempDir()
	bnk := newClusterKeygenCmd()
	bnk.SetOut(&strings.Builder{})
	bnk.SetErr(&strings.Builder{})
	bnk.SetArgs([]string{"--out", filepath.Join(secrets, "broker.nk")})
	if err := bnk.Execute(); err != nil {
		t.Fatal(err)
	}
	if err := run(append(home, "--secrets-dir", secrets, "--cert-fp", "ABCDEF1234")...); err != nil {
		t.Errorf("join prepare with broker.nk + --cert-fp override must succeed: %v", err)
	}
}

// TestC8RecoverVsRecoveryNoPrefixAmbiguity: with prefix-matching off, `recover` resolves to the hidden
// alias and `recovery` to the primary namespace (distinct commands).
func TestC8RecoverVsRecoveryNoPrefixAmbiguity(t *testing.T) {
	root := newClusterCmd()
	rec, _, err := root.Find([]string{"recover"})
	if err != nil || rec.Name() != "recover" {
		t.Errorf("`recover` must resolve to the hidden alias, got %v (%v)", rec.Name(), err)
	}
	recovery, _, err := root.Find([]string{"recovery"})
	if err != nil || recovery.Name() != "recovery" {
		t.Errorf("`recovery` must resolve to the primary namespace, got %v (%v)", recovery.Name(), err)
	}
}
