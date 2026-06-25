package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/auth"
)

// signJoinSeed writes a node-ident seed and returns its path + the raw seed bytes.
func signJoinSeed(t *testing.T) (string, []byte) {
	t.Helper()
	dir := t.TempDir()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "node-ident.nk")
	if err := os.WriteFile(p, seed, 0o600); err != nil {
		t.Fatal(err)
	}
	return p, seed
}

// TestSignJoinBareTokenBackwardCompat (B3 review M5): WITHOUT --raft-addr, sign-join keeps the
// pre-B3 contract — the bare token on stdout — and never leaks the seed.
func TestSignJoinBareTokenBackwardCompat(t *testing.T) {
	seedPath, seed := signJoinSeed(t)
	cmd := newClusterSignJoinCmd()
	var out, errb bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errb)
	cmd.SetArgs([]string{"brk-b", "nonce123", "--seed", seedPath})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "nonce123:") || strings.Contains(out.String(), "cluster add") {
		t.Errorf("bare-token stdout wrong: %q", out.String())
	}
	if !strings.Contains(errb.String(), "node-pub:") {
		t.Errorf("stderr should carry node-pub: %q", errb.String())
	}
	if strings.Contains(out.String()+errb.String(), string(seed)) {
		t.Fatal("PRIVATE seed leaked")
	}
}

// TestSignJoinCompleteLine (B3 review M5): WITH --raft-addr, stdout is the SOLE complete
// `cluster add …` line; a missing tunnel cert yields a <sha256:…> placeholder + a stderr WARN;
// the seed never leaks.
func TestSignJoinCompleteLine(t *testing.T) {
	seedPath, seed := signJoinSeed(t)
	cmd := newClusterSignJoinCmd()
	var out, errb bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errb)
	cmd.SetArgs([]string{"brk-b", "nonce123", "--seed", seedPath, "--secrets-dir", t.TempDir(),
		"--raft-addr", "10.0.0.2:7400", "--tunnel-addr", "h:7000", "--nats-route", "nats://h:6222", "--public-host", "brk-b"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "tether cluster add brk-b 10.0.0.2:7400 ") {
		t.Errorf("complete-line stdout wrong: %q", out.String())
	}
	if !strings.Contains(out.String(), "--cert-fp <sha256:…>") {
		t.Errorf("a missing tunnel cert should leave a <sha256:…> placeholder: %q", out.String())
	}
	if !strings.Contains(errb.String(), "WARN") {
		t.Errorf("a missing tunnel cert should WARN on stderr: %q", errb.String())
	}
	if strings.Contains(out.String()+errb.String(), string(seed)) {
		t.Fatal("PRIVATE seed leaked")
	}
}

// TestSignJoinEmbedsJoinerVersions — Stage-C MAJOR-6: the complete add line embeds the JOINER
// binary's --joiner-proto + --joiner-release (the leader's B6 version-skew gate reads them).
func TestSignJoinEmbedsJoinerVersions(t *testing.T) {
	seedPath, _ := signJoinSeed(t)
	cmd := newClusterSignJoinCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"brk-b", "nonce123", "--seed", seedPath, "--secrets-dir", t.TempDir(),
		"--raft-addr", "10.0.0.2:7400", "--tunnel-addr", "h:7000", "--nats-route", "nats://h:6222", "--public-host", "brk-b"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	line := out.String()
	if !strings.Contains(line, "--joiner-proto 2") {
		t.Errorf("add line must embed --joiner-proto 2 (this binary's proto): %q", line)
	}
	if !strings.Contains(line, "--joiner-release ") {
		t.Errorf("add line must embed --joiner-release: %q", line)
	}
}

// TestSignJoinMissingSeedErrorsBeforePrint (B3 review M5): a missing node-ident seed errors before
// any output (existing behavior preserved).
func TestSignJoinMissingSeedErrorsBeforePrint(t *testing.T) {
	cmd := newClusterSignJoinCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"brk-b", "nonce123", "--seed", filepath.Join(t.TempDir(), "absent.nk")})
	if err := cmd.Execute(); err == nil {
		t.Fatal("missing seed must error")
	}
	if strings.Contains(out.String(), "cluster add") || strings.Contains(out.String(), "nonce123:") {
		t.Errorf("must not print any token/line on a seed error: %q", out.String())
	}
}
