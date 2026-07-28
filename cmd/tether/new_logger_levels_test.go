package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// origin: b5_test.go (renamed in B6) — docs/reviews/b5-plan.md, docs/reviews/b5-review.md
//
// TestB5NewLoggerLevels (B5 plan §F.8): newLogger filters by level (info suppresses debug, like the
// prior default), and a bad level is a usage error (exit 64) — never a silent default.
func TestB5NewLoggerLevels(t *testing.T) {
	info, err := newLogger("info", false)
	if err != nil {
		t.Fatal(err)
	}
	h := info.Handler()
	if !h.Enabled(context.Background(), slog.LevelInfo) {
		t.Error(`newLogger("info") must enable Info (the prior default)`)
	}
	if h.Enabled(context.Background(), slog.LevelDebug) {
		t.Error(`newLogger("info") must suppress Debug (byte-equivalence with the old default handler)`)
	}

	dbg, err := newLogger("DEBUG", false) // case-insensitive
	if err != nil {
		t.Fatal(err)
	}
	if !dbg.Handler().Enabled(context.Background(), slog.LevelDebug) {
		t.Error(`newLogger("DEBUG") must enable Debug`)
	}

	for _, bad := range []string{"bogus", ""} {
		if _, err := newLogger(bad, false); err == nil {
			t.Fatalf("a bad level %q must error", bad)
		} else if got := classifyExit(err); got != exitUsage {
			t.Errorf("bad level %q exit class = %d, want %d (usage)", bad, got, exitUsage)
		}
	}

	if _, err := newLogger("info", true); err != nil { // JSON handler still constructs
		t.Fatalf("--log-json must construct: %v", err)
	}
}

// TestB5TakeoverPlanJSON (B5 plan §F.19): the plan schema carries the branch-load-bearing `changed`
// + `schema`/`schema_version` (never omitempty) and normalizes the slice fields to [].
func TestB5TakeoverPlanJSON(t *testing.T) {
	b, err := json.Marshal(takeoverPlanJSON{
		Schema: "takeover_natsconf_plan", SchemaVersion: 1, Changed: false,
		ServerName: "brk-a", ClientListen: "127.0.0.1:4222",
		Routes: normSlice([]string{}), AuthUsers: normSlice([]string{}),
		OwnershipRegenerated: []string{"authorization", "cluster", "server_name"},
		DryRun:               "ok", RestartOrderHint: "re-run on every broker",
	})
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{`"schema":"takeover_natsconf_plan"`, `"schema_version":1`, `"changed":false`, `"routes":[]`, `"auth_users":[]`, `"dry_run":"ok"`} {
		if !strings.Contains(s, want) {
			t.Errorf("takeover plan JSON missing %q: %s", want, s)
		}
	}
}
