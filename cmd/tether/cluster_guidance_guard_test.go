package main

import (
	"os"
	"strings"
	"testing"
)

// cluster_guidance_guard_test.go (external review F2) — a source-level guard against deleted/relocated
// cluster command guidance leaking into LIVE operator output. C8 deleted `cluster add`/`sign-join`/
// `wait` and relocated the escape verbs under `cluster recovery` (force-single/rejoin prepare/restore/
// incident export/node remove). The worst stale strings are emitted at FAILURE/RECOVERY time, when an
// operator copies the exact line the tool prints — so a dead-end command name there is a real trap.
//
// Scope: the files that emit runtime recovery/offline guidance + the recovery/backup/offline CLI + the
// status nextStep guidance. Comments are skipped (they legitimately document the C8 reparenting). The
// explicitly-latent D9 `cluster add` backend (internal/broker/clusteradmin.go + the OpClusterAdd handler
// in clusterstatus.go) keeps `cluster add`/`cluster sign-join` in its internal error/log context, so for
// clusterstatus.go those two tokens are NOT forbidden (its LIVE status nextSteps are still guarded
// against the relocated recover/restore/force-single spellings — round-2 caught one such miss at
// clusterstatus.go's force-single nextStep). clusteradmin.go is excluded entirely (pure latent backend).

// The relocated-to-recovery + plain-deleted spellings. Each token is chosen so a VALID new spelling is
// not a false positive: "cluster recovery restore" ⊅ "cluster restore"; "cluster recovery force-single"
// ⊅ "cluster force-single"; "cluster recovery node remove" ⊅ "cluster remove"; "cluster recovery …" ⊅
// "cluster recover " (trailing space) or "cluster recover`".
var relocatedSpellings = []string{
	"cluster restore",
	"cluster force-single",
	"cluster recover ",
	"cluster recover`",
	"cluster remove",
	"cluster wait",
}

// deletedAddSpellings are forbidden in pure operator-output files but NOT in clusterstatus.go (whose
// latent OpClusterAdd handler legitimately keeps them in internal error/log context).
var deletedAddSpellings = []string{"cluster add", "cluster sign-join"}

func TestExternalReviewF2NoStaleCommandGuidanceInLiveOutput(t *testing.T) {
	full := append(append([]string{}, relocatedSpellings...), deletedAddSpellings...)
	files := []struct {
		path      string
		forbidden []string
	}{
		{"../../internal/clusteroffline/offline.go", full},
		{"../../internal/clusteroffline/restore.go", full},
		{"../../internal/clusteroffline/init.go", full},
		{"../../internal/broker/cutover.go", full},
		{"../../internal/broker/clusterdrain.go", full},
		{"../../internal/broker/cluster_operation_controller.go", full},
		{"../../internal/clusterspec/spec.go", full},
		// clusterstatus.go has LIVE status nextSteps (guarded for relocated commands) AND the latent
		// OpClusterAdd backend (which legitimately keeps `cluster add`/`sign-join`) → relocated-only.
		{"../../internal/broker/clusterstatus.go", relocatedSpellings},
		{"cluster_backup.go", full},
		{"cluster_offline.go", full},
		{"cluster_offline_wizard.go", full},
		{"cluster_recovery.go", full},
	}
	for _, f := range files {
		raw, err := os.ReadFile(f.path)
		if err != nil {
			t.Fatalf("read %s: %v", f.path, err)
		}
		for i, line := range strings.Split(string(raw), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue // comment-only line: documents the migration, not operator output
			}
			// Strip a trailing line-comment (" //…") so a code line that ANNOTATES the old name in its
			// comment isn't flagged. " //" (leading space) never matches scheme separators like nats://.
			code := line
			if idx := strings.Index(code, " //"); idx >= 0 {
				code = code[:idx]
			}
			for _, bad := range f.forbidden {
				if strings.Contains(code, bad) {
					t.Errorf("%s:%d emits stale command guidance %q in live output — use the C8 primary (`cluster join prepare`/`approve`, `cluster recovery restore`/`force-single`/`rejoin prepare`/`node remove`):\n\t%s", f.path, i+1, bad, trimmed)
				}
			}
		}
	}
}
