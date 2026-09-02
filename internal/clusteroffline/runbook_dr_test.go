package clusteroffline_test

import (
	"os"
	"strings"
	"testing"
)

// runbook_dr_test.go (formerly r10_runbook_dr_test.go) — R10 D2: the disaster-recovery runbook is a CONTRACT, so pin it.
//
// R10 P2's finding was not "restore is missing a flag" — it was that docs/cluster-runbook.md §5.2
// was STRUCTURALLY IMPOSSIBLE TO EXECUTE: an operator who followed it verbatim after a total-loss
// event started the daemon into a boot FATAL (cluster-seeded DB, no broker.cluster seam) and, if
// they got past that, into a crash-loop (lone voter on a clustered nats.conf). The runbook is what
// a human actually runs at 3am, so a correct implementation behind a wrong runbook is still a
// broken recovery path.
//
// simcluster drill 51 executes §5.2 VERBATIM on real systemd; these assertions are the cheap,
// hermetic half that fails in `make test` seconds after someone edits the ordering back.
//
// Precedent for doc-pinning in this repo: internal/broker/force_single_online_external_review_test.go
// and test/d7/external_review_test.go both read this same file.

func runbook(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../docs/cluster-runbook.md")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// section returns the body of the given heading, up to the next heading of the same-or-shallower
// level. Slicing matters: an assertion that only grepped the whole file would pass on a runbook that
// says the right thing in §1 and the wrong thing in §5.2.
//
// It is FENCE-AWARE. This runbook's sections are mostly ```bash blocks full of `# step N` comments,
// which are indistinguishable from an h1 heading if you scan raw lines — a fence-blind version of
// this helper terminated every section at its first shell comment and made all of these assertions
// vacuously fail (and, with inverted assertions, would have made them vacuously PASS).
func section(t *testing.T, doc, heading string) string {
	t.Helper()
	lines := strings.Split(doc, "\n")
	level := strings.Count(strings.SplitN(heading, " ", 2)[0], "#")

	start := -1
	inFence := false
	var body []string
	for _, ln := range lines {
		if strings.HasPrefix(strings.TrimSpace(ln), "```") {
			inFence = !inFence
			if start >= 0 {
				body = append(body, ln)
			}
			continue
		}
		if start < 0 {
			if !inFence && strings.HasPrefix(ln, heading) {
				start = 1
			}
			continue
		}
		// A real heading only outside a fence.
		if !inFence && strings.HasPrefix(ln, "#") {
			h := 0
			for h < len(ln) && ln[h] == '#' {
				h++
			}
			if h <= level {
				break
			}
		}
		body = append(body, ln)
	}
	if start < 0 {
		t.Fatalf("runbook has no %q heading — the DR contract moved; re-point this test", heading)
	}
	if len(body) == 0 {
		t.Fatalf("section %q sliced EMPTY — the helper is broken, so every assertion on it is vacuous", heading)
	}
	return strings.Join(body, "\n")
}

// TestRunbookDRSectionsAreExecutable pins the ORDER that makes recovery work. Both restore sections
// must fix nats.conf BEFORE starting the daemon; that ordering is the entire P4/#64 fix.
func TestRunbookDRSectionsAreExecutable(t *testing.T) {
	doc := runbook(t)

	for _, sec := range []string{
		"### 5.1 Restore (OFFLINE, IRREVERSIBLE)",
		"### 5.2 Full-cluster disaster recovery (all nodes lost)",
	} {
		t.Run(sec, func(t *testing.T) {
			body := section(t, doc, sec)

			// (1) --config must be named: without the broker.cluster seam a restored host cannot
			// boot at all, and install.sh ships broker.yaml with `cluster:` commented out.
			if !strings.Contains(body, "--config") {
				t.Error("must name `--config` — a restored host without the broker.cluster seam FATALs at boot")
			}
			// (2) the lone-voter nats.conf step must be present AND come first.
			iConf := strings.Index(body, "reconcile nats --manual")
			iStart := strings.Index(body, "systemctl start tether-broker")
			if iConf < 0 {
				t.Fatal("no offline nats.conf render step — a lone voter on a clustered conf crash-loops the broker")
			}
			if iStart < 0 {
				t.Fatal("section never starts the daemon; the sequence is incomplete")
			}
			if iConf > iStart {
				t.Error("nats.conf must be rendered BEFORE tether-broker is started — that ordering IS the fix")
			}
			// (3) the scope of what did NOT come back must be stated where the operator is looking.
			if !strings.Contains(body, "JetStream") {
				t.Error("must state that JetStream (history/audit) is not restored — silence here is #53")
			}
		})
	}
}

// TestRunbookBackupStatesBundleScope: the operator forms the belief "I have a backup" in §5, so that
// is where the scope has to be stated — and it has to name the tool that fills the gap.
func TestRunbookBackupStatesBundleScope(t *testing.T) {
	body := section(t, runbook(t), "## 5. Backup & disaster recovery")

	for _, want := range []struct{ tok, why string }{
		{"nats stream backup", "must give the separate JetStream backup command"},
		{"history", "must name what is missing in the operator's own words"},
	} {
		if !strings.Contains(body, want.tok) {
			t.Errorf("§5 %s (missing %q)", want.why, want.tok)
		}
	}
	// The scope statement must be UNMISSABLE, not a footnote — it is the reason #53 was a defect.
	if !strings.Contains(body, "BUNDLE SCOPE") {
		t.Error("§5 must carry the explicit BUNDLE SCOPE callout")
	}
}

// TestRunbookNamesAllFiveSeamFields — `serve` keys cluster mode on data_dir alone, so a PARTIAL seam
// boots the host in SINGLE mode and lands on the identical boot FATAL. The pre-R10 runbook listed
// three fields; that omission was itself a defect, and drill 51 asserts all five.
func TestRunbookNamesAllFiveSeamFields(t *testing.T) {
	body := section(t, runbook(t), "## 5. Backup & disaster recovery")
	for _, f := range []string{"data_dir", "raft_addr", "secrets_dir", "nats_conf_path", "nats_server_bin"} {
		if !strings.Contains(body, f) {
			t.Errorf("§5 must name seam field %q — a partial seam boots SINGLE mode and FATALs identically", f)
		}
	}
}
