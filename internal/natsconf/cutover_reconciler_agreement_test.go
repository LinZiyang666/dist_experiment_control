package natsconf

import (
	"strings"
	"testing"
)

// cutover_reconciler_agreement_test.go — the grow cutover's render and the reconciler's render must
// agree except for the monitor (B5, BUG-3).
//
// WHAT WENT WRONG WITH THE OLD CLAIM
// ----------------------------------
// broker/cluster_grow_cutover.go carried a one-line comment asserting the cutover's conf is
// "byte-identical to the one the reconciler DryRun-validated then withheld". It was false: the cutover
// FORCES MonitorListen to 127.0.0.1:8223 (its own m2 comment, twenty lines below, says why —
// restartAndVerifyClustered probes exactly that address, and harvesting a possibly-absent http block
// would report a healthy revival as cutover_revival_failed after a 45s connection-refused).
//
// The claim was harmless only because the two agree whenever the live conf already has that http line,
// and the fixtures all did. The real contract is "identical MODULO the monitor", and it was pinned by
// nothing at all — the roadmap's concern was that adding one field to either render would silently
// break it.
//
// EXACTLY WHAT THIS TEST PINS, AND WHAT IT DOES NOT
// -------------------------------------------------
// It pins a property of BuildMergedConf: given two Configs that differ ONLY in MonitorListen, the merged
// texts differ only in the `http:` line. That is the half of the contract this package owns, and it is
// what makes "identical modulo the monitor" a meaningful statement rather than a hope.
//
// It does NOT pin that the broker's two ASSEMBLIES are identical modulo the monitor — it builds both
// from one shared base, so a field added to only one of the real call sites is invisible to it.
// Reconstructing those two assemblies here would mean copying production code into a test, which is the
// failure mode that got a source-text metric test deleted in the previous batch's external review: the
// copy stops tracking the original the moment either changes.
//
// The missing half is a STRUCTURAL check over the two real composite literals, in
// internal/natsconf/assembly_parity_test.go. The two halves are complementary and neither substitutes
// for the other (testing-standards S1): this one would not notice a new field present in one assembly,
// and that one cannot tell whether BuildMergedConf treats the fields as it claims to.
func TestCutoverRenderMatchesReconcilerRenderExceptTheMonitor(t *testing.T) {
	// A live STANDALONE conf with NO http block. This is the shape m2 exists for, and the shape under
	// which the old "byte-identical" claim was false.
	const standaloneNoMonitor = `server_name: "brk-a"
host: "0.0.0.0"
port: 4222
jetstream {
  store_dir: "/var/lib/tether/js"
}
`
	own, err := Preflight(writeConf(t, standaloneNoMonitor))
	if err != nil {
		t.Fatalf("fixture must preflight clean: %v", err)
	}
	if own.MonitorHTTP() != "" {
		t.Fatalf("fixture must have NO http block (that is the interesting case), got %q", own.MonitorHTTP())
	}

	// The shared half: everything both paths derive from the same topology inputs + Ownership.
	base := sampleClusterConfig()
	base.JSStoreDir = own.JSStoreDir()
	base.ClientListen = own.ClientListen()

	// The reconciler's assembly leaves MonitorListen empty (BuildMergedConf then harvests it).
	reconciler := base
	// The cutover's assembly forces it.
	const topoMonitorListen = "127.0.0.1:8223"
	cutover := base
	cutover.MonitorListen = topoMonitorListen

	recText, err := BuildMergedConf(own, reconciler)
	if err != nil {
		t.Fatalf("reconciler render: %v", err)
	}
	cutText, err := BuildMergedConf(own, cutover)
	if err != nil {
		t.Fatalf("cutover render: %v", err)
	}

	if recText == cutText {
		t.Fatal("the fixture is degenerate: with no http block in the live conf the two renders MUST differ " +
			"(that is the m2 case), so an equality here means the monitor forcing is not being exercised")
	}

	// The ONLY permitted difference is the monitor line. Strip it from the cutover text and the two must
	// be byte-identical — this is the real invariant, and it fails the moment a field lands in one
	// assembly and not the other.
	stripped := dropLinesWithPrefix(cutText, "http:")
	if stripped != recText {
		t.Errorf("the cutover and reconciler renders differ by MORE than the monitor line.\n"+
			"That is the divergence BUG-3 was about: a field present in one assembly and absent from the "+
			"other means the cutover applies a conf the reconciler will immediately want to replace — an "+
			"unplanned swap + SIGHUP on a broker that has just restarted.\n"+
			"--- cutover (monitor line removed) ---\n%s\n--- reconciler ---\n%s", stripped, recText)
	}

	// And with the monitor PRESENT in the live conf at the same address, they agree with no stripping at
	// all — which is why the false claim survived so long, and is worth pinning so the distinction stays
	// visible.
	withMonitor := strings.Replace(standaloneNoMonitor, "port: 4222\n",
		"port: 4222\nhttp: \""+topoMonitorListen+"\"\n", 1)
	own2, err := Preflight(writeConf(t, withMonitor))
	if err != nil {
		t.Fatalf("second fixture must preflight clean: %v", err)
	}
	base2 := sampleClusterConfig()
	base2.JSStoreDir = own2.JSStoreDir()
	base2.ClientListen = own2.ClientListen()
	rec2, err := BuildMergedConf(own2, base2)
	if err != nil {
		t.Fatalf("reconciler render (monitor present): %v", err)
	}
	cut2cfg := base2
	cut2cfg.MonitorListen = topoMonitorListen
	cut2, err := BuildMergedConf(own2, cut2cfg)
	if err != nil {
		t.Fatalf("cutover render (monitor present): %v", err)
	}
	if rec2 != cut2 {
		t.Errorf("when the live conf already carries the same monitor address the two renders must be "+
			"byte-identical with no exception at all:\n--- cutover ---\n%s\n--- reconciler ---\n%s", cut2, rec2)
	}
}

// dropLinesWithPrefix removes every line whose trimmed form starts with prefix. Used to subtract the ONE
// declared difference between the two renders, so the assertion is "identical modulo the monitor" rather
// than a substring check that would pass while other fields drifted.
func dropLinesWithPrefix(s, prefix string) string {
	lines := strings.Split(s, "\n")
	kept := make([]string, 0, len(lines))
	for _, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), prefix) {
			continue
		}
		kept = append(kept, l)
	}
	return strings.Join(kept, "\n")
}
