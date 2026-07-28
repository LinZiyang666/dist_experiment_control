package broker

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// clusterstatus_schema_agreement_test.go — every producer of a ClusterStatusReport must stamp the SAME
// schema version, and that version must be the one constant.
//
// origin: batch B2 independent external review B2-2
//
// WHAT WENT WRONG
// ---------------
// `stream_actual` acquired the sentinel -1 ("observation did not complete") while `schema_version`
// stayed 1. docs/usage.md's bump policy names a changed key MEANING as breaking, so v1 collectors could
// read -1 as malformed, as a real count, or as a catastrophic replica deficit, with nothing to tell them
// the domain had widened. That is now v2.
//
// The second half of the defect is the one that would have come back. ClusterStatusReport has TWO
// producers — the broker's live socket view and the CLI's offline disk-snapshot view — and each carried
// its own literal `SchemaVersion: 1`. Bumping only the one that emits the sentinel would leave one
// struct claiming two versions, which is strictly worse than not bumping: a monitor dispatching on
// (view, schema_version) sees a version that depends on which command produced the report. Both now
// reference adminsock.ClusterStatusSchemaVersion, and this file is what keeps that true.
//
// The reviewer's own counterexample (TestClusterStatusSchemaBumpsWhenStreamActualGainsAnUnobservedSentinel)
// pins the FIRST half: sentinel present => version >= 2. It is kept as the permanent negative control.
// This file pins the second half, which a single-producer test cannot see.

func TestClusterStatusProducersAgreeOnTheSchemaVersion(t *testing.T) {
	if statusSchemaVersion != adminsock.ClusterStatusSchemaVersion {
		t.Errorf("the broker's statusSchemaVersion is %d but adminsock.ClusterStatusSchemaVersion is %d — "+
			"one struct may not claim two versions; the constant next to the struct is the SSOT",
			statusSchemaVersion, adminsock.ClusterStatusSchemaVersion)
	}

	// The offline producer lives in cmd/tether, which this package cannot import (it is package main).
	// Read it as source: the check is that it does NOT stamp an integer literal, because a literal is
	// exactly the drift this test exists to prevent.
	root := repoRootForSchemaAgreement(t)
	path := filepath.Join(root, "cmd", "tether", "cluster.go")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	f, err := parser.ParseFile(token.NewFileSet(), path, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	found := 0
	ast.Inspect(f, func(n ast.Node) bool {
		kv, ok := n.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != "SchemaVersion" {
			return true
		}
		found++
		if lit, isLit := kv.Value.(*ast.BasicLit); isLit && lit.Kind == token.INT {
			t.Errorf("cmd/tether/cluster.go stamps SchemaVersion as the literal %s. The offline view emits "+
				"the SAME adminsock.ClusterStatusReport as the socket view, so a literal here drifts the "+
				"moment the socket view bumps — which is exactly what happened at v1->v2. Reference "+
				"adminsock.ClusterStatusSchemaVersion instead.", lit.Value)
		}
		return true
	})
	// NON-VACUITY, and legitimate here (docs/testing-standards.md G2b): the success state of this check
	// still contains the offline producer's SchemaVersion field, so finding none means the scan went blind
	// (the file moved, the field was renamed), not that the codebase got clean.
	if found == 0 {
		t.Fatalf("found no SchemaVersion assignment in %s — this cross-producer check has gone blind, so "+
			"the offline view could silently stamp an old version", path)
	}
}

// TestClusterStatusV2SentinelIsWireHonest pins the v2 contract the bump was for: -1 must survive JSON
// round-trip as -1 (not be omitted, not become 0), and it must compare as below-target so every
// at-target decision stays fail-closed.
//
// This is the behavioural half of the bump. A version number that nothing checks is bookkeeping; what a
// v2 collector actually needs is that the sentinel is present, negative, and distinguishable from the
// measurement 0.
func TestClusterStatusV2SentinelIsWireHonest(t *testing.T) {
	rep := adminsock.ClusterStatusReport{
		SchemaVersion: statusSchemaVersion,
		View:          "ctl-nats",
		Nodes: []adminsock.ClusterNodeStatus{
			{NodeID: "unobserved", StreamActual: StreamActualUnobserved, StreamTarget: 3},
			{NodeID: "measured-zero", StreamActual: 0, StreamTarget: 3},
		},
	}
	payload, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	var back adminsock.ClusterStatusReport
	if err := json.Unmarshal(payload, &back); err != nil {
		t.Fatalf("v2 payload does not round-trip: %v\n%s", err, payload)
	}

	if back.SchemaVersion < 2 {
		t.Fatalf("a payload carrying the sentinel must declare >= v2, got %d: %s", back.SchemaVersion, payload)
	}
	if !strings.Contains(string(payload), `"stream_actual":-1`) {
		t.Errorf("the sentinel must appear on the wire as -1. If it were omitempty-omitted or coerced to 0 "+
			"a collector could not distinguish 'not measured' from 'measured zero replicas', which is the "+
			"fabricated-measurement defect the sentinel replaced:\n%s", payload)
	}
	if back.Nodes[0].StreamActual != StreamActualUnobserved {
		t.Errorf("sentinel round-tripped to %d, want %d", back.Nodes[0].StreamActual, StreamActualUnobserved)
	}
	if back.Nodes[1].StreamActual != 0 {
		t.Errorf("a genuine measurement of 0 must stay 0 and remain distinct from the sentinel, got %d",
			back.Nodes[1].StreamActual)
	}
	// Fail-closed: the sentinel must never read as at-target.
	if back.Nodes[0].StreamActual >= back.Nodes[0].StreamTarget {
		t.Errorf("the sentinel (%d) must compare BELOW stream_target (%d), or an unobserved node reads as "+
			"converged and every at-target gate fails open (§S1)",
			back.Nodes[0].StreamActual, back.Nodes[0].StreamTarget)
	}
}

func repoRootForSchemaAgreement(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatal("could not locate the repo root")
	return ""
}
