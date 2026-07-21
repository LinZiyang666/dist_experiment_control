package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// stubCallAdmin swaps the callAdmin seam and restores it.
func stubCallAdmin(t *testing.T, fn func(string, adminsock.Request) (*adminsock.Response, error)) {
	t.Helper()
	orig := callAdmin
	callAdmin = fn
	t.Cleanup(func() { callAdmin = orig })
}

// TestAdminRuntimeJSONStreamStaysClean is the R11 discipline applied to the new verb: with --json,
// the SUCCESS path must put ONLY machine JSON on stdout so `admin runtime --json 2>&1 | jq` never
// sees prose mixed into the JSON stream. We MERGE stdout and stderr (the R11 lesson: test cleanliness
// AT the sink) and assert the merged output parses as the RuntimeReport.
func TestAdminRuntimeJSONStreamStaysClean(t *testing.T) {
	stubCallAdmin(t, func(_ string, req adminsock.Request) (*adminsock.Response, error) {
		if req.Op != adminsock.OpRuntime {
			t.Fatalf("unexpected op %q", req.Op)
		}
		return &adminsock.Response{Op: adminsock.OpRuntime, OK: true, Runtime: &adminsock.RuntimeReport{
			Schema: "admin_runtime", SchemaVersion: 1, Goroutines: 12, Threads: 4, OpenFDs: 9, RSSBytes: 1 << 20, UptimeSeconds: 5,
			Reconcilers: []adminsock.ReconcilerTick{{Name: "ports", IntervalMS: 1000, LeaderOnly: true, Runs: 3}},
		}}, nil
	})

	// Single merged sink for stdout+stderr, exactly as `2>&1` presents it.
	var merged bytes.Buffer
	root := newRootCmd()
	root.SetOut(&merged)
	root.SetErr(&merged)
	root.SetArgs([]string{"admin", "runtime", "--json"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}

	var got adminsock.RuntimeReport
	if err := json.Unmarshal(merged.Bytes(), &got); err != nil {
		t.Fatalf("merged stdout+stderr is not clean JSON (%v):\n%s", err, merged.String())
	}
	if got.Schema != "admin_runtime" || got.Goroutines != 12 {
		t.Errorf("decoded report wrong: %+v", got)
	}
}

// TestAdminRuntimeJSONFailureWritesNothingToStdout: on a transport failure the --json path must NOT
// emit a partial/garbage object to stdout — the failure travels via a non-zero exit + a stderr line,
// so a `2>&1 | jq` sees no corrupt JSON prefix. (Distinct from R11's doctor case, which produced a
// VALID JSON body; here there is simply no report, so nothing must land on stdout.)
func TestAdminRuntimeJSONFailureWritesNothingToStdout(t *testing.T) {
	stubCallAdmin(t, func(string, adminsock.Request) (*adminsock.Response, error) {
		return nil, unavailErr("admin socket /x: dial: no such file")
	})
	var out bytes.Buffer
	root := newRootCmd()
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{}) // stderr goes elsewhere; stdout must stay empty
	root.SetArgs([]string{"admin", "runtime", "--json"})
	if err := root.Execute(); err == nil {
		t.Fatal("expected a transport error")
	}
	if out.Len() != 0 {
		t.Errorf("stdout must be empty on failure, got %q", out.String())
	}
}
