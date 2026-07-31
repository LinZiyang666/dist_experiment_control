package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/spf13/cobra"
)

// stubFetch swaps the fetchClusterStatusReport seam for a test and restores it.
func stubFetch(t *testing.T, fn func(string) (*adminsock.ClusterStatusReport, error)) {
	t.Helper()
	orig := fetchClusterStatusReport
	fetchClusterStatusReport = fn
	t.Cleanup(func() { fetchClusterStatusReport = orig })
}

func waitCmd(ctx context.Context, t *testing.T) *cobra.Command {
	t.Helper()
	c := &cobra.Command{}
	c.SetOut(&testBuf{})
	c.SetErr(&testBuf{})
	c.SetContext(ctx)
	return c
}

type testBuf struct{}

func (testBuf) Write(p []byte) (int, error) { return len(p), nil }

// TestB5WaitForConvergeConverges: a pred that reports done returns nil immediately.
func TestB5WaitForConvergeConverges(t *testing.T) {
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		return &adminsock.ClusterStatusReport{Nodes: []adminsock.ClusterNodeStatus{{NodeID: "n1", Phase: "VOTER"}}}, nil
	})
	pred := func(n *adminsock.ClusterNodeStatus, _ *adminsock.ClusterStatusReport) (bool, string) {
		return n != nil && n.Phase == "VOTER", ""
	}
	if err := waitForConverge(waitCmd(context.Background(), t), "/sock", "n1", pred, time.Minute, time.Second); err != nil {
		t.Fatalf("converged pred must return nil, got %v", err)
	}
}

// TestB5WaitForConvergeTimeout (plan §F.12): timeout → exit 75 (transient). A fake clock that
// advances past the deadline on the first check makes it deterministic without the 2s ticker.
func TestB5WaitForConvergeTimeout(t *testing.T) {
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		return &adminsock.ClusterStatusReport{Nodes: []adminsock.ClusterNodeStatus{{NodeID: "n1", Phase: "CATCHING_UP"}}}, nil
	})
	base := time.Date(2026, 6, 25, 0, 0, 0, 0, time.UTC)
	calls := 0
	orig := nowFunc
	nowFunc = func() time.Time {
		calls++
		return base.Add(time.Duration(calls) * time.Hour) // advances every call → past any tiny deadline
	}
	t.Cleanup(func() { nowFunc = orig })

	pred := func(n *adminsock.ClusterNodeStatus, _ *adminsock.ClusterStatusReport) (bool, string) {
		return false, ""
	}
	err := waitForConverge(waitCmd(context.Background(), t), "/sock", "n1", pred, time.Nanosecond, time.Second)
	var ee *ExitError
	if !errors.As(err, &ee) || ee.Class != exitTransient {
		t.Fatalf("timeout must be exit 75 (transient), got %v", err)
	}
}

// TestB5WaitForConvergeCancel (plan §F.12): a cancelled context → exit 75, promptly (no ticker wait).
func TestB5WaitForConvergeCancel(t *testing.T) {
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		return &adminsock.ClusterStatusReport{Nodes: []adminsock.ClusterNodeStatus{{NodeID: "n1", Phase: "CATCHING_UP"}}}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled
	pred := func(n *adminsock.ClusterNodeStatus, _ *adminsock.ClusterStatusReport) (bool, string) {
		return false, ""
	}
	err := waitForConverge(waitCmd(ctx, t), "/sock", "n1", pred, 0, time.Second) // timeout 0 = no deadline
	var ee *ExitError
	if !errors.As(err, &ee) || ee.Class != exitTransient {
		t.Fatalf("ctx-cancel must be exit 75 (transient), got %v", err)
	}
}

// TestB5WaitForConvergeFailTerminal: a failure-terminal pred (VOTER_ADD_FAILED) returns immediately
// with a non-transient error (NOT exit 75 — it must not be retried).
func TestB5WaitForConvergeFailTerminal(t *testing.T) {
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		return &adminsock.ClusterStatusReport{Nodes: []adminsock.ClusterNodeStatus{{NodeID: "n1", Phase: "VOTER_ADD_FAILED"}}}, nil
	})
	pred := func(n *adminsock.ClusterNodeStatus, _ *adminsock.ClusterStatusReport) (bool, string) {
		if n != nil && n.Phase == "VOTER_ADD_FAILED" {
			return false, "node entered VOTER_ADD_FAILED"
		}
		return false, ""
	}
	err := waitForConverge(waitCmd(context.Background(), t), "/sock", "n1", pred, time.Minute, time.Second)
	if err == nil {
		t.Fatal("failure-terminal must return an error")
	}
	var ee *ExitError
	if errors.As(err, &ee) && ee.Class == exitTransient {
		t.Fatalf("failure-terminal must NOT be exit 75 (transient): %v", err)
	}
}

// TestB5WaitForConvergeFirstMatch: a roster listing the node TWICE (roster row + orphan-voter
// append) passes the FIRST match to the predicate deterministically.
func TestB5WaitForConvergeFirstMatch(t *testing.T) {
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		return &adminsock.ClusterStatusReport{Nodes: []adminsock.ClusterNodeStatus{
			{NodeID: "n1", Phase: "VOTER"},            // first
			{NodeID: "n1", Phase: "VOTER_ADD_FAILED"}, // orphan duplicate
		}}, nil
	})
	var sawPhase string
	pred := func(n *adminsock.ClusterNodeStatus, _ *adminsock.ClusterStatusReport) (bool, string) {
		if n != nil {
			sawPhase = n.Phase
		}
		return true, "" // converge so it returns after one observation
	}
	_ = waitForConverge(waitCmd(context.Background(), t), "/sock", "n1", pred, time.Minute, time.Second)
	if sawPhase != "VOTER" {
		t.Fatalf("pred must see the FIRST matching row (VOTER), saw %q", sawPhase)
	}
}

// TestB5RenderTakeoverPlanNoMutation (plan §F.18, BLOCKER-tier): --plan rendering reads the conf
// but writes NOTHING — the file bytes + mtime are unchanged and no .bak is created. `changed` is
// true when the merged conf differs from the current bytes.
func TestB5RenderTakeoverPlanNoMutation(t *testing.T) {
	dir := t.TempDir()
	confPath := filepath.Join(dir, "nats.conf")
	original := []byte("host: 127.0.0.1\nport: 4222\n")
	if err := os.WriteFile(confPath, original, 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(confPath)
	if err != nil {
		t.Fatal(err)
	}

	peers := []natsconf.Broker{{ServerName: "brk-a", NkeyPub: "Uabc", RouteURL: "nats://10.0.0.1:6222"}}
	cmd := waitCmd(context.Background(), t)
	if err := renderTakeoverPlan(cmd, confPath, "brk-a", "127.0.0.1:4222", "/var/lib/js", peers, "MERGED DIFFERENT CONTENT", "ok", true); err != nil {
		t.Fatalf("renderTakeoverPlan (json): %v", err)
	}
	if err := renderTakeoverPlan(cmd, confPath, "brk-a", "127.0.0.1:4222", "/var/lib/js", peers, "MERGED DIFFERENT CONTENT", "ok", false); err != nil {
		t.Fatalf("renderTakeoverPlan (text): %v", err)
	}

	after, err := os.Stat(confPath)
	if err != nil {
		t.Fatal(err)
	}
	now, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(now) != string(original) {
		t.Fatal("--plan must NOT modify the conf bytes")
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("--plan must NOT touch the conf mtime")
	}
	if baks, _ := filepath.Glob(confPath + ".bak*"); len(baks) != 0 {
		t.Fatalf("--plan must NOT create a .bak, found %v", baks)
	}
}

// captureBuf collects everything written to it, unlike testBuf which discards. The JSONL contract is
// a property of the BYTES, so asserting it needs them.
type captureBuf struct{ b []byte }

func (c *captureBuf) Write(p []byte) (int, error) { c.b = append(c.b, p...); return len(p), nil }
func (c *captureBuf) String() string              { return string(c.b) }

// origin: line-2 external review PC-1 (the round's only CRITICAL).
//
// `cluster status --watch --json` must emit ONE parseable JSON object per line and nothing else —
// BLK-1 (Stage-C), which B5's external review already had to repair once. It broke a SECOND time as a
// side effect of a pure lint cleanup: the frame preamble was a three-way if/else-if/else whose --json
// arm was an EMPTY BLOCK, revive's empty-block flagged the empty block, and collapsing three arms into
// two (`if !asJSON && isTTY {clear} else {separator}`) moved --json into the separator arm. Every frame
// then began with `--- <ts> ---`, so a `jq -c` reader failed on line 1.
//
// The reason it could break twice is that watchClusterStatus had NO test at all. This is that test. It
// asserts the property (every line parses) rather than the shape of the code, so it survives the
// preamble being rewritten again — including into whatever form the next linter prefers.
func TestClusterStatusWatchJSONEmitsOnlyJSONLines(t *testing.T) {
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		return &adminsock.ClusterStatusReport{
			Health: "HEALTHY_HA",
			Nodes:  []adminsock.ClusterNodeStatus{{NodeID: "n1", Phase: "VOTER"}},
		}, nil
	})

	out := &captureBuf{}
	cmd := &cobra.Command{}
	cmd.SetOut(out)
	cmd.SetErr(&testBuf{})
	// Cancelled after the first frame: watchClusterStatus prints and fetches BEFORE its select, so one
	// full frame always lands, and the select then takes the ctx.Done arm instead of waiting out the
	// 2s floor.
	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	cancel()

	if err := watchClusterStatus(cmd, "/sock", true, minWatchInterval); err != nil {
		t.Fatalf("watchClusterStatus: %v", err)
	}

	got := out.String()
	if strings.TrimSpace(got) == "" {
		t.Fatal("no output at all — the frame did not run, so this test would pass vacuously")
	}
	for i, line := range strings.Split(strings.TrimRight(got, "\n"), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var probe map[string]any
		if err := json.Unmarshal([]byte(line), &probe); err != nil {
			t.Errorf("line %d of --watch --json output is not JSON: %q\n  (%v)\n\n"+
				"Every line must be one object: a JSONL consumer reads line-by-line and dies on the "+
				"first non-JSON byte. A screen clear, a `--- <ts> ---` separator or a human-readable "+
				"table all break the contract identically.", i+1, line, err)
		}
	}
}

// TestClusterStatusWatchNonJSONStillSeparatesFrames is the other half: fixing the --json arm must not
// silently drop the separator that the non-TTY human path relies on to tell frames apart. Without this,
// "make --json clean" could be satisfied by deleting the preamble entirely.
func TestClusterStatusWatchNonJSONStillSeparatesFrames(t *testing.T) {
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		return &adminsock.ClusterStatusReport{Health: "HEALTHY_HA"}, nil
	})

	out := &captureBuf{}
	cmd := &cobra.Command{}
	cmd.SetOut(out)
	cmd.SetErr(&testBuf{})
	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	cancel()

	if err := watchClusterStatus(cmd, "/sock", false, minWatchInterval); err != nil {
		t.Fatalf("watchClusterStatus: %v", err)
	}
	// go test's stdout is not a terminal, so isTTY is false and the default arm must run.
	if !strings.Contains(out.String(), "--- ") {
		t.Errorf("non-JSON watch on a non-TTY must print a `--- <ts> ---` frame separator; got:\n%s",
			out.String())
	}
}

// TestClusterStatusWatchJSONEmitsAFrameEvenWhenFetchFails pins the JSONL contract on the FAILURE path.
//
// origin: line-2 closure verification §6 B2. `--watch --json` wrote the retry notice to stderr and nothing
// to stdout when the admin socket did not answer, so a stream consumer saw a silent gap — indistinguishable
// from "the cluster is fine and quiet". The file's own comment claimed "each frame is one parseable JSON
// object per line", which was true only while the socket was healthy.
//
// Driven through a socket path that cannot exist, so fetchClusterStatusReport fails for real rather than
// through a stub. The watcher loops forever by design, so the context is cancelled after the first frame.
func TestClusterStatusWatchJSONEmitsAFrameEvenWhenFetchFails(t *testing.T) {
	var out, errBuf bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	cmd.SetErr(&errBuf)

	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	cmd.SetContext(ctx)

	// minWatchInterval is the floor; one frame lands immediately, then the ticker waits and the context
	// expires, so exactly one failed frame is produced.
	if err := watchClusterStatus(cmd, filepath.Join(t.TempDir(), "no-such.sock"), true, minWatchInterval); err != nil {
		t.Fatalf("watch returned %v, want nil (a watch exits 0 on cancel)", err)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) == 0 || lines[0] == "" {
		t.Fatalf("stdout is EMPTY on a failed frame — the JSONL stream has a silent gap.\n"+
			"stderr was: %q\n\nA consumer counting frames cannot tell this from a healthy quiet cluster, "+
			"which is the whole reason the failure path has to emit a line.", errBuf.String())
	}
	for i, line := range lines {
		var probe map[string]any
		if err := json.Unmarshal([]byte(line), &probe); err != nil {
			t.Errorf("stdout line %d is not JSON: %q (%v) — every --json frame must be parseable", i+1, line, err)
			continue
		}
		if probe["error"] == nil || probe["stage"] != "fetch" {
			t.Errorf("line %d = %q; want an object carrying `error` and stage \"fetch\"", i+1, line)
		}
		// origin: line-2 external review 疑惑 #1 — a heterogeneous JSONL stream is only decodable if the
		// error line carries the SAME (schema, schema_version) discriminator every other machine-JSON
		// payload carries. Without it a strict decoder that negotiated a version has to exit on the first
		// transient socket error, which is the moment it is most needed.
		if probe["schema"] != watchFrameErrorSchema {
			t.Errorf("line %d schema = %v, want %q — a versioned decoder cannot classify this line",
				i+1, probe["schema"], watchFrameErrorSchema)
		}
		if v, ok := probe["schema_version"].(float64); !ok || int(v) != watchFrameErrorSchemaVersion {
			t.Errorf("line %d schema_version = %v, want %d", i+1, probe["schema_version"], watchFrameErrorSchemaVersion)
		}
	}
	// The human-facing notice must survive too: this fix adds a stdout line, it does not move the message.
	if !strings.Contains(errBuf.String(), "retrying") {
		t.Errorf("stderr lost the retry notice (%q) — the fix is additive, a human watching the terminal "+
			"still needs it", errBuf.String())
	}
}

// TestWatchJSONLDiscriminatorSeparatesFrameErrorsFromDegradedFrames pins the heterogeneous-JSONL contract
// from BOTH sides, and pins the singular/plural trap that makes it silently mis-decodable.
//
// origin: line-2 external review 疑惑 #1. The reviewer's concern was that a strict decoder exits on a
// transient error line. The other half of that concern — the one that is silent rather than loud — is that
// a LENIENT decoder gets the two shapes backwards: a status frame that DID arrive carries `errors` (plural,
// broker-side problems), and a frame that did NOT arrive carries `error` (singular). A monitor keying on the
// wrong one reads a degraded-but-alive cluster as a dead admin socket, and stays quiet about a real outage.
//
// Adversarial on purpose: the degraded sample is the shape most likely to be confused (non-empty `errors`,
// partial=true), not an all-clear report.
func TestWatchJSONLDiscriminatorSeparatesFrameErrorsFromDegradedFrames(t *testing.T) {
	frameErr, err := json.Marshal(watchFrameError{
		Schema: watchFrameErrorSchema, SchemaVersion: watchFrameErrorSchemaVersion,
		Error: "dial /run/tether/admin.sock: connection refused", Stage: "fetch", At: "2026-07-30T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal frame error: %v", err)
	}
	degraded, err := json.Marshal(adminsock.ClusterStatusReport{
		SchemaVersion: 1, View: "ctl-nats", Health: "DEGRADED", ExitCode: 1,
		Errors: []string{"brk2: no response"}, Partial: true,
	})
	if err != nil {
		t.Fatalf("marshal degraded report: %v", err)
	}

	classify := func(line []byte) string {
		var m map[string]any
		if uerr := json.Unmarshal(line, &m); uerr != nil {
			return "unparseable"
		}
		switch {
		case m["schema"] == watchFrameErrorSchema:
			return "frame-error"
		case m["view"] != nil:
			return "report"
		}
		return "unclassified"
	}

	for _, tc := range []struct {
		name string
		line []byte
		want string
	}{
		{"a frame that never arrived", frameErr, "frame-error"},
		{"a frame that arrived carrying broker errors", degraded, "report"},
	} {
		if got := classify(tc.line); got != tc.want {
			t.Errorf("%s classified as %q, want %q\nline: %s", tc.name, got, tc.want, tc.line)
		}
	}

	// The trap, asserted rather than described: singular vs plural must not overlap in either direction.
	var fe, rp map[string]any
	if err := json.Unmarshal(frameErr, &fe); err != nil {
		t.Fatalf("unmarshal frame error: %v", err)
	}
	if err := json.Unmarshal(degraded, &rp); err != nil {
		t.Fatalf("unmarshal degraded report: %v", err)
	}
	if _, bad := fe["errors"]; bad {
		t.Errorf("the frame-error line grew an `errors` key: %s\nA monitor selecting on `errors` would now "+
			"count dead sockets as degraded frames.", frameErr)
	}
	if _, bad := rp["error"]; bad {
		t.Errorf("the status report grew a singular `error` key: %s\nA monitor selecting on `error` would now "+
			"count every degraded frame as a dead admin socket.", degraded)
	}
	if _, ok := rp["errors"]; !ok {
		t.Errorf("the status report lost `errors`: %s — it is branch-load-bearing and must never be omitempty",
			degraded)
	}
}
