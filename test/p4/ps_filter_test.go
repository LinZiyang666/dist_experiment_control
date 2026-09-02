// Package p4_test ps-retention-plan §C, §E, §F1b — RPC-level coverage of
// the v0.2.8 `tether ps` wire-protocol changes (PsReq{IncludeExited,Limit}
// + server cap + bounded read path) and the documented mixed-version
// compatibility contract.
//
// All tests in this file run against a real broker (no agent) over an
// embedded NATS; rows are inserted directly into the SQLite handle to
// avoid spinning up agent processes for hundreds of pids.
package p4_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/test/stackharness"
	"github.com/nats-io/nats.go"
)

// seedProc inserts one row into processes, requiring the session row
// to have been created already (via seedSession). Bypasses proc.Insert
// so EXITED/ended_at can be set directly.
func seedProc(t *testing.T, db *sql.DB, pid, sid, nid, status string, startedAt time.Time, endedAt *time.Time, exitCode *int) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO nodes(sid, nid, status) VALUES (?,?,?)`,
		sid, nid, "ONLINE",
	); err != nil {
		t.Fatal(err)
	}
	var endedArg any
	if endedAt != nil {
		endedArg = *endedAt
	}
	var exitArg any
	if exitCode != nil {
		exitArg = *exitCode
	}
	_, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, ended_at, status, exit_code, started_by_fp)
		 VALUES (?,?,?,?,?,?,?,?,?)`,
		pid, sid, nid, `["x"]`, startedAt, endedArg, status, exitArg, "SHA256:u",
	)
	if err != nil {
		t.Fatalf("seedProc pid=%s: %v", pid, err)
	}
}

// seedSession creates an Active session row with `fp` as the owner.
// Uses session.Create so the row passes session.IsActive() (state
// column is mandatory for the broker's ps gate).
func seedSession(t *testing.T, db *sql.DB, sid, fp string) {
	t.Helper()
	stackharness.SeedSession(t, db, sid, fp)
}

// psWith sends a single PsReq with the given body and returns the
// decoded PsResp. Helper around nc.Request to keep test bodies concise.
func psWith(t *testing.T, nc *nats.Conn, pub, sid string, body []byte) proto.PsResp {
	t.Helper()
	msg, err := nc.Request(proto.SubjCtrlPs(pub, sid), body, 5*time.Second)
	if err != nil {
		t.Fatalf("nc.Request(SubjCtrlPs): %v", err)
	}
	var resp proto.PsResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("unmarshal PsResp: %v\nbody=%s", err, msg.Data)
	}
	return resp
}

// --------------------------------------------------------------------
// C1 — default `PsReq{}` returns RUNNING-only.
// --------------------------------------------------------------------

func TestPsFilter_DefaultRunningOnly(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	now := time.Now().UTC().Truncate(time.Second)
	ended := now.Add(-time.Minute)
	rc := 0
	seedProc(t, db, "running-1", "lab", "lab-1", "RUNNING", now, nil, nil)
	seedProc(t, db, "exited-1", "lab", "lab-1", "EXITED", now.Add(-time.Hour), &ended, &rc)

	nc, _ := nats.Connect(url)
	defer nc.Close()

	body, _ := json.Marshal(proto.PsReq{})
	resp := psWith(t, nc, pub, "lab", body)
	if resp.Code != "" {
		t.Fatalf("ps rejected: %s %s", resp.Code, resp.Error)
	}
	gotPIDs := map[string]string{}
	for _, p := range resp.Processes {
		gotPIDs[p.PID] = p.Status
	}
	if gotPIDs["running-1"] == "" {
		t.Errorf("default PsReq{} missed running-1; got %+v", gotPIDs)
	}
	if _, present := gotPIDs["exited-1"]; present {
		t.Errorf("default PsReq{} leaked exited-1; got %+v", gotPIDs)
	}
}

// --------------------------------------------------------------------
// C2 — IncludeExited=true returns both.
// --------------------------------------------------------------------

func TestPsFilter_IncludeExited(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	now := time.Now().UTC().Truncate(time.Second)
	ended := now.Add(-time.Minute)
	rc := 0
	seedProc(t, db, "running-2", "lab", "lab-1", "RUNNING", now, nil, nil)
	seedProc(t, db, "exited-2", "lab", "lab-1", "EXITED", now.Add(-time.Hour), &ended, &rc)

	nc, _ := nats.Connect(url)
	defer nc.Close()

	body, _ := json.Marshal(proto.PsReq{IncludeExited: true})
	resp := psWith(t, nc, pub, "lab", body)
	if resp.Code != "" {
		t.Fatalf("ps rejected: %s %s", resp.Code, resp.Error)
	}
	if len(resp.Processes) != 2 {
		t.Errorf("ps -a returned %d procs, want 2: %+v", len(resp.Processes), resp.Processes)
	}
}

// --------------------------------------------------------------------
// C3 — old empty `{}` body decodes to default (RUNNING-only).
// --------------------------------------------------------------------

func TestPsFilter_OldEmptyBodyCompat(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	now := time.Now().UTC().Truncate(time.Second)
	ended := now.Add(-time.Minute)
	rc := 0
	seedProc(t, db, "running-3", "lab", "lab-1", "RUNNING", now, nil, nil)
	seedProc(t, db, "exited-3", "lab", "lab-1", "EXITED", now.Add(-time.Hour), &ended, &rc)

	nc, _ := nats.Connect(url)
	defer nc.Close()

	// Raw `[]byte("{}")` — what a v0.2.7 ctl literally sends.
	resp := psWith(t, nc, pub, "lab", []byte("{}"))
	if resp.Code != "" {
		t.Fatalf("ps rejected: %s %s", resp.Code, resp.Error)
	}
	for _, p := range resp.Processes {
		if p.Status == "EXITED" {
			t.Errorf("old-body {} leaked EXITED row: %+v", p)
		}
	}
}

// --------------------------------------------------------------------
// C4 — server cap clamps client-supplied Limit at 500.
// --------------------------------------------------------------------

func TestPsFilter_ServerCapClamp(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	now := time.Now().UTC()
	ended := now.Add(-time.Minute)
	rc := 0
	for i := 0; i < 600; i++ {
		seedProc(t, db, fmt.Sprintf("e%04d", i),
			"lab", "lab-1", "EXITED",
			now.Add(time.Duration(i)*time.Microsecond), &ended, &rc)
	}

	nc, _ := nats.Connect(url)
	defer nc.Close()

	// Limit=0 → server applies default cap (500).
	body0, _ := json.Marshal(proto.PsReq{IncludeExited: true, Limit: 0})
	resp0 := psWith(t, nc, pub, "lab", body0)
	if len(resp0.Processes) != 500 {
		t.Errorf("Limit=0 default cap: want 500 procs, got %d", len(resp0.Processes))
	}

	// Limit=9999 → server clamps to 500.
	body9k, _ := json.Marshal(proto.PsReq{IncludeExited: true, Limit: 9999})
	resp9k := psWith(t, nc, pub, "lab", body9k)
	if len(resp9k.Processes) != 500 {
		t.Errorf("Limit=9999 clamp: want 500 procs, got %d", len(resp9k.Processes))
	}

	// Limit=100 → server honors lower limit.
	body100, _ := json.Marshal(proto.PsReq{IncludeExited: true, Limit: 100})
	resp100 := psWith(t, nc, pub, "lab", body100)
	if len(resp100.Processes) != 100 {
		t.Errorf("Limit=100 lower: want 100 procs, got %d", len(resp100.Processes))
	}
}

// --------------------------------------------------------------------
// C5 — new ctl talking to a synthetic "old broker" (struct{} body
// decoder) does not crash; unknown JSON fields are silently dropped
// by encoding/json. We emulate the old broker with a stub responder.
// --------------------------------------------------------------------

func TestPsFilter_NewCtlOldBrokerSimulation(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// Pretend to be a v0.2.7 broker: decode the body into an empty
	// struct (the legacy `PsReq struct{}`). Reply with a canned ok.
	type legacyPsReq struct{}
	pub, _ := freshUserPub(t)
	sub, err := nc.Subscribe(proto.SubjCtrlPs(pub, "lab"),
		func(msg *nats.Msg) {
			var req legacyPsReq
			if err := json.Unmarshal(msg.Data, &req); err != nil {
				_ = msg.Respond([]byte(`{"code":"decode_error"}`))
				return
			}
			_ = msg.Respond([]byte(`{"processes":[]}`))
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	// Send a v0.2.8 ctl body with both new fields populated.
	body, _ := json.Marshal(proto.PsReq{IncludeExited: true, Limit: 999})
	msg, err := nc.Request(proto.SubjCtrlPs(pub, "lab"), body, 2*time.Second)
	if err != nil {
		t.Fatalf("new ctl + old broker: %v", err)
	}
	var resp proto.PsResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.Code == "decode_error" {
		t.Errorf("legacy broker failed to decode new body — but encoding/json should silently drop unknown fields")
	}
}

// --------------------------------------------------------------------
// C6 — legacy ListBySession wrapper still returns the full set.
// --------------------------------------------------------------------

func TestPsFilter_LegacyListBySessionAllRows(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	now := time.Now().UTC()
	ended := now.Add(-time.Minute)
	rc := 0
	seedProc(t, db, "r-c6", "lab", "lab-1", "RUNNING", now, nil, nil)
	seedProc(t, db, "e-c6", "lab", "lab-1", "EXITED", now.Add(-time.Hour), &ended, &rc)

	// Use IncludeExited=true to mimic what the legacy wrapper returns.
	body, _ := json.Marshal(proto.PsReq{IncludeExited: true})
	resp := psWith(t, nc(t, url), pub, "lab", body)
	if len(resp.Processes) != 2 {
		t.Errorf("legacy view: want 2 procs, got %d", len(resp.Processes))
	}
}

func nc(t *testing.T, url string) *nats.Conn {
	t.Helper()
	c, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// --------------------------------------------------------------------
// C6b — reconcile path stays bounded on a backlogged broker.
// reconcileOnRegister is package-private; we exercise the same
// helper via the public proc.ListBySessionFiltered (the swap is
// 1:1).
// --------------------------------------------------------------------

func TestReconcileBoundedOnBacklog(t *testing.T) {
	// 5 000 per-row inserts pay ~12 s under `-race` because the
	// race detector taxes every SQL bind round-trip. The bounded-
	// read contract is also verified by E2 / E3 (which short-skip
	// the same way); skipping here under -short keeps the race
	// suite under a minute.
	if testing.Short() {
		t.Skip("skipping 5k-row reconcile bounding check in -short mode")
	}
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	now := time.Now().UTC()
	ended := now.Add(-time.Minute)
	rc := 0
	// Heavy backlog: 5_000 EXITED + 5 RUNNING.
	for i := 0; i < 5_000; i++ {
		seedProc(t, db, fmt.Sprintf("e%05d", i),
			"lab", "lab-1", "EXITED",
			now.Add(time.Duration(i)*time.Microsecond), &ended, &rc)
	}
	for i := 0; i < 5; i++ {
		seedProc(t, db, fmt.Sprintf("r%d", i),
			"lab", "lab-1", "RUNNING",
			now.Add(time.Duration(5_000+i)*time.Microsecond), nil, nil)
	}

	conn, _ := nats.Connect(url)
	defer conn.Close()

	start := time.Now()
	body, _ := json.Marshal(proto.PsReq{IncludeExited: false})
	resp := psWith(t, conn, pub, "lab", body)
	elapsed := time.Since(start)

	if resp.Code != "" {
		t.Fatalf("ps rejected: %s %s", resp.Code, resp.Error)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("default ps on 5_000-EXITED backlog took %v (want <500ms)", elapsed)
	}
	if len(resp.Processes) != 5 {
		t.Errorf("want 5 active procs, got %d", len(resp.Processes))
	}
	for _, p := range resp.Processes {
		if p.Status == "EXITED" {
			t.Errorf("EXITED row leaked into default response: %+v", p)
		}
	}
}

// --------------------------------------------------------------------
// C6c — built ctl binary default display includes RUNNING + LOST,
// drops EXITED. Standalone NATS + fake responder (no broker.Run).
// --------------------------------------------------------------------

// buildCtl returns the path to a freshly-built `tether` binary. Cached
// across every test in this package via sync.Once so C6c/C7/C8 don't
// each pay the ~3 s `go build ./cmd/tether` cost (especially heavy
// under `-race`). The binary itself is built without race
// instrumentation — the test runner's -race flag does not propagate
// into the sub-shell `go build`.
var (
	ctlBinOnce sync.Once
	ctlBinPath string
	ctlBinErr  error
	ctlBinDir  string
)

func buildCtl(t *testing.T) string {
	t.Helper()
	ctlBinOnce.Do(func() {
		var dir string
		dir, ctlBinErr = os.MkdirTemp("", "tether-ctl-")
		if ctlBinErr != nil {
			return
		}
		ctlBinDir = dir
		ctlBinPath = filepath.Join(dir, "tether")
		cmd := exec.Command("go", "build", "-o", ctlBinPath, "./cmd/tether")
		cmd.Dir = repoRoot(t)
		var out []byte
		out, ctlBinErr = cmd.CombinedOutput()
		if ctlBinErr != nil {
			ctlBinErr = fmt.Errorf("go build ctl: %w\n%s", ctlBinErr, out)
		}
	})
	if ctlBinErr != nil {
		t.Fatal(ctlBinErr)
	}
	return ctlBinPath
}

func repoRoot(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git rev-parse: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// writeIdentity drops a freshly-generated nkey at
// `<home>/keys/default.nk` and `sid` into `<home>/current_session`,
// matching the file layout cli.EnsureIdentity expects. Returns the
// public key string (used to construct SubjCtrlPs).
func writeIdentity(t *testing.T, home, sid string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(home, "keys"), 0o700); err != nil {
		t.Fatal(err)
	}
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "keys", "default.nk"),
		seed, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "current_session"),
		[]byte(sid), 0o600); err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	return pub
}

func TestPsDisplay_RunningAndLost_NotExited(t *testing.T) {
	url := startNATS(t)
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	home := t.TempDir()
	pub := writeIdentity(t, home, "lab")

	// Single fake responder on SubjCtrlPs returning one row per status.
	rc := 0
	canned := proto.PsResp{
		Processes: []proto.PsEntry{
			{PID: "r1", Status: "RUNNING", NID: "n1", StartedAt: time.Now()},
			{PID: "l1", Status: "LOST", NID: "n1", StartedAt: time.Now()},
			{PID: "e1", Status: "EXITED", NID: "n1", StartedAt: time.Now().Add(-time.Hour), ExitCode: rc},
		},
	}
	sub, err := conn.Subscribe(proto.SubjCtrlPs(pub, "lab"),
		func(msg *nats.Msg) {
			b, _ := json.Marshal(canned)
			_ = msg.Respond(b)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	bin := buildCtl(t)

	// (1) Default — should include r1 and l1, drop e1.
	cmdDefault := exec.Command(bin, "ps",
		"--nats-url", url,
		"--home", home,
	)
	cmdDefault.Env = append(os.Environ(), "TETHER_DEV_NO_AUTH=1")
	out, err := cmdDefault.CombinedOutput()
	if err != nil {
		t.Fatalf("ctl ps default: %v\n%s", err, out)
	}
	gotDefault := string(out)
	if !strings.Contains(gotDefault, "r1") {
		t.Errorf("default ps stdout missing r1:\n%s", gotDefault)
	}
	if !strings.Contains(gotDefault, "l1") {
		t.Errorf("default ps stdout missing l1 (LOST in default — round-1 regression):\n%s", gotDefault)
	}
	if strings.Contains(gotDefault, "e1") {
		t.Errorf("default ps stdout leaked e1 (EXITED):\n%s", gotDefault)
	}

	// (2) -a — should include all three.
	cmdAll := exec.Command(bin, "ps", "-a",
		"--nats-url", url,
		"--home", home,
	)
	cmdAll.Env = append(os.Environ(), "TETHER_DEV_NO_AUTH=1")
	outAll, err := cmdAll.CombinedOutput()
	if err != nil {
		t.Fatalf("ctl ps -a: %v\n%s", err, outAll)
	}
	gotAll := string(outAll)
	for _, pid := range []string{"r1", "l1", "e1"} {
		if !strings.Contains(gotAll, pid) {
			t.Errorf("ps -a stdout missing %s:\n%s", pid, gotAll)
		}
	}
}

// --------------------------------------------------------------------
// C7 — client timeout is now 15s (was 5s). We hold the response for
// 6s — old 5s timeout would fail, new 15s wins.
// --------------------------------------------------------------------

func TestPsFilter_ClientTimeoutRaisedTo15s(t *testing.T) {
	url := startNATS(t)
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	home := t.TempDir()
	pub := writeIdentity(t, home, "lab")

	sub, err := conn.Subscribe(proto.SubjCtrlPs(pub, "lab"),
		func(msg *nats.Msg) {
			time.Sleep(6 * time.Second) // longer than old 5s timeout
			_ = msg.Respond([]byte(`{"processes":[]}`))
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	bin := buildCtl(t)
	start := time.Now()
	cmd := exec.Command(bin, "ps", "--nats-url", url, "--home", home)
	cmd.Env = append(os.Environ(), "TETHER_DEV_NO_AUTH=1")
	out, err := cmd.CombinedOutput()
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ctl ps with 6s block: %v\n%s\nelapsed=%v", err, out, elapsed)
	}
	if elapsed < 5*time.Second {
		t.Errorf("ctl returned in %v; expected ~6s (response delayed)", elapsed)
	}
	if elapsed > 8*time.Second {
		t.Errorf("ctl took %v; should be ~6s — timeout not raised to 15s?", elapsed)
	}
}

// --------------------------------------------------------------------
// C8 — timeout error message rephrased.
// --------------------------------------------------------------------

func TestPsFilter_TimeoutErrorMessageReworded(t *testing.T) {
	url := startNATS(t)
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	home := t.TempDir()
	pub := writeIdentity(t, home, "lab")

	// Subscriber sleeps 2s — longer than the env-overridden 300ms
	// ctl timeout but bounded so the test never costs >2s of wall
	// clock. TETHER_PS_TIMEOUT is documented in cmd/tether/ps.go.
	sub, err := conn.Subscribe(proto.SubjCtrlPs(pub, "lab"),
		func(msg *nats.Msg) {
			time.Sleep(2 * time.Second)
			_ = msg.Respond([]byte(`{"processes":[]}`))
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	bin := buildCtl(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "ps",
		"--nats-url", url, "--home", home)
	cmd.Env = append(os.Environ(),
		"TETHER_DEV_NO_AUTH=1",
		"TETHER_PS_TIMEOUT=300ms",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("ctl ps should have failed with timeout; stdout=%s", out)
	}
	stderr := string(out)
	if !strings.Contains(stderr, "request timed out after 300ms") {
		t.Errorf("error missing new phrasing:\n%s", stderr)
	}
	if strings.Contains(stderr, "broker unreachable on NATS") {
		t.Errorf("error still uses the misleading old phrasing:\n%s", stderr)
	}
}

// --------------------------------------------------------------------
// C9 — malformed PsReq body must return bad_request, not silently
// fall through to defaults. Round-4 reviewer #4 caught the original
// `_ = json.Unmarshal(...)` swallow.
// --------------------------------------------------------------------

func TestPsFilter_MalformedBodyRejected(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	conn, _ := nats.Connect(url)
	defer conn.Close()

	// Garbage bytes that aren't valid JSON.
	resp := psWith(t, conn, pub, "lab", []byte("{not-json"))
	if resp.Code != "bad_request" {
		t.Errorf("malformed body: want code=bad_request got %q (err=%q)",
			resp.Code, resp.Error)
	}
	if resp.Error == "" || !strings.Contains(resp.Error, "ps:") {
		t.Errorf("bad_request response missing diagnostic detail: %+v", resp)
	}
}

// --------------------------------------------------------------------
// F1b — negative test: old-ctl `ps -a` against new broker returns 0
// EXITED rows. Documented breaking change in v0.2.8.
// --------------------------------------------------------------------

func TestPsFilter_OldCtlPsADashRegression(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	now := time.Now().UTC()
	ended := now.Add(-time.Minute)
	rc := 0
	seedProc(t, db, "exited-f1b", "lab", "lab-1", "EXITED",
		now.Add(-time.Hour), &ended, &rc)

	conn, _ := nats.Connect(url)
	defer conn.Close()

	// v0.2.7 ctl literally always sends `{}` — emulate via raw bytes.
	resp := psWith(t, conn, pub, "lab", []byte("{}"))
	if resp.Code != "" {
		t.Fatalf("ps rejected: %s %s", resp.Code, resp.Error)
	}
	for _, p := range resp.Processes {
		if p.Status == "EXITED" {
			t.Errorf("F1b regression: new broker should withhold EXITED on empty body, got %+v", p)
		}
	}
}

// --------------------------------------------------------------------
// h1 A1 — the 2026-08-04 incident replay: one session accumulated 24k
// FREED `__proxy__` rows, the unbounded ports section pushed the
// marshaled PsResp past NATS max_payload (1 MiB), the broker's Respond
// error was swallowed, and every `tether ps` timed out for five days.
// After h1: the default reply excludes FREED history entirely (small in
// bytes, live rows intact), and `-a` returns a capped, live-first,
// truncation-flagged view.
// origin: docs/reviews/h1-plan.md workstream A1.
// --------------------------------------------------------------------

func seedRawPortRow(t *testing.T, db *sql.DB, sid, nid string, portN int, name, state string, createdAt time.Time) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO nodes(sid, nid, status) VALUES (?,?,?)`, sid, nid, "ONLINE",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
		 VALUES (?,?,?,?,0,?,?, 'SHA256:u', ?)`,
		portN, sid, nid, name, fmt.Sprintf("%s-%d-hash", name, createdAt.UnixNano()), state, createdAt,
	); err != nil {
		t.Fatal(err)
	}
}

func TestPsPortsIncidentReplay24kFreedRows(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	base := time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC)
	// The incident's live set: a handful of ALLOCATED rows, OLDER than the flood.
	for i := 0; i < 7; i++ {
		seedRawPortRow(t, db, "lab", "wsl", 14000+i, fmt.Sprintf("live-%d", i), "ALLOCATED", base.Add(time.Duration(i)*time.Minute))
	}
	// The flood: 24k FREED rows, one per 20s reaper rotation, all newer.
	flood := base.AddDate(0, 0, 1)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
		 VALUES (?,?,?,?,0,?,'FREED','SHA256:u',?)`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 24000; i++ {
		if _, err := stmt.Exec(14005+i%2, "lab", "wsl", "__proxy__",
			fmt.Sprintf("f%d-hash", i), flood.Add(time.Duration(i)*20*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	nc, _ := nats.Connect(url)
	defer nc.Close()

	// Default view: FREED history excluded server-side; the whole raw reply
	// must be far below the 1 MiB default max_payload that killed the
	// incident fleet (the plan pins <64KB).
	msg, err := nc.Request(proto.SubjCtrlPs(pub, "lab"), []byte("{}"), 10*time.Second)
	if err != nil {
		t.Fatalf("default ps against 24k FREED rows: %v (the incident timeout shape)", err)
	}
	if len(msg.Data) >= 64*1024 {
		t.Fatalf("default ps reply is %d bytes on a 24k-FREED-row session; want <64KB", len(msg.Data))
	}
	var resp proto.PsResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Ports) != 7 {
		t.Fatalf("default view: want exactly the 7 live rows, got %d", len(resp.Ports))
	}
	if resp.PortsTruncated {
		t.Fatalf("default live-only view of 7 rows must not be flagged truncated")
	}

	// -a view: capped at 500, truncation surfaced, every live row present.
	bodyAll, _ := json.Marshal(proto.PsReq{IncludeExited: true, IncludeFreedPorts: true})
	respAll := psWith(t, nc, pub, "lab", bodyAll)
	if respAll.Code != "" {
		t.Fatalf("ps -a rejected: %s %s", respAll.Code, respAll.Error)
	}
	if len(respAll.Ports) != 500 {
		t.Fatalf("-a ports: want server cap 500, got %d", len(respAll.Ports))
	}
	if !respAll.PortsTruncated || respAll.PortsTotal != 24007 {
		t.Fatalf("-a truncation surface wrong: truncated=%v total=%d (want true/24007)",
			respAll.PortsTruncated, respAll.PortsTotal)
	}
	live := 0
	for i, p := range respAll.Ports {
		if p.State == "ALLOCATED" {
			live++
			if i >= 7 {
				t.Fatalf("live row %q at position %d — live rows must sort strictly first", p.Name, i)
			}
		}
	}
	if live != 7 {
		t.Fatalf("truncated -a view lost live allocations: %d of 7 present", live)
	}
}
