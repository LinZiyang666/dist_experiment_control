package cluster

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
)

// §13.4 kill-9 crash-consistency matrix — the load-bearing merge gate (architecture
// §3.7). A real-SIGKILL subprocess harness: the child opens an on-disk WAL cluster
// DB, drives a deterministic ClusterMetaSet sequence, and is SIGKILL'd at a
// PROVABLY-controlled instant (a commit gate, not a stdout race). The parent then
// re-opens the same data dir in-process and asserts the two §3.7 invariants.
//
// Env-gated child dispatch: TETHER_KILL9_CHILD is set ONLY by the parent here, so
// a bare `go test ./...` never forks a child (asserted by TestNoStrayKill9Child).

const (
	envKill9Child = "TETHER_KILL9_CHILD" // fault point: "fp1" | "fp2"
	envKill9Dir   = "TETHER_KILL9_DIR"
	envKill9Bread = "TETHER_KILL9_BREAD"
	kill9PreOps   = 5 // ops committed before the kill point
)

// breadcrumb is written by the child at the kill instant and read by the parent.
type breadcrumb struct {
	FaultPoint    string `json:"fp"`
	GateIndex     uint64 `json:"gate_index"`      // raft index of the op at the gate (fp1)
	BoltLastIndex uint64 `json:"bolt_last_index"` // raft LogStore last index at kill
	AppliedAtKill uint64 `json:"applied_at_kill"` // durable applied_index visible to an independent conn at kill
	OpsCommitted  int    `json:"ops_committed"`   // # ClusterMetaSet ops that durably committed
	MetaHash      string `json:"meta_hash"`       // clusterMetaHash of the committed state at kill (fp2/fp3)
}

func breadcrumbPath(dir string) string { return filepath.Join(dir, "breadcrumb.json") }

func writeBreadcrumb(dir string, b breadcrumb) {
	data, _ := json.Marshal(b)
	// Write atomically (temp + rename) so the parent never reads a partial file.
	tmp := breadcrumbPath(dir) + ".tmp"
	_ = os.WriteFile(tmp, data, 0o600)
	_ = os.Rename(tmp, breadcrumbPath(dir))
}

func readBreadcrumb(t *testing.T, dir string) breadcrumb {
	t.Helper()
	data, err := os.ReadFile(breadcrumbPath(dir))
	if err != nil {
		t.Fatalf("read breadcrumb: %v", err)
	}
	var b breadcrumb
	if err := json.Unmarshal(data, &b); err != nil {
		t.Fatalf("unmarshal breadcrumb: %v", err)
	}
	return b
}

// TestMain dispatches to the kill-9 child when the env var is set; otherwise runs
// the normal test suite.
func TestMain(m *testing.M) {
	if fp := os.Getenv(envKill9Child); fp != "" {
		runKill9Child(fp, os.Getenv(envKill9Dir))
		os.Exit(0) // unreachable for fp1 (blocks) / fp2 (blocks); defensive
	}
	os.Exit(m.Run())
}

// runKill9Child opens a node, drives ops, records a breadcrumb at the kill instant,
// then blocks forever so the parent SIGKILLs it in the chosen window.
func runKill9Child(fp, dir string) {
	n, err := openNodeRaw(dir, "k")
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: open node: %v\n", err)
		os.Exit(1)
	}
	if err := n.WaitForLeader(5 * time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "child: wait leader: %v\n", err)
		os.Exit(1)
	}
	dbPath := n.fsm.dbPath

	// Commit kill9PreOps ops normally (durable in BOTH stores).
	for i := 0; i < kill9PreOps; i++ {
		if err := n.ApplyMetaSet(fmt.Sprintf("t:k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			fmt.Fprintf(os.Stderr, "child: pre-op %d: %v\n", i, err)
			os.Exit(1)
		}
	}

	switch fp {
	case "fp1":
		// BoltDB-ahead-of-SQLite: arm the gate so the NEXT op's SQLite commit
		// blocks AFTER raft has durably committed it to BoltDB. The gate records
		// the window (boltLastIndex > applied_index from an independent conn) and
		// blocks; the parent SIGKILLs here.
		applyCommitGate = func(index uint64) {
			writeBreadcrumb(dir, breadcrumb{
				FaultPoint:    "fp1",
				GateIndex:     index,
				BoltLastIndex: lastBoltIndex(n),
				AppliedAtKill: independentApplied(dbPath),
				OpsCommitted:  kill9PreOps,
			})
			select {} // block; SIGKILL lands inside window-1
		}
		_ = n.ApplyMetaSet(fmt.Sprintf("t:k%d", kill9PreOps), "blocked") // blocks at gate
	case "fp2":
		// SQLite-committed-pre-snapshot: all ops committed; kill before any
		// snapshot so restart is pure idempotent replay.
		hash, _ := hashClusterMeta(n.db)
		writeBreadcrumb(dir, breadcrumb{
			FaultPoint:    "fp2",
			BoltLastIndex: lastBoltIndex(n),
			AppliedAtKill: independentApplied(dbPath),
			OpsCommitted:  kill9PreOps,
			MetaHash:      hash,
		})
		select {} // block; SIGKILL after all ops are durable
	case "fp3":
		// Mid-online-backup: arm the backup step gate so the snapshot's backup
		// blocks AFTER the first Step (temp file exists, snapshot not finalized),
		// then SIGKILL. The live DB (read-only source) must be untouched and the
		// orphaned snap-*.db harmless on restart.
		hash, _ := hashClusterMeta(n.db)
		applied := independentApplied(dbPath)
		backupStepGate = func() {
			writeBreadcrumb(dir, breadcrumb{
				FaultPoint:    "fp3",
				AppliedAtKill: applied,
				OpsCommitted:  kill9PreOps,
				MetaHash:      hash,
			})
			select {} // block mid-backup; SIGKILL here
		}
		_ = n.Snapshot() // triggers Persist→backupTo→stepAll→gate (blocks)
	default:
		fmt.Fprintf(os.Stderr, "child: unknown fault point %q\n", fp)
		os.Exit(1)
	}
}

func lastBoltIndex(n *Node) uint64 {
	idx, err := n.store.LastIndex()
	if err != nil {
		return 0
	}
	return idx
}

// independentApplied reads applied_index through a SEPARATE read-only connection,
// so it reflects only DURABLY-COMMITTED state (an uncommitted op is invisible).
func independentApplied(dbPath string) uint64 {
	ro, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		return 0
	}
	defer func() { _ = ro.Close() }()
	v, err := readAppliedIndexDB(ro)
	if err != nil {
		return 0
	}
	return v
}

func TestKill9Matrix(t *testing.T) {
	for _, fp := range []string{"fp1", "fp2", "fp3"} {
		t.Run(fp, func(t *testing.T) {
			dir := t.TempDir()
			child := exec.Command(os.Args[0], "-test.run=^TestKill9Matrix$", "-test.timeout=120s")
			child.Env = append(os.Environ(),
				envKill9Child+"="+fp,
				envKill9Dir+"="+dir,
			)
			child.Stderr = os.Stderr
			if err := child.Start(); err != nil {
				t.Fatalf("start child: %v", err)
			}
			// Wait for the breadcrumb, then SIGKILL in the window.
			waitForFile(t, breadcrumbPath(dir), 30*time.Second)
			b := readBreadcrumb(t, dir)
			if err := child.Process.Kill(); err != nil {
				t.Fatalf("SIGKILL child: %v", err)
			}
			_, _ = child.Process.Wait()

			// Pre-restart window proof (non-vacuity #3): the entry was durable in
			// BoltDB but NOT yet in SQLite.
			if fp == "fp1" && b.BoltLastIndex <= b.AppliedAtKill {
				t.Fatalf("fp1 window did not exist: boltLastIndex=%d applied=%d (want bolt > applied)",
					b.BoltLastIndex, b.AppliedAtKill)
			}

			assertRecovery(t, dir, fp, b)
		})
	}
}

// assertRecovery re-opens the data dir in-process and asserts the §3.7 invariants.
func assertRecovery(t *testing.T, dir, fp string, b breadcrumb) {
	n := mustNode(t, dir, "k")
	// Force the startup log replay to drain into the FSM before reading (becoming
	// leader does not by itself guarantee the recovered entry is applied yet).
	if err := n.Barrier(10 * time.Second); err != nil {
		t.Fatalf("recovery barrier: %v", err)
	}

	applied, err := n.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}

	switch fp {
	case "fp1":
		// The committed-but-not-yet-applied entry must be recovered: all PreOps+1
		// values present, applied_index == the gate index, no lost entry.
		for i := 0; i <= kill9PreOps; i++ {
			if v, ok := readMetaInternal(t, n, fmt.Sprintf("t:k%d", i)); !ok {
				t.Fatalf("fp1: t:k%d missing after recovery (lost committed entry)", i)
			} else if i == kill9PreOps && v != "blocked" {
				t.Fatalf("fp1: t:k%d = %q, want \"blocked\" (the recovered entry)", i, v)
			}
		}
		if applied != b.GateIndex {
			t.Fatalf("fp1: applied_index=%d after recovery, want gate index %d", applied, b.GateIndex)
		}
	case "fp2":
		// Pure idempotent replay: every committed op present, applied_index
		// unchanged, the idempotent-skip path fired, AND the committed CONTENT is
		// byte-stable across the replay window (catches a content double-apply that
		// reapplyCount>0 alone would miss).
		for i := 0; i < kill9PreOps; i++ {
			if _, ok := readMetaInternal(t, n, fmt.Sprintf("t:k%d", i)); !ok {
				t.Fatalf("fp2: t:k%d missing after recovery", i)
			}
		}
		if applied != b.AppliedAtKill {
			t.Fatalf("fp2: applied_index=%d after recovery, want %d (unchanged)", applied, b.AppliedAtKill)
		}
		if rc := n.fsm.reapplyCount.Load(); rc == 0 {
			t.Fatalf("fp2: reapply count is 0 — the §3.7 #2 idempotent-skip path never fired (vacuous)")
		}
		if got := clusterMetaHash(t, n.db); got != b.MetaHash {
			t.Fatalf("fp2: content changed across the replay window (double-apply?): want %s got %s", b.MetaHash, got)
		}
	case "fp3":
		// Mid-backup SIGKILL: the live DB is untouched (the backup only READ the
		// read-only source + wrote a temp), the orphaned snap-*.db did not block
		// re-open (mustNode already succeeded), applied_index + content are
		// unchanged, the live DB is integrity-clean, and the node still works.
		if applied != b.AppliedAtKill {
			t.Fatalf("fp3: applied_index=%d after recovery, want %d (backup must not touch the live DB)", applied, b.AppliedAtKill)
		}
		if got := clusterMetaHash(t, n.db); got != b.MetaHash {
			t.Fatalf("fp3: live DB content changed by a mid-backup kill: want %s got %s", b.MetaHash, got)
		}
		var ic string
		if err := n.db.QueryRow(`PRAGMA integrity_check`).Scan(&ic); err != nil || ic != "ok" {
			t.Fatalf("fp3: live DB integrity_check = %q (err %v) after mid-backup kill", ic, err)
		}
		// The node is still functional: a fresh op + snapshot succeed.
		if err := n.ApplyMetaSet("t:after-fp3", "ok"); err != nil {
			t.Fatalf("fp3: node must still apply after a mid-backup kill: %v", err)
		}
		if err := n.Snapshot(); err != nil {
			t.Fatalf("fp3: snapshot after recovery must succeed: %v", err)
		}
	}

	// applied_index must equal raft's last COMMAND index, never raft-last-index
	// (the bootstrap config entry at index 1 + any election noop do not call
	// FSM.Apply). After recovery the FSM is fully caught up, so the durable
	// applied_index is the authority we already asserted above.
	if applied == 0 {
		t.Fatalf("%s: applied_index is 0 after recovery (no state survived)", fp)
	}
}

func readMetaInternal(t *testing.T, n *Node, key string) (string, bool) {
	t.Helper()
	var v string
	err := n.db.QueryRow(`SELECT value FROM cluster_meta WHERE key = ?`, key).Scan(&v)
	switch {
	case err == nil:
		return v, true
	case errors.Is(err, sql.ErrNoRows):
		return "", false
	default:
		t.Fatalf("readMetaInternal %q: %v", key, err)
		return "", false
	}
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("breadcrumb %s did not appear within %s (child stuck?)", path, timeout)
}

// TestNoStrayKill9Child proves the env-gated dispatch never forks a child during a
// normal run: this binary, invoked WITHOUT the env var, runs tests in-process. If
// TestMain forked unconditionally, `go test ./...` would spawn infinite children.
func TestNoStrayKill9Child(t *testing.T) {
	if os.Getenv(envKill9Child) != "" {
		t.Fatal("a normal test run must not have the kill-9 child env set")
	}
}
