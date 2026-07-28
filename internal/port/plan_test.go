package port

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

var testRehomeTime = time.Unix(1700000000, 0).UTC()

// TestD6PlanAllocateInertHome (R-6, build-and-prove): PlanAllocate with an EMPTY
// home bakes the pre-D6 9-column INSERT (no home_broker/epoch — byte-equivalent
// to before D6, the production path); a non-empty home bakes the 11-column form
// with home_broker=<lit>, epoch=0. Neither uses Statement.Args (all-literal).
func TestD6PlanAllocateInertHome(t *testing.T) {
	db := openDB(t)

	_, cmd, err := PlanAllocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:a", "", false, tinyBand())
	if err != nil {
		t.Fatalf("plan allocate (no home): %v", err)
	}
	sql := cmd.Body[0].SQL
	if strings.Contains(sql, "home_broker") || strings.Contains(sql, "epoch") {
		t.Errorf("empty-home INSERT must NOT name home_broker/epoch: %s", sql)
	}
	if len(cmd.Body[0].Args) != 0 {
		t.Errorf("empty-home INSERT must be all-literal (no Args): %v", cmd.Body[0].Args)
	}
	if !strings.Contains(sql, "(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)") {
		t.Errorf("empty-home column list drifted: %s", sql)
	}

	db2 := openDB(t)
	_, cmd2, err := PlanAllocate(db2, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:a", "node-2", false, tinyBand())
	if err != nil {
		t.Fatalf("plan allocate (home): %v", err)
	}
	sql2 := cmd2.Body[0].SQL
	if !strings.Contains(sql2, "home_broker, epoch)") {
		t.Errorf("homed INSERT must name home_broker, epoch: %s", sql2)
	}
	if !strings.Contains(sql2, "'node-2', 0)") {
		t.Errorf("homed INSERT must bake home_broker='node-2', epoch=0: %s", sql2)
	}
	if len(cmd2.Body[0].Args) != 0 {
		t.Errorf("homed INSERT must be all-literal (no Args): %v", cmd2.Body[0].Args)
	}
}

// TestD6ReassignHomeMonotonic (R-7): PlanReassignHome reads the current epoch and
// bakes home_broker=<lit>, epoch=<cur+1> with a monotonic CAS guard (epoch <
// new). Applying it advances the row; re-applying the SAME baked command is a
// deterministic no-op (CAS); absent / non-ALLOCATED rows return ErrNotFound; an
// empty home is rejected.
func TestD6ReassignHomeMonotonic(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	now := time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC)
	a, err := Allocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:a", false, tinyBand())
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}

	newEpoch, cmd, err := PlanReassignHome(db, a.Port, "node-2", testRehomeTime)
	if err != nil {
		t.Fatalf("reassign: %v", err)
	}
	if newEpoch != 1 {
		t.Fatalf("first reassign epoch = %d, want 1", newEpoch)
	}
	if !strings.Contains(cmd.Body[0].SQL, "epoch < 1") {
		t.Errorf("missing monotonic CAS guard: %s", cmd.Body[0].SQL)
	}
	// Apply it: row → {home_broker='node-2', epoch=1}.
	if _, err := db.Exec(cmd.Body[0].SQL); err != nil {
		t.Fatalf("apply reassign: %v", err)
	}
	gotHome, gotEpoch := readHomeEpoch(t, db, a.Port)
	if gotHome != "node-2" || gotEpoch != 1 {
		t.Fatalf("after reassign: home=%q epoch=%d, want node-2/1", gotHome, gotEpoch)
	}
	// Re-applying the SAME command is a CAS no-op (epoch is already 1, guard
	// epoch < 1 is false → RowsAffected 0).
	res, err := db.Exec(cmd.Body[0].SQL)
	if err != nil {
		t.Fatalf("re-apply reassign: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 0 {
		t.Fatalf("re-apply must be a CAS no-op, RowsAffected = %d", n)
	}

	// A second reassign reads the now-current epoch (1) and bakes 2.
	e2, _, err := PlanReassignHome(db, a.Port, "node-3", testRehomeTime)
	if err != nil || e2 != 2 {
		t.Fatalf("second reassign epoch = %d err = %v, want 2/nil", e2, err)
	}

	// Absent port → ErrNotFound.
	if _, _, err := PlanReassignHome(db, 19999, "node-2", testRehomeTime); !errors.Is(err, ErrNotFound) {
		t.Fatalf("absent port err = %v, want ErrNotFound", err)
	}
	// Empty home → rejected (not ErrNotFound).
	if _, _, err := PlanReassignHome(db, a.Port, "", testRehomeTime); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("empty home must be a hard reject, got %v", err)
	}
	// Non-ALLOCATED row → ErrNotFound (free it first).
	if err := Free(db, a.Port, now); err != nil {
		t.Fatalf("free: %v", err)
	}
	if _, _, err := PlanReassignHome(db, a.Port, "node-2", testRehomeTime); !errors.Is(err, ErrNotFound) {
		t.Fatalf("freed port err = %v, want ErrNotFound", err)
	}
}

// TestReassignHomeStampsLastRehomeAtomically (external review RESIDUAL-2): the retire/drain convergence
// gate's F3 recency scope (internal/broker.pendingRetireConvergence) rests ENTIRELY on last_rehome_at being
// stamped in the SAME UPDATE that bumps the epoch. A refactor that dropped the stamp would leave the row with
// a stale/absent last_rehome_at, and the retire gate — which scopes by op.CreatedAt <= last_rehome_at — would
// silently fail OPEN (drop this op's own still-stranded row → RemoveServer with the data plane stranded). No
// existing test caught deleting the stamp, so pin the atomic co-stamp at the plan layer.
func TestReassignHomeStampsLastRehomeAtomically(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	a, err := Allocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:a", false, tinyBand())
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	_, cmd, err := PlanReassignHome(db, a.Port, "node-2", testRehomeTime)
	if err != nil {
		t.Fatalf("reassign: %v", err)
	}
	if len(cmd.Body) != 1 {
		t.Fatalf("reassign must be a single atomic UPDATE, got %d statements", len(cmd.Body))
	}
	sql := cmd.Body[0].SQL
	if !strings.Contains(sql, "epoch=") {
		t.Fatalf("reassign UPDATE no longer bumps epoch: %s", sql)
	}
	if !strings.Contains(sql, "last_rehome_at=") {
		t.Fatalf("reassign UPDATE no longer stamps last_rehome_at IN THE SAME UPDATE as the epoch bump — the "+
			"retire/drain convergence gate's F3 recency scope would silently fail OPEN (RESIDUAL-2): %s", sql)
	}
	// And applying it persists a NON-EMPTY last_rehome_at (the F3 recency origin has something to compare).
	if _, err := db.Exec(sql); err != nil {
		t.Fatalf("apply: %v", err)
	}
	var lr string
	if err := db.QueryRow(`SELECT last_rehome_at FROM port_allocations WHERE port=?`, a.Port).Scan(&lr); err != nil {
		t.Fatalf("read last_rehome_at: %v", err)
	}
	if lr == "" {
		t.Fatal("last_rehome_at is empty after a reassign — the F3 recency origin has nothing to compare")
	}
}

func TestD6ReassignHomeSelectsActiveReusedPort(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	now := time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC)

	first, err := Allocate(db, "lab", "lab-1", "old", 8888, 0, "SHA256:a", false, tinyBand())
	if err != nil {
		t.Fatalf("allocate first: %v", err)
	}
	if err := Free(db, first.Port, now); err != nil {
		t.Fatalf("free first: %v", err)
	}
	second, err := Allocate(db, "lab", "lab-1", "new", 9999, 0, "SHA256:a", false, tinyBand())
	if err != nil {
		t.Fatalf("allocate second: %v", err)
	}
	if second.Port != first.Port {
		t.Fatalf("test setup expected port reuse, got first=%d second=%d", first.Port, second.Port)
	}

	newEpoch, cmd, err := PlanReassignHome(db, second.Port, "node-2", testRehomeTime)
	if err != nil {
		t.Fatalf("reassign reused active port: %v", err)
	}
	if newEpoch != 1 {
		t.Fatalf("epoch = %d, want 1", newEpoch)
	}
	if _, err := db.Exec(cmd.Body[0].SQL); err != nil {
		t.Fatalf("apply reassign: %v", err)
	}
	var oldHome, oldState, newHome, newState string
	if err := db.QueryRow(`SELECT home_broker, state FROM port_allocations WHERE name='old'`).Scan(&oldHome, &oldState); err != nil {
		t.Fatalf("read old row: %v", err)
	}
	if err := db.QueryRow(`SELECT home_broker, state FROM port_allocations WHERE name='new'`).Scan(&newHome, &newState); err != nil {
		t.Fatalf("read new row: %v", err)
	}
	if oldHome != "" || oldState != "FREED" {
		t.Fatalf("historical row mutated: home=%q state=%q", oldHome, oldState)
	}
	if newHome != "node-2" || newState != "ALLOCATED" {
		t.Fatalf("active row not rehomed: home=%q state=%q", newHome, newState)
	}
}

func TestD9PlanAllocationStateChangeFencesPortReuse(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	now := time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC)

	first, err := Allocate(db, "lab", "lab-1", "old", 8888, 0, "SHA256:a", false, tinyBand())
	if err != nil {
		t.Fatalf("allocate first: %v", err)
	}
	if err := FreeAllocation(db, *first, now); err != nil {
		t.Fatalf("free first: %v", err)
	}
	second, err := Allocate(db, "lab", "lab-1", "new", 9999, first.Port, "SHA256:a", false, tinyBand())
	if err != nil {
		t.Fatalf("reuse port: %v", err)
	}
	if _, err := PlanFreeAllocation(db, *first, now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale PlanFreeAllocation err = %v, want ErrNotFound", err)
	}
	if _, err := PlanRevokeAllocation(db, *first, now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale PlanRevokeAllocation err = %v, want ErrNotFound", err)
	}

	cmd, err := PlanFreeAllocation(db, *second, now)
	if err != nil {
		t.Fatalf("valid PlanFreeAllocation: %v", err)
	}
	if _, err := db.Exec(cmd.Body[0].SQL); err != nil {
		t.Fatalf("apply free allocation: %v", err)
	}
	var oldState, newState string
	if err := db.QueryRow(`SELECT state FROM port_allocations WHERE name='old'`).Scan(&oldState); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT state FROM port_allocations WHERE name='new'`).Scan(&newState); err != nil {
		t.Fatal(err)
	}
	if oldState != "FREED" || newState != "FREED" {
		t.Fatalf("states after fenced free: old=%s new=%s, want FREED/FREED", oldState, newState)
	}
}

func TestD9PlanRevokeAllocationUpdatesOnlySelectedRow(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	now := time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC)

	first, err := Allocate(db, "lab", "lab-1", "old", 8888, 0, "SHA256:a", false, tinyBand())
	if err != nil {
		t.Fatalf("allocate first: %v", err)
	}
	if err := FreeAllocation(db, *first, now); err != nil {
		t.Fatalf("free first: %v", err)
	}
	second, err := Allocate(db, "lab", "lab-1", "new", 9999, first.Port, "SHA256:a", false, tinyBand())
	if err != nil {
		t.Fatalf("reuse port: %v", err)
	}
	cmd, err := PlanRevokeAllocation(db, *second, now)
	if err != nil {
		t.Fatalf("valid PlanRevokeAllocation: %v", err)
	}
	if _, err := db.Exec(cmd.Body[0].SQL); err != nil {
		t.Fatalf("apply revoke allocation: %v", err)
	}
	var oldState, newState string
	if err := db.QueryRow(`SELECT state FROM port_allocations WHERE name='old'`).Scan(&oldState); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT state FROM port_allocations WHERE name='new'`).Scan(&newState); err != nil {
		t.Fatal(err)
	}
	if oldState != "FREED" || newState != "REVOKED" {
		t.Fatalf("states after fenced revoke: old=%s new=%s, want FREED/REVOKED", oldState, newState)
	}
}

func readHomeEpoch(t *testing.T, db *sql.DB, port int) (string, int64) {
	t.Helper()
	var home string
	var epoch int64
	if err := db.QueryRow(
		`SELECT home_broker, epoch FROM port_allocations WHERE port=?`, port,
	).Scan(&home, &epoch); err != nil {
		t.Fatalf("read home/epoch: %v", err)
	}
	return home, epoch
}
