package port

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestD6PlanAllocateInertHome (R-6, build-and-prove): PlanAllocate with an EMPTY
// home bakes the pre-D6 9-column INSERT (no home_broker/epoch — byte-equivalent
// to before D6, the production path); a non-empty home bakes the 11-column form
// with home_broker=<lit>, epoch=0. Neither uses Statement.Args (all-literal).
func TestD6PlanAllocateInertHome(t *testing.T) {
	db := openDB(t)

	_, cmd, err := PlanAllocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:a", "", tinyBand())
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
	_, cmd2, err := PlanAllocate(db2, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:a", "node-2", tinyBand())
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
	a, err := Allocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:a", tinyBand())
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}

	newEpoch, cmd, err := PlanReassignHome(db, a.Port, "node-2")
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
	e2, _, err := PlanReassignHome(db, a.Port, "node-3")
	if err != nil || e2 != 2 {
		t.Fatalf("second reassign epoch = %d err = %v, want 2/nil", e2, err)
	}

	// Absent port → ErrNotFound.
	if _, _, err := PlanReassignHome(db, 19999, "node-2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("absent port err = %v, want ErrNotFound", err)
	}
	// Empty home → rejected (not ErrNotFound).
	if _, _, err := PlanReassignHome(db, a.Port, ""); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("empty home must be a hard reject, got %v", err)
	}
	// Non-ALLOCATED row → ErrNotFound (free it first).
	if err := Free(db, a.Port, now); err != nil {
		t.Fatalf("free: %v", err)
	}
	if _, _, err := PlanReassignHome(db, a.Port, "node-2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("freed port err = %v, want ErrNotFound", err)
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
