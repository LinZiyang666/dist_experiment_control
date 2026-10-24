package proc

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// A SID and even the complete process key can be reused. Neither a successful
// handler precheck nor a previously rendered leader plan authorizes that row.
// File-backed fixtures exercise rollback/commit and descriptor cleanup directly.
// origin: hot-update S8 plan §6.2 / §11.2 / P34
func TestProcessMutationRejectsRecreatedSession(t *testing.T) {
	goroutines, descriptors := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseProcessMutation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "guarded process mutations", goroutines)
	testharness.AssertNoFDLeak(t, "guarded process mutations", descriptors, 0)
}

func exerciseProcessMutation(t *testing.T) {
	t.Helper()
	// Migrate one real file per round, then reset the row set between cases.
	// Repeating schema creation for every attack obscures the mutation cost
	// and adds machine-wide fsync load without exercising another boundary.
	db, err := storage.Open(filepath.Join(t.TempDir(), "processes.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
		if db.Stats().OpenConnections != 0 {
			t.Error("process mutation round retained a database connection")
		}
	}()
	for _, operation := range []string{"exit", "refile", "batch"} {
		for _, replacement := range []string{"salt", "owner", "created-at"} {
			t.Run(operation+"/saved-plan/"+replacement, func(t *testing.T) {
				db, creation, guard := processAuthorityFixture(t, db)
				cmd := processMutationPlan(t, db, guard, operation)
				// Exercise the real existing replicated payload rather than retaining
				// an in-memory proof outside the statement sent to Apply.
				encoded, err := json.Marshal(cmd)
				if err != nil {
					t.Fatal("encode guarded process command")
				}
				var retained cluster.Command
				if err := json.Unmarshal(encoded, &retained); err != nil {
					t.Fatal("decode guarded process command")
				}
				if err := cluster.ExecCommand(db, &retained); err != nil {
					t.Fatalf("live creation refused command: %v", err)
				}
				assertProcessMutation(t, db, operation)
				fresh := replaceProcessCreation(creation, replacement)
				recreateProcessRows(t, db, fresh)
				before := processRows(t, db)
				if err := cluster.ExecCommand(db, &retained); err != nil {
					t.Fatalf("guarded stale command apply: %v", err)
				}
				if !reflect.DeepEqual(before, processRows(t, db)) {
					t.Fatal("saved process plan modified a recreated session")
				}
				assertProcessRefusal(t, db, guard, operation)
			})
		}
		t.Run(operation+"/live-and-idempotent", func(t *testing.T) {
			db, _, guard := processAuthorityFixture(t, db)
			if operation == "batch" {
				if err := cluster.ExecCommand(db, processMutationPlan(t, db, guard, operation)); err != nil {
					t.Fatal(err)
				}
			} else {
				moved, actual, err := directProcessMutation(db, guard, operation)
				if err != nil || actual != guard.Incarnation || (operation == "refile" && !moved) {
					t.Fatalf("live guarded mutation: moved=%v actual-match=%v err=%v", moved, actual == guard.Incarnation, err)
				}
			}
			assertProcessMutation(t, db, operation)
			before := processRows(t, db)
			if operation == "batch" {
				if err := cluster.ExecCommand(db, processMutationPlan(t, db, guard, operation)); err != nil {
					t.Fatal(err)
				}
			} else {
				moved, actual, err := directProcessMutation(db, guard, operation)
				if err != nil || actual != guard.Incarnation || moved {
					t.Fatalf("idempotent mutation: moved=%v actual-match=%v err=%v", moved, actual == guard.Incarnation, err)
				}
			}
			if !reflect.DeepEqual(before, processRows(t, db)) {
				t.Fatal("idempotent mutation rewrote process fields")
			}
		})
		for _, invalid := range []string{"empty", "malformed", "different", "missing-session", "deleting-recreated"} {
			t.Run(operation+"/refusal/"+invalid, func(t *testing.T) {
				db, creation, guard := processAuthorityFixture(t, db)
				switch invalid {
				case "empty":
					guard.Incarnation = ""
				case "malformed":
					guard.Incarnation = "invalid"
				case "different":
					guard.Incarnation = strings.Repeat("f", 64)
				case "missing-session":
					processSQL(t, db, `DELETE FROM sessions WHERE sid=?`, guard.SID)
				case "deleting-recreated":
					recreateProcessRows(t, db, replaceProcessCreation(creation, "salt"))
					processSQL(t, db, `UPDATE sessions SET state='DELETING' WHERE sid=?`, guard.SID)
				}
				before := processRows(t, db)
				assertProcessRefusal(t, db, guard, operation)
				if !reflect.DeepEqual(before, processRows(t, db)) {
					t.Fatal("refused process mutation changed persistent rows")
				}
			})
		}
	}
	for _, operation := range []string{"exit", "refile"} {
		t.Run(operation+"/write-boundary", func(t *testing.T) {
			db, creation, guard := processAuthorityFixture(t, db)
			// Force recreation after the comparison and before SQL execution.
			// The injected planner runs on the actual writer transaction; a
			// handler-entry comparison and an unguarded late UPDATE both lose.
			moved, _, err := mutateProcess(db, &guard, func(reader upgradeauthority.RowReader) (*cluster.Command, error) {
				tx, ok := reader.(*sql.Tx)
				if !ok {
					t.Fatal("process authority comparison is outside the writer transaction")
				}
				recreateProcessRows(t, tx, replaceProcessCreation(creation, "salt"))
				if operation == "exit" {
					return planMarkExited(tx, processAuthorityPID, guard.SID, 137, processAuthorityTime().Add(time.Hour))
				}
				return planRefile(guard.SID, "lab-1", "lab-2", []string{processAuthorityPID})
			})
			if err != nil || moved {
				t.Fatalf("late process mutation: moved=%v err=%v", moved, err)
			}
			for _, row := range processRows(t, db) {
				if row.Status != StateRunning || row.NID != "lab-1" || row.ExitCode != nil || row.EndedAt != nil {
					t.Fatal("late mutation changed the new creation's process row")
				}
			}
		})
	}
	t.Run("batch/cross-session", func(t *testing.T) {
		db, _, guard := processAuthorityFixture(t, db)
		in := ReconcileBatchInput{SID: "foreign", Marks: []ExitMark{{PID: processAuthorityPID, ExitCode: 137, When: processAuthorityTime()}}}
		_, cmd, err := PlanReconcileBatchGuarded(db, guard, in)
		if !errors.Is(err, ErrUnscopedExit) || cmd != nil {
			t.Fatal("guarded reconcile accepted a different session")
		}
	})
	t.Run("refile/empty-plan-still-compares", func(t *testing.T) {
		db, creation, guard := processAuthorityFixture(t, db)
		recreateProcessRows(t, db, replaceProcessCreation(creation, "salt"))
		actual, statements, err := PlanRefileStatementsGuarded(db, guard, "", "", nil)
		assertProcessIncarnationError(t, err)
		if actual != "" || statements != nil {
			t.Fatal("stale empty plan returned authority evidence")
		}
	})
	t.Run("batch/empty-plan-still-compares", func(t *testing.T) {
		db, creation, guard := processAuthorityFixture(t, db)
		recreateProcessRows(t, db, replaceProcessCreation(creation, "salt"))
		actual, cmd, err := PlanReconcileBatchGuarded(db, guard, ReconcileBatchInput{SID: guard.SID})
		assertProcessIncarnationError(t, err)
		if actual != "" || cmd != nil {
			t.Fatal("stale empty batch returned authority evidence")
		}
	})
}

const processAuthorityPID = "process'a?--"

func processAuthorityTime() time.Time { return time.Date(2026, 10, 12, 12, 0, 0, 123, time.UTC) }

func processAuthorityFixture(t *testing.T, db *sql.DB) (*sql.DB, upgradeauthority.Creation, upgradeauthority.Guard) {
	t.Helper()
	processSQL(t, db, `DELETE FROM sessions`)
	salt := bytes.Repeat([]byte{17}, 16)
	creation := upgradeauthority.Creation{
		SID: "lab", Owner: "owner'a?--", CreatedAtText: processAuthorityTime().String(), EncodedPINHash: processAuthorityPHC(salt),
	}
	seedProcessRows(t, db, creation)
	incarnation, err := upgradeauthority.Derive(creation.SID, creation.Owner, processAuthorityTime().UnixNano(), salt)
	if err != nil {
		t.Fatal(err)
	}
	return db, creation, upgradeauthority.Guard{SID: creation.SID, Incarnation: incarnation, InstanceID: "aaaaaaaaaaaaaaaaaaaaaaaaaa", ConfiguredNID: "lab-1"}
}

func processAuthorityPHC(salt []byte) string {
	return "$argon2id$v=19$m=65536,t=3,p=2$" + base64.RawStdEncoding.EncodeToString(salt) + "$" + base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{23}, 32))
}

func replaceProcessCreation(creation upgradeauthority.Creation, field string) upgradeauthority.Creation {
	switch field {
	case "salt":
		creation.EncodedPINHash = processAuthorityPHC(bytes.Repeat([]byte{18}, 16))
	case "owner":
		creation.Owner += "replacement"
	case "created-at":
		creation.CreatedAtText = processAuthorityTime().Add(time.Nanosecond).String()
	}
	return creation
}

type processSQLWriter interface {
	Exec(string, ...any) (sql.Result, error)
}

func processSQL(t *testing.T, writer processSQLWriter, query string, args ...any) {
	t.Helper()
	if _, err := writer.Exec(query, args...); err != nil {
		// SQL or driver errors can contain local creation comparison material.
		t.Fatal("process fixture SQL failed")
	}
}

func seedProcessRows(t *testing.T, writer processSQLWriter, creation upgradeauthority.Creation) {
	t.Helper()
	processSQL(t, writer, `INSERT INTO sessions(sid,name,owner_pubkey_fp,created_at,pin_hash) VALUES (?,?,?,?,?)`,
		creation.SID, "process authority", creation.Owner, creation.CreatedAtText, creation.EncodedPINHash)
	for _, nid := range []string{"lab-1", "lab-2"} {
		processSQL(t, writer, `INSERT INTO nodes(sid,nid,status) VALUES (?,?,'ONLINE')`, creation.SID, nid)
	}
	for _, pid := range []string{processAuthorityPID, "process-b"} {
		processSQL(t, writer, `INSERT INTO processes(pid,sid,nid,argv,cwd,started_at,status,started_by_fp,boot_id,start_time_ticks) VALUES (?,?,'lab-1','["keep"]','/tmp',?,'RUNNING','actor','boot',1234)`,
			pid, creation.SID, processAuthorityTime())
	}
}

func recreateProcessRows(t *testing.T, writer processSQLWriter, creation upgradeauthority.Creation) {
	t.Helper()
	processSQL(t, writer, `DELETE FROM sessions WHERE sid=?`, creation.SID)
	seedProcessRows(t, writer, creation)
}

func processRows(t *testing.T, db *sql.DB) []Process {
	t.Helper()
	rows, err := ListBySession(db, "lab")
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func directProcessMutation(db *sql.DB, guard upgradeauthority.Guard, operation string) (bool, string, error) {
	if operation == "exit" {
		actual, err := MarkExitedGuarded(db, guard, processAuthorityPID, 137, processAuthorityTime().Add(time.Hour))
		return false, actual, err
	}
	return RefileGuarded(db, guard, processAuthorityPID, "lab-1", "lab-2")
}

func processMutationPlan(t *testing.T, db *sql.DB, guard upgradeauthority.Guard, operation string) *cluster.Command {
	t.Helper()
	actual, cmd, err := plannedProcessMutation(db, guard, operation)
	if err != nil || cmd == nil || actual != guard.Incarnation {
		t.Fatalf("guarded process plan: present=%v actual-match=%v err=%v", cmd != nil, actual == guard.Incarnation, err)
	}
	for _, statement := range cmd.Body {
		if len(statement.Args) != 0 {
			t.Fatal("guarded plan must use literals in the existing op")
		}
	}
	return cmd
}

func plannedProcessMutation(db *sql.DB, guard upgradeauthority.Guard, operation string) (string, *cluster.Command, error) {
	switch operation {
	case "exit":
		return PlanMarkExitedGuarded(db, guard, processAuthorityPID, 137, processAuthorityTime().Add(time.Hour))
	case "refile":
		actual, statements, err := PlanRefileStatementsGuarded(db, guard, "lab-1", "lab-2", []string{"process-b", processAuthorityPID, "process-b", ""})
		if err != nil {
			return actual, nil, err
		}
		return actual, cluster.NewCommand(cluster.OpNodeRegister, statements...), nil
	default:
		return PlanReconcileBatchGuarded(db, guard, ReconcileBatchInput{SID: guard.SID, Marks: []ExitMark{
			{PID: "process-b", ExitCode: 137, When: processAuthorityTime().Add(time.Hour)},
			{PID: processAuthorityPID, ExitCode: 137, When: processAuthorityTime().Add(time.Hour)},
		}})
	}
}

func assertProcessRefusal(t *testing.T, db *sql.DB, guard upgradeauthority.Guard, operation string) {
	t.Helper()
	if operation != "batch" {
		moved, actual, err := directProcessMutation(db, guard, operation)
		assertProcessIncarnationError(t, err)
		if moved || actual != "" {
			t.Fatal("refused process mutation returned ownership evidence")
		}
	}
	actual, cmd, err := plannedProcessMutation(db, guard, operation)
	assertProcessIncarnationError(t, err)
	if actual != "" || cmd != nil {
		t.Fatal("refused process plan returned ownership evidence")
	}
}

func assertProcessIncarnationError(t *testing.T, err error) {
	t.Helper()
	var refusal *upgradetx.Error
	if !errors.As(err, &refusal) || refusal.Code != upgradetx.CodeSessionIncarnationMismatch {
		t.Fatalf("stale process mutation returned %v; want session_incarnation_mismatch", err)
	}
}

func assertProcessMutation(t *testing.T, db *sql.DB, operation string) {
	t.Helper()
	row, err := GetInSession(db, "lab", processAuthorityPID)
	if err != nil {
		t.Fatal(err)
	}
	if operation == "refile" {
		if row.NID != "lab-2" || row.Status != StateRunning || row.EndedAt != nil || row.ExitCode != nil {
			t.Fatal("guarded refile did not move only the live process ownership")
		}
	} else if row.Status != StateExited || row.NID != "lab-1" || row.ExitCode == nil || *row.ExitCode != 137 || row.EndedAt == nil || !row.EndedAt.Equal(processAuthorityTime().Add(time.Hour)) {
		t.Fatal("guarded exit did not persist its terminal fields")
	}
}
