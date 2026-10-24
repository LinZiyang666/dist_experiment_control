package xfergc

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

func checkpointFixture(t *testing.T) (string, x.WriterState, RetirementCheckpointJournal) {
	t.Helper()
	root := t.TempDir()
	id := x.Identity{SID: "session", NID: "node", TransferID: "transfer", EntryNonce: "entry", ObjectVersion: "version", ActorFP: "actor", AgentInstance: "agent", IntentDigest: "intent"}
	state := x.WriterState{ObjectSpec: x.ObjectSpec{Identity: id, Stream: "OBJ_xfer-session", Bucket: "xfer-session", StreamCreated: "original-incarnation"}, Phase: x.WriteRetired, AttachmentGeneration: 1,
		RetirementFence: x.RetirementFence{WriterRetired: true, ConsumersRetired: true, Purged: true, Receipt: &x.HistoryReceipt{Stream: "OBJ_xfer-session", Sequence: 3, RecordDigest: "fence"}}}
	body, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, checkpointName(id)), body, 0600); err != nil {
		t.Fatal(err)
	}
	journal, err := NewRetirementCheckpointJournal(root, &sync.Mutex{})
	if err != nil {
		t.Fatal(err)
	}
	return root, state, journal
}

// The real scoped filesystem adapter owns neither a persistent descriptor nor
// a goroutine. Confirmed collection unlinks the exact content revision only.
// origin: hot-update S7 plan §5.3 / s7-p9m remaining request 4
func TestTransferRetirementCheckpointJournalNoLeaks(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRetirementCheckpoint(t)
	}
	testharness.AssertNoGoroutineLeak(t, "retirement checkpoint journal", g)
	testharness.AssertNoFDLeak(t, "retirement checkpoint journal", fd, 0)
}
func exerciseRetirementCheckpoint(t *testing.T) {
	t.Helper()
	root, state, journal := checkpointFixture(t)
	id := state.ObjectSpec.Identity
	err := CollectRetirementCheckpoints(journal, func(got x.Identity, _ x.Deadline) (RetirementResponsibility, error) {
		return RetirementResponsibility{Identity: got, Authoritative: true, PrimaryAbsent: true, OutboxAbsent: true, CourierAbsent: true, PermitExpired: true, ResultExpired: true}, nil
	}, gcDeadline(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(root, checkpointName(id))); !os.IsNotExist(err) {
		t.Fatalf("confirmed checkpoint did not retire: %v", err)
	}
}

// Origin uncertainty, incomplete fences and replacement writes all preserve
// the disk record, even when a caller claims other responsibilities absent.
// origin: hot-update S7 plan §5.3 / s7-p9m remaining request 4
func TestTransferRetirementCheckpointRetainsUncertainOrChangedRecords(t *testing.T) {
	for _, fault := range []string{"unpurged", "writer", "consumer", "no_fence", "changed", "future_field", "oversized", "wrong_name"} {
		t.Run(fault, func(t *testing.T) {
			root, state, journal := checkpointFixture(t)
			d := gcDeadline(t, 5*time.Second)
			if _, err := journal.ScanRetirementCheckpoints(65, d); err == nil {
				t.Fatal("journal accepted an oversized enumeration bound")
			}
			before, err := journal.ScanRetirementCheckpoints(64, d)
			if err != nil || len(before) != 1 {
				t.Fatal(err)
			}
			switch fault {
			case "unpurged":
				state.RetirementFence.Purged = false
			case "writer":
				state.RetirementFence.WriterRetired = false
			case "consumer":
				state.RetirementFence.ConsumersRetired = false
			case "no_fence":
				state.RetirementFence.Receipt = nil
			case "changed":
				state.ObjectSpec.StreamCreated = "replacement-incarnation"
			}
			body, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if fault == "future_field" {
				body = append(body[:len(body)-1], []byte(`,"future_responsibility":true}`)...)
			}
			if fault == "oversized" {
				state.ObjectSpec.StreamCreated = strings.Repeat("a", 65537)
				body, err = json.Marshal(state)
				if err != nil {
					t.Fatal(err)
				}
			}
			name := checkpointName(state.ObjectSpec.Identity)
			if fault == "wrong_name" {
				if err = os.Rename(filepath.Join(root, name), filepath.Join(root, "foreign.json")); err != nil {
					t.Fatal(err)
				}
				name = "foreign.json"
			}
			if err = os.WriteFile(filepath.Join(root, name), body, 0600); err != nil {
				t.Fatal(err)
			}
			if fault == "changed" {
				err = journal.ConsumeRetirementCheckpoint(before[0], d)
			} else {
				_, err = journal.ScanRetirementCheckpoints(64, d)
			}
			if err == nil {
				t.Fatal("uncertain or changed retirement checkpoint was accepted")
			}
			if _, err = os.Stat(filepath.Join(root, name)); err != nil {
				t.Fatalf("uncertain checkpoint was deleted: %v", err)
			}
		})
	}
}

// Borrowing the producer lock cannot exceed the collector's pass budget.
// The release is unconditional so even a mutant never leaves a blocked child.
// origin: hot-update S7 plan §5.3 / s7-p9m remaining request 4
func exerciseRetirementCheckpointProducerLockDeadline(t *testing.T) {
	mutex := &sync.Mutex{}
	journal, err := NewRetirementCheckpointJournal(t.TempDir(), mutex)
	if err != nil {
		t.Fatal(err)
	}
	mutex.Lock()
	release := time.AfterFunc(100*time.Millisecond, mutex.Unlock)
	defer release.Stop()
	_, err = journal.ScanRetirementCheckpoints(64, gcDeadline(t, 10*time.Millisecond))
	if err == nil {
		t.Fatal("producer lock replenished the journal pass deadline")
	}
	// Join the scheduled release before returning, including the normal early
	// timeout. This is a timer callback, not an unowned background fixture.
	mutex.Lock()
	mutex.Unlock() //nolint:staticcheck // Acquiring the producer lock joins the scheduled release before fixture teardown.
}

// The content-CAS test proves the real unlink. This on-disk guard pins the
// directory Sync that turns it into a durable deletion receipt; the test
// checks the actual return path, not a descriptive table or unused helper.
// origin: hot-update S7 plan §5.3 / s7-p9m remaining request 4
func TestTransferRetirementCheckpointDeletionSyncsDirectory(t *testing.T) {
	_, here, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("source location unavailable")
	}
	source, err := parser.ParseFile(token.NewFileSet(), filepath.Join(filepath.Dir(here), "orphan.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	consume, syncSeen := false, false
	for _, decl := range source.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Name.Name == "ConsumeRetirementCheckpoint" {
			last, ok := fn.Body.List[len(fn.Body.List)-1].(*ast.ReturnStmt)
			if !ok || len(last.Results) != 1 {
				t.Fatal("durable deletion must return its Sync result")
			}
			call, ok := last.Results[0].(*ast.CallExpr)
			if !ok {
				t.Fatal("durable deletion skipped its Sync")
			}
			name, ok := call.Fun.(*ast.Ident)
			consume = ok && name.Name == "syncCheckpointDirectory"
		}
		if fn.Name.Name == "syncCheckpointDirectory" {
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				syncSeen = syncSeen || ok && selector.Sel.Name == "Sync"
				return true
			})
		}
	}
	if !consume || !syncSeen {
		t.Fatal("retirement checkpoint deletion has no actual directory Sync")
	}
}

// origin: hot-update S7 plan §5.3 / s7-p9m remaining request 4
func TestTransferRetirementCheckpointProducerLockKeepsDeadline(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRetirementCheckpointProducerLockDeadline(t)
	}
	testharness.AssertNoGoroutineLeak(t, "retirement checkpoint producer lock", g)
	testharness.AssertNoFDLeak(t, "retirement checkpoint producer lock", fd, 0)
}
