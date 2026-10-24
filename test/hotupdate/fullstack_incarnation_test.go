//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/fullstackharness"
	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
	"golang.org/x/sys/unix"
)

type fullstackAuthorityMember struct {
	Identity c.Process
	PGID     int
	Stopped  bool
}

type fullstackLateCleanup struct {
	Entry, Incarnation, Code string
	AtNS                     int64
	Accepted                 bool
	Effects                  int
	Before, After            c.Digest
}

type fullstackIncarnationProof struct {
	Round                                         int
	RunID, SID, Old, Current                      string
	PreparedNS, DeletedNS, RecreatedNS, RetiredNS int64
	FixtureCleanupNS                              int64
	DeleteEventDelivered                          bool
	Victims                                       []fullstackAuthorityMember
	EmptyCensusNS                                 []int64
	Remaining                                     []fullstackAuthorityMember
	Late                                          []fullstackLateCleanup
	SurvivorBefore, SurvivorAfter                 c.Process
	SurvivorSent, SurvivorReceived                []byte
	SurvivorEffects                               int
	TransferID, TransferIncarnation               string
	TransferBefore, TransferAfter                 c.Digest
	Rollback                                      fullstackRollbackProof
}

// origin: hot-update S10 plan §6 / §10.2 P24 / S8 incarnation
func TestFullstackIncarnationRejectsLateCleanup(t *testing.T) {
	t.Run("oracle", fullstackIncarnationControls)
	t.Run("boundary", func(t *testing.T) {
		if err := fullstackRecreationBoundary("../../internal/agent/session_destroyed_pty_test.go"); err != nil {
			t.Fatal(err)
		}
	})
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseIncarnationCut(t, round) })
	}
	testharness.AssertNoGoroutineLeak(t, "fullstack incarnation runtime and cleanup x5", beforeG)
	testharness.AssertNoFDLeak(t, "fullstack incarnation runtime and cleanup x5", beforeFD, 0)
}

func exerciseIncarnationCut(t *testing.T, round int) {
	t.Helper()
	started := time.Now()
	// These invoke the real embedded broker, SQLite and NATS handlers. Their
	// cleanup runs at the child boundary, before the parent's leak assertions.
	for _, entry := range []string{"g1", "lease", "reconnect", "late-exit", "farewell"} {
		t.Run("handler-"+entry, func(t *testing.T) { exerciseLateAuthorityCleanup(t, entry) })
	}
	t.Run("atomic-writer", func(t *testing.T) { exerciseAuthorityPredicate(t) })
	t.Logf("cost: incarnation startup+I/O+settle+join=%s", time.Since(started))
	t.Run("current-rollback", func(t *testing.T) {
		name := fmt.Sprintf("s10/p24/round-%d/incarnation", round)
		var supplied fullstackIncarnationProof
		if err := readFullstackEvidence(name, &supplied); err != nil {
			t.Fatalf("SETUP-RED: NOT_RUN pending S10 P24 live incarnation/current-rollback proof supplier: %v", err)
		}
		if supplied.Round != round || supplied.RunID != name {
			t.Fatal("incarnation receipt reused another exercise")
		}
		if err := fullstackJudgeIncarnation(supplied); err != nil {
			t.Fatal(err)
		}
		for _, victim := range supplied.Victims {
			actual, err := fullstackharness.ReadKernelProcess(victim.Identity.Key, victim.Identity.PID, victim.Identity.InstanceID)
			if err == nil && actual == victim.Identity {
				t.Fatal("old incarnation member is still present, including zombie custody")
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, unix.ESRCH) {
				t.Fatalf("victim retirement is unknown: %v", err)
			}
		}
		survivor := supplied.SurvivorAfter
		actual, err := fullstackharness.ReadKernelProcess(survivor.Key, survivor.PID, survivor.InstanceID)
		if err != nil || actual != survivor {
			t.Fatalf("new incarnation survivor was replaced or lost: %v", err)
		}
	})
}

// The old test once asserted that a recreated SID authorized the old PTY.
// Parse function bodies so comments or merely retaining its name cannot sign
// the reversal. This source check supplements the runtime victim/survivor test.
// gate-control: TestFullstackIncarnationRejectsLateCleanup
// origin: hot-update S10 plan §6 / IDENT-11
func fullstackRecreationBoundary(path string) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, body, 0)
	if err != nil {
		return err
	}
	functions := make(map[string]*ast.FuncDecl)
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok {
			functions[fn.Name.Name] = fn
		}
	}
	root, exercise := functions["TestPTYSessionRecreationBoundary"], functions["exercisePTYRecreatedAuthority"]
	if root == nil || exercise == nil || !fullstackCalls(root.Body, "exercisePTYRecreatedAuthority") {
		return fmt.Errorf("S8 recreation boundary no longer exercises the imported old PTY")
	}
	revoked, mismatch, dead := false, false, false
	ast.Inspect(exercise.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "waitPTYRevoked" {
				revoked = true
			}
		}
		if branch, ok := node.(*ast.IfStmt); ok && (fullstackCalls(branch.Body, "Fatal") || fullstackCalls(branch.Body, "Fatalf")) {
			condition := string(body[fset.Position(branch.Cond.Pos()).Offset:fset.Position(branch.Cond.End()).Offset])
			mismatch = mismatch || strings.Contains(condition, `!strings.Contains(err.Error(), "session_incarnation_mismatch")`)
			dead = dead || strings.Contains(condition, "ptyMemberAlive(task.row.OSPID, task.row.StartTimeTicks) || ptyMemberAlive(task.memberPID, task.memberTicks)")
		}
		return true
	})
	if !revoked || !mismatch || !dead {
		return fmt.Errorf("S8 recreation boundary must reject registration, revoke the old PTY and assert both members dead")
	}
	return nil
}

func fullstackCalls(node ast.Node, name string) bool {
	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch function := call.Fun.(type) {
		case *ast.Ident:
			found = found || function.Name == name
		case *ast.SelectorExpr:
			found = found || function.Sel.Name == name
		}
		return true
	})
	return found
}

// All times here belong to the same observer, not clocks from different hosts.
// Product retirement must precede fixture cleanup; killing a leftover victim
// during teardown is evidence of failure, never evidence of revocation.
func fullstackJudgeIncarnation(e fullstackIncarnationProof) error {
	if e.SID == "" || !transactionHex(e.Old, 32) || !transactionHex(e.Current, 32) || e.Old == e.Current ||
		e.PreparedNS <= 0 || e.DeletedNS <= e.PreparedNS || e.RecreatedNS <= e.DeletedNS ||
		e.DeleteEventDelivered || len(e.Victims) < 2 || len(e.Remaining) != 0 || len(e.EmptyCensusNS) != 2 ||
		e.EmptyCensusNS[0] <= e.RecreatedNS || e.EmptyCensusNS[1] <= e.EmptyCensusNS[0] ||
		e.RetiredNS <= e.EmptyCensusNS[1] || e.FixtureCleanupNS <= e.RetiredNS {
		return fmt.Errorf("incarnation cut lacks lost-event, post-Prepare deletion or product-owned retirement")
	}
	groups := make(map[int]bool)
	members := make(map[c.Process]bool)
	stopped := false
	for _, member := range e.Victims {
		if member.Identity.PID <= 0 || member.Identity.StartTicks == 0 || member.PGID <= 0 || members[member.Identity] {
			return fmt.Errorf("victim census has missing or repeated kernel identity")
		}
		groups[member.PGID], members[member.Identity], stopped = true, true, stopped || member.Stopped
	}
	if len(groups) < 2 || !stopped {
		return fmt.Errorf("revocation did not exercise multiple groups and a stopped member")
	}
	want := []string{"register", "g1", "farewell", "transfer-settlement", "lease", "late-exit"}
	if len(e.Late) != len(want) {
		return fmt.Errorf("late authority callback census is incomplete")
	}
	seen := make(map[string]bool)
	for _, late := range e.Late {
		if !slices.Contains(want, late.Entry) || seen[late.Entry] || late.Incarnation != e.Old ||
			late.AtNS <= e.RecreatedNS || late.Accepted || late.Effects != 0 || late.Before == (c.Digest{}) || late.Before != late.After ||
			(late.Entry == "register" || late.Entry == "g1") && late.Code != "session_incarnation_mismatch" {
			return fmt.Errorf("late cleanup crossed the incarnation boundary or changed replacement state")
		}
		seen[late.Entry] = true
	}
	if e.SurvivorBefore.PID <= 0 || e.SurvivorBefore.StartTicks == 0 || e.SurvivorBefore != e.SurvivorAfter ||
		members[e.SurvivorBefore] || len(e.SurvivorSent) == 0 || !bytes.Equal(e.SurvivorSent, e.SurvivorReceived) ||
		e.SurvivorEffects != 1 || e.TransferID == "" || e.TransferIncarnation != e.Current ||
		e.TransferBefore == (c.Digest{}) || e.TransferBefore != e.TransferAfter {
		return fmt.Errorf("replacement survivor or transfer settlement was lost/replayed")
	}
	return fullstackJudgeRollback(e.Rollback)
}

func fullstackIncarnationControl() fullstackIncarnationProof {
	e := fullstackIncarnationProof{SID: "lab", Old: strings.Repeat("a", 64), Current: strings.Repeat("b", 64),
		PreparedNS: 10, DeletedNS: 20, RecreatedNS: 30, EmptyCensusNS: []int64{50, 60}, RetiredNS: 70, FixtureCleanupNS: 80,
		SurvivorEffects: 1, SurvivorSent: []byte("still alive"), SurvivorReceived: []byte("still alive"),
		TransferID: "original-transfer", TransferIncarnation: strings.Repeat("b", 64), TransferBefore: c.Digest{1}, TransferAfter: c.Digest{1}, Rollback: fullstackRollbackControl()}
	e.Victims = []fullstackAuthorityMember{{Identity: c.Process{PID: 100, StartTicks: 10}, PGID: 100}, {Identity: c.Process{PID: 101, StartTicks: 11}, PGID: 101, Stopped: true}}
	e.SurvivorBefore, e.SurvivorAfter = c.Process{PID: 102, StartTicks: 12}, c.Process{PID: 102, StartTicks: 12}
	for _, entry := range []string{"register", "g1", "farewell", "transfer-settlement", "lease", "late-exit"} {
		e.Late = append(e.Late, fullstackLateCleanup{Entry: entry, Incarnation: e.Old, AtNS: 40, Code: "session_incarnation_mismatch", Before: c.Digest{1}, After: c.Digest{1}})
	}
	return e
}

// origin: hot-update S10 plan §6 / P24
func fullstackIncarnationControls(t *testing.T) {
	if err := fullstackJudgeIncarnation(fullstackIncarnationControl()); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*fullstackIncarnationProof){
		"same-incarnation":       func(e *fullstackIncarnationProof) { e.Current = e.Old },
		"event-delivered":        func(e *fullstackIncarnationProof) { e.DeleteEventDelivered = true },
		"early-delete":           func(e *fullstackIncarnationProof) { e.DeletedNS = e.PreparedNS },
		"single-census":          func(e *fullstackIncarnationProof) { e.EmptyCensusNS = e.EmptyCensusNS[:1] },
		"fixture-nuke":           func(e *fullstackIncarnationProof) { e.FixtureCleanupNS = e.RetiredNS },
		"zombie-left":            func(e *fullstackIncarnationProof) { e.Remaining = e.Victims[:1] },
		"single-group":           func(e *fullstackIncarnationProof) { e.Victims[1].PGID = e.Victims[0].PGID },
		"no-stopped-member":      func(e *fullstackIncarnationProof) { e.Victims[1].Stopped = false },
		"late-register-accepted": func(e *fullstackIncarnationProof) { e.Late[0].Accepted = true },
		"g1-changed-survivor":    func(e *fullstackIncarnationProof) { e.Late[1].After = c.Digest{2} },
		"late-farewell-effect":   func(e *fullstackIncarnationProof) { e.Late[2].Effects++ },
		"late-transfer-effect":   func(e *fullstackIncarnationProof) { e.Late[3].After = c.Digest{2} },
		"survivor-replaced":      func(e *fullstackIncarnationProof) { e.SurvivorAfter.PID++ },
		"transfer-retired":       func(e *fullstackIncarnationProof) { e.TransferAfter = c.Digest{2} },
		"rollback-resurrected": func(e *fullstackIncarnationProof) {
			e.Rollback.Reverse.After.Observation.Current.Outcome = upgradetx.OutcomeUnknown
		},
	} {
		t.Run(name, func(t *testing.T) {
			e := fullstackIncarnationControl()
			mutate(&e)
			if err := fullstackJudgeIncarnation(e); err == nil {
				t.Fatalf("%s: invalid incarnation evidence accepted", name)
			}
		})
	}
	t.Run("boundary-source-controls", func(t *testing.T) {
		path := "../../internal/agent/session_destroyed_pty_test.go"
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := fullstackRecreationBoundary(path); err != nil {
			t.Fatal(err)
		}
		for name, replace := range map[string][2]string{
			"no-revoke":           {"waitPTYRevoked(t, b, task)", "ignoreOldPTY(t, b, task)"},
			"accept-old-register": {`!strings.Contains(err.Error(), "session_incarnation_mismatch")`, `strings.Contains(err.Error(), "session_incarnation_mismatch")`},
			"old-members-alive":   {"ptyMemberAlive(task.row.OSPID, task.row.StartTimeTicks) || ptyMemberAlive(task.memberPID, task.memberTicks)", "!ptyMemberAlive(task.row.OSPID, task.row.StartTimeTicks) || !ptyMemberAlive(task.memberPID, task.memberTicks)"},
		} {
			t.Run(name, func(t *testing.T) {
				mutant := strings.ReplaceAll(string(body), replace[0], replace[1])
				if mutant == string(body) {
					t.Fatal("source mutation did not hit")
				}
				copy := filepath.Join(t.TempDir(), "boundary.go")
				if err := os.WriteFile(copy, []byte(mutant), 0600); err != nil {
					t.Fatal(err)
				}
				if err := fullstackRecreationBoundary(copy); err == nil {
					t.Fatal("inverted source assertion accepted")
				}
			})
		}
	})
}
