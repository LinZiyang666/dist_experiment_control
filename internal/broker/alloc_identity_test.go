package broker

import (
	"database/sql"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/port"
)

// alloc_identity_test.go (batch B, commit 10) — the net for the one real bug this
// increment fixes.
//
// THE DEFECT, precisely
//
// Every port state change fences on five columns: internal/port/plan.go's
// planAllocationStateChange (raft path) and internal/port/port.go's updateAllocationState
// (direct path) both key on `port, sid, nid, name, token_hash`. But before allocIdentity,
// the broker fed those planners TWO DIFFERENT VALUES depending on which path a request
// took:
//
//	leader-local  →  the full 13-field port.Allocation, captured by the Propose closure
//	forwarded     →  a 5-field rebuild, because that is all PortFreeAllocationPayload carries
//
// Correct only by coincidence. The moment a sixth field is fenced on, the leader keeps working
// and the follower path silently plans against a zero value. Every leader-direct test stays
// green; the failure needs ctl to hit a follower.
//
// (An earlier draft of this paragraph said the project "has already fenced PlanAllocate on
// Epoch". Internal review showed that is false — PlanAllocate WRITES epoch=0 into its baked
// INSERT, it does not fence a read on it. The nearest real precedent is tunnelTokenLookup's
// epoch ladder in expose.go, which gates a REGISTER on exactly this column.)
//
// WHAT THESE TESTS DO
//
// TestAllocIdentityDropsEverythingBeyondTheFence pins the projection itself.
// TestAllocIdentitySurvivesTheWireRoundTrip pins the DATA half: what the originator marshals
// reconstructs, on the far side, into what the leader's own closure was handed.
// TestAllocationCallSitesPassTheNarrowedValue pins the CALL-SITE half — the half a revert of
// the fix would break, and the half nothing covered until internal review pointed it out.
// TestPortFenceColumnsAreStillFive is the tripwire for the future change that makes this
// matter — it fails when a planner starts reading a sixth field, which is exactly when a
// human needs to decide whether the payload must carry it.

// seedAllocFKParents returns a DB with the session + node rows port_allocations' foreign
// keys require, so a seed failure here is a real failure and not a fixture accident.
func seedAllocFKParents(t *testing.T) *sql.DB {
	t.Helper()
	db := openDBWithSession(t, "lab")
	if _, err := db.Exec(
		`INSERT INTO nodes(sid,nid,status) VALUES(?,?, 'ONLINE')`, "lab", "lab-1",
	); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	return db
}

func fullyPopulatedAllocation() port.Allocation {
	end := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	rc := end
	return port.Allocation{
		Port:        14000,
		SID:         "lab",
		NID:         "lab-1",
		Name:        "svc",
		LocalPort:   9001,
		TokenHash:   "deadbeef",
		State:       port.StateAllocated,
		CreatedByFP: "SHA256:someone",
		CreatedAt:   end,
		RevokedAt:   &rc,
		Token:       "raw-token-never-persisted",
		HomeBroker:  "br-2",
		Epoch:       7,
		// RebuildOff was missing from the first version of this fixture, which meant the
		// "everything beyond the fence is dropped" assertions were silent about it: a zero
		// field cannot be observed to have been dropped. Internal review caught it. Every
		// non-fenced field must be NON-ZERO here or the narrowing assertions are partly vacuous.
		RebuildOff: true,
	}
}

// TestFixtureIsActuallyFullyPopulated is the guard for the flaw above. A field added to
// port.Allocation and not added here would silently reduce what the narrowing tests can see,
// and nothing else in the file would notice.
func TestFixtureIsActuallyFullyPopulated(t *testing.T) {
	fenced := map[string]bool{"Port": true, "SID": true, "NID": true, "Name": true, "TokenHash": true}
	v := reflect.ValueOf(fullyPopulatedAllocation())
	rt := v.Type()
	if rt.NumField() < 14 {
		t.Fatalf("port.Allocation has %d fields; the fixture and allocIdentity's godoc both assume "+
			"14 — one was removed and the counts have drifted", rt.NumField())
	}
	for i := 0; i < rt.NumField(); i++ {
		if v.Field(i).IsZero() {
			t.Errorf("fixture leaves %s at its zero value. The narrowing assertions check that "+
				"non-fenced fields are ZERO after projection; a field that was already zero going "+
				"in proves nothing, so this one is silently uncovered.", rt.Field(i).Name)
		}
	}
	// Sanity: the fenced set must actually be a subset of the struct, else a rename would make
	// the check above pass while testing the wrong columns.
	for name := range fenced {
		if _, ok := rt.FieldByName(name); !ok {
			t.Errorf("port.Allocation has no field %q — the fenced-column set is stale", name)
		}
	}
}

func TestAllocIdentityDropsEverythingBeyondTheFence(t *testing.T) {
	a := fullyPopulatedAllocation()
	got := allocIdentity(a)
	want := PortFreeAllocationPayload{
		Port: 14000, SID: "lab", NID: "lab-1", Name: "svc", TokenHash: "deadbeef",
	}
	if got != want {
		t.Fatalf("allocIdentity: got %+v want %+v", got, want)
	}

	// The round trip must be a fixed point: narrowing an already-narrow allocation is
	// the identity. If it were not, the leader and the wire would still disagree — just
	// one projection further down.
	if again := allocIdentity(got.allocation()); again != got {
		t.Errorf("allocIdentity is not idempotent: %+v -> %+v", got, again)
	}
}

// TestAllocIdentitySurvivesTheWireRoundTrip checks the half of the property that is about
// DATA: the payload the originator marshals reconstructs, on the receiving side, into exactly
// the allocation the leader's own closure was handed.
//
// An earlier version of this test compared `allocIdentity(a).allocation()` against
// `allocIdentity(a).allocation()` — literally the same expression twice — while its comments
// claimed one side was the leader's value and the other the forwarded one. It was a tautology,
// and internal review proved it by reverting the entire fix and watching the suite stay green.
// The lesson is recorded rather than quietly deleted: a differential whose two operands are not
// produced by two different mechanisms is not a differential.
//
// (The old test's name is deliberately not written here. test/determinism/promised_guard_test.go
// treats any TestXxx token in a comment as a promise that such a test exists, and it is right
// to: a historical note that names a deleted test sends the next reader looking for coverage
// that is gone.)
//
// This version puts the payload through the real encode/decode. The CALL SITES — the other half
// of the property, and the half a revert would break — are guarded structurally by
// TestAllocationCallSitesPassTheNarrowedValue below.
func TestAllocIdentitySurvivesTheWireRoundTrip(t *testing.T) {
	a := fullyPopulatedAllocation()

	// What freePortAllocation / revokePortAllocation hand to the leader-local Plan closure.
	leaderSide := allocIdentity(a).allocation()

	// What actually travels: the same projection, marshalled exactly as clusterwrite.go does...
	payload, err := json.Marshal(allocIdentity(a))
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	// ...and decoded exactly as dispatchForward's VerbPortFreeAllocation arm does.
	var decoded PortFreeAllocationPayload
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	forwardSide := decoded.allocation()

	if !reflect.DeepEqual(leaderSide, forwardSide) {
		t.Fatalf("leader path plans with %+v but the forwarded path reconstructs %+v — the wire "+
			"round trip is lossy for a field the planner fences on", leaderSide, forwardSide)
	}

	// And it must genuinely be NARROWER than the input, else the comparison above says nothing:
	// two identical copies of the full struct would also be DeepEqual.
	if reflect.DeepEqual(leaderSide, a) {
		t.Fatal("the projected allocation still equals the full input — allocIdentity is not " +
			"narrowing anything, so this differential proves nothing")
	}
	for _, f := range []struct {
		name string
		got  any
	}{
		{"LocalPort", leaderSide.LocalPort},
		{"State", leaderSide.State},
		{"CreatedByFP", leaderSide.CreatedByFP},
		{"Token", leaderSide.Token},
		{"HomeBroker", leaderSide.HomeBroker},
		{"Epoch", leaderSide.Epoch},
	} {
		if !reflect.ValueOf(f.got).IsZero() {
			t.Errorf("projected allocation still carries %s=%v — either the fence grew (see "+
				"TestPortFenceColumnsAreStillFive) or the projection is leaking a field the wire "+
				"cannot carry", f.name, f.got)
		}
	}
}

// TestAllocationCallSitesPassTheNarrowedValue is the guard that a revert of the fix would
// actually break.
//
// The defect was NOT in allocIdentity — it was in the CALL SITES: freePortAllocation and
// revokePortAllocation marshalled a 5-field payload onto the wire while handing their
// leader-local Plan closure the full 13-field `a`. No value-level test can see that, because
// the two planners agree on the five fields today; the divergence only becomes a defect when a
// sixth field is fenced on, and only on the forwarded path. Internal review demonstrated the
// gap by reverting the fix and watching the whole suite stay green.
//
// So this asserts the shape from the AST: inside those two functions, every port mutator and
// planner must receive the NARROWED identifier, never the function's own `a` parameter.
// It checks the BINDING, not the name. The first version compared the argument identifier
// against the literal "narrowed", which internal review pointed out is a proxy for the property:
// rewriting the two lines as
//
//	narrowed := a
//	payload, err := json.Marshal(allocIdentity(a))
//
// restores the exact pre-fix divergence — five fields on the wire, the full struct on the leader
// and single-mode paths — while every call site still passes something spelled `narrowed`, and
// every other test in this file stays green. So the identifier must be traced back to an
// assignment derived from allocIdentity.
func TestAllocationCallSitesPassTheNarrowedValue(t *testing.T) {
	// callee -> 0-based index of the allocation argument.
	allocationArg := map[string]int{
		"FreeAllocation":       1,
		"RevokeAllocation":     1,
		"PlanFreeAllocation":   1,
		"PlanRevokeAllocation": 1,
	}
	guarded := map[string]bool{"freePortAllocation": true, "revokePortAllocation": true}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "clusterwrite.go", nil, 0)
	if err != nil {
		t.Fatalf("parse clusterwrite.go: %v", err)
	}

	seen := map[string]int{}
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || !guarded[fd.Name.Name] {
			continue
		}
		// Collect `x := <expr>` bindings so an argument identifier can be traced to what
		// produced it. `derived` holds the names bound to an allocIdentity(...) result.
		derived := map[string]bool{}
		ast.Inspect(fd, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for i, lhs := range as.Lhs {
				id, ok := lhs.(*ast.Ident)
				if !ok || i >= len(as.Rhs) {
					continue
				}
				if exprDerivesFromAllocIdentity(as.Rhs[i], derived) {
					derived[id.Name] = true
				}
			}
			return true
		})

		ast.Inspect(fd, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			idx, guardedCallee := allocationArg[sel.Sel.Name]
			if !guardedCallee || idx >= len(call.Args) {
				return true
			}
			seen[fd.Name.Name]++
			arg := call.Args[idx]
			if id, ok := arg.(*ast.Ident); ok && derived[id.Name] {
				return true
			}
			if exprDerivesFromAllocIdentity(arg, derived) {
				return true
			}
			t.Errorf("%s passes an allocation to %s that is NOT derived from allocIdentity.\n"+
				"That is the original defect: the wire carries allocIdentity's five fields while "+
				"this path plans against the full 14-field struct. It is correct only while no "+
				"planner reads a sixth field, and the day one does, the leader keeps working and "+
				"only a FOLLOWER-routed request plans against a zero value — invisible to every "+
				"leader-direct test.\n"+
				"Note this checks the BINDING, not the identifier's name: `narrowed := a` does not "+
				"satisfy it.", fd.Name.Name, sel.Sel.Name)
			return true
		})
	}

	// Non-vacuity: both functions must exist and each must actually contain guarded calls.
	// A renamed function or a moved mutator would otherwise make this pass by finding nothing.
	for name := range guarded {
		if seen[name] == 0 {
			t.Errorf("found no port mutator/planner call inside %s — it was renamed, moved to "+
				"another file, or stopped calling the port package, and this guard silently stopped "+
				"covering it", name)
		}
	}
	if total := seen["freePortAllocation"] + seen["revokePortAllocation"]; total < 4 {
		t.Errorf("only %d guarded call(s) found across both functions; each has a single-mode "+
			"direct mutator AND a leader-local planner, so 4 is the floor", total)
	}
}

// exprDerivesFromAllocIdentity reports whether an expression's value came from allocIdentity —
// directly (`allocIdentity(a)`), through its projection (`allocIdentity(a).allocation()`,
// `id.allocation()` where id is derived), or from an identifier already known to be derived.
func exprDerivesFromAllocIdentity(e ast.Expr, derived map[string]bool) bool {
	switch x := e.(type) {
	case *ast.Ident:
		return derived[x.Name]
	case *ast.CallExpr:
		switch fn := x.Fun.(type) {
		case *ast.Ident:
			return fn.Name == "allocIdentity"
		case *ast.SelectorExpr:
			// <expr>.allocation() — derived iff the receiver is.
			if fn.Sel.Name == "allocation" {
				return exprDerivesFromAllocIdentity(fn.X, derived)
			}
		}
	}
	return false
}

// TestPortFenceColumnsAreStillFive is the tripwire. allocIdentity is safe ONLY while the
// planners fence on exactly the five columns it keeps. This asserts that against real
// behaviour, not against a comment: it plans a state change with the full allocation and
// again with the narrowed one, and requires the emitted command to be byte-identical.
// A planner that started reading a sixth field would produce different SQL for the two.
func TestPortFenceColumnsAreStillFive(t *testing.T) {
	db := seedAllocFKParents(t)
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)

	full := fullyPopulatedAllocation()
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
		 VALUES (?,?,?,?,?,?, 'ALLOCATED', ?, ?)`,
		full.Port, full.SID, full.NID, full.Name, full.LocalPort, full.TokenHash, full.CreatedByFP, now,
	); err != nil {
		t.Fatalf("seed allocation: %v", err)
	}

	cmdFull, err := port.PlanFreeAllocation(db, full, now)
	if err != nil {
		t.Fatalf("plan with full allocation: %v", err)
	}
	cmdNarrow, err := port.PlanFreeAllocation(db, allocIdentity(full).allocation(), now)
	if err != nil {
		t.Fatalf("plan with narrowed allocation: %v", err)
	}
	if !reflect.DeepEqual(cmdFull, cmdNarrow) {
		t.Fatalf("PlanFreeAllocation now produces DIFFERENT commands for the full and the narrowed "+
			"allocation, so it reads a column beyond the five allocIdentity keeps.\n"+
			"  full:    %+v\n  narrow:  %+v\n"+
			"This is the moment to decide: either PortFreeAllocationPayload must carry the new "+
			"field (an ADDITIVE wire change with a stated cross-version story), or the planner must "+
			"not read it. Do NOT simply widen allocIdentity — that reopens the leader/follower "+
			"divergence it was written to close.", cmdFull, cmdNarrow)
	}

	// Same question for the revoke planner, which shares planAllocationStateChange but is a
	// separate exported entry point and could grow its own fence.
	revFull, err := port.PlanRevokeAllocation(db, full, now)
	if err != nil {
		t.Fatalf("plan revoke with full allocation: %v", err)
	}
	revNarrow, err := port.PlanRevokeAllocation(db, allocIdentity(full).allocation(), now)
	if err != nil {
		t.Fatalf("plan revoke with narrowed allocation: %v", err)
	}
	if !reflect.DeepEqual(revFull, revNarrow) {
		t.Fatalf("PlanRevokeAllocation diverges between the full and narrowed allocation:\n"+
			"  full:    %+v\n  narrow:  %+v", revFull, revNarrow)
	}
}

// TestDirectMutatorsAcceptTheNarrowedAllocation covers the entry point the Plan* differential
// above does NOT: the SINGLE-mode direct mutators.
//
// freePortAllocation and revokePortAllocation now hand the narrowed allocation to
// port.FreeAllocation / port.RevokeAllocation as well, which the plan did not ask for — the
// plan's §5 decision #4 only required the two cluster paths to agree. Extending it to single
// mode makes the property uniform, but it also means a fence added to updateAllocationState
// (internal/port/port.go) now sees ZEROES for every column beyond the five.
//
// Internal review demonstrated the consequence end to end: add `AND local_port=?` to that
// WHERE — the natural way anyone would tighten it — and every `tether expose rm` on a
// single-broker fleet returns free_failed while every offline-node revoke silently stops
// reclaiming ports. racknerd IS a single broker, so that is the whole fleet, and the Plan*
// tripwire above does not touch this path at all.
func TestDirectMutatorsAcceptTheNarrowedAllocation(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	full := fullyPopulatedAllocation()

	seed := func(t *testing.T) *sql.DB {
		t.Helper()
		db := seedAllocFKParents(t)
		if _, err := db.Exec(
			`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
			 VALUES (?,?,?,?,?,?, 'ALLOCATED', ?, ?)`,
			full.Port, full.SID, full.NID, full.Name, full.LocalPort, full.TokenHash, full.CreatedByFP, now,
		); err != nil {
			t.Fatalf("seed allocation: %v", err)
		}
		return db
	}
	stateOf := func(t *testing.T, db *sql.DB) string {
		t.Helper()
		var s string
		if err := db.QueryRow(
			`SELECT state FROM port_allocations WHERE port=?`, full.Port,
		).Scan(&s); err != nil {
			t.Fatalf("read state: %v", err)
		}
		return s
	}

	for _, c := range []struct {
		name string
		call func(db *sql.DB, a port.Allocation) error
		want string
	}{
		{"FreeAllocation", func(db *sql.DB, a port.Allocation) error {
			return port.FreeAllocation(db, a, now)
		}, "FREED"},
		{"RevokeAllocation", func(db *sql.DB, a port.Allocation) error {
			return port.RevokeAllocation(db, a, now)
		}, "REVOKED"},
	} {
		t.Run(c.name, func(t *testing.T) {
			dbFull, dbNarrow := seed(t), seed(t)
			if err := c.call(dbFull, full); err != nil {
				t.Fatalf("%s with the FULL allocation: %v", c.name, err)
			}
			if err := c.call(dbNarrow, allocIdentity(full).allocation()); err != nil {
				t.Fatalf("%s with the NARROWED allocation: %v.\n"+
					"The single-mode direct path passes the narrowed value since batch B, so this "+
					"failure means updateAllocationState now fences on a column allocIdentity drops "+
					"— i.e. `tether expose rm` is broken on every single-broker deployment, which "+
					"is the whole production fleet.", c.name, err)
			}
			if gotF, gotN := stateOf(t, dbFull), stateOf(t, dbNarrow); gotF != gotN || gotN != c.want {
				t.Errorf("%s landed the row in %q with the full allocation and %q with the narrowed "+
					"one (want %q for both)", c.name, gotF, gotN, c.want)
			}
		})
	}

	// Non-vacuity: the direct mutators must still REFUSE a wrong identity, else "the narrowed
	// value works" would be true of any value at all.
	wrong := allocIdentity(full).allocation()
	wrong.Name = "not-the-same-name"
	if err := port.FreeAllocation(seed(t), wrong, now); err == nil {
		t.Fatal("FreeAllocation accepted an allocation whose Name does not match the row — the " +
			"identity fence is not being applied, so both assertions above are vacuous")
	}
}

// TestPortFenceTripwireIsNotVacuous proves the differential above can actually fail. If
// PlanFreeAllocation ignored its allocation argument entirely (or the comparison were
// against the same value twice), the tripwire would be green forever.
func TestPortFenceTripwireIsNotVacuous(t *testing.T) {
	db := seedAllocFKParents(t)
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	full := fullyPopulatedAllocation()
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
		 VALUES (?,?,?,?,?,?, 'ALLOCATED', ?, ?)`,
		full.Port, full.SID, full.NID, full.Name, full.LocalPort, full.TokenHash, full.CreatedByFP, now,
	); err != nil {
		t.Fatalf("seed allocation: %v", err)
	}

	base, err := port.PlanFreeAllocation(db, full, now)
	if err != nil {
		t.Fatalf("baseline plan: %v", err)
	}
	// Perturb one of the FIVE fenced columns. The planner must notice — it fences on it —
	// which proves the DeepEqual comparison in the tripwire is capable of going red.
	perturbed := full
	perturbed.Name = "svc-other"
	if _, err := port.PlanFreeAllocation(db, perturbed, now); err == nil {
		t.Fatal("planning against a non-existent (port,sid,nid,name,token_hash) tuple SUCCEEDED — " +
			"the fence is not being applied, so every differential in this file is vacuous")
	}

	// And a field OUTSIDE the fence must not change the command — that is the property
	// allocIdentity relies on. Stated as a positive assertion so it is visible, not implied.
	outside := full
	outside.Epoch = 999
	outside.HomeBroker = "br-99"
	outside.Token = "different"
	alt, err := port.PlanFreeAllocation(db, outside, now)
	if err != nil {
		t.Fatalf("plan with perturbed out-of-fence fields: %v", err)
	}
	if !reflect.DeepEqual(base, alt) {
		t.Fatalf("changing Epoch/HomeBroker/Token changed the planned command — the fence is WIDER "+
			"than five columns and allocIdentity is dropping load-bearing data:\n  base: %+v\n  alt:  %+v",
			base, alt)
	}
}
