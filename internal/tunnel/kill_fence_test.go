package tunnel

import (
	"context"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// kill_fence_test.go — every kill verb must invalidate an in-flight REGISTER (B6).
//
// WHY THIS FILE REPLACED THREE OTHERS
// -----------------------------------
// This is the refactor roadmap's flagship example, and it is worth stating plainly because the cost was
// paid in review rounds rather than in incidents.
//
// A REGISTER can race a kill: the handler passes the authoritative token lookup, then a kill verb runs,
// then the handler installs its serverSession — and a public listener appears AFTER the kill switch
// already returned. The fix is a fence generation the handler re-checks before installing.
//
// The invariant was discovered ONCE and then re-discovered THREE TIMES, once per external review round,
// because each round tested one verb in a file named after that round:
//
//	round 2  CloseProxy      missing the fence   → p13_external_review_round2_test.go
//	round 5  CloseSession    the same hole       → p13_external_review_round5_test.go
//	round 6  ForgetSession   the same hole again → p13_external_review_round6_test.go
//
// Three files, three near-identical bodies, one invariant. Had round 2 been written as the table below,
// rounds 5 and 6 would STRUCTURALLY not have happened: adding a verb to killVerbs is one line, and
// forgetting to add one is visible in a way that "there is no test file for this verb" is not.
//
// origin: p13 external review rounds 2 / 5 / 6 (F1, F1, F4) — merged here, provenance kept per row.
//
// THE FOURTH VERB
// ---------------
// CloseProxyIf is included and was NOT covered by any of the three rounds. Its only existing tests are
// white-box (they read s.killGenAllocation directly, in what is now register_and_fence_test.go), so the
// black-box in-flight-REGISTER property below is NEW coverage rather than a re-expression of old
// coverage. That asymmetry is stated because it changes what a mutation proves: deleting the allocation
// fence bump reddens the white-box tests TODAY, so the honest claim for this file is narrower — see
// TestKillVerbsInvalidateInFlightRegister's subtests, which must be checked individually.

// killVerb is one kill entry point plus the fence dimension it is responsible for moving.
//
// dims names the fenceSnap FIELDS the verb must move. It is not a count: four verbs move three
// dimensions (CloseProxy and CloseProxyIf both act on a port allocation, from different directions), so
// any assertion of the form len(verbs) == NumField(fenceSnap) is arithmetically wrong AND, worse, would
// go green exactly when someone adds a fourth fence dimension without a verb to move it — the scenario
// the fence's own comment says it exists to catch.
type killVerb struct {
	name string
	kill func(s *Server, sid string, publicPort int)
	dims []string
}

func killVerbs() []killVerb {
	return []killVerb{
		{
			name: "CloseProxy",
			kill: func(s *Server, _ string, port int) { s.CloseProxy(port) },
			dims: []string{"port"},
		},
		{
			name: "CloseProxyIf",
			kill: func(s *Server, sid string, port int) {
				// TokenHash must be the HASH, not the raw token: the REGISTER handler fences on
				// sessionFenceKeyFor(port, sid, nid, hashToken(token)), so passing the raw string here
				// bumps a DIFFERENT key and the kill silently misses. Writing this test the obvious way
				// produced exactly that false "CloseProxyIf does not fence" finding before the fixture was
				// checked against the handler — worth recording, because a per-allocation fence keyed on a
				// hash is easy to mis-target from a caller that holds the plaintext.
				s.CloseProxyIf(SessionInfo{
					PublicPort: port, SID: sid, NID: "lab-1", TokenHash: hashToken("token"),
				})
			},
			dims: []string{"alloc"},
		},
		{
			name: "CloseSession",
			kill: func(s *Server, sid string, _ int) { s.CloseSession(sid) },
			dims: []string{"sess"},
		},
		{
			name: "ForgetSession",
			kill: func(s *Server, sid string, _ int) { s.ForgetSession(sid) },
			dims: []string{"sess"},
		},
	}
}

// TestKillVerbsInvalidateInFlightRegister is the merged invariant: for EVERY kill verb, a REGISTER that
// has already passed token authorization must not go on to bind the public port.
func TestKillVerbsInvalidateInFlightRegister(t *testing.T) {
	for _, v := range killVerbs() {
		t.Run(v.name, func(t *testing.T) {
			controlPort := findFreePort(t)
			publicPort := findFreePort(t)

			authorized := make(chan struct{})
			releaseLookup := make(chan struct{})
			lookup := func(_, _ string, _ int, _ string, _ int64) error {
				close(authorized)
				<-releaseLookup
				return nil
			}

			srv := NewServer(
				net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
				"127.0.0.1",
				lookup,
				silentLog(),
			)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := srv.Start(ctx); err != nil {
				t.Fatal(err)
			}

			cli := NewClient(
				net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
				"lab", "lab-1",
				func(int) (int, error) { return 1, nil },
				silentLog(),
			)
			cli.Start(ctx)

			openDone := make(chan error, 1)
			go func() { openDone <- cli.Open(publicPort, 1, "token") }()

			select {
			case <-authorized:
			case <-time.After(2 * time.Second):
				t.Fatal("REGISTER never reached token authorization — the fixture is not exercising the race")
			}

			// The kill lands AFTER the lookup authorized this token and BEFORE handleAgent installed the
			// session in s.sessions. That window is the whole defect.
			v.kill(srv, "lab", publicPort)
			close(releaseLookup)

			select {
			case err := <-openDone:
				if err != nil {
					return // REGISTER refused outright — also correct
				}
				conn, dialErr := net.DialTimeout(
					"tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)), 500*time.Millisecond)
				if dialErr == nil {
					_ = conn.Close()
					t.Fatalf("the public proxy listener appeared AFTER %s returned — an exit that was "+
						"killed came back, and re-bound the public port. This is the hole rounds 2, 5 and 6 "+
						"each found in a different verb.", v.name)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("in-flight REGISTER did not finish")
			}
		})
	}
}

// TestEveryFenceDimensionHasAVerbThatMovesIt is the structural companion, and it asserts a MAPPING, not
// a cardinality.
//
// A count comparison (len(killVerbs) vs NumField(fenceSnap)) is wrong twice over: it is 4-vs-3 today, and
// the mutation it is supposed to catch — someone adds a fourth fence dimension and no verb moves it —
// would make it 4-vs-4, i.e. GREEN in exactly the scenario it exists for. The production comment on
// fenceSnap says a fourth dimension is "entirely plausible (per-nid, for multi-home)", so this is not a
// theoretical concern.
//
// What is asserted instead: every fenceSnap FIELD (read by reflection) is claimed by at least one verb,
// every dim a verb claims is a real field, and the field NAMES match what this table was written against.
//
// Mutation-verified in both directions: adding `nid int64` to fenceSnap without a verb fails here
// ("fenceSnap has a field \"nid\" this table has never heard of"), and renaming a field fails from the
// other side. The FIRST version of this test failed both mutations silently — it compared two literals
// that both lived in this file — which is why the reflection is load-bearing rather than stylistic.
func TestEveryFenceDimensionHasAVerbThatMovesIt(t *testing.T) {
	// THE FIELD SET IS READ FROM THE PRODUCTION TYPE BY REFLECTION, and the expected names are asserted
	// separately below.
	//
	// The first version of this test hand-wrote the field list, reasoning that a literal would fail
	// loudly on a rename while reflection would silently re-satisfy itself against the new name. That
	// reasoning was wrong in the worst possible direction, and the internal review demonstrated it with
	// two `go test -overlay` mutations that BOTH passed: adding a fourth fenceSnap field with no verb to
	// move it, and renaming fenceSnap.alloc. Neither could fail, because nothing in the test named the
	// production type at all — a literal-vs-literal comparison inside one file.
	//
	// Reflection restores the coupling: a new field appears here immediately and has to be claimed by a
	// verb. The rename concern is handled by asserting the EXPECTED NAMES explicitly (fenceSnapFieldNames
	// below), which is a deliberate, reviewable edit when a field is genuinely renamed rather than
	// something a rename silently satisfies.
	fields := map[string]bool{}
	st := reflect.TypeOf(fenceSnap{})
	for i := 0; i < st.NumField(); i++ {
		fields[st.Field(i).Name] = true
	}
	if len(fields) == 0 {
		t.Fatal("reflection found no fields on fenceSnap — the type moved or became opaque, and this gate " +
			"is now blind")
	}

	// The names this table was written against. A field RENAME shows up here as a failure naming both
	// sides, which is the loud outcome the literal version was trying to buy — without also making the
	// gate blind to a field being ADDED.
	fenceSnapFieldNames := map[string]bool{"port": true, "sess": true, "alloc": true}
	for f := range fields {
		if !fenceSnapFieldNames[f] {
			t.Errorf("fenceSnap has a field %q this table has never heard of. If it is a new fence "+
				"dimension, add a verb that moves it AND list it here. If it is a rename, update both. "+
				"Silence here is how a dimension nothing bumps gets shipped.", f)
		}
	}
	for f := range fenceSnapFieldNames {
		if !fields[f] {
			t.Errorf("this table expects fenceSnap.%s, which no longer exists (renamed or removed). The "+
				"verb rows below still claim it, so they are now claiming a dimension that is not there.", f)
		}
	}

	claimed := map[string]bool{}
	for _, v := range killVerbs() {
		if len(v.dims) == 0 {
			t.Errorf("kill verb %q declares no fence dimension — then nothing says what it is responsible "+
				"for moving, and a regression in it is invisible here", v.name)
		}
		for _, d := range v.dims {
			if !fields[d] {
				t.Errorf("kill verb %q claims fence dimension %q, which is not a field of fenceSnap. Either "+
					"the field was renamed (update this map AND the verb) or the claim is wrong.", v.name, d)
			}
			claimed[d] = true
		}
	}
	for f := range fields {
		if !claimed[f] {
			t.Errorf("fenceSnap.%s is moved by NO kill verb in the table.\n"+
				"A fence dimension nothing bumps is a dimension the REGISTER re-check compares against a "+
				"value that never changes — i.e. a fence that is always open. fenceSnap's own comment "+
				"predicts a fourth dimension (per-nid, multi-home); this is the assertion that makes adding "+
				"one without a verb fail instead of pass.", f)
		}
	}
}
