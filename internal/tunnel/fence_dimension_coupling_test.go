package tunnel

import (
	"reflect"
	"sort"
	"strings"
	"testing"
)

// fence_dimension_coupling_test.go — the kill-verb table's `dims` must be tied to the REAL fenceSnap.
//
// origin: adversarial review of the B6 kill-fence merge.
//
// WHY THIS EXISTS ALONGSIDE kill_fence_test.go
// --------------------------------------------
// HISTORICAL, and it is why this file was written: TestEveryFenceDimensionHasAVerbThatMovesIt used to
// compare two LITERALS that both lived in kill_fence_test.go — `fields := map[string]bool{"port": true,
// "sess": true, "alloc": true}` and each row's `dims`. It never read the production type, so BOTH
// failures its own doc comment promised to catch were green:
//
//   - add a fourth dimension to fenceSnap with no verb that moves it  -> still green
//     ("this is the assertion that makes adding one without a verb fail instead of pass" — it was not);
//   - rename fenceSnap.alloc                                          -> still green
//     ("a RENAMED field fails here loudly" — it did not; nothing there named the production type).
//
// Both were confirmed by building the package against a mutated tunnel.go, not inferred.
//
// THAT IS NO LONGER TRUE. kill_fence_test.go now builds its field set by reflection over
// `reflect.TypeOf(fenceSnap{})`, so the 4th-dimension mutation reddens THERE too — verified, not
// assumed. This paragraph is kept as the record of why the coupling was added rather than deleted as
// stale, because a reader who finds two files apparently doing the same reflection deserves to know
// which came first and what the other one is still for.
//
// What this file still supplies that kill_fence_test.go does not is the BEHAVIOURAL direction, and one
// dimension the other file's table deliberately under-claims (see the CloseSession/port case below).
// The two directions the mapping needs:
//
//	STRUCTURAL  reflect over fenceSnap  -> every field is claimed by some verb, every claim is a field.
//	BEHAVIOURAL run each verb           -> it moves EXACTLY the dimensions its row claims.
//
// The behavioural half is the one that cannot be faked by editing a literal, and it is also the half
// that pins the pruning subtlety below.

// fenceSnapFieldNames reflects the production fence type. Reflection is the point: a literal list is
// exactly what makes the existing check tautological.
func fenceSnapFieldNames() []string {
	t := reflect.TypeOf(fenceSnap{})
	out := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		out = append(out, t.Field(i).Name)
	}
	sort.Strings(out)
	return out
}

// movedFenceDims names the fenceSnap fields whose value differs between two snapshots.
func movedFenceDims(before, after fenceSnap) []string {
	t := reflect.TypeOf(fenceSnap{})
	bv := reflect.ValueOf(before)
	av := reflect.ValueOf(after)
	var moved []string
	for i := 0; i < t.NumField(); i++ {
		if bv.Field(i).Int() != av.Field(i).Int() {
			moved = append(moved, t.Field(i).Name)
		}
	}
	sort.Strings(moved)
	return moved
}

// TestFenceSnapFieldsAreExactlyTheDimensionsTheVerbTableClaims binds the table to the production type.
//
// Failure it catches that kill_fence_test.go does not: someone adds fenceSnap.nid (its own comment
// predicts exactly this — "entirely plausible (per-nid, for multi-home)") and no kill verb bumps it. The
// REGISTER re-check at tunnel.go:402/:438 then compares a dimension against a value nothing ever changes,
// i.e. a fence permanently open in that direction — the same shape rounds 2, 5 and 6 each patched.
func TestFenceSnapFieldsAreExactlyTheDimensionsTheVerbTableClaims(t *testing.T) {
	fields := fenceSnapFieldNames()
	isField := map[string]bool{}
	for _, f := range fields {
		isField[f] = true
	}

	claimed := map[string]bool{}
	for _, v := range killVerbs() {
		if len(v.dims) == 0 {
			t.Errorf("kill verb %q claims no fence dimension", v.name)
		}
		for _, d := range v.dims {
			if !isField[d] {
				t.Errorf("kill verb %q claims dimension %q, which is NOT a field of fenceSnap %v. "+
					"Either the field was renamed (a rename kill_fence_test.go's literal map cannot see) "+
					"or the claim was always wrong.", v.name, d, fields)
			}
			claimed[d] = true
		}
	}
	for _, f := range fields {
		if !claimed[f] {
			t.Errorf("fenceSnap.%s is moved by NO verb in killVerbs().\n"+
				"A fence dimension nothing bumps is compared by the REGISTER re-check "+
				"(tunnel.go:402 and :438) against a value that never changes — a fence that is always "+
				"open in that direction, which is how a killed exit re-binds its public port.\n"+
				"fenceSnap fields: %v", f, fields)
		}
	}
}

// TestEachKillVerbMovesExactlyTheDimensionsItClaims is the behavioural half, on the NO-SESSION-INSTALLED
// arm — the arm the in-flight-REGISTER race actually runs in, and the only arm in which the table's
// `dims` are a complete description (see the sibling test below for why they are not, in general).
//
// Failure it catches: any verb that stops bumping the generation its row names, e.g. the allocation bump
// at tunnel.go:549.
//
// THE PRUNING TRAP (why the in-flight counters below are not decoration)
// ---------------------------------------------------------------------
// CloseProxyIf ends in maybePruneAllocationLocked and ForgetSession in maybePruneSessionLocked. With no
// REGISTER in flight, both DELETE the map entry they just incremented, and a deleted entry reads back as
// 0 — so a naive before/after snapshot sees 0 -> 0 and cannot tell "the verb bumped the fence" from "the
// verb did nothing". Registering an in-flight handler (exactly what handleAgent does at tunnel.go:359-360
// before it can be raced) is what keeps the bump observable.
func TestEachKillVerbMovesExactlyTheDimensionsItClaims(t *testing.T) {
	const (
		sid   = "lab"
		nid   = "lab-1"
		token = "token"
	)

	for _, v := range killVerbs() {
		t.Run(v.name, func(t *testing.T) {
			srv := NewServer("127.0.0.1:0", "127.0.0.1",
				func(_, _ string, _ int, _ string, _ int64) error { return nil }, silentLog())

			// A port number is all the fence needs; nothing is bound, so no listener is installed and
			// each verb's "no session" arm runs — which is precisely the arm the in-flight-REGISTER race
			// exercises, and the arm in which the declared dims are the ONLY dims that may move.
			const port = 45001
			key := sessionFenceKeyFor(port, sid, nid, hashToken(token))

			srv.mu.Lock()
			srv.ensureAllocationFenceMapsLocked()
			srv.inflightBySID[sid]++
			srv.inflightByAllocation[key]++
			before := srv.fenceSnapLocked(port, sid, key)
			srv.mu.Unlock()

			v.kill(srv, sid, port)

			srv.mu.Lock()
			after := srv.fenceSnapLocked(port, sid, key)
			srv.mu.Unlock()

			moved := movedFenceDims(before, after)
			want := append([]string(nil), v.dims...)
			sort.Strings(want)

			if strings.Join(moved, ",") != strings.Join(want, ",") {
				t.Fatalf("%s moved fence dimensions %v, but its row in killVerbs() claims %v.\n"+
					"before=%+v after=%+v\n"+
					"If the verb stopped bumping a dimension, an in-flight REGISTER that already passed "+
					"token authorization will pass the re-check and install a public listener AFTER the "+
					"kill returned. If the row is merely out of date, fix the row — a dims list nothing "+
					"verifies is documentation that drifts silently.",
					v.name, moved, want, before, after)
			}
		})
	}
}

// TestSessionScopedKillVerbsAlsoMoveThePortDimension pins the arm the killVerbs table under-describes.
//
// CloseSession (tunnel.go:721-728) and ForgetSession (:619-625) both walk s.sessions and bump
// s.killGen[port] for every listener they close, so with a session INSTALLED they move `port` as well as
// `sess`. Their rows in killVerbs() claim only "sess", and TestEveryFenceDimensionHasAVerbThatMovesIt
// asserts a UNION, so the under-claim is invisible there: `port` is covered by CloseProxy's row.
//
// That matters because the per-port bump is what fences an in-flight REGISTER for a DIFFERENT sid racing
// on the same public port. Deleting `s.killGen[port]++` from CloseSession leaves the whole tunnel package
// green today — confirmed by building against a mutated tunnel.go. This is the assertion that reddens it.
func TestSessionScopedKillVerbsAlsoMoveThePortDimension(t *testing.T) {
	const (
		sid   = "lab"
		nid   = "lab-1"
		token = "token"
		port  = 45002
	)

	for _, name := range []string{"CloseSession", "ForgetSession"} {
		t.Run(name, func(t *testing.T) {
			var verb killVerb
			for _, v := range killVerbs() {
				if v.name == name {
					verb = v
				}
			}
			if verb.kill == nil {
				t.Fatalf("killVerbs() no longer contains %q — a session-scoped kill verb was removed or "+
					"renamed without updating this assertion", name)
			}

			srv := NewServer("127.0.0.1:0", "127.0.0.1",
				func(_, _ string, _ int, _ string, _ int64) error { return nil }, silentLog())
			key := sessionFenceKeyFor(port, sid, nid, hashToken(token))

			srv.mu.Lock()
			srv.ensureAllocationFenceMapsLocked()
			srv.inflightBySID[sid]++
			// An installed listener owned by sid. closeServerSession tolerates nil listener/yamux/conn;
			// cancel must be non-nil because it is called unconditionally (tunnel.go:591).
			srv.sessions[port] = &serverSession{
				sid: sid, nid: nid, publicPort: port, tokenHash: hashToken(token),
				cancel: func() {},
			}
			before := srv.fenceSnapLocked(port, sid, key)
			srv.mu.Unlock()

			verb.kill(srv, sid, port)

			srv.mu.Lock()
			after := srv.fenceSnapLocked(port, sid, key)
			_, stillInstalled := srv.sessions[port]
			srv.mu.Unlock()

			if stillInstalled {
				t.Fatalf("%s left the session installed on port %d", name, port)
			}
			moved := map[string]bool{}
			for _, d := range movedFenceDims(before, after) {
				moved[d] = true
			}
			for _, want := range []string{"sess", "port"} {
				if !moved[want] {
					t.Errorf("%s closed an installed listener but did NOT move fenceSnap.%s "+
						"(before=%+v after=%+v).\n"+
						"An in-flight REGISTER that snapshotted this port before the kill will pass the "+
						"re-check at tunnel.go:402/:438 and re-bind the public port after the kill switch "+
						"already returned.", name, want, before, after)
				}
			}
		})
	}
}
