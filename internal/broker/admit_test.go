package broker

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
)

// admit_test.go (batch B, B1) — the safety properties of the shared gate itself.
//
// ingress_characterization_test.go proves the six converted verbs still BEHAVE the same. This
// file proves the things that file cannot: that the gate's failure modes point the safe way,
// and that a spec written carelessly refuses rather than admits.

// TestVerbSpecZeroValueDenies is the single most important assertion about admit(). Every field
// of verbSpec was given a polarity such that its zero value is the STRICTER choice, because the
// realistic way this gate gets weakened is not a wrong value — it is a field nobody set.
func TestVerbSpecZeroValueDenies(t *testing.T) {
	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger(), Now: time.Now}}
	owner := freshUserActor(t)
	fp, err := auth.FingerprintFromActor(owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(b.cfg.DB, "lab", "lab", fp, "pin-hash", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.cfg.DB.Exec(`INSERT INTO nodes(sid,nid,status) VALUES(?,?, 'ONLINE')`, "lab", "lab-1"); err != nil {
		t.Fatal(err)
	}
	subject := proto.SubjCmdBy("lab", owner, "lab-1", "exec")

	// 1. An entirely zero spec must admit NOTHING: its empty verb matches no subject.
	if _, den, ok := b.admit(subject, verbSpec{}); ok {
		t.Error("the ZERO verbSpec admitted a request — a spec added without any fields set must " +
			"refuse everything, not inherit whatever the subject happens to say")
	} else if den.code != "subject_malformed" {
		t.Errorf("zero spec refused with %q; expected subject_malformed (empty verb matches nothing)", den.code)
	}
	// 1b. The empty-verb attack, and the reason there are TWO guards for it.
	//
	// proto.ParseCmdBy ACCEPTS a well-formed subject whose verb token is empty:
	// subjects.go:324-334 requires exactly 11 segments and validates sid/actor/nid, returning
	// parts[9] (the verb) unchecked. So `…node.<nid>..req` parses with ok=true and verb=="".
	//
	// Two comments here have already been wrong about this, in opposite directions, so the state
	// of play is written out once:
	//
	//   - The FIRST version claimed ParseCmdBy REJECTS those subjects. False — the probe behind
	//     that claim used an INVALID actor, so the parser refused at the actor check and it looked
	//     like the verb check was covered.
	//   - Internal review then read it as "admit's `verb == \"\"` clause is the load-bearing guard
	//     and nothing tests it". Also not quite right: that clause is REDUNDANT given
	//     `spec.verb == ""`, because reaching it requires verb == spec.verb == "", which the first
	//     guard already refused. A mutation deleting only `verb == ""` leaves every test green,
	//     and that is correct behaviour, not a coverage hole.
	//
	// What actually needs pinning is the COMBINED property, from both directions:
	//   case A — a spec nobody filled in matches nothing, including an empty-verb subject;
	//   case B — a populated spec is never satisfied by an empty verb token.
	emptyVerbSubject := proto.SubjectPrefix + ".s.lab.cmd.by." + owner + ".node.lab-1..req"
	if _, _, _, parsedVerb, ok := proto.ParseCmdBy(emptyVerbSubject); !ok || parsedVerb != "" {
		t.Fatalf("proto.ParseCmdBy(%q) now returns ok=%v verb=%q. It used to accept the subject and "+
			"hand back an EMPTY verb, which is what makes admit's own `verb == \"\"` guard load-bearing. "+
			"A stricter parser is fine — but case B below then no longer reaches that guard, so update "+
			"this test rather than letting it pass vacuously.", emptyVerbSubject, ok, parsedVerb)
	}
	// Case A: a spec with no verb matches nothing.
	for _, malformed := range []string{
		emptyVerbSubject,
		proto.SubjectPrefix + ".s.lab.cmd.by." + owner + ".node.lab-1.req", // one segment short
	} {
		if _, _, ok := b.admit(malformed, verbSpec{}); ok {
			t.Errorf("the ZERO verbSpec ADMITTED %q — the zero value stopped denying", malformed)
		}
	}
	// Case B: a populated spec must not be satisfied by an empty verb token. Today this holds
	// because "" != "exec"; it is asserted anyway because the property is what matters, not the
	// line that currently delivers it — a future spec-matching change (prefix match, normalisation,
	// a wildcard verb) could make "" match while every other test stayed green.
	if _, den, ok := b.admit(emptyVerbSubject, verbSpec{verb: "exec", role: admitMember}); ok {
		t.Error("a populated verbSpec ADMITTED a subject whose verb token is EMPTY — admit's " +
			"`verb == \"\"` guard is gone, and ParseCmdBy does not do it for us")
	} else if den.code != "subject_malformed" {
		t.Errorf("empty verb token refused with %q, want subject_malformed", den.code)
	}

	// 2. A spec that names the verb but forgets `role` must demand OWNERSHIP, not membership.
	//    The owner passes (proving the test is not just observing a broken gate)...
	if _, den, ok := b.admit(subject, verbSpec{verb: "exec"}); !ok {
		t.Fatalf("the session OWNER was refused by a role-less spec (%q) — the zero role must be "+
			"admitOwner, and the owner must satisfy it", den.code)
	}
	//    ...and a plain member does NOT.
	member := freshUserActor(t)
	mfp, err := auth.FingerprintFromActor(member)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.AddMember(b.cfg.DB, "lab", mfp, session.RoleMember, session.ViaPin, time.Now().UTC()); err != nil {
		t.Fatalf("add member: %v", err)
	}
	memberSubject := proto.SubjCmdBy("lab", member, "lab-1", "exec")
	if _, _, ok := b.admit(memberSubject, verbSpec{verb: "exec", role: admitMember}); !ok {
		t.Fatal("a real member was refused by an explicit admitMember spec — the fixture is wrong, " +
			"so the next assertion would pass for the wrong reason")
	}
	if _, den, ok := b.admit(memberSubject, verbSpec{verb: "exec"}); ok {
		t.Error("a role-less spec admitted a non-owner MEMBER. The zero value of admitRole must be " +
			"admitOwner (the strictest standing), so that a spec someone forgot to fill in refuses " +
			"more than intended rather than less.")
	} else if den.code != "not_owner" {
		t.Errorf("role-less spec refused a member with %q, want not_owner", den.code)
	}

	// 3. A spec that says nothing about the node must RUN the node check. skipNodeCheck is
	//    inverted for exactly this reason.
	if _, err := b.cfg.DB.Exec(`UPDATE nodes SET status='OFFLINE' WHERE sid=? AND nid=?`, "lab", "lab-1"); err != nil {
		t.Fatal(err)
	}
	if _, den, ok := b.admit(subject, verbSpec{verb: "exec"}); ok {
		t.Error("a spec that set neither skipNodeCheck nor anything else admitted a request for an " +
			"OFFLINE node — the zero value must run the check")
	} else if den.code != "node_offline" {
		t.Errorf("expected node_offline, got %q", den.code)
	}
	// And the opt-out genuinely opts out, else the assertion above proves only that the node is
	// offline.
	if _, _, ok := b.admit(subject, verbSpec{verb: "exec", skipNodeCheck: true}); !ok {
		t.Error("skipNodeCheck did not suppress the node check — the field is inert, which would " +
			"make expose-rm reject every removal once its agent went offline")
	}
}

// TestAdmitReturnsIdentityOnLateRefusal pins the contract the audit-emitting callers depend on:
// a refusal that happens AFTER the actor is resolved must still carry the identity, or every
// converted handler would attribute its refusal audit to an empty session and fingerprint.
func TestAdmitReturnsIdentityOnLateRefusal(t *testing.T) {
	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger(), Now: time.Now}}
	owner := freshUserActor(t)
	fp, err := auth.FingerprintFromActor(owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(b.cfg.DB, "lab", "lab", fp, "pin-hash", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Session exists, node row does not -> a LATE refusal.
	ing, den, ok := b.admit(proto.SubjCmdBy("lab", owner, "lab-1", "exec"),
		verbSpec{verb: "exec", role: admitMember})
	if ok || den.code != "node_not_found" {
		t.Fatalf("expected a node_not_found refusal, got ok=%v code=%q", ok, den.code)
	}
	if ing.sid != "lab" || ing.nid != "lab-1" || ing.actor != owner || ing.fp != fp {
		t.Errorf("late refusal lost the identity: %+v — the callers emit pubAuditCall with these "+
			"fields, so an empty ingress writes an unattributable audit row", ing)
	}
	if ing.verb != "exec" {
		t.Errorf("late refusal lost the verb (%q) — handlers use it to build the forward subject", ing.verb)
	}
}

// TestAdmitRefusalRenderingsStayDistinct pins the three-way rendering split by asking ADMIT
// for a refusal and inspecting what it produced.
//
// The first version of this test called `deny("node_offline", "STALE", "node_offline: status=STALE")`
// and then asserted the three fields equalled the three arguments — it tested that a struct
// literal assigns its own arguments, not that admit() renders anything. Internal review caught
// it. The distinction matters because the rendering split is admit()'s job: it is the one place
// that decides the typed reply gets a bare "STALE", the chunk reply gets "status=STALE", and the
// audit row gets "node_offline:STALE" with no space.
func TestAdmitRefusalRenderingsStayDistinct(t *testing.T) {
	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger(), Now: time.Now}}
	owner := freshUserActor(t)
	fp, err := auth.FingerprintFromActor(owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(b.cfg.DB, "lab", "lab", fp, "pin-hash", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.cfg.DB.Exec(`INSERT INTO nodes(sid,nid,status) VALUES(?,?, 'STALE')`, "lab", "lab-1"); err != nil {
		t.Fatal(err)
	}

	_, d, ok := b.admit(proto.SubjCmdBy("lab", owner, "lab-1", "exec"), execSpec)
	if ok {
		t.Fatal("a STALE node must be refused")
	}
	if d.code != "node_offline" {
		t.Fatalf("typed-reply Code: got %q want node_offline", d.code)
	}
	if d.detail != "STALE" {
		t.Errorf("typed-reply Error must be the BARE status (expose/upgrade put this straight into "+
			"Error): got %q want %q", d.detail, "STALE")
	}
	if d.reason != "node_offline: status=STALE" {
		t.Errorf("chunk-reply reason must carry the \"status=\" prefix (exec/run/kill render one "+
			"string and ctl prints it whole): got %q", d.reason)
	}
	if got := auditRefusal(d); got != "node_offline:STALE" {
		t.Errorf("audit rendering must be %q — no space, no \"status=\" — got %q",
			"node_offline:STALE", got)
	}
	// The three must be genuinely DIFFERENT strings; if a refactor collapsed any two, the
	// assertions above could all still pass against a single shared value.
	if d.detail == d.reason || d.reason == auditRefusal(d) || d.detail == auditRefusal(d) {
		t.Errorf("two of the three renderings became the same string (detail=%q reason=%q audit=%q) "+
			"— one of the three consumers is now getting another one's bytes",
			d.detail, d.reason, auditRefusal(d))
	}

	// A non-node refusal renders identically in the audit stream and the code field.
	if _, d2, ok := b.admit(proto.SubjCmdBy("lab", freshUserActor(t), "lab-1", "exec"), execSpec); ok {
		t.Error("a non-member must be refused")
	} else if got := auditRefusal(d2); got != "not_a_member" {
		t.Errorf("non-node audit rendering changed: %q", got)
	}
}

// TestStoreErrorDetailReachesTheLogNotTheWire is the other half of plan §5 decision #1.
//
// The characterization net proves the detail LEFT the wire. This proves it ARRIVED in the log —
// and those are different claims. Batch A's own M13 review note records the exact failure of
// conflating them: A2 moved a store_error detail "to the broker log", the detail was simply
// dropped, and the log line it was supposedly moved to did not exist. The operator lost the one
// string that says whether the DB is locked, corrupt, or missing a table.
func TestStoreErrorDetailReachesTheLogNotTheWire(t *testing.T) {
	var logged bytes.Buffer
	b := &Broker{cfg: Config{
		DB:     openDB(t),
		Logger: slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})),
		Now:    time.Now,
	}}
	owner := freshUserActor(t)
	fp, err := auth.FingerprintFromActor(owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(b.cfg.DB, "lab", "lab", fp, "pin-hash", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Break the pool so every gate read fails.
	if err := b.cfg.DB.Close(); err != nil {
		t.Fatal(err)
	}

	_, den, ok := b.admit(proto.SubjCmdBy("lab", owner, "lab-1", "exec"), execSpec)
	if ok {
		t.Fatal("a closed pool must refuse")
	}
	if den.code != "store_error" {
		t.Fatalf("code: got %q want store_error", den.code)
	}
	if den.detail != "" || den.reason != "store_error" {
		t.Errorf("the SQLite text is still on the wire (detail=%q reason=%q) — §S4 requires the "+
			"code alone; that text carries the db path, table and constraint names",
			den.detail, den.reason)
	}

	out := logged.String()
	if !strings.Contains(out, "broker: ingress gate storage error") {
		t.Fatalf("no storage-error log line was emitted. The detail was REMOVED from the wire and "+
			"went NOWHERE — which is strictly worse than leaving it on the wire, and is exactly the "+
			"defect batch-A review M13 recorded for the same change on transferGate.\nlog:\n%s", out)
	}
	if !strings.Contains(out, "op=session.IsActive") {
		t.Errorf("the log line does not name WHICH read failed. Without the op an operator cannot "+
			"tell a missing table from a locked db.\nlog:\n%s", out)
	}
	if !strings.Contains(out, "err=") {
		t.Errorf("the log line carries no error text — the thing that was moved off the wire is "+
			"not in the log either.\nlog:\n%s", out)
	}
}

// TestStoreErrorLogIsNotVacuous proves the assertion above can fail: a broker with a nil Logger
// must not panic (several fixtures build one), and a HEALTHY pool must produce no such line — so
// "the line is present" is evidence of the failure path running, not of the handler being noisy.
func TestStoreErrorLogIsNotVacuous(t *testing.T) {
	// nil Logger: the gate must still refuse, without panicking.
	nb := &Broker{cfg: Config{DB: openDB(t), Now: time.Now}}
	_ = nb.cfg.DB.Close()
	if _, den, ok := nb.admit(proto.SubjCmdBy("lab", freshUserActor(t), "lab-1", "exec"), execSpec); ok ||
		den.code != "store_error" {
		t.Errorf("with a nil Logger the gate must still refuse store_error, got ok=%v code=%q", ok, den.code)
	}

	// Healthy pool, real refusal (non-member): no storage-error line.
	var logged bytes.Buffer
	b := &Broker{cfg: Config{
		DB:     openDB(t),
		Logger: slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})),
		Now:    time.Now,
	}}
	owner := freshUserActor(t)
	fp, _ := auth.FingerprintFromActor(owner)
	if _, err := session.Create(b.cfg.DB, "lab", "lab", fp, "pin-hash", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.cfg.DB.Exec(`INSERT INTO nodes(sid,nid,status) VALUES(?,?, 'ONLINE')`, "lab", "lab-1"); err != nil {
		t.Fatal(err)
	}
	if _, den, ok := b.admit(proto.SubjCmdBy("lab", freshUserActor(t), "lab-1", "exec"), execSpec); ok ||
		den.code != "not_a_member" {
		t.Fatalf("expected a not_a_member refusal, got ok=%v code=%q", ok, den.code)
	}
	if strings.Contains(logged.String(), "ingress gate storage error") {
		t.Errorf("a non-member refusal logged a STORAGE error — the log line fires on the wrong "+
			"path, so its presence proves nothing:\n%s", logged.String())
	}
}

// admitRefusalCodes is the CLOSED set of codes admit() can return. It is deliberately a
// package-private list checked by a package-private test: plan §5 decision #7 considered
// exporting it for cmd/tether's error-code gate to consume and rejected that, because a
// cross-package code registry is precisely the sync point batch-A's A1 was built to remove.
//
// What this buys: cmd/tether/error_code_coverage_test.go exempts the six admit() reply sites
// because their code is a variable there. That exemption's honesty depends on the set being
// CLOSED — a ninth code appearing here would reach the wire through an exempted site and be
// classified by nothing. This test is what makes "closed" a fact instead of a claim.
var admitRefusalCodes = []string{
	"subject_malformed",
	"actor_invalid",
	"store_error",
	"session_not_found_or_deleting",
	"not_a_member",
	"not_owner",
	"node_not_found",
	"node_offline",
}

// TestAdmitRefusalCodeSet re-derives admit's refusal codes from source and asserts they are
// exactly admitRefusalCodes.
func TestAdmitRefusalCodeSet(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "admit.go", nil, 0)
	if err != nil {
		t.Fatalf("parse admit.go: %v", err)
	}

	found := map[string]bool{}
	calls := 0
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if !ok || id.Name != "deny" || len(call.Args) == 0 {
			return true
		}
		calls++
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			t.Errorf("deny() is called with a non-literal code (%T) — the set is no longer "+
				"statically knowable, and the cmd/tether exemption that points at this test "+
				"becomes unbacked", call.Args[0])
			return true
		}
		v, err := strconv.Unquote(lit.Value)
		if err != nil {
			t.Errorf("unquote %s: %v", lit.Value, err)
			return true
		}
		found[v] = true
		return true
	})

	if calls < 8 {
		t.Fatalf("found only %d deny() call(s) in admit.go — the scan is broken, and a broken "+
			"scan reports the set unchanged", calls)
	}
	// storeErrDenial builds its refusal through deny() too, but with the code as a literal
	// inside that helper; the walk above covers the whole file, so it is included.
	want := map[string]bool{}
	for _, c := range admitRefusalCodes {
		want[c] = true
	}
	for c := range found {
		if !want[c] {
			t.Errorf("admit() can now return the code %q, which is NOT in admitRefusalCodes.\n"+
				"cmd/tether/error_code_coverage_test.go exempts the six admit() reply sites on the "+
				"grounds that this set is closed and every member is emitted as a literal elsewhere "+
				"in the scanned tree. Add %q there too (or give it an exit class), then list it here.", c, c)
		}
	}
	for c := range want {
		if !found[c] {
			t.Errorf("admitRefusalCodes lists %q but admit() no longer emits it — delete it, or the "+
				"list stops describing the real set", c)
		}
	}
}

// TestConvertedVerbSpecsMatchTheirHandlers guards the one thing a wrong spec would do silently:
// admit asserts spec.verb against the subject, so a spec whose verb does not match the subject
// its handler is subscribed to refuses EVERY request with subject_malformed. That reads as "the
// feature is broken" rather than "the gate is misconfigured", and no existing test would say why.
func TestConvertedVerbSpecsMatchTheirHandlers(t *testing.T) {
	for _, c := range []struct {
		spec verbSpec
		verb string
	}{
		{execSpec, "exec"},
		{runSpec, "run"},
		{killSpec, "kill"},
		{exposeSpec, "expose"},
		{exposeRmSpec, "expose-rm"},
		{upgradeSpec, "upgrade"},
	} {
		if c.spec.verb != c.verb {
			t.Errorf("spec for %q declares verb %q — admit would refuse every request for this "+
				"handler with subject_malformed", c.verb, c.spec.verb)
		}
	}
	// The role assignments are a product decision, not an implementation detail: upgrade is
	// owner-only (J.4 §) and the other five are member-level. Stating it here means a change
	// has to be deliberate.
	if upgradeSpec.role != admitOwner {
		t.Error("upgrade is no longer owner-gated — it rewrites the agent's binary, so member-level " +
			"access to it is a privilege escalation")
	}
	for _, c := range []struct {
		name string
		spec verbSpec
	}{{"exec", execSpec}, {"run", runSpec}, {"kill", killSpec},
		{"expose", exposeSpec}, {"expose-rm", exposeRmSpec}} {
		if c.spec.role != admitMember {
			t.Errorf("%s is no longer member-gated (role=%v) — that is a behaviour change for every "+
				"non-owner member of every session", c.name, c.spec.role)
		}
	}
	// expose-rm is the ONLY converted verb that skips the node check.
	if !exposeRmSpec.skipNodeCheck {
		t.Error("expose-rm now runs the node check — `expose rm` would start failing exactly when " +
			"an operator needs it, i.e. after the agent went away")
	}
	for _, c := range []struct {
		name string
		spec verbSpec
	}{{"exec", execSpec}, {"run", runSpec}, {"kill", killSpec},
		{"expose", exposeSpec}, {"upgrade", upgradeSpec}} {
		if c.spec.skipNodeCheck {
			t.Errorf("%s now skips the node check — it forwards to an agent, so it would publish to "+
				"a subject with no subscriber and hang ctl until its own timeout", c.name)
		}
	}
	// exec is the only verb that echoes the subject.
	if !execSpec.echoSubjectOnMalformed {
		t.Error("exec stopped echoing the offending subject on subject_malformed — a wire-visible change")
	}
	for _, c := range []struct {
		name string
		spec verbSpec
	}{{"run", runSpec}, {"kill", killSpec}, {"expose", exposeSpec},
		{"expose-rm", exposeRmSpec}, {"upgrade", upgradeSpec}} {
		if c.spec.echoSubjectOnMalformed {
			t.Errorf("%s started echoing the subject on subject_malformed — a wire-visible change, "+
				"and it leaks the actor nkey back to an unauthenticated caller", c.name)
		}
	}
}
