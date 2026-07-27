package broker

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	nodepkg "github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
)

// wire_freeze_test.go (batch B, commit 1) — freeze the cluster.apply broker<->broker wire.
//
// WHAT THIS GUARDS
//
// `cluster.apply.<verb>` is a CROSS-VERSION wire. During a rolling broker upgrade an old
// broker forwards to a new leader and vice versa, so BOTH halves are load-bearing:
//
//	the VERB STRING   — dispatchForward's switch matches it literally; a renamed const
//	                    value makes every in-flight forward from the other version fall
//	                    into `default:` and come back as a permanent business error.
//	the PAYLOAD KEYS  — the leader re-decodes the payload and re-runs the domain Plan.
//	                    A renamed JSON key decodes to the zero value, so the leader plans
//	                    against a DIFFERENT input than the originator intended, commits it
//	                    to raft, and every replica applies the wrong thing. There is no
//	                    error anywhere on that path.
//
// Before this file, NOTHING pinned either half: there was not a single verb literal in any
// _test.go in the repo. Two payload types are worse than untagged —
// `node.RegisterInput` and `proc.Process` carry NO json tags at all, so their Go FIELD
// NAMES are the wire. Renaming `RegisterInput.ProxyCapable` is, today, a silent
// cross-version data loss that compiles, passes `make test`, and passes
// `make e2e-parallel` (both are single-binary, so both ends always agree).
//
// WHAT THIS IS AND IS NOT
//
// This is a FREEZE, not a correctness assertion. The golden below records what the wire
// IS as of batch B, derived mechanically from the types. Its job is to make a change
// VISIBLE and force the author to answer "is this a compatible change, and what is the
// migration?" — not to claim the current shape is right. Changing an entry here is
// allowed; changing it WITHOUT a stated cross-version story is the defect.
//
// SCOPE, stated honestly (testing-standards A3): this freezes the 17 verbs, the envelope, and
// every payload type dispatchForward decodes — the last of those re-derived from source by
// TestEveryDispatchedPayloadTypeIsFrozen, so a new arm cannot skip the freeze.
//
// Nesting is covered TRANSITIVELY, not one level deep. Types reachable only through a frozen
// root are frozen too and there are three of them today: `proto.NodeRegisterReq` (through
// ReconcilePayload.Req) and `proto.LocalProcess` / `proto.LocalPort` (through
// NodeRegisterReq's LocalProcesses/LocalPorts slices).
// TestFrozenWireTypesHaveNoUnfrozenNesting enforces the rule — every struct reachable from a
// frozen root must itself be a frozen root — and its walk sees through pointers, slices,
// arrays and maps.
//
// (An earlier version of this paragraph said such types were "none today" and were "NOT walked
// recursively". Both halves were false, and the file's own note further down describes catching
// exactly the LocalProcess/LocalPort nesting the paragraph denied existed. It survived one round
// of internal review that recorded it as corrected without correcting it; this is the correction.)
//
// NOT covered, and it cannot be: an `any`/interface-typed field's dynamic type is unknowable
// statically. The walk reports such a field rather than passing over it.

// frozenForwardVerbs maps the Go constant name to its wire value. Both halves matter:
// the NAME so a deleted const is caught, the VALUE so a retyped one is.
var frozenForwardVerbs = map[string]string{
	"VerbProvision":          "provision",
	"VerbJoin":               "join",
	"VerbReconcile":          "reconcile",
	"VerbTransferAudit":      "xferaudit",
	"VerbAlertSignal":        "alertsignal",
	"VerbAlertAck":           "alertack",
	"VerbSessionCreate":      "sessioncreate",
	"VerbBusNkeySet":         "busnkeyset",
	"VerbPortFree":           "portfree",
	"VerbPortRevoke":         "portrevoke",
	"VerbPortFreeAllocation": "portfreealloc",
	"VerbNodeRegister":       "noderegister",
	"VerbProcInsert":         "procinsert",
	"VerbProcMarkExited":     "procmarkexited",
	"VerbSessionTombstone":   "sessiontombstone",
	"VerbSessionDrop":        "sessiondrop",
	"VerbNodeEvict":          "nodeevict",
}

// frozenEnvelopeKeys freezes the envelope and reply that wrap EVERY verb. A change here
// breaks all 17 at once.
var frozenEnvelopeKeys = map[string][]string{
	"forwardEnvelope": {"payload", "req_id", "verb"},
	"forwardReply":    {"err_kind", "err_msg", "status"},
}

// frozenPayloadKeys freezes each verb payload's JSON key set (sorted). A key derived from
// a Go field NAME rather than a json tag is recorded with its field name — see
// TestUntaggedPayloadsAreDeclared, which pins exactly which types are in that state so
// nobody discovers it by causing an outage.
var frozenPayloadKeys = map[string][]string{
	"ProvisionPayload":          {"fp", "nid", "pin", "sid"},
	"JoinPayload":               {"fp", "pin", "sid"},
	"ReconcilePayload":          {"nid", "req", "sid"},
	"AlertSignalPayload":        {"active", "kind", "message", "node", "severity"},
	"AlertAckPayload":           {"acked_by", "dedup_key"},
	"SessionCreatePayload":      {"fp", "name", "pin_hash"},
	"BusNkeySetPayload":         {"bus_nkey_pub", "node_id"},
	"PortMutatePayload":         {"port"},
	"PortFreeAllocationPayload": {"name", "nid", "port", "sid", "token_hash"},
	"ProcMarkExitedPayload":     {"ended_at", "exit_code", "pid"},
	"SessionMutatePayload":      {"sid"},
	"EvictPayload":              {"nid", "sid"},

	// UNTAGGED — the Go field names below ARE the wire. See TestUntaggedPayloadsAreDeclared.
	"node.RegisterInput": {"Arch", "BootID", "NID", "NatsServer", "OS", "ProtoVersion",
		"ProxyCapable", "ReleaseVersion", "SID"},
	"proc.Process": {"Argv", "BootID", "Cwd", "EndedAt", "ExitCode", "NID", "PID", "SID",
		"StartTimeTicks", "StartedAt", "StartedByFP", "Status"},

	// Tagged, and shared with other wires (the audit stream) — frozen here because the
	// forward envelope carries it verbatim.
	"schema.AuditTransfer": {"actor_fp", "actor_nkey", "bucket", "bytes", "code", "duration_ms",
		"error", "kind", "node", "path", "session", "sha256", "size", "tier", "transfer_id",
		"ts", "v", "verb"},

	// Reached through ReconcilePayload.Req. These ride the forward envelope AND are the
	// agent->broker register wire, so a change here breaks two wires at once.
	"proto.NodeRegisterReq": {"arch", "boot_id", "capabilities", "local_ports", "local_processes",
		"nid", "os", "proto_version", "release_version", "roster_gen", "roster_refresh_only",
		"server_id"},
	"proto.LocalProcess": {"pid", "rc", "start_time_ticks", "started_at", "state"},
	"proto.LocalPort":    {"local_port", "name", "port", "token_hash"},
}

// untaggedPayloadTypes names the payload types whose wire keys come from Go field names
// because the struct has no json tags. Adding a type here is a decision, not an accident:
// for these, `go vet` sees a rename as a refactor and the compiler helps you do it
// consistently — across the whole repo, on BOTH sides — which is exactly why the wire
// break is invisible.
var untaggedPayloadTypes = map[string]string{
	"node.RegisterInput": "D9 §3 forwards this verbatim as VerbNodeRegister; it is a domain " +
		"input type that predates the forward wire and was never tagged.",
	"proc.Process": "D9 §3 forwards this verbatim as VerbProcInsert; same history.",
}

// payloadSpecimens binds each frozen name to a value to reflect over. Kept as an explicit
// table (not discovered) so a payload that stops being forwarded has to be deleted here.
func payloadSpecimens() map[string]any {
	return map[string]any{
		"ProvisionPayload":          ProvisionPayload{},
		"JoinPayload":               JoinPayload{},
		"ReconcilePayload":          ReconcilePayload{},
		"AlertSignalPayload":        AlertSignalPayload{},
		"AlertAckPayload":           AlertAckPayload{},
		"SessionCreatePayload":      SessionCreatePayload{},
		"BusNkeySetPayload":         BusNkeySetPayload{},
		"PortMutatePayload":         PortMutatePayload{},
		"PortFreeAllocationPayload": PortFreeAllocationPayload{},
		"ProcMarkExitedPayload":     ProcMarkExitedPayload{},
		"SessionMutatePayload":      SessionMutatePayload{},
		"EvictPayload":              EvictPayload{},
		"node.RegisterInput":        nodepkg.RegisterInput{},
		"proc.Process":              proc.Process{},
		"schema.AuditTransfer":      schema.AuditTransfer{},
		"proto.NodeRegisterReq":     proto.NodeRegisterReq{},
		"proto.LocalProcess":        proto.LocalProcess{},
		"proto.LocalPort":           proto.LocalPort{},
	}
}

func envelopeSpecimens() map[string]any {
	return map[string]any{
		"forwardEnvelope": forwardEnvelope{},
		"forwardReply":    forwardReply{},
	}
}

// jsonKeys derives the wire key set of a struct: the json tag name when present, else the
// Go field name (which IS the wire for an untagged struct). Fields tagged `-` are excluded
// because they never reach the wire.
//
// EMBEDDED STRUCTS ARE PROMOTED (external review M3). This used to emit the embedded TYPE's name as
// a single key, which is not what encoding/json does: an anonymous struct field without a json tag
// has its fields flattened into the parent object. No frozen specimen embeds a struct today, so the
// bug was latent — but latent in the worst direction, because the golden would then have agreed with
// itself while disagreeing with the actual wire, and this file's whole purpose is to be the thing
// that does not do that. An embedded field WITH a tag is not promoted (encoding/json nests it under
// the tag name), so that case keeps the tag.
func jsonKeys(t reflect.Type) []string {
	out := jsonKeysInto(t, nil)
	sort.Strings(out)
	return out
}

func jsonKeysInto(t reflect.Type, out []string) []string {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag, tagged := f.Tag.Lookup("json")
		head, _, _ := strings.Cut(tag, ",")
		if tagged && head == "-" {
			continue
		}
		if f.Anonymous && head == "" {
			// Promotion: encoding/json flattens an untagged embedded struct (or pointer to one).
			ft := f.Type
			if ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				out = jsonKeysInto(ft, out)
				continue
			}
			// An embedded non-struct (e.g. a named string type) is NOT flattened; it keys on
			// its type name, which is the existing behaviour.
		}
		if f.PkgPath != "" { // unexported: encoding/json skips it
			continue
		}
		name := f.Name
		if tagged && head != "" {
			name = head
		}
		out = append(out, name)
	}
	return out
}

func TestForwardVerbsAreWireFrozen(t *testing.T) {
	live := map[string]string{
		"VerbProvision":          VerbProvision,
		"VerbJoin":               VerbJoin,
		"VerbReconcile":          VerbReconcile,
		"VerbTransferAudit":      VerbTransferAudit,
		"VerbAlertSignal":        VerbAlertSignal,
		"VerbAlertAck":           VerbAlertAck,
		"VerbSessionCreate":      VerbSessionCreate,
		"VerbBusNkeySet":         VerbBusNkeySet,
		"VerbPortFree":           VerbPortFree,
		"VerbPortRevoke":         VerbPortRevoke,
		"VerbPortFreeAllocation": VerbPortFreeAllocation,
		"VerbNodeRegister":       VerbNodeRegister,
		"VerbProcInsert":         VerbProcInsert,
		"VerbProcMarkExited":     VerbProcMarkExited,
		"VerbSessionTombstone":   VerbSessionTombstone,
		"VerbSessionDrop":        VerbSessionDrop,
		"VerbNodeEvict":          VerbNodeEvict,
	}
	for name, want := range frozenForwardVerbs {
		got, ok := live[name]
		if !ok {
			t.Errorf("frozen verb %s has no live binding in this test — the const was renamed "+
				"or deleted; a rolling upgrade would route its forwards to dispatchForward's default", name)
			continue
		}
		if got != want {
			t.Errorf("verb %s: wire value changed %q -> %q. This is a CROSS-VERSION wire break: an "+
				"old broker still publishes cluster.apply.%s and the new leader answers "+
				"'unknown verb'. State the migration or revert.", name, want, got, want)
		}
	}
	for name := range live {
		if _, ok := frozenForwardVerbs[name]; !ok {
			t.Errorf("verb %s is bound in this test but absent from frozenForwardVerbs — add it "+
				"with its wire value", name)
		}
	}
}

// TestForwardVerbConstantsAreAllFrozen is the completeness half: it re-derives the verb
// const set from source, so a NEW verb added to cluster_forward.go cannot skip the freeze
// by simply not being listed in the hand-written binding above.
func TestForwardVerbConstantsAreAllFrozen(t *testing.T) {
	found := scanVerbConsts(t, "cluster_forward.go")
	if len(found) < 10 {
		t.Fatalf("verb-const scan found only %d consts — the scanner is broken, not the source "+
			"(there are 17 today). A vacuous freeze is worse than none.", len(found))
	}
	for name, val := range found {
		want, ok := frozenForwardVerbs[name]
		if !ok {
			t.Errorf("NEW forward verb %s = %q is not frozen. Add it to frozenForwardVerbs AND "+
				"give it a dispatchForward arm; an unfrozen verb can be renamed silently.", name, val)
			continue
		}
		if want != val {
			t.Errorf("verb %s: source says %q, freeze says %q", name, val, want)
		}
	}
	for name := range frozenForwardVerbs {
		if _, ok := found[name]; !ok {
			t.Errorf("frozen verb %s no longer exists in cluster_forward.go — deleting a verb is a "+
				"wire change; an old broker may still publish it", name)
		}
	}
}

// scanVerbConsts parses one file in this package and returns every `Verb<X> = "<lit>"`
// const it declares.
func scanVerbConsts(t *testing.T, file string) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	return verbConstsFromAST(f)
}

// verbConstsFromAST is the predicate, split out so TestWireFreezeScannerSelfCheck can
// exercise the SAME code path the live scan uses (a self-check against a re-implementation
// proves nothing).
func verbConstsFromAST(f *ast.File) map[string]string {
	out := map[string]string{}
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range vs.Names {
				if !strings.HasPrefix(name.Name, "Verb") || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				if v, err := strconv.Unquote(lit.Value); err == nil {
					out[name.Name] = v
				}
			}
		}
	}
	return out
}

// TestDispatchForwardCoversEveryFrozenVerb pins the OTHER direction: every frozen verb must
// have an arm. A verb with a freeze entry but no arm answers `unknown verb` at runtime while
// looking fully wired at review time.
func TestDispatchForwardCoversEveryFrozenVerb(t *testing.T) {
	arms := scanDispatchArms(t, "cluster_forward.go")
	if len(arms) < 10 {
		t.Fatalf("dispatch-arm scan found only %d arms — scanner broken (17 today)", len(arms))
	}
	for name := range frozenForwardVerbs {
		if !arms[name] {
			t.Errorf("frozen verb %s has NO arm in dispatchForward — a forward for it returns "+
				"'unknown verb' as a permanent business error", name)
		}
	}
	for name := range arms {
		if _, ok := frozenForwardVerbs[name]; !ok {
			t.Errorf("dispatchForward has an arm for %s, which is not frozen", name)
		}
	}
}

// scanDispatchArms returns the verb constants the leader-side dispatch table maps.
//
// It reads the KEYS of the `writeVerbs` map literal. Batch B / B2 replaced dispatchForward's
// 17-arm switch with that table, and this scanner had to move with it: it used to collect
// CaseClause idents, which after the conversion would have matched NOTHING and reported every
// frozen verb as un-dispatched — or, with the reverse check removed, silently reported success.
// A scanner coupled to a syntactic shape has to be updated in the SAME commit that changes the
// shape; the `len(arms) < 10` floor in the caller is what would have caught it if it had not been.
func scanDispatchArms(t *testing.T, file string) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	return dispatchArmsFromAST(f)
}

// dispatchArmsFromAST is scanDispatchArms with the parse lifted out, so the self-check can drive
// THIS function on synthetic source instead of reimplementing it (external review M3). The old
// self-check contained its own inline case-clause scanner and tested that — while the live scanner
// had been rewritten to read the writeVerbs map. It was therefore proving a scanner that no longer
// existed was not vacuous.
func dispatchArmsFromAST(f *ast.File) map[string]bool {
	out := map[string]bool{}
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok || len(vs.Names) == 0 || vs.Names[0].Name != "writeVerbs" || len(vs.Values) == 0 {
				continue
			}
			lit, ok := vs.Values[0].(*ast.CompositeLit)
			if !ok {
				continue
			}
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if id, ok := kv.Key.(*ast.Ident); ok && strings.HasPrefix(id.Name, "Verb") {
					out[id.Name] = true
				}
			}
		}
	}
	return out
}

// TestEveryDispatchedPayloadTypeIsFrozen closes the completeness hole in the payload half.
//
// The verb half is already complete: TestForwardVerbConstantsAreAllFrozen re-derives the const
// set from source, so a new verb cannot skip the freeze. The payload half was NOT:
// payloadSpecimens() is a hand-kept table, so a new verb whose arm decodes a brand-new type
// simply would not appear in it, and the entire freeze suite would stay green while that type's
// keys were unfrozen — worst of all if it were UNTAGGED, where the Go field names are the wire.
// Internal review found this.
//
// So: re-derive the decoded type of every dispatchForward arm from the AST and require each to
// be a frozen root.
func TestEveryDispatchedPayloadTypeIsFrozen(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "cluster_forward.go", nil, 0)
	if err != nil {
		t.Fatalf("parse cluster_forward.go: %v", err)
	}

	// Source spelling -> the name frozenPayloadKeys uses. cluster_forward.go imports
	// internal/node under the alias `nodepkg`, so the AST sees a name the freeze table does not.
	freezeNameFor := map[string]string{
		"nodepkg.RegisterInput": "node.RegisterInput",
	}
	frozen := map[string]bool{}
	for name := range frozenPayloadKeys {
		frozen[name] = true
	}

	// The payload type of each table entry is the type of the SECOND parameter of the plan closure
	// passed to propose()/proposeWithReqID(): `func(db *sql.DB, p PayloadT, now time.Time)`.
	//
	// This replaced a scan for `var p T` inside dispatchForward's case clauses when B2 turned the
	// switch into the writeVerbs table. Both shapes are syntactic, and that is the point of the
	// non-vacuity floor below: a scanner silently matching nothing reports every payload frozen.
	var decoded []string
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok || len(vs.Names) == 0 || vs.Names[0].Name != "writeVerbs" || len(vs.Values) == 0 {
				continue
			}
			lit, ok := vs.Values[0].(*ast.CompositeLit)
			if !ok {
				continue
			}
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				builder, ok := kv.Value.(*ast.CallExpr) // propose(...) / proposeWithReqID(...)
				if !ok || len(builder.Args) != 1 {
					continue
				}
				plan, ok := builder.Args[0].(*ast.FuncLit)
				if !ok || plan.Type.Params == nil || len(plan.Type.Params.List) < 2 {
					continue
				}
				switch tp := plan.Type.Params.List[1].Type.(type) {
				case *ast.Ident:
					decoded = append(decoded, tp.Name)
				case *ast.SelectorExpr:
					if pkg, ok := tp.X.(*ast.Ident); ok {
						decoded = append(decoded, pkg.Name+"."+tp.Sel.Name)
					}
				}
			}
		}
	}

	if len(decoded) < 10 {
		t.Fatalf("found only %d decoded payload type(s) in dispatchForward — the scan is broken, "+
			"and a broken scan reports every payload frozen (there are 15 distinct types today)",
			len(decoded))
	}
	for _, name := range decoded {
		canonical := name
		if mapped, ok := freezeNameFor[name]; ok {
			canonical = mapped
		}
		if !frozen[canonical] {
			t.Errorf("dispatchForward decodes into %s, which is NOT in frozenPayloadKeys.\n"+
				"Its JSON key set is therefore unfrozen — and if the type carries no json tags, its "+
				"Go FIELD NAMES are the wire and a rename is a silent cross-version break. Add it to "+
				"frozenPayloadKeys + payloadSpecimens (and to untaggedPayloadTypes if it has no tags).",
				name)
		}
	}
}

func TestForwardPayloadKeysAreWireFrozen(t *testing.T) {
	for name, spec := range payloadSpecimens() {
		want, ok := frozenPayloadKeys[name]
		if !ok {
			t.Errorf("payload %s has no freeze entry", name)
			continue
		}
		got := jsonKeys(reflect.TypeOf(spec))
		if !reflect.DeepEqual(got, want) {
			t.Errorf("payload %s wire keys changed:\n  frozen: %v\n  live:   %v\n"+
				"A renamed key decodes to the ZERO VALUE on the other version's leader, which then "+
				"plans against different input and commits it to raft. There is no error on that path.",
				name, want, got)
		}
	}
	for name := range frozenPayloadKeys {
		if _, ok := payloadSpecimens()[name]; !ok {
			t.Errorf("frozen payload %s has no specimen — it was deleted or renamed", name)
		}
	}
}

func TestForwardEnvelopeKeysAreWireFrozen(t *testing.T) {
	for name, spec := range envelopeSpecimens() {
		want := frozenEnvelopeKeys[name]
		got := jsonKeys(reflect.TypeOf(spec))
		if !reflect.DeepEqual(got, want) {
			t.Errorf("envelope %s wire keys changed:\n  frozen: %v\n  live:   %v\n"+
				"This wraps all %d verbs — a change here breaks every forward at once.",
				name, want, got, len(frozenForwardVerbs))
		}
	}
	// Reverse direction: a frozen entry with no specimen means the type was renamed or deleted and
	// the golden is now describing nothing. The payload freeze has always had this check; the
	// envelope freeze did not (external review M3).
	for name := range frozenEnvelopeKeys {
		if _, ok := envelopeSpecimens()[name]; !ok {
			t.Errorf("frozen envelope %s has no specimen — it was renamed or deleted, and its "+
				"golden is now unenforced", name)
		}
	}
}

// TestEveryForwardWrapperTypeHasAnEnvelopeSpecimen closes the gap the two hand-kept maps leave
// between them (external review M3): a NEW wrapper type can be absent from BOTH, and then nothing
// fails. Both maps are hand-written, so agreement between them proves nothing about completeness.
//
// The inventory is derived from the SOURCE instead: every struct type in cluster_forward.go that is
// marshalled or unmarshalled on the forward path must appear in envelopeSpecimens. That is the same
// technique TestEveryDispatchedPayloadTypeIsFrozen uses for payloads.
func TestEveryForwardWrapperTypeHasAnEnvelopeSpecimen(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "cluster_forward.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Every UNEXPORTED struct in cluster_forward.go counts, not just ones whose names end in
	// Envelope/Reply. The suffix rule made the "every wrapper" claim naming-convention dependent, so
	// a future `forwardFrame`/`wireResult` would evade the freeze by not being named for it.
	var wrappers []string
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			if _, isStruct := ts.Type.(*ast.StructType); !isStruct {
				continue
			}
			if !ts.Name.IsExported() {
				wrappers = append(wrappers, ts.Name.Name)
			}
		}
	}

	if len(wrappers) < 2 {
		t.Fatalf("found only %d wrapper type(s) in cluster_forward.go (%v); the forward wire has at "+
			"least forwardEnvelope and forwardReply, so the scan is not reading the file and this "+
			"check is vacuous", len(wrappers), wrappers)
	}
	specimens := envelopeSpecimens()
	for _, name := range wrappers {
		if _, ok := specimens[name]; !ok {
			if reason, exempt := offWireForwardStructs[name]; exempt {
				if reason == "" {
					t.Errorf("%s is exempted from the envelope freeze with an EMPTY reason", name)
				}
				continue
			}
			t.Errorf("%s is an unexported struct in cluster_forward.go with NO envelope specimen, so "+
				"its keys are frozen nowhere. If it rides the forward wire, add a specimen: it wraps "+
				"every verb, and a renamed key there breaks all of them at once in a mixed-release "+
				"window with no error on the path. If it is implementation-only and never marshalled, "+
				"declare it in offWireForwardStructs with the reason.", name)
		}
	}

	// A stale exemption is worse than none: it silently keeps a type that HAS since joined the wire
	// out of the freeze. Every declared exemption must still name a real off-wire struct.
	live := map[string]bool{}
	for _, n := range wrappers {
		live[n] = true
	}
	for name := range offWireForwardStructs {
		switch {
		case !live[name]:
			t.Errorf("offWireForwardStructs declares %q, which is no longer an unexported struct in "+
				"cluster_forward.go — remove the stale entry", name)
		case specimens[name] != nil:
			t.Errorf("%q is declared off-wire but now HAS an envelope specimen; one of the two is "+
				"wrong, and the exemption is the half that silences the check", name)
		}
	}
}

// offWireForwardStructs names the unexported structs in cluster_forward.go that are
// implementation-only and never cross the cluster.apply wire, so they have no frozen key set.
//
// It exists because the discovery above is deliberately over-broad (every unexported struct), which
// is the right trade — a false positive costs one line here, a false negative is an unfrozen wire
// type. The empty map is the honest state today: both unexported structs in that file ARE the wire.
// Adding an entry is a decision with a reason attached, and the reverse check above makes a stale one
// fail rather than quietly widen the hole.
var offWireForwardStructs = map[string]string{}

// TestUntaggedPayloadsAreDeclared makes the "field name IS the wire" hazard explicit and
// bounded. If a NEW untagged type joins the forward wire, this fails and the author has to
// write down why, rather than discovering it during a rolling upgrade.
func TestUntaggedPayloadsAreDeclared(t *testing.T) {
	for name, spec := range payloadSpecimens() {
		rt := reflect.TypeOf(spec)
		tagged := false
		for i := 0; i < rt.NumField(); i++ {
			if _, ok := rt.Field(i).Tag.Lookup("json"); ok {
				tagged = true
				break
			}
		}
		_, declared := untaggedPayloadTypes[name]
		switch {
		case !tagged && !declared:
			t.Errorf("%s is on the forward wire with NO json tags, so its Go FIELD NAMES are the "+
				"wire — a rename is a silent cross-version break that the compiler helps you make "+
				"consistently on both sides. Declare it in untaggedPayloadTypes with the reason, or tag it.", name)
		case tagged && declared:
			t.Errorf("%s is declared untagged but now HAS json tags — remove it from "+
				"untaggedPayloadTypes (a stale declaration hides the next real one)", name)
		}
	}
}

// TestFrozenWireTypesHaveNoUnfrozenNesting is what makes the freeze's COVERAGE claim true
// rather than merely asserted. jsonKeys is one level deep, so a nested struct would be
// recorded as a single key ("req", "local_ports") whose INTERIOR is unfrozen — the freeze
// would look complete while covering none of it.
//
// The rule is transitive: every struct reachable from a frozen root must itself be a frozen
// root. The only exemption is time.Time, which marshals as an RFC3339 scalar and has no
// wire key set of its own.
//
// This is the check that caught proto.NodeRegisterReq's own LocalProcesses/LocalPorts
// nesting when this file was first written; the first draft froze NodeRegisterReq and
// stopped, silently leaving nine keys uncovered.
func TestFrozenWireTypesHaveNoUnfrozenNesting(t *testing.T) {
	roots := payloadSpecimens()
	frozen := map[string]bool{"time.Time": true}
	for _, spec := range roots {
		frozen[reflect.TypeOf(spec).String()] = true
	}
	for name, spec := range roots {
		for _, hit := range unfrozenNestedFields(reflect.TypeOf(spec), frozen) {
			t.Errorf("%s.%s nests %s, which is NOT a frozen root. jsonKeys is one level deep, "+
				"so its interior keys are unfrozen while the freeze reads as complete. Add %s to "+
				"frozenPayloadKeys + payloadSpecimens, or flatten the field.",
				name, hit.field, hit.typeName, hit.typeName)
		}
	}
}

type nestingHit struct{ field, typeName string }

// unfrozenNestedFields is THE walk — the live check and the self-check below both call it, so
// the self-check exercises the real code path rather than a copy of it. (The first version
// re-implemented the walk inline in the self-check, which meant a bug in the real one would go
// unnoticed while the self-check stayed green — internal review caught that.)
//
// It unwraps pointers, slices, arrays AND maps. Maps matter: encoding/json marshals a
// map[string]SomeStruct into an object whose VALUES carry that struct's keys, so a map value
// type is on the wire exactly like a slice element type.
func unfrozenNestedFields(rt reflect.Type, frozen map[string]bool) []nestingHit {
	var out []nestingHit
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.PkgPath != "" {
			continue // unexported: never marshalled
		}
		if tag, ok := f.Tag.Lookup("json"); ok {
			if head, _, _ := strings.Cut(tag, ","); head == "-" {
				continue
			}
		}
		for _, ft := range reachableStructTypes(f.Type) {
			if !frozen[ft.String()] {
				out = append(out, nestingHit{field: f.Name, typeName: ft.String()})
			}
		}
	}
	return out
}

// reachableStructTypes returns every struct type a field's declared type can put on the wire.
// A map contributes BOTH its key and its value type (a struct key marshals to a JSON object
// key only via TextMarshaler, but the value is unconditionally on the wire).
func reachableStructTypes(t reflect.Type) []reflect.Type {
	var out []reflect.Type
	var walk func(reflect.Type, int)
	walk = func(t reflect.Type, depth int) {
		if depth > 8 {
			return // self-referential type; the frozen-root check below still applies at depth 0
		}
		switch t.Kind() {
		case reflect.Ptr, reflect.Slice, reflect.Array:
			walk(t.Elem(), depth+1)
		case reflect.Map:
			walk(t.Key(), depth+1)
			walk(t.Elem(), depth+1)
		case reflect.Struct:
			out = append(out, t)
		case reflect.Interface:
			// An interface field's dynamic type is unknowable statically, so the freeze
			// cannot cover it. Report it as a struct-shaped hole by name.
			out = append(out, t)
		}
	}
	walk(t, 0)
	return out
}

// TestNestingWalkSelfCheck proves the transitive walk rejects an unfrozen nested struct in
// every shape it must see through. It calls the SAME function the live check calls.
func TestNestingWalkSelfCheck(t *testing.T) {
	type unfrozenInner struct{ X string }
	type outer struct {
		Inner     unfrozenInner
		Slice     []unfrozenInner
		Ptr       *unfrozenInner
		MapValue  map[string]unfrozenInner
		Array     [2]unfrozenInner
		Iface     any
		Scalar    int
		Skipped   unfrozenInner `json:"-"`
		unxported unfrozenInner //nolint:unused // exercises the unexported skip
	}
	frozen := map[string]bool{"time.Time": true}
	hits := unfrozenNestedFields(reflect.TypeOf(outer{}), frozen)

	got := map[string]bool{}
	for _, h := range hits {
		got[h.field] = true
	}
	for _, want := range []string{"Inner", "Slice", "Ptr", "MapValue", "Array", "Iface"} {
		if !got[want] {
			t.Errorf("the nesting walk did not see through %s — a field of that shape escapes the "+
				"freeze entirely while every test stays green", want)
		}
	}
	if got["Scalar"] {
		t.Error("the walk reported a scalar field as nested — it over-matches")
	}
	if got["Skipped"] {
		t.Error(`the walk reported a json:"-" field — that field never reaches the wire`)
	}
	if got["unxported"] {
		t.Error("the walk reported an unexported field — encoding/json never marshals one")
	}
	// And it must be capable of reporting NOTHING, else "no hits" is not evidence of coverage.
	frozen["broker.unfrozenInner"] = true
	if rest := unfrozenNestedFields(reflect.TypeOf(outer{}), frozen); len(rest) != 1 {
		// `Iface` stays, because an interface's dynamic type cannot be frozen.
		t.Errorf("after freezing the inner type the walk still reports %v; only the interface "+
			"field should remain", rest)
	}
}

// TestWireFreezeScannerSelfCheck proves the two AST scanners are not vacuous. Without it, a
// scanner that silently degenerated to matching nothing would report a perfectly green
// "every verb is frozen" — the exact failure mode error_code_coverage_test.go was built to
// avoid, and the reason its own self-check exists.
func TestWireFreezeScannerSelfCheck(t *testing.T) {
	// The `switch` below is a DECOY: it is the shape the dispatch scanner used to read. A scanner
	// still matching case clauses would report VerbStaleSwitchOnly, which the live table does not
	// contain — that is the regression this pins (external review M3).
	const src = `package broker
const (
	VerbSynthetic = "synthetic"
	NotAVerb      = "ignored"
	VerbNoLiteral = someOtherConst
)
var writeVerbs = map[string]verbHandler{
	VerbSynthetic: propose(planSynthetic),
	NotAVerb:      propose(planIgnored),
}
func leftoverDispatch() {
	switch v {
	case VerbStaleSwitchOnly:
	}
}`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "synthetic.go", src, 0)
	if err != nil {
		t.Fatalf("parse synthetic source: %v", err)
	}

	consts := verbConstsFromAST(f)
	if got, ok := consts["VerbSynthetic"]; !ok || got != "synthetic" {
		t.Errorf("const scanner missed the synthetic verb (got %q, present=%v) — the live scan "+
			"would report every verb frozen while seeing none", got, ok)
	}
	if _, ok := consts["NotAVerb"]; ok {
		t.Error("const scanner matched a non-Verb const — it over-matches")
	}
	if _, ok := consts["VerbNoLiteral"]; ok {
		t.Error("const scanner resolved a non-literal value — it must report only what it can " +
			"resolve exactly, not guess")
	}

	// Drive the LIVE scanner, not a copy of it.
	arms := dispatchArmsFromAST(f)
	if !arms["VerbSynthetic"] {
		t.Error("the live dispatch scanner missed a writeVerbs key — against the real table it " +
			"would report every frozen verb as un-dispatched, or (with the reverse check absent) " +
			"report success while reading nothing")
	}
	if arms["NotAVerb"] {
		t.Error("the live dispatch scanner matched a non-Verb map key — it over-matches")
	}
	if arms["VerbStaleSwitchOnly"] {
		t.Error("the live dispatch scanner is still reading `case` clauses. dispatchForward is a " +
			"TABLE now; a scanner that reads both would keep passing after the table was emptied, " +
			"as long as some leftover switch mentioned the verbs")
	}
}

// TestJSONKeyDeriverSelfCheck proves jsonKeys distinguishes the three shapes the freeze
// depends on: a tag, no tag, and `-`. A deriver that always returned field names would make
// every tagged freeze entry wrong-but-consistent, and nobody would notice until a rename.
func TestJSONKeyDeriverSelfCheck(t *testing.T) {
	type sample struct {
		Tagged   string `json:"tagged_name"`
		Untagged string
		Omitted  string `json:"-"`
		WithOpts string `json:"with_opts,omitempty"`
		lower    string //nolint:unused // exercises the unexported-field skip
	}
	got := jsonKeys(reflect.TypeOf(sample{}))
	want := []string{"Untagged", "tagged_name", "with_opts"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("jsonKeys is not deriving wire names correctly: got %v, want %v", got, want)
	}

	// EMBEDDED PROMOTION (external review M3). No frozen specimen embeds a struct today, so this is
	// the only place the behaviour is exercised — and it is checked against encoding/json itself
	// rather than against a hand-written expectation, because "what the wire actually is" is the one
	// claim this whole file rests on and a golden derived from the same wrong assumption as the
	// deriver would agree with it.
	type inner struct {
		A string `json:"a"`
		B string
	}
	type nestedByTag struct {
		X string `json:"x"`
	}
	type embedder struct {
		inner              // untagged embed: encoding/json PROMOTES a and B
		Tagged nestedByTag `json:"nested"` // named field: nests under "nested"
		Own    string      `json:"own"`
	}
	for _, tc := range []struct {
		name string
		spec any
	}{
		{"promoted embed", embedder{}},
		{"flat sample", sample{}},
	} {
		derived := jsonKeys(reflect.TypeOf(tc.spec))
		actual := marshalledKeys(t, tc.spec)
		if !reflect.DeepEqual(derived, actual) {
			t.Errorf("%s: jsonKeys derived %v but encoding/json actually emits %v.\n"+
				"Every freeze golden in this file is computed by jsonKeys, so a deriver that "+
				"disagrees with the marshaller freezes a wire shape that does not exist.",
				tc.name, derived, actual)
		}
	}
}

// marshalledKeys is the ground truth: the top-level object keys encoding/json really produces. Used
// only in the deriver's self-check — the freeze goldens themselves stay hand-kept, so a wire change
// still has to be written down by a human.
//
// It POPULATES every field first. A zero-valued specimen drops every `omitempty` key, so comparing
// against one would report the deriver wrong for a field that is simply absent — the same
// fully-populated-fixture requirement the allocation-identity freeze needed.
func marshalledKeys(t *testing.T, v any) []string {
	t.Helper()
	filled := reflect.New(reflect.TypeOf(v)).Elem()
	fillNonZero(filled)
	raw, err := json.Marshal(filled.Interface())
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(obj))
	for k := range obj {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// fillNonZero sets every settable field to a non-zero value, recursing into structs, so no
// `omitempty` key is dropped from the marshalled form.
func fillNonZero(v reflect.Value) {
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if !f.CanSet() {
				continue // unexported: encoding/json skips it too
			}
			fillNonZero(f)
		}
	case reflect.String:
		v.SetString("x")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fillNonZero(v.Index(0))
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
	case reflect.Ptr:
		v.Set(reflect.New(v.Type().Elem()))
		fillNonZero(v.Elem())
	}
}
