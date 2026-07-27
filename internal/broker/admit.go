package broker

import (
	"errors"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
)

// admit.go (batch B, B1) — the shared ingress admission decision.
//
// # WHY THIS EXISTS
//
// Six ctl-facing handlers carried the same five-step prologue, hand-copied:
// parse the subject, fingerprint the actor, check the session is ACTIVE, check the actor's
// standing in it, and (for most) check the target node is ONLINE. That is the wire
// authorization boundary. auth_callout JWTs carry a 24h TTL with NO revocation list, so this
// application-layer gate is the system's ONLY runtime revocation point — and broker.go:1320
// records that a session-scoped ingress has already shipped MISSING it once.
//
// WHAT IT DELIBERATELY DOES NOT DO — read before "finishing the job"
//
//   - It does not REPLY. The six verbs answer in four different shapes (ExecChunk's single
//     string, RunChunk's, KillResp's fixed Code:"rejected" with the real code in Error, and
//     the typed Code/Error structs). A gate that replied would need the message and a type
//     switch — the abstraction would be in the wrong place, and every one of those shapes is
//     pinned by ingress_characterization_test.go.
//   - It does not emit AUDIT. The refusal audit is not uniform and the differences are not
//     obviously intentional: exec emits on both node refusals, kill — twenty lines below it in
//     the same file, with a doc comment saying "same pre-flight as run.req" — emits on
//     neither, and no verb emits on the session/member refusals. Folding audit into the gate
//     would silently make those uniform, which is a behaviour change wearing a refactor's
//     clothes. The caller keeps it.
//   - It does not decide the CLUSTER-ROLE short circuit. Three different predicates guard
//     these handlers (isClusterFollower, clusterMode&&!IsLeader, transferHomeGate) and expose
//     runs its check BEFORE parsing while exec runs it after — an externally observable
//     difference in whether a follower answers a malformed subject at all. Each handler keeps
//     its own, in its own position.
//
// Its scope is exactly: "given a subject and a spec, who is this and may they proceed".
type admitRole int

const (
	// admitOwner is the ZERO VALUE on purpose. A verbSpec that fails to name a role demands
	// session OWNERSHIP — the strictest standing — so a spec added without thought refuses
	// more than it should rather than admitting more than it should. The polarity of every
	// field in verbSpec follows this rule.
	admitOwner admitRole = iota
	admitMember
)

// verbSpec is the per-verb configuration of the shared gate. EVERY field's zero value is the
// STRICTER choice; see admitOwner.
type verbSpec struct {
	// verb is asserted against the parsed subject's verb token. A spec with an empty verb
	// matches nothing, so a forgotten field refuses everything.
	verb string

	// role selects the standing predicate. Zero value = admitOwner (strictest).
	role admitRole

	// skipNodeCheck INVERTS the natural polarity deliberately. The zero value RUNS the
	// node-ONLINE check, so a spec that says nothing gets the stricter behaviour. Only
	// expose-rm sets it: it resolves its target by (sid, name) rather than by nid — an
	// expose must remain removable after its agent is gone, which is exactly when an
	// operator reaches for `expose rm`.
	skipNodeCheck bool

	// echoSubjectOnMalformed makes the refusal carry the offending subject as its detail.
	// Zero value = do not echo, which is both the majority behaviour and the less
	// information-leaking one. Only exec sets it, and only because it always has.
	echoSubjectOnMalformed bool
}

// ingress is the identity the gate resolved.
//
// It is populated PROGRESSIVELY, and admit returns whatever it had resolved when it refused —
// the sid/actor/nid as soon as the subject parses, the fp once the actor verifies. That is
// deliberate: the callers that emit an audit row on a LATE refusal (a node that is offline or
// absent) need the real identity to attribute it to, and reconstructing it at the call site
// would put a second subject parse back in every handler.
//
// The gate is the `ok` return, NEVER this struct. A populated ingress alongside ok=false means
// "here is who was asking", not "let them through".
type ingress struct {
	sid   string
	actor string
	nid   string
	verb  string
	fp    string
	// There is deliberately NO `status` field. The first version carried one, populated from the
	// node-online check, with a godoc explaining when callers could read it — and no caller ever
	// did. Internal review flagged it: dead state whose doc states a contract nobody relies on is
	// the same false-promise shape this batch exists to remove, and the next reader would have to
	// work out whether the contract was load-bearing. Add it back the day a caller needs it.
}

// denial carries a refusal in BOTH renderings the six verbs need, so no call site has to
// switch on the code to format its reply.
//
//	code + detail  — the typed-reply verbs (expose, expose-rm, upgrade)
//	reason         — the single-string form (exec, run, kill)
//
// They are built together at each refusal point rather than derived from one another,
// because the two families disagree about node_offline: the chunk verbs render
// "node_offline: status=OFFLINE" while the typed verbs put a bare "OFFLINE" in Error.
// Deriving one from the other would silently pick a winner.
type denial struct {
	code   string
	detail string
	reason string
}

func deny(code, detail, reason string) denial {
	return denial{code: code, detail: detail, reason: reason}
}

// admitSubject is the FIRST half: parse the subject and check it names this verb. It touches
// no storage.
//
// It is separate from admitACL for one reason, and the reason is not aesthetics: every
// converted handler except upgrade runs a CLUSTER-ROLE short circuit BETWEEN the two halves,
// and the short circuit's position relative to each is externally observable.
//
//	exec / run / kill / expose-rm : parse -> [follower? return silently] -> ACL
//	expose                        : [not leader? return silently] -> parse -> ACL
//	upgrade                       : parse -> ACL  (no short circuit at all)
//
// A single admit() doing both halves would move the follower short circuit to AFTER the
// session, membership and node lookups. That is not a micro-optimisation: exec, run, kill and
// expose-rm are all in isBroadcastClusterSubject (clusterwrite.go:59-80), so EVERY broker in
// the cluster receives EVERY one of these requests and every follower returns silently. Doing
// the storage reads first means an N-broker cluster performs 3*(N-1) extra RODB queries per
// ctl command, none of which can affect any reply.
//
// No test in the repo would see it: the characterization net constructs single-mode brokers,
// where isClusterFollower() is always false. internal/broker/admit_ordering_test.go guards the
// ordering structurally instead.
func (b *Broker) admitSubject(subject string, spec verbSpec) (ingress, denial, bool) {
	// A spec with no verb matches NOTHING. This has to be explicit, not implied by
	// `verb != spec.verb`: proto.ParseCmdBy accepts a well-formed subject whose verb token is
	// EMPTY (e.g. `…node.<nid>..req`, with a valid actor), and against such a subject the
	// comparison "" == "" succeeds. A verbSpec someone added without filling in `verb` would
	// then admit that request — the exact inversion of the zero-value-denies rule the rest of
	// this file is built on.
	//
	// Internal review found this. It is not reachable through any spec in the tree today
	// (all six set `verb`), which is precisely why it needed a structural answer rather than
	// a note: the failure mode is a spec added LATER.
	if spec.verb == "" {
		return ingress{}, deny("subject_malformed", "", "subject_malformed"), false
	}
	sid, actor, nid, verb, ok := proto.ParseCmdBy(subject)
	// `verb == ""` is REDUNDANT GIVEN the guard above, and that is worth stating rather than
	// implying: for it to matter you would need verb == "" AND verb == spec.verb, i.e.
	// spec.verb == "" — which the `spec.verb == ""` check already refused. It stays as
	// defence-in-depth so that deleting EITHER check alone still denies, and it is honest about
	// costing nothing: no test can exercise it independently while the first guard exists.
	if !ok || verb == "" || verb != spec.verb {
		d := deny("subject_malformed", "", "subject_malformed")
		if spec.echoSubjectOnMalformed {
			d = deny("subject_malformed", subject, "subject_malformed: "+subject)
		}
		return ingress{}, d, false
	}
	return ingress{sid: sid, actor: actor, nid: nid, verb: verb}, denial{}, true
}

// ctrlVerbSpec is the ctrl-family counterpart of verbSpec.
//
// THREE SUBJECT FAMILIES, ONE ACL. The nine hand-copied prologues are not one shape:
//
//	exec/run/kill/expose/expose-rm/upgrade : `cmd.by` — proto.ParseCmdBy, carries a nid
//	ps / node.list                        : `ctrl.by` + a hand-parsed leaf — ParseCtrlBy, no nid
//	proxy.status                          : `ctrl.<sid>.proxy.<action>` — ParseCtrlProxy, no nid
//
// The families cannot share a parser, and the reason is not cosmetic: ParseCmdBy VALIDATES the
// actor token while ParseCtrlBy and ParseCtrlProxy do not. Feeding all nine through one parser
// would make a malformed actor report subject_malformed on one family and actor_invalid on the
// other — swapping two wire codes on half the verb set. So each family keeps its own subject
// resolver and they share admitACL, which is where the actual authorization lives.
type ctrlVerbSpec struct {
	// tail is the leaf tokens AFTER the sid: {"ps","req"}, {"node","list","req"}. The full leaf is
	// always "s.<sid>." + tail. A spec with an empty tail matches nothing — same zero-value-denies
	// rule as verbSpec.
	tail []string

	// role selects the standing predicate. Zero value = admitOwner (strictest).
	role admitRole
}

// admitCtrlSubject is admitSubject for the `ctrl.by.<actor>.s.<sid>.<tail…>` family.
//
// It reproduces that family's OWN refusal conventions rather than the cmd.by ones, because both
// are on the wire:
//
//	ParseCtrlBy fails  → subject_malformed with NO detail
//	leaf shape wrong   → subject_malformed with the LEAF as detail (not the whole subject, which
//	                     is what exec echoes — a third convention, faithfully preserved)
func (b *Broker) admitCtrlSubject(subject string, spec ctrlVerbSpec) (ingress, denial, bool) {
	if len(spec.tail) == 0 {
		return ingress{}, deny("subject_malformed", "", "subject_malformed"), false
	}
	actor, leaf, ok := proto.ParseCtrlBy(subject)
	if !ok {
		return ingress{}, deny("subject_malformed", "", "subject_malformed"), false
	}
	parts := splitDot(leaf)
	if len(parts) != 2+len(spec.tail) || parts[0] != "s" {
		return ingress{}, deny("subject_malformed", leaf, "subject_malformed: "+leaf), false
	}
	for i, want := range spec.tail {
		if parts[2+i] != want {
			return ingress{}, deny("subject_malformed", leaf, "subject_malformed: "+leaf), false
		}
	}
	return ingress{sid: parts[1], actor: actor}, denial{}, true
}

// admitCtrl is admitCtrlSubject followed by admitACL. None of the ctrl-family verbs carries a nid,
// so the node check never applies — that is enforced structurally here rather than left to each
// spec to remember, because a spec that forgot `skipNodeCheck` would look up node "" and refuse
// every request with node_not_found.
func (b *Broker) admitCtrl(subject string, spec ctrlVerbSpec) (ingress, denial, bool) {
	ing, den, ok := b.admitCtrlSubject(subject, spec)
	if !ok {
		return ing, den, false
	}
	den, ok = b.admitACL(&ing, verbSpec{verb: "-", role: spec.role, skipNodeCheck: true})
	return ing, den, ok
}

// The ctrl-family specs. Both are member-readable.
//
// There is deliberately NO proxyStatusSpec, though one was written. proxy.status is the THIRD
// subject family — `ctrl.<sid>.proxy.<action>`, parsed by ParseCtrlProxy — not the
// `ctrl.by.<actor>.s.<sid>.<tail…>` shape a ctrlVerbSpec's `tail` describes. A spec here would have
// declared a subject the product never publishes and admitCtrlSubject would never match: a false map
// of the wire, which is worse than no map. handleProxyStatus resolves its own subject and calls the
// shared admitACL, which is where the authorization actually lives.
var (
	psSpec       = ctrlVerbSpec{tail: []string{"ps", "req"}, role: admitMember}
	nodeListSpec = ctrlVerbSpec{tail: []string{"node", "list", "req"}, role: admitMember}
)

// admit runs the whole prologue in one call, for the two handlers with nothing to interleave:
// upgrade (no cluster-role short circuit at all) and expose (its leader check runs BEFORE the
// subject is parsed, so by the time it reaches the gate there is nothing left to insert). It is
// admitSubject followed by admitACL and nothing else.
func (b *Broker) admit(subject string, spec verbSpec) (ingress, denial, bool) {
	ing, den, ok := b.admitSubject(subject, spec)
	if !ok {
		return ing, den, false
	}
	den, ok = b.admitACL(&ing, spec)
	return ing, den, ok
}

// admitACL is the SECOND half: resolve the actor, then apply the session / standing / node
// checks. It runs against storage and must not be called before the caller's cluster-role
// short circuit.
func (b *Broker) admitACL(ing *ingress, spec verbSpec) (denial, bool) {
	sid, actor, nid := ing.sid, ing.actor, ing.nid

	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		return deny("actor_invalid", err.Error(), "actor_invalid: "+err.Error()), false
	}
	ing.fp = fp

	// C.1 §6: every session-scoped ingress rejects a missing or DELETING session before it
	// can mutate anything.
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		return b.storeErrDenial("session.IsActive", err), false
	}
	if !active {
		return deny("session_not_found_or_deleting", "", "session_not_found_or_deleting"), false
	}

	switch spec.role {
	case admitMember:
		member, err := session.IsMember(b.cfg.DB, sid, fp)
		if err != nil {
			return b.storeErrDenial("session.IsMember", err), false
		}
		if !member {
			return deny("not_a_member", "", "not_a_member"), false
		}
	default: // admitOwner — also the zero value, so an unset role lands here.
		owner, err := session.IsOwner(b.cfg.DB, sid, fp)
		if err != nil {
			return b.storeErrDenial("session.IsOwner", err), false
		}
		if !owner {
			return deny("not_owner", "", "not_owner"), false
		}
	}

	if spec.skipNodeCheck {
		return denial{}, true
	}

	status, err := node.LookupStatus(b.cfg.DB, sid, nid)
	switch {
	case errors.Is(err, node.ErrNotFound):
		return deny("node_not_found", "", "node_not_found"), false
	case err != nil:
		return b.storeErrDenial("node.LookupStatus", err), false
	case status != node.StateOnline:
		// The two families render this differently and both renderings are pinned:
		// the chunk verbs prefix the status with "status=", the typed verbs do not.
		return deny("node_offline", string(status), "node_offline: status="+string(status)), false
	}
	return denial{}, true
}

// auditRefusal renders the audit.call error string for a refusal. It is a THIRD rendering,
// distinct from both reply forms: the audit row carries "node_offline:OFFLINE" — no space,
// no "status=" — where the chunk reply says "node_offline: status=OFFLINE" and the typed
// reply splits it into Code + Error. Three renderings of one fact is not a design; it is
// what the six hand-copied prologues had, and admit reproduces it rather than quietly
// picking a winner.
func auditRefusal(d denial) string {
	if d.code == "node_offline" {
		return "node_offline:" + d.detail
	}
	return d.code
}

// The per-verb specs. Their zero-value polarity is load-bearing — see verbSpec.
var (
	execSpec = verbSpec{verb: "exec", role: admitMember, echoSubjectOnMalformed: true}
	runSpec  = verbSpec{verb: "run", role: admitMember}
	killSpec = verbSpec{verb: "kill", role: admitMember}

	exposeSpec = verbSpec{verb: "expose", role: admitMember}
	// expose-rm resolves its target by (sid, name), not by nid, so an expose stays removable
	// after its agent is gone — which is exactly when an operator reaches for `expose rm`.
	exposeRmSpec = verbSpec{verb: "expose-rm", role: admitMember, skipNodeCheck: true}

	// upgrade is the one owner-gated verb here (J.4 §): pushing a new binary to an agent is
	// not a member-level action.
	upgradeSpec = verbSpec{verb: "upgrade", role: admitOwner}
)

// storeErrDenial renders a storage failure: the CODE goes on the wire, the SQLite text goes to
// the broker log.
//
// THIS IS AN INTENTIONAL, EXTERNALLY-VISIBLE BEHAVIOUR CHANGE (plan §5 decision #1).
//
// Every converted verb used to concatenate err.Error() onto the reply. docs/testing-standards.md
// §S4 says it must not — 存储层错误文本可能泄露路径与结构 — because SQLite's message carries the
// database path, table names and constraint names, and it was being handed to any session member
// who could provoke a read failure. transferGate was moved to this shape in batch A (its logStore
// closure); these nine are the remainder.
//
// WHAT DOES NOT CHANGE, and why the change is safe to make:
//
//   - The exit code. cmd/tether maps by CODE, not detail: brokerCodeExitClass("store_error") is
//     exitInternal either way, so no automation reclassifies.
//   - The operator's next step. error_hints.go's hint for store_error ALREADY reads "the broker hit
//     a SQLite error; check the broker log" — the log is where this text now is.
//   - Any other code's detail. actor_invalid / subject_malformed / node_offline keep theirs: §S4 is
//     about STORAGE text specifically, and those details are either the caller's own input echoed
//     back or a status enum.
//
// The reason this waited: ingress_characterization_test.go had no store_error case, so the change
// could not be shown to touch only what it intended. It has one now (the scenario closes the pool
// before the request), covering all nine verbs — so the diff in that file IS the evidence.
func (b *Broker) storeErrDenial(op string, err error) denial {
	if b.cfg.Logger != nil {
		b.cfg.Logger.Warn("broker: ingress gate storage error", "op", op, "err", err)
	}
	return deny("store_error", "", "store_error")
}
