package broker

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// ingress_characterization_test.go (batch B, commit 2) — the CHARACTERIZATION net for the
// broker's ingress admission surface.
//
// WHY THIS EXISTS BEFORE ANY PRODUCTION CHANGE
//
// B1 collapses nine hand-copied ingress prologues into one admit(). "Behaviour-preserving"
// is not checkable against a surface nobody has written down: before this file, the repo had
// essentially no coverage of the REFUSAL paths (grepping the handlers' names across
// internal/broker/*_test.go found one file, and it exercises a success path). Every negative
// case below is therefore recorded from the CURRENT implementation, on an unmodified tree,
// so that afterwards a diff in this file is the review artifact — not a judgement call.
//
// This file makes no claim that the recorded behaviour is RIGHT. Three of the shapes it pins
// are, in the plan's judgement, wrong and are scheduled to change (docs/reviews/batch-b-plan.md
// §5 decision #1). When they do, the diff here is exactly the evidence external review needs.
//
// WHAT IT PINS, AND HOW EXACTLY
//
//	exec / run  — the FULL reply string, byte for byte. These are the only two verbs whose
//	              detail actually reaches a human: cmd/tether's execFailureMessage and
//	              runFailureMessage print the whole "<code>: <detail>". Everywhere else
//	              cmd/tether/error_hints.go's brokerErrorMessage DISCARDS the detail when the
//	              code has a hint, and every gate code has one — so a byte-golden on the
//	              others would be pinning a surface no operator can see.
//	the rest    — (code, detail) as decoded from that verb's own reply type, plus the audit
//	              multiset. Not byte-golden, deliberately.
//
// FOUR REPLY SHAPES, not three. The obvious ones are the single-string form (exec, run) and
// the (code, detail) form (expose, expose-rm, upgrade). The fourth is easy to miss and is
// load-bearing: replyKillFailed emits KillResp{Code: "rejected", Error: reason}, so for kill
// the Code field is ALWAYS the literal "rejected" and the real gate code is buried in Error.
// Any unification that "normalises" kill onto Code changes a wire value.
//
// SCOPE, stated honestly (testing-standards A3): this covers the six verbs B1 converts in
// commits 7-8 (exec, run, kill, expose, expose-rm, upgrade). ps / node.list / proxy.status
// are commit 9, which is first on the plan's budget-cut list (§8); they are NOT characterized
// here, and if commit 9 proceeds it must extend this table first. transfer and proxy-owner
// are DEFERRED by the plan and keep their existing coverage.

// ingressReply is the normalized view of a refusal, decoded per verb.
type ingressReply struct {
	code   string
	detail string
	raw    string // the full reply payload, for the byte-golden verbs
}

// ingressVerb binds one handler to its subject, its invocation and its reply decoder.
// The decoder is per-verb ON PURPOSE: collapsing them would hide exactly the divergence
// this file exists to record.
type ingressVerb struct {
	name    string
	subject func(sid, actor, nid string) string
	handle  func(b *Broker, nc *nats.Conn, msg *nats.Msg)
	decode  func(t *testing.T, data []byte) ingressReply
	// ownerGated verbs answer not_owner where the others answer not_a_member.
	ownerGated bool
	// byteGolden marks the verbs whose full reply string reaches a human.
	byteGolden bool
	// overrides records, per scenario name, where this verb genuinely DIVERGES from the
	// shared expectation. Every entry is a divergence the plan says must survive B1 — the
	// point of writing them down is that a refactor which "tidies" them away has to delete
	// an override and explain itself, rather than silently unify.
	overrides map[string]ingressExpect
}

// ingressExpect is a per-verb override of the shared scenario expectation.
type ingressExpect struct {
	code   string
	detail string   // same "" / "*" / exact convention as ingressScenario.wantDetail
	audit  []string // nil = this verb refuses SILENTLY in the audit stream
	why    string   // required: an override with no reason is just a green test
}

// splitCodeDetail mirrors cmd/tether/error_hints.go's runFailureMessage: the wire carries
// "<code>" or "<code>: <detail>" in ONE string and the CLI splits on the first colon.
func splitCodeDetail(s string) (code, detail string) {
	if i := strings.IndexByte(s, ':'); i >= 0 {
		return strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1:])
	}
	return s, ""
}

func ingressVerbs() []ingressVerb {
	decodeExec := func(t *testing.T, data []byte) ingressReply {
		t.Helper()
		var c proto.ExecChunk
		if err := json.Unmarshal(data, &c); err != nil {
			t.Fatalf("decode ExecChunk: %v (raw %q)", err, data)
		}
		if c.Kind != "error" {
			t.Fatalf("exec refusal must carry Kind=error, got %q (raw %q)", c.Kind, data)
		}
		code, detail := splitCodeDetail(c.Error)
		return ingressReply{code: code, detail: detail, raw: c.Error}
	}
	decodeRun := func(t *testing.T, data []byte) ingressReply {
		t.Helper()
		var c proto.RunChunk
		if err := json.Unmarshal(data, &c); err != nil {
			t.Fatalf("decode RunChunk: %v (raw %q)", err, data)
		}
		if c.Kind != "failed" {
			t.Fatalf("run refusal must carry Kind=failed, got %q (raw %q)", c.Kind, data)
		}
		code, detail := splitCodeDetail(c.Reason)
		return ingressReply{code: code, detail: detail, raw: c.Reason}
	}
	// kill is the fourth shape: Code is pinned to "rejected" and the gate code rides Error.
	decodeKill := func(t *testing.T, data []byte) ingressReply {
		t.Helper()
		var r proto.KillResp
		if err := json.Unmarshal(data, &r); err != nil {
			t.Fatalf("decode KillResp: %v (raw %q)", err, data)
		}
		if r.Code != "rejected" {
			t.Fatalf("kill refusal Code changed from the literal \"rejected\" to %q — that literal "+
				"is the wire, and the real gate code rides Error (raw %q)", r.Code, data)
		}
		code, detail := splitCodeDetail(r.Error)
		return ingressReply{code: code, detail: detail, raw: r.Error}
	}
	decodeExpose := func(t *testing.T, data []byte) ingressReply {
		t.Helper()
		var r proto.ExposeResp
		if err := json.Unmarshal(data, &r); err != nil {
			t.Fatalf("decode ExposeResp: %v (raw %q)", err, data)
		}
		return ingressReply{code: r.Code, detail: r.Error, raw: string(data)}
	}
	decodeExposeRm := func(t *testing.T, data []byte) ingressReply {
		t.Helper()
		var r proto.ExposeRmResp
		if err := json.Unmarshal(data, &r); err != nil {
			t.Fatalf("decode ExposeRmResp: %v (raw %q)", err, data)
		}
		return ingressReply{code: r.Code, detail: r.Error, raw: string(data)}
	}
	decodeUpgrade := func(t *testing.T, data []byte) ingressReply {
		t.Helper()
		var r proto.UpgradeResp
		if err := json.Unmarshal(data, &r); err != nil {
			t.Fatalf("decode UpgradeResp: %v (raw %q)", err, data)
		}
		return ingressReply{code: r.Code, detail: r.Error, raw: string(data)}
	}

	cmdSubject := func(verb string) func(sid, actor, nid string) string {
		return func(sid, actor, nid string) string { return proto.SubjCmdBy(sid, actor, nid, verb) }
	}

	return []ingressVerb{
		{
			name: "exec", subject: cmdSubject("exec"), byteGolden: true,
			handle: func(b *Broker, nc *nats.Conn, msg *nats.Msg) { b.handleExecReq(nc, msg) },
			decode: decodeExec,
		},
		{
			name: "run", subject: cmdSubject("run"), byteGolden: true,
			handle: func(b *Broker, nc *nats.Conn, msg *nats.Msg) { b.handleRunReq(nc, msg) },
			decode: decodeRun,
		},
		{
			name: "kill", subject: cmdSubject("kill"),
			handle: func(b *Broker, nc *nats.Conn, msg *nats.Msg) { b.handleKillReq(nc, msg) },
			decode: decodeKill,
			// kill runs the SAME five checks as run — it sits in the same file, twenty lines
			// below, and its doc comment says "Same pre-flight as run.req" — but it emits NO
			// audit.call on either node refusal, where run emits one. Nothing in the source
			// says whether that is a decision or an omission, and the two are indistinguishable
			// from the outside. Recorded as-is: making kill match run would be a behaviour
			// CHANGE (a new audit row per refusal), and making run match kill would delete
			// evidence. B1 must reproduce this asymmetry exactly; changing it is its own call.
			overrides: map[string]ingressExpect{
				"node_offline":   {code: "node_offline", detail: "*", audit: nil, why: "kill emits no audit.call on node refusal; run, twenty lines above, does"},
				"node_stale":     {code: "node_offline", detail: "*", audit: nil, why: "same"},
				"node_not_found": {code: "node_not_found", detail: "", audit: nil, why: "same"},
			},
		},
		{
			name: "expose", subject: cmdSubject("expose"),
			handle: func(b *Broker, nc *nats.Conn, msg *nats.Msg) { b.handleExposeReq(nc, msg) },
			decode: decodeExpose,
		},
		{
			name: "expose-rm", subject: cmdSubject("expose-rm"),
			handle: func(b *Broker, nc *nats.Conn, msg *nats.Msg) { b.handleExposeRmReq(nc, msg) },
			decode: decodeExposeRm,
			// expose-rm has NO node check at all: it discards the subject's nid
			// (`sid, actor, _, verb, ok := proto.ParseCmdBy(...)`) and resolves the target by
			// (sid, name) instead, because an expose can be removed after its agent has gone.
			// So all three node scenarios fall through the gate and fail at port.LookupByName
			// with not_found + the NAME as detail. B1 must give expose-rm needNodeOnline=false;
			// a spec that defaulted the node check ON would make `expose rm` stop working the
			// moment the agent went offline — which is precisely when an operator reaches for it.
			overrides: map[string]ingressExpect{
				"node_offline":   {code: "not_found", detail: "svc", audit: nil, why: "no node check; falls through to the by-name lookup"},
				"node_stale":     {code: "not_found", detail: "svc", audit: nil, why: "same"},
				"node_not_found": {code: "not_found", detail: "svc", audit: nil, why: "same"},
			},
		},
		{
			name: "upgrade", subject: cmdSubject("upgrade"), ownerGated: true,
			handle: func(b *Broker, nc *nats.Conn, msg *nats.Msg) { b.handleUpgradeReq(nc, msg) },
			decode: decodeUpgrade,
		},

		// --- the ctrl-family verbs (batch B follow-up, plan §15.2) -------------------------
		//
		// THREE subject families, not one. This is the divergence the plan's §0.1 grouping was
		// pointing at, and it is sharper than "9 hand-copied prologues" suggests:
		//
		//	exec/run/kill/expose/expose-rm/upgrade : ctrl `cmd.by` — proto.ParseCmdBy
		//	ps / node.list                        : `ctrl.by` + a hand-parsed leaf — ParseCtrlBy
		//	proxy.status                          : `ctrl.<sid>.proxy.*` — ParseCtrlProxy
		//
		// ParseCmdBy VALIDATES the actor token; ParseCtrlBy and ParseCtrlProxy do not. So a
		// malformed actor is subject_malformed in the first family and actor_invalid in the other
		// two — a SHARED parser would swap those two codes on half the verb set. That is why
		// admit keeps one subject resolver per family and shares only admitACL.
		//
		// None of these three take a nid, so none runs the node check.
		{
			name: "ps",
			subject: func(sid, actor, _ string) string {
				return proto.SubjCtrlPs(actor, sid)
			},
			handle: func(b *Broker, _ *nats.Conn, msg *nats.Msg) { b.handlePsReq(msg) },
			decode: func(t *testing.T, data []byte) ingressReply {
				t.Helper()
				var r proto.PsResp
				if err := json.Unmarshal(data, &r); err != nil {
					t.Fatalf("decode PsResp: %v (raw %q)", err, data)
				}
				return ingressReply{code: r.Code, detail: r.Error, raw: string(data)}
			},
			overrides: ctrlFamilyNodeOverrides("ps"),
		},
		{
			name: "node.list",
			subject: func(sid, actor, _ string) string {
				return proto.SubjCtrlNodeList(actor, sid)
			},
			handle: func(b *Broker, _ *nats.Conn, msg *nats.Msg) { b.handleNodeListReq(msg) },
			decode: func(t *testing.T, data []byte) ingressReply {
				t.Helper()
				var r proto.NodeListResp
				if err := json.Unmarshal(data, &r); err != nil {
					t.Fatalf("decode NodeListResp: %v (raw %q)", err, data)
				}
				return ingressReply{code: r.Code, detail: r.Error, raw: string(data)}
			},
			overrides: ctrlFamilyNodeOverrides("node.list"),
		},
		{
			name: "proxy.status",
			subject: func(sid, actor, _ string) string {
				return proto.SubjCtrlProxyStatus(actor, sid)
			},
			handle: func(b *Broker, _ *nats.Conn, msg *nats.Msg) { b.handleProxyStatus(msg) },
			decode: func(t *testing.T, data []byte) ingressReply {
				t.Helper()
				var r proto.ProxyStatusResp
				if err := json.Unmarshal(data, &r); err != nil {
					t.Fatalf("decode ProxyStatusResp: %v (raw %q)", err, data)
				}
				return ingressReply{code: r.Code, detail: r.Error, raw: string(data)}
			},
			overrides: ctrlFamilyNodeOverrides("proxy.status"),
		},
	}
}

// ctrlFamilyNodeOverrides records that the three ctrl-family verbs carry NO nid and therefore run
// no node check: all three node scenarios fall through the gate and the handler proceeds to its
// own work, answering OK (no refusal code) with whatever the session happens to hold.
//
// Written once rather than three times because the reason is identical, and because a per-verb copy
// is how the transfer family ended up with five subtly different comments saying the same thing.
func ctrlFamilyNodeOverrides(verb string) map[string]ingressExpect {
	why := verb + " carries no nid (its subject has no node segment), so no node check runs and the " +
		"node scenarios reach the handler's own work instead of a refusal"
	ok := ingressExpect{code: "", detail: "", audit: nil, why: why}
	return map[string]ingressExpect{
		"node_offline":   ok,
		"node_stale":     ok,
		"node_not_found": ok,
	}
}

// ingressScenario seeds one refusal condition and states the expected outcome.
type ingressScenario struct {
	name string
	// seed prepares the DB and returns the actor that will send the request.
	seed func(t *testing.T, b *Broker, owner, sid, nid string) (actor string)
	// wantCode for member-gated verbs; wantOwnerCode overrides it for owner-gated ones
	// (upgrade answers not_owner where exec answers not_a_member).
	wantCode      string
	wantOwnerCode string
	// wantDetail: "" = must be empty; "*" = must be non-empty (its exact text is a
	// SQLite / status string we do not want to pin); anything else = exact match.
	wantDetail string
	// wantAuditErrs is the audit.call error multiset this refusal emits. Empty means the
	// refusal is SILENT in the audit stream — which for several of these is the current
	// behaviour and is deliberately recorded, not endorsed.
	wantAuditErrs []string
	// ownerAuditErrs overrides wantAuditErrs for owner-gated verbs.
	ownerAuditErrs []string
	// rawGolden is the EXACT reply string for the byteGolden verbs (exec, run) — the whole
	// ExecChunk.Error / RunChunk.Reason, separator included.
	//
	// This field exists because the first version of this file declared `byteGolden`, set it on
	// exec and run, and then NEVER READ IT, while the header claimed those two verbs were pinned
	// "byte for byte". Internal review found it in five independent lanes. Asserting (code,
	// detail) after splitting on the colon does NOT pin the byte form: it cannot see a changed
	// separator, added surrounding text, or a swapped order — and exec/run are the only two verbs
	// whose full string a human ever reads (cmd/tether's execFailureMessage / runFailureMessage
	// print it whole; everywhere else brokerErrorMessage substitutes the hint).
	rawGolden string
}

func seedOwnedSession(t *testing.T, b *Broker, owner, sid, nid, nodeStatus string) {
	t.Helper()
	fp, err := auth.FingerprintFromActor(owner)
	if err != nil {
		t.Fatalf("fingerprint owner: %v", err)
	}
	if _, err := session.Create(b.cfg.DB, sid, sid, fp, "pin-hash", time.Now().UTC()); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := b.cfg.DB.Exec(
		`INSERT INTO nodes(sid,nid,status) VALUES(?,?,?)`, sid, nid, nodeStatus,
	); err != nil {
		t.Fatalf("seed node: %v", err)
	}
}

func ingressScenarios() []ingressScenario {
	return []ingressScenario{
		{
			name: "session_missing",
			seed: func(t *testing.T, b *Broker, owner, sid, nid string) string {
				// Nothing seeded at all: no session row.
				return owner
			},
			wantCode:  "session_not_found_or_deleting",
			rawGolden: "session_not_found_or_deleting",
		},
		{
			name: "session_deleting",
			seed: func(t *testing.T, b *Broker, owner, sid, nid string) string {
				seedOwnedSession(t, b, owner, sid, nid, "ONLINE")
				if err := session.Tombstone(b.cfg.DB, sid, time.Now().UTC()); err != nil {
					t.Fatalf("tombstone: %v", err)
				}
				return owner
			},
			wantCode:  "session_not_found_or_deleting",
			rawGolden: "session_not_found_or_deleting",
		},
		{
			name: "not_a_member",
			seed: func(t *testing.T, b *Broker, owner, sid, nid string) string {
				seedOwnedSession(t, b, owner, sid, nid, "ONLINE")
				return freshUserActor(t) // a valid identity that never joined
			},
			wantCode:      "not_a_member",
			wantOwnerCode: "not_owner",
			rawGolden:     "not_a_member",
			// upgrade emits an audit row on its owner refusal; the member-gated verbs do not.
			ownerAuditErrs: []string{"not_owner"},
		},
		{
			name: "node_offline",
			seed: func(t *testing.T, b *Broker, owner, sid, nid string) string {
				seedOwnedSession(t, b, owner, sid, nid, "OFFLINE")
				return owner
			},
			wantCode:       "node_offline",
			wantDetail:     "*",
			rawGolden:      "node_offline: status=OFFLINE",
			wantAuditErrs:  []string{"node_offline:OFFLINE"},
			ownerAuditErrs: []string{"node_offline:OFFLINE"},
		},
		{
			name: "node_stale",
			seed: func(t *testing.T, b *Broker, owner, sid, nid string) string {
				seedOwnedSession(t, b, owner, sid, nid, "STALE")
				return owner
			},
			wantCode:       "node_offline",
			wantDetail:     "*",
			rawGolden:      "node_offline: status=STALE",
			wantAuditErrs:  []string{"node_offline:STALE"},
			ownerAuditErrs: []string{"node_offline:STALE"},
		},
		{
			// store_error is the ninth gate code and was the only one this net did not cover:
			// plan §5 decision #1 was DEFERRED partly because "the change could not be shown to be
			// behaviour-preserving" without it. Closing the pool before the request is the cheapest
			// way to make every gate read fail deterministically.
			name: "store_error",
			seed: func(t *testing.T, b *Broker, owner, sid, nid string) string {
				seedOwnedSession(t, b, owner, sid, nid, "ONLINE")
				if err := b.cfg.DB.Close(); err != nil {
					t.Fatalf("close pool: %v", err)
				}
				return owner
			},
			wantCode: "store_error",
			// The SQLite text is NO LONGER on the wire — plan §5 decision #1, applied only AFTER
			// this scenario existed to measure it. §S4: 存储层错误文本可能泄露路径与结构; it can carry
			// the db path, table and constraint names to any session member.
			//
			// The diff that produced this line IS the evidence for the change: flipping
			// storeErrDenial to log-and-omit turned exactly these NINE subtests red — one per verb —
			// and left the other 72 recorded behaviours untouched. That is what "touches only what
			// it intended" looks like when it is measured instead of asserted.
			//
			// The exit code is unchanged (cmd/tether maps by CODE), and error_hints.go's hint for
			// store_error already read "check the broker log" — which is where the text now is.
			wantDetail: "",
		},
		{
			name: "node_not_found",
			seed: func(t *testing.T, b *Broker, owner, sid, nid string) string {
				fp, err := auth.FingerprintFromActor(owner)
				if err != nil {
					t.Fatalf("fingerprint: %v", err)
				}
				if _, err := session.Create(b.cfg.DB, sid, sid, fp, "pin-hash", time.Now().UTC()); err != nil {
					t.Fatalf("seed session: %v", err)
				}
				return owner // session exists, node row does not
			},
			wantCode:       "node_not_found",
			rawGolden:      "node_not_found",
			wantAuditErrs:  []string{"node_not_found"},
			ownerAuditErrs: []string{"node_not_found"},
		},
	}
}

// TestIngressRefusalSurface records, per verb and per refusal, the reply code, the reply
// detail and the audit.call multiset. One embedded NATS server serves the whole table:
// the handlers reply through msg.Respond, so a real conn is needed, but a server per case
// would add seconds to the package that is already e2e's longest pole.
func TestIngressRefusalSurface(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	const sid, nid = "lab", "lab-1"

	for _, verb := range ingressVerbs() {
		for _, sc := range ingressScenarios() {
			t.Run(verb.name+"/"+sc.name, func(t *testing.T) {
				owner := freshUserActor(t)
				b := &Broker{cfg: Config{
					DB:                      openDB(t),
					Logger:                  silentLogger(),
					Now:                     time.Now,
					PublicHost:              "broker.example.test",
					PortBandLow:             14000,
					PortBandHigh:            14010,
					ExposeForwardTimeoutDur: time.Second,
				}}
				b.nc.Store(nc)
				actor := sc.seed(t, b, owner, sid, nid)

				// The tap fires on the NATS callback goroutine while the assertions run on the
				// test goroutine, so the slice needs a lock — and, more importantly, a
				// HAPPENS-BEFORE. Every converted handler REPLIES FIRST and emits its audit row
				// afterwards, so nc.Request returning proves nothing about the audit. The first
				// version of this test read auditErrs straight after the reply; it passed
				// standalone and failed under `make e2e-parallel` with both a data race and an
				// empty multiset. That is docs/testing-standards.md §T3 exactly: it assumed a
				// state observed in one place still held in the next step.
				var auditMu sync.Mutex
				var auditErrs []string
				auditTapForTest = func(subject string, payload []byte) {
					if subject != proto.SubjAuditCall(sid) {
						return
					}
					var rec schema.AuditCall
					if err := json.Unmarshal(payload, &rec); err != nil {
						t.Errorf("audit.call payload is not AuditCall: %v", err)
						return
					}
					if !rec.OK {
						auditMu.Lock()
						auditErrs = append(auditErrs, rec.Error)
						auditMu.Unlock()
					}
				}
				t.Cleanup(func() { auditTapForTest = nil })

				subj := verb.subject(sid, actor, nid)
				// handled closes after the handler RETURNS, which is the only point at which
				// every side effect it is going to have has happened. Signalling completion
				// rather than sleeping keeps this deterministic under CPU starvation, which
				// docs/testing-standards.md §T4 records as a routine cause of false failures here.
				handled := make(chan struct{})
				sub, err := nc.Subscribe(subj, func(msg *nats.Msg) {
					defer close(handled)
					verb.handle(b, nc, msg)
				})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = sub.Unsubscribe() }()
				if err := nc.Flush(); err != nil {
					t.Fatal(err)
				}

				reply, err := nc.Request(subj, ingressRequestBody(t, verb.name), 5*time.Second)
				if err != nil {
					t.Fatalf("%s/%s: no reply (%v) — a refusal that answers NOTHING makes ctl hang "+
						"until its own timeout, which is a different failure than a clean refusal",
						verb.name, sc.name, err)
				}
				select {
				case <-handled:
				case <-time.After(5 * time.Second):
					t.Fatalf("%s/%s: the handler replied but never returned", verb.name, sc.name)
				}
				got := verb.decode(t, reply.Data)

				wantCode := sc.wantCode
				if verb.ownerGated && sc.wantOwnerCode != "" {
					wantCode = sc.wantOwnerCode
				}
				wantDetail := sc.wantDetail
				wantAudit := sc.wantAuditErrs
				if verb.ownerGated && sc.ownerAuditErrs != nil {
					wantAudit = sc.ownerAuditErrs
				}
				ov, hasOverride := verb.overrides[sc.name]
				if hasOverride {
					if ov.why == "" {
						t.Fatalf("override for %s/%s has no `why` — an override without a stated "+
							"reason is indistinguishable from a test bent to fit the code", verb.name, sc.name)
					}
					wantCode, wantDetail, wantAudit = ov.code, ov.detail, ov.audit
				}

				if got.code != wantCode {
					t.Errorf("code: got %q want %q (raw %q)", got.code, wantCode, got.raw)
				}
				switch wantDetail {
				case "":
					if got.detail != "" {
						t.Errorf("detail: got %q, want empty (raw %q)", got.detail, got.raw)
					}
				case "*":
					if got.detail == "" {
						t.Errorf("detail: got empty, want non-empty (raw %q)", got.raw)
					}
				default:
					if got.detail != wantDetail {
						t.Errorf("detail: got %q want %q", got.detail, wantDetail)
					}
				}
				// THE BYTE GOLDEN. Only exec and run get one, because they are the only verbs
				// whose whole string a human reads. An override means this verb diverges from the
				// shared scenario, so the shared golden does not apply to it.
				if verb.byteGolden && sc.rawGolden != "" && !hasOverride {
					if got.raw != sc.rawGolden {
						t.Errorf("BYTE GOLDEN: %s replied %q, want %q.\n"+
							"cmd/tether's %sFailureMessage prints this string WHOLE to the operator, so a "+
							"changed separator, an added prefix or a reordering is directly visible to them "+
							"— and none of that is caught by the (code, detail) assertions above, which "+
							"split on the colon first.", verb.name, got.raw, sc.rawGolden, verb.name)
					}
				}

				auditMu.Lock()
				gotAudit := append([]string(nil), auditErrs...)
				auditMu.Unlock()
				assertAuditMultiset(t, gotAudit, wantAudit)
			})
		}
	}
}

// TestIngressPreDBRefusals records the two refusals that fire BEFORE any DB access, and where
// the reply detail diverges the most across verbs. They are recorded separately from the table
// above because each needs a DIFFERENT subject rather than different DB state.
//
// The divergence being pinned:
//
//	subject_malformed — exec ECHOES the offending subject; run, kill, expose, expose-rm and
//	                    upgrade send the bare code. Six sites, two behaviours, no stated reason.
//	actor_invalid     — all six carry the nkey parse error as detail. This one IS uniform, and
//	                    recording it is what makes the exec outlier above meaningful rather
//	                    than "well, they all differ anyway".
func TestIngressPreDBRefusals(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	const sid, nid = "lab", "lab-1"
	// exec is the only verb that echoes the subject on a parse failure.
	echoesSubject := map[string]bool{"exec": true}

	for _, verb := range ingressVerbs() {
		t.Run(verb.name+"/subject_malformed", func(t *testing.T) {
			b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger(), Now: time.Now}}
			b.nc.Store(nc)
			// A well-formed cmd subject carrying the WRONG verb token: parsing succeeds but the
			// handler's `verb != "<its own>"` check rejects it.
			subj := proto.SubjCmdBy(sid, freshUserActor(t), nid, "not-"+verb.name)
			sub, err := nc.Subscribe(subj, func(msg *nats.Msg) { verb.handle(b, nc, msg) })
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			if err := nc.Flush(); err != nil {
				t.Fatal(err)
			}
			reply, err := nc.Request(subj, ingressRequestBody(t, verb.name), 2*time.Second)
			if err != nil {
				t.Fatalf("no reply: %v", err)
			}
			got := verb.decode(t, reply.Data)
			if got.code != "subject_malformed" {
				t.Fatalf("code: got %q want subject_malformed (raw %q)", got.code, got.raw)
			}
			if echoesSubject[verb.name] {
				if got.detail != subj {
					t.Errorf("%s must echo the offending subject as detail: got %q want %q",
						verb.name, got.detail, subj)
				}
			} else if got.detail != "" {
				t.Errorf("%s must NOT echo a detail on subject_malformed (only exec does): got %q",
					verb.name, got.detail)
			}
		})

		t.Run(verb.name+"/actor_invalid", func(t *testing.T) {
			b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger(), Now: time.Now}}
			b.nc.Store(nc)
			// actor_invalid sits in a NARROW window: proto.ParseCmdBy validates the actor token
			// itself, so a plainly-wrong string ("NOTANKEY") is rejected earlier as
			// subject_malformed. The code is only reachable for a token that PASSES that
			// syntactic check and then fails nkey verification — i.e. a well-formed actor whose
			// checksum is wrong. Recording the narrowness matters: a refactor that "simplifies"
			// the two checks into one would delete a code that looks live but is nearly
			// unreachable, and nobody would notice from the outside.
			subj := verb.subject(sid, corruptActor(t, freshUserActor(t)), nid)
			sub, err := nc.Subscribe(subj, func(msg *nats.Msg) { verb.handle(b, nc, msg) })
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			if err := nc.Flush(); err != nil {
				t.Fatal(err)
			}
			reply, err := nc.Request(subj, ingressRequestBody(t, verb.name), 2*time.Second)
			if err != nil {
				t.Fatalf("no reply: %v", err)
			}
			got := verb.decode(t, reply.Data)
			if got.code != "actor_invalid" {
				t.Fatalf("code: got %q want actor_invalid (raw %q)", got.code, got.raw)
			}
			if got.detail == "" {
				t.Errorf("%s dropped the nkey parse detail — all six verbs carry it today, and "+
					"it is the only thing telling an operator WHY their identity was rejected", verb.name)
			}
		})
	}
}

// corruptActor flips one character of a valid actor nkey so the string still passes
// proto.ParseCmdBy's token validation but fails the nkey checksum in
// auth.FingerprintFromActor. It fails the test rather than returning a string that would
// silently be rejected one stage earlier.
func corruptActor(t *testing.T, actor string) string {
	t.Helper()
	if len(actor) < 10 {
		t.Fatalf("actor %q is too short to corrupt", actor)
	}
	b := []byte(actor)
	// Stay inside the base32 alphabet so only the checksum breaks.
	for _, c := range []byte{'A', 'B', 'C'} {
		if b[5] != c {
			b[5] = c
			break
		}
	}
	corrupted := string(b)
	if _, err := auth.FingerprintFromActor(corrupted); err == nil {
		t.Fatalf("corrupting %q did not actually invalidate it — the probe is testing nothing", actor)
	}
	return corrupted
}

// ingressRequestBody returns a body that is VALID for the verb, so the refusal under test is
// the gate's and not a json_parse / field-validation refusal further down.
func ingressRequestBody(t *testing.T, verb string) []byte {
	t.Helper()
	switch verb {
	case "exec":
		return mustJSON(proto.ExecReq{Argv: []string{"/bin/true"}})
	case "run":
		return mustJSON(proto.RunReq{Argv: []string{"/bin/true"}})
	case "kill":
		return mustJSON(proto.KillReq{PID: "p1"})
	case "expose":
		return mustJSON(proto.ExposeReq{Name: "svc", LocalPort: 9001})
	case "expose-rm":
		return mustJSON(proto.ExposeRmReq{Name: "svc"})
	case "upgrade":
		return mustJSON(proto.UpgradeReq{
			URL:    "https://example.test/tether",
			SHA256: strings.Repeat("a", 64),
		})
	// The three ctrl-family verbs are all reads with optional bodies. `{}` is what the CLI sends
	// for ps/node.list; proxy.status accepts an empty body as "single-broker view" (its handler
	// does `_ = json.Unmarshal(...)` deliberately).
	case "ps", "node.list", "proxy.status":
		return []byte(`{}`)
	default:
		t.Fatalf("no request body defined for verb %q", verb)
		return nil
	}
}

func assertAuditMultiset(t *testing.T, got, want []string) {
	t.Helper()
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)
	if len(g) != len(w) {
		t.Errorf("audit.call refusal multiset: got %v want %v", g, w)
		return
	}
	for i := range g {
		if g[i] != w[i] {
			t.Errorf("audit.call refusal multiset: got %v want %v", g, w)
			return
		}
	}
}

// TestIngressCharacterizationCoversEveryConvertedVerb is the completeness half. B1's commits
// 7-8 convert exactly these six verbs; if the table stops covering one, the "behaviour
// preserved" claim silently narrows.
func TestIngressCharacterizationCoversEveryConvertedVerb(t *testing.T) {
	want := map[string]bool{
		"exec": true, "run": true, "kill": true,
		"expose": true, "expose-rm": true, "upgrade": true,
		// The three ctrl-family verbs, added when plan §15.2's deferral was completed.
		"ps": true, "node.list": true, "proxy.status": true,
	}
	got := map[string]bool{}
	for _, v := range ingressVerbs() {
		got[v.name] = true
	}
	for name := range want {
		if !got[name] {
			t.Errorf("verb %q is converted by B1 but is not characterized — its refusal surface "+
				"would change with no diff to review", name)
		}
	}
	for name := range got {
		if !want[name] {
			t.Errorf("verb %q is characterized but not in B1's converted set — either it joined "+
				"the conversion (update this list) or the table drifted", name)
		}
	}
	if n := len(ingressScenarios()); n < 6 {
		t.Errorf("only %d refusal scenarios — the table lost cases; the six recorded are "+
			"session_missing, session_deleting, not_a_member, node_offline, node_stale, node_not_found", n)
	}
}

// TestIngressAuditTapSelfCheck proves the audit assertion is not vacuous. Every scenario that
// expects an EMPTY multiset would pass identically if the tap were never installed, if the
// subject filter never matched, or if AuditCall failed to decode — three silent ways for this
// file to assert nothing at all about the audit surface.
func TestIngressAuditTapSelfCheck(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	const sid = "lab"
	var seen []string
	auditTapForTest = func(subject string, payload []byte) {
		if subject != proto.SubjAuditCall(sid) {
			return
		}
		var rec schema.AuditCall
		if err := json.Unmarshal(payload, &rec); err != nil {
			t.Errorf("decode: %v", err)
			return
		}
		if !rec.OK {
			seen = append(seen, rec.Error)
		}
	}
	t.Cleanup(func() { auditTapForTest = nil })

	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger(), Now: time.Now}}
	b.nc.Store(nc)
	b.pubAuditCall(sid, "fp", "actor", "exec", "lab-1", false, "synthetic_refusal", "", nil)
	b.pubAuditCall(sid, "fp", "actor", "exec", "lab-1", true, "", "", nil)
	b.pubAuditCall("other-session", "fp", "actor", "exec", "lab-1", false, "wrong_subject", "", nil)

	if len(seen) != 1 || seen[0] != "synthetic_refusal" {
		t.Fatalf("audit tap captured %v, want exactly [synthetic_refusal] — the tap must fire, "+
			"must filter by subject, and must ignore OK rows; an empty-multiset assertion is "+
			"meaningless unless all three hold", seen)
	}
}

// TestIngressReplyDecodersSelfCheck proves each decoder reads the field the handler actually
// writes. A decoder reading the wrong field would return ("", "") for every case, and every
// wantCode assertion would fail loudly — EXCEPT that the kill decoder's "rejected" assertion
// and the exec/run Kind assertions are themselves the thing under test here.
func TestIngressReplyDecodersSelfCheck(t *testing.T) {
	cases := []struct {
		verb    string
		payload []byte
		want    ingressReply
	}{
		{"exec", mustJSON(proto.ExecChunk{Kind: "error", Error: "not_a_member"}),
			ingressReply{code: "not_a_member", detail: "", raw: "not_a_member"}},
		{"exec", mustJSON(proto.ExecChunk{Kind: "error", Error: "node_offline: status=STALE"}),
			ingressReply{code: "node_offline", detail: "status=STALE", raw: "node_offline: status=STALE"}},
		{"run", mustJSON(proto.RunChunk{Kind: "failed", Reason: "store_error: disk"}),
			ingressReply{code: "store_error", detail: "disk", raw: "store_error: disk"}},
		{"kill", mustJSON(proto.KillResp{Code: "rejected", Error: "not_a_member"}),
			ingressReply{code: "not_a_member", detail: "", raw: "not_a_member"}},
		{"expose", mustJSON(proto.ExposeResp{Code: "not_a_member"}),
			ingressReply{code: "not_a_member", detail: ""}},
		{"upgrade", mustJSON(proto.UpgradeResp{Code: "not_owner"}),
			ingressReply{code: "not_owner", detail: ""}},
	}
	byName := map[string]ingressVerb{}
	for _, v := range ingressVerbs() {
		byName[v.name] = v
	}
	for i, c := range cases {
		v, ok := byName[c.verb]
		if !ok {
			t.Fatalf("case %d: no verb %q", i, c.verb)
		}
		got := v.decode(t, c.payload)
		if got.code != c.want.code || got.detail != c.want.detail {
			t.Errorf("case %d (%s): decoded (%q,%q), want (%q,%q)",
				i, c.verb, got.code, got.detail, c.want.code, c.want.detail)
		}
		if c.want.raw != "" && got.raw != c.want.raw {
			t.Errorf("case %d (%s): raw %q, want %q", i, c.verb, got.raw, c.want.raw)
		}
	}
	if _, ok := byName["kill"]; !ok {
		t.Fatal("kill must stay in the table: it is the only verb whose Code field is a fixed " +
			"literal, and that shape is the easiest one for a refactor to erase")
	}
}

var _ = fmt.Sprintf // keep fmt imported if the table above stops using it
