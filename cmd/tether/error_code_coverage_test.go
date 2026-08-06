package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Batch-A A1: the wire error-code coverage gate.
//
// WHAT THIS GUARDS
//
// A broker/agent reply code that no table in error_hints.go knows falls through
// brokerCodeExitClass to exitInternal (70) — and docs/usage.md §9.13 tells
// automation to treat 70 as RETRYABLE. So an unclassified terminal failure
// (too_large, self_path, verb_mismatch...) reads to a monitor as "retry me
// forever". This gate makes adding an unclassified code a test failure.
//
// WHY THE SCANNER LOOKS THE WAY IT DOES (read before "simplifying" it)
//
// Codes are not emitted in one syntactic shape. Measured forms in this repo:
//
//	 1. proto.XResp{Code: "literal"}                      — a KeyValueExpr
//	 2. proto.XResp{Code: proto.CodeFoo}                   — KeyValueExpr, const
//	 3. b.replyExposeErr(msg, "port_taken", …)             — a helper argument
//	 4. return codeXferBrokerRestarting, …                 — a returned const
//	 5. return "too_many_in_flight"                        — a returned literal
//	 6. return fmt.Errorf("try_again")                     — wrapped in an error
//	 7. proto.ClusterGrowResp{Code: ar.Code}               — pass-through
//	 8. RunChunk{Reason: "pty_alloc_failed"}               — a DIFFERENT field
//
// A scanner that only understands form 1 sees well under half of them. That
// matters more than it sounds: a gate which claims to cover "every code" while
// silently missing most of them is worse than no gate, because reviewers stop
// looking. (This repo already had one such false promise — proto.RehomeDirective's
// doc claimed "a guard test asserts it has no live publisher"; no such test
// existed.)
//
// Forms 4-6 cannot be recovered by syntax alone: a code can travel through any
// chain of return values, so enumerating them exactly is undecidable without
// full data-flow analysis. This scanner therefore covers forms 1-3 and 8
// EXACTLY, and is HONEST about the rest: form 7 (pass-through) and any Code:
// whose value is a non-constant expression are reported as unresolved and must
// be listed in unresolvedCodeSites with a reason. It does not pretend to be
// complete — see TestErrorCodeScannerDeclaresItsLimits.
//
// The self-check (TestErrorCodeCoverageSelfCheck) synthesises one sample per
// supported form. Without it a scanner that silently degenerates to matching
// nothing would report a perfectly green "0 unclassified codes".

// codeCarryingHelpers maps a helper function name to the 0-based index of the
// parameter that becomes a reply Code. Derived mechanically: a function whose
// body assigns one of its own parameters to a `Code:` field is code-carrying.
// TestCodeCarryingHelperListIsComplete re-derives this from source and fails if
// a new helper appears, so the list cannot rot silently.
var codeCarryingHelpers = map[string]int{
	"replyErr":            1,
	"replyExposeErr":      1,
	"replyExposeRmErr":    1,
	"replyPushErr":        1,
	"replyPullErr":        1,
	"replyCommitErr":      1,
	"replyUpgradeErr":     1,
	"fsRefuse":            1,
	"pathErr":             0,
	"cleanupEntry":        1,
	"pubTransferEvFailed": 4,
	"pubPtyFailed":        2,
	"replyRunFailed":      1,
}

// unclassifiedCodeAllowlist lists emitted codes that deliberately have no exit
// class. EVERY entry needs a reason — an allowlist without reasons is just a
// slower way of having no gate. TestAllowlistEntriesStillHaveEmitters fails if
// an entry stops being emitted, so this cannot become a graveyard.
//
// REASONS NAME SYMBOLS, NOT LINE NUMBERS. origin: p-b2 internal review m3, which found FOUR of these
// prose references already pointing at the wrong line — including one the plan had explicitly registered
// as needing a sweep, whose site had moved two commits before this map was last touched. That is the
// same rot the site KEYS above were re-keyed to escape, in the same file, arrived at from the other
// direction: a reader who follows a stale line lands on unrelated code and either deletes a live
// exemption as expired or re-derives an argument that was already settled. A function name survives
// every edit above it, and `grep` finds it.
var unclassifiedCodeAllowlist = map[string]string{
	"agent_rejected": "NOT a terminal code: it is the documented PREFIX of " +
		"`\"agent_rejected:\" + agentResp.Code`, surfaced here because the scanner now reads " +
		"literal prefixes out of concatenations (form 9). cmd/tether's brokerErrorMessage " +
		"strips it before the class lookup — pinned by TestBrokerErrorMessageStripsAgentRejectedPrefix " +
		"and by the agent_rejected: rows of TestBrokerErrorMessageExitClass — so the agent's own code is what " +
		"actually classifies, and that code is scanned on the agent side. " +
		"Giving the prefix its own exit class would shadow every underlying code with one class.",
	"bucket_create_failed": "the deliberately-UNCLASSIFIED remainder of the tier-B split: " +
		"jetstream_not_ready (75) and tier_b_store_too_small (64) are its classified halves. " +
		"Documented at the tier_b_store_too_small entry of brokerCodeExitClasses in error_hints.go; " +
		"70 is the correct answer for 'we do not know'.",
	"home_broker_restart": "audit-only: emitted into schema.AuditTransfer by " +
		"(*Broker).finalizeStrandedXfers in internal/broker/xfer_inflight.go, never onto a ctl reply, " +
		"so no exit class applies.",

	// h1 C3: the five proto.ProcEventAck codes. Consumed EXCLUSIVELY by the
	// agent's proc-event courier (internal/agent/proc_delivery.go — it reads
	// ack.OK, and OK carries the whole retry/stop decision); no ctl command
	// ever receives them, so no exit class applies. The OK bit, not the code,
	// is the machine contract — the code is operator-log vocabulary.
	"recorded":         "ProcEventAck code, agent-courier-only (see the h1 C3 block note above).",
	"already_recorded": "ProcEventAck code, agent-courier-only (see the h1 C3 block note above).",
	"already_exited":   "ProcEventAck code, agent-courier-only (see the h1 C3 block note above).",
	"unknown_pid":      "ProcEventAck code, agent-courier-only (see the h1 C3 block note above).",
	"node_missing":     "ProcEventAck code, agent-courier-only (see the h1 C3 block note above).",

	// The three below are CATCH-ALL branches: both a permanent cause (disk full,
	// quota) and a transient one (momentary I/O pressure) land here, and the
	// code carries no way to tell them apart. Guessing 75 would tell a monitor
	// to retry a full disk forever — the exact failure mode A1 exists to remove
	// — and guessing 64 would make a recoverable blip terminal. 70 is the
	// honest answer, and it is the same call error_hints.go already made for
	// bucket_create_failed. Split them into classified halves (as G67 did with
	// jetstream_not_ready / tier_b_store_too_small) before classifying.
	"alloc_failed": "catch-all `case err != nil` of public-port allocation in " +
		"(*Broker).handleExposeReq; covers both DB faults and exhaustion-shaped races with no way to " +
		"distinguish them.",
	"io_error": "catch-all I/O failure of the agent's transfer path ((*Agent).handlePushForwarded and " +
		"its finalize calls); a full disk and a momentary EIO are indistinguishable here. Retrying a " +
		"full disk is exactly what A1 removes.",
	"object_put_failed": "catch-all JetStream object-store Put failure in (*Agent).handlePullForwarded. " +
		"Its classified halves already exist (jetstream_not_ready=75, tier_b_store_too_small=64); this " +
		"is the remainder.",

	// pty_alloc_failed and download_failed used to live here. External review M2 put them here because
	// A1 had classified both 75 on the strength of their most common cause while each emitter funnelled
	// outcomes with OPPOSITE remedies into one code — telling automation to retry a missing /dev/ptmx
	// and a 404 forever. 70 (unclassified) was the honest answer while that was true, and this entry
	// named the fix: "splitting them at the emitter (download_http_status / download_too_large /
	// pty_unavailable / attach_subscribe_failed) adds new wire values, which is its own increment."
	//
	// origin: line-2 §12 Y2 — that increment. All four codes now exist and each carries the retry
	// semantics its own cause deserves:
	//
	//   pty_unavailable          64  no /dev/ptmx or a container ban — a host property
	//   pty_alloc_failed         75  a resource limit: fd (EMFILE/ENFILE) or the pty count (ENOSPC)
	//   attach_subscribe_failed  75  the attach SubscribeSync failed; the PTY was fine
	//   download_http_status     64  non-2xx mirror — the operator's URL is wrong
	//   download_too_large       64  over the ceiling — same size on every retry
	//   download_failed          75  transport/read failure only, which really does clear
	//
	// This table was WRONG in both places the increment later changed, and the closure verification (M17)
	// caught it: it said pty_unavailable was 69 after error_hints.go had moved it to 64 (69 sits in
	// usage.md's RETRYABLE class while this code's own hint says retrying will not help), and it said
	// pty_alloc_failed was "EMFILE/ENFILE only" after ptyTransientErrnos grew to five errnos — ENOSPC
	// being the important one, since devpts exhaustion is what /dev/ptmx actually returns when the pty
	// limit is hit.
	//
	// A published table inside the gate file that contradicts the gate's own subject is the defect this
	// whole increment is about. It is prose, so nothing checks it; the number that IS checked is
	// brokerCodeExitClasses itself, and TestCauseSplitCodesHaveTriggerTests below is what ties each of
	// these codes to a test of its trigger.
	//
	// So the exemption is gone rather than reworded: there is no longer a code here that mixes a
	// retryable cause with a terminal one.
}

// unresolvedCodeSites lists the exact SITES whose code the scanner cannot
// resolve statically. Each needs a reason.
//
// THE KEY IS file:FUNCTION#ordinal, AND IT USED TO BE file:line
// ---------------------------------------------------------------
// External review (and internal M3): these were originally keyed by FILE, so one exemption
// blanket-covered every unresolved site in that file — and the files involved are the hottest reply
// paths in broker and agent. That was fixed by keying on file:line, which was site-scoped and correct.
//
// It also drifted ELEVEN times. Every one of the eleven had the same cause: someone added a comment or a
// constant ABOVE a site, and all the keys below it went stale at once. The re-key that follows is
// mechanical, unreviewable, and — this is the part that matters — INDISTINGUISHABLE IN A DIFF from
// someone silencing a genuinely new unresolved site. A gate whose maintenance looks identical to its
// subversion is a gate that will eventually be subverted by accident.
//
// FUNCTION is RECEIVER-QUALIFIED — `(*Broker).handleRunReq`, not `handleRunReq`. The ordinal is the
// site's 1-based index among the UNRESOLVED sites of that one function, counted in source order (see
// nextSite in scanTree). That choice is deliberate on three axes:
//
//	site-scoped   #ordinal means one entry covers one site. A function-wide key would be the old
//	              file-wide defect at a smaller scale, and it would bite immediately:
//	              clusterstatus.go's HandleCluster holds TEN unresolved sites.
//	drift-proof   nothing above the function can move it. That immunises the exact cause of all eleven
//	              historical drifts.
//	honest        it DOES go stale when someone adds or removes an unresolved site inside that same
//	              function — and that is precisely when the surrounding exemptions deserve re-reading,
//	              so the remaining churn is the useful kind rather than the mechanical kind.
//
// ONE EDIT SHAPE SLIPS THROUGH, and it is worth knowing about (internal review m9). "Goes stale when a
// site is added or removed" holds for an add OR a remove — not for an add AND a remove that CANCEL. Make
// one site in a function resolvable (turn a variable into a literal) and add a new unresolved one below
// it in the same function, and the count is unchanged, every key still names a live site, and all three
// gates stay green while each reason now describes a different physical site than it was written for.
// That is an ordinary refactor shape, not a contrived one. No key that is not a full site fingerprint
// can catch it; what catches it is reading the reasons when you touch a function that has them, which
// is what the ordinal makes cheap by keeping the churn down to functions you actually edited.
//
// The receiver is the part internal review had to add (M1/M2). Without it the key was NOT INJECTIVE and
// the failure mode was the one thing worse than churn — SILENT ABSORPTION. Two shapes, both reproduced
// on the real tree, both leaving all three gates green while a brand-new unresolved site inherited an
// existing exemption:
//
//	a second method of the same name on a different receiver in the same file restarted the ordinal at
//	#1 and landed inside the ten HandleCluster entries below;
//
//	the counter was also zeroed on every FuncDecl EXIT, so the <file-scope> bucket restarted after every
//	func — two package-level sites separated by any function both keyed <file-scope>#1, with no name
//	collision required at all.
//
// A quieter gate is a worse gate than a noisier one. file:line at least went RED when it rotted; this
// key must be unique BY CONSTRUCTION, which is why the receiver is in it and the ordinal counter is
// per-file and never reset. TestUnresolvedSiteKeysAreInjective is the guard.
//
// Only unresolved sites are counted, so adding a resolvable literal to the same function changes
// nothing. TestExternalReviewUnresolvedCodeExemptionsAreSiteScopedAndLive rejects a key without an
// ordinal, and both live-checks below still reject a key that names no unresolved site.
var unresolvedCodeSites = map[string]string{
	// Agent run reasons are finite locals selected immediately above each site.
	"internal/agent/run.go:(*Agent).handleRunForwarded#1":                                  "Reason is remoteFSFailReason(ferr); its literal outcomes are scanned at their definitions.",
	"internal/agent/run.go:(*Agent).handleRunForwarded#2":                                  "pubPtyFailed forwards the same remoteFSFailReason value as #1 in this function.",
	"internal/agent/run.go:(*Agent).handleRunForwarded#3":                                  "Reason is exec_failed or remoteFSFailReason(startErr), selected immediately above.",
	"internal/agent/run.go:(*Agent).handleRunForwarded#4":                                  "pubPtyFailed forwards the same finite reason value as #3 in this function.",
	"internal/agent/transfer.go:(*Agent).handlePushForwarded#1":                            "code is io_error or PathValidationError.Code; both origins are scanned.",
	"internal/agent/transfer.go:(*Agent).handlePushForwarded#2":                            "audit emission forwards the same code as #1 in this function.",
	"internal/agent/transfer.go:(*Agent).handlePushTierA#1":                                "finalize's code parameter is supplied by literal/PathValidationError call sites in this function.",
	"internal/agent/transfer.go:(*Agent).handlePushTierA#2":                                "audit emission forwards finalize's same code parameter.",
	"internal/agent/transfer.go:(*Agent).handlePushCommitForwarded#1":                      "PathValidationError.Code is defined by the path validator's finite code set.",
	"internal/agent/transfer.go:(*Agent).handlePullForwarded#1":                            "transfer failure code is selected by the local state machine before this reply.",
	"internal/agent/transfer.go:(*Agent).handlePullForwarded#2":                            "transfer failure audit forwards the same locally selected code.",
	"internal/broker/cluster_grow_trigger.go:(*Broker).handleGrowTrigger#1":                "verbatim adminsock response code; adminsock has its own registry.",
	"internal/broker/cluster_grow_trigger.go:(*Broker).handleGrowTrigger#2":                "verbatim adminsock response code; adminsock has its own registry.",
	"internal/broker/cluster_grow_trigger.go:(*Broker).handleGrowTrigger#3":                "verbatim adminsock response code; adminsock has its own registry.",
	"internal/broker/cluster_upgrade_trigger.go:(*Broker).handleUpgradeTrigger#1":          "verbatim adminsock response code; adminsock has its own registry.",
	"internal/broker/cluster_upgrade_trigger.go:(*Broker).handleUpgradeTrigger#2":          "verbatim adminsock response code; adminsock has its own registry.",
	"internal/broker/cluster_upgrade_trigger.go:(*Broker).handleUpgradeTrigger#3":          "codeDataplaneNotConverged is a local alias of the classified proto code.",
	"internal/broker/cluster_manifest.go:(*clusterAdminBackend).handleSeedsPublish#1":      "clusterCodeFor returns the typed adminsock error namespace.",
	"internal/broker/clusterstatus.go:(*clusterAdminBackend).HandleCluster#1":              "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:(*clusterAdminBackend).HandleCluster#2":              "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:(*clusterAdminBackend).HandleCluster#3":              "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:(*clusterAdminBackend).HandleCluster#4":              "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:(*clusterAdminBackend).HandleCluster#5":              "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:(*clusterAdminBackend).HandleCluster#6":              "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:(*clusterAdminBackend).HandleCluster#7":              "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:(*clusterAdminBackend).HandleCluster#8":              "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:(*clusterAdminBackend).HandleCluster#9":              "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:(*clusterAdminBackend).HandleCluster#10":             "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:(*clusterAdminBackend).handleDrain#1":                "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:(*clusterAdminBackend).handleAdd#1":                  "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/run.go:(*Broker).handleRunReq#1":                                      "admit() denial code; the eight-code set is pinned by internal/broker.TestAdmitRefusalCodeSet.",
	"internal/broker/run.go:(*Broker).handleRunReq#2":                                      "admit() denial code; the eight-code set is pinned by internal/broker.TestAdmitRefusalCodeSet.",
	"internal/broker/expose.go:(*Broker).handleExposeReq#1":                                "admit() denial code; the eight-code set is pinned by internal/broker.TestAdmitRefusalCodeSet.",
	"internal/broker/expose.go:(*Broker).handleExposeRmReq#1":                              "admit() denial code; the eight-code set is pinned by internal/broker.TestAdmitRefusalCodeSet.",
	"internal/broker/expose.go:(*Broker).handleExposeRmReq#2":                              "admit() denial code; the eight-code set is pinned by internal/broker.TestAdmitRefusalCodeSet.",
	"internal/broker/upgrade.go:(*Broker).handleUpgradeReq#1":                              "admit() denial code; the eight-code set is pinned by internal/broker.TestAdmitRefusalCodeSet.",
	"internal/broker/exec.go:(*Broker).handleNodeListReq#1":                                "admitCtrl() denial code (node.list); same closed set, pinned by internal/broker.TestAdmitRefusalCodeSet.",
	"internal/broker/exec.go:(*Broker).handlePsReq#1":                                      "admitCtrl() denial code (ps); same closed set, pinned by internal/broker.TestAdmitRefusalCodeSet.",
	"internal/broker/force_single_online.go:(*clusterAdminBackend).handleForceSingleArm#1": "force-single refusal code is a typed adminsock constant returned by fsArmVerdict.",
	"internal/broker/proxy_cluster_wire.go:(*Broker).handleProxySubCreateCluster#1":        "proxyDegradedCode returns the finite proxy cluster status code set.",
	"internal/broker/topology_reconcile.go:(*Broker).reconcileTopologyOnce#1":              "false positive: topoSelfReport.Reason is an internal report field, not a wire reply code.",
	"internal/broker/transfer.go:(*Broker).startTransferWatchdog#1":                        "audit-only watchdog code selected by the switch immediately above.",
	"internal/broker/transfer.go:(*Broker).handlePushReq#1":                                "transferGate returns the finite session/member/node/store refusal set.",
	"internal/broker/transfer.go:(*Broker).handlePushReq#2":                                "xferProvisionRefusal returns the finite bucket provisioning refusal set.",
	"internal/broker/transfer.go:(*Broker).handlePushReq#3":                                "transfers.put returns duplicate or in-flight-cap refusal codes.",
	"internal/broker/transfer.go:(*Broker).handlePullReq#1":                                "transferGate returns the finite session/member/node/store refusal set.",
	"internal/broker/transfer.go:(*Broker).handlePullReq#2":                                "xferProvisionRefusal returns the finite bucket provisioning refusal set.",
	"internal/broker/transfer.go:(*Broker).handlePullReq#3":                                "transfers.put returns duplicate or in-flight-cap refusal codes.",
	"internal/broker/transfer.go:(*Broker).handlePushCommitReq#1":                          "transferGate returns the finite session/member/node/store refusal set.",
	"internal/broker/transfer.go:(*Broker).handleCapsReq#1":                                "transferGate returns the finite session/member/store refusal set.",
	"internal/broker/transfer.go:(*Broker).handleFinalizeReq#1":                            "transferGate returns the finite session/member/store refusal set.",

	// Agent transfer codes come from PathValidationError or the local transfer
	// state machine; this scanner does not perform interprocedural data flow.

	// Cluster trigger/admin handlers pass through the typed adminsock namespace.

	// Every site below is an adminsock.Response.Code populated by clusterCodeFor.
	//
	// Batch B: all twelve shifted by +8 when versionSkewRefusal / ErrJoinVersionSkew were
	// extracted above them. Batch B2 / external review B2-2: they shifted again (+24 at the top,
	// tapering) when the statusSchemaVersion v1->v2 contract was documented above them. The reasons are
	// unchanged, byte for byte — only the lines moved, for the TENTH time in this file's history.
	//
	// That count is the finding, not the churn: keying an exemption to file:line means every comment
	// added above a site invalidates it, and the re-key is mechanical, unreviewable and indistinguishable
	// from someone silencing a NEW site. docs/testing-standards.md G3 requires site-scoped exemptions and
	// it is right to — file-scoped ones hide future problems — but "site" does not have to mean "line".
	//
	// That is what the file:FUNCTION#ordinal key above now is; this paragraph is kept because the
	// TENTH-drift measurement is the argument for it. The ELEVENTH drift then landed in a SECOND gate
	// (internal/auth's dynamicSubscriptionExemptions), which is why both were re-keyed together.

	// Batch B / B1: the six ingress handlers converted to admit() pass the gate's `den.code`
	// through to their reply helper, so the value is a variable rather than a literal here.
	//
	// An earlier version of these entries claimed "the literals live in internal/broker/admit.go
	// and are scanned there". That was FALSE and internal review caught it: admit.go builds every
	// refusal through deny(), which is not in codeCarryingHelpers, so the scanner does not read
	// those literals either. Registering deny() would not work — TestCodeCarryingHelperListIsComplete
	// derives the list from bodies that assign a parameter to a `Code:`/`Reason:` field, and
	// denial's fields are lower-case, so deny() cannot be auto-derived and a manual entry is
	// rejected as stale.
	//
	// The SECOND version of this reason claimed "all eight codes are still emitted as literals
	// elsewhere in the scanned tree". Internal review refuted that too, by mutation, on both
	// trees: node_offline and node_not_found have NO scanner-visible emitter left anywhere. Their
	// only other emitters are transferGate's bare `return "node_offline"` / `return
	// "node_not_found"` (internal/broker/transfer.go), and this file's own header says the scanner
	// covers forms 1-3 and 8 EXACTLY — a returned bare literal is form 5, which it explicitly
	// cannot resolve. Twice wrong in one increment, in the sentence whose job is to say what is true.
	//
	// So, stated without inference:
	//
	//   - The scanner cannot resolve the six sites below. That is why they are exempted.
	//   - admit()'s refusal set is CLOSED at eight, re-derived from the AST by
	//     internal/broker.TestAdmitRefusalCodeSet, which fails on a ninth.
	//   - Every one of the eight has an exit class today, asserted by
	//     TestAdmitRefusalCodesAreClassified below. That assertion exists precisely BECAUSE two of
	//     them have no visible emitter: without it, deleting their entry from brokerCodeExitClasses
	//     would be invisible to this gate (no emitter found -> nothing to check) and they would
	//     silently fall to exit 70, which docs/usage.md §9.13 tells automation to retry.
	//
	// (An exported broker.AdmitRefusalCodes() consumed from here was considered and rejected: it
	// manufactures the cross-package sync point A1 exists to remove. The eight literals are
	// duplicated below instead — a bounded, local list whose broker-side counterpart is kept
	// closed by the test named above.)
	// The ctrl-family verbs, converted when plan §15.2's deferral was completed. Same closed
	// eight-code set — admitCtrl shares admitACL with the cmd.by family, so it cannot introduce a
	// code the set does not already contain. proxy.status is absent from this list because it
	// replies through proxyErr, which is not in codeCarryingHelpers and is therefore invisible to
	// the scanner either way (it was invisible before the conversion too).

	// Transfer reply helpers receive finite values from transferGate,
	// xferProvisionRefusal, or transfers.put; their literal origins are scanned.
}

type emittedCode struct {
	code string
	site string
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	// cmd/tether -> repo root
	root := filepath.Dir(filepath.Dir(wd))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repo root %q has no go.mod: %v", root, err)
	}
	return root
}

// scanTree walks the given package dirs (relative to root) and returns every
// statically resolvable emitted code, plus the set of files holding a Code:
// whose value could not be resolved.
// literalCodePrefix reads the machine-readable code out of a `"code: " + detail`
// concatenation. Only a STRING literal on the left counts, and only up to the
// first colon — the same split cmd/tether performs when classifying the reply,
// so the gate and the consumer agree on where the code ends. A concatenation
// that does not start with a literal carries no statically visible code and is
// reported as unresolved rather than guessed at.
func literalCodePrefix(e *ast.BinaryExpr) (string, bool) {
	if e.Op != token.ADD {
		return "", false
	}
	lit, ok := e.X.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	code, _, found := strings.Cut(s, ":")
	if !found || code == "" {
		return "", false
	}
	return code, true
}

func scanTree(t *testing.T, root string, dirs []string) (codes []emittedCode, unresolved map[string]string) {
	t.Helper()
	unresolved = map[string]string{}

	// Pass 0: string constants (name -> value), used to resolve form 2 and 4.
	// internal/adminsock is included even when it is not being scanned for
	// emissions: broker replies reference adminsock.Code* by selector, and
	// error_hints.go really does classify those values (see its "adminsock
	// cluster codes" block), so they must resolve rather than land in the
	// unresolved list.
	constVal := map[string]string{}
	constDirs := append(append([]string{}, dirs...), "internal/adminsock")
	forEachGoFile(t, root, constDirs, func(rel string, f *ast.File, fset *token.FileSet) {
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, s := range gd.Specs {
				vs, ok := s.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, nm := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						if v, err := strconv.Unquote(lit.Value); err == nil {
							constVal[nm.Name] = v
						}
					}
				}
			}
		}
	})

	add := func(v, site string) {
		if v != "" {
			codes = append(codes, emittedCode{v, site})
		}
	}

	forEachGoFile(t, root, dirs, func(rel string, f *ast.File, fset *token.FileSet) {
		// Track the parameter names of the function we are inside. A
		// `Code: <param>` is the DEFINITION BODY of a code-carrying helper, not
		// an emission site: the actual codes arrive at its call sites and are
		// picked up by form 3. Reporting those bodies as "unresolved" would
		// force a dozen meaningless exemptions and train readers to ignore the
		// unresolved list — the one signal that is supposed to stay short.
		//
		// External review R1: that exemption applied to EVERY function, so any
		// ordinary handler forwarding its own parameter into a code-carrying
		// helper was skipped too — the most likely shape of a real dynamic code,
		// and the scanner reported zero. The exemption now requires the enclosing
		// function to BE a code-carrying helper, which is the only case the
		// "covered at its call sites" argument actually holds for.
		params := map[string]bool{}
		inCodeHelper := false
		// curFunc / siteOrd build the STABLE site key. See unresolvedCodeSites for why it is not a
		// line number any more.
		//
		// siteOrd is per FILE and keyed by the qualified function name; it is NEVER reset. Internal
		// review M1/M2 broke the previous shape — a single counter zeroed on every FuncDecl boundary —
		// two independent ways, and both silently ABSORBED a brand-new unresolved site into an existing
		// exemption while all three gates stayed green:
		//
		//	same-name methods  curFunc was fd.Name.Name, so a second `HandleCluster` on a different
		//	                   receiver in clusterstatus.go restarted at #1 and collided with the ten
		//	                   entries already exempted for *clusterAdminBackend.
		//	<file-scope>       zeroing on FuncDecl EXIT reset the file-scope bucket too, so any two
		//	                   package-level sites separated by any func both keyed <file-scope>#1 — no
		//	                   name collision required at all.
		//
		// Both were reproduced on the real tree. The receiver is now part of the key, so each method has
		// its own independent sequence: the counter is unique by construction rather than by two names
		// happening to differ. internal/auth's dynamicSubscriptionExemptions already used the per-file
		// map and was injective; the same commit shipped two implementations of one key scheme and used
		// the unsafe one on the gate guarding unclassified wire codes.
		siteOrd := map[string]int{}
		curFunc := ""
		nextSite := func() string {
			fn := curFunc
			if fn == "" {
				fn = "<file-scope>"
			}
			key := rel + ":" + fn
			siteOrd[key]++
			return key + "#" + itoa(siteOrd[key])
		}
		var walk func(n ast.Node) bool
		walk = func(n ast.Node) bool {
			if fd, ok := n.(*ast.FuncDecl); ok {
				params = map[string]bool{}
				_, inCodeHelper = codeCarryingHelpers[fd.Name.Name]
				// Site keys are file:FUNCTION#ordinal (see unresolvedCodeSites), FUNCTION being
				// receiver-qualified. The ordinal counts unresolved sites within THIS function, in source
				// order, and is independent of everything above the function in the file.
				curFunc = qualifiedFuncName(fd)
				if fd.Type.Params != nil {
					for _, fl := range fd.Type.Params.List {
						for _, nm := range fl.Names {
							params[nm.Name] = true
						}
					}
				}
				if fd.Body != nil {
					ast.Inspect(fd.Body, walk)
				}
				params = map[string]bool{}
				inCodeHelper = false
				curFunc = ""
				return false
			}
			switch node := n.(type) {
			// Forms 1, 2, 7, 8: `Code:` / `Reason:` in a composite literal.
			case *ast.KeyValueExpr:
				id, ok := node.Key.(*ast.Ident)
				if !ok || (id.Name != "Code" && id.Name != "Reason") {
					return true
				}
				if v, ok := node.Value.(*ast.Ident); ok && inCodeHelper && params[v.Name] {
					return true // helper definition body; covered at its call sites
				}
				site := rel + ":" + itoa(fset.Position(node.Pos()).Line)
				switch v := node.Value.(type) {
				case *ast.BinaryExpr:
					// Form 9, see the CallExpr arm below.
					if c, ok := literalCodePrefix(v); ok {
						add(c, site)
					} else {
						unresolved[nextSite()] = rel
					}
				case *ast.BasicLit:
					if v.Kind == token.STRING {
						if s, err := strconv.Unquote(v.Value); err == nil {
							add(s, site)
						}
					}
				case *ast.Ident:
					if s, ok := constVal[v.Name]; ok {
						add(s, site)
					} else {
						unresolved[nextSite()] = rel
					}
				case *ast.SelectorExpr:
					if s, ok := constVal[v.Sel.Name]; ok {
						add(s, site)
					} else {
						unresolved[nextSite()] = rel
					}
				default:
					unresolved[nextSite()] = rel
				}

			// Form 3: an argument flowing into a known code-carrying helper.
			case *ast.CallExpr:
				var name string
				switch fn := node.Fun.(type) {
				case *ast.Ident:
					name = fn.Name
				case *ast.SelectorExpr:
					name = fn.Sel.Name
				default:
					return true
				}
				idx, ok := codeCarryingHelpers[name]
				if !ok || idx >= len(node.Args) {
					return true
				}
				site := rel + ":" + itoa(fset.Position(node.Pos()).Line)
				// External review B3: this switch used to have no default and no
				// else on the Ident/Selector arms, so a code arriving through a
				// local variable or a function result fell out of BOTH results —
				// not classified, not reported. The gate's own spec (plan D1
				// segment 3) requires an unresolvable value to hard-fail or carry
				// an explicit exemption; silently vanishing is the one outcome it
				// must never produce, because it makes "0 unclassified codes" mean
				// "we could not see any".
				unres := func() { unresolved[nextSite()] = rel }
				switch a := node.Args[idx].(type) {
				case *ast.BinaryExpr:
					// Form 9: `"code: " + err.Error()`. These reached the gate as
					// "unresolvable" and, before external review R1 narrowed the
					// parameter exemption, were not reported at all — seven of them
					// in internal/broker/run.go alone. But the code IS statically
					// present: it is the literal prefix before the colon, which is
					// exactly what cmd/tether's runFailureMessage splits back off
					// (batch-A A1 step 4). Reading it is strictly better than
					// exempting it, since the code then enters the classification
					// check like any other.
					if c, ok := literalCodePrefix(a); ok {
						add(c, site)
					} else {
						unres()
					}
				case *ast.BasicLit:
					if a.Kind == token.STRING {
						if s, err := strconv.Unquote(a.Value); err == nil {
							add(s, site)
						} else {
							unres()
						}
					} else {
						unres()
					}
				case *ast.Ident:
					if s, ok := constVal[a.Name]; ok {
						add(s, site)
					} else if !inCodeHelper || !params[a.Name] {
						// A parameter is exempt ONLY inside a code-carrying helper's
						// own definition, where the real codes arrive at its call
						// sites. Anywhere else, a parameter is exactly the dynamic
						// value this gate exists to report (external review R1).
						unres()
					}
				case *ast.SelectorExpr:
					if s, ok := constVal[a.Sel.Name]; ok {
						add(s, site)
					} else {
						unres()
					}
				default:
					unres()
				}
			}
			return true
		}
		ast.Inspect(f, walk)
	})
	return codes, unresolved
}

func forEachGoFile(t *testing.T, root string, dirs []string, fn func(rel string, f *ast.File, fset *token.FileSet)) {
	t.Helper()
	fset := token.NewFileSet()
	for _, d := range dirs {
		abs := filepath.Join(root, d)
		err := filepath.Walk(abs, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, p, nil, 0)
			if perr != nil {
				return nil
			}
			rel, _ := filepath.Rel(root, p)
			fn(filepath.ToSlash(rel), f, fset)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", abs, err)
		}
	}
}

func itoa(i int) string { return strconv.Itoa(i) }

// qualifiedFuncName renders a FuncDecl as it appears in a site key: `handleFoo` for a plain function,
// `(*Broker).handleFoo` for a method. The receiver is load-bearing, not decoration — see the siteOrd
// comment in scanTree. It also makes the key readable on its own: `HandleCluster#7` does not say which
// of two admin backends it belongs to, and every one of the 26 functions holding an exemption today is a
// method.
func qualifiedFuncName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	return "(" + recvTypeName(fd.Recv.List[0].Type) + ")." + fd.Name.Name
}

func recvTypeName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return "*" + recvTypeName(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr: // generic receiver: Foo[T]
		return recvTypeName(t.X)
	case *ast.IndexListExpr: // generic receiver: Foo[T, U]
		return recvTypeName(t.X)
	}
	// Unreachable for well-formed Go, but a silent "" would make two receivers collide again, which is
	// the defect this function exists to close. A visible placeholder fails the live-site check loudly.
	return "<unknown-recv>"
}

// scannedTrees is the set of packages that emit ctl-facing reply codes.
var scannedTrees = []string{"internal/broker", "internal/agent", "internal/proto", "internal/spawnsafe"}

// TestErrorCodeCoverage is the gate: every statically resolvable emitted code
// must either have an exit class or be on the allowlist with a reason.
func TestErrorCodeCoverage(t *testing.T) {
	root := repoRoot(t)
	codes, unresolved := scanTree(t, root, scannedTrees)

	if len(codes) == 0 {
		t.Fatal("scanner found no emitted codes at all — it has degenerated; see TestErrorCodeCoverageSelfCheck")
	}

	firstSite := map[string]string{}
	for _, c := range codes {
		if _, seen := firstSite[c.code]; !seen {
			firstSite[c.code] = c.site
		}
	}

	var missing []string
	for code, site := range firstSite {
		if _, classified := brokerCodeExitClasses[code]; classified {
			continue
		}
		if _, allowed := unclassifiedCodeAllowlist[code]; allowed {
			continue
		}
		missing = append(missing, code+"  (first emitted at "+site+")")
	}
	sort.Strings(missing)

	if len(missing) > 0 {
		t.Errorf("%d emitted reply code(s) have no exit class and no allowlist entry.\n"+
			"Each one currently exits 70, which docs/usage.md §9.13 tells automation to RETRY.\n"+
			"Classify it in brokerCodeExitClasses, or add it to unclassifiedCodeAllowlist WITH A REASON:\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}

	// Unresolved sites must be declared, so "the scanner cannot see it" is a
	// recorded decision rather than an accident.
	// Keyed by SITE, not by file: a file-keyed map kept only the first dynamic
	// site in each file, so a second one in the same file vanished — the same
	// "one entry blankets the rest" defect the file-wide exemptions had, moved
	// into the reporting side (external review R1). File-wide exemption entries
	// are gone with it; an exemption names ONE SITE and covers one site — see the
	// unresolvedCodeSites header for why that site is file:FUNCTION#ordinal rather
	// than file:line.
	var undeclared []string
	for site := range unresolved {
		if _, ok := unresolvedCodeSites[site]; ok {
			continue // exempted at this exact site
		}
		undeclared = append(undeclared, site)
	}
	sort.Strings(undeclared)
	if len(undeclared) > 0 {
		t.Errorf("%d site(s) hold a Code:/Reason: the scanner cannot resolve statically and that are not "+
			"declared in unresolvedCodeSites. Add an entry explaining why the code is covered elsewhere "+
			"(or make the value a constant):\n  %s", len(undeclared), strings.Join(undeclared, "\n  "))
	}
}

// TestAllowlistEntriesStillHaveEmitters keeps the allowlist from rotting into a
// graveyard of codes nobody emits any more.
func TestAllowlistEntriesStillHaveEmitters(t *testing.T) {
	root := repoRoot(t)
	codes, unresolved := scanTree(t, root, scannedTrees)
	emitted := map[string]bool{}
	for _, c := range codes {
		emitted[c.code] = true
	}
	// bucket_create_failed is emitted via a returned literal (form 5), which the
	// scanner deliberately does not cover; grep the tree for its literal instead.
	raw := map[string]bool{}
	forEachGoFile(t, root, scannedTrees, func(rel string, f *ast.File, fset *token.FileSet) {
		ast.Inspect(f, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if s, err := strconv.Unquote(lit.Value); err == nil {
					raw[s] = true
				}
			}
			return true
		})
	})
	for code, reason := range unclassifiedCodeAllowlist {
		if reason == "" {
			t.Errorf("allowlist entry %q has an empty reason — every exemption must justify itself", code)
		}
		if !emitted[code] && !raw[code] {
			t.Errorf("allowlist entry %q is no longer emitted anywhere; delete it", code)
		}
	}
	for key, reason := range unresolvedCodeSites {
		if reason == "" {
			t.Errorf("unresolvedCodeSites entry %q has an empty reason", key)
		}
		i := strings.LastIndexByte(key, ':')
		if i < 0 {
			t.Errorf("unresolvedCodeSites entry %q is file-wide; every exemption must name one site", key)
			continue
		}
		h := strings.LastIndexByte(key, '#')
		if h < i {
			t.Errorf("unresolvedCodeSites entry %q is function-wide; every exemption must name ONE site, "+
				"so the key needs a #ordinal (HandleCluster alone holds ten unresolved sites)", key)
			continue
		}
		if n, err := strconv.Atoi(key[h+1:]); err != nil || n < 1 {
			t.Errorf("unresolvedCodeSites entry %q has a malformed site ordinal %q", key, key[h+1:])
			continue
		}
		if _, ok := unresolved[key]; !ok {
			t.Errorf("unresolvedCodeSites entry %q no longer identifies an unresolved site; "+
				"the code moved, became resolvable, or was deleted — re-check and remove/update it", key)
		}
	}
}

// TestCodeCarryingHelperListIsComplete re-derives the helper list from source:
// any function that assigns one of its own parameters to a Code: field is
// code-carrying. A new helper that is not registered would make the coverage
// gate blind to every call site that uses it.
func TestCodeCarryingHelperListIsComplete(t *testing.T) {
	root := repoRoot(t)
	found := map[string]int{}
	forEachGoFile(t, root, scannedTrees, func(rel string, f *ast.File, fset *token.FileSet) {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil || fd.Type.Params == nil {
				continue
			}
			paramIdx := map[string]int{}
			idx := 0
			for _, fl := range fd.Type.Params.List {
				for _, nm := range fl.Names {
					paramIdx[nm.Name] = idx
					idx++
				}
				if len(fl.Names) == 0 {
					idx++
				}
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				kv, ok := n.(*ast.KeyValueExpr)
				if !ok {
					return true
				}
				k, ok := kv.Key.(*ast.Ident)
				if !ok || (k.Name != "Code" && k.Name != "Reason") {
					return true
				}
				if v, ok := kv.Value.(*ast.Ident); ok {
					if pi, isParam := paramIdx[v.Name]; isParam {
						found[fd.Name.Name] = pi
					}
				}
				return true
			})
		}
	})

	for name, idx := range found {
		got, ok := codeCarryingHelpers[name]
		if !ok {
			t.Errorf("helper %q takes a reply code at arg #%d but is NOT in codeCarryingHelpers — "+
				"every call site that uses it is invisible to TestErrorCodeCoverage. Register it.", name, idx)
			continue
		}
		if got != idx {
			t.Errorf("helper %q carries its code at arg #%d, but codeCarryingHelpers says #%d", name, idx, got)
		}
	}
	for name := range codeCarryingHelpers {
		if _, ok := found[name]; !ok {
			t.Errorf("codeCarryingHelpers lists %q but no such code-carrying helper exists any more; delete it", name)
		}
	}
}

// admitRefusalCodesForClassCheck duplicates internal/broker's admitRefusalCodes. The duplication
// is deliberate and bounded — see the note above unresolvedCodeSites' six admit() entries for why
// an exported accessor was rejected — and the broker side is kept closed by
// internal/broker.TestAdmitRefusalCodeSet, which re-derives the set from admit.go's AST.
var admitRefusalCodesForClassCheck = []string{
	"subject_malformed",
	"actor_invalid",
	"store_error",
	"session_not_found_or_deleting",
	"not_a_member",
	"not_owner",
	"node_not_found",
	"node_offline",
}

// TestAdmitRefusalCodesAreClassified closes the gap the six admit() exemptions would otherwise
// leave open.
//
// TestErrorCodeCoverage checks that every code it can SEE has an exit class. Two of admit()'s
// eight — node_offline and node_not_found — have no scanner-visible emitter anywhere since B1
// collapsed the inline copies (their only other emitters are transferGate's bare returns, a form
// this scanner explicitly cannot resolve). For those two, "no emitter found" means "nothing to
// check", so deleting their entry from brokerCodeExitClasses would be completely silent — and
// they would fall to exitInternal (70), which docs/usage.md §9.13 tells automation to RETRY. A
// non-member hitting an offline node would be retried forever.
//
// This asserts the classification directly, from the code list rather than from what the scanner
// happened to find.
func TestAdmitRefusalCodesAreClassified(t *testing.T) {
	for _, code := range admitRefusalCodesForClassCheck {
		class := brokerCodeExitClass(code)
		if class == exitInternal {
			// exitInternal is also the DEFAULT for an unknown code, so it cannot distinguish
			// "deliberately 70" from "fell off the table". Require the code to be present in the
			// table explicitly for that case.
			if _, listed := brokerCodeExitClasses[code]; !listed {
				t.Errorf("admit() can reply %q and it has NO entry in brokerCodeExitClasses, so it "+
					"falls through to exit 70 — which docs/usage.md §9.13 tells automation to retry. "+
					"Nothing else catches this: %q has no scanner-visible emitter left, so "+
					"TestErrorCodeCoverage never examines it.", code, code)
			}
		}
	}
	// Non-vacuity: a code that genuinely has no class must be reported, else the loop above is
	// checking nothing.
	if _, listed := brokerCodeExitClasses["definitely_not_a_real_code_xyz"]; listed {
		t.Fatal("the control code is somehow present in brokerCodeExitClasses — this test's " +
			"lookup is not doing what it claims")
	}
	if len(admitRefusalCodesForClassCheck) != 8 {
		t.Errorf("this list has %d entries; internal/broker.TestAdmitRefusalCodeSet pins admit()'s "+
			"set at 8. If admit() gained or lost a code, update BOTH.", len(admitRefusalCodesForClassCheck))
	}
}

// TestErrorCodeCoverageSelfCheck is the reason this gate can be trusted.
//
// A scanner that silently stops matching reports "0 unclassified codes" — a
// perfect green that means nothing. So: synthesise one source sample per
// SUPPORTED form and assert the scanner finds each. If someone simplifies the
// scanner and breaks a form, this fails instead of the gate going quietly blind.
func TestErrorCodeCoverageSelfCheck(t *testing.T) {
	samples := []struct {
		form string
		src  string
		want string
	}{
		{
			form: "1: Code: string literal",
			src: `package x
type R struct{ Code string }
func f() R { return R{Code: "selfcheck_literal"} }`,
			want: "selfcheck_literal",
		},
		{
			form: "2: Code: named constant",
			src: `package x
const codeSelfcheckConst = "selfcheck_const"
type R struct{ Code string }
func f() R { return R{Code: codeSelfcheckConst} }`,
			want: "selfcheck_const",
		},
		{
			form: "3: code-carrying helper argument",
			src: `package x
func replyExposeErr(msg any, code string, detail string) {}
func f() { replyExposeErr(nil, "selfcheck_helper", "") }`,
			want: "selfcheck_helper",
		},
		{
			form: "8: Reason: field (a different field name)",
			src: `package x
type C struct{ Reason string }
func f() C { return C{Reason: "selfcheck_reason"} }`,
			want: "selfcheck_reason",
		},
	}

	for _, s := range samples {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "sample.go"), []byte(s.src), 0o600); err != nil {
			t.Fatalf("%s: write sample: %v", s.form, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module selfcheck\n\ngo 1.25\n"), 0o600); err != nil {
			t.Fatalf("%s: write go.mod: %v", s.form, err)
		}
		codes, _ := scanTree(t, dir, []string{"."})
		var got []string
		for _, c := range codes {
			got = append(got, c.code)
		}
		if !contains(got, s.want) {
			t.Errorf("SELF-CHECK FAILED for form %q: the scanner did not find %q (found %v).\n"+
				"The coverage gate is therefore blind to this emission form and its green result is meaningless.",
				s.form, s.want, got)
		}
	}
}

// TestErrorCodeScannerDeclaresItsLimits pins the scanner's honesty: the forms it
// does NOT cover must stay documented. A gate that overstates its reach is the
// failure mode this whole file exists to avoid.
func TestErrorCodeScannerDeclaresItsLimits(t *testing.T) {
	root := repoRoot(t)
	src, err := os.ReadFile(filepath.Join(root, "cmd", "tether", "error_code_coverage_test.go"))
	if err != nil {
		t.Fatalf("read own source: %v", err)
	}
	for _, must := range []string{
		"undecidable without full data-flow analysis",
		"return \"too_many_in_flight\"",
		"It does not pretend to be complete",
	} {
		if !strings.Contains(string(src), must) {
			t.Errorf("the scanner's documented limits no longer mention %q — if coverage really was "+
				"extended, update this test; if not, restore the caveat.", must)
		}
	}
}

func contains(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// The REVERSE direction: every key in a classification table must still be emitted.
// ---------------------------------------------------------------------------
//
// origin: line-2 D1. Everything above this line asserts E ⊆ C — every code the tree EMITS is known to
// some table. That is the direction that stops an unclassified failure from exiting 70 and being retried
// forever. It is not the whole gate, and the repo has been running on half of it.
//
// The other direction is C ⊆ E: every key a table CLASSIFIES is still emitted by something. Nothing
// asserted it. The tables can therefore accumulate keys for codes that were renamed or deleted, and a
// stale key is not harmless: it is a hint that will never be printed sitting next to a live one, so the
// next person editing the table cannot tell which entries are load-bearing, and a rename that should
// have broken something silently does not.
//
// S3 §5 G3.1 called this out as the finding it had that the three lane reports did not -- "17 hint keys
// have no emitter at all; the table drifted in BOTH directions, so the gate must be bidirectional". The
// forward half landed with batch A. This is the half that did not.
//
// Today the stale count is ZERO: A1 grew brokerCodeExitClasses from 45 to 106 keys and reconciled the
// whole set as it went. So this is a zero-baseline gate — it demands no remediation, only that the
// tables stop being able to drift back.

// staleClassificationKeys lists table keys that are deliberately kept without a live emitter. It is
// empty, and the failure text below is written on the assumption that it usually will be: a key with no
// emitter is nearly always a leftover, and the rare legitimate case (a code the broker may still send
// from an older release) has to argue for itself in writing.
var staleClassificationKeys = map[string]string{}

// classificationTables is the set reconciled in the reverse direction, named so the failure message can
// say which table a stale key is in.
func classificationTables() map[string][]string {
	keysOf := func(m map[string]string) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		return out
	}
	intKeysOf := func(m map[string]int) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		return out
	}
	return map[string][]string{
		"brokerCodeHints":       keysOf(brokerCodeHints),
		"brokerCodeExitClasses": intKeysOf(brokerCodeExitClasses),
		"runFailureReasons":     keysOf(runFailureReasons),
	}
}

// adminsockCodeConstants parses internal/adminsock/protocol.go for `CodeXxx = "literal"` declarations.
//
// This indirection is not optional. Two codes are emitted ONLY as `return adminsock.CodeNotAVoter` /
// `adminsock.CodeRemoveOwnsResources` from the error->code mapper in internal/broker/clusterstatus.go —
// form 5, which the forward scanner is explicitly honest about not covering, and whose literal
// therefore never appears anywhere in the scanned trees. A universe built from literals alone reports
// both as dead, which is a false positive that would have this gate demand the deletion of two live
// classifications on its first run. (It did, before this function existed.)
func adminsockCodeConstants(t *testing.T, root string) map[string]string {
	t.Helper()
	path := filepath.Join(root, "internal", "adminsock", "protocol.go")
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	out := map[string]string{}
	ast.Inspect(f, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range vs.Names {
			if i >= len(vs.Values) || !strings.HasPrefix(name.Name, "Code") {
				continue
			}
			lit, ok := vs.Values[i].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				continue
			}
			if s, uerr := strconv.Unquote(lit.Value); uerr == nil {
				out[name.Name] = s
			}
		}
		return true
	})
	if len(out) == 0 {
		t.Fatalf("no Code* string constants found in %s — the parser is broken, and every "+
			"constant-referenced code would be reported dead", path)
	}
	return out
}

// emittedCodeUniverse is the conservative "is this string produced anywhere" set:
//
//	the forward scanner's exact results
//	∪ every string literal in the scanned trees
//	∪ the VALUE of every adminsock.Code* constant referenced from the scanned trees
//
// The union is deliberate. The scanner is honest that forms 5 and 7 are undecidable, so its exact set
// alone would flag keys for codes that really are emitted through a return value. Each extra term can
// only make this gate more permissive, never less — the right direction for a check whose false
// positives cost someone a deletion they should not make.
func emittedCodeUniverse(t *testing.T, root string) map[string]bool {
	t.Helper()
	codes, _ := scanTree(t, root, scannedTrees)
	universe := map[string]bool{}
	for _, c := range codes {
		universe[c.code] = true
	}
	constants := adminsockCodeConstants(t, root)
	forEachGoFile(t, root, scannedTrees, func(rel string, f *ast.File, fset *token.FileSet) {
		ast.Inspect(f, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.BasicLit:
				if v.Kind == token.STRING {
					if s, err := strconv.Unquote(v.Value); err == nil {
						universe[s] = true
					}
				}
			case *ast.SelectorExpr:
				// adminsock.CodeXxx -> the code it stands for.
				if pkg, ok := v.X.(*ast.Ident); ok && pkg.Name == "adminsock" {
					if val, known := constants[v.Sel.Name]; known {
						universe[val] = true
					}
				}
			}
			return true
		})
	})
	return universe
}

// TestClassificationTableKeysStillHaveEmitters is the reverse reconciliation.
func TestClassificationTableKeysStillHaveEmitters(t *testing.T) {
	root := repoRoot(t)
	universe := emittedCodeUniverse(t, root)

	var stale []string
	for table, keys := range classificationTables() {
		sort.Strings(keys)
		for _, k := range keys {
			if universe[k] {
				continue
			}
			if reason, exempt := staleClassificationKeys[k]; exempt {
				if reason == "" {
					t.Errorf("staleClassificationKeys[%q] has an empty reason — an exemption that does "+
						"not argue for itself is just a slower way of having no gate", k)
				}
				continue
			}
			stale = append(stale, table+"["+k+"]")
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d classification key(s) name a code that nothing emits any more:\n  %s\n\n"+
			"Delete them. A hint that can never print sits in the table looking exactly like one that "+
			"can, so the next person editing it cannot tell which entries are load-bearing — and a "+
			"rename that should have broken something quietly did not. If a key must stay (an older "+
			"broker can still send it, say), add it to staleClassificationKeys WITH a reason.",
			len(stale), strings.Join(stale, "\n  "))
	}

	// Reverse-of-the-reverse: an exemption for a key that IS emitted again, or that no table holds any
	// more, is itself rot. Same rule as every other ledger in this repo.
	all := map[string]bool{}
	for _, keys := range classificationTables() {
		for _, k := range keys {
			all[k] = true
		}
	}
	for k := range staleClassificationKeys {
		switch {
		case !all[k]:
			t.Errorf("staleClassificationKeys[%q] is not a key of any classification table — delete it", k)
		case universe[k]:
			t.Errorf("staleClassificationKeys[%q] is emitted again; delete the exemption", k)
		}
	}
}

// TestClassificationReverseCheckIsNonVacuous proves the reverse check can fail. Its success state is
// "no stale keys", which is indistinguishable from "the universe scan returned everything" — and the
// universe is a UNION with every string literal in the tree, so degenerating to always-true is the
// single most likely way for this gate to quietly stop working.
func TestClassificationReverseCheckIsNonVacuous(t *testing.T) {
	root := repoRoot(t)
	universe := emittedCodeUniverse(t, root)

	if len(universe) < 100 {
		t.Fatalf("the emitted-code universe has only %d entries — the scan is broken", len(universe))
	}
	// A string that is certainly not in the tree must NOT be in the universe. If it is, the union has
	// degenerated and every key would pass.
	const impossible = "definitely_not_a_real_wire_code_zzz_line2_d1"
	if universe[impossible] {
		t.Fatalf("the universe contains %q, which appears nowhere in the tree — the scan is matching "+
			"everything, so the reverse check would pass vacuously", impossible)
	}
	// And the tables must be non-empty, or there would be nothing to reconcile.
	total := 0
	for table, keys := range classificationTables() {
		if len(keys) == 0 {
			t.Errorf("classification table %s is empty", table)
		}
		total += len(keys)
	}
	if total < 100 {
		t.Errorf("the three classification tables hold only %d keys between them; A1 left them at ~151", total)
	}
	// Finally: a key the tables really do hold must be found in the universe, proving the lookup works
	// in the passing direction too and not just the failing one.
	if !universe["not_owner"] {
		t.Error("`not_owner` is classified and emitted, but the universe scan did not find it")
	}
}

// causeSplitTriggerTests names, for each code the line-2 §12 Y2 split introduced, the test that asserts WHEN it
// fires. The value is a test function name; the gate below checks that function actually exists.
//
// origin: line-2 external review M18 / PC-3. TestClassificationTableKeysStillHaveEmitters (above) checks
// that each classified code has an EMITTER — a string literal somewhere in the tree. That is a real check
// and it caught a real dead key (`already_voter`), but it is blind to the failure Y2 actually risks: the
// literal stays put while the branch leading to it stops being reachable, or the sentinel chain that
// selects between two literals gets cut. Both were measured on this very increment, and both left every
// package green:
//
//	`fmt.Errorf("%w: ...", ErrUpgradeHTTPStatus)` -> `%v`   every 404 falls back to download_failed
//	the pty errno condition -> `if false`                  pty_alloc_failed becomes unreachable
//
// Y2's entire deliverable is the mapping from cause to code, because that is what tells a monitor whether
// to retry. A code whose trigger nothing asserts is a code that has a literal and no meaning.
//
// This registry is deliberately narrow: the four Y2 codes, not every code in the repo. Demanding a named
// trigger test for all ~90 codes would be a large retrofit with most of the value already covered by the
// emitter check; these four are the ones whose whole point IS the discrimination.
var causeSplitTriggerTests = map[string]string{
	"download_http_status":    "TestFetchURLWrapsStatusAndSizeSentinels",
	"download_too_large":      "TestFetchURLWrapsStatusAndSizeSentinels",
	"pty_unavailable":         "TestPTYFailureTransientClassification",
	"attach_subscribe_failed": "TestAttachSubscribeFailureIsItsOwnCode",
}

// TestCauseSplitCodesHaveTriggerTests reconciles causeSplitTriggerTests against the tree in both directions.
func TestCauseSplitCodesHaveTriggerTests(t *testing.T) {
	root := repoRoot(t)

	declared := map[string]bool{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		for _, m := range regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]*)\s*\(`).
			FindAllStringSubmatch(string(src), -1) {
			declared[m[1]] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(declared) < 100 {
		t.Fatalf("the test-function scan found only %d declarations — it is broken, so every check below "+
			"would pass vacuously", len(declared))
	}

	var missing []string
	for code, testName := range causeSplitTriggerTests {
		if !declared[testName] {
			missing = append(missing, code+" -> "+testName)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("%d Y2 code(s) name a trigger test that does not exist:\n  %s\n\n"+
			"Either the test was renamed (update this map in the same commit) or it was deleted, in which "+
			"case the code's TRIGGER is now unasserted: the literal survives, the branch reaching it does "+
			"not have to.", len(missing), strings.Join(missing, "\n  "))
	}

	// REVERSE: every Y2 code must be in the registry. The four are enumerated in the plan's §12 Y2 item;
	// they are also exactly the codes whose hint text in error_hints.go cites Y2.
	for _, code := range []string{
		"download_http_status", "download_too_large", "pty_unavailable", "attach_subscribe_failed",
	} {
		if _, ok := causeSplitTriggerTests[code]; !ok {
			t.Errorf("Y2 code %q has no entry in causeSplitTriggerTests — it was added to the wire without a test "+
				"asserting when it fires", code)
		}
	}
}
