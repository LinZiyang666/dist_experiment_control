package main

import (
	"fmt"
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

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/infracli"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
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
// S6 additionally scans the finite process/cluster refusal mapper returns and
// the lock mapper's constant-selected local codes. Any dynamic lock-code write
// or escape remains unresolved; these bounded cases do not extend the claim
// to arbitrary return chains.
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
	"replyErr":           1,
	"replyExposeErr":     1,
	"replyExposeRmErr":   1,
	"replyPushErr":       1,
	"replyPullErr":       1,
	"replyCommitErr":     1,
	"replyUpgradeErr":    1,
	"fsRefuse":           1,
	"pathErr":            0,
	"cleanupEntry":       1,
	"PublishFailedEvent": 4,
	"pubPtyFailed":       2,
	"replyRunFailed":     1,
	// hot-update S1: the broker's process-resume refusal; its call sites pass
	// proto.CodeProcess* constants, den.code from admitACL, or forward_failed.
	"processResumeReject": 3,
	// hot-update S7: the transfer leaves' typed refusals. xfercontrol/xfertrack
	// refusal(code) and the client's clientError(code, ...) carry xfercontract
	// Code* constants that reach the wire Code field and the CLI exit class.
	// origin: hot-update S7 plan §7 / T34 / INTEG-D cmd/tether race suite
	"refusal":     0,
	"clientError": 0,
	// The moved broker upgrade adapter forwards the existing admitACL code.
	// origin: S7 internal review ledgers6 F3 (error-code missing-emitter-keys)
	"AgentTransactionReply": 1,
	// origin: hot-update S8 BROKER-EXPORTER R-9 — the async admission reply moved here.
	"Forward": 1,
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
	// origin: hot-update S7 plan §7 / T34 / T50; INTEG-D cmd/tether race suite. The accepted-result
	// class 74 is presentation-only: the transfer engine's phase-aware classifier (xferclient.ExitCode,
	// reached through transferCLI.Classify) assigns it after a send, and brokerCodeExitClasses may not
	// hold 74 (TestExitUnsettledIsPresentationOnly). TestTransferErrorEmittersReachCLIExit closes the
	// emitter -> classifier -> exit path for these codes on the transfer command.
	"transfer_result_unknown":     "accepted-result 74, assigned by the transfer engine classifier, never a pre-accept broker class (T50 closes it).",
	"transfer_terminal_conflict":  "accepted-result 74, assigned by the transfer engine classifier, never a pre-accept broker class (T50 closes it).",
	"transfer_commit_inflight":    "accepted-result 74, assigned by the transfer engine classifier, never a pre-accept broker class (T50 closes it).",
	"transfer_durability_unknown": "accepted-result 74, assigned by the transfer engine classifier, never a pre-accept broker class (T50 closes it).",
	"transfer_reconcile_required": "an engine-internal control value (plan §7: never printed as a result); the classifier maps it to 74 (T50 closes it).",
	// origin: hot-update S7 plan §6.4 / lead LQ1; INTEG-D cmd/tether race suite.
	"origin_census_unavailable": "a settlement PendingTransfer reason, rendered inside session rm's error text; the reply Code is transfer_owner_recovering (75).",
	"revoked":                   "xfercontract.CancelRevoked, the CancelRequest.Reason the settlement barrier passes to the tracker in-process; never a reply Code.",
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
	// origin: hot-update S7 plan §11.4 / T50; legacy audit projections moved into LegacyOutcome literals.
	"internal/broker/transfer.go:(*Broker).handleEvTransfer#1":  "LegacyOutcome.Code copies the receiver's TransferEvent.Code into the canonical audit only; LegacyTerminal retains it only for failed outcomes. This was the existing rec.Code assignment, not a new reply emitter; TestLegacyTransferHistoryRetainsAdmissionIdentity checks the retained decision.",
	"internal/broker/transfer.go:(*Broker).handleFinalizeReq#2": "LegacyOutcome.Code copies the creator's TransferFinalizeReq.Code into the canonical audit only; LegacyTerminal retains it only for failed outcomes. The existing transferGate refusal remains #1; TestLegacySettleConfirmsStartBeforeTerminalAndRetainsOnRefusal checks exact settlement.",
	// origin: hot-update S8 plan §7.1 / P15; the signed roll verifier preserves the existing missing-route class.
	"internal/brokerupgrade/agent_roll.go:WithAgentRollAuthority#1": "VerifyRoll returns upgradetx.CodeNotWired (upgrade_not_wired) when account authority is unavailable; upgradetx.ExitCode maps it to 74, and TestUpgradeExitCodePreservesAcceptance plus TestDynamicUpgradeRefusalsKeepExitClasses check the transaction presentation path.",
	// origin: hot-update S8 BROKER-EXPORTER R-9 — follow the extracted async reply and preserve scanner limits.
	"internal/brokerupgrade/agent_forward.go:(*AgentForwarder).Forward#1": "refusal(upgradetx.CodeDeferred) refuses a full async forwarding owner before dispatch (75); TestAgentForwardRelaysOffTheCallerWithinTheOriginalDeadline checks the capacity refusal, and TestDynamicUpgradeRefusalsKeepExitClasses checks the S8 wire and CLI classes.",
	"internal/broker/clusterwrite.go:(*Broker).proposeOrForward#1":        "The name-based helper scanner also sees cluster.Forwarder.Forward(verb, reqID, payload); its second argument is the Raft request identity, never a reply Code. The actual AgentForwarder.Forward admission code is covered at handleUpgradeTxReq#1.",
	// Exact S7/S8/S9 projection sites added after the original scanner roots.
	// Their producers retain the established classifiers: S7 §7, S8 §7.1,
	// S9 §7.3. A projection never gets a file-wide or function-wide exemption.
	// origin: S7 internal review ledgers6 F1 (error-code unresolved-sites)
	"internal/agentupgradeport/infra_control.go:BindInfraControl#1":     "proto.InfraCode(err) returns the closed S9 family: invalid/unsupported 64, permission/policy 77, admission/health/budget 75, result/recovery unknown 74, internal 70; TestDynamicUpgradeRefusalsKeepExitClasses checks its real CLI path.",
	"internal/proto/infra_upgrade.go:InfraStatusReport#1":               "status projection uses the same proto.InfraCode family as bindAgentInfra (64/77/75/74/70), classified by infracli.ExitClasses and checked through brokerErrorMessage by TestDynamicUpgradeRefusalsKeepExitClasses.",
	"internal/agent/upgrade.go:ProbeUpgradeControl#1":                   "upgradetx.CodeUnsettled is upgrade_unsettled (74); upgradewire.ErrorResponse and upgradeExit retain that class, checked by TestDynamicUpgradeRefusalsKeepExitClasses.",
	"internal/agentupgradeport/runtime_snapshot.go:RuntimeDeferrals#1":  "ReasonUpgradeInProgress is deferral detail, carried by upgrade_deferred (75) before acceptance or upgrade_unsettled (74) after acceptance; never a new wire Code.",
	"internal/agentupgradeport/runtime_snapshot.go:RuntimeDeferrals#2":  "ReasonFrozenTable is deferral detail with the same upgrade_deferred (75)/accepted upgrade_unsettled (74) policy; never a wire Code.",
	"internal/agentupgradeport/runtime_snapshot.go:SnapshotRuntime#1":   "ReasonDomainCensus is deferral detail under upgrade_deferred (75)/accepted upgrade_unsettled (74); private census details do not originate a reply Code.",
	"internal/agentupgradeport/runtime_snapshot.go:SnapshotRuntime#2":   "ReasonUpgradeInProgress is the same busy-cut detail as transactionDeferrals#1, not a wire Code.",
	"internal/agentupgradeport/runtime_snapshot.go:SnapshotRuntime#3":   "ReasonFrozenTable is the same frozen-owner detail as transactionDeferrals#2, not a wire Code.",
	"internal/broker/exec.go:(*Broker).handleProcEvent#1":               "upgradewire.ErrorResponse(err).Code preserves the closed S8 64/75/74/77 family and maps accepted uncertainty to 74; this ProcEventAck is courier-only, while TestDynamicUpgradeRefusalsKeepExitClasses checks the actual transaction CLI classes.",
	"internal/broker/reconcile_upgrade_lock.go:registerAuthority#1":     "proto.CodeSessionIncarnationMismatch aliases upgradetx.CodeSessionIncarnationMismatch (77); TestDynamicUpgradeRefusalsKeepExitClasses checks its wire and CLI paths. The separate CodeLeaderUnavailable refusal is already resolved (75).",
	"internal/broker/upgrade.go:(*Broker).handleUpgradeReq#2":           "CheckLegacyAgentRequest returns the finite refusal set scanned at its moved owner internal/brokerupgrade/agent_entry.go: malformed/config/clone-family 64, legacy busy try_again 75.",
	"internal/broker/upgrade.go:handleUpgradeTxReq#1":                   "AgentForwarder.Forward forwards den.code from admitACL; the existing closed admission set and its 64/75/77 classes are pinned by TestAdmitRefusalCodesAreClassified.",
	"internal/filetransfer/handler.go:(*Handler).HandlePullForwarded#3": "legacyOnce failure selects object_put_failed (explicit catch-all 70) or bucket_unknown (64); both literal origins remain scanned immediately above this reply.",
	"internal/filetransfer/worker_owner.go:refuseLink#1":                "result.Code is empty for the cap0 probe, otherwise xfercontract.CodeOwnerRecovering (75); S7 T50 checks the leaf classifier and CLI exit.",
	"internal/xfercontrol/routing.go:DecodePullSelectionProbe#1":        "projects the original peer probe Code into ProbeResult; the S7 leaf refusal classes 64/75/77 and accepted-result presentation 74 are closed by T50.",
	"internal/xfercontrol/routing.go:DecodePullSelectionProbe#2":        "forwards the same ProbeResult.Code as #1 into Response, originating no code; T50 checks emitter/classifier/CLI closure.",
	"internal/xfercontrol/routing.go:WireResponse#6":                    "projects the controller Response Code (status/source/cancel/prepare) onto the wire DTO; T50 closes the existing S7 emitter/classifier/CLI path.",
	"internal/brokerupgrade/agent_entry.go:admitAgentTransaction#1":     "refusal(upgradetx.CodeRejected) rejects a nonprivate reply inbox before acceptance (64); TestDynamicUpgradeRefusalsKeepExitClasses checks the S8 wire and CLI classes.",
	"internal/brokerupgrade/agent_entry.go:admitAgentTransaction#2":     "refusal(upgradetx.CodeDeferred) reports a legacy target without a quiescence proof before acceptance (75); TestDynamicUpgradeRefusalsKeepExitClasses checks the S8 wire and CLI classes.",
	"internal/brokerupgrade/agent_entry.go:admitAgentTransaction#3":     "refusal(upgradetx.CodeRejected) rejects a legacy target before acceptance (64); TestDynamicUpgradeRefusalsKeepExitClasses checks the S8 wire and CLI classes.",
	// origin: hot-update S8 BROKER-EXPORTER R-9 — async relay preserves the original deadline.
	"internal/brokerupgrade/agent_entry.go:(agentForward).relay#1": "refusal(upgradetx.CodeDeferred) reports an expired original deadline before sending (75); TestAgentForwardRelaysOffTheCallerWithinTheOriginalDeadline checks no dispatch, and TestDynamicUpgradeRefusalsKeepExitClasses checks the S8 wire and CLI classes.",
	"internal/brokerupgrade/agent_entry.go:(agentForward).relay#2": "refusal(upgradetx.CodeDeferred) reports a missing transaction responder before acceptance (75); TestDynamicUpgradeRefusalsKeepExitClasses checks the S8 wire and CLI classes.",
	"internal/brokerupgrade/agent_entry.go:(agentForward).relay#3": "refusal(upgradetx.CodeDeferred) reports a lost read-only Prepare/Query response before an effect is accepted (75); TestDynamicUpgradeRefusalsKeepExitClasses checks the S8 wire and CLI classes.",
	"internal/brokerupgrade/agent_entry.go:(agentForward).relay#4": "accepted(refusal(upgradetx.CodeUnsettled)) reports an unknown mutation result (74), never a retryable new transaction; TestDynamicUpgradeRefusalsKeepExitClasses checks the S8 wire and CLI classes.",
	// hot-update S5 AGENT-SLIM moved the transfer handlers into internal/filetransfer.
	"internal/agent/transfer.go:(*Agent).pubTransferEvFailed#1": "Thin delegation forwards the same code parameter to PublishFailedEvent; its callers and all literal origins are scanned in internal/filetransfer.",
	// hot-update S1 (docs/reviews/hot-update-reliable-exec-plan.md §2.7 / §3.4).
	"internal/agent/exec.go:handleProcessResumeForwarded#1": "Code is processRefusalCode(err) (or CodeProcessUnsupported): its finite return set is scanned directly in processRefusalCode; its outcomes are classified before create, with exec.go and run.go applying the post-create phase rule.",
	"internal/broker/exec.go:handleProcessResumeReq#1":      "code is den.code from admitACL, whose eight denial codes TestAdmitRefusalCodesAreClassified covers; the other processResumeReject call sites pass proto.CodeProcess* constants or `forward_failed`, which the scanner resolves through the helper.",
	// hot-update S3 (docs/reviews/hot-update-agent-bootstrap-plan.md §4.1/§4.2): these `Reason:` fields are
	// agentimage deferral reasons (the fixed set agentimage.DeferredReasons, pinned by
	// TestDeferredReasonSetIsTheDocumentedOne). They never reach the wire as a code: they are rendered into
	// the Error text of the literal `upgrade_deferred` reply, which is the classified code.
	"internal/agent/upgrade_image.go:freezeForUpgrade#1": "Reason is agentimage.ReasonRuntimeClosed, a deferral reason rendered inside the literal upgrade_deferred reply's Error text, not a wire code.",
	// origin: hot-update S5 plan §5.4 / §10.2. The agentimage constants are
	// outside the scanned trees; the capacity refusal remains deferral text.
	"internal/agent/upgrade_image.go:freezeForUpgrade#2":         "Reason is agentimage.ReasonImageTooLarge for SS capacity exhaustion, rendered inside the classified upgrade_deferred reply's Error text, not a wire code.",
	"internal/agentupgrade/image_policy.go:PreflightDeferrals#1": "Reason is one member of agentimage.DeferredReasons selected by the busy-check table; rendered inside the literal upgrade_deferred reply's Error text, not a wire code.",
	// Agent run reasons are finite locals selected immediately above each site.
	"internal/agent/run.go:(*Agent).handleRunForwarded#1":                                  "Reason is remoteFSFailReason(ferr); its literal outcomes are scanned at their definitions.",
	"internal/agent/run.go:(*Agent).handleRunForwarded#2":                                  "pubPtyFailed forwards the same remoteFSFailReason value as #1 in this function.",
	"internal/filetransfer/handler.go:(*Handler).HandlePushForwarded#1":                    "code is io_error or PathValidationError.Code; both origins are scanned.",
	"internal/filetransfer/handler.go:(*Handler).HandlePushForwarded#2":                    "audit emission forwards the same code as #1 in this function.",
	"internal/filetransfer/handler.go:(*Handler).HandlePushTierA#1":                        "finalize's code parameter is supplied by literal/PathValidationError call sites in this function.",
	"internal/filetransfer/handler.go:(*Handler).HandlePushTierA#2":                        "audit emission forwards finalize's same code parameter.",
	"internal/filetransfer/handler.go:(*Handler).HandlePushCommitForwarded#1":              "PathValidationError.Code is defined by the path validator's finite code set.",
	"internal/filetransfer/handler.go:(*Handler).HandlePullForwarded#1":                    "ValidateForRead refusal is transferFailure(err, io_error): PathValidationError codes retain their 64/77 classes and the explicit io_error catch-all retains 70; both origins are scanned.",
	"internal/filetransfer/handler.go:(*Handler).HandlePullForwarded#2":                    "OpenForReadAtomic refusal uses the same transferFailure(err, io_error) path-code/catch-all split as #1; this is a second reply, not an audit emission.",
	"internal/brokerupgrade/brokerruntime/cluster_grow_trigger.go:HandleGrowTrigger#1":     "verbatim adminsock response code; adminsock has its own registry.",
	"internal/brokerupgrade/brokerruntime/cluster_grow_trigger.go:HandleGrowTrigger#2":     "verbatim adminsock response code; adminsock has its own registry.",
	"internal/brokerupgrade/brokerruntime/cluster_grow_trigger.go:HandleGrowTrigger#3":     "verbatim adminsock response code; adminsock has its own registry.",
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
	// origin: hot-update S7 P11 moved the legacy watchdog (and this site) into xfertrack.
	"internal/xfertrack/watchdog.go:(*legacyWatchdog).startTransferWatchdog#1": "audit-only watchdog code selected by the switch immediately above.",
	// origin: hot-update S7 INTEG-A: codeXferBrokerRestarting aliases xfercontrol.CodeLegacyBrokerRestarting,
	// a cross-package const alias the scanner cannot fold; it moved the three older sites to #2–#4.
	"internal/broker/transfer.go:(*Broker).handlePushReq#1": "codeXferBrokerRestarting = xfercontrol.CodeLegacyBrokerRestarting (\"broker_restarting\", classified 75).",
	"internal/broker/transfer.go:(*Broker).handlePushReq#2": "transferGate returns the finite session/member/node/store refusal set.",
	"internal/broker/transfer.go:(*Broker).handlePushReq#3": "xferProvisionRefusal returns the finite bucket provisioning refusal set.",
	"internal/broker/transfer.go:(*Broker).handlePushReq#4": "transfers.put returns duplicate or in-flight-cap refusal codes.",
	"internal/broker/transfer.go:(*Broker).handlePullReq#1": "codeXferBrokerRestarting = xfercontrol.CodeLegacyBrokerRestarting (\"broker_restarting\", classified 75).",
	"internal/broker/transfer.go:(*Broker).handlePullReq#2": "transferGate returns the finite session/member/node/store refusal set.",
	"internal/broker/transfer.go:(*Broker).handlePullReq#3": "xferProvisionRefusal returns the finite bucket provisioning refusal set.",
	"internal/broker/transfer.go:(*Broker).handlePullReq#4": "transfers.put returns duplicate or in-flight-cap refusal codes.",

	// hot-update S7 continuation transfer. None of these sites ORIGINATES a code: each projects or
	// forwards a code an xfer leaf already produced through refusal(...)/clientError(...) — both
	// registered in codeCarryingHelpers, so every literal Code* origin is scanned — or a peer's reply
	// code. The transfer command's exit path for all of them is the leaf classifier, closed by
	// TestTransferErrorEmittersReachCLIExit (T50). Cancel/revocation `Reason:` values are internal
	// CancelReason enum members, never a reply code.
	// origin: hot-update S7 plan §7 / T34 / T50; INTEG-D cmd/tether race suite
	"internal/proto/transfer_agent.go:EncodeAgentTransferRequest#1":    "Reason is the CancelReason enum value carried by an agent cancel request, not a reply code.",
	"internal/proto/transfer_codec.go:EncodeTransferRequest#1":         "Reason is the CancelReason enum value carried by a ctl cancel request, not a reply code.",
	"internal/proto/transfer_codec.go:DecodeTransferResponse#1":        "codec projection of a probe reply's Code into the neutral ProbeResult; originates nothing.",
	"internal/proto/transfer_codec.go:contractStatus#1":                "codec projection of a status reply's Code into the neutral Status; originates nothing.",
	"internal/proto/transfer_codec.go:wireOutcome#1":                   "codec projection of a receiver outcome's Code onto the wire; the outcome codes are leaf constants.",
	"internal/proto/transfer_codec.go:DecodeTransferRequest#1":         "codec projection of a terminal request's receiver-outcome Code; originates nothing.",
	"internal/proto/transfer_codec.go:DecodeTransferRequest#2":         "Reason is the CancelReason enum value of a decoded cancel request, not a reply code.",
	"internal/proto/transfer_codec.go:EncodeTransferResponse#1":        "codec projection of the controller response's Code (probe); produced via refusal(...).",
	"internal/proto/transfer_codec.go:EncodeTransferResponse#2":        "codec projection of the controller response's Code (permit); produced via refusal(...).",
	"internal/proto/transfer_codec.go:EncodeTransferResponse#3":        "codec projection of the controller response's Code (push prepare); produced via refusal(...).",
	"internal/proto/transfer_codec.go:EncodeTransferResponse#4":        "codec projection of the controller response's Code (pull prepare); produced via refusal(...).",
	"internal/proto/transfer_codec.go:EncodeTransferResponse#5":        "codec projection of the controller response's Code (commit); produced via refusal(...).",
	"internal/proto/transfer_codec.go:EncodeTransferResponse#6":        "codec projection of the controller response's Code (reserve); produced via refusal(...).",
	"internal/proto/transfer_codec.go:EncodeTransferResponse#7":        "codec projection of the controller response's Code (terminal/finalize); produced via refusal(...).",
	"internal/proto/transfer_codec.go:EncodeTransferResponse#8":        "codec projection of the controller response's Code (status/source/cancel); produced via refusal(...).",
	"internal/proto/transfer_codec.go:wireStatus#1":                    "codec projection of a tracker Status's Code onto the wire; originates nothing.",
	"internal/transferpath/transfer_atomic_unix.go:(*Atomic).Commit#1": "returns the commit-admission outcome code selected by the finite checks just above (Code* constants).",
	"internal/xferclient/engine.go:(*Client).cancel#1":                 "Reason is the caller's CancelReason enum value, not a reply code.",
	"internal/xferclient/engine.go:(*Client).failure#1":                "failure forwards its code parameter into clientError; callers pass xfercontract Code* constants or a peer reply's code.",
	"internal/xferclient/legacy.go:(*Legacy).Run#1":                    "forwards the cap0 legacy result's failed code into clientError; the legacy broker's codes are classified on their own wire.",
	"internal/xferclient/status.go:(*Client).resultError#1":            "forwards the exact terminal receipt's failed code into clientError; receipt codes are leaf constants.",
	"internal/xferclient/status.go:QueryStatus#1":                      "forwards the broker status reply's code into clientError (result_unknown when absent).",
	"internal/xferclient/status.go:validateStatus#1":                   "forwards a status's own code (or a fixed Code* constant) into clientError.",
	"internal/xfercontrol/controller.go:(*controller).probe#1":         "forwards the transport reply's code into refusal(...).",
	"internal/xfercontrol/controller.go:(*controller).probe#2":         "forwards the agent probe result's code into refusal(...).",
	"internal/xfercontrol/routing.go:CommitCarrier#1":                  "forwards the original agent's delivery reply code into refusal(...).",
	"internal/xfercontrol/controller.go:(*controller).forward#1":       "forwards the transport reply's code into refusal(...).",
	"internal/xfercontrol/controller.go:validateStatus#1":              "forwards the tracker status's own code into refusal(...).",
	"internal/xfercontrol/controller.go:errorResponse#1":               "projects a leaf *Error's code (from refusal(...)) into the controller Response.",
	"internal/xfercontrol/routing.go:DecodeWire#1":                     "Reason is the decoded CancelReason enum value, not a reply code.",
	"internal/xfercontrol/routing.go:WireResponse#1":                   "projects the controller Response's Code (probe) onto the wire DTO.",
	"internal/xfercontrol/routing.go:WireResponse#2":                   "projects the same probe Code as #1 into the pull-selection probe DTO; T50 closes the existing S7 emitter/classifier/CLI path.",
	"internal/xfercontrol/routing.go:WireResponse#3":                   "projects the controller Response Code (permit) onto the wire DTO; originates no code.",
	"internal/xfercontrol/routing.go:WireResponse#4":                   "projects the controller Response Code (reserve/commit) onto the wire DTO; originates no code.",
	"internal/xfercontrol/routing.go:WireResponse#5":                   "projects the controller Response Code (terminal/finalize) onto the wire DTO; originates no code.",
	"internal/xferfinal/cleanup.go:rowStatus#1":                        "reads the code back out of the already-canonical terminal bytes for display; originates nothing.",
	"internal/xferworker/registry.go:(*owner).Apply#1":                 "Reason is the control's revocation reason enum value retained in worker state, not a reply code.",
	"internal/filetransfer/handler.go:handleContinuation#1":            "workerCode maps a worker refusal to its own xfercontract Code* constant (or the refusal fallback); originates no new code.",

	"internal/broker/transfer.go:(*Broker).handlePushCommitReq#1": "transferGate returns the finite session/member/node/store refusal set.",
	"internal/broker/transfer.go:(*Broker).handleCapsReq#1":       "transferGate returns the finite session/member/store refusal set.",
	"internal/broker/transfer.go:(*Broker).handleFinalizeReq#1":   "transferGate returns the finite session/member/store refusal set.",

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

// The lock mapper selects a local code from a finite set of constants. Inspect
// every write to that exact binding (not its name: sibling branches shadow it).
// A dynamic assignment or address escape refuses resolution, so adding a new
// outcome cannot silently borrow the known constant set. This is intentionally
// bounded to the lock mapper; arbitrary interprocedural flows remain unresolved.
// origin: hot-update S6 plan §9.3 / T53 / GATES-S6-E
func finiteLockCodeValues(fn *ast.FuncDecl, target *ast.Ident, constants map[string]string, imports map[string]bool) ([]string, bool) {
	if fn == nil || target.Obj == nil || target.Obj.Kind != ast.Var {
		return nil, false
	}
	switch target.Obj.Decl.(type) {
	case *ast.AssignStmt, *ast.ValueSpec:
	default:
		return nil, false
	}
	values := map[string]bool{}
	valid := true
	bound := func(e ast.Expr) bool {
		for {
			paren, ok := e.(*ast.ParenExpr)
			if !ok {
				break
			}
			e = paren.X
		}
		id, ok := e.(*ast.Ident)
		return ok && id.Obj == target.Obj
	}
	addValue := func(e ast.Expr) {
		var value string
		var ok bool
		switch expr := e.(type) {
		case *ast.BasicLit:
			if expr.Kind == token.STRING {
				value, _ = strconv.Unquote(expr.Value)
				ok = true
			}
		case *ast.Ident:
			if expr.Obj == nil || expr.Obj.Kind == ast.Con {
				value, ok = constants[expr.Name]
			}
		case *ast.SelectorExpr:
			if pkg, isPackage := expr.X.(*ast.Ident); isPackage && imports[pkg.Name] && (pkg.Obj == nil || pkg.Obj.Kind == ast.Pkg) {
				value, ok = constants[expr.Sel.Name]
			}
		}
		if !ok {
			valid = false
		}
		values[value] = true
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch stmt := n.(type) {
		case *ast.AssignStmt:
			for i, lhs := range stmt.Lhs {
				if bound(lhs) {
					if len(stmt.Lhs) != len(stmt.Rhs) || stmt.Tok != token.ASSIGN && stmt.Tok != token.DEFINE {
						valid = false
					} else {
						addValue(stmt.Rhs[i])
					}
				}
			}
		case *ast.ValueSpec:
			for i, name := range stmt.Names {
				if bound(name) {
					if len(stmt.Names) != len(stmt.Values) {
						valid = false
					} else {
						addValue(stmt.Values[i])
					}
				}
			}
		case *ast.UnaryExpr:
			if stmt.Op == token.AND && bound(stmt.X) {
				valid = false
			}
		case *ast.RangeStmt:
			if bound(stmt.Key) || bound(stmt.Value) {
				valid = false
			}
		case *ast.IncDecStmt:
			if bound(stmt.X) {
				valid = false
			}
		}
		return true
	})
	var out []string
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out, valid && len(out) > 0
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
	constDirs := append(append([]string{}, dirs...), "internal/adminsock", "internal/xfercontract")
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
		imports := map[string]bool{}
		for _, spec := range f.Imports {
			path, _ := strconv.Unquote(spec.Path.Value)
			name := filepath.Base(path)
			if spec.Name != nil {
				name = spec.Name.Name
			}
			imports[name] = true
		}
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
		var curDecl *ast.FuncDecl
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
				curDecl = fd
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
				curDecl = nil
				return false
			}
			switch node := n.(type) {
			case *ast.ReturnStmt:
				// These finite runtime mappers originate PTY and cluster refusal
				// branches. Scan their returns rather than assuming every proto
				// declaration is actually emitted. Dynamic returns fail closed.
				// origin: hot-update S4 plan §10 / T49; hot-update S6 plan §9.3 / T53
				if curFunc != "processRefusalCode" && curFunc != "ClusterCodeFor" {
					return true
				}
				site := rel + ":" + itoa(fset.Position(node.Pos()).Line)
				if len(node.Results) != 1 {
					unresolved[nextSite()] = rel
					return true
				}
				switch value := node.Results[0].(type) {
				case *ast.BasicLit:
					if value.Kind == token.STRING {
						if code, err := strconv.Unquote(value.Value); err == nil {
							add(code, site)
						}
					}
				case *ast.SelectorExpr:
					if code, ok := constVal[value.Sel.Name]; ok {
						add(code, site)
					} else {
						unresolved[nextSite()] = rel
					}
				case *ast.Ident:
					if code, ok := constVal[value.Name]; ok {
						add(code, site)
					} else {
						unresolved[nextSite()] = rel
					}
				default:
					unresolved[nextSite()] = rel
				}
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
				// origin: hot-update S4 plan §9.2 / N1-REFUSAL. Run's
				// literal Reason can carry detail just like its concatenated
				// form; classify the prefix the client actually reads. Code
				// fields remain exact tokens and must not inherit this split.
				addField := func(code string) {
					if id.Name == "Reason" {
						code, _, _ = strings.Cut(code, ":")
						code = strings.TrimSpace(code)
					}
					add(code, site)
				}
				switch v := node.Value.(type) {
				case *ast.BinaryExpr:
					// Form 9, see the CallExpr arm below.
					if c, ok := literalCodePrefix(v); ok {
						addField(c)
					} else {
						unresolved[nextSite()] = rel
					}
				case *ast.BasicLit:
					if v.Kind == token.STRING {
						if s, err := strconv.Unquote(v.Value); err == nil {
							addField(s)
						}
					}
				case *ast.Ident:
					// A same-named constant from another scanned package cannot
					// turn the lock mapper's local binding into a constant emission.
					// origin: hot-update S6 plan §9.3 / T53 / GATES-S6-E
					if s, ok := constVal[v.Name]; ok && (curFunc != "handleUpgradeLock" || v.Obj == nil || v.Obj.Kind == ast.Con) {
						addField(s)
					} else if curFunc == "handleUpgradeLock" {
						if values, finite := finiteLockCodeValues(curDecl, v, constVal, imports); finite {
							for _, value := range values {
								addField(value)
							}
						} else {
							unresolved[nextSite()] = rel
						}
					} else {
						unresolved[nextSite()] = rel
					}
				case *ast.SelectorExpr:
					if s, ok := constVal[v.Sel.Name]; ok {
						addField(s)
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
// The S8 extraction moved the legacy upgrade producer and deferral projection
// together with their comments; both forward and reverse scans follow them.
// origin: S7 internal review ledgers6 F3 (error-code missing-emitter-keys)
// gate-control: TestErrorCodeCoverageSelfCheck/moved_owner_roots
var scannedTrees = append([]string{"internal/broker", "internal/agent", "internal/proto", "internal/spawnsafe", "internal/transferpath", "internal/filetransfer", "internal/brokercontrol/clusterstatus.go", "internal/brokerupgrade/agent_entry.go", "internal/agentupgrade/image_policy.go",
	// origin: hot-update S8 BROKER-EXPORTER R-9 — follow the moved admission reply emitter.
	"internal/brokerupgrade/agent_forward.go",
	// origin: S10 internal review ARCH-REDS-3 F1 — retain the moved authority refusal.
	"internal/brokerupgrade/agent_roll.go",
	// Follow the signed grow/upgrade emitters and staged cutover after the responsibility move.
	// origin: hot-update S7 plan §11.4 / S7 BROKER-SLIM-2
	"internal/brokerupgrade/brokerruntime/cluster_grow_trigger.go",
	"internal/brokerupgrade/brokerruntime/cluster_grow_cutover.go",
	"internal/brokerupgrade/brokerruntime/cluster_upgrade_trigger.go",
	// Follow the unchanged dynamic refusals after their S8 responsibility move.
	// origin: S10 internal review MAKETEST-GATES F4
	"internal/agentupgradeport/runtime_snapshot.go",
	"internal/agentupgradeport/infra_control.go",
}, transferErrorRoots...)

// Source discovery follows the owners, not their pre-slim parent paths. Values
// returned as sentinels or wrapped errors are inventoried below independently
// of the older Code:/Reason: syntax scanner.
// origin: hot-update S7 plan §7 / §11.2 / T50
// gate-control: TestTransferErrorScannerControls
var transferErrorRoots = []string{
	"internal/jsstream", "internal/xferworker", "internal/xfersend", "internal/xferreceive",
	"internal/xfercourier", "internal/xfertrack", "internal/xferfinal", "internal/xferledger",
	"internal/xferhistory", "internal/xferbucket", "internal/xfergc", "internal/xfercontrol",
	"internal/xferclient", "internal/xferimage", "internal/xferauth", "internal/xferbridge", "internal/xferaudit",
}

// Pin the moved emitters separately so an unrelated unresolved integration site
// cannot obscure loss of coverage when a broker responsibility changes owners.
// origin: S7 internal review BROKER-SLIM-2 F1
func TestBrokerRuntimeErrorEmittersStayCovered(t *testing.T) {
	codes, unresolved := scanTree(t, repoRoot(t), scannedTrees)
	for _, path := range []string{
		"internal/brokerupgrade/brokerruntime/cluster_grow_trigger.go",
		"internal/brokerupgrade/brokerruntime/cluster_grow_cutover.go",
		"internal/brokerupgrade/brokerruntime/cluster_upgrade_trigger.go",
	} {
		count := 0
		for _, code := range codes {
			if !strings.HasPrefix(code.site, path+":") {
				continue
			}
			count++
			if _, ok := brokerCodeExitClasses[code.code]; ok {
				continue
			}
			if _, ok := unclassifiedCodeAllowlist[code.code]; !ok {
				t.Errorf("%s: moved reply code %q has no exit class", code.site, code.code)
			}
		}
		if count == 0 {
			t.Errorf("%s: moved emitter is absent from the live scanner", path)
		}
		for site := range unresolved {
			if strings.HasPrefix(site, path+":") && unresolvedCodeSites[site] == "" {
				t.Errorf("%s: moved dynamic code lacks its original site receipt", site)
			}
		}
	}
}

// TestErrorCodeCoverage is the gate: every statically resolvable emitted code
// must either have an exit class or be on the allowlist with a reason.
// origin: S7 internal review ledgers6 F1 (error-code unresolved-sites)
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
		if _, classified := runFailureExitClasses[code]; classified {
			continue // lifecycle Reason emitted after attach, not an admission refusal
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
// origin: S7 internal review ledgers6 F2 (error-code stale-sites)
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
	// Moving the owner must move both discovery directions. Inject an unknown
	// code into each real destination: declarations or classifier strings in
	// some other package cannot stand in for these emission sites.
	// origin: S7 internal review ledgers6 F3 (error-code missing-emitter-keys)
	t.Run("moved_owner_roots", func(t *testing.T) {
		for _, path := range []string{"internal/brokerupgrade/agent_entry.go", "internal/agentupgrade/image_policy.go",
			"internal/agentupgradeport/runtime_snapshot.go", "internal/agentupgradeport/infra_control.go"} {
			t.Run(path, func(t *testing.T) {
				root := t.TempDir()
				abs := filepath.Join(root, path)
				if err := os.MkdirAll(filepath.Dir(abs), 0o700); err != nil {
					t.Fatal(err)
				}
				const code = "selfcheck_moved_owner_unclassified"
				src := `package owner; func fail(dynamic string) { _ = R{Code: "selfcheck_moved_owner_unclassified"}; _ = R{Code: dynamic} }`
				if err := os.WriteFile(abs, []byte(src), 0o600); err != nil {
					t.Fatal(err)
				}
				codes, unresolved := scanTree(t, root, scannedTrees)
				if len(codes) != 1 || codes[0].code != code || len(unresolved) != 1 || unresolved[path+":fail#1"] != path {
					t.Fatalf("moved owner disappeared: codes=%v unresolved=%v", codes, unresolved)
				}
				if _, classified := brokerCodeExitClasses[code]; classified {
					t.Fatal("unknown emitter control unexpectedly has an exit class")
				}
				if _, allowed := unclassifiedCodeAllowlist[code]; allowed {
					t.Fatal("unknown emitter control unexpectedly has an exemption")
				}
			})
		}
	})
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
		{
			// origin: hot-update S4 plan §9.2 / N1-REFUSAL
			form: "8: literal Reason with upgrade detail",
			src: `package x
type C struct{ Reason string }
func f() C { return C{Reason: "try_again: agent_upgrading: image switch in progress; retry"} }`,
			want: "try_again",
		},
		{
			// Code is read verbatim by expose/transfer, unlike RunChunk.Reason.
			// origin: hot-update S4 plan §9.2 / N1-REFUSAL
			form: "1: Code with colon remains an exact token",
			src: `package x
type R struct{ Code string }
func f() R { return R{Code: "selfcheck_literal: detail"} }`,
			want: "selfcheck_literal: detail",
		},
		{
			// origin: hot-update S4 plan §2.2 / T49
			form: "8: PTY input overflow Reason",
			src: `package x
type C struct{ Reason string }
func f() C { return C{Reason: "pty_input_overflow"} }`,
			want: "pty_input_overflow",
		},
		{
			// origin: hot-update S4 plan §10 / T49
			form: "finite process refusal mapper return",
			src: `package x
const code = "selfcheck_process_refusal"
func processRefusalCode(err error) string { if err != nil { return code }; return "" }`,
			want: "selfcheck_process_refusal",
		},
		{
			// origin: hot-update S6 plan §9.3 / T53 / GATES-S6-E
			form: "moved finite cluster refusal mapper return",
			src: `package x
const code = "selfcheck_cluster_refusal"
func ClusterCodeFor(err error) string { if err != nil { return code }; return "" }`,
			want: "selfcheck_cluster_refusal",
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
	// A newly dynamic mapper return cannot disappear from both result sets.
	// origin: hot-update S4 plan §10 / T49
	t.Run("dynamic_process_refusal_is_unresolved", func(t *testing.T) {
		dir := t.TempDir()
		src := `package x
func processRefusalCode(err error) string { return err.Error() }`
		if err := os.WriteFile(filepath.Join(dir, "sample.go"), []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
		_, unresolved := scanTree(t, dir, []string{"."})
		if _, found := unresolved["sample.go:processRefusalCode#1"]; !found || len(unresolved) != 1 {
			t.Fatalf("dynamic process mapper return vanished: %v", unresolved)
		}
	})
	// Exercise exact sets and object identity. A partially resolvable local
	// is unresolved in its entirety: never sign just its known branches.
	// origin: hot-update S6 plan §9.3 / T53 / GATES-S6-E
	t.Run("finite_lock_codes", func(t *testing.T) {
		cases := []struct {
			name, body string
			want       []string
			unresolved int
		}{
			{"finite", `code := first; if b { code = second }; return R{Code: code}`, []string{"first", "second"}, 0},
			{"shadowed_branches", `if b { code := first; if b { code = second }; return R{Code: code} }; code := third; return R{Code: code}`, []string{"first", "second", "third"}, 0},
			{"declared_local", `var code = first; if b { code = second }; return R{Code: code}`, []string{"first", "second"}, 0},
			{"dynamic_assignment", `code := first; if b { code = dynamic() }; return R{Code: code}`, nil, 1},
			{"dynamic_closure", `code := first; mutate := func(){ code = dynamic() }; mutate(); return R{Code: code}`, nil, 1},
			{"address_escape", `code := first; mutate(&code); return R{Code: code}`, nil, 1},
			{"parenthesized_escape", `code := first; mutate(&(code)); return R{Code: code}`, nil, 1},
			{"shadowed_constant", `code := first; if b { second := dynamic(); code = second }; return R{Code: code}`, nil, 1},
			{"field_named_like_constant", `code := first; code = value.second; return R{Code: code}`, nil, 1},
			{"tuple_result", `code := first; code, b = dynamicPair(); return R{Code: code}`, nil, 1},
			{"range_assignment", `code := first; for _, code = range codes {}; return R{Code: code}`, nil, 1},
			{"compound_assignment", `code := first; code += second; return R{Code: code}`, nil, 1},
			{"unknown_constant", `code := first; if b { code = CodeUnknown }; return R{Code: code}`, nil, 1},
			{"parameter", `return R{Code: parameter}`, nil, 1},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				dir := t.TempDir()
				src := "package x\nconst first = \"first\"; const second = \"second\"; const third = \"third\"; const code = \"global_code\"\ntype R struct{ Code string }\nfunc handleUpgradeLock(parameter string, b bool) R {" + tc.body + "}\n"
				if err := os.WriteFile(filepath.Join(dir, "sample.go"), []byte(src), 0600); err != nil {
					t.Fatal(err)
				}
				codes, unresolved := scanTree(t, dir, []string{"."})
				var got []string
				for _, code := range codes {
					got = append(got, code.code)
				}
				sort.Strings(got)
				if strings.Join(got, ",") != strings.Join(tc.want, ",") || len(unresolved) != tc.unresolved {
					t.Fatalf("codes=%v unresolved=%v; want codes=%v unresolved=%d", got, unresolved, tc.want, tc.unresolved)
				}
			})
		}
	})
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
// A1 reconciled the whole set as it grew brokerCodeExitClasses from 45 to 106 keys. Any later key
// without a current emitter must have a specific compatibility obligation documented below.

// staleClassificationKeys lists table keys that are deliberately kept without a live emitter. It is
// normally empty: a key with no emitter is nearly always a leftover. The rare legitimate case (a
// code a peer may still send from an older release) has to argue for itself in writing.
var staleClassificationKeys = map[string]string{
	// The grantless install handler now refuses before download/install. These
	// exact Code values still arrive from N-1 agents/brokers during rollout;
	// architecture §21.2 forbids retiring their CLI meaning with the producer.
	// upgrade_in_progress and image_abi_unsupported now also occur as S8 detail,
	// which is not proof of a current Code emitter. Keep each wire obligation
	// explicit rather than counting classifier/detail strings as emissions.
	// origin: S7 internal review ledgers6 F3 (error-code missing-emitter-keys)
	"download_failed":         "architecture §21.2 N-1 agents report transport/read download failure; retain retryable 75 until the adjacent-release window retires.",
	"download_http_retryable": "architecture §21.2 N-1 agents distinguish retryable HTTP mirror responses; retain 75 during rollout.",
	"download_http_status":    "architecture §21.2 N-1 agents report permanent non-2xx mirror responses; retain invalid-argument 64 during rollout.",
	"download_too_large":      "architecture §21.2 N-1 agents reject an oversized artifact; retain 64 during rollout.",
	"image_abi_unsupported":   "architecture §21.2 N-1 agents reject incompatible image readers with this Code (64); S8 uses it only as upgrade_rejected detail.",
	"install_failed":          "architecture §21.2 N-1 agents report a failed pre-exec install with this Code; retain operator-action 64 during rollout.",
	"self_path":               "architecture §21.2 N-1 agents cannot resolve their executable path before install/re-exec; retain invalid-target 64 during rollout.",
	"sha256_mismatch":         "architecture §21.2 N-1 agents report artifact digest mismatch; retain invalid-artifact 64 during rollout.",
	"sha256_required":         "architecture §21.2 N-1 agents require an artifact digest before installation; retain invalid-request 64 during rollout.",
	"smoke_failed":            "architecture §21.2 N-1 agents reject artifacts failing the pre-install version probe; retain 64 during rollout.",
	"upgrade_in_progress":     "architecture §21.2 N-1 agents refuse a second install while their marker is pending; retain retryable 75, independently of S8's same-named deferral detail.",
	"url_not_allowed_local":   "architecture §21.2 N-1 agents enforce their own URL allowlist after broker admission; retain operator-action 64 during rollout.",
	// origin: hot-update S4 plan §9.2 / N1-REFUSAL. S3 agents already send
	// this bare code; current ctl must still diagnose them during rollout.
	"agent_upgrading": "S3 agents emit this code; S4 legacy refusals use try_again with agent_upgrading detail for v0.6.0 ctl",
}

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
		"runFailureExitClasses": intKeysOf(runFailureExitClasses),
	}
}

// adminsockCodeConstants parses internal/adminsock/protocol.go for `CodeXxx = "literal"` declarations.
//
// This indirection is not optional. Two codes are emitted ONLY as `return adminsock.CodeNotAVoter` /
// `adminsock.CodeRemoveOwnsResources` from the error->code mapper in internal/brokercontrol/clusterstatus.go —
// form 5 (the forward scanner now explicitly scans this finite mapper), and whose literal
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
// The reverse classifier audit must follow the final S7/S8/S9 leaf owners,
// including aliases and function-return constants outside the old parent paths.
// The S10 union gate independently discovers these directories on disk; deleting
// a root is red even when another producer emits the same code. Local setup and
// observation errors are not promoted into the wire Code namespace.
// origin: hot-update S10 plan §12.2 / P29
var fullstackErrorLeafRoots = []string{
	"internal/agentupgrade",
	"internal/agentupgradeport",
	"internal/binaryupgrade",
	"internal/brokerupgrade",
	"internal/brokerupgradeport",
	"internal/clusterupgrade",
	"internal/infraartifact",
	"internal/infraattach",
	"internal/infraauth",
	"internal/infracli",
	// origin: hot-update S10 plan §12.2 / P29; S9 composition retains the actual error emitter root.
	"internal/infracompose",
	"internal/infracontract",
	"internal/infrahost",
	"internal/infralaunch",
	"internal/inframaint",
	"internal/infrapriv",
	"internal/infrareadiness",
	"internal/infrarepair",
	"internal/infraroot",
	"internal/infratransfer",
	"internal/infratransport",
	"internal/infraunit",
	"internal/infraupgrade",
	"internal/infrawitness",
	"internal/jsstream",
	"internal/upgradeartifact",
	"internal/upgradeauthority",
	"internal/upgradebudget",
	"internal/upgradecli",
	"internal/upgradecompat",
	"internal/upgradecut",
	"internal/upgradehost",
	"internal/upgradelegacy",
	"internal/upgradelock",
	"internal/upgradeobserve",
	"internal/upgradeport",
	"internal/upgradeprotocol",
	"internal/upgraderoll",
	"internal/upgradestage",
	"internal/upgradestore",
	"internal/upgradetx",
	"internal/upgradewire",
	"internal/xferagent",
	"internal/xferagentimage",
	"internal/xferaudit",
	"internal/xferauth",
	"internal/xferbridge",
	"internal/xferbucket",
	"internal/xfercapacity",
	"internal/xferclient",
	"internal/xfercontract",
	"internal/xfercontrol",
	"internal/xfercourier",
	"internal/xferfinal",
	"internal/xferforward",
	"internal/xfergc",
	"internal/xferhistory",
	"internal/xferid",
	"internal/xferimage",
	"internal/xferledger",
	"internal/xferproof",
	"internal/xferreceive",
	"internal/xfersend",
	"internal/xferserve",
	"internal/xfertrack",
	"internal/xferworker",
}

func emittedCodeUniverse(t *testing.T, root string) map[string]bool {
	t.Helper()
	codes, _ := scanTree(t, root, scannedTrees)
	universe := map[string]bool{}
	for _, c := range codes {
		universe[c.code] = true
	}
	constants := adminsockCodeConstants(t, root)
	forEachGoFile(t, root, scannedTrees, func(rel string, f *ast.File, fset *token.FileSet) {
		// A classifier cannot prove that its own legacy keys are still emitted.
		// origin: S7 internal review rv-n1compat F6
		if rel == "internal/xferclient/errors.go" {
			return
		}
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
	for code := range fullstackLeafCodeUniverse(t, root) {
		universe[code] = true
	}
	return universe
}

// Constants establish values, not emission. Only Code fields, finite code
// mapper bodies and returned Code constants contribute new leaf codes. This
// keeps legacy CLI classification maps and image deferral DETAILS from falsely
// proving that an N-1-only wire code is still emitted by the current product.
// origin: hot-update S10 plan §12.2 / P29
func fullstackLeafCodeUniverse(t *testing.T, root string) map[string]bool {
	t.Helper()
	files := map[string]*ast.File{}
	imports := map[string]map[string]string{}
	constants := map[string]string{}
	valueMaps := map[string]struct {
		path    string
		literal *ast.CompositeLit
	}{}
	forEachGoFile(t, root, append(append([]string{}, fullstackErrorLeafRoots...), "internal/proto", "internal/adminsock"), func(path string, file *ast.File, _ *token.FileSet) {
		files[path] = file
		imports[path] = map[string]string{}
		for _, imp := range file.Imports {
			pkg, _ := strconv.Unquote(imp.Path.Value)
			name := filepath.Base(pkg)
			if imp.Name != nil {
				name = imp.Name.Name
			}
			imports[path][name] = strings.TrimPrefix(pkg, "github.com/LinZiyang666/tether/")
		}
		for _, decl := range file.Decls {
			if gd, ok := decl.(*ast.GenDecl); ok && gd.Tok == token.VAR {
				for _, spec := range gd.Specs {
					vs := spec.(*ast.ValueSpec)
					for i, name := range vs.Names {
						if i < len(vs.Values) {
							if literal, ok := vs.Values[i].(*ast.CompositeLit); ok {
								if _, isMap := literal.Type.(*ast.MapType); isMap {
									valueMaps[filepath.Dir(path)+"."+name.Name] = struct {
										path    string
										literal *ast.CompositeLit
									}{path, literal}
								}
							}
						}
					}
				}
			}
		}
	})
	resolve := func(path string, expr ast.Expr) string {
		switch expr := expr.(type) {
		case *ast.BasicLit:
			if expr.Kind == token.STRING {
				value, _ := strconv.Unquote(expr.Value)
				return value
			}
		case *ast.Ident:
			return constants[filepath.Dir(path)+"."+expr.Name]
		case *ast.SelectorExpr:
			if alias, ok := expr.X.(*ast.Ident); ok {
				return constants[imports[path][alias.Name]+"."+expr.Sel.Name]
			}
		}
		return ""
	}
	// Alias chains are finite. Stop at the fixed point rather than depending
	// on filesystem iteration order or accepting a silently unresolved alias.
	for changed := true; changed; {
		changed = false
		for path, file := range files {
			for _, decl := range file.Decls {
				gd, ok := decl.(*ast.GenDecl)
				if !ok || gd.Tok != token.CONST {
					continue
				}
				for _, spec := range gd.Specs {
					vs := spec.(*ast.ValueSpec)
					for i, name := range vs.Names {
						key := filepath.Dir(path) + "." + name.Name
						if i < len(vs.Values) && constants[key] == "" {
							if value := resolve(path, vs.Values[i]); value != "" {
								constants[key], changed = value, true
							}
						}
					}
				}
			}
		}
	}
	universe := map[string]bool{}
	machineCode := regexp.MustCompile(`^[a-z][a-z0-9]*_[a-z0-9_]+$`)
	for path, file := range files {
		if strings.Contains(path, "/contractstub/") {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Name.Name == "ExitCode" || fn.Name.Name == "ErrorExit" {
				continue
			}
			mapper := strings.Contains(fn.Name.Name, "Code") || strings.HasPrefix(fn.Name.Name, "code")
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				add := func(expr ast.Expr) {
					if code := resolve(path, expr); code != "" {
						universe[code] = true
					}
				}
				if mapper {
					key := ""
					switch reference := node.(type) {
					case *ast.Ident:
						key = filepath.Dir(path) + "." + reference.Name
					case *ast.SelectorExpr:
						if alias, ok := reference.X.(*ast.Ident); ok {
							key = imports[path][alias.Name] + "." + reference.Sel.Name
						}
					}
					if table := valueMaps[key]; table.literal != nil {
						for _, element := range table.literal.Elts {
							if pair, ok := element.(*ast.KeyValueExpr); ok {
								if code := resolve(table.path, pair.Value); machineCode.MatchString(code) {
									universe[code] = true
								}
							}
						}
					}
				}
				switch node := node.(type) {
				case *ast.CompositeLit:
					if typ, ok := node.Type.(*ast.SelectorExpr); ok && typ.Sel.Name == "AuditTransfer" {
						if alias, ok := typ.X.(*ast.Ident); ok && imports[path][alias.Name] == "internal/schema" {
							return false // durable history outcome, a separate Code namespace
						}
					}
				case *ast.KeyValueExpr:
					if key, ok := node.Key.(*ast.Ident); ok && key.Name == "Code" {
						add(node.Value)
					}
				case *ast.ReturnStmt:
					for _, expr := range node.Results {
						if id, ok := expr.(*ast.Ident); ok && strings.HasPrefix(id.Name, "Code") {
							add(expr)
						}
						if selector, ok := expr.(*ast.SelectorExpr); ok && strings.HasPrefix(selector.Sel.Name, "Code") {
							add(expr)
						}
					}
				case *ast.BasicLit:
					if mapper && node.Kind == token.STRING {
						if code, _ := strconv.Unquote(node.Value); machineCode.MatchString(code) {
							universe[code] = true
						}
					}
				}
				return true
			})
		}
	}
	return universe
}

func fullstackTransferClassifier(t *testing.T, root string) map[string]bool {
	t.Helper()
	path := "internal/xferclient/errors.go"
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	classes, err := transferEmittedCodes(map[string]string{path: string(raw)}, transferCodeConstants(t, root))
	if err != nil {
		t.Fatal(err)
	}
	return classes
}

func fullstackLeafCodeProblem(emitted, transfer map[string]bool) error {
	var unknown []string
	for code := range emitted {
		_, broker := brokerCodeExitClasses[code]
		_, run := runFailureExitClasses[code]
		_, infra := infracli.ExitClasses[code]
		_, upgrade := upgradewire.ErrorExit(code)
		// Accepted transfer results use their own explicit 74-class path;
		// broker admission tables deliberately cannot express that class.
		if !broker && !run && !infra && !upgrade && !transfer[code] {
			unknown = append(unknown, code)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("unclassified actual leaf code %s", strings.Join(unknown, "\nunclassified actual leaf code "))
	}
	return nil
}

// origin: hot-update S10 plan §12.2 / P29
// gate-control: TestFullstackLeafCodeControls
func TestFullstackLeafErrorEmittersAreClassified(t *testing.T) {
	emitted := fullstackLeafCodeUniverse(t, repoRoot(t))
	if len(emitted) < 20 {
		t.Fatal("empty actual leaf error-code union")
	}
	if err := fullstackLeafCodeProblem(emitted, fullstackTransferClassifier(t, repoRoot(t))); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S10 plan §12.2 / P29
func TestFullstackLeafCodeControls(t *testing.T) {
	root, real := t.TempDir(), repoRoot(t)
	forEachGoFile(t, real, append(append([]string{}, fullstackErrorLeafRoots...), "internal/proto", "internal/adminsock"), func(path string, _ *ast.File, _ *token.FileSet) {
		raw, err := os.ReadFile(filepath.Join(real, path))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(root, filepath.Dir(path)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, path), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	})
	check := func() error {
		return fullstackLeafCodeProblem(fullstackLeafCodeUniverse(t, root), fullstackTransferClassifier(t, root))
	}
	if err := check(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "internal/infrarepair/gate.go")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, source := range map[string]string{
		"field":      `type inventoryRefusal struct { Code string }; func inventoryRefuse() inventoryRefusal { return inventoryRefusal{Code: "unregistered_fullstack_code"} }`,
		"alias":      `const inventoryCode = "unregistered_fullstack_code"; const inventoryAlias = inventoryCode; type inventoryRefusal struct { Code string }; func inventoryRefuse() inventoryRefusal { return inventoryRefusal{Code: inventoryAlias} }`,
		"return":     `func inventoryCodeFor() string { return "unregistered_fullstack_code" }`,
		"map-return": `func inventoryCodeFor() string { return map[string]string{"failure": "unregistered_fullstack_code"}["failure"] }`,
		"global-map": `var inventoryCodes = map[string]string{"failure": "unregistered_fullstack_code"}; func inventoryCodeFor() string { return inventoryCodes["failure"] }`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, append(append([]byte{}, original...), []byte("\n"+source+"\n")...), 0o600); err != nil {
				t.Fatal(err)
			}
			red := check()
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatal(err)
			}
			if red == nil || !strings.Contains(red.Error(), "unclassified actual leaf code unregistered_fullstack_code") {
				t.Fatalf("actual on-disk %s emitter escaped: %v", name, red)
			}
			if err := check(); err != nil {
				t.Fatalf("restore stays red: %v", err)
			}
			t.Logf("ASSERTION_RED: %v; restored GREEN", red)
		})
	}
	// A local setup message or unused declaration is not a wire emission.
	positive := "\nconst unusedInventoryCode = \"unused_fullstack_code\"\nvar localInventoryError = errors.New(\"local_setup_error\")\n"
	if err := os.WriteFile(path, append(append([]byte{}, original...), []byte(positive)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := check(); err != nil {
		t.Fatalf("local/unused error was promoted into the wire namespace: %v", err)
	}
}

// TestClassificationTableKeysStillHaveEmitters is the reverse reconciliation.
// origin: S7 internal review ledgers6 F3 (error-code missing-emitter-keys)
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
	tables := classificationTables()
	// Omitting the lifecycle table would leave Reason mappings unchecked in
	// the reverse direction while the older three tables still looked healthy.
	// origin: hot-update S4 plan §10 / T49
	for _, name := range []string{"brokerCodeHints", "brokerCodeExitClasses", "runFailureReasons", "runFailureExitClasses"} {
		if _, present := tables[name]; !present {
			t.Fatalf("classification table %s is absent from reverse coverage", name)
		}
	}
	if len(tables) != 4 {
		t.Fatalf("reverse coverage has %d classification tables, want 4", len(tables))
	}
	total := 0
	for table, keys := range tables {
		if len(keys) == 0 {
			t.Errorf("classification table %s is empty", table)
		}
		total += len(keys)
	}
	if total < 100 {
		t.Errorf("the four classification tables hold only %d keys between them; A1 left them at ~151", total)
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

// Resolve contract constants by import identity before collecting live error
// expressions. Declarations alone do not count as emitters. Walking function
// bodies and initialized variables also sees errors.New, fmt.Errorf wrappers,
// callback closures, string returns and exported sentinel aliases.
func transferEmittedCodes(sources map[string]string, constants map[string]string) (map[string]bool, error) {
	if len(sources) == 0 || len(constants) == 0 {
		return nil, fmt.Errorf("empty transfer error scanner root")
	}
	result := map[string]bool{}
	for path, source := range sources {
		file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
		if err != nil {
			return nil, err
		}
		imports := map[string]string{}
		for _, imp := range file.Imports {
			pkg, _ := strconv.Unquote(imp.Path.Value)
			name := filepath.Base(pkg)
			if imp.Name != nil {
				name = imp.Name.Name
			}
			imports[name] = pkg
		}
		// A structured-log KEY ("transfer_id", id) names a field, not a refusal:
		// keys sit at the odd argument positions of a logger method. Values and
		// every other literal still count, so a code logged as a value or
		// returned as an error remains an emitter.
		// origin: INTEG-D cmd/tether race suite (xferledger/filetransfer slog keys)
		logKeys := map[*ast.BasicLit]bool{}
		collect := func(node ast.Node) {
			ast.Inspect(node, func(node ast.Node) bool {
				code := ""
				switch value := node.(type) {
				case *ast.CallExpr:
					if sel, ok := value.Fun.(*ast.SelectorExpr); ok {
						switch sel.Sel.Name {
						case "Debug", "Info", "Warn", "Error":
							for i := 1; i < len(value.Args); i += 2 {
								if key, ok := value.Args[i].(*ast.BasicLit); ok {
									logKeys[key] = true
								}
							}
						}
					}
				case *ast.BasicLit:
					if value.Kind == token.STRING && !logKeys[value] {
						text, _ := strconv.Unquote(value.Value)
						if strings.HasPrefix(text, "transfer_") && regexp.MustCompile(`^transfer_[a-z_]+$`).MatchString(text) {
							code = text
						}
					}
				case *ast.SelectorExpr:
					if alias, ok := value.X.(*ast.Ident); ok {
						code = constants[imports[alias.Name]+"."+value.Sel.Name]
					}
				case *ast.Ident:
					if imports["."] != "" {
						code = constants[imports["."]+"."+value.Name]
					}
				}
				if code != "" {
					result[code] = true
				}
				return true
			})
		}
		for _, decl := range file.Decls {
			switch node := decl.(type) {
			case *ast.FuncDecl:
				if node.Body != nil {
					collect(node.Body)
				}
			case *ast.GenDecl:
				if node.Tok == token.VAR {
					collect(node)
				}
			}
		}
	}
	return result, nil
}

func checkTransferExitClosure(emitted map[string]bool, classified map[string]int, exit func(string) int) error {
	if len(emitted) == 0 || len(classified) == 0 || exit == nil {
		return fmt.Errorf("empty transfer emitter/classifier/CLI exit root")
	}
	for code := range emitted {
		class, ok := classified[code]
		if !ok || class != exit(code) {
			return fmt.Errorf("%s: emitter -> classifier -> CLI exit is not closed", code)
		}
	}
	for code := range classified {
		if !emitted[code] {
			return fmt.Errorf("%s: dead transfer classifier entry", code)
		}
	}
	return nil
}

// origin: hot-update S7 plan §7 / §11.2 / T50
func TestTransferErrorEmittersReachCLIExit(t *testing.T) {
	root := repoRoot(t)
	constants := transferCodeConstants(t, root)

	sources := map[string]string{}
	for _, path := range append(append([]string{}, transferErrorRoots...), "internal/transferpath", "internal/filetransfer") {
		entries, err := os.ReadDir(filepath.Join(root, path))
		if err != nil {
			t.Errorf("empty/missing transfer error scanner root %s: %v", path, err)
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			rel := path + "/" + entry.Name()
			bytes, err := os.ReadFile(filepath.Join(root, rel))
			if err != nil {
				t.Fatal(err)
			}
			// The classifier is the middle set, never its own emitter proof.
			if rel != "internal/xferclient/errors.go" {
				sources[rel] = string(bytes)
			}
		}
	}
	emitted, err := transferEmittedCodes(sources, constants)
	if err != nil {
		t.Fatal(err)
	}
	leafSource, err := os.ReadFile(filepath.Join(root, "internal/xferclient/errors.go"))
	if err != nil {
		t.Fatalf("P25 classifier: %v", err)
	}
	leaf, err := transferEmittedCodes(map[string]string{"internal/xferclient/errors.go": string(leafSource)}, constants)
	if err != nil {
		t.Fatal(err)
	}
	// The CLI exit is the production transfer port's classifier, the one
	// RunTransferCommand reaches through TransferLeafError. It assigns the
	// accepted-result class 74 after a send; brokerCodeExitClasses may not
	// (74 is presentation-only, TestExitUnsettledIsPresentationOnly), so the
	// broker-code table is not this command's exit path.
	// origin: hot-update S7 plan §7 / T50; INTEG-D cmd/tether race suite
	cliExit := func(code string) int {
		return classifyExit(adaptTransferCommandError(cli.TransferLeafError(transferCLI, &xferclient.Error{Code: code, Phase: x.PhasePrepared})))
	}
	classified := map[string]int{}
	for code := range leaf {
		class := xferclient.ExitCode(&xferclient.Error{Code: code, Phase: x.PhasePrepared})
		if class != 64 && class != 74 && class != 75 && class != 77 {
			t.Errorf("%s: transfer refusal has invalid CLI exit class %d", code, class)
		}
		classified[code] = class
		// origin: S7 internal review rv-n1compat F6
		// Compare the actual leaf to the independent wire-refusal table too.
		// Only DELETING and unresolved presentation have distinct contracts.
		if code != x.CodeSessionDeleting && class != 74 && class != brokerCodeExitClass(code) {
			t.Errorf("%s: leaf exit %d differs from wire-refusal class %d", code, class, brokerCodeExitClass(code))
		}
	}
	if err := checkTransferExitClosure(emitted, classified, cliExit); err != nil {
		t.Error(err)
	}
}

// origin: hot-update S7 plan §7 / §11.2 / T50
func TestTransferErrorScannerControls(t *testing.T) {
	constants := map[string]string{"github.com/LinZiyang666/tether/internal/xfercontract.CodeResultUnknown": "transfer_result_unknown"}
	root := t.TempDir()
	path := filepath.Join(root, "emitter_other.go")
	for _, source := range []string{
		`package leaf;func fail(){return R{Code:"transfer_result_unknown"}}`,
		`package leaf;import c "github.com/LinZiyang666/tether/internal/xfercontract";func fail(){return c.CodeResultUnknown}`,
		`package leaf;import c "github.com/LinZiyang666/tether/internal/xfercontract";var Err=errors.New(c.CodeResultUnknown);func fail(){return fmt.Errorf("%w",Err)}`,
		`package leaf;import . "github.com/LinZiyang666/tether/internal/xfercontract";func fail(){f:=func(){return CodeResultUnknown};return f()}`,
		// A structured-log key is not an emitter, but a code logged as a VALUE still is: with the key
		// counted the set has two members, with the value dropped it has none.
		// origin: INTEG-D cmd/tether race suite (log-key exclusion control)
		`package leaf;func fail(){logger.Warn("refused","transfer_id",id,"code","transfer_result_unknown")}`,
	} {
		if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		emitted, err := transferEmittedCodes(map[string]string{path: string(raw)}, constants)
		if err != nil || !emitted["transfer_result_unknown"] || len(emitted) != 1 {
			t.Fatalf("on-disk sentinel/alias/wrapper emitter control: %v %v", emitted, err)
		}
		classified := map[string]int{"transfer_result_unknown": 74}
		if err := checkTransferExitClosure(emitted, classified, func(string) int { return 74 }); err != nil {
			t.Fatal(err)
		}
		if checkTransferExitClosure(emitted, nil, func(string) int { return 74 }) == nil || checkTransferExitClosure(emitted, classified, func(string) int { return 0 }) == nil || checkTransferExitClosure(nil, classified, func(string) int { return 74 }) == nil {
			t.Fatal("missing classifier/unknown exits success/empty emitter root escaped")
		}
	}
	if len(transferErrorRoots) < 17 {
		t.Fatal("transfer error scanner lost a leaf")
	}
}
