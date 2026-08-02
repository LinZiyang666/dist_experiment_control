package proto

// Machine-readable reply codes carried on the NATS control plane (every
// proto.*Resp has a Code field). They are WIRE VALUES: the string bytes are
// part of the protocol, so a mixed-version fleet (old ctl + new broker, or the
// reverse) depends on them staying byte-identical. Renaming a constant is free;
// changing its VALUE is a wire break.
//
// Why this file exists (batch-A A1). The codes used to live as bare string
// literals scattered across internal/broker, internal/agent and cmd/tether,
// with no compile-time link between the emitter and cmd/tether's
// brokerCodeExitClasses / brokerCodeHints tables. The tables had drifted in
// both directions — codes were emitted that no table knew (falling through to
// exit 70, which docs/usage.md §9.13 tells automation to RETRY), and table keys
// survived whose emitter had been deleted.
//
// Note what is and is not GUARDED, as opposed to merely observed (review F-10):
// TestErrorCodeCoverage enforces the first direction only — every emitted code
// must be classified. The reverse (no table key without an emitter) is checked
// for the allowlist alone, by TestAllowlistEntriesStillHaveEmitters. A stale
// classification for a deleted code is therefore still possible; it is
// harmless (nothing looks it up) which is why it was left, not because it is
// covered.
//
// WHAT THIS FILE IS — AND IS NOT (batch-A review F-11/M9)
//
// It is a REGISTRY: the authoritative written-down list of the NATS-wire codes,
// with each one's meaning and exit-class rationale next to it. It is NOT yet a
// single source of truth in the mechanical sense: most emitters still write the
// literal directly rather than referencing these constants, so changing a
// constant's value here does not change what the broker sends.
//
// An earlier version of this comment claimed SSOT status outright. That was
// wrong in the way this batch has been repeatedly caught being wrong — asserting
// a guarantee the code does not provide. Two things do hold, and are tested:
//
//   - every constant's value corresponds to a literal production really emits,
//     or to a call site that already references the constant
//     (TestEveryDeclaredCodeHasAProductionEmitter);
//   - codes shared with internal/adminsock carry identical values in both
//     packages (TestWireCodeNamespacesAgree).
//
// Migrating the emitters onto these constants — which is what would make the
// SSOT claim true — is deliberate follow-up work across broker, agent and
// spawnsafe, not something to assert prematurely here.
//
// Scope boundary — there are TWO code namespaces in this repo and this file is
// only one of them:
//
//   - internal/proto (this file): codes on the NATS control plane, set on
//     proto.*Resp.Code and read by cmd/tether over NATS.
//   - internal/adminsock (protocol.go): codes on the LOCAL unix admin socket,
//     set on adminsock.Response.Code. That package is the SSOT for its own
//     namespace and is NOT duplicated here.
//
// proto MUST NOT import adminsock (the dependency runs the other way), so the
// handful of codes that legitimately travel on BOTH transports are declared in
// both packages. TestWireCodeNamespacesAgree pins those to identical values —
// see the "shared with adminsock" block below.
const (
	// --- session / membership / ownership ---

	// CodeNotOwner / CodeNotOwnerOrCreator / CodeNotAMember are the ownership
	// and membership refusals. cmd/tether maps all three to exit 77 (EX_NOPERM).
	CodeNotOwner          = "not_owner"
	CodeNotOwnerOrCreator = "not_owner_or_creator"
	CodeNotAMember        = "not_a_member"
	// CodeSessionNotFound / CodeSessionNotFoundOrDeleting: the session row is
	// gone, or is mid-delete. Operator-actionable, never worth a blind retry.
	CodeSessionNotFound           = "session_not_found"
	CodeSessionNotFoundOrDeleting = "session_not_found_or_deleting"
	// CodeActorInvalid: the actor field on the subject did not parse into a
	// fingerprint. A well-formed ctl cannot produce this — it is version skew
	// or a tether bug, not operator input.
	CodeActorInvalid = "actor_invalid"

	// --- node ---

	CodeNodeNotFound = "node_not_found"
	CodeNodeOffline  = "node_offline"
	// CodeAgentNoResponders: the request reached the broker but no agent
	// answered on the node subject. Self-healing once the agent reconnects.
	CodeAgentNoResponders = "agent_no_responders"
	// CodeAgentMalformedResp: an agent replied with something we could not
	// decode. Version skew or an agent bug — ours to fix, not the operator's.
	CodeAgentMalformedResp = "agent_malformed_resp"

	// --- expose / public port allocation ---

	CodeNameTaken        = "name_taken"
	CodeNameReserved     = "name_reserved"
	CodePortTaken        = "port_taken"
	CodePortExhausted    = "port_exhausted"
	CodePortOutOfBand    = "port_out_of_band"
	CodeLocalPortInvalid = "local_port_invalid"
	// CodeAlreadyRevoked: the proxy subscription/expose was already revoked.
	// Terminal by construction — retrying cannot change it.
	CodeAlreadyRevoked = "already_revoked"

	// --- proxy subscriptions (P13) ---

	CodeSubNameInvalid = "sub_name_invalid"
	CodeSubNameTaken   = "sub_name_taken"
	CodeSubNotFound    = "sub_not_found"

	// --- upgrade / artifact fetch ---

	CodeSha256Invalid  = "sha256_invalid"
	CodeSha256Mismatch = "sha256_mismatch"
	CodeURLNotAllowed  = "url_not_allowed"
	// CodeURLNotAllowedLocal: the artifact URL resolved to a loopback/private
	// address on the agent. A deliberate SSRF-shaped refusal, not a transient.
	CodeURLNotAllowedLocal = "url_not_allowed_local"
	// CodeProtoBumpRequiresReinstall: the peer's ProtoVersion differs across a
	// breaking boundary. Upgrading in place cannot fix it — a ProtoVersion bump
	// is an epoch change, the one permitted compatibility break of the N-1
	// window (requirements §6.7; the bumper's obligations live in
	// distributed-broker-architecture §21.4). Reinstall, do not upgrade.
	CodeProtoBumpRequiresReinstall = "proto_bump_requires_reinstall"
	// CodeSmokeFailed: the downloaded binary failed the pre-install smoke gate
	// (could not exec, or `version` printed no parsable release tag). The disk
	// was NOT touched. The artifact is bad for every node it would be sent to
	// (wrong arch, truncated, not a tether binary) — not retryable, and
	// `node upgrade --all` must abort the fleet rather than fan it out.
	CodeSmokeFailed = "smoke_failed"
	// CodeUpgradeInProgress: an upgrade marker in state "pending" exists — a
	// prior upgrade on this agent has staged its binary but has not yet
	// committed (re-registered as the new version) or rolled back. Installing
	// over it would clobber the only known-good binary in the prev slot.
	// Transient: resolves as soon as the in-flight upgrade commits or rolls
	// back (bounded by the register deadline).
	CodeUpgradeInProgress = "upgrade_in_progress"
	// CodeFrpcFailed: the agent's data-plane helper failed to come up.
	// Needs a human to read the agent log; blind retry does not help.
	CodeFrpcFailed = "frpc_failed"

	// --- transport / encoding ---

	// CodeJSONParse: the broker could not parse our request. Always a tether
	// bug (or a mixed-version fleet), never operator input.
	CodeJSONParse = "json_parse"
	// CodeSubjectMalformed: the request subject did not match the expected
	// grammar. Same class as CodeJSONParse.
	CodeSubjectMalformed = "subject_malformed"

	// --- cluster / JetStream (member-facing half) ---
	//
	// NOTE: two codes in this namespace predate this file and keep their
	// original homes, because their doc comments carry load-bearing protocol
	// rules that belong next to the messages they describe:
	//
	//   - CodeLeaderUnavailable ("leader_unavailable")  messages.go:227
	//   - ReasonHomeCatchingUp  ("home_catching_up")    messages.go:216
	//
	// They are NOT redeclared here (that would be a second SSOT). The coverage
	// test scans the whole package, so they are covered either way.

	// CodeJetStreamUnavailable: this broker has no JetStream at all. That is a
	// config/boot state, NOT a transient — a human must fix the deployment.
	CodeJetStreamUnavailable = "jetstream_unavailable"
	// CodeDataplaneNotConverged: the control plane committed a rehome but the
	// agents have not confirmed the new home yet. EX_TEMPFAIL, not a bug: the
	// broker keeps re-delivering, so re-running the verb is correct. Crucially
	// NOT success — `cluster drain` returning 0 on an unconverged data plane
	// was a release blocker (R8a P1). cmd/tether pins this value from its side
	// in TestDataplaneNotConvergedCodeIsWireStable.
	CodeDataplaneNotConverged = "dataplane_not_converged"

	// --- transfer ---

	// CodeTransferBudgetExceeded (batch C / C2): the broker's tier-B watchdog fired before the
	// receiver's finalization signal arrived. It replaces CodeAgentNoResponders on THAT path only,
	// because that attribution was wrong in the common case: the agent may have been reachable and
	// transferring the entire time — what ran out was the broker's budget, which before batch C was a
	// fixed 5 minutes that did not derive from the 2 GiB size ceiling it had to cover TWICE. Sending
	// the operator to "check the agent is running and connected" for a transfer that was healthy but
	// slow is the misdirection this code exists to end. EX_TEMPFAIL: retrying a smaller file, or the
	// same file on a faster link, genuinely works. The real no-responders emitters (expose, upgrade,
	// the cluster upgrade trigger) keep CodeAgentNoResponders.
	CodeTransferBudgetExceeded = "transfer_budget_exceeded"

	// --- shared with internal/adminsock ---
	//
	// These wire strings travel on BOTH the NATS control plane and the local
	// admin socket. proto cannot import adminsock (dependency direction), so
	// each package declares its own constant and TestWireCodeNamespacesAgree
	// asserts the values are identical. Do not "fix" this by deleting one side.

	// CodeStoreError is the catch-all storage failure. The DETAIL (the SQLite
	// error text) must stay in the broker log and never ride the wire — see
	// docs/reviews/batch-a-plan.md decision D8.
	CodeStoreError = "store_error"
	// CodeBadRequest is operator input the broker refuses to act on (malformed
	// args, a directory that already exists, an unparseable --since). Terminal:
	// exit 64, never the unclassified 70 that automation is told to retry.
	CodeBadRequest = "bad_request"
)
