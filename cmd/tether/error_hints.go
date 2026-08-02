package main

import (
	"fmt"
	"github.com/LinZiyang666/tether/internal/proto"
	"strings"
)

// brokerCodeHint translates a broker-reply Code (architecture-stable
// identifier) into one user-facing sentence the operator can act on.
// Used by every command that surfaces a `Code+Error` reply pair so
// the same `not_owner` from `expose-rm` and `node upgrade` reads
// the same way to the user. Returns "" when no hint is registered;
// callers then fall back to the raw code+error pair.
//
// We keep this in cmd/tether (not a deeper internal package) on
// purpose: the audience is the human running the CLI, not other
// daemons. Broker-internal callers should keep using the bare
// codes for log + audit.
var brokerCodeHints = map[string]string{
	// origin: line-2 §12 Y2. `node upgrade` download failures, split out of the single download_failed
	// catch-all. These are UpgradeForwardedResp.Code values (NOT RunChunk.Reason), which is why they are
	// here and not in runFailureReasons — see the note there.
	"download_http_status":    "the upgrade mirror answered with a non-2xx status; check the --url you passed (a typo'd path returns 404 and will do so on every retry).",
	"smoke_failed":            "the downloaded binary passed sha256 but failed the pre-install smoke gate (could not exec, or `version` printed no release tag) — wrong architecture, truncated artifact, or not a tether binary. Nothing on disk was changed. Fix the --url artifact; the same tarball will fail on every node.",
	"upgrade_in_progress":     "a previous upgrade on this node staged its binary and is still inside its register deadline (commit-or-rollback resolves within ~2 minutes). Retry this node after it settles; `node upgrade --all` skips it and keeps going — unless it is the CANARY, whose failure aborts the fleet by design.",
	"download_http_retryable": "the upgrade mirror is temporarily unavailable or asks for a retry (408/421/425/429/500/502/503/504) — the URL and the artifact are fine. Retry with backoff; if the reply carried a Retry-After it is in the error text. `node upgrade --all` skips this node and keeps going rather than aborting the fleet.",
	"download_too_large":      "the upgrade tarball is larger than the agent's ceiling; publish a smaller artifact or raise the limit — the same URL will be the same size next time.",
	// Membership / ownership / lifecycle
	"not_owner":                     "only the session owner can do this; ask the owner to run it.",
	"not_owner_or_creator":          "only the session owner or the resource creator can do this.",
	"not_a_member":                  "you're not a member of this session; ask the owner for a PIN and run `tether login -s <sid> --pin <pin>`.",
	"session_not_found_or_deleting": "the session doesn't exist or is being deleted; check `tether session ls`.",
	"session_not_found":             "the session doesn't exist; check `tether session ls`.",
	// Q4 (docs/reviews/r6-findings.md): a session create routes through raft. It now reports success
	// on the FIRST attempt even when the committed write is not yet locally visible, so already_exists
	// means the name is genuinely taken. If a PRIOR create's request itself timed out (no reply), the
	// write may still have committed — check `tether session ls` before picking a new name.
	"already_exists": "a session with that name already exists. If an earlier `session create` request timed out with no reply, the write may still have committed — check `tether session ls`; otherwise the name is taken, pick another.",
	"actor_invalid":  "your identity is malformed; if this persists, regenerate keys with `rm -rf ~/.tether/keys/` (loses session memberships).",
	// Node lifecycle
	"node_not_found":      "no agent registered under that nid in this session; check `tether ps`.",
	"node_offline":        "the agent is OFFLINE (no recent heartbeat); start it with `tether agent --session <sid> --nid <nid>`.",
	"agent_no_responders": "the agent isn't reachable on NATS; check it's running and connected.",
	// batch C: the tier-B watchdog's own code. Deliberately NOT the agent_no_responders hint — this
	// fires on a transfer that may have been perfectly healthy, just slower than the budget.
	"transfer_budget_exceeded": "the transfer did not finish inside the broker's budget (derived from the file size and the slowest link tether promises to cover). The agent may be fine — check the link, or split the file; `tether expose` + rsync is the escape hatch for very large or very slow transfers.",
	"agent_malformed_resp":     "the agent sent a reply we can't decode; usually a version skew — try `tether node upgrade <nid>`.",
	// Upgrade
	"url_not_allowed":               "the broker hasn't whitelisted that URL prefix; ask the broker operator to add it under `broker.upgrade.url_allow` in broker.yaml.",
	"url_not_allowed_local":         "the agent re-checks the URL against its OWN allowlist, and that URL isn't on it; set the agent's `--upgrade-url-allow` flag or the `upgrade.url_allow` list in its agent.yaml (opening the broker's allowlist alone is not enough).",
	"sha256_invalid":                "SHA256 must be 64 lowercase hex chars; double-check the value.",
	"sha256_mismatch":               "the downloaded tarball's SHA256 doesn't match what you supplied; redownload and re-run.",
	"proto_bump_requires_reinstall": "the agent's proto version differs from the broker's; this needs a full reinstall (architecture J.3), not `node upgrade`.",
	// Expose
	"name_taken":         "another expose with that name already exists in this session; pick another --name or `tether expose rm --name <X>` first.",
	"port_exhausted":     "the broker has no free public port in its 14000-14999 band; ask the operator to free an old expose.",
	"local_port_invalid": "--local must be 1..65535.",
	"port_taken":         "that public port is already allocated; pick another port, omit --remote-port to auto-pick a free one, or release the existing one first.",
	"port_out_of_band":   "--remote-port must be within the broker's public band (default 14000-14999); pick an in-band port or omit it to auto-pick.",
	"frpc_failed":        "the agent couldn't start the local proxy; check the agent log (`~/.tether/agent/<sid>/agent.log`).",
	"name_reserved":      "that name is reserved for the system proxy; pick a different --name.",
	// P13 proxy subscription
	"subject_malformed": "the request subject was malformed; this is a tether bug or version skew — please report.",
	"proxy_disabled":    "the proxy switch is off for this session; an owner must run `tether proxy on` first.",
	"sub_name_invalid":  "subscriber --name must be 1..64 printable ASCII with no '/'.",
	"sub_name_taken":    "an active subscriber already uses that name; pick another or revoke the existing one.",
	"sub_not_found":     "no subscriber by that name in this session; check `tether proxy sub ls`.",
	"already_revoked":   "that subscriber is already revoked.",
	// Storage / generic
	"store_error": "the broker hit a SQLite error; check the broker log.",
	"json_parse":  "the broker couldn't parse our request; this is a tether bug — please report.",
	// Cluster transient states (B1 item 6). All three are FAILOVER/transient artifacts — nothing
	// the user did wrong, just wait and retry. NOTE: as of today NONE of these reaches a ctl
	// reply: home_catching_up + try_again are returned from tunnelTokenLookup (the AGENT's tunnel
	// REGISTER DENY path, which the agent auto-retries — see internal/broker/expose.go), and
	// leader_unavailable is consumed by the agent's register loop. These entries are defensive
	// future-proofing — so that IF a ctl path ever carries one, the user sees a sentence instead
	// of a raw token — and a friendly gloss for anyone reading broker logs.
	"home_catching_up":   "the broker that hosts this expose is briefly catching up after a failover; this is transient — wait a few seconds and retry.",
	"leader_unavailable": "the broker cluster is electing a new leader (a routine failover); this is transient — wait a few seconds and retry.",
	"try_again":          "the broker hit a transient storage hiccup (not a permanent failure); wait a moment and retry.",
}

// brokerCodeExitClasses maps a broker-reply Code to a process exit class (B2 item 3). Only codes
// the CLI commands actually surface are listed; an unmapped code falls through to exitInternal
// (70 = "a fault tether could not classify"). The classes are reviewed against the hint semantics:
// permission/ownership -> 77; positively self-healing transients -> 75; operator-action-required
// (a human must free a port / start an agent, NOT blind-retry) -> 64; our-bug/version-skew -> 70.
var brokerCodeExitClasses = map[string]int{
	// permission / ownership / membership
	"not_owner": exitNoPerm, "not_owner_or_creator": exitNoPerm, "not_a_member": exitNoPerm,
	// positively transient (self-healing) -> retry-later
	"agent_no_responders": exitTransient, "leader_unavailable": exitTransient,
	// batch C: a budget overrun is retry-able — the same file on a faster link, or a smaller file,
	// genuinely succeeds. 75 rather than 70 keeps a monitor from reading it as a tether bug.
	"transfer_budget_exceeded": exitTransient,
	"home_catching_up":         exitTransient, "try_again": exitTransient,
	// Mega-audit MAJ-7: C5 proxy quorum-loss is a designed self-healing transient (the leader heals on
	// re-election) — map to 75 so a monitor retries instead of treating it as a tether bug (70).
	"proxy_disabled_no_quorum": exitTransient, "proxy_frozen_readonly": exitTransient,
	// G67 #67: tier-B object-store provisioning could not complete because JetStream did not accept
	// the request (a broker restart, a leader election, or a cluster grow in flight). The broker has
	// already retried it a bounded number of times and classified it; 75 tells a monitor to retry
	// rather than treat a transient stall as a tether bug. `bucket_create_failed` deliberately stays
	// unmapped (70): it is the PERMANENT half of the same split.
	"jetstream_not_ready": exitTransient,
	// G67 (internal review M5): these two are PERMANENT and operator-actionable, so they must be
	// TERMINAL (64) rather than the unclassified 70 that docs/usage.md §9.13 tells automation to
	// retry with backoff. tier_b_store_too_small = give this broker more disk;
	// jetstream_unavailable = this broker has no JetStream at all (a config/boot state).
	// bucket_create_failed deliberately stays 70: it is the genuinely UNCLASSIFIED remainder.
	"tier_b_store_too_small": exitUsage, "jetstream_unavailable": exitUsage,
	// A restarting broker is the most retriable condition there is (internal review, adopted).
	"broker_restarting": exitTransient,
	// G67: both are already worded "retry shortly" by the broker (internal/broker/transfer.go) yet
	// landed on the unclassified 70. Same path, same self-healing shape.
	"too_many_in_flight": exitTransient, "transfer_id_in_flight": exitTransient,
	"ha_policy_invalid": exitUsage,
	// operator-action-required (NOT blind-retry) -> usage
	"node_offline": exitUsage, "node_not_found": exitUsage, "port_exhausted": exitUsage,
	"name_taken": exitUsage, "port_taken": exitUsage, "port_out_of_band": exitUsage,
	"local_port_invalid": exitUsage, "url_not_allowed": exitUsage, "url_not_allowed_local": exitUsage,
	"sha256_invalid": exitUsage, "sha256_mismatch": exitUsage,
	"session_not_found": exitUsage, "session_not_found_or_deleting": exitUsage,
	// Q4: with the same-owner idempotency exit in place, a residual already_exists is a genuine
	// name clash by ANOTHER owner — operator-actionable (pick another name), so exit 64, not 70.
	"already_exists": exitUsage,
	// our bug / version skew -> internal
	"agent_malformed_resp": exitInternal, "json_parse": exitInternal,
	"store_error": exitInternal,
	// External review M1: proto_bump_requires_reinstall was exitInternal(70)
	// while proto_mismatch — the same refusal, same remedy — was 64. docs/usage.md
	// says both need a full reinstall, and §9.13 tells automation that 70 is
	// retryable. So the code whose own hint says "this needs a full reinstall,
	// not `node upgrade`" was the one instructing monitors to retry it forever.
	// Two names for one operator action get one class.
	"proto_bump_requires_reinstall": exitUsage,
	// adminsock cluster codes (Item 4 sets these on the wire; the CLI maps them here).
	// `already_voter` used to be classified here too. It was removed by line-2 D1's reverse
	// reconciliation: nothing has ever emitted it. B2 item 4 declared the code speculatively, and the
	// design then went the other way -- adding a node that is already a voter is treated as IDEMPOTENT
	// SUCCESS on the resume path (cluster_operation_controller.go, "staged (nonvoter, or already a
	// voter on a resume)"), not as an error. A class for a code no binary can send is indistinguishable
	// from a live one to the next reader, which is precisely what the reverse gate exists to stop.
	"not_leader": exitNoPerm, "not_a_voter": exitUsage,
	"catch_up_stalled": exitTransient, "quorum_confirm_required": exitUsage, "nonce_used": exitUsage,
	// External-review F11: bad_request is operator input (backup dir exists, malformed args, bad
	// --since) — exit 64 (usage), NOT 70. 70 is reserved for genuine internal failures.
	"cluster_not_enabled": exitUsage, "node_unknown": exitUsage, "bad_request": exitUsage,
	"remove_owns_resources": exitUsage, // B3 item 7: operator-actionable (drain --retire or --force)
	// origin: line-2 §12 Y2. Both of these used to fall through to exitInternal (70), and
	// docs/usage.md §9.13 tells automation to treat 70 as RETRYABLE — so a host that physically cannot
	// allocate a PTY was being retried forever. They are classified oppositely because they ARE
	// opposite: no /dev/ptmx is a property of the host (operator must act), while a failed attach
	// subscription is a property of this instant (retry is the right response).
	// exitUsage (64), NOT exitUnavailable (69). docs/usage.md's robust-retry rule says "treat 69/70/75 as
	// retryable, only 64/77 as terminal" -- so 69 would have put this code in the retry class while its
	// own hint says "Retrying will not help until the host changes". That is Y2's founding complaint
	// (usage.md tells automation to retry 70, and too_large can never succeed) reproduced by Y2's own
	// output; three review lanes caught it. 64 is the class for "a human has to change something",
	// which is exactly what a missing /dev/ptmx is.
	"pty_unavailable":         exitUsage,
	"pty_alloc_failed":        exitTransient, // fd OR pty-count exhaustion (EMFILE/ENFILE/ENOSPC/ENOMEM/EAGAIN) — clears as sessions close
	"attach_subscribe_failed": exitTransient, // the agent's NATS connection was unhealthy just now
	// The other half of Y2: `download_failed` was carrying a 404, an oversize tarball and a transport
	// blip under one name. The first two cannot succeed on a retry, so they must not land in the class
	// automation retries.
	"download_http_status": exitUsage, // wrong URL / wrong mirror — the operator has to fix the argument
	// origin: external review M1. 408/429/5xx are NOT the operator's argument being wrong — the mirror is
	// down or rate-limiting and the same command succeeds later. Folding them into download_http_status
	// aborted the whole fleet and told automation never to retry.
	"download_http_retryable": exitTransient,
	"download_too_large":      exitUsage,     // the artifact is over the ceiling; same size on every retry
	"download_failed":         exitTransient, // transport or read failure — this one really does clear
	// origin: upgrade-safety plan §4. smoke_failed = the sha-verified artifact could not exec or did not
	// answer `version` — wrong arch, truncated upload, not a tether binary. The ARTIFACT is bad, so it is
	// bad on every node and on every retry: operator-action class, and `node upgrade --all` aborts the
	// fleet on it (configUpgradeCodes). upgrade_in_progress is its temporal opposite — a prior upgrade on
	// THIS node is still inside its register deadline; it self-resolves (commit or rollback) within that
	// bound, which is the definition of the retry class.
	"smoke_failed":        exitUsage,
	"upgrade_in_progress": exitTransient,
	"version_skew":        exitUsage, // B6 A3: reinstall the joiner on a matching release
	// R8a P1: the control plane committed the rehome but the agents have not confirmed the new
	// home yet. This is EX_TEMPFAIL, not a tether bug: the broker keeps re-delivering, so
	// re-running the verb is the correct response. Crucially it is NOT 0 — `cluster drain`
	// returning success on an unconverged data plane was the batch's headline release blocker.
	// The literal is authored in internal/broker (codeDataplaneNotConverged); there is no
	// compile-time link across the two packages, so TestDataplaneNotConvergedCodeIsWireStable
	// pins it from this side.
	dataplaneNotConvergedCode: exitTransient,

	// ---------------------------------------------------------------------
	// Batch-A A1. Everything below was previously UNMAPPED, i.e. it exited 70
	// — and docs/usage.md §9.13 tells automation to retry 70 with backoff. So
	// `too_large` (a file over the hard 2 GiB ceiling) was instructing monitors
	// to retry forever, recomputing a full-file SHA-256 each round. These are
	// classified by the same taxonomy as the block above: permission -> 77,
	// self-healing transient -> 75, operator-action-required -> 64, our bug or
	// version skew -> 70. Codes whose class is genuinely NOT KNOWN are left
	// unmapped on purpose and recorded in unclassifiedCodeAllowlist (see
	// error_code_coverage_test.go) rather than being guessed at.

	// permission / security boundary -> 77
	// path_outside_roots is a REFUSAL, not a usage slip: the path resolved
	// outside every configured allow_root. Same shape as not_owner.
	"path_outside_roots": exitNoPerm,

	// positively self-healing transients -> 75
	"attach_timeout":          exitTransient, // PTY attach did not land in time; the next attempt usually does
	"path_race":               exitTransient, // the path changed under us mid-check; re-resolving is the fix
	"forward_failed":          exitTransient, // broker->broker forward failed; the caller re-runs the verb
	"broker_forward_failed":   exitTransient,
	"cluster_not_ready":       exitTransient, // the cluster is still converging
	"remote_fs_unhealthy":     exitTransient, // spawnsafe: the path lives on a wedged mount; heals when the mount does
	"remote_fs_spawn_timeout": exitTransient, // spawnsafe: the bounded stat gave up; the slot is released on fs recovery
	"too_many_wedged_spawns":  exitTransient, // spawnsafe: slot exhaustion, self-clearing (spawnsafe.go:812-814)

	// operator-action-required (a human must change something) -> 64
	"name_required":         exitUsage,
	"name_reserved":         exitUsage,
	"not_found":             exitUsage, // the target does not exist; retrying cannot make it appear
	"on_broker_single_mode": exitUsage, // this verb needs a cluster; the broker is single-mode
	"on_broker_unknown":     exitUsage,
	"already_deleting":      exitUsage, // terminal state, not a race to wait out
	"already_revoked":       exitUsage,
	"sub_name_invalid":      exitUsage,
	"sub_name_taken":        exitUsage,
	"sub_not_found":         exitUsage,
	"argv_required":         exitUsage,
	"pid_required":          exitUsage,
	"pid_unknown":           exitUsage, // that PID is not tracked; a retry will not find it
	"path_not_absolute":     exitUsage,
	"path_not_found":        exitUsage,
	"path_parent_missing":   exitUsage,
	"not_a_regular_file":    exitUsage,
	"dst_exists":            exitUsage,
	"transfer_disabled":     exitUsage, // a config state; the operator must enable transfers
	"tier_invalid":          exitUsage,
	"too_large":             exitUsage, // over the hard size ceiling — physically cannot succeed on retry
	"size_mismatch":         exitUsage,
	"sha_mismatch":          exitUsage,
	"bucket_unknown":        exitUsage,
	"install_failed":        exitUsage, // read the agent log; blind retry re-runs the same failing install
	"self_path":             exitUsage, // refusing to overwrite our own running binary
	"sha256_required":       exitUsage,
	"frpc_failed":           exitUsage, // the data-plane helper failed to start; needs a human
	"nid_mismatch":          exitUsage, // this agent is configured for a different node id
	"proto_mismatch":        exitUsage, // reinstall the peer at a matching release (see CLAUDE.md §5)
	"request_invalid":       exitUsage,
	"rejected":              exitUsage, // kill refused by policy/state; the reason string says which
	"lock_not_held":         exitUsage, // grow lock lost — cluster_grow_trigger.go:124 wants the keeper to STOP
	"remote_fs_unsafe_cwd":  exitUsage,
	"remote_fs_not_found":   exitUsage,
	"verb_mismatch":         exitUsage,
	"transfer_unknown":      exitUsage, // no such transfer id; it will not appear on retry

	// permission -> 77 (second entry; grouped with path_outside_roots above)
	"unauthorized": exitNoPerm, // the grow trigger's account-signature check refused this caller

	// The online force-single anti-split-brain gates (adminsock.Code*). These
	// were the most dangerous unmapped codes in the repo: force-single is the
	// operation that can END a cluster, every one of its refusals used to exit
	// 70, and §9.13 tells automation that 70 is retryable — i.e. "keep retrying
	// the force-single we refused because the other broker is still ALIVE".
	// All five are decisions a human must act on; none self-heals.
	"peer_alive":           exitUsage, // a confirmed-dead peer answered a probe — the refusal is the point
	"quorum_not_lost":      exitUsage, // quorum is intact; force-single is not the right tool
	"force_single_refused": exitUsage,
	"arm_expired":          exitUsage, // the arming window closed; re-arm deliberately, do not auto-retry
	"is_leader":            exitUsage, // transfer leadership off this broker first (reexec.go:58)

	// our bug / version skew -> 70 (stated explicitly, so "we judged this" is
	// distinguishable from "nobody looked")
	"actor_invalid":          exitInternal, // a well-formed ctl cannot produce this
	"subject_malformed":      exitInternal,
	"marshal":                exitInternal,
	"internal_error":         exitInternal,
	"state_write_failed":     exitInternal,
	"signal_failed":          exitInternal,
	"cutover_revival_failed": exitInternal,
	"free_failed":            exitInternal,
}

// dataplaneNotConvergedCode aliases the proto SSOT, which internal/broker also
// aliases. Batch-A review F-17: these were two independent string literals kept
// in sync only by a test, and A1 made it three. Now the compiler holds them
// together and TestDataplaneNotConvergedCodeIsWireStable pins the VALUE against
// a literal written into the test file — which is the assertion that still
// matters, since renaming is free but changing the bytes is a wire break.
const dataplaneNotConvergedCode = proto.CodeDataplaneNotConverged

// brokerCodeExitClass returns the exit class for a broker code (default exitInternal=70).
func brokerCodeExitClass(code string) int {
	if c, ok := brokerCodeExitClasses[code]; ok {
		return c
	}
	return exitInternal
}

// brokerErrorMessage formats a broker-rejected reply as one
// human-friendly line: "<verb> failed: <code-hint or fallback>
// (<raw-code>: <raw-error>)". The raw pair is preserved in
// parens so logs / bug reports can still grep the architecture-
// stable codes. B2: the returned error is an *ExitError carrying the
// code's exit class — the HUMAN STRING is byte-identical to before.
func brokerErrorMessage(verb, code, errMsg string) error {
	lookup := code
	hint := brokerCodeHints[code]
	if hint == "" {
		// Some agent-rejected codes arrive prefixed:
		// "agent_rejected:install_failed". Strip the wrapper
		// before lookup so the underlying code can still match.
		if rest, ok := stripPrefix(code, "agent_rejected:"); ok {
			lookup = rest
			hint = brokerCodeHints[rest]
		}
	}
	msg := errMsg
	if hint != "" {
		msg = hint
	}
	// Code carries `lookup` (the agent_rejected:-stripped form), NOT the raw code: a caller branching on
	// the code must not have to know which errors travelled wrapped. The PROSE still shows the raw code,
	// because that is what an operator sees in the broker's own logs.
	return &ExitError{
		Class: brokerCodeExitClass(lookup),
		Code:  lookup,
		Err:   fmt.Errorf("%s failed: %s (%s)", verb, msg, code),
	}
}

// connectError wraps a NATS connect failure with what the operator
// should check. The wrapped err preserves the underlying %w chain
// so errors.Is on the original still works.
//
// Special case: an "Authorization Violation" at connect time means
// the auth_callout rejected this nkey for this connection-name
// template — *not* a network/TLS issue. Surface the four likely
// causes (session not found, not a member, PIN failed, evicted)
// so the operator stops debugging the wrong layer.
func connectError(verb, natsURL string, err error) error {
	if err != nil && strings.Contains(err.Error(), "Authorization Violation") {
		// auth_callout rejection is a PERMISSION problem (77), not unreachability.
		return permErr("%s: broker auth_callout rejected the connection: %w\n"+
			"  this is NOT a network problem. Check:\n"+
			"    - session exists and is ACTIVE     (run `tether session ls`)\n"+
			"    - you are a member of that session (run `tether login -s <sid> --pin <pin>` if first time)\n"+
			"    - your PIN matches the session's   (re-check with the session owner)\n"+
			"    - your nkey hasn't been evicted    (ask broker admin to check `tether admin sessions`)",
			verb, err)
	}
	return unavailErr("%s: cannot reach broker at %s: %w (verify the broker is running and --nats-url is correct)",
		verb, natsURL, err)
}

// runFailureReasons maps a PTY-side RunChunk{Kind:failed}.Reason to
// a one-line operator-facing diagnosis. Reasons are agent-emitted
// (architecture C.5.1), so the set is fixed.
var runFailureReasons = map[string]string{
	"attach_timeout": "agent allocated the PTY but ctl didn't subscribe in time (default 15s); on high-RTT WSS links, raise TETHER_AGENT_ATTACH_DEADLINE on the agent side.",
	// origin: line-2 external review M17. This used to say only "ran out of file descriptors", which named
	// the LESS likely of the two exhaustions: opening /dev/ptmx past the devpts limit returns ENOSPC, not
	// EMFILE, so an operator hitting /proc/sys/kernel/pty/max was told to look at the wrong resource.
	"pty_alloc_failed": "agent could not allocate a PTY because a limit is exhausted: either file descriptors (EMFILE/ENFILE) or the pty count itself (ENOSPC — see /proc/sys/kernel/pty/max, minus pty_reserve for non-root, or a devpts `max=` mount option). Close some sessions and retry; this one is transient. A host that cannot provide a PTY at all reports pty_unavailable instead.",
	"pty_unavailable":  "the agent host cannot provide a PTY at all: check /dev/ptmx and any container restrictions. Retrying will not help until the host changes.",
	// origin: line-2 §12 Y2 — split out of pty_alloc_failed, which was reporting both "this host
	// cannot do PTYs" (terminal) and "the attach subscription did not come up" (transient) under one
	// name. They classify differently, which is the whole reason to have two.
	"attach_subscribe_failed": "the agent allocated the PTY but could not subscribe to the attach subject; its NATS connection is unhealthy. Retry; if it persists, check the agent's broker connectivity.",
	// NOTE for the next person adding a Y2 code: download_http_status / download_too_large are NOT in
	// this map. They travel on UpgradeForwardedResp.Code, not RunChunk.Reason, so their hints live in
	// brokerCodeHints. A hint filed in the wrong map is never printed — it just reads, to whoever greps
	// for the code, as though the operator was given guidance.
	"exec_failed":   "agent allocated the PTY but the command failed to start; check argv (typo? not in PATH? not executable?).",
	"argv_required": "you supplied no command to run.",
	"json_parse":    "the agent couldn't parse our run request — tether bug, please report.",
	// remote-fs-resilience (docs/reviews/remote-fs-resilience-plan.md): the
	// agent refused/abandoned the spawn because a network filesystem is wedged.
	"remote_fs_unhealthy":     "argv[0] is on an unresponsive network mount (NFS/CIFS/...); use a binary on local disk, or --cwd a local dir.",
	"remote_fs_unsafe_cwd":    "the requested --cwd is on an unresponsive network mount; pick a local working directory.",
	"remote_fs_not_found":     "argv[0] was not found in the network-safe PATH (its dir may be on a wedged mount); use an absolute local path.",
	"remote_fs_spawn_timeout": "fork/exec stalled (likely a hung network mount under argv[0] or cwd) and was abandoned; the binary/data may live on dead NFS.",
	"too_many_wedged_spawns":  "too many spawns are already wedged on a hung filesystem; wait for the mount to recover (or restart the agent).",
}

func runFailureMessage(reason string) error {
	// Batch-A A1 Step 4. The broker sends RunChunk.Reason as either "<code>" or
	// "<code>: <detail>" (internal/broker/run.go builds the latter with
	// `"store_error: " + err.Error()`). This function only ever looked up the
	// WHOLE string, so every reason carrying a detail missed both the hint table
	// and the exit-class table and came out as a bare 70 — which
	// docs/usage.md §9.13 tells automation to retry.
	//
	// execFailureMessage already split on the colon; run did not. Same wire
	// shape, two different readers. The split is done here rather than by
	// changing RunChunk: adding a Code field would be a wire change, and batch A
	// is explicitly zero-wire.
	code := reason
	if i := strings.IndexByte(reason, ':'); i >= 0 {
		code = strings.TrimSpace(reason[:i])
	}
	class := brokerCodeExitClass(code)
	if hint := runFailureReasons[code]; hint != "" {
		return &ExitError{Class: class, Err: fmt.Errorf("run failed: %s (%s)", hint, reason)}
	}
	if hint := brokerCodeHints[code]; hint != "" {
		return &ExitError{Class: class, Err: fmt.Errorf("run failed: %s (%s)", hint, reason)}
	}
	return &ExitError{Class: class, Err: fmt.Errorf("run rejected by agent (%s)", reason)}
}

// execFailureMessage wraps an exec error-chunk string, appending the operator
// hint when the chunk carries a remote_fs_* / too_many_wedged_spawns reason code
// (the chunk is "<code>" or "<code>: <detail>"). Review m3 — exec gets the same
// guidance run already surfaces via runFailureMessage, instead of a raw code.
func execFailureMessage(chunkErr string) error {
	code := chunkErr
	if i := strings.IndexByte(chunkErr, ':'); i >= 0 {
		code = chunkErr[:i]
	}
	if hint := runFailureReasons[strings.TrimSpace(code)]; hint != "" {
		return fmt.Errorf("exec: %s (%s)", hint, chunkErr)
	}
	return fmt.Errorf("exec: %s", chunkErr)
}

func stripPrefix(s, prefix string) (string, bool) {
	if strings.HasPrefix(s, prefix) {
		return s[len(prefix):], true
	}
	return "", false
}
