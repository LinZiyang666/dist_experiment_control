package main

import (
	"fmt"
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
	// Membership / ownership / lifecycle
	"not_owner":                     "only the session owner can do this; ask the owner to run it.",
	"not_owner_or_creator":          "only the session owner or the resource creator can do this.",
	"not_a_member":                  "you're not a member of this session; ask the owner for a PIN and run `tether login -s <sid> --pin <pin>`.",
	"session_not_found_or_deleting": "the session doesn't exist or is being deleted; check `tether session list`.",
	"session_not_found":             "the session doesn't exist; check `tether session list`.",
	// Q4 (docs/reviews/r6-findings.md): a session create routes through raft. It now reports success
	// on the FIRST attempt even when the committed write is not yet locally visible, so already_exists
	// means the name is genuinely taken. If a PRIOR create's request itself timed out (no reply), the
	// write may still have committed — check `tether session ls` before picking a new name.
	"already_exists": "a session with that name already exists. If an earlier `session create` request timed out with no reply, the write may still have committed — check `tether session ls`; otherwise the name is taken, pick another.",
	"actor_invalid":  "your identity is malformed; if this persists, regenerate keys with `rm -rf ~/.tether/keys/` (loses session memberships).",
	// Node lifecycle
	"node_not_found":       "no agent registered under that nid in this session; check `tether ps`.",
	"node_offline":         "the agent is OFFLINE (no recent heartbeat); start it with `tether agent --session <sid> --nid <nid>`.",
	"agent_no_responders":  "the agent isn't reachable on NATS; check it's running and connected.",
	"agent_malformed_resp": "the agent sent a reply we can't decode; usually a version skew — try `tether node upgrade <nid>`.",
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
	"home_catching_up": exitTransient, "try_again": exitTransient,
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
	"proto_bump_requires_reinstall": exitInternal, "store_error": exitInternal,
	// adminsock cluster codes (Item 4 sets these on the wire; the CLI maps them here)
	"not_leader": exitNoPerm, "already_voter": exitUsage, "not_a_voter": exitUsage,
	"catch_up_stalled": exitTransient, "quorum_confirm_required": exitUsage, "nonce_used": exitUsage,
	// External-review F11: bad_request is operator input (backup dir exists, malformed args, bad
	// --since) — exit 64 (usage), NOT 70. 70 is reserved for genuine internal failures.
	"cluster_not_enabled": exitUsage, "node_unknown": exitUsage, "bad_request": exitUsage,
	"remove_owns_resources": exitUsage, // B3 item 7: operator-actionable (drain --retire or --force)
	"version_skew":          exitUsage, // B6 A3: reinstall the joiner on a matching release
	// R8a P1: the control plane committed the rehome but the agents have not confirmed the new
	// home yet. This is EX_TEMPFAIL, not a tether bug: the broker keeps re-delivering, so
	// re-running the verb is the correct response. Crucially it is NOT 0 — `cluster drain`
	// returning success on an unconverged data plane was the batch's headline release blocker.
	// The literal is authored in internal/broker (codeDataplaneNotConverged); there is no
	// compile-time link across the two packages, so TestDataplaneNotConvergedCodeIsWireStable
	// pins it from this side.
	dataplaneNotConvergedCode: exitTransient,
}

// dataplaneNotConvergedCode mirrors internal/broker's codeDataplaneNotConverged. Renaming
// either side silently downgrades a terminal "not converged" signal to exit 70; the wire-
// stability test is the only thing standing between those two literals.
const dataplaneNotConvergedCode = "dataplane_not_converged"

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
	return &ExitError{Class: brokerCodeExitClass(lookup), Err: fmt.Errorf("%s failed: %s (%s)", verb, msg, code)}
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
	"attach_timeout":   "agent allocated the PTY but ctl didn't subscribe in time (default 15s); on high-RTT WSS links, raise TETHER_AGENT_ATTACH_DEADLINE on the agent side.",
	"pty_alloc_failed": "agent couldn't open a PTY pair; check the agent host's /dev/ptmx and any container restrictions.",
	"exec_failed":      "agent allocated the PTY but the command failed to start; check argv (typo? not in PATH? not executable?).",
	"argv_required":    "you supplied no command to run.",
	"json_parse":       "the agent couldn't parse our run request — tether bug, please report.",
	// remote-fs-resilience (docs/reviews/remote-fs-resilience-plan.md): the
	// agent refused/abandoned the spawn because a network filesystem is wedged.
	"remote_fs_unhealthy":     "argv[0] is on an unresponsive network mount (NFS/CIFS/...); use a binary on local disk, or --cwd a local dir.",
	"remote_fs_unsafe_cwd":    "the requested --cwd is on an unresponsive network mount; pick a local working directory.",
	"remote_fs_not_found":     "argv[0] was not found in the network-safe PATH (its dir may be on a wedged mount); use an absolute local path.",
	"remote_fs_spawn_timeout": "fork/exec stalled (likely a hung network mount under argv[0] or cwd) and was abandoned; the binary/data may live on dead NFS.",
	"too_many_wedged_spawns":  "too many spawns are already wedged on a hung filesystem; wait for the mount to recover (or restart the agent).",
}

func runFailureMessage(reason string) error {
	if hint := runFailureReasons[reason]; hint != "" {
		return fmt.Errorf("run failed: %s (%s)", hint, reason)
	}
	return fmt.Errorf("run rejected by agent (%s)", reason)
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
