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
	"actor_invalid":                 "your identity is malformed; if this persists, regenerate keys with `rm -rf ~/.tether/keys/` (loses session memberships).",
	// Node lifecycle
	"node_not_found":       "no agent registered under that nid in this session; check `tether ps`.",
	"node_offline":         "the agent is OFFLINE (no recent heartbeat); start it with `tether agent --session <sid> --nid <nid>`.",
	"agent_no_responders":  "the agent isn't reachable on NATS; check it's running and connected.",
	"agent_malformed_resp": "the agent sent a reply we can't decode; usually a version skew — try `tether node upgrade <nid>`.",
	// Upgrade
	"url_not_allowed":               "the broker hasn't whitelisted that URL prefix; ask the broker operator to add it under `broker.upgrade.url_allow` in broker.yaml.",
	"url_not_allowed_local":         "the agent's local allowlist doesn't accept that URL; check the agent's --upgrade-url-allow flag.",
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
}

// brokerErrorMessage formats a broker-rejected reply as one
// human-friendly line: "<verb> failed: <code-hint or fallback>
// (<raw-code>: <raw-error>)". The raw pair is preserved in
// parens so logs / bug reports can still grep the architecture-
// stable codes.
func brokerErrorMessage(verb, code, errMsg string) error {
	hint := brokerCodeHints[code]
	if hint == "" {
		// Some agent-rejected codes arrive prefixed:
		// "agent_rejected:install_failed". Strip the wrapper
		// before lookup so the underlying code can still match.
		if rest, ok := stripPrefix(code, "agent_rejected:"); ok {
			hint = brokerCodeHints[rest]
		}
	}
	if hint == "" {
		return fmt.Errorf("%s failed: %s (%s)", verb, errMsg, code)
	}
	return fmt.Errorf("%s failed: %s (%s)", verb, hint, code)
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
		return fmt.Errorf("%s: broker auth_callout rejected the connection: %w\n"+
			"  this is NOT a network problem. Check:\n"+
			"    - session exists and is ACTIVE     (run `tether session ls`)\n"+
			"    - you are a member of that session (run `tether login -s <sid> --pin <pin>` if first time)\n"+
			"    - your PIN matches the session's   (re-check with the session owner)\n"+
			"    - your nkey hasn't been evicted    (ask broker admin to check `tether admin sessions`)",
			verb, err)
	}
	return fmt.Errorf("%s: cannot reach broker at %s: %w (verify the broker is running and --nats-url is correct)",
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
}

func runFailureMessage(reason string) error {
	if hint := runFailureReasons[reason]; hint != "" {
		return fmt.Errorf("run failed: %s (%s)", hint, reason)
	}
	return fmt.Errorf("run rejected by agent (%s)", reason)
}

func stripPrefix(s, prefix string) (string, bool) {
	if strings.HasPrefix(s, prefix) {
		return s[len(prefix):], true
	}
	return "", false
}
