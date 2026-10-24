// Package legacy holds the pre-S8 upgrade CLI execution helpers — the old
// `node upgrade` wire, the `--all` canary loop and the S6 `cluster upgrade`
// planner/executor — moved verbatim (comments included) out of cmd/tether so
// the command files keep only registration and parsing. Nothing here can issue
// or start an S8 transaction; the shared broker policy decides every old route.
// origin: hot-update S8 plan §11.2 P19 slim move (§12.1 per-file net-zero budget)
package legacy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// The legacy (pre-S8) `node upgrade` wire helpers moved verbatim from
// cmd/tether/node.go with their comments (hot-update S8 plan §11.2 P19 slim
// move). cmd keeps thin wrappers with the original names so the fleet loop,
// its tests and the leak anchor still call them where they did.

// RequestFunc is one ctl NATS request/reply under the caller's context. It is
// an alias so the identical upgradecli port converts without a wrapper.
type RequestFunc = func(ctx context.Context, subject string, body []byte) ([]byte, error)

// DispatchLegacyUpgrade (cmd: dispatchUpgrade) fires one upgrade.req at (sid, nid) and reports the
// broker's reply. Returns the smoke-gate-normalized release tag the agent
// staged (empty from a pre-upgrade-safety agent) so callers can wait for the
// matching re-register. Extracted so the --all loop can reuse it. refuse is
// cmd's brokerErrorMessage, which owns the hint and exit-class tables.
func DispatchLegacyUpgrade(ctx context.Context, request RequestFunc, out io.Writer, refuse func(verb, code, msg string) error,
	sid, actor, nid, url, sha256 string, timeout time.Duration,
) (string, error) {
	body, _ := json.Marshal(proto.UpgradeReq{
		URL:          url,
		SHA256:       sha256,
		ProtoVersion: proto.ProtoVersion,
	})
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	respData, err := request(ctx,
		proto.SubjCmdBy(sid, actor, nid, "upgrade"), body)
	if err != nil {
		return "", fmt.Errorf("upgrade %s/%s: %w (broker or agent unreachable on NATS)", sid, nid, err)
	}
	var resp proto.UpgradeResp
	if err := json.Unmarshal(respData, &resp); err != nil {
		return "", fmt.Errorf("decode reply for %s: %w (broker version skew?)", nid, err)
	}
	if !resp.OK {
		return "", refuse(fmt.Sprintf("upgrade %s/%s", sid, nid), resp.Code, resp.Error)
	}
	// origin: internal review S3. A PRE-upgrade-safety agent (every deployed
	// release up to and including the one before this shipped) fills
	// NewVersion with the FULL first line of `tether version` — e.g.
	// "tether v0.4.7 (proto v2)" — not the bare tag: its readVersionString
	// was best-effort whole-line. Treating that as a comparable tag makes
	// --wait's equality test unsatisfiable → every successful first-hop
	// upgrade reads as ROLLED BACK and the canary aborts the whole fleet.
	// Normalize defensively: anything with whitespace or without a leading
	// "v" is legacy — drop to "" so --wait uses the release-changed fallback.
	newVersion := resp.NewVersion
	if newVersion != "" && (strings.ContainsAny(newVersion, " \t") || !strings.HasPrefix(newVersion, "v")) {
		newVersion = ""
	}
	if newVersion != "" {
		_, _ = fmt.Fprintf(out,
			"✔ %s/%s: staged (→ %s, smoke ok); agent re-exec in progress\n", sid, nid, newVersion)
	} else {
		_, _ = fmt.Fprintf(out,
			"✔ upgrade dispatched to %s/%s (agent re-exec in progress)\n", sid, nid)
	}
	return newVersion, nil
}

// LegacyFleet supplies the cmd-owned steps of the legacy `--all` loop: the
// release baseline, one dispatch, the canary's re-register wait and the two
// exit-class classifiers (which read cmd's ExitError).
type LegacyFleet struct {
	Baseline          func(nid string) (string, error)
	Dispatch          func(nid string) (string, error)
	Wait              func(nid, newVersion, startRelease string) error
	Transient, Config func(error) bool
}

// RunLegacyFleet (cmd: runUpgradeAll) is the `--all` body (extracted from RunE for testability).
// The CANARY (first node of the sorted target list) is always fully
// confirmed — staged AND re-registered as the new release — before any other
// node is touched; a canary failure leaves the rest of the fleet untouched
// (upgrade-safety plan §5). The remainder is NOT per-node waited (N×2min of
// serial waiting buys nothing after a confirmed canary) — the closing hint
// points at `tether node ls`.
func RunLegacyFleet(out, errOut io.Writer, f LegacyFleet, sid string, targets []string) error {
	canary, rest := targets[0], targets[1:]
	if len(rest) > 0 {
		_, _ = fmt.Fprintf(out,
			"canary: %s (%d more node(s) held until it commits)\n", canary, len(rest))
	}
	// Baseline before dispatch (S11); failure holds the WHOLE fleet (F4) —
	// a canary whose confirmation would be blind is no canary.
	startRelease, err := f.Baseline(canary)
	if err != nil {
		return fmt.Errorf("canary %s failed, %d node(s) untouched: %w", canary, len(rest), err)
	}
	newVersion, err := f.Dispatch(canary)
	if err != nil {
		return fmt.Errorf("canary %s failed, %d node(s) untouched: %w", canary, len(rest), err)
	}
	if err := f.Wait(canary, newVersion, startRelease); err != nil {
		return fmt.Errorf("canary %s failed, %d node(s) untouched: %w", canary, len(rest), err)
	}

	// Fleet rollouts must distinguish transient from configuration errors:
	//   - node_offline / agent_no_responders / upgrade_in_progress /
	//     timeout: keep going; the operator will retry these later.
	//   - everything else (not_owner / url_not_allowed / proto_bump /
	//     sha256_invalid / smoke_failed): config bug, abort the rest of the
	//     fleet so we don't fan-out a known-bad request.
	var skipped, failed int
	for _, nid := range rest {
		_, err := f.Dispatch(nid)
		if err == nil {
			continue
		}
		if f.Config(err) {
			return err
		}
		if f.Transient(err) {
			_, _ = fmt.Fprintf(errOut,
				"⚠ %s/%s skipped (transient): %v\n", sid, nid, err)
			skipped++
			continue
		}
		_, _ = fmt.Fprintf(errOut,
			"✗ %s/%s failed: %v\n", sid, nid, err)
		failed++
	}
	if failed > 0 {
		return fmt.Errorf("upgrade --all: %d failed (%d transiently skipped)", failed, skipped)
	}
	if skipped > 0 {
		_, _ = fmt.Fprintf(out,
			"(%d node(s) skipped due to transient errors — retry later)\n", skipped)
	}
	if len(rest) > 0 {
		_, _ = fmt.Fprintf(out,
			"fleet dispatched; verify releases with `tether node ls`\n")
	}
	return nil
}

// LookupNodeEntry (cmd: lookupNodeEntry) fetches one node's row from the node.list RPC.
func LookupNodeEntry(ctx context.Context, request RequestFunc, sid, actor, nid string) (proto.NodeListEntry, error) {
	body, _ := json.Marshal(proto.NodeListReq{})
	respCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	respData, err := request(respCtx, proto.SubjCtrlNodeList(actor, sid), body)
	if err != nil {
		return proto.NodeListEntry{}, err
	}
	var nl proto.NodeListResp
	if err := json.Unmarshal(respData, &nl); err != nil {
		return proto.NodeListEntry{}, err
	}
	if nl.Code != "" {
		return proto.NodeListEntry{}, fmt.Errorf("node.list rejected: %s %s", nl.Code, nl.Error)
	}
	for _, n := range nl.Nodes {
		if n.NID == nid {
			return n, nil
		}
	}
	return proto.NodeListEntry{}, fmt.Errorf("node %s not in node.list", nid)
}

// HOW `node upgrade --all` CLASSIFIES A PER-NODE FAILURE
//
// Two disjoint sets of WIRE CODES, matched EXACTLY against the code the error carries structurally
// (ExitError.Code, set at the source by brokerErrorMessage), plus one narrow prose fallback for
// errors that have no wire code at all.
//
// origin: line-2 external review 疑惑 #2. Both functions used to be
// `strings.Contains(err.Error(), needle)` over the same code names — exactly what exitcode.go's own
// comment forbids ("The classifier never string-sniffs prose for a class — that would make a
// reworded message silently change a script's exit code"). The prose these matched is the
// OPERATOR-FACING hint text from brokerCodeHints, which exists to be reworded; a hint that came to
// mention "download_http_status" while explaining something else would silently turn a skip into a
// fleet abort. Neither direction is safe: a false transient keeps rolling out a broken artifact, a
// false config error halts a fleet over one bad box.
//
// The fallback is deliberately NOT the old full list. It holds only needles that can never arrive as
// a wire code — Go's own transport/context prose — because for those there is nothing structural to
// match and today's behaviour is all there is. Every code-bearing needle moved to exact matching.
var (
	// LegacyTransientCodes (cmd: transientUpgradeCodes): "this specific node cannot upgrade right now". `--all` logs + skips these
	// so a single OFFLINE box doesn't abort a fleet-wide rollout; the operator gets a final summary
	// and can retry just the skipped subset.
	LegacyTransientCodes = map[string]bool{
		"node_offline":         true,
		"node_not_found":       true,
		"agent_no_responders":  true,
		"agent_malformed_resp": true,
		// origin: line-2 closure verification §6 B1. The other half of the Y2 split. These two really do
		// clear on their own — a transport blip mid-download, and a PTY/fd/pty-count limit that frees as
		// sessions close — so a fleet rollout should SKIP the node and keep going rather than counting it
		// as a hard failure the operator has to interpret.
		//
		// pty_alloc_failed is here rather than in the config set for the same reason its exit class is 75:
		// see internal/agent/run.go's ptyTransientErrnos. Its sibling pty_unavailable is deliberately in
		// NEITHER set — a host with no /dev/ptmx is not transient, but it is also not a bad CALL, so
		// aborting the whole fleet over one misconfigured box would be wrong. It stays a per-node failure.
		"download_failed": true,
		// origin: external review M1 — a mirror that is down or rate-limiting is exactly the case for
		// skipping this node and continuing: the artifact is fine and the next node may well succeed.
		"download_http_retryable": true,
		"pty_alloc_failed":        true,
		// origin: upgrade-safety plan §4 — a prior upgrade on THIS node is inside its register
		// deadline; it self-resolves (commit or rollback) within that bound. Skip and retry later.
		"upgrade_in_progress": true,
		// origin: hot-update S3 plan §4.2. Busy resources defer this node; the canary still aborts before this classifier.
		"upgrade_deferred": true,
	}
	// LegacyConfigCodes (cmd: configUpgradeCodes): the request or candidate needs operator action. Aborts `--all` before the
	// same request is sent to the remaining nodes, including candidates unable to carry live tasks.
	LegacyConfigCodes = map[string]bool{
		"not_owner":                     true,
		"url_not_allowed":               true,
		"sha256_invalid":                true,
		"proto_bump_requires_reinstall": true,
		"actor_invalid":                 true,
		"session_not_found_or_deleting": true,
		// origin: line-2 closure verification §6 B1, the review's self-declared most substantive item.
		//
		// The line-2 §12 Y2 split created these two codes precisely because `download_failed` was
		// telling automation to retry a permanently-broken download forever. It fixed the EXIT CLASS
		// (both → 64) and the hint text, and missed this list — a THIRD classification of the same
		// codes, in the same package, reached by the one command Y2 was written to rescue.
		//
		// The consequence was worse than a wrong exit code. Neither code matched here nor in the
		// transient set, so `node upgrade --all` counted the node as "✗ failed" and CARRIED ON —
		// fanning a typo'd --url out to every remaining node in the fleet. Aborting on exactly that is
		// what the comment above isConfigError's call site says this function is for.
		//
		// download_too_large is the same shape: the artifact is over the agent's ceiling, so it will be
		// over the ceiling on node 2 through node N as well.
		"download_http_status": true,
		"download_too_large":   true,
		// origin: upgrade-safety plan §4 — the sha-verified artifact failed the agent's pre-install
		// smoke gate (cannot exec / no version line): wrong arch or truncated build, which is wrong for
		// EVERY node. `--all` presumes a homogeneous fleet (one URL, one sha for all), so fanning it out
		// after the first smoke failure would push a known-bad artifact at the remaining N-1 nodes.
		"smoke_failed": true,
		// origin: hot-update S3 plan §4.2. Stop distributing a candidate that cannot carry the node's live image.
		"image_abi_unsupported": true,
	}
	// LegacyCodelessNeedles (cmd: transientCodelessNeedles): prose that can only come from Go's transport/context layer, which never
	// carries a wire code. Consulted ONLY when the error carries no code — never as an override.
	LegacyCodelessNeedles = []string{
		"deadline exceeded",
		"context canceled",
	}
)

// ListOnlineNIDs (cmd: listOnlineNIDs) round-trips a node.list.req (architecture B.1
// line 129) and returns the nids whose `nodes.status` is ONLINE.
// Uses the dedicated node-enum RPC instead of process-derived
// inference: a fresh agent that registered but never exec'd
// shows up here AND can be upgrade target, which the older
// ps-based heuristic missed.
// It also reports how many ONLINE rows were EXCLUDED for running under an
// assigned lease name, so the caller can say so out loud rather than silently
// upgrading a smaller fleet than the operator expects.
func ListOnlineNIDs(ctx context.Context, request RequestFunc, sid, actor string) ([]string, int, error) {
	body, _ := json.Marshal(proto.NodeListReq{})
	respCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	respData, err := request(respCtx,
		proto.SubjCtrlNodeList(actor, sid), body)
	if err != nil {
		return nil, 0, fmt.Errorf("upgrade --all: node.list lookup: %w", err)
	}
	var nl proto.NodeListResp
	if err := json.Unmarshal(respData, &nl); err != nil {
		return nil, 0, fmt.Errorf("upgrade --all: node.list decode: %w", err)
	}
	if nl.Code != "" {
		return nil, 0, fmt.Errorf("upgrade --all: node.list rejected: %s %s", nl.Code, nl.Error)
	}
	var out []string
	var skippedLeased int
	for _, n := range nl.Nodes {
		if n.Status != "ONLINE" {
			continue
		}
		// Q4: a LEASED instance is excluded from the fleet fan-out.
		//
		// Two concrete hazards, not a preference. (1) Upgrading one is
		// meaningless: it runs the binary baked into its image and reverts on
		// its next restart. (2) Targets are sorted lexicographically and
		// targets[0] is the canary that gates the entire fleet — a leased
		// instance can sort first, and if its pod recycles inside the wait
		// budget the whole rollout aborts having upgraded nothing.
		//
		// Explicit upgrades of the leased row and its basename are refused too:
		// shared-home clones may share one binary and rollback marker, so there is
		// no honest per-instance remote-upgrade boundary.
		if n.Leased {
			skippedLeased++
			continue
		}
		out = append(out, n.NID)
	}
	return out, skippedLeased, nil
}
