#!/bin/sh
# 31-node-upgrade-fleet.sh — S5, N=1 cluster + 2 agent + ctl: the agent `node upgrade` surface on the real
# stack. #28 (the agent-side upgrade allow-list was UNCONFIGURABLE — hardcoded to a GitHub prefix, so a
# self-hosted mirror the BROKER whitelisted was still refused by the AGENT with url_not_allowed_local) is now
# FIXED (R15): the agent honours a top-level `upgrade.url_allow` block in agent.yaml (cmd/tether/agent.go
# resolveAgentUpgradeAllow; internal/agent/upgrade.go:76 consumes Config.UpgradeURLAllowlist). This drill
# CONFIGURES that block (labeled deployment config, Mandate ③ — the operator's own job, not compensating for a
# tether gap) and PROVES the fix two ways:
#   (a) the CONFIGURED artifact URL now PASSES the agent's local allow-list — an upgrade carrying a deliberately
#       WRONG (but 64-hex-valid) sha reaches the agent's POST-DOWNLOAD digest check and fails `sha256_mismatch`,
#       NOT `url_not_allowed_local` (the old wall). Had the config not taken, the agent would still answer
#       url_not_allowed_local and the assert_refuses sig would MISMATCH → ASSERT-FAIL (config that failed to
#       apply cannot pass this arm).
#   (b) the allow-list is still LIVE, not disabled: a URL the BROKER whitelists but the AGENT does not (the
#       broker's list is deliberately wider) is still refused `url_not_allowed_local` AT THE AGENT gate.
# GREEN negatives (broker-side url_not_allowed / sha256_invalid / not_owner) remain independently reachable.
# FORBIDDEN: faking github.com via /etc/hosts (Mandate ①). Consumes S0-artifact (real https tarball + sha) +
# S0-layout (agent binary in ~/.local/bin). U1-pinned N=1 cluster.
#
# FALSE-GREEN GUARDS:
#  - (a) is signed on `sha256_mismatch` (the AGENT's post-download digest check) — reachable ONLY after the
#    request cleared BOTH the broker AND the agent URL gates AND downloaded the tarball, so a wrong-sha probe
#    can never be confused with a url gate. url_not_allowed_local / url_not_allowed / sha256_invalid /
#    download_failed all MISS the sig → ASSERT-FAIL.
#  - (b) is signed on `url_not_allowed_local` (the `_local` suffix). The broker's own `url_not_allowed` (no
#    `_local`) does NOT match, so had the broker refused first (its list too narrow) the assert would FAIL
#    LOUD — proving the refusal came from the AGENT, not the broker. The broker's wider 2nd prefix
#    (see _allow_artifact) is load-bearing for this.
#  - the wrong sha is 64 lowercase hex (passes the broker's sha256 sanity) so it cannot short-circuit to
#    sha256_invalid; a NON-hex sha is exercised separately by N2.
#  - NO arm triggers a real re-exec (which would mutate the shared binary + re-home the fleet mid-drill): (a)
#    fails on the wrong sha; the non-vacuity + F3 probes fail at the agent url gate; N3 fails at the broker
#    owner gate (pre-forward); F4's --timeout 0 request is never even published (nats.go context.go:49). The
#    SUCCESSFUL re-exec path is a first-class not_covered (see drill_end). A dedicated success drill must
#    be created; no owner currently exists.
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/agentyaml.sh"; . "$HERE/drills/lib/ident.sh"
. "$HERE/drills/lib/ingress.sh"; . "$HERE/drills/lib/artifact.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
CA=/usr/local/share/ca-certificates/tether-sim-ca.crt
VALIDSHA=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef   # valid 64-hex, wrong value
WRONGSHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa   # valid 64-hex, deliberately != the real ART_SHA — the #28-FLIPPED sha-gate probe
# EXT-REVIEW-B5 (lint rule `combined-signal-trap`): a single `trap … EXIT INT TERM` is WRONG for INT/TERM —
# a POSIX handler RETURNS to the next command, so a Ctrl-C would reap the artifact and then RESUME the
# upgrade steps below (racing cmd_drill's own nuke). drill_install_traps registers EXIT separately and makes
# INT/TERM exit 128+signo without resuming. Cleanup body unchanged.
_cleanup() { artifact_down 2>/dev/null; }
drill_install_traps _cleanup
CTL() { "$SIM" ctl -- "$@"; }

# labeled operator config: add broker.upgrade.url_allow (install.sh writes NO upgrade block — spike-confirmed).
# Uses python3-yaml so it works regardless of the existing broker.yaml shape; broker.yaml is root-owned (dexec
# defaults to root). This is a DEPLOYMENT config step (Mandate ③), not compensating for tether.
# NB: dexec runs `docker exec` WITHOUT -i, so a heredoc stdin does NOT reach the container. Pass the edit as a
# python3 -c program (one arg). $INSTANCE is interpolated by the drill shell (double-quoted); the code uses
# single quotes only, so there is no quote clash.
_allow_artifact() {
    # NON-destructive APPEND (indent 2, sibling of broker.cluster). A yaml round-trip rewrite dropped install.sh's
    # cluster seam and bricked the broker (session create → no active session). broker.yaml is one top-level
    # `broker:` map, so a 2-space-indented `upgrade:` block appended at EOF lands under broker:.
    # TWO prefixes: `-artifact/` is the served release; `-mirror/` is a DELIBERATELY WIDER broker entry with no
    # matching agent entry, so the (b) non-vacuity + F3 probes can present a broker-allowed-but-agent-denied URL
    # that reaches the AGENT url gate. N1's off-band `evil.invalid` matches NEITHER prefix → still broker-refused.
    dexec brk1 -- sh -c "grep -q 'url_allow' /etc/tether/broker.yaml || printf '  upgrade:\n    url_allow:\n      - https://%s-artifact/\n      - https://%s-mirror/\n' '$INSTANCE' '$INSTANCE' >> /etc/tether/broker.yaml" \
        && sctl brk1 restart tether-broker && poll_until 30 2 "brk1 broker back after url_allow config" -- _broker_up
}
# labeled DEPLOYMENT config (Mandate ③, NOT compensating for a tether gap): configure the AGENT's OWN upgrade
# URL allow-list — the #28 FIX (cmd/tether/agent.go resolveAgentUpgradeAllow + internal/agent/upgrade.go:76,
# which consumes Config.UpgradeURLAllowlist). agent-join runs the daemon FLAGFUL (--session/--nid/--nats-url)
# but with NO --upgrade-url-allow, and the daemon ALWAYS loads agent.yaml (agent.go:285) — so the yaml's
# top-level `upgrade.url_allow` is the ONLY lever a systemd-managed agent has, and it is exactly what an
# operator self-hosting a mirror must set. agent-join writes NO agent.yaml, so this CREATEs a minimal one
# (only the upgrade block; every other value stays on the unit's flags — a lone `upgrade:` map loads clean
# under the strict-KnownFields loader). Written as user sim so it stays sim:sim 0600 under the 0700 agent/<sid>
# tree (install.sh shape). The agent list carries ONLY the `-artifact/` prefix (NOT the broker's wider
# `-mirror/`) so the (b) non-vacuity + F3 probes can hit the AGENT gate with a broker-allowed-but-agent-denied
# URL. Then restart + poll ONLINE so the NEW yaml-driven daemon is serving before any upgrade arm runs.
_allow_artifact_agent() {
    dexec agt1 -- runuser -u sim -- sh -c "
        set -e
        install -d -m 0700 /home/sim/.tether/agent/$SID
        f=/home/sim/.tether/agent/$SID/agent.yaml
        grep -q 'url_allow' \"\$f\" 2>/dev/null || {
            printf 'upgrade:\n  url_allow:\n    - https://%s-artifact/\n' '$INSTANCE' >> \"\$f\"
            chmod 600 \"\$f\"
        }" \
        && sctl agt1 restart tether-agent \
        && poll_until 30 2 "agt1 back ONLINE after agent.upgrade.url_allow config" -- _pre_agt1_owner
}
# after a restart, is-active is NOT enough — the broker must be FUNCTIONALLY ready (leader elected) before
# session create works, or it fails "no active session". Poll the leader view.
_broker_up() { "$SIM" exec brk1 -- tether cluster status --json 2>/dev/null | jq -e '.leader_id=="brk1"' >/dev/null 2>&1; }

# #28-FLIPPED 3-point preconditions: make `sha256_mismatch` the ONLY wall the (a) probe can hit — the request
# must fetch+trust (else download_failed), be broker-whitelisted (else url_not_allowed), and be ONLINE+owner
# (else node_offline/not_owner). With all three, the wrong-sha upgrade can only fail at the agent's digest check.
_pre_reachable() { dexec agt1 -- curl -sf --max-time 8 --cacert "$CA" "$ART_URL" -o /dev/null; }   # (i) fetch+trust
_pre_broker_allows() { dexec brk1 -- grep -q "$INSTANCE-artifact" /etc/tether/broker.yaml; }        # (ii) broker whitelisted
_pre_agt1_owner() { "$SIM" exec brk1 -- runuser -u tether -- tether admin nodes 2>&1 | grep -E 'agt1' | grep -q ONLINE; }  # (iii) ONLINE (broker-authoritative)
_n3_notowner() {   # a joined MEMBER (not owner) is refused node upgrade
    CTLH mem login -s "$SID" --pin "$PIN" --broker nats://brk1:4222 >/dev/null 2>&1
    CTLH mem node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA" 2>&1 | grep -qiE 'not_owner|owner-only|not the owner'
}
# --- Arm FLEET (external-review M4): --all enumeration/dispatch/--timeout, INDEPENDENT of #28's blocked success ---
# `node upgrade --all`'s target set is listOnlineNIDs (node.go:341) = node.list.req filtered to status==ONLINE —
# the SAME RPC + ONLINE filter `node ls` uses (node.go:34). So the ONLINE-only `node ls --json` IS the --all
# target set. assert_ok runs these in a FUNCTION-inheriting subshell (never sh -c).
_ls_online_nids()   { "$SIM" ctl -- node ls --json 2>/dev/null | jq -r '.nodes[]?|select(.status=="ONLINE")|.nid' 2>/dev/null; }
_ls_has_online()    { _ls_online_nids | grep -qx "$1"; }
_ls_not_online()    { ! _ls_online_nids | grep -qx "$1"; }
_ls_offline_stale() { "$SIM" ctl -- node ls -a --json 2>/dev/null | jq -e --arg n "$1" '.nodes[]?|select(.nid==$n and (.status=="OFFLINE" or .status=="STALE"))' >/dev/null 2>&1; }

drill_begin "31-node-upgrade-fleet (#28 FIXED: agent allow-list configurable + GREEN broker/agent negatives)"
"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 1 broker + 2 agents + 1 ctl"          "$SIM" up --brokers 1 --agents 2 --ctl 1
assert_ok "init brk1 (N=1 cluster)"                  "$SIM" init brk1
# broker.upgrade.url_allow config + restart FIRST — a broker restart AFTER ctl login would invalidate the ctl's
# session (70/72 never restart the broker, so they don't hit this). Do it before session create.
assert_ok "SETUP broker.upgrade.url_allow += artifact prefix (labeled operator config) + broker restart" \
          _allow_artifact
assert_ok "session lab + ctl login (owner)"          "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1"                          "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_ok "agent-join agt2"                          "$SIM" agent-join agt2 --session "$SID" --pin "$PIN"
# S0-artifact: serve tether-next as a real https release tarball + host-computed sha (sets ART_URL/ART_SHA).
# Call DIRECTLY (not via assert_ok — its command-sub subshell would discard the ART_URL/ART_SHA globals).
artifact_up "$INSTANCE" "$HERE/vendor/tether-next" "tether-next.tar.gz" || die "31: artifact_up failed"
assert_ok "SETUP artifact serving tether-next (ART_URL/ART_SHA populated)" \
          sh -c "[ -n '$ART_URL' ] && [ -n '$ART_SHA' ]"
assert_ok "SETUP inject instance CA into agt1 trust store (so the fetch is trusted — isolates #28)" \
          ingress_trust_inject agt1
log "31: ART_URL=$ART_URL ART_SHA=$ART_SHA"
# A URL the BROKER whitelists (the `-mirror/` 2nd prefix in _allow_artifact) but the AGENT does NOT (its
# url_allow lists only `-artifact/`): it PASSES the broker gate and is refused at the AGENT's local allow-list.
# The (b) non-vacuity probe + F3's config-abort trigger both use it. Its host need never resolve — the agent
# rejects at the url gate BEFORE any fetch.
AGENT_DENIED_URL="https://$INSTANCE-mirror/x.tar.gz"
# DBG BEFORE the agent allow-list is configured: still the old wall (url_not_allowed_local) — harmless, and it
# proves the flip below is caused by _allow_artifact_agent, not a pre-existing state. Uses the REAL sha, so it
# MUST run before _allow_artifact_agent — afterwards this exact call would pass every gate and RE-EXEC agt1.
log "31-DBG pre-config upgrade (real sha, agent UNconfigured) >>> $("$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA" 2>&1 | tr '\n' '~')"

# labeled deployment config (Mandate ③): wire the agent's OWN upgrade allow-list. MUST come after the DBG line
# above (real-sha) and before any owner-side upgrade with the real sha, or it would trigger a real re-exec.
assert_ok "SETUP agt1 agent.upgrade.url_allow += artifact prefix (labeled operator config) + agent restart + ONLINE" \
          _allow_artifact_agent

# ── Arm #28 — FIXED (R15): the agent allow-list is now configurable; the CONFIGURED URL passes the agent gate ──
assert_ok "P28-i  precondition: agt1 can FETCH+TRUST the artifact URL (so the flip reaches the sha gate, not download_failed)"  _pre_reachable
assert_ok "P28-ii precondition: broker WHITELISTED the URL (so the request forwards, not broker url_not_allowed)"               _pre_broker_allows
assert_ok "P28-iii precondition: agt1 ONLINE + owner=caller (rules out node_offline/not_owner)"                                 _pre_agt1_owner
assert_refuses "#28 FLIPPED (R15): the CONFIGURED artifact URL PASSES the agent local allow-list — it now fails on the WRONG sha (sha256_mismatch), NOT url_not_allowed_local (was: unconfigurable → always url_not_allowed_local)" \
               "sha256_mismatch" \
               "$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "$WRONGSHA"
assert_refuses "#28 non-vacuity: the agent allow-list is LIVE, not disabled — a broker-whitelisted-but-agent-denied URL (VALID sha) is still refused url_not_allowed_local AT THE AGENT gate (broker-side url_not_allowed would MISS this sig → FAIL LOUD)" \
               "url_not_allowed_local" \
               "$SIM" ctl -- node upgrade agt1 --url "$AGENT_DENIED_URL" --sha256 "$VALIDSHA"

# ── GREEN negatives (independently reachable today) ──────────────────────────────────────────────────
assert_refuses "N1 broker url_not_allowed (off-allowlist URL + VALID sha → not the sha gate)" \
               "url_not_allowed([^_]|\$)" \
               "$SIM" ctl -- node upgrade agt1 --url "https://evil.invalid/x.tar.gz" --sha256 "$VALIDSHA"
assert_refuses "N2 broker sha256_invalid (malformed sha, not 64-lowercase-hex)" \
               "sha256_invalid|sha256|hex" \
               "$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "not-a-valid-sha"
assert_ok "N3 not_owner (a joined member, not the session owner, is refused node upgrade)"  _n3_notowner

# ── Arm FLEET (external-review M4) — --all enumeration + dispatch + --timeout (independent of #28) ─────
# F1 — ONLINE enumeration baseline: both agents are in the --all target set (ONLINE).
assert_ok "F1 --all target set (node ls --json ONLINE filter) includes agt1 (ONLINE)"  _ls_has_online agt1
assert_ok "F1b --all target set includes agt2 (ONLINE) — a real 2-node fleet"           _ls_has_online agt2
# F2 — OFFLINE enumeration exclusion: stop agt2's daemon → it drops OUT of the --all target set (listOnlineNIDs
# only picks ONLINE), while agt1 stays. This is the exact exclusion --all applies at dispatch enumeration.
assert_ok "F2-setup stop agt2 daemon → make it OFFLINE/STALE"  "$SIM" exec agt2 -- systemctl stop tether-agent
assert_ok "F2 agt2 goes OFFLINE/STALE in the broker view"     poll_until 45 3 "agt2 OFFLINE/STALE" -- _ls_offline_stale agt2
assert_ok "F2b OFFLINE agt2 is EXCLUDED from the --all target set (listOnlineNIDs ONLINE-only)"  _ls_not_online agt2
assert_ok "F2c ONLINE agt1 stays in the --all target set"     _ls_has_online agt1
# F3 — --all DISPATCHES to the ONLINE target + config-abort: targets only agt1 (agt2 excluded), dispatches, and
# aborts on url_not_allowed_local (a CONFIG error — node.go:221/318 isConfigError aborts the fleet). Trigger is
# AGENT_DENIED_URL (broker-allowed, agent-denied — since #28 is FIXED, $ART_URL would now PASS the agent gate and
# re-exec, so it can no longer be the abort trigger). Proves the fleet enumerated+dispatched (not a no-op) at the
# AGENT gate, aborted, and never dispatched to the OFFLINE agt2. VALID sha so it isn't short-circuited pre-forward.
_allcfg=$("$SIM" ctl -- node upgrade --all --url "$AGENT_DENIED_URL" --sha256 "$VALIDSHA" 2>&1); _allcfg_rc=$?
log "31-DBG --all(agent-denied) rc=$_allcfg_rc out=$(printf '%s' "$_allcfg" | tr '\n' '~')"
assert_ok "F3 --all dispatched to ONLINE agt1 → aborted on the agent's url_not_allowed_local gate (isConfigError aborts the fleet; reached an agent, not a no-op)"  sh -c '[ "$1" -ne 0 ] && printf "%s" "$2" | grep -q "url_not_allowed_local"' _ "$_allcfg_rc" "$_allcfg"
assert_ok "F3b --all did NOT dispatch to the OFFLINE agt2 (enumerated out — the abort names agt1, never agt2)"  sh -c '! printf "%s" "$1" | grep -q "agt2"' _ "$_allcfg"
# F4 — --timeout threading + transient skip-continue: bring agt2 back ONLINE, run --all --timeout 0 → every
# dispatch hits an immediately-expired context (deadline exceeded = TRANSIENT, node.go:298-313) → --all SKIPS +
# continues past each node → exit 0 with the "(N skipped)" summary. Proves --timeout is threaded AND the
# transient-skip path (distinct from F3's config-abort).
assert_ok "F4-setup restart agt2 daemon"                      "$SIM" exec agt2 -- systemctl start tether-agent
assert_ok "F4-setup2 agt2 back ONLINE (2-node fleet again)"   poll_until 45 3 "agt2 ONLINE" -- _ls_has_online agt2
# external-review R2-M6: capture the exact ONLINE set + rc + per-node skip lines; assert rc=0, distinct skipped
# count == ONLINE count, both agt1+agt2 skipped, and the exact "(N node(s) skipped...)" summary — NOT just a
# loose grep for "skipped|transient" (which a nonzero rc + one synthetic skip line would have satisfied).
_online_n=$(_ls_online_nids | grep -c .)
_allto=$("$SIM" ctl -- node upgrade --all --timeout 0 --url "$ART_URL" --sha256 "$ART_SHA" 2>&1); _allto_rc=$?
_skip_n=$(printf '%s' "$_allto" | grep -c 'skipped (transient)')
log "31-DBG --all --timeout 0 rc=$_allto_rc online_n=$_online_n skip_lines=$_skip_n out=$(printf '%s' "$_allto" | tr '\n' '~')"
assert_ok "F4a --all --timeout 0 exits 0 (transient-skip CONTINUES + summarises, does NOT abort like F3's config error)"  sh -c "[ '$_allto_rc' = 0 ]"
assert_ok "F4b every ONLINE node is skipped-transient: distinct skip-line count ($_skip_n) == ONLINE set ($_online_n) AND both agt1+agt2 appear in a 'skipped (transient)' line"  sh -c "[ '$_skip_n' = '$_online_n' ] && [ '$_online_n' -ge 2 ] && printf '%s' \"\$1\" | grep -qE 'agt1 skipped \\(transient\\)' && printf '%s' \"\$1\" | grep -qE 'agt2 skipped \\(transient\\)'" _ "$_allto"
assert_ok "F4c fleet continues to the EXACT skip summary '($_online_n node(s) skipped due to transient errors...)' (--timeout threaded; transient-skip path, NOT F3's config-abort)"  sh -c "printf '%s' \"\$1\" | grep -qE '\\($_online_n node\\(s\\) skipped due to transient'" _ "$_allto"
not_covered "31 (external-review M4, honest): a SUCCESSFUL fleet upgrade (PID-preserving re-exec + version bump + two-node summary)" \
    "DELIBERATELY not exercised here: #28 is FIXED (this drill PROVES the configured URL passes the agent gate), so the success path is now reachable in principle — but a real re-exec would REWRITE the shared binary + re-home the 2-node fleet mid-drill, polluting the enumeration/dispatch/timeout arms. This drill covers the CONTROL surface: agent allow-list flip + non-vacuity (#28), ENUMERATION (F1/F2), DISPATCH + config-abort (F3), --timeout + transient-skip (F4). A dedicated destructive success/PID/version/rollback/--wait drill must be created; it currently has no owner." gap

drill_end
