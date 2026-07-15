#!/bin/sh
# 31-node-upgrade-fleet.sh — S5 (#28 EXPECTED-RED + GREEN negatives, N=1 cluster + 2 agent + ctl): the agent
# `node upgrade` surface on the real stack. Code-predicted gotcha #28: the agent-side upgrade allow-list is
# hardcoded to a GitHub prefix with NO operator wiring (no flag/yaml/env), while usage §9.3/error_hints claim a
# --upgrade-url-allow agent flag that does not exist — so a self-hosted mirror the BROKER whitelisted is still
# refused by the AGENT (url_not_allowed_local). Pinned by assert_bug with a 3-point discriminator so the RED is
# provably that wall, not a download/CA/owner failure. FORBIDDEN: faking github.com via /etc/hosts (Mandate ①).
# GREEN negatives (broker-side url_not_allowed / sha256_invalid / not_owner) are independently reachable today.
# Consumes S0-artifact (real https tarball + sha) + S0-layout (agent binary in ~/.local/bin). U1-pinned N=1 cluster.
#
# FALSE-GREEN GUARDS (plan §10-31):
#  - #28 signature is `url_not_allowed_local` (the `_local` suffix); the broker-side `url_not_allowed` (no _local),
#    download_failed, sha256_mismatch, not_owner all TRIP the guard → HARD FAIL (wrong-reason can't pass).
#  - the 3-point precondition (agent can fetch+trust the URL, broker DID whitelist it, agt1 ONLINE+owner) makes
#    `url_not_allowed_local` the ONLY remaining possible wall.
#  - broker `url_not_allowed` negative uses a VALID 64-hex sha so it can't be short-circuited by sha256_invalid.
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/agentyaml.sh"; . "$HERE/drills/lib/ident.sh"
. "$HERE/drills/lib/ingress.sh"; . "$HERE/drills/lib/artifact.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
CA=/usr/local/share/ca-certificates/tether-sim-ca.crt
VALIDSHA=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef   # valid 64-hex, wrong value
trap 'artifact_down 2>/dev/null' EXIT INT TERM
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
    dexec brk1 -- sh -c "grep -q 'url_allow' /etc/tether/broker.yaml || printf '  upgrade:\n    url_allow:\n      - https://%s-artifact/\n' '$INSTANCE' >> /etc/tether/broker.yaml" \
        && sctl brk1 restart tether-broker && poll_until 30 2 "brk1 broker back after url_allow config" -- _broker_up
}
# after a restart, is-active is NOT enough — the broker must be FUNCTIONALLY ready (leader elected) before
# session create works, or it fails "no active session". Poll the leader view.
_broker_up() { "$SIM" exec brk1 -- tether cluster status --json 2>/dev/null | jq -e '.leader_id=="brk1"' >/dev/null 2>&1; }

# #28 3-point discriminator predicates (make url_not_allowed_local the ONLY possible wall)
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

drill_begin "31-node-upgrade-fleet (#28 agent allow-list unconfigurable + GREEN broker negatives)"
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
log "31-DBG upgrade full output >>> $("$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA" 2>&1 | tr '\n' '~')"

# ── Arm #28 — agent-side allow-list is unconfigurable (EXPECTED-RED, assert_bug) ──────────────────────
assert_ok "P28-i  precondition: agt1 can FETCH+TRUST the artifact URL (rules out download_failed/CA)"  _pre_reachable
assert_ok "P28-ii precondition: broker WHITELISTED the URL (rules out broker url_not_allowed)"          _pre_broker_allows
assert_ok "P28-iii precondition: agt1 ONLINE + owner=caller (rules out node_offline/not_owner)"         _pre_agt1_owner
assert_bug "#28 self-hosted mirror upgrade (broker-allowlisted) REFUSED by agent local allow-list" \
           "#28" "url_not_allowed_local" \
           "$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA"

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
# F3 — --all DISPATCHES to the ONLINE target + config-abort on #28: targets only agt1 (agt2 excluded), dispatches,
# and aborts on url_not_allowed_local (a CONFIG error — node.go:221/318 isConfigError aborts the fleet). Proves the
# fleet enumerated+dispatched (not a no-op), and that the OFFLINE agt2 was never dispatched.
_all28=$("$SIM" ctl -- node upgrade --all --url "$ART_URL" --sha256 "$ART_SHA" 2>&1); _all28_rc=$?
log "31-DBG --all(#28) rc=$_all28_rc out=$(printf '%s' "$_all28" | tr '\n' '~')"
assert_ok "F3 --all dispatched to ONLINE agt1 → aborted on the #28 url_not_allowed_local config wall (fleet reached an agent, not a no-op)"  sh -c '[ "$1" -ne 0 ] && printf "%s" "$2" | grep -q "url_not_allowed_local"' _ "$_all28_rc" "$_all28"
assert_ok "F3b --all did NOT dispatch to the OFFLINE agt2 (enumerated out — the abort names agt1, never agt2)"  sh -c '! printf "%s" "$1" | grep -q "agt2"' _ "$_all28"
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
warn "31 NOT-COVERED (external-review M4, honest): a SUCCESSFUL fleet upgrade (PID-preserving re-exec + version bump + two-node summary) is UNREACHABLE while #28 walls every self-hosted URL at the agent allow-list. This arm covers ENUMERATION (F1/F2), DISPATCH + config-abort (F3), and --timeout + transient-skip (F4) — NOT the success/PID/version path, which flips in once #28 is fixed."

drill_end
