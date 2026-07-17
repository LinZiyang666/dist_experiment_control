#!/bin/sh
# 92-js503-remote-alert.sh — S8 (N=2, G7b debt): the `cluster status --remote` quorum-loss / force-single
# surface (the operator's off-box eyes; ctl-over-NATS, no SSH). Two legs:
#  (a) 22-INDEPENDENT: kill the peer (N=2 quorum loss) → --remote exit 2 + "READ-ONLY / no writable leader"
#      + `session rm` default-refuse (BLOCKED, needs --ack-alerts) → --ack-alerts bypasses the advisory gate.
#      The JS-503 BANNER itself is by-design NOT-COVERED here (a natural N=2 quorum loss is control-plane-DOWN,
#      already surfaced as exit-2 READ-ONLY; the banner targets the control-healthy/data-wedged case — a RED
#      for correct code = Mandate inversion, plan §11-U3).
#  (b) #20③ DEDICATED leg (rides 22 = REACHABLE per SB-22): online force-single leaves the conf clustered →
#      --remote → `DATA-PLANE DEGRADED — JetStream UNAVAILABLE` + exit 3 + `reconcile nats --to-standalone`
#      remedy → operator to-standalone recovery → banner clears + tier-B works again (R-DATAPLANE terminus).
#
# FALSE-GREEN GUARDS (plan §9-92): the survivor is provably alive/serving while JS-meta is 1/2 (not "all
# failed"); --ack-alerts asserts the gate was BYPASSED (a downstream discriminator), not command-success;
# recovery ends in tier-B push working again, not a status field.
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/setup-forcesingle.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab
# cluster status --remote from the ctl (ctl-over-NATS). Capture rc separately (status exits with a health code).
_remote()     { "$SIM" ctl -- cluster status --remote 2>&1; }
_remote_rc()  { "$SIM" ctl -- cluster status --remote >/dev/null 2>&1; echo $?; }
_remote_readonly() {
    _rr_out=$("$SIM" ctl -- cluster status --remote --json 2>&1); _rr_rc=$?
    [ "$_rr_rc" = 2 ] && printf '%s' "$_rr_out" | jq -e '.schema=="ctl_cluster_summary" and .view=="ctl-remote" and .any_reply==true and .all_stale==true and .writable_leader_seen==false' >/dev/null 2>&1
}

drill_begin "S8-92 js503-remote-alert: (a) quorum-loss READ-ONLY + --ack-alerts (b) force-single JS-503 banner (G7b)"

# ── SETUP: healthy N=2 clustered-JS (fixture asserts JS cluster_size==2 + tier-B baseline before any kill) ─
setup_forcesingle_n2
assert_setup "baseline ctl login" "$SIM" ctl -- login -s "$SID" --pin "$PIN"
RM_SID="rmcase-$$"
assert_setup "create an independent destructive-gate session" "$SIM" session "$RM_SID" --pin 246810

# Creating RM_SID activates it; switch back to the baseline. RM_SID remains an independent destructive
# target for M17 in leg-b, where force_single_active is present but the N=1 control plane is writable.
assert_setup "restore baseline ctl session before quorum-loss leg" "$SIM" ctl -- login -s "$SID" --pin "$PIN"

# ── BASE (FIRST): healthy --remote → no DATA-PLANE DEGRADED banner + writable verdict (exit 0) ───────
log "DIAG healthy --remote (rc=$(_remote_rc)) →"; _remote | sed 's/^/[diag base] /' | head -6
assert_ok "BASE: healthy --remote has NO 'DATA-PLANE DEGRADED' banner" \
    sh -c "! $SIM ctl -- cluster status --remote 2>&1 | grep -qiE 'DATA-PLANE DEGRADED|JetStream UNAVAILABLE'"

# ── Leg (a): kill peer (N=2 quorum loss) → --remote exit 2 + READ-ONLY ──────────────────────────────
node_kill brk2
assert_ok "a③ brk2 :7400 refused (peer provably dead; quorum lost 1/2)" poll_until 30 2 "brk2 :7400 refused" -- tcp_refused brk2 7400
log "DIAG quorum-loss --remote (rc=$(_remote_rc)) →"; _remote | sed 's/^/[diag a] /' | head -8
# Stage-C M1 CORRECTION: #42 is a BOUNDED ~TFence(10s) WINDOW gotcha, NOT a permanent mis-report (the earlier
# assert_bug asserting "permanent" was a mis-classified RED — the opposite mandate error). `--remote` is
# TFence-lagged (read.go:18): within ~10s of the kill it still says "electing a leader (transient)", but
# AFTER TFence it SELF-CORRECTS to READ-ONLY / exit 2 (cluster_status_nats.go:116-136). §11-U3's exit-2
# READ-ONLY verdict STANDS (TFence-delayed), NOT overturned. HONEST oracle = the surface self-corrects (GREEN).
# measure-and-record: does --remote self-correct to READ-ONLY after TFence? Poll 90s (TFence + leader-lease
# + the over-NATS aggregate refresh may exceed the ~10s Stage-C M1 estimate). If it self-corrects → GREEN;
# if it does NOT even after 90s, that STRENGTHENS #42 (the misleading "transient" window is much longer than
# ~10s, closer to persistent) — recorded honestly as a stronger exposure, not hidden.
if poll_until 90 4 "READ-ONLY remote JSON + exact rc2 (post-TFence)" -- _remote_readonly; then
    _as_pass "a-GREEN: one --remote --json sample has rc=2, all_stale=true, writable_leader_seen=false after TFence"
else
    # --remote did NOT self-correct to READ-ONLY within 90s of the kill — the misleading "electing a leader
    # (transient)" verdict persists FAR beyond the ~10s TFence estimate. This is #42 reproduced and STRONGER
    # than documented (the operator is misled far longer than thought) → PRODUCT-RED, not a silent warn-GREEN.
    product_red "#42 --remote quorum-loss verdict did NOT self-correct to READ-ONLY within 90s [#42-stronger] — the misleading 'transient' window persists far beyond the ~10s TFence estimate; the operator is misled far longer than documented"
fi
# #42 window (measure-and-record, timing-fragile — NOT a hard assert): the on-broker socket (fast) vs
# --remote (TFence-lagged) may disagree within the ~10s window. The ledger #42 documents it.
log "DIAG #42 window (on-broker socket verdict — flips ~1s after step-down, vs the TFence-lagged --remote above):"
"$SIM" exec brk1 -- runuser -u tether -- tether cluster status 2>&1 | grep -iE 'quorum|leader|force|read-only' | head -2 | sed 's/^/[socket] /' || true
log "BY-DESIGN SCOPE (plan §11-U3, CONFIRMED by Stage-C M2): leg-a does NOT assert the JS-503 BANNER — a natural N=2 quorum loss is control-plane-DOWN (already surfaced as exit-2 READ-ONLY above after TFence); the dedicated banner is leg-b's control-healthy/data-wedged case. Asserting the banner here would be a Mandate inversion (RED for correct code)."

# ── Recover to healthy N=2 for leg (b): restart brk2 + rejoin ───────────────────────────────────────
node_start brk2
assert_setup "leg-b prerequisite: N=2 fully recovers" poll_until 90 4 "N=2 back to 2 voters" -- sh -c "[ \"\$($SIM status --json 2>/dev/null | jq '[.nodes[]?|select(.phase==\"VOTER\")]|length')\" = 2 ]"
assert_setup "leg-b restore ctl login to the untouched baseline session" "$SIM" ctl -- login -s "$SID" --pin "$PIN"

# ── Leg (b): ONLINE force-single (rides 22 = REACHABLE, SB-22) → JS-503 banner + exit 3 + recovery ───
# Drive online force-single on brk1 (kill brk2, dwell, commit) — conf stays clustered (R3) → JS meta 1/2
# unavailable while brk1 is a control-healthy single leader → the DEDICATED banner fires.
node_kill brk2
assert_ok "leg-b brk2 is provably dead" poll_until 40 2 "brk2 dead" -- tcp_refused brk2 7400
log "leg-b: driving online force-single on brk1 (dwell ~25-70s, then pty commit)"
# Round-5 §M1 hygiene: --dry-run is zero-mutation so a truncation here is harmless, but the pattern is
# banned outright (lint) — a later edit dropping --dry-run would silently make it destructive. Capture to
# completion via out_matches instead of piping the command into grep -q.
assert_setup "leg-b online force-single dwell becomes eligible" poll_until 120 5 "dwell satisfied" -- \
    out_matches 'would proceed' "$SIM" exec brk1 -- runuser -u tether -- tether cluster recovery force-single --online --dry-run --self-id brk1 --confirm-peers-dead brk2
assert_setup "leg-b online force-single commit succeeds" "$SIM" exec brk1 -- runuser -u tether -- python3 /opt/sim/pty-confirm.py brk1 -- tether cluster recovery force-single --online --self-id brk1 --confirm-peers-dead brk2
sleep 5
log "DIAG force-single --remote (rc=$(_remote_rc)) →"; _remote | sed 's/^/[diag b] /' | head -8
# Stage-C M2: the `force.single` catch-all was a FALSE-GREEN (post-force-single the verdict ALWAYS contains
# "force_single_active", so it matched without the banner ever firing). Require the REAL JS-503 banner string
# ONLY, poll ≥70s (jsDownThreshold=60s), and corroborate with a data-plane probe failing. The remedy hint
# lives in the SAME `if JetStreamUnavailable` block (cluster_status_nats.go:161-168), so if the banner fires
# the remedy is present — the earlier #43 "missing remedy" was source-false (dropped; it violated §11-U3).
assert_ok "b: --remote force_single exit code == 3 (#16)" \
    sh -c "[ \"\$($SIM ctl -- cluster status --remote >/dev/null 2>&1; echo \$?)\" = 3 ]"
# M17 belongs here, not in natural quorum loss: auth_callout fail-closes before commands reach the gate
# while quorum is absent. Online force-single restores a writable N=1 control plane and persists the
# exact hard-gate marker, so default refusal + acknowledged committed deletion is a non-vacuous bypass.
assert_refuses "M17 force_single_active blocks destructive session rm by default" \
    "BLOCKED.*--ack-alerts|acknowledge the alert|force.single" "$SIM" ctl -- session rm "$RM_SID"
assert_ok "M17 --ack-alerts bypasses force_single_active and commits the delete" \
    "$SIM" ctl -- session rm "$RM_SID" --ack-alerts
assert_refuses "M17 committed write is observable: removed session cannot be activated" \
    "unknown session|not found|Authorization Violation|rejected" "$SIM" ctl -- login -s "$RM_SID"
if poll_until 90 6 "JS-503 banner" -- sh -c "$SIM ctl -- cluster status --remote 2>&1 | grep -qiE 'DATA-PLANE DEGRADED|JetStream UNAVAILABLE'"; then
    _as_pass "b: --remote shows the REAL 'DATA-PLANE DEGRADED — JetStream UNAVAILABLE' banner (≥60s sustained 503)"
    assert_ok "b: the banner CARRIES the 'reconcile nats --to-standalone' remedy (same JetStreamUnavailable block)" \
        sh -c "$SIM ctl -- cluster status --remote 2>&1 | grep -qiE 'to-standalone|reconcile nats'"
    # M1: a >8 MiB payload forces the tier-B (JetStream object-store) path — corroborate it FAILS while JS
    # meta is 1/2 (a small tier-A file would not exercise the JS plane).
    assert_ok "b: build a payload strictly larger than 8 MiB" "$SIM" exec ctl1 -- sh -c 'head -c 12582912 /dev/urandom > /tmp/js.bin && test "$(stat -c %s /tmp/js.bin)" -gt 8388608'
    _bfail=$("$SIM" ctl -- push /tmp/js.bin agt1:/tmp/js.bin --ack-alerts 2>&1); _bfrc=$?
    if [ "$_bfrc" != 0 ] && printf '%s' "$_bfail" | grep -qiE 'JetStream|jetstream_unavailable|503|tier.?B|object.?store'; then
        _as_pass "b: >8MiB tier-B push fails for an exact JetStream/503/tier-B reason while banner is active"
    else
        _as_fail "b: tier-B corroboration did not fail for a JetStream-specific reason (rc=$_bfrc)"
    fi
else
    # The JS-503 dedicated banner did not fire within 90s — the by-design non-firing case (#41 RESERVED,
    # needs a control-healthy sustained-503). Not exercised this run → an honest INCOMPLETE gap, never GREEN.
    product_red "#41 dedicated JS-503 banner failed to fire within 90s in a control-healthy online-force-single fixture with clustered JS 1/2"
fi
# Recovery: operator to-standalone + JS reset → tier-B works. explore→pin: tier-B recovery is timing/state
# fragile after a force-single (b-TERMINUS RED'd exit 70 first pass — could be an incomplete recovery or a
# real gap). Drive the documented recovery + RECORD the outcome (measure-and-record, don't hard-fail here).
assert_ok "leg-b recovery: reconcile nats --to-standalone succeeds" \
    "$SIM" exec brk1 -- runuser -u tether -- tether cluster reconcile nats --to-standalone --confirm-single \
        --server-name brk1 --account-issuer "$(secrets_account_pub "$INSTANCE")" --broker-nkey "$(secrets_broker_pub "$INSTANCE" brk1)"
assert_ok "leg-b recovery: JS reset and service restart both succeed" \
    "$SIM" exec brk1 -- sh -eu -c 'systemctl stop tether-broker nats-server; test -d /var/lib/tether/jetstream; mv /var/lib/tether/jetstream /var/lib/tether/jetstream.bak.$(date +%s); systemctl start nats-server tether-broker; test "$(systemctl is-active nats-server)" = active; test "$(systemctl is-active tether-broker)" = active'
assert_ok "leg-b recovery: ctl re-login succeeds after broker auth responder is ready" \
    poll_until 60 3 "ctl auth responder ready" -- "$SIM" ctl -- login -s "$SID" --pin "$PIN"
assert_ok "leg-b recovery: --remote banner clears on the recovered control plane" \
    poll_until 60 4 "JS-503 banner clears" -- sh -c "! $SIM ctl -- cluster status --remote 2>&1 | grep -qiE 'DATA-PLANE DEGRADED|JetStream UNAVAILABLE'"
# M1: recovery terminus uses a >8 MiB payload (real tier-B) and asserts the push stdout reports tier=b.
assert_ok "leg-b recovery: build a >8MiB tier-B terminus payload" "$SIM" exec ctl1 -- sh -c 'head -c 12582912 /dev/urandom > /tmp/b.bin && test "$(stat -c %s /tmp/b.bin)" -gt 8388608'
_bpush=$("$SIM" ctl -- push /tmp/b.bin agt1:/tmp/b.bin --ack-alerts 2>&1); _bprc=$?
printf '%s\n' "$_bpush" | sed 's/^/[recover push] /' | head -4
if [ "$_bprc" = 0 ] && printf '%s' "$_bpush" | grep -qiE 'tier[=: ]+b|tier b|via tier b|object.?store'; then
    _as_pass "b recovery TERMINUS: 12 MiB tier-B push WORKS again at N=1 standalone (rc=0 + stdout reports tier=b) after the operator to-standalone (R-DATAPLANE)"
else
    _as_fail "b recovery TERMINUS failed: rc=$_bprc or stdout did not identify tier=B"
fi

# ── Remote command surface: --homes, --remote+--offline mutex (explore→pin, capture real behavior) ───
log "DIAG --homes --remote / --remote+--offline →"; "$SIM" ctl -- cluster status --homes --remote 2>&1 | sed 's/^/[diag homes] /' | head -3; "$SIM" ctl -- cluster status --remote --offline 2>&1 | sed 's/^/[diag mutex] /' | head -2
assert_ok "REMOTE-homes: cluster status --homes --remote runs from ctl" "$SIM" ctl -- cluster status --homes --remote
# --remote + --offline mutex: assert it refuses (rc≠0); the exact message is captured in the diag above.
assert_refuses "REMOTE-mutex: --remote + --offline is an exact usage/mutex refusal" \
    "cannot be combined|mutually exclusive|--remote.*--offline|--offline.*--remote|if any flags in the group.*offline remote" \
    "$SIM" ctl -- cluster status --remote --offline

drill_end
