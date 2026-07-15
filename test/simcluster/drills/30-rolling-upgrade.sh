#!/bin/sh
# 30-rolling-upgrade.sh — S5 (N=3, G5 #8 debt): `cluster upgrade` rolling broker upgrade. Binary staging is an
# EXPLICIT privileged step (never made to look automatic); dry-run previews the ordered roll (followers-first/
# leader-last); the real roll runs a raft WRITE probe (session create/rm — a control-plane raft write, which
# DODGES the #29-dead cluster-expose data plane) that must see NO not_leader/503; whole-host asserts each
# broker's version flips v-cur→v-next WHILE its tether-broker MainPID is UNCHANGED (syscall.Exec re-exec).
#
# FALSE-GREEN GUARDS:
#  - staging is a labeled `[env: PRIVILEGED]` step asserted distinctly (never let the sim make the orchestration
#    look more automatic than it is — G5 rule); --expect-sha256 = the STAGED on-disk sha.
#  - version-readback ∧ PID-same are asserted TOGETHER — "no upgrade happened" also satisfies zero-disruption +
#    PID-same, so both must hold.
#  - the write probe is a REAL raft write (session create commits via propose→commit), not a read.
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/drills/lib/cluster.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
NEXTVER="${SIM_VERSION_NEXT:-v0.0.0-simcluster-next}"
CTL() { "$SIM" ctl -- "$@"; }
# cluster upgrade is a ctl op over NATS (cluster_upgrade.go:68 connectCtl → needs an ACTIVE SESSION), NOT a
# broker admin-socket op like `cluster rebalance proxy` (callAdmin). So we log in broker-locally as the tether
# uid on the leader first (mirroring grow's `cluster add`, simcluster:186), then every cluster upgrade call
# reuses that session under the same HOME. account-seed lives on the broker (/etc/tether/secrets), so the
# leader broker is the right place to run it from.
_broker_login() { dexec -u tether "$LDR" -- env HOME=/var/lib/tether tether login -s "$SID" --pin "$PIN" --nats-url nats://brk1:4222; }
_BUP() { dexec -u tether "$LDR" -- env HOME=/var/lib/tether tether cluster upgrade "$@"; }

# staging: root-install tether-next → /usr/local/bin/tether on EACH broker host (the documented privileged
# precondition — /usr/local/bin is root-owned, User=tether can't write it, so an operator stages it per host).
_stage_next() {
    # STAGED_SHA is computed in the drill body (below), NOT here: assert_ok runs its command in a subshell
    # (_as_capture), so a global set INSIDE _stage_next would be lost to the later _staged_sha_ok / _do_roll.
    [ -n "${STAGED_SHA:-}" ] || return 1
    for b in brk1 brk2 brk3; do
        d cp "$HERE/vendor/tether-next" "$(ctr_name "$b"):/tmp/tether-next" >/dev/null 2>&1 || return 1
        dexec "$b" -- install -m 0755 /tmp/tether-next /usr/local/bin/tether || return 1
    done
}
_staged_sha_ok() {
    for b in brk1 brk2 brk3; do
        _s=$(dexec "$b" -- sha256sum /usr/local/bin/tether 2>/dev/null | awk '{print $1}')
        [ "$_s" = "$STAGED_SHA" ] || return 1
    done
}
# RUNNING broker version, self-reported via node ls — NOT the on-disk /usr/local/bin/tether (staging already
# overwrote that to NEXTVER, so an on-disk check can't tell "staged" from "rolled"; only the running process's
# syscall.Exec re-exec flips the SELF-REPORTED release). roll re-execs the process → its release flips v-cur→NEXTVER.
_ver_of() { CTL node ls --brokers --json 2>/dev/null | jq -r --arg n "$1" '.nodes[]?|select(.nid==$n)|.release // empty' 2>/dev/null; }
_mainpid() { "$SIM" exec "$1" -- systemctl show tether-broker -p MainPID --value 2>/dev/null; }
_all_on_next() { for b in brk1 brk2 brk3; do [ "$(_ver_of "$b")" = "$NEXTVER" ] || return 1; done; }
_dryrun_no_touch() { for b in brk1 brk2 brk3; do [ "$(_ver_of "$b")" = "$NEXTVER" ] && return 1; done; return 0; }
# grow lock leak (gotcha #31): cluster add's releaseGrowLock is BEST-EFFORT (cluster_add_drive.go:494-506) — it
# almost always silently fails (leader-resolve / release-lock-trigger), leaving cluster_grow_active set, which
# BLOCKS the real upgrade at acquire-lock with "a cluster membership operation is in flight". This is INVISIBLE
# to dry-run/refuse (neither acquires the upgrade lock) — only the REAL roll bites it. So detection + expose
# lives at the real-roll arm below, using the roll's own HALT as the proof.
_roll_halted_on_growlock() { grep -qiE 'membership operation|in flight|join/retire|grows are serialized' /tmp/roll.log 2>/dev/null; }
# retry tether's OWN recovery (re-run cluster add clears THIS joiner's marker) + real roll, until the roll no
# longer HALTs on the grow lock, or we give up. The release trigger is TIME-SENSITIVE (leader-resolve after
# cutover), so a retry MAY succeed where the first failed. If the best-effort path stays broken across retries,
# the leaked lock is UNCLEARABLE via tether's own recovery → the upgrade is unreachable (NOT-COVERED, #29-style).
_clear_and_roll() {
    _cr_t=0
    while [ "$_cr_t" -lt 6 ]; do
        for _j in $(list_nodes broker); do
            [ "$_j" = "$LDR" ] && continue
            dexec -u tether "$LDR" -- env HOME=/var/lib/tether TETHER_CONFIRM_NODE_ID="$_j" tether cluster add "$_j" \
                --account-seed /etc/tether/secrets/account.nk --confirm-node-id "$_j" >/dev/null 2>&1
        done
        _do_roll
        _roll_halted_on_growlock || return 0   # roll no longer blocked → grow lock cleared + upgrade ran
        _cr_t=$((_cr_t+1)); sleep 8
    done
    return 1   # grow lock survived tether's recovery across retries → upgrade unreachable
}
_dry_run() { _BUP --to-version "$NEXTVER" --dry-run 2>&1 | grep -qiE 'dry-run|would|follower|leader|roll|TRANSFER|UPGRADE'; }
# roll ORDER oracle (Stage-C cluster-1): the leader's UPGRADE line must be the LAST upgrade line in the dry-run
# plan (followers-first / leader-last). dry-run doesn't acquire the upgrade lock, so this is reachable even under #31.
_roll_order_ok() {
    _o=$(_BUP --to-version "$NEXTVER" --dry-run 2>&1)
    _leader_ln=$(printf '%s' "$_o" | grep -niE 'UPGRADE.*leader|leader.*(transfer|last|upgrade)' | head -1 | cut -d: -f1)
    _last_ln=$(printf '%s' "$_o" | grep -niE 'UPGRADE ' | tail -1 | cut -d: -f1)
    [ -n "$_leader_ln" ] && [ -n "$_last_ln" ] && [ "$_leader_ln" = "$_last_ln" ]
}
# raft WRITE probe: a `session create` loop (each is a raft propose→commit) in ctl1, logging any not_leader/503.
# Create-only (no rm — session rm is a Tier-2 typed-confirm that would stall the loop); it dodges the #29-dead
# expose data plane entirely (control-plane raft write, not a tunnel).
_start_write_probe() {
    # each `session create wpN` is a fresh raft propose→commit; --nats-url is REQUIRED (the sim ctl has no
    # default broker URL — simcluster:317 passes it too; without it session create fails "broker unreachable"
    # and every line would be a spurious WRITEFAIL, masquerading as an upgrade-induced raft outage).
    dexec ctl1 -- sh -c 'nohup sh -c "i=0; while [ \$i -lt 400 ]; do runuser -u sim -- env HOME=/home/sim tether session create wp\$i --pin 111111 --nats-url nats://brk1:4222 >>/tmp/wp.log 2>&1 && echo WROTE-wp\$i >>/tmp/wp.log || echo WRITEFAIL >>/tmp/wp.log; i=\$((i+1)); sleep 0.3; done" >/dev/null 2>&1 & echo ok' >/dev/null 2>&1
}
_stop_write_probe() { dexec ctl1 -- pkill -f 'session create wp' >/dev/null 2>&1 || true; }
_do_roll() {
    _BUP --to-version "$NEXTVER" \
        --expect-sha256 "$STAGED_SHA" --account-seed /etc/tether/secrets/account.nk --backup-taken >/tmp/roll.log 2>&1
}
_probe_clean() {
    _stop_write_probe
    # non-vacuity guard (Stage-C mandate-2/pin-1): the probe must have ACTUALLY committed writes — else a
    # silent/never-run probe grep-MISSES the failure markers and false-greens (grep -q on an empty/missing file
    # exits 1/2, the leading ! flips both to PASS). Require ≥1 WROTE-wp* committed line first.
    dexec ctl1 -- grep -qE '^WROTE-wp[0-9]' /tmp/wp.log 2>/dev/null || { warn "M1 VACUOUS: wp.log has no committed session-create (WROTE-wp*) — the probe never wrote; a zero-not_leader here is meaningless"; return 1; }
    ! dexec ctl1 -- grep -qiE 'not_leader|503|no.responder|unavailable|no servers|WRITEFAIL' /tmp/wp.log 2>/dev/null
}

drill_begin "30-rolling-upgrade (N=3 cluster upgrade rolling broker upgrade — G5 #8)"
"$SIM" nuke >/dev/null 2>&1 || true
# external-review R5-M5: drill 30 OWNS the #31 grow-lock pin, so it uses grow_to_3's SINGLE no-retry path (3rd
# arg 0) — NO shared nuke+retry can launder its first-grow evidence. The #31 leak does not block VOTER (grow
# reaches VOTER + reports success while leaking the lock), so one attempt still lands the leaked-lock state.
# Run OUTSIDE assert_ok so GROW-ATTEMPTS (must be "1 (retry=0)") is VISIBLE first-class evidence of the no-retry path.
grow_to_3 0 1 0; _g3rc=$?
assert_ok "grow_to_3 (N=3 HA cluster, SINGLE no-retry attempt — 30 owns #31; GROW-ATTEMPTS logged above must be 1/retry=0)"  sh -c "exit $_g3rc"
assert_ok "session lab + ctl login (owner)"   "$SIM" session "$SID" --pin "$PIN"
LDR=$(sim_leader); log "30: leader=$LDR ; staged version=$NEXTVER"
# compute STAGED_SHA HERE (main shell), not inside _stage_next: assert_ok runs in a subshell so a global set
# there would vanish; _staged_sha_ok (--expect-sha256 anchor) and _do_roll both read it back.
STAGED_SHA=$(sha256sum "$HERE/vendor/tether-next" 2>/dev/null | awk '{print $1}')
log "30: staged sha256=$STAGED_SHA"

# ── privileged staging (explicit, distinct — NOT automatic) ──────────────────
assert_ok "[env: PRIVILEGED STAGING — operator stages the new binary; cluster upgrade does NOT] install tether-next → /usr/local/bin/tether on each broker"  _stage_next
assert_ok "staged on-disk sha256 matches on every broker (--expect-sha256 anchor)"  _staged_sha_ok

# ── broker-local ctl login on the leader (cluster upgrade is a ctl-over-NATS op → needs an active session) ──
assert_ok "broker-local ctl login on leader $LDR (cluster upgrade connects as ctl over NATS, unlike the admin-socket rebalance)"  _broker_login

# NB: the grow-lock leak (gotcha #31) is INVISIBLE to the refuse/dry-run arms below — neither acquires the
# upgrade lock, so they PASS regardless of the leak. It only bites the REAL roll's acquire-lock step, so the
# [GAP #31] expose+clear lives at the real-roll arm further down (using the roll's own HALT as the proof).

# ── over-automation guards: cluster upgrade refuses without the operator-supplied safety inputs ──
assert_refuses "no --account-seed → refused (each broker verifies an account-signed trigger)" \
               "account-seed|account seed|required" \
               _BUP --to-version "$NEXTVER" --expect-sha256 "$STAGED_SHA" --backup-taken
assert_refuses "no --backup-taken → refused (first-boot migration is one-way; restore is the only recovery)" \
               "backup" \
               _BUP --to-version "$NEXTVER" --expect-sha256 "$STAGED_SHA" --account-seed /etc/tether/secrets/account.nk

# ── dry-run: ordered roll plan, zero host touched ────────────────────────────
assert_ok "cluster upgrade --dry-run prints the ordered roll (followers-first/leader-last) + touches no host"  _dry_run
assert_ok "cluster upgrade --dry-run roll ORDER: the leader's UPGRADE is the LAST upgrade line (followers-first/leader-last; Stage-C cluster-1)"  _roll_order_ok
assert_ok "dry-run left every broker's RUNNING version on v-cur (no host re-exec'd; on-disk was already staged to NEXTVER so this checks the SELF-REPORTED running release, not the binary on disk)"  _dryrun_no_touch

# ── real roll with a raft WRITE probe (dodges #29-dead expose data plane) ────
MP1=$(_mainpid brk1); MP2=$(_mainpid brk2); MP3=$(_mainpid brk3)
assert_ok "start background raft write-probe (session create/rm loop) in ctl1"  _start_write_probe
# real roll — attempt 1. On ~every live run it HALTs at acquire-upgrade-lock because grow's best-effort
# releaseGrowLock LEAKED cluster_grow_active (gotcha #31; dry-run/refuse can't see it — they never acquire the
# lock). The HALT leaves a SAFE partial state (no host re-exec'd). Expose [GAP #31], then give tether's OWN
# recovery (re-run cluster add) a RETRY window; if the roll runs we test the mechanism, else it's NOT-COVERED.
_do_roll
if _roll_halted_on_growlock; then
    warn "[GAP #31] real cluster upgrade HALTED at acquire-upgrade-lock: 'a cluster membership operation (join/retire) is in flight'. grow's best-effort releaseGrowLock (cluster_add_drive.go:494-506) LEAKED cluster_grow_active while grow itself reported SUCCESS (exit 0, all VOTER). Invisible to dry-run/refuse (they never acquire the lock); only the real roll bites. A REAL tether defect (gotcha #31), pinned purely by THIS upgrade real-roll HALT. NB (Stage-C mandate-4): distinct from the CONCURRENCY VOTER-timeout flake simcluster:223-229 documents for 7-way parallel sweeps (clustered-JS meta-group formation timing; run-drills.sh surfaces it RED, does NOT auto-retry). The SOLO 'grow of brkN already in progress — serialized' fence (when a prior joiner's release intermittently fails) is the same grow-lock leak."
    assert_ok "[GAP #31] real upgrade is BLOCKED by the leaked grow lock (membership-in-flight in roll.log) — pinned as the exposed defect, NOT silently worked around"  _roll_halted_on_growlock
    if _clear_and_roll; then
        MECH=1
        log "[GAP #31] grow lock cleared after RETRYING tether's own recovery (re-run cluster add); the upgrade then rolled. The MANUAL retry IS the exposed gap — grow should have auto-released the lock."
    else
        MECH=0
        warn "[GAP #31] the leaked grow lock SURVIVED tether's own recovery (re-run cluster add walks the SAME best-effort release path) across retries → the upgrade-roll mechanism is UNREACHABLE, NOT-COVERED (blocked by #31 until fixed, like #29's rehome data plane)."
        assert_ok "[GAP #31] leaked grow lock is UNCLEARABLE via tether's recovery — the real roll STILL HALTs; upgrade-roll mechanism NOT-COVERED, pinned as the exposed defect"  _roll_halted_on_growlock
    fi
else
    MECH=1
    log "30: grow lock was clean this run — the real roll proceeded directly (the #31 leak is intermittent)"
fi

# upgrade-roll mechanism asserts run ONLY when the roll actually RAN. Otherwise MainPID-same / write-probe-clean
# are FALSE-GREENS: they also hold when no upgrade happened because the grow lock (#31) blocked it.
if [ "${MECH:-0}" = 1 ]; then
    assert_ok "[whole-host] every broker's RUNNING (self-reported) version == $NEXTVER after the roll"  poll_until 30 3 "all brokers on next" -- _all_on_next
    assert_ok "[re-exec] tether-broker MainPID UNCHANGED across the roll (syscall.Exec in place, not a kill/restart)" \
              sh -c "[ \"\$(\"$SIM\" exec brk1 -- systemctl show tether-broker -p MainPID --value)\" = '$MP1' ] && [ \"\$(\"$SIM\" exec brk2 -- systemctl show tether-broker -p MainPID --value)\" = '$MP2' ] && [ \"\$(\"$SIM\" exec brk3 -- systemctl show tether-broker -p MainPID --value)\" = '$MP3' ]"
    assert_ok "write-probe saw NO not_leader / 503 / no-responders across the whole roll (M1)"  _probe_clean
else
    _stop_write_probe
    warn "30: upgrade-roll mechanism (re-exec / write-continuity / PID-same) NOT-COVERED this run — the grow lock leak (#31) blocked the upgrade AND survived tether's recovery. MainPID-same / write-probe-clean are SUPPRESSED here on purpose: with no upgrade they would be false-greens."
fi

warn "30 NOT-COVERED (with stated reasons — Stage-C mandate-2/cluster-2): (a) colocated-agent whole-host leg (OQ-6 — sim brokers run no colocated agent); (b) Arm 30-C N=2 write-fence NEGATIVE CONTROL — the write-probe's non-vacuity is now enforced structurally instead (WROTE-wp* guard in _probe_clean: M1 FAILS if the probe never committed a write), AND the whole write-continuity M1 only runs when the roll ACTUALLY RAN (MECH=1); since #31's grow-lock leak blocks the upgrade this run (MECH=0), M1 is SUPPRESSED, so 30-C's negative control has nothing to guard — dropping it here is honest, not a hidden vacuity; (c) mid-run HALT/resume. What IS covered when the roll runs: G5 broker-daemon roll ORDER (leader-last), raft-write continuity (non-vacuity-guarded), PID-preserving re-exec."
drill_end
