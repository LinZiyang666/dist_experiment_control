# drills/lib/cluster.sh — G-A shared cluster fixture. Sourced by drills 71/73/74/30 (and future S6/S7).
# Depends on lib/docker.sh + lib/log.sh (poll_until) + the drill's $SIM. Keeps the "how to stand up an
# N=3 HA cluster" recipe in ONE place so §9's concurrent-grow flake accounting is accurate (the grow
# family shares this exact path).

# grow_to_3 [agents=0] [ctl=1] [retry=1] : up 3 brokers + 1 ctl (+ agents), init brk1 to N=1, grow brk2+brk3 to a
# functional N=3 HA cluster. Each `$SIM grow` internally polls up to 150s for the joiner to reach VOTER
# (see drill 10-grow-to-3), so this returns only once all three are VOTER. Non-zero on any step failure.
#
# retry (3rd arg, default 1) — external-review R5-M5 (attempt-accounting honesty):
#   - retry=1 (71/73/74, which only NEED a clean N=3 as SETUP; drill 30 OWNS + pins #31 separately): the N=3 grow
#     intermittently strands a reachable learner in CATCHING_UP short of VOTER (#47). Supply a clean
#     N=3 by NUKING + retrying the WHOLE grow ONCE. The attempt count + per-attempt grow rc are emitted as
#     FIRST-CLASS EVIDENCE (a `GROW-ATTEMPTS: N` trailer + a per-attempt log line + a warn on each retry), so a
#     run's "strict / single-attempt" claim is LOG-VERIFIABLE — never a silent laundering of product evidence.
#   - retry=0 (drill 30): a SINGLE grow attempt with NO nuke/retry. Drill 30 uses this so its own first-grow #31
#     state is NEVER laundered by a shared silent retry — a genuine short grow there goes RED honestly. (The #31
#     lock leak does NOT block VOTER — grow reports success + reaches VOTER, so 30's single attempt still lands
#     the leaked-lock state it needs to pin the upgrade-blocked defect.)
# grow rc is CAPTURED + logged (never silently `|| true`d away). NB do NOT abort lingering ops or release the
# grow lock here: drill 30 RELIES on the #31 grow-lock leak to pin the upgrade-blocked defect.
grow_to_3() {
    _g3_a=${1:-0}; _g3_c=${2:-1}; _g3_retry=${3:-1}; _g3_try=1
    if [ "$_g3_retry" = 0 ]; then _g3_max=1; else _g3_max=2; fi
    while [ "$_g3_try" -le "$_g3_max" ]; do
        "$SIM" up --brokers 3 --agents "$_g3_a" --ctl "$_g3_c" || return 1
        "$SIM" init brk1 || return 1
        "$SIM" grow brk2; _g3_r2=$?
        "$SIM" grow brk3; _g3_r3=$?
        log "grow_to_3: attempt $_g3_try/$_g3_max (retry=$_g3_retry) — grow brk2 rc=$_g3_r2, grow brk3 rc=$_g3_r3 (rc!=0 = that joiner did not reach VOTER)"
        if poll_until 90 3 "N=3 all VOTER" -- _three_voters; then
            # R6 (non-blocking advice): a non-zero `grow brkN` rc that nonetheless reaches VOTER during the extra
            # 90s poll is a LATE-CONVERGENCE / CLI-timeout defect — label it explicitly, never a silent clean GREEN.
            if [ "$_g3_r2" != 0 ] || [ "$_g3_r3" != 0 ]; then warn "grow_to_3: LATE-CONVERGENCE — a \`grow\` CLI returned non-zero (brk2 rc=$_g3_r2, brk3 rc=$_g3_r3) yet N=3 reached VOTER during the extra 90s poll. The grow CLI timed out / returned before convergence though the joiner later converged — a labeled CLI-timeout defect, NOT a clean grow (R6 advice)."; fi
            printf 'GROW-ATTEMPTS: %s (retry=%s)\n' "$_g3_try" "$_g3_retry"   # first-class attempt evidence (R5-M5)
            return 0
        fi
        if [ "$_g3_try" -lt "$_g3_max" ]; then
            warn "grow_to_3: N=3 not reached on attempt $_g3_try (intermittent #47 catch-up failure or another real grow defect) — NUKING + retrying the whole grow ONCE only for legacy retry=1 consumers; strict review drills pass retry=0"
            "$SIM" nuke >/dev/null 2>&1 || true
        fi
        _g3_try=$((_g3_try+1))
    done
    printf 'GROW-ATTEMPTS: %s (INCOMPLETE, retry=%s)\n' "$_g3_max" "$_g3_retry"
    if [ "$_g3_retry" = 0 ]; then
        err "grow_to_3: N=3 NOT reached in the SINGLE no-retry attempt (drill 30 path) — a REAL short grow, NOT retried; honest RED (R5-M5)"
    else
        err "grow_to_3: N=3 NOT reached after $_g3_max attempts — a persistent grow failure (not the intermittent flake); refusing"
    fi
    return 1
}

_three_voters() {
    _tv_l=$(sim_leader) || return 1
    [ "$("$SIM" exec "$_tv_l" -- tether cluster status --json 2>/dev/null \
        | jq '[.nodes[]?|select(.phase=="VOTER")]|length' 2>/dev/null)" = 3 ]
}

# sim_leader : node_id of the current raft leader via the `status --json` machine surface (authoritative
# leader view, is_leader_view=true; drill 10). Falls back to asking brk1 directly. Prints the id, or fails.
sim_leader() {
    _sl=$("$SIM" status --json 2>/dev/null | jq -r 'select(.is_leader_view==true).leader_id // .leader_id // empty' 2>/dev/null | head -1)
    [ -n "$_sl" ] && { printf '%s' "$_sl"; return 0; }
    _sl=$("$SIM" exec brk1 -- tether cluster status --json 2>/dev/null | jq -r '.leader_id // empty' 2>/dev/null)
    [ -n "$_sl" ] && { printf '%s' "$_sl"; return 0; }
    return 1
}

# a_non_leader_voter : print one VOTER broker that is NOT the current leader (for `--on-broker` home pins
# and drain/kill targets that must keep the leader alive as the authoritative event emitter). Fails if none.
a_non_leader_voter() {
    _nlv_l=$(sim_leader) || return 1
    for _nlv_b in $(list_nodes broker); do
        [ "$_nlv_b" = "$_nlv_l" ] && continue
        node_running "$_nlv_b" || continue
        printf '%s' "$_nlv_b"; return 0
    done
    return 1
}
