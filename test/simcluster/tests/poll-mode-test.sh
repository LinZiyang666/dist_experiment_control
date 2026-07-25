#!/bin/sh
# poll-mode-test.sh — HERMETIC gate for V2's fast-start poll grid and its exemption set. No docker.
#
# WHY THIS EXISTS. V2 rewrote poll_until from a fixed grid to a FAST-START grid GLOBALLY — every call
# site not converted to poll_until_fixed silently flipped fixed→fast. Two classes MUST stay fixed:
#   - EFFECTFUL predicates that run a product mutation per tick (a rebalance/expose-recreate each sample):
#     faster sampling = more mutations = a different load profile on the very drills (73/74) whose job is
#     the rebalance/flap defect families — a Mandate-① fidelity deviation.
#   - STABILITY-WINDOW predicates (leader-flap / distribution-settle): denser early sampling raises the
#     chance of banking a transiently-true state (the #46 class).
# Internal review round-2 MAJOR-1 found the first sweep missed `_construct_111` (which the PLAN named
# verbatim) and five other effectful sites, precisely because the exemption was AUDIT-enforced, not
# GATE-enforced. This gate makes it gate-enforced: a future `poll_until_fixed → poll_until` flip on any
# listed predicate fails here.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
DRILLS="$(cd "$HERE/../drills" && pwd)"
LOGSH="$HERE/../lib/log.sh"
FAILS=0
pass() { printf '  ok   %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; FAILS=$((FAILS+1)); }

echo "── the audited exemption set MUST be on poll_until_fixed ────────────────────────"
# drill:predicate pairs whose poll_until MUST be poll_until_fixed. EFFECTFUL (a mutation per tick) or
# STABILITY-WINDOW (#46 flap). Adding a new such predicate WITHOUT listing it here, or flipping a listed
# one back to plain poll_until, is the MAJOR-1 recurrence — caught below.
check_fixed() {
    _d="$DRILLS/$1.sh"; _p="$2"; _why="$3"
    [ -f "$_d" ] || { fail "$1: drill file missing"; return; }
    # every poll_until(_fixed) call whose LAST token is this predicate
    _bad=$(grep -nE "poll_until [0-9\$A-Za-z_]+ [0-9]+ \"[^\"]*\" -- $_p( |;|\$)" "$_d" | grep -v 'poll_until_fixed')
    if [ -n "$_bad" ]; then
        fail "$1:$_p is on plain poll_until but is $_why — must be poll_until_fixed"
        printf '       %s\n' "$_bad"
    else
        # non-vacuity: the predicate must actually be USED via poll_until_fixed somewhere
        if grep -qE "poll_until_fixed [0-9\$A-Za-z_]+ [0-9]+ \"[^\"]*\" -- $_p( |;|\$)" "$_d"; then
            pass "$1:$_p on poll_until_fixed ($_why)"
        else
            fail "$1:$_p — the gate references a predicate this drill no longer polls (stale row)"
        fi
    fi
}
# EFFECTFUL — a product mutation per tick
check_fixed 74-rebalance-on-return _construct_111      "EFFECTFUL (cluster rebalance proxy per tick)"
check_fixed 74-rebalance-on-return _rebalance_tick     "EFFECTFUL (cluster rebalance proxy per tick)"
check_fixed 73-proxy-cluster-ha    _construct_nontunnel "EFFECTFUL (cluster rebalance proxy per tick)"
check_fixed 73-proxy-cluster-ha    _qconstruct         "EFFECTFUL (cluster rebalance proxy per tick)"
check_fixed 32-install-lifecycle   _mkbiz              "EFFECTFUL (expose rm+create per tick)"
check_fixed 10-grow-to-3           _ha_write_commits   "EFFECTFUL (session create raft write per tick)"
# STABILITY-WINDOW — denser sampling could bank a transient
check_fixed 74-rebalance-on-return _dist_stable        "STABILITY-WINDOW (distribution settle)"
check_fixed 93-metrics-observability _wh_leader_stable "STABILITY-WINDOW (#46 leader flap)"
# RETRY-SPACING — a product command RE-ATTEMPTED per tick until a subsystem is ready (external review
# Major 1: these three were on plain fast; each re-issues a product op every sample).
check_fixed 22-forcesingle-online    _sra_ok           "RETRY-SPACING (set-raft-addr re-issued per tick)"
# AUDIT NOTE (round-2 MAJOR-1): _ctl_write_ok (40:275) and _d3_survivor_write (96:563) are EFFECTFUL
# (a session-create raft write per tick) but TERMINAL-SUCCESS-ONLY — a failed tick commits NOTHING and the
# poll exits at first success, so fast-start only makes FAILED, side-effect-free attempts more frequent.
# They are DELIBERATELY left on fast (bounded, no fidelity impact); recorded here so the sweep is provably
# complete rather than silently missing them.

echo "── mechanical backstop: no UNCLASSIFIED effectful plain-fast poll ──────────────"
# External review Major 1: a hand-maintained list cannot PROVE completeness. A reliable mechanical
# "does this predicate mutate?" classifier is infeasible in shell (function-body extraction bleeds into
# jq strings, comments, and adjacent functions — verified), so this is the honest middle: a NARROW
# heavy-mutation matcher over every plain-fast poll site, checked against a CLASSIFIED allowlist. Every
# site the matcher flags must be either (a) already poll_until_fixed, or (b) listed below with its human
# classification. A NEW flagged site that is neither fails this gate — forcing it to be classified rather
# than silently inheriting fast-start. The allowlist entries are drill:predicate → why-fast-is-OK; most
# are matcher false positives (a read-only predicate whose body extraction caught a neighbouring verb).
allow() { printf '%s\n' "$1"; }
ALLOWLIST="$(cat <<'EOF'
32-install-lifecycle:_biz_serves        false-positive: reads dp_curl; matcher caught neighbouring _mkbiz
40-drain-retire:_op_state_is            false-positive: jq read of op_state; verb is in a jq filter string
40-drain-retire:_retire_done            false-positive: jq read; verb in filter
40-drain-retire:_retire_pre_remove      false-positive: jq read; verb in filter
40-drain-retire:_ctl_write_ok           fast-ok: terminal-success-only session-create (a failed tick commits nothing)
72-proxy-subscription:_hold_exited      false-positive: test -f marker read
72-proxy-subscription:_exit_port_gone   false-positive: port-listen read
72-proxy-subscription:_proxy_alloc_zero false-positive: sqlite count read
74-rebalance-on-return:_ge3_eligible    false-positive: reads _elig_voters; caught neighbouring _construct_111
74-rebalance-on-return:_reg_ready       false-positive: expose-explain read
74-rebalance-on-return:_negctrl_post    false-positive: expose-explain read
74-rebalance-on-return:_auto_tick       false-positive: _spread_le1 read, no manual verb (drill comment)
74-rebalance-on-return:_par_landed      false-positive: admin-events read
82-agent-onboarding-invite:_m1_ok       false-positive: curl cluster.json read
82-agent-onboarding-invite:_m2_ok       false-positive: curl cluster.json read
91-client-converge:_ep_has              false-positive: seeds-show grep read
96-mid-flight-chaos:_d3_survivor_write  fast-ok: terminal-success-only session-create
EOF
)"
NARROW='cluster rebalance proxy[^-]|cluster (retire|drain|add) |set-raft-addr|expose rm |agent config (refresh|set)|recovery force-single|ctl -- login|_rebal '
bs_fail=0
for d in "$DRILLS"/[0-9]*.sh; do
    dn=$(basename "$d" .sh)
    grep -nE 'poll_until [0-9$A-Za-z_]+ [0-9]+ "' "$d" | grep -v poll_until_fixed | while IFS=: read -r ln rest; do
        pred=$(printf '%s' "$rest" | sed -E 's/.*-- (.*)$/\1/' | sed 's/;.*//' | awk '{print $1}')
        if printf '%s' "$rest" | grep -q 'sh -c'; then body="$rest"; else body=$(sed -n "/^$pred()/,/^}/{/^}/q;p}" "$d" 2>/dev/null); fi
        hit=$(printf '%s\n' "$body" | grep -vE '^\s*#|--dry-run' | grep -oE "$NARROW" | head -1)
        [ -n "$hit" ] || continue
        # inline sh -c site (no named predicate) → key by drill:line
        case "$pred" in sh|env|"$SIM"|'"$SIM"') key="$dn:INLINE@$ln" ;; *) key="$dn:$pred" ;; esac
        # already fixed at THIS site?  (the exact line uses poll_until_fixed — but we filtered those out,
        # so a flagged plain-fast site is NOT fixed here) → must be allowlisted.
        if printf '%s\n' "$ALLOWLIST" | grep -qE "^$dn:$pred[[:space:]]"; then
            :   # classified false-positive / fast-ok
        else
            printf 'UNCLASSIFIED\t%s\t%s\t%s\n' "$dn" "$ln" "$hit"
        fi
    done
done > "${TMPDIR:-/tmp}/pm-bs.$$" 2>/dev/null
if [ -s "${TMPDIR:-/tmp}/pm-bs.$$" ]; then
    while IFS="$(printf '\t')" read -r _t _dn _ln _hit; do
        [ -n "$_dn" ] || continue
        fail "an UNCLASSIFIED effectful-looking plain-fast poll at $_dn:$_ln (matched '$_hit') — convert to poll_until_fixed or add it to the classified allowlist in this gate"
    done < "${TMPDIR:-/tmp}/pm-bs.$$"
else
    pass "no unclassified effectful-looking plain-fast poll (backstop clean against the allowlist)"
fi
rm -f "${TMPDIR:-/tmp}/pm-bs.$$"

echo "── the -- false settle timers MUST be on poll_until_fixed ───────────────────────"
# A `-- false` poll never returns early — it is a deliberate settle WINDOW. Fast-start would just spin it
# with 1s wakeups. Any `poll_until … -- false` (not fixed) is a mis-classification.
badfalse=$(grep -rnE "poll_until [^|]*-- false" "$DRILLS"/*.sh | grep -v poll_until_fixed || true)
if [ -n "$badfalse" ]; then fail "a -- false settle timer is on plain poll_until (must be poll_until_fixed):"; printf '       %s\n' "$badfalse"
else pass "all -- false settle timers are poll_until_fixed"; fi

echo "── fast vs fixed dispatch BEHAVIOUR (the machinery itself) ──────────────────────"
# Prove poll_until (fast) samples BEFORE the declared interval while poll_until_fixed does not — this is
# what a mutation removing the fast branch or the mode guard would break.
. "$LOGSH"
# A predicate true on its 2nd sample. With interval 5: fast catches it at ~1s; fixed not before ~5s.
_c=0; _cond() { _c=$((_c+1)); [ "$_c" -ge 2 ]; }
_c=0; t0=$(date +%s); poll_until       30 5 "fast probe"  -- _cond; fast_el=$(( $(date +%s) - t0 ))
_c=0; t0=$(date +%s); poll_until_fixed 30 5 "fixed probe" -- _cond; fixed_el=$(( $(date +%s) - t0 ))
[ "$fast_el" -le 2 ]  && pass "poll_until (fast) caught a 2nd-sample condition in ${fast_el}s (≤2)"   || fail "fast took ${fast_el}s — fast-start not working (want ≤2)"
[ "$fixed_el" -ge 4 ] && pass "poll_until_fixed held the ${fixed_el}s grid (≥4)"                       || fail "fixed took ${fixed_el}s — the fixed grid was not honoured (want ≥5; a mode-guard mutation shows here)"
# The two MUST differ, or the mode dispatch is dead.
[ "$fixed_el" -gt "$fast_el" ] && pass "fixed grid is strictly slower than fast-start (dispatch is live)" || fail "fast and fixed behave identically — the [ \$_pu_mode = fast ] dispatch is dead"

echo "── cross-mode NESTING preserves each frame's mode (external review Major 1) ─────"
# Mode lives in the frame, not a global, so an inner poll of the OTHER mode cannot change the outer's
# grid. Both directions are pinned here (the external adversarial file tests one; this is the permanent
# gate). The inner predicate returns immediately; we count the OUTER's sample cadence via a counter.
# Direction 1: outer FIXED, inner FAST — the outer must stay on its fixed grid.
_oc=0
_inner_fast() { poll_until 10 3 "inner-fast" -- true; return 1; }   # inner runs (fast), then outer keeps polling
_outer_fixed_cond() { _oc=$((_oc+1)); _inner_fast; [ "$_oc" -ge 2 ]; }  # true on 2nd outer sample
_oc=0; t0=$(date +%s); poll_until_fixed 30 5 "outer fixed / inner fast" -- _outer_fixed_cond; el1=$(( $(date +%s) - t0 ))
[ "$el1" -ge 4 ] && pass "outer FIXED stays on its grid despite an inner FAST poll (${el1}s ≥ 4)" || fail "outer fixed flipped to fast via nesting (${el1}s) — mode not frame-scoped"
# Direction 2: outer FAST, inner FIXED — the outer must still fast-start.
_inner_fixed() { poll_until_fixed 10 3 "inner-fixed" -- true; return 1; }
_outer_fast_cond() { _oc=$((_oc+1)); _inner_fixed; [ "$_oc" -ge 2 ]; }
_oc=0; t0=$(date +%s); poll_until 30 5 "outer fast / inner fixed" -- _outer_fast_cond; el2=$(( $(date +%s) - t0 ))
[ "$el2" -le 2 ] && pass "outer FAST keeps fast-start despite an inner FIXED poll (${el2}s ≤ 2)" || fail "outer fast lost fast-start via nesting (${el2}s) — mode not frame-scoped"

echo "────────────────────────────────────────────────────────────────────────────────"
if [ "$FAILS" = 0 ]; then echo "poll-mode-test: ALL PASS"; exit 0; else echo "poll-mode-test: $FAILS FAILED"; exit 1; fi
