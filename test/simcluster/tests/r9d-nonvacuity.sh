#!/bin/sh
# r9d-nonvacuity.sh — the NON-VACUITY PROOF for every oracle R9-D added or changed.
#
# WHY IT IS A LANDED GATE AND NOT A ONE-OFF SCRIPT. This suite's recurring failure is not "an assertion was
# wrong", it is "an assertion COULD NOT FAIL": H1 (a jq path matching nothing ⇒ its twin permanently true),
# H12 (`grep -qE ''` matching everything), and — found BY this harness while it was being written — a
# `.assumed // empty` whose jq `//` swallows `false`, making `assumed=false` permanently unassertable. Running
# the drills cannot catch that family: a permanently-true oracle passes every run, forever. Only driving each
# oracle with deliberately BAD input can, and that has to be repeatable or it rots.
#
# Every oracle added/changed in this batch is EXTRACTED VERBATIM from the drill file (awk pulls the real
# function body out of the real source — no re-typed copy that could drift from what runs) and then driven
# with DELIBERATELY BAD inputs. An oracle that cannot go RED is a permanently-true oracle, which is the
# failure family this suite keeps getting bitten by (H1: a jq path matching nothing; H12: grep -qE '').
#
# Usage: sh tests/r9d-nonvacuity.sh   (exit 0 = every oracle proved two-sided). Wired into tests/run-all.sh.
set -u
SIMDIR="${SIMDIR:-$(cd "$(dirname "$0")/.." && pwd)}"
TMP="${TMPDIR:-/tmp}/nv.$$"; mkdir -p "$TMP"; trap 'rm -rf "$TMP"' EXIT INT TERM
PASS=0; FAIL=0

# extract <file> <fn...> : pull each named shell function VERBATIM out of <file> (from `fn() {` to the
# column-0 `}`), so the proof runs the SAME text the drill runs.
extract() {
    _ef=$1; shift
    for _fn in "$@"; do
        awk -v fn="$_fn" '
            $0 ~ "^"fn"\\(\\)[ \t]*\\{" && !inf { inf=1; print; if ($0 ~ /\}[ \t]*$/) inf=0; next }
            inf { print; if ($0 ~ /^\}$/) inf=0 }
        ' "$_ef"
    done
}
# expect <want-rc: T|F> <label> <cmd...>
expect() {
    _w=$1; _l=$2; shift 2
    if "$@" >/dev/null 2>&1; then _got=T; else _got=F; fi
    if [ "$_got" = "$_w" ]; then PASS=$((PASS+1)); printf '  ok   [%s] %s\n' "$_w" "$_l"
    else FAIL=$((FAIL+1)); printf '  FAIL [want %s got %s] %s\n' "$_w" "$_got" "$_l"; fi
}
section() { printf '\n== %s ==\n' "$1"; }
# nv_arm_ok <results-dir> <arm-key> : one arm's OWN receipt from a per-arm driver fan-out passed. rc 0
# alone is not enough: a driver that ran zero tests, or only skipped ones, also exits 0 — so the arm's log
# must say it ran at least one test and ended on a bare `OK` (unittest prints `OK (skipped=…)` otherwise).
# A missing receipt is red: an arm that never ran is never a pass.
nv_arm_ok() {
    [ "$(cat "$1/$2.rc" 2>/dev/null)" = 0 ] || { tail -25 "$1/$2.out" 2>/dev/null | sed 's/^/        /' >&2; return 1; }
    grep -qx 'OK' "$1/$2.out" && grep -Eq '^Ran [1-9][0-9]* tests? in ' "$1/$2.out"
}
section "r9d harness — nv_arm_ok accepts only a receipt that ran tests and passed them all"
mkdir -p "$TMP/arm-receipts"
nv_receipt() { printf '%s\n' "$2" > "$TMP/arm-receipts/$1.rc"; printf '%b\n' "$3" > "$TMP/arm-receipts/$1.out"; }
nv_receipt good 0 'test_x ... ok\n\n----\nRan 2 tests in 0.1s\n\nOK'
nv_receipt skipped 0 'test_x ... skipped\n\n----\nRan 2 tests in 0.1s\n\nOK (skipped=1)'
nv_receipt zero 0 '\n----\nRan 0 tests in 0.0s\n\nOK'
nv_receipt failed 1 'test_x ... FAIL\n\n----\nRan 2 tests in 0.1s\n\nFAILED (failures=1)'
nv_receipt lying 0 'test_x ... FAIL\n\n----\nRan 2 tests in 0.1s\n\nFAILED (failures=1)'
expect T "CONTROL rc0, two tests run, bare OK"                          nv_arm_ok "$TMP/arm-receipts" good
expect F "CONTROL a skipped test is never a pass (OK (skipped=1))"     nv_arm_ok "$TMP/arm-receipts" skipped
expect F "CONTROL zero tests run is never a pass"                      nv_arm_ok "$TMP/arm-receipts" zero
expect F "CONTROL a failing arm (rc1) is red"                          nv_arm_ok "$TMP/arm-receipts" failed
expect F "CONTROL rc0 over a FAILED log is red (the log is read, not only the rc)" nv_arm_ok "$TMP/arm-receipts" lying
expect F "CONTROL a missing receipt is red"                            nv_arm_ok "$TMP/arm-receipts" absent

# origin: hot-update S8 plan §11.2 P22 (FIX-ORPHAN); r9d D-11. The S6–S10 arm controls run REAL drill
# entry points, and those refuse to start without the shared process supervisor. run-all.sh exports
# it; a standalone run builds it once from this tree, exactly as run-all does (lib/supervisor.sh). A
# failed build is never a skipped section — every section that needs the adapter then fails closed.
if [ -z "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -r "$SIMDIR/lib/supervisor.sh" ]; then
    . "$SIMDIR/lib/supervisor.sh"
    SIM_UPGRADE_SUPERVISOR=$(sim_supervisor_build "$(cd "$SIMDIR/../.." && pwd)" "$TMP" 2>/dev/null) || SIM_UPGRADE_SUPERVISOR=
fi
export SIM_UPGRADE_SUPERVISOR="${SIM_UPGRADE_SUPERVISOR:-}"

# ─────────────────────────────────────────────────────────────────────────────────────────
section "drill 71 — _drain_dataplane_pending (the R8 rc oracle: rc==75 AND the authored string)"
eval "$(extract "$SIMDIR/drills/71-expose-rehome-failover.sh" _drain_dataplane_pending)"
GOODMSG='error: cluster drain: cluster drain brk3: the control plane committed the rehome but 1 expose(s) have NOT confirmed the new home yet: wstrand(port 14001, sid lab, nid agt1, want epoch 2, home brk1)'
expect T "TRUE  on the real R8 pair (rc=75 + authored string)"                 _drain_dataplane_pending 75 "$GOODMSG"
expect F "MUTATION rc=0 with the string  → must NOT accept (a printed message is not an exit code)" _drain_dataplane_pending 0  "$GOODMSG"
expect F "MUTATION rc=70 with the string → must NOT accept (wrong exit class)"  _drain_dataplane_pending 70 "$GOODMSG"
expect F "MUTATION rc=75, DIFFERENT transient (catch_up_stalled) → must NOT be laundered into P1" \
        _drain_dataplane_pending 75 'error: cluster drain: catch_up_stalled: the joiner is behind'
expect F "MUTATION rc=75, empty output → must NOT pass on nothing"              _drain_dataplane_pending 75 ''
expect F "MUTATION rc=75, only HALF the sentence (no 'have NOT confirmed')"      _drain_dataplane_pending 75 'the control plane committed the rehome but everything is fine'

# ─────────────────────────────────────────────────────────────────────────────────────────
section "drill 71 — _agt_silent_since (the P1 premise + its anti-vacuity guard)"
eval "$(extract "$SIMDIR/drills/71-expose-rehome-failover.sh" _agt_silent_since)"
# stub the journal reader: NV_PUSH / NV_REG are what the two greps would have counted.
_agt_journal_count() { case "$3" in *"home directives pushed"*) printf '%s' "$NV_PUSH" ;; *) printf '%s' "$NV_REG" ;; esac; }
log() { :; }
NV_PUSH=2; NV_REG=0; expect T "TRUE  when directives were pushed AND the agent never re-registered" _agt_silent_since agt1 now
NV_PUSH=2; NV_REG=1; expect F "MUTATION one reconnect in the window → the P1 premise is NOT earned"  _agt_silent_since agt1 now
NV_PUSH=0; NV_REG=0; expect F "MUTATION journal shows NO active delivery → anti-vacuity: 'zero reconnects' over an empty window must NOT pass" _agt_silent_since agt1 now
NV_PUSH='';  NV_REG=0;  expect F "MUTATION journal UNREADABLE (push count empty) → fail closed"      _agt_silent_since agt1 now
NV_PUSH=2;   NV_REG='';  expect F "MUTATION journal UNREADABLE (reg count empty)  → fail closed"      _agt_silent_since agt1 now

# ─────────────────────────────────────────────────────────────────────────────────────────
section "drill 30 — version/correlation oracles over node ls --brokers --json"
eval "$(extract "$SIMDIR/drills/30-rolling-upgrade.sh" _nlb_field _ver_of _whole_host_at _field_is _broker_ver_is _not_whole_host_at _host_pair_is _all_on_next)"
NEXTVER=v-next
_nlb() { cat "$TMP/nlb.json"; }
mk_nlb() {  # mk_nlb <brk1 pair> <brk2 pair> <brk3 pair> ; pair = ver,agentnid,assumed,agentver,skew
    printf '{"schema":"node_ls_brokers","schema_version":2,"brokers":[' > "$TMP/nlb.json"
    _sep=''
    for _row in "$@"; do
        _n=${_row%%:*}; _r=${_row#*:}
        _bv=$(printf '%s' "$_r" | cut -d, -f1); _an=$(printf '%s' "$_r" | cut -d, -f2)
        _as=$(printf '%s' "$_r" | cut -d, -f3); _av=$(printf '%s' "$_r" | cut -d, -f4)
        _sk=$(printf '%s' "$_r" | cut -d, -f5)
        printf '%s{"node_id":"%s","agent_nid":"%s","assumed":%s,"broker_ver":"%s","agent_ver":"%s","skew":%s,"whole_host_at":false}' \
            "$_sep" "$_n" "$_an" "$_as" "$_bv" "$_av" "$_sk" >> "$TMP/nlb.json"
        _sep=','
    done
    printf ']}' >> "$TMP/nlb.json"
}
# the intended post-roll world: brk2 declared+whole-host, brk3 observed+whole-host, brk1 agentless
mk_nlb 'brk1:v-next,brk1,true,?,false' 'brk2:v-next,colo-brk2,false,v-next,false' 'brk3:v-next,brk3,true,v-next,false'
expect T "TRUE  _whole_host_at brk2 v-next (both legs at target)"                 _whole_host_at brk2 v-next
expect T "TRUE  _host_pair_is brk1 'v-next|?' (state (c): at target, still agentless)" _host_pair_is brk1 'v-next|?'
expect T "TRUE  _field_is brk2 assumed false (the DECLARATION was consumed)"      _field_is brk2 assumed false
expect T "TRUE  _field_is brk3 assumed true (the node_id==nid CONVENTION)"        _field_is brk3 assumed true
expect T "TRUE  _all_on_next with every broker_ver at target"                     _all_on_next
expect F "MUTATION _not_whole_host_at must be FALSE when the host IS whole-host"  _not_whole_host_at brk2 v-next
# the P3 regression this batch exists to catch: the agent leg silently left behind
mk_nlb 'brk1:v-next,brk1,true,?,false' 'brk2:v-next,colo-brk2,false,v-cur,true' 'brk3:v-next,brk3,true,v-next,false'
expect F "MUTATION half-upgraded host (broker v-next, agent v-cur) must NOT read whole-host" _whole_host_at brk2 v-next
expect T "TRUE  _not_whole_host_at fires on exactly that half-upgraded host"      _not_whole_host_at brk2 v-next
expect T "TRUE  _field_is brk2 skew true on the half-upgraded host"               _field_is brk2 skew true
expect F "MUTATION _field_is brk2 skew false must NOT hold while it is skewed"    _field_is brk2 skew false
expect T "TRUE  _all_on_next still true (BROKER legs are all at target) — proves whole-host is a STRICTLY stronger claim than the old broker-only oracle" _all_on_next
# the H1 family: an unreadable / schema-drifted payload must never satisfy anything
printf '{"schema":"node_ls_brokers","schema_version":2,"brokers":[]}' > "$TMP/nlb.json"
expect F "MUTATION empty brokers[] → _whole_host_at must fail closed (H1: the permanently-empty oracle)" _whole_host_at brk2 v-next
expect F "MUTATION empty brokers[] → _host_pair_is must fail closed"              _host_pair_is brk1 'v-next|?'
expect F "MUTATION empty brokers[] → _field_is must fail closed"                  _field_is brk2 assumed false
expect F "MUTATION empty brokers[] → _broker_ver_is must fail closed"             _broker_ver_is brk2 v-next
printf 'not json at all' > "$TMP/nlb.json"
expect F "MUTATION unparseable payload → _whole_host_at fails closed"             _whole_host_at brk2 v-next
expect F "MUTATION unparseable payload → _all_on_next fails closed"               _all_on_next
# the OLD H1 mutation, reproduced: the pre-R4 schema (.nodes[].nid/.release) yields no .brokers rows
printf '{"nodes":[{"nid":"brk2","release":"v-next"}]}' > "$TMP/nlb.json"
expect F "MUTATION pre-R4 schema (.nodes[].nid/.release) → every oracle fails closed instead of empty-greening" _whole_host_at brk2 v-next

# ─────────────────────────────────────────────────────────────────────────────────────────
section "drill 30 — the PLAN oracles (P3 presence states as the operator is shown them)"
eval "$(extract "$SIMDIR/drills/30-rolling-upgrade.sh" _plan_line _plan_is_broker_only _plan_is_whole_host _plan_hosts_all_three _first_hop)"
_plan_out() { cat "$TMP/plan.txt"; }
cat > "$TMP/plan.txt" <<'EOF'
rolling upgrade plan → v-next (3 host(s) to upgrade):
  UPGRADE brk2
  UPGRADE brk3
  UPGRADE brk1 (leader — transfer to brk2 first) [broker-only — no co-located agent on this host]
EOF
expect T "TRUE  _plan_is_broker_only brk1 (state (c) rendered in the plan)"        _plan_is_broker_only brk1
expect T "TRUE  _plan_is_whole_host brk2"                                          _plan_is_whole_host brk2
expect T "TRUE  _plan_hosts_all_three (a COUNT, not a keyword grep)"               _plan_hosts_all_three
expect F "MUTATION _plan_is_broker_only brk2 must be FALSE for a whole-host step"  _plan_is_broker_only brk2
expect F "MUTATION _plan_is_whole_host brk1 must be FALSE for a broker-only step"  _plan_is_whole_host brk1
[ "$(_first_hop)" = brk2 ] && { PASS=$((PASS+1)); echo "  ok   [T] _first_hop reads brk2 off the plan"; } || { FAIL=$((FAIL+1)); echo "  FAIL _first_hop"; }
# MUTATION: a host absent from the plan must not read as whole-host (the "no line at all" vacuity)
expect F "MUTATION a host with NO plan line at all is neither whole-host…"          _plan_is_whole_host brk9
expect F "MUTATION …nor broker-only (both fail closed on a missing line)"           _plan_is_broker_only brk9
# MUTATION: P3 regression — the DECLARED-agent host wrongly planned broker-only
cat > "$TMP/plan.txt" <<'EOF'
rolling upgrade plan → v-next (3 host(s) to upgrade):
  UPGRADE brk2 [broker-only — no co-located agent on this host]
  UPGRADE brk3
  UPGRADE brk1 (leader — transfer to brk2 first) [broker-only — no co-located agent on this host]
EOF
expect F "MUTATION P3 regression (declared host planned BROKER-ONLY) → PLAN-(a) goes RED" _plan_is_whole_host brk2
# MUTATION: a two-host plan must fail the count
cat > "$TMP/plan.txt" <<'EOF'
rolling upgrade plan → v-next (2 host(s) to upgrade):
  UPGRADE brk2
  UPGRADE brk1 (leader — transfer to brk2 first)
EOF
expect F "MUTATION a plan missing a host → _plan_hosts_all_three goes RED"          _plan_hosts_all_three
printf '' > "$TMP/plan.txt"
expect F "MUTATION empty plan output (ctl failure) → count oracle fails closed"     _plan_hosts_all_three

# ─────────────────────────────────────────────────────────────────────────────────────────
section "drill 30 — the HALT / lock-fence log oracles"
eval "$(extract "$SIMDIR/drills/30-rolling-upgrade.sh" _halted_on_agent_leg _roll_not_blocked _roll_halted_on_growlock)"
FIRSTHOP=brk2
ROLL=$TMP/roll.log
# The extracted production predicates still name their remote /tmp/roll.log.
# Map only that pathname at the hermetic grep boundary: parallel engineers
# must not overwrite or remove another selftest's evidence on this host.
# origin: hot-update S6 plan §13; concurrent hermetic integration receipts.
grep() {
    for _nv_grep_arg do
        case "$_nv_grep_arg" in
            /tmp/roll.log) set -- "$@" "$ROLL" ;;
            *) set -- "$@" "$_nv_grep_arg" ;;
        esac
        shift
    done
    command grep "$@"
}
cp /dev/null "$ROLL" 2>/dev/null || true
printf 'HALTED at brk2: agent re-exec refused: agent_no_responders (no agent answered for nid colo-brk2)\n' > "$ROLL"
expect T "TRUE  _halted_on_agent_leg on the real P3 (b) failure chain"              _halted_on_agent_leg
expect T "TRUE  _roll_not_blocked (an agent-leg HALT is NOT a lock fence)"          _roll_not_blocked
printf 'HALTED at brk2: reload refused: sha256_mismatch\n' > "$ROLL"
expect F "MUTATION halted at the right host but on the RELOAD leg → must NOT count as the agent-leg HALT" _halted_on_agent_leg
printf 'HALTED at brk3: agent re-exec refused: agent_no_responders\n' > "$ROLL"
expect F "MUTATION agent-leg HALT at the WRONG host → must NOT count (the per-hop expectation is host-specific)" _halted_on_agent_leg
printf 'rolling upgrade complete.\n' > "$ROLL"
expect F "MUTATION a COMPLETED roll must not match the HALT signature"              _halted_on_agent_leg
printf 'cluster upgrade HALTED at : acquire upgrade lock refused: bad_request a `cluster add` grow of brk3 is in progress\n' > "$ROLL"
expect T "TRUE  _roll_halted_on_growlock catches the G4 §B 'grow … is in progress' wording (the shape the PRE-R9-D pattern MISSED)" _roll_halted_on_growlock
expect F "MUTATION _roll_not_blocked must be FALSE while the roll is lock-fenced"   _roll_not_blocked
printf 'cluster upgrade HALTED at brk2: a cluster membership operation (join/retire) is in flight\n' > "$ROLL"
expect T "TRUE  _roll_halted_on_growlock still catches the round2-B2 wording"       _roll_halted_on_growlock
rm -f "$ROLL"
expect F "MUTATION missing roll.log → _halted_on_agent_leg fails closed (no verdict from no evidence)" _halted_on_agent_leg

# ─────────────────────────────────────────────────────────────────────────────────────────
section "drill 30 — cluster unlock lock-state oracles"
eval "$(extract "$SIMDIR/drills/30-rolling-upgrade.sh" _upgrade_lock_held _upgrade_lock_clear _grow_lock_held _grow_lock_clear)"
_UNLOCK() { cat "$TMP/unlock.txt"; return "$NV_ULRC"; }
NV_ULRC=1; cat > "$TMP/unlock.txt" <<'EOF'
  upgrade lock: HELD — lease LIVE until 2026-07-19T18:04:11Z (12m3s from now) — an orchestrator is still renewing it
error: cluster unlock: the upgrade lock's lease is still being renewed
EOF
expect T "TRUE  _upgrade_lock_held reads HELD off a run that EXITS NON-ZERO (a live lease makes the verb fail after printing the state — an rc-gated oracle would have missed it)" _upgrade_lock_held
expect F "MUTATION _upgrade_lock_clear must be FALSE while it is held"             _upgrade_lock_clear
NV_ULRC=0; printf '  upgrade lock: not held\nno membership locks are held — nothing to clear.\n' > "$TMP/unlock.txt"
expect T "TRUE  _upgrade_lock_clear after a real clear"                            _upgrade_lock_clear
expect F "MUTATION _upgrade_lock_held must be FALSE once it is gone"               _upgrade_lock_held
NV_ULRC=0; printf '  grow lock: HELD — NO LEASE (acquired by a broker released before leases existed)\n' > "$TMP/unlock.txt"
expect T "TRUE  _grow_lock_held on the #31 marker"                                 _grow_lock_held
expect F "MUTATION _grow_lock_clear must be FALSE while #31 is live"               _grow_lock_clear
NV_ULRC=0; printf '' > "$TMP/unlock.txt"
expect F "MUTATION empty output (ctl failure) → _grow_lock_clear fails closed rather than declaring victory" _grow_lock_clear
expect F "MUTATION empty output → _upgrade_lock_held fails closed too"             _upgrade_lock_held

# ─────────────────────────────────────────────────────────────────────────────────────────
section "drill 30 — the roll's OWN mechanism log (what cluster upgrade DID, not what the end state is)"
eval "$(extract "$SIMDIR/drills/30-rolling-upgrade.sh" _roll_reexeced_agent_of _roll_skipped)"
NEXTVER=v-next
cat > "$ROLL" <<'EOR'
rolling upgrade plan -> v-next (2 host(s) to upgrade):
  SKIP    brk2 (already at target)
  UPGRADE brk3
-> re-exec brk3's co-located agent into v-next
  ✓ brk3 (broker+agent) at v-next
rolling upgrade complete.
EOR
expect T "TRUE  _roll_reexeced_agent_of brk3 (the roll really ran the agent leg AND declared the whole host done)" _roll_reexeced_agent_of brk3
expect T "TRUE  _roll_skipped brk2 (idempotent resume)"                            _roll_skipped brk2
expect F "MUTATION _roll_reexeced_agent_of brk2 — brk2 was SKIPPED, so crediting the roll with its agent leg must FAIL" _roll_reexeced_agent_of brk2
# MUTATION: the roll ANNOUNCES the re-exec but never confirms the host — half-evidence must not pass
cat > "$ROLL" <<'EOR'
-> re-exec brk3's co-located agent into v-next
error: cluster upgrade HALTED at brk3: agent re-exec refused
EOR
expect F "MUTATION announced the agent re-exec but the host was never declared done → must NOT count" _roll_reexeced_agent_of brk3
# MUTATION: broker-only completion must not satisfy the whole-host mechanism oracle
printf '  ✓ brk1 (broker-only host) at v-next\n' > "$ROLL"
expect F "MUTATION a broker-ONLY host completing must NOT read as an agent-leg re-exec" _roll_reexeced_agent_of brk1
rm -f "$ROLL"
expect F "MUTATION missing roll.log → mechanism oracle fails closed"                _roll_reexeced_agent_of brk3
expect F "MUTATION missing roll.log → _roll_skipped fails closed"                   _roll_skipped brk2

# ─────────────────────────────────────────────────────────────────────────────────────────
section "drills/lib/dataplane.sh — the sentinel token must be SHELL-SAFE whatever a drill is titled"
# The token is interpolated into a single-quoted `sh -c "… printf '%s' '<tok>' …"` payload, and $_AS_DRILL is
# free human text. This proves the sanitiser is what stands between a drill's PROSE and a dead data plane.
_payload_parses() { printf "mkdir -p /srv/s && printf '%%s' '%s' > /srv/s/i" "$1" | sh -n 2>/dev/null; }
_sanitise() { printf '%s' "$1" | tr -c 'A-Za-z0-9_-' '-' | cut -c1-40; }
BADTITLE="71-expose-rehome-failover (N=3 - x; drain-migrate = P1/R8's deploy-tier verifier)"
expect F "MUTATION the RAW drill title in the token breaks the remote shell (this is the live 2026-07-19 failure: 'sh: 1: Syntax error: \")\" unexpected', 3 runs aborted SETUP-RED before any product assertion)" \
        _payload_parses "SENTINEL-$BADTITLE-1-agt1-8080"
expect T "TRUE  the SANITISED token parses — no title can disable a drill's data plane any more" \
        _payload_parses "SENTINEL-$(_sanitise "$BADTITLE")-1-agt1-8080"
expect T "TRUE  a plain title still parses (the sanitiser is not the only thing making it work)" \
        _payload_parses "SENTINEL-$(_sanitise "30-rolling-upgrade")-1-agt1-8080"

# ═════════════════════════════════════════════════════════════════════════════════════════
# R10 — the string-parsing oracles the R10 flips added. Every one reads a captured variable; a `//`-style
# swallow or a one-clause grep would make them permanently TRUE (the failure family this suite exists for).
# ═════════════════════════════════════════════════════════════════════════════════════════
section "drill 50 — #53-silence BACKUP-end scope warning (_d_scope_warned)"
eval "$(extract "$SIMDIR/drills/50-backup-restore.sh" _d_scope_warned)"
GOOD_BK='online backup complete: /x (n bytes)
⚠ BUNDLE SCOPE: this bundle contains the FSM state DB ONLY. JetStream is NOT in it — history lives in nats.
  To have a recoverable history, back JetStream up SEPARATELY: nats stream backup "$s" "<dir>/$s"'
_D_CAP="$GOOD_BK"; expect T "TRUE  on the real three-clause warning"                                  _d_scope_warned
_D_CAP="$(printf '%s' "$GOOD_BK" | grep -v 'nats stream backup')"; expect F "MUTATION drop the runnable remedy → a header with no alternative is still the silence #53 was about" _d_scope_warned
_D_CAP="$(printf '%s' "$GOOD_BK" | grep -v 'JetStream is NOT in it')"; expect F "MUTATION drop the scope clause → must not pass"    _d_scope_warned
_D_CAP='online backup complete: /x'; expect F "MUTATION completion line only, no warning → the pre-R10 silence must FAIL" _d_scope_warned

section "drill 50 — #53-silence RESTORE-end history warning (_j2_history_warned)"
eval "$(extract "$SIMDIR/drills/50-backup-restore.sh" _j2_history_warned)"
GOOD_J2='restore complete.
⚠ HISTORY/AUDIT NOT RESTORED: empty JetStream. The re-derive cursor does NOT backfill.
  If you took a JetStream backup, restore it: nats stream restore "$d" "$d"'
_J2_CAP="$GOOD_J2"; expect T "TRUE  on the real three-clause restore warning"                          _j2_history_warned
_J2_CAP="$(printf '%s' "$GOOD_J2" | grep -v 'does NOT backfill')"; expect F "MUTATION drop the 'does NOT backfill' clause → must not pass" _j2_history_warned
_J2_CAP="$(printf '%s' "$GOOD_J2" | grep -v 'nats stream restore')"; expect F "MUTATION drop the runnable inverse → must not pass"        _j2_history_warned
_J2_CAP='restore complete.'; expect F "MUTATION completion only, no history warning → the pre-R10 silence must FAIL" _j2_history_warned

section "drill 50 — #64 advice: leads with de-cluster + names the consequence (_k64_advice_leads_decluster)"
eval "$(extract "$SIMDIR/drills/50-backup-restore.sh" _k64_advice_leads_decluster)"
GOOD_ADV='NEXT (run in order):
  1. nats.conf is CLUSTERED, but this node is now a LONE VOTER — de-cluster FIRST:
     so tether-broker will REFUSE to start (crash-loop).'
_J2_CAP="$GOOD_ADV"; expect T "TRUE  on the clustered-conf advice that names REFUSE-to-start + leads with de-cluster" _k64_advice_leads_decluster
_J2_CAP="$(printf '%s' "$GOOD_ADV" | grep -v 'REFUSE to start')"; expect F "MUTATION drop the consequence clause → a de-cluster mention with no 'why' is theatre" _k64_advice_leads_decluster
_J2_CAP='NEXT (run in order):
  1. start tether-broker'; expect F "MUTATION the pre-#64 one-liner (no clustered warning) → must FAIL" _k64_advice_leads_decluster

section "drill 50 — #64 remedy is copy-paste-runnable, not a placeholder (_k64_remedy_runnable)"
eval "$(extract "$SIMDIR/drills/50-backup-restore.sh" _k64_remedy_runnable)"
GOOD_REM='     tether cluster reconcile nats --manual --conf /etc/tether/nats.d/nats.conf --secrets-dir /s --server-name brk1 --route-url nats://brk1:6222 --account-issuer ADEGRS5UTMA4JXG4BOJ2XQPILXDAB3YZZJFNO6EFMC5NRY2QGKH7D5RD --broker-nkey UC6HTJWANS5DKY5NFB4YOY3M4ZTQR2QZCF7WP3GEJ4I4LD7TQAOPLFCM'
_J2_CAP="$GOOD_REM"; expect T "TRUE  on real substituted nkeys (A… issuer + U… broker)"                _k64_remedy_runnable
_J2_CAP='     tether cluster reconcile nats --manual --conf /c --secrets-dir /s --server-name brk1 --route-url nats://brk1:6222 --account-issuer <account-public-nkey> --broker-nkey <broker-public-nkey>'
expect F "MUTATION the <…> placeholders (secrets unreadable) → an un-pasteable line must FAIL"          _k64_remedy_runnable

section "drill 51 — restore SEAM APPLIED line (_f_seam_applied) + ordered next steps (_f_next_and_seamok)"
eval "$(extract "$SIMDIR/drills/51-full-dr.sh" _f_seam_applied _f_next_and_seamok)"
GOOD_F='restore complete.
broker.cluster seam applied to /etc/tether/broker.yaml (data_dir=/var/lib/tether raft_addr=brk1:7400 ...).
NEXT (run in order):
  3. ✓ broker.cluster seam is in /etc/tether/broker.yaml — start the daemon'
_F_CAP="$GOOD_F"; expect T "TRUE  _f_seam_applied on the real 'seam applied to' line"                  _f_seam_applied
_F_CAP='restore complete.'; expect F "MUTATION no seam line (restore had no --config, pre-P2) → must FAIL" _f_seam_applied
_F_CAP="$GOOD_F"; expect T "TRUE  _f_next_and_seamok on the ordered list + ✓ seam"                     _f_next_and_seamok
_F_CAP="$(printf '%s' "$GOOD_F" | grep -v 'seam is in')"; expect F "MUTATION drop the ✓ seam confirmation → must FAIL" _f_next_and_seamok
_F_CAP='NEXT: start tether-broker, then cluster join approve'; expect F "MUTATION the pre-R10 one-liner → must FAIL" _f_next_and_seamok

section "drill 51 — #53-silence BACKUP end (_bk_scope_warned) + RESTORE end (_f_history_warned)"
eval "$(extract "$SIMDIR/drills/51-full-dr.sh" _bk_scope_warned _f_history_warned)"
_BK_CAP="$GOOD_BK"; expect T "TRUE  _bk_scope_warned on the real three-clause backup warning"          _bk_scope_warned
_BK_CAP="$(printf '%s' "$GOOD_BK" | grep -v 'nats stream backup')"; expect F "MUTATION drop the remedy → must FAIL" _bk_scope_warned
_F_CAP="$GOOD_J2"; expect T "TRUE  _f_history_warned on the real three-clause restore warning"          _f_history_warned
_F_CAP="$(printf '%s' "$GOOD_J2" | grep -v 'HISTORY/AUDIT NOT RESTORED')"; expect F "MUTATION drop the header clause → must FAIL" _f_history_warned

# ─────────────────────────────────────────────────────────────────────────────────────────
# R12 — the four PRODUCT-RED→GREEN flips (#25/#26/#27/#46). Each flipped oracle is proved two-sided so the
# positive regression cannot become a permanently-true oracle the way the old inverted asserts could.
mkdir -p "$TMP/bin"

section "drill 81 — _child_reaped (#26 CLOSED: evict reaps the managed OS child)"
eval "$(extract "$SIMDIR/drills/81-admin-evict-session-rm.sh" _child_reaped)"
_daemon_gone() { [ "$NV_DAEMON" = gone ]; }
_child_alive() { [ "$NV_CHILD" = alive ]; }
NV_DAEMON=gone; NV_CHILD=dead;  expect T "TRUE  daemon EXITED and child GONE (evict reaped — the flip's GREEN)"          _child_reaped
NV_DAEMON=gone; NV_CHILD=alive; expect F "MUTATION daemon gone but child STILL ALIVE (the pre-fix #26 leak) → must RED" _child_reaped
NV_DAEMON=up;   NV_CHILD=dead;  expect F "MUTATION daemon still up → evict not effective yet → must RED"                 _child_reaped

section "drill 82 — _manifest_refused / _manifest_bound / _unbound_port_refused (#27 CLOSED: default bind)"
SIM=_sim_code_stub
_sim_code_stub() { printf '%s' "$NV_CODE"; }   # emulate `$SIM exec brk1 -- curl … -w %{http_code}`
eval "$(extract "$SIMDIR/drills/82-agent-onboarding-invite.sh" _manifest_refused _manifest_bound _unbound_port_refused)"
NV_CODE=000; expect T "TRUE  _manifest_refused on curl 000 (no listener)"                                _manifest_refused
NV_CODE=200; expect F "MUTATION _manifest_refused on curl 200 (listener answers) → not refused"          _manifest_refused
NV_CODE=503; expect F "MUTATION _manifest_refused on curl 503 (bound but 503 — answered, not refused)"   _manifest_refused
NV_CODE=200; expect T "TRUE  _manifest_bound when the listener answers 200 (the #27-CLOSED positive)"     _manifest_bound
NV_CODE=000; expect F "MUTATION _manifest_bound on 000 (no default bind) → the flip would RED"            _manifest_bound
NV_CODE=000; expect T "TRUE  _unbound_port_refused on 000 (control discriminates a refused port)"         _unbound_port_refused
NV_CODE=200; expect F "MUTATION _unbound_port_refused on 200 (a truly-bound control port) → notices"      _unbound_port_refused

section "drill 91 — _ep_has (#46 CLOSED: the 3rd voter enters seeds endpoints)"
eval "$(extract "$SIMDIR/drills/91-client-converge.sh" _ep_has)"
S() { printf '%s\n' "$NV_SEEDS"; }   # emulate `S show` (seeds show on the leader admin socket)
NV_SEEDS='seed_generation: 42
endpoints: tls://brk1:443,tls://brk2:443,tls://brk3:443'
expect T "TRUE  _ep_has brk3 when brk3 IS in the endpoints line (the flip's GREEN)"                _ep_has brk3
NV_SEEDS='seed_generation: 42
endpoints: tls://brk1:443,tls://brk2:443'
expect F "MUTATION _ep_has brk3 when brk3 ABSENT (the old #46 defect) → must RED"                   _ep_has brk3
NV_SEEDS='seed_generation: 42
endpoints: '
expect F "MUTATION _ep_has brk3 on an empty endpoints line → fail closed"                          _ep_has brk3

section "drill 80 — _rl_logged (#25 CLOSED: broker rate-limit signature; H12 empty-needle guard)"
# h1 F3: the seam moved. This section used to stub `journalctl` onto PATH and
# emulate `$SIM exec brk1 -- sh -c "<payload>"`, because _rl_logged inlined its
# own journal read. It now delegates to drills/lib/logs.sh, so the honest seam
# is that LIBRARY FUNCTION — stub it and drive the same two-sided table.
#
# This is worth a note beyond the mechanical fix, because of HOW the break
# presented: after the migration the old stub no longer intercepted anything,
# _rl_logged became permanently false, and the two MUTATION arms went on
# "passing" — for entirely the wrong reason. Only the positive arm caught it.
# A nonvacuity suite whose negative arms can pass on a dead oracle is exactly
# the thing this file exists to prevent, so the lesson is the general one: when
# a predicate is re-pointed at a new seam, the stub must follow it to that seam,
# never sit on the transport it used to reach through.
sim_broker_slog_grep() { printf '%s\n' "$NV_LOG" | grep -qE "$2"; }
eval "$(extract "$SIMDIR/drills/80-session-isolation.sh" _rl_logged)"
NV_LOG='ts level=WARN msg="authcallout: ctl PIN attempt rate-limited" sid=lab ip=10.0.0.9'
expect T "TRUE  _rl_logged when the broker log carries the rate-limit warning (per-IP §E.6 limiter fired)" _rl_logged
NV_LOG='ts level=INFO msg="broker: ready"'
expect F "MUTATION _rl_logged when the log has NO rate-limit line → must RED (not a permanently-true needle)" _rl_logged
NV_LOG=''
expect F "MUTATION _rl_logged on an empty log → fail closed"                                                _rl_logged

# ─────────────────────────────────────────────────────────────────────────────────────────
# R13 — the drill-side batch: 95-D predicate tightening, 94 ps LOST (D6), 97 goroutine gate, 93 admin
# runtime freshness. Each new/changed oracle is driven two-sided so it cannot become permanently-true.

section "drill 95 — _d_raft_ok (R6/R13: leader EXISTS + STABLE, NOT pinned to brk1)"
eval "$(extract "$SIMDIR/drills/95-broker-selfheal.sh" _d_leader_via _d_raft_ok _d_neg_no_leader _d_neg_churn _d_neg1_ok _d_neg2_ok)"
sleep() { :; }                              # the real _d_raft_ok sleeps 2s between reads; irrelevant to the logic
_bt() { printf '{"leader_id":"brk2"}'; }    # a STABLE, NON-brk1 leader on both reads
expect T "TRUE  _d_raft_ok when a STABLE non-brk1 leader (brk2) is named on both reads (the OLD brk1-pin would FALSE this — the whole R6 fix)" _d_raft_ok
_bt() { printf '{"leader_id":null}'; }
expect F "MUTATION _d_raft_ok when NO leader is visible (leader_id null) → must RED (existence half)" _d_raft_ok
_bt() { printf 'not json at all'; }
expect F "MUTATION _d_raft_ok on garbage broker output (jq yields empty) → fail closed" _d_raft_ok
expect T "TRUE  _d_neg1_ok — the in-drill no-leader negative arm correctly sees _d_raft_ok RED" _d_neg1_ok
expect T "TRUE  _d_neg2_ok — the in-drill churning-leader negative arm correctly sees _d_raft_ok RED (stability half)" _d_neg2_ok
_d_stable_shadow() { ( _d_leader_via() { printf brk2; }; _d_raft_ok ); }
expect T "CONTROL a STABLE shadow (same id both reads) PASSES _d_raft_ok — so _d_neg2's RED is the churn, not a rigged-false predicate" _d_stable_shadow

section "drill 94 — ps LOST derivation (D6: a storage-RUNNING row on an OFFLINE node reads as LOST)"
eval "$(extract "$SIMDIR/drills/94-agent-reconcile.sh" _a1_lost1 _a1_ctrl_running)"
LOST1=x; LCTRL=y
_ps_status() { case "$1" in x) printf '%s' "$NV_LOST" ;; y) printf '%s' "$NV_CTRL" ;; esac; }
NV_CTRL=RUNNING
NV_LOST=LOST;    expect T "TRUE  _a1_lost1 when ps DERIVES LOST for the offline node's proc (the D6 positive)"          _a1_lost1
NV_LOST=RUNNING; expect F "MUTATION _a1_lost1 when ps still says RUNNING (no LOST derivation) → must RED"              _a1_lost1
NV_LOST=EXITED;  expect F "MUTATION _a1_lost1 when ps says EXITED (already reconciled, not LOST) → must RED"           _a1_lost1
NV_LOST='';      expect F "MUTATION _a1_lost1 on empty status → fail closed"                                          _a1_lost1
NV_CTRL=RUNNING; expect T "TRUE  _a1_ctrl_running when the ONLINE node's control proc stays RUNNING (the discriminator)" _a1_ctrl_running
NV_CTRL=LOST;    expect F "MUTATION _a1_ctrl_running if the control proc were ALSO LOST (a table-wide mislabel) → must RED" _a1_ctrl_running

section "drill 97 — _gor_within_tol (R13 goroutine gate JUDGE: post floor within tolerance of pre floor)"
eval "$(extract "$SIMDIR/drills/97-soak-cycles.sh" _gor_within_tol)"
GOR_TOL=24
expect T "TRUE  post floor within tolerance (120 -> 130, <= 120+24)"                                 _gor_within_tol 120 130
expect T "TRUE  post floor exactly at the tolerance edge (120 -> 144)"                                _gor_within_tol 120 144
expect F "MUTATION post floor ONE past tolerance (120 -> 145) → must RED"                             _gor_within_tol 120 145
expect F "MUTATION post floor 3x the pre floor (120 -> 360, a real leak) → must RED"                  _gor_within_tol 120 360
expect F "MUTATION empty post floor → fail closed (a missing sample is never within tolerance)"        _gor_within_tol 120 ''
expect F "MUTATION empty pre floor → fail closed"                                                     _gor_within_tol '' 130

section "drill 93 — _rt_advanced (R13 admin runtime reconciler last_tick freshness)"
eval "$(extract "$SIMDIR/drills/93-metrics-observability.sh" _rt_advanced)"
RT0='2026-07-19T12:00:00Z'
_rt_max() { printf '%s' "$NV_TICK"; }
NV_TICK='2026-07-19T12:00:05Z'; expect T "TRUE  _rt_advanced when the max last_tick moved LATER (a live pass ticked)"          _rt_advanced
NV_TICK='2026-07-19T12:00:00Z'; expect F "MUTATION _rt_advanced when last_tick is UNCHANGED (a stalled reconciler) → must RED"  _rt_advanced
NV_TICK='2026-07-19T11:59:55Z'; expect F "MUTATION _rt_advanced when last_tick went BACKWARDS → must RED"                       _rt_advanced
NV_TICK='';                     expect F "MUTATION _rt_advanced on empty (no reconcilers / admin miss) → fail closed"           _rt_advanced

section "drill 93 — _wh_leader_stable (webhook precond: STABLE leadership before the delta arm can RED)"
eval "$(extract "$SIMDIR/drills/93-metrics-observability.sh" _wh_leader_stable)"
sleep() { :; }
sim_leader() { if [ -f "$TMP/whl" ]; then rm -f "$TMP/whl"; printf '%s' "$WH2"; else : >"$TMP/whl"; printf '%s' "$WH1"; fi; }
WH1=brk1; WH2=brk1; rm -f "$TMP/whl"; expect T "TRUE  stable leader (brk1 == brk1 across the two spaced reads)"    _wh_leader_stable
WH1=brk1; WH2=brk2; rm -f "$TMP/whl"; expect F "MUTATION leadership CHURNS (brk1 → brk2) → must RED (re-seed risk)" _wh_leader_stable
WH1='';   WH2='';   rm -f "$TMP/whl"; expect F "MUTATION no leader visible (empty) → fail closed"                  _wh_leader_stable

section "drill 33 — _b3_pending_staged / _b4_rolled_back (the whole-conjunction upgrade oracles)"
eval "$(extract "$SIMDIR/drills/33-node-upgrade-success.sh" _b3_pending_staged _b4_rolled_back)"
# stub every probe the conjunctions compose (the drill's probes do container IO; the conjunction LOGIC is
# what must be two-sided). PID0/NEXTBIN_SHA/OLD_SHA are the drill globals the functions read.
_marker_json()    { [ -n "$NV_MARKER" ] && printf '%s' "$NV_MARKER"; }
_dst_sha()        { printf '%s' "$NV_DST"; }
_mainpid()        { printf '%s' "$NV_PID"; }
_exe_sha_of_pid() { printf '%s' "$NV_EXE"; }
_prev_gone()      { [ "$NV_PREVGONE" = 1 ]; }
PID0=777; NEXTBIN_SHA=shaNEW; OLD_SHA=shaOLD
M_B3='{"state":"pending","boot_count":1}'
NV_MARKER=$M_B3; NV_DST=shaNEW; NV_PID=777; NV_EXE=shaNEW; NV_PREVGONE=0
expect T "TRUE  B3 on the full pending conjunction (pending ∧ boot_count>=1 ∧ dst==NEW ∧ pid ∧ exe==NEW)" _b3_pending_staged
NV_MARKER='{"state":"pending","boot_count":0}'
expect F "MUTATION B3 boot_count==0 (install-time marker; the staged image never booted) → must RED" _b3_pending_staged
NV_MARKER='{"state":"committed","boot_count":1}'
expect F "MUTATION B3 already committed → not the pending window"                                    _b3_pending_staged
NV_MARKER=$M_B3; NV_DST=shaOLD
expect F "MUTATION B3 dst still OLD (flip never happened) → must RED"                                _b3_pending_staged
NV_DST=shaNEW; NV_PID=888
expect F "MUTATION B3 MainPID changed (a supervisor restart, not an in-place exec) → must RED"       _b3_pending_staged
NV_PID=777; NV_EXE=shaOLD
expect F "MUTATION B3 running image is OLD (flip-without-exec) → must RED"                           _b3_pending_staged
NV_MARKER=''
expect F "MUTATION B3 marker unreadable → fail closed"                                               _b3_pending_staged

M_B4='{"state":"rolled_back","detail":"register deadline exceeded (2m0s) without a successful register"}'
NV_MARKER=$M_B4; NV_DST=shaOLD; NV_PID=777; NV_EXE=shaOLD; NV_PREVGONE=1
expect T "TRUE  B4 on the full rollback conjunction (rolled_back ∧ watchdog Detail ∧ dst==OLD ∧ prev consumed ∧ pid ∧ exe==OLD)" _b4_rolled_back
NV_MARKER='{"state":"rolled_back","detail":"syscall.Exec of the new binary failed: x"}'
expect F "MUTATION B4 exec-fail sibling Detail → must NOT be laundered into the watchdog leg"         _b4_rolled_back
NV_MARKER='{"state":"rolled_back","detail":"boot: budget/deadline exhausted (boot_count=3/3, deadline=x)"}'
expect F "MUTATION B4 boot-budget sibling Detail → must NOT be laundered into the watchdog leg"       _b4_rolled_back
NV_MARKER=$M_B4; NV_PREVGONE=0
expect F "MUTATION B4 .prev still present (restore rename never consumed it) → must RED"              _b4_rolled_back
NV_PREVGONE=1; NV_DST=shaNEW
expect F "MUTATION B4 dst still NEW (no restore happened) → must RED"                                 _b4_rolled_back
NV_DST=shaOLD; NV_EXE=shaNEW
expect F "MUTATION B4 running image still NEW (marker written but no re-exec into the restored dst) → must RED" _b4_rolled_back
NV_EXE=shaOLD; NV_MARKER=''
expect F "MUTATION B4 marker unreadable → fail closed (internal review TQ-7: B3 had this arm, B4 did not)" _b4_rolled_back

# ─────────────────────────────────────────────────────────────────────────────────────────
# origin: hot-update S8 plan §9.4 / P29; r9d D-11. 31 and 33 were split into V1 (the historical body,
# kept verbatim inside the V1 branch) and V2 (P29's shared read-only regression oracle, embedded once in
# 31 and reached by 33 through `31 --regression-driver 33`). Neither arm had a section, so neither the
# V1 predicates that moved under regression_v1_setup nor the V2 native boundary was ever driven with bad
# input here. V1 below extracts the real predicates (function bodies VERBATIM; the inline `sh -c`
# predicates and refusal signatures pulled out of their assert line, not re-typed); V2 drives P29's own
# two-sided oracle tests, each drill's real V2 native mapping, and the real V2 dispatch.
# nv_inline_pred <file> <assert-label-prefix> : the single-quoted `sh -c '<script>'` of that assert_ok line.
nv_inline_pred() {
    awk -v d="$2" 'index($0, "assert_ok \"" d) == 1 {
        s = $0; i = index(s, "sh -c '\''"); if (!i) exit
        s = substr(s, i + 7); j = index(s, "'\'' _ "); if (!j) exit
        print substr(s, 1, j - 1); exit }' "$1"
}
# nv_refuses_sig <file> <assert-label-prefix> : the <sig-regex> argument (next line) of that assert_refuses.
nv_refuses_sig() {
    _nrs_raw=$(awk -v d="$2" 'index($0, "assert_refuses \"" d) == 1 { getline; sub(/^[ \t]*/, ""); sub(/[ \t]*\\$/, ""); print; exit }' "$1")
    [ -n "$_nrs_raw" ] || return 1
    eval "printf '%s' $_nrs_raw"
}
nv_sig_hits() { printf '%s\n' "$2" | grep -qiE "$1"; }   # how assert_refuses / assert_bug match a signature

section "drill 31 arm V1 — --all enumeration, config-abort and canary-hold predicates; #28 refusal signatures"
D31="$SIMDIR/drills/31-node-upgrade-fleet.sh"
eval "$(extract "$D31" _ls_online_nids _ls_has_online _ls_not_online _ls_offline_stale)"
SIM=_nv_fleet_sim
_nv_fleet_sim() { [ "$1 $2 $3 $4" = "ctl -- node ls" ] && printf '%s' "$NV_NODES"; }
NV_NODES='{"nodes":[{"nid":"agt1","status":"ONLINE"},{"nid":"agt2","status":"ONLINE"}]}'
expect T "31.V1 F1 ONLINE agt1 is in the --all target set"                         _ls_has_online agt1
expect T "31.V1 F1b ONLINE agt2 is in the --all target set (a real 2-node fleet)"   _ls_has_online agt2
expect F "MUTATION F2b an ONLINE agt2 must not read as excluded"                     _ls_not_online agt2
expect F "MUTATION F2 an ONLINE agt2 must not read as OFFLINE/STALE"                 _ls_offline_stale agt2
NV_NODES='{"nodes":[{"nid":"agt1","status":"ONLINE"},{"nid":"agt2","status":"OFFLINE"}]}'
expect T "31.V1 F2 stopped agt2 reads OFFLINE"                                      _ls_offline_stale agt2
expect T "31.V1 F2b OFFLINE agt2 is excluded from the --all target set"              _ls_not_online agt2
expect F "MUTATION F1 an OFFLINE agt2 must not count as an --all target"            _ls_has_online agt2
expect F "MUTATION F2 the ONLINE agt1 must not read as OFFLINE/STALE"               _ls_offline_stale agt1
NV_NODES='{"nodes":[{"nid":"agt1","status":"ONLINE"},{"nid":"agt2","status":"STALE"}]}'
expect T "31.V1 F2 a STALE agt2 also satisfies the drop-out predicate"               _ls_offline_stale agt2
NV_NODES='{"nodes":[{"nid":"agt1","status":"ONLINE"},{"nid":"agt2x","status":"ONLINE"}]}'
expect F "MUTATION F1 a prefix-sharing nid (agt2x) must not satisfy agt2"            _ls_has_online agt2
NV_NODES='not json'
expect F "MUTATION F1/F2c unreadable node ls → agt1 is never 'ONLINE' (fail closed; F2c guards F2b's negation)" _ls_has_online agt1
expect F "MUTATION F2 unreadable node ls → nothing reads OFFLINE/STALE"              _ls_offline_stale agt2
_nv_f3=$(nv_inline_pred "$D31" 'F3 --all dispatched'); _nv_f3b=$(nv_inline_pred "$D31" 'F3b --all did NOT dispatch')
_nv_f4b=$(nv_inline_pred "$D31" 'F4b canary transient failure'); _nv_f4c=$(nv_inline_pred "$D31" 'F4c agt2 was NEVER dispatched')
expect T "31.V1 the four inline predicates are pulled from their real assert lines"  test -n "$_nv_f3" -a -n "$_nv_f3b" -a -n "$_nv_f4b" -a -n "$_nv_f4c"
expect T "31.V1 F3 non-zero fleet abort on the AGENT gate (url_not_allowed_local)"    sh -c "$_nv_f3" _ 1 'node upgrade --all: agt1: agent_rejected:url_not_allowed_local'
expect F "MUTATION F3 rc 0 with the gate text is not an abort"                        sh -c "$_nv_f3" _ 0 'agt1: agent_rejected:url_not_allowed_local'
expect F "MUTATION F3 a BROKER-side url_not_allowed (no _local) is not the agent gate" sh -c "$_nv_f3" _ 1 'agt1: url_not_allowed'
expect T "31.V1 F3b the abort names agt1 only"                                         sh -c "$_nv_f3b" _ 'agt1: agent_rejected:url_not_allowed_local'
expect F "MUTATION F3b any line naming the OFFLINE agt2 is a dispatch to it"           sh -c "$_nv_f3b" _ 'agt2: node_offline'
expect T "31.V1 F4b canary failure holds the rest (non-zero + exact untouched count)"  sh -c "$_nv_f4b" _ 1 'canary agt1 failed, 1 node(s) untouched'
expect F "MUTATION F4b rc 0 cannot be a canary abort"                                  sh -c "$_nv_f4b" _ 0 'canary agt1 failed, 1 node(s) untouched'
expect F "MUTATION F4b a different untouched count is not this 2-node fleet"           sh -c "$_nv_f4b" _ 1 'canary agt1 failed, 2 node(s) untouched'
expect T "31.V1 F4c no agt2 line of any kind"                                          sh -c "$_nv_f4c" _ 'canary: agt1 (1 held)'
expect F "MUTATION F4c a SUCCESS line for agt2 is still a dispatch (external review F6)" sh -c "$_nv_f4c" _ '✔ lab/agt2: staged (→ v-next)'
_nv_s28=$(nv_refuses_sig "$D31" '#28 FLIPPED'); _nv_snv=$(nv_refuses_sig "$D31" '#28 non-vacuity'); _nv_sn1=$(nv_refuses_sig "$D31" 'N1 broker url_not_allowed')
expect T "31.V1 the three refusal signatures are pulled from their real assert lines" test -n "$_nv_s28" -a -n "$_nv_snv" -a -n "$_nv_sn1"
expect T "31.V1 #28 FLIPPED signs on the agent's post-download digest check"           nv_sig_hits "$_nv_s28" 'error: agent_rejected:sha256_mismatch'
expect F "MUTATION #28 FLIPPED the old url_not_allowed_local wall must miss"           nv_sig_hits "$_nv_s28" 'error: agent_rejected:url_not_allowed_local'
expect T "31.V1 #28 non-vacuity signs on the AGENT gate"                               nv_sig_hits "$_nv_snv" 'error: agent_rejected:url_not_allowed_local'
expect F "MUTATION #28 non-vacuity a broker url_not_allowed must miss"                 nv_sig_hits "$_nv_snv" 'error: url_not_allowed'
expect T "31.V1 N1 signs on the broker's own url_not_allowed"                          nv_sig_hits "$_nv_sn1" 'error: url_not_allowed'
expect F "MUTATION N1 the agent's url_not_allowed_local must not satisfy the broker refusal" nv_sig_hits "$_nv_sn1" 'error: agent_rejected:url_not_allowed_local'

section "drill 33 arm V1 — in-place success, interlock, sibling release and rollback journal predicates"
D33="$SIMDIR/drills/33-node-upgrade-success.sh"
eval "$(extract "$D33" _brk_log _b6_outcome_logged _b6_marker_gone _a2_inplace _a3_interlock _c2_committed)"
PID0=777; TS0='Mon 2026-10-12 10:00:00 UTC'; NEXTBIN_SHA=shaNEW; OLD_SHA=shaOLD; NEXT_RELEASE=v-next; AGT1B_PID0=888
_mainpid()        { case "$1" in tether-agent) printf '%s' "$NV_PID" ;; tether-agent-b) printf '%s' "$NV_PIDB" ;; esac; }
_exec_ts()        { printf '%s' "$NV_TS"; }
_exe_sha_of_pid() { printf '%s' "$NV_EXE"; }
_dst_sha()        { printf '%s' "$NV_DST"; }
_prev_sha()       { printf '%s' "$NV_PREV"; }
_marker_json()    { [ -n "$NV_MARKER" ] && printf '%s' "$NV_MARKER"; }
_online_at()      { [ "$1@$2" = "$NV_ONLINE_AT" ] || [ "$1@$2" = "${NV_ONLINE_AT2:-}" ]; }
sim_broker_slog() { [ "$1" = brk1 ] && printf '%s\n' "$NV_BLOG"; }
dexec()           { [ "$1 $2 $3 $4" = "agt1 -- test !" ] && [ "$5" = -e ] && [ "$NV_MARKER_FILE" = gone ]; }
MARKER=/home/sim/.local/bin/.tether-upgrade.json
NV_PID=777; NV_TS=$TS0; NV_EXE=shaNEW
expect T "33.V1 A2 same MainPID, same ExecMainStartTimestamp, running image NEW"      _a2_inplace
NV_PID=778; expect F "MUTATION A2 MainPID changed (a supervisor restart, not syscall.Exec)" _a2_inplace
NV_PID=777; NV_TS='Mon 2026-10-12 10:00:05 UTC'; expect F "MUTATION A2 ExecMainStartTimestamp moved (unit restarted under a recycled pid)" _a2_inplace
NV_TS=$TS0; NV_EXE=shaOLD; expect F "MUTATION A2 running image still OLD (flip without exec)" _a2_inplace
NV_ONLINE_AT=agt1@v-next; NV_DST=shaNEW; NV_PREV=shaOLD
NV_MARKER='{"state":"committed","boot_count":1,"new_sha":"shaNEW","target_nid":"agt1"}'
NV_BLOG='time=x level=INFO msg="agent-reported upgrade outcome" nid=agt1 outcome=committed'
expect T "33.V1 A3 release, disk, .prev, committed marker and journal all agree"     _a3_interlock
NV_ONLINE_AT=agt1@v-cur; expect F "MUTATION A3 ONLINE at the OLD release → not committed" _a3_interlock
NV_ONLINE_AT=agt1@v-next; NV_PREV=''; expect F "MUTATION A3 .prev missing (no rollback image kept)" _a3_interlock
NV_PREV=shaOLD; NV_MARKER='{"state":"committed","boot_count":2,"new_sha":"shaNEW","target_nid":"agt1"}'
expect F "MUTATION A3 boot_count 2 (the new image needed a second boot)"            _a3_interlock
NV_MARKER='{"state":"committed","boot_count":1,"new_sha":"shaNEW","target_nid":"agt1b"}'
expect F "MUTATION A3 marker targets the sibling, not agt1"                          _a3_interlock
NV_MARKER='{"state":"committed","boot_count":1,"new_sha":"shaNEW","target_nid":"agt1"}'
NV_BLOG='time=x level=INFO msg="agent-reported upgrade outcome" nid=agt1 outcome=rolled_back'
expect F "MUTATION A3 journal carries only the rollback outcome"                     _a3_interlock
NV_BLOG=''; expect F "MUTATION A3 unreadable broker slog → fail closed"               _a3_interlock
NV_BLOG='time=x level=INFO msg="agent-reported upgrade outcome" nid=agt1 outcome=rolled_back'
expect T "33.V1 B6c journal carries the agent-reported rolled_back outcome"          _b6_outcome_logged
NV_BLOG='time=x level=INFO msg="agent-reported upgrade outcome" nid=agt1 outcome=committed'
expect F "MUTATION B6c a committed outcome is not the rollback"                      _b6_outcome_logged
NV_BLOG='time=x level=INFO msg="upgrade rolled_back by operator"'; expect F "MUTATION B6c 'rolled_back' outside the agent-reported line" _b6_outcome_logged
NV_MARKER_FILE=gone;    expect T "33.V1 B6d delivered terminal marker removed"       _b6_marker_gone
NV_MARKER_FILE=present; expect F "MUTATION B6d marker still present → not delivered" _b6_marker_gone
_c2_rc=0; _c2_out='upgrade COMMITTED → v-next'; NV_ONLINE_AT2=agt1b@v-next; NV_PIDB=888
NV_MARKER='{"state":"committed","target_nid":"agt1b"}'
expect T "33.V1 C2 the sibling's own upgrade commits in place"                       _c2_committed
_c2_rc=75; expect F "MUTATION C2 the sibling was still refused (rc 75)"              _c2_committed
_c2_rc=0; _c2_out='staged (→ v-next) — waiting for re-register'; expect F "MUTATION C2 staged but never COMMITTED" _c2_committed
_c2_out='upgrade COMMITTED → v-next'; NV_PIDB=889; expect F "MUTATION C2 sibling MainPID changed" _c2_committed
NV_PIDB=888; NV_MARKER='{"state":"committed","target_nid":"agt1"}'; expect F "MUTATION C2 shared marker still names agt1" _c2_committed
NV_MARKER='{"state":"committed","target_nid":"agt1b"}'; unset _c2_rc; expect F "MUTATION C2 ctl status never captured → fail closed" _c2_committed

# V2: P29's oracle tests run once under the shared supervisor; each arm reads only its own tests' lines.
# Each named test is itself two-sided (a positive per row plus that row's rejections), so a pass here is
# the oracle having gone RED on every listed bad input and GREEN on its good one.
NV_REG_TESTS31='test_each_v2_row_has_a_positive_control test_full_plan_and_child_handles_survive_driver_resume test_canary_failure_never_dispatches_held_children test_agentless_and_broker_only_keep_agent_scope test_noop_and_release_need_fresh_fleet_and_original_holder test_dispatch_and_child_completion_are_exact test_each_fleet_scope_requires_its_own_fixture'
NV_REG_TESTS33='test_each_v2_row_has_a_positive_control test_deleted_journal_cannot_sign_committed test_staged_phrase_requires_accepted_action_and_disk test_reverse_cannot_reuse_forward_or_preexec_state test_completed_wait_needs_three_independent_receipts test_manifest_never_drops_a_regression_row test_missing_adapter_is_setup_red_without_starting_fixture'
NV_REG_OUT="$TMP/upgrade-regression.out"
# shellcheck disable=SC2086 # the selectors are a word list by construction
sh "$SIMDIR/tests/upgrade-regression-test.sh" $(printf '%s\n' $NV_REG_TESTS31 $NV_REG_TESTS33 | sort -u | sed 's/^/UpgradeRegressionOracle./') > "$NV_REG_OUT" 2>&1
NV_REG_RC=$?
# A bare `OK` means nothing selected failed, errored or was skipped (a skip prints `OK (skipped=N)`); each
# name must also have actually started (a test may print oracle JSON before its own `ok`).
nv_reg_ok() {
    [ "$NV_REG_RC" = 0 ] && grep -qx 'OK' "$NV_REG_OUT" || return 1
    for _nro in "$@"; do grep -q "^$_nro (__main__\.UpgradeRegressionOracle\.$_nro) \.\.\. " "$NV_REG_OUT" || return 1; done
}
# nv_reg_native <drill> <native> : that drill's REAL V2 case body, run through lib/assert.sh's verdict.
nv_reg_native() {
    _nrn_body=$(awk '/^    V2\)$/ { v2 = 1 } v2 && /^        case "\$_reg_rc" in$/ { on = 1 } on { print } on && /^        esac$/ { exit }' "$1")
    [ -n "$_nrn_body" ] || return 9
    (
        INSTANCE=drill-r9d-p29-native; SIM_EVIDENCE_DIR="$TMP/p29-native"; HERE=$SIMDIR
        . "$SIMDIR/lib/log.sh"; . "$SIMDIR/lib/assert.sh"
        eval "nv_regression_native() { _reg_rc=\$1
$_nrn_body
}"
        drill_begin 'r9d p29 native control'
        nv_regression_native "$2"
        drill_end
    ) > "$TMP/p29-native.out" 2>&1
}
# nv_reg_dispatch <drill-file> : the REAL drill, arm V2, with no live adapter: the arm must reach the
# shared regression oracle and fail CLOSED as SETUP-RED — never GREEN, never a translated CLI status.
nv_reg_dispatch() {
    mkdir -p "$TMP/p29-dispatch"
    env -u UPGRADE_REGRESSION_MANIFEST -u UPGRADE_REGRESSION_EVIDENCE -u S8_REGRESSION_SUPERVISED \
        HERE="$SIMDIR" ARM=V2 INSTANCE=drill-r9d-p29-dispatch SIM_EVIDENCE_DIR="$TMP/p29-dispatch" \
        /bin/sh "$1" > "$TMP/p29-dispatch.out" 2>&1
    _nrd_rc=$?
    [ "$_nrd_rc" = 2 ] && grep -q '^DRILL-VERDICT verdict=SETUP-RED rc=2 assert_fail=0 setup_red=1 product_red=0 ' "$TMP/p29-dispatch.out"
}
nv_rc_is() { _nri_want=$1; shift; "$@"; [ "$?" = "$_nri_want" ]; }

section "drill 31 arm V2 — fleet regression rows: positive per row, resume/canary/scope/dispatch rejections"
# shellcheck disable=SC2086
expect T "31.V2 P29 fleet rows: every row GREEN on its positive and RED on its own mutations" nv_reg_ok $NV_REG_TESTS31
for _nv_n in 0 1 2 3 4; do
    expect T "31.V2 real V2 case keeps native $_nv_n through the shell boundary" nv_rc_is "$_nv_n" nv_reg_native "$D31" "$_nv_n"
done
expect T "31.V2 a non-native driver status is SETUP-RED, not GREEN"                    nv_rc_is 2 nv_reg_native "$D31" 9
expect F "MUTATION 31.V2 an ASSERT-RED oracle must not leave the shell GREEN"         nv_rc_is 0 nv_reg_native "$D31" 1
expect T "31.V2 real arm with no live adapter reaches the oracle and fails closed SETUP-RED" nv_reg_dispatch "$D31"

section "drill 33 arm V2 — pair regression rows: positive per row, journal/stage/reverse/receipt rejections"
# shellcheck disable=SC2086
expect T "33.V2 P29 pair rows: every row GREEN on its positive and RED on its own mutations" nv_reg_ok $NV_REG_TESTS33
for _nv_n in 0 1 2 3 4; do
    expect T "33.V2 real V2 case keeps native $_nv_n through the shell boundary" nv_rc_is "$_nv_n" nv_reg_native "$D33" "$_nv_n"
done
expect T "33.V2 a non-native driver status is SETUP-RED, not GREEN"                    nv_rc_is 2 nv_reg_native "$D33" 9
expect F "MUTATION 33.V2 a PRODUCT-RED oracle must not leave the shell GREEN"         nv_rc_is 0 nv_reg_native "$D33" 3
expect T "33.V2 real arm with no live adapter reaches 31's shared driver and fails closed SETUP-RED" nv_reg_dispatch "$D33"
expect F "CONTROL a selector that never ran cannot pass for either arm"                nv_reg_ok test_no_such_regression_row

# ─────────────────────────────────────────────────────────────────────────────────────────
# simcluster-speed D-11 (round-2 review R3-F12): the ARM-SPLIT drills' new per-arm oracles, one section per
# arm, and a reconciliation below that every `# arms:` drill has at least as many sections here as arms.
section "drill 74 arm SRAB — _ktgt_empty / _ktgt_loaded (default-off: KTGT stays EMPTY, fail-closed on an unreadable snapshot)"
eval "$(extract "$SIMDIR/drills/74-rebalance-on-return.sh" _ktgt_empty _ktgt_loaded)"
KTGT=brk2
_count_on() { printf '%s' "$NV_COUNT"; }
NV_COUNT=0;  expect T "TRUE  _ktgt_empty when KTGT holds 0 __proxy__ homes"                              _ktgt_empty
NV_COUNT=1;  expect F "MUTATION one home landed on KTGT → default-off 'stays empty' must RED"               _ktgt_empty
NV_COUNT=-1; expect F "MUTATION snapshot INVALID (-1) → 'empty' must NOT pass on a failed read (fail-closed)" _ktgt_empty
NV_COUNT=2;  expect T "TRUE  _ktgt_loaded when KTGT holds homes"                                            _ktgt_loaded
NV_COUNT=0;  expect F "MUTATION _ktgt_loaded on an empty KTGT → RED"                                        _ktgt_loaded
NV_COUNT=-1; expect F "MUTATION _ktgt_loaded on an invalid snapshot → RED (-1 is not 'loaded')"             _ktgt_loaded

section "drill 74 arm C — _par_landed (the proxy_auto_rebalanced event count advanced past the pre-edge baseline)"
eval "$(extract "$SIMDIR/drills/74-rebalance-on-return.sh" _par_landed)"
_par_count() { printf '%s' "$NV_PAR"; }
_PAR_BEFORE=0; NV_PAR=1;  expect T "TRUE  count 0 → 1 (the auto event landed)"                              _par_landed
_PAR_BEFORE=0; NV_PAR=0;  expect F "MUTATION count unchanged → not landed"                                  _par_landed
_PAR_BEFORE=3; NV_PAR=3;  expect F "MUTATION a pre-existing event is not a NEW one (baseline absorbs it)"    _par_landed
_PAR_BEFORE=3; NV_PAR=4;  expect T "TRUE  baseline 3 → 4"                                                  _par_landed
_PAR_BEFORE=0; NV_PAR='';        expect F "MUTATION events unreadable (empty) → fail closed"                 _par_landed
_PAR_BEFORE=0; NV_PAR='null';    expect F "MUTATION jq printed null → fail closed"                          _par_landed
_PAR_BEFORE=''; NV_PAR=5;        expect F "MUTATION baseline never captured → fail closed (no 'landed' on nothing)" _par_landed

section "drill 96 arm A — _xfer_at_or_below (the OBJ_xfer count dropped to the clean baseline; unreadable = not yet)"
eval "$(extract "$SIMDIR/drills/96-mid-flight-chaos.sh" _xfer_at_or_below)"
_xfer_obj_count() { printf '%s' "$NV_XFER"; }
NV_XFER=3;          expect T "TRUE  count 3 at baseline 3"                                                 _xfer_at_or_below brk1 3
NV_XFER=2;          expect T "TRUE  count 2 below baseline 3"                                              _xfer_at_or_below brk1 3
NV_XFER=4;          expect F "MUTATION count 4 above baseline 3 → not reaped yet"                         _xfer_at_or_below brk1 3
NV_XFER=unreadable; expect F "MUTATION jsz unreadable → not yet (never 'reaped' on a failed read)"        _xfer_at_or_below brk1 3
NV_XFER='';         expect F "MUTATION empty → not yet"                                                    _xfer_at_or_below brk1 3

section "drill 96 arm D — _c3_committed_by (brk1's OWN broker.log names canary3 — the #65 committer attribution)"
eval "$(extract "$SIMDIR/drills/96-mid-flight-chaos.sh" _c3_committed_by)"
# The oracle's first grep runs INSIDE the container (dexec … sh -c 'grep -ahF "broker: session created" …'),
# so the stub applies that same filter to the fixture log: what dexec returns is only the commit lines.
dexec() { printf '%s\n' "$NV_LOG" | grep -aF 'broker: session created'; }
NV_LOG='time=… msg="broker: session created" sid=canary3 owner_fp=x';   expect T "TRUE  the commit line names canary3" _c3_committed_by brk1
NV_LOG='time=… msg="broker: session created" sid=canary2 owner_fp=x';   expect F "MUTATION a commit line for a DIFFERENT session → not canary3" _c3_committed_by brk1
NV_LOG='D4b canary3 written by the drill itself (not a broker line)';   expect F "MUTATION 'canary3' without the commit message → not a commit" _c3_committed_by brk1
NV_LOG='';                                                             expect F "MUTATION empty log → RED" _c3_committed_by brk1

section "drill 96 arm F — _f_precond_healthy (three voters ∧ agt1 ONLINE ∧ agt2 ONLINE — every conjunct load-bearing)"
eval "$(extract "$SIMDIR/drills/96-mid-flight-chaos.sh" _f_precond_healthy)"
_d0_three_voters() { [ "$NV_V" = 1 ]; }; _f_agt1_online() { [ "$NV_A1" = 1 ]; }; _agt_online() { [ "$1" = agt2 ] && [ "$NV_A2" = 1 ]; }
NV_V=1; NV_A1=1; NV_A2=1; expect T "TRUE  all three hold"                    _f_precond_healthy
NV_V=0; NV_A1=1; NV_A2=1; expect F "MUTATION voters not 3 → RED"           _f_precond_healthy
NV_V=1; NV_A1=0; NV_A2=1; expect F "MUTATION agt1 not ONLINE → RED"        _f_precond_healthy
NV_V=1; NV_A1=1; NV_A2=0; expect F "MUTATION agt2 not ONLINE → RED"        _f_precond_healthy

# ─────────────────────────────────────────────────────────────────────────────────────────
# drill 35 (hot-update S1). The continuity oracles themselves are python (probe.py carrier /
# legacy-carrier, driven two-sided by its own selftest in run-all.sh); the shell predicates below are
# the drill's gates AROUND them — the non-vacuity guard before injection, the fault-window progress
# poll, the final task-row read and the redial receipt — each proved RED on bad input here.
section "drill 35 arm A — _final_ps_exited (exactly one nonce row, EXITED(23); a jq path that matches nothing must be RED, not green)"
eval "$(extract "$SIMDIR/drills/35-hot-update-exec-carrier.sh" _final_ps_exited)"
mkdir -p "$TMP/35/final"; L_EV="$TMP/35"; L_NONCE=0123456789abcdef0123456789abcdef
SIM="$TMP/35/simstub"; printf '#!/bin/sh\nprintf "%%s" "$NV_PS"\n' > "$SIM"; chmod +x "$SIM"; export NV_PS
NV_PS='{"processes":[{"argv":["python3","--nonce","0123456789abcdef0123456789abcdef"],"status":"EXITED","exit_code":23}]}'
expect T "TRUE  one nonce row, EXITED(23)"                                                                  _final_ps_exited
NV_PS='{"processes":[{"argv":["python3","--nonce","0123456789abcdef0123456789abcdef"],"status":"RUNNING"}]}'
expect F "MUTATION the row is still RUNNING → not yet"                                                       _final_ps_exited
NV_PS='{"processes":[{"argv":["python3","--nonce","0123456789abcdef0123456789abcdef"],"status":"EXITED","exit_code":-1}]}'
expect F "MUTATION EXITED(-1) (missed exit) → RED"                                                           _final_ps_exited
NV_PS='{"processes":[{"argv":["python3","--nonce","0123456789abcdef0123456789abcdef"],"status":"EXITED","exit_code":23},{"argv":["sh","0123456789abcdef0123456789abcdef"],"status":"EXITED","exit_code":23}]}'
expect F "MUTATION TWO nonce rows (executed twice) → RED"                                                     _final_ps_exited
NV_PS='{"processes":[{"argv":["python3","--nonce","ffffffffffffffffffffffffffffffff"],"status":"EXITED","exit_code":23}]}'
expect F "MUTATION only a FOREIGN nonce row → RED"                                                            _final_ps_exited
NV_PS='{"processes":[]}';  expect F "MUTATION empty ps → RED"                                                _final_ps_exited
NV_PS='';                  expect F "MUTATION unreadable ps → RED"                                           _final_ps_exited

section "drill 35 arm B — _stream_advancing / _advanced_by (the non-vacuity guard: two live samples strictly increasing; window progress >= MIN)"
eval "$(extract "$SIMDIR/drills/35-hot-update-exec-carrier.sh" _stream_advancing _advanced_by)"
MIN=20
# The oracle samples both counters in $(...) subshells, so each stub's sequence lives in a file: one
# sample per line, consumed in order; an EMPTY line is "not readable" (snapshot / count fails).
_nv_samples() { printf '%s\n' "$@" > "$TMP/35/sc"; }
_nv_ctl()     { printf '%s\n' "$@" > "$TMP/35/cc"; }
_snapshot_count() { _s=$(sed -n '1p' "$TMP/35/sc"); sed -i '1d' "$TMP/35/sc"; [ -n "$_s" ] || return 1; printf '%s' "$_s"; }
_ctl_count()      { _c=$(sed -n '1p' "$TMP/35/cc"); sed -i '1d' "$TMP/35/cc"; [ -n "$_c" ] || return 1; printf '%s' "$_c"; }
_nv_samples 10 11; _nv_ctl 5 6;    expect T "TRUE  source 10 → 11 AND ctl 5 → 6 (both advancing)"          _stream_advancing
_nv_samples 10 10; _nv_ctl 5 6;    expect F "MUTATION source stalled → RED"                                 _stream_advancing
_nv_samples 10 9;  _nv_ctl 5 6;    expect F "MUTATION source went backwards → RED"                         _stream_advancing
_nv_samples 10 11; _nv_ctl 5 5;    expect F "MUTATION ctl transcript stalled (old ctl died) → RED (D1)"    _stream_advancing
_nv_samples 10 11; _nv_ctl '' 6;   expect F "MUTATION ctl transcript unreadable → RED"                      _stream_advancing
_nv_samples '' 11; _nv_ctl 5 6;    expect F "MUTATION child not alive at the first sample → RED"           _stream_advancing
_nv_samples 10 ''; _nv_ctl 5 6;    expect F "MUTATION child died between the samples → RED"                _stream_advancing
_nv_samples 30;      expect T "TRUE  30 >= 10 + MIN"                                                        _advanced_by 10
_nv_samples 29;      expect F "MUTATION 29 < 10 + MIN → not yet"                                            _advanced_by 10
_nv_samples '';      expect F "MUTATION child not alive → not yet"                                          _advanced_by 10

section "drill 35 arm C — _agt1_at_prev (ONLINE *at the previous release*: both conjuncts load-bearing) and _rebuild_seen (empty count is never 'seen')"
eval "$(extract "$SIMDIR/drills/35-hot-update-exec-carrier.sh" _agt1_at_prev _rebuild_seen)"
PREV_TAG=v0.6.0; L_CURSOR=0
_release_of() { printf '%s' "$NV_REL"; }; _online() { [ "$NV_ON" = 1 ]; }
NV_REL=v0.6.0; NV_ON=1;         expect T "TRUE  release v0.6.0 and ONLINE"                                   _agt1_at_prev
NV_REL=v0.0.0-simcluster; NV_ON=1; expect F "MUTATION ONLINE but at TODAY's release (binary swap did not take) → RED" _agt1_at_prev
NV_REL=v0.6.0; NV_ON=0;         expect F "MUTATION right release but not ONLINE → RED"                       _agt1_at_prev
NV_REL=''; NV_ON=1;             expect F "MUTATION release unreadable → RED"                                 _agt1_at_prev
sim_agent_slog_count() { printf '%s' "$NV_RB"; }
NV_RB=1;  expect T "TRUE  one rebuild line"                                                                  _rebuild_seen
NV_RB=3;  expect T "TRUE  three rebuild lines"                                                               _rebuild_seen
NV_RB=0;  expect F "MUTATION zero → the outage never crossed redialAfter → RED"                              _rebuild_seen
NV_RB=''; expect F "MUTATION slog unreadable (empty) → RED, never 'seen'"                                    _rebuild_seen
# deploy-V2: unreadable is rc 2 (a SETUP failure the caller splits off), not the product's rc 1.
NV_RB=''; _rebuild_seen; _rc=$?; if [ "$_rc" = 2 ]; then PASS=$((PASS+1)); printf '  ok   [2] unreadable slog is rc 2 (setup), not rc 1 (product)\n'; else FAIL=$((FAIL+1)); printf '  FAIL unreadable slog returned rc %s, want 2\n' "$_rc"; fi
NV_RB=0;  _rebuild_seen; _rc=$?; if [ "$_rc" = 1 ]; then PASS=$((PASS+1)); printf '  ok   [1] zero lines is rc 1 (product)\n'; else FAIL=$((FAIL+1)); printf '  FAIL zero lines returned rc %s, want 1\n' "$_rc"; fi

# origin: hot-update S3 plan §8.2. The Python driver extracts these predicates
# verbatim and runs their positive and mutation controls without tether/Docker.
section "drill 34 arm A — image continuity and injection non-vacuity"
expect T "34.A real predicates accept positive evidence and reject mutated evidence" python3 "$SIMDIR/tests/hot-update-process-probe-test.py" --arm A
section "drill 34 arm G — image continuity and injection non-vacuity"
expect T "34.G real predicates accept positive evidence and reject mutated evidence" python3 "$SIMDIR/tests/hot-update-process-probe-test.py" --arm G
section "drill 34 arm U — image continuity and injection non-vacuity"
expect T "34.U real predicates accept positive evidence and reject mutated evidence" python3 "$SIMDIR/tests/hot-update-process-probe-test.py" --arm U
section "drill 34 arm N — image continuity and injection non-vacuity"
expect T "34.N real predicates accept positive evidence and reject mutated evidence" python3 "$SIMDIR/tests/hot-update-process-probe-test.py" --arm N
section "drill 34 arm P — observed cut/activation, start handshake and exercised mixed work"
expect T "34.P real predicates accept positive evidence and reject mutated evidence" python3 "$SIMDIR/tests/hot-update-process-probe-test.py" --arm P
section "drill 34 arm W — image continuity and injection non-vacuity"
expect T "34.W real predicates accept positive evidence and reject mutated evidence" python3 "$SIMDIR/tests/hot-update-process-probe-test.py" --arm W
section "drill 34 arm F — mount before boot, retained probe census and refusal continuity"
expect T "34.F real predicates accept positive evidence and reject mutated evidence" python3 "$SIMDIR/tests/hot-update-process-probe-test.py" --arm F
section "drill 39 arm X — image continuity and injection non-vacuity"
expect T "39.X real predicates accept positive evidence and reject mutated evidence" python3 "$SIMDIR/tests/hot-update-process-probe-test.py" --arm X
section "drill 39 arm Y — image continuity and injection non-vacuity"
expect T "39.Y real predicates accept positive evidence and reject mutated evidence" python3 "$SIMDIR/tests/hot-update-process-probe-test.py" --arm Y

# origin: hot-update S4 plan §9.3 / P15. These controls exercise the actual
# shell predicates. The raw-byte judge has its independent P14 real fixtures;
# the small documents here test orchestration and verdict classification only.
section "drill 36 arm A — real supply, original image identity, strict mixed requests"
eval "$(extract "$SIMDIR/drills/36-hot-update-pty.sh" _pty_supply_valid _pty_hop_valid _pty_legacy_hop_valid _pty_strict_mixed _pty_current_state _pty_active_progress _pty_watchdog_valid _pty_rebuild_seen _pty_legacy_refused _pty_legacy_gate_valid _pty_baseline)"
PTY_EV="$TMP/36"; mkdir -p "$PTY_EV"
PTY_OLD_SHA=$(printf '%064d' 1); PTY_NEW_SHA=$(printf '%064d' 2)
PTY_PID=771; PTY_TICKS=990; PTY_NONCE=0123456789abcdef0123456789abcdef
PTY_CURSOR=0
PROBE="$SIMDIR/drills/lib/hot_update_pty_probe.py"
PTY_FIXTURE="$SIMDIR/tests/pty-nonvacuity-fixture.py"
HOT_UPDATE_REGISTER_DEADLINE_S=120
jq -n --arg old "$PTY_OLD_SHA" --arg new "$PTY_NEW_SHA" \
    '{old_sha:$old,new_sha:$new,artifact_sha:$new,served_sha:$new,pid:771,start_ticks:990}' > "$PTY_EV/supply.json"
jq -n --arg sha "$PTY_NEW_SHA" \
    '{pid:771,start_ticks:990,running_sha:$sha,disk_sha:$sha,marker:{state:"committed",new_sha:$sha,target_sid:"lab",target_nid:"agt1",boot_count:1,generation:2,resource_phase:"active"}}' > "$PTY_EV/hop-1.json"
printf '%s\n' '{"request_failures":0,"connect_failures":0,"refusals":0,"over_5s":0,"configured_rate":10,"rate":9.5,"writes_in_window":1,"in_window":4,"successful":{"read":1,"write":1,"short":1},"dup_side_effects":0,"rc0_without_effect":0,"effect_without_rc0":0,"unrequested_effects":0}' > "$PTY_EV/mixed.json"
# Edit/restore fixture data; a predicate accepting any bad document fails r9d.
nv_pty_mutate() {
    _npm_file=$1; _npm_filter=$2; shift 2
    cp "$_npm_file" "$_npm_file.good"
    jq "$_npm_filter" "$_npm_file.good" > "$_npm_file"
    expect F "MUTATION $_npm_filter must reject" "$@"
    mv "$_npm_file.good" "$_npm_file"
}
expect T "36.A distinct running and served binary supply" _pty_supply_valid
for _m in '.new_sha=.old_sha' '.served_sha="wrong"' '.pid=0' '.start_ticks=0' 'del(.artifact_sha)'; do
    nv_pty_mutate "$PTY_EV/supply.json" "$_m" _pty_supply_valid
done
expect T "36.A actual exec and image activation, same PID and ticks" _pty_hop_valid "$PTY_NEW_SHA" committed "$PTY_EV/hop-1.json"
for _m in '.running_sha="old"' '.disk_sha="old"' '.pid=772' '.start_ticks=991' '.marker.state="pending"' '.marker.boot_count=0' '.marker.generation=0' '.marker.resource_phase="staged"' '.marker.target_nid="foreign"' 'del(.marker)'; do
    nv_pty_mutate "$PTY_EV/hop-1.json" "$_m" _pty_hop_valid "$PTY_NEW_SHA" committed "$PTY_EV/hop-1.json"
done
expect T "36.A strict reads writes short work and nonempty cut" _pty_strict_mixed "$PTY_EV/mixed.json"
for _m in '.request_failures=1' '.connect_failures=1' '.refusals=1' '.over_5s=1' '.rate=8.9' '.configured_rate=5' '.writes_in_window=0' '.in_window=0' '.successful.read=0' '.successful.write=0' '.successful.short=0' '.dup_side_effects=1' '.rc0_without_effect=1' '.effect_without_rc0=1' '.unrequested_effects=1' 'del(.over_5s)'; do
    nv_pty_mutate "$PTY_EV/mixed.json" "$_m" _pty_strict_mixed "$PTY_EV/mixed.json"
done

section "drill 36 arm W — accepted queued bytes progress in B and current state returns to A"
# origin: hot-update S4 plan §9.2 / 36.W. Generate real-shaped gen1/gen2
# cuts through the runtime parser; the checkpoint boundary still reads the log proof.
command python3 "$PTY_FIXTURE" "$PTY_EV" "$PTY_NONCE" watchdog
_pty_checkpoint() {
    command python3 -I -B - "$PROBE" "$PTY_EV" "$PTY_NONCE" <<'PYREAD'
from pathlib import Path
import runpy
import sys
sensor = runpy.run_path(sys.argv[1])
sensor["runtime_records"](Path(sys.argv[2]), sys.argv[3], terminal=False)
PYREAD
}
expect T "36.W disconnected candidate actually advances input and output" _pty_active_progress
nv_pty_mutate "$PTY_EV/active.json" '.input.applied=30' _pty_active_progress
nv_pty_mutate "$PTY_EV/active.json" '.output.end=50' _pty_active_progress
nv_pty_mutate "$PTY_EV/logs.json" '.read_ok=false' _pty_active_progress
for _m in '.version=0' '.nonce="foreign"' '.protocol="legacy-run"' '.ctl_identity.pid=0'; do
    nv_pty_mutate "$PTY_EV/active.json" "$_m" _pty_active_progress
done
expect T "36.W current applied input output and original absolute KA survive rollback" _pty_current_state
for _m in '.input.applied=30' '.input.accepted=20' '.output.end=50' '.output.base=0' '.controls[0].deadline=1000' '.controls[0].seq=8' '.controls[0].ack=0' '.version=0' '.nonce="foreign"' '.protocol="legacy-run"' '.ctl_identity.pid=0' '.phase="cut"' '.generation=1' 'del(.input)' '.output.end="120"'; do
    nv_pty_mutate "$PTY_EV/rollback.json" "$_m" _pty_current_state
done
nv_pty_mutate "$PTY_EV/active.json" '.input.applied=30' _pty_current_state
nv_pty_mutate "$PTY_EV/active.json" '.output.end=50' _pty_current_state
nv_pty_mutate "$PTY_EV/active.json" '.generation=1' _pty_current_state
nv_pty_mutate "$PTY_EV/queued.json" '.input.accepted=.input.applied' _pty_current_state
nv_pty_mutate "$PTY_EV/queued.json" '.output.end=.output.base' _pty_current_state
# A missing/invalid captured receipt must remain red; no synthetic oracle.valid
# or logs.read_ok field is appended to an already parsed current-state receipt.
for _receipt in active rollback; do
    mv "$PTY_EV/$_receipt.json" "$PTY_EV/$_receipt.good"
    expect F "MUTATION unreadable $_receipt current receipt" _pty_current_state
    printf '%s\n' 'not-json' > "$PTY_EV/$_receipt.json"
    expect F "MUTATION invalid $_receipt current receipt" _pty_current_state
    mv "$PTY_EV/$_receipt.good" "$PTY_EV/$_receipt.json"
done
jq '.marker.state="pending"' "$PTY_EV/hop-1.json" > "$PTY_EV/hop-active.json"
# origin: hot-update S4 plan §9.2 / T48; drill-run3 W. Marker ownership
# remains redispatched after rollback; activation is a separate runtime receipt.
jq --arg sha "$PTY_OLD_SHA" '.running_sha=$sha | .disk_sha=$sha | .marker.state="rolled_back" | .marker.resource_phase="redispatched" | .marker.generation=3 | .marker.detail="register deadline exceeded (2m0s) without a successful register"' "$PTY_EV/hop-1.json" > "$PTY_EV/hop-rollback.json"
printf '%s\n' '{"blackhole_rc":124,"elapsed_s":121,"offline":true,"prev_consumed":true}' > "$PTY_EV/fault.json"
expect T "36.W real 120s watchdog, offline observation, new generation and consumed prev" _pty_watchdog_valid
for _m in '.blackhole_rc=1' '.elapsed_s=119' '.elapsed_s="121"' '.offline=false' '.prev_consumed=false'; do
    nv_pty_mutate "$PTY_EV/fault.json" "$_m" _pty_watchdog_valid
done
nv_pty_mutate "$PTY_EV/hop-rollback.json" '.marker.generation=2' _pty_watchdog_valid
nv_pty_mutate "$PTY_EV/hop-rollback.json" '.marker.detail="syscall.Exec failed"' _pty_watchdog_valid
nv_pty_mutate "$PTY_EV/hop-rollback.json" '.marker.resource_phase="active"' _pty_watchdog_valid

section "drill 36 arm C — original controlling terminal and actual session rebuild"
# origin: hot-update S4 plan §9.1 / C1 / T48. The four independent raw
# files and original identity samples precede the fault; no final envelope exists.
command python3 "$PTY_FIXTURE" "$PTY_EV" "$PTY_NONCE" carrier
NV_CHECKPOINT_RC=0
_pty_checkpoint() { [ "$NV_CHECKPOINT_RC" = 0 ] && test -s "$PTY_EV/agent/before.json"; }
expect T "36.C nonempty original terminal baseline" _pty_baseline
nv_pty_mutate "$PTY_EV/ctl.identity.json" '.identity.tty_nr=0' _pty_baseline
for _raw in ctl.sent ctl.received remote.sent remote.received; do
    mv "$PTY_EV/$_raw" "$PTY_EV/$_raw.good"
    expect F "MUTATION unreadable $_raw baseline" _pty_baseline
    : > "$PTY_EV/$_raw"
    expect F "MUTATION empty $_raw baseline" _pty_baseline
    printf '%s\n' 'malformed raw frame' > "$PTY_EV/$_raw"
    expect F "MUTATION invalid $_raw baseline" _pty_baseline
    mv "$PTY_EV/$_raw.good" "$PTY_EV/$_raw"
done
for _m in '.samples=[]' '.samples[0].ctty=false' '.samples[0].hup=1' '.samples[0].identity.ppid=0' '.nonce="foreign"'; do
    nv_pty_mutate "$PTY_EV/remote.trace.json" "$_m" _pty_baseline
done
nv_pty_mutate "$PTY_EV/ctl.identity.json" '.nonce="foreign"' _pty_baseline
nv_pty_mutate "$PTY_EV/ctl.identity.json" 'del(.identity)' _pty_baseline
NV_CHECKPOINT_RC=1; expect F "MUTATION unreadable baseline snapshot" _pty_baseline; NV_CHECKPOINT_RC=0
# Only collection boundaries are doubles. Restoration is parsed by the actual
# nonce-bound runtime_records join against the original started record and window.
NV_COLLECT_RC=0; NV_LOGS_RC=0
pty_fixture_collect() { return "$NV_COLLECT_RC"; }
pty_collect_logs() { return "$NV_LOGS_RC"; }
sim_agent_slog_count() { printf '%s' "$NV_REBUILD"; }
NV_REBUILD=1; expect T "36.C1 actual rebuild in fresh cursor" _pty_rebuild_seen
NV_REBUILD=0; expect F "MUTATION no carrier rebuild" _pty_rebuild_seen
NV_REBUILD=''; expect F "MUTATION unreadable logs are never zero" _pty_rebuild_seen
NV_REBUILD=1
NV_COLLECT_RC=1; expect F "MUTATION carrier fixture collection fails" _pty_rebuild_seen; NV_COLLECT_RC=0
NV_LOGS_RC=1; expect F "MUTATION shared log snapshot fails" _pty_rebuild_seen; NV_LOGS_RC=0
for _mode in missing-restored foreign-token; do
    command python3 "$PTY_FIXTURE" "$PTY_EV" "$PTY_NONCE" "$_mode"
    expect F "MUTATION carrier $_mode despite rebuild-start" _pty_rebuild_seen
done
command python3 "$PTY_FIXTURE" "$PTY_EV" "$PTY_NONCE" carrier
nv_pty_mutate "$PTY_EV/logs.json" '.read_ok=false' _pty_rebuild_seen
nv_pty_mutate "$PTY_EV/logs.json" '.cursors |= map(if .stream=="agent_slog" then .read_ok=false else . end)' _pty_rebuild_seen
nv_pty_mutate "$PTY_EV/logs.json" '.cursors |= map(if .stream=="agent_slog" then .inode=2 else . end)' _pty_rebuild_seen
nv_pty_mutate "$PTY_EV/windows.json" '.windows=[]' _pty_rebuild_seen
nv_pty_mutate "$PTY_EV/windows.json" '.windows[0].start=3' _pty_rebuild_seen
nv_pty_mutate "$PTY_EV/windows.json" '.windows[0].start=4' _pty_rebuild_seen
nv_pty_mutate "$PTY_EV/ctl.trace.json" '.nonce="foreign"' _pty_rebuild_seen
nv_pty_mutate "$PTY_EV/ctl.identity.json" '.identity.pid=0' _pty_rebuild_seen
nv_pty_mutate "$PTY_EV/ctl.identity.json" '.identity.start_ticks=0' _pty_rebuild_seen
nv_pty_mutate "$PTY_EV/logs.json" '.cursors |= map(if .stream=="agent_slog" then .sha256="wrong" else . end)' _pty_rebuild_seen
mv "$PTY_EV/logs/agent_slog.log" "$PTY_EV/logs/agent_slog.good"
expect F "MUTATION missing shared runtime log" _pty_rebuild_seen
mv "$PTY_EV/logs/agent_slog.good" "$PTY_EV/logs/agent_slog.log"
expect T "36.C1 restored complete snapshot remains readable" _pty_rebuild_seen


# origin: hot-update S4 internal review rv-drill DRILL-5. Preserve FX-D's
# independent current raw-file collection positive beside P16-SIM's passive
# audit fixture; both exercise the real baseline/restoration parsers.
NV_C_PARENT=$PTY_EV; NV_C_NONCE=$PTY_NONCE
PTY_EV="$TMP/36-raw-carrier"; PTY_NONCE=abcdef0123456789abcdef0123456789
command python3 "$PTY_FIXTURE" "$PTY_EV" "$PTY_NONCE" raw-carrier
expect T "36.C independent probe-suite raw-file baseline" _pty_baseline
expect T "36.C1 independent probe-suite restoration receipts" _pty_rebuild_seen
PTY_EV=$NV_C_PARENT; PTY_NONCE=$NV_C_NONCE

# origin: hot-update S4 internal review rv-drill DRILL-2..4. The old blanket
# rejection of carrier gaps, held leaders, and absent P ownership is obsolete.
# Positive exceptions need causal receipts; these negatives bound each one.
nv_pty_rule() {
    _npr_mode=$1; _npr_expected=$2; _npr_reason=$3
    _npr_root="$TMP/rule-$_npr_mode"
    _npr_nonce=abcdef0123456789abcdef0123456789
    command python3 "$PTY_FIXTURE" "$_npr_root" "$_npr_nonce" "$_npr_mode" || return 1
    case "$_npr_mode" in
        legacy-positive-missing|legacy-unreadable)
            command python3 "$PROBE" trace --role assemble --evidence "$_npr_root" --nonce "$_npr_nonce" > "$_npr_root/result.out" 2> "$_npr_root/result.err" ;;
        legacy-*)
            command python3 "$PROBE" legacy-exporter --evidence "$_npr_root" --nonce "$_npr_nonce" > "$_npr_root/result.out" 2> "$_npr_root/result.err" ;;
        *)
            command python3 "$PROBE" continuity --evidence "$_npr_root" --nonce "$_npr_nonce" > "$_npr_root/result.out" 2> "$_npr_root/result.err" ;;
    esac
    _npr_rc=$?
    [ "$_npr_rc" = "$_npr_expected" ] || return 1
    grep -q "$_npr_reason" "$_npr_root/result.out" "$_npr_root/result.err"
}
expect T "36.C1 observed 25s carrier outage within original recovery cap" nv_pty_rule carrier-bounded 0 '"continuous": true'
expect T "36.C1 unrelated six-second silence fails business oracle" nv_pty_rule carrier-unexplained 1 business_stall_output
expect T "36.C1 carrier recovery cannot renew original 180s deadline" nv_pty_rule carrier-over-recovery 1 business_stall_input
expect T "36.C1 unobserved fault never licenses a carrier stall" nv_pty_rule carrier-unobserved 2 window_observed
expect T "36.C1 normal operation retains five-second stall limit" nv_pty_rule carrier-normal-stall 1 business_stall_output
expect T "36.C2 original held leader during measured slave tail passes" nv_pty_rule draining-held 0 '"continuous": true'
for _mode in foreign late before-exit settled wrong-session no-timestamp; do
    expect T "MUTATION draining $_mode zombie must fail" nv_pty_rule "draining-$_mode" 1 zombies
done
expect T "39.P preserved missing ownership is valid business failure" nv_pty_rule legacy-loss 1 management_terminal
expect T "39.P positive continuity still requires real terminal ownership" nv_pty_rule legacy-positive-missing 2 'missing real terminal audit'
expect T "39.P unreadable acquisition cannot become product loss" nv_pty_rule legacy-unreadable 2 'unverified runtime log snapshot'
expect T "39.P missing replacement cannot become product loss" nv_pty_rule legacy-no-replacement 2 'legacy bare exec not committed'
expect T "39.P missing original waitpid cannot become product loss" nv_pty_rule legacy-no-waitpid 2 'missing original ctl waitpid'
expect T "39.P claimed absence must agree with fresh management collection" nv_pty_rule legacy-false-absence 2 'legacy management absence differs'
expect T "39.P current supply cannot masquerade as the real old exporter" nv_pty_rule legacy-not-old 2 'legacy exporter was not real old supply'

section "drill 36 arm L — legacy busy refusal and pending admission reject before Start"
printf '%s\n' 'agent_rejected:upgrade_deferred legacy_session_busy' > "$PTY_EV/refusal.out"
expect T "36.L exact transient class and reason" _pty_legacy_refused 75 "$PTY_EV/refusal.out"
expect F "MUTATION rc0 cannot be a refusal" _pty_legacy_refused 0 "$PTY_EV/refusal.out"
expect F "MUTATION wrong exit class" _pty_legacy_refused 64 "$PTY_EV/refusal.out"
printf '%s\n' 'agent_rejected:upgrade_deferred remote_fs_probe_in_flight' > "$PTY_EV/wrong-refusal.out"
expect F "MUTATION other busy reason does not prove legacy protection" _pty_legacy_refused 75 "$PTY_EV/wrong-refusal.out"
printf '%s\n' '{"rc":75,"spawn_lines":0,"effect_absent":true,"pending":true,"reason":"agent_upgrading","candidate_unchanged":true}' > "$PTY_EV/gate.json"
expect T "36.L pending gate rejects old wire without Start or side effect" _pty_legacy_gate_valid "$PTY_EV/gate.json"
for _m in '.rc=0' '.spawn_lines=1' '.effect_absent=false' '.pending=false' '.reason="agent_no_responders"' '.candidate_unchanged=false' 'del(.reason)' 'del(.spawn_lines)'; do
    nv_pty_mutate "$PTY_EV/gate.json" "$_m" _pty_legacy_gate_valid "$PTY_EV/gate.json"
done

section "drill 39 arm P — unverified first-run signatures and invalid evidence never become PRODUCT-RED"
eval "$(extract "$SIMDIR/drills/39-hot-update-legacy-n1.sh" _pty_legacy_exporter _pty_legacy_assertion)"
# origin: hot-update S8 plan §9.3 / P28; r9d stale-closure repair. Since P28 the
# exporter and the classifier first call the shared historical-exec proof and the
# shared classifier in drills/lib/hot_update_pty.sh. Extracting only the two drill
# functions left upgrade_legacy_exec_required undefined (`not found`, rc127), so
# every check below that wants rc2 passed for that reason alone and every check
# that wants rc0/1/3 failed: the section proved nothing about the arm. The helpers
# are now extracted VERBATIM beside the drill functions, and the provenance their
# proof reads is supplied as the same files the drill's snapshot writes.
eval "$(extract "$SIMDIR/drills/lib/hot_update_pty.sh" upgrade_legacy_exec_required upgrade_legacy_assert upgrade_legacy_observation)"
nv_legacy_role() {  # nv_legacy_role <file> <pid> <running-sha>
    printf '{"pid":%s,"start_ticks":%s,"boot_id":"nv-boot","running_sha":"%s"}\n' "$2" "$PTY_TICKS" "$3" > "$1"
}
nv_legacy_exec_files() {
    # The old release really ran: ctl and broker stayed on the old image and
    # the agent target changed image under the same pid/ticks/boot (target=agent).
    printf '%s\n' "$PTY_OLD_SHA" > "$1/legacy-source.sha"
    printf '%s\n' "$PTY_NEW_SHA" > "$1/legacy-target.sha"
    printf '%s\n' '{"commit":"a3431a12be3cf947cc797c28fea93382fa68472d","tag":"v0.6.0"}' > "$1/legacy-source.json"
    printf '0\n' > "$1/legacy-command.rc"
    nv_legacy_role "$1/legacy-broker-before.json" "$PTY_PID" "$PTY_OLD_SHA"
    nv_legacy_role "$1/legacy-broker-after.json" "$PTY_PID" "$PTY_OLD_SHA"
    nv_legacy_role "$1/legacy-agent-before.json" "$PTY_PID" "$PTY_OLD_SHA"
    nv_legacy_role "$1/legacy-agent-after.json" "$PTY_PID" "$PTY_NEW_SHA"
    printf '%s  /usr/local/bin/tether-prev\n' "$PTY_OLD_SHA" > "$1/legacy-ctl-before.sha"
    printf '%s  /usr/local/bin/tether-prev\n' "$PTY_OLD_SHA" > "$1/legacy-ctl-after.sha"
}
nv_legacy_exec_files "$PTY_EV"
expect T "39.P actual historical exec provenance satisfies the shared proof" upgrade_legacy_exec_required "$PTY_EV" agent
expect F "MUTATION broker named as target while only the agent changed image" upgrade_legacy_exec_required "$PTY_EV" broker
# Edit/restore one provenance file; each edit is a way the old release did NOT run.
nv_legacy_exec_mutate() {  # nv_legacy_exec_mutate <file> <label> <writer...>
    _nlem_file=$1; _nlem_label=$2; shift 2
    cp "$PTY_EV/$_nlem_file" "$PTY_EV/$_nlem_file.good"
    "$@" > "$PTY_EV/$_nlem_file"
    expect F "MUTATION $_nlem_label is not a historical exec" upgrade_legacy_exec_required "$PTY_EV" agent
    mv "$PTY_EV/$_nlem_file.good" "$PTY_EV/$_nlem_file"
}
nv_legacy_exec_mutate legacy-agent-after.json 'agent never changed image' nv_legacy_role /dev/stdout "$PTY_PID" "$PTY_OLD_SHA"
nv_legacy_exec_mutate legacy-agent-after.json 'agent restarted (new pid)' nv_legacy_role /dev/stdout 772 "$PTY_NEW_SHA"
nv_legacy_exec_mutate legacy-broker-after.json 'broker also replaced' nv_legacy_role /dev/stdout "$PTY_PID" "$PTY_NEW_SHA"
nv_legacy_exec_mutate legacy-command.rc 'legacy command refused rc75' printf '75\n'
nv_legacy_exec_mutate legacy-ctl-after.sha 'ctl was the new binary' printf '%s  ctl\n' "$PTY_NEW_SHA"
nv_legacy_exec_mutate legacy-target.sha 'target equals source' printf '%s\n' "$PTY_OLD_SHA"
nv_legacy_exec_mutate legacy-source.json 'source commit unrecorded' printf '%s\n' '{"commit":"","tag":"v0.6.0"}'
expect T "39.P real old-exporter bare exec fault identity" _pty_legacy_hop_valid "$PTY_EV/hop-1.json"
for _m in '.running_sha="old"' '.pid=772' '.marker.boot_count=0' '.marker.state="pending"'; do
    nv_pty_mutate "$PTY_EV/hop-1.json" "$_m" _pty_legacy_hop_valid "$PTY_EV/hop-1.json"
done
# The legacy CLI boundary emits the implemented contract: JSON stdout,
# one categorical signature on stderr. The predicates/classifier are real.
python3() {
    if [ "${1:-}" = "$PROBE" ] && [ "${2:-}" = legacy-exporter ]; then
        printf '%s\n' '{"valid":true,"continuous":false}'
        if [ "$NV_SIGNATURE_STREAM" = stderr ]; then
            printf '%s\n' "$NV_LEGACY_OUT" >&2
        else
            printf '%s\n' "$NV_LEGACY_OUT"
        fi
        return "$NV_LEGACY_RC"
    fi
    command python3 "$@"
}
_pty_continuity() { return "$NV_CONTINUITY_RC"; }
NV_CONTINUITY_RC=1; NV_LEGACY_RC=1; NV_SIGNATURE_STREAM=stderr
# The old PTY may already be gone after bare exec. A post-fault alive/ctty gate
# would discard the product failure before its signature could be classified.
NV_LEGACY_OUT="HOT_UPDATE_LEGACY_PTY_EXPORTER output=interrupted hup=1 management_terminal=0 ctl=-9 pid=123 nonce=$PTY_NONCE reason=bytes_output,management_terminal,courier_terminal"
PTY_LEGACY_SIGNATURE=UNPINNED
_pty_legacy_exporter > "$PTY_EV/unpinned.out"; _nv_rc=$?
expect T "39.P unverified first run is rc2 and has no matchable product signature" test "$_nv_rc" = 2
# origin: hot-update S4 plan §9.3 / T48; drill-run4 diagnostics. The
# observed line is visible, but only a reviewed configured pin can earn rc3.
expect T "39.P unpinned diagnostic exposes the retained observation" grep -Fx "$NV_LEGACY_OUT" "$PTY_EV/unpinned.out"
expect F "39.P unpinned diagnostic cannot match product signature" grep -xE "$PTY_LEGACY_SIGNATURE" "$PTY_EV/unpinned.out"
PTY_LEGACY_SIGNATURE="^HOT_UPDATE_LEGACY_PTY_EXPORTER output=interrupted hup=1 management_terminal=0 ctl=-9 pid=[0-9]+ nonce=$PTY_NONCE reason=bytes_output,management_terminal,courier_terminal$"
_pty_legacy_exporter > "$PTY_EV/pinned.out"; _nv_rc=$?
expect T "39.P reviewed single shape returns the original failing rc1" test "$_nv_rc" = 1
expect T "39.P raw continuity failure receipt is saved separately" test "$(cat "$PTY_EV/continuity_oracle.rc")" = 1
NV_SIGNATURE_STREAM=stdout
_pty_legacy_exporter > "$PTY_EV/wrong-stream.out"; _nv_rc=$?
expect T "39.P stdout signature cannot substitute for stderr evidence" test "$_nv_rc" = 2
NV_SIGNATURE_STREAM=stderr
NV_CONTINUITY_RC=0; expect T "39.P unexpectedly fixed continuity returns success to assert_bug" _pty_legacy_exporter
NV_CONTINUITY_RC=2; _pty_legacy_exporter > "$PTY_EV/invalid.out"; _nv_rc=$?
expect T "39.P invalid continuity is rc2" test "$_nv_rc" = 2
NV_CONTINUITY_RC=1; NV_LEGACY_RC=2; _pty_legacy_exporter > "$PTY_EV/invalid.out"; _nv_rc=$?
expect T "39.P invalid observation is rc2 even with signature-like output" test "$_nv_rc" = 2
NV_LEGACY_RC=1; NV_LEGACY_OUT='different undocumented failure'
expect F "39.P another failure shape cannot match the reviewed pin" _pty_legacy_exporter
_pty_legacy_exporter > "$PTY_EV/unexpected.out"; _nv_rc=$?
expect T "39.P undocumented failure is evidence rc2" test "$_nv_rc" = 2
nv_pty_native_verdict() {
    _npv_expected=$1; _npv_rc_expected=$2
    (
        INSTANCE=drill-p15-predicate
        SIM_EVIDENCE_DIR="$PTY_EV/verdict"
        . "$SIMDIR/lib/log.sh"
        . "$SIMDIR/lib/assert.sh"
        drill_begin '39.P hermetic predicate classification'
        _pty_legacy_assertion
        drill_end
    ) > "$PTY_EV/native-verdict.out" 2>&1
    _npv_rc=$?
    [ "$_npv_rc" = "$_npv_rc_expected" ] || return 1
    grep -q "^DRILL-VERDICT verdict=$_npv_expected rc=$_npv_rc_expected " "$PTY_EV/native-verdict.out" || return 1
    case "$_npv_expected" in
        PRODUCT-RED) grep -q 'assert_fail=0 setup_red=0 product_red=1 not_covered=0 nc_gap=0 ' "$PTY_EV/native-verdict.out" ;;
        SETUP-RED) grep -q 'assert_fail=0 setup_red=1 product_red=0 not_covered=0 nc_gap=0 ' "$PTY_EV/native-verdict.out" ;;
        *) grep -q 'assert_fail=1 setup_red=0 product_red=0 not_covered=0 nc_gap=0 ' "$PTY_EV/native-verdict.out" ;;
    esac
}
NV_LEGACY_OUT="HOT_UPDATE_LEGACY_PTY_EXPORTER output=interrupted hup=1 management_terminal=0 ctl=-9 pid=123 nonce=$PTY_NONCE reason=bytes_output,management_terminal,courier_terminal"
expect T "39.P reviewed product shape is native PRODUCT-RED rc3" nv_pty_native_verdict PRODUCT-RED 3
# origin: hot-update S8 plan §9.3 / P28. The shared classifier keeps assert_bug
# for the exporter's own 0/1 and routes its evidence rc2 (unpinned, mismatched or
# invalid) to SETUP-RED: "old exporter 未执行属 SETUP/INCOMPLETE，不是 expected
# legacy loss". Before P28 these three classified as ASSERT-FAIL through a bare
# assert_bug; the r9d expectations had not followed the classifier. What is
# load-bearing is unchanged: none of them can ever reach PRODUCT-RED or GREEN.
NV_LEGACY_OUT="HOT_UPDATE_LEGACY_PTY_EXPORTER output=interrupted hup=2 management_terminal=0 ctl=-9 pid=123 nonce=$PTY_NONCE reason=bytes_output,management_terminal,courier_terminal"
expect T "39.P another product-shaped failure is native SETUP-RED, never PRODUCT-RED" nv_pty_native_verdict SETUP-RED 2
expect F "MUTATION the mismatched shape must not classify as the reviewed PRODUCT-RED" nv_pty_native_verdict PRODUCT-RED 3
NV_LEGACY_OUT="HOT_UPDATE_LEGACY_PTY_EXPORTER output=interrupted hup=1 management_terminal=0 ctl=-9 pid=123 nonce=$PTY_NONCE reason=bytes_output,management_terminal,courier_terminal"
NV_CONTINUITY_RC=0
expect T "39.P unexpectedly fixed is native ASSERT-FAIL rc1" nv_pty_native_verdict ASSERT-FAIL 1
NV_CONTINUITY_RC=2
expect T "39.P invalid evidence is native SETUP-RED rc2, never PRODUCT-RED" nv_pty_native_verdict SETUP-RED 2
NV_CONTINUITY_RC=1; PTY_LEGACY_SIGNATURE=UNPINNED
expect T "39.P first-run unverified shape is native SETUP-RED rc2, never PRODUCT-RED" nv_pty_native_verdict SETUP-RED 2
# origin: hot-update S8 plan §11.2 P28 mutant: no historical exec is SETUP-RED
# even when the continuity oracle and the reviewed signature would both match.
# The exec proof runs first, so the destructive observer is never reached.
PTY_LEGACY_SIGNATURE="^HOT_UPDATE_LEGACY_PTY_EXPORTER output=interrupted hup=1 management_terminal=0 ctl=-9 pid=[0-9]+ nonce=$PTY_NONCE reason=bytes_output,management_terminal,courier_terminal$"
expect T "CONTROL with exec provenance the same evidence is the reviewed PRODUCT-RED" nv_pty_native_verdict PRODUCT-RED 3
mv "$PTY_EV/legacy-agent-after.json" "$PTY_EV/legacy-agent-after.json.good"
rm -f "$PTY_EV/continuity.out" "$PTY_EV/continuity_oracle.rc"
expect T "39.P matching loss without a historical exec is native SETUP-RED rc2" nv_pty_native_verdict SETUP-RED 2
expect F "39.P the classifier never ran the destructive observer without a historical exec" test -e "$PTY_EV/continuity_oracle.rc"
# The exporter carries its own guard (the classifier is not its only caller), so prove that one too.
rm -f "$PTY_EV/continuity.out" "$PTY_EV/continuity_oracle.rc"
_pty_legacy_exporter > "$PTY_EV/no-exec.out" 2>&1; _nv_rc=$?
expect T "39.P exporter without a historical exec is rc2 before any observation" test "$_nv_rc" = 2
expect F "39.P the exporter itself never ran the observer without a historical exec" test -e "$PTY_EV/continuity_oracle.rc"
mv "$PTY_EV/legacy-agent-after.json.good" "$PTY_EV/legacy-agent-after.json"

# Drive the real public oracle when P14 is integrated. Missing P14 is an explicit
# failed integration check, never a substitute synthetic byte-judge success.
mkdir -m 700 "$TMP/pty-selftest"
expect T "36/39.P same raw-byte oracle positive negative and removed-check controls" command python3 "$SIMDIR/drills/lib/hot_update_pty_probe.py" selftest --evidence "$TMP/pty-selftest" --nonce "$PTY_NONCE"

# origin: hot-update S5 plan §8 T48 / §9.1–§9.3. Invoke the owned
# hermetic controls against the real shell guards. No missing P24 fixture is
# replaced with a synthetic continuity success here.
section "drill 75 arm A — same cohort OPEN and partial cipher priors"
expect T "75.A two-sided supply and fault controls" sh "$SIMDIR/tests/hot-update-flow-drill-test.sh" A
section "drill 75 arm C — live topology and native action status"
expect T "75.C two-sided supply and fault controls" sh "$SIMDIR/tests/hot-update-flow-drill-test.sh" C
section "drill 75 arm R — observed revoke barriers and native status"
expect T "75.R two-sided supply and fault controls" sh "$SIMDIR/tests/hot-update-flow-drill-test.sh" R
section "drill 75 arm B — current-state reverse hop and native wait"
expect T "75.B two-sided supply and fault controls" sh "$SIMDIR/tests/hot-update-flow-drill-test.sh" B
section "drill 75 arm W — ready pending real watchdog priors"
expect T "75.W two-sided supply and fault controls" sh "$SIMDIR/tests/hot-update-flow-drill-test.sh" W
section "drill 75 arm L — actual fixed old broker and legacy refusal"
expect T "75.L two-sided supply and fault controls" sh "$SIMDIR/tests/hot-update-flow-drill-test.sh" L
section "drill 39 arm F — historical flow exporter fault and native product classification"
expect T "39.F two-sided original fault and native classification controls" sh "$SIMDIR/tests/hot-update-flow-drill-test.sh" F

# origin: hot-update S6 plan §13 / T58. Register the actual owner's two-sided
# controls for each added arm, retaining every S3/S4/S5 section above.
section "drill 37 arm A — single store and original cohort dispatch"
expect T "37.A real owner dispatch setup and native controls" sh "$SIMDIR/tests/hot-update-broker-drill-test.sh" A
section "drill 37 arm A1 — sole-voter plan and write-stall dispatch"
expect T "37.A1 real owner dispatch child custody and native controls" sh "$SIMDIR/tests/hot-update-broker-drill-test.sh" A1
section "drill 37 arm B — quorum maintenance and broker-only scope"
expect T "37.B real owner scope child custody and native controls" sh "$SIMDIR/tests/hot-update-broker-drill-test.sh" B
section "drill 37 arm T — transfer defer and terminal-cut dispatch"
expect T "37.T real owner transfer setup and native controls" sh "$SIMDIR/tests/hot-update-broker-drill-test.sh" T
section "drill 37 arm R — held salvage and active rollback fault barriers"
expect T "37.R real owner fault overlays dispatch and native controls" sh "$SIMDIR/tests/hot-update-broker-drill-test.sh" R
section "drill 39 arm BR — reviewed legacy N3 unwind and native product classification"
expect T "39.BR real owner old-artifact pin and native loss controls" sh "$SIMDIR/tests/hot-update-broker-drill-test.sh" BR

# origin: hot-update S7 plan §10.3 / §10.5 / T43 / T44. Drills 63/64 run their REAL drill file, driver
# and transfer_mutations predicates in a private sandbox per arm (tests/hot-update-transfer-drill-test.py):
# each arm must reach its expected native verdict with every scheduled cell cut, AND be driven off it by
# that arm's own fault (missed cut, empty socket map, undelivered injection, unobserved watchdog, missing
# barrier artifact, surviving/unbound mutant). Writing these sections found two permanently-unreachable
# states and one laundering: transfer_fixture.py could not even start under the driver's `python3 -I`, and
# an injection that failed with exit 1 (C3 restart, X cancel) let the arm go GREEN on a fault it never made.
# The arms are fanned out once (bounded concurrency); each section reads only its own arm's receipt, and a
# missing receipt is red.
#
# origin: r9d D-11 (S6–S10 arm closure). The S8–S10 drills 38, 39 (compatibility rows and NI/CI), 85/86 and
# 99/100 gained per-arm drivers of the same shape, so all five drivers are launched here ONCE and
# CONCURRENTLY, after every in-process section above has run (their load cannot perturb a timing-sensitive
# control above), and are all joined before the first section below reads a receipt. Each driver keeps its
# own bounded per-arm concurrency. The launcher is a FOREGROUND process: background `&` jobs of a
# non-interactive sh ignore SIGINT, so an interrupt would orphan the drivers that a single foreground
# fan-out used to take down with it.
section "drills 38/39/63/64/85/86/99/100 — per-arm drivers fanned out once, concurrently"
TRANSFER_ARMS="$TMP/transfer-arms"; ROLLBACK_ARMS="$TMP/rollback-arms"; COMPAT_ARMS="$TMP/compat-arms"
INFRA_ARMS="$TMP/infra-arms"; FULLSTACK_ARMS="$TMP/fullstack-arms"
command python3 -I -B - "$SIMDIR/tests" "$TMP" >/dev/null 2>&1 <<'NVFAN'
import subprocess, sys
tests, tmp = sys.argv[1], sys.argv[2]
drivers = (('hot-update-transfer-drill-test.py', 'transfer-arms'), ('hot-update-rollback-drill-test.py', 'rollback-arms'),
           ('hot-update-compat-drill-test.py', 'compat-arms'), ('hot-update-infra-arm-test.py', 'infra-arms'),
           ('hot-update-fullstack-arm-test.py', 'fullstack-arms'))
children = [subprocess.Popen([sys.executable, '-I', '-B', tests + '/' + name, '--arm', 'all', '--results', tmp + '/' + results],
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for name, results in drivers]
for child in children:
    child.wait()
NVFAN
transfer_arm_ok() {
    [ "$(cat "$TRANSFER_ARMS/$1.rc" 2>/dev/null)" = 0 ] || { tail -25 "$TRANSFER_ARMS/$1.out" 2>/dev/null | sed 's/^/        /' >&2; return 1; }
    grep -q '^OK$' "$TRANSFER_ARMS/$1.out" && grep -Eq '^Ran [1-9][0-9]* tests? in ' "$TRANSFER_ARMS/$1.out"
}
section "drill 63 arm A — single store: every leg cut; a refused agent cut is INCOMPLETE"
expect T "63.A real drill reaches GREEN and its missed cut is coverage" transfer_arm_ok A
section "drill 63 arm A1 — sole-voter: every leg cut; a refused agent cut is INCOMPLETE"
expect T "63.A1 real drill reaches GREEN and its missed cut is coverage" transfer_arm_ok A1
section "drill 63 arm B — quorum roll over every hop; refused roll INCOMPLETE, failed grow SETUP"
expect T "63.B real drill reaches GREEN, refused roll and failed grow stay red" transfer_arm_ok B
section "drill 63 arm C1 — carrier sockets from the observed map; an empty map is SETUP"
expect T "63.C1 real drill kills mapped carrier sockets, empty map stays red" transfer_arm_ok C1
section "drill 63 arm C2 — data sockets from the observed map; an empty map is SETUP"
expect T "63.C2 real drill kills mapped data sockets, empty map stays red" transfer_arm_ok C2
section "drill 63 arm C3 — same-store NATS restart; a restart that did not happen is SETUP"
expect T "63.C3 real drill restarts NATS, a failed restart never goes GREEN" transfer_arm_ok C3
section "drill 63 arm R1 — pristine import barrier per cell; a missing cell artifact is SETUP"
expect T "63.R1 real drill dispatches every barrier cell, missing artifact stays red" transfer_arm_ok R1
section "drill 63 arm R2 — ACTIVE import barrier per cell; a missing cell artifact is SETUP"
expect T "63.R2 real drill dispatches every barrier cell, missing artifact stays red" transfer_arm_ok R2
section "drill 63 arm X — cancel and session-rm victims; undelivered cancel SETUP, refused rm INCOMPLETE"
expect T "63.X real drill injects both victims, failed injections never go GREEN" transfer_arm_ok X
section "drill 63 arm W1 — leg-1 watchdog outage; an unobserved rollback is INCOMPLETE and still healed"
expect T "63.W1 real drill observes the watchdog, a miss is coverage counted once" transfer_arm_ok W1
section "drill 63 arm W2 — leg-2 watchdog outage; an unobserved rollback is INCOMPLETE and still healed"
expect T "63.W2 real drill observes the watchdog, a miss is coverage counted once" transfer_arm_ok W2
section "drill 64 arm M01 — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M01 real drill kills only with a bound receipt" transfer_arm_ok M01
section "drill 64 arm M02 — mutant kill bound to its owner, roles, 120s window and native ASSERT-FAIL only"
expect T "64.M02 real drill kills only with a bound receipt" transfer_arm_ok M02
section "drill 64 arm M03 — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M03 real drill kills only with a bound receipt" transfer_arm_ok M03
section "drill 64 arm M04 — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M04 real drill kills only with a bound receipt" transfer_arm_ok M04
section "drill 64 arm M05 — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M05 real drill kills only with a bound receipt" transfer_arm_ok M05
section "drill 64 arm M06 — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M06 real drill kills only with a bound receipt" transfer_arm_ok M06
section "drill 64 arm M07 — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M07 real drill kills only with a bound receipt" transfer_arm_ok M07
section "drill 64 arm M08A — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M08A real drill kills only with a bound receipt" transfer_arm_ok M08A
section "drill 64 arm M08B — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M08B real drill kills only with a bound receipt" transfer_arm_ok M08B
section "drill 64 arm M08C — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M08C real drill kills only with a bound receipt" transfer_arm_ok M08C
section "drill 64 arm M09 — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M09 real drill kills only with a bound receipt" transfer_arm_ok M09
section "drill 64 arm M10A — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M10A real drill kills only with a bound receipt" transfer_arm_ok M10A
section "drill 64 arm M10B — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M10B real drill kills only with a bound receipt" transfer_arm_ok M10B
section "drill 64 arm M11A — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M11A real drill kills only with a bound receipt" transfer_arm_ok M11A
section "drill 64 arm M11B — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M11B real drill kills only with a bound receipt" transfer_arm_ok M11B
section "drill 64 arm M11C — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M11C real drill kills only with a bound receipt" transfer_arm_ok M11C
section "drill 64 arm M12A — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M12A real drill kills only with a bound receipt" transfer_arm_ok M12A
section "drill 64 arm M12B — mutant kill bound to its owner, roles, 120s window and native ASSERT-FAIL only"
expect T "64.M12B real drill kills only with a bound receipt" transfer_arm_ok M12B
section "drill 64 arm M12C — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M12C real drill kills only with a bound receipt" transfer_arm_ok M12C
section "drill 64 arm M13A — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M13A real drill kills only with a bound receipt" transfer_arm_ok M13A
section "drill 64 arm M13B — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M13B real drill kills only with a bound receipt" transfer_arm_ok M13B
section "drill 64 arm M13C — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M13C real drill kills only with a bound receipt" transfer_arm_ok M13C
section "drill 64 arm M14 — mutant kill bound to its owner, roles and native ASSERT-FAIL only"
expect T "64.M14 real drill kills only with a bound receipt" transfer_arm_ok M14
expect F "an arm with no receipt fails closed (a skipped arm is never a pass)" transfer_arm_ok NO-SUCH-ARM

# origin: hot-update S8 plan §9.2 / §11.2 P27; r9d D-11. Drill 38 runs its REAL drill file per arm (the
# arm's own `case "${ARM:?}"` label, the heredoc PY38 runner fed byte-for-byte to the interpreter, the
# upgrade38_native mapping and lib/assert.sh) against the arm's REAL upgrade_tx_arms.tsv row inside a
# private sandbox (tests/hot-update-rollback-drill-test.py). Only the deployment boundary is a double:
# the P26 fixture start/stop/drain/sample/judge/companion, supervised command execution, the fault
# injector's environment effect, the mutation build supplier and the clock the 300 s calibrations sleep
# on; docker/ssh/sudo/systemctl/journalctl/tether are poisoned. Each arm must reach its expected-verdicts
# native verdict walking exactly its declared operations, and be driven off it to one SPECIFIC native
# status by its own bad inputs (last-command CLI rc, own proof set, cleanup census, plus the family
# obligation: R checkpoint/reverse receipts, W injector/hit, A calibration interval, H reverse step,
# M executed-bytes kill, I judge binding). No cut, rollback, kill or deploy receipt is claimed. The arms
# were fanned out above (bounded concurrency); each section reads only its own receipt and a missing one is red.
section "drill 38 — rollback/compat arm controls, one isolated receipt per arm"
section "drill 38 arm A-single — two 300s calibrations and three cuts reach GREEN; an unfittable deadline is INCOMPLETE"
expect T "38.A-single two 300s calibrations and three cuts reach GREEN; an unfittable deadline is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" A-single
section "drill 38 arm A-sole — two 300s calibrations and three cuts reach GREEN; an unfittable deadline is INCOMPLETE"
expect T "38.A-sole two 300s calibrations and three cuts reach GREEN; an unfittable deadline is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" A-sole
section "drill 38 arm A-n3 — two 300s calibrations and three cuts reach GREEN; an unfittable deadline is INCOMPLETE"
expect T "38.A-n3 two 300s calibrations and three cuts reach GREEN; an unfittable deadline is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" A-n3
section "drill 38 arm R0 — pristine cancel keeps the original owner; a replaced owner is PRODUCT-RED"
expect T "38.R0 pristine cancel keeps the original owner; a replaced owner is PRODUCT-RED" nv_arm_ok "$ROLLBACK_ARMS" R0
section "drill 38 arm R1 — partial take then fresh reverse; preflight take INCOMPLETE, reused forward cut ASSERT-RED"
expect T "38.R1 partial take then fresh reverse; preflight take INCOMPLETE, reused forward cut ASSERT-RED" nv_arm_ok "$ROLLBACK_ARMS" R1
section "drill 38 arm R2 — new admission then next-generation reverse; none INCOMPLETE, generation gap ASSERT-RED"
expect T "38.R2 new admission then next-generation reverse; none INCOMPLETE, generation gap ASSERT-RED" nv_arm_ok "$ROLLBACK_ARMS" R2
section "drill 38 arm H-root — rollback, cold boot and reupgrade walked in order; a skipped reverse step is SETUP-RED"
expect T "38.H-root rollback, cold boot and reupgrade walked in order; a skipped reverse step is SETUP-RED" nv_arm_ok "$ROLLBACK_ARMS" H-root
section "drill 38 arm H-user — rollback, cold boot and reupgrade walked in order; a skipped reverse step is SETUP-RED"
expect T "38.H-user rollback, cold boot and reupgrade walked in order; a skipped reverse step is SETUP-RED" nv_arm_ok "$ROLLBACK_ARMS" H-user
section "drill 38 arm H-no-systemd — rollback, cold boot and reupgrade walked in order; a skipped reverse step is SETUP-RED"
expect T "38.H-no-systemd rollback, cold boot and reupgrade walked in order; a skipped reverse step is SETUP-RED" nv_arm_ok "$ROLLBACK_ARMS" H-no-systemd
section "drill 38 arm H-colocated — rollback, cold boot and reupgrade walked in order; a skipped reverse step is SETUP-RED"
expect T "38.H-colocated rollback, cold boot and reupgrade walked in order; a skipped reverse step is SETUP-RED" nv_arm_ok "$ROLLBACK_ARMS" H-colocated
section "drill 38 arm H-sibling — rollback, cold boot and reupgrade walked in order; a skipped reverse step is SETUP-RED"
expect T "38.H-sibling rollback, cold boot and reupgrade walked in order; a skipped reverse step is SETUP-RED" nv_arm_ok "$ROLLBACK_ARMS" H-sibling
section "drill 38 arm H-readonly — rollback, cold boot and reupgrade walked in order; a skipped reverse step is SETUP-RED"
expect T "38.H-readonly rollback, cold boot and reupgrade walked in order; a skipped reverse step is SETUP-RED" nv_arm_ok "$ROLLBACK_ARMS" H-readonly
section "drill 38 arm G-two — two live generations roll back GREEN; a refused rollback is PRODUCT-RED"
expect T "38.G-two two live generations roll back GREEN; a refused rollback is PRODUCT-RED" nv_arm_ok "$ROLLBACK_ARMS" G-two
section "drill 38 arm G-third — third generation refused rc64; accepting it is PRODUCT-RED"
expect T "38.G-third third generation refused rc64; accepting it is PRODUCT-RED" nv_arm_ok "$ROLLBACK_ARMS" G-third
section "drill 38 arm G-concurrent — competing upgrade deferred rc75; accepting it is PRODUCT-RED"
expect T "38.G-concurrent competing upgrade deferred rc75; accepting it is PRODUCT-RED" nv_arm_ok "$ROLLBACK_ARMS" G-concurrent
section "drill 38 arm C-driver — durable journal across driver-resume reaches completed; another arm judgment is ASSERT-RED"
expect T "38.C-driver durable journal across driver-resume reaches completed; another arm judgment is ASSERT-RED" nv_arm_ok "$ROLLBACK_ARMS" C-driver
section "drill 38 arm C-reopen — durable journal across query reaches completed; another arm judgment is ASSERT-RED"
expect T "38.C-reopen durable journal across query reaches completed; another arm judgment is ASSERT-RED" nv_arm_ok "$ROLLBACK_ARMS" C-reopen
section "drill 38 arm C-gc — expired-start refused rc64; acceptance is PRODUCT-RED, another arm judgment ASSERT-RED"
expect T "38.C-gc expired-start refused rc64; acceptance is PRODUCT-RED, another arm judgment ASSERT-RED" nv_arm_ok "$ROLLBACK_ARMS" C-gc
section "drill 38 arm C-late-writer — durable journal across late-callback reaches completed; another arm judgment is ASSERT-RED"
expect T "38.C-late-writer durable journal across late-callback reaches completed; another arm judgment is ASSERT-RED" nv_arm_ok "$ROLLBACK_ARMS" C-late-writer
section "drill 38 arm C-history-rollback — historical-rollback refused rc64; acceptance is PRODUCT-RED, another arm judgment ASSERT-RED"
expect T "38.C-history-rollback historical-rollback refused rc64; acceptance is PRODUCT-RED, another arm judgment ASSERT-RED" nv_arm_ok "$ROLLBACK_ARMS" C-history-rollback
section "drill 38 arm C-owner-dead — durable journal across archive reaches abandoned; another arm judgment is ASSERT-RED"
expect T "38.C-owner-dead durable journal across archive reaches abandoned; another arm judgment is ASSERT-RED" nv_arm_ok "$ROLLBACK_ARMS" C-owner-dead
section "drill 38 arm W00-F0-agent-L-known-none — P26 injector hit then settle rc64 (rejected); an unacknowledged injection is INCOMPLETE"
expect T "38.W00-F0-agent-L-known-none P26 injector hit then settle rc64 (rejected); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W00-F0-agent-L-known-none
section "drill 38 arm W01-F1-single-L-known-none — P26 injector hit then settle rc64 (cancelled); an unacknowledged injection is INCOMPLETE"
expect T "38.W01-F1-single-L-known-none P26 injector hit then settle rc64 (cancelled); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W01-F1-single-L-known-none
section "drill 38 arm W02-F2-sole-voter-L-known-none — P26 injector hit then settle rc64 (cancelled); an unacknowledged injection is INCOMPLETE"
expect T "38.W02-F2-sole-voter-L-known-none P26 injector hit then settle rc64 (cancelled); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W02-F2-sole-voter-L-known-none
section "drill 38 arm W03-F3-N3-L-known-none — P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE"
expect T "38.W03-F3-N3-L-known-none P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W03-F3-N3-L-known-none
section "drill 38 arm W04-F4-agent-L-known-none — P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE"
expect T "38.W04-F4-agent-L-known-none P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W04-F4-agent-L-known-none
section "drill 38 arm W05-F5-single-L-known-inside — P26 injector hit then settle rc0 (completed); an unacknowledged injection is INCOMPLETE"
expect T "38.W05-F5-single-L-known-inside P26 injector hit then settle rc0 (completed); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W05-F5-single-L-known-inside
section "drill 38 arm W06-F6-sole-voter-L-known-none — P26 injector hit then settle rc77 (rejected); an unacknowledged injection is INCOMPLETE"
expect T "38.W06-F6-sole-voter-L-known-none P26 injector hit then settle rc77 (rejected); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W06-F6-sole-voter-L-known-none
section "drill 38 arm W07-F7-N3-L-known-inside — P26 injector hit then settle rc0 (completed); an unacknowledged injection is INCOMPLETE"
expect T "38.W07-F7-N3-L-known-inside P26 injector hit then settle rc0 (completed); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W07-F7-N3-L-known-inside
section "drill 38 arm W08-F0-agent-D-known-none — P26 injector hit then settle rc64 (rolled_back); an unacknowledged injection is INCOMPLETE"
expect T "38.W08-F0-agent-D-known-none P26 injector hit then settle rc64 (rolled_back); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W08-F0-agent-D-known-none
section "drill 38 arm W09-F1-single-D-known-none — P26 injector hit then settle rc64 (rolled_back); an unacknowledged injection is INCOMPLETE"
expect T "38.W09-F1-single-D-known-none P26 injector hit then settle rc64 (rolled_back); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W09-F1-single-D-known-none
section "drill 38 arm W10-F2-sole-voter-D-known-none — P26 injector hit then settle rc64 (rolled_back); an unacknowledged injection is INCOMPLETE"
expect T "38.W10-F2-sole-voter-D-known-none P26 injector hit then settle rc64 (rolled_back); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W10-F2-sole-voter-D-known-none
section "drill 38 arm W11-F8-N3-D-known-none — P26 injector hit then settle rc64 (rolled_back); an unacknowledged injection is INCOMPLETE"
expect T "38.W11-F8-N3-D-known-none P26 injector hit then settle rc64 (rolled_back); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W11-F8-N3-D-known-none
section "drill 38 arm W12-F9-N3-D-known-none — P26 injector hit then settle rc75 (deferred); an unacknowledged injection is INCOMPLETE"
expect T "38.W12-F9-N3-D-known-none P26 injector hit then settle rc75 (deferred); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W12-F9-N3-D-known-none
section "drill 38 arm W05-F5-single-L-known-outside — P26 injector hit then settle rc64 (cancelled); an unacknowledged injection is INCOMPLETE"
expect T "38.W05-F5-single-L-known-outside P26 injector hit then settle rc64 (cancelled); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W05-F5-single-L-known-outside
section "drill 38 arm W07-F7-N3-L-known-outside — P26 injector hit then settle rc64 (cancelled); an unacknowledged injection is INCOMPLETE"
expect T "38.W07-F7-N3-L-known-outside P26 injector hit then settle rc64 (cancelled); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W07-F7-N3-L-known-outside
section "drill 38 arm W09-F5-agent-D-known-inside — P26 injector hit then settle rc0 (completed); an unacknowledged injection is INCOMPLETE"
expect T "38.W09-F5-agent-D-known-inside P26 injector hit then settle rc0 (completed); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W09-F5-agent-D-known-inside
section "drill 38 arm W09-F5-agent-D-known-outside — P26 injector hit then settle rc64 (rolled_back); an unacknowledged injection is INCOMPLETE"
expect T "38.W09-F5-agent-D-known-outside P26 injector hit then settle rc64 (rolled_back); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W09-F5-agent-D-known-outside
section "drill 38 arm W09-F7-N3-D-known-inside — P26 injector hit then settle rc0 (completed); an unacknowledged injection is INCOMPLETE"
expect T "38.W09-F7-N3-D-known-inside P26 injector hit then settle rc0 (completed); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W09-F7-N3-D-known-inside
section "drill 38 arm W09-F7-N3-D-known-outside — P26 injector hit then settle rc64 (rolled_back); an unacknowledged injection is INCOMPLETE"
expect T "38.W09-F7-N3-D-known-outside P26 injector hit then settle rc64 (rolled_back); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W09-F7-N3-D-known-outside
section "drill 38 arm W08-F0-single-D-unknown-none — P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE"
expect T "38.W08-F0-single-D-unknown-none P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W08-F0-single-D-unknown-none
section "drill 38 arm W09-F1-agent-D-unknown-none — P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE"
expect T "38.W09-F1-agent-D-unknown-none P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W09-F1-agent-D-unknown-none
section "drill 38 arm W12-F9-N3-D-unknown-none — P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE"
expect T "38.W12-F9-N3-D-unknown-none P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W12-F9-N3-D-unknown-none
section "drill 38 arm W08-F3-single-D-known-none — P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE"
expect T "38.W08-F3-single-D-known-none P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W08-F3-single-D-known-none
section "drill 38 arm W09-F4-sole-voter-D-known-none — P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE"
expect T "38.W09-F4-sole-voter-D-known-none P26 injector hit then settle rc74 (recovery_required); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W09-F4-sole-voter-D-known-none
section "drill 38 arm W09-F6-agent-D-known-none — P26 injector hit then settle rc77 (recovery_required); an unacknowledged injection is INCOMPLETE"
expect T "38.W09-F6-agent-D-known-none P26 injector hit then settle rc77 (recovery_required); an unacknowledged injection is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" W09-F6-agent-D-known-none
section "drill 38 arm I-late-revoke-current-rollback — late revocation wins rc77; a permitted settle is PRODUCT-RED"
expect T "38.I-late-revoke-current-rollback late revocation wins rc77; a permitted settle is PRODUCT-RED" nv_arm_ok "$ROLLBACK_ARMS" I-late-revoke-current-rollback
section "drill 38 arm I-nats-cut-leader-move — both effects settle GREEN; a laundered judge rc is ASSERT-RED"
expect T "38.I-nats-cut-leader-move both effects settle GREEN; a laundered judge rc is ASSERT-RED" nv_arm_ok "$ROLLBACK_ARMS" I-nats-cut-leader-move
section "drill 38 arm I-begin-archive-gc — both effects settle GREEN; a laundered judge rc is ASSERT-RED"
expect T "38.I-begin-archive-gc both effects settle GREEN; a laundered judge rc is ASSERT-RED" nv_arm_ok "$ROLLBACK_ARMS" I-begin-archive-gc
section "drill 38 arm I-driver-death-host-reservation — both effects settle GREEN; a laundered judge rc is ASSERT-RED"
expect T "38.I-driver-death-host-reservation both effects settle GREEN; a laundered judge rc is ASSERT-RED" nv_arm_ok "$ROLLBACK_ARMS" I-driver-death-host-reservation
section "drill 38 arm M01 — mutant dual_active killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M01 mutant dual_active killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M01
section "drill 38 arm M02 — mutant disk_without_exec killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M02 mutant disk_without_exec killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M02
section "drill 38 arm M03 — mutant old_worker_retained killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M03 mutant old_worker_retained killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M03
section "drill 38 arm M04 — mutant missing_begin killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M04 mutant missing_begin killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M04
section "drill 38 arm M05 — mutant false_control killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M05 mutant false_control killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M05
section "drill 38 arm M06 — mutant missing_domain killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M06 mutant missing_domain killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M06
section "drill 38 arm M07 — mutant forward_receipt_reused killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M07 mutant forward_receipt_reused killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M07
section "drill 38 arm M08 — mutant early_unlock killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M08 mutant early_unlock killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M08
section "drill 38 arm M09 — mutant parent_changed killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M09 mutant parent_changed killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M09
section "drill 38 arm M10 — mutant gc_restart killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M10 mutant gc_restart killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M10
section "drill 38 arm M11 — mutant late_writer killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M11 mutant late_writer killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M11
section "drill 38 arm M12 — mutant historical_rollback killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M12 mutant historical_rollback killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M12
section "drill 38 arm M13 — mutant stale_database killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M13 mutant stale_database killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M13
section "drill 38 arm M14 — mutant missing_transfer_state killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M14 mutant missing_transfer_state killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M14
section "drill 38 arm M15 — mutant stale_nonce killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M15 mutant stale_nonce killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M15
section "drill 38 arm M16 — mutant cross_grant_artifact killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M16 mutant cross_grant_artifact killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M16
section "drill 38 arm M17 — mutant stale_incarnation_cleanup killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M17 mutant stale_incarnation_cleanup killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M17
section "drill 38 arm M18 — mutant voters_only killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M18 mutant voters_only killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M18
section "drill 38 arm M19 — mutant fake_hit killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M19 mutant fake_hit killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M19
section "drill 38 arm M20 — mutant original_deleted killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M20 mutant original_deleted killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M20
section "drill 38 arm M21 — mutant orphan_descendant killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE"
expect T "38.M21 mutant orphan_descendant killed only with executed bytes and native ASSERT-FAIL; a survivor is INCOMPLETE" nv_arm_ok "$ROLLBACK_ARMS" M21
expect F "drill 38: an arm with no receipt fails closed" nv_arm_ok "$ROLLBACK_ARMS" NO-SUCH-ARM

# origin: hot-update S8 plan §9.3 / §11.2 P28; r9d D-11. The 56 S8 compatibility arms of drill 39 run
# their REAL dispatch (_upgrade_39_new_arm -> upgrade_39_compat PYCOMPAT -> broker_native -> drill_end) in a
# private sandbox per arm (tests/hot-update-compat-drill-test.py). Only the P24/P22 supplier ports, the
# product commands (finite local actors) and, for ABI-adjacent, the P26 fixture INTERFACE are doubles;
# docker/ssh/systemctl/tether are poisoned. Each arm must reach its expected-verdicts.tsv native with its own
# CASES39 CLI statuses, and every one of its CLI statuses, required judge checks, frozen wire argv/quadrant,
# companion, parser flags, --interrupting scope, live fence and fixture judge must drive it to a named red.
# The arms were fanned out above (bounded concurrency); each section reads only its own arm's receipt, and a
# missing receipt is red.
section "drill 39 — S8 compatibility arm controls, one isolated receipt per arm"
section "drill 39 arm Q00B0-exec — frozen Q00B0 exec argv, exact nonce and one execution (ctl old, agent old, broker old)"
expect T "39.Q00B0-exec frozen Q00B0 exec argv, exact nonce and one execution (ctl old, agent old, broker old)" nv_arm_ok "$COMPAT_ARMS" Q00B0-exec
section "drill 39 arm Q00B0-run — frozen Q00B0 run argv, real PTY input/output/resize and one terminal (ctl old, agent old, broker old)"
expect T "39.Q00B0-run frozen Q00B0 run argv, real PTY input/output/resize and one terminal (ctl old, agent old, broker old)" nv_arm_ok "$COMPAT_ARMS" Q00B0-run
section "drill 39 arm Q00B0-kill — frozen Q00B0 kill-probe argv, original pid SIGTERM and one terminal (ctl old, agent old, broker old)"
expect T "39.Q00B0-kill frozen Q00B0 kill-probe argv, original pid SIGTERM and one terminal (ctl old, agent old, broker old)" nv_arm_ok "$COMPAT_ARMS" Q00B0-kill
section "drill 39 arm Q00B0-expose — frozen Q00B0 expose argv plus cleanup companion, public-port nonce and cleanup (ctl old, agent old, broker old)"
expect T "39.Q00B0-expose frozen Q00B0 expose argv plus cleanup companion, public-port nonce and cleanup (ctl old, agent old, broker old)" nv_arm_ok "$COMPAT_ARMS" Q00B0-expose
section "drill 39 arm Q00B0-transfer — frozen Q00B0 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl old, agent old, broker old)"
expect T "39.Q00B0-transfer frozen Q00B0 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl old, agent old, broker old)" nv_arm_ok "$COMPAT_ARMS" Q00B0-transfer
section "drill 39 arm Q01B0-exec — frozen Q01B0 exec argv, exact nonce and one execution (ctl old, agent new, broker old)"
expect T "39.Q01B0-exec frozen Q01B0 exec argv, exact nonce and one execution (ctl old, agent new, broker old)" nv_arm_ok "$COMPAT_ARMS" Q01B0-exec
section "drill 39 arm Q01B0-run — frozen Q01B0 run argv, real PTY input/output/resize and one terminal (ctl old, agent new, broker old)"
expect T "39.Q01B0-run frozen Q01B0 run argv, real PTY input/output/resize and one terminal (ctl old, agent new, broker old)" nv_arm_ok "$COMPAT_ARMS" Q01B0-run
section "drill 39 arm Q01B0-kill — frozen Q01B0 kill-probe argv, original pid SIGTERM and one terminal (ctl old, agent new, broker old)"
expect T "39.Q01B0-kill frozen Q01B0 kill-probe argv, original pid SIGTERM and one terminal (ctl old, agent new, broker old)" nv_arm_ok "$COMPAT_ARMS" Q01B0-kill
section "drill 39 arm Q01B0-expose — frozen Q01B0 expose argv plus cleanup companion, public-port nonce and cleanup (ctl old, agent new, broker old)"
expect T "39.Q01B0-expose frozen Q01B0 expose argv plus cleanup companion, public-port nonce and cleanup (ctl old, agent new, broker old)" nv_arm_ok "$COMPAT_ARMS" Q01B0-expose
section "drill 39 arm Q01B0-transfer — frozen Q01B0 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl old, agent new, broker old)"
expect T "39.Q01B0-transfer frozen Q01B0 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl old, agent new, broker old)" nv_arm_ok "$COMPAT_ARMS" Q01B0-transfer
section "drill 39 arm Q10B0-exec — frozen Q10B0 exec argv, exact nonce and one execution (ctl new, agent old, broker old)"
expect T "39.Q10B0-exec frozen Q10B0 exec argv, exact nonce and one execution (ctl new, agent old, broker old)" nv_arm_ok "$COMPAT_ARMS" Q10B0-exec
section "drill 39 arm Q10B0-run — frozen Q10B0 run argv, real PTY input/output/resize and one terminal (ctl new, agent old, broker old)"
expect T "39.Q10B0-run frozen Q10B0 run argv, real PTY input/output/resize and one terminal (ctl new, agent old, broker old)" nv_arm_ok "$COMPAT_ARMS" Q10B0-run
section "drill 39 arm Q10B0-kill — frozen Q10B0 kill-probe argv, original pid SIGTERM and one terminal (ctl new, agent old, broker old)"
expect T "39.Q10B0-kill frozen Q10B0 kill-probe argv, original pid SIGTERM and one terminal (ctl new, agent old, broker old)" nv_arm_ok "$COMPAT_ARMS" Q10B0-kill
section "drill 39 arm Q10B0-expose — frozen Q10B0 expose argv plus cleanup companion, public-port nonce and cleanup (ctl new, agent old, broker old)"
expect T "39.Q10B0-expose frozen Q10B0 expose argv plus cleanup companion, public-port nonce and cleanup (ctl new, agent old, broker old)" nv_arm_ok "$COMPAT_ARMS" Q10B0-expose
section "drill 39 arm Q10B0-transfer — frozen Q10B0 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl new, agent old, broker old)"
expect T "39.Q10B0-transfer frozen Q10B0 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl new, agent old, broker old)" nv_arm_ok "$COMPAT_ARMS" Q10B0-transfer
section "drill 39 arm Q11B0-exec — frozen Q11B0 exec argv, exact nonce and one execution (ctl new, agent new, broker old)"
expect T "39.Q11B0-exec frozen Q11B0 exec argv, exact nonce and one execution (ctl new, agent new, broker old)" nv_arm_ok "$COMPAT_ARMS" Q11B0-exec
section "drill 39 arm Q11B0-run — frozen Q11B0 run argv, real PTY input/output/resize and one terminal (ctl new, agent new, broker old)"
expect T "39.Q11B0-run frozen Q11B0 run argv, real PTY input/output/resize and one terminal (ctl new, agent new, broker old)" nv_arm_ok "$COMPAT_ARMS" Q11B0-run
section "drill 39 arm Q11B0-kill — frozen Q11B0 kill-probe argv, original pid SIGTERM and one terminal (ctl new, agent new, broker old)"
expect T "39.Q11B0-kill frozen Q11B0 kill-probe argv, original pid SIGTERM and one terminal (ctl new, agent new, broker old)" nv_arm_ok "$COMPAT_ARMS" Q11B0-kill
section "drill 39 arm Q11B0-expose — frozen Q11B0 expose argv plus cleanup companion, public-port nonce and cleanup (ctl new, agent new, broker old)"
expect T "39.Q11B0-expose frozen Q11B0 expose argv plus cleanup companion, public-port nonce and cleanup (ctl new, agent new, broker old)" nv_arm_ok "$COMPAT_ARMS" Q11B0-expose
section "drill 39 arm Q11B0-transfer — frozen Q11B0 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl new, agent new, broker old)"
expect T "39.Q11B0-transfer frozen Q11B0 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl new, agent new, broker old)" nv_arm_ok "$COMPAT_ARMS" Q11B0-transfer
section "drill 39 arm Q00B1-exec — frozen Q00B1 exec argv, exact nonce and one execution (ctl old, agent old, broker new)"
expect T "39.Q00B1-exec frozen Q00B1 exec argv, exact nonce and one execution (ctl old, agent old, broker new)" nv_arm_ok "$COMPAT_ARMS" Q00B1-exec
section "drill 39 arm Q00B1-run — frozen Q00B1 run argv, real PTY input/output/resize and one terminal (ctl old, agent old, broker new)"
expect T "39.Q00B1-run frozen Q00B1 run argv, real PTY input/output/resize and one terminal (ctl old, agent old, broker new)" nv_arm_ok "$COMPAT_ARMS" Q00B1-run
section "drill 39 arm Q00B1-kill — frozen Q00B1 kill-probe argv, original pid SIGTERM and one terminal (ctl old, agent old, broker new)"
expect T "39.Q00B1-kill frozen Q00B1 kill-probe argv, original pid SIGTERM and one terminal (ctl old, agent old, broker new)" nv_arm_ok "$COMPAT_ARMS" Q00B1-kill
section "drill 39 arm Q00B1-expose — frozen Q00B1 expose argv plus cleanup companion, public-port nonce and cleanup (ctl old, agent old, broker new)"
expect T "39.Q00B1-expose frozen Q00B1 expose argv plus cleanup companion, public-port nonce and cleanup (ctl old, agent old, broker new)" nv_arm_ok "$COMPAT_ARMS" Q00B1-expose
section "drill 39 arm Q00B1-transfer — frozen Q00B1 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl old, agent old, broker new)"
expect T "39.Q00B1-transfer frozen Q00B1 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl old, agent old, broker new)" nv_arm_ok "$COMPAT_ARMS" Q00B1-transfer
section "drill 39 arm Q01B1-exec — frozen Q01B1 exec argv, exact nonce and one execution (ctl old, agent new, broker new)"
expect T "39.Q01B1-exec frozen Q01B1 exec argv, exact nonce and one execution (ctl old, agent new, broker new)" nv_arm_ok "$COMPAT_ARMS" Q01B1-exec
section "drill 39 arm Q01B1-run — frozen Q01B1 run argv, real PTY input/output/resize and one terminal (ctl old, agent new, broker new)"
expect T "39.Q01B1-run frozen Q01B1 run argv, real PTY input/output/resize and one terminal (ctl old, agent new, broker new)" nv_arm_ok "$COMPAT_ARMS" Q01B1-run
section "drill 39 arm Q01B1-kill — frozen Q01B1 kill-probe argv, original pid SIGTERM and one terminal (ctl old, agent new, broker new)"
expect T "39.Q01B1-kill frozen Q01B1 kill-probe argv, original pid SIGTERM and one terminal (ctl old, agent new, broker new)" nv_arm_ok "$COMPAT_ARMS" Q01B1-kill
section "drill 39 arm Q01B1-expose — frozen Q01B1 expose argv plus cleanup companion, public-port nonce and cleanup (ctl old, agent new, broker new)"
expect T "39.Q01B1-expose frozen Q01B1 expose argv plus cleanup companion, public-port nonce and cleanup (ctl old, agent new, broker new)" nv_arm_ok "$COMPAT_ARMS" Q01B1-expose
section "drill 39 arm Q01B1-transfer — frozen Q01B1 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl old, agent new, broker new)"
expect T "39.Q01B1-transfer frozen Q01B1 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl old, agent new, broker new)" nv_arm_ok "$COMPAT_ARMS" Q01B1-transfer
section "drill 39 arm Q10B1-exec — frozen Q10B1 exec argv, exact nonce and one execution (ctl new, agent old, broker new)"
expect T "39.Q10B1-exec frozen Q10B1 exec argv, exact nonce and one execution (ctl new, agent old, broker new)" nv_arm_ok "$COMPAT_ARMS" Q10B1-exec
section "drill 39 arm Q10B1-run — frozen Q10B1 run argv, real PTY input/output/resize and one terminal (ctl new, agent old, broker new)"
expect T "39.Q10B1-run frozen Q10B1 run argv, real PTY input/output/resize and one terminal (ctl new, agent old, broker new)" nv_arm_ok "$COMPAT_ARMS" Q10B1-run
section "drill 39 arm Q10B1-kill — frozen Q10B1 kill-probe argv, original pid SIGTERM and one terminal (ctl new, agent old, broker new)"
expect T "39.Q10B1-kill frozen Q10B1 kill-probe argv, original pid SIGTERM and one terminal (ctl new, agent old, broker new)" nv_arm_ok "$COMPAT_ARMS" Q10B1-kill
section "drill 39 arm Q10B1-expose — frozen Q10B1 expose argv plus cleanup companion, public-port nonce and cleanup (ctl new, agent old, broker new)"
expect T "39.Q10B1-expose frozen Q10B1 expose argv plus cleanup companion, public-port nonce and cleanup (ctl new, agent old, broker new)" nv_arm_ok "$COMPAT_ARMS" Q10B1-expose
section "drill 39 arm Q10B1-transfer — frozen Q10B1 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl new, agent old, broker new)"
expect T "39.Q10B1-transfer frozen Q10B1 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl new, agent old, broker new)" nv_arm_ok "$COMPAT_ARMS" Q10B1-transfer
section "drill 39 arm Q11B1-exec — frozen Q11B1 exec argv, exact nonce and one execution (ctl new, agent new, broker new)"
expect T "39.Q11B1-exec frozen Q11B1 exec argv, exact nonce and one execution (ctl new, agent new, broker new)" nv_arm_ok "$COMPAT_ARMS" Q11B1-exec
section "drill 39 arm Q11B1-run — frozen Q11B1 run argv, real PTY input/output/resize and one terminal (ctl new, agent new, broker new)"
expect T "39.Q11B1-run frozen Q11B1 run argv, real PTY input/output/resize and one terminal (ctl new, agent new, broker new)" nv_arm_ok "$COMPAT_ARMS" Q11B1-run
section "drill 39 arm Q11B1-kill — frozen Q11B1 kill-probe argv, original pid SIGTERM and one terminal (ctl new, agent new, broker new)"
expect T "39.Q11B1-kill frozen Q11B1 kill-probe argv, original pid SIGTERM and one terminal (ctl new, agent new, broker new)" nv_arm_ok "$COMPAT_ARMS" Q11B1-kill
section "drill 39 arm Q11B1-expose — frozen Q11B1 expose argv plus cleanup companion, public-port nonce and cleanup (ctl new, agent new, broker new)"
expect T "39.Q11B1-expose frozen Q11B1 expose argv plus cleanup companion, public-port nonce and cleanup (ctl new, agent new, broker new)" nv_arm_ok "$COMPAT_ARMS" Q11B1-expose
section "drill 39 arm Q11B1-transfer — frozen Q11B1 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl new, agent new, broker new)"
expect T "39.Q11B1-transfer frozen Q11B1 push argv plus pull companion, 8MiB tier-B hash and unique terminals (ctl new, agent new, broker new)" nv_arm_ok "$COMPAT_ARMS" Q11B1-transfer
section "drill 39 arm LB-new — new ctl legacy-busy refusal rc64, all-entry policy, running image and business unchanged"
expect T "39.LB-new new ctl legacy-busy refusal rc64, all-entry policy, running image and business unchanged" nv_arm_ok "$COMPAT_ARMS" LB-new
section "drill 39 arm LB-old — old ctl legacy-busy refusal rc64 bound to old_ctl_rc64, running image and business unchanged"
expect T "39.LB-old old ctl legacy-busy refusal rc64 bound to old_ctl_rc64, running image and business unchanged" nv_arm_ok "$COMPAT_ARMS" LB-old
section "drill 39 arm LI-defer — unknown quiescence defers rc75 without --interrupting, original business kept"
expect T "39.LI-defer unknown quiescence defers rc75 without --interrupting, original business kept" nv_arm_ok "$COMPAT_ARMS" LI-defer
section "drill 39 arm LI-interrupting — --interrupting dispatches once rc74 and stays INCOMPLETE (outcome unknown, non-hot)"
expect T "39.LI-interrupting --interrupting dispatches once rc74 and stays INCOMPLETE (outcome unknown, non-hot)" nv_arm_ok "$COMPAT_ARMS" LI-interrupting
section "drill 39 arm M-route — mixed-consumer route change refused rc75 with no dispatch"
expect T "39.M-route mixed-consumer route change refused rc75 with no dispatch" nv_arm_ok "$COMPAT_ARMS" M-route
section "drill 39 arm CC-idle — complete idle census under a held fence, rc0 and three receipts"
expect T "39.CC-idle complete idle census under a held fence, rc0 and three receipts" nv_arm_ok "$COMPAT_ARMS" CC-idle
section "drill 39 arm CC-late-create — late create inside the observed live fence refused rc75, no untracked admission"
expect T "39.CC-late-create late create inside the observed live fence refused rc75, no untracked admission" nv_arm_ok "$COMPAT_ARMS" CC-late-create
section "drill 39 arm CL-reject — missing required reader refused rc64 before activation"
expect T "39.CL-reject missing required reader refused rc64 before activation" nv_arm_ok "$COMPAT_ARMS" CL-reject
section "drill 39 arm ABI-adjacent — adjacent ABI decodes both directions under the P26 fixture judge (interface double)"
expect T "39.ABI-adjacent adjacent ABI decodes both directions under the P26 fixture judge (interface double)" nv_arm_ok "$COMPAT_ARMS" ABI-adjacent
section "drill 39 arm ABI-too-far — two-epoch ABI gap refused rc64 before activation"
expect T "39.ABI-too-far two-epoch ABI gap refused rc64 before activation" nv_arm_ok "$COMPAT_ARMS" ABI-too-far
section "drill 39 arm ABI-unknown — unknown reader ABI refused rc64 before activation"
expect T "39.ABI-unknown unknown reader ABI refused rc64 before activation" nv_arm_ok "$COMPAT_ARMS" ABI-unknown
section "drill 39 arm GO-hash — distinct toolchains continue the checkpoint digest from exporter to reader"
expect T "39.GO-hash distinct toolchains continue the checkpoint digest from exporter to reader" nv_arm_ok "$COMPAT_ARMS" GO-hash
section "drill 39 arm CFG-nats-old — old nats-server parser (-t -c) executes the same rendered config"
expect T "39.CFG-nats-old old nats-server parser (-t -c) executes the same rendered config" nv_arm_ok "$COMPAT_ARMS" CFG-nats-old
section "drill 39 arm CFG-nats-new — new nats-server parser (-t -c) executes the same rendered config"
expect T "39.CFG-nats-new new nats-server parser (-t -c) executes the same rendered config" nv_arm_ok "$COMPAT_ARMS" CFG-nats-new
section "drill 39 arm CFG-caddy-old — old caddy validate --config executes the same rendered config"
expect T "39.CFG-caddy-old old caddy validate --config executes the same rendered config" nv_arm_ok "$COMPAT_ARMS" CFG-caddy-old
section "drill 39 arm CFG-caddy-new — new caddy validate --config executes the same rendered config"
expect T "39.CFG-caddy-new new caddy validate --config executes the same rendered config" nv_arm_ok "$COMPAT_ARMS" CFG-caddy-new
expect F "drill 39 compat: an arm with no receipt fails closed" nv_arm_ok "$COMPAT_ARMS" NO-SUCH-ARM

# origin: hot-update S9 plan §9.3 / P33 / T80; r9d D-11.
# Drills 85/86 and the 39.NI/CI legacy infra refusals run their REAL drill file per arm
# (tests/hot-update-infra-arm-test.py): the real case branch, infra_drill_* helpers, grow fixtures,
# sim_log_snapshot validator, assert.sh and native drill_end execute under the real FIX-ORPHAN
# supervisor, with only $SIM, the P32 infra_fixture seam, the vendor supply verifiers, the
# release-source supplier and the container transport as recording doubles. Each arm must reach its
# expected-verdicts.tsv verdict with a recorded trace equal to its own obligation sequence (rounds,
# hop count, refusal/heal/halt/recover, reload/support steps, CLI verb and binary, the CLI rc handed to
# each judge) and be driven off it by faults aimed at that obligation, each with its exact native
# verdict and a proof the fault was reached. Writing these sections found two seam defects: the landed
# drills/lib/hot_update_infra.sh does not implement the verbs 85/86 call (every live arm is SETUP-RED
# before its first oracle), and _infra_legacy_refusal calls sim_prev_source_ok without its tag, so
# under `set -u` 39.NI/CI abort with "parameter not set" and no verdict at all. A missing receipt is red.
section "drills 85/86/39 — infra arm controls, one isolated receipt per arm"
section "drill 85 arm A — single default: upgrade, rollback, reupgrade over three hops via admin infra"
expect T "85.A real drill reaches GREEN with its three-hop single cycle; rollback rc75, cycle/round reds and bootstrap rc1 stay red" nv_arm_ok "$INFRA_ARMS" 85.A
section "drill 85 arm A1 — sole-voter standalone JetStream: init brk1, cluster upgrade cycle"
expect T "85.A1 real drill reaches GREEN through cluster upgrade; failed init SETUP, reupgrade rc75 and cycle red stay red" nv_arm_ok "$INFRA_ARMS" 85.A1
section "drill 85 arm B — quorum adjacent cycle across nine hops after a declared grow_to_3"
expect T "85.B real drill reaches GREEN with nine hops; failed grow SETUP, rollback round red and cycle INCOMPLETE stay red" nv_arm_ok "$INFRA_ARMS" 85.B
section "drill 85 arm D — N2 refusal, product shrink heal to sole-voter, then a sole-voter roll"
expect T "85.D real drill refuses, heals to sole-voter and rolls; refusal red, heal SETUP, post-heal rc75 and leader drift stay red" nv_arm_ok "$INFRA_ARMS" 85.D
section "drill 85 arm Q — lost-quorum refusal before effect, heal, then a quorum roll"
expect T "85.Q real drill refuses lost quorum and rolls after heal; fault SETUP, refusal red and roll INCOMPLETE stay red" nv_arm_ok "$INFRA_ARMS" 85.Q
section "drill 85 arm L — stream-lag and consumer-lag check-only refusals, then one roll"
expect T "85.L real drill refuses both lags then rolls; consumer-lag red, stream-lag heal SETUP and final rc75 stay red" nv_arm_ok "$INFRA_ARMS" 85.L
section "drill 85 arm S — second launch and legacy owner cannot double-open the store"
expect T "85.S real drill exercises the double-open guard; exercise SETUP and store-lock red stay red" nv_arm_ok "$INFRA_ARMS" 85.S
section "drill 85 arm J — mismatched release refuses, exact release joins"
expect T "85.J real drill exercises exact-release join; exercise SETUP, join red and join INCOMPLETE stay red" nv_arm_ok "$INFRA_ARMS" 85.J
section "drill 85 arm F — post-LDM reply-cut halt and product recover of the same transaction"
expect T "85.F real drill halts and recovers its tx; empty/non-hex tx SETUP, halt and recover reds stay red" nv_arm_ok "$INFRA_ARMS" 85.F
section "drill 86 arm R — valid managed Caddy reload keeps PID and business"
expect T "86.R real drill reloads with the staged digest; reload rc64, short digest SETUP and reload red stay red" nv_arm_ok "$INFRA_ARMS" 86.R
section "drill 86 arm B — quorum Caddy adjacent cycle across nine hops"
expect T "86.B real drill reaches GREEN with nine hops; failed grow SETUP, cycle red and upgrade rc75 stay red" nv_arm_ok "$INFRA_ARMS" 86.B
section "drill 86 arm I — stock-root retrofit under the ACME late-write fault"
expect T "86.I real drill cycles under the ACME fault; fault SETUP, round red and bootstrap rc1 stay red" nv_arm_ok "$INFRA_ARMS" 86.I
section "drill 86 arm H — healthy N2 pair cycle across six hops"
expect T "86.H real drill reaches GREEN with six hops; leader drift SETUP, cycle red and brk2 bootstrap red stay red" nv_arm_ok "$INFRA_ARMS" 86.H
section "drill 86 arm C — invalid config and invalid certificate both refused rc64 before effect"
expect T "86.C real drill refuses both invalid inputs; accepted rc0, invalid-cert red and staging SETUP stay red" nv_arm_ok "$INFRA_ARMS" 86.C
section "drill 86 arm K — unsupported live responsibilities defer without forced close"
expect T "86.K real drill exercises the responsibility gates; exercise SETUP, gate INCOMPLETE and gate red stay red" nv_arm_ok "$INFRA_ARMS" 86.K
section "drill 86 arm F — controller death over 120s halts and admin recover settles"
expect T "86.F real drill halts and recovers; halt red, non-hex tx SETUP and recover red stay red" nv_arm_ok "$INFRA_ARMS" 86.F
section "drill 86 arm U — support update, rollback, update then a full cycle"
expect T "86.U real drill runs three support generations then cycles; support red/INCOMPLETE and cycle red stay red" nv_arm_ok "$INFRA_ARMS" 86.U
section "drill 39 arm NI — genuine tether-prev refuses a NATS infra upgrade with rc64"
expect T "39.NI real drill refuses on tether-prev with rc64; accepted rc0, wrong source, unproven release and refusal INCOMPLETE stay red" nv_arm_ok "$INFRA_ARMS" 39.NI
section "drill 39 arm CI — genuine tether-prev refuses a Caddy infra upgrade with rc64"
expect T "39.CI real drill refuses on tether-prev with rc64; accepted rc0, wrong source, unproven release and refusal INCOMPLETE stay red" nv_arm_ok "$INFRA_ARMS" 39.CI
expect F "infra: an arm with no receipt fails closed" nv_arm_ok "$INFRA_ARMS" 85.NO-SUCH-ARM

# origin: hot-update S10 plan §9 / §10.2 P19 / P20; r9d D-11. Drills 99/100 run their REAL drill shell (shared-
# supervisor re-exec, native case mapping, drill_end), fullstack_fixture.py run and fullstack_mutations.py run-arm in
# a private sandbox per arm (tests/hot-update-fullstack-arm-test.py). Only the deployment boundary is a double: the
# P25 source-pinned tool rebuild, the P10 source archive, the Go assembly host, the P01 judge and the P19/P25
# mutation host (tests/hot_update_fullstack_arm_boundary.py); docker/ssh/sudo/systemctl/journalctl/tether are
# poisoned. Each 99 arm must reach GREEN on its own sealed profile and go SETUP-RED through the exact guard for a
# missing step or product execution of its own topology's runtime, a flipped topology, another arm's profile, a
# GREEN judge with unmet criteria, foreign running bytes and a host status outside the vocabulary; ASSERT-FAIL,
# PRODUCT-RED and INCOMPLETE stay untranslated, and the real P25 tool resolution stays NOT_RUN/INCOMPLETE. Each
# 100 arm must reach its 0->1->0 ASSERT-FAIL kill on its own catalog row and go SETUP-RED for a comment-only or
# unbound patch, a surviving or translated mutant, a red outside its window or at another assertion, an
# unclassified driver crash (Python's default exit 1 is the kill status) and its row-specific tier/cell/overlay/
# restore obligation; M10C stays INCOMPLETE until P19/P25 supplies its probe sender. A missing receipt is red.
# SIM_UPGRADE_SUPERVISOR (the shared FIX-ORPHAN adapter) is required; without it every arm is red.
section "drills 99/100 — fullstack arm controls, one isolated receipt per arm"
section "drill 99 arm C1 — single calibration profile reaches GREEN; its agent a3 step/execution, topology and identity guards go red"
expect T "99.C1 real drill reaches GREEN and each guard turns it red" nv_arm_ok "$FULLSTACK_ARMS" 99.C1
section "drill 99 arm C3 — quorum calibration profile reaches GREEN; its brk3 broker step/execution, topology and identity guards go red"
expect T "99.C3 real drill reaches GREEN and each guard turns it red" nv_arm_ok "$FULLSTACK_ARMS" 99.C3
section "drill 99 arm A — single seven-row fullstack profile reaches GREEN; its agent a3 step/execution, topology and identity guards go red"
expect T "99.A real drill reaches GREEN and each guard turns it red" nv_arm_ok "$FULLSTACK_ARMS" 99.A
section "drill 99 arm B — quorum seven-row fullstack profile reaches GREEN; its brk3 broker step/execution, topology and identity guards go red"
expect T "99.B real drill reaches GREEN and each guard turns it red" nv_arm_ok "$FULLSTACK_ARMS" 99.B
section "drill 99 arm S1 — single SS profile reaches GREEN; its agent a3 step/execution, topology and identity guards go red"
expect T "99.S1 real drill reaches GREEN and each guard turns it red" nv_arm_ok "$FULLSTACK_ARMS" 99.S1
section "drill 99 arm S3 — quorum SS profile reaches GREEN; its brk3 broker step/execution, topology and identity guards go red"
expect T "99.S3 real drill reaches GREEN and each guard turns it red" nv_arm_ok "$FULLSTACK_ARMS" 99.S3
section "drill 99 arm F1 — single remote-fs profile reaches GREEN; its agent a3 step/execution, topology and identity guards go red"
expect T "99.F1 real drill reaches GREEN and each guard turns it red" nv_arm_ok "$FULLSTACK_ARMS" 99.F1
section "drill 99 arm F3 — quorum remote-fs profile reaches GREEN; its brk3 broker step/execution, topology and identity guards go red"
expect T "99.F3 real drill reaches GREEN and each guard turns it red" nv_arm_ok "$FULLSTACK_ARMS" 99.F3
section "drill 99 arm K — quorum soak profile reaches GREEN; its brk3 broker step/execution, topology and identity guards go red"
expect T "99.K real drill reaches GREEN and each guard turns it red" nv_arm_ok "$FULLSTACK_ARMS" 99.K
section "drill 100 arm M01A — agent kill bound to accepted-cut-retained-exec window, fullstackCheckRetained assertion"
expect T "100.M01A real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M01A
section "drill 100 arm M01B — broker kill bound to original-socket-unacked-duplex window, fullstackFlowTransition assertion, both cells"
expect T "100.M01B real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M01B
section "drill 100 arm M02E — agent kill bound to accepted-cut-retained-exec window, fullstackCheckRetained assertion"
expect T "100.M02E real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M02E
section "drill 100 arm M02P — agent kill bound to partial-write-pending-control-unacked-output window, fullstackCheckPTYCheckpoint assertion"
expect T "100.M02P real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M02P
section "drill 100 arm M02F — agent kill bound to original-socket-unacked-duplex window, fullstackFlowTransition assertion, both cells"
expect T "100.M02F real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M02F
section "drill 100 arm M02X — agent kill bound to nonzero-sync-prefix window, fullstackCheckTransferCheckpoint assertion"
expect T "100.M02X real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M02X
section "drill 100 arm M03E — agent kill bound to accepted-cut-retained-exec window, fullstackCheckRetained assertion"
expect T "100.M03E real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M03E
section "drill 100 arm M03P — agent kill bound to partial-write-pending-control-unacked-output window, fullstackCheckPTYCheckpoint assertion"
expect T "100.M03P real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M03P
section "drill 100 arm M03F — agent kill bound to original-socket-unacked-duplex window, fullstackFlowTransition assertion, both cells"
expect T "100.M03F real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M03F
section "drill 100 arm M03X — agent kill bound to nonzero-sync-prefix window, fullstackCheckTransferCheckpoint assertion"
expect T "100.M03X real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M03X
section "drill 100 arm M04A — agent kill bound to original-socket-unacked-duplex window, fullstackFlowTransition assertion, both cells"
expect T "100.M04A real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M04A
section "drill 100 arm M04B — broker kill bound to original-socket-unacked-duplex window, fullstackFlowTransition assertion, both cells"
expect T "100.M04B real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M04B
section "drill 100 arm M04P — agent kill bound to held-zombie-tail-before-eof window, fullstackCheckHeldTail assertion"
expect T "100.M04P real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M04P
section "drill 100 arm M05E — agent kill bound to accepted-cut-retained-exec window, fullstackCheckRetained assertion"
expect T "100.M05E real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M05E
section "drill 100 arm M05P — agent kill bound to partial-write-pending-control-unacked-output window, fullstackCheckPTYCheckpoint assertion"
expect T "100.M05P real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M05P
section "drill 100 arm M05X — broker kill bound to nonzero-sync-prefix window, fullstackCheckTransferCheckpoint assertion, both cells"
expect T "100.M05X real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M05X
section "drill 100 arm M05R — agent kill bound to active-current-reverse-new-retained-work window, fullstackJudgeRollback assertion"
expect T "100.M05R real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M05R
section "drill 100 arm M06A — agent kill bound to activation-effect-interval window, fullstackJudgeOwnership assertion"
expect T "100.M06A real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M06A
section "drill 100 arm M06B — broker kill bound to activation-effect-interval window, fullstackJudgeOwnership assertion, both cells"
expect T "100.M06B real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M06B
section "drill 100 arm M06R — agent kill bound to activation-effect-interval window, fullstackJudgeOwnership assertion, both cells"
expect T "100.M06R real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M06R
section "drill 100 arm M06F — agent kill bound to activation-effect-interval window, fullstackJudgeOwnership assertion, both cells"
expect T "100.M06F real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M06F
section "drill 100 arm M07A — agent kill bound to same-image-five-effect-settle-cycles window, fullstackCutIdentity assertion"
expect T "100.M07A real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M07A
section "drill 100 arm M07B — broker kill bound to same-image-five-effect-settle-cycles window, fullstackCutIdentity assertion, both cells"
expect T "100.M07B real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M07B
section "drill 100 arm M07N — nats kill bound to same-image-five-effect-settle-cycles window, fullstackCutIdentity assertion, deploy tier, both cells"
expect T "100.M07N real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M07N
section "drill 100 arm M07C — caddy kill bound to same-image-five-effect-settle-cycles window, fullstackCutIdentity assertion, deploy tier, both cells"
expect T "100.M07C real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M07C
section "drill 100 arm M08E — agent kill bound to same-image-five-effect-settle-cycles window, fullstackFloorWithinImage assertion"
expect T "100.M08E real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M08E
section "drill 100 arm M08P — agent kill bound to same-image-five-effect-settle-cycles window, fullstackFloorWithinImage assertion"
expect T "100.M08P real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M08P
section "drill 100 arm M08A — agent kill bound to same-image-five-effect-settle-cycles window, fullstackFloorWithinImage assertion"
expect T "100.M08A real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M08A
section "drill 100 arm M08B — broker kill bound to same-image-five-effect-settle-cycles window, fullstackFloorWithinImage assertion, both cells"
expect T "100.M08B real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M08B
section "drill 100 arm M08D — broker kill bound to same-image-five-effect-settle-cycles window, fullstackFloorWithinImage assertion, both cells"
expect T "100.M08D real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M08D
section "drill 100 arm M08X — agent kill bound to primitive-or-consumer-retirement-before-seal window, fullstackCheckTransferConsumers assertion, both cells"
expect T "100.M08X real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M08X
section "drill 100 arm M08C — caddy kill bound to same-image-five-effect-settle-cycles window, fullstackFloorWithinImage assertion, deploy tier, both cells"
expect T "100.M08C real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M08C
section "drill 100 arm M08O — fixture kill bound to same-image-five-effect-settle-cycles window, fullstackFloorWithinImage assertion, both cells"
expect T "100.M08O real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M08O
section "drill 100 arm M09A — agent kill bound to active-current-reverse-new-retained-work window, fullstackJudgeRollback assertion"
expect T "100.M09A real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M09A
section "drill 100 arm M09B — broker kill bound to active-current-reverse-new-retained-work window, fullstackJudgeRollback assertion, both cells"
expect T "100.M09B real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M09B
section "drill 100 arm M10S — fixture kill bound to sealed-required-slot-or-cut-probe window, fullstackScheduledRequests assertion"
expect T "100.M10S real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M10S
section "drill 100 arm M10C — pending probe sender stays INCOMPLETE, never the kill; bad or unparsable patches are SETUP-RED"
expect T "100.M10C stays INCOMPLETE (NOT_RUN pending P19/P25) and bad patches turn it red" nv_arm_ok "$FULLSTACK_ARMS" 100.M10C
section "drill 100 arm M11W — agent kill bound to held-zombie-tail-before-eof window, fullstackCheckHeldTail assertion"
expect T "100.M11W real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M11W
section "drill 100 arm M11I — agent kill bound to accepted-cut-retained-exec window, fullstackCheckRetained assertion"
expect T "100.M11I real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M11I
section "drill 100 arm M11O — agent kill bound to accepted-cut-retained-exec window, fullstackCheckRetained assertion"
expect T "100.M11O real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M11O
section "drill 100 arm M11P — agent kill bound to partial-write-pending-control-unacked-output window, fullstackCheckPTYCheckpoint assertion"
expect T "100.M11P real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M11P
section "drill 100 arm M11Q — agent kill bound to partial-write-pending-control-unacked-output window, fullstackCheckPTYCheckpoint assertion"
expect T "100.M11Q real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M11Q
section "drill 100 arm M11F — agent kill bound to original-socket-unacked-duplex window, fullstackFlowTransition assertion, both cells"
expect T "100.M11F real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M11F
section "drill 100 arm M11S — agent kill bound to pending-frame-current-nonce window, fullstackSSPrimitiveLedger assertion, both cells"
expect T "100.M11S real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M11S
section "drill 100 arm M11X — agent kill bound to primitive-or-consumer-retirement-before-seal window, fullstackCheckTransferEffects assertion, both cells"
expect T "100.M11X real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M11X
section "drill 100 arm M11D — broker kill bound to activation-effect-interval window, fullstackJudgePrivateCustody assertion, both cells"
expect T "100.M11D real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M11D
section "drill 100 arm M11C — caddy kill bound to activation-effect-interval window, fullstackJudgeOwnership assertion, deploy tier, both cells"
expect T "100.M11C real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M11C
section "drill 100 arm M11L — agent kill bound to activation-effect-interval window, fullstackJudgePrivateCustody assertion, both cells"
expect T "100.M11L real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M11L
section "drill 100 arm M11U — agent kill bound to activation-effect-interval window, fullstackJudgePrivateCustody assertion, both cells"
expect T "100.M11U real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M11U
section "drill 100 arm M11K — agent kill bound to activation-effect-interval window, fullstackJudgePrivateCustody assertion, both cells"
expect T "100.M11K real drill kills only with its bound 0->1->0 receipt" nv_arm_ok "$FULLSTACK_ARMS" 100.M11K
expect F "fullstack: an arm with no receipt fails closed" nv_arm_ok "$FULLSTACK_ARMS" 99.NO-SUCH-ARM

# `# arms:` reconciliation: every arm-split drill under drills/ must have at least one section per arm in
# this file, named `drill <NN> arm <ARM>` — a new arm whose oracles were never driven with bad input is the
# vacuity class this gate exists for (plan §5.5 / §6.3 D-11).
#
# origin: r9d D-11 hardening (S6–S10 arm closure). A header alone used to satisfy this loop, so the cheapest
# way to turn 209 missing arms green was 209 empty headers — a gate whose pass condition is a heading is the
# permanently-true oracle this file exists to catch. Each arm's section must now also contain at least one
# check (`expect T|F`) before the next section starts.
# nv_section_checks <file> <num> <arm> : how many expect lines sit inside that arm's section(s) of <file>.
nv_section_checks() {
    awk -v h="section \"drill $2 arm $3 " '
        index($0, h) == 1 { on = 1; next }
        /^section / { on = 0 }
        on && !/^[ \t]*#/ && /(^|[ ;])expect [TF] / { n++ }
        END { print n + 0 }' "$1"
}
section "arm-split drills ↔ sections in this file"
# Drill 999 does not exist, so these control lines (which this file also contains) can never count for a real arm.
cat > "$TMP/recon-control.sh" <<'EOR'
section "drill 999 arm Z — a heading with setup code but no check under it"
eval "$(extract "$SIMDIR/drills/999-no-such-drill.sh" _no_such_oracle)"
# expect T "999.Z a commented-out check proves nothing" true
section "drill 999 arm Y — a real section"
expect T "999.Y one check" true
EOR
expect F "CONTROL an empty arm heading proves nothing and is not counted"   test "$(nv_section_checks "$TMP/recon-control.sh" 999 Z)" -gt 0
expect T "CONTROL an arm heading followed by a check is counted"           test "$(nv_section_checks "$TMP/recon-control.sh" 999 Y)" -gt 0
expect F "CONTROL an arm with no heading at all is not counted"            test "$(nv_section_checks "$TMP/recon-control.sh" 999 X)" -gt 0
. "$SIMDIR/lib/manifest.sh"
for _f in "$SIMDIR"/drills/*.sh; do
    manifest_has "$_f" || continue
    _n=$(basename "$_f" .sh); _num=${_n%%-*}
    for _a in $(manifest_arms "$_f"); do
        _nc=$(nv_section_checks "$0" "$_num" "$_a")
        if [ "$_nc" -gt 0 ]; then
            PASS=$((PASS+1)); printf '  ok   [T] %s arm %s has a nonvacuity section (%s checks)\n' "$_n" "$_a" "$_nc"
        elif grep -q "^section \"drill $_num arm $_a " "$0"; then
            FAIL=$((FAIL+1)); printf '  FAIL %s arm %s has a section with NO check in r9d-nonvacuity.sh (an empty heading proves nothing)\n' "$_n" "$_a"
        else
            FAIL=$((FAIL+1)); printf '  FAIL %s arm %s has NO section in r9d-nonvacuity.sh (D-11: every arm oracle is proved two-sided)\n' "$_n" "$_a"
        fi
    done
done

printf '\n─────────────────────────────────────────────\nnonvacuity: %s proved, %s FAILED\n' "$PASS" "$FAIL"
[ "$FAIL" = 0 ]
