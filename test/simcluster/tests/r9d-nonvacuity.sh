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
# shadow the hardcoded path by re-defining the two predicates' file via a tiny wrapper is NOT possible
# (they read /tmp/roll.log literally) — so write the real path, which is what the drill uses too.
cp /dev/null /tmp/roll.log 2>/dev/null || true
printf 'HALTED at brk2: agent re-exec refused: agent_no_responders (no agent answered for nid colo-brk2)\n' > /tmp/roll.log
expect T "TRUE  _halted_on_agent_leg on the real P3 (b) failure chain"              _halted_on_agent_leg
expect T "TRUE  _roll_not_blocked (an agent-leg HALT is NOT a lock fence)"          _roll_not_blocked
printf 'HALTED at brk2: reload refused: sha256_mismatch\n' > /tmp/roll.log
expect F "MUTATION halted at the right host but on the RELOAD leg → must NOT count as the agent-leg HALT" _halted_on_agent_leg
printf 'HALTED at brk3: agent re-exec refused: agent_no_responders\n' > /tmp/roll.log
expect F "MUTATION agent-leg HALT at the WRONG host → must NOT count (the per-hop expectation is host-specific)" _halted_on_agent_leg
printf 'rolling upgrade complete.\n' > /tmp/roll.log
expect F "MUTATION a COMPLETED roll must not match the HALT signature"              _halted_on_agent_leg
printf 'cluster upgrade HALTED at : acquire upgrade lock refused: bad_request a `cluster add` grow of brk3 is in progress\n' > /tmp/roll.log
expect T "TRUE  _roll_halted_on_growlock catches the G4 §B 'grow … is in progress' wording (the shape the PRE-R9-D pattern MISSED)" _roll_halted_on_growlock
expect F "MUTATION _roll_not_blocked must be FALSE while the roll is lock-fenced"   _roll_not_blocked
printf 'cluster upgrade HALTED at brk2: a cluster membership operation (join/retire) is in flight\n' > /tmp/roll.log
expect T "TRUE  _roll_halted_on_growlock still catches the round2-B2 wording"       _roll_halted_on_growlock
rm -f /tmp/roll.log
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
cat > /tmp/roll.log <<'EOR'
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
cat > /tmp/roll.log <<'EOR'
-> re-exec brk3's co-located agent into v-next
error: cluster upgrade HALTED at brk3: agent re-exec refused
EOR
expect F "MUTATION announced the agent re-exec but the host was never declared done → must NOT count" _roll_reexeced_agent_of brk3
# MUTATION: broker-only completion must not satisfy the whole-host mechanism oracle
printf '  ✓ brk1 (broker-only host) at v-next\n' > /tmp/roll.log
expect F "MUTATION a broker-ONLY host completing must NOT read as an agent-leg re-exec" _roll_reexeced_agent_of brk1
rm -f /tmp/roll.log
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

printf '\n─────────────────────────────────────────────\nnonvacuity: %s proved, %s FAILED\n' "$PASS" "$FAIL"
[ "$FAIL" = 0 ]
