#!/bin/sh
# 33-node-upgrade-success.sh — S5, N=1 cluster + 1 agent container (TWO agent instances) + ctl: the REAL
# `node upgrade` success / rollback / upgrade-domain paths on the deployed stack — the dedicated drill that
# 31-node-upgrade-fleet's not_covered has pointed at since external-review M4. Plan (arm/oracle table,
# bad-artifact adjudication, false-green guards): docs/reviews/upgrade-success-drill-plan.md.
#
# ARMS (order is load-bearing):
#   B  real WATCHDOG ROLLBACK: a perfectly healthy tether-next artifact + a SYN-only egress block on the
#      NATS client port (fault_synblock_on — broker-ops §8.7's weak-NAT shape: the OLD process's
#      ESTABLISHED registration lives on, the re-exec'd NEW process can never complete its first register)
#      → staged + flip + re-exec → 120s register-deadline watchdog restores .prev in place.
#   C  UPGRADE DOMAIN (nested in B's pending window): the shared-binary sibling agt1b is refused
#      `upgrade_in_progress` at the MARKER gate (phrase-pinned) while B is pending.
#   B6 HEAL: lift the fault → the rolled-back OLD binary re-registers ONLINE at OLD_RELEASE, the broker
#      journal carries the agent-reported rolled_back outcome, and the delivered terminal marker is REMOVED.
#   A  real SUCCESS: same artifact, no fault → staged → in-place syscall.Exec (SAME MainPID + SAME
#      ExecMainStartTimestamp) → running-image sha == tether-next → re-register ONLINE at NEXT_RELEASE →
#      ctl --wait prints COMMITTED → marker committed (boot_count=1, target agt1).
#   C2 DOMAIN RELEASE: after A commits, agt1b's own upgrade goes through (its own four-part proof), proving
#      C's refusal was windowed, not permanent. ⚠ HARD CLAUSE: C2 wholesale-REPLACES the shared marker —
#      every agt1 marker assertion MUST complete before C2; nothing may read agt1's marker after it.
#
# FALSE-GREEN GUARDS (full reverse arguments in the plan §3 table):
#  - B2's gate is the verbatim staged line (node.go dispatchUpgrade) — reachable only after BOTH url gates
#    + download + sha + smoke + flip; "never upgraded at all" cannot satisfy later old-sha assertions.
#  - B3 polls a WHOLE-conjunction (pending ∧ boot_count>=1 ∧ dst==NEXT ∧ MainPID==PID0 ∧ /proc/exe==NEXT):
#    boot_count is written 0 at install and only the staged image's boot shim increments it — the only
#    in-window direct proof the new process actually ran; PID alone is weak (a systemd restart changes it,
#    and exe-sha alone can't tell flip-without-exec), the conjunction only holds for an in-place exec.
#  - B4's marker Detail must contain `register deadline exceeded` — pinning the WATCHDOG leg and excluding
#    its two siblings (`syscall.Exec …` exec-fail recovery, `boot: budget/deadline exhausted` boot budget).
#  - C is pinned on `agent_rejected:upgrade_in_progress`. HONEST LIMIT (first real run): ctl prints the
#    operator HINT for the code, not the agent's own sentence, so the three upgrade_in_progress emitters
#    (in-process TryLock / host flock / marker gate) are indistinguishable HERE — the deploy tier proves
#    the refusal reached a real sibling PROCESS over the real stack; which gate fired is pinned by the
#    hermetic test/p10 TestUpgradeRejectedWhilePending. The `agent_rejected:` prefix still excludes every
#    broker-side refusal, and the marker gate sits BEFORE the agent's allowlist check, so agt1b's
#    refusal does not depend on its own url_allow config.
#  - A1 greps the shared phrases + NEXT_RELEASE, not whole lines (two COMMITTED line variants exist).
#  - FIN punch-through (real behavior, not a GAP): the exec teardown's FIN escapes the SYN-only block, so
#    agt1 goes OFFLINE inside B's window — no arm asserts agt1 ONLINE in-window; C uses agt1b instead.
# NOT-COVERED (honest, registered at drill_end): gotcha #73 — a NON-tether artifact that fakes the version
# line passes the smoke gate and then has NO boot shim: budget never ticks, watchdog died with the old
# image, marker pends forever. This drill deliberately does NOT stage such an artifact (it would wedge
# agt1 for manual recovery); the gap is owned by #73 until a probe drill or a product defense lands.
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/agentyaml.sh"; . "$HERE/drills/lib/ident.sh"
. "$HERE/drills/lib/ingress.sh"; . "$HERE/drills/lib/artifact.sh"
. "$HERE/drills/lib/upgradecfg.sh"; . "$HERE/drills/lib/fault.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
CA=/usr/local/share/ca-certificates/tether-sim-ca.crt
DST=/home/sim/.local/bin/tether
MARKER=/home/sim/.local/bin/.tether-upgrade.json
PREV=/home/sim/.local/bin/tether.prev

_cleanup() {
    artifact_down 2>/dev/null
    fault_cleanup_all 2>/dev/null
    dexec agt1 -- sh -c 'systemctl disable --now tether-agent-b 2>/dev/null; rm -f /etc/systemd/system/tether-agent-b.service; systemctl daemon-reload' >/dev/null 2>&1
    true
}
drill_install_traps _cleanup

# ---- probes (assert_ok runs these in a function-inheriting subshell) ----------------------------------
_broker_up()      { "$SIM" exec brk1 -- tether cluster status --json 2>/dev/null | jq -e '.leader_id=="brk1"' >/dev/null 2>&1; }
_ls_json()        { "$SIM" ctl -- node ls --json 2>/dev/null; }
_online()         { _ls_json | jq -e --arg n "$1" '.nodes[]?|select(.nid==$n and .status=="ONLINE")' >/dev/null 2>&1; }
# NB the wire field is `release_version` (proto.NodeListEntry), NOT `.release` — the first real run
# of this drill captured OLD_RELEASE="null" from the wrong path and every release comparison below
# silently compared against that literal.
_online_at()      { _ls_json | jq -e --arg n "$1" --arg r "$2" '.nodes[]?|select(.nid==$n and .status=="ONLINE" and .release_version==$r)' >/dev/null 2>&1; }
_release_of()     { _ls_json | jq -r --arg n "$1" '.nodes[]?|select(.nid==$n)|.release_version' | head -1; }
_agt1_online()    { _online agt1; }
_agt1b_online()   { _online agt1b; }
_marker_json()    { dexec agt1 -- cat "$MARKER" 2>/dev/null; }
_dst_sha()        { dexec agt1 -- sha256sum "$DST" 2>/dev/null | awk '{print $1}'; }
_prev_sha()       { dexec agt1 -- sha256sum "$PREV" 2>/dev/null | awk '{print $1}'; }
_mainpid()        { "$SIM" exec agt1 -- systemctl show "$1" -p MainPID --value 2>/dev/null; }
_exec_ts()        { "$SIM" exec agt1 -- systemctl show "$1" -p ExecMainStartTimestamp --value 2>/dev/null; }
_exe_sha_of_pid() { dexec agt1 -- sha256sum "/proc/$1/exe" 2>/dev/null | awk '{print $1}'; }
_prev_gone()      { dexec agt1 -- test ! -e "$PREV"; }

# B3/B4 whole-conjunction poll predicates (single jq over the marker + disk/process facts).
_b3_pending_staged() {
    _m=$(_marker_json) || return 1
    printf '%s' "$_m" | jq -e '.state=="pending" and .boot_count>=1' >/dev/null 2>&1 || return 1
    [ "$(_dst_sha)" = "$NEXTBIN_SHA" ] || return 1
    [ "$(_mainpid tether-agent)" = "$PID0" ] || return 1
    [ "$(_exe_sha_of_pid "$PID0")" = "$NEXTBIN_SHA" ]
}
_b4_rolled_back() {
    _m=$(_marker_json) || return 1
    printf '%s' "$_m" | jq -e '.state=="rolled_back" and (.detail|contains("register deadline exceeded"))' >/dev/null 2>&1 || return 1
    [ "$(_dst_sha)" = "$OLD_SHA" ] || return 1
    _prev_gone || return 1     # prev consumed by the restore rename
    [ "$(_mainpid tether-agent)" = "$PID0" ] || return 1
    [ "$(_exe_sha_of_pid "$PID0")" = "$OLD_SHA" ]
}
# origin: first real run (2026-08-02), TWO wrong reads in a row — recorded because the second one
# is the instructive one. (1) `$SIM logs` reads only the last 200 journal lines, so a line written at
# the start of B6b's 120s poll had scrolled away. Widening the window did NOT fix it, which exposed
# (2) the real cause: install.sh's broker unit sets `StandardOutput=append:/var/log/tether/broker.log`
# (+ .err), so the broker's log never reaches journald at all — `journalctl -u tether-broker` is
# EMPTY for this unit by construction. The evidence was never missing (B6d proves the register
# carried upgrade_state: the terminal marker is only cleared when the reported state matches); the
# drill was reading a stream the product does not write to. Read the FILE the unit actually writes.
_brk_log()           { dexec brk1 -- sh -c 'cat /var/log/tether/broker.log /var/log/tether/broker.err 2>/dev/null'; }
_b6_outcome_logged() { _brk_log | grep 'agent-reported upgrade outcome' | grep -q 'rolled_back'; }
_b6_marker_gone()    { dexec agt1 -- test ! -e "$MARKER"; }
_a2_inplace() {
    [ "$(_mainpid tether-agent)" = "$PID0" ] || return 1
    [ "$(_exec_ts tether-agent)" = "$TS0" ] || return 1
    [ "$(_exe_sha_of_pid "$PID0")" = "$NEXTBIN_SHA" ]
}
_a3_interlock() {
    _online_at agt1 "$NEXT_RELEASE" || return 1
    [ "$(_dst_sha)" = "$NEXTBIN_SHA" ] || return 1
    [ "$(_prev_sha)" = "$OLD_SHA" ] || return 1
    _marker_json | jq -e --arg s "$NEXTBIN_SHA" '.state=="committed" and .boot_count==1 and .new_sha==$s and .target_nid=="agt1"' >/dev/null || return 1
    _brk_log | grep 'agent-reported upgrade outcome' | grep -q committed
}
_c2_committed() {
    [ -n "${_c2_rc:-}" ] && [ "$_c2_rc" -eq 0 ] || return 1
    printf '%s' "$_c2_out" | grep -q 'upgrade COMMITTED' || return 1
    _online_at agt1b "$NEXT_RELEASE" || return 1
    [ "$(_mainpid tether-agent-b)" = "$AGT1B_PID0" ] || return 1
    _marker_json | jq -e '.state=="committed" and .target_nid=="agt1b"' >/dev/null
}

# agt1b: the SHARED-BINARY SIBLING — a second systemd unit in the SAME container, own HOME (own nkey,
# own member binding via one-shot --pin connect), SAME ExecStart binary path (the upgrade domain).
# Spike-verified: two units coexist active/active on the sim image.
_agt1b_provision() {
    dexec -u sim agt1 -- sh -c "HOME=/home/sim/agt1b-home; export HOME; mkdir -p \$HOME; timeout 6 tether agent --session '$SID' --nid agt1b --pin '$PIN' --nats-url 'nats://brk1:4222' >/tmp/agt1b-bind.log 2>&1 || true"
    dexec agt1 -- sh -c "cat > /etc/systemd/system/tether-agent-b.service <<UNIT
[Unit]
Description=tether agent b (sim shared-binary sibling)
After=network-online.target
[Service]
Type=simple
User=sim
Environment=HOME=/home/sim/agt1b-home
ExecStart=$DST agent --session $SID --nid agt1b --nats-url nats://brk1:4222
Restart=on-failure
RestartSec=2
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload"
    sctl agt1 enable --now tether-agent-b
}
_agt1b_setup() { _agt1b_provision && poll_until 40 3 "agt1b ONLINE" -- _agt1b_online; }
# die-gate: the upgrade-domain premise is non-vacuous only if both units execute the SAME binary path
# resolving to the SAME inode.
_same_inode() {
    dexec agt1 -- sh -c "
        a=\$(systemctl show tether-agent -p ExecStart 2>/dev/null | grep -o 'path=[^ ;]*' | head -1 | cut -d= -f2)
        b=\$(systemctl show tether-agent-b -p ExecStart 2>/dev/null | grep -o 'path=[^ ;]*' | head -1 | cut -d= -f2)
        [ -n \"\$a\" ] && [ \"\$a\" = \"\$b\" ] && [ \"\$(stat -c %i \"\$a\")\" = \"\$(stat -c %i \"\$b\")\" ]"
}

drill_begin "33-node-upgrade-success (real re-exec success + watchdog rollback + upgrade domain)"
"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 1 broker + 1 agent + 1 ctl"           "$SIM" up --brokers 1 --agents 1 --ctl 1
assert_ok "init brk1 (N=1 cluster)"                  "$SIM" init brk1
assert_ok "SETUP broker.upgrade.url_allow += artifact prefix (labeled operator config) + broker restart" \
          upgradecfg_allow_broker brk1 _broker_up
assert_ok "session lab + ctl login (owner)"          "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1"                          "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_ok "SETUP agt1b sibling unit (own HOME/nkey, SAME binary path) + ONLINE" _agt1b_setup
assert_ok "DIE-GATE both units execute the same binary path AND inode (upgrade-domain premise non-vacuous)" \
          _same_inode

artifact_up "$INSTANCE" "$HERE/vendor/tether-next" "tether-next.tar.gz" || die "33: artifact_up failed"
assert_ok "SETUP artifact serving tether-next (ART_URL/ART_SHA populated)" \
          sh -c "[ -n '$ART_URL' ] && [ -n '$ART_SHA' ]"
assert_ok "SETUP inject instance CA into agt1 trust store" ingress_trust_inject agt1
assert_ok "SETUP agt1 agent.upgrade.url_allow += artifact prefix + restart + ONLINE" \
          upgradecfg_allow_agent agt1 _agt1_online
# origin: internal review F33-2 (S2) — agt1b's allow-list MUST be configured HERE, not at C2 time.
# Doing it after arm A would restart agt1b onto the already-committed NEXT binary, and its own
# upgrade would then hit the same-tag gate (UNCONFIRMED, rc!=0) — the exact case the plan excluded
# from this drill. Configuring it now costs arm C nothing: C's refusal comes from the marker entry
# gate, which sits BEFORE the agent's allow-list check in the install pipeline.
# NOTE the node argument is agt1: agt1b is a second UNIT inside the agt1 container, not a container.
assert_ok "SETUP agt1b agent.upgrade.url_allow (own HOME tree, unit tether-agent-b) + restart + ONLINE" \
          upgradecfg_allow_agent agt1 _agt1b_online "$SID" /home/sim/agt1b-home/.tether tether-agent-b

# ---- version/sha capture (never literal-pinned: plan §1) ----------------------------------------------
OLD_RELEASE=$(_release_of agt1)
# vendor/tether-next is a single staged BINARY (lib/stage.sh: same source, version bumped to
# SIM_VERSION_NEXT) — parse its frozen first line host-side; hash the BINARY (what /proc/exe and dst
# must equal), never the tarball.
NEXT_RELEASE=$("$HERE/vendor/tether-next" version 2>/dev/null | head -1 | awk '{print $2}')
NEXTBIN_SHA=$(sha256sum "$HERE/vendor/tether-next" 2>/dev/null | awk '{print $1}')
OLD_SHA=$(_dst_sha)
PID0=$(_mainpid tether-agent)
TS0=$(_exec_ts tether-agent)
log "33: OLD_RELEASE=$OLD_RELEASE NEXT_RELEASE=$NEXT_RELEASE OLD_SHA=$OLD_SHA NEXTBIN_SHA=$NEXTBIN_SHA PID0=$PID0"
[ -n "$OLD_RELEASE" ] && [ -n "$NEXT_RELEASE" ] && [ "$OLD_RELEASE" != "$NEXT_RELEASE" ] \
    || die "33: OLD_RELEASE ($OLD_RELEASE) vs NEXT_RELEASE ($NEXT_RELEASE) — same/empty would collapse A/B into UNCONFIRMED"
[ -n "$NEXTBIN_SHA" ] && [ -n "$OLD_SHA" ] && [ -n "$PID0" ] || die "33: sha/pid capture failed"

# ═══ Arm B — watchdog rollback (SYN-only egress block; established flows live) ═══════════════════════
assert_ok "B1 syn-block agt1 -> brk1:4222 (new connections only)"  fault_synblock_on agt1 4222
assert_ok "B1b self-proof: rule took effect — a NEW connection to brk1:4222 black-holes (rc 124, not refused)" \
          fault_assert_blackholed agt1 brk1 4222
assert_ok "B1c established flows live: agt1 still ONLINE through its pre-fault registration" _agt1_online

B_OUT=$(mktemp); B_RC=$(mktemp)
( "$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA" >"$B_OUT" 2>&1; echo $? >"$B_RC" ) &
B_JOB=$!
assert_ok "B2 staged: ctl printed the verbatim staged line (both url gates + download + sha + smoke + flip all passed)" \
          poll_until 60 2 "staged line in ctl output" -- sh -c "grep -q 'staged (→ $NEXT_RELEASE, smoke ok); agent re-exec in progress' '$B_OUT'"
assert_ok "B3 pending window (whole conjunction): marker pending ∧ boot_count>=1 ∧ dst==NEXT ∧ MainPID==PID0 ∧ /proc/exe==NEXT (in-place exec of the staged image, and its boot shim really ran)" \
          poll_until 45 2 "B3 conjunction" -- _b3_pending_staged

# ── Arm C — upgrade domain, nested in B's pending window ──
assert_ok "C-pre agt1b still ONLINE (its established registration is untouched by the SYN-only block)" _agt1b_online
# origin: first real run (2026-08-02). The marker-gate's own sentence never reaches ctl: ctl replaces
# the agent's detail with the operator-facing HINT keyed by the wire code (brokerCodeHints), so the
# three upgrade_in_progress emitters are INDISTINGUISHABLE from the ctl side. What ctl DOES prove is
# the `agent_rejected:` prefix — the refusal came from the AGENT process, over the real stack, not
# from a broker-side gate. Pin that here, and leave "WHICH agent gate fired" to the hermetic test
# that can read the raw reply (test/p10 TestUpgradeRejectedWhilePending pins the marker-gate phrase).
assert_refuses "C sibling agt1b refused BY THE AGENT while B pends — agent_rejected:upgrade_in_progress (a broker-side refusal carries no agent_rejected: prefix; which agent-side gate fired is not observable from ctl — see the header note)" \
               "agent_rejected:upgrade_in_progress" \
               "$SIM" ctl -- node upgrade agt1b --url "$ART_URL" --sha256 "$ART_SHA"

assert_ok "B4 watchdog rollback (whole conjunction, fault still armed ⇒ marker static): rolled_back ∧ Detail~'register deadline exceeded' ∧ dst==OLD ∧ .prev consumed ∧ MainPID==PID0 ∧ /proc/exe==OLD" \
          poll_until 210 5 "B4 conjunction" -- _b4_rolled_back
wait "$B_JOB" 2>/dev/null || true
_b_rc=$(cat "$B_RC" 2>/dev/null); _b_out=$(cat "$B_OUT" 2>/dev/null); rm -f "$B_OUT" "$B_RC"
log "33-DBG B ctl rc=$_b_rc out=$(printf '%s' "$_b_out" | tr '\n' '~')"
assert_ok "B5 ctl verdict: rc!=0 AND the output carries BOTH the staged line AND 'likely ROLLED BACK' naming $OLD_RELEASE (inference oracle — the truth is B4's disk/process conjunction)" \
          sh -c '[ -n "$1" ] && [ "$1" -ne 0 ] && printf "%s" "$2" | grep -q "staged (→" && printf "%s" "$2" | grep -q "likely ROLLED BACK" && printf "%s" "$2" | grep -q "'"$OLD_RELEASE"'"' _ "$_b_rc" "$_b_out"

assert_ok "B6 heal: lift the syn-block"              fault_synblock_off agt1
assert_ok "B6b the rolled-back OLD binary re-registers ONLINE at OLD_RELEASE (pure product self-heal, no operator action)" \
          poll_until 120 3 "agt1 ONLINE@OLD" -- _online_at agt1 "$OLD_RELEASE"
assert_ok "B6c broker journal carries the agent-reported rolled_back outcome (only a register carrying upgrade_state from the marker's target can emit it)" \
          poll_until 60 3 "outcome line" -- _b6_outcome_logged
assert_ok "B6d delivered terminal marker is REMOVED (one-shot delivery proof — S24 asymmetry)" \
          poll_until 60 3 "marker gone" -- _b6_marker_gone

# ═══ Arm A — real success ══════════════════════════════════════════════════════════════════════════════
_a_out=$("$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA" 2>&1); _a_rc=$?
log "33-DBG A ctl rc=$_a_rc out=$(printf '%s' "$_a_out" | tr '\n' '~')"
assert_ok "A1 ctl success: rc=0 ∧ staged line ∧ 'waiting for re-register (deadline 120s)' ∧ 'upgrade COMMITTED' naming $NEXT_RELEASE (phrases not whole lines — two COMMITTED variants exist)" \
          sh -c '[ "$1" -eq 0 ] && printf "%s" "$2" | grep -q "staged (→" && printf "%s" "$2" | grep -q "waiting for re-register (deadline 120s)" && printf "%s" "$2" | grep -q "upgrade COMMITTED" && printf "%s" "$2" | grep -q "'"$NEXT_RELEASE"'"' _ "$_a_rc" "$_a_out"
assert_ok "A2 in-place exec: MainPID unchanged ∧ ExecMainStartTimestamp unchanged ∧ /proc/PID0/exe sha==NEXT (same PID through THREE execs; only a real re-exec runs the new bytes)" \
          _a2_inplace
assert_ok "A3 registration/disk interlock: node ls RELEASE==NEXT ∧ ONLINE; dst==NEXT; .prev==OLD; marker committed ∧ boot_count==1 ∧ new_sha==NEXT ∧ target_nid==agt1; journal committed outcome" \
          _a3_interlock
assert_ok "A4 version-skew pin: agt1b still ONLINE at OLD_RELEASE (the sibling keeps running the old image; the four-part proof kept it from claiming A's commit)" \
          _online_at agt1b "$OLD_RELEASE"

# ═══ Arm C2 — domain release ═══════════════════════════════════════════════════════════════════════════
# ⚠ HARD CLAUSE: A's marker assertions are COMPLETE (A3). C2 wholesale-replaces the shared marker; nothing
# below may read agt1's marker.
AGT1B_PID0=$(_mainpid tether-agent-b)
_c2_out=$("$SIM" ctl -- node upgrade agt1b --url "$ART_URL" --sha256 "$ART_SHA" 2>&1); _c2_rc=$?
log "33-DBG C2 ctl rc=$_c2_rc out=$(printf '%s' "$_c2_out" | tr '\n' '~')"
assert_ok "C2 domain released: sibling's own upgrade COMMITs (rc=0 ∧ COMMITTED ∧ agt1b ONLINE@NEXT ∧ agt1b MainPID unchanged ∧ marker target_nid==agt1b)" \
          _c2_committed

not_covered "33 (gotcha #73, honest): a NON-tether artifact that fakes the frozen version line passes the smoke gate, then has NO boot shim — budget never ticks, the watchdog died with the old image, the marker pends forever, the node is stranded behind NAT" \
    "DELIBERATELY not staged here: it would wedge agt1 into exactly that stranded state and require manual .prev recovery mid-drill. The scenario is registered as deploy-tier-gotchas #73 (OPEN); owner lands either as a dedicated probe drill (34) or as a product defense (artifact shim self-attestation) — whichever closes #73 flips this entry." gap

drill_end
