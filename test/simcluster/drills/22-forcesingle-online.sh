#!/bin/sh
# 22-forcesingle-online.sh — S6 (N=2, explore→pin): the ONLINE force-single escape hatch (the "preferred
# path", cluster_offline.go:220) — the FIRST drill to actually exercise it (12/20 use OFFLINE because the
# repo flagged the online dwell as bounce-fragile; 20:8-13). arm→confirm→commit over the running broker's
# LOCAL admin socket, in-process (RecoverToSelfOnline: no daemon stop, MainPID preserved).
#
# Arm-0 is a TOTAL FUNCTION (plan §2.5, no false-green hiding region):
#   POSITIVE  iff after killing the peer brk1 stays continuously up (NRestarts unchanged, MainPID stable)
#             AND the dwell reaches WouldProceed=true  → run the positive commit path.
#   #35 RED   iff brk1 RESTARTS (NRestarts++) AND the dwell never reaches WouldProceed=true AND the journal
#             shows the startup JS-unavailable crash-loop → a PRODUCTION-REAL unreachable-dwell deadlock.
#   TAMED     iff NRestarts>0 but the dwell still eventually reaches WouldProceed=true → a container-timing
#             artifact; label it + a production-fidelity NOT-COVERED note; fall through to the positive path.
#   else      → INCONCLUSIVE/RED (never a silent POSITIVE). APPEARS-FIXED: if the bounce is gone → POSITIVE.
# Stage-B spike (SB-22, plan §12) on a MINIMAL N=2 already observed POSITIVE (NRestarts=0, WouldProceed=true
# at ~70s); this drill re-runs it on the FULL setup_forcesingle_n2 (agent + tier-B JS activity = the bounce
# context of 20:8-13) to see whether the JS activity flips it to the bounce branch.
#
# THE MECHANISM (source-verified): 4 gates evaluated at arm (force_single_online.go): quorum_not_lost
# (leader contact / within the 15s dwell, DwellRemaining set), peer_alive (HARD-REFUSE a peer answering its
# port), arm_expired (single-use/TTL token). Online does NOT de-cluster nats.conf (R3, offline-only) — it
# prints the `reconcile nats --to-standalone` remedy. The PRIMARY positive oracle is a CONTROL-SOURCE WRITE
# (a protected-mode routine op that was refused during quorum loss now SUCCEEDS post-commit) + MainPID
# unchanged (a changed MainPID = a restart = HARD FAIL). We DELIBERATELY do NOT assert tier-B recovers
# (data plane stays 503 until the operator's --to-standalone = 92(b)/offline scope; asserting here = over-reach).
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/drills/lib/setup-forcesingle.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab

FS_ONLINE="tether cluster recovery force-single --online --self-id brk1 --confirm-peers-dead brk2"
FS_DRY()  { dexec -u tether brk1 -- sh -c "$FS_ONLINE --dry-run 2>&1"; }
FS_ARM()  { dexec -u tether brk1 -- sh -c "$FS_ONLINE 2>&1"; }                                  # gate-refusal arms (no pty; refuses before confirm)
FS_COMMIT() { dexec -u tether brk1 -- python3 /opt/sim/pty-confirm.py brk1 -- $FS_ONLINE; }     # pty-fed typed confirm (the real commit)
_pid()    { "$SIM" exec brk1 -- systemctl show tether-broker -p MainPID  --value 2>/dev/null | tr -d '\r'; }
_nr()     { "$SIM" exec brk1 -- systemctl show tether-broker -p NRestarts --value 2>/dev/null | tr -d '\r'; }
# _dry_proceeds: the online --dry-run gate reports it WOULD proceed (dwell satisfied). Capture first.
_dry_proceeds() { FS_DRY | grep -qiE 'would proceed|WouldProceed=true|proceed: *true'; }
# protected-mode control op: set-raft-addr is a raft WRITE (Propose PlanClusterNodeReaddr) — refused under
# quorum loss (ErrNotLeader), succeeds once force-single makes brk1 a writable quorum-of-1.
SRA="tether cluster set-raft-addr brk1:7400 --route nats://brk1:6222"
_set_raft_addr() { dexec -u tether brk1 -- sh -c "$SRA 2>&1"; }
_sra_ok()        { dexec -u tether brk1 -- sh -c "$SRA" >/dev/null 2>&1; }   # rc-only, for poll_until
_pin_brk1() {
    _pbo=$(dexec -u tether brk1 -- tether cluster transfer-leader brk1 --wait 2>&1); _pbr=$?
    [ "$_pbr" = 0 ] || printf '%s' "$_pbo" | grep -qiE 'already the leader|is this node'
}
# Send one exact JSON request over the root-only Unix admin socket. This is deliberately below the CLI:
# the CLI never prints the arm token, while the replay/TTL safety property is a wire-protocol property.
_admin_json() {
    dexec -u tether brk1 -- python3 -c 'import socket,sys
s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); s.connect("/var/run/tether/admin.sock")
s.sendall((sys.argv[1]+"\n").encode()); b=b""
while not b.endswith(b"\n"):
 c=s.recv(65536)
 if not c: break
 b+=c
sys.stdout.write(b.decode())' "$1"
}

drill_begin "S6-22 ONLINE force-single: dwell + refusal gates + protected-mode (explore→pin, expect POSITIVE)"

# ── SETUP: full N=2 clustered-JS + agent + tier-B baseline (the bounce context of 20:8-13) ──────────
setup_forcesingle_n2
# The survivor must be the raft LEADER (RecoverToSelfOnline + the set-raft-addr control both gate on
# state==Leader at node.go:401-404). Pin brk1 as leader.
dexec -u tether brk1 -- tether cluster transfer-leader brk1 --wait >/dev/null 2>&1 || warn "transfer-leader brk1 warned"
sleep 3
LDR=$("$SIM" status --json 2>/dev/null | jq -r '.leader_id // empty' 2>/dev/null)
assert_ok "survivor brk1 is the raft leader (control-source ops require it)" sh -c "[ '$LDR' = brk1 ]"

# ── PRE-KILL arms (healthy, reachable every run) ────────────────────────────────────────────────────
# DRY on a healthy cluster: WouldProceed=false, reason = leader contact; zero-mutation (MainPID unchanged).
PID_H=$(_pid)
assert_ok "DRY (healthy) reports would-NOT-proceed (leader contact) + no mutation" sh -c "
  out=\$($SIM exec brk1 -- sh -c '$FS_ONLINE --dry-run 2>&1'); echo \"\$out\" | grep -qiE 'would NOT proceed|leader contact|NO changes'"
assert_ok "DRY (healthy) is zero-mutation: MainPID unchanged, no force_single_active" sh -c "
  [ \"\$($SIM exec brk1 -- systemctl show tether-broker -p MainPID --value | tr -d '\r')\" = '$PID_H' ] &&
  ! $SIM exec brk1 -- sh -c 'tether cluster status --json 2>/dev/null | jq -e \".force_single==true\" >/dev/null'"
# GATE-a: a REAL (non-dry) arm on a healthy cluster refuses at the gate (quorum_not_lost), no confirm prompt.
assert_refuses "GATE-a healthy REAL arm refuses (quorum_not_lost / leader contact)" \
    "quorum_not_lost|leader contact|quorum-loss escape" FS_ARM
# YES-off: the OFFLINE path with --yes is Tier-2-rejected (robust always-reachable anchor; offline has no dwell).
assert_refuses "YES-off: offline --yes rejected (Tier-2, NO --yes override)" \
    "NO --yes override|cannot run unattended" \
    dexec -u tether brk1 -- sh -c "tether cluster recovery force-single --self-id brk1 --confirm-peers-dead brk2 --yes 2>&1"
# YES-online (#36 candidate): the ONLINE path with --yes is NOT routed through the offline Tier-2 rejector —
# on a healthy cluster it refuses via the online quorum_not_lost gate ("leader contact"), NEVER the offline
# "NO --yes override" string. The distinct refusal string (vs YES-off above) IS the #36 finding: online
# force-single's --yes handling diverges from offline's Tier-2 rejection (cluster_offline.go:155-158 early
# return skips the :165 rejector). TTY protection stays intact (a real commit still needs the typed confirm).
assert_refuses "YES-online (#36): online --yes refuses via the online gate (leader contact), NOT offline 'NO --yes override'" \
    "leader contact|quorum-loss escape|requires an interactive terminal" \
    dexec -u tether brk1 -- sh -c "tether cluster recovery force-single --online --self-id brk1 --confirm-peers-dead brk2 --yes 2>&1"

# ── ARM-0: DIAGNOSIS SPIKE — kill the peer (REAL Restart=always unit NEVER masked) ──────────────────
PID0=$(_pid); NR0=$(_nr)
log "Arm-0 baseline: brk1 MainPID=$PID0 NRestarts=$NR0"
node_kill brk2
assert_ok "Arm-0③ brk2 raft :7400 refused (peer provably dead)" poll_until 30 2 "brk2 :7400 refused" -- tcp_refused brk2 7400
# observe up to 120s: does brk1 restart? does the dwell reach WouldProceed=true?
DRY_PROCEEDED=no; i=0
while [ $i -lt 24 ]; do
    sleep 5; i=$((i+1))
    if _dry_proceeds; then DRY_PROCEEDED=yes; log "Arm-0: dwell reached WouldProceed=true at t=$((i*5))s (MainPID=$(_pid) NRestarts=$(_nr))"; break; fi
done
PID1=$(_pid); NR1=$(_nr)
log "Arm-0 post-observe: MainPID=$PID1 NRestarts=$NR1 DRY_PROCEEDED=$DRY_PROCEEDED"

# classify the branch (TOTAL function)
BRANCH=inconclusive
if [ "$NR1" = "$NR0" ] && [ "$PID1" = "$PID0" ] && [ "$DRY_PROCEEDED" = yes ]; then BRANCH=positive
elif [ "$NR1" != "$NR0" ] && [ "$DRY_PROCEEDED" = no ]; then BRANCH=gotcha
elif [ "$NR1" != "$NR0" ] && [ "$DRY_PROCEEDED" = yes ]; then BRANCH=tamed
fi
log "Arm-0 verdict: BRANCH=$BRANCH (NR0=$NR0 NR1=$NR1 PID0=$PID0 PID1=$PID1 dwell=$DRY_PROCEEDED)"

# ── PROT protected-mode: during quorum loss, a routine raft-write op (set-raft-addr) is refused ──────
# R-CONTROLSRC victim leg (the control leg = the SAME op SUCCEEDS post-commit in the POSITIVE branch).
# Live signature (SB-22): quorum loss surfaces as "no leader (election in progress)" (the raft write can't
# find a leader to propose to), not a bare "not the leader".
assert_refuses "PROT set-raft-addr refused during quorum loss (no leader / election in progress)" \
    "no leader|election in progress|not .*leader|ErrNotLeader|no quorum" _set_raft_addr

case "$BRANCH" in
positive|tamed)
    if [ "$BRANCH" = tamed ]; then
        warn "Arm-0 TAMED: brk1 restarted (NRestarts $NR0→$NR1) but the dwell still satisfied — a container-timing artifact. Falling through to the positive path."
        not_covered "Arm-0 uninterrupted-dwell production-fidelity (TAMED: the survivor restarted but the dwell still satisfied — a container-timing artifact)" \
            "the positive commit path IS exercised below, but the production-real uninterrupted-dwell (no survivor bounce) was not reproduced this run — a container-timing artifact (SB-22 follow-up)"
    fi
    # GATE-d: exercise the single-shot and TTL gates on the real socket before the destructive commit.
    ARM1=$(_admin_json '{"op":"cluster_force_single_arm","node_id":"brk1","confirm_peers_dead":["brk2"]}')
    TOK1=$(printf '%s' "$ARM1" | jq -r '.force_single.arm_token // empty' 2>/dev/null)
    assert_ok "GATE-d real arm mints a non-empty token after the dwell" sh -c \
        "printf '%s' '$ARM1' | jq -e '.ok==true and (.force_single.arm_token|length)>0' >/dev/null"
    WRONG=$(_admin_json '{"op":"cluster_force_single_commit","node_id":"brk1","arm_token":"wrong-token","confirm_peers_dead":["brk2"]}')
    assert_ok "GATE-d wrong token is refused with exact arm_expired code" sh -c \
        "printf '%s' '$WRONG' | jq -e '.ok!=true and .code==\"arm_expired\"' >/dev/null"
    REPLAY=$(_admin_json "{\"op\":\"cluster_force_single_commit\",\"node_id\":\"brk1\",\"arm_token\":\"$TOK1\",\"confirm_peers_dead\":[\"brk2\"]}")
    assert_ok "GATE-d captured token is stale after a failed consume attempt (single-shot clear)" sh -c \
        "printf '%s' '$REPLAY' | jq -e '.ok!=true and .code==\"arm_expired\"' >/dev/null"
    ARM2=$(_admin_json '{"op":"cluster_force_single_arm","node_id":"brk1","confirm_peers_dead":["brk2"]}')
    TOK2=$(printf '%s' "$ARM2" | jq -r '.force_single.arm_token // empty' 2>/dev/null)
    assert_ok "GATE-d second real arm mints a fresh token" sh -c "[ -n '$TOK2' ] && [ '$TOK2' != '$TOK1' ]"
    sleep 61
    EXPIRED=$(_admin_json "{\"op\":\"cluster_force_single_commit\",\"node_id\":\"brk1\",\"arm_token\":\"$TOK2\",\"confirm_peers_dead\":[\"brk2\"]}")
    assert_ok "GATE-d token older than the 60s TTL is refused with exact arm_expired code" sh -c \
        "printf '%s' '$EXPIRED' | jq -e '.ok!=true and .code==\"arm_expired\"' >/dev/null"

    # POS: the CLI obtains a fresh token after those negatives, then typed-confirms and commits.
    assert_ok "POS commit: online force-single (pty-fed typed confirm) succeeds in-process" sh -c "
      out=\$($SIM exec brk1 -- python3 /opt/sim/pty-confirm.py brk1 -- $FS_ONLINE 2>&1); echo \"\$out\" | grep -qiE 'single-voter cluster|writable WITHOUT a broker restart|force.single'"
    assert_ok "POS PRIMARY oracle: the PREVIOUSLY-REFUSED set-raft-addr now SUCCEEDS (raft writable at quorum-of-1)" \
        poll_until 20 3 "set-raft-addr succeeds" -- _sra_ok
    assert_ok "POS in-process discriminator: brk1 MainPID UNCHANGED across the commit (== $PID0, no restart)" \
        sh -c "[ \"\$($SIM exec brk1 -- systemctl show tether-broker -p MainPID --value | tr -d '\r')\" = '$PID0' ]"
    assert_ok "POS force_single_active banner: cluster status exits 3 / reports force-single" \
        sh -c "$SIM exec brk1 -- sh -c 'tether cluster status --json 2>/dev/null | jq -e \".force_single==true or (.health_label|test(\\\"FORCE.?SINGLE\\\";\\\"i\\\"))\" >/dev/null'"
    # R3: online does NOT de-cluster nats.conf (unlike offline #20) — it stays clustered + prints the remedy.
    assert_ok "POS R3: nats.conf STAYS clustered (online never de-clusters; offline-only #20)" \
        sh -c "$SIM exec brk1 -- grep -qE '^cluster' /etc/tether/nats.d/nats.conf"
    # (YES-online #36 is asserted PRE-kill on the healthy cluster — after the commit brk1 regains leader
    # contact, so --online --yes hits the quorum_not_lost gate, not the --yes/TTY surface.)
    ;;
gotcha)
    # #35: the online force-single dwell is structurally unreachable — brk1 restarted (resetting the
    # in-memory leaderlessSince) and the dwell never satisfied. Pin it by the conjunction.
    assert_bug "#35 online force-single dwell structurally unreachable (brk1 restarted, dwell never satisfied)" "#35" \
        "would NOT proceed|leader contact|not yet quorum-lost|dwell" FS_DRY
    "$SIM" exec brk1 -- sh -c 'journalctl -u tether-broker --no-pager -n 15 2>/dev/null | tail -15' | sed 's/^/[#35 jrnl] /' >&2
    ;;
*)
    _as_fail "Arm-0 INCONCLUSIVE (NR0=$NR0 NR1=$NR1 PID0=$PID0 PID1=$PID1 dwell=$DRY_PROCEEDED) — refusing a silent POSITIVE; investigate"
    ;;
esac

# ── Stage-C M8: the REAL #35 — a survivor RESTART during quorum-loss makes the online force-single dwell
# structurally unreachable. The peer-kill fixture above does NOT bounce brk1 (its nats stays up), so it
# reaches the POSITIVE path — #35's TRUE trigger is an explicit restart (broker.go:948 EJECTED trap → exit 70
# → Restart=always/RestartSec=2 on tether-broker.service install.sh:754 → each ~2s life < 15s dwell +
# newForceSingleArm zeroes leaderlessSince every boot). Dedicated fresh N=2 fixture to EXPOSE it: ──────────
log "#35-restart: rebuilding a fresh N=2 to trigger the survivor-restart form of #35 (peer-kill fixture reaches POSITIVE, not #35)"
assert_setup "#35 fixture reset" "$SIM" nuke
assert_setup "#35 fixture provision fresh N=2 containers" "$SIM" up --brokers 2 --agents 0 --ctl 1
assert_setup "#35 fixture initialize brk1" "$SIM" init brk1
assert_setup "#35 fixture grow brk2" "$SIM" grow brk2
assert_setup "#35 fixture pin brk1 leader (already-leader is success)" _pin_brk1
node_kill brk2
assert_ok "#35 fixture: brk2 is provably dead before survivor restart" poll_until 20 2 "brk2 dead" -- tcp_refused brk2 7400

# Capture a bounded, single journal window. Every exact startup-JS-unavailable line carries systemd's _PID;
# >=2 distinct PIDs proves automatic restarts for the documented cause, not merely one manual restart.
PIDb=$(_pid); SINCE=$(date --iso-8601=seconds)
assert_ok "#35 trigger: manual systemctl restart succeeds" "$SIM" exec brk1 -- systemctl restart tether-broker
PIDm=; _tries=0
while [ $_tries -lt 10 ]; do
    PIDm=$(_pid); [ -n "$PIDm" ] && [ "$PIDm" != 0 ] && [ "$PIDm" != "$PIDb" ] && break
    sleep 1; _tries=$((_tries+1))
done
assert_ok "#35 fixture validity: manual restart changes MainPID ($PIDb→${PIDm:-missing})" \
    sh -c "[ -n '${PIDm:-}' ] && [ '${PIDm:-0}' != 0 ] && [ '${PIDm:-}' != '$PIDb' ]"

DRY_EVER_PROCEEDED=no; _i=0
while [ $_i -lt 12 ]; do
    sleep 3; _i=$((_i+1))
    if _dry_proceeds; then DRY_EVER_PROCEEDED=yes; fi
    log "#35 observation t=$((_i*3))s MainPID=$(_pid) NRestarts=$(_nr) DRY_ever_proceeded=$DRY_EVER_PROCEEDED"
done
NRa=$(_nr)
ERR_PIDS=$("$SIM" exec brk1 -- sh -c "journalctl -u tether-broker --since '$SINCE' -o json --no-pager 2>/dev/null" | \
    jq -r 'select((.MESSAGE // "")|test("cluster mode requires JetStream, but it is UNAVAILABLE at boot.*raft config still lists peer|EJECTED.*recovery rejoin prepare";"i"))|._PID // empty' | sort -u | grep -c . || true)
log "#35 bounded evidence: manual MainPID $PIDb→$PIDm; final NRestarts=$NRa; exact-cause distinct PIDs=$ERR_PIDS; DRY_ever_proceeded=$DRY_EVER_PROCEEDED"
if [ "${NRa:-0}" -ge 2 ] 2>/dev/null && [ "$ERR_PIDS" -ge 2 ] && [ "$DRY_EVER_PROCEEDED" = no ]; then
    product_red "#35 reproduced: ≥2 automatic restarts in a 36s window (NRestarts=$NRa), ≥2 distinct crash PIDs carry the exact startup JetStream-unavailable/EJECTED diagnostic, and EVERY 3s DRY sample stayed non-proceeding"
elif [ "$DRY_EVER_PROCEEDED" = yes ]; then
    _as_pass "#35 candidate NOT reproduced: after a proven manual restart the online dwell proceeded during the complete 36s observation window"
else
    _as_fail "#35 observation was neither a proved exact-cause crash-loop nor a reachable dwell (NRestarts=${NRa:-?}, exact-cause PIDs=$ERR_PIDS, DRY=$DRY_EVER_PROCEEDED)"
fi

drill_end
