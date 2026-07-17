#!/bin/sh
# 42-rejoin-returning.sh — S6 (N=2 force-single fixture, GREEN + DOC-2): the returning-node recovery
# surface — offline `recovery diagnose` (dead-peer pasteable cmd vs alive-peer refusal, the pos/neg pair
# that proves the probe is real, not "all-probes-fail"), `rejoin prepare` (dump 0600 + O_EXCL WIPE-REFUSED
# on an intact DB), `resnapshot` (SINGLE-VOTER-only refusal), and the Tier-2 / machine-confirm surface.
# recovery diagnose/resnapshot are absent from cluster.md/runbook → DOC-2 (confirmed here, register only).
#
# machineEscapable (source-verified): resnapshot=true, init=true; rejoin-prepare(recover)=false
# (never-escapable), restore=false. So the machine-confirm-missing-env negative is ONLY meaningful for
# resnapshot; rejoin-prepare refuses for the TTY reason (no env escape).
#
# TRANSPORTS: recovery/diagnose/resnapshot/rejoin = OFFLINE local tools (daemon stopped) run as the
# data-dir owner (tether) or root; pty-confirm.py feeds the typed confirm. FALSE-GREEN GUARDS (plan §9-42):
# diagnose covers BOTH real outcomes (dead→pasteable+exit0; alive→non-zero+"peer reachable"); the O_EXCL
# sub-arm runs on an INTACT DB (asserts raft/db STILL present = wipe refused); resnapshot single-voter
# refusal needs daemon-stopped + roster still {brk1,brk2}.
set -u
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/drills/lib/setup-forcesingle.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab
# offline recovery tool on a node, as the data-dir owner (tether), pty-fed if it needs a typed confirm.
REC()     { dexec -u tether "$1" -- sh -c "$2"; }               # $1=node $2=command string
REC_PTY() { dexec -u tether "$1" -- sh -c "python3 /opt/sim/pty-confirm.py $2"; }  # $2="<confirm> -- tether ..."
_roster_digest() { "$SIM" status --json 2>/dev/null | jq -c '{leader_id,nodes:([.nodes[]?|{node_id,phase}]|sort_by(.node_id))}'; }
_dead_diag() {
    _dd_out=$(dexec -u tether brk1 -- tether cluster recovery diagnose --self-id brk1 2>&1); _dd_rc=$?
    [ "$_dd_rc" = 0 ] && printf '%s' "$_dd_out" | grep -qiE 'force-single .*--confirm-peers-dead brk2|All peers are dead'
}
_rejoin_diag() {
    # install.sh routes StandardError to this durable service log; journalctl intentionally has no
    # broker payload. Reading the journal made the original namesake arm a deterministic false failure.
    _rj=$($SIM exec brk2 -- tail -n 1600 /var/log/tether/broker.err 2>/dev/null) || return 1
    printf '%s' "$_rj" | grep -qi 'recovery rejoin prepare' && printf '%s' "$_rj" | grep -qiE 'EJECTED|raft config still lists peer'
}
_reset_after_force_single() {
    # Keep the reset strict, but leave enough evidence to distinguish a missing
    # JS store from a NATS/broker startup failure. assert_ok otherwise prints only
    # the command's final three lines and the remote shell used to be silent.
    "$SIM" exec brk1 -- sh -eu -c '
        trap '\''rc=$?; printf "reset failed rc=%s nats=%s broker=%s js=%s\n" "$rc" "$(systemctl is-active nats-server 2>/dev/null || true)" "$(systemctl is-active tether-broker 2>/dev/null || true)" "$(test -d /var/lib/tether/jetstream && echo present || echo missing)"; tail -n 2 /var/log/tether/nats.err /var/log/tether/broker.err 2>/dev/null || true; exit "$rc"'\'' 0
        systemctl stop nats-server
        test -d /var/lib/tether/jetstream
        mv /var/lib/tether/jetstream /var/lib/tether/jetstream.bak.$(date +%s)
        systemctl start nats-server
        systemctl start tether-broker
        test "$(systemctl is-active nats-server)" = active
        test "$(systemctl is-active tether-broker)" = active
        trap - 0
    '
}

drill_begin "S6-42 rejoin-returning: diagnose pos/neg + rejoin-prepare O_EXCL + resnapshot single-voter + Tier-2 (N=2)"

# ── SETUP: healthy N=2 clustered-JS (fixture asserts JS cluster_size==2 + tier-B baseline before any kill) ─
setup_forcesingle_n2

# ── B+C diagnose: ALIVE-peer refusal (brk1 is the running N=2 leader, brk2 alive) — the real gate ────
# Run diagnose --self-id brk2 while brk1 is alive → probes brk1 ALIVE → refuse (peer still reachable).
# This is the pos/neg anti-"all-probes-fail" evidence: a live peer is correctly detected and blocks.
log "DIAG recovery diagnose --self-id brk2 (brk1 alive) →"
dexec -u tether brk2 -- sh -c "tether cluster recovery diagnose --self-id brk2 2>&1" | sed 's/^/[diag] /' | head -8
assert_refuses "C diagnose: ALIVE peer detected → force-single blocked (a peer is still reachable)" \
    "a peer is still reachable|still ALIVE|SPLIT THE BRAIN|force-single blocked" \
    dexec -u tether brk2 -- sh -c "tether cluster recovery diagnose --self-id brk2 2>&1"
# zero-mutation on the read-only diagnose path: compare the same leader+roster projection byte-for-byte.
ROSTER_ALIVE=$(_roster_digest) || setup_fail "cannot capture pre-diagnose N=2 roster"
assert_ok "C diagnose is READ-ONLY: exact leader+roster projection unchanged" \
    sh -c "[ \"\$($SIM status --json 2>/dev/null | jq -c '{leader_id,nodes:([.nodes[]?|{node_id,phase}]|sort_by(.node_id))}')\" = '$ROSTER_ALIVE' ]"

# ── Kill brk2, then (Stage-C M6) diagnose dead-peer POSITIVE while brk1's roster STILL lists brk2 ────
node_kill brk2   # setup action; DEATH is proven by the hard assert below (not by the kill's exit code)
assert_ok "brk2 provably dead (:7400 connection-refused)" \
    poll_until 20 2 "brk2 :7400 refused" -- tcp_refused brk2 7400
# Stage-C M6: a REAL dead-peer diagnose (roster still {brk1,brk2}, brk2 dead) prints the pasteable
# `force-single … --confirm-peers-dead brk2` (cluster_offline_wizard.go:47-48) + exit 0, executes nothing —
# the missing POSITIVE half of the pos/neg pair (the alive-peer REFUSE ran above at arm C). NOT the
# force-single's own output (that never calls forceSingleGuided — the earlier "exercised inline" claim was false).
ROSTER_DEAD=$(_roster_digest) || setup_fail "cannot capture pre-dead-diagnose roster"
assert_ok "B diagnose (dead-peer POSITIVE): command exits 0 and prints pasteable force-single command" _dead_diag
assert_ok "B diagnose is READ-ONLY: exact leader+roster projection unchanged" \
    sh -c "[ \"\$($SIM status --json 2>/dev/null | jq -c '{leader_id,nodes:([.nodes[]?|{node_id,phase}]|sort_by(.node_id))}')\" = '$ROSTER_DEAD' ]"

# H must run before force-single prunes brk2: daemon stopped while on-disk roster is still N=2.
assert_setup "H stop brk1 daemon while its on-disk roster still contains brk2" "$SIM" exec brk1 -- systemctl stop tether-broker
assert_refuses "H resnapshot refuses unless the on-disk roster is SINGLE-VOTER" \
    "SINGLE-VOTER only|roster has .*non-self|non-self node|more than one voter" \
    dexec -u tether brk1 -- sh -c "TETHER_CONFIRM_NODE_ID=brk1 tether cluster recovery resnapshot --self-id brk1 --raft-addr brk1:7400 --confirm-node-id brk1 2>&1"

# ── Now the OFFLINE force-single (prune brk2) → lone N=1 survivor (drill 20 pattern) ─────────────────
# Round-5 §M1: this piped force-single into `grep -qiE '…|single-voter|force.single'` — the SAME truncation
# that made drill 91 brick its survivor and blame the product. grep -q exits at ForceSingle's INTERNAL
# "…single-voter cluster" log line (emitted BEFORE the CLI's nats.conf de-cluster) and SIGPIPEs tether.
# Run to completion (rc-checked), then assert the REAL #20 post-condition on the file.
assert_ok "OFFLINE force-single brk1 → lone N=1 survivor (prunes dead brk2; runs to completion, rc=0)" \
    "$SIM" exec brk1 -- runuser -u tether -- python3 /opt/sim/pty-confirm.py brk1 -- tether cluster recovery force-single --self-id brk1 --self-addr brk1:7400 --confirm-peers-dead brk2
assert_ok "OFFLINE force-single de-clustered nats.conf to standalone (#20 post-condition on the FILE, not a log line)" \
    sh -c "! $SIM exec brk1 -- grep -qE '^cluster[[:space:]]*\\{' /etc/tether/nats.d/nats.conf"
assert_ok "OFFLINE force-single operator reset succeeds and both services return active" \
    _reset_after_force_single
sleep 5

# ── Stage-C M5: the NAMESAKE Arm A — cold-start the abandoned brk2 → ACTIONABLE rejoin diagnostic ────
# brk2's OWN disk still lists {brk1,brk2}; on boot it sees voters>=2 + peer-in-config but no quorum → emits
# the ranked EJECTED-vs-transient rejoin diagnostic (broker.go:941-958). The FIX is the MESSAGE, not liveness
# (brk2 still exits 70 + Restart=always bounces — the diagnostic prints once per boot).
node_start brk2
# brk2 crash-loops (exit 70 + Restart=always), so the diagnostic prints once per boot into the durable
# StandardError file. Poll that real sink rather than systemd's empty journal payload.
if poll_until 90 4 "brk2 exact rejoin diagnostic in journal" -- _rejoin_diag; then
    _as_pass "A Arm-A (namesake): abandoned brk2 cold-start emits the ACTIONABLE rejoin diagnostic (recovery rejoin prepare / EJECTED / raft config still lists peer) in its journal"
else
    # The arm is PRESENT (the drill DID cold-start the abandoned brk2 + scan its journal), but the catch did
    # not land this run — either brk2 never emits it (a real gap) OR the crash-loop rolled it past the tail
    # (timing). Not proven → an honest INCOMPLETE gap, NEVER a silent GREEN.
    _as_fail "A Arm-A namesake regression: abandoned brk2 did not emit BOTH 'recovery rejoin prepare' and the EJECTED/raft-config explanation within 90s"
fi
# stop brk2 again so its stale process cannot race the offline wipe.
"$SIM" exec brk2 -- systemctl stop tether-broker >/dev/null 2>&1 || true

# ── D rejoin prepare — O_EXCL refusal then a real manifest-producing wipe on RETURNING brk2 ─────────
# The negative and positive act on the same stale returning node.  A pre-existing forensic pathname must
# preserve its intact DB/raft; removing only that pathname then allows the durable 0600 dump+manifest and
# wipes the divergent timeline.
assert_setup "D pre-create brk2 forensic dump path as the data-dir owner (force O_EXCL)" \
    dexec -u tether brk2 -- touch /var/lib/tether/div-brk2.json
# O_EXCL WIPE-REFUSED is a KEPT safety guard (correct behavior) → assert_refuses, not assert_bug.
assert_refuses "D O_EXCL: rejoin prepare with a pre-existing dump on an INTACT DB → WIPE REFUSED (safety)" \
    "WIPE REFUSED|file exists" \
    dexec -u tether brk2 -- sh -c "python3 /opt/sim/pty-confirm.py brk2 -- tether cluster recovery rejoin prepare --self-id brk2 --dump-divergent /var/lib/tether/div-brk2.json --emit-manifest /var/lib/tether/rejoin-brk2.json --secrets-dir /etc/tether/secrets 2>&1"
assert_ok "D O_EXCL: raft/ and tether.db STILL present after the refused wipe (data intact)" \
    "$SIM" exec brk2 -- sh -c 'test -f /var/lib/tether/tether.db && test -d /var/lib/tether/raft'
assert_setup "D remove only the colliding empty forensic pathname" dexec -u tether brk2 -- rm /var/lib/tether/div-brk2.json
assert_ok "D positive: returning brk2 emits forensic dump + identity manifest, then wipes old timeline" \
    dexec -u tether brk2 -- sh -c "python3 /opt/sim/pty-confirm.py brk2 -- tether cluster recovery rejoin prepare --self-id brk2 --dump-divergent /var/lib/tether/div-brk2.json --emit-manifest /var/lib/tether/rejoin-brk2.json --secrets-dir /etc/tether/secrets 2>&1"
assert_ok "D dump+manifest are non-empty 0600 and the divergent DB/raft are gone" \
    "$SIM" exec brk2 -- sh -eu -c 'test -s /var/lib/tether/div-brk2.json; test -s /var/lib/tether/rejoin-brk2.json; test "$(stat -c %a /var/lib/tether/div-brk2.json)" = 600; test "$(stat -c %a /var/lib/tether/rejoin-brk2.json)" = 600; test ! -e /var/lib/tether/tether.db; test ! -e /var/lib/tether/raft'
assert_ok "D manifest carries brk2's complete identity, cluster mode, and no business-state replay" \
    sh -c "$SIM exec brk2 -- jq -e '.kind==\"recover-divergent\" and .mode==\"cluster\" and .self_id==\"brk2\" and (.node_ident_pub|length)>0 and .raft_addr==\"brk2:7400\" and .nats_route==\"nats://brk2:6222\"' /var/lib/tether/rejoin-brk2.json >/dev/null"

# ── E consume the identity-only manifest; the local cert pin is re-derived from live secrets ─────────
assert_ok "E init --from-manifest creates a fresh single-voter identity for brk2" \
    dexec -u tether brk2 -- sh -c "TETHER_CONFIRM_NODE_ID=brk2 tether cluster init --from-manifest /var/lib/tether/rejoin-brk2.json --secrets-dir /etc/tether/secrets --confirm-node-id brk2 2>&1"
assert_ok "E fresh DB/raft contain exactly the manifest self identity (not the old two-node roster)" \
    "$SIM" exec brk2 -- sh -eu -c 'test -f /var/lib/tether/tether.db; test -d /var/lib/tether/raft; test "$(sqlite3 /var/lib/tether/tether.db "SELECT group_concat(node_id, '\''|'\'') FROM cluster_nodes")" = brk2'

# ── G+I resnapshot audit-window guard and explicit bounded-loss override on lone brk1 ───────────────
# A real transfer commits OpTransferAudit entries after force-single's recovery snapshot. Merely setting
# audit_published_index below raw LastIndex is NOT sufficient: config/noop/checkpoint records carry no
# re-derivable audit, and treating those as a loss window would manufacture a false RED.
assert_setup "I create real post-force-single transfer-audit Raft entries" \
    sh -c "$SIM exec ctl1 -- sh -c 'head -c 4096 /dev/urandom > /tmp/pre-resnapshot.bin' && $SIM ctl -- push /tmp/pre-resnapshot.bin agt1:/tmp/pre-resnapshot.bin"
assert_setup "G stop lone brk1 before offline resnapshot" "$SIM" exec brk1 -- systemctl stop tether-broker
# Fault injection is limited to the documented publisher cursor. The preceding real transfer proves the
# scanned tail contains audit-bearing OpTransferAudit records; this is not a raw-index surrogate.
assert_setup "I inject a stale audit_published_index=0 in the offline FSM DB" \
    "$SIM" exec brk1 -- sh -eu -c 'sqlite3 /var/lib/tether/tether.db "UPDATE cluster_meta SET value='\''0'\'' WHERE key='\''audit_published_index'\'';"; test "$(sqlite3 /var/lib/tether/tether.db "SELECT value FROM cluster_meta WHERE key='\''audit_published_index'\''")" = 0'
assert_refuses "I resnapshot without --accept-audit-loss refuses the real unpublished audit window" \
    "UNPUBLISHED audit|accept-audit-loss|audit_published_index" \
    dexec -u tether brk1 -- sh -c "TETHER_CONFIRM_NODE_ID=brk1 tether cluster recovery resnapshot --self-id brk1 --raft-addr brk1:7400 --confirm-node-id brk1 2>&1"
assert_ok "G+I explicit --accept-audit-loss performs the single-voter resnapshot and becomes grow-ready" \
    dexec -u tether brk1 -- sh -c "TETHER_CONFIRM_NODE_ID=brk1 tether cluster recovery resnapshot --self-id brk1 --raft-addr brk1:7400 --confirm-node-id brk1 --accept-audit-loss 2>&1"
# RecoverCluster replays the local tail before snapshotting. The preflight must therefore reason about the
# recovered FSM, not only the pre-replay SQLite projection: an old brk2 admission must never be resurrected
# into cluster_nodes while the Raft configuration is rewritten to {brk1}.
assert_ok "G resnapshot does not resurrect a stale non-self peer from the Raft tail" \
    "$SIM" exec brk1 -- sh -eu -c 'test "$(sqlite3 /var/lib/tether/tether.db "SELECT group_concat(node_id, '\''|'\'') FROM cluster_nodes")" = brk1'
assert_setup "G restart resnapshotted brk1 and recover a writable leader" "$SIM" exec brk1 -- systemctl start tether-broker
assert_setup "G brk1 is leader again" poll_until 60 3 "brk1 leader after resnapshot" -- sh -c "$SIM status --json 2>/dev/null | jq -e '.leader_id==\"brk1\" and .is_leader_view==true' >/dev/null"

# ── F real join approve --wait in parallel with cluster-add's mesh/start orchestration ──────────────
BUNDLE=$(dexec -u tether brk2 -- tether cluster join prepare --node-id brk2 --name brk2 --raft-addr brk2:7400 --nats-route nats://brk2:6222 --tunnel-addr brk2:7000 --public-host brk2 --secrets-dir /etc/tether/secrets 2>/dev/null | head -1) \
    || setup_fail "F join prepare failed"
printf '%s' "$BUNDLE" | grep -q '^tether-join:v1:' || setup_fail "F join prepare did not emit a bundle"
APPROVE_LOG="/tmp/s6s8-42-approve-$$.log"
dexec -u tether brk1 -- tether cluster join approve "$BUNDLE" --wait --timeout 5m >"$APPROVE_LOG" 2>&1 &
APPROVE_PID=$!
sleep 3
assert_setup "F cluster add resumes the pre-approved returning-node op and performs mesh/start orchestration" "$SIM" grow brk2
wait "$APPROVE_PID"; APPROVE_RC=$?
if [ "$APPROVE_RC" = 0 ] && grep -qiE 'reached (SERVING|RETIRED)|operation .* reached' "$APPROVE_LOG"; then
    _as_pass "F genuine cluster join approve <bundle> --wait reached SERVING while cluster add orchestrated the returning node"
else
    _as_fail "F join approve --wait did not reach SERVING (rc=$APPROVE_RC)" "$(tail -5 "$APPROVE_LOG" 2>/dev/null)"
fi
assert_ok "F returning brk2 is a reachable VOTER in the authoritative roster" \
    sh -c "$SIM status --json 2>/dev/null | jq -e '.nodes[]?|select(.node_id==\"brk2\" and .phase==\"VOTER\" and .reachable==true)' >/dev/null"
assert_setup "F seed a real tier-A workload after rejoin" "$SIM" exec ctl1 -- sh -c 'head -c 65536 /dev/urandom > /tmp/rejoin.bin'
assert_ok "F post-rejoin real workload transfers through the surviving session/agent" "$SIM" ctl -- push /tmp/rejoin.bin agt1:/tmp/rejoin.bin
assert_ok "F post-rejoin workload bytes are exact on agt1" sh -c \
    "[ \"\$($SIM exec ctl1 -- sha256sum /tmp/rejoin.bin | awk '{print \$1}')\" = \"\$($SIM exec agt1 -- sha256sum /tmp/rejoin.bin | awk '{print \$1}')\" ]"

# ── J Tier-2 --yes negatives (rejoin-prepare + resnapshot; both quorum-affecting, no --yes override) ─
assert_refuses "J rejoin prepare --yes rejected (Tier-2, never-escapable)" \
    "NO --yes override|cannot run unattended" \
    dexec brk1 -- sh -c "tether cluster recovery rejoin prepare --self-id brk1 --dump-divergent /root/d.json --yes 2>&1"
assert_refuses "J resnapshot --yes rejected (Tier-2)" \
    "NO --yes override|cannot run unattended" \
    dexec brk1 -- sh -c "tether cluster recovery resnapshot --self-id brk1 --raft-addr brk1:7400 --yes 2>&1"

# ── K machine-confirm: resnapshot=machineEscapable=true → missing env refused; rejoin-prepare=false (TTY) ─
assert_refuses "K resnapshot machine-confirm missing env → refused" \
    "type the|confirm-node-id|TTY|interactive terminal|aborted" \
    dexec brk1 -- sh -c "TETHER_CONFIRM_NODE_ID= tether cluster recovery resnapshot --self-id brk1 --raft-addr brk1:7400 --confirm-node-id brk1 2>&1"
# CONTRAST: rejoin-prepare is never-escapable → it has NO --confirm-node-id flag at all (unlike resnapshot,
# which is machineEscapable=true and DOES take it). Passing it → "unknown flag" = the never-escapable proof.
assert_refuses "K CONTRAST: rejoin-prepare has NO --confirm-node-id flag (never-escapable, unlike resnapshot)" \
    "unknown flag.*confirm-node-id|interactive terminal|TTY|cannot run unattended" \
    dexec brk1 -- sh -c "TETHER_CONFIRM_NODE_ID=brk1 tether cluster recovery rejoin prepare --self-id brk1 --dump-divergent /root/d2.json --confirm-node-id brk1 2>&1"

drill_end
