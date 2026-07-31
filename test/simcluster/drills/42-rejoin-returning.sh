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
    # R16 A3: the JS-store reset is now the PRODUCT's job (force-single --reset-js moved it aside above). The
    # only step left here is the sim-side SERVICE restart the product tells the operator to run (systemctl is
    # legitimately sim-side per Mandate ③): a FULL nats-server restart to load the standalone conf + the fresh
    # store, then start the broker. NO hand-rolled mv/rm — that concealment is retired.
    "$SIM" exec brk1 -- sh -eu -c '
        trap '\''rc=$?; printf "reset failed rc=%s nats=%s broker=%s js=%s\n" "$rc" "$(systemctl is-active nats-server 2>/dev/null || true)" "$(systemctl is-active tether-broker 2>/dev/null || true)" "$(test -d /var/lib/tether/jetstream && echo present || echo missing)"; tail -n 2 /var/log/tether/nats.err /var/log/tether/broker.err 2>/dev/null || true; exit "$rc"'\'' 0
        systemctl restart nats-server
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
# R16 A3: --reset-js makes force-single MOVE the survivor's clustered JS store aside as a PRODUCT step
# (jetstream.force-single-bak.<epoch>, never deleted) — retiring the drill's old hand-rolled `mv` compensation
# (a Mandate-④ concealment). Without --reset-js the command now REFUSES a non-empty clustered store (loud gate).
assert_ok "OFFLINE force-single --reset-js brk1 → lone N=1 survivor (prunes dead brk2; de-clusters conf AND moves the clustered JS store aside; runs to completion, rc=0)" \
    "$SIM" exec brk1 -- runuser -u tether -- python3 /opt/sim/pty-confirm.py brk1 -- tether cluster recovery force-single --self-id brk1 --self-addr brk1:7400 --confirm-peers-dead brk2 --reset-js
assert_ok "OFFLINE force-single de-clustered nats.conf to standalone (#20 post-condition on the FILE, not a log line)" \
    sh -c "! $SIM exec brk1 -- grep -qE '^cluster[[:space:]]*\\{' /etc/tether/nats.d/nats.conf"
# R16 A3 file-postcondition: the product (not the drill) moved the clustered JS store aside. This is the
# assertion that pins the sim `mv` compensation is GONE — force-single itself reset the store.
assert_ok "OFFLINE force-single --reset-js MOVED the clustered JS store aside itself (jetstream.force-single-bak.* present — the product verb, not the sim's old mv)" \
    sh -c "$SIM exec brk1 -- sh -c 'ls -d /var/lib/tether/jetstream.force-single-bak.* >/dev/null 2>&1'"
assert_ok "OFFLINE force-single: the operator's systemctl restart loads the standalone conf + fresh store; both services return active (systemctl stays sim-side per Mandate ③; the JS reset was the product's job)" \
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
assert_setup "I stage the transfer-audit payload on ctl1" \
    "$SIM" exec ctl1 -- sh -c 'head -c 4096 /dev/urandom > /tmp/pre-resnapshot.bin'
# H8 (2026-07-18 full-suite run): this fixture's push used to run WITHOUT --ack-alerts and was BLOCKED
# (rc=70) by the client-synth severe gate — the force-single above leaves force_single_active active, and
# gateDestructive (cmd/tether/alert_gate.go) covers push/pull/run/expose/session rm. That is the DESIGN,
# not a defect, so the repair is to feed the fixture the override — and to PIN the gate itself rather than
# quietly route around it. The negative runs FIRST and is side-effect-free (the gate returns before any
# transfer starts), so it cannot disturb the audit window the positive below is building.
assert_refuses "I GATE: the same push WITHOUT --ack-alerts is BLOCKED by the force_single_active severe gate (gateDestructive, alert_gate.go — a KEPT design guard, not a defect; this is why the fixture below must pass the override)" \
    "force_single_active|--ack-alerts|BLOCKED: this command needs a healthy broker cluster" \
    "$SIM" ctl -- push /tmp/pre-resnapshot.bin agt1:/tmp/pre-resnapshot-blocked.bin
assert_setup "I create real post-force-single transfer-audit Raft entries (--ack-alerts overrides the force_single_active gate the assertion above pins)" \
    "$SIM" ctl -- push --ack-alerts /tmp/pre-resnapshot.bin agt1:/tmp/pre-resnapshot.bin
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
# ── R9-D: the re-grow is a PRODUCT outcome, not a fixture step ─────────────────────────────────────────
# H8's `--ack-alerts` fix moved this drill's failure point from assertion 27 to 37, which finally let the
# whole rejoin/resnapshot spine (audit window, --accept-audit-loss, no stale-peer resurrection, identity
# manifest) execute — all of it now GREEN. The one thing still failing is HERE, and the pre-R9-D shape made
# it un-attributable in two separate ways:
#   • `assert_setup … "$SIM" grow brk2` captured the output and printed only `tail -3`, so the 2026-07-18
#     run's entire evidence was "want exit 0, got 1" plus three lines of an unrelated JSON dump. The grow
#     therefore runs OUTSIDE the assertion now (the R5-M5 pattern grow_to_3 already uses), with its full
#     output echoed as first-class log lines before anything is judged.
#   • SETUP-RED says "the drill's own fixture broke". A returning node that cannot rejoin is a statement
#     about TETHER — the fixture (force-single, rejoin prepare, init --from-manifest, resnapshot) all
#     succeeded, and every one of those steps is asserted GREEN above. So the outcome is classified: a
#     known defect signature lands PRODUCT-RED against its owner, anything else is an honest ASSERT-FAIL,
#     and the downstream arms are recorded as uncovered instead of being run over a node that never joined.
_F_OUT=$("$SIM" grow brk2 2>&1); _F_GROWRC=$?
printf '%s\n' "$_F_OUT" | sed 's/^/  F-grow| /' >&2
log "42: F re-grow of the returning brk2 → rc=$_F_GROWRC"
wait "$APPROVE_PID"; APPROVE_RC=$?
if [ "$_F_GROWRC" = 0 ]; then
    assert_ok "F [REJOIN TERMINUS] the returning node RE-GROWS into the cluster: 'cluster add' resumed the pre-approved join op and completed the mesh/start orchestration (rc=0). This is the terminus of the whole recovery journey — force-single, rejoin prepare, init --from-manifest, resnapshot are all worthless if the node cannot come back"  sh -c "[ '$_F_GROWRC' = 0 ]"
# CLASSIFY force-single FIRST (R15 fix): #GROW-ONTO-FORCE-SINGLE is the ROOT and CATCHING_UP is its
# DOWNSTREAM symptom — a force-single'd survivor leaves its JetStream clustered-lone-voter 503, so the
# returning joiner crash-loops (broker.go:1063 n1ClusteredJetStreamFatal), never meshes, and therefore
# NEVER becomes reachable+caught-up; the leader's catch-up probe simply never gets a reply and the op
# sits at CATCHING_UP as a side effect. Both signatures land in one grow-status dump, so ordering the
# CATCHING_UP branch first (the old bug) mis-attributed this to #47 by grep-order — the #47 message even
# claimed a "REACHABLE joiner" while the very same dump showed brk2 reachable:false. #47's AddVoter/isVoter
# branch is UPSTREAM-blocked here and is not actually exercised by this fixture until the force-single root
# is fixed.
elif printf '%s' "$_F_OUT" | grep -qiE 'force.single|FORCE-SINGLE|force_single_active'; then
    product_red "#GROW-ONTO-FORCE-SINGLE (owner: R9 product side; the deploy-tier finding this arm exists to surface) — every documented recovery step SUCCEEDED (force-single, rejoin prepare with dump+manifest, init --from-manifest, resnapshot with --accept-audit-loss, brk1 leader again) and yet the re-grow of the returning node FAILED with a force-single signature (rc=$_F_GROWRC): the survivor's JetStream was left clustered-lone-voter (503), so the returning joiner crash-loops on n1ClusteredJetStreamFatal, never meshes, and stalls the op at CATCHING_UP. A survivor that can never be grown back out of force-single means the documented escape hatch is one-way: the cluster stays permanently N=1 with no redundancy. Full 'cluster add' output logged verbatim above"
elif printf '%s' "$_F_OUT" | grep -qiE 'CATCHING_UP|catch_up_stalled|did not reach VOTER'; then
    # Only a GENUINE #47 now: reached here means NO force-single signature is present, i.e. the survivor
    # was healthy and the returning node was admitted + REACHABLE but still never promoted to VOTER.
    product_red "#47 (owner: R9 product side) — the returning brk2 was admitted but never converged to VOTER: 'cluster add' left a REACHABLE joiner in CATCHING_UP (rc=$_F_GROWRC) with NO force-single root present. docs/deploy-tier-gotchas.md:521 — the subsequent grow is then blocked by the serialization lock. R7b bounded the CATCHING_UP state and R9 added the controller-loop tests, but the deploy-tier verifier for the AddVoter/isVoter branch is this drill (R9 risk 2 named it explicitly: that branch is unreachable in a hermetic single-node raft)"
else
    assert_ok "F [REJOIN TERMINUS] the returning node must RE-GROW into the cluster — it did not (rc=$_F_GROWRC), and the failure matches NEITHER the #47 CATCHING_UP signature NOR a force-single one. Undocumented: surfaced rather than laundered into a known defect (full 'cluster add' output logged verbatim above)"  sh -c "false"
fi
if [ "$APPROVE_RC" = 0 ] && grep -qiE 'reached (SERVING|RETIRED)|operation .* reached' "$APPROVE_LOG"; then
    _as_pass "F genuine cluster join approve <bundle> --wait reached SERVING while cluster add orchestrated the returning node"
else
    _as_fail "F join approve --wait did not reach SERVING (rc=$APPROVE_RC)" "$(tail -5 "$APPROVE_LOG" 2>/dev/null)"
fi
if [ "$_F_GROWRC" = 0 ]; then
    assert_ok "F returning brk2 is a reachable VOTER in the authoritative roster" \
        sh -c "$SIM status --json 2>/dev/null | jq -e '.nodes[]?|select(.node_id==\"brk2\" and .phase==\"VOTER\" and .reachable==true)' >/dev/null"
    assert_setup "F seed a real tier-A workload after rejoin" "$SIM" exec ctl1 -- sh -c 'head -c 65536 /dev/urandom > /tmp/rejoin.bin'
    assert_ok "F post-rejoin real workload transfers through the surviving session/agent" "$SIM" ctl -- push /tmp/rejoin.bin agt1:/tmp/rejoin.bin
    assert_ok "F post-rejoin workload bytes are exact on agt1" sh -c \
        "[ \"\$($SIM exec ctl1 -- sha256sum /tmp/rejoin.bin | awk '{print \$1}')\" = \"\$($SIM exec agt1 -- sha256sum /tmp/rejoin.bin | awk '{print \$1}')\" ]"
else
    not_covered "42 F post-rejoin VOTER + tier-A workload round-trip THIS RUN" "the returning node never rejoined (the RED / PRODUCT-RED above). Asserting 'brk2 is a reachable VOTER' or pushing a workload 'after rejoin' over a cluster the node never re-entered would judge a state the drill never reached — and a push that happened to succeed through the surviving N=1 broker would read as a post-rejoin success it is not." gap
fi

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
