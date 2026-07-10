#!/bin/sh
# 10-grow-to-3.sh — grow N=1→2→3 via the real cross-process flow (plan §5.1, GREEN).
# Success = FUNCTIONAL HA: 3 VOTER + all reachable + streams at R=3 + a 3-node clustered JS meta +
# a tier-B push that still works after a follower's nats is killed (quorum 2/3). Env: SIM, HERE, INSTANCE.
#
# NOTE on the health LABEL: after a grow the cluster is functionally HA (asserted here) but
# `cluster status` may report DEGRADED-WRITABLE for a while — the in-broker topology reconciler
# observes "did nats load the new conf?" via /varz config_load_time, and `nats-server --signal reload`
# does NOT advance config_load_time when the rendered conf is byte-identical to what's already loaded,
# so observed_gen can lag desired_gen even though routes/JS are fully converged. We therefore assert
# the FUNCTIONAL invariants (voters/reachable/replicas/meta/transfer), not health==HEALTHY_HA.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
SIM="${SIM:-$HERE/simcluster}"
PIN=${SIMPIN:-135790}; SID=lab
S() { "$SIM" exec brk1 -- tether cluster status --json 2>/dev/null; }   # leader status JSON

drill_begin "10-grow-to-3 (N=1→2→3 functional HA + follower-kill proof)"

"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 3 brokers + 1 agent + 1 ctl"      "$SIM" up --brokers 3 --agents 1 --ctl 1
assert_ok "init brk1 (N=1)"                      "$SIM" init brk1
# grow N=1→2→3. Each grow internally polls up to 150s for the joiner to reach VOTER.
# ── DO-NOT-RE-INVESTIGATE (established 2026-07-10) ────────────────────────────────────────────────
# If `grow brk3` REDs with "brk3 reaches VOTER" TIMED OUT during a FULL-PARALLEL sweep (run-drills.sh
# with no -j cap), it is the grow-timing CONCURRENCY flake, NOT a G1-G7 / product defect. This is the
# HEAVIEST grow drill (3 brokers + two grows + follower-kill); clustered-JS meta-group formation +
# raft VOTER promotion is timing-sensitive, so at 7-way concurrency it can miss the 150s window while
# every OTHER drill stays green. PROVEN flake, not bug: SOLO run = 19/19 GREEN, 30s-staggered = GREEN;
# only the 7-way no-stagger sweep RED'd here. (The earlier "5/7 RED at up" was a SEPARATE root cause —
# fs.inotify.max_user_instances=128; fixed via sysctl=8192. This VOTER-timeout is the leftover, distinct
# grow-concurrency item.) Confirm by re-running SOLO or with -j. See simcluster cmd_grow's VOTER-timeout
# branch + README "CAVEAT (grow-concurrency)".
assert_ok "grow brk2 (N=2)"                      "$SIM" grow brk2
_gf=$_AS_FAIL
assert_ok "grow brk3 (N=3)"                      "$SIM" grow brk3
if [ "$_AS_FAIL" -gt "$_gf" ]; then
    warn "grow brk3 RED — if this was a PARALLEL run, it is the grow-timing flake documented above,"
    warn "  NOT a defect. Re-run this drill SOLO ('simcluster drill 10-grow-to-3'); it should be GREEN (19/19)."
fi

# F1 (external review): every voter's tether-broker must be ACTIVE after grow — the former-N1 nats reset
# can trip #23 and strand the leader; this catches that regression regardless of the grow verb's recovery.
assert_ok "every voter's tether-broker active after grow (#23 guard)" \
    sh -c "for b in brk1 brk2 brk3; do [ \"\$($SIM exec \$b -- systemctl is-active tether-broker 2>/dev/null)\" = active ] || { echo \"\$b tether-broker not active\"; exit 1; }; done"

# m6: reachable/stream_actual/stream_target are LEADER-ONLY-populated (plan §4) → read them from the
# resolved leader, not a hardcoded brk1 (which may be a follower after a SIGHUP-reload leadership move).
LDR=$("$SIM" exec brk1 -- tether cluster status --json 2>/dev/null | jq -r '.leader_id // "brk1"')
assert_ok "3 VOTERs"          sh -c "[ \"\$($SIM exec $LDR -- tether cluster status --json 2>/dev/null | jq '[.nodes[]?|select(.phase==\"VOTER\")]|length')\" = 3 ]"
assert_ok "all 3 reachable"   sh -c "[ \"\$($SIM exec $LDR -- tether cluster status --json 2>/dev/null | jq '[.nodes[]?|select(.reachable==true)]|length')\" = 3 ]"
assert_ok "streams at R=3 (stream_actual==stream_target==3)" \
    sh -c "$SIM exec $LDR -- tether cluster status --json 2>/dev/null | jq -e '[.nodes[]?|select(.stream_actual==3 and .stream_target==3)]|length==3' >/dev/null"
# clustered JS meta corroboration (the R=3 stream assertion above already PROVES a 3-node meta — you
# cannot replicate a stream to R=3 without one; this /jsz check is a lenient cross-check, tolerant of the
# leader's own replicas-list shape which varies by which node answers).
assert_ok "clustered JS meta present (≥3-node)"  sh -c "for i in \$(seq 1 6); do $SIM exec $LDR -- sh -c 'curl -s \"localhost:8223/jsz?meta=1\"' 2>/dev/null | jq -e '.meta_cluster!=null and (.meta_cluster.cluster_size>=3 or (.meta_cluster.replicas|length>=2))' >/dev/null && exit 0; sleep 3; done; exit 1"

# M8 (OQ-6 cheap hollow-voter parity — the fidelity crux, minus the deferred InstallSnapshot-forcing arm):
# a fresh joiner replaying a migrated leader's log is the FK-panic / hollow-voter bug class (32b28e9). The
# checks above are all TRUE even with a hollow cluster_nodes, so add the row-count parity + a joiner
# journal grep so a reintroduced hollow-voter/FK-panic regression can't pass GREEN.
assert_ok "joiner brk3 roster NOT hollow (row count == leader's)" \
    sh -c "[ \"\$($SIM exec brk3 -- tether cluster status --json 2>/dev/null | jq '.nodes|length')\" = \"\$($SIM exec $LDR -- tether cluster status --json 2>/dev/null | jq '.nodes|length')\" ]"
assert_ok "no FK-panic / hollow-voter in joiner brk3 tether-broker journal" \
    sh -c "! $SIM exec brk3 -- journalctl -u tether-broker --no-pager 2>/dev/null | grep -qiE 'panic|FOREIGN KEY|unrecognized raft role'"

# F4 / round-2 R1 / round-3 (external re-review): the `status --json` machine surface must proxy the
# AUTHORITATIVE LEADER. Test it survives a leadership change — but do NOT assume brk1 is the leader (grow/
# SIGHUP-reload can move it, and `transfer-leader` run on a NON-leader fails, which would false-red this
# GREEN drill in a legal state). Resolve the CURRENT leader, transfer to a DIFFERENT broker, then require
# status --json to report that new leader with is_leader_view=true (the old first-broker-wins code would
# return the now-follower brk1's non-authoritative view).
CUR=$("$SIM" status --json 2>/dev/null | jq -r '.leader_id // empty'); [ -n "$CUR" ] || CUR=brk1
TGT=""; for b in brk1 brk2 brk3; do [ "$b" != "$CUR" ] && TGT=$b && break; done
assert_ok "transfer leadership $CUR→$TGT (--wait, run on the current leader)" \
    "$SIM" exec "$CUR" -- tether cluster transfer-leader "$TGT" --wait
assert_ok "status --json is the AUTHORITATIVE leader view after the move (is_leader_view=true, leader=$TGT)" \
    sh -c "for i in \$(seq 1 8); do [ \"\$($SIM status --json 2>/dev/null | jq -r 'select(.is_leader_view==true).leader_id' 2>/dev/null)\" = $TGT ] && exit 0; sleep 3; done; exit 1"
LDR=$TGT   # leadership moved — the follower-kill below picks a follower relative to the NEW leader

# --- follower-LOSS HA proof (M9): kill a NON-leader follower's WHOLE CONTAINER (≠ the ctl's brk1). This
# drops its raft node → real quorum 2/3. (Stopping only nats-server leaves the raft node on :7400 → 3/3,
# a non-falsifiable proof.) Prove the survivors keep a leader + serve the control plane.
assert_ok "session + ctl login"     "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1"         "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
F=""; for b in brk2 brk3; do [ "$b" != "$LDR" ] && F=$b && break; done
log "HA proof: leader=$LDR, killing follower CONTAINER=$F"
assert_ok "docker-kill follower $F (raft node drops → quorum 2/3)"  sh -c "docker kill sim-\${INSTANCE}-$F >/dev/null"
# generous poll windows: after a raft node drops the survivors re-confirm leadership + the JS/roster
# re-stabilizes over several seconds before node ls serves again.
assert_ok "survivors still elect a leader at quorum 2/3"  sh -c "for i in \$(seq 1 12); do $SIM exec brk1 -- tether cluster status --json 2>/dev/null | jq -e '.leader_id!=null' >/dev/null && exit 0; sleep 3; done; exit 1"
assert_ok "control plane alive: node ls still lists agt1"  sh -c "for i in \$(seq 1 12); do $SIM ctl -- node ls 2>/dev/null | grep -q agt1 && exit 0; sleep 3; done; exit 1"
# F2 (external review): a WRITE must still COMMIT at quorum 2/3, not just reads. `session create` is a
# raft-replicated write (propose → commit); its success proves write-forwarding/propose still commits with
# one voter gone — the read checks above would pass even if writes were wedged.
assert_ok "control-plane WRITE commits at quorum 2/3 (session create via raft)" \
    sh -c "for i in \$(seq 1 10); do $SIM ctl -- session create ha-write-proof --pin $PIN >/dev/null 2>&1 && exit 0; sleep 3; done; exit 1"

drill_end
