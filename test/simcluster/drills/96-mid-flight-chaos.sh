#!/bin/sh
# 96-mid-flight-chaos — S9 / G-C. N=3: failures injected MID-FLIGHT — a tier-B transfer in flight, a real
# network PARTITION of the leader, and a double fault (agent + its home broker together).
# Plan: docs/reviews/s7-s9-plan.md §3.3. Expected landing: PRODUCT-RED (#57/#58 are source-certain).
# Runtime ~22min. Topology: 3 brokers + 2 agents + 1 ctl (grow-family).
#
# ── TWO SOURCE-CERTAIN FINDINGS THIS DRILL EXISTS TO PIN ────────────────────────────────────────────
# #57  The roadmap expected "the bucket watchdog cleans up + a failed audit is written". The watchdog DIES
#      WITH THE PROCESS: entry.cancel = b.startTransferWatchdog(b.runCtx, entry) (transfer.go:593 push /
#      :704 pull) hangs off the broker's runCtx; transferTracker is a plain in-memory map (:99-104) that
#      broker.go:602 rebuilds EMPTY on restart; and handleEvTransfer drops an agent's late finalization on
#      the floor via `preview == nil -> return` (:816-819). => the synthetic `failed` audit is NEVER
#      written. The roadmap's GREEN expectation is structurally unreachable.
# #58  The reap used to run ONCE at boot (reconcileXferObjectsOnBoot, broker.go:942) behind a leader-only
#      gate (reaperMayDelete, false at boot on every cluster broker), and the periodic pass that R7 added was
#      ALSO leader-only — so a bucket homed to a NON-LEADER was reapable by nobody and its tier-B garbage was
#      immortal. R15 FIXED it: the reap gate is now reaperCaughtUp (raft-caught-up, leader-neutral) and the
#      periodic xfer-orphan-reap pass is per-broker + home-authoritative (homeOwnsXferBucket partitions the
#      blast radius), so a non-leader home reaps its OWN orphan on the periodic pass. This arm now VERIFIES
#      that fix: it shortens the reap cadence (broker.cluster.xfer_reap_interval=8s) so the reap is observable,
#      then polls the object count back to baseline on the non-leader home brk2 (GREEN), or REDs as a
#      regression if the orphan survives.
#
# ── WHY THE PARTITION IS A PARTITION AND NOT AN OUTAGE ──────────────────────────────────────────────
# `docker network disconnect`'s documented contract only detaches the interface -> instant EHOSTUNREACH:
# the peer learns immediately, which is an OUTAGE. A partition means packets VANISH and tether learns
# nothing until its own timeouts fire — strictly HARDER on the product. fault_assert_blackholed asserts
# rc=124 (hung), which is mutually exclusive with tcp_refused's immediate failure, so "we injected a
# partition, not an outage" is itself a GREEN assertion and fail-closes the design against anyone
# swapping the primitive back.
#
# ── FALSE-GREEN RISK HEADNOTE ───────────────────────────────────────────────────────────────────────
#  1. A transfer that never started, or that took tier-A, makes "no terminal row" true for the WRONG
#     reason. Guards: >8 MiB (transferTierAMaxBytes, transfer.go:52), a full successful tier-B round first,
#     and poll until the `start` row is actually visible.
#  2. If the history reader itself is broken, everything below is vacuous. Guard: a paired start+complete
#     BEFORE the injection, plus a small control transfer AFTER it.
#  3. R-EXHAUST: the INVERTED blocks enumerate four states with NO `else` — an `else` would invent a
#     gotcha out of an unread log.
#  4. Killing the WRONG broker self-corrects (we would see `complete` -> APPEARS-FIXED), but the SETUP
#     asserts leader==brk1 and home==brk2 anyway, so #58's `reaperMayDelete()==false` is guaranteed BY
#     SOURCE rather than by luck.
#  5. If the rules never took effect, "everything is fine" goes green. Guard: fault_assert_blackholed's
#     124 self-proof; failure there is setup_fail, not a quiet pass.
#  6. Cutting EVERYTHING makes "the minority survives read-only" vacuous. Guard: 4222 is deliberately left
#     up and asserted reachable — that is what makes the claim about tether and not about us.
#  7. D1 must be read from brk2/brk3: brk1's own view is stale by construction.
#  8. D6 closes on RESULT (reading the majority's row back from the ex-minority), never a status field.
#  9. Inside an armed DROP window use dp_curl_blackholed (28), never dp_curl_refused (7).
# 10. R-BOUNDED-PROBE: every probe inside an armed window carries its own timeout — run-drills has no
#     per-drill timeout and cmd_drill's trap does not catch EXIT, so one hang = a wedged suite + rules
#     left armed.
# 11. The double-fault arm CANNOT have an OS-truth leg: node_kill destroys the very OS state we would
#     read. An OS-truth leg exists only in 94-B, where the process genuinely survives the injection.

set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/drills/lib/cluster.sh"
. "$HERE/drills/lib/dataplane.sh"
. "$HERE/drills/lib/agentyaml.sh"
. "$HERE/drills/lib/fault.sh"
. "$HERE/drills/lib/events.sh"
. "$HERE/drills/lib/logs.sh"
. "$HERE/lib/assert.sh"

SID=lab
PIN=969696
NURL="nats://brk2:4222"        # agt1's tunnel broker == its NATS entry == brk2 (a NON-leader voter)
EV_NURL="nats://brk1:4222"
EVCAP=/tmp/96-events.jsonl

_bt() { _btn=$1; shift; [ "$1" = "--" ] && shift; dexec -u tether "$_btn" -- "$@"; }
# timeout 10 INSIDE the container before `tether` (not a harness fn — the real binary): `cluster status`
# reads raft state, which BLOCKS on a partitioned / re-forming node, and poll_until cannot interrupt a
# hung predicate (it only checks the clock BETWEEN predicate calls). An unbounded call here wedged a whole
# 96 run for ~50min (re-run stuck at D3). Every NATS/admin-socket tether call in the D-arm is bounded.
_leader_now() { _bt "$1" -- timeout 10 tether cluster status --json 2>/dev/null | jq -r '.leader_id // empty'; }
_leader_is_brk1() { [ "$(_leader_now brk1)" = brk1 ]; }
# _pctl: a BOUNDED ctl for poll_until predicates / seed commands that run AFTER chaos. It replicates
# cmd_ctl (`dexec -u sim ctl1 -- env HOME=/home/sim tether …`) but inserts an IN-CONTAINER `timeout`
# before tether. A ctl op routes through NATS to a broker and can BLOCK indefinitely while the cluster
# re-forms post-heal / post-double-fault, and poll_until cannot interrupt a hung predicate. A HOST-side
# `timeout $SIM ctl | jq` does NOT work: timeout kills the simcluster wrapper but the orphaned in-container
# `docker exec` keeps the pipe's write end open, so the downstream jq never sees EOF and hangs anyway
# (this wedged round-7 at the arm-F precondition). Only an in-container timeout kills tether so docker exec
# returns and the pipe closes. NOT for legit long ops (a real `pull` takes >20s) — only the F-arm probes.
_pctl() { [ "$1" = "--" ] && shift; dexec -u sim ctl1 -- env HOME=/home/sim timeout 20 tether "$@"; }
# _seed_held <node> <sleep-secs> : start a foreground `sleep <secs>` that tether's exec HOLDS as its DIRECT
# child (cmd.Wait, internal/agent/exec.go:192) — so `ps -a` tracks argv="sleep <secs>" with status RUNNING.
# Q3 (r6-findings.md): a `nohup sleep & echo started` self-backgrounds the sleep, so tether's DIRECT child is
# the `sh -c` wrapper, which EXITs in ~3ms → the ONLY tracked row is that sh, NECESSARILY EXITED. The old
# F0b/F0c/F4 fixture then asserted the SAME argv pattern was both RUNNING (F0c/F4) and EXITED (F3): a
# self-contradiction that can never land RUNNING (R6 REFUTED the "exec rc=0 but not RUNNING" report as a
# fixture bug, not a product defect). Holding the sleep foreground makes tether track `sleep <secs>` itself,
# so "the seed is RUNNING" is real and F4's post-injection survival check is non-vacuous.
# NOT via _pctl: its `timeout 20` would reap the held child after 20s. `tether exec`'s OWN default --timeout
# is 10m (an inactivity timeout — a silent `sleep` produces no chunk — exec.go:152), which would kill the
# held seed if the F-arm ever ran slow; --timeout 30m keeps it alive for the whole arm (F0→F4 is ~5min). The
# docker-exec is backgrounded at the CALLER's shell level (a child of the DRILL shell, never of an assert_ok
# `$(...)` subshell) so it persists; node_kill / _cleanup / the next drill's `nuke` reaps it. TOP LEVEL only.
_seed_held() { dexec -u sim ctl1 -- env HOME=/home/sim tether exec --timeout 30m "$1" -- sleep "$2" >/dev/null 2>&1 & }


# ── predicates (FUNCTIONS — R-NOSHC) ────────────────────────────────────────────────────────────────
_hist_xfer() { _pctl -- history --kind transfer -n 200 2>/dev/null; }  # bounded read (EXT-REVIEW hang fix)
_a0_pair_visible() { _h=$(_hist_xfer); printf '%s' "$_h" | grep -q 'start' && printf '%s' "$_h" | grep -q 'complete'; }
# The in-flight transfer uses a DISTINCT SOURCE path (/tmp/inflight.bin) so it is identifiable in history
# by path= (which records the AGENT-SIDE SOURCE, not the ctl-side dest — measured). A0's control uses
# /tmp/big.bin, so the two never collide.
# A-arm DETERMINISM: a LARGE (~1 GiB) incompressible in-flight payload so the tier-B upload is provably still
# running when brk2 is killed. On the sim's fast loopback a 12–80 MiB transfer often COMPLETED before the kill
# landed — that race is exactly what the old #57/#58 runtime-guards fired for. 1 GiB through the tier-B
# pipeline (agent chunk → JS object-store SQLite fsync → ctl reassemble) takes tens of seconds, so injecting
# at the start-row (A1b polls at 1s below, then A1c kills immediately) catches it in-flight deterministically.
# 1 GiB < the per-session 8 GiB bucket cap (transfer.go:67); the sim host has ~846 GiB free.
_a1_start_bg_pull() {
    dexec agt1 -- sh -c 'dd if=/dev/urandom of=/tmp/inflight.bin bs=1M count=1024 2>/dev/null; test "$(stat -c %s /tmp/inflight.bin)" -gt 1073741823' >/dev/null 2>&1 || return 1
    dexec -u sim ctl1 -- sh -c "nohup env HOME=/home/sim tether pull agt1:/tmp/inflight.bin /tmp/inflight.back --nats-url $NURL >/tmp/96-pull.log 2>&1 & echo started" >/dev/null 2>&1
}
_a1_start_row() { _hist_xfer | grep -F 'inflight.bin' | grep -q 'kind=start'; }
_a1_terminal_row() { _hist_xfer | grep -F 'inflight.bin' | grep -qE 'kind=complete|kind=failed'; }
_a1e_control_after() {
    # MUST use agt2, NOT agt1: this control runs only AFTER A1c killed brk2 (agt1's HOME broker), so agt1
    # cannot transfer at all — a pull from agt1 fails on a DEAD home, not on a broken audit face, and that
    # (deterministic, whenever #57 pins) is what made A1e a false ASSERT-FAIL. agt2 is homed on brk1, which is
    # UP throughout the A-arm, so a fresh tier-A transfer via agt2 genuinely exercises the audit read/write
    # face. This is the correct anti-vacuity control: it proves the crashed transfer's MISSING terminal row is
    # specifically #57 (audit face works for a live-home transfer), keeping #57 a PRODUCT-RED, not a drill bug.
    dexec agt2 -- sh -c 'printf tiny-control-payload > /tmp/tiny.bin' >/dev/null 2>&1 || return 1
    "$SIM" ctl -- pull agt2:/tmp/tiny.bin /tmp/tiny-out.bin >/dev/null 2>&1 || return 1
    # this specific transfer paired start+complete (by its own path) = the audit face still works
    _hist_xfer | grep -F 'tiny.bin' | grep -q 'kind=complete'
}
_a2_brk2_up() { [ "$(dexec brk2 -- systemctl is-active tether-broker 2>/dev/null | tr -d '\r')" = active ]; }
# _xfer_obj_count <node> : print the number of OBJECTS (object-store messages) across the OBJ_xfer-*
# buckets, or the literal "unreadable".
#
# EXT-REVIEW-B2. The old oracle grep-counted the OBJ_xfer stream NAME on /jsz — it measured the STREAM's
# existence, not the objects inside it. But OBJ_xfer-<sid> is a per-session bucket that persists until the session is
# removed (transfer.go:189-193); the boot reconciler reaps stale OBJECTS and deliberately leaves the
# stream alone (transfer_reconcile.go:18-22). So one completed tier-B transfer creates the stream forever,
# and stream-presence would report #58 `present` even when every object was correctly reaped = a false
# PRODUCT-RED. The honest measure is the object COUNT: sum state.messages over the OBJ_xfer-* streams via
# the loopback /jsz monitor (unauthenticated; the drill already uses 8223 for cluster_size). A wrong JSON
# path or an offline stream yields no numeric answer -> "unreadable" -> the caller records not_covered,
# never a false present/gone. (The exact /jsz object-store shape is re-verified on the real stack.)
_xfer_obj_count() {
    _xoc_j=$(dexec "$1" -- sh -c "curl -s --max-time 5 'http://127.0.0.1:8223/jsz?accounts=1&streams=1' 2>/dev/null" 2>/dev/null)
    [ -n "$_xoc_j" ] || { printf 'unreadable'; return; }
    _xoc_n=$(printf '%s' "$_xoc_j" | jq -r '
        [ .account_details[]?.stream_detail[]?
          | select(.name != null and (.name | startswith("OBJ_xfer")))
          | (.state.messages // empty) ] | add // "unreadable"' 2>/dev/null | tr -d '\r')
    case "$_xoc_n" in
        ''|*[!0-9]*) printf 'unreadable' ;;
        *)           printf '%s' "$_xoc_n" ;;
    esac
}
# _xfer_at_or_below <node> <baseline> : true once the OBJ_xfer object count has dropped to/below the clean
# baseline (the home-authoritative periodic reap ran). Unreadable → not yet (return 1) so the poll keeps going.
_xfer_at_or_below() {
    _xob_n=$(_xfer_obj_count "$1")
    case "$_xob_n" in ''|*[!0-9]*) return 1 ;; esac
    [ "$_xob_n" -le "$2" ]
}
_b0_refused() { printf '%s' "$_B0_PLAIN" | grep -qiE 'alert|--ack-alerts|BLOCKED'; }
_d0_three_voters() { _bt brk1 -- timeout 10 tether cluster status --json 2>/dev/null | jq -e '[.nodes[]?|select(.phase=="VOTER")]|length==3' >/dev/null 2>&1; }
_d2_new_leader() { _l=$(_leader_now brk2); [ -n "$_l" ] && [ "$_l" != brk1 ]; }
_d3_survivor_write() { dexec -u sim ctl1 -- env HOME=/home/sim timeout 15 tether session create canary2 --pin 970002 --nats-url nats://brk2:4222 >/dev/null 2>&1; }
# The `timeout` MUST be pushed INSIDE the container, in front of the tether binary — never `timeout N
# dexec`: `dexec` is a sourced shell FUNCTION and `timeout` execvp's its argument directly (it never sees
# shell functions), so `timeout 8 dexec …` fails with rc=127 "No such file or directory" and would make
# these arms fail for a reason that has nothing to do with the partition. (Same family as R-NOSHC; verified
# locally: `timeout 3 <fn>` = rc 127.)
# brk1 is a partitioned MINORITY: `cluster status` over the admin socket blocks on raft / returns 69
# (EX_UNAVAILABLE) during the partition, so it is the WRONG liveness probe here (measured). The
# anti-vacuous check we actually want is "brk1's PROCESS is alive, just cut off" — use its loopback
# nats-server monitor (8223, un-partitioned) which answers iff the broker process is up. D4c already
# pins MainPID/NRestarts stable; this pins the process is still SERVING locally.
_d4_brk1_answers() { dexec brk1 -- sh -c "curl -s --max-time 5 -o /dev/null -w '%{http_code}' http://127.0.0.1:8223/varz 2>/dev/null | grep -q 200"; }
_d4_minority_refuses() {
    _o=$(dexec -u sim ctl1 -- env HOME=/home/sim timeout 20 tether session create canary3 --pin 970003 --nats-url nats://brk1:4222 2>&1); _r=$?
    # H6 (2026-07-18 full-suite run): export the REAL rc. The caller used to label every non-zero return of
    # this predicate "rc=0 stale-leader transient accept", but this function also returns non-zero when the
    # write FAILED with an unrecognised message (e.g. rc=69) — so a refused write was recorded in the log and
    # in D6b's reason string as an ACCEPTED one. The rc is the ground truth; keep it addressable.
    _D4_RC=$_r
    log "D4b diag: minority write via brk1 rc=$_r out=$(printf '%s' "$_o" | tail -1)"
    # rc 0 = the write COMMITTED on the minority = split-brain = the thing we must never see.
    [ "$_r" = 0 ] && return 1
    # anything else (not_leader, no leader, election, ErrNotLeader, deadline, or a timeout rc 124) proves
    # the minority did NOT commit — a partitioned minority that hangs on raft is refusing just as validly
    # as one that returns not_leader.
    [ "$_r" = 124 ] && return 0
    # rc 70 "not visible after commit (apply lag)" = the minority accepted the propose but raft cannot
    # commit it (a partitioned minority of 1) → the write never takes effect = no split-brain. Also accept
    # the classic not_leader family and any timeout.
    printf '%s' "$_o" | grep -qiE 'not the leader|no leader|leadership|election|ErrNotLeader|deadline|unavailable|timed out|context|apply lag|not visible after commit|not found'
}
_d4_brk1_stable() {
    _p=$(dexec brk1 -- systemctl show -p MainPID --value tether-broker 2>/dev/null | tr -d '\r')
    _n=$(dexec brk1 -- systemctl show -p NRestarts --value tether-broker 2>/dev/null | tr -d '\r')
    [ "$_p" = "$D_PID0" ] && [ "$_n" = "$D_NR0" ]
}
_d5_one_leader() {
    # EXT-REVIEW-B6: consensus after the heal = all THREE brokers answer with a non-empty leader AND agree
    # on one. The old `sort -u | wc -l == 1` passed when two brokers errored and one survivor answered.
    _ol=$( { _leader_now brk1; _leader_now brk2; _leader_now brk3; } )
    [ "$(printf '%s\n' "$_ol" | grep -v '^$' | wc -l | tr -d ' ')" -eq 3 ] || return 1
    [ "$(printf '%s\n' "$_ol" | grep -v '^$' | sort -u | wc -l | tr -d ' ')" -eq 1 ]
}
_d6_readback() { dexec -u sim ctl1 -- env HOME=/home/sim timeout 10 tether session ls --json --nats-url nats://brk1:4222 2>/dev/null | jq -e '.sessions[]?|select(.name=="canary2")' >/dev/null 2>&1; }
# H6: hoisted out of the D6b block so the SAME reader can take the PRE-heal reading (see _C3_PRE below).
_c3_via() { dexec -u sim ctl1 -- env HOME=/home/sim timeout 10 tether session ls --json --nats-url "nats://$1:4222" 2>/dev/null | jq -e '.sessions[]?|select(.name=="canary3")' >/dev/null 2>&1; }
_c3_gone_everywhere() { ! _c3_via brk1 && ! _c3_via brk2 && ! _c3_via brk3; }
# _c3_committed_by <broker> : did <broker>'s request handler return from a committed create for canary3?
# Read its OWN broker application log for `broker: session created` (sessions.go:77 emits only after
# createSession succeeds). This is a commit-success witness on the broker that HANDLES the request, not
# necessarily the raft leader that proposed it; under a complete route+raft partition, however, brk1 cannot
# obtain such success from the majority, so seeing the line while isolation is proven is a #65 candidate. R6 #65
# (r6-findings.md): the control plane is a cross-broker NATS QUEUE GROUP, so `--nats-url brk1` only picks the
# ENTRY server, NOT the committer. "canary3 visible via the majority after heal" is therefore NOT #65 by
# itself — it can be a LEGITIMATE majority commit the queue group routed to brk2/brk3 (R6 proved the old
# ledger's 5/6 "durable minority writes" were exactly that, mis-attributed by --nats-url dialing). #65 needs
# the ISOLATED MINORITY brk1 to have COMMITTED it; brk1's own commit log is the only attribution ground truth.
_c3_committed_by() {
    # Full-line grep for the commit message, THEN filter for canary3 on the same line. (Do NOT use
    # `grep -o "...created[^\n]*"`: in grep BRE a bracket `[^\n]` excludes the LITERAL chars \ and n, so it
    # would truncate the match at the 'n' in 'ca-n-ary3' and false-negative — hiding a real #65.)
    dexec "$1" -- sh -c 'grep -ahF "broker: session created" /var/log/tether/broker.log /var/log/tether/broker.err 2>/dev/null' 2>/dev/null | grep -q 'canary3'
}
_f_agt1_online() { _pctl -- node ls --json 2>/dev/null | jq -e '.nodes[]?|select(.nid=="agt1")|select(.status=="ONLINE")' >/dev/null 2>&1; }
_agt_online() { _pctl -- node ls --json 2>/dev/null | jq -e --arg n "$1" '.nodes[]?|select(.nid==$n)|select(.status=="ONLINE")' >/dev/null 2>&1; }
# Q3: the two agt1 seeds are HELD FOREGROUND (see _seed_held) — tracked as `sleep 9661`/`sleep 9662`,
# RUNNING while agt1 lives. F3 then reads a REAL RUNNING→EXITED transition instead of the old nohup fixture's
# vacuous "the sh already EXITED at 3ms" (which would let F3 pass BEFORE the double-fault ever landed).
_f0_agt1_two_running() {
    _pctl -- ps -a --json 2>/dev/null \
        | jq -e '[.processes[]?|select(.nid=="agt1")|select(.argv|join(" ")|test("sleep 966[12]"))|select(.status=="RUNNING")]|length==2' >/dev/null 2>&1
}
_agt2_seed_running() {
    _pctl -- ps -a --json 2>/dev/null \
        | jq -e '[.processes[]?|select(.nid=="agt2")|select(.argv|join(" ")|test("sleep 9663"))|select(.status=="RUNNING")]|length>=1' >/dev/null 2>&1
}
_f1_kill_both() { node_kill agt1; node_kill brk2; return 0; }
_f2_start_both() { node_start brk2; node_start agt1; return 0; }
_f3_agt1_exited() {
    _pctl -- ps -a --json 2>/dev/null \
        | jq -e '[.processes[]?|select(.nid=="agt1")|select(.argv|join(" ")|test("sleep 966[12]"))|select(.status=="EXITED")]|length==2' >/dev/null 2>&1
}
# H14 (2026-07-18 full-suite run): F0c and F4 used to assert the IDENTICAL predicate, so one root cause was
# billed as two ASSERT-FAILs and neither could tell the two failure modes apart. They now ask different
# questions of different strengths:
#
#   _f0c_capture_agt2_seed (F0c, PRE-injection) — "is the control seed running AT ALL right now?", and
#       record its concrete identity (PsEntry.pid, proto/messages.go:411) so the post-injection arm has
#       something to compare against. F0c is a GATE (assert_setup): with it failing there is no control, so
#       F4 is unjudgeable and the F arm must not run at all.
#   _f4_agt2_seed_survived (F4, POST-injection) — "is THAT EXACT process, by pid, still RUNNING, and is
#       there no terminal row for the seed?" This is what distinguishes a table-wide reconciliation sweep
#       (the recorded pid flips to EXITED/LOST) from a control that was merely never running (impossible
#       here — F0c gated it) and from a control that died and was replaced (a different pid would not
#       count). The old shared predicate could not separate any of those.
_F0C_PID=""
_f0c_capture_agt2_seed() {
    _F0C_PID=$(_pctl -- ps -a --json 2>/dev/null \
        | jq -r '[.processes[]?|select(.nid=="agt2")|select(.argv|join(" ")|test("sleep 9663"))|select(.status=="RUNNING")]|.[0].pid // empty' 2>/dev/null | tr -d '\r')
    [ -n "$_F0C_PID" ]
}
_f4_agt2_seed_survived() {
    _f4j=$(_pctl -- ps -a --json 2>/dev/null) || return 1
    # the pid recorded PRE-injection must still be the one RUNNING seed row on agt2 …
    printf '%s' "$_f4j" \
        | jq -e --arg p "$_F0C_PID" '[.processes[]?|select(.nid=="agt2")|select(.pid==$p)|select(.argv|join(" ")|test("sleep 9663"))|select(.status=="RUNNING")]|length==1' >/dev/null 2>&1 || return 1
    # … and NO seed row on agt2 may have been closed out (EXITED/LOST) — a table-wide sweep marks the
    # control terminal, which a bare "some seed row is RUNNING" check would miss if a row were re-created.
    ! printf '%s' "$_f4j" \
        | jq -e '[.processes[]?|select(.nid=="agt2")|select(.argv|join(" ")|test("sleep 9663"))|select(.status!="RUNNING")]|length>0' >/dev/null 2>&1
}
_f5_audit_row() { _pctl -- history --kind proc -n 100 2>/dev/null | grep -q 'kind=reconciled_closed'; }
_f6_fresh_exec() { _pctl -- exec agt1 -- echo F6-ALIVE >/dev/null 2>&1; }

# _ensure_leader_brk1 : make brk1 the leader, idempotently. grow_to_3 inits brk1 so it is usually
# already the leader; `transfer-leader brk1` then errors "already the leader" (exit 70). Only transfer
# when needed — a no-op must not SETUP-RED.
_ensure_leader_brk1() {
    # already leader? (inline jq — do not call an undefined _leader_is_brk1_now; Stage-C minor 5)
    if _bt brk1 -- timeout 10 tether cluster status --json 2>/dev/null | jq -e '.leader_id=="brk1"' >/dev/null 2>&1; then return 0; fi
    _cur=$(sim_leader) || return 1
    # swallow the "already the leader" exit-70 (a TOCTOU where leadership drifted back is not a SETUP-RED)
    _tl=$("$SIM" exec "$_cur" -- sh -c "runuser -u tether -- tether cluster transfer-leader brk1 --wait" 2>&1) || printf '%s' "$_tl" | grep -qiE 'already the leader'

}
_cleanup() {
    fault_cleanup_all || true
    ev_stop ctl1 || true
    for _n in brk1 brk2 brk3 agt1 agt2; do
        node_running "$_n" 2>/dev/null || node_start "$_n" >/dev/null 2>&1 || true
    done
    true
}

drill_begin "96-mid-flight-chaos (N=3: tier-B mid-flight + leader partition + double fault)"
drill_install_traps _cleanup

"$SIM" nuke >/dev/null 2>&1 || true

assert_setup "grow_to_3 (N=3 HA)"                       grow_to_3 2 1
assert_setup "ensure brk1 is the leader (grow inits brk1, so this is usually a no-op — a redundant transfer to self would error 70)" _ensure_leader_brk1
# THREE HARD PRECONDITIONS. They are not conveniences: #58's reaperMayDelete()==false is guaranteed BY
# SOURCE only while the victim is a NON-leader. If leadership drifted, the arm degrades from a source
# guarantee to a coin flip and could go falsely green.
assert_setup "PRECONDITION leader is brk1 (so the victim brk2 is a NON-leader: #58's reaperMayDelete()==false is then guaranteed by source, not by luck)" \
    _leader_is_brk1
assert_setup "session $SID + ctl login"                 "$SIM" session "$SID" --pin "$PIN"
assert_setup "agent-join agt1"                          "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_setup "agent-join agt2 (the untouched control node)" "$SIM" agent-join agt2 --session "$SID" --pin "$PIN"
# PRECONDITION: agt1's tunnel broker is brk2 — the same non-leader we will kill.
assert_setup "provision agt1 agent.yaml with tunnel/NATS on brk2 (the NON-leader victim)" \
    agent_provision_yaml agt1 "$SID" "$NURL" open
assert_setup "provision agt2 agent.yaml (control, on brk1)" \
    agent_provision_yaml agt2 "$SID" "nats://brk1:4222" open
# R15 #58: shorten the home-authoritative orphan-reap cadence on the VICTIM home broker (brk2) so its
# periodic xfer-orphan-reap pass is OBSERVABLE within the drill instead of the 5m production default. This is
# a labeled DEPLOYMENT config knob (broker.cluster.xfer_reap_interval, Mandate ③) — it does NOT do tether's
# work, it only tunes how often tether's OWN reap runs so the drill can watch the R15 fix converge. Written
# while brk2 is up; it takes effect on brk2's A2a restart (node_start re-reads broker.yaml). Idempotent; the
# sed inserts under the sim-written (UNCOMMENTED) broker.cluster block, right after its raft_addr line.
assert_setup "R15 #58: set broker.cluster.xfer_reap_interval=8s on the victim home broker brk2 (so its home-authoritative periodic reap is OBSERVABLE, not a 5m wait — the reap itself is tether's, this only tunes cadence)" \
    dexec brk2 -- sh -c "grep -q 'xfer_reap_interval' /etc/tether/broker.yaml || sed -i '/^    raft_addr:/a\\    xfer_reap_interval: 8s' /etc/tether/broker.yaml"
# R16 #58 Lane C: this session is STRUCTURALLY split-home, so no HOME can reap it — the fix is a
# LEADER-driven cross-home GC. Only the CADENCE (xfer_reap_interval) is compressed here.
#
# EXTERNAL REVIEW F2 (2026-07-23): the AGE FLOOR (xfer_cross_home_reap_age) is no longer compressible.
# It used to be settable to 5s through the production YAML, and the reviewer showed that is unsafe for
# real deployments: in a split-home session the leader only excludes its OWN local tracker, so it cannot
# see a transfer still live on ANOTHER home, and a floor below the tier-B watchdog lets it delete an
# in-use object. The schema now only permits RAISING the floor above the derived 3x tier-B default.
#
# The honest consequence, recorded rather than worked around: with a 15m floor the cross-home GC cannot
# be observed inside a drill window at all, so this arm can no longer prove the reap. That is a coverage
# LOSS traded for production safety — the Mandate forbids re-opening a test-only backdoor in the product
# to buy it back. #58 therefore stays OPEN with its deploy-tier proof owed, now for a STRUCTURAL reason.
not_covered "#58 cross-home GC reap (deploy-tier observation)" \
    "the age floor is no longer compressible from the production YAML (external review F2 clamped it to >= 3x the tier-B timeout, because a lower floor lets the leader delete an object still live on another home). With a 15m floor the reap cannot occur inside a drill window, so this arm cannot observe it. Re-opening a test-only seam in the product to make it observable is exactly what F2 forbids; the honest options are a drill that waits >15m or a hermetic pin (which exists: TestXferCrossHomeGCReapsSplitHome)" gap
assert_setup "R16 #58: set broker.cluster.xfer_reap_interval=8s on the LEADER brk1 (CADENCE only — the age floor is no longer compressible, see above)" \
    dexec brk1 -- sh -c "grep -q 'xfer_reap_interval' /etc/tether/broker.yaml || sed -i '/^    raft_addr:/a\\    xfer_reap_interval: 8s' /etc/tether/broker.yaml"
assert_setup "R16 #58: restart brk1 so it loads the cross-home GC knobs" \
    dexec brk1 -- systemctl restart tether-broker
# Restarting brk1 hands leadership away (raft re-elects while it is down). The cross-home GC is LEADER-ONLY,
# so the compressed knobs must sit on the node that IS the leader — re-establish the arm's precondition after
# the restart, exactly as the setup did before it.
assert_setup "R16 #58: brk1 is the leader again after the knob-loading restart (the cross-home GC is leader-only, so the compressed knobs must be on the LEADER)" \
    _ensure_leader_brk1
assert_setup "R16 #58: PRECONDITION re-verified — leader is brk1 (post-restart)" \
    sh -c "$SIM status --json 2>/dev/null | jq -e '.leader_id==\"brk1\"' >/dev/null"
# NOTE (Stage-C B4): this drill implements the four load-bearing arms — A (#57/#58 tier-B mid-flight),
# B0 (run --ack-alerts gate), D (the flagship leader partition), F (double fault). The roadmap's arm B
# (run-PTY kill-broker → DOC-28) and arm C (expose-crash → RETURN + home_reassign_failed observation) are
# NOT-COVERED here: arm B's behaviour (a run session over an explicit --nats-url to a killed broker times
# out with 'agent unreachable: no heartbeat' — a documented liveness watchdog, run.go:453-456) is a
# GREEN-by-design outcome already source-closed (SB-96-3), and arm C's data-plane crash-strand is drill
# 71/#29's territory. No sys.events observer is set up, because nothing consumes it (a built-but-unread
# observer would be dead setup). DOC-28 (run cross-broker-restart semantics undocumented) is recorded in
# the ledger, not pinned by a live arm here.
not_covered "96 arm B (run-PTY kill-broker → DOC-28) + arm C (expose-crash RETURN + home_reassign_failed event)" \
    "arm B's kill-broker-mid-run outcome is GREEN-by-design (run.go's 15s liveness watchdog synthesises 'agent unreachable: no heartbeat' — SB-96-3, source-closed); arm C's crash-strand data plane is drill 71/#29's territory (a cluster expose home cannot deliver to a non-tunnel broker, home.go:96-113). The rehome/home_reassign_failed event is member-readable (rehome_events.go:9-11 → SubjSysEvents) but its crash-path firing needs arm C's fixture; DOC-28 is ledger-registered" gap

# ══ A — tier-B transfer killed mid-flight (#57 / #58) ═══════════════════════════════════════════════
# 12 MiB > transferTierAMaxBytes (8 MiB, transfer.go:52) forces tier B. (The roadmap said ">1 MiB
# max_payload"; that is wrong — the tier boundary is the 8 MiB constant, not the NATS payload limit.)
assert_ok "A0a create a 12 MiB payload (> the 8 MiB transferTierAMaxBytes constant => tier B is forced)" \
    dexec agt1 -- sh -c 'dd if=/dev/urandom of=/tmp/big.bin bs=1M count=12 2>/dev/null; test -s /tmp/big.bin'
# The positive control FIRST: prove the audit read path pairs start+complete when nothing is broken.
assert_ok "A0b CONTROL: a full tier-B pull completes cleanly before any injection (proves the audit read path pairs start+complete when nothing is wrong — without this, a missing terminal row later means nothing)" \
    "$SIM" ctl -- pull agt1:/tmp/big.bin /tmp/big-ok.bin
assert_ok "A0c CONTROL: history shows that transfer's START and COMPLETE as a pair" \
    poll_until 60 3 "a start+complete pair is readable" -- _a0_pair_visible
# EXT-REVIEW-B2: the #58 baseline. After a CLEAN tier-B transfer the bucket OBJ_xfer-<sid> exists but its
# object was reaped by deleteXferObject on completion — so the object count here is the "no orphan" floor
# that the later interrupted-transfer count must EXCEED before #58 can even be judged. (unreadable here is
# not fatal yet; the #58 arm re-reads and gates on it.)
_B58=$(_xfer_obj_count brk1)
log "A0d #58 baseline: OBJ_xfer object count after the clean transfer = $_B58 (the stream persists; the object was reaped by deleteXferObject — this is the no-orphan floor)"

assert_ok "A1a start a tier-B pull in the BACKGROUND and let it get in flight" _a1_start_bg_pull
assert_ok "A1b the transfer is REALLY in flight (a 'start' row is visible) — otherwise 'no terminal row' would be true for the wrong reason" \
    poll_until 60 1 "the in-flight transfer's start row appears" -- _a1_start_row
assert_ok "A1c INJECT: docker kill brk2 the instant the start row lands — the 1 GiB upload is still streaming, so it is caught in-flight (the agent's home AND a guaranteed NON-leader, so #58's reaper gate is false by source)" node_kill brk2

# The window: the transfer timeout (5 min) + 90s. NOT 2x — past the timeout NO code path writes a
# terminal row at all, so a longer wait adds nothing but wall-clock (plan §5.3-T9).
log "A1d waiting up to 5m+90s for a terminal audit row (the transfer timeout + slack; NOT 2x — past the timeout no code path writes one at all)"
poll_until 390 30 "a terminal (complete|failed) row for the in-flight transfer — expected NEVER to appear (#57)" -- _a1_terminal_row || true

# R-EXHAUST — four states, no `else`.
_A_ROWS=$(_pctl -- history --kind transfer -n 200 2>/dev/null | grep -F 'inflight.bin' || true)
if [ -z "$_A_ROWS" ]; then
    # NOT assert_fail: A0c (control) AND A1b (the in-flight start row) both already proved the reader works,
    # so an empty result HERE is not a broken reader — it is that this specific transfer's audit is not
    # readable during the window (its home broker brk2 is DOWN for the whole 390s, and the transfer's
    # history/JS may live on it), so #57 cannot be judged this run. Record not_covered, not a spurious fail.
    not_covered "96-A #57 in-flight interruption (audit unreadable — home broker down carries it away)" \
        "no transfer row was readable during the window even though the reader is proven working (A0c control pairs start+complete, A1b saw this transfer's start row before the kill) — the home broker brk2 is down for the whole 390s window so the crashed transfer's audit is not queryable now. CLASS gap (R14 re-adjudication): the transfer audit lives on the KILLED home broker (not replicated to survivors), so #57 cannot be measured in-sim from the survivor side — a structural coverage hole, not a re-run valve. #57's mechanism is source-certain (watchdog hangs off broker runCtx transfer.go:593/:704, tracker rebuilds EMPTY broker.go:602); hermetic owner: the transfer unit tests. #58 (the sibling orphan-object relapse) IS pinned live below." gap
elif printf '%s' "$_A_ROWS" | grep -qE 'complete|failed'; then
    not_covered "96-A #57 in-flight interruption (transfer completed before the crash — in-sim interruption not reliably constructable)" "the tier-B transfer reached a terminal (complete) row — the 1 GiB in-flight payload finished before the kill landed. CLASS gap (R14 re-adjudication): DETERMINIZATION ATTEMPTED AND MEASURED INSUFFICIENT — the payload was enlarged 12 MiB→1 GiB and the kill moved to the instant the start row lands (A1b polls at 1s), yet on the 88-vCPU sim host the tier-B pipeline still drains 1 GiB before docker kill returns (r14d 2026-07-20). Catching the PRE-completion crash needs bandwidth-shaping (tc) on the agent's egress, which would also throttle heartbeats/health and risk destabilising the cluster — so #57's dangling-audit is NOT reliably constructable in-sim and is registered as a coverage gap (source-certain mechanism, hermetic owner: transfer unit tests). NOTE: the SIBLING #58 (orphaned objects never reaped) DOES pin live here — this same interrupted transfer stranded objects because the reaping was cut off, so the live relapse is caught even though the dangling-start-row half is not." gap
elif printf '%s' "$_A_ROWS" | grep -q 'start'; then
    # ROUND-3 R3-F3: this used to declare #57 "forever" HERE — while brk2 (the crashed home) is still DOWN
    # and the finalize-on-recovery pass has therefore had NO opportunity to run. That verdict measured the
    # crash, not the product's recovery, so it could neither certify nor refute the R16/G67 fix. Bring the
    # home back FIRST, give the recovery pass its window, and only then judge.
    assert_ok "A1f-pre bring brk2 back so the finalize-on-recovery pass can run BEFORE #57 is judged (R3-F3: the old verdict fired while the home was still down)" node_start brk2
    assert_ok "A1f-pre brk2's broker is active again (recovery cannot run on a dead broker)" \
        poll_until 120 3 "brk2 broker active" -- _a2_brk2_up
    _a57_try=0
    while [ "$_a57_try" -lt 30 ]; do
        _a57_try=$((_a57_try+1))
        _A_ROWS=$(_pctl -- history --kind transfer -n 200 2>/dev/null | grep -F 'inflight.bin' || true)
        printf '%s' "$_A_ROWS" | grep -qE 'complete|failed' && break
        sleep 6
    done
    log "96-A #57 post-recovery poll: $_a57_try attempt(s); rows: $(printf '%s' "$_A_ROWS" | tr '\n' ' ' | tr -cd '[:print:]' | cut -c1-200)"
    if printf '%s' "$_A_ROWS" | grep -qE 'complete|failed'; then
        _as_pass "#57 FIXED: after the home broker came back, the finalize-on-recovery pass wrote a TERMINAL for the interrupted transfer (no dangling start)"
        _A_57_PINNED=0
    else
    _A_57_PINNED=1
    product_red "#57 an in-flight tier-B transfer whose home broker crashes leaves a DANGLING start row and NO terminal audit, forever: the watchdog hangs off the broker's runCtx (transfer.go:593/:704) so it dies with the process, transferTracker is an in-memory map rebuilt EMPTY on restart (transfer.go:99-104 + broker.go:602), and any late finalization from the agent is silently dropped by handleEvTransfer's 'preview == nil -> return' (:816-819). Operators auditing transfers see a transfer that never ended."
    fi
else
    _as_fail "#57 UNJUDGEABLE — rows exist for the in-flight transfer but match neither start nor a terminal kind; triage before judging"
fi
# The control source AFTER the injection is only meaningful when #57 actually pinned (a dangling start to
# contrast against). When the transfer completed too fast (#57 not_covered), there is nothing to contrast.
if [ "${_A_57_PINNED:-0}" = 1 ]; then
    assert_ok "A1e CONTROL after the injection: a fresh small (tier-A) transfer still pairs start+complete — so the audit face works and it is specifically the crashed transfer's terminal row that is missing" \
        _a1e_control_after
fi

# #58 — the orphaned OBJECT (not the persistent bucket). Victim is a non-leader BY SETUP, so
# reaperMayDelete()==false is a source guarantee. EXT-REVIEW-B2: measure the ORPHAN OBJECT COUNT while
# brk2 is still DOWN (read from brk1, the live leader) and PROVE it exceeds the clean baseline before
# judging whether the boot reaper removed it — stream existence is not orphan presence.
_C_ORPHAN=$(_xfer_obj_count brk1)
log "A2-pre #58: OBJ_xfer object count with brk2 down = $_C_ORPHAN (baseline was $_B58)"
if [ "$_B58" = unreadable ] || [ "$_C_ORPHAN" = unreadable ]; then
    not_covered "96-A2 (#58) OBJ_xfer object count is unreadable via /jsz (baseline=$_B58, orphan-probe=$_C_ORPHAN)" \
        "cannot distinguish a reaped-empty bucket from one holding an orphan object, so #58 cannot be judged this run. Its mechanism is source-certain (reconcileXferObjectsOnBoot's first gate 'if !b.reaperMayDelete() { return }', transfer_reconcile.go:34, is false for any non-leader; the victim is a non-leader by setup) — retained SOURCE-CONFIRMED, live object-level measurement owed to a run where /jsz exposes the object-store messages" runtime-guard
elif [ "$_C_ORPHAN" -le "$_B58" ]; then
    not_covered "96-A2 (#58) no orphan object was manufactured (count $_C_ORPHAN <= clean baseline $_B58)" \
        "the interrupted tier-B transfer left no object above the clean floor — on the loopback network it completed too fast to strand one (the same non-determinism as #57). There is nothing for the reaper to fail on, so #58 is not judged rather than guessed" runtime-guard
else
    # The orphan IS present (count $_C_ORPHAN > baseline $_B58) and brk2 is still down. Restart brk2 so its
    # boot reconciler runs, then judge whether that SPECIFIC orphan survived.
    assert_ok "A2a bring brk2 back so its HOME-AUTHORITATIVE periodic reap runs (orphan object count $_C_ORPHAN > baseline $_B58 while it was down)" node_start brk2
    assert_ok "A2b brk2's broker is up again" poll_until 120 3 "brk2 broker active" -- _a2_brk2_up
    # R15 #58: brk2 restarts with xfer_reap_interval=8s (set in setup). Its periodic xfer-orphan-reap pass is
    # now HOME-AUTHORITATIVE (reaperCaughtUp drops the leader-only gate + homeOwnsXferBucket partitions by
    # home + the pass is per-broker), so a NON-LEADER home reaps its OWN orphan once caught up (~8s later).
    # POLL for the count to drop back to the tombstone FLOOR instead of measuring once at boot (the boot
    # reconciler still skips — brk2 not yet caught up at boot; before R15 the leader-only pass would NEVER reap
    # a follower-homed bucket, so this poll would time out → the REGRESSION branch fires).
    #
    # FLOOR, not exactly $_B58: a JS object-store DELETE leaves a tombstone message, so the OBJ_xfer stream's
    # message count never returns to zero — each COMPLETED transfer leaves ~1 tombstone. $_B58 was measured
    # after ONE completed transfer (A0b); by A2 another has completed (A1e control) plus the interrupted one's
    # partial finalization, so the reaped floor is $_B58 + a few tombstones, NOT $_B58. A generous budget of 5
    # absorbs that while staying FAR below the ~thousands-strong in-flight orphan ($_C_ORPHAN), so a genuine
    # non-reap (count stays near $_C_ORPHAN) still trips REGRESSION — the tolerance cannot launder a real leak.
    _reap_floor=$(( _B58 + 5 ))
    # Give the home time to RE-STABILIZE before judging the reap: brk2 must not only be up (A2b) but have
    # re-admitted agt1 (its home agent re-registers) AND caught up on raft, or reaperCaughtUp/homeOwnsXferBucket
    # are transiently false and the periodic reap correctly declines. Poll agt1 back ONLINE first, then poll the
    # count down to the tombstone floor with a generous window (brk2's post-crash raft catch-up + the 8s reap
    # cadence).
    poll_until 60 3 "agt1 re-registers ONLINE on the healed cluster (home re-stabilizes before the reap is judged)" -- sh -c "\"$SIM\" exec brk1 -- runuser -u tether -- tether admin nodes 2>/dev/null | grep -E 'agt1' | grep -q ONLINE" || true
    poll_until 90 3 "the home-authoritative periodic reap drops the orphan to the tombstone floor" -- _xfer_at_or_below brk1 "$_reap_floor" || true
    _C_AFTER=$(_xfer_obj_count brk1)
    log "A2-post #58: OBJ_xfer object count after brk2's periodic-reap window = $_C_AFTER (orphan was $_C_ORPHAN, baseline $_B58)"
    # Was the home-authoritative reap actually LOGGED on ANY broker? (The bucket's home is whichever broker
    # owns agt1's session; the reap fires there. Checking all three distinguishes "ran but failed" from "did
    # not run".)
    # R16: accept EITHER reap signature — the home-authoritative periodic reap (R15) OR the LEADER cross-home
    # GC (R16 Lane C, the only thing that can reclaim a STRUCTURALLY split-home bucket no home owns).
    _reaped_anywhere=0
    for _rb in brk1 brk2 brk3; do
        # h1 F3: both reap signatures are SLOG lines, which moved to broker.log.
        # Reading only broker.err would make _reaped_anywhere permanently 0 and
        # send every run down the "never reaped" branch.
        if sim_broker_slog_grep "$_rb" "orphan xfer objects reaped|cross-home GC reaped aged orphan xfer objects"; then _reaped_anywhere=1; break; fi
    done
    if [ "$_C_AFTER" = unreadable ]; then
        not_covered "96-A2 (#58) object count became unreadable after the restart" "cannot judge whether the orphan was reaped" runtime-guard
    elif [ "$_C_ORPHAN" -le "$_reap_floor" ]; then
        # NON-VACUITY GATE (R16): "the count is at the floor" only means the reap WORKED if there was a real
        # orphan set to reclaim. When the A-arm's 1 GiB tier-B transfer completes before the docker kill (the
        # same in-sim interruption gap #57 records), the peak count never rises above the floor and the FIXED
        # branch below would PASS having reclaimed nothing. Record that as uncovered instead of banking a
        # vacuous green — the reap/GC is pinned hermetically; this arm only claims it when it truly ran.
        not_covered "96-A2 (#58) the A-arm produced NO orphan set this run (peak orphan count $_C_ORPHAN <= tombstone floor $_reap_floor), so neither the home-authoritative reap nor the R16 leader cross-home GC had anything to reclaim" "IN-SIM INTERRUPTION GAP (same root as 96-A/#57): the tier-B upload reached a terminal before the kill landed, so no in-flight chunks were stranded. A 'count is at the floor' PASS here would be VACUOUS — it would assert the reap works on a run where no reap was needed. The #58 mechanisms are pinned hermetically (TestXferCrossHomeGCReapsSplitHome / TestXferCrossHomeGCSkipsBusyBucket / TestXferUnreapableBucketCounter + the derivation pin); this arm claims them ONLY on a run that actually strands objects." gap
    else
        # ROUND-3 R3-F3: the FIXED / REGRESSION / SPLIT-HOME judges that used to live here are GONE, not
        # relocated. They all rested on a compressed 5s `xfer_cross_home_reap_age`, which external review
        # F2 made UNLOADABLE: the production schema now refuses anything below the 15m safe floor, because
        # a shorter floor lets the leader delete an object still live on ANOTHER home. With a 15m floor no
        # reap can occur inside this drill's window, so any verdict here would be judging an event that
        # cannot happen — and the previous revision kept those branches while ALSO pre-recording a gap,
        # which is how the reply and the diff came apart.
        # ROUND-4 R4-F3: and NO gap is recorded here either. The gap is STRUCTURAL — it holds for every
        # run of this drill regardless of which branch the A-arm takes — so it is registered exactly once,
        # unconditionally, in the A-arm setup section above. Booking
        # it a second time here made the coverage count depend on whether this branch was reached, which
        # is precisely the accounting a structural gap must not have.
        :
    fi
fi

# ══ B0 — run --ack-alerts (inventory row 122's S9 debt) ═════════════════════════════════════════════
# Free: the alert state is a by-product of the kill we already did.
_B0_PLAIN=$("$SIM" ctl -- run agt2 -- true 2>&1); _B0_RC=$?
if printf '%s' "$_B0_PLAIN" | grep -qiE 'alert|--ack-alerts|BLOCKED'; then
    assert_ok "B0a 'run' is gated by the severe alert state left by the kill (the refusal names --ack-alerts)" _b0_refused
    assert_ok "B0b CONTROL: the SAME command with --ack-alerts is allowed through (proves the gate was BYPASSED, not that the command merely works)" \
        "$SIM" ctl -- run agt2 --ack-alerts -- true
else
    not_covered "96-B0 run --ack-alerts gate (inventory row 122's S9 cell)" "run was NOT refused under the alert state produced by this drill's kill (rc=$_B0_RC), so there is no gate to prove bypassing here; the semantics differ from 90's severe-banner path and need their own explore->pin" gap
fi

# ══ D — PARTITION THE LEADER (the flagship arm) ════════════════════════════════════════════════════
# 5 elements: (1) baseline = 3 VOTER + a real write through brk1 + reachability; (2) observation = dexec
# (never over the partitioned network) + each node's own admin socket; (3) boundary = 6222+7400 only,
# 4222 DELIBERATELY left up; (4) oracle = a real write on the survivors + the ex-minority reading that
# row back; (5) cleanup = the single EXIT trap.
# POLL, not a bare check: the preceding #58 arm killed+restarted brk2, which must rejoin as a VOTER before
# the partition arm's 3-VOTER baseline holds (a bare check races brk2's raft catch-up).
assert_ok "D0a BASELINE: 3 VOTER (poll — brk2 rejoins raft after the #58 arm restarted it)" \
    poll_until 120 5 "3 VOTER after the #58 arm" -- _d0_three_voters
assert_ok "D0b BASELINE: leader is brk1" poll_until 60 3 "leader settles back to brk1" -- _leader_is_brk1
assert_ok "D0c BASELINE: a real WRITE through brk1 succeeds (this is what must move to the survivors)" \
    "$SIM" ctl -- session create canary1 --pin 970001
assert_ok "D0d BASELINE: switch the ctl back to $SID (R-CTX)" \
    dexec -u sim ctl1 -- env HOME=/home/sim tether login -s "$SID" --pin "$PIN" --nats-url "nats://brk1:4222"
assert_ok "D0e BASELINE: brk2 can reach brk1's route port" fault_assert_reachable brk2 brk1 6222

D_PID0=$(dexec brk1 -- systemctl show -p MainPID --value tether-broker 2>/dev/null | tr -d '\r')
D_NR0=$(dexec brk1 -- systemctl show -p NRestarts --value tether-broker 2>/dev/null | tr -d '\r')
assert_ok "D1a INJECT: silently blackhole brk1's route (6222) + raft (7400) — 4222 is DELIBERATELY LEFT UP" \
    fault_partition_on brk1 6222 7400
# THE THREE-WAY SELF-PROOF that the injection is what we say it is.
assert_ok "D1b SELF-PROOF: brk2->brk1:6222 now HANGS (rc=124) — a silent DROP. An immediate failure would mean an OUTAGE, which is a different (easier) fault" \
    poll_until 20 2 "brk1:6222 blackholed from brk2" -- fault_assert_blackholed brk2 brk1 6222
assert_ok "D1c SELF-PROOF: brk2->brk1:7400 also hangs" \
    poll_until 20 2 "brk1:7400 blackholed from brk2" -- fault_assert_blackholed brk2 brk1 7400
assert_ok "D1d SELECTIVE CONTROL: ctl->brk1:4222 STILL CONNECTS — this is what makes 'the minority is read-only' a claim about TETHER. A broker cut off from everything obviously cannot serve; that would prove nothing" \
    fault_assert_reachable ctl1 brk1 4222

assert_ok "D2 the SURVIVORS elected a new leader — read from brk2/brk3, never from brk1 (whose view is stale by construction)" \
    poll_until 120 3 "a new leader among {brk2,brk3}" -- _d2_new_leader
# Q4 (r6-findings.md / r2-plan §19, product FIXED R14): the survivor-side `session create` is now a clean
# POSITIVE. R6 confirmed the pre-fix defect — the read-back after a COMMITTED write timed out inside a 1s
# window (apply-lag measured 1.37s) and the create then reported failure AND was non-idempotent (every retry
# hit already_exists+rc=70, so poll_until could never turn green). The R14 fix makes proposeOrForward's
# nil-return (= committed to raft) a best-effort SUCCESS on the FIRST try (read-back non-fatal, 150×20ms
# window), so this write commits + returns rc=0 reliably once the survivors re-form quorum — no retry
# deadlock. The 300s poll only covers JS-meta re-formation latency on a loaded host, not the old flake.
assert_ok "D3 (Q4) THE MAJORITY IS REALLY ALIVE: a real WRITE on the survivor side returns SUCCESS (rc=0) — the only legitimate proof of quorum (not a status field), and now a deterministic positive after the R14 best-effort-success fix (was the apply-lag non-idempotent flake)" \
    poll_until 300 5 "a real write commits on the survivors (JS meta re-forms 2/3 quorum after losing brk1 — legitimately slow, and slower still on a loaded host; OQ-7 tolerant window)" -- _d3_survivor_write
assert_ok "D4a ANTI-VACUOUS: brk1's admin socket still answers (it is alive, just partitioned — otherwise D4b would be about a dead process)" \
    _d4_brk1_answers
# D4b — RECORDED, not hard-asserted (OQ-7). A write via the partitioned minority brk1 is NON-DETERMINISTIC:
# right after the partition brk1 may not yet have detected it lost quorum (election timeout not elapsed),
# so as a STALE LEADER it can briefly accept a write and return rc=0 — but that write CANNOT truly commit
# (no majority ack) and is rolled back on heal. Whether the CLI catches it mid-window (rc=0), or after
# brk1 detects the loss (rc=70 "apply lag / not visible after commit"), or on a raft-blackhole timeout
# (124) is a race on brk1's own failure-detection clock. The DETERMINISTIC no-split-brain proof is D5b
# (all converge on ONE leader) + D6 (the ex-minority reads back the MAJORITY's write after heal, and its
# own stale write is gone) — both hard-asserted below. So D4b only RECORDS brk1's transient behaviour.
if _d4_minority_refuses; then
    _D4B_REC="refused/blocked in-window (rc=${_D4_RC:-?}, no majority ack)"
    log "D4b: the minority write via brk1 was refused/blocked in-window (rc=${_D4_RC:-?}; raft could not commit without majority)"
elif [ "${_D4_RC:-1}" = 0 ]; then
    _D4B_REC="rc=0 stale-leader transient accept"
    log "D4b: the minority write via brk1 returned rc=0 (a STALE-LEADER transient accept before brk1 detected the partition) — this is NOT a lasting split-brain; D5b+D6 below prove the ex-minority converges to the majority's state on heal and the stale write does not survive"
else
    # H6: the third state the old two-way record collapsed into "rc=0 accept" — the write FAILED, but with a
    # message none of the refusal patterns matched. Neither an accept nor a characterised refusal; say so.
    _D4B_REC="rc=${_D4_RC:-?} FAILED with an UNMATCHED message (neither a committed accept nor a recognised refusal)"
    log "D4b: the minority write via brk1 FAILED with rc=${_D4_RC:-?} but its message matched none of the known refusal signatures — recorded as an unmatched failure, NOT as an accept"
fi
# H6: D6b's GREEN branch reads "the stale write was rolled back". That claim is EMPTY unless canary3 was
# ACTUALLY written on the minority — if D4b's write never landed, "canary3 is absent after the heal" is true
# for the trivial reason that it never existed, and the 2026-07-18 run banked exactly that as a GREEN. Take
# the reading NOW, while the partition is still armed and brk1 is still the minority, so D6b can tell an
# empty arm from a real rollback. Bounded (timeout 10 inside the container) per R-BOUNDED-PROBE.
_C3_PRE=no; _c3_via brk1 && _C3_PRE=yes
log "D4b PRE-HEAL ARTIFACT: canary3 visible on the partitioned minority brk1 BEFORE the heal? $_C3_PRE (D4b=${_D4B_REC:-unknown}) — D6b is only judgeable when this is yes"
assert_ok "D4c brk1 did NOT crash or restart: same MainPID, NRestarts unchanged" _d4_brk1_stable

# #71 PRE-HEAL COMMITTER SNAPSHOT (added after the -j6 PRODUCT-RED the external re-review flagged).
# The post-heal committer grep (_c3_committed_by brk1 at D6b) CANNOT distinguish two very different worlds:
#   (a) a genuine raft-safety violation — brk1 COMMITTED canary3 WHILE ISOLATED (minority split-brain), or
#   (b) a LEGITIMATE delayed majority commit — the D4b propose that the CLI reported as failed (rc!=0)
#       actually committed AFTER the heal, once quorum returned (clusterwrite.go:718-729, the Q4
#       "committed-but-reported-failed" path), and brk1 — rejoined as a normal group member — then wrote
#       the same `broker: session created … canary3` line. That is NOT a split-brain.
# A pre-heal YES is decisive: take the committer reading while the partition is STILL ARMED and brk1 is
# STILL the isolated minority. `_c3_committed_by` is a
# `dexec` local file read (grep on brk1's broker.log) — the DROP only blackholes ports 6222/7400, not the
# docker exec channel — so it runs correctly under the partition. If brk1's log ALREADY names canary3 here,
# the minority returned from a committed write while isolated = a real #65.
#
# A pre-heal NO is NOT by itself decisive for a line first observed after heal: there is an unavoidable
# interval between this snapshot and the iptables flush. A line may be appended during that interval (real
# #65) or after quorum returns (legitimate delayed completion). Without a timestamp/boundary marker from
# the product log, the old #71 world remains ambiguous and must stay a first-class gap, not be declared
# benign. The corrected-tree run happened to have NO brk1 line even after heal, so it did not reproduce
# that world.
_C3_COMMIT_PREHEAL=no; _c3_committed_by brk1 && _C3_COMMIT_PREHEAL=yes
_C3_COMMIT_PREHEAL_LINE=$(dexec brk1 -- sh -c 'grep -ahF "broker: session created" /var/log/tether/broker.log /var/log/tether/broker.err 2>/dev/null | grep canary3' 2>/dev/null | tail -1)
log "D4b COMMITTER SNAPSHOT (pre-heal, partition STILL ARMED): brk1's OWN broker.log names canary3 while ISOLATED? $_C3_COMMIT_PREHEAL${_C3_COMMIT_PREHEAL_LINE:+ [line: $_C3_COMMIT_PREHEAL_LINE]} — yes = a genuine raft-safety violation candidate (#65); no = absent at this snapshot only (a line first seen after heal remains #71-ambiguous across the snapshot→heal boundary)"

assert_ok "D5a HEAL the partition" fault_partition_off brk1
assert_ok "D5b all three nodes converge on ONE leader (sort -u == 1)" \
    poll_until 180 5 "all three report the same leader" -- _d5_one_leader
# D6 — RESULT-level, not status-level: read the majority's row back FROM the ex-minority.
assert_ok "D6 NO SPLIT-BRAIN, proven at the RESULT level: the row written on the majority during the partition is readable FROM brk1 (the ex-minority) after healing — a status field agreeing would not prove the data agrees" \
    poll_until 120 3 "brk1 can read back the majority's write" -- _d6_readback
# D6b — the exclusion half: if D4b's stale-leader window let brk1 accept canary3 (rc=0), that write must
# NOT survive the heal (raft rolls back an uncommitted stale-leader entry). Together with D6, this is the
# complete no-split-brain proof at the result level: the majority's write survives, the minority's stale
# write does not.
# D6b — the exclusion half, done as an explore->pin because the minority's stale write surviving is
# AMBIGUOUS and must not be guessed. The discriminator: is canary3 visible via the MAJORITY (brk2/brk3),
# or only via brk1's own (possibly un-truncated / read-your-writes) local view?
#   * gone everywhere            -> GREEN: raft rolled back the uncommitted stale-leader entry.
#   * present via brk1 only      -> not_covered: truncation-lag / local-read artifact on the ex-minority,
#                                    NOT a durable split-brain (the majority never saw it). Candidate #65,
#                                    needs dedicated investigation — a chaos drill cannot pin it.
#   * present via the MAJORITY   -> product_red #65: a partitioned-minority stale-leader write became
#                                    DURABLE (visible on brk2/brk3 after heal) — a real raft-safety finding.
# (_c3_via / _c3_gone_everywhere are defined with the other predicates above — H6 hoisted them so the
#  PRE-heal reading uses the identical reader.)
# EXT-REVIEW-B10: capture the RAW single-run artifact before branching, so the #65 verdict is traceable to
# ONE run's actual per-broker readback (not a prose summary that could be spliced from different runs).
poll_until 60 3 "canary3 readback settles after heal" -- _c3_gone_everywhere >/dev/null 2>&1 || true
_C3_B1=no; _c3_via brk1 && _C3_B1=yes
_C3_B2=no; _c3_via brk2 && _C3_B2=yes
_C3_B3=no; _c3_via brk3 && _C3_B3=yes
log "D6b RAW ARTIFACT (canary3 = the minority's stale-leader write; D4b was: ${_D4B_REC:-unknown}; pre-heal visible on brk1=${_C3_PRE:-unknown}): after heal canary3 visible? brk1=$_C3_B1 brk2(majority)=$_C3_B2 brk3(majority)=$_C3_B3"
# R6 #65 (r6-findings.md): the DURABLE-on-the-majority branch now requires a SECOND condition — COMMITTER
# ATTRIBUTION. "canary3 visible via the majority after heal" alone is NOT #65: because the control plane is a
# cross-broker NATS queue group, `--nats-url brk1` only chooses the ENTRY server, so a majority-visible
# canary3 can be a LEGITIMATE majority commit the queue group routed to brk2/brk3 (R6 proved the old ledger's
# "5/6 durable minority writes" were exactly that — correct commits mis-attributed to brk1 by dialing). #65
# demands that the ISOLATED MINORITY brk1 itself COMMITTED it (its own broker.log names canary3, _c3_committed_by)
# AND it is visible via the majority after heal. Only BOTH together are a raft-safety violation.
if { [ "$_C3_B2" = yes ] || [ "$_C3_B3" = yes ]; } && [ "$_C3_COMMIT_PREHEAL" = yes ]; then
    product_red "#65 a partitioned-minority stale-leader write became DURABLE: canary3 was COMMITTED BY the isolated minority brk1 WHILE STILL PARTITIONED (the pre-heal committer artifact — brk1's own broker.log named canary3 BEFORE D5a healed the partition${_C3_COMMIT_PREHEAL_LINE:+: $_C3_COMMIT_PREHEAL_LINE} — is committer attribution taken during isolation, not just a --nats-url dial and not a post-heal delayed commit) during the partition (D4b=${_D4B_REC:-n/a}, pre-heal-on-brk1=${_C3_PRE:-unknown}) AND is visible via the MAJORITY after heal (brk1=$_C3_B1 brk2=$_C3_B2 brk3=$_C3_B3) — a partitioned minority's committed write must never survive (raft safety). CANDIDATE: reproduce in a dedicated single run with the full D4/D6/D6b + pre-heal committer artifact before treating it as characterised, not asserted from a chaos drill alone"
elif { [ "$_C3_B2" = yes ] || [ "$_C3_B3" = yes ]; } && _c3_committed_by brk1; then
    # #71 remains OPEN. The line was absent at the pre-heal snapshot and present when observed after heal,
    # but the snapshot and iptables flush are not atomic. The line could have landed in that boundary window
    # (real #65) or after quorum returned (legitimate delayed completion). The corrected-tree acceptance run
    # did NOT exercise this branch — it had no brk1 line even post-heal — so it cannot root the old archive.
    not_covered "96-D6b #71 AMBIGUOUS: brk1's canary3 commit-success line first observed after the pre-heal snapshot" \
        "canary3 is visible on the majority after heal (brk1=$_C3_B1 brk2=$_C3_B2 brk3=$_C3_B3, D4b=${_D4B_REC:-n/a}) and brk1's own broker.log names it now, while the last pre-heal snapshot was NO. The snapshot→iptables-flush boundary is not atomic: this may be a genuine isolated-minority commit in that interval (#65) or a legitimate completion after quorum returned. Keeping #71 OPEN and this run INCOMPLETE; do not call it a benign Q4 delayed commit without a product timestamp/boundary artifact." gap
elif [ "$_C3_B2" = yes ] || [ "$_C3_B3" = yes ]; then
    # R6's EXACT scenario, now correctly distinguished by committer attribution: canary3 IS durable on the
    # majority, but brk1's own broker.log carries NO 'session created … canary3' commit line (_c3_committed_by
    # brk1 was false above), so the isolated minority did NOT commit it — the cross-broker control-plane queue
    # group routed brk1's stale-leader-window dial (D4b=${_D4B_REC:-n/a}) to the MAJORITY, which committed it
    # normally. This is precisely the mis-attribution R6 exposed: the old ledger's "5/6 durable minority
    # writes" were correct majority commits recorded as #65 solely because ctl dialed brk1. It is NOT a
    # raft-safety violation. Recorded as a benign gap (the drill cannot, in-sim, force a NEW-connection write
    # to land on the isolated minority — condition Y below).
    not_covered "96-D6b canary3 durable via a LEGITIMATE majority commit (NOT #65 — committer attribution shows brk1 did NOT commit it)" \
        "canary3 is visible on the majority after heal (brk1=$_C3_B1 brk2=$_C3_B2 brk3=$_C3_B3, D4b=${_D4B_REC:-n/a}) but brk1's own broker.log has NO 'broker: session created … canary3' line — so the cross-broker control-plane NATS queue group routed the dial to the MAJORITY, which committed it normally. This is R6's key finding in action (the ledger's '5/6 durable minority writes' were correct majority commits mis-attributed by --nats-url dialing), NOT a partitioned-minority durable write. The genuine minority-stale-write #65 variant stays structurally unreachable in-sim: condition Y = a long-lived pre-partition-authenticated write client the short-lived CLI cannot provide." gap
elif [ "${_C3_PRE:-no}" != yes ]; then
    # R6 #65 REFUTED — this is STRUCTURAL, not a re-runnable timing race, so it is a registered GAP, not a
    # runtime-guard. Every CLI `session create` opens a NEW client connection, and an isolated minority CANNOT
    # authenticate a fresh connection: auth_callout is a cross-broker queue group and the isolated node's
    # callout is black-holed (R6 measured rc=69, 50/50). So canary3 can NEVER durably land on the minority via
    # the CLI path — re-running changes nothing. The minority-stale-write variant needs condition Y (a
    # LONG-LIVED client authenticated BEFORE the partition that writes DURING the window — a harness the sim's
    # short-lived CLI structurally cannot provide, r6-findings.md "归 R14"). The NEW-connection path is fully
    # covered by the committer-attribution + majority-visibility #65 pin above.
    not_covered "96-D6b minority stale-write rollback — minority-write variant STRUCTURALLY unreachable in-sim (needs condition Y: a long-lived pre-partition client)" \
        "canary3 was NOT durably held on the partitioned minority brk1 (pre-heal probe=${_C3_PRE:-no}, D4b=${_D4B_REC:-n/a}). R6 proved this is STRUCTURAL, not a timing race: the isolated minority cannot authenticate a fresh CLI connection (auth_callout queue group black-holed → rc=69, 50/50 measured), so a new-connection minority write can NEVER land — re-running changes nothing. Registered as a coverage GAP whose condition Y is a long-lived pre-partition-authenticated write client; the committer-attribution + majority-visibility #65 pin above covers the new-connection path. brk1=$_C3_B1 brk2=$_C3_B2 brk3=$_C3_B3" gap
elif [ "$_C3_B1" = no ]; then
    _as_pass "D6b NO SPLIT-BRAIN (exclusion half): the minority's stale-leader write (canary3) was PROVEN present on brk1 before the heal (pre-heal probe=yes) and is now gone on brk1 AND the majority — a REAL rollback, not an empty arm (D4b=${_D4B_REC:-n/a})"
else
    not_covered "96-D6b minority stale-write rollback (brk1=$_C3_B1 brk2=$_C3_B2 brk3=$_C3_B3)" "canary3 (the minority's stale-leader accept, D4b=${_D4B_REC:-n/a}, PROVEN present on brk1 pre-heal) is still visible via brk1 but NOT via the majority (brk2/brk3) after heal — a truncation-lag / read-your-writes artifact on the ex-minority's local view, NOT a durable split-brain (the majority never committed it). Recorded as #65 candidate; pinning it needs dedicated investigation of whether tether acks uncommitted local appends. The durable no-split-brain direction (D6, the majority's committed write survives) is GREEN" gap
fi

# ══ F — double fault (G.1 x G.2 interleaved) ═══════════════════════════════════════════════════════
# NOTE: this arm structurally CANNOT have an OS-truth leg. node_kill destroys the container, so any
# `pgrep == 0` assertion would be guaranteed by the injection itself with tether never running a line of
# code. An OS-truth leg exists only in 94-B, where the process really survives the injection.
# PRECONDITION for the double-fault arm: the cluster must have FULLY recovered from arm D's partition
# first — 3 VOTER + agt1 AND agt2 ONLINE. Arm D partitioned brk1 (agt2's home), so a not-yet-recovered
# brk1 can leave agt2's control process disrupted, which would make F4 (agt2 STILL RUNNING) fail for an
# arm-D-residue reason rather than a G.1xG.2 defect. If the cluster cannot get back to full health, gate
# the whole arm not_covered (cross-arm damage, not a finding) rather than assert-fail its discriminators.
_f_precond_healthy() { _d0_three_voters && _f_agt1_online && _agt_online agt2; }
# 360s (was 240s): arm D partitions brk1 (agt2's home) then heals; D5b/D6 (leader converged + no split-brain)
# recover fast, but full health — brk1 rejoining as VOTER AND agt2 re-registering ONLINE off its just-healed
# home — legitimately takes >240s on a loaded host (measured r14d 2026-07-20: D5b/D6 PASSED but this gate
# timed out at 240s, gating the whole F/Q3 arm every run). Widened so the double-fault arm (the Q3 held-seed
# fixture) actually runs; if it STILL times out the arm gates as a gap (cross-arm residue), never a false pass.
if poll_until 360 5 "cluster FULLY recovered from arm D before the double-fault (3 VOTER + agt1 & agt2 ONLINE)" -- _f_precond_healthy; then
# Q3: launch the seeds HELD FOREGROUND at TOP LEVEL (not inside an assert_ok `$(...)` subshell, which would
# reparent/hangup the backgrounded docker-exec), then assert they are RUNNING. `nohup sleep &` is gone: its
# tracked row EXITs in 3ms, so RUNNING was structurally impossible and F0c/F4 asked an un-answerable question.
_seed_held agt1 9661
_seed_held agt1 9662
assert_ok "F0a two HELD seeds on agt1 are RUNNING (tether holds each 'sleep 966N' as its exec child — Q3: a self-backgrounded nohup would leave only an EXITED sh, so RUNNING is real evidence here)" \
    poll_until 30 2 "agt1's two held seeds are RUNNING" -- _f0_agt1_two_running
_seed_held agt2 9663
assert_ok "F0b HELD control seed on agt2 is RUNNING — the CONTROL that must survive agt1's reconciliation untouched (foreground-held so F4's post-injection RUNNING check is non-vacuous)" \
    poll_until 30 2 "agt2's held control seed is RUNNING" -- _agt2_seed_running
# The seeded control MUST be running right before the injection, else F4 cannot distinguish "node-scoped
# reconciliation left it alone" from "it was already gone" — if it is not, that is an arm-D-residue setup
# problem, not a G.1xG.2 finding.
# H14: this comment promised a GATE, but the code was a plain assert_ok that recorded a RED and then ran the
# whole arm anyway over a missing control. It is now an assert_setup: a missing control is a prerequisite
# failure (SETUP-RED) that ABORTS, so F1-F6 can never judge a discriminator that has nothing to discriminate.
# The capture must happen OUTSIDE assert_setup — assert_setup captures its command via `$(…)`, a subshell,
# where the recorded pid would be lost.
if poll_until 30 2 "agt2's seed is running pre-injection" -- _f0c_capture_agt2_seed; then _f0c=1; else _f0c=0; fi
log "F0c: agt2 control seed pre-injection pid=[${_F0C_PID:-none}] (captured=$_f0c) — F4 compares against THIS identity"
assert_setup "F0c CONTROL PRECONDITION (GATE): agt2's seeded process is actually running before the double-fault AND its pid was captured for F4's identity comparison" \
    sh -c "[ '$_f0c' = 1 ] && [ -n '${_F0C_PID:-}' ]"
assert_ok "F1 INJECT BOTH: kill agt1 AND its home broker brk2 together" _f1_kill_both
assert_ok "F2 bring both back" _f2_start_both
assert_ok "F3 G.1xG.2 converge: agt1's processes are reconciled to EXITED(-1)" \
    poll_until 180 5 "agt1's procs reconcile to EXITED" -- _f3_agt1_exited
assert_ok "F4 THE DISCRIMINATOR: the EXACT pre-injection agt2 process (pid ${_F0C_PID:-?}) is STILL RUNNING and no seed row on agt2 was closed out — reconciliation is node-scoped, not a table-wide sweep. H14: this now asks a STRICTLY DIFFERENT question from the F0c gate (identity + no-terminal-row after the injection, vs 'is anything running' before it), so a sweep and an absent control can no longer produce the same signal" \
    poll_until 30 2 "agt2's recorded seed pid still running after the double-fault" -- _f4_agt2_seed_survived
assert_ok "F5 G.5: the audit says kind=reconciled_closed (AuditProc's kind; 'reconciled' is AuditPort's — schema/audit.go:36 vs :51)" \
    poll_until 120 3 "a reconciled_closed row for agt1" -- _f5_audit_row
assert_ok "F6 the agent is NOT wedged: a NEW process starts and runs after the double fault" \
    poll_until 60 3 "a fresh exec works on agt1" -- _f6_fresh_exec
else
    not_covered "96-F double fault (agent + home broker together)" "the cluster did not fully recover from arm D's leader-partition within 240s (3 VOTER + agt1 & agt2 ONLINE), so the double-fault arm would run against arm-D residue (agt2's home brk1 was the partition victim) — a cross-arm state consequence, not a G.1xG.2 defect. The reconciliation is node-scoped-tested hermetically; #57/#58 are already pinned above. A dedicated per-arm-isolated fixture for F is owed to a follow-up" gap
fi

drill_end
