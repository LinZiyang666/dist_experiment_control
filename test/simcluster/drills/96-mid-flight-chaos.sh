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
# #58  The cleanup is not the watchdog's job at all — it is the boot reconciler
#      reconcileXferObjectsOnBoot (transfer_reconcile.go:27-94), called ONCE from broker.go:942 at startup
#      with no periodic pass, and its FIRST gate is `if !b.reaperMayDelete() { return }` (:34-36), which in
#      cluster mode is false for a non-leader (clusterwrite.go:478-486). => orphaned transfer objects on a
#      non-leader home broker are never reaped.
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


# ── predicates (FUNCTIONS — R-NOSHC) ────────────────────────────────────────────────────────────────
_hist_xfer() { _pctl -- history --kind transfer -n 200 2>/dev/null; }  # bounded read (EXT-REVIEW hang fix)
_a0_pair_visible() { _h=$(_hist_xfer); printf '%s' "$_h" | grep -q 'start' && printf '%s' "$_h" | grep -q 'complete'; }
# The in-flight transfer uses a DISTINCT SOURCE path (/tmp/inflight.bin) so it is identifiable in history
# by path= (which records the AGENT-SIDE SOURCE, not the ctl-side dest — measured). A0's control uses
# /tmp/big.bin, so the two never collide.
_a1_start_bg_pull() {
    dexec agt1 -- cp /tmp/big.bin /tmp/inflight.bin >/dev/null 2>&1 || return 1
    dexec -u sim ctl1 -- sh -c "nohup env HOME=/home/sim tether pull agt1:/tmp/inflight.bin /tmp/inflight.back --nats-url $NURL >/tmp/96-pull.log 2>&1 & echo started" >/dev/null 2>&1
}
_a1_start_row() { _hist_xfer | grep -F 'inflight.bin' | grep -q 'kind=start'; }
_a1_terminal_row() { _hist_xfer | grep -F 'inflight.bin' | grep -qE 'kind=complete|kind=failed'; }
_a1e_control_after() {
    dexec agt1 -- sh -c 'printf tiny-control-payload > /tmp/tiny.bin' >/dev/null 2>&1 || return 1
    "$SIM" ctl -- pull agt1:/tmp/tiny.bin /tmp/tiny-out.bin >/dev/null 2>&1 || return 1
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
_f_agt1_online() { _pctl -- node ls --json 2>/dev/null | jq -e '.nodes[]?|select(.nid=="agt1")|select(.status=="ONLINE")' >/dev/null 2>&1; }
_agt_online() { _pctl -- node ls --json 2>/dev/null | jq -e --arg n "$1" '.nodes[]?|select(.nid==$n)|select(.status=="ONLINE")' >/dev/null 2>&1; }
_f0_seed_agt1() {
    _pctl -- exec agt1 -- sh -c 'nohup sleep 9661 >/dev/null 2>&1 & echo started' >/dev/null 2>&1 || return 1
    _pctl -- exec agt1 -- sh -c 'nohup sleep 9662 >/dev/null 2>&1 & echo started' >/dev/null 2>&1
}
_f1_kill_both() { node_kill agt1; node_kill brk2; return 0; }
_f2_start_both() { node_start brk2; node_start agt1; return 0; }
_f3_agt1_exited() {
    _pctl -- ps -a --json 2>/dev/null \
        | jq -e '[.processes[]?|select(.nid=="agt1")|select(.argv|join(" ")|test("sleep 966[12]"))|select(.status=="EXITED")]|length==2' >/dev/null 2>&1
}
_f4_agt2_running() {
    _pctl -- ps -a --json 2>/dev/null \
        | jq -e '[.processes[]?|select(.nid=="agt2")|select(.argv|join(" ")|test("sleep 9663"))|select(.status=="RUNNING")]|length==1' >/dev/null 2>&1
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
    "arm B's kill-broker-mid-run outcome is GREEN-by-design (run.go's 15s liveness watchdog synthesises 'agent unreachable: no heartbeat' — SB-96-3, source-closed); arm C's crash-strand data plane is drill 71/#29's territory (a cluster expose home cannot deliver to a non-tunnel broker, home.go:96-113). The rehome/home_reassign_failed event is member-readable (rehome_events.go:9-11 → SubjSysEvents) but its crash-path firing needs arm C's fixture; DOC-28 is ledger-registered"

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
    poll_until 60 3 "the in-flight transfer's start row appears" -- _a1_start_row
assert_ok "A1c INJECT: docker kill brk2 — the agent's home AND a guaranteed NON-leader (so #58's reaper gate is false by source)" node_kill brk2

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
    not_covered "96-A #57 in-flight interruption (audit unreadable this run)" \
        "no transfer row was readable during the window even though the reader is proven working (A0c control pairs start+complete, A1b saw this transfer's start row before the kill) — the home broker brk2 is down for the whole 390s window so the crashed transfer's audit is not queryable now. #57's mechanism is source-certain (watchdog hangs off broker runCtx transfer.go:593/:704, tracker rebuilds EMPTY broker.go:602); catching the dangling row in-sim is non-deterministic. hermetic owner: the transfer unit tests"
elif printf '%s' "$_A_ROWS" | grep -qE 'complete|failed'; then
    not_covered "96-A #57 in-flight interruption" "the tier-B transfer reached a terminal (complete) row — on the sim's loopback network an 80 MiB transfer often FINISHES before brk2 can be killed, so we did not actually catch it in-flight. #57's mechanism is source-certain (the watchdog hangs off broker runCtx transfer.go:593/:704, dies with the process; the tracker rebuilds EMPTY broker.go:602), but reliably interrupting a loopback-speed transfer is non-deterministic in-sim. hermetic owner: the transfer unit tests"
elif printf '%s' "$_A_ROWS" | grep -q 'start'; then
    _A_57_PINNED=1
    product_red "#57 an in-flight tier-B transfer whose home broker crashes leaves a DANGLING start row and NO terminal audit, forever: the watchdog hangs off the broker's runCtx (transfer.go:593/:704) so it dies with the process, transferTracker is an in-memory map rebuilt EMPTY on restart (transfer.go:99-104 + broker.go:602), and any late finalization from the agent is silently dropped by handleEvTransfer's 'preview == nil -> return' (:816-819). Operators auditing transfers see a transfer that never ended."
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
        "cannot distinguish a reaped-empty bucket from one holding an orphan object, so #58 cannot be judged this run. Its mechanism is source-certain (reconcileXferObjectsOnBoot's first gate 'if !b.reaperMayDelete() { return }', transfer_reconcile.go:34, is false for any non-leader; the victim is a non-leader by setup) — retained SOURCE-CONFIRMED, live object-level measurement owed to a run where /jsz exposes the object-store messages"
elif [ "$_C_ORPHAN" -le "$_B58" ]; then
    not_covered "96-A2 (#58) no orphan object was manufactured (count $_C_ORPHAN <= clean baseline $_B58)" \
        "the interrupted tier-B transfer left no object above the clean floor — on the loopback network it completed too fast to strand one (the same non-determinism as #57). There is nothing for the reaper to fail on, so #58 is not judged rather than guessed"
else
    # The orphan IS present (count $_C_ORPHAN > baseline $_B58) and brk2 is still down. Restart brk2 so its
    # boot reconciler runs, then judge whether that SPECIFIC orphan survived.
    assert_ok "A2a bring brk2 back so its boot reconciler runs (orphan object count $_C_ORPHAN > baseline $_B58 while it was down)" node_start brk2
    assert_ok "A2b brk2's broker is up again" poll_until 120 3 "brk2 broker active" -- _a2_brk2_up
    _C_AFTER=$(_xfer_obj_count brk1)
    log "A2-post #58: OBJ_xfer object count after brk2's boot reconciler ran = $_C_AFTER (orphan was $_C_ORPHAN, baseline $_B58)"
    if [ "$_C_AFTER" = unreadable ]; then
        not_covered "96-A2 (#58) object count became unreadable after the restart" "cannot judge whether the orphan was reaped"
    elif [ "$_C_AFTER" -gt "$_B58" ]; then
        product_red "#58 orphaned tier-B transfer objects are NEVER reaped on a non-leader home broker: after brk2 (a NON-leader BY SETUP, so reaperMayDelete()==false is a source guarantee — transfer_reconcile.go:34-36, clusterwrite.go:478-486) restarts and runs its boot reconciler ONCE (broker.go:942, no periodic pass), the OBJ_xfer object count stayed at $_C_AFTER, still ABOVE the clean baseline $_B58 (the orphan measured $_C_ORPHAN while brk2 was down). These accumulate against the per-session 8 GiB bucket cap (transfer.go:67) = a #21-family relapse."
    elif dexec brk2 -- sh -c 'grep -q "orphan xfer objects reaped" /var/log/tether/broker.err' 2>/dev/null; then
        _as_fail "#58 APPEARS FIXED — the boot reaper DID reap the orphan on a non-leader (object count dropped $_C_ORPHAN->$_C_AFTER back to baseline $_B58 AND 'orphan xfer objects reaped' in broker.err). The reaperMayDelete gate no longer blocks it; flip the ledger"
    else
        not_covered "96-A2 (#58) the orphan object is gone (count $_C_ORPHAN->$_C_AFTER back to baseline $_B58) but NOT via the boot reaper (no 'orphan xfer objects reaped' line)" "something else removed it and the root cause is undetermined; judging #58 either way would be a guess"
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
    not_covered "96-B0 run --ack-alerts gate (inventory row 122's S9 cell)" "run was NOT refused under the alert state produced by this drill's kill (rc=$_B0_RC), so there is no gate to prove bypassing here; the semantics differ from 90's severe-banner path and need their own explore->pin"
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
assert_ok "D3 THE MAJORITY IS REALLY ALIVE: a real WRITE succeeds on the survivor side (the only legitimate proof of quorum — not a status field)" \
    poll_until 300 5 "a real write commits on the survivors (JS meta re-forms 2/3 quorum after losing brk1 — legitimately slow, and slower still on a loaded host; OQ-7 tolerant window widened from 150s after a re-run flake)" -- _d3_survivor_write
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
    _D4B_REC="refused/blocked in-window (no majority ack)"
    log "D4b: the minority write via brk1 was refused/blocked in-window (raft could not commit without majority)"
else
    _D4B_REC="rc=0 stale-leader transient accept"
    log "D4b: the minority write via brk1 returned rc=0 (a STALE-LEADER transient accept before brk1 detected the partition) — this is NOT a lasting split-brain; D5b+D6 below prove the ex-minority converges to the majority's state on heal and the stale write does not survive"
fi
assert_ok "D4c brk1 did NOT crash or restart: same MainPID, NRestarts unchanged" _d4_brk1_stable

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
_c3_via() { dexec -u sim ctl1 -- env HOME=/home/sim timeout 10 tether session ls --json --nats-url "nats://$1:4222" 2>/dev/null | jq -e '.sessions[]?|select(.name=="canary3")' >/dev/null 2>&1; }
_c3_gone_everywhere() { ! _c3_via brk1 && ! _c3_via brk2 && ! _c3_via brk3; }
# EXT-REVIEW-B10: capture the RAW single-run artifact before branching, so the #65 verdict is traceable to
# ONE run's actual per-broker readback (not a prose summary that could be spliced from different runs).
poll_until 60 3 "canary3 readback settles after heal" -- _c3_gone_everywhere >/dev/null 2>&1 || true
_C3_B1=no; _c3_via brk1 && _C3_B1=yes
_C3_B2=no; _c3_via brk2 && _C3_B2=yes
_C3_B3=no; _c3_via brk3 && _C3_B3=yes
log "D6b RAW ARTIFACT (canary3 = the minority's stale-leader write; D4b was: ${_D4B_REC:-unknown}): after heal canary3 visible? brk1=$_C3_B1 brk2(majority)=$_C3_B2 brk3(majority)=$_C3_B3"
if [ "$_C3_B1" = no ] && [ "$_C3_B2" = no ] && [ "$_C3_B3" = no ]; then
    _as_pass "D6b NO SPLIT-BRAIN (exclusion half): the minority's stale-leader write (canary3) was rolled back — gone on brk1 AND the majority after heal (D4b=${_D4B_REC:-n/a})"
elif [ "$_C3_B2" = yes ] || [ "$_C3_B3" = yes ]; then
    product_red "#65 a partitioned-minority stale-leader write became DURABLE: canary3, accepted by the isolated minority brk1 during the partition (D4b=${_D4B_REC:-n/a}), is visible via the MAJORITY after heal (brk1=$_C3_B1 brk2=$_C3_B2 brk3=$_C3_B3) — a partitioned minority's write must never survive (raft safety). CANDIDATE: this is a raft-safety-level claim; it must be reproduced in a dedicated single run with the full D4/D6/D6b artifact before it is treated as characterised, not asserted from a chaos drill alone"
else
    not_covered "96-D6b minority stale-write rollback (brk1=$_C3_B1 brk2=$_C3_B2 brk3=$_C3_B3)" "canary3 (the minority's stale-leader accept, D4b=${_D4B_REC:-n/a}) is still visible via brk1 but NOT via the majority (brk2/brk3) after heal — a truncation-lag / read-your-writes artifact on the ex-minority's local view, NOT a durable split-brain (the majority never committed it). Recorded as #65 candidate; pinning it needs dedicated investigation of whether tether acks uncommitted local appends. The durable no-split-brain direction (D6, the majority's committed write survives) is GREEN"
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
if poll_until 240 5 "cluster FULLY recovered from arm D before the double-fault (3 VOTER + agt1 & agt2 ONLINE)" -- _f_precond_healthy; then
assert_ok "F0a seed two processes on agt1" _f0_seed_agt1
assert_ok "F0b seed one on agt2 — the CONTROL that must survive agt1's reconciliation untouched" \
    _pctl -- exec agt2 -- sh -c 'nohup sleep 9663 >/dev/null 2>&1 & echo started'
# The seeded control MUST be running right before the injection, else F4 cannot distinguish "node-scoped
# reconciliation left it alone" from "it was already gone" — if it is not, that is an arm-D-residue setup
# problem, gated below, not a G.1xG.2 finding.
assert_ok "F0c CONTROL PRECONDITION: agt2's seeded process is actually running before the double-fault" \
    poll_until 30 2 "agt2's seed is running pre-injection" -- _f4_agt2_running
assert_ok "F1 INJECT BOTH: kill agt1 AND its home broker brk2 together" _f1_kill_both
assert_ok "F2 bring both back" _f2_start_both
assert_ok "F3 G.1xG.2 converge: agt1's processes are reconciled to EXITED(-1)" \
    poll_until 180 5 "agt1's procs reconcile to EXITED" -- _f3_agt1_exited
assert_ok "F4 THE DISCRIMINATOR: agt2's process is STILL RUNNING — reconciliation is node-scoped, not a table-wide sweep (the only assertion that can tell those apart)" \
    poll_until 30 2 "agt2's seed still running after the double-fault" -- _f4_agt2_running
assert_ok "F5 G.5: the audit says kind=reconciled_closed (AuditProc's kind; 'reconciled' is AuditPort's — schema/audit.go:36 vs :51)" \
    poll_until 120 3 "a reconciled_closed row for agt1" -- _f5_audit_row
assert_ok "F6 the agent is NOT wedged: a NEW process starts and runs after the double fault" \
    poll_until 60 3 "a fresh exec works on agt1" -- _f6_fresh_exec
else
    not_covered "96-F double fault (agent + home broker together)" "the cluster did not fully recover from arm D's leader-partition within 240s (3 VOTER + agt1 & agt2 ONLINE), so the double-fault arm would run against arm-D residue (agt2's home brk1 was the partition victim) — a cross-arm state consequence, not a G.1xG.2 defect. The reconciliation is node-scoped-tested hermetically; #57/#58 are already pinned above. A dedicated per-arm-isolated fixture for F is owed to a follow-up"
fi

drill_end
