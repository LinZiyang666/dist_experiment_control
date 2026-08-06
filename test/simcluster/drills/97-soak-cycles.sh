#!/bin/sh
# 97-soak-cycles — S9 / G-C. N=3: the parameterised soak. P8's exit prototype was "24h + chaos every
# hour"; this is its SCALED STAND-IN, plus the fd/RSS leak oracle a log grep structurally cannot provide.
# Plan: docs/reviews/s7-s9-plan.md §3.4. Expected landing: GREEN (thresholds UNCALIBRATED).
# Runtime ~16min at SOAK_CYCLES=6. Topology: 3 brokers + 2 agents + 1 ctl (grow-family).
#
# ── PARAMETERS ──────────────────────────────────────────────────────────────────────────────────────
#   SOAK_CYCLES=6   the default is the STRUCTURAL FLOOR of the slope test, not a taste: below 6 samples
#                   the slope judgement is not meaningful and the arm degrades to not_covered every run.
#   SOAK_SETTLE=25  seconds to let each cycle reach steady state before sampling.
# Release-time deep run: SOAK_CYCLES=48 SOAK_SETTLE=90 ./remote.sh drill 97-soak-cycles  (~24h-ish).
#
# ── WHY THE MEASURED PROCESS IS NEVER THE VICTIM (the arm is otherwise incapable of failing) ────────
# If a cycle kills the process we sample, its fd count resets to zero every cycle and no leak can EVER
# show. So brk1 (the leader) and agt2 are NEVER victims, and their MainPID generation is asserted
# unchanged across all N cycles. A changed MainPID means the series was reset -> the run CANNOT judge and
# records not_covered rather than a meaningless green.
#
# ── WHY A JOURNAL GREP CANNOT DO THIS (the roadmap asks for the argument) ───────────────────────────
# journald records EVENTS. An fd leak emits ZERO log lines until it hits the rlimit — six cycles leaking
# a few fd each sit 3-5 orders of magnitude below LimitNOFILE, so the log is literally empty. RSS is the
# same until the OOM killer writes one kernel line, at which point it is an incident, not a detection.
# Slow leaks are only visible as a TREND in sampled counters. The panic/FK scan below therefore stays a
# SEPARATE, orthogonal crash/integrity oracle; the two never substitute for each other.
#
# ── goroutines: NOW COVERED (R13) via `admin runtime` — the gap is RETIRED, not registered ─────────
# Until R13 this was a PERMANENT NOT-COVERED: the product exposed no goroutine gauge, the hermetic suites
# call runtime.NumGoroutine() from INSIDE the process (a cross-process drill cannot), and /proc Threads is
# OS threads (M) NOT goroutines (G) — using Threads as a proxy would itself be a false-green oracle (10k
# parked leaked goroutines add ZERO OS threads). R13 added the broker-local admin verb `tether admin
# runtime --json`, whose `.goroutines` is runtime.NumGoroutine() — the IN-PROCESS TRUTH, reachable from a
# cross-process drill over the root/tether-owned 0600 admin socket. So this drill now carries a REAL
# goroutine leak gate (the GOROUTINE arm below); the plan §5.1 goroutine carve-out is retired drill-side
# (the doc owes the same close — reported to the owner). The Threads dimension of the fd/RSS oracle stays:
# goroutines (G) and Threads (M) are DIFFERENT quantities and neither substitutes for the other.
#
# ── THE GOROUTINE GATE CONTRACT (FIXED AT PLAN STAGE — never tuned at runtime; total mandate) ────────
# Predicate: sample brk1's goroutine FLOOR before the soak (pre-load baseline), run the 6-cycle chaos soak
# as the load, QUIESCE, then re-sample the floor; PASS iff post_floor <= pre_floor + GOR_TOL. A healthy
# process returns to its baseline once handler goroutines return; a leak (goroutines that never exit) keeps
# the post floor elevated by the leaked count, which ACCUMULATES across the 6 chaos cycles. brk1 is the
# same never-victim, MainPID-guarded process the fd/RSS oracle samples, so a restart (which would reset the
# count) invalidates the compare exactly as it does for fd/RSS. The THREE contract constants + derivations,
# fixed HERE so no run can move them to force green:
#   GOR_TOL          = 2*NPROC + 16   the max goroutines the quiesced floor may sit above the pre-load
#                                     floor. DERIVATION: brk1 runs a FIXED set of long-lived goroutines
#                                     (Run loop, the single R7a reconcile ticker, the NATS client's
#                                     read/flush/ping, the adminsock accept loop, the http listeners). The
#                                     only legitimate floor-to-floor variation is GC assist/mark workers
#                                     (~GOMAXPROCS=NPROC) plus a few transient scheduler goroutines.
#                                     Sampling the FLOOR (min over GOR_*_SAMPLES) already removes most GC
#                                     jitter, so the residual is small; 2*NPROC+16 is that residual WIDENED
#                                     — the same nproc-scaled shape as leak.sh's Threads high-water
#                                     (`th0 + 2*nproc + 16`). [UNCALIBRATED] — a RED is triaged as
#                                     jitter-or-leak FIRST; the threshold is NEVER what moves to make a run
#                                     green. A per-cycle goroutine leak in the broker's hot paths would
#                                     accumulate FAR past this over 6 cycles; a leak slower than the
#                                     tolerance stays owed to the release-time deep run (SOAK_CYCLES=48),
#                                     the same carve-out as the fd/RSS slope.
#   GOR_PRE_SAMPLES  = 3              samples for the pre-load floor. DERIVATION: 3 gives a floor (min)
#   GOR_POST_SAMPLES = 3              robust to a single transient spike; more only slows the drill (matches
#                                     the mean-of-3 window leak.sh uses for fd/RSS).
#   GOR_QUIESCE      = SOAK_SETTLE(25s) the judgment window between the last load and the post sample.
#                                     DERIVATION: goroutine reclamation is PROMPT — a handler goroutine
#                                     exits the instant its function returns (not on the next GC), so one
#                                     settle window is ample for the count to fall back. Reusing SOAK_SETTLE
#                                     keeps the quiesce identical to the per-cycle settle the fd/RSS oracle
#                                     uses.
# NON-VACUITY: the JUDGE is a pure integer predicate `_gor_within_tol <floor> <post>`; tests/r9d-nonvacuity
# drives it two-sided (a within-tolerance post PASSES, a leaked post 3x the floor REDS, an empty post fails
# closed). In-run: the SAME brk1 process is sampled pre and post, so equal floors prove NO leak while a real
# leak would push the post floor out of tolerance — not a run that could pass whatever the product did.
#
# ── FALSE-GREEN RISK HEADNOTE ───────────────────────────────────────────────────────────────────────
#  1. A loop that does nothing obviously does not leak. EVERY cycle asserts the injection really took AND
#     the recovery really happened; a failure there is an ASSERT-FAIL, never the leak oracle's problem.
#  2. t0 is sampled AFTER warm-up, never at boot: lazy init makes early counts artificially low and would
#     manufacture a false RED.
#  3. RSS uses slope + a relative ceiling only — never an absolute high-water: Go's GC sawtooth would fire
#     on a perfectly healthy process.
#  4. The MainPID is RE-RESOLVED at every sample, never cached: after a restart a cached pid samples a
#     dead (or recycled) process.
#  5. Thresholds are [UNCALIBRATED] and deliberately widened 4x. epsilon is a HARNESS parameter, not a
#     product promise; a RED is triaged as jitter-or-leak FIRST. The threshold is never the thing that
#     moves to make a run green.
#  6. One bad cycle poisons the landing verdict BY DESIGN. "Report only the last cycle" / "per-cycle
#     verdicts" are forbidden.

set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/drills/lib/cluster.sh"
. "$HERE/drills/lib/logs.sh"
. "$HERE/drills/lib/dataplane.sh"
. "$HERE/drills/lib/agentyaml.sh"
. "$HERE/drills/lib/fault.sh"
. "$HERE/drills/lib/leak.sh"
. "$HERE/lib/assert.sh"

: "${SOAK_CYCLES:=6}"
: "${SOAK_SETTLE:=25}"
# ── goroutine gate contract (see the header block; FIXED here, never tuned at runtime) ──────────────
# GOR_TOL is nproc-derived and set just before the gate (NPROC is resolved after the loop); the two
# sample counts + the quiesce window are the plan-fixed constants.
: "${GOR_PRE_SAMPLES:=3}"
: "${GOR_POST_SAMPLES:=3}"
: "${GOR_QUIESCE:=$SOAK_SETTLE}"

SID=lab
PIN=979797
NURL="nats://brk1:4222"
SER_B=/tmp/97-brk1.series
SER_A=/tmp/97-agt2.series
# Go panics / raft FK violations / SQLite corruption. An ORTHOGONAL oracle: it never substitutes for the
# leak judgement and the leak judgement never substitutes for it.
JOURNAL_BAD_SIG='panic: |goroutine [0-9]+ \[running\]:|FOREIGN KEY constraint failed|WARNING: DATA RACE|database disk image is malformed'

_bt() { _btn=$1; shift; [ "$1" = "--" ] && shift; dexec -u tether "$_btn" -- "$@"; }
_leader_is_brk1() { _bt brk1 -- timeout 10 tether cluster status --json 2>/dev/null | jq -e '.leader_id=="brk1"' >/dev/null 2>&1; }
_agt_online() { "$SIM" ctl -- node ls --json 2>/dev/null | jq -e --arg n "$1" '.nodes[]?|select(.nid==$n)|select(.status=="ONLINE")' >/dev/null 2>&1; }
_agt1_online() { _agt_online agt1; }
_agt1_offline() { ! _agt_online agt1; }
_one_leader() {
    # EXT-REVIEW-B6: consensus after a heal = all THREE brokers answer with a non-empty leader AND all name
    # the SAME one. The old form only did `sort -u | wc -l == 1`, which passes when two brokers error and a
    # single survivor answers — that is a lone survivor, not consensus, so the partitioned side could still
    # be down and this would go GREEN.
    _ol=$( { _bt brk1 -- timeout 10 tether cluster status --json 2>/dev/null | jq -r '.leader_id // empty';
             _bt brk2 -- timeout 10 tether cluster status --json 2>/dev/null | jq -r '.leader_id // empty';
             _bt brk3 -- timeout 10 tether cluster status --json 2>/dev/null | jq -r '.leader_id // empty'; } )
    [ "$(printf '%s\n' "$_ol" | grep -v '^$' | wc -l | tr -d ' ')" -eq 3 ] || return 1
    [ "$(printf '%s\n' "$_ol" | grep -v '^$' | sort -u | wc -l | tr -d ' ')" -eq 1 ]
}
_brk3_pid() { leak_pid brk3 tether-broker; }
_biz_ok() { "$SIM" ctl -- exec agt2 -- echo SOAK-ALIVE >/dev/null 2>&1; }
# Scoped to THIS cycle's unique source path (Stage-C minor 7): a bare complete|failed over all history
# would match a prior cycle's leftover when SOAK_CYCLES is large. _SOAK_XFER_SRC is set per type-3 cycle.
_xfer_terminal() { "$SIM" ctl -- history --kind transfer -n 100 2>/dev/null | grep -F "${_SOAK_XFER_SRC:-soak.bin}" | grep -qE 'kind=complete|kind=failed'; }
# The injection's non-vacuity = the transfer was really ATTEMPTED (a row for THIS cycle's unique source
# appears), NOT that it terminated: a tier-B transfer whose broker is restarted mid-flight can legitimately
# DANGLE with no terminal row (that is the chaos itself, and exactly #57's dangling-transfer phenomenon).
# 97's deliverable is the leak oracle, so requiring completion here would fail on the very disruption the
# cycle injects.
_xfer_started() { "$SIM" ctl -- history --kind transfer -n 100 2>/dev/null | grep -qF "${_SOAK_XFER_SRC:-soak.bin}"; }
# h1 F3: this oracle asks "is there ANY crash/integrity signature in the
# broker's output", and after h1 that output is SPLIT — panics and data races
# go to journald (raw fd 2), while FK violations and DB-corruption slog lines
# go to broker.log. Merging is correct HERE precisely because the question is
# stream-agnostic; the slog-vs-panic assertions elsewhere must NOT merge, or a
# panic line could satisfy a slog expectation.
_brk_err_clean() {
    ! sim_broker_slog_grep "$1" "$JOURNAL_BAD_SIG" && ! sim_broker_panic_journal "$1" "$JOURNAL_BAD_SIG"
}
# Same split on the agent side: slog -> agent.log, panics -> agent.boot.err
# (dup2'd fd 2) with the journal still carrying pre-logger boot output.
_agt_journal_clean() {
    ! sim_agent_slog_grep "$1" "$JOURNAL_BAD_SIG" &&
        ! sim_agent_panic_sink "$1" "$JOURNAL_BAD_SIG" &&
        ! dexec "$1" -- sh -c "journalctl -u tether-agent --no-pager 2>/dev/null | grep -qE '$JOURNAL_BAD_SIG'"
}

# ── goroutine leak gate helpers (R13) — admin runtime .goroutines is runtime.NumGoroutine() (the
#    IN-PROCESS TRUTH), NOT /proc Threads. Broker-local admin socket, run ON the broker as tether. ──────
# one goroutine sample from <brk> (empty on any failure — fail-closed, never a fabricated 0).
_gor_one() { _bt "$1" -- timeout 10 tether admin runtime --json 2>/dev/null | jq -e '.goroutines' 2>/dev/null; }
# floor over <n> samples 3s apart: the MIN (steady-state floor rejects a transient spike). Empty if none read.
_gor_floor() {
    _gf_min=''; _gf_i=0
    while [ "$_gf_i" -lt "$2" ]; do
        _gf_v=$(_gor_one "$1")
        if [ -n "$_gf_v" ] && [ "$_gf_v" -gt 0 ] 2>/dev/null; then
            { [ -z "$_gf_min" ] || [ "$_gf_v" -lt "$_gf_min" ]; } && _gf_min=$_gf_v
        fi
        _gf_i=$((_gf_i + 1)); [ "$_gf_i" -lt "$2" ] && sleep 3
    done
    [ -n "$_gf_min" ] && printf '%s' "$_gf_min"
}
# the JUDGE (pure — no admin call, so tests/r9d-nonvacuity can drive it two-sided): the post-quiesce floor
# ($2) must sit within GOR_TOL of the pre-load floor ($1). Fail-closed on an empty/non-numeric operand.
_gor_within_tol() { [ -n "${1:-}" ] && [ -n "${2:-}" ] && [ "$2" -le "$(( ${1} + GOR_TOL ))" ] 2>/dev/null; }


# ── cycle predicates (FUNCTIONS — R-NOSHC) ──────────────────────────────────────────────────────────
_c1_pid_changed() { _n=$(_brk3_pid); [ -n "$_n" ] && [ "$_n" != 0 ] && [ "$_n" != "$_p_before" ]; }
# EXT-REVIEW-B6: genuinely route a control-plane read THROUGH the formerly-partitioned broker (brk2's own
# NATS URL) — proving it rejoined and serves, not the default brk1 path. The old body ran `ctl exec agt2`
# over the default (brk1) route and merely proved the cluster-via-brk1 works, which a still-down brk2 does
# not contradict. (A data op involving agt2 can't be forced onto brk2 since agt2 is homed on brk1; a
# control-plane read routed AT brk2:4222 is the request that actually exercises the recovered side.)
_c2_biz_via_brk2() { dexec -u sim ctl1 -- env HOME=/home/sim tether session ls --json --nats-url nats://brk2:4222 >/dev/null 2>&1; }
_c3_xfer_and_restart() {
    # _SOAK_XFER_SRC is set in the MAIN body before this runs (R-CTX: assert_ok runs this fn in a $()
    # subshell, so a set here would be lost in the parent, and the poll description + _xfer_started read it
    # in the parent). The subshell INHERITS the parent's value, so we just read it here.
    # Self-seed the 12 MiB payload (> the 8 MiB tier-A cap → tier B) FRESH each cycle. Do NOT cp from a
    # setup-time /tmp/soak.bin: /tmp is a tmpfs under the container's systemd, so cycle-1's `docker kill
    # agt1` (which restarts systemd) wipes it, and a later cp would fail for a reason unrelated to the test.
    dexec agt1 -- sh -c "dd if=/dev/urandom of=/tmp/$_SOAK_XFER_SRC bs=1M count=12 2>/dev/null; test -s /tmp/$_SOAK_XFER_SRC" >/dev/null 2>&1 || return 1
    dexec -u sim ctl1 -- sh -c "nohup env HOME=/home/sim tether pull agt1:/tmp/$_SOAK_XFER_SRC /tmp/soak-out.bin --nats-url $NURL >/tmp/97-pull.log 2>&1 & echo started" >/dev/null 2>&1 || return 1
    dexec brk3 -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl restart tether-broker' >/dev/null 2>&1
    return 0
}
# The DETERMINISTIC proof this cycle's injection ran = the concurrent broker restart took (brk3's MainPID
# changed). The tier-B transfer is best-effort CONCURRENT LOAD: a transfer whose route is being restarted
# mid-flight may be disrupted so early it never even registers a history row (racy), so requiring a
# transfer row would flake on the very disruption the cycle injects. 97's deliverable is the leak oracle.
_c3_restart_took() { _n=$(_brk3_pid); [ -n "$_n" ] && [ "$_n" != 0 ] && [ "$_n" != "$_c3_pid_before" ]; }

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
    for _n in brk1 brk2 brk3 agt1 agt2; do node_running "$_n" 2>/dev/null || node_start "$_n" >/dev/null 2>&1 || true; done
    rm -f "$SER_B" "$SER_A" 2>/dev/null || true
    true
}

drill_begin "97-soak-cycles (N=3, SOAK_CYCLES=$SOAK_CYCLES: parameterised chaos soak + fd/RSS leak oracle)"
drill_install_traps _cleanup

"$SIM" nuke >/dev/null 2>&1 || true
rm -f "$SER_B" "$SER_A" 2>/dev/null || true

assert_setup "grow_to_3 (N=3 HA)"                     grow_to_3 2 1
assert_setup "ensure brk1 is the leader (grow inits brk1, so this is usually a no-op — a redundant transfer to self would error 70)" _ensure_leader_brk1
assert_setup "PRECONDITION leader is brk1 (brk1 is a MEASURED process and must never be a victim)" _leader_is_brk1
assert_setup "session $SID + ctl login"               "$SIM" session "$SID" --pin "$PIN"
assert_setup "agent-join agt1 (the VICTIM agent)"     "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_setup "agent-join agt2 (a MEASURED process — never a victim)" "$SIM" agent-join agt2 --session "$SID" --pin "$PIN"
assert_setup "provision agt1 agent.yaml"              agent_provision_yaml agt1 "$SID" "$NURL" open
assert_setup "provision agt2 agent.yaml"              agent_provision_yaml agt2 "$SID" "$NURL" open
assert_setup "seed a 12 MiB payload on agt1 for the transfer-concurrency cycles (> the 8 MiB tier-A ceiling)" \
    dexec agt1 -- sh -c 'dd if=/dev/urandom of=/tmp/soak.bin bs=1M count=12 2>/dev/null; test -s /tmp/soak.bin'

# ── WARM-UP, then t0. Sampling at boot would catch lazy init and manufacture a false RED. ───────────
assert_setup "warm-up: real business traffic before the baseline (t0 at boot would catch lazy init and false-RED)" _biz_ok
poll_until_fixed "$SOAK_SETTLE" 5 "steady state before the baseline sample" -- false || true

PID_B0=$(leak_pid brk1 tether-broker); PID_A0=$(leak_pid agt2 tether-agent)
[ -n "$PID_B0" ] && [ "$PID_B0" != 0 ] || setup_fail "could not resolve brk1's broker MainPID for the leak baseline"
[ -n "$PID_A0" ] && [ "$PID_A0" != 0 ] || setup_fail "could not resolve agt2's agent MainPID for the leak baseline"
# EXT-REVIEW-B7: leak_sample is fail-closed now (empty + nonzero on a failed /proc read). A missing
# baseline sample cannot be treated as 0 0 0 — the whole oracle would be built on a bogus origin.
_S_B0=$(leak_sample brk1 "$PID_B0") || setup_fail "baseline leak sample of brk1 broker failed to read /proc (fail-closed: a missing sample is never a 0 0 0 sample)"
_S_A0=$(leak_sample agt2 "$PID_A0") || setup_fail "baseline leak sample of agt2 agent failed to read /proc"
leak_series_add "$SER_B" "$_S_B0"
leak_series_add "$SER_A" "$_S_A0"
log "97 baseline: brk1 broker pid=$PID_B0 [$(head -1 "$SER_B")] · agt2 agent pid=$PID_A0 [$(head -1 "$SER_A")] (fd threads rss_kb)"
# GOROUTINE baseline (R13): brk1's NumGoroutine FLOOR before the soak load, via admin runtime. A missing
# baseline is NOT fatal here — the GOROUTINE gate after the loop records not_covered rather than inventing
# an origin (mirrors the fd/RSS fail-closed discipline: a run cannot judge a floor it never captured).
GOR_BASE=$(_gor_floor brk1 "$GOR_PRE_SAMPLES")
[ -n "$GOR_BASE" ] && log "97 goroutine baseline: brk1 NumGoroutine floor=$GOR_BASE over $GOR_PRE_SAMPLES samples (admin runtime .goroutines — in-process truth, not /proc Threads)" \
    || log "97 goroutine baseline: admin runtime yielded no .goroutines at t0 — the GOROUTINE gate below will record not_covered rather than fabricate a baseline"

# ── THE SOAK LOOP ───────────────────────────────────────────────────────────────────────────────────
# Four injection types rotate (cycle % 4), exactly the four the roadmap names:
#   0 agent kill/return · 1 broker restart · 2 network partition/heal · 3 transfer concurrency
# Type 3 is not optional: it is #57/#58's accumulation amplifier (orphaned objects piling up against the
# per-session 8 GiB bucket cap = a #21-family relapse), and it is the only thing 97 contributes to 96-A.
_c=1
while [ "$_c" -le "$SOAK_CYCLES" ]; do
    _t=$(( (_c - 1) % 4 ))
    log "── soak cycle $_c/$SOAK_CYCLES (injection type $_t) ──"
    case "$_t" in
      0)
        assert_ok "cycle $_c/0 INJECT: docker kill agt1" node_kill agt1
        assert_ok "cycle $_c/0 NON-VACUITY: agt1 really left ONLINE (an injection that did not take would make the whole cycle a no-op)" \
            poll_until 90 3 "agt1 leaves ONLINE" -- _agt1_offline
        assert_ok "cycle $_c/0 return agt1" node_start agt1
        assert_ok "cycle $_c/0 NON-VACUITY: agt1 really came back ONLINE" poll_until 120 3 "agt1 back ONLINE" -- _agt1_online
        ;;
      1)
        _p_before=$(_brk3_pid)
        assert_ok "cycle $_c/1 INJECT: restart brk3's broker" dexec brk3 -- sh -c 'systemctl reset-failed tether-broker 2>/dev/null; systemctl restart tether-broker'
        assert_ok "cycle $_c/1 NON-VACUITY: brk3's MainPID really CHANGED (proves the restart happened rather than being a no-op)" \
            poll_until 90 3 "brk3 broker MainPID changes" -- _c1_pid_changed
        ;;
      2)
        assert_ok "cycle $_c/2 INJECT: silently partition brk2's route+raft" fault_partition_on brk2 6222 7400
        assert_ok "cycle $_c/2 NON-VACUITY: the partition really took (rc=124 hang = a silent DROP, not a refusal)" \
            poll_until 30 2 "brk2:6222 blackholed from brk1" -- fault_assert_blackholed brk1 brk2 6222
        assert_ok "cycle $_c/2 heal" fault_partition_off brk2
        assert_ok "cycle $_c/2 NON-VACUITY: healed — all three nodes agree on ONE leader again" \
            poll_until 180 5 "one leader after the heal" -- _one_leader
        assert_ok "cycle $_c/2 NON-VACUITY: real business runs THROUGH the formerly-partitioned side (without this, 'healed' is unproven)" \
            poll_until 60 3 "business through brk2" -- _c2_biz_via_brk2
        ;;
      3)
        # Capture brk3's pre-restart MainPID in the MAIN body (R-CTX): assert_ok runs _c3_xfer_and_restart
        # in a $() subshell, so a global set inside it is lost in the parent — mirror the type-1 cycle's
        # `_p_before` capture above.
        # Set _SOAK_XFER_SRC + capture brk3's pid in the MAIN body (R-CTX): assert_ok runs the INJECT fn in
        # a $() subshell, so both must be set in the parent for the poll description / _xfer_started / non-vacuity.
        _SOAK_XFER_SRC="soak-c${_c}.bin"
        _c3_pid_before=$(_brk3_pid)
        assert_ok "cycle $_c/3 INJECT: a tier-B transfer runs CONCURRENTLY with a broker restart" _c3_xfer_and_restart
        assert_ok "cycle $_c/3 NON-VACUITY (restart half): the concurrent broker restart really took (brk3 MainPID changed) — the deterministic proof the restart ran" \
            poll_until 90 3 "brk3 MainPID changed after the concurrent restart" -- _c3_restart_took
        # EXT-REVIEW round-2 R2-F2: the restart alone does NOT prove the TRANSFER entered the product path
        # (the background pull can exit before registering). Require THIS cycle's unique source to appear as
        # a `start` history row; if it does not (a restart-disrupted pull can die before registering),
        # record the transfer-concurrency half not_covered rather than claim a four-type-self-proven cycle.
        if poll_until 60 3 "this cycle's transfer entered the product path (a start history row for $_SOAK_XFER_SRC)" -- _xfer_started; then
            _as_pass "cycle $_c/3 NON-VACUITY (transfer half): the concurrent tier-B transfer really reached the product history (start row) — not just the restart"
        else
            not_covered "97 cycle $_c/3 transfer-concurrency half" "the broker restart took (proven above) but this cycle's tier-B transfer produced no start history row within 60s — a restart-disrupted background pull can exit before registering, so the transfer half is not observably exercised this cycle; recorded not_covered rather than claiming all four injection types self-proved" runtime-guard
        fi
        ;;
    esac

    poll_until_fixed "$SOAK_SETTLE" 5 "steady state before sampling cycle $_c" -- false || true

    # PID GENERATION GUARD — the measured processes must never have been restarted, or the series is
    # reset and the leak judgement is invalid for this run.
    _pb=$(leak_pid brk1 tether-broker); _pa=$(leak_pid agt2 tether-agent)
    if [ "$_pb" != "$PID_B0" ] || [ "$_pa" != "$PID_A0" ]; then
        not_covered "97 leak judgement invalid for this run: a MEASURED process restarted at cycle $_c (brk1 broker $PID_B0->$_pb, agt2 agent $PID_A0->$_pa)" \
            "the fd/RSS series was reset, so judging it either way would be meaningless. The unexpected restart is itself the finding — triage it (NRestarts brk1=$(dexec brk1 -- systemctl show -p NRestarts --value tether-broker 2>/dev/null | tr -d '\r'))" runtime-guard
        break
    fi
    # EXT-REVIEW-B7 (fail-closed): a failed /proc read at any cycle invalidates the series for this run —
    # record not_covered rather than appending an empty/zero line that would silently lower the curve.
    _S_B=$(leak_sample brk1 "$_pb"); _rcb=$?
    _S_A=$(leak_sample agt2 "$_pa"); _rca=$?
    if [ "$_rcb" != 0 ] || [ "$_rca" != 0 ]; then
        not_covered "97 leak judgement invalid: a leak sample failed to read /proc at cycle $_c (brk1 rc=$_rcb, agt2 rc=$_rca)" \
            "a missing sample cannot be judged and must NOT be treated as a 0 0 0 (that would make a resetting series look bounded); recording not_covered instead of a meaningless bounded verdict" runtime-guard
        break
    fi
    leak_series_add "$SER_B" "$_S_B"
    leak_series_add "$SER_A" "$_S_A"
    assert_ok "cycle $_c END: real business still works (every cycle must end proving the system is usable, or 'no leak' would be trivially true of a dead system)" _biz_ok
    _c=$((_c + 1))
done

# ── THE LEAK ORACLE ─────────────────────────────────────────────────────────────────────────────────
log "97 series brk1(fd threads rss_kb): $(tr '\n' '|' < "$SER_B")"
log "97 series agt2(fd threads rss_kb): $(tr '\n' '|' < "$SER_A")"
NPROC=$(dexec brk1 -- nproc 2>/dev/null | tr -d '\r'); [ -n "$NPROC" ] || NPROC=8

_lv_b=0; leak_verdict "$SER_B" brk1-broker "$NPROC" || _lv_b=$?
_lv_a=0; leak_verdict "$SER_A" agt2-agent "$NPROC" || _lv_a=$?
_leak_b_ok() { [ "$_lv_b" = 0 ]; }
_leak_a_ok() { [ "$_lv_a" = 0 ]; }

if [ "$_lv_b" = 2 ] || [ "$_lv_a" = 2 ]; then
    not_covered "97 leak slope judgement skipped: SOAK_CYCLES=$SOAK_CYCLES gives fewer than the $LEAK_MIN_N samples the slope test needs" \
        "with <6 samples the slope is not statistically meaningful; re-run with SOAK_CYCLES>=6 (the default). This is a parameterisation guard, not a product finding" runtime-guard
else
    assert_ok "LEAK brk1's broker fd/Threads/RSS stayed bounded across $SOAK_CYCLES chaos cycles (thresholds [UNCALIBRATED], widened 4x pending the 3-run baseline — a RED here is triaged as jitter-or-leak FIRST; the threshold is never what moves to make it green)" \
        _leak_b_ok
    assert_ok "LEAK agt2's agent fd/Threads/RSS stayed bounded across $SOAK_CYCLES chaos cycles" _leak_a_ok
fi

# ── THE GOROUTINE LEAK GATE (R13) — load(soak) → quiesce → floor returns to the pre-load baseline ────
# Contract constants are fixed in the header block. GOR_TOL is nproc-derived (same shape as leak.sh's
# Threads high-water). The compare is only valid while brk1's MainPID is unchanged (a leader restart would
# reset the goroutine count) — the loop's PID guard already broke with not_covered if that happened; we
# re-check here so a break-free-but-restarted run is caught too.
GOR_TOL=$(( 2 * NPROC + 16 ))
_pb_now=$(leak_pid brk1 tether-broker)
if [ "$_pb_now" != "$PID_B0" ]; then
    not_covered "97 goroutine gate skipped: brk1's broker restarted during the soak (MainPID $PID_B0->$_pb_now)" \
        "the goroutine FLOOR resets on a restart, so a pre/post compare is meaningless — the restart is itself the finding, triage it (same guard as the fd/RSS series)" runtime-guard
elif [ -z "${GOR_BASE:-}" ]; then
    not_covered "97 goroutine gate skipped: no pre-load goroutine baseline was captured at t0" \
        "admin runtime yielded no .goroutines at baseline (socket not ready / jq miss), so there is nothing to compare the post-quiesce floor against; recorded not_covered rather than inventing a baseline" runtime-guard
else
    poll_until_fixed "$GOR_QUIESCE" 5 "goroutine quiesce after the soak load (let handler goroutines return before the post sample)" -- false || true
    GOR_POST=$(_gor_floor brk1 "$GOR_POST_SAMPLES")
    log "97 goroutine gate: pre-load floor=$GOR_BASE post-quiesce floor=${GOR_POST:-<none>} tol=$GOR_TOL (=2*NPROC[$NPROC]+16) [UNCALIBRATED]"
    if [ -z "$GOR_POST" ]; then
        not_covered "97 goroutine gate: no post-quiesce goroutine sample" \
            "admin runtime yielded no .goroutines after the quiesce window, so the floor cannot be judged; recorded not_covered rather than a meaningless verdict" runtime-guard
    else
        assert_ok "GOROUTINE brk1's NumGoroutine floor returned to the pre-load baseline after the soak+quiesce (post $GOR_POST <= pre $GOR_BASE + tol $GOR_TOL) — a real leak would keep the post floor elevated by the accumulated leaked count; .goroutines is the in-process truth (runtime.NumGoroutine), NOT the /proc Threads proxy" \
            _gor_within_tol "$GOR_BASE" "$GOR_POST"
    fi
fi

# ── THE ORTHOGONAL CRASH/INTEGRITY ORACLE (never substitutes for the leak oracle, nor it for this) ──
# h1 F3 (was R-BROKERLOG): the broker's output is no longer one file. slog is
# /var/log/tether/broker.log (process-owned, capped); panics/stacktraces/data
# races reach journald via raw fd 2. Both are checked — see _brk_err_clean.
assert_ok "INTEGRITY brk1 has no panic / FK violation / data race / DB corruption (broker slog AND journal)" _brk_err_clean brk1
assert_ok "INTEGRITY brk2 is clean (broker slog AND journal)" _brk_err_clean brk2
assert_ok "INTEGRITY brk3 is clean (broker slog AND journal)" _brk_err_clean brk3
assert_ok "INTEGRITY agt2's agent journal is clean (the agent unit is NOT redirected, so journald is right here)" _agt_journal_clean agt2

# ── P8 24h PARITY: a batch-level registration, deliberately NOT a not_covered() ─────────────────────
# Three deltas from P8's prototype: DURATION (6 cycles ~16min vs 24h — timer wraparound, cert/lease/TTL
# expiry, log rotation, JS retention boundaries and any leak below epsilon are structurally out of reach),
# CADENCE (back-to-back injections never exercise defects that need long quiet intervals), and TENANCY
# (simcluster is an on-demand dev tool on throwaway instances; README NON-GOALS says it does not sit
# resident). MITIGATION, not a substitute: SOAK_CYCLES=48 SOAK_SETTLE=90 before a release. The P8 24h
# exit therefore REMAINS OWED to staging/real hardware — recorded in the plan's NOT-COVERED ledger rather
# than spent here, for the same reason as the goroutine gap (the waivers are suite-global).
log "97 P8-PARITY: this run = $SOAK_CYCLES cycles x ${SOAK_SETTLE}s settle. P8's '24h + hourly chaos' exit is NOT met by it (duration/cadence/tenancy deltas) and remains owed to staging/real hardware; release-time deep run = SOAK_CYCLES=48 SOAK_SETTLE=90. Registered at batch level in s7-s9-plan §5.1, not spent as a suite-global --allow-incomplete waiver."

drill_end
