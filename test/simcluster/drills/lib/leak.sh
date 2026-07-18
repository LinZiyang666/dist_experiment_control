# drills/lib/leak.sh — soak leak oracle (G-C / S9-97; s7-s9-plan §1.2-(7), §3.4).
# POSIX sh. Sourced by drill 97. Depends on lib/docker.sh (dexec) + lib/log.sh.
#
# WHAT IT MEASURES AND WHY IT IS SHAPED THIS WAY.
#
# 1. WHY NOT JUST GREP THE LOGS. The roadmap asks this be argued, so: journald records EVENTS. An fd leak
#    emits exactly ZERO log lines until it hits the rlimit — 6 cycles leaking a few fd each sits 3-5 orders
#    of magnitude below LimitNOFILE, so the log is literally empty. RSS is the same: nothing until the
#    OOM-killer writes one kernel line, and by then it is an incident, not a detection. Slow leaks are only
#    visible as a TREND in sampled counters. The journal panic/FK scan stays as a SEPARATE, orthogonal
#    crash/integrity oracle — the two never substitute for each other.
#
# 2. WHY goroutines ARE NOT HERE (permanent NOT-COVERED, registered at batch level in the plan §5.1).
#    The product exposes no pprof/expvar/goroutine gauge (`grep -rE 'pprof|expvar' cmd/ internal/` = zero
#    hits; none of the broker's HTTP listeners serve /debug/pprof). The hermetic suites use
#    runtime.NumGoroutine() from INSIDE the process, which a cross-process drill structurally cannot do.
#    And `/proc/<pid>/status`'s Threads is OS threads (M), NOT goroutines (G): Go multiplexes G onto M, so
#    10k leaked goroutines can show ZERO thread growth. Using Threads as a goroutine proxy would itself be
#    a false-green oracle, so we do not. Threads is measured only as its own bounded quantity.
#
# 3. WHY THE VICTIM MUST NEVER BE THE MEASURED PROCESS. If a cycle kills the process being sampled, its fd
#    count resets to zero every cycle and the series can never show a leak — the oracle would be
#    structurally incapable of failing. Drill 97 therefore never makes brk1 (leader) or agt2 a victim, and
#    asserts their PID generation is unchanged across all N cycles (a changed MainPID means the series was
#    reset -> the run cannot judge, so it records not_covered rather than a meaningless green).
#
# 4. WHY THE THRESHOLDS ARE DELIBERATELY LOOSE FOR NOW. epsilon is a HARNESS parameter, not a product
#    promise. A tight epsilon (0.5 fd/cycle => 1.5 fd total tolerance at N=6) in a system that kills an
#    agent, restarts a broker and partitions the network every cycle is drawing the target around the
#    arrow: it would fire on normal jitter and train everyone to ignore it. The first version is widened 4x
#    and tagged [UNCALIBRATED]; the 3-run baseline (SB-97-2) supplies the real distribution, and the
#    tightening happens then — never by loosening in response to a single RED. A RED is triaged as jitter
#    or leak FIRST; the threshold is not the thing that moves to make it green.

: "${LEAK_K_FD:=64}"          # bounded high-water: max <= fd_0 + K            [UNCALIBRATED, widened 4x]
: "${LEAK_EPS_FD:=2}"         # slope tolerance, fd per cycle                  [UNCALIBRATED, widened 4x]
: "${LEAK_EPS_RSS_KB:=4096}"  # slope tolerance, KiB per cycle (4 MiB)         [UNCALIBRATED, widened 4x]
: "${LEAK_MIN_N:=6}"          # below this the slope judgement is meaningless -> not_covered

# leak_pid <node> <unit> : ALWAYS re-resolve the MainPID. Never cache it — after a restart a cached pid
# silently samples a dead process (or worse, a recycled one).
leak_pid() { dexec "$1" -- systemctl show -p MainPID --value "$2" 2>/dev/null | tr -d '\r'; }

# leak_sample <node> <pid> : print "<fd> <threads> <rss_kb>" for the process, read INSIDE the container
# (R-NO-HOST-LEAK: never probe from the host). Returns 0 and prints three integers ONLY when all three
# fields were read successfully; otherwise prints NOTHING and returns nonzero.
#
# EXT-REVIEW-B7 (fail-closed). The old form fell back to `${fd:-0}` per field and always returned 0, so a
# dead PID / permission error / PID-reuse read surfaced as a healthy "0 0 0" sample — a soak false-GREEN,
# because a resetting series looks bounded. A failed read must be a MISSING sample (caller records
# not_covered), never a zero one. So: guard /proc existence, require all three fields non-empty and
# numeric with fd>0 (a live process always holds >0 fds), and emit only on full success.
leak_sample() {
    _lk=$(dexec "$1" -- sh -c "
        [ -d /proc/$2 ] || exit 3
        fd=\$(ls /proc/$2/fd 2>/dev/null | wc -l | tr -d ' ')
        th=\$(awk '/^Threads:/{print \$2}' /proc/$2/status 2>/dev/null)
        rs=\$(awk '/^VmRSS:/{print \$2}' /proc/$2/status 2>/dev/null)
        [ -n \"\$fd\" ] && [ -n \"\$th\" ] && [ -n \"\$rs\" ] && [ \"\$fd\" -gt 0 ] 2>/dev/null || exit 4
        printf '%s %s %s' \"\$fd\" \"\$th\" \"\$rs\"
    " 2>/dev/null | tr -d '\r')
    # Validate host-side too (the pipe hides the inner exit code): exactly three non-negative integers,
    # fd>0. Anything else is a failed sample -> no output, nonzero return (caller must fail closed).
    # shellcheck disable=SC2086
    set -- $_lk
    { [ "$#" -eq 3 ] && [ "$1" -gt 0 ] 2>/dev/null \
        && [ "$2" -ge 0 ] 2>/dev/null && [ "$3" -ge 0 ] 2>/dev/null; } || return 1
    printf '%s %s %s' "$1" "$2" "$3"
}

# leak_series_add <file> <sample...> : append one sample line to a series file (host-side, per drill).
leak_series_add() { _ls_f=$1; shift; printf '%s\n' "$*" >> "$_ls_f"; }

# _leak_mean_first <file> <col> <n> / _leak_mean_last <file> <col> <n> : mean of the first/last n values.
_leak_mean_first() { head -n "$3" "$1" | awk -v c="$2" '{s+=$c; n++} END{if(n) printf "%.2f", s/n; else print "0"}'; }
_leak_mean_last()  { tail -n "$3" "$1" | awk -v c="$2" '{s+=$c; n++} END{if(n) printf "%.2f", s/n; else print "0"}'; }
_leak_max()        { awk -v c="$1" 'BEGIN{m=0} {if($c>m) m=$c} END{print m+0}' "$2"; }
_leak_first()      { head -n 1 "$1" | awk -v c="$2" '{print $c+0}'; }

# leak_verdict <series-file> <label> <nproc> : judge one process's series. Prints a verdict line and
# returns 0 (bounded) / 1 (breach) / 2 (too few samples to judge).
#
# fd:      bounded high-water AND slope.
# Threads: bounded high-water only (scaled by nproc — the Go runtime's M count tracks GOMAXPROCS).
# RSS:     slope + RELATIVE ceiling only. Never an absolute high-water: Go's GC sawtooth would make that
#          fire on a perfectly healthy process.
leak_verdict() {
    _lv_f=$1; _lv_label=$2; _lv_nproc=${3:-8}
    _lv_n=$(wc -l < "$_lv_f" | tr -d ' ')
    if [ "$_lv_n" -lt "$LEAK_MIN_N" ]; then
        warn "leak_verdict[$_lv_label]: only $_lv_n samples (< $LEAK_MIN_N) — slope judgement is not statistically meaningful"
        return 2
    fi
    _lv_fd0=$(_leak_first "$_lv_f" 1);   _lv_fdmax=$(_leak_max 1 "$_lv_f")
    _lv_th0=$(_leak_first "$_lv_f" 2);   _lv_thmax=$(_leak_max 2 "$_lv_f")
    _lv_rss0=$(_leak_first "$_lv_f" 3)
    _lv_fdf=$(_leak_mean_first "$_lv_f" 1 3); _lv_fdl=$(_leak_mean_last "$_lv_f" 1 3)
    _lv_rssf=$(_leak_mean_first "$_lv_f" 3 3); _lv_rssl=$(_leak_mean_last "$_lv_f" 3 3)
    _lv_span=$((_lv_n - 3)); [ "$_lv_span" -ge 1 ] || _lv_span=1

    _lv_fd_hw_lim=$((_lv_fd0 + LEAK_K_FD))
    _lv_th_hw_lim=$((_lv_th0 + 2 * _lv_nproc + 16))
    _lv_fd_slope_lim=$(awk -v e="$LEAK_EPS_FD" -v s="$_lv_span" 'BEGIN{printf "%.2f", e*s}')
    _lv_rss_slope_lim=$(awk -v e="$LEAK_EPS_RSS_KB" -v s="$_lv_span" 'BEGIN{printf "%.2f", e*s}')
    _lv_rss_ceil=$((_lv_rss0 * 2))

    _lv_bad=0
    [ "$_lv_fdmax" -le "$_lv_fd_hw_lim" ] || { err "leak[$_lv_label]: fd high-water $_lv_fdmax > $_lv_fd_hw_lim (fd_0=$_lv_fd0 + K=$LEAK_K_FD)"; _lv_bad=1; }
    [ "$_lv_thmax" -le "$_lv_th_hw_lim" ] || { err "leak[$_lv_label]: Threads high-water $_lv_thmax > $_lv_th_hw_lim"; _lv_bad=1; }
    awk -v a="$_lv_fdl" -v b="$_lv_fdf" -v l="$_lv_fd_slope_lim" 'BEGIN{exit !((a-b) <= l)}' \
        || { err "leak[$_lv_label]: fd slope mean(last3)=$_lv_fdl - mean(first3)=$_lv_fdf > $_lv_fd_slope_lim"; _lv_bad=1; }
    awk -v a="$_lv_rssl" -v b="$_lv_rssf" -v l="$_lv_rss_slope_lim" 'BEGIN{exit !((a-b) <= l)}' \
        || { err "leak[$_lv_label]: RSS slope mean(last3)=${_lv_rssl}K - mean(first3)=${_lv_rssf}K > ${_lv_rss_slope_lim}K"; _lv_bad=1; }
    awk -v a="$_lv_rssl" -v c="$_lv_rss_ceil" 'BEGIN{exit !(a <= c)}' \
        || { err "leak[$_lv_label]: RSS mean(last3)=${_lv_rssl}K > 2x rss_0 (${_lv_rss_ceil}K)"; _lv_bad=1; }

    log "LEAK-SERIES[$_lv_label] n=$_lv_n fd: ${_lv_fd0}->${_lv_fdl} (max $_lv_fdmax, lim $_lv_fd_hw_lim) | thr: ${_lv_th0}->max ${_lv_thmax} (lim $_lv_th_hw_lim) | rss: ${_lv_rss0}K->${_lv_rssl}K (ceil ${_lv_rss_ceil}K) [UNCALIBRATED thresholds — widened 4x pending SB-97-2]"
    return $_lv_bad
}
