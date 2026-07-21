#!/usr/bin/env bash
# run-drills.sh — run the WHOLE simcluster drill suite in parallel. Runs ON THE SIM SERVER (needs
# docker + ./simcluster). Drive it from the WSL box with `./remote.sh drill-all [opts]`, or run it
# directly on the server from test/simcluster/.
#
# WHY THE SUITE WOULDN'T PARALLELIZE (root cause, established 2026-07-09)
#   `simcluster drill <name>` runs ONE drill on an isolated throwaway instance (own network / volumes /
#   containers). They are docker-isolated, so in principle they parallelize freely. In practice, firing
#   all of them at once made ~5/7 go spuriously RED at the `up` step: "systemd never came up on brkN",
#   then "brkN container not running". It looked like a boot/IO storm — it was NOT. Measured facts:
#     - 16-25 containers cold-boot concurrently in 3-4s; vmstat io-wait <2%, load <0.8. Not IO/CPU/RAM.
#     - The failing containers exit 255 in ~270ms with EMPTY logs — systemd PID1 dies before printing.
#     - A SOLE `docker start` of a failed container also fails — so it is a GLOBAL, persistent limit,
#       not a transient launch race.
#     - Decisive test: 40 concurrent containers → 13 FAIL at fs.inotify.max_user_instances=128 (default),
#       0 FAIL at 8192, 12 FAIL again back at 128. One sysctl flips it. Root cause == inotify instances.
#   MECHANISM: every systemd container (systemd + journald + units) opens several inotify instances under
#   the host's uid 0. `fs.inotify.max_user_instances` is a PER-UID cap (default 128) shared by ALL
#   privileged containers (no userns remap). Parallel drills exhaust it; the next systemd's inotify_init()
#   fails → PID1 aborts → container exits → wait_sysd times out after 60s.
#
#   COROLLARY: staggering launches or capping -j does NOT fix this — inotify instances are held for a
#   container's WHOLE LIFETIME, not just at boot. The ONLY real fix is raising the cap. Once it is high
#   enough, the drills parallelize with no stagger at all. So this runner PREFLIGHTS the cap (raising it
#   if it can), then runs everything concurrently. -j / --stagger remain available for small hosts, and a
#   post-pass re-runs any residual infra flake.
#
# USAGE (from test/simcluster/ on the server, or `./remote.sh drill-all …` from WSL)
#   ./run-drills.sh                          # ALL drills, full parallelism (preflight guards inotify)
#   ./run-drills.sh -j 3                      # cap at 3 concurrent drills (smaller host)
#   ./run-drills.sh --stagger 15             # 15s between launches (default 0 — not needed post-fix)
#   ./run-drills.sh --no-retry               # do not auto-re-run infra flakes (raw parallel result)
#   ./run-drills.sh --drill-timeout 900      # bound EACH drill at 900s (default 2700 = 45min)
#   ./run-drills.sh --skip-preflight         # do not check/raise fs.inotify.max_user_instances
#   ./run-drills.sh --allow-product-red       # explicit owner waiver: known product defects do not fail suite
#   ./run-drills.sh --allow-incomplete        # explicit owner waiver: coverage gaps do not fail suite
#   ./run-drills.sh 10-grow-to-3 20-forcesingle-natsconf   # only the named drills
#
# EXIT CODE: number of unwaived non-GREEN drills after retry, saturated at 125 (0 == all green, or every
# PRODUCT-RED/INCOMPLETE explicitly waived). Per-drill logs land in $LOGDIR (default /tmp/simdrills).
#
# ARTIFACTS: besides <drill>.log/.rc, the summary is ALSO persisted to $LOGDIR/rollup.txt (the human
# summary verbatim, colour stripped) and $LOGDIR/rollup.tsv (one machine-parsable row per drill:
# drill<TAB>verdict<TAB>rc<TAB>assert_fail<TAB>setup_red<TAB>product_red<TAB>not_covered<TAB>pass, no
# header, plus a trailing `WAIVER-USED<TAB><flag>` row per waiver flag actually used). WHY: the summary
# used to exist only on stdout, so when the ssh pipe carrying it wedged, the entire sweep's conclusion
# was lost even though every drill had finished on the server. The rollup survives that — re-read it
# with a fresh ssh instead of re-running hours of docker.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
SIM="$HERE/simcluster"
[ -x "$SIM" ] || { echo "run-drills: no ./simcluster next to this script ($SIM)" >&2; exit 9; }

JOBS=0                              # 0 == all (no concurrency cap)
STAGGER=0                           # seconds between launches; 0 = fire together (inotify preflight, not
                                    # stagger, is what makes parallelism safe — see the header)
RETRY=1                             # 1 == serially re-run infra flakes after the parallel pass
PREFLIGHT=1                         # 1 == check (and try to raise) fs.inotify.max_user_instances
ALLOW_PRODUCT_RED=0                 # fail closed unless an owner explicitly supplies the waiver
ALLOW_INCOMPLETE=0                  # fail closed unless an owner explicitly supplies the waiver
DRILL_TIMEOUT=2700                  # per-drill wall-clock ceiling, seconds. 45min is ~2x the slowest
                                    # observed drill (96/97 run 22/16min), so a trip means "wedged",
                                    # never "slow host". A tripped drill is INFRA-ABORT and NOT retried.
INOTIFY_MIN=2048                    # cap must be >= this to run (40 containers already exhaust 128)
INOTIFY_WANT=8192                   # value we raise it to when too low
LOGDIR="${LOGDIR:-/tmp/simdrills}"
DRILLS=()

# A drill failure is an infra FLAKE (safe to re-run) only with one of these signatures — the inotify
# starvation surfaces exactly here. A plain assertion failure (real regression, or assert_bug "APPEARS
# FIXED") matches none of these and is NEVER auto-re-run (that would hide real signal).
# EXTERNAL-REVIEW ROUND-2 R2-F1 (reverts round-1 R12): a VOTER-promotion timeout is DELIBERATELY NOT a flake
# signature. `is_flake` has no concurrency input, so auto-retrying a VOTER timeout would (a) misclassify a
# `-j 1` SOLO timeout — which simcluster:223-232 says is a REAL regression, not the flake — as retryable, and
# (b) let the retry overwrite the first-run evidence. Only the genuine INFRA flakes below (systemd PID1 dying
# under the inotify-cap exhaustion, container-not-running) are auto-retried. A grow-timing timeout in a
# FULL-PARALLEL sweep shows RED and is re-run SINGLY by the operator per the CAVEAT — that manual step is
# correct, because a solo/low-concurrency timeout must NOT be silently swallowed. OQ-8's two-wave family
# split (grow serial/-j2 + N=1 parallel) is the primary mitigation that keeps the flake from arising at all.
FLAKE_SIG='systemd never came up|timed out after [0-9]+s waiting for: systemd responsive|is not running|container not running'

usage() { sed -n '2,/^set -u/p' "$0" | sed 's/^# \{0,1\}//; s/^set -u$//'; }

while [ $# -gt 0 ]; do
    case "$1" in
        -j|--jobs)          JOBS="${2:?}"; shift 2 ;;
        -j*)                JOBS="${1#-j}"; shift ;;
        --jobs=*)           JOBS="${1#--jobs=}"; shift ;;
        --stagger)          STAGGER="${2:?}"; shift 2 ;;
        --stagger=*)        STAGGER="${1#--stagger=}"; shift ;;
        --retry)            RETRY=1; shift ;;
        --no-retry)         RETRY=0; shift ;;
        --skip-preflight)   PREFLIGHT=0; shift ;;
        --drill-timeout)    DRILL_TIMEOUT="${2:?}"; shift 2 ;;
        --drill-timeout=*)  DRILL_TIMEOUT="${1#--drill-timeout=}"; shift ;;
        --allow-product-red) ALLOW_PRODUCT_RED=1; shift ;;
        --allow-incomplete)  ALLOW_INCOMPLETE=1; shift ;;
        --logdir)           LOGDIR="${2:?}"; shift 2 ;;
        --logdir=*)         LOGDIR="${1#--logdir=}"; shift ;;
        -h|--help)          usage; exit 0 ;;
        --)                 shift; while [ $# -gt 0 ]; do DRILLS+=("$1"); shift; done ;;
        -*)                 echo "run-drills: unknown option '$1' (see --help)" >&2; exit 2 ;;
        *)                  DRILLS+=("$1"); shift ;;
    esac
done

case "$JOBS"    in ''|*[!0-9]*) echo "run-drills: -j must be a non-negative integer" >&2; exit 2 ;; esac
case "$STAGGER" in ''|*[!0-9]*) echo "run-drills: --stagger must be a non-negative integer" >&2; exit 2 ;; esac
# 0 is rejected (not just non-numeric): `timeout 0` means "no limit", which would silently reinstate the
# unbounded hang this option exists to prevent. There is no supported way to disable the ceiling.
case "$DRILL_TIMEOUT" in ''|*[!0-9]*) echo "run-drills: --drill-timeout must be a positive integer (seconds)" >&2; exit 2 ;; esac
[ "$DRILL_TIMEOUT" -gt 0 ] || { echo "run-drills: --drill-timeout must be > 0 (0 would mean 'no limit')" >&2; exit 2 ; }

# Discover all drills if none were named (top-level drills/*.sh only; drills/lib/* are helpers).
if [ "${#DRILLS[@]}" -eq 0 ]; then
    for f in "$HERE"/drills/*.sh; do
        [ -f "$f" ] || continue
        DRILLS+=("$(basename "$f" .sh)")
    done
fi
[ "${#DRILLS[@]}" -gt 0 ] || { echo "run-drills: no drills found under $HERE/drills/" >&2; exit 2; }
for d in "${DRILLS[@]}"; do
    [ -f "$HERE/drills/$d.sh" ] || { echo "run-drills: no such drill '$d' ($HERE/drills/$d.sh)" >&2; exit 2; }
done

# JOBS=0 (or over-large) means "no cap" → all concurrently.
[ "$JOBS" -gt 0 ] 2>/dev/null || JOBS="${#DRILLS[@]}"
[ "$JOBS" -le "${#DRILLS[@]}" ] || JOBS="${#DRILLS[@]}"

# ── preflight: the inotify cap is THE thing that makes parallel drills safe (see header) ────────────
preflight_inotify() {
    local cur; cur=$(cat /proc/sys/fs/inotify/max_user_instances 2>/dev/null || echo 0)
    if [ "$cur" -ge "$INOTIFY_MIN" ]; then
        echo "preflight: fs.inotify.max_user_instances=$cur (ok)"
        return 0
    fi
    echo "preflight: fs.inotify.max_user_instances=$cur is TOO LOW — parallel systemd containers will" >&2
    echo "  exhaust the per-uid cap and their PID1 systemd will die (exit 255 → 'container not running')." >&2
    if sudo -n sysctl -w "fs.inotify.max_user_instances=$INOTIFY_WANT" >/dev/null 2>&1; then
        echo "preflight: raised fs.inotify.max_user_instances to $INOTIFY_WANT (THIS BOOT ONLY)." >&2
        echo "  Persist it once so you never hit this again:" >&2
        echo "    echo 'fs.inotify.max_user_instances=$INOTIFY_WANT' | sudo tee /etc/sysctl.d/99-simcluster.conf && sudo sysctl --system" >&2
        return 0
    fi
    echo "  Could not raise it automatically (no passwordless sudo). FIX once on the sim host:" >&2
    echo "    echo 'fs.inotify.max_user_instances=$INOTIFY_WANT' | sudo tee /etc/sysctl.d/99-simcluster.conf && sudo sysctl --system" >&2
    echo "  (or, this boot only:  sudo sysctl -w fs.inotify.max_user_instances=$INOTIFY_WANT )" >&2
    echo "  Then re-run.  (Bypass this check with --skip-preflight if you know what you're doing.)" >&2
    return 1
}
if [ "$PREFLIGHT" = 1 ]; then preflight_inotify || exit 3; fi

mkdir -p "$LOGDIR"
rm -f "$LOGDIR"/*.log "$LOGDIR"/*.rc "$LOGDIR"/*.timeout "$LOGDIR"/rollup.txt "$LOGDIR"/rollup.tsv 2>/dev/null || true

if [ -t 1 ]; then C_G=$'\033[32m'; C_R=$'\033[31m'; C_Y=$'\033[33m'; C_0=$'\033[0m'; else C_G=; C_R=; C_Y=; C_0=; fi

# Every drill is bounded by --drill-timeout. WHY: a drill that wedges (an unbounded poll_until is the
# usual way) used to hang the WHOLE sweep — `wait` never returns, the summary is never printed, and the
# ssh driver on WSL just sits there looking dead. `timeout` turns that unbounded hang into a bounded,
# self-explaining INFRA-ABORT. `-k 30` matters as much as the ceiling itself: without it a child that
# ignores TERM would re-introduce exactly the hang we are trying to kill.
# The elapsed check exists because `-k` changes the reported status: a TERM-ignoring drill is SIGKILLed
# and surfaces as 137, not 124. 137 alone is ambiguous (a plain OOM-kill is also 137) and must not be
# mislabelled a timeout, so 137 only counts when the clock actually ran out.
run_one() {
    local t0 rc
    t0=$(secs)
    ( timeout -k 30 "$DRILL_TIMEOUT" "$SIM" drill "$1" >"$LOGDIR/$1.log" 2>&1; echo $? >"$LOGDIR/$1.rc" )
    rc=$(cat "$LOGDIR/$1.rc" 2>/dev/null || echo '?')
    if [ "$rc" = 124 ] || { [ "$rc" = 137 ] && [ "$(( $(secs) - t0 ))" -ge "$DRILL_TIMEOUT" ]; }; then
        # Marker file, not a log grep: the log of a wedged drill very often ALSO carries a flake
        # signature from an earlier step, and is_flake must not resurrect it (see is_flake).
        : >"$LOGDIR/$1.timeout"
        printf '\n*** run-drills: KILLED BY --drill-timeout after %ss (rc=%s). The drill never reached\n*** drill_end, so it counts as INFRA-ABORT and is deliberately NOT retried — a wedge is not a\n*** flake. The tail above is the last step it made progress on; start there.\n' \
            "$DRILL_TIMEOUT" "$rc" >>"$LOGDIR/$1.log"
    fi
}
# effective_verdict is the one strict parser for the verdict contract. It requires exactly one line, the
# complete anchored grammar, a known enum, canonical enum/rc/counter precedence, and process-rc agreement.
# A missing line is an INFRA-ABORT; every malformed, duplicate, injected, or contradictory line is a
# CONTRACT-ERROR. Contract errors are blockers and are NEVER eligible for retry.
effective_verdict() {
    local log="$LOGDIR/$1.log" line count v lrc af sr pr nc pass prc expected
    count=$(grep -c '^DRILL-VERDICT\([[:space:]]\|$\)' "$log" 2>/dev/null || true)
    if [ "$count" = 0 ]; then printf 'INFRA-ABORT'; return; fi
    if [ "$count" != 1 ]; then printf 'CONTRACT-ERROR'; return; fi
    line=$(grep '^DRILL-VERDICT\([[:space:]]\|$\)' "$log")
    if [[ ! "$line" =~ ^DRILL-VERDICT\ verdict=(GREEN|ASSERT-FAIL|SETUP-RED|PRODUCT-RED|INCOMPLETE)\ rc=([0-9]+)\ assert_fail=([0-9]+)\ setup_red=([0-9]+)\ product_red=([0-9]+)\ not_covered=([0-9]+)\ nc_gap=([0-9]+)\ nc_guard=([0-9]+)\ pass=([0-9]+)\ --\ .+$ ]]; then
        printf 'CONTRACT-ERROR'; return
    fi
    v=${BASH_REMATCH[1]}; lrc=${BASH_REMATCH[2]}; af=${BASH_REMATCH[3]}; sr=${BASH_REMATCH[4]}
    pr=${BASH_REMATCH[5]}; nc=${BASH_REMATCH[6]}
    ncg=${BASH_REMATCH[7]}; ncw=${BASH_REMATCH[8]}; pass=${BASH_REMATCH[9]}
    prc=$(cat "$LOGDIR/$1.rc" 2>/dev/null || echo '?')
    case "$v" in
        GREEN)       expected=0; [ "$af" = 0 ] && [ "$sr" = 0 ] && [ "$pr" = 0 ] && [ "$nc" = 0 ] || { printf 'CONTRACT-ERROR'; return; } ;;
        ASSERT-FAIL) expected=1; [ "$af" -gt 0 ] 2>/dev/null || { printf 'CONTRACT-ERROR'; return; } ;;
        SETUP-RED)   expected=2; [ "$af" = 0 ] && [ "$sr" -gt 0 ] 2>/dev/null || { printf 'CONTRACT-ERROR'; return; } ;;
        PRODUCT-RED) expected=3; [ "$af" = 0 ] && [ "$sr" = 0 ] && [ "$pr" -gt 0 ] 2>/dev/null || { printf 'CONTRACT-ERROR'; return; } ;;
        INCOMPLETE)  expected=4; [ "$af" = 0 ] && [ "$sr" = 0 ] && [ "$pr" = 0 ] && [ "$nc" -gt 0 ] 2>/dev/null || { printf 'CONTRACT-ERROR'; return; } ;;
    esac
    if [ "$lrc" != "$expected" ] || [ "$prc" != "$expected" ]; then printf 'CONTRACT-ERROR'; else printf '%s' "$v"; fi
}
# An INFRA flake (safe to auto-re-run) is: the inotify-starvation signature present AND the drill did not
# reach a PRODUCT/ASSERT signal — i.e. it aborted (INFRA-ABORT) or the prerequisite fixture died (SETUP-RED,
# e.g. grow_to_3 could not bring systemd up). A real ASSERT-FAIL / PRODUCT-RED / INCOMPLETE is NEVER a flake
# (retrying would hide real signal / overwrite evidence).
is_flake() {
    # A --drill-timeout kill is never a flake, whatever the log says. A wedged drill has usually logged a
    # flake signature earlier on (that is often WHY it wedged), so without this gate the retry pass would
    # re-run it and burn another full DRILL_TIMEOUT — and overwrite the evidence of the first wedge.
    [ -e "$LOGDIR/$1.timeout" ] && return 1
    grep -qE "$FLAKE_SIG" "$LOGDIR/$1.log" 2>/dev/null || return 1
    case "$(effective_verdict "$1")" in INFRA-ABORT|SETUP-RED) return 0 ;; *) return 1 ;; esac
}
secs() { date +%s; }

started=$(secs)
echo "run-drills: ${#DRILLS[@]} drills | jobs=$JOBS | stagger=${STAGGER}s | retry=$RETRY | logs=$LOGDIR"
echo "  drills: ${DRILLS[*]}"
echo

# ── parallel pass ──────────────────────────────────────────────────────────────────────────────────
# external-review R4 Q5: tell drills whether this is a CONCURRENT run so a grow VOTER-timeout is diagnosed as the
# grow-timing concurrency flake (JOBS>1) vs the #31 grow-lock serialized-fence / real constructibility (solo).
[ "$JOBS" -gt 1 ] && export SIM_CONCURRENT=1 || export SIM_CONCURRENT=0
launched=0
for d in "${DRILLS[@]}"; do
    if [ "$launched" -gt 0 ] && [ "$STAGGER" -gt 0 ]; then sleep "$STAGGER"; fi
    while [ "$(jobs -rp | wc -l)" -ge "$JOBS" ]; do sleep 2; done
    printf '[%s] launch %-30s (%d/%d)\n' "$(date +%H:%M:%S)" "$d" "$((launched+1))" "${#DRILLS[@]}"
    run_one "$d" &
    launched=$((launched+1))
done
wait

# ── retry pass: serially re-run only infra flakes (assertion failures are real, left alone) ─────────
export SIM_CONCURRENT=0   # retries are SOLO/serial — a grow timeout here is the #31/real path, not concurrency
retried=()
if [ "$RETRY" = 1 ]; then
    flakes=()
    for d in "${DRILLS[@]}"; do is_flake "$d" && flakes+=("$d"); done
    if [ "${#flakes[@]}" -gt 0 ]; then
        echo
        echo "${C_Y}run-drills: re-running ${#flakes[@]} infra flake(s) serially: ${flakes[*]}${C_0}"
        for d in "${flakes[@]}"; do
            printf '[%s] retry  %-30s\n' "$(date +%H:%M:%S)" "$d"
            # R2-F1: PRESERVE the first-run evidence (never silently truncate it) before the retry overwrites.
            cp -f "$LOGDIR/$d.log" "$LOGDIR/$d.attempt1.log" 2>/dev/null || true
            cp -f "$LOGDIR/$d.rc"  "$LOGDIR/$d.attempt1.rc"  2>/dev/null || true
            run_one "$d"
            retried+=("$d")
        done
    fi
fi

# ── summary ────────────────────────────────────────────────────────────────────────────────────────
# Classify EACH drill by its complete DRILL-VERDICT contract. Every non-GREEN state blocks by default.
# PRODUCT-RED/INCOMPLETE may be waived only by their explicit command-line flags; a waiver is displayed.
#
# The summary is written to $LOGDIR/rollup.{txt,tsv} as well as stdout, line by line as it is produced.
# WHY line-by-line rather than a single dump at the end: the failure this defends against is the sweep
# dying (or its ssh pipe wedging) part-way through, and a partially written rollup is still evidence
# whereas a buffered one is nothing at all.
ROLLUP_TXT="$LOGDIR/rollup.txt"
ROLLUP_TSV="$LOGDIR/rollup.tsv"
: >"$ROLLUP_TXT"; : >"$ROLLUP_TSV"
# say = printf to stdout AND to rollup.txt with the SGR colour codes stripped, so the on-disk copy stays
# greppable (a `grep BLOCKER rollup.txt` must not have to know about escape sequences).
say() { printf "$@"; printf "$@" | sed 's/\x1b\[[0-9;]*m//g' >>"$ROLLUP_TXT"; }
# The counters are re-extracted here rather than returned by effective_verdict, which deliberately emits
# only the verdict enum and is called from a subshell (globals could not escape it anyway). A verdict
# that is not one of the five enum values has no trustworthy counters, so they are reported as '-'.
verdict_counters() {
    local line
    case "$2" in GREEN|ASSERT-FAIL|SETUP-RED|PRODUCT-RED|INCOMPLETE) ;; *) printf -- '-\t-\t-\t-\t-\t-\t-'; return ;; esac
    line=$(grep -m1 '^DRILL-VERDICT\([[:space:]]\|$\)' "$LOGDIR/$1.log" 2>/dev/null || true)
    if [[ "$line" =~ assert_fail=([0-9]+)\ setup_red=([0-9]+)\ product_red=([0-9]+)\ not_covered=([0-9]+)\ nc_gap=([0-9]+)\ nc_guard=([0-9]+)\ pass=([0-9]+) ]]; then
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}" \
            "${BASH_REMATCH[4]}" "${BASH_REMATCH[5]}" "${BASH_REMATCH[6]}" "${BASH_REMATCH[7]}"
    else
        printf -- '-\t-\t-\t-\t-\t-\t-'
    fi
}
say '\n'
say '================================ drill summary ================================\n'
n_green=0; n_prod=0; n_inc=0; n_setup=0; n_assert=0; n_abort=0; blockers=0
for d in "${DRILLS[@]}"; do
    rc="$(cat "$LOGDIR/$d.rc" 2>/dev/null || echo '?')"
    v="$(effective_verdict "$d")"
    tag=""; for r in "${retried[@]:-}"; do [ "$r" = "$d" ] && tag=" ${C_Y}(retried)${C_0}"; done
    case "$v" in
        GREEN)              col="$C_G"; n_green=$((n_green+1));   note="" ;;
        PRODUCT-RED)        col="$C_Y"; n_prod=$((n_prod+1));     if [ "$ALLOW_PRODUCT_RED" = 1 ]; then note=" ${C_Y}[WAIVED: --allow-product-red]${C_0}"; else blockers=$((blockers+1)); note=" ${C_R}[BLOCKER: known product defect]${C_0}"; fi ;;
        INCOMPLETE)         col="$C_Y"; n_inc=$((n_inc+1));       if [ "$ALLOW_INCOMPLETE" = 1 ]; then note=" ${C_Y}[WAIVED: --allow-incomplete]${C_0}"; else blockers=$((blockers+1)); note=" ${C_R}[BLOCKER: coverage incomplete]${C_0}"; fi ;;
        SETUP-RED)          col="$C_R"; n_setup=$((n_setup+1));   blockers=$((blockers+1)); note=" ${C_R}[BLOCKER: prereq/infra]${C_0}" ;;
        ASSERT-FAIL)        col="$C_R"; n_assert=$((n_assert+1)); blockers=$((blockers+1)); note=" ${C_R}[BLOCKER: broken invariant]${C_0}" ;;
        CONTRACT-ERROR)     col="$C_R"; n_abort=$((n_abort+1));  blockers=$((blockers+1)); note=" ${C_R}[BLOCKER: malformed/duplicate/inconsistent verdict contract]${C_0}" ;;
        *)                  col="$C_R"; n_abort=$((n_abort+1));   blockers=$((blockers+1)); v="INFRA-ABORT"; note=" ${C_R}[BLOCKER: aborted before drill_end — no verdict]${C_0}" ;;
    esac
    say '  %s%-19s%s %-30s rc=%s%s%s\n' "$col" "$v" "$C_0" "$d" "$rc" "$note" "$tag"
    printf '%s\t%s\t%s\t%s\n' "$d" "$v" "$rc" "$(verdict_counters "$d" "$v")" >>"$ROLLUP_TSV"
done
say -- '-------------------------------------------------------------------------------\n'
say '  %d drills: %sGREEN=%d%s  %sPRODUCT-RED=%d%s  %sINCOMPLETE=%d%s  %sSETUP-RED=%d%s  %sASSERT-FAIL=%d%s  %sINFRA-ABORT=%d%s  (%ds)\n' \
    "${#DRILLS[@]}" "$C_G" "$n_green" "$C_0" "$C_Y" "$n_prod" "$C_0" "$C_Y" "$n_inc" "$C_0" \
    "$C_R" "$n_setup" "$C_0" "$C_R" "$n_assert" "$C_0" "$C_R" "$n_abort" "$C_0" "$(( $(secs) - started ))"
[ "${#retried[@]}" -gt 0 ] && say '  %sretried (infra flake): %s — first-run evidence in %s/<name>.attempt1.log%s\n' "$C_Y" "${retried[*]}" "$LOGDIR" "$C_0"
# A waiver is the one thing a reader of the rollup must never have to infer: it is the difference between
# "the suite passed" and "someone decided the failures were acceptable". Record it in BOTH artifacts,
# machine-readably in the .tsv, so an automated gate cannot mistake a waived run for a clean one.
[ "$ALLOW_PRODUCT_RED" = 1 ] && { say '  WAIVER-USED --allow-product-red\n'; printf 'WAIVER-USED\t--allow-product-red\n' >>"$ROLLUP_TSV"; }
[ "$ALLOW_INCOMPLETE"  = 1 ] && { say '  WAIVER-USED --allow-incomplete\n';  printf 'WAIVER-USED\t--allow-incomplete\n'  >>"$ROLLUP_TSV"; }
if [ "$blockers" -eq 0 ]; then
    if [ "$((n_prod+n_inc))" -eq 0 ]; then say '  %sALL GREEN%s\n' "$C_G" "$C_0"
    else say '  %sWAIVED NON-GREEN%s — PRODUCT-RED=%d INCOMPLETE=%d; explicit owner waiver flags supplied. NOT all-green.\n' "$C_Y" "$C_0" "$n_prod" "$n_inc"; fi
else
    say '  %s%d BLOCKER(S)%s — inspect %s/<name>.log\n' "$C_R" "$blockers" "$C_0" "$LOGDIR"
fi
say '  rollup: %s | %s\n' "$ROLLUP_TSV" "$ROLLUP_TXT"
# Keep shell exit status meaningful instead of wrapping modulo 256 for very large suites.
[ "$blockers" -le 125 ] || blockers=125
exit "$blockers"
