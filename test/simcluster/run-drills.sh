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
#   ./run-drills.sh --skip-preflight         # do not check/raise fs.inotify.max_user_instances
#   ./run-drills.sh 10-grow-to-3 20-forcesingle-natsconf   # only the named drills
#
# EXIT CODE: number of drills still RED after the retry pass (0 == all green). Per-drill combined logs
# land in $LOGDIR (default /tmp/simdrills); inspect $LOGDIR/<name>.log on any RED.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
SIM="$HERE/simcluster"
[ -x "$SIM" ] || { echo "run-drills: no ./simcluster next to this script ($SIM)" >&2; exit 9; }

JOBS=0                              # 0 == all (no concurrency cap)
STAGGER=0                           # seconds between launches; 0 = fire together (inotify preflight, not
                                    # stagger, is what makes parallelism safe — see the header)
RETRY=1                             # 1 == serially re-run infra flakes after the parallel pass
PREFLIGHT=1                         # 1 == check (and try to raise) fs.inotify.max_user_instances
INOTIFY_MIN=2048                    # cap must be >= this to run (40 containers already exhaust 128)
INOTIFY_WANT=8192                   # value we raise it to when too low
LOGDIR="${LOGDIR:-/tmp/simdrills}"
DRILLS=()

# A drill failure is an infra FLAKE (safe to re-run) only with one of these signatures — the inotify
# starvation surfaces exactly here. A plain assertion failure (real regression, or assert_bug "APPEARS
# FIXED") matches none of these and is NEVER auto-re-run (that would hide real signal).
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
rm -f "$LOGDIR"/*.log "$LOGDIR"/*.rc 2>/dev/null || true

if [ -t 1 ]; then C_G=$'\033[32m'; C_R=$'\033[31m'; C_Y=$'\033[33m'; C_0=$'\033[0m'; else C_G=; C_R=; C_Y=; C_0=; fi

run_one() { ( "$SIM" drill "$1" >"$LOGDIR/$1.log" 2>&1; echo $? >"$LOGDIR/$1.rc" ); }
verdict_of() {
    local v; v="$(grep -oE ': (GREEN|RED) \([^)]*\) ===' "$LOGDIR/$1.log" 2>/dev/null | tail -1 | sed -E 's/^: //; s/ ===$//')"
    [ -n "$v" ] && printf '%s' "$v" || printf '(no verdict — infra failure before drill_end)'
}
is_flake() { [ "$(cat "$LOGDIR/$1.rc" 2>/dev/null || echo 1)" != 0 ] && grep -qE "$FLAKE_SIG" "$LOGDIR/$1.log" 2>/dev/null; }
secs() { date +%s; }

started=$(secs)
echo "run-drills: ${#DRILLS[@]} drills | jobs=$JOBS | stagger=${STAGGER}s | retry=$RETRY | logs=$LOGDIR"
echo "  drills: ${DRILLS[*]}"
echo

# ── parallel pass ──────────────────────────────────────────────────────────────────────────────────
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
retried=()
if [ "$RETRY" = 1 ]; then
    flakes=()
    for d in "${DRILLS[@]}"; do is_flake "$d" && flakes+=("$d"); done
    if [ "${#flakes[@]}" -gt 0 ]; then
        echo
        echo "${C_Y}run-drills: re-running ${#flakes[@]} infra flake(s) serially: ${flakes[*]}${C_0}"
        for d in "${flakes[@]}"; do
            printf '[%s] retry  %-30s\n' "$(date +%H:%M:%S)" "$d"
            run_one "$d"
            retried+=("$d")
        done
    fi
fi

# ── summary ────────────────────────────────────────────────────────────────────────────────────────
echo
echo "================================ drill summary ================================"
fails=0
for d in "${DRILLS[@]}"; do
    rc="$(cat "$LOGDIR/$d.rc" 2>/dev/null || echo '?')"
    tag=""; for r in "${retried[@]:-}"; do [ "$r" = "$d" ] && tag=" ${C_Y}(retried)${C_0}"; done
    if [ "$rc" = 0 ]; then
        printf '  %sGREEN%s  %-30s rc=%s  %s%s\n' "$C_G" "$C_0" "$d" "$rc" "$(verdict_of "$d")" "$tag"
    else
        printf '  %sRED  %s  %-30s rc=%s  %s%s\n' "$C_R" "$C_0" "$d" "$rc" "$(verdict_of "$d")" "$tag"
        fails=$((fails+1))
    fi
done
echo "-------------------------------------------------------------------------------"
printf '  %d drills, %d RED, %ds elapsed' "${#DRILLS[@]}" "$fails" "$(( $(secs) - started ))"
[ "${#retried[@]}" -gt 0 ] && printf ' (%d retried)' "${#retried[@]}"
echo
if [ "$fails" -eq 0 ]; then echo "  ${C_G}ALL GREEN${C_0}"; else echo "  ${C_R}$fails RED${C_0} — inspect $LOGDIR/<name>.log"; fi
exit "$fails"
