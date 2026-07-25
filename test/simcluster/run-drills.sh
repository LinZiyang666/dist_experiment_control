#!/usr/bin/env bash
# run-drills.sh — run the WHOLE simcluster drill suite in parallel. Runs ON THE SIM SERVER (needs
# docker + ./simcluster). On weilandserver run it directly; from an external box use `./remote.sh drill-all [opts]`, or run it
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
# USAGE (from test/simcluster/ on the server, or `./remote.sh drill-all …` from an external driver box)
#   ./run-drills.sh                          # ALL drills, full parallelism (preflight guards inotify)
#   ./run-drills.sh -j 3                      # cap at 3 concurrent drills (smaller host)
#   ./run-drills.sh --stagger 15             # 15s between launches (default 0 — not needed post-fix)
#   ./run-drills.sh --no-retry               # do not auto-re-run infra flakes (raw parallel result)
#   ./run-drills.sh --drill-timeout 900      # bound EACH drill at 900s (default 2700 = 45min)
#   ./run-drills.sh --skip-preflight         # do not check/raise the per-uid kernel counters
#   ./run-drills.sh --no-lpt                 # launch in NAME order, not longest-first (forensics)
#   ./run-drills.sh --no-attribute           # skip the post-report solo re-run of deviating drills
#   ./run-drills.sh --attr-budget 1800       # cap the attribution pass at 1800s (default 3600)
#   ./run-drills.sh --replay --logdir <dir>  # re-analyse an EXISTING $LOGDIR; runs nothing
#   ./run-drills.sh --allow-product-red       # explicit owner waiver: known product defects do not fail suite
#   ./run-drills.sh --allow-incomplete        # explicit owner waiver: coverage gaps do not fail suite
#   ./run-drills.sh 10-grow-to-3 20-forcesingle-natsconf   # only the named drills
#
# EXIT CODE: number of unwaived non-GREEN drills after retry, saturated at 125 (0 == all green, or every
# PRODUCT-RED/INCOMPLETE explicitly waived). The attribution pass NEVER changes it. Per-drill logs land
# in $LOGDIR (default /tmp/simdrills).
#
# ARTIFACTS in $LOGDIR:
#   <drill>.log/.rc             per drill; <drill>.secs its wall cost; <drill>.attempt2.* the solo re-run
#   evidence/<drill>.evidence   full failure record (argv, rc, FULL output, host telemetry) per non-green
#   rollup.txt                  the human summary verbatim, colour-stripped
#   rollup.tsv                  one machine row per drill, NO header, 15 columns:
#                                 drill  verdict  rc  assert_fail  setup_red  product_red  not_covered
#                                 nc_gap  nc_guard  pass  duration_s  attempts  first_verdict  expected  match
#                               plus trailing `WAIVER-USED<TAB><flag>` rows and, after attribution,
#                               `ATTRIBUTION<TAB><drill><TAB><label><TAB><re-run verdict>` rows (a parser
#                               keying on field COUNT or the first column must expect these shorter rows).
#   progress.tsv                append-only launch/done stream + a final `RUN-COMPLETE` sentinel; a reader
#                               MUST require the sentinel before treating it as a finished result.
#   .simdrills-owned            the ownership capability the destructive startup cleanup requires (exact
#                               value, see below). ONE-TIME MIGRATION NOTE: a log directory produced BEFORE
#                               this marker existed carries no marker, so the first hardened sweep into it
#                               REFUSES to clean (exit 2) instead of wiping it. That is the intended
#                               protection — on the sim server `/tmp/simdrills` holds the pre-lever baseline
#                               archives the plan cites, and they must not be reaped by the next sweep.
#                               Point `--logdir` at a fresh directory (what the corrected-tree runs did),
#                               or delete the old one deliberately once its archives are no longer cited.
# WHY the rollup exists on disk: the summary used to live only on stdout, so a wedged ssh pipe lost the
# whole sweep's conclusion even though every drill had finished. Re-read the rollup with a fresh ssh (or
# `--replay`) instead of re-running hours of docker.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
SIM="$HERE/simcluster"
[ -x "$SIM" ] || { echo "run-drills: no ./simcluster next to this script ($SIM)" >&2; exit 9; }

JOBS=0                              # 0 == all (no concurrency cap)
STAGGER=0                           # seconds between launches; 0 = fire together (inotify preflight, not
                                    # stagger, is what makes parallelism safe — see the header)
RETRY=1                             # 1 == serially re-run infra flakes after the parallel pass
# --replay: re-analyse an EXISTING $LOGDIR without running anything. It runs the real classifier, the
# real expectation diff and the real deviation report over logs that are already on disk, which is what
# lets the hermetic gate replay the archived 2026-07-23 sweep and assert that the machinery would have
# flagged exactly the five rows a human took an evening to find. It also means an archived sweep can be
# re-read after the expectation table changes, without burning hours of docker.
REPLAY=0
RETRY_EXPLICIT=0                    # only an EXPLICIT --retry conflicts with --replay
ATTRIBUTE=1                         # 1 == after the report, re-run deviating/banded drills SOLO
                                    # once to label them (M4). Never changes a verdict or the exit code.
ATTR_BUDGET=3600                    # seconds for the whole attribution pass. A TIME budget, not a count:
                                    # a count of 4 would have dropped 91 on 2026-07-23 — a real regression.
LPT=1                               # 1 == launch longest-first from drill-costs.tsv (V1). --no-lpt
                                    # restores name order, which forensics occasionally wants so two
                                    # sweeps can be diffed launch-for-launch against an older log.
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
        --retry)            RETRY=1; RETRY_EXPLICIT=1; shift ;;
        --no-retry)         RETRY=0; shift ;;
        --replay)           REPLAY=1; shift ;;
        --no-lpt)           LPT=0; shift ;;
        --no-attribute)     ATTRIBUTE=0; shift ;;
        --attr-budget)      ATTR_BUDGET="${2:?}"; shift 2 ;;
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
# MI1: --attr-budget was unvalidated — a non-numeric value errored per re-run and evaluated falsy, so it
# silently DISABLED the budget instead of refusing, unlike every other numeric flag here.
case "$ATTR_BUDGET" in ''|*[!0-9]*) echo "run-drills: --attr-budget must be a non-negative integer (seconds)" >&2; exit 2 ;; esac
# MA6: --replay is a MODE, not a set of independently-toggled flags. Arg parsing is left-to-right, so
# `--replay --retry` would have ended with RETRY=1 and run LIVE drills over the archive being analysed,
# overwriting the very logs it claims only to read. Reject the contradiction, and hard-force the mode's
# invariants AFTER parsing so no ordering of flags can undo them.
if [ "$REPLAY" = 1 ]; then
    # Reject only an EXPLICIT --retry (RETRY=1 is also the default, which --replay silently overrides).
    [ "${RETRY_EXPLICIT:-0}" = 1 ] && { echo "run-drills: --retry cannot be combined with --replay (replay runs nothing)" >&2; exit 2; }
    RETRY=0; ATTRIBUTE=0; PREFLIGHT=0
fi
# 0 is rejected (not just non-numeric): `timeout 0` means "no limit", which would silently reinstate the
# unbounded hang this option exists to prevent. There is no supported way to disable the ceiling.
case "$DRILL_TIMEOUT" in ''|*[!0-9]*) echo "run-drills: --drill-timeout must be a positive integer (seconds)" >&2; exit 2 ;; esac
[ "$DRILL_TIMEOUT" -gt 0 ] || { echo "run-drills: --drill-timeout must be > 0 (0 would mean 'no limit')" >&2; exit 2 ; }
# MI3 + external review Medium 7/re-review Medium 5: the startup cleanup does `rm -f "$LOGDIR"/*.log …`
# and `rm -rf "$LOGDIR"/evidence`. An empty logdir made that `rm -f /*.log`; a literal `/`, the repo root,
# or $HOME deletes real files there. Worse, an ALIAS like `victim/new/..` slips past a cd-based canonicalize
# (cd fails on the not-yet-existent `new`, the raw string is kept, then `mkdir -p` creates `new` and the
# cleanup globs resolve `..` back to `victim` and delete its files). Defences:
#   1. reject any path containing a `..` component (an alias whose target differs from its spelling);
#   2. canonicalize AFTER the dir exists (mkdir -p first, then `cd && pwd -P`);
#   3. refuse the well-known system roots / $HOME / the source dir;
#   4. before the destructive cleanup, require the dir to be OURS — the default, empty, or carrying a
#      runner-written `.simdrills-owned` marker — so pointing --logdir at an arbitrary populated directory
#      cannot wipe it (the ownership check lives at the cleanup site below).
[ -n "$LOGDIR" ] || { echo "run-drills: --logdir must not be empty" >&2; exit 2; }
case "/$LOGDIR/" in *"/../"*) echo "run-drills: refusing --logdir '$LOGDIR' (contains a '..' component — its real target differs from its spelling)" >&2; exit 2 ;; esac
mkdir -p "$LOGDIR" 2>/dev/null || { echo "run-drills: cannot create --logdir '$LOGDIR'" >&2; exit 9; }
_ld_canon=$(cd "$LOGDIR" 2>/dev/null && pwd -P || printf '%s' "$LOGDIR")
LOGDIR="$_ld_canon"     # use the resolved path from here on
case "$_ld_canon" in
    /|/root|/home|/usr|/etc|/var|/bin|/sbin|/lib|/boot|/dev|/proc|/sys) echo "run-drills: refusing --logdir '$_ld_canon' (a system root — the startup cleanup would delete files there)" >&2; exit 2 ;;
esac
[ "$_ld_canon" = "${HOME:-/nonexistent}" ] && { echo "run-drills: refusing --logdir = \$HOME (cleanup would wipe it)" >&2; exit 2; }
[ "$_ld_canon" = "$HERE" ] && { echo "run-drills: refusing --logdir = the simcluster source dir" >&2; exit 2; }

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
# ── M5: THE REST OF THE PER-UID KERNEL COUNTERS ─────────────────────────────────────────────────────
# The inotify story above is not a one-off, it is a CLASS. Docker isolates namespaces; it does not
# isolate per-uid kernel counters, and every privileged systemd container draws from the SAME uid-0
# pools. inotify was simply the first one we hit hard enough to notice, after a misdiagnosis that cost
# the project a "drills must run serially" belief for weeks.
#
# DIVISION OF LABOUR (external review Major 4 — plan M5 amended to match this, with justification):
#   - INOTIFY is the ONE counter with a PROVEN container-density hard failure (systemd PID1 dies, 40
#     containers → 13 fails at the default, 0 at 8192). It REFUSES the run (preflight_inotify below,
#     `exit 3`). Nothing else has a reproduced hard-failure threshold.
#   - The EXTENDED TABLE below is CHECK-AND-REPORT: each counter is printed with ok/LOW and the exact
#     one-time remediation, but a low value does NOT block, because refusing on an unproven threshold
#     would wrongly abort a host whose lower cap is still sufficient for the actual run. Reporting is the
#     value — it convicts or exonerates the NEXT per-uid limit by a table lookup instead of another
#     multi-week misdiagnosis. (The original plan said "check-and-refuse" for the whole set; that is
#     narrowed HERE, explicitly, to refuse-inotify / report-the-rest — see docs/reviews/…-plan.md M5.)
# `sudo -n` FAILS on weilandserver (verified 2026-07-23), so the best-effort auto-raise never fires here;
# the printed remediation is the useful behaviour. Raising a SIM host's caps to host 38 co-tenant clusters
# accommodates the simulator's density, not tether — not a Mandate ① accommodation of a defect.
#
# An UNREADABLE counter (no such sysctl on this kernel) is reported and skipped: it cannot be checked, and
# on the privileged-systemd container hosts the drills actually run on, all of these are readable.
preflight_kernel() {
    local rc=0 cur name min usage
    printf 'preflight: per-uid kernel counters (docker does NOT isolate these)\n'
    # sysctl <minimum> <why it is drawn on by a systemd container>
    while IFS='|' read -r name min why; do
        [ -n "$name" ] || continue
        cur=$(sysctl -n "$name" 2>/dev/null || echo '?')
        case "$cur" in
            ''|'?'|*[!0-9]*) printf '  %-42s %-10s (unreadable — skipped)\n' "$name" "$cur"; continue ;;
        esac
        if [ "$cur" -ge "$min" ]; then
            printf '  %-42s %-10s ok (>= %s)\n' "$name" "$cur" "$min"
        else
            printf '  %-42s %-10s LOW (want >= %s) — %s\n' "$name" "$cur" "$min" "$why"
            rc=1
        fi
    done <<'COUNTERS'
fs.inotify.max_user_instances|2048|systemd+journald+units each open several; the convicted 2026-07-09 root cause
fs.inotify.max_user_watches|65536|journald and unit-file watching scale with container count
kernel.keys.root_maxkeys|5000|systemd PID1 allocates session keyrings; privileged containers run PID1 as host uid 0, so root_maxkeys (NOT the non-root maxkeys) applies
kernel.keys.root_maxbytes|500000|paired with root_maxkeys; exhaustion kills PID1 the same way
kernel.pid_max|65536|38 clusters x (systemd + journald + nats-server + tether + sshd) plus exec children
kernel.threads-max|100000|Go runtimes are thread-hungry; nats-server and tether both are Go
fs.file-max|500000|sockets + JS store fds across every container
fs.aio-max-nr|65536|drawn on by the storage stack under concurrent fsync
net.netfilter.nf_conntrack_max|262144|one bridge network per drill instance, all NATed
net.ipv4.neigh.default.gc_thresh3|4096|ARP/neighbour table across ~200 container veth pairs at high -j (plan M5)
COUNTERS
    if [ "$rc" != 0 ]; then
        printf '  usage right now: inotify instances held=%s ; keys in use=%s\n' \
            "$(find /proc/*/fd -lname 'anon_inode:inotify' 2>/dev/null | wc -l)" \
            "$(awk 'NR>1{n+=$2} END{print n+0}' /proc/key-users 2>/dev/null || echo '?')"
        printf '  FIX ONCE on this host (sudo -n is unavailable here, so this is interactive). The block\n'
        printf '  below is COLUMN-0 on purpose so it can be pasted verbatim — an indented heredoc EOF\n'
        printf '  would never terminate (MI2):\n'
        # The pasteable block MUST be flush-left: a heredoc delimiter after leading spaces does not close
        # the heredoc, so an indented EOF would swallow `sudo sysctl --system` into the file.
        printf 'sudo tee /etc/sysctl.d/99-simcluster.conf >/dev/null <<EOF\n'
        while IFS='|' read -r name min _; do [ -n "$name" ] && printf '%s=%s\n' "$name" "$min"; done <<'COUNTERS2'
fs.inotify.max_user_instances|8192|
fs.inotify.max_user_watches|524288|
kernel.keys.root_maxkeys|5000|
kernel.keys.root_maxbytes|1000000|
COUNTERS2
        printf 'EOF\nsudo sysctl --system\n'
    fi
    # REPORT-not-refuse for the extended table: a low value is printed loudly, but only the inotify cap
    # (checked separately below) actually blocks the run — it is the one with a proven, reproduced failure
    # mode. The rest are density headroom, surfaced so the next real limit is caught by this table rather
    # than by another multi-week misdiagnosis.
    return 0
}
if [ "$PREFLIGHT" = 1 ]; then preflight_kernel; preflight_inotify || exit 3; fi

# External review Medium 6: drill argv/output routinely carry session PINs, invite tokens, and confirm
# values. The flight recorder persists them, so $LOGDIR must not be world-readable on a shared sim host.
# Create everything under a 077 umask (dirs 0700, files 0600) so evidence is owner-only.
umask 077
mkdir -p "$LOGDIR"; chmod 0700 "$LOGDIR" 2>/dev/null || true
# Re-review Medium 5, defence 4: destructive cleanup may run ONLY on a directory that is OURS. A marker
# is an authorization capability, so it must have a versioned exact value; mere existence (or a symlink)
# is not enough. There is deliberately no special exemption for /tmp/simdrills: an arbitrary populated
# directory is unsafe even when its spelling equals the default.
#
# REPLAY MUST NOT WRITE THE MARKER. The previous implementation blessed every arbitrary directory passed
# to --replay; a later live invocation then treated it as owned and deleted unrelated *.log files. Replay
# may rewrite only its documented derived rollups and cannot grant future destructive authority.
_ld_marker="$LOGDIR/.simdrills-owned"
_ld_marker_value="tether-simdrills-v1"
if [ "$REPLAY" = 0 ]; then
    if [ -L "$_ld_marker" ]; then
        echo "run-drills: refusing to clean --logdir '$LOGDIR' — ownership marker is a symlink" >&2
        exit 2
    elif [ -e "$_ld_marker" ]; then
        _ld_marker_got=$(cat "$_ld_marker" 2>/dev/null)
        if [ "$_ld_marker_got" = "$_ld_marker_value" ]; then
            :
        elif [ -z "$_ld_marker_got" ] && [ -f "$LOGDIR/rollup.tsv" ] &&
             grep -q '^RUN-COMPLETE' "$LOGDIR/progress.tsv" 2>/dev/null; then
            # One-time safe migration from the developer response's empty marker. Require a completed
            # historical run, not mere marker presence: replay could create rollup.tsv in an arbitrary
            # directory, but it cannot forge RUN-COMPLETE.
            printf '%s\n' "$_ld_marker_value" >"$_ld_marker" || exit 9
        else
            echo "run-drills: refusing to clean --logdir '$LOGDIR' — ownership marker is invalid or an unverifiable legacy marker" >&2
            exit 2
        fi
    elif [ -n "$(ls -A "$LOGDIR" 2>/dev/null)" ]; then
        echo "run-drills: refusing to clean --logdir '$LOGDIR' — it is non-empty and not a simcluster run dir (no valid .simdrills-owned marker). Point --logdir at a dedicated/empty directory." >&2
        exit 2
    else
        printf '%s\n' "$_ld_marker_value" >"$_ld_marker" || {
            echo "run-drills: cannot write ownership marker in '$LOGDIR'" >&2
            exit 9
        }
    fi
fi
if [ "$REPLAY" = 0 ]; then
    rm -f "$LOGDIR"/*.log "$LOGDIR"/*.rc "$LOGDIR"/*.timeout "$LOGDIR"/*.secs \
          "$LOGDIR"/*.runpid "$LOGDIR"/rollup.txt "$LOGDIR"/rollup.tsv "$LOGDIR"/progress.tsv \
          "$LOGDIR"/host-telemetry.tsv 2>/dev/null || true
    rm -rf "$LOGDIR"/evidence "$LOGDIR"/evidence-attempt2 2>/dev/null || true
    mkdir -p "$LOGDIR/evidence" 2>/dev/null || true; chmod 0700 "$LOGDIR/evidence" 2>/dev/null || true
else
    # Replay must not destroy what it is analysing. Only the derived rollup is rewritten.
    rm -f "$LOGDIR"/rollup.txt "$LOGDIR"/rollup.tsv 2>/dev/null || true
fi

if [ -t 1 ]; then C_G=$'\033[32m'; C_R=$'\033[31m'; C_Y=$'\033[33m'; C_0=$'\033[0m'; else C_G=; C_R=; C_Y=; C_0=; fi

# Every drill is bounded by --drill-timeout. WHY: a drill that wedges (an unbounded poll_until is the
# usual way) used to hang the WHOLE sweep — `wait` never returns, the summary is never printed, and the
# ssh driver on the external box just sits there looking dead. `timeout` turns that unbounded hang into a bounded,
# self-explaining INFRA-ABORT. `-k 30` matters as much as the ceiling itself: without it a child that
# ignores TERM would re-introduce exactly the hang we are trying to kill.
# The elapsed check exists because `-k` changes the reported status: a TERM-ignoring drill is SIGKILLed
# and surfaces as 137, not 124. 137 alone is ambiguous (a plain OOM-kill is also 137) and must not be
# mislabelled a timeout, so 137 only counts when the clock actually ran out.
# run_one <drill> [out_basename] [evidence_subdir]
# The drill NAME (passed to `simcluster drill`) is always $1. The OUTPUT basename defaults to $1 but the
# attribution pass overrides it to "$1.attempt2" so a re-run writes to its OWN files (B4): the previous
# scheme copied $1.* aside, re-ran onto $1.*, then moved the copies back with NO trap — a SIGKILL in that
# window left the GREEN re-run as $1.log and the correct red orphaned under a name nobody reads, so a
# replay reported the regression as ALL GREEN. Writing straight to a distinct basename removes the window
# entirely. The evidence subdir is likewise separated (MA3) so the two runs' records never commingle.
run_one() {
    local drill="$1" out="${2:-$1}" evsub="${3:-evidence}" t0 rc el
    t0=$(secs)
    progress_row launch "$out" - - "$t0"
    # M1: the drill's flight recorder writes here. Exported (not passed) because it must reach
    # lib/assert.sh inside `simcluster drill`, several processes down. SIM_DRILL_ID is the OUTPUT basename
    # so the evidence file agrees with what the deviation report opens for this run (B1).
    ( export SIM_EVIDENCE_DIR="$LOGDIR/$evsub" SIM_DRILL_ID="$out"
      mkdir -p "$LOGDIR/$evsub" 2>/dev/null
      # Each drill owns a process group. A signal to the runner can then terminate timeout, simcluster,
      # the drill shell, and every grandchild atomically; killing only run_one's wrapper shell orphaned the
      # rest of the tree. The pid file is private under the 0700 logdir and removed after wait.
      setsid timeout -k 30 "$DRILL_TIMEOUT" "$SIM" drill "$drill" >"$LOGDIR/$out.log" 2>&1 &
      _run_pg=$!
      printf '%s\n' "$_run_pg" >"$LOGDIR/$out.runpid"
      wait "$_run_pg"; _run_rc=$?
      rm -f "$LOGDIR/$out.runpid"
      echo "$_run_rc" >"$LOGDIR/$out.rc" )
    rc=$(cat "$LOGDIR/$out.rc" 2>/dev/null || echo '?')
    # M3/R5: this elapsed value was ALREADY computed here for the --drill-timeout classification below
    # and then discarded. Persisting it is what makes longest-first scheduling (V1) possible at all, and
    # it costs one file write. Accumulate across attempts so a retried drill reports total server time.
    el=$(( $(secs) - t0 ))
    if [ -f "$LOGDIR/$out.secs" ]; then el=$(( el + $(cat "$LOGDIR/$out.secs" 2>/dev/null || echo 0) )); fi
    echo "$el" >"$LOGDIR/$out.secs"
    progress_row done "$out" "$rc" "$el" "$(secs)"
    if [ "$rc" = 124 ] || { [ "$rc" = 137 ] && [ "$(( $(secs) - t0 ))" -ge "$DRILL_TIMEOUT" ]; }; then
        # Marker file, not a log grep: the log of a wedged drill very often ALSO carries a flake
        # signature from an earlier step, and is_flake must not resurrect it (see is_flake).
        : >"$LOGDIR/$out.timeout"
        printf '\n*** run-drills: KILLED BY --drill-timeout after %ss (rc=%s). The drill never reached\n*** drill_end, so it counts as INFRA-ABORT and is deliberately NOT retried — a wedge is not a\n*** flake. The tail above is the last step it made progress on; start there.\n' \
            "$DRILL_TIMEOUT" "$rc" >>"$LOGDIR/$out.log"
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
    # Internal review MA8: match FLAKE_SIG only on lines that could actually carry an infra failure —
    # NOT on a SUCCESSFUL poll's own description or a PASS/NOT-COVERED line. Since M3, a successful
    # `poll_until` logs "poll_until: condition met after Ns … <desc>", and a desc that happens to contain
    # a FLAKE_SIG phrase (e.g. "…container not running…") would otherwise make a genuine SETUP-RED
    # retry-eligible → the green retry becomes the verdict of record → a real red laundered. Exclude the
    # non-failure lines before matching.
    grep -vE '^\[simcluster\] poll_until: condition met|^\[ ok \] PASS|NOT-COVERED' "$LOGDIR/$1.log" 2>/dev/null \
        | grep -qE "$FLAKE_SIG" || return 1
    case "$(effective_verdict "$1")" in INFRA-ABORT|SETUP-RED) return 0 ;; *) return 1 ;; esac
}
secs() { date +%s; }

# The first failing assertion's line, colour-stripped, capped — the "signature" the attribution pass and
# the band matcher compare against. Takes an output basename (so it can read $d or $d.attempt2). Capped at
# 240 (was 120): the distinguishing tail of a long assertion — e.g. drill 52's "… error: not leader" that
# separates its #69 band from any other retire failure — sits past 120 chars, so a band signature could
# not reach it. 240 is generous while still bounding a pathological line.
_first_fail_sig() {
    grep -m1 -E '^\[err \] (FAIL|SETUP-FAIL|PRODUCT-RED|HARNESS-ERROR)' "$LOGDIR/$1.log" 2>/dev/null \
        | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-240
}

# ── M3: THE EXPECTATION TABLE ───────────────────────────────────────────────────────────────────────
# Until now this runner never opened expected-verdicts.tsv. It failed closed on every non-GREEN verdict
# and printed a flat blocker count — so on 2026-07-23 it reported "14 BLOCKER(S)" of which NINE were
# recorded, owned, deliberate INCOMPLETEs, and the TWO rows that were a genuine product regression
# (c6b9c9e's new mandatory --reset-js gate, unswept to the drill call sites) were indistinguishable from
# the background noise. Finding them cost a human evening of solo re-runs.
#
# The expectation is a SECOND AXIS, never a waiver: `match` says whether this run agrees with what we
# already knew, while the verdict still decides whether the drill blocks. Nothing below can turn a red
# green — see the exit-code law at the bottom of this file, which is untouched.
EXPECTED_TSV="$HERE/expected-verdicts.tsv"
COSTS_TSV="$HERE/drill-costs.tsv"
_exp_field() {  # _exp_field <drill> <col>  → the column, or '' when the drill/table is absent
    [ -f "$EXPECTED_TSV" ] || return 0
    awk -F'\t' -v d="$1" -v c="$2" '!/^#/ && NF==6 && $1==d {print $c; exit}' "$EXPECTED_TSV"
}
EXPECTED_LOG="$HERE/expected-verdicts-log.md"
# Resolve a band's `sig:<slug>` to the ERE it stands for. Definitions live in expected-verdicts-log.md as
#     sig:<slug> := <ERE>
# so a band names a pattern a human can read AND a machine can match, and validate-verdicts.sh proves the
# definition exists. Prints the ERE, or '' if undefined.
_sig_regex() {
    [ -f "$EXPECTED_LOG" ] || return 0
    # Re-review Medium 6: only a literal safe slug may be interpolated into this sed (a `/` in the slug
    # would terminate the s/// and silently resolve to empty). validate-verdicts enforces the same grammar;
    # this is defence in depth so a hand-edited table can never inject a sed metacharacter here.
    case "$1" in *[!A-Za-z0-9._-]*|'') return 0 ;; [!A-Za-z0-9]*) return 0 ;; esac
    sed -n "s/^[[:space:]]*sig:$1[[:space:]]*:=[[:space:]]*//p" "$EXPECTED_LOG" | head -1
}
# classify_match <drill> <verdict> <nc_gap> <first_fail_sig> → MATCH | MATCH-BAND(#NN) | DEVIATION | NO-EXPECTATION
# _fail_context <drill> : the CAUSE diagnostic — the physical line IMMEDIATELY before the first `[err]`,
# colour-stripped, and only if it is `[simcluster]`/`[warn]`. A band signature is matched against THIS,
# NOT the `[err]`
# assertion-title line. External review re-review Major 1: drill 74's B-negctrl assertion title is a bare
# positive-assertion label ("… create command succeeded (want exit 0, got 1)") that carries no cause, so a
# title signature laundered ANY failure (rc=64 permission, malformed args, …) as MATCH-BAND(#67). Matching
# a WINDOW (or merely "the last diagnostic anywhere") is also unsafe: an old matching rc=70 can authorize
# a later cause-free failure after intervening PASS/output lines. Requiring physical adjacency makes stale
# causes fail closed. A
# drill that wants a band MUST therefore emit its actual cause as the final `[simcluster]`/`[warn]` line
# immediately before the assertion.
_fail_context() {
    local log="$LOGDIR/$1.log" ln
    ln=$(grep -nm1 -E '^\[err \] (FAIL|SETUP-FAIL|PRODUCT-RED|HARNESS-ERROR)' "$log" 2>/dev/null | cut -d: -f1)
    [ -n "$ln" ] || return 0
    [ "$ln" -gt 1 ] || return 0
    sed -n "$((ln-1))p" "$log" 2>/dev/null \
        | sed 's/\x1b\[[0-9;]*m//g' \
        | grep -E '^\[(simcluster|warn)'
}

classify_match() {
    local d="$1" v="$2" ncg="$3" ffsig="$4" exp expncg bands b bv bid bsig re
    exp=$(_exp_field "$d" 2); [ -n "$exp" ] || { printf 'NO-EXPECTATION'; return; }
    expncg=$(_exp_field "$d" 3)
    if [ "$v" = "$exp" ]; then
        # An expectation pins the coverage debt too: a drill that lands INCOMPLETE with a DIFFERENT
        # number of gaps than recorded has changed, even though its verdict enum did not. That is how a
        # silently-lost (or silently-added) gap gets caught instead of blending into "still INCOMPLETE".
        if [ -n "$expncg" ] && [ "$expncg" != '-' ] && [ -n "$ncg" ] && [ "$ncg" != '-' ] && [ "$expncg" != "$ncg" ]; then
            printf 'DEVIATION'; return
        fi
        printf 'MATCH'; return
    fi
    bands=$(_exp_field "$d" 4)
    if [ -n "$bands" ] && [ "$bands" != '-' ]; then
        local IFS=,
        for b in $bands; do
            # B3: a band is VERDICT@#NN@sig:<slug>. It matches ONLY when the verdict enum agrees AND the
            # first-failure line matches the band's signature. Matching on the enum alone (the original
            # code) let a signature-bearing band absorb a BRAND-NEW, unrelated red of the same enum — the
            # exact "20/91 blindness inside every banded drill" the plan and the table header forbid. A
            # band whose sig does not match this run's failure is therefore a DEVIATION, not a MATCH-BAND
            # (which is also rule 4: "a band whose re-run shows a different signature is reclassified").
            bv=${b%%@*}; bid=${b#*@}; bid=${bid%%@*}
            bsig=${b##*@}                     # e.g. sig:c-ss-flow
            [ "$bv" = "$v" ] || continue
            re=$(_sig_regex "${bsig#sig:}")
            # Match the sig against the FAILURE CONTEXT (cause + assertion), never the bare title (re-review
            # Major 1). A missing log yields no context and no match → DEVIATION (safe: an unconfirmable
            # cause is never laundered as a known band). ffsig is retained in the signature only for the
            # attribution pass's cross-run comparison, not for band matching.
            if [ -n "$re" ] && _fail_context "$d" | grep -qE "$re"; then
                printf 'MATCH-BAND(%s)' "$bid"; return
            fi
        done
    fi
    printf 'DEVIATION'
}

# ── M3: THE PROGRESS STREAM ─────────────────────────────────────────────────────────────────────────
# rollup.tsv is written only after every drill has finished. On 2026-07-23 one 22-minute straggler meant
# there was NO machine-readable summary for 47 minutes, and a killed sweep leaves none at all. Each row
# below is a single short `printf >>`, i.e. under PIPE_BUF, so concurrent writers cannot interleave a
# partial line. The RUN-COMPLETE sentinel is the ONLY thing that makes this file a conclusion: without
# it a reader must report "partial", never "these are the results".
PROGRESS_TSV="$LOGDIR/progress.tsv"
progress_row() { printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" >>"$PROGRESS_TSV" 2>/dev/null || true; }

# ── M3: HOST CO-MEASUREMENT ─────────────────────────────────────────────────────────────────────────
# fsync latency is the one coupling docker does not isolate (measured p50 6.4ms / p99 13.0ms at -j6, vs
# 0.1-0.5ms nominal for this NVMe). Recording it per sweep means every verdict permanently carries the
# storage regime it ran under, so a later disposition can say "this red happened on a loaded disk" with
# evidence instead of a hunch. $LOGDIR and /var/lib/docker are on the same filesystem here, which the
# check below verifies rather than assumes — a canary on a different device would measure nothing.
# fsync_probe_pctl : "p50 p99" over N 4KiB fdatasync writes, in ms; "- -" if it cannot be measured.
# Internal review MA4: this reports BOTH percentiles because R6's Phase-4 trigger is written against the
# p99 (the tail is what widens under contention; the p50 is device-bound here at ~6.4 ms idle or loaded,
# so a p50-only number cannot distinguish load from the disk's own flush cost).
fsync_probe_pctl() {
    local n="${1:-200}" f="$LOGDIR/.fsync-canary.$$"
    command -v python3 >/dev/null 2>&1 || { printf -- '- -'; return; }
    python3 - "$f" "$n" <<'PY' 2>/dev/null || printf -- '- -'
import os,sys,time
p,n=sys.argv[1],int(sys.argv[2]); buf=b'\0'*4096; s=[]
fd=os.open(p,os.O_CREAT|os.O_WRONLY|os.O_TRUNC,0o600)
try:
    for _ in range(n):
        t=time.perf_counter(); os.pwrite(fd,buf,0); os.fsync(fd); s.append((time.perf_counter()-t)*1000)
finally:
    os.close(fd); os.unlink(p)
s.sort(); print(f"{s[len(s)//2]:.3f} {s[min(len(s)-1,(len(s)*99)//100)]:.3f}")
PY
}
storage_same_fs() {  # 0 iff $LOGDIR and the docker root are on one device (else the canary is bogus)
    local a b
    a=$(findmnt -no SOURCE -T "$LOGDIR" 2>/dev/null || echo A)
    b=$(findmnt -no SOURCE -T /var/lib/docker 2>/dev/null || echo B)
    [ "$a" = "$b" ]
}

started=$(secs)
# MA4: take an IDLE storage baseline before any drill runs, so the loaded numbers recorded per-failure
# have something to be compared against (R6's trigger is a delta, not an absolute). same_fs tells us
# whether the canary even measures what the drills experience.
FSYNC_BASELINE="- -"; FSYNC_SAMEFS="unknown"; TELEMETRY_PID=""
HOST_TELEMETRY_TSV="$LOGDIR/host-telemetry.tsv"
if [ "$REPLAY" = 0 ]; then
    # M3 preflight (external review Major 4): the canary must be measurable and on the SAME filesystem as
    # the drills' store, or it measures the wrong device. An UNMEASURABLE probe (no python3 / probe error)
    # is an INFRA-ABORT — the sweep's storage evidence would be a fabrication. A DIFFERENT filesystem is a
    # loud, recorded WARNING rather than an abort: the canary is then merely non-representative, which is
    # worth flagging but not worth killing a whole sweep over (plan M5-style amendment, recorded here).
    FSYNC_BASELINE="$(fsync_probe_pctl 200)"
    case "$FSYNC_BASELINE" in
        '- -'|'-'*) echo "run-drills: fsync canary could not be measured (no python3 or probe error) — INFRA-ABORT (the storage regime this sweep runs under would be unknown)" >&2; exit 3 ;;
    esac
    if storage_same_fs; then FSYNC_SAMEFS="yes (LOGDIR and docker root share a device)"
    else FSYNC_SAMEFS="NO — canary is on a DIFFERENT device than the drills' store (readings are non-representative)"
        echo "run-drills: WARNING — $LOGDIR and /var/lib/docker are on different filesystems; the fsync canary does not measure what the drills experience." >&2
    fi
    # M3 background sampler: every 60s append the host's storage/load regime, so a per-drill red can be
    # correlated with the contention at that moment (not just the idle baseline). Killed at sweep end.
    # stdin/out/err are redirected to /dev/null so the sampler does NOT hold open a command-substitution
    # pipe (a `out=$(run-drills …)` caller would otherwise block until the sampler died — this hung the
    # hermetic verdict-contract-test's synthetic sweeps).
    _runner_pid=$$
    ( while :; do
        # SELF-TERMINATE if the runner is gone, so a lost trap (e.g. an ssh SIGHUP the parent did not
        # forward) can never leak an immortal 60s loop on a shared sim host.
        kill -0 "$_runner_pid" 2>/dev/null || exit 0
        _ts=$(date +%s); _la=$(cut -d' ' -f1 /proc/loadavg 2>/dev/null)
        # running_drills = launched − completed (external review re-review Major 2: the old `ls *.rc | wc`
        # counted CUMULATIVE completed attempts, which never decreases and is not "running").
        # grep -c PRINTS 0 while returning rc=1 when there are no matches. `|| echo 0` therefore produces
        # "0\n0", which is not an integer and killed the sampler precisely while drills were launched but
        # none had completed. Preserve grep's printed count and swallow only its status.
        _lc=$(grep -c '^launch' "$PROGRESS_TSV" 2>/dev/null || true)
        _dc=$(grep -c '^done' "$PROGRESS_TSV" 2>/dev/null || true)
        case "$_lc" in ''|*[!0-9]*) _lc=0 ;; esac
        case "$_dc" in ''|*[!0-9]*) _dc=0 ;; esac
        _rd=$(( _lc - _dc )); [ "$_rd" -lt 0 ] && _rd=0
        _cc=$(docker ps -q 2>/dev/null | wc -l); _fp=$(fsync_probe_pctl 40 | awk '{print $1}')
        printf '%s\t%s\t%s\t%s\t%s\n' "$_ts" "${_la:-?}" "$_rd" "${_fp:--}" "$_cc" >>"$HOST_TELEMETRY_TSV" 2>/dev/null || true
        sleep 60
      done ) </dev/null >/dev/null 2>&1 &
    TELEMETRY_PID=$!
    # Ensure the sampler dies with the sweep however it exits (HUP included — an ssh drop is a SIGHUP).
    # External review re-review Major 2: a signal handler must TERMINATE a partial sweep, not clean up and
    # RETURN to the runner — returning let execution flow on to the summary and forge a RUN-COMPLETE
    # sentinel for an interrupted run. The EXIT trap does the sampler cleanup (fires on any exit); the
    # INT/TERM/HUP handler kills the sampler AND the drill children AND exits with a signal-derived status,
    # so it never reaches the sentinel line.
    trap '[ -n "${TELEMETRY_PID:-}" ] && kill "$TELEMETRY_PID" 2>/dev/null; wait "$TELEMETRY_PID" 2>/dev/null' EXIT
    # Recursively signal a tracked run_one process and all descendants. Killing only run_one's shell
    # orphaned timeout -> simcluster -> drill; those processes kept mutating the cluster and log after the
    # runner had returned 143. Linux /proc is available on the only supported sim host and avoids adding a
    # pgrep dependency. Descendants are signalled before their parent so the tree cannot be re-parented
    # before it is enumerated.
    _signal_tree() {
        local _st_sig="$1" _st_root="$2" _st_child
        [ -n "$_st_root" ] || return 0
        if [ -r "/proc/$_st_root/task/$_st_root/children" ]; then
            for _st_child in $(cat "/proc/$_st_root/task/$_st_root/children" 2>/dev/null); do
                _signal_tree "$_st_sig" "$_st_child"
            done
        fi
        kill "-$_st_sig" "$_st_root" 2>/dev/null || true
    }
    _on_signal() {
        local _spf _spg
        [ -n "${TELEMETRY_PID:-}" ] && kill "$TELEMETRY_PID" 2>/dev/null
        for _spf in "$LOGDIR"/*.runpid; do
            [ -f "$_spf" ] || continue
            _spg=$(cat "$_spf" 2>/dev/null)
            case "$_spg" in ''|*[!0-9]*) continue ;; esac
            kill -TERM -- "-$_spg" 2>/dev/null || true
        done
        for _sp in ${_drill_pids:-}; do _signal_tree TERM "$_sp"; done
        /bin/sleep 1
        for _spf in "$LOGDIR"/*.runpid; do
            [ -f "$_spf" ] || continue
            _spg=$(cat "$_spf" 2>/dev/null)
            case "$_spg" in ''|*[!0-9]*) continue ;; esac
            kill -KILL -- "-$_spg" 2>/dev/null || true
        done
        for _sp in ${_drill_pids:-}; do
            kill -0 "$_sp" 2>/dev/null && _signal_tree KILL "$_sp"
            wait "$_sp" 2>/dev/null || true
        done
        rm -f "$LOGDIR"/*.runpid 2>/dev/null || true
        printf '\nrun-drills: interrupted by SIG%s — partial sweep, NO RUN-COMPLETE sentinel written.\n' "$1" >&2
        exit "$2"
    }
    trap '_on_signal INT 130'  INT
    trap '_on_signal TERM 143' TERM
    trap '_on_signal HUP 129'  HUP
fi
echo "run-drills: ${#DRILLS[@]} drills | jobs=$JOBS | stagger=${STAGGER}s | retry=$RETRY | logs=$LOGDIR"
echo "  drills: ${DRILLS[*]}"
echo

# ── parallel pass ──────────────────────────────────────────────────────────────────────────────────
# external-review R4 Q5: tell drills whether this is a CONCURRENT run so a grow VOTER-timeout is diagnosed as the
# grow-timing concurrency flake (JOBS>1) vs the #31 grow-lock serialized-fence / real constructibility (solo).
[ "$JOBS" -gt 1 ] && export SIM_CONCURRENT=1 || export SIM_CONCURRENT=0
# ── V1: LONGEST-PROCESSING-TIME-FIRST ───────────────────────────────────────────────────────────────
# The wall clock of a parallel sweep is bounded below by max(sum/j, longest_single_drill). Lexicographic
# launch order does not change either bound, but it routinely BREAKS the first one: on 2026-07-23 the
# 22.2-minute 96-mid-flight-chaos started at minute 25 of a 47.4-minute sweep, so the tail was one drill
# running alone while everything else had finished. The true floor at -j6 is max(195.1/6, 22.3) = 32.5
# min; greedy longest-first reaches ~32.7. Starting the long poles first costs nothing and recovers ~15
# of the 47.4 - 32.5 minutes the alphabetical tail wasted. (It is NOT 47.4 -> 28.8: that arithmetic
# removed 96 from the sum while still dividing by all 6 workers, one of which runs 96 — impossible.)
#
# Ordering is DETERMINISTIC (cost desc, then name) so two sweeps of the same suite launch identically —
# a reordering that shuffled neighbours between runs would make timing-coupled reds impossible to
# compare. An unknown drill sorts first at max cost: a new drill is far more dangerous as an unexpected
# straggler than as an unnecessarily early start.
# LAUNCH order and SUMMARY order are different concerns: the runner wants long-poles-first, a human
# reading the rollup wants the drills in name order. Sorting DRILLS itself would silently reorder every
# rollup row too, so the cost order lives in its own array.
LAUNCH_ORDER=("${DRILLS[@]}")
if [ "$LPT" = 1 ] && [ -f "$COSTS_TSV" ] && [ "${#DRILLS[@]}" -gt 1 ]; then
    mapfile -t LAUNCH_ORDER < <(
        for d in "${DRILLS[@]}"; do
            c=$(awk -F'\t' -v d="$d" '!/^#/ && NF>=2 && $1==d {print $2; exit}' "$COSTS_TSV")
            case "$c" in ''|*[!0-9]*) c=999999 ;; esac
            printf '%s\t%s\n' "$c" "$d"
        done | sort -k1,1nr -k2,2 | cut -f2
    )
fi

launched=0
if [ "$REPLAY" = 1 ]; then
    echo "run-drills: --replay — classifying ${#DRILLS[@]} drill(s) from existing logs in $LOGDIR (nothing is run)"
else
# V6 was reverted after external review proved every public precheck token forgeable. Fast pre-abort:
# check the image once up front so a stale binary fails the whole sweep here with one
# clear message rather than 38 identical per-drill failures. Correctness no longer depends on this —
# cmd_drill re-checks every drill (re-review Medium 4 removed the forgeable skip) — it is purely an early
# exit.
"$SIM" check-image >/dev/null 2>&1 || { "$SIM" check-image || exit 3; }
# The M3 telemetry sampler is also a background job, so the concurrency cap and the final wait must
# EXCLUDE it — a bare `wait` here would block forever on the sampler's infinite loop, and `jobs -rp`
# would over-count it by one. Track drill PIDs explicitly and offset the cap by the sampler.
_samp_running() { [ -n "${TELEMETRY_PID:-}" ] && kill -0 "$TELEMETRY_PID" 2>/dev/null && echo 1 || echo 0; }
_drill_pids=""
run_one_serial() {
    _drill_pids=""
    run_one "$@" &
    _drill_pids="$!"
    wait "$_drill_pids" 2>/dev/null || true
    _drill_pids=""
}
for d in "${LAUNCH_ORDER[@]}"; do
    if [ "$launched" -gt 0 ] && [ "$STAGGER" -gt 0 ]; then sleep "$STAGGER"; fi
    while [ "$(( $(jobs -rp | wc -l) - $(_samp_running) ))" -ge "$JOBS" ]; do sleep 2; done
    printf '[%s] launch %-30s (%d/%d)\n' "$(date +%H:%M:%S)" "$d" "$((launched+1))" "${#DRILLS[@]}"
    run_one "$d" &
    _drill_pids="$_drill_pids $!"
    launched=$((launched+1))
done
for _p in $_drill_pids; do wait "$_p" 2>/dev/null || true; done
_drill_pids=""
fi

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
            run_one_serial "$d"
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
devs=(); banded=(); bandreds=0; noexp=0
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
    # M3: the second axis. `match` compares this run against the recorded expectation; it NEVER changes
    # `note`, `blockers`, or the exit code above — a banded or expected red still blocks. What it buys is
    # that a reader no longer has to hand-diff 38 rows against a 60-line prose table to find the ones
    # that actually changed.
    ctr="$(verdict_counters "$d" "$v")"
    ncg="$(printf '%s' "$ctr" | cut -f5)"
    exp="$(_exp_field "$d" 2)"; [ -n "$exp" ] || exp='-'
    match="$(classify_match "$d" "$v" "$ncg" "$(_first_fail_sig "$d")")"
    dur="$(cat "$LOGDIR/$d.secs" 2>/dev/null || echo -)"
    att=1; fv="$v"
    if [ -f "$LOGDIR/$d.attempt1.log" ]; then
        att=2
        fv=$(sed -n 's/^DRILL-VERDICT verdict=\([^ ]*\) .*/\1/p' "$LOGDIR/$d.attempt1.log" 2>/dev/null | head -1)
        [ -n "$fv" ] || fv=INFRA-ABORT
    fi
    case "$match" in
        DEVIATION) devs+=("$d"); mcol="$C_R" ;;
        MATCH-BAND*) bandreds=$((bandreds+1)); banded+=("$d"); mcol="$C_Y" ;;
        NO-EXPECTATION) noexp=$((noexp+1)); mcol="$C_Y" ;;
        *) mcol="" ;;
    esac
    say '  %s%-19s%s %-30s rc=%s  %s%s%s%s%s\n' "$col" "$v" "$C_0" "$d" "$rc" "$mcol" "$match" "$C_0" "$note" "$tag"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$d" "$v" "$rc" "$ctr" "$dur" "$att" "$fv" "$exp" "$match" >>"$ROLLUP_TSV"
done
say -- '-------------------------------------------------------------------------------\n'
say '  %d drills: %sGREEN=%d%s  %sPRODUCT-RED=%d%s  %sINCOMPLETE=%d%s  %sSETUP-RED=%d%s  %sASSERT-FAIL=%d%s  %sINFRA-ABORT=%d%s  (%ds)\n' \
    "${#DRILLS[@]}" "$C_G" "$n_green" "$C_0" "$C_Y" "$n_prod" "$C_0" "$C_Y" "$n_inc" "$C_0" \
    "$C_R" "$n_setup" "$C_0" "$C_R" "$n_assert" "$C_0" "$C_R" "$n_abort" "$C_0" "$(( $(secs) - started ))"
[ "${#retried[@]}" -gt 0 ] && say '  %sretried (infra flake): %s — first-run evidence in %s/<name>.attempt1.log%s\n' "$C_Y" "${retried[*]}" "$LOGDIR" "$C_0"
# MA4: the storage regime this sweep ran under, recorded so a later LOAD-SENSITIVE disposition has a
# same-host baseline to reason about (the p50 is device-bound; only the tail moves under load).
[ "$REPLAY" = 0 ] && say '  fsync 4KiB idle baseline: p50/p99 = %s ms | same-fs as drill store: %s\n' "$FSYNC_BASELINE" "$FSYNC_SAMEFS"
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

# ── M3: THE DEVIATION REPORT ────────────────────────────────────────────────────────────────────────
# The blocker list above answers "what is not green?". This answers the question that actually costs
# time: "what CHANGED?" On 2026-07-23 the answers were 5 rows out of 14 blockers, and two of those five
# were a real product regression that looked exactly like the other twelve.
# M4 queue: expected-GREEN deviations FIRST. A drill whose expectation is GREEN has no recorded reason
# to be red at all, so it is the likeliest place a genuine regression is hiding — and on 2026-07-23 that
# priority would have put 20, 52 and 91 ahead of 30 and 74, i.e. both real regressions in the first three.
# Banded reds go last: they are the ones we already have a story for, and re-running them CONFIRMS the
# story rather than discovering it.
attrq=()
for d in "${devs[@]:-}"; do [ -n "$d" ] && [ "$(_exp_field "$d" 2)" = GREEN ] && attrq+=("$d"); done
for d in "${devs[@]:-}"; do [ -n "$d" ] && [ "$(_exp_field "$d" 2)" != GREEN ] && attrq+=("$d"); done
for d in "${banded[@]:-}"; do [ -n "$d" ] && attrq+=("$d"); done

say -- '-------------------------------------------------------------------------------\n'
# MA7: NO-EXPECTATION rows are the match axis silently NOT RUNNING for a drill (table absent, drill
# unlisted, or a stray tab making a row not-6-fields). Reporting "NO DEVIATIONS" while N drills had no
# expectation to compare against is the exact false all-clear this axis exists to prevent — announce it.
if [ "$noexp" -gt 0 ]; then
    say '  %s%d DRILL(S) WITH NO EXPECTATION%s — the match axis did not run for them (absent/malformed\n' "$C_Y" "$noexp" "$C_0"
    say '     row in expected-verdicts.tsv). Run `sh tests/validate-verdicts.sh` to see why.\n'
fi
if [ "${#devs[@]}" -eq 0 ]; then
    if [ "$noexp" -gt 0 ]; then
        say '  %sNO DEVIATIONS among drills that HAD an expectation%s%s\n' "$C_Y" "$C_0" \
            "$( [ "$bandreds" -gt 0 ] && printf ' (%d banded red(s), still blocking)' "$bandreds" )"
    else
        say '  %sNO DEVIATIONS%s — every drill matched its recorded expectation%s\n' "$C_G" "$C_0" \
            "$( [ "$bandreds" -gt 0 ] && printf ' (%d banded red(s), still blocking)' "$bandreds" )"
    fi
else
    say '  %s%d DEVIATION(S) FROM expected-verdicts.tsv%s — these are what changed:\n' "$C_R" "${#devs[@]}" "$C_0"
    for d in "${devs[@]}"; do
        v="$(effective_verdict "$d")"; exp="$(_exp_field "$d" 2)"; [ -n "$exp" ] || exp='(none)'
        say '\n  %s%s%s: got %s, expected %s\n' "$C_R" "$d" "$C_0" "$v" "$exp"
        first=$(grep -m1 -E '^\[err \] (FAIL|SETUP-FAIL|PRODUCT-RED|HARNESS-ERROR)' "$LOGDIR/$d.log" 2>/dev/null \
                | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-200)
        [ -n "$first" ] && say '    first failure: %s\n' "$first"
        # B1: the evidence path is whatever the drill ANNOUNCED in its DRILL-EVIDENCE line, not a name we
        # reconstruct — reconstructing it from $d disagreed with the writer (which named the file from the
        # free-text drill_begin title) for all 38 drills, so this always missed and fell back to a log tail.
        ev=$(sed -n 's/^DRILL-EVIDENCE file=\([^ ]*\) .*/\1/p' "$LOGDIR/$d.log" 2>/dev/null | tail -1)
        [ -n "$ev" ] || ev="$LOGDIR/evidence/$d.evidence"
        ffo=$(sed -n 's/^DRILL-EVIDENCE .*first_fail_ord=\([0-9]*\).*/\1/p' "$LOGDIR/$d.log" 2>/dev/null | tail -1)
        # MA2: anchor the body on the FIRST-failure record, not a tail of the whole file. For a drill that
        # continues past its first failure (20/74 print "N PASSED after the first failure") a plain tail is
        # PASS lines and the two appended DRILL-EVIDENCE/DRILL-POLL-WAIT lines, none of which is the cause.
        body=""; crc=""
        if [ -s "$ev" ] && [ -n "$ffo" ] && [ "$ffo" != 0 ]; then
            # The record for ordinal $ffo, from its `=== … ord=$ffo …` header to the next `=== ` or EOF.
            rec=$(awk -v o="$ffo" '
                /^=== / { keep = ($0 ~ ("ord=" o " ")) ? 1 : 0 }
                keep { print }' "$ev")
            body=$(printf '%s\n' "$rec" | awk '/^--- stdout\+stderr/{f=1;next} /^--- end ---/{f=0} f' | head -6)
            crc=$(printf '%s\n' "$rec" | sed -n 's/^rc:[[:space:]]*//p' | head -1)
            say '    evidence: %s (ord %s)\n' "$ev" "$ffo"
        elif [ -s "$ev" ]; then
            body=$(awk '/^--- stdout\+stderr/{f=1;next} /^--- end ---/{f=0} f' "$ev" | head -6)
            say '    evidence: %s\n' "$ev"
        else
            # No evidence file (a drill predicate that swallowed its output — see lib/assert.sh). Fall back
            # to the first-failure line + a few after it, NOT a blind tail of the whole log.
            body=$(sed -n "/^\[err \] \(FAIL\|SETUP-FAIL\|PRODUCT-RED\|HARNESS-ERROR\)/,+4p" "$LOGDIR/$d.log" 2>/dev/null \
                   | sed 's/\x1b\[[0-9;]*m//g' | head -5)
        fi
        [ -n "$body" ] && printf '%s\n' "$body" | while IFS= read -r bl; do say '      | %s\n' "$(printf '%s' "$bl" | cut -c1-160)"; done
        # A refusal-shaped failure is the signature of a CLI CONTRACT CHANGE that did not reach its call
        # sites — the class this project has now recorded SIX times. rc 64-78 is sysexits territory; the
        # obvious `exit 6[0-9]` regex would have missed BOTH observed rcs (70 and 77), so match the rc
        # range numerically and the text separately. MA2: the text arm must match a REFUSAL shape, not any
        # long flag — `--require-credential-rotation` (drill 52, leadership churn) is not a contract change,
        # so a bare `--[a-z-]+` match false-tagged it while missing drill 20.
        drc="$(cat "$LOGDIR/$d.rc" 2>/dev/null || echo 0)"
        shaped=0
        printf '%s' "$body" | grep -qiE 'requires?[ ]+--[a-z-]+|--reset-js|unknown flag|usage:|refus(e|ed|es|al)|must be (run|standalone)|is required' && shaped=1
        for x in "$drc" "$crc"; do
            case "$x" in ''|*[!0-9]*) continue ;; esac
            [ "$x" -ge 64 ] && [ "$x" -le 78 ] && shaped=1
        done
        [ "$shaped" = 1 ] && say '    %sCLI-CONTRACT-SHAPED%s — refusal/usage text or a sysexits rc. Suspect a product\n                         contract change whose call sites were not swept.\n' "$C_Y" "$C_0"
        # A FAIL followed by PASSes means the drill kept going and later steps still worked — usually a
        # journalled/forward-completing sequence rather than a broken one. Worth saying, because it is
        # the difference between "tether is bricked" and "tether refused and told you how to continue".
        # `grep -c` already prints exactly one number and returns 1 when that number is zero, so a
        # `|| echo 0` fallback APPENDS a second line and the arithmetic test below dies with
        # "integer expression expected". Swallow the rc instead of substituting a value.
        seq=$(sed -n "/^\[err \] \(FAIL\|SETUP-FAIL\)/,\$p" "$LOGDIR/$d.log" 2>/dev/null | grep -cE '^\[ ok \] PASS' || true)
        case "${seq:-0}" in ''|*[!0-9]*) seq=0 ;; esac
        [ "$seq" -gt 0 ] && say '    note: %s assertion(s) PASSED after the first failure (sequence continued)\n' "$seq"
    done
    say '\n  Next: the attribution pass re-runs these SOLO. A re-run can only ADD a label\n'
    say '        (REGRESSION / LOAD-SENSITIVE / UNSTABLE); it never changes a verdict or this exit code.\n'
fi

# ── M4: THE ATTRIBUTION PASS ────────────────────────────────────────────────────────────────────────
# The ritual this replaces is "it went red, re-run it, it went green, call it a flake". That ritual is
# how a real regression dies quietly: on 2026-07-23 two of the five deviations were c6b9c9e's unswept
# --reset-js contract change, and a green-on-retry would have buried both.
#
# FOUR RULES, and every one of them is what separates this from the ritual:
#   1. The verdict of record and the EXIT CODE come from the FIRST run, always. A re-run may only ADD a
#      label. `blockers` below is already computed and is never recomputed.
#   2. Comparison is against the EXPECTATION, not against GREEN. Defining "recovered" as "went green"
#      would mislabel 30 and 74, whose expectations are not GREEN in the first place.
#   3. LOAD-SENSITIVE is not an acquittal. It still blocks, and it is closed only by a written
#      disposition — which may well conclude the product misbehaves under load, a product finding.
#   4. Bands are CONFIRMED, not trusted: a banded red is re-run too, and a band whose re-run shows a
#      different signature is reclassified DEVIATION.
if [ "$ATTRIBUTE" = 1 ] && [ "$REPLAY" = 0 ] && [ "${#attrq[@]}" -gt 0 ]; then
    say '\n'
    say '============================== attribution pass ===============================\n'
    say '  %d drill(s) to re-run SOLO, budget %ds. First run stays the verdict of record.\n' "${#attrq[@]}" "$ATTR_BUDGET"
    attr_t0=$(secs)
    for d in "${attrq[@]}"; do
        if [ "$(( $(secs) - attr_t0 ))" -ge "$ATTR_BUDGET" ]; then
            say '  %sUNATTRIBUTED%s %-30s (budget exhausted — still a blocker)\n' "$C_Y" "$C_0" "$d"
            printf 'ATTRIBUTION\t%s\tUNATTRIBUTED\t-\n' "$d" >>"$ROLLUP_TSV"
            continue
        fi
        v1="$(effective_verdict "$d")"; exp="$(_exp_field "$d" 2)"; [ -n "$exp" ] || exp='-'
        sig1=$(_first_fail_sig "$d")
        # B4/MA3: write the re-run to its OWN basename and its OWN evidence subdir. The first-run
        # artifacts ($d.log/.rc/.secs and evidence/$d.evidence) are NEVER touched, so there is no window
        # in which a crash could leave the re-run masquerading as the run of record, and the two runs'
        # evidence records never commingle. effective_verdict/_first_fail_sig read attempt2 explicitly.
        say '  [%s] re-run %-30s (solo)\n' "$(date +%H:%M:%S)" "$d"
        SIM_CONCURRENT=0 run_one_serial "$d" "$d.attempt2" "evidence-attempt2"
        v2="$(effective_verdict "$d.attempt2")"
        sig2=$(_first_fail_sig "$d.attempt2")
        if [ "$v2" = "$exp" ]; then
            label=LOAD-SENSITIVE
            say '    %s%s%s — solo run matched the expectation (%s). STILL A BLOCKER: close it with a\n' "$C_Y" "$label" "$C_0" "$exp"
            say '                    written disposition, which may well be "tether misbehaves under load".\n'
        elif [ "$v2" = "$v1" ] && [ "$sig2" = "$sig1" ]; then
            # MI8: a wedge/timeout/CONTRACT-ERROR has no `[err ]` line, so both signatures are empty and
            # "same signature" is vacuously true. Say so rather than claiming a matched signature.
            if [ -z "$sig1" ]; then
                label=REGRESSION
                say '    %s%s%s (verdict only — no comparable [err] signature; check for a wedge/timeout).\n' "$C_R" "$label" "$C_0"
            else
                label=REGRESSION
                say '    %s%s%s — reproduced SOLO with the same first-failure signature. Deterministic;\n' "$C_R" "$label" "$C_0"
                say '                 this is a product/drill defect, not the environment.\n'
            fi
        else
            label=UNSTABLE
            say '    %s%s%s — solo run matched neither the first run nor the expectation (got %s).\n' "$C_Y" "$label" "$C_0" "$v2"
            say '               Investigate before banding it.\n'
        fi
        printf 'ATTRIBUTION\t%s\t%s\t%s\n' "$d" "$label" "$v2" >>"$ROLLUP_TSV"
    done
    say '  attribution is ADDITIVE: the suite exit code is still %d, from the first run.\n' "$blockers"
fi

say '  rollup: %s | %s\n' "$ROLLUP_TSV" "$ROLLUP_TXT"
say '  progress: %s | evidence: %s/\n' "$PROGRESS_TSV" "$LOGDIR/evidence"
if [ "$REPLAY" = 0 ]; then
    # The sentinel is what distinguishes "the sweep finished" from "the sweep was killed and this file is
    # whatever it had written by then". Readers MUST require it before treating progress.tsv as a result.
    # Written ONLY by a live sweep — a replay analysing an archive must never forge one (MI13/B4), else N
    # replays leave N sentinels over a progress stream with no launch/done rows.
    progress_row RUN-COMPLETE - "$blockers" "${#devs[@]}" "$(secs)"
elif [ -f "$PROGRESS_TSV" ] && ! grep -q '^RUN-COMPLETE' "$PROGRESS_TSV" 2>/dev/null; then
    # Replaying an archive whose own sweep never completed: the on-disk logs are real (each drill's
    # first run), but the SET of drills is whatever had finished when the sweep died — so the deviation
    # picture may be incomplete. Say so; do not present it as a settled result. (A pre-M3 archive has no
    # progress.tsv at all, and is replayed without this warning.)
    say '  %sPARTIAL ARCHIVE%s — progress.tsv has no RUN-COMPLETE sentinel; the replayed sweep did not\n' "$C_Y" "$C_0"
    say '                   finish, so drills it never reached are simply absent, not GREEN.\n'
fi
# Keep shell exit status meaningful instead of wrapping modulo 256 for very large suites.
[ "$blockers" -le 125 ] || blockers=125
exit "$blockers"
