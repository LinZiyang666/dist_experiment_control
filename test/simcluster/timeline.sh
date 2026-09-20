#!/bin/sh
# timeline.sh — read a drill's timeline sidecar (<unit>.timeline.tsv, written by lib/log.sh when
# SIM_TIMELINE_FILE is set: run-drills.sh sets it per unit; for a solo run export it yourself:
#     SIM_TIMELINE_FILE=/tmp/98.tl ./local.sh drill 98-stuck-redial-recovery
#
#     sh timeline.sh <file.tsv> [N]      # the N (default 10) largest gaps between consecutive lines,
#                                        # each with the line that PRECEDED the gap — i.e. what the drill
#                                        # was waiting on — plus the poll total and the wall.
#
# WHY. A drill log has no timestamps and poll_until's console line hides everything under 5 s and
# everything inside an assert_ok's captured predicate. Before this file the answer to "where did 22
# minutes go" was a solo re-run with a stopwatch (docs/reviews/simcluster-accel-plan.md §1.2). POSIX sh.
# origin: simcluster-speed plan §5.2 0b.
set -u
F="${1:-}"; N="${2:-10}"
[ -n "$F" ] && [ -f "$F" ] || { printf 'usage: %s <unit.timeline.tsv> [N]\n' "$0" >&2; exit 2; }
case "$N" in ''|*[!0-9]*) printf 'timeline: N must be an integer\n' >&2; exit 2 ;; esac
awk -F'\t' -v N="$N" '
    NF < 3 { next }
    {
        n++
        t[n] = $1; kind[n] = $2; text[n] = $3
        if (n > 1) gap[n] = t[n] - t[n-1]
        if ($2 == "poll") {
            polls++
            if (match($3, /^(met|TIMEOUT) [0-9]+s/)) {
                s = substr($3, RSTART, RLENGTH); sub(/^(met|TIMEOUT) /, "", s); sub(/s$/, "", s)
                pollsum += s
            }
        }
    }
    END {
        if (n == 0) { print "timeline: empty"; exit 0 }
        printf "lines=%d wall=%ds polls=%d poll_sum=%ds (top-level and nested both counted; nested time is inside its parent)\n", n, t[n]-t[1], polls, pollsum
        printf "%-8s %-8s %s\n", "gap_s", "at_s", "line BEFORE the gap (what was being waited on)"
        # selection sort of the N largest gaps — files are hundreds of lines, not millions
        for (k = 1; k <= N; k++) {
            best = 0; bi = 0
            for (i = 2; i <= n; i++) if (!(i in used) && gap[i] > best) { best = gap[i]; bi = i }
            if (bi == 0) break
            used[bi] = 1
            printf "%-8d %-8d [%s] %s\n", best, t[bi-1]-t[1], kind[bi-1], text[bi-1]
        }
    }
' "$F"
