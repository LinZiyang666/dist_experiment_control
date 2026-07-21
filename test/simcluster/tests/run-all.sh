#!/bin/sh
# run-all.sh — the hermetic gate set for test/simcluster/. Seconds to run; no docker, no server.
#
# WHY THIS EXISTS: every gate below was written because a specific false-green got through. Left as
# separate scripts they get run individually and unevenly — the install lint in particular shipped with
# nothing calling it, which is the same failure mode (a gate nobody runs is a gate that does not exist).
# Run this before any deploy-tier run and before closing any batch.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
RC=0
for t in poll-reentrancy-test verdict-contract-test lint-drills lint-install ledger-crosscheck r9d-nonvacuity; do
    printf '%-24s ' "$t"
    if out=$(sh "$HERE/$t.sh" 2>&1); then printf 'PASS\n'
    else printf 'FAIL\n'; printf '%s\n' "$out" | tail -5 | sed 's/^/    /'; RC=1; fi
done
printf '%-24s ' "kept-sites --check"
if out=$(sh "$HERE/kept-sites.sh" --check "$HERE/kept-sites.baseline.tsv" 2>&1); then printf 'PASS\n'
else printf 'FAIL\n'; printf '%s\n' "$out" | tail -5 | sed 's/^/    /'; RC=1; fi
printf -- '--------------------------------------------------------------------------------\n'
[ "$RC" = 0 ] && echo "simcluster hermetic gates: ALL PASS" || echo "simcluster hermetic gates: FAILURES above" >&2
exit $RC
