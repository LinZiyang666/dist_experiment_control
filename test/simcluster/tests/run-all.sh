#!/bin/sh
# run-all.sh — the hermetic gate set for test/simcluster/. No docker, no server. About two minutes:
# poll-reentrancy / deviation-report / accel-final-review / verdict-contract / poll-mode wait on real
# timers (~115s of the total, measured 2026-09-01); everything else is sub-second.
#
# WHY THIS EXISTS: every gate below was written because a specific false-green got through. Left as
# separate scripts they get run individually and unevenly — the install lint in particular shipped with
# nothing calling it, which is the same failure mode (a gate nobody runs is a gate that does not exist).
# Run this before any deploy-tier run and before closing any batch.
#
# AND THEN THE SAME THING HAPPENED TO THIS FILE. Until 2026-09-01 nothing called run-all.sh either —
# not the Makefile, not CI. It is now a line in `make gates` and a step in ci.yml's build-test, and
# test/architecture/simcluster_gate_set_test.go reconciles the loop below against the directory both
# ways (a script beside the set but not in it is red; a name here without a file is red). When it was
# first wired it was already red on a clean tree: ledger-crosscheck reported gotcha #80 as unowned
# because its heading said 已修 and the gate's closed-vocabulary is 已修复. The heading was corrected;
# the vocabulary was not widened (已修 also matches 已修改).
#
# Scripts are run by their shebang: the loop used to invoke everything with `sh`, which made the one
# bash-only script (r16-g67-g69-external-rereview.sh, `set -o pipefail`) exit 2 before its first
# check — and it was not in the loop anyway, so nobody saw that either.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
RC=0
run_script() {
    case "$(sed -n '1p' "$1")" in
        *bash*) bash "$1" ;;
        *) sh "$1" ;;
    esac
}
for t in poll-reentrancy-test verdict-contract-test validate-verdicts validate-verdicts-selftest deviation-report-test poll-mode-test dns-preflight-test simcluster-accel-external-review-test simcluster-accel-external-rereview-test simcluster-accel-final-review-test remote-fs-oracle-contract-test lint-drills lint-install ledger-crosscheck r9d-nonvacuity teardown-recovery-nonvacuity-test kept-sites-selftest r16-g67-g69-external-review r16-g67-g69-external-rereview s7-s9-external-review; do
    printf '%-40s ' "$t"
    if out=$(run_script "$HERE/$t.sh" 2>&1); then printf 'PASS\n'
    else printf 'FAIL\n'; printf '%s\n' "$out" | tail -5 | sed 's/^/    /'; RC=1; fi
done
printf '%-40s ' "kept-sites --check"
if out=$(sh "$HERE/kept-sites.sh" --check "$HERE/kept-sites.baseline.tsv" 2>&1); then printf 'PASS\n'
else printf 'FAIL\n'; printf '%s\n' "$out" | tail -5 | sed 's/^/    /'; RC=1; fi
printf -- '--------------------------------------------------------------------------------\n'
[ "$RC" = 0 ] && echo "simcluster hermetic gates: ALL PASS" || echo "simcluster hermetic gates: FAILURES above" >&2
exit $RC
