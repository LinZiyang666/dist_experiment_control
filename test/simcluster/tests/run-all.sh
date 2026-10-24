#!/bin/sh
# run-all.sh — the hermetic gate set for test/simcluster/. No docker, no server. About three minutes:
# poll-reentrancy / deviation-report / accel-final-review / verdict-contract / poll-mode wait on real
# timers (~115s of the total, measured 2026-09-01), and arm-aggregation-test adds ~50s more (its lane
# and ceiling cases must let real units run and be killed); everything else is sub-second.
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
#
# SHARED SUPERVISOR. The S8 supply scripts (prev-supply-test, upgrade-artifacts-test) build real Go
# artifacts and refuse to run without SIM_UPGRADE_SUPERVISOR, the executable adapter to the shared
# FIX-ORPHAN supervisor. Nothing provided it, so the set went red on "shared process supervisor is
# required" the day those scripts landed. lib/supervisor.sh builds the adapter once per run from this
# tree into a private directory (a caller may export its own instead). A hermetic copy of this
# dispatcher outside the repository builds nothing and refuses dispatch without a caller-supplied
# adapter — the requirement is never satisfied by skipping a gate.
# origin: hot-update S8 plan §11.2 P22 request (adapter + run-all registration).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
RC=0
SUPERVISOR_DIR=
_runall_cleanup() { [ -z "$SUPERVISOR_DIR" ] || rm -rf "$SUPERVISOR_DIR"; }
trap _runall_cleanup EXIT
trap '_runall_cleanup; trap - EXIT; exit 130' INT
trap '_runall_cleanup; trap - EXIT; exit 143' TERM
_runall_repo=$(CDPATH= cd -- "$HERE/../../.." 2>/dev/null && pwd) || _runall_repo=
if [ -z "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -r "$HERE/../lib/supervisor.sh" ] && [ -f "$_runall_repo/go.mod" ]; then
    . "$HERE/../lib/supervisor.sh"
    printf '%-40s ' "shared supervisor (FIX-ORPHAN) build"
    SUPERVISOR_DIR=$(mktemp -d)
    if out=$(sim_supervisor_build "$_runall_repo" "$SUPERVISOR_DIR" 2>&1); then
        printf 'PASS\n'
        SIM_UPGRADE_SUPERVISOR=$(printf '%s\n' "$out" | tail -1)
        export SIM_UPGRADE_SUPERVISOR
    else
        printf 'FAIL\n'; printf '%s\n' "$out" | tail -5 | sed 's/^/    /'; RC=1
    fi
fi
# Fullstack entries require their P05 name for the same selected candidate
# launcher, including when the caller supplies SIM_UPGRADE_SUPERVISOR.
# origin: S10 internal review FIX-SUPERVISE F2
FULLSTACK_SUPERVISE=${SIM_UPGRADE_SUPERVISOR:-}
export FULLSTACK_SUPERVISE
run_script() {
    # Interpreter selection is exact and fails closed. A Python gate must keep
    # isolation, a Bash gate must retain Bash semantics, and every descendant
    # must be in the shared supervisor's custody before a script can start.
    # origin: hot-update S9 plan §9.5 / P37 / T88 / M34
    [ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] || {
        printf 'hermetic gate: shared supervisor is required\n' >&2; return 2;
    }
    case "$(sed -n '1p' "$1")" in
        '#!/bin/bash'|'#!/usr/bin/env bash') "$SIM_UPGRADE_SUPERVISOR" -- bash "$@" ;;
        '#!/bin/sh'|'#!/usr/bin/env sh') "$SIM_UPGRADE_SUPERVISOR" -- sh "$@" ;;
        '#!/usr/bin/python3'|'#!/usr/bin/env python3') "$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B "$@" ;;
        *) printf 'hermetic gate: unsupported or missing shebang: %s\n' "$1" >&2; return 2 ;;
    esac
}
for t in caddy-artifact-test poll-reentrancy-test verdict-contract-test validate-verdicts validate-verdicts-selftest deviation-report-test poll-mode-test dns-preflight-test simcluster-accel-external-review-test simcluster-accel-external-rereview-test simcluster-accel-final-review-test remote-fs-oracle-contract-test hot-update-process-probe-test hot-update-mixed-probe-test hot-update-pty-probe-test hot-update-pty-ledger-test hot-update-broker-ledger-test hot-update-broker-probe-test hot-update-broker-drill-test hot-update-flow-probe-test hot-update-flow-drill-test hot-update-fullstack-test hot-update-fullstack-logs-test transfer-fixture-selftest drill38-contract-test drill39-contract-test upgrade-regression-test prev-supply-test upgrade-artifacts-test artifact-supply-test upgrade-fixture-test assert-shell-state-test hot-update-infra-fixture-test hot-update-infra-composer-test hot-update-infra-instruments-test hot-update-infra-supply-test hot-update-infra-drill-test hot-update-infra-ledger-test lint-drills lint-install lint-install-selftest arm-manifest-lint arm-manifest-selftest contention-registry-check contention-registry-selftest arm-aggregation-test ledger-crosscheck ledger-crosscheck-selftest r9d-nonvacuity teardown-recovery-nonvacuity-test kept-sites-selftest assert-identity-selftest timeline-test r16-g67-g69-external-review r16-g67-g69-external-rereview s7-s9-external-review; do
    printf '%-40s ' "$t"
    if out=$(run_script "$HERE/$t.sh" 2>&1); then printf 'PASS\n'
    else printf 'FAIL\n'; printf '%s\n' "$out" | tail -5 | sed 's/^/    /'; RC=1; fi
done
printf '%-40s ' "kept-sites --check"
if out=$(run_script "$HERE/kept-sites.sh" --check "$HERE/kept-sites.baseline.tsv" 2>&1); then printf 'PASS\n'
else printf 'FAIL\n'; printf '%s\n' "$out" | tail -5 | sed 's/^/    /'; RC=1; fi
# simcluster-speed M0: the identity companion of kept-sites — a count-neutral swap of one claim for
# another is invisible to the count above and red here (testing-standards G4).
printf '%-40s ' "assert-identity --check"
if out=$(run_script "$HERE/assert-identity.sh" --check "$HERE/assert-identity.baseline.tsv" 2>&1); then printf 'PASS\n'
else printf 'FAIL\n'; printf '%s\n' "$out" | tail -8 | sed 's/^/    /'; RC=1; fi
printf -- '--------------------------------------------------------------------------------\n'
[ "$RC" = 0 ] && echo "simcluster hermetic gates: ALL PASS" || echo "simcluster hermetic gates: FAILURES above" >&2
exit $RC
