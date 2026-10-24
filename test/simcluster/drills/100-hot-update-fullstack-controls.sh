#!/bin/sh
# origin: hot-update S10 plan §8 / §9 / §10.2 P20.
# arms: M01A M01B M02E M02P M02F M02X M03E M03P M03F M03X M04A M04B M04P M05E M05P M05X M05R M06A M06B M06R M06F M07A M07B M07N M07C M08E M08P M08A M08B M08D M08X M08C M08O M09A M09B M10S M10C M11W M11I M11O M11P M11Q M11F M11S M11X M11D M11C M11L M11U M11K
# fixture: M01A=N1 M01B=N3-live M02E=N1 M02P=N1 M02F=N3-live M02X=N1 M03E=N1 M03P=N1 M03F=N3-live M03X=N1 M04A=N3-live M04B=N3-live M04P=N1 M05E=N1 M05P=N1 M05X=N3-live M05R=N1 M06A=N1 M06B=N3-live M06R=N3-live M06F=N3-live M07A=N1 M07B=N3-live M07N=N3-live M07C=N3-live M08E=N1 M08P=N1 M08A=N1 M08B=N3-live M08D=N3-live M08X=N3-live M08C=N3-live M08O=N3-live M09A=N1 M09B=N3-live M10S=N1 M10C=N3-live M11W=N1 M11I=N1 M11O=N1 M11P=N1 M11Q=N1 M11F=N3-live M11S=N3-live M11X=N3-live M11D=N3-live M11C=N3-live M11L=N3-live M11U=N3-live M11K=N3-live
# grows: M01A=0 M01B=2 M02E=0 M02P=0 M02F=2 M02X=0 M03E=0 M03P=0 M03F=2 M03X=0 M04A=2 M04B=2 M04P=0 M05E=0 M05P=0 M05X=2 M05R=0 M06A=0 M06B=2 M06R=2 M06F=2 M07A=0 M07B=2 M07N=2 M07C=2 M08E=0 M08P=0 M08A=0 M08B=2 M08D=2 M08X=2 M08C=2 M08O=2 M09A=0 M09B=2 M10S=0 M10C=2 M11W=0 M11I=0 M11O=0 M11P=0 M11Q=0 M11F=2 M11S=2 M11X=2 M11D=2 M11C=2 M11L=2 M11U=2 M11K=2
# worst: M01A=600 M01B=600 M02E=600 M02P=600 M02F=600 M02X=600 M03E=600 M03P=600 M03F=600 M03X=600 M04A=600 M04B=600 M04P=600 M05E=600 M05P=600 M05X=600 M05R=600 M06A=600 M06B=600 M06R=600 M06F=600 M07A=600 M07B=600 M07N=600 M07C=600 M08E=600 M08P=600 M08A=600 M08B=600 M08D=600 M08X=600 M08C=600 M08O=600 M09A=600 M09B=600 M10S=600 M10C=600 M11W=600 M11I=600 M11O=600 M11P=600 M11Q=600 M11F=600 M11S=600 M11X=600 M11D=600 M11C=600 M11L=600 M11U=600 M11K=600
# forgoes: M01A=- M01B=- M02E=- M02P=- M02F=- M02X=- M03E=- M03P=- M03F=- M03X=- M04A=- M04B=- M04P=- M05E=- M05P=- M05X=- M05R=- M06A=- M06B=- M06R=- M06F=- M07A=- M07B=- M07N=- M07C=- M08E=- M08P=- M08A=- M08B=- M08D=- M08X=- M08C=- M08O=- M09A=- M09B=- M10S=- M10C=- M11W=- M11I=- M11O=- M11P=- M11Q=- M11F=- M11S=- M11X=- M11D=- M11C=- M11L=- M11U=- M11K=-
# Delegated setup follows this independent catalog's topology cells: an arm
# with a quorum cell needs the grow lane until its two fixture grows finish.
# delegated-grows: lib/fullstack_mutations.json
set -u

# Every descendant inherits the shared subreaper/CLOEXEC lease. A native 125
# from emergency cleanup is retained as setup failure, never an expected red.
case "${SIM_UPGRADE_SUPERVISOR:-}" in
    /*) [ -x "$SIM_UPGRADE_SUPERVISOR" ] || exit 2 ;;
    *) printf '%s\n' 'fullstack controls require the shared P05 supervisor' >&2; exit 2 ;;
esac
if [ "${SIM_FULLSTACK_MUTATION_SUPERVISED:-}" != 1 ]; then
    exec "$SIM_UPGRADE_SUPERVISOR" -- /usr/bin/env SIM_FULLSTACK_MUTATION_SUPERVISED=1 /bin/sh "$0" "$@"
fi
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
drill_begin "100-hot-update-fullstack-controls [arm ${ARM:-parent}]"
[ "$#" = 0 ] || setup_fail 'controls accept only the runner-provided arm'
case "${SIM_FULLSTACK_ROOT:-}" in
    /*) ;;
    *) setup_fail 'controls require an absolute fresh evidence root' ;;
esac
if [ -z "${ARM:-}" ]; then
    [ -n "${SIM_FULLSTACK_MUTATION_CHILDREN:-}" ] || setup_fail 'controls require original native child chains'
    python3 -I -B "$HERE/drills/lib/fullstack_mutations.py" join --children "$SIM_FULLSTACK_MUTATION_CHILDREN"
    FULLSTACK_CONTROL_RC=$?
else
    # origin: S10 internal review FIX-ARM99 / arm-manifest R3-R4.
    # Validate every declared child before handing its original ID to Python.
    case "${ARM:?}" in
        M01A) ;;
        M01B) ;;
        M02E) ;;
        M02P) ;;
        M02F) ;;
        M02X) ;;
        M03E) ;;
        M03P) ;;
        M03F) ;;
        M03X) ;;
        M04A) ;;
        M04B) ;;
        M04P) ;;
        M05E) ;;
        M05P) ;;
        M05X) ;;
        M05R) ;;
        M06A) ;;
        M06B) ;;
        M06R) ;;
        M06F) ;;
        M07A) ;;
        M07B) ;;
        M07N) ;;
        M07C) ;;
        M08E) ;;
        M08P) ;;
        M08A) ;;
        M08B) ;;
        M08D) ;;
        M08X) ;;
        M08C) ;;
        M08O) ;;
        M09A) ;;
        M09B) ;;
        M10S) ;;
        M10C) ;;
        M11W) ;;
        M11I) ;;
        M11O) ;;
        M11P) ;;
        M11Q) ;;
        M11F) ;;
        M11S) ;;
        M11X) ;;
        M11D) ;;
        M11C) ;;
        M11L) ;;
        M11U) ;;
        M11K) ;;
        *) setup_fail 'unknown fullstack control arm' ;;
    esac
    [ -n "${SIM_FULLSTACK_MUTATION_PATCH:-}" ] || setup_fail 'source owner mutation patch is missing'
    [ -n "${SIM_FULLSTACK_MUTATION_EXPECTATIONS:-}" ] || setup_fail 'presealed schedule and required cells are missing'
    python3 -I -B "$HERE/drills/lib/fullstack_mutations.py" run-arm --mutation "$ARM" \
        --root "$SIM_FULLSTACK_ROOT/control" --patch "$SIM_FULLSTACK_MUTATION_PATCH" \
        --expectations "$SIM_FULLSTACK_MUTATION_EXPECTATIONS"
    FULLSTACK_CONTROL_RC=$?
fi
printf '%s\n' "$FULLSTACK_CONTROL_RC" > "$SIM_FULLSTACK_ROOT/control.native.rc" || setup_fail 'cannot retain native control status'
case "$FULLSTACK_CONTROL_RC" in
    1) assert_ok 'fullstack source mutation hits its exact window and independent assertion with native 0 to 1 to 0' false ;;
    2) setup_fail 'fullstack mutation source, overlay, custody or supervised cleanup is invalid' ;;
    4) not_covered 'all required fullstack mutation cells have native chains' \
        'NOT_RUN pending the exact source owner or P19/P25 mutation assembly adapter' runtime-guard ;;
    *) setup_fail 'fullstack controls lack the required native assertion-red chain' ;;
esac
drill_end
