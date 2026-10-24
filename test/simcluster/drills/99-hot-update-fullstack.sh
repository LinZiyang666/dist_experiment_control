#!/bin/sh
# origin: hot-update S10 plan §9 / §10.2 P19.
# The runner gives every arm a fresh instance, volume and port allocation.
# Parent aggregation consumes receipts; it never relaunches these children.
# arms: C1 C3 A B S1 S3 F1 F3 K
# fixture: C1=N1 C3=N3-live A=N1 B=N3-live S1=N1 S3=N3-live F1=N1 F3=N3-live K=N3-live
# grows: C1=0 C3=2 A=0 B=2 S1=0 S3=2 F1=0 F3=2 K=2
# worst: C1=2400 C3=2400 A=6000 B=6000 S1=2700 S3=2700 F1=2700 F3=2700 K=1800
# forgoes: C1=- C3=- A=- B=- S1=- S3=- F1=- F3=- K=-
# Delegated setup reads this independent catalog: single grows zero nodes;
# quorum grows two. The shell must not repeat the assembly host's setup.
# delegated-grows: lib/fullstack_manifest.json
set -u

# Supervise the shell too, so even its short status/receipt commands inherit
# the shared orphan lease. No process-group-only or Pdeathsig fallback exists.
case "${SIM_UPGRADE_SUPERVISOR:-}" in
    /*) [ -x "$SIM_UPGRADE_SUPERVISOR" ] || exit 2 ;;
    *) printf '%s\n' 'fullstack requires the shared P05 supervisor' >&2; exit 2 ;;
esac
if [ "${SIM_FULLSTACK_SUPERVISED:-}" != "$0" ]; then
    exec "$SIM_UPGRADE_SUPERVISOR" -- /usr/bin/env SIM_FULLSTACK_SUPERVISED="$0" /bin/sh "$0" "$@"
fi
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
drill_begin "99-hot-update-fullstack [arm ${ARM:-parent}]"
[ "$#" = 0 ] || setup_fail 'fullstack accepts only the runner-provided arm'
case "${SIM_FULLSTACK_ROOT:-}" in
    /*) ;;
    *) setup_fail 'fullstack requires an absolute fresh evidence root' ;;
esac

if [ -z "${ARM:-}" ]; then
    # origin: S10 internal review rv3-assembly F1. Standalone archived rows
    # cannot prove native Join; P32 must carry the original runner authority.
    [ -n "${SIM_FULLSTACK_CHILDREN:-}" ] || setup_fail 'parent requires original child receipts'
    python3 -I -B "$HERE/drills/lib/fullstack_fixture.py" join --receipts "$SIM_FULLSTACK_CHILDREN"
    FULLSTACK_NATIVE_RC=$?
else
    # origin: S10 internal review FIX-ARM99 / arm-manifest R3-R4.
    # Keep each launch identity visible to the manifest's branch owner scan.
    case "${ARM:?}" in
        C1) ;;
        C3) ;;
        A) ;;
        B) ;;
        S1) ;;
        S3) ;;
        F1) ;;
        F3) ;;
        K) ;;
        *) setup_fail 'unknown fullstack arm' ;;
    esac
    [ -n "${SIM_FULLSTACK_PROFILE:-}" ] || setup_fail 'sealed fullstack profile is missing'
    python3 -I -B "$HERE/drills/lib/fullstack_fixture.py" run --arm "$ARM" \
        --root "$SIM_FULLSTACK_ROOT" --profile "$SIM_FULLSTACK_PROFILE"
    FULLSTACK_NATIVE_RC=$?
fi
# Capture before any diagnostic/cleanup. The assembly host has already joined
# every owner; timeout without this point remains the runner's INFRA-ABORT.
printf '%s\n' "$FULLSTACK_NATIVE_RC" > "$SIM_FULLSTACK_ROOT/assembly.native.rc" || setup_fail 'cannot preserve native status'
case "$FULLSTACK_NATIVE_RC" in
    0) assert_ok 'fullstack required children and original cohorts satisfy the independent judges' true ;;
    1) assert_ok 'fullstack required children and original cohorts satisfy the independent judges' false ;;
    2) setup_fail 'fullstack custody, supply, logs or supervised cleanup failed' ;;
    3) product_red 'original fullstack judge receipt: verdict=PRODUCT-RED rc=3' ;;
    4) not_covered 'fullstack required live windows and upstream receipts are complete' \
        'missing original child, leg or upstream delivery; no replacement permitted' runtime-guard ;;
    *) setup_fail 'fullstack host or supervisor returned a non-native status' ;;
esac
drill_end
