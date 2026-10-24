#!/bin/sh
# origin: hot-update S7 plan §10.3 / §10.6 / T43.
# Each launch owns one arm; run-drills' ARMS join owns the positive parent.
# arms: A A1 B C1 C2 C3 R1 R2 X W1 W2
# fixture: A=install A1=N1 B=N3-live C1=N1 C2=N1 C3=N1 R1=N1 R2=N1 X=N1 W1=N1 W2=N1
# grows: A=0 A1=0 B=2 C1=0 C2=0 C3=0 R1=0 R2=0 X=0 W1=0 W2=0
# worst: A=1200 A1=1200 B=1200 C1=1000 C2=1000 C3=1000 R1=1200 R2=1200 X=1100 W1=1100 W2=1100
# forgoes: A=- A1=- B=- C1=- C2=- C3=- R1=- R2=- X=- W1=- W2=-
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/logs.sh"
. "$HERE/drills/lib/hot_update_transfer.sh"
SIM=${SIM:-$HERE/simcluster}
SIM_VENDOR=${SIM_VENDOR:-$HERE/vendor}

drill_begin "63-hot-update-transfer [arm ${ARM:-?}]"
case "${ARM:?}" in
    A) transfer_run A ;;
    A1) transfer_run A1 ;;
    B) transfer_run B grow_to_3 ;;
    C1) transfer_run C1 ;;
    C2) transfer_run C2 ;;
    C3) transfer_run C3 ;;
    R1) transfer_run R1 ;;
    R2) transfer_run R2 ;;
    X) transfer_run X ;;
    W1) transfer_run W1 ;;
    W2) transfer_run W2 ;;
    *) setup_fail "unknown transfer arm '$ARM'" ;;
esac
# Identity scanner sees this literal claim; the predicate is read-only. It
# never converts the original native judge's INCOMPLETE or failure into GREEN.
assert_setup 'positive manifest requires every leg and child' \
    python3 -I -B "$HERE/drills/lib/transfer_mutations.py" validate --manifest "$TRANSFER_MANIFEST"
# Seal and join custody before emitting the single native parent verdict.
transfer_cleanup
transfer_native "$?"
drill_end
TRANSFER_DRILL_RC=$?
exit "$TRANSFER_DRILL_RC"
