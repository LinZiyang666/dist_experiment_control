#!/bin/sh
# origin: hot-update S7 plan §10.5 / §10.6 / T44.
# Expected ASSERT-FAIL is evidence of a killed mutant, never positive GREEN.
# arms: M01 M02 M03 M04 M05 M06 M07 M08A M08B M08C M09 M10A M10B M11A M11B M11C M12A M12B M12C M13A M13B M13C M14
# fixture: M01=N1 M02=N1 M03=N1 M04=N1 M05=N1 M06=N1 M07=N1 M08A=N1 M08B=N1 M08C=N1 M09=N1 M10A=N1 M10B=N1 M11A=N1 M11B=N1 M11C=N1 M12A=N1 M12B=N1 M12C=N1 M13A=N1 M13B=N1 M13C=N1 M14=N1
# grows: M01=0 M02=0 M03=0 M04=0 M05=0 M06=0 M07=0 M08A=0 M08B=0 M08C=0 M09=0 M10A=0 M10B=0 M11A=0 M11B=0 M11C=0 M12A=0 M12B=0 M12C=0 M13A=0 M13B=0 M13C=0 M14=0
# worst: M01=900 M02=900 M03=900 M04=900 M05=900 M06=900 M07=900 M08A=900 M08B=900 M08C=900 M09=900 M10A=900 M10B=900 M11A=900 M11B=900 M11C=900 M12A=900 M12B=900 M12C=900 M13A=900 M13B=900 M13C=900 M14=900
# forgoes: M01=- M02=- M03=- M04=- M05=- M06=- M07=- M08A=- M08B=- M08C=- M09=- M10A=- M10B=- M11A=- M11B=- M11C=- M12A=- M12B=- M12C=- M13A=- M13B=- M13C=- M14=-
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/logs.sh"
. "$HERE/drills/lib/hot_update_transfer.sh"
SIM=${SIM:-$HERE/simcluster}
SIM_VENDOR=${SIM_VENDOR:-$HERE/vendor}

drill_begin "64-hot-update-transfer-controls [arm ${ARM:-?}]"
case "${ARM:?}" in
    M01) transfer_run M01 ;;
    M02) transfer_run M02 ;;
    M03) transfer_run M03 ;;
    M04) transfer_run M04 ;;
    M05) transfer_run M05 ;;
    M06) transfer_run M06 ;;
    M07) transfer_run M07 ;;
    M08A) transfer_run M08A ;;
    M08B) transfer_run M08B ;;
    M08C) transfer_run M08C ;;
    M09) transfer_run M09 ;;
    M10A) transfer_run M10A ;;
    M10B) transfer_run M10B ;;
    M11A) transfer_run M11A ;;
    M11B) transfer_run M11B ;;
    M11C) transfer_run M11C ;;
    M12A) transfer_run M12A ;;
    M12B) transfer_run M12B ;;
    M12C) transfer_run M12C ;;
    M13A) transfer_run M13A ;;
    M13B) transfer_run M13B ;;
    M13C) transfer_run M13C ;;
    M14) transfer_run M14 ;;
    *) setup_fail "unknown transfer control '$ARM'" ;;
esac
# Keep the original ASSERT-FAIL counter. This setup predicate validates its
# expected/actual receipt relation and never rewrites native1 to GREEN0.
assert_setup 'production control source build and runtime receipt are bound' \
    python3 -I -B "$HERE/drills/lib/transfer_mutations.py" kill --evidence "$TRANSFER_EV" \
    --mutation "$ARM" --native-rc "$(cat "$TRANSFER_EV/judge.rc")"
transfer_cleanup
transfer_native "$?"
drill_end
TRANSFER_DRILL_RC=$?
exit "$TRANSFER_DRILL_RC"
