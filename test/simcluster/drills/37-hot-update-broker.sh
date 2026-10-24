#!/bin/sh
# origin: hot-update S6 plan §10.2 / T58. Each child keeps its native verdict;
# run-drills' existing manifest join owns the parent and must join all five.
# arms: A A1 B T R
# fixture: A=install A1=N1 B=N3-live T=N3-live R=N1
# grows: A=0 A1=0 B=2 T=2 R=0
# worst: A=420 A1=420 B=720 T=1200 R=360
# forgoes: A=- A1=- B=- T=- R=-
set -u
# origin: hot-update S8 plan §9.1 / L21. The external shared owner survives
# shell death and reaps detached descendants; shell EXIT traps are secondary.
if [ "${SIM_HOT_UPDATE_OWNER:-}" != "$$" ]; then
    case "${SIM_UPGRADE_SUPERVISOR:-}" in /*) ;; *) printf '%s\n' 'shared process supervisor is required' >&2; exit 2 ;; esac
    [ -x "$SIM_UPGRADE_SUPERVISOR" ] || exit 2
    exec "$SIM_UPGRADE_SUPERVISOR" -- /bin/sh -c \
        'SIM_HOT_UPDATE_OWNER=$$; export SIM_HOT_UPDATE_OWNER; exec /bin/sh "$@"' supervised "$0" "$@"
fi
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/assert.sh"
. "$HERE/drills/lib/logs.sh"
. "$HERE/drills/lib/cluster.sh"
. "$HERE/drills/lib/colocated.sh"
. "$HERE/drills/lib/hot_update_broker.sh"
SIM=${SIM:-$HERE/simcluster}
SIM_VENDOR=${SIM_VENDOR:-$HERE/vendor}
SID=lab
PIN=135790

# 39.BR delegates the independent signed path without entering a 37 positive
# arm or its N1 provisioning. Its native verdict stays in the caller's harness.
# origin: hot-update S8 plan §9.3 / P28.
if [ "${BROKER_LIBRARY_ONLY:-0}" = 1 ]; then return 0; fi

drill_begin "37-hot-update-broker [arm ${ARM:-?}]"
case "${ARM:?}" in
    A)
        broker_prepare single
        broker_reload forward "$SIM_VENDOR/tether-next"
        broker_finish
        ;;
    A1)
        broker_prepare sole-voter
        broker_supply sole-voter-plan
        broker_reload forward "$SIM_VENDOR/tether-next"
        broker_supply sole-voter-write-stall
        broker_finish
        ;;
    B)
        broker_prepare quorum grow_to_3
        broker_supply auto-rebalance-prior
        broker_roll "$SIM_VENDOR/tether-next"
        broker_finish
        ;;
    T)
        # S7 supplies a SECOND named single fixture inside this same arm.
        # It must never borrow A/A1 state or stop the transfer to make census 0.
        broker_prepare quorum grow_to_3
        broker_transfer_lane quorum
        broker_finish
        broker_cleanup
        broker_prepare single
        broker_transfer_lane single
        broker_finish
        ;;
    R)
        broker_prepare single
        broker_fault held
        broker_fault active
        broker_finish
        ;;
    *) setup_fail "unknown broker arm '$ARM'" ;;
esac
log "37 broker receipts retained: $BROKER_EV"
drill_end
