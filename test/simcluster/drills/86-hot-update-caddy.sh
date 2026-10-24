#!/bin/sh
# origin: hot-update S9 plan §5 / §9.3 / P33. Canonical fresh TLS first attempts
# and logical requests are independent ledgers; a retry cannot repair a TLS gap.
# arms: R B I H C K F U
# fixture: R=N1 B=N3-live I=N1 H=N2-live C=N1 K=N1 F=N1 U=N1
# grows: R=0 B=2 I=0 H=1 C=0 K=0 F=0 U=0
# worst: R=600 B=1200 I=840 H=720 C=300 K=300 F=600 U=720
# forgoes: R=- B=- I=- H=- C=- K=- F=- U=-
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
INFRA_LIBRARY_ONLY=1
. "$HERE/drills/85-hot-update-nats.sh"
unset INFRA_LIBRARY_ONLY

infra_caddy_reload() {
    _icr_variant=${1:-valid}
    infra_drill_setup 'supply managed Caddy configuration input' infra_fixture stage-config --evidence "$INFRA_EV" --variant "$_icr_variant"
    INFRA_CONFIG_SHA=$(infra_fixture config-digest --evidence "$INFRA_EV") || setup_fail 'missing staged config digest'
    case "$INFRA_CONFIG_SHA" in ''|*[!0-9a-f]*) setup_fail 'invalid staged config digest' ;; esac
    [ "${#INFRA_CONFIG_SHA}" = 64 ] || setup_fail 'invalid staged config digest length'
    infra_drill_setup 'observe configuration generation and original sockets' infra_fixture observe \
        --evidence "$INFRA_EV" --direction reload --nodes "$INFRA_NODES"
    infra_drill_step infra_drill_command --reload-config --config-sha256 "$INFRA_CONFIG_SHA"
    INFRA_COMMAND_RC=$INFRA_STEP_RC; INFRA_COMMAND_LOG=$INFRA_STEP_LOG
    if [ "$_icr_variant" = valid ]; then
        assert_ok 'valid managed Caddy reload completes' test "$INFRA_COMMAND_RC" -eq 0
        infra_drill_check 'reload changes config generation while preserving Caddy PID and original business' reload \
            --config-sha256 "$INFRA_CONFIG_SHA" --command-rc "$INFRA_COMMAND_RC" --command-log "$INFRA_COMMAND_LOG"
    else
        assert_ok 'invalid Caddy input is explicitly refused with CLI exit 64' test "$INFRA_COMMAND_RC" -eq 64
        infra_drill_check 'bad config or certificate refuses before effect and preserves originals' invalid-config-cert \
            --variant "$_icr_variant" --config-sha256 "$INFRA_CONFIG_SHA" \
            --command-rc "$INFRA_COMMAND_RC" --command-log "$INFRA_COMMAND_LOG"
    fi
}

infra_caddy_support() {
    for _ics_action in update-support rollback-support update-support; do
        infra_drill_setup 'observe real warm and supporting runtime generations' infra_fixture observe \
            --evidence "$INFRA_EV" --direction "$_ics_action" --nodes "$INFRA_NODES"
        infra_drill_step "$SIM" exec brk1 -- /usr/local/libexec/tether-infra-helper "$_ics_action"
        INFRA_COMMAND_RC=$INFRA_STEP_RC; INFRA_COMMAND_LOG=$INFRA_STEP_LOG
        infra_drill_check 'support current/previous archive bytes execute and old workers exit' support \
            --action "$_ics_action" --command-rc "$INFRA_COMMAND_RC" --command-log "$INFRA_COMMAND_LOG" || return 1
    done
    infra_drill_cycle
}

[ "$#" = 0 ] || setup_fail '86 accepts only the runner-provided --arm; extra arguments are invalid'
infra_drill_supervise "$@"
drill_begin "86-hot-update-caddy [arm ${ARM:-?}]"
infra_drill_load
case "${ARM:?}" in
    R)
        infra_drill_prepare caddy single config-reload
        infra_caddy_reload && infra_drill_finish
        ;;
    B)
        infra_drill_prepare caddy quorum adjacent-cycle grow_to_3
        infra_drill_cycle && infra_drill_finish
        ;;
    I)
        # READY is still the stock root/on-failure invocation. Only the product
        # may bootstrap management, fence starts, move current ACME and change
        # the unit during LIVE; a pre-retrofitted fixture cannot pass baseline.
        infra_drill_prepare caddy single stock-root-retrofit
        infra_drill_fault acme-late-write-final-copy-over-2s
        infra_drill_cycle && infra_drill_finish
        ;;
    H)
        infra_drill_prepare caddy pair healthy-n2 grow_to_2
        infra_drill_cycle && infra_drill_finish
        ;;
    C)
        infra_drill_prepare caddy single invalid-config-cert
        infra_caddy_reload invalid-config && infra_caddy_reload invalid-cert && infra_drill_finish
        ;;
    K)
        infra_drill_prepare caddy single responsibility-permission-gates
        infra_drill_setup 'exercise real UID override policy lateUpgrade and unsupported live responsibilities' infra_fixture exercise \
            --evidence "$INFRA_EV" --scenario caddy-responsibility-gates
        infra_drill_check 'unknown or unsupported live responsibility defers without forced close' responsibility-gates && infra_drill_finish
        ;;
    F)
        infra_drill_prepare caddy single shadow-recovery
        # Fault observer holds beyond the component deadline, with canonical
        # traffic still scheduled. There is no shadow lifetime timer or sleep
        # that can silently turn a normal timeout into successful completion.
        infra_drill_halt main-failure-controller-death-shadow-over-120s && infra_drill_finish
        ;;
    U)
        infra_drill_prepare caddy single support-two-generation
        infra_caddy_support && infra_drill_finish
        ;;
    *) setup_fail "unknown Caddy arm '${ARM:-}'" ;;
esac
infra_drill_close
drill_end
