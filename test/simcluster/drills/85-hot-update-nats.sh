#!/bin/sh
# origin: hot-update S9 plan §9.3 / P33. Positive children only; mutants use
# separate instances with these same arms and the unmodified P32 oracle.
# arms: A A1 B D Q L S J F
# fixture: A=N1 A1=N1 B=N3-live D=N2-live Q=N3-live L=N3-live S=N1 J=N3-live F=N3-live
# grows: A=0 A1=0 B=2 D=1 Q=2 L=2 S=0 J=2 F=2
# worst: A=720 A1=720 B=1200 D=300 Q=360 L=360 S=300 J=360 F=540
# forgoes: A=- A1=- B=- D=- Q=- L=- S=- J=- F=-

# Shared driver plumbing stays in this owned file. Library mode defines only
# functions; 86 and 39.NI/CI never dispatch a NATS arm while sourcing it.
# P32 owns infra_fixture and the independent judge. Missing operations are a
# setup failure, never a substitute observation or a successful empty cohort.
infra_drill_supervise() {
    case "${SIM_UPGRADE_SUPERVISOR:-}" in
        /*) [ -x "$SIM_UPGRADE_SUPERVISOR" ] || setup_fail 'shared process supervisor is not executable' ;;
        *) setup_fail 'shared FIX-ORPHAN supervisor is required (P27)' ;;
    esac
    if [ "${SIM_INFRA_SUPERVISED:-}" != "$0" ]; then
        # The shared subreaper owns the entire shell, including short probes,
        # transport children and fixture workers. No Pdeathsig-only fallback.
        exec "$SIM_UPGRADE_SUPERVISOR" -- env SIM_INFRA_SUPERVISED="$0" sh "$0" "$@"
    fi
}

# origin: hot-update S9 plan §9.3 / T80. This is the oracle's native status,
# not the product CLI's exit taxonomy. In particular neither CLI75 nor an
# unpinned rc3 may certify a positive arm or launder an ordinary mutant's rc1.
infra_drill_native() {
    _idn_rc=$1; _idn_label=$2
    log "infra oracle: $_idn_label (native rc=$_idn_rc)"
    case "$_idn_rc" in
        0) assert_ok 'independent infra oracle satisfies the selected frozen scope' true ;;
        1) assert_ok 'independent infra oracle satisfies the selected frozen scope' false ;;
        2) setup_fail "$_idn_label: missing or invalid observation custody" ;;
        4) not_covered 'original infra required cell remains exercisable' "$_idn_label: no replacement cohort" runtime-guard ;;
        *) setup_fail "$_idn_label: invalid native status $_idn_rc (no historical loss pin)" ;;
    esac
    [ "$_idn_rc" = 0 ]
}

infra_drill_step() {
    INFRA_STEP=$((INFRA_STEP + 1))
    INFRA_STEP_LOG="$INFRA_EV/step-$INFRA_STEP.log"
    "$@" > "$INFRA_STEP_LOG" 2>&1
    INFRA_STEP_RC=$?
    printf '%s\n' "$INFRA_STEP_RC" > "$INFRA_EV/step-$INFRA_STEP.rc" || setup_fail 'cannot retain command status'
    return "$INFRA_STEP_RC"
}

infra_drill_setup() {
    _ids_label=$1; shift
    # Run in the owning shell: assert_setup's command substitution would lose
    # the fixture's child handles and sealed-FD custody.
    infra_drill_step "$@" || setup_fail "$_ids_label; see $INFRA_STEP_LOG"
    log "infra prerequisite: $_ids_label ($INFRA_STEP_LOG)"
    assert_ok 'declared infra prerequisite completed with retained command evidence' true
}

infra_drill_check() {
    _idc_label=$1; shift
    infra_drill_step infra_fixture check "$@" --evidence "$INFRA_EV"
    infra_drill_native "$?" "$_idc_label"
}

infra_drill_cleanup() {
    [ -n "${INFRA_EV:-}" ] || return 0
    # Product retirement is judged BEFORE this cleanup. Reaping a fixture or
    # a deployment cgroup cannot be used as evidence that old product owners
    # retired. P32 joins its workers; the shared supervisor catches survivors.
    infra_fixture cleanup --evidence "$INFRA_EV" > "$INFRA_EV/cleanup.log" 2>&1
}

infra_drill_close() {
    [ -n "${INFRA_EV:-}" ] || return 0
    # The normal path joins before emitting its single verdict. EXIT retains
    # the emergency cleanup until this succeeds, so cleanup failures are never
    # printed after a GREEN receipt with their status discarded by the trap.
    infra_drill_cleanup || setup_fail 'fixture cleanup or supervised worker join failed'
    INFRA_EV=
}

infra_drill_logs() {
    _idl_phase=$1
    mkdir -m 700 "$INFRA_EV/logs-$_idl_phase" || return 2
    for _idl_node in $INFRA_LOG_NODES agt1 agt2; do
        case "$_idl_node" in agt*) _idl_streams='agent_slog agent_panic' ;; *) _idl_streams='broker_slog broker_panic' ;; esac
        for _idl_stream in $_idl_streams; do
            sim_log_snapshot "$_idl_stream" "$_idl_node" \
                "$INFRA_EV/logs-$_idl_phase/$_idl_node.$_idl_stream.raw" \
                "$INFRA_EV/logs-$_idl_phase/$_idl_node.$_idl_stream.json" || return 2
        done
    done
}

infra_drill_prepare() {
    INFRA_COMPONENT=$1; INFRA_TOPOLOGY=$2; INFRA_PROFILE=$3
    INFRA_NODES=brk1
    INFRA_INITIAL_VERSION=2.14.5
    [ "$INFRA_COMPONENT" != caddy ] || INFRA_INITIAL_VERSION=2.7.5
    case "$INFRA_TOPOLOGY" in
        single|sole-voter) ;;
        pair) INFRA_NODES='brk1 brk2' ;;
        quorum) INFRA_NODES='brk1 brk2 brk3' ;;
        *) setup_fail 'unknown infra topology' ;;
    esac
    INFRA_LOG_NODES=$INFRA_NODES
    [ -r "$HERE/drills/lib/hot_update_infra.sh" ] || setup_fail 'NOT_RUN pending P32 live infra fixture'
    . "$HERE/drills/lib/hot_update_infra.sh"
    command -v infra_fixture >/dev/null 2>&1 || setup_fail 'P32 infra_fixture entry point is unavailable'
    INFRA_EV=$(mktemp -d "${SIM_EVIDENCE_DIR:-/tmp}/hot-update-infra.${INFRA_COMPONENT}.${ARM}.XXXXXX") || setup_fail 'create private infra evidence'
    chmod 0700 "$INFRA_EV" || setup_fail 'protect infra evidence'
    INFRA_STEP=0
    drill_install_traps infra_drill_cleanup
    infra_drill_setup 'verify real adjacent artifacts and support archives' \
        python3 "$HERE/lib/infra-artifact.py" verify "$HERE/vendor" "$HERE/vendor/install.sh"
    # This is repeated per child, including independently restaged mutations.
    # The outer simcluster image guard independently checks final image files.
    infra_drill_setup 'verify every current Docker COPY and source input' \
        python3 "$HERE/lib/infra-image-manifest.py" check "$HERE" "$HERE/vendor/infra-image-inputs.json"
    infra_drill_setup 'fixture executable schema source and positive/negative receipts' \
        infra_fixture preflight --evidence "$INFRA_EV" --component "$INFRA_COMPONENT" --profile "$INFRA_PROFILE"
    infra_drill_setup 'discard only this isolated drill instance' "$SIM" nuke
    if [ "$INFRA_TOPOLOGY" = single ] || [ "$INFRA_TOPOLOGY" = sole-voter ]; then
        infra_drill_setup 'one broker two agents and one ctl' "$SIM" up --brokers 1 --agents 2 --ctl 1
        if [ "$INFRA_TOPOLOGY" = sole-voter ]; then
            infra_drill_setup 'initialize tether Raft without inventing JetStream peers' "$SIM" init brk1
        fi
    else
        [ "$#" = 4 ] || setup_fail 'multi-broker arm requires its declared grow supplier'
        infra_drill_setup 'strict declared grow with two agents and ctl' "$4" 2 1 0
    fi
    infra_drill_setup 'prepare stock runtime and independent observation channels' \
        infra_fixture prepare --evidence "$INFRA_EV" --component "$INFRA_COMPONENT" \
        --topology "$INFRA_TOPOLOGY" --profile "$INFRA_PROFILE" --nodes "$INFRA_NODES" \
        --agents agt1,agt2 --ctl ctl1 --carrier-transition unchanged --initial-version "$INFRA_INITIAL_VERSION"
    infra_drill_setup 'capture fresh cursors for all four mapped log streams' infra_drill_logs baseline
    # start seals the original requiredset and startup --rate-limit for 2 push
    # and 2 pull transfers (66,549,440 B and 1 GiB each direction). It is called
    # exactly once across upgrade -> rollback -> upgrade, including all 9 hops.
    infra_drill_setup 'READY: original SSH TCP SS PTY exec and four paced transfers' \
        infra_fixture start --evidence "$INFRA_EV" --carrier-transition unchanged \
        --probe-rate "$HOT_UPDATE_PROBE_RATE" --request-budget "$HOT_UPDATE_REQUEST_S" \
        --flow-budget "$HOT_UPDATE_LIVE_FLOW_STALL_S" --transfer-budget "$HOT_UPDATE_TRANSFER_PROGRESS_S"
    infra_drill_check 'nonempty original business and stock topology before first effect' baseline || { drill_end; exit "$?"; }
    if [ "$INFRA_PROFILE" != legacy-capability ]; then
        # Explicit product management activation occurs in LIVE. In particular
        # I must still be root/on-failure at READY; the fixture cannot pre-enable
        # the socket, retrofit the unit or move ACME storage to make it pass.
        for _ida_node in $INFRA_NODES; do
            infra_drill_step "$SIM" exec "$_ida_node" -- /usr/local/libexec/tether-infra-helper bootstrap
            INFRA_COMMAND_RC=$INFRA_STEP_RC; INFRA_COMMAND_LOG=$INFRA_STEP_LOG
            assert_ok 'product bootstrap activates management successfully during original business' test "$INFRA_COMMAND_RC" -eq 0
            infra_drill_check 'management bootstrap preserves the stock invocation and all original business' bootstrap \
                --node "$_ida_node" --command-rc "$INFRA_COMMAND_RC" --command-log "$INFRA_COMMAND_LOG" || { drill_end; exit "$?"; }
        done
    fi
}

infra_drill_command() {
    if [ "$INFRA_TOPOLOGY" = single ]; then
        "$SIM" exec brk1 -- runuser -u tether -- env HOME=/var/lib/tether \
            "${INFRA_CTL_BINARY:-/usr/local/bin/tether}" admin infra upgrade --component "$INFRA_COMPONENT" "$@"
    else
        "$SIM" exec brk1 -- runuser -u tether -- env HOME=/var/lib/tether \
            "${INFRA_CTL_BINARY:-/usr/local/bin/tether}" cluster upgrade --component "$INFRA_COMPONENT" \
            --account-seed /etc/tether/secrets/account.nk "$@"
    fi
}

infra_drill_target() {
    case "$INFRA_COMPONENT" in
        nats) INFRA_VERSION=2.14.6; INFRA_BINARY="$HERE/vendor/nats-server-next" ;;
        caddy) INFRA_VERSION=2.7.6; INFRA_BINARY="$HERE/vendor/caddy-next" ;;
        *) return 2 ;;
    esac
    INFRA_SHA=$(sha256sum "$INFRA_BINARY") || return 2
    INFRA_SHA=${INFRA_SHA%% *}
    [ "${#INFRA_SHA}" = 64 ]
}

# A round is one product roll. The independent fixture records EVERY host hop
# (including before/moving/verifying/returning) and refuses missing physical
# evidence. Product status and final file SHA alone cannot sign completion.
infra_drill_round() {
    INFRA_DIRECTION=$1
    infra_drill_setup 'arm immutable per-host cut observations' infra_fixture observe \
        --evidence "$INFRA_EV" --direction "$INFRA_DIRECTION" --nodes "$INFRA_NODES"
    if [ "$INFRA_DIRECTION" = rollback ]; then
        infra_drill_step infra_drill_command --rollback
    else
        infra_drill_target || setup_fail 'adjacent target identity unavailable'
        infra_drill_step infra_drill_command --to-version "$INFRA_VERSION" --expect-sha256 "$INFRA_SHA"
    fi
    INFRA_COMMAND_RC=$INFRA_STEP_RC
    INFRA_COMMAND_LOG=$INFRA_STEP_LOG
    assert_ok 'normal component roll returns completed CLI status' test "$INFRA_COMMAND_RC" -eq 0
    # Preserve CLI rc in its own file and pass it to the judge. The judge checks
    # it together with actual effect/physical/retirement receipts, not vice versa.
    infra_drill_check 'every hop keeps original business and retires its old runtime' round \
        --direction "$INFRA_DIRECTION" --nodes "$INFRA_NODES" \
        --command-rc "$INFRA_COMMAND_RC" --command-log "$INFRA_COMMAND_LOG"
}

infra_drill_cycle() {
    infra_drill_round upgrade || return 1
    infra_drill_round rollback || return 1
    infra_drill_round reupgrade || return 1
    _idcy_hops=3
    case "$INFRA_TOPOLOGY" in quorum) _idcy_hops=9 ;; pair) _idcy_hops=6 ;; esac
    infra_drill_check 'same original cohort spans every adjacent cycle hop and returns to the pin' cycle \
        --expected-hops "$_idcy_hops" --final-version "$INFRA_VERSION"
}

infra_drill_finish() {
    # This census must occur while the original infinite tasks/transfers still
    # live. FINAL then settles those originals and records their native status.
    infra_drill_check 'product old/temp owners exited before fixture cleanup' retirement || return 1
    infra_drill_step infra_fixture finish --evidence "$INFRA_EV"
    infra_drill_native "$?" 'FINAL: original bytes FIN native exits and complete trace' || return 1
    infra_drill_setup 'capture final logs before fixture cleanup' infra_drill_logs final
    infra_drill_check 'fresh log windows contain no hidden panic or missing stream' logs || return 1
    log "infra receipts retained: $INFRA_EV"
}

infra_drill_fault() {
    infra_drill_setup "install observation-bound fault $1" infra_fixture fault \
        --evidence "$INFRA_EV" --point "$1"
}

infra_drill_refusal() {
    _idr_point=$1
    infra_drill_fault "$_idr_point"
    infra_drill_target || setup_fail 'adjacent target identity unavailable'
    infra_drill_step infra_drill_command --to-version "$INFRA_VERSION" --expect-sha256 "$INFRA_SHA"
    INFRA_COMMAND_RC=$INFRA_STEP_RC; INFRA_COMMAND_LOG=$INFRA_STEP_LOG
    infra_drill_check 'unsafe NATS cut refused before any effect with original window retained' refusal \
        --point "$_idr_point" --command-rc "$INFRA_COMMAND_RC" --command-log "$INFRA_COMMAND_LOG" || return 1
    # Restore only the injected fault. No producer pause, observer reset or new
    # original workload is allowed, and the refusal trace remains in the ledger.
    if [ "$_idr_point" = nats-n2 ]; then
        # The frozen D manifest permits only one grow. Recovery must use the
        # real product shrink to standalone JS, never an undeclared third peer.
        # Its N2 refusal remains a separate window in the original trace.
        infra_drill_setup 'restore a supported topology through product shrink without another grow' infra_fixture heal \
            --evidence "$INFRA_EV" --point "$_idr_point" --recovery-topology sole-voter
        INFRA_TOPOLOGY=sole-voter; INFRA_NODES=brk1
    else
        infra_drill_setup 'restore faulted topology without resetting evidence' infra_fixture heal \
            --evidence "$INFRA_EV" --point "$_idr_point"
    fi
    [ "${2:-complete}" = check-only ] || infra_drill_round upgrade
}

infra_drill_recover() {
    # P32 obtains the Tx from a strict product status receipt; no caller-supplied
    # journal edit, manual restart or fabricated terminal status is permitted.
    INFRA_TX=$(infra_fixture transaction --evidence "$INFRA_EV") || setup_fail 'missing transaction receipt for recovery'
    case "$INFRA_TX" in ''|*[!0-9a-f]*) setup_fail 'invalid recovery transaction ID' ;; esac
    [ "${#INFRA_TX}" = 32 ] || setup_fail 'invalid recovery transaction ID length'
    if [ "$INFRA_TOPOLOGY" = single ]; then
        infra_drill_step "$SIM" exec brk1 -- runuser -u tether -- env HOME=/var/lib/tether \
            /usr/local/bin/tether admin infra recover --tx "$INFRA_TX"
    else
        infra_drill_step "$SIM" exec brk1 -- runuser -u tether -- env HOME=/var/lib/tether \
            "${INFRA_CTL_BINARY:-/usr/local/bin/tether}" cluster upgrade --component "$INFRA_COMPONENT" --recover "$INFRA_TX" \
            --account-seed /etc/tether/secrets/account.nk
    fi
    INFRA_COMMAND_RC=$INFRA_STEP_RC; INFRA_COMMAND_LOG=$INFRA_STEP_LOG
    infra_drill_check 'product recover completes the same transaction and retires remaining owners' recovered \
        --tx "$INFRA_TX" --command-rc "$INFRA_COMMAND_RC" --command-log "$INFRA_COMMAND_LOG"
}

infra_drill_halt() {
    infra_drill_fault "$1"
    infra_drill_target || setup_fail 'adjacent target identity unavailable'
    infra_drill_setup 'observe real sent/effect/reply-cut fault window' infra_fixture observe \
        --evidence "$INFRA_EV" --direction upgrade --nodes "$INFRA_NODES"
    infra_drill_step infra_drill_command --to-version "$INFRA_VERSION" --expect-sha256 "$INFRA_SHA"
    INFRA_COMMAND_RC=$INFRA_STEP_RC; INFRA_COMMAND_LOG=$INFRA_STEP_LOG
    infra_drill_check 'fault hit halts progression and leaves the healthy serving path live' halted \
        --point "$1" --command-rc "$INFRA_COMMAND_RC" --command-log "$INFRA_COMMAND_LOG" || return 1
    infra_drill_recover
}

infra_drill_load() {
    . "$HERE/lib/docker.sh"
    . "$HERE/lib/tether.sh"
    . "$HERE/lib/secrets.sh"
    . "$HERE/drills/lib/cluster.sh"
    . "$HERE/drills/lib/logs.sh"
    . "$HERE/drills/lib/hot_update_budget.sh"
    SIM=${SIM:-$HERE/simcluster}
}

if [ "${INFRA_LIBRARY_ONLY:-0}" = 1 ]; then return 0; fi
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
[ "$#" = 0 ] || setup_fail '85 accepts only the runner-provided --arm; extra arguments are invalid'
infra_drill_supervise "$@"
drill_begin "85-hot-update-nats [arm ${ARM:-?}]"
infra_drill_load
case "${ARM:?}" in
    A)
        infra_drill_prepare nats single default-single
        infra_drill_cycle && infra_drill_finish
        ;;
    A1)
        infra_drill_prepare nats sole-voter standalone-js
        infra_drill_cycle && infra_drill_finish
        ;;
    B)
        infra_drill_prepare nats quorum adjacent-cycle grow_to_3
        infra_drill_cycle && infra_drill_finish
        ;;
    D)
        infra_drill_prepare nats pair readiness-refusal grow_to_2
        infra_drill_refusal nats-n2 && infra_drill_finish
        ;;
    Q)
        infra_drill_prepare nats quorum readiness-refusal grow_to_3
        infra_drill_refusal nats-lost-quorum && infra_drill_finish
        ;;
    L)
        infra_drill_prepare nats quorum readiness-refusal grow_to_3
        infra_drill_refusal nats-stream-lag check-only && \
            infra_drill_refusal nats-consumer-lag check-only && infra_drill_round upgrade && infra_drill_finish
        ;;
    S)
        infra_drill_prepare nats single lifetime-store-lock
        infra_drill_setup 'exercise actual second launch and legacy store owner guards' infra_fixture exercise \
            --evidence "$INFRA_EV" --scenario store-double-open
        infra_drill_check 'same store cannot be opened twice while any stock owner lives' store-lock && infra_drill_finish
        ;;
    J)
        infra_drill_prepare nats quorum exact-release-join grow_to_3
        infra_drill_setup 'attempt genuine mismatched and exact-release temporary broker joins' infra_fixture exercise \
            --evidence "$INFRA_EV" --scenario exact-release-join
        infra_drill_check 'mismatched release refuses and healthy exact release joins' join && infra_drill_finish
        ;;
    F)
        infra_drill_prepare nats quorum post-ldm-recovery grow_to_3
        infra_drill_halt after-ldm-reply-cut && infra_drill_finish
        ;;
    *) setup_fail "unknown NATS arm '${ARM:-}'" ;;
esac
infra_drill_close
drill_end
