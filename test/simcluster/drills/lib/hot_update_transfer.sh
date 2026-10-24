#!/bin/sh
# origin: hot-update S7 plan §10 / §12.2 P30.
# P29 owns processes/custody/continuous oracles. This driver owns every product
# upgrade/fault command; the fixture's five verbs cannot perform a cut for us.
. "$HERE/drills/lib/hot_update_budget.sh"
TRANSFER_EV=
TRANSFER_STARTED=0
TRANSFER_JUDGED=0
TRANSFER_FIXTURE_JOB=
TRANSFER_STEP=0
TRANSFER_CELL=
TRANSFER_FAULT_NODES=
TRANSFER_PRIOR_GOFLAGS=${GOFLAGS:-}

transfer_native() {
    # Business75 is never a drill4. Only the fixture's original native judge
    # produces a five-state status; a command75 remains in its separate receipt.
    case "$1" in
        0) _AS_PASS=$((_AS_PASS + 1)) ;;
        1) _AS_FAIL=$((_AS_FAIL + 1)) ;;
        2) _AS_SETUP=$((_AS_SETUP + 1)) ;;
        3) _AS_PRODUCT_RED=$((_AS_PRODUCT_RED + 1)) ;;
        4) _AS_NC=$((_AS_NC + 1)); _AS_NC_GUARD=$((_AS_NC_GUARD + 1)) ;;
        *) _AS_SETUP=$((_AS_SETUP + 1)) ;;
    esac
}

transfer_require() {
    # origin: hot-update S7 plan L16 / T43. No custom Pdeathsig supervisor.
    # FIX-ORPHAN supplies this shared helper; missing helper is a prerequisite.
    [ -r "$HERE/drills/lib/transfer_fixture.py" ] || return 2
    [ -r "$HERE/drills/lib/transfer_fixture_manifest.py" ] || return 2
    # The shared launcher implements both legacy -- argv and evidence flags.
    TRANSFER_PROCESS_SUPERVISOR=${TRANSFER_PROCESS_SUPERVISOR:-${SIM_UPGRADE_SUPERVISOR:-}}
    case "${TRANSFER_PROCESS_SUPERVISOR:-}" in /*) ;; *) return 2 ;; esac
    [ -x "$TRANSFER_PROCESS_SUPERVISOR" ] || return 2
    export TRANSFER_PROCESS_SUPERVISOR
    [ -r "${TRANSFER_DEPLOYMENT_MANIFEST:-}" ] || return 2
    case "$SIM" in "$HERE"/*) ;; *) return 2 ;; esac
    [ -x "$SIM" ] || return 2
}

transfer_step() {
    TRANSFER_STEP=$((TRANSFER_STEP + 1))
    TRANSFER_STEP_LOG="$TRANSFER_EV/step-$TRANSFER_STEP.out"
    TRANSFER_STEP_STATUS="$TRANSFER_EV/step-$TRANSFER_STEP.rc"
    "$TRANSFER_PROCESS_SUPERVISOR" --log "$TRANSFER_STEP_LOG" --receipt "$TRANSFER_STEP_STATUS" -- "$@"
    TRANSFER_STEP_RC=$?
    # Save BEFORE any pipe, readback or teardown. The shared supervisor must
    # preserve the child's native code, not convert a failed child to success.
    printf '%s\n' "$TRANSFER_STEP_RC" > "$TRANSFER_STEP_STATUS"
    return "$TRANSFER_STEP_RC"
}

transfer_fixture() {
    if [ "$1" = start ] && [ -z "$TRANSFER_FIXTURE_JOB" ]; then
        # origin: hot-update S6 plan §10.1 / T58; DRILL-ASSETS / foreground keeper startup.
        # start serves until stop, so retain its supervisor and await the real
        # flushed receipt. Ordinary transfer_step would block before baseline.
        TRANSFER_STEP_LOG="$TRANSFER_EV/fixture-start.out"
        "$TRANSFER_PROCESS_SUPERVISOR" --log "$TRANSFER_STEP_LOG" --receipt "$TRANSFER_EV/fixture-start.rc" -- \
            python3 -I -B "$HERE/drills/lib/transfer_fixture.py" start \
            --manifest "$TRANSFER_FIXTURE_MANIFEST" --evidence "$TRANSFER_EV" &
        TRANSFER_FIXTURE_JOB=$!
        python3 -I -B "$HERE/drills/lib/transfer_fixture_manifest.py" wait-start \
            --manifest "$TRANSFER_FIXTURE_MANIFEST" --log "$TRANSFER_STEP_LOG" \
            --receipt "$TRANSFER_EV/fixture-start.rc" --pid "$TRANSFER_FIXTURE_JOB"
        _tfx_start=$?
        if [ "$_tfx_start" != 0 ]; then
            kill -TERM "$TRANSFER_FIXTURE_JOB" 2>/dev/null || true
            wait "$TRANSFER_FIXTURE_JOB"; _tfx_join=$?
            printf '%s\n' "$_tfx_join" > "$TRANSFER_EV/fixture-start.rc"
            TRANSFER_FIXTURE_JOB=
        fi
        return "$_tfx_start"
    fi
    transfer_step python3 -I -B "$HERE/drills/lib/transfer_fixture.py" "$1" \
        --manifest "$TRANSFER_FIXTURE_MANIFEST" --evidence "$TRANSFER_EV" >/dev/null
    _tfx_rc=$?
    if [ "$1" = stop ] && [ -n "$TRANSFER_FIXTURE_JOB" ]; then
        # A failed stop still retires the supervised tree. Join on every path;
        # merely dropping the PID would orphan a keeper after an aborted drill.
        python3 -I -B "$HERE/drills/lib/transfer_fixture_manifest.py" wait-stop \
            --receipt "$TRANSFER_EV/fixture-start.rc" --pid "$TRANSFER_FIXTURE_JOB" || \
            kill -TERM "$TRANSFER_FIXTURE_JOB" 2>/dev/null || true
        wait "$TRANSFER_FIXTURE_JOB"; _tfx_join=$?
        TRANSFER_FIXTURE_JOB=
        # A matching assertion exit belongs to the keeper. Any unexplained
        # supervisor or join failure is SETUP2, never a successful stop.
        [ "$_tfx_join" = "$_tfx_rc" ] || _tfx_rc=2
    fi
    return "$_tfx_rc"
}

transfer_setup() {
    _ts_description=$1; shift
    "$@"; _ts_rc=$?
    [ "$_ts_rc" = 0 ] || setup_fail "$_ts_description (native rc=$_ts_rc)"
    _AS_PASS=$((_AS_PASS + 1))
}

transfer_cleanup() {
    [ "$TRANSFER_STARTED" = 1 ] || return 0
    # On abort, collect the failure BEFORE releasing custody. A stop can never
    # prevent a late failed terminal from reaching the original judge.
    if [ "$TRANSFER_JUDGED" != 1 ]; then
        transfer_fixture sample || true
        transfer_fixture drain || true
        transfer_fixture judge
        _tc_rc=$?
        cp "$TRANSFER_STEP_LOG" "$TRANSFER_EV/abort-judge.out" 2>/dev/null || true
        printf '%s\n' "$_tc_rc" > "$TRANSFER_EV/abort-judge.rc"
    fi
    transfer_fixture stop
    _tc_stop_rc=$?
    cp "$TRANSFER_STEP_LOG" "$TRANSFER_EV/stop.out" || _tc_stop_rc=2
    printf '%s\n' "$_tc_stop_rc" > "$TRANSFER_EV/stop.rc"
    # Fault cleanup follows failed-run sampling/judge, so healing cannot hide
    # missed rollback or a late contradictory terminal from the oracle.
    for _tc_node in $TRANSFER_FAULT_NODES; do
        transfer_deploy dexec "$_tc_node" -- iptables -D OUTPUT -p tcp --syn --dport 4222 -j DROP || _tc_stop_rc=2
    done
    TRANSFER_FAULT_NODES=
    GOFLAGS=$TRANSFER_PRIOR_GOFLAGS; export GOFLAGS
    TRANSFER_STARTED=0
    return "$_tc_stop_rc"
}

# A nested37.T fixture cannot replace the caller's original flow-custody EXIT
# owner. On failure both owners must release, including a start/setup failure.
# origin: hot-update S7 plan L16 / §10.4 / T43.
transfer_cleanup_all() {
    transfer_cleanup; _tca_rc=$?
    if [ "${TRANSFER_PARENT_CLEANUP:-}" = broker ]; then
        broker_cleanup || _tca_rc=2
    fi
    return "$_tca_rc"
}

transfer_prepare() {
    transfer_require || setup_fail 'transfer fixture shared supervisor or deployment manifest is unavailable'
    mkdir -p "${SIM_EVIDENCE_DIR:-/tmp}" || setup_fail 'create transfer evidence parent'
    TRANSFER_EV=$(mktemp -d "${SIM_EVIDENCE_DIR:-/tmp}/hot-update-transfer.$1.XXXXXX") || setup_fail 'create transfer evidence'
    chmod 0700 "$TRANSFER_EV" || setup_fail 'protect transfer evidence'
    TRANSFER_STEP=0; TRANSFER_STARTED=0; TRANSFER_JUDGED=0
    TRANSFER_MANIFEST="$TRANSFER_EV/manifest.json"
    TRANSFER_FIXTURE_MANIFEST="$TRANSFER_EV/fixture-manifest.json"
    # The runner binds control receipts to this invocation's private directory.
    printf 'TRANSFER-EVIDENCE directory=%s\n' "$TRANSFER_EV"
    if [ -n "${TRANSFER_TOPOLOGY:-}" ]; then
        transfer_setup 'freeze four cohorts rates and every leg before business' \
            python3 -I -B "$HERE/drills/lib/transfer_mutations.py" manifest --deployment "$TRANSFER_DEPLOYMENT_MANIFEST" \
            --arm "$1" --topology "$TRANSFER_TOPOLOGY" --output "$TRANSFER_MANIFEST"
    else
        transfer_setup 'freeze four cohorts rates and every leg before business' \
            python3 -I -B "$HERE/drills/lib/transfer_mutations.py" manifest --deployment "$TRANSFER_DEPLOYMENT_MANIFEST" \
            --arm "$1" --output "$TRANSFER_MANIFEST"
    fi
    drill_install_traps transfer_cleanup_all
    case "$1" in M*) transfer_control_build "$1" ;; esac
    if [ "${2:-}" = grow_to_3 ]; then
        transfer_setup 'fresh quorum grows before business' transfer_deploy grow_to_3 4 1 0
    fi
    transfer_setup 'project the frozen fixture manifest after artifact binding' \
        python3 -I -B "$HERE/drills/lib/transfer_fixture_manifest.py" manifest \
        --manifest "$TRANSFER_MANIFEST" --output "$TRANSFER_FIXTURE_MANIFEST"
    # All artifacts/HTTPS URLs/instance paths must be in the input manifest
    # before start. P29's start provisions observation and launches original
    # CLI push/pull with the sealed rate; it cannot mint another transfer ID.
    TRANSFER_STARTED=1
    transfer_setup 'start four isolated original transfers and ten control slots per second' transfer_fixture start
    transfer_setup 'record original cohort baseline and independent source hashes' transfer_fixture sample
}

transfer_control_build() {
    [ -r "${TRANSFER_MUTATION_PATCHES:-}/$1.json" ] || setup_fail 'production mutation owner patch is unavailable'
    transfer_setup 'apply exact SHA-bound production predicate overlay' \
        python3 -I -B "$HERE/drills/lib/transfer_mutations.py" overlay --repo "$HERE/../.." \
        --output "$TRANSFER_EV/mutation" --mutation "$1" --patch "$TRANSFER_MUTATION_PATCHES/$1.json"
    # origin: hot-update S7 plan §10.5 / T44. GOFLAGS reaches EVERY nested
    # builder; an outer go-test overlay alone does not alter its go-build child.
    GOFLAGS=$(cat "$TRANSFER_EV/mutation/goflags.txt") || setup_fail 'read nested builder selector'
    export GOFLAGS
    transfer_setup 'build original capable mutant before original business starts' transfer_step \
        go build -trimpath -o "$TRANSFER_EV/mutation/tether-original" "$HERE/../../cmd/tether"
    transfer_setup 'build real mutant before original business starts' transfer_step \
        go build -trimpath=false -o "$TRANSFER_EV/mutation/tether" "$HERE/../../cmd/tether"
    _tcb_sha=$(sha256sum "$TRANSFER_EV/mutation/tether" | awk '{print $1}') || setup_fail 'hash built mutant'
    printf '{"artifact_sha":"%s"}\n' "$_tcb_sha" > "$TRANSFER_EV/mutation/build.json"
    for _tcb_reader in A B; do
        _tcb_binary="$TRANSFER_EV/mutation/tether"
        [ "$_tcb_reader" != A ] || _tcb_binary="$TRANSFER_EV/mutation/tether-original"
        transfer_setup 'read actual compiled capable mutation reader' transfer_step timeout 5 "$_tcb_binary" version
        cp "$TRANSFER_STEP_LOG" "$TRANSFER_EV/mutation/reader-$_tcb_reader.out" || setup_fail 'retain actual mutation reader output'
    done
    # Both A and B contain the SAME source mutation and are real distinct
    # builds. Otherwise a baseline-only mutant would vanish on the first cut,
    # or a candidate-only mutant would miss the original ctl's reconnect path.
    transfer_setup 'bind real mutated A and B before sealing original business' \
        python3 -I -B "$HERE/drills/lib/transfer_mutations.py" bind --manifest "$TRANSFER_MANIFEST" --evidence "$TRANSFER_EV"
    # Frozen fixture convention: mutation/source.json and the two binaries
    # select the affected startup roles; runtime JSON proves postcut /proc SHA.
}

transfer_wait_window() {
    _tww_step=$1
    printf '%s\n' "$_tww_step" > "$TRANSFER_EV/requested-step"
    _tww_until=$(( $(date +%s) + 190 ))
    while :; do
        transfer_fixture sample
        _tww_rc=$?
        [ "$_tww_rc" = 0 ] || { transfer_native "$_tww_rc"; return 1; }
        python3 -I -B "$HERE/drills/lib/transfer_mutations.py" sample --manifest "$TRANSFER_MANIFEST" \
            --sample "$TRANSFER_EV/sample.json" --step "$_tww_step"
        _tww_rc=$?
        case "$_tww_rc" in
            0) return 0 ;;
            4)
                # The fixture continuously observes actual leg-start clocks;
                # no new deadline or rate is granted when the window is gone.
                if jq -e '.window_expired==true' "$TRANSFER_EV/sample.json" >/dev/null 2>&1 || \
                    [ "$(date +%s)" -ge "$_tww_until" ]; then
                    transfer_native 4; return 1
                fi
                sleep 1
                ;;
            *) transfer_native 2; return 1 ;;
        esac
    done
}

transfer_artifact() {
    # Artifacts are prebuilt, and B→A always reads the original capable A.
    # No product state or secrets are serialized into this public manifest.
    _ta_key=$1
    TRANSFER_BINARY=$(jq -er --arg key "$_ta_key" '.artifacts[$key].binary' "$TRANSFER_MANIFEST") || return 2
    TRANSFER_URL=$(jq -er --arg key "$_ta_key" '.artifacts[$key].url' "$TRANSFER_MANIFEST") || return 2
    TRANSFER_SHA=$(jq -er --arg key "$_ta_key" '.artifacts[$key].sha256' "$TRANSFER_MANIFEST") || return 2
    TRANSFER_VERSION=$(jq -er --arg key "$_ta_key" '.artifacts[$key].version' "$TRANSFER_MANIFEST") || return 2
    [ -r "$TRANSFER_BINARY" ] && [ "$(sha256sum "$TRANSFER_BINARY" | awk '{print $1}')" = "$TRANSFER_SHA" ] || return 2
}

transfer_agent_cut() {
    _tac_artifact=$1
    transfer_artifact "$_tac_artifact" || { transfer_native 2; return 1; }
    for _tac_nid in $(jq -er '.cohorts[].nid' "$TRANSFER_MANIFEST"); do
        # origin: hot-update S7 plan §10.4 / T43. Actual product entry point.
        transfer_step "$SIM" ctl -- node upgrade "$_tac_nid" --url "$TRANSFER_URL" --sha256 "$TRANSFER_SHA"
        _tac_rc=$?
        printf '%s\n' "$_tac_rc" > "$TRANSFER_EV/command-$TRANSFER_CELL-$_tac_nid.rc"
        # Command75/defer is a missed positive cut, never successful coverage.
        case "$ARM" in R1|R2) ;; *)
            [ "$_tac_rc" = 0 ] || { transfer_native 4; return 1; } ;;
        esac
    done
}

# Shell deployment helpers are functions, not executables. The shared supervisor
# owns this fresh shell and all of its transport descendants; no eval is used.
transfer_deploy() {
    transfer_step sh -c 'HERE=$1; shift; . "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/drills/lib/logs.sh"; . "$HERE/drills/lib/cluster.sh"; "$@"' \
        transfer-deploy "$HERE" "$@"
}

transfer_stage_broker() {
    _tsb_node=$1
    # Copy candidate and preserve ACTUAL current reader; never overwrite the
    # mapped executable inode. broker reload remains the product's own verb.
    transfer_deploy d cp "$TRANSFER_BINARY" "$(ctr_name "$_tsb_node"):/tmp/tether-s7-candidate" || return 2
    transfer_deploy dexec "$_tsb_node" -- sh -eu -c \
        'p=$(systemctl show tether-broker -p MainPID --value); test "$p" -gt 0; install -m 0755 "/proc/$p/exe" /usr/local/bin/tether.prev; install -m 0755 /tmp/tether-s7-candidate /usr/local/bin/tether'
}

transfer_broker_cut() {
    transfer_artifact "$1" || { transfer_native 2; return 1; }
    _tbc_topology=$(jq -er '.topology' "$TRANSFER_MANIFEST") || return 1
    for _tbc_node in $(jq -er '.brokers[].node' "$TRANSFER_MANIFEST"); do
        transfer_stage_broker "$_tbc_node" || { transfer_native 2; return 1; }
    done
    if [ "$_tbc_topology" = quorum ]; then
        # S6's production roll performs fresh leadership transfer and checks
        # follower-first/leader-last at EVERY hop. P29 judges each hop against
        # source-follower/original-leader/third-voter, never just final SHA.
        transfer_step "$SIM" exec brk1 -- runuser -u tether -- env HOME=/var/lib/tether \
            /usr/local/bin/tether cluster upgrade --broker-only --to-version "$TRANSFER_VERSION" \
            --expect-sha256 "$TRANSFER_SHA" --account-seed /etc/tether/secrets/account.nk --backup-taken
    else
        _tbc_socket=$(jq -er '.brokers[0].socket' "$TRANSFER_MANIFEST") || return 1
        transfer_step "$SIM" exec brk1 -- /usr/local/bin/tether broker upgrade reload \
            --socket "$_tbc_socket" --to-version "$TRANSFER_VERSION" --expect-sha256 "$TRANSFER_SHA"
    fi
    _tbc_rc=$?
    printf '%s\n' "$_tbc_rc" > "$TRANSFER_EV/command-$TRANSFER_CELL.rc"
    case "$ARM" in R1|R2) ;; *)
        [ "$_tbc_rc" = 0 ] || { transfer_native 4; return 1; } ;;
    esac
    return 0
}

# origin: hot-update S7 plan §8.4 / §10.6 / T43. Separate legacy and unknown
# responsibilities still refuse. The original fixture checks exact detail,
# unchanged running SHA/PID, zero cut effects and eventual original completion.
transfer_safe_defer() {
    _tsd_kind=$(jq -er --arg id "$TRANSFER_CELL" '.steps[] | select(.id==$id) | .kind' "$TRANSFER_MANIFEST") || return 2
    _tsd_node=$(jq -er --arg kind "$_tsd_kind" '.refusal_lanes[] | select(.kind==$kind) | .node' "$TRANSFER_MANIFEST") || return 2
    _tsd_socket=$(jq -er --arg kind "$_tsd_kind" '.refusal_lanes[] | select(.kind==$kind) | .socket' "$TRANSFER_MANIFEST") || return 2
    _tsd_artifact=$(jq -er --arg kind "$_tsd_kind" '.refusal_lanes[] | select(.kind==$kind) | .artifact_key' "$TRANSFER_MANIFEST") || return 2
    transfer_artifact "$_tsd_artifact" || return 2
    transfer_stage_broker "$_tsd_node" || return 2
    transfer_step "$SIM" exec "$_tsd_node" -- /usr/local/bin/tether broker upgrade reload \
        --socket "$_tsd_socket" --to-version "$TRANSFER_VERSION" --expect-sha256 "$TRANSFER_SHA"
    _tsd_rc=$?
    printf '%s\n' "$_tsd_rc" > "$TRANSFER_EV/command-$TRANSFER_CELL.rc"
    [ "$_tsd_rc" = 75 ] || { transfer_native 1; return 1; }
    return 0
}

# Return protocol shared with transfer_run: 0 = injected, 1 = this function has
# ALREADY recorded the native status, anything else = the native status still
# to record. A raw command status therefore never escapes: an injection that
# failed with exit 1 used to read as "already recorded", so transfer_run broke
# off the ladder with nothing counted and the drill went GREEN on a fault it
# never injected (C3 restart, X cancel). origin: r9d D-11 arm controls.
transfer_fault() {
    _tf_kind=$1
    case "$_tf_kind" in
        C1|C2)
            _tf_field=carrier; [ "$_tf_kind" != C2 ] || _tf_field=data
            # Kill only actual attachment TCP sockets from the predeclared
            # mapping. A TCP kill is distinct from JS server restart below.
            jq -er --arg field "$_tf_field" '.connections[$field][] | [.node,.peer,.port] | @tsv' \
                "$TRANSFER_EV/sample.json" > "$TRANSFER_EV/fault-connections.tsv" || return 2
            while IFS="$(printf '\t')" read -r _tf_node _tf_peer _tf_port; do
                transfer_deploy dexec "$_tf_node" -- ss -K dst "$_tf_peer" dport = "$_tf_port" || return 2
            done < "$TRANSFER_EV/fault-connections.tsv"
            ;;
        C3)
            # Same installed binary and store_dir, checked before/after by P29.
            # A restart that did not happen is a harness failure, not a cut.
            python3 -I -B -c 'import json,sys,time; n=time.monotonic_ns(); open(sys.argv[1],"w").write(json.dumps(dict(start_ns=n,end_ns=n+5000000000)))' \
                "$TRANSFER_EV/reader-fault.json" || return 2
            transfer_deploy sctl brk1 restart nats-server || return 2
            ;;
        W1|W2)
            # Isolated per-agent NATS data+register SYN blackhole. Existing
            # control delivers upgrade, then new-image attachments cannot
            # reconnect. P29 must independently prove byte IO is pending.
            # The already
            # launched shared-supervised fixture observes the real120s rollback
            # and receipt before marking release; it cannot issue an upgrade.
            for _tf_node in $(jq -er '.cohorts[].agent_node' "$TRANSFER_MANIFEST"); do
                transfer_deploy dexec "$_tf_node" -- iptables -I OUTPUT -p tcp --syn --dport 4222 -j DROP || return 2
                TRANSFER_FAULT_NODES="$TRANSFER_FAULT_NODES $_tf_node"
            done
            # transfer_agent_cut already recorded its native status; returning
            # 4 here counted the same missed cut a second time.
            transfer_agent_cut B || return 1
            printf '%s\n' "$_tf_kind-release" > "$TRANSFER_EV/requested-step"
            _tf_until=$(( $(date +%s) + HOT_UPDATE_REGISTER_DEADLINE_S + 30 ))
            while :; do
                transfer_fixture sample || return 2
                jq -e --arg arm "$_tf_kind" '.watchdog[$arm].current_cut_observed==true and .watchdog[$arm].elapsed_s>=120 and .watchdog[$arm].pending_data==true' \
                    "$TRANSFER_EV/sample.json" >/dev/null 2>&1 && break
                [ "$(date +%s)" -lt "$_tf_until" ] || return 4
                sleep 1
            done
            for _tf_node in $(jq -er '.cohorts[].agent_node' "$TRANSFER_MANIFEST"); do
                transfer_deploy dexec "$_tf_node" -- iptables -D OUTPUT -p tcp --syn --dport 4222 -j DROP || return 2
            done
            TRANSFER_FAULT_NODES=
            ;;
        cancel)
            _tf_pid=$(jq -er '.victims.cancel.ctl_pid' "$TRANSFER_EV/sample.json") || return 2
            # A Ctrl-C the harness could not deliver is a setup failure.
            transfer_step "$SIM" exec ctl -- kill -INT "$_tf_pid" || return 2
            ;;
        session-rm)
            _tf_sid=$(jq -er '.victims[] | select(.kind=="session-rm") | .sid' "$TRANSFER_MANIFEST") || return 2
            # The product's refusal leaves the victim window unexercised:
            # missed coverage, exactly like a refused cut command.
            transfer_step "$SIM" ctl -- session rm "$_tf_sid" || return 4
            ;;
        R1|R2)
            # P32 supplies separate real import-barrier artifacts per matrix
            # cell (including broker NewStore versus NewControl). Current-state
            # rollback is performed by the product, never simulated by restore.
            if ! transfer_artifact "$TRANSFER_CELL"; then return 2; fi
            _tf_layer=$(jq -er --arg id "$TRANSFER_CELL" '.steps[] | select(.id==$id) | .layer' "$TRANSFER_MANIFEST") || return 2
            case "$_tf_layer" in
                agent) transfer_agent_cut "$TRANSFER_CELL" ;;
                broker-*) transfer_broker_cut "$TRANSFER_CELL" ;;
                *) return 2 ;;
            esac
            # The command can end unknown after a genuine rollback. Its raw
            # native status is kept; only the fixture judges the import effect.
            ;;
        *) return 2 ;;
    esac
}

transfer_finish() {
    # P29 drain joins owners, waits quiet>=2s, fresh drain and fresh JS cursor.
    # Missing ACK is SETUP2; an unfinished data/cut matrix is INCOMPLETE4.
    transfer_fixture drain
    _tfin_drain=$?
    transfer_native "$_tfin_drain"
    transfer_fixture judge
    _tfin_rc=$?
    cp "$TRANSFER_STEP_LOG" "$TRANSFER_EV/judge.out" || { transfer_native 2; return; }
    printf '%s\n' "$_tfin_rc" > "$TRANSFER_EV/judge.rc"
    TRANSFER_JUDGED=1
    cp "$TRANSFER_EV/judge.out" "$TRANSFER_EV/initial-judge.out" || transfer_native 2
    cp "$TRANSFER_EV/judge.rc" "$TRANSFER_EV/initial-judge.rc" || transfer_native 2
    # origin: S7 internal review rv-drill F8. Late poison and the final join
    # participate in the only parent verdict, before any control signs a kill.
    transfer_cleanup
    _tfin_stop=$?
    transfer_native "$_tfin_rc"
    [ "$_tfin_stop" = 0 ] || transfer_native "$_tfin_stop"
    if [ "$_tfin_rc" = 0 ] && [ "$_tfin_stop" != 0 ]; then
        _tfin_rc=$_tfin_stop
        cp "$TRANSFER_EV/stop.out" "$TRANSFER_EV/judge.out" || transfer_native 2
        printf '%s\n' "$_tfin_rc" > "$TRANSFER_EV/judge.rc"
    fi
    case "$ARM" in M*)
        python3 -I -B "$HERE/drills/lib/transfer_mutations.py" kill --evidence "$TRANSFER_EV" \
            --mutation "$ARM" --native-rc "$_tfin_rc"
        _tfin_kill=$?
        [ "$_tfin_kill" = 0 ] || transfer_native 2
        ;;
    esac
}

transfer_run() {
    transfer_prepare "$1" "${2:-}"
    python3 -I -B "$HERE/drills/lib/transfer_mutations.py" schedule --manifest "$TRANSFER_MANIFEST" \
        > "$TRANSFER_EV/schedule.tsv" || setup_fail 'required transfer schedule is incomplete'
    while IFS="$(printf '\t')" read -r TRANSFER_CELL _tr_leg _tr_action; do
        case "$ARM" in R1|R2)
            printf '%s\n' "$TRANSFER_CELL" > "$TRANSFER_EV/requested-step"
            transfer_fixture start || { transfer_native 2; break; }
            ;; esac
        transfer_wait_window "$TRANSFER_CELL" || break
        case "$_tr_action" in
            agent-forward) transfer_agent_cut B ;;
            agent-rollback) transfer_agent_cut A ;;
            broker-forward) transfer_broker_cut B ;;
            broker-rollback) transfer_broker_cut A ;;
            safe-defer) transfer_safe_defer ;;
            *) transfer_fault "$_tr_action" ;;
        esac
        _tr_rc=$?
        [ "$_tr_rc" = 0 ] || { [ "$_tr_rc" = 1 ] || transfer_native "$_tr_rc"; break; }
        # P29 reads real postcut progress and process/attachment identities.
        transfer_fixture sample || { transfer_native 2; break; }
    done < "$TRANSFER_EV/schedule.tsv"
    transfer_finish
}
