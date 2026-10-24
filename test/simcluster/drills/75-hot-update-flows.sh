#!/bin/sh
# 75-hot-update-flows.sh — one original cohort across agent/carrier replacement.
# origin: hot-update S5 plan §9.1–§9.5 / T48.
# arms: A C R B W L
# fixture: A=N1 C=N3-live R=N1 B=N1 W=N1 L=N1
# grows: A=0 C=2 R=0 B=0 W=0 L=0
# worst: A=420 C=600 R=420 B=540 W=660 L=420
# forgoes: A=- C=- R=- B=- W=- L=-
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
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/ingress.sh"
. "$HERE/drills/lib/artifact.sh"
. "$HERE/drills/lib/upgradecfg.sh"
. "$HERE/drills/lib/logs.sh"
. "$HERE/drills/lib/hot_update_pty.sh"
. "$HERE/drills/lib/fault.sh"
. "$HERE/drills/lib/cluster.sh"
SIM_REPO=$(cd "$HERE/../.." && pwd)
SIM_VENDOR="$HERE/vendor"
. "$HERE/lib/stage.sh"
. "$HERE/drills/lib/legacy_flow_supply.sh"
. "$HERE/drills/lib/hot_update_flow_faults.sh"
SIM=${SIM:-$HERE/simcluster}
SID=lab
PIN=135790
FLOW_EV=
FLOW_NONCE=
FLOW_UPGRADE_JOB=
FLOW_STEP_COUNT=0
# The live fixture consumes FLOW_EV/FLOW_NONCE and dispatches the commands
# below. P24's byte oracle alone does not provide workload orchestration.
# Missing implementation is a setup failure, never a fabricated trace.
if [ -r "$HERE/drills/lib/flow.sh" ]; then . "$HERE/drills/lib/flow.sh"; fi
# origin: hot-update S6 plan §10.1 / §12.1 P30. P29 owns the callable
# wrapper. Load it LAST so the old S5 shell adapter cannot shadow its custody.
if [ -r "$HERE/drills/lib/flow_fixture" ]; then . "$HERE/drills/lib/flow_fixture"; fi

_flow_cleanup() {
    [ -n "$FLOW_EV" ] || return 0
    flow_fault_cleanup
    if command -v flow_fixture >/dev/null 2>&1; then flow_fixture cleanup || true; fi
    if [ -n "$FLOW_UPGRADE_JOB" ]; then wait "$FLOW_UPGRADE_JOB" 2>/dev/null || true; fi
    artifact_down >/dev/null 2>&1 || true
}
_flow_broker_ready() {
    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Restarting brk1 in N3
    # can elect another healthy voter; readiness belongs to the cluster and
    # the restarted member, not the identity of its current leader.
    if [ "$ARM" = C ]; then
        "$SIM" exec brk1 -- /usr/local/bin/tether cluster status --json | jq -e '
            .health=="HEALTHY_HA" and .partial==false and (.errors|length)==0 and
            (.leader_id|type=="string" and length>0) and
            ([.nodes[]? | select(.phase=="VOTER" and .reachable==true and
                .applied_lag==0 and .inconsistent==false)] | length)==3 and
            ([.nodes[]? | select(.node_id=="brk1")] | length)==1' >/dev/null
        return $?
    fi
    "$SIM" exec brk1 -- /usr/local/bin/tether cluster status --json | jq -e '.leader_id=="brk1"' >/dev/null
}
# Every N3 member can accept the operator request after an election. Configure
# the mirror on all voters before business, using the existing deployment owner.
# origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.C
_flow_allow_brokers() {
    _fab_nodes=brk1
    [ "$ARM" != C ] || _fab_nodes='brk1 brk2 brk3'
    for _fab_node in $_fab_nodes; do
        upgradecfg_allow_broker "$_fab_node" _flow_broker_ready || return $?
    done
}
_flow_agent_ready() {
    "$SIM" ctl -- node ls --json | jq -e '[.nodes[]? | select(.nid=="agt1" and .status=="ONLINE")] | length==1' >/dev/null
}
_flow_evidence_init() {
    # origin: hot-update S5 plan §9.1 / T48. An absent live collector must
    # fail before installing cleanup that would touch an isolated cluster.
    [ -r "${HERE:-}/drills/lib/flow_fixture" ] && command -v flow_fixture >/dev/null 2>&1 || setup_fail 'S5 live flow fixture implementation is unavailable'
    FLOW_STARTED=$(python3 -c 'import time; print(time.monotonic())') || setup_fail 'read monotonic clock'
    mkdir -p "${SIM_EVIDENCE_DIR:-/tmp}" || setup_fail 'create evidence parent'
    FLOW_EV=$(mktemp -d "${SIM_EVIDENCE_DIR:-/tmp}/hot-update-flow.${ARM}.XXXXXX") || setup_fail 'create flow evidence'
    chmod 0700 "$FLOW_EV" || setup_fail 'protect private identity evidence'
    FLOW_NONCE=$(python3 -c 'import secrets; print(secrets.token_hex(16))') || setup_fail 'create cohort nonce'
    drill_install_traps _flow_cleanup
}
_flow_prepare() {
    if [ -z "$FLOW_EV" ]; then _flow_evidence_init; fi
    flow_step_run flow_fixture selftest
    _flow_native_rc=$?
    assert_setup "independent raw and AEAD oracle controls" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    flow_step_run artifact_flow_readers_ok "$SIM_VENDOR/tether" "$SIM_VENDOR/tether-next" "$FLOW_EV/readers.json"
    _flow_native_rc=$?
    assert_setup "both artifacts read flow/SS domains" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    if [ "$ARM" != C ]; then
        flow_step_run "$SIM" nuke
        _flow_native_rc=$?
        assert_setup "discard only isolated drill instance" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        flow_step_run "$SIM" up --brokers 1 --agents 1 --ctl 1
        _flow_native_rc=$?
        assert_setup "N1 broker agent and ctl" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        flow_step_run "$SIM" init brk1
        _flow_native_rc=$?
        assert_setup "initialize N1" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    fi
    flow_step_run _flow_allow_brokers
    _flow_native_rc=$?
    assert_setup "broker artifact allowlist deployment" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    if [ "$ARM" = F ]; then
        assert_setup "fixed flow exporter source" sim_flow_legacy_source_ok
        # origin: S8 internal review s8-hermetic F9. Select the ctl path in
        # the parent; assert_setup captures only remote installation status.
        LEGACY_CTL_BIN=$FLOW_LEGACY_CTL
        assert_setup "old flow broker and explicit upgrade ctl before business" upgrade_legacy_stack "$SIM_VENDOR/tether-flow-legacy" "$SIM_VENDOR/tether-flow-legacy-source.json" "$FLOW_LEGACY_CTL" "$FLOW_EV"
        assert_setup "old flow broker ready" poll_until 60 1 "old broker" -- _flow_broker_ready
    fi
    # L switches the actual broker before the FIRST session or business object.
    if [ "$ARM" = L ]; then
        flow_step_run legacy_flow_broker_install
        _flow_native_rc=$?
        assert_setup "fixed historical broker before first business" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    fi
    flow_step_run "$SIM" session "$SID" --pin "$PIN"
    _flow_native_rc=$?
    assert_setup "create isolated session" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    flow_step_run "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
    _flow_native_rc=$?
    assert_setup "onboard agent" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    flow_step_run upgradecfg_allow_agent agt1 _flow_agent_ready
    _flow_native_rc=$?
    assert_setup "agent artifact allowlist deployment" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    if [ "$ARM" = F ]; then
        flow_step_run legacy_flow_agent_install
        _flow_native_rc=$?
        assert_setup "fixed historical exporter before business" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    fi
    flow_step_run artifact_up "$INSTANCE" "$SIM_VENDOR/tether-next" candidate.tar.gz
    _flow_native_rc=$?
    assert_setup "serve candidate artifact" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    flow_step_run ingress_trust_inject agt1
    _flow_native_rc=$?
    assert_setup "artifact CA deployment" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    flow_step_run artifact_add "$SIM_VENDOR/tether" original.tar.gz
    _flow_native_rc=$?
    assert_setup "serve original reader for reverse hop" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    FLOW_FORWARD_URL=$ART_URL; FLOW_FORWARD_SHA=$ART_SHA
    FLOW_REVERSE_URL=$ART2_URL; FLOW_REVERSE_SHA=$ART2_SHA
    cp "$SIM_VENDOR/tether-source.json" "$FLOW_EV/source.json" || setup_fail 'retain source provenance'
    FLOW_CURSOR=$(sim_agent_slog_cursor agt1) || setup_fail 'read fresh agent log cursor'
    # The fixture declares roles, final sequences/deadlines and normal <=224MiB profile
    # before starting any sockets; C declares separate saturation/normal phases.
    if [ "$ARM" = F ]; then
        assert_setup "historical flow original role identities" upgrade_legacy_snapshot before "$FLOW_EV"
        sha256sum "$SIM_VENDOR/tether-next" | awk '{print $1}' > "$FLOW_EV/legacy-target.sha"
        # The original fixture must honor the explicit old executable; its
        # endpoint receipts remain mandatory, even if it ignores these inputs.
        export FLOW_CTL_BINARY="$FLOW_LEGACY_CTL" FLOW_CONTROL_STACK=old-old-old
    fi
    flow_step_run flow_fixture prepare "$1"
    _flow_native_rc=$?
    assert_setup "declare original cohort and finite final ledger" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    if [ "$ARM" = C ]; then
        # origin: hot-update S5 plan lead C1 / §3.4 / DRILL-RUN.
        # Finish saturation by declared FIN before starting normal requests;
        # the N3 cluster and product image remain live throughout both phases.
        flow_step_run flow_fixture saturation
        _flow_native_rc=$?
        assert_ok "32 active reservations typed capacity refusals natural FIN and recovery" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    fi
    flow_step_run flow_fixture start
    _flow_native_rc=$?
    assert_setup "start original SSH TCP OPEN half-close SS and ten requests/s" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    flow_step_run flow_fixture baseline
    _flow_native_rc=$?
    assert_setup "at least twenty samples in each original direction" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    if [ "$ARM" = L ]; then
        flow_step_run legacy_flow_broker_verify
        _flow_native_rc=$?
        assert_setup "running historical broker still owns first business" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        flow_step_run flow_fixture checkpoint legacy-transport
        _flow_native_rc=$?
        assert_setup "capture empty ALPN and unframed original bytes" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        flow_step_run legacy_flow_raw_valid
        _flow_native_rc=$?
        assert_setup "legacy transport evidence is nonempty" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    fi
}
_flow_upgrade_start() {
    _fus_hop=$1; _fus_url=$2; _fus_sha=$3
    # Start/reap in the main shell. Do not pipe through tee or assert_setup:
    # either would hide the command's native status or lose its child identity.
    (
        if [ "$ARM" = F ]; then
            upgrade_legacy_ctl node upgrade agt1 --url "$_fus_url" --sha256 "$_fus_sha" > "$FLOW_EV/upgrade-$_fus_hop.out" 2>&1
        else
            timeout 90 "$SIM" ctl -- node upgrade agt1 --url "$_fus_url" --sha256 "$_fus_sha" > "$FLOW_EV/upgrade-$_fus_hop.out" 2>&1
        fi
        _fus_rc=$?
        printf '%s\n' "$_fus_rc" > "$FLOW_EV/upgrade-$_fus_hop.rc"
        exit "$_fus_rc"
    ) &
    FLOW_UPGRADE_JOB=$!
}
_flow_upgrade_wait() {
    wait "$FLOW_UPGRADE_JOB"
    FLOW_UPGRADE_RC=$?
    FLOW_UPGRADE_JOB=
    assert_setup 'upgrade native command succeeded' flow_step_status "$FLOW_UPGRADE_RC" "$FLOW_EV/upgrade-$1.out"
    flow_step_run flow_fixture checkpoint "hop-$1"
    _flow_native_rc=$?
    assert_setup "same PID different running SHA and committed marker" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
}
_flow_finish() {
    flow_step_run flow_fixture finish
    _flow_native_rc=$?
    assert_setup "finish original finite streams and save final identity" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    flow_step_run flow_fixture collect
    _flow_native_rc=$?
    assert_setup "collect original ledgers SQL per-domain FD/replay and four logs before cleanup" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    # The common raw judge is the product assertion, never a setup assertion.
    flow_step_run flow_fixture continuity
    _ff_rc=$?
    assert_ok 'original bidirectional bytes identities and maximum five-second stalls' flow_step_status "$_ff_rc" "$FLOW_STEP_LOG"
    _ff_verdict=GREEN
    if [ "$_AS_FAIL" != 0 ]; then _ff_verdict=ASSERT-FAIL; fi
    artifact_flow_receipt "$ARM" "$_ff_rc" "$_ff_verdict" "$FLOW_STARTED" || setup_fail 'save complete source artifact oracle receipt'
}
_flow_revoke_window() {
    flow_step_run flow_fixture wait-event "$1" 5
    _flow_native_rc=$?
    assert_setup "observed original cut event before revoke" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    # origin: hot-update S5 plan §9.1 / DRILL-RUN. The SYN fault ends at
    # the observed cut; ledger/SQL collection must not spend the original
    # connect deadline while keeping the target artificially unreachable.
    if [ "$#" = 2 ]; then
        flow_step_run "$2"
        _flow_native_rc=$?
        assert_setup "release only target SYN block after cut" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    fi
    flow_step_run flow_fixture revoke alice
    _flow_native_rc=$?
    assert_setup "revoke silent alice through real product authority" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    flow_step_run flow_fixture checkpoint revoked
    _flow_native_rc=$?
    assert_setup "applied revision silent victim shutdown dup and replay zero while bob advances" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
}
_flow_upgrade() {
    _flow_upgrade_start "$1" "$2" "$3"
    _flow_upgrade_wait "$1"
}
_flow_legacy_arm() {
    [ "$#" = 2 ] && [ "$2" = old-old-old ] || setup_fail 'explicit historical flow control stack is required'
    FLOW_LEGACY_CTL=$1
    _flow_prepare legacy-exporter
    _flow_upgrade 1 "$FLOW_FORWARD_URL" "$FLOW_FORWARD_SHA"
    cp "$FLOW_EV/upgrade-1.rc" "$FLOW_EV/legacy-command.rc" || setup_fail 'retain historical command rc'
    assert_setup "historical flow exec kernel evidence" upgrade_legacy_snapshot after "$FLOW_EV"
    assert_setup "historical flow exec is required" upgrade_legacy_exec_required "$FLOW_EV" agent
    flow_step_run flow_fixture checkpoint legacy-fault
    _flow_native_rc=$?
    assert_setup "retain actual bare exec event and historical supply proof" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    flow_step_run flow_fixture finish
    _flow_native_rc=$?
    assert_setup "finish original legacy workload and capture sticky EOF/RST loss" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    flow_step_run flow_fixture collect
    _flow_native_rc=$?
    assert_setup "collect legacy oracle inputs before cleanup" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
    # Fault validity is a setup predicate; safe busy or unobserved exec is rc2.
    flow_step_run flow_fault_valid "$FLOW_EV/legacy-fault.json"
    _flow_native_rc=$?
    assert_setup "real bare exec fault was observed" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
}
_flow_legacy_receipt() {
    _fla_verdict=ASSERT-FAIL
    if [ "$_AS_FAIL" = 0 ] && [ "$_AS_PRODUCT_RED" != 0 ]; then _fla_verdict=PRODUCT-RED; fi
    artifact_flow_receipt F "$(cat "$FLOW_EV/continuity.rc")" "$_fla_verdict" "$FLOW_STARTED" '39-hot-update-legacy-n1.F' || setup_fail 'save historical flow exporter receipt'
}

if [ "${FLOW_LIBRARY_ONLY:-0}" != 1 ]; then
    drill_begin "75-hot-update-flows [arm ${ARM:-?}]"
    case "${ARM:?}" in
    A)
        _flow_prepare normal
        # Begin artifact staging before the pending connector's fixed budget.
        # The OPEN prior below still requires the original EINPROGRESS socket;
        # an upgrade that reaches the cut too early cannot sign that receipt.
        # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
        flow_step_run flow_artifact_gate_arm
        _flow_native_rc=$?
        assert_setup "gate final artifact byte before pending OPEN" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        _flow_upgrade_start 1 "$FLOW_FORWARD_URL" "$FLOW_FORWARD_SHA"
        flow_step_run flow_artifact_gate_wait
        _flow_native_rc=$?
        assert_setup "real artifact download reached final byte" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        flow_step_run flow_open_syn_on "$FLOW_OPEN_IP" "$FLOW_OPEN_PORT"
        _flow_native_rc=$?
        assert_setup "target-specific SYN block for original OPEN" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        flow_step_run flow_fixture open "$ART_BASE_URL"
        _flow_native_rc=$?
        assert_setup "start original OPEN after native extraction under target fault" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        flow_step_run flow_fixture checkpoint open-partial
        _flow_native_rc=$?
        assert_setup "collect original EINPROGRESS and partial cipher prior" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        flow_step_run flow_open_partial_valid "$FLOW_EV/open-partial.json"
        _flow_native_rc=$?
        assert_setup "true original EINPROGRESS FD and nonempty partial SS ciphertext" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        flow_step_run flow_artifact_gate_release
        _flow_native_rc=$?
        assert_setup "verify artifact delivery and original OPEN prior" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        _flow_revoke_window final-cut flow_open_syn_off
        _flow_upgrade_wait 1
        flow_step_run flow_fixture checkpoint open-completed
        _flow_native_rc=$?
        assert_setup "same pending FD completes without redial" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        flow_step_run flow_fixture checkpoint post-upgrade
        _flow_native_rc=$?
        assert_setup "same survivors progress after committed upgrade" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        flow_step_run flow_carrier_rebuild
        _flow_native_rc=$?
        assert_setup "force current carrier TCP replacement with same original cohort" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        _flow_finish
        ;;
    C)
        _flow_evidence_init
        flow_step_run grow_to_3 1 1 0
        _flow_native_rc=$?
        assert_setup "N3 live topology without join bypass or retries" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        _flow_prepare capacity-32
        _flow_upgrade 1 "$FLOW_FORWARD_URL" "$FLOW_FORWARD_SHA"
        flow_step_run flow_fixture checkpoint allocation
        _flow_native_rc=$?
        assert_setup "same SQL allocation home epoch and live default reaper" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        _flow_finish
        ;;
    R)
        _flow_prepare silent-revoke
        # The fixture predeclares three original victims, one per observed
        # barrier; callbacks revoke via the real control plane, never on a timer.
        flow_step_run flow_fixture arm-revoke prepare final-cut resume
        _flow_native_rc=$?
        assert_setup "arm three actual product revoke event barriers" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        _flow_upgrade 1 "$FLOW_FORWARD_URL" "$FLOW_FORWARD_SHA"
        flow_step_run flow_fixture checkpoint revoke-races
        _flow_native_rc=$?
        assert_setup "each applied revoke clears original victim while bob advances" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        _flow_finish
        ;;
    B)
        _flow_prepare normal
        _flow_upgrade 1 "$FLOW_FORWARD_URL" "$FLOW_FORWARD_SHA"
        flow_step_run flow_fixture checkpoint active-progress
        _flow_native_rc=$?
        assert_setup "B offsets Base nonce retirement actually advance" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        _flow_upgrade 2 "$FLOW_REVERSE_URL" "$FLOW_REVERSE_SHA"
        flow_step_run flow_fixture checkpoint current-rollback
        _flow_native_rc=$?
        assert_setup "second committed cut uses current B state" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        _flow_finish
        ;;
    W)
        _flow_prepare normal
        flow_step_run flow_watchdog_arm
        _flow_native_rc=$?
        assert_setup "arm bounded NATS-only SYN fault" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        _flow_upgrade_start 1 "$FLOW_FORWARD_URL" "$FLOW_FORWARD_SHA"
        flow_step_run flow_watchdog_first_commit_fault
        _flow_native_rc=$?
        assert_setup "first real register commit filesystem failure after persisted boot count" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        # Candidate is ready but pending: do not require committed hop-1.
        wait "$FLOW_UPGRADE_JOB"; FLOW_UPGRADE_RC=$?; FLOW_UPGRADE_JOB=
        assert_setup 'watchdog upgrade native rc' flow_step_status "$FLOW_UPGRADE_RC" "$FLOW_EV/upgrade-1.out"
        flow_step_run flow_fixture wait-event watchdog-rollback 150
        _flow_native_rc=$?
        assert_setup "original deadline automatically creates second current cut" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        flow_step_run flow_fixture checkpoint watchdog
        _flow_native_rc=$?
        assert_setup "capture real ready pending watchdog receipt" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        flow_step_run flow_watchdog_valid "$FLOW_EV/watchdog.json"
        _flow_native_rc=$?
        assert_setup "watchdog fault and current-cut priors are nonempty" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        _flow_finish
        ;;
    L)
        _flow_prepare legacy-broker
        "$SIM" ctl -- node upgrade agt1 --url "$FLOW_FORWARD_URL" --sha256 "$FLOW_FORWARD_SHA" > "$FLOW_EV/legacy-refusal.out" 2>&1
        FLOW_REFUSAL_RC=$?
        assert_ok 'live legacy responsibility safely refuses with native 75 and exact reason' flow_legacy_refused "$FLOW_REFUSAL_RC" "$FLOW_EV/legacy-refusal.out"
        flow_step_run flow_fixture checkpoint refusal-unchanged
        _flow_native_rc=$?
        assert_setup "legacy refusal leaves running disk marker and business unchanged" flow_step_status "$_flow_native_rc" "$FLOW_STEP_LOG"
        _flow_finish
        ;;
    *) setup_fail "unknown flow arm '$ARM'" ;;
    esac
    log "75 flow receipts retained: $FLOW_EV"
    drill_end
fi
