#!/bin/sh
# 34-hot-update-process.sh — original process survives agent image replacement.
# origin: hot-update S3 plan §8.1. Each arm owns an isolated N1 fixture.
# arms: A G U N P W F
# fixture: A=N1 G=N1 U=N1 N=N1 P=N1 W=N1 F=N1
# grows: A=0 G=0 U=0 N=0 P=0 W=0 F=0
# worst: A=300 G=420 U=420 N=360 P=420 W=600 F=420
# forgoes: A=- G=- U=- N=- P=- W=- F=-
# The source ledger never repairs output. PID triples, original ctl transcripts,
# marker generations, FD classes and agent audit lines are independent evidence.
# Every injection requires >=20 source AND ctl records of progress. Freeze and
# request latency samples are calibration data, never a relaxed acceptance gate.
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/assert.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/ingress.sh"
. "$HERE/drills/lib/artifact.sh"
. "$HERE/drills/lib/upgradecfg.sh"
. "$HERE/drills/lib/logs.sh"
. "$HERE/drills/lib/fault.sh"
. "$HERE/drills/lib/hot_update_budget.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab
PIN=135790
PROBE="$HERE/drills/lib/hot_update_process_probe.py"
MARKER=/home/sim/.local/bin/.tether-upgrade.json
H0_EVIDENCE=
H0_NONCE=
H0_REMOTE=
H0_REMOTE_PROBE=
H0_CTL_PIDFILE=
H0_CTL_JOB=
H0_LOGS_CAPTURED=0
DST=/home/sim/.local/bin/tether
PREV=$DST.prev
AGENT_UNIT=tether-agent
NOHUP_PIDFILE=
MNT=/mnt/hung
HOPS=1
DEFERRED=0
MIXED_JOB=
WEDGE_JOB=

_capture_logs() {
    [ -n "$H0_EVIDENCE" ] && [ "$H0_LOGS_CAPTURED" = 0 ] || return 0
    H0_LOGS_CAPTURED=1
    # Diagnostics are captured even on a setup abort, through the shared map.
    sim_broker_slog brk1 1000 > "$H0_EVIDENCE/broker.slog" 2>&1 || true
    sim_broker_panic_journal_dump brk1 1000 > "$H0_EVIDENCE/broker.boot" 2>&1 || true
    sim_agent_slog_tail agt1 1000 > "$H0_EVIDENCE/agent.slog" 2>&1 || true
    sim_agent_panic_sink_dump agt1 1000 > "$H0_EVIDENCE/agent.boot" 2>&1 || true
}

_cleanup() {
    _capture_logs
    # Stop only this test's producer and verified original ctl. These actions
    # happen after evidence capture and never count as product recovery.
    if [ -n "$H0_REMOTE_PROBE" ] && [ -n "$H0_NONCE" ]; then
        dexec -u sim agt1 -- python3 "$H0_REMOTE_PROBE" stop \
            --root "$H0_REMOTE" --nonce "$H0_NONCE" >/dev/null 2>&1 || true
        dexec -u sim ctl1 -- python3 "$H0_REMOTE_PROBE" stop-ctl \
            --pidfile "$H0_CTL_PIDFILE" --nonce "$H0_NONCE" >/dev/null 2>&1 || true
        if ! poll_until 3 1 "own producer cleanup" -- _producer_finished >/dev/null 2>&1; then
            dexec -u sim agt1 -- python3 "$H0_REMOTE_PROBE" stop --terminate \
                --root "$H0_REMOTE" --nonce "$H0_NONCE" >/dev/null 2>&1 || true
        fi
    fi
    if [ -n "$H0_CTL_JOB" ]; then
        if poll_until 8 1 "own ctl cleanup" -- _ctl_finished >/dev/null 2>&1; then
            wait "$H0_CTL_JOB" 2>/dev/null || true
        else
            warn "34 cleanup: original ctl did not finish; outer command timeout and instance teardown remain armed"
        fi
    fi
    fault_synblock_off agt1 >/dev/null 2>&1 || true
    if [ "${ARM:-}" = F ]; then
        _heal >/dev/null 2>&1 || true
        dexec agt1 -- timeout 8 umount -f -l "$MNT" >/dev/null 2>&1 || true
        dexec agt1 -- pkill -TERM -f '[h]angfs.py' >/dev/null 2>&1 || true
    fi
    if [ -n "$NOHUP_PIDFILE" ] && [ -s "$H0_EVIDENCE/agent-0.json" ]; then
        d cp "$H0_EVIDENCE/agent-0.json" "$(ctr_name agt1):/tmp/agent-$H0_NONCE.json" >/dev/null 2>&1 || true
        dexec -u sim agt1 -- python3 "$H0_REMOTE_PROBE" stop-agent --nonce "$H0_NONCE" \
            --pidfile "$NOHUP_PIDFILE" --identity "/tmp/agent-$H0_NONCE.json" >/dev/null 2>&1 || true
    fi
    artifact_down >/dev/null 2>&1 || true
    true
}
drill_install_traps _cleanup

_broker_ready() {
    "$SIM" exec brk1 -- tether cluster status --json 2>/dev/null \
        | jq -e '.leader_id == "brk1"' >/dev/null
}
_agent_ready() {
    "$SIM" ctl -- node ls --json 2>/dev/null \
        | jq -e '[.nodes[]? | select(.nid == "agt1" and .status == "ONLINE")] | length == 1' >/dev/null
}
_agent_pid() {
    if [ -n "$NOHUP_PIDFILE" ]; then _agent_pid_nohup
    else dexec agt1 -- systemctl show "$AGENT_UNIT" -p MainPID --value; fi
}
_agent_start() {
    if [ -n "$NOHUP_PIDFILE" ]; then
        dexec agt1 -- python3 "$H0_REMOTE_PROBE" identity --pid "$(_agent_pid)" | jq -er '.start_ticks|tostring'
    else dexec agt1 -- systemctl show "$AGENT_UNIT" -p ExecMainStartTimestamp --value; fi
}
_running_sha() { dexec agt1 -- sha256sum "/proc/$1/exe" | awk '{print $1}'; }

_install_probe() {
    d cp "$PROBE" "$(ctr_name agt1):$H0_REMOTE_PROBE" || return 1
    d cp "$PROBE" "$(ctr_name ctl1):$H0_REMOTE_PROBE" || return 1
    dexec agt1 -- chmod 0644 "$H0_REMOTE_PROBE" || return 1
    dexec ctl1 -- chmod 0644 "$H0_REMOTE_PROBE"
}

_launch_producer() {
    # Same user/HOME as simcluster's ctl command. The shell exec preserves its
    # PID so cleanup can verify the exact ctl argv and pin it with pidfd.
    # Redirect the entire group: no command-substitution stdout keeps it alive.
    (
        # The original call outlives the 60s staging request + 120s product
        # register budget and all bounded evidence polls. This is a fixture
        # lifetime, not the hot-update plan's future 5s continuity target.
        dexec -u sim ctl1 -- timeout --signal=TERM --kill-after=3s 590s env HOME=/home/sim sh -c \
            'printf "%s\n" "$$" > "$1"; shift; exec tether "$@"' \
            _ "$H0_CTL_PIDFILE" exec --timeout 580s agt1 -- \
            python3 -u "$H0_REMOTE_PROBE" producer --root "$H0_REMOTE" \
            --nonce "$H0_NONCE" --period 0.05 --lifetime 600
        _h0_ctl_rc=$?
        printf '%s\n' "$_h0_ctl_rc" > "$H0_EVIDENCE/ctl.rc.tmp"
        mv "$H0_EVIDENCE/ctl.rc.tmp" "$H0_EVIDENCE/ctl.rc"
    ) > "$H0_EVIDENCE/live.stdout" 2> "$H0_EVIDENCE/live.stderr" &
    H0_CTL_JOB=$!
}

_stream_ready() {
    python3 "$PROBE" ready --evidence "$H0_EVIDENCE" --nonce "$H0_NONCE" --minimum 20
}

_capture_before() {
    d cp "$(ctr_name agt1):$H0_REMOTE/identity.json" "$H0_EVIDENCE/before/identity.json" || return 1
    d cp "$(ctr_name agt1):$H0_REMOTE/ledger.jsonl" "$H0_EVIDENCE/before/ledger.jsonl" || return 1
    cp "$H0_EVIDENCE/live.stdout" "$H0_EVIDENCE/before/stdout" || return 1
    cp "$H0_EVIDENCE/live.stderr" "$H0_EVIDENCE/before/stderr" || return 1
    "$SIM" ctl -- ps -a --json > "$H0_EVIDENCE/before/ps.json" || return 1
    python3 "$PROBE" baseline --evidence "$H0_EVIDENCE" --nonce "$H0_NONCE" --minimum 20
}

_capture_upgrade() {
    _h0_pid=$(_agent_pid) || return 1
    _h0_start=$(_agent_start) || return 1
    _h0_sha=$(_running_sha "$_h0_pid") || return 1
    dexec agt1 -- cat "$MARKER" > "$H0_EVIDENCE/marker.json" || return 1
    "$SIM" ctl -- node ls --json > "$H0_EVIDENCE/node-after.json" || return 1
    _h0_release=$(jq -er '[.nodes[]? | select(.nid == "agt1" and .status == "ONLINE") | .release_version] |
        if length == 1 then .[0] else error("agent target is absent/ambiguous") end' \
        "$H0_EVIDENCE/node-after.json") || return 1
    jq -n --arg old_sha "$H0_OLD_SHA" --arg new_sha "$H0_NEW_SHA" --arg actual_sha "$_h0_sha" \
        --arg artifact_sha "$ART_SHA" --argjson pid_before "$H0_PID_BEFORE" --argjson pid_after "$_h0_pid" \
        --arg exec_ts_before "$H0_START_BEFORE" --arg exec_ts_after "$_h0_start" \
        --argjson upgrade_rc "$H0_UPGRADE_RC" --arg target_release "$H0_NEXT_RELEASE" \
        --arg online_release "$_h0_release" --slurpfile marker "$H0_EVIDENCE/marker.json" \
        '{old_sha:$old_sha,new_sha:$new_sha,actual_sha:$actual_sha,artifact_sha:$artifact_sha,
          pid_before:$pid_before,pid_after:$pid_after,exec_ts_before:$exec_ts_before,
          exec_ts_after:$exec_ts_after,upgrade_rc:$upgrade_rc,target_release:$target_release,
          online_release:$online_release,marker:$marker[0]}' > "$H0_EVIDENCE/upgrade.json" || return 1
    python3 "$PROBE" upgrade-proof --evidence "$H0_EVIDENCE"
}

_capture_post() {
    dexec -u sim agt1 -- python3 "$H0_REMOTE_PROBE" snapshot --root "$H0_REMOTE" --nonce "$H0_NONCE" \
        > "$H0_EVIDENCE/post/snapshot.json" || return 1
    if [ ! -f "$H0_EVIDENCE/post/snapshot-start.json" ]; then
        cp "$H0_EVIDENCE/post/snapshot.json" "$H0_EVIDENCE/post/snapshot-start.json" || return 1
    fi
    "$SIM" ctl -- ps -a --json > "$H0_EVIDENCE/post/ps.json" || return 1
    python3 "$PROBE" post-ready --evidence "$H0_EVIDENCE" --nonce "$H0_NONCE" --minimum 20 || return 1
    jq -e --arg n "$H0_NONCE" '[.processes[]? | select(.argv | index($n))] |
        length==1 and .[0].status=="RUNNING"' "$H0_EVIDENCE/post/ps.json" >/dev/null
}

_producer_finished() { dexec agt1 -- test -s "$H0_REMOTE/final.json"; }
_ctl_finished() { test -s "$H0_EVIDENCE/ctl.rc"; }
_capture_final() {
    d cp "$(ctr_name agt1):$H0_REMOTE/ledger.jsonl" "$H0_EVIDENCE/final/ledger.jsonl" || return 1
    d cp "$(ctr_name agt1):$H0_REMOTE/final.json" "$H0_EVIDENCE/final/final.json" || return 1
    cp "$H0_EVIDENCE/live.stdout" "$H0_EVIDENCE/final/stdout" || return 1
    cp "$H0_EVIDENCE/live.stderr" "$H0_EVIDENCE/final/stderr" || return 1
    if _ctl_finished; then
        _h0_final_rc=$(cat "$H0_EVIDENCE/ctl.rc") || return 1
        jq -n --argjson rc "$_h0_final_rc" '{finished:true,exit_code:$rc}' \
            > "$H0_EVIDENCE/final/ctl-outcome.json"
    else
        printf '%s\n' '{"finished":false}' > "$H0_EVIDENCE/final/ctl-outcome.json"
    fi
}

_continuity() {
    python3 "$PROBE" continuity --evidence "$H0_EVIDENCE" --nonce "$H0_NONCE" --minimum 20
}

# origin: hot-update S3 plan §8.1. Both live counters must advance before a
# fault; a producer which was already stalled cannot earn continuity credit.
_snapshot() { dexec -u sim agt1 -- python3 "$H0_REMOTE_PROBE" snapshot --root "$H0_REMOTE" --nonce "$H0_NONCE"; }
_stream_advancing() {
    _sa_source=$(_snapshot | jq -er 'select(.alive and (.finished|not)) | .data_count') || return 1
    _sa_ctl=$(python3 "$PROBE" count --evidence "$H0_EVIDENCE" --nonce "$H0_NONCE") || return 1
    poll_until 10 1 "source and original ctl advance by 20" -- _advanced_by "$_sa_source" "$_sa_ctl"
}
_advanced_by() {
    _ab_source=$(_snapshot | jq -er 'select(.alive and (.finished|not)) | .data_count') || return 1
    _ab_ctl=$(python3 "$PROBE" count --evidence "$H0_EVIDENCE" --nonce "$H0_NONCE") || return 1
    [ "$_ab_source" -ge "$(($1 + 20))" ] || return 1
    printf '%s\n' "$_ab_ctl" | jq -e --argjson before "$2" \
        '.stdout >= ($before.stdout+20) and .stderr >= ($before.stderr+20)' >/dev/null
}
_agent_pid_nohup() {
    _ap_pid=$(dexec agt1 -- cat "$NOHUP_PIDFILE") || return 1
    # Exactly one decimal PID: empty, two lines and whitespace all fail closed.
    printf '%s\n' "$_ap_pid" | python3 -c 'import re,sys; s=sys.stdin.read(); sys.exit(0 if re.fullmatch(r"[1-9][0-9]*\n",s) and int(s)>1 else 1)' || return 1
    [ "$(dexec agt1 -- systemctl show tether-agent -p MainPID --value)" = 0 ] || return 1
    _ap_doc=$(dexec agt1 -- python3 "$H0_REMOTE_PROBE" identity --pid "$_ap_pid") || return 1
    printf '%s\n' "$_ap_doc" | jq -e --arg exe "$DST" --arg sid "$SID" \
        '.argv == [$exe,"agent","--session",$sid,"--nid","agt1"]' >/dev/null || return 1
    printf '%s\n' "$_ap_pid"
}
_nohup_original() {
    _no_pid=$(_agent_pid_nohup) || return 1
    [ "$(_running_sha "$_no_pid")" = "$(sha256sum "$HERE/vendor/tether" | awk '{print $1}')" ]
}
_user_unit() {
    # Render the product body directly. User=sim is the sole system-manager
    # adaptation; the exact Restart/ExecStart/logging body cannot drift.
    python3 - "$HERE/../../cmd/tether/agent.go" > "$H0_EVIDENCE/user-body.service" <<'PYUNIT'
import pathlib, re, sys
source = pathlib.Path(sys.argv[1]).read_text()
body = re.search(r'func agentUserUnitBody\([^\n]+\) string \{\s*return fmt.Sprintf\(`(.*?)`, sid, nid, exe, sid, nid, extraArgs, sid, envLines\)', source, re.S)
assert body, "agentUserUnitBody rendering contract changed"
print((body[1] % ('lab','agt1','/home/sim/.local/bin/tether','lab','agt1','','lab','Environment=HOME=/home/sim\n')).replace('[Service]\n','[Service]\nUser=sim\n'), end='')
PYUNIT
    [ "$?" = 0 ] || return 1
    sctl agt1 disable --now tether-agent || return 1
    d cp "$H0_EVIDENCE/user-body.service" "$(ctr_name agt1):/etc/systemd/system/tether-user-body.service" || return 1
    sctl agt1 daemon-reload || return 1
    sctl agt1 start tether-user-body
}
_nohup_start() {
    sctl agt1 disable --now tether-agent || return 1
    dexec -u sim agt1 -- env HOME=/home/sim sh -c \
        'setsid nohup /home/sim/.local/bin/tether agent --session "$1" --nid agt1 </dev/null >/dev/null 2>>"$3" & echo $! > "$2"' \
        _ "$SID" "$NOHUP_PIDFILE" "$(sim_agent_panic_sink_path "$SID")"
}
_record_observation() {
    _ro_pid=$(_agent_pid) || return 1
    dexec agt1 -- python3 "$H0_REMOTE_PROBE" identity --pid "$_ro_pid" > "$H0_EVIDENCE/identity.tmp" || return 1
    dexec agt1 -- python3 "$H0_REMOTE_PROBE" fdcensus --pid "$_ro_pid" > "$H0_EVIDENCE/fds-$1.json" || return 1
    _ro_main=$(dexec agt1 -- systemctl show "$AGENT_UNIT" -p MainPID --value) || return 1
    _ro_restarts=$(dexec agt1 -- systemctl show "$AGENT_UNIT" -p NRestarts --value) || return 1
    _ro_ts=$(_agent_start) || return 1
    # AF_UNIX spawn leases are independent of replay descriptors.
    dexec agt1 -- ss -xlp > "$H0_EVIDENCE/leases-$1.txt" || return 1
    _ro_leases=$(grep -c tether-spawnexec- "$H0_EVIDENCE/leases-$1.txt") || [ "$_ro_leases" = 0 ] || return 1
    jq --arg ts "$_ro_ts" --argjson main "$_ro_main" --argjson restarts "$_ro_restarts" --argjson leases "$_ro_leases" \
        '. + {exec_ts:$ts,mainpid:$main,nrestarts:$restarts,leases:$leases}' \
        "$H0_EVIDENCE/identity.tmp" > "$H0_EVIDENCE/agent-$1.json"
}
_upgrade() {
    "$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA" \
        > "$H0_EVIDENCE/upgrade-$1.stdout" 2> "$H0_EVIDENCE/upgrade-$1.stderr"
    H0_UPGRADE_RC=$?
    printf '%s\n' "$H0_UPGRADE_RC" > "$H0_EVIDENCE/upgrade-$1.rc"
}
_hop_complete() {
    _capture_upgrade || return 1
    cp "$H0_EVIDENCE/upgrade.json" "$H0_EVIDENCE/hop-$1.json" || return 1
    _record_observation "$1"
}
_upgrade_transcript() {
    grep -q 'staged' "$H0_EVIDENCE/upgrade-$1.stdout" &&
        grep -q "waiting.*${HOT_UPDATE_REGISTER_DEADLINE_S}s" "$H0_EVIDENCE/upgrade-$1.stdout" &&
        grep -q 'COMMITTED' "$H0_EVIDENCE/upgrade-$1.stdout" &&
        sim_broker_slog_grep brk1 'agent-reported upgrade outcome.*nid=agt1 .*upgrade_state=committed'
}
_final_ps_exited() {
    "$SIM" ctl -- ps -a --json > "$H0_EVIDENCE/final/ps.tmp" || return 1
    jq -e --arg n "$H0_NONCE" '[.processes[]? | select(.argv | index($n))] |
        length == 1 and .[0].status == "EXITED" and .[0].exit_code == 23' "$H0_EVIDENCE/final/ps.tmp" >/dev/null || return 1
    mv "$H0_EVIDENCE/final/ps.tmp" "$H0_EVIDENCE/final/ps.json"
}
_finish() {
    rm -f "$H0_EVIDENCE/post/snapshot-start.json"
    poll_until 20 1 "original producer advances after image switch" -- _capture_post || return 1
    dexec -u sim agt1 -- python3 "$H0_REMOTE_PROBE" stop --root "$H0_REMOTE" --nonce "$H0_NONCE" || return 1
    poll_until 15 1 "original producer exits 23" -- _producer_finished || return 1
    if [ "${ARM:-}" = W ]; then
        poll_until 10 1 "original ctl outcome (record only)" -- _ctl_finished || true
    else
        poll_until 30 1 "original ctl exit delivered" -- _ctl_finished || return 1
    fi
    _capture_final || return 1
    poll_until 20 1 "courier reports EXITED(23)" -- _final_ps_exited
}
_write_image_evidence() {
    sim_agent_slog_since agt1 "$H0_CURSOR" 100000 > "$H0_EVIDENCE/activations.log" || return 1
    python3 "$PROBE" log-audit --log "$H0_EVIDENCE/activations.log" --nonce "$H0_NONCE" \
        > "$H0_EVIDENCE/image-audit.json" || return 1
    python3 "$PROBE" freeze-window --log "$H0_EVIDENCE/activations.log" > "$H0_EVIDENCE/freeze-windows.json" || return 1
    jq -n --arg nonce "$H0_NONCE" --slurpfile audit "$H0_EVIDENCE/image-audit.json" \
        --slurpfile hops "$H0_EVIDENCE/hops.jsonl" --slurpfile agents "$H0_EVIDENCE/agents.jsonl" --slurpfile fds "$H0_EVIDENCE/fds.jsonl" \
        '{nonce:$nonce,hops:$hops,agents:$agents,fds:$fds,audit:$audit[0]}' > "$H0_EVIDENCE/hotupdate.json"
}
_hotupdate() {
    if [ "${ARM:-}" = W ]; then
        python3 "$PROBE" hotupdate --evidence "$H0_EVIDENCE" --nonce "$H0_NONCE" --minimum 20 --hops "$HOPS" --rollback
    else
        python3 "$PROBE" hotupdate --evidence "$H0_EVIDENCE" --nonce "$H0_NONCE" --minimum 20 --hops "$HOPS" --deferred "$DEFERRED"
    fi
}
_image_receipt() {
    cat "$H0_EVIDENCE"/hop-*.json > "$H0_EVIDENCE/hops.jsonl" || return 1
    cat "$H0_EVIDENCE"/agent-*.json > "$H0_EVIDENCE/agents.jsonl" || return 1
    cat "$H0_EVIDENCE"/fds-*.json > "$H0_EVIDENCE/fds.jsonl" || return 1
    _write_image_evidence && _hotupdate
}
_w_pending() {
    dexec agt1 -- cat "$MARKER" > "$H0_EVIDENCE/marker-pending.json" || return 1
    jq -e --argjson expected "$((H0_BOOT_GENERATION + 1))" '.state=="pending" and .generation==$expected and .resource_phase=="active" and .imported_resources==1' \
        "$H0_EVIDENCE/marker-pending.json" >/dev/null || return 1
    [ "$(_running_sha "$H0_PID_BEFORE")" = "$H0_NEW_SHA" ] || return 1
    _record_observation 1 || return 1
    jq -n --arg old "$H0_OLD_SHA" --arg new "$H0_NEW_SHA" --argjson pid "$H0_PID_BEFORE" \
        --slurpfile marker "$H0_EVIDENCE/marker-pending.json" \
        '{old_sha:$old,new_sha:$new,pid_before:$pid,pid_after:$pid,marker:$marker[0]}' > "$H0_EVIDENCE/hop-1.json"
}
_w_rolled_back() {
    _wr_m=$(dexec agt1 -- cat "$MARKER") || return 1
    printf '%s\n' "$_wr_m" | jq -e --argjson expected "$((H0_BOOT_GENERATION + 2))" '.state=="rolled_back" and (.detail|contains("register deadline exceeded"))
        and .resource_phase=="redispatched" and .generation==$expected' >/dev/null || return 1
    [ "$(dexec agt1 -- sha256sum "$DST" | awk '{print $1}')" = "$H0_OLD_SHA" ] || return 1
    dexec agt1 -- test ! -e "$PREV" || return 1
    [ "$(_agent_pid)" = "$H0_PID_BEFORE" ] && [ "$(_agent_start)" = "$H0_START_BEFORE" ] || return 1
    [ "$(_running_sha "$H0_PID_BEFORE")" = "$H0_OLD_SHA" ] || return 1
    printf '%s\n' "$_wr_m" > "$H0_EVIDENCE/marker-rollback.json"
}
_lost_row() {
    "$SIM" ctl -- ps -a --json > "$H0_EVIDENCE/lost-ps.tmp" || return 1
    jq -e --arg n "$H0_NONCE" '[.processes[]? | select(.argv | index($n))] |
        length==1 and .[0].status=="LOST"' "$H0_EVIDENCE/lost-ps.tmp" >/dev/null || return 1
    mv "$H0_EVIDENCE/lost-ps.tmp" "$H0_EVIDENCE/lost-ps.json"
}
_deferred_reason() {
    grep -q 'agent_rejected:upgrade_deferred' "$H0_EVIDENCE/deferred.out" &&
        grep -q 'remote_fs_probe_in_flight' "$H0_EVIDENCE/deferred.out" &&
        grep -qE 'spawnsafe: probes in flight=[1-9][0-9]*, retained=[1-9][0-9]*' "$H0_EVIDENCE/deferred.out" &&
        sim_agent_slog_grep agt1 'agent: upgrade deferred.*remote_fs_probe_in_flight' "$H0_CURSOR"
}
_deferred_left_disk_untouched() {
    [ "$(dexec agt1 -- sha256sum "$DST" | awk '{print $1}')" = "$H0_OLD_SHA" ] || return 1
    dexec agt1 -- test ! -e "$MARKER" || return 1
    [ "$(_agent_pid)" = "$H0_PID_BEFORE" ] || return 1
    [ "$(_running_sha "$H0_PID_BEFORE")" = "$H0_OLD_SHA" ] || return 1
    _stream_advancing
}
_deferred_upgrade() {
    "$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA" > "$H0_EVIDENCE/deferred.out" 2>&1
    _du_rc=$?
    cat "$H0_EVIDENCE/deferred.out"
    [ "$_du_rc" = 75 ] || return 0 # assert_refuses must reject the wrong exit class
    return 75
}
_hangfs_mounted() { "$SIM" exec agt1 -- grep -q fuse.hangfs /proc/mounts; }
_statfs_healthy() { "$SIM" exec agt1 -- timeout "$HOT_UPDATE_SAFE_POINT_S" stat -f "$MNT" >/dev/null 2>&1; }
_statfs_blocks() {
    "$SIM" exec agt1 -- timeout "$HOT_UPDATE_SAFE_POINT_S" stat -f "$MNT" >/dev/null 2>&1
    [ "$?" = 124 ]
}
_mount_hangfs() {
    "$SIM" exec agt1 -- sh -c "mkdir -p $MNT; grep -q fuse.hangfs /proc/mounts && exit 0; setsid python3 /opt/sim/hangfs.py $MNT >/tmp/hangfs.log 2>&1 &"
    poll_until 20 1 "fuse.hangfs mounted" -- _hangfs_mounted
}
_wedge() { "$SIM" exec agt1 -- pkill -STOP -f hangfs.py; }
_heal() { "$SIM" exec agt1 -- pkill -CONT -f hangfs.py; poll_until 10 1 "statfs healthy" -- _statfs_healthy; }
_probe_wedged() {
    grep -q 'remote_fs_unsafe_cwd' "$H0_EVIDENCE/wedge.out"
}
_mixed_judge() {
    python3 "$MIXED" judge --evidence "$H0_EVIDENCE/$1" --ledger "$H0_EVIDENCE/$1/ledger" --mode "$2" \
        ${3:+--window-start} ${3:-} ${4:+--window-end} ${4:-} > "$H0_EVIDENCE/$1/judge.json" || return 1
    cat "$H0_EVIDENCE/$1/judge.json"
    if [ "$2" = calibrate ]; then
        jq -e '.request_failures==0 and .connect_failures==0 and .rate>=(0.9*.configured_rate)' "$H0_EVIDENCE/$1/judge.json" >/dev/null
    else
        jq -e '.dup_side_effects==0 and .rc0_without_effect==0 and .request_failures==0 and
            .connect_failures==0 and .stale_offline_failures==0 and .in_window>=1 and
            .rate>=(0.9*.configured_rate) and .effect_without_rc0==0 and .unrequested_effects==0 and
            .successful.read>0 and .successful.write>0 and .successful.short>0 and .writes_in_window>=1 and
            .agent_upgrading<=.starts_in_window' "$H0_EVIDENCE/$1/judge.json" >/dev/null
    fi
}
_mixed_copy() {
    d cp "$(ctr_name ctl1):/tmp/mixed-$H0_NONCE-$1/." "$H0_EVIDENCE/$1/" || return 1
    d cp "$(ctr_name agt1):/tmp/hot-update-mixed-$2/ledger" "$H0_EVIDENCE/$1/ledger"
}

# origin: hot-update S3 plan §8.1; DR-5. The probe owns the monotonic clock.
# A log cursor excludes setup/previous-image events; neither mark is scheduled.
_mixed_started() {
    dexec -u sim ctl1 -- test -s "/tmp/mixed-$H0_NONCE-p1/started.json"
}
_mixed_mark_seen() {
    sim_agent_slog_grep agt1 "agent: image $1" "$H0_CURSOR" || return 1
    dexec -u sim ctl1 -- python3 "$MIXED_REMOTE/hot_update_mixed_probe.py" mark \
        --evidence "/tmp/mixed-$H0_NONCE-p1" --name "$1"
}
_mixed_switch_observed() {
    if [ "$P1_CUT_SEEN" = 0 ]; then
        _mixed_mark_seen cut || return 1
        P1_CUT_SEEN=1
    fi
    _mixed_mark_seen activated
}
_mixed_paths() {
    sim_agent_slog_since agt1 "$H0_CURSOR" 100000 > "$H0_EVIDENCE/p1/agent.slog" || return 1
    python3 "$PROBE" log-audit --log "$H0_EVIDENCE/p1/agent.slog" --nonce "$P1_NONCE" \
        > "$H0_EVIDENCE/p1/exec-paths.json" || return 1
    # Path selection is calibration data, never a reliable-only acceptance gate.
    cat "$H0_EVIDENCE/p1/exec-paths.json"
}

drill_begin "34-hot-update-process (S3 image continuity) [arm ${ARM:-?}]"
assert_setup "independent oracle positive, negative and nonempty controls" python3 "$PROBE" selftest
mkdir -p "${SIM_EVIDENCE_DIR:-/tmp}" || setup_fail "create evidence parent"
H0_EVIDENCE=$(mktemp -d "${SIM_EVIDENCE_DIR:-/tmp}/hot-update-process.XXXXXX") \
    || setup_fail "create private evidence directory"
H0_NONCE=$(python3 -c 'import secrets; print(secrets.token_hex(16))') || setup_fail "mint workload nonce"
H0_REMOTE="/tmp/tether-h0-work-$H0_NONCE"
H0_REMOTE_PROBE="/tmp/tether-h0-probe-$H0_NONCE.py"
H0_CTL_PIDFILE="/tmp/tether-h0-ctl-$H0_NONCE.pid"
mkdir -p "$H0_EVIDENCE/before" "$H0_EVIDENCE/post" "$H0_EVIDENCE/final" || setup_fail "create evidence phases"
printf '%s\n' "$H0_NONCE" > "$H0_EVIDENCE/nonce"
cp "$HERE/vendor/tether-source.json" "$H0_EVIDENCE/tether-source.json" || setup_fail "retain current source identity"
log "H0 evidence: $H0_EVIDENCE (nonce=$H0_NONCE; no application reconnect permitted)"

# Machine supply and every fixture-induced restart finish BEFORE the workload.
"$SIM" nuke >/dev/null 2>&1 || true
assert_setup "up isolated broker + agent + ctl" "$SIM" up --brokers 1 --agents 1 --ctl 1
assert_setup "init brk1 N=1" "$SIM" init brk1
assert_setup "operator broker artifact allowlist" upgradecfg_allow_broker brk1 _broker_ready
assert_setup "create session and activate real ctl" "$SIM" session "$SID" --pin "$PIN"
assert_setup "onboard real agent" "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
artifact_up "$INSTANCE" "$HERE/vendor/tether-next" tether-next.tar.gz || setup_fail "serve real next artifact"
assert_setup "artifact TLS trust supply" ingress_trust_inject agt1
# origin: hot-update S3 plan §8.1; DR-2. Like drill 62, boot with the
# healthy remote mount so Policy.Prepare does not take its local-only shortcut.
if [ "$ARM" = F ]; then
    assert_setup "F mount healthy hangfs before the final agent setup restart" _mount_hangfs
    assert_setup "F healthy statfs positive control before agent boot" _statfs_healthy
fi
assert_setup "operator agent allowlist and its required restart" upgradecfg_allow_agent agt1 _agent_ready
assert_setup "copy workload/oracle without changing product or unit" _install_probe


if [ "$ARM" = U ]; then
    assert_setup "U supply: product user-unit body under User=sim system manager" _user_unit
    AGENT_UNIT=tether-user-body
fi
if [ "$ARM" = N ]; then
    NOHUP_PIDFILE="/tmp/hup-agent-$H0_NONCE.pid"
    assert_setup "N supply: disable system unit and launch setsid nohup as sim" _nohup_start
    assert_setup "N nonce pidfile identifies one agent, vendor SHA, MainPID zero" poll_until 30 1 "nohup identity" -- _nohup_original
fi
assert_setup "agent ONLINE before workload" poll_until 45 1 "agent online" -- _agent_ready
H0_PID_BEFORE=$(_agent_pid) || setup_fail "read original agent PID"
H0_START_BEFORE=$(_agent_start) || setup_fail "read original agent start timestamp"
H0_OLD_SHA=$(_running_sha "$H0_PID_BEFORE") || setup_fail "hash original running agent"
# origin: hot-update S5 plan §7.1 / T48 / DRILL-RUN 34.A/G/W.
# Freeze the original running artifact's boot contract before any binary flip.
dexec agt1 -- "/proc/$H0_PID_BEFORE/exe" version > "$H0_EVIDENCE/initial-image.version" || setup_fail "read original image domains"
printf '%s\n' "$H0_OLD_SHA" > "$H0_EVIDENCE/initial-image.sha"
H0_BOOT_GENERATION=$(python3 "$PROBE" initial-generation --evidence "$H0_EVIDENCE" --running-sha "$H0_OLD_SHA") || setup_fail "bind initial owner generation"
H0_NEW_SHA=$(sha256sum "$HERE/vendor/tether-next" | awk '{print $1}') || setup_fail "hash candidate"
H0_NEXT_RELEASE=$("$HERE/vendor/tether-next" version | awk 'NR==1 {print $2}') || setup_fail "read candidate release"
assert_setup "candidate binary differs from running image" test "$H0_OLD_SHA" != "$H0_NEW_SHA"
H0_CURSOR=$(sim_agent_slog_cursor agt1)
if [ "$ARM" != P ]; then
    _launch_producer
    assert_setup "real ctl receives a nonempty ordered output baseline" poll_until 35 1 "producer output present" -- _stream_ready
    assert_setup "no-upgrade source/output and RUNNING task positive control" poll_until 15 1 "baseline agrees" -- _capture_before
    assert_setup "non-vacuity: source and original ctl advance by 20 before injection" _stream_advancing
    assert_setup "initial agent identity and descriptor census" _record_observation 0
fi
log "calibration budgets: freeze ${HOT_UPDATE_FREEZE_MS}ms, request ${HOT_UPDATE_REQUEST_S}s; samples only"
case "${ARM:?}" in
A)
    _upgrade 1
    assert_ok "A same PID and service start, new SHA and committed marker" poll_until 25 1 "image committed" -- _hop_complete 1
    assert_ok "A staged, waiting deadline, COMMITTED and broker outcome receipts" _upgrade_transcript 1
    assert_ok "A original child progresses and courier delivers EXITED(23)" _finish
    assert_ok "original process output and management survive real agent upgrade" _continuity
    assert_ok "A image generation, audit, FD ownership and final task agree" _image_receipt
    ;;
G)
    HOPS=2
    _upgrade 1
    assert_ok "G first hop committed with original PID and new SHA" poll_until 25 1 "first hop" -- _hop_complete 1
    assert_ok "G non-vacuity: original source and ctl advance before reverse hop" _stream_advancing
    artifact_add "$HERE/vendor/tether" tether-original.tar.gz || setup_fail "add reverse artifact to running backend"
    ART_URL=$ART2_URL; ART_SHA=$ART2_SHA
    H0_NEW_SHA=$H0_OLD_SHA
    H0_OLD_SHA=$(_running_sha "$H0_PID_BEFORE")
    H0_NEXT_RELEASE=$("$HERE/vendor/tether" version | awk 'NR==1 {print $2}')
    _upgrade 2
    assert_ok "G reverse hop committed with original PID and original SHA" poll_until 25 1 "reverse hop" -- _hop_complete 2
    assert_ok "G original child progresses and courier delivers EXITED(23)" _finish
    assert_ok "G two generations form A to B to A with one execution and stable FD ownership" _image_receipt
    ;;
U)
    _upgrade 1
    assert_ok "U body-equivalent system unit keeps PID, timestamp and commits" poll_until 25 1 "user body hop" -- _hop_complete 1
    assert_ok "U original child progresses and courier delivers EXITED(23)" _finish
    assert_ok "U original child and ctl continuity under the product unit body" _continuity
    assert_ok "U image marker, NRestarts and descriptor ownership remain sound" _image_receipt
    ;;
N)
    _upgrade 1
    assert_ok "N unsupervised same PID and startticks commits new image" poll_until 25 1 "nohup hop" -- _hop_complete 1
    assert_ok "N original child progresses and courier delivers EXITED(23)" _finish
    assert_ok "N original child and ctl continuity without a supervisor" _continuity
    assert_ok "N image marker, stable fd2 inode and descriptor ownership" _image_receipt
    ;;
P)
    MIXED="$HERE/drills/lib/hot_update_mixed_probe.py"
    # The probe reads its budgets from the hot_update_budget.sh that sits beside it, so both
    # travel together into one private directory; the agent-side ledger directory must exist
    # before the first write, or every write request fails for a reason that is not the product.
    MIXED_REMOTE="/tmp/mixed-tool-$H0_NONCE"
    assert_setup "P mixed probe directory in ctl container" dexec -u sim ctl1 -- mkdir -p "$MIXED_REMOTE"
    assert_setup "P mixed probe installed in ctl container" d cp "$MIXED" "$(ctr_name ctl1):$MIXED_REMOTE/hot_update_mixed_probe.py"
    assert_setup "P mixed probe budgets installed beside it" \
        d cp "$HERE/drills/lib/hot_update_budget.sh" "$(ctr_name ctl1):$MIXED_REMOTE/hot_update_budget.sh"
    dexec ctl1 -- chmod -R a+rX "$MIXED_REMOTE" >/dev/null 2>&1 || true
    mkdir -p "$H0_EVIDENCE/p0" "$H0_EVIDENCE/p1"
    assert_setup "P0 agent-side ledger directory" dexec -u sim agt1 -- mkdir -p "/tmp/hot-update-mixed-$H0_NONCE"
    assert_setup "P0 calibration runs at the required rate for 30 seconds" dexec -u sim ctl1 -- env HOME=/home/sim \
        python3 "$MIXED_REMOTE/hot_update_mixed_probe.py" run --evidence "/tmp/mixed-$H0_NONCE-p0" \
        --rate "$HOT_UPDATE_PROBE_RATE" --duration 30 --node agt1 --nonce "$H0_NONCE"
    assert_setup "P0 retain calibration samples and source ledger" _mixed_copy p0 "$H0_NONCE"
    assert_setup "P0 sensor has zero request/connect failures and at least nine requests per second" _mixed_judge p0 calibrate
    P1_NONCE=$(python3 -c 'import secrets; print(secrets.token_hex(16))')
    assert_setup "P1 agent-side ledger directory" dexec -u sim agt1 -- mkdir -p "/tmp/hot-update-mixed-$P1_NONCE"
    dexec -u sim ctl1 -- env HOME=/home/sim python3 "$MIXED_REMOTE/hot_update_mixed_probe.py" run \
        --evidence "/tmp/mixed-$H0_NONCE-p1" --rate "$HOT_UPDATE_PROBE_RATE" --node agt1 \
        --nonce "$P1_NONCE" --until-mark activated --tail 10 --max-duration 180 > "$H0_EVIDENCE/p1.run" 2>&1 &
    MIXED_JOB=$!
    assert_setup "P1 probe start receipt precedes the upgrade" poll_until 20 1 "mixed probe started" -- _mixed_started
    P1_CUT_SEEN=0
    _upgrade 1 & P1_UPGRADE_JOB=$!
    assert_ok "P1 observes cut and activation in the probe clock" poll_until_fixed 150 0.1 "image switch observed" -- _mixed_switch_observed
    wait "$P1_UPGRADE_JOB" || true
    H0_UPGRADE_RC=$(cat "$H0_EVIDENCE/upgrade-1.rc") || setup_fail "read mixed upgrade exit receipt"
    wait "$MIXED_JOB"; P1_RC=$?
    assert_ok "P1 mixed workload continued through observed activation and its tail" test "$P1_RC" = 0
    assert_ok "P1 upgrade committed" poll_until 25 1 "mixed image committed" -- _capture_upgrade
    assert_setup "P1 retain mixed samples and source ledger" _mixed_copy p1 "$P1_NONCE"
    assert_ok "P1 exercised read write and short work with a successful write across the observed cut and no unsafe refusals" _mixed_judge p1 upgrade cut activated
    assert_setup "P1 record reliable and legacy exec path counts for the probe nonce" _mixed_paths
    ;;
W)
    HOPS=2
    assert_setup "W inject new-connection SYN blackhole after producer is established" fault_synblock_on agt1 4222
    assert_setup "W fault self-proof: new NATS connections are blackholed" fault_assert_blackholed agt1 brk1 4222
    # The CLI can wait beyond rollback: capture active image while it is running.
    _upgrade 1 & W_UPGRADE_JOB=$!
    assert_ok "W candidate imports and thaws while unable to register" poll_until 45 1 "pending active image" -- _w_pending
    assert_ok "W watchdog reexports current state and restores old image under the same PID" \
        poll_until "$((HOT_UPDATE_REGISTER_DEADLINE_S + 90))" 2 "watchdog rollback" -- _w_rolled_back
    assert_ok "W broker saw original task LOST before reconnection" poll_until 20 1 "LOST task" -- _lost_row
    assert_ok "W rollback descriptor census" _record_observation 2
    jq -n --arg old "$H0_NEW_SHA" --arg new "$H0_OLD_SHA" --argjson pid "$H0_PID_BEFORE" \
        --slurpfile marker "$H0_EVIDENCE/marker-rollback.json" \
        '{old_sha:$old,new_sha:$new,pid_before:$pid,pid_after:$pid,marker:$marker[0],disk_sha:$new,prev_consumed:true}' \
        > "$H0_EVIDENCE/hop-2.json"
    assert_ok "W heal the new-connection blackhole" fault_synblock_off agt1
    assert_ok "W previous image re-registers" poll_until 60 1 "previous image ONLINE" -- _agent_ready
    wait "$W_UPGRADE_JOB" || true
    assert_ok "W original source progresses and previous courier reports EXITED(23); ctl outcome recorded" _finish
    assert_ok "W two activations, one execution, LOST to RUNNING, original pipes retained" _image_receipt
    ;;
F)
    assert_setup "F hangfs remains mounted before injection" _hangfs_mounted
    assert_setup "F healthy statfs positive control before injection" _statfs_healthy
    assert_setup "F wedge hangfs" _wedge
    assert_setup "F non-vacuity: statfs actually times out" _statfs_blocks
    "$SIM" ctl -- exec --timeout 20s --cwd "$MNT" agt1 -- true > "$H0_EVIDENCE/wedge.out" 2>&1 &
    WEDGE_JOB=$!
    assert_setup "F remote filesystem probe was exercised" poll_until 25 1 "remote fs refusal" -- _probe_wedged
    assert_refuses "F upgrade defers while a remote filesystem probe is in flight" "agent_rejected:upgrade_deferred" _deferred_upgrade
    assert_ok "F deferral carries remote_fs_probe_in_flight and a retained probe census on wire and in agent slog" _deferred_reason
    assert_ok "F deferral leaves disk and PID untouched while source and ctl advance" _deferred_left_disk_untouched
    dexec agt1 -- ps -L -p "$H0_PID_BEFORE" -o pid,tid,stat,wchan:32 > "$H0_EVIDENCE/wedged-threads.txt" 2>&1 || true
    assert_setup "F heal hangfs" _heal
    wait "$WEDGE_JOB" || true
    DEFERRED=1
    _upgrade 1
    assert_ok "F retry imports and commits the image" poll_until 25 1 "healed hop" -- _hop_complete 1
    assert_ok "F original child progresses and courier delivers EXITED(23)" _finish
    assert_ok "F original child and ctl continuity after deferred retry" _continuity
    assert_ok "F one generation after deferral with stable descriptor ownership" _image_receipt
    ;;
*) setup_fail "unknown arm '$ARM'" ;;
esac
_capture_logs
log "34 receipts retained: $H0_EVIDENCE"
drill_end
