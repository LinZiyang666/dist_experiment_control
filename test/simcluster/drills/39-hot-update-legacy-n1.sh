#!/bin/sh
# 39-hot-update-legacy-n1.sh — real previous-release target/exporter boundary.
# origin: hot-update S3 plan §8.1. Y is the negative control for drill 34.
# arms: X Y P F BR Q00B0-exec Q00B0-run Q00B0-kill Q00B0-expose Q00B0-transfer Q01B0-exec Q01B0-run Q01B0-kill Q01B0-expose Q01B0-transfer Q10B0-exec Q10B0-run Q10B0-kill Q10B0-expose Q10B0-transfer Q11B0-exec Q11B0-run Q11B0-kill Q11B0-expose Q11B0-transfer Q00B1-exec Q00B1-run Q00B1-kill Q00B1-expose Q00B1-transfer Q01B1-exec Q01B1-run Q01B1-kill Q01B1-expose Q01B1-transfer Q10B1-exec Q10B1-run Q10B1-kill Q10B1-expose Q10B1-transfer Q11B1-exec Q11B1-run Q11B1-kill Q11B1-expose Q11B1-transfer LB-new LB-old LI-defer LI-interrupting M-route CC-idle CC-late-create CL-reject ABI-adjacent ABI-too-far ABI-unknown GO-hash CFG-nats-old CFG-nats-new CFG-caddy-old CFG-caddy-new NI CI
# fixture: X=N1 Y=N1 P=N1 F=N1 BR=N3-live Q00B0-exec=N1 Q00B0-run=N1 Q00B0-kill=N1 Q00B0-expose=N1 Q00B0-transfer=N1 Q01B0-exec=N1 Q01B0-run=N1 Q01B0-kill=N1 Q01B0-expose=N1 Q01B0-transfer=N1 Q10B0-exec=N1 Q10B0-run=N1 Q10B0-kill=N1 Q10B0-expose=N1 Q10B0-transfer=N1 Q11B0-exec=N1 Q11B0-run=N1 Q11B0-kill=N1 Q11B0-expose=N1 Q11B0-transfer=N1 Q00B1-exec=N1 Q00B1-run=N1 Q00B1-kill=N1 Q00B1-expose=N1 Q00B1-transfer=N1 Q01B1-exec=N1 Q01B1-run=N1 Q01B1-kill=N1 Q01B1-expose=N1 Q01B1-transfer=N1 Q10B1-exec=N1 Q10B1-run=N1 Q10B1-kill=N1 Q10B1-expose=N1 Q10B1-transfer=N1 Q11B1-exec=N1 Q11B1-run=N1 Q11B1-kill=N1 Q11B1-expose=N1 Q11B1-transfer=N1 LB-new=N1 LB-old=N1 LI-defer=N1 LI-interrupting=N1 M-route=N3-live CC-idle=N1 CC-late-create=N1 CL-reject=N1 ABI-adjacent=N1 ABI-too-far=N1 ABI-unknown=N1 GO-hash=N1 CFG-nats-old=install CFG-nats-new=install CFG-caddy-old=install CFG-caddy-new=install NI=N1 CI=N1
# grows: X=0 Y=0 P=0 F=0 BR=2 Q00B0-exec=0 Q00B0-run=0 Q00B0-kill=0 Q00B0-expose=0 Q00B0-transfer=0 Q01B0-exec=0 Q01B0-run=0 Q01B0-kill=0 Q01B0-expose=0 Q01B0-transfer=0 Q10B0-exec=0 Q10B0-run=0 Q10B0-kill=0 Q10B0-expose=0 Q10B0-transfer=0 Q11B0-exec=0 Q11B0-run=0 Q11B0-kill=0 Q11B0-expose=0 Q11B0-transfer=0 Q00B1-exec=0 Q00B1-run=0 Q00B1-kill=0 Q00B1-expose=0 Q00B1-transfer=0 Q01B1-exec=0 Q01B1-run=0 Q01B1-kill=0 Q01B1-expose=0 Q01B1-transfer=0 Q10B1-exec=0 Q10B1-run=0 Q10B1-kill=0 Q10B1-expose=0 Q10B1-transfer=0 Q11B1-exec=0 Q11B1-run=0 Q11B1-kill=0 Q11B1-expose=0 Q11B1-transfer=0 LB-new=0 LB-old=0 LI-defer=0 LI-interrupting=0 M-route=2 CC-idle=0 CC-late-create=0 CL-reject=0 ABI-adjacent=0 ABI-too-far=0 ABI-unknown=0 GO-hash=0 CFG-nats-old=0 CFG-nats-new=0 CFG-caddy-old=0 CFG-caddy-new=0 NI=0 CI=0
# worst: X=300 Y=420 P=300 F=420 BR=300 Q00B0-exec=60 Q00B0-run=90 Q00B0-kill=90 Q00B0-expose=90 Q00B0-transfer=120 Q01B0-exec=60 Q01B0-run=90 Q01B0-kill=90 Q01B0-expose=90 Q01B0-transfer=120 Q10B0-exec=60 Q10B0-run=90 Q10B0-kill=90 Q10B0-expose=90 Q10B0-transfer=120 Q11B0-exec=60 Q11B0-run=90 Q11B0-kill=90 Q11B0-expose=90 Q11B0-transfer=120 Q00B1-exec=60 Q00B1-run=90 Q00B1-kill=90 Q00B1-expose=90 Q00B1-transfer=120 Q01B1-exec=60 Q01B1-run=90 Q01B1-kill=90 Q01B1-expose=90 Q01B1-transfer=120 Q10B1-exec=60 Q10B1-run=90 Q10B1-kill=90 Q10B1-expose=90 Q10B1-transfer=120 Q11B1-exec=60 Q11B1-run=90 Q11B1-kill=90 Q11B1-expose=90 Q11B1-transfer=120 LB-new=120 LB-old=120 LI-defer=120 LI-interrupting=120 M-route=240 CC-idle=240 CC-late-create=240 CL-reject=120 ABI-adjacent=240 ABI-too-far=120 ABI-unknown=120 GO-hash=120 CFG-nats-old=60 CFG-nats-new=60 CFG-caddy-old=60 CFG-caddy-new=60 NI=300 CI=300
# forgoes: X=- Y=- P=- F=- BR=- Q00B0-exec=- Q00B0-run=- Q00B0-kill=- Q00B0-expose=- Q00B0-transfer=- Q01B0-exec=- Q01B0-run=- Q01B0-kill=- Q01B0-expose=- Q01B0-transfer=- Q10B0-exec=- Q10B0-run=- Q10B0-kill=- Q10B0-expose=- Q10B0-transfer=- Q11B0-exec=- Q11B0-run=- Q11B0-kill=- Q11B0-expose=- Q11B0-transfer=- Q00B1-exec=- Q00B1-run=- Q00B1-kill=- Q00B1-expose=- Q00B1-transfer=- Q01B1-exec=- Q01B1-run=- Q01B1-kill=- Q01B1-expose=- Q01B1-transfer=- Q10B1-exec=- Q10B1-run=- Q10B1-kill=- Q10B1-expose=- Q10B1-transfer=- Q11B1-exec=- Q11B1-run=- Q11B1-kill=- Q11B1-expose=- Q11B1-transfer=- LB-new=- LB-old=- LI-defer=- LI-interrupting=- M-route=- CC-idle=- CC-late-create=- CL-reject=- ABI-adjacent=- ABI-too-far=- ABI-unknown=- GO-hash=- CFG-nats-old=- CFG-nats-new=- CFG-caddy-old=- CFG-caddy-new=- NI=- CI=-
# No fake release: source.json, sha, tag and the absent image ABI line must
# identify vendor/tether-prev. Y's timeout signature is predicted, NOT_RUN.
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
# origin: hot-update S9 plan §9.5 / L22. Supervise the complete drill shell.
INFRA_LIBRARY_ONLY=1
. "$HERE/drills/85-hot-update-nats.sh"
unset INFRA_LIBRARY_ONLY
infra_drill_supervise "$@"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/ingress.sh"
. "$HERE/drills/lib/artifact.sh"
. "$HERE/drills/lib/upgradecfg.sh"
. "$HERE/drills/lib/logs.sh"
. "$HERE/drills/lib/hot_update_pty.sh"
SIM_REPO="$(cd "$HERE/../.." && pwd)"
SIM_VENDOR="$HERE/vendor"
. "$HERE/lib/stage.sh"
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
PREV_BIN=/usr/local/bin/tether-prev
CTL_TIMEOUT=360s

# origin: hot-update S8 plan §9.3 / P28. Flat rows keep refusal, ordinary
# wire and non-hot migration out of the hot-success count. Seconds are budgets,
# not measured costs. P32 appends matching expected/cost rows after integration.
upgrade_39_cases() {
    cat <<'CASES39'
X	refusal	0	64	N1	0	300
Y	historical	3	0	N1	0	420
P	historical	3	0	N1	0	300
F	historical	3	0	N1	0	420
BR	historical	3	0	N3-live	2	300
Q00B0-exec	wire	0	0	N1	0	60
Q00B0-run	wire	0	0	N1	0	90
Q00B0-kill	wire	0	0	N1	0	90
Q00B0-expose	wire	0	0,0	N1	0	90
Q00B0-transfer	wire	0	0,0	N1	0	120
Q01B0-exec	wire	0	0	N1	0	60
Q01B0-run	wire	0	0	N1	0	90
Q01B0-kill	wire	0	0	N1	0	90
Q01B0-expose	wire	0	0,0	N1	0	90
Q01B0-transfer	wire	0	0,0	N1	0	120
Q10B0-exec	wire	0	0	N1	0	60
Q10B0-run	wire	0	0	N1	0	90
Q10B0-kill	wire	0	0	N1	0	90
Q10B0-expose	wire	0	0,0	N1	0	90
Q10B0-transfer	wire	0	0,0	N1	0	120
Q11B0-exec	wire	0	0	N1	0	60
Q11B0-run	wire	0	0	N1	0	90
Q11B0-kill	wire	0	0	N1	0	90
Q11B0-expose	wire	0	0,0	N1	0	90
Q11B0-transfer	wire	0	0,0	N1	0	120
Q00B1-exec	wire	0	0	N1	0	60
Q00B1-run	wire	0	0	N1	0	90
Q00B1-kill	wire	0	0	N1	0	90
Q00B1-expose	wire	0	0,0	N1	0	90
Q00B1-transfer	wire	0	0,0	N1	0	120
Q01B1-exec	wire	0	0	N1	0	60
Q01B1-run	wire	0	0	N1	0	90
Q01B1-kill	wire	0	0	N1	0	90
Q01B1-expose	wire	0	0,0	N1	0	90
Q01B1-transfer	wire	0	0,0	N1	0	120
Q10B1-exec	wire	0	0	N1	0	60
Q10B1-run	wire	0	0	N1	0	90
Q10B1-kill	wire	0	0	N1	0	90
Q10B1-expose	wire	0	0,0	N1	0	90
Q10B1-transfer	wire	0	0,0	N1	0	120
Q11B1-exec	wire	0	0	N1	0	60
Q11B1-run	wire	0	0	N1	0	90
Q11B1-kill	wire	0	0	N1	0	90
Q11B1-expose	wire	0	0,0	N1	0	90
Q11B1-transfer	wire	0	0,0	N1	0	120
LB-new	refusal	0	64	N1	0	120
LB-old	refusal	0	64	N1	0	120
LI-defer	refusal	0	75	N1	0	120
LI-interrupting	non_hot	4	74	N1	0	120
M-route	refusal	0	75	N3-live	2	240
CC-idle	hot	0	0	N1	0	240
CC-late-create	hot	0	0,75	N1	0	240
CL-reject	refusal	0	64	N1	0	120
ABI-adjacent	hot	0	0	N1	0	240
ABI-too-far	refusal	0	64	N1	0	120
ABI-unknown	refusal	0	64	N1	0	120
GO-hash	decoder	0	0	N1	0	120
CFG-nats-old	config	0	0	install	0	60
CFG-nats-new	config	0	0	install	0	60
CFG-caddy-old	config	0	0	install	0	60
CFG-caddy-new	config	0	0	install	0	60
NI	refusal	0	64	N1	0	300
CI	refusal	0	64	N1	0	300
CASES39
}

# origin: hot-update S4 plan §9.3. No predicted rc is a product receipt.
# Pinned by the lead on 2026-10-03 after reviewing the first real old-exporter
# run (image ab9568ac, s4-evidence/drill-run4/evidence/hot-update-pty.P.*):
#   HOT_UPDATE_LEGACY_PTY_EXPORTER output=interrupted hup=0 management_terminal=0 ctl=-9 pid=1397
#   nonce=c53008f2… reason=bytes_input,bytes_output,…,early_eof_foreground,…,courier_terminal,…
# The v0.6.0 exporter bare-execs, loses the PTY's master/wait ownership, the
# foreground reads early EOF and no terminal ever reaches the original ctl
# (killed by the fixture after its measured 300 s timeout = ctl=-9). The pin
# keeps exactly that: interrupted output, no management terminal, a ctl that
# never ended on its own, lost output bytes and no courier terminal. pid/nonce
# are per run; HUP-vs-EOF order and zombie timing were seen to vary, so they
# are not part of the pin (they stay in the retained evidence).
PTY_LEGACY_SIGNATURE='HOT_UPDATE_LEGACY_PTY_EXPORTER output=interrupted hup=[01] management_terminal=0 ctl=-9 pid=[1-9][0-9]* nonce=[0-9a-f]{32} reason=([a-z_]+,)*bytes_output,([a-z_]+,)*courier_terminal(,[a-z_]+)*'

# origin: hot-update S4 plan §9.3 / 39.P. Invalid observation rc2 cannot be
# signature-matched by assert_bug, even when a diagnostic contains similar text.
_pty_legacy_exporter() {
    upgrade_legacy_exec_required "$PTY_EV" agent || return 2;
    # A failed/fixed recollection must not retain an earlier product signature.
    rm -f "$PTY_EV/legacy_exporter.rc" "$PTY_EV/legacy-exporter.out" "$PTY_EV/legacy-exporter.json" || return 2
    _pty_continuity > "$PTY_EV/continuity.out" 2>&1
    _ple_rc=$?
    printf '%s\n' "$_ple_rc" > "$PTY_EV/continuity_oracle.rc"
    case "$_ple_rc" in 0) return 0 ;; 1) ;; *) tail -n 1 "$PTY_EV/continuity.out" | cut -c 1-1000 | sed 's/^/PTY invalid continuity: /'; printf '%s\n' 'PTY continuity evidence invalid'; return 2 ;; esac
    python3 "$PROBE" legacy-exporter --evidence "$PTY_EV" --nonce "$PTY_NONCE" \
        > "$PTY_EV/legacy-exporter.json" 2> "$PTY_EV/legacy-exporter.out"
    _ple_legacy_rc=$?
    printf '%s\n' "$_ple_legacy_rc" > "$PTY_EV/legacy_exporter.rc"
    if [ "$PTY_LEGACY_SIGNATURE" = UNPINNED ]; then
        cat "$PTY_EV/legacy-exporter.out"
        printf '%s\n' 'PTY legacy exporter first-run signature UNPINNED; retain legacy-exporter.out and pin after review'
        return 2
    fi
    case "$_ple_rc:$_ple_legacy_rc" in 0:0) return 0 ;; 1:1) ;; *) sed 's/^/PTY invalid observation: /' "$PTY_EV/legacy-exporter.out"; printf '%s\n' 'PTY legacy signature evidence invalid'; return 2 ;; esac
    [ "$(wc -l < "$PTY_EV/legacy-exporter.out")" = 1 ] || { printf '%s\n' 'PTY legacy observation is not a single signature'; return 2; }
    grep -xE "$PTY_LEGACY_SIGNATURE" "$PTY_EV/legacy-exporter.out" || { cat "$PTY_EV/legacy-exporter.out"; printf '%s\n' 'PTY legacy signature differs from reviewed pin'; return 2; }
    return 1
}
_pty_legacy_assertion() {
    upgrade_legacy_assert "$PTY_EV" agent "original PTY survives a real legacy exporter upgrade" \
        "hot-update-plan H1 legacy PTY exporter; S8 legacy-to-capable first migration" \
        "^${PTY_LEGACY_SIGNATURE}$" _pty_legacy_exporter
}


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
    artifact_down >/dev/null 2>&1 || true
    true
}
# origin: hot-update S6 plan §10.2 / T58. BR installs its own custody trap
# only after validating its supplier/pin; an early refusal has no artifacts.
case "${ARM:-}" in X|Y) drill_install_traps _cleanup ;; esac

_broker_ready() {
    "$SIM" exec brk1 -- tether cluster status --json 2>/dev/null \
        | jq -e '.leader_id == "brk1"' >/dev/null
}
_agent_ready() {
    "$SIM" ctl -- node ls --json 2>/dev/null \
        | jq -e '[.nodes[]? | select(.nid == "agt1" and .status == "ONLINE")] | length == 1' >/dev/null
}
_agent_pid() { dexec agt1 -- systemctl show tether-agent -p MainPID --value; }
_agent_start() { dexec agt1 -- systemctl show tether-agent -p ExecMainStartTimestamp --value; }
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
        dexec -u sim ctl1 -- timeout --signal=TERM --kill-after=3s 370s env HOME=/home/sim sh -c \
            'printf "%s\n" "$$" > "$1"; shift; exec "$@"' \
            _ "$H0_CTL_PIDFILE" "$H0_EXEC_BIN" exec --timeout "$CTL_TIMEOUT" agt1 -- \
            python3 -u "$H0_REMOTE_PROBE" producer --root "$H0_REMOTE" \
            --nonce "$H0_NONCE" --period 0.05 --lifetime 420
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
    python3 "$PROBE" post-ready --evidence "$H0_EVIDENCE" --nonce "$H0_NONCE" --minimum 20
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

_final_ps_exited() {
    "$SIM" ctl -- ps -a --json > "$H0_EVIDENCE/final/ps.tmp" || return 1
    jq -e --arg n "$H0_NONCE" '[.processes[]? | select(.argv | index($n))] |
        length == 1 and .[0].status == "EXITED" and .[0].exit_code == 23' "$H0_EVIDENCE/final/ps.tmp" >/dev/null || return 1
    mv "$H0_EVIDENCE/final/ps.tmp" "$H0_EVIDENCE/final/ps.json"
}


_prev_die_gate() {
    sim_prev_source_ok "$PREV_TAG" || return 1
    [ "$PREV_TAG" = v0.6.0 ] || return 1
    _pdg_want=$(sha256sum "$HERE/vendor/tether-prev" | awk '{print $1}')
    _pdg_today=$(sha256sum "$HERE/vendor/tether" | awk '{print $1}')
    [ "$_pdg_want" != "$_pdg_today" ] || return 1
    d cp "$HERE/vendor/tether-prev" "$(ctr_name ctl1):$PREV_BIN" || return 1
    dexec ctl1 -- chmod 0755 "$PREV_BIN" || return 1
    _pdg_got=$(dexec ctl1 -- sha256sum "$PREV_BIN" | awk '{print $1}')
    [ "$_pdg_got" = "$_pdg_want" ] || return 1
    _pdg_version=$(dexec ctl1 -- "$PREV_BIN" version) || return 1
    [ "$(printf '%s\n' "$_pdg_version" | awk 'NR==1 {print $2}')" = "$PREV_TAG" ] || return 1
    [ "$(printf '%s\n' "$_pdg_version" | wc -l)" = 3 ] || return 1
    ! printf '%s\n' "$_pdg_version" | grep -q 'image-abi'
}
_install_prev() {
    sctl agt1 stop tether-agent || return 1
    d cp "$HERE/vendor/tether-prev" "$(ctr_name agt1):$DST" || return 1
    dexec agt1 -- sh -c "chown sim:sim '$DST' && chmod 0755 '$DST'" || return 1
    sctl agt1 start tether-agent
}
_prev_online() {
    "$SIM" ctl -- node ls --json | jq -e --arg version "$PREV_TAG" \
        '[.nodes[]? | select(.nid=="agt1" and .status=="ONLINE" and .release_version==$version)] | length==1' >/dev/null || return 1
    [ "$(_running_sha "$(_agent_pid)")" = "$(sha256sum "$HERE/vendor/tether-prev" | awk '{print $1}')" ]
}
_abi_refused() {
    "$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA" > "$H0_EVIDENCE/refused.out" 2>&1
    _ar_rc=$?
    cat "$H0_EVIDENCE/refused.out"
    [ "$_ar_rc" = 64 ] || return 0
    return 64
}
_unchanged() {
    [ "$(dexec agt1 -- sha256sum "$DST" | awk '{print $1}')" = "$H0_OLD_SHA" ] || return 1
    dexec agt1 -- test ! -e "$MARKER" || return 1
    [ "$(_agent_pid)" = "$H0_PID_BEFORE" ] && [ "$(_agent_start)" = "$H0_START_BEFORE" ] || return 1
    [ "$(_running_sha "$H0_PID_BEFORE")" = "$H0_OLD_SHA" ] || return 1
    _stream_advancing
}
_fd_nonempty() {
    dexec agt1 -- python3 "$H0_REMOTE_PROBE" fdcensus --pid "$H0_PID_BEFORE" > "$H0_EVIDENCE/fds-before.json" || return 1
    jq -e '.counts.pipe>=2 and .counts.handoff==0' "$H0_EVIDENCE/fds-before.json" >/dev/null
}
_refusal_continuity() { python3 "$PROBE" refused --evidence "$H0_EVIDENCE" --nonce "$H0_NONCE" --minimum 20; }
_legacy_exporter() { upgrade_legacy_exec_required "$H0_EVIDENCE" agent || return 2; python3 "$PROBE" legacy-exporter --evidence "$H0_EVIDENCE" --nonce "$H0_NONCE" --minimum 20; }
_legacy_audit() {
    _la_exec=$(sim_agent_slog_count agt1 "agent: exec.*$H0_NONCE" "$H0_CURSOR")
    _la_reliable=$(sim_agent_slog_count agt1 "agent: exec.*$H0_NONCE.*reliable=true" "$H0_CURSOR")
    _la_probe=$(sim_agent_slog_count agt1 'unknown forwarded verb.*process-resume' "$H0_CURSOR")
    for _la_n in "$_la_exec" "$_la_reliable" "$_la_probe"; do
        case "$_la_n" in ''|*[!0-9]*) return 1 ;; esac
    done
    jq -n --argjson ex "$_la_exec" --argjson reliable "$_la_reliable" --argjson probe "$_la_probe" \
        '{exec_lines:$ex,reliable_lines:$reliable,probe_lines:$probe}' > "$H0_EVIDENCE/legacy-audit.json"
}
_stop_and_capture() {
    dexec -u sim agt1 -- python3 "$H0_REMOTE_PROBE" stop --root "$H0_REMOTE" --nonce "$H0_NONCE" || return 1
    poll_until 15 1 "producer final record" -- _producer_finished || return 1
    poll_until 75 1 "original ctl exit or legacy timeout" -- _ctl_finished || return 1
    _capture_final
}

_h0_prepare() {
    drill_begin "39-hot-update-legacy-n1 [arm ${ARM:-?}]"
    assert_setup "independent continuity and legacy exporter controls" python3 "$PROBE" selftest
    mkdir -p "${SIM_EVIDENCE_DIR:-/tmp}" || setup_fail "create evidence parent"
    H0_EVIDENCE=$(mktemp -d "${SIM_EVIDENCE_DIR:-/tmp}/hot-update-legacy-n1.${ARM:-x}.XXXXXX") || setup_fail "create evidence"
    H0_NONCE=$(python3 -c 'import secrets; print(secrets.token_hex(16))')
    H0_REMOTE="/tmp/tether-39-work-$H0_NONCE"
    H0_REMOTE_PROBE="/tmp/tether-39-probe-$H0_NONCE.py"
    H0_CTL_PIDFILE="/tmp/tether-39-ctl-$H0_NONCE.pid"
    mkdir -p "$H0_EVIDENCE/before" "$H0_EVIDENCE/post" "$H0_EVIDENCE/final"
    printf '%s\n' "$H0_NONCE" > "$H0_EVIDENCE/nonce"
    "$SIM" nuke >/dev/null 2>&1 || true
    assert_setup "up isolated N1 broker agent and ctl" "$SIM" up --brokers 1 --agents 1 --ctl 1
    assert_setup "init brk1 N1" "$SIM" init brk1
    assert_setup "broker artifact allowlist" upgradecfg_allow_broker brk1 _broker_ready
    if [ "$ARM" = Y ]; then
        PREV_TAG=$(sim_fixed_prev_tag) || setup_fail "resolve fixed historical release" # origin: S8 internal review rv-n1 F6
        assert_setup "fixed old source before control-stack installation" sim_prev_source_ok "$PREV_TAG"
        # origin: S8 internal review s8-hermetic F9. Select the ctl path in
        # the parent; assert_setup captures only remote installation status.
        LEGACY_CTL_BIN=$PREV_BIN
        assert_setup "old broker and explicit old ctl before business" upgrade_legacy_stack "$HERE/vendor/tether-prev" "$HERE/vendor/tether-prev-source.json" "$PREV_BIN" "$H0_EVIDENCE"
        assert_setup "old broker ready" poll_until 60 1 "old broker" -- _broker_ready
    fi
    assert_setup "create session and activate ctl" "$SIM" session "$SID" --pin "$PIN"
    assert_setup "onboard agent" "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
    PREV_TAG=$(sim_fixed_prev_tag) || setup_fail "resolve fixed historical release" # origin: S8 internal review rv-n1 F6
    assert_setup "previous release provenance, distinct sha, tag and absent ABI line" _prev_die_gate
    cp "$HERE/vendor/tether-prev-source.json" "$H0_EVIDENCE/tether-prev-source.json" || setup_fail "retain previous source identity"
    cp "$HERE/vendor/tether-source.json" "$H0_EVIDENCE/tether-source.json" || setup_fail "retain current source identity"
    if [ "$ARM" = X ]; then CANDIDATE="$HERE/vendor/tether-prev"; else CANDIDATE="$HERE/vendor/tether-next"; fi
    artifact_up "$INSTANCE" "$CANDIDATE" candidate.tar.gz || setup_fail "serve real release artifact"
    assert_setup "artifact TLS trust" ingress_trust_inject agt1
    assert_setup "agent artifact allowlist and setup restart" upgradecfg_allow_agent agt1 _agent_ready
    assert_setup "install producer and independent oracle" _install_probe
    if [ "$ARM" = Y ]; then
        assert_setup "Y supply: run real previous release agent" _install_prev
        assert_setup "Y old agent ONLINE at the previous release" poll_until 60 1 "old agent online" -- _prev_online
        CTL_TIMEOUT=60s
    fi
    H0_PID_BEFORE=$(_agent_pid)
    H0_START_BEFORE=$(_agent_start)
    H0_OLD_SHA=$(_running_sha "$H0_PID_BEFORE")
    H0_NEW_SHA=$(sha256sum "$CANDIDATE" | awk '{print $1}')
    H0_NEXT_RELEASE=$("$CANDIDATE" version | awk 'NR==1 {print $2}')
    H0_CURSOR=$(sim_agent_slog_cursor agt1)
    H0_EXEC_BIN=/usr/local/bin/tether
    if [ "$ARM" = Y ]; then
        H0_EXEC_BIN=$LEGACY_CTL_BIN
        assert_setup "all historical roles before original business" upgrade_legacy_snapshot before "$H0_EVIDENCE"
        printf '%s\n' "$H0_NEW_SHA" > "$H0_EVIDENCE/legacy-target.sha"
    fi
    _launch_producer
    assert_setup "nonempty original ctl stdout and stderr baseline" poll_until 35 1 "stream baseline" -- _stream_ready
    assert_setup "original source agrees with RUNNING task" poll_until 15 1 "source baseline" -- _capture_before
    assert_setup "non-vacuity: original source and ctl advance by 20 before injection" _stream_advancing
    assert_setup "non-vacuity: running agent owns nonempty pipe descriptors" _fd_nonempty
}

# origin: hot-update S8 plan §9.3 / P28. Each explicit case below keeps
# its own launch unit, budget and native result in the existing parent join.
_upgrade_39_new_arm() {
    . "$HERE/drills/lib/hot_update_broker.sh"
    drill_begin "39-hot-update-legacy-n1 [arm $ARM]"
    upgrade_39_compat "$ARM"
    _u39_rc=$?
    broker_native "$_u39_rc"
}

# origin: hot-update S9 plan §7.1 / §9.3 / T80. These are safe refusal
# controls, not the reviewed historical loss signatures of P/F/BR above.
_infra_legacy_refusal() {
    [ "$#" = 1 ] || setup_fail 'legacy infra control needs exactly one component'
    infra_drill_load
    # Historical refusal controls bind the FIXED v0.6.0 supply (S8 FIX-CONS); a bare call
    # aborted under `set -u` before any verdict. origin: S7 r9d non-vacuity review (39.NI/CI)
    sim_prev_source_ok "$(sim_fixed_prev_tag)" || setup_fail 'NI/CI require the genuine historical tether-prev source and binary receipt'
    INFRA_CTL_BINARY=/opt/sim/tether-next
    infra_drill_prepare "$1" single legacy-capability
    infra_drill_check 'target executes genuine tether-prev and explicitly lacks infra capability' legacy-source || return 1
    infra_drill_target || setup_fail 'missing adjacent infra target'
    infra_drill_step infra_drill_command --to-version "$INFRA_VERSION" --expect-sha256 "$INFRA_SHA"
    INFRA_COMMAND_RC=$INFRA_STEP_RC; INFRA_COMMAND_LOG=$INFRA_STEP_LOG
    assert_ok 'historical capability refusal uses explicit unsupported CLI exit 64' test "$INFRA_COMMAND_RC" -eq 64
    infra_drill_check 'old target refuses infra before effect without broker reexec or business loss' legacy-refusal \
        --command-rc "$INFRA_COMMAND_RC" --command-log "$INFRA_COMMAND_LOG" || return 1
    infra_drill_finish
}

# origin: hot-update S4 plan §9.3 / P15. P imports the same fixture and
# continuity oracle as 36.A; post-fault death remains product evidence.
case "${ARM:?}" in
BR)
    # origin: hot-update S6 plan §10.2 / T58. Dispatch independently BEFORE
    # the common N1 setup. A legacy sole-voter leader refusal is not this fault.
    BROKER_LIBRARY_ONLY=1
    . "$HERE/drills/37-hot-update-broker.sh"
    drill_begin "39-hot-update-legacy-n1 [arm BR]"
    broker_legacy_arm grow_to_3
    ;;
F)
    # origin: hot-update S5 plan §9.3 / T48. Dispatch before common exec-only
    # setup: the flow negative must start with the actual historical exporter.
    FLOW_LIBRARY_ONLY=1
    . "$HERE/drills/75-hot-update-flows.sh"
    drill_begin '39-hot-update-legacy-n1 [arm F]'
    _flow_legacy_arm "$PREV_BIN" old-old-old
    assert_setup "F historical exec before loss classification" upgrade_legacy_exec_required "$FLOW_EV" agent
    FLOW_LEGACY_SIGNATURE=$(flow_legacy_signature) || {
        # Retain the actual raw oracle status/signature for review; its loss
        # alone cannot sign a predicted historical PRODUCT-RED classification.
        flow_legacy_exporter > "$FLOW_EV/legacy-exporter-unpinned.out" 2>&1
        setup_fail '39.F requires a reviewed FLOW_LEGACY_EXPECTED_LOSS pin; unpinned oracle evidence retained'
    }
    upgrade_legacy_assert "$FLOW_EV" agent "original legacy flow survives historical bare exec" \
        "hot-update-plan H2 legacy flow exporter; S8 legacy-to-capable first migration" \
        "$FLOW_LEGACY_SIGNATURE" flow_legacy_exporter
    _flow_legacy_receipt
    ;;
P)
    PTY_NEGATIVE_CONTROL=1
    PTY_LIBRARY_ONLY=1
    . "$HERE/drills/36-hot-update-pty.sh"
    drill_begin "39-hot-update-legacy-n1 [arm P]"
    drill_install_traps _pty_cleanup
    _pty_prepare_fixture "$PREV_BIN" old-old-old
    _pty_upgrade_and_finish
    _pty_legacy_assertion
    ;;
X)
    _h0_prepare
    assert_refuses "X live image refuses a legacy candidate" "agent_rejected:image_abi_unsupported" _abi_refused
    assert_ok "X refusal leaves disk, marker and running identity untouched while producer advances" _unchanged
    assert_ok "X original process advances after refused upgrade" poll_until 15 1 "post refusal progress" -- _capture_post
    assert_ok "X original producer stops and ctl receives its terminal outcome" _stop_and_capture
    assert_ok "X original task ends EXITED(23)" poll_until 20 1 "terminal row" -- _final_ps_exited
    assert_ok "X complete original output and rc23 survive ABI refusal" _refusal_continuity
    ;;
Y)
    _h0_prepare
    upgrade_legacy_ctl node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA" > "$H0_EVIDENCE/upgrade.stdout" 2> "$H0_EVIDENCE/upgrade.stderr"
    H0_UPGRADE_RC=$?
    printf '%s\n' "$H0_UPGRADE_RC" > "$H0_EVIDENCE/legacy-command.rc"
    assert_setup "Y real bare exec changes SHA and commits under original agent PID" poll_until 25 1 "legacy exporter upgrade" -- _capture_upgrade
    assert_setup "Y capture actual old exporter exec" upgrade_legacy_snapshot after "$H0_EVIDENCE"
    assert_setup "Y source and original control stack really executed" upgrade_legacy_exec_required "$H0_EVIDENCE" agent
    assert_setup "Y original child identity survives and independent source advances" poll_until 15 1 "post bare-exec source" -- _capture_post
    assert_setup "Y preserve final source, original transcripts and ctl timeout" _stop_and_capture
    assert_setup "Y agent audit proves exactly one legacy execution after reliable probe fallback" _legacy_audit
    # Predicted from cmd/tether/exec.go legacy NextMsgWithContext + exitcode.go:
    # timeout returns an untyped error => 70. Re-pin only after a real run.
    upgrade_legacy_assert "$H0_EVIDENCE" agent "original process output and management survive a legacy exporter upgrade" \
        "hot-update-plan H0 legacy exporter; S8 legacy-to-capable first migration" \
        '^HOT_UPDATE_LEGACY_EXPORTER output=epipe management=exited_minus_one ctl=idle_timeout_rc70 pid=[0-9]+ nonce=[0-9a-f]{32}$' _legacy_exporter
    ;;
Q00B0-exec)
    _upgrade_39_new_arm
    ;;
Q00B0-run)
    _upgrade_39_new_arm
    ;;
Q00B0-kill)
    _upgrade_39_new_arm
    ;;
Q00B0-expose)
    _upgrade_39_new_arm
    ;;
Q00B0-transfer)
    _upgrade_39_new_arm
    ;;
Q01B0-exec)
    _upgrade_39_new_arm
    ;;
Q01B0-run)
    _upgrade_39_new_arm
    ;;
Q01B0-kill)
    _upgrade_39_new_arm
    ;;
Q01B0-expose)
    _upgrade_39_new_arm
    ;;
Q01B0-transfer)
    _upgrade_39_new_arm
    ;;
Q10B0-exec)
    _upgrade_39_new_arm
    ;;
Q10B0-run)
    _upgrade_39_new_arm
    ;;
Q10B0-kill)
    _upgrade_39_new_arm
    ;;
Q10B0-expose)
    _upgrade_39_new_arm
    ;;
Q10B0-transfer)
    _upgrade_39_new_arm
    ;;
Q11B0-exec)
    _upgrade_39_new_arm
    ;;
Q11B0-run)
    _upgrade_39_new_arm
    ;;
Q11B0-kill)
    _upgrade_39_new_arm
    ;;
Q11B0-expose)
    _upgrade_39_new_arm
    ;;
Q11B0-transfer)
    _upgrade_39_new_arm
    ;;
Q00B1-exec)
    _upgrade_39_new_arm
    ;;
Q00B1-run)
    _upgrade_39_new_arm
    ;;
Q00B1-kill)
    _upgrade_39_new_arm
    ;;
Q00B1-expose)
    _upgrade_39_new_arm
    ;;
Q00B1-transfer)
    _upgrade_39_new_arm
    ;;
Q01B1-exec)
    _upgrade_39_new_arm
    ;;
Q01B1-run)
    _upgrade_39_new_arm
    ;;
Q01B1-kill)
    _upgrade_39_new_arm
    ;;
Q01B1-expose)
    _upgrade_39_new_arm
    ;;
Q01B1-transfer)
    _upgrade_39_new_arm
    ;;
Q10B1-exec)
    _upgrade_39_new_arm
    ;;
Q10B1-run)
    _upgrade_39_new_arm
    ;;
Q10B1-kill)
    _upgrade_39_new_arm
    ;;
Q10B1-expose)
    _upgrade_39_new_arm
    ;;
Q10B1-transfer)
    _upgrade_39_new_arm
    ;;
Q11B1-exec)
    _upgrade_39_new_arm
    ;;
Q11B1-run)
    _upgrade_39_new_arm
    ;;
Q11B1-kill)
    _upgrade_39_new_arm
    ;;
Q11B1-expose)
    _upgrade_39_new_arm
    ;;
Q11B1-transfer)
    _upgrade_39_new_arm
    ;;
LB-new)
    _upgrade_39_new_arm
    ;;
LB-old)
    _upgrade_39_new_arm
    ;;
LI-defer)
    _upgrade_39_new_arm
    ;;
LI-interrupting)
    _upgrade_39_new_arm
    ;;
M-route)
    _upgrade_39_new_arm
    ;;
CC-idle)
    _upgrade_39_new_arm
    ;;
CC-late-create)
    _upgrade_39_new_arm
    ;;
CL-reject)
    _upgrade_39_new_arm
    ;;
ABI-adjacent)
    _upgrade_39_new_arm
    ;;
ABI-too-far)
    _upgrade_39_new_arm
    ;;
ABI-unknown)
    _upgrade_39_new_arm
    ;;
GO-hash)
    _upgrade_39_new_arm
    ;;
CFG-nats-old)
    _upgrade_39_new_arm
    ;;
CFG-nats-new)
    _upgrade_39_new_arm
    ;;
CFG-caddy-old)
    _upgrade_39_new_arm
    ;;
CFG-caddy-new)
    _upgrade_39_new_arm
    ;;
# origin: hot-update S9 plan §7.1 / §9.3 / T80 — safe infra refusal controls
# (listed last so the label order matches the flat CASES39 manifest).
NI)
    drill_begin "39-hot-update-legacy-n1 [arm NI]"
    _infra_legacy_refusal nats
    infra_drill_close
    ;;
CI)
    drill_begin "39-hot-update-legacy-n1 [arm CI]"
    _infra_legacy_refusal caddy
    infra_drill_close
    ;;
*) setup_fail "unknown arm '$ARM'" ;;
esac
if [ "$ARM" = BR ]; then
    log "39.BR receipts retained: $BROKER_EV (native old Run-unwind oracle)"
elif [ "$ARM" = F ]; then
    log "39.F flow receipts retained: $FLOW_EV (native oracle rc and verdict are separate)"
elif [ "$ARM" = X ] || [ "$ARM" = Y ]; then
    _capture_logs
    log "39 receipts retained: $H0_EVIDENCE"
elif [ "$ARM" = P ]; then
    log "39.P receipts retained: $PTY_EV (native verdict and raw oracle rc are distinct)"
fi
drill_end
