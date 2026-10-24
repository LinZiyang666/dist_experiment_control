#!/bin/sh
# 35-hot-update-exec-carrier.sh — hot-update S1: a reliable `tether exec` survives its CARRIER being
# replaced under it. The original child keeps running under the same PID triple, its ctl receives every
# source byte on both streams and the exact exit code, the agent daemon never restarts and the command
# is executed exactly once — through a nats-server restart, a 25 s nats-server outage (past the agent's
# 20 s redial, so the whole NATS session is rebuilt) and a tether-broker restart (with its auth_callout
# black window). Plan: docs/reviews/hot-update-reliable-exec-plan.md §5.2. Expected: three GREEN arms.
#
# arms: A B C
# fixture: A=N1 B=N1 C=N1
# grows: A=0 B=0 C=0
# worst: A=360 B=300 C=420
# forgoes: A=- B=- C=-
#
# ── WHAT EACH ARM CLAIMS ────────────────────────────────────────────────────────────────────────────
#  A  nats-server restart (A1, seconds) then a 25 s nats-server outage (A2): the reliable path resumes
#     on the rebuilt NATS session. Calibration only: 10 post-recovery `exec agt1 -- true` wall times +
#     which path each took (carrier.json.calibration) — NOT a judged number.
#  B  tether-broker restart: the ctl re-attaches once the broker's subscriptions are back. B-new records
#     the rc/path/wall of a NEW exec launched right after the restart — recorded, never judged (plan §4
#     exit #5: new-request latency is not a criterion; connection-level retry is proven hermetically).
#  C  the N-1 window, in order. C1: the PREVIOUS RELEASE ctl (vendor/tether-dynamic-prev, built from the last v*
#     tag) against today's agent under the A2 fault — it can only take the legacy exec path, so the claim
#     is "legacy path taken, executed once, discontinuity recorded honestly" (requirements §6.7 promises
#     interoperability across the window, not carrier survival; `legacy-outcome.json` is the receipt).
#     C2: today's ctl against the PREVIOUS RELEASE agent (tether-prev installed as agt1's binary): the
#     reliable probe reaches the old agent (its `unknown forwarded verb` warning is the receipt), the ctl
#     falls back to legacy after the 2 s probe tax, and the run is complete.
#
# ── FALSE-GREEN RISK HEADNOTE ───────────────────────────────────────────────────────────────────────
#  1. A producer that stalled before the fault makes "no bytes lost" vacuous. Guard: _stream_advancing
#     (two live samples strictly increasing, child alive) gates every injection, and every fault window
#     must show >= MIN new source records (carrier.json.faults[].seq_*; judged).
#  2. A ctl that silently fell back to legacy would still stream. Guard: the agent's own audit line —
#     exactly one `agent: exec … reliable=true` for the nonce (arms A/B), exactly zero for arm C.
#  3. A second execution (create retried after the reply was lost) would also "complete". Guard: exactly
#     one `agent: exec` line for the nonce AND exactly one task row in `ps -a`.
#  4. The oracle is python probe.py; its positive AND negative synthetic controls run hermetically in
#     tests/run-all.sh (probe selftest + hot-update-process-probe-test). C1 is additionally the real
#     old-binary NEGATIVE control of the A/B oracle: under the same fault the previous ctl's transcript
#     must NOT satisfy `carrier`.
#  5. Every log read goes through drills/lib/logs.sh; a count that comes back EMPTY (log unreadable) is a
#     setup failure, never a zero.
#  6. The previous release is a DIE-GATE: the selected dynamic source receipt must name the tag, the binary's
#     sha256 must match it inside the container, `tether-prev version` must print the tag and the sha
#     must differ from today's build. tether-next (same source, bumped version) is never accepted.
#  7. A2's 25 s outage may trip tether-broker's systemd StartLimit. That is a deployment-tier finding
#     (SETUP-RED here, a gotcha there) — the 25 s is NOT tuned down to buy green (plan §5.4).
set -u
. "$HERE/lib/log.sh"
. "$HERE/lib/docker.sh"
. "$HERE/lib/tether.sh"
. "$HERE/lib/assert.sh"
SIM_REPO="$(cd "$HERE/../.." && pwd)"
SIM_VENDOR="$HERE/vendor"
. "$HERE/lib/stage.sh"
. "$HERE/drills/lib/logs.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab
PIN=135791
PROBE="$HERE/drills/lib/hot_update_process_probe.py"
DST=/home/sim/.local/bin/tether
PREV_BIN=/usr/local/bin/tether-prev
MIN=20
EV=
REMOTE_PROBE=
LOGS_CAPTURED=0
PREV_TAG=
# per-leg state, set by _leg_begin (a leg = one producer + one ctl; arm C runs two)
L_EV=; L_NONCE=; L_ROOT=; L_PIDFILE=; L_CTL=; L_CTL_JOB=; L_PATH=; L_CURSOR=
# per-fault state, set by _fault_begin
F_KIND=; F_T0=; F_SEQ0=; F_CTL0=; F_REG_CURSOR=

_capture_logs() {
    [ -n "$EV" ] && [ "$LOGS_CAPTURED" = 0 ] || return 0
    LOGS_CAPTURED=1
    sim_broker_slog brk1 4000 > "$EV/broker.slog" 2>&1 || true
    sim_broker_panic_journal_dump brk1 1000 > "$EV/broker.boot" 2>&1 || true
    sim_agent_slog_tail agt1 4000 > "$EV/agent.slog" 2>&1 || true
    sim_agent_panic_sink_dump agt1 1000 > "$EV/agent.boot" 2>&1 || true
}

_cleanup() {
    _capture_logs
    # Stop only this drill's own producers and verified ctls, after evidence
    # capture; none of this is product recovery.
    for _lf in "$EV"/*/leg.env; do
        [ -f "$_lf" ] || continue
        . "$_lf"
        dexec -u sim agt1 -- python3 "$REMOTE_PROBE" stop --terminate \
            --root "$L_ROOT" --nonce "$L_NONCE" >/dev/null 2>&1 || true
        dexec -u sim ctl1 -- python3 "$REMOTE_PROBE" stop-ctl --ctl-name "$L_CTL" \
            --pidfile "$L_PIDFILE" --nonce "$L_NONCE" >/dev/null 2>&1 || true
    done
    true
}
drill_install_traps _cleanup

# ── probes (FUNCTIONS — R-NOSHC) ────────────────────────────────────────────────────────────────────
_broker_up()    { "$SIM" exec brk1 -- tether cluster status --json 2>/dev/null | jq -e '.leader_id == "brk1"' >/dev/null; }
_ls_json()      { "$SIM" ctl -- node ls --json 2>/dev/null; }
_online()       { _ls_json | jq -e --arg n "$1" '.nodes[]?|select(.nid==$n and .status=="ONLINE")' >/dev/null 2>&1; }
# ONLINE can survive a short NATS outage as a stale roster row. Require a
# native registration after this fault before sampling the original stream;
# all existing recovery deadlines and progress assertions remain unchanged.
# origin: hot-update S5 plan §9.5 / DRILL-RUN 35.A run257
_agt1_online() {
    # A broker-only restart preserves the existing NATS session, so it does
    # not require a new registration. Fresh registration fences NATS faults.
    case "$F_KIND" in
        nats-restart|nats-outage) ;;
        broker-restart) _online agt1 || return 1; return 0 ;;
        *) return 1 ;;
    esac
    case "${F_REG_CURSOR:-}" in ''|*[!0-9]*) return 1 ;; esac
    [ "$F_REG_CURSOR" -gt 0 ] || return 1
    _online agt1 || return 1
    sim_agent_slog_grep agt1 '(^|[[:space:]{,])"?msg"?[=:][[:space:]]*"agent: (registered|re-registered after reconnect)"([[:space:],}]|$)' "$F_REG_CURSOR"
}
_release_of()   { _ls_json | jq -r --arg n "$1" '.nodes[]?|select(.nid==$n)|.release_version' | head -1; }
_agt1_at_prev() { [ "$(_release_of agt1)" = "$PREV_TAG" ] && _online agt1; }
_nats_active()  { sctl brk1 is-active --quiet nats-server; }
_agent_pid()    { dexec agt1 -- systemctl show tether-agent -p MainPID --value; }
_agent_start()  { dexec agt1 -- systemctl show tether-agent -p ExecMainStartTimestamp --value; }
_now_ns()       { date +%s%N; }

_install_probe() {
    d cp "$PROBE" "$(ctr_name agt1):$REMOTE_PROBE" || return 1
    d cp "$PROBE" "$(ctr_name ctl1):$REMOTE_PROBE" || return 1
    dexec agt1 -- chmod 0644 "$REMOTE_PROBE" || return 1
    dexec ctl1 -- chmod 0644 "$REMOTE_PROBE"
}

# The die-gate for the previous release (headnote 6). Every clause is a separate failure line so a red
# says which identity check failed; the whole thing is one assert_setup.
_prev_die_gate() {
    # origin: S8 internal review rv-n1 F6 — verify and copy the same adjacent release.
    sim_dynamic_prev_source_ok "$PREV_TAG" || { echo "$SIM_PREVIOUS_SOURCE does not vouch for $PREV_TAG"; return 1; }
    _pdg_want=$(sha256sum "$SIM_PREVIOUS_BINARY" | awk '{print $1}')
    _pdg_today=$(sha256sum "$HERE/vendor/tether" | awk '{print $1}')
    [ "$_pdg_want" != "$_pdg_today" ] || { echo "tether-prev is byte-identical to today's tether"; return 1; }
    d cp "$SIM_PREVIOUS_BINARY" "$(ctr_name ctl1):$PREV_BIN" || return 1
    dexec ctl1 -- chmod 0755 "$PREV_BIN" || return 1
    _pdg_got=$(dexec ctl1 -- sha256sum "$PREV_BIN" | awk '{print $1}')
    [ "$_pdg_got" = "$_pdg_want" ] || { echo "in-container tether-prev sha $_pdg_got != source $_pdg_want"; return 1; }
    _pdg_ver=$(dexec ctl1 -- "$PREV_BIN" version | awk 'NR==1 {print $2}')
    [ "$_pdg_ver" = "$PREV_TAG" ] || { echo "tether-prev reports release '$_pdg_ver', want $PREV_TAG"; return 1; }
}

# _leg_begin <name> <ctl-binary> <reliable|legacy> — a fresh nonce/root/evidence dir for one producer+ctl.
_leg_begin() {
    L_EV="$EV/$1"; L_CTL=$2; L_PATH=$3
    mkdir -p "$L_EV/before" "$L_EV/post" "$L_EV/final" || return 1
    L_NONCE=$(python3 -c 'import secrets; print(secrets.token_hex(16))') || return 1
    L_ROOT="/tmp/tether-35-work-$L_NONCE"
    L_PIDFILE="/tmp/tether-35-ctl-$L_NONCE.pid"
    L_CTL_JOB=""
    L_CURSOR=$(sim_agent_slog_cursor agt1)
    case "$L_CURSOR" in ''|*[!0-9]*) L_CURSOR=0 ;; esac
    printf 'L_NONCE=%s\nL_ROOT=%s\nL_PIDFILE=%s\nL_CTL=%s\n' "$L_NONCE" "$L_ROOT" "$L_PIDFILE" "$L_CTL" > "$L_EV/leg.env"
    printf '%s\n' "$L_NONCE" > "$L_EV/nonce"
    : > "$L_EV/faults.jsonl"
    : > "$L_EV/calibration.jsonl"
    log "leg $1: ctl=$L_CTL path=$L_PATH nonce=$L_NONCE evidence=$L_EV"
}

# Same user/HOME as simcluster's ctl command; the shell exec keeps the ctl's PID in the pidfile so
# cleanup can pin exactly this argv. The whole group is redirected: no command substitution may keep
# the pipe alive. The 360 s is a fixture lifetime, not a continuity budget.
_launch_producer() {
    (
        dexec -u sim ctl1 -- timeout --signal=TERM --kill-after=3s 370s env HOME=/home/sim sh -c \
            'printf "%s\n" "$$" > "$1"; shift; exec "$@"' \
            _ "$L_PIDFILE" "$L_CTL" exec --timeout 360s agt1 -- \
            python3 -u "$REMOTE_PROBE" producer --root "$L_ROOT" \
            --nonce "$L_NONCE" --period 0.05 --lifetime 420
        _lp_rc=$?
        printf '%s\n' "$_lp_rc" > "$L_EV/ctl.rc.tmp"
        mv "$L_EV/ctl.rc.tmp" "$L_EV/ctl.rc"
    ) > "$L_EV/live.stdout" 2> "$L_EV/live.stderr" &
    L_CTL_JOB=$!
}

_stream_ready() { python3 "$PROBE" ready --evidence "$L_EV" --nonce "$L_NONCE" --minimum "$MIN"; }

_capture_before() {
    d cp "$(ctr_name agt1):$L_ROOT/identity.json" "$L_EV/before/identity.json" || return 1
    d cp "$(ctr_name agt1):$L_ROOT/ledger.jsonl" "$L_EV/before/ledger.jsonl" || return 1
    cp "$L_EV/live.stdout" "$L_EV/before/stdout" || return 1
    cp "$L_EV/live.stderr" "$L_EV/before/stderr" || return 1
    "$SIM" ctl -- ps -a --json > "$L_EV/before/ps.json" || return 1
    python3 "$PROBE" baseline --evidence "$L_EV" --nonce "$L_NONCE" --minimum "$MIN"
}

_snapshot() { dexec -u sim agt1 -- python3 "$REMOTE_PROBE" snapshot --root "$L_ROOT" --nonce "$L_NONCE"; }
_snapshot_count() { _snapshot | jq -e 'select(.alive == true) | .data_count'; }

# Headnote 1: BOTH sides must be provably moving BEFORE anything is injected — the source ledger on
# the agent (child alive, counter increasing) AND the ctl's own transcript (records still arriving),
# each from two samples strictly increasing. The ctl side is what makes a later loss attributable to
# the injection: an old ctl that died at baseline+ε would otherwise be read as an honest N-1
# discontinuity (S1 review D1 / deploy-V1). `sleep 0.3` is the sampling gap of live counters, not a
# readiness barrier.
_ctl_count() { python3 "$PROBE" count --evidence "$L_EV" --nonce "$L_NONCE" | jq -e '.stdout'; }
_stream_advancing() {
    _sa_a=$(_snapshot_count) || return 1
    _sa_c=$(_ctl_count) || return 1
    sleep 0.3
    _sa_b=$(_snapshot_count) || return 1
    _sa_d=$(_ctl_count) || return 1
    [ "$_sa_b" -gt "$_sa_a" ] && [ "$_sa_d" -gt "$_sa_c" ]
}
_advanced_by() { _ab=$(_snapshot_count) || return 1; [ "$_ab" -ge $(( $1 + MIN )) ]; }

# _fault_begin <kind> / _fault_end — bracket one injection; the window's source progress is polled to
# >= MIN records (bounded) and then RECORDED whatever it is: the judge, not the poll, decides. The
# ctl-side transcript counts at injection (ctl_seq_before) are the judge's attribution floor.
_fault_begin() {
    F_KIND=$1
    F_SEQ0=$(_snapshot_count) || setup_fail "read the source counter before injecting $F_KIND (child not alive?)"
    F_CTL0=$(python3 "$PROBE" count --evidence "$L_EV" --nonce "$L_NONCE") || setup_fail "read the ctl transcript counts before injecting $F_KIND"
    F_T0=$(_now_ns)
    F_REG_CURSOR=$(sim_agent_slog_cursor agt1) || setup_fail "read native registration cursor before $F_KIND"
    case "$F_REG_CURSOR" in ''|*[!0-9]*|0) setup_fail "missing native registration cursor before $F_KIND" ;; esac
    printf '%s\n' "$F_REG_CURSOR" > "$L_EV/$F_KIND.registration.cursor" || setup_fail "retain native registration cursor"
    log "fault $F_KIND: begin (seq=$F_SEQ0 ctl=$F_CTL0)"
}
_fault_end() {
    poll_until 20 1 "source advanced >= $MIN through $F_KIND" -- _advanced_by "$F_SEQ0" >/dev/null 2>&1 || true
    _fe_seq=$(_snapshot_count) || _fe_seq=-1
    jq -cn --arg kind "$F_KIND" --argjson t0 "$F_T0" --argjson t1 "$(_now_ns)" \
        --argjson s0 "$F_SEQ0" --argjson s1 "$_fe_seq" --argjson ctl "$F_CTL0" \
        '{kind:$kind,t_start_ns:$t0,t_end_ns:$t1,seq_before:$s0,seq_after:$s1,ctl_seq_before:$ctl}' >> "$L_EV/faults.jsonl"
    log "fault $F_KIND: end (seq=$_fe_seq)"
}

_capture_post() {
    _snapshot > "$L_EV/post/snapshot.json" || return 1
    if [ ! -f "$L_EV/post/snapshot-start.json" ]; then
        cp "$L_EV/post/snapshot.json" "$L_EV/post/snapshot-start.json" || return 1
    fi
    "$SIM" ctl -- ps -a --json > "$L_EV/post/ps.json" || return 1
    python3 "$PROBE" post-ready --evidence "$L_EV" --nonce "$L_NONCE" --minimum "$MIN"
}

_stop_producer()     { dexec -u sim agt1 -- python3 "$REMOTE_PROBE" stop --root "$L_ROOT" --nonce "$L_NONCE"; }
_producer_finished() { dexec agt1 -- test -s "$L_ROOT/final.json"; }
_ctl_finished()      { test -s "$L_EV/ctl.rc"; }
_capture_final() {
    d cp "$(ctr_name agt1):$L_ROOT/ledger.jsonl" "$L_EV/final/ledger.jsonl" || return 1
    d cp "$(ctr_name agt1):$L_ROOT/final.json" "$L_EV/final/final.json" || return 1
    cp "$L_EV/live.stdout" "$L_EV/final/stdout" || return 1
    cp "$L_EV/live.stderr" "$L_EV/final/stderr" || return 1
    if _ctl_finished; then
        jq -n --argjson rc "$(cat "$L_EV/ctl.rc")" '{finished:true,exit_code:$rc}' > "$L_EV/final/ctl-outcome.json"
    else
        printf '%s\n' '{"finished":false}' > "$L_EV/final/ctl-outcome.json"
    fi
}
# The final `ps -a` row for the nonce must be EXITED(23): the agent reported the real exit to the
# broker (proc.exit through the courier) — polled, because the event is asynchronous to the ctl's exit.
_final_ps_exited() {
    "$SIM" ctl -- ps -a --json > "$L_EV/final/ps.json.tmp" || return 1
    jq -e --arg n "$L_NONCE" '[.processes[]? | select(.argv | index($n))] | length == 1 and .[0].status == "EXITED" and .[0].exit_code == 23' \
        "$L_EV/final/ps.json.tmp" >/dev/null || return 1
    mv "$L_EV/final/ps.json.tmp" "$L_EV/final/ps.json"
}
# Judged evidence is written ONCE: `final/ps.json` is what _final_ps_exited proved and moved into
# place. This later, unguarded re-sample goes to a sibling file — a hiccup in it must never truncate
# the file the judge reads (S1 review D2: a drill-made false RED is the Mandate's worst output).
_final_ps_record() { "$SIM" ctl -- ps -a --json > "$L_EV/final/ps.after.json"; }

# carrier.json — everything the judge reads about the carrier: agent identity before/after, the fault
# windows, and the agent's own audit counts for this nonce. An EMPTY count means the log could not be
# read (logs.sh contract) and is a hard failure here, never a zero.
_write_carrier() {
    _wc_exec=$(sim_agent_slog_count agt1 "agent: exec.*$L_NONCE" "$L_CURSOR")
    _wc_rel=$(sim_agent_slog_count agt1 "agent: exec.*$L_NONCE.*reliable=true" "$L_CURSOR")
    _wc_probe=$(sim_agent_slog_count agt1 'unknown forwarded verb.*process-resume' "$L_CURSOR")
    _wc_rebuild=$(sim_agent_slog_count agt1 'NATS stuck-disconnected; rebuilding session' "$L_CURSOR")
    for _wc_v in "$_wc_exec" "$_wc_rel" "$_wc_probe" "$_wc_rebuild"; do
        case "$_wc_v" in ''|*[!0-9]*) echo "agent slog unreadable (count='$_wc_v')"; return 1 ;; esac
    done
    _wc_pid_after=$(_agent_pid) || return 1
    _wc_ts_after=$(_agent_start) || return 1
    jq -n --arg nonce "$L_NONCE" --arg arm "${ARM:-?}" --arg path "$L_PATH" \
        --argjson pid0 "$AGENT_PID0" --argjson pid1 "$_wc_pid_after" \
        --arg ts0 "$AGENT_TS0" --arg ts1 "$_wc_ts_after" \
        --argjson ex "$_wc_exec" --argjson rel "$_wc_rel" --argjson pr "$_wc_probe" --argjson rb "$_wc_rebuild" \
        --slurpfile faults "$L_EV/faults.jsonl" --slurpfile cal "$L_EV/calibration.jsonl" \
        '{nonce:$nonce,arm:$arm,path:$path,faults:$faults,
          agent:{mainpid_before:$pid0,mainpid_after:$pid1,exec_ts_before:$ts0,exec_ts_after:$ts1},
          exec_start_lines:$ex,reliable_lines:$rel,probe_reached_lines:$pr,rebuild_lines:$rb,
          calibration:$cal}' > "$L_EV/carrier.json"
}

_judge_carrier()        { python3 "$PROBE" carrier --evidence "$L_EV" --nonce "$L_NONCE" --minimum "$MIN" --path "$L_PATH"; }
_judge_legacy_carrier() { python3 "$PROBE" legacy-carrier --evidence "$L_EV" --nonce "$L_NONCE" --minimum "$MIN" > "$L_EV/legacy-outcome.json"; }

# _calibrate <label> <n> — n × `exec agt1 -- true`: wall ms, rc and which path each took, RECORDED only.
_calibrate() {
    _cb_i=0
    while [ "$_cb_i" -lt "$2" ]; do
        _cb_cur=$(sim_agent_slog_cursor agt1); case "$_cb_cur" in ''|*[!0-9]*) _cb_cur=0 ;; esac
        _cb_t0=$(_now_ns)
        "$SIM" ctl -- exec --timeout 60s agt1 -- true >/dev/null 2>&1; _cb_rc=$?
        _cb_t1=$(_now_ns)
        _cb_rel=$(sim_agent_slog_count agt1 'agent: exec.*reliable=true' "$_cb_cur"); case "$_cb_rel" in ''|*[!0-9]*) _cb_rel=-1 ;; esac
        _cb_any=$(sim_agent_slog_count agt1 'agent: exec' "$_cb_cur"); case "$_cb_any" in ''|*[!0-9]*) _cb_any=-1 ;; esac
        # path: reliable / legacy (an exec line without reliable=true) / none (nothing spawned — an
        # rc≠0 refused before the agent) / unknown (log unreadable). S1 review D10.
        jq -cn --arg label "$1" --argjson i "$_cb_i" --argjson rc "$_cb_rc" \
            --argjson ms $(( (_cb_t1 - _cb_t0) / 1000000 )) --argjson rel "$_cb_rel" --argjson any "$_cb_any" \
            '{label:$label,i:$i,rc:$rc,wall_ms:$ms,path:(if $rel<0 or $any<0 then "unknown" elif $rel>=1 then "reliable" elif $any>=1 then "legacy" else "none" end)}' \
            >> "$L_EV/calibration.jsonl"
        _cb_i=$((_cb_i + 1))
    done
    log "calibration $1: $(jq -sc '[.[]|select(.label==$l)] | {n:length, rc0:([.[]|select(.rc==0)]|length), reliable:([.[]|select(.path=="reliable")]|length), max_ms:(map(.wall_ms)|max)}' --arg l "$1" "$L_EV/calibration.jsonl")"
}

# _leg_end — finish the leg's own fixtures (never product recovery): reap the ctl job if it exited, else
# stop it by pinned pidfd after evidence capture.
_leg_end() {
    if [ -n "$L_CTL_JOB" ]; then
        if _ctl_finished; then wait "$L_CTL_JOB" 2>/dev/null || true
        else
            dexec -u sim ctl1 -- python3 "$REMOTE_PROBE" stop-ctl --ctl-name "$L_CTL" \
                --pidfile "$L_PIDFILE" --nonce "$L_NONCE" >/dev/null 2>&1 || true
            wait "$L_CTL_JOB" 2>/dev/null || true
        fi
    fi
}

# The shared fault shapes (A2 == C1's fault): a 25 s nats-server outage is > redialAfter (20 s), so the
# agent deterministically rebuilds its NATS session (roster.go fireRedial). The sleep IS the fault.
_inject_nats_outage() {
    sctl brk1 stop nats-server || return 1
    sleep 25
    sctl brk1 start nats-server
}
# _rebuild_seen — rc 0: the agent logged its session rebuild; rc 1: it did not; rc 2: the slog could
# not be read (headnote 5: never a zero). The callers split rc 2 off as a SETUP failure.
_rebuild_seen() {
    _rs=$(sim_agent_slog_count agt1 'NATS stuck-disconnected; rebuilding session' "$L_CURSOR")
    case "$_rs" in ''|*[!0-9]*) return 2 ;; esac
    [ "$_rs" -ge 1 ]
}
_agent_slog_readable() { _asr=$(sim_agent_slog_count agt1 'agent:' "$L_CURSOR"); case "$_asr" in ''|*[!0-9]*) return 1 ;; esac; return 0; }

drill_begin "35-hot-update-exec-carrier (reliable exec survives its carrier: nats restart / outage / broker restart / N-1) [arm ${ARM:-?}]"
assert_setup "independent oracle positive, negative and nonempty controls" python3 "$PROBE" selftest
mkdir -p "${SIM_EVIDENCE_DIR:-/tmp}" || setup_fail "create evidence parent"
EV=$(mktemp -d "${SIM_EVIDENCE_DIR:-/tmp}/hot-update-exec-carrier.${ARM:-x}.XXXXXX") || setup_fail "create private evidence directory"
REMOTE_PROBE="/tmp/tether-35-probe-$(python3 -c 'import secrets; print(secrets.token_hex(8))').py"
# Source identity into the evidence (S1 review deploy-V3): current JSON here, selected previous in C;
# what ran must match the binary that was staged — a stale tether-source.json (a previous staging's,
# left behind by a failed prev build) would bind this run to the wrong code.
_source_json_matches() {
    [ -f "$HERE/vendor/tether-source.json" ] || return 1
    [ "$(jq -r .tether_sha256 "$HERE/vendor/tether-source.json")" = "$(sha256sum "$HERE/vendor/tether" | awk '{print $1}')" ]
}
assert_setup "vendor/tether-source.json describes the staged vendor/tether" _source_json_matches
cp "$HERE/vendor/tether-source.json" "$EV/tether-source.json" || setup_fail "copy tether-source.json into the evidence"
log "evidence: $EV"

"$SIM" nuke >/dev/null 2>&1 || true
assert_setup "up isolated broker + agent + ctl" "$SIM" up --brokers 1 --agents 1 --ctl 1
assert_setup "init brk1 N=1" "$SIM" init brk1
assert_setup "create session and activate real ctl" "$SIM" session "$SID" --pin "$PIN"
assert_setup "onboard real agent" "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_setup "copy workload/oracle without changing product or unit" _install_probe
AGENT_PID0=$(_agent_pid) || setup_fail "read agent MainPID"
AGENT_TS0=$(_agent_start) || setup_fail "read agent start timestamp"

case "${ARM:?}" in
A)
_leg_begin a tether reliable
_launch_producer
assert_setup "A0 real ctl streams a nonempty ordered baseline on both streams" poll_until 35 1 "producer outputs present" -- _stream_ready
assert_setup "A0 baseline: source/output agree and the task is RUNNING" poll_until 15 1 "baseline task and bytes agree" -- _capture_before
assert_setup "A0 non-vacuity: the source is provably advancing before injection" _stream_advancing
# A1 — clean nats-server restart (seconds; TCP close, fast reconnect on both agent and ctl).
_fault_begin nats-restart
assert_setup "A1 inject: restart nats-server on brk1" sctl brk1 restart nats-server
assert_setup "A1 recovery: nats-server active" poll_until 30 1 "nats-server active" -- _nats_active
assert_setup "A1 recovery: broker up" poll_until 60 2 "tether-broker leader again" -- _broker_up
assert_setup "A1 recovery: agt1 ONLINE" poll_until 60 2 "agt1 ONLINE" -- _agt1_online
_fault_end
# After an injection this is a PRODUCT claim (the child and its stream survived the restart), so it
# lands ASSERT-FAIL, not SETUP-RED, when the carrier fault killed them (S1 review D3).
assert_ok "A1 original child and its stream still advancing after the nats-server restart" _stream_advancing
# A2 — 25 s outage: past redialAfter, the agent rebuilds its whole NATS session.
_fault_begin nats-outage
assert_setup "A2 inject: nats-server stopped for 25 s then started" _inject_nats_outage
assert_setup "A2 recovery: nats-server active" poll_until 30 1 "nats-server active" -- _nats_active
assert_setup "A2 recovery: broker up (Restart=always revives a self-exit)" poll_until 90 2 "tether-broker leader again" -- _broker_up
assert_setup "A2 recovery: agt1 ONLINE" poll_until 90 2 "agt1 ONLINE" -- _agt1_online
_fault_end
assert_setup "A2 agent slog readable after the outage (an unreadable log is a fixture failure, never a zero)" _agent_slog_readable
assert_ok "A2 the outage really crossed redialAfter: agent logged 'NATS stuck-disconnected; rebuilding session'" _rebuild_seen
assert_ok "A3 original child alive under the same identity, source advancing, task still RUNNING after both faults" \
    poll_until 20 1 "post-fault producer observation" -- _capture_post
assert_setup "A4 request the producer's normal stop" _stop_producer
assert_ok "A4 workload wrote its terminal record (exit 23)" poll_until 15 1 "producer final record" -- _producer_finished
assert_ok "A4 original ctl exited on its own within 30 s of the stop" poll_until 30 1 "ctl exit delivery" -- _ctl_finished
assert_setup "A4 capture source ledger and unmodified ctl transcripts" _capture_final
assert_ok "A4 ps -a shows exactly one nonce row, EXITED(23)" poll_until 20 1 "final task row" -- _final_ps_exited
_final_ps_record >/dev/null 2>&1 || true
_calibrate post-A 10
assert_setup "A5 carrier record (agent identity, fault windows, audit counts)" _write_carrier
assert_ok "A5 CARRIER: original child + both streams + exact exit survived nats restart AND 25 s outage on the reliable path, executed once" _judge_carrier
_leg_end
;;
B)
_leg_begin b tether reliable
_launch_producer
assert_setup "B0 real ctl streams a nonempty ordered baseline on both streams" poll_until 35 1 "producer outputs present" -- _stream_ready
assert_setup "B0 baseline: source/output agree and the task is RUNNING" poll_until 15 1 "baseline task and bytes agree" -- _capture_before
assert_setup "B0 non-vacuity: the source is provably advancing before injection" _stream_advancing
_fault_begin broker-restart
assert_setup "B1 inject: restart tether-broker on brk1" sctl brk1 restart tether-broker
# B-new: a NEW exec launched immediately after the restart — its rc/path/wall are recorded, never judged.
_calibrate B-new 1
assert_setup "B1 recovery: broker up" poll_until 90 2 "tether-broker leader again" -- _broker_up
assert_setup "B1 recovery: agt1 ONLINE" poll_until 90 2 "agt1 ONLINE" -- _agt1_online
_fault_end
assert_ok "B2 original child alive under the same identity, source advancing, task still RUNNING after the broker restart" \
    poll_until 20 1 "post-fault producer observation" -- _capture_post
assert_setup "B3 request the producer's normal stop" _stop_producer
assert_ok "B3 workload wrote its terminal record (exit 23)" poll_until 15 1 "producer final record" -- _producer_finished
assert_ok "B3 original ctl exited on its own within 30 s of the stop" poll_until 30 1 "ctl exit delivery" -- _ctl_finished
assert_setup "B3 capture source ledger and unmodified ctl transcripts" _capture_final
assert_ok "B3 ps -a shows exactly one nonce row, EXITED(23)" poll_until 20 1 "final task row" -- _final_ps_exited
_final_ps_record >/dev/null 2>&1 || true
assert_setup "B4 carrier record (agent identity, fault windows, audit counts)" _write_carrier
assert_ok "B4 CARRIER: original child + both streams + exact exit survived the tether-broker restart on the reliable path, executed once" _judge_carrier
_leg_end
;;
C)
# origin: S8 internal review rv-n1 F6 — retain the selected receipt only after C's die-gate.
sim_select_previous dynamic || setup_fail "resolve the adjacent previous release supply"
PREV_TAG=$SIM_PREVIOUS_TAG
assert_setup "C-gate previous release $PREV_TAG: source.json vouches, in-container sha matches, version prints the tag, differs from today" _prev_die_gate
cp "$SIM_PREVIOUS_SOURCE" "$EV/tether-dynamic-prev-source.json" || setup_fail "retain the adjacent previous release source identity"
cmp -s "$SIM_PREVIOUS_SOURCE" "$EV/tether-dynamic-prev-source.json" || setup_fail "retained adjacent source identity differs from the selected receipt"
assert_setup "C-gate selected dynamic source receipt retained in the evidence" test -s "$EV/tether-dynamic-prev-source.json"
# C1 — previous-release CTL against today's agent, under the A2 fault.
_leg_begin c1 tether-prev legacy
_launch_producer
assert_setup "C1.0 $PREV_TAG ctl streams a nonempty ordered baseline (N-1 interoperability)" poll_until 35 1 "producer outputs present" -- _stream_ready
assert_setup "C1.0 baseline: source/output agree and the task is RUNNING" poll_until 15 1 "baseline task and bytes agree" -- _capture_before
assert_setup "C1.0 non-vacuity: the source is provably advancing before injection" _stream_advancing
_fault_begin nats-outage
assert_setup "C1.1 inject: nats-server stopped for 25 s then started" _inject_nats_outage
assert_setup "C1.1 recovery: nats-server active" poll_until 30 1 "nats-server active" -- _nats_active
assert_setup "C1.1 recovery: broker up" poll_until 90 2 "tether-broker leader again" -- _broker_up
assert_setup "C1.1 recovery: agt1 ONLINE" poll_until 90 2 "agt1 ONLINE" -- _agt1_online
_fault_end
assert_setup "C1.1 agent slog readable after the outage" _agent_slog_readable
assert_ok "C1.1 the outage really crossed redialAfter: agent logged the session rebuild" _rebuild_seen
assert_ok "C1.2 today's agent kept the legacy child alive and RUNNING through the outage" \
    poll_until 20 1 "post-fault producer observation" -- _capture_post
assert_setup "C1.3 request the producer's normal stop" _stop_producer
assert_ok "C1.3 workload wrote its terminal record (exit 23)" poll_until 15 1 "producer final record" -- _producer_finished
# The old ctl's exit is MEASURED, not required: a legacy exec whose exit chunk was published on the
# dead connection idles to its --timeout. A bounded wait, then the outcome is whatever it is.
if poll_until 10 1 "$PREV_TAG ctl exit delivery" -- _ctl_finished >/dev/null 2>&1; then :; fi
assert_setup "C1.3 capture source ledger and unmodified $PREV_TAG ctl transcripts" _capture_final
assert_ok "C1.3 ps -a shows exactly one nonce row, EXITED(23) (today's agent reported the real exit)" poll_until 20 1 "final task row" -- _final_ps_exited
_final_ps_record >/dev/null 2>&1 || true
assert_setup "C1.4 carrier record (agent identity, fault window, audit counts)" _write_carrier
assert_ok "C1.4 N-1 CTL: legacy path taken (0 reliable lines), executed once, discontinuity recorded honestly in legacy-outcome.json" _judge_legacy_carrier
_leg_end
# C2 — today's ctl against the PREVIOUS RELEASE agent (binary swap under the same unit and home).
assert_setup "C2.0 stop today's agent" sctl agt1 stop tether-agent
assert_setup "C2.0 install $PREV_TAG as agt1's binary" d cp "$SIM_PREVIOUS_BINARY" "$(ctr_name agt1):$DST"
assert_setup "C2.0 binary ownership/mode as install.sh leaves it" dexec agt1 -- sh -c "chown sim:sim '$DST' && chmod 0755 '$DST'"
assert_setup "C2.0 start the $PREV_TAG agent" sctl agt1 start tether-agent
assert_setup "C2.0 agt1 ONLINE at release $PREV_TAG" poll_until 90 2 "agt1 ONLINE at $PREV_TAG" -- _agt1_at_prev
AGENT_PID0=$(_agent_pid) || setup_fail "read $PREV_TAG agent MainPID"
AGENT_TS0=$(_agent_start) || setup_fail "read $PREV_TAG agent start timestamp"
_leg_begin c2 tether legacy
_launch_producer
assert_setup "C2.1 today's ctl streams a nonempty ordered baseline from the $PREV_TAG agent (after the 2 s probe tax)" poll_until 35 1 "producer outputs present" -- _stream_ready
assert_setup "C2.1 baseline: source/output agree and the task is RUNNING" poll_until 15 1 "baseline task and bytes agree" -- _capture_before
assert_ok "C2.2 child alive under the same identity and source advancing (no fault: the claim is interoperability)" \
    poll_until 20 1 "producer observation" -- _capture_post
assert_setup "C2.3 request the producer's normal stop" _stop_producer
assert_ok "C2.3 workload wrote its terminal record (exit 23)" poll_until 15 1 "producer final record" -- _producer_finished
assert_ok "C2.3 today's ctl exited on its own within 30 s of the stop" poll_until 30 1 "ctl exit delivery" -- _ctl_finished
assert_setup "C2.3 capture source ledger and unmodified ctl transcripts" _capture_final
assert_ok "C2.3 ps -a shows exactly one nonce row, EXITED(23)" poll_until 20 1 "final task row" -- _final_ps_exited
_final_ps_record >/dev/null 2>&1 || true
assert_setup "C2.4 carrier record (agent identity, audit counts incl. the old agent's 'unknown forwarded verb' receipt)" _write_carrier
assert_ok "C2.4 N-1 AGENT: probe reached the $PREV_TAG agent, ctl fell back to legacy, executed once, complete transcript and exact exit" _judge_carrier
_leg_end
;;
*) setup_fail "unknown arm '${ARM}' (arms: A B C)" ;;
esac

_capture_logs
log "35 receipts retained: $EV"
drill_end
