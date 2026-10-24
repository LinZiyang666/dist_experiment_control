#!/bin/sh
# 36-hot-update-pty.sh — original terminals across image and carrier replacement.
# origin: hot-update S4 plan §9.1–§9.4 / P15.
# arms: A W C L
# fixture: A=N1 W=N1 C=N1 L=N1
# grows: A=0 W=0 C=0 L=0
# worst: A=300 W=420 C=360 L=300
# forgoes: A=- W=- C=- L=-
# The same driver supplies 39.P. A dead post-fault foreground is evidence there,
# never a setup failure. Cleanup runs after the original ctl outcome is saved.
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
. "$HERE/drills/lib/fault.sh"
. "$HERE/drills/lib/hot_update_budget.sh"
SIM_REPO="$(cd "$HERE/../.." && pwd)"
SIM_VENDOR="$HERE/vendor"
. "$HERE/lib/stage.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab
NID=agt1
PIN=135790
PROBE="$HERE/drills/lib/hot_update_pty_probe.py"
MIXED="$HERE/drills/lib/hot_update_mixed_probe.py"
DST=/home/sim/.local/bin/tether
PREV_BIN=/usr/local/bin/tether-prev
MARKER=/home/sim/.local/bin/.tether-upgrade.json
PTY_EV=
PTY_STARTED=0
PTY_JOB=
MIXED_JOB=
PTY_LOGS_CAPTURED=0

# origin: hot-update S4 plan §9.1–§9.3 / T48; drill-run4 W/L.
# Execute in the parent: fixture globals and jobs must survive assertion capture.
# The saved predicate re-emits diagnostics when the harness judges its status.
_pty_step_run() {
    PTY_STEP_COUNT=$((${PTY_STEP_COUNT:-0} + 1))
    mkdir -p "$PTY_EV/diagnostics" || return 2
    PTY_STEP_LOG="$PTY_EV/diagnostics/step-$PTY_STEP_COUNT.out"
    printf 'command:' > "$PTY_STEP_LOG"
    printf ' %s' "$@" >> "$PTY_STEP_LOG"
    printf '\n' >> "$PTY_STEP_LOG"
    "$@" >> "$PTY_STEP_LOG" 2>&1
}
_pty_step_ok() {
    [ "$1" = 0 ] && return 0
    _pso_log=${2:-${PTY_STEP_LOG:-unavailable}}
    printf 'PTY step failed rc=%s; evidence=%s\n' "$1" "$_pso_log" >&2
    if [ -r "$_pso_log" ]; then
        tail -n 20 "$_pso_log" | cut -c 1-1000 >&2
    fi
    return "$1"
}

_pty_broker_ready() {
    "$SIM" exec brk1 -- tether cluster status --json | jq -e '.leader_id=="brk1"' >/dev/null || { printf '%s\n' 'PTY broker readiness failed: require leader brk1' >&2; return 1; }
}
_pty_agent_ready() {
    "$SIM" ctl -- node ls --json | jq -e '[.nodes[]? | select(.nid=="agt1" and .status=="ONLINE")] | length==1' >/dev/null || { printf '%s\n' 'PTY agent readiness failed: require one ONLINE agt1' >&2; return 1; }
}
_pty_pid() { dexec agt1 -- systemctl show tether-agent -p MainPID --value; }
_pty_sha() { dexec agt1 -- sha256sum "/proc/$1/exe" | awk '{print $1}'; }
_pty_ticks() {
    dexec agt1 -- python3 -c 'import pathlib,sys; print(pathlib.Path("/proc/"+sys.argv[1]+"/stat").read_text().rsplit(")",1)[1].split()[19])' "$1"
}

# origin: hot-update S4 plan §9.1–§9.3 / DRILL-SUBSHELL. assert_* captures
# predicates in a command-substitution subshell. Establish fixture globals and
# start/reap jobs here, then assert the exact saved status in a pure predicate.
# This keeps the original claim identity and exposes each failed operation.
# Shell-owned receipts remain separate from the raw-byte trace owned by P14.
# A path flip without exec, or a supervisor restart, must not earn a cut.
# origin: hot-update S4 plan §9.1 / P15 (old SHA, fault not exercised).
_pty_supply_valid() {
    jq -e --arg old "$PTY_OLD_SHA" --arg new "$PTY_NEW_SHA" '
      .old_sha==$old and .new_sha==$new and .old_sha!=.new_sha and
      (.old_sha|test("^[0-9a-f]{64}$")) and (.new_sha|test("^[0-9a-f]{64}$")) and
      (.pid|type=="number") and .pid>1 and (.start_ticks|type=="number") and .start_ticks>0 and
      .artifact_sha==.served_sha and (.artifact_sha|test("^[0-9a-f]{64}$"))' "$PTY_EV/supply.json" >/dev/null || {
        printf '%s\n' 'PTY receipt predicate failed; observed receipt follows' >&2
        jq -c . "$PTY_EV/supply.json" >&2
        return 1
    }
}
# origin: hot-update S4 plan §9.2 / T48; drill-run3 W. The rollback
# marker retains dispatch ownership; fresh runtime logs prove its activation.
_pty_hop_valid() {
    jq -e --argjson pid "$PTY_PID" --argjson ticks "$PTY_TICKS" --arg sha "$1" --arg state "$2" '
      def uint: type=="number" and .>=0 and floor==.;
      .pid==$pid and .start_ticks==$ticks and .running_sha==$sha and .disk_sha==$sha and
      .marker.state==$state and .marker.target_sid=="lab" and .marker.target_nid=="agt1" and
      (.marker.boot_count|uint) and (.marker.generation|uint) and
      .marker.boot_count>0 and .marker.generation>0 and
      (if $state=="rolled_back" then .marker.resource_phase=="redispatched"
       else .marker.resource_phase=="active" end)' "$3" >/dev/null || {
        printf '%s\n' 'PTY receipt predicate failed; observed receipt follows' >&2
        jq -c . "$3" >&2
        return 1
    }
}
_pty_record_hop() {
    _prh_pid=$(_pty_pid) || return 1
    _prh_ticks=$(_pty_ticks "$_prh_pid") || return 1
    _prh_sha=$(_pty_sha "$_prh_pid") || return 1
    _prh_disk=$(dexec agt1 -- sha256sum "$DST" | awk '{print $1}') || return 1
    dexec agt1 -- cat "$MARKER" > "$PTY_EV/marker.tmp" || return 1
    jq -n --argjson pid "$_prh_pid" --argjson ticks "$_prh_ticks" --arg sha "$_prh_sha" --arg disk "$_prh_disk" \
        --slurpfile m "$PTY_EV/marker.tmp" \
        '{pid:$pid,start_ticks:$ticks,running_sha:$sha,disk_sha:$disk,marker:$m[0]}' > "$PTY_EV/$3.tmp" || return 1
    mv "$PTY_EV/$3.tmp" "$PTY_EV/$3" || return 1
    if [ "${PTY_NEGATIVE_CONTROL:-0}" = 1 ]; then
        # Bare exec has no carried image generation. Prove the fault separately
        # and let the identical byte oracle expose the missing terminal owner.
        _pty_legacy_hop_valid "$PTY_EV/$3" || return 1
    else
        _pty_hop_valid "$1" "$2" "$PTY_EV/$3" || return 1
    fi
    pty_agent_snapshot "${3%.json}"
}
_pty_legacy_hop_valid() {
    jq -e --argjson pid "$PTY_PID" --argjson ticks "$PTY_TICKS" --arg sha "$PTY_NEW_SHA" '
      def uint: type=="number" and .>=0 and floor==.;
      .pid==$pid and .start_ticks==$ticks and .running_sha==$sha and .disk_sha==$sha and
      .marker.state=="committed" and .marker.new_sha==$sha and (.marker.boot_count|uint) and .marker.boot_count>0 and
      .marker.target_sid=="lab" and .marker.target_nid=="agt1"' "$1" >/dev/null || {
        printf '%s\n' 'PTY receipt predicate failed; observed receipt follows' >&2
        jq -c . "$1" >&2
        return 1
    }
}
_pty_upgrade() {
    if [ "${PTY_NEGATIVE_CONTROL:-0}" = 1 ]; then
        upgrade_legacy_ctl node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA" > "$PTY_EV/upgrade-$1.out" 2>&1
    else
        "$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA" > "$PTY_EV/upgrade-$1.out" 2>&1
    fi
    _pu_rc=$?
    printf '%s\n' "$_pu_rc" > "$PTY_EV/upgrade-$1.rc"
    if [ "${PTY_NEGATIVE_CONTROL:-0}" = 1 ]; then printf '%s\n' "$_pu_rc" > "$PTY_EV/legacy-command.rc"; fi
    return "$_pu_rc"
}
_pty_checkpoint() {
    pty_checkpoint "$1"
}
_pty_baseline() {
    _pty_checkpoint before || return 1
    # origin: hot-update S4 plan §9.1 / T48. Baseline is a nonempty observation,
    # not a final envelope: no waitpid, fault window or future ACK is invented.
    python3 -I -B - "$PROBE" "$PTY_EV" "$PTY_NONCE" <<'PY'
from pathlib import Path
import json
import runpy
import sys
sensor = runpy.run_path(sys.argv[1])
root, nonce = Path(sys.argv[2]), sys.argv[3]
for name in ("ctl.sent", "ctl.received", "remote.sent", "remote.received"):
    rows = sensor["parse_frames"]((root/name).read_bytes(), nonce, complete=False)
    if len(rows) < 20:
        raise SystemExit("PTY baseline has fewer than 20 real records in "+name)
remote = json.loads((root/"remote.trace.json").read_text())
ctl = json.loads((root/"ctl.identity.json").read_text())
agent = json.loads((root/"agent/before.json").read_text())
if remote.get("nonce") != nonce or ctl.get("nonce") != nonce:
    raise SystemExit("foreign original terminal receipt")
samples = remote["samples"]
leader = next(r for r in samples if r["role"] == "leader")
foreground = next(r for r in samples if r["role"] == "foreground")
if (len(samples) < 20 or not all(r["ctty"] and r["alive"] and not r["hup"] and not r["early_eof"] for r in samples)
        or leader["identity"]["pid"] == foreground["identity"]["pid"]
        or leader["identity"]["ppid"] != agent["identity"]["pid"]
        or foreground["foreground_pgid"] != foreground["identity"]["pid"]
        or ctl["identity"]["tty_nr"] == 0):
    raise SystemExit("original controlling terminal/foreground identity not established")
PY
}
_pty_window() {
    if [ "$1" = end ]; then
        case "$2" in
            carrier) _pty_rebuild_seen || return 2 ;;
            watchdog) _pty_watchdog_valid || return 2 ;;
            refusal) _pty_unchanged || return 2 ;;
        esac
    fi
    # Containers share the host monotonic clock. Byte/sample timestamps are
    # rebased to this original ctl origin when the final envelope is assembled.
    python3 -I -B - "$PTY_EV" "$PTY_NONCE" "$1" "$2" <<'PY'
import json
from pathlib import Path
import sys
import time
root, nonce, action, kind = Path(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
origin = json.loads((root/"ctl.identity.json").read_text())["started_monotonic"]
path = root/"windows.json"
doc = json.loads(path.read_text()) if path.exists() else dict(nonce=nonce, windows=[])
if action == "start":
    doc["windows"].append(dict(start=time.monotonic()-origin, kind=kind))
else:
    row = doc["windows"][-1]
    if kind in ("image","draining"):
        before=json.loads((root/"agent/before.json").read_text())
        snapshots=[json.loads(p.read_text()) for p in (root/"agent").glob("hop-*.json") if not p.name.endswith(".ps.json")]
        keys=("pid","start_ticks","boot_id")
        if not any(s["sha256"] != before["sha256"] and all(s["identity"][k] == before["identity"][k] for k in keys) for s in snapshots):
            raise SystemExit("no observed original-agent image switch")
    row.update(end=time.monotonic()-origin, fault_observed=True)
    if kind == "watchdog":
        row["blackhole_s"] = json.loads((root/"fault.json").read_text())["elapsed_s"]
    if kind == "draining":
        receipt=json.loads((root/"draining.receipt.json").read_text())
        if receipt["nonce"] != nonce: raise SystemExit("foreign draining receipt")
        row.update(leader_exited=receipt["leader_exited"],leader_exit_at=receipt["at"]-origin,tail_s=receipt["tail_s"])
path.write_text(json.dumps(doc)+"\n")
PY
}
_pty_continuity() {
    python3 "$PROBE" continuity --evidence "$PTY_EV" --nonce "$PTY_NONCE"
}

# origin: hot-update S4 plan §9.2 / 36.W. Compare three actual generations,
# not the install bundle twice. ACK/Base/control advancement while disconnected
# is intentionally absent: the communication-capable T24/T35 arm owns it.
_pty_current_state() {
    jq -e -s '
      def uint: type=="number" and .>=0 and floor==.;
      def ka: [.controls[] | select(.op=="keepalive")] | last;
      .[0] as $a | .[1] as $b | .[2] as $r |
      all(.[]; .version==1 and .nonce==$a.nonce and (.nonce|test("^[0-9a-f]{32}$")) and
        .protocol=="reliable-pty-v1" and .ctl_identity==$a.ctl_identity and
        .ctl_identity.pid>1 and .ctl_identity.start_ticks>0 and (.ctl_identity.boot_id|type=="string")) and
      all(.[]; all([.input.accepted,.input.applied,.output.end,.output.base,(ka).seq,(ka).deadline][]; uint)) and
      $a.phase=="cut" and $b.phase=="cut" and $r.phase=="activated" and
      ($a.generation|uint) and $b.generation==$a.generation+1 and $r.generation==$b.generation and
      $a.input.accepted>$a.input.applied and $a.output.end>$a.output.base and
      $b.input.applied>$a.input.applied and $b.output.end>$a.output.end and
      $r.input.applied>=$b.input.applied and $r.input.accepted>=$b.input.accepted and
      $r.output.end>=$b.output.end and $r.output.base>=$b.output.base and
      all(.[]; (ka).ack==(ka).seq) and ($a|ka).seq>0 and ($a|ka).deadline>0 and
      ($a|ka).deadline==($b|ka).deadline and ($b|ka).deadline==($r|ka).deadline and
      ($a|ka).seq==($b|ka).seq and ($b|ka).seq==($r|ka).seq' \
      "$PTY_EV/queued.json" "$PTY_EV/active.json" "$PTY_EV/rollback.json" >/dev/null || {
        printf '%s\n' 'PTY current state failed: require pending input, unACKed output, current byte progress and unchanged committed KA' >&2
        jq -c '{generation,phase,input,output,controls}' "$PTY_EV/queued.json" "$PTY_EV/active.json" "$PTY_EV/rollback.json" >&2
        return 1
    }
}
_pty_active_progress() {
    _pty_checkpoint active || return 1
    # Activation is not proof that queued bytes reached the kernel. Wait on
    # those receipts while the candidate is still disconnected, not a sleep.
    jq -e -s '
      .[0] as $a | all(.[]; .version==1 and .nonce==$a.nonce and .protocol=="reliable-pty-v1" and
        .ctl_identity==$a.ctl_identity and
        (.input.applied|type=="number") and (.output.end|type=="number")) and
      .[1].input.applied>.[0].input.applied and .[1].output.end>.[0].output.end' \
      "$PTY_EV/queued.json" "$PTY_EV/active.json" >/dev/null || {
        printf '%s\n' 'PTY receipt predicate failed; observed receipt follows' >&2
        jq -c . "$PTY_EV/queued.json" "$PTY_EV/active.json" >&2
        return 1
    }
}
_pty_watchdog_valid() {
    _pty_hop_valid "$PTY_OLD_SHA" rolled_back "$PTY_EV/hop-rollback.json" || return 1
    jq -e -s --argjson budget "$HOT_UPDATE_REGISTER_DEADLINE_S" '
      .[0].marker as $a | .[1].marker as $b | .[2] as $f |
      $b.generation==$a.generation+1 and
      ($b.detail|contains("register deadline exceeded")) and
      $f.blackhole_rc==124 and ($f.elapsed_s|type=="number") and $f.elapsed_s>=$budget and $f.offline==true and
      $f.prev_consumed==true' "$PTY_EV/hop-active.json" "$PTY_EV/hop-rollback.json" "$PTY_EV/fault.json" >/dev/null || {
        printf '%s\n' 'PTY receipt predicate failed; observed receipt follows' >&2
        jq -c . "$PTY_EV/fault.json" >&2
        return 1
    }
}
_pty_rebuild_seen() {
    _prs_n=$(sim_agent_slog_count agt1 'rebuilding session' "$PTY_CURSOR") || return 2
    case "$_prs_n" in ''|*[!0-9]*) return 2 ;; esac
    [ "$_prs_n" -gt 0 ] || { printf 'PTY carrier rebuild not observed: rebuilding_session_lines=%s\n' "$_prs_n" >&2; return 1; }
    # origin: hot-update S4 plan §6.1 / §9.2 / C1 / T48. A rebuild-start
    # message precedes registration. End the fault only after the original
    # task's successor session is established, through the shared snapshot.
    pty_fixture_collect || return 2
    pty_collect_logs || return 2
    python3 -I -B - "$PROBE" "$PTY_EV" "$PTY_NONCE" <<'PY'
from pathlib import Path
import runpy
import sys
sensor = runpy.run_path(sys.argv[1])
root, nonce = Path(sys.argv[2]), sys.argv[3]
try:
    runtime = sensor["runtime_records"](root, nonce, terminal=False)
    window = sensor["read_json"](root/"windows.json")["windows"][-1]
    sensor["require"](window["kind"] == "carrier" and any(
        window["start"] <= p["lost"]["at"] <= p["restored"]["at"] for p in runtime["carrier_pairs"]),
        "original carrier restoration not yet observed")
except (sensor["EvidenceError"], OSError, ValueError, KeyError, TypeError, StopIteration) as exc:
    print("PTY carrier receipt failed: " + str(exc), file=sys.stderr)
    raise SystemExit(2)
PY
}
_pty_node_offline() {
    # origin: hot-update S4 plan §9.2 / T48; drill-run3 W. JSON retains
    # node ls's default ONLINE filter; an empty list cannot prove OFFLINE.
    "$SIM" ctl -- node ls -a --json > "$PTY_EV/offline.json" || return 1
    jq -e '[.nodes[]? | select(.nid=="agt1" and .status=="OFFLINE")] | length==1' "$PTY_EV/offline.json" >/dev/null || {
        printf '%s\n' 'PTY receipt predicate failed; observed receipt follows' >&2
        jq -c . "$PTY_EV/offline.json" >&2
        return 1
    }
}
_pty_blackhole() {
    fault_assert_blackholed "$@" || return 1
    jq -n --arg n "$PTY_NONCE" --argjson rc "$_fb_rc" '{nonce:$n,rc:$rc}' > "$PTY_EV/blackhole-observation.json"
}
_pty_fault_receipt() {
    dexec agt1 -- test ! -e "$DST.prev" || return 1
    jq -e '[.nodes[]? | select(.nid=="agt1" and .status=="OFFLINE")] | length==1' "$PTY_EV/offline.json" >/dev/null || return 1
    _pfr_elapsed=$(python3 -c 'import sys,time; print(time.monotonic()-float(sys.argv[1]))' "$PTY_FAULT_START") || return 1
    jq -n --argjson elapsed "$_pfr_elapsed" --slurpfile observation "$PTY_EV/blackhole-observation.json" \
        '{blackhole_rc:$observation[0].rc,elapsed_s:$elapsed,offline:true,prev_consumed:true}' > "$PTY_EV/fault.json"
}
_pty_legacy_refused() {
    if [ "$1" = 75 ] && grep -q 'agent_rejected:upgrade_deferred' "$2" && grep -q 'legacy_session_busy' "$2"; then return 0; fi
    printf 'PTY legacy refusal failed rc=%s; expected upgrade_deferred legacy_session_busy\n' "$1" >&2
    cat "$2" >&2
    return 1
}
_pty_unchanged() {
    [ "$(_pty_pid)" = "$PTY_PID" ] && [ "$(_pty_ticks "$PTY_PID")" = "$PTY_TICKS" ] || return 1
    [ "$(_pty_sha "$PTY_PID")" = "$PTY_OLD_SHA" ] || return 1
    [ "$(dexec agt1 -- sha256sum "$DST" | awk '{print $1}')" = "$PTY_OLD_SHA" ] || return 1
    dexec agt1 -- test ! -e "$MARKER"
}
_pty_refusal() {
    _pty_upgrade "$1"
    _pf_rc=$?
    _pty_legacy_refused "$_pf_rc" "$PTY_EV/upgrade-$1.out" && _pty_unchanged
}
_pty_strict_mixed() {
    jq -e '
      .request_failures==0 and .connect_failures==0 and .refusals==0 and .over_5s==0 and
      .configured_rate==10 and .rate>=9 and .writes_in_window>0 and .in_window>0 and
      .successful.read>0 and .successful.write>0 and .successful.short>0 and
      .dup_side_effects==0 and .rc0_without_effect==0 and .effect_without_rc0==0 and .unrequested_effects==0' "$1" >/dev/null || {
        printf '%s\n' 'PTY receipt predicate failed; observed receipt follows' >&2
        jq -c . "$1" >&2
        return 1
    }
}
_pty_mixed_mark() {
    sim_agent_slog_grep agt1 "agent: image $1" "$PTY_CURSOR" || return 1
    dexec -u sim ctl1 -- python3 "$MIXED_REMOTE/hot_update_mixed_probe.py" mark --evidence "$MIXED_ROOT" --name "$1"
}
_pty_mixed_switch() {
    if [ "$PTY_CUT_SEEN" = 0 ]; then
        _pty_mixed_mark cut || return 1
        PTY_CUT_SEEN=1
    fi
    _pty_mixed_mark activated
}
_pty_mixed_started() { dexec ctl1 -- test -s "$MIXED_ROOT/started.json"; }
_pty_mixed_prepare() {
    MIXED_REMOTE="/tmp/pty-mixed-tool-$PTY_NONCE"
    MIXED_ROOT="/tmp/pty-mixed-$PTY_NONCE"
    dexec -u sim ctl1 -- mkdir -p "$MIXED_REMOTE" || return 1
    d cp "$MIXED" "$(ctr_name ctl1):$MIXED_REMOTE/hot_update_mixed_probe.py" || return 1
    d cp "$HERE/drills/lib/hot_update_budget.sh" "$(ctr_name ctl1):$MIXED_REMOTE/hot_update_budget.sh" || return 1
    dexec -u sim agt1 -- mkdir -p "/tmp/hot-update-mixed-$PTY_NONCE"
}
_pty_mixed_collect() {
    mkdir -p "$PTY_EV/mixed" || return 1
    d cp "$(ctr_name ctl1):$MIXED_ROOT/." "$PTY_EV/mixed/" || return 1
    d cp "$(ctr_name agt1):/tmp/hot-update-mixed-$PTY_MIXED_NONCE/ledger" "$PTY_EV/mixed/ledger" || return 1
    python3 "$MIXED" judge --strict --evidence "$PTY_EV/mixed" --ledger "$PTY_EV/mixed/ledger" \
        --mode upgrade --window-start cut --window-end activated > "$PTY_EV/mixed/judge.json" || return 1
    _pty_strict_mixed "$PTY_EV/mixed/judge.json"
}
_pty_previous_supply() {
    PREV_TAG=$(sim_fixed_prev_tag) || return 1 # origin: S8 internal review rv-n1 F6
    sim_prev_source_ok "$PREV_TAG" || return 1
    d cp "$HERE/vendor/tether-prev" "$(ctr_name ctl1):$PREV_BIN" || return 1
    dexec ctl1 -- chmod 0755 "$PREV_BIN" || return 1
    _pps_sha=$(sha256sum "$HERE/vendor/tether-prev" | awk '{print $1}') || return 1
    [ "$(dexec ctl1 -- sha256sum "$PREV_BIN" | awk '{print $1}')" = "$_pps_sha" ] || return 1
    dexec ctl1 -- "$PREV_BIN" version > "$PTY_EV/previous.version" || return 1
    [ "$(awk 'NR==1 {print $2}' "$PTY_EV/previous.version")" = "$PREV_TAG" ] || return 1
    ! grep -q 'image-abi' "$PTY_EV/previous.version" || return 1
    cp "$HERE/vendor/tether-prev-source.json" "$PTY_EV/tether-prev-source.json"
}
_pty_install_previous() {
    sctl agt1 stop tether-agent || return 1
    d cp "$HERE/vendor/tether-prev" "$(ctr_name agt1):$DST" || return 1
    dexec agt1 -- chown sim:sim "$DST" || return 1
    dexec agt1 -- chmod 0755 "$DST" || return 1
    sctl agt1 start tether-agent || return 1
    poll_until 60 1 "previous agent online" -- _pty_agent_ready || return 1
    [ "$(_pty_sha "$(_pty_pid)")" = "$(sha256sum "$HERE/vendor/tether-prev" | awk '{print $1}')" ]
}
_pty_collect() {
    pty_collect_logs || return 1
    PTY_LOGS_CAPTURED=1
    python3 "$PROBE" trace --role assemble --evidence "$PTY_EV" --nonce "$PTY_NONCE" > "$PTY_EV/envelope.json"
}
_pty_gate_pending() {
    [ "$(_pty_sha "$(_pty_pid)")" = "$PTY_NEW_SHA" ] || return 1
    dexec agt1 -- cat "$MARKER" > "$PTY_EV/gate-marker.json" || return 1
    jq -e --arg sha "$PTY_NEW_SHA" '.state=="pending" and .boot_count>0 and .new_sha==$sha and .target_sid=="lab" and .target_nid=="agt1"' "$PTY_EV/gate-marker.json" >/dev/null || { printf '%s\n' 'PTY first migration gate failed: require booted pending target candidate' >&2; jq -c . "$PTY_EV/gate-marker.json" >&2; return 1; }
}
_pty_gate_reachable() {
    _pty_gate_pending || return 1
    _pty_agent_ready || return 1
    _pgr_n=$(sim_agent_slog_count agt1 'agent: upgrade commit: marker write failed' "$PTY_CURSOR") || return 1
    case "$_pgr_n" in ''|*[!0-9]*) return 1 ;; esac
    [ "$_pgr_n" -gt 0 ] || { printf 'PTY pending gate not reachable: commit_write_failure_lines=%s\n' "$_pgr_n" >&2; return 1; }
}
_pty_legacy_gate_valid() {
    # origin: hot-update S4 internal review rv-drill DRILL-6. rc75 alone
    # also describes no responders; require the reached handler's own reason.
    jq -e '.rc==75 and .reason=="agent_upgrading" and .spawn_lines==0 and .effect_absent==true and .pending==true and .candidate_unchanged==true' "$1" >/dev/null || {
        printf '%s\n' 'PTY legacy gate failed: require rc75 agent_upgrading, zero spawns, absent effect and unchanged pending candidate' >&2
        jq -c . "$1" >&2
        return 1
    }
}
_pty_legacy_gate() {
    # First migration pending must reject old wire too, before any Start. The
    # fresh previous agent has no live terminal that would defer installation.
    _pty_install_previous || return 1
    _plg_pid=$(_pty_pid) || return 1
    _plg_ticks=$(_pty_ticks "$_plg_pid") || return 1
    if [ "${PTY_NEGATIVE_CONTROL:-0}" = 1 ]; then
        assert_setup "historical PTY original role identities" upgrade_legacy_snapshot before "$PTY_EV"
        printf '%s\n' "$PTY_NEW_SHA" > "$PTY_EV/legacy-target.sha"
    fi
    PTY_CURSOR=$(sim_agent_slog_cursor agt1) || return 1
    fault_synblock_on agt1 4222 || return 1
    fault_assert_blackholed agt1 brk1 4222 || return 1
    _pty_upgrade legacy-gate & PTY_JOB=$!
    poll_until 45 1 "first migration gate pending" -- _pty_gate_pending || return 1
    # Fault only commit persistence after the real boot proof has landed. Heal
    # transport before admission: registration/subscriptions must be reachable.
    # origin: hot-update S4 internal review rv-drill DRILL-6.
    dexec agt1 -- chmod 0555 /home/sim/.local/bin || return 1
    PTY_COMMIT_FAULT=1
    fault_synblock_off agt1 || return 1
    poll_until 30 1 "candidate registered with failed durable commit" -- _pty_gate_reachable || return 1
    [ "$(_pty_pid)" = "$_plg_pid" ] && [ "$(_pty_ticks "$_plg_pid")" = "$_plg_ticks" ] || return 1
    _plg_cursor=$(sim_agent_slog_cursor agt1) || return 1
    dexec -u sim ctl1 -- timeout 10 env HOME=/home/sim "$PREV_BIN" run agt1 -- \
        sh -c 'printf "%s\n" "$1" > "$2"' _ "$PTY_NONCE" "/tmp/legacy-effect-$PTY_NONCE" \
        > "$PTY_EV/legacy-gate.out" 2>&1
    _plg_rc=$?
    _plg_spawns=$(sim_agent_slog_count agt1 "agent: run.*$PTY_NONCE" "$_plg_cursor") || return 1
    case "$_plg_spawns" in ''|*[!0-9]*) return 1 ;; esac
    dexec agt1 -- test ! -e "/tmp/legacy-effect-$PTY_NONCE" || {
        printf 'PTY pending gate executed old wire: rc=%s spawn_lines=%s; effect exists\n' "$_plg_rc" "$_plg_spawns" >&2
        cat "$PTY_EV/legacy-gate.out" >&2
        return 1
    }
    # Read the product reason from the old ctl's own output, independently of
    # its exit class. A missing or unrelated refusal cannot receive gate credit.
    _plg_reason=$(sed -n 's/.*(\(try_again: \)\{0,1\}agent_upgrading\(: [^)]*\)\{0,1\})$/agent_upgrading/p' "$PTY_EV/legacy-gate.out")
    [ "$_plg_reason" = agent_upgrading ] || {
        printf 'PTY old wire refusal lacks agent_upgrading: rc=%s spawn_lines=%s\n' "$_plg_rc" "$_plg_spawns" >&2
        cat "$PTY_EV/legacy-gate.out" >&2
        return 1
    }
    _pty_gate_pending || return 1
    [ "$(_pty_pid)" = "$_plg_pid" ] && [ "$(_pty_ticks "$_plg_pid")" = "$_plg_ticks" ] || return 1
    jq -n --argjson rc "$_plg_rc" --argjson spawns "$_plg_spawns" --arg reason "$_plg_reason" \
        --argjson pid "$_plg_pid" --argjson ticks "$_plg_ticks" --slurpfile marker "$PTY_EV/gate-marker.json" \
        '{rc:$rc,reason:$reason,spawn_lines:$spawns,effect_absent:true,pending:($marker[0].state=="pending"),candidate_unchanged:true,pid:$pid,start_ticks:$ticks,marker:$marker[0]}'  > "$PTY_EV/legacy-gate.json" || return 1
    _pty_legacy_gate_valid "$PTY_EV/legacy-gate.json" || return 1
    # Heal commit persistence only after admission evidence. Reap staging before
    # instance teardown rather than leaving an outer docker exec behind.
    dexec agt1 -- chmod 0755 /home/sim/.local/bin || return 1
    PTY_COMMIT_FAULT=0
    wait "$PTY_JOB"; _plg_upgrade_rc=$?
    printf '%s\n' "$_plg_upgrade_rc" > "$PTY_EV/legacy-gate-command.rc"
    [ "$_plg_upgrade_rc" = 0 ]
}
_pty_legacy_quadrants() {
    # Each compatibility supply is independent; replacing a binary underneath
    # an existing workload would turn a fallback baseline into another fault.
    _plq_parent=$PTY_EV
    for _plq_kind in old-agent old-broker; do
        "$SIM" nuke || return 1
        "$SIM" up --brokers 1 --agents 1 --ctl 1 || return 1
        # origin: hot-update S4 plan §9.2 / T48; drill-run3 L. Provision the
        # current fixture first: the previous offline reconciler cannot read
        # its ping_interval/ping_max directives. The fallback workload still
        # starts only after the real previous broker has replaced it.
        "$SIM" init brk1 || return 1
        if [ "$_plq_kind" = old-broker ]; then
            sctl brk1 stop tether-broker || return 1
            d cp "$HERE/vendor/tether-prev" "$(ctr_name brk1):/usr/local/bin/tether" || return 1
            dexec brk1 -- chmod 0755 /usr/local/bin/tether || return 1
            sctl brk1 start tether-broker || return 1
            poll_until 60 1 "previous broker ready" -- _pty_broker_ready || return 1
        fi
        "$SIM" session "$SID" --pin "$PIN" || return 1
        "$SIM" agent-join agt1 --session "$SID" --pin "$PIN" || return 1
        if [ "$_plq_kind" = old-agent ]; then _pty_install_previous || return 1; fi
        PTY_FIXTURE_SCENARIO="L-$_plq_kind"
        PTY_EV="$_plq_parent/$_plq_kind"
        mkdir -p "$PTY_EV" || return 1
        PTY_LOGS_CAPTURED=0
        pty_fixture_start "$PTY_EV" "$PTY_NONCE" /usr/local/bin/tether || return 1
        poll_until 35 1 "new ctl compatibility baseline" -- _pty_baseline || return 1
        _pty_checkpoint resize-before || return 1
        _pty_window start compatibility || return 1
        _pty_checkpoint compatibility || return 1
        _pty_window end compatibility || return 1
        _pty_checkpoint resize-after || return 1
        _pty_finish || return 1
        PTY_EV=$_plq_parent
    done
    # Restore fresh current supply for the separate first-migration gate arm.
    "$SIM" nuke || return 1
    "$SIM" up --brokers 1 --agents 1 --ctl 1 || return 1
    "$SIM" init brk1 || return 1
    upgradecfg_allow_broker brk1 _pty_broker_ready || return 1
    "$SIM" session "$SID" --pin "$PIN" || return 1
    "$SIM" agent-join agt1 --session "$SID" --pin "$PIN" || return 1
    _pty_previous_supply || return 1
    artifact_up "$INSTANCE" "$HERE/vendor/tether-next" pty-next.tar.gz || return 1
    ingress_trust_inject agt1 || return 1
    upgradecfg_allow_agent agt1 _pty_agent_ready
}
_pty_finish() {
    _pty_checkpoint after || return 1
    pty_fixture_finish || return 1
    _pty_collect || return 1
    case "${PTY_FIXTURE_SCENARIO:-}" in
        L-old-agent|L-old-broker)
            python3 -I -B "$PROBE" trace --role compatibility --evidence "$PTY_EV" --nonce "$PTY_NONCE"
            ;;
        *) _pty_continuity ;;
    esac
}
_pty_cleanup() {
    # Never finish/kill the fixture to make an oracle pass. The outer instance
    # teardown owns emergency cleanup if the original ctl has not ended.
    if [ "$PTY_STARTED" = 1 ] && [ "$PTY_LOGS_CAPTURED" = 0 ]; then
        pty_collect_logs >/dev/null 2>&1 || warn "PTY diagnostic log capture failed"
    fi
    if [ "${PTY_COMMIT_FAULT:-0}" = 1 ]; then
        dexec agt1 -- chmod 0755 /home/sim/.local/bin || warn "commit fault cleanup failed"
    fi
    fault_cleanup_all >/dev/null 2>&1 || true
    artifact_down >/dev/null 2>&1 || true
}


_pty_prepare_fixture() {
    assert_setup "PTY driver is supplied by P14" test -r "$HERE/drills/lib/hot_update_pty.sh"
    . "$HERE/drills/lib/hot_update_pty.sh"
    PTY_EV=$(mktemp -d "${SIM_EVIDENCE_DIR:-/tmp}/hot-update-pty.${ARM:-x}.XXXXXX") || setup_fail "create PTY evidence"
    PTY_NONCE=$(python3 -c 'import secrets; print(secrets.token_hex(16))') || setup_fail "create PTY nonce"
    assert_setup "independent raw-byte PTY positive and negative controls" python3 "$PROBE" selftest --evidence "$PTY_EV" --nonce "$PTY_NONCE"
    cp "$HERE/vendor/tether-source.json" "$PTY_EV/tether-source.json" || setup_fail "retain staged source identity"
    "$SIM" nuke >/dev/null 2>&1 || true
    assert_setup "up independent N1 broker agent and ctl" "$SIM" up --brokers 1 --agents 1 --ctl 1
    assert_setup "init brk1 N1" "$SIM" init brk1
    assert_setup "operator broker artifact allowlist" upgradecfg_allow_broker brk1 _pty_broker_ready
    if [ "${PTY_NEGATIVE_CONTROL:-0}" = 1 ]; then
        [ "$#" = 2 ] && [ "$2" = old-old-old ] || setup_fail "explicit historical PTY control stack is required"
        assert_setup "fixed PTY source" sim_prev_source_ok "$(sim_fixed_prev_tag)" # origin: S8 internal review rv-n1 F6
        # origin: S8 internal review s8-hermetic F9. Select the ctl path in
        # the parent; assert_setup captures only remote installation status.
        LEGACY_CTL_BIN=$1
        assert_setup "old PTY broker and upgrade ctl before business" upgrade_legacy_stack "$HERE/vendor/tether-prev" "$HERE/vendor/tether-prev-source.json" "$1" "$PTY_EV"
        assert_setup "old PTY broker ready" poll_until 60 1 "old broker" -- _pty_broker_ready
    fi
    assert_setup "create session and activate ctl" "$SIM" session "$SID" --pin "$PIN"
    assert_setup "onboard original agent" "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
    _pty_step_rc=0; _pty_step_run artifact_up "$INSTANCE" "$HERE/vendor/tether-next" pty-next.tar.gz || _pty_step_rc=$?
    assert_setup "serve real distinct candidate" _pty_step_ok "$_pty_step_rc"
    assert_setup "artifact TLS trust" ingress_trust_inject agt1
    assert_setup "operator agent allowlist and setup restart" upgradecfg_allow_agent agt1 _pty_agent_ready
    PTY_CTL_BIN=/usr/local/bin/tether
    if [ "$ARM" = L ] || [ "${PTY_NEGATIVE_CONTROL:-0}" = 1 ]; then
        _pty_step_rc=0; _pty_step_run _pty_previous_supply || _pty_step_rc=$?
        assert_setup "real old ctl provenance and binary SHA" _pty_step_ok "$_pty_step_rc"
        PTY_CTL_BIN=$PREV_BIN
    fi
    if [ "${PTY_NEGATIVE_CONTROL:-0}" = 1 ]; then
        _pty_step_rc=0; _pty_step_run _pty_install_previous || _pty_step_rc=$?
        assert_setup "P real previous exporter, never a current binary with an old label" _pty_step_ok "$_pty_step_rc"
    fi
    PTY_PID=$(_pty_pid) || setup_fail "read agent PID"
    PTY_TICKS=$(_pty_ticks "$PTY_PID") || setup_fail "read original start ticks"
    PTY_OLD_SHA=$(_pty_sha "$PTY_PID") || setup_fail "hash running image"
    PTY_NEW_SHA=$(sha256sum "$HERE/vendor/tether-next" | awk '{print $1}') || setup_fail "hash candidate"
    PTY_CURSOR=$(sim_agent_slog_cursor agt1) || setup_fail "capture fresh agent log cursor"
    _ps_served=$(dexec "$ARTIFACT_NODE" -- sha256sum /artifacts/pty-next.tar.gz | awk '{print $1}') || setup_fail "hash served archive"
    jq -n --arg old "$PTY_OLD_SHA" --arg new "$PTY_NEW_SHA" --arg artifact "$ART_SHA" --arg served "$_ps_served" \
        --argjson pid "$PTY_PID" --argjson ticks "$PTY_TICKS" \
        '{old_sha:$old,new_sha:$new,artifact_sha:$artifact,served_sha:$served,pid:$pid,start_ticks:$ticks}' > "$PTY_EV/supply.json" || setup_fail "retain real supply"
    assert_setup "running image and served candidate are distinct and identified" _pty_supply_valid
    PTY_FIXTURE_SCENARIO=$ARM
    PTY_LOGS_CAPTURED=0
    _pty_step_rc=0; _pty_step_run pty_fixture_start "$PTY_EV" "$PTY_NONCE" "$PTY_CTL_BIN" || _pty_step_rc=$?
    assert_setup "real controlling tty and one original ctl" _pty_step_ok "$_pty_step_rc"
    PTY_STARTED=1
    if [ "$ARM" = L ]; then
        _pty_step_rc=0; _pty_step_run _pty_checkpoint before-attach || _pty_step_rc=$?
        assert_setup "L pause original legacy run before attach" _pty_step_ok "$_pty_step_rc"
        _pty_step_rc=0; _pty_step_run _pty_refusal preattach || _pty_step_rc=$?
        assert_ok "L pre-attach legacy admission refuses upgrade without disk mutation" _pty_step_ok "$_pty_step_rc"
        _pty_step_rc=0; _pty_step_run _pty_checkpoint attached || _pty_step_rc=$?
        assert_setup "L release original attach and exercise legacy resize and dual publish" _pty_step_ok "$_pty_step_rc"
    fi
    _pty_step_rc=0; _pty_step_run poll_until 35 1 "PTY baseline" -- _pty_baseline || _pty_step_rc=$?
    assert_setup "two-sided nonempty original PTY progress before injection" _pty_step_ok "$_pty_step_rc"
}
_pty_upgrade_and_finish() {
    _pty_step_rc=0; _pty_step_run _pty_checkpoint resize-before || _pty_step_rc=$?
    assert_setup "resize original terminal before cut" _pty_step_ok "$_pty_step_rc"
    if [ "$ARM" = P ]; then
        # origin: hot-update S4 plan §9.3 / S5 T48 / DRILL-RUN 39.P.
        # The historical pin requires output loss, so carry an observed
        # finite backlog through bare exec rather than racing a paced writer.
        _pty_step_rc=0; _pty_step_run _pty_checkpoint queued || _pty_step_rc=$?
        assert_setup "P original read barrier and nonempty output backlog before bare exec" _pty_step_ok "$_pty_step_rc"
    fi
    if [ "$ARM" = A ]; then
        _pty_step_rc=0; _pty_step_run _pty_mixed_prepare || _pty_step_rc=$?
        assert_setup "supply strict mixed workload" _pty_step_ok "$_pty_step_rc"
        assert_setup "stable 30 second strict workload calibration" dexec -u sim ctl1 -- env HOME=/home/sim \
            python3 "$MIXED_REMOTE/hot_update_mixed_probe.py" run --strict --evidence "$MIXED_ROOT-calibration" \
            --rate "$HOT_UPDATE_PROBE_RATE" --duration 30 --node agt1 --nonce "$PTY_NONCE"
        assert_setup "retain calibration data" d cp "$(ctr_name ctl1):$MIXED_ROOT-calibration/." "$PTY_EV/calibration/"
        assert_setup "retain calibration side effects" d cp "$(ctr_name agt1):/tmp/hot-update-mixed-$PTY_NONCE/ledger" "$PTY_EV/calibration/ledger"
        assert_setup "calibration has zero failures and the declared schedule rate" python3 "$MIXED" judge --strict \
            --evidence "$PTY_EV/calibration" --ledger "$PTY_EV/calibration/ledger" --mode calibrate
        # A different nonce isolates upgrade effects from calibration effects.
        PTY_MIXED_NONCE=$(python3 -c 'import secrets; print(secrets.token_hex(16))') || setup_fail "mixed upgrade nonce"
        assert_setup "fresh mixed effect ledger" dexec -u sim agt1 -- mkdir -p "/tmp/hot-update-mixed-$PTY_MIXED_NONCE"
        dexec -u sim ctl1 -- env HOME=/home/sim python3 "$MIXED_REMOTE/hot_update_mixed_probe.py" run --strict \
            --evidence "$MIXED_ROOT" --rate "$HOT_UPDATE_PROBE_RATE" --node agt1 --nonce "$PTY_MIXED_NONCE" \
            --until-mark activated --tail 10 --max-duration 180 > "$PTY_EV/mixed.run" 2>&1 &
        MIXED_JOB=$!
        _pty_step_rc=0; _pty_step_run poll_until 20 1 "mixed started" -- _pty_mixed_started || _pty_step_rc=$?
        assert_setup "mixed workload started before upgrade" _pty_step_ok "$_pty_step_rc"
    fi
    _pty_step_rc=0; _pty_step_run _pty_window start image || _pty_step_rc=$?
    assert_setup "retain image fault start on original ctl clock" _pty_step_ok "$_pty_step_rc"
    _pty_upgrade 1 & PTY_JOB=$!
    if [ "$ARM" = A ]; then
        PTY_CUT_SEEN=0
        _pty_step_rc=0; _pty_step_run poll_until_fixed 150 0.1 "image switch" -- _pty_mixed_switch || _pty_step_rc=$?
        assert_ok "strict workload observes the actual cut and activation" _pty_step_ok "$_pty_step_rc"
        wait "$MIXED_JOB"; _pa_mixed_rc=$?
        assert_ok "strict workload completes through activation" _pty_step_ok "$_pa_mixed_rc" "$PTY_EV/mixed.run"
        _pty_step_rc=0; _pty_step_run _pty_mixed_collect || _pty_step_rc=$?
        assert_ok "strict new requests and effects meet the five second budget" _pty_step_ok "$_pty_step_rc"
    fi
    wait "$PTY_JOB"; _pa_rc=$?
    assert_ok "upgrade command succeeds" _pty_step_ok "$_pa_rc" "$PTY_EV/upgrade-1.out"
    _pty_step_rc=0; _pty_step_run poll_until 30 1 "PTY candidate committed" -- _pty_record_hop "$PTY_NEW_SHA" committed hop-1.json || _pty_step_rc=$?
    assert_setup "fault really execs candidate under original PID and commits" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_window end image || _pty_step_rc=$?
    assert_setup "retain observed image activation window" _pty_step_ok "$_pty_step_rc"
    if [ "$ARM" = P ]; then
        assert_setup "historical PTY exec kernel evidence" upgrade_legacy_snapshot after "$PTY_EV"
        assert_setup "historical PTY exec is required" upgrade_legacy_exec_required "$PTY_EV" agent
        _pty_step_rc=0; _pty_step_run _pty_checkpoint active-release || _pty_step_rc=$?
        assert_setup "P release original read barrier after actual bare exec" _pty_step_ok "$_pty_step_rc"
        _pty_step_rc=0; _pty_step_run _pty_checkpoint post-fault || _pty_step_rc=$?
        assert_setup "P collect post-fault death without requiring a live foreground" _pty_step_ok "$_pty_step_rc"
        _pty_step_rc=0; _pty_step_run pty_fixture_finish || _pty_step_rc=$?
        assert_setup "P retain original waitpid and raw evidence even if foreground died" _pty_step_ok "$_pty_step_rc"
        _pty_step_rc=0; _pty_step_run _pty_collect || _pty_step_rc=$?
        assert_setup "P complete readable four-stream logs and resource census" _pty_step_ok "$_pty_step_rc"
    else
        _pty_step_rc=0; _pty_step_run _pty_checkpoint resize-after || _pty_step_rc=$?
        assert_setup "resize the original terminal after cut" _pty_step_ok "$_pty_step_rc"
        _pty_step_rc=0; _pty_step_run _pty_finish || _pty_step_rc=$?
        assert_ok "original PTY exact bytes foreground rc23 and resources survive upgrade" _pty_step_ok "$_pty_step_rc"
    fi
}

# 39.P imports the same driver and invokes it in its own manifest branch.
if [ "${PTY_LIBRARY_ONLY:-0}" = 1 ]; then return 0; fi
drill_begin "36-hot-update-pty [arm ${ARM:-?}]"
drill_install_traps _pty_cleanup
_pty_prepare_fixture
case "${ARM:?}" in
A)
    _pty_upgrade_and_finish
    ;;
W)
    # origin: hot-update S4 internal review rv-drill DRILL-1. Only real
    # local ioctl actions can supply the resize receipts across this fault.
    _pty_step_rc=0; _pty_step_run _pty_checkpoint resize-before || _pty_step_rc=$?
    assert_setup "W resize original terminal before blackhole" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_checkpoint queued || _pty_step_rc=$?
    assert_setup "W commit KA and retain accepted pending input plus unACKed output" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_window start watchdog || _pty_step_rc=$?
    assert_setup "W retain blackhole start on original ctl clock" _pty_step_ok "$_pty_step_rc"
    assert_setup "W inject new connection SYN blackhole" fault_synblock_on agt1 4222
    _pty_step_rc=0; _pty_step_run _pty_blackhole agt1 brk1 4222 || _pty_step_rc=$?
    assert_setup "W blackhole is timeout rather than refusal" _pty_step_ok "$_pty_step_rc"
    PTY_FAULT_START=$(python3 -c 'import time; print(time.monotonic())')
    PTY_DISCONNECTED=1
    _pty_upgrade watchdog & PTY_JOB=$!
    _pty_step_rc=0; _pty_step_run poll_until 45 1 "active pending PTY" -- _pty_record_hop "$PTY_NEW_SHA" pending hop-active.json || _pty_step_rc=$?
    assert_ok "W candidate activates without registering" _pty_step_ok "$_pty_step_rc"
    # P18 reads current owners at the final rollback cut. No polling request
    # can manufacture a state receipt while this candidate is disconnected.
    _pty_step_rc=0; _pty_step_run _pty_checkpoint active-release || _pty_step_rc=$?
    assert_setup "W release queued input on the current candidate" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run poll_until 60 1 "original node offline" -- _pty_node_offline || _pty_step_rc=$?
    assert_ok "W original node crosses node_offline" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run poll_until "$((HOT_UPDATE_REGISTER_DEADLINE_S + 90))" 2 "PTY rollback" -- _pty_record_hop "$PTY_OLD_SHA" rolled_back hop-rollback.json || _pty_step_rc=$?
    assert_ok "W real 120 second watchdog restores previous image" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_checkpoint rollback || _pty_step_rc=$?
    assert_setup "W retain current state before heal" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_fault_receipt || _pty_step_rc=$?
    assert_setup "W retain measured watchdog interval and consumed prev" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_active_progress || _pty_step_rc=$?
    assert_ok "W candidate current cut proves actual kernel progress" _pty_step_ok "$_pty_step_rc"
    assert_ok "W current state is preserved and KA was not granted again" _pty_current_state
    assert_ok "W rollback has a new generation and the exact watchdog cause" _pty_watchdog_valid
    assert_setup "W heal after rollback evidence" fault_synblock_off agt1
    PTY_DISCONNECTED=0
    _pty_step_rc=0; _pty_step_run poll_until 60 1 "previous online" -- _pty_agent_ready || _pty_step_rc=$?
    assert_ok "W previous image registers" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_window end watchdog || _pty_step_rc=$?
    assert_setup "W retain actual recovery window through registration" _pty_step_ok "$_pty_step_rc"
    wait "$PTY_JOB"; printf '%s\n' "$?" > "$PTY_EV/watchdog-command.rc"
    _pty_step_rc=0; _pty_step_run _pty_checkpoint resize-after || _pty_step_rc=$?
    assert_setup "W resize original terminal after heal" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_finish || _pty_step_rc=$?
    assert_ok "W original ctl and handle finish after heal with exact bytes and rc23" _pty_step_ok "$_pty_step_rc"
    ;;
C)
    _pty_step_rc=0; _pty_step_run _pty_checkpoint resize-before || _pty_step_rc=$?
    assert_setup "C1 resize original terminal before carrier fault" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_checkpoint carrier-before || _pty_step_rc=$?
    assert_setup "C1 carrier starts on original PTY" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_window start carrier || _pty_step_rc=$?
    assert_setup "C1 retain carrier fault start" _pty_step_ok "$_pty_step_rc"
    assert_setup "C1 stop actual NATS server" sctl brk1 stop nats-server
    # Fault duration is deliberate: > production redialAfter, not a ready sleep.
    sleep 25
    assert_setup "C1 restart NATS server after 25 second outage" sctl brk1 start nats-server
    _pty_step_rc=0; _pty_step_run poll_until 60 1 "session rebuilt" -- _pty_rebuild_seen || _pty_step_rc=$?
    assert_ok "C1 agent actually rebuilds session" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_window end carrier || _pty_step_rc=$?
    assert_setup "C1 retain observed session rebuild window" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_checkpoint resize-after || _pty_step_rc=$?
    assert_setup "C1 resize original terminal after carrier recovery" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_checkpoint carrier-after || _pty_step_rc=$?
    assert_ok "C1 raw duplex continues with original terminal and KA" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_finish || _pty_step_rc=$?
    assert_ok "C1 exact original bytes and autonomous ctl rc23" _pty_step_ok "$_pty_step_rc"
    PTY_FIXTURE_SCENARIO=C2
    # A finished C1 must not suppress emergency log capture for the new fixture.
    PTY_LOGS_CAPTURED=0
    _pty_step_rc=0; _pty_step_run pty_fixture_start "$PTY_EV/c2" "$PTY_NONCE" "$PTY_CTL_BIN" || _pty_step_rc=$?
    assert_setup "C2 independent draining leader tail FIN canonical and raw fixture" _pty_step_ok "$_pty_step_rc"
    _pc_parent=$PTY_EV; PTY_EV="$PTY_EV/c2"
    _pty_step_rc=0; _pty_step_run poll_until 35 1 "tail fixture baseline" -- _pty_baseline || _pty_step_rc=$?
    assert_setup "C2 nonempty input and output" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_checkpoint resize-before || _pty_step_rc=$?
    assert_setup "C2 resize before draining cut" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_checkpoint canonical || _pty_step_rc=$?
    assert_setup "C2 canonical Ctrl-D and Ctrl-C have measured remote effects" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_checkpoint draining || _pty_step_rc=$?
    assert_setup "C2 leader exits before more than 200ms of slave tail" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_window start draining || _pty_step_rc=$?
    assert_setup "C2 retain image fault start" _pty_step_ok "$_pty_step_rc"
    _pty_upgrade tail; _pc_upgrade_rc=$?
    assert_ok "C2 upgrade during draining tail succeeds" _pty_step_ok "$_pc_upgrade_rc" "$PTY_EV/upgrade-draining.out"
    _pty_step_rc=0; _pty_step_run poll_until 30 1 "tail image committed" -- _pty_record_hop "$PTY_NEW_SHA" committed hop-tail.json || _pty_step_rc=$?
    assert_ok "C2 draining terminal crosses an actual image activation" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_window end draining || _pty_step_rc=$?
    assert_setup "C2 retain observed draining image cut" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_checkpoint resize-after || _pty_step_rc=$?
    assert_setup "C2 resize after draining cut" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_checkpoint tail-after-cut || _pty_step_rc=$?
    assert_setup "C2 retain real tail bytes intersecting the cut" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_finish || _pty_step_rc=$?
    assert_ok "C2 tail FIN Ctrl-D Ctrl-C and terminal outcome are complete" _pty_step_ok "$_pty_step_rc"
    PTY_EV=$_pc_parent
    ;;
L)
    # origin: hot-update S4 internal review rv-drill V1. Bracket the refusal
    # with actual geometry changes on the original old ctl terminal.
    _pty_step_rc=0; _pty_step_run _pty_checkpoint resize-before || _pty_step_rc=$?
    assert_setup "L resize original terminal before refusal" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_window start refusal || _pty_step_rc=$?
    assert_setup "L retain refusal window start" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_refusal running || _pty_step_rc=$?
    assert_ok "L running legacy terminal defers upgrade and keeps original disk" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_window end refusal || _pty_step_rc=$?
    assert_setup "L retain observed unchanged-image refusal window" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_checkpoint resize-after || _pty_step_rc=$?
    assert_setup "L resize original terminal after refusal" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_checkpoint legacy-controls || _pty_step_rc=$?
    assert_ok "L legacy resize and dual publish preserve exact byte trace" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_finish || _pty_step_rc=$?
    assert_ok "L original old ctl finishes before provisioning other quadrants" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_legacy_quadrants || _pty_step_rc=$?
    assert_setup "L new ctl old agent and old broker fallback quadrants" _pty_step_ok "$_pty_step_rc"
    _pty_step_rc=0; _pty_step_run _pty_legacy_gate || _pty_step_rc=$?
    assert_ok "L gate closed legacy75 and zero spawn receipts" _pty_step_ok "$_pty_step_rc"
    ;;
*) setup_fail "unknown PTY arm '$ARM'" ;;
esac
log "PTY receipts retained: $PTY_EV; 500ms freeze is calibration, raw maxima retained"
drill_end
