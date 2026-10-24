#!/bin/sh
# origin: hot-update S4 plan §9.1 / §12.1.
# Source after docker.sh; HERE is the simcluster root, as in 34/35.
# Log API supplied by logs.sh:
# sim_log_snapshot <stream> <node> <raw-destination> <metadata-destination>
# writes complete raw bytes and {inode,length,read_ok:true}. Journald needs a
# stable journal identity and full cursor-derived bytes. Missing/rotated/read
# failures return nonzero. No guessed inode or tail-N fallback is permitted.
. "$HERE/drills/lib/logs.sh"

pty_fixture_start() {
    [ "$#" = 3 ] || return 2
    PTY_EVIDENCE=$1
    PTY_NONCE=$2
    PTY_CTL_BINARY=$3
    PTY_PROBE="$HERE/drills/lib/hot_update_pty_probe.py"
    PTY_CTL_NODE=${PTY_CTL_NODE:-ctl1}
    PTY_AGENT_NODE=${PTY_AGENT_NODE:-${NID:-agt1}}
    PTY_REMOTE_ROOT="/tmp/hot-update-pty-$PTY_NONCE-$(basename "$PTY_EVIDENCE")"
    PTY_REMOTE_PROBE="/tmp/hot-update-pty-$PTY_NONCE.py"
    case "$PTY_CTL_BINARY" in /*) ;; *) return 2 ;; esac
    python3 -I -B -c 'import re,sys; sys.exit(0 if re.fullmatch("[0-9a-f]{32}",sys.argv[1]) else 2)' "$PTY_NONCE" || return 2
    mkdir -p "$PTY_EVIDENCE" || return 2
    [ ! -e "$PTY_EVIDENCE/runner.pid" ] || return 2
    mkdir -p "$PTY_EVIDENCE/checkpoints" "$PTY_EVIDENCE/agent" || return 2
    pty_collect_logs before || return 2
    PTY_EVIDENCE=$(cd "$PTY_EVIDENCE" && pwd) || return 2
    if [ "${PTY_LOCAL_TRACE:-0}" != 1 ]; then
        # Version output is captured from the actual three binaries, before
        # admission. A legacy started audit separately proves selected path.
        for _pty_role in ctl agent broker; do
            case "$_pty_role" in
                ctl) _pty_node=$PTY_CTL_NODE; _pty_binary=$PTY_CTL_BINARY ;;
                agent) _pty_node=$PTY_AGENT_NODE; _pty_binary=/proc/$(dexec "$PTY_AGENT_NODE" -- systemctl show tether-agent -p MainPID --value)/exe ;;
                broker) _pty_node=${PTY_BROKER_NODE:-brk1}; _pty_binary=/proc/$(dexec "${PTY_BROKER_NODE:-brk1}" -- systemctl show tether-broker -p MainPID --value)/exe ;;
            esac
            dexec "$_pty_node" -- "$_pty_binary" version > "$PTY_EVIDENCE/endpoint-$_pty_role.version" || return 2
            dexec "$_pty_node" -- sha256sum "$_pty_binary" > "$PTY_EVIDENCE/endpoint-$_pty_role.sha" || return 2
        done
    fi
    if [ "${PTY_LOCAL_TRACE:-0}" = 1 ]; then
        command -v pty_local_trace >/dev/null 2>&1 || return 2
        pty_local_trace "$PTY_EVIDENCE" "$PTY_NONCE" "$PTY_CTL_BINARY" > "$PTY_EVIDENCE/runner.stdout" 2> "$PTY_EVIDENCE/runner.stderr" &
    else
        for _pty_node in "$PTY_CTL_NODE" "$PTY_AGENT_NODE"; do
            d cp "$PTY_PROBE" "$(ctr_name "$_pty_node"):$PTY_REMOTE_PROBE" || return 2
            dexec "$_pty_node" -- mkdir -p "$PTY_REMOTE_ROOT" || return 2
            dexec "$_pty_node" -- chown sim:sim "$PTY_REMOTE_ROOT" || return 2
        done
        set -- trace --role ctl --evidence "$PTY_REMOTE_ROOT" --nonce "$PTY_NONCE" --ctl-binary "$PTY_CTL_BINARY" \
            --node "$PTY_AGENT_NODE" --scenario "${PTY_FIXTURE_SCENARIO:-A}" --remote-probe "$PTY_REMOTE_PROBE" --remote-evidence "$PTY_REMOTE_ROOT" \
            --timeout "${PTY_TIMEOUT:-300}"
        if [ "${PTY_FIXTURE_SCENARIO:-}" = W ]; then PTY_INPUT_BYTES=32768; else PTY_INPUT_BYTES=; fi
        if [ -n "${PTY_INPUT_BYTES:-}" ]; then set -- "$@" --input-bytes "$PTY_INPUT_BYTES"; fi
        dexec -u sim "$PTY_CTL_NODE" -- python3 -I -B "$PTY_REMOTE_PROBE" "$@" > "$PTY_EVIDENCE/runner.stdout" 2> "$PTY_EVIDENCE/runner.stderr" &
    fi
    PTY_FIXTURE_JOB=$!
    printf '%s\n' "$PTY_FIXTURE_JOB" > "$PTY_EVIDENCE/runner.pid"
}

pty_checkpoint() {
    [ "$#" = 1 ] && [ -n "${PTY_EVIDENCE:-}" ] || return 2
    _pty_checkpoint_name=$1
    if [ "$_pty_checkpoint_name" = canonical ]; then
        dexec -u sim "$PTY_CTL_NODE" -- touch "$PTY_REMOTE_ROOT/canonical.request" || return 2
        for _pty_canonical_phase in ready eof complete; do
            dexec "$PTY_AGENT_NODE" -- python3 -I -B -c '
import json,sys,time
from pathlib import Path
p,phase,nonce=Path(sys.argv[1]),sys.argv[2],sys.argv[3]
deadline=time.monotonic()+5
while True:
    r=json.loads(p.read_text()) if p.exists() else {}
    if r.get("nonce")==nonce and r.get("phase")==phase: break
    if time.monotonic()>deadline: raise SystemExit("remote canonical effect not observed: "+phase)
    time.sleep(.01)
if phase=="complete" and (r.get("eof") is not True or r.get("sigint")!=1):
    raise SystemExit("canonical EOF/SIGINT effect absent")
' "$PTY_REMOTE_ROOT/canonical.remote.json" "$_pty_canonical_phase" "$PTY_NONCE" || return 2
            case "$_pty_canonical_phase" in ready) _pty_key=eof ;; eof) _pty_key=sigint ;; complete) _pty_key=complete ;; esac
            dexec -u sim "$PTY_CTL_NODE" -- touch "$PTY_REMOTE_ROOT/canonical.$_pty_key" || return 2
        done
    fi
    # The pre-attach phase has no remote child yet. Read its kernel barrier
    # receipt before any operation that requires a foreground trace/census.
    case "$_pty_checkpoint_name" in
        before-attach|attached)
            if [ "$_pty_checkpoint_name" = attached ]; then
                dexec -u sim "$PTY_CTL_NODE" -- touch "$PTY_REMOTE_ROOT/attach.release" || return 2
            fi
            dexec "$PTY_CTL_NODE" -- python3 -I -B -c '
import json,sys,time
from pathlib import Path
root, name, nonce=Path(sys.argv[1]),sys.argv[2],sys.argv[3]
deadline=time.monotonic()+10
while not (root/(name+".json")).exists():
    if time.monotonic()>deadline: raise SystemExit("attach barrier receipt absent")
    time.sleep(.01)
r=json.loads((root/(name+".json")).read_text())
if r["nonce"]!=nonce or (name=="before-attach" and (r["syscall"]!="ioctl" or r["fd"]!=0 or r["request"]!=0x5402)):
    raise SystemExit("foreign or unobserved pre-attach boundary")
' "$PTY_REMOTE_ROOT" "$_pty_checkpoint_name" "$PTY_NONCE" || return 2
            d cp "$(ctr_name "$PTY_CTL_NODE"):$PTY_REMOTE_ROOT/$_pty_checkpoint_name.json" "$PTY_EVIDENCE/checkpoints/$_pty_checkpoint_name.json" || return 2
            return 0
            ;;
    esac
    # origin: hot-update S4 plan §9.2. Geometry is exercised by an actual
    # ioctl, then measured independently in the foreground trace.
    case "$_pty_checkpoint_name" in
        resize-before) PTY_COLS=91; PTY_ROWS=31 ;;
        resize-after) PTY_COLS=111; PTY_ROWS=37 ;;
        legacy-controls) PTY_COLS=101; PTY_ROWS=29 ;;
        *) PTY_COLS=; PTY_ROWS= ;;
    esac
    if [ "$_pty_checkpoint_name" = queued ]; then
        # origin: hot-update S4 plan §9.2 / T48; drill-run4 W. Pausing only
        # remote input leaves the ctl free to ACK every output byte before cut.
        # Observe a real local read barrier and fill beyond the kernel buffers;
        # the image-state assertion still requires actual unACKed replay bytes.
        dexec -u sim "$PTY_CTL_NODE" -- touch "$PTY_REMOTE_ROOT/pause-output" || return 2
        dexec "$PTY_CTL_NODE" -- python3 -I -B -c '
import json,sys,time
from pathlib import Path
root,nonce=Path(sys.argv[1]),sys.argv[2]
deadline=time.monotonic()+5
while not (root/"output-paused.json").exists():
    if time.monotonic()>deadline: raise SystemExit("original output read barrier not observed")
    time.sleep(.01)
r=json.loads((root/"output-paused.json").read_text())
if r["nonce"]!=nonce: raise SystemExit("foreign output read barrier")
print(r["received"])
' "$PTY_REMOTE_ROOT" "$PTY_NONCE" > "$PTY_EVIDENCE/output-paused.bytes" || return 2
        d cp "$(ctr_name "$PTY_CTL_NODE"):$PTY_REMOTE_ROOT/output-paused.json" "$PTY_EVIDENCE/output-paused.json" || return 2
        dexec -u sim "$PTY_AGENT_NODE" -- touch "$PTY_REMOTE_ROOT/pause-input" || return 2
        dexec "$PTY_AGENT_NODE" -- python3 -I -B -c '
import sys,time
from pathlib import Path
root,received=Path(sys.argv[1]),int(sys.argv[2])
deadline=time.monotonic()+10
while (root/"remote.sent").stat().st_size-received<128*1024:
    if time.monotonic()>deadline: raise SystemExit("nonempty unACKed output backlog not established")
    time.sleep(.01)
' "$PTY_REMOTE_ROOT" "$(cat "$PTY_EVIDENCE/output-paused.bytes")" || return 2
    elif [ "$_pty_checkpoint_name" = active-release ]; then
        dexec -u sim "$PTY_CTL_NODE" -- rm -f "$PTY_REMOTE_ROOT/pause-output" || return 2
        dexec -u sim "$PTY_AGENT_NODE" -- rm -f "$PTY_REMOTE_ROOT/pause-input" || return 2
    fi
    set -- trace --role checkpoint --name "$_pty_checkpoint_name" --nonce "$PTY_NONCE"
    # The receipt proves the local ioctl; actual remote geometry must still
    # come from remote.trace.json, not from the requested dimensions.
    if [ -n "${PTY_COLS:-}" ] || [ -n "${PTY_ROWS:-}" ]; then
        set -- "$@" --cols "${PTY_COLS:-}" --rows "${PTY_ROWS:-}"
    fi
    if [ "${PTY_LOCAL_TRACE:-0}" = 1 ]; then
        python3 -I -B "$PTY_PROBE" "$@" --evidence "$PTY_EVIDENCE" || return 2
        cp "$PTY_EVIDENCE/$_pty_checkpoint_name.json" "$PTY_EVIDENCE/checkpoints/$_pty_checkpoint_name.json" || return 2
    else
        dexec -u sim "$PTY_CTL_NODE" -- python3 -I -B "$PTY_REMOTE_PROBE" "$@" --evidence "$PTY_REMOTE_ROOT" || return 2
        d cp "$(ctr_name "$PTY_CTL_NODE"):$PTY_REMOTE_ROOT/$_pty_checkpoint_name.json" "$PTY_EVIDENCE/checkpoints/$_pty_checkpoint_name.json" || return 2
    fi
    pty_fixture_collect || return 2
    pty_agent_snapshot "$_pty_checkpoint_name" || return 2
    case "$_pty_checkpoint_name" in
        active|rollback)
            pty_collect_logs || return 2
            python3 -I -B "$PTY_PROBE" trace --role current --name "$_pty_checkpoint_name" \
                --evidence "$PTY_EVIDENCE" --nonce "$PTY_NONCE" || return 2
            ;;
        draining)
            dexec -u sim "$PTY_AGENT_NODE" -- touch "$PTY_REMOTE_ROOT/draining" || return 2
            dexec "$PTY_AGENT_NODE" -- python3 -I -B -c '
import json,time
from pathlib import Path
import sys
root=Path(sys.argv[1])
leader=json.loads((root/"leader.identity.json").read_text())["identity"]
before=(root/"remote.sent").stat().st_size
start=time.monotonic(); deadline=start+5; exited_at=None
while True:
    path=Path("/proc")/str(leader["pid"])/"stat"
    exited=not path.exists() or path.read_text().rsplit(")",1)[1].split()[0]=="Z"
    if exited and exited_at is None:
        exited_at=time.monotonic(); before=(root/"remote.sent").stat().st_size
    tail=time.monotonic()-exited_at if exited_at is not None else 0
    if exited and tail>.2 and (root/"remote.sent").stat().st_size>before:
        (root/"draining.receipt.json").write_text(json.dumps(dict(nonce=sys.argv[2],leader_exited=True,at=exited_at,tail_s=tail)))
        break
    if time.monotonic()>deadline: raise SystemExit("draining leader/tail not observed")
    time.sleep(.01)
' "$PTY_REMOTE_ROOT" "$PTY_NONCE" || return 2
            d cp "$(ctr_name "$PTY_AGENT_NODE"):$PTY_REMOTE_ROOT/draining.receipt.json" "$PTY_EVIDENCE/draining.receipt.json" || return 2
            ;;
        tail-after-cut)
            [ -r "$PTY_EVIDENCE/draining.receipt.json" ] || return 2
            ;;
    esac
}

# Live traces are atomically published by each collector. Raw files may still
# grow; only final collection is used for envelope hashes and byte partitions.
pty_fixture_collect() {
    rm -f "$PTY_EVIDENCE/runtime.receipts.json" || return 2
    [ "${PTY_LOCAL_TRACE:-0}" != 1 ] || return 0
    for _pty_name in ctl.sent ctl.received ctl.identity.json ctl.trace.json; do
        d cp "$(ctr_name "$PTY_CTL_NODE"):$PTY_REMOTE_ROOT/$_pty_name" "$PTY_EVIDENCE/$_pty_name" || return 2
    done
    for _pty_name in remote.sent remote.received remote.identity.json remote.trace.json; do
        d cp "$(ctr_name "$PTY_AGENT_NODE"):$PTY_REMOTE_ROOT/$_pty_name" "$PTY_EVIDENCE/$_pty_name" || return 2
    done
    # Obsolete sidecars must never survive a recollection. P18 sources are
    # acquired exclusively through the complete logs.sh snapshot window.
    rm -f "$PTY_EVIDENCE/runtime.receipts.json" || return 2
}

pty_agent_snapshot() {
    [ "$#" = 1 ] || return 2
    _pty_snapshot_name=$1
    _pty_owner=$(dexec "$PTY_AGENT_NODE" -- systemctl show tether-agent -p MainPID --value) || return 2
    set -- trace --role agent --evidence "$PTY_REMOTE_ROOT" --nonce "$PTY_NONCE" --pid "$_pty_owner" \
        --marker /home/sim/.local/bin/.tether-upgrade.json --foreground "$PTY_REMOTE_ROOT/remote.identity.json"
    if [ -r "$PTY_EVIDENCE/agent/before.json" ]; then
        d cp "$PTY_EVIDENCE/agent/before.json" "$(ctr_name "$PTY_AGENT_NODE"):$PTY_REMOTE_ROOT/baseline.json" || return 2
        # The census baseline is the resources object, not the enclosing image.
        dexec "$PTY_AGENT_NODE" -- python3 -I -B -c 'import json,sys; from pathlib import Path; p=Path(sys.argv[1]); p.write_text(json.dumps(json.loads(p.read_text())["resources"]))' \
            "$PTY_REMOTE_ROOT/baseline.json" || return 2
        set -- "$@" --baseline "$PTY_REMOTE_ROOT/baseline.json"
    fi
    dexec "$PTY_AGENT_NODE" -- python3 -I -B "$PTY_REMOTE_PROBE" "$@" > "$PTY_EVIDENCE/agent/$_pty_snapshot_name.tmp" || return 2
    mv "$PTY_EVIDENCE/agent/$_pty_snapshot_name.tmp" "$PTY_EVIDENCE/agent/$_pty_snapshot_name.json" || return 2
    # Management is the real broker receipt, distinct from foreground waitpid.
    if [ "${PTY_DISCONNECTED:-0}" = 0 ]; then
        "$SIM" ctl -- ps -a --json > "$PTY_EVIDENCE/agent/$_pty_snapshot_name.ps.json" || return 2
    fi
}

pty_fixture_finish() {
    [ -n "${PTY_FIXTURE_JOB:-}" ] || return 2
    if [ "${PTY_LOCAL_TRACE:-0}" = 1 ]; then
        : > "$PTY_EVIDENCE/finish"
    else
        dexec -u sim "$PTY_CTL_NODE" -- touch "$PTY_REMOTE_ROOT/finish" || return 2
    fi
    _pty_runner_rc=0
    wait "$PTY_FIXTURE_JOB" || _pty_runner_rc=$?
    printf '%s\n' "$_pty_runner_rc" > "$PTY_EVIDENCE/runner.rc"
    if [ "${PTY_LOCAL_TRACE:-0}" != 1 ]; then
        for _pty_name in ctl.sent ctl.received ctl.identity.json ctl.trace.json ctl.wait.json; do
            d cp "$(ctr_name "$PTY_CTL_NODE"):$PTY_REMOTE_ROOT/$_pty_name" "$PTY_EVIDENCE/$_pty_name" || return 2
        done
        for _pty_name in remote.sent remote.received remote.identity.json remote.trace.json; do
            d cp "$(ctr_name "$PTY_AGENT_NODE"):$PTY_REMOTE_ROOT/$_pty_name" "$PTY_EVIDENCE/$_pty_name" || return 2
        done
    fi
    rm -f "$PTY_EVIDENCE/runtime.receipts.json" || return 2
    if [ "${PTY_FIXTURE_SCENARIO:-}" = C2 ]; then
        for _pty_name in canonical.sent fin.local.json; do
            d cp "$(ctr_name "$PTY_CTL_NODE"):$PTY_REMOTE_ROOT/$_pty_name" "$PTY_EVIDENCE/$_pty_name" || return 2
        done
        for _pty_name in canonical.remote.json fin.remote.json; do
            d cp "$(ctr_name "$PTY_AGENT_NODE"):$PTY_REMOTE_ROOT/$_pty_name" "$PTY_EVIDENCE/$_pty_name" || return 2
        done
    fi
    # Collector status and original ctl wait_status are separate receipts.
    [ "$_pty_runner_rc" = 0 ] || return "$_pty_runner_rc"
    pty_agent_snapshot settled
}

pty_collect_logs() {
    [ -n "${PTY_EVIDENCE:-}" ] || return 2
    _pty_phase=${1:-after}
    case "$_pty_phase" in before|after) ;; *) return 2 ;; esac
    # A failed recollection must invalidate the previous completion receipt.
    if [ "$_pty_phase" = after ]; then rm -f "$PTY_EVIDENCE/logs.json"; fi
    # origin: hot-update S4 plan §9.1 / T48. Tail/count readers cannot prove
    # inode continuity or an untruncated window. Missing API is NOT zero.
    command -v sim_log_snapshot >/dev/null 2>&1 || {
        printf '%s\n' 'PTY logs: logs.sh lacks sim_log_snapshot; complete cursor evidence unavailable' >&2
        return 2
    }
    mkdir -p "$PTY_EVIDENCE/logs/$_pty_phase" || return 2
    for _pty_stream in broker_slog broker_panic agent_slog agent_panic; do
        case "$_pty_stream" in broker_*) _pty_node=${PTY_BROKER_NODE:-brk1} ;; *) _pty_node=$PTY_AGENT_NODE ;; esac
        sim_log_snapshot "$_pty_stream" "$_pty_node" "$PTY_EVIDENCE/logs/$_pty_phase/$_pty_stream.raw" \
            "$PTY_EVIDENCE/logs/$_pty_phase/$_pty_stream.json" || return 2
    done
    [ "$_pty_phase" = after ] || return 0
    python3 -I -B - "$PTY_EVIDENCE" <<'PY' || return 2
import hashlib
import json
from pathlib import Path
import sys
root = Path(sys.argv[1])
cursors = []
for stream in ("broker_slog", "broker_panic", "agent_slog", "agent_panic"):
    snapshots, data = [], []
    for phase in ("before", "after"):
        folder = root / "logs" / phase
        meta = json.loads((folder / (stream+".json")).read_text())
        raw = (folder / (stream+".raw")).read_bytes()
        if (meta.get("read_ok") is not True or type(meta.get("inode")) is not int or meta["inode"] <= 0
                or type(meta.get("length")) is not int or meta["length"] != len(raw)):
            raise SystemExit("PTY log snapshot unreadable or incomplete")
        snapshots.append(meta)
        data.append(raw)
    if snapshots[0]["inode"] != snapshots[1]["inode"]:
        raise SystemExit("PTY log rotated")
    if "parts" in snapshots[0] or "parts" in snapshots[1]:
        before_parts, after_parts = snapshots[0]["parts"], snapshots[1]["parts"]
        if [(p["path"],p["dev"],p["inode"]) for p in before_parts] != [(p["path"],p["dev"],p["inode"]) for p in after_parts]:
            raise SystemExit("PTY log components rotated")
        suffixes, offsets = [], [0, 0]
        for before_part, after_part in zip(before_parts, after_parts):
            chunks = [raw[offset:offset+part["length"]] for raw,offset,part in
                      zip(data,offsets,(before_part,after_part))]
            if not chunks[1].startswith(chunks[0]):
                raise SystemExit("PTY log component lost its cursor prefix")
            suffixes.append(chunks[1][len(chunks[0]):])
            offsets = [offset+part["length"] for offset,part in zip(offsets,(before_part,after_part))]
        if offsets != [len(raw) for raw in data]:
            raise SystemExit("PTY log component lengths incomplete")
        suffix = b"".join(suffixes)
    else:
        if not data[1].startswith(data[0]):
            raise SystemExit("PTY log lost its cursor prefix")
        suffix = data[1][len(data[0]):]
    if "journal_cursors" in snapshots[0] or "journal_cursors" in snapshots[1]:
        old, new = snapshots[0]["journal_cursors"], snapshots[1]["journal_cursors"]
        if snapshots[0]["journal_identity"] != snapshots[1]["journal_identity"] or new[:len(old)] != old:
            raise SystemExit("PTY journal vacuumed or lost its cursor prefix")
    name = "logs/" + stream + ".log"
    (root / name).write_bytes(suffix)
    cursors.append(dict(stream=stream, inode=snapshots[1]["inode"], inode_before=snapshots[0]["inode"], before=len(data[0]), after=len(data[1]),
                        read_ok=True, file=name, length=len(suffix),
                        sha256=hashlib.sha256(suffix).hexdigest(), required={} if stream == "agent_slog" else []))
(root / "logs.json").write_text(json.dumps(dict(cursors=cursors, read_ok=True), sort_keys=True)+"\n")
PY
}

# origin: hot-update S8 plan §9.3 / P28. Historical loss needs a real old
# control stack, provisioned before business. This never edits product journals.
# The caller supplies the fixed artifact and its P22-validated source receipt.
# origin: S8 internal review s8-hermetic F9. The caller selects its ctl
# path before capture; installer scratch cannot export globals from a subshell.
upgrade_legacy_stack() {
    [ "$#" = 4 ] || return 2
    _ulstack_binary=$1; _ulstack_source=$2; _ulstack_ctl=$3; _ulstack_ev=$4
    case "$_ulstack_ctl" in /*) ;; *) return 2 ;; esac
    [ -r "$_ulstack_source" ] && [ -x "$_ulstack_binary" ] || return 2
    _ulstack_sha=$(sha256sum "$_ulstack_binary" | awk '{print $1}') || return 2
    cp "$_ulstack_source" "$_ulstack_ev/legacy-source.json" || return 2
    printf '%s\n' "$_ulstack_sha" > "$_ulstack_ev/legacy-source.sha"
    sctl brk1 stop tether-broker || return 2
    for _ulstack_node in brk1 ctl1; do
        d cp "$_ulstack_binary" "$(ctr_name "$_ulstack_node"):$_ulstack_ctl" || return 2
        dexec "$_ulstack_node" -- chmod 0755 "$_ulstack_ctl" || return 2
        _ulstack_observed_sha=$(dexec "$_ulstack_node" -- sha256sum "$_ulstack_ctl" | awk '{print $1}') || return 2
        [ "$_ulstack_observed_sha" = "$_ulstack_sha" ] || return 2
    done
    # Existing flow peers use the isolated ctl's canonical entry internally.
    # Select the same historical bytes there before admission as well; explicit
    # upgrade argv still uses the caller-selected LEGACY_CTL_BIN and is checked independently.
    d cp "$_ulstack_binary" "$(ctr_name ctl1):/usr/local/bin/tether" || return 2
    dexec ctl1 -- chmod 0755 /usr/local/bin/tether || return 2
    # Positional arguments preserve paths; a fixture cannot change the service
    # user, state ownership, config or source identity to rescue an upgrade.
    dexec brk1 -- sh -eu -c '
        "$1" serve --config /etc/tether/broker.yaml --config-check
        mkdir -p /etc/systemd/system/tether-broker.service.d
        printf "[Service]\nExecStart=\nExecStart=%s serve --config /etc/tether/broker.yaml\n" "$1" > /etc/systemd/system/tether-broker.service.d/legacy-control.conf
        systemctl daemon-reload
    ' legacy "$_ulstack_ctl" || return 2
    sctl brk1 start tether-broker || return 2
}

upgrade_legacy_ctl() {
    [ -n "${LEGACY_CTL_BIN:-}" ] || return 2
    dexec -u sim ctl1 -- env HOME=/home/sim "$LEGACY_CTL_BIN" "$@"
}

# Capture kernel identities, not a marker or the selected pathname. Together
# the two samples prove a same-PID exec, including the process incarnation.
upgrade_legacy_snapshot() {
    [ "$#" = 2 ] || return 2
    _uls_phase=$1; _uls_evidence=$2
    case "$_uls_phase" in before|after) ;; *) return 2 ;; esac
    for _uls_role in broker agent; do
        case "$_uls_role" in broker) _uls_node=brk1 ;; agent) _uls_node=agt1 ;; esac
        _uls_pid=$(dexec "$_uls_node" -- systemctl show "tether-$_uls_role" -p MainPID --value) || return 2
        dexec "$_uls_node" -- python3 -I -B -c '
import hashlib,json,pathlib,sys
pid=int(sys.argv[1]); assert pid>1
p=pathlib.Path("/proc")/str(pid)
def identity():
    return dict(pid=pid,start_ticks=int((p/"stat").read_text().rsplit(")",1)[1].split()[19]),
        boot_id=pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip())
before=identity(); sha=hashlib.sha256((p/"exe").read_bytes()).hexdigest()
assert before==identity()
print(json.dumps(dict(**before,running_sha=sha)))
' "$_uls_pid" > "$_uls_evidence/legacy-$_uls_role-$_uls_phase.json" || return 2
    done
    dexec ctl1 -- sha256sum "$LEGACY_CTL_BIN" > "$_uls_evidence/legacy-ctl-$_uls_phase.sha" || return 2
}

# origin: hot-update S8 plan §11.2 P28 mutant: no old exec is SETUP-RED,
# even if an unrelated error contains the reviewed loss signature. Call again
# at classification so a stale successful setup cannot authorize a later arm.
upgrade_legacy_exec_required() {
    [ "$#" = 2 ] || return 2
    python3 -I -B - "$1" "$2" <<'PYEXEC'
import json,pathlib,re,sys
try:
    root=pathlib.Path(sys.argv[1]); target=sys.argv[2]
    old=(root/'legacy-source.sha').read_text().strip()
    new=(root/'legacy-target.sha').read_text().strip()
    source=json.loads((root/'legacy-source.json').read_text())
    assert re.fullmatch('[0-9a-f]{40}',source['commit'])
    assert re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+',source['tag'])
    assert re.fullmatch('[0-9a-f]{64}',old) and re.fullmatch('[0-9a-f]{64}',new) and old!=new
    assert target in ('agent','broker')
    assert (root/'legacy-command.rc').read_text().strip()=='0'
    for role in ('broker','agent'):
        before=json.loads((root/('legacy-'+role+'-before.json')).read_text())
        after=json.loads((root/('legacy-'+role+'-after.json')).read_text())
        for row in (before,after):
            assert type(row['pid']) is int and row['pid']>1
            assert type(row['start_ticks']) is int and row['start_ticks']>0 and row['boot_id']
        assert all(before[key]==after[key] for key in ('pid','start_ticks','boot_id'))
        assert before['running_sha']==old
        assert after['running_sha']==(new if role==target else old)
    for phase in ('before','after'):
        assert (root/('legacy-ctl-'+phase+'.sha')).read_text().split()[0]==old
except (AssertionError,OSError,ValueError,KeyError,TypeError,IndexError):
    print('legacy exporter execution or old control provenance is unproven',file=sys.stderr)
    sys.exit(2)
PYEXEC
}

# origin: hot-update S8 plan §9.3 / P28. assert_bug accepts any matching
# nonzero diagnostic, so first separate invalid/missing evidence from actual
# loss rc1. Preserve the raw oracle once; never rerun a destructive observer.
upgrade_legacy_assert() {
    _ula_root=$1; _ula_target=$2; _ula_desc=$3; _ula_owner=$4; _ula_signature=$5; shift 5
    if ! upgrade_legacy_exec_required "$_ula_root" "$_ula_target"; then
        _AS_SETUP=$((_AS_SETUP + 1)); return 0
    fi
    "$@" > "$_ula_root/legacy-oracle.raw" 2>&1
    _ula_rc=$?
    printf '%s\n' "$_ula_rc" > "$_ula_root/legacy-oracle.rc"
    case "$_ula_rc" in
        0|1) assert_bug "$_ula_desc" "$_ula_owner" "$_ula_signature" upgrade_legacy_observation "$_ula_rc" "$_ula_root/legacy-oracle.raw" ;;
        4) _AS_NC=$((_AS_NC + 1)); _AS_NC_GUARD=$((_AS_NC_GUARD + 1)) ;;
        # origin: S8 internal review s8-hermetic F2. Invalid observations
        # retain their native setup class and expose the saved diagnostic.
        *) cat "$_ula_root/legacy-oracle.raw"; _AS_SETUP=$((_AS_SETUP + 1)) ;;
    esac
}
upgrade_legacy_observation() {
    cat "$2"
    return "$1"
}
