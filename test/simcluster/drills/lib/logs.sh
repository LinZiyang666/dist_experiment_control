#!/bin/sh
# logs.sh — the ONE place that knows WHERE each tether log stream lives.
#
# origin: docs/reviews/h1-external-review.md F3. h1 moved two of the four
# streams, and because every drill had inlined its own `journalctl …` or
# `tail /var/log/tether/broker.err`, the move turned working oracles into
# silent false verdicts: drill 94 reported two ASSERTION FAILURES against a
# product that was reconciling correctly, purely because the agent's slog was
# no longer in the journal. A failing oracle that reads as a product failure is
# the single worst outcome this harness can produce (README mandate: expose
# defects faithfully, never manufacture them), so the mapping now lives here.
#
# THE FOUR STREAMS, AND WHY THEY MUST STAY DISTINGUISHABLE
# -------------------------------------------------------
#   broker slog   -> /var/log/tether/broker.log   (process-owned, size-capped)
#   broker panic  -> journald (unit Standard*=journal; raw fd 2)
#   agent  slog   -> $HOME/.tether/agent/<sid>/agent.log (process-owned, capped)
#   agent  panic  -> $HOME/.tether/agent/<sid>/agent.boot.err (dup2'd fd 2)
#
# Before h1: broker slog went to broker.err (it is stderr output, and the unit
# had StandardError=append:broker.err); the agent's slog went to journald
# because the sim unit sets no Standard*=. A helper that merges the streams
# would let a panic satisfy a slog assertion — which is why these are separate
# functions rather than one "grep the logs" catch-all.
#
# THE RULE THAT DECIDES WHICH READER YOU WANT
# -------------------------------------------
# Ask WHEN the line is written, not who wrote it:
#
#   * anything emitted BEFORE the logger exists — config-validation refusals,
#     startup fail-closed diagnostics, "cannot open X, exiting" — is ONLY ever
#     in the BOOT stream (journald for the broker, agent.boot.err/journal for
#     the agent). It cannot be in the slog, because the slog sink is one of the
#     things that has not been built yet.
#   * anything a running daemon logs is in the SLOG.
#   * panics and stacktraces are in the BOOT stream at any time: the Go runtime
#     writes them to raw fd 2, bypassing every Go-level writer.
#
# This is not theoretical: drill 93's "startup fails loudly on a non-http(s)
# webhook URL" arm was pointed at the slog during the h1 migration and went red
# against a broker that was refusing exactly as designed — the unit really did
# fail to start, the message was simply in the other stream. A startup-refusal
# assertion that reads the slog is guaranteed to fail no matter how correct the
# product is.
#
# The broker readers deliberately include the LEGACY broker.err too: an image
# or host provisioned before h1 still has content there, and a drill that ran
# green yesterday must not go red merely because the file moved. Post-h1
# broker.err simply stops growing.

# sim_broker_slog <node> [lines] — the broker's application log.
sim_broker_slog() {
    dexec "$1" -- sh -c "cat /var/log/tether/broker.log /var/log/tether/broker.err 2>/dev/null | tail -n ${2:-80}"
}

# sim_broker_slog_grep <node> <extended-regex> — quiet grep over the broker
# slog; rc=0 iff matched.
sim_broker_slog_grep() {
    dexec "$1" -- sh -c "cat /var/log/tether/broker.log /var/log/tether/broker.err 2>/dev/null | grep -qE '$2'"
}

# sim_broker_slog_count <node> <extended-regex> — match count (stdout), or
# NOTHING if the log could not be read at all. Callers must treat empty as a
# failure, never as zero: "I read the log and saw none" and "I could not read
# the log" are different answers, and collapsing them is how an oracle becomes
# permanently green.
#
# NOTE the missing `|| echo 0`: `grep -c` prints 0 AND exits 1 when there is no
# match, so an `|| echo 0` fallback emits a SECOND zero. The two-line result
# then breaks every numeric comparison downstream (`[ "$n" -gt "$m" ]` on a
# multiline value is a shell error, not a false), which reads as a product
# failure. `tail -1` keeps the count and drops the fallback's echo if one is
# ever re-added.
# The "could not read" case is made EXPLICIT rather than inferred: a missing
# file also makes `cat | grep -c` print 0, so without the -f probe the two
# answers really would be indistinguishable.
sim_broker_slog_count() {
    dexec "$1" -- sh -c "[ -f /var/log/tether/broker.log ] || [ -f /var/log/tether/broker.err ] || exit 0
cat /var/log/tether/broker.log /var/log/tether/broker.err 2>/dev/null | grep -cE '$2'" 2>/dev/null | tr -d '\r' | tail -1
}

# sim_broker_panic_journal <node> <extended-regex> — the OTHER broker stream:
# panics, stacktraces and pre-logger boot output, which h1 routes to journald.
sim_broker_panic_journal() {
    dexec "$1" -- sh -c "journalctl -u tether-broker --no-pager 2>/dev/null | grep -qE '$2'"
}

# sim_broker_panic_journal_dump <node> [lines] — the same stream as CONTENT, for
# oracles that need to apply several greps to one sample (a startup refusal is
# usually asserted as a conjunction of clauses, and re-reading the journal
# between clauses would let a crash-loop rotate the evidence out from under
# them mid-assertion).
sim_broker_panic_journal_dump() {
    dexec "$1" -- sh -c "journalctl -u tether-broker --no-pager -n ${2:-800} 2>/dev/null"
}

# sim_agent_slog_cursor <node> — byte offset into the agent slog, for "did X
# appear AFTER this point" questions. A byte offset needs no clock-format
# agreement and cannot let a line written earlier false-pass.
sim_agent_slog_cursor() {
    dexec "$1" -- sh -c '. /etc/tether/agent.env 2>/dev/null; f=/home/sim/.tether/agent/${SID:-lab}/agent.log; [ -f "$f" ] && wc -c < "$f" || echo 0' 2>/dev/null | tr -d '\r '
}

# sim_agent_slog_grep <node> <extended-regex> [cursor] — quiet grep over the
# agent slog, optionally only the bytes after <cursor>.
sim_agent_slog_grep() {
    dexec "$1" -- sh -c ". /etc/tether/agent.env 2>/dev/null; f=/home/sim/.tether/agent/\${SID:-lab}/agent.log; [ -f \"\$f\" ] || exit 1; tail -c +\$(( ${3:-0} + 1 )) \"\$f\" 2>/dev/null | grep -qE '$2'"
}

# sim_agent_slog_count <node> <extended-regex> [cursor] — match count (stdout),
# or NOTHING when the slog is unreadable. Same contract and same `grep -c`
# double-zero hazard as sim_broker_slog_count above; the anti-vacuity guards in
# 71-expose-rehome-failover depend on empty≠zero, so a missing file must print
# nothing rather than a confident "0 matches".
sim_agent_slog_count() {
    _sasc_cursor=${3:-0}
    case "$_sasc_cursor" in ''|*[!0-9]*) return 2 ;; esac
    dexec "$1" -- sh -c ". /etc/tether/agent.env 2>/dev/null; f=/home/sim/.tether/agent/\${SID:-lab}/agent.log; [ -f \"\$f\" ] || exit 0; tail -c +\$(( $_sasc_cursor + 1 )) \"\$f\" 2>/dev/null | grep -cE '$2'" 2>/dev/null | tr -d '\r' | tail -1
}

# sim_agent_slog_tail <node> [lines] — diagnostics.
sim_agent_slog_tail() {
    dexec "$1" -- sh -c ". /etc/tether/agent.env 2>/dev/null; tail -n ${2:-40} /home/sim/.tether/agent/\${SID:-lab}/agent.log 2>/dev/null"
}

# sim_agent_panic_sink <node> <extended-regex> — the agent's panic stream
# (dup2'd fd 2). Separate from the slog reader on purpose.
sim_agent_panic_sink() {
    dexec "$1" -- sh -c ". /etc/tether/agent.env 2>/dev/null; grep -qE '$2' /home/sim/.tether/agent/\${SID:-lab}/agent.boot.err 2>/dev/null"
}

# sim_agent_panic_sink_path <sid> — where the agent's panic stream lives, for a drill that LAUNCHES
# an agent the way scripts/install.sh's setsid-nohup branch does (`2>> <this path>`) and must not
# carry its own copy of the location (drill 34.N; hot-update S3).
sim_agent_panic_sink_path() {
    printf '/home/sim/.tether/agent/%s/agent.boot.err\n' "$1"
}

# sim_agent_panic_sink_dump <node> [lines] — content from the same agent boot
# sink as sim_agent_panic_sink, kept separate from slog and pre-logger journald.
# A missing sink is an error, not an empty successful observation.
sim_agent_panic_sink_dump() {
    _sapsd_lines=${2:-800}
    case "$_sapsd_lines" in ''|*[!0-9]*) return 2 ;; esac
    dexec "$1" -- sh -c '
        if [ -r /etc/tether/agent.env ]; then . /etc/tether/agent.env; fi
        tail -n "$1" "/home/sim/.tether/agent/${SID:-lab}/agent.boot.err"
    ' _ "$_sapsd_lines"
}

# sim_agent_panic_cursor <node> — byte length of the agent's boot stream (0 when absent), so a caller can
# attribute a later match to ITS OWN daemon attempt rather than to an earlier one in the same file. The
# boot stream is where a daemon that refuses to start says why (fd 2 is pointed there right after the log
# sink is armed — h1 F), so a drill asserting "this reconnect was refused for reason X" reads it here, with
# a cursor, never the process's stdout (drill 81 B3 read stdout for two weeks and saw the banner only).
sim_agent_panic_cursor() {
    dexec "$1" -- sh -c '. /etc/tether/agent.env 2>/dev/null; f=/home/sim/.tether/agent/${SID:-lab}/agent.boot.err; [ -f "$f" ] && wc -c < "$f" || echo 0' 2>/dev/null | tr -d '\r '
}

# sim_agent_panic_sink_since <node> <extended-regex> <cursor> — sim_agent_panic_sink restricted to the
# bytes written after <cursor>.
sim_agent_panic_sink_since() {
    _saps_cursor=${3:-0}
    case "$_saps_cursor" in ''|*[!0-9]*) return 2 ;; esac
    dexec "$1" -- sh -c ". /etc/tether/agent.env 2>/dev/null; f=/home/sim/.tether/agent/\${SID:-lab}/agent.boot.err; [ -f \"\$f\" ] || exit 1; tail -c +\$(( $_saps_cursor + 1 )) \"\$f\" 2>/dev/null | grep -qE '$2'"
}

# sim_agent_slog_since <node> <cursor> [max-lines] — the agent slog written AFTER <cursor>,
# capped at max-lines (default 200). Added for drill 62's Arm 1S evidence dumps: a plain
# tail is not bounded to the command being diagnosed, and because the agent restarts
# between arms (and appends to the same file) a tail can be entirely post-restart lines
# while the window under investigation scrolls off. Same one-mapping rule as the readers
# above — drills must not reach into the slog path themselves.
sim_agent_slog_since() {
    _sass_cursor=${2:-0}
    _sass_lines=${3:-200}
    case "$_sass_cursor" in ''|*[!0-9]*) return 2 ;; esac
    case "$_sass_lines" in ''|*[!0-9]*) return 2 ;; esac
    dexec "$1" -- sh -c ". /etc/tether/agent.env 2>/dev/null; f=/home/sim/.tether/agent/\${SID:-lab}/agent.log; [ -f \"\$f\" ] || exit 0; tail -c +\$(( $_sass_cursor + 1 )) \"\$f\" 2>/dev/null | head -n $_sass_lines"
}

# sim_log_snapshot <stream> <node> <raw_destination> <metadata_destination>
# origin: hot-update S4 plan §9.1 / T48. This reader belongs beside the existing
# mapping: a tail or a failed read interpreted as empty would forge evidence.
# Transport is base64, so docker exec cannot reinterpret NULs or line endings.
# File identity includes every mapped component; journald retains its cursors
# alongside a boot/unit identity so vacuuming cannot masquerade as an append.
sim_log_snapshot() {
    [ "$#" = 4 ] || return 2
    case "$1" in broker_slog|broker_panic|agent_slog|agent_panic) ;; *) return 2 ;; esac
    _slss_raw=$3
    _slss_meta=$4
    [ "$_slss_raw" != "$_slss_meta" ] || return 2
    rm -f "$_slss_meta" || return 2
    _slss_transfer=$(mktemp) || return 2
    _slss_program=$(cat <<'PY'
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

stream, sid = sys.argv[1:]

def stable_id(value):
    return int.from_bytes(hashlib.sha256(value.encode()).digest()[:8], "big") or 1

def file_snapshot(path):
    with open(path, "rb", buffering=0) as source:
        before = os.fstat(source.fileno())
        data = source.read(before.st_size)
        after = os.fstat(source.fileno())
        current = os.stat(path)
        if (len(data) != before.st_size or (before.st_dev, before.st_ino) != (current.st_dev, current.st_ino)
                or after.st_size < before.st_size or current.st_size < before.st_size):
            raise RuntimeError("log read incomplete or rotated")
        source.seek(0)
        if source.read(len(data)) != data:
            raise RuntimeError("log truncated or rewritten during snapshot")
        final = os.stat(path)
        if (final.st_dev, final.st_ino) != (before.st_dev, before.st_ino) or final.st_size < len(data):
            raise RuntimeError("log rotated after read verification")
    return data, dict(path=path, dev=before.st_dev, inode=before.st_ino, length=len(data))

def journal_snapshot():
    # MESSAGE is the journal's original byte payload, not rendered timestamps
    # or tail output. A second full query detects vacuum during acquisition.
    def query():
        result = subprocess.run(["journalctl", "-u", "tether-broker", "--no-pager", "--all", "-o", "json"],
                                check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        rows = [json.loads(line) for line in result.stdout.splitlines()]
        cursors, chunks = [], []
        for row in rows:
            cursor, message = row["__CURSOR"], row["MESSAGE"]
            if not isinstance(cursor, str) or not cursor or cursor in cursors:
                raise RuntimeError("journal cursor missing or duplicated")
            cursors.append(cursor)
            chunks.append(message.encode() if isinstance(message, str) else bytes(message))
        return cursors, b"".join(chunk+b"\n" for chunk in chunks)
    identity = Path("/etc/machine-id").read_text().strip()+":"+Path("/proc/sys/kernel/random/boot_id").read_text().strip()+":tether-broker"
    cursors, raw = query()
    again, second = query()
    if again[:len(cursors)] != cursors or not second.startswith(raw):
        raise RuntimeError("journal cursor prefix lost during snapshot")
    return raw, dict(inode=stable_id(identity), journal_identity=identity, journal_cursors=cursors)

try:
    if stream == "broker_panic":
        raw, meta = journal_snapshot()
    else:
        # Same four-stream mapping as the readers above, including the legacy
        # broker.err component. Missing optional legacy content is not a read
        # failure; a missing named stream or unreadable present component is.
        paths = (["/var/log/tether/broker.log", "/var/log/tether/broker.err"] if stream == "broker_slog" else
                 [f"/home/sim/.tether/agent/{sid}/"+("agent.log" if stream == "agent_slog" else "agent.boot.err")])
        parts, chunks = [], []
        for path in paths:
            if stream == "broker_slog" and not os.path.exists(path):
                continue
            data, part = file_snapshot(path)
            chunks.append(data)
            parts.append(part)
        if not parts:
            raise RuntimeError("named log stream absent")
        raw = b"".join(chunks)
        meta = dict(inode=parts[0]["inode"], parts=parts)
    meta.update(length=len(raw), read_ok=True)
    print(json.dumps(dict(metadata=meta, raw=base64.b64encode(raw).decode())))
except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.CalledProcessError) as exc:
    print("log snapshot failed: "+str(exc), file=sys.stderr)
    sys.exit(2)
PY
    )
    if ! dexec "$2" -- sh -c '
        if [ -r /etc/tether/agent.env ]; then . /etc/tether/agent.env; fi
        exec python3 -I -B -c "$2" "$1" "${SID:-lab}"
    ' _ "$1" "$_slss_program" > "$_slss_transfer"; then
        rm -f "$_slss_transfer"
        return 2
    fi
    _slss_rc=0
    python3 -I -B - "$_slss_transfer" "$_slss_raw" "$_slss_meta" <<'PY' || _slss_rc=$?
import base64
import json
from pathlib import Path
import sys
receipt = json.loads(Path(sys.argv[1]).read_bytes())
raw = base64.b64decode(receipt["raw"], validate=True)
meta = receipt["metadata"]
if meta.get("read_ok") is not True or type(meta.get("inode")) is not int or meta["inode"] <= 0 or meta.get("length") != len(raw):
    raise SystemExit("invalid remote log snapshot")
Path(sys.argv[2]).write_bytes(raw)
Path(sys.argv[3]).write_text(json.dumps(meta, sort_keys=True)+"\n")
PY
    rm -f "$_slss_transfer"
    return "$_slss_rc"
}

# origin: hot-update S10 plan §5.5 / FullstackLogCaptureTest. The P05/P16
# supervisor starts the persistent collector and sets FULLSTACK_LOGS_SOCKET.
# Keeping the old file descriptors belongs to that lifecycle; a fresh remote
# snapshot cannot prove that rotation preserved the bytes between two calls.
# FULLSTACK_LOGS_PY is an explicit tool location, never an implicit stdin config.
_fullstack_logs_call() {
    [ -n "${FULLSTACK_LOGS_PY:-}" ] || {
        printf '%s\n' 'strict logs: FULLSTACK_LOGS_PY is required' >&2
        return 2
    }
    _fsl_rc=0
    "${PYTHON3:-python3}" -I -B "$FULLSTACK_LOGS_PY" "$@" || _fsl_rc=$?
    # Save the reader's original status before any caller aggregates a verdict.
    return "$_fsl_rc"
}

fullstack_logs_start() {
    [ "$#" = 2 ] || return 2
    _fullstack_logs_call start "$1" "$2"
}

fullstack_logs_drain() {
    [ "$#" = 3 ] || return 2
    _fullstack_logs_call drain "$1" "$2" "$3"
}

fullstack_logs_verify() {
    [ "$#" = 2 ] || return 2
    _fullstack_logs_call verify "$1" "$2"
}
