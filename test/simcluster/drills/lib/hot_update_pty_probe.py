#!/usr/bin/env python3
"""Raw duplex PTY collector and fail-closed offline continuity oracle.

S4 §12.1 contract: all commands take --evidence DIR --nonce HEX. continuity
and legacy-exporter return 0 continuous, 1 valid discontinuity, 2 invalid
evidence. legacy-exporter preserves the positive oracle's rc and reports
observed reasons; it never guesses an old ctl's exit code.

trace --role remote is the long foreground job on the remote PTY, writes
remote.sent/remote.received and exits 23 on a STOP frame. trace --role ctl
launches an explicit absolute --ctl-binary with a real local controlling tty.
Default argv: run NODE -- python3 REMOTE_PROBE trace --role leader ...
The leader forks the distinct foreground and assigns it the original tty.
C2 alone requests intentional leader exit; its original foreground records the
resulting kernel HUP and retains a measured tail without a remote slave keeper.
Arguments after -- override this for hermetic tests or container paths.
A finish file requests normal STOP. Copy the four raw files and both trace
receipts into one evidence directory before judging. No slave keeper remotely.
trace --role checkpoint --name NAME samples original ctl identity/progress.
Optional --cols/--rows requests local tty resize and waits for the collector's
ioctl receipt. This is a local kernel receipt, not a remote ControlACK.
Remote trace samples retain actual winsize for P15's independent effect proof.

trace --role agent captures /proc identity, executable SHA, marker and census.
trace --role assemble joins the final independent receipts into evidence.json.
P18 scalar receipts are read from the verified logs.sh snapshot window. The
started record's original OS PID/start ticks binds its management token to this
nonce's leader; ps argv corroborates the binding, never supplies byte offsets.
Cut/activation and terminal records supply A/Q, Base/End, FIN and absolute KA.
Missing or stale receipts are rc2. Local ioctls are not remote control ACKs.
Byte blobs:
{file,length,sha256,records:[{id,offset,length,sha256,at}]}; records partition
the raw wire, including headers, without CR/LF normalization or gap filling.
Process samples: role,at,identity,session_id,foreground_pgid,ctty,alive,hup,
early_eof,nonce. Identity: boot_id,pid,start_ticks (leader also ppid).
Images: identity,sha256,generation,phase,spawn_count,terminal_count,exit_code.
Windows: start,end,fault_observed. Resource snapshots: owner,counts,masters,
wrongfd,zombies,foreground_alive; master: dev,ino,rdev,mount_id,pty_index.
Input/output checkpoints carry at and their two byte offsets. input.protocol
defaults to reliable-pty-v1. A legacy receipt sets protocol=legacy, ctl.path=legacy
and observed pty_version=0 on ctl or the original agent image; unsupported
ACK/base/end/control sequences remain zero. Legacy has no replay requirement,
but all byte/identity/foreground/waitpid/management/FD/log checks still apply.
Both judging commands use this same mode-aware oracle; a fixed legacy path
returns 0 and cannot be mislabeled PRODUCT-RED for missing reliable telemetry.
Logs: read_ok,cursors (exactly four streams), each with stream,inode,before,
after,read_ok,file,length,sha256,required. Log bytes come ONLY from logs.sh.
The agent_slog must contain this nonce's started/activation/exit receipts.
500ms freeze windows are calibration; ordinary business gaps exceeding 5s fail.
A window explicitly tagged kind=watchdog additionally requires blackhole_s>=120
and a rolled_back final image. Its intersecting carrier gap may use the fixed
180s original-ctl recovery window; raw timestamps and maxima remain evidence.
The fixture honors a pause-input file remotely for W's real queued-input setup.
--input-bytes controls ctl payload size (at least the fixed raw payload, at most 32768), never output rate.
"""

import argparse
import contextlib
import datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import select
import shlex
import signal
import stat
import struct
import sys
import tempfile
import termios
import time
import tty

STREAMS = ("broker_slog", "broker_panic", "agent_slog", "agent_panic")
KINDS = ("master", "replay", "handoff", "pidfd")
MASTER_KEYS = ("dev", "ino", "rdev", "mount_id", "pty_index")
TOP_KEYS = ("version", "nonce", "images", "processes", "windows", "input", "output",
            "controls", "ctl", "resources", "logs", "oracle")


class EvidenceError(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise EvidenceError(reason)


def integer(value):
    return type(value) is int and value >= 0


def stamp(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def write_json(path, value):
    path = Path(path)
    staging = path.with_name(path.name + ".tmp")
    staging.write_text(json.dumps(value, sort_keys=True, allow_nan=False) + "\n")
    staging.replace(path)


def read_json(path):
    value = json.loads(Path(path).read_text())
    require(isinstance(value, dict), "non-object evidence")
    return value


def sha(data):
    return hashlib.sha256(data).hexdigest()


def identity(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return dict(pid=pid, start_ticks=int(fields[19]),
                boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                ppid=int(fields[1]), state=fields[0], session_id=int(fields[3]),
                foreground_pgid=int(fields[5]), process_group=int(fields[2]), tty_nr=int(fields[4]))


def valid_identity(value):
    require(isinstance(value, dict) and integer(value.get("pid")) and value["pid"] > 1
            and integer(value.get("start_ticks")) and value["start_ticks"] > 0
            and isinstance(value.get("boot_id"), str) and value["boot_id"], "missing process identity")
    return tuple(value[key] for key in ("pid", "start_ticks", "boot_id"))


def safe_file(root, name):
    require(isinstance(name, str) and name and not Path(name).is_absolute()
            and ".." not in Path(name).parts, "unsafe evidence filename")
    path = root / name
    require(path.resolve().is_relative_to(root.resolve()), "evidence symlink escapes directory")
    return path


def blob_bytes(root, blob, records=True):
    require(isinstance(blob, dict) and integer(blob.get("length"))
            and re.fullmatch(r"[0-9a-f]{64}", str(blob.get("sha256", ""))), "invalid raw blob metadata")
    raw = safe_file(root, blob["file"]).read_bytes()
    require(len(raw) == blob["length"] and sha(raw) == blob["sha256"], "raw blob length/hash mismatch")
    if records:
        require(isinstance(blob.get("records"), list), "missing raw byte records")
        offset, previous = 0, -1
        for row in blob["records"]:
            require(all(integer(row.get(key)) for key in ("id", "offset", "length"))
                    and row["length"] > 0 and stamp(row.get("at")) and row["at"] >= previous,
                    "invalid raw byte record")
            require(row["offset"] == offset and row["length"] <= len(raw) - offset,
                    "raw record does not partition byte file")
            chunk = raw[offset:offset + row["length"]]
            require(sha(chunk) == row.get("sha256"), "raw record hash mismatch")
            offset += row["length"]
            previous = row["at"]
        require(offset == len(raw), "unrecorded raw bytes")
    return raw


def judge(root, nonce, compatibility=False):
    # origin: hot-update S4 plan §9.1 / T48. Missing evidence is setup failure;
    # a complete receipt of lost bytes is a business failure. Keep them distinct.
    root = Path(root)
    doc = read_json(root / "evidence.json")
    require(all(key in doc for key in TOP_KEYS) and type(doc["version"]) is int and doc["version"] == 1 and doc["nonce"] == nonce,
            "missing or foreign top-level evidence")
    require(isinstance(doc["oracle"], dict) and all(k in doc["oracle"] for k in ("valid", "continuous", "reason"))
            and type(doc["oracle"]["valid"]) is bool and type(doc["oracle"]["continuous"]) is bool
            and isinstance(doc["oracle"]["reason"], str), "missing oracle shape")
    require(all(isinstance(doc[key], dict) for key in ("input", "output", "ctl", "resources", "logs")),
            "invalid evidence object shape")
    failures, invalid = [], []

    def check(name, condition, setup=False):
        # Named branches each have independent negative fixtures and mutations.
        if not condition:
            failures.append(name)
            if setup:
                invalid.append(name)

    protocol = doc["input"].get("protocol", "reliable-pty-v1")
    require(protocol in ("reliable-pty-v1", "legacy"), "unknown input protocol")
    legacy = protocol == "legacy"
    negative_legacy = legacy and doc.get("scenario") == "P"
    if negative_legacy:
        verify_legacy_fault(root, nonce, doc)
        check("management_terminal", not doc["legacy_loss"]["management_missing"])
    if legacy:
        old_versions = [doc["ctl"].get("pty_version")]
        if isinstance(doc["images"], list) and doc["images"]:
            old_versions.append(doc["images"][0].get("pty_version"))
        require(doc["ctl"].get("path") == "legacy" and any(type(v) is int and v == 0 for v in old_versions),
                "legacy protocol not bound to an observed old endpoint")
    raw = {}
    byte_files = {("input", "sent"): "ctl.sent", ("input", "received"): "remote.received",
                  ("output", "sent"): "remote.sent", ("output", "received"): "ctl.received"}
    for direction in ("input", "output"):
        for side in ("sent", "received"):
            blob = doc[direction][side]
            # origin: hot-update S4 plan §9.1 / T48. A source ledger must
            # never stand in for what the independent ctl actually received.
            require(blob.get("file") == byte_files[direction, side], "raw source ledger substituted for receiver")
            raw[direction, side] = blob_bytes(root, blob)
            try:
                wire = parse_frames(raw[direction, side], nonce)
                require([(r["id"], r["offset"], r["length"]) for r in wire] ==
                        [(r["id"], r["offset"], r["length"]) for r in blob["records"]],
                        "byte metadata differs from raw protocol")
                protocol_ok = True
            except EvidenceError:
                protocol_ok = False
            check("protocol_" + direction + "_" + side, protocol_ok)
            check(direction + "_order_" + side, [r["id"] for r in blob["records"]] == list(range(len(blob["records"]))))
        check("bytes_" + direction, raw[direction, "sent"] == raw[direction, "received"])
        require(not os.path.samefile(root / doc[direction]["sent"]["file"],
                                     root / doc[direction]["received"]["file"]), "raw evidence files share one inode")
        check("nonempty_" + direction, len(doc[direction]["sent"]["records"]) >= 40)
    inp, out = doc["input"], doc["output"]
    require(all(integer(inp.get(k)) for k in ("applied", "accepted"))
            and all(integer(out.get(k)) for k in ("base", "end")), "invalid byte offsets")
    action_bytes = 0
    if doc.get("scenario") == "C2":
        actions = doc.get("canonical_actions", {})
        require(actions.get("nonce") == nonce and actions.get("remote", {}).get("nonce") == nonce,
                "missing canonical action receipts")
        sent_actions = blob_bytes(root, actions["sent"], records=False)
        check("canonical_keys", sent_actions == b"\x04\x03")
        check("canonical_eof_signal", actions["remote"].get("eof") is True and actions["remote"].get("sigint") == 1)
        require(valid_identity(actions["remote"]["identity"]) == valid_identity(next(r["identity"] for r in doc["processes"] if r["role"] == "foreground")),
                "canonical action belongs to another foreground")
        fin = doc.get("terminal_receipt", {})
        check("logical_fin", fin.get("input_fin") is True and fin.get("input_ended") is True and fin.get("output_fin") is True)
        require(actions.get("local_fin", {}).get("nonce") == nonce and actions.get("remote_fin", {}).get("nonce") == nonce,
                "missing independently observed FIN actions")
        check("fin_tail", actions["remote_fin"].get("tail_s", 0) > .2)
        action_bytes = len(sent_actions)
    check("final_input_offsets", inp["applied"] == inp["accepted"] == (0 if legacy else len(raw["input", "sent"])+action_bytes))
    check("final_output_offsets", out["base"] == out["end"] == (0 if legacy else len(raw["output", "sent"])))
    for direction, fields in ((inp, ("applied", "accepted")), (out, ("base", "end"))):
        require(isinstance(direction.get("checkpoints"), list) and len(direction["checkpoints"]) >= 2,
                "missing offset checkpoints")
        previous, previous_at = [0, 0], -1
        for row in direction["checkpoints"]:
            require(stamp(row.get("at")) and row["at"] > previous_at
                    and all(integer(row.get(k)) for k in fields), "invalid offset checkpoint")
            values = [row[k] for k in fields]
            check("offset_monotonic_" + fields[0], values[0] <= values[1] and all(a <= b for a, b in zip(previous, values)))
            previous, previous_at = values, row["at"]
        check("offset_current_" + fields[0], all(a <= direction[k] for a, k in zip(previous, fields)))

    windows = doc["windows"]
    if compatibility:
        require(legacy and doc.get("fallback", {}).get("nonce") == nonce and
                doc["fallback"].get("precreate_only") is True and
                any(r.get("old") is True for r in doc["fallback"].get("endpoints", [])),
                "unproved precreate fallback quadrant")
    else:
        require(not any(w.get("kind") == "compatibility" for w in windows),
                "compatibility observation is not a hot-update fault")
    require(isinstance(windows, list), "invalid fault windows")
    check("windows_nonempty", bool(windows), setup=True)
    for window in windows:
        require(stamp(window.get("start")) and stamp(window.get("end"))
                and type(window.get("fault_observed")) is bool, "missing fault observation")
        check("window_observed", window["fault_observed"] and window["start"] < window["end"], setup=True)
        for direction in ("input", "output"):
            for side in ("sent", "received"):
                rows = doc[direction][side]["records"]
                check("preprogress_" + direction + "_" + side, sum(r["at"] < window["start"] for r in rows) >= 20)
                check("cutprogress_" + direction + "_" + side, any(window["start"] <= r["at"] <= window["end"] for r in rows))
                check("postprogress_" + direction + "_" + side, any(r["at"] > window["end"] for r in rows))

    drains = [w for w in windows if w.get("kind") == "draining"]
    for window in drains:
        require(window.get("leader_exited") is True and stamp(window.get("leader_exit_at"))
                and window["leader_exit_at"] <= window["start"] and stamp(window.get("tail_s"))
                and window["tail_s"] > .2 and stamp(window.get("draining_end_at", window["end"]))
                and window.get("draining_end_at", window["end"]) >= window["end"], "missing causal draining leader/tail receipt")
    samples = doc["processes"]
    require(isinstance(samples, list), "invalid process samples")
    # origin: hot-update S4 plan §9.2 / T48; drill-run3 C2. A /proc
    # observation is an upper bound on leader exit, which already caused HUP.
    # Bind its lower bound to the last real live-leader sample. This admits
    # only that measured interval, never a blanket pre-drain HUP tolerance.
    for window in drains:
        if "leader_exit_lower_at" in window:
            lower = window["leader_exit_lower_at"]
            live = [r["at"] for r in samples if r.get("role") == "leader" and r.get("alive") is True]
            require(live and stamp(lower) and lower == max(live) and lower <= window["leader_exit_at"],
                    "draining exit lower bound differs from live leader observation")
    check("observation_samples", len(samples) >= 20)
    for role in ("leader", "foreground", "ctl"):
        rows = [r for r in samples if r.get("role") == role]
        require(len(rows) >= 2, "missing " + role + " identity observations")
        first = valid_identity(rows[0]["identity"])
        previous_at = -1
        for row in rows:
            require(stamp(row.get("at")) and row["at"] > previous_at
                    and all(type(row.get(k)) is bool for k in ("ctty", "alive", "early_eof"))
                    and integer(row.get("hup")) and integer(row.get("session_id"))
                    and type(row.get("foreground_pgid")) is int and (row["foreground_pgid"] >= 0 or
                        role == "foreground" and any(w.get("leader_exit_lower_at", w["leader_exit_at"]) <= row["at"] <= w.get("draining_end_at", w["end"]) for w in drains)), "invalid process observation")
            previous_at = row["at"]
            check("identity_" + role, valid_identity(row["identity"]) == first and row.get("nonce") == nonce)
            draining = role == "foreground" and any(w.get("leader_exit_lower_at", w["leader_exit_at"]) <= row["at"] <= w.get("draining_end_at", w["end"]) for w in drains)
            check("ctty_" + role, row["session_id"] > 1 and (draining or row["ctty"] and row["foreground_pgid"] > 1))
            check("alive_" + role, row["alive"])
            check("hup_" + role, row["hup"] <= 1 if draining else row["hup"] == 0)
            check("early_eof_" + role, not row["early_eof"])
            if role == "foreground":
                check("foreground_group", (row["identity"].get("process_group") if draining else row["foreground_pgid"]) == row["identity"]["pid"])
        for window in windows:
            check("identity_window_" + role, rows[0]["at"] < window["start"] and
                  (window.get("kind") == "draining" and role == "leader" or rows[-1]["at"] > window["end"]))
    leader = next(r for r in samples if r["role"] == "leader")
    foreground = next(r for r in samples if r["role"] == "foreground")
    check("foreground_distinct", leader["identity"]["pid"] != foreground["identity"]["pid"])
    check("foreground_session", leader["session_id"] == foreground["session_id"])

    images = doc["images"]
    switches = any(w.get("kind", "image") in ("image", "watchdog", "draining") for w in windows)
    require(isinstance(images, list) and len(images) >= (2 if switches else 1), "missing running image observations")
    original = valid_identity(images[0]["identity"])
    for i, image in enumerate(images):
        require(re.fullmatch(r"[0-9a-f]{64}", str(image.get("sha256", "")))
                and all(integer(image.get(k)) for k in ("generation", "spawn_count", "terminal_count"))
                and (type(image.get("exit_code")) is int or negative_legacy and image.get("exit_code") is None and image["terminal_count"] == 0) and image.get("phase") in (("none", "active", "committed", "rolled_back") if legacy or i == 0 else ("active", "committed", "rolled_back")),
                "invalid image/marker receipt")
        check("agent_identity", valid_identity(image["identity"]) == original)
        check("image_generation", legacy or image["generation"] == images[0]["generation"] + i)
        check("one_spawn", image["spawn_count"] == 1)
        if i:
            check("image_changed", not switches or image["sha256"] != images[i-1]["sha256"], setup=True)
    check("courier_terminal", images[-1]["terminal_count"] == 1 and images[-1]["exit_code"] == 23)
    check("agent_parent", leader["identity"].get("ppid") == images[0]["identity"]["pid"])

    controls = doc["controls"]
    require(isinstance(controls, list), "invalid control evidence")
    resize = [c for c in controls if c.get("op") == "resize"]
    check("resize_nonempty", len(resize) >= 2)
    for i, control in enumerate(controls):
        require(integer(control.get("seq")) and integer(control.get("ack")) and stamp(control.get("at"))
                and control.get("op") in ("resize", "signal", "keepalive", "hangup"), "invalid control receipt")
        check("control_sequence", control["seq"] == control["ack"] == 0 if legacy else
              control["seq"] > (controls[i-1]["seq"] if i else 0) and control["ack"] == control["seq"])
        if control["op"] == "resize":
            require(all(integer(control.get(k)) for k in ("cols", "rows", "observed_cols", "observed_rows")), "invalid resize")
            check("resize_effect", control["cols"] > 0 and control["rows"] > 0
                  and (control["cols"], control["rows"]) == (control["observed_cols"], control["observed_rows"]))
    for window in windows:
        check("resize_cut", any(c["at"] < window["start"] for c in resize) and any(c["at"] > window["end"] for c in resize))

    ctl = doc["ctl"]
    require(integer(ctl.get("pid")) and integer(ctl.get("start_ticks"))
            and type(ctl.get("wait_status")) is int and ctl["wait_status"] >= 0
            and type(ctl.get("reaped")) is bool, "missing original ctl waitpid")
    observed_ctl = next(r for r in samples if r["role"] == "ctl")["identity"]
    check("ctl_original", (ctl["pid"], ctl["start_ticks"]) == (observed_ctl["pid"], observed_ctl["start_ticks"]))
    check("ctl_reaped", ctl["reaped"] and os.WIFEXITED(ctl["wait_status"]))
    check("ctl_exit", os.waitstatus_to_exitcode(ctl["wait_status"]) == 23 if os.WIFEXITED(ctl["wait_status"]) or os.WIFSIGNALED(ctl["wait_status"]) else False)

    resources = doc["resources"]
    require(all(k in resources for k in ("before", "after", "settled"))
            and isinstance(resources["after"], list) and resources["after"], "missing PTY census")
    snapshots = [resources["before"], *resources["after"], resources["settled"]]
    for census in snapshots:
        require(isinstance(census.get("counts"), dict) and all(integer(census["counts"].get(k)) for k in KINDS)
                and isinstance(census.get("masters"), list) and isinstance(census.get("wrongfd"), list)
                and isinstance(census.get("zombies"), list) and type(census.get("foreground_alive")) is bool,
                "invalid resource census")
        valid_identity(census["owner"])
        for master in census["masters"]:
            require(all(integer(master.get(k)) for k in MASTER_KEYS), "missing master identity")
        check("resource_owner", valid_identity(census["owner"]) == original)
        check("wrongfd", not census["wrongfd"])
        # origin: hot-update S4 internal review rv-drill DRILL-3. ReapHeld
        # retains only the original exited leader while its slave tail is live.
        # Settlement and unrelated identities never inherit this exception.
        held = (census is not resources["settled"] and stamp(census.get("at")) and
                census["foreground_alive"] and census["counts"]["master"] == 1 and
                any(w["leader_exit_at"] <= census["at"] <= w.get("draining_end_at", w["end"]) for w in drains))
        check("zombies", all(held and z.get("state") == "Z" and
              valid_identity(z) == valid_identity(leader["identity"]) and
              z.get("session_id") == leader["session_id"] for z in census["zombies"]))
        check("handoff", census["counts"]["handoff"] == 0)
        check("master_count", census["counts"]["master"] == len(census["masters"]))
    before = resources["before"]
    check("resources_retained", before["counts"]["master"] == 1 and (legacy or
          before["counts"]["replay"] == 2 and before["counts"]["pidfd"] == 1))
    for census in resources["after"]:
        check("resources_stable", census["counts"] == before["counts"] and census["masters"] == before["masters"])
        check("foreground_retained", census["foreground_alive"])
    settled = resources["settled"]
    check("resources_settled", all(settled["counts"][k] == 0 for k in KINDS) and not settled["foreground_alive"])

    logs = doc["logs"]
    require(type(logs.get("read_ok")) is bool and isinstance(logs.get("cursors"), list), "missing logs")
    require(sorted(c.get("stream", "") for c in logs["cursors"]) == sorted(STREAMS), "missing/duplicate log stream")
    check("log_read_ok", logs["read_ok"], setup=True)
    log_objects = set()
    for cursor in logs["cursors"]:
        require(all(integer(cursor.get(k)) for k in ("inode", "inode_before", "before", "after")) and cursor["inode"] > 0
                and type(cursor.get("read_ok")) is bool and isinstance(cursor.get("required"), (list, dict)), "invalid log cursor")
        require(Path(cursor["file"]).name == cursor["stream"] + ".log", "log stream points at another stream's bytes")
        log_stat = safe_file(root, cursor["file"]).stat()
        log_object = (log_stat.st_dev, log_stat.st_ino)
        require(log_object not in log_objects, "log streams share one evidence inode")
        log_objects.add(log_object)
        content = blob_bytes(root, cursor, records=False)
        check("log_cursor", cursor["read_ok"] and cursor["after"] >= cursor["before"]
              and cursor["after"] - cursor["before"] == len(content), setup=True)
        check("log_inode", cursor["inode"] == cursor["inode_before"], setup=True)
        required = cursor["required"]
        if cursor["stream"] == "agent_slog":
            require(isinstance(required, dict) and set(required) == {"started", "activation", "exit"},
                    "missing started/activation/exit log audit")
            require(all(isinstance(parts, list) and (parts or negative_legacy and key == "exit" and
                        doc["legacy_loss"]["terminal_missing"]) and all(isinstance(p, str) and p for p in parts)
                        for key, parts in required.items()), "invalid log audit fragments")
            if nonce not in required["started"]:
                binding = doc.get("audit_binding", {})
                require(binding.get("nonce") == nonce and binding.get("pid") and
                        re.fullmatch(r"[0-9a-f]{64}", str(binding.get("request_id_hash", ""))) and
                        (binding.get("os_pid"), binding.get("start_time_ticks")) ==
                        (leader["identity"]["pid"], leader["identity"]["start_ticks"]),
                        "started audit not bound to nonce")
                start = next((r for _, r in parse_slog(content) if r.get("msg") ==
                              "agent: run reliable=true started" and r.get("pid") == binding["pid"]), None)
                require(start is not None and all(str(start.get(k)) == str(binding[k]) for k in
                        ("pid", "os_pid", "start_time_ticks", "request_id_hash")), "foreign started audit")
            check("log_nonempty", bool(content), setup=True)
            check("log_required", all(any(all(part.encode() in line for part in parts)
                  for line in content.splitlines()) for parts in required.values()), setup=True)
        else:
            require(isinstance(required, list) and all(isinstance(p, str) and p for p in required), "invalid required log fragments")
            check("log_required", all(part.encode() in content for part in required))
        check("log_panic", not re.search(rb"(?:panic:|fatal error:|DATA RACE)", content))
    watchdogs = [w for w in windows if w.get("kind") == "watchdog"]
    for window in watchdogs:
        require(stamp(window.get("blackhole_s")), "missing watchdog blackhole duration")
        check("watchdog_nonvacuous", window["blackhole_s"] >= 120 and len(images) >= 3 and images[-1]["phase"] == "rolled_back"
              and images[-1]["sha256"] == images[0]["sha256"], setup=True)
    for direction in ("input", "output"):
        times = [r["at"] for r in doc[direction]["received"]["records"]]
        # origin: hot-update S4 plan §9.2 / T48. A's 5s performance promise
        # does not apply to W's deliberately observed 120s carrier blackhole.
        # W still uses the original ctl's absolute 180s recovery cap.
        # origin: hot-update S4 internal review rv-drill DRILL-2. Exempt
        # only a gap crossing the independently observed outage, with <=5s
        # unexplained silence at either edge and one original 180s deadline.
        recovery = [w for w in windows if w.get("kind") in ("watchdog", "carrier")]
        check("business_stall_" + direction, all(b-a <= 5 or any(
              w["start"]-5 <= a <= w["end"] and w["start"] <= b <= w["end"]+5
              and b <= w["start"]+180 for w in recovery)
              for a, b in zip(times, times[1:])))
    return dict(valid=not invalid, continuous=not failures, reason=",".join(dict.fromkeys(failures)))


def verify_legacy_fault(root, nonce, doc):
    """Prove supply and acquisition before interpreting absent old ownership.

    origin: hot-update S4 internal review rv-drill DRILL-4. P alone may
    preserve loss; unreadable logs or a different failure remain invalid.
    """
    if doc["ctl"].get("timed_out"):
        observation = doc["ctl"].get("timeout_observation", {})
        require(observation.get("nonce") == nonce and stamp(observation.get("elapsed_s")) and
                stamp(observation.get("deadline_s")) and observation["deadline_s"] > 0 and
                observation["elapsed_s"] >= observation["deadline_s"] and
                (observation["identity"]["pid"], observation["identity"]["start_ticks"]) ==
                (doc["ctl"]["pid"], doc["ctl"]["start_ticks"]) and
                os.WIFSIGNALED(doc["ctl"]["wait_status"]), "unproved legacy ctl timeout")
    loss = doc.get("legacy_loss", {})
    require(loss.get("nonce") == nonce and type(loss.get("terminal_missing")) is bool and
            type(loss.get("management_missing")) is bool, "missing legacy loss observations")
    runtime = runtime_records(root, nonce)
    require(runtime["protocol"] == "legacy" and runtime["pid"] == loss.get("task_pid") and
            loss["terminal_missing"] == (not runtime["exits"]), "legacy terminal absence differs from fresh log")
    management = read_json(root / "agent/settled.ps.json")
    tasks = [r for r in management["processes"] if r.get("pid") == runtime["pid"] and nonce in r.get("argv", [])]
    require(loss["management_missing"] == (not tasks), "legacy management absence differs from real ps")
    supply = read_json(root / "supply.json")
    images = doc["images"]
    require(len(images) == 2 and all(valid_identity(i["identity"]) == valid_identity(images[0]["identity"]) for i in images),
            "legacy fault lacks original agent replacement")
    require(supply["old_sha"] == images[0]["sha256"] and supply["new_sha"] == images[-1]["sha256"] and
            supply["old_sha"] != supply["new_sha"] and supply["artifact_sha"] == supply["served_sha"] and
            (supply["pid"], supply["start_ticks"]) == (images[0]["identity"]["pid"], images[0]["identity"]["start_ticks"]),
            "legacy fault supply differs from original process")
    hop = read_json(root / "hop-1.json")
    marker = hop["marker"]
    require(hop["pid"] == supply["pid"] and hop["start_ticks"] == supply["start_ticks"] and
            hop["running_sha"] == hop["disk_sha"] == supply["new_sha"] and marker["state"] == "committed" and
            marker["new_sha"] == supply["new_sha"] and marker["boot_count"] > 0 and
            marker["target_sid"] == "lab" and marker["target_nid"] == "agt1", "legacy bare exec not committed")
    endpoints = doc.get("fallback", {}).get("endpoints", [])
    require(any(r.get("role") == "agent" and r.get("old") is True and
                r.get("executable_sha256") == supply["old_sha"] for r in endpoints), "legacy exporter was not real old supply")
    require(any(r.get("msg", "").startswith("agent: upgrade COMMITTED") for _, r in runtime["rows"]), "missing old-style activation audit")


def frame(nonce, seq, data):
    return f"PTY1 {nonce} {seq} {len(data)} {sha(data)}\n".encode() + data


def payload(seq):
    # origin: hot-update S4 plan §9.1 / T48. Raw controls and split UTF-8/escape
    # bytes exercise exact transport; ISIG and VEOF are disabled in this stream.
    return struct.pack("!I", seq) + b"\x00\r\n\x1b[31m\xe2\x82\xac\x04\x03\xff" + bytes(range(64))


def parse_frames(raw, nonce, complete=True):
    records, offset = [], 0
    while offset < len(raw):
        end = raw.find(b"\n", offset)
        if end < 0:
            break
        match = re.fullmatch(rb"PTY1 ([0-9a-f]{32}) ([0-9]+) ([0-9]+) ([0-9a-f]{64})", raw[offset:end])
        require(match is not None and match[1].decode() == nonce, "foreign/malformed raw protocol")
        length = int(match[3])
        require(length <= 32768, "oversized raw frame")
        finish = end + 1 + length
        if finish > len(raw):
            break
        require(sha(raw[end+1:finish]) == match[4].decode(), "raw protocol payload hash mismatch")
        records.append(dict(id=int(match[2]), offset=offset, length=finish-offset, sha256=sha(raw[offset:finish])))
        offset = finish
    require(not complete or offset == len(raw), "truncated raw protocol")
    return records


def describe(path, nonce, arrivals):
    raw = path.read_bytes()
    # Retain broken output as evidence rather than throwing away the very
    # discontinuity the legacy negative control is supposed to observe.
    try:
        records = parse_frames(raw, nonce, complete=False)
    except EvidenceError:
        records = []
    offset = sum(r["length"] for r in records)
    if offset < len(raw):
        records.append(dict(id=len(records), offset=offset, length=len(raw)-offset,
                            sha256=sha(raw[offset:])))
    for row in records:
        end = row["offset"] + row["length"]
        row["at"] = next(at for count, at in arrivals if count >= end)
    return dict(file=path.name, length=len(raw), sha256=sha(raw), records=records)


def census(pid, foreground=None, baseline=None):
    # origin: hot-update S4 plan §9.1 / T48. PTYs are a separate classifier;
    # do not relax drill 34's existing unsafe-ptmx policy.
    owner = identity(pid)
    counts = dict.fromkeys(KINDS, 0)
    masters, wrongfd = [], []
    objects = {kind: [] for kind in KINDS}
    if baseline is not None:
        require(valid_identity(baseline["owner"]) == valid_identity(owner)
                and isinstance(baseline.get("objects"), dict)
                and all(isinstance(baseline["objects"].get(k), list) for k in KINDS),
                "census baseline belongs to another owner or lacks objects")
    for path in sorted(Path(f"/proc/{pid}/fd").iterdir(), key=lambda p: int(p.name)):
        try:
            link = os.readlink(path)
        except FileNotFoundError:
            continue
        info = path.stat()
        fdinfo = Path(f"/proc/{pid}/fdinfo/{path.name}").read_text()
        flags = re.search(r"^flags:\s+([0-7]+)$", fdinfo, re.M)
        require(flags is not None, "descriptor flags unreadable")
        if int(path.name) > 2 and not int(flags[1], 8) & os.O_CLOEXEC:
            wrongfd.append(dict(fd=int(path.name), target=link, reason="inherited"))
        kind = None
        item = dict(dev=info.st_dev, ino=info.st_ino, rdev=info.st_rdev)
        if re.fullmatch(r"/memfd:tether-replay(?: \(deleted\))?", link):
            kind = "replay"
        elif re.fullmatch(r"/memfd:tether-handoff(?: \(deleted\))?", link):
            kind = "handoff"
        elif link == "anon_inode:[pidfd]":
            kind = "pidfd"
            target = re.search(r"^Pid:\s+(-?\d+)$", fdinfo, re.M)
            require(target is not None, "pidfd target unreadable")
            # A dead pidfd reports Pid=-1; its inode still identifies the held
            # descriptor. Dropping it from the baseline would hide a final leak.
        elif link.endswith("/ptmx") or link == "/dev/ptmx":
            mount = re.search(r"^mnt_id:\s+(\d+)$", fdinfo, re.M)
            index = re.search(r"^tty-index:\s+(\d+)$", fdinfo, re.M)
            require(mount is not None and index is not None, "master fdinfo missing identity")
            item.update(mount_id=int(mount[1]), pty_index=int(index[1]))
            kind = "master"
        elif int(path.name) > 2 and ("/dev/pts/" in link or stat.S_ISCHR(info.st_mode)):
            wrongfd.append(dict(fd=int(path.name), target=link, reason="unexpected_device"))
        if kind is not None:
            # origin: hot-update S4 plan §9.1 / T48. A baseline captured before
            # mixed traffic identifies this task's kernel objects. Other tasks
            # must not inflate its leak counters; duplicate references DO count.
            if baseline is not None and kind != "handoff" and item not in baseline["objects"][kind]:
                continue
            objects[kind].append(item)
            counts[kind] += 1
            if kind == "master":
                masters.append(item)
    zombies, fg_alive = [], False
    # origin: hot-update S4 drill-run2. The walk sees every task, pid 1 and
    # kernel threads included; only the foreground receipt must be a valid
    # identity. Validating each child here crashed every real snapshot.
    fg_key = valid_identity(foreground) if foreground else None
    for path in Path("/proc").iterdir():
        if not path.name.isdigit():
            continue
        try:
            child = identity(int(path.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        related = child["ppid"] == pid or foreground and child["session_id"] == foreground.get("session_id")
        if related and child["state"] == "Z":
            zombies.append(child)
        if fg_key is not None and (child["pid"], child["start_ticks"], child["boot_id"]) == fg_key:
            fg_alive = child["state"] != "Z"
    require(valid_identity(identity(pid)) == valid_identity(owner), "census owner changed during scan")
    return dict(owner=owner, counts=counts, objects=objects, masters=masters, wrongfd=wrongfd,
                zombies=zombies, foreground_alive=fg_alive)


def trace_leader(args):
    # origin: hot-update S4 plan §9.2 / C2. The fixture leader and foreground
    # are real separate processes in one controlling session. No third process
    # keeps the slave open after the leader exits; the foreground owns its tail.
    root = Path(args.evidence)
    root.mkdir(parents=True, exist_ok=True)
    require(os.isatty(0), "fixture leader has no controlling terminal")
    original = identity(os.getpid())
    require(original["tty_nr"] != 0, "fixture leader lacks a controlling tty")
    write_json(root / "leader.identity.json", dict(nonce=args.nonce, identity=original))
    read_end, write_end = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(write_end)
        os.setpgid(0, 0)
        os.read(read_end, 1)
        os.close(read_end)
        os.execv(sys.executable, [sys.executable, "-I", "-B", str(Path(__file__).resolve()),
                 "trace", "--role", "remote", "--evidence", str(root), "--nonce", args.nonce,
                 "--period", str(args.period), "--timeout", str(args.timeout), "--scenario", args.scenario])
    os.close(read_end)
    os.setpgid(pid, pid)
    os.tcsetpgrp(0, pid)
    os.write(write_end, b"1")
    os.close(write_end)
    deadline = time.monotonic()+args.timeout
    while True:
        waited, status = os.waitpid(pid, os.WNOHANG)
        if waited == pid:
            return os.waitstatus_to_exitcode(status)
        if args.scenario == "C2" and (root / "draining").exists():
            return 23
        require(time.monotonic() < deadline, "fixture leader deadline exceeded")
        time.sleep(.01)


def trace_remote(args):
    root = Path(args.evidence)
    root.mkdir(parents=True, exist_ok=True)
    require(os.isatty(0), "remote fixture has no PTY")
    tty.setraw(0, termios.TCSANOW)
    origin = time.monotonic()
    original = identity(os.getpid())
    write_json(root / "remote.identity.json", dict(nonce=args.nonce, identity=original))
    arrivals, sent_arrivals, raw, seq, sent_count = [], [], b"", 0, 0
    hup, early_eof, normal_stop, samples = [0], False, False, []
    backlog_packets = 0
    canonical = dict(active=False, eof=False, sigint=0)
    stop_at = None
    def canonical_receipt(phase):
        attrs = termios.tcgetattr(0)
        write_json(root / "canonical.remote.json", dict(nonce=args.nonce, phase=phase, identity=identity(os.getpid()),
                   eof=canonical["eof"], sigint=canonical["sigint"], lflag=attrs[3], at=time.monotonic()))
    def on_int(*_):
        require(canonical["active"], "SIGINT outside canonical fixture")
        canonical["sigint"] += 1
        tty.setraw(0, termios.TCSANOW)
        canonical["active"] = False
        canonical_receipt("complete")
    if args.scenario == "C2":
        signal.signal(signal.SIGINT, on_int)

    class HupStop(Exception):
        pass

    def on_hup(*_):
        # origin: hot-update S4 plan §9.3 / T48. The fixture is HUP-sensitive.
        # Its death is a product observation, not an expect-alive setup check.
        hup[0] += 1
        if args.scenario == "C2" and (root / "draining").exists():
            # C2 deliberately exits the session leader. Linux sends HUP to
            # its foreground even with the original slave retained. This
            # dedicated tail fixture records that event and keeps writing;
            # A/W/P retain the HUP-sensitive fixture without this marker.
            return
        raise HupStop()

    def save_trace():
        write_json(root / "remote.trace.json", dict(nonce=args.nonce, hup=hup[0], early_eof=early_eof,
                   normal_stop=normal_stop, samples=samples, started_monotonic=origin,
                   input=describe(root / "remote.received", args.nonce, arrivals),
                   output=describe(root / "remote.sent", args.nonce, sent_arrivals)))

    # origin: hot-update S4 plan §9.3 / T48; drill-run3 P. Even the first
    # HUP must wait for a completed nonblocking syscall's raw bytes AND time
    # receipt. Otherwise final describe lacks an arrival for the last bytes.
    # The saved handler runs immediately after bookkeeping, before further I/O.
    @contextlib.contextmanager
    def byte_receipt():
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGHUP})
        try:
            yield
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)

    signal.signal(signal.SIGHUP, on_hup)
    os.set_blocking(1, False)
    try:
        with (root / "remote.sent").open("xb", buffering=0) as sent, (root / "remote.received").open("xb", buffering=0) as received:
            while True:
                ident = identity(os.getpid())
                at = time.monotonic()-origin
                cols_rows = fcntl.ioctl(0, termios.TIOCGWINSZ, bytes(8))
                rows, cols, _, _ = struct.unpack("HHHH", cols_rows)
                samples.append(dict(role="foreground", nonce=args.nonce, at=at, identity=ident,
                    ctty=ident["tty_nr"] != 0, session_id=ident["session_id"], foreground_pgid=ident["foreground_pgid"],
                    alive=True, hup=hup[0], early_eof=False, cols=cols, rows=rows))
                write_json(root / "remote.observation.json", samples[-1])
                try:
                    leader = identity(original["ppid"])
                    if leader["state"] != "Z":
                        samples.append(dict(role="leader", nonce=args.nonce, at=at, identity=leader,
                            ctty=leader["tty_nr"] != 0, session_id=leader["session_id"],
                            foreground_pgid=leader["foreground_pgid"], alive=True,
                            hup=0, early_eof=False))
                except (FileNotFoundError, ProcessLookupError):
                    # A draining leader may already be reaped; retain the actual
                    # prior identity rather than sampling a replacement PID.
                    pass
                data = payload(seq)
                # origin: hot-update S4 plan §9.2 / T48; drill-run4 W.
                # Fill beyond local/kernel buffers while the driver withholds
                # reads. A bounded burst leaves replay capacity for the outage.
                # origin: hot-update S4 plan §9.3 / S5 T48 / DRILL-RUN 39.P.
                # P declares a finite 2 MiB burst to fill the legacy path; W
                # retains its existing eight-packet replay pressure budget.
                pressure_packets = 64 if args.scenario == "P" else 8
                if args.scenario in ("W", "P") and (root / "pause-input").exists() and backlog_packets < pressure_packets:
                    data += bytes(32768-len(data))
                    backlog_packets += 1
                packet, offset = frame(args.nonce, seq, data), 0
                while offset < len(packet):
                    require(time.monotonic()-origin < args.timeout, "remote write deadline exceeded")
                    if not select.select([], [1], [], .05)[1]:
                        continue
                    with byte_receipt():
                        try:
                            count = os.write(1, packet[offset:])
                        except BlockingIOError:
                            continue
                        sent.write(packet[offset:offset+count])
                        sent_count += count
                        sent_arrivals.append((sent_count, time.monotonic()-origin))
                        offset += count
                seq += 1
                readable = False
                if (root / "pause-input").exists():
                    time.sleep(args.period)
                else:
                    readable = bool(select.select([0], [], [], args.period)[0])
                if readable:
                    with byte_receipt():
                        chunk = os.read(0, 32768)
                        if chunk:
                            received.write(chunk)
                            raw += chunk
                            arrivals.append((len(raw), time.monotonic()-origin))
                    if not chunk and canonical["active"]:
                        canonical["eof"] = True
                        canonical_receipt("eof")
                        continue
                    if not chunk:
                        early_eof = True
                        break
                    parsed = parse_frames(raw, args.nonce, complete=False)
                    if parsed:
                        row = parsed[-1]
                        part = raw[row["offset"]:row["offset"]+row["length"]]
                        if part.endswith(b"CANON") and args.scenario == "C2":
                            attrs = termios.tcgetattr(0)
                            attrs[3] |= termios.ICANON | termios.ISIG
                            attrs[3] &= ~(termios.ECHO | termios.ECHONL)
                            attrs[6][termios.VEOF], attrs[6][termios.VINTR] = b"\x04", b"\x03"
                            termios.tcsetattr(0, termios.TCSANOW, attrs)
                            canonical["active"] = True
                            canonical_receipt("ready")
                        if part.endswith(b"STOP"):
                            normal_stop = True
                            if args.scenario != "C2":
                                break
                            # Logical local FIN closes input responsibility;
                            # the original slave keeps an observable output tail.
                            stop_at = time.monotonic()
                if stop_at is not None and time.monotonic()-stop_at > .3:
                    write_json(root / "fin.remote.json", dict(nonce=args.nonce, tail_s=time.monotonic()-stop_at,
                               hup=hup[0], identity=identity(os.getpid())))
                    break
                if seq % 10 == 0:
                    save_trace()
                require(time.monotonic()-origin < args.timeout, "remote fixture deadline exceeded")
    except (HupStop, BrokenPipeError):
        pass
    finally:
        # origin: hot-update S4 plan §9.3 / T48; drill-run3 P. Master loss
        # and leader exit can both deliver HUP. Once this HUP-sensitive worker
        # has stopped, a second HUP must not interrupt its final file receipts
        # and leave periodic hashes over newer bytes. No PTY I/O resumes here.
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGHUP})
        if (root / "remote.sent").exists() and (root / "remote.received").exists():
            if samples:
                final = dict(next(r for r in reversed(samples) if r["role"] == "foreground"),
                             at=time.monotonic()-origin, hup=hup[0], early_eof=early_eof)
                samples.append(final)
            if args.scenario == "C2" and (root / "fin.remote.json").exists():
                # The last tty sample still belongs to the live slave tail.
                # Close its measured interval after that sample, before exit.
                # origin: hot-update S4 internal review rv-drill DRILL-3.
                fin = read_json(root / "fin.remote.json")
                fin["source_ended_monotonic"] = time.monotonic()
                write_json(root / "fin.remote.json", fin)
            save_trace()
    return 23 if normal_stop else 128 + signal.SIGHUP if hup[0] else 74


def legacy_attach_barrier(root, nonce, pid):
    """Hold the real ctl's TCSETS entry after ready and before attach.

    origin: hot-update S4 plan §9.2 / 36.L. No timing sleep, binary patch or
    substitute attach request can prove this boundary. Unsupported ptrace is
    an evidence error; it never falls through to a running-session refusal.
    """
    import ctypes
    require(sys.platform == "linux" and os.uname().machine == "x86_64", "legacy attach barrier requires Linux x86_64")
    libc = ctypes.CDLL(None, use_errno=True)
    libc.ptrace.restype = ctypes.c_long
    def ptrace(op, tid, addr=0, data=0):
        result = libc.ptrace(ctypes.c_uint(op), ctypes.c_uint(tid), ctypes.c_void_p(addr), ctypes.c_void_p(data))
        require(result != -1, "ptrace legacy attach barrier: " + os.strerror(ctypes.get_errno()))
        return result
    waited, status = os.waitpid(pid, 0)
    require(waited == pid and os.WIFSTOPPED(status), "legacy ctl did not enter trace barrier")
    ptrace(0x4200, pid, data=1 | 8)  # TRACESYSGOOD | TRACECLONE.
    ptrace(24, pid)
    traced, held, released, wake_pending = {pid}, None, False, set()
    deadline = time.monotonic()+12
    while traced:
        require(time.monotonic() < deadline, "pre-attach barrier timed out")
        if held is not None and (root / "attach.release").exists() and not released:
            wake_pending = traced - {held}
            for tid in wake_pending:
                # Wake traced blocking syscalls with a suppressed trace signal,
                # so detach cannot wait on an unrelated Go network poller.
                require(libc.syscall(200, tid, signal.SIGTRAP) == 0, "cannot wake legacy trace thread")
            ptrace(17, held)  # DETACH resumes the original ioctl unchanged.
            traced.remove(held)
            held, released = None, True
        tid, status = os.waitpid(-1, os.WNOHANG | 0x40000000)
        if tid == 0:
            time.sleep(.0001)
            continue
        require(os.WIFSTOPPED(status), "legacy ctl exited before attach barrier release")
        traced.add(tid)
        sig, event = os.WSTOPSIG(status), status >> 16
        if event == 3:
            child = ctypes.c_ulonglong()
            ptrace(0x4201, tid, data=ctypes.addressof(child))
            traced.add(child.value)
        if released:
            if tid not in wake_pending or sig == signal.SIGTRAP and event == 0:
                # Detach only at delivery of the wake signal. Detaching at an
                # earlier syscall stop would deliver the pending SIGTRAP to ctl.
                ptrace(17, tid)
                traced.remove(tid)
                wake_pending.discard(tid)
            else:
                ptrace(7, tid)
            continue
        if sig == (signal.SIGTRAP | 0x80):
            registers = (ctypes.c_ulonglong * 27)()
            ptrace(12, tid, data=ctypes.addressof(registers))
            if registers[15] == 16 and registers[14] == 0 and registers[13] == termios.TCSETS and registers[10] == (1 << 64)-38:
                require(held is None, "duplicate pre-attach TCSETS")
                held = tid
                write_json(root / "before-attach.json", dict(version=1, nonce=nonce, identity=identity(pid),
                           tid=tid, syscall="ioctl", fd=0, request=termios.TCSETS, at=time.monotonic()))
                continue
        deliver = 0 if sig in (signal.SIGTRAP, signal.SIGSTOP, signal.SIGTRAP | 0x80) else sig
        ptrace(24, tid, data=deliver)
    require(released, "legacy attach gate was never observed")
    write_json(root / "attached.json", dict(version=1, nonce=nonce, identity=identity(pid), at=time.monotonic(), released=True))


def trace_ctl(args):
    root = Path(args.evidence)
    root.mkdir(parents=True, exist_ok=True)
    require(args.ctl_binary and Path(args.ctl_binary).is_absolute(), "ctl binary must be an explicit absolute path")
    argv = [args.ctl_binary] + (args.argv[1:] if args.argv[:1] == ["--"] else args.argv)
    if len(argv) == 1:
        argv += ["run", args.node, "--", "python3", args.remote_probe or str(Path(__file__).resolve()),
                 "trace", "--role", "leader", "--scenario", args.scenario,
                 "--evidence", args.remote_evidence or args.evidence, "--nonce", args.nonce,
                 "--period", str(args.period), "--timeout", str(args.timeout)]
    master, slave = os.openpty()
    tty.setraw(slave, termios.TCSANOW)
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
    read_end, write_end = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(read_end)
            os.close(master)
            os.setsid()
            # origin: hot-update S4 plan §9.1 / T48. A PTY fd alone is not a
            # controlling terminal; failing this ioctl is a setup failure.
            fcntl.ioctl(slave, termios.TIOCSCTTY, 0)
            os.tcsetpgrp(slave, os.getpgrp())
            for fd in range(3):
                os.dup2(slave, fd)
            if slave > 2:
                os.close(slave)
            os.write(write_end, json.dumps(identity(os.getpid())).encode() + b"\n")
            os.close(write_end)
            if args.scenario == "L":
                import ctypes
                libc = ctypes.CDLL(None, use_errno=True)
                require(libc.ptrace(0, 0, 0, 0) == 0, "cannot trace original legacy ctl")
                os.kill(os.getpid(), signal.SIGSTOP)
            os.execv(argv[0], argv)
        except BaseException as exc:
            with contextlib.suppress(OSError):
                os.write(write_end, ("ERROR " + str(exc)).encode())
            os._exit(2)
    os.close(write_end)
    origin = time.monotonic()
    wall_origin = time.time()
    wall_span = time.monotonic()-origin
    status, stopped, seq = None, False, 0
    timeout_observation = None
    arrivals, sent_arrivals, sent_count, received_count, samples = [], [], 0, 0, []
    try:
        require(select.select([read_end], [], [], 5)[0], "controlling tty handshake timed out")
        receipt = os.read(read_end, 4096)
        require(receipt and not receipt.startswith(b"ERROR"), "TIOCSCTTY failed: " + receipt.decode())
        ident = json.loads(receipt)
        require(ident["tty_nr"] != 0 and ident["session_id"] == pid and ident["foreground_pgid"] == pid, "ctl controlling tty not established")
        write_json(root / "ctl.identity.json", dict(nonce=args.nonce, identity=ident, started_monotonic=origin, started_wall=wall_origin))
        if args.scenario == "L":
            legacy_attach_barrier(root, args.nonce, pid)
        os.set_blocking(master, False)
        pending, next_at, ready_raw, resized = b"", 0, False, set()
        canonical_paused, canonical_steps, local_fin = False, set(), False
        (root / "canonical.sent").write_bytes(b"")
        sampled = -1
        def save_trace():
            write_json(root / "ctl.trace.json", dict(nonce=args.nonce, started_monotonic=origin,
                       started_wall=wall_origin, wall_span_s=wall_span, scenario=args.scenario, samples=samples, input=describe(root / "ctl.sent", args.nonce, sent_arrivals),
                       output=describe(root / "ctl.received", args.nonce, arrivals)))
        with (root / "ctl.sent").open("xb", buffering=0) as sent, (root / "ctl.received").open("xb", buffering=0) as received:
            while status is None:
                now = time.monotonic()-origin
                waited, candidate = os.waitpid(pid, os.WNOHANG)
                if waited == pid:
                    status = candidate
                    break
                if now >= args.timeout:
                    # origin: hot-update S4 internal review rv-drill DRILL-4.
                    # Lost legacy wait ownership may leave the original ctl
                    # waiting forever. Preserve that measured timeout and its
                    # actual waitpid status; never call it an autonomous exit.
                    require(args.scenario == "P", "original ctl waitpid deadline exceeded")
                    current = identity(pid)
                    require(valid_identity(current) == valid_identity(ident), "timed-out ctl identity changed")
                    timeout_observation = dict(nonce=args.nonce, identity=current, elapsed_s=now, deadline_s=args.timeout)
                    os.killpg(pid, signal.SIGKILL)
                    waited, status = os.waitpid(pid, 0)
                    require(waited == pid, "timeout waitpid belongs to another ctl")
                    break
                if now - sampled >= .5:
                    current = identity(pid)
                    samples.append(dict(role="ctl", nonce=args.nonce, at=now, identity=current,
                        ctty=current["tty_nr"] != 0, session_id=current["session_id"],
                        foreground_pgid=current["foreground_pgid"], alive=current["state"] != "Z",
                        hup=0, early_eof=False))
                    save_trace()
                    sampled = now
                request_path = root / "resize.request.json"
                if request_path.exists():
                    resize = read_json(request_path)
                    require(resize.get("nonce") == args.nonce and isinstance(resize.get("name"), str)
                            and all(type(resize.get(k)) is int and 0 < resize[k] <= 65535 for k in ("cols", "rows")),
                            "invalid local resize request")
                    if resize["name"] not in resized:
                        fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", resize["rows"], resize["cols"], 0, 0))
                        rows, cols, _, _ = struct.unpack("HHHH", fcntl.ioctl(master, termios.TIOCGWINSZ, bytes(8)))
                        write_json(root / "resize.receipt.json", dict(nonce=args.nonce, name=resize["name"],
                                   cols=cols, rows=rows, at=time.monotonic()-origin))
                        resized.add(resize["name"])
                if args.scenario == "C2" and not pending and canonical_paused:
                    for name, key in (("eof", b"\x04"), ("sigint", b"\x03")):
                        if (root / ("canonical."+name)).exists() and name not in canonical_steps:
                            require(os.write(master, key) == 1, "canonical control write incomplete")
                            with (root / "canonical.sent").open("ab") as action:
                                action.write(key)
                            canonical_steps.add(name)
                    if (root / "canonical.complete").exists():
                        canonical_paused = False
                if ready_raw and not pending and not stopped and not canonical_paused and now >= next_at:
                    stopped = (root / "finish").exists()
                    data = payload(seq)
                    data += bytes(args.input_bytes - len(data))
                    if args.scenario == "C2" and (root / "canonical.request").exists() and not canonical_steps:
                        data, canonical_paused = b"CANON", True
                    pending = frame(args.nonce, seq, b"STOP" if stopped else data)
                    seq += 1
                    next_at = now + args.period
                if args.scenario == "C2" and stopped and not pending and not local_fin:
                    # A canonical VEOF on the LOCAL slave yields read(0)==0 to
                    # the real ctl without closing its controlling terminal.
                    attrs = termios.tcgetattr(slave)
                    attrs[3] |= termios.ICANON
                    attrs[3] &= ~(termios.ECHO | termios.ISIG)
                    attrs[6][termios.VEOF] = b"\x04"
                    termios.tcsetattr(slave, termios.TCSANOW, attrs)
                    require(os.write(master, b"\x04\x04") == 2, "local FIN VEOF write incomplete")
                    write_json(root / "fin.local.json", dict(nonce=args.nonce, identity=ident,
                               lflag=termios.tcgetattr(slave)[3], veof=4, writes=2, at=time.monotonic()-origin))
                    local_fin = True
                # origin: hot-update S4 plan §9.2 / T48; drill-run4 W.
                # Apply real stdout backpressure, keeping the original ctl and
                # every byte intact. This never fabricates protocol offsets.
                output_paused = (root / "pause-output").exists()
                if output_paused and not (root / "output-paused.json").exists():
                    write_json(root / "output-paused.json", dict(nonce=args.nonce, identity=ident,
                               received=received_count, at=time.monotonic()-origin))
                ready, writable, _ = select.select([] if output_paused else [master], [master] if pending else [], [], .01)
                if writable:
                    try:
                        count = os.write(master, pending)
                    except BlockingIOError:
                        count = 0
                    if count:
                        sent.write(pending[:count])
                        sent_count += count
                        sent_arrivals.append((sent_count, time.monotonic()-origin))
                        pending = pending[count:]
                if ready:
                    chunk = os.read(master, 32768)
                    if chunk:
                        received.write(chunk)
                        received_count += len(chunk)
                        arrivals.append((received_count, time.monotonic()-origin))
                        if not ready_raw:
                            received.flush()
                            try:
                                ready_raw = bool(parse_frames((root / "ctl.received").read_bytes(), args.nonce, complete=False))
                            except EvidenceError:
                                pass
                waited, candidate = os.waitpid(pid, os.WNOHANG)
                if waited == pid:
                    status = candidate
            # Retained LOCAL slave prevents EOF; drain only available bytes
            # after original waitpid. Never wait for master EOF.
            while select.select([master], [], [], 0)[0]:
                try:
                    chunk = os.read(master, 32768)
                except OSError as exc:
                    if exc.errno != 5:
                        raise
                    break
                if not chunk:
                    break
                received.write(chunk)
                received_count += len(chunk)
                arrivals.append((received_count, time.monotonic()-origin))
        outcome = dict(pid=pid, start_ticks=ident["start_ticks"], wait_status=status, reaped=True)
        if timeout_observation is not None:
            outcome.update(timed_out=True, timeout_observation=timeout_observation)
        write_json(root / "ctl.wait.json", outcome)
        save_trace()
    finally:
        os.close(read_end)
        os.close(master)
        os.close(slave)
        if status is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
    return 0


def parse_slog(raw):
    rows = []
    for line in raw.decode("utf-8", errors="strict").splitlines():
        if line.lstrip().startswith("{"):
            row = json.loads(line)
        else:
            parts = shlex.split(line)
            require(all("=" in part for part in parts), "unparseable agent slog")
            row = dict(part.split("=", 1) for part in parts)
        require(isinstance(row, dict), "non-object agent slog record")
        rows.append((line, row))
    return rows


def runtime_records(root, nonce, terminal=True):
    """Join safe scalar records to the original child, never to a PID guess.

    origin: hot-update S4 plan §9.1 / §9.2 / T48; P18 observability.
    The four-stream snapshot proof must precede parsing. A ps row corroborates
    the task token but cannot fill an omitted or stale runtime state record.
    """
    trace = read_json(root / "ctl.trace.json")
    remote = read_json(root / "remote.trace.json")
    require(trace.get("nonce") == remote.get("nonce") == nonce, "foreign runtime receipts")
    ctl_identity = read_json(root / "ctl.identity.json")
    require(ctl_identity.get("nonce") == nonce, "foreign ctl identity")
    require(all(valid_identity(r["identity"]) == valid_identity(ctl_identity["identity"])
                for r in trace["samples"]), "runtime receipt belongs to another ctl")
    leader = next(r["identity"] for r in remote["samples"] if r["role"] == "leader")
    logs = read_json(root / "logs.json")
    cursor = next(c for c in logs["cursors"] if c["stream"] == "agent_slog")
    require(logs.get("read_ok") is True and cursor.get("read_ok") is True and
            cursor.get("inode") == cursor.get("inode_before") and cursor["before"] <= cursor["after"],
            "unverified runtime log snapshot")
    rows = parse_slog(blob_bytes(root, cursor, records=False))
    started = [(line, r) for line, r in rows if r.get("msg") == "agent: run reliable=true started" and
               str(r.get("os_pid")) == str(leader["pid"]) and
               str(r.get("start_time_ticks")) == str(leader["start_ticks"])]
    legacy = [(line, r) for line, r in rows if r.get("msg") == "agent: run" and nonce in str(r.get("argv", ""))]
    require(len(started) == 1 or not started and len(legacy) == 1, "missing or duplicated original-child run audit")
    reliable = bool(started)
    line, start = (started or legacy)[0]
    token = start["pid"]
    if reliable:
        require(re.fullmatch(r"[0-9a-f]{64}", str(start.get("request_id_hash", ""))), "missing request identity hash")
    def at(row):
        require(stamp(trace.get("started_wall")), "missing observed wall/monotonic clock pair")
        when = datetime.datetime.fromisoformat(row["time"].replace("Z", "+00:00"))
        require(when.tzinfo is not None, "slog timestamp has no timezone")
        result = when.timestamp()-trace["started_wall"]
        require(stamp(result), "runtime receipt predates original ctl")
        return result
    def uint(row, key):
        v = row.get(key)
        require(integer(v) or isinstance(v, str) and re.fullmatch(r"[0-9]+", v), "missing runtime " + key)
        return int(v)
    def flag(row, key):
        v = row.get(key)
        require(type(v) is bool or v in ("true", "false"), "missing runtime " + key)
        return v if type(v) is bool else v == "true"
    states, controls, exits, settlements = [], [], [], []
    for raw_line, row in rows:
        if row.get("pid") != token:
            continue
        msg = row.get("msg")
        if msg == "agent: pty image state":
            state = dict(at=at(row), phase=row.get("phase"), pid=token, source=raw_line)
            require(state["phase"] in ("cut", "activated", "carrier-lost", "carrier-restored"), "invalid runtime image phase")
            for key in ("generation", "input_applied", "input_accepted", "output_base", "output_end",
                        "control_seq", "ka_seq", "ka_deadline_boot_ns"):
                state[key] = uint(row, key)
            for key in ("input_fin", "input_ended", "output_fin", "revoked", "master_closed"):
                state[key] = flag(row, key)
            require(state["input_applied"] <= state["input_accepted"] and state["output_base"] <= state["output_end"],
                    "invalid runtime byte responsibility")
            states.append(state)
        elif msg == "agent: pty control committed":
            require(row.get("op") in ("resize", "signal", "hangup") and row.get("outcome") == "ok",
                    "missing successful committed control")
            controls.append(dict(at=at(row), op=row["op"], seq=uint(row, "seq"), ack=uint(row, "seq"), ack_source="agent_committed_receipt",
                                 clock_resolution_s=(.000001 if raw_line.lstrip().startswith("{") else .001)+trace.get("wall_span_s", 0), source=raw_line))
        elif msg == ("agent: run reliable=true exited" if reliable else "agent: run exit"):
            exit_row = dict(code=int(row["code"]), source=raw_line, at=at(row))
            if reliable:
                exit_row["signal"] = uint(row, "signal")
                for key in ("input_applied", "input_accepted", "output_base", "output_end"):
                    exit_row[key] = uint(row, key)
                for key in ("input_fin", "input_ended", "output_fin"):
                    exit_row[key] = flag(row, key)
            exits.append(exit_row)
        elif reliable and msg == "agent: run reliable=true settled":
            settlement = dict(pid=token, code=int(row["code"]), signal=uint(row, "signal"), source=raw_line, at=at(row))
            for key in ("input_applied", "input_accepted", "output_base", "output_end"):
                settlement[key] = uint(row, key)
            settlements.append(settlement)
    # origin: hot-update S4 internal review rv-drill DRILL-4. Bare exec
    # may erase the old wait owner; preserve absence only for the P control.
    negative_legacy = not reliable and trace.get("scenario") == "P"
    require(len(exits) <= 1 and (not terminal or negative_legacy or len(exits) == 1), "missing real terminal audit (cannot backfill from ps)")
    # origin: hot-update S4 plan §6.1 / §9.2 / T48. A carrier may rebuild
    # repeatedly in one generation; it must not occupy the image phase slots.
    carrier_pairs, lost = [], None
    for state in states:
        if state["phase"] == "carrier-lost":
            # Exec resets the transport-local loss flag. A later generation
            # can observe a fresh loss without carrying that flag in its ABI.
            require(lost is None or state["generation"] > lost["generation"], "duplicate carrier loss")
            lost = state
        elif state["phase"] == "carrier-restored":
            require(lost is not None and lost["at"] <= state["at"] and state["generation"] >= lost["generation"],
                    "carrier pair mismatched")
            require(all(state[k] >= lost[k] for k in ("input_applied", "input_accepted", "output_base", "output_end", "control_seq", "ka_seq")) and
                    (state["ka_deadline_boot_ns"] == lost["ka_deadline_boot_ns"] if state["ka_seq"] == lost["ka_seq"] else
                     state["ka_deadline_boot_ns"] >= lost["ka_deadline_boot_ns"]), "carrier lost current byte or KA responsibility")
            require(all(not lost[k] or state[k] for k in ("input_fin", "input_ended", "output_fin", "revoked", "master_closed")),
                    "carrier lost current terminal state")
            carrier_pairs.append(dict(lost=lost, restored=state))
            lost = None
    require(not terminal or lost is None or any(s["phase"] == "cut" and s["generation"] > lost["generation"] and
            s["at"] >= lost["at"] for s in states), "terminal has unmatched carrier loss")
    pairs = {}
    for state in states:
        if state["phase"] not in ("cut", "activated"):
            continue
        key = state["generation"]
        pair = pairs.setdefault(key, {})
        require(state["phase"] not in pair, "duplicate runtime generation phase")
        pair[state["phase"]] = state
    generations = sorted(pairs)
    for i, generation in enumerate(generations):
        require(not i or generation == generations[i-1]+1, "stale runtime generation")
        pair = pairs[generation]
        if "activated" in pair:
            require("cut" in pair and pair["cut"]["at"] <= pair["activated"]["at"], "activation lacks current generation cut")
            cut, active = pair["cut"], pair["activated"]
            require(all(active[k] >= cut[k] for k in ("input_applied", "input_accepted", "output_base", "output_end", "control_seq")),
                    "activation reports initial bundle values")
            require(active["ka_seq"] >= cut["ka_seq"] and active["control_seq"] >= active["ka_seq"] and
                    (active["ka_deadline_boot_ns"] == cut["ka_deadline_boot_ns"] if active["ka_seq"] == cut["ka_seq"] else
                     active["ka_deadline_boot_ns"] >= cut["ka_deadline_boot_ns"]), "activation renewed absolute KA deadline")
            require(all(not cut[k] or active[k] for k in ("input_fin", "input_ended", "output_fin", "revoked", "master_closed")),
                    "activation lost current terminal state")
    if terminal and reliable:
        # origin: hot-update S4 plan §3 / §9.1 / T48. Exit snapshots precede
        # the final ACK; only settlement can claim acknowledged Base == End.
        require(len(settlements) == 1, "missing or duplicated terminal-ack settlement")
        settled, exited = settlements[0], exits[0]
        require(settled["at"] >= exited["at"] and all(settled[k] == exited[k] for k in
                ("code", "signal", "input_applied", "input_accepted", "output_end")) and
                exited["output_base"] <= settled["output_base"], "settlement differs from original terminal")
        require(settled["input_applied"] <= settled["input_accepted"] and settled["output_base"] == settled["output_end"],
                "settlement Base differs from End")
    return dict(version=1, nonce=nonce, protocol="reliable-pty-v1" if reliable else "legacy",
                ctl_identity=ctl_identity["identity"], pid=token, started=line, start=start, states=states,
                controls=controls, exits=exits, settlements=settlements, pairs=pairs, carrier_pairs=carrier_pairs,
                rows=rows, spawn_count=len(started or legacy))


def snapshot_generation(root, snapshot, legacy_bare_exec=False):
    # origin: hot-update S5 plan §7.1 / T48 / DRILL-RUN 36.A/W/C.
    # A fresh Linux flow owner starts at one before any upgrade marker exists.
    # Use the independently captured running artifact, never infer the original
    # generation by subtracting from the candidate cut we are trying to judge.
    marker = snapshot.get("marker")
    if marker is not None:
        generation = marker.get("generation")
        # origin: hot-update S5 plan §9.3 / T48 / DRILL-RUN 39.P.
        # v0.6 bare exec has no image-generation field. Preserve that native
        # absence only for the bound legacy fault, never for a reliable cut.
        if generation is None and legacy_bare_exec:
            root = Path(root)
            before = read_json(root/"agent/before.json")
            require(before.get("marker") is None and snapshot_generation(root, before) == 0 and
                    (root/"endpoint-agent.version").read_text().startswith("tether v0.6.0 ") and
                    marker.get("state") == "committed" and marker.get("prev_sha") == before["sha256"] and
                    marker.get("new_sha") == snapshot["sha256"] and
                    snapshot["identity"]["pid"] == before["identity"]["pid"] and
                    marker.get("target_sid") == "lab" and marker.get("target_nid") == "agt1" and
                    all(key not in marker for key in ("generation", "imported_resources", "resource_phase")),
                    "generation absent outside bound legacy bare exec")
            return 0
        require(integer(generation), "missing retained image generation")
        return generation
    root = Path(root)
    bound = re.fullmatch(r"([0-9a-f]{64})[ \t]+/proc/([0-9]+)/exe\n?", (root/"endpoint-agent.sha").read_text())
    require(bound is not None and bound[1] == snapshot["sha256"] and int(bound[2]) == snapshot["identity"]["pid"],
            "initial generation belongs to another running artifact")
    version = (root/"endpoint-agent.version").read_text()
    domains = [line.split()[1:] for line in version.splitlines() if line.startswith("image-domains ")]
    require(len(domains) <= 1, "ambiguous initial image domains")
    return 1 if domains and "flow-v1" in domains[0] else 0


def current_receipts(root, nonce, name):
    root = Path(root)
    # A failed observation cannot leave a prior generation's success available.
    for target in ("queued", "active", "rollback"):
        with contextlib.suppress(FileNotFoundError):
            (root / (target+".json")).unlink()
    receipt = runtime_records(root, nonce, terminal=False)
    generations = sorted(receipt["pairs"])
    require(generations, "missing runtime cut record")
    first = receipt["pairs"][generations[0]]
    require("cut" in first and (name == "queued" or "activated" in first), "missing original candidate activation record")
    before = read_json(root / "agent/before.json")
    baseline_generation = snapshot_generation(root, before)
    require(generations[0] == baseline_generation+1, "stale current-state generation")
    selected = [("queued", first["cut"])]
    if len(generations) > 1:
        second = receipt["pairs"][generations[1]]
        selected.append(("active", second["cut"]))
        if "activated" in second:
            selected.append(("rollback", second["activated"]))
    for target, state in selected:
        row = dict(version=1, nonce=nonce, protocol=receipt["protocol"], ctl_identity=receipt["ctl_identity"],
                   generation=state["generation"], phase=state["phase"], source=state["source"],
                   input=dict(applied=state["input_applied"], accepted=state["input_accepted"]),
                   output=dict(base=state["output_base"], end=state["output_end"]),
                   controls=[dict(op="keepalive", seq=state["ka_seq"], ack=state["ka_seq"],
                                  deadline=state["ka_deadline_boot_ns"], ack_source="agent_image_committed_ka")])
        write_json(root / (target+".json"), row)
    require(name == "queued" or (root / (name+".json")).exists(), "current candidate cut not yet observed")
    return receipt


def assemble_observations(root, nonce, runtime):
    snapshots = []
    for path in (root / "agent").glob("*.json"):
        if path.name.endswith(".ps.json"):
            continue
        row = read_json(path)
        require(row.get("nonce") == nonce, "foreign agent snapshot")
        snapshots.append((path.stem, row))
    snapshots.sort(key=lambda item: item[1]["at"])
    by_name = dict(snapshots)
    require("before" in by_name and "settled" in by_name, "missing baseline/settled ownership receipts")
    management = read_json(root / "agent/settled.ps.json")
    # origin: hot-update S4 plan §9.2 / T48; drill-run3 C2. The started
    # OS identity already binds runtime's token; an earlier C1 task can retain
    # the same fixture nonce in ps -a. Require this token AND its nonce.
    tasks = [r for r in management["processes"] if r.get("pid") == runtime["pid"] and nonce in r.get("argv", [])]
    negative_legacy = runtime["protocol"] == "legacy" and read_json(root / "ctl.trace.json").get("scenario") == "P"
    require((negative_legacy and not tasks) or len(tasks) == 1 and tasks[0]["pid"] == runtime["pid"],
            "management token differs from original-child audit")
    if not negative_legacy:
        require(tasks[0]["status"] == "EXITED" and tasks[0].get("exit_code") == runtime["exits"][0]["code"],
                "management terminal differs from runtime audit")
    windows = read_json(root / "windows.json")["windows"]
    kind = windows[0].get("kind", "image") if windows else "image"
    fault_prefix = ("agent: upgrade COMMITTED" if negative_legacy else "rebuilding session" if kind == "carrier" else "agent: upgrade deferred" if kind == "refusal" else
                    "agent: run" if kind == "compatibility" else "agent: image activated")
    activations = [line for line,r in runtime["rows"] if fault_prefix in r.get("msg", "")]
    require(activations, "missing real fault audit")
    images = []
    for name, snapshot in snapshots:
        if name == "settled":
            continue
        marker = snapshot["marker"]
        image = dict(identity=snapshot["identity"], sha256=snapshot["sha256"],
                     generation=snapshot_generation(root, snapshot, legacy_bare_exec=negative_legacy),
                     phase=marker["state"] if marker else "none", spawn_count=runtime["spawn_count"], terminal_count=0, exit_code=0)
        if image["phase"] == "pending" and marker.get("resource_phase") == "active":
            image["phase"] = "active"
        if not images or image["sha256"] != images[-1]["sha256"]:
            images.append(image)
    require(images, "missing original running image")
    if runtime["protocol"] != "legacy":
        for image in images[1:]:
            require(image["generation"] in runtime["pairs"] and "activated" in runtime["pairs"][image["generation"]],
                    "image marker lacks matching runtime generation")
    images[-1].update(terminal_count=len(runtime["exits"]), exit_code=runtime["exits"][0]["code"] if runtime["exits"] else None)
    origin = read_json(root / "ctl.identity.json")["started_monotonic"]
    for name, snapshot in snapshots:
        snapshot["resources"]["at"] = snapshot["at"]-origin
    after = [s["resources"] for name,s in snapshots if name not in ("before", "settled")]
    require(after, "missing post-fault ownership snapshot")
    observations = dict(nonce=nonce, images=images, resources=dict(before=by_name["before"]["resources"],
                         after=after, settled=by_name["settled"]["resources"]))
    write_json(root / "observations.json", observations)
    write_json(root / "audit.json", dict(nonce=nonce, task_pid=runtime["pid"],
               required=dict(started=[runtime["started"], nonce] if runtime["protocol"] == "legacy" else [runtime["started"]], activation=[activations[-1]], exit=[runtime["exits"][0]["source"]] if runtime["exits"] else [])))
    if negative_legacy:
        observations["legacy_loss"] = dict(nonce=nonce, management_missing=not tasks,
            terminal_missing=not runtime["exits"], task_pid=runtime["pid"])
    return observations


def assemble_evidence(root, nonce):
    """Join P18 slog records and independent collectors; missing telemetry is rc2.

    Application byte counts cannot prove A/Q, Base/End or KA deadlines.
    No caller may reconstruct the ctl receiver from remote.sent.
    """
    # origin: hot-update S4 plan §12.1 / T48. A complete schema is a claim;
    # require each sidecar before publishing it and invalidate stale results.
    root = Path(root)
    with contextlib.suppress(FileNotFoundError):
        (root / "evidence.json").unlink()
    ctl_trace = read_json(root / "ctl.trace.json")
    remote_trace = read_json(root / "remote.trace.json")
    require(ctl_trace.get("nonce") == remote_trace.get("nonce") == nonce, "foreign trace receipts")
    origin = ctl_trace["started_monotonic"]
    shift = remote_trace["started_monotonic"] - origin
    require(stamp(origin) and math.isfinite(shift), "missing trace clock origins")
    def timed(value, delta):
        result = dict(value)
        result["at"] += delta
        return result
    def blob(value, expected, delta):
        require(value.get("file") == expected, "collector source differs from expected byte file")
        result = dict(value, records=[timed(row, delta) for row in value["records"]])
        blob_bytes(root, result)
        return result
    inp = dict(sent=blob(ctl_trace["input"], "ctl.sent", 0),
               received=blob(remote_trace["input"], "remote.received", shift))
    out = dict(sent=blob(remote_trace["output"], "remote.sent", shift),
               received=blob(ctl_trace["output"], "ctl.received", 0))
    processes = [timed(row, 0) for row in ctl_trace["samples"]]
    processes += [timed(row, shift) for row in remote_trace["samples"]]
    wait = read_json(root / "ctl.wait.json")
    ident = read_json(root / "ctl.identity.json")["identity"]
    require((wait.get("pid"), wait.get("start_ticks")) == (ident["pid"], ident["start_ticks"]),
            "waitpid belongs to a different ctl")
    windows = read_json(root / "windows.json")
    require(windows.get("nonce") == nonce and isinstance(windows.get("windows"), list), "foreign fault window")
    runtime = runtime_records(root, nonce)
    legacy = runtime["protocol"] == "legacy"
    terminal = runtime["exits"][0] if runtime["exits"] else None
    final = terminal if legacy else runtime["settlements"][0]
    if not legacy and any(w.get("kind") == "carrier" for w in windows["windows"]):
        # C1 keeps one established session transition on the original PTY.
        # Neither an image cut nor an unrelated rebuild can fill this window.
        # origin: hot-update S4 plan §6.1 / §9.2 / C1 / T48.
        before = read_json(root / "agent/before.json")
        generation = snapshot_generation(root, before)
        for window in (w for w in windows["windows"] if w.get("kind") == "carrier"):
            matching = [p for p in runtime["carrier_pairs"] if window["start"] <= p["lost"]["at"] <= p["restored"]["at"] <= window["end"]]
            require(len(matching) == 1, "no passive carrier offset checkpoints: missing window-bound carrier pair")
            lost, restored = matching[0]["lost"], matching[0]["restored"]
            require(lost["generation"] == restored["generation"] == generation and not runtime["pairs"],
                    "carrier pair mismatched with original generation")
            require(lost["ka_seq"] > 0 and lost["ka_seq"] == restored["ka_seq"] and
                    lost["ka_deadline_boot_ns"] > 0 and lost["ka_deadline_boot_ns"] == restored["ka_deadline_boot_ns"],
                    "carrier rebuild renewed KA authority")
    for target, prefix, fields in ((inp, "input", ("applied", "accepted")), (out, "output", ("base", "end"))):
        target.update({key: 0 if legacy else final[prefix+"_"+key] for key in fields})
        target["checkpoints"] = [dict(at=state["at"], **{key:state[prefix+"_"+key] for key in fields})
                                 for state in runtime["states"]]
        if terminal is not None:
            target["checkpoints"].append(dict(at=terminal["at"], **{key:0 if legacy else terminal[prefix+"_"+key] for key in fields}))
        elif legacy:
            # Unsupported legacy offsets remain zero. This timestamp comes
            # from the original ctl observation, never an invented exit audit.
            target["checkpoints"].append(dict(at=ctl_trace["samples"][-1]["at"], **dict.fromkeys(fields, 0)))
        if not legacy:
            target["checkpoints"].append(dict(at=final["at"], **{key:target[key] for key in fields}))
        if legacy:
            # These zero slots explicitly mean unsupported legacy offsets;
            # they are never labeled observed reliable A/Q or Base/End.
            target["checkpoints"].insert(0, dict(at=0, **dict.fromkeys(fields, 0)))
        require(legacy or runtime["states"], "no passive carrier offset checkpoints: missing live runtime state")
    inp["protocol"] = runtime["protocol"]
    ctl = dict(wait, path="legacy" if legacy else "reliable", pty_version=0 if legacy else 1)
    # Local ioctl receipts stay separate. Controls' seq/ack and remote effects
    # must already have been observed by the passive runtime collector.
    local_resize = []
    for path in sorted((root / "checkpoints").glob("*.json")):
        receipt = read_json(path)
        require(receipt.get("nonce") == nonce, "foreign checkpoint")
        if receipt.get("local_resize") is not None:
            local_resize.append(receipt["local_resize"])
    local_resize.sort(key=lambda r:r["at"])
    controls = runtime["controls"]
    for resize in local_resize:
        observed = [row for row in processes if row.get("role") == "foreground" and row["at"] >= resize["at"]
                    and (row.get("cols"), row.get("rows")) == (resize["cols"], resize["rows"])]
        require(observed, "remote resize lacks measured foreground geometry")
        if legacy:
            control = dict(op="resize", seq=0, ack=0, at=resize["at"])
            controls.append(control)
        else:
            # Text slog rounds to milliseconds. Its measured clock interval
            # bounds matching; it never turns a local ioctl into a commit.
            candidates = [c for c in controls if c["op"] == "resize" and "cols" not in c and
                          resize["at"] <= c["at"]+c["clock_resolution_s"] and c["at"] <= observed[-1]["at"]]
            require(candidates, "remote resize lacks committed control receipt")
            control = candidates[0]
        control.update(cols=resize["cols"], rows=resize["rows"], observed_cols=observed[0]["cols"], observed_rows=observed[0]["rows"])
    require(all(c["op"] != "resize" or "cols" in c for c in controls), "committed resize lacks independent local ioctl")
    controls.sort(key=lambda c:c["at"])
    observations = assemble_observations(root, nonce, runtime)
    images = observations["images"]
    resources = observations["resources"]
    logs = read_json(root / "logs.json")
    audit = read_json(root / "audit.json")
    require(audit.get("nonce") == nonce and isinstance(audit.get("required"), dict), "foreign log audit")
    for cursor in logs["cursors"]:
        if cursor["stream"] == "agent_slog":
            cursor["required"] = audit["required"]
    result = dict(version=1, nonce=nonce, images=images, processes=processes, windows=windows["windows"],
                  input=inp, output=out, controls=controls, ctl=ctl, resources=resources, logs=logs,
                  oracle=dict(valid=False, continuous=False, reason="unjudged"), local_resize=local_resize)
    if "legacy_loss" in observations:
        result["legacy_loss"] = observations["legacy_loss"]
    if legacy:
        endpoints = []
        for role in ("ctl", "agent", "broker"):
            version_file = "endpoint-"+role+".version"
            version_raw = (root / version_file).read_bytes()
            executable_sha = (root / ("endpoint-"+role+".sha")).read_text().split()[0]
            require(re.fullmatch(r"[0-9a-f]{64}", executable_sha) and version_raw.startswith(b"tether "),
                    "unidentified legacy endpoint")
            domains = [line.split()[1:] for line in version_raw.splitlines() if line.startswith(b"image-domains ")]
            endpoints.append(dict(role=role, file=version_file, length=len(version_raw), sha256=sha(version_raw),
                                  executable_sha256=executable_sha, old=not any(b"pty-v1" in row for row in domains)))
        require(any(row["old"] for row in endpoints), "legacy fallback lacks actual old endpoint")
        result["fallback"] = dict(nonce=nonce, precreate_only=True, started=runtime["started"], endpoints=endpoints)
    result["scenario"] = ctl_trace.get("scenario", "A")
    if result["scenario"] == "C2":
        action_raw = (root / "canonical.sent").read_bytes()
        result["canonical_actions"] = dict(nonce=nonce, sent=dict(file="canonical.sent", length=len(action_raw), sha256=sha(action_raw)),
            remote=read_json(root / "canonical.remote.json"), local_fin=read_json(root / "fin.local.json"),
            remote_fin=read_json(root / "fin.remote.json"))
        ended = result["canonical_actions"]["remote_fin"].get("source_ended_monotonic")
        require(stamp(ended), "missing observed draining source end")
        for window in result["windows"]:
            if window.get("kind") == "draining":
                window["draining_end_at"] = ended-origin
                window["leader_exit_lower_at"] = max(r["at"] for r in processes if r["role"] == "leader" and r["alive"])
    if not legacy:
        result["audit_binding"] = dict(nonce=nonce, **{k:runtime["start"][k] for k in
                                      ("pid", "os_pid", "start_time_ticks", "request_id_hash")})
        for key in ("os_pid", "start_time_ticks"):
            result["audit_binding"][key] = int(result["audit_binding"][key])
        result["runtime_states"] = runtime["states"]
        if runtime["states"]:
            last = runtime["states"][-1]
            result["keepalive"] = dict(seq=last["ka_seq"], deadline_boot_ns=last["ka_deadline_boot_ns"], source=last["source"])
        result["terminal_receipt"] = terminal
        result["settlement_receipt"] = final
        result["carrier_pairs"] = runtime["carrier_pairs"]
    write_json(root / "evidence.json", result)
    return result


def selftest_evidence(root, nonce):
    # origin: hot-update S4 plan §9.3 / T48. These are synthetic oracle controls,
    # never deployment receipts; tests build their own independent fixture.
    def ident(pid):
        return dict(pid=pid, start_ticks=pid, boot_id="selftest", ppid=4000)

    def blob(name):
        raw = b"".join(frame(nonce, i, payload(i)) for i in range(50))
        path = root / name
        path.write_bytes(raw)
        rows = parse_frames(raw, nonce)
        for i, row in enumerate(rows):
            row["at"] = i*.1
        return dict(file=name, length=len(raw), sha256=sha(raw), records=rows)

    inp = dict(sent=blob("ctl.sent"), received=blob("remote.received"))
    out = dict(sent=blob("remote.sent"), received=blob("ctl.received"))
    inp.update(applied=inp["sent"]["length"], accepted=inp["sent"]["length"],
               checkpoints=[dict(at=1, applied=1, accepted=2), dict(at=4, applied=3, accepted=4)])
    out.update(base=out["sent"]["length"], end=out["sent"]["length"],
               checkpoints=[dict(at=1, base=1, end=2), dict(at=4, base=3, end=4)])
    samples = [dict(role=role, at=i*.5, identity=ident(pid), session_id=4100 if role != "ctl" else 4300,
                   foreground_pgid=4200 if role != "ctl" else 4300,
                   ctty=True, alive=True, hup=0, early_eof=False, nonce=nonce)
               for i in range(10) for role, pid in (("leader",4100),("foreground",4200),("ctl",4300))]
    images = [dict(identity=ident(4000), sha256=char*64, generation=i, phase="committed",
                   spawn_count=1, terminal_count=i, exit_code=23 if i else 0) for i, char in enumerate("ab")]
    before = dict(owner=ident(4000), counts=dict(master=1, replay=2, handoff=0, pidfd=1),
                  masters=[dict(dev=1,ino=2,rdev=3,mount_id=4,pty_index=5)], wrongfd=[], zombies=[], foreground_alive=True)
    settled = dict(owner=ident(4000), counts=dict.fromkeys(KINDS,0), masters=[], wrongfd=[], zombies=[], foreground_alive=False)
    cursors = []
    for i, stream in enumerate(STREAMS):
        raw = "".join(event+" "+nonce+"\\n" for event in ("started","activation","exit")).encode() if stream == "agent_slog" else b""
        (root / (stream + ".log")).write_bytes(raw)
        cursors.append(dict(stream=stream, inode=i+1, inode_before=i+1, before=0, after=len(raw), read_ok=True,
                            file=stream+".log", length=len(raw), sha256=sha(raw),
                            required={event:[event,nonce] for event in ("started","activation","exit")} if stream == "agent_slog" else []))
    return dict(version=1, nonce=nonce, images=images, processes=samples, windows=[dict(start=2.5,end=3,fault_observed=True)],
                input=inp, output=out, controls=[dict(seq=i+1,ack=i+1,op="resize",at=at,
                    cols=80+i,rows=24,observed_cols=80+i,observed_rows=24) for i,at in enumerate((2,4))],
                ctl=dict(pid=4300,start_ticks=4300,wait_status=23<<8,reaped=True),
                resources=dict(before=before,after=[before],settled=settled), logs=dict(cursors=cursors,read_ok=True),
                oracle=dict(valid=False,continuous=False,reason="unjudged"))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("identity", "trace", "census", "continuity", "legacy-exporter", "selftest"):
        command = commands.add_parser(name)
        command.add_argument("--evidence", required=True)
        command.add_argument("--nonce", required=True)
        if name in ("identity", "census"):
            command.add_argument("--pid", type=int, default=os.getpid())
            command.add_argument("--foreground", help="JSON file of original foreground identity")
            command.add_argument("--baseline", help="pre-traffic census JSON; count only original kernel objects")
        if name == "trace":
            command.add_argument("--role", choices=("ctl", "remote", "leader", "checkpoint", "agent", "assemble", "current", "compatibility"), required=True)
            command.add_argument("--ctl-binary")
            command.add_argument("--node", default="agt1")
            command.add_argument("--scenario", default="A")
            command.add_argument("--remote-probe")
            command.add_argument("--remote-evidence")
            command.add_argument("--name")
            command.add_argument("--pid", type=int)
            command.add_argument("--marker")
            command.add_argument("--baseline")
            command.add_argument("--foreground")
            command.add_argument("--cols", type=int)
            command.add_argument("--rows", type=int)
            command.add_argument("--input-bytes", type=int, default=len(payload(0)))
            command.add_argument("--period", type=float, default=.05)
            command.add_argument("--timeout", type=float, default=300)
            command.add_argument("argv", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    require(re.fullmatch(r"[0-9a-f]{32}", args.nonce), "nonce must be 128-bit lowercase hex")
    root = Path(args.evidence)
    if args.command in ("continuity", "legacy-exporter"):
        oracle = judge(root, args.nonce)
        result = read_json(root / "evidence.json")
        result["oracle"] = oracle
        print(json.dumps(result, sort_keys=True))
        if args.command == "legacy-exporter" and oracle["valid"] and not oracle["continuous"]:
            hup = max(r["hup"] for r in result["processes"] if r["role"] == "foreground")
            fg = next(r["identity"]["pid"] for r in result["processes"] if r["role"] == "foreground")
            rc = os.waitstatus_to_exitcode(result["ctl"]["wait_status"])
            output = "complete" if blob_bytes(root, result["output"]["sent"]) == blob_bytes(root, result["output"]["received"]) else "interrupted"
            print(f'HOT_UPDATE_LEGACY_PTY_EXPORTER output={output} hup={hup} '
                  f'management_terminal={result["images"][-1]["terminal_count"]} ctl={rc} pid={fg} '
                  f'nonce={args.nonce} reason={oracle["reason"]}', file=sys.stderr)
        if args.command == "continuity" and not oracle["continuous"]:
            print("HOT_UPDATE_PTY_CONTINUITY_FAILED: " + oracle["reason"], file=sys.stderr)
        return 0 if oracle["continuous"] else 1 if oracle["valid"] else 2
    if args.command == "identity":
        print(json.dumps(dict(version=1, nonce=args.nonce, identity=identity(args.pid)), sort_keys=True))
    elif args.command == "census":
        foreground = read_json(args.foreground) if args.foreground else None
        if foreground is not None:
            foreground = foreground.get("identity", foreground)
        print(json.dumps(dict(version=1, nonce=args.nonce, **census(args.pid, foreground, read_json(args.baseline) if args.baseline else None)), sort_keys=True))
    elif args.command == "trace":
        require(len(payload(0)) <= args.input_bytes <= 32768, "invalid input payload size")
        require(math.isfinite(args.period) and .001 <= args.period <= 1 and math.isfinite(args.timeout) and 1 <= args.timeout <= 600, "invalid trace budget")
        if args.role == "assemble":
            print(json.dumps(assemble_evidence(root, args.nonce), sort_keys=True))
            return 0
        if args.role == "compatibility":
            oracle = judge(root, args.nonce, compatibility=True)
            result = read_json(root / "evidence.json")
            result["oracle"] = oracle
            write_json(root / "compatibility.json", result)
            print(json.dumps(result, sort_keys=True))
            if not oracle["continuous"]:
                print("HOT_UPDATE_PTY_COMPATIBILITY_FAILED: " + oracle["reason"], file=sys.stderr)
            return 0 if oracle["continuous"] else 1 if oracle["valid"] else 2
        if args.role == "current":
            current_receipts(root, args.nonce, args.name)
            return 0
        if args.role == "agent":
            require(args.pid and args.pid > 1 and args.marker, "missing agent snapshot identity/marker")
            current = identity(args.pid)
            marker_path = Path(args.marker)
            marker = read_json(marker_path) if marker_path.exists() else None
            foreground = read_json(args.foreground)["identity"] if args.foreground else None
            snapshot = census(args.pid, foreground, read_json(args.baseline) if args.baseline else None)
            require(valid_identity(identity(args.pid)) == valid_identity(current), "agent changed during snapshot")
            print(json.dumps(dict(version=1, nonce=args.nonce, at=time.monotonic(), identity=current,
                  sha256=sha(Path(f"/proc/{args.pid}/exe").read_bytes()), marker=marker, resources=snapshot), sort_keys=True))
            return 0
        if args.role == "checkpoint":
            require(args.name and re.fullmatch(r"[A-Za-z0-9_.-]+", args.name), "invalid checkpoint name")
            ident = read_json(root / "ctl.identity.json")
            reaped = False
            try:
                current = identity(ident["identity"]["pid"])
                require(valid_identity(current) == valid_identity(ident["identity"]), "original ctl changed")
            except (FileNotFoundError, ProcessLookupError):
                require(args.name == "post-fault" and (root / "ctl.wait.json").exists(), "original ctl unavailable")
                current = ident["identity"]
                reaped = True
            if args.cols is not None or args.rows is not None:
                require(all(type(v) is int and 0 < v <= 65535 for v in (args.cols, args.rows)), "invalid checkpoint resize")
                write_json(root / "resize.request.json", dict(nonce=args.nonce, name=args.name, cols=args.cols, rows=args.rows))
                deadline = time.monotonic()+5
                while True:
                    receipt_path = root / "resize.receipt.json"
                    if receipt_path.exists():
                        receipt = read_json(receipt_path)
                        if receipt.get("name") == args.name and receipt.get("nonce") == args.nonce:
                            require((receipt["cols"], receipt["rows"]) == (args.cols, args.rows), "local resize ioctl did not take effect")
                            break
                    require(time.monotonic() < deadline, "local resize receipt timed out")
                    time.sleep(.01)
            write_json(root / (args.name + ".json"), dict(nonce=args.nonce, identity=current,
                       at=time.monotonic()-ident["started_monotonic"],
                       sent=(root / "ctl.sent").stat().st_size, received=(root / "ctl.received").stat().st_size,
                       ctl_reaped=reaped, local_resize=receipt if args.cols is not None else None))
        else:
            return trace_ctl(args) if args.role == "ctl" else trace_leader(args) if args.role == "leader" else trace_remote(args)
    elif args.command == "selftest":
        data = b"".join(frame(args.nonce, i, payload(i)) for i in range(40))
        require([r["id"] for r in parse_frames(data, args.nonce)] == list(range(40)), "raw sequence selftest failed")
        require(len(parse_frames(data[:-1], args.nonce, complete=False)) == 39, "partial raw frame accepted")
        try:
            parse_frames(data[:-1], args.nonce)
        except EvidenceError:
            pass
        else:
            raise EvidenceError("truncated frame selftest escaped")
        with tempfile.TemporaryDirectory(prefix="pty-oracle-selftest-") as directory:
            root = Path(directory)
            doc = selftest_evidence(root, args.nonce)
            write_json(root / "evidence.json", doc)
            require(judge(root, args.nonce)["continuous"], "positive oracle selftest failed")
            doc["processes"][-2]["hup"] = 1
            write_json(root / "evidence.json", doc)
            outcome = judge(root, args.nonce)
            require(outcome["valid"] and not outcome["continuous"], "legacy discontinuity selftest escaped")
            doc["processes"][-2]["hup"] = 0
            doc["windows"] = []
            write_json(root / "evidence.json", doc)
            require(not judge(root, args.nonce)["valid"], "empty window selftest escaped")
        print("hot-update PTY protocol/oracle selftest: PASS")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (EvidenceError, OSError, ValueError, KeyError, TypeError, IndexError, AttributeError, OverflowError, StopIteration) as exc:
        print("HOT_UPDATE_PTY_EVIDENCE_INVALID: " + str(exc), file=sys.stderr)
        sys.exit(2)
