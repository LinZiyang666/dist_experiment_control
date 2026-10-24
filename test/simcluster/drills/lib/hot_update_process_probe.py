#!/usr/bin/env python3
"""Independent process-continuity producer/oracle; no product recovery code.

The producer's private ledger is evidence, never a replacement output channel.
`continuity` is the positive contract. `legacy` recognizes one narrow known
failure and returns nonzero, for assert_bug/PRODUCT-RED until that call is
promoted to assert_ok. `selftest` uses synthetic evidence only, never Docker.

Hot-update S1 (drill 35): `carrier` is the positive oracle for "the original
child and its ctl survived a carrier fault" on the reliable path (arms A/B) or,
with --path legacy, for a previous-release AGENT reached through the legacy
fallback (arm C2); `legacy-carrier` judges a previous-release CTL under the
same fault (arm C1): legacy path taken, executed once, discontinuity recorded
honestly. Both read carrier.json, which the drill writes around each fault.
"""

import argparse
import contextlib
import errno
import io
import hashlib
import json
import os
from pathlib import Path
import re
import select
import shlex
import signal
import sys
import tempfile
import time


PREFIX = "HUPROBE "
SIGNATURE = "H0_LEGACY_EXEC_INTERRUPTED"
STREAMS = ("stdout", "stderr")


class EvidenceError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise EvidenceError(message)


def json_file(path):
    try:
        value = json.loads(Path(path).read_text())
        require(isinstance(value, dict), f"non-object evidence {path}")
        return value
    except (OSError, ValueError) as exc:
        raise EvidenceError(f"invalid evidence {path}: {exc}") from exc


def write_json(path, value):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as out:
        json.dump(value, out, sort_keys=True)
        out.write("\n")
        out.flush()
        os.fsync(out.fileno())
    os.replace(tmp, path)


def process_identity(pid):
    stat = Path(f"/proc/{pid}/stat").read_text()
    fields = stat[stat.rfind(")") + 2:].split()
    return {
        "pid": pid,
        "ppid": int(fields[1]),
        "start_ticks": int(fields[19]),
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "state": fields[0],
    }


def same_process(a, b):
    return all(a.get(k) == b.get(k) for k in ("pid", "start_ticks", "boot_id"))


def records(path, nonce, transcript=False, live=False):
    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        raise EvidenceError(f"missing evidence {path}: {exc}") from exc
    # Readers polling a live append-only file may see an unfinished final line.
    # Final evidence is read strictly; a truncated record can never pass it.
    if raw and not raw.endswith(b"\n"):
        require(live, f"truncated final record: {path}")
        raw = raw[:raw.rfind(b"\n") + 1]
    result = []
    for line in raw.decode("utf-8", errors="strict").splitlines():
        if transcript:
            if not line.startswith(PREFIX):
                continue  # CLI diagnostics are retained but are not workload data.
            line = line[len(PREFIX):]
        try:
            rec = json.loads(line)
        except ValueError as exc:
            raise EvidenceError(f"malformed probe record: {path}") from exc
        require(isinstance(rec, dict), f"non-object record: {path}")
        require(rec.get("nonce") == nonce, f"foreign nonce in {path}")
        result.append(rec)
    return result


def data_by_stream(recs):
    return {stream: [r for r in recs if r.get("kind") == "data" and r.get("stream") == stream]
            for stream in STREAMS}


def check_sequence(recs, minimum, description):
    require(type(minimum) is int and minimum > 0, "minimum must be a positive integer")
    require(len(recs) >= minimum, f"empty/short {description}: {len(recs)} < {minimum}")
    require([r.get("seq") for r in recs] == list(range(len(recs))),
            f"gap/duplicate/reorder in {description}")


def task_row(path, nonce):
    doc = json_file(path)
    require(doc.get("schema") == "ps" and doc.get("schema_version") == 1,
            f"unexpected ps schema: {path}")
    require(doc.get("procs_truncated", False) is False, f"truncated ps evidence: {path}")
    rows = [r for r in doc.get("processes", []) if nonce in r.get("argv", [])]
    require(len(rows) == 1, f"expected one nonce task in {path}, got {len(rows)}")
    row = rows[0]
    require(row.get("nid") == "agt1" and bool(row.get("pid")), f"wrong task identity: {path}")
    return row


def check_baseline(root, nonce, minimum):
    before = Path(root) / "before"
    ident = json_file(before / "identity.json")
    require(ident.get("nonce") == nonce, "baseline identity nonce mismatch")
    require(isinstance(ident.get("pid"), int) and ident["pid"] > 1, "baseline PID missing")
    require(isinstance(ident.get("start_ticks"), int) and ident["start_ticks"] > 0,
            "baseline start ticks missing")
    require(bool(ident.get("boot_id")), "baseline boot ID missing")
    require(type(ident.get("ppid")) is int and ident["ppid"] > 1, "baseline parent PID missing")
    baseline_records = records(before / "ledger.jsonl", nonce, live=True)
    require(all(r.get("kind") == "data" and r.get("stream") in STREAMS
                for r in baseline_records), "baseline source already contains an error")
    source = data_by_stream(baseline_records)
    for stream in STREAMS:
        got = records(before / stream, nonce, transcript=True, live=True)
        require(all(r.get("kind") == "data" and r.get("stream") == stream for r in got),
                f"wrong record type in baseline {stream}")
        check_sequence(source[stream], minimum, f"baseline source {stream}")
        check_sequence(got, minimum, f"baseline ctl {stream}")
        # Snapshots of source and destination are independent; compare their
        # common proven prefix, never fabricate an atomic cross-host snapshot.
        common = min(len(got), len(source[stream]))
        require(got[:common] == source[stream][:common], f"baseline payload mismatch: {stream}")
    row = task_row(before / "ps.json", nonce)
    require(row.get("status") == "RUNNING", "baseline task was not RUNNING")
    return ident, row


def check_upgrade(root):
    return check_upgrade_doc(json_file(Path(root) / "upgrade.json"))


def check_upgrade_doc(up):
    for key in ("old_sha", "new_sha", "actual_sha", "artifact_sha"):
        require(re.fullmatch(r"[0-9a-f]{64}", str(up.get(key, ""))) is not None,
                f"missing/invalid upgrade {key}")
    require(up["old_sha"] != up["new_sha"], "running image was not changed")
    require(up["actual_sha"] == up["new_sha"], "new image never executed")
    require(up.get("pid_before", 0) > 1 and up["pid_before"] == up.get("pid_after"),
            "agent restarted instead of in-place exec")
    require(bool(up.get("exec_ts_before")) and up["exec_ts_before"] == up.get("exec_ts_after"),
            "agent service restarted")
    require(up.get("upgrade_rc") == 0, "upgrade command did not succeed")
    require(bool(up.get("target_release")) and up["target_release"] == up.get("online_release"),
            "new agent did not register at target release")
    marker = up.get("marker", {})
    require(marker.get("state") == "committed" and marker.get("boot_count", 0) >= 1
            and marker.get("new_sha") == up["new_sha"] and marker.get("target_nid") == "agt1"
            and marker.get("target_sid") == "lab", "upgrade commit proof missing")
    return up


def check_post(root, nonce, minimum, ident, before_row):
    # Two post-event samples of the ORIGINAL child: alive under the same PID
    # triple and parent, time ordered, and the source advanced by >= minimum
    # between them. Shared by the H0 upgrade oracle and the S1 carrier oracle.
    root = Path(root)
    post = json_file(root / "post" / "snapshot.json")
    require(post.get("nonce") == nonce and post.get("alive") is True, "original child was not alive after upgrade")
    require(same_process(ident, post.get("current", {})), "original process identity changed")
    require(post.get("finished") is False, "workload ended before post-upgrade observation")
    start = json_file(root / "post" / "snapshot-start.json")
    require(start.get("nonce") == nonce and start.get("alive") is True
            and start.get("finished") is False and same_process(ident, start.get("current", {})),
            "post-upgrade first sample did not identify the live original child")
    require(start.get("current", {}).get("ppid") == ident.get("ppid")
            and post.get("current", {}).get("ppid") == ident.get("ppid"), "original parent changed")
    require(type(start.get("observed_at_ns")) is int and start["observed_at_ns"] > 0
            and post.get("observed_at_ns", 0) > start["observed_at_ns"], "post samples not time ordered")
    require(post.get("data_count", 0) >= start.get("data_count", 0) + minimum,
            "no source progress between two post-upgrade samples")
    after_row = task_row(root / "post" / "ps.json", nonce)
    require(after_row["pid"] == before_row["pid"], "managed task ID changed")
    return post, start, after_row


def load_final(root, nonce, minimum):
    root = Path(root)
    ident, before_row = check_baseline(root, nonce, minimum)
    up = check_upgrade(root)
    require(ident["ppid"] == up["pid_before"], "original producer was not the upgraded agent's child")
    post, start, after_row = check_post(root, nonce, minimum, ident, before_row)
    source, observed, source_records, outcome = check_final_output(root, nonce, minimum, ident, post, start)
    return ident, after_row, source, observed, source_records, outcome


def check_final_output(root, nonce, minimum, ident, post, start, prefix_only=True):
    # The terminal record, the complete source ledger and what the ctl
    # transcripts actually contain (a source-prefix at most), plus the ctl's
    # own exit outcome. Judges decide what counts as continuity on top.
    # prefix_only=False (the N-1 legacy judge) also admits a transcript with
    # an INTERNAL break — still only source records, still in source order,
    # never a byte the source did not write — because a legacy exec that
    # resumes publishing on a rebuilt connection produces exactly that shape.
    root = Path(root)
    final = json_file(root / "final" / "final.json")
    require(final.get("nonce") == nonce and final.get("reason") == "stop"
            and final.get("exit_code") == 23, "workload did not finish by its requested normal stop")
    require(same_process(ident, final.get("identity", {})), "terminal record from another process")
    source_records = records(root / "final" / "ledger.jsonl", nonce)
    source = data_by_stream(source_records)
    count = final.get("count", 0)
    require(type(count) is int and count >= minimum * 2, "final source is empty/short")
    require(count >= post.get("data_count", 0), "post sample exceeds final source")
    require(all(r.get("stream") in STREAMS and r.get("kind") in ("data", "pipe_error")
                for r in source_records), "unknown source record")
    observed = {}
    for stream in STREAMS:
        require(len(source[stream]) == count, f"final source count mismatch: {stream}")
        check_sequence(source[stream], minimum * 2, f"final source {stream}")
        baseline = data_by_stream(records(root / "before" / "ledger.jsonl", nonce, live=True))[stream]
        require(baseline == source[stream][:len(baseline)], f"source ledger replaced after baseline: {stream}")
        require(start.get("data_count", 0) >= len(baseline), "post sample predates baseline")
        observed[stream] = records(root / "final" / stream, nonce, transcript=True)
        require(all(r.get("kind") == "data" and r.get("stream") == stream for r in observed[stream]),
                f"wrong output record type: {stream}")
        if prefix_only:
            check_sequence(observed[stream], minimum, f"final ctl {stream}")
            require(observed[stream] == source[stream][:len(observed[stream])],
                    f"output has non-source bytes: {stream}")
        else:
            seqs = [r.get("seq") for r in observed[stream]]
            require(len(seqs) >= minimum, f"empty/short final ctl {stream}: {len(seqs)} < {minimum}")
            require(all(type(s) is int and 0 <= s < len(source[stream]) for s in seqs)
                    and seqs == sorted(set(seqs)), f"duplicate/reordered/foreign output: {stream}")
            require(all(r == source[stream][r["seq"]] for r in observed[stream]),
                    f"output has non-source bytes: {stream}")
    outcome = json_file(root / "final" / "ctl-outcome.json")
    require(outcome.get("finished") is False or (outcome.get("finished") is True
            and type(outcome.get("exit_code")) is int), "invalid ctl outcome")
    return source, observed, source_records, outcome


def continuity(root, nonce, minimum):
    _, row, source, observed, source_records, outcome = load_final(root, nonce, minimum)
    require(row.get("status") == "RUNNING", "original live task lost management")
    require(not [r for r in source_records if r.get("kind") == "pipe_error"], "original pipe closed")
    for stream in STREAMS:
        require(observed[stream] == source[stream], f"original {stream} lost output")
    require(outcome == {"finished": True, "exit_code": 23}, "original ctl missed the exact exit code")


def legacy(root, nonce, minimum):
    # Preserve the normal positive oracle; fixing the product returns zero and
    # assert_bug must then say APPEARS FIXED, never silently become GREEN.
    try:
        continuity(root, nonce, minimum)
        return False
    except EvidenceError:
        pass
    ident, row, source, observed, source_records, outcome = load_final(root, nonce, minimum)
    errors = [r for r in source_records if r.get("kind") == "pipe_error"]
    require(len(errors) == 2 and all(r.get("errno") == errno.EPIPE for r in errors),
            "not the known failure: unexpected pipe errors")
    require({r.get("stream") for r in errors if r.get("errno") == errno.EPIPE} == set(STREAMS),
            "not the known failure: both original pipes must report EPIPE")
    require(row.get("status") == "EXITED" and row.get("exit_code") == -1,
            "not the known failure: original live task was not reconciled EXITED(-1)")
    for stream in STREAMS:
        require(len(observed[stream]) < len(source[stream]), f"known failure missing {stream} loss")
        error = next(r for r in errors if r.get("stream") == stream)
        baseline = data_by_stream(records(Path(root) / "before" / "ledger.jsonl", nonce, live=True))[stream]
        require(type(error.get("seq")) is int and len(baseline) <= error["seq"] < len(source[stream]),
                "known failure has invalid pipe error position")
        require(len(observed[stream]) <= error["seq"], "ctl output continued past claimed EPIPE")
    require(outcome != {"finished": True, "exit_code": 23}, "known failure unexpectedly delivered real exit")
    # S1 re-pin (hot-update-reliable-exec-plan §5.3): the ctl half changed. The
    # ctl now takes the reliable path, the exec'd agent is a new runtime
    # incarnation, its registry refuses the old lease (process_instance_lost)
    # and the ctl exits 74 within seconds instead of idling to its --timeout.
    # The pre-S1 shape ({"finished": false}) is no longer the known failure.
    require(outcome == {"finished": True, "exit_code": 74},
            "not the S1 shape: ctl must exit 74 (owner instance lost) promptly")
    print(f"{SIGNATURE} output=epipe management=exited_minus_one ctl=owner_lost_rc74 "
          f"pid={ident['pid']} nonce={nonce}", file=sys.stderr)
    return True


CARRIER_PATHS = ("reliable", "legacy")
FAULT_KINDS = ("nats-restart", "nats-outage", "broker-restart")


def check_carrier(root, nonce, minimum, path):
    # carrier.json is written by drill 35 around each injected fault; every
    # field a judge reads is validated here so a drill that forgot to record
    # something cannot pass on a missing key.
    root = Path(root)
    require(path in CARRIER_PATHS, "unknown exec path")
    doc = json_file(root / "carrier.json")
    require(doc.get("nonce") == nonce, "carrier record nonce mismatch")
    require(doc.get("path") == path, f"carrier record was not declared for the {path} path")
    agent = doc.get("agent", {})
    require(type(agent.get("mainpid_before")) is int and agent["mainpid_before"] > 1
            and agent.get("mainpid_before") == agent.get("mainpid_after"), "agent daemon was restarted")
    require(bool(agent.get("exec_ts_before")) and agent["exec_ts_before"] == agent.get("exec_ts_after"),
            "agent service start timestamp changed")
    faults = doc.get("faults")
    require(isinstance(faults, list) and all(isinstance(f, dict) for f in faults), "invalid fault list")
    for fault in faults:
        require(fault.get("kind") in FAULT_KINDS, "unknown fault kind")
        require(type(fault.get("t_start_ns")) is int and type(fault.get("t_end_ns")) is int
                and 0 < fault["t_start_ns"] < fault["t_end_ns"], "fault window not time ordered")
        require(type(fault.get("seq_before")) is int and type(fault.get("seq_after")) is int
                and fault["seq_after"] >= fault["seq_before"] + minimum,
                f"source did not advance through the {fault.get('kind')} window")
        # The CTL side of the same instant: how many records each transcript
        # held when the fault was injected. It is the attribution floor for
        # any loss (S1 review D1 / deploy-V1): bytes the ctl had already
        # received cannot have been lost to the fault.
        ctl = fault.get("ctl_seq_before")
        require(isinstance(ctl, dict) and all(type(ctl.get(s)) is int and ctl[s] >= 0 for s in STREAMS),
                f"ctl-side transcript counts missing at the {fault.get('kind')} injection")
    lines = {k: doc.get(k) for k in ("exec_start_lines", "reliable_lines", "probe_reached_lines")}
    require(all(type(v) is int and v >= 0 for v in lines.values()), "agent log counts missing")
    require(lines["exec_start_lines"] == 1, "the workload was not started exactly once")
    if path == "reliable":
        require(lines["reliable_lines"] == 1, "the exec did not take the reliable path exactly once")
        require(len(faults) >= 1, "a carrier claim needs at least one recorded fault window")
    else:
        require(lines["reliable_lines"] == 0, "a legacy-path claim saw a reliable exec line")
    return doc, faults


def check_terminal_row(root, nonce):
    row = task_row(Path(root) / "final" / "ps.json", nonce)
    require(row.get("status") == "EXITED" and row.get("exit_code") == 23,
            "final task row is not EXITED(23)")
    return row


def carrier(root, nonce, minimum, path="reliable"):
    # S1 positive oracle (drill 35 arms A/B, and arm C2 with path=legacy): the
    # ORIGINAL child kept running under the same identity through every
    # recorded fault window, the ctl received every source byte on both
    # streams and the exact exit code, the agent daemon never restarted and
    # the command was executed exactly once.
    root = Path(root)
    ident, before_row = check_baseline(root, nonce, minimum)
    doc, faults = check_carrier(root, nonce, minimum, path)
    if path == "legacy":
        require(doc["probe_reached_lines"] >= 1,
                "the reliable probe never reached the previous-release agent")
    post, start, after_row = check_post(root, nonce, minimum, ident, before_row)
    source, observed, source_records, outcome = check_final_output(root, nonce, minimum, ident, post, start)
    require(all(f["seq_after"] <= len(source["stdout"]) for f in faults), "fault window beyond final source")
    require(after_row.get("status") == "RUNNING", "original live task lost management")
    require(not [r for r in source_records if r.get("kind") == "pipe_error"], "original pipe closed")
    for stream in STREAMS:
        require(observed[stream] == source[stream], f"original {stream} lost output")
    require(outcome == {"finished": True, "exit_code": 23}, "original ctl missed the exact exit code")
    check_terminal_row(root, nonce)


def legacy_carrier(root, nonce, minimum):
    # Drill 35 arm C1: a previous-release ctl against today's agent under the
    # same fault. The claim is NOT continuity — requirements §6.7 promises the
    # N-1 window interoperability, not carrier survival — but that the legacy
    # path was taken, nothing was executed twice, and the discontinuity was
    # recorded honestly rather than hidden. A transcript that turned out
    # complete AND delivered the exact exit is a claim this judge refuses,
    # because then "N-1 is not continuous" would have been asserted on nothing.
    root = Path(root)
    ident, before_row = check_baseline(root, nonce, minimum)
    doc, faults = check_carrier(root, nonce, minimum, "legacy")
    require(len(faults) >= 1, "a legacy discontinuity claim needs a recorded fault window")
    post, start, after_row = check_post(root, nonce, minimum, ident, before_row)
    source, observed, source_records, outcome = check_final_output(
        root, nonce, minimum, ident, post, start, prefix_only=False)
    require(not [r for r in source_records if r.get("kind") == "pipe_error"],
            "legacy child lost its pipes: not the N-1 shape (the agent is the new one)")
    require(all(f["seq_after"] <= len(source["stdout"]) for f in faults), "fault window beyond final source")
    require(after_row.get("status") == "RUNNING", "today's agent lost management of the legacy child")
    lost = {stream: len(source[stream]) - len(observed[stream]) for stream in STREAMS}
    delivered = outcome == {"finished": True, "exit_code": 23}
    require(any(lost.values()) or not delivered,
            "previous-release ctl was fully continuous: the N-1 discontinuity claim is unfounded")
    # Attribution (S1 review D1): the loss must begin at or after the first
    # injection. A transcript that stopped BEFORE the ctl-side count sampled
    # at the first fault is an old ctl that died on its own (a legacy-path
    # interop regression — exactly what C1 exists to expose), not an honest
    # N-1 discontinuity. first_lost_seq is the first source seq the ctl never
    # received (or len(source) when only the exit was undelivered).
    first_lost = {}
    for stream in STREAMS:
        got = {r["seq"] for r in observed[stream]}
        missing = [r["seq"] for r in source[stream] if r["seq"] not in got]
        first_lost[stream] = missing[0] if missing else len(source[stream])
        require(first_lost[stream] >= faults[0]["ctl_seq_before"][stream],
                f"{stream} loss began at seq {first_lost[stream]}, before the first fault was injected "
                f"(ctl already held {faults[0]['ctl_seq_before'][stream]} records): not attributable to the fault")
    check_terminal_row(root, nonce)
    print(json.dumps({"nonce": nonce, "path": "legacy", "continuous": False, "lost": lost,
                      "first_lost_seq": first_lost, "ctl_outcome": outcome,
                      "source_count": len(source["stdout"]),
                      "faults": [{"kind": f.get("kind"), "seq_before": f.get("seq_before"),
                                  "seq_after": f.get("seq_after"), "ctl_seq_before": f.get("ctl_seq_before")}
                                 for f in faults]}, sort_keys=True))


def producer(args):
    root = Path(args.root)
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    ident = process_identity(os.getpid())
    ident["nonce"] = args.nonce
    write_json(root / "identity.json", ident)
    broken = set()
    started = time.monotonic()
    count = 0
    reason = "lifetime"
    with (root / "ledger.jsonl").open("x", buffering=1) as ledger:
        def record(value):
            ledger.write(json.dumps(value, sort_keys=True) + "\n")
            ledger.flush()
            os.fsync(ledger.fileno())

        while time.monotonic() - started < args.lifetime:
            if (root / "stop").exists():
                reason = "stop"
                break
            for stream, fd in (("stdout", 1), ("stderr", 2)):
                rec = {"kind": "data", "nonce": args.nonce, "stream": stream,
                       "seq": count, "at_ns": time.monotonic_ns()}
                record(rec)
                if stream not in broken:
                    payload = (PREFIX + json.dumps(rec, sort_keys=True) + "\n").encode()
                    try:
                        # Complete a short write; never claim unwritten suffixes
                        # reached the real ctl stream.
                        while payload:
                            written = os.write(fd, payload)
                            require(written > 0, "zero-length pipe write")
                            payload = payload[written:]
                    except OSError as exc:
                        if exc.errno != errno.EPIPE:
                            raise
                        broken.add(stream)
                        record({"kind": "pipe_error", "nonce": args.nonce,
                                "stream": stream, "seq": count, "errno": exc.errno})
            count += 1
            time.sleep(args.period)  # workload cadence, never a fixture readiness barrier
        result = {"nonce": args.nonce, "count": count, "reason": reason,
                  "exit_code": 23 if reason == "stop" else 71, "identity": process_identity(os.getpid())}
        write_json(root / "final.json", result)
    return result["exit_code"]


def snapshot(args):
    root = Path(args.root)
    ident = json_file(root / "identity.json")
    require(ident.get("nonce") == args.nonce, "snapshot nonce mismatch")
    current = {}
    try:
        current = process_identity(ident["pid"])
    except (OSError, ValueError):
        pass
    recs = records(root / "ledger.jsonl", args.nonce, live=True)
    data = data_by_stream(recs)
    print(json.dumps({"nonce": args.nonce, "current": current,
                      "alive": same_process(ident, current) and current.get("state") != "Z",
                      "finished": (root / "final.json").exists(),
                      "observed_at_ns": time.monotonic_ns(),
                      "data_count": min(len(data[s]) for s in STREAMS)}, sort_keys=True))


def stop(args):
    root = Path(args.root)
    ident = json_file(root / "identity.json")
    require(ident.get("nonce") == args.nonce, "stop nonce mismatch")
    (root / "stop").touch(exist_ok=True)
    if not args.terminate:
        return
    # Cleanup only, after evidence capture. The PID triple and nonce must both
    # match; a missing/reused PID is never signalled.
    require(hasattr(os, "pidfd_open"), "cleanup requires Linux Python with pidfd support")
    try:
        fd = os.pidfd_open(ident["pid"])
    except ProcessLookupError:
        return
    try:
        try:
            current = process_identity(ident["pid"])
            argv = Path(f"/proc/{ident['pid']}/cmdline").read_bytes().split(b"\0")
        except FileNotFoundError:
            return
        if current.get("state") == "Z":
            return
        require(same_process(ident, current) and args.nonce.encode() in argv and b"producer" in argv,
                "refuse to terminate unrelated producer")
        terminate_pinned(fd)
    finally:
        os.close(fd)


def terminate_pinned(fd):
    try:
        signal.pidfd_send_signal(fd, signal.SIGTERM)
        if not select.select([fd], [], [], 2)[0]:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
            require(bool(select.select([fd], [], [], 2)[0]), "owned fixture process did not terminate")
    except ProcessLookupError:
        pass


def stop_ctl(args):
    # A pidfd pins the specific process while cmdline is verified. Never kill
    # the generic tether process family or an unverified reused PID.
    require(hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"),
            "cleanup requires Linux Python with pidfd support")
    try:
        pid = int(Path(args.pidfile).read_text())
    except FileNotFoundError:
        return
    require(pid > 1, "refuse invalid ctl PID")
    try:
        fd = os.pidfd_open(pid)
    except ProcessLookupError:
        return
    try:
        try:
            argv = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        except FileNotFoundError:
            return
        if not any(argv):
            return  # The pinned process is already a zombie.
        require(args.nonce.encode() in argv and b"exec" in argv, "refuse to stop unrelated ctl")
        # Drill 35 arm C1 launches the PREVIOUS release as `tether-prev`; the
        # caller names the binary it started, the default stays `tether`.
        require(Path(argv[0].decode()).name == args.ctl_name, "pidfile is not the original tether ctl")
        terminate_pinned(fd)
    finally:
        os.close(fd)


def ready(args):
    for stream in STREAMS:
        got = records(Path(args.evidence) / f"live.{stream}", args.nonce, transcript=True, live=True)
        require(all(r.get("stream") == stream and r.get("kind") == "data" for r in got),
                f"unexpected ready {stream} record")
        check_sequence(got, args.minimum, f"ready {stream}")


def count(args):
    # The ctl-side counterpart of `snapshot`: how many records each live
    # transcript holds right now (a partial final line is not counted).
    print(json.dumps({stream: len(records(Path(args.evidence) / f"live.{stream}", args.nonce,
                                          transcript=True, live=True)) for stream in STREAMS},
                     sort_keys=True))


def post_ready(args):
    root = Path(args.evidence)
    ident, row = check_baseline(root, args.nonce, args.minimum)
    post = json_file(root / "post" / "snapshot.json")
    require(post.get("nonce") == args.nonce and post.get("alive") is True,
            "post-upgrade original workload not alive")
    require(same_process(ident, post.get("current", {})), "post-upgrade PID identity changed")
    require(post.get("finished") is False, "post-upgrade workload already finished")
    start = json_file(root / "post" / "snapshot-start.json")
    require(start.get("nonce") == args.nonce and start.get("alive") is True
            and start.get("finished") is False and same_process(ident, start.get("current", {})),
            "first post-upgrade sample lacks the live original process")
    require(post.get("observed_at_ns", 0) > start.get("observed_at_ns", 0) > 0,
            "post-upgrade samples not time ordered")
    require(post.get("data_count", 0) >= start.get("data_count", 0) + args.minimum,
            "source has not progressed between post-upgrade samples")
    post_row = task_row(root / "post" / "ps.json", args.nonce)
    require(post_row["pid"] == row["pid"], "post-upgrade managed task ID changed")


def write_records(path, recs, transcript=False):
    prefix = PREFIX if transcript else ""
    Path(path).write_text("".join(prefix + json.dumps(r, sort_keys=True) + "\n" for r in recs))


def synthetic_evidence(root, nonce):
    for name in ("before", "post", "final"):
        (root / name).mkdir()
    ident = {"nonce": nonce, "pid": 4312, "ppid": 4300, "start_ticks": 123456,
             "boot_id": "fixture-boot", "state": "S"}
    recs = [{"kind": "data", "nonce": nonce, "stream": stream, "seq": seq, "at_ns": seq + 1}
            for seq in range(30) for stream in STREAMS]
    before = [r for r in recs if r["seq"] < 10]
    row = {"pid": "task-fixture", "nid": "agt1", "argv": ["probe", "--nonce", nonce], "status": "RUNNING"}
    # Production omits false procs_truncated; exercise that actual shape.
    ps = {"schema": "ps", "schema_version": 1, "processes": [row]}
    write_json(root / "before" / "identity.json", ident)
    write_records(root / "before" / "ledger.jsonl", before)
    write_records(root / "final" / "ledger.jsonl", recs)
    for stream in STREAMS:
        write_records(root / "before" / stream, [r for r in before if r["stream"] == stream], True)
        write_records(root / "final" / stream, [r for r in recs if r["stream"] == stream], True)
    write_json(root / "before" / "ps.json", ps)
    write_json(root / "post" / "ps.json", ps)
    write_json(root / "post" / "snapshot.json", {"nonce": nonce, "alive": True,
               "current": ident, "finished": False, "data_count": 25, "observed_at_ns": 200})
    write_json(root / "post" / "snapshot-start.json", {"nonce": nonce, "alive": True,
               "current": ident, "finished": False, "data_count": 15, "observed_at_ns": 100})
    write_json(root / "final" / "final.json", {"nonce": nonce, "reason": "stop", "exit_code": 23,
               "count": 30, "identity": ident})
    write_json(root / "final" / "ctl-outcome.json", {"finished": True, "exit_code": 23})
    write_json(root / "upgrade.json", {"old_sha": "a" * 64, "new_sha": "b" * 64,
               "actual_sha": "b" * 64, "artifact_sha": "c" * 64,
               "pid_before": 4300, "pid_after": 4300, "exec_ts_before": "fixture-start",
               "exec_ts_after": "fixture-start", "upgrade_rc": 0,
               "online_release": "v-fixture-next", "target_release": "v-fixture-next",
               "marker": {"state": "committed", "boot_count": 1, "new_sha": "b" * 64,
                          "target_sid": "lab", "target_nid": "agt1"}})


def mutate_legacy(root, nonce):
    recs = records(root / "final" / "ledger.jsonl", nonce)
    for stream in STREAMS:
        write_records(root / "final" / stream,
                      [r for r in recs if r["stream"] == stream and r["seq"] < 12], True)
        recs.append({"kind": "pipe_error", "nonce": nonce, "stream": stream, "seq": 12, "errno": errno.EPIPE})
    write_records(root / "final" / "ledger.jsonl", recs)
    ps = json_file(root / "post" / "ps.json")
    ps["processes"][0].update(status="EXITED", exit_code=-1)
    write_json(root / "post" / "ps.json", ps)
    write_json(root / "final" / "ctl-outcome.json", {"finished": True, "exit_code": 74})


def synthetic_carrier(root, nonce, path="reliable"):
    # Drill 35 evidence on top of the H0 fixture: the fault record and the
    # final task row. upgrade.json is irrelevant to the carrier judges.
    write_json(root / "carrier.json", {
        "nonce": nonce, "arm": "A", "path": path,
        "faults": [{"kind": "nats-restart", "t_start_ns": 100, "t_end_ns": 200,
                    "seq_before": 10, "seq_after": 16, "ctl_seq_before": {"stdout": 10, "stderr": 10}},
                   {"kind": "nats-outage", "t_start_ns": 300, "t_end_ns": 400,
                    "seq_before": 16, "seq_after": 24, "ctl_seq_before": {"stdout": 16, "stderr": 16}}],
        "agent": {"mainpid_before": 4300, "mainpid_after": 4300,
                  "exec_ts_before": "fixture-start", "exec_ts_after": "fixture-start"},
        "exec_start_lines": 1, "reliable_lines": 1 if path == "reliable" else 0,
        "probe_reached_lines": 0 if path == "reliable" else 4, "rebuild_lines": 1})
    row = {"pid": "task-fixture", "nid": "agt1", "argv": ["probe", "--nonce", nonce],
           "status": "EXITED", "exit_code": 23}
    write_json(root / "final" / "ps.json", {"schema": "ps", "schema_version": 1, "processes": [row]})


def mutate_legacy_carrier(root, nonce, shape):
    # The two honest N-1 discontinuity shapes: a clean prefix (nothing after
    # the old connection died) and an internal break (publishing resumed on
    # the rebuilt connection); both with the exit undelivered, as measured.
    synthetic_carrier(root, nonce, "legacy")
    recs = records(root / "final" / "ledger.jsonl", nonce)
    for stream in STREAMS:
        mine = [r for r in recs if r["stream"] == stream]
        if shape == "prefix":
            keep = mine[:18]
        else:
            keep = mine[:12] + mine[20:]
        write_records(root / "final" / stream, keep, True)
    write_json(root / "final" / "ctl-outcome.json", {"finished": False})


def selftest():
    nonce = "0123456789abcdef0123456789abcdef"
    count = 0
    def run_case(name, mutate, expected, judge=continuity):
        nonlocal count
        with tempfile.TemporaryDirectory(prefix="hot-update-oracle-") as tmp:
            root = Path(tmp)
            synthetic_evidence(root, nonce)
            mutate(root)
            passed = True
            try:
                # Judges print their receipt lines (the legacy signature, the
                # N-1 discontinuity summary); the selftest is about verdicts.
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    result = judge(root, nonce, 5)
                if judge is legacy:
                    passed = result is True
            except EvidenceError:
                passed = False
            require(passed is expected, f"selftest {name}: got {passed}, wanted {expected}")
        count += 1

    def replace_json(root, rel, change):
        doc = json_file(root / rel)
        change(doc)
        write_json(root / rel, doc)

    run_case("normal continuity", lambda root: None, True)
    run_case("empty baseline", lambda root: (root / "before" / "stdout").write_text(""), False)
    run_case("missing final stdout", lambda root: (root / "final" / "stdout").write_text(""), False)
    run_case("duplicate stdout", lambda root: (root / "final" / "stdout").write_text(
        (root / "final" / "stdout").read_text() * 2), False)
    run_case("missing middle record", lambda root: (root / "final" / "stdout").write_text(
        "\n".join((root / "final" / "stdout").read_text().splitlines()[:10]
                  + (root / "final" / "stdout").read_text().splitlines()[11:]) + "\n"), False)
    run_case("reordered stdout", lambda root: (root / "final" / "stdout").write_text(
        "\n".join(reversed((root / "final" / "stdout").read_text().splitlines())) + "\n"), False)
    run_case("changed child PID", lambda root: replace_json(root, "post/snapshot.json",
        lambda doc: doc["current"].update(pid=9999)), False)
    run_case("disk changed but old image runs", lambda root: replace_json(root, "upgrade.json",
        lambda doc: doc.update(actual_sha="a" * 64)), False)
    run_case("wrong exit code", lambda root: replace_json(root, "final/ctl-outcome.json",
        lambda doc: doc.update(exit_code=0)), False)
    run_case("no post-upgrade source progress", lambda root: replace_json(root, "post/snapshot.json",
        lambda doc: doc.update(data_count=15)), False)
    run_case("truncated source", lambda root: (root / "final" / "ledger.jsonl").write_text(
        (root / "final" / "ledger.jsonl").read_text()[:-2]), False)
    run_case("foreign final PID", lambda root: replace_json(root, "final/final.json",
        lambda doc: doc["identity"].update(pid=9999)), False)
    run_case("changed parent", lambda root: replace_json(root, "post/snapshot.json",
        lambda doc: doc["current"].update(ppid=1)), False)
    run_case("post sample beyond final source", lambda root: replace_json(root, "post/snapshot.json",
        lambda doc: doc.update(data_count=1000)), False)
    run_case("post samples reversed", lambda root: replace_json(root, "post/snapshot.json",
        lambda doc: doc.update(observed_at_ns=50)), False)
    run_case("foreign first post sample", lambda root: replace_json(root, "post/snapshot-start.json",
        lambda doc: doc.update(nonce="f" * 32)), False)
    run_case("invalid terminal JSON shape", lambda root: (root / "final/final.json").write_text("[]"), False)
    run_case("missing source", lambda root: (root / "final/ledger.jsonl").unlink(), False)
    run_case("invalid ctl outcome", lambda root: write_json(root / "final/ctl-outcome.json", {}), False)
    run_case("already broken baseline", lambda root: (root / "before/ledger.jsonl").write_text(
        (root / "before/ledger.jsonl").read_text() + json.dumps({"nonce": nonce,
        "kind": "pipe_error", "stream": "stdout", "seq": 9, "errno": errno.EPIPE}) + "\n"), False)
    run_case("legacy exact signature", lambda root: mutate_legacy(root, nonce), True, legacy)
    run_case("healthy cannot match legacy signature", lambda root: None, False, legacy)
    run_case("legacy violates normal contract", lambda root: mutate_legacy(root, nonce), False)
    def unrelated_death(root):
        mutate_legacy(root, nonce)
        replace_json(root, "post/snapshot.json", lambda doc: doc.update(alive=False))
    run_case("death cannot match legacy signature", unrelated_death, False, legacy)
    def empty_legacy(root):
        mutate_legacy(root, nonce)
        (root / "before" / "stderr").write_text("")
    run_case("empty baseline cannot match legacy signature", empty_legacy, False, legacy)
    def no_exec(root):
        mutate_legacy(root, nonce)
        replace_json(root, "upgrade.json", lambda doc: doc.update(actual_sha="a" * 64))
    run_case("no exec cannot match legacy signature", no_exec, False, legacy)
    def early_epipe(root):
        mutate_legacy(root, nonce)
        recs = records(root / "final/ledger.jsonl", nonce)
        for rec in recs:
            if rec["kind"] == "pipe_error":
                rec["seq"] = 2
        write_records(root / "final/ledger.jsonl", recs)
    run_case("pre-baseline EPIPE cannot match legacy signature", early_epipe, False, legacy)
    # S1 re-pin: the ctl half of the known failure is now a prompt rc 74.
    def pre_s1_shape(root):
        mutate_legacy(root, nonce)
        write_json(root / "final" / "ctl-outcome.json", {"finished": False})
    run_case("pre-S1 ctl shape (never exited) cannot match the S1 signature", pre_s1_shape, False, legacy)
    def rc74_but_complete(root):
        # Everything of the known failure except the loss itself: EPIPE rows,
        # EXITED(-1), rc 74 — but the ctl transcripts hold the whole source.
        mutate_legacy(root, nonce)
        recs = records(root / "final" / "ledger.jsonl", nonce)
        for stream in STREAMS:
            write_records(root / "final" / stream,
                          [r for r in recs if r["kind"] == "data" and r["stream"] == stream], True)
    run_case("rc 74 with complete output is not the known failure", rc74_but_complete, False, legacy)
    run_case("rc 74 with complete output is not continuity either", rc74_but_complete, False)

    # Drill 35 carrier judges (hot-update-reliable-exec-plan §5.2).
    def with_carrier(mutate=None, path="reliable"):
        def apply(root):
            synthetic_carrier(root, nonce, path)
            if mutate is not None:
                mutate(root)
        return apply
    legacy_path = lambda root, n, minimum: carrier(root, n, minimum, "legacy")  # noqa: E731
    run_case("carrier continuity (reliable)", with_carrier(), True, carrier)
    run_case("carrier continuity (legacy, C2)", with_carrier(path="legacy"), True, legacy_path)
    run_case("carrier without its record", lambda root: None, False, carrier)
    run_case("carrier: agent MainPID changed", with_carrier(lambda root: replace_json(root, "carrier.json",
        lambda doc: doc["agent"].update(mainpid_after=4301))), False, carrier)
    run_case("carrier: agent service restarted", with_carrier(lambda root: replace_json(root, "carrier.json",
        lambda doc: doc["agent"].update(exec_ts_after="later"))), False, carrier)
    run_case("carrier: zero progress inside a fault window", with_carrier(lambda root: replace_json(root, "carrier.json",
        lambda doc: doc["faults"][1].update(seq_after=doc["faults"][1]["seq_before"]))), False, carrier)
    run_case("carrier: no fault recorded at all", with_carrier(lambda root: replace_json(root, "carrier.json",
        lambda doc: doc.update(faults=[]))), False, carrier)
    run_case("carrier: fault window not time ordered", with_carrier(lambda root: replace_json(root, "carrier.json",
        lambda doc: doc["faults"][0].update(t_end_ns=doc["faults"][0]["t_start_ns"]))), False, carrier)
    run_case("carrier: exec took the legacy path (reliable_lines=0)", with_carrier(lambda root: replace_json(root, "carrier.json",
        lambda doc: doc.update(reliable_lines=0))), False, carrier)
    run_case("carrier: executed twice", with_carrier(lambda root: replace_json(root, "carrier.json",
        lambda doc: doc.update(exec_start_lines=2, reliable_lines=2))), False, carrier)
    run_case("carrier: declared for the other path", with_carrier(lambda root: replace_json(root, "carrier.json",
        lambda doc: doc.update(path="legacy"))), False, carrier)
    run_case("carrier: output lost", with_carrier(lambda root: (root / "final" / "stdout").write_text(
        "".join((root / "final" / "stdout").read_text().splitlines(True)[:20]))), False, carrier)
    run_case("carrier: exit not delivered", with_carrier(lambda root: write_json(
        root / "final" / "ctl-outcome.json", {"finished": False})), False, carrier)
    run_case("carrier: wrong exit code", with_carrier(lambda root: write_json(
        root / "final" / "ctl-outcome.json", {"finished": True, "exit_code": 74})), False, carrier)
    run_case("carrier: final task row still RUNNING", with_carrier(lambda root: replace_json(root, "final/ps.json",
        lambda doc: doc["processes"][0].update(status="RUNNING"))), False, carrier)
    run_case("carrier: final task row missing", with_carrier(lambda root: (root / "final/ps.json").unlink()), False, carrier)
    run_case("carrier: post-fault child identity changed", with_carrier(lambda root: replace_json(root, "post/snapshot.json",
        lambda doc: doc["current"].update(pid=9999))), False, carrier)
    run_case("carrier (legacy, C2): probe never reached the old agent", with_carrier(lambda root: replace_json(
        root, "carrier.json", lambda doc: doc.update(probe_reached_lines=0)), "legacy"), False, legacy_path)
    run_case("carrier (legacy, C2): a reliable line contradicts the path", with_carrier(lambda root: replace_json(
        root, "carrier.json", lambda doc: doc.update(reliable_lines=1)), "legacy"), False, legacy_path)
    run_case("legacy carrier: clean prefix + exit undelivered", lambda root: mutate_legacy_carrier(root, nonce, "prefix"),
             True, legacy_carrier)
    run_case("legacy carrier: internal break + exit undelivered", lambda root: mutate_legacy_carrier(root, nonce, "break"),
             True, legacy_carrier)
    def full_but_no_exit(root):
        synthetic_carrier(root, nonce, "legacy")
        write_json(root / "final" / "ctl-outcome.json", {"finished": False})
    run_case("legacy carrier: complete transcript but exit undelivered", full_but_no_exit, True, legacy_carrier)
    run_case("legacy carrier: fully continuous transcript is NOT an N-1 discontinuity",
             with_carrier(path="legacy"), False, legacy_carrier)
    def broken_and_delivered(root):
        mutate_legacy_carrier(root, nonce, "break")
        write_json(root / "final" / "ctl-outcome.json", {"finished": True, "exit_code": 23})
    run_case("legacy carrier: internal break with the exit delivered still counts", broken_and_delivered,
             True, legacy_carrier)
    def reliable_line_present(root):
        mutate_legacy_carrier(root, nonce, "prefix")
        replace_json(root, "carrier.json", lambda doc: doc.update(reliable_lines=1))
    run_case("legacy carrier: a reliable exec line means the old ctl was not on the legacy path",
             reliable_line_present, False, legacy_carrier)
    def foreign_bytes(root):
        mutate_legacy_carrier(root, nonce, "break")
        recs = records(root / "final" / "stdout", nonce, transcript=True)
        recs[3] = dict(recs[3], at_ns=999999)
        write_records(root / "final" / "stdout", recs, True)
    run_case("legacy carrier: a non-source byte is never a discontinuity", foreign_bytes, False, legacy_carrier)
    def duplicated(root):
        mutate_legacy_carrier(root, nonce, "break")
        recs = records(root / "final" / "stdout", nonce, transcript=True)
        write_records(root / "final" / "stdout", recs + recs[-1:], True)
    run_case("legacy carrier: a duplicated record is never a discontinuity", duplicated, False, legacy_carrier)
    def executed_twice(root):
        mutate_legacy_carrier(root, nonce, "prefix")
        replace_json(root, "carrier.json", lambda doc: doc.update(exec_start_lines=2))
    run_case("legacy carrier: executed twice", executed_twice, False, legacy_carrier)
    def no_fault(root):
        mutate_legacy_carrier(root, nonce, "prefix")
        replace_json(root, "carrier.json", lambda doc: doc.update(faults=[]))
    run_case("legacy carrier: discontinuity without a recorded fault", no_fault, False, legacy_carrier)
    def lost_management(root):
        mutate_legacy_carrier(root, nonce, "prefix")
        replace_json(root, "post/ps.json", lambda doc: doc["processes"][0].update(status="EXITED", exit_code=-1))
    run_case("legacy carrier: today's agent lost the child", lost_management, False, legacy_carrier)
    # S1 review D1: a loss that began BEFORE the first injection (the ctl held
    # 10 records when the fault was injected; the transcript stops at 5) is
    # an old ctl dying on its own, never an honest N-1 discontinuity.
    def died_before_fault(root):
        synthetic_carrier(root, nonce, "legacy")
        recs = records(root / "final" / "ledger.jsonl", nonce)
        for stream in STREAMS:
            write_records(root / "final" / stream, [r for r in recs if r["stream"] == stream][:5], True)
        write_json(root / "final" / "ctl-outcome.json", {"finished": True, "exit_code": 70})
    run_case("legacy carrier: loss before the first fault is not attributable", died_before_fault, False, legacy_carrier)
    def lost_exactly_at_fault(root):
        synthetic_carrier(root, nonce, "legacy")
        recs = records(root / "final" / "ledger.jsonl", nonce)
        for stream in STREAMS:
            write_records(root / "final" / stream, [r for r in recs if r["stream"] == stream][:10], True)
        write_json(root / "final" / "ctl-outcome.json", {"finished": False})
    run_case("legacy carrier: loss from exactly the injection instant is attributable", lost_exactly_at_fault, True, legacy_carrier)
    def no_ctl_counts(root):
        mutate_legacy_carrier(root, nonce, "prefix")
        replace_json(root, "carrier.json", lambda doc: [f.pop("ctl_seq_before") for f in doc["faults"]])
    run_case("legacy carrier: fault record without ctl-side counts is invalid evidence", no_ctl_counts, False, legacy_carrier)
    run_case("carrier: fault record without ctl-side counts is invalid evidence", with_carrier(lambda root: replace_json(
        root, "carrier.json", lambda doc: [f.pop("ctl_seq_before") for f in doc["faults"]])), False, carrier)
    with tempfile.TemporaryDirectory(prefix="hot-update-empty-floor-") as tmp:
        root = Path(tmp)
        synthetic_evidence(root, nonce)
        try:
            continuity(root, nonce, 0)
        except EvidenceError:
            count += 1
        else:
            raise EvidenceError("zero minimum disabled nonempty floor")
    print(f"oracle selftest: {count} positive/negative synthetic controls passed (no Docker)")


# origin: hot-update S3 plan §5.2 / §8.2. Counts classify kernel objects, never
# descriptor numbers: import may allocate different fd numbers after exec.
REPLAY_NAME = "tether-replay"
HANDOFF_NAME = "tether-handoff"
ACTIVATED = "agent: image activated"
CUT = "agent: image cut"
DEFERRED = "agent: upgrade deferred"


def fd_class(link):
    if re.fullmatch(r"pipe:\[[0-9]+\]", link):
        return "pipe"
    for kind, name in (("replay", REPLAY_NAME), ("handoff", HANDOFF_NAME)):
        if link in (f"/memfd:{name}", f"/memfd:{name} (deleted)"):
            return kind
    if link == "anon_inode:[pidfd]":
        return "pidfd"
    return "other"


def fdcensus(pid):
    counts = dict.fromkeys(("pipe", "replay", "handoff", "pidfd", "other"), 0)
    objects = {key: [] for key in counts}
    unsafe = []
    for entry in Path(f"/proc/{pid}/fd").iterdir():
        try:
            link = os.readlink(entry)
            info = Path(f"/proc/{pid}/fdinfo/{entry.name}").read_text()
        except FileNotFoundError:
            continue  # a transient network descriptor closed during the census
        kind = fd_class(link)
        counts[kind] += 1
        objects[kind].append(link)
        flags = re.search(r"^flags:\s+([0-7]+)$", info, re.M)
        require(flags is not None, "fd flags unreadable")
        if int(entry.name) > 2 and not int(flags[1], 8) & os.O_CLOEXEC:
            unsafe.append(link)
        if any(part in link for part in (".tether-upgrade.lock", "/dev/ptmx")):
            unsafe.append(link)
    return {"counts": counts, "objects": {k: sorted(v) for k, v in objects.items()}, "unsafe": unsafe}


def agent_identity(pid):
    ident = process_identity(pid)
    require(ident["state"] != "Z", "agent is a zombie")
    ident["argv"] = [v.decode() for v in Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0") if v]
    digest = hashlib.sha256()
    with open(f"/proc/{pid}/exe", "rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
    ident["exe_sha"] = digest.hexdigest()
    fd2 = os.stat(f"/proc/{pid}/fd/2")
    ident["fd2"] = [fd2.st_dev, fd2.st_ino]
    children = set()
    for task in Path(f"/proc/{pid}/task").iterdir():
        try:
            children.update(map(int, (task / "children").read_text().split()))
        except FileNotFoundError:
            continue
    ident["children"] = sorted(children)
    require(same_process(ident, process_identity(pid)), "agent changed during identity census")
    return ident


def stop_agent(args):
    # Pin before checking identity/argv so PID reuse cannot redirect the signal.
    ident = json_file(args.identity)
    require(Path(args.pidfile).name == f"hup-agent-{args.nonce}.pid", "agent pidfile nonce mismatch")
    pids = Path(args.pidfile).read_text().split()
    require(pids == [str(ident.get("pid"))] and ident.get("pid", 0) > 1, "ambiguous agent pidfile")
    try:
        fd = os.pidfd_open(ident["pid"])
    except ProcessLookupError:
        return
    try:
        current = agent_identity(ident["pid"])
        require(same_process(ident, current) and current["argv"] == ident.get("argv"),
                "refuse unrelated agent identity")
        require(Path(current["argv"][0]).name == "tether" and current["argv"][1:] ==
                ["agent", "--session", args.session, "--nid", "agt1"], "refuse unrelated agent argv")
        terminate_pinned(fd)
    finally:
        os.close(fd)


def unique_fields(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate log field {key}")
        result[key] = value
    return result


def slog_record(line):
    # origin: hot-update S3 plan §3.2; DR-1. The fields of activation live
    # INSIDE msg, unlike cut/exec attributes. Decode the handler envelope first.
    if line.lstrip().startswith("{"):
        record = json.loads(line, object_pairs_hook=unique_fields)
    else:
        parts = shlex.split(line)
        require(all("=" in part for part in parts), "invalid text log record")
        record = unique_fields(part.split("=", 1) for part in parts)
    require(isinstance(record, dict) and isinstance(record.get("msg"), str), "missing log message")
    return record


def freeze_windows(text):
    samples = []
    keys = ("generation", "tasks", "freeze_window_ms", "shim_ms", "import_ms")
    for line in text.splitlines():
        if ACTIVATED not in line:
            continue
        message = slog_record(line)["msg"]
        require(message.startswith(ACTIVATED + " "), "invalid activation message")
        fields = shlex.split(message[len(ACTIVATED):])
        require(all("=" in field for field in fields), "invalid activation fields")
        fields = unique_fields(field.split("=", 1) for field in fields)
        row = {}
        for key in keys:
            require(key in fields and re.fullmatch(r"[0-9]+", fields[key]) is not None,
                    f"activation missing or malformed {key}")
            row[key] = int(fields[key])
        # generation/tasks/freeze_window_ms are never zero on a real switch (the window spans
        # exec + Go init). shim_ms/import_ms are Duration.Milliseconds() truncations and ARE 0
        # when the step took <1 ms: the first real 34.G run (2026-10-02) imported generation 2
        # in under a millisecond and an all-fields-positive rule reported a healthy switch red.
        for key in ("generation", "tasks", "freeze_window_ms"):
            require(row[key] > 0, f"empty activation {key}")
        samples.append(row)
    require(bool(samples), "no image activation samples")
    require(len({s["generation"] for s in samples}) == len(samples), "duplicate image activation")
    return samples


def log_audit(text, nonce):
    # origin: hot-update S3 plan §8.1. Text attributes use '='; JSON uses ':'.
    # Old legacy exec records omit reliable, which is itself the legacy path.
    result = dict.fromkeys(("exec_lines", "reliable_lines", "legacy_lines", "restored_lines",
                            "cut_lines", "deferred_lines", "forbidden_lines"), 0)
    for line in text.splitlines():
        if "agent:" not in line:
            continue
        record = slog_record(line)
        message = record["msg"]
        if message == "agent: exec" and nonce in str(record.get("argv", "")):
            reliable = record.get("reliable", False)
            require(reliable in (True, False, "true", "false"), "invalid exec path")
            result["exec_lines"] += 1
            result["reliable_lines" if reliable in (True, "true") else "legacy_lines"] += 1
        result["restored_lines"] += message.startswith(ACTIVATED + " ")
        result["cut_lines"] += message == CUT
        result["deferred_lines"] += message == DEFERRED
        result["forbidden_lines"] += any(message.startswith(prefix) for prefix in (
            "agent: image abandoned", "agent: dispatched image unreadable", "agent: upgrade rollback blocked"))
    return result


def initial_image_generation(root, running_sha):
    # origin: hot-update S5 plan §7.1 / T48 / DRILL-RUN 34.A/G/W.
    # The fresh flow owner starts at one. Read the ORIGINAL /proc executable's
    # version and SHA captured before the fault, independently of the candidate
    # marker whose exact generation transition this judge verifies.
    root = Path(root)
    require((root/"initial-image.sha").exists() and (root/"initial-image.version").exists(),
            "missing initial running artifact")
    require((root/"initial-image.sha").read_text().strip() == running_sha,
            "initial generation belongs to another running artifact")
    version = (root/"initial-image.version").read_text()
    require(version.startswith("tether "), "missing initial artifact version")
    domains = [line.split()[1:] for line in version.splitlines() if line.startswith("image-domains ")]
    require(len(domains) <= 1, "ambiguous initial image domains")
    return 1 if domains and "flow-v1" in domains[0] else 0


def hotupdate(root, nonce, minimum, hops=1, rollback=False, deferred=0):
    root = Path(root)
    require(type(hops) is int and hops > 0, "empty hop chain")
    ident, row = check_baseline(root, nonce, minimum)
    post, start, after_row = check_post(root, nonce, minimum, ident, row)
    source, observed, recs, outcome = check_final_output(root, nonce, minimum, ident, post, start)
    require(after_row.get("status") == "RUNNING", "restored task is not RUNNING")
    require(not [r for r in recs if r.get("kind") == "pipe_error"], "image closed workload pipes")
    if not rollback:
        require(all(observed[s] == source[s] for s in STREAMS), "image lost original output")
        require(outcome == {"finished": True, "exit_code": 23}, "image lost original ctl exit")
    terminal = check_terminal_row(root, nonce)
    require(terminal["pid"] == row["pid"], "terminal row belongs to a different task")
    doc = json_file(root / "hotupdate.json")
    require(doc.get("nonce") == nonce, "foreign hot-update evidence")
    audit = doc.get("audit", {})
    for key, value in {"exec_lines": 1, "reliable_lines": 1, "restored_lines": hops,
                       "cut_lines": hops, "deferred_lines": deferred, "forbidden_lines": 0}.items():
        require(type(audit.get(key)) is int and audit[key] == value, f"wrong audit {key}")
    chain = doc.get("hops", [])
    agents = doc.get("agents", [])
    fds = doc.get("fds", [])
    require(len(chain) == hops and len(agents) == hops + 1 and len(fds) == hops + 1,
            "incomplete hop observations")
    samples = freeze_windows((root / "activations.log").read_text())
    initial = initial_image_generation(root, agents[0]["exe_sha"])
    require([s["generation"] for s in samples] == list(range(initial + 1, initial + hops + 1)), "activation chain changed")
    require(all(s["tasks"] == 1 for s in samples), "activation carried wrong task count")
    first = agents[0]
    require(first.get("pid") == ident["ppid"] and first.get("pid", 0) > 1
            and first.get("start_ticks", 0) > 0 and first.get("boot_id"), "missing agent identity")
    for i, agent in enumerate(agents):
        require(same_process(first, agent), "agent PID/starttime/boot changed")
        require(agent.get("children") == [ident["pid"]], "child set changed")
        for key in ("exec_ts", "nrestarts", "mainpid", "fd2", "leases"):
            require(key in first and agent.get(key) == first[key], f"agent {key} changed")
        require(first["mainpid"] in (0, first["pid"]), "invalid service MainPID")
        require(first["mainpid"] == 0 or bool(first["exec_ts"]), "empty service timestamp")
        census = fds[i]
        c = census.get("counts", {})
        require(c.get("handoff") == 0 and c.get("pidfd") == 1 and c.get("replay", 0) >= 2
                and c.get("pipe", 0) >= 2, "missing/leaked task descriptor")
        require(census.get("unsafe") == [], "unsafe inherited descriptors")
        for kind in ("pipe", "replay", "pidfd"):
            require(c[kind] == fds[0]["counts"][kind], f"{kind} class changed")
        for kind in ("pipe", "replay"):
            objects = census.get("objects", {}).get(kind, [])
            require(len(objects) == c[kind] and objects == fds[0]["objects"][kind],
                    f"{kind} ownership changed")
            if kind == "pipe":
                require(len(set(objects)) == len(objects), "duplicate pipe references")
    for i, hop in enumerate(chain):
        if rollback and i == hops - 1:
            marker = hop.get("marker", {})
            require(marker.get("state") == "rolled_back" and
                    "register deadline exceeded" in marker.get("detail", "") and
                    marker.get("resource_phase") == "redispatched", "wrong rollback marker")
            require(hop.get("prev_consumed") is True and hop.get("disk_sha") == chain[0]["old_sha"],
                    "rollback did not consume previous image")
        else:
            if not rollback:
                check_upgrade_doc(hop)
            marker = hop.get("marker", {})
            if rollback:
                require(marker.get("state") == "pending", "candidate registered despite the blackhole")
            require(marker.get("resource_phase") == "active", "image was not activated")
        require(marker.get("generation") == initial + i + 1 and marker.get("imported_resources") == 1
                and marker.get("freeze_window_ms", 0) > 0, "incomplete image marker")
        require(hop.get("old_sha") == agents[i].get("exe_sha") and
                hop.get("new_sha") == agents[i+1].get("exe_sha") and
                hop["old_sha"] != hop["new_sha"], "broken hop chain")
        require(hop.get("pid_before") == first["pid"] == hop.get("pid_after"), "hop restarted agent")
    if hops == 2:
        require(agents[-1]["exe_sha"] == agents[0]["exe_sha"], "reverse hop did not return to original image")
    if rollback:
        lost = task_row(root / "lost-ps.json", nonce)
        require(lost.get("pid") == row["pid"] and lost.get("status") == "LOST", "no LOST to RUNNING proof")


def refused_continuity(root, nonce, minimum):
    ident, row = check_baseline(root, nonce, minimum)
    post, start, after = check_post(root, nonce, minimum, ident, row)
    source, observed, recs, outcome = check_final_output(root, nonce, minimum, ident, post, start)
    require(after.get("status") == "RUNNING", "refusal lost management")
    require(not [r for r in recs if r.get("kind") == "pipe_error"] and source == observed,
            "refusal disrupted output")
    require(outcome == {"finished": True, "exit_code": 23}, "refusal lost ctl exit")
    terminal = check_terminal_row(root, nonce)
    require(terminal["pid"] == row["pid"], "refusal terminal row belongs to a different task")


def legacy_exporter(root, nonce, minimum):
    try:
        continuity(root, nonce, minimum)
        return False
    except EvidenceError:
        pass
    ident, row, source, observed, recs, outcome = load_final(root, nonce, minimum)
    errors = [r for r in recs if r.get("kind") == "pipe_error"]
    require(len(errors) == 2 and {r.get("stream") for r in errors} == set(STREAMS)
            and all(r.get("errno") == errno.EPIPE for r in errors), "legacy exporter needs both EPIPEs")
    require(row.get("status") == "EXITED" and row.get("exit_code") == -1, "legacy exporter row not lost")
    for stream in STREAMS:
        err = next(r for r in errors if r["stream"] == stream)
        baseline = records(Path(root) / "before" / stream, nonce, transcript=True)
        require(len(baseline) <= err.get("seq", -1) < len(source[stream])
                and len(observed[stream]) <= err["seq"], "legacy EPIPE outside injection window")
    # Current ctl legacy branch returns an untyped timeout error: exitcode.go's
    # default is EX_SOFTWARE (70), unlike the reliable owner's lost-instance 74.
    require(outcome == {"finished": True, "exit_code": 70}, "legacy ctl did not time out with rc70")
    stderr = (Path(root) / "final/stderr").read_text()
    require("exec: timed out after 1m0s waiting for chunk" in stderr, "legacy timeout diagnostic missing")
    audit = json_file(Path(root) / "legacy-audit.json")
    require(audit.get("exec_lines") == 1 and audit.get("reliable_lines") == 0
            and audit.get("probe_lines", 0) > 0, "legacy fallback not proven")
    print(f"HOT_UPDATE_LEGACY_EXPORTER output=epipe management=exited_minus_one ctl=idle_timeout_rc70 "
          f"pid={ident['pid']} nonce={nonce}", file=sys.stderr)
    return True


def synthetic_hotupdate(root, nonce, hops=1):
    import copy
    synthetic_carrier(root, nonce)
    up = json_file(root / "upgrade.json")
    (root/"initial-image.version").write_text("tether fixture (proto v1)\nimage-domains exec-v1 pty-v1\nimage-abi v1\n")
    (root/"initial-image.sha").write_text("a"*64+"\n")
    agents, chain = [], []
    for i in range(hops + 1):
        agents.append({"pid": 4300, "start_ticks": 42, "boot_id": "fixture-boot", "children": [4312],
                       "exe_sha": ("a" if i % 2 == 0 else "b") * 64, "exec_ts": "fixture-start",
                       "nrestarts": 0, "mainpid": 4300, "fd2": [1, 2], "leases": 0})
    for i in range(hops):
        hop = copy.deepcopy(up)
        hop.update(old_sha=agents[i]["exe_sha"], new_sha=agents[i+1]["exe_sha"], actual_sha=agents[i+1]["exe_sha"])
        hop["marker"].update(new_sha=hop["new_sha"], generation=i+1, resource_phase="active",
                             imported_resources=1, freeze_window_ms=12)
        chain.append(hop)
    census = {"counts": {"pipe": 2, "replay": 2, "handoff": 0, "pidfd": 1, "other": 5},
              "objects": {"pipe": ["pipe:[1]", "pipe:[2]"], "replay": ["/memfd:tether-replay (deleted)"]*2}, "unsafe": []}
    write_json(root / "hotupdate.json", {"nonce": nonce, "hops": chain, "agents": agents,
               "fds": [copy.deepcopy(census) for _ in agents], "audit": {"exec_lines": 1, "reliable_lines": 1,
               "restored_lines": hops, "cut_lines": hops, "deferred_lines": 0, "forbidden_lines": 0}})
    (root / "activations.log").write_text("".join(
        f'time=2026-10-02T00:00:00.000Z level=INFO msg="{ACTIVATED} generation={i+1} tasks=1 freeze_window_ms=12 shim_ms=2 import_ms=4"\n' for i in range(hops)))


def image_selftest():
    # Evidence mutations execute every claimed guard against a known-good case.
    # They are deliberately independent of the guard's expression.
    nonce = "0123456789abcdef0123456789abcdef"
    mutations = {
        "handoff leak": lambda d: d["fds"][1]["counts"].update(handoff=1),
        "replay changed": lambda d: d["fds"][1]["counts"].update(replay=3),
        "pidfd missing": lambda d: d["fds"][1]["counts"].update(pidfd=0),
        "pidfd duplicate": lambda d: d["fds"][1]["counts"].update(pidfd=2),
        "extra child": lambda d: d["agents"][1]["children"].append(4321),
        "exec twice": lambda d: d["audit"].update(exec_lines=2),
        "no restore": lambda d: d["audit"].update(restored_lines=0),
        "extra restore": lambda d: d["audit"].update(restored_lines=2),
        "freeze absent": lambda d: d["hops"][0]["marker"].pop("freeze_window_ms"),
        "wrong resource phase": lambda d: d["hops"][0]["marker"].update(resource_phase="lost"),
        "wrong imported count": lambda d: d["hops"][0]["marker"].update(imported_resources=2),
        "zero freeze": lambda d: d["hops"][0]["marker"].update(freeze_window_ms=0),
        "agent reused": lambda d: d["agents"][1].update(start_ticks=43),
        "agent PID changed": lambda d: d["agents"][1].update(pid=4301),
        "service start changed": lambda d: d["agents"][1].update(exec_ts="restart"),
        "lease leak": lambda d: d["agents"][1].update(leases=1),
        "restart": lambda d: d["agents"][1].update(nrestarts=1),
        "stderr rebound": lambda d: d["agents"][1].update(fd2=[3, 4]),
        "unsafe fd": lambda d: d["fds"][1]["unsafe"].append("socket:[12]"),
        "broken chain": lambda d: d["hops"][0].update(old_sha="c"*64),
        "wrong generation": lambda d: d["hops"][0]["marker"].update(generation=2),
        "duplicate pipe": lambda d: d["fds"][1]["objects"].update(pipe=["pipe:[1]", "pipe:[1]"]),
    }
    for name in ("positive", "two hops", "S5 boot", "S5 wrong SHA", "S5 missing version", "S5 stale generation", "broken second hop", "wrong reverse image", "S1 signature", "final RUNNING", "terminal replaced", "activation absent", *mutations):
        with tempfile.TemporaryDirectory(prefix="hot-update-image-") as tmp:
            root = Path(tmp)
            synthetic_evidence(root, nonce)
            hops = 2 if name in ("two hops", "broken second hop", "wrong reverse image") else 1
            synthetic_hotupdate(root, nonce, hops)
            if name.startswith("S5 "):
                (root/"initial-image.version").write_text("tether fixture (proto v1)\nimage-domains exec-v1 pty-v1 flow-v1 ss-v1\nimage-abi v1\n")
                doc = json_file(root/"hotupdate.json")
                for hop in doc["hops"]: hop["marker"]["generation"] += 1
                write_json(root/"hotupdate.json", doc)
                path = root/"activations.log"
                path.write_text(path.read_text().replace("generation=1", "generation=2"))
                if name == "S5 wrong SHA": (root/"initial-image.sha").write_text("c"*64+"\n")
                elif name == "S5 missing version": (root/"initial-image.version").write_text("")
                elif name == "S5 stale generation":
                    doc["hops"][0]["marker"]["generation"] = 1
                    write_json(root/"hotupdate.json", doc)
            elif name in mutations:
                doc = json_file(root / "hotupdate.json")
                mutations[name](doc)
                write_json(root / "hotupdate.json", doc)
            elif name == "broken second hop":
                doc = json_file(root / "hotupdate.json")
                doc["hops"][1]["old_sha"] = "c"*64
                write_json(root / "hotupdate.json", doc)
            elif name == "wrong reverse image":
                doc = json_file(root / "hotupdate.json")
                doc["agents"][2]["exe_sha"] = "d"*64
                doc["hops"][1].update(new_sha="d"*64, actual_sha="d"*64)
                doc["hops"][1]["marker"]["new_sha"] = "d"*64
                write_json(root / "hotupdate.json", doc)
            elif name == "S1 signature":
                mutate_legacy(root, nonce)
            elif name == "final RUNNING":
                write_json(root / "final/ps.json", json_file(root / "post/ps.json"))
            elif name == "terminal replaced":
                doc = json_file(root / "final/ps.json")
                doc["processes"][0]["pid"] = "replacement-task"
                write_json(root / "final/ps.json", doc)
            elif name == "activation absent":
                (root / "activations.log").write_text("")
            try:
                hotupdate(root, nonce, 5, hops)
                passed = True
            except EvidenceError:
                passed = False
            require(passed == (name in ("positive", "two hops", "S5 boot")), f"image mutation escaped: {name}")
    for link, kind in (("pipe:[123]", "pipe"), ("/memfd:tether-replay (deleted)", "replay"),
                       ("/memfd:tether-handoff (deleted)", "handoff"), ("anon_inode:[pidfd]", "pidfd"),
                       ("/memfd:tether-replay-other (deleted)", "other"), ("socket:[123]", "other")):
        require(fd_class(link) == kind, f"fd classification changed: {link}")
    for text in ("", f"{ACTIVATED} generation=1 tasks=1 shim_ms=2 import_ms=4",
                 f"{ACTIVATED} generation=1 tasks=1 freeze_window_ms=0 shim_ms=2 import_ms=4",
                 f"{ACTIVATED} generation=0 tasks=1 freeze_window_ms=9 shim_ms=2 import_ms=4",
                 f"{ACTIVATED} generation=1 tasks=0 freeze_window_ms=9 shim_ms=2 import_ms=4"):
        try:
            freeze_windows(json.dumps({"level": "INFO", "msg": text}) if text else "")
        except EvidenceError:
            continue
        raise EvidenceError("freeze-window accepted missing/empty sample")
    # A sub-millisecond import or shim is a real, healthy sample (Milliseconds() truncates):
    # the first real 34.G run recorded `import_ms=0` on generation 2.
    for text in (f"{ACTIVATED} generation=2 tasks=1 freeze_window_ms=112 shim_ms=99 import_ms=0",
                 f"{ACTIVATED} generation=2 tasks=1 freeze_window_ms=112 shim_ms=0 import_ms=0"):
        require(len(freeze_windows(json.dumps({"level": "INFO", "msg": text}))) == 1,
                f"freeze-window rejected a sub-millisecond sample: {text}")
    for shape in ("positive", "healthy", "S1", "wrong audit", "no diagnostic"):
        with tempfile.TemporaryDirectory(prefix="legacy-exporter-") as tmp:
            root = Path(tmp)
            synthetic_evidence(root, nonce)
            if shape != "healthy":
                mutate_legacy(root, nonce)
                if shape != "S1":
                    write_json(root / "final/ctl-outcome.json", {"finished": True, "exit_code": 70})
                if shape != "no diagnostic":
                    with (root / "final/stderr").open("a") as out:
                        out.write("exec: timed out after 1m0s waiting for chunk\n")
                write_json(root / "legacy-audit.json", {"exec_lines": 1, "reliable_lines": int(shape == "wrong audit"), "probe_lines": 4})
            try:
                with contextlib.redirect_stderr(io.StringIO()):
                    matched = legacy_exporter(root, nonce, 5)
            except EvidenceError:
                matched = False
            require(matched == (shape == "positive"), f"legacy exporter mutation escaped: {shape}")
    for name in ("positive", "EPIPE", "wrong exit", "still running", "terminal replaced"):
        with tempfile.TemporaryDirectory(prefix="refused-continuity-") as tmp:
            root = Path(tmp)
            synthetic_evidence(root, nonce)
            synthetic_carrier(root, nonce)
            if name == "EPIPE":
                mutate_legacy(root, nonce)
            elif name == "wrong exit":
                write_json(root / "final/ctl-outcome.json", {"finished": True, "exit_code": 74})
            elif name == "terminal replaced":
                doc = json_file(root / "final/ps.json")
                doc["processes"][0]["pid"] = "replacement-task"
                write_json(root / "final/ps.json", doc)
            elif name == "still running":
                write_json(root / "final/ps.json", json_file(root / "post/ps.json"))
            try:
                refused_continuity(root, nonce, 5)
                passed = True
            except EvidenceError:
                passed = False
            require(passed == (name == "positive"), f"refused continuity mutation escaped: {name}")
    print(f"image selftest: {len(mutations) + 31} positive/negative controls passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("producer")
    p.add_argument("--root", required=True)
    p.add_argument("--nonce", required=True)
    p.add_argument("--period", type=float, default=0.05)
    p.add_argument("--lifetime", type=float, default=240)
    for command in ("snapshot", "stop"):
        p = sub.add_parser(command)
        p.add_argument("--root", required=True)
        p.add_argument("--nonce", required=True)
        if command == "stop":
            p.add_argument("--terminate", action="store_true", help="cleanup-only: stop the verified fixture PID")
    p = sub.add_parser("stop-ctl")
    p.add_argument("--pidfile", required=True)
    p.add_argument("--nonce", required=True)
    p.add_argument("--ctl-name", default="tether", help="basename of the ctl binary the caller launched")
    for command in ("ready", "post-ready", "baseline", "continuity", "legacy", "carrier", "legacy-carrier", "count", "hotupdate", "legacy-exporter", "refused"):
        p = sub.add_parser(command)
        p.add_argument("--evidence", required=True)
        p.add_argument("--nonce", required=True)
        p.add_argument("--minimum", type=int, default=20)
        if command == "hotupdate":
            p.add_argument("--hops", type=int, default=1)
            p.add_argument("--rollback", action="store_true")
            p.add_argument("--deferred", type=int, default=0)
        if command == "carrier":
            p.add_argument("--path", choices=CARRIER_PATHS, default="reliable")
    p = sub.add_parser("initial-generation")
    p.add_argument("--evidence", required=True)
    p.add_argument("--running-sha", required=True)
    p = sub.add_parser("upgrade-proof")
    p.add_argument("--evidence", required=True)
    for command in ("identity", "fdcensus"):
        p = sub.add_parser(command)
        p.add_argument("--pid", type=int, required=True)
    p = sub.add_parser("stop-agent")
    p.add_argument("--identity", required=True)
    p.add_argument("--pidfile", required=True)
    p.add_argument("--nonce", required=True)
    p.add_argument("--session", default="lab")
    for command in ("freeze-window", "log-audit"):
        p = sub.add_parser(command)
        p.add_argument("--log", required=True)
        if command == "log-audit":
            p.add_argument("--nonce", required=True)
    sub.add_parser("selftest")
    args = parser.parse_args()
    if hasattr(args, "nonce"):
        require(re.fullmatch(r"[0-9a-f]{32}", args.nonce) is not None, "nonce must be 128-bit lowercase hex")
    if hasattr(args, "minimum"):
        require(args.minimum > 0, "minimum must be positive")
    if args.command == "producer":
        require(0.001 <= args.period <= 1 and 1 <= args.lifetime <= 600, "invalid producer budget")
        return producer(args)
    if args.command == "snapshot":
        snapshot(args)
    elif args.command == "stop":
        stop(args)
    elif args.command == "stop-ctl":
        stop_ctl(args)
    elif args.command == "ready":
        ready(args)
    elif args.command == "count":
        count(args)
    elif args.command == "post-ready":
        post_ready(args)
    elif args.command == "baseline":
        check_baseline(args.evidence, args.nonce, args.minimum)
    elif args.command == "continuity":
        continuity(args.evidence, args.nonce, args.minimum)
    elif args.command == "legacy":
        return 1 if legacy(args.evidence, args.nonce, args.minimum) else 0
    elif args.command == "carrier":
        carrier(args.evidence, args.nonce, args.minimum, args.path)
    elif args.command == "legacy-carrier":
        legacy_carrier(args.evidence, args.nonce, args.minimum)
    elif args.command == "initial-generation":
        print(initial_image_generation(args.evidence, args.running_sha))
    elif args.command == "upgrade-proof":
        check_upgrade(args.evidence)
    elif args.command == "refused":
        refused_continuity(args.evidence, args.nonce, args.minimum)
    elif args.command == "hotupdate":
        hotupdate(args.evidence, args.nonce, args.minimum, args.hops, args.rollback, args.deferred)
    elif args.command == "legacy-exporter":
        return 1 if legacy_exporter(args.evidence, args.nonce, args.minimum) else 0
    elif args.command == "identity":
        print(json.dumps(agent_identity(args.pid), sort_keys=True))
    elif args.command == "fdcensus":
        print(json.dumps(fdcensus(args.pid), sort_keys=True))
    elif args.command == "stop-agent":
        stop_agent(args)
    elif args.command == "log-audit":
        print(json.dumps(log_audit(Path(args.log).read_text(), args.nonce), sort_keys=True))
    elif args.command == "freeze-window":
        print(json.dumps(freeze_windows(Path(args.log).read_text()), sort_keys=True))
    elif args.command == "selftest":
        selftest()
        image_selftest()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (EvidenceError, OSError, ValueError) as exc:
        print(f"HOT_UPDATE_EVIDENCE_INVALID: {exc}", file=sys.stderr)
        sys.exit(2)
