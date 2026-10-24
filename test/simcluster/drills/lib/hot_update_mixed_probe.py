#!/usr/bin/env python3
"""Mixed control traffic and an offline oracle for hot-update drill 34.P.

Run inside the ctl container; judge on the host with the copied ledger. The
caller creates /tmp/hot-update-mixed-<nonce> on the target before starting run.
The adjacent hot_update_budget.sh must accompany this file into the container.

marks.json maps names to seconds relative to run's time.monotonic() origin.
External writers atomically rename a file into marks.d/<name>; its entire body
is a finite, nonnegative decimal timestamp in that same time base (a newline
is allowed). run.json exposes started_monotonic for writers in the container.
Names use letters, digits, underscore, dot or hyphen. Marks are immutable;
conflicting timestamps fail the run. The loop ingests marks while draining too.
judge also ingests marks.d, allowing a final mark after run has finished.

--strict on run adds short new run commands with a nonce-bearing started
response on stdout; --strict on judge requires those receipts, zero refusals,
zero requests over 5s, all three request classes, >=9/s of the declared 10/s,
and a nonempty observed fault window with a successful ledger write across it.
Without --strict, drill 34 retains its original verdict contract.

The default upgrade window is [cut, activated]; calibration uses the whole
run. started.json is the start handshake; --until-mark keeps scheduling through
the observed mark plus --tail, failing closed at --max-duration if it is absent.
Explicit --window-start/--window-end select named endpoints. Intersection
is inclusive at both ends. Rate counts launched requests over the scheduling
duration, excluding the final drain; skipped slots never count as requests.
run.json's complete flag is written only after every launched request is reaped.
seq_written is the full <nonce>-<seq> ledger token. dup_side_effects counts
distinct ledger lines appearing more than once, including unexpected lines.
Empty classes have null percentiles. over_5s uses HOT_UPDATE_REQUEST_S, despite
its historical field name. A local hard timeout is rc 124, a request failure:
it cannot prove the remote command was never accepted and is never retried.
"""

import argparse
from collections import Counter
import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import time


CLASSES = ("read", "write", "short")
MARK_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*\Z")


def positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError("expected a finite positive number")
    return number


def budgets(path=None):
    # origin: hot-update S3 plan §6. Parse assignments; never execute shell code.
    path = path or Path(__file__).with_name("hot_update_budget.sh")
    values = {}
    for line in path.read_text().splitlines():
        match = re.fullmatch(r"\s*(HOT_UPDATE_PROBE_RATE|HOT_UPDATE_REQUEST_S)=(\d+(?:\.\d+)?)\s*(?:#.*)?", line)
        if match:
            if match[1] in values:
                raise ValueError("duplicate budget assignment")
            values[match[1]] = positive(match[2])
    return values["HOT_UPDATE_PROBE_RATE"], values["HOT_UPDATE_REQUEST_S"]


def write_json(path, value):
    staging = path.with_name(path.name + ".tmp")
    staging.write_text(json.dumps(value, sort_keys=True, allow_nan=False) + "\n")
    staging.replace(path)


def add_mark(marks, name, value):
    stamp = float(value)
    if not MARK_NAME.fullmatch(name) or not math.isfinite(stamp) or stamp < 0:
        raise ValueError("invalid mark name or timestamp")
    if name in marks and marks[name] != stamp:
        raise ValueError("conflicting mark: " + name)
    marks[name] = stamp


def collect_marks(directory, marks):
    for path in sorted(directory.iterdir()):
        if path.name.startswith("."):
            continue  # Writers may stage hidden files before atomic rename.
        add_mark(marks, path.name, path.read_text().strip())


def request(seq, node, nonce, strict=False):
    kind = CLASSES[seq % len(CLASSES)]
    sample = {"seq": seq, "class": kind}
    if kind == "read":
        command = ["ps", "--json"] if (seq // 3) % 2 == 0 else ["node", "ls", "--json"]
        sample["argv0_kind"] = command[0]
    elif kind == "write":
        token = f"{nonce}-{seq}"
        sample.update(argv0_kind="exec", seq_written=token)
        # origin: hot-update S3 plan §8.1; DR-5. Overlapping writes make
        # the switch intersection measurable; the ledger still records one effect.
        command = ["exec", node, "--", "sh", "-c",
                   f'sleep 0.5; printf "%s\\n" {token} >> /tmp/hot-update-mixed-{nonce}/ledger; sleep 0.5']
    else:
        sample["argv0_kind"] = "exec"
        # Keep the nonce in both exec classes for the agent audit census.
        if strict and (seq // 3) % 2 == 0:
            sample["argv0_kind"] = "run"
            sample["started_marker"] = "PTY_STARTED:" + nonce
            command = ["run", node, "--", "sh", "-c",
                       'printf "PTY_STARTED:%s\\n" "$1"', "pty-start", nonce]
        else:
            command = ["exec", node, "--", "sh", "-c", "true", nonce]
    return ["tether"] + command, sample


def kill_group(child):
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def consume_output(entry, channel, chunk, origin):
    if channel == "stderr":
        entry["tail"] = (entry["tail"] + chunk)[-512:]
        return
    entry["stdout_tail"] = (entry["stdout_tail"] + chunk)[-512:]
    marker = entry["sample"]["started_marker"].encode()
    if (entry["sample"].get("started_latency_s") is None
            and re.search(re.escape(marker) + rb"\r?\n", entry["stdout_tail"])):
        entry["sample"]["started_latency_s"] = time.monotonic() - origin - entry["sample"]["t_start"]


def run(args):
    _, request_s = budgets()
    rate = positive(args.rate)
    duration = positive(args.max_duration if args.until_mark else args.duration)
    if args.until_mark and (not MARK_NAME.fullmatch(args.until_mark) or args.tail is None):
        raise ValueError("until-mark requires a valid name and tail")
    if not args.until_mark and (args.tail is not None or args.max_duration is not None):
        raise ValueError("tail/max-duration require until-mark")
    if not re.fullmatch(r"[0-9a-fA-F]{32}", args.nonce):
        raise ValueError("nonce must contain exactly 32 hexadecimal characters")
    pending_marks, marks = {}, {}
    for entry in args.mark:
        name, value = entry.rsplit("@", 1)
        add_mark(pending_marks, name, value)
        if name == args.until_mark:
            raise ValueError("until-mark must be observed externally")
        if pending_marks[name] > duration:
            raise ValueError("scheduled mark exceeds run duration")
    root = Path(args.evidence)
    root.mkdir(parents=True, exist_ok=True)
    # origin: hot-update S3 plan §8.2. Never mix generations of evidence.
    if any((root / name).exists() for name in ("run.json", "samples.jsonl", "marks.json", "started.json")):
        raise ValueError("evidence directory already contains a run")
    (root / "marks.d").mkdir(exist_ok=True)
    origin = time.monotonic()
    metadata = dict(rate=rate, duration_s=duration, request_s=request_s,
                    nonce=args.nonce, started_monotonic=origin, skipped=0,
                    scheduled=math.ceil(rate * duration), complete=False,
                    until_mark=args.until_mark, tail_s=args.tail, strict=getattr(args, "strict", False))
    write_json(root / "run.json", metadata)
    write_json(root / "marks.json", marks)
    active, seq = {}, 0
    limit = max(1, math.floor(4 * rate))
    selector = selectors.DefaultSelector()
    try:
        with (root / "samples.jsonl").open("x", buffering=1) as output:
            write_json(root / "started.json", {"started_monotonic": origin, "nonce": args.nonce})
            while seq < metadata["scheduled"] or active or time.monotonic() - origin < duration:
                now = time.monotonic() - origin
                previous_marks = dict(marks)
                for name, stamp in pending_marks.items():
                    if stamp <= now:
                        add_mark(marks, name, stamp)
                collect_marks(root / "marks.d", marks)
                if marks != previous_marks:
                    write_json(root / "marks.json", marks)
                # origin: hot-update S3 plan §8.1; DR-5. Staging can outlast
                # the nominal run. Only an observed activation ends traffic.
                if args.until_mark and args.until_mark in marks:
                    duration = marks[args.until_mark] + args.tail
                    if duration > args.max_duration:
                        raise ValueError("activation tail exceeds max-duration")
                    metadata.update(duration_s=duration, scheduled=math.ceil(rate * duration))

                # origin: hot-update S3 plan §8.1 row 34.P. A slow request
                # must not move subsequent slots; missed slots are evidence.
                while seq < metadata["scheduled"] and seq / rate <= now:
                    slot = seq
                    seq += 1
                    if now >= duration or now - slot / rate >= 1 / rate or len(active) >= limit:
                        metadata["skipped"] += 1
                        continue
                    argv, sample = request(slot, args.node, args.nonce, strict=metadata["strict"])
                    sample["t_start"] = time.monotonic() - origin
                    started_probe = sample["argv0_kind"] == "run"
                    child = subprocess.Popen(argv, stdin=subprocess.DEVNULL,
                                             stdout=subprocess.PIPE if started_probe else subprocess.DEVNULL, stderr=subprocess.PIPE,
                                             start_new_session=True)
                    os.set_blocking(child.stderr.fileno(), False)
                    entry = dict(child=child, sample=sample, tail=b"", stdout_tail=b"", timed_out=False)
                    active[child.pid] = entry
                    selector.register(child.stderr, selectors.EVENT_READ, (entry, "stderr"))
                    if started_probe:
                        sample["started_latency_s"] = None
                        os.set_blocking(child.stdout.fileno(), False)
                        selector.register(child.stdout, selectors.EVENT_READ, (entry, "stdout"))

                # Drain stderr incrementally, retaining only its tail. A noisy
                # failing ctl cannot block on a full pipe or grow probe memory.
                delay = 0.02
                if seq < metadata["scheduled"]:
                    delay = min(delay, max(0, seq / rate - (time.monotonic() - origin)))
                for key, _ in selector.select(delay):
                    entry, channel = key.data
                    chunk = os.read(key.fd, 65536)
                    if chunk:
                        consume_output(entry, channel, chunk, origin)
                    else:
                        selector.unregister(key.fileobj)

                for pid, entry in list(active.items()):
                    child, sample = entry["child"], entry["sample"]
                    now = time.monotonic() - origin
                    rc = child.poll()
                    if rc is None and now - sample["t_start"] >= 2 * request_s:
                        kill_group(child)
                        entry["timed_out"] = True
                        rc = child.wait()
                    if rc is None:
                        continue
                    # Do not wait for EOF: a grandchild may have inherited the
                    # pipe. Terminate the local group once its ctl has exited.
                    kill_group(child)
                    for channel, pipe in (("stderr", child.stderr), ("stdout", child.stdout)):
                        if pipe is None:
                            continue
                        while True:
                            try:
                                chunk = os.read(pipe.fileno(), 65536)
                            except BlockingIOError:
                                break
                            if not chunk:
                                break
                            consume_output(entry, channel, chunk, origin)
                        try:
                            selector.unregister(pipe)
                        except KeyError:
                            pass
                        pipe.close()
                    if entry["timed_out"]:
                        entry["tail"] = (entry["tail"] + b"\nprobe: hard request timeout")[-512:]
                    now = time.monotonic() - origin
                    sample.update(t_end=now, latency_s=now - sample["t_start"],
                                  rc=124 if entry["timed_out"] else rc,
                                  stderr_tail=entry["tail"].decode("utf-8", errors="ignore"))
                    output.write(json.dumps(sample, sort_keys=True) + "\n")
                    del active[pid]
        collect_marks(root / "marks.d", marks)
        write_json(root / "marks.json", marks)
        if args.until_mark and args.until_mark not in marks:
            raise ValueError("until-mark was not observed before max-duration")
        metadata.update(complete=True, finished_s=time.monotonic() - origin)
        write_json(root / "run.json", metadata)
    finally:
        for entry in active.values():
            kill_group(entry["child"])
            entry["child"].wait()
            entry["child"].stderr.close()
            if entry["child"].stdout is not None:
                entry["child"].stdout.close()
        selector.close()
    return 0


def classification(sample):
    rc, stderr = sample["rc"], sample["stderr_tail"].lower()
    if rc == 0:
        return "ok"
    if rc == 75 and re.search(r"\bagent_upgrading\b", stderr):
        return "agent_upgrading"
    # origin: hot-update S3 plan §8.1 row 34.P. Generic timeout text is
    # insufficient: ps/exec also time out after the request has been sent.
    # connectWithAuthRetry can log a failed dial, then connect successfully.
    # That earlier retry must not relabel the eventual request failure.
    stderr = "\n".join(line for line in stderr.splitlines()
                       if not re.search(r"broker not ready .*; retrying in ", line))
    if rc == 69 or re.search(r"(?:cannot|could not) reach (?:the )?(?:broker|nats)\b", stderr):
        return "connect_failures"
    if re.search(r"\bdial (?:tcp|udp|unix)\b[^\n]*(?:connection refused|i/o timeout|timed out)", stderr):
        return "connect_failures"
    if re.search(r"(?:^|\n)(?:error: )?(?:connection refused|connect: (?:connection refused|i/o timeout|timed out))\s*$", stderr):
        return "connect_failures"
    return "request_failures"


def percentiles(values):
    values = sorted(values)
    result = {"count": len(values)}
    for name, percentile in (("p50", 50), ("p95", 95), ("p99", 99), ("max", 100)):
        result[name] = values[math.ceil(len(values) * percentile / 100) - 1] if values else None
    return result


def summarize(samples, ledger, metadata, window):
    counts = Counter(ledger)
    result = dict.fromkeys(("request_failures", "connect_failures", "stale_offline_failures",
                            "agent_upgrading", "rc0_without_effect", "effect_without_rc0",
                            "in_window", "writes_in_window", "starts_in_window", "over_5s"), 0)
    issued = {s["seq_written"] for s in samples if s["class"] == "write"}
    result["unrequested_effects"] = sum(n for token, n in counts.items() if token not in issued)
    result["dup_side_effects"] = sum(n > 1 for n in counts.values())
    result["skipped"] = metadata["skipped"]
    result["rate"] = len(samples) / metadata["duration_s"]
    result["configured_rate"] = metadata["rate"]
    result["refusals"] = 0
    new_runs = [s for s in samples if s["argv0_kind"] == "run"]
    started = [s["started_latency_s"] for s in new_runs if s.get("started_latency_s") is not None]
    result["new_run_count"] = len(new_runs)
    result["started_missing"] = len(new_runs) - len(started)
    result["started_over_5s"] = sum(t > metadata["request_s"] for t in started)
    result["started_response"] = percentiles(started)
    result["paths"] = {path: percentiles([s["latency_s"] for s in samples if s["argv0_kind"] == path])
                       for path in sorted({s["argv0_kind"] for s in samples})}
    result["classes"] = {kind: percentiles([s["latency_s"] for s in samples if s["class"] == kind])
                         for kind in CLASSES}
    result["successful"] = {kind: sum(s["class"] == kind and s["rc"] == 0 for s in samples) for kind in CLASSES}
    for sample in samples:
        category = classification(sample)
        starts_inside = window[0] <= sample["t_start"] <= window[1]
        overlaps = sample["t_start"] <= window[1] and sample["t_end"] >= window[0]
        if category != "ok":
            result[category] += 1
        if category == "agent_upgrading":
            result["refusals"] += 1
        # origin: hot-update S3 plan §8.1; DR-4. A refused request outside
        # the observed cut is a failure even if another request used that slot.
        if category == "agent_upgrading" and not starts_inside:
            result["request_failures"] += 1
        if sample["rc"] != 0 and re.search(r"\b(?:node_offline|STALE)\b", sample["stderr_tail"], re.I):
            result["stale_offline_failures"] += 1
        if sample["class"] == "write":
            present = counts[sample["seq_written"]] > 0
            result["rc0_without_effect"] += sample["rc"] == 0 and not present
            result["effect_without_rc0"] += sample["rc"] != 0 and present
            result["writes_in_window"] += sample["rc"] == 0 and present and overlaps
        result["in_window"] += overlaps
        result["starts_in_window"] += starts_inside
        result["over_5s"] += sample["latency_s"] > metadata["request_s"]
    return result


def accepted(result, mode, configured_rate, strict=False):
    common = (result["request_failures"] == 0 and result["connect_failures"] == 0
              and result["rate"] >= 0.9 * configured_rate)
    # origin: hot-update S4 plan §9.1 / T48. Strict is opt-in: 34's
    # calibration and in-window refusal contract remain unchanged.
    if strict:
        return (common and configured_rate == 10 and result["rate"] >= 9
                and result["refusals"] == 0 and result["over_5s"] == 0
                and result["new_run_count"] > 0 and result["started_missing"] == 0
                and result["started_over_5s"] == 0
                and result["dup_side_effects"] == 0 and result["rc0_without_effect"] == 0
                and result["effect_without_rc0"] == 0 and result["unrequested_effects"] == 0
                and result["stale_offline_failures"] == 0
                and (mode == "calibrate" or (result["in_window"] > 0
                     and result["starts_in_window"] > 0 and result["writes_in_window"] > 0))
                and all(result["successful"][kind] > 0 for kind in CLASSES))
    if mode == "calibrate":
        return common
    # origin: hot-update S3 plan §8.1; DR-4/DRILLS-V1. P0 cannot prove that
    # P1 exercised any work. A refusal is only safe when it has no side effect.
    return (common and result["dup_side_effects"] == 0 and result["rc0_without_effect"] == 0
            and result["effect_without_rc0"] == 0 and result["unrequested_effects"] == 0
            and all(result["successful"][kind] > 0 for kind in CLASSES)
            and result["agent_upgrading"] <= result["starts_in_window"]
            and result["stale_offline_failures"] == 0 and result["in_window"] >= 1
            and result["writes_in_window"] >= 1)


def judge(args):
    root = Path(args.evidence)
    metadata = json.loads((root / "run.json").read_text())
    samples = [json.loads(line) for line in (root / "samples.jsonl").read_text().splitlines()]
    # origin: hot-update S3 plan §8.2. Missing/truncated samples must fail
    # closed, even when every surviving request happened to succeed.
    if (metadata["complete"] is not True or not samples or metadata["skipped"] < 0
            or len(samples) + metadata["skipped"] != metadata["scheduled"]):
        raise ValueError("incomplete request evidence")
    positive(metadata["rate"])
    positive(metadata["duration_s"])
    positive(metadata["request_s"])
    seen = set()
    for sample in samples:
        seq = sample["seq"]
        _, expected = request(seq, "unused", metadata["nonce"], strict=metadata.get("strict", False))
        if (seq in seen or not 0 <= seq < metadata["scheduled"]
                or any(sample.get(key) != value for key, value in expected.items())
                or not isinstance(sample["rc"], int)
                or len(sample["stderr_tail"].encode()) > 512
                or not all(math.isfinite(sample[key]) for key in ("t_start", "t_end", "latency_s"))
                or not 0 <= sample["t_start"] <= sample["t_end"] <= metadata["finished_s"]
                or not math.isclose(sample["latency_s"], sample["t_end"] - sample["t_start"], abs_tol=1e-8)):
            raise ValueError("invalid or duplicate request sample")
        if sample["argv0_kind"] == "run":
            started = sample.get("started_latency_s")
            if started is not None and (type(started) not in (int, float) or not math.isfinite(started)
                                        or not 0 <= started <= sample["latency_s"]):
                raise ValueError("invalid started response evidence")
        seen.add(seq)
    marks = json.loads((root / "marks.json").read_text())
    collect_marks(root / "marks.d", marks)
    strict = getattr(args, "strict", False)
    # origin: hot-update S4 internal review rv-drill DRILL-7. Healthy
    # calibration observes the full run and has no fault to mark.
    start_name = args.window_start or ("cut" if args.mode == "upgrade" else None)
    start = float(marks[start_name]) if start_name else 0
    end_name = args.window_end or ("activated" if args.mode == "upgrade" else None)
    end = float(marks[end_name]) if end_name else metadata["finished_s"]
    if not math.isfinite(start) or not math.isfinite(end) or not 0 <= start <= end:
        raise ValueError("invalid window endpoints")
    if strict and (start >= end or end > metadata["finished_s"]):
        raise ValueError("strict mode requires a nonempty observed fault window")
    if strict and (metadata["rate"] != 10 or metadata["request_s"] != 5):
        raise ValueError("strict mode requires the declared 10/s and 5s budgets")
    result = summarize(samples, Path(args.ledger).read_text().splitlines(), metadata, (start, end))
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0 if accepted(result, args.mode, metadata["rate"], strict=strict) else 1


def selftest():
    # origin: hot-update S3 plan §8.2. Synthetic controls exercise the same
    # classifier and verdict as the file-based judge, without launching ctl.
    rate, request_s = budgets()
    metadata = dict(rate=rate, duration_s=3 / rate, request_s=request_s, skipped=0)
    samples = []
    for seq in range(3):
        _, sample = request(seq, "node", "a" * 32)
        sample.update(t_start=0, t_end=1, latency_s=1, rc=0, stderr_tail="")
        samples.append(sample)
    token = samples[1]["seq_written"]
    baseline = summarize(samples, [token], metadata, (0.5, 0.5))
    for mode in ("calibrate", "upgrade"):
        assert accepted(baseline, mode, rate), mode
    for field in ("dup_side_effects", "rc0_without_effect", "request_failures", "connect_failures",
                  "stale_offline_failures", "effect_without_rc0", "unrequested_effects", "in_window", "writes_in_window"):
        broken = dict(baseline, **{field: 0 if field in ("in_window", "writes_in_window") else 1})
        assert not accepted(broken, "upgrade", rate), field
    for field in ("request_failures", "connect_failures", "rate"):
        broken = dict(baseline, **{field: rate * 0.89 if field == "rate" else 1})
        assert not accepted(broken, "calibrate", rate), field
    for rc, stderr, category in (
        (0, "agent_upgrading STALE", "ok"),
        (75, "exec: an image switch is in progress; retry (try_again: agent_upgrading: image switch in progress; retry)", "agent_upgrading"),
        (70, "agent_upgrading", "request_failures"),
        (69, "nats: no responders available", "connect_failures"),
        (1, "cannot reach broker at nats://local:4222", "connect_failures"),
        (1, "could not reach the NATS server", "connect_failures"),
        (1, "dial tcp 127.0.0.1:4222: connect: connection refused", "connect_failures"),
        (1, "dial tcp 127.0.0.1:4222: i/o timeout", "connect_failures"),
        (1, "connection refused", "connect_failures"),
        (1, "error: connect: i/o timeout", "connect_failures"),
        (75, "ps: request timed out: context deadline exceeded", "request_failures"),
        (75, "node ls: nats: timeout (broker unreachable on NATS)", "request_failures"),
        (75, "tether: ps: broker not ready (dial tcp local:4222: connect: connection refused); retrying in 1s\n"
             "error: ps: request timed out: context deadline exceeded", "request_failures"),
        (74, "exec: node_offline: status=STALE", "request_failures"),
        (77, "broker auth_callout rejected the connection", "request_failures"),
        (124, "probe: hard request timeout", "request_failures"),
    ):
        assert classification(dict(rc=rc, stderr_tail=stderr)) == category, (rc, stderr)
    for label, rows, lines, changes, field in (
        ("duplicate", samples, [token, token], {}, "dup_side_effects"),
        ("missing", samples, [], {}, "rc0_without_effect"),
        ("stale", [dict(s, rc=74, stderr_tail="node_offline: status=STALE") for s in samples], [token], {}, "stale_offline_failures"),
        ("connect", [dict(s, rc=69) for s in samples], [token], {}, "connect_failures"),
        ("starved", samples, [token], {"duration_s": 6 / rate}, "rate"),
    ):
        result = summarize(rows, lines, dict(metadata, **changes), (0.5, 0.5))
        assert result[field] > 0, label
        assert not accepted(result, "calibrate" if label == "starved" else "upgrade", rate), label
    failed = [dict(s, rc=75, stderr_tail="try_again: agent_upgrading: image switch in progress; retry") for s in samples]
    result = summarize(failed, [token], metadata, (0.5, 0.5))
    assert result["agent_upgrading"] == 3 and result["request_failures"] == 3
    assert result["effect_without_rc0"] == 1 and not accepted(result, "upgrade", rate)
    for kind in CLASSES:
        broken = dict(baseline, successful=dict(baseline["successful"], **{kind: 0}))
        assert not accepted(broken, "upgrade", rate), kind
    for changes in ({"rate": rate * 0.89}, {"agent_upgrading": 1, "starts_in_window": 0}):
        assert not accepted(dict(baseline, **changes), "upgrade", rate)
    assert percentiles(list(range(20, 0, -1))) == dict(count=20, p50=10, p95=19, p99=20, max=20)
    assert percentiles([]) == dict(count=0, p50=None, p95=None, p99=None, max=None)
    # origin: hot-update S4 plan §9.1 / T48. Refusals inside the cut remain
    # permitted by 34, but no request or five-second violation is exempt in 36.
    strict_baseline = dict(baseline, starts_in_window=1, new_run_count=1)
    assert accepted(strict_baseline, "upgrade", rate, strict=True)
    for field in ("refusals", "over_5s", "started_missing", "started_over_5s", "request_failures", "connect_failures",
                  "dup_side_effects", "rc0_without_effect", "effect_without_rc0",
                  "unrequested_effects", "stale_offline_failures"):
        assert not accepted(dict(strict_baseline, **{field: 1}), "upgrade", rate, strict=True), field
    for field in ("in_window", "starts_in_window", "writes_in_window", "new_run_count"):
        assert not accepted(dict(strict_baseline, **{field: 0}), "upgrade", rate, strict=True), field
    for kind in CLASSES:
        broken = dict(strict_baseline, successful=dict(baseline["successful"], **{kind: 0}))
        assert not accepted(broken, "upgrade", rate, strict=True), kind
    assert not accepted(dict(baseline, rate=8.99), "upgrade", rate, strict=True)
    print("hot-update mixed probe selftest: PASS")
    return 0


def mark(args):
    # origin: hot-update S3 plan §8.1; DR-5. This runs in the SAME container
    # as the sensor, after the host observes an agent log event through logs.sh.
    root = Path(args.evidence)
    receipt = json.loads((root / "started.json").read_text())
    marks = {}
    add_mark(marks, args.name, time.monotonic() - receipt["started_monotonic"])
    path = root / "marks.d" / args.name
    if path.exists():
        raise ValueError("mark already observed")
    staging = path.with_name("." + path.name)
    staging.write_text(str(marks[args.name]) + "\n")
    staging.replace(path)
    return 0


def main():
    # origin: hot-update S6 plan §8.3 / §12.1 P29. The S3/S4 oracle stays
    # frozen; broker samples require the dedicated production attempt channel.
    if len(sys.argv) > 1 and sys.argv[1] in ("broker-run", "broker-judge", "broker-config"):
        import importlib.util
        spec = importlib.util.spec_from_file_location("broker_probe", Path(__file__).with_name("hot_update_broker_probe.py"))
        broker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(broker)
        if sys.argv[1] == "broker-judge":
            return broker.main(["judge"] + sys.argv[2:])
        try:
            return broker_run_main(broker, sys.argv[1:])
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
            print('{"rc":2,"verdict":"SETUP-RED","criterion":"broker_collector_failed"}')
            return 2
    default_rate, _ = budgets()
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    runner = commands.add_parser("run")
    runner.add_argument("--evidence", required=True)
    runner.add_argument("--strict", action="store_true", help="36: include short run requests with started-response receipts")
    runner.add_argument("--rate", type=positive, default=default_rate)
    ending = runner.add_mutually_exclusive_group(required=True)
    ending.add_argument("--duration", type=positive)
    ending.add_argument("--until-mark")
    runner.add_argument("--tail", type=positive)
    runner.add_argument("--max-duration", type=positive)
    runner.add_argument("--node", required=True)
    runner.add_argument("--nonce", required=True)
    runner.add_argument("--mark", action="append", default=[])
    checker = commands.add_parser("judge")
    checker.add_argument("--evidence", required=True)
    checker.add_argument("--ledger", required=True)
    checker.add_argument("--mode", choices=("calibrate", "upgrade"), required=True)
    checker.add_argument("--strict", action="store_true", help="36: zero refusals/failures/over-budget requests, 10/s >=9/s")
    checker.add_argument("--window-start")
    checker.add_argument("--window-end")
    marker = commands.add_parser("mark")
    marker.add_argument("--evidence", required=True)
    marker.add_argument("--name", required=True)
    commands.add_parser("selftest")
    args = parser.parse_args()
    return {"run": run, "mark": mark, "judge": judge, "selftest": lambda _: selftest()}[args.command](args)


# origin: hot-update S6 plan §8.3 / T50.
def broker_run_main(broker, argv):
    """Absolute-slot scheduler. Only the child executable boundary is injected.

    Config supplies absolute argv arrays for read/write/short, a read_kind
    (nodes/processes), and the production sink's FD/logical-ID environment names.
    Tokens {logical_id} and {nonce} are substituted as individual argv strings;
    no shell command is assembled by the collector. A separate target effects
    ledger must be copied after the late tail, then supplied to broker-judge.
    """
    parser = argparse.ArgumentParser(description=broker_run_main.__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    runner = commands.add_parser("broker-run")
    runner.add_argument("--config", type=Path, required=True)
    runner.add_argument("--evidence", type=Path, required=True)
    runner.add_argument("--events", type=Path, required=True)
    runner.add_argument("--duration", type=positive, required=True)
    runner.add_argument("--late-tail", type=positive, default=5)
    runner.add_argument("--nonce", required=True)
    runner.add_argument("--hop", action="append", required=True)
    profile = runner.add_mutually_exclusive_group()
    profile.add_argument("--calibrate", action="store_true")
    profile.add_argument("--calibration", type=Path)
    maker = commands.add_parser("broker-config")
    maker.add_argument("--output", type=Path, required=True)
    maker.add_argument("--binary", required=True)
    maker.add_argument("--node", required=True)
    maker.add_argument("--effects-path", required=True)
    for command in (runner, maker):
        if command is maker:
            command.add_argument("--attempt-fd-env", default="TETHER_ATTEMPT_RECEIPT_FD")
            command.add_argument("--logical-id-env", default="TETHER_ATTEMPT_LOGICAL_ID")
    args = parser.parse_args(argv)
    if args.command == "broker-config":
        broker.require(Path(args.binary).is_absolute() and re.fullmatch(r"[A-Za-z0-9._-]+", args.node), "invalid_command_supply")
        # This witness runs as the reliable target process, outside broker SQL
        # and image codecs. A second spawn appends a second independent row.
        script = ('import json,os,pathlib,sys,time; pid=os.getpid(); '
                  'ticks=int(pathlib.Path("/proc/self/stat").read_text().rsplit(")",1)[1].split()[19]); '
                  'row=dict(logical_id=sys.argv[1],pid=pid,start_ticks=ticks,'
                  'boot_id=pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip(),'
                  'at_ns=time.clock_gettime_ns(time.CLOCK_BOOTTIME)); '
                  'stable=sys.argv[2]+".stable-writes"; '
                  'write_fd=os.open(stable,os.O_CREAT|os.O_WRONLY|os.O_APPEND,0o600) if sys.argv[3]=="write" else -1; '
                  'os.write(write_fd,(sys.argv[1]+"\\n").encode()) if write_fd>=0 else None; '
                  'os.fsync(write_fd) if write_fd>=0 else None; '
                  'os.close(write_fd) if write_fd>=0 else None; '
                  'fd=os.open(sys.argv[2],os.O_CREAT|os.O_WRONLY|os.O_APPEND,0o600); '
                  'os.write(fd,(json.dumps(row)+"\\n").encode()); os.close(fd); '
                  'print(json.dumps(dict(row,kind="broker-probe-started-v1")),flush=True)')
        base = [args.binary, "exec", args.node, "--", "python3", "-I", "-B", "-c", script, "{logical_id}", args.effects_path]
        config = dict(attempt_fd_env=args.attempt_fd_env, logical_id_env=args.logical_id_env,
                      commands=dict(read=[args.binary, "node", "ls", "--json"], write=base+["write"], short=base+["short"]), read_kind="nodes",
                      read_alternate=dict(argv=[args.binary, "ps", "--json"], kind="processes"))
        broker.publish(args.output, config)
        return 0
    config = broker.read_json(args.config)
    broker.require(args.late_tail * 1_000_000_000 >= broker.REQUEST_NS, "late_effect_tail_too_short")
    broker.require(re.fullmatch(r"[0-9a-f]{32}", args.nonce) and len(args.hop) == len(set(args.hop)), "invalid_run_supply")
    for field in ("attempt_fd_env", "logical_id_env"):
        broker.require(re.fullmatch(r"[A-Z][A-Z0-9_]+", config.get(field, "")), "missing_production_sink_binding")
    broker.require(config["attempt_fd_env"] != config["logical_id_env"], "conflicting_sink_binding")
    for kind in broker.CLASSES:
        command = config["commands"][kind]
        broker.require(isinstance(command, list) and command and all(isinstance(arg, str) for arg in command)
                       and Path(command[0]).is_absolute(), "missing_absolute_command")
    broker.require(config.get("read_kind") in ("nodes", "processes"), "missing_read_validator")
    if "read_alternate" in config:
        alternate = config["read_alternate"]
        broker.require(alternate.get("kind") in ("nodes", "processes") and isinstance(alternate.get("argv"), list)
                       and alternate["argv"] and all(isinstance(arg, str) for arg in alternate["argv"])
                       and Path(alternate["argv"][0]).is_absolute(), "invalid_alternate_read")
    rate, request_s = budgets()
    broker.require(rate == 10 and request_s * 1_000_000_000 == broker.REQUEST_NS, "budget_contract_changed")
    root = args.evidence
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    broker.require(not any(root.iterdir()), "evidence_already_contains_run")
    os.chmod(root, 0o700)
    origin = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
    # origin: hot-update S6 plan §8.3 / T50 / FIX-BLACKOUT-2.
    # Calibration uses joined batches of one operation per class. Half of that
    # measured throughput leaves capacity for retry/settlement and fresh probes.
    # Only the hermetic consumer opts in; the live drill retains fixed 10/s.
    interval = 100_000_000
    calibration = None
    if args.calibration is not None:
        calibration = broker.read_json(args.calibration)
        interval = broker.calibrated_interval(calibration)
    duration_ns = int(args.duration * 1_000_000_000)
    count = 36 if args.calibrate else (duration_ns + interval - 1) // interval
    broker.require(count <= broker.MAX_SLOTS, "slot_inventory_overflow")
    ending = origin + duration_ns
    metadata = dict(schema=broker.SCHEMA, nonce=args.nonce, rate=1_000_000_000 / interval, request_ns=broker.REQUEST_NS,
                    started_ns=origin, ended_ns=ending, expected_slots=count, hops=args.hop,
                    scheduling_ended_ns=ending, late_tail_ns=int(args.late_tail * 1_000_000_000),
                    complete=False, late_tail_complete=False)
    if args.calibrate:
        metadata["profile"] = "calibration"
        metadata["batches"] = []
    elif calibration is not None:
        metadata.update(profile="calibrated", interval_ns=interval, calibration=calibration)
    broker.publish(root / "run.json", metadata)
    ordinary = [dict(logical_id=f"{args.nonce}-{slot}", slot=slot, **{"class": broker.CLASSES[slot % 3]},
                     scheduled_ns=origin + slot * interval, targeted=False) for slot in range(count)]
    active, windows, targeted, next_slot = {}, {}, set(), 0
    selector = selectors.DefaultSelector()
    streams = {}

    def now_ns():
        return time.clock_gettime_ns(time.CLOCK_BOOTTIME)

    def append(name, row):
        raw = json.dumps(row, sort_keys=True, allow_nan=False) + "\n"
        streams[name].write(raw)
        broker.require(sum(stream.tell() for stream in streams.values()) <= broker.MAX_BYTES, "sink_overflow")

    def start(slot):
        if slot["targeted"] or args.calibrate:
            append("slots", slot)  # Register before any child can act.
        if len(active) >= 256:
            raise ValueError("collector_child_capacity")
        read_fd, write_fd = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)
        os.set_blocking(write_fd, True)
        env = dict(os.environ, **{config["attempt_fd_env"]: str(write_fd), config["logical_id_env"]: slot["logical_id"]})
        supplied, read_kind = config["commands"][slot["class"]], config["read_kind"]
        if slot["class"] == "read" and "read_alternate" in config and (slot.get("slot", 0) // 3) % 2:
            supplied, read_kind = config["read_alternate"]["argv"], config["read_alternate"]["kind"]
        command = [arg.replace("{logical_id}", slot["logical_id"]).replace("{nonce}", args.nonce) for arg in supplied]
        started = now_ns()
        try:
            child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     pass_fds=(write_fd,), env=env, start_new_session=True)
        except BaseException:
            os.close(read_fd)
            raise
        finally:
            os.close(write_fd)
        entry = dict(child=child, slot=slot, started_ns=started, buffers=dict(stdout=b"", stderr=b"", attempts=b""),
                     attempt_lines=[], process=None, pipes=[child.stdout, child.stderr, read_fd], read_kind=read_kind)
        active[child.pid] = entry
        # The observation starts immediately before Popen; scheduler and exec
        # overhead belong to the slot, not to an additional request budget.
        for channel, pipe in zip(("stdout", "stderr", "attempts"), entry["pipes"]):
            fd = pipe if isinstance(pipe, int) else pipe.fileno()
            os.set_blocking(fd, False)
            selector.register(fd, selectors.EVENT_READ, (entry, channel))

    def consume(entry, channel, chunk):
        entry["buffers"][channel] += chunk
        broker.require(len(entry["buffers"][channel]) <= 65536, "child_output_overflow")
        if channel == "attempts":
            while b"\n" in entry["buffers"][channel]:
                line, entry["buffers"][channel] = entry["buffers"][channel].split(b"\n", 1)
                row = broker.decode(line)
                broker.require(row.get("logical_id") == entry["slot"]["logical_id"], "attempt_identity_changed")
                entry["attempt_lines"].append(row)
                broker.require(len(entry["attempt_lines"]) <= 129, "attempt_overflow")
                append("attempts", row)
        if channel == "stdout" and entry["slot"]["class"] != "read":
            # Completed lines are consumed once; a second marker is a failure.
            while b"\n" in entry["buffers"][channel]:
                line, entry["buffers"][channel] = entry["buffers"][channel].split(b"\n", 1)
                row = broker.decode(line)
                broker.require(row.get("kind") == "broker-probe-started-v1"
                               and row.get("logical_id") == entry["slot"]["logical_id"] and entry["process"] is None,
                               "invalid_true_started_receipt")
                broker.process_identity(row)
                # origin: hot-update S6 plan §8.3 / T50
                # The target's BOOTTIME is the actual spawn witness. Polling
                # can lag stdout delivery and supplies only an upper bound.
                broker.require(broker.integer(row.get("at_ns"), 1)
                               and entry["started_ns"] <= row["at_ns"] <= now_ns(), "invalid_true_started_time")
                entry["process"] = dict(pid=row["pid"], start_ticks=row["start_ticks"], boot_id=row["boot_id"], true_started_ns=row["at_ns"])

    # One lexical owner closes every pipe, kills process groups and waits every
    # Popen child on success, parse failure, cancellation and setup failure.
    try:
        for name in ("slots", "results", "attempts", "child-output"):
            streams[name] = (root / (name + ".jsonl")).open("x", buffering=1)
            os.chmod(root / (name + ".jsonl"), 0o600)
        if not args.calibrate:
            for slot in ordinary:
                append("slots", slot)
        broker.publish(root / "started.json", dict(schema=broker.SCHEMA, started_ns=origin, nonce=args.nonce))
        while next_slot < count or active or now_ns() < ending + int(args.late_tail * 1_000_000_000):
            stamp = now_ns()
            if args.events.exists():
                for event in broker.read_rows(args.events):
                    broker.require(event.get("hop") in args.hop and event.get("phase") in ("freeze", "auth", "control")
                                   and broker.integer(event.get("started_ns"), 1) and event["started_ns"] <= now_ns(), "invalid_phase_event")
                    key = (event["hop"], event["phase"])
                    prior = windows.get(key)
                    broker.require(prior is None or prior["started_ns"] == event["started_ns"]
                                   and (prior.get("finished_ns") is None or prior == event), "phase_event_rewritten")
                    windows[key] = event
                    if key not in targeted:
                        targeted.add(key)
                        for kind in broker.CLASSES:
                            slot = dict(logical_id=f"{args.nonce}-target-{len(targeted)}-{kind}", **{"class": kind},
                                        scheduled_ns=event["started_ns"], targeted=True, hop=key[0], phase=key[1])
                            start(slot)
            if args.calibrate:
                if not active and metadata["batches"] and "finished_ns" not in metadata["batches"][-1]:
                    metadata["batches"][-1]["finished_ns"] = stamp
                    ending = stamp
                if not active and next_slot < count:
                    metadata["batches"].append(dict(started_ns=stamp))
                    for _ in broker.CLASSES:
                        ordinary[next_slot]["scheduled_ns"] = stamp
                        start(ordinary[next_slot])
                        next_slot += 1
            else:
                while next_slot < count and ordinary[next_slot]["scheduled_ns"] <= stamp:
                    start(ordinary[next_slot])
                    next_slot += 1
            for key, _ in selector.select(.01):
                entry, channel = key.data
                chunk = os.read(key.fd, 65536)
                if chunk:
                    consume(entry, channel, chunk)
                else:
                    selector.unregister(key.fd)
            for pid, entry in list(active.items()):
                child, slot = entry["child"], entry["slot"]
                expired = now_ns() - slot["scheduled_ns"] >= broker.REQUEST_NS
                rc = child.poll()
                if rc is None and expired:
                    kill_group(child)
                    rc = child.wait()
                if rc is None:
                    continue
                kill_group(child)
                child.wait()
                for channel, pipe in zip(("stdout", "stderr", "attempts"), entry["pipes"]):
                    fd = pipe if isinstance(pipe, int) else pipe.fileno()
                    while True:
                        try:
                            chunk = os.read(fd, 65536)
                        except BlockingIOError:
                            break
                        if not chunk:
                            break
                        consume(entry, channel, chunk)
                    try:
                        selector.unregister(fd)
                    except KeyError:
                        pass
                for pipe in entry["pipes"]:
                    os.close(pipe) if isinstance(pipe, int) else pipe.close()
                entry["pipes"] = []
                broker.require(not entry["buffers"]["attempts"], "truncated_attempt_record")
                row = dict(logical_id=slot["logical_id"], started_ns=entry["started_ns"], finished_ns=now_ns(), rc=124 if expired else rc)
                if slot["class"] == "read":
                    data = broker.decode(entry["buffers"]["stdout"]) if rc == 0 else {}
                    row["validated_read"] = broker.validated_read(data, entry["read_kind"])
                else:
                    row["process"] = entry["process"]
                append("results", row)
                append("child-output", dict(logical_id=slot["logical_id"], rc=row["rc"], stderr=entry["buffers"]["stderr"].decode(errors="replace"), stdout=entry["buffers"]["stdout"].decode(errors="replace")))
                del active[pid]
        if args.calibrate:
            metadata["scheduling_ended_ns"] = ending
        broker.publish(root / "run.json", dict(metadata, ended_ns=now_ns(), complete=True, late_tail_complete=True))
        with (root / "windows.jsonl").open("x") as out:
            for row in windows.values():
                if row.get("finished_ns") is not None:
                    out.write(json.dumps(row, sort_keys=True) + "\n")
        return 0
    finally:
        for entry in active.values():
            kill_group(entry["child"])
            entry["child"].wait()
            for pipe in entry["pipes"]:
                os.close(pipe) if isinstance(pipe, int) else pipe.close()
        selector.close()
        for stream in streams.values():
            stream.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print("mixed probe: " + str(exc), file=sys.stderr)
        sys.exit(2)
