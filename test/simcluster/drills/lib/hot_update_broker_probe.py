#!/usr/bin/env python3
"""Broker collector and independent admission oracle (broker-probe-v1).

run.json declares nonce, default rate=10, request_ns=5000000000, start/end BOOTTIME,
expected slot count, complete and late_tail_complete. slots/results/attempts/
effects/windows are JSONL. Attempts use controlbudget.Attempt's exact JSON
fields; each CLI sink also emits an observer_close record with logical_id,
attempt_count and overflow. Records can complete out of ordinal order. The hermetic calibrated profile
adds an independently checked capacity receipt and fixed interval; its joined
calibration batches never stand in for timed absolute slots or cut coverage.

Window rows contain hop, phase (freeze/auth/control), started_ns, finished_ns.
All timestamps are the collector host's CLOCK_BOOTTIME; remote absolute clocks
must be mapped by the sender before supplying a window. Missing window coverage
is native INCOMPLETE=4. Slot loss, bad results, duplicate/late effects and sink
overflow are ASSERT-FAIL=1. Unreadable or malformed inputs are SETUP-RED=2.
"""
import argparse
from collections import defaultdict
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time


SCHEMA = "broker-probe-v1"
CLASSES = ("read", "write", "short")
PHASES = frozenset("dns proxy tls ws connect inbox_probe inbox_fallback discovery banner probe create request backoff cancel result freeze exec import auth ready maintenance".split())
ATTEMPT_CLASSES = frozenset("ok retryable refused timeout no_responders connection auth denied unsupported canceled skipped terminal result_unknown".split())
ATTEMPT_FIELDS = frozenset("logical_id ordinal phase started_mono_ns finished_mono_ns sent class backoff_ns".split())
MAX_BYTES = 128 * 1024 * 1024
REQUEST_NS = 5_000_000_000
MAX_SLOTS = 43_200  # The 4320-second stage ceiling at the frozen 10/s rate.


class InvalidEvidence(ValueError):
    pass


def require(value, criterion):
    if not value:
        raise InvalidEvidence(criterion)


def integer(value, minimum=0):
    return type(value) is int and value >= minimum


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate_json_field")
        result[key] = value
    return result


def decode(raw):
    return json.loads(raw, object_pairs_hook=unique_object,
                      parse_constant=lambda _: require(False, "nonfinite_json"))


def read_json(path):
    require(path.stat().st_size <= MAX_BYTES, "sink_overflow")
    return decode(path.read_bytes())


def read_rows(path):
    # origin: hot-update S6 plan §8.3. A partial final write is a lost sample.
    require(path.stat().st_size <= MAX_BYTES, "sink_overflow")
    raw = path.read_bytes()
    require(not raw or raw.endswith(b"\n"), "truncated_jsonl")
    return [decode(line) for line in raw.splitlines()]


def publish(path, value):
    temporary = path.with_name("." + path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, allow_nan=False)
        stream.write("\n")
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def verdict(failures, incomplete=(), **extra):
    rc = 1 if failures else 4 if incomplete else 0
    return dict(rc=rc, verdict={0: "GREEN", 1: "ASSERT-FAIL", 4: "INCOMPLETE"}[rc],
                failures=sorted(set(failures)), incomplete=sorted(set(incomplete)), **extra)


def join_children(children):
    # origin: hot-update S6 plan §10.2. Native child statuses stay native,
    # including missing custody and INCOMPLETE; no expected-red translation.
    priority = {1: 0, 2: 1, 3: 2, 4: 3, 0: 4}
    for child in children:
        require(type(child.get("rc")) is int and child["rc"] in priority, "invalid_native_child_rc")
    winner = min(children, key=lambda child: priority[child["rc"]])["rc"]
    return dict(rc=winner, verdict={0: "GREEN", 1: "ASSERT-FAIL", 2: "SETUP-RED", 3: "PRODUCT-RED", 4: "INCOMPLETE"}[winner],
                failures=[failure for child in children for failure in child.get("failures", [])],
                incomplete=[item for child in children for item in child.get("incomplete", [])], children=children)


def process_identity(row):
    require(isinstance(row, dict) and integer(row.get("pid"), 1)
            and integer(row.get("start_ticks"), 1) and isinstance(row.get("boot_id"), str)
            and bool(row["boot_id"]), "invalid_process_identity")
    return row["pid"], row["start_ticks"], row["boot_id"]


def validated_read(value, kind):
    if not isinstance(value, dict) or value.get("error") or value.get("code") or not isinstance(value.get(kind), list):
        return False
    if kind == "nodes":
        return bool(value[kind]) and all(isinstance(row, dict) and isinstance(row.get("nid"), str)
                                         and bool(row["nid"]) and row.get("status") in ("ONLINE", "STALE", "OFFLINE")
                                         for row in value[kind])
    if kind == "processes":
        return all(isinstance(row, dict) and isinstance(row.get("pid"), str) and bool(row["pid"])
                   and isinstance(row.get("nid"), str) and bool(row["nid"])
                   and row.get("status") in ("RUNNING", "EXITED", "LOST") for row in value[kind])
    return False


def logical_id(row, nonce):
    value = row.get("logical_id")
    require(isinstance(value, str) and re.fullmatch(re.escape(nonce) + r"-[A-Za-z0-9._-]{1,100}", value),
            "invalid_logical_id")
    return value


# origin: hot-update S6 plan §8.3 / T50 / FIX-BLACKOUT-2.
# A rate is derived from complete batches, never from just successful samples
# or the fastest round. Both halves must sustain it; timed slots then offer 1/2.
def calibrated_interval(receipt):
    require(isinstance(receipt, dict) and receipt.get("schema") == "broker-calibration-v1"
            and receipt.get("fraction") == [1, 2], "invalid_calibration_contract")
    batches = receipt.get("batches")
    require(isinstance(batches, list) and len(batches) == 12, "missing_calibration_batches")
    previous = 0
    for batch in batches:
        require(integer(batch.get("started_ns"), 1) and integer(batch.get("finished_ns"), 1)
                and batch["finished_ns"] > batch["started_ns"] >= previous, "invalid_calibration_batch")
        previous = batch["finished_ns"]
    # Six batches contain eighteen completed operations. Include scheduling
    # overhead between batches in capacity, not the subsequent late-effect tail.
    slower = max(batches[5]["finished_ns"] - batches[0]["started_ns"],
                 batches[11]["finished_ns"] - batches[6]["started_ns"])
    interval = max(100_000_000, (2 * slower + 17) // 18)
    require(integer(receipt.get("interval_ns"), 1) and receipt["interval_ns"] == interval, "calibration_rate_changed")
    return interval


# origin: hot-update S6 plan §8.3 / T50 / FIX-BLACKOUT-2.
def cut_residual(slot, result, attempts, cut_start, cut_end):
    # An auth request sent into the gap can time out AFTER auth is restored.
    # Charge that observed failed attempt and retry backoff to recovery. Union
    # the intervals so a timeout overlapping the blackout is never subtracted
    # twice. Successful slow work and failures first sent after readiness remain
    # charged to the baseline comparison; the absolute 5s oracle charges ALL.
    intervals = [(cut_start, cut_end)]
    for attempt in attempts:
        start, end = attempt["started_mono_ns"], attempt["finished_mono_ns"]
        failed = attempt["class"] in ("auth", "timeout", "no_responders", "connection", "retryable")
        # The CONNECT observer can classify a transport close as terminal even
        # though the outer historical-receipt retry policy reconnects safely.
        failed = failed or attempt["phase"] == "connect" and attempt["class"] == "terminal"
        if failed and start <= cut_end and end >= cut_start:
            intervals.append((start, end))
        if attempt["phase"] == "backoff":
            intervals.append((max(start, end - attempt["backoff_ns"]), end))
    cursor, recovered = slot["scheduled_ns"], 0
    for start, end in sorted(intervals):
        start, end = max(start, cursor), min(end, result["finished_ns"])
        if end > start:
            recovered += end - start
            cursor = end
    return result["finished_ns"] - slot["scheduled_ns"] - recovered


def control_judge(root, effects_path=None):
    metadata = read_json(root / "run.json")
    profile = metadata.get("profile", "fixed")
    require(profile in ("fixed", "calibration", "calibrated"), "unknown_run_profile")
    interval = calibrated_interval(metadata.get("calibration")) if profile == "calibrated" else 100_000_000
    require(metadata.get("interval_ns", interval) == interval, "calibration_rate_changed")
    require(metadata.get("schema") == SCHEMA and re.fullmatch(r"[0-9a-f]{32}", metadata.get("nonce", ""))
            and metadata.get("rate") == 1_000_000_000 / interval and metadata.get("request_ns") == REQUEST_NS,
            "invalid_run_contract")
    require(integer(metadata.get("started_ns"), 1) and integer(metadata.get("ended_ns"), 1)
            and metadata["ended_ns"] > metadata["started_ns"]
            and integer(metadata.get("expected_slots"), 1), "invalid_run_interval")
    require(metadata["expected_slots"] <= MAX_SLOTS, "slot_inventory_overflow")
    require(integer(metadata.get("scheduling_ended_ns"), 1)
            and metadata["scheduling_ended_ns"] > metadata["started_ns"]
            and integer(metadata.get("late_tail_ns"), REQUEST_NS)
            and metadata["ended_ns"] >= metadata["scheduling_ended_ns"] + metadata["late_tail_ns"],
            "invalid_schedule_or_late_tail")
    require(metadata.get("complete") is True and metadata.get("late_tail_complete") is True,
            "collector_not_joined")
    paths = [root / (name + ".jsonl") for name in ("slots", "results", "attempts", "windows")]
    paths.append(effects_path or root / "effects.jsonl")
    require((root / "run.json").stat().st_size + sum(path.stat().st_size for path in paths) <= MAX_BYTES, "sink_overflow")
    slots, results, attempts, windows, effects = [read_rows(path) for path in paths]
    failures, incomplete = [], []
    duration_ns = metadata["scheduling_ended_ns"] - metadata["started_ns"]
    if profile != "calibration" and metadata["expected_slots"] != (duration_ns + interval - 1) // interval:
        failures.append("schedule_denominator_changed")
    nonce = metadata["nonce"]
    expected, ordinary = {}, {}
    for slot in slots:
        key = logical_id(slot, nonce)
        require(slot.get("class") in CLASSES and integer(slot.get("scheduled_ns"), 1), "invalid_slot")
        if key in expected:
            failures.append("duplicate_slot")
        expected[key] = slot
        if slot.get("targeted") is not True:
            require(integer(slot.get("slot")), "invalid_slot_number")
            if slot["slot"] in ordinary:
                failures.append("duplicate_slot_number")
            ordinary[slot["slot"]] = slot
            if profile != "calibration" and slot["scheduled_ns"] != metadata["started_ns"] + slot["slot"] * interval:
                failures.append("nonabsolute_slot")
            if slot["class"] != CLASSES[slot["slot"] % 3]:
                failures.append("slot_class_changed")
    # origin: hot-update S6 plan §8.3 / T50. The predeclared denominator never
    # shrinks to the number of samples that happened to survive collection.
    if set(ordinary) != set(range(metadata["expected_slots"])):
        failures.append("missing_slot")
    observed = {}
    for row in results:
        key = logical_id(row, nonce)
        if key in observed:
            failures.append("duplicate_result")
        observed[key] = row
        require(integer(row.get("started_ns"), 1) and integer(row.get("finished_ns"), 1)
                and row["finished_ns"] >= row["started_ns"] and type(row.get("rc")) is int,
                "invalid_result_interval")
    if set(observed) != set(expected):
        failures.append("missing_or_unrequested_result")
    attempt_groups, closes = defaultdict(list), {}
    for row in attempts:
        key = logical_id(row, nonce)
        if row.get("kind") == "observer_close":
            require(set(row) == {"kind", "logical_id", "attempt_count", "overflow"}
                    and integer(row["attempt_count"]) and type(row["overflow"]) is bool, "invalid_sink_close")
            if key in closes:
                failures.append("duplicate_sink_close")
            closes[key] = row
            if row["overflow"]:
                failures.append("attempt_overflow")
            continue
        require(set(row) == ATTEMPT_FIELDS and row["phase"] in PHASES and integer(row["ordinal"], 1)
                and integer(row["started_mono_ns"], 1) and integer(row["finished_mono_ns"], 1)
                and row["finished_mono_ns"] >= row["started_mono_ns"] and type(row["sent"]) is bool
                and row["class"] in ATTEMPT_CLASSES and integer(row["backoff_ns"]),
                "invalid_attempt")
        attempt_groups[key].append(row)
    if set(attempt_groups) != set(expected) or set(closes) != set(expected):
        failures.append("missing_or_unrequested_attempt")
    for key, group in attempt_groups.items():
        group.sort(key=lambda row: row["ordinal"])
        count = closes.get(key, {}).get("attempt_count", -1)
        if count > 128 or len(group) != count or [row["ordinal"] for row in group] != list(range(1, count + 1)):
            failures.append("attempt_count_or_ordinal_loss")
        if sum(row["phase"] == "result" for row in group) != 1 or not any(row["phase"] == "connect" for row in group):
            failures.append("missing_connect_or_terminal_attempt")
        if key in expected:
            admission = "request" if expected[key]["class"] == "read" else "create"
            if not any(row["phase"] == admission and row["sent"] for row in group):
                failures.append("read_or_reliable_admission_not_sent")
        if key in observed:
            result = observed[key]
            if any(row["started_mono_ns"] < result["started_ns"] or row["finished_mono_ns"] > result["finished_ns"]
                   for row in group):
                failures.append("attempt_outside_operation")
    effect_groups = defaultdict(list)
    for effect in effects:
        key = logical_id(effect, nonce)
        process_identity(effect)
        require(integer(effect.get("at_ns"), 1), "invalid_effect_time")
        effect_groups[key].append(effect)
    if set(effect_groups) - set(expected):
        failures.append("unrequested_effect")
    for key, slot in expected.items():
        row = observed.get(key)
        if row is None:
            continue
        if row["started_ns"] < slot["scheduled_ns"] or row["finished_ns"] - slot["scheduled_ns"] > REQUEST_NS:
            failures.append("slot_budget_exceeded")
        if row["rc"] != 0:
            failures.append("request_failed")
        if slot["class"] == "read":
            if row.get("validated_read") is not True or effect_groups[key]:
                failures.append("read_not_validated")
        else:
            # origin: hot-update S6 plan §8.3 / T50. accepted/armed says
            # nothing about spawn. Both stdout and the target witness bind the
            # actual PID/start ticks; a late second effect remains a failure.
            process = row.get("process")
            if not isinstance(process, dict) or not process.get("true_started_ns"):
                failures.append("true_started_missing")
                continue
            identity = process_identity(process)
            require(integer(process["true_started_ns"], 1), "invalid_true_start")
            if not row["started_ns"] <= process["true_started_ns"] <= row["finished_ns"]:
                failures.append("invalid_true_start_interval")
            if process["true_started_ns"] - slot["scheduled_ns"] > REQUEST_NS:
                failures.append("started_budget_exceeded")
            if len(effect_groups[key]) != 1 or any(process_identity(effect) != identity for effect in effect_groups[key]):
                failures.append("duplicate_missing_or_changed_effect")
    if profile == "calibration":
        batches = metadata.get("batches")
        require(metadata["expected_slots"] == 36 and isinstance(batches, list) and len(batches) == 12,
                "missing_calibration_batches")
        receipt = dict(schema="broker-calibration-v1", fraction=[1, 2], batches=batches)
        slower = max(batches[5]["finished_ns"] - batches[0]["started_ns"],
                     batches[11]["finished_ns"] - batches[6]["started_ns"])
        receipt["interval_ns"] = max(100_000_000, (2 * slower + 17) // 18)
        calibrated_interval(receipt)
        if windows or len(expected) != 36:
            failures.append("calibration_contains_cut_or_extra_work")
        for slot in ordinary.values():
            batch = batches[slot["slot"] // 3]
            row = observed.get(slot["logical_id"])
            if (slot["scheduled_ns"] != batch["started_ns"] or row is None
                    or row["finished_ns"] > batch["finished_ns"]):
                failures.append("calibration_batch_not_joined")
        if (batches[0]["started_ns"] < metadata["started_ns"]
                or batches[-1]["finished_ns"] != metadata["scheduling_ended_ns"]):
            failures.append("calibration_interval_changed")
        receipt["evidence_sha256"] = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
        result = verdict(failures, slots=len(expected), calibration=receipt if not failures else None)
        if not failures:
            publish(root / "calibration.json", receipt)
        return result
    coverage = []
    comparison = []
    seen = set()
    for window in windows:
        require(isinstance(window.get("hop"), str) and bool(window["hop"])
                and window.get("phase") in ("freeze", "auth", "control")
                and integer(window.get("started_ns"), 1) and integer(window.get("finished_ns"), 1)
                and window["finished_ns"] >= window["started_ns"], "invalid_window")
        key = (window["hop"], window["phase"])
        require(key not in seen, "duplicate_window")
        seen.add(key)
        require(metadata["started_ns"] <= window["started_ns"] <= window["finished_ns"] <= metadata["ended_ns"],
                "window_outside_run")
        for kind in CLASSES:
            rows = [observed[k] for k in expected.keys() & observed.keys() if expected[k]["class"] == kind]
            fresh = sum(window["started_ns"] <= row["started_ns"] <= window["finished_ns"] for row in rows)
            inflight = sum(row["started_ns"] < window["started_ns"] <= row["finished_ns"] for row in rows)
            coverage.append(dict(hop=key[0], phase=key[1], **{"class": kind}, fresh=fresh, inflight=inflight))
            if not fresh:
                incomplete.append("missing_fresh_window_start")
    hops = metadata.get("hops")
    require(isinstance(hops, list) and hops and len(hops) == len(set(hops))
            and all(isinstance(hop, str) and hop for hop in hops), "invalid_hop_inventory")
    if seen != {(hop, phase) for hop in hops for phase in ("freeze", "auth", "control")}:
        incomplete.append("missing_phase_window")
    for hop in hops:
        starts = [row["started_ns"] for row in windows if row["hop"] == hop]
        ends = [row["finished_ns"] for row in windows if row["hop"] == hop]
        if not starts:
            continue
        for kind in CLASSES:
            cohort = [observed[k] for k in expected.keys() & observed.keys() if expected[k]["class"] == kind]
            if sum(row["finished_ns"] < min(starts) for row in cohort) < 20 or sum(row["started_ns"] > max(ends) for row in cohort) < 20:
                incomplete.append("missing_pre_post_cohort")
        if profile == "calibrated" and starts:
            # Nine targeted fresh probes plus ordinary arrivals form a real
            # queue. Compare recovery with the pre-cut baseline AND the service
            # demand at measured capacity; treating the burst as one baseline
            # request confuses intentional concurrency with a product stall.
            cut_keys = [k for k in expected.keys() & observed.keys()
                        if observed[k]["started_ns"] <= max(ends) and observed[k]["finished_ns"] >= min(starts)]
            queued_service = max(0, len(cut_keys) - 1) * interval // 2
            for kind in CLASSES:
                keys = [k for k in expected.keys() & observed.keys() if expected[k]["class"] == kind]
                pre = [observed[k]["finished_ns"] - expected[k]["scheduled_ns"] for k in keys
                       if observed[k]["finished_ns"] < min(starts)]
                cut = [k for k in keys if observed[k]["started_ns"] <= max(ends)
                       and observed[k]["finished_ns"] >= min(starts)]
                if not pre or not cut:
                    incomplete.append("missing_baseline_comparison")
                    continue
                residual = max(cut_residual(expected[k], observed[k], attempt_groups[k], min(starts), max(ends))
                               for k in cut)
                allowed = max(pre) + queued_service
                comparison.append(dict(hop=hop, **{"class": kind}, baseline_max_ns=max(pre),
                                       queued_service_ns=queued_service, cut_residual_max_ns=max(0, residual),
                                       cut_operations=len(cut)))
                if residual > allowed:
                    failures.append("cut_latency_exceeds_calibrated_baseline")
    return verdict(failures, incomplete, slots=len(expected), attempts=sum(map(len, attempt_groups.values())),
                   coverage=coverage, baseline_comparison=comparison)


def ack_judge(witness):
    """Independent ACK prefix, checked before replay can repair a lost row."""
    require(witness.get("schema") == "broker-ack-v1" and witness.get("acks"), "missing_ack_witness")
    wanted = {}
    for ack in witness["acks"]:
        require(isinstance(ack.get("request_id"), str) and bool(ack["request_id"])
                and re.fullmatch(r"[0-9a-f]{64}", ack.get("digest", "")), "invalid_ack")
        require(ack["request_id"] not in wanted, "duplicate_ack")
        wanted[ack["request_id"]] = ack["digest"]
    failures = []
    floor = witness.get("sealed_applied_index")
    require(integer(floor, 1), "invalid_ack_floor")
    for phase in ("before_replay", "after"):
        snapshot = witness[phase]
        require(integer(snapshot.get("applied_index")), "invalid_ack_cursor")
        rows = snapshot["rows"]
        actual = {row["request_id"]: row["digest"] for row in rows}
        if len(actual) != len(rows) or snapshot["applied_index"] < floor or any(actual.get(key) != value for key, value in wanted.items()):
            failures.append("ack_prefix_lost_" + phase)
        floor = max(floor, snapshot["applied_index"])
    return verdict(failures)


def js_judge(witness):
    require(witness.get("schema") == "broker-js-v1", "invalid_js_witness")
    fields = {"server_id", "nuid", "chunks_sha256", "stream_config_sha256", "consumer_seq", "audit_terminal_sha256"}
    for phase in ("before", "after"):
        row = witness[phase]
        require(set(row) == fields and integer(row["consumer_seq"], 1)
                and all(isinstance(row[key], str) and row[key] for key in fields - {"consumer_seq"}), "missing_js_witness")
        require(all(re.fullmatch(r"[0-9a-f]{64}", row[key]) for key in fields if key.endswith("sha256")), "invalid_js_digest")
    return verdict([] if witness["before"] == witness["after"] else ["js_identity_or_history_changed"])


def image_judge(witness):
    require(witness.get("schema") == "broker-image-witness-v1", "invalid_image_witness")
    failures = []
    before, after = witness["before"], witness["after"]
    for row in (before, after):
        process_identity(row["broker"])
        require(re.fullmatch(r"[0-9a-f]{64}", row["broker"].get("running_sha", "")), "invalid_running_sha")
        require(row.get("listeners") and row.get("allocations") and len(row.get("agents", [])) >= 2, "missing_image_identity")
        for agent in row["agents"]:
            process_identity(agent)
            require(re.fullmatch(r"[0-9a-f]{64}", agent.get("running_sha", "")), "invalid_agent_sha")
        for allocation in row["allocations"]:
            require(set(allocation) == {"port", "sid", "nid", "token_hash", "home_broker", "epoch", "state"}
                    and integer(allocation["port"], 1) and integer(allocation["epoch"])
                    and re.fullmatch(r"[0-9a-f]{64}", allocation["token_hash"]), "invalid_full_allocation_identity")
        for listener in row["listeners"]:
            require(all(key in listener for key in ("id", "device", "inode", "cookie"))
                    and integer(listener["device"]) and integer(listener["inode"], 1)
                    and integer(listener["cookie"], 1), "invalid_listener_identity")
    if process_identity(before["broker"]) != process_identity(after["broker"]):
        failures.append("broker_pid_changed")
    if before["broker"]["running_sha"] == after["broker"]["running_sha"]:
        failures.append("running_sha_did_not_change")
    if before["listeners"] != after["listeners"] or before["allocations"] != after["allocations"]:
        failures.append("listener_or_full_allocation_changed")
    if before["agents"] != after["agents"]:
        failures.append("broker_only_changed_agent")
    return verdict(failures)


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# origin: hot-update S6 plan §8.1 / §8.3. These are product phase records,
# never inferred CONNECT attempts. The emitter's message is configured by its
# owner; the collector does not guess frozen intervals from a reload command.
def phase_record(line, message):
    if line.startswith("{"):
        row = decode(line)
    else:
        fields = [field.split("=", 1) for field in shlex.split(line) if "=" in field]
        row = unique_object(fields)
    if row.get("msg") != message:
        return None
    require(isinstance(row.get("cut_id"), str) and isinstance(row.get("event"), str), "phase_fields_missing")
    return row


class PhaseWindows:
    def __init__(self, config):
        require(isinstance(config.get("message"), str) and bool(config["message"])
                and isinstance(config.get("cuts"), dict) and bool(config["cuts"])
                and set(config.get("pairs", {})) == {"freeze", "auth", "control"}, "invalid_phase_binding")
        require(len(set(config["cuts"].values())) == len(config["cuts"])
                and all(isinstance(cut, str) and cut and isinstance(hop, str) and hop for cut, hop in config["cuts"].items()),
                "invalid_phase_cut_inventory")
        for pair in config["pairs"].values():
            require(isinstance(pair, list) and len(pair) == 2 and pair[0] != pair[1]
                    and all(isinstance(event, str) and event for event in pair), "invalid_phase_pair")
        self.config, self.windows, self.seen = config, {}, set()

    def record(self, row, observed_ns):
        require(integer(observed_ns, 1) and row["cut_id"] in self.config["cuts"], "unknown_phase_cut")
        # origin: hot-update S6 internal review rv-ctl-3
        # Transport observation is only an upper bound. Poll latency cannot
        # enlarge a product window or manufacture a fresh in-window request.
        stamp = row.get("boot_ns")
        if isinstance(stamp, str) and stamp.isdecimal():
            stamp = int(stamp)
        require(integer(stamp, 1) and stamp <= observed_ns, "invalid_product_phase_time")
        key = (row["cut_id"], row["event"])
        require(key not in self.seen, "duplicate_phase_record")
        self.seen.add(key)
        hop = self.config["cuts"][row["cut_id"]]
        for phase, (start, end) in self.config["pairs"].items():
            window_key = (hop, phase)
            if row["event"] == start:
                require(window_key not in self.windows, "duplicate_phase_start")
                self.windows[window_key] = dict(hop=hop, phase=phase, started_ns=stamp, finished_ns=None)
            if row["event"] == end:
                require(window_key in self.windows, "phase_end_without_start")
                require(stamp >= self.windows[window_key]["started_ns"], "product_phase_time_reversed")
                self.windows[window_key]["finished_ns"] = stamp

    def incomplete(self):
        return len(self.windows) != len(self.config["cuts"]) * 3 or any(row["finished_ns"] is None for row in self.windows.values())

    def write(self, path):
        staging = path.with_name("." + path.name + ".tmp")
        staging.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in self.windows.values()))
        os.chmod(staging, 0o600)
        staging.replace(path)


def watch(args):
    module = load("flow_fixture")
    transport = module.Transport(args)
    windows = PhaseWindows(read_json(args.config))
    args.root.mkdir(mode=0o700, parents=True, exist_ok=False)
    args.events.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(not args.events.exists(), "phase_events_already_exist")
    raw, meta = args.root / "broker_slog.raw", args.root / "broker_slog.json"
    offset = 0
    try:
        # The raw file/metadata mapping comes from logs.sh through its existing
        # transport. A cursor captures only this watcher's observed generation.
        transport.run("logs", "broker_slog", args.broker, raw, meta)
        before, baseline = read_json(meta), raw.read_bytes()
        module.log_suffix(before, baseline, before, baseline)
        windows.write(args.events)
        deadline = time.monotonic() + args.duration
        while time.monotonic() < deadline:
            transport.run("logs", "broker_slog", args.broker, raw, meta)
            suffix = module.log_suffix(before, baseline, read_json(meta), raw.read_bytes())
            # Never consume a partial record; the next read must retain it.
            complete = suffix.rfind(b"\n") + 1
            for line in suffix[offset:complete].decode().splitlines():
                row = phase_record(line, windows.config["message"])
                if row is not None:
                    windows.record(row, time.clock_gettime_ns(time.CLOCK_BOOTTIME))
                    if args.flow_fixture_root is not None and row["event"] == windows.config["pairs"]["freeze"][0]:
                        mark = module.call(args.flow_fixture_root, ["broker-mark"])
                        require(mark.get("marked") is True and mark.get("rc", 0) == 0, "original_flow_cut_mark_missing")
                    windows.write(args.events)
            offset = complete
            if not windows.incomplete():
                break
            time.sleep(.01)
        result = verdict([], ["missing_phase_window"] if windows.incomplete() else [], windows=list(windows.windows.values()))
        publish(args.root / "phase-watch.json", result)
        return result
    except module.SetupError:
        raise InvalidEvidence("phase_log_read_failed") from None
    finally:
        transport.close()


# origin: hot-update S6 plan §8.1 / T50; rv-ctl-3
# Hermetic real-process consumers use the same phase parser and producer clock
# as live transport. The first actual freeze binds the single expected cut;
# cold-start records and polling latency cannot manufacture a window.
def watch_local(args):
    require(args.hop and args.message and not args.events.exists(), "invalid_local_phase_binding")
    args.events.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    args.events.write_text("")
    os.chmod(args.events, 0o600)
    args.events.with_suffix(".started").write_text("ready")
    offset, windows = args.log_file.read_bytes().rfind(b"\n") + 1, None
    deadline = time.monotonic() + args.duration
    while time.monotonic() < deadline:
        data = args.log_file.read_bytes()
        complete = data.rfind(b"\n") + 1
        for line in data[offset:complete].decode().splitlines():
            row = phase_record(line, args.message)
            if row is None or not row["cut_id"] or row["cut_id"] == "boot":
                continue
            if windows is None:
                require(row["event"] == "freeze_begin", "local_cut_missing_freeze_start")
                windows = PhaseWindows(dict(message=args.message, cuts={row["cut_id"]: args.hop},
                                             pairs=dict(freeze=["freeze_begin", "control_ready"],
                                                        auth=["auth_off", "auth_usable"],
                                                        control=["freeze_begin", "control_ready"])))
            windows.record(row, time.clock_gettime_ns(time.CLOCK_BOOTTIME))
            windows.write(args.events)
        offset = complete
        if windows is not None and not windows.incomplete():
            return verdict([], windows=list(windows.windows.values()))
        time.sleep(.001)
    return verdict([], ["missing_phase_window"])


def capture(args):
    """Use the existing transport/log mapping; never inline remote log paths."""
    module = load("flow_fixture")
    transport = module.Transport(args)
    require(re.fullmatch(r"[a-z][a-z0-9-]{0,60}", args.label), "invalid_checkpoint_label")
    folder = args.root / args.label
    folder.mkdir(mode=0o700, parents=True, exist_ok=False)
    try:
        for stream in ("broker_slog", "broker_panic", "agent_slog", "agent_panic"):
            node = args.broker if stream.startswith("broker") else args.agent
            raw, meta = folder / (stream + ".raw"), folder / (stream + ".json")
            transport.run("logs", stream, node, raw, meta)
            receipt = read_json(meta)
            require(receipt.get("read_ok") is True and receipt.get("length") == raw.stat().st_size, "log_capture_incomplete")
        # The existing peer owns kernel process/FD inspection. Its remote path
        # is supplied explicitly after staging, never guessed from a log name.
        kernel = transport.json("exec", args.broker, "python3", "-I", "-B", args.peer_script,
                                "kernel", "--root", args.peer_root, "--unit", "tether-broker")
        publish(folder / "broker-kernel.json", kernel)
        query = "SELECT port,sid,nid,token_hash,home_broker,epoch,state FROM port_allocations ORDER BY port,sid,nid;"
        allocations = decode(transport.run("exec", args.broker, "runuser", "-u", "tether", "--", "sqlite3", "-json", args.db_path, query))
        require(isinstance(allocations, list) and allocations, "missing_allocation_witness")
        publish(folder / "allocations.json", allocations)
        # Independent JS/ACK witnesses remain owned by their live supplier.
        # Capture exact bytes only after the supplier has produced them.
        failures = []
        for name, source in (("ack", args.ack), ("js", args.js)):
            value = read_json(source)
            publish(folder / (name + "-witness.json"), value)
            failures.extend((ack_judge if name == "ack" else js_judge)(value)["failures"])
    except module.SetupError:
        raise InvalidEvidence("checkpoint_capture_failed") from None
    finally:
        transport.close()
    publish(folder / "capture.json", dict(schema=SCHEMA, complete=True, label=args.label))
    return verdict(failures, capture_complete=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("judge", "bundle-judge"):
        command = commands.add_parser(name)
        command.add_argument("--evidence", type=Path, required=True)
        command.add_argument("--effects", type=Path)
        if name == "bundle-judge":
            flow_input = command.add_mutually_exclusive_group(required=True)
            flow_input.add_argument("--flow-evidence", type=Path)
            flow_input.add_argument("--flow-fixture-root", type=Path)
    command = commands.add_parser("capture")
    for field in ("here", "root", "ack", "js"):
        command.add_argument("--" + field, type=Path, required=True)
    for field in ("instance", "sim", "label", "broker", "agent", "peer-script", "peer-root", "db-path"):
        command.add_argument("--" + field, required=True)
    command = commands.add_parser("watch")
    for field in ("here", "root", "events", "config"):
        command.add_argument("--" + field, type=Path, required=True)
    for field in ("instance", "sim", "broker"):
        command.add_argument("--" + field, required=True)
    command.add_argument("--duration", type=float, required=True)
    command.add_argument("--flow-fixture-root", type=Path)
    command = commands.add_parser("watch-local")
    command.add_argument("--log-file", type=Path, required=True)
    command.add_argument("--events", type=Path, required=True)
    command.add_argument("--hop", required=True)
    command.add_argument("--message", default="broker: image phase")
    command.add_argument("--duration", type=float, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "watch-local":
            require(args.duration > 0 and args.duration <= 4320, "invalid_watch_duration")
            result = watch_local(args)
        elif args.command == "watch":
            require(args.duration > 0 and args.duration <= 4320, "invalid_watch_duration")
            result = watch(args)
        elif args.command == "capture":
            result = capture(args)
        else:
            result = control_judge(args.evidence, args.effects)
            if args.command == "bundle-judge":
                children = [result, ack_judge(read_json(args.evidence / "ack-witness.json")),
                            js_judge(read_json(args.evidence / "js-witness.json")),
                            image_judge(read_json(args.evidence / "image-witness.json"))]
                nonce = read_json(args.evidence / "run.json")["nonce"]
                if args.flow_fixture_root is not None:
                    # SS descriptors live in the remote S5 keeper. Ask that
                    # exact owner to run the shared judge before cleanup;
                    # copying public JSON cannot move sealed FD custody.
                    fixture = load("flow_fixture")
                    require(read_json(args.flow_fixture_root / "fixture.json")["nonce"] == nonce, "foreign_flow_fixture")
                    try:
                        child = fixture.call(args.flow_fixture_root, ["continuity"])
                    except fixture.SetupError:
                        raise InvalidEvidence("flow_keeper_unavailable") from None
                    require(child.get("rc") in (0, 1, 2, 4), "invalid_flow_native_rc")
                    if child["rc"] == 0:
                        require(child.get("flows") and integer(child.get("seal_calls"), 1), "missing_flow_custody_receipt")
                    publish(args.evidence / "flow-judge.json", child)
                    children.append(child)
                else:
                    flow = load("hot_update_flow_probe")
                    try:
                        flow.judge(args.flow_evidence, nonce, "replaced")
                        children.append(verdict([]))
                    except flow.ContinuityError:
                        children.append(verdict(["original_flow_continuity_failed"]))
                    except flow.EvidenceError:
                        raise InvalidEvidence("invalid_flow_evidence") from None
                result = join_children(children)
        print(json.dumps(result, sort_keys=True))
        return result["rc"]
    except InvalidEvidence as exc:
        print(json.dumps(dict(rc=2, verdict="SETUP-RED", criterion=str(exc))))
        return 2
    except (OSError, ValueError, KeyError, TypeError, AttributeError, IndexError, subprocess.SubprocessError):
        # Do not echo opaque private data, malformed JSON, argv or raw errors.
        print('{"rc":2,"verdict":"SETUP-RED","criterion":"invalid_broker_evidence"}')
        return 2


if __name__ == "__main__":
    sys.exit(main())
