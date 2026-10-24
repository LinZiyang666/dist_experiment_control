#!/usr/bin/env python3
"""Hermetic broker collector controls; never invoke a deployed product binary.

Real: collector CLI, pipes, original TCP sockets, process groups and waitpid.
Fake: CLI child replies/attempt sink, target effects and deployment transport.
Not covered: product hot cut, Raft/JS behavior or S6 admission implementation.
origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
"""
import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock


LIB = Path(__file__).resolve().parents[1] / "drills/lib"
NONCE = "abcdef0123456789abcdef0123456789"


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


broker = load(LIB / "hot_update_broker_probe.py", "broker_probe")
wrapper = load(LIB / "hot_update_flow_fixture.py", "fixture_wrapper")


def rows(path, values):
    path.write_text("".join(json.dumps(value, sort_keys=True) + "\n" for value in values))


def complete_control(root):
    """An external witness generator, not the collector's scheduling code."""
    origin, total = 1_000_000_000, 240
    metadata = dict(schema="broker-probe-v1", nonce=NONCE, rate=10, request_ns=5_000_000_000,
                    started_ns=origin, ended_ns=origin + 29_000_000_000, expected_slots=total,
                    scheduling_ended_ns=origin + 24_000_000_000, late_tail_ns=5_000_000_000,
                    complete=True, late_tail_complete=True, hops=["one"])
    slots = [dict(logical_id=f"{NONCE}-{seq}", slot=seq, **{"class": ("read", "write", "short")[seq % 3]},
                  scheduled_ns=origin + seq * 100_000_000, targeted=False) for seq in range(total)]
    windows = [dict(hop="one", phase=phase, started_ns=origin + 12_000_000_000 + index * 100_000_000,
                    finished_ns=origin + 12_500_000_000 + index * 100_000_000)
               for index, phase in enumerate(("freeze", "auth", "control"))]
    for index, window in enumerate(windows):
        for kind in ("read", "write", "short"):
            slots.append(dict(logical_id=f"{NONCE}-target-{index}-{kind}", **{"class": kind},
                              targeted=True, scheduled_ns=window["started_ns"] + 2_000_000))
    results, attempts, effects = [], [], []
    for index, slot in enumerate(slots):
        start = slot["scheduled_ns"] + 1_000_000
        end = start + 10_000_000
        result = dict(logical_id=slot["logical_id"], started_ns=start, finished_ns=end, rc=0)
        if slot["class"] == "read":
            result["validated_read"] = True
        else:
            identity = dict(pid=500 + index, start_ticks=77 + index, boot_id="independent-target-boot")
            result["process"] = dict(identity, true_started_ns=start + 9_000_000)
            effects.append(dict(identity, logical_id=slot["logical_id"], at_ns=98_000 + index))
        results.append(result)
        for ordinal, phase in enumerate(("connect", "backoff", "connect", "request" if slot["class"] == "read" else "create", "result"), 1):
            attempts.append(dict(logical_id=slot["logical_id"], ordinal=ordinal, phase=phase,
                                 started_mono_ns=start + ordinal * 100, finished_mono_ns=start + ordinal * 100 + 50,
                                 sent=phase in ("request", "create"), **{"class": "auth" if ordinal == 1 else "ok"},
                                 backoff_ns=50 if phase == "backoff" else 0))
        attempts.append(dict(kind="observer_close", logical_id=slot["logical_id"], attempt_count=5, overflow=False))
    broker.publish(root / "run.json", metadata)
    for name, value in (("slots", slots), ("results", results), ("attempts", attempts), ("windows", windows), ("effects", effects)):
        rows(root / (name + ".jsonl"), value)
    return dict(slots=slots, results=results, attempts=attempts, windows=windows, effects=effects)


def witnesses(root):
    digest = hashlib.sha256(b"independent-ACK-bytes").hexdigest()
    ack = dict(schema="broker-ack-v1", sealed_applied_index=12, acks=[dict(request_id="ACK-one", digest=digest)],
               before_replay=dict(applied_index=12, rows=[dict(request_id="ACK-one", digest=digest)]),
               after=dict(applied_index=14, rows=[dict(request_id="ACK-one", digest=digest)]))
    js_row = dict(server_id="external-server-one", nuid="object-not-its-name", chunks_sha256="a" * 64,
                  stream_config_sha256="b" * 64, consumer_seq=24, audit_terminal_sha256="c" * 64)
    js = dict(schema="broker-js-v1", before=js_row, after=copy.deepcopy(js_row))
    before = dict(broker=dict(pid=90, start_ticks=4, boot_id="broker-boot", running_sha="a" * 64),
                  agents=[dict(pid=91 + n, start_ticks=5 + n, boot_id="agent-boot", running_sha="b" * 64) for n in range(2)],
                  listeners=[dict(id="public-one", device=0, inode=8, cookie=10)],
                  allocations=[dict(port=14000, sid="session-one", nid="agent-one", token_hash="c" * 64,
                                    home_broker="broker-one", epoch=7, state="ALLOCATED")])
    after = copy.deepcopy(before)
    after["broker"]["running_sha"] = "d" * 64
    image = dict(schema="broker-image-witness-v1", before=before, after=after)
    for name, value in (("ack", ack), ("js", js), ("image", image)):
        broker.publish(root / (name + "-witness.json"), value)
    return ack, js, image


class BrokerCollectorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="broker-collector-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.data = complete_control(self.root)

    def save(self, name):
        rows(self.root / (name + ".jsonl"), self.data[name])

    def invoke(self, bundle=None):
        args = ["judge", "--evidence", str(self.root)]
        if bundle:
            args = ["bundle-judge", "--evidence", str(self.root), "--flow-evidence", str(bundle)]
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = broker.main(args)
        return rc, json.loads(out.getvalue())

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_complete_slots_attempts_effects_pass(self):
        rc, result = self.invoke()
        self.assertEqual((rc, result["verdict"]), (0, "GREEN"))
        self.assertEqual(result["slots"], 249)
        self.assertEqual(result["attempts"], 249 * 5)
        self.assertTrue(all(row["fresh"] for row in result["coverage"]))

    # origin: hot-update S6 plan §8.3 / T50 / FIX-BLACKOUT-2.
    def calibration_fixture(self):
        # Independent external witness, deliberately much slower in the second
        # half. Choosing only the faster half must not set the offered rate.
        kept = {f"{NONCE}-{n}" for n in range(36)}
        for name in ("slots", "results", "attempts", "effects"):
            self.data[name] = [row for row in self.data[name] if row["logical_id"] in kept]
        batches, stamp = [], 1_000_000_000
        for batch in range(12):
            finish = stamp + (600_000_000 if batch < 6 else 900_000_000)
            batches.append(dict(started_ns=stamp, finished_ns=finish))
            for n in range(batch * 3, batch * 3 + 3):
                slot = self.data["slots"][n]
                old = slot["scheduled_ns"]
                slot["scheduled_ns"] = stamp
                delta = stamp - old
                result = self.data["results"][n]
                for field in ("started_ns", "finished_ns"):
                    result[field] += delta
                if "process" in result:
                    result["process"]["true_started_ns"] += delta
                for attempt in self.data["attempts"]:
                    if attempt["logical_id"] == slot["logical_id"] and "kind" not in attempt:
                        attempt["started_mono_ns"] += delta
                        attempt["finished_mono_ns"] += delta
            stamp = finish
        self.data["windows"] = []
        for name in self.data:
            self.save(name)
        metadata = broker.read_json(self.root / "run.json")
        metadata.update(profile="calibration", batches=batches, expected_slots=36,
                        scheduling_ended_ns=stamp, ended_ns=stamp + 5_000_000_000)
        broker.publish(self.root / "run.json", metadata)
        return metadata

    def calibrated_fixture(self):
        receipt = dict(schema="broker-calibration-v1", fraction=[1, 2], interval_ns=100_000_000,
                       batches=[dict(started_ns=1 + n * 150_000_000, finished_ns=1 + (n + 1) * 150_000_000)
                                for n in range(12)])
        metadata = broker.read_json(self.root / "run.json")
        metadata.update(profile="calibrated", interval_ns=100_000_000, calibration=receipt)
        broker.publish(self.root / "run.json", metadata)
        return metadata

    def test_cut_comparison_counts_recovery_once_and_keeps_late_stalls(self):
        slot, result = dict(scheduled_ns=1), dict(finished_ns=401)
        failed = dict(phase="connect", **{"class": "auth"}, started_mono_ns=21, finished_mono_ns=201, backoff_ns=0)
        backoff = dict(phase="backoff", **{"class": "ok"}, started_mono_ns=221, finished_mono_ns=251, backoff_ns=30)
        self.assertEqual(broker.cut_residual(slot, result, [failed, backoff], 11, 101), 180)
        self.assertEqual(broker.cut_residual(slot, result, [dict(failed, **{"class": "terminal"}), backoff], 11, 101), 180)
        # Success, or a failed attempt first started after readiness, cannot
        # excuse a stall. Nested observations cannot subtract recovery twice.
        self.assertEqual(broker.cut_residual(slot, result, [dict(failed, **{"class": "ok"}), backoff], 11, 101), 280)
        self.assertEqual(broker.cut_residual(slot, result, [dict(failed, started_mono_ns=151), backoff], 11, 101), 280)
        self.assertEqual(broker.cut_residual(slot, result, [failed, failed, backoff], 11, 101), 180)

    def test_calibration_uses_slower_half_and_fixed_fraction(self):
        self.calibration_fixture()
        rc, result = self.invoke()
        self.assertEqual(rc, 0, result)
        self.assertEqual(result["slots"], 36)
        self.assertEqual(result["calibration"]["interval_ns"], 600_000_000)
        self.assertEqual(result["calibration"]["fraction"], [1, 2])
        self.assertEqual(broker.read_json(self.root / "calibration.json"), result["calibration"])
        for field, value in (("fraction", [1, 1]), ("interval_ns", 300_000_000), ("batches", [])):
            with self.subTest(field=field), self.assertRaises(broker.InvalidEvidence):
                broker.calibrated_interval(dict(result["calibration"], **{field: value}))

    def test_calibration_keeps_failed_missing_and_unjoined_operations(self):
        self.calibration_fixture()
        self.data["results"][0]["rc"] = 75
        self.save("results")
        rc, result = self.invoke()
        self.assertEqual(rc, 1, result)
        self.assertIsNone(result["calibration"])
        self.assertFalse((self.root / "calibration.json").exists())
        self.data["results"][0]["rc"] = 0
        self.data["results"][0]["finished_ns"] += 700_000_000
        self.save("results")
        rc, result = self.invoke()
        self.assertEqual(rc, 1, result)
        self.assertIn("calibration_batch_not_joined", result["failures"])
        self.data["results"].pop()
        self.save("results")
        self.assertIn("missing_or_unrequested_result", self.invoke()[1]["failures"])

    def test_calibrated_slots_keep_absolute_denominator(self):
        metadata = self.calibrated_fixture()
        self.assertEqual(self.invoke()[0], 0)
        metadata["calibration"]["interval_ns"] = 200_000_000
        broker.publish(self.root / "run.json", metadata)
        self.assertEqual(self.invoke()[0], 2)
        metadata = self.calibrated_fixture()
        self.data["slots"][0]["scheduled_ns"] += 1
        self.save("slots")
        self.assertIn("nonabsolute_slot", self.invoke()[1]["failures"])
        metadata["expected_slots"] -= 1
        broker.publish(self.root / "run.json", metadata)
        self.assertIn("schedule_denominator_changed", self.invoke()[1]["failures"])

    def test_calibrated_cut_accounts_for_fresh_probe_queue(self):
        self.calibrated_fixture()
        for result in self.data["results"]:
            if "target" in result["logical_id"]:
                result["finished_ns"] += 900_000_000
        self.save("results")
        rc, result = self.invoke()
        self.assertEqual(rc, 0, result)
        self.assertTrue(all(row["queued_service_ns"] > 0 for row in result["baseline_comparison"]))

    def test_calibrated_cut_compares_latency_with_pre_cut_baseline(self):
        self.calibrated_fixture()
        rc, result = self.invoke()
        self.assertEqual(rc, 0, result)
        self.assertEqual(len(result["baseline_comparison"]), 3)
        # Each delayed operation still meets the absolute five-second budget;
        # the independent comparison must detect its unexplained cut stall.
        for result in self.data["results"]:
            if "target" in result["logical_id"]:
                result["finished_ns"] += 3_000_000_000
        self.save("results")
        rc, result = self.invoke()
        self.assertEqual(rc, 1, result)
        self.assertEqual(result["failures"], ["cut_latency_exceeds_calibrated_baseline"])

    # origin: hot-update S6 plan §8.3 / T50: no serial or lossy denominator.
    def test_missing_slot_is_sticky_failure(self):
        lost = self.data["slots"].pop(15)["logical_id"]
        for name in ("results", "attempts", "effects"):
            self.data[name] = [row for row in self.data[name] if row["logical_id"] != lost]
        for name in ("slots", "results", "attempts", "effects"):
            self.save(name)
        rc, result = self.invoke()
        self.assertEqual(rc, 1)
        self.assertIn("missing_slot", result["failures"])

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_shrinking_schedule_denominator_is_failure(self):
        metadata = broker.read_json(self.root / "run.json")
        metadata["expected_slots"] -= 1
        broker.publish(self.root / "run.json", metadata)
        rc, result = self.invoke()
        self.assertEqual(rc, 1)
        self.assertIn("schedule_denominator_changed", result["failures"])

    # origin: hot-update S6 plan §8.3 / §10.2 / §12.1 P29.
    def test_declared_slot_overflow_is_rejected_before_allocation(self):
        metadata = broker.read_json(self.root / "run.json")
        metadata["expected_slots"] = 43_201
        broker.publish(self.root / "run.json", metadata)
        self.assertEqual(self.invoke()[0], 2)

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_scheduler_delay_counts_against_slot_budget(self):
        result = self.data["results"][0]
        result["started_ns"] += 5_000_000_000
        result["finished_ns"] += 5_000_000_000
        key = result["logical_id"]
        for attempt in self.data["attempts"]:
            if attempt["logical_id"] == key and "ordinal" in attempt:
                attempt["started_mono_ns"] += 5_000_000_000
                attempt["finished_mono_ns"] += 5_000_000_000
        self.save("results")
        self.save("attempts")
        rc, result = self.invoke()
        self.assertEqual(rc, 1)
        self.assertIn("slot_budget_exceeded", result["failures"])

    # origin: hot-update S6 plan §8.3 / T35. Failed connects remain after success.
    def test_missing_attempt_before_success_is_failure(self):
        self.data["attempts"].pop(0)
        self.save("attempts")
        rc, result = self.invoke()
        self.assertEqual(rc, 1)
        self.assertIn("attempt_count_or_ordinal_loss", result["failures"])

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_missing_attempt_tail_is_failure(self):
        self.data["attempts"].pop(4)
        self.save("attempts")
        rc, result = self.invoke()
        self.assertEqual(rc, 1)
        self.assertIn("missing_connect_or_terminal_attempt", result["failures"])

    # origin: hot-update S6 plan §8.2 / §8.3 / §12.1 P29.
    def test_probe_only_or_legacy_result_is_not_reliable_admission(self):
        for index in (0, 1):
            original = complete_control(self.root)
            self.data = original
            key = self.data["results"][index]["logical_id"]
            for row in self.data["attempts"]:
                if row["logical_id"] == key and row.get("phase") in ("request", "create"):
                    row["phase"], row["sent"] = "probe", False
            self.save("attempts")
            rc, result = self.invoke()
            self.assertEqual(rc, 1)
            self.assertIn("read_or_reliable_admission_not_sent", result["failures"])

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_attempt_completion_order_can_differ_from_begin_order(self):
        self.data["attempts"].reverse()
        self.save("attempts")
        self.assertEqual(self.invoke()[0], 0)

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_attempt_overflow_and_unknown_phase_fail(self):
        self.data["attempts"][5]["overflow"] = True
        self.save("attempts")
        self.assertEqual(self.invoke()[0], 1)
        self.data["attempts"][0]["phase"] = "invented-phase"
        self.save("attempts")
        self.assertEqual(self.invoke()[0], 2)
        # origin: hot-update S6 internal review rv-ctl-8.
        self.data["attempts"][0]["phase"] = "connect"
        self.data["attempts"][0]["class"] = "invented-class"
        self.save("attempts")
        self.assertEqual(self.invoke()[0], 2)

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_accepted_is_not_true_process_started(self):
        self.data["results"][1]["process"] = dict(accepted=True)
        self.save("results")
        rc, result = self.invoke()
        self.assertEqual(rc, 1)
        self.assertIn("true_started_missing", result["failures"])

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_late_second_effect_is_sticky_failure(self):
        effect = dict(self.data["effects"][0], pid=90000, start_ticks=99, at_ns=999999999999)
        self.data["effects"].append(effect)
        self.save("effects")
        rc, result = self.invoke()
        self.assertEqual(rc, 1)
        self.assertIn("duplicate_missing_or_changed_effect", result["failures"])

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_failed_request_effect_and_missing_result_are_failures(self):
        self.data["results"][1]["rc"] = 74
        self.save("results")
        self.assertEqual(self.invoke()[0], 1)
        self.data["results"].pop(1)
        self.save("results")
        self.assertEqual(self.invoke()[0], 1)

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_inflight_cannot_replace_fresh_window_start(self):
        for row in self.data["windows"]:
            row["started_ns"] = 13_004_000_000
            row["finished_ns"] = 13_004_000_001
        self.save("windows")
        rc, result = self.invoke()
        self.assertEqual((rc, result["verdict"]), (4, "INCOMPLETE"))
        self.assertIn("missing_fresh_window_start", result["incomplete"])

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_missing_phase_and_pre_post_cohort_are_incomplete(self):
        self.data["windows"].pop()
        self.save("windows")
        self.assertEqual(self.invoke()[0], 4)
        self.data = complete_control(self.root)
        for row in self.data["windows"]:
            row["started_ns"] = 1_000_000_000
        self.save("windows")
        rc, result = self.invoke()
        self.assertEqual(rc, 4)
        self.assertIn("missing_pre_post_cohort", result["incomplete"])

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_missing_file_truncation_and_bad_json_are_setup_red(self):
        path = self.root / "attempts.jsonl"
        path.write_bytes(path.read_bytes()[:-1])
        self.assertEqual(self.invoke()[0], 2)
        path.unlink()
        self.assertEqual(self.invoke()[0], 2)
        path.write_text('{"logical_id":"private","logical_id":"other"}\n')
        rc, result = self.invoke()
        self.assertEqual(rc, 2)
        self.assertNotIn("private", json.dumps(result))

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_unjoined_collector_and_short_late_tail_are_setup_red(self):
        original = broker.read_json(self.root / "run.json")
        for field, value in (("complete", False), ("late_tail_complete", False), ("late_tail_ns", 1)):
            broker.publish(self.root / "run.json", dict(original, **{field: value}))
            self.assertEqual(self.invoke()[0], 2, field)

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_read_validation_requires_protocol_result(self):
        self.assertTrue(broker.validated_read(dict(nodes=[dict(nid="agt1", status="ONLINE")]), "nodes"))
        self.assertTrue(broker.validated_read(dict(processes=[]), "processes"))
        for value in ({}, dict(nodes=[{}]), dict(nodes=[]), dict(nodes=[dict(nid="agt1", status="ONLINE")], error="refused")):
            self.assertFalse(broker.validated_read(value, "nodes"))

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_frozen_go_attempt_shape_and_shared_budgets_match(self):
        import re
        contracts = LIB.parents[3] / "internal/controlbudget/contracts.go"
        source = contracts.read_text().split("type Attempt struct {", 1)[1].split("}", 1)[0]
        self.assertEqual(set(re.findall(r'json:"([a-z_]+)"', source)), broker.ATTEMPT_FIELDS)
        mixed = load(LIB / "hot_update_mixed_probe.py", "mixed_budgets")
        rate, request_s = mixed.budgets()
        self.assertEqual((rate, int(request_s * 1_000_000_000)), (10, broker.REQUEST_NS))

    # origin: hot-update S6 internal review rv-ctl-13.
    def test_generated_commands_use_production_bindings_and_distinct_effects(self):
        mixed = load(LIB / "hot_update_mixed_probe.py", "mixed_command_contract")
        output = self.root / "commands.json"
        effects = self.root / "effects-produced.jsonl"
        self.assertEqual(mixed.broker_run_main(broker, ["broker-config", "--output", str(output),
                         "--binary", str(self.root / "sandbox-tether"), "--node", "node", "--effects-path", str(effects)]), 0)
        config = broker.read_json(output)
        self.assertEqual(config["attempt_fd_env"], "TETHER_ATTEMPT_RECEIPT_FD")
        self.assertEqual(config["logical_id_env"], "TETHER_ATTEMPT_LOGICAL_ID")
        self.assertNotEqual(config["commands"]["write"], config["commands"]["short"])
        for kind in ("write", "short"):
            command = [sys.executable] + [arg.replace("{logical_id}", NONCE + "-" + kind)
                                         for arg in config["commands"][kind][5:]]
            result = subprocess.run(command, capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            started = json.loads(result.stdout)
            self.assertEqual(started["kind"], "broker-probe-started-v1")
            self.assertFalse(Path(f'/proc/{started["pid"]}').exists())
        self.assertEqual(Path(str(effects) + ".stable-writes").read_text(), NONCE + "-write\n")
        self.assertEqual(len(broker.read_rows(effects)), 2)

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_phase_parser_and_windows_keep_actual_start_boundaries(self):
        config = dict(message="broker: image phase", cuts=dict(cut1="one"),
                      pairs=dict(freeze=["freeze_begin", "control_ready"], auth=["auth_off", "auth_usable"],
                                 control=["freeze_begin", "control_ready"]))
        windows = broker.PhaseWindows(config)
        for index, event in enumerate(("freeze_begin", "auth_off", "auth_usable", "control_ready")):
            row = broker.phase_record(f'time=x level=INFO msg="broker: image phase" cut_id=cut1 event={event} boot_ns={100 + index}', config["message"])
            windows.record(row, 300_000_100 + index)
        self.assertFalse(windows.incomplete())
        self.assertEqual(windows.windows[("one", "auth")]["started_ns"], 101)
        self.assertEqual(windows.windows[("one", "auth")]["finished_ns"], 102)
        # A request at poll-observation time starts after the actual blackout.
        self.assertFalse(101 <= 300_000_101 <= windows.windows[("one", "auth")]["finished_ns"])
        with self.assertRaises(broker.InvalidEvidence):
            windows.record(dict(cut_id="cut1", event="control_ready"), 110)
        with self.assertRaises(broker.InvalidEvidence):
            broker.PhaseWindows(config).record(dict(cut_id="cut1", event="auth_usable"), 111)
        self.assertIsNone(broker.phase_record('{"msg":"unrelated"}', config["message"]))

    # origin: hot-update S6 internal review rv-ctl-3 / T50
    def test_product_phase_clock_rejects_missing_future_and_reversed_stamps(self):
        config = dict(message="broker: image phase", cuts=dict(cut1="one"),
                      pairs=dict(freeze=["freeze_begin", "control_ready"],
                                 auth=["auth_off", "auth_usable"],
                                 control=["freeze_begin", "control_ready"]))
        for stamp in (None, 0, -1, True, 1.5, "1.5", "", 301):
            with self.subTest(stamp=stamp), self.assertRaises(broker.InvalidEvidence):
                broker.PhaseWindows(config).record(dict(cut_id="cut1", event="freeze_begin", boot_ns=stamp), 300)
        windows = broker.PhaseWindows(config)
        windows.record(dict(cut_id="cut1", event="auth_off", boot_ns=200), 300)
        with self.assertRaises(broker.InvalidEvidence):
            windows.record(dict(cut_id="cut1", event="auth_usable", boot_ns=199), 600)

    # origin: hot-update S6 plan §8.1 / T50
    def test_local_phase_watcher_preserves_delayed_product_clock(self):
        log, events = self.root / "product.log", self.root / "phase-events.jsonl"
        log.write_text(json.dumps(dict(msg="broker: image phase", cut_id="boot", event="control_ready", boot_ns=1)) + "\n")
        args = SimpleNamespace(log_file=log, events=events, hop="one", message="broker: image phase", duration=2)
        stamp = time.clock_gettime_ns(time.CLOCK_BOOTTIME) - 300_000_000
        def producer():
            deadline = time.monotonic() + 2
            while not events.with_suffix(".started").exists() and time.monotonic() < deadline:
                time.sleep(.001)
            with log.open("a") as out:
                out.write(json.dumps(dict(msg=args.message, cut_id="boot", event="auth_usable", boot_ns=stamp-1)) + "\n")
                for index, event in enumerate(("freeze_begin", "auth_off", "auth_usable", "control_ready")):
                    out.write(json.dumps(dict(msg=args.message, cut_id="cut1", event=event, boot_ns=stamp+index)) + "\n")
        worker = threading.Thread(target=producer)
        worker.start()
        try:
            result = broker.watch_local(args)
        finally:
            worker.join()
        self.assertEqual(result["rc"], 0)
        auth = next(row for row in broker.read_rows(events) if row["phase"] == "auth")
        self.assertEqual((auth["started_ns"], auth["finished_ns"]), (stamp+1, stamp+2))
        self.assertLess(auth["finished_ns"], time.clock_gettime_ns(time.CLOCK_BOOTTIME)-200_000_000)

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_capture_reuses_four_stream_mapping_and_closes_transport(self):
        ack, js, _ = witnesses(self.root)
        calls = []

        class Transport:
            closed = False

            def run(inner, *args):
                calls.append(args)
                if args[0] == "logs":
                    Path(args[3]).write_bytes(b"original mapped log\n")
                    broker.publish(Path(args[4]), dict(read_ok=True, inode=1, length=20))
                    return b""
                return json.dumps([dict(port=14000, sid="s", nid="n", token_hash="a" * 64,
                                        home_broker="b", epoch=1, state="ALLOCATED")]).encode()

            def json(inner, *args):
                return dict(identity=dict(pid=10, start_ticks=9, boot_id="test"), fds=[])

            def close(inner):
                inner.closed = True

        transport = Transport()
        args = SimpleNamespace(root=self.root / "capture", label="before", here=LIB.parents[1], sim="/unused", instance="hermetic",
                               broker="brk1", agent="agt1", peer_script="/supplied/peer.py", peer_root="/supplied/root",
                               db_path="/supplied/db", ack=self.root / "ack-witness.json", js=self.root / "js-witness.json")
        with mock.patch.object(broker, "load", return_value=SimpleNamespace(Transport=lambda _: transport)):
            self.assertEqual(broker.capture(args)["rc"], 0)
        self.assertTrue(transport.closed)
        self.assertEqual([args[1] for args in calls if args[0] == "logs"], ["broker_slog", "broker_panic", "agent_slog", "agent_panic"])
        self.assertTrue((args.root / "before/capture.json").exists())

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_phase_watch_reads_actual_log_suffix_and_closes_transport(self):
        config = dict(message="broker: image phase", cuts=dict(cut1="one"),
                      pairs=dict(freeze=["freeze_begin", "control_ready"], auth=["auth_off", "auth_usable"],
                                 control=["freeze_begin", "control_ready"]))
        broker.publish(self.root / "watch-config.json", config)
        fixture = wrapper.implementation()

        class Transport:
            closed = False
            count = 0

            def run(inner, *args):
                raw = b"" if not inner.count else b"".join(json.dumps(dict(msg=config["message"], cut_id="cut1", event=event, boot_ns=time.clock_gettime_ns(time.CLOCK_BOOTTIME))).encode() + b"\n"
                                                        for event in ("freeze_begin", "auth_off", "auth_usable", "control_ready"))
                inner.count += 1
                Path(args[3]).write_bytes(raw)
                broker.publish(Path(args[4]), dict(read_ok=True, inode=1, length=len(raw)))

            def close(inner):
                inner.closed = True

        transport = Transport()
        args = SimpleNamespace(root=self.root / "watch", events=self.root / "events.jsonl", duration=.1, broker="brk1", flow_fixture_root=None,
                               config=self.root / "watch-config.json")
        with mock.patch.object(broker, "load", return_value=SimpleNamespace(Transport=lambda _: transport, log_suffix=fixture.log_suffix)):
            result = broker.watch(args)
        self.assertEqual(result["rc"], 0)
        self.assertTrue(transport.closed)
        self.assertEqual(len(broker.read_rows(args.events)), 3)

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_pre_replay_ack_loss_cannot_be_repaired_by_replay(self):
        ack, _, _ = witnesses(self.root)
        self.assertEqual(broker.ack_judge(ack)["rc"], 0)
        ack["before_replay"]["rows"] = []
        self.assertEqual(broker.ack_judge(ack)["rc"], 1)

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_js_recreation_and_history_reset_are_failures(self):
        _, js, _ = witnesses(self.root)
        self.assertEqual(broker.js_judge(js)["rc"], 0)
        for field in ("nuid", "consumer_seq", "chunks_sha256", "stream_config_sha256", "audit_terminal_sha256", "server_id"):
            mutant = copy.deepcopy(js)
            mutant["after"][field] = 1 if field == "consumer_seq" else "d" * 64
            self.assertEqual(broker.js_judge(mutant)["rc"], 1, field)

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_original_listener_allocation_and_broker_only_agent_identity(self):
        _, _, image = witnesses(self.root)
        self.assertEqual(broker.image_judge(image)["rc"], 0)
        for section, field in (("listeners", "cookie"), ("allocations", "token_hash"), ("agents", "running_sha")):
            mutant = copy.deepcopy(image)
            mutant["after"][section][0][field] = 11 if field == "cookie" else "f" * 64
            self.assertEqual(broker.image_judge(mutant)["rc"], 1)
        image["after"]["broker"]["running_sha"] = image["before"]["broker"]["running_sha"]
        self.assertEqual(broker.image_judge(image)["rc"], 1)

    def flow(self):
        # Reuse the independent S5 test fixture rather than a second byte/SS
        # decoder. Its sealed captures stay alive through this judge's call.
        module = load(Path(__file__).with_name("hot-update-flow-probe-test.py"), "flow_controls")
        case = module.FlowProbeTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.save()
        witnesses(self.root)
        return case

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_target_accept_identity_uses_shared_original_socket_oracle(self):
        flow = self.flow()
        self.assertEqual(self.invoke(flow.root)[0], 0)
        flow.document["flows"][0]["accept_count"] = 2
        flow.save()
        rc, result = self.invoke(flow.root)
        self.assertEqual(rc, 1)
        self.assertIn("original_flow_continuity_failed", result["failures"])

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_truncated_bytes_use_shared_original_socket_oracle(self):
        flow = self.flow()
        self.assertEqual(self.invoke(flow.root)[0], 0)
        path = flow.root / "down.received"
        path.write_bytes(path.read_bytes()[:-1])
        self.assertEqual(self.invoke(flow.root)[0], 1)

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_sealed_ss_custody_uses_shared_crypto_oracle(self):
        flow = self.flow()
        flow.add_ss()
        flow.save()
        self.assertEqual(self.invoke(flow.root)[0], 0)
        flow.document["flows"][0]["ss"]["key_fd"] = -1
        flow.save()
        self.assertEqual(self.invoke(flow.root)[0], 2)

    # origin: hot-update S6 plan §10.1 / §10.2 / §12.1 P29.
    def test_live_flow_keeper_preserves_native_custody_verdict(self):
        witnesses(self.root)
        root = self.root / "live-fixture"
        root.mkdir()
        broker.publish(root / "fixture.json", dict(nonce=NONCE))
        for native in (0, 1, 2, 4):
            child = dict(rc=native, verdict={0: "GREEN", 1: "ASSERT-FAIL", 2: "SETUP-RED", 4: "INCOMPLETE"}[native], flows=[{}], seal_calls=12)
            factory = SimpleNamespace(call=mock.Mock(return_value=child))
            with mock.patch.object(broker, "load", return_value=factory), contextlib.redirect_stdout(io.StringIO()):
                rc = broker.main(["bundle-judge", "--evidence", str(self.root), "--flow-fixture-root", str(root)])
            self.assertEqual(rc, native)
            factory.call.assert_called_once_with(root, ["continuity"])
        self.assertEqual(broker.join_children([dict(rc=1), dict(rc=2)])["rc"], 1)

    # origin: hot-update S6 plan §10.1 / §12.1 P29.
    def test_broker_profile_checkpoint_keeps_agent_and_allocation_identity(self):
        real = wrapper.implementation()
        agent = dict(identity=dict(pid=100, start_ticks=7, boot_id="agent"), running_sha="a" * 64)
        source = dict(identity=dict(pid=90, start_ticks=9, boot_id="broker"), running_sha="b" * 64)

        class Base:
            def checkpoint(inner, label):
                inner.snapshots[label] = dict(agent=copy.deepcopy(agent), sql=[dict(port=9, epoch=1)])
                return dict(checkpoint=label)

        fixture = wrapper.broker_fixture_class(SimpleNamespace(Fixture=Base, safe_label=real.safe_label, require=real.require, peer=real.peer))()
        fixture.root, fixture.snapshots, fixture.transport = self.root, {}, SimpleNamespace(json=lambda *args: source)
        fixture.checkpoint("before")
        self.assertEqual(fixture.checkpoint("hop-1")["checkpoint"], "hop-1")
        agent["running_sha"] = "c" * 64
        with self.assertRaises(real.SetupError):
            fixture.checkpoint("hop-2")

    # origin: hot-update S6 plan §10.1 / §12.1 P29.
    def test_broker_profile_declares_idle_slow_before_business(self):
        real = wrapper.implementation()

        class Base:
            def prepare(inner):
                inner.specs = [dict(id=f"tcp-{n}", protocol="raw", **{"class": "normal"}, declared_at=1, size=128,
                                    up=dict(records=20), down=dict(records=20), key="private-key") for n in (1, 2)]
                return dict(prepared=True)

        fixture = wrapper.broker_fixture_class(SimpleNamespace(Fixture=Base, peer=real.peer))()
        fixture.root, fixture.nonce, fixture.remote_call = self.root, NONCE, mock.Mock()
        self.assertTrue(fixture.prepare()["prepared"])
        self.assertEqual(fixture.specs[0]["class"], "idle")
        self.assertEqual(fixture.specs[0]["up"]["records"], 0)
        self.assertEqual(fixture.specs[1]["class"], "slow")
        self.assertNotIn("private-key", (self.root / "declarations.json").read_text())

    # origin: hot-update S6 plan §10.1 / §12.1 P29.
    def test_broker_baseline_requires_idle_and_half_close_prior(self):
        real = wrapper.implementation()
        fixture = wrapper.broker_fixture_class(SimpleNamespace(Fixture=object, require=real.require))()
        fixture.specs = [dict(id="idle", **{"class": "idle"}), dict(id="half", **{"class": "half-close"})]
        document = dict(flows=dict(idle=dict(sent=[], received=[], events=[]), half=dict(sent=[{}] * 20, received=[{}] * 20, events=[dict(kind="eof")])))
        fixture.phase, fixture.remote_call = "running", mock.Mock(return_value=document)
        fixture.request_mark, fixture.checkpoint = mock.Mock(), mock.Mock(return_value=dict(checkpoint="baseline"))
        self.assertEqual(fixture.baseline()["checkpoint"], "baseline")
        for section, replacement in (("idle", dict(sent=[{}], received=[], events=[])), ("half", dict(sent=[{}] * 20, received=[{}] * 20, events=[]))):
            broken = copy.deepcopy(document)
            broken["flows"][section] = replacement
            fixture.remote_call.return_value = broken
            with mock.patch.object(wrapper.time, "monotonic", side_effect=[0, 21]), self.assertRaises(real.SetupError):
                fixture.baseline()

    # origin: hot-update S6 plan §10.1 / §12.1 P29.
    def test_idle_peer_finishes_without_closing_original_receive_owner(self):
        class Base:
            def __init__(inner, kind):
                inner.spec, inner.start, inner.end, inner.done, inner.worker_live = {"class": kind}, 1, None, threading.Event(), False

            def run(inner):
                inner.worker_live = True

        module = SimpleNamespace(Flow=Base)

        def run():
            for kind in ("idle", "normal"):
                flow = module.Flow(kind)
                flow.run()
                self.assertTrue(flow.worker_live)
                self.assertEqual(flow.done.is_set(), kind == "idle")
                self.assertEqual(flow.end, 1 if kind == "idle" else None)
            return 0

        module.main = run
        with mock.patch.object(wrapper, "load", return_value=module):
            self.assertEqual(wrapper.peer_main(["serve"]), 0)

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_callable_wrapper_and_missing_fixture_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            isolated = Path(directory) / "hot_update_flow_fixture.py"
            isolated.write_bytes((LIB / "hot_update_flow_fixture.py").read_bytes())
            result = subprocess.run([sys.executable, "-I", "-B", str(isolated), "selftest"], capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(json.loads(result.stdout)["verdict"], "SETUP-RED")
        result = subprocess.run([str(LIB / "flow_fixture"), "call", "--root", str(self.root), "unknown"],
                                capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 2)

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_sourced_wrapper_owns_and_waits_main_shell_job(self):
        body = (LIB / "flow_fixture").read_text()
        self.assertIn('FLOW_FIXTURE_JOB=$!', body)
        self.assertIn('wait "$FLOW_FIXTURE_JOB"', body)
        self.assertIn('"$HERE/drills/lib/hot_update_flow_fixture.py" call', body)
        result = subprocess.run(["sh", "-c", '. "$1"; flow_fixture unknown', "fixture-test", str(LIB / "flow_fixture")],
                                capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 2)


CHILD = r'''
import json,os,pathlib,sys,time
kind,effects=sys.argv[1:]; fd=int(os.environ['TETHER_ATTEMPT_RECEIPT_FD']); logical=os.environ['TETHER_ATTEMPT_LOGICAL_ID']
os.set_inheritable(fd,False)
now=lambda:time.clock_gettime_ns(time.CLOCK_BOOTTIME)
ordinal=0

def begin(phase):
    global ordinal
    ordinal+=1
    return dict(logical_id=logical,ordinal=ordinal,phase=phase,started_mono_ns=now(),sent=phase in ('request','probe','create'),**{'class':'ok'},backoff_ns=0)

def complete(row):
    row['finished_mono_ns']=now()
    os.write(fd,(json.dumps(row)+'\n').encode())

complete(begin('connect'))
if kind!='read': complete(begin('probe'))
admission=begin('request' if kind=='read' else 'create')
if kind=='read':
    print(json.dumps(dict(nodes=[dict(nid='real-fake-boundary',status='ONLINE')])),flush=True)
    time.sleep(.25)  # A blocked request must not serialize the next absolute slot.
else:
    row=dict(logical_id=logical,pid=os.getpid(),start_ticks=int(pathlib.Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19]),boot_id=pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip(),at_ns=now())
    target=os.open(effects,os.O_WRONLY|os.O_APPEND|os.O_CREAT,0o600);os.write(target,(json.dumps(row)+'\n').encode());os.close(target)
    print(json.dumps(dict(row,kind='broker-probe-started-v1')),flush=True)
complete(admission)
complete(begin('result'))
os.write(fd,(json.dumps(dict(kind='observer_close',logical_id=logical,attempt_count=ordinal,overflow=False))+'\n').encode());os.close(fd)
'''


def fd_census():
    # Counting FDs misses close-old/open-new substitutions. Retain identity.
    result = {}
    for name in os.listdir("/proc/self/fd"):
        path = Path("/proc/self/fd") / name
        try:
            stat = path.stat()
            result[name] = (os.readlink(path), stat.st_dev, stat.st_ino, stat.st_mode)
        except FileNotFoundError:
            pass  # The directory scan's own descriptor is already closed.
    return result


class BrokerCustodyTests(unittest.TestCase):
    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_service_sigterm_runs_custody_finally(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            adapter = root / "hot_update_flow_fixture.py"
            adapter.write_bytes((LIB / adapter.name).read_bytes())
            # Replace only the external service factory, so the wrapper's
            # actual signal/finally path runs in a separate, waitable process.
            (root / "flow_fixture.py").write_text('''import pathlib,socket,sys,time
def main():
    root=pathlib.Path(sys.argv[-1]);listener=socket.socket();listener.bind(("127.0.0.1",0));listener.listen()
    (root/"ready").write_text(str(listener.getsockname()[1]))
    try: time.sleep(60)
    finally:
        listener.close();(root/"closed").write_text("custody-finally")
''')
            child = subprocess.Popen([sys.executable, "-I", "-B", str(adapter), "serve", str(root)],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 5
                while not (root / "ready").exists():
                    self.assertIsNone(child.poll())
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(.01)
                child.terminate()
                child.communicate(timeout=5)
                self.assertEqual(child.returncode, 2)
                self.assertEqual((root / "closed").read_text(), "custody-finally")
            finally:
                if child.poll() is None:
                    child.kill()
                child.wait()
                child.stdout.close()
                child.stderr.close()

    # origin: hot-update S6 plan §12.1 P29: five real socket/process rounds.
    def test_child_socket_and_process_custody_closes(self):
        baseline = fd_census()
        for round in range(5):
            self.exercise_socket_process()
        self.assertEqual(fd_census(), baseline)
        self.assertFalse(threading.enumerate()[1:])

    def exercise_socket_process(self):
        fixture = load(Path(__file__).parent / "testdata/broker-socket-custody.py", "socket_custody")
        fixture.exercise_socket_process(self, wrapper.implementation(), LIB.parents[1])

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_scheduler_pipes_children_and_targeted_probes_close(self):
        baseline = fd_census()
        for round in range(5):
            self.exercise_scheduler()
        self.assertEqual(fd_census(), baseline)
        self.assertFalse(threading.enumerate()[1:])

    # origin: hot-update S6 plan §8.3 / §10.1 / §12.1 P29.
    def test_partial_scheduler_failure_reaps_children_and_pipes(self):
        baseline = fd_census()
        for round in range(5):
            self.exercise_partial_scheduler()
        self.assertEqual(fd_census(), baseline)
        self.assertFalse(threading.enumerate()[1:])

    # origin: hot-update S6 plan §8.3 / T50 / FIX-BLACKOUT-2.
    def test_calibration_scheduler_joins_every_batch_and_child(self):
        baseline = fd_census()
        for round in range(5):
            self.exercise_calibration_scheduler()
        self.assertEqual(fd_census(), baseline)
        self.assertFalse(threading.enumerate()[1:])

    def exercise_calibration_scheduler(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / "child.py"
            script.write_text(CHILD)
            config = dict(attempt_fd_env="TETHER_ATTEMPT_RECEIPT_FD", logical_id_env="TETHER_ATTEMPT_LOGICAL_ID", read_kind="nodes",
                          commands={kind: [sys.executable, "-I", "-B", str(script), kind, str(root / "effects.jsonl")]
                                    for kind in ("read", "write", "short")})
            broker.publish(root / "config.json", config)
            evidence = root / "evidence"
            mixed = load(LIB / "hot_update_mixed_probe.py", "mixed_calibration")
            self.assertEqual(mixed.broker_run_main(broker, ["broker-run", "--calibrate", "--config", str(root / "config.json"),
                             "--evidence", str(evidence), "--events", str(root / "absent.jsonl"), "--duration", "24",
                             "--late-tail", "5", "--nonce", NONCE, "--hop", "one"]), 0)
            batches = broker.read_json(evidence / "run.json")["batches"]
            self.assertEqual(len(batches), 12)
            self.assertTrue(all("finished_ns" in batch for batch in batches), "calibration left an unjoined batch")
            self.assertTrue(all(batches[i]["finished_ns"] <= batches[i+1]["started_ns"] for i in range(11)))
            result = broker.control_judge(evidence, root / "effects.jsonl")
            self.assertEqual(result["rc"], 0, result)
            self.assertEqual(result["slots"], 36)
            self.assertEqual(len(result["calibration"]["batches"]), 12)
            for effect in broker.read_rows(root / "effects.jsonl"):
                self.assertFalse(Path(f'/proc/{effect["pid"]}').exists())
                with self.assertRaises(ChildProcessError):
                    os.waitpid(effect["pid"], os.WNOHANG)

    def exercise_partial_scheduler(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / "partial.py"
            script.write_text('import os,pathlib,time; pathlib.Path(os.environ["TETHER_ATTEMPT_LOGICAL_ID"]).write_text(str(os.getpid())); '
                              'os.write(int(os.environ["TETHER_ATTEMPT_RECEIPT_FD"]),b"invalid-json\\n"); time.sleep(60)')
            # The logical ID is a safe basename, so place child evidence in its
            # inherited working directory, independent of the collector root.
            script.write_text(script.read_text().replace('pathlib.Path(os.environ["TETHER_ATTEMPT_LOGICAL_ID"])',
                                                        f'pathlib.Path({str(root / "child-pid")!r})'))
            config = dict(attempt_fd_env="TETHER_ATTEMPT_RECEIPT_FD", logical_id_env="TETHER_ATTEMPT_LOGICAL_ID", read_kind="nodes",
                          commands={kind: [sys.executable, "-I", "-B", str(script)] for kind in ("read", "write", "short")})
            broker.publish(root / "config.json", config)
            mixed = load(LIB / "hot_update_mixed_probe.py", "mixed_partial")
            with self.assertRaises(ValueError):
                mixed.broker_run_main(broker, ["broker-run", "--config", str(root / "config.json"), "--evidence", str(root / "evidence"),
                                              "--events", str(root / "absent-events.jsonl"), "--duration", ".05", "--late-tail", "5", "--nonce", NONCE, "--hop", "one"])
            pid = int((root / "child-pid").read_text())
            self.assertFalse(Path(f"/proc/{pid}").exists())
            with self.assertRaises(ChildProcessError):
                os.waitpid(pid, os.WNOHANG)
            self.assertFalse(broker.read_json(root / "evidence/run.json")["complete"])

    def exercise_scheduler(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child_script = root / "child.py"
            child_script.write_text(CHILD)
            config = dict(attempt_fd_env="TETHER_ATTEMPT_RECEIPT_FD", logical_id_env="TETHER_ATTEMPT_LOGICAL_ID", read_kind="nodes",
                          commands={kind: [sys.executable, "-I", "-B", str(child_script), kind, str(root / "effects.jsonl")]
                                    for kind in ("read", "write", "short")})
            broker.publish(root / "config.json", config)
            evidence, events = root / "evidence", root / "events.jsonl"
            mixed = load(LIB / "hot_update_mixed_probe.py", "mixed_custody")
            command = ["broker-run", "--config", str(root / "config.json"), "--evidence", str(evidence), "--events", str(events),
                       "--duration", ".3", "--late-tail", "5", "--nonce", NONCE, "--hop", "one"]
            errors = []

            def observe_phase():
                try:
                    deadline = time.monotonic() + 5
                    while not (evidence / "started.json").exists():
                        if time.monotonic() >= deadline:
                            raise ValueError("scheduler handshake missing")
                        time.sleep(.01)
                    stamp = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
                    temporary = root / "events.tmp"
                    rows(temporary, [dict(hop="one", phase=phase, started_ns=stamp, finished_ns=stamp + 200_000_000)
                                     for phase in ("freeze", "auth", "control")])
                    temporary.replace(events)
                except Exception as error:
                    errors.append(error)

            observer = threading.Thread(target=observe_phase)
            observer.start()
            try:
                self.assertEqual(mixed.broker_run_main(broker, command), 0)
                observer.join(timeout=6)
                self.assertFalse(observer.is_alive())
                self.assertFalse(errors)

                metadata = broker.read_json(evidence / "run.json")
                self.assertTrue(metadata["complete"] and metadata["late_tail_complete"])
                slots = broker.read_rows(evidence / "slots.jsonl")
                self.assertEqual(len(slots), 12)
                self.assertEqual([row["slot"] for row in slots if not row["targeted"]], [0, 1, 2])
                self.assertEqual(len(broker.read_rows(evidence / "results.jsonl")), len(slots))
                results = {row["logical_id"]: row for row in broker.read_rows(evidence / "results.jsonl")}
                self.assertLess(results[NONCE + "-1"]["started_ns"], results[NONCE + "-0"]["finished_ns"])
                attempts = broker.read_rows(evidence / "attempts.jsonl")
                self.assertEqual(len(attempts), sum(4 if slot["class"] == "read" else 5 for slot in slots))
                self.assertTrue(all(row["kind"] == "observer_close" for row in attempts if "kind" in row))
                self.assertTrue(all("process" in row for row in broker.read_rows(evidence / "results.jsonl")
                                    if row["logical_id"] in {slot["logical_id"] for slot in slots if slot["class"] != "read"}))
                for effect in broker.read_rows(root / "effects.jsonl"):
                    self.assertEqual(results[effect["logical_id"]]["process"]["true_started_ns"], effect["at_ns"], "poll time replaced the target start witness")
                    self.assertFalse(Path(f'/proc/{effect["pid"]}').exists(), "target child was not reaped")
            finally:
                observer.join(timeout=6)


if __name__ == "__main__":
    unittest.main()
