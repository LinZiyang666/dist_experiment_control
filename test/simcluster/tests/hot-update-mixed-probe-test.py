#!/usr/bin/env python3
"""Exercise the actual scheduler/CLI with a shell shim in a private PATH.

Only the ctl is fake; scheduling, subprocess deadlines, evidence and judging
are real. Product hot-update behavior remains the deploy drill's responsibility.
"""

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import runpy
import shlex
import subprocess
import sys
import tempfile
import time
import unittest


PROBE = Path(__file__).resolve().parents[1] / "drills/lib/hot_update_mixed_probe.py"
NONCE = "abcdef0123456789abcdef0123456789"


def fake_ctl(argv):
    """The shim calls this entry; it never resolves or executes another ctl."""
    mode = os.environ["FAKE_MODE"]
    trace = Path(os.environ["FAKE_TRACE"])
    with trace.open("a") as output:
        output.write(json.dumps(dict(pid=os.getpid(), argv=argv)) + "\n")
    if mode == "hang":
        print("waiting inside fake ctl", file=sys.stderr, flush=True)
        time.sleep(60)
        return 0
    if mode == "healthy":
        time.sleep(0.35)  # exercise overlapping requests; not a readiness barrier
    failures = {
        "fail": (70, "exec: request failed"),
        "connect": (69, "ps: cannot reach broker at nats://test:4222"),
        "stale": (74, "exec: node_offline: status=STALE"),
        "upgrading": (75, "exec: an image switch is in progress; retry (try_again: agent_upgrading: image switch in progress; retry)"),
    }
    if mode in failures:
        rc, message = failures[mode]
        print(message, file=sys.stderr)
        return rc
    if argv[:1] == ["exec"] and argv[3:5] == ["sh", "-c"] and "printf" in argv[5]:
        parts = argv[5].split("; ")
        assert parts[0] == "sleep 0.5" and parts[2] == "sleep 0.5", parts
        tokens = shlex.split(parts[1])
        assert tokens[:2] == ["printf", "%s\\n"] and tokens[3] == ">>", tokens
        assert tokens[4] == f"/tmp/hot-update-mixed-{NONCE}/ledger", tokens
        assert tokens[2].startswith(NONCE + "-")
        if mode != "missing":
            line = tokens[2] + "\n"
            with Path(os.environ["FAKE_LEDGER"]).open("a") as output:
                output.write(line * (2 if mode == "duplicate" else 1))
        if mode == "effectfail":
            print("exec: result unknown after acceptance", file=sys.stderr)
            return 74
    elif argv[:1] == ["run"]:
        assert argv[1:5] == ["test-node", "--", "sh", "-c"]
        assert argv[5:] == ['printf "PTY_STARTED:%s\\n" "$1"', "pty-start", NONCE], argv
        print("PTY_STARTED:" + NONCE, flush=True)
    else:
        assert argv in (["ps", "--json"], ["node", "ls", "--json"],
                        ["exec", "test-node", "--", "sh", "-c", "true", NONCE]), argv
    if mode == "noisy":
        os.write(2, ("é" * 4096 + " END").encode())
    return 0


class MixedProbeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sensor = runpy.run_path(str(PROBE))

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="hot-update-mixed-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def fixture(self, name, mode):
        root = self.root / name
        root.mkdir()
        binary = root / "bin"
        binary.mkdir()
        shim = binary / "tether"
        shim.write_text('#!/bin/sh\nexec "$FAKE_PYTHON" -I -B "$FAKE_DRIVER" --fake "$@"\n')
        shim.chmod(0o700)
        ledger = root / "ledger"
        ledger.touch()
        # PATH has no fallback to an installed tether, and the child home is
        # private even if a future fixture starts consulting configuration.
        env = dict(os.environ, PATH=str(binary), HOME=str(root),
                   FAKE_PYTHON=sys.executable, FAKE_DRIVER=str(Path(__file__).resolve()),
                   FAKE_MODE=mode, FAKE_LEDGER=str(ledger), FAKE_TRACE=str(root / "trace"))
        return root, env

    def invoke(self, *args, env=None, timeout=22):
        return subprocess.run([sys.executable, "-I", "-B", str(PROBE), *map(str, args)],
                              env=env, capture_output=True, text=True, timeout=timeout)

    def schedule(self, root, env, duration=0.8, rate=5):
        result = self.invoke("run", "--evidence", root / "evidence", "--rate", rate,
                             "--duration", duration, "--node", "test-node", "--nonce", NONCE,
                             "--mark", "cut@0", "--mark", "activated@0.7", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        evidence = root / "evidence"
        return [json.loads(line) for line in (evidence / "samples.jsonl").read_text().splitlines()]

    def judge(self, root, mode="upgrade", *window, expected=0):
        result = self.invoke("judge", "--evidence", root / "evidence", "--ledger", root / "ledger",
                             "--mode", mode, *window)
        self.assertEqual(result.returncode, expected, (result.stdout, result.stderr))
        value = json.loads(result.stdout)
        self.assertEqual(len(result.stdout.splitlines()), 1)
        return value

    # origin: hot-update S3 plan §8.2. This checks commands observed by the
    # external shim, not just the request builder's internal representation.
    def test_scheduler_rotates_overlaps_and_collects_external_marks(self):
        root, env = self.fixture("healthy", "healthy")
        evidence = root / "evidence"
        child = subprocess.Popen([sys.executable, "-I", "-B", str(PROBE), "run",
                                  "--evidence", str(evidence), "--rate", "5", "--duration", "2.4",
                                  "--node", "test-node", "--nonce", NONCE, "--mark", "cut@0.3", "--mark", "activated@0.6"],
                                 env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 4
            while not (evidence / "run.json").exists():
                self.assertIsNone(child.poll())
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.01)
            meta = json.loads((evidence / "run.json").read_text())
            stamp = time.monotonic() - meta["started_monotonic"]
            staging = evidence / "marks.d/.external"
            staging.write_text(str(stamp) + "\n")
            staging.replace(evidence / "marks.d/external")
            stdout, stderr = child.communicate(timeout=6)
            self.assertEqual(child.returncode, 0, (stdout, stderr))
        finally:
            if child.poll() is None:
                child.kill()
            child.communicate()
        samples = sorted((json.loads(line) for line in (evidence / "samples.jsonl").read_text().splitlines()),
                         key=lambda s: s["seq"])
        self.assertEqual([s["seq"] for s in samples], list(range(12)))
        self.assertEqual([s["class"] for s in samples], ["read", "write", "short"] * 4)
        self.assertEqual([s["argv0_kind"] for s in samples if s["class"] == "read"],
                         ["ps", "node", "ps", "node"])
        self.assertTrue(any(a["t_end"] > b["t_start"] for a, b in zip(samples, samples[1:])))
        trace = [json.loads(line)["argv"] for line in (root / "trace").read_text().splitlines()]
        self.assertEqual(trace.count(["ps", "--json"]), 2)
        self.assertEqual(trace.count(["node", "ls", "--json"]), 2)
        self.assertEqual(trace.count(["exec", "test-node", "--", "sh", "-c", "true", NONCE]), 4)
        self.assertEqual(sorted((root / "ledger").read_text().splitlines()),
                         sorted(f"{NONCE}-{seq}" for seq in (1, 4, 7, 10)))
        marks = json.loads((evidence / "marks.json").read_text())
        self.assertEqual(marks, {"cut": 0.3, "activated": 0.6, "external": stamp})
        for mode in ("calibrate", "upgrade"):
            result = self.judge(root, mode)
            self.assertEqual(result["rate"], 5)
            self.assertEqual(result["skipped"], 0)
            self.assertEqual(result["over_5s"], 0)
            self.assertEqual({k: v["count"] for k, v in result["classes"].items()},
                             {"read": 4, "write": 4, "short": 4})
        # A mark arriving after run is read by judge as well.
        (evidence / "marks.d/after").write_text("100\n")
        result = self.judge(root, "upgrade", "--window-start", "after", "--window-end", "after", expected=1)
        self.assertEqual(result["in_window"], 0)
        result = self.judge(root, "upgrade", "--window-start", "external", "--window-end", "external", expected=1)
        self.assertGreater(result["in_window"], 0)

    # origin: hot-update S3 plan §8.1 row 34.P. Each injected defect must
    # change the actual CLI verdict, including success with no ledger effect.
    def test_shim_failures_change_verdicts_and_preserve_classification(self):
        def exercise(mode):
            root, env = self.fixture(mode, mode)
            samples = self.schedule(root, env)
            expected = 0 if mode == "noisy" else 1
            result = self.judge(root, expected=expected)
            self.assertEqual(len(samples), 4)
            if mode in ("fail", "connect", "stale", "upgrading"):
                key = {"fail": "request_failures", "connect": "connect_failures",
                       "stale": "stale_offline_failures", "upgrading": "agent_upgrading"}[mode]
                self.assertEqual(result[key], 4)
                self.assertEqual(result["request_failures"], 4 if mode in ("fail", "stale") else 0)
            elif mode in ("duplicate", "missing", "effectfail"):
                key = {"duplicate": "dup_side_effects", "missing": "rc0_without_effect",
                       "effectfail": "effect_without_rc0"}[mode]
                self.assertEqual(result[key], 1)
            else:
                self.assertTrue(all(s["stderr_tail"].endswith(" END") for s in samples))
                self.assertTrue(all(500 <= len(s["stderr_tail"].encode()) <= 512 for s in samples))
            calibration_rc = 1 if mode in ("fail", "connect", "stale", "effectfail") else 0
            self.judge(root, "calibrate", expected=calibration_rc)
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(exercise, ("fail", "connect", "stale", "upgrading",
                                     "duplicate", "missing", "effectfail", "noisy")))

    # origin: hot-update S3 plan §8.2. Real timeouts force the concurrency
    # ceiling without a test-only scheduler knob; every child must be reaped.
    def test_saturation_counts_skips_and_hard_timeouts_reap_children(self):
        root, env = self.fixture("saturation", "hang")
        samples = self.schedule(root, env, duration=4.6, rate=2)
        self.assertEqual(len(samples), 8)
        metadata = json.loads((root / "evidence/run.json").read_text())
        self.assertEqual(metadata["skipped"], 2)
        self.assertEqual(metadata["scheduled"], 10)
        _, request_s = self.sensor["budgets"]()
        for sample in samples:
            self.assertEqual(sample["rc"], 124)
            self.assertIn("hard request timeout", sample["stderr_tail"])
            self.assertGreaterEqual(sample["latency_s"], 2 * request_s)
            self.assertLess(sample["latency_s"], 2 * request_s + 1.5)
        result = self.judge(root, "calibrate", expected=1)
        self.assertEqual(result["skipped"], 2)
        self.assertEqual(result["request_failures"], 8)
        self.assertEqual(result["over_5s"], 8)
        self.assertLess(result["rate"], 0.9 * 2)
        for line in (root / "trace").read_text().splitlines():
            with self.assertRaises(ProcessLookupError):
                os.kill(json.loads(line)["pid"], 0)

    def synthetic(self):
        root = self.root / "synthetic"
        evidence = root / "evidence"
        (evidence / "marks.d").mkdir(parents=True)
        samples = []
        for seq in range(60):
            _, row = self.sensor["request"](seq, "test-node", NONCE)
            latency = 20 - seq // 3
            row.update(t_start=10, t_end=10 + latency, latency_s=latency, rc=0, stderr_tail="")
            samples.append(row)
        (evidence / "samples.jsonl").write_text("".join(json.dumps(s) + "\n" for s in samples))
        (root / "ledger").write_text("".join(s["seq_written"] + "\n" for s in samples if s["class"] == "write"))
        (evidence / "marks.json").write_text(json.dumps(dict(cut=10, activated=10.2, before=9, start=10, end=30, after=31)))
        metadata = dict(complete=True, rate=10, duration_s=6, request_s=self.sensor["budgets"]()[1],
                        finished_s=30, scheduled=60, skipped=0, nonce=NONCE)
        (evidence / "run.json").write_text(json.dumps(metadata))
        return root, samples, metadata

    # origin: hot-update S3 plan §8.2. Integer nearest-rank expectations
    # distinguish interpolation, floor rounding and an off-by-one index.
    def test_nearest_rank_and_closed_interval_overlap_are_exact(self):
        root, samples, _ = self.synthetic()
        result = self.judge(root)
        for stats in result["classes"].values():
            self.assertEqual(stats, dict(count=20, p50=10, p95=19, p99=20, max=20))
        self.assertEqual(result["over_5s"], sum(s["latency_s"] > self.sensor["budgets"]()[1] for s in samples))
        for mark, count in (("before", 0), ("start", 60), ("end", 3), ("after", 0)):
            result = self.judge(root, "upgrade", "--window-start", mark, "--window-end", mark,
                                expected=0 if count else 1)
            self.assertEqual(result["in_window"], count)
        self.assertEqual(self.sensor["percentiles"]([7]), dict(count=1, p50=7, p95=7, p99=7, max=7))
        self.assertEqual(self.sensor["percentiles"]([]), dict(count=0, p50=None, p95=None, p99=None, max=None))

    # origin: hot-update S3 plan §8.2. Isolate starvation from errors: every
    # recorded request is still successful at either side of the rate bound.
    def test_calibration_rejects_starvation_and_accepts_threshold(self):
        root, _, metadata = self.synthetic()
        for rate, rc in ((9, 0), (8.99, 1)):
            metadata.update(duration_s=60 / rate)
            (root / "evidence/run.json").write_text(json.dumps(metadata))
            result = self.judge(root, "calibrate", expected=rc)
            self.assertAlmostEqual(result["rate"], rate)
            self.assertEqual(result["request_failures"], 0)
            self.assertEqual(result["connect_failures"], 0)

    # origin: hot-update S3 plan §8.2. A broken sensor must not produce a
    # green verdict from a subset of evidence or a silently missing window.
    def test_incomplete_evidence_and_invalid_marks_are_rejected(self):
        root, samples, metadata = self.synthetic()
        evidence = root / "evidence"
        original = (evidence / "samples.jsonl").read_text()
        for rows in (samples[:-1], samples[:-1] + [samples[0]], [dict(samples[0], t_end=float("nan"))] + samples[1:]):
            (evidence / "samples.jsonl").write_text("".join(json.dumps(s) + "\n" for s in rows))
            result = self.invoke("judge", "--evidence", evidence, "--ledger", root / "ledger", "--mode", "upgrade")
            self.assertEqual(result.returncode, 2, result.stderr)
        (evidence / "samples.jsonl").write_text(original)
        (evidence / "run.json").write_text(json.dumps(dict(metadata, complete=False)))
        result = self.invoke("judge", "--evidence", evidence, "--ledger", root / "ledger", "--mode", "upgrade")
        self.assertEqual(result.returncode, 2, result.stderr)
        (evidence / "run.json").write_text(json.dumps(metadata))
        for start, end in (("missing", "end"), ("end", "start")):
            result = self.invoke("judge", "--evidence", evidence, "--ledger", root / "ledger", "--mode", "upgrade",
                                 "--window-start", start, "--window-end", end)
            self.assertEqual(result.returncode, 2, result.stderr)
        for value in ("11", "nan", "-1"):
            (evidence / "marks.d/cut").write_text(value)
            result = self.invoke("judge", "--evidence", evidence, "--ledger", root / "ledger", "--mode", "upgrade")
            self.assertEqual(result.returncode, 2, result.stderr)

    # origin: hot-update S3 plan §6 and §8.2. Copied budget fixtures prove
    # the parser reads both assignments without sourcing arbitrary shell.
    def test_budget_assignments_and_run_inputs_fail_closed(self):
        budget = self.root / "budget.sh"
        budget.write_text("HOT_UPDATE_PROBE_RATE=7\nHOT_UPDATE_REQUEST_S=2.5\n")
        self.assertEqual(self.sensor["budgets"](budget), (7, 2.5))
        for text in ("HOT_UPDATE_PROBE_RATE=7\n", "HOT_UPDATE_PROBE_RATE=7\nHOT_UPDATE_REQUEST_S=0\n",
                     "HOT_UPDATE_PROBE_RATE=7\nHOT_UPDATE_PROBE_RATE=8\nHOT_UPDATE_REQUEST_S=2\n"):
            budget.write_text(text)
            with self.assertRaises((ValueError, KeyError)):
                self.sensor["budgets"](budget)
        root, env = self.fixture("invalid", "healthy")
        for option in (("--nonce", "not-hex"), ("--rate", "0"), ("--duration", "nan"),
                       ("--mark", "late@2"), ("--mark", "bad/name@0.1"),
                       ("--mark", "same@0.1", "--mark", "same@0.2")):
            result = self.invoke("run", "--evidence", root / "evidence", "--rate", "2", "--duration", "1",
                                 "--node", "test-node", "--nonce", NONCE, *option, env=env)
            self.assertEqual(result.returncode, 2, result.stderr)
        self.assertFalse((root / "trace").exists())
        for options in (("--until-mark", "activated", "--max-duration", "1"),
                        ("--until-mark", "bad/name", "--tail", ".1", "--max-duration", "1"),
                        ("--until-mark", "activated", "--tail", ".1"),
                        ("--until-mark", "activated", "--tail", ".1", "--max-duration", "1", "--mark", "activated@.2"),
                        ("--duration", "1", "--tail", ".1"),
                        ("--duration", "1", "--max-duration", "2")):
            result = self.invoke("run", "--evidence", root / "evidence", "--node", "test-node",
                                 "--nonce", NONCE, *options, env=env)
            self.assertEqual(result.returncode, 2, result.stderr)
        self.assertFalse((root / "trace").exists())
        (root / "evidence").mkdir(exist_ok=True)
        (root / "evidence/run.json").write_text("{}")
        result = self.invoke("run", "--evidence", root / "evidence", "--duration", "1",
                             "--node", "test-node", "--nonce", NONCE, env=env)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertFalse((root / "trace").exists())

    # origin: hot-update S3 plan §8.1; DR-4/DR-5/DRILLS-V1. These are complete
    # file receipts, including the review's read-only and all-refused runs.
    def test_upgrade_requires_exercised_work_and_reconciled_effects(self):
        cases = ("healthy", "threshold", "starved_read_only", "starved", "all_exec_refused",
                 "no_read_success", "no_short_success", "effect_on_refusal", "outside_refusal",
                 "allowed_refusal", "unrequested", "switch_after_traffic", "no_write_overlap")
        for name in cases:
            with self.subTest(name=name):
                root = self.root / name
                evidence = root / "evidence"
                (evidence / "marks.d").mkdir(parents=True)
                rows, ledger = [], []
                for seq in range(400):
                    _, row = self.sensor["request"](seq, "test-node", NONCE)
                    row.update(t_start=seq/10, t_end=seq/10+.15, latency_s=.15, rc=0, stderr_tail="")
                    if name == "starved_read_only" and seq != 102:
                        continue
                    refused = (name == "all_exec_refused" and row["class"] != "read"
                               or name == "no_read_success" and row["class"] == "read"
                               or name == "no_short_success" and row["class"] == "short"
                               or name in ("effect_on_refusal", "allowed_refusal") and seq == 103
                               or name == "outside_refusal" and seq == 1)
                    if refused:
                        row.update(rc=75, stderr_tail="try_again: agent_upgrading: image switch in progress; retry")
                    if row["class"] == "write" and (not refused or name == "effect_on_refusal"):
                        ledger.append(row["seq_written"])
                    if name == "no_write_overlap" and row["class"] == "write":
                        row.update(t_start=0, t_end=.01, latency_s=.01)
                    rows.append(row)
                if name == "unrequested":
                    ledger.append(NONCE + "-402")
                duration = 400/9 if name == "threshold" else 400/8.99 if name == "starved" else 40
                metadata = dict(rate=10, duration_s=duration, request_s=5, nonce=NONCE,
                                skipped=400-len(rows), scheduled=400, complete=True, finished_s=max(duration,40.15))
                window = dict(cut=10, activated=10.4)
                if name in ("all_exec_refused", "no_read_success", "no_short_success"):
                    window = dict(cut=0, activated=40)
                if name == "switch_after_traffic":
                    window = dict(cut=45, activated=45.2)
                # Scheduled marks remain as decoys: the default MUST select observations.
                window.update(upgrade=10, end=40)
                (evidence / "run.json").write_text(json.dumps(metadata))
                (evidence / "marks.json").write_text(json.dumps(window))
                (evidence / "samples.jsonl").write_text("".join(json.dumps(r)+"\n" for r in rows))
                (root / "ledger").write_text("".join(token+"\n" for token in ledger))
                result = self.judge(root, expected=0 if name in ("healthy", "threshold", "allowed_refusal") else 1)
                if name == "effect_on_refusal":
                    self.assertEqual(result["effect_without_rc0"], 1)
                    self.assertEqual(result["request_failures"], 0)
                if name == "unrequested":
                    self.assertEqual(result["unrequested_effects"], 1)
                if name == "outside_refusal":
                    self.assertEqual(result["request_failures"], 1)
                    self.assertLess(result["agent_upgrading"], result["starts_in_window"])
                if name == "switch_after_traffic":
                    self.assertEqual(result["in_window"], 0)
                if name == "no_write_overlap":
                    self.assertGreater(result["in_window"], 0)
                    self.assertEqual(result["writes_in_window"], 0)

    # origin: hot-update S3 plan §8.1; DR-5. Hold activation until traffic has
    # demonstrably run; the real mark CLI and scheduler share a monotonic clock.
    def test_until_mark_waits_for_observation_and_runs_the_tail(self):
        root, env = self.fixture("observed", "healthy")
        evidence = root / "evidence"
        command = [sys.executable, "-I", "-B", str(PROBE), "run", "--evidence", str(evidence),
                   "--rate", "10", "--until-mark", "activated", "--tail", "0.6",
                   "--max-duration", "6", "--node", "test-node", "--nonce", NONCE]
        child = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic()+5
            for name, minimum in (("cut", 10), ("activated", 16)):
                while True:
                    self.assertIsNone(child.poll(), "traffic stopped before the observation")
                    self.assertLess(time.monotonic(), deadline)
                    trace = root / "trace"
                    if (evidence / "started.json").exists() and trace.exists() and len(trace.read_text().splitlines()) >= minimum:
                        break
                    time.sleep(.01)
                result = self.invoke("mark", "--evidence", evidence, "--name", name)
                self.assertEqual(result.returncode, 0, result.stderr)
            stdout, stderr = child.communicate(timeout=5)
            self.assertEqual(child.returncode, 0, (stdout, stderr))
        finally:
            if child.poll() is None:
                child.kill()
            child.communicate()
        marks = json.loads((evidence / "marks.json").read_text())
        meta = json.loads((evidence / "run.json").read_text())
        receipt = json.loads((evidence / "started.json").read_text())
        self.assertEqual(receipt, dict(started_monotonic=meta["started_monotonic"], nonce=NONCE))
        self.assertGreater(marks["activated"], marks["cut"])
        self.assertAlmostEqual(meta["duration_s"], marks["activated"]+.6)
        rows = [json.loads(line) for line in (evidence / "samples.jsonl").read_text().splitlines()]
        self.assertGreater(max(r["t_start"] for r in rows), marks["activated"]+.4)
        self.assertGreater(self.judge(root)["writes_in_window"], 0)
        duplicate = self.invoke("mark", "--evidence", evidence, "--name", "cut")
        self.assertEqual(duplicate.returncode, 2)

    # origin: hot-update S3 plan §8.1; DR-5. Missing activation must time out
    # with an incomplete receipt, never turn a bounded run into a false pass.
    def test_until_mark_missing_activation_fails_closed(self):
        root, env = self.fixture("missing-activation", "healthy")
        result = self.invoke("run", "--evidence", root / "evidence", "--rate", "5",
                             "--until-mark", "activated", "--tail", ".2", "--max-duration", ".8",
                             "--node", "test-node", "--nonce", NONCE, env=env)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("not observed", result.stderr)
        self.assertFalse(json.loads((root / "evidence/run.json").read_text())["complete"])
        root, env = self.fixture("tail-overflow", "healthy")
        marks = root / "evidence/marks.d"
        marks.mkdir(parents=True)
        (marks / "activated").write_text(".7\n")
        result = self.invoke("run", "--evidence", root / "evidence", "--rate", "5",
                             "--until-mark", "activated", "--tail", ".2", "--max-duration", ".8",
                             "--node", "test-node", "--nonce", NONCE, env=env)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("tail exceeds", result.stderr)

    # origin: hot-update S4 plan §9.1 / T48. Real scheduler and response
    # capture, with a private shim for the product, exercise the strict run path.
    def test_strict_scheduler_observes_new_run_started_responses(self):
        root, env = self.fixture("strict-scheduler", "healthy")
        evidence = root / "evidence"
        result = self.invoke("run", "--strict", "--evidence", evidence, "--rate", 10,
                             "--duration", 2.4, "--node", "test-node", "--nonce", NONCE,
                             "--mark", "cut@.7", "--mark", "activated@1.4", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        result = self.invoke("judge", "--strict", "--evidence", evidence, "--ledger", root / "ledger",
                             "--mode", "upgrade")
        self.assertEqual(result.returncode, 0, (result.stdout, result.stderr))
        summary = json.loads(result.stdout)
        self.assertEqual(summary["paths"]["run"]["count"], 4)
        self.assertEqual(summary["started_response"]["count"], 4)
        self.assertEqual(summary["started_missing"], 0)
        self.assertLess(summary["started_response"]["max"], 5)
        rows = [json.loads(line) for line in (evidence / "samples.jsonl").read_text().splitlines()]
        self.assertTrue(all(0 <= r["started_latency_s"] <= r["latency_s"]
                            for r in rows if r["argv0_kind"] == "run"))
        trace = [json.loads(line)["argv"] for line in (root / "trace").read_text().splitlines()]
        self.assertEqual(sum(argv[0] == "run" for argv in trace), 4)

    # origin: hot-update S4 plan §9.1 / T48. The public strict judge is
    # file-driven; 34's allowed-refusal and over-budget behavior stay unchanged.
    def test_strict_rejects_refusals_slow_requests_and_empty_fault_windows(self):
        for name in ("positive", "over_5s", "refusal", "request_failure", "connect_failure",
                     "empty_window", "outside_window", "missing_window", "starved",
                     "no_write", "no_read", "no_short", "missing_effect", "duplicate_effect", "no_new_run",
                     "missing_started", "slow_started", "wrong_budgets"):
            with self.subTest(name=name):
                root = self.root / name
                evidence = root / "evidence"
                (evidence / "marks.d").mkdir(parents=True)
                samples, ledger = [], []
                for seq in range(100):
                    _, row = self.sensor["request"](seq, "test-node", NONCE, strict=name != "no_new_run")
                    row.update(t_start=seq/10, t_end=seq/10+.2, latency_s=.2, rc=0, stderr_tail="")
                    if row["argv0_kind"] == "run":
                        row["started_latency_s"] = .05
                    if name == "missing_started" and seq == 26:
                        row["started_latency_s"] = None
                    if name == "slow_started" and seq == 26:
                        row.update(started_latency_s=5.1, t_end=8.1, latency_s=5.5)
                    if name == "over_5s" and seq == 25:
                        row.update(t_end=8.501, latency_s=6.001)
                    if name == "refusal" and seq == 26:
                        row.update(rc=75, stderr_tail="try_again: agent_upgrading: image switch in progress; retry")
                    if name == "request_failure" and seq == 26:
                        row.update(rc=74, stderr_tail="request failed")
                    if name == "connect_failure" and seq == 26:
                        row.update(rc=69, stderr_tail="cannot reach broker")
                    if row["class"] == "write" and not (name == "missing_effect" and seq == 25):
                        ledger.append(row["seq_written"])
                    if name in ("no_write", "no_read", "no_short") and row["class"] == name[3:]:
                        continue
                    samples.append(row)
                if name == "duplicate_effect":
                    ledger.append(ledger[0])
                duration = 100/8.99 if name == "starved" else 10
                metadata = dict(rate=10, duration_s=duration, request_s=5, nonce=NONCE,
                                skipped=100-len(samples), scheduled=100, complete=True, finished_s=12, strict=name != "no_new_run")
                if name == "wrong_budgets":
                    metadata["request_s"] = 6
                marks = dict(cut=2.5, activated=3)
                if name == "empty_window":
                    marks["activated"] = marks["cut"]
                if name == "outside_window":
                    marks.update(cut=10.5, activated=11)
                if name == "missing_window":
                    marks.pop("cut")
                (evidence / "run.json").write_text(json.dumps(metadata))
                (evidence / "samples.jsonl").write_text("".join(json.dumps(row)+"\n" for row in samples))
                (evidence / "marks.json").write_text(json.dumps(marks))
                (root / "ledger").write_text("".join(token+"\n" for token in ledger))
                result = self.invoke("judge", "--evidence", evidence, "--ledger", root / "ledger",
                                     "--mode", "upgrade", "--strict")
                expected = 0 if name == "positive" else 2 if name in ("empty_window", "missing_window", "wrong_budgets") else 1
                self.assertEqual(result.returncode, expected, (name, result.stdout, result.stderr))
                if name in ("refusal", "over_5s"):
                    default = self.invoke("judge", "--evidence", evidence, "--ledger", root / "ledger", "--mode", "upgrade")
                    self.assertEqual(default.returncode, 0, (default.stdout, default.stderr))
                    summary = json.loads(result.stdout)
                    self.assertEqual(summary["refusals" if name == "refusal" else "over_5s"], 1)
                    self.assertEqual(summary["classes"]["write"]["max"], 6.001 if name == "over_5s" else .2)

    # origin: hot-update S3 plan §8.2. Keep the public offline selftest usable
    # inside the same minimal Python environment as the copied probe.
    def test_selftest_exercises_all_verdict_controls(self):
        result = self.invoke("selftest")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("PASS", result.stdout)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--fake"]:
        sys.exit(fake_ctl(sys.argv[2:]))
    unittest.main()
