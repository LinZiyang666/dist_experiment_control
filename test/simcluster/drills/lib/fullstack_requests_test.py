#!/usr/bin/env python3
"""Hermetic P17 adversarial selftests; no product, deployment, or Go build.

Virtual handles model the nonblocking P05/P18 boundary, not product success.
P25 owns the five real scheduler lifecycle rounds and P18's actual CLI receipts.
The immutable expected rows here are made before running the dispatcher, and
the launch callback records actual launches independently of its result list.
"""

from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

# The S10 runner executes every selftest with -I (isolated mode), which drops
# the script directory from sys.path; import the sibling module explicitly
# instead of depending on whoever happens to launch this file.
# origin: hot-update S10 plan §10.2 P32 / hermetic gate set registration.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import fullstack_requests as requests


START_NS = 10_000_000_000


def sealed_schedule(count=20):
    # This oracle does not call scheduler code or infer slots from launches.
    keys = [requests.Key("agent", "agt" + str(i // 2 + 1), "a" + str(i) + ".service",
                         "s10-" + str(i), "a" + str(i), "domain-" + str(i), 2000 + i)
            for i in range(4)]
    pattern = ("read", "read", "read", "write", "short-exec")
    rows = [{"id": "test/slot/%08d" % i, "class": pattern[i % 5],
             "target": asdict(keys[i % 4]), "scheduled_ns": START_NS + i * 100_000_000,
             "deadline_ns": START_NS + i * 100_000_000 + 5_000_000_000}
            for i in range(count)]
    raw = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    return requests.load_schedule(raw, hashlib.sha256(raw).hexdigest())


def raw_ref(name="cli"):
    data = (name + ":complete-raw").encode()
    return requests.RawRef(name + ".json", hashlib.sha256(data).hexdigest(), len(data), "p17-selftest")


class Clock:
    def __init__(self, now=START_NS):
        self.now = now

    def read(self):
        return self.now

    def wait(self, deadline):
        if deadline <= self.now:
            raise AssertionError("dispatcher tried to stop or rewind the clock")
        self.now = deadline


class Handle:
    def __init__(self, request, clock, latency=20_000_000, rc=0):
        self.request, self.clock = request, clock
        self.started, self.ready = clock.read(), clock.read() + latency
        self.cancelled, self.joins, self.rc = False, [], rc
        self.outcome = None

    def poll(self, now):
        if now < self.ready and not self.cancelled:
            return None
        if self.outcome is None:
            request = self.request
            phase = "request" if request.slot.kind == "read" else "create"
            attempt = requests.Attempt(request.slot.id, request.call_id, phase, 1,
                self.started, min(now, self.ready), True, True, self.rc, "", raw_ref())
            stdout = request.expected_stdout if request.slot.kind == "short-exec" else b"real-result\n"
            self.outcome = requests.Outcome((attempt,), stdout, self.rc, True, True, True, raw_ref())
        return self.outcome

    def cancel(self):
        self.cancelled = True

    def join(self, deadline):
        self.joins.append(deadline)
        return self.poll(self.clock.read())


class Adapter:
    def __init__(self, clock, latency=20_000_000):
        self.clock, self.latency = clock, latency
        self.actual_launches, self.effects, self.handles = [], [], []
        self.fail_slot = None

    def launch(self, request):
        self.actual_launches.append(request)
        handle = Handle(request, self.clock, self.latency,
                        rc=75 if request.slot.id == self.fail_slot else 0)
        self.handles.append(handle)
        if request.effect_token:
            self.effects.append(requests.Effect(request.slot.target, request.effect_token))
        return handle


# origin: hot-update S10 plan §10.2 P17 / §3.2 / §4.4 / §8 M10S
class FullstackRequestScheduleTest(unittest.TestCase):
    def run_schedule(self, count=20, latency=20_000_000, start=START_NS, fail_index=None):
        schedule, clock = sealed_schedule(count), Clock(start)
        adapter = Adapter(clock, latency)
        if fail_index is not None:
            adapter.fail_slot = schedule.slots[fail_index].id
        scheduler = requests.Scheduler(schedule, adapter.launch, clock.read, clock.wait)
        report = scheduler.run()
        return schedule, clock, adapter, report

    def test_fixed_slots_launch_without_pending_throttle(self):
        schedule, _, adapter, report = self.run_schedule(80, 4_990_000_000)
        self.assertEqual([r.slot.id for r in adapter.actual_launches], [s.id for s in schedule.slots],
                         "M10S: every sealed slot must actually launch")
        self.assertEqual(requests.audit_requests(schedule, report, adapter.effects)["rc"], 0)
        self.assertGreaterEqual(report.pending_peak, 49)
        self.assertGreaterEqual(report.pending_create_peak, 19)
        self.assertEqual(set(report.pending_create_by_target.values()), {6})
        self.assertEqual(len(report.attempts()), 80)
        self.assertEqual([r.started_ns for r in report.launches], [s.scheduled_ns for s in schedule.slots])
        self.assertEqual([r.operation for r in adapter.actual_launches[:3]],
                         ["node-ls", "ps", "status"])
        self.assertEqual(counter_targets(adapter.actual_launches), {"a0": 20, "a1": 20, "a2": 20, "a3": 20})
        self.assertTrue(all(len(h.joins) == 1 for h in adapter.handles))

    def test_absolute_deadline_includes_launch_queue(self):
        # Slot 0 launches 50ms late and completes 10ms beyond its scheduled
        # deadline. A start-relative budget would falsely make it successful.
        schedule, _, adapter, report = self.run_schedule(4, 4_960_000_000, START_NS + 50_000_000)
        self.assertEqual(report.launches[0].started_ns, START_NS + 50_000_000)
        result = requests.audit_requests(schedule, report, adapter.effects)
        self.assertEqual(result["rc"], 1, "queue time must consume the original five seconds")
        self.assertIn(schedule.slots[0].id + ":deadline", result["failures"])
        self.assertTrue(adapter.handles[0].cancelled)
        # The independent latency guard is tested even if an adapter delivers
        # its result after cancellation and claims a clean product RC.
        first = report.launches[0]
        first.error = ""
        first.finished_ns = first.started_ns + 4_960_000_000
        self.assertIn(schedule.slots[0].id + ":scheduled-deadline",
                      requests.audit_requests(schedule, report, adapter.effects)["failures"])

    def test_failed_samples_keep_every_attempt(self):
        schedule, _, adapter, report = self.run_schedule(fail_index=2)
        attempts = report.attempts()
        self.assertEqual(len(attempts), len(schedule.slots), "failed samples cannot disappear")
        self.assertEqual([a.rc for a in attempts if a.slot_id == schedule.slots[2].id], [75])
        result = requests.audit_requests(schedule, report, adapter.effects)
        self.assertEqual(result["rc"], 1)
        self.assertIn(schedule.slots[2].id + ":result-failed", result["failures"])
        self.assertEqual(result["scheduled"], 20)
        self.assertEqual(result["completed"], 20)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(len(adapter.actual_launches), 20, "a failed business request must not relaunch")

    def test_connection_attempts_keep_one_business_identity(self):
        schedule, _, adapter, report = self.run_schedule()
        first = report.launches[0]
        done = first.outcome.attempts[0]
        connect = replace(done, phase="connect", finished_ns=done.started_ns + 1,
                          completed=False, sent=False, rc=124)
        command = replace(done, index=2, started_ns=connect.finished_ns)
        first.outcome = replace(first.outcome, attempts=(connect, command))
        self.assertEqual(requests.audit_requests(schedule, report, adapter.effects)["rc"], 0)
        self.assertEqual(len(report.attempts()), 21)
        first.outcome = replace(first.outcome, attempts=(connect, replace(command, call_id="new-task")))
        self.assertIn(schedule.slots[0].id + ":attempt-identity-or-clock",
                      requests.audit_requests(schedule, report, adapter.effects)["failures"])

    def test_launch_failure_never_resubmits_business_command(self):
        schedule, clock = sealed_schedule(), Clock()
        adapter = Adapter(clock)
        target = schedule.slots[3].id
        calls = []

        def launch(request):
            calls.append(request.slot.id)
            if request.slot.id == target:
                raise RuntimeError("original acceptance is unknown")
            return adapter.launch(request)

        report = requests.Scheduler(schedule, launch, clock.read, clock.wait).run()
        self.assertEqual(calls.count(target), 1, "implicit resubmission creates a second business task")
        self.assertEqual(calls, [s.id for s in schedule.slots])
        self.assertEqual(report.launches[3].error, "launch-error")
        self.assertEqual(requests.audit_requests(schedule, report, adapter.effects)["rc"], 1)
        self.assertEqual(report.launches[4].started_ns, schedule.slots[4].scheduled_ns)

    def test_missing_launch_duplicate_effect_and_changed_expected_are_red(self):
        schedule, _, adapter, report = self.run_schedule()
        self.assertEqual(requests.audit_requests(schedule, report, adapter.effects)["rc"], 0)
        row = report.launches.pop(3)
        self.assertIn("launch-conservation", requests.audit_requests(schedule, report, adapter.effects)["failures"])
        report.launches.insert(3, row)
        for effects in (adapter.effects[:-1], adapter.effects + adapter.effects[:1],
                        adapter.effects + [requests.Effect(schedule.slots[0].target, "unissued-token")]):
            with self.subTest(effects=len(effects)):
                self.assertIn("effect-conservation", requests.audit_requests(schedule, report, effects)["failures"])
        report.launches.append(row)
        self.assertIn("duplicate-launch:" + row.request.slot.id,
                      requests.audit_requests(schedule, report, adapter.effects)["failures"])
        report.launches.pop()
        report.schedule = replace(schedule, digest="0" * 64)
        self.assertIn("schedule-changed", requests.audit_requests(schedule, report, adapter.effects)["failures"])

    def test_short_result_terminal_reliability_and_read_are_required(self):
        schedule, _, adapter, report = self.run_schedule()
        cases = [(4, {"stdout": b""}, "short-result"), (4, {"rc": 7}, "result-failed"),
                 (3, {"terminal": False}, "result-failed"),
                 (3, {"reliable": False}, "reliable-path-missing"),
                 (0, {"result_valid": False}, "read-result"),
                 (0, {"stdout": b""}, "read-result"),
                 (0, {"attempts": ()}, "missing-attempts"),
                 (0, {"raw": replace(raw_ref(), bytes=0)}, "result-failed")]
        for index, changes, failure in cases:
            with self.subTest(changes=changes):
                original = report.launches[index].outcome
                report.launches[index].outcome = replace(original, **changes)
                result = requests.audit_requests(schedule, report, adapter.effects)
                self.assertIn(schedule.slots[index].id + ":" + failure, result["failures"])
                report.launches[index].outcome = original

    def test_attempt_refusal_is_not_erased_by_later_success(self):
        schedule, _, adapter, report = self.run_schedule()
        first = report.launches[0]
        good = first.outcome.attempts[0]
        refusal = replace(good, finished_ns=good.started_ns, code="upgrade_deferred", rc=75)
        first.outcome = replace(first.outcome, attempts=(refusal, replace(good, index=2)))
        self.assertIn(schedule.slots[0].id + ":attempt-refusal",
                      requests.audit_requests(schedule, report, adapter.effects)["failures"])
        self.assertEqual(len(report.attempts()), 21)

    def test_cancellation_keeps_future_slots_and_joins_pending(self):
        schedule, clock = sealed_schedule(), Clock()
        adapter = Adapter(clock, latency=4_000_000_000)
        scheduler = requests.Scheduler(schedule, adapter.launch, clock.read, clock.wait)
        scheduler.tick()
        scheduler.stop()
        report = scheduler.run()
        self.assertEqual(len(report.launches), 20)
        self.assertEqual(len(adapter.actual_launches), 1)
        self.assertTrue(adapter.handles[0].cancelled)
        self.assertEqual(len(adapter.handles[0].joins), 1)
        self.assertEqual(requests.audit_requests(schedule, report, adapter.effects)["rc"], 1)
        with self.assertRaises(ValueError):
            scheduler.run()

    def test_expired_and_missed_slots_do_not_pause_the_schedule(self):
        schedule, _, adapter, report = self.run_schedule(start=START_NS + 5_000_000_001)
        self.assertEqual(report.launches[0].error, "not-launched-deadline")
        self.assertNotIn(schedule.slots[0].id, [r.slot.id for r in adapter.actual_launches])
        self.assertEqual(len(report.launches), 20)
        result = requests.audit_requests(schedule, report, adapter.effects)
        self.assertEqual(result["rc"], 1)
        self.assertIn(schedule.slots[1].id + ":missed-launch-slot", result["failures"])

    def test_interrupted_wait_cancels_and_joins_original_handles(self):
        schedule, clock = sealed_schedule(), Clock()
        adapter = Adapter(clock, latency=4_000_000_000)

        def interrupted_wait(deadline):
            raise KeyboardInterrupt()

        scheduler = requests.Scheduler(schedule, adapter.launch, clock.read, interrupted_wait)
        with self.assertRaises(KeyboardInterrupt):
            scheduler.run()
        self.assertTrue(adapter.handles[0].cancelled)
        self.assertEqual(len(adapter.handles[0].joins), 1)
        result = requests.audit_requests(schedule, scheduler.report, adapter.effects)
        self.assertIn("launch-conservation", result["failures"])
        self.assertEqual(result["scheduled"], 20)
        self.assertEqual(result["missing"], 19)
        self.assertEqual(result["censored"], 1)

    def test_join_failure_and_changed_final_result_remain_red(self):
        for changed in (False, True):
            with self.subTest(changed=changed):
                schedule, clock = sealed_schedule(4), Clock()
                adapter = Adapter(clock)

                def launch(request):
                    handle = adapter.launch(request)
                    if request.slot.id == schedule.slots[0].id:
                        def join(deadline):
                            if not changed:
                                raise RuntimeError("supervisor cleanup rc125")
                            return replace(handle.poll(clock.read()), rc=125)
                        handle.join = join
                    return handle

                report = requests.Scheduler(schedule, launch, clock.read, clock.wait).run()
                expected = "join-result-changed" if changed else "join-error"
                self.assertEqual(report.launches[0].error, expected)
                self.assertEqual(requests.audit_requests(schedule, report, adapter.effects)["rc"], 1)

    def test_saved_evidence_retains_failed_raw_and_schedule_hash(self):
        schedule, _, adapter, report = self.run_schedule(fail_index=2)
        with tempfile.TemporaryDirectory(prefix="s10-p17-") as root:
            refs = report.save(root)
            self.assertEqual(len(refs), 4)
            for ref in refs:
                data = Path(ref.path).read_bytes()
                self.assertEqual(hashlib.sha256(data).hexdigest(), ref.sha)
                self.assertEqual(len(data), ref.bytes)
            self.assertEqual(Path(root, "schedule.json").read_bytes(), schedule.raw)
            attempts = json.loads(Path(root, "attempts.json").read_bytes())
            self.assertEqual(len(attempts), 20)
            self.assertEqual(attempts[2]["rc"], 75)
            self.assertEqual(attempts[2]["raw"], asdict(raw_ref()))
            with self.assertRaises(FileExistsError):
                report.save(root)
        self.assertEqual(requests.audit_requests(schedule, report, adapter.effects)["rc"], 1)

    def test_schedule_codec_rejects_missing_slots_unknown_fields_and_shared_domains(self):
        schedule = sealed_schedule()
        rows = json.loads(schedule.raw)
        defects = []
        for defect in ("missing", "unknown", "class", "gap", "deadline", "target", "uid", "round-robin"):
            copy = json.loads(schedule.raw)
            if defect == "missing":
                copy.pop(2)
            elif defect == "unknown":
                copy[0]["ignored"] = True
            elif defect == "class":
                copy[0]["class"] = "write"
            elif defect == "gap":
                copy[1]["scheduled_ns"] += 1
                copy[1]["deadline_ns"] += 1
            elif defect == "deadline":
                copy[0]["deadline_ns"] += 1
            elif defect == "target":
                for row in copy:
                    if row["target"]["nid"] == "a1":
                        row["target"]["domain"] = copy[0]["target"]["domain"]
            elif defect == "uid":
                copy[0]["target"]["uid"] = True
            elif defect == "round-robin":
                copy[4]["target"] = copy[1]["target"]
            defects.append(json.dumps(copy).encode())
        defects += [b"[]", json.dumps(rows[:3]).encode(), schedule.raw.replace(b'"class":"read"',
                      b'"class":"write","class":"read"', 1), b"\xff", b"[NaN]"]
        for raw in defects:
            with self.subTest(raw=raw[:40]):
                with self.assertRaises((ValueError, UnicodeError)):
                    requests.load_schedule(raw, hashlib.sha256(raw).hexdigest())
        with self.assertRaises(ValueError):
            requests.load_schedule(schedule.raw, "0" * 64)
        self.assertEqual(len(schedule.slots), 20)

    def test_real_clock_dispatch_and_waiters_complete(self):
        # A short real-timer arm catches accidentally blocked polling while
        # keeping run-all hermetic and independent of any installed binary.
        original = sealed_schedule(4)
        start = requests.boottime_ns() + 20_000_000
        rows = [dict(s.disk(), scheduled_ns=start + i * 100_000_000,
                     deadline_ns=start + i * 100_000_000 + 5_000_000_000)
                for i, s in enumerate(original.slots)]
        raw = json.dumps(rows).encode()
        schedule = requests.load_schedule(raw, hashlib.sha256(raw).hexdigest())

        class RealClock:
            read = staticmethod(requests.boottime_ns)

        adapter = Adapter(RealClock())
        report = requests.Scheduler(schedule, adapter.launch).run()
        self.assertEqual(len(adapter.actual_launches), 4)
        self.assertEqual(requests.audit_requests(schedule, report, adapter.effects)["rc"], 0)


def counter_targets(launches):
    counts = {}
    for request in launches:
        counts[request.slot.target.nid] = counts.get(request.slot.target.nid, 0) + 1
    return counts


def native_schedule_control():
    """P20's M10S hermetic entry: real missing launch, independent fixed seal."""
    schedule, clock = sealed_schedule(), Clock()
    # This digest is independent of the mutated dispatcher and is not derived
    # from its launches. P19 supplies the deployment manifest's corresponding
    # seal; this fixed hermetic seal cannot be promoted to a deploy receipt.
    if schedule.digest != "b8b951775f9f9e0963266060efcde0f428d1ea2e3c8970de3fbe5efbbcb35ae6":
        raise ValueError("M10S expected schedule changed")
    adapter = Adapter(clock, latency=4_990_000_000)
    report = requests.Scheduler(schedule, adapter.launch, clock.read, clock.wait).run()
    result = requests.audit_requests(schedule, report, adapter.effects)
    result.update(tier="hermetic", surface="harness", schedule_sha=schedule.digest,
                  actual_launches=len(adapter.actual_launches),
                  scheduler_sha=hashlib.sha256(Path(requests.__file__).read_bytes()).hexdigest())
    return result


if __name__ == "__main__":
    if sys.argv[1:] == ["--native-control"]:
        result = native_schedule_control()
        print(json.dumps(result, sort_keys=True))
        sys.exit(result["rc"])
    unittest.main()
