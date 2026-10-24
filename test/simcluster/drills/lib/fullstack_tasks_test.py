#!/usr/bin/env python3
"""fullstack_tasks_test.py — hermetic adversarial task/TTY adapter selftests.

These static receipts exercise domain validation and mock the shared-launcher
protocol. They sign no Linux kernel, product, or deployment behavior. P21 owns
the real-binary five-round lifecycle and S4 adapter receipts.
"""

import copy
import hashlib
import json
import os
from pathlib import Path
import signal
import struct
import sys
import tempfile
import unittest
from unittest.mock import patch

# The S10 runner executes every selftest with -I (isolated mode), which drops
# the script directory from sys.path; import the sibling modules explicitly
# instead of depending on whoever happens to launch this file.
# origin: hot-update S10 plan §10.2 P32 / hermetic gate set registration.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import fullstack_tasks as tasks
import fullstack_pty as terminal


TOKEN = "0123456789abcdef0123456789abcdef"
CHILD = {"boot_id": "remote-boot", "pidns": "pid:[40]", "pid": 301, "start_ticks": 60}
CTL = {"boot_id": "local-boot", "pidns": "pid:[41]", "pid": 401, "start_ticks": 70}
PRODUCT = {"boot_id": "remote-boot", "pidns": "pid:[40]", "pid": 201, "start_ticks": 50}


def terminal_fixture():
    return {"device": [1, 2, 3], "termios": [1, 2, 3, 4, 9600, 9600, [0, 4, 3]],
            "status_flags": 2, "fd_flags": 1, "winsize": [24, 80]}


def ctty_fixture(process=CHILD):
    return {"identity": copy.deepcopy(process), "session_id": process["pid"], "pgid": process["pid"],
            "foreground_pgid": process["pid"], "tty_nr": 34816, "terminal": terminal_fixture(),
            "source": "kernel-ctty"}


def task_fixture(kind="pty"):
    resources = [{"id": "task.master", "kind": "master", "ofd": "ofd-master", "owner": "actor-1"},
                 {"id": "task.input", "kind": "input-replay", "ofd": "ofd-in", "owner": "actor-1"},
                 {"id": "task.output", "kind": "output-replay", "ofd": "ofd-out", "owner": "actor-1"}]
    return {"nonce": TOKEN, "kind": kind, "handle_id": "original-handle-digest", "child": copy.deepcopy(CHILD),
            "ctl": copy.deepcopy(CTL), "at_boot_ns": 100, "generation": 1, "state": "running",
            "terminal_id": "", "exit_code": 0, "signal": 0, "lease_deadline_boot_ns": 1000,
            "create_count": 1, "wait_owner": "original-manager", "wait_count": 0, "resources": resources,
            "input_applied": 4, "input_accepted": 7, "stdout_base": 2, "stdout_end": 6,
            "stderr_base": 0, "stderr_end": 0}


def pending_fixture():
    before = {"task": task_fixture(), "product": copy.deepcopy(PRODUCT), "control_seq": 2, "ka_seq": 1,
              "ka_deadline_boot_ns": 800, "input_pending_hex": b"efg".hex(),
              "output_unacked_hex": b"2345".hex(), "master_ofd": "ofd-master",
              "slave_census": {"complete": True, "child": copy.deepcopy(CHILD),
                  "holders": [{"identity": copy.deepcopy(CHILD), "fd": 0, "role": "business"}]},
              "remote_ctty": ctty_fixture()}
    after = copy.deepcopy(before)
    after["task"].update(at_boot_ns=200, generation=2)
    syscall = {"source": "kernel-write", "identity": copy.deepcopy(PRODUCT), "fd": 17, "attempt_len": 4,
               "n": 2, "errno": 11, "start_boot_ns": 80, "end_boot_ns": 90, "sequence": 1,
               "offset": 2, "ofd": "ofd-master", "attempt_sha256": hashlib.sha256(b"cdef").hexdigest()}
    return before, after, syscall, b"abcdefg", b"012345"


def resize_fixture():
    return {"nonce": TOKEN, "child": copy.deepcopy(CHILD), "seq": 3, "ack": 3, "replay_ack": 3,
            "requested": [30, 90], "first_observed": [30, 90], "receiver_changed": [40, 100],
            "after_replay": [40, 100], "commits": 1, "first_boot_ns": 280, "receiver_boot_ns": 290,
            "replay_boot_ns": 300}


def signal_fixture():
    return {"nonce": TOKEN, "child": copy.deepcopy(CHILD), "seq": 4, "ack": 4, "replay_ack": 4,
            "signal": int(signal.SIGINT), "handshake_count": 1, "after_replay_count": 1,
            "commits": 1, "handshake_boot_ns": 320, "replay_boot_ns": 330}


def wait_fixture():
    return {"identity": copy.deepcopy(CTL), "source": "shared-supervisor-waitpid", "wait_calls": 1,
            "exit_code": 23, "signal": 0, "native_rc": 0, "joined": True}


class ProtocolOwner:
    """Hermetic protocol double, never attached to a real product fixture."""

    def __init__(self):
        self.started = {"identity": copy.deepcopy(CTL)}
        self.result = wait_fixture()
        self.sent, self.waits, self.cancels = [], [], 0

    def send(self, data):
        self.sent.append(data)

    def cancel(self):
        self.cancels += 1

    def waiter(self, deadline_ns):
        self.waits.append(deadline_ns)
        return copy.deepcopy(self.result)


# origin: hot-update S10 plan §10.2 P13 / §6 / FullstackTaskPTYTest.
class FullstackTaskPTYTest(unittest.TestCase):
    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_cohort_predeclares_four_runtimes_and_six_original_tasks(self):
        tokens = [f"{number:032x}" for number in range(6)]
        cohort = tasks.original_cohort(17_000_000_000_000, tokens)
        self.assertEqual([(p.kind, p.runtime) for p in cohort],
                         [("exec", "a0"), ("exec", "a1"), ("exec", "a2"), ("exec", "a3"),
                          ("pty", "a0"), ("pty", "a2")])
        for plan in cohort:
            self.assertGreaterEqual(plan.records * plan.period_ns, plan.ceiling_ns + plan.drain_records * plan.period_ns)
            self.assertGreater(plan.records, 2400)
        for bad in (tokens[:-1], tokens[:-1] + [tokens[0]]):
            with self.assertRaises(tasks.EvidenceError):
                tasks.original_cohort(10, bad)
        for kwargs in ({"kind": "legacy"}, {"runtime": "agt1"}, {"nonce": ""}, {"ceiling_ns": 0},
                       {"period_ns": True}, {"drain_records": 0}):
            args = dict(task_id="exec-a0", runtime="a0", kind="exec", nonce=TOKEN, ceiling_ns=10)
            args.update(kwargs)
            with self.assertRaises(tasks.EvidenceError):
                tasks.TaskPlan(**args)

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_raw_frames_preserve_order_digest_and_unconsumed_suffix(self):
        packets = [tasks.frame(TOKEN, "stdout", seq, data)
                   for seq, data in enumerate((b"\x00\r\n\x03", b"tail\x04"))]
        raw = b"".join(packets)
        self.assertEqual(tasks.decode_frames(raw, TOKEN, "stdout"), ([b"\x00\r\n\x03", b"tail\x04"], b""))
        for corrupt in (raw[:-1], packets[0] * 2, packets[1] + packets[0], raw[:-1] + b"x",
                        tasks.frame("f" * 32, "stdout", 0, b"foreign"), raw + b"\x00"):
            with self.assertRaises(tasks.EvidenceError):
                tasks.decode_frames(corrupt, TOKEN, "stdout")
        records, pending = tasks.decode_frames(raw[:-1], TOKEN, "stdout", complete=False)
        self.assertEqual(records, [b"\x00\r\n\x03"])
        self.assertEqual(pending, packets[1][:-1])
        self.assertEqual(tasks.decode_frames(packets[1], TOKEN, "stdout", first_sequence=1)[0], [b"tail\x04"])
        for corrupt in (True, -1, 1 << 64):
            with self.assertRaises(tasks.EvidenceError):
                tasks.frame(TOKEN, "stdout", corrupt, b"data")

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_raw_receiver_must_be_independent_and_exact(self):
        raw = b"".join(tasks.frame(TOKEN, "input", seq, b"DATA:\r\n\x00") for seq in range(2))
        with tempfile.TemporaryDirectory(prefix="fullstack-task-raw-") as tmp:
            root = Path(tmp)
            sent, received = root / "input.sent", root / "input.received"
            sent.write_bytes(raw)
            received.write_bytes(raw)
            tasks.verify_raw_pair(root, TOKEN, "input")
            for corrupt in (raw[:-1], raw + raw, raw.replace(b"\r\n", b"\n")):
                received.write_bytes(corrupt)
                with self.assertRaisesRegex(tasks.EvidenceError, "bytes differ"):
                    tasks.verify_raw_pair(root, TOKEN, "input")
            received.unlink()
            os.link(sent, received)
            with self.assertRaisesRegex(tasks.EvidenceError, "substituted"):
                tasks.verify_raw_pair(root, TOKEN, "input")
            received.unlink()
            received.symlink_to(sent)
            with self.assertRaisesRegex(tasks.EvidenceError, "unsafe"):
                tasks.verify_raw_pair(root, TOKEN, "input")

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_original_handle_terminal_retention_and_lease_survive_reverse(self):
        live = task_fixture("exec")
        active = copy.deepcopy(live)
        active.update(at_boot_ns=200, generation=2)
        unacked = copy.deepcopy(active)
        unacked.update(at_boot_ns=300, generation=3, state="terminal-unacked", terminal_id="term-1",
                       exit_code=23, wait_count=1)
        settled = copy.deepcopy(unacked)
        settled.update(at_boot_ns=400, state="settled-unexpired", resources=[], stdout_base=6)
        expired = copy.deepcopy(settled)
        expired.update(at_boot_ns=1000, state="expired")
        chain = [live, active, unacked, settled, expired]
        tasks.verify_task_chain(chain)
        mutations = {
            "handle changed": lambda c: c[1].update(handle_id="replacement"),
            "child replaced": lambda c: c[1]["child"].update(start_ticks=61),
            "ctl replaced": lambda c: c[1]["ctl"].update(pid=402),
            "duplicate create": lambda c: c[1].update(create_count=2),
            "dual wait owner": lambda c: c[1].update(wait_owner="other-manager"),
            "dual wait": lambda c: c[2].update(wait_count=2),
            "terminal replaced": lambda c: c[3].update(terminal_id="term-2"),
            "exit rewritten": lambda c: c[3].update(exit_code=0),
            "kind erased": lambda c: c[3].update(kind=""),
            "lease renewed": lambda c: c[3].update(lease_deadline_boot_ns=1001),
            "collected early": lambda c: c[4].update(at_boot_ns=999),
            "retained FD leak": lambda c: c[3].update(resources=copy.deepcopy(c[0]["resources"])),
            "live source lost": lambda c: c[1].update(resources=[]),
            "terminal output lost": lambda c: c[2].update(resources=[]),
            "resource reopened": lambda c: c[1]["resources"][0].update(ofd="new-ofd"),
            "duplicate resource": lambda c: c[1]["resources"].append(copy.deepcopy(c[1]["resources"][0])),
            "offset regressed": lambda c: c[1].update(input_applied=3),
            "stale sample": lambda c: c[1].update(at_boot_ns=100),
            "unknown field": lambda c: c[1].update(unknown=True),
            "boolean offset": lambda c: c[1].update(input_applied=True),
        }
        for name, mutate in mutations.items():
            with self.subTest(defect=name):
                bad = copy.deepcopy(chain)
                mutate(bad)
                with self.assertRaises(tasks.EvidenceError):
                    tasks.verify_task_chain(bad)
        held = copy.deepcopy(live)
        held.update(state="reap-held", at_boot_ns=200)
        tasks.verify_task_chain([live, held])
        held["wait_count"] = 1
        with self.assertRaisesRegex(tasks.EvidenceError, "sole-wait"):
            tasks.verify_task_chain([live, held])

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_pending_cut_requires_kernel_partial_and_original_bytes(self):
        good = pending_fixture()
        terminal.verify_pending_cut(*good)
        mutations = {
            "synthetic write": lambda v: v[2].update(source="synthetic-partial"),
            "zero write": lambda v: v[2].update(n=0),
            "wrong product": lambda v: v[2]["identity"].update(pid=202),
            "wrong ofd": lambda v: v[2].update(ofd="reopened"),
            "old syscall": lambda v: v[2].update(end_boot_ns=101),
            "applied plus one": lambda v: v[1]["task"].update(input_applied=5),
            "pending lost": lambda v: v[1].update(input_pending_hex=""),
            "pending wrong bytes": lambda v: v[1].update(input_pending_hex=b"bad".hex()),
            "output unacked lost": lambda v: v[1].update(output_unacked_hex=""),
            "new master": lambda v: v[1].update(master_ofd="replacement"),
            "raw input digest differs": lambda v: v[2].update(attempt_sha256="f" * 64),
            "duplicate stderr": lambda v: v[1]["task"].update(stderr_end=1),
            "pending resource owner replaced": lambda v: v[1]["task"]["resources"][0].update(owner="actor-2"),
            "pending master reopened": lambda v: v[1]["task"]["resources"][0].update(ofd="reopened"),
            "pending lease renewed": lambda v: v[1]["task"].update(lease_deadline_boot_ns=1001),
        }
        for name, mutate in mutations.items():
            with self.subTest(defect=name):
                bad = list(copy.deepcopy(good))
                mutate(bad)
                with self.assertRaises(tasks.EvidenceError):
                    terminal.verify_pending_cut(*bad)
        # A full syscall may leave later accepted packets pending. That is not
        # the required actual short syscall, even though A<Q still holds.
        before, after, syscall, inp, out = pending_fixture()
        syscall["n"] = syscall["attempt_len"]
        for snapshot in (before, after):
            snapshot["task"]["input_applied"] = 6
            snapshot["input_pending_hex"] = b"g".hex()
        with self.assertRaisesRegex(tasks.EvidenceError, "no actual partial"):
            terminal.verify_pending_cut(before, after, syscall, inp, out)

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_resize_replay_preserves_receiver_change(self):
        terminal.verify_resize_replay(resize_fixture(), TOKEN, CHILD, 3)
        for changes in ({"after_replay": [30, 90]}, {"receiver_changed": [30, 90]}, {"commits": 2},
                        {"first_observed": [31, 90]}, {"replay_ack": 4}, {"seq": True}, {"requested": [0, 90]},
                        {"receiver_boot_ns": 310}, {"nonce": "f" * 32}, {"child": PRODUCT}):
            bad = resize_fixture()
            bad.update(changes)
            with self.subTest(changes=changes), self.assertRaises(tasks.EvidenceError):
                terminal.verify_resize_replay(bad, TOKEN, CHILD, 3)

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_signal_replay_requires_original_child_handshake_once(self):
        terminal.verify_signal_replay(signal_fixture(), TOKEN, CHILD, 4)
        for changes in ({"handshake_count": 0}, {"after_replay_count": 2}, {"commits": 2},
                        {"handshake_boot_ns": 340}, {"signal": 9}, {"ack": 5}, {"handshake_count": True},
                        {"nonce": "f" * 32}, {"child": PRODUCT}, {"seq": 5, "ack": 5, "replay_ack": 5}):
            bad = signal_fixture()
            bad.update(changes)
            with self.subTest(changes=changes), self.assertRaises(tasks.EvidenceError):
                terminal.verify_signal_replay(bad, TOKEN, CHILD, 4)

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_keepalive_deadline_survives_activation(self):
        before, after, syscall, inp, out = pending_fixture()
        after["ka_deadline_boot_ns"] += 1
        with self.assertRaisesRegex(tasks.EvidenceError, "ka_deadline_boot_ns"):
            terminal.verify_pending_cut(before, after, syscall, inp, out)
        after["ka_deadline_boot_ns"] = before["ka_deadline_boot_ns"]
        terminal.verify_pending_cut(before, after, syscall, inp, out)

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_remote_slave_keeper_is_rejected(self):
        before, after, syscall, inp, out = pending_fixture()
        before["slave_census"]["holders"].append({"identity": copy.deepcopy(CHILD), "fd": 5, "role": "fixture"})
        with self.assertRaisesRegex(tasks.EvidenceError, "slave keeper"):
            terminal.verify_pending_cut(before, after, syscall, inp, out)
        for changed in ({"complete": False}, {"holders": []}):
            sample = pending_fixture()[0]
            sample["slave_census"].update(changed)
            with self.assertRaises(tasks.EvidenceError):
                terminal.pty_sample(sample)
        bad = pending_fixture()[0]
        bad["slave_census"]["holders"][0]["identity"]["pid"] += 1
        with self.assertRaisesRegex(tasks.EvidenceError, "undeclared"):
            terminal.pty_sample(bad)

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_ctl_waitpid_and_terminal_restoration_are_independent(self):
        tasks.validate_wait(wait_fixture(), CTL)
        terminal.verify_restoration(terminal_fixture(), terminal_fixture())
        for changes in ({"source": "master-eof"}, {"wait_calls": 2}, {"joined": False},
                        {"exit_code": 0}, {"signal": 15}, {"native_rc": 125}, {"wait_calls": True}):
            bad = wait_fixture()
            bad.update(changes)
            with self.subTest(changes=changes), self.assertRaises(tasks.EvidenceError):
                tasks.validate_wait(bad, CTL)
        bad = wait_fixture()
        bad["identity"]["start_ticks"] += 1
        with self.assertRaisesRegex(tasks.EvidenceError, "replacement ctl"):
            tasks.validate_wait(bad, CTL)
        for key, replacement in (("termios", [0] * 6 + [[0]]), ("fd_flags", 0), ("status_flags", 3),
                                 ("device", [1, 2, 4])):
            bad = terminal_fixture()
            bad[key] = replacement
            with self.subTest(field=key), self.assertRaisesRegex(tasks.EvidenceError, "restore original"):
                terminal.verify_restoration(terminal_fixture(), bad)
        for changes in ({"tty_nr": 0}, {"source": "ignored-TIOCSCTTY"}, {"session_id": 2},
                        {"foreground_pgid": 2}, {"tty_nr": True}):
            bad = ctty_fixture(CTL)
            bad.update(changes)
            with self.assertRaises(tasks.EvidenceError):
                terminal.validate_ctty(bad, CTL)

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_supervised_ctl_joins_returned_waiter_and_keeps_cleanup_failure(self):
        request = tasks.LaunchRequest(("/fixture/tether-supplied", "exec"), "/fixture", {}, 0, 1, 2, None, "lease-1")
        owner = ProtocolOwner()
        task = tasks.SupervisedTask(request, lambda _: owner, 1000)
        task.send(TOKEN, b"DATA:prefix")
        task.stop(TOKEN)
        self.assertEqual(tasks.decode_frames(b"".join(owner.sent), TOKEN, "input")[0], [b"DATA:prefix", b"STOP"])
        task.join(1000)
        self.assertEqual(owner.waits, [1000])
        with self.assertRaisesRegex(tasks.EvidenceError, "already joined"):
            task.join(1000)
        for rc in (125, 1):
            owner = ProtocolOwner()
            owner.result["native_rc"] = rc
            task = tasks.SupervisedTask(request, lambda _: owner, 1000)
            with self.assertRaisesRegex(tasks.EvidenceError, "cleanup failed"):
                task.join(1000)
            self.assertEqual(owner.waits, [1000])
            self.assertEqual(task.receipt["native_rc"], rc)
        owner = ProtocolOwner()
        owner.started["identity"] = {}
        with self.assertRaises(tasks.EvidenceError):
            tasks.SupervisedTask(request, lambda _: owner, 1000)
        self.assertEqual((owner.cancels, owner.waits), (1, [1000]))
        owner.result["native_rc"] = 125
        with self.assertRaises(ExceptionGroup) as caught:
            tasks.SupervisedTask(request, lambda _: owner, 1000)
        self.assertIn("supervisor cleanup failed", str(caught.exception.exceptions[1]))
        owner = ProtocolOwner()
        task = tasks.SupervisedTask(request, lambda _: owner, 1000)
        task.abort(1000)
        self.assertEqual((owner.cancels, owner.waits), (1, [1000]))
        owner = ProtocolOwner()
        del owner.started
        with self.assertRaises(AttributeError):
            tasks.SupervisedTask(request, lambda _: owner, 1000)
        self.assertEqual((owner.cancels, owner.waits), (1, [1000]))
        owner = ProtocolOwner()
        owner.send = None
        with self.assertRaisesRegex(tasks.EvidenceError, "owner is incomplete"):
            tasks.SupervisedTask(request, lambda _: owner, 1000)
        self.assertEqual((owner.cancels, owner.waits), (1, [1000]))
        owner = ProtocolOwner()
        task = tasks.SupervisedTask(request, lambda _: owner, 1000)
        with patch.object(owner, "cancel", side_effect=RuntimeError("injected cancellation failure")):
            with self.assertRaises(ExceptionGroup):
                task.abort(1000)
        self.assertEqual(owner.waits, [1000])

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_private_launch_state_and_missing_supervisor_fail_before_start(self):
        request = dict(argv=("/fixture/tether-supplied",), cwd="/fixture", env={}, stdin_fd=0,
                       stdout_fd=1, stderr_fd=2, controlling_tty_fd=None, remote_lease="lease-1")
        for changes in ({"argv": ("tether",)}, {"remote_lease": ""}, {"stdin_fd": True},
                        {"env": {"TETHER_TEST_DRILL_CUSTODY_FD": "7"}},
                        {"env": {"TETHER_HANDOFF_FD": "8"}}, {"env": {"_TETHER_HANDOFF": "private"}},
                        {"env": {"_TETHER_SPAWN_EXEC_HELPER": "private"}}, {"env": {"A": "\0"}}):
            bad = dict(request)
            bad.update(changes)
            with self.assertRaises(tasks.EvidenceError):
                tasks.LaunchRequest(**bad)
        with self.assertRaisesRegex(tasks.EvidenceError, "P05"):
            tasks.SupervisedTask(tasks.LaunchRequest(**request), None, 1000)

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_duplicate_and_nonfinite_json_cannot_enter_receipts(self):
        with tempfile.TemporaryDirectory(prefix="fullstack-task-json-") as tmp:
            path = Path(tmp) / "raw.json"
            path.write_text(json.dumps(wait_fixture()))
            self.assertEqual(tasks.strict_json(path), wait_fixture())
            for raw in ('{"rc":0,"rc":125}', '{"rc":NaN}', '{"rc":Infinity}'):
                path.write_text(raw)
                with self.assertRaises(tasks.EvidenceError):
                    tasks.strict_json(path)

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_producer_records_actual_write_prefix_and_requires_stop(self):
        plan = tasks.TaskPlan("exec-a0", "a0", "exec", TOKEN, 100, period_ns=1, drain_records=2)
        writes = {1: bytearray(), 2: bytearray()}

        def short_write(fd, data):
            count = min(3, len(data))
            writes[fd].extend(data[:count])
            return count

        # System calls are mocked only here; these receipts stay hermetic.
        with tempfile.TemporaryDirectory(prefix="fullstack-task-producer-") as tmp:
            root = Path(tmp) / "producer"
            with patch.object(tasks, "process_identity", return_value=CHILD), \
                 patch.object(tasks, "boot_ns", return_value=10), \
                 patch.object(tasks.os, "set_blocking"), \
                 patch.object(tasks.os, "read", return_value=tasks.frame(TOKEN, "input", 0, b"STOP")), \
                 patch.object(tasks.os, "write", side_effect=short_write), \
                 patch.object(tasks.select, "select", side_effect=[([], [], []), ([0], [], [])]):
                self.assertEqual(tasks.produce(plan, root), 23)
            for name, fd in (("stdout", 1), ("stderr", 2)):
                raw = (root / (name + ".sent")).read_bytes()
                self.assertEqual(raw, bytes(writes[fd]))
                records = tasks.decode_frames(raw, TOKEN, name)[0]
                self.assertEqual(len(records), 1)
                self.assertTrue(records[0].endswith(b"\r\n\x00\x03\x04"))
            lifecycle = [json.loads(line) for line in (root / "lifecycle.jsonl").read_text().splitlines()]
            self.assertEqual([row["event"] for row in lifecycle], ["started", "stopped"])
            self.assertEqual(lifecycle[-1]["identity"], CHILD)
            with patch.object(tasks, "process_identity", return_value=CHILD), \
                 patch.object(tasks, "boot_ns", return_value=10), patch.object(tasks.os, "set_blocking"), \
                 patch.object(tasks.os, "read", return_value=b""), \
                 patch.object(tasks.select, "select", return_value=([0], [], [])):
                with self.assertRaisesRegex(tasks.EvidenceError, "before STOP"):
                    tasks.produce(plan, Path(tmp) / "early-eof")

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_local_tty_adapter_keeps_original_waiter_through_partial_start(self):
        for bad_ready in (False, True):
            owner = ProtocolOwner()
            owner.started = ctty_fixture(CTL)
            if bad_ready:
                owner.started["tty_nr"] = 0
            closed = []
            with patch.object(terminal.os, "openpty", return_value=(41, 42)), \
                 patch.object(terminal.os, "set_blocking"), \
                 patch.object(terminal.os, "close", side_effect=closed.append), \
                 patch.object(terminal.fcntl, "ioctl"), \
                 patch.object(terminal, "terminal_state", side_effect=lambda _: terminal_fixture()), \
                 patch.object(terminal, "process_identity", return_value=CTL), \
                 patch.object(terminal, "boot_ns", return_value=10), \
                 patch.object(terminal.os, "write", return_value=3):
                local = terminal.DualTTY()
                if bad_ready:
                    with self.assertRaisesRegex(tasks.EvidenceError, "TIOCSCTTY"):
                        local.start(("/fixture/tether-supplied", "run"), "/fixture", {}, "lease-1", lambda _: owner, 1000)
                    self.assertEqual((owner.cancels, owner.waits), (1, [1000]))
                else:
                    task = local.start(("/fixture/tether-supplied", "run"), "/fixture", {}, "lease-1", lambda _: owner, 1000)
                    with self.assertRaisesRegex(tasks.EvidenceError, "unjoined"):
                        local.close()
                    receipt = local.write(b"abcdef")
                    self.assertEqual((receipt["attempt_len"], receipt["n"], receipt["sequence"]), (6, 3, 1))
                    task.stop(TOKEN)
                    task.join(1000)
                    self.assertEqual(local.restored(), terminal_fixture())
                local.close()
                local.close()
            self.assertEqual(closed, [41, 42])

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_producer_cannot_extend_predeclared_supply(self):
        plan = tasks.TaskPlan("exec-a0", "a0", "exec", TOKEN, 1, period_ns=1, drain_records=1)
        clock = iter(range(100))
        with tempfile.TemporaryDirectory(prefix="fullstack-task-supply-") as tmp:
            with patch.object(tasks, "process_identity", return_value=CHILD), \
                 patch.object(tasks, "boot_ns", side_effect=lambda: next(clock)), \
                 patch.object(tasks.os, "set_blocking"), \
                 patch.object(tasks.os, "write", side_effect=lambda _fd, data: len(data)), \
                 patch.object(tasks.select, "select", return_value=([], [], [])):
                with self.assertRaisesRegex(tasks.EvidenceError, "supply exhausted before seal"):
                    tasks.produce(plan, Path(tmp) / "producer")

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_receiver_resize_uses_its_original_terminal_and_records_effect(self):
        plan = tasks.TaskPlan("pty-a0", "a0", "pty", TOKEN, 100)
        commands = tasks.frame(TOKEN, "input", 0, b"RESIZE:40:100") + tasks.frame(TOKEN, "input", 1, b"STOP")

        def ioctl(_fd, op, _value):
            return struct.pack("HHHH", 40, 100, 0, 0) if op == terminal.termios.TIOCGWINSZ else 0

        with tempfile.TemporaryDirectory(prefix="fullstack-pty-receiver-") as tmp:
            root = Path(tmp) / "producer"
            with patch.object(tasks, "process_identity", return_value=CHILD), \
                 patch.object(tasks, "boot_ns", return_value=10), patch.object(tasks.os, "set_blocking"), \
                 patch.object(tasks.os, "read", return_value=commands), \
                 patch.object(tasks.select, "select", return_value=([0], [], [])), \
                 patch.object(terminal.fcntl, "ioctl", side_effect=ioctl) as observed:
                self.assertEqual(tasks.produce(plan, root), 23)
            observed.assert_any_call(0, terminal.termios.TIOCSWINSZ, struct.pack("HHHH", 40, 100, 0, 0))
            receipt = json.loads((root / "controls.jsonl").read_text())
            self.assertEqual((receipt["event"], receipt["identity"], receipt["winsize"]),
                             ("receiver-resize", CHILD, [40, 100]))

    # origin: hot-update S10 plan §6 / §10.2 P13 / FullstackTaskPTYTest.
    def test_remote_child_restores_terminal_on_normal_and_failed_producer(self):
        plan = tasks.TaskPlan("pty-a0", "a0", "pty", TOKEN, 100)
        attrs = [1, 2, 3, 4, 9600, 9600, [b"\x04", b"\x03"]]
        for failed in (False, True):
            handlers = {number: signal.SIG_DFL for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}

            def install(number, handler):
                original = handlers[number]
                handlers[number] = handler
                return original

            def producer(_plan, root, ready):
                Path(root).mkdir()
                ready(Path(root))
                handlers[signal.SIGINT](int(signal.SIGINT), None)
                if failed:
                    raise tasks.EvidenceError("injected producer failure")
                return 23

            with tempfile.TemporaryDirectory(prefix="fullstack-pty-child-") as tmp:
                root = Path(tmp) / "producer"
                with patch.object(terminal, "controlling_tty", return_value=ctty_fixture()), \
                     patch.object(terminal, "process_identity", return_value=CHILD), \
                     patch.object(terminal, "boot_ns", return_value=10), \
                     patch.object(terminal.termios, "tcgetattr", return_value=attrs), \
                     patch.object(terminal.termios, "tcsetattr") as restored, \
                     patch.object(terminal.tty, "setraw"), \
                     patch.object(terminal.signal, "signal", side_effect=install), \
                     patch.object(terminal, "produce", side_effect=producer):
                    if failed:
                        with self.assertRaisesRegex(tasks.EvidenceError, "injected producer failure"):
                            terminal.remote_produce(plan, root)
                    else:
                        self.assertEqual(terminal.remote_produce(plan, root), 23)
                    restored.assert_called_once_with(0, terminal.termios.TCSANOW, attrs)
                self.assertEqual(handlers, {number: signal.SIG_DFL for number in handlers})
                receipts = [json.loads(line) for line in (root / "controls.jsonl").read_text().splitlines()]
                self.assertEqual([r["event"] for r in receipts], ["ctty-ready", "signal-handshake"])
                self.assertEqual((receipts[-1]["identity"], receipts[-1]["count"]), (CHILD, 1))


if __name__ == "__main__":
    unittest.main(verbosity=2)
