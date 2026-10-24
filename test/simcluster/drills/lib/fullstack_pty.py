#!/usr/bin/env python3
"""fullstack_pty.py — dual controlling terminals and nonempty PTY seams.

DualTTY owns only the local ctl PTY. P05's injected launcher must establish the
ctl session/controlling terminal and return its kernel handshake before READY.
The remote business process uses the product-created PTY; this module never
opens its slave. P21 joins passive product checkpoints to kernel syscall,
receiver/control, and original ctl wait receipts before checking these seams.
The remote producer is invoked by real run, with this file supplied by P10/P11.
"""

import argparse
import hashlib
import fcntl
import os
from pathlib import Path
import signal
import struct
import sys
import termios
import tty

# Run as a script under `python3 -I` the script directory is not on sys.path;
# the sibling import must not depend on how the producer happens to be launched.
# origin: hot-update S10 plan §10.2 P13; same pattern as flow_open_fault.py.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fullstack_tasks import (EvidenceError, LaunchRequest, SupervisedTask, TaskPlan,
                             append_receipt, boot_ns, exact, identity, nonce,
                             process_identity, produce, require, task_sample, uint, validate_join, verify_task_chain)


def terminal_state(fd):
    stat = os.fstat(fd)
    attrs = termios.tcgetattr(fd)
    # Convert cc bytes explicitly so JSON roundtrips preserve their values.
    attrs[6] = [v[0] if isinstance(v, bytes) else v for v in attrs[6]]
    rows, cols, _, _ = struct.unpack("HHHH", fcntl.ioctl(fd, termios.TIOCGWINSZ, bytes(8)))
    return {"device": [stat.st_dev, stat.st_ino, stat.st_rdev], "termios": attrs,
            "status_flags": fcntl.fcntl(fd, fcntl.F_GETFL), "fd_flags": fcntl.fcntl(fd, fcntl.F_GETFD),
            "winsize": [rows, cols]}


def controlling_tty(fd=0):
    """Kernel receipt from the process that actually acquired the terminal."""
    require(os.isatty(fd), "process has no tty")
    raw = Path("/proc/self/stat").read_text()
    fields = raw[raw.rfind(")") + 2:].split()
    return {"identity": process_identity(), "session_id": os.getsid(0),
            "foreground_pgid": os.tcgetpgrp(fd), "pgid": os.getpgrp(),
            "tty_nr": int(fields[4]), "terminal": terminal_state(fd), "source": "kernel-ctty"}


def validate_ctty(receipt, original):
    exact(receipt, ("identity", "session_id", "foreground_pgid", "pgid", "tty_nr", "terminal", "source"),
          "controlling terminal")
    require(identity(receipt["identity"]) == identity(original) and receipt["source"] == "kernel-ctty",
            "controlling terminal belongs to another process")
    for key in ("session_id", "foreground_pgid", "pgid"):
        uint(receipt[key], key, True)
    require(type(receipt["tty_nr"]) is int and receipt["tty_nr"] != 0, "TIOCSCTTY was not established")
    require(receipt["session_id"] == original["pid"] and receipt["pgid"] == receipt["foreground_pgid"],
            "ctl did not own its controlling session")
    validate_terminal(receipt["terminal"])


def validate_terminal(state):
    exact(state, ("device", "termios", "status_flags", "fd_flags", "winsize"), "terminal state")
    require(type(state["device"]) is list and len(state["device"]) == 3, "terminal identity missing")
    for value in state["device"]:
        uint(value, "terminal device")
    attrs = state["termios"]
    require(type(attrs) is list and len(attrs) == 7 and type(attrs[6]) is list and attrs[6],
            "termios receipt missing")
    for value in attrs[:6] + attrs[6]:
        uint(value, "termios value")
    for key in ("status_flags", "fd_flags"):
        uint(state[key], key)
    size(state["winsize"])


def size(value):
    require(type(value) is list and len(value) == 2 and all(type(n) is int and 0 < n <= 65535 for n in value),
            "invalid winsize")
    return value


def verify_restoration(before, after):
    # origin: hot-update S10 plan §6 / FullstackTaskPTYTest. Restoring a new
    # terminal or matching only lflag cannot prove the original ctl's cleanup.
    validate_terminal(before)
    validate_terminal(after)
    for key in ("device", "termios", "status_flags", "fd_flags"):
        require(before[key] == after[key], "ctl did not restore original " + key)


class DualTTY:
    """Local master/slave lifetime; keep the local slave until ctl Wait finishes.

    No remote slave keeper exists here. Local custody is necessary to read the
    original terminal attributes after the original ctl has restored and exited.
    close refuses a live owner: the caller must stop/join or abort/join first.
    """

    def __init__(self):
        self.master, self.slave = os.openpty()
        self.task, self.closed, self.calls = None, False, []
        try:
            fcntl.ioctl(self.slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
            self.before = terminal_state(self.slave)
            os.set_blocking(self.master, False)
        except BaseException:
            os.close(self.master)
            os.close(self.slave)
            raise

    def start(self, argv, cwd, env, remote_lease, start_supervised, deadline_ns):
        require(not self.closed and self.task is None, "local PTY already started or closed")
        request = LaunchRequest(tuple(argv), cwd, env, self.slave, self.slave, self.slave,
                                self.slave, remote_lease)
        self.task = SupervisedTask(request, start_supervised, deadline_ns)
        # Store the owner before checking READY so an invalid ctty still has a
        # returned waiter. The caller can abort/join through that owner.
        try:
            validate_ctty(self.task.owner.started, self.task.original)
            require(self.task.owner.started["terminal"]["device"] == self.before["device"],
                    "launcher supplied a different local terminal")
        except EvidenceError as setup_error:
            try:
                self.task.abort(deadline_ns)
            except Exception as cleanup_error:
                raise ExceptionGroup("controlling tty and cleanup failed", [setup_error, cleanup_error])
            raise
        return self.task

    def write(self, data):
        require(not self.closed and type(data) is bytes and data, "invalid local PTY write")
        start = boot_ns()
        try:
            count, error = os.write(self.master, data), 0
        except BlockingIOError as exc:
            count, error = 0, exc.errno
        receipt = {"source": "os.write", "identity": process_identity(), "fd": self.master,
                   "attempt_len": len(data), "n": count, "errno": error, "start_boot_ns": start,
                   "end_boot_ns": boot_ns(), "sequence": len(self.calls) + 1}
        self.calls.append(receipt)
        return receipt

    def read(self, limit=32768):
        require(not self.closed and type(limit) is int and 0 < limit <= 32768, "invalid local PTY read")
        return os.read(self.master, limit)

    def restored(self):
        require(self.task is not None and self.task.receipt is not None, "original ctl has not been waited")
        validate_join(self.task.receipt, self.task.original)
        after = terminal_state(self.slave)
        verify_restoration(self.before, after)
        return after

    def close(self):
        if self.closed:
            return
        require(self.task is None or self.task.receipt is not None, "local PTY still has an unjoined owner")
        # Close both even if closing the first descriptor fails; retain failure.
        errors = []
        for fd in (self.master, self.slave):
            try:
                os.close(fd)
            except OSError as exc:
                errors.append(exc)
        self.closed = True
        require(not errors, "local PTY descriptor cleanup failed")


def verify_slave_census(census, child):
    exact(census, ("complete", "child", "holders"), "remote slave census")
    require(type(census["complete"]) is bool and census["complete"], "unknown remote slave holders")
    require(identity(census["child"]) == identity(child), "slave census belongs to another child")
    require(type(census["holders"]) is list and census["holders"], "empty remote slave census")
    for holder in census["holders"]:
        exact(holder, ("identity", "fd", "role"), "remote slave holder")
        identity(holder["identity"])
        uint(holder["fd"], "slave holder fd")
        # Only business processes may hold the slave. Fixture/agent/helper
        # custody changes last-slave EOF and can hide premature master close.
        require(holder["role"] == "business", "remote slave keeper masks last-slave EOF")
        require(holder["identity"] == child, "undeclared remote slave holder")


PTY_FIELDS = ("task", "product", "control_seq", "ka_seq", "ka_deadline_boot_ns", "input_pending_hex",
              "output_unacked_hex", "master_ofd", "slave_census", "remote_ctty")


def pty_sample(sample):
    exact(sample, PTY_FIELDS, "PTY checkpoint")
    task_sample(sample["task"])
    identity(sample["product"])
    require(sample["task"]["kind"] == "pty", "checkpoint is not a PTY")
    for key in ("control_seq", "ka_seq", "ka_deadline_boot_ns"):
        uint(sample[key], key)
    require(sample["ka_seq"] <= sample["control_seq"], "KA sequence exceeds control")
    require(isinstance(sample["master_ofd"], str) and sample["master_ofd"], "original PTY OFD missing")
    for key in ("input_pending_hex", "output_unacked_hex"):
        require(isinstance(sample[key], str), "pending bytes missing")
        try:
            data = bytes.fromhex(sample[key])
        except ValueError as exc:
            raise EvidenceError("invalid pending byte encoding") from exc
        require(data.hex() == sample[key], "noncanonical pending byte encoding")
    task = sample["task"]
    require(len(bytes.fromhex(sample["input_pending_hex"])) == task["input_accepted"] - task["input_applied"],
            "pending suffix differs from product offsets")
    require(task["input_accepted"] - task["input_applied"] <= 256 * 1024,
            "PTY pending input exceeds its real replay capacity")
    require(len(bytes.fromhex(sample["output_unacked_hex"])) == task["stdout_end"] - task["stdout_base"],
            "unacknowledged output differs from product offsets")
    require(task["stdout_end"] - task["stdout_base"] <= 8 * 1024 * 1024, "PTY output exceeds replay capacity")
    require(task["stderr_base"] == task["stderr_end"] == 0, "PTY acquired a separate stderr owner")
    verify_slave_census(sample["slave_census"], task["child"])
    validate_ctty(sample["remote_ctty"], task["child"])
    return sample


def verify_pending_cut(before, after, syscall, accepted_input, output_read):
    # origin: hot-update S10 plan §6 / FullstackTaskPTYTest. Actual partial
    # syscall plus nonempty pending input and unACKed output is the seam witness.
    pty_sample(before)
    pty_sample(after)
    verify_task_chain([before["task"], after["task"]])
    require(before["task"]["state"] in ("running", "reap-held") and
            after["task"]["state"] == before["task"]["state"], "pending cut changed live task state")
    exact(syscall, ("source", "identity", "fd", "attempt_len", "n", "errno", "start_boot_ns",
                    "end_boot_ns", "sequence", "offset", "ofd", "attempt_sha256"), "PTY write syscall")
    require(syscall["source"] == "kernel-write" and syscall["identity"] == before["product"],
            "partial write needs a real product syscall witness")
    for key in ("fd", "n", "errno", "offset"):
        uint(syscall[key], key)
    for key in ("attempt_len", "start_boot_ns", "end_boot_ns", "sequence"):
        uint(syscall[key], key, True)
    require(syscall["start_boot_ns"] <= syscall["end_boot_ns"] <= before["task"]["at_boot_ns"],
            "partial syscall is outside the frozen prefix")
    require(0 < syscall["n"] < syscall["attempt_len"], "no actual partial PTY write")
    require(syscall["ofd"] == before["master_ofd"] == after["master_ofd"], "original master OFD changed")
    require(type(accepted_input) is bytes and type(output_read) is bytes, "raw syscall bytes missing")
    attempt = accepted_input[syscall["offset"]:syscall["offset"] + syscall["attempt_len"]]
    require(len(attempt) == syscall["attempt_len"] and hashlib.sha256(attempt).hexdigest() == syscall["attempt_sha256"],
            "partial syscall payload differs from raw input")
    task = before["task"]
    require(task["input_applied"] == syscall["offset"] + syscall["n"] and
            task["input_accepted"] == len(accepted_input) and task["input_accepted"] > task["input_applied"],
            "cut did not retain the actual short-write suffix")
    require(bytes.fromhex(before["input_pending_hex"]) == accepted_input[task["input_applied"]:],
            "pending input suffix bytes differ")
    require(task["stdout_end"] == len(output_read) and task["stdout_end"] > task["stdout_base"] and
            bytes.fromhex(before["output_unacked_hex"]) == output_read[task["stdout_base"]:],
            "cut has no independently read unacknowledged output")
    # P21 captures the imported checkpoint before resuming pumps/accepting more
    # controls. Comparing a later progressed sample would hide a dropped prefix.
    for key in PTY_FIELDS:
        if key != "task":
            require(before[key] == after[key], "activation changed PTY " + key)
    for key in ("nonce", "kind", "handle_id", "child", "ctl", "input_applied", "input_accepted",
                "stdout_base", "stdout_end", "stderr_base", "stderr_end", "wait_owner", "wait_count"):
        require(before["task"][key] == after["task"][key], "activation changed task " + key)
    require(after["task"]["generation"] > task["generation"] and
            after["task"]["at_boot_ns"] > task["at_boot_ns"], "activation checkpoint is stale")


def verify_control_identity(receipt, task_nonce, child, sequence):
    nonce(task_nonce)
    identity(child)
    uint(sequence, "original control sequence", True)
    require(receipt["nonce"] == task_nonce and receipt["child"] == child and receipt["seq"] == sequence,
            "control receipt is not the original child and operation")


def verify_resize_replay(receipt, task_nonce, child, sequence):
    # origin: hot-update S10 plan §6 / FullstackTaskPTYTest. The receiver changes
    # winsize between original and replay, making the duplicate effect visible.
    exact(receipt, ("nonce", "child", "seq", "ack", "replay_ack", "requested", "first_observed",
                    "receiver_changed", "after_replay", "commits", "first_boot_ns", "receiver_boot_ns",
                    "replay_boot_ns"), "resize replay")
    nonce(receipt["nonce"])
    identity(receipt["child"])
    verify_control_identity(receipt, task_nonce, child, sequence)
    for key in ("seq", "ack", "replay_ack", "commits", "first_boot_ns", "receiver_boot_ns", "replay_boot_ns"):
        uint(receipt[key], key, True)
    for key in ("requested", "first_observed", "receiver_changed", "after_replay"):
        size(receipt[key])
    require(receipt["seq"] == receipt["ack"] == receipt["replay_ack"] and receipt["commits"] == 1,
            "resize control committed more than once")
    require(receipt["requested"] == receipt["first_observed"] and
            receipt["receiver_changed"] != receipt["requested"] and
            receipt["after_replay"] == receipt["receiver_changed"], "resize replay repeated kernel effect")
    require(receipt["first_boot_ns"] < receipt["receiver_boot_ns"] < receipt["replay_boot_ns"],
            "receiver winsize change is outside the resize replay window")


def verify_signal_replay(receipt, task_nonce, child, sequence):
    exact(receipt, ("nonce", "child", "seq", "ack", "replay_ack", "signal", "handshake_count",
                    "after_replay_count", "commits", "handshake_boot_ns", "replay_boot_ns"), "signal replay")
    nonce(receipt["nonce"])
    identity(receipt["child"])
    verify_control_identity(receipt, task_nonce, child, sequence)
    for key in ("seq", "ack", "replay_ack", "commits", "handshake_boot_ns", "replay_boot_ns"):
        uint(receipt[key], key, True)
    require(type(receipt["signal"]) is int and receipt["signal"] in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP),
            "unsupported PTY signal")
    require(receipt["seq"] == receipt["ack"] == receipt["replay_ack"] and receipt["commits"] == 1,
            "signal control committed more than once")
    require(type(receipt["handshake_count"]) is int and type(receipt["after_replay_count"]) is int and
            receipt["handshake_count"] == receipt["after_replay_count"] == 1 and
            receipt["handshake_boot_ns"] < receipt["replay_boot_ns"], "signal needs exactly one child handshake")


def remote_produce(plan, root):
    """Product's original foreground child; observe signals without a keeper."""
    require(plan.kind == "pty", "remote PTY plan required")
    started = controlling_tty()
    validate_ctty(started, started["identity"])
    attrs = termios.tcgetattr(0)
    counts = {}

    def on_signal(number, _frame):
        counts[number] = counts.get(number, 0) + 1
        append_receipt(Path(root) / "controls.jsonl", {"event": "signal-handshake", "nonce": plan.nonce,
            "identity": process_identity(), "signal": number, "count": counts[number], "at_boot_ns": boot_ns()})

    original_handlers = {number: signal.signal(number, on_signal)
                         for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}

    def ready(ledger_root):
        append_receipt(ledger_root / "controls.jsonl", {"event": "ctty-ready", "nonce": plan.nonce,
                        "ctty": controlling_tty(), "at_boot_ns": boot_ns()})

    try:
        tty.setraw(0, termios.TCSANOW)
        # produce creates the ledger directory before its first output. Its
        # lifecycle binds this same kernel child, not a helper-held slave.
        return produce(plan, root, ready)
    finally:
        termios.tcsetattr(0, termios.TCSANOW, attrs)
        for number, handler in original_handlers.items():
            signal.signal(number, handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--nonce", required=True)
    parser.add_argument("--runtime", choices=("a0", "a1", "a2", "a3"), required=True)
    parser.add_argument("--ceiling-ns", type=int, required=True)
    args = parser.parse_args()
    return remote_produce(TaskPlan("pty-" + args.runtime, args.runtime, "pty", args.nonce,
                                  args.ceiling_ns), args.root)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (EvidenceError, OSError, ValueError) as exc:
        print("FULLSTACK_PTY_SETUP: " + str(exc), file=sys.stderr)
        sys.exit(2)
