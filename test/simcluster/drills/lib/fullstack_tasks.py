#!/usr/bin/env python3
"""fullstack_tasks.py — original task witnesses and supervised ctl ownership.

P19 supplies start_supervised(LaunchRequest) from P05. It returns an owner with
started (identity/controlling-tty receipt), send(bytes), cancel(), and waiter(deadline_ns).
The returned waiter joins the target AND shared supervisor and returns the exact
wait receipt consumed by validate_wait. There is no local spawn fallback.
P21 supplies product checkpoints and independently captured receiver bytes;
these domain checks do not replace P19's contract evidence judge.

The producer command is a workload run by the real exec/run CLI, never a daemon
launcher. Its finite supply is declared before READY and only a nonce-bound STOP
frame permits normal exit23. No helper forks or opens the remote PTY slave.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import select
import struct
import sys
import time
from dataclasses import dataclass


class EvidenceError(ValueError):
    """Missing, contradictory, or discontinuous task evidence."""


def require(condition, reason):
    if not condition:
        raise EvidenceError(reason)


def uint(value, name, positive=False):
    require(type(value) is int and (0 < value if positive else 0 <= value)
            and value < 1 << 64, "invalid " + name)
    return value


def exact(value, fields, name):
    require(type(value) is dict and set(value) == set(fields), "invalid " + name + " fields")
    return value


def nonce(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{32}", value), "invalid task nonce")
    return value


def identity(value):
    exact(value, ("boot_id", "pidns", "pid", "start_ticks"), "process identity")
    for key in ("boot_id", "pidns"):
        require(isinstance(value[key], str) and bool(value[key]), "missing " + key)
    uint(value["pid"], "pid", True)
    uint(value["start_ticks"], "start ticks", True)
    return value


def process_identity(pid=None):
    """Read the kernel identity; an unavailable /proc is a setup error."""
    pid = os.getpid() if pid is None else pid
    raw = Path(f"/proc/{pid}/stat").read_text()
    fields = raw[raw.rfind(")") + 2:].split()
    return identity({"boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                     "pidns": os.readlink(f"/proc/{pid}/ns/pid"), "pid": pid,
                     "start_ticks": int(fields[19])})


def boot_ns():
    # All pairing stays on this host's BOOTTIME, including suspend.
    require(hasattr(time, "CLOCK_BOOTTIME"), "Linux BOOTTIME is required for task evidence")
    return time.clock_gettime_ns(time.CLOCK_BOOTTIME)


def strict_json(path):
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate evidence member")
            result[key] = value
        return result
    return json.loads(Path(path).read_text(encoding="utf-8"), object_pairs_hook=object_pairs,
                      parse_constant=lambda _: require(False, "non-finite evidence number"))


def append_receipt(path, value):
    with Path(path).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, sort_keys=True, allow_nan=False) + "\n")


HEADER = struct.Struct(">16sBQI32s")
STREAMS = {"input": 0, "stdout": 1, "stderr": 2}


def frame(task_nonce, stream, sequence, data):
    nonce(task_nonce)
    require(stream in STREAMS, "unknown stream")
    uint(sequence, "frame sequence")
    require(type(data) is bytes and 0 < len(data) <= 32768, "invalid frame payload")
    return HEADER.pack(bytes.fromhex(task_nonce), STREAMS[stream], sequence, len(data),
                       hashlib.sha256(data).digest()) + data


def decode_frames(raw, task_nonce, stream, complete=True, first_sequence=0):
    """Return frames and a retained suffix; never repair a truncated record."""
    nonce(task_nonce)
    uint(first_sequence, "first frame sequence")
    require(type(raw) is bytes and stream in STREAMS, "invalid raw stream")
    offset, records = 0, []
    while len(raw) - offset >= HEADER.size:
        token, kind, seq, length, digest = HEADER.unpack_from(raw, offset)
        require(token.hex() == task_nonce and kind == STREAMS[stream], "foreign stream frame")
        require(0 < length <= 32768, "invalid frame length")
        end = offset + HEADER.size + length
        if end > len(raw):
            break
        data = raw[offset + HEADER.size:end]
        require(hashlib.sha256(data).digest() == digest, "frame digest differs")
        require(seq == first_sequence + len(records), "frame missing, reordered, or duplicated")
        records.append(data)
        offset = end
    require(not complete or offset == len(raw), "truncated stream frame")
    return records, raw[offset:]


@dataclass(frozen=True)
class TaskPlan:
    task_id: str
    runtime: str
    kind: str
    nonce: str
    ceiling_ns: int
    period_ns: int = 50_000_000
    drain_records: int = 40

    def __post_init__(self):
        require(bool(self.task_id) and self.runtime in ("a0", "a1", "a2", "a3"), "invalid task target")
        require(self.kind in ("exec", "pty"), "unknown task kind")
        nonce(self.nonce)
        uint(self.ceiling_ns, "task ceiling", True)
        uint(self.period_ns, "task period", True)
        uint(self.drain_records, "drain supply", True)

    @property
    def records(self):
        return (self.ceiling_ns + self.period_ns - 1) // self.period_ns + self.drain_records


def original_cohort(ceiling_ns, nonces):
    """Predeclare four exec and two PTY tasks in the four-runtime roster."""
    require(len(nonces) == 6 and len(set(nonces)) == 6, "cohort needs six distinct nonces")
    entries = [("exec", "a0"), ("exec", "a1"), ("exec", "a2"), ("exec", "a3"),
               ("pty", "a0"), ("pty", "a2")]
    return tuple(TaskPlan(kind + "-" + runtime, runtime, kind, token, ceiling_ns)
                 for (kind, runtime), token in zip(entries, nonces))


@dataclass(frozen=True)
class LaunchRequest:
    """P05 adapter input. controlling_tty_fd requests setsid plus TIOCSCTTY."""
    argv: tuple
    cwd: str
    env: dict
    stdin_fd: int
    stdout_fd: int
    stderr_fd: int
    controlling_tty_fd: int | None
    remote_lease: str

    def __post_init__(self):
        require(type(self.argv) is tuple and bool(self.argv) and
                all(isinstance(arg, str) and "\0" not in arg for arg in self.argv), "invalid ctl argv")
        require(Path(self.argv[0]).is_absolute(), "ctl must be an explicit supplied artifact")
        require(Path(self.cwd).is_absolute() and type(self.env) is dict, "invalid launch environment")
        require(all(isinstance(k, str) and isinstance(v, str) and "\0" not in k + v
                    for k, v in self.env.items()), "invalid launch environment entry")
        require(not any(k.startswith("TETHER_TEST_DRILL_") or k.startswith("TETHER_HANDOFF_") or k.startswith("_TETHER_")
                        for k in self.env), "private custody cannot enter a business child")
        for fd in (self.stdin_fd, self.stdout_fd, self.stderr_fd):
            uint(fd, "stdio fd")
        if self.controlling_tty_fd is not None:
            uint(self.controlling_tty_fd, "controlling tty fd")
        require(isinstance(self.remote_lease, str) and bool(self.remote_lease), "remote supervision lease missing")


WAIT_FIELDS = ("identity", "source", "wait_calls", "exit_code", "signal", "native_rc", "joined")


def validate_join(receipt, original=None):
    # origin: hot-update S10 plan §6 / FullstackTaskPTYTest. EOF cannot replace
    # the original ctl's waitpid, and successful emergency cleanup retains125.
    exact(receipt, WAIT_FIELDS, "ctl wait receipt")
    identity(receipt["identity"])
    if original is not None:
        require(receipt["identity"] == identity(original), "wait belongs to a replacement ctl")
    require(receipt["source"] == "shared-supervisor-waitpid" and type(receipt["wait_calls"]) is int
            and receipt["wait_calls"] == 1,
            "original ctl needs exactly one supervised waiter")
    require(type(receipt["joined"]) is bool and receipt["joined"], "supervisor was not joined")
    require(type(receipt["native_rc"]) is int and receipt["native_rc"] == 0, "supervisor cleanup failed")
    require(type(receipt["exit_code"]) is int and type(receipt["signal"]) is int, "invalid ctl terminal status")


def validate_wait(receipt, original, exit_code=23, expected_signal=0):
    validate_join(receipt, original)
    require(type(receipt["exit_code"]) is int and receipt["exit_code"] == exit_code
            and type(receipt["signal"]) is int and receipt["signal"] == expected_signal,
            "original ctl terminal status differs")


class SupervisedTask:
    """Hold the returned shared waiter across normal stop and partial setup."""

    def __init__(self, request, start_supervised, deadline_ns):
        require(callable(start_supervised), "P05 supervised launcher is required")
        uint(deadline_ns, "startup join deadline", True)
        self.owner = start_supervised(request)
        self.receipt = None
        self.original = None
        # Keep the owner even if the subsequent READY/ctty handshake is rejected.
        try:
            require(callable(getattr(self.owner, "waiter", None)) and
                    callable(getattr(self.owner, "send", None)) and
                    callable(getattr(self.owner, "cancel", None)), "supervised owner is incomplete")
            self.original = identity(self.owner.started["identity"]).copy()
        except Exception as setup_error:
            try:
                self.abort(deadline_ns)
            except Exception as cleanup_error:
                raise ExceptionGroup("startup and supervisor cleanup failed", [setup_error, cleanup_error])
            raise
        self.stopped = False
        self.input_sequence = 0

    def send(self, task_nonce, data):
        require(not self.stopped and self.receipt is None, "task already stopped or joined")
        packet = frame(task_nonce, "input", self.input_sequence, data)
        self.owner.send(packet)
        self.input_sequence += 1

    def stop(self, task_nonce):
        require(not self.stopped and self.receipt is None, "task already stopped or joined")
        self.send(task_nonce, b"STOP")
        self.stopped = True

    def join(self, deadline_ns):
        uint(deadline_ns, "join deadline", True)
        require(self.receipt is None, "returned waiter already joined")
        # Save a failure receipt too: retrying Wait could steal sole-wait custody.
        self.receipt = self.owner.waiter(deadline_ns)
        validate_wait(self.receipt, self.original)
        return self.receipt

    def abort(self, deadline_ns):
        """Cancel the fixture ctl, then join the returned waiter exactly once."""
        uint(deadline_ns, "abort deadline", True)
        require(self.receipt is None, "returned waiter already joined")
        errors = []
        try:
            self.owner.cancel()
        except Exception as exc:
            errors.append(exc)
        # A failed cancellation cannot discard the returned waiter's custody.
        try:
            self.receipt = self.owner.waiter(deadline_ns)
            validate_join(self.receipt, self.original)
        except Exception as exc:
            errors.append(exc)
        if errors:
            raise ExceptionGroup("ctl cancellation or supervisor cleanup failed", errors)
        return self.receipt


SAMPLE_FIELDS = ("nonce", "kind", "handle_id", "child", "ctl", "at_boot_ns", "generation",
                 "state", "terminal_id", "exit_code", "signal", "lease_deadline_boot_ns",
                 "create_count", "wait_owner", "wait_count", "resources", "input_applied",
                 "input_accepted", "stdout_base", "stdout_end", "stderr_base", "stderr_end")
STATES = ("running", "reap-held", "terminal-unacked", "settled-unexpired", "expired")


def task_sample(sample):
    exact(sample, SAMPLE_FIELDS, "task checkpoint")
    nonce(sample["nonce"])
    identity(sample["child"])
    identity(sample["ctl"])
    require(sample["kind"] in ("exec", "pty") and sample["state"] in STATES, "unknown task state")
    for key in ("handle_id", "wait_owner"):
        require(isinstance(sample[key], str) and bool(sample[key]), "missing " + key)
    require(sample["create_count"] == 1 and type(sample["create_count"]) is int, "task spawned more than once")
    for key in ("at_boot_ns", "generation", "lease_deadline_boot_ns"):
        uint(sample[key], key, True)
    for key in ("wait_count", "input_applied", "input_accepted", "stdout_base", "stdout_end",
                "stderr_base", "stderr_end", "signal"):
        uint(sample[key], key)
    require(type(sample["exit_code"]) is int and isinstance(sample["terminal_id"], str), "invalid terminal")
    require(sample["input_applied"] <= sample["input_accepted"] and
            sample["stdout_base"] <= sample["stdout_end"] and sample["stderr_base"] <= sample["stderr_end"],
            "inconsistent task offsets")
    require(type(sample["resources"]) is list, "missing resource census")
    ids = []
    for resource in sample["resources"]:
        exact(resource, ("id", "kind", "ofd", "owner"), "task resource")
        require(all(isinstance(v, str) and v for v in resource.values()), "unknown task resource")
        ids.append(resource["id"])
    require(len(ids) == len(set(ids)), "duplicate resource owner")
    terminal = sample["state"] in STATES[2:]
    require(sample["wait_count"] == int(terminal) and bool(sample["terminal_id"]) == terminal,
            "task terminal or sole-wait receipt missing")
    if not terminal:
        require(sample["exit_code"] == sample["signal"] == 0, "live task already reports a terminal status")
    if sample["state"] == "terminal-unacked":
        require(sample["resources"], "terminal-unacked task lost replay custody")
    if sample["state"] == "reap-held":
        require(sample["resources"], "ReapHeld lost its live source")
    if sample["state"] in ("settled-unexpired", "expired"):
        require(not sample["resources"], "settled task retained data-plane resources")
        require(sample["stdout_base"] == sample["stdout_end"] and
                sample["stderr_base"] == sample["stderr_end"], "settled output still unacknowledged")
    if sample["state"] == "settled-unexpired":
        require(sample["at_boot_ns"] < sample["lease_deadline_boot_ns"], "settled lease already expired")
    if sample["state"] == "expired":
        require(sample["at_boot_ns"] >= sample["lease_deadline_boot_ns"], "original lease collected early")
    return sample


def verify_task_chain(samples):
    # origin: hot-update S10 plan §4.2 / §6 / FullstackTaskPTYTest. New work
    # accepted in ACTIVE must remain the same handle through current rollback.
    require(type(samples) is list and len(samples) >= 2, "task chain is empty")
    first = task_sample(samples[0])
    previous = first
    original_resources = {r["id"]: r for r in first["resources"]}
    for sample in samples[1:]:
        task_sample(sample)
        for key in ("nonce", "kind", "handle_id", "child", "ctl", "wait_owner", "lease_deadline_boot_ns"):
            require(sample[key] == first[key], "original task " + key + " changed")
        require(sample["at_boot_ns"] > previous["at_boot_ns"] and
                sample["generation"] >= previous["generation"], "stale task checkpoint")
        require(STATES.index(sample["state"]) >= STATES.index(previous["state"]), "task state regressed")
        for key in ("input_applied", "input_accepted", "stdout_base", "stdout_end", "stderr_base", "stderr_end"):
            require(sample[key] >= previous[key], "task byte offset regressed")
        for resource in sample["resources"]:
            require(resource == original_resources.get(resource["id"]), "task resource or OFD was replaced")
        if sample["state"] in ("running", "reap-held"):
            require(sample["resources"] == first["resources"] and sample["resources"], "live task lost resources")
        if previous["terminal_id"]:
            require(all(sample[k] == previous[k] for k in ("terminal_id", "exit_code", "signal")),
                    "terminal receipt was replaced")
        previous = sample


def verify_raw_pair(root, task_nonce, stream, minimum=2):
    """Require independent files and exact bytes, including CR/LF and controls."""
    require(stream in STREAMS, "unknown stream")
    uint(minimum, "minimum records", True)
    root = Path(root).resolve(strict=True)
    paths = [root / (stream + "." + side) for side in ("sent", "received")]
    for path in paths:
        require(path.resolve(strict=True).parent == root and not path.is_symlink(), "unsafe raw evidence path")
        require(path.is_file(), "raw evidence is not a regular file")
    require(not os.path.samefile(*paths), "source ledger substituted for receiver")
    sent, received = (path.read_bytes() for path in paths)
    require(sent == received, "original task bytes differ")
    records, _ = decode_frames(received, task_nonce, stream)
    require(len(records) >= minimum, "empty task byte witness")


def write_all(fd, data, ledger):
    offset = 0
    while offset < len(data):
        written = os.write(fd, data[offset:])
        require(written > 0, "workload write made no progress")
        ledger.write(data[offset:offset + written])
        offset += written


def produce(plan, root, ready=None):
    """Run inside the original business process; never reconnect or respawn."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=False)
    append_receipt(root / "lifecycle.jsonl", {"event": "started", "nonce": plan.nonce,
                                              "identity": process_identity(), "at_boot_ns": boot_ns()})
    if ready is not None:
        ready(root)
    # Workload stdin is owned by this process; no shared ctl OFD is modified.
    os.set_blocking(0, False)
    input_raw, input_seq, sent_seq, next_at = b"", 0, 0, boot_ns()
    streams = ("stdout", "stderr") if plan.kind == "exec" else ("stdout",)
    with (root / "input.received").open("xb", buffering=0) as received:
        ledgers = {name: (root / (name + ".sent")).open("xb", buffering=0) for name in streams}
        try:
            while True:
                readable, _, _ = select.select([0], [], [], max(0, (next_at - boot_ns()) / 1e9))
                if readable:
                    data = os.read(0, 32768)
                    require(data, "original workload stdin ended before STOP")
                    received.write(data)
                    input_raw += data
                    commands, input_raw = decode_frames(input_raw, plan.nonce, "input", complete=False,
                                                        first_sequence=input_seq)
                    for index, command in enumerate(commands):
                        input_seq += 1
                        if command == b"STOP":
                            require(index == len(commands) - 1 and not input_raw, "bytes after STOP")
                            append_receipt(root / "lifecycle.jsonl", {"event": "stopped", "nonce": plan.nonce,
                                "identity": process_identity(), "at_boot_ns": boot_ns(), "exit_code": 23})
                            return 23
                        if command.startswith(b"RESIZE:"):
                            require(plan.kind == "pty", "exec cannot change winsize")
                            # The receiver itself changes size between resize
                            # and replay; natural ioctl idempotence is no proof.
                            import fcntl
                            import termios
                            parts = command.decode("ascii").split(":")
                            require(len(parts) == 3, "invalid receiver resize")
                            rows, cols = int(parts[1]), int(parts[2])
                            require(0 < rows <= 65535 and 0 < cols <= 65535, "invalid receiver winsize")
                            fcntl.ioctl(0, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
                            observed = struct.unpack("HHHH", fcntl.ioctl(0, termios.TIOCGWINSZ, bytes(8)))[:2]
                            append_receipt(root / "controls.jsonl", {"event": "receiver-resize", "nonce": plan.nonce,
                                "identity": process_identity(), "winsize": list(observed), "at_boot_ns": boot_ns()})
                        else:
                            require(command.startswith(b"DATA:"), "unknown workload command")
                if boot_ns() >= next_at:
                    require(sent_seq < plan.records, "predeclared task supply exhausted before seal")
                    for stream in streams:
                        payload = hashlib.sha256(f"{plan.nonce}:{stream}:{sent_seq}".encode()).digest() + b"\r\n\x00\x03\x04"
                        write_all(STREAMS[stream], frame(plan.nonce, stream, sent_seq, payload), ledgers[stream])
                    sent_seq += 1
                    next_at += plan.period_ns
        finally:
            for ledger in ledgers.values():
                ledger.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--nonce", required=True)
    parser.add_argument("--runtime", choices=("a0", "a1", "a2", "a3"), required=True)
    parser.add_argument("--ceiling-ns", type=int, required=True)
    args = parser.parse_args()
    return produce(TaskPlan("exec-" + args.runtime, args.runtime, "exec", args.nonce, args.ceiling_ns), args.root)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (EvidenceError, OSError, ValueError) as exc:
        print("FULLSTACK_TASK_SETUP: " + str(exc), file=sys.stderr)
        sys.exit(2)
