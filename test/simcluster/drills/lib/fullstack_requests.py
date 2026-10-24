#!/usr/bin/env python3
"""Dispatch the presealed P17 slots through P18's nonblocking CLI adapter.

There is no subprocess or CLI fallback here. launch(Request) must return a P05
supervised handle with poll(now_ns), cancel(), and join(absolute_deadline_ns).
poll returns None or Outcome; join returns the final Outcome and joins the
original waiter. Discovery/auth/queue and connection attempts belong inside
that handle, use the scheduler's clock, and retain the original CallID.
P19 supplies the Go SealRequestSchedule bytes/digest and an independent effects
ledger. This collector reports native ASSERT-FAIL=1 for incomplete requests;
it cannot certify product/deployment success by itself.
"""

from collections import Counter
from dataclasses import asdict, dataclass, field
import hashlib
import json
from pathlib import Path
import time


INTERVAL_NS = 100_000_000
REQUEST_NS = 5_000_000_000
PATTERN = ("read", "read", "read", "write", "short-exec")
# Semantic binding keys, not invented argv. P18 resolves status through its
# frozen command tree; the current tree has no `node status` command.
READS = ("node-ls", "ps", "status")
PHASES = frozenset(("dns", "proxy", "tls", "ws", "connect", "inbox_probe",
                    "inbox_fallback", "discovery", "banner", "probe", "create",
                    "request", "backoff", "cancel", "result", "freeze", "exec",
                    "import", "auth", "ready", "maintenance"))
KEY_FIELDS = frozenset(("role", "host", "unit", "sid", "nid", "domain", "uid"))
SLOT_FIELDS = frozenset(("id", "class", "target", "scheduled_ns", "deadline_ns"))


def boottime_ns():
    # Both schedule bounds and receipts must use this host's clock; a remote
    # BOOTTIME or wall clock cannot be subtracted from these timestamps.
    if hasattr(time, "CLOCK_BOOTTIME"):
        return time.clock_gettime_ns(time.CLOCK_BOOTTIME)
    return time.monotonic_ns()


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate request evidence field")
        result[key] = value
    return result


def _integer(value, minimum=0, maximum=(1 << 63) - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("invalid request evidence integer")
    return value


def _text(value):
    if type(value) is not str or not value or any(0xD800 <= ord(c) <= 0xDFFF for c in value):
        raise ValueError("invalid request evidence text")
    return value


@dataclass(frozen=True)
class Key:
    role: str
    host: str
    unit: str
    sid: str
    nid: str
    domain: str
    uid: int

    @classmethod
    def decode(cls, value):
        if type(value) is not dict or value.keys() != KEY_FIELDS:
            raise ValueError("request target fields differ from P00")
        for field in KEY_FIELDS - {"uid"}:
            _text(value[field])
        _integer(value["uid"], maximum=(1 << 32) - 1)
        if value["role"] != "agent":
            raise ValueError("request target is not an agent")
        return cls(**value)


@dataclass(frozen=True)
class Slot:
    id: str
    kind: str
    target: Key
    scheduled_ns: int
    deadline_ns: int

    def disk(self):
        return {"id": self.id, "class": self.kind, "target": asdict(self.target),
                "scheduled_ns": self.scheduled_ns, "deadline_ns": self.deadline_ns}


@dataclass(frozen=True)
class Schedule:
    slots: tuple
    digest: str
    raw: bytes


# origin: hot-update S10 plan §3.2 / §8 M10S / §10.3
def load_schedule(raw, expected_digest):
    """Validate Go's sealed bytes without regenerating expected from launches."""
    if type(raw) is not bytes or hashlib.sha256(raw).hexdigest() != expected_digest:
        raise ValueError("request schedule digest mismatch")
    rows = json.loads(raw.decode("utf-8", errors="strict"), object_pairs_hook=_object)
    if type(rows) is not list or not rows or len(rows) > 120_000:
        raise ValueError("request schedule is empty")
    slots, ids = [], set()
    for row in rows:
        if type(row) is not dict or row.keys() != SLOT_FIELDS:
            raise ValueError("request slot fields differ from P00")
        slot = Slot(_text(row["id"]), _text(row["class"]), Key.decode(row["target"]),
                    _integer(row["scheduled_ns"]), _integer(row["deadline_ns"], 1))
        if slot.id in ids or slot.kind != PATTERN[len(slots) % 5]:
            raise ValueError("request slot identity or class pattern changed")
        if slot.scheduled_ns != rows[0]["scheduled_ns"] + len(slots) * INTERVAL_NS:
            raise ValueError("request schedule lost a 100ms slot")
        if slot.deadline_ns != slot.scheduled_ns + REQUEST_NS:
            raise ValueError("request slot renewed its scheduled deadline")
        ids.add(slot.id)
        slots.append(slot)
    # Very short schedules still need all four targets in their sealed manifest.
    # P19's real profile lasts much longer; refusing <4 slots prevents a fixture
    # from claiming four-node round robin with an invisible runtime.
    if len(slots) < 4:
        raise ValueError("request schedule does not cover four runtimes")
    targets = tuple(slot.target for slot in slots[:4])
    for axis in (lambda k: k.sid, lambda k: k.nid, lambda k: k.domain,
                 lambda k: (k.host, k.unit), lambda k: (k.host, k.uid)):
        if len({axis(k) for k in targets}) != 4:
            raise ValueError("request runtimes share an installation identity")
    if targets != tuple(sorted(targets, key=lambda k: k.nid)):
        raise ValueError("request runtimes are not in sealed node order")
    if any(slot.target != targets[i % 4] for i, slot in enumerate(slots)):
        raise ValueError("request target round robin changed")
    return Schedule(tuple(slots), expected_digest, raw)


@dataclass(frozen=True)
class RawRef:
    path: str
    sha: str
    bytes: int
    schema: str

    def valid(self):
        return (type(self.path) is str and bool(self.path) and type(self.sha) is str
                and len(self.sha) == 64 and all(c in "0123456789abcdef" for c in self.sha)
                and type(self.bytes) is int and self.bytes > 0
                and type(self.schema) is str and bool(self.schema))


@dataclass(frozen=True)
class Attempt:
    slot_id: str
    call_id: str
    phase: str
    index: int
    started_ns: int
    finished_ns: int
    sent: bool
    completed: bool
    rc: int
    code: str
    raw: RawRef


@dataclass(frozen=True)
class Request:
    slot: Slot
    call_id: str
    operation: str
    effect_token: str
    expected_stdout: bytes


def request_for(schedule, index):
    slot = schedule.slots[index]
    # These are public correlation tokens, not auth material. They are stable
    # across connection attempts and cannot authorize another business launch.
    token = hashlib.sha256((schedule.digest + "\x00" + slot.id).encode()).hexdigest()
    operation = READS[(index // 5 * 3 + index % 5) % 3] if slot.kind == "read" else "reliable-exec"
    return Request(slot, token, operation, token if slot.kind == "write" else "",
                   ("S10_RESULT:" + token + "\n").encode() if slot.kind == "short-exec" else b"")


@dataclass(frozen=True)
class Outcome:
    attempts: tuple
    stdout: bytes
    rc: int
    terminal: bool
    reliable: bool
    result_valid: bool
    raw: RawRef


@dataclass(frozen=True)
class Effect:
    target: Key
    token: str


@dataclass
class Launch:
    request: Request
    started_ns: int
    finished_ns: int = None
    outcome: Outcome = None
    error: str = ""
    joined: bool = False


@dataclass
class Report:
    schedule: Schedule
    launches: list
    pending_peak: int = 0
    pending_create_peak: int = 0
    pending_create_by_target: dict = field(default_factory=dict)

    def attempts(self):
        # origin: hot-update S10 plan §4.4 / P17. Failed/cancelled outcomes
        # retain every attempt too; P02's population is the separate slots list.
        return tuple(a for row in self.launches if row.outcome is not None for a in row.outcome.attempts)

    def save(self, directory):
        """Write complete evidence once; no tail trimming or success filter."""
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        values = {
            "schedule.json": self.schedule.raw,
            "attempts.json": _json_bytes([asdict(a) for a in self.attempts()]),
            "launches.json": _json_bytes([
                {"slot_id": row.request.slot.id, "call_id": row.request.call_id,
                 "operation": row.request.operation, "effect_token": row.request.effect_token,
                 "expected_stdout_hex": row.request.expected_stdout.hex(),
                 "started_ns": row.started_ns, "finished_ns": row.finished_ns,
                 "error": row.error, "joined": row.joined,
                 "outcome": _outcome_disk(row.outcome)} for row in self.launches]),
            "scheduler.json": _json_bytes({"schedule_sha": self.schedule.digest,
                "scheduled": len(self.schedule.slots), "recorded": len(self.launches),
                "launched": sum(type(r.started_ns) is int for r in self.launches),
                "pending_peak": self.pending_peak, "pending_create_peak": self.pending_create_peak,
                "pending_create_by_target": [{"target": asdict(k), "peak": v}
                    for k, v in sorted(self.pending_create_by_target.items(), key=lambda item: item[0].nid)]}),
        }
        refs = []
        for name, data in values.items():
            path = directory / name
            with path.open("xb") as stream:
                stream.write(data)
            refs.append(RawRef(str(path), hashlib.sha256(data).hexdigest(), len(data),
                               "fullstack-request-ledger-v1"))
        return tuple(refs)


def _json_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def _outcome_disk(outcome):
    if outcome is None:
        return None
    value = asdict(outcome)
    value["stdout_hex"] = value.pop("stdout").hex()
    return value


class Scheduler:
    """One fixed scheduling loop, with pending counts measured after launch.

    launch/poll/cancel must not block. join gets one shared cleanup deadline;
    P05 must enforce it and retain cleanup failures. No new process can be
    launched from join. A stop fails all remaining slots instead of resealing.
    """

    def __init__(self, schedule, launch, clock=boottime_ns, wait=None):
        self.schedule, self.launch, self.clock = schedule, launch, clock
        self.wait = wait or self._wait
        self.report = Report(schedule, [])
        self.next_index = 0
        self.handles = []
        self.pending = []
        self.stopped = False
        self.ran = False

    def _wait(self, deadline_ns):
        time.sleep(max(0, deadline_ns - self.clock()) / 1_000_000_000)

    def stop(self):
        self.stopped = True

    def _collect(self, handle, row, now):
        try:
            outcome = handle.poll(now)
            if outcome is not None:
                if not isinstance(outcome, Outcome):
                    raise ValueError("invalid CLI outcome")
                row.outcome, row.finished_ns = outcome, now
                return True
        except Exception:
            row.error = row.error or "poll-error"
        if now >= row.request.slot.deadline_ns or self.stopped or row.error:
            row.error = row.error or ("cancelled" if self.stopped else "deadline")
            try:
                handle.cancel()
            except Exception:
                row.error += ":cancel-error"
            return True
        return False

    # origin: hot-update S10 plan §3.2 / §8 M10S / P17
    def tick(self):
        active = []
        for handle, row in self.pending:
            if not self._collect(handle, row, self.clock()):
                active.append((handle, row))
        self.pending = active
        # Catch up all elapsed slots. Backpressure never stops the clock and
        # never changes the expected set; late or expired slots fail the audit.
        while self.next_index < len(self.schedule.slots):
            request = request_for(self.schedule, self.next_index)
            now = self.clock()
            if not self.stopped and now < request.slot.scheduled_ns:
                break
            self.next_index += 1
            row = Launch(request, None)
            self.report.launches.append(row)
            if self.stopped or now >= request.slot.deadline_ns:
                row.error = "cancelled" if self.stopped else "not-launched-deadline"
                continue
            row.started_ns = now
            try:
                handle = self.launch(request)
                if handle is None:
                    raise ValueError("missing CLI handle")
                self.handles.append((handle, row))
                self.pending.append((handle, row))
            except Exception:
                # A failed unsafe operation is never resubmitted with a new
                # CallID, even if its effect or original waiter is unknown.
                row.error = "launch-error"
                row.finished_ns = self.clock()
            self.report.pending_peak = max(self.report.pending_peak, len(self.pending))
            creates = sum(r.request.slot.kind != "read" for _, r in self.pending)
            self.report.pending_create_peak = max(self.report.pending_create_peak, creates)
            for target in {r.request.slot.target for _, r in self.pending}:
                count = sum(r.request.slot.kind != "read" and r.request.slot.target == target
                            for _, r in self.pending)
                self.report.pending_create_by_target[target] = max(
                    self.report.pending_create_by_target.get(target, 0), count)

    def run(self, cleanup_ns=REQUEST_NS):
        if self.ran:
            raise ValueError("request scheduler cannot relaunch its schedule")
        _integer(cleanup_ns, 1)
        self.ran = True
        try:
            while self.next_index < len(self.schedule.slots) or self.pending:
                self.tick()
                now = self.clock()
                wakes = [now + 10_000_000]
                if self.next_index < len(self.schedule.slots):
                    wakes.append(self.schedule.slots[self.next_index].scheduled_ns)
                wakes.extend(r.request.slot.deadline_ns for _, r in self.pending)
                if self.next_index < len(self.schedule.slots) or self.pending:
                    self.wait(min(wakes))
        finally:
            # A clock/adapter exception or KeyboardInterrupt must still cancel
            # and join every already-started handle. The sealed future slots
            # stay missing evidence, never a newly shortened successful run.
            for handle, row in self.pending:
                row.error = row.error or "scheduler-aborted"
                try:
                    handle.cancel()
                except Exception:
                    row.error += ":cancel-error"
            self.pending = []
            deadline = self.clock() + cleanup_ns
            for handle, row in self.handles:
                try:
                    outcome = handle.join(deadline)
                    if not isinstance(outcome, Outcome):
                        raise ValueError("missing joined CLI outcome")
                    if row.outcome is None:
                        row.outcome, row.finished_ns = outcome, self.clock()
                    elif outcome != row.outcome:
                        row.error = row.error or "join-result-changed"
                    row.joined = True
                except Exception:
                    row.error = row.error or "join-error"
        return self.report


# origin: hot-update S10 plan §4.4 / §8 M10S / §10.2 P17
def audit_requests(required_schedule, report, effects):
    """Independent launch/effect conservation; all failures stay in the set."""
    failures = []
    if required_schedule.digest != report.schedule.digest or required_schedule.raw != report.schedule.raw:
        failures.append("schedule-changed")
    rows = {}
    expected = {slot.id for slot in required_schedule.slots}
    for row in report.launches:
        if row.request.slot.id in rows:
            failures.append("duplicate-launch:" + row.request.slot.id)
        rows[row.request.slot.id] = row
    if rows.keys() != expected:
        failures.append("launch-conservation")
    expected_effects = Counter()
    for i, slot in enumerate(required_schedule.slots):
        request = request_for(required_schedule, i)
        if slot.kind == "write":
            expected_effects[Effect(slot.target, request.effect_token)] += 1
        row = rows.get(slot.id)
        if row is None:
            continue
        prefix = slot.id + ":"
        if row.request != request:
            failures.append(prefix + "request-identity")
        if row.error:
            failures.append(prefix + row.error)
        if (type(row.started_ns) is not int or row.started_ns < slot.scheduled_ns
                or row.started_ns >= slot.scheduled_ns + INTERVAL_NS):
            failures.append(prefix + "missed-launch-slot")
        # Queue and discovery time are inside the same five seconds. Using
        # started_ns here would renew the budget after a delayed launch.
        if (type(row.finished_ns) is not int or row.started_ns is None
                or row.finished_ns < row.started_ns or row.finished_ns > slot.deadline_ns):
            failures.append(prefix + "scheduled-deadline")
        if not row.joined:
            failures.append(prefix + "waiter-not-joined")
        outcome = row.outcome
        if outcome is None:
            failures.append(prefix + "missing-result")
            continue
        if (type(outcome.rc) is not int or outcome.rc != 0 or outcome.terminal is not True
                or not outcome.raw.valid() or type(outcome.stdout) is not bytes):
            failures.append(prefix + "result-failed")
        if slot.kind == "read" and (outcome.result_valid is not True or not outcome.stdout):
            failures.append(prefix + "read-result")
        if slot.kind != "read" and outcome.reliable is not True:
            failures.append(prefix + "reliable-path-missing")
        if slot.kind == "short-exec" and outcome.stdout != request.expected_stdout:
            failures.append(prefix + "short-result")
        if not outcome.attempts:
            failures.append(prefix + "missing-attempts")
        for index, attempt in enumerate(outcome.attempts, 1):
            if (attempt.slot_id != slot.id or attempt.call_id != request.call_id
                    or type(attempt.index) is not int or attempt.index != index
                    or attempt.phase not in PHASES or not attempt.raw.valid()
                    or type(attempt.sent) is not bool or type(attempt.completed) is not bool
                    or type(attempt.rc) is not int or type(attempt.code) is not str
                    or type(attempt.started_ns) is not int or type(attempt.finished_ns) is not int
                    or row.started_ns is None or row.finished_ns is None
                    or not row.started_ns <= attempt.started_ns <= attempt.finished_ns <= row.finished_ns):
                failures.append(prefix + "attempt-identity-or-clock")
        if not any(a.sent and a.completed and a.phase in ("request", "create", "result")
                   and a.rc == 0 for a in outcome.attempts):
            failures.append(prefix + "missing-command-attempt")
        # A connection-level timeout may precede success with the same CallID.
        # A business refusal/unknown cannot be erased by that later success.
        if any(a.code or (a.rc != 0 and a.phase in ("request", "create", "result"))
               for a in outcome.attempts):
            failures.append(prefix + "attempt-refusal")
    if Counter(effects) != expected_effects:
        failures.append("effect-conservation")
    failed_slots = {slot.id for slot in required_schedule.slots
                    if slot.id not in rows or any(f.startswith(slot.id + ":") for f in failures)}
    return {"verdict": "ASSERT-FAIL" if failures else "GREEN", "rc": 1 if failures else 0,
            "failures": failures, "scheduled": len(required_schedule.slots),
            "started": sum(type(r.started_ns) is int for r in report.launches),
            "completed": sum(r.outcome is not None for r in report.launches),
            "failed": len(failed_slots),
            "censored": sum(r.error.startswith(("deadline", "cancelled", "scheduler-aborted"))
                            for r in report.launches),
            "missing": sum(s.id not in rows or rows[s.id].outcome is None for s in required_schedule.slots)}
