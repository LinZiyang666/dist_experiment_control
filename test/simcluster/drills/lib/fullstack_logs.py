#!/usr/bin/env python3
"""Complete log windows; the enclosing P05/P16 owner supervises the collector.

Real files and inotify are exercised by the hermetic selftest. Journald and
remote transport are injected by their supervised owners; no process is spawned
here. Diagnostic tails in logs.sh cannot produce these absence receipts.
"""
import argparse
import base64
import array
import fcntl
import ctypes
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import re
import select
import signal
import threading
import socket
import struct
import sys
import time

class SetupError(RuntimeError):
    """Incomplete evidence has native SETUP-RED status 2."""

# origin: hot-update S10 plan §3.1 / §5.5. A whole omitted SID must fail too,
# not just an omitted member of a SID group still present in the input.
RUNTIMES = {
    "s10-small-push": ("agt1", "tether-agent-a0.service", "/home/sim-a0"),
    "s10-small-pull": ("agt1", "tether-agent-a1.service", "/home/sim-a1"),
    "s10-large-push": ("agt2", "tether-agent-a2.service", "/home/sim-a2"),
    "s10-large-pull": ("agt2", "tether-agent-a3.service", "/home/sim-a3"),
}
LIMIT = 256 * 1024 * 1024

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()

def sha(data):
    return hashlib.sha256(data).hexdigest()

def seal(value):
    value = dict(value)
    value["digest"] = sha(canonical({k: v for k, v in value.items() if k != "digest"}))
    return value

def require(ok, message):
    if not ok:
        raise SetupError(message)

def unique_pairs(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "duplicate JSON field")
        value[key] = item
    return value

def decode(data):
    return json.loads(data, object_pairs_hook=unique_pairs)

@lru_cache(maxsize=1)
def schema():
    # P11 stages this exact P00 artifact beside remote collectors. Resolve it
    # lazily so a missing supply artifact is reported as SETUP-RED by the CLI.
    path = (Path(os.environ["FULLSTACK_LOGS_SCHEMA"]) if "FULLSTACK_LOGS_SCHEMA" in os.environ else
            Path(__file__).resolve().parents[3] / "hotupdate/acceptance/contract/schema.json")
    return decode(path.read_bytes())

def validate(value, name):
    # Use P00's actual frozen schema rather than a second permissive DTO codec.
    definitions = schema()["$defs"]
    def check(item, spec):
        if "$ref" in spec:
            return check(item, definitions[spec["$ref"].split("/")[-1]])
        kind = spec.get("type")
        types = {"object": dict, "array": list, "string": str, "integer": int, "boolean": bool}
        if kind:
            require(type(item) is types[kind], "wrong JSON field type")
        if "const" in spec:
            require(type(item) is type(spec["const"]) and item == spec["const"], "wrong schema version")
        if "enum" in spec:
            require(item in spec["enum"], "unknown log vocabulary")
        if kind == "object":
            require(set(spec.get("required", [])) <= item.keys(), "missing JSON field")
            require(not (item.keys() - spec["properties"].keys()), "unknown JSON field")
            for key, child in item.items():
                check(child, spec["properties"][key])
        if kind == "array":
            require(len(item) >= spec.get("minItems", 0), "empty required log population")
            if spec.get("uniqueItems"):
                require(len({canonical(x) for x in item}) == len(item), "duplicate log member")
            for child in item:
                check(child, spec["items"])
        if kind == "string":
            require(len(item) >= spec.get("minLength", 0), "empty log identity")
            if "pattern" in spec:
                require(re.fullmatch(spec["pattern"], item) is not None, "invalid log identity")
        if kind == "integer":
            require(spec.get("minimum", item) <= item <= spec.get("maximum", item), "log integer out of range")
    check(value, definitions[name])
    require(value["digest"] == seal(value)["digest"], "log document digest mismatch")
    return value

def roster_streams(roster):
    validate(roster, "log_roster")
    groups, ids, agents = {}, set(), set()
    for stream in roster["streams"]:
        require(stream["id"] not in ids, "duplicate stream identity")
        ids.add(stream["id"])
        require(Path(stream["home"]).is_absolute() and ".." not in Path(stream["home"]).parts,
                "invalid managed HOME")
        key = tuple(stream[k] for k in ("host", "unit", "sid", "home", "boot_id", "invocation_id"))
        kinds = groups.setdefault(key, set())
        require(stream["source_kind"] not in kinds, "duplicate mapped log source")
        kinds.add(stream["source_kind"])
    brokers = 0
    for (host, unit, sid, home, _boot, _invocation), kinds in groups.items():
        if sid:
            require(sid in RUNTIMES and (host, unit, home) == RUNTIMES[sid], "wrong SID/unit/HOME mapping")
            require(sid not in agents, "duplicate runtime log mapping")
            agents.add(sid)
            require(kinds == {"agent-slog", "agent-boot-err", "prelogger-journal"}, "missing agent log stream")
        else:
            require(kinds == {"broker-slog", "broker-journal", "prelogger-journal"}, "missing broker log stream")
            brokers += 1
    require(agents == set(RUNTIMES) and brokers > 0, "missing required SID or broker")
    return roster["streams"]

class Watch:
    """A lost replacement event cannot be reconstructed from the final pathname."""
    def __init__(self, path):
        require(sys.platform == "linux", "strict log rotation requires Linux inotify")
        self.libc = ctypes.CDLL(None, use_errno=True)
        self.fd = self.libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if self.fd < 0:
            raise OSError(ctypes.get_errno(), "inotify initialization failed")
        self.name = path.name.encode()
        self.inodes = set()
        if self.libc.inotify_add_watch(self.fd, os.fsencode(path.parent), 0xFC6) < 0:
            self.close()
            raise OSError(ctypes.get_errno(), "log directory watch failed")

    def retain_inode(self, fd):
        # A directory watch stops naming a renamed/unlinked writer. Keep the
        # inode watch as long as its read FD so flag revocation cannot hide.
        wd = self.libc.inotify_add_watch(self.fd, os.fsencode('/proc/self/fd/' + str(fd)), 0x4)
        require(wd >= 0, "log inode watch failed")
        self.inodes.add(wd)

    def events(self):
        events = []
        while True:
            try:
                data = os.read(self.fd, 65536)
            except BlockingIOError:
                return events
            require(bool(data), "rotation watch closed")
            offset = 0
            while offset < len(data):
                _wd, mask, _cookie, length = struct.unpack_from("iIII", data, offset)
                name = data[offset + 16:offset + 16 + length].rstrip(b"\0")
                offset += 16 + length
                require(not mask & (0x4000 | 0x8000 | 0x400 | 0x800), "rotation event overflow or watch lost")
                if name == self.name or _wd in self.inodes:
                    events.append(mask)

    def close(self):
        if self.fd >= 0:
            fd, self.fd = self.fd, -1
            os.close(fd)

def append_only(fd):
    """Read the kernel guarantee; unsupported filesystems fail closed.

    The collector never changes inode flags or repairs logging. Provisioning
    owns append-only custody before READY; mutable/copytruncate sources need
    an upstream immutable spool and cannot currently sign this file reader.
    """
    flags = array.array('L', [0])
    try:
        # FS_IOC_GETFLAGS is _IOR('f', 1, long); use the native long width.
        fcntl.ioctl(fd, 0x80006601 | (flags.itemsize << 16), flags, True)
    except OSError:
        return False
    return bool(flags[0] & 0x20)  # FS_APPEND_FL prohibits overwrite and truncate.

def no_prior_writer(fd):
    # +a cannot certify a writable OFD opened before the flag was set. A read
    # lease proves those OFDs/mappings are absent before capture starts. The
    # brief probe only occurs during setup; unsupported/busy means unprovable.
    blocked = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGIO})
    try:
        try:
            fcntl.fcntl(fd, 15, struct.pack('ii', 0, threading.get_native_id()))  # F_SETOWN_EX / F_OWNER_TID
            fcntl.fcntl(fd, fcntl.F_SETLEASE, fcntl.F_RDLCK)
        except OSError:
            return False
        # A failed unlock must close this FD through the caller's unwind;
        # retaining a read lease would block the log writer after failed setup.
        fcntl.fcntl(fd, fcntl.F_SETLEASE, fcntl.F_UNLCK)
        return True
    finally:
        while signal.sigtimedwait({signal.SIGIO}, 0) is not None:
            pass
        signal.pthread_sigmask(signal.SIG_SETMASK, blocked)

class FileTrack:
    def __init__(self, path, limit=LIMIT):
        self.path, self.segments = path, []
        self.proven = True
        self.watch = Watch(path)
        try:
            self.poll(initial=True, limit=limit)
        except BaseException:
            self.close()
            raise

    def poll(self, initial=False, limit=LIMIT):
        # A replacement while reading is resumable because the original FD is
        # still held. Bound resampling; constant rotation cannot sign a fence.
        for _attempt in range(3):
            if self._poll(initial=initial, limit=limit):
                return
            initial = False
            select.select([self.watch.fd], [], [], 0.05)
        raise SetupError("log rotation or missing pathname never reached a stable end fence")

    def _poll(self, initial=False, limit=LIMIT):
        events = self.watch.events()
        # origin: S10 internal review rv-honesty F4
        # MODIFY/size/prefix cannot distinguish append from truncate+regrow.
        # Only kernel append-only custody across the WHOLE window can sign an
        # absence claim. ATTRIB is sticky: clearing then restoring +a is loss.
        self.proven = self.proven and not any(mask & 0x4 for mask in events)
        creations = sum(bool(mask & (0x100 | 0x80)) for mask in events)
        require(creations <= 1, "unobserved intermediate log rotation")
        try:
            current = os.stat(self.path, follow_symlinks=False)
        except FileNotFoundError:
            return initial or not self.segments
        require((current.st_mode & 0o170000) == 0o100000, "log source is not a regular file")
        key = (current.st_dev, current.st_ino)
        if not self.segments or key != self.segments[-1]["identity"]:
            require(initial or creations == 1, "log replacement lacks rotation witness")
            require(key not in [s["identity"] for s in self.segments], "log inode reused in rotation")
            fd = os.open(self.path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                opened = os.fstat(fd)
                require((opened.st_dev, opened.st_ino) == key, "log changed while opening")
                require((opened.st_mode & 0o170000) == 0o100000, "log source is not a regular file")
            except BaseException:
                os.close(fd)
                raise
            try:
                self.watch.retain_inode(fd)
                # A newly discovered inode has an unobserved history since
                # creation; setting +a before this read cannot repair that gap.
                self.proven = self.proven and initial and append_only(fd) and no_prior_writer(fd)
            except BaseException:
                os.close(fd)
                raise
            self.segments.append({"fd": fd, "identity": key, "data": b"", "mtime": None, "ctime": opened.st_ctime_ns, "rotation": not initial})
        total = 0
        for segment in self.segments:
            fd, old = segment["fd"], segment["data"]
            self.proven = self.proven and append_only(fd)
            observed = os.fstat(fd)
            # The generic SETFLAGS path does not promise an inotify event.
            # ctime cannot distinguish flag revocation from benign appends, so
            # either invalidates this sampler's proof. Live writers need an
            # immutable upstream spool, not a sampled +a flag assertion.
            self.proven = self.proven and observed.st_ctime_ns == segment["ctime"]
            fence = observed.st_size
            total += fence
            require(total <= limit, "log archive overflow before read")
            require(fence >= len(old), "log truncated on original inode")
            data = os.pread(fd, fence, 0)
            require(len(data) == fence, "short log read")
            require(data.startswith(old), "log rewritten on original inode")
            again = os.pread(fd, fence, 0)
            after = os.fstat(fd)
            require(again == data and after.st_size >= fence and
                    (after.st_size > fence or after.st_mtime_ns == observed.st_mtime_ns),
                    "log changed during read verification")
            # inotify can deliver an append event AFTER the read already
            # captured it. The inode's observed mtime distinguishes that late
            # notification from a fresh identical-content rewrite, including
            # one through an unlinked old writer FD.
            if segment["mtime"] is not None:
                require(fence > len(old) or observed.st_mtime_ns == segment["mtime"],
                        "in-place log mutation without append")
            segment["data"] = data
            segment["mtime"] = observed.st_mtime_ns
        try:
            current = os.stat(self.path, follow_symlinks=False)
        except FileNotFoundError:
            return False
        return (current.st_dev, current.st_ino) == key

    def retained(self):
        # Notifications not consumed by the last read cannot certify a fence.
        self.proven = self.proven and not self.watch.events()
        return self.proven and all(append_only(s["fd"]) and os.fstat(s["fd"]).st_ctime_ns == s["ctime"]
                                   for s in self.segments)

    def snapshot(self):
        return {"path": str(self.path), "retained": self.retained(), "segments": [
            {"device": s["identity"][0], "inode": s["identity"][1], "rotation": s["rotation"],
             "bytes": len(s["data"]), "sha": sha(s["data"]),
             "data": base64.b64encode(s["data"]).decode()} for s in self.segments]}

    def close(self):
        errors, segments, self.segments = [], self.segments, []
        for fd in [s["fd"] for s in segments] + [self.watch.fd]:
            try:
                os.close(fd)
            except OSError as exc:
                errors.append(str(exc))
        self.watch.fd = -1
        require(not errors, "log descriptor cleanup failed: " + "; ".join(errors))

class FileStream:
    def __init__(self, paths, limit=LIMIT):
        self.tracks = []
        try:
            for path in paths:
                used = sum(len(s["data"]) for t in self.tracks for s in t.segments)
                self.tracks.append(FileTrack(path, limit=limit - used))
            require(any(t.segments for t in self.tracks), "required log file absent")
        except BaseException:
            self.close()
            raise

    def poll(self, limit=LIMIT):
        used = 0
        for track in self.tracks:
            track.poll(limit=limit - used)
            used += sum(len(s["data"]) for s in track.segments)

    def snapshot(self):
        return {"kind": "file", "parts": [t.snapshot() for t in self.tracks]}

    def close(self):
        errors, tracks, self.tracks = [], self.tracks, []
        for track in tracks:
            try:
                track.close()
            except (SetupError, OSError) as exc:
                errors.append(str(exc))
        require(not errors, "log stream cleanup failed: " + "; ".join(errors))

class JournalStream:
    def __init__(self, stream, query, limit=LIMIT):
        self.stream, self.query, self.rows = stream, query, []
        self.reader_rc = None
        self.poll(limit=limit)

    def poll(self, limit=LIMIT):
        # origin: hot-update S10 plan §5.5. Keep native reader status and every
        # cursor, including the prelogger rows preceding fd2 redirection.
        rc, data = self.query(self.stream)
        self.reader_rc = rc
        require(type(rc) is int and rc == 0, "journal reader failed (native rc " + str(rc) + ")")
        require(len(data) <= limit, "journal archive overflow")
        rows = [decode(line) for line in data.splitlines()]
        require(bool(rows), "journal lacks valid start/end cursor")
        cursors = set()
        for row in rows:
            require(isinstance(row, dict) and type(row.get("__CURSOR")) is str and row["__CURSOR"],
                    "journal cursor missing")
            require(row["__CURSOR"] not in cursors, "duplicate journal cursor")
            cursors.add(row["__CURSOR"])
            require(row.get("_BOOT_ID") == self.stream["boot_id"] and
                    row.get("_SYSTEMD_UNIT", row.get("UNIT")) == self.stream["unit"] and
                    row.get("_SYSTEMD_INVOCATION_ID") == self.stream["invocation_id"],
                    "journal boot/unit/invocation mismatch")
            message = row.get("MESSAGE")
            require(type(message) is str or (type(message) is list and
                    all(type(x) is int and 0 <= x <= 255 for x in message)), "invalid journal payload")
        require(rows[:len(self.rows)] == self.rows, "journal vacuum or cursor prefix lost")
        self.rows = rows

    def snapshot(self):
        return {"kind": "journal", "reader_rc": self.reader_rc, "rows": self.rows}

    def close(self):
        self.rows = []

class LocalReader:
    """One supervised host adapter. P16 supplies a reader per roster host.

    identity(unit) returns the observed (boot_id, invocation_id), and journal
    returns (native_rc, complete JSON bytes) via the P05 supervised command path.
    No shell snippets or caller-supplied log paths are accepted.
    """
    def __init__(self, root, identity, journal):
        self.root = Path(root).resolve()
        self.identity, self.journal = identity, journal

    def open(self, stream, limit=LIMIT):
        require(self.identity(stream["unit"]) == (stream["boot_id"], stream["invocation_id"]),
                "observed unit identity mismatch")
        kind = stream["source_kind"]
        if kind in {"broker-journal", "prelogger-journal"}:
            return JournalStream(stream, self.journal, limit=limit)
        # This is the strict branch of the sole logs owner. Legacy broker.err
        # remains a distinct component, never a substitute for a missing agent.
        paths = (["/var/log/tether/broker.log", "/var/log/tether/broker.err"] if kind == "broker-slog" else
                 [stream["home"] + "/.tether/agent/" + stream["sid"] + "/" +
                  ("agent.log" if kind == "agent-slog" else "agent.boot.err")])
        resolved = [self.root / path.lstrip("/") for path in paths]
        require(all(path.resolve().is_relative_to(self.root) for path in resolved), "log path escapes host root")
        return FileStream(resolved, limit=limit)

def write_document(path, value):
    path = Path(path)
    data = canonical(value) + b"\n"
    with path.open("xb") as output:
        os.chmod(path, 0o600)
        require(output.write(data) == len(data), "short log archive write")
        output.flush()
        os.fsync(output.fileno())
    return {"path": str(path.resolve()), "sha": sha(data), "bytes": len(data), "schema": "fullstack-log-raw-v1"}

def raw_document(ref):
    require(ref["schema"] == "fullstack-log-raw-v1", "unknown log raw schema")
    require(0 <= ref["bytes"] <= LIMIT and Path(ref["path"]).stat().st_size == ref["bytes"],
            "log raw archive digest/length mismatch")
    with Path(ref["path"]).open("rb") as source:
        data = source.read(ref["bytes"] + 1)
    require(len(data) == ref["bytes"] and sha(data) == ref["sha"], "log raw archive digest/length mismatch")
    raw = decode(data)
    require(type(raw) is dict and set(raw) == {"version", "stream", "sequence", "fence_ns", "snapshot"},
            "unknown or missing log raw field")
    return raw

def position(stream, snapshot, sequence, ref):
    if snapshot["kind"] == "journal":
        return dict(stream=stream, cursor=snapshot["rows"][-1]["__CURSOR"], device=0, inode=0,
                    offset=len(snapshot["rows"]), sequence=sequence, raw=ref)
    segments = [s for part in snapshot["parts"] for s in part["segments"]]
    primary = next(part["segments"][-1] for part in snapshot["parts"] if part["segments"])
    return dict(stream=stream, cursor="file:" + sha(canonical(snapshot)), device=primary["device"],
                inode=primary["inode"], offset=sum(s["bytes"] for s in segments), sequence=sequence, raw=ref)

class Capture:
    """No owned thread: P16 polls and closes/joins this scope in its lifecycle."""
    def __init__(self, readers, archive_root, limit=LIMIT):
        self.readers, self.archive_root, self.limit = readers, Path(archive_root).resolve(), limit
        self.sources, self.streams, self.cursor, self.poison = [], [], None, None
        self.sequence, self.closed, self.drained, self.archived_bytes = 0, False, False, 0

    def start(self, roster):
        require(self.cursor is None and not self.sources and not self.closed, "log capture already started")
        try:
            self.streams = roster_streams(roster)
            require({s["host"] for s in self.streams} == self.readers.keys(), "required host reader absent or extra")
            used = 0
            for stream in self.streams:
                require(stream["host"] in self.readers, "required host reader absent")
                source = self.readers[stream["host"]].open(stream, limit=self.limit - used)
                self.sources.append(source)
                used += len(canonical(source.snapshot()))
                require(used <= self.limit, "log archive overflow")
            self.poll()
            positions = self._archive(self.archive_root / "start")
            self.cursor = seal(dict(version=1, roster_digest=roster["digest"], positions=positions))
            validate(self.cursor, "log_cursor")
            return self.cursor
        except BaseException as exc:
            self._poison(exc)
            self.close()
            raise

    def poll(self):
        require(not self.closed and self.poison is None, "log capture closed or poisoned")
        try:
            used = 0
            for stream, source in zip(self.streams, self.sources):
                reader = self.readers[stream["host"]]
                require(reader.identity(stream["unit"]) == (stream["boot_id"], stream["invocation_id"]),
                        "unit identity changed during capture")
                source.poll(limit=self.limit - used)
                used += len(canonical(source.snapshot()))
                require(used <= self.limit, "log archive overflow")
            self.sequence += 1
        except BaseException as exc:
            self._poison(exc)
            raise

    def _poison(self, exc):
        if self.poison is None:
            self.poison = str(exc)
        # Preserve the original reader failure even if the failed start could
        # not produce a cursor. A failed archive never has a complete end fence.
        path = self.archive_root / "failure.json"
        if not path.exists():
            try:
                write_document(path, dict(version=1, complete=False, sequence=self.sequence, error=self.poison))
            except (OSError, SetupError):
                pass  # The original failure still propagates; never return success.

    def _archive(self, directory):
        directory = directory.resolve()
        require(directory.is_relative_to(self.archive_root), "raw directory escapes capture archive")
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        positions = []
        for index, (stream, source) in enumerate(zip(self.streams, self.sources)):
            snapshot = source.snapshot()
            raw = dict(version=1, stream=stream, sequence=self.sequence, fence_ns=time.monotonic_ns(),
                       snapshot=snapshot)
            self.archived_bytes += len(canonical(raw)) + 1
            require(self.archived_bytes <= self.limit, "log archive overflow at end fence")
            ref = write_document(directory / (str(index) + ".json"), raw)
            positions.append(position(stream, snapshot, self.sequence, ref))
        return positions

    def drain(self, cursor, raw_dir):
        require(cursor == self.cursor and not self.drained, "wrong or replayed log capture cursor")
        try:
            self.poll()
            end = seal(dict(version=1, cursor_digest=cursor["digest"], positions=self._archive(Path(raw_dir)), complete=True))
            verify(cursor, end)
            self.drained = True
            return end
        except BaseException as exc:
            self._poison(exc)
            raise

    def close(self):
        errors, sources, self.sources = [], self.sources, []
        for source in sources:
            try:
                source.close()
            except (SetupError, OSError) as exc:
                errors.append(str(exc))
        self.closed = True
        if errors:
            self._poison(SetupError("log collector cleanup failed: " + "; ".join(errors)))
            raise SetupError(self.poison)

    def join(self):
        require(self.closed and not self.sources, "log collector must close before join")
        require(self.poison is None, "log collector cleanup retains observation poison")

def file_bytes(segment):
    require(set(segment) == {"device", "inode", "rotation", "bytes", "sha", "data"} and
            all(type(segment[k]) is int and segment[k] >= 0 for k in ("device", "inode", "bytes")) and
            segment["inode"] > 0 and type(segment["rotation"]) is bool, "invalid log segment fields")
    data = base64.b64decode(segment["data"], validate=True)
    require(len(data) == segment["bytes"] and sha(data) == segment["sha"], "log segment digest/length mismatch")
    return data

def verify(cursor, end):
    # origin: hot-update S10 plan §5.5 / FullstackLogCaptureTest. Reconstruct
    # every source interval independently; a completed marker cannot hide gaps.
    validate(cursor, "log_cursor")
    validate(end, "log_end")
    require(end["complete"] is True and end["cursor_digest"] == cursor["digest"], "log end fence incomplete")
    roster = seal(dict(version=1, streams=[p["stream"] for p in cursor["positions"]]))
    roster_streams(roster)
    require(cursor["roster_digest"] == roster["digest"], "log roster digest mismatch")
    require([p["stream"] for p in end["positions"]] == [p["stream"] for p in cursor["positions"]],
            "log end population changed")
    for start, finish in zip(cursor["positions"], end["positions"]):
        before, after = raw_document(start["raw"]), raw_document(finish["raw"])
        for pos, raw in ((start, before), (finish, after)):
            require(raw["version"] == 1 and raw["stream"] == pos["stream"] and raw["sequence"] == pos["sequence"],
                    "raw log identity/sequence mismatch")
            require(position(pos["stream"], raw["snapshot"], pos["sequence"], pos["raw"]) == pos,
                    "raw log position mismatch")
        require(finish["sequence"] > start["sequence"] and after["fence_ns"] > before["fence_ns"],
                "stale log end fence")
        a, b = before["snapshot"], after["snapshot"]
        require(a["kind"] == b["kind"], "log source kind changed")
        kind = "journal" if finish["stream"]["source_kind"] in {"broker-journal", "prelogger-journal"} else "file"
        require(a["kind"] == kind, "raw log source does not match mapping")
        if a["kind"] == "journal":
            require(set(a) == set(b) == {"kind", "reader_rc", "rows"}, "invalid journal raw fields")
            require(a["reader_rc"] == b["reader_rc"] == 0 and b["rows"][:len(a["rows"])] == a["rows"],
                    "journal end lost cursor prefix")
            JournalStream(finish["stream"], lambda _s: (0, b"\n".join(canonical(r) for r in b["rows"])))
        else:
            require(set(a) == set(b) == {"kind", "parts"}, "invalid file raw fields")
            require([p["path"] for p in a["parts"]] == [p["path"] for p in b["parts"]], "log component missing")
            for old, new in zip(a["parts"], b["parts"]):
                require(set(old) == set(new) == {"path", "retained", "segments"}, "invalid log component fields")
                require(old["retained"] is True and new["retained"] is True,
                        "log byte retention is unprovable; append-only custody or immutable spool required")
                require(len(new["segments"]) >= len(old["segments"]), "rotated log segment missing")
                identities = [(s["device"], s["inode"]) for s in new["segments"]]
                require(len(set(identities)) == len(identities), "duplicate rotated log segment")
                for index, segment in enumerate(new["segments"]):
                    data = file_bytes(segment)
                    if index < len(old["segments"]):
                        prior = old["segments"][index]
                        require(identities[index] == (prior["device"], prior["inode"]) and
                                data.startswith(file_bytes(prior)), "log original prefix changed")
                    else:
                        require(segment["rotation"] is True, "new log segment lacks rotation witness")
    return True

def serve(capture, endpoint, done):
    """P05/P16 owns process death, done, Close/Join and the private endpoint."""
    with socket.socket(socket.AF_UNIX) as listener:
        listener.bind(str(endpoint))
        os.chmod(endpoint, 0o600)
        listener.listen(8)
        try:
            while not done.is_set():
                if not select.select([listener], [], [], 0.25)[0]:
                    if capture.cursor is not None and not capture.drained and capture.poison is None:
                        try:
                            capture.poll()
                        except (SetupError, OSError, ValueError):
                            pass  # Poison remains sticky and drain reports SETUP-RED.
                    continue
                with listener.accept()[0] as connection:
                    connection.settimeout(5)
                    try:
                        request = decode(connection.makefile("rb").readline(4 * 1024 * 1024 + 1))
                        action = request["action"]
                        if action == "start" and set(request) == {"action", "roster"}:
                            result = capture.start(request["roster"])
                        elif action == "drain" and set(request) == {"action", "cursor", "raw_dir"}:
                            result = capture.drain(request["cursor"], request["raw_dir"])
                        else:
                            raise SetupError("unknown log collector request")
                        response = dict(rc=0, result=result)
                    except (SetupError, OSError, ValueError, KeyError, TypeError) as exc:
                        response = dict(rc=2, error=str(exc))
                    connection.sendall(canonical(response) + b"\n")
        finally:
            try:
                capture.close()
            finally:
                Path(endpoint).unlink()

def client(endpoint, request):
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(30)
        require(os.stat(endpoint).st_uid == os.getuid() and os.stat(endpoint).st_mode & 0o077 == 0,
                "log endpoint is not private to collector UID")
        connection.connect(endpoint)
        connection.sendall(canonical(request) + b"\n")
        response = decode(connection.makefile("rb").readline(4 * 1024 * 1024 + 1))
        require(type(response.get("rc")) is int and response["rc"] == 0, response.get("error", "log collector failed"))
        return response["result"]

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "drain", "verify"))
    parser.add_argument("paths", nargs="+")
    args = parser.parse_args(argv)
    expected = {"start": 2, "drain": 3, "verify": 2}[args.action]
    require(len(args.paths) == expected, "wrong strict logs argument count")
    document = decode(Path(args.paths[0]).read_bytes())
    if args.action == "verify":
        verify(document, decode(Path(args.paths[1]).read_bytes()))
        return
    endpoint = os.environ.get("FULLSTACK_LOGS_SOCKET")
    require(bool(endpoint), "P05/P16 supervised log collector endpoint missing")
    if args.action == "start":
        roster_streams(document)
        request = dict(action="start", roster=document)
    else:
        validate(document, "log_cursor")
        request = dict(action="drain", cursor=document, raw_dir=args.paths[1])
    result = client(endpoint, request)
    validate(result, "log_cursor" if args.action == "start" else "log_end")
    write_document(args.paths[-1], result)

if __name__ == "__main__":
    try:
        main()
    except (SetupError, OSError, ValueError, KeyError, TypeError) as error:
        print("strict log capture failed: " + str(error), file=sys.stderr)
        sys.exit(2)
