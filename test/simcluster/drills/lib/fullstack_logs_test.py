#!/usr/bin/env python3
"""Real temporary log files/inotify; injected journal JSON, no service processes.

Does not certify deploy journald, remote transport or P16 process supervision.
"""
import copy
import importlib.util
import json
import os
from pathlib import Path
import struct
import tempfile
import threading
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("fullstack_logs", Path(__file__).with_name("fullstack_logs.py"))
logs = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(logs)


def stream(identity, host, unit, sid, home, kind):
    return dict(id=identity, host=host, unit=unit, sid=sid, home=home, boot_id="boot-" + host,
                invocation_id="inv-" + unit, source_kind=kind)


class Fixture:
    def __init__(self, root, limit=logs.LIMIT):
        self.root, self.archive = Path(root), Path(root) / "archive"
        self.archive.mkdir()
        streams = [stream("broker-" + kind, "brk1", "tether-broker.service", "", "/var/lib/tether", kind)
                   for kind in ("broker-slog", "broker-journal", "prelogger-journal")]
        for index, (sid, (host, unit, home)) in enumerate(logs.RUNTIMES.items()):
            streams.extend(stream("a" + str(index) + "-" + kind, host, unit, sid, home, kind)
                           for kind in ("agent-slog", "agent-boot-err", "prelogger-journal"))
        self.roster = logs.seal(dict(version=1, streams=streams))
        self.journals, self.status = {}, 0
        for item in streams:
            key = (item["host"], item["unit"])
            self.journals[key] = [self.row(item, 0, "baseline")]
        for path in self.file_paths():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"baseline\n")
        reader = logs.LocalReader(self.root, self.identity, self.query)
        self.capture = logs.Capture({host: reader for host in ("brk1", "agt1", "agt2")}, self.archive, limit)

    def identity(self, unit):
        for item in self.roster["streams"]:
            if item["unit"] == unit:
                return item["boot_id"], item["invocation_id"]
        raise logs.SetupError("unknown unit")

    def row(self, item, index, message):
        return {"__CURSOR": item["unit"] + ":" + str(index), "_BOOT_ID": item["boot_id"],
                "_SYSTEMD_UNIT": item["unit"], "_SYSTEMD_INVOCATION_ID": item["invocation_id"], "MESSAGE": message}

    def query(self, item):
        return self.status, b"\n".join(logs.canonical(row) for row in self.journals[(item["host"], item["unit"])])

    def file_paths(self):
        yield self.root / "var/log/tether/broker.log"
        yield self.root / "var/log/tether/broker.err"
        for sid, (_host, _unit, home) in logs.RUNTIMES.items():
            for name in ("agent.log", "agent.boot.err"):
                yield self.root / home.lstrip("/") / ".tether/agent" / sid / name

    def append(self, path, data):
        with path.open("ab") as output:
            output.write(data)

    def end(self, cursor):
        return self.capture.drain(cursor, self.archive / "end")

    def close(self):
        self.capture.close()


# origin: hot-update S10 plan §5.5 / §10.2 P09.
class FullstackLogCaptureTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="s10-log-capture-")
        self.fixture = Fixture(self.temporary.name)
        # These reader/codec tests inject only the retention certificate; bytes,
        # inode watches, rotation and all existing loss assertions remain real.
        # StrictLogRetentionTest exercises the actual kernel proof without it.
        self.retention = patch.object(logs.FileTrack, "retained", return_value=True)
        self.retention.start()
        self.addCleanup(self.retention.stop)

    def tearDown(self):
        self.fixture.close()
        self.temporary.cleanup()

    def rejected(self, call, message):
        with self.assertRaisesRegex(logs.SetupError, message):
            call()

    def test_all_four_streams_preserve_exact_bytes_and_prelogger(self):
        f = self.fixture
        cursor = f.capture.start(f.roster)
        expected = {}
        for index, path in enumerate(f.file_paths()):
            suffix = ("panic: stream " + str(index)).encode() + b"\x00\xff\r\n"
            f.append(path, suffix)
            expected[str(path)] = b"baseline\n" + suffix
        for item in f.roster["streams"]:
            if item["source_kind"] == "prelogger-journal":
                key = (item["host"], item["unit"])
                f.journals[key].append(f.row(item, 1, "panic: before fd2 redirection"))
        end = f.end(cursor)
        self.assertTrue(logs.verify(cursor, end))
        self.assertEqual(len(end["positions"]), 15)
        kinds = {p["stream"]["source_kind"] for p in end["positions"]}
        self.assertEqual(kinds, {"broker-slog", "broker-journal", "agent-slog", "agent-boot-err", "prelogger-journal"})
        for pos in end["positions"]:
            raw = logs.raw_document(pos["raw"])["snapshot"]
            if raw["kind"] == "journal":
                self.assertEqual(raw["rows"][-1]["MESSAGE"], "panic: before fd2 redirection")
            else:
                item = pos["stream"]
                wanted = ([f.root / "var/log/tether" / name for name in ("broker.log", "broker.err")]
                          if item["source_kind"] == "broker-slog" else
                          [f.root / item["home"].lstrip("/") / ".tether/agent" / item["sid"] /
                           ("agent.log" if item["source_kind"] == "agent-slog" else "agent.boot.err")])
                self.assertEqual([part["path"] for part in raw["parts"]], [str(path) for path in wanted])
                for part in raw["parts"]:
                    self.assertEqual(logs.file_bytes(part["segments"][0]), expected[part["path"]])
        f.capture.close()
        f.capture.join()

    def test_missing_sid_or_each_required_stream_is_setup_failure(self):
        for item in self.fixture.roster["streams"]:
            with self.subTest(stream=item["id"]):
                roster = copy.deepcopy(self.fixture.roster)
                roster["streams"].remove(item)
                self.rejected(lambda: logs.roster_streams(logs.seal(roster)), "missing")
        for sid in logs.RUNTIMES:
            with self.subTest(sid=sid):
                roster = copy.deepcopy(self.fixture.roster)
                roster["streams"] = [s for s in roster["streams"] if s["sid"] != sid]
                self.rejected(lambda: logs.roster_streams(logs.seal(roster)), "missing required SID")

    def test_wrong_unit_home_boot_or_invocation_is_rejected(self):
        for field in ("unit", "home", "host"):
            with self.subTest(field=field):
                roster = copy.deepcopy(self.fixture.roster)
                for item in roster["streams"]:
                    if item["sid"] == "s10-small-push":
                        item[field] = "/wrong" if field == "home" else "wrong"
                self.rejected(lambda: logs.roster_streams(logs.seal(roster)), "mapping")
        for field in ("_BOOT_ID", "_SYSTEMD_UNIT", "_SYSTEMD_INVOCATION_ID"):
            with self.subTest(field=field):
                item = self.fixture.roster["streams"][1]
                row = self.fixture.row(item, 0, "panic")
                row[field] = "wrong"
                self.rejected(lambda: logs.JournalStream(item, lambda _s: (0, logs.canonical(row))), "mismatch")

    def test_rotation_retains_original_fd_and_late_old_writer(self):
        f = self.fixture
        cursor = f.capture.start(f.roster)
        path = next(f.file_paths())
        writer = path.open("ab")
        self.addCleanup(writer.close)
        original = path.stat()
        path.rename(path.with_suffix(".rotated"))
        path.write_bytes(b"successor\n")
        f.capture.poll()
        # Unlink makes reopening impossible. The old writer still has valid
        # byte responsibility, so keeping only the final path loses its panic.
        path.with_suffix(".rotated").unlink()
        writer.write(b"panic: old inode tail\n")
        writer.flush()
        f.append(path, b"new inode tail\n")
        end = f.end(cursor)
        raw = logs.raw_document(end["positions"][0]["raw"])["snapshot"]
        segments = raw["parts"][0]["segments"]
        self.assertEqual(len(segments), 2)
        self.assertEqual((segments[0]["device"], segments[0]["inode"]), (original.st_dev, original.st_ino))
        self.assertEqual(logs.file_bytes(segments[0]), b"baseline\npanic: old inode tail\n")
        self.assertEqual(logs.file_bytes(segments[1]), b"successor\nnew inode tail\n")
        self.assertEqual(end["positions"][0]["inode"], path.stat().st_ino)
        self.assertTrue(logs.verify(cursor, end))

    def test_intermediate_rotation_loss_is_rejected(self):
        f = self.fixture
        cursor = f.capture.start(f.roster)
        path = next(f.file_paths())
        for index in range(2):
            path.rename(path.with_suffix(".rotate" + str(index)))
            path.write_bytes(b"intermediate panic\n")
        self.rejected(lambda: f.end(cursor), "intermediate log rotation")

    def test_late_append_notification_does_not_poison_complete_bytes(self):
        f = self.fixture
        cursor = f.capture.start(f.roster)
        path, source = next(f.file_paths()), f.capture.sources[0].tracks[0]
        f.append(path, b"append already captured before notification\n")
        with patch.object(source.watch, "events", return_value=[]):
            source.poll()
        # The real IN_MODIFY is still pending, but the prior read already
        # accounts for its exact bytes and inode mtime. It is not a rewrite.
        try:
            end = f.end(cursor)
        except logs.SetupError as exc:
            self.fail("delayed append notification poisoned complete bytes: " + str(exc))
        self.assertTrue(logs.verify(cursor, end))

    def test_rotation_during_read_resamples_without_losing_original_bytes(self):
        f = self.fixture
        cursor = f.capture.start(f.roster)
        path, source = next(f.file_paths()), f.capture.sources[0].tracks[0]
        fd, pread, rotated = source.segments[0]["fd"], os.pread, []
        def rotating_read(actual_fd, count, offset):
            data = pread(actual_fd, count, offset)
            if actual_fd == fd and not rotated:
                path.rename(path.with_suffix(".during-read"))
                path.write_bytes(b"panic: successor during original read\n")
                rotated.append(True)
            return data
        with patch.object(logs.os, "pread", rotating_read):
            end = f.end(cursor)
        raw = logs.raw_document(end["positions"][0]["raw"])["snapshot"]
        self.assertEqual(len(raw["parts"][0]["segments"]), 2, "end fence lost rotation during read")
        self.assertEqual(logs.file_bytes(raw["parts"][0]["segments"][1]), b"panic: successor during original read\n")
        self.assertEqual(end["positions"][0]["inode"], path.stat().st_ino)

    def test_original_inode_truncate_and_equal_size_rewrite_are_rejected(self):
        for data in (b"", b"rewritten", b"baseline\n"):
            with self.subTest(data=data), tempfile.TemporaryDirectory(prefix="s10-log-truncate-") as directory:
                f = Fixture(directory)
                try:
                    cursor = f.capture.start(f.roster)
                    path = next(f.file_paths())
                    inode = path.stat().st_ino
                    path.write_bytes(data)
                    self.assertEqual(path.stat().st_ino, inode)
                    self.rejected(lambda: f.end(cursor), "truncated|rewritten|mutation")
                finally:
                    f.close()

    def test_empty_vacuum_and_reader_failure_are_not_zero_anomalies(self):
        f = self.fixture
        cursor = f.capture.start(f.roster)
        key = ("brk1", "tether-broker.service")
        baseline = copy.deepcopy(f.journals[key])
        for rows, rc, message in (([], 0, "cursor"), (baseline, 75, "reader failed"),
                                  ([dict(baseline[0], __CURSOR="replacement")], 0, "vacuum")):
            with self.subTest(rc=rc, rows=rows):
                source = logs.JournalStream(f.roster["streams"][1], lambda _s: (0, b"\n".join(logs.canonical(r) for r in baseline)))
                source.query = lambda _s: (rc, b"\n".join(logs.canonical(r) for r in rows))
                self.rejected(source.poll, message)
        f.status = 75
        self.rejected(lambda: f.end(cursor), "journal reader failed")
        failure = logs.decode((f.archive / "failure.json").read_bytes())
        self.assertFalse(failure["complete"])
        self.assertIn("native rc 75", failure["error"])
        f.status = 0
        self.rejected(lambda: f.end(cursor), "poisoned")

    def test_midread_byte_loss_is_rejected(self):
        f = self.fixture
        cursor = f.capture.start(f.roster)
        f.append(next(f.file_paths()), b"fresh suffix\n")
        pread = os.pread
        with patch.object(logs.os, "pread", side_effect=lambda fd, count, offset: pread(fd, count, offset)[:-1]):
            self.rejected(lambda: f.end(cursor), "short log read")

    def test_identical_rewrite_between_reads_is_rejected(self):
        f = self.fixture
        cursor = f.capture.start(f.roster)
        path, source = next(f.file_paths()), f.capture.sources[0].tracks[0]
        fd, pread, changed = source.segments[0]["fd"], os.pread, []
        def rewritten_read(actual_fd, count, offset):
            data = pread(actual_fd, count, offset)
            if actual_fd == fd and not changed:
                path.write_bytes(b"baseline\n")
                changed.append(True)
            return data
        with patch.object(logs.os, "pread", rewritten_read):
            self.rejected(lambda: f.end(cursor), "changed during read verification")

    def test_missing_live_path_cannot_sign_an_end_fence(self):
        f = self.fixture
        cursor = f.capture.start(f.roster)
        next(f.file_paths()).unlink()
        self.rejected(lambda: f.end(cursor), "missing pathname.*stable end fence")

    def test_missing_unreadable_overflow_and_watch_loss_are_rejected(self):
        f = self.fixture
        missing = list(f.file_paths())[2]
        missing.unlink()
        self.rejected(lambda: f.capture.start(f.roster), "required log file absent")
        with tempfile.TemporaryDirectory(prefix="s10-log-permission-") as directory:
            other = Fixture(directory)
            try:
                with patch.object(logs.os, "pread", side_effect=PermissionError("injected read refusal")):
                    with self.assertRaises(PermissionError):
                        other.capture.start(other.roster)
                self.assertEqual(other.capture.sources, [])
            finally:
                other.close()
        with tempfile.TemporaryDirectory(prefix="s10-log-overflow-") as directory:
            other = Fixture(directory, limit=1)
            try:
                self.rejected(lambda: other.capture.start(other.roster), "overflow")
            finally:
                other.close()
        event = struct.pack("iIII", -1, 0x4000, 0, 0)
        watch = logs.Watch(next(f.file_paths()))
        try:
            with patch.object(logs.os, "read", side_effect=[event, BlockingIOError()]):
                self.rejected(watch.events, "overflow")
        finally:
            watch.close()

    def test_nonregular_and_symlink_log_sources_are_setup_failures(self):
        for kind in ("fifo", "symlink"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory(prefix="s10-log-kind-") as directory:
                f = Fixture(directory)
                try:
                    path = list(f.file_paths())[2]
                    path.unlink()
                    if kind == "fifo":
                        os.mkfifo(path)
                    else:
                        path.symlink_to(next(f.file_paths()))
                    self.rejected(lambda: f.capture.start(f.roster), "regular file")
                finally:
                    f.close()

    def test_sealed_archives_reject_tamper_stale_fence_and_missing_population(self):
        f = self.fixture
        cursor = f.capture.start(f.roster)
        end = f.end(cursor)
        for change in ("digest", "population", "complete", "cursor", "sequence"):
            with self.subTest(change=change):
                bad = copy.deepcopy(end)
                if change == "digest":
                    bad["digest"] = "0" * 64
                elif change == "population":
                    bad["positions"].pop()
                    bad = logs.seal(bad)
                elif change == "complete":
                    bad["complete"] = False
                    bad = logs.seal(bad)
                elif change == "cursor":
                    bad["cursor_digest"] = "0" * 64
                    bad = logs.seal(bad)
                else:
                    bad["positions"][0]["sequence"] = 0
                    bad = logs.seal(bad)
                self.rejected(lambda: logs.verify(cursor, bad), "digest|population|incomplete|sequence")
        ref = end["positions"][0]["raw"]
        path = Path(ref["path"])
        path.write_bytes(path.read_bytes()[:-1])
        self.rejected(lambda: logs.verify(cursor, end), "digest/length")

    def test_schema_unknown_duplicate_and_overflow_fields_fail_closed(self):
        roster = copy.deepcopy(self.fixture.roster)
        for value in (dict(roster, extra=True), dict(roster, version=True)):
            self.rejected(lambda: logs.roster_streams(logs.seal(value)), "field|version")
        self.rejected(lambda: logs.decode('{"version":1,"version":1}'), "duplicate")
        cursor = self.fixture.capture.start(self.fixture.roster)
        cursor["positions"][0]["sequence"] = 2 ** 64
        self.rejected(lambda: logs.validate(logs.seal(cursor), "log_cursor"), "out of range")

    def test_staged_collector_consumes_the_frozen_supplied_schema(self):
        stage = self.root_path("staged")
        stage.mkdir()
        tool, contract = stage / "logs.py", stage / "contract.json"
        tool.write_bytes(Path(logs.__file__).read_bytes())
        contract.write_bytes((Path(logs.__file__).resolve().parents[3] /
                              "hotupdate/acceptance/contract/schema.json").read_bytes())
        spec = importlib.util.spec_from_file_location("staged_strict_logs", tool)
        peer = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {"FULLSTACK_LOGS_SCHEMA": str(contract)}):
            spec.loader.exec_module(peer)
            try:
                self.assertEqual(peer.validate(self.fixture.roster, "log_roster"), self.fixture.roster)
            except (OSError, IndexError) as exc:
                self.fail("staged collector cannot consume the supplied contract: " + str(exc))

    def test_private_collector_control_client_preserves_setup_status(self):
        f = self.fixture
        baseline = len(list(Path("/proc/self/fd").iterdir()))
        endpoint, done = self.root_path("collector.sock"), threading.Event()
        errors = []
        def run():
            try:
                logs.serve(f.capture, endpoint, done)
            except BaseException as exc:
                errors.append(exc)
        # Bind is the readiness receipt. No daemon, service, process or sleep
        # barrier is involved in this protocol-only selftest.
        ready = threading.Event()
        original = logs.socket.socket.listen
        def listen(sock, backlog):
            original(sock, backlog)
            ready.set()
        with patch.object(logs.socket.socket, "listen", listen):
            worker = threading.Thread(target=run)
            worker.start()
            try:
                self.assertTrue(ready.wait(5), "collector failed to bind")
                roster_path, cursor_path, end_path = (self.root_path(name) for name in
                                                     ("roster.json", "cursor.json", "end.json"))
                logs.write_document(roster_path, f.roster)
                with patch.dict(os.environ, {"FULLSTACK_LOGS_SOCKET": str(endpoint)}):
                    logs.main(["start", str(roster_path), str(cursor_path)])
                    cursor = logs.decode(cursor_path.read_bytes())
                    for path in f.file_paths():
                        f.append(path, b"protocol bytes\n")
                    logs.main(["drain", str(cursor_path), str(f.archive / "end"), str(end_path)])
                    end = logs.decode(end_path.read_bytes())
                    self.assertTrue(logs.verify(cursor, end))
                    logs.main(["verify", str(cursor_path), str(end_path)])
                self.rejected(lambda: logs.client(str(endpoint), dict(action="unknown")), "unknown")
                endpoint.chmod(0o666)
                try:
                    self.rejected(lambda: logs.client(str(endpoint), dict(action="unknown")), "private")
                finally:
                    endpoint.chmod(0o600)
            finally:
                done.set()
                worker.join(5)
            self.assertFalse(worker.is_alive(), "collector thread failed to join")
        self.assertEqual(errors, [])
        self.assertFalse(endpoint.exists(), "collector endpoint leaked")
        f.capture.join()
        self.assertEqual(len(list(Path("/proc/self/fd").iterdir())), baseline)

    def test_archive_projection_cannot_hide_loss_after_redigesting(self):
        f = self.fixture
        cursor = f.capture.start(f.roster)
        f.append(next(f.file_paths()), b"complete suffix\n")
        end = f.end(cursor)
        ref = end["positions"][0]["raw"]
        original = Path(ref["path"]).read_bytes()
        for defect in ("prefix", "component", "identity", "source", "unknown"):
            with self.subTest(defect=defect):
                raw, bad = logs.decode(original), copy.deepcopy(end)
                if defect == "prefix":
                    segment = raw["snapshot"]["parts"][0]["segments"][0]
                    data = b"rewritten" + b"complete suffix\n"
                    segment.update(data=logs.base64.b64encode(data).decode(), sha=logs.sha(data), bytes=len(data))
                elif defect == "component":
                    raw["snapshot"]["parts"].pop()
                elif defect == "identity":
                    raw["sequence"] = cursor["positions"][0]["sequence"]
                elif defect == "source":
                    raw["snapshot"]["kind"] = "unknown"
                else:
                    raw["extra"] = True
                path = f.archive / ("tamper-" + defect + ".json")
                new_ref = logs.write_document(path, raw)
                bad["positions"][0] = logs.position(raw["stream"], raw["snapshot"], raw["sequence"], new_ref)
                self.rejected(lambda: logs.verify(cursor, logs.seal(bad)),
                              "prefix|component|stale|source|field")
        wrong = copy.deepcopy(cursor)
        wrong["roster_digest"] = "0" * 64
        wrong = logs.seal(wrong)
        bad = logs.seal(dict(end, cursor_digest=wrong["digest"]))
        self.rejected(lambda: logs.verify(wrong, bad), "roster digest")

    def test_collector_scope_refuses_replay_missing_host_and_early_join(self):
        f = self.fixture
        self.rejected(f.capture.join, "close before join")
        cursor = f.capture.start(f.roster)
        self.rejected(lambda: f.capture.drain(logs.seal(dict(cursor, roster_digest="0" * 64)), f.archive / "end"), "wrong")
        self.assertTrue(logs.verify(cursor, f.end(cursor)))
        self.rejected(lambda: f.end(cursor), "replayed")
        with tempfile.TemporaryDirectory(prefix="s10-log-host-") as directory:
            other = Fixture(directory)
            try:
                other.capture.readers.pop("agt2")
                self.rejected(lambda: other.capture.start(other.roster), "host reader absent")
                self.assertEqual(other.capture.sources, [])
            finally:
                other.close()
        with tempfile.TemporaryDirectory(prefix="s10-log-extra-host-") as directory:
            other = Fixture(directory)
            try:
                other.capture.readers["brk3"] = other.capture.readers["brk1"]
                self.rejected(lambda: other.capture.start(other.roster), "host reader absent or extra")
            finally:
                other.close()

    def test_cleanup_failure_remains_poison_and_closes_other_descriptors(self):
        f = self.fixture
        baseline = len(list(Path("/proc/self/fd").iterdir()))
        f.capture.start(f.roster)
        close, calls = logs.os.close, []
        def faulty_close(fd):
            close(fd)
            calls.append(fd)
            if len(calls) == 1:
                raise OSError("injected cleanup failure after close")
        with patch.object(logs.os, "close", faulty_close):
            self.rejected(f.capture.close, "cleanup failed")
        self.assertGreater(len(calls), 1, "cleanup stopped at first error")
        self.assertEqual(len(list(Path("/proc/self/fd").iterdir())), baseline)
        self.rejected(f.capture.join, "observation poison")

    def root_path(self, name):
        return self.fixture.root / name

    def test_five_lifecycles_release_original_and_watch_descriptors(self):
        baseline = len(list(Path("/proc/self/fd").iterdir()))
        for round_index in range(5):
            with tempfile.TemporaryDirectory(prefix="s10-log-cycle-") as directory:
                f = Fixture(directory)
                try:
                    cursor = f.capture.start(f.roster)
                    for path in f.file_paths():
                        f.append(path, ("round " + str(round_index) + "\n").encode())
                    self.assertTrue(logs.verify(cursor, f.end(cursor)))
                finally:
                    f.close()
                f.capture.join()
        self.assertEqual(len(list(Path("/proc/self/fd").iterdir())), baseline, "collector descriptor leak")


class StrictLogRetentionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="s10-log-retention-")
        self.fixture = Fixture(self.temporary.name)

    def tearDown(self):
        self.fixture.close()
        self.temporary.cleanup()

    # origin: S10 internal review rv-honesty F4
    def test_truncate_regrow_cannot_erase_unobserved_panic(self):
        f = self.fixture
        path = next(p for p in f.file_paths() if p.name == "agent.boot.err")
        path.write_bytes(b"")
        cursor = f.capture.start(f.roster)
        f.append(path, b"panic: lost between polls\n")
        path.write_bytes(b"benign replacement after truncate\n")
        rejected = False
        try:
            end = f.end(cursor)
            logs.verify(cursor, end)
        except logs.SetupError:
            rejected = True
        self.assertTrue(rejected, "truncate/regrow erased panic and still signed complete log capture")

    # origin: S10 internal review rv-honesty F4
    def test_nonempty_prefix_and_old_fd_loss_are_unprovable(self):
        for old_fd in (False, True):
            with self.subTest(old_fd=old_fd), tempfile.TemporaryDirectory() as directory:
                f = Fixture(directory)
                try:
                    path = next(f.file_paths())
                    cursor = f.capture.start(f.roster)
                    with path.open("r+b") as writer:
                        if old_fd:
                            path.rename(path.with_suffix(".rotated"))
                            path.write_bytes(b"successor\n")
                            f.capture.poll()
                        writer.seek(0, 2)
                        writer.write(b"panic: lost tail\n")
                        writer.flush()
                        writer.truncate(0)
                        writer.seek(0)
                        writer.write(b"baseline\nbenign regrowth longer than prefix\n")
                        writer.flush()
                    with self.assertRaisesRegex(logs.SetupError, "retention is unprovable"):
                        f.end(cursor)
                finally:
                    f.close()

    # origin: S10 internal review rv-honesty F4
    def test_plain_append_also_requires_retention_proof(self):
        for append in (False, True):
            with self.subTest(append=append), tempfile.TemporaryDirectory() as directory:
                f = Fixture(directory)
                try:
                    cursor = f.capture.start(f.roster)
                    if append:
                        f.append(next(f.file_paths()), b"ordinary append\n")
                    with self.assertRaisesRegex(logs.SetupError, "retention is unprovable"):
                        f.end(cursor)
                    self.assertFalse(logs.decode((f.archive / "failure.json").read_bytes())["complete"])
                finally:
                    f.close()

    # origin: S10 internal review rv-honesty F4
    def test_preexisting_writer_prevents_append_only_certification(self):
        f = self.fixture
        with next(f.file_paths()).open("r+b"), patch.object(logs, "append_only", return_value=True):
            cursor = f.capture.start(f.roster)
            with self.assertRaisesRegex(logs.SetupError, "retention is unprovable"):
                f.end(cursor)

    # origin: S10 internal review rv-honesty F4
    def test_late_inode_cannot_claim_prior_append_custody(self):
        f = self.fixture
        with patch.object(logs, "append_only", return_value=True):
            cursor = f.capture.start(f.roster)
            path = next(f.file_paths())
            path.rename(path.with_suffix(".rotated"))
            path.write_bytes(b"new inode lacks a custody start fence\n")
            with self.assertRaisesRegex(logs.SetupError, "retention is unprovable"):
                f.end(cursor)

        # The optional companion may appear later without changing any old
        # inode's ctime; its earlier writer custody is still unknown.
        with tempfile.TemporaryDirectory() as directory:
            other = Fixture(directory)
            try:
                companion = list(other.file_paths())[1]
                companion.unlink()
                with patch.object(logs, "append_only", return_value=True):
                    cursor = other.capture.start(other.roster)
                    companion.write_bytes(b"newly discovered companion\n")
                    with self.assertRaisesRegex(logs.SetupError, "retention is unprovable"):
                        other.end(cursor)
            finally:
                other.close()

    # origin: S10 internal review rv-honesty F4
    def test_sampled_flags_cannot_certify_intervening_writes(self):
        f = self.fixture
        with patch.object(logs, "append_only", return_value=True):
            cursor = f.capture.start(f.roster)
            f.append(next(f.file_paths()), b"append or erased unobserved bytes\n")
            with self.assertRaisesRegex(logs.SetupError, "retention is unprovable"):
                f.end(cursor)

    # origin: S10 internal review rv-honesty F4
    def test_kernel_flag_revocation_poison_survives_restoration(self):
        f = self.fixture
        # This is an ioctl protocol control; no privilege is requested and it
        # does not claim that the filesystem has actually granted +a custody.
        with patch.object(logs, "append_only", return_value=True):
            cursor = f.capture.start(f.roster)
            source = f.capture.sources[0].tracks[0]
            with patch.object(source.watch, "events", return_value=[0x4]):
                source.poll()
            with self.assertRaisesRegex(logs.SetupError, "retention is unprovable"):
                f.end(cursor)


if __name__ == "__main__":
    unittest.main()
