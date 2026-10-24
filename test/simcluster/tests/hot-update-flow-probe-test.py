#!/usr/bin/env python3
"""Hermetic adversarial controls for the shared S5 continuity judge (T47).

No deployed binary or broker is involved. Mutations use --mutations: each
require guard is disabled ON DISK, its corresponding test must turn red, then the original is restored. Core
continuity fixtures must become GREEN; layered parser guards may still reject
through another criterion, and their exact native result is retained.
Full diffs, source hashes, native statuses and failing assertions are retained
in the requested private --mutation-report directory.
"""

import ast
import contextlib
import copy
import errno
import difflib
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import select
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

PROBE = Path(__file__).resolve().parents[1] / "drills/lib/hot_update_flow_probe.py"
LIBRARY = PROBE.with_name("flow.sh")
NONCE = "abcdef0123456789abcdef0123456789"


class FlowProbeTests(unittest.TestCase):
    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Throughput may change;
    # independent ChaCha bytes, including unaligned tails, must not change.
    def test_native_reference_chacha_matches_scalar_reference(self):
        self.assertIsNotNone(self.sensor["NATIVE_CHACHA"])
        for length in (0, 1, 63, 64, 65, 0x3fff):
            key, nonce, data = bytes(range(32)), bytes(range(12)), bytes((n % 251 for n in range(length)))
            self.assertEqual(self.sensor["xor_stream"](key, nonce, data),
                             self.sensor["reference_xor_stream"](key, nonce, data))

    def setUp(self):
        self.sensor = runpy.run_path(str(PROBE))
        self.temporary = tempfile.TemporaryDirectory(prefix="hot-update-flow-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.private = []
        self.addCleanup(lambda: [os.close(fd) for fd in self.private])
        self.document = self.fixture()

    def sealed(self, data):
        fd = os.memfd_create("flow-probe-test", os.MFD_ALLOW_SEALING)
        self.private.append(fd)
        os.write(fd, data)
        seals = fcntl.F_SEAL_SEAL | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_GROW | fcntl.F_SEAL_WRITE
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS, seals)
        return fd

    def wire(self, count=42, payload_length=64):
        # Independent application encoder; using the probe's own frame() to
        # generate every expected byte would let framing bugs self-validate.
        result = bytearray()
        for seq in range(count):
            data = (hashlib.sha256(b"payload" + struct.pack("!Q", seq)).digest() * ((payload_length + 31) // 32))[:payload_length]
            result.extend(b"S5PROBE1" + bytes.fromhex(NONCE) + struct.pack("!QI", seq, len(data)) + hashlib.sha256(data).digest() + data)
        return bytes(result)

    def capture(self, name, raw, times, clock_shift=0):
        (self.root / name).write_bytes(raw)
        rows, offset = [], 0
        while offset < len(raw):
            seq, length = struct.unpack_from("!QI", raw, offset + 24)
            data = raw[offset + 68:offset + 68 + length]
            rows.append(dict(seq=seq, length=length, sha256=hashlib.sha256(data).hexdigest(), at=times[len(rows)] + clock_shift))
            offset += 68 + length
        window = dict(start=clock_shift, fault=3 + clock_shift, end=6 + clock_shift, finish_deadline=7 + clock_shift)
        return dict(file=name, ledger=rows, window=window)

    def fixture(self):
        raw = self.wire()
        times = [.1 + i * .1 for i in range(21)] + [3.1 + i * .1 for i in range(21)]
        process = dict(pid=100, boot_id="fixture-boot", start_ticks=7)
        original = dict(process, dev=0, ino=456, type=stat.S_IFSOCK, tuple=["private-local", "private-remote"], app_nonce=NONCE)
        identities = {role: [copy.deepcopy(original if role in ("client", "target") else process) for _ in range(3)]
                      for role in ("client", "target", "agent", "ctl")}
        directions = {}
        for direction in ("up", "down"):
            # Huge sender clock offset proves no cross-host subtraction.
            sent = self.capture(direction + ".sent", raw, times, clock_shift=10000)
            received = self.capture(direction + ".received", raw, times)
            directions[direction] = dict(expected=dict(records=42, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()), sent=sent, received=received,
                                         window=copy.deepcopy(received["window"]), completed_at=5.1, eof_expected=True, events=[dict(kind="eof", at=5.2)])
        flow = dict(id="original-flow", protocol="raw", **{"class": "normal"}, declared_at=0, identities=identities,
                    accept_count=1, redials=0, carrier_before="original-tls", carrier_after="rebuilt-tls", directions=directions)
        return dict(version=1, nonce=NONCE, flows=[flow], freeze_windows=[.2, .7],
                    requests=dict(rate=10, window=dict(start=0, end=6), samples=[dict(scheduled=i / 10, start=i / 10, end=i / 10 + .05, ok=True) for i in range(60)]))

    def save(self):
        (self.root / "evidence.json").write_text(json.dumps(self.document))

    def invoke(self, shell=False, arm=""):
        self.save()
        if shell:
            command = ["sh", "-c", '. "$1"; flow_continuity_judge "$2" "$3"', "flow-test", str(LIBRARY), str(self.root), NONCE]
            env = dict(os.environ, HERE=str(PROBE.parents[2]), PYTHON3=sys.executable, ARM=arm)
        else:
            command = [sys.executable, "-I", "-B", str(PROBE), "judge", "--evidence", str(self.root), "--nonce", NONCE]
            if arm == "L":
                command.extend(["--carrier-transition", "unchanged"])
            env = None
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=15, pass_fds=tuple(self.private))
        return result.returncode, json.loads(result.stdout)

    # origin: hot-update S5 plan §9.1 / T47. Safe legacy refusal preserves
    # the actual carrier; replacing it during L is a product assertion failure.
    def test_legacy_refusal_requires_unchanged_carrier(self):
        flow = self.document["flows"][0]
        flow["carrier_after"] = flow["carrier_before"]
        for shell in (False, True):
            self.assertEqual(self.invoke(shell=shell, arm="L")[0], 0)
            self.assertEqual(self.invoke(shell=shell)[0], 1)
        flow["carrier_after"] = "replacement"
        rc, result = self.invoke(shell=True, arm="L")
        self.assertEqual((rc, result["criterion"]), (1, "carrier_unchanged"))

    # origin: hot-update S5 plan §9.1 / T47. Equality of two missing IDs
    # cannot certify an unchanged legacy carrier.
    def test_legacy_refusal_requires_nonempty_carrier_identity(self):
        flow = self.document["flows"][0]
        flow["carrier_before"] = flow["carrier_after"] = ""
        rc, result = self.invoke(arm="L")
        self.assertEqual((rc, result["criterion"]), (1, "carrier_identity"))

    def seal_wire(self, clear, master, salt):
        # Encryption belongs only to fixture generation. Published RFC
        # vectors below independently pin both stream cipher and authenticator.
        key = self.sensor["ss_subkey"](master, salt)
        wire, calls, counter = bytearray(salt), [], 0
        for offset in range(0, len(clear), 0x3fff):
            part = clear[offset:offset + 0x3fff]
            for value in (len(part).to_bytes(2, "big"), part):
                nonce = counter.to_bytes(12, "little")
                cipher = self.sensor["xor_stream"](key, nonce, value)
                wire.extend(cipher + self.sensor["aead_tag"](key, nonce, cipher))
                calls.append(dict(subkey_sha256=hashlib.sha256(key).hexdigest(), nonce=nonce.hex()))
                counter += 1
        return bytes(wire), calls

    def add_ss(self):
        master = bytes(range(32))
        flow = self.document["flows"][0]
        flow["protocol"] = "ss"
        ss = dict(key_fd=self.sealed(master), directions={})
        for index, direction in enumerate(("up", "down")):
            clear = (self.root / (direction + ".sent")).read_bytes()
            prefix = b"\x01\x7f\x00\x00\x01\x12\x34" if direction == "up" else b""
            wire, calls = self.seal_wire(prefix + clear, master, bytes([index + 3]) * 32)
            ss["directions"][direction] = dict(file=dict(fd=self.sealed(wire)), plaintext_prefix_bytes=len(prefix),
                                               actual_seals=dict(fd=self.sealed(json.dumps(calls).encode())),
                                               partial=dict(attempt_len=100, written=17, suffix_bytes=83, seal_calls=1))
        flow["ss"] = ss

    # origin: hot-update S5 plan §8 / T47.
    # origin: hot-update S5 plan §6.2 / §9.1 / T47.
    def test_complete_duplex_trace_passes(self):
        for shell in (False, True):
            rc, result = self.invoke(shell)
            self.assertEqual(rc, 0, result)
            self.assertEqual(result["verdict"], "GREEN")
            self.assertEqual(result["freeze_over_500ms"], 1)
            self.assertEqual(result["freeze"]["max"], .7)
            self.assertLess(result["flows"][0]["delays"]["up"]["max"], 5)

    def half_close_up(self, flow):
        record = flow["directions"]["up"]
        for side in ("sent", "received"):
            shift = record[side]["window"]["start"]
            for index, row in enumerate(record[side]["ledger"]): row["at"] = shift + .02 + index * .04
        record["completed_at"] = 2.2
        record["events"][0]["at"] = 2.3

    # origin: hot-update S5 plan §6.2 / §9.1 / T47.
    def test_half_close_preserves_more_than_replay_capacity_after_cut(self):
        flow = self.document["flows"][0]
        flow["class"] = "half-close"
        self.half_close_up(flow)
        self.large_reverse(flow)
        self.assertEqual(self.invoke()[0], 0)

    def large_reverse(self, flow):
        raw = self.wire(280, 32768)
        times = [.1 + index * .1 for index in range(20)] + [3.1 + index * .007 for index in range(260)]
        record = flow["directions"]["down"]
        record["sent"] = self.capture("down.sent", raw, times, clock_shift=10000)
        record["received"] = self.capture("down.received", raw, times)
        record["expected"] = dict(records=280, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())

    # origin: hot-update S5 plan §6.2 / §9.1 / T47.
    def test_declared_idle_and_slow_keep_identity_checks(self):
        flow = self.document["flows"][0]
        flow["class"] = "slow"
        for direction in flow["directions"].values():
            direction["window"].update(end=20, finish_deadline=21)
            direction["received"]["window"].update(end=20, finish_deadline=21)
        self.assertEqual(self.invoke()[0], 0)
        flow["class"] = "idle"
        for name, direction in flow["directions"].items():
            for side in ("sent", "received"):
                (self.root / (name + "." + side)).write_bytes(b"")
                direction[side]["ledger"] = []
            direction.update(expected=dict(records=0, bytes=0, sha256=hashlib.sha256(b"").hexdigest()), eof_expected=False, events=[])
        self.assertEqual(self.invoke()[0], 0)
        flow["identities"]["client"][1]["pid"] += 1
        self.assertEqual(self.invoke()[0], 1)

    # origin: hot-update S5 plan §6.2 / §9.1 / T47.
    def test_first_progress_gap_is_included(self):
        flow = self.document["flows"][0]
        for direction in flow["directions"].values():
            for side in ("sent", "received"):
                capture = direction[side]
                for row in capture["ledger"]: row["at"] += 6
                for key in ("fault", "end", "finish_deadline"): capture["window"][key] += 6
            for key in ("fault", "end", "finish_deadline"): direction["window"][key] += 6
            direction["completed_at"] += 6
            direction["events"][0]["at"] += 6
        rc, result = self.invoke()
        self.assertEqual((rc, result["criterion"]), (1, "maximum_gap"))

    # origin: hot-update S5 plan §6.2 / §9.1 / T47.
    def test_shell_helpers_preserve_failure_status(self):
        self.document["flows"][0]["directions"]["up"]["events"].append(dict(kind="rst", at=5.3))
        self.assertEqual(self.invoke(shell=True)[0], 1)
        self.document["version"] = 0
        self.assertEqual(self.invoke(shell=True)[0], 2)
        for name in ("flow_continuity_judge", "flow_describe_capture", "flow_process_identity", "flow_socket_identity"):
            command = ["sh", "-c", '. "$1"; "$2"', "flow-test", str(LIBRARY), name]
            result = subprocess.run(command, capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 2, name)

    # origin: hot-update S5 plan §6.2 / §9.1 / T47.
    def test_independent_ss_trace_passes_and_redacts_summary(self):
        self.add_ss()
        rc, result = self.invoke()
        self.assertEqual(rc, 0, result)
        self.assertEqual(result["seal_calls"], 4)
        output = json.dumps(result)
        self.assertNotIn(bytes(range(32)).hex(), output)
        self.assertNotIn("subkey_sha256", output)
        self.assertNotIn("private-local", output)

    # origin: hot-update S5 plan §6.2 / §9.1 / T47.
    def test_nonce_domain_spans_opposite_directions(self):
        self.add_ss()
        ss = self.document["flows"][0]["ss"]
        raw = (self.root / "up.sent").read_bytes()
        wire, calls = self.seal_wire(raw, bytes(range(32)), bytes([4]) * 32)
        ss["directions"]["up"].update(file=dict(fd=self.sealed(wire)), plaintext_prefix_bytes=0,
                                        actual_seals=dict(fd=self.sealed(json.dumps(calls).encode())))
        rc, result = self.invoke()
        self.assertEqual((rc, result["criterion"]), (1, "nonce_reuse"))

    # origin: hot-update S5 plan §6.2 / §9.1 / T47.
    def test_reference_primitives_match_published_rfc_vectors(self):
        # Independently generated with OpenSSL's HKDF-SHA1 for the SS info
        # string, locking the subkey derivation separately from round trips.
        self.assertEqual(self.sensor["ss_subkey"](bytes(range(32)), bytes([3]) * 32).hex(),
                         "6388d415c04ab0ba1046a78e54b2ebee3bc73aae3f156d0d9d74baabffe9631f")
        # RFC 8439 §2.3.2 and §2.5.2 (fixed goldens, not product-generated).
        block = self.sensor["chacha_block"](bytes(range(32)), bytes.fromhex("000000090000004a00000000"), 1)
        self.assertEqual(block.hex(), "10f1e7e4d13b5915500fdd1fa32071c4c7d1f4c733c068030422aa9ac3d46c4ed2826446079faa0914c2d705d98b02a2b5129cd1de164eb9cbd083e8a2503c4e")
        key = bytes.fromhex("85d6be7857556d337f4452fe42d506a80103808afb0db2fd4abff6af4149f51b")
        self.assertEqual(self.sensor["poly1305"](key, b"Cryptographic Forum Research Group").hex(), "a8061dc1305136c6c22b8baf0c0127a9")
        # RFC 8439 §2.8.2 full AEAD ciphertext; the length block and tag are
        # pinned independently of the SS fixture's encryption helper.
        key = bytes.fromhex("808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f")
        nonce = bytes.fromhex("070000004041424344454647")
        aad = bytes.fromhex("50515253c0c1c2c3c4c5c6c7")
        clear = b"Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future, sunscreen would be it."
        cipher = self.sensor["xor_stream"](key, nonce, clear)
        self.assertEqual(cipher.hex(), "d31a8d34648e60db7b86afbc53ef7ec2a4aded51296e08fea9e2b5a736ee62d63dbea45e8ca9671282fafb69da92728b1a71de0a9e060b2905d6a5b67ecd3b3692ddbd7f2d778b8c9803aee328091b58fab324e4fad675945585808b4831d7bc3ff4def08e4b7a9de576d26586cec64b6116")
        self.assertEqual(self.sensor["aead_tag"](key, nonce, cipher, aad).hex(), "1ae10b594f09e26a7e902ecbd0600691")

    # origin: hot-update S5 plan §6.2 / §9.1 / T47.
    def test_malformed_evidence_is_setup_red_and_does_not_leak(self):
        self.save()
        valid = json.dumps(self.document)
        for raw in ('{"version":0,' + valid[1:], '{"secret":"private-local"}', '{"unused":NaN,' + valid[1:]):
            (self.root / "evidence.json").write_text(raw)
            stream = io.StringIO()
            with contextlib.redirect_stdout(stream):
                rc = self.sensor["main"](["judge", "--evidence", str(self.root), "--nonce", NONCE])
            self.assertEqual(rc, 2)
            self.assertNotIn("private-local", stream.getvalue())

    # origin: hot-update S5 plan §6.2 / §9.1 / T47.
    def test_private_capture_requires_sealed_memfd(self):
        self.add_ss()
        self.document["flows"][0]["ss"]["directions"]["up"]["file"] = "up.sent"
        self.assertEqual(self.invoke()[0], 2)
        self.document["flows"][0]["ss"]["key_fd"] = self.sealed(b"short")
        self.assertEqual(self.invoke()[0], 2)

    # origin: hot-update S5 plan §6.2 / §9.1 / T47.
    def test_linux_collector_keeps_original_kernel_identity(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            with socket.create_connection(listener.getsockname()) as client:
                target, _ = listener.accept()
                with target:
                    before = self.sensor["identity"](os.getpid(), client.fileno(), NONCE)
                    client.sendall(b"original-byte")
                    self.assertEqual(target.recv(64), b"original-byte")
                    after = self.sensor["identity"](os.getpid(), client.fileno(), NONCE)
                    self.assertEqual(before, after)
                    self.assertEqual(before["ino"], os.fstat(client.fileno()).st_ino)
                    self.assertEqual(before["type"], stat.S_IFSOCK)
                    row = next(line.split() for line in Path('/proc/self/net/tcp').read_text().splitlines()[1:]
                               if line.split()[9] == str(before['ino']))
                    self.assertEqual(before['tuple'], ['tcp', hashlib.sha256(row[1].encode()).hexdigest(), hashlib.sha256(row[2].encode()).hexdigest()])
                    # origin: hot-update S5 plan §9.1 / T47 / DRILL-RUN 75.A.
                    # seq_file enumeration can omit a live socket. Its own
                    # read-only kernel name queries must retain the same tuple.
                    original_read = Path.read_text
                    def absent_proc_tuple(path, *args, **kwargs):
                        if str(path) in (f"/proc/{os.getpid()}/net/tcp", f"/proc/{os.getpid()}/net/tcp6"):
                            return "header\n"
                        return original_read(path, *args, **kwargs)
                    flags = fcntl.fcntl(client.fileno(), fcntl.F_GETFL)
                    count = len(os.listdir('/proc/self/fd'))
                    for round in range(5):
                        with mock.patch.object(Path, 'read_text', absent_proc_tuple):
                            self.assertEqual(self.sensor['identity'](os.getpid(), client.fileno(), NONCE), before)
                    self.assertEqual(fcntl.fcntl(client.fileno(), fcntl.F_GETFL), flags)
                    self.assertEqual(len(os.listdir('/proc/self/fd')), count)
                    client.sendall(b'original-still-open')
                    self.assertEqual(target.recv(64), b'original-still-open')

                    native_tuple = self.sensor['_native_socket_tuple']
                    with socket.socket() as replacement:
                        def replace_after_names(fd):
                            result = native_tuple(fd)
                            os.dup2(replacement.fileno(), fd)
                            return result
                        with mock.patch.dict(self.sensor['identity'].__globals__, _native_socket_tuple=replace_after_names):
                            with self.assertRaises(self.sensor['EvidenceError']):
                                self.sensor['identity'](os.getpid(), client.fileno(), NONCE)

        # The identity sensor must not consume a pending connect's SO_ERROR.
        with socket.socket() as reserved, socket.socket() as pending:
            reserved.bind(('127.0.0.1', 0))
            pending.setblocking(False)
            self.assertIn(pending.connect_ex(reserved.getsockname()), (errno.EINPROGRESS, errno.ECONNREFUSED))
            self.assertTrue(select.select([], [pending], [], 2)[1])
            flags = fcntl.fcntl(pending.fileno(), fcntl.F_GETFL)
            with self.assertRaises(self.sensor['EvidenceError']):
                self.sensor['identity'](os.getpid(), pending.fileno(), NONCE)
            self.assertEqual(pending.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR), errno.ECONNREFUSED)
            self.assertEqual(fcntl.fcntl(pending.fileno(), fcntl.F_GETFL), flags)

        with socket.socket(socket.AF_INET6) as listener:
            listener.bind(('::1', 0))
            listener.listen()
            with socket.socket(socket.AF_INET6) as client:
                client.connect(listener.getsockname())
                target, _ = listener.accept()
                with target:
                    for round in range(5):
                        snapshot = self.sensor['identity'](os.getpid(), client.fileno(), NONCE)
                        row = next(line.split() for line in Path('/proc/self/net/tcp6').read_text().splitlines()[1:]
                                   if line.split()[9] == str(snapshot['ino']))
                        self.assertEqual(snapshot['tuple'], ['tcp6', hashlib.sha256(row[1].encode()).hexdigest(), hashlib.sha256(row[2].encode()).hexdigest()])

        unsupported, unused = socket.socketpair()
        with unsupported, unused:
            with self.assertRaises(self.sensor['EvidenceError']):
                self.sensor['_native_socket_tuple'](unsupported.fileno())
        with self.assertRaises(OSError):
            self.sensor['_native_socket_tuple'](-1)

        library = self.sensor['ctypes'].CDLL(None, use_errno=True)
        class NativeReply:
            def __init__(self, name, defect): self.name, self.defect = name, defect
            def __call__(self, fd, address, size):
                result = getattr(library, self.name)(fd, address, size)
                if self.defect == 'short':
                    self_ctypes.cast(size, self_ctypes.POINTER(self_ctypes.c_uint))[0] = 2
                elif self.defect == 'bad_family':
                    self_ctypes.memmove(address, socket.AF_UNIX.to_bytes(2, sys.byteorder), 2)
                    self_ctypes.cast(size, self_ctypes.POINTER(self_ctypes.c_uint))[0] = 128
                elif self.name == 'getpeername':
                    self_ctypes.memmove(address, socket.AF_INET6.to_bytes(2, sys.byteorder), 2)
                    self_ctypes.cast(size, self_ctypes.POINTER(self_ctypes.c_uint))[0] = 28
                return result
        self_ctypes = self.sensor['ctypes']
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0)); listener.listen()
            with socket.create_connection(listener.getsockname()) as client:
                target, _ = listener.accept()
                with target:
                    for defect in ('short', 'mixed_family', 'bad_family'):
                        fake = type('Names', (), {})()
                        fake.getsockname, fake.getpeername = NativeReply('getsockname', defect), NativeReply('getpeername', defect)
                        with mock.patch.object(self_ctypes, 'CDLL', return_value=fake):
                            with self.assertRaises(self.sensor['EvidenceError']):
                                self.sensor['_native_socket_tuple'](client.fileno())

        # A full local accept queue holds the original second connect in
        # SYN_SENT. Proc fallback must retain its peer without consuming error.
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0)); listener.listen(0)
            with socket.create_connection(listener.getsockname(), timeout=1) as occupied, socket.socket() as pending:
                pending.setblocking(False)
                self.assertEqual(pending.connect_ex(listener.getsockname()), errno.EINPROGRESS)
                self.assertFalse(select.select([], [pending], [], .02)[1])
                snapshot = self.sensor['identity'](os.getpid(), pending.fileno(), NONCE)
                self.assertEqual(snapshot['ino'], os.fstat(pending.fileno()).st_ino)
                self.assertEqual(pending.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR), 0)

    # origin: hot-update S5 plan §6.2 / §9.1 / T47.
    def test_describe_preserves_actual_bytes_and_arrival_partition(self):
        arrivals = self.root / "arrivals.json"
        rows = self.document["flows"][0]["directions"]["up"]["received"]["ledger"]
        arrivals.write_text(json.dumps([row["at"] for row in rows]))
        command = [sys.executable, "-I", "-B", str(PROBE), "describe", "--raw", str(self.root / "up.received"), "--arrivals", str(arrivals), "--output", str(self.root / "ledger.json"), "--nonce", NONCE]
        result = subprocess.run(command, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(json.loads(result.stdout)["ledger"], rows)
        arrivals.write_text("[]")
        result = subprocess.run(command, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 2, result.stdout)

    def defect(self, criterion):
        flow = self.document["flows"][0]
        record = flow["directions"]["up"]
        capture = record["received"]
        if criterion == "sequence":
            # Duplicate a middle frame, keeping declared totals and hashes
            # coherent: the independent sequence criterion is indispensable.
            raw = (self.root / "up.received").read_bytes()
            raw = raw[:132] + raw[:132] + raw[264:]
            capture.update(self.capture("up.received", raw, [row["at"] for row in capture["ledger"]]))
            (self.root / "up.sent").write_bytes(raw)
            record["sent"]["ledger"][1].update(seq=0, sha256=capture["ledger"][1]["sha256"])
            record["expected"]["sha256"] = hashlib.sha256(raw).hexdigest()
        elif criterion == "final_totals":
            # A dropped final sequence with independently declared last seq.
            record["expected"]["records"] += 1
        elif criterion == "final_hash":
            record["expected"]["sha256"] = "0" * 64
        elif criterion in ("application_nonce", "payload_hash", "frame_length", "truncated_payload", "truncated_frame"):
            raw = bytearray((self.root / "up.received").read_bytes())
            if criterion == "application_nonce": raw[8] ^= 1
            elif criterion == "payload_hash": raw[68] ^= 1
            elif criterion == "frame_length": raw[32:36] = struct.pack("!I", MAX_PAYLOAD + 1)
            elif criterion == "truncated_payload": raw = raw[:-1]
            else: raw = raw[:-100]
            (self.root / "up.received").write_bytes(raw)
        elif criterion == "independent_ledger": capture["ledger"][0]["sha256"] = "0" * 64
        elif criterion == "monotonic_ledger": capture["ledger"][1]["at"] = .05
        elif criterion == "observation_bounds":
            capture["ledger"][-1]["at"] = 6.2
            record["completed_at"] = 6.2
            record["events"][0]["at"] = 6.3
        elif criterion == "identity_samples": flow["identities"]["client"] = flow["identities"]["client"][:1]
        elif criterion == "identity_fields":
            for row in flow["identities"]["client"]: row.pop("ino")
        elif criterion == "identity_continuity": flow["identities"]["target"][1]["ino"] += 1
        elif criterion == "identity_roles": flow["identities"].pop("ctl")
        elif criterion == "target_accept_once": flow["accept_count"] = 2
        elif criterion == "no_application_redial": flow["redials"] = 1
        elif criterion == "carrier_changed": flow["carrier_after"] = flow["carrier_before"]
        elif criterion == "declared_class": flow["class"] = "posthoc-slow"
        elif criterion == "class_before_start": flow["declared_at"] = 1
        elif criterion == "crypto_required": flow["protocol"] = "ss"
        elif criterion == "unique_flows": self.document["flows"].append(copy.deepcopy(flow))
        elif criterion == "nonempty_cohort": self.document["flows"] = []
        elif criterion == "duplex_directions": flow["directions"]["ignored"] = copy.deepcopy(record)
        elif criterion == "receiver_window": record["window"]["finish_deadline"] += 1
        elif criterion == "finite_finish":
            record["completed_at"] = 8
            record["events"][0]["at"] = 8
            record["window"]["finish_deadline"] = 9
            record["received"]["window"]["finish_deadline"] = 9
            record["completed_at"] = -1
        elif criterion == "event_shape": record["events"].append(dict(kind="forgotten-error", at=5.2))
        elif criterion == "no_reset": record["events"].append(dict(kind="rst", at=5.3))
        elif criterion == "no_redial_event": record["events"].append(dict(kind="redial", at=5.3))
        elif criterion == "expected_eof": record["eof_expected"] = False
        elif criterion == "eof_after_tail": record["events"][0]["at"] = 1
        elif criterion == "pre_post_samples":
            for side in ("sent", "received"): record[side]["window"]["fault"] -= 1
            record["window"]["fault"] -= 1
        elif criterion == "maximum_gap":
            # Trailing silence is invisible to a naive consecutive-arrival
            # p95. It alone exceeds the threshold in this valid byte trace.
            for window in (record["window"], capture["window"]):
                window.update(end=10.2, finish_deadline=11)
        elif criterion == "half_close_before_cut":
            flow["class"] = "half-close"
            self.large_reverse(flow)
        elif criterion == "reverse_tail_after_cut":
            flow["class"] = "half-close"
            self.half_close_up(flow)
        elif criterion == "idle_stays_idle": flow["class"] = "idle"
        elif criterion.startswith("request_"):
            requests = self.document["requests"]
            if criterion == "request_profile": requests["rate"] = 9
            elif criterion == "request_schedule": requests["samples"].pop()
            elif criterion == "request_success": requests["samples"][0]["ok"] = False
            else: requests["samples"][-1]["end"] += 5
        elif criterion in ("aead_authentication", "ss_plaintext", "seal_ledger", "nonce_reuse", "partial_cipher_write", "ss_prefix", "ss_salt", "ss_length_suffix", "ss_payload_suffix"):
            self.add_ss()
            ss = flow["ss"]["directions"]["up"]
            if criterion == "partial_cipher_write": ss["partial"]["written"] = 0
            elif criterion == "ss_prefix":
                ss = flow["ss"]["directions"]["down"]
                clear = (self.root / "down.sent").read_bytes()
                wire, calls = self.seal_wire(b"prefix!" + clear, bytes(range(32)), bytes([4]) * 32)
                ss.update(file=dict(fd=self.sealed(wire)), plaintext_prefix_bytes=7,
                          actual_seals=dict(fd=self.sealed(json.dumps(calls).encode())))
            elif criterion in ("nonce_reuse", "seal_ledger"):
                calls = json.loads(self.sensor["sealed_bytes"](ss["actual_seals"]["fd"]))
                if criterion == "nonce_reuse":
                    alias = copy.deepcopy(flow)
                    alias["id"] = "different-actor-same-actual-material"
                    self.document["flows"].append(alias)
                else: calls[0]["nonce"] = "01" * 12
                ss["actual_seals"] = dict(fd=self.sealed(json.dumps(calls).encode()))
            else:
                wire = self.sensor["sealed_bytes"](ss["file"]["fd"])
                if criterion == "aead_authentication": wire = wire[:-1] + bytes([wire[-1] ^ 1])
                elif criterion == "ss_salt": wire = wire[:31]
                elif criterion == "ss_length_suffix": wire = wire[:33]
                elif criterion == "ss_payload_suffix": wire = wire[:-1]
                else:
                    wire, _ = self.seal_wire(b"different plaintext", bytes(range(32)), bytes([3]) * 32)
                ss["file"] = dict(fd=self.sealed(wire))
        else:
            raise AssertionError("unknown defect " + criterion)


MAX_PAYLOAD = 32768
# origin: hot-update S5 plan §8 / T47. Every criterion has a named negative
# fixture; duplicate/reordered frames and dropped suffixes have distinct guards.
CRITERIA = "sequence final_totals final_hash application_nonce payload_hash frame_length truncated_payload truncated_frame independent_ledger monotonic_ledger observation_bounds identity_samples identity_fields identity_continuity identity_roles target_accept_once no_application_redial carrier_changed declared_class class_before_start crypto_required unique_flows nonempty_cohort duplex_directions receiver_window finite_finish event_shape no_reset no_redial_event expected_eof eof_after_tail pre_post_samples maximum_gap request_profile request_schedule request_success request_maximum aead_authentication ss_plaintext seal_ledger nonce_reuse partial_cipher_write ss_prefix ss_salt ss_length_suffix ss_payload_suffix half_close_before_cut reverse_tail_after_cut idle_stays_idle".split()


def negative(criterion):
    def exercise(self):
        self.defect(criterion)
        rc, result = self.invoke()
        self.assertEqual(rc, 1, result)
        self.assertEqual(result["criterion"], criterion)
    exercise.__name__ = "test_rejects_" + criterion
    return exercise


for criterion in CRITERIA:
    setattr(FlowProbeTests, "test_rejects_" + criterion, negative(criterion))


def mutations(report):
    report.mkdir(parents=True, exist_ok=False)
    original = PROBE.read_text()
    tree = ast.parse(original)
    lines = original.splitlines(keepends=True)
    offsets = [0]
    for line in lines: offsets.append(offsets[-1] + len(line))
    receipts = []
    try:
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name) or node.func.id != "require": continue
            criterion = node.args[1].value
            if criterion not in CRITERIA: continue
            guard = node.args[0]
            begin, end = offsets[guard.lineno - 1] + guard.col_offset, offsets[guard.end_lineno - 1] + guard.end_col_offset
            mutant = original[:begin] + "True" + original[end:]
            PROBE.write_text(mutant)
            name = "FlowProbeTests.test_rejects_" + criterion
            result = subprocess.run([sys.executable, "-I", "-B", str(Path(__file__).resolve()), name], capture_output=True, text=True, timeout=30)
            # Independently observe the native negative fixture becoming green.
            exercise = FlowProbeTests(name.split(".")[1])
            exercise.setUp()
            try:
                exercise.defect(criterion)
                native_rc, native_result = exercise.invoke()
            finally: exercise.doCleanups()
            PROBE.write_text(original)
            (report / (criterion + ".diff")).write_text("".join(difflib.unified_diff(original.splitlines(True), mutant.splitlines(True), fromfile=str(PROBE), tofile=str(PROBE))))
            (report / (criterion + ".log")).write_text(result.stdout + result.stderr)
            receipts.append(dict(criterion=criterion, test=name, command=[sys.executable, "-I", "-B", str(Path(__file__).resolve()), name], rc=result.returncode, negative_native_rc=native_rc, negative_verdict=native_result,
                                 original_sha256=hashlib.sha256(original.encode()).hexdigest(), mutant_sha256=hashlib.sha256(mutant.encode()).hexdigest()))
    finally:
        PROBE.write_text(original)
        (report / "report.json").write_text(json.dumps(receipts, indent=2) + "\n")
    library_original = LIBRARY.read_text()
    try:
        for index, count in enumerate((2, 4, 1, 3)):
            mutant = library_original.replace('[ "$#" = ' + str(count) + ' ] || return 2', '[ "$#" = ' + str(count) + ' ] || return 0')
            LIBRARY.write_text(mutant)
            name = "FlowProbeTests.test_shell_helpers_preserve_failure_status"
            command = [sys.executable, "-I", "-B", str(Path(__file__).resolve()), name]
            result = subprocess.run(command, capture_output=True, text=True, timeout=30)
            LIBRARY.write_text(library_original)
            criterion = "shell_arity_" + str(index)
            (report / (criterion + ".diff")).write_text("".join(difflib.unified_diff(library_original.splitlines(True), mutant.splitlines(True), fromfile=str(LIBRARY), tofile=str(LIBRARY))))
            (report / (criterion + ".log")).write_text(result.stdout + result.stderr)
            receipts.append(dict(criterion=criterion, test=name, command=command, rc=result.returncode, negative_native_rc=None,
                                 original_sha256=hashlib.sha256(library_original.encode()).hexdigest(), mutant_sha256=hashlib.sha256(mutant.encode()).hexdigest()))
        mutant = library_original.replace('flow_probe judge --evidence "$1" --nonce "$2"', 'flow_probe judge --evidence "$1" --nonce "$2"; return 0')
        LIBRARY.write_text(mutant)
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        criterion = "shell_swallow_native_status"
        (report / (criterion + ".diff")).write_text("".join(difflib.unified_diff(library_original.splitlines(True), mutant.splitlines(True), fromfile=str(LIBRARY), tofile=str(LIBRARY))))
        (report / (criterion + ".log")).write_text(result.stdout + result.stderr)
        receipts.append(dict(criterion=criterion, test=name, command=command, rc=result.returncode, negative_native_rc=None,
                             original_sha256=hashlib.sha256(library_original.encode()).hexdigest(), mutant_sha256=hashlib.sha256(mutant.encode()).hexdigest()))
    finally:
        LIBRARY.write_text(library_original)
        (report / "report.json").write_text(json.dumps(receipts, indent=2) + "\n")
    primary = {"sequence", "final_totals", "final_hash", "maximum_gap", "identity_continuity", "nonce_reuse", "no_reset", "expected_eof", "no_application_redial", "half_close_before_cut", "reverse_tail_after_cut"}
    green_controls = {row["criterion"] for row in receipts if row["negative_native_rc"] == 0}
    passed = receipts and all(row["rc"] == 1 for row in receipts) and primary <= green_controls
    print(json.dumps(dict(mutations=len(receipts), report=str(report), all_red=all(row["rc"] == 1 for row in receipts),
                          false_green_controls=len(green_controls), required_controls_green=primary <= green_controls)))
    return 0 if passed else 1


if __name__ == "__main__":
    if sys.argv[1:2] == ["--mutations"]:
        sys.exit(mutations(Path(sys.argv[2])))
    unittest.main()
