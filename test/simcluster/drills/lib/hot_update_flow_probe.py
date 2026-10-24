#!/usr/bin/env python3
"""Independent S5 byte, identity and Shadowsocks continuity oracle.

P24/P25 interface: `judge --evidence DIR --nonce HEX` (alias `continuity`)
reads evidence.json, returns 0/GREEN, 1/ASSERT-FAIL, 2/SETUP-RED. Negative
controls use the identical command and preserve its native status. Summary
output contains criterion names and counts, never raw identities or crypto.

Evidence version=1, nonce=cohort HEX, flows=[{id,protocol,class,declared_at,identities,
accept_count,redials,carrier_before,carrier_after,directions,ss?}]. Classes are
normal/slow/idle/half-close and are declared before each local fault window.
Identities maps client,target,agent,ctl to >=2 snapshots. Each snapshot has
pid,boot_id,start_ticks; client/target also dev,ino,type,tuple,app_nonce. Extra
snapshot fields are compared too: changing an unrecognized field fails closed.
Each direction up/down contains expected={records,bytes,sha256}, sent and
received={file,ledger:[{seq,length,sha256,at}]}, window={start,fault,end,
finish_deadline}, completed_at,eof_expected,events=[{kind,at}]. Raw files use
application frames below, NOT tether-flow framing; bytes include headers.
Ledgers partition the parsed raw capture and timestamps are the observer's
local monotonic seconds. Sender and receiver clocks are never subtracted.
Expected totals/hash must be published before cohort start, not derived from
the final receiver. Per-side windows live inside sent/received; the direction
window is the receiving observer's window. Frames start at seq=0.

SS adds key_fd (sealed memfd containing the 32-byte master), directions={up,
down:{file,plaintext_prefix_bytes,actual_seals:{fd:N} containing JSON [{subkey_sha256,nonce}],
partial:{attempt_len,written,suffix_bytes,seal_calls}}}. File is the original
salt+AEAD stream. Actual Seal calls include ALL attempts, including failed
and repeated calls, and are compared against an independent parser. Neither
KeyID, actor nor direction partitions the global (actual subkey digest,nonce)
uniqueness domain. Secret evidence stays in inherited sealed memfds: an SS
file reference may instead be {fd:N}; ordinary SS file paths are refused.
The declared target prefix is removed only from uplink plaintext, before
comparison against the independent application sender. Salt/subkeys/nonce
values never appear in the judge's result or errors.

Optional requests={rate:10,window:{start,end},samples:[{scheduled,start,end,
ok}]} is checked at every scheduled tick, including the final tail; no p95
substitution. Optional freeze_windows seconds report >500ms calibration but
do not change the independent 5s business criterion.

`describe --raw PATH --arrivals PATH --output PATH --nonce HEX` builds a byte
ledger from raw capture and a JSON list of independently measured per-frame
arrival times. It never synthesizes arrival times or reconnects a socket.
`identity --pid PID [--fd FD --app-nonce HEX]` reads Linux kernel identity.
P25 owns orchestration, collection, custody of predeclared expectations and
source/artifact/running identities. This module never invokes tether.
"""

import argparse
import ctypes
import ctypes.util
import errno
import fcntl
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import socket
import stat
import struct
import sys

MAGIC = b"S5PROBE1"
HEADER = struct.Struct("!8s16sQI32s")
MAX_PAYLOAD = 32768


class EvidenceError(Exception):
    pass


class ContinuityError(Exception):
    pass


def require(condition, criterion):
    if not condition:
        raise ContinuityError(criterion)


def number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def uint(value):
    return type(value) is int and value >= 0


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    # Duplicate keys can overwrite a sticky failure or an original identity.
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise EvidenceError("duplicate JSON field")
            result[key] = value
        return result
    return json.loads(Path(path).read_text(), object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(EvidenceError("nonfinite JSON")))


def sealed_bytes(fd):
    if not uint(fd):
        raise EvidenceError("invalid private descriptor")
    seals = fcntl.fcntl(fd, fcntl.F_GET_SEALS)
    wanted = fcntl.F_SEAL_SEAL | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_GROW | fcntl.F_SEAL_WRITE
    if seals & wanted != wanted or not stat.S_ISREG(os.fstat(fd).st_mode):
        raise EvidenceError("private descriptor is not sealed")
    size = os.fstat(fd).st_size
    if size > 128 << 20:
        raise EvidenceError("private descriptor exceeds probe limit")
    return os.pread(fd, size, 0)


def raw_bytes(root, reference, private=False):
    if isinstance(reference, dict) and set(reference) == {"fd"}:
        return sealed_bytes(reference["fd"])
    if private:
        raise EvidenceError("private capture requires sealed descriptor")
    path = (root / reference).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise EvidenceError("raw capture is outside evidence directory")
    return path.read_bytes()


def frame(nonce, seq, payload):
    if not uint(seq) or seq >= 1 << 64 or not 0 < len(payload) <= MAX_PAYLOAD:
        raise EvidenceError("invalid application frame")
    return HEADER.pack(MAGIC, bytes.fromhex(nonce), seq, len(payload), hashlib.sha256(payload).digest()) + payload


# origin: hot-update S5 plan §6.2 / §9.1 / T47.
def parse_frames(raw, nonce):
    rows, offset = [], 0
    while offset < len(raw):
        require(len(raw) - offset >= HEADER.size, "truncated_frame")
        magic, session, seq, length, wanted = HEADER.unpack_from(raw, offset)
        require(magic == MAGIC and session.hex() == nonce, "application_nonce")
        require(0 < length <= MAX_PAYLOAD, "frame_length")
        end = offset + HEADER.size + length
        require(end <= len(raw), "truncated_payload")
        payload = raw[offset + HEADER.size:end]
        require(hashlib.sha256(payload).digest() == wanted, "payload_hash")
        rows.append(dict(seq=seq, length=length, sha256=digest(payload)))
        offset = end
    return rows


def percentiles(values):
    ordered = sorted(values)
    return {name: ordered[max(0, math.ceil(len(ordered) * p) - 1)] if ordered else 0
            for name, p in (("p50", .5), ("p95", .95), ("max", 1))}


# origin: hot-update S5 plan §6.2 / §9.1 / T47.
def check_window(window):
    values = [window[k] for k in ("start", "fault", "end", "finish_deadline")]
    if not all(number(v) for v in values) or not values[0] <= values[1] <= values[2] <= values[3]:
        raise EvidenceError("invalid local observation window")


def check_capture(root, capture, expected, nonce):
    raw = raw_bytes(root, capture["file"])
    rows = parse_frames(raw, nonce)
    # origin: hot-update S5 plan §9.1 / T47. A sender ledger cannot excuse a
    # missing or duplicate application byte; the finish contract predates cut.
    require([row["seq"] for row in rows] == list(range(len(rows))), "sequence")
    require(len(rows) == expected["records"] and len(raw) == expected["bytes"], "final_totals")
    require(digest(raw) == expected["sha256"], "final_hash")
    ledger = capture["ledger"]
    require([{k: row[k] for k in ("seq", "length", "sha256")} for row in ledger] == rows, "independent_ledger")
    times = [row["at"] for row in ledger]
    require(all(number(at) for at in times) and times == sorted(times), "monotonic_ledger")
    window = capture["window"]
    check_window(window)
    require(all(window["start"] <= at <= window["end"] for at in times), "observation_bounds")
    # Each observer's fault boundary contributes even if there was no arrival
    # on the boundary; trailing silence must survive an otherwise good p95.
    gaps = [b - a for a, b in zip([window["start"]] + times, times + [window["end"]])]
    post = [at for at in times if at >= window["fault"]]
    gaps.append((post[0] if post else window["end"]) - window["fault"])
    return raw, times, gaps


# origin: hot-update S5 plan §6.2 / §9.1 / T47.
def check_identity(snapshots, socket_identity):
    require(isinstance(snapshots, list) and len(snapshots) >= 2, "identity_samples")
    required = {"pid", "boot_id", "start_ticks"}
    if socket_identity:
        required |= {"dev", "ino", "type", "tuple", "app_nonce"}
    for row in snapshots:
        valid = required <= row.keys() and all(uint(row[k]) and row[k] > 0 for k in ("pid", "start_ticks")) and bool(row["boot_id"])
        if socket_identity:
            valid = valid and all(uint(row[k]) for k in ("dev", "ino", "type")) and row["ino"] > 0 and row["type"] == stat.S_IFSOCK and bool(row["tuple"]) and bool(row["app_nonce"])
        require(valid, "identity_fields")
    require(all(row == snapshots[0] for row in snapshots[1:]), "identity_continuity")


# Independent RFC 8439 primitives: neither the Go SS codec nor its encryptor
# supplies expected plaintext. The hermetic test pins published RFC vectors.
def chacha_block(key, nonce, counter):
    state = list(struct.unpack("<4I", b"expand 32-byte k") + struct.unpack("<8I", key) + (counter,) + struct.unpack("<3I", nonce))
    work = state.copy()

    def quarter(a, b, c, d):
        for x, y, z, shift in ((a, b, d, 16), (c, d, b, 12), (a, b, d, 8), (c, d, b, 7)):
            work[x] = (work[x] + work[y]) & 0xffffffff
            work[z] ^= work[x]
            work[z] = ((work[z] << shift) | (work[z] >> (32 - shift))) & 0xffffffff
    for _ in range(10):
        for indexes in ((0, 4, 8, 12), (1, 5, 9, 13), (2, 6, 10, 14), (3, 7, 11, 15),
                        (0, 5, 10, 15), (1, 6, 11, 12), (2, 7, 8, 13), (3, 4, 9, 14)):
            quarter(*indexes)
    return struct.pack("<16I", *((a + b) & 0xffffffff for a, b in zip(state, work)))


def reference_xor_stream(key, nonce, data):
    result = bytearray()
    for offset in range(0, len(data), 64):
        block = chacha_block(key, nonce, offset // 64 + 1)
        result.extend(a ^ b for a, b in zip(data[offset:offset + 64], block))
    return bytes(result)


def native_chacha():
    # origin: hot-update S5 plan §9.1 / DRILL-RUN. A real backpressure
    # workload exceeds the carrier's 8 MiB credit. OpenSSL's independent
    # primitive keeps Python encryption from throttling that workload; the
    # scalar reference and published vectors remain the correctness controls.
    name = ctypes.util.find_library("crypto")
    if name is None:
        return None
    lib = ctypes.CDLL(name)
    for symbol, arguments, result in (
            ("EVP_CIPHER_CTX_new", [], ctypes.c_void_p),
            ("EVP_CIPHER_CTX_free", [ctypes.c_void_p], None),
            ("EVP_chacha20", [], ctypes.c_void_p),
            ("EVP_EncryptInit_ex", [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p], ctypes.c_int),
            ("EVP_EncryptUpdate", [ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_int), ctypes.c_void_p, ctypes.c_int], ctypes.c_int)):
        function = getattr(lib, symbol)
        function.argtypes, function.restype = arguments, result
    return lib


NATIVE_CHACHA = native_chacha()


def xor_stream(key, nonce, data):
    if NATIVE_CHACHA is None:
        return reference_xor_stream(key, nonce, data)
    lib = NATIVE_CHACHA
    context = lib.EVP_CIPHER_CTX_new()
    require(context, "reference_cipher_allocation")
    try:
        # The SS chunk is bounded to 16 KiB, so the 32-bit IETF counter
        # cannot carry into OpenSSL's extended counter/nonce representation.
        require(lib.EVP_EncryptInit_ex(context, lib.EVP_chacha20(), None, key, struct.pack("<I", 1)+nonce) == 1,
                "reference_cipher_initialization")
        output = ctypes.create_string_buffer(len(data)+16)
        length = ctypes.c_int()
        require(lib.EVP_EncryptUpdate(context, output, ctypes.byref(length), data, len(data)) == 1 and length.value == len(data),
                "reference_cipher_update")
        return output.raw[:length.value]
    finally:
        lib.EVP_CIPHER_CTX_free(context)


def poly1305(key, data):
    r = int.from_bytes(key[:16], "little") & 0x0ffffffc0ffffffc0ffffffc0fffffff
    accumulator = 0
    for offset in range(0, len(data), 16):
        accumulator = ((accumulator + int.from_bytes(data[offset:offset + 16] + b"\x01", "little")) * r) % ((1 << 130) - 5)
    return ((accumulator + int.from_bytes(key[16:], "little")) % (1 << 128)).to_bytes(16, "little")


def aead_tag(key, nonce, cipher, aad=b""):
    padded = aad + bytes((-len(aad)) % 16) + cipher + bytes((-len(cipher)) % 16) + struct.pack("<QQ", len(aad), len(cipher))
    return poly1305(chacha_block(key, nonce, 0)[:32], padded)


def open_aead(key, nonce, wire):
    require(len(wire) >= 16 and hmac.compare_digest(aead_tag(key, nonce, wire[:-16]), wire[-16:]), "aead_authentication")
    return xor_stream(key, nonce, wire[:-16])


def ss_subkey(master, salt):
    prk = hmac.digest(salt, master, "sha1")
    first = hmac.digest(prk, b"ss-subkey\x01", "sha1")
    return (first + hmac.digest(prk, first + b"ss-subkey\x02", "sha1"))[:32]


# origin: hot-update S5 plan §6.2 / §9.1 / T47.
def decrypt_ss(wire, master):
    require(len(wire) >= 32, "ss_salt")
    key, offset, counter, clear, calls = ss_subkey(master, wire[:32]), 32, 0, bytearray(), []
    while offset < len(wire):
        require(offset + 18 <= len(wire), "ss_length_suffix")
        nonce = counter.to_bytes(12, "little")
        length = int.from_bytes(open_aead(key, nonce, wire[offset:offset + 18]), "big")
        calls.append(dict(subkey_sha256=digest(key), nonce=nonce.hex()))
        counter += 1
        offset += 18
        require(0 < length <= 0x3fff and offset + length + 16 <= len(wire), "ss_payload_suffix")
        nonce = counter.to_bytes(12, "little")
        clear.extend(open_aead(key, nonce, wire[offset:offset + length + 16]))
        calls.append(dict(subkey_sha256=digest(key), nonce=nonce.hex()))
        counter += 1
        offset += length + 16
    return bytes(clear), calls


def check_ss(root, ss, application, used):
    master = sealed_bytes(ss["key_fd"])
    if len(master) != 32:
        raise EvidenceError("invalid sealed master length")
    for direction in ("up", "down"):
        record = ss["directions"][direction]
        wire = raw_bytes(root, record["file"], private=True)
        clear, expected_calls = decrypt_ss(wire, master)
        prefix = record["plaintext_prefix_bytes"]
        require(uint(prefix) and (direction == "up" or prefix == 0) and prefix <= len(clear), "ss_prefix")
        require(clear[prefix:] == application[direction], "ss_plaintext")
        calls = json.loads(raw_bytes(root, record["actual_seals"], private=True))
        # origin: hot-update S5 plan §6.2 / T47. Aliases and opposite
        # directions with the same actual subkey share a nonce domain.
        for call in calls:
            pair = (call["subkey_sha256"], call["nonce"])
            require(pair not in used, "nonce_reuse")
            used.add(pair)
        require(calls == expected_calls, "seal_ledger")
        partial = record["partial"]
        require(all(uint(partial[k]) for k in ("attempt_len", "written", "suffix_bytes", "seal_calls")) and 0 < partial["written"] < partial["attempt_len"] and partial["suffix_bytes"] == partial["attempt_len"] - partial["written"] and partial["seal_calls"] == 1, "partial_cipher_write")


# origin: hot-update S5 plan §6.2 / §9.1 / T47.
def judge(root, nonce, carrier_transition="replaced"):
    document = read_json(root / "evidence.json")
    if document["version"] != 1 or document["nonce"] != nonce or not re.fullmatch("[0-9a-f]{32}", nonce):
        raise EvidenceError("foreign or unsupported evidence")
    flows = document["flows"]
    require(isinstance(flows, list) and bool(flows), "nonempty_cohort")
    require(len({flow["id"] for flow in flows}) == len(flows), "unique_flows")
    # origin: hot-update S5 plan §6.2 / DRILL-OBSERVER. This includes attempts
    # whose ciphertext never reached a peer, and revoked actors omitted from
    # the survivor business trace. A direction or KeyID cannot partition it.
    if 'product_attempts' in document:
        attempts=json.loads(raw_bytes(root,document['product_attempts'],private=True))
        require(isinstance(attempts,list) and attempts,'product_attempts_nonempty')
        pairs=[(row['subkey_sha256'],row['nonce']) for row in attempts]
        require(len(pairs)==len(set(pairs)),'nonce_reuse')
    summaries, used = [], set()
    for flow in flows:
        category = flow["class"]
        require(flow["protocol"] in ("raw", "ss") and (flow["protocol"] == "ss") == ("ss" in flow), "crypto_required")
        require(category in ("normal", "slow", "idle", "half-close"), "declared_class")
        require(number(flow["declared_at"]) and all(flow["declared_at"] <= flow["directions"][d]["window"]["start"] for d in ("up", "down")), "class_before_start")
        require(set(flow["identities"]) == {"client", "target", "agent", "ctl"}, "identity_roles")
        for role, snapshots in flow["identities"].items():
            check_identity(snapshots, role in ("client", "target"))
        require(flow["accept_count"] == 1, "target_accept_once")
        require(flow["redials"] == 0, "no_application_redial")
        # origin: hot-update S5 plan §9.1 / T47. L proves refusal preserves
        # the original carrier; all exec/rebuild arms must prove replacement.
        require(bool(flow["carrier_before"]) and bool(flow["carrier_after"]), "carrier_identity")
        if carrier_transition == "unchanged":
            require(flow["carrier_before"] == flow["carrier_after"], "carrier_unchanged")
        else:
            require(flow["carrier_before"] != flow["carrier_after"], "carrier_changed")
        require(set(flow["directions"]) == {"up", "down"}, "duplex_directions")
        application, delays = {}, {}
        for direction in ("up", "down"):
            record = flow["directions"][direction]
            window = record["window"]
            check_window(window)
            expected = record["expected"]
            if not uint(expected["records"]) or not uint(expected["bytes"]):
                raise EvidenceError("invalid finish declaration")
            sent, _, send_gaps = check_capture(root, record["sent"], expected, nonce)
            received, times, receive_gaps = check_capture(root, record["received"], expected, nonce)
            require(sent == received, "duplex_bytes")
            application[direction] = sent
            require(record["received"]["window"] == window, "receiver_window")
            require(number(record["completed_at"]) and window["start"] <= record["completed_at"] <= window["finish_deadline"], "finite_finish")
            events = record["events"]
            require(all(event["kind"] in ("eof", "rst", "redial") and number(event["at"]) and window["start"] <= event["at"] <= window["finish_deadline"] for event in events), "event_shape")
            require(not any(event["kind"] == "rst" for event in events), "no_reset")
            require(not any(event["kind"] == "redial" for event in events), "no_redial_event")
            eof = [event["at"] for event in events if event["kind"] == "eof"]
            require(type(record["eof_expected"]) is bool and len(eof) == int(record["eof_expected"]), "expected_eof")
            require(not eof or eof[0] >= max(times, default=window["start"]) and eof[0] >= record["completed_at"], "eof_after_tail")
            if category in ("normal", "slow") or category == "half-close" and direction == "down":
                # Each observer must see both sides; comparing clocks across
                # hosts could manufacture progress through the freeze window.
                for capture in (record["sent"], record["received"]):
                    own = capture["window"]
                    arrivals = [row["at"] for row in capture["ledger"]]
                    require(sum(at < own["fault"] for at in arrivals) >= 20 and sum(at >= own["fault"] for at in arrivals) >= 20, "pre_post_samples")
            if category == "normal":
                require(max(send_gaps + receive_gaps) <= 5, "maximum_gap")
            if category == "half-close":
                if direction == "up":
                    require(len(eof) == 1 and eof[0] < window["fault"], "half_close_before_cut")
                else:
                    require(sum(row["length"] for row in record["received"]["ledger"] if row["at"] >= window["fault"]) > 8 << 20, "reverse_tail_after_cut")
            if category == "idle":
                require(expected["records"] == 0 and not events, "idle_stays_idle")
            delays[direction] = percentiles(send_gaps + receive_gaps)
        if "ss" in flow:
            check_ss(root, flow["ss"], application, used)
        summaries.append(dict(classification=category, delays=delays))
    if "requests" in document:
        requests = document["requests"]
        window, samples = requests["window"], requests["samples"]
        require(requests["rate"] == 10 and number(window["start"]) and number(window["end"]) and window["end"] > window["start"], "request_profile")
        ticks = math.ceil((window["end"] - window["start"]) * 10 - 1e-9)
        require(len(samples) == ticks and all(abs(row["scheduled"] - (window["start"] + i / 10)) < 1e-6 for i, row in enumerate(samples)), "request_schedule")
        require(all(row["ok"] is True and all(number(row[k]) for k in ("start", "end")) and row["scheduled"] <= row["start"] <= row["end"] for row in samples), "request_success")
        require(all(row["end"] - row["scheduled"] <= 5 for row in samples), "request_maximum")
    freezes = document.get("freeze_windows", [])
    if not all(number(value) for value in freezes):
        raise EvidenceError("invalid freeze calibration")
    return dict(verdict="GREEN", flows=summaries, seal_calls=len(used), freeze=percentiles(freezes), freeze_over_500ms=sum(value > .5 for value in freezes))


# origin: hot-update S5 plan §6.2 / §9.1 / T47 / DRILL-RUN 75.A.
def _native_socket_tuple(fd):
    # /proc/net is a seq_file walk, not an atomic census: a live socket can
    # disappear from one read. Query the owned OFD without dup, ownership
    # wrappers, flag changes, byte reads or consuming its pending SO_ERROR.
    library = ctypes.CDLL(None, use_errno=True)
    names = []
    for operation in ('getsockname', 'getpeername'):
        function = getattr(library, operation)
        function.argtypes = (ctypes.c_int, ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint))
        function.restype = ctypes.c_int
        address = ctypes.create_string_buffer(128)
        size = ctypes.c_uint(len(address))
        if function(fd, address, ctypes.byref(size)) != 0:
            raise OSError(ctypes.get_errno(), 'socket identity unavailable')
        raw = address.raw
        family = int.from_bytes(raw[:2], sys.byteorder)
        if family not in (socket.AF_INET, socket.AF_INET6) or size.value < (16 if family == socket.AF_INET else 28):
            raise EvidenceError('original socket tuple unavailable')
        packed = raw[4:8] if family == socket.AF_INET else raw[8:24]
        # Match the kernel's native-word proc encoding so both independent
        # observers retain exactly the same existing private tuple digests.
        encoded = ''.join(f'{word:08X}' for word in struct.unpack('=I' if family == socket.AF_INET else '=4I', packed))
        endpoint = encoded+f':{int.from_bytes(raw[2:4], "big"):04X}'
        names.append(('tcp' if family == socket.AF_INET else 'tcp6', digest(endpoint.encode())))
    if names[0][0] != names[1][0]:
        raise EvidenceError('original socket tuple unavailable')
    return [names[0][0], names[0][1], names[1][1]]


def identity(pid, fd=None, app_nonce=None):
    # Read stat twice to detect exit/PID reuse while collecting the socket.
    root = Path("/proc") / str(pid)
    before = (root / "stat").read_text().rsplit(")", 1)[1].split()
    result = dict(pid=pid, boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip(), start_ticks=int(before[19]))
    if fd is not None:
        path = root / "fd" / str(fd)
        value = path.stat()
        if not stat.S_ISSOCK(value.st_mode) or not app_nonce:
            raise EvidenceError("original socket or application nonce absent")
        tuples = []
        if pid == os.getpid():
            try:
                tuples.append(_native_socket_tuple(fd))
            except OSError as error:
                if error.errno != errno.ENOTCONN:
                    raise
        # Foreign owners and an actual pending connect still need proc's
        # recorded peer. Never consume SO_ERROR or infer a cached tuple.
        if not tuples:
            for name in ("tcp", "tcp6"):
                for line in (root / "net" / name).read_text().splitlines()[1:]:
                    fields = line.split()
                    if fields[9] == str(value.st_ino):
                        tuples.append([name, hashlib.sha256(fields[1].encode()).hexdigest(), hashlib.sha256(fields[2].encode()).hexdigest()])
        if len(tuples) != 1:
            raise EvidenceError("original socket tuple unavailable")
        # Both name reads must belong to the same original descriptor. A
        # close/reuse between the reads must remain a sticky identity failure.
        after_fd = path.stat()
        if (after_fd.st_dev, after_fd.st_ino, after_fd.st_mode) != (value.st_dev, value.st_ino, value.st_mode):
            raise EvidenceError('original socket changed during identity collection')
        result.update(dev=value.st_dev, ino=value.st_ino, type=stat.S_IFMT(value.st_mode), tuple=tuples[0], app_nonce=app_nonce)
    after = (root / "stat").read_text().rsplit(")", 1)[1].split()
    if before[19] != after[19]:
        raise EvidenceError("process changed during identity collection")
    return result


# origin: hot-update S5 plan §6.2 / §9.1 / T47.
def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("judge", "continuity"):
        command = commands.add_parser(name)
        command.add_argument("--evidence", required=True, type=Path)
        command.add_argument("--nonce", required=True)
        command.add_argument("--carrier-transition", choices=("replaced", "unchanged"), default="replaced")
    command = commands.add_parser("describe")
    for name in ("raw", "arrivals", "output"):
        command.add_argument("--" + name, required=True, type=Path)
    command.add_argument("--nonce", required=True)
    command = commands.add_parser("identity")
    command.add_argument("--pid", type=int, required=True)
    command.add_argument("--fd", type=int)
    command.add_argument("--app-nonce")
    args = parser.parse_args(argv)
    try:
        if args.command in ("judge", "continuity"):
            result = judge(args.evidence, args.nonce, args.carrier_transition)
        elif args.command == "identity":
            result = identity(args.pid, args.fd, args.app_nonce)
        else:
            raw = args.raw.read_bytes()
            rows, arrivals = parse_frames(raw, args.nonce), read_json(args.arrivals)
            if len(rows) != len(arrivals) or not all(number(at) for at in arrivals):
                raise EvidenceError("arrival observations do not partition capture")
            result = dict(file=args.raw.name, bytes=len(raw), sha256=digest(raw), ledger=[dict(row, at=at) for row, at in zip(rows, arrivals)])
            args.output.write_text(json.dumps(result, sort_keys=True) + "\n")
        print(json.dumps(result, sort_keys=True))
        return 0
    except ContinuityError as error:
        print(json.dumps(dict(verdict="ASSERT-FAIL", criterion=str(error))))
        return 1
    except (EvidenceError, OSError, ValueError, KeyError, TypeError, AttributeError, IndexError, OverflowError, struct.error):
        # Parser/FD/path failures are setup failures; never expose raw values.
        print(json.dumps(dict(verdict="SETUP-RED", criterion="invalid_evidence")))
        return 2


if __name__ == "__main__":
    sys.exit(main())
