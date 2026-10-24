#!/usr/bin/env python3
"""Independent file, physical JetStream and canonical-history readers."""
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import time
from urllib.parse import urlsplit

from transfer_observation import SetupError, require, unique_object


def digest_file(fd, limit=None):
    digest, offset = hashlib.sha256(), 0
    while limit is None or offset < limit:
        data = os.pread(fd, min(1 << 20, limit - offset) if limit is not None else 1 << 20, offset)
        if not data:
            break
        digest.update(data)
        offset += len(data)
    return offset, digest.hexdigest()


class TargetOracle:
    """Open once per observation, fixing the inode throughout its hash read.

    A detected partial file remains a failure even after a later correct rename
    or in-place restoration. Sampling never uses the worker's tmp descriptor.
    """
    def __init__(self, path, old, new):
        self.path, self.old, self.new = Path(path), old, new
        self.samples, self.failed = [], False
        self.fingerprint, self.content = None, None

    def sample(self, boundary='poll'):
        fd = None
        try:
            fd = os.open(self.path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
            before = os.fstat(fd)
            require(__import__('stat').S_ISREG(before.st_mode), 'target_not_regular')
            fingerprint = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            if fingerprint == self.fingerprint:
                length, digest = self.content
            else:
                length, digest = digest_file(fd)
            after = os.fstat(fd)
            # Atomic replacement can unlink this pinned old inode during the
            # read, changing ctime without changing any content. Content writes
            # change size/mtime; ctime participates in the next cache lookup.
            stable = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) == (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            state = 'new' if [length, digest] == self.new else 'old' if [length, digest] == self.old else 'partial'
            if not stable:
                state = 'partial'
            else:
                self.fingerprint, self.content = fingerprint, (length, digest)
            row = dict(boundary=boundary, at_ns=time.monotonic_ns(), state=state,
                       device=before.st_dev, inode=before.st_ino, bytes=length, sha256=digest)
        except FileNotFoundError:
            row = dict(boundary=boundary, at_ns=time.monotonic_ns(), state='absent')
            if self.old is not None:
                row['state'] = 'partial'
        finally:
            if fd is not None:
                os.close(fd)
        # origin: hot-update S7 plan §10.4 / T42 / M10B.
        self.failed |= row['state'] == 'partial'
        self.samples.append(row)
        return row


class NATSReader:
    """Bounded read-only NATS API client, including real TLS/WebSocket frames.

    Read credentials are provided by inherited descriptor, never argv/evidence.
    This reader issues only STREAM.INFO and STREAM.MSG.GET requests. It never
    updates a stream, writes history, retries a transfer or executes a command.
    """
    def __init__(self, url, connect=None, ca_file=None, timeout=5):
        parsed = urlsplit(url)
        require(parsed.scheme in ('nats', 'tls', 'ws', 'wss') and parsed.hostname and
                not parsed.username and not parsed.password, 'nats_endpoint')
        self.url, self.connect, self.ca_file, self.timeout = parsed, connect or {}, ca_file, timeout
        self.socket, self.buffer, self.websocket = None, bytearray(), parsed.scheme in ('ws', 'wss')
        self.deadline = time.monotonic() + timeout

    def __enter__(self):
        p = self.url
        self.deadline = time.monotonic() + self.timeout
        self.socket = socket.create_connection((p.hostname, p.port or (443 if p.scheme == 'wss' else
                                               80 if p.scheme == 'ws' else 4222)), self.timeout)
        try:
            if p.scheme in ('tls', 'wss'):
                self.socket = ssl.create_default_context(cafile=self.ca_file).wrap_socket(
                    self.socket, server_hostname=p.hostname)
            if self.websocket:
                key = base64.b64encode(os.urandom(16)).decode()
                self.socket.sendall((f'GET {p.path or "/"} HTTP/1.1\r\nHost: {p.netloc}\r\n'
                    f'Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n'
                    'Sec-WebSocket-Version: 13\r\n\r\n').encode())
                header = bytearray()
                while not header.endswith(b'\r\n\r\n'):
                    require(len(header) < 16384, 'websocket_header')
                    header.extend(self.exact(1))
                lines = header.decode('ascii').split('\r\n')
                fields = {k.lower(): v.strip() for k, v in (line.split(':', 1) for line in lines[1:] if ':' in line)}
                expected = base64.b64encode(hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
                require(lines[0].startswith('HTTP/1.1 101 ') and fields.get('sec-websocket-accept') == expected,
                        'websocket_upgrade')
            require(self.line().startswith(b'INFO '), 'nats_info')
            self.send(b'CONNECT ' + json.dumps(dict(self.connect, verbose=False, pedantic=True,
                                                  protocol=1, headers=True)).encode() + b'\r\nPING\r\n')
            while self.line() != b'PONG':
                pass
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_):
        if self.socket:
            self.socket.close()
            self.socket = None

    def exact(self, length):
        data = bytearray()
        while len(data) < length:
            self.bound()
            part = self.socket.recv(length - len(data))
            require(part, 'nats_truncated')
            data.extend(part)
        return bytes(data)

    def bound(self):
        remaining = self.deadline - time.monotonic()
        require(remaining > 0, 'nats_deadline')
        self.socket.settimeout(remaining)

    def send(self, data, opcode=2):
        self.bound()
        if self.websocket:
            mask = os.urandom(4)
            count = len(data)
            import struct
            header = bytes((0x80 | opcode, 0x80 | count)) if count < 126 else (
                bytes((0x80 | opcode, 0x80 | 126)) + struct.pack('!H', count) if count < 65536 else
                bytes((0x80 | opcode, 0x80 | 127)) + struct.pack('!Q', count))
            data = header + mask + bytes(v ^ mask[i % 4] for i, v in enumerate(data))
        self.socket.sendall(data)

    def fill(self):
        self.bound()
        if not self.websocket:
            data = self.socket.recv(65536)
            require(data, 'nats_truncated')
            self.buffer.extend(data)
            return
        import struct
        first, second = self.exact(2)
        require(not second & 128 and not first & 0x70, 'websocket_frame')
        length = second & 127
        length = struct.unpack('!H', self.exact(2))[0] if length == 126 else (
            struct.unpack('!Q', self.exact(8))[0] if length == 127 else length)
        require(length <= 8 << 20, 'websocket_capacity')
        data, opcode = self.exact(length), first & 15
        if opcode == 9:
            self.send(data, 10)
        elif opcode in (0, 1, 2):
            self.buffer.extend(data)
        elif opcode != 10:
            raise SetupError('websocket_closed')

    def line(self):
        while b'\r\n' not in self.buffer:
            require(len(self.buffer) < 8 << 20, 'nats_line_capacity')
            self.fill()
        offset = self.buffer.index(b'\r\n')
        value = bytes(self.buffer[:offset])
        del self.buffer[:offset + 2]
        require(not value.startswith(b'-ERR'), 'nats_api_denied')
        if value == b'PING':
            self.send(b'PONG\r\n')
            return self.line()
        return value

    def request(self, subject, body):
        return self.request_many([(subject, body)])[0]

    def request_many(self, requests):
        # Pipeline independent physical reads. One request per RTT cannot keep
        # up with four live cohorts at a real 100ms RTT; this changes sampler
        # IO only, never the writer's frozen single-pending-chunk contract.
        require(0 < len(requests) <= 64, 'nats_pipeline_capacity')
        self.deadline = time.monotonic() + self.timeout
        inbox = '_INBOX.xferfixture.' + os.urandom(16).hex()
        outgoing, expected = bytearray(), {}
        for index, (subject, body) in enumerate(requests, 1):
            require(all(c.isalnum() or c in '.$_-' for c in subject), 'nats_subject')
            reply = inbox + '.' + str(index)
            payload = json.dumps(body).encode()
            outgoing += f'SUB {reply} {index}\r\nUNSUB {index} 1\r\nPUB {subject} {reply} {len(payload)}\r\n'.encode()
            outgoing += payload + b'\r\n'
            expected[str(index).encode()] = reply.encode()
        self.send(outgoing)
        results = {}
        deadline = time.monotonic() + self.timeout
        while len(results) < len(requests):
            remaining = deadline - time.monotonic()
            require(remaining > 0, 'nats_request_deadline')
            self.socket.settimeout(remaining)
            line = self.line().split()
            if line and line[0] in (b'MSG', b'HMSG'):
                require(line[2] in expected and line[1] == expected[line[2]] and line[2] not in results,
                        'nats_reply_identity')
                size = int(line[-1])
                require(0 <= size <= 8 << 20, 'nats_reply_capacity')
                while len(self.buffer) < size + 2:
                    self.fill()
                raw = bytes(self.buffer[:size])
                require(self.buffer[size:size + 2] == b'\r\n', 'nats_reply_suffix')
                del self.buffer[:size + 2]
                if line[0] == b'HMSG':
                    raw = raw[int(line[-2]):]
                try:
                    results[line[2]] = json.loads(raw, object_pairs_hook=unique_object)
                except (ValueError, UnicodeError):
                    raise SetupError('nats_reply_json') from None
        return [results[str(index).encode()] for index in range(1, len(requests) + 1)]


class StreamScan:
    """Read every physical sequence, preserving holes and exact record bytes."""
    def __init__(self, stream):
        require(stream and all(c.isalnum() or c in '_-' for c in stream), 'stream_name')
        self.stream, self.created, self.cursor, self.rows = stream, None, 0, []
        self.hashers, self.config = {}, None

    def scan(self, reader):
        base = '$JS.API.STREAM.'
        info = reader.request(base + 'INFO.' + self.stream, {})
        require('error' not in info and 'created' in info and 'state' in info, 'stream_info')
        require(self.created in (None, info['created']), 'stream_recreated')
        self.created = info['created']
        # origin: S7 internal review rv-drill F7.
        # Missing storage metadata after a restart is evidence damage too;
        # skipping it would let the reader silently forget its pinned store.
        require(isinstance(info.get('config'), dict) and info['config'].get('name') == self.stream and
                info['config'].get('storage') in ('file', 'memory'), 'stream_config_missing')
        require(self.config in (None, info['config']), 'stream_config_changed')
        self.config = info['config']
        end = info['state']['last_seq']
        require(type(end) is int and end >= self.cursor, 'stream_cursor_reversed')
        # The deadline bounds the entire walk, not only each network attempt.
        deadline = time.monotonic() + reader.timeout
        for batch in range(self.cursor + 1, end + 1, 64):
            require(time.monotonic() < deadline, 'stream_scan_deadline')
            sequences = range(batch, min(batch + 64, end + 1))
            replies = reader.request_many([(base + 'MSG.GET.' + self.stream, {'seq': seq}) for seq in sequences])
            for sequence, reply in zip(sequences, replies):
                if 'error' in reply:
                    require(reply['error'].get('err_code') == 10037, 'stream_scan_error')
                    self.rows.append(dict(sequence=sequence, absent=True))
                else:
                    msg = reply.get('message', {})
                    require(msg.get('seq') == sequence and isinstance(msg.get('subject'), str), 'stream_scan_identity')
                    try:
                        raw = base64.b64decode(msg.get('data', ''), validate=True)
                        headers = base64.b64decode(msg.get('hdrs', ''), validate=True)
                    except ValueError:
                        raise SetupError('stream_scan_base64') from None
                    chunk = msg['subject'].startswith('$O.') and '.C.' in msg['subject']
                    prefix = self.hashers.setdefault(msg['subject'], hashlib.sha256())
                    prefix.update(raw)
                    self.rows.append(dict(sequence=sequence, subject=msg['subject'], bytes=len(raw),
                                         sha256=hashlib.sha256(raw).hexdigest(), prefix_sha256=prefix.hexdigest(),
                                         data_b64=None if chunk else base64.b64encode(raw).decode(),
                                         headers_b64=base64.b64encode(headers).decode(), time=msg.get('time')))
                self.cursor = sequence
        return dict(stream=self.stream, created=self.created, cursor=self.cursor,
                    first_seq=info['state']['first_seq'], messages=info['state']['messages'])

    def verify_retained(self, reader, subjects):
        # Cached prior observations cannot prove that live chunks/history still
        # exist after a reaper pass. Re-read the required physical rows at both
        # ending barriers; comparison is against original bytes, not receipts.
        rows = [r for r in self.rows if r.get('subject') in subjects]
        deadline = time.monotonic() + reader.timeout
        for offset in range(0, len(rows), 64):
            require(time.monotonic() < deadline, 'stream_retention_deadline')
            group = rows[offset:offset + 64]
            replies = reader.request_many([('$JS.API.STREAM.MSG.GET.' + self.stream,
                                           {'seq': r['sequence']}) for r in group])
            for row, reply in zip(group, replies):
                msg = reply.get('message', {})
                require('error' not in reply and msg.get('seq') == row['sequence'] and
                        msg.get('subject') == row['subject'], 'required_physical_row_missing')
                raw = base64.b64decode(msg.get('data', ''), validate=True)
                require(hashlib.sha256(raw).hexdigest() == row['sha256'], 'physical_row_changed')


def judge_history(rows, transfer):
    subject = transfer['history_subject']
    records = [row for row in rows if row.get('subject') == subject]
    # Compare full canonical bytes, including opposite terminal kinds. A boolean
    # "terminal exists" test can hide a late failed after a valid complete.
    # origin: hot-update S7 plan §10.4 / T42 / M12.
    expected = [transfer['start_b64'], transfer['terminal_b64']]
    return len(records) == 2 and [r['data_b64'] for r in records] == expected


def judge_object(rows, transfer, events):
    subject = '$O.' + transfer['bucket'] + '.C.' + transfer['object_version']
    chunks = [row for row in rows if row.get('subject') == subject]
    receipts = [e for e in events if e['transfer_id'] == transfer['transfer_id'] and
                e['kind'] in ('append', 'resolve') and e.get('actual_ack_seq', 0) > 0]
    by_index = {}
    for event in receipts:
        index = event['index']
        receipt = (event['actual_ack_seq'], event['offset'], event['length'], event['sha256'])
        if index in by_index and by_index[index] != receipt:
            return False
        by_index[index] = receipt
    if not chunks or sorted(by_index) != list(range(len(chunks))):
        return False
    offset = 0
    for index, row in enumerate(chunks):
        seq, expected_offset, length, sha = by_index[index]
        if (seq, expected_offset, length, sha) != (row['sequence'], offset, row['bytes'], row['sha256']):
            return False
        offset += row['bytes']
    if offset != transfer['size'] or chunks[-1]['prefix_sha256'] != transfer['sha256']:
        return False
    # Metadata is independently read from the physical stream; a receipt saying
    # Finalize succeeded is insufficient if meta named a foreign NUID or was
    # published before the final chunk. The SDK Get oracle lives in the Go
    # harness, while this check keeps the live Python judge format-aware.
    meta_subject = '$O.' + transfer['bucket'] + '.M.' + base64.urlsafe_b64encode(transfer['object_name'].encode()).decode()
    all_meta = [r for r in rows if r.get('subject') == meta_subject]
    try:
        # Finalizer's SDK deleted tombstone is a legitimate later physical row.
        # Preserve the original readable version proof before cleanup, rather
        # than asking Get to read an object whose owners correctly retired it.
        meta = [r for r in all_meta if not json.loads(base64.b64decode(r['data_b64'])).get('deleted')]
    except (ValueError, KeyError, TypeError):
        return False
    if len(meta) != 1 or meta[0]['sequence'] <= chunks[-1]['sequence']:
        return False
    try:
        info = json.loads(base64.b64decode(meta[0]['data_b64']), object_pairs_hook=unique_object)
        digest = 'SHA-256=' + base64.urlsafe_b64encode(bytes.fromhex(transfer['sha256'])).decode()
        return info.get('nuid') == transfer['object_version'] and info.get('size') == transfer['size'] and \
            info.get('chunks') == len(chunks) and info.get('digest') == digest and not info.get('deleted')
    except (ValueError, KeyError, TypeError):
        return False


# origin: S7 internal review rv-drill F2. Retained owners are not byte senders.
def responsibility(row, role):
    if role == 'sender':
        return row['kind'] == 'append' and row.get('actual_ack_seq', 0) > 0
    if role == 'receiver':
        return row['kind'] == 'synced' and row.get('last_consumed_stream_seq', 0) > 0
    if role == 'prepared':
        return row['kind'] in ('control', 'freeze', 'import') and row['phase'] == 'prepared'
    if role == 'courier':
        return row['kind'] in ('control', 'freeze', 'import', 'finish') and row['phase'] in (
            'terminal_pending', 'cleanup_pending') and bool(row.get('canonical_digest'))
    if role == 'tracker':
        return row['kind'] in ('control', 'freeze', 'import', 'reserve', 'ledger_sync') and \
            row.get('revision', 0) > 0 and bool(row.get('original_deadline'))
    return role == 'pending-data' and row['kind'] in ('append', 'fetched', 'synced')


def progress(row):
    return row.get('durable_offset', 0) if row['kind'] == 'synced' else row.get('offset', 0) + row.get('length', 0)


def cut_coverage(manifest, events):
    missing = []
    for cut in manifest['cuts']:
        rows = [r for r in events if all(r.get(k) == cut[k] for k in ('transfer_id', 'leg', 'producer'))]
        size = next(t['size'] for t in manifest['transfers'] if t['transfer_id'] == cut['transfer_id'])
        installs = [r for r in rows if r['kind'] in ('install', 'rollback') and
                    r['cut_id'] == cut['cut_id'] and r['generation'] == cut['generation']]
        valid = len(installs) == 1
        if valid:
            installed = installs[0]
            identity = ('pid', 'process_start', 'boot_id')
            own = [r for r in rows if all(k in installed and r.get(k) == installed[k] for k in identity)]
            before = [r for r in own if r['boottime_ns'] < installed['boottime_ns'] and
                      r['generation'] == installed['generation'] - 1 and responsibility(r, cut['role'])]
            after = [r for r in own if r['boottime_ns'] > installed['boottime_ns'] and
                     r['generation'] == installed['generation'] and responsibility(r, cut['role'])]
            # Every role retains a live transfer, but only actual data receipts
            # can prove the leg was unfinished at installation and advanced later.
            data = [r for r in events if r.get('transfer_id') == cut['transfer_id'] and r.get('leg') == cut['leg']
                    and r['kind'] in ('append', 'synced') and
                    (cut['role'] not in ('sender', 'receiver') or r in own and responsibility(r, cut['role']))]
            prior = sorted((r for r in data if r['boottime_ns'] <= installed['boottime_ns']),
                           key=lambda r: r['boottime_ns'])
            following = [r for r in data if r['boottime_ns'] > installed['boottime_ns']]
            first = min((r['boottime_ns'] for r in data), default=0)
            valid = bool(before and after and prior and following and first and
                         0 < progress(prior[-1]) < size and
                         max(map(progress, following)) > progress(prior[-1]) and
                         0 <= installed['boottime_ns'] - first <= cut['max_after_first_ns'])
        if not valid:
            missing.append(cut['cut_id'] + ':' + cut['transfer_id'] + ':' + cut['leg'])
    return missing


# origin: S7 internal review rv-drill F6. The clock creates slots, never the
# observed success set. Budget includes every retry and time spent backing off.
def judge_control(control, events, effects):
    required = {'baseline_ns', 'drain_ns', 'logical_ids'}
    require(required <= set(control), 'control_clock_missing')
    start, end = control['baseline_ns'], control['drain_ns']
    require(type(start) is int and type(end) is int and 0 < start < end, 'control_clock')
    slots = list(range(start, end, 100_000_000))
    ids = control['logical_ids']
    require(ids and len(ids) == len(set(ids)) == len(slots), 'control_slot_gap')
    grouped = {key: [] for key in ids}
    for row in events:
        if row['kind'] == 'attempt':
            require(row['attempt']['logical_id'] in grouped, 'control_foreign_slot')
            grouped[row['attempt']['logical_id']].append(row)
    for logical, scheduled in zip(ids, slots):
        rows = sorted(grouped[logical], key=lambda r: r['attempt']['ordinal'])
        require(rows, 'control_slot_gap')
        attempts = [r['attempt'] for r in rows]
        for attempt in attempts:
            require({'logical_id', 'ordinal', 'phase', 'started_mono_ns', 'finished_mono_ns',
                     'sent', 'class', 'backoff_ns'} <= set(attempt), 'control_attempt_fields')
            require(all(type(attempt[k]) is int and attempt[k] > 0 for k in
                        ('ordinal', 'started_mono_ns', 'finished_mono_ns')) and
                    type(attempt['backoff_ns']) is int and attempt['backoff_ns'] >= 0 and
                    attempt['finished_mono_ns'] >= attempt['started_mono_ns'] and
                    type(attempt['sent']) is bool, 'control_attempt_interval')
        require([a['ordinal'] for a in attempts] == list(range(1, len(attempts) + 1)), 'control_attempt_gap')
        require(scheduled <= attempts[0]['started_mono_ns'] < scheduled + 100_000_000, 'control_slot_late')
        # S6 phases may nest (connect contains DNS/TLS), and a backoff's
        # duration is already inside its start/finish interval. Count wall
        # elapsed once while still rejecting a fabricated backoff duration.
        for attempt in attempts:
            require(attempt['backoff_ns'] <= attempt['finished_mono_ns'] - attempt['started_mono_ns'] and
                    (attempt['backoff_ns'] == 0 or attempt['phase'] == 'backoff' and not attempt['sent']),
                    'control_backoff')
        for previous, current in zip(attempts, attempts[1:]):
            require(current['started_mono_ns'] >= previous['started_mono_ns'], 'control_attempt_order')
        finished = max(a['finished_mono_ns'] for a in attempts)
        require(finished - scheduled <= 5_000_000_000, 'control_budget_exceeded')
        require(attempts[-1]['class'] == 'ok' and any(a['sent'] for a in attempts) and
                rows[-1].get('native_outcome') == 0, 'control_terminal_missing')
        effect = effects.get(logical)
        require(effect is not None and {'finished_ns', 'native_rc', 'effect_count', 'observed'} <= set(effect), 'control_effect_fields')
        require(type(effect['finished_ns']) is int and finished <= effect['finished_ns'] <= scheduled + 5_000_000_000 and effect['native_rc'] == 0 and effect.get('effect_count') == 1 and
                effect.get('observed') is True, 'control_effect_missing')


# origin: S7 internal review rv-drill F5. An independent cell must actually
# fail import, roll back the same owner, and make new progress afterwards. R2
# additionally proves partial activation; an ordinary R1 failure cannot sign it.
def rollback_task_coverage(manifest, events):
    missing = []
    for task in manifest.get('rollback_tasks', []):
        own = [e for e in events if e['transfer_id'] == task['transfer']['transfer_id']]
        imported = [e for e in own if e['kind'] == 'import' and e.get('outcome') == 'failed' and
                    e['cut_id'] == task['cell']]
        rolled = [e for e in own if e['kind'] == 'rollback' and e['cut_id'] == task['cell']]
        valid = len(imported) == len(rolled) == 1
        if valid:
            failed, restored = imported[0], rolled[0]
            identity = ('pid', 'process_start', 'boot_id', 'producer')
            valid = all(failed.get(k) is not None and failed[k] == restored.get(k) for k in identity) and \
                    failed['boottime_ns'] < restored['boottime_ns'] and restored['generation'] >= failed['generation']
            if manifest['arm'] == 'R2':
                valid = valid and any(e.get('active') is True and e['boottime_ns'] <= failed['boottime_ns'] and
                    all(e.get(k) == failed[k] for k in identity) for e in own)
            if task['tier'] == 'A':
                valid = valid and any(e['kind'] == 'primitive_exit' and e.get('outcome') == 'landed' and
                    e['boottime_ns'] > restored['boottime_ns'] for e in own)
            else:
                data = [e for e in own if e['kind'] in ('append', 'synced')]
                before = [progress(e) for e in data if e['boottime_ns'] <= restored['boottime_ns']]
                after = [progress(e) for e in data if e['boottime_ns'] > restored['boottime_ns']]
                valid = valid and bool(before and after and 0 < max(before) < task['transfer']['size'] and
                                       max(after) > max(before))
        if not valid:
            missing.append(task['cell'] + ':original-rollback-task')
    return missing
