#!/usr/bin/env python3
"""Read private sealed transfer custody without trusting product summaries.

This is a reader, not a second producer. P28 owns the transfer-v1 payload and
the existing TDRILL1 envelope. Only digests, counters and resource IDs leave
custody; arbitrary payloads and exception text never become public evidence.
"""
import array
import fcntl
import hashlib
import json
import os
import socket
import struct

SEALS = fcntl.F_SEAL_SEAL | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_GROW | fcntl.F_SEAL_WRITE
MAX_FRAME = 1 << 20
MAX_ARCHIVE = 256 << 20
KINDS = frozenset('append resolve finalize fetched synced reader_stop primitive_enter '
                  'primitive_exit directory_sync freeze import control acknowledge reserve finish '
                  'callback_enter callback_join ledger_sync history retirement install rollback '
                  'attempt drain'.split())
IDENTITY = ('process_start', 'boot_id', 'pid', 'generation', 'cut_id', 'transfer_id',
            'entry_nonce', 'object_version', 'leg', 'phase', 'event_seq', 'boottime_ns')
# origin: hot-update S7 plan §10.4 / P29. This handoff is also the reader matrix
# supplied to P34/P35; missing a producer is missing evidence, never an exemption.
PRODUCERS = {
    'P02': ('append', 'resolve', 'finalize'),
    'P03': ('fetched', 'synced', 'reader_stop'),
    'P05': ('primitive_enter', 'primitive_exit', 'directory_sync', 'freeze', 'import'),
    'P06/P09': ('control', 'acknowledge'),
    'P07': ('synced',),
    'P11': ('reserve', 'finish', 'callback_enter', 'callback_join'),
    'P12/P13/P14/P15': ('ledger_sync', 'history', 'retirement'),
    'P21/P24': ('install', 'rollback'),
    'P23/P26': ('attempt',),
}
TEXT = frozenset(IDENTITY[1:2] + IDENTITY[4:10] + (
    'kind', 'sha256', 'hash_state_digest', 'consumer_identity', 'connection_identity',
    'server_identity', 'stream_identity', 'primitive', 'outcome', 'ofd_witness_digest',
    'grant_nonce', 'canonical_digest', 'stop_nonce', 'retirement_digest', 'reader_sha'))
NUMBERS = frozenset(('process_start', 'pid', 'generation', 'event_seq', 'boottime_ns',
    'index', 'offset', 'length', 'expected_subject_seq', 'expected_stream_seq', 'actual_ack_seq',
    'attachment_generation', 'fetched_offset', 'durable_offset', 'last_consumed_stream_seq',
    'dropped', 'errno', 'revision', 'primary_revision', 'outbox_revision', 'raft_index',
    'history_seq', 'publisher_checkpoint', 'native_outcome'))
BOOLEANS = frozenset(('pong_confirmed', 'consumer_deleted', 'parent_synced', 'admission_closed',
    'file_synced', 'directory_synced', 'active', 'installed_before_reaper'))
NESTED = frozenset(('tmp', 'parent', 'source', 'original_deadline', 'attempt'))


class SetupError(ValueError):
    """A fixed diagnostic code; never include untrusted packet/error text."""


def require(condition, code):
    if not condition:
        raise SetupError(code)


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, 'duplicate_json_field')
        value[key] = item
    return value


def validate_event(row):
    require(isinstance(row, dict) and row.get('schema') == 1, 'observation_schema')
    require(set(row) <= TEXT | NUMBERS | BOOLEANS | NESTED | {'schema'}, 'observation_extra_field')
    require(set(IDENTITY) <= set(row) and row.get('kind') in KINDS, 'observation_fields')
    for key, value in row.items():
        if key in TEXT:
            require(isinstance(value, str) and len(value) <= 256, 'observation_text')
        elif key in NUMBERS:
            require(type(value) is int and 0 <= value < 1 << 64, 'observation_number')
        elif key in BOOLEANS:
            require(type(value) is bool, 'observation_boolean')
        elif key in NESTED:
            require(isinstance(value, dict), 'observation_nested')
            # Exact subfields are frozen P00 values. Reject extra string payloads
            # so a capsule/path cannot sneak through an allegedly typed field.
            allowed = {
                'tmp': {'resource_id', 'kind', 'name'},
                'parent': {'resource_id', 'kind', 'name'},
                'source': {'resource_id', 'kind', 'name'},
                'original_deadline': {'boot_id', 'deadline'},
                'attempt': {'logical_id', 'ordinal', 'phase', 'started_mono_ns',
                            'finished_mono_ns', 'sent', 'class', 'backoff_ns'},
            }[key]
            require(set(value) <= allowed, 'observation_nested_fields')
            require(all(type(v) in (str, int, bool) for v in value.values()), 'observation_nested_value')
            require(all(not isinstance(v, str) or len(v) <= 256 for v in value.values()),
                    'observation_nested_text')
    # origin: S7 internal review rv-drill F6. Missing times cannot mean zero cost.
    if row['kind'] == 'attempt':
        attempt = row.get('attempt', {})
        require({'logical_id', 'ordinal', 'phase', 'started_mono_ns', 'finished_mono_ns',
                 'sent', 'class'} <= set(attempt), 'attempt_fields')
        require(all(type(attempt[k]) is int and attempt[k] > 0 for k in
                    ('ordinal', 'started_mono_ns', 'finished_mono_ns')), 'attempt_number')
        require(attempt['finished_mono_ns'] >= attempt['started_mono_ns'] and
                type(attempt['sent']) is bool and attempt['logical_id'] and attempt['phase'] and
                type(attempt.get('backoff_ns', 0)) is int and attempt.get('backoff_ns', 0) >= 0,
                'attempt_interval')
    require(all(row[k] > 0 for k in ('pid', 'process_start', 'generation', 'event_seq', 'boottime_ns')),
            'observation_identity')
    require(row['boot_id'] and row['transfer_id'] and row['entry_nonce'] and
            row['leg'] in ('leg1', 'leg2'), 'observation_binding')
    return row


def sealed(data):
    fd = os.memfd_create('transfer-custody', os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    try:
        require(os.write(fd, data) == len(data), 'custody_short_write')
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS, SEALS)
        return fd
    except BaseException:
        os.close(fd)
        raise


def receive(channel, capacity=16):
    raw, ancillary, flags, _ = channel.recvmsg(MAX_FRAME, socket.CMSG_SPACE(capacity * 4))
    fds = []
    for level, kind, data in ancillary:
        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
            values = array.array('i')
            values.frombytes(data[:len(data) - len(data) % values.itemsize])
            fds.extend(values)
    try:
        require(not flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC), 'custody_packet_truncated')
        require(len(fds) <= capacity, 'custody_descriptor_capacity')
        for fd in fds:
            os.set_inheritable(fd, False)
        return raw, fds
    except BaseException:
        for fd in fds:
            os.close(fd)
        raise


def decode(fd):
    require(fcntl.fcntl(fd, fcntl.F_GET_SEALS) & SEALS == SEALS, 'custody_unsealed')
    size = os.fstat(fd).st_size
    require(76 <= size <= MAX_FRAME, 'custody_size')
    raw = os.pread(fd, size, 0)
    require(len(raw) == size and raw[:8] == b'TDRILL1\0', 'custody_header')
    sequence, poison, count = struct.unpack_from('<QQI', raw, 56)
    require(poison == 0, 'custody_producer_poison')
    require(sequence > 0 and count <= 16, 'custody_batch')
    offset, events = 76, []

    def take(length):
        nonlocal offset
        require(offset + length <= len(raw), 'custody_truncated')
        value, offset = raw[offset:offset + length], offset + length
        return value

    for _ in range(count):
        kind, domain = take(take(1)[0]), take(take(1)[0])
        # P28 must confirm this adapter before any production GREEN receipt.
        # S5 events may coexist on the shared custody channel; they remain in
        # the sealed archive but cannot supply transfer-v1 evidence.
        take(92)
        at, = struct.unpack_from('<Q', take(136))
        length, = struct.unpack('<I', take(4))
        require(length <= 32768 and at > 0, 'custody_event_size')
        payload = take(length)
        if kind == b'transfer-v1' and domain == b'transfer':
            try:
                row = json.loads(payload, object_pairs_hook=unique_object)
            except (UnicodeError, json.JSONDecodeError):
                raise SetupError('observation_json') from None
            events.append(validate_event(row))
        else:
            require(domain in (b'ss', b'flow', b'tunnel', b'agent', b'exec', b'pty', b'proxy'),
                    'custody_domain')
    require(offset == len(raw), 'custody_trailing')
    return raw[8:40], raw[40:56], sequence, events, raw


class Records:
    """Monotone custody and event cursors with sticky poisoning across exec."""
    def __init__(self):
        self.events, self.frames = [], []
        self.identities, self.sequences, self.incarnations, self.event_sequences = {}, {}, {}, {}
        self.bytes, self.failure = 0, None

    def add(self, fd, source, expected_identity):
        try:
            identity, incarnation, seq, rows, raw = decode(fd)
            require(identity == expected_identity, 'custody_identity_changed')
            key = (source, incarnation)
            require(seq == self.sequences.get(key, 0) + 1, 'custody_sequence_gap')
            prior = self.incarnations.get(source)
            require(prior in (None, incarnation) or key not in self.sequences,
                    'custody_replayed_incarnation')
            require(self.bytes + len(raw) <= MAX_ARCHIVE, 'custody_capacity')
            for row in rows:
                actor = (source, row['pid'], row['process_start'], row['boot_id'], row['generation'])
                require(row['event_seq'] == self.event_sequences.get(actor, 0) + 1,
                        'observation_sequence_gap')
                self.event_sequences[actor] = row['event_seq']
                self.events.append(dict(row, producer=source))
            self.sequences[key], self.incarnations[source] = seq, incarnation
            self.identities[source] = identity
            self.bytes += len(raw)
            # Retain the exact envelope in sealed memory, including nontransfer
            # events. JSON receipts carry only its SHA and absolute cursor.
            self.frames.append(sealed(raw))
            if len(self.frames) >= 128:
                # Compact sealed envelopes without dropping their exact bytes.
                # A long, rate-limited live run must not consume one FD per
                # batch. The byte ceiling still applies after compaction.
                archive = sealed(b''.join(os.pread(f, os.fstat(f).st_size, 0) for f in self.frames))
                for held in self.frames:
                    os.close(held)
                self.frames[:] = [archive]
            return rows
        except (SetupError, OSError) as error:
            self.failure = str(error) if isinstance(error, SetupError) else 'custody_io'
            raise
        finally:
            os.close(fd)

    def close(self):
        for fd in self.frames:
            os.close(fd)
        self.frames.clear()


# origin: S7 internal review rv-drill F3. P28 Send is the sole wire owner.
def decode_ofd(metadata):
    require(fcntl.fcntl(metadata, fcntl.F_GET_SEALS) & SEALS == SEALS, 'ofd_unsealed')
    size = os.fstat(metadata).st_size
    require(0 < size <= 4096, 'ofd_metadata_size')
    row = json.loads(os.pread(metadata, size, 0), object_pairs_hook=unique_object)
    require(set(row) == {'schema', 'process_start', 'boot_id', 'pid', 'resource',
                        'witness_digest', 'owned_fds', 'peak_fds'}, 'ofd_metadata_fields')
    require(row['schema'] == 1 and row['owned_fds'] == 1 and row['peak_fds'] == 2 and
            all(type(row[k]) is int and row[k] > 0 for k in ('pid', 'process_start')) and
            isinstance(row['boot_id'], str) and row['boot_id'], 'ofd_metadata_identity')
    ref = row['resource']
    require(isinstance(ref, dict) and set(ref) == {'resource_id', 'kind', 'name'} and
            all(isinstance(v, str) and v and len(v) <= 256 for v in ref.values()), 'ofd_resource')
    digest = row['witness_digest']
    require(isinstance(digest, str) and len(digest) == 64 and
            all(c in '0123456789abcdef' for c in digest), 'ofd_digest')
    require(ref['kind'] == 'xfer-' + ref['name'] + '-v1' and ref['name'] in ('source', 'tmp', 'parent'), 'ofd_resource_kind')
    return row


# origin: S7 internal review rv-drill F1/F8. The native line is shared by the
# fixture CLI, archived control receipt and parent reader; JSON is only detail.
def native_verdict(result):
    code = result['native_rc']
    names = ('GREEN', 'ASSERT-FAIL', 'SETUP-RED', 'PRODUCT-RED', 'INCOMPLETE')
    require(type(code) is int and 0 <= code < len(names), 'native_verdict_code')
    return ('DRILL-VERDICT verdict=%s rc=%d assert_fail=%d setup_red=%d product_red=%d '
            'not_covered=%d nc_gap=0 nc_guard=%d pass=1 -- transfer-fixture' %
            (names[code], code, int(code == 1), int(code == 2), int(code == 3), int(code == 4), int(code == 4)))


# origin: S7 internal review rv-drill F1/F6. Reuse the production S6 attempt
# sink. Its descriptor is opened only in the already supervised exec bootstrap.
def exec_control(attempts, logical_id, home, argv):
    fd = os.open(attempts, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.set_inheritable(fd, True)
    env = dict(os.environ, HOME=home, TETHER_ATTEMPT_RECEIPT_FD=str(fd), TETHER_ATTEMPT_LOGICAL_ID=logical_id)
    os.execve(argv[0], argv, env)


class ControlOwner:
    """Absolute 100ms slots, real native outcomes, independently read effects."""
    def __init__(self, fixture):
        import threading
        self.fixture, self.stop_event, self.thread = fixture, threading.Event(), None
        self.active, self.attempts, self.effects, self.finished = [], [], {}, []
        self.control = fixture.manifest['control']

    def now(self):
        import time
        return time.clock_gettime_ns(time.CLOCK_BOOTTIME)

    def start(self):
        import threading
        from pathlib import Path
        spec = self.control['launch']
        require(spec['argv'] and Path(spec['argv'][0]).is_absolute() and Path(spec['home']).is_absolute() and
                Path(spec['home']) != Path.home(), 'control_launch_isolation')
        require(any('{effect}' in arg for arg in spec['argv']) and any('{logical_id}' in arg for arg in spec['argv']),
                'control_effect_command')
        self.control.update(baseline_ns=self.now(), logical_ids=[])
        self.thread = threading.Thread(target=self.run, name='transfer-control-slots')
        self.thread.start()

    def launch(self, index):
        import subprocess
        import sys
        from pathlib import Path
        root = self.fixture.keeper.evidence
        logical = 'control-' + str(index)
        self.control['logical_ids'].append(logical)
        spec = self.control['launch']
        effect, attempts = root / (logical + '.effect'), root / (logical + '.attempts')
        argv = [arg.replace('{logical_id}', logical).replace('{effect}', str(effect)) for arg in spec['argv']]
        code = ('import sys; sys.path.insert(0,sys.argv[1]); from transfer_observation import exec_control; '
                'exec_control(sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5:])')
        supervisor = os.environ.get('TRANSFER_PROCESS_SUPERVISOR') or os.environ.get('SIM_UPGRADE_SUPERVISOR')
        require(supervisor and Path(supervisor).is_absolute(), 'control_supervisor')
        log = (root / (logical + '.out')).open('wb')
        try:
            child = subprocess.Popen([supervisor, '--', sys.executable, '-I', '-B', '-c', code,
                str(Path(__file__).parent), str(attempts), logical, spec['home'], *argv], stdout=log, stderr=log)
        finally:
            log.close()
        self.active.append(dict(child=child, logical=logical, attempts=attempts, effect=effect,
                                scheduled=self.control['baseline_ns'] + index * 100_000_000))

    def reap(self, entry, expired=False):
        import subprocess
        child, logical = entry['child'], entry['logical']
        if expired:
            child.terminate()
        try:
            native = child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
            raise SetupError('control_join') from None
        finished = self.now()
        # The sink's final census detects missing attempts, including a lost
        # final success. A missing field is damaged evidence, never zero cost.
        raw = entry['attempts'].read_bytes()
        require(len(raw) <= 65536 and raw.endswith(b'\n'), 'control_attempt_tail')
        rows = [json.loads(line, object_pairs_hook=unique_object) for line in raw.splitlines()]
        closings = [r for r in rows if r.get('kind') == 'observer_close']
        attempts = [r for r in rows if 'kind' not in r]
        require(len(closings) == 1 and closings[0]['logical_id'] == logical and
                closings[0]['attempt_count'] == len(attempts) and closings[0]['overflow'] is False, 'control_attempt_census')
        for attempt in attempts:
            require(attempt['logical_id'] == logical, 'control_attempt_identity')
            self.attempts.append(dict(kind='attempt', attempt=attempt, native_outcome=124 if expired else native))
        self.finished.append(entry)
        effects = entry['effect'].read_text().splitlines() if entry['effect'].exists() else []
        self.effects[logical] = dict(native_rc=124 if expired else native, effect_count=effects.count(logical),
                                    observed=effects == [logical], finished_ns=finished)

    def run(self):
        try:
            index = 0
            while not self.stop_event.is_set() or self.active or self.control['baseline_ns'] + index * 100_000_000 < self.control['drain_ns']:
                now = self.now()
                until = self.control['drain_ns'] - 1 if self.stop_event.is_set() else now
                if self.control['baseline_ns'] + index * 100_000_000 <= until:
                    while self.control['baseline_ns'] + index * 100_000_000 <= until:
                        require(len(self.active) < 64, 'control_child_capacity')
                        self.launch(index)
                        index += 1
                for entry in list(self.active):
                    expired = now - entry['scheduled'] >= 5_000_000_000
                    if entry['child'].poll() is not None or expired:
                        self.active.remove(entry)
                        self.reap(entry, expired)
                self.stop_event.wait(.005) if not self.stop_event.is_set() else __import__('time').sleep(.005)
        except (SetupError, OSError, ValueError, KeyError, TypeError):
            self.fixture.keeper.poison('control_collection')
        finally:
            for entry in self.active:
                entry['child'].terminate()
                try:
                    entry['child'].wait(timeout=5)
                except __import__('subprocess').TimeoutExpired:
                    entry['child'].kill()
                    entry['child'].wait()
            self.active.clear()

    def close(self):
        if not self.thread:
            return
        if not self.stop_event.is_set():
            self.control['drain_ns'] = self.now()
            self.stop_event.set()
        self.thread.join(timeout=10)
        require(not self.thread.is_alive(), 'control_slot_join')
        for entry in self.finished:
            lines = entry['effect'].read_text().splitlines() if entry['effect'].exists() else []
            self.effects[entry['logical']].update(effect_count=lines.count(entry['logical']), observed=lines == [entry['logical']])
        self.fixture.keeper.save('control-clock.json', self.control)
        self.fixture.keeper.save('control-attempts.json', self.attempts)
        self.fixture.keeper.save('control-effects.json', self.effects)
