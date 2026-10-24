#!/usr/bin/env python3
"""Bounded private custody intake, fresh drains, and collector Close/Join.

Product batches and OFD witnesses stay in sealed memory. Public receipts contain
only opaque references and digests; closing observation never stops a product.
The permanent product writer is deliberately outside this collector's lifetime.
"""
import array
import fcntl
import hashlib
import json
import os
from pathlib import Path
import select
import socket
import stat
import struct
import sys
import threading
import time

# Isolated runner mode removes the sibling directory from the import path.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import flow_observation as flow
import transfer_observation as transfer

MAX_BYTES = 256 << 20
MAX_FDS = 512
MAX_BATCH = 1 << 20


class SetupError(ValueError):
    """Only fixed criteria escape private custody, never packet contents."""


class PendingObservation(RuntimeError):
    pass


def require(ok, criterion):
    if not ok:
        raise SetupError(criterion)


def boottime_ns():
    return time.clock_gettime_ns(time.CLOCK_BOOTTIME)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def decode_batch(fd):
    """Compose upstream decoders without widening either closed vocabulary.

    S7 validates its typed events in the original envelope. S5/P06 sees a sealed
    projection of the other events, while the archive retains the original bytes.
    The projection owns one temporary FD, included in the admission peak.
    """
    identity, incarnation, sequence, transfers, raw = transfer.decode(fd)
    require(identity != bytes(32) and incarnation != bytes(16), 'custody_identity')
    offset, slices = 76, []
    count, = struct.unpack_from('<I', raw, 72)
    for _ in range(count):
        start = offset
        size = raw[offset]
        kind = raw[offset + 1:offset + 1 + size].decode('ascii')
        offset += 1 + size
        size = raw[offset]
        domain = raw[offset + 1:offset + 1 + size].decode('ascii')
        offset += 1 + size
        length, = struct.unpack_from('<I', raw, offset + 228)
        offset += 232 + length
        if kind == 'transfer-v1':
            require(domain == 'transfer', 'custody_kind')
        else:
            if kind == 'runtime-sample' and kind not in flow.KINDS:
                raise PendingObservation('NOT_RUN pending P06 runtime-sample decoder')
            require(kind in flow.KINDS, 'custody_kind')
            slices.append(raw[start:offset])
    projected = raw[:72] + struct.pack('<I', len(slices)) + b''.join(slices)
    copy = flow.sealed(projected)
    try:
        _, _, _, events = flow.decode(copy, retain_events=True)
    finally:
        os.close(copy)
    return identity, incarnation, sequence, events, transfers, raw


def consume_runtime(row, expected, barrier):
    """Validate the P06 decoded event as a consumer, retaining explicit unknowns.

    No export/freeze or private-map access can fill missing counters. Actual
    same-stamp owner evidence is supplied separately by the P07/P08/P25 assembly.
    """
    require(row['kind'] == 'runtime-sample' and row['domain'] == 'agent', 'custody_runtime_kind')
    values, data = row['values'], row['data']
    require(len(values) == 16 and values[0] == 1 and len(data) >= 2, 'custody_runtime_schema')
    unknown, = struct.unpack_from('<H', data)
    require(unknown & 0x803f == 0 and all(values[i] > 0 for i in (1, 2, 3, 4, 15)) and
            row['digest'] != bytes(32), 'custody_runtime_identity_unknown')
    offset, strings = 2, []
    for _ in range(5):
        require(offset + 2 <= len(data), 'custody_runtime_truncated')
        size, = struct.unpack_from('<H', data, offset)
        offset += 2
        require(size > 0 and offset + size <= len(data), 'custody_runtime_truncated')
        try:
            strings.append(data[offset:offset + size].decode('utf-8', errors='strict'))
        except UnicodeError:
            raise SetupError('custody_runtime_utf8') from None
        offset += size
    require(offset == len(data), 'custody_runtime_trailing')
    process = expected['process']
    require((values[1], values[2], values[3], *strings) ==
            (process['pid'], process['start_ticks'], expected['generation'], process['boot_id'],
             process['instance_id'], process['pidns'], expected['cut_id'], expected['scope_digest']),
            'custody_runtime_stamp')
    require(row['nonce'].hex() == expected['query_nonce'] == barrier['query_nonce'] and
            barrier['started_ns'] <= row['at'] <= barrier['finished_ns'], 'custody_runtime_freshness')
    fields = ('replay_reserved', 'active_exec', 'active_pty', 'retained_records', 'courier_pending',
              'consumer_count', 'temp_count', 'upgrade_lock_count', 'old_owner_count')
    return dict(source_kind='runtime-sample', stamp=expected, exe_sha=row['digest'].hex(),
                goroutines=values[4], fds=values[5], sample_sequence=values[15], unknown=unknown,
                counters={name: None if unknown & (1 << i) else values[i]
                          for i, name in enumerate(fields, 6)}, complete=False,
                pending='P07/P08 same-stamp resource owners')


class Collector:
    """Sole seqpacket readers; every received descriptor has one close owner.

    Sources map arbitrary roster IDs to (connected endpoint, startup identity).
    Endpoint ownership transfers only after validation succeeds. No processes
    are spawned here: P05 owns launchers, P11 supplies the actual custody pair.
    """
    # origin: hot-update S10 plan §5.3 / §10.2 P16
    def __init__(self, sources, *, byte_limit=MAX_BYTES, fd_limit=MAX_FDS, logs=None):
        require(sources and type(byte_limit) is int and 76 <= byte_limit <= MAX_BYTES and
                type(fd_limit) is int and 4 <= fd_limit <= MAX_FDS, 'custody_limits')
        endpoints, cookies = [], []
        for name, (channel, identity) in sources.items():
            require(isinstance(name, str) and name and len(name) <= 128 and
                    isinstance(identity, bytes) and len(identity) == 32 and identity != bytes(32) and
                    channel.family == socket.AF_UNIX and
                    channel.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) == socket.SOCK_SEQPACKET,
                    'custody_source')
            channel.getpeername()
            endpoints.append(channel.fileno())
            # SO_COOKIE (Linux 57) is an OFD witness for this socket. Distinct
            # descriptor numbers alone would let two workers steal one stream.
            # origin: hot-update S10 plan §4.2 / §10.2 P16
            cookie, = struct.unpack('<Q', channel.getsockopt(socket.SOL_SOCKET, 57, 8))
            require(cookie > 0, 'custody_socket_identity')
            cookies.append(cookie)
        require(len(endpoints) == len(set(endpoints)) and len(endpoints) + 3 <= fd_limit,
                'custody_descriptor_capacity')
        require(len(cookies) == len(set(cookies)), 'custody_endpoint_shared')
        require(logs is None or (logs.cursor is not None and not logs.closed), 'custody_logs_not_ready')
        self.sources = dict(sources)
        self.byte_limit, self.fd_limit = byte_limit, fd_limit
        self.condition, self.operation = threading.Condition(), threading.Lock()
        self.frames, self.pending, self.archives, self.witnesses = [], [], [], []
        self.bytes, self.pending_bytes, self.failure, self.pending_dependency = 0, 0, None, None
        self.sequences, self.incarnations, self.event_sequences = {}, {}, {}
        self.requests, self.drains, self.used_nonces, self.runtime_rows = {}, [], set(), {}
        self.cursors, self.borrowed = {}, set()
        self.logs, self.logs_lock, self.logs_end, self.logs_paused = logs, threading.Lock(), None, False
        self.closed, self.joined, self.threads, self.sealed = False, False, [], False
        try:
            for name, (channel, _) in self.sources.items():
                os.set_inheritable(channel.fileno(), False)
                thread = threading.Thread(target=self._intake, args=(name, channel),
                                          name='fullstack-custody-' + digest(name.encode())[:12])
                thread.start()
                self.threads.append(thread)
        except BaseException:
            self.close()
            self.join(2)
            raise

    def _poison(self, criterion):
        # Preserve the FIRST cause across every subsequent drain or cleanup.
        # origin: hot-update S10 plan §5.3 / FullstackCustodyCollectorTest
        if self.failure is None:
            self.failure = criterion
        self.condition.notify_all()

    def _healthy(self):
        if self.pending_dependency:
            raise PendingObservation(self.pending_dependency)
        require(self.failure is None, self.failure or 'custody_broken')

    def _compact(self):
        if not self.pending:
            return
        raw = b''.join(os.pread(self.frames[i]['fd'], self.frames[i]['bytes'], 0)
                       for i in self.pending)
        require(len(raw) == self.pending_bytes, 'custody_archive_truncated')
        # Do not retire old descriptors until the replacement is sealed.
        require(self._fd_count() + 1 <= self.fd_limit, 'custody_descriptor_capacity')
        archived = flow.sealed(raw)
        self.archives.append(archived)
        offset = 0
        for index in self.pending:
            frame = self.frames[index]
            os.close(frame['fd'])
            frame['fd'], frame['offset'] = archived, offset
            offset += frame['bytes']
        self.pending, self.pending_bytes = [], 0

    def _fd_count(self):
        return (len(self.sources) + len(self.archives) + len(self.pending) +
                2 * len(self.witnesses) + len(self.borrowed))

    def _add(self, fd, source):
        owned = True
        try:
            identity, incarnation, sequence, events, transfers, raw = decode_batch(fd)
            require(identity == self.sources[source][1], 'custody_identity_changed')
            key = source, incarnation
            require(sequence == self.sequences.get(key, 0) + 1, 'custody_sequence_gap')
            prior = self.incarnations.get(source)
            require(prior in (None, incarnation) or key not in self.sequences,
                    'custody_replayed_incarnation')
            # Hitting a ceiling stops certification; never evict old evidence.
            # origin: hot-update S10 plan §5.3 / §10.2 P16
            require(self.bytes + len(raw) <= self.byte_limit, 'custody_capacity')
            next_sequences = dict(self.event_sequences)
            for row in transfers:
                actor = source, row['pid'], row['process_start'], row['boot_id'], row['generation']
                require(row['event_seq'] == next_sequences.get(actor, 0) + 1,
                        'observation_sequence_gap')
                next_sequences[actor] = row['event_seq']
            if self.pending and (self.pending_bytes + len(raw) > MAX_BATCH or len(self.pending) >= 64):
                self._compact()
            require(self._fd_count() + 3 <= self.fd_limit, 'custody_descriptor_capacity')
            self.frames.append(dict(fd=fd, offset=0, bytes=len(raw), sha=digest(raw),
                                    source=source, sequence=sequence, incarnation=incarnation))
            self.pending.append(len(self.frames) - 1)
            self.pending_bytes += len(raw)
            owned = False
            self.bytes += len(raw)
            self.sequences[key], self.incarnations[source] = sequence, incarnation
            self.event_sequences = next_sequences
            for row in events:
                if row['kind'] == 'runtime-sample':
                    self.runtime_rows[source] = dict(row, batch_sequence=sequence, incarnation=incarnation)
            self.condition.notify_all()
        finally:
            if owned:
                os.close(fd)

    def _ofd(self, fds):
        require(len(fds) == 2, 'custody_ofd_packet')
        raw = flow.read_sealed(fds[0])
        require(len(raw) <= 4096, 'custody_ofd_size')
        row = json.loads(raw, object_pairs_hook=transfer.unique_object)
        require(set(row) == {'schema', 'process_start', 'boot_id', 'pid', 'resource',
                            'witness_digest', 'owned_fds', 'peak_fds'}, 'custody_ofd_schema')
        require(type(row['schema']) is int and row['schema'] == 1 and type(row['pid']) is int and row['pid'] > 0 and
                type(row['process_start']) is int and row['process_start'] > 0 and
                isinstance(row['boot_id'], str) and row['boot_id'] and
                row['owned_fds'] == 1 and row['peak_fds'] == 2 and
                isinstance(row['witness_digest'], str) and len(row['witness_digest']) == 64 and
                isinstance(row['resource'], dict) and set(row['resource']) == {'resource_id', 'kind', 'name'},
                'custody_ofd_identity')
        require(all(isinstance(v, str) and 0 < len(v) <= 256 for v in row['resource'].values()) and
                (row['resource']['name'], row['resource']['kind']) in
                {('tmp', 'xfer-tmp-v1'), ('parent', 'xfer-parent-v1'), ('source', 'xfer-source-v1')} and
                len(bytes.fromhex(row['witness_digest'])) == 32, 'custody_ofd_resource')
        mode = os.fstat(fds[1]).st_mode
        require(stat.S_ISREG(mode) or stat.S_ISDIR(mode), 'custody_ofd_kind')
        require(self.bytes + len(raw) <= self.byte_limit and self._fd_count() + 4 <= self.fd_limit,
                'custody_descriptor_capacity')
        self.witnesses.append(tuple(fds))
        self.bytes += len(raw)
        fds.clear()
        self.condition.notify_all()

    def _poll_logs(self):
        with self.logs_lock:
            if self.logs is not None and not self.closed and not self.logs_paused:
                self.logs.poll()

    def _ack(self, source, raw):
        require(len(raw) == 36 and raw[:4] == b'TDA2', 'custody_reply_invalid')
        request = self.requests.get(source)
        require(request is not None and raw[4:20] == request['challenge'] and not request['done'],
                'custody_fresh_ack_replayed')
        sequence, poison = struct.unpack_from('<QQ', raw, 20)
        incarnation = self.incarnations.get(source)
        require(poison == 0, 'custody_producer_poison')
        require(incarnation is not None and sequence == self.sequences[source, incarnation] and
                sequence > request['floor'], 'custody_fresh_ack_missing_batch')
        request.update(done=True, sequence=sequence, incarnation=incarnation, finished_ns=boottime_ns())
        self.condition.notify_all()

    def _intake(self, source, channel):
        while True:
            fds = []
            try:
                # One existing intake worker polls P09; no second scheduler or
                # competing log reader is introduced for each required source.
                if source == next(iter(self.sources)):
                    self._poll_logs()
                if not select.select([channel], [], [], .05)[0]:
                    with self.condition:
                        if self.closed:
                            return
                    continue
                raw, fds = transfer.receive(channel, capacity=16)
                with self.condition:
                    if not raw:
                        require(self.closed, 'custody_channel_incomplete')
                        return
                    if self.closed:
                        require(not fds, 'custody_close_pending_batch')
                        return
                    self._healthy()
                    if raw == b'TDC1' and len(fds) == 1:
                        self._add(fds.pop(), source)
                    elif raw == b'TDO1':
                        self._ofd(fds)
                    else:
                        require(not fds, 'custody_reply_invalid')
                        self._ack(source, raw)
            except PendingObservation as error:
                with self.condition:
                    self.pending_dependency = str(error)
                    self.condition.notify_all()
                return
            except Exception as error:
                with self.condition:
                    if not self.closed or isinstance(error, (SetupError, transfer.SetupError, flow.CustodyError)):
                        criterion = str(error) if isinstance(error, (SetupError, transfer.SetupError,
                                                                    flow.CustodyError)) else 'custody_io_failure'
                        self._poison(criterion)
                return
            finally:
                for fd in fds:
                    os.close(fd)

    # origin: hot-update S10 plan §5.3 / §10.2 P16 / fresh ACK replay
    def drain(self, source, nonce, timeout=2):
        with self.operation, self.condition:
            self._healthy()
            require(not self.closed and source in self.sources and isinstance(nonce, str) and
                    nonce and nonce not in self.used_nonces and timeout > 0, 'custody_drain_request')
            challenge = os.urandom(16)
            incarnation = self.incarnations.get(source)
            floor = self.sequences.get((source, incarnation), 0)
            request = dict(challenge=challenge, floor=floor, done=False, started_ns=boottime_ns())
            self.requests[source] = request
            self.used_nonces.add(nonce)
            try:
                self.sources[source][0].sendall(b'TDP2' + challenge)
                ready = self.condition.wait_for(lambda: request['done'] or self.failure or
                                               self.pending_dependency or self.closed, timeout)
                self._healthy()
                require(ready and request['done'], 'custody_barrier_timeout')
                self._compact()
                cursor = self.cursors.get(source, 0)
                receipt = dict(schema='fullstack-custody-drain-v1', source=digest(source.encode()),
                               query_nonce=nonce, sequence=request['sequence'], cursor=len(self.frames),
                               incarnation=digest(request['incarnation']), started_ns=request['started_ns'],
                               finished_ns=request['finished_ns'], bytes=self.bytes,
                               frames=[dict(id='custody/' + f['sha'], sha=f['sha'], bytes=f['bytes'], cursor=i)
                                       for i, f in enumerate(self.frames[cursor:], cursor) if f['source'] == source])
                self.cursors[source] = len(self.frames)
                self.drains.append(receipt)
                return receipt
            except PendingObservation:
                raise
            except Exception as error:
                self._poison(str(error) if isinstance(error, SetupError) else 'custody_barrier_transport')
                self._healthy()

    def seal(self, nonce, timeout=2):
        # Owner retirement/history sealing belongs to P14. These are two FRESH
        # observation barriers, never a substitute for those product receipts.
        first = [self.drain(source, nonce + '/first/' + digest(source.encode()), timeout) for source in self.sources]
        second = [self.drain(source, nonce + '/second/' + digest(source.encode()), timeout) for source in self.sources]
        with self.logs_lock:
            if self.logs is not None:
                self.logs_paused = True
                try:
                    self.logs_end = self.logs.drain(self.logs.cursor, self.logs.archive_root / 'end')
                except Exception:
                    with self.condition:
                        self._poison('custody_log_drain_failure')
                    raise SetupError('custody_log_drain_failure') from None
        with self.condition:
            self._healthy()
            self.sealed = True
        return dict(schema='fullstack-custody-seal-v1', first=first, second=second, logs=self.logs_end)

    def borrow_frame(self, cursor):
        with self.condition:
            self._healthy()
            require(not self.joined and type(cursor) is int and 0 <= cursor < len(self.frames),
                    'custody_cursor')
            require(self._fd_count() + 1 <= self.fd_limit, 'custody_descriptor_capacity')
            frame = self.frames[cursor]
            raw = os.pread(frame['fd'], frame['bytes'], frame['offset'])
            require(len(raw) == frame['bytes'] and digest(raw) == frame['sha'], 'custody_archive_truncated')
            fd = flow.sealed(raw)
            self.borrowed.add(fd)
            return fd  # Release through this collector, keeping the FD peak exact.

    def release_frame(self, fd):
        with self.condition:
            require(fd in self.borrowed, 'custody_borrow_owner')
            self.borrowed.remove(fd)
            os.close(fd)

    def runtime_sample(self, source, nonce, expected=None):
        with self.condition:
            self._healthy()
            row = self.runtime_rows.get(source)
            if row is None:
                raise PendingObservation('NOT_RUN pending P06/P07 runtime-sample producer')
            try:
                require(row['nonce'].hex() == nonce, 'custody_runtime_nonce')
                barrier = next((r for r in reversed(self.drains) if r['query_nonce'] == nonce and
                                r['source'] == digest(source.encode())), None)
                require(barrier and expected is not None and row['batch_sequence'] <= barrier['sequence'] and
                        digest(row['incarnation']) == barrier['incarnation'], 'custody_runtime_barrier')
                return consume_runtime(row, expected, barrier)
            except Exception:
                self._poison('custody_runtime_invalid')
                raise SetupError('custody_runtime_invalid') from None

    def close(self):
        with self.condition:
            if self.closed:
                return
            self.closed = True
            self.condition.notify_all()
            for channel, _ in self.sources.values():
                try:
                    channel.shutdown(socket.SHUT_RDWR)
                except OSError:
                    self._poison('custody_close_failure')
        with self.logs_lock:
            if self.logs is not None and not self.logs.closed:
                try:
                    self.logs.close()
                except Exception:
                    with self.condition:
                        self._poison('custody_log_close_failure')

    # origin: hot-update S10 plan §4.2 / §10.2 P16 / Flush is not Join
    def join(self, timeout=2):
        require(self.closed and timeout > 0, 'custody_close_required')
        deadline = time.monotonic() + timeout
        for thread in self.threads:
            thread.join(max(0, deadline - time.monotonic()))
        with self.condition:
            if any(thread.is_alive() for thread in self.threads):
                self._poison('custody_join_timeout')
                raise SetupError('custody_join_timeout')
            if not self.joined:
                if self.borrowed:
                    self._poison('custody_borrow_unreleased')
                for fd in set(self.archives + [self.frames[i]['fd'] for i in self.pending] +
                              [fd for pair in self.witnesses for fd in pair] + list(self.borrowed)):
                    try:
                        os.close(fd)
                    except OSError:
                        self._poison('custody_close_failure')
                for channel, _ in self.sources.values():
                    channel.close()
                self.joined = True
                self.archives, self.pending, self.witnesses = [], [], []
                self.borrowed.clear()
                if self.logs is not None:
                    try:
                        self.logs.join()
                    except Exception:
                        self._poison('custody_log_join_failure')
            self._healthy()

    def lifecycle(self):
        with self.condition:
            complete = self.closed and self.joined and self.sealed and all(not t.is_alive() for t in self.threads)
            complete = complete and (self.logs is None or self.logs_end is not None)
            return dict(schema='fullstack-custody-lifecycle-v1', closed=self.closed, joined=self.joined,
                        complete=complete and self.failure is None and self.pending_dependency is None,
                        rc=2 if self.failure else (4 if self.pending_dependency or not complete else 0),
                        criterion=self.failure, pending=self.pending_dependency)


def same_current_stamp(before, after):
    # Fresh queries advance observation time/sequence while retaining one image.
    # Comparing whole documents would reject every genuinely fresh current read.
    # origin: hot-update S10 plan §5.3 / FullstackCustodyCollectorTest
    fixed = ('process', 'tx_id', 'cut_id', 'direction', 'query_nonce', 'scope_digest', 'generation', 'revision')
    return (all(before.get(field) == after.get(field) for field in fixed) and
            after.get('sequence', 0) >= before.get('sequence', 0) and
            after.get('at_boot_ns', 0) >= before.get('at_boot_ns', 0))


def broker_runtime(path, expected, query_current, timeout=2):
    """Read one authorized local admin response with kernel peer credentials.

    query_current is P18's real S8 read-only query, not an expected-state factory.
    P04 wraps the resulting source with its independent held executable witness.
    """
    if query_current is None:
        raise PendingObservation('NOT_RUN pending P18 broker current-stamp query')
    nonce = expected['query_nonce']
    before = json.loads(json.dumps(query_current(nonce)))
    require(nonce and same_current_stamp(expected, before), 'broker_current_stamp')
    process = before['process']
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(timeout)
        connection.connect(str(path))
        pid, uid, _ = struct.unpack('3i', connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        require(pid == process['pid'] and uid == process['key']['uid'], 'broker_peer_identity')
        started = boottime_ns()
        connection.sendall(b'{"op":"runtime"}\n')
        raw = bytearray()
        while not raw.endswith(b'\n'):
            block = connection.recv(min(4096, MAX_BATCH + 1 - len(raw)))
            require(block and len(raw) + len(block) <= MAX_BATCH, 'broker_response_incomplete')
            raw.extend(block)
        finished = boottime_ns()
    response = json.loads(raw, object_pairs_hook=transfer.unique_object)
    runtime = response.get('runtime', {})
    image = runtime.get('broker_image')
    require(response.get('error', '') == '' and runtime.get('schema') == 'admin_runtime' and
            runtime.get('schema_version') == 2 and type(runtime.get('goroutines')) is int and
            runtime['goroutines'] > 0 and type(runtime.get('open_fds')) is int and
            runtime['open_fds'] >= 0 and isinstance(image, dict), 'broker_runtime_unknown')
    after = json.loads(json.dumps(query_current(nonce)))
    require(image.get('generation') == before['generation'] and image.get('cut_id') == before['cut_id'] and
            isinstance(image.get('running_sha256'), str) and len(image['running_sha256']) == 64 and
            all(c in '0123456789abcdef' for c in image['running_sha256']) and
            same_current_stamp(before, after), 'broker_current_stamp')
    return dict(source_kind='admin_runtime', before=before, after=after, peer_pid=pid, peer_uid=uid,
                query_nonce=nonce, sent_ns=started, received_ns=finished, response=bytes(raw))
