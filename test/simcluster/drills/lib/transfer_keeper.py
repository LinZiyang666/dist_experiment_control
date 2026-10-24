#!/usr/bin/env python3
"""Process-free keeper: pinned OFDs, continuous custody and target inspection.

The caller starts this keeper through the shared FIX-ORPHAN supervisor. This
module never forks, executes an upgrade, repairs a transfer or kills an owner.
The keeper outlives judge so a late effect still poisons the saved verdict.
"""
import array
import ctypes
import hashlib
import json
import os
from pathlib import Path
import platform
import select
import socket
import struct
import threading
import time

from transfer_observation import Records, SetupError, receive, require, sealed, decode_ofd
from transfer_oracle import TargetOracle, digest_file

IMAGE_DIGESTS = {}


def process_identity(pid):
    require(type(pid) is int and pid > 0, 'process_pid')
    base = Path('/proc') / str(pid)
    stat = (base / 'stat').read_text()
    fields = stat[stat.rfind(')') + 1:].split()
    require(len(fields) >= 20 and fields[0] not in ('Z', 'X'), 'process_dead')
    start = int(fields[19])
    fd = os.open(base / 'exe', os.O_RDONLY | os.O_CLOEXEC)
    try:
        image = os.fstat(fd)
        fingerprint = (image.st_dev, image.st_ino, image.st_size, image.st_mtime_ns, image.st_ctime_ns)
        digest = IMAGE_DIGESTS.get(fingerprint)
        if digest is None:
            _, digest = digest_file(fd)
            if len(IMAGE_DIGESTS) >= 128:
                IMAGE_DIGESTS.clear()
            IMAGE_DIGESTS[fingerprint] = digest
    finally:
        os.close(fd)
    current = (base / 'stat').read_text()
    require(int(current[current.rfind(')') + 1:].split()[19]) == start, 'process_recycled')
    return dict(pid=pid, process_start=start,
                boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(), running_sha=digest)


def compare_ofd(pid, fd, witness):
    # kcmp is the independent kernel oracle; matching stat/offset is insufficient
    # when Import reopened the same inode. EPERM/ENOSYS is SETUP2, never equal.
    # origin: hot-update S7 plan §3 / §10.4 / M09.
    numbers = {'x86_64': 312, 'aarch64': 272}
    require(platform.system() == 'Linux' and platform.machine() in numbers, 'ofd_kernel_unavailable')
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.syscall(ctypes.c_long(numbers[platform.machine()]), ctypes.c_int(os.getpid()),
                          ctypes.c_int(pid), ctypes.c_int(0), ctypes.c_ulong(witness), ctypes.c_ulong(fd))
    require(result >= 0, 'ofd_kernel_unavailable')
    return result == 0


class Keeper:
    def __init__(self, manifest, evidence):
        self.manifest, self.evidence = manifest, Path(evidence)
        self.lock, self.changed = threading.RLock(), threading.Condition(threading.RLock())
        self.records, self.channels, self.identities, self.processes, self.acks = Records(), {}, {}, {}, {}
        self.witnesses, self.stop_event, self.thread = {}, threading.Event(), None
        self.pending_drains, self.pending_bindings = {}, []
        self.targets = {t['transfer_id']: TargetOracle(t['target'], t.get('old'), [t['size'], t['sha256']])
                        for t in manifest['transfers']}
        self.failures, self.setup_failures, self.judged, self.verdict = [], [], False, None
        self.peak_fds, self.last_event_ns = 0, time.monotonic_ns()
        self.evidence.mkdir(mode=0o700, parents=True, exist_ok=True)

    def save(self, name, value):
        # fsync both file and directory so failure evidence survives the driver
        # dying after judge. Only public typed observations reach these files.
        path = self.evidence / name
        temp = path.with_suffix('.pending')
        with temp.open('w') as out:
            json.dump(value, out, sort_keys=True)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temp, path)
        fd = os.open(self.evidence, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def poison(self, code, assertion=False):
        with self.lock:
            rows = self.failures if assertion else self.setup_failures
            if code not in rows:
                rows.append(code)
            if self.judged:
                # origin: hot-update S7 plan §10.4 / T42. Judge is not teardown:
                # later contradictory evidence invalidates its saved GREEN.
                updated = dict(self.verdict or {}, native_rc=1 if self.failures else 2,
                               failures=list(self.failures), setup=list(self.setup_failures))
                self.save('verdict.json', updated)
                self.verdict = updated

    def lease(self, label):
        with self.lock:
            expected = {p['label'] for p in self.manifest['processes']}
            require(label in expected and label not in self.channels, 'custody_label')
            producer, consumer = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
            # The shared launcher receives these as ExtraFiles[0:2], fd3/fd4.
            # v2 opts this invocation into the final native exit/release handshake.
            # Older flow keepers retain v1 and require no new release message.
            # origin: S7 internal review rv-drill F1
            config = sealed(b'TDCFG2\0\0' + struct.pack('<Q', 3) + os.urandom(16))
            self.channels[label], self.acks[label] = consumer, 0
            self.identities[label] = hashlib.sha256(os.pread(config, 32, 0)).digest()
            return producer, config

    def start(self):
        self.thread = threading.Thread(target=self.collect, name='transfer-custody')
        self.thread.start()

    def inspect_targets(self, boundary):
        for transfer, target in self.targets.items():
            row = target.sample(boundary)
            if row['state'] == 'partial':
                self.poison('partial_destination:' + transfer, assertion=True)

    def add_event(self, source, row):
        spec = next(p for p in self.manifest['processes'] if p['label'] == source)
        identity = process_identity(row['pid'])
        require(all(identity[k] == row[k] for k in ('pid', 'process_start', 'boot_id')), 'process_binding')
        prior = self.processes.get(source)
        require(prior is None or all(prior[k] == identity[k] for k in ('pid', 'process_start', 'boot_id')),
                'process_identity_changed')
        require(identity['running_sha'] in spec['artifact_shas'], 'process_artifact')
        self.processes[source] = identity
        transfer = next((t for t in self.manifest['transfers'] if t['transfer_id'] == row['transfer_id']), None)
        if transfer is None:
            transfer = next((t for t in self.manifest['transfers'] if t.get('discover_identity') and
                not t.get('identity_bound') and source in (t.get('ctl_producer'), t.get('agent_producer'))), None)
            if transfer is not None:
                previous = transfer['transfer_id']
                transfer.update(transfer_id=row['transfer_id'], entry_nonce=row['entry_nonce'],
                                object_version=row['object_version'], identity_bound=True)
                if not transfer.get('object_name'):
                    transfer['object_name'] = row['transfer_id']
                self.targets[row['transfer_id']] = self.targets.pop(previous)
                for cut in self.manifest['cuts']:
                    if cut['transfer_id'] == previous:
                        cut['transfer_id'] = row['transfer_id']
                self.save('identity-' + transfer['cohort'] + '.json', dict(transfer_id=row['transfer_id'],
                    entry_nonce=row['entry_nonce'], object_version=row['object_version'], **identity))
                pending, self.pending_bindings = self.pending_bindings, []
                for owner, waiting in pending:
                    self.add_event(owner, waiting)
            elif any(t.get('discover_identity') and not t.get('identity_bound') for t in self.manifest['transfers']):
                require(len(self.pending_bindings) < 4096, 'identity_binding_capacity')
                self.pending_bindings.append((source, row))
                return
        if transfer is not None and transfer.get('discover_identity') and not transfer['object_version'] and row['object_version']:
            transfer['object_version'] = row['object_version']
        require(transfer is not None and transfer['entry_nonce'] == row['entry_nonce'] and
                (transfer['object_version'] == row['object_version'] or not row['object_version'] and
                 row['phase'] == 'prepared'), 'transfer_binding')
        self.last_event_ns = time.monotonic_ns()
        if row['kind'] in ('fetched', 'synced', 'primitive_enter', 'primitive_exit', 'directory_sync'):
            self.inspect_targets(row['kind'])
        if row['kind'] in ('freeze', 'import', 'install', 'rollback'):
            for resource, witness in self.witnesses.items():
                if witness['pid'] != row['pid']:
                    continue
                transfer = next(t for t in self.manifest['transfers'] if resource in t['required_resources'])
                if transfer['transfer_id'] != row['transfer_id']:
                    continue
                schedule = transfer['required_resources']
                if isinstance(schedule, dict) and row['cut_id'] not in schedule[resource]:
                    continue
                require(witness['process_start'] == row['process_start'], 'ofd_process_recycled')
                equal = compare_ofd(row['pid'], witness['product_fd'], witness['fd'])
                witness['checks'].append(dict(cut_id=row['cut_id'], generation=row['generation'],
                                              at_ns=row['boottime_ns'], equal=equal))
                if not equal:
                    self.poison('ofd_reopened:' + resource, assertion=True)
        if row['kind'] == 'primitive_exit' and row.get('outcome') == 'landed':
            successes = [e for e in self.records.events if e['transfer_id'] == row['transfer_id'] and
                         e['kind'] == 'primitive_exit' and e.get('outcome') == 'landed']
            if len(successes) > 1:
                self.poison('duplicate_primitive:' + row['transfer_id'], assertion=True)
        if row['kind'] in ('finish', 'history', 'acknowledge') and row.get('canonical_digest'):
            expected = {hashlib.sha256(__import__('base64').b64decode(transfer[k])).hexdigest()
                        for k in ('start_b64', 'terminal_b64') if k in transfer}
            if len(expected) == 2 and row['canonical_digest'] not in expected:
                self.poison('opposite_terminal:' + row['transfer_id'], assertion=True)

    def collect(self):
        while not self.stop_event.is_set():
            with self.lock:
                channels = dict(self.channels)
            try:
                ready, _, _ = select.select(list(channels.values()), [], [], .02)
                with self.lock:
                    for label, channel in channels.items():
                        # A final sealed ACK can release a channel after this
                        # select snapshot; only the still-owned socket is readable.
                        # origin: S7 internal review rv-drill F1
                        if self.channels.get(label) is not channel or channel not in ready:
                            continue
                        raw, fds = receive(channel)
                        try:
                            if raw == b'TDC1' and len(fds) == 1:
                                rows = self.records.add(fds.pop(), label, self.identities[label])
                                for row in rows:
                                    self.add_event(label, row)
                            elif raw == b'TDO1' and len(fds) == 2:
                                metadata = decode_ofd(fds[0])
                                self.accept_ofd(label, metadata, fds.pop())
                            elif raw.startswith(b'TDA2') and not fds:
                                self.accept_ack(label, raw)
                            else:
                                self.poison('custody_closed' if not raw else 'custody_reply')
                                # A closed socket stays readable forever. Remove it
                                # after poisoning, preserving the missing-owner fact.
                                if not raw:
                                    self.channels.pop(label).close()
                        finally:
                            for fd in fds:
                                os.close(fd)
                    self.inspect_targets('poll')
                    self.peak_fds = max(self.peak_fds, len(os.listdir('/proc/self/fd')))
            except (SetupError, OSError, ValueError, KeyError, TypeError) as error:
                self.poison(str(error) if isinstance(error, SetupError) else 'collector_io')
            with self.changed:
                self.changed.notify_all()

    def drain(self, timeout=5):
        # One request in flight per channel. An unsolicited/reused TDOK poisons
        # custody; the previous successful barrier cannot satisfy this request.
        # origin: S7 internal review rv-drill F4; hot-update S7 plan §10.4 / T42.
        with self.lock:
            require(set(self.channels) == {p['label'] for p in self.manifest['processes']}, 'missing_custody')
            require(not self.setup_failures and not self.records.failure, 'custody_poisoned')
            require(not self.pending_drains, 'drain_inflight')
            requested = {}
            for label, channel in self.channels.items():
                nonce = os.urandom(16)
                incarnation = self.records.incarnations.get(label)
                sequence = self.records.sequences.get((label, incarnation), 0)
                requested[label] = nonce.hex()
                self.pending_drains[label] = (nonce, incarnation, sequence)
                channel.send(b'TDP2' + nonce)
        deadline = time.monotonic() + timeout
        with self.changed:
            while True:
                with self.lock:
                    done = all(self.acks[k] == v for k, v in requested.items())
                    broken = bool(self.setup_failures)
                if done:
                    return dict(acks=requested, cursor=len(self.records.events), at_ns=time.monotonic_ns())
                require(not broken, 'custody_poisoned')
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.poison('drain_ack_missing')
                    raise SetupError('drain_ack_missing')
                self.changed.wait(remaining)

    # origin: S7 internal review rv-drill F4. ACK authenticates a newly consumed
    # sealed watermark, not merely receipt of a packet on a live socket.
    def accept_ack(self, label, raw):
        require(label in self.pending_drains and len(raw) == 36, 'drain_ack_unrequested')
        nonce, incarnation, previous = self.pending_drains[label]
        sequence, poison = struct.unpack_from('<QQ', raw, 20)
        current = self.records.incarnations.get(label)
        require(raw[4:20] == nonce and poison == 0 and
                (incarnation is None or current == incarnation) and current is not None and sequence > previous and
                sequence == self.records.sequences.get((label, current), 0), 'drain_ack_stale')
        self.acks[label] = nonce.hex()
        del self.pending_drains[label]

    # origin: S7 internal review rv-drill F3. Metadata and duplicate travel in
    # one P28 packet. Kernel equality, never inode/offset, locates the owner FD.
    def accept_ofd(self, label, metadata, fd):
        try:
            identity = process_identity(metadata['pid'])
            require(all(identity[k] == metadata[k] for k in ('pid', 'process_start', 'boot_id')),
                    'ofd_process_binding')
            spec = next(p for p in self.manifest['processes'] if p['label'] == label)
            require(identity['running_sha'] in spec['artifact_shas'], 'ofd_artifact')
            resource = metadata['resource']['resource_id']
            require(any(resource in t['required_resources'] for t in self.manifest['transfers']),
                    'ofd_foreign_resource')
            require(resource not in self.witnesses, 'ofd_duplicate_resource')
            candidates = []
            for name in (Path('/proc') / str(identity['pid']) / 'fd').iterdir():
                number = int(name.name)
                if identity['pid'] == os.getpid() and number == fd:
                    continue
                try:
                    if compare_ofd(identity['pid'], number, fd):
                        candidates.append(number)
                except FileNotFoundError:
                    continue
            require(candidates, 'ofd_mismatch')
            stat = os.fstat(fd)
            self.witnesses[resource] = dict(fd=fd, pid=identity['pid'], process_start=identity['process_start'],
                boot_id=identity['boot_id'], product_fd=min(candidates), device=stat.st_dev, inode=stat.st_ino,
                witness_digest=metadata['witness_digest'], resource=metadata['resource'], checks=[])
        except BaseException:
            os.close(fd)
            raise

    def hold_ofd(self, resource, fd, pid, process_start, product_fd):
        with self.lock:
            try:
                require(resource not in self.witnesses, 'ofd_duplicate_resource')
                require(any(resource in t['required_resources'] for t in self.manifest['transfers']),
                        'ofd_foreign_resource')
                identity = process_identity(pid)
                require(identity['process_start'] == process_start, 'ofd_process_recycled')
                require(compare_ofd(pid, product_fd, fd), 'ofd_mismatch')
                stat = os.fstat(fd)
                self.witnesses[resource] = dict(fd=fd, pid=pid, process_start=process_start,
                                                product_fd=product_fd, device=stat.st_dev, inode=stat.st_ino, checks=[])
                return dict(resource=resource, device=stat.st_dev, inode=stat.st_ino, equal=True)
            except BaseException:
                os.close(fd)
                raise

    # origin: S7 internal review rv-drill F1. The fault driver receives only
    # TCP tuples backed by a socket FD in the independently identified owner.
    def connections(self):
        result = {'carrier': [], 'data': []}
        for spec in self.manifest['processes']:
            identity = self.processes.get(spec['label'])
            if identity is None or not spec.get('connection_ports'):
                continue
            before = process_identity(identity['pid'])
            require(all(before[k] == identity[k] for k in ('pid', 'process_start', 'boot_id')), 'connection_process')
            base = Path('/proc') / str(identity['pid'])
            inodes = set()
            for fd in (base / 'fd').iterdir():
                try:
                    name = os.readlink(fd)
                except FileNotFoundError:
                    continue
                if name.startswith('socket:['):
                    inodes.add(name[8:-1])
            for version, family in (('tcp', socket.AF_INET), ('tcp6', socket.AF_INET6)):
                for line in (base / 'net' / version).read_text().splitlines()[1:]:
                    fields = line.split()
                    if fields[9] not in inodes or fields[3] != '01':
                        continue
                    address, port = fields[2].split(':')
                    data = bytes.fromhex(address)
                    data = b''.join(data[i:i+4][::-1] for i in range(0,len(data),4))
                    peer, port = socket.inet_ntop(family,data), int(port,16)
                    for kind, ports in spec['connection_ports'].items():
                        require(kind in result, 'connection_kind')
                        if port in ports:
                            result[kind].append(dict(node=spec['node'],peer=peer,port=port,producer=spec['label'],
                                                     **identity))
        return result

    def sample(self):
        with self.lock:
            self.inspect_targets('sample')
            return dict(events=list(self.records.events), processes=dict(self.processes), connections=self.connections(),
                        targets={key: list(t.samples) for key, t in self.targets.items()},
                        failures=list(self.failures), setup=list(self.setup_failures),
                        custody=dict(bytes=self.records.bytes, frames=len(self.records.frames)),
                        resources=dict(channels=len(self.channels), witnesses=len(self.witnesses),
                                       peak_fds=self.peak_fds,
                                       ofd_checks={key: {k: v for k, v in row.items() if k != 'fd'}
                                                   for key, row in self.witnesses.items()}), verdict=self.verdict)

    def close(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)
            require(not self.thread.is_alive(), 'collector_join')
        with self.lock:
            for channel in self.channels.values():
                channel.close()
            self.channels.clear()
            for witness in self.witnesses.values():
                os.close(witness['fd'])
            self.witnesses.clear()
            self.records.close()
