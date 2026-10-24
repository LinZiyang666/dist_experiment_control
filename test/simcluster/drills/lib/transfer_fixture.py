#!/usr/bin/env python3
"""Live transfer fixture: start|sample|drain|judge|stop --manifest --evidence.

start runs the keeper in the caller's supervised process, emits one startup
receipt and serves its private control socket until stop. Original commands
and control slots launch under the shared supervisor. Their exec bootstrap
leases custody via SCM_RIGHTS as fd3/fd4. The driver owns cuts; this fixture
reads product effects and preserves native outcomes without repairing work.
"""
import argparse
import array
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import struct
import sys
import threading
import time

# The driver launches this file as `python3 -I -B .../transfer_fixture.py`;
# isolated mode drops the script directory from sys.path, so without this the
# sibling imports below fail before the first verb and every 63/64 arm is a
# permanent SETUP-RED. The hermetic selftest imports with its own path entry
# and could never see that. Same pattern as flow_open_fault.py.
# origin: hot-update S7 plan §10.4 / P29; r9d D-11 arm controls.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from transfer_keeper import Keeper
from transfer_observation import SetupError, receive, require, unique_object, native_verdict, ControlOwner
from transfer_oracle import NATSReader, StreamScan, judge_history, judge_object, cut_coverage, judge_control

COHORTS = ('small-push', 'small-pull', 'large-push', 'large-pull')


def read_manifest(path):
    raw = Path(path).read_bytes()
    value = json.loads(raw, object_pairs_hook=unique_object)
    require(value.get('schema') == 1 and value.get('mode') in ('live', 'hermetic'), 'manifest_schema')
    require(value.get('quiet_seconds', 0) >= 2 and value['quiet_seconds'] <= 30, 'quiet_window')
    require(value.get('processes') and value.get('transfers') and isinstance(value.get('cuts'), list), 'manifest_empty')
    labels = [p['label'] for p in value['processes']]
    require(len(labels) == len(set(labels)) and all(isinstance(k, str) and k.isalnum() for k in labels), 'process_labels')
    require(all(p.get('artifact_shas') and all(len(s) == 64 and all(c in '0123456789abcdef' for c in s)
                                             for s in p['artifact_shas']) for p in value['processes']), 'artifact_shas')
    transfers = value['transfers']
    for key in ('transfer_id', 'entry_nonce', 'bucket', 'session_id', 'target'):
        values = [t[key] for t in transfers if t[key] or not t.get('discover_identity')]
        require(len(set(values)) == len(values), 'transfer_isolation')
    for t in transfers:
        require(type(t['size']) is int and t['size'] > 0 and len(t['sha256']) == 64 and (t['object_version'] or t.get('discover_identity')), 'transfer_size')
        require(Path(t['target']).is_absolute() and '/.tether/' not in t['target'] and
                t['target'] != str(Path.home() / '.tether'), 'target_path')
        require(t['history_stream'] and t['object_stream'] and t['history_subject'] and (t['object_name'] or
                value.get('consumer') == 'infra-transfer-v1' and t.get('discover_identity')), 'transfer_streams')
        for key in ('start_b64', 'terminal_b64'):
            if key not in t:
                continue
            record = json.loads(base64.b64decode(t[key], validate=True), object_pairs_hook=unique_object)
            require(record.get('transfer_id') == t['transfer_id'] and record.get('kind') in
                    (('start',) if key == 'start_b64' else ('complete', 'failed')), 'canonical_binding')
        require(t.get('required_resources') is not None, 'resource_manifest')
    # origin: hot-update S9 plan §9.1 / LQ3. Consumer-specific admission must
    # never relax drill 63/64's frozen image matrix or startup pacing.
    require(value.get('consumer') in (None, 'infra-transfer-v1'), 'unknown_transfer_consumer')
    if value.get('consumer') == 'infra-transfer-v1':
        from infra_transfer_fixture import validate_manifest, bind_discovered
        validate_manifest(value)
        bind_discovered(value, hashlib.sha256(raw).hexdigest())
    elif value['mode'] == 'live':
        require({t['cohort'] for t in transfers} == set(COHORTS) and len(transfers) == 4, 'four_cohorts')
        require(value.get('cuts') and value.get('control', {}).get('launch'), 'live_schedule')
        from transfer_fixture_manifest import observer_requirements, resource_requirements
        require(value.get('driver'), 'live_driver_missing')
        expected = observer_requirements(value['driver'], transfers, value['processes'])
        require(sorted(value['cuts'], key=lambda row: json.dumps(row, sort_keys=True)) ==
                sorted(expected, key=lambda row: json.dumps(row, sort_keys=True)), 'live_matrix_drift')
        for transfer in transfers:
            require(transfer['required_resources'] == resource_requirements(transfer, expected), 'live_resource_drift')
        require(len({p['home'] for p in value['processes'] if p.get('role') == 'agent'}) == 4 and
                len({p['binary_path'] for p in value['processes'] if p.get('role') == 'agent'}) == 4,
                'agent_install_isolation')
        for t in transfers:
            small = t['cohort'].startswith('small-')
            require(t['size'] == (66549440 if small else 1 << 30), 'cohort_size')
            normal = 256 << 10 if small else 4 << 20
            watchdog = 1 << 20 if small else 8 << 20
            require(t['rate_limit'] == (watchdog if value.get('arm') in ('W1', 'W2') else normal), 'cohort_rate')
            require(t['required_resources'] and all(t['required_resources'].values()), 'live_ofd_resources')
    value['manifest_sha256'] = hashlib.sha256(raw).hexdigest()
    return value


class Fixture:
    def __init__(self, manifest, evidence):
        # Cell tasks are independent original invocations; their channels exist
        # in the required census before launch, so omitting a cell cannot pass.
        for task in manifest.get('rollback_tasks', []):
            transfer = task['transfer']
            transfer['tier'] = task['tier']
            manifest['transfers'].append(transfer)
            manifest['processes'].append(dict(label=transfer['ctl_producer'], role='ctl', cell=task['cell'],
                launch=transfer['launch'], artifact_shas=manifest['processes'][0]['artifact_shas']))
        self.manifest, self.keeper = manifest, Keeper(manifest, evidence)
        streams = {t[k] for t in manifest['transfers'] for k in ('object_stream', 'history_stream')}
        self.scans = {name: StreamScan(name) for name in streams}
        self.drains, self.scan_receipts, self.scan_failures = [], [], []
        self.sample_sequence = 0
        self.launcher, self.controls = None, None
        self.started_ns = time.monotonic_ns()
        self.scan_lock, self.scan_thread = threading.RLock(), None
        self.io_lock, self.active_reader = threading.Lock(), None
        self.outage = None
        self.fault_interval = None
        # C3 declares a bounded initial availability interval before any IO.
        # Later restart windows are sealed by the driver before its command.
        if manifest.get('arm') == 'C3':
            self.fault_interval = (time.monotonic_ns(), time.monotonic_ns() + 5_000_000_000)

    def scan(self):
        with self.scan_lock:
            return self.scan_locked()

    def scan_locked(self):
        spec = self.manifest['nats']
        auth = {}
        if spec.get('credentials_fd') is not None:
            fd = spec['credentials_fd']
            require(type(fd) is int and fd >= 3, 'credentials_descriptor')
            # Auth content is used only in the socket handshake; no snapshot or
            # diagnostic ever contains it. The shared launcher supplies custody.
            auth = json.loads(os.pread(fd, 65536, 0))
        with NATSReader(spec['url'], auth, spec.get('ca_file')) as reader:
            with self.io_lock:
                self.active_reader = reader
            try:
                receipt = {}
                for name, scan in sorted(self.scans.items()):
                    require(not self.keeper.stop_event.is_set(), 'raw_scan_stopped')
                    if self.outage:
                        require(time.monotonic_ns() <= self.fault_interval[1], 'reader_fault_expired')
                        # Re-read all retained rows before continuing the saved
                        # cursor. A vanished row cannot turn into an honest gap.
                        scan.verify_retained(reader, {r.get('subject') for r in scan.rows})
                    receipt[name] = scan.scan(reader)
                    if self.outage:
                        require(not any(r.get('absent') for r in scan.rows if
                            r['sequence'] > self.outage['cursors'][name]), 'reader_fault_purged')
                if self.outage:
                    self.keeper.save('reader-recovery.json', dict(self.outage, recovered_ns=time.monotonic_ns(),
                                                                 streams=receipt))
                    self.outage = None
            finally:
                with self.io_lock:
                    self.active_reader = None
        self.scan_receipts.append(dict(at_ns=time.monotonic_ns(), streams=receipt))
        for name, scan in self.scans.items():
            self.keeper.save('raw-' + name + '.json', scan.rows)
        # History remains under continuous observation, including after judge.
        for transfer in self.manifest['transfers']:
            if transfer.get('discover_identity') and not transfer.get('identity_bound'):
                continue
            records = [r for r in self.scans[transfer['history_stream']].rows
                       if r.get('subject') == transfer['history_subject']]
            for record in records:
                if not record.get('data_b64'):
                    continue
                canonical = json.loads(base64.b64decode(record['data_b64']), object_pairs_hook=unique_object)
                require(canonical.get('transfer_id') == transfer['transfer_id'], 'canonical_identity')
                kind = canonical.get('kind')
                key = 'start_b64' if kind == 'start' else 'terminal_b64'
                if key not in transfer:
                    require(kind in ('start', transfer.get('expected_terminal', 'complete')), 'canonical_outcome')
                    transfer[key] = record['data_b64']
                    self.keeper.save('canonical-' + transfer['cohort'] + '.json',
                                     {k: transfer[k] for k in ('start_b64', 'terminal_b64') if k in transfer})
            if len(records) > 2 or (len(records) == 2 and not judge_history(records, transfer)):
                self.keeper.poison('canonical_history:' + transfer['transfer_id'], assertion=True)
        return receipt

    def start(self):
        self.keeper.start()
        if self.manifest.get('control', {}).get('launch'):
            self.controls = ControlOwner(self)
            self.controls.start()
        elif self.manifest.get('mode') == 'live':
            self.keeper.poison('control_launch_missing')
        def collect_raw():
            while not self.keeper.stop_event.wait(.25):
                try:
                    if self.manifest.get('mode') == 'live' and not self.keeper.records.events:
                        continue
                    self.scan()
                except (SetupError, OSError, ValueError, KeyError, TypeError) as error:
                    if not self.keeper.stop_event.is_set():
                        try:
                            self.reader_error(error)
                        except (SetupError, OSError, ValueError, KeyError, TypeError):
                            self.keeper.poison('reader_fault_invalid')
        self.scan_thread = threading.Thread(target=collect_raw, name='transfer-raw-js')
        self.scan_thread.start()
        if self.manifest.get('mode') == 'live' or self.manifest.get('launch_owners'):
            from transfer_fixture_manifest import LaunchOwner
            self.launcher = LaunchOwner(self)
            try:
                self.launcher.start()
            except (ValueError, OSError, KeyError, TypeError):
                self.keeper.poison('launch_setup')

    # origin: S7 internal review rv-drill F7. Network loss within a declared
    # fault is recoverable; malformed, missing, or rewritten evidence is not.
    def reader_error(self, error):
        now = time.monotonic_ns()
        path = self.keeper.evidence / 'reader-fault.json'
        if path.exists():
            declared = json.loads(path.read_text(), object_pairs_hook=unique_object)
            require(self.manifest.get('arm') == 'C3' and declared['end_ns'] - declared['start_ns'] <= 5_000_000_000,
                    'reader_fault_interval')
            self.fault_interval = (declared['start_ns'], declared['end_ns'])
        if isinstance(error, (ConnectionError, TimeoutError, socket.timeout)) and self.fault_interval and                 self.fault_interval[0] <= now <= self.fault_interval[1]:
            if self.outage is None:
                self.outage = dict(start_ns=now, cursors={name: scan.cursor for name, scan in self.scans.items()})
            return
        self.keeper.poison('raw_scan_unavailable')

    def close(self):
        # origin: S7 internal review rv-drill F8. Failure in one closer cannot
        # abandon the remaining threads or supervised children. Report it only
        # after every owner has had its bounded join attempt.
        errors = []
        if self.controls:
            try:
                self.controls.close()
            except (SetupError, OSError, ValueError, KeyError, TypeError):
                errors.append('control_join')
        self.keeper.stop_event.set()
        with self.io_lock:
            if self.active_reader and self.active_reader.socket:
                try:
                    self.active_reader.socket.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        if self.scan_thread:
            self.scan_thread.join(timeout=10)
            if self.scan_thread.is_alive():
                errors.append('raw_scan_join')
        for owner in (self.keeper, self.launcher):
            if owner:
                try:
                    owner.close()
                except (SetupError, OSError, ValueError, KeyError, TypeError):
                    errors.append('owner_join')
        for channel in getattr(self, 'released_channels', []):
            channel.close()
        self.released_channels = []
        require(not errors, 'fixture_join_failed')

    # origin: S7 internal review rv-drill F1. Native completion is sealed by
    # main before waiting for release; the supervised Wait is checked at stop.
    def native_results(self):
        keeper, results = self.keeper, {}
        with keeper.lock:
            require(not keeper.records.failure, 'native_custody_poisoned')
            for fd in keeper.records.frames:
                raw, offset = os.pread(fd, os.fstat(fd).st_size, 0), 0
                while offset < len(raw):
                    require(raw[offset:offset+8] == b'TDRILL1\0', 'native_custody_header')
                    identity = raw[offset+8:offset+40]
                    label = next((k for k, v in keeper.identities.items() if v == identity), None)
                    require(label is not None, 'native_custody_identity')
                    sequence, poison, count = struct.unpack_from('<QQI', raw, offset+56)
                    require(sequence > 0 and poison == 0 and count <= 16, 'native_custody_poisoned')
                    offset += 76
                    for _ in range(count):
                        length = raw[offset]; offset += 1
                        kind = raw[offset:offset+length]; offset += length
                        length = raw[offset]; offset += 1
                        domain = raw[offset:offset+length]; offset += length
                        size = struct.unpack_from('<I', raw, offset+228)[0]
                        offset += 232
                        body = raw[offset:offset+size]; offset += size
                        require(offset <= len(raw), 'native_custody_truncated')
                        if kind != b'native-exit':
                            continue
                        require(domain == b'exec' and label not in results, 'native_result_duplicate')
                        value = json.loads(body, object_pairs_hook=unique_object)
                        require(set(value) == {'pid', 'process_start', 'boot_id', 'native_rc'} and
                                type(value['native_rc']) is int and 0 <= value['native_rc'] <= 255,
                                'native_result_shape')
                        process = keeper.processes.get(label)
                        require(process and all(value[k] == process[k] for k in
                                ('pid', 'process_start', 'boot_id')), 'native_ctl_binding')
                        results[label] = value
        return results

    def release_originals(self):
        keeper, results = self.keeper, self.native_results()
        require(self.launcher is not None and self.drains, 'native_release_owner_missing')
        released = []
        with keeper.lock:
            for transfer in self.manifest['transfers']:
                label = transfer['ctl_producer']
                require(label in results and label in keeper.channels and label not in keeper.pending_drains,
                        'native_release_not_ready')
                nonce = bytes.fromhex(self.drains[-1]['acks'][label])
                require(keeper.acks[label] == nonce.hex(), 'native_release_ack_missing')
                sequence = keeper.records.sequences[(label, keeper.records.incarnations[label])]
                channel = keeper.channels.pop(label)
                # Retain the local socket until the collector has joined; its
                # select snapshot can still own this FD at the release boundary.
                self.released_channels = getattr(self, 'released_channels', []) + [channel]
                channel.send(b'TDR2' + nonce + struct.pack('<Q', sequence))
                released.append(dict(producer=label, nonce=nonce.hex(), sealed_sequence=sequence))
        # The collector cannot read these released channels; an unexpected EOF
        # before this boundary still poisons custody in its normal read loop.
        deadline = time.monotonic() + 10
        while True:
            self.launcher.sample()
            pending = []
            for transfer in self.manifest['transfers']:
                label = transfer['ctl_producer']
                spec, child, receipt = next(row for row in self.launcher.children if row[0]['label'] == label)
                code = child.poll()
                if code is None:
                    pending.append(label)
                    continue
                require(receipt.exists() and int(receipt.read_text()) == code == results[label]['native_rc'],
                        'native_exit_changed')
                native = json.loads(Path(transfer['ctl_receipt']).read_bytes(), object_pairs_hook=unique_object)
                require(all(native[k] == results[label][k] for k in results[label]), 'native_exit_binding')
            if not pending:
                keeper.save('native-release.json', dict(releases=released, results=results))
                return
            require(time.monotonic() < deadline, 'native_exit_missing')
            keeper.stop_event.wait(.02)

    def snapshot(self):
        if self.manifest.get('mode') != 'live' or self.keeper.records.events:
            try:
                self.scan()
            except (SetupError, OSError, ValueError, KeyError, TypeError) as error:
                # origin: S7 internal review rv-drill F7. Foreground sampling
                # follows the same bounded recovery rule as the raw collector.
                self.reader_error(error)
                require(self.outage and not self.keeper.setup_failures, 'raw_scan_unavailable')
        if self.launcher:
            self.launcher.sample()
        value = self.keeper.sample()
        value.update(manifest_sha256=self.manifest['manifest_sha256'], drains=list(self.drains),
                     scan_receipts=list(self.scan_receipts))
        self.keeper.save('snapshot.json', value)
        from transfer_fixture_manifest import scheduler_sample
        self.sample_sequence += 1
        sample = scheduler_sample(self.manifest, value, self.sample_sequence)
        self.keeper.save('sample.json', sample)
        return value

    def drain(self):
        receipt = self.keeper.drain()
        # A declared restart may still be reconnecting when the driver asks
        # for the barrier. Preserve custody/cursors while retrying only inside
        # that fixed interval; all permanent damage remains SETUP.
        while True:
            try:
                receipt['streams'] = self.scan()
                break
            except (SetupError, OSError, ValueError, KeyError, TypeError) as error:
                self.reader_error(error)
                require(self.outage and not self.keeper.setup_failures, 'raw_scan_unavailable')
                self.keeper.stop_event.wait(.02)
        with self.scan_lock:
            spec = self.manifest['nats']
            auth = json.loads(os.pread(spec['credentials_fd'], 65536, 0)) if spec.get('credentials_fd') else {}
            with NATSReader(spec['url'], auth, spec.get('ca_file')) as reader:
                for name, scan in self.scans.items():
                    subjects = {t['history_subject'] for t in self.manifest['transfers'] if t['history_stream'] == name}
                    # History remains permanent. OBJ can already be tombstoned
                    # and purged after exact ACK plus retirement; its original
                    # physical bytes/digest were collected continuously above.
                    scan.verify_retained(reader, subjects)
        self.drains.append(receipt)
        return receipt

    # origin: S7 internal review rv-drill F8. Stop is a final evidence barrier,
    # and its joined result must exist before the parent prints its one verdict.
    def stop(self):
        require(self.keeper.judged, 'stop_before_verdict')
        try:
            self.drain()
            self.snapshot()
            if self.manifest['mode'] == 'live':
                self.release_originals()
        except (SetupError, OSError, ValueError, KeyError, TypeError):
            self.keeper.poison('final_drain_unavailable')
        try:
            self.close()
        except (SetupError, OSError):
            self.keeper.poison('fixture_join_failed')
        from transfer_fixture_manifest import mutation_receipts
        mutation_receipts(self, self.keeper.verdict)
        return self.keeper.verdict

    def coverage(self, events):
        return cut_coverage(self.manifest, events)

    def owners_joined(self, events, transfer):
        rows = [e for e in events if e['transfer_id'] == transfer['transfer_id']]
        # Each registered process which participated must declare its closed
        # admission, completion/join and retirement. Two seconds of silence is
        # not a substitute for any of those receipts.
        participants = {r['producer'] for r in rows}
        if not participants:
            return False
        for producer in participants:
            own = [r for r in rows if r['producer'] == producer]
            if not any(r['kind'] == 'retirement' and r.get('admission_closed') and
                       r.get('retirement_digest') and r.get('stop_nonce') for r in own):
                return False
            entered = {r.get('stop_nonce') for r in own if r['kind'] == 'callback_enter'}
            joined = {r.get('stop_nonce') for r in own if r['kind'] == 'callback_join'}
            if not entered <= joined:
                return False
            if any(r['kind'] == 'fetched' for r in own) and not any(
                r['kind'] == 'reader_stop' and r.get('consumer_deleted') and r.get('pong_confirmed') for r in own):
                return False
            if any(r['kind'] == 'control' for r in own) and not any(r['kind'] == 'acknowledge' for r in own):
                return False
        return True

    def await_originals(self):
        # origin: S7 internal review rv-drill F1. Completing the cut ladder is
        # not completion of the original transfers. Observe their real joins
        # within the frozen budget before judging final history/target bytes.
        if self.manifest['mode'] != 'live':
            return
        # A native receipt cannot arrive without the original supervised owner.
        # Fail setup now instead of consuming a product transfer's byte budget.
        # origin: S7 internal review rv-drill F1
        if self.launcher is None:
            self.keeper.poison('native_ctl_receipt_missing')
            raise SetupError('native_ctl_receipt_missing')
        while True:
            self.snapshot()
            events = self.keeper.records.events
            results = self.native_results()
            pending = [t for t in self.manifest['transfers'] if not self.owners_joined(events, t) or
                       t['ctl_producer'] not in results]
            if any(row['native_rc'] != 0 for row in results.values()):
                return
            if not pending or self.keeper.failures or self.keeper.setup_failures:
                return
            now = time.monotonic_ns()
            for transfer in pending:
                budget = 1084 if transfer['size'] > 66549440 else (
                    300 if self.manifest.get('arm') in ('W1', 'W2') else 568)
                if self.manifest.get('consumer') == 'infra-transfer-v1':
                    from infra_transfer_fixture import completion_seconds
                    budget = completion_seconds(transfer)
                if now - transfer.get('launched_ns', self.started_ns) >= budget * 1_000_000_000:
                    self.keeper.poison('original_completion_deadline', assertion=True)
                    return
            self.keeper.stop_event.wait(.1)

    def judge(self):
        self.await_originals()
        keeper, failures, missing, product = self.keeper, [], [], []
        snapshot = self.snapshot()
        events = snapshot['events']
        joined = True
        if keeper.pending_bindings:
            keeper.poison('unbound_observations')
        for transfer in self.manifest['transfers']:
            tid = transfer['transfer_id']
            if self.manifest['mode'] == 'live':
                # The result is the original main's sealed native decision.
                # Later barriers still need this process's observer alive;
                # stop verifies its actual supervised exit after final release.
                try:
                    native = self.native_results()[transfer['ctl_producer']]
                    keeper.save('native-held-' + transfer['ctl_producer'] + '.json', native)
                    if native['native_rc'] != 0:
                        product.append('native_ctl:' + tid + ':' + str(native['native_rc']))
                except (SetupError, OSError, ValueError, KeyError, TypeError):
                    keeper.poison('native_ctl_receipt_missing')
            history = self.scans[transfer['history_stream']].rows
            if not judge_history(history, transfer):
                failures.append('canonical_history:' + tid)
            if transfer.get('tier') != 'A' and not judge_object(self.scans[transfer['object_stream']].rows, transfer, events):
                failures.append('physical_object:' + tid)
            if not self.owners_joined(events, transfer):
                joined = False
            primitives = [e for e in events if e['transfer_id'] == tid and e['kind'] == 'primitive_exit' and
                          e.get('outcome') == 'landed']
            if len(primitives) != 1 or not any(e['kind'] == 'directory_sync' and e.get('parent_synced') and
                                              e['transfer_id'] == tid for e in events):
                failures.append('primitive_durability:' + tid)
            target = keeper.targets[tid]
            if not target.samples or target.samples[-1]['state'] != 'new':
                failures.append('final_destination:' + tid)
            for resource in transfer['required_resources']:
                witness = keeper.witnesses.get(resource)
                if witness is None:
                    keeper.poison('ofd_witness_missing')
                else:
                    # Workers close their FDs before the ending join. Sign the
                    # kernel comparisons collected at each import/cut, not a
                    # lookup of the deliberately closed FD at final judge.
                    schedule = transfer['required_resources']
                    if not isinstance(schedule, dict):
                        keeper.poison('ofd_resource_schedule_missing')
                    expected = set(schedule.get(resource, [])) if isinstance(schedule, dict) else set()
                    observed = {c['cut_id'] for c in witness['checks']}
                    if not expected <= observed:
                        keeper.poison('ofd_cut_witness_missing')
                    if not all(c['equal'] for c in witness['checks']):
                        failures.append('ofd_reopened:' + resource)
        missing += self.coverage(events)
        from transfer_oracle import rollback_task_coverage
        missing += rollback_task_coverage(self.manifest, events)
        for code in failures:
            keeper.poison(code, assertion=True)
        if not joined:
            keeper.poison('owners_not_joined')
        if joined:
            self.drain()
            # Quiet is predeclared, measured from the first fresh drain and raw
            # scan. Intake/target sampling continue for the entire interval.
            deadline = time.monotonic() + self.manifest['quiet_seconds']
            while time.monotonic() < deadline:
                keeper.stop_event.wait(min(.1, deadline - time.monotonic()))
                self.scan()
            self.drain()
        if self.controls:
            self.controls.close()
        control = self.manifest.get('control')
        if control is not None:
            try:
                judge_control(control, keeper.records.events + (self.controls.attempts if self.controls else []),
                              self.controls.effects if self.controls else {})
            except (SetupError, KeyError, TypeError):
                keeper.poison('control_contract', assertion=True)
        with keeper.lock:
            rc = 1 if keeper.failures else 2 if keeper.setup_failures else 3 if product else 4 if missing else 0
            verdict = dict(native_rc=rc, failures=list(keeper.failures), setup=list(keeper.setup_failures),
                           product=product, not_covered=missing, manifest_sha256=self.manifest['manifest_sha256'],
                           drains=list(self.drains), oracle_input_sha=hashlib.sha256(json.dumps(
                               {name: s.rows for name, s in self.scans.items()}, sort_keys=True).encode()).hexdigest(),
                           reader_digest=reader_digest())
            keeper.judged, keeper.verdict = True, verdict
            keeper.save('verdict.json', verdict)
        from transfer_fixture_manifest import mutation_receipts
        mutation_receipts(self, verdict)
        return verdict


def reader_digest():
    digest = hashlib.sha256()
    for name in ('transfer_fixture.py', 'transfer_observation.py', 'transfer_oracle.py', 'transfer_keeper.py'):
        digest.update(Path(__file__).with_name(name).read_bytes())
    adapter = Path(__file__).with_name('infra_transfer_fixture.py')
    if adapter.is_file():
        digest.update(adapter.read_bytes())
    return digest.hexdigest()


def serve(manifest, evidence):
    if manifest.get('consumer') == 'infra-transfer-v1':
        from infra_transfer_fixture import fixture as infra_fixture
        fixture = infra_fixture(manifest, evidence)
    else:
        fixture = Fixture(manifest, evidence)
    path = Path(evidence) / 'control.sock'
    server = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    try:
        if manifest.get('consumer') == 'infra-transfer-v1':
            from infra_transfer_fixture import unix_call
            unix_call(server, 'bind', evidence)
        else:
            server.bind(str(path))
        os.chmod(path, 0o600)
        server.listen(8)
        fixture.start()
        print(json.dumps(dict(fixture_pid=os.getpid(), custody_refs=dict(control=str(path), socket_fd=3,
                              config_fd=4), manifest_sha256=manifest['manifest_sha256'])), flush=True)
        stopping = False
        while not stopping:
            with server.accept()[0] as channel:
                rights, producer, config = [], None, None
                fds, command = [], ''
                try:
                    raw, fds = receive(channel)
                    request = json.loads(raw, object_pairs_hook=unique_object)
                    require(request.get('manifest_sha256') == manifest['manifest_sha256'], 'manifest_changed')
                    command = request['command']
                    if command == 'lease':
                        require(not fds, 'unexpected_descriptors')
                        producer, config = fixture.keeper.lease(request['label'])
                        rights = [producer.fileno(), config]
                        result = dict(native_rc=0, socket_fd=3, config_fd=4)
                    elif command == 'start':
                        require(not fds and fixture.launcher is not None, 'cell_launch_owner')
                        fixture.launcher.start_cell((Path(evidence) / 'requested-step').read_text().strip())
                        result = dict(native_rc=0)
                    else:
                        require(not fds, 'unexpected_descriptors')
                        if command == 'sample':
                            value = fixture.snapshot()
                            rc = 1 if value['failures'] else 2 if value['setup'] else (
                                value.get('verdict') or {}).get('native_rc', 0)
                            result = dict(native_rc=rc,
                                          snapshot=str(Path(evidence) / 'snapshot.json'),
                                          event_count=len(value['events']))
                        elif command == 'drain':
                            result = fixture.drain()
                        elif command == 'judge':
                            result = fixture.judge()
                        elif command == 'stop':
                            require(fixture.keeper.judged or (Path(evidence) / 'failure.json').exists(),
                                    'stop_before_verdict')
                            if not fixture.keeper.judged:
                                fixture.keeper.verdict = json.loads((Path(evidence) / 'failure.json').read_text())
                                fixture.keeper.judged = True
                            result, stopping = fixture.stop(), True
                        else:
                            raise SetupError('fixture_command')
                except (SetupError, OSError, ValueError, KeyError, TypeError):
                    fixture.keeper.poison('fixture_setup')
                    result = dict(native_rc=1 if fixture.keeper.failures else 2, setup=['fixture_setup'])
                    fixture.keeper.save('failure.json', result)
                    if locals().get('command') == 'judge':
                        fixture.keeper.judged, fixture.keeper.verdict = True, result
                        fixture.keeper.save('verdict.json', result)
                finally:
                    for fd in locals().get('fds', []):
                        os.close(fd)
                ancillary = [] if not rights else [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', rights))]
                try:
                    channel.sendmsg([json.dumps(result).encode()], ancillary)
                except OSError:
                    fixture.keeper.poison('control_channel_closed')
                finally:
                    if producer:
                        producer.close()
                    if config is not None:
                        os.close(config)
        return fixture.keeper.verdict['native_rc']
    finally:
        fixture.close()
        server.close()
        path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('start', 'sample', 'drain', 'judge', 'stop'))
    parser.add_argument('--manifest', required=True, type=Path)
    parser.add_argument('--evidence', required=True, type=Path)
    args = parser.parse_args()
    manifest = read_manifest(args.manifest)
    if args.command == 'start' and not (args.evidence / 'control.sock').exists():
        return serve(manifest, args.evidence)
    with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as channel:
        channel.settimeout(1200 if args.command == 'judge' else 60)
        if args.command == 'judge' and manifest.get('consumer') == 'infra-transfer-v1':
            from infra_transfer_fixture import completion_seconds
            channel.settimeout(max(completion_seconds(t) for t in manifest['transfers']) + 30)
        if manifest.get('consumer') == 'infra-transfer-v1':
            from infra_transfer_fixture import unix_call
            unix_call(channel, 'connect', args.evidence)
        else:
            channel.connect(str(args.evidence / 'control.sock'))
        channel.send(json.dumps(dict(command=args.command, manifest_sha256=manifest['manifest_sha256'])).encode())
        raw, fds = receive(channel)
        for fd in fds:
            os.close(fd)
        require(not fds, 'unexpected_descriptors')
        result = json.loads(raw)
        print(native_verdict(result) if args.command in ('judge', 'stop') else json.dumps(result))
        return result.get('native_rc', 0)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (SetupError, OSError, ValueError, KeyError, TypeError):
        print(json.dumps(dict(native_rc=2, setup=['fixture_setup'])))
        sys.exit(2)
