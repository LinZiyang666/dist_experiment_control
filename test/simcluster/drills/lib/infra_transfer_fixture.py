#!/usr/bin/env -S python3 -I -B
"""S9 consumer of S7 custody, native outcomes, targets and raw JS readers.

Only infra-transfer-v1 opts into these rules. Drill 63/64 retain their frozen
image-cut matrix, four replaceable binaries and normal/watchdog rates. Infra
never replaces an agent image: four session homes may share supplied immutable
binary bytes. P32 owns physical infra hop coverage; S7 still owns every original
OFD, canonical history, primitive, quiet barrier and final native result.

Rates are startup values of the real --rate-limit flag. This adapter neither
pauses a producer nor extends its original product deadline. The product's
2108-second ceiling is admission, not an invitation to clamp a slower rate.
origin: hot-update S9 plan §9.1 / lead LQ3; S7 frozen plan §1.2 / §10.3–10.4.
"""
import hashlib
import argparse
import json
import os
from pathlib import Path
import socket
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from transfer_observation import require

CONSUMER = 'infra-transfer-v1'
SIZES = {'small-push': 66549440, 'small-pull': 66549440,
         'large-push': 1 << 30, 'large-pull': 1 << 30}
RESOURCE_CUT = 'infra-custody'


def completion_seconds(transfer):
    """Exact S7 tier-B admission formula; no clock or start is minted here."""
    size, requested = transfer['size'], transfer['rate_limit']
    require(type(size) is int and size > 0 and type(requested) is int and requested > 0,
            'infra_product_budget')
    rate = min(2 << 20, requested)
    seconds = max(300, 2 * ((size + rate - 1) // rate) + 60)
    require(seconds <= 2108, 'infra_product_budget')
    return seconds


def rates(seconds):
    # Reserve a startup margin without needlessly stretching a short arm to
    # the maximum product deadline. The cap is admission, never a clamp of an
    # invalid rate. A template freezes these values before any ctl is started.
    require(type(seconds) is int and 0 < seconds < 1024, 'infra_product_budget')
    leg = min(1024, seconds + 32)
    result = {name: (size + leg - 1) // leg for name, size in SIZES.items()}
    for name, rate in result.items():
        completion_seconds(dict(size=SIZES[name], rate_limit=rate))
        require(SIZES[name] // rate > seconds, 'infra_startup_rate_budget')
    return result


def validate_manifest(value):
    """Validate the different consumer, never relabel an S7 arm as infra."""
    from hot_update_infra_verbs import PROFILES
    require(value.get('consumer') == CONSUMER and value['mode'] == 'live', 'infra_consumer')
    require(not any(k in value for k in ('arm', 'driver', 'driver_sha', 'rollback_tasks')),
            'infra_s7_driver_confusion')
    profile = value.get('infra', {})
    require(set(profile) == {'schema', 'component', 'topology', 'profile', 'seconds', 'rates'} and
            profile['schema'] == 'infra-transfer-profile-v1', 'infra_profile')
    entry = PROFILES.get((profile['component'], profile['profile']))
    require(entry and profile['topology'] in entry['topologies'], 'infra_arm')
    seconds = profile['seconds']
    require(type(seconds) is int and 0 < seconds <= 4320, 'infra_seconds')
    transfers = value['transfers']
    require(len(transfers) == 4 and {t['cohort'] for t in transfers} == set(SIZES), 'four_cohorts')
    require(type(profile['rates']) is dict and set(profile['rates']) == set(SIZES), 'infra_rates')
    processes = {p['label']: p for p in value['processes']}
    agents = [p for p in processes.values() if p.get('role') == 'agent']
    # Independent sessions still need independent homes. Unlike image cuts,
    # infra does not mutate agent binary paths, so shared supplied bytes are OK.
    require(len(agents) == 4 and len({p['home'] for p in agents}) == 4, 'infra_agent_sessions')
    require(len({t['agent_producer'] for t in transfers}) == 4 and
            len({t['ctl_producer'] for t in transfers}) == 4, 'infra_original_isolation')
    require(value['cuts'] == [] and value.get('control', {}).get('launch'), 'infra_schedule_owner')
    all_resources = []
    for transfer in transfers:
        require(transfer['size'] == SIZES[transfer['cohort']], 'cohort_size')
        budget = completion_seconds(transfer)
        rate = transfer['rate_limit']
        require(rate == profile['rates'][transfer['cohort']], 'infra_rate_changed')
        require(transfer['size'] // rate > seconds and budget > seconds, 'infra_startup_rate_budget')
        require(processes.get(transfer.get('agent_producer'), {}).get('role') == 'agent' and
                processes.get(transfer.get('ctl_producer'), {}).get('role') == 'ctl', 'infra_original_owner')
        refs = transfer.get('resource_refs', {})
        require(set(refs) == {'source', 'tmp', 'parent'} and
                all(isinstance(v, str) and v for v in refs.values()), 'infra_resource_refs')
        require(transfer['required_resources'] == {v: [RESOURCE_CUT] for v in refs.values()},
                'infra_required_resources')
        all_resources.extend(refs.values())
    require(len(set(all_resources)) == 12, 'infra_shared_resource')
    return value


def bind_discovered(value, expected):
    """Read S7's actual custody discovery without changing its manifest hash.

    The original five-verb control channel continues to use the startup file's
    digest. P32 reads the SAME file plus S7's actual discovery archive; it never
    needs a rewritten manifest (which would break that control binding).
    """
    root = value.get('identity_evidence')
    if root is None:
        return
    root = Path(root)
    require(root.is_absolute() and not root.is_symlink(), 'infra_identity_evidence')
    from transfer_observation import unique_object, validate_event
    from transfer_keeper import process_identity
    snapshot = root / 'snapshot.json'
    if not snapshot.exists():
        return
    require(snapshot.is_file() and not snapshot.is_symlink(), 'infra_identity_snapshot')
    snapshot = json.loads(snapshot.read_bytes(), object_pairs_hook=unique_object)
    require(snapshot.get('manifest_sha256') == expected and not snapshot.get('setup'), 'infra_identity_snapshot')
    for transfer in value['transfers']:
        if not transfer.get('discover_identity'):
            continue
        path = root / ('identity-' + transfer['cohort'] + '.json')
        if not path.exists():
            continue  # Startup validation precedes first product observation.
        require(path.is_file() and not path.is_symlink(), 'infra_identity_archive')
        row = json.loads(path.read_bytes(), object_pairs_hook=unique_object)
        initial = [event for event in snapshot['events'] if all(event.get(k) == row[k] for k in
                   ('transfer_id', 'entry_nonce', 'pid', 'process_start', 'boot_id'))]
        require(initial, 'infra_identity_custody')
        if not row.get('object_version'):
            versions = {e['object_version'] for e in snapshot['events'] if e.get('object_version') and
                e.get('transfer_id') == row['transfer_id'] and e.get('entry_nonce') == row['entry_nonce']}
            require(len(versions) <= 1, 'infra_identity_custody')
            if not versions:
                continue  # Prepared is real custody; READY still needs NUID.
            row['object_version'] = versions.pop()
        require(all(isinstance(row.get(k), str) and row[k] for k in
                    ('transfer_id', 'entry_nonce', 'object_version')), 'infra_identity_unbound')
        matched = [event for event in snapshot['events'] if all(event.get(k) == row[k] for k in (
                    'transfer_id', 'entry_nonce', 'object_version',
                    'pid', 'process_start', 'boot_id')) and event['producer'] in
                    (transfer['agent_producer'], transfer['ctl_producer'])]
        require(matched, 'infra_identity_custody')
        for event in matched:
            validate_event({k: v for k, v in event.items() if k != 'producer'})
        identity = process_identity(row['pid'])
        require(all(identity[k] == row[k] for k in ('pid', 'process_start', 'boot_id')) and any(
            identity['running_sha'] in process['artifact_shas'] for process in value['processes'] if
            process['label'] in {event['producer'] for event in matched}), 'infra_identity_process')
        transfer.update(**{k: row[k] for k in ('transfer_id', 'entry_nonce', 'object_version')})
        if not transfer.get('object_name'):
            transfer['object_name'] = row['transfer_id']
    for key in ('transfer_id', 'entry_nonce', 'object_version'):
        bound = [t[key] for t in value['transfers'] if t[key]]
        require(len(set(bound)) == len(bound), 'infra_identity_isolation')


def fixture(manifest, evidence):
    """Extend S7's real collector; the original five-verb protocol is intact."""
    import transfer_fixture as upstream
    from transfer_keeper import Keeper, compare_ofd, process_identity
    from transfer_oracle import responsibility

    class InfraKeeper(Keeper):
        def __init__(self, value, root):
            super().__init__(value, root)
            self.pending_ofds, self.bound_resources = [], {}

        def accept_ofd(self, label, metadata, fd):
            # TDO1 may precede its TDC1 event. Retain custody, bounded by the
            # twelve predeclared kind slots, until the sealed event binds its
            # actual product-generated ID to one original transfer and owner.
            if len(self.pending_ofds) >= 12:
                os.close(fd)
                require(False, 'infra_pending_resource_capacity')
            self.pending_ofds.append((label, metadata, fd))
            self.resolve_ofds()

        def resolve_ofds(self):
            for pending in list(self.pending_ofds):
                label, metadata, fd = pending
                ref = metadata['resource']
                kind = ref['name']
                matched = [event for event in self.records.events if event['producer'] == label and
                    all(event.get(k) == metadata[k] for k in ('pid', 'process_start', 'boot_id')) and
                    event.get(kind) == ref]
                if not matched:
                    continue
                self.pending_ofds.remove(pending)
                try:
                    ids = {event['transfer_id'] for event in matched}
                    require(len(ids) == 1, 'infra_resource_transfer')
                    transfers = [t for t in self.manifest['transfers'] if t['transfer_id'] in ids]
                    require(len(transfers) == 1, 'infra_resource_transfer')
                    transfer = transfers[0]
                    require(all(event['entry_nonce'] == transfer['entry_nonce'] for event in matched),
                            'infra_resource_nonce')
                    push = transfer['cohort'].endswith('push')
                    owner = transfer['ctl_producer'] if (kind == 'source') == push else transfer['agent_producer']
                    require(label == owner, 'infra_resource_owner')
                    slot = (transfer['transfer_id'], kind)
                    require(slot not in self.bound_resources, 'infra_resource_replaced')
                    require(not any(ref['resource_id'] in t['required_resources'] for t in self.manifest['transfers']),
                            'infra_resource_shared')
                    previous = transfer['resource_refs'][kind]
                    require(transfer['required_resources'].pop(previous) == [RESOURCE_CUT], 'infra_resource_schedule')
                    transfer['resource_refs'][kind] = ref['resource_id']
                    transfer['required_resources'][ref['resource_id']] = [RESOURCE_CUT]
                    self.bound_resources[slot] = ref['resource_id']
                except BaseException:
                    os.close(fd)
                    raise
                # The shared S7 implementation now owns fd, including its
                # close on error. It locates the actual product FD with kcmp;
                # inode/offset equality cannot sign this initial binding.
                super().accept_ofd(label, metadata, fd)
                self.witnesses[ref['resource_id']]['checks'].append(dict(cut_id=RESOURCE_CUT,
                    generation=matched[-1]['generation'], at_ns=matched[-1]['boottime_ns'], equal=True))

        def add_event(self, source, row):
            super().add_event(source, row)
            self.resolve_ofds()

        def close(self):
            # Stop and join the shared collector before retiring its pending
            # SCM_RIGHTS entries. Closing them while it resolves a sealed row
            # races a reused descriptor and can steal another owner's FD.
            try:
                super().close()
            finally:
                for _, _, fd in self.pending_ofds:
                    os.close(fd)
                self.pending_ofds.clear()

    class InfraFixture(upstream.Fixture):
        def __init__(self, value, root):
            super().__init__(value, root)
            self.keeper = InfraKeeper(value, root)
            self.reader_attempt_ns = time.monotonic_ns()

        def scan_locked(self):
            self.reader_attempt_ns = time.monotonic_ns()
            return super().scan_locked()

        def reader_error(self, error):
            from hot_update_infra_probe import budgets
            from transfer_observation import SetupError
            transient = isinstance(error, (ConnectionError, TimeoutError)) or isinstance(error, SetupError) and str(error) in (
                'nats_truncated', 'websocket_closed', 'nats_deadline', 'nats_request_deadline')
            if not transient:
                return super().reader_error(error)
            # A fresh independent observer connection can hit the planned
            # NATS/WSS handoff. Keep the FIRST attempt's five-second deadline,
            # saved raw cursor and outage. The inherited scan rechecks every
            # retained row before accepting recovery; corrupt JSON, changed JS
            # bytes, missing rows and certificate errors never enter this path.
            if self.outage is None:
                self.fault_interval = (self.reader_attempt_ns, self.reader_attempt_ns +
                    budgets()['HOT_UPDATE_REQUEST_S'] * 1_000_000_000)
                self.outage = dict(start_ns=self.reader_attempt_ns,
                                  cursors={name: scan.cursor for name, scan in self.scans.items()})
            if time.monotonic_ns() > self.fault_interval[1]:
                self.keeper.poison('infra_reader_gap', assertion=True)
                require(False, 'infra_reader_gap')

        def snapshot(self):
            # P32 samples before/after physical phases and every healthy second.
            # Compare live resources at that boundary in the actual custodian;
            # matching inode/offset never substitutes for kernel OFD equality.
            with self.keeper.lock:
                for transfer in self.manifest['transfers']:
                    for kind, resource in transfer['resource_refs'].items():
                        witness = self.keeper.witnesses.get(resource)
                        if witness is None:
                            continue  # Base final judge requires all three.
                        own = [r for r in self.keeper.records.events if
                            r['transfer_id'] == transfer['transfer_id'] and r['pid'] == witness['pid']]
                        role = 'sender' if kind == 'source' else 'receiver'
                        if not own or own[-1]['phase'] != ('sending' if role == 'sender' else 'receiving') or not any(
                                responsibility(row, role) for row in own):
                            continue  # This data leg has not started or retired.
                        identity = process_identity(witness['pid'])
                        require(identity['process_start'] == witness['process_start'], 'ofd_process_recycled')
                        equal = compare_ofd(witness['pid'], witness['product_fd'], witness['fd'])
                        witness['checks'].append(dict(cut_id=RESOURCE_CUT, generation=own[-1]['generation'],
                            at_ns=time.clock_gettime_ns(time.CLOCK_BOOTTIME), equal=equal))
                        if not equal:
                            self.keeper.poison('ofd_reopened:' + resource, assertion=True)
            return super().snapshot()

    return InfraFixture(manifest, evidence)


def freeze(value, context):
    """Explicit templates choose pacing once; concrete invocations stay exact."""
    import copy
    value = copy.deepcopy(value)
    template = value.get('schema') == 'infra-transfer-template-v1'
    require((template or value.get('schema') == 1) and value.get('mode') == 'live', 'infra_source_manifest')
    require(value.get('consumer') == CONSUMER, 'infra_consumer')
    value['schema'] = 1
    value['infra'] = dict(schema='infra-transfer-profile-v1', **{key: context[key] for key in
                        ('component', 'topology', 'profile', 'seconds', 'rates')})
    processes = {p['label']: p for p in value['processes']}
    for transfer in value['transfers']:
        rate = context['rates'][transfer['cohort']]
        launch = processes[transfer['ctl_producer']]['launch']['argv']
        require(launch.count('--rate-limit') == 1, 'infra_real_rate_flag')
        i = launch.index('--rate-limit') + 1
        require(i < len(launch), 'infra_real_rate_flag')
        if template:
            require(launch[i] == transfer['rate_limit'] == '{rate_limit}', 'infra_template_rate')
            transfer['rate_limit'], launch[i] = rate, str(rate)
        # A concrete different rate is a conflict, never permission to alter
        # another owner's invocation. Only the explicitly typed template above
        # has unbound startup parameters; it cannot be read as an S7 manifest.
        require(launch[i].isdigit(), 'infra_real_rate_flag')
        require(int(launch[i]) == rate and transfer['rate_limit'] == rate, 'infra_rate_changed')
    return validate_manifest(value)


def project(source, context, output, evidence=None):
    """Freeze an explicitly supplied S7 deployment before starting originals.

    This is a consumer projection, not a copy of a drill63 driver. Identities,
    launch argv, source hashes, streams and target baselines remain supplied by
    the real deployment owner. Source bytes are never read from a test model.
    """
    from transfer_observation import unique_object
    value = freeze(json.loads(Path(source).read_bytes(), object_pairs_hook=unique_object), context)
    if evidence is not None:
        value['identity_evidence'] = str(Path(evidence).absolute())
    raw = (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()
    path = Path(output)
    with path.open('xb') as stream:
        stream.write(raw)
    return hashlib.sha256(raw).hexdigest()


def paths(context):
    root = Path(context['evidence']) / 's7'
    require(root.is_absolute() and not root.is_symlink(), 'infra_evidence')
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return root / 'manifest.json', root / 'observer'


def unix_call(channel, operation, evidence):
    """Long composer evidence paths must not consume sockaddr_un's budget."""
    fd = os.open(evidence, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        getattr(channel, operation)('/proc/self/fd/' + str(fd) + '/control.sock')
    finally:
        os.close(fd)


def call(manifest, evidence, verb):
    """Use S7's existing socket and retain its original native response."""
    import transfer_fixture as upstream
    value = upstream.read_manifest(manifest)
    return custody_call(manifest, evidence, verb, value['manifest_sha256'])


def custody_call(manifest, evidence, verb, expected=None):
    """Read terminal custody after the original ctl has been reaped.

    The original foreground custodian owns the immutable startup manifest.
    Re-running live discovery at this point would demand a new process for a
    successfully completed original. Socket admission still binds the exact
    startup bytes; this function never restarts a caller or changes its NUID.
    origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-4.
    """
    from transfer_observation import receive, unique_object
    import infra_composer_contract as contract
    require(verb in ('sample', 'drain', 'judge', 'stop'), 'infra_custody_verb')
    contract.read(manifest)
    digest = contract.sha(manifest)
    require(expected is None or expected == digest, 'infra_manifest_changed')
    with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as channel:
        channel.settimeout(60)
        unix_call(channel, 'connect', evidence)
        channel.send(json.dumps(dict(command=verb, manifest_sha256=digest)).encode())
        raw, fds = receive(channel)
        for fd in fds:
            os.close(fd)
        require(not fds, 'unexpected_descriptors')
    return json.loads(raw, object_pairs_hook=unique_object)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('prepare', 'keeper', 'stop', 'binding'))
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--context', required=True, type=Path)
    parser.add_argument('--evidence', type=Path)
    args = parser.parse_args()
    import transfer_fixture as upstream
    from transfer_observation import SetupError, unique_object
    try:
        context = json.loads(args.context.read_bytes(), object_pairs_hook=unique_object)
        require(context['schema'] == 'infra-composer-context-v1' and context['mode'] == 'live', 'infra_context')
        manifest, evidence = paths(context)
        if args.operation in ('prepare', 'keeper'):
            if manifest.exists():
                current = upstream.read_manifest(manifest)
                require(all(current['infra'][key] == context[key] for key in
                    ('component', 'topology', 'profile', 'seconds', 'rates')), 'infra_context_changed')
            else:
                project(args.source, context, manifest, evidence)
            if args.operation == 'keeper':
                # Real S7 startup, custody, raw reader and original commands.
                # Composer already owns this foreground process via FIX-ORPHAN.
                return upstream.serve(upstream.read_manifest(manifest), evidence)
            return 0
        if args.operation == 'stop':
            result = call(manifest, evidence, 'stop')
            print(json.dumps(result))
            return result['native_rc']
        result = call(manifest, evidence, 'sample')
        require(result.get('native_rc') == 0, 'infra_s7_sample_failed')
        actual = upstream.read_manifest(manifest)
        require(all(all(t.get(key) for key in ('transfer_id', 'entry_nonce', 'object_version'))
                    for t in actual['transfers']), 'infra_identity_unbound')
        from hot_update_infra_workload import process_identity
        snapshot = json.loads(Path(result['snapshot']).read_bytes(), object_pairs_hook=unique_object)
        public = []
        for transfer in actual['transfers']:
            row = {key: transfer[key] for key in ('cohort', 'transfer_id', 'entry_nonce', 'session_id',
                'bucket', 'object_version', 'size', 'rate_limit', 'ctl_producer')}
            ctl = snapshot['processes'][transfer['ctl_producer']]
            row['ctl'] = process_identity(ctl['pid'], transfer['transfer_id'])
            require(row['ctl']['StartTicks'] == ctl['process_start'] and row['ctl']['BootID'] == ctl['boot_id']
                    and row['ctl']['ExeSHA256'] == ctl['running_sha'], 'infra_ctl_changed')
            row['started_ns'] = min(e['boottime_ns'] for e in snapshot['events']
                                    if e['transfer_id'] == transfer['transfer_id'])
            public.append(row)
        print(json.dumps(dict(manifest=str(manifest), evidence=str(evidence),
            manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
            reader_digest=upstream.reader_digest(), transfers=public)))
        return 0
    except (SetupError, ValueError, OSError, KeyError, TypeError):
        print('infra transfer: SETUP-RED missing or changed original custody', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
