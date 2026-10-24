#!/usr/bin/env python3
"""Project a driver deployment into the fixture's frozen schema and await start.

Custody identities, canonical records, streams and resource schedules come from
the deployment owner. This adapter never invents product observations or IDs.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from transfer_fixture import read_manifest
from transfer_mutations import check_manifest, load, require, requirements


# origin: S7 internal review rv-drill F2. One frozen matrix drives scheduler
# and observer. The deployment binds names, but cannot choose fewer cells.
def observer_requirements(plan, transfers, processes):
    cuts, generations = [], {}
    brokers = [p for p in processes if p.get('role') == 'broker']
    for step in requirements(plan['arm'], plan['topology']):
        owners = []
        for transfer in transfers:
            cohort = transfer['cohort']
            if cohort not in step['cohorts']:
                continue
            role = step.get('roles', {}).get(cohort)
            if 'hops' in step:
                require(len(brokers) == len(step['hops']), 'observer broker hops')
                targets = [(next(p['label'] for p in brokers if p.get('node') == node['node']), hop)
                           for node, hop in zip(plan['brokers'], step['hops'])]
            else:
                targets = [(transfer['agent_producer'], 'agent')]
            for producer, hop in targets:
                owners.append((producer, hop, transfer, role))
        for producer in {item[0] for item in owners}:
            generations[producer] = generations.get(producer, 1) + 1
        for producer, hop, transfer, role in owners:
            cuts.append(dict(arm=plan['arm'], topology=plan['topology'], cohort=transfer['cohort'],
                hop=hop, cut_id=step['id'], transfer_id=transfer['transfer_id'], leg='leg' + str(step['leg']),
                producer=producer, role=role, generation=generations[producer],
                max_after_first_ns=step['latest_s'] * 1_000_000_000))
    return cuts


def resource_requirements(transfer, cuts):
    refs, result = transfer.get('resource_refs', {}), {}
    for cut in cuts:
        if cut['transfer_id'] != transfer['transfer_id']:
            continue
        needed = {'sender': ('source',), 'receiver': ('tmp', 'parent'), 'prepared': ('parent',),
                  'courier': (), 'tracker': (), 'pending-data': ()}.get(cut['role'])
        require(needed is not None, 'unknown observer role')
        for kind in needed:
            require(isinstance(refs.get(kind), str) and refs[kind], 'required resource reference')
            result.setdefault(refs[kind], []).append(cut['cut_id'])
    return {key: sorted(set(value)) for key, value in result.items()}


# origin: S7 internal review rv-drill F5. Each rollback cell declares an
# independent original invocation; a tier label cannot turn bulk into inline.
def validate_rollback_tasks(plan):
    tasks = plan.get('rollback_tasks', [])
    require(len(tasks) == len(plan['steps']) and {t['cell'] for t in tasks} ==
            {s['id'] for s in plan['steps']}, 'rollback task matrix')
    require(len({t['transfer']['transfer_id'] for t in tasks}) == len(tasks), 'rollback invocation reuse')
    for task in tasks:
        step = next(s for s in plan['steps'] if s['id'] == task['cell'])
        transfer = task['transfer']
        require(all(task[k] == step[k] for k in ('tier', 'direction', 'layer')), 'rollback task cell')
        require(0 < transfer['size'] <= 4096 if task['tier'] == 'A' else transfer['size'] >= 66549440,
                'rollback actual tier')
        require(transfer.get('source') and transfer.get('ctl_producer') and transfer.get('launch'), 'rollback original command')
        argv = transfer['launch']['argv']
        require(task['direction'] in argv and transfer['source'] in argv and
                transfer['target'] in argv, 'rollback command binding')
    return tasks


# origin: hot-update S6 plan §10.1 / T58; DRILL-ASSETS / fixture schema is the contract.
def write_manifest(driver, output):
    plan = check_manifest(load(driver))
    spec = plan.get('fixture')
    if spec is None:
        # make_manifest retains deployment fields while replacing schema with
        # transfer-driver-v1. Accept an existing flat fixture deployment too;
        # project its reader fields rather than forwarding driver-only keys.
        spec = {key: plan[key] for key in ('mode', 'processes', 'transfers', 'cuts', 'nats', 'control') if key in plan}
        spec['schema'] = 1
    require(isinstance(spec, dict) and spec.get('schema') == 1 and spec.get('mode') == 'live',
            'missing live fixture deployment')
    value = {key: copy.deepcopy(spec[key]) for key in
             ('schema', 'mode', 'quiet_seconds', 'processes', 'transfers', 'cuts', 'nats', 'control') if key in spec}
    value['arm'], value['quiet_seconds'] = plan['arm'], plan['quiet_window_s']
    expected = observer_requirements(plan, value['transfers'], value['processes'])
    require(sorted(value['cuts'], key=lambda row: json.dumps(row, sort_keys=True)) ==
            sorted(expected, key=lambda row: json.dumps(row, sort_keys=True)), 'observer matrix drift')
    for transfer in value['transfers']:
        require(transfer['required_resources'] == resource_requirements(transfer, expected), 'observer resource drift')
    value['rollback_tasks'] = copy.deepcopy(plan.get('rollback_tasks', []))
    value['driver'] = copy.deepcopy(plan)
    value['driver_sha'] = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
    transfers = {t['cohort']: t for t in value['transfers']}
    require(len(value['transfers']) == 4 and set(transfers) == {c['label'] for c in plan['cohorts']},
            'fixture cohort binding')
    for cohort in plan['cohorts']:
        transfer = transfers[cohort['label']]
        # A canonical transfer ID/nonce cannot be derived from a node or SID.
        # Require the predeclared fixture data, retaining it byte for byte.
        require(all(transfer.get(key) == cohort[source] for key, source in (
            ('session_id', 'sid'), ('bucket', 'bucket'), ('target', 'destination'),
            ('size', 'size'), ('sha256', 'sha256'), ('rate_limit', 'rate_limit_bps'))),
            'fixture transfer drift')
    agents = [p for p in value['processes'] if p.get('role') == 'agent']
    require({(p.get('home'), p.get('binary_path')) for p in agents} ==
            {(c['home'], c['binary_path']) for c in plan['cohorts']}, 'fixture installation drift')
    # Control builds bind A/B before this projection. The keeper must observe
    # those actual readers, rather than the deployment's pre-mutation SHAs.
    shas = sorted({a['sha256'] for a in plan['artifacts'].values()})
    for process in value['processes']:
        process['artifact_shas'] = shas
    output = Path(output)
    pending = output.with_suffix('.pending')
    try:
        pending.write_text(json.dumps(value, sort_keys=True) + '\n')
        os.chmod(pending, 0o600)
        read_manifest(pending)  # The real reader, not a second schema validator.
        pending.replace(output)
    finally:
        pending.unlink(missing_ok=True)


# start is a foreground keeper. Read its flushed startup receipt without
# mistaking the long-lived process's eventual exit for startup success.
# origin: hot-update S6 plan §10.1 / T58; DRILL-ASSETS / foreground fixture lifetime.
def wait_start(manifest, log, receipt, supervisor_pid, timeout):
    expected = hashlib.sha256(Path(manifest).read_bytes()).hexdigest()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        require(not Path(receipt).exists() or not Path(receipt).read_text().strip(),
                'fixture exited before startup')
        os.kill(supervisor_pid, 0)
        if Path(log).exists():
            for line in Path(log).read_text().splitlines():
                try:
                    ready = json.loads(line)
                except ValueError:
                    continue
                if 'fixture_pid' in ready:
                    require(ready.get('manifest_sha256') == expected and
                            ready.get('custody_refs', {}).get('control') == str(Path(log).parent / 'control.sock'),
                            'fixture startup binding')
                    require(type(ready['fixture_pid']) is int and ready['fixture_pid'] > 0,
                            'fixture startup identity')
                    os.kill(ready['fixture_pid'], 0)
                    return
        time.sleep(.02)
    require(False, 'fixture startup deadline')


def wait_stop(receipt, supervisor_pid, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if Path(receipt).exists() and Path(receipt).read_text().strip():
            int(Path(receipt).read_text().strip())
            return
        os.kill(supervisor_pid, 0)
        time.sleep(.02)
    require(False, 'fixture stop deadline')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    project = sub.add_parser('manifest')
    project.add_argument('--manifest', required=True, type=Path)
    project.add_argument('--output', required=True, type=Path)
    start = sub.add_parser('wait-start')
    for name in ('manifest', 'log', 'receipt'):
        start.add_argument('--' + name, required=True, type=Path)
    start.add_argument('--pid', required=True, type=int)
    start.add_argument('--timeout', type=float, default=10)
    stop = sub.add_parser('wait-stop')
    stop.add_argument('--receipt', required=True, type=Path)
    stop.add_argument('--pid', required=True, type=int)
    stop.add_argument('--timeout', type=float, default=15)
    args = parser.parse_args()
    if args.command == 'manifest':
        write_manifest(args.manifest, args.output)
    elif args.command == 'wait-start':
        wait_start(args.manifest, args.log, args.receipt, args.pid, args.timeout)
    else:
        wait_stop(args.receipt, args.pid, args.timeout)


# origin: S7 internal review rv-drill F1. This is a read-only projection of the
# custody archive, not a supplier of ready flags or a second product scheduler.
def scheduler_sample(manifest, snapshot, sequence):
    from transfer_oracle import responsibility, progress
    plan, events = manifest.get('driver'), snapshot['events']
    result = dict(manifest_sha=manifest.get('driver_sha'), sequence=sequence, windows={},
                  connections={'carrier': [], 'data': []}, watchdog={}, victims={}, rollback_tasks={})
    result['connections'] = snapshot.get('connections', result['connections'])
    if not plan:
        return result
    now = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
    transfers = {t['cohort']: t for t in manifest['transfers']}
    for step in plan['steps']:
        rows = []
        for cohort in step['cohorts']:
            transfer = transfers[cohort]
            relevant = [e for e in events if e['transfer_id'] == transfer['transfer_id'] and e['leg'] == 'leg' + str(step['leg'])]
            data = [e for e in relevant if e['kind'] in ('append', 'synced')]
            cuts = [c for c in manifest['cuts'] if c['cut_id'] == step['id'] and c['cohort'] == cohort]
            role = step.get('roles', {}).get(cohort)
            retained = all(any(e['producer'] == c['producer'] and responsibility(e, c['role'])
                               and e['generation'] == c['generation'] - 1 for e in relevant) for c in cuts)
            latest = max(data, key=lambda e: e['boottime_ns']) if data else None
            elapsed = (now - min(e['boottime_ns'] for e in data)) / 1e9 if data else 0
            complete = bool(latest and progress(latest) >= transfer['size'])
            rows.append(dict(label=cohort, leg=step['leg'], role=role, ready=bool(data and cuts and retained),
                elapsed_s=elapsed, complete=complete, durable_bytes=progress(latest) if latest else 0,
                size=transfer['size'], pending_responsibilities=int(retained)))
        result['windows'][step['id']] = rows
    for task in manifest.get('rollback_tasks', []):
        transfer = task['transfer']
        own = [e for e in events if e['transfer_id'] == transfer['transfer_id']]
        phase = ('prepared', 'receiving') if task['direction'] == 'push' else ('prepared', 'sending')
        pending = [e for e in own if e['phase'] in phase and e['kind'] in ('control', 'append', 'fetched', 'synced')]
        result['rollback_tasks'][task['cell']] = dict(tier=task['tier'], direction=task['direction'],
            ready=bool(pending) and not any(e['kind'] in ('primitive_exit', 'retirement') for e in own))
    result['window_expired'] = False
    for arm in ('W1', 'W2'):
        rollbacks = [e for e in events if e['kind'] == 'rollback' and e['cut_id'] == arm]
        starts = [e for e in events if e['kind'] == 'freeze' and e['cut_id'] == arm]
        if rollbacks and starts:
            result['watchdog'][arm] = dict(current_cut_observed=True,
                elapsed_s=(max(e['boottime_ns'] for e in rollbacks) - min(e['boottime_ns'] for e in starts)) / 1e9,
                pending_data=all(e['phase'] in ('sending', 'receiving') for e in starts))
    for victim in plan.get('victims', []):
        identity = snapshot['processes'].get(victim.get('ctl_producer'))
        if identity:
            result['victims'][victim['kind']] = dict(ctl_pid=identity['pid'], **identity)
    return result


# origin: S7 internal review rv-drill F1 / L16. The shared supervisor launches
# this bootstrap; custody is leased afterwards so no CLI ExtraFiles extension
# or unsafe preexec_fn is needed. exec keeps the original invocation PID.
def exec_owner(control, manifest_sha, label, argv, home):
    import array
    import socket
    from transfer_observation import receive
    with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as channel:
        channel.settimeout(10)
        if len(control.encode()) >= 108:
            from infra_transfer_fixture import unix_call
            unix_call(channel, 'connect', Path(control).parent)
        else:
            channel.connect(control)
        channel.send(json.dumps(dict(command='lease', manifest_sha256=manifest_sha, label=label)).encode())
        raw, fds = receive(channel)
        try:
            require(json.loads(raw).get('native_rc') == 0 and len(fds) == 2, 'launch custody')
            # Duplicate out of the destination range before installing fd3/4:
            # recvmsg is allowed to return either number in either order.
            import fcntl
            held = []
            try:
                for fd in fds:
                    held.append(fcntl.fcntl(fd, fcntl.F_DUPFD_CLOEXEC, 10))
            except BaseException:
                for fd in held:
                    os.close(fd)
                raise
        finally:
            for fd in fds:
                os.close(fd)
    try:
        os.dup2(held[0], 3, inheritable=True)
        os.dup2(held[1], 4, inheritable=True)
    finally:
        for fd in held:
            os.close(fd)
    env = dict(os.environ, HOME=home, TETHER_TEST_DRILL_OBSERVER='v1', TETHER_TEST_DRILL_CUSTODY_FD='4')
    os.execve(argv[0], argv, env)


class LaunchOwner:
    """One owner of original commands, their native exits and bounded joins.

    Only the shared supervisor creates the process tree. This owner never
    resumes a transfer, invokes an upgrade, or supplies product observations.
    """
    def __init__(self, fixture):
        import threading
        self.fixture, self.children = fixture, []
        self.lock = threading.RLock()

    def start(self):
        for spec in self.fixture.manifest['processes']:
            if not spec.get('cell'):
                self.launch(spec)

    def start_cell(self, cell):
        tasks = [t for t in self.fixture.manifest.get('rollback_tasks', []) if t['cell'] == cell]
        require(len(tasks) == 1, 'rollback cell launch')
        transfer = tasks[0]['transfer']
        require(not any(spec['label'] == transfer['ctl_producer'] for spec, _, _ in self.children), 'rollback invocation restarted')
        from transfer_oracle import digest_file
        with open(transfer['source'], 'rb') as source:
            require(digest_file(source.fileno()) == (transfer['size'], transfer['sha256']), 'rollback original source')
        transfer['launched_ns'] = time.monotonic_ns()
        self.launch(next(p for p in self.fixture.manifest['processes'] if p['label'] == transfer['ctl_producer']))

    def launch(self, spec):
        import subprocess
        fixture = self.fixture
        supervisor = os.environ.get('TRANSFER_PROCESS_SUPERVISOR') or os.environ.get('SIM_UPGRADE_SUPERVISOR')
        require(supervisor and Path(supervisor).is_absolute() and os.access(supervisor, os.X_OK), 'launch supervisor')
        launch = spec.get('launch')
        require(isinstance(launch, dict), 'launch command missing')
        argv, home = launch['argv'], launch['home']
        require(argv and Path(argv[0]).is_absolute() and Path(home).is_absolute() and
                Path(home) != Path.home() and '.tether' not in Path(home).parts, 'launch isolation')
        code = ('import sys; sys.path.insert(0,sys.argv[1]); '
                'from transfer_fixture_manifest import exec_owner; '
                'exec_owner(sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[6:],sys.argv[5])')
        log = fixture.keeper.evidence / ('owner-' + spec['label'] + '.out')
        receipt = fixture.keeper.evidence / ('owner-' + spec['label'] + '.rc')
        with open(os.devnull, 'wb') as sink:
            child = subprocess.Popen([supervisor, '--log', str(log), '--receipt', str(receipt), '--',
                sys.executable, '-I', '-B', '-c', code, str(Path(__file__).parent),
                str(fixture.keeper.evidence / 'control.sock'), fixture.manifest['manifest_sha256'],
                spec['label'], home, *argv], stdout=sink, stderr=sink)
        self.children.append((spec, child, receipt))

    def sample(self):
        for spec, child, receipt in self.children:
            code = child.poll()
            if code is None:
                continue
            identity = self.fixture.keeper.processes.get(spec['label'])
            require(identity and receipt.exists() and int(receipt.read_text()) == code, 'launch native identity')
            for transfer in self.fixture.manifest['transfers']:
                if transfer.get('ctl_producer') == spec['label']:
                    self.fixture.keeper.save('native-' + spec['label'] + '.json',
                        dict(identity, native_rc=code, transfer_id=transfer['transfer_id']))
                    transfer['ctl_receipt'] = str(self.fixture.keeper.evidence / ('native-' + spec['label'] + '.json'))

    def close(self):
        import subprocess
        failed = False
        for _, child, _ in self.children:
            if child.poll() is None:
                child.terminate()
            try:
                code = child.wait(timeout=10)
                failed |= code in (125, 126)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
                failed = True
        self.children.clear()
        require(not failed, 'launch join')


# origin: S7 internal review rv-drill F1. A writer archives actual keeper and
# independent reader inputs. The kill reader verifies their digests again.
def mutation_receipts(fixture, verdict):
    root, keeper = fixture.keeper.evidence, fixture.keeper
    if not (root / 'mutation/source.json').exists():
        return
    build = load(root / 'mutation/source.json')
    binary = load(root / 'mutation/build.json')
    flags = (root / 'mutation/goflags.txt').read_text().strip()
    roles, processes = set(build['roles']), []
    for spec in fixture.manifest['processes']:
        actual = keeper.processes.get(spec['label'])
        if spec.get('role') in roles and actual:
            processes.append(dict(actual, role=spec['role'], artifact_sha=actual['running_sha']))
    keeper.save('mutation-runtime.json', dict(mutation_id=build['mutation_id'], build_selector=flags, processes=processes))
    keeper.save('mutation-raw.json', keeper.sample())
    keeper.save('mutation-oracle.json', {name: scan.rows for name, scan in fixture.scans.items()})
    mutation = build['mutation_id']
    criteria = {
        'M01': ('physical_object:', 'append'), 'M02': ('physical_object:', 'append'),
        'M03': ('physical_object:', 'finalize'), 'M04': ('physical_object:', 'fetched'),
        'M05': ('physical_object:', 'reader_stop'), 'M06': ('canonical_history:', 'reserve'),
        'M07': ('canonical_history:', 'control'), 'M08A': ('physical_object:', 'synced'),
        'M08B': ('physical_object:', 'synced'), 'M08C': ('physical_object:', 'synced'),
        'M09': ('ofd_reopened:', 'import'), 'M10A': ('duplicate_primitive:', 'primitive_exit'),
        'M10B': ('partial_destination:', 'primitive_exit'), 'M11A': ('canonical_history:', 'install'),
        'M11B': ('canonical_history:', 'ledger_sync'), 'M11C': ('canonical_history:', 'install'),
        'M12A': ('canonical_history:', 'acknowledge'), 'M12B': ('canonical_history:', 'history'),
        'M12C': ('opposite_terminal:', 'finish'), 'M13A': ('physical_object:', 'retirement'),
        'M13B': ('physical_object:', 'retirement'), 'M13C': ('physical_object:', 'retirement'),
        'M14': ('physical_object:', 'control')}
    prefix, kind = criteria[mutation]
    witness = dict(mutation_id=mutation, native_rc=verdict['native_rc'], criterion=prefix,
        exercised=any(f.startswith(prefix) for f in keeper.failures) and
                  any(e['kind'] == kind for e in keeper.records.events), reader_digest=verdict['reader_digest'])
    for field, name in (('raw', 'mutation-raw.json'), ('oracle_input', 'mutation-oracle.json')):
        witness[field + '_path'] = name
        witness[field + '_sha'] = hashlib.sha256((root / name).read_bytes()).hexdigest()
    if mutation in ('M02', 'M12B'):
        configs = [scan.config for scan in fixture.scans.values() if scan.config]
        if configs and all(config.get('duplicate_window') == 120_000_000_000 for config in configs):
            witness['default_dedup_ns'] = 120_000_000_000
            witness['elapsed_s'] = (time.monotonic_ns() - fixture.started_ns) / 1e9
    keeper.save('mutation-witness.json', witness)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError):
        print('transfer fixture adapter: setup failed', file=sys.stderr)
        sys.exit(2)
