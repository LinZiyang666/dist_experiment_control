#!/usr/bin/env -S python3 -I -B
"""Executable S9 owner ports; --plan FILE --plan-sha256 SHA --port NAME.

All calls take --context FILE. Keeper/stop also take --evidence DIR. CLI
commands exec the pinned product in place so P32's attempt sink FD survives.
Keepers retain the existing drill probes and their original shared waiters.

Physical and fault facts require an independent deployment-local instrument,
not a product Status projection. native.endpoint is a private Unix stream
socket, served by the SHA-pinned native.executable under the shared supervisor.
One newline JSON request is {schema,nonce,context_sha256,context,port,arguments};
one response has {schema,nonce,context_sha256,port,at_ns,payload}, schema
infra-native-observation-v1. The nonce is new on every call; at_ns is local
monotonic time within that call. Peer UID, executing SHA, source argv and birth
are checked around the response. Requests carry only public context/arguments.
The native producer must observe kernel/target facts or execute its named fault;
it must never read a stored expected receipt. Absence is a named NOT_RUN. This
adapter does not supply that deployment-specific privileged collector.

origin: hot-update S9 plan §2.3 / §9.1 / §9.4 / L22; S9 INSTRUMENTS-2.
"""
import argparse
import hashlib
import os
from pathlib import Path
import re
import secrets
import socket
import stat
import struct
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import infra_composer_contract as contract
import hot_update_infra_scope as scope
import hot_update_infra_workload as workload
import infra_owner_lifecycle as lifecycle
import infra_owner_plan as plans
import infra_owner_workloads as owners


def context(path):
    value = contract.read(path)
    plans.need(value.get('schema') == 'infra-composer-context-v1' and value.get('mode') == 'live'
        and set(value) == {'schema', 'mode', 'component', 'topology', 'profile', 'nodes', 'agents', 'ctl',
            'initial_sha256', 'seconds', 'rates', 'nonce', 'evidence', 'instance', 'hops'}, 'context', 'schema')
    plans.need(re.fullmatch(r'[0-9a-f]{32}', value['nonce']) and
        type(value['seconds']) is int and 0 < value['seconds'] <= 4320 and
        value['nodes'] == list(contract.verbs.TOPOLOGY[value['topology']]), 'context', 'arm_binding')
    lifecycle.private(value['evidence'])
    return value


def peer_identity(channel, native, port):
    pid, uid, _ = struct.unpack('3i', channel.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
    plans.need(uid == os.geteuid(), port, 'native_peer_uid')
    start = lifecycle.birth(pid)
    # Hash a held executable FD, not a pathname which might now name a later
    # binary. Recheck start and executable inode after the bytes are read.
    fd = os.open('/proc/' + str(pid) + '/exe', os.O_RDONLY | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        digest = hashlib.sha256()
        while True:
            part = os.read(fd, 1 << 20)
            if not part:
                break
            digest.update(part)
        after = os.stat('/proc/' + str(pid) + '/exe')
        plans.need((before.st_dev, before.st_ino) == (after.st_dev, after.st_ino) and
            digest.hexdigest() == native['executable']['sha256'] and lifecycle.birth(pid) == start,
            port, 'native_peer_executable')
    finally:
        os.close(fd)
    argv = (Path('/proc') / str(pid) / 'cmdline').read_bytes().split(b'\0')
    plans.need(all(pin['path'].encode() in argv for pin in native['inputs']
                   if pin['path'].endswith('.py')), port, 'native_peer_source')
    return pid, start, before.st_dev, before.st_ino


def native(plan, ctx, port, arguments, timeout=10):
    # origin: hot-update S9 plan §9.1 / L22; S9 INSTRUMENTS-3.
    # The in-tree producer runs in this already-supervised owner. Legacy
    # independently supplied collectors retain the exact authenticated RPC ABI.
    if 'deployment' in plan:
        import infra_native
        supervised(os.getpid(), plan['supervisor'], port)
        return infra_native.execute(plan, ctx, port, arguments, timeout)
    config = plan['native']
    endpoint = Path(config['endpoint'])
    plans.need(endpoint.exists() and stat.S_ISSOCK(endpoint.lstat().st_mode) and
        not endpoint.is_symlink(), port, 'independent_native_collector_absent')
    lifecycle.private(endpoint.parent)
    request = dict(schema='infra-native-observation-v1', nonce=secrets.token_hex(16),
        context_sha256=hashlib.sha256(contract.canonical(ctx)).hexdigest(), context=ctx,
        port=port, arguments=arguments)
    started = time.monotonic_ns()
    # As with owner controls, deep evidence paths cannot consume sun_path.
    directory = os.open(endpoint.parent, os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        with socket.socket(socket.AF_UNIX) as peer:
            peer.settimeout(timeout)
            peer.connect('/proc/self/fd/' + str(directory) + '/' + endpoint.name)
            identity = peer_identity(peer, config, port)
            supervised(identity[0], plan['supervisor'], port)
            peer.sendall(contract.canonical(request))
            response = lifecycle.receive(peer)
            plans.need(peer_identity(peer, config, port) == identity, port, 'native_peer_changed')
            supervised(identity[0], plan['supervisor'], port)
    finally:
        os.close(directory)
    return bound_response(response, request, started, time.monotonic_ns(), port)


def bound_response(response, request, started, ended, port):
    plans.need(type(response) is dict and set(response) ==
        {'schema', 'nonce', 'context_sha256', 'port', 'at_ns', 'payload'} and
        all(response[k] == request[k] for k in ('schema', 'nonce', 'context_sha256', 'port')) and
        type(response['at_ns']) is int and started <= response['at_ns'] <= ended,
        port, 'native_response_binding')
    return response['payload']


def supervised(pid, supervisor, port):
    # An executable pin alone does not own the collector's descendants after
    # caller death. Require a live shared-launcher ancestor, not an env claim.
    for _ in range(64):
        try:
            fields = (Path('/proc') / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()
            parent = int(fields[1])
            if parent <= 1 or parent == pid:
                break
            if contract.sha('/proc/' + str(parent) + '/exe') == supervisor['sha256']:
                return
        except OSError:
            break
        pid = parent
    plans.need(False, port, 'shared_collector_supervisor_required')


def business(value):
    plans.need(type(value) is dict and set(value) == set(contract.probe.ORIGINALS), 'reader', 'original_requiredset')
    for name, row in value.items():
        contract.probe.fields(row, 'business')
        plans.need({'identity', 'accepts', 'redials', 'tx', 'rx', 'tx_sha256', 'rx_sha256', 'fin', 'carrier'}
                   <= set(row), 'reader', 'business_fields')
        original_identity(name, row['identity'])
        scalars({key: item for key, item in row.items() if key not in ('identity', 'fin')})
        plans.need(type(row['fin']) is list and len(row['fin']) == 2 and
            all(type(item) is bool for item in row['fin']), 'reader', 'fin_fields')


def original_identity(name, identity):
    if name in ('pty', 'exec'):
        plans.need(set(identity) == set(contract.probe.SCHEMA_FIELDS['task_original'].split()), 'reader', 'task_fields')
        contract.probe.process(identity['ctl'])
        contract.probe.process(identity['child'])
    else:
        plans.need(set(identity) == set(contract.probe.SCHEMA_FIELDS['socket_original'].split()), 'reader', 'socket_fields')
        contract.probe.socket(identity['socket'])
    scalars({key: item for key, item in identity.items() if key not in ('ctl', 'child', 'socket')})


def scalars(value):
    # A known field name is not permission to embed an arbitrary private
    # subtree (for example fin={private_key:...}). Containers are checked by
    # their owner schema; every other value is a public scalar.
    plans.need(all(type(item) in (str, int, bool, type(None)) for item in value.values()),
               'reader', 'public_scalar_fields')


def census(value):
    contract.probe.fields(value, 'census')
    for row in value['processes']:
        contract.probe.process(row)
    scalars({key: item for key, item in value.items() if key != 'processes'})


def phase_lanes(value):
    for row in value:
        contract.probe.fields(row, 'phase_lane')
        scalars({key: item for key, item in row.items() if key != 'events'})
        for event in row['events']:
            contract.probe.validate_event(event)


def public(port, payload):
    """Reject private/unknown outer fields before printing a public receipt.

    P32 still judges every fact. Validation here cannot sign continuity, turn
    a false fault-hit into true or erase a nonzero native product status.
    """
    if port == 'owners.prepare':
        plans.need(payload == {'preconditions': True}, port, 'native_preconditions')
    elif port == 'owners.ready':
        plans.need(type(payload) is dict and set(payload) == contract.READY_FIELDS and
            payload['schema'] == 'infra-cohort-v1' and payload['producer'] == 'composer' and
            payload['phase'] == 'ready', port, 'ready_fields')
        plans.need(set(payload['originals']) == set(contract.probe.ORIGINALS), port, 'ready_requiredset')
        for name, original in payload['originals'].items():
            plans.need(set(original) == {'identity', 'carrier'}, port, 'ready_original_fields')
            original_identity(name, original['identity'])
            scalars({'carrier': original['carrier']})
        for attachment in payload['attachments'].values():
            contract.probe.attachment(attachment)
        for row in payload['transfers']:
            contract.probe.fields(row, 'transfer')
            contract.probe.process(row['ctl'])
            scalars({key: item for key, item in row.items() if key != 'ctl'})
        plans.need(set(payload['s7']) == {'manifest', 'evidence'} and
            set(payload['flow']) == {'evidence', 'nonce'} and type(payload['phase_lanes']) is dict,
            port, 'ready_binding_fields')
        for key in ('s7', 'flow', 'phase_lanes'):
            scalars(payload[key])
        scalars({key: item for key, item in payload.items() if key not in
                 ('originals', 'attachments', 'transfers', 's7', 'flow', 'phase_lanes')})
    elif port == 'readers.sample':
        contract.probe.fields(payload, 'sample')
        plans.need(payload.get('producer') == 'kernel-and-target-reader' and 's7' not in payload,
                   port, 'sample_producer')
        business(payload['business'])
        for row in payload['attachments'].values():
            contract.probe.attachment(row)
        for row in payload['accepted'].values():
            contract.probe.socket(row)
        for row in payload.get('translations', {}).values():
            contract.probe.fields(row, 'translation')
            scalars(row)
        for row in payload['prefix_reads']:
            contract.probe.fields(row, 'prefix')
            for chunk in row['expected'] + row['actual']:
                plans.need(set(chunk) == {'index', 'length', 'sha256'}, port, 'prefix_fields')
                scalars(chunk)
            scalars({key: item for key, item in row.items() if key not in ('expected', 'actual')})
        for name in ('before', 'current'):
            if name in payload:
                contract.probe.process(payload[name])
        for row in payload.get('temporary', []):
            contract.probe.process(row)
        if 'census' in payload:
            census(payload['census'])
        if 'main_ready' in payload:
            contract.probe.socket(payload['main_ready'])
        scalars({key: item for key, item in payload.items() if key not in
            ('business', 'attachments', 'accepted', 'prefix_reads', 'translations', 'before', 'current', 'temporary', 'census', 'main_ready')})
    elif port == 'readers.progress':
        plans.need(set(payload) == {'at_ns', 'producer', 'business'} and
            payload['producer'] == 'original-target-reader', port, 'progress_fields')
        business(payload['business'])
        scalars({key: item for key, item in payload.items() if key != 'business'})
    elif port == 'readers.final':
        plans.need(set(payload) == {'business', 'controls', 'phase_lanes'}, port, 'final_fields')
        business(payload['business'])
        for row in payload['controls']:
            contract.probe.fields(row, 'control')
            scalars(row)
        phase_lanes(payload['phase_lanes'])
    elif port == 'readers.effects':
        plans.need(type(payload) is list, port, 'effects_fields')
        for row in payload:
            plans.need(type(row) is dict and set(row) == {'logical_id', 'effect'} and row['effect'] == 'applied' and
                re.fullmatch(r'[0-9a-f]{32}-[0-9]{1,8}', row['logical_id']), port, 'effect_fields')
    elif port == 'readers.node':
        plans.need(set(payload) == scope.NODE_FIELDS and payload['producer'] == 'kernel-reader', port, 'node_fields')
        contract.probe.process(payload['server'])
        contract.probe.process(payload['broker'])
        for row in payload['temporary']:
            contract.probe.process(row)
        census(payload['census'])
        plans.need(set(payload['invocation']) == {'user', 'restart', 'managed'}, port, 'invocation_fields')
        scalars(payload['invocation'])
        scalars({key: item for key, item in payload.items() if key not in ('server', 'broker', 'temporary', 'census', 'invocation')})
    elif port.startswith('tls.'):
        plans.need({'accepted'} <= set(payload) <= {'accepted', 'translation', 'bypass', 'main'}, port, 'tls_fields')
        contract.probe.socket(payload['accepted'])
        if 'main' in payload:
            contract.probe.process(payload['main'])
        if 'translation' in payload:
            contract.probe.fields(payload['translation'], 'translation')
            scalars(payload['translation'])
        if 'bypass' in payload:
            plans.need(type(payload['bypass']) is bool, port, 'tls_bypass_fields')
    elif port.startswith('faults.'):
        action = port.split('.')[-1]
        fields = {'install': {'point', 'installed', 'nodes', 'at_ns', 'mechanism'},
            'status': scope.FAULT_STATUS_FIELDS, 'heal': {'point', 'healed', 'topology', 'nodes', 'at_ns'}}[action]
        plans.need(set(payload) == fields and payload['point'] == port.split('.')[1], port, 'fault_fields')
        if action == 'status':
            plans.need(all(type(payload[key]) is bool for key in ('active', 'sent', 'effect', 'reply_cut')) and
                contract.probe.uint(payload['hits']) and (payload['digest'] is None or contract.probe.digest(payload['digest'])),
                port, 'fault_status_fields')
        else:
            plans.need(payload['nodes'] and set(payload['nodes']) <= {'brk1', 'brk2', 'brk3'} and
                contract.probe.uint(payload['at_ns'], 1), port, 'fault_node_fields')
            if action == 'install':
                plans.need(type(payload['installed']) is bool and
                    re.fullmatch(r'[a-zA-Z0-9_:.-]+', payload['mechanism']), port, 'fault_mechanism_fields')
            else:
                plans.need(type(payload['healed']) is bool and payload['topology'] in contract.verbs.TOPOLOGY,
                    port, 'fault_heal_fields')
    elif port.startswith('exercises.'):
        plans.need(set(payload) == scope.EXERCISE_FIELDS and payload['scenario'] == port.split('.')[1], port, 'exercise_fields')
        for row in payload['rows']:
            plans.need(set(row) == scope.ROW_FIELDS and isinstance(row['code'], str) and
                re.fullmatch(r'[a-zA-Z0-9_:.-]*', row['code']), port, 'exercise_row')
            scalars(row)
        if payload['holder'] is not None:
            contract.probe.process(payload['holder'])
        plans.need(type(payload['retired']) is bool, port, 'exercise_retirement_fields')
    elif port.startswith('configs.'):
        plans.need(set(payload) == {'variant', 'staged', 'sha256'} and
            payload['variant'] == port.split('.')[1] and payload['staged'] is True and
            contract.probe.digest(payload['sha256']), port, 'config_fields')
    return payload


def command(plan, kind, logical):
    plans.need(re.fullmatch(r'[0-9a-f]{32}-[0-9]{1,8}', logical or ''), 'commands.' + kind, 'logical_id_required')
    row = plan['product']
    if kind == 'node_ls':
        return owners.cli(plan, ['node', 'ls', '--json'])
    if kind == 'ps':
        return owners.cli(plan, ['ps', '--json'])
    suffix = ['cat', row['read_path']] if kind == 'read' else [row['remote_python'], '-I', '-B',
        str(plans.LIB / 'infra_request_effect.py'), kind, '--root', row['effect_root'], '--logical-id', logical]
    return owners.cli(plan, ['exec', '--timeout', '5s']) + [row['node'], '--', *suffix]


def prepare(plan, ctx):
    for argv, field in ((owners.cli(plan, ['node', 'ls', '--json']), 'nodes'),
                        (owners.cli(plan, ['ps', '--json']), 'processes')):
        row = workload.decode(owners.run(plan, argv))
        plans.need(type(row.get(field)) is list and (field != 'nodes' or row[field]), 'owners.prepare', 'product_readiness')
    for kind, local in (('pty', 'hot_update_pty_probe.py'), ('exec', 'hot_update_process_probe.py')):
        raw = owners.remote(plan, 'sha256sum', plan[kind]['remote_probe'])
        plans.need(raw.split()[0].decode() == contract.sha(plans.LIB / local), 'owners.prepare', kind + '_remote_source_sha')
    raw = owners.remote(plan, 'sha256sum', str(plans.LIB / 'infra_request_effect.py'))
    plans.need(raw.split()[0].decode() == contract.sha(plans.LIB / 'infra_request_effect.py'), 'owners.prepare', 'effect_remote_source_sha')
    plans.need(owners.remote(plan, 'cat', plan['product']['read_path']).strip() == b'infra-read-v1',
               'owners.prepare', 'public_read_marker')
    return public('owners.prepare', native(plan, ctx, 'owners.prepare', [], 120))


def execute(args):
    plan = plans.load(args.plan, args.plan_sha256)
    plans.prerequisites(plan, args.port)
    ctx = context(args.context)
    digest = contract.sha(args.context)
    supervisor = plan['supervisor']['path']
    plans.need(os.environ.get('SIM_UPGRADE_SUPERVISOR') == supervisor, args.port, 'supervisor_binding')
    if args.port.startswith('commands.'):
        argv = command(plan, args.port.split('.')[1], args.logical_id)
        # P32 has already supervised this invocation and opened its attempt
        # sink. exec preserves the sink, logical ID, native rc and original PID.
        os.execv(argv[0], argv)
    if args.port == 'owners.prepare':
        return prepare(plan, ctx)
    if args.port.startswith('owners.workloads.'):
        _, _, kind, action = args.port.split('.')
        root = lifecycle.private(args.evidence)
        plans.need(root == Path(ctx['evidence']) / kind, args.port, 'foreign_evidence')
        if action == 'stop':
            return lifecycle.request(root, digest, 'stop')
        lifecycle.signals()
        owner = owners.Flows(plan, ctx, root) if kind == 'flow' else owners.Task(plan, ctx, root, kind)
        def intact():
            plans.need(contract.sha(args.context) == digest, args.port, 'context_changed')
            plans.load(args.plan, args.plan_sha256)
            owner.intact()
        lifecycle.Lease(root, digest, ctx['seconds']).serve(owner.start, owner.drain, owner.close, intact, owner.snapshot)
        return None
    if args.port == 'owners.ready':
        deadline = time.monotonic() + 60
        for kind in ('flow', 'pty', 'exec'):
            root = Path(ctx['evidence']) / kind
            while not (root / 'lease.json').exists():
                plans.need(time.monotonic() < deadline, args.port, 'original_owner_ready_deadline_' + kind)
                time.sleep(.05)
            state = contract.read(root / 'lease.json')
            plans.need(state['context_sha256'] == digest and lifecycle.birth(state['pid']) == state['start_ticks'],
                       args.port, 'stale_owner_' + kind)
    if args.port == 'readers.effects':
        raw = owners.remote(plan, 'cat', str(Path(plan['product']['effect_root']) / 'effects.jsonl'))
        plans.need(not raw or raw.endswith(b'\n'), args.port, 'truncated_effect_ledger')
        return public(args.port, [workload.decode(line) for line in raw.splitlines()])
    if args.port == 'readers.final':
        # Capture all remaining original bytes before the native final reader.
        # drain retains owner services until the composer's explicit stop.
        for kind in ('flow', 'pty', 'exec'):
            lifecycle.request(Path(ctx['evidence']) / kind, digest, 'drain', 60)
    arguments = args.arguments + (['--evidence', args.evidence] if args.evidence is not None else [])
    if args.port.startswith('configs.'):
        variant = args.port.split('.')[1]
        # Stage the real pinned public config through the deployment's catalog
        # instrument. A local copy is not evidence of product staging.
        arguments = arguments + ['--source', plan['configs'][variant]['path'],
                                 '--source-sha256', plan['configs'][variant]['sha256']]
    timeout = 10
    if args.port == 'readers.sample' or args.port.startswith('configs.') or args.port.endswith('.install'):
        timeout = 120
    if args.port.endswith('.heal'):
        timeout = 300
    if args.port.startswith('exercises.') or args.port == 'readers.final':
        timeout = 600
    payload = public(args.port, native(plan, ctx, args.port, arguments, timeout))
    if args.port.startswith('configs.'):
        plans.need(payload['sha256'] == plan['configs'][args.port.split('.')[1]]['sha256'], args.port, 'staged_config_sha')
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', required=True)
    parser.add_argument('--plan-sha256', required=True)
    parser.add_argument('--port', required=True, choices=plans.PORTS)
    parser.add_argument('--context', required=True)
    parser.add_argument('--evidence')
    parser.add_argument('--logical-id')
    args, args.arguments = parser.parse_known_args()
    try:
        value = execute(args)
        if value is not None:
            sys.stdout.buffer.write(contract.canonical(value))
        return 0
    except Exception as error:
        # Existing drill probes also define their own custody exception types.
        # At this instrument boundary they are SETUP-RED with a fixed reason;
        # an exception's argv or raw reader body must never escape to stderr.
        message = str(error) if type(error) is ValueError and str(error).startswith('NOT_RUN ') else 'NOT_RUN ' + args.port + ': input_or_custody_failed'
        print(message, file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
