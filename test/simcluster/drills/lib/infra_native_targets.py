#!/usr/bin/env -S python3 -I -B
"""Original owner, S7 custody and kernel joins for deployment-native readers.

participants maps each original participant to {node, ingress_node, reader}, where reader is
its public NC-attachment export instrument ({argv,sha256,inputs}). The export
is never trusted for physical identity: both its Raw and accepted socket must
occur in independently sampled P02 censuses. Missing product generation/cid
exports cannot be reconstructed from a URL, Status or a caller-supplied row.
repair_fence and phase_lanes likewise require actual P18/S7 observation ports;
absence is named rather than replaced by an expected transition timeline.
origin: hot-update S9 plan §2.3 / §4.3 / §9.1; S9 INSTRUMENTS-3.
"""
from concurrent.futures import ThreadPoolExecutor
import hashlib
from pathlib import Path
import re
import time

import infra_composer_contract as contract
import infra_owner_lifecycle as lifecycle
import infra_owner_plan as plans
import hot_update_infra_workload as workload
import infra_native as native

FLOW_NAMES = {'ssh': 'ssh', 'tcp-active': 'tcp-0', 'tcp-idle': 'tcp-1',
              'tcp-half-close': 'half-close', 'tcp-reverse': 'tcp-3',
              'ss-alice': 'alice', 'ss-bob': 'bob'}


def conntrack_translation(raw, local, remote, netns, generation):
    from ipaddress import ip_address
    def endpoint(value):
        if value.startswith('['):
            host, port = value[1:].split(']:')
        else:
            host, port = value.rsplit(':', 1)
        return ip_address(host), int(port)
    def address(host, port):
        host = str(ip_address(host))
        return ('[' + host + ']' if ':' in host else host) + ':' + port
    wanted = (endpoint(local), endpoint(remote))
    matches = []
    plans.need(type(netns) is int and netns > 0 and type(generation) is int and generation > 0,
               'tls', 'conntrack_namespace_required')
    for line in raw.decode('ascii').splitlines():
        values = re.findall(r'\b(src|dst|sport|dport)=([^ ]+)', line)
        if len(values) != 8 or [k for k, _ in values] != ['src', 'dst', 'sport', 'dport'] * 2:
            continue
        first = dict(values[:4]); reply = dict(values[4:])
        source, destination = address(first['src'], first['sport']), address(first['dst'], first['dport'])
        if (endpoint(source), endpoint(destination)) != wanted:
            continue
        matches.append(dict(producer='kernel-conntrack-reader', generation=generation, netns=netns,
            original_local=local, original_remote=remote,
            translated_local=address(reply['dst'], reply['dport']),
            translated_remote=address(reply['src'], reply['sport'])))
    plans.need(len(matches) == 1, 'tls', 'conntrack_tuple_ambiguous')
    return matches[0]


def owners(ctx):
    digest = contract.sha(Path(ctx['evidence']) / 'context.json')
    with ThreadPoolExecutor(max_workers=3) as workers:
        reads = {kind: workers.submit(lifecycle.request, Path(ctx['evidence']) / kind, digest, 'sample', 30)
                 for kind in ('flow', 'pty', 'exec')}
        return {kind: read.result() for kind, read in reads.items()}


def socket_from_identity(plan, config, node, identity):
    observed = native.physical(plan, config, node, 'census')
    matches = [dict(row, Owner=native.process(row['Owner'])) for row in observed['Sockets'] or []
               if row['Inode'] == identity['ino'] and row['Device'] == identity['dev'] and
               row['Owner']['PID'] == identity['pid'] and row['Owner']['StartTicks'] == identity['start_ticks']]
    plans.need(len(matches) == 1, 'reader', 'original_socket_not_in_kernel_census')
    return matches[0]


def file_stats(plan, config, node, path):
    value = native.physical(plan, config, node, 'file', path=str(path))
    plans.need(set(value) == {'length', 'sha256'} and contract.probe.uint(value['length']) and
               contract.probe.digest(value['sha256']), 'reader', 'target_byte_digest')
    return value


def flow_business(plan, config, ctx, snapshot):
    result = {}
    root = snapshot['remote_root']
    client, target = snapshot['client'], snapshot['target']
    for name, flow_id in FLOW_NAMES.items():
        # Keep the existing original name; carrier identity comes from sealed
        # S5 custody, independently of the application's process/socket identity.
        actual = flow_id
        a, b = client['flows'].get(actual), target['flows'].get(actual)
        plans.need(a and b and a['identities'] and b['identities'], 'reader', 'original_flow_required_' + name)
        identities = b['identities']
        plans.need(not any(v.get('owner_lost') for v in identities) and
            all(v == identities[0] for v in identities), 'reader', 'original_socket_identity_changed')
        original = socket_from_identity(plan, config, 'agt1', identities[-1])
        files = [(node, Path(root) / (actual + suffix)) for node, suffix in
                 (('ctl1', '.sent'), ('agt1', '.sent'), ('agt1', '.received'), ('ctl1', '.received'))]
        with ThreadPoolExecutor(max_workers=4) as workers:
            stats = list(workers.map(lambda row: file_stats(plan, config, *row), files))
        tx, rx = stats[:2], stats[2:]
        events = a['events'] + b['events']
        result[name] = dict(identity=dict(application_nonce=ctx['nonce'], socket=original),
            accepts=b['target_accepts'], redials=sum(e['kind'] == 'redial' for e in events),
            tx=sum(s['length'] for s in tx), rx=sum(s['length'] for s in rx),
            tx_sha256=hashlib.sha256(contract.canonical(tx)).hexdigest(),
            rx_sha256=hashlib.sha256(contract.canonical(rx)).hexdigest(),
            fin=[any(e['kind'] == 'eof' for e in b['events']), any(e['kind'] == 'eof' for e in a['events'])],
            carrier=snapshot['bindings'][actual]['after'])
    return result


def task_business(plan, config, ctx, kind, snapshot):
    root = Path(ctx['evidence']) / kind
    target = snapshot['target']
    identity = target.get('current', target.get('identity'))
    plans.need(target.get('nonce') == ctx['nonce'] and type(identity) is dict,
               'reader', 'original_task_nonce')
    child = native.process(native.physical(plan, config, plan['product']['node'], 'process', pid=identity['pid']))
    plans.need(child['StartTicks'] == identity['start_ticks'] and child['BootID'] == identity['boot_id'],
               'reader', 'original_task_process_changed')
    ctl = workload.process_identity(snapshot['ctl_pid'], ctx['nonce'])
    if kind == 'pty':
        sent, received = (root / 'ctl.sent').read_bytes(), (root / 'ctl.received').read_bytes()
    else:
        # Source and receiver are read independently. stdout is not rebuilt
        # from the target ledger. The original native waiter is retained.
        sent = native.remote(plan, config, plan['product']['node'],
            ['/usr/bin/cat', snapshot['remote_root'] + '/ledger.jsonl'])
        received = (root / 'stdout').read_bytes()
    return dict(identity=dict(application_nonce=ctx['nonce'], ctl=ctl, child=child,
            task_id=snapshot['task']['pid']), accepts=snapshot['accepts'], redials=snapshot['redials'], tx=len(sent), rx=len(received),
        tx_sha256=hashlib.sha256(sent).hexdigest(), rx_sha256=hashlib.sha256(received).hexdigest(),
        fin=[False, False], carrier=str(snapshot['task']['pid']))


def business(plan, config, ctx):
    snapshots = owners(ctx)
    result = flow_business(plan, config, ctx, snapshots['flow'])
    for kind in ('pty', 'exec'):
        result[kind] = task_business(plan, config, ctx, kind, snapshots[kind])
    from infra_owner_ports import business as validate
    validate(result)
    return result


def attachments(plan, config, ctx):
    declarations = config.get('participants', {})
    plans.need(type(declarations) is dict and declarations, 'reader', 'public_participant_export_required')
    result, accepted = {}, {}
    for participant, source in declarations.items():
        plans.need(set(source) == {'node', 'ingress_node', 'reader'}, 'reader', 'participant_source_fields')
        if 'custody' in source['reader']:
            from infra_native_exports import attachment
            row = attachment(plan, config, source, participant)
        else:
            _, raw = native.invoke(plan, ctx, 'readers.sample', source['reader'], [], 10)
            row = workload.decode(raw)
        contract.probe.attachment(row)
        plans.need(row['Participant'] == participant, 'reader', 'participant_export_binding')
        node = source['node']
        original = row['Raw']
        physical = native.physical(plan, config, node, 'census')
        # Require the exact socket incarnation, not matching URLs or serverID.
        raws = [dict(s, Owner=native.process(s['Owner'])) for s in physical['Sockets'] or []]
        plans.need(original in raws, 'reader', 'attachment_raw_not_in_kernel')
        ingress_node = source['ingress_node']
        plans.need(ingress_node is not None, 'reader', 'ingress_scope_required')
        ingress = native.physical(plan, config, ingress_node, 'census')
        sock = native.accepted_socket(ingress, original['Local'], original['Remote'], None)
        plans.need(sock['Owner'] == row['Ingress'], 'reader', 'attachment_ingress_changed')
        result[participant], accepted[participant] = row, sock
    return result, accepted


def s7_binding(plan, ctx):
    from infra_transfer_fixture import paths
    manifest, evidence = paths(ctx)
    spec = dict(argv=[str(Path('/usr/bin/python3').resolve()), '-I', '-B',
        str(plans.LIB / 'infra_transfer_fixture.py'), 'binding', '--source', str(manifest),
        '--context', str(Path(ctx['evidence']) / 'context.json')],
        sha256=contract.sha(Path('/usr/bin/python3').resolve()),
        inputs=[contract.pin(plans.LIB / 'infra_transfer_fixture.py')])
    _, raw = native.invoke(plan, ctx, 'owners.ready', spec, [], 60)
    result = workload.decode(raw)
    plans.need(result['manifest'] == str(manifest) and result['evidence'] == str(evidence),
               'owners.ready', 's7_original_binding')
    return result


def ready(plan, config, ctx):
    originals = business(plan, config, ctx)
    attached, _ = attachments(plan, config, ctx)
    binding = s7_binding(plan, ctx)
    lanes = config.get('phase_lanes', {})
    expected = {'writer_lost_ack', 'reader_partial_sync', 'commit_rename', 'terminal_courier'} if ctx['hops'] else set()
    plans.need(set(lanes) == expected, 'owners.ready', 's7_phase_lane_instruments_required')
    # Each lane selects an ACTUAL original transfer. A supplied expected ID is
    # insufficient: its source instrument must read the retained S7 custody.
    lane_ids = {}
    for lane, spec in lanes.items():
        _, raw = native.invoke(plan, ctx, 'owners.ready', spec,
            ['binding', '--context', str(Path(ctx['evidence']) / 'context.json')], 10)
        row = workload.decode(raw)
        plans.need(set(row) == {'transfer_id'} and row['transfer_id'] in {t['transfer_id'] for t in binding['transfers']},
                   'owners.ready', 'phase_lane_original')
        lane_ids[lane] = row['transfer_id']
    return dict(schema='infra-cohort-v1', phase='ready', producer='composer', seconds=ctx['seconds'],
        originals={name: dict(identity=row['identity'], carrier=row['carrier']) for name, row in originals.items()},
        attachments=attached, transfers=binding['transfers'], s7=dict(manifest=binding['manifest'], evidence=binding['evidence']),
        flow=dict(evidence=str(Path(ctx['evidence']) / 'flow'), nonce=ctx['nonce']),
        s7_manifest_sha256=binding['manifest_sha256'], s7_reader_digest=binding['reader_digest'], phase_lanes=lane_ids)


def observation(plan, config, ctx, port, arguments, timeout):
    if port == 'owners.prepare':
        from infra_native_admission import validate
        validate(config, port)
        plans.need(set(ctx['nodes']) <= set(config.get('nodes', {})), port, 'node_supply_required')
        for node in ctx['nodes']:
            native.node_observation(plan, config, ctx, node, ctx['component'])
        # No required live read can be discovered for the first time after
        # originals start. Missing product exports/hooks refuse preparation.
        plans.need(bool(config.get('participants')), port, 'public_participant_export_required')
        plans.need('repair_fence' in config or not ctx['hops'], port, 'p18_pre_repair_reader_required')
        plans.need('phase_reader' in config or not ctx['hops'], port, 'physical_phase_export_required')
        plans.need('final' in config.get('controls', {}), port, 's5_raw_final_and_control_instrument_required')
        profile = contract.verbs.PROFILES[(ctx['component'], ctx['profile'])]
        for point in profile.get('faults', ()):
            actions = config.get('faults', {}).get(point, {})
            plans.need('status' in actions, port, 'fault_status_instrument_required_' + point)
            if point not in ('nats-lost-quorum', 'nats-stream-lag', 'nats-consumer-lag'):
                plans.need({'install', 'heal'} <= set(actions), port, 'compound_fault_instrument_required_' + point)
        for scenario in profile.get('exercises', ()):
            plans.need(bool(config.get('exercises', {}).get(scenario)), port, 'exercise_recipes_required_' + scenario)
        if profile.get('configs'):
            plans.need('config_stage' in config, port, 'p26_config_stage_hook_required')
        return dict(preconditions=True)
    if port == 'owners.ready':
        return ready(plan, config, ctx)
    if port == 'readers.progress':
        return dict(at_ns=time.monotonic_ns(), producer='original-target-reader', business=business(plan, config, ctx))
    if port == 'readers.sample':
        plans.need(len(arguments) == 2 and arguments[1] in contract.probe.PHASES, port, 'sample_arguments')
        hop_id, phase = arguments
        index = int(hop_id.removeprefix('hop-'))
        plans.need(hop_id == 'hop-' + str(index) and 0 <= index < len(ctx['hops']), port, 'declared_hop_required')
        plans.need('repair_fence' in config, port, 'p18_pre_repair_reader_required')
        plans.need('phase_reader' in config, port, 'physical_phase_export_required')
        # This hook must read the real ACKed JS prefixes under P18's existing
        # before-repair gate. The harness never pauses a writer to create it.
        if 'custody' in config['repair_fence']:
            from infra_native_exports import repair
            binding = s7_binding(plan, ctx)
            prefix = repair(plan, config, ctx, hop_id, phase, [r['transfer_id'] for r in binding['transfers']])
        else:
            _, raw = native.invoke(plan, ctx, port, config['repair_fence'], [hop_id, phase], min(timeout, 120))
            prefix = workload.decode(raw)
        plans.need(type(prefix) is list and len(prefix) == 4, port, 'four_pre_repair_prefixes_required')
        for row in prefix:
            contract.probe.fields(row, 'prefix')
            plans.need(row['at_ns'] <= row['before_repair_ns'] and row['expected'] and type(row['actual']) is list,
                       port, 'pre_repair_prefix_custody')
        attached, accepted = attachments(plan, config, ctx)
        _, raw = native.invoke(plan, ctx, port, config['phase_reader'], [hop_id, phase], min(timeout, 120))
        metadata = workload.decode(raw)
        allowed = set(contract.probe.SCHEMA_FIELDS['sample'].split()) - {
            'hop', 'phase', 'at_ns', 'producer', 'business', 's7', 'prefix_reads', 'attachments', 'accepted'}
        plans.need(type(metadata) is dict and {'before', 'current', 'temporary', 'census'} <= set(metadata) <= allowed,
                   port, 'complete_physical_phase_export_required')
        component, _, node, _ = ctx['hops'][index]
        current = native.node_observation(plan, config, ctx, node, component)
        plans.need(metadata['current'] == current['server'], port, 'phase_current_not_executing_main')
        return dict(metadata, hop=hop_id, phase=phase, at_ns=time.monotonic_ns(), producer='kernel-and-target-reader',
            business=business(plan, config, ctx), prefix_reads=prefix, attachments=attached, accepted=accepted)
    if port == 'readers.final':
        # Final work belongs to the same original flow service. Missing raw S5
        # projection/control exports cannot be replaced with progress==success.
        from infra_native_actions import final
        return final(plan, config, ctx, arguments, timeout)
    plans.need(False, port, 'unknown_observation')
