#!/usr/bin/env -S python3 -I -B
"""Pinned host-local inputs for the S9 live owner port adapters.

An infra-owner-plan-v1 supplies product {binary,home,nats_url,node,read_path,
effect_root,remote_python}, supervisor (file pin), optional flow {here,sim},
pty/exec {remote_probe}, native (an independently supervised collector's
{endpoint,executable,inputs}), tls (six numeric endpoint/CA rows), and configs
(three file pins). No product observation or private credential belongs here.
The native collector protocol is documented in infra_owner_ports.py. A missing
collector is a named NOT_RUN, never a synthetic sample built from Status.

origin: hot-update S9 plan §9.1 / §9.4 / L22; S9 INSTRUMENTS-2.
"""
import copy
import os
from pathlib import Path
import re
import socket
import sys
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
import infra_composer_contract as contract

LIB = Path(__file__).resolve().parent
PORTS = ('owners.prepare', 'owners.ready') + tuple(
    'owners.workloads.' + owner + '.' + action
    for owner in ('flow', 'pty', 'exec') for action in ('keeper', 'stop')) + tuple(
    'commands.' + name for name in contract.probe.KINDS) + tuple(
    'readers.' + name for name in ('sample', 'progress', 'effects', 'final', 'node')) + tuple(
    'tls.' + lane + '.reader' for lane in contract.probe.TLS_LANES) + tuple(
    'faults.' + point + '.' + action for point in contract.verbs.ALL_FAULTS
    for action in ('install', 'status', 'heal')) + tuple(
    'exercises.' + name for name in contract.verbs.ALL_EXERCISES) + tuple(
    'configs.' + name for name in contract.verbs.ALL_CONFIGS)
SOURCES = ('infra_owner_plan.py', 'infra_owner_ports.py', 'infra_owner_lifecycle.py',
           'infra_deployment_plan.py', 'infra_deployment_phase.py', 'infra_deployment_faults.py', 'infra_deployment_final.py',
           'infra_native_admission.py', 'infra_config_stage.py', 'infra_s7_lanes.py',
           'infra_native.py', 'infra_native_targets.py', 'infra_native_actions.py',
           'infra_native_custody.py', 'infra_native_exports.py',
           'infra_native_faults.sh',
           'infraobserve/main.go', 'infraobserve/probe.go',
           'infra_owner_workloads.py', 'infra_flow_peer.py', 'infra_request_effect.py', 'infra_composer_contract.py',
           'hot_update_infra_probe.py', 'hot_update_infra_scope.py', 'hot_update_infra_verbs.py',
           'hot_update_infra_workload.py', 'hot_update_budget.sh', 'transfer_observation.py')


def need(ok, port, reason):
    if not ok:
        raise ValueError('NOT_RUN ' + port + ': ' + reason)


def path(value, port):
    need(isinstance(value, str) and value and Path(value).is_absolute()
         and '.tether' not in Path(value).parts, port, 'isolated_absolute_path_required')
    return Path(value)


def pin(value, port, executable=False):
    try:
        contract.file_pin(value)
        target = path(value['path'], port)
        if executable:
            need(os.access(target, os.X_OK), port, 'executable_required')
        return target
    except (OSError, KeyError, TypeError, ValueError):
        raise ValueError('NOT_RUN ' + port + ': missing_or_changed_pin') from None


def product(value, port):
    row = value.get('product', {})
    need(type(row) is dict and set(row) == {'binary', 'home', 'nats_url', 'node',
         'read_path', 'effect_root', 'remote_python'}, port, 'product_inputs_required')
    binary = pin(row['binary'], port, True)
    with binary.open('rb') as stream:
        elf = stream.read(4) == b'\x7fELF'
    need(binary.name.startswith('tether') and
         (binary.name != 'tether' or 'vendor' in binary.parts) and elf, port, 'prebuilt_tether_required')
    home = path(row['home'], port)
    need(home.is_dir() and home != Path.home() and not home.is_symlink(), port, 'isolated_home_required')
    url = urlsplit(row['nats_url'])
    need(url.scheme in ('nats', 'tls', 'ws', 'wss') and url.hostname and
         url.username is None and url.password is None and not url.query and not url.fragment,
         port, 'public_nats_url_required')
    need(row['node'] in ('agt1', 'agt2'), port, 'agent_required')
    for key in ('read_path', 'effect_root', 'remote_python'):
        path(row[key], port)
    return row


def load(source, expected):
    pin(dict(path=str(source), sha256=expected), 'owner-plan')
    value = contract.read(source)
    need(type(value) is dict and value.get('schema') == 'infra-owner-plan-v1' and
         set(value) <= {'schema', 'product', 'supervisor', 'flow', 'pty', 'exec', 'native', 'tls', 'configs', 'deployment'},
         'owner-plan', 'schema')
    return value


def prerequisites(value, port):
    need(port in PORTS, 'owner-plan', 'unknown_port')
    inputs = [value.get('supervisor', {})]
    pin(inputs[0], port, True)
    row = product(value, port)
    inputs += [row['binary'], contract.pin(LIB / 'infra_request_effect.py')]
    if port.startswith('owners.workloads.flow.') or port == 'owners.prepare':
        flow = value.get('flow', {})
        need(type(flow) is dict and set(flow) == {'here', 'sim'}, port, 'flow_deployment_required')
        here = path(flow['here'], port)
        need(here == LIB.parents[1], port, 'flow_source_tree_required')
        pin(flow['sim'], port, True)
        inputs.append(flow['sim'])
        # The existing transport sources simcluster libraries dynamically.
        # Pin that whole local source closure rather than just its trampoline.
        inputs += [contract.pin(p) for directory in (here / 'lib', LIB) for p in sorted(directory.iterdir())
                   if p.is_file() and p.suffix in ('.py', '.sh')]
    for owner, helper in (('pty', 'hot_update_pty_probe.py'), ('exec', 'hot_update_process_probe.py')):
        if port.startswith('owners.workloads.' + owner + '.') or port == 'owners.prepare':
            spec = value.get(owner, {})
            need(type(spec) is dict and set(spec) == {'remote_probe'}, port, owner + '_deployment_required')
            path(spec['remote_probe'], port)
            inputs.append(contract.pin(LIB / helper))
    # These facts belong to the independent kernel/target/fault instrument.
    # The adapter checks peer credentials and its executing SHA for every RPC.
    native_port = port in ('owners.prepare', 'owners.ready') or port.startswith(('readers.', 'tls.', 'faults.', 'exercises.', 'configs.'))
    if native_port and 'deployment' in value:
        from infra_native import inputs as native_inputs
        inputs += native_inputs(value, port)
    elif native_port:
        native = value.get('native', {})
        need(type(native) is dict and set(native) == {'endpoint', 'executable', 'inputs'},
             port, 'independent_native_collector_required')
        path(native['endpoint'], port)
        pin(native['executable'], port, True)
        need(type(native['inputs']) is list, port, 'native_source_pins_required')
        for source in native['inputs']:
            pin(source, port)
        inputs += [native['executable'], *native['inputs']]
    if port.startswith('tls.'):
        lane = port.split('.')[1]
        config = value.get('tls', {}).get(lane, {})
        need(type(config) is dict and {'address', 'server_name', 'ca_file'} <= set(config)
             <= {'address', 'server_name', 'ca_file', 'source_address'}, port, 'numeric_tls_lane_required')
        try:
            socket.inet_pton(socket.AF_INET6 if lane.endswith('6') else socket.AF_INET, config['address'])
        except OSError:
            need(False, port, 'numeric_tls_lane_required')
        need(isinstance(config['server_name'], str) and re.fullmatch(r'[A-Za-z0-9.-]+', config['server_name']),
             port, 'tls_server_name_required')
        inputs.append(contract.pin(path(config['ca_file'], port)))
    if port.startswith('configs.'):
        source = value.get('configs', {}).get(port.split('.')[1], {})
        pin(source, port)
        inputs.append(source)
    for source in inputs:
        pin(source, port)
    return inputs


def supply(source):
    """Compile actual adapters into the composer's unchanged instrument ABI."""
    source = Path(source).absolute()
    value = load(source, contract.sha(source))
    if 'deployment' in value:
        from infra_native import deployment
        from infra_native_admission import validate
        validate(deployment(value, 'owners.prepare'), 'owners.prepare')
    sources = [contract.pin(source)] + [contract.pin(LIB / name) for name in SOURCES]
    python = str(Path('/usr/bin/python3').resolve())
    prefix = [python, '-I', '-B', str(LIB / 'infra_owner_ports.py'), '--plan', str(source),
              '--plan-sha256', contract.sha(source)]
    result = {}
    for port in PORTS:
        # origin: hot-update S9 plan §9.4; S9 INSTRUMENTS-4.
        # Each port retains its complete execution closure. Unioning every
        # unrelated port into each copy exceeds the bounded supply codec once
        # all eighteen profiles use the real native adapters.
        by_path = {row['path']: row for row in sources + prerequisites(value, port)}
        inputs = [by_path[key] for key in sorted(by_path)]
        spec = dict(argv=prefix + ['--port', port], sha256=contract.sha(python), inputs=copy.deepcopy(inputs))
        if port.startswith('commands.'):
            spec['argv'] += ['--logical-id', '{logical_id}']
            spec['mode'] = 'cli'
            if port == 'commands.read':
                # prepare verifies this public marker at the actual target.
                spec['expected_output'] = 'infra-read-v1'
            if port == 'commands.short_exec':
                spec['expected_output'] = '{logical_id}'
        cursor = result
        for key in port.split('.')[:-1]:
            cursor = cursor.setdefault(key, {})
        cursor[port.split('.')[-1]] = spec
    for lane in contract.probe.TLS_LANES:
        result['tls'][lane].update(value['tls'][lane])
    return result
