#!/usr/bin/env -S python3 -I -B
"""Deployment-native owner instruments, without an external collector daemon.

An owner plan may pin a deployment file instead of native.endpoint. Its schema
is infra-native-deployment-v1: sim and physical are executable file pins; nodes
map deployment names to {units,scope,configs,probes} (optional per-component scopes);
observers maps each node to its registered remote {root,cgroup}. tls maps each lane to its target
node; config_stage is the P26 {argv,sha256,inputs} hook and config_root is the
PRODUCT catalog directory. participants, faults and exercises are described in
the responsibility modules. These are input recipes, never expected receipts.

All execution uses the shared supervisor. Physical observations use P02 itself
through infraobserve; no Status field supplies process/socket identity. Missing
provisioning is a named NOT_RUN before invoking an operation. Public response digests and native rc are retained, including failures, in a private run root.
origin: hot-update S9 plan §2.3 / §9.1 / §9.4 / L22; S9 INSTRUMENTS-3.
"""
import hashlib
import os
from pathlib import Path
import platform
import re
import secrets
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import infra_composer_contract as contract
import infra_owner_lifecycle as lifecycle
import infra_owner_plan as plans
import hot_update_infra_workload as workload

PORTS = tuple(p for p in plans.PORTS if p.startswith(('readers.', 'tls.', 'faults.', 'exercises.', 'configs.'))
              and p != 'readers.effects') + ('owners.prepare', 'owners.ready')
FIELDS = {'schema', 'sim', 'physical', 'nodes', 'tls', 'participants', 'config_stage', 'config_root',
          'faults', 'exercises', 'controls', 'phase_lanes', 'repair_fence', 'phase_reader', 'observers'}


def deployment(plan, port):
    source = plan.get('deployment')
    plans.need(type(source) is dict, port, 'deployment_inputs_required')
    plans.pin(source, port)
    row = contract.read(source['path'])
    plans.need(type(row) is dict and row.get('schema') == 'infra-native-deployment-v1' and
               set(row) <= FIELDS, port, 'deployment_schema')
    return row


def inputs(plan, port):
    row = deployment(plan, port)
    result = [plan['deployment']]
    for key in ('sim', 'physical'):
        plans.need(key in row, port, key + '_supply_required')
        plans.pin(row[key], port, True)
        result.append(row[key])
    for key in ('config_stage', 'repair_fence', 'phase_reader'):
        if key in row:
            if key == 'repair_fence' and 'custody' in row[key]:
                continue  # Its live journal command is pinned by recursive collection.
            spec = contract.spec(row[key])
            result += [contract.pin(spec['argv'][0]), *spec.get('inputs', [])]
    # Recipes contain executable specs, not observation documents. Bind every
    # transitive source now and repeat admission immediately before each spawn.
    def collect(value):
        if type(value) is dict:
            if 'argv' in value:
                spec = contract.spec(value)
                result.extend([contract.pin(spec['argv'][0]), *spec.get('inputs', [])])
            else:
                for child in value.values(): collect(child)
        elif type(value) is list:
            for child in value: collect(child)
    for key in ('participants', 'faults', 'exercises', 'controls', 'phase_lanes', 'repair_fence'):
        collect(row.get(key, {}))
    # Bind the source closure of P02 and the existing remote supervisor/flow
    # adapters as well as the thin command. A moved or changed dependency must
    # invalidate the supplied instrument before its next invocation.
    for source in sorted(plans.LIB.iterdir()):
        if source.is_file() and source.suffix in ('.py', '.sh'):
            result.append(contract.pin(source))
    repo = plans.LIB.parents[3]
    for directory in ('internal/infrahost', 'internal/infracontract', 'internal/controlbudget'):
        result.extend(contract.pin(p) for p in sorted((repo / directory).glob('*.go')) if not p.name.endswith('_test.go'))
    result += [contract.pin(repo / name) for name in ('go.mod', 'go.sum')]
    for node in row.get('nodes', {}).values():
        for probe in node.get('probes', {}).values():
            if probe.get('protocol') == 'tls':
                result.append(contract.pin(plans.path(probe['ca_file'], port)))
    return result


def run(plan, spec, arguments, timeout):
    plans.pin(plan['supervisor'], 'native', True)
    plans.need(os.environ.get('SIM_UPGRADE_SUPERVISOR') == plan['supervisor']['path'],
               'native', 'supervisor_binding')
    plans.need(type(timeout) in (float, int) and 0 < timeout <= 600, 'native', 'bounded_operation_required')
    argv = workload.command(contract.spec(spec)) + list(arguments)
    return workload.Supervisor(plan['supervisor']['path']).run(argv, timeout)


def record(ctx, port, rc, raw):
    # Each invocation has its own immutable receipt; concurrent 10/s readers
    # cannot overwrite a prior nonzero native status with a later success.
    root = lifecycle.private(ctx['evidence']) / 'native'
    root.mkdir(mode=0o700, exist_ok=True)
    lifecycle.private(root)
    stem = port + '-' + secrets.token_hex(16)
    # Unknown command output is kept in memory only. Recording an unvalidated
    # response here could persist private fields before the public schema guard.
    workload.publish(root / (stem + '.json'), dict(port=port, native_rc=rc,
        context_sha256=hashlib.sha256(contract.canonical(ctx)).hexdigest(),
        raw_sha256=hashlib.sha256(raw).hexdigest(), raw_bytes=len(raw), at_ns=time.monotonic_ns()))


def invoke(plan, ctx, port, spec, arguments, timeout, allowed=(0,)):
    rc, raw = run(plan, spec, arguments, timeout)
    record(ctx, port, rc, raw)
    plans.need(rc in allowed, port, 'native_rc_' + str(rc))
    return rc, raw


def remote(plan, config, node, argv, timeout=10):
    plans.need(node in config.get('nodes', {}), 'physical', 'node_scope_required')
    # A local supervisor around a docker client cannot reap its remote child.
    # Reuse the existing remote heartbeat/cgroup owner as well as FIX-ORPHAN.
    from infra_owner_workloads import module
    from fullstack_provision import PeerOwner
    from types import SimpleNamespace
    binding = config.get('observers', {}).get(node)
    plans.need(type(binding) is dict and set(binding) == {'root', 'cgroup'},
               'physical', 'registered_remote_observer_required')
    ctx = config.get('_context', {})
    plans.need(bool(ctx), 'physical', 'observer_context_required')
    root = lifecycle.private(ctx['evidence']) / ('read-' + secrets.token_hex(16))
    transport = module('flow_fixture').Transport(SimpleNamespace(here=Path(plan['flow']['here']),
        instance=ctx['instance'], sim=config['sim']['path']))
    # The source of this fixed pre-exec check is pinned with this module. Check
    # the EXECUTING remote supervisor's SHA, not a claim in the environment.
    check = ("import hashlib,os,sys; p=os.getppid(); found=False\n"
        "for _ in range(64):\n"
        " try:\n"
        "  found=hashlib.sha256(open('/proc/%d/exe'%p,'rb').read()).hexdigest()==sys.argv[1]\n"
        "  if found: break\n"
        "  p=int(open('/proc/%d/stat'%p).read().rsplit(')',1)[1].split()[1])\n"
        "  if p<=1: break\n"
        " except OSError: break\n"
        "if not found: raise SystemExit(2)\n"
        "os.execv(sys.argv[2],sys.argv[2:])\n")
    owner = PeerOwner(transport, root, node, binding['root'], binding['cgroup'],
        ['/usr/bin/python3', '-I', '-B', '-c', check, plan['supervisor']['sha256'], *map(str, argv)])
    try:
        owner.child.wait(timeout=timeout)
    except BaseException:
        owner.cancel()
        owner.join()
        raise
    rc = owner.join()
    raw = (root / 'supervisor.log').read_bytes()
    plans.need(rc == 0, 'physical', 'target_native_rc_' + str(rc))
    return raw


def physical(plan, config, node, operation, **values):
    row = config.get('nodes', {}).get(node)
    plans.need(type(row) is dict and {'units', 'scope', 'configs', 'probes'} <= set(row) <=
               {'units', 'scope', 'configs', 'probes', 'scopes'}, 'physical', 'complete_node_scope_required')
    # The target path is a pre-provisioned immutable copy of this binary.
    # Check its actual bytes before every invocation; host bytes alone cannot
    # certify a stale copy in the deployment namespace.
    path = config['physical']['path']
    actual = remote(plan, config, node, ['/usr/bin/sha256sum', path]).split()
    plans.need(actual and actual[0].decode() == config['physical']['sha256'], 'physical', 'target_reader_sha')
    request = dict(operation=operation, **values)
    if operation in ('unit', 'census'):
        request['scope'] = row['scope']
    if operation == 'unit':
        component = next((name for name, unit in row['units'].items() if unit == values['unit']), None)
        if component != 'broker':
            plans.need(component in row['probes'], 'physical', 'main_protocol_probe_required')
            request['probe'] = row['probes'][component]
            request['scope'] = row.get('scopes', {}).get(component, row['scope'])
    raw = remote(plan, config, node, [path, '--request', contract.canonical(request).decode().strip()])
    return workload.decode(raw)


def process(value):
    # Go's frozen private codec spells Digest as [32]byte. P32 spells only
    # these public observation digests in hex. Do not recursively rewrite IDs.
    value = dict(value)
    digest = value['ExeSHA256']
    if type(digest) is list:
        value['ExeSHA256'] = bytes(digest).hex()
    contract.probe.process(value)
    return value


def census(value):
    plans.need(value['Complete'] is True and value['Generation'] > 0 and value['Processes'],
               'physical', 'incomplete_kernel_census')
    return dict(producer='kernel-reader', complete=True, generation=value['Generation'],
        processes=[process(p) for p in value['Processes']], live_business=value['LiveBusiness'],
        old_owner_pending=value['OldOwnerPending'])


def accepted_socket(value, local, remote, translation):
    from infra_owner_ports import public
    rows = []
    for item in value.get('Sockets') or []:
        item = dict(item, Owner=process(item['Owner']))
        if contract.probe.connection_binding(item, local, remote, translation):
            rows.append(item)
    plans.need(len(rows) == 1, 'tls', 'accepted_socket_ambiguous')
    public('tls.external4.reader', dict(accepted=rows[0]))
    return rows[0]


def node_observation(plan, config, ctx, node, component):
    row = config.get('nodes', {}).get(node, {})
    units = row.get('units', {})
    plans.need(set(units) == {'nats', 'caddy', 'broker'}, 'readers.node', 'unit_bindings_required')
    serving = physical(plan, config, node, 'unit', unit=units[component])
    broker = physical(plan, config, node, 'unit-process', unit=units['broker'])
    server = process(serving['process'])
    observed = census(serving['census'])
    configs = row['configs']
    plans.need(component in configs, 'readers.node', 'config_path_required')
    raw = remote(plan, config, node, ['/usr/bin/sha256sum', configs[component]])
    digest = raw.split()[0].decode()
    plans.need(contract.probe.digest(digest), 'readers.node', 'target_config_digest')
    runtime = workload.decode(remote(plan, config, node,
        ['/proc/' + str(broker['process']['PID']) + '/exe', 'admin', 'runtime', '--json']))
    capability = runtime.get('infra_capability', {})
    # A process in the full P02 census other than main is retained as temporary;
    # never infer retirement from unit MainPID or matching executable SHA.
    temporary = [p for p in observed['processes'] if p != server]
    return dict(producer='kernel-reader', node=node, component=component, at_ns=time.monotonic_ns(),
        server=server, invocation=dict(user=serving['user'], restart=serving['restart'], managed=serving['managed']),
        config=digest, serving=serving['serving'], temporary=temporary, broker=process(broker['process']),
        infra_capable=capability.get('version') == 1 and capability.get(component) is True, census=observed)


def tls_reader(plan, config, ctx, lane, arguments):
    row = config.get('tls', {}).get(lane)
    plans.need(type(row) is dict and set(row) == {'node', 'component', 'translation'},
               'tls.' + lane + '.reader', 'tls_target_binding_required')
    plans.need(len(arguments) == 2, 'tls.' + lane + '.reader', 'held_client_tuple_required')
    node = row['node']
    unit = config['nodes'][node]['units'][row['component']]
    observed = physical(plan, config, node, 'unit', unit=unit)
    translation = None
    if row['translation'] is not None:
        # An actual conntrack dump, never a DNAT address guessed from config.
        from infra_native_targets import conntrack_translation
        raw = remote(plan, config, node, ['/usr/sbin/conntrack', '-L', '-p', 'tcp', '-o', 'extended'])
        translation = conntrack_translation(raw, arguments[0], arguments[1],
            next(iter(observed['census']['Sockets'] or []), {}).get('NetNS'),
            observed['census']['Generation'])
    accepted = accepted_socket(observed['census'], *arguments, translation)
    result = dict(accepted=accepted)
    if translation is not None: result['translation'] = translation
    return result


def stage_config(plan, config, ctx, variant, arguments, timeout):
    port = 'configs.' + variant
    source = plan.get('configs', {}).get(variant, {})
    plans.pin(source, port)
    plans.need('config_stage' in config, port, 'p26_config_stage_hook_required')
    root = plans.path(config.get('config_root', ''), port)
    forwarded = list(arguments)
    bindings = [('--variant', variant), ('--source', source['path']), ('--source-sha256', source['sha256'])]
    if 'evidence' in ctx:
        # The context is a scoped input to the staging hook, not a path that
        # the hook can infer from its source plan or a previous deployment run.
        bindings.append(('--context', str(Path(ctx['evidence']) / 'context.json')))
    for flag, expected in bindings:
        plans.need(forwarded.count(flag) <= 1, port, 'duplicate_stage_argument')
        if flag in forwarded:
            index = forwarded.index(flag)
            plans.need(index + 1 < len(forwarded) and forwarded[index + 1] == expected, port, 'stage_argument_binding')
            del forwarded[index:index + 2]
    # Only P26 may stage the product catalog. The hook receives the public
    # source and expected digest, then an independent target read checks the
    # resulting content-addressed product input. A local copy is insufficient.
    _, raw = invoke(plan, ctx, port, config['config_stage'],
        forwarded + [value for binding in bindings for value in binding], timeout)
    result = workload.decode(raw)
    plans.need(result == dict(variant=variant, staged=True, sha256=source['sha256']), port, 'p26_stage_receipt')
    for node in ctx['nodes']:
        actual = remote(plan, config, node, ['/usr/bin/sha256sum', str(root / source['sha256'])]).split()
        plans.need(actual and actual[0].decode() == source['sha256'], port, 'product_catalog_readback_sha')
    return result


def execute(plan, ctx, port, arguments, timeout):
    plans.need(port in PORTS, port, 'unknown_native_port')
    # origin: hot-update S9 plan lead LQ1; refuse before any deployment effect.
    plans.need(sys.platform == 'linux' and platform.machine() in ('x86_64', 'amd64'), port, 'unsupported')
    config = deployment(plan, port)
    plans.need(ctx.get('mode') == 'live', port, 'live_context_required')
    inputs(plan, port)
    config['_context'] = ctx
    if port.startswith('tls.'):
        result = tls_reader(plan, config, ctx, port.split('.')[1], arguments)
    elif port == 'readers.node':
        plans.need(len(arguments) == 2,
                   port, 'node_arguments')
        result = node_observation(plan, config, ctx, arguments[0], arguments[1])
    elif port.startswith('configs.'):
        result = stage_config(plan, config, ctx, port.split('.')[1], arguments, timeout)
    elif port.startswith(('faults.', 'exercises.')):
        from infra_native_actions import action
        result = action(plan, config, ctx, port, arguments, timeout)
    else:
        from infra_native_targets import observation
        result = observation(plan, config, ctx, port, arguments, timeout)
    # No path can publish raw private fields just because a producer succeeded.
    from infra_owner_ports import public
    return public(port, result)
