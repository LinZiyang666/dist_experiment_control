#!/usr/bin/env -S python3 -I -B
"""S9 fixture verb engine behind drills/lib/hot_update_infra.sh.

Every infra_fixture verb that drills 85/86 and 39.NI/CI call lands here, except
the two job spawns (composer keeper, observer scheduler) that must belong to the
drill shell. This file is a producer only. It validates the drill's arguments,
runs the live requiredset composer's pinned commands and the product's read-only
status through the shared FIX-ORPHAN supervisor, retains every raw receipt under
the private evidence root and hands each scoped check to the independent oracle
(hot_update_infra_scope.py). It never performs a product migration, fence,
redirect or retirement, never pauses a producer and never invents an observation.

Supply contract (vendor/infra-fixture.json, schema infra-binding-v1): the live
composer (S9 P19/P30 with the lead-accepted S7 transfer-v1 handoff) declares its
keeper/control, five request commands, independent readers, TLS lanes and the
closed fault/exercise/config instruments as {argv, sha256, inputs} specs. Without
that supply every arm stops SETUP-RED at preflight naming the pending owner.
Native status: 0 done, 2 setup/custody; check and finish return the oracle's
0/1/2/4 unchanged. origin: S9 FIX-INFRAVERBS (r9d finding 5); hot-update S9
plan §8 / §9.1 / §9.3 / §12 P32
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hot_update_infra_probe as probe
import hot_update_infra_workload as workload

LIB = Path(__file__).resolve().parent
HERE = LIB.parents[1]
BINDING = 'vendor/infra-fixture.json'
SOURCES = ('hot_update_infra.sh', 'hot_update_infra_verbs.py', 'hot_update_infra_scope.py',
           'hot_update_infra_workload.py', 'hot_update_infra_probe.py')
TOPOLOGY = {'single': ('brk1',), 'sole-voter': ('brk1',), 'pair': ('brk1', 'brk2'), 'quorum': ('brk1', 'brk2', 'brk3')}
BINARY = {'nats': 'nats-server', 'caddy': 'caddy'}
QUORUM_FAULTS = ('nats-lost-quorum', 'nats-stream-lag', 'nats-consumer-lag')
GENERIC_SCOPES = ('baseline', 'bootstrap', 'retirement', 'logs')
# The closed arm table. plan derives the frozen hop schedule; nothing outside
# this table (a point, scenario, variant, window or scope) is accepted.
PROFILES = {
    ('nats', 'default-single'): dict(topologies=('single',), plan='cycle'),
    ('nats', 'standalone-js'): dict(topologies=('sole-voter',), plan='cycle'),
    ('nats', 'adjacent-cycle'): dict(topologies=('quorum',), plan='cycle'),
    ('nats', 'readiness-refusal'): dict(topologies=('pair', 'quorum'), plan='roll', scopes=('refusal',),
                                        faults=('nats-n2',) + QUORUM_FAULTS),
    ('nats', 'lifetime-store-lock'): dict(topologies=('single',), plan='none', scopes=('store-lock',),
                                          exercises=('store-double-open',)),
    ('nats', 'exact-release-join'): dict(topologies=('quorum',), plan='none', scopes=('join',),
                                         exercises=('exact-release-join',)),
    ('nats', 'post-ldm-recovery'): dict(topologies=('quorum',), plan='roll', scopes=('halted', 'recovered'),
                                        faults=('after-ldm-reply-cut',)),
    ('nats', 'legacy-capability'): dict(topologies=('single',), plan='none', scopes=('legacy-source', 'legacy-refusal')),
    ('caddy', 'config-reload'): dict(topologies=('single',), plan='reload', scopes=('reload',), configs=('valid',)),
    ('caddy', 'adjacent-cycle'): dict(topologies=('quorum',), plan='cycle'),
    ('caddy', 'stock-root-retrofit'): dict(topologies=('single',), plan='cycle',
                                           faults=('acme-late-write-final-copy-over-2s',)),
    ('caddy', 'healthy-n2'): dict(topologies=('pair',), plan='cycle'),
    ('caddy', 'invalid-config-cert'): dict(topologies=('single',), plan='none', scopes=('invalid-config-cert',),
                                           configs=('invalid-config', 'invalid-cert'), windows=('reload',)),
    ('caddy', 'responsibility-permission-gates'): dict(topologies=('single',), plan='none',
                                                       scopes=('responsibility-gates',),
                                                       exercises=('caddy-responsibility-gates',)),
    ('caddy', 'shadow-recovery'): dict(topologies=('single',), plan='roll', scopes=('halted', 'recovered'),
                                       faults=('main-failure-controller-death-shadow-over-120s',)),
    ('caddy', 'support-two-generation'): dict(topologies=('single',), plan='support', scopes=('support',)),
    ('caddy', 'legacy-capability'): dict(topologies=('single',), plan='none', scopes=('legacy-source', 'legacy-refusal')),
}
ALL_FAULTS = sorted({p for row in PROFILES.values() for p in row.get('faults', ())})
ALL_EXERCISES = sorted({p for row in PROFILES.values() for p in row.get('exercises', ())})
ALL_CONFIGS = sorted({p for row in PROFILES.values() for p in row.get('configs', ())})
# direction -> (hop component, hop action); None means the prepared component.
DIRECTIONS = {'upgrade': (None, 'upgrade'), 'reupgrade': (None, 'upgrade'), 'rollback': (None, 'rollback'),
              'reload': ('caddy', 'reload'), 'update-support': ('support', 'upgrade'),
              'rollback-support': ('support', 'rollback')}
# scope -> exact scope flags (argparse dest names); --evidence is always required.
SCOPES = {
    'baseline': (), 'retirement': (), 'logs': (), 'store-lock': (), 'join': (), 'responsibility-gates': (),
    'legacy-source': (), 'bootstrap': ('node', 'command_rc', 'command_log'),
    'round': ('direction', 'nodes', 'command_rc', 'command_log'), 'cycle': ('expected_hops', 'final_version'),
    'refusal': ('point', 'command_rc', 'command_log'), 'halted': ('point', 'command_rc', 'command_log'),
    'recovered': ('tx', 'command_rc', 'command_log'), 'legacy-refusal': ('command_rc', 'command_log'),
    'reload': ('config_sha256', 'command_rc', 'command_log'),
    'invalid-config-cert': ('variant', 'config_sha256', 'command_rc', 'command_log'),
    'support': ('action', 'command_rc', 'command_log'),
}
STATUS_FIELDS = ('Tx', 'Step', 'Phase', 'Revision', 'Code', 'Detail', 'Before', 'Current', 'Census', 'Proof')
HALTED = 11  # internal/infracontract Phase Halted


def fail(code):
    raise probe.EvidenceError(code)


def sha_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def evidence_root(value):
    """The drill's private evidence: absolute, 0700, owned by this user."""
    root = Path(value)
    info = root.lstat() if root.is_absolute() else None
    probe.require(info is not None and root.is_dir() and not root.is_symlink() and info.st_uid == os.geteuid()
                  and info.st_mode & 0o077 == 0, 'evidence_custody')
    return root


class State:
    """Single-writer verb ledger; every writing verb holds the lock while it runs.

    The composer keeper's exec only reads: the prepared verb holds the lock
    while it waits for that keeper, so a locking reader would deadlock it.
    """
    def __init__(self, root, fresh=False, reader=False):
        self.root = root
        directory = root / 'fixture'
        if fresh:
            probe.require(not directory.exists(), 'fresh_evidence_required')
            directory.mkdir(mode=0o700)
        probe.require(directory.is_dir(), 'fixture_not_preflighted')
        self.lock = open(directory / '.lock', 'a+')
        if not reader:
            fcntl.flock(self.lock, fcntl.LOCK_EX)
        path = directory / 'state.json'
        self.value = probe.read_json(path) if path.exists() else {}

    def require(self, *stages):
        probe.require(self.value.get('stage') in stages, 'verb_out_of_order')
        return self.value

    def save(self, **updates):
        self.value.update(updates)
        workload.publish(self.root / 'fixture/state.json', self.value)

    def record(self, name, raw):
        # Retain the producer's raw bytes; the oracle validates them itself.
        directory = self.root / 'fixture/receipts'
        directory.mkdir(mode=0o700, exist_ok=True)
        index = len(list(directory.iterdir()))
        path = directory / ('%03d-%s.json' % (index, name))
        path.write_bytes(raw)
        path.chmod(0o600)
        return str(path)


def ledger(root, verb, argv, rc):
    # The fixture's own verb trace, appended after each verb's native status.
    with open(Path(root) / 'fixture/verbs.jsonl', 'a', encoding='utf-8') as output:
        output.write(json.dumps(dict(verb=verb, argv=argv, rc=rc, at_ns=time.time_ns())) + '\n')


def supervisor():
    return workload.Supervisor(os.environ.get('SIM_UPGRADE_SUPERVISOR', ''))


def load_binding(state=None):
    path = HERE / BINDING
    if not path.is_file():
        fail('NOT_RUN pending S9 P19/P30 live requiredset composer binding ' + BINDING +
             ' (lead-accepted S7 transfer-v1 handoff)')
    raw = path.read_bytes()
    binding = workload.decode(raw)
    if state is not None:
        # Fully validated at preflight; later verbs bind the same bytes and
        # re-validate only the spec they execute (run_spec -> command()).
        probe.require(hashlib.sha256(raw).hexdigest() == state['binding_sha256'], 'binding_changed')
        return binding
    probe.require(isinstance(binding, dict) and set(binding) == {'schema', 'mode', 'handoff', 'cohort', 'commands',
                  'readers', 'tls', 'faults', 'exercises', 'configs'} and binding['schema'] == 'infra-binding-v1'
                  and binding['mode'] in ('live', 'hermetic'), 'binding_schema')
    probe.fields(binding['handoff'], 'handoff')
    probe.require(binding['handoff'].get('accepted_by') == 'lead' and binding['handoff'].get('schema') == 'transfer-v1'
                  and set(binding['handoff'].get('files', {})) == {'executable', 'source', 'schema', 'positive', 'negative'},
                  'NOT_RUN pending lead-accepted S7 transfer-v1 handoff')
    for row in binding['handoff']['files'].values():
        probe.fields(row, 'file')
        probe.require(Path(row['path']).is_absolute() and Path(row['path']).is_file()
                      and sha_file(row['path']) == row['sha256'], 's7_handoff_bytes')
    probe.require(set(binding['cohort']) == {'keeper', 'control'} and set(binding['commands']) == set(probe.KINDS)
                  and set(binding['readers']) == {'sample', 'progress', 'effects', 'final', 'node'}
                  and set(binding['tls']) <= set(probe.TLS_LANES), 'binding_entries')
    specs = list(binding['cohort'].values()) + list(binding['commands'].values()) + list(binding['readers'].values())
    for name, table, keys in (('faults', binding['faults'], ALL_FAULTS), ('exercises', binding['exercises'], ALL_EXERCISES),
                              ('configs', binding['configs'], ALL_CONFIGS)):
        probe.require(isinstance(table, dict) and set(table) <= set(keys), 'binding_unknown_' + name)
    for row in binding['faults'].values():
        probe.require(set(row) == {'install', 'status', 'heal'}, 'binding_fault_entry')
        specs += list(row.values())
    specs += list(binding['exercises'].values()) + list(binding['configs'].values())
    for config in binding['tls'].values():
        probe.fields(config, 'tls_config')
        specs.append(config['reader'])
    for spec in specs:
        workload.command(spec)
    return binding


def profile_of(state):
    return PROFILES[(state['component'], state['profile'])]


def need(binding, table, key):
    probe.require(key in binding[table], 'NOT_RUN pending S9 P30 composer ' + table[:-1] + ' instrument ' + key)
    return binding[table][key]


def run_spec(spec, arguments, timeout):
    rc, raw = supervisor().run(workload.command(spec) + list(arguments), timeout)
    probe.require(rc == 0, 'composer_command_failed:%d' % rc)
    return raw


def node_observations(handle, binding, label):
    """Fresh independent observation of every current topology node."""
    rows = {}
    for node in handle.value['nodes']:
        raw = run_spec(binding['readers']['node'], [node, handle.value['component']], 30)
        rows[node] = handle.record('obs-' + label + '-' + node, raw)
    return rows


def vendor_sha(*parts):
    path = HERE.joinpath('vendor', *parts)
    probe.require(path.is_file() and not path.is_symlink(), 'adjacent_artifact_missing:' + '/'.join(parts))
    return sha_file(path)


def target(component, action, prepared):
    if component == 'support':
        return vendor_sha('infra', 'tether-infra-helper-' + ('current' if action == 'upgrade' else 'previous'))
    if action == 'reload':
        return prepared['initial_sha256']
    return vendor_sha(BINARY[component] + ('-next' if action == 'upgrade' else '-prev'))


def schedule(state):
    """Freeze the whole hop schedule from the closed arm table, never the caller."""
    plan, nodes, component = profile_of(state)['plan'], list(TOPOLOGY[state['topology']]), state['component']
    if plan == 'roll' and state['topology'] == 'pair':
        nodes = list(TOPOLOGY['sole-voter'])  # 85.D rolls only after its product shrink
    steps = []
    if plan == 'support':
        steps += [('support', action, 'brk1') for action in ('upgrade', 'rollback', 'upgrade')]
    if plan in ('cycle', 'support'):
        steps += [(component, action, host) for action in ('upgrade', 'rollback', 'upgrade') for host in nodes]
    elif plan == 'roll':
        steps += [(component, 'upgrade', host) for host in nodes]
    elif plan == 'reload':
        steps.append(('caddy', 'reload', 'brk1'))
    return [(c, a, h, target(c, a, state)) for c, a, h in steps]


def preflight(args):
    root = evidence_root(args.evidence)
    probe.require(args.component in BINARY and (args.component, args.profile) in PROFILES, 'unknown_component_profile')
    handle = State(root, fresh=True)
    binding = load_binding()
    sources = {name: sha_file(LIB / name) for name in SOURCES}
    handle.save(stage='preflighted', component=args.component, profile=args.profile, mode=binding['mode'],
                binding_sha256=sha_file(HERE / BINDING), sources=sources, faults={}, exercises={}, configs={},
                blocks=[], checks=[])
    return dict(native_rc=0, mode=binding['mode'], sources=sources)


def prepare(args):
    handle = State(evidence_root(args.evidence))
    state = handle.require('preflighted')
    load_binding(state)
    probe.require((args.component, args.profile) == (state['component'], state['profile']), 'prepare_changed_arm')
    probe.require(args.topology in profile_of(state)['topologies'] and args.nodes.split() == list(TOPOLOGY[args.topology])
                  and args.agents == 'agt1,agt2' and args.ctl == 'ctl1'
                  and args.carrier_transition in ('unchanged', 'replaced'), 'prepare_topology')
    binary = BINARY[args.component]
    initial = HERE / 'vendor/infra' / (binary + '-' + args.initial_version)
    probe.require(re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', args.initial_version) is not None and initial.is_file()
                  and sha_file(initial) == vendor_sha(binary + '-prev'), 'initial_version_not_previous_adjacent')
    handle.save(stage='preparing', topology=args.topology, log_topology=args.topology, nodes=list(TOPOLOGY[args.topology]),
                carrier_transition=args.carrier_transition, initial_version=args.initial_version,
                initial_sha256=sha_file(initial))
    return dict(native_rc=0, topology=args.topology)


def cohort_exec(args):
    """Become the composer keeper inside the supervised job the shell owns."""
    root = evidence_root(args.evidence)
    handle = State(root, reader=True)
    state = handle.require('preparing')
    binding = load_binding(state)
    directory = root / 'cohort'
    directory.mkdir(mode=0o700)
    argv = workload.command(binding['cohort']['keeper']) + [
        '--evidence', str(directory), '--component', state['component'], '--topology', state['topology'],
        '--profile', state['profile'], '--nodes', ' '.join(state['nodes']), '--agents', 'agt1,agt2', '--ctl', 'ctl1',
        '--initial-sha256', state['initial_sha256']]
    handle.lock.close()
    os.execv(argv[0], argv)


def exited(root, label):
    # The supervisor creates its receipt at launch and writes the status only
    # after the subtree is reaped: an empty receipt means still running.
    path = root / (label + '.rc')
    return path.exists() and path.read_text().strip() != ''


def wait_receipt(root, name, deadline):
    # The keeper's own native receipt ends the wait: an exited composer can
    # never be mistaken for one that is still preparing the requiredset.
    path = root / 'cohort' / name
    while not path.exists():
        probe.require(not exited(root, 'cohort'), 'composer_exited_before_' + name)
        probe.require(time.monotonic() < deadline, 'composer_deadline_' + name)
        time.sleep(.05)
    return workload.decode(path.read_bytes())


def prepared(args):
    root = evidence_root(args.evidence)
    handle = State(root)
    state = handle.require('preparing')
    row = wait_receipt(root, 'prepared.json', time.monotonic() + 600)
    probe.require(row == dict(schema='infra-cohort-v1', phase='prepared', producer='composer',
                              component=state['component'], topology=state['topology'], profile=state['profile'],
                              initial_sha256=state['initial_sha256']), 'composer_prepared_binding')
    handle.save(stage='prepared')
    return dict(native_rc=0)


def start(args):
    root = evidence_root(args.evidence)
    handle = State(root)
    state = handle.require('prepared')
    binding = load_binding(state)
    budget = probe.budgets()
    # Never widen a pinned budget: the drill passes them only to be compared.
    probe.require((args.probe_rate, args.request_budget, args.flow_budget, args.transfer_budget) ==
                  (budget['HOT_UPDATE_PROBE_RATE'], budget['HOT_UPDATE_REQUEST_S'],
                   budget['HOT_UPDATE_LIVE_FLOW_STALL_S'], budget['HOT_UPDATE_TRANSFER_PROGRESS_S'])
                  and args.carrier_transition == state['carrier_transition'], 'start_budget_not_pinned')
    run_spec(binding['cohort']['control'], ['--evidence', str(root / 'cohort'), 'launch'], 600)
    ready = wait_receipt(root, 'ready.json', time.monotonic() + 600)
    keys = {'schema', 'phase', 'producer', 'seconds', 'originals', 'attachments', 'transfers', 's7', 'flow',
            's7_manifest_sha256', 's7_reader_digest', 'phase_lanes'}
    probe.require(isinstance(ready, dict) and set(ready) == keys and ready['schema'] == 'infra-cohort-v1'
                  and ready['phase'] == 'ready' and ready['producer'] == 'composer', 'composer_ready_schema')
    hops = schedule(state)
    participants = sorted(ready['attachments']) if isinstance(ready['attachments'], dict) else []
    caddy = any(c == 'caddy' for c, _, _, _ in hops)
    manifest = dict(schema=probe.SCHEMA, mode=binding['mode'], nonce=secrets.token_hex(16), seconds=ready['seconds'],
                    profile='cycle' if profile_of(state)['plan'] == 'cycle' else state['profile'],
                    carrier_transition=state['carrier_transition'], originals=ready['originals'],
                    attachments=ready['attachments'], transfers=ready['transfers'],
                    hops=[dict(id='hop-%d' % i, host=h, component=c, action=a, target_sha256=t, participants=participants)
                          for i, (c, a, h, t) in enumerate(hops)],
                    tls_lanes=list(probe.TLS_LANES) if caddy else [], s7_handoff=binding['handoff'],
                    s7_manifest_sha256=ready['s7_manifest_sha256'], s7_reader_digest=ready['s7_reader_digest'],
                    phase_lanes=ready['phase_lanes'],
                    deployment=dict(commands=binding['commands'], tls=binding['tls'] if caddy else {},
                                    readers={k: v for k, v in binding['readers'].items() if k != 'node'},
                                    s7=ready['s7'], flow=ready['flow']))
    probe.validate_manifest(manifest)
    workload.publish(root / 'manifest.json', manifest)
    handle.save(stage='starting', manifest_sha256=probe.sha(root / 'manifest.json'), cursor=0, hops=len(hops))
    return dict(native_rc=0, hops=len(hops), mode=manifest['mode'])


def started(args):
    handle = State(evidence_root(args.evidence))
    handle.require('starting')
    handle.save(stage='started')
    return dict(native_rc=0)


def serve_request(root, state, payload, timeout):
    probe.require(probe.sha(root / 'manifest.json') == state['manifest_sha256'], 'manifest_changed')
    return workload.request(root / 'observer', root / 'manifest.json', payload, timeout)


def observe(args, handle, state, binding):
    component, action = DIRECTIONS[args.direction]
    component = component or state['component']
    probe.require(args.nodes.split() == state['nodes'], 'observe_topology')
    manifest = probe.read_json(handle.root / 'manifest.json')
    block = manifest['hops'][state['cursor']:state['cursor'] + len(state['nodes'])]
    wanted = [(component, action, node) for node in state['nodes']]
    window = node_observations(handle, binding, 'window')
    if [(h['component'], h['action'], h['host']) for h in block] == wanted:
        serve_request(handle.root, state, dict(verb='observe', hops=[h['id'] for h in block]), 30)
        state['cursor'] += len(block)
    else:
        # Only a declared refusal window may observe a command with no hop.
        probe.require(args.direction in profile_of(state).get('windows', ()), 'undeclared_observation')
        block = []
    state['blocks'].append(dict(direction=args.direction, nodes=state['nodes'], hops=[h['id'] for h in block],
                                window=window))
    handle.save(blocks=state['blocks'], cursor=state['cursor'])
    return dict(native_rc=0, hops=[h['id'] for h in block])


def fault(args, handle, state, binding):
    allowed = profile_of(state).get('faults', ())
    probe.require(args.point in allowed and args.point not in state['faults'], 'undeclared_fault')
    probe.require(args.point != 'nats-n2' or state['topology'] == 'pair', 'fault_topology')
    probe.require(args.point not in QUORUM_FAULTS or state['topology'] == 'quorum', 'fault_topology')
    window = node_observations(handle, binding, 'fault')
    entry = need(binding, 'faults', args.point)
    raw = run_spec(entry['install'], ['--evidence', str(handle.root / 'faults' / args.point), '--point', args.point,
                                      '--nodes', ' '.join(state['nodes'])], 120)
    receipt = workload.decode(raw)
    probe.require(receipt == dict(receipt, point=args.point, installed=True, nodes=state['nodes'])
                  and set(receipt) == {'point', 'installed', 'nodes', 'at_ns', 'mechanism'}
                  and probe.uint(receipt['at_ns'], 1) and isinstance(receipt['mechanism'], str)
                  and receipt['mechanism'], 'fault_install_receipt')
    state['faults'][args.point] = dict(install=handle.record('fault-' + args.point, raw), window=window,
                                       healed=None)
    handle.save(faults=state['faults'])
    return dict(native_rc=0, point=args.point)


def heal(args, handle, state, binding, cleanup=False):
    record = state['faults'].get(args.point)
    probe.require(record is not None and record['healed'] is None, 'heal_without_fault')
    recovery = getattr(args, 'recovery_topology', None)
    # Only the N2 pair heals by the product's shrink to a supported topology.
    probe.require(cleanup or (recovery == 'sole-voter') == (args.point == 'nats-n2'), 'heal_recovery_topology')
    arguments = ['--evidence', str(handle.root / 'faults' / args.point), '--point', args.point,
                 '--nodes', ' '.join(state['nodes'])] + (['--recovery-topology', recovery] if recovery else [])
    raw = run_spec(binding['faults'][args.point]['heal'], arguments + (['--cleanup'] if cleanup else []), 300)
    receipt = workload.decode(raw)
    topology = recovery or state['topology']
    probe.require(set(receipt) == {'point', 'healed', 'topology', 'nodes', 'at_ns'} and receipt['point'] == args.point
                  and receipt['healed'] is True and receipt['topology'] == topology
                  and receipt['nodes'] == list(TOPOLOGY[topology]) and probe.uint(receipt['at_ns'], 1),
                  'fault_heal_receipt')
    record['healed'] = handle.record('heal-' + args.point, raw)
    handle.save(faults=state['faults'], topology=topology, nodes=list(TOPOLOGY[topology]))
    return dict(native_rc=0, topology=topology)


def exercise(args, handle, state, binding):
    probe.require(args.scenario in profile_of(state).get('exercises', ()) and args.scenario not in state['exercises'],
                  'undeclared_exercise')
    raw = run_spec(need(binding, 'exercises', args.scenario), ['--evidence', str(handle.root / 'exercises' / args.scenario),
                   '--scenario', args.scenario, '--nodes', ' '.join(state['nodes'])], 600)
    state['exercises'][args.scenario] = handle.record('exercise-' + args.scenario, raw)
    handle.save(exercises=state['exercises'])
    return dict(native_rc=0, scenario=args.scenario)


def stage_config(args, handle, state, binding):
    probe.require(args.variant in profile_of(state).get('configs', ()) and args.variant not in state['configs'],
                  'undeclared_config_variant')
    raw = run_spec(need(binding, 'configs', args.variant),
                   ['--evidence', str(handle.root / 'configs' / args.variant), '--variant', args.variant], 120)
    receipt = workload.decode(raw)
    probe.require(set(receipt) == {'variant', 'staged', 'sha256'} and receipt['variant'] == args.variant
                  and receipt['staged'] is True and probe.digest(receipt['sha256']), 'config_stage_receipt')
    state['configs'][args.variant] = dict(receipt=handle.record('config-' + args.variant, raw),
                                          sha256=receipt['sha256'])
    handle.save(configs=state['configs'], staged=args.variant)
    return dict(native_rc=0, variant=args.variant)


def config_digest(args, handle, state, binding):
    probe.require(state.get('staged') in state['configs'], 'no_staged_config')
    return state['configs'][state['staged']]['sha256']


def transaction(args, handle, state, binding):
    """Tx from the halted command's own output, confirmed by product status."""
    log = state.get('halted_log')
    probe.require(log is not None, 'transaction_without_halt')
    found = set(re.findall(r'(?<![0-9a-f])[0-9a-f]{32}(?![0-9a-f])', Path(log).read_text(errors='replace')[:1 << 20]))
    probe.require(len(found) == 1, 'halted_output_tx_not_unique')
    tx = found.pop()
    base = [args.sim, 'exec', 'brk1', '--', 'runuser', '-u', 'tether', '--', 'env', 'HOME=/var/lib/tether',
            '/usr/local/bin/tether']
    if state['topology'] == 'single':
        argv = base + ['admin', 'infra', 'status', '--tx', tx]
    else:
        argv = base + ['cluster', 'upgrade', '--component', state['component'], '--status', tx,
                       '--account-seed', '/etc/tether/secrets/account.nk']
    probe.require(Path(args.sim).is_absolute(), 'sim_driver_required')
    rc, raw = supervisor().run(argv, 60)
    try:
        status = workload.decode(raw)
        valid = (rc == 0 and isinstance(status, dict) and set(status) == set(STATUS_FIELDS)
                 and isinstance(status['Tx'], list) and len(status['Tx']) == 16
                 and all(type(b) is int and 0 <= b < 256 for b in status['Tx']))
    except ValueError:
        valid = False
    probe.require(valid, 'NOT_RUN pending S9 P24 infra status receipt (rc %d)' % rc)
    probe.require(bytes(status['Tx']).hex() == tx and status['Phase'] == HALTED, 'status_not_halted_tx')
    state['tx'] = dict(tx=tx, status=handle.record('status-' + tx, raw))
    handle.save(tx=state['tx'])
    return tx


def check(args, handle, state, binding):
    scope = args.scope
    allowed = GENERIC_SCOPES + profile_of(state).get('scopes', ())
    if profile_of(state)['plan'] in ('cycle', 'support', 'roll'):
        allowed += ('round', 'cycle') if profile_of(state)['plan'] != 'roll' else ('round',)
    probe.require(scope in allowed and not (scope == 'bootstrap' and state['profile'] == 'legacy-capability'),
                  'undeclared_scope')
    given = {k for k in ('node', 'command_rc', 'command_log', 'direction', 'nodes', 'expected_hops', 'final_version',
                         'point', 'tx', 'config_sha256', 'variant', 'action') if getattr(args, k) is not None}
    probe.require(given == set(SCOPES[scope]), 'scope_arguments')
    probe.require(state['stage'] == ('finished' if scope == 'logs' else 'started'), 'verb_out_of_order')
    if args.command_log is not None:
        log = Path(args.command_log)
        probe.require(log.parent == handle.root and re.fullmatch(r'step-[0-9]+\.log', log.name) is not None
                      and log.is_file() and re.fullmatch(r'[0-9]{1,3}', args.command_rc) is not None, 'command_receipt')
    joining = scope in ('round', 'cycle', 'reload', 'support', 'recovered', 'retirement')
    snapshot = None
    if state['stage'] == 'started':
        snapshot = serve_request(handle.root, state, dict(verb='snapshot', progress=1 if scope == 'baseline' else 0,
                                 join=probe.budgets()['HOT_UPDATE_REGISTER_DEADLINE_S'] + 20 if joining else 0), 200)
    observed = node_observations(handle, binding, scope) if scope != 'logs' else {}
    faults = {}
    if scope in ('refusal', 'halted'):
        entry = state['faults'].get(args.point)
        probe.require(entry is not None, 'check_without_fault')
        faults[args.point] = handle.record('fault-status-' + args.point,
                                           run_spec(binding['faults'][args.point]['status'], ['--evidence',
                                                    str(handle.root / 'faults' / args.point), '--point', args.point], 60))
    bundle = dict(scope=scope, arguments={k: getattr(args, k) for k in sorted(given)}, root=str(handle.root),
                  manifest=str(handle.root / 'manifest.json'), trace=str(handle.root / 'observer/trace.json'),
                  state=state, snapshot=snapshot, observed=observed, fault_status=faults, here=str(HERE))
    if scope == 'halted':
        handle.save(halted_log=args.command_log)
    path = handle.root / 'fixture' / ('check-%03d-%s.json' % (len(state['checks']), scope))
    workload.publish(path, bundle)
    rc, output = supervisor().run([sys.executable, '-I', '-B', str(LIB / 'hot_update_infra_scope.py'), 'judge',
                                   '--bundle', str(path)], 120)
    sys.stdout.write(output.decode(errors='replace'))
    state['checks'].append(dict(scope=scope, rc=rc, bundle=str(path)))
    updates = dict(checks=state['checks'])
    if scope == 'baseline' and rc == 0:
        updates['baseline'] = observed
    handle.save(**updates)
    return rc


def finish(args, handle, state, binding):
    result = serve_request(handle.root, state, dict(verb='finish'),
                           state_seconds(handle) + probe.budgets()['HOT_UPDATE_REGISTER_DEADLINE_S'] + 120)
    workload.publish(handle.root / 'fixture/final.json', result)
    handle.save(stage='finished', final=result.get('native_rc'))
    sys.stdout.write(json.dumps(result, sort_keys=True) + '\n')
    return result['native_rc']


def state_seconds(handle):
    return probe.read_json(handle.root / 'manifest.json')['seconds']


def stop(args):
    """Cleanup half run by the shell before it joins its two supervised jobs."""
    root = Path(args.evidence)
    if not (root / 'fixture/state.json').exists():
        return 0
    handle = State(evidence_root(args.evidence))
    state = handle.value
    if state.get('stage') == 'closed':
        return 0
    problems = []
    if state.get('stage') in ('starting', 'started', 'finished') and (root / 'observer/fixture.sock').exists():
        try:
            workload.request(root / 'observer', root / 'manifest.json', dict(verb='stop'), 120)
        except (OSError, ValueError, KeyError, TypeError) as error:
            problems.append('observer_stop:' + type(error).__name__)
    try:
        binding = load_binding(state)
        # Every instrument is removed by cleanup; a fault never outlives its drill.
        for point, record in state.get('faults', {}).items():
            if record['healed'] is None:
                heal(argparse.Namespace(point=point), handle, state, binding, cleanup=True)
        if (root / 'cohort').is_dir() and not exited(root, 'cohort'):
            run_spec(binding['cohort']['control'], ['--evidence', str(root / 'cohort'), 'stop'], 300)
    except (OSError, ValueError, KeyError, TypeError) as error:
        problems.append('composer_stop:' + str(error)[:120])
    handle.save(stopped=problems)
    return 2 if problems else 0


def closed(args):
    root = Path(args.evidence)
    if not (root / 'fixture/state.json').exists():
        return 0
    handle = State(evidence_root(args.evidence))
    if handle.value.get('stage') == 'closed':
        return 0
    # Native receipts are written by the shared supervisor only after the
    # descendant tree is reaped; a missing or nonzero receipt is not clean.
    receipts = {}
    for label in ('cohort', 'fixture'):
        if (root / (label + '.out')).exists():
            path = root / (label + '.rc')
            receipts[label] = path.read_text().strip() if path.exists() else 'missing'
    problems = handle.value.get('stopped', ['stop_not_run'])
    handle.save(stage='closed', receipts=receipts)
    return 0 if not problems and all(value == '0' for value in receipts.values()) else 2


VERBS = dict(observe=observe, fault=fault, heal=heal, exercise=exercise, check=check, finish=finish,
             transaction=transaction, **{'stage-config': stage_config, 'config-digest': config_digest})


def parser():
    result = argparse.ArgumentParser(description=__doc__.splitlines()[0], allow_abbrev=False)
    result.add_argument('verb', choices=('preflight', 'prepare', 'cohort-exec', 'prepared', 'start', 'started', 'stop',
                                         'closed') + tuple(VERBS))
    result.add_argument('scope', nargs='?', choices=tuple(SCOPES))
    result.add_argument('--evidence', required=True)
    for name in ('component', 'profile', 'topology', 'nodes', 'agents', 'ctl', 'carrier-transition', 'initial-version',
                 'direction', 'point', 'recovery-topology', 'scenario', 'variant', 'node', 'command-rc', 'command-log',
                 'final-version', 'tx', 'config-sha256', 'action', 'sim'):
        result.add_argument('--' + name)
    for name in ('probe-rate', 'request-budget', 'flow-budget', 'transfer-budget', 'expected-hops'):
        result.add_argument('--' + name, type=int)
    return result


# Exact flags per verb: an unknown or missing flag is a SETUP error, never ignored.
FLAGS = {
    'preflight': {'component', 'profile'}, 'prepare': {'component', 'topology', 'profile', 'nodes', 'agents', 'ctl',
                                                         'carrier_transition', 'initial_version'},
    'start': {'carrier_transition', 'probe_rate', 'request_budget', 'flow_budget', 'transfer_budget'},
    'observe': {'direction', 'nodes'}, 'fault': {'point'}, 'exercise': {'scenario'}, 'stage-config': {'variant'},
    'transaction': {'sim'}, 'config-digest': set(), 'finish': set(),
}


def main(argv):
    args = parser().parse_args(argv)
    given = {k for k, v in vars(args).items() if v is not None and k not in ('verb', 'scope', 'evidence')}
    if args.verb == 'heal':
        probe.require(given in ({'point'}, {'point', 'recovery_topology'}), 'verb_arguments')
    elif args.verb == 'check':
        # Exact per-scope flags are checked in check(); any other flag is refused here.
        probe.require(args.scope is not None and given <= {f for row in SCOPES.values() for f in row}, 'verb_arguments')
    elif args.verb in FLAGS:
        probe.require(given == FLAGS[args.verb], 'verb_arguments')
    else:
        probe.require(not given, 'verb_arguments')
    probe.require((args.scope is not None) == (args.verb == 'check'), 'verb_arguments')
    probe.require(args.direction is None or args.direction in DIRECTIONS, 'unknown_direction')
    simple = dict(preflight=preflight, prepare=prepare, prepared=prepared, start=start, started=started)
    if args.verb == 'cohort-exec':
        cohort_exec(args)
    if args.verb in ('stop', 'closed'):
        return (stop if args.verb == 'stop' else closed)(args), None
    if args.verb in simple:
        result = simple[args.verb](args)
        return 0, json.dumps(result, sort_keys=True)
    handle = State(evidence_root(args.evidence))
    state = handle.require('started', 'finished') if args.verb == 'check' else handle.require('started')
    result = VERBS[args.verb](args, handle, state, load_binding(state))
    if args.verb in ('check', 'finish'):
        return result, None
    if args.verb in ('transaction', 'config-digest'):
        return 0, result
    return 0, json.dumps(result, sort_keys=True)


if __name__ == '__main__':
    rc, line = 2, None
    try:
        rc, line = main(sys.argv[1:])
    except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
        # Diagnostics name the failed custody code; no receipt bytes or keys.
        print(json.dumps(dict(native_rc=2, verdict='SETUP-RED', failure=str(error)[:240])), file=sys.stderr)
        rc = 2
    if line is not None:
        print(line)
    evidence = next((sys.argv[i + 1] for i, a in enumerate(sys.argv[:-1]) if a == '--evidence'), None)
    if evidence and Path(evidence, 'fixture').is_dir() and sys.argv[1:2] != ['cohort-exec']:
        ledger(evidence, sys.argv[1], sys.argv[2:], rc)
    sys.exit(rc)
