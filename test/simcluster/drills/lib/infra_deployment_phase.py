"""Join original hop events, two journal readers and a fresh P02 observation.

The event exporter must retain original start/finish times and temporary
incarnations after retirement. Poll times and Status.Current cannot substitute
for those facts. Root and transaction revisions come from separate exports.
Absent product exports are named NOT_RUN, before publishing a phase sample.
origin: hot-update S9 plan §2.3 / §9.1; S9 INSTRUMENTS-5.
"""
from pathlib import Path
import time

import infra_composer_contract as contract
import infra_native as native
import infra_owner_plan as plans
import hot_update_infra_workload as workload

PORT = 'readers.sample'
KINDS = {'started': {'before', 'config'}, 'temporary': {'process'},
         'main_ready': {'socket'}, 'returned': set(), 'finished': {'config'}}


def need(ok, reason):
    plans.need(ok, PORT, reason)


def hop_binding(ctx, hop):
    need(type(hop) is str and hop.startswith('hop-'), 'declared_hop_required')
    index = int(hop[4:])
    need(hop == 'hop-' + str(index) and 0 <= index < len(ctx['hops']), 'declared_hop_required')
    return ctx['hops'][index][:3]


def export(plan, ctx, source, arguments):
    plans.need(type(source) is dict and 'argv' in source, PORT, 'original_phase_export_required')
    _, raw = native.invoke(plan, ctx, PORT, source,
        ['--context', str(Path(ctx['evidence']) / 'context.json'), *arguments], 120)
    return workload.decode(raw)


def project(ctx, hop, phase, batch, observed, root, transaction, sockets, now_ns):
    component, action, node = hop_binding(ctx, hop)
    need(phase in contract.probe.PHASES, 'observed_phase_required')
    need(type(batch) is dict and set(batch) == {'context_sha256', 'hop', 'phase', 'node',
         'component', 'action', 'tx', 'step', 'events'} and
         batch['context_sha256'] == contract.sha(Path(ctx['evidence']) / 'context.json') and
         (batch['hop'], batch['phase'], batch['node'], batch['component'], batch['action']) ==
         (hop, phase, node, component, action) and contract.probe.uint(batch['step'], 1) and
         type(batch['tx']) is str and len(batch['tx']) == 32 and
         all(ch in '0123456789abcdef' for ch in batch['tx']), 'original_phase_binding')
    events = batch['events']
    need(type(events) is list and 0 < len(events) <= 4096, 'bounded_original_events_required')
    groups = {kind: [] for kind in KINDS}
    prior = 0
    for event in events:
        need(type(event) is dict and event.get('kind') in KINDS, 'original_event_kind')
        kind = event['kind']
        need(set(event) == {'kind', 'at_ns'} | KINDS[kind] and
             contract.probe.uint(event['at_ns'], 1) and prior <= event['at_ns'] <= now_ns,
             'original_event_time_or_fields')
        prior = event['at_ns']
        groups[kind].append(event)
    need(len(groups['started']) == 1 and events[0]['kind'] == 'started', 'one_original_start_required')
    need(all(len(groups[kind]) <= 1 for kind in ('finished', 'returned')), 'one_original_finish_required')
    started = groups['started'][0]
    before = native.process(started['before'])
    current = native.process(observed['server'])
    need(contract.probe.digest(started['config']) and contract.probe.digest(observed['config']), 'config_facts_required')
    census = observed['census']
    need(census['producer'] == 'kernel-reader' and census['complete'] is True and
         contract.probe.uint(census['generation'], 1) and current in census['processes'], 'complete_physical_census_required')
    for process in census['processes']:
        contract.probe.process(process)
    if phase == 'before':
        need(before == current and before in census['processes'], 'before_not_physical_original')
    for row in (root, transaction):
        need(type(row) is dict and set(row) == {'tx', 'step', 'revision'} and
             row['tx'] == batch['tx'] and row['step'] == batch['step'] and
             contract.probe.uint(row['revision'], 1), 'independent_journal_binding')
    temporary = [native.process(event['process']) for event in groups['temporary']]
    need(len({contract.canonical(row) for row in temporary}) == len(temporary) and
         before not in temporary, 'temporary_incarnation_set')
    result = dict(before=before, current=current, temporary=temporary, census=census,
        root_revision=root['revision'], transaction_revision=transaction['revision'],
        started_ns=started['at_ns'], config_before=started['config'], config_after=observed['config'],
        return_verified=bool(groups['returned']))
    if groups['finished']:
        finished = groups['finished'][0]
        need(finished['config'] == observed['config'] and events[-1] == finished, 'final_config_not_executing')
        result['finished_ns'] = finished['at_ns']
    if groups['main_ready']:
        ready = groups['main_ready'][-1]['socket']
        contract.probe.socket(ready)
        need(ready['Owner'] == current and ready in sockets, 'main_ready_not_physical_main')
        result['main_ready'] = ready
    if phase == 'retired':
        need('finished_ns' in result and result['return_verified'] and
             root['revision'] == transaction['revision'] and census['old_owner_pending'] == 0 and
             census['live_business'] > 0 and all(p not in census['processes'] for p in temporary),
             'retirement_facts_incomplete')
        if action == 'reload':
            need(before == current and started['config'] != observed['config'], 'reload_identity_or_config')
        else:
            need(before not in census['processes'] and (component not in ('nats', 'caddy') or
                 not any(all(p[key] == before[key] for key in ('BootID', 'PID', 'StartTicks'))
                         for p in census['processes'])), 'old_incarnation_still_alive')
        need(component != 'caddy' or 'main_ready' in result, 'actual_main_ready_required')
    return result


def read(plan, config, ctx, hop, phase):
    declarations = config.get('phase', {})
    need(type(declarations) is dict and set(declarations) == {'events', 'root', 'transaction'},
         'original_phase_and_journal_exports_required')
    component, _, node = hop_binding(ctx, hop)
    args = [hop, phase]
    batch = export(plan, ctx, declarations['events'], args)
    root = export(plan, ctx, declarations['root'], args)
    transaction = export(plan, ctx, declarations['transaction'], args)
    # Independent P02 reads follow the event exports. A stored event can never
    # certify that a shadow, stale executable or dead main is currently serving.
    observed = native.node_observation(plan, config, ctx, node, component)
    physical = native.physical(plan, config, node, 'census')
    sockets = [dict(row, Owner=native.process(row['Owner'])) for row in physical['Sockets'] or []]
    return project(ctx, hop, phase, batch, observed, root, transaction, sockets, time.monotonic_ns())
