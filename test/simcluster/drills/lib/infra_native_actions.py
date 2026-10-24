#!/usr/bin/env -S python3 -I -B
"""Supervised native fault/exercise/config operation custody.

Network faults use the existing drill fault library. A compound fault requires
a deployment's real syscall instrument as a pinned install/status/heal recipe;
its response is bound by that foreground invocation, never a stored expected
receipt. Exercises contain actual product command specs plus optional setup
and mandatory cleanup. The command's native rc determines the reported outcome.
Missing live responsibilities or target instruments remain named NOT_RUN.
origin: hot-update S9 plan §3 / §5 / §9.1 / L22; S9 INSTRUMENTS-3.
"""
from pathlib import Path
import re
import time

import infra_composer_contract as contract
import infra_owner_plan as plans
import infra_native as native
import infra_native_targets as targets
import hot_update_infra_scope as scope
import hot_update_infra_workload as workload

NETWORK = ('nats-lost-quorum', 'nats-stream-lag', 'nats-consumer-lag')
EXERCISE_ROWS = {'store-double-open': ('second-launch', 'legacy-owner'),
    'exact-release-join': ('mismatched-release', 'exact-release'), 'caddy-responsibility-gates': scope.GATES}


def network_spec(plan, point, operation, arguments):
    flow = plan.get('flow', {})
    plans.need(set(flow) == {'here', 'sim'}, 'faults.' + point + '.' + operation, 'simcluster_fault_library_required')
    script = plans.LIB / 'infra_native_faults.sh'
    shell = str(Path('/bin/sh').resolve())
    return dict(argv=[shell, str(script), flow['here'], flow['sim']['path'], operation, point, *arguments],
        sha256=contract.sha(shell), inputs=[contract.pin(script), flow['sim'],
            *[contract.pin(Path(flow['here']) / name) for name in
                ('lib/docker.sh', 'lib/log.sh', 'drills/lib/fault.sh')]])


def fault(plan, config, ctx, port, arguments, timeout):
    _, point, operation = port.split('.')
    source = config.get('faults', {}).get(point, {})
    spec = source.get(operation)
    # These default mechanisms are actual network cable pulls. Stream versus
    # consumer lag still requires the independent JS status instrument: a rule
    # being installed is not evidence that a particular asset group is lagging.
    if spec is None and point in NETWORK and operation in ('install', 'heal'):
        spec = network_spec(plan, point, operation, arguments)
    plans.need(spec is not None, port, 'deployment_fault_' + operation + '_instrument_required')
    _, raw = native.invoke(plan, ctx, port, spec, arguments if operation == 'status' or source.get(operation) else [], timeout)
    payload = workload.decode(raw)
    from infra_owner_ports import public
    public(port, payload)
    if operation == 'status':
        # Native booleans are never inferred from a successful injector or from
        # the requested point name. The status reader observes actual hits.
        plans.need(payload['hits'] == 0 or payload['digest'] is not None, port, 'fault_hit_digest_required')
    return payload


def exercise(plan, config, ctx, port, arguments, timeout):
    name = port.split('.')[1]
    recipes = config.get('exercises', {}).get(name, {})
    plans.need(set(recipes) == set(EXERCISE_ROWS[name]), port, 'real_product_exercise_recipes_required')
    before = targets.business(plan, config, ctx)
    observed = native.node_observation(plan, config, ctx, ctx['nodes'][0], ctx['component'])
    rows = []
    for label in EXERCISE_ROWS[name]:
        recipe = recipes[label]
        plans.need(type(recipe) is dict and {'command', 'cleanup'} <= set(recipe) <= {'setup', 'command', 'cleanup'},
                   port, 'exercise_recipe_fields')
        command = contract.spec(recipe['command'])
        # A command must actually invoke the admitted real product, either
        # directly or through the pinned deployment transport. A JSON-producing
        # assertion script cannot stand in for a product refusal/join result.
        executable = Path(command['argv'][0])
        plans.need(executable == Path(plan['product']['binary']['path']) or
                   executable == Path(config['sim']['path']), port, 'real_product_command_required')
        try:
            if 'setup' in recipe:
                native.invoke(plan, ctx, port, recipe['setup'], arguments, min(timeout, 120))
            rc, raw = native.invoke(plan, ctx, port, command, [], min(timeout, 120), allowed=(0, 64, 74, 75))
        finally:
            # Cleanup is unconditional, including setup failure. Missing native
            # cleanup success is not turned into a retired temporary process.
            native.invoke(plan, ctx, port, recipe['cleanup'], arguments, min(timeout, 120))
        after = targets.business(plan, config, ctx)
        changed = any(after[k]['identity'] != before[k]['identity'] or
            after[k]['accepts'] != before[k]['accepts'] or after[k]['redials'] != before[k]['redials'] for k in before)
        codes = set(re.findall(rb'(?<![a-z_])infra_[a-z_]+(?![a-z_])', raw))
        plans.need(rc == 0 or len(codes) == 1, port, 'product_refusal_code_required')
        outcome = {64: 'refused', 74: 'halted', 75: 'deferred'}.get(rc, 'joined' if label == 'exact-release' else 'opened')
        code = next(iter(codes)).decode() if codes else ''
        rows.append(dict(name=label, outcome=outcome, code=code, forced_close=changed))
    current = [native.node_observation(plan, config, ctx, node, ctx['component']) for node in ctx['nodes']]
    return dict(scenario=name, rows=rows, holder=observed['server'] if name == 'store-double-open' else None,
                retired=all(not row['temporary'] and row['census']['old_owner_pending'] == 0 for row in current))


def final(plan, config, ctx, arguments, timeout):
    # Native final collection requires the actual S5 raw/log projection and
    # control effects. This is intentionally a typed deployment input: neither
    # a successful wait nor the last progress row can supply those facts.
    spec = config.get('controls', {}).get('final')
    plans.need(spec is not None, 'readers.final', 's5_raw_final_and_control_instrument_required')
    _, raw = native.invoke(plan, ctx, 'readers.final', spec, arguments, timeout)
    payload = workload.decode(raw)
    from infra_owner_ports import public
    return public('readers.final', payload)


def action(plan, config, ctx, port, arguments, timeout):
    if port.startswith('faults.'):
        return fault(plan, config, ctx, port, arguments, timeout)
    return exercise(plan, config, ctx, port, arguments, timeout)
