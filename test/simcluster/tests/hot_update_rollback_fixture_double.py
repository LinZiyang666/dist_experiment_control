#!/usr/bin/env python3
"""Deployment-boundary double for drill 38's P26 fixture (r9d D-11 arm controls only).

origin: hot-update S8 plan §9.0 / §9.2 / §11.2 P27; r9d D-11.

tests/hot-update-rollback-drill-test.py copies this file into a private sandbox
as drills/lib/upgrade_tx_fixture.py. The REAL drill-38 runner (the embedded PY38
program, fed by the real drill's heredoc) then loads it through its own
load_fixture(). Only what crosses the deployment boundary is replaced here: the
live workload start/stop, the product observe/drain ports, the P26 judge, the
companion producer, the supervisor's command execution and the mutation build
supplier. Every validator that is not a deployment effect stays REAL: the
manifest is read by the real upgrade_tx_fixture.manifest_read, commands are
checked by the real supervisor.check_spec, evidence is read/published/digested by
the real collector, and the mutation recipe is bound by the real
upgrade_tx_faults.mutation_recipe of the arm's own table row.

Nothing here is a cut, a judge verdict or a deployment receipt. The scenario file
(R9D38_SCENARIO) selects what the boundary answers; the trace (R9D38_TRACE) lets
the driver prove the runner really walked the arm's declared operations.
"""
import importlib.util
import json
import os
from pathlib import Path
import re
import sys

# Deliberately NOT resolved: the sandbox directory holds the faults double that
# the runner loads from fixture.HERE.
HERE = Path(__file__).parent
REAL_LIB = Path(os.environ['R9D38_REAL_LIB'])


def _load(name, path):
    module = sys.modules.get(name)
    if module is None:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return module


# The real fixture loads the real collector (and through it the real supervisor
# module) under their production module names; the doubles below delegate every
# pure validator to them.
real_fixture = _load('r9d38_real_fixture', REAL_LIB / 'upgrade_tx_fixture.py')
real_collect = real_fixture.collect
real_supervisor = real_fixture.supervisor
real_faults = _load('r9d38_real_faults', REAL_LIB / 'upgrade_tx_faults.py')
_state = dict(op='start', samples=0)


def scenario():
    return json.loads(Path(os.environ['R9D38_SCENARIO']).read_text())


def clock_now():
    clock = sys.modules.get('r9d38_clock')
    return None if clock is None else clock.t


def trace(event, **fields):
    row = dict(event=event, op=_state['op'], clock=clock_now(), **fields)
    with open(os.environ['R9D38_TRACE'], 'a') as stream:
        stream.write(json.dumps(row, sort_keys=True) + '\n')


def note_operation(name):
    # Shared with the faults double: inject/release are operations the runner
    # samples after, exactly like a supervised command.
    _state['op'] = name


OLD, NEW = 'a' * 64, 'b' * 64
ORIGIN = dict(pid=4242, start_ticks=777)
SUCCESSOR = dict(pid=4242, start_ticks=777, instance_id='successor-runtime')


def _resources(moved=(), extra=()):
    rows = [dict(id=name, origin_owner=dict(ORIGIN), current_owner=dict(SUCCESSOR if name in moved else ORIGIN))
            for name in ('exec.original', 'flow.original')]
    rows += [dict(id=name, origin_owner=dict(SUCCESSOR), current_owner=dict(SUCCESSOR)) for name in extra]
    return rows


def _receipts(prefix, complete):
    return dict(control_receipt=dict(evidence_digest=prefix + '-control'),
                resources_receipt=dict(complete=complete, evidence_digest=prefix + '-resources'),
                released_receipt=dict(evidence_digest=prefix + '-released'))


def baseline():
    return dict(running_sha=OLD, pid=4242, start_ticks=777, owner_boot_id='boot-r9d38', sequence=1,
                direction='none', cut_id='cut-baseline', generation=1, resources=_resources(),
                **_receipts('baseline', True))


def _sample_for(op, arm):
    """Public observation the product ports would return after operation `op`.

    The model is the honest one for each arm's declared operations: an upgrade
    moves the running image forward, partial-take moves only part of the
    original roster, new-request admits a new resource, rollback reverses with a
    fresh cut/generation and fresh receipts. Arms whose samples the runner does
    not inspect still see a well-formed, identity-preserving row.
    """
    row = baseline()
    forward = dict(running_sha=NEW, direction='forward', cut_id='cut-forward', generation=2, sequence=2,
                   **_receipts('forward', False))
    if op in ('upgrade', 'salvage', 'partial-take', 'new-request'):
        row.update(forward)
        if op in ('partial-take', 'salvage'):
            row['resources'] = _resources(moved=('flow.original',))
        if op == 'new-request':
            row.update(sequence=3, resources=_resources(extra=('exec.new-request',)))
    elif op == 'rollback' and arm in ('R1', 'R2'):
        row.update(direction='reverse', cut_id='cut-reverse', generation=3, sequence=4, **_receipts('reverse', True))
    return row


def manifest_read(path):
    # REAL manifest validator: actors/ports/faults/expect/budget are all checked.
    return real_fixture.manifest_read(path)


def start(root, manifest, lifeline_fd):
    trace('start', lifeline=type(lifeline_fd) is int)
    wanted = scenario()
    if wanted.get('start_error'):
        raise real_collect.EvidenceError(wanted['start_error'], 2)
    root = Path(root)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    real_collect.require(not list(root.iterdir()), 'evidence_directory_not_empty', 1)
    real_collect.publish(root / 'manifest.json', manifest)
    body = dict(version=1, manifest_digest=real_collect.digest(manifest), run_id=manifest['run_id'],
                domains=sorted(manifest['domains']), resources=[dict(id=r['id']) for r in baseline()['resources']])
    originals = dict(body, original_set_digest=real_collect.digest(body))
    real_collect.publish(root / 'originals.json', originals)
    real_collect.publish(root / 'start-observation.json', baseline())
    return dict(rc=0, verdict='GREEN', run_id=manifest['run_id'], original_set_digest=originals['original_set_digest'])


def drain(root, manifest):
    trace('drain')
    return dict(rc=0, verdict='GREEN', independent_drains=2)


def stop(root, manifest):
    wanted = scenario()
    trace('stop')
    return dict(rc=0, verdict='GREEN', empty=wanted.get('stop_empty', True), remaining=wanted.get('stop_remaining', []))


class _Supervisor:
    SupervisionError = real_supervisor.SupervisionError
    check_spec = staticmethod(real_supervisor.check_spec)

    @staticmethod
    def call(root, request):
        real_collect.require(request.get('op') == 'run', 'double_supports_run_only')
        payload = request['input']
        originals = real_collect.read(Path(root) / 'originals.json')
        real_collect.require(payload['evidence'] == str(root) and payload['original_set_digest'] ==
                             originals['original_set_digest'], 'double_input_not_bound_to_originals')
        op = payload['operation']
        note_operation(op)
        wanted = scenario()
        rc = wanted['command_rc'][op]
        trace('run', operation=op, argv=request['spec']['argv'], native_rc=rc, arm=payload['arm'])
        if wanted['family'] == 'M' and op == 'prepare':
            # The live adapter names the mutated owner incarnation after prepare.
            real_collect.publish(Path(root) / 'mutation-owner.json', dict(pid=os.getpid(), start_ticks=1))
        return dict(native_rc=rc, stdout=json.dumps(dict(operation=op, native_rc=rc)))


supervisor = _Supervisor()


class _MutationTools:
    @staticmethod
    def validate_mutation_build(path, row):
        record = real_collect.read(path)
        # REAL recipe binding: the arm's own table row decides file/symbol/edit.
        recipe = real_faults.mutation_recipe(row)
        real_collect.require(record['arm'] == row['arm'] and record['recipe'] == recipe, 'mutation_wrong_symbol_or_edit')
        trace('validate_mutation_build', arm=row['arm'], build=recipe['build'])
        return dict(artifact=record['artifact'], patch_digest=record['patch_digest'], recipe=recipe,
                    binary_sha=record['binary_sha'])


class _Collect:
    EvidenceError = real_collect.EvidenceError
    DOMAINS = real_collect.DOMAINS
    PROFILE = real_collect.PROFILE
    VERDICTS = real_collect.VERDICTS
    read = staticmethod(real_collect.read)
    publish = staticmethod(real_collect.publish)
    digest = staticmethod(real_collect.digest)
    require = staticmethod(real_collect.require)

    @staticmethod
    def mutation_tools():
        return _MutationTools

    @staticmethod
    def sample(root, case_id, nonce):
        manifest = real_collect.read(Path(root) / 'manifest.json')
        real_collect.require(case_id == manifest['case_id'], 'case_not_declared')
        real_collect.require(re.fullmatch('[0-9a-f]{32}', nonce), 'query_nonce_invalid')
        wanted = scenario()
        op = _state['op']
        _state['samples'] += 1
        advance = wanted.get('advance_clock_after', {}).get(op)
        clock = sys.modules.get('r9d38_clock')
        if advance and clock is not None:
            clock.t += advance
        row = _sample_for(op, manifest['arm'])
        row.update(wanted.get('sample_overrides', {}).get(op, {}))
        trace('sample', operation=op)
        return row

    @staticmethod
    def produce_companion(root, manifest, contract, shared, timeout_s):
        trace('produce_companion', arm=contract['arm'], shared=shared, timeout_ok=timeout_s > 0)
        wanted = scenario()
        if wanted.get('companion_error'):
            raise real_collect.EvidenceError(wanted['companion_error'], 2)

    @staticmethod
    def judge(root, manifest):
        wanted = scenario()
        originals = real_collect.read(Path(root) / 'originals.json')
        rc = wanted['judge_rc']
        proof = dict(version=1, run_id=wanted.get('judge_run_id', manifest['run_id']),
                     arm=wanted.get('judge_arm', manifest['arm']), source_commit=manifest['source_commit'],
                     original_set_digest=originals['original_set_digest'], proofs=wanted['judge_proofs'],
                     rc=wanted.get('judge_scenario_rc', rc))
        trace('judge', rc=rc)
        return dict(rc=rc, verdict=real_collect.VERDICTS[rc], scenario=proof)


collect = _Collect()
