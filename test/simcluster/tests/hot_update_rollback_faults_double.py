#!/usr/bin/env python3
"""Deployment-boundary double for drill 38's P26 fault injector (r9d D-11 arm controls only).

origin: hot-update S8 plan §8.2 / §8.3 / §11.2 P27; r9d D-11.

Copied into the private sandbox as drills/lib/upgrade_tx_faults.py next to the
fixture double; the REAL drill-38 runner loads it from fixture.HERE. The fault
must still be predeclared in the arm's manifest (the REAL upgrade_tx_faults
.declared decides that) and a mutant's build is still bound to the arm's own
recipe by the real mutation_recipe; only the environment side effect (namespace,
prlimit, ptrace...) and the /proc read of the mutated owner are replaced. An
injector whose ACK fails raises the same EvidenceError(4) the real one raises.
"""
from pathlib import Path
import sys

fixture = sys.modules['upgrade_tx_fixture']
collect = fixture.real_collect


def arm(root, manifest, case_id):
    fixture.real_faults.declared(manifest, case_id)
    fixture.note_operation('inject')
    fixture.trace('inject', case=case_id)
    if fixture.scenario().get('inject_error'):
        raise collect.EvidenceError('fault_injector_native_failure', 4)
    return dict(rc=0, verdict='GREEN', case=case_id, applied=True, hit_proven=False)


def release(root, manifest, case_id):
    fixture.real_faults.declared(manifest, case_id)
    fixture.note_operation('release')
    fixture.trace('release', case=case_id)
    if fixture.scenario().get('release_error'):
        raise collect.EvidenceError('fault_injector_native_failure', 4)
    return dict(rc=0, verdict='GREEN', case=case_id, released=True)


def observe_mutation(root, manifest, row, build_path):
    build = fixture.collect.mutation_tools().validate_mutation_build(build_path, row)
    owner = collect.read(Path(root) / 'mutation-owner.json')
    fixture.trace('observe_mutation', arm=row['arm'], owner=owner['pid'])
    if fixture.scenario().get('mutation_not_executed'):
        raise collect.EvidenceError('mutation_binary_not_executed', 4)
    result = dict(version=1, arm=row['arm'], run_id=manifest['run_id'], owner=owner,
                  binary_sha=build['binary_sha'], patch_digest=build['patch_digest'])
    collect.publish(Path(root) / 'mutation-execution.json', result)
    return result
