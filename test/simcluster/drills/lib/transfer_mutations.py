#!/usr/bin/env python3
"""S7 driver manifests and production overlays; no fixture or product oracle.

The fixture owns observations and its native judge. This module checks driver
coverage and binds mutation patches/builds/running artifacts to archived inputs.
Missing owner patches or unlanded implementations are SETUP-RED, never kills.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import sys

# Isolated-mode entrypoints must resolve the shared projection validator.
sys.path.insert(0, str(Path(__file__).resolve().parent))

POSITIVE = 'A A1 B C1 C2 C3 R1 R2 X W1 W2'.split()
# Every split case has an independent artifact, launch and native verdict.
# origin: hot-update S7 plan §10.5 / T44.
MUTATIONS = {
    'M01': ('internal/xferclient/', 'SDK Put or new NUID after sent-unknown', 'ctl'),
    'M02': ('internal/jsstream/object_writer.go', 'remove chunk CAS receipt guard', 'ctl agent'),
    'M03': ('internal/jsstream/object_writer.go', 'publish metadata before final receipt', 'ctl agent'),
    'M04': ('internal/jsstream/object_reader.go', 'skip reader rebuild after healthy drop', 'ctl agent'),
    'M05': ('internal/jsstream/object_reader.go', 'forget dirty barrier on last close', 'ctl agent'),
    'M06': ('internal/xfertrack/', 'watchdog fails an outstanding Grant', 'broker'),
    'M07': ('internal/xfercontrol/ internal/xferworker/', 'create with expired or foreign-home permit', 'broker agent'),
    'M08A': ('internal/xferreceive/ internal/xferimage/', 'drop synced checkpoint', 'agent ctl'),
    'M08B': ('internal/xferreceive/ internal/xferimage/', 'restore wrong offset or hash ahead', 'agent ctl'),
    'M08C': ('internal/xferreceive/', 'ConfirmSynced before successful Sync', 'agent ctl'),
    'M09': ('internal/transferpath/', 'reopen original OFD by path', 'agent ctl'),
    'M10A': ('internal/transferpath/', 'repeat successful commit primitive', 'agent ctl'),
    'M10B': ('internal/transferpath/', 'write partial destination in place then restore', 'agent ctl'),
    'M11A': ('internal/xferimage/ internal/xferbridge/', 'skip tracker import', 'broker'),
    'M11B': ('internal/xferledger/', 'late source seal overwrites terminal', 'broker'),
    'M11C': ('internal/xfertrack/ internal/xferimage/', 'reset original deadline on import', 'broker'),
    'M12A': ('internal/xferhistory/ internal/xfercourier/', 'any terminal is an exact ACK', 'broker agent ctl'),
    'M12B': ('internal/xferhistory/', 'remove history subject CAS', 'broker'),
    'M12C': ('internal/xfercourier/ internal/xfertrack/', 'replace failed outcome with complete', 'broker agent ctl'),
    'M13A': ('internal/xfergc/', 'bypass live owner GC protection', 'broker'),
    'M13B': ('internal/xfergc/ internal/xferfinal/', 'discard cleanup responsibility', 'broker'),
    'M13C': ('internal/xfergc/', 'stop reaper to fake live protection', 'broker'),
    'M14': ('internal/xferbucket/', 'full ObjectStoreConfig loses Duplicates or TTL', 'broker'),
}
COHORTS = ['small-push', 'small-pull', 'large-push', 'large-pull']
NATIVE = {'GREEN': 0, 'ASSERT-FAIL': 1, 'SETUP-RED': 2, 'PRODUCT-RED': 3, 'INCOMPLETE': 4}
VERDICT = re.compile(r'^DRILL-VERDICT verdict=(GREEN|ASSERT-FAIL|SETUP-RED|PRODUCT-RED|INCOMPLETE) rc=([0-4]) assert_fail=(\d+) setup_red=(\d+) product_red=(\d+) not_covered=(\d+) nc_gap=(\d+) nc_guard=(\d+) pass=(\d+) -- (.+)$', re.M)


class ContractError(ValueError):
    pass


class CoverageError(ContractError):
    pass


def require_window(ok, reason):
    if not ok:
        raise CoverageError(reason)


def require(ok, reason):
    if not ok:
        raise ContractError(reason)


def load(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'duplicate JSON key')
            result[key] = value
        return result
    return json.loads(Path(path).read_text(), object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(ContractError('nonfinite JSON')))


def sha(data):
    return hashlib.sha256(data).hexdigest()


def digest(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def token(value):
    return isinstance(value, str) and re.fullmatch('[A-Za-z0-9_-]+', value) is not None


def archive_file(root, name):
    require(isinstance(name, str) and name, 'missing archived file')
    relative = Path(name)
    require(not relative.is_absolute() and '..' not in relative.parts, 'archive escape')
    path = (root / relative).resolve()
    require(path.is_relative_to(root.resolve()) and path.is_file(), 'missing archived input')
    return path


def parse_native(text, rc):
    rows = VERDICT.findall(text)
    require(len(rows) == 1, 'missing or duplicate native verdict')
    verdict, code, *rest = rows[0]
    counts = list(map(int, rest[:7]))
    require(type(rc) is int and int(code) == rc == NATIVE[verdict], 'native rc was translated')
    fail, setup, product, missing, gap, guard, passed = counts
    require(missing == gap + guard, 'inconsistent gap counters')
    actual = 'ASSERT-FAIL' if fail else 'SETUP-RED' if setup else 'PRODUCT-RED' if product else 'INCOMPLETE' if missing else 'GREEN'
    require(verdict == actual and passed > 0, 'native counters lack verdict or exercise')
    return rc


# Same required set is consumed by the manifest checks and the live scheduler.
# A prepare record can never satisfy a sender/receiver data cut.
# origin: hot-update S7 plan §10.3 / T43.
def requirements(arm, topology):
    require(arm in POSITIVE or arm in MUTATIONS or arm == 'T', 'unknown arm')
    steps = []
    brokers = ['brk1'] if topology != 'quorum' else ['source-follower', 'original-leader', 'third-voter']
    for leg in (1, 2):
        for direction, limit in (('forward', 30), ('rollback', 190)):
            steps.append(dict(id=f'leg{leg}-agent-{direction}', leg=leg, action='agent-'+direction,
                              latest_s=limit, cohorts=list(COHORTS), roles={name: (
                                  'sender' if name.endswith('pull') else 'prepared') if leg == 1 else (
                                  'receiver' if name.endswith('push') else 'courier') for name in COHORTS}))
            steps.append(dict(id=f'leg{leg}-broker-{direction}', leg=leg, action='broker-'+direction,
                              latest_s=110 if direction == 'forward' else 190,
                              cohorts=list(COHORTS), hops=list(brokers), roles={name: 'tracker' for name in COHORTS}))
        if arm in ('C1', 'C2', 'C3'):
            steps.append(dict(id=f'leg{leg}-{arm}', leg=leg, action=arm, latest_s=190,
                              cohorts=list(COHORTS), roles={name: 'pending-data' for name in COHORTS}))
    if arm in ('W1', 'W2'):
        # One true watchdog outage on the selected leg. The other leg still
        # cuts normally; never insert two120s outages into one small call.
        leg = 1 if arm == 'W1' else 2
        steps = [s for s in steps if not (s['leg'] == leg and s['action'].startswith('agent-'))]
        point = next(i for i, s in enumerate(steps) if s['leg'] == leg)
        steps.insert(point, dict(id=arm, leg=leg, action=arm, latest_s=190,
                     cohorts=list(COHORTS), roles={name: ('sender' if name.endswith('pull') else 'prepared')
                     if leg == 1 else ('receiver' if name.endswith('push') else 'courier') for name in COHORTS}))
    if arm in ('R1', 'R2'):
        # Real import failures, independently supplied P32 artifacts. Neither
        # a preflight refusal nor explicit rollback signs partial ACTIVE.
        steps = []
        for layer in ('agent', 'broker-store', 'broker-control'):
            for tier in ('A', 'B'):
                for direction in ('push', 'pull'):
                    steps.append(dict(id=f'{arm}-{layer}-{tier}-{direction}', leg=1 if direction == 'pull' else 2,
                                      action=arm, layer=layer, tier=tier, direction=direction,
                                      latest_s=190, cohorts=list(COHORTS), roles={name: 'tracker' if layer != 'agent' else
                                      ('sender' if name.endswith('pull') else 'prepared') if direction == 'pull' else
                                      ('receiver' if name.endswith('push') else 'courier') for name in COHORTS}))
    # origin: S7 internal review rv-drill F5. Original tasks cannot return to leg1.
    steps.sort(key=lambda step: step['leg'])
    if arm == 'T':
        steps = [dict(id='defer-'+kind, leg=1, action='safe-defer', kind=kind, latest_s=190,
                      cohorts=list(COHORTS), roles={name: 'tracker' for name in COHORTS})
                 for kind in ('legacy', 'unknown')] + steps
    if arm == 'X':
        steps = [dict(id='cancel-victim', leg=1, action='cancel', latest_s=190, cohorts=list(COHORTS),
                      roles={name: 'tracker' for name in COHORTS}),
                 dict(id='session-rm-victim', leg=1, action='session-rm', latest_s=190, cohorts=list(COHORTS),
                      roles={name: 'tracker' for name in COHORTS})] + steps
    return steps


def check_manifest(plan):
    require(plan.get('schema') == 'transfer-driver-v1', 'wrong driver schema')
    arm = plan.get('arm')
    required = MUTATIONS.keys() if plan.get('controls') else POSITIVE + ['T']
    require(arm in required, 'control mixed into positive parent')
    topology = plan.get('topology')
    require(topology in ('single', 'sole-voter', 'quorum'), 'missing topology')
    require(plan.get('required_children') == (list(MUTATIONS) if plan['controls'] else POSITIVE), 'missing required child')
    require(plan.get('steps') == requirements(arm, topology), 'missing leg, hop or boundary')
    require(plan.get('control_rate') == 10 and plan.get('control_budget_s') == 5 and
            plan.get('transfer_progress_s') == 10 and plan.get('maintenance_ttl_s') == 8 and
            plan.get('watchdog_s') == 120 and plan.get('reaper') == 'default' and
            plan.get('quiet_window_s') == 2, 'frozen budgets or default reaper changed')
    cohorts = plan.get('cohorts')
    require(isinstance(cohorts, list) and [c.get('label') for c in cohorts] == COHORTS, 'four cohorts are required')
    watched = arm in ('W1', 'W2')
    for c in cohorts:
        small = c['label'].startswith('small')
        require(c.get('size') == (66549440 if small else 1073741824), 'cohort size changed')
        require(c.get('rate_limit_bps') == ((1048576 if small else 8388608) if watched else (262144 if small else 4194304)), 'rate profile changed after start')
        require(digest(c.get('sha256')), 'source SHA must precede business')
        require(c.get('verb') == ('push' if c['label'].endswith('push') else 'pull'), 'direction missing')
        for key in ('sid', 'bucket', 'nid', 'agent_node'):
            require(token(c.get(key)), 'invalid cohort identity '+key)
        for key in ('binary_path', 'home', 'source', 'destination'):
            require(isinstance(c.get(key), str) and c[key].startswith('/') and '\n' not in c[key] and '\t' not in c[key], 'invalid isolated path')
        require(type(c.get('force')) is bool, 'force/no-force declaration missing')
    for key in ('sid', 'bucket', 'nid'):
        require(len({c[key] for c in cohorts}) == 4, 'cohorts share '+key)
    for key in ('binary_path', 'home'):
        require(len({(c['agent_node'], c[key]) for c in cohorts}) == 4, 'shared agent installation '+key)
    for verb in ('push', 'pull'):
        require({c['force'] for c in cohorts if c['verb'] == verb} == {False, True}, 'force/no-force direction missing')
    require(plan.get('transport') == 'caddy-wss', 'real Caddy/WSS required')
    artifacts = plan.get('artifacts', {})
    for name in ('A', 'B'):
        artifact = artifacts.get(name, {})
        require(digest(artifact.get('sha256')) and artifact.get('transfer_version') == 'transfer-v1', 'missing capable prebuilt artifact')
        require(isinstance(artifact.get('binary'), str) and artifact['binary'].startswith('/') and
                isinstance(artifact.get('url'), str) and artifact['url'].startswith('https://') and
                isinstance(artifact.get('version'), str) and re.fullmatch('[A-Za-z0-9.+_-]+', artifact['version']), 'invalid artifact identity')
    require(artifacts['A']['sha256'] != artifacts['B']['sha256'], 'A and B artifacts have the same SHA')
    brokers = plan.get('brokers', [])
    require([b.get('node') for b in brokers] == (['brk1', 'brk2', 'brk3'] if topology == 'quorum' else ['brk1']), 'missing broker topology hop')
    require(all(isinstance(b.get('socket'), str) and b['socket'].startswith('/') for b in brokers), 'missing broker reload socket')
    if arm in ('R1', 'R2'):
        from transfer_fixture_manifest import validate_rollback_tasks
        validate_rollback_tasks(plan)
    if arm == 'T':
        refusals = plan.get('refusal_lanes', [])
        require([r.get('kind') for r in refusals] == ['legacy', 'unknown'], 'legacy/unknown refusal lanes missing')
        require(all(token(r.get('node')) and isinstance(r.get('socket'), str) and r['socket'].startswith('/')
                    and r.get('artifact_key') in artifacts and r.get('reason') for r in refusals), 'refusal lane identity missing')
    if plan['controls']:
        require(plan.get('mutation') == dict(id=arm, defect=MUTATIONS[arm][1], roles=MUTATIONS[arm][2].split(),
                minimum_wait_s=125 if arm in ('M02', 'M12B') else 0), 'missing production scenario')
    if arm == 'X':
        victims = plan.get('victims', [])
        require([v.get('kind') for v in victims] == ['cancel', 'session-rm'], 'independent victims missing')
        require(len({v.get('sid') for v in victims}) == 2 and len({v.get('bucket') for v in victims}) == 2, 'victims share responsibility')
        require(not {v.get('sid') for v in victims} & {c['sid'] for c in cohorts} and
                not {v.get('bucket') for v in victims} & {c['bucket'] for c in cohorts}, 'victim borrows survivor bucket')
    return plan


def make_manifest(deployment, arm, topology=None):
    plan = dict(deployment)
    topology = topology or ('quorum' if arm == 'B' else 'sole-voter' if arm == 'A1' else 'single')
    plan.update(schema='transfer-driver-v1', arm=arm, controls=arm in MUTATIONS, topology=topology,
                required_children=list(MUTATIONS) if arm in MUTATIONS else POSITIVE,
                steps=requirements(arm, topology), control_rate=10, control_budget_s=5,
                transfer_progress_s=10, maintenance_ttl_s=8, watchdog_s=120, reaper='default', quiet_window_s=2)
    if arm in MUTATIONS:
        plan['mutation'] = dict(id=arm, defect=MUTATIONS[arm][1], roles=MUTATIONS[arm][2].split(),
                                minimum_wait_s=125 if arm in ('M02', 'M12B') else 0)
    return check_manifest(plan)


# A sample is a ready-to-cut observation, not permission to claim coverage.
# The native fixture judge additionally proves original OFD/PID, every hop,
# postcut progress, syscall effects, history and its two fresh drain barriers.
# origin: hot-update S7 plan §10.3 / T43.
def check_sample(plan, sample, step_id):
    check_manifest(plan)
    step = next((s for s in plan['steps'] if s['id'] == step_id), None)
    require(step is not None, 'unknown scheduled cut')
    require(sample.get('manifest_sha') == sha(json.dumps(plan, sort_keys=True).encode()), 'sample from another manifest')
    require(type(sample.get('sequence')) is int and sample['sequence'] > 0, 'missing sample sequence')
    rows = sample.get('windows', {}).get(step_id)
    if rows is None:
        return 4
    require_window(isinstance(rows, list) and [r.get('label') for r in rows] == COHORTS, 'missing cohort window')
    for row in rows:
        require_window(row.get('leg') == step['leg'] and row.get('ready') is True, 'wrong leg or unready sample')
        require_window(type(row.get('elapsed_s')) in (int, float) and 0 <= row['elapsed_s'] <= step['latest_s'], 'early finish or late cut')
        role = step.get('roles', {}).get(row['label'])
        require_window(row.get('role') == role if role else True, 'prepared cannot impersonate sender or receiver')
        require(type(row.get('complete')) is bool, 'missing completion evidence')
        require_window(row.get('complete') is False, 'leg finished before cut')
        if role in ('sender', 'receiver', 'tracker', 'pending-data'):
            expected = next(c['size'] for c in plan['cohorts'] if c['label'] == row['label'])
            require(row.get('size') == expected and type(row.get('durable_bytes')) is int, 'missing or false durable size')
            require_window(0 < row['durable_bytes'] < row['size'], 'empty durable data window')
        else:
            require(type(row.get('pending_responsibilities')) is int, 'missing retained evidence')
            require_window(row['pending_responsibilities'] > 0, 'empty retained responsibility')
    if plan['arm'] in ('R1', 'R2'):
        task = sample.get('rollback_tasks', {}).get(step_id, {})
        require_window(task.get('ready') is True and task.get('tier') == step['tier'] and
                       task.get('direction') == step['direction'], 'independent rollback task missing')
    return 0


# Patch files are exact, SHA-bound owner handoffs, not external executable
# suppliers. The implementation locations are unknown until their owners land;
# refusing a missing patch is safer than replacing a similarly named stub.
# origin: hot-update S7 plan §10.5 / T44.
def go_tokens(source):
    # Preserve literals before deleting comments: a string containing // must
    # still be a behavior change. This is also a nonempty gate against comments.
    pattern = r'"(?:\\.|[^"\\])*"|`[^`]*`|\'(?:\\.|[^\'\\])*\'|//[^\n]*|/\*.*?\*/|[A-Za-z_][A-Za-z_0-9]*|\d+|[^\s]'
    return [t for t in re.findall(pattern, source, re.S) if not t.startswith(('//', '/*'))]


def apply_overlay(repo, output, mutation, patch, inherited=''):
    require(mutation in MUTATIONS, 'unknown mutation')
    roots, defect, roles = MUTATIONS[mutation]
    require(patch.get('schema') == 'transfer-production-patch-v1' and patch.get('mutation_id') == mutation and
            patch.get('defect') == defect and patch.get('roles') == roles.split(), 'wrong production patch identity')
    changes = patch.get('changes')
    require(isinstance(changes, list) and changes, 'missing production predicate patch')
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    mapping, sources = {}, []
    flags = shlex.split(inherited)
    kept = []
    for flag in flags:
        require(flag != '-overlay', 'GOFLAGS overlay must use =')
        if flag.startswith('-overlay='):
            old = load(flag.split('=', 1)[1]).get('Replace')
            require(isinstance(old, dict), 'invalid inherited overlay')
            for name, replacement in old.items():
                require(Path(name).is_absolute() and replacement and Path(replacement).is_absolute(), 'unsafe inherited overlay')
                require(name not in mapping, 'duplicate inherited overlay')
                mapping[name] = replacement
        else:
            kept.append(flag)
    for index, change in enumerate(changes):
        relative = change.get('path', '')
        require(any(relative.startswith(root) if root.endswith('/') else relative == root for root in roots.split()), 'mutation outside production owner')
        require(relative.endswith('.go') and not relative.endswith(('_test.go', 'contracts.go')) and '/contractstub/' not in relative, 'test or compile stub is not production mutation')
        original = (repo / relative).resolve()
        require(original.is_relative_to(repo.resolve()) and str(original) not in mapping, 'overlay collision or source escape')
        source = original.read_text()
        require('not implemented (S7' not in source and 'contractstub.' not in source, 'owner implementation is still a P00 stub')
        require(change.get('source_sha256') == sha(original.read_bytes()), 'production source drift')
        before, after = change.get('before'), change.get('after')
        require(isinstance(before, str) and before and isinstance(after, str) and source.count(before) == 1, 'missing or ambiguous predicate')
        require(go_tokens(before) != go_tokens(after), 'comment-only or no-op mutation')
        function = change.get('function')
        require(isinstance(function, str) and re.fullmatch('[A-Za-z_][A-Za-z_0-9]*', function), 'missing predicate function')
        # Exact source location must be inside a real function, not an import,
        # declaration, build constraint or log string outside the operation.
        prior = source[:source.index(before)]
        declarations = re.findall(r'^func (?:\([^\n]*\) )?([A-Za-z_][A-Za-z_0-9]*)\s*\(', prior, re.M)
        require(declarations and declarations[-1] == function, 'patch misses declared operation')
        mutated = source.replace(before, after, 1)
        replacement = output / f'production-{index}.go'
        replacement.write_text(mutated)
        mapping[str(original)] = str(replacement.resolve())
        (output / f'original-{index}.go').write_text(source)
        sources.append(dict(path=relative, function=function, source_file=f'original-{index}.go',
                            replacement_file=f'production-{index}.go', source_sha256=sha(source.encode()),
                            mutation_sha256=sha(mutated.encode()), before=before, after=after))
    selector = output / 'overlay.json'
    selector.write_text(json.dumps({'Replace': mapping}, sort_keys=True)+'\n')
    require(not re.search(r'\s', str(selector.resolve())), 'overlay path contains GOFLAGS whitespace')
    kept.append('-overlay='+str(selector.resolve()))
    flags_value = shlex.join(kept)
    receipt = dict(schema='transfer-mutation-build-v1', mutation_id=mutation, defect=defect, roles=roles.split(),
                   sources=sources, overlay_sha256=sha(selector.read_bytes()), build_selector=flags_value)
    (output / 'source.json').write_text(json.dumps(receipt, sort_keys=True)+'\n')
    (output / 'goflags.txt').write_text(flags_value+'\n')
    return receipt


# Only the original native judge can kill a mutant. The source/build/runtime
# chain must be complete before an ASSERT-FAIL is recorded as a control receipt.
# origin: hot-update S7 plan §10.5 / T44.
def check_default_window(mutation, witness):
    if mutation in ('M02', 'M12B'):
        require(witness.get('default_dedup_ns') == 120000000000 and
                type(witness.get('elapsed_s')) in (int, float) and witness['elapsed_s'] >= 125,
                'default120s late publish window was shortened')


def check_kill(root, mutation, native_rc):
    require(mutation in MUTATIONS, 'unknown control mutation')
    build = load(root / 'mutation/source.json')
    roots, defect, roles = MUTATIONS[mutation]
    require(build.get('mutation_id') == mutation and build.get('defect') == defect and
            build.get('roles') == roles.split() and build.get('sources'), 'missing production patch receipt')
    selector = root / 'mutation/overlay.json'
    require(sha(selector.read_bytes()) == build.get('overlay_sha256'), 'overlay digest changed')
    replacements = load(selector).get('Replace', {})
    for source in build['sources']:
        relative = source.get('path', '')
        require(any(relative.startswith(owner) if owner.endswith('/') else relative == owner for owner in roots.split()), 'wrong mutation owner in receipt')
        before = archive_file(root / 'mutation', source.get('source_file'))
        after = archive_file(root / 'mutation', source.get('replacement_file'))
        require(sha(before.read_bytes()) == source.get('source_sha256') and
                sha(after.read_bytes()) == source.get('mutation_sha256'), 'archived production patch changed')
        require(before.read_text().count(source['before']) == 1 and
                before.read_text().replace(source['before'], source['after'], 1) == after.read_text(), 'source patch does not reconstruct mutant')
        require(str(after.resolve()) in replacements.values(), 'mutated source not selected by overlay')
    flags = (root / 'mutation/goflags.txt').read_text().strip()
    require(flags == build.get('build_selector') and '-overlay='+str(selector.resolve()) in shlex.split(flags), 'nested build lost GOFLAGS overlay')
    binary = root / 'mutation/tether'
    artifact_sha = sha(binary.read_bytes())
    require(load(root / 'mutation/build.json').get('artifact_sha') == artifact_sha, 'build artifact mismatch')
    runtime = load(root / 'mutation-runtime.json')
    require(runtime.get('mutation_id') == mutation and runtime.get('build_selector') == flags, 'runtime selected another build')
    processes = runtime.get('processes', [])
    require({p.get('role') for p in processes} == set(build['roles']), 'mutant did not run in every required role')
    for process in processes:
        running_sha = process.get('artifact_sha')
        require(digest(running_sha) and running_sha in {load(root / 'mutation/build.json').get('original_artifact_sha'), sha(binary.read_bytes())} and process.get('running_sha') == running_sha and
                type(process.get('pid')) is int and process['pid'] > 0 and type(process.get('process_start')) is int and
                process['process_start'] > 0 and process.get('boot_id'), 'actual running SHA or process identity missing')
    witness = load(root / 'mutation-witness.json')
    require(witness.get('mutation_id') == mutation and witness.get('exercised') is True and witness.get('criterion') and
            digest(witness.get('reader_digest')), 'mutation not exercised or reader unbound')
    for key in ('raw_path', 'oracle_input_path'):
        raw = archive_file(root, witness.get(key))
        require(sha(raw.read_bytes()) == witness.get(key.replace('_path', '_sha')), 'mutation witness input digest mismatch')
    require(witness.get('native_rc') == native_rc == 1, 'compile, setup or incomplete red is not a kill')
    check_default_window(mutation, witness)
    log = (root / 'judge.out').read_text()
    parse_native(log, native_rc)
    return dict(mutation_id=mutation, expected_native_rc=1, actual_native_rc=native_rc,
                killed=True, artifact_sha=artifact_sha, running_sha=artifact_sha,
                oracle_input_sha=witness['oracle_input_sha'], reader_digest=witness.get('reader_digest'))


def capable_reader(text):
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    require(lines and re.fullmatch(r'tether [A-Za-z0-9.+_-]+ \(proto v[0-9]+\)', lines[0]), 'invalid actual reader version')
    require(lines[-1] == 'image-abi v1' and [line for line in lines if line.startswith('image-abi')] == ['image-abi v1'], 'mutation reader ABI unsupported')
    domains = [line.split()[1:] for line in lines if line.startswith('image-domains ')]
    require(len(domains) == 1 and sum(line.startswith('image-domains') for line in lines) == 1 and
            'transfer-v1' in domains[0] and len(domains[0]) == len(set(domains[0])), 'mutation reader lacks transfer-v1')
    return lines[0].split()[1]


# Binding precedes start; it changes neither rates nor original deadlines. P29
# stages these real binaries at predeclared HTTPS URLs before original business.
def bind_artifacts(manifest, root):
    plan = check_manifest(load(manifest))
    require(plan['controls'], 'mutant artifacts in positive parent')
    urls = plan.get('mutation_artifact_urls', {})
    hashes = {}
    for name, filename in (('A', 'tether-original'), ('B', 'tether')):
        binary = root/'mutation'/filename
        require(isinstance(urls.get(name), str) and urls[name].startswith('https://'), 'missing predeclared mutant artifact URL')
        hashes[name] = sha(binary.read_bytes())
        version = capable_reader((root/'mutation'/('reader-'+name+'.out')).read_text())
        plan['artifacts'][name] = dict(plan['artifacts'][name], binary=str(binary.resolve()),
                                      url=urls[name], sha256=hashes[name], version=version)
    require(hashes['A'] != hashes['B'], 'mutant A/B builds have the same SHA')
    check_manifest(plan)
    manifest.write_text(json.dumps(plan, sort_keys=True)+'\n')
    build = load(root/'mutation/build.json')
    require(build.get('artifact_sha') == hashes['B'], 'candidate mutation build drift')
    build['original_artifact_sha'] = hashes['A']
    (root/'mutation/build.json').write_text(json.dumps(build, sort_keys=True)+'\n')


def selftest():
    # Pure parser controls; these intentionally make no product-run claim.
    import copy
    import tempfile
    controls = 0
    deployment = dict(transport='caddy-wss', brokers=[dict(node='brk1', socket='/isolated/admin.sock')],
                      artifacts={name: dict(binary='/isolated/'+name, url='https://artifact.invalid/'+name, sha256=char*64,
                      version='v1.0.0', transfer_version='transfer-v1') for name, char in [('A', 'a'), ('B', 'b')]}, cohorts=[dict(label=name, size=66549440 if name.startswith('small') else 1073741824,
                      rate_limit_bps=262144 if name.startswith('small') else 4194304, sha256='a'*64,
                      verb='push' if name.endswith('push') else 'pull', sid='sid'+str(i), bucket='bucket'+str(i), nid='nid'+str(i),
                      agent_node='agt'+str(i), binary_path='/isolated/bin/tether', home='/isolated/home',
                      source='/isolated/source', destination='/isolated/destination', force=name.startswith('large'))
                      for i, name in enumerate(COHORTS)])
    victim_deployment = dict(deployment, victims=[dict(kind=kind, sid='victim'+str(i), bucket='victim-bucket'+str(i))
                               for i, kind in enumerate(('cancel', 'session-rm'))])
    victim_plan = make_manifest(victim_deployment, 'X')
    require([s['action'] for s in victim_plan['steps'][:2]] == ['cancel', 'session-rm'], 'victims must precede data ladder')
    plan = make_manifest(deployment, 'A')
    sample = dict(manifest_sha=sha(json.dumps(plan, sort_keys=True).encode()), sequence=1, windows={})
    step = plan['steps'][0]
    sample['windows'][step['id']] = [dict(label=name, leg=1, ready=True, elapsed_s=1, role=step['roles'][name],
                                         complete=False, durable_bytes=1, size=66549440 if name.startswith('small') else 1073741824, pending_responsibilities=1) for name in COHORTS]
    require(check_sample(plan, sample, step['id']) == 0, 'valid sample refused')
    for field, value in [('complete', True), ('role', 'prepared'), ('durable_bytes', 0), ('elapsed_s', 191)]:
        broken = copy.deepcopy(sample)
        broken['windows'][step['id']][1][field] = value
        try:
            check_sample(plan, broken, step['id'])
        except ContractError:
            controls += 1
        else:
            raise ContractError('sample defect survived '+field)
    for edit in ('child', 'leg', 'control'):
        broken = copy.deepcopy(plan)
        if edit == 'child': broken['required_children'].pop()
        if edit == 'leg': broken['steps'].pop()
        if edit == 'control': broken['required_children'].append('M01')
        try:
            check_manifest(broken)
        except ContractError:
            controls += 1
        else:
            raise ContractError('manifest defect survived '+edit)
    with tempfile.TemporaryDirectory(prefix='transfer-overlay-control-') as directory:
        repo = Path(directory)
        original = repo/'internal/xferclient/engine.go'
        original.parent.mkdir(parents=True)
        original.write_text('package xferclient\nfunc Run() error {\n\tif pending { return reconcile() }\n\treturn nil\n}\n')
        source = original.read_bytes()
        patch = dict(schema='transfer-production-patch-v1', mutation_id='M01', defect=MUTATIONS['M01'][1], roles=['ctl'],
                     changes=[dict(path='internal/xferclient/engine.go', source_sha256=sha(source), function='Run',
                                   before='if pending { return reconcile() }', after='if pending { return put() }')])
        inherited = repo/'prior.json'
        inherited.write_text(json.dumps({'Replace': {str(repo/'prior.go'): str(repo/'prior-mutant.go')}}))
        out = repo/'mutation'
        apply_overlay(repo, out, 'M01', patch, '-tags=custom -overlay='+str(inherited))
        require(original.read_bytes() == source, 'overlay modified original')
        require(str(repo/'prior.go') in load(out/'overlay.json')['Replace'], 'inherited overlay lost')
        require('-tags=custom' in (out/'goflags.txt').read_text(), 'nested build flags lost')
        for field, value in [('source_sha256', 'b'*64), ('function', 'Unrelated'),
                             ('after', 'if pending { return reconcile() } // comment')]:
            broken = copy.deepcopy(patch)
            broken['changes'][0][field] = value
            try:
                apply_overlay(repo, repo/field, 'M01', broken)
            except ContractError:
                controls += 1
            else:
                raise ContractError('overlay defect survived '+field)
        # Synthetic bytes exercise only this archive reader; they are never
        # installed or presented as a real build/run/mutation receipt.
        ev = repo/'evidence'
        ev.mkdir()
        out.rename(ev/'mutation')
        out = ev/'mutation'
        # Relocation is forbidden in real receipts. Rebind this temporary
        # synthetic archive after moving it so paths match its current custody.
        selector = load(out/'overlay.json')
        selector['Replace'][str(original.resolve())] = str((out/'production-0.go').resolve())
        (out/'overlay.json').write_text(json.dumps(selector, sort_keys=True)+'\n')
        flags = '-tags=custom -overlay='+str((out/'overlay.json').resolve())
        (out/'goflags.txt').write_text(flags+'\n')
        build = load(out/'source.json')
        build.update(overlay_sha256=sha((out/'overlay.json').read_bytes()), build_selector=flags)
        (out/'source.json').write_text(json.dumps(build)+'\n')
        (out/'tether').write_bytes(b'synthetic parser bytes; never executed')
        artifact = sha((out/'tether').read_bytes())
        (out/'build.json').write_text(json.dumps(dict(artifact_sha=artifact)))
        runtime = dict(mutation_id='M01', build_selector=flags, processes=[dict(role='ctl',
                       pid=123, process_start=456, boot_id='synthetic-boot', running_sha=artifact, artifact_sha=artifact)])
        (ev/'mutation-runtime.json').write_text(json.dumps(runtime))
        (ev/'raw.jsonl').write_text('synthetic predicate witness\n')
        witness = dict(mutation_id='M01', exercised=True, criterion='synthetic-parser-control', native_rc=1,
                       raw_path='raw.jsonl', raw_sha=sha((ev/'raw.jsonl').read_bytes()),
                       oracle_input_path='raw.jsonl', oracle_input_sha=sha((ev/'raw.jsonl').read_bytes()), reader_digest='c'*64)
        (ev/'mutation-witness.json').write_text(json.dumps(witness))
        native = 'DRILL-VERDICT verdict=ASSERT-FAIL rc=1 assert_fail=1 setup_red=0 product_red=0 not_covered=0 nc_gap=0 nc_guard=0 pass=1 -- synthetic parser control\n'
        (ev/'judge.out').write_text(native)
        require(check_kill(ev, 'M01', 1)['killed'], 'complete synthetic archive refused')
        for field, value in [('running_sha', 'd'*64), ('pid', 0), ('process_start', 0), ('boot_id', '')]:
            broken = copy.deepcopy(runtime)
            broken['processes'][0][field] = value
            (ev/'mutation-runtime.json').write_text(json.dumps(broken))
            try:
                check_kill(ev, 'M01', 1)
            except ContractError:
                controls += 1
            else:
                raise ContractError('runtime identity defect survived '+field)
        (ev/'mutation-runtime.json').write_text(json.dumps(runtime))
        for field, value in [('exercised', False), ('native_rc', 2), ('raw_sha', 'd'*64), ('reader_digest', ''),
                             ('oracle_input_path', '../raw.jsonl')]:
            broken = dict(witness)
            broken[field] = value
            (ev/'mutation-witness.json').write_text(json.dumps(broken))
            try:
                check_kill(ev, 'M01', 1)
            except ContractError:
                controls += 1
            else:
                raise ContractError('witness defect survived '+field)
        (ev/'mutation-witness.json').write_text(json.dumps(witness))
        for rc in (0, 2, 3, 4, 75):
            try:
                check_kill(ev, 'M01', rc)
            except ContractError:
                controls += 1
            else:
                raise ContractError('non-assert failure signed a kill')
        for target in (out/'goflags.txt', out/'production-0.go', ev/'judge.out'):
            prior = target.read_bytes()
            target.write_bytes(b'tampered')
            try:
                check_kill(ev, 'M01', 1)
            except ContractError:
                controls += 1
            else:
                raise ContractError('archive tampering survived')
            finally:
                target.write_bytes(prior)
        plan = make_manifest(deployment, 'M01')
        plan['mutation_artifact_urls'] = {name: 'https://artifact.invalid/mutant-'+name for name in ('A', 'B')}
        path = ev/'control-manifest.json'
        path.write_text(json.dumps(plan))
        (out/'tether-original').write_bytes(b'synthetic original parser artifact')
        version = 'tether dev (proto v2)\nimage-domains exec-v1 transfer-v1\nimage-abi v1\n'
        for name in ('A', 'B'):
            (out/('reader-'+name+'.out')).write_text(version)
        bind_artifacts(path, ev)
        bound = load(path)
        require(bound['cohorts'] == plan['cohorts'] and bound['steps'] == plan['steps'], 'artifact binding changed live profile')
        for broken_version in (version.replace('transfer-v1', 'legacy'), version.replace('image-abi v1', 'image-abi v2'),
                               version.replace('transfer-v1', 'transfer-v1 transfer-v1'),
                               version.replace('image-domains', 'image-abi v2\nimage-domains'),
                               version.replace('image-domains', 'image-domains-bad ignored\nimage-domains')):
            try:
                capable_reader(broken_version)
            except ContractError:
                controls += 1
            else:
                raise ContractError('unsupported reader signed control')
        for label in ('missing-url', 'same-sha'):
            broken = copy.deepcopy(plan)
            if label == 'missing-url': del broken['mutation_artifact_urls']['A']
            else: (out/'tether-original').write_bytes((out/'tether').read_bytes())
            path.write_text(json.dumps(broken))
            try:
                bind_artifacts(path, ev)
            except ContractError:
                controls += 1
            else:
                raise ContractError('invalid mutant artifact binding survived')
    for mutation in ('M02', 'M12B'):
        check_default_window(mutation, dict(default_dedup_ns=120000000000, elapsed_s=125))
        for duration, elapsed in ((20000000000, 125), (120000000000, 124)):
            try:
                check_default_window(mutation, dict(default_dedup_ns=duration, elapsed_s=elapsed))
            except ContractError:
                controls += 1
            else:
                raise ContractError('default dedup timing defect survived')
    print('transfer driver pure controls: PASS ('+str(controls)+' rejected defects; no deploy receipt)')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='op', required=True)
    sub.add_parser('selftest')
    sub.add_parser('catalog')
    p = sub.add_parser('manifest')
    p.add_argument('--deployment', required=True); p.add_argument('--arm', required=True)
    p.add_argument('--output', required=True); p.add_argument('--topology')
    for op in ('schedule', 'validate', 'sample'):
        p = sub.add_parser(op); p.add_argument('--manifest', required=True)
        if op == 'sample':
            p.add_argument('--sample', required=True); p.add_argument('--step', required=True)
    p = sub.add_parser('overlay')
    for name in ('repo', 'output', 'mutation', 'patch'):
        p.add_argument('--'+name, required=True)
    p = sub.add_parser('bind')
    p.add_argument('--manifest', required=True); p.add_argument('--evidence', required=True)
    p = sub.add_parser('kill')
    p.add_argument('--evidence', required=True); p.add_argument('--mutation', required=True); p.add_argument('--native-rc', type=int, required=True)
    args = parser.parse_args()
    if args.op == 'selftest': selftest()
    elif args.op == 'catalog':
        print(json.dumps({key: dict(roots=roots.split(), defect=defect, roles=roles.split()) for key, (roots, defect, roles) in MUTATIONS.items()}, sort_keys=True))
    elif args.op == 'manifest':
        value = make_manifest(load(args.deployment), args.arm, args.topology)
        Path(args.output).write_text(json.dumps(value, sort_keys=True)+'\n')
    elif args.op in ('schedule', 'validate', 'sample'):
        plan = check_manifest(load(args.manifest))
        if args.op == 'schedule':
            for s in plan['steps']: print(s['id']+'\t'+str(s['leg'])+'\t'+s['action'])
        elif args.op == 'sample':
            try:
                return check_sample(plan, load(args.sample), args.step)
            except CoverageError:
                # Valid custody but missing/late responsibility is coverage4;
                # unreadable/corrupt sample stays setup2 in the outer handler.
                return 4
    elif args.op == 'overlay':
        apply_overlay(Path(args.repo), Path(args.output), args.mutation, load(args.patch), os.environ.get('GOFLAGS', ''))
    elif args.op == 'bind':
        bind_artifacts(Path(args.manifest), Path(args.evidence))
    elif args.op == 'kill':
        receipt = check_kill(Path(args.evidence), args.mutation, args.native_rc)
        (Path(args.evidence)/'control-receipt.json').write_text(json.dumps(receipt, sort_keys=True)+'\n')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ContractError, OSError, ValueError, KeyError, TypeError) as error:
        # Error text contains only criterion names, never private transfer state.
        print('transfer driver SETUP-RED: '+str(error), file=sys.stderr)
        sys.exit(2)
