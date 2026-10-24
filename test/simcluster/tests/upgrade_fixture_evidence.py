"""Synthetic oracle inputs only; no product cut is certified by these rows."""
import copy
import ast
import ctypes
import csv
import difflib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import socket
import sys
import tempfile
import tarfile
import time
import unittest

LIB = Path(os.environ['S8_FIXTURE_LIB'])
def load(name):
    spec = importlib.util.spec_from_file_location(name, LIB / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
C = load('upgrade_tx_collect')
F = load('upgrade_tx_fixture')
I = load('upgrade_tx_faults')
S = C.supervisor
# The outer test owner is a last-resort mutation cleanup owner. Its custody
# cannot sign the inner supervisor's oracle, which runs before this fallback.
assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
SHA, OTHER, ID, CUT, NONCE = 'a' * 64, 'b' * 64, '1' * 32, '2' * 32, '3' * 32
OWNER = dict(boot_id='oracle-boot', pid=100, start_ticks=1000, instance_id='oracle-owner')


def synthetic_observation():
    # Oracle input only: this function never supplies a runtime/fixture port.
    resources = []
    domain_by_family = dict(ssh='flow', tcp='flow', pty='pty', exec='exec', transfer='transfer_worker',
                            ss='ss', remote_fs='exec', control='control')
    for family, count in C.PROFILE.items():
        for ordinal in range(count):
            resource_id = family + '.' + str(ordinal)
            ofd = dict(known=True, method='kernel_cookie', identity='kernel.' + resource_id,
                       original_identity='kernel.' + resource_id, same=True, observed_ns=1_000_000_000_000)
            resources.append(dict(id=resource_id, domain=domain_by_family[family], family=family,
                origin_owner=copy.deepcopy(OWNER), current_owner=copy.deepcopy(OWNER), generation=2,
                known=True, ofd=ofd, sequence=10, content_hash=SHA, fin=False, terminal='pending',
                result='pending', gc_permit='retained', reconnects=0, admission_sequence=1,
                state='active', custody='current', files=[], mount_identity='not_applicable',
                transfer=dict(applicable=False, known=False, tier='none', direction='none', business_id='not_applicable',
                    object_nuid_hash=SHA, size_bytes=0, rate_bytes_s=0, original_start_ns=0, budget_deadline_ns=0, durable_bytes=0)))
            row = resources[-1]
            if family in {'transfer', 'remote_fs'}:
                row['files'] = [dict(role=role, proof={**ofd, 'identity':resource_id+'.'+role,
                    'original_identity':resource_id+'.'+role}) for role in ('source', 'tmp', 'parent')]
            if family == 'remote_fs': row['mount_identity'] = 'kernel.mount.original'
            if family == 'transfer':
                row['transfer'].update(applicable=True, known=True, tier='B', direction='push' if ordinal % 2 else 'pull',
                    business_id=resource_id, object_nuid_hash=SHA, size_bytes=737280, rate_bytes_s=1024,
                    original_start_ns=100_000_000_000, budget_deadline_ns=2_000_000_000_000, durable_bytes=1024)
    for domain, family in [('transfer_tracker', 'tracker'), ('broker_store', 'store'), ('broker_runtime', 'runtime'),
                           ('authority', 'authority'), ('upgrade', 'upgrade')]:
        row = copy.deepcopy(resources[0])
        row.update(id=domain + '.0', domain=domain, family=family)
        row['ofd']['method'] = 'not_applicable'
        resources.append(row)
    handle = dict(key=dict(domain_id='oracle-domain', component='broker'), tx_id=ID, cut_id=CUT, generation=2, revision=1)
    stamp = dict(attempt=handle, producer=copy.deepcopy(OWNER), nonce=NONCE, sequence=10,
                 at=1_700_000_000_000_000_050, scope_digest=SHA)
    row = dict(version=1, source_commit='c' * 40, artifact_sha=OTHER, run_id=ID, arm='A-single', role='single',
        topology='single', original_set_digest=SHA, owner_boot_id=OWNER['boot_id'], pid=OWNER['pid'],
        start_ticks=OWNER['start_ticks'], tx_id=ID, cut_id=CUT, generation=2, direction='forward',
        source_sha=SHA, target_sha=OTHER, running_sha=OTHER, effective_sha=OTHER, scope_digest=SHA,
        sequence=10, domains=sorted(C.DOMAINS), resources=resources, hit_begin_ns=0, hit_end_ns=0,
        syscall='none', errno=0, native_rc=0, nonce=NONCE, known=True, complete=True,
        observed_begin_ns=1_000_000_000_000, observed_end_ns=1_000_000_000_100, clock='host_boottime',
        observed_begin_unix_ns=1_700_000_000_000_000_000, observed_end_unix_ns=1_700_000_000_000_000_100,
        phase='completed', outcome='success', admissions=[], admission_complete=True)
    for field, kind in [('control_receipt', 'control_ready'), ('resources_receipt', 'resources_migrated'),
                        ('released_receipt', 'old_generation_released')]:
        row[field] = dict(kind=kind, stamp=copy.deepcopy(stamp), known=True, complete=True, running_sha=OTHER,
            census=dict(stamp=copy.deepcopy(stamp), known=True, complete=True, resources=contract_rows(row),
                        expected_digest=SHA, admitting=0, old_owners=[]), evidence_digest=C.digest(kind))
    row['hit'] = dict(known=False, begin_ns=0, end_ns=0, syscall='none', errno=0, owner=copy.deepcopy(OWNER),
        cut_id=CUT, resource_id=resources[0]['id'], mechanism='none', kernel_evidence_digest=SHA,
        window_begin_ns=0, window_end_ns=0)
    row['safety_receipt'] = dict(known=False, complete=False, nonce=NONCE, tx_id=ID, cut_id=CUT, generation=2,
        owner=copy.deepcopy(OWNER), scope_digest=SHA, original_owner_dead=False, current_owner_known=True,
        reverse_execs=0, begin_ns=row['observed_begin_ns'], end_ns=row['observed_end_ns'], evidence_digest=SHA)
    row['metrics'] = synthetic_metrics(row)
    return row


def contract_rows(row):
    return [dict(domain=x['domain'], id=x['id'], origin='original', owner=copy.deepcopy(x['current_owner']),
                 generation=x['generation'], state=x['state'], custody=x['custody']) for x in row['resources']]


def synthetic_metrics(row):
    start, end = 900_000_000_000, 930_000_000_000
    freezes = [dict(cut_id=str(i + 4) * 32, tx_id=ID, generation=i+2, owner=copy.deepcopy(OWNER),
        source_sha=SHA if i % 2 == 0 else OTHER, target_sha=OTHER if i % 2 == 0 else SHA,
        earliest_fence_ns=start + (i + 1) * 5_000_000_000,
        last_pump_ready_ns=start + (i + 1) * 5_000_000_000 + 400_000_000, domains=sorted(C.DOMAINS)) for i in range(3)]
    metrics = dict(schedule_start_ns=start, schedule_end_ns=end, expected_slots=300, node_replay_bytes=32 * 1024 * 1024,
                   freezes=freezes, requests=[], calibrations=[], flow_progress=[], transfer_progress=[])
    for slot in range(300):
        kind = ['read', 'write', 'shortcmd'][slot % 3]
        at = start + slot * 100_000_000
        metrics['requests'].append(dict(slot=slot, kind=kind, call_id='oracle.' + str(slot), scheduled_ns=at,
            run_e_ns=at + 10_000, result_ns=at + 1_000_000, native_rc=0, durable_effects=int(kind != 'read'), attempts=1))
    for round in (1, 2):
        for load in ('idle', 'full'):
            metrics['calibrations'].append(dict(round=round, start_ns=1, end_ns=300_000_000_001, load=load,
                cpu_millis=10, rss_bytes=1000, fd_count=10, disk_bytes=1000, toolchain='oracle-only', network='oracle-only'))
    for item in row['resources']:
        field = 'transfer_progress' if item['family'] == 'transfer' else 'flow_progress'
        if item['family'] not in {'ssh', 'tcp', 'ss', 'transfer'}:
            continue
        for ordinal in range(31):
            metrics[field].append(dict(resource_id=item['id'], at_ns=start + ordinal * 1_000_000_000,
                                      sequence=ordinal + 1, durable_bytes=(ordinal + 1) * 1024))
    return metrics


def synthetic_manifest(row):
    command = dict(argv=[sys.executable, '-c', 'raise SystemExit(2)'], cwd=str(Path.cwd()), timeout_s=5)
    # Commands intentionally refuse execution: no fake S7 implementation can
    # sign full profile. They exist only to exercise manifest shape and judge.
    actors = [dict(id=x['id'], family=x['family'], resource_ids=[x['id']], command=command)
              for x in row['resources'] if x['family'] in C.PROFILE]
    return {key: row[key] for key in ('source_commit', 'artifact_sha', 'run_id', 'arm', 'role', 'topology')} | dict(
        version=1, case_id='normal', actors=actors, ports={name: command for name in ('observe', 'admissions', 'drain')},
        domains=sorted(C.DOMAINS), expect=dict(phase='completed', outcome='success', native_rc=0,
        completion=True, hit_required=False, a6=True), faults=[], deadline_s=600,
        transfer_budget=dict(rate_bytes_s=1024, cut_budget_s=600, drain_budget_s=30, margin_s=60, size_bytes=737280))


def sealed_pair():
    row = synthetic_observation()
    manifest = synthetic_manifest(row)
    originals = C.seal(row, C.digest(manifest))
    row['original_set_digest'] = originals['original_set_digest']
    for field in ('control_receipt', 'resources_receipt', 'released_receipt'):
        row[field]['census']['expected_digest'] = originals['original_set_digest']
    return row, originals, manifest


def drains(originals):
    terminal_ids = [item['id'] for item in originals['resources'] if item['family'] in {'exec', 'pty', 'transfer', 'remote_fs'}]
    history_ids = [item['id'] for item in originals['resources'] if item['family'] == 'transfer']
    first = dict(version=1, nonce='8' * 32, run_id=ID, original_set_digest=originals['original_set_digest'],
        at_ns=1_001_000_000_000, cursor=20, known=True, complete=True, joined=True, writers_retired=True,
        pending_gc=[], late_results=[], terminals=[dict(id=x, start_count=1, terminal_count=1, result_digest=SHA) for x in terminal_ids],
        history=[dict(id=x, physical_records=2, canonical_start=1, canonical_terminal=1, content_digest=SHA) for x in history_ids])
    second = copy.deepcopy(first)
    second.update(nonce='9' * 32, at_ns=first['at_ns'] + 2_000_000_000)
    return [first, second]


def evidence(root, row, originals, manifest):
    root = Path(root)
    (root / 'samples').mkdir()
    (root / 'admissions').mkdir()
    C.publish(root / 'originals.json', originals)
    C.publish(root / 'manifest.json', manifest)
    C.publish(root / 'start-observation.json', row)
    if manifest['expect']['hit_required']:
        (root/'faults').mkdir()
        case = dict(armed_ns=100, released_ns=130, mechanism='namespace_readonly', syscall='write', errno=30,
                    target=dict(owner=copy.deepcopy(OWNER), cut_id=CUT, resource_id=row['resources'][0]['id']))
        C.publish(root/'faults'/(manifest['case_id']+'.json'),case)
    for ordinal, freeze in enumerate(row['metrics']['freezes']):
        observed = copy.deepcopy(row)
        observed.update(nonce=str(ordinal+10)*16, cut_id=freeze['cut_id'], generation=freeze['generation'],
            source_sha=freeze['source_sha'], target_sha=freeze['target_sha'], running_sha=freeze['target_sha'],
            effective_sha=freeze['target_sha'], sequence=ordinal+10,
            observed_begin_unix_ns=row['observed_begin_unix_ns']+ordinal*1000, observed_end_unix_ns=row['observed_end_unix_ns']+ordinal*1000,
            observed_begin_ns=row['observed_begin_ns']+ordinal*1000, observed_end_ns=row['observed_end_ns']+ordinal*1000)
        for item in observed['resources']: item['generation'] = observed['generation']
        observed['safety_receipt'].update(nonce=observed['nonce'], cut_id=observed['cut_id'], generation=observed['generation'],
            begin_ns=observed['observed_begin_ns'], end_ns=observed['observed_end_ns'])
        if manifest['expect']['hit_required']:
            observed['hit'].update(known=True, begin_ns=110, end_ns=120, syscall='write', errno=30,
                                   mechanism='namespace_readonly', window_begin_ns=100, window_end_ns=130)
            observed.update(hit_begin_ns=110, hit_end_ns=120, syscall='write', errno=30)
        if 'archive_receipt' in observed:
            observed['archive_receipt'].update(nonce=observed['nonce'], cut_id=observed['cut_id'], generation=observed['generation'],
                                               at_ns=observed['observed_begin_ns'])
        for field in ('control_receipt', 'resources_receipt', 'released_receipt'):
            receipt = observed[field]
            receipt['running_sha'] = observed['running_sha']
            stamp = receipt['stamp']
            stamp.update(nonce=observed['nonce'], sequence=observed['sequence'], at=observed['observed_begin_unix_ns']+50)
            stamp['attempt'].update(cut_id=observed['cut_id'], generation=observed['generation'])
            receipt['census'].update(stamp=copy.deepcopy(stamp), resources=contract_rows(observed))
        C.publish(root / 'samples' / (observed['nonce'] + '.json'), observed)
        C.publish(root / 'admissions' / (observed['nonce'] + '.json'), dict(version=1, nonce=observed['nonce'], run_id=ID,
            sequence=observed['sequence'], known=True, complete=True, resources=observed['admissions']))
    C.publish(root / 'drains.json', drains(originals))
    if not manifest['arm'].startswith('A-'):
        synthetic_companion(root, originals, manifest)


def file_reference(root, name, body):
    path = Path(root) / name
    S.write_json(path, body)
    return dict(path=name, sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def synthetic_companion(root, originals, manifest):
    # Test input for the evidence reader, never installed as a product port.
    contract = C.arm_contract(manifest)
    resources = [dict(domain=r['domain'], id=r['id'], owner=r['origin_owner']) for r in originals['resources']]
    seal = dict(SourceCommit=manifest['source_commit'], BinarySHA=manifest['artifact_sha'], RunID='p23/round-0/example',
                OriginalDigest=SHA, Original=dict(expected=resources))
    payload = dict(Seal=seal, RunID=seal['RunID'])
    files = [file_reference(root, 'companion-payload.json', payload)]
    common = {key: manifest[key] for key in ('version', 'run_id', 'arm', 'source_commit', 'artifact_sha')}
    common.update(original_set_digest=originals['original_set_digest'], evidence_run_id=seal['RunID'],
                  test=contract['test'], files=files, proofs=contract['proofs'].split(','))
    validation = dict(common, native_rc=0, begin_ns=1_005_000_000_000, end_ns=1_006_000_000_000,
                      events=[dict(Action=action, Test=contract['test']) for action in ('run', 'pass')])
    validation['process'] = dict(native_rc=0,owner={k:v for k,v in OWNER.items() if k != 'instance_id'},executable_sha=SHA,
                                stdout='\n'.join(json.dumps(event) for event in validation['events']))
    invocation=dict(command=C.companion_command(root,contract,'/oracle-input/supervisor','/oracle-input/go'),
                    cwd=str(LIB.parents[3]),corpus=files,sources=C.companion_sources(),process=validation['process'],
                    begin_ns=validation['begin_ns'],end_ns=validation['end_ns'])
    validation['invocation']=file_reference(root,'companion-invocation.json',invocation)
    companion = dict(common, rc=0, validation=file_reference(root, 'companion-validation.json', validation))
    S.write_json(Path(root) / 'scenario-companion.json', companion)
    return companion


def reseal(row, manifest):
    original = synthetic_observation()
    original.update(run_id=manifest['run_id'], arm=manifest['arm'])
    originals = C.seal(original, C.digest(manifest))
    row['original_set_digest'] = originals['original_set_digest']
    for field in ('control_receipt', 'resources_receipt', 'released_receipt'):
        row[field]['census']['expected_digest'] = originals['original_set_digest']
    return originals


def arm_pair(arm):
    row = synthetic_observation()
    manifest = synthetic_manifest(row)
    with (LIB / 'upgrade_tx_arms.tsv').open() as stream:
        contract = next(r for r in csv.DictReader((line for line in stream if not line.startswith('#')), delimiter='\t') if r['arm'] == arm)
    manifest.update(arm=arm, role=contract['role'], topology=contract['topology'], case_id=arm)
    manifest['expect'].update(phase=contract['expected_phase'], outcome=contract['expected_outcome'],
        native_rc=int(contract['cli_rc']), completion=contract['expected_phase'] in {'completed', 'rolled_back'},
        hit_required=contract['family'] == 'W', a6=contract['family'] == 'A')
    row.update(arm=arm, role=manifest['role'], topology=manifest['topology'],
               **{k:manifest['expect'][k] for k in ('phase', 'outcome', 'native_rc')})
    return row, reseal(row, manifest), manifest


def import_drill():
    # Definitions only: never run the deploy-tier script.
    source = (LIB.parent / '38-hot-update-rollback-compat.sh').read_text()
    code = source.split("<<'PY38'\n", 1)[1].split('\nPY38\n', 1)[0]
    namespace = dict(__name__='drill_contract_only')
    exec(compile(code, 'drill38-contract-only', 'exec'), namespace)
    return namespace


def cold_evidence(root):
    row, originals, manifest = arm_pair('H-root')
    (root/'samples').mkdir()
    (root/'admissions').mkdir()
    before = copy.deepcopy(row)
    before.update(nonce='4'*32, direction='reverse', phase='rolled_back', outcome='rolled_back',
                  source_sha=OTHER, target_sha=SHA, running_sha=SHA, effective_sha=SHA)
    for field in ('control_receipt', 'resources_receipt', 'released_receipt'):
        before[field]['running_sha'] = SHA
        before[field]['stamp']['nonce'] = before['nonce']
        before[field]['census']['stamp']['nonce'] = before['nonce']
    row.update(nonce='5'*32, tx_id='6'*32, cut_id='7'*32, generation=3, pid=200, start_ticks=2000, sequence=11,
               observed_begin_ns=1_005_000_000_000, observed_end_ns=1_005_000_000_100)
    fresh = copy.deepcopy(row['resources'][0])
    fresh.update(id='post-cold.control', family='control', domain='control', generation=3,
                 current_owner=dict(OWNER, pid=200, start_ticks=2000, instance_id='cold-owner'), admission_sequence=11)
    for item in row['resources']: item.update(known=False, state='terminated', custody='retired')
    row['resources'].append(fresh)
    row['admissions'] = contract_rows(dict(resources=[fresh]))
    for field in ('control_receipt', 'resources_receipt', 'released_receipt'):
        receipt = row[field]
        stamp = receipt['stamp']
        stamp.update(nonce=row['nonce'], sequence=row['sequence'], producer=copy.deepcopy(fresh['current_owner']))
        stamp['attempt'].update(tx_id=row['tx_id'], cut_id=row['cut_id'], generation=row['generation'])
        receipt['census'].update(stamp=copy.deepcopy(stamp), resources=copy.deepcopy(row['admissions']))
    for sample in (before,row):
        C.publish(root/'samples'/(sample['nonce']+'.json'), sample)
        C.publish(root/'admissions'/(sample['nonce']+'.json'),dict(version=1,nonce=sample['nonce'],run_id=ID,
                  sequence=sample['sequence'],known=True,complete=True,resources=sample['admissions']))
    C.publish(root/'start-observation.json', before)
    C.publish(root/'originals.json', originals)
    C.publish(root/'manifest.json', manifest)
    closed = drains(originals)
    references = [file_reference(root, 'cold-drain-'+str(i)+'.json', value) for i,value in enumerate(closed)]
    boundary = {key:manifest[key] for key in ('version','run_id','arm','source_commit','artifact_sha')}
    boundary.update(original_set_digest=originals['original_set_digest'], begin_ns=1_004_000_000_000,
        end_ns=1_004_500_000_000, before_nonce=before['nonce'],owner_before=copy.deepcopy(OWNER),
        owner_after=copy.deepcopy(fresh['current_owner']),canonical_path='/private/canonical',selected_sha=SHA,
        retired_ids=[r['id'] for r in originals['resources']],drains=references)
    C.publish(root/'cold-boot.json',boundary)
    for value in closed: value['at_ns'] += 5_000_000_000
    C.publish(root/'drains.json',closed)
    synthetic_companion(root, originals, manifest)
    return row, originals, manifest
