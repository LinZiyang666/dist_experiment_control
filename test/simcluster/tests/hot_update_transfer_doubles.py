#!/usr/bin/env python3
"""Two-sided arm controls for drills 63 and 64 (r9d-nonvacuity D-11 sections).

origin: hot-update S7 plan §10.3 / §10.5 / §10.6 / T43 / T44; r9d D-11.

The REAL drill files, the REAL driver library (drills/lib/hot_update_transfer.sh)
and the REAL transfer_mutations.py predicates run inside a private sandbox. Only
the deployment boundary is a double: TRANSFER_PROCESS_SUPERVISOR is a recorder
that executes nothing and answers with scenario-selected native statuses, and
docker/ssh/sudo/systemctl/tether are poisoned on PATH. Nothing here is a
transfer, cut, kill or deployment receipt. Each arm's oracle is driven both to
its expected verdict and, with deliberately bad input, away from it: an oracle
that cannot leave its expected verdict is the permanently-true family r9d exists
for, and one that cannot reach it is a permanently-red arm nobody can sign.

Usage: python3 -I -B tests/hot-update-transfer-drill-test.py --arm <ARM>|all
"""
import argparse
import base64
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

SIM = Path(__file__).resolve().parents[1]
DRIVER = SIM / 'drills/lib/hot_update_transfer.sh'
_spec = importlib.util.spec_from_file_location('transfer_mutations', SIM / 'drills/lib/transfer_mutations.py')
tm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tm)

NAMES = {'63': '63-hot-update-transfer', '64': '64-hot-update-transfer-controls'}
VERDICT = re.compile(r'^DRILL-VERDICT verdict=(\S+) rc=(\d+) assert_fail=(\d+) setup_red=(\d+) '
                     r'product_red=(\d+) not_covered=(\d+) nc_gap=(\d+) nc_guard=(\d+) pass=(\d+) -- ', re.M)
SANDBOX_LINKS = ('lib/log.sh', 'lib/docker.sh', 'lib/tether.sh', 'lib/assert.sh', 'drills/lib/logs.sh',
                 'drills/lib/hot_update_transfer.sh', 'drills/lib/hot_update_budget.sh',
                 'drills/lib/transfer_mutations.py', 'drills/lib/transfer_fixture.py',
                 'drills/lib/transfer_fixture_manifest.py',
                 'drills/lib/transfer_keeper.py', 'drills/lib/transfer_observation.py',
                 'drills/lib/transfer_oracle.py', 'drills/' + NAMES['63'] + '.sh', 'drills/' + NAMES['64'] + '.sh')
CAPABLE_READER = 'tether v9.9.9-hermetic (proto v2)\nimage-domains exec-v1 transfer-v1\nimage-abi v1\n'
PRODUCTION_SOURCE = 'package owner\n\nfunc Run() error {\n\tif pending { return reconcile() }\n\treturn nil\n}\n'


def fixture_deployment(deployment, arm, topology):
    """Explicit custody inputs for the recording boundary; no live receipt."""
    processes, transfers, cuts = [], [], []
    for i, cohort in enumerate(deployment['cohorts']):
        label, tid = 'agent' + str(i), 'transfer' + str(i)
        processes.append(dict(label=label, role='agent', home=cohort['home'],
                              binary_path=cohort['binary_path'], artifact_shas=['a' * 64]))
        processes.append(dict(label='ctl' + str(i), role='ctl', artifact_shas=['a' * 64]))
        transfers.append(dict(cohort=cohort['label'], transfer_id=tid, entry_nonce='nonce' + str(i),
            object_version='NUID' + str(i), object_name='object' + str(i),
            bucket=cohort['bucket'], session_id=cohort['sid'], target=cohort['destination'],
            size=cohort['size'], sha256=cohort['sha256'], rate_limit=cohort['rate_limit_bps'],
            object_stream='OBJ_' + cohort['bucket'], history_stream='history', history_subject='history.' + tid,
            required_resources={'source' + str(i): [s['id'] for s in tm.requirements(arm, topology)]},
            agent_producer=label, resource_refs={k: k + str(i) for k in ('source', 'tmp', 'parent')},
            ctl_producer='ctl' + str(i), ctl_receipt='/isolated/ctl' + str(i) + '.json',
            start_b64=base64.b64encode(json.dumps(dict(transfer_id=tid, kind='start')).encode()).decode(),
            terminal_b64=base64.b64encode(json.dumps(dict(transfer_id=tid, kind='complete')).encode()).decode()))
    for broker in deployment['brokers']:
        processes.append(dict(label=broker['node'], node=broker['node'], role='broker', artifact_shas=['a' * 64]))
    # Import the production projection so test data cannot retain an obsolete
    # observer matrix while driver labels keep passing their separate checks.
    sys.path.insert(0, str(SIM / 'drills/lib'))
    from transfer_fixture_manifest import observer_requirements, resource_requirements
    plan = dict(arm=arm, topology=topology, brokers=deployment['brokers'])
    cuts = observer_requirements(plan, transfers, processes)
    for transfer in transfers:
        transfer['required_resources'] = resource_requirements(transfer, cuts)
    if arm in ('R1', 'R2'):
        tasks = []
        for i, step in enumerate(tm.requirements(arm, topology)):
            transfer = copy.deepcopy(next(t for t in transfers if t['cohort'].endswith(step['direction'])))
            for field in ('transfer_id', 'entry_nonce', 'object_version', 'object_name', 'bucket', 'session_id', 'cohort'):
                transfer[field] = 'cell' + str(i) + transfer[field]
            transfer.update(source='/isolated/cell-source' + str(i), target='/isolated/cell-target' + str(i),
                            ctl_producer='cellctl' + str(i), required_resources={})
            if step['tier'] == 'A': transfer['size'] = 4096
            transfer['launch'] = dict(argv=['/isolated/cell-binary', 'transfer', step['direction'], transfer['source'], transfer['target']],
                                      home='/isolated/cell-home' + str(i))
            tasks.append(dict(cell=step['id'], tier=step['tier'], direction=step['direction'], layer=step['layer'], transfer=transfer))
        deployment['rollback_tasks'] = tasks
    return dict(schema=1, mode='live', quiet_seconds=2, processes=processes,
                transfers=transfers, cuts=cuts, nats=dict(url='nats://127.0.0.1:9'),
                control=dict(launch=dict(argv=['/isolated/control', '{logical_id}', '{effect}'], home='/isolated/control-home')))

# The recorder speaks the driver's documented --log/--receipt protocol. It is a
# double for the deployment boundary only: it never runs a product, docker or
# fixture command, and the driver still writes its own receipt from our status.
SUPERVISOR = r'''#!/bin/sh
[ "$#" -ge 6 ] && [ "$1" = --log ] && [ "$3" = --receipt ] && [ "$5" = -- ] || exit 96
log=$2; receipt=$4; shift 5
trap '_receipt_rc=$?; printf "%s\n" "$_receipt_rc" > "$receipt"' EXIT
F=$FAKE_DIR
printf '%s\n' "$*" >> "$F/calls"
: > "$log" || exit 95
rule_rc() {
    # rules: <glob><TAB><rc>[<TAB><nth>] — the first matching rule answers; with
    # nth, only that occurrence of the match does (later ones fall through).
    _rr=0; _ri=0
    if [ -f "$F/rules" ]; then
        while IFS='<TAB>' read -r _rp _rc _rn; do
            _ri=$((_ri + 1))
            case "$*" in $_rp) ;; *) continue ;; esac
            if [ -n "$_rn" ]; then
                _seen=$(( $(cat "$F/rule.$_ri" 2>/dev/null || echo 0) + 1 ))
                echo "$_seen" > "$F/rule.$_ri"
                [ "$_seen" = "$_rn" ] || continue
            fi
            _rr=$_rc; break
        done < "$F/rules"
    fi
    echo "$_rr"
}
case "${4:-}" in
*/transfer_fixture.py)
    verb=$5; manifest=$7; ev=$9
    [ "$6" = --manifest ] && [ "$8" = --evidence ] || exit 95
    case "$verb" in
    start)
        [ ! -f "$ev/requested-step" ] || exit 0
        rc=0; [ ! -f "$F/rc.start" ] || rc=$(cat "$F/rc.start")
        [ "$rc" = 0 ] || exit "$rc"
        python3 -I -B "$F/manifest_sha.py" "$manifest" "$log" "$ev" "$$" > "$F/manifest.sha" || exit 94
        while [ ! -f "$ev/fake-stopped" ]; do /bin/sleep .02; done
        exit 0 ;;
    stop) : > "$ev/fake-stopped" ;;
    sample)
        step=$(cat "$ev/requested-step" 2>/dev/null) || step=baseline
        src="$F/samples/$step.json"; [ -f "$src" ] || src="$F/samples/baseline.json"
        sed "s/@MANIFEST_SHA@/$(cat "$F/manifest.sha")/" "$src" > "$ev/sample.json" || exit 94 ;;
    judge)
        [ ! -f "$F/judge.out" ] || cp "$F/judge.out" "$log"
        [ ! -f "$F/archive.json" ] || python3 -I -B "$F/archive.py" "$F/archive.json" "$ev" || exit 94 ;;
    esac
    rc=0; [ ! -f "$F/rc.$verb" ] || rc=$(cat "$F/rc.$verb")
    exit "$rc" ;;
esac
case "$1" in
go)
    printf 'GOFLAGS=%s\n' "${GOFLAGS:-}" >> "$F/calls"
    [ "$2" = build ] && [ "$4" = -o ] || exit 97
    printf 'hermetic mutant bytes %s\n' "$3" > "$5" || exit 94
    chmod 0755 "$5"
    exit "$(rule_rc "$@")" ;;
timeout)
    case "$3" in */tether-original) cat "$F/reader.A" > "$log" ;; *) cat "$F/reader.B" > "$log" ;; esac
    exit "$(rule_rc "$@")" ;;
sh)
    [ "${4:-}" = transfer-deploy ] || { printf 'UNEXPECTED %s\n' "$*" >> "$F/calls"; exit 97; }
    exit "$(rule_rc "$@")" ;;
esac
[ "$1" = "$SIM" ] || { printf 'UNEXPECTED %s\n' "$*" >> "$F/calls"; exit 97; }
exit "$(rule_rc "$@")"
'''.replace('<TAB>', '\t')

MANIFEST_SHA = '''import hashlib, json, sys
from pathlib import Path
manifest = Path(sys.argv[1])
plan = json.loads(manifest.with_name('manifest.json').read_text())
print(hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest())
Path(sys.argv[2]).write_text(json.dumps(dict(fixture_pid=int(sys.argv[4]),
    manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
    custody_refs=dict(control=str(Path(sys.argv[3]) / 'control.sock')))) + '\\n')
'''

# P29 archives these beside its judge. The double derives them from the real
# build the driver just made, so a stale or foreign archive cannot be supplied.
ARCHIVE = '''import hashlib, json, sys
from pathlib import Path
config, ev = json.loads(Path(sys.argv[1]).read_text()), Path(sys.argv[2])
build = json.loads((ev / 'mutation/source.json').read_text())
flags = (ev / 'mutation/goflags.txt').read_text().strip()
artifact = hashlib.sha256((ev / 'mutation/tether').read_bytes()).hexdigest()
roles = list(build['roles']) + config.get('extra_roles', [])
if config.get('drop_role'):
    roles = roles[:-1]
processes = [dict(role=role, pid=1000 + i, process_start=5000 + i, boot_id='hermetic-boot',
                  running_sha=config.get('running_sha', artifact), artifact_sha=artifact)
             for i, role in enumerate(roles)]
(ev / 'mutation-runtime.json').write_text(json.dumps(dict(mutation_id=build['mutation_id'],
                                                          build_selector=flags, processes=processes)))
raw = b'hermetic oracle input; not a product witness\\n'
(ev / 'raw.jsonl').write_bytes(raw)
digest = hashlib.sha256(raw).hexdigest()
witness = dict(mutation_id=config.get('witness_id', build['mutation_id']), exercised=config.get('exercised', True),
               criterion='hermetic-arm-control', native_rc=1, raw_path='raw.jsonl', raw_sha=digest,
               oracle_input_path='raw.jsonl', oracle_input_sha=digest, reader_digest='c' * 64)
if build['mutation_id'] in ('M02', 'M12B'):
    witness.update(default_dedup_ns=120000000000, elapsed_s=config.get('elapsed_s', 125))
(ev / 'mutation-witness.json').write_text(json.dumps(witness))
'''


def sha(data):
    return hashlib.sha256(data).hexdigest()


def drill_of(arm):
    return '64' if arm in tm.MUTATIONS else '63'


def topology_of(arm):
    return 'quorum' if arm == 'B' else 'sole-voter' if arm == 'A1' else 'single'


def native_line(verdict, rc):
    counts = dict(GREEN=(0, 0, 0, 0), **{'ASSERT-FAIL': (1, 0, 0, 0), 'SETUP-RED': (0, 1, 0, 0),
                                       'PRODUCT-RED': (0, 0, 1, 0), 'INCOMPLETE': (0, 0, 0, 1)})[verdict]
    return ('DRILL-VERDICT verdict=%s rc=%d assert_fail=%d setup_red=%d product_red=%d not_covered=%d '
            'nc_gap=%d nc_guard=0 pass=1 -- hermetic transfer judge\n' % ((verdict, rc) + counts + (counts[3],)))


def owner_path(arm):
    root = tm.MUTATIONS[arm][0].split()[0]
    return root + 'engine.go' if root.endswith('/') else root


def owned_by(arm, relative):
    return any(relative.startswith(owner) if owner.endswith('/') else relative == owner
               for owner in tm.MUTATIONS[arm][0].split())


def foreign_owner_path(arm):
    for other in tm.MUTATIONS:
        candidate = owner_path(other)
        if not owned_by(arm, candidate):
            return candidate
    raise AssertionError('no foreign owner for ' + arm)


def production_patch(repo, arm, relative=None):
    relative = relative or owner_path(arm)
    path = repo / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(PRODUCTION_SOURCE)
    _, defect, roles = tm.MUTATIONS[arm]
    return dict(schema='transfer-production-patch-v1', mutation_id=arm, defect=defect, roles=roles.split(),
                changes=[dict(path=relative, source_sha256=sha(path.read_bytes()), function='Run',
                              before='if pending { return reconcile() }', after='if pending { return put() }')])


def window_rows(plan, step, **override):
    rows = []
    for cohort in plan['cohorts']:
        role = step.get('roles', {}).get(cohort['label'])
        row = dict(label=cohort['label'], leg=step['leg'], ready=True, elapsed_s=1, complete=False,
                   size=cohort['size'])
        if role:
            row['role'] = role
        if role in ('sender', 'receiver', 'tracker', 'pending-data'):
            row['durable_bytes'] = cohort['size'] // 2
        else:
            row['pending_responsibilities'] = 1
        row.update(override)
        rows.append(row)
    return rows


def signature_step(plan, arm):
    # The arm-specific responsibility, or the arm's last ladder cell when the
    # arm differs only by deployment (A, A1, B and every control).
    special = [s for s in plan['steps'] if not s['action'].startswith(('agent-', 'broker-'))]
    return special[0] if special else plan['steps'][-1]


class Sandbox:
    """A private HERE with the real drill and driver, and a recording boundary."""

    def __init__(self, arm):
        self.arm, self.drill = arm, drill_of(arm)
        self.temp = tempfile.TemporaryDirectory(prefix='transfer-arm-' + arm + '-')
        self.root = Path(self.temp.name)
        self.repo = self.root / 'repo'
        self.here = self.repo / 'test/simcluster'
        for relative in SANDBOX_LINKS:
            target = self.here / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.symlink_to(SIM / relative)
        self.sim = self.here / 'simcluster'
        self.sim.write_text('#!/bin/sh\necho "hermetic SIM must never execute" >&2\nexit 98\n')
        self.sim.chmod(0o700)
        self.fake = self.root / 'fake'
        (self.fake / 'samples').mkdir(parents=True)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        for name in ('docker', 'ssh', 'sudo', 'systemctl', 'tether', 'iptables', 'runuser', 'nsenter'):
            self.write(self.bin / name, '#!/bin/sh\nprintf "%s\\n" "$0 $*" >> "$FAKE_DIR/poisoned"\nexit 99\n', True)
        # The assert frame's first-failure recorder reads `docker ps` as host
        # telemetry. That read-only listing is allowed (and finds nothing);
        # every other docker verb is still a reached deployment command.
        self.write(self.bin / 'docker', '#!/bin/sh\n[ "$1" = ps ] && exit 1\n'
                   'printf "%s\\n" "$0 $*" >> "$FAKE_DIR/poisoned"\nexit 99\n', True)
        self.write(self.bin / 'sleep', '#!/bin/sh\nexit 0\n', True)
        # A deterministic clock for the driver's own deadlines; other formats
        # (evidence timestamps) still come from the real date(1).
        self.write(self.bin / 'date', '#!/bin/sh\nif [ "$*" = +%s ]; then\n'
                   '    n=$(cat "$FAKE_DIR/clock" 2>/dev/null || echo 1700000000); n=$((n + 10))\n'
                   '    echo "$n" > "$FAKE_DIR/clock"; echo "$n"\nelse\n    exec "$REAL_DATE" "$@"\nfi\n', True)
        self.write(self.fake / 'supervisor', SUPERVISOR, True)
        self.write(self.fake / 'manifest_sha.py', MANIFEST_SHA)
        self.write(self.fake / 'archive.py', ARCHIVE)
        self.write(self.fake / 'reader.A', CAPABLE_READER)
        self.write(self.fake / 'reader.B', CAPABLE_READER)
        self.evidence = self.root / 'evidence'
        self.deployment = self.make_deployment()
        self.save_deployment()
        self.plan = tm.make_manifest(self.deployment, arm)
        self.write_samples()
        if self.drill == '64':
            (self.root / 'patches').mkdir()
            self.write(self.root / 'patches' / (arm + '.json'), json.dumps(production_patch(self.repo, arm)))

    @staticmethod
    def write(path, text, executable=False):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        if executable:
            path.chmod(0o700)

    def close(self):
        self.temp.cleanup()

    def artifact(self, key):
        binary = self.root / 'artifacts' / key
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_bytes(('hermetic capable artifact ' + key + '\n').encode())
        return dict(binary=str(binary), url='https://artifact.invalid/' + key, sha256=sha(binary.read_bytes()),
                    version='v9.9.9-' + key.lower(), transfer_version='transfer-v1')

    def make_deployment(self):
        arm, topology = self.arm, topology_of(self.arm)
        watched = arm in ('W1', 'W2')
        cohorts = []
        for i, label in enumerate(tm.COHORTS):
            small = label.startswith('small')
            cohorts.append(dict(label=label, size=66549440 if small else 1073741824,
                                rate_limit_bps=(1048576 if small else 8388608) if watched else (262144 if small else 4194304),
                                sha256=('%x' % (i + 10)) * 64, verb='push' if label.endswith('push') else 'pull',
                                sid='sid' + str(i), bucket='bucket' + str(i), nid='nid' + str(i), agent_node='agt' + str(i),
                                binary_path='/isolated/agt%d/tether' % i, home='/isolated/agt%d/home' % i,
                                source='/isolated/agt%d/source' % i, destination='/isolated/agt%d/destination' % i,
                                force=not small))
        nodes = ['brk1', 'brk2', 'brk3'] if topology == 'quorum' else ['brk1']
        value = dict(transport='caddy-wss', cohorts=cohorts, artifacts={key: self.artifact(key) for key in ('A', 'B')},
                     brokers=[dict(node=node, socket='/run/tether/%s-admin.sock' % node) for node in nodes])
        if arm in ('R1', 'R2'):
            for step in tm.requirements(arm, topology):
                value['artifacts'][step['id']] = self.artifact(step['id'])
        if arm == 'X':
            value['victims'] = [dict(kind=kind, sid='victim-sid' + str(i), bucket='victim-bucket' + str(i))
                                for i, kind in enumerate(('cancel', 'session-rm'))]
        if arm in tm.MUTATIONS:
            value['mutation_artifact_urls'] = {name: 'https://artifact.invalid/mutant-' + name for name in ('A', 'B')}
        value['fixture'] = fixture_deployment(value, arm, topology)
        return value

    def save_deployment(self):
        self.write(self.root / 'deployment.json', json.dumps(self.deployment, sort_keys=True))

    def sample_doc(self, step, rows=None, **extra):
        doc = dict(manifest_sha='@MANIFEST_SHA@', sequence=2, windows={step['id']: rows or window_rows(self.plan, step)})
        if step['action'] in ('C1', 'C2'):
            doc['connections'] = {'carrier': [dict(node='agt0', peer='10.9.0.1', port=443),
                                              dict(node='agt2', peer='10.9.0.3', port=443)],
                                  'data': [dict(node='agt1', peer='10.9.0.2', port=4222)]}
        if step['action'] == 'cancel':
            doc['victims'] = {'cancel': dict(ctl_pid=4242)}
        if self.arm in ('R1', 'R2'):
            doc['rollback_tasks'] = {step['id']: dict(ready=True, tier=step['tier'], direction=step['direction'])}
        doc.update(extra)
        return doc

    def write_samples(self):
        self.write(self.fake / 'samples/baseline.json', json.dumps(dict(manifest_sha='@MANIFEST_SHA@', sequence=1, windows={})))
        for step in self.plan['steps']:
            self.set_sample(step['id'], self.sample_doc(step))
            if step['action'] in ('W1', 'W2'):
                self.set_sample(step['id'] + '-release', dict(
                    manifest_sha='@MANIFEST_SHA@', sequence=3, windows={},
                    watchdog={step['action']: dict(current_cut_observed=True, elapsed_s=120, pending_data=True)}))

    def set_sample(self, name, doc):
        self.write(self.fake / 'samples' / (name + '.json'), json.dumps(doc))

    def scenario(self, judge_rc=None, rules=(), archive=None):
        for name in ('calls', 'rules', 'judge.out', 'archive.json', 'rc.judge', 'rc.drain', 'poisoned', 'clock'):
            (self.fake / name).unlink(missing_ok=True)
        for counter in self.fake.glob('rule.*'):
            counter.unlink()
        if judge_rc is None:
            judge_rc = 1 if self.drill == '64' else 0
        verdict = ('GREEN', 'ASSERT-FAIL', 'SETUP-RED', 'PRODUCT-RED', 'INCOMPLETE')[judge_rc]
        self.write(self.fake / 'judge.out', native_line(verdict, judge_rc))
        self.write(self.fake / 'rc.judge', str(judge_rc))
        self.write(self.fake / 'rules', ''.join('\t'.join(map(str, rule)) + '\n' for rule in rules))
        if self.drill == '64':
            self.write(self.fake / 'archive.json', json.dumps(archive or {}))

    def env(self, arm=None):
        env = dict(PATH=str(self.bin) + os.pathsep + os.environ.get('PATH', os.defpath), HOME=str(self.root / 'home'),
                   LC_ALL='C', HERE=str(self.here), ARM=arm or self.arm, SIM_DRILL_NAME=NAMES[self.drill],
                   INSTANCE='drill-' + NAMES[self.drill], SIM_EVIDENCE_DIR=str(self.evidence), SIM=str(self.sim),
                   SIM_VENDOR=str(self.root / 'vendor'), TRANSFER_PROCESS_SUPERVISOR=str(self.fake / 'supervisor'),
                   TRANSFER_DEPLOYMENT_MANIFEST=str(self.root / 'deployment.json'), FAKE_DIR=str(self.fake),
                   REAL_DATE=shutil.which('date'))
        if self.drill == '64':
            env['TRANSFER_MUTATION_PATCHES'] = str(self.root / 'patches')
        return env

    def run_drill(self, arm=None):
        for old in self.evidence.glob('hot-update-transfer.*'):
            shutil.rmtree(old)
        result = subprocess.run(['sh', str(self.here / 'drills' / (NAMES[self.drill] + '.sh'))], cwd=self.root,
                                env=self.env(arm), capture_output=True, text=True, timeout=120)
        return Outcome(self, result)

    def run_functions(self, body, names, timeout=60):
        source = DRIVER.read_text()
        functions = ''
        for name in names:
            match = re.search(r'^' + re.escape(name) + r'\(\) \{\n.*?^}\n', source, re.M | re.S)
            if match is None:
                raise AssertionError('missing real driver function ' + name)
            functions += match.group()
        frame = ('HERE="$1"\n. "$HERE/lib/assert.sh"\nlog() { :; }; ok() { :; }; err() { :; }\n'
                 '. "$HERE/drills/lib/hot_update_budget.sh"\nTRANSFER_STEP=0\n'
                 'TRANSFER_STARTED=1; TRANSFER_JUDGED=0; TRANSFER_FAULT_NODES=; TRANSFER_FIXTURE_JOB=; TRANSFER_PRIOR_GOFLAGS=\n')
        result = subprocess.run(['sh', '-c', frame + functions + body, 'transfer-control', str(self.here)],
                                cwd=self.root, env=self.env(), capture_output=True, text=True, timeout=timeout)
        return Outcome(self, result)

    def calls(self):
        path = self.fake / 'calls'
        return path.read_text().splitlines() if path.exists() else []

    def events(self):
        events = []
        deploy = ' transfer-deploy ' + str(self.here) + ' '
        for line in self.calls():
            if deploy in line:
                words = line.split(deploy, 1)[1]
                match = re.fullmatch(r'd cp (\S+) sim-\S+-(brk\d):/tmp/tether-s7-candidate', words)
                if match:
                    events.append(('stage-cp', match[2], match[1]))
                elif re.fullmatch(r'dexec (brk\d) -- sh -eu -c .*install -m 0755 /tmp/tether-s7-candidate /usr/local/bin/tether', words):
                    events.append(('stage-install', words.split()[1]))
                elif re.fullmatch(r'dexec \S+ -- ss -K dst \S+ dport = \S+', words):
                    w = words.split()
                    events.append(('kill-conn', w[1], w[6], w[9]))
                elif words == 'sctl brk1 restart nats-server':
                    events.append(('restart-nats',))
                elif re.fullmatch(r'dexec \S+ -- iptables -[ID] OUTPUT -p tcp --syn --dport 4222 -j DROP', words):
                    w = words.split()
                    events.append(('drop-syn' if w[4] == '-I' else 'undrop-syn', w[1]))
                elif words == 'grow_to_3 4 1 0':
                    events.append(('grow',))
                else:
                    events.append(('deploy?', words))
            elif line.startswith(str(self.sim) + ' '):
                words = line[len(str(self.sim)) + 1:]
                match = re.fullmatch(r'ctl -- node upgrade (\S+) --url (\S+) --sha256 (\S+)', words)
                if match:
                    events.append(('agent',) + match.groups())
                elif re.fullmatch(r'exec brk1 -- /usr/local/bin/tether broker upgrade reload --socket \S+ '
                                  r'--to-version \S+ --expect-sha256 \S+', words):
                    events.append(('reload', words.split()[-1]))
                elif re.fullmatch(r'exec brk1 -- runuser -u tether -- env HOME=/var/lib/tether /usr/local/bin/tether '
                                  r'cluster upgrade --broker-only --to-version \S+ --expect-sha256 \S+ '
                                  r'--account-seed /etc/tether/secrets/account.nk --backup-taken', words):
                    events.append(('roll', words.split()[-4]))
                elif re.fullmatch(r'exec ctl -- kill -INT \d+', words):
                    events.append(('cancel', words.split()[-1]))
                elif re.fullmatch(r'ctl -- session rm \S+', words):
                    events.append(('session-rm', words.split()[-1]))
                else:
                    events.append(('sim?', words))
        return events

    def fixture_verbs(self):
        return [line.split()[4] for line in self.calls() if '/transfer_fixture.py ' in line]

    def expected_events(self, plan):
        nids = [c['nid'] for c in plan['cohorts']]
        agents = [c['agent_node'] for c in plan['cohorts']]

        def agent(key):
            a = plan['artifacts'][key]
            return [('agent', nid, a['url'], a['sha256']) for nid in nids]

        def broker(key):
            a = plan['artifacts'][key]
            events = []
            for b in plan['brokers']:
                events += [('stage-cp', b['node'], a['binary']), ('stage-install', b['node'])]
            return events + [('roll' if plan['topology'] == 'quorum' else 'reload', a['sha256'])]

        events = [('grow',)] if self.arm == 'B' else []
        for step in plan['steps']:
            action = step['action']
            if action.startswith('agent-'):
                events += agent('B' if action.endswith('forward') else 'A')
            elif action.startswith('broker-'):
                events += broker('B' if action.endswith('forward') else 'A')
            elif action in ('C1', 'C2'):
                doc = json.loads((self.fake / 'samples' / (step['id'] + '.json')).read_text())
                field = 'carrier' if action == 'C1' else 'data'
                events += [('kill-conn', c['node'], c['peer'], str(c['port'])) for c in doc['connections'][field]]
            elif action == 'C3':
                events.append(('restart-nats',))
            elif action in ('W1', 'W2'):
                events += [('drop-syn', n) for n in agents] + agent('B') + [('undrop-syn', n) for n in agents]
            elif action == 'cancel':
                events.append(('cancel', '4242'))
            elif action == 'session-rm':
                events.append(('session-rm', next(v['sid'] for v in plan['victims'] if v['kind'] == 'session-rm')))
            elif action in ('R1', 'R2'):
                events += agent(step['id']) if step['layer'] == 'agent' else broker(step['id'])
            else:
                raise AssertionError('unscheduled action ' + action)
        return events

    def transfer_evidence(self):
        found = sorted(self.evidence.glob('hot-update-transfer.' + self.arm + '.*'))
        return found[-1] if found else None


class Outcome:
    def __init__(self, sandbox, result):
        self.sandbox, self.rc, self.stdout, self.stderr = sandbox, result.returncode, result.stdout, result.stderr
        rows = VERDICT.findall(result.stderr)
        self.verdicts = rows
        row = rows[-1] if rows else None
        self.verdict = row[0] if row else None
        self.counts = dict(zip(('rc', 'assert_fail', 'setup_red', 'product_red', 'not_covered', 'nc_gap',
                                'nc_guard', 'pass'), map(int, row[1:]))) if row else {}

    def describe(self):
        return 'rc=%s verdict=%s counts=%s\nstderr tail:\n%s\ncalls tail:\n%s' % (
            self.rc, self.verdict, self.counts, '\n'.join(self.stderr.splitlines()[-25:]),
            '\n'.join(self.sandbox.calls()[-12:]))


