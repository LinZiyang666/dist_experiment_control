#!/usr/bin/env -S python3 -I -B
"""S10 assembly and native receipt wiring, with no product repair authority.

The Go Fixture command adapter owns the profile/plan and P18 driver. P25 binds
host-local workload owners; P11b supplies start_peer(source, role) using its
remote shared-supervisor lease. No command is ever read from evidence. The
standalone entry verifies prebuilt host bytes against candidate source before
READY. Verification never installs, repairs or deploys product artifacts.
"""
import argparse
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
try:
    import fullstack_flow as flows
    import fullstack_transfer as transfers
    from fullstack_cli import ProductCLIDriver
    from fullstack_collect import Collector
    from fullstack_requests import load_schedule
    from flow_observer_service import open_custody
except Exception as error:
    # A broken sibling (e.g. a merge conflict left in a helper) would exit 1
    # before main() could classify it, and drill 99 reads 1 as ASSERT-FAIL.
    # Importers keep the original exception; only the drill entry is SETUP.
    # origin: hot-update S10 plan §9 / L9; r9d REPORT Request 4
    if __name__ != '__main__':
        raise
    print('fullstack assembly SETUP-RED: helper import failed: ' + type(error).__name__, file=sys.stderr)
    raise SystemExit(2) from None

ARMS = ('C1', 'C3', 'A', 'B', 'S1', 'S3', 'F1', 'F3', 'K')
PREFIX = '99-hot-update-fullstack.'
SECOND = 1_000_000_000
VERDICTS = ('GREEN', 'ASSERT-FAIL', 'SETUP-RED', 'PRODUCT-RED', 'INCOMPLETE')
OBLIGATIONS = ['prepare', 'product-stage', 'start', 'control-ready', 'resources-ready', 'old-released']
_host_runs = {}  # Native statuses are held by the original in-process waiters.
INFRA = ['physical-cohort', 'temporary-ingress', 'helper', 'launcher', 'canonical-ingress',
         'old-worker-exit', 'temporary-owner-exit']


class Incomplete(RuntimeError):
    native_rc = 4


class AssertionFailure(ValueError):
    native_rc = 1


def require(ok, reason, error=ValueError):
    if not ok:
        raise error(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def unique(rows):
    result = {}
    for key, value in rows:
        require(key not in result, 'duplicate assembly member')
        result[key] = value
    return result


def decode(raw):
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda _: require(False, 'nonfinite assembly number'))


def read_ref(ref):
    require(set(ref) == {'path', 'sha', 'bytes', 'schema'}, 'raw reference fields')
    path = Path(ref['path'])
    require(path.is_absolute() and not path.is_symlink() and path.is_file() and
            type(ref['bytes']) is int and 0 < ref['bytes'] <= 16 << 20, 'raw reference path or size')
    raw = path.read_bytes()
    require(len(raw) == ref['bytes'] and hashlib.sha256(raw).hexdigest() == ref['sha'], 'raw reference digest')
    return raw


def archive(root, name, value):
    raw = canonical(value)
    path = Path(root) / name
    # Each sample/seal is append-only. A failed run cannot overwrite its first
    # receipts or a prior child instance's profile to make a rerun appear green.
    with path.open('xb') as out:
        out.write(raw)
        out.flush()
        os.fsync(out.fileno())
    return dict(path=str(path), sha=hashlib.sha256(raw).hexdigest(), bytes=len(raw), schema=name.split('.')[0])


def load_manifest(path=HERE / 'fullstack_manifest.json'):
    value = decode(Path(path).read_bytes())
    validate_manifest(value)
    return value


# origin: hot-update S10 plan §2.2 / §9 / FullstackFixtureAssemblyTest
def validate_manifest(value):
    require(set(value) == {'schema', 'required_children', 'arms', 'rows', 'obligations',
                          'infra_obligations', 'cohort_windows', 'unit_ceilings', 'soak_cycles',
                          'probe_rate', 'expected_native'}, 'manifest fields')
    require(value['schema'] == 'fullstack-fixture-v1' and value['required_children'] == list(ARMS),
            'required child missing or replaced')
    require(value['probe_rate'] == 10 and value['expected_native'] == 0 and value['soak_cycles'] == 6,
            'workload or verdict changed')
    require(value['obligations'] == OBLIGATIONS and value['infra_obligations'] == INFRA,
            'required component or support obligation missing')
    require(value['cohort_windows'] == dict(start_skew_s=5, leg1_s=900, leg2_ready_s=1050,
            leg2_s=840, last_cut_s=1890, small_budget_s=2092, large_budget_s=2108), 'cohort deadline changed')
    arms = value['arms']
    require([a['id'] for a in arms] == list(ARMS), 'positive arm set differs')
    for arm in arms:
        name = arm['id']
        mode = ('calibration' if name in ('C1', 'C3') else 'fullstack' if name in ('A', 'B') else
                'ss' if name.startswith('S') else 'remote-fs' if name.startswith('F') else 'soak')
        rounds = ['R' + str(i) for i in range(1, 8 if mode == 'fullstack' else 4)]
        if mode in ('calibration', 'soak'):
            rounds = []
        expected = dict(id=name, topology='single' if name in ('C1', 'A', 'S1', 'F1') else 'quorum',
                        mode=mode, rounds=rounds, cohorts=['C0', 'C1'] if mode == 'fullstack' else
                        [] if mode == 'soak' else ['C0'], calibration='C1' if name == 'A' else
                        'C3' if name == 'B' else '' if mode == 'calibration' else 'inline')
        require(arm == expected, 'arm lost a required trajectory or cohort')
    tether = ['T0', 'T1', 'T0', 'T2', 'T3', 'T4', 'T5', 'T5']
    require(len(value['rows']) == 7, 'required rollout row missing')
    for i, row in enumerate(value['rows']):
        roles = (['caddy', 'nats', 'broker', 'agent'] if i == 1 else
                 ['caddy', 'nats', 'agent', 'broker'] if i in (3, 5) else
                 ['nats', 'caddy'] if i == 6 else ['agent', 'broker', 'nats', 'caddy'])
        actions = [[r, 'reverse' if i == 1 or i in (3, 5) and r in ('caddy', 'nats') else 'forward'] for r in roles]
        require(row == dict(id='R' + str(i + 1), **{'from': [tether[i], 'AB'[i % 2]]},
                            to=[tether[i+1], 'BA'[i % 2]], cohort='C0' if i < 3 else 'C1',
                            leg='leg1' if i in (0, 3) else 'leg2', actions=actions),
                'mixed direction, transfer leg or artifact vector changed')
    bounds = [(2400, 4800, 2048)] * 2 + [(6000, 12000, 4096)] * 2 + [(2700, 5400, 2048)] * 4 + [(1800, 3600, 1)]
    require(value['unit_ceilings'] == [dict(unit=PREFIX+a, worst_s=w, ceiling_s=c, minimum_s=m)
            for a, (w, c, m) in zip(ARMS, bounds)], 'exact unit ceilings changed')


def arm_for(manifest, profile):
    require(profile['id'].startswith(PREFIX), 'profile must name its exact launch unit')
    arm = next((a for a in manifest['arms'] if PREFIX+a['id'] == profile['id']), None)
    require(arm is not None and arm['topology'] == profile['topology'], 'arm topology mismatch')
    return arm


def step_manifest(manifest, profile):
    """Consume P01-validated P00 steps; attach support/retirement requirements.

    This check binds the static manifest to the Go plan. It never manufactures a
    product View, transfer receipt or missing runtime from observed survivors.
    """
    arm = arm_for(manifest, profile)
    rows = {r['id']: r for r in manifest['rows']}
    required = [k for k in profile['required'] if k['role'] in ('agent', 'broker', 'nats', 'caddy')]
    expected = {(r['id'], canonical(k)) for r in rows.values() for k in required
                if k['role'] in [a[0] for a in r['actions']]}
    actual = [(s['parent_id'], canonical(s['target'])) for s in profile['steps']]
    require(len(actual) == len(expected) and set(actual) == expected, 'required child runtime step missing')
    result = []
    for step in profile['steps']:
        row, role = rows[step['parent_id']], step['target']['role']
        direction = dict(row['actions'])[role]
        require((step['direction'], step['cohort'], step['leg']) == (direction, row['cohort'], row['leg']),
                'step mixed direction or required leg changed')
        require(step['action'] == 'rollback' if direction == 'reverse' else step['action'] in ('upgrade', 'reload'),
                'step action disagrees with component direction')
        if step['parent_id'] in arm['rounds']:
            result.append(dict(step=copy.deepcopy(step), obligations=OBLIGATIONS + (INFRA if role in ('nats', 'caddy') else [])))
    return result


# origin: hot-update S10 plan §3.3 / FullstackFixtureAssemblyTest
def transfer_window(cohort, leg, originals, receipts, cut_ns, clock_id):
    require(cohort in ('C0', 'C1') and leg in ('leg1', 'leg2'), 'unknown cohort leg')
    require(len(originals) == len(receipts) == 4 and {x['runtime'] for x in originals} == {'a0', 'a1', 'a2', 'a3'},
            'missing required transfer leg', Incomplete)
    # Durations must be projected on the collection host through S7's remaining
    # budget protocol. Subtracting two hosts' raw BOOTTIME values is invalid.
    require(isinstance(clock_id, str) and clock_id and
            all(x['clock_id'] == clock_id for x in originals + receipts), 'cross-host transfer clock', Incomplete)
    ids = {x['transfer_id'] for x in originals}
    require(len(ids) == 4 and {x['transfer_id'] for x in receipts} == ids, 'replacement transfer ID', AssertionFailure)
    starts = [x['start_ns'] for x in originals]
    require(type(cut_ns) is int and cut_ns > 0 and all(type(n) is int and n > 0 for n in starts),
            'transfer clock fields')
    require(max(starts) - min(starts) <= 5 * SECOND, 'cohort start skew', Incomplete)
    for original in originals:
        plan = next(x for x in transfers.cohort_plan(cohort) if x['runtime'] == original['runtime'])
        row = next(x for x in receipts if x['transfer_id'] == original['transfer_id'])
        require(original['cohort'] == cohort and all(type(row[k]) is int for k in
                ('deadline_ns', 'ack_chunks', 'total_chunks', 'first_durable_ns')), 'transfer receipt fields')
        deadline = original['start_ns'] + plan['budget_ns']
        require(original['deadline_ns'] == row['deadline_ns'] == deadline and cut_ns < deadline,
                'same transfer ID passed its original expiry', AssertionFailure)
        require(row['cohort'] == cohort and row['leg'] == leg and row['role'] == ('sender' if leg == 'leg1' else 'receiver')
                and 0 < row['ack_chunks'] < row['total_chunks'], 'missing required live transfer leg', Incomplete)
        first = row['first_durable_ns']
        require(original['start_ns'] <= first <= cut_ns, 'transfer durable receipt clock', Incomplete)
        if leg == 'leg1':
            require(cut_ns - max(x['first_durable_ns'] for x in receipts) <= 900 * SECOND,
                    'leg1 cut window exceeded', Incomplete)
        else:
            require(first - original['start_ns'] <= 1050 * SECOND and cut_ns - first <= 840 * SECOND and
                    cut_ns - original['start_ns'] <= 1890 * SECOND, 'leg2 cut window exceeded', Incomplete)


def upper_bounds(profile, steps, refs):
    """Check same-workload upstream upper bounds BEFORE starting originals.

    Receipts include Prepare and product Stage and every host hop. The sum is
    conservative; a missing timing receipt is never replaced with an estimate.
    """
    require(set(refs) == {r['step']['id'] for r in steps}, 'NOT_RUN pending S6/S8/S9 upper-bound receipts', Incomplete)
    totals = {}
    for entry in steps:
        step = entry['step']
        row = decode(read_ref(refs[step['id']]))
        require(set(row) == {'topology', 'budget_digest', 'workload_digest', 'artifact_digest', 'step_id', 'direction',
                            'from', 'to', 'upper_ns', 'includes_prepare_stage'}, 'upper-bound receipt fields')
        require(all(row[k] == profile[k] for k in ('topology', 'budget_digest', 'workload_digest')) and
                row['artifact_digest'] == hashlib.sha256(canonical(profile['artifacts'])).hexdigest() and
                all(row[k] == step[k] for k in ('direction', 'from', 'to')) and row['step_id'] == step['id'] and
                row['includes_prepare_stage'] is True and type(row['upper_ns']) is int and
                0 < row['upper_ns'] <= 120 * SECOND, 'upper-bound receipt profile mismatch', Incomplete)
        key = step['cohort'], step['leg']
        totals[key] = totals.get(key, 0) + row['upper_ns']
    require(all(n <= (900 if leg == 'leg1' else 840) * SECOND for (_, leg), n in totals.items()),
            'upstream upper bounds cannot fit the transfer window', Incomplete)
    return totals


def artifact_supplier():
    path = HERE.parents[1] / 'lib' / 'fullstack-artifacts.py'
    spec = importlib.util.spec_from_file_location('fullstack_assembly_artifacts', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def bind_image_output(profile, ref):
    """Consume P10 `image` output before READY without renaming its IDs."""
    rows = decode(read_ref(ref))
    expected = [a for a in profile['artifacts'] if a['role'] in ('agent', 'broker', 'ctl')]
    require(len(rows) == 18 and len(expected) == 18 and
            {a['id'] for a in rows} == {f'T{i}-{role}' for i in range(6) for role in ('agent', 'broker', 'ctl')} and
            sorted(rows, key=lambda a: a['id']) == sorted(expected, key=lambda a: a['id']),
            'P10 image output differs from sealed profile')
    return rows


# origin: S10 internal review rv3-assembly F1
def join_children(manifest, children):
    """Only original waiters can authorize a parent; data cannot sign itself.

    The standalone parent has no live waiter capabilities and stays INCOMPLETE
    until P32 binds its runner. It must not replay archived four-field summaries.
    """
    validate_manifest(manifest)
    require(type(children) is list, 'child receipts required')
    seen, codes, instances = set(), [], set()
    for row in children:
        require(set(row) == {'arm', 'instance', 'native_rc', 'verdict', 'raw'}, 'child receipt fields')
        require(row['arm'] in ARMS and row['arm'] not in seen and row['instance'] and row['instance'] not in instances,
                'duplicate child or shared instance')
        require(type(row['native_rc']) is int and 0 <= row['native_rc'] <= 4 and
                row['verdict'] == VERDICTS[row['native_rc']], 'invalid child native verdict')
        raw = decode(read_ref(row['raw']))
        require(all(raw[k] == row[k] for k in ('arm', 'instance', 'native_rc', 'verdict')), 'child raw mismatch')
        require(_host_runs.get(row['instance']) == raw and row['instance'] in _host_runs,
                'NOT_RUN pending original runner native child Join capability', Incomplete)
        seen.add(row['arm']); instances.add(row['instance']); codes.append(raw['native_rc'])
    if seen != set(manifest['required_children']):
        codes.append(4)
    return next((rc for rc in (1, 2, 3, 4) if rc in codes), 0)


def complete_step(ports, step, current, nonce, obligations):
    # origin: S10 internal review rv3-assembly F3
    require(current['phase'] == 'completed' and current['outcome'] == 'success' and not current['deferred'],
            'current component has not completed', Incomplete)
    stamp = current['stamp']
    require(stamp['process']['key'] == step['target'] and stamp['direction'] == step['direction'] and
            stamp['query_nonce'] == nonce and stamp['tx_id'] and stamp['cut_id'], 'current step identity')
    for name, kind in zip(('control', 'resources', 'released'),
                          ('control_ready', 'resources_migrated', 'old_generation_released')):
        proof = current[name]
        require(proof['kind'] == kind and proof['known'] is True and proof['complete'] is True and
                all(proof['stamp'][k] == stamp[k] for k in ('process', 'tx_id', 'cut_id', 'direction',
                                                         'generation', 'scope_digest')), 'current completion receipt')
    # The authoritative Go predicate consumes unchanged S8 DTOs. S9's adapter
    # must also prove retirement; absent adapters never release a dependency.
    require(callable(getattr(ports, 'complete_step', None)),
            'NOT_RUN pending P18 native completion/infra retirement adapter', Incomplete)
    ports.complete_step(step, current, nonce)
    for kind, ref in obligations.items():
        proof = decode(read_ref(ref))
        require(ref['schema'] == 'fullstack-step-obligation-v1' and set(proof) ==
                {'schema', 'kind', 'step', 'stamp', 'known', 'complete', 'evidence'}, 'obligation content schema')
        require(proof['schema'] == ref['schema'] and proof['kind'] == kind and proof['step'] == step and
                proof['stamp'] == stamp and proof['known'] is True and proof['complete'] is True,
                'obligation belongs to another step or cut')
        # Each typed upstream record is validated by its native adapter. A
        # checksummed unrelated document cannot substitute for a release seal.
        require(callable(getattr(ports, 'validate_obligation', None)),
                'NOT_RUN pending P18/P15 native obligation decoder', Incomplete)
        ports.validate_obligation(step, kind, decode(read_ref(proof['evidence'])), current)


class Assembly:
    """Bind P11/P12/P16 with P13–P17 host owners supplied by P25.

    ports.validate(profile) calls the Go P01 validator before any side effect.
    ports.workloads owns original tasks, finite transfers, infra witnesses and
    the sole P17 scheduler; start/sample/drain/stop/join retain its native raw.
    Its drain must join sent attempts, task waiters and transfer owners before
    returning. ports.finish performs the independent P01/P03/P14/P15 judges.
    before_step returns originals/receipts/at_ns/clock_id for transfer_window; the first
    C1 window also carries P14's previous/current/result/retirement relay proof.
    after_step returns obligations (name -> RawRef), window and P04 kernel_raw.
    inputs supplies P10 image_output, same-profile upper_bounds, P11 custody and
    flow_bindings, the original nonce, and P17 request_schedule. The Go adapter
    writes fullstack-request-schedule.json before starting this command; bind
    its RawRef from SealRequestSchedule's exact bytes. Lifecycle sidecars stay
    distinct from schema-registered P00 product observations in evidence.json.
    No synthetic workload implementation or fallback exists in this assembly.
    """
    def __init__(self, profile, inputs, ports, manifest=None):
        self.manifest = load_manifest() if manifest is None else copy.deepcopy(manifest)
        validate_manifest(self.manifest)
        self.profile, self.inputs = copy.deepcopy(profile), copy.deepcopy(inputs)
        self.arm = arm_for(self.manifest, self.profile)
        self.steps = step_manifest(self.manifest, self.profile)
        self.ports, self.phase, self.errors = ports, 'new', []
        self.collector, self.flow = None, None
        self.controls, self.endpoints, self.used_nonces = [], [], set()
        self.work_started = False
        self.root = Path(profile['evidence_root'])
        self.sequence = 0
        self.sent, self.completed = set(), set()
        self.originals = {}
        self.driver = ProductCLIDriver(ports.call_driver)

    def record(self, kind, value):
        self.sequence += 1
        return archive(self.root, kind + '.' + str(self.sequence) + '.json', value)

    def start(self, profile):
        require(self.phase == 'new' and profile == self.profile, 'profile changed or fixture restarted')
        # Validate all dependency evidence before any new business command.
        self.ports.validate(copy.deepcopy(self.profile))
        bind_image_output(self.profile, self.inputs['image_output'])
        upper_bounds(self.profile, self.steps, self.inputs['upper_bounds'])
        schedule_ref = self.inputs['request_schedule']
        self.schedule = load_schedule(read_ref(schedule_ref), schedule_ref['sha'])
        require(self.schedule.slots[0].scheduled_ns == profile['start_ns'] and
                self.schedule.slots[-1].scheduled_ns < profile['end_ns'] <=
                self.schedule.slots[-1].scheduled_ns + 100_000_000, 'schedule differs from profile window')
        require(all(callable(getattr(self.ports, name, None)) for name in
                    ('preflight', 'start_peer', 'before_step', 'after_step', 'finish')) and
                all(callable(getattr(self.ports.workloads, name, None)) for name in
                    ('start', 'sample', 'drain', 'stop', 'join')) and self.ports.logs is not None,
                'NOT_RUN pending P25 workload and observation bindings', Incomplete)
        self.ports.preflight(self.arm, self.inputs)  # P10 image, P11 READY, calibration, sealed P17 slots.
        self.phase = 'starting'
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.record('fullstack-profile', self.profile)
        try:
            sources = {}
            require(set(self.inputs['custody']) == {'a0', 'a1', 'a2', 'a3'}, 'four custody runtimes required')
            for runtime, binding in self.inputs['custody'].items():
                endpoint, control = open_custody(binding['root'], runtime)
                self.endpoints.append(endpoint); self.controls.append(control)
                sources[runtime] = (endpoint, bytes.fromhex(binding['identity']))
            self.collector = Collector(sources, logs=self.ports.logs)
            ceiling = next(r['ceiling_s'] for r in self.manifest['unit_ceilings'] if r['unit'] == profile['id'])
            declared = flows.original_sources(flows.peer_module(), self.inputs['nonce'], ceiling, self.inputs['flow_bindings'])
            self.flow = flows.FlowCohort(self.root / 'original-flows', self.inputs['nonce'], declared, self.ports.start_peer)
            self.flow.start()
            self.work_started = True  # Partial startup still requires Stop/Join.
            receipt = self.ports.workloads.start(self.profile, self.arm, self.inputs)
            self.phase = 'running'
            return self.record('fullstack-start', dict(workloads=receipt, sources_sha=self.flow.digest))
        except BaseException:
            self.stop()
            self.join()
            raise

    def sample(self, nonce):
        require(self.phase == 'running' and nonce and nonce not in self.used_nonces, 'sample phase or nonce')
        self.used_nonces.add(nonce)
        self.record('fullstack-flow-checkpoint', self.flow.sample(nonce))
        for source in self.collector.sources:
            self.collector.drain(source, nonce + '/' + source)
        # P04/P07/P08/P16 keep exact raw stamps and private descriptor custody.
        return self.ports.workloads.sample(nonce, self.collector)

    def apply(self, step_id):
        require(self.phase == 'running', 'cut outside live fixture')
        entry = next((r for r in self.steps if r['step']['id'] == step_id), None)
        require(entry is not None, 'cut not in arm')
        step = entry['step']
        require(step_id not in self.sent and set(step['depends_on']) <= self.completed,
                'cut repeated or previous component has not retired')
        # The owner checks predecessor retirement, finite windows and actual
        # physical cohorts before Prepare; no extra sleeps enlarge a cut window.
        window = self.ports.before_step(step, entry['obligations'])
        cohort = step['cohort']
        if cohort in self.originals:
            require(window['originals'] == self.originals[cohort], 'original transfer identity or deadline changed', AssertionFailure)
        else:
            if cohort == 'C1':
                require('C0' in self.originals and
                        {r['transfer_id'] for r in window['originals']}.isdisjoint(
                            r['transfer_id'] for r in self.originals['C0']), 'relay reused an original ID', AssertionFailure)
                relay = window['relay']
                transfers.check_relay(relay['previous'], relay['current'], relay['result'], relay['retirement'])
            self.originals[cohort] = copy.deepcopy(window['originals'])
        transfer_window(step['cohort'], step['leg'], window['originals'], window['receipts'], window['at_ns'], window['clock_id'])
        self.sent.add(step_id)  # Lost replies keep the original intent reserved.
        staged = self.driver.stage(step)
        applied = self.driver.apply(step)
        nonce = os.urandom(16).hex()
        current = self.driver.query(step['target'], applied['stamp']['tx_id'], nonce)
        receipt = self.ports.after_step(step, entry['obligations'], staged, applied, current)
        require(set(receipt['obligations']) == set(entry['obligations']), 'component retirement obligation missing')
        complete_step(self.ports, step, current, nonce, receipt['obligations'])
        transfer_window(step['cohort'], step['leg'], window['originals'], receipt['window']['receipts'],
                        receipt['window']['at_ns'], receipt['window']['clock_id'])
        artifact = next(a for a in self.profile['artifacts'] if a['id'] == step['to'] and a['role'] == step['target']['role'])
        # P10 joins the genuine product-stage/current stamp to P04's retained
        # executable-FD bytes. Neither a disk hash nor a native exit alone signs it.
        if step['target']['role'] in ('agent', 'broker'):
            receipt['execution_join'] = artifact_supplier().join_execution(artifact, current['stamp'], receipt['kernel_raw'])
        self.completed.add(step_id)
        return self.record('fullstack-step', receipt)

    def drain(self, nonce):
        require(self.phase == 'running' and nonce and nonce not in self.used_nonces, 'drain phase or nonce')
        require(self.completed == {r['step']['id'] for r in self.steps}, 'required component step missing', Incomplete)
        self.used_nonces.add(nonce)
        # Seal owners and sent attempts first. Observation silence is not an
        # owner Join; two fresh drains cannot compensate for an active courier.
        owners = self.ports.workloads.drain(nonce)
        self.flow.drain()
        sealed = self.collector.seal(nonce)
        result = self.ports.finish(self.profile, owners, sealed)
        require(result['rc'] == 0 and result['verdict'] == 'GREEN' and not result['failures'] and not result['missing'],
                'independent fullstack judge rejected the assembly', AssertionFailure)
        self.phase = 'drained'
        return self.record('fullstack-drain', dict(owners=owners, custody=sealed, result=result))

    def _cleanup(self, label, operation):
        try:
            operation()
        except BaseException:
            # Log only a closed criterion; private custody exceptions can
            # contain secrets. Native worker statuses remain in their own raw.
            self.errors.append(label)

    def stop(self):
        if self.phase in ('stopped', 'joined'):
            require(not self.errors, 'fixture cleanup failed')
            return
        if self.work_started:
            self._cleanup('workload_stop', self.ports.workloads.stop)
        if self.flow is not None:
            self._cleanup('flow_close_join', self.flow.close)
        if self.collector is not None:
            self._cleanup('collector_close', self.collector.close)
        self.phase = 'stopped'

    def join(self):
        require(self.phase in ('stopped', 'joined'), 'Stop required before Join')
        if self.phase != 'joined':
            if self.work_started:
                self._cleanup('workload_join', self.ports.workloads.join)
            if self.collector is not None:
                self._cleanup('collector_join', self.collector.join)
            # Collector.join closes intake endpoints; close again is harmless.
            # The P11 control socket must remain alive through that final join.
            for endpoint in self.endpoints:
                self._cleanup('endpoint_close', endpoint.close)
            for control in self.controls:
                self._cleanup('control_close', control.close)
            self.phase = 'joined'
            self.record('fullstack-cleanup', dict(errors=self.errors, complete=not self.errors))
        require(not self.errors, 'fixture cleanup or native supervisor join failed')


def serve(assembly, incoming, outgoing):
    """P25 host entry: use the Go adapter's supervised stdin/stdout protocol."""
    sequence = 0
    native_codes = []
    try:
        for raw in incoming:
            require(len(raw) <= 16 << 20, 'fixture request exceeds ceiling')
            request = decode(raw)
            require(set(request) == {'id', 'method', 'payload'} and type(request['id']) is int and
                    request['id'] == sequence + 1, 'fixture request identity')
            sequence = request['id']
            method, payload, rc, result = request['method'], request['payload'], 0, {}
            try:
                if method == 'start': result = assembly.start(payload)
                elif method == 'sample': result = assembly.sample(payload['nonce'])
                elif method == 'apply': result = assembly.apply(payload['step_id'])
                elif method == 'drain': result = assembly.drain(payload['nonce'])
                elif method == 'stop':
                    assembly.stop(); assembly.join()
                else: raise ValueError('unknown fixture method')
            except Exception as error:
                rc = getattr(error, 'native_rc', 2)
                result = {'criterion': 'assembly_' + method + '_failed'}
            native_codes.append(rc)
            outgoing.write(canonical(dict(id=sequence, rc=rc, payload=result)) + b'\n')
            outgoing.flush()
            if method == 'stop': return next((n for n in (1, 2, 3, 4) if n in native_codes), 0)
    finally:
        assembly.stop()
        assembly.join()
    return 2  # EOF without Stop is not a successful lifecycle.


def driver_bridge(incoming, outgoing):
    def call(method, payload):
        outgoing.write(canonical(dict(driver=method, payload=payload)) + b'\n'); outgoing.flush()
        raw = incoming.readline(16 << 20)
        value = decode(raw)
        require(set(value) == {'view', 'error'} and not value['error'], 'Go product driver failed')
        return canonical(value['view'])
    return call


def run_host(arm, root, profile_path, host, supervisor, judge):
    # origin: S10 internal review rv3-assembly F1
    try:
        paths, owned = flows.source_pinned_tools((host, supervisor, judge))
    except (RuntimeError, ValueError) as error:
        raise Incomplete('NOT_RUN pending P25 source-pinned host, P05 supervisor and P01 judge') from None
    try:
        return _run_host(arm, root, profile_path, *paths)
    finally:
        for fd in owned:
            os.close(fd)


def _run_host(arm, root, profile_path, host, supervisor, judge):
    """Run an explicitly supplied Go assembly host; native status is retained."""
    manifest = load_manifest()
    profile = decode(Path(profile_path).read_bytes())
    require(profile['tier'] == 'deploy' and profile['id'] == PREFIX+arm and
            arm_for(manifest, profile)['id'] == arm and profile['evidence_root'] == str(root), 'deploy profile mismatch')
    require(set(profile) == {'id', 'topology', 'regime', 'tier', 'evidence_root', 'command_bindings',
            'probe_rate', 'start_ns', 'end_ns', 'budget_digest', 'workload_digest', 'required', 'steps', 'artifacts'},
            'complete sealed profile required')
    step_manifest(manifest, profile)
    supplier = artifact_supplier()
    snapshot, _ = supplier.source_snapshot(HERE.parents[3])
    require(all(a['source_commit'] == snapshot['source_commit'] and a['tree'] == snapshot['tree_digest'] and
                a['dirty'] == snapshot['dirty_digest'] for a in profile['artifacts'] if a['role'] in ('agent', 'broker', 'ctl')),
            'profile artifacts differ from current candidate source')
    inputs = decode(Path(profile_path).with_name('inputs.json').read_bytes())
    bind_image_output(profile, inputs['image_output'])
    root = Path(root)
    require(root.is_absolute() and not root.is_symlink(), 'private evidence root required')
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    home = root / 'host-home'
    home.mkdir(mode=0o700)
    env = dict(HOME=str(home), PATH='/usr/bin:/bin', LANG='C', LC_ALL='C', SIM_UPGRADE_SUPERVISOR=supervisor)
    with (root / 'host.stdout').open('xb') as stdout, (root / 'host.stderr').open('xb') as stderr:
        with subprocess.Popen([supervisor, '--', host, '--profile', str(profile_path), '--arm', arm],
                              stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr, cwd=root, env=env) as child:
            try:
                rc = child.wait()
            except BaseException:
                child.terminate(); child.wait()
                raise
    host_status = archive(root, 'fullstack-host-status.json', dict(native_rc=rc))
    require(type(rc) is int and 0 <= rc <= 4, 'supervisor or host failed outside native verdict vocabulary')
    # A worker's exit0 cannot sign a deployment. Use P01's prebuilt, read-only
    # evidence command over the retained archive; never execute an argv in raw.
    with (root / 'judge.stdout').open('xb') as stdout, (root / 'judge.stderr').open('xb') as stderr:
        with subprocess.Popen([supervisor, '--', judge, 'judge', '--profile', str(profile_path), '--evidence', str(root)],
                              stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr, cwd=root, env=env) as child:
            try:
                judge_rc = child.wait()
            except BaseException:
                child.terminate(); child.wait()
                raise
    judge_status = archive(root, 'fullstack-judge-status.json', dict(native_rc=judge_rc))
    require(type(judge_rc) is int and 0 <= judge_rc <= 4, 'judge or supervisor returned non-native status')
    result = decode((root / 'judge.stdout').read_bytes())
    require(set(result) == {'rc', 'verdict', 'failures', 'missing', 'inputs'} and result['rc'] == judge_rc and
            result['verdict'] == VERDICTS[judge_rc], 'complete judge native result required')
    evidence = decode((root / 'evidence.json').read_bytes())
    require(evidence['profile'] == profile and result['inputs'] == evidence['raw'] and evidence['raw'],
            'judge inputs differ from this run')
    if judge_rc == 0:
        require(not result['failures'] and not result['missing'], 'GREEN with unmet criteria')
        for key in (k for k in profile['required'] if k['role'] in ('agent', 'broker')):
            samples = [r for r in evidence['samples'] if r['stamp']['process']['key'] == key]
            require(samples, 'product execution sample missing')
            for sample in samples:
                artifact = next(a for a in profile['artifacts'] if a['role'] == key['role'] and a['binary'] == sample['exe_sha'])
                supplier.join_execution(artifact, sample['stamp'], sample['raw'])
    native = next((n for n in (1, 2, 3, 4) if n in (rc, judge_rc)), 0)
    instance = os.urandom(16).hex()
    row = dict(arm=arm, instance=instance, native_rc=native, verdict=VERDICTS[native], profile=profile,
               source=snapshot, host_status=host_status, judge_status=judge_status, result=result,
               image_output=inputs['image_output'])
    archive(root, 'fullstack-child.json', row)
    _host_runs[instance] = copy.deepcopy(row)
    return native


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    run = sub.add_parser('run')
    run.add_argument('--arm', choices=ARMS, required=True)
    run.add_argument('--root', required=True)
    run.add_argument('--profile', required=True)
    join = sub.add_parser('join')
    join.add_argument('--receipts', required=True)
    args = parser.parse_args()
    try:
        if args.command == 'join':
            return join_children(load_manifest(), decode(Path(args.receipts).read_bytes()))
        return run_host(args.arm, args.root, args.profile, os.environ.get('SIM_FULLSTACK_HOST', ''),
                        os.environ.get('SIM_UPGRADE_SUPERVISOR', ''), os.environ.get('SIM_FULLSTACK_JUDGE', ''))
    except Incomplete as error:
        print(str(error), file=sys.stderr)
        return 4
    except (OSError, ValueError, LookupError, TypeError):
        # A sealed profile, judge result or receipt missing a member is setup
        # evidence, not a judged assertion: drill 99 maps native 1 to ASSERT-FAIL.
        # origin: hot-update S10 plan §9 / L9; r9d REPORT Request 4
        print('fullstack assembly setup evidence invalid', file=sys.stderr)
        return 2
    except Exception as error:
        # Python's own status for an uncaught exception is 1, i.e. ASSERT-FAIL.
        # Native 1 comes only from the host/judge waiters or child receipts.
        print('fullstack assembly SETUP-RED: unclassified driver failure: ' + type(error).__name__, file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
