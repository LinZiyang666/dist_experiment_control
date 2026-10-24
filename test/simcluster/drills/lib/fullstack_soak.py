#!/usr/bin/env -S python3 -I -B
"""Fixed-image 99.K lifecycle adapter and its hermetic rejection controls.

Wrap P19 Assembly in SoakAssembly and pass it to fullstack_fixture.serve.
The host supplies exercise(cycle, kind, nonce, survivors) -> RawRef using P05
supervised victims and P07/P08/P16 observations. It must perform one nonempty
create/IO/settle cycle on BOTH survivors as well as the named fault. Samples
are cumulative P00 Evidence, including active resources and retirement events.
No process is started here, and no deployment action is available as a CLI.
"""
import argparse
import copy
from collections import Counter
import hashlib
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fullstack_fixture import archive, canonical, decode, read_ref

CYCLES = ('agent-kill', 'broker-restart', 'partition', 'transfer-concurrency',
          'agent-kill', 'broker-restart')
IDENTITY = ('process', 'tx_id', 'cut_id', 'generation', 'direction', 'scope_digest')
SIDECARS = ('consumers', 'temps', 'locks', 'old_images')


class SoakFailure(ValueError):
    native_rc = 1


class SoakSetup(ValueError):
    native_rc = 2


def require(ok, criterion, error=SoakFailure):
    if not ok:
        raise error('fullstack soak: ' + criterion)


def positive(value):
    return type(value) is int and value > 0


def image(sample):
    return canonical([sample['exe_sha']] + [sample['stamp'][k] for k in IDENTITY])


def key(sample):
    return canonical(sample['stamp']['process']['key'])


def physical(sample):
    # Compare a multiset, not FD totals: close(original)+leak(dup) must be red.
    # P03 separately judges legitimate metadata retention and all non-FD data.
    return Counter(canonical({**r, 'owner': {k: r['owner'][k] for k in IDENTITY}})
                   for r in sample['resources'] if r['fd'] >= 0)


def checked_ref(ref, *, decode_json=True):
    try:
        raw = read_ref(ref)
        return decode(raw) if decode_json else raw
    except (ValueError, OSError, KeyError, TypeError):
        raise SoakSetup('fullstack soak: missing or corrupt immutable raw') from None


# origin: hot-update S10 plan §4.3 / §10.2 P26 / fixed SHA and M08A/B/D/X
class FixedImageSeries:
    def __init__(self, survivors):
        require(type(survivors) is list and len(survivors) == 2 and
                {s['role'] for s in survivors} == {'agent', 'broker'}, 'survivor population', SoakSetup)
        self.keys = {canonical(s) for s in survivors}
        self.base, self.last, self.seen = {}, {}, {}
        self.failed = False

    def observe(self, evidence, nonce):
        require(not self.failed, 'poisoned series cannot reset its baseline')
        try:
            self._observe(evidence, nonce)
        except (ValueError, KeyError, TypeError):
            self.failed = True
            raise

    def _observe(self, evidence, nonce):
        require(isinstance(nonce, str) and nonce, 'fresh sample nonce')
        latest = {}
        for sample in evidence['samples']:
            owner = key(sample)
            if owner not in self.keys:
                continue
            stamp, proc = sample['stamp'], sample['stamp']['process']
            require(sample['complete'] is True and positive(sample['goroutines']) and
                    type(sample['fds']) is int and sample['fds'] >= 0 and
                    positive(proc['pid']) and positive(proc['start_ticks']) and
                    all(proc[k] for k in ('boot_id', 'pidns', 'instance_id')) and
                    positive(stamp['generation']) and positive(stamp['sequence']) and
                    positive(stamp['revision']) and positive(stamp['at_boot_ns']) and
                    all(stamp[k] for k in ('tx_id', 'cut_id', 'scope_digest', 'query_nonce')) and
                    len(sample['exe_sha']) == 64 and sample['exe_sha'] != '0'*64,
                    'incomplete live product sample')
            raw = checked_ref(sample['raw'], decode_json=False)
            # Raw may be an upstream custody projection. Its digest is always
            # checked; P16 owns decoding and independent executable-FD joins.
            require(bool(raw), 'empty product observation')
            if owner in self.base:
                require(image(sample) == image(self.base[owner]), 'survivor image changed')
            identity = (owner, stamp['sequence'])
            encoded = canonical(sample)
            if identity in self.seen:
                require(self.seen[identity] == encoded, 'historical sample was rewritten')
                continue
            if owner in self.last:
                previous = self.last[owner]['stamp']
                require(stamp['sequence'] > previous['sequence'] and
                        stamp['at_boot_ns'] > previous['at_boot_ns'] and
                        stamp['revision'] >= previous['revision'] and
                        stamp['query_nonce'] != previous['query_nonce'], 'replayed product observation')
            self.seen[identity] = encoded
            self.last[owner] = copy.deepcopy(sample)
            latest[owner] = sample
        require(set(latest) == self.keys, 'missing live survivor sample')
        for owner, sample in latest.items():
            require(sample['stamp']['query_nonce'] == nonce, 'sample did not answer this challenge')
            fds = [r['fd'] for r in sample['resources'] if r['fd'] >= 0]
            require(len(fds) == len(set(fds)) == sample['fds'] and
                    all(r['ofd'] for r in sample['resources'] if r['fd'] >= 0), 'classified FD census')
            require(all({k: r['owner'][k] for k in IDENTITY} ==
                        {k: sample['stamp'][k] for k in IDENTITY} for r in sample['resources']),
                    'foreign resource owner')
            require(not sample['consumers'] and not sample['temps'] and not sample['old_images'],
                    'unsettled consumer, temp or old image')
            if owner not in self.base:
                self.base[owner] = copy.deepcopy(sample)
            base = self.base[owner]
            require(sample['goroutines'] <= base['goroutines'] + 2, 'same-image goroutine floor exceeds baseline +2')
            require(sample['fds'] == base['fds'] and physical(sample) == physical(base), 'owned FD multiset changed')
            require(all(Counter(sample[k]) == Counter(base[k]) for k in SIDECARS), 'owned resource set changed')


# Fault receipts are P26 lifecycle sidecars, not additions to the P00 wire
# schema. Times below share the fault host clock; never subtract product clocks.
# origin: hot-update S10 plan §4.3 / §10.2 P26 / six nonempty cycles
def check_effect(ref, cycle, kind, nonce, survivors, required):
    row = checked_ref(ref)
    require(set(row) == {'cycle', 'kind', 'nonce', 'clock_id', 'started_ns', 'effect_ns',
                        'settled_ns', 'targets', 'observations'}, 'fault receipt fields')
    require(row['cycle'] == cycle and row['kind'] == kind and row['nonce'] == nonce and row['clock_id'] and
            all(positive(row[k]) for k in ('started_ns', 'effect_ns', 'settled_ns')) and
            row['started_ns'] < row['effect_ns'] < row['settled_ns'], 'fault effect identity or window')
    targets = {canonical(k) for k in row['targets']}
    require(targets and len(targets) == len(row['targets']) and targets <= {canonical(k) for k in required},
            'unregistered fault target')
    if kind != 'transfer-concurrency':
        require(targets.isdisjoint({canonical(k) for k in survivors}), 'survivor selected as victim')
    observations = [checked_ref(r) for r in row['observations']]
    require(observations and all(o['nonce'] == nonce and o['clock_id'] == row['clock_id'] for o in observations),
            'fault observation replay or cross-host clock')
    if kind in ('agent-kill', 'broker-restart'):
        require(len(observations) == 1, 'process fault witness count')
        o = observations[0]
        role = 'agent' if kind == 'agent-kill' else 'broker'
        before, after = o['before'], o['after']
        require(canonical(before['key']) in targets and before['key']['role'] == role and
                after['key'] == before['key'] and before['boot_id'] == after['boot_id'] and
                before['pidns'] == after['pidns'] and positive(before['pid']) and positive(after['pid']) and
                positive(before['start_ticks']) and positive(after['start_ticks']) and
                (before['pid'], before['start_ticks']) != (after['pid'], after['start_ticks']) and
                o['waited_process'] == before and o['reaped'] is True and
                (o['signal'] == 9 if kind == 'agent-kill' else o['exit_code'] == 0) and
                positive(o['recovery_bytes']), 'fault did not kill/reap and recover the actual victim')
    elif kind == 'partition':
        require(len(observations) == 1, 'partition witness count')
        o = observations[0]
        require(o['flow_id'] and {canonical(k) for k in o['targets']} == targets and
                positive(o['dropped_bytes']) and positive(o['recovered_bytes']) and
                o['blocked_ns'] == row['effect_ns'] and row['effect_ns'] < o['healed_ns'] < row['settled_ns'],
                'partition had no physical loss and recovery')
    elif kind == 'transfer-concurrency':
        require(len(observations) >= 2 and len({o['transfer_id'] for o in observations}) == len(observations),
                'concurrent transfers missing or replaced')
        require(max(o['started_ns'] for o in observations) < min(o['terminal_ns'] for o in observations),
                'transfers did not overlap')
        for o in observations:
            require(canonical(o['target']) in targets and positive(o['sent_bytes']) and
                    o['sent_bytes'] == o['received_bytes'] and o['sent_sha'] == o['received_sha'] and
                    len(o['sent_sha']) == 64 and o['sent_sha'] != '0'*64 and
                    row['started_ns'] <= o['started_ns'] <= row['effect_ns'] < o['terminal_ns'] <= row['settled_ns'] and
                    o['terminal_ns'] < o['original_deadline_ns'] and o['native_rc'] == 0 and
                    o['terminal_count'] == 1 and not o['consumers'] and not o['temps'],
                    'transfer bytes, original budget or settlement failed')
    else:
        raise SoakFailure('fullstack soak: unknown fault kind')
    return row


# origin: hot-update S10 plan §4.3 / §11.2 P26 / cleanup-before-sample
class SoakAssembly:
    """P19 protocol: Start, baseline Sample, six cycle Samples, final Sample,
    Drain, Stop, Join. Exercise must use the existing shared supervisor. Host
    bindings, actual fault observations and P03 resource judging are mandatory;
    their absence is setup-red. An attempted failed cycle is never replayed.
    """
    def __init__(self, assembly, survivors, exercise):
        self.assembly, self.profile = assembly, copy.deepcopy(assembly.profile)
        require(self.profile['id'] == '99-hot-update-fullstack.K' and self.profile['topology'] == 'quorum',
                '99.K profile required', SoakSetup)
        require(callable(exercise), 'NOT_RUN pending P19/P25 real soak fault host', SoakSetup)
        require(all(k in self.profile['required'] for k in survivors), 'survivor not in sealed roster', SoakSetup)
        self.survivors, self.exercise = copy.deepcopy(survivors), exercise
        self.series, self.phase, self.errors = FixedImageSeries(survivors), 'new', []
        self.snapshots, self.effects, self.nonces = [], [], set()
        self.root = Path(self.profile['evidence_root'])

    def start(self, profile):
        require(self.phase == 'new' and profile == self.profile, 'changed profile or repeated start', SoakSetup)
        self.phase = 'starting'
        result = self.assembly.start(profile)
        self.phase = 'running'
        archive(self.root, 'fullstack-soak-plan.json', dict(cycles=list(CYCLES), survivors=self.survivors,
                profile_sha=hashlib.sha256(canonical(self.profile)).hexdigest()))
        return result

    def sample(self, nonce):
        require(self.phase == 'running' and not self.errors and nonce and nonce not in self.nonces,
                'sample outside healthy live series')
        self.nonces.add(nonce)
        try:
            index = len(self.snapshots) - 1
            if 0 <= index < len(CYCLES):
                ref = self.exercise(index, CYCLES[index], nonce, copy.deepcopy(self.survivors))
                check_effect(ref, index, CYCLES[index], nonce, self.survivors, self.profile['required'])
                self.effects.append(ref)
            evidence = self.assembly.sample(nonce)
            require(evidence['profile'] == self.profile, 'sampled profile changed')
            self.series.observe(evidence, nonce)
            ref = archive(self.root, 'fullstack-soak-sample.' + str(len(self.snapshots)) + '.json', evidence)
            self.snapshots.append(dict(nonce=nonce, raw=ref))
            if len(self.snapshots) == len(CYCLES) + 2:
                self.phase = 'sampled'
            return evidence
        except BaseException:
            self.errors.append('cycle_or_sample_failed')
            raise

    def drain(self, nonce):
        require(self.phase == 'sampled' and not self.errors and nonce and nonce not in self.nonces and
                len(self.effects) == len(CYCLES), 'six cycles and final live sample required before drain')
        self.nonces.add(nonce)
        try:
            # P19's natural drain joins owners and sent attempts, then P03/P16
            # judge resources while daemons live. Only Stop may tear them down.
            drained = self.assembly.drain(nonce)
            proof = checked_ref(drained)
            result = proof['result']
            require(result['rc'] == 0 and result['verdict'] == 'GREEN' and
                    not result['failures'] and not result['missing'], 'natural drain resource judge failed')
            record = dict(profile=self.profile, survivors=self.survivors, cycles=list(CYCLES),
                          snapshots=self.snapshots, effects=self.effects, drain=drained)
            ref = archive(self.root, 'fullstack-soak-series.json', record)
            self.phase = 'drained'
            return ref
        except BaseException:
            self.errors.append('natural_drain_failed')
            raise

    def stop(self):
        if self.phase in ('stopped', 'joined'):
            return
        if self.phase != 'drained':
            self.errors.append('cleanup_before_final_live_sample')
        try:
            self.assembly.stop()
        except BaseException:
            self.errors.append('supervised_stop_failed')
        self.phase = 'stopped'
        # P19 serve calls Stop then Join sequentially, including in finally.
        # Defer reporting until Join so a failed Stop cannot skip the waiter.

    def join(self):
        require(self.phase in ('stopped', 'joined'), 'Stop required before Join')
        if self.phase != 'joined':
            try:
                self.assembly.join()
            except BaseException:
                self.errors.append('supervised_join_failed')
            self.phase = 'joined'
            archive(self.root, 'fullstack-soak-cleanup.json', dict(errors=self.errors, complete=not self.errors))
        require(not self.errors, 'cleanup or native supervisor join failed')


def check_archive(path):
    """Recompute the fixed-image proof from immutable inputs, before cleanup."""
    row = decode(Path(path).read_bytes())
    require(row['cycles'] == list(CYCLES) and len(row['effects']) == len(CYCLES) and
            len(row['snapshots']) == len(CYCLES) + 2, 'incomplete cycle or sample set')
    require(row['profile']['id'] == '99-hot-update-fullstack.K' and
            row['profile']['topology'] == 'quorum', 'archive arm mismatch')
    series = FixedImageSeries(row['survivors'])
    nonces = set()
    for index, snapshot in enumerate(row['snapshots']):
        nonce = snapshot['nonce']
        require(nonce not in nonces, 'archive challenge replay')
        nonces.add(nonce)
        evidence = checked_ref(snapshot['raw'])
        require(evidence['profile'] == row['profile'], 'archive profile changed')
        if 0 < index <= len(CYCLES):
            check_effect(row['effects'][index-1], index-1, CYCLES[index-1], nonce,
                         row['survivors'], row['profile']['required'])
        series.observe(evidence, nonce)
    result = checked_ref(row['drain'])['result']
    require(result['rc'] == 0 and result['verdict'] == 'GREEN' and
            not result['failures'] and not result['missing'], 'archive natural drain failed')
    return row


class FullstackSoakTest(unittest.TestCase):
    """Fabricated raw tests the adapter, never signs a live product mutation."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='fullstack-soak-control-')
        self.addCleanup(self.tmp.cleanup)
        self.root, self.serial = Path(self.tmp.name), 0
        self.keys = [dict(role=role, host='local', unit=role, sid='sid', nid=role, domain=role, uid=1)
                     for role in ('agent', 'broker')]
        self.required = self.keys + [{**k, 'unit': k['unit']+'-victim', 'nid': k['nid']+'-victim'} for k in self.keys]
        self.profile = dict(id='99-hot-update-fullstack.K', topology='quorum', tier='hermetic',
                            evidence_root=str(self.root), required=self.required)

    def ref(self, value):
        self.serial += 1
        return archive(self.root, f'control.{self.serial}.json', value)

    def evidence(self, sequence, nonce):
        samples = []
        for index, k in enumerate(self.keys):
            stamp = dict(process=dict(key=k, boot_id='boot', pidns='ns', instance_id=k['role'],
                                      pid=101+index, start_ticks=100), tx_id='tx', cut_id='cut', generation=1,
                         direction='forward', scope_digest='scope', query_nonce=nonce,
                         sequence=sequence, revision=1, at_boot_ns=sequence*10)
            sample = dict(stamp=stamp, exe_sha='a'*64, goroutines=10, fds=1, complete=True,
                          consumers=[], temps=[], locks=[], old_images=[], resources=[dict(
                              id='socket', kind='raw-socket', domain='flow', fd=3, ofd='original', flags=1,
                              bytes=0, release_after_ns=0, state='live', owner=copy.deepcopy(stamp))])
            sample['raw'] = self.ref(copy.deepcopy(sample))
            samples.append(sample)
        return dict(profile=self.profile, samples=samples)

    def effect(self, cycle, kind, nonce, survivors):
        common = dict(nonce=nonce, clock_id='fault-host')
        targets = self.keys if kind == 'transfer-concurrency' else [self.required[2 if kind == 'agent-kill' else 3]]
        if kind in ('agent-kill', 'broker-restart'):
            before = dict(key=targets[0], pid=200, start_ticks=200, boot_id='boot', pidns='ns')
            observations = [dict(**common, before=before, after={**before, 'pid': 201, 'start_ticks': 201},
                                 waited_process=before, reaped=True, signal=9 if kind == 'agent-kill' else 0,
                                 exit_code=0, recovery_bytes=32)]
        elif kind == 'partition':
            observations = [dict(**common, targets=targets, flow_id='real-flow', dropped_bytes=32,
                                 recovered_bytes=32, blocked_ns=20, healed_ns=25)]
        else:
            observations = [dict(**common, target=k, transfer_id=f'transfer-{i}', started_ns=12+i,
                                 terminal_ns=27+i, original_deadline_ns=100, sent_bytes=32, received_bytes=32,
                                 sent_sha='b'*64, received_sha='b'*64, native_rc=0, terminal_count=1,
                                 consumers=[], temps=[]) for i, k in enumerate(targets)]
        return self.ref(dict(cycle=cycle, kind=kind, nonce=nonce, clock_id='fault-host', started_ns=10,
                             effect_ns=20, settled_ns=30, targets=targets,
                             observations=[self.ref(o) for o in observations]))

    def adapter(self):
        # This double records ordering only; the Go live arm cannot select it.
        owner = self
        class AssemblyDouble:
            profile = owner.profile
            def __init__(self): self.events = []; self.sequence = 0
            def start(self, profile): self.events.append('start'); return owner.ref(profile)
            def sample(self, nonce):
                self.events.append('sample'); self.sequence += 1
                return owner.evidence(self.sequence, nonce)
            def drain(self, nonce):
                self.events.append('drain')
                return owner.ref(dict(result=dict(rc=0, verdict='GREEN', failures=[], missing=[])))
            def stop(self): self.events.append('stop')
            def join(self): self.events.append('join')
        return SoakAssembly(AssemblyDouble(), self.keys, self.effect)

    def run_series(self, adapter):
        adapter.start(self.profile)
        for i in range(8): adapter.sample('challenge-' + str(i))
        adapter.drain('drain')

    def test_six_cycles_precede_live_sample_and_cleanup(self):
        adapter = self.adapter()
        self.run_series(adapter)
        check_archive(self.root / 'fullstack-soak-series.json')
        self.assertEqual(adapter.assembly.events, ['start'] + ['sample']*8 + ['drain'])
        adapter.stop(); adapter.join()
        self.assertEqual(adapter.assembly.events[-2:], ['stop', 'join'])
        self.assertEqual(decode((self.root / 'fullstack-soak-cleanup.json').read_bytes()), dict(errors=[], complete=True))

    def test_one_leak_per_cycle_is_not_noise(self):
        for role in range(2):
            for field in ('goroutines', 'fds', 'owned-fd', 'consumers', 'temps'):
                with self.subTest(role=role, field=field):
                    series = FixedImageSeries(self.keys)
                    series.observe(self.evidence(1, 'base'), 'base')
                    with self.assertRaises(SoakFailure):
                        for cycle in range(1, 7):
                            e = self.evidence(cycle+1, str(cycle))
                            sample = e['samples'][role]
                            if field == 'owned-fd':
                                sample['fds'] += cycle
                                for leaked in range(cycle):
                                    sample['resources'].append({**sample['resources'][0],
                                        'fd': 4+leaked, 'ofd': f'leak-{leaked}', 'id': f'leak-{leaked}'})
                            else:
                                value = sample[field]
                                sample[field] = value+cycle if type(value) is int else [f'leak-{cycle}']
                            series.observe(e, str(cycle))
                    self.assertTrue(series.failed)
                    with self.assertRaises(SoakFailure): series.observe(self.evidence(9, 'reset'), 'reset')

    def test_fixed_identity_includes_sha_cut_and_process(self):
        for axis in ('exe_sha', 'pid', 'start_ticks', 'generation', 'cut_id', 'instance_id', 'boot_id'):
            with self.subTest(axis=axis):
                series = FixedImageSeries(self.keys)
                series.observe(self.evidence(1, 'base'), 'base')
                e = self.evidence(2, 'next'); s = e['samples'][0]
                target = s if axis == 'exe_sha' else s['stamp'] if axis in ('generation', 'cut_id') else s['stamp']['process']
                target[axis] = target[axis]+1 if type(target[axis]) is int else 'b'*64
                with self.assertRaisesRegex(SoakFailure, 'survivor image changed'): series.observe(e, 'next')

    def test_balanced_fd_count_cannot_replace_original(self):
        series = FixedImageSeries(self.keys)
        series.observe(self.evidence(1, 'base'), 'base')
        e = self.evidence(2, 'next'); e['samples'][0]['resources'][0]['ofd'] = 'replacement'
        with self.assertRaisesRegex(SoakFailure, 'owned FD multiset'): series.observe(e, 'next')

    def test_baseline_never_borrows_another_process_owner(self):
        series = FixedImageSeries(self.keys)
        e = self.evidence(1, 'base')
        e['samples'][0]['resources'][0]['owner']['process']['pid'] += 1
        with self.assertRaisesRegex(SoakFailure, 'foreign resource owner'): series.observe(e, 'base')

    def test_missing_replayed_or_poisoned_samples_fail(self):
        for defect in ('missing', 'nonce', 'complete', 'raw', 'foreign', 'duplicate-fd'):
            with self.subTest(defect=defect):
                series = FixedImageSeries(self.keys); series.observe(self.evidence(1, 'base'), 'base')
                e = self.evidence(2, 'next'); s = e['samples'][0]
                if defect == 'missing': e['samples'].pop()
                if defect == 'nonce': s['stamp']['query_nonce'] = 'base'
                if defect == 'complete': s['complete'] = False
                if defect == 'raw': s['raw']['sha'] = '0'*64
                if defect == 'foreign': s['resources'][0]['owner']['process']['pid'] += 1
                if defect == 'duplicate-fd': s['resources'].append(copy.deepcopy(s['resources'][0]))
                with self.assertRaises((SoakFailure, SoakSetup)): series.observe(e, 'next')

    def test_faults_require_effect_and_recovery(self):
        for cycle, kind in enumerate(CYCLES):
            with self.subTest(kind=kind, cycle=cycle):
                ref = self.effect(cycle, kind, 'nonce', self.keys)
                check_effect(ref, cycle, kind, 'nonce', self.keys, self.required)
                row = checked_ref(ref); o = checked_ref(row['observations'][0])
                o['recovery_bytes' if kind in ('agent-kill', 'broker-restart') else
                  'dropped_bytes' if kind == 'partition' else 'received_bytes'] = 0
                row['observations'][0] = self.ref(o)
                with self.assertRaises(SoakFailure): check_effect(self.ref(row), cycle, kind, 'nonce', self.keys, self.required)
        row = checked_ref(self.effect(0, CYCLES[0], 'nonce', self.keys)); row['targets'] = [self.keys[0]]
        with self.assertRaisesRegex(SoakFailure, 'survivor selected'): check_effect(self.ref(row), 0, CYCLES[0], 'nonce', self.keys, self.required)

    def test_cleanup_first_never_manufactures_a_floor(self):
        adapter = self.adapter(); adapter.start(self.profile); adapter.sample('base')
        adapter.stop()
        with self.assertRaises(SoakFailure): adapter.join()
        self.assertEqual(adapter.assembly.events[-2:], ['stop', 'join'])
        with self.assertRaises(SoakFailure): adapter.sample('after-kill')
        with self.assertRaises(SoakFailure): adapter.drain('after-kill')
        self.assertFalse((self.root / 'fullstack-soak-series.json').exists())

    def test_missing_cycle_final_sample_or_supply_fails(self):
        adapter = self.adapter(); adapter.start(self.profile)
        for i in range(7): adapter.sample(str(i))
        with self.assertRaises(SoakFailure): adapter.drain('too-early')
        with self.assertRaises(SoakSetup): SoakAssembly(adapter.assembly, self.keys, None)

    def test_failed_cycle_and_cleanup_are_sticky(self):
        adapter = self.adapter(); adapter.start(self.profile); adapter.sample('base')
        def failed(*args): raise SoakFailure('fault host failed')
        adapter.exercise = failed
        with self.assertRaises(SoakFailure): adapter.sample('cycle')
        with self.assertRaises(SoakFailure): adapter.sample('retry')
        adapter.stop()
        adapter.assembly.join = failed
        with self.assertRaises(SoakFailure): adapter.join()
        self.assertIn('supervised_join_failed', adapter.errors)

    def test_archive_cannot_omit_cycle_or_rewrite_history(self):
        adapter = self.adapter(); self.run_series(adapter)
        path = self.root / 'fullstack-soak-series.json'
        original = decode(path.read_bytes())
        for defect in ('cycle', 'effect', 'sample', 'nonce', 'drain', 'profile'):
            with self.subTest(defect=defect):
                row = copy.deepcopy(original)
                if defect == 'cycle': row['cycles'][0] = 'no-op'
                if defect == 'effect': row['effects'].pop()
                if defect == 'sample': row['snapshots'].pop()
                if defect == 'nonce': row['snapshots'][1]['nonce'] = row['snapshots'][0]['nonce']
                if defect == 'drain': row['drain'] = self.ref(dict(result=dict(rc=1, verdict='ASSERT-FAIL', failures=['leak'], missing=[])))
                if defect == 'profile': row['profile']['id'] = '99-hot-update-fullstack.A'
                mutant = self.root / (defect + '.json'); mutant.write_bytes(canonical(row))
                with self.assertRaises(SoakFailure): check_archive(mutant)

    def test_fault_witness_cannot_replay_wait_or_renew_transfer(self):
        for kind, field, value in (
                ('agent-kill', 'reaped', False), ('agent-kill', 'signal', 0),
                ('broker-restart', 'exit_code', 1), ('partition', 'healed_ns', 20),
                ('transfer-concurrency', 'original_deadline_ns', 21),
                ('transfer-concurrency', 'terminal_count', 2),
                ('transfer-concurrency', 'started_ns', 28),
                ('transfer-concurrency', 'received_sha', 'c'*64)):
            with self.subTest(kind=kind, field=field):
                cycle = CYCLES.index(kind)
                row = checked_ref(self.effect(cycle, kind, 'nonce', self.keys))
                observation = checked_ref(row['observations'][0]); observation[field] = value
                row['observations'][0] = self.ref(observation)
                with self.assertRaises(SoakFailure): check_effect(self.ref(row), cycle, kind, 'nonce', self.keys, self.required)

    def test_stop_error_still_joins_and_preserves_failure(self):
        adapter = self.adapter(); self.run_series(adapter)
        def failed(): raise OSError('control stop failure')
        adapter.assembly.stop = failed
        adapter.stop()
        with self.assertRaises(SoakFailure): adapter.join()
        self.assertEqual(adapter.assembly.events[-1], 'join')
        with self.assertRaises(SoakFailure): adapter.join()

    def test_historical_image_change_cannot_hide_between_floors(self):
        series = FixedImageSeries(self.keys)
        baseline = self.evidence(1, 'base'); series.observe(baseline, 'base')
        middle = self.evidence(2, 'middle'); middle['samples'][0]['exe_sha'] = 'c'*64
        final = self.evidence(3, 'next'); final['samples'] = baseline['samples'] + middle['samples'] + final['samples']
        with self.assertRaisesRegex(SoakFailure, 'survivor image changed'): series.observe(final, 'next')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--selftest', action='store_true')
    mode.add_argument('--check-archive')
    args = parser.parse_args()
    # P32's receipt worker executes the real zero-argument __main__ entry and
    # intercepts TextTestRunner. This module exposes no fault/deploy CLI.
    if not args.check_archive:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(FullstackSoakTest)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    check_archive(args.check_archive)
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (SoakFailure, SoakSetup) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(error.native_rc)
    except (OSError, ValueError, KeyError, TypeError):
        # Invalid or absent raw is supply failure. Keep private source values
        # out of diagnostics and never relabel missing evidence as assertion1.
        print('fullstack soak: setup evidence is missing or malformed', file=sys.stderr)
        raise SystemExit(2)
