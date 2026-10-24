#!/usr/bin/env python3
"""fullstack_transfer_test.py — hermetic S10 transfer protocol adversaries.

All product/JS/GC rows here are static protocol controls, never deployment
receipts. Only atomic-target reads exercise real temporary local files.
P23 owns real-binary continuation, infrastructure cuts and five-round leaks.
"""

import base64
import copy
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import sys
import struct
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fullstack_transfer as transfer
from transfer_oracle import TargetOracle


def sha(data):
    return hashlib.sha256(data).hexdigest()


def encoded(value):
    return base64.b64encode(json.dumps(value, sort_keys=True).encode()).decode()


@lru_cache(maxsize=8)
def zero_digest(size):
    # These static physical-row vectors use actual digests of the declared
    # zero-filled bytes, computed in bounded memory rather than invented SHA.
    h = hashlib.sha256()
    block = bytes(1 << 20)
    for offset in range(0, size, len(block)):
        h.update(block[:min(len(block), size - offset)])
    return h.hexdigest()


def event(t, producer, kind, leg='leg1', at=10):
    row = dict.fromkeys(transfer.TEXT, '')
    row.update(dict.fromkeys(transfer.NUMBERS, 0))
    row.update(dict.fromkeys(transfer.BOOLEANS, False))
    row.update(schema=1, kind=kind, phase='sending', process_start=80,
               boot_id='boot-' + producer, pid=100 + int(t['runtime'][1]), generation=1,
               transfer_id=t['transfer_id'], entry_nonce=t['entry_nonce'],
               object_version=t['object_version'], leg=leg, event_seq=1,
               boottime_ns=at * transfer.SECOND, producer=producer)
    for name in ('tmp', 'source', 'parent'):
        row[name] = dict(resource_id=t['transfer_id'] + '.' + name, name=name, kind='xfer-' + name + '-v1')
    row['original_deadline'] = dict(boot_id=row['boot_id'], deadline=t['budget_ns'] + transfer.SECOND)
    row['attempt'] = dict(logical_id='', ordinal=0, phase='', started_mono_ns=0,
                          finished_mono_ns=0, sent=False, **{'class': ''}, backoff_ns=0)
    return row


def manifest_fixture(cohort='C0', invocation=''):
    ts, cuts, processes = [], [], []
    for p in transfer.cohort_plan(cohort):
        runtime = p['runtime']
        tid = cohort + '-' + runtime
        for role in ('agent', 'ctl'):
            processes.append(dict(label=role + runtime, role=role, home='/fixture/' + role + runtime,
                                  binary_path='/fixture/bin/' + role + runtime, artifact_shas=[sha(b'fixture')]))
        t = dict(p, transfer_id=tid, entry_nonce='entry-' + tid + invocation, object_version='NUID-' + tid,
                 bucket='bucket-' + runtime, object_name=tid, target='/fixture/target/' + tid,
                 sha256=zero_digest(p['size']), history_stream='history-' + runtime,
                 object_stream='objects-' + runtime, history_subject='audit.' + runtime,
                 owners=['agent' + runtime, 'ctl' + runtime, 'broker'])
        t['identity'] = dict(sid='sid-' + runtime, nid=runtime, actor_fp='actor', agent_instance='instance-' + runtime,
            transfer_id=tid, entry_nonce=t['entry_nonce'], intent_digest=sha(tid.encode()), object_version=t['object_version'])
        t['origin'] = dict(broker_id='broker', instance_id='instance-broker', boot_id='boot-broker', generation=1)
        t['budget'] = dict(started_at='2026-10-12T00:00:00Z', size=p['size'], rate_limit_bps=p['rate_limit'],
                          effective_budget_ns=p['budget_ns'], remaining_ns=p['budget_ns'], source_sealed=True)
        t['start_b64'] = encoded(dict(transfer_id=tid, kind='start', entry_nonce=t['entry_nonce']))
        terminal_id = sha(('xfer-terminal-v1\n' + t['identity']['sid'] + '\n' + tid + '\n' + t['entry_nonce']).encode())
        t['terminal_b64'] = encoded(dict(transfer_id=tid, kind='complete', entry_nonce=t['entry_nonce'],
            bytes=p['size'], sha256=t['sha256'], terminal_id=terminal_id, session=t['identity']['sid'],
            node=runtime, actor_fp='actor', agent_instance=t['identity']['agent_instance'],
            intent_digest=t['identity']['intent_digest'], object_version=t['object_version']))
        sender = ('ctl' if p['direction'] == 'push' else 'agent') + runtime
        receiver = ('agent' if p['direction'] == 'push' else 'ctl') + runtime
        for leg, rounds in transfer.ROUNDS[cohort].items():
            for rnd in rounds:
                for component in (('nats', 'caddy') if rnd == 'R7' else transfer.COMPONENTS):
                    cuts.append(dict(transfer_id=tid, leg=leg, round=rnd, component=component,
                        cut_id=rnd + '-' + component, producer=sender if leg == 'leg1' else receiver,
                        role='sender' if leg == 'leg1' else 'receiver'))
        t['resource_kinds'] = {tid + '.' + name: name for name in ('source', 'tmp', 'parent')}
        t['required_resources'] = {tid + '.' + name: sorted({c['cut_id'] for c in cuts if c['transfer_id'] == tid and
                                   c['leg'] == ('leg1' if name == 'source' else 'leg2')})
                                   for name in ('source', 'tmp', 'parent')}
        ts.append(t)
    processes.append(dict(label='broker', role='broker', artifact_shas=[sha(b'fixture')]))
    return dict(schema=1, mode='hermetic', cohort=cohort, transfers=ts, cuts=cuts, processes=processes,
                quiet_seconds=2, nats=dict(url='nats://fixture.invalid'), control=dict(logical_ids=[]))


def settled_fixture(cohort='C0', invocation='', attempts_per_transfer=1):
    m = manifest_fixture(cohort, invocation)
    events, targets, ofds, rows, gc, starts, attempts = [], {}, {}, {}, {}, {}, []
    for t in m['transfers']:
        tid, runtime, half = t['transfer_id'], t['runtime'], t['size'] // 2
        sender = ('ctl' if t['direction'] == 'push' else 'agent') + runtime
        receiver = ('agent' if t['direction'] == 'push' else 'ctl') + runtime
        for leg, owner, kind, at in (('leg1', sender, 'append', 10), ('leg2', receiver, 'synced', 1025)):
            for index in range(2):
                e = event(t, owner, kind, leg, at + index * 100)
                e.update(index=index, offset=index * half, length=half, sha256=zero_digest(half),
                         actual_ack_seq=index + 1, durable_offset=(index + 1) * half,
                         fetched_offset=(index + 1) * half, last_consumed_stream_seq=index + 1,
                         hash_state_digest=zero_digest((index + 1) * half), file_synced=True)
                if kind == 'synced':
                    e['phase'] = 'receiving'
                events.append(e)
            for c in m['cuts']:
                if c['transfer_id'] == tid and c['leg'] == leg:
                    e = event(t, owner, 'rollback' if c['round'] == 'R2' else 'install', leg, at + 20)
                    e['cut_id'] = c['cut_id']
                    events.append(e)
        p = event(t, receiver, 'primitive_exit', 'leg2', 1150)
        p.update(primitive='rename', outcome='landed', phase='decided')
        events.append(p)
        p = event(t, receiver, 'directory_sync', 'leg2', 1151)
        p.update(parent_synced=True, phase='decided')
        events.append(p)
        for owner in t['owners']:
            p = event(t, owner, 'retirement', 'leg2', 1160)
            p.update(admission_closed=True, retirement_digest=sha((tid + owner).encode()),
                     stop_nonce='stop-' + tid, phase='settled')
            events.append(p)
        targets[tid] = [dict(state='old'), dict(state='new')]
        for resource, cuts in t['required_resources'].items():
            ofds[resource] = dict(checks=[dict(cut_id=c, equal=True) for c in cuts])
        rows[t['history_stream']] = [dict(subject=t['history_subject'], sequence=i + 1, data_b64=t[k])
                                     for i, k in enumerate(('start_b64', 'terminal_b64'))]
        subject = '$O.' + t['bucket'] + '.C.' + t['object_version']
        objects = [dict(subject=subject, sequence=i + 1, bytes=half, sha256=zero_digest(half),
                        prefix_sha256=zero_digest((i + 1) * half)) for i in range(2)]
        objects.append(dict(subject='$O.' + t['bucket'] + '.M.' + base64.urlsafe_b64encode(t['object_name'].encode()).decode(),
            sequence=3, data_b64=encoded(dict(nuid=t['object_version'], size=t['size'], chunks=2,
                digest='SHA-256=' + base64.urlsafe_b64encode(bytes.fromhex(t['sha256'])).decode()))))
        rows[t['object_stream']] = objects
        gc[tid] = dict(transfer_id=tid, entry_nonce=t['entry_nonce'], object_version=t['object_version'],
            default_pass=True, live_preserved=True, orphan_removed=True, writer_closed=True,
            cleanup_complete=True, owner_generation=1, retirement_digest=sha((tid + 'broker').encode()), raw_sha256=sha(b'GC'))
        starts[tid] = dict(scheduler_start_ns=transfer.SECOND, leg2_elapsed_ns=1025 * transfer.SECOND,
                          last_cut_elapsed_ns=1045 * transfer.SECOND, settled_elapsed_ns=2070 * transfer.SECOND)
        body = base64.b64decode(t['terminal_b64'])
        publication = dict(identity=copy.deepcopy(t['identity']), origin=copy.deepcopy(t['origin']),
            terminal_id=json.loads(body)['terminal_id'], req_id=sha(b'xferaudit-record:' + body),
            canonical_digest=sha(body))
        for ordinal in range(1, attempts_per_transfer + 1):
            sent = event(t, 'broker', 'attempt', 'leg2', 1155)
            sent.update(phase='history_confirmed', history_seq=2, stop_nonce=publication['terminal_id'],
                        canonical_digest=sha(body), cut_id='R3-caddy')
            sent['attempt'].update(logical_id=publication['terminal_id'], ordinal=ordinal, phase='request', sent=True,
                                  started_mono_ns=1153 * transfer.SECOND, finished_mono_ns=1154 * transfer.SECOND)
            sent['attempt']['class'] = 'ok'
            events.append(sent)
            attempts.append(dict(owner='broker', logical_id=publication['terminal_id'], ordinal=ordinal, sent=True,
                disposition='acknowledged', sent_ns=1153 * transfer.SECOND, accepted_ns=0,
                closed_ns=1154 * transfer.SECOND, late=False, publication=publication))
    events.sort(key=lambda e: e['boottime_ns'])
    sequences = {}
    for e in events:
        key = e['producer']
        sequences[key] = sequences.get(key, 0) + 1
        e['event_seq'] = sequences[key]
        if e['kind'] == 'attempt':
            receipt = next(a for a in attempts if a['publication']['identity']['transfer_id'] == e['transfer_id']
                           and a['ordinal'] == e['attempt']['ordinal'])
            receipt['event'] = {k: e[k] for k in ('producer', 'pid', 'process_start', 'boot_id',
                                                 'generation', 'event_seq', 'cut_id')}
            receipt['raw_sha256'] = sha(json.dumps(e, sort_keys=True, separators=(',', ':')).encode())
    snapshot = dict(events=events, targets=targets, resources=dict(ofd_checks=ofds), failures=[], setup=[])
    owners = [p['label'] for p in m['processes']]
    seals = [dict(acks=dict.fromkeys(owners, i + 1), cursor=len(events), at_ns=(1200 + i) * transfer.SECOND,
                  nonce='fresh-' + str(i), history_verified=True, default_window_ns=120 * transfer.SECOND,
                  raw_sha256=sha(str(i).encode())) for i in range(2)]
    js = {name: dict(before=dict(store_identity='store-' + name, stream=name, created='original',
                                cursor=1, consumers=['old']),
                     after=dict(store_identity='store-' + name, stream=name, created='original',
                                cursor=3, consumers=[], retired_consumers=['old'])) for name in rows}
    statuses = {}
    for t in m['transfers']:
        before = dict(state='live', phase='sending', identity=t['identity'], origin=t['origin'], budget=t['budget'],
                      durable_offset=0, chunk_index=0, last_consumed_stream_seq=0, terminal=None,
                      cleanup_pending=False, code='', error='')
        after = copy.deepcopy(before)
        after.update(state='terminal', phase='settled', durable_offset=t['size'], chunk_index=2,
                     last_consumed_stream_seq=2, terminal=dict(kind='complete', history_seq=2,
                         history_stream=t['history_stream'], terminal_id=json.loads(base64.b64decode(t['terminal_b64']))['terminal_id'],
                         bytes=t['size'], canonical_digest=sha(base64.b64decode(t['terminal_b64'])),
                         req_id=sha(b'xferaudit-record:' + base64.b64decode(t['terminal_b64'])), sha256=t['sha256'], code=''))
        after['budget']['remaining_ns'] = 0
        statuses[t['transfer_id']] = dict(before=before, after=after)
    return m, snapshot, rows, dict(gc=gc, starts=starts, attempts=attempts, js=js, status=statuses,
                                 attempt_population_complete=True, seals=seals)


class FullstackTransferFixtureTest(unittest.TestCase):
    # origin: S10 internal review rv-harness F3
    def test_transfer_judge_rejects_unrelated_publication_closure(self):
        # The reviewer's Python counterexample, kept in the owned Python suite.
        m, snapshot, rows, closing = settled_fixture()
        baseline = transfer.judge_evidence(m, snapshot, rows, closing)
        self.assertEqual(baseline['native_rc'], 0, baseline)
        closing['attempts'] = [dict(closing['attempts'][0], owner='foreign-owner', logical_id='prior-arm-call')]
        got = transfer.judge_evidence(m, snapshot, rows, closing)
        self.assertNotEqual(got['native_rc'], 0, 'foreign attempt closes all four current transfers')
        self.assertEqual(got['native_rc'], 1, got)
        self.assertEqual(got['setup'], [], got)

    # origin: S10 internal review rv-harness F3
    def test_prior_invocation_cannot_close_current_publications(self):
        m, snapshot, rows, closing = settled_fixture(invocation='-current')
        old_m, old_snapshot, old_rows, old_closing = settled_fixture(invocation='-old')
        self.assertEqual(transfer.judge_evidence(m, snapshot, rows, closing)['native_rc'], 0)
        self.assertEqual(transfer.judge_evidence(old_m, old_snapshot, old_rows, old_closing)['native_rc'], 0)
        self.assertEqual([t['transfer_id'] for t in m['transfers']],
                         [t['transfer_id'] for t in old_m['transfers']])
        for current, old in zip(m['transfers'], old_m['transfers']):
            self.assertNotEqual(current['identity'], old['identity'])
            self.assertNotEqual(current['terminal_b64'], old['terminal_b64'])
        closing['attempts'] = old_closing['attempts']
        got = transfer.judge_evidence(m, snapshot, rows, closing)
        self.assertEqual(got['native_rc'], 1, 'old invocation closed current publications: ' + repr(got))
        self.assertEqual(got['setup'], [], got)

    # origin: S10 internal review rv-harness F3
    def test_publication_closure_precedes_each_seal_cursor(self):
        m, snapshot, rows, closing = settled_fixture()
        self.assertEqual(transfer.judge_evidence(m, snapshot, rows, closing)['native_rc'], 0)
        # Custody cursors share the collector's order; remote BOOTTIME does not.
        for index in range(2):
            with self.subTest(seal=index):
                bad = copy.deepcopy(closing)
                bad['seals'][index]['cursor'] = 0
                got = transfer.judge_evidence(m, snapshot, rows, bad)
                self.assertEqual(got['native_rc'], 1, got)
                self.assertEqual(got['setup'], [], got)

    # origin: S10 internal review rv-harness F3
    def test_foreign_publications_are_rejected_before_first_drain(self):
        m, snapshot, rows, closing = settled_fixture()
        fixture = object.__new__(transfer.TransferFixture)
        fixture.manifest = m
        fixture.keeper = SimpleNamespace(sample=lambda: snapshot, save=Mock(),
                                         stop_event=SimpleNamespace(wait=Mock(return_value=False)))
        fixture.history_windows = {t['history_stream']: 120 * transfer.SECOND for t in m['transfers']}
        fixture.scans = {name: SimpleNamespace(rows=value) for name, value in rows.items()}
        fixture.snapshot = lambda: snapshot
        fixture.drain = Mock(side_effect=copy.deepcopy(closing['seals']))
        self.assertEqual(fixture.judge(closing)['native_rc'], 0)
        self.assertEqual(fixture.drain.call_count, 2)
        for replacement in ([dict(closing['attempts'][0], owner='foreign-owner', logical_id='prior-arm-call')],
                            settled_fixture(invocation='-old')[3]['attempts']):
            fixture.drain = Mock(side_effect=copy.deepcopy(closing['seals']))
            closing['attempts'] = replacement
            with self.assertRaises(transfer.SetupError):
                fixture.judge(closing)
            fixture.drain.assert_not_called()

    # origin: S10 internal review rv-harness F3
    def test_publication_binding_and_raw_event_fields_are_exact(self):
        m, snapshot, rows, closing = settled_fixture()
        mutations = [('publication', key, 'f' * 64) for key in ('req_id', 'terminal_id', 'canonical_digest')]
        mutations += [('identity', key, 'foreign') for key in m['transfers'][0]['identity']]
        mutations += [('origin', key, 2 if key == 'generation' else 'foreign')
                      for key in m['transfers'][0]['origin']]
        mutations += [('event', key, 'foreign' if type(value) is str else value + 1)
                      for key, value in closing['attempts'][0]['event'].items()]
        mutations += [('row', key, value) for key, value in (('ordinal', 2), ('raw_sha256', 'f' * 64),
                      ('sent_ns', 1), ('closed_ns', 1156 * transfer.SECOND))]
        for section, key, value in mutations:
            with self.subTest(section=section, key=key):
                bad = copy.deepcopy(closing)
                receipt = bad['attempts'][0]
                target = {'row': receipt, 'publication': receipt['publication'], 'event': receipt['event'],
                          'identity': receipt['publication']['identity'], 'origin': receipt['publication']['origin']}[section]
                target[key] = value
                got = transfer.judge_evidence(m, snapshot, rows, bad)
                self.assertEqual(got['native_rc'], 1, got)
                self.assertEqual(got['setup'], [], got)

    # origin: S10 internal review rv-harness F3
    def test_sent_ledger_requires_every_retry_and_rejects_complete_boolean(self):
        m, snapshot, rows, closing = settled_fixture(attempts_per_transfer=2)
        self.assertEqual(transfer.judge_evidence(m, snapshot, rows, closing)['native_rc'], 0)
        for mutation in ('missing-retry', 'duplicate', 'no-ledger', 'no-digest', 'wrong-digest', 'unretired'):
            with self.subTest(mutation=mutation):
                s, c = copy.deepcopy(snapshot), copy.deepcopy(closing)
                if mutation == 'missing-retry':
                    c['attempts'].pop()
                elif mutation == 'duplicate':
                    c['attempts'][-1] = copy.deepcopy(c['attempts'][0])
                elif mutation == 'no-ledger':
                    s['events'] = [e for e in s['events'] if e['kind'] != 'attempt']
                elif mutation in ('no-digest', 'wrong-digest'):
                    next(e for e in s['events'] if e['kind'] == 'attempt')['canonical_digest'] = (
                        '' if mutation == 'no-digest' else 'f' * 64)
                else:
                    # Retirement of the same label from another incarnation is insufficient.
                    for e in s['events']:
                        if e['kind'] == 'retirement' and e['producer'] == 'broker':
                            e['process_start'] += 1
                got = transfer.judge_evidence(m, s, rows, c)
                self.assertEqual(got['native_rc'], 1, got)
                self.assertEqual(got['setup'], [], got)

    # origin: S10 internal review rv-honesty F5
    def test_closing_rejects_live_cleanup(self):
        m, s, r, c = settled_fixture()
        self.assertEqual(transfer.judge_evidence(m, s, r, c)['native_rc'], 0)
        for pair in c['status'].values():
            pair['after'].update(state='live', phase='cleanup_pending', terminal=None,
                                 cleanup_pending=True, code='settlement_pending')
        result = transfer.judge_evidence(m, s, r, c)
        self.assertNotEqual(result['native_rc'], 0, 'unsettled live Status accepted: ' + repr(result))
        self.assertEqual(result['setup'], [])

    # origin: S10 internal review rv-honesty F5
    def test_closing_rejects_unrelated_terminal(self):
        m, s, r, c = settled_fixture()
        self.assertEqual(transfer.judge_evidence(m, s, r, c)['native_rc'], 0)
        for pair in c['status'].values():
            pair['after']['terminal'].update(history_stream='other-stream', history_seq=999,
                canonical_digest='f' * 64, sha256='a' * 64, code='failed')
        result = transfer.judge_evidence(m, s, r, c)
        self.assertNotEqual(result['native_rc'], 0, 'unrelated terminal receipt accepted: ' + repr(result))
        self.assertEqual(result['setup'], [])

    # origin: S10 internal review rv-honesty F5
    def test_every_closing_field_is_joined_to_history_and_retirement(self):
        m, s, r, original = settled_fixture()
        tid = m['transfers'][0]['transfer_id']
        mutations = [("status", field, value) for field, value in (
            ('phase', 'cleanup_pending'), ('cleanup_pending', True), ('code', 'settlement_pending'),
            ('error', 'cleanup failed'), ('durable_offset', 1))]
        mutations += [("terminal", field, value) for field, value in (
            ('history_stream', 'wrong'), ('history_seq', 999), ('canonical_digest', 'f' * 64),
            ('req_id', 'wrong'), ('terminal_id', 'wrong'), ('bytes', 1), ('sha256', 'a' * 64), ('code', 'failed'))]
        mutations += [("gc", 'owner_generation', 2), ("gc", 'retirement_digest', 'a' * 64),
                      ("attempt", 'owner', 'unknown'), ("attempt", 'logical_id', 'unknown')]
        for section, field, value in mutations:
            with self.subTest(section=section, field=field):
                c = copy.deepcopy(original)
                row = {'status': c['status'][tid]['after'], 'terminal': c['status'][tid]['after']['terminal'],
                       'gc': c['gc'][tid], 'attempt': c['attempts'][0]}[section]
                row[field] = value
                result = transfer.judge_evidence(m, s, r, c)
                self.assertEqual(result['native_rc'], 1, result)
                self.assertEqual(result['setup'], [], result)
        # Intermediate queries remain legal without closing a cohort.
        transfer.check_status(original['status'][tid]['before'], original['status'][tid]['before'])

    # origin: S10 internal review rv-honesty F5
    def test_history_reencoding_cannot_change_terminal_identity_or_outcome(self):
        for field, value in (('terminal_id', 'wrong'), ('sha256', 'a' * 64), ('bytes', 1),
                             ('code', 'failed'), ('error', 'failed'), ('outcome_unknown', True),
                             ('entry_nonce', 'wrong'), ('session', 'wrong'), ('node', 'wrong'),
                             ('actor_fp', 'wrong'), ('agent_instance', 'wrong'), ('intent_digest', 'a' * 64),
                             ('object_version', 'wrong')):
            with self.subTest(field=field):
                m, s, r, c = settled_fixture()
                t = m['transfers'][0]
                record = json.loads(base64.b64decode(t['terminal_b64']))
                record[field] = value
                t['terminal_b64'] = encoded(record)
                r[t['history_stream']][-1]['data_b64'] = t['terminal_b64']
                body = base64.b64decode(t['terminal_b64'])
                c['status'][t['transfer_id']]['after']['terminal'].update(
                    canonical_digest=sha(body), req_id=sha(b'xferaudit-record:' + body))
                with self.assertRaises(transfer.SetupError):
                    transfer.check_closing_status(c['status'][t['transfer_id']]['before'],
                        c['status'][t['transfer_id']]['after'], t, r[t['history_stream']])
                result = transfer.judge_evidence(m, s, r, c)
                self.assertEqual(result['native_rc'], 1, result)
                self.assertEqual(result['setup'], [], result)

    # origin: hot-update S10 plan §10.2 P14 / §3.3 / §5.4.
    def test_frozen_cohorts_use_four_real_domains_and_original_budgets(self):
        for cohort in ('C0', 'C1'):
            m = manifest_fixture(cohort)
            self.assertIs(transfer.validate_manifest(m), m)
            self.assertEqual([p['budget_ns'] // transfer.SECOND for p in transfer.cohort_plan(cohort)],
                             [2092, 2092, 2108, 2108])
        for key in ('size', 'rate_limit', 'budget_ns'):
            m = manifest_fixture()
            m['transfers'][0][key] += 1
            with self.subTest(field=key), self.assertRaises(transfer.SetupError):
                transfer.validate_manifest(m)
        m = manifest_fixture()
        t = m['transfers'][0]
        t['required_resources'][t['transfer_id'] + '.source'] = []
        with self.assertRaises(transfer.SetupError):
            transfer.validate_manifest(m)

    def test_original_identity_origin_and_budget_projection_are_complete(self):
        for field in ('identity', 'origin', 'budget'):
            t = manifest_fixture()['transfers'][0]
            t[field]['foreign'] = 'not an extension'
            with self.subTest(field=field), self.assertRaises(transfer.SetupError):
                transfer.validate_binding(t)
        t = manifest_fixture()['transfers'][0]
        t['budget']['source_sealed'] = False
        with self.assertRaises(transfer.SetupError):
            transfer.validate_binding(t)

    def test_every_s7_raw_field_including_zero_is_required(self):
        t = manifest_fixture()['transfers'][0]
        row = event(t, 'ctla0', 'append')
        row.pop('producer')
        transfer.validate_event(row)
        for field in transfer.FIELDS:
            bad = copy.deepcopy(row)
            bad.pop(field)
            with self.subTest(field=field), self.assertRaises(transfer.SetupError):
                transfer.validate_event(bad)
        for key, value in (('index', 1 << 32), ('errno', 1 << 31), ('durable_offset', 1 << 63)):
            bad = copy.deepcopy(row)
            bad[key] = value
            with self.subTest(width=key), self.assertRaises(transfer.SetupError):
                transfer.validate_event(bad)
        for key in ('tmp', 'parent', 'source', 'attempt', 'original_deadline'):
            bad = copy.deepcopy(row)
            bad[key]['unknown'] = 'private data'
            with self.subTest(nested=key), self.assertRaises(transfer.SetupError):
                transfer.validate_event(bad)

    def test_both_cohorts_have_nonempty_sender_receiver_windows(self):
        for cohort in ('C0', 'C1'):
            m, snapshot, rows, closing = settled_fixture(cohort)
            self.assertEqual(transfer.coverage(m, snapshot['events']), [])
            self.assertEqual(transfer.judge_evidence(m, snapshot, rows, closing)['native_rc'], 0)

    def test_metadata_cannot_supply_receiver_coverage(self):
        m, snapshot, _, _ = settled_fixture()
        # Mandatory mutation: metadata is present but no original tmp Sync.
        for e in snapshot['events']:
            if e['kind'] == 'synced':
                e['kind'], e['phase'] = 'finalize', 'prepared'
        self.assertTrue(transfer.coverage(m, snapshot['events']))

    def test_same_name_new_nuid_is_rejected(self):
        m, snapshot, rows, closing = settled_fixture()
        # Mandatory mutation: same pathname/business ID, replacement object.
        snapshot['events'][0]['object_version'] = 'replacement-NUID'
        result = transfer.judge_evidence(m, snapshot, rows, closing)
        self.assertEqual(result['native_rc'], 1)
        self.assertIn('original_transfer_binding', result['failures'])

    def test_partial_target_then_restoration_remains_failed(self):
        # Mandatory mutation exercises actual read-only target observations.
        with tempfile.TemporaryDirectory(prefix='s10-transfer-') as directory:
            path = Path(directory) / 'target'
            old, new = b'complete old destination', b'complete new destination'
            path.write_bytes(old)
            observer = TargetOracle(path, [len(old), sha(old)], [len(new), sha(new)])
            self.assertEqual(observer.sample('before')['state'], 'old')
            path.write_bytes(new[:7])
            self.assertEqual(observer.sample('write')['state'], 'partial')
            path.write_bytes(new)
            self.assertEqual(observer.sample('sync')['state'], 'new')
            self.assertTrue(observer.failed)
            m, snapshot, rows, closing = settled_fixture()
            snapshot['targets'][m['transfers'][0]['transfer_id']] = observer.samples
            self.assertIn('atomic_destination', transfer.judge_evidence(m, snapshot, rows, closing)['failures'])

    def test_atomic_target_reads_cover_both_tiers_and_receiver_directions(self):
        with tempfile.TemporaryDirectory(prefix='s10-atomic-matrix-') as directory:
            for tier in ('A', 'B'):
                for direction in ('push', 'pull'):
                    with self.subTest(tier=tier, direction=direction):
                        path = Path(directory) / (tier + direction)
                        old, new = b'old complete content', b'new complete content'
                        if direction == 'push':
                            path.write_bytes(old)
                        observer = transfer.DestinationObserver(dict(tier=tier, direction=direction,
                            transfer_id=tier + direction, entry_nonce='entry', target=path,
                            old=[len(old), sha(old)] if direction == 'push' else None,
                            size=len(new), sha256=sha(new)))
                        self.assertEqual(observer.sample('write')['state'], 'old' if direction == 'push' else 'absent')
                        tmp = path.with_suffix('.tmp')
                        tmp.write_bytes(new)
                        self.assertEqual(observer.sample('sync')['state'], 'old' if direction == 'push' else 'absent')
                        os.replace(tmp, path)
                        final = observer.sample('primitive_exit')
                        self.assertEqual(final['state'], 'new')
                        self.assertEqual(final['transfer_id'], tier + direction)
                        self.assertFalse(observer.failed)

    def test_late_opposite_terminal_after_second_seal_is_rejected(self):
        m, snapshot, rows, closing = settled_fixture()
        self.assertEqual(transfer.judge_evidence(m, snapshot, rows, closing)['native_rc'], 0)
        t = m['transfers'][0]
        # Mandatory mutation: late frame adds failed after exact complete.
        rows[t['history_stream']].append(dict(subject=t['history_subject'], sequence=4,
            data_b64=encoded(dict(transfer_id=t['transfer_id'], kind='failed', code='late'))))
        self.assertIn('canonical_history', transfer.judge_evidence(m, snapshot, rows, closing)['failures'])

    def test_history_scans_all_matching_rows_and_allows_other_transfers(self):
        m, _, rows, _ = settled_fixture()
        t = m['transfers'][0]
        history = rows[t['history_stream']]
        history.insert(1, dict(subject=t['history_subject'], sequence=3,
                              data_b64=encoded(dict(transfer_id='another-call', kind='start'))))
        transfer.check_history(history, t)
        history.append(dict(subject=t['history_subject'], sequence=4, data_b64=t['terminal_b64']))
        with self.assertRaises(transfer.SetupError):
            transfer.check_history(history, t)

    def test_gc_and_owner_receipts_precede_two_fresh_barriers(self):
        m, snapshot, rows, closing = settled_fixture()
        result = transfer.judge_evidence(m, snapshot, rows, None)
        self.assertEqual(result['native_rc'], 4)
        self.assertTrue(result['missing'])
        for defect in ('gc', 'attempt', 'owner', 'barrier'):
            s, c = copy.deepcopy(snapshot), copy.deepcopy(closing)
            if defect == 'gc':
                c['gc'][m['transfers'][0]['transfer_id']]['orphan_removed'] = False
            elif defect == 'attempt':
                c['attempts'][0]['disposition'] = 'pending'
            elif defect == 'owner':
                s['events'] = [e for e in s['events'] if e['producer'] != 'broker']
            else:
                c['seals'][1] = copy.deepcopy(c['seals'][0])
            with self.subTest(defect=defect):
                self.assertEqual(transfer.judge_evidence(m, s, rows, c)['native_rc'], 1)

    def test_collection_damage_is_setup_and_missing_receipts_are_incomplete(self):
        m, snapshot, rows, closing = settled_fixture()
        snapshot['events'][0].pop('file_synced')
        result = transfer.judge_evidence(m, snapshot, rows, closing)
        self.assertEqual(result['native_rc'], 2)
        self.assertFalse(result['failures'])
        m, snapshot, rows, closing = settled_fixture()
        closing.pop('status')
        result = transfer.judge_evidence(m, snapshot, rows, closing)
        self.assertEqual(result['native_rc'], 4)
        self.assertTrue(result['missing'])

    def test_default_window_requires_real_sent_frame_fence_after_125_seconds(self):
        attempt = settled_fixture()[3]['attempts'][0]
        attempt.update(late=True, disposition='fence_rejected', accepted_ns=attempt['sent_ns'] + transfer.SECOND,
                       closed_ns=attempt['sent_ns'] + 126 * transfer.SECOND)
        transfer.check_attempts([attempt])
        attempt['closed_ns'] = attempt['sent_ns'] + 3 * transfer.SECOND
        with self.assertRaises(transfer.SetupError):
            transfer.check_attempts([attempt])

    def test_absolute_original_deadlines_and_cut_windows_are_enforced(self):
        m, snapshot, _, closing = settled_fixture()
        transfer.check_starts(m, closing['starts'])
        for field, value in (('leg2_elapsed_ns', 1051), ('last_cut_elapsed_ns', 1891),
                             ('settled_elapsed_ns', 2093), ('scheduler_start_ns', 7)):
            bad = copy.deepcopy(closing['starts'])
            bad[m['transfers'][0]['transfer_id']][field] = value * transfer.SECOND
            with self.subTest(field=field), self.assertRaises(transfer.SetupError):
                transfer.check_starts(m, bad)
        for e in snapshot['events']:
            if e['kind'] == 'install' and e['leg'] == 'leg1':
                e['boottime_ns'] += 901 * transfer.SECOND
        self.assertTrue(transfer.coverage(m, snapshot['events']))

    def test_js_immutables_survive_legitimate_server_and_replica_changes(self):
        before = dict(store_identity='store', stream='OBJ', created='created', cursor=4,
                      consumers=['old'], server_id='old-server', replicas=['n1', 'n2', 'n3'])
        after = dict(before, cursor=5, consumers=['new'], retired_consumers=['old'],
                     server_id='new-server', replicas=['n2', 'n3', 'n4'])
        transfer.check_js(before, after)
        for field in ('store_identity', 'stream', 'created'):
            bad = dict(after)
            bad[field] = 'replacement'
            with self.subTest(field=field), self.assertRaises(transfer.SetupError):
                transfer.check_js(before, bad)

    def test_duplicate_json_keys_and_nonfinite_values_are_rejected(self):
        for raw in ('{"identity":1,"identity":2}', '{"pending":NaN}'):
            with self.subTest(raw=raw), self.assertRaises(transfer.SetupError):
                transfer.strict_load(raw)

    def test_status_cannot_renew_budget_or_change_original_origin(self):
        pair = settled_fixture()[3]['status']['C0-a0']
        transfer.check_status(pair['before'], pair['after'])
        for name in ('origin', 'identity', 'started_at', 'remaining_ns'):
            bad = copy.deepcopy(pair['after'])
            if name in ('origin', 'identity'):
                bad[name][next(iter(bad[name]))] = 'replacement'
            elif name == 'remaining_ns':
                bad['budget'][name] = pair['before']['budget'][name] + 1
            else:
                bad['budget'][name] = '2026-10-13T00:00:00Z'
            with self.subTest(field=name), self.assertRaises(transfer.SetupError):
                transfer.check_status(pair['before'], bad)
        bad = copy.deepcopy(pair['after'])
        bad['budget']['capsule'] = 'foreign private payload'
        with self.assertRaises(transfer.SetupError):
            transfer.check_status(pair['before'], bad)

    def test_relay_requires_exact_previous_settle_and_cleanup(self):
        previous, current = manifest_fixture('C0'), manifest_fixture('C1')
        retired = {t['transfer_id']: dict(entry_nonce=t['entry_nonce'], start_count=1, terminal_count=1,
            terminal_b64=t['terminal_b64'], pending_grants=0, pending_publications=0, consumers=0, cleanup=0,
            raw_sha256=sha(t['transfer_id'].encode())) for t in previous['transfers']}
        transfer.check_relay(previous, current, dict(native_rc=0), retired)
        for name in ('pending_grants', 'pending_publications', 'consumers', 'cleanup', 'terminal_count'):
            bad = copy.deepcopy(retired)
            bad['C0-a0'][name] += 1
            with self.subTest(responsibility=name), self.assertRaises(transfer.SetupError):
                transfer.check_relay(previous, current, dict(native_rc=0), bad)

    def test_fresh_custody_ack_binds_nonce_and_received_sealed_watermark(self):
        with tempfile.TemporaryDirectory(prefix='s10-custody-protocol-') as directory:
            keeper = transfer.FullstackKeeper(manifest_fixture(), directory)
            try:
                label, incarnation, nonce = 'broker', bytes(range(16)), bytes(range(16, 32))
                keeper.records.incarnations[label] = incarnation
                keeper.records.sequences[(label, incarnation)] = 3
                keeper.acks[label], keeper.challenges[label] = -1, nonce
                packet = b'TDA2' + nonce + struct.pack('<QQ', 3, 0)
                keeper.accept_packet(label, packet, [])
                self.assertEqual(keeper.acks[label], 1)
                self.assertEqual(keeper.barrier_sequences[(label, incarnation)], 3)
                for defect in ('replayed', 'wrong-nonce', 'unreceived-watermark', 'poison'):
                    keeper.acks[label], keeper.challenges[label] = -2, nonce
                    altered = (b'TDA2' + (bytes(16) if defect == 'wrong-nonce' else nonce) +
                               struct.pack('<QQ', 4 if defect in ('unreceived-watermark', 'poison') else 3,
                                           1 if defect == 'poison' else 0))
                    with self.subTest(defect=defect), self.assertRaises(transfer.SetupError):
                        keeper.accept_packet(label, altered, [])
                keeper.acks[label], keeper.challenges[label] = -2, bytes(reversed(nonce))
                keeper.records.sequences[(label, incarnation)] = 4
                keeper.accept_packet(label, b'TDA2' + bytes(reversed(nonce)) + struct.pack('<QQ', 4, 0), [])
                self.assertEqual(keeper.acks[label], 2)
            finally:
                keeper.close()


if __name__ == '__main__':
    unittest.main()
