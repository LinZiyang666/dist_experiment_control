#!/usr/bin/env python3
"""fullstack_transfer.py — S10 finite cohorts over the real S7 observer.

P19 starts this adapter through P05 and supplies product GC/attempt receipts.
The S7 keeper owns custody, continuous target reads, OFD witnesses and raw JS
scans. This module never starts a product, repairs state or substitutes an ID.
Missing upstream receipts remain INCOMPLETE; selftests sign only the protocol.
"""

import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import select
import struct
import sys
import time

# Isolated Python drops the script directory, including when P19 imports us.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import transfer_fixture as upstream
from transfer_keeper import Keeper
from transfer_observation import (BOOLEANS, IDENTITY, KINDS, NESTED, NUMBERS,
                                  TEXT, SetupError, receive, require, unique_object)
from transfer_oracle import TargetOracle, judge_object

SECOND = 1_000_000_000
COMPONENTS = ('agent', 'broker', 'nats', 'caddy')
ROUNDS = {'C0': {'leg1': ('R1',), 'leg2': ('R2', 'R3')},
          'C1': {'leg1': ('R4',), 'leg2': ('R5', 'R6', 'R7')}}
FIELDS = TEXT | NUMBERS | BOOLEANS | NESTED | {'schema'}
PHASES = frozenset('open prepared sending receiving commit_reserved decided '
                   'terminal_pending history_confirmed cleanup_pending settled'.split())

def strict_load(raw):
    return json.loads(raw, object_pairs_hook=unique_object,
                      parse_constant=lambda _: require(False, 'nonfinite_json'))

def exact(value, fields, code):
    require(type(value) is dict and set(value) == set(fields), code)
    return value

def digest(value):
    require(type(value) is str and re.fullmatch('[0-9a-f]{64}', value), 'invalid_digest')
    return value

def cohort_plan(cohort):
    """Freeze workload geometry before READY; actual starts are separate receipts."""
    require(cohort in ROUNDS, 'cohort_name')
    return [dict(runtime='a' + str(i), cohort=cohort, direction=direction, tier='B',
                 size=66549440 if small else 1 << 30,
                 rate_limit=65536 if small else 1048576,
                 budget_ns=(2092 if small else 2108) * SECOND)
            for i, (small, direction) in enumerate(((True, 'push'), (True, 'pull'),
                                                    (False, 'push'), (False, 'pull')))]

def validate_event(row):
    """Require every frozen S7 field, including zero and nested zero values."""
    # origin: hot-update S10 plan §5.1 / §10.2 P14 — no lossy projection.
    exact(row, FIELDS, 'full_transfer_schema')
    upstream.require(row['schema'] == 1 and type(row['schema']) is int, 'observation_schema')
    from transfer_observation import validate_event as validate_s7
    validate_s7(row)
    require(row['phase'] in PHASES and row['dropped'] == 0, 'observation_phase_or_drop')
    require(0 <= row['durable_offset'] <= row['fetched_offset'], 'durable_exceeds_fetched')
    require(row['primitive'] in ('', 'rename', 'link') and
            row['outcome'] in ('', 'not_entered', 'inflight', 'landed'), 'primitive_vocabulary')
    require(row['index'] < 1 << 32 and row['length'] < 1 << 32 and row['errno'] < 1 << 31 and
            all(row[k] < 1 << 63 for k in ('boottime_ns', 'offset', 'fetched_offset', 'durable_offset')),
            'observation_integer_width')
    for name in ('sha256', 'hash_state_digest', 'ofd_witness_digest', 'canonical_digest',
                 'retirement_digest', 'reader_sha'):
        if row[name]:
            digest(row[name])
    for name in ('tmp', 'parent', 'source'):
        ref = exact(row[name], ('resource_id', 'kind', 'name'), 'resource_fields')
        require(all(type(v) is str for v in ref.values()), 'resource_types')
        require(not any(ref.values()) or (ref['resource_id'] and ref['name'] == name and
                ref['kind'] == 'xfer-' + name + '-v1'), 'resource_binding')
    deadline = exact(row['original_deadline'], ('boot_id', 'deadline'), 'deadline_fields')
    require(type(deadline['deadline']) is int and deadline['deadline'] >= 0 and
            ((deadline['deadline'] == 0 and deadline['boot_id'] == '') or
             (deadline['deadline'] > 0 and deadline['boot_id'] == row['boot_id'])), 'deadline_clock')
    attempt = exact(row['attempt'], ('logical_id', 'ordinal', 'phase', 'started_mono_ns',
                    'finished_mono_ns', 'sent', 'class', 'backoff_ns'), 'attempt_fields')
    require(type(attempt['sent']) is bool and all(type(attempt[k]) is int and attempt[k] >= 0
            for k in ('ordinal', 'started_mono_ns', 'finished_mono_ns', 'backoff_ns')), 'attempt_types')
    return row

def validate_binding(transfer):
    identity = exact(transfer['identity'], ('sid', 'nid', 'actor_fp', 'agent_instance',
        'transfer_id', 'entry_nonce', 'intent_digest', 'object_version'), 'identity_fields')
    require(all(type(v) is str and v for v in identity.values()), 'identity_empty')
    require(all(identity[k] == transfer[k] for k in ('transfer_id', 'entry_nonce', 'object_version'))
            and identity['nid'] == transfer['runtime'], 'identity_binding')
    origin = exact(transfer['origin'], ('broker_id', 'instance_id', 'boot_id', 'generation'), 'origin_fields')
    require(all(type(origin[k]) is str and origin[k] for k in ('broker_id', 'instance_id', 'boot_id'))
            and type(origin['generation']) is int and origin['generation'] > 0, 'origin_binding')
    budget = exact(transfer['budget'], ('started_at', 'size', 'rate_limit_bps', 'effective_budget_ns',
                                      'remaining_ns', 'source_sealed'), 'budget_fields')
    require(type(budget['started_at']) is str and budget['started_at'] and budget['source_sealed'] is True,
            'source_not_sealed')
    require(budget['size'] == transfer['size'] and budget['rate_limit_bps'] == transfer['rate_limit'] and
            budget['effective_budget_ns'] == transfer['budget_ns'] and
            type(budget['remaining_ns']) is int and 0 < budget['remaining_ns'] <= transfer['budget_ns'],
            'original_budget')
    digest(identity['intent_digest'])
    digest(transfer['sha256'])

def validate_manifest(value):
    """S10 rates differ from drill63; never modify S7's admission to accept them."""
    require(value.get('schema') == 1 and value.get('mode') in ('live', 'hermetic'), 'manifest_schema')
    plan = cohort_plan(value['cohort'])
    transfers = value['transfers']
    require(len(transfers) == 4 and {t['runtime'] for t in transfers} == {'a0', 'a1', 'a2', 'a3'},
            'four_runtime_cohort')
    for key in ('transfer_id', 'entry_nonce', 'object_version', 'bucket', 'target'):
        require(len({t[key] for t in transfers}) == 4, 'shared_transfer_' + key)
    require(len({t['identity']['sid'] for t in transfers}) == 4, 'shared_session')
    require(2 <= value['quiet_seconds'] <= 30, 'quiet_window')
    labels = [p['label'] for p in value['processes']]
    require(labels and len(labels) == len(set(labels)), 'process_labels')
    agents = [p for p in value['processes'] if p['role'] == 'agent']
    require(len(agents) == 4 and all(len({p[key] for p in agents}) == 4
            for key in ('home', 'binary_path')), 'agent_install_isolation')
    for t in transfers:
        expected = next(p for p in plan if p['runtime'] == t['runtime'])
        require(all(t[k] == v for k, v in expected.items()), 'cohort_geometry')
        validate_binding(t)
        require(t['owners'] and set(t['owners']) <= set(labels) and len(set(t['owners'])) == len(t['owners']),
                'owner_manifest')
        require(type(t['required_resources']) is dict and t['required_resources'], 'ofd_manifest')
        # Product IDs are opaque. Their declared kinds, not a fixture naming
        # convention, bind source/tmp/parent to the appropriate retained phase.
        exact(t['resource_kinds'], t['required_resources'], 'resource_kind_fields')
        require(set(t['resource_kinds'].values()) == {'source', 'tmp', 'parent'}, 'ofd_closure')
        for name, kind in (('start_b64', 'start'), ('terminal_b64', 'complete')):
            rec = strict_load(base64.b64decode(t[name], validate=True))
            require(rec['transfer_id'] == t['transfer_id'] and rec['kind'] == kind, 'canonical_binding')
    cuts = value['cuts']
    expected = {(t['transfer_id'], leg, rnd, component) for t in transfers
                for leg, rounds in ROUNDS[value['cohort']].items() for rnd in rounds
                for component in (('nats', 'caddy') if rnd == 'R7' else COMPONENTS)}
    actual = {(c['transfer_id'], c['leg'], c['round'], c['component']) for c in cuts}
    require(actual == expected and len({(c['cut_id'], c['transfer_id'], c['producer']) for c in cuts}) == len(cuts),
            'cut_matrix')
    require(all(c['producer'] in labels and c['role'] in ('sender', 'receiver') for c in cuts), 'cut_role')
    for c in cuts:
        t = next(t for t in transfers if t['transfer_id'] == c['transfer_id'])
        owner = ('ctl' if (t['direction'] == 'push') == (c['role'] == 'sender') else 'agent') + t['runtime']
        # Labels are fixed by the four-runtime fixture, so a broker metadata
        # actor cannot claim the receiver's durable byte window.
        require(c['producer'] == owner, 'cut_data_owner')
    for component in COMPONENTS:
        require({c['role'] for c in cuts if c['component'] == component} == {'sender', 'receiver'},
                'component_leg_roles')
    # Source is retained in sender leg1; tmp/parent in receiver leg2. Data FDs
    # can retire when their phase ends, so do not require a closed source during
    # later courier/receiver cuts or a tmp before its actual creation.
    for t in transfers:
        required = {c['cut_id'] for c in cuts if c['transfer_id'] == t['transfer_id']}
        for resource, schedule in t['required_resources'].items():
            leg = 'leg1' if t['resource_kinds'][resource] == 'source' else 'leg2'
            live = {c['cut_id'] for c in cuts if c['transfer_id'] == t['transfer_id'] and c['leg'] == leg}
            require(live <= set(schedule) <= required, 'ofd_cut_schedule')
    return value

def read_manifest(path):
    raw = Path(path).read_bytes()
    value = validate_manifest(strict_load(raw))
    value['manifest_sha256'] = hashlib.sha256(raw).hexdigest()
    return value

class DestinationObserver(TargetOracle):
    """The same read-only atomic oracle covers both tiers and both receivers."""
    # origin: hot-update S10 plan §5.4 / P14 — inline is not exempt from atomicity.
    def __init__(self, spec):
        require(spec['tier'] in ('A', 'B') and spec['direction'] in ('push', 'pull') and
                spec['transfer_id'] and spec['entry_nonce'] and spec['size'] > 0, 'destination_binding')
        digest(spec['sha256'])
        self.binding = {k: spec[k] for k in ('transfer_id', 'entry_nonce', 'tier', 'direction')}
        super().__init__(spec['target'], spec.get('old'), [spec['size'], spec['sha256']])

    def sample(self, boundary='poll'):
        row = super().sample(boundary)
        row.update(self.binding)
        return row

def check_leg(transfer, rows, role):
    """Only JS ACK or real Sync advances bytes; preparation is a separate phase."""
    kinds = ('synced',) if role == 'receiver' else ('append', 'resolve')
    progress, last_seq = 0, 0
    accepted = []
    for row in rows:
        if row['kind'] not in kinds:
            continue
        if role == 'receiver':
            # AtomicReceiver and checkpoint owner emit separate Sync rows.
            # Join their real resource/checkpoint evidence, preserving both raw
            # records; a failed/pending Sync or mere fetch cannot count as bytes.
            if not row['file_synced'] or not row['hash_state_digest']:
                continue
            require(row['phase'] == 'receiving' and any(r['tmp']['resource_id'] for r in rows), 'receiver_not_sync')
            offset, seq = row['durable_offset'], row['last_consumed_stream_seq']
        else:
            # S7 append records the attempt BEFORE ACK; resolve carries the
            # readback ACK. A pending append remains visible but is not progress.
            if row['actual_ack_seq'] == 0:
                continue
            require(row['phase'] == 'sending' and any(r['source']['resource_id'] for r in rows), 'sender_not_ack')
            offset, seq = row['offset'] + row['length'], row['actual_ack_seq']
        require(0 < offset <= transfer['size'] and offset >= progress and seq > 0 and seq >= last_seq,
                'progress_reversed')
        progress, last_seq = offset, seq
        accepted.append((row, offset))
    return accepted

def check_status(original, current):
    """Bind full S7 status values; only remaining budget and progress may move."""
    fields = ('state', 'phase', 'identity', 'origin', 'budget', 'durable_offset', 'chunk_index',
              'last_consumed_stream_seq', 'terminal', 'cleanup_pending', 'code', 'error')
    exact(original, fields, 'original_status_fields')
    exact(current, fields, 'current_status_fields')
    budget_fields = ('started_at', 'size', 'rate_limit_bps', 'effective_budget_ns', 'remaining_ns', 'source_sealed')
    exact(original['budget'], budget_fields, 'original_budget_fields')
    exact(current['budget'], budget_fields, 'current_budget_fields')
    require(current['identity'] == original['identity'] and current['origin'] == original['origin'], 'status_binding')
    for name in ('started_at', 'size', 'rate_limit_bps', 'effective_budget_ns', 'source_sealed'):
        require(current['budget'][name] == original['budget'][name], 'status_budget_reset')
    require(0 <= current['budget']['remaining_ns'] <= original['budget']['remaining_ns'], 'remaining_budget_reset')
    require(all(current[k] >= original[k] for k in ('durable_offset', 'chunk_index', 'last_consumed_stream_seq')),
            'status_progress_reversed')
    require(current['state'] in ('live', 'terminal') and current['phase'] in PHASES, 'status_unknown')
    if current['state'] == 'terminal':
        receipt = current['terminal']
        require(type(receipt) is dict, 'terminal_fields')
        exact({k: v for k, v in receipt.items() if k != 'outcome_unknown'}, ('terminal_id', 'kind',
            'canonical_digest', 'req_id', 'history_stream', 'history_seq', 'bytes', 'sha256', 'code'), 'terminal_fields')
        require(type(receipt) is dict and receipt['kind'] == 'complete' and receipt['history_seq'] > 0 and
                receipt['history_stream'] and receipt['terminal_id'] and receipt['bytes'] == current['budget']['size']
                and not receipt.get('outcome_unknown', False), 'status_terminal_not_confirmed')
        digest(receipt['canonical_digest'])


def check_closing_status(original, current, transfer, rows):
    # origin: S10 internal review rv-honesty F5
    # Live status is useful during cuts but cannot retire a finite cohort.
    check_status(original, current)
    require(current['state'] == 'terminal' and current['phase'] == 'settled' and
            current['cleanup_pending'] is False and not current['code'] and not current['error'],
            'status_not_settled')
    check_history(rows, transfer)
    terminal = [r for r in rows if r.get('subject') == transfer['history_subject'] and
                strict_load(base64.b64decode(r['data_b64'], validate=True)).get('transfer_id') ==
                transfer['transfer_id']][-1]
    body = base64.b64decode(terminal['data_b64'], validate=True)
    record, receipt = strict_load(body), current['terminal']
    terminal_id = hashlib.sha256(('xfer-terminal-v1\n' + transfer['identity']['sid'] + '\n' +
        transfer['transfer_id'] + '\n' + transfer['entry_nonce']).encode()).hexdigest()
    expected = dict(terminal_id=terminal_id, kind='complete', history_stream=transfer['history_stream'],
        history_seq=terminal['sequence'], canonical_digest=hashlib.sha256(body).hexdigest(),
        req_id=hashlib.sha256(b'xferaudit-record:' + body).hexdigest(), bytes=transfer['size'],
        sha256=transfer['sha256'], code='')
    require(all(receipt[k] == v for k, v in expected.items()), 'terminal_history_mismatch')
    require(all(record.get(k, '') == expected[k] for k in ('terminal_id', 'kind', 'bytes', 'sha256', 'code'))
            and not record.get('error') and not record.get('outcome_unknown', False), 'terminal_outcome_mismatch')
    names = dict(sid='session', nid='node')
    require(all(record.get(names.get(k, k)) == v for k, v in transfer['identity'].items()),
            'terminal_identity_mismatch')
    require(current['durable_offset'] == transfer['size'], 'terminal_progress_mismatch')

def check_closing_owners(manifest, events, closing):
    # origin: S10 internal review rv-honesty F5
    # Individually plausible receipts must describe the same retired owners.
    transfers = {t['transfer_id']: t for t in manifest['transfers']}
    for tid, t in transfers.items():
        require(any(a['publication']['identity']['transfer_id'] == tid and a['disposition'] == 'acknowledged'
                    for a in closing['attempts']), 'terminal_attempt_missing')
        retired = [r for r in events if r['transfer_id'] == tid and r['kind'] == 'retirement']
        require(len(retired) == len(t['owners']) and {r['producer'] for r in retired} == set(t['owners']) and
                all(r['phase'] == 'settled' and r['admission_closed'] for r in retired), 'retirement_unsettled')
        gc = closing['gc'][tid]
        require(any(r['producer'] == t['origin']['broker_id'] and
                    r['generation'] == gc['owner_generation'] and r['retirement_digest'] == gc['retirement_digest']
                    for r in retired), 'gc_retirement_mismatch')

def coverage(manifest, events):
    missing = []
    for cut in manifest['cuts']:
        t = next(t for t in manifest['transfers'] if t['transfer_id'] == cut['transfer_id'])
        rows = [r for r in events if all(r.get(k) == cut[k] for k in ('transfer_id', 'leg', 'producer'))]
        markers = [r for r in rows if r['kind'] in ('install', 'rollback') and r['cut_id'] == cut['cut_id']]
        points = check_leg(t, rows, cut['role'])
        if len(markers) != 1 or not points:
            missing.append('leg:' + cut['cut_id'] + ':' + t['transfer_id'])
            continue
        marker, first = markers[0], points[0][0]
        # Every comparison uses the producer's own BOOTTIME; infra wall clocks
        # and product generations cannot substitute for this local cut marker.
        same = [p for p in points if all(p[0][k] == marker[k] for k in ('pid', 'process_start', 'boot_id'))]
        before = [p for p in same if p[0]['boottime_ns'] < marker['boottime_ns'] and 0 < p[1] < t['size']]
        after = [p for p in same if p[0]['boottime_ns'] > marker['boottime_ns']]
        limit = (900 if cut['leg'] == 'leg1' else 840) * SECOND
        if not before or not after or max(p[1] for p in after) <= max(p[1] for p in before):
            missing.append('leg:' + cut['cut_id'] + ':' + t['transfer_id'])
        elif not 0 <= marker['boottime_ns'] - first['boottime_ns'] <= limit:
            missing.append('cut_window:' + cut['cut_id'] + ':' + t['transfer_id'])
        require(marker['object_version'] == t['object_version'], 'cut_object_replaced')
    return missing

def check_starts(manifest, starts):
    exact(starts, (t['transfer_id'] for t in manifest['transfers']), 'start_population')
    for s in starts.values():
        exact(s, ('scheduler_start_ns', 'leg2_elapsed_ns', 'last_cut_elapsed_ns', 'settled_elapsed_ns'), 'start_fields')
        require(all(type(v) is int and v > 0 for v in s.values()), 'start_clock_values')
    # elapsed_ns is measured locally from each original start; cross-host
    # starts are compared by the scheduler's launch receipt, not BOOTTIME.
    require(max(s['scheduler_start_ns'] for s in starts.values()) -
            min(s['scheduler_start_ns'] for s in starts.values()) <= 5 * SECOND, 'start_skew')
    for t in manifest['transfers']:
        s = starts[t['transfer_id']]
        require(0 < s['leg2_elapsed_ns'] <= 1050 * SECOND and 0 < s['last_cut_elapsed_ns'] <= 1890 * SECOND,
                'cohort_absolute_window')
        require(0 < s['settled_elapsed_ns'] <= t['budget_ns'], 'transfer_deadline')

def check_history(rows, transfer):
    matching = []
    for row in rows:
        if row.get('subject') != transfer['history_subject']:
            continue
        # Decode the ENTIRE subject: other calls may share it. An undecodable
        # row is missing evidence, never something to drop from the population.
        record = strict_load(base64.b64decode(row['data_b64'], validate=True))
        if record.get('transfer_id') == transfer['transfer_id']:
            matching.append(row)
    require(len(matching) == 2 and [r['data_b64'] for r in matching] ==
            [transfer['start_b64'], transfer['terminal_b64']], 'canonical_history')
    require(matching[0]['sequence'] < matching[1]['sequence'], 'history_order')

def check_js(before, after):
    """Compare immutable storage fields; server/consumer/replica changes are legal."""
    for name in ('store_identity', 'stream', 'created'):
        require(before[name] == after[name] and bool(before[name]), 'js_incarnation')
    require(after['cursor'] >= before['cursor'], 'js_cursor_reversed')
    retired = set(after['retired_consumers'])
    require(set(before['consumers']) - set(after['consumers']) <= retired, 'consumer_unretired')

def check_gc(receipt, transfer):
    exact(receipt, ('transfer_id', 'entry_nonce', 'object_version', 'default_pass', 'live_preserved',
        'orphan_removed', 'writer_closed', 'cleanup_complete', 'owner_generation', 'retirement_digest',
        'raw_sha256'), 'gc_fields')
    require(all(receipt[k] == transfer[k] for k in ('transfer_id', 'entry_nonce', 'object_version')), 'gc_binding')
    digest(receipt['raw_sha256'])
    digest(receipt['retirement_digest'])
    require(type(receipt['owner_generation']) is int and receipt['owner_generation'] > 0 and
            all(receipt[k] is True for k in ('default_pass', 'live_preserved', 'orphan_removed',
                                           'writer_closed', 'cleanup_complete')), 'default_gc_pending')

def check_attempts(attempts):
    keys = set()
    for row in attempts:
        exact(row, ('owner', 'logical_id', 'ordinal', 'sent', 'disposition', 'sent_ns', 'accepted_ns',
                    'closed_ns', 'late', 'raw_sha256', 'publication', 'event'), 'publication_attempt_fields')
        key = row['owner'], row['logical_id'], row['ordinal']
        require(key not in keys and row['owner'] and row['logical_id'] and row['ordinal'] > 0, 'attempt_duplicate')
        keys.add(key)
        digest(row['raw_sha256'])
        require(row['sent'] is True and row['disposition'] in ('acknowledged', 'fence_rejected') and
                0 < row['sent_ns'] <= row['closed_ns'], 'sent_attempt_pending')
        if row['late']:
            # Real default dedupe expiry is additional evidence, never 2s quiet.
            require(row['accepted_ns'] >= row['sent_ns'] and row['closed_ns'] - row['accepted_ns'] >=
                    125 * SECOND and row['disposition'] == 'fence_rejected', 'default_window_not_crossed')

def check_publications(manifest, events, attempts):
    # origin: S10 internal review rv-harness F3
    # The sealed sent ledger supplies the population, never a caller's boolean.
    # Empty canonical data on a terminal attempt is missing proof, not exclusion.
    sent = {i + 1: e for i, e in enumerate(events) if e['kind'] == 'attempt' and e['attempt']['sent'] and
            (e['canonical_digest'] or e['phase'] in ('terminal_pending', 'history_confirmed', 'cleanup_pending', 'settled'))}
    require(sent and len(sent) == len(attempts), 'publication_population_mismatch')
    closed_cursor = max(sent)
    check_attempts(attempts)
    for row in attempts:
        refs = ('producer', 'pid', 'process_start', 'boot_id', 'generation', 'event_seq', 'cut_id')
        matches = [i for i, e in sent.items() if row['event'] == {k: e[k] for k in refs}]
        require(len(matches) == 1, 'publication_event_mismatch')
        e = sent.pop(matches[0])
        t = next(t for t in manifest['transfers'] if t['transfer_id'] == e['transfer_id'])
        body = base64.b64decode(t['terminal_b64'], validate=True)
        terminal_id = hashlib.sha256(('xfer-terminal-v1\n' + t['identity']['sid'] + '\n' +
            t['transfer_id'] + '\n' + t['entry_nonce']).encode()).hexdigest()
        binding = dict(identity=t['identity'], origin=t['origin'], terminal_id=terminal_id,
            req_id=hashlib.sha256(b'xferaudit-record:' + body).hexdigest(),
            canonical_digest=hashlib.sha256(body).hexdigest())
        require(row['publication'] == binding, 'publication_binding_mismatch')
        require(row['owner'] == e['producer'] and row['owner'] in t['owners'] and
                all(e[k] == t[k] for k in ('entry_nonce', 'object_version')) and
                e['canonical_digest'] == binding['canonical_digest'], 'publication_owner_mismatch')
        require(row['logical_id'] == e['attempt']['logical_id'] and
                row['logical_id'] in (binding['req_id'], terminal_id) and
                row['ordinal'] == e['attempt']['ordinal'], 'publication_attempt_mismatch')
        require(row['sent_ns'] == e['attempt']['started_mono_ns'] and
                row['closed_ns'] == e['attempt']['finished_mono_ns'] <= e['boottime_ns'], 'publication_closure_clock')
        require(row['raw_sha256'] == hashlib.sha256(json.dumps(e, sort_keys=True,
                separators=(',', ':')).encode()).hexdigest(), 'publication_raw_mismatch')
        # Owner-local sequence/clock and collector cursor establish causality;
        # comparing a remote BOOTTIME with the collector's monotonic time cannot.
        retired = [i + 1 for i, r in enumerate(events) if r['kind'] == 'retirement' and
            all(r[k] == e[k] for k in ('producer', 'transfer_id', 'entry_nonce', 'object_version',
                                      'pid', 'process_start', 'boot_id', 'generation')) and
            r['admission_closed'] and r['phase'] == 'settled' and r['event_seq'] > e['event_seq'] and
            r['boottime_ns'] >= e['boottime_ns'] and i + 1 > matches[0]]
        require(retired, 'publication_not_retired')
        closed_cursor = max(closed_cursor, max(retired))
    return closed_cursor

def check_seals(seals, owners):
    require(len(seals) == 2, 'two_fresh_seals')
    first, second = seals
    for seal in seals:
        require(set(seal['acks']) == set(owners) and seal['history_verified'] is True and
                seal['default_window_ns'] == 120 * SECOND, 'seal_incomplete')
        digest(seal['raw_sha256'])
    require(first['nonce'] and second['nonce'] and first['nonce'] != second['nonce'] and
            second['at_ns'] > first['at_ns'] and second['cursor'] >= first['cursor'] and
            all(second['acks'][p] > first['acks'][p] > 0 for p in owners), 'seal_replayed')

def check_relay(previous, current, result, retirement):
    """C1 admission consumes C0's exact settle proof, never a replacement retry."""
    require(previous['cohort'] == 'C0' and current['cohort'] == 'C1' and result['native_rc'] == 0,
            'relay_before_settle')
    require({t['transfer_id'] for t in previous['transfers']}.isdisjoint(
            {t['transfer_id'] for t in current['transfers']}), 'relay_reuses_id')
    exact(retirement, (t['transfer_id'] for t in previous['transfers']), 'relay_retirement_population')
    for old in previous['transfers']:
        new = next(t for t in current['transfers'] if t['runtime'] == old['runtime'])
        require(new['bucket'] == old['bucket'] and new['identity']['sid'] == old['identity']['sid'], 'relay_domain')
        receipt = retirement[old['transfer_id']]
        require(receipt['entry_nonce'] == old['entry_nonce'] and receipt['start_count'] == 1 and
                receipt['terminal_count'] == 1 and receipt['terminal_b64'] == old['terminal_b64'] and
                all(receipt[k] == 0 for k in ('pending_grants', 'pending_publications', 'consumers', 'cleanup')),
                'relay_pending_responsibility')
        digest(receipt['raw_sha256'])

def judge_evidence(manifest, snapshot, raw_rows, closing):
    """Return native codes; no synthetic receipt can fill a missing collection."""
    failures, missing, setup = list(snapshot.get('failures', [])), [], list(snapshot.get('setup', []))
    try:
        validate_manifest(manifest)
        for row in snapshot['events']:
            validate_event({k: v for k, v in row.items() if k != 'producer'})
            t = next(t for t in manifest['transfers'] if t['transfer_id'] == row['transfer_id'])
            require(row['entry_nonce'] == t['entry_nonce'] and row['object_version'] == t['object_version'],
                    'original_transfer_binding')
            for name in ('source', 'tmp', 'parent'):
                resource = row[name]['resource_id']
                require(not resource or t['resource_kinds'].get(resource) == name, 'original_resource_binding')
        missing += coverage(manifest, snapshot['events'])
        for t in manifest['transfers']:
            samples = snapshot['targets'][t['transfer_id']]
            require(samples and all(s['state'] in ('old', 'new', 'absent') for s in samples) and
                    samples[-1]['state'] == 'new', 'atomic_destination')
            check_history(raw_rows[t['history_stream']], t)
            require(judge_object(raw_rows[t['object_stream']], t, snapshot['events']), 'physical_object')
            for resource, cuts in t['required_resources'].items():
                checks = snapshot['resources']['ofd_checks'].get(resource, {}).get('checks', [])
                require(set(cuts) <= {c['cut_id'] for c in checks} and checks and
                        all(c['equal'] is True for c in checks), 'original_ofd')
            participants = {r['producer'] for r in snapshot['events'] if r['transfer_id'] == t['transfer_id']}
            require(participants == set(t['owners']), 'owner_population')
            for owner in t['owners']:
                own = [r for r in snapshot['events'] if r['transfer_id'] == t['transfer_id'] and r['producer'] == owner]
                require(any(r['kind'] == 'retirement' and r['admission_closed'] and r['retirement_digest']
                            for r in own), 'owner_not_retired')
            require(upstream.Fixture.owners_joined(None, snapshot['events'], t), 'owner_not_joined')
            primitive = [r for r in snapshot['events'] if r['transfer_id'] == t['transfer_id'] and
                         r['kind'] == 'primitive_exit' and r['outcome'] == 'landed']
            require(len(primitive) == 1 and any(r['kind'] == 'directory_sync' and r['parent_synced'] and
                    r['boottime_ns'] > primitive[0]['boottime_ns'] and r['producer'] == primitive[0]['producer']
                    for r in snapshot['events'] if r['transfer_id'] == t['transfer_id']), 'once_primitive_sync')
        if closing is None:
            missing.append('pending S7 default GC, publication attempts and two fresh history seals')
        else:
            check_starts(manifest, closing['starts'])
            require(set(closing['gc']) == {t['transfer_id'] for t in manifest['transfers']}, 'gc_population')
            for t in manifest['transfers']:
                check_gc(closing['gc'][t['transfer_id']], t)
            require(closing['attempts'] and closing['attempt_population_complete'] is True, 'attempt_population')
            closed_cursor = check_publications(manifest, snapshot['events'], closing['attempts'])
            require(all(s['cursor'] >= closed_cursor for s in closing['seals']), 'publication_after_seal')
            check_closing_owners(manifest, snapshot['events'], closing)
            check_seals(closing['seals'], [p['label'] for p in manifest['processes']])
            for name in ('js', 'status'):
                if name not in closing:
                    missing.append('pending S7 ' + name + ' original/current receipts')
            if 'js' in closing:
                require(set(closing['js']) == set(raw_rows), 'js_population')
                for pair in closing['js'].values():
                    check_js(pair['before'], pair['after'])
            if 'status' in closing:
                require(set(closing['status']) == {t['transfer_id'] for t in manifest['transfers']}, 'status_population')
                for t in manifest['transfers']:
                    pair = closing['status'][t['transfer_id']]
                    check_closing_status(pair['before'], pair['after'], t, raw_rows[t['history_stream']])
                    require(pair['before']['identity'] == t['identity'] and pair['before']['origin'] == t['origin'] and
                            pair['before']['budget'] == t['budget'], 'status_original_binding')
    except SetupError as error:
        code = str(error)
        # Broken collection is SETUP2. It cannot count as a continuity mutant
        # killed at the intended assertion window merely because it is nonzero.
        if code.endswith(('_fields', '_types', '_width')) or code in (
                'full_transfer_schema', 'observation_schema', 'observation_extra_field',
                'observation_number', 'observation_boolean', 'observation_nested_value'):
            setup.append(code)
        else:
            failures.append(code)
    except (KeyError, TypeError, ValueError, StopIteration):
        setup.append('transfer_evidence_malformed')
    return dict(native_rc=1 if failures else 2 if setup else 4 if missing else 0,
                failures=failures, missing=missing, setup=setup)

class FullstackKeeper(Keeper):
    def __init__(self, manifest, evidence):
        super().__init__(manifest, evidence)
        self.targets = {t['transfer_id']: DestinationObserver(t) for t in manifest['transfers']}
        self.challenges, self.barrier_sequences = {}, {}

    def add_event(self, source, row):
        validate_event({k: v for k, v in row.items() if k != 'producer'})
        super().add_event(source, row)
        # Sender writes and receiver durability/primitive boundaries all trigger
        # independent target reads, alongside the inherited continuous sampler.
        if row['kind'] in ('append', 'resolve', 'finalize', 'fetched', 'synced',
                           'primitive_enter', 'primitive_exit', 'directory_sync'):
            boundary = {k: row[k] for k in ('kind', 'event_seq', 'generation', 'cut_id', 'boottime_ns')}
            boundary['producer'] = source
            sample = self.targets[row['transfer_id']].sample(boundary)
            if sample['state'] == 'partial':
                self.poison('partial_destination:' + row['transfer_id'], assertion=True)

    def accept_packet(self, label, raw, fds):
        if raw == b'TDC1' and len(fds) == 1:
            rows = self.records.add(fds.pop(), label, self.identities[label])
            for row in rows:
                self.add_event(label, row)
            return
        for fd in fds:
            os.close(fd)
        require(not fds and len(raw) == 36 and raw[:4] == b'TDA2', 'fresh_drain_packet')
        challenge = self.challenges.get(label)
        require(challenge is not None and raw[4:20] == challenge and self.acks[label] < 0, 'fresh_drain_nonce')
        sequence, poison = struct.unpack('<QQ', raw[20:])
        incarnation = self.records.incarnations.get(label)
        key = (label, incarnation)
        require(poison == 0 and incarnation is not None and sequence > 0 and
                sequence == self.records.sequences.get(key) and sequence > self.barrier_sequences.get(key, 0),
                'fresh_drain_watermark')
        self.barrier_sequences[key] = sequence
        self.acks[label] = -self.acks[label]
        del self.challenges[label]

    def collect(self):
        # The old S7 Python keeper speaks TDP1. S10 uses the landed product's
        # nonce/sequence-bound TDP2 without changing that concurrently owned file.
        # origin: hot-update S10 plan §5.4 / P14; S7 plan §10.4 / T41.
        while not self.stop_event.is_set():
            with self.lock:
                channels = dict(self.channels)
            try:
                ready, _, _ = select.select(list(channels.values()), [], [], .02)
                with self.lock:
                    for label, channel in channels.items():
                        if channel not in ready:
                            continue
                        raw, fds = receive(channel)
                        if not raw:
                            for fd in fds:
                                os.close(fd)
                            self.poison('custody_closed')
                            self.channels.pop(label).close()
                        else:
                            self.accept_packet(label, raw, fds)
                    self.inspect_targets('poll')
                    self.peak_fds = max(self.peak_fds, len(os.listdir('/proc/self/fd')))
            except (SetupError, OSError) as error:
                self.poison(str(error) if isinstance(error, SetupError) else 'collector_io')
            with self.changed:
                self.changed.notify_all()

    def drain(self, timeout=5):
        challenge = os.urandom(16)
        with self.lock:
            require(set(self.channels) == {p['label'] for p in self.manifest['processes']}, 'missing_custody')
            require(not self.setup_failures and not self.records.failure, 'custody_poisoned')
            requested = {}
            for label, channel in self.channels.items():
                require(self.acks[label] >= 0 and label not in self.challenges, 'drain_inflight')
                requested[label] = self.acks[label] + 1
                self.acks[label], self.challenges[label] = -requested[label], challenge
                channel.send(b'TDP2' + challenge)
        deadline = time.monotonic() + timeout
        with self.changed:
            while True:
                with self.lock:
                    require(not self.setup_failures, 'custody_poisoned')
                    if all(self.acks[k] == v for k, v in requested.items()):
                        return dict(acks=requested, nonce=challenge.hex(), cursor=len(self.records.events),
                            at_ns=time.monotonic_ns(), watermarks={p: dict(
                                incarnation=self.records.incarnations[p].hex(),
                                sequence=self.barrier_sequences[(p, self.records.incarnations[p])]) for p in requested})
                remaining = deadline - time.monotonic()
                require(remaining > 0, 'drain_ack_missing')
                self.changed.wait(remaining)

class TransferFixture(upstream.Fixture):
    """P19 owns Start/Close/Join and supplies closing receipts from real owners."""
    def __init__(self, manifest, evidence):
        validate_manifest(manifest)
        super().__init__(manifest, evidence)
        self.keeper = FullstackKeeper(manifest, evidence)
        self.history_windows = {}

    def scan_locked(self):
        # The S7 implementation assumes an exclusive history subject. S10 has
        # multiple calls on each permanent subject, so scan without that judge;
        # the full matching-transfer population is checked at each S10 seal.
        spec = self.manifest['nats']
        auth = strict_load(os.pread(spec['credentials_fd'], 65536, 0)) if spec.get('credentials_fd') else {}
        with upstream.NATSReader(spec['url'], auth, spec.get('ca_file')) as reader:
            with self.io_lock:
                self.active_reader = reader
            try:
                receipt = {name: scan.scan(reader) for name, scan in sorted(self.scans.items())}
                for name in {t['history_stream'] for t in self.manifest['transfers']}:
                    info = reader.request('$JS.API.STREAM.INFO.' + name, {})
                    require('error' not in info and info['config']['duplicate_window'] == 120 * SECOND,
                            'history_default_window')
                    self.history_windows[name] = info['config']['duplicate_window']
            finally:
                with self.io_lock:
                    self.active_reader = None
        self.scan_receipts.append(dict(at_ns=time.monotonic_ns(), streams=receipt))
        for name, scan in self.scans.items():
            self.keeper.save('raw-' + name + '.json', scan.rows)
        # Observation continues after a successful judge; a late opposite or
        # duplicate terminal poisons the saved verdict instead of being hidden
        # by the two already successful ending scans.
        for t in self.manifest['transfers']:
            matching = [r for r in self.scans[t['history_stream']].rows
                        if r.get('subject') == t['history_subject'] and
                        strict_load(base64.b64decode(r['data_b64'], validate=True)).get('transfer_id') == t['transfer_id']]
            if len(matching) >= 2:
                try:
                    check_history(matching, t)
                except SetupError:
                    self.keeper.poison('canonical_history', assertion=True)
        return receipt

    def judge(self, closing=None):
        # GC/attempt owners must close BEFORE either barrier. A caller cannot
        # hand us already captured seals: both drains and retained-row readbacks
        # are performed now while the original custody/target sampler stays live.
        if closing is not None:
            require(closing['attempt_population_complete'] is True and closing['attempts'], 'attempt_population')
            for t in self.manifest['transfers']:
                check_gc(closing['gc'][t['transfer_id']], t)
            before = self.keeper.sample()
            check_publications(self.manifest, before['events'], closing['attempts'])
            check_closing_owners(self.manifest, before['events'], closing)
            for t in self.manifest['transfers']:
                require(upstream.Fixture.owners_joined(self, before['events'], t), 'owner_not_joined')
            closing = copy.deepcopy(closing)
            closing['seals'] = []
            for barrier in range(2):
                if barrier:
                    # Supplementary quiet observation keeps both background
                    # readers alive. Owner/attempt closure above and both fresh
                    # history readbacks remain the actual sealing requirements.
                    require(not self.keeper.stop_event.wait(self.manifest['quiet_seconds']), 'seal_observer_stopped')
                receipt = self.drain()
                require(self.history_windows and set(self.history_windows.values()) == {120 * SECOND},
                        'history_default_window_missing')
                encoded = json.dumps(receipt, sort_keys=True).encode()
                closing['seals'].append(dict(receipt,
                    history_verified=True, default_window_ns=next(iter(self.history_windows.values())),
                    raw_sha256=hashlib.sha256(encoded).hexdigest()))
        snapshot = self.snapshot()
        result = judge_evidence(self.manifest, snapshot, {name: s.rows for name, s in self.scans.items()}, closing)
        self.keeper.judged, self.keeper.verdict = True, result
        self.keeper.save('verdict.json', result)
        return result
