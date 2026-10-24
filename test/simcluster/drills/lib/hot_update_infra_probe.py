#!/usr/bin/env -S python3 -I -B
"""Independent S9 mixed-cohort oracle; judge --manifest FILE --trace FILE.

infra-fixture-v1 fixes originals, NC participants, the whole hop schedule and
S7's accepted executable/source/schema handoff before LIVE. infra-trace-v1 is
an append-only observer ledger, never a projection of product Status. Times
are observer-local monotonic nanoseconds; remote producers must map their
clocks before delivery. Missing live cells are INCOMPLETE4, observed defects
ASSERT-FAIL1 and invalid custody SETUP-RED2. No historical-loss rc3 is inferred.

PRODUCERS is the field-level producer -> reader handoff for P35. Kernel readers
supply process/socket census, S7 supplies sealed transfer observations, targets
supply byte/effect ledgers and the scheduler supplies every intended slot.
origin: hot-update S9 plan §2.3 / §9.1 / §12 P32 / T76 / T77
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import sys

# Isolated Python deliberately excludes the script directory. Import only the
# repository siblings, independent of PYTHONPATH or the caller's cwd.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from transfer_observation import validate_event

SCHEMA = 'infra-fixture-v1'
TRACE_SCHEMA = 'infra-trace-v1'
KINDS = ('node_ls', 'ps', 'read', 'write', 'short_exec')
ORIGINALS = ('ssh', 'tcp-active', 'tcp-idle', 'tcp-half-close', 'tcp-reverse',
             'ss-alice', 'ss-bob', 'pty', 'exec')
COHORTS = ('small-push', 'small-pull', 'large-push', 'large-pull')
PHASES = ('before', 'moving', 'cutting', 'verifying', 'returning', 'retired')
NO_ROLL_PROFILES = ('lifetime-store-lock', 'exact-release-join', 'responsibility-permission-gates',
                    'invalid-config-cert', 'legacy-capability')
TLS_LANES = ('external4', 'external6', 'output4', 'output6', 'hairpin4', 'hairpin6')
PROCESS_FIELDS = ('BootID', 'PID', 'StartTicks', 'InvocationID', 'ExeDevice',
                  'ExeInode', 'ExeSHA256', 'Cgroup')
SOCKET_FIELDS = ('NetNS', 'Device', 'Inode', 'Cookie', 'Local', 'Remote', 'Owner')
ATTACHMENT_FIELDS = ('Participant', 'Generation', 'ServerID', 'CID', 'URL',
                     'Raw', 'Ingress', 'HealthGeneration')
ATTEMPT_FIELDS = ('logical_id', 'ordinal', 'phase', 'started_mono_ns',
                  'finished_mono_ns', 'sent', 'class', 'backoff_ns')
ATTEMPT_PHASES = frozenset('dns proxy tls ws connect inbox_probe inbox_fallback discovery banner probe create request backoff cancel result freeze exec import auth ready maintenance'.split())
ATTEMPT_CLASSES = frozenset('ok retryable refused timeout no_responders connection auth denied unsupported canceled skipped terminal result_unknown'.split())
PRODUCERS = {
    'scheduler': ('ordinal', 'logical_id', 'kind', 'scheduled_ns', 'dispatched_ns',
                  'terminal_ns', 'native_rc', 'attempts', 'complete', 'tail_complete', 'progress'),
    'kernel-reader/P02': PROCESS_FIELDS + SOCKET_FIELDS + ATTACHMENT_FIELDS +
        ('complete', 'generation', 'processes', 'old_owner_pending', 'producer'),
    'original-target-reader': ('identity', 'accepts', 'redials', 'tx', 'rx',
        'tx_sha256', 'rx_sha256', 'fin', 'native_rc', 'carrier', 'logical_id', 'effect'),
    'S7-custody': ('events', 'processes', 'transfer_id', 'entry_nonce', 'object_version',
        'durable_offset', 'offset', 'length', 'consumer_identity', 'kind', 'boottime_ns'),
    'pre-repair-JS-reader': ('transfer_id', 'at_ns', 'before_repair_ns', 'expected', 'actual'),
    'fresh-TLS-reader': ('ordinal', 'lane', 'scheduled_ns', 'started_ns', 'terminal_ns',
        'success', 'attempts', 'accepted', 'bypass', 'local', 'remote', 'translation'),
    'kernel-conntrack-reader': ('producer', 'generation', 'netns', 'original_local',
        'original_remote', 'translated_local', 'translated_remote'),
    'control-target-reader': ('op', 'subject', 'at_ns', 'effect', 'producer'),
    'S7-final-reader': ('native_rc', 'manifest_sha256', 'reader_digest', 'oracle_input_sha'),
    'S7-phase-custody': ('lane', 'hop', 'events', 'sent', 'effect', 'reply_cut'),
}
MAX_BYTES = 128 << 20
# Exact field paths for the private observer inventory. The nested Process,
# Socket, Attachment and Attempt rows use the frozen contracts above. S7 owns
# the inner custody schema; this adapter never adds fields to transfer-v1.
SCHEMA_FIELDS = {
    'manifest': 'schema mode nonce seconds profile carrier_transition originals attachments transfers hops tls_lanes s7_handoff s7_manifest_sha256 s7_reader_digest phase_lanes deployment',
    'trace': 'schema nonce manifest_sha256 t0_ns producer complete tail_complete slots effects samples progress controls tls flow_native_rc s7_final phase_lanes final_business',
    'progress': 'at_ns producer business s7',
    'sample': 'hop phase at_ns producer business s7 prefix_reads attachments accepted translations before current temporary census return_verified root_revision transaction_revision started_ns finished_ns main_ready config_before config_after',
    'original': 'identity carrier',
    'socket_original': 'application_nonce socket',
    'task_original': 'application_nonce ctl child task_id',
    'business': 'identity accepts redials tx rx tx_sha256 rx_sha256 fin carrier revoked native_rc',
    'slot': 'ordinal logical_id kind scheduled_ns dispatched_ns terminal_ns native_rc attempts validated',
    'effect': 'logical_id effect',
    'prefix': 'transfer_id at_ns before_repair_ns expected actual',
    'chunk': 'index length sha256',
    'census': 'producer complete generation processes live_business old_owner_pending',
    'tls': 'ordinal lane scheduled_ns started_ns terminal_ns success attempts accepted bypass main local remote translation',
    'translation': 'producer generation netns original_local original_remote translated_local translated_remote',
    'control': 'op subject at_ns effect producer',
    'phase_lane': 'lane hop events sent effect reply_cut',
    'hop': 'id host component action target_sha256 participants',
    'transfer': 'cohort transfer_id entry_nonce session_id bucket object_version ctl_producer ctl started_ns size rate_limit',
    'handoff': 'accepted_by schema files',
    'file': 'path sha256',
    'deployment': 'commands readers tls s7 flow',
    'command': 'argv sha256 mode expected_output inputs',
    'tls_config': 'address server_name ca_file reader port source_address',
    's7_binding': 'manifest evidence',
    'flow_binding': 'evidence nonce',
}
# P35 consumes every (schema, field, producer, reader) row from these tables.
# Containers have a producer too; overrides name the source of nested receipts.
# Frozen Process/Socket/Attachment/Attempt fields and transfer-v1 remain owned
# by their original schemas rather than silently copied into a second ABI.
SCHEMA_PRODUCERS = {
    'manifest': 'deployment-caller', 'trace': 'scheduler', 'progress': 'original-target-reader',
    'sample': 'kernel-and-target-reader', 'original': 'original-target-reader',
    'socket_original': 'original-target-reader', 'task_original': 'original-target-reader',
    'business': 'original-target-reader', 'slot': 'scheduler', 'effect': 'original-target-reader',
    'prefix': 'pre-repair-JS-reader', 'chunk': 'pre-repair-JS-reader', 'census': 'kernel-reader/P02',
    'tls': 'fresh-TLS-reader', 'translation': 'kernel-conntrack-reader', 'control': 'control-target-reader',
    'phase_lane': 'S7-phase-custody', 'hop': 'deployment-caller', 'transfer': 'S7-custody',
    'handoff': 'lead-accepted-S7-handoff', 'file': 'deployment-caller', 'deployment': 'deployment-caller',
    'command': 'deployment-caller', 'tls_config': 'deployment-caller', 's7_binding': 'deployment-caller',
    'flow_binding': 'deployment-caller',
}
FIELD_PRODUCERS = {
    'trace.s7_final': 'S7-final-reader', 'trace.flow_native_rc': 'S5-flow-oracle',
    'sample.s7': 'S7-custody', 'progress.s7': 'S7-custody', 'slot.attempts': 'controlbudget-attempt-sink',
    'tls.accepted': 'kernel-reader/P02', 'tls.main': 'kernel-reader/P02',
    'tls.translation': 'kernel-conntrack-reader', 'sample.translations': 'kernel-conntrack-reader',
}


def fields(row, kind):
    require(isinstance(row, dict) and set(row) <= set(SCHEMA_FIELDS[kind].split()), 'unknown_' + kind + '_field')


class EvidenceError(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise EvidenceError(code)


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, 'duplicate_json_field')
        value[key] = item
    return value


def read_json(path):
    raw = Path(path).read_bytes()
    require(len(raw) <= MAX_BYTES, 'evidence_overflow')
    return json.loads(raw, object_pairs_hook=unique_object,
                      parse_constant=lambda _: require(False, 'nonfinite_json'))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def manifest_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def digest(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def uint(value, minimum=0):
    return type(value) is int and minimum <= value < 1 << 64


def process(row):
    require(isinstance(row, dict) and set(row) == set(PROCESS_FIELDS), 'process_fields')
    require(all(uint(row[k], 1) for k in ('PID', 'StartTicks', 'ExeDevice', 'ExeInode'))
            and all(isinstance(row[k], str) and row[k] for k in
                    ('BootID', 'InvocationID', 'Cgroup')) and digest(row['ExeSHA256']), 'process_identity')
    return tuple(row[k] for k in PROCESS_FIELDS)


def socket(row):
    require(isinstance(row, dict) and set(row) == set(SOCKET_FIELDS), 'socket_fields')
    require(all(uint(row[k], 1) for k in ('NetNS', 'Inode', 'Cookie'))
            and uint(row['Device']) and all(isinstance(row[k], str) and row[k]
                                          for k in ('Local', 'Remote')), 'socket_identity')
    return tuple(row[k] for k in SOCKET_FIELDS[:-1]) + (process(row['Owner']),)


def attachment(row):
    require(isinstance(row, dict) and set(row) == set(ATTACHMENT_FIELDS), 'attachment_fields')
    require(all(uint(row[k], 1) for k in ('Generation', 'CID', 'HealthGeneration'))
            and all(isinstance(row[k], str) and row[k] for k in ('Participant', 'ServerID', 'URL')),
            'attachment_identity')
    socket(row['Raw'])
    process(row['Ingress'])
    return row


def connection_binding(accepted, local, remote, translation=None):
    """Bind a held client tuple through an independently read kernel mapping.

    Canonical443 may DNAT to shadow8443 (and hairpin may SNAT). The accepted
    tuple alone cannot equal the client tuple in that case. A URL/port guess
    never substitutes for the original/reply pair read from conntrack.
    origin: hot-update S9 plan §2.3 / §5.1 / T77
    """
    socket(accepted)
    if translation is None:
        return accepted['Local'] == remote and accepted['Remote'] == local
    fields(translation, 'translation')
    require(translation['producer'] == 'kernel-conntrack-reader'
            and uint(translation['generation'], 1) and uint(translation['netns'], 1), 'translation_custody')
    return (translation['netns'] == accepted['NetNS'] and
            translation['original_local'] == local and translation['original_remote'] == remote and
            translation['translated_local'] == accepted['Remote'] and
            translation['translated_remote'] == accepted['Local'])


def budgets():
    # Read the pinned deploy half of upgradebudget. Never execute its shell.
    names = ('HOT_UPDATE_REQUEST_S', 'HOT_UPDATE_PROBE_RATE', 'HOT_UPDATE_LIVE_FLOW_STALL_S',
             'HOT_UPDATE_TRANSFER_PROGRESS_S', 'HOT_UPDATE_REGISTER_DEADLINE_S')
    values = {}
    for line in Path(__file__).with_name('hot_update_budget.sh').read_text().splitlines():
        match = re.fullmatch(r'([A-Z_]+)=([0-9]+)', line)
        if match and match[1] in names:
            require(match[1] not in values, 'duplicate_budget')
            values[match[1]] = int(match[2])
    require(set(values) == set(names) and values['HOT_UPDATE_PROBE_RATE'] == 10, 'budget_contract')
    return values


def validate_manifest(m):
    fields(m, 'manifest')
    require(m.get('schema') == SCHEMA and re.fullmatch('[0-9a-f]{32}', m.get('nonce', '')),
            'manifest_schema')
    require(m.get('mode') in ('live', 'hermetic'), 'fixture_mode')
    require(uint(m.get('seconds'), 1) and m['seconds'] <= 4320, 'manifest_duration')
    require(m.get('carrier_transition') in ('unchanged', 'replaced'), 'carrier_profile')
    require(set(m.get('originals', {})) == set(ORIGINALS), 'requiredset_originals')
    for name, original in m['originals'].items():
        fields(original, 'original')
        identity = original['identity']
        require(isinstance(identity, dict) and bool(identity.get('application_nonce'))
                and bool(original['carrier']), 'original_identity_empty')
        if name in ('pty', 'exec'):
            fields(identity, 'task_original')
            process(identity['ctl'])
            process(identity['child'])
            require(bool(identity['task_id']), 'original_task_id')
        else:
            fields(identity, 'socket_original')
            socket(identity['socket'])
    require(bool(m.get('attachments')), 'requiredset_attachments')
    for participant, row in m['attachments'].items():
        require(attachment(row)['Participant'] == participant, 'participant_binding')
    hops = m.get('hops')
    # origin: S9 FIX-INFRAVERBS. Only an arm that never asks the product to roll
    # (refusal, exercise or legacy control) may freeze an empty hop schedule.
    require(isinstance(hops, list) and (bool(hops) or m.get('profile') in NO_ROLL_PROFILES), 'requiredset_hops')
    require(len({h['id'] for h in hops}) == len(hops), 'duplicate_hop')
    for hop in hops:
        fields(hop, 'hop')
        require(hop['component'] in ('nats', 'caddy', 'support') and
                hop['action'] in ('upgrade', 'rollback', 'reload', 'recover') and
                bool(hop['host']) and digest(hop['target_sha256']), 'hop_contract')
        require(bool(hop['participants']) and len(hop['participants']) == len(set(hop['participants']))
                and set(hop['participants']) <= set(m['attachments']), 'hop_participant_set')
    if m.get('profile') == 'cycle':
        hosts = list(dict.fromkeys(h['host'] for h in hops))
        # A healthy Caddy pair cycles (86.H); a NATS pair must defer instead.
        require(len(hosts) in ((1, 3) if any(h['component'] == 'nats' for h in hops) else (1, 2, 3)) and
                [(h['host'], h['action']) for h in hops] ==
                [(host, action) for action in ('upgrade', 'rollback', 'upgrade') for host in hosts],
                'full_adjacent_cycle')
    transfers = m.get('transfers', [])
    require(len(transfers) == 4 and {t['cohort'] for t in transfers} == set(COHORTS), 'four_original_transfers')
    for key in ('transfer_id', 'entry_nonce', 'session_id', 'bucket', 'object_version', 'ctl_producer'):
        require(len({t[key] for t in transfers}) == 4 and all(t[key] for t in transfers), 'transfer_isolation')
    for t in transfers:
        fields(t, 'transfer')
        require(t['size'] == (66549440 if t['cohort'].startswith('small-') else 1 << 30)
                and uint(t['rate_limit'], 1), 'transfer_profile')
        require(uint(t['started_ns'], 1) and process(t['ctl']), 'transfer_ctl')
        # The slowest payload must still be live at the last predeclared hop.
        # A start-time product rate limit is the only allowed pacing mechanism.
        require(t['size'] * 1_000_000_000 // t['rate_limit'] > m['seconds'] * 1_000_000_000,
                'transfer_budget_too_short')
    handoff = m.get('s7_handoff', {})
    fields(handoff, 'handoff')
    require(handoff.get('accepted_by') == 'lead' and handoff.get('schema') == 'transfer-v1'
            and set(handoff.get('files', {})) == {'executable', 'source', 'schema', 'positive', 'negative'},
            's7_handoff_pending')
    for row in handoff['files'].values():
        fields(row, 'file')
        require(isinstance(row.get('path'), str) and Path(row['path']).is_absolute()
                and digest(row.get('sha256')) and sha(row['path']) == row['sha256'], 's7_handoff_bytes')
    require(set(m.get('tls_lanes', [])) == (set(TLS_LANES) if any(
            h['component'] == 'caddy' for h in hops) else set()), 'canonical_tls_lanes')
    require(digest(m.get('s7_manifest_sha256')) and digest(m.get('s7_reader_digest')), 's7_reader_binding')
    require(set(m.get('phase_lanes', {})) == ({'writer_lost_ack', 'reader_partial_sync',
            'commit_rename', 'terminal_courier'} if hops else set()), 'transfer_phase_schedule')
    if m['mode'] == 'live':
        validate_s7_binding(m)
    return m


def validate_s7_binding(manifest):
    """Refuse unavailable S7 rate/schema support before the first LIVE slot.

    A label saying the handoff was accepted is insufficient: load the actual
    pinned S7 manifest through its own validator and bind all four originals.
    origin: hot-update S9 plan §8 / §9.1 / T77
    """
    import transfer_fixture
    binding = manifest['deployment']['s7']
    fields(binding, 's7_binding')
    require(sha(binding['manifest']) == manifest['s7_manifest_sha256'] and
            transfer_fixture.reader_digest() == manifest['s7_reader_digest'], 'live_s7_source_binding')
    actual = transfer_fixture.read_manifest(binding['manifest'])
    require(actual['mode'] == 'live', 'live_s7_fixture_required')
    transfers = {t['cohort']: t for t in actual['transfers']}
    for original in manifest['transfers']:
        require(all(transfers[original['cohort']][key] == original[key] for key in
                    ('transfer_id', 'entry_nonce', 'session_id', 'bucket', 'object_version', 'size', 'rate_limit')),
                'live_s7_original_binding')


def s7_checkpoint(snapshot, transfer, at=None):
    """Read the landed transfer-v1 schema, preserving original NUID and ctl."""
    require(not snapshot.get('setup') and isinstance(snapshot.get('events'), list)
            and isinstance(snapshot.get('processes'), dict), 's7_snapshot_custody')
    ctl = snapshot['processes'][transfer['ctl_producer']]
    expected = transfer['ctl']
    require(all(ctl[k] == expected[v] for k, v in
                (('pid', 'PID'), ('process_start', 'StartTicks'), ('boot_id', 'BootID'),
                 ('running_sha', 'ExeSHA256'))), 's7_original_ctl_changed')
    own = []
    for event in snapshot['events']:
        # Producer is attached by S7's custody reader, outside its closed wire row.
        row = {k: v for k, v in event.items() if k != 'producer'}
        require(bool(event.get('producer')) and event['producer'] in snapshot['processes'], 's7_event_producer_missing')
        validate_event(row)
        owner = snapshot['processes'][event['producer']]
        require(all(row[key] == owner[key] for key in ('pid', 'process_start', 'boot_id')), 's7_event_incarnation')
        if row['transfer_id'] != transfer['transfer_id']:
            continue
        require(row['entry_nonce'] == transfer['entry_nonce'] and
                row['object_version'] == transfer['object_version'], 's7_original_nuid_changed')
        if at is None or row['boottime_ns'] <= at:
            own.append(row)
    durable = max((r.get('durable_offset', 0) for r in own if r['kind'] in ('synced', 'ledger_sync')), default=0)
    return durable, own


def judge_samples(m, samples, t0, end, check, missing):
    """Per-hop physical rules over m['hops'], shared by FINAL and scoped checks.

    origin: S9 FIX-INFRAVERBS. A scoped round check passes the manifest with
    only that round's hops and the attachments it started from, so it applies
    exactly these rules rather than a second, drifting copy of them.
    """
    b = budgets()
    check(bool(samples), 'empty_samples')
    schedule = {(h['id'], p) for h in m['hops'] for p in PHASES}
    check(Counter((s['hop'], s['phase']) for s in samples) == Counter(schedule), 'hop_phase_coverage')
    originals = m['originals']
    previous_at = 0
    previous_attachments = m['attachments']
    previous_current = {}
    expected_sequence = [(h['id'], p) for h in m['hops'] for p in PHASES]
    check([(s['hop'], s['phase']) for s in samples] == expected_sequence, 'hop_phase_order')
    for sample in samples:
        fields(sample, 'sample')
        at = sample['at_ns']
        require(uint(at, 1) and max(t0, previous_at) <= at <= end, 'sample_clock')
        previous_at = at
        require(sample.get('producer') == 'kernel-and-target-reader', 'sample_producer')
        check(set(sample['business']) == set(originals), 'requiredset_changed')
        for name, row in sample['business'].items():
            fields(row, 'business')
            require(name in originals, 'unknown_original')
            base = originals[name]
            check(row['identity'] == base['identity'] and row['accepts'] == 1
                  and row['redials'] == 0, 'original_socket_or_child_changed:' + name)
            check(row['carrier'] == base['carrier'] or m['carrier_transition'] == 'replaced', 'carrier_changed:' + name)
            check(uint(row['tx']) and uint(row['rx'])
                  and digest(row['tx_sha256']) and row['tx_sha256'] == row['rx_sha256'], 'business_bytes:' + name)
        snapshot = sample['s7']
        check(not snapshot.get('failures'), 's7_sticky_failure')
        prefixes = {r['transfer_id']: r for r in sample['prefix_reads']}
        check(len(prefixes) == 4 and len(sample['prefix_reads']) == 4, 'prefix_requiredset')
        for t in m['transfers']:
            durable, events = s7_checkpoint(snapshot, t, at)
            tid = t['transfer_id']
            if not 0 < durable < t['size'] or any(r['kind'] == 'finish' for r in events):
                missing.append('original_transfer_not_live:' + tid)
            prefix = prefixes.get(tid)
            if prefix is not None:
                fields(prefix, 'prefix')
                # origin: hot-update S9 plan §4.3 / T77. A later repaired SHA
                # cannot erase the first independent read of the ACKed prefix.
                check(prefix['at_ns'] <= prefix['before_repair_ns'] and prefix['at_ns'] <= at
                      and bool(prefix['expected']) and prefix['expected'] == prefix['actual'], 'pre_repair_prefix:' + tid)
                chunks = prefix['expected']
                require(all(set(c) == {'index', 'length', 'sha256'} and uint(c['index'])
                            and uint(c['length'], 1) and digest(c['sha256']) for c in chunks), 'prefix_chunk_fields')
                check([c['index'] for c in chunks] == list(range(len(chunks)))
                      and sum(c['length'] for c in chunks) >= durable, 'prefix_chunk_coverage:' + tid)
        check(set(sample['attachments']) == set(m['attachments']), 'attachment_requiredset')
        hop = next(h for h in m['hops'] if h['id'] == sample['hop'])
        check(set(sample['accepted']) == set(sample['attachments']), 'accepted_requiredset')
        for participant, new in sample['attachments'].items():
            attachment(new)
            old = previous_attachments[participant]
            check(new['Participant'] == participant, 'attachment_participant')
            accepted = sample['accepted'][participant]
            socket(accepted)
            check(process(accepted['Owner']) == process(new['Ingress'])
                  and connection_binding(accepted, new['Raw']['Local'], new['Raw']['Remote'],
                      sample.get('translations', {}).get(participant)), 'accepted_ingress_owner')
            if sample['phase'] in ('moving', 'returning') and hop['action'] != 'reload' and participant in hop['participants']:
                check(new['Generation'] > old['Generation'] and
                      (new['ServerID'], new['CID']) != (old['ServerID'], old['CID'])
                      and socket(new['Raw'])[:6] != socket(old['Raw'])[:6], 'physical_attachment_unchanged')
                if hop['component'] == 'caddy':
                    check(process(new['Ingress']) != process(old['Ingress']), 'caddy_ingress_unchanged')
            previous_attachments = dict(previous_attachments, **{participant: new})
        if sample['phase'] == 'retired':
            census = sample['census']
            fields(census, 'census')
            require(census.get('producer') == 'kernel-reader' and census.get('complete') is True
                    and uint(census.get('generation'), 1) and bool(census.get('processes')), 'census_unknown')
            running = {process(p) for p in census['processes']}
            before, current = sample['before'], sample['current']
            # One chain per host AND component: a support exec is not the server (86.U).
            chain = (hop['host'], hop['component'])
            if chain in previous_current:
                check(process(before) == process(previous_current[chain]), 'before_incarnation_changed')
            previous_current[chain] = current
            check(current['ExeSHA256'] == hop['target_sha256'] and process(current) in running, 'disk_only_runtime')
            # Per-step incarnation checks allow a rollback to the initial SHA.
            # Reload preserves the process; support exec preserves PID/start but
            # must remove its previous executable incarnation from the census.
            if hop['action'] == 'reload':
                check(process(before) == process(current), 'reload_process_changed')
                check(sample['config_before'] != sample['config_after'], 'reload_config_unchanged')
            else:
                check(process(before) not in running, 'old_incarnation_alive')
                if hop['component'] in ('nats', 'caddy'):
                    # Stock servers must exit. A same-PID support exec is a
                    # different contract and cannot stand in for that exit.
                    check(not any(all(p[k] == before[k] for k in ('BootID', 'PID', 'StartTicks'))
                                  for p in census['processes']), 'old_server_process_alive')
            check(all(process(p) not in running for p in sample['temporary']), 'temporary_alive')
            check(census['old_owner_pending'] == 0 and sample['return_verified'] is True
                  and sample['root_revision'] == sample['transaction_revision'] > 0, 'retirement_incomplete')
            check(uint(census['live_business'], 1), 'live_business_missing')
            check(t0 <= sample['started_ns'] <= sample['finished_ns'] <= at
                  and sample['finished_ns'] - sample['started_ns'] <=
                  b['HOT_UPDATE_REGISTER_DEADLINE_S'] * 1_000_000_000, 'component_budget')
            if hop['component'] == 'caddy':
                socket(sample['main_ready'])
                check(process(sample['main_ready']['Owner']) == process(current), 'main_ready_hit_shadow')
                if hop['action'] != 'reload':
                    check(all(process(sample['attachments'][p]['Ingress']) == process(current)
                              for p in hop['participants']), 'return_attachment_not_current_main')


def judge(m, trace):
    validate_manifest(m)
    b = budgets()
    request_ns = b['HOT_UPDATE_REQUEST_S'] * 1_000_000_000
    flow_ns = b['HOT_UPDATE_LIVE_FLOW_STALL_S'] * 1_000_000_000
    progress_ns = b['HOT_UPDATE_TRANSFER_PROGRESS_S'] * 1_000_000_000
    failures, missing = [], []

    def check(ok, code):
        if not ok:
            failures.append(code)

    fields(trace, 'trace')
    require(trace.get('schema') == TRACE_SCHEMA and trace.get('nonce') == m['nonce']
            and trace.get('manifest_sha256') == manifest_digest(m)
            and uint(trace.get('t0_ns'), 1), 'trace_binding')
    require(trace.get('producer') == 'independent-observer', 'status_is_not_observation')
    t0, count = trace['t0_ns'], m['seconds'] * b['HOT_UPDATE_PROBE_RATE']
    end = t0 + m['seconds'] * 1_000_000_000
    # origin: hot-update S9 plan §9.1 / T77. Age before LIVE consumes the
    # original launch budget too; four newly declared rates cannot rewind it.
    require(all(t['started_ns'] <= t0 and t['started_ns'] +
                t['size'] * 1_000_000_000 // t['rate_limit'] > end
                for t in m['transfers']), 'original_transfer_budget_expired')
    # origin: hot-update S9 plan §9.1 / T77. Intended slots remain in the
    # denominator even when dispatch failed or the observer stopped early.
    slots = trace.get('slots', [])
    check(Counter(r['ordinal'] for r in slots) == Counter(range(count)), 'exact_ten_slots')
    effects = Counter(r['logical_id'] for r in trace.get('effects', []) if r.get('effect') == 'applied')
    for effect in trace.get('effects', []):
        fields(effect, 'effect')
    expected_effects = set()
    for row in slots:
        fields(row, 'slot')
        ordinal = row['ordinal']
        require(uint(ordinal) and ordinal < count, 'slot_ordinal')
        logical = m['nonce'] + '-' + str(ordinal)
        scheduled = t0 + ordinal * 100_000_000
        check(row['logical_id'] == logical and row['kind'] == KINDS[ordinal % len(KINDS)]
              and row['scheduled_ns'] == scheduled, 'slot_schedule')
        require(uint(row['dispatched_ns']) and uint(row['terminal_ns']), 'slot_clock')
        check(scheduled <= row['dispatched_ns'] <= row['terminal_ns'] <= scheduled + request_ns
              and row['native_rc'] == 0 and row['validated'] is True, 'request_budget_or_outcome')
        attempts = row['attempts']
        check(bool(attempts), 'missing_attempt')
        prior = row['dispatched_ns']
        for number, a in enumerate(attempts, 1):
            require(set(a) == set(ATTEMPT_FIELDS) and type(a['sent']) is bool, 'attempt_fields')
            require(a['phase'] in ATTEMPT_PHASES and a['class'] in ATTEMPT_CLASSES
                    and uint(a['backoff_ns']), 'attempt_vocabulary')
            check(a['logical_id'] == logical and a['ordinal'] == number and
                  prior <= a['started_mono_ns'] <= a['finished_mono_ns'] <= row['terminal_ns'], 'attempt_binding')
            # Begin order is monotonic; an outer connect span can encompass
            # nested DNS/TLS spans whose Record completes first.
            prior = a['started_mono_ns']
        if row['kind'] in ('write', 'short_exec'):
            expected_effects.add(logical)
    check(set(effects) == expected_effects and all(n == 1 for n in effects.values()), 'exactly_once_effects')
    check(trace.get('complete') is True and trace.get('tail_complete') is True, 'half_trace')

    samples = trace.get('samples', [])
    originals = m['originals']
    # A no-roll arm freezes no hops (validate_manifest); any sample is then red.
    if m['hops'] or samples:
        judge_samples(m, samples, t0, end, check, missing)

    controls = trace.get('controls', [])
    expected_controls = {'resize', 'signal', 'keepalive_lost_ack', 'input', 'output',
                         'ps_victim', 'kill_victim', 'revoke_alice'}
    check({r['op'] for r in controls} == expected_controls, 'control_coverage')
    for r in controls:
        fields(r, 'control')
        check(r['producer'] == 'control-target-reader' and r['effect'] is True
              and t0 <= r['at_ns'] <= end, 'control_effect')
        check(r['subject'] == ('ss-alice' if r['op'] == 'revoke_alice' else
              'victim' if r['op'] in ('ps_victim', 'kill_victim') else 'pty'), 'control_subject')
    # Without a roll the revocation is witnessed by the same periodic reader.
    check(any(s['business'].get('ss-alice', {}).get('revoked') is True
              for s in (samples if m['hops'] else trace.get('progress', []))), 'alice_not_revoked')
    # Phase boundaries may be 120 seconds apart. Independent periodic reads
    # carry progress between them; a sparse boundary sample never proves a
    # stall or hides one. Each direction has its own last-advance clock.
    # origin: hot-update S9 plan §9.1 / T77
    progress = trace.get('progress', [])
    check(bool(progress), 'progress_missing')
    for row in progress:
        fields(row, 'progress')
    last_bytes = {key: ([0, t0], [0, t0]) for key in originals}
    last_progress = {t['transfer_id']: (0, t0) for t in m['transfers']}
    for row in sorted(samples + progress, key=lambda r: r['at_ns']):
        at = row['at_ns']
        require(uint(at, 1) and t0 <= at <= end
                and row['producer'] == 'kernel-and-target-reader', 'progress_custody')
        check(set(row['business']) == set(originals), 'progress_requiredset')
        for name, business in row['business'].items():
            fields(business, 'business')
            require(name in originals, 'unknown_progress_original')
            check(business['identity'] == originals[name]['identity'] and business['redials'] == 0
                  and business['accepts'] == 1, 'progress_original_changed:' + name)
            idle = name in ('tcp-idle', 'tcp-half-close') or (name == 'ss-alice' and business.get('revoked') is True)
            for direction, prior in zip(('tx', 'rx'), last_bytes[name]):
                value, advanced = prior
                check(uint(business[direction]) and business[direction] >= value, 'progress_regressed:' + name)
                if not idle:
                    check(at - advanced <= flow_ns, 'flow_stall:' + name)
                prior[:] = [business[direction], at if business[direction] > value else advanced]
        check(not row['s7'].get('failures'), 's7_sticky_failure')
        for transfer in m['transfers']:
            durable, _ = s7_checkpoint(row['s7'], transfer, at)
            tid = transfer['transfer_id']
            prior, advanced = last_progress[tid]
            check(durable >= prior and at - advanced <= progress_ns, 'transfer_progress:' + tid)
            last_progress[tid] = (durable, at if durable > prior else advanced)
    for name, directions in last_bytes.items():
        if name not in ('tcp-idle', 'tcp-half-close', 'ss-alice'):
            check(all(end - advanced <= flow_ns for _, advanced in directions), 'flow_tail_stall:' + name)
    for tid, (_, advanced) in last_progress.items():
        check(end - advanced <= progress_ns, 'transfer_tail_stall:' + tid)
    final = trace.get('final_business', {})
    check(set(final) == set(originals), 'final_requiredset')
    for name, row in final.items():
        fields(row, 'business')
        check(row['identity'] == originals[name]['identity'] and row['redials'] == 0
              and row['accepts'] == 1 and row['native_rc'] == 0
              and row['fin'] == [True, True] and row['tx_sha256'] == row['rx_sha256'], 'final_original:' + name)

    tls = trace.get('tls', [])
    expected_tls = {(i, lane) for i in range(count) for lane in m['tls_lanes']}
    check(Counter((r['ordinal'], r['lane']) for r in tls) == Counter(expected_tls), 'fresh_tls_slot_coverage')
    for r in tls:
        fields(r, 'tls')
        scheduled = t0 + r['ordinal'] * 100_000_000
        check(r['scheduled_ns'] == scheduled and scheduled <= r['started_ns'] <= r['terminal_ns']
              <= scheduled + request_ns and r['success'] is True and r['attempts'] == 1, 'first_tls_attempt')
        if r['success'] is not True:
            continue
        socket(r['accepted'])
        check(connection_binding(r['accepted'], r['local'], r['remote'], r.get('translation')), 'fresh_tls_connection_binding')
        if r.get('bypass'):
            check(process(r['accepted']['Owner']) == process(r['main']), 'bypass_hit_shadow')

    s7_final = trace.get('s7_final', {})
    check(s7_final.get('native_rc') == 0 and all(digest(s7_final.get(k)) for k in
          ('manifest_sha256', 'reader_digest', 'oracle_input_sha')), 's7_final_receipt')
    check(s7_final.get('manifest_sha256') == m['s7_manifest_sha256']
          and s7_final.get('reader_digest') == m['s7_reader_digest'], 's7_final_binding')
    lanes = trace.get('phase_lanes', [])
    check(Counter(r['lane'] for r in lanes) == Counter(m['phase_lanes'].keys()), 'transfer_phase_coverage')
    expected_kind = {'writer_lost_ack': 'append', 'reader_partial_sync': 'synced',
                     'commit_rename': 'primitive_exit', 'terminal_courier': 'history'}
    for lane in lanes:
        fields(lane, 'phase_lane')
        name = lane['lane']
        require(name in m['phase_lanes'], 'unknown_transfer_phase')
        rows = [validate_event({k: v for k, v in r.items() if k != 'producer'}) for r in lane['events']]
        cut = next((s['at_ns'] for s in samples if s['hop'] == lane['hop'] and s['phase'] == 'cutting'), None)
        own = [r for r in rows if r['transfer_id'] == m['phase_lanes'][name]]
        original = next((t for t in m['transfers'] if t['transfer_id'] == m['phase_lanes'][name]), None)
        if original is not None:
            check(all(r['entry_nonce'] == original['entry_nonce'] and r['object_version'] == original['object_version']
                      for r in own), 'phase_original_changed:' + name)
        check(cut is not None and any(r['boottime_ns'] <= cut for r in own)
              and any(r['boottime_ns'] >= cut for r in own)
              and any(r['kind'] == expected_kind[name] for r in own), 'phase_did_not_cross_cut:' + name)
        if name == 'writer_lost_ack':
            check(lane.get('sent') is True and lane.get('effect') is True
                  and lane.get('reply_cut') is True
                  and any(r.get('actual_ack_seq', 0) > 0 for r in own), 'lost_ack_not_hit')
        elif name == 'reader_partial_sync':
            check(any(0 < r.get('durable_offset', 0) < r.get('fetched_offset', 0) for r in own), 'reader_not_partial')
        elif name == 'commit_rename':
            check(any(r.get('outcome') == 'landed' for r in own)
                  and any(r['kind'] == 'directory_sync' and r.get('parent_synced') is True for r in own), 'commit_not_durable')
        else:
            check(any(r['kind'] == 'history' and r.get('native_outcome') == 0 for r in own), 'terminal_not_observed')
    # The unchanged-carrier S5 oracle and original S7 judge are conjuncts, never
    # replaced with a final hash or a new transfer's successful outcome.
    check(trace.get('flow_native_rc') == 0, 'flow_oracle')
    if m['mode'] == 'live':
        check(verify_live_readers(m, trace), 'live_flow_raw_oracle')
    rc = 1 if failures else 4 if missing else 0
    return dict(native_rc=rc, verdict={0: 'GREEN', 1: 'ASSERT-FAIL', 4: 'INCOMPLETE'}[rc],
                failures=sorted(set(failures)), incomplete=sorted(set(missing)),
                slots=count, hops=len(m['hops']), reader_sha256=sha(__file__), evidence_scope=m['mode'])


def verify_live_readers(manifest, trace):
    """The recorded native status cannot replace the original raw oracles."""
    import transfer_fixture
    import hot_update_flow_probe
    deployment = manifest['deployment']
    s7 = deployment['s7']
    require(sha(s7['manifest']) == manifest['s7_manifest_sha256'] and
            transfer_fixture.reader_digest() == manifest['s7_reader_digest'], 'live_s7_source_binding')
    root = Path(s7['evidence'])
    require(read_json(root / 'verdict.json') == trace['s7_final'], 'live_s7_receipt_binding')
    spec = transfer_fixture.read_manifest(s7['manifest'])
    names = {t[k] for t in spec['transfers'] for k in ('object_stream', 'history_stream')}
    require(all(re.fullmatch('[A-Za-z0-9_-]+', name) for name in names), 'live_stream_path')
    inputs = {name: read_json(root / ('raw-' + name + '.json')) for name in names}
    require(hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest() ==
            trace['s7_final']['oracle_input_sha'], 'live_s7_raw_input_binding')
    flow = deployment['flow']
    try:
        result = hot_update_flow_probe.judge(Path(flow['evidence']), flow['nonce'],
                                           carrier_transition=manifest['carrier_transition'])
        return result.get('verdict') == 'GREEN'
    except hot_update_flow_probe.ContinuityError:
        return False
    except hot_update_flow_probe.EvidenceError:
        raise EvidenceError('live_flow_custody_missing') from None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('judge', 'validate'))
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--trace', type=Path)
    parser.add_argument('--live', action='store_true')
    args = parser.parse_args()
    manifest = read_json(args.manifest)
    if args.live:
        require(manifest.get('mode') == 'live', 'live_fixture_required')
    if args.command == 'validate':
        validate_manifest(manifest)
        return 0
    require(args.trace is not None, 'trace_required')
    result = judge(manifest, read_json(args.trace))
    print(json.dumps(result, sort_keys=True))
    return result['native_rc']


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, TypeError):
        print(json.dumps(dict(native_rc=2, verdict='SETUP-RED', failures=['invalid_observer_custody'])))
        sys.exit(2)
