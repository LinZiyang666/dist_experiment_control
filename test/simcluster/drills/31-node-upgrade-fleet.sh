#!/bin/sh
# 31-node-upgrade-fleet.sh — S5, N=1 cluster + 2 agent + ctl: the agent `node upgrade` surface on the real
# stack. #28 (the agent-side upgrade allow-list was UNCONFIGURABLE — hardcoded to a GitHub prefix, so a
# self-hosted mirror the BROKER whitelisted was still refused by the AGENT with url_not_allowed_local) is now
# FIXED (R15): the agent honours a top-level `upgrade.url_allow` block in agent.yaml (cmd/tether/agent.go
# resolveAgentUpgradeAllow; internal/agent/upgrade.go:76 consumes Config.UpgradeURLAllowlist). This drill
# CONFIGURES that block (labeled deployment config, Mandate ③ — the operator's own job, not compensating for a
# tether gap) and PROVES the fix two ways:
#   (a) the CONFIGURED artifact URL now PASSES the agent's local allow-list — an upgrade carrying a deliberately
#       WRONG (but 64-hex-valid) sha reaches the agent's POST-DOWNLOAD digest check and fails `sha256_mismatch`,
#       NOT `url_not_allowed_local` (the old wall). Had the config not taken, the agent would still answer
#       url_not_allowed_local and the assert_refuses sig would MISMATCH → ASSERT-FAIL (config that failed to
#       apply cannot pass this arm).
#   (b) the allow-list is still LIVE, not disabled: a URL the BROKER whitelists but the AGENT does not (the
#       broker's list is deliberately wider) is still refused `url_not_allowed_local` AT THE AGENT gate.
# GREEN negatives (broker-side url_not_allowed / sha256_invalid / not_owner) remain independently reachable.
# FORBIDDEN: faking github.com via /etc/hosts (Mandate ①). Consumes S0-artifact (real https tarball + sha) +
# S0-layout (agent binary in ~/.local/bin). U1-pinned N=1 cluster.
#
# FALSE-GREEN GUARDS:
#  - (a) is signed on `sha256_mismatch` (the AGENT's post-download digest check) — reachable ONLY after the
#    request cleared BOTH the broker AND the agent URL gates AND downloaded the tarball, so a wrong-sha probe
#    can never be confused with a url gate. url_not_allowed_local / url_not_allowed / sha256_invalid /
#    download_failed all MISS the sig → ASSERT-FAIL.
#  - (b) is signed on `url_not_allowed_local` (the `_local` suffix). The broker's own `url_not_allowed` (no
#    `_local`) does NOT match, so had the broker refused first (its list too narrow) the assert would FAIL
#    LOUD — proving the refusal came from the AGENT, not the broker. The broker's wider 2nd prefix
#    (see _allow_artifact) is load-bearing for this.
#  - the wrong sha is 64 lowercase hex (passes the broker's sha256 sanity) so it cannot short-circuit to
#    sha256_invalid; a NON-hex sha is exercised separately by N2.
#  - NO arm triggers a real re-exec (which would mutate the shared binary + re-home the fleet mid-drill): (a)
#    fails on the wrong sha; the non-vacuity + F3 probes fail at the agent url gate; N3 fails at the broker
#    owner gate (pre-forward); F4's --timeout 0 request is never even published (nats.go context.go:49). The
#    SUCCESSFUL re-exec path is a first-class not_covered (see drill_end). A dedicated success drill must
#    be created; no owner currently exists.
set -u
# origin: hot-update S8 plan §9.4 / P29. V1 keeps the original oracle below;
# V2 consumes real CLI actions and independent, read-only transaction evidence.
# arms: V1 V2
# fixture: V1=install V2=install
# grows: V1=0 V2=0
# worst: V1=300 V2=900
# forgoes: V1=- V2=-
. "$HERE/lib/log.sh"
. "$HERE/lib/assert.sh"
. "$HERE/lib/supervisor.sh"
if [ "${S8_REGRESSION_SUPERVISED:-0}" != 1 ]; then
    [ -x "${SIM_UPGRADE_SUPERVISOR:-}" ] || {
        drill_begin "31-node-upgrade-fleet"
        setup_fail "shared process supervisor is required"
    }
    S8_REGRESSION_SUPERVISED=1
    export S8_REGRESSION_SUPERVISED
    exec "$SIM_UPGRADE_SUPERVISOR" -- /bin/sh "$0" "$@"
fi

regression_v2() {
    python3 -I -B - "$HERE" "$1" "${UPGRADE_REGRESSION_MANIFEST:-}" "${UPGRADE_REGRESSION_EVIDENCE:-}" <<'S8_REGRESSION_PY'
# S8_REGRESSION_PYTHON_BEGIN
"""P29's read-only regression oracle and supervised action driver.

Manifest v1: drill, fixture (P26 manifest path for 33; label/path map for 31), observer (command descriptor),
cases (ordered label/command/deadline_s/want_rc/expect rows). expect contains the frozen
Key, Owner, source_sha, target_sha, parent_tx_id and parent_slot. Each observer
returns version/label/nonce/at_ns/record/journal/node/plan. journal is a public
projection: exists/durable/current/archives/metadata; never PIN or SS material.
node is key/owner/incarnation/online/probe_nonce/probe_ok/running_sha/effective_sha/disk_sha.
plan is null for 33, or before/after/dispatches/children/step_receipts/participants/holder/
released_holder/agents_before/agents_after/driver_before/driver_after/driver_dead for 31.
Before/after are frozen Plans, agents are frozen Fences, drivers are frozen Owners;
children are fresh P26 completion samples; participants hashes the independently
observed complete managed roster, never the Plan's own surviving subset. The
observer must read, never write, state. B6 releases the real SYN fault only after
the reverse cut is observed; healing precedes B5's three-receipt verdict.

Every case runs a declared product command (or a P23 fault/driver-death adapter),
then samples the SAME P26 original roster and journal throughout each fixture.
31 uses separate fixtures for its different fleet scopes; resume kills and rejoins
its own driver with its original saved plan. Supply is done before
start; this driver never copies .prev, edits journals or reconnects business IO.
An absent integration adapter is SETUP-RED, not a weaker historical assertion.
"""
import importlib.util
import os
from pathlib import Path
import secrets
import sys
import time

LIB = Path(sys.argv[1]).resolve() / 'drills' / 'lib'
spec = importlib.util.spec_from_file_location('upgrade_tx_fixture', LIB / 'upgrade_tx_fixture.py')
F = importlib.util.module_from_spec(spec)
spec.loader.exec_module(F)
C, S = F.collect, F.supervisor
LABELS = {'31': ('fleet', 'canary', 'agentless', 'brokeronly', 'noop', 'resume', 'lease'),
          '33': ('B2', 'B3', 'C', 'B6', 'B5', 'B6b', 'B6c', 'B6d', 'A1', 'A3', 'C2')}
RECEIPTS = (('control_receipt', 'control_ready'), ('resources_receipt', 'resources_migrated'),
            ('released_receipt', 'old_generation_released'))


def check(value, reason):
    C.require(value, reason, 1)


def shape(row, keys, reason):
    C.require(type(row) is dict and set(row) == set(keys.split()), reason)


def public_record(record):
    # origin: hot-update S8 plan §2 / §9.4. Journals contain only frozen public
    # DTOs. An accidental private/secret field must be rejected before writing
    # observer evidence, just as P26 rejects unknown sample fields.
    fields = {
        'record': 'version key revision grant forward reverse actions reason effective_sha recorded_previous_sha',
        'key': 'domain_id component sid configured_nid config_path uid',
        'owner': 'boot_id pid start_ticks instance_id',
        'handle': 'key tx_id cut_id generation revision',
        'grant': 'handle parent_tx_id parent_slot client_nonce issuer actor_fp mode source_release target_release source_sha target_sha scope_digest capability_digest expires_at',
        'attempt': 'handle direction source_sha target_sha source_generation phase cut_stage outcome scope control resources released',
        'action': 'id handle kind intent_digest result_digest state nonce',
        'scope': 'id participants domains expected',
        'receipt': 'kind stamp known complete running_sha census evidence_digest',
        'stamp': 'attempt producer nonce sequence at scope_digest',
        'census': 'stamp known complete resources expected_digest admitting old_owners',
        'resource': 'domain id origin owner generation state custody',
    }
    edges = {'record': dict(key='key', grant='grant', forward='attempt', reverse='attempt', actions='action'),
             'handle': dict(key='key'), 'grant': dict(handle='handle', issuer='owner'),
             'attempt': dict(handle='handle', scope='scope', control='receipt', resources='receipt', released='receipt'),
             'action': dict(handle='handle'), 'scope': dict(participants='key', expected='resource'),
             'receipt': dict(stamp='stamp', census='census'), 'stamp': dict(attempt='handle', producer='owner'),
             'census': dict(stamp='stamp', resources='resource', old_owners='owner'), 'resource': dict(owner='owner')}
    def walk(row, kind, depth=0):
        C.require(depth < 16 and type(row) is dict and set(row) <= set(fields[kind].split()), 'private_or_unknown_journal_field')
        for name, child in edges.get(kind, {}).items():
            if name in row:
                if row[name] is None and (kind, name) in {('record', 'actions'), ('scope', 'participants'),
                        ('scope', 'expected'), ('census', 'resources'), ('census', 'old_owners')}:
                    continue  # Go encodes nil slices as null, including the unused reverse attempt.
                for value in row[name] if type(row[name]) is list else [row[name]]:
                    walk(value, child, depth+1)
    walk(record, 'record')


def public_evidence(row):
    shape(row, 'version label nonce at_ns record journal node plan', 'regression_observer_schema')
    shape(row['journal'], 'exists durable current archives metadata', 'journal_observer_schema')
    public_record(row['record'])
    for record in row['journal']['archives'] + ([row['journal']['current']] if row['journal']['current'] is not None else []):
        public_record(record)
    shape(row['node'], 'key owner incarnation online probe_nonce probe_ok running_sha effective_sha disk_sha', 'node_observer_schema')
    public_record(dict(key=row['node']['key'], grant=dict(issuer=row['node']['owner'])))
    for metadata in row['journal']['metadata']:
        C.require(type(metadata) is dict and set(metadata) <= set('tx_id reported_revision window pins archived_at'.split()),
                  'private_metadata_field')
    if row['plan'] is not None:
        plan = row['plan']
        shape(plan, 'before after dispatches children step_receipts participants holder released_holder agents_before agents_after driver_before driver_after driver_dead', 'fleet_observer_schema')
        for value in (plan['before'], plan['after']):
            shape(value, 'parent scope steps source_manifest_digest target_manifest_digest deadline', 'plan_dto_schema')
            public_record(dict(grant=dict(handle=value['parent']), forward=dict(scope=value['scope'])))
            for step in value['steps']:
                shape(step, 'ordinal participant grant kind depends_on', 'step_dto_schema')
                public_record(dict(key=step['participant'], grant=step['grant']))
        for dispatch in plan['dispatches']:
            shape(dispatch, 'ordinal grant native_rc', 'dispatch_observer_schema')
            public_record(dict(grant=dispatch['grant']))
        for child in plan['children']:
            C.schema_check(child)
        for receipt in plan['step_receipts']:
            public_record(dict(forward=dict(released=receipt)))
        for fence in plan['agents_before'] + plan['agents_after']:
            shape(fence, 'current owner running_sha previous_sha incarnation', 'agent_fence_schema')
            public_record(dict(grant=dict(handle=fence['current'], issuer=fence['owner'])))
        for owner in (plan['driver_before'], plan['driver_after']):
            public_record(dict(grant=dict(issuer=owner)))


def completion(sample):
    # origin: hot-update S8 plan §9.4 / 33.A1. COMMITTED text cannot sign this.
    check(sample['phase'] in ('completed', 'rolled_back') and sample['outcome'] in ('success', 'rolled_back')
          and sample['running_sha'] == sample['effective_sha'] == sample['target_sha'], 'completion_not_current')
    for field, kind in RECEIPTS:
        C.validate_receipt(sample, field, kind)
    check(len({sample[field]['evidence_digest'] for field, _ in RECEIPTS}) == 3, 'receipts_not_independent')


def identity(row, sample, expected, nonce, begin, end):
    public_evidence(row)
    check(type(row['version']) is int and row['version'] == 1 and row['nonce'] == nonce
          and type(row['at_ns']) is int and begin <= row['at_ns'] <= end, 'regression_observation_stale')
    record, node = row['record'], row['node']
    shape(node, 'key owner incarnation online probe_nonce probe_ok running_sha effective_sha disk_sha', 'node_observer_schema')
    grant = record['grant']
    handle = grant['handle']
    # origin: hot-update S8 plan §9.4 / 33.A3. Same tag and ONLINE are insufficient.
    check(record['version'] == 2 and record['key'] == handle['key'] == node['key'] == expected['key']
          and handle['tx_id'] == sample['tx_id'], 'regression_target_key_or_tx_changed')
    check(grant['source_sha'] == expected['source_sha'] and grant['target_sha'] == expected['target_sha']
          and grant.get('parent_tx_id', '') == expected['parent_tx_id']
          and grant.get('parent_slot', '') == expected['parent_slot'], 'grant_or_parent_changed')
    check(node['owner'] == expected['owner'] and node['incarnation'] == expected['incarnation'] and node['incarnation'] and
          node['owner']['pid'] == sample['pid'] and node['owner']['start_ticks'] == sample['start_ticks']
          and node['owner']['boot_id'] == sample['owner_boot_id'] and sample['nonce'] == nonce and
          node['running_sha'] == sample['running_sha'] and node['effective_sha'] == sample['effective_sha'],
          'running_owner_or_authority_changed')
    shape(row['journal'], 'exists durable current archives metadata', 'journal_observer_schema')
    # origin: hot-update S8 plan §9.4 / P29 mutant. Unlinking the sole journal
    # must stay red even when ctl still prints COMMITTED and node ls is ONLINE.
    check(row['journal']['exists'] is True and row['journal']['durable'] is True, 'journal_deleted_or_not_durable')
    records = row['journal']['archives'] + ([row['journal']['current']] if row['journal']['current'] is not None else [])
    check([item for item in records if item['grant']['handle']['tx_id'] == handle['tx_id']] == [record],
          'journal_execution_not_retained')
    # origin: hot-update S8 plan §9.4 / 33.C2. A rewritten Current cannot
    # borrow a genuine observation of another Key, revision or execution cut.
    attempt = record[sample['direction']]
    check(attempt['handle']['key'] == record['key'] and attempt['handle']['revision'] == record['revision']
          and all(attempt['handle'][key] == sample[key] for key in ('tx_id', 'cut_id', 'generation'))
          and all(sample[field]['stamp']['attempt'] == attempt['handle'] and sample[field]['stamp']['producer'] == node['owner'] for field, _ in RECEIPTS)
          and attempt['phase'] == sample['phase'] and attempt['outcome'] == sample['outcome'], 'current_attempt_not_observed')


def fresh_control(row, sample, nonce):
    node = row['node']
    # origin: hot-update S8 plan §9.4 / 33.B6b/B6c. A real harmless command
    # must cross the restored SUB/Flush path with this query's nonce.
    check(node['online'] is True and node['probe_ok'] is True and node['probe_nonce'] == nonce,
          'online_without_command_subscription')
    C.validate_receipt(sample, 'control_receipt', 'control_ready')


def archived(row, record):
    journal = row['journal']
    matches = [item for item in journal['archives'] if item['grant']['handle']['tx_id'] == record['grant']['handle']['tx_id']]
    check(matches == [record], 'archive_missing_or_execution_rewritten')
    metadata = [item for item in journal['metadata'] if item['tx_id'] == record['grant']['handle']['tx_id']]
    check(len(metadata) == 1 and metadata[0]['archived_at'] > 0
          and metadata[0]['reported_revision'] >= record['revision'], 'durable_report_metadata_missing')
    # origin: hot-update S8 plan §9.4 / 33.B6d. A surviving archive shell
    # without its durable completion proofs is as useless as an unlinked marker.
    attempt = record['reverse'] if record['reverse'].get('direction') == 'reverse' else record['forward']
    for field, kind in (('control', 'control_ready'), ('resources', 'resources_migrated'), ('released', 'old_generation_released')):
        receipt = attempt[field]
        handle = receipt['stamp']['attempt']
        check(receipt['known'] is True and receipt['complete'] is True
              and all(handle[key] == attempt['handle'][key] for key in ('key', 'tx_id', 'cut_id', 'generation'))
              and 0 < handle['revision'] <= attempt['handle']['revision'] and receipt['kind'] == kind
              and receipt['running_sha'] == attempt['target_sha'] and receipt['evidence_digest'], 'archived_receipt_missing')


def judge33(label, row, sample, rc, previous):
    record, journal = row['record'], row['journal']
    if label == 'B2':
        # origin: hot-update S8 plan §9.4 / 33.B2. Acceptance and disk stage
        # are recorded actions, not a phrase that could change before exec.
        check(sample['phase'] == 'staged' and sample['outcome'] == 'pending'
              and sample['direction'] == 'forward', 'staged_phrase_without_acceptance')
        check(journal['current'] == record and record['forward']['handle']['tx_id'] == sample['tx_id'], 'stage_current_missing')
        actions = [a for a in record['actions'] if a['kind'] == 'publish' and a['state'] == 'durable'
                   and all(a['handle'][key] == record['forward']['handle'][key] for key in ('key', 'tx_id', 'cut_id', 'generation'))
                   and 0 < a['handle']['revision'] <= record['revision'] and a['id'] and a['nonce']
                   and a['intent_digest'] and a['result_digest']]
        check(len(actions) == 1 and row['node']['disk_sha'] == record['grant']['target_sha'], 'disk_stage_not_observed')
    elif label == 'B3':
        check(sample['direction'] == 'forward' and sample['running_sha'] == record['grant']['target_sha']
              and sample['phase'] in ('activating', 'serving_new', 'migrating'), 'staged_image_never_executed')
    elif label == 'C':
        old = previous['B3']['record']
        check(rc == 75 and journal['current'] == record and record['grant'] == old['grant']
              and record['key'] == old['key'] and record['revision'] >= old['revision']
              and sample['outcome'] == 'pending', 'sibling_pending_not_deferred')
    elif label in ('B6', 'B5', 'B6b', 'B6c', 'B6d'):
        # origin: hot-update S8 plan §9.4 / 33.B5. Reverse receipts must be
        # produced after real activation, never written before a failed exec.
        completion(sample)
        forward, reverse = record['forward'], record['reverse']
        check(sample['phase'] == 'rolled_back' and sample['direction'] == 'reverse'
              and record['recorded_previous_sha'] == reverse['target_sha'] == sample['target_sha']
              and reverse['handle']['cut_id'] == sample['cut_id']
              and reverse['handle']['generation'] == sample['generation']
              and reverse['handle']['cut_id'] != forward['handle']['cut_id']
              and reverse['handle']['generation'] > forward['handle']['generation'], 'reverse_not_fresh')
        if label == 'B5':
            check(previous['B3']['node']['running_sha'] == record['grant']['target_sha'], 'rollback_without_forward_exec')
        check(rc == (64 if label == 'B5' else 0), 'rollback_cli_exit_wrong')
        if label != 'B5':
            fresh_control(row, sample, row['nonce'])
        if label == 'B6d':
            # Current is cleared by the product reporter; history remains durable.
            check(journal['current'] is None, 'terminal_current_not_cleared')
            archived(row, record)
    else:
        completion(sample)
        fresh_control(row, sample, row['nonce'])
        check(rc == 0 and sample['direction'] == 'forward' and sample['phase'] == 'completed'
              and sample['target_sha'] == record['grant']['target_sha']
              and record['recorded_previous_sha'] == record['grant']['source_sha'], 'target_not_completed')
        if label == 'A3':
            check(record == previous['A1']['record'], 'completed_execution_changed')
        if label == 'C2':
            old = previous['A3']['record']
            # origin: hot-update S8 plan §9.4 / 33.C/C2. Two actual children
            # share a domain but retain their own Key, grant and immutable history.
            check(record['key']['domain_id'] == old['key']['domain_id'] and record['key'] != old['key']
                  and record['grant']['handle']['tx_id'] != old['grant']['handle']['tx_id']
                  and record['grant']['parent_tx_id'] == old['grant']['parent_tx_id']
                  and record['grant']['parent_tx_id'] and record['grant']['parent_slot'] == old['grant']['parent_slot']
                  and record['revision'] > 0, 'sibling_child_overwrote_current')
            check(journal['current'] == record, 'late_callback_overwrote_new_current')
            archived(row, old)


def judge31(label, row, sample, rc, previous):
    plan = row['plan']
    shape(plan, 'before after dispatches children step_receipts participants holder released_holder agents_before agents_after driver_before driver_after driver_dead', 'fleet_observer_schema')
    before, after = plan['before'], plan['after']
    steps = before['steps']
    # origin: hot-update S8 plan §9.4 / 31 fleet. Resume cannot enumerate a new
    # target set or mint replacement child grants after the first effect.
    check(before == after and steps and [step['ordinal'] for step in steps] == list(range(1, len(steps)+1))
          and all(step['grant']['parent_tx_id'] == before['parent']['tx_id']
                  and step['grant']['handle']['key'] == step['participant'] for step in steps), 'ordered_plan_or_child_changed')
    check(len({step['grant']['handle']['tx_id'] for step in steps}) == len(steps)
          and set(plan['participants']) == {C.digest(key) for key in before['scope']['participants']}, 'full_fleet_participant_missing')
    dispatches = plan['dispatches']
    by_ordinal = {step['ordinal']: step for step in steps}
    cuts = [step for step in steps if step['kind'] in ('broker_cut', 'agent_cut')]
    check(len(cuts) == len(plan['participants'])
          and {C.digest(step['participant']) for step in cuts} == set(plan['participants']), 'fleet_cut_participant_missing')
    check(dispatches and all(item['ordinal'] in by_ordinal and item['grant'] == by_ordinal[item['ordinal']]['grant'] for item in dispatches)
          and len({item['ordinal'] for item in dispatches}) == len(dispatches), 'dispatch_not_original_child')
    if label == 'canary':
        check(rc in (64, 74, 75) and len(dispatches) == 1 and cuts and dispatches[0]['ordinal'] == cuts[0]['ordinal']
              and dispatches[0]['native_rc'] != 0, 'canary_failure_did_not_stop')
        return
    completion(sample)
    fresh_control(row, sample, row['nonce'])
    check(rc == 0, 'fleet_cli_exit_wrong')
    check([item['ordinal'] for item in dispatches] == [step['ordinal'] for step in cuts]
          and all(item['native_rc'] == 0 for item in dispatches), 'fleet_dispatch_incomplete_or_reordered')
    check(cuts and len(plan['children']) == len(cuts), 'fleet_child_completion_missing')
    for step, child in zip(cuts, plan['children']):
        check(child['tx_id'] == step['grant']['handle']['tx_id']
              and child['target_sha'] == step['grant']['target_sha']
              and child['control_receipt']['stamp']['attempt']['key'] == step['participant'], 'fleet_child_receipt_changed')
        completion(child)
        check(child['nonce'] == row['nonce'], 'fleet_child_completion_stale')
    effects = [step for step in steps if step not in cuts]
    check(len(plan['step_receipts']) == len(effects), 'host_or_cluster_receipt_missing')
    for step, receipt in zip(effects, plan['step_receipts']):
        check(receipt['known'] is True and receipt['complete'] is True and receipt['stamp']['attempt'] == step['grant']['handle']
              and receipt['stamp']['nonce'] == row['nonce'],
              'host_or_cluster_receipt_unbound')
    if label == 'agentless':
        check(not any(step['kind'] == 'agent_cut' for step in steps), 'agentless_plan_has_agent_step')
    if label == 'brokeronly':
        check(not any(step['kind'] == 'agent_cut' for step in steps)
              and plan['agents_before'] and plan['agents_before'] == plan['agents_after'], 'broker_only_touched_agent')
    if label == 'noop':
        check(sample['source_sha'] == sample['target_sha'] and len(plan['participants']) == len(plan['children']), 'noop_lacks_fresh_full_fleet_proof')
    if label == 'resume':
        check(plan['driver_dead'] is True and plan['driver_before']['pid'] > 0
              and plan['driver_after']['pid'] > 0 and plan['driver_before'] != plan['driver_after'], 'driver_resume_not_observed')
    if label == 'lease':
        check(plan['holder'] and plan['released_holder'] == plan['holder'], 'wrong_holder_released')


def validate(drill, label, row, sample, rc, expected, nonce, begin, end, previous):
    C.schema_check(sample)
    identity(row, sample, expected, nonce, begin, end)
    check(row['label'] == label, 'regression_label_changed')
    (judge31 if drill == '31' else judge33)(label, row, sample, rc, previous)


def manifest_read(path, drill):
    manifest = C.read(path)
    shape(manifest, 'version drill fixture observer cases', 'regression_manifest_schema')
    C.require(type(manifest['version']) is int and manifest['version'] == 1 and manifest['drill'] == drill,
              'regression_manifest_identity')
    if drill == '31':
        shape(manifest['fixture'], ' '.join(LABELS['31']), 'fleet_fixture_scope_missing')
    S.check_spec(manifest['observer'])
    C.require([case['label'] for case in manifest['cases']] == list(LABELS[drill]), 'regression_cases_missing_or_reordered')
    for case in manifest['cases']:
        shape(case, 'label command deadline_s want_rc expect', 'regression_case_schema')
        shape(case['expect'], 'key owner incarnation source_sha target_sha parent_tx_id parent_slot', 'regression_expect_schema')
        S.check_spec(case['command'])
        C.require(type(case['want_rc']) is int and case['want_rc'] in (0, 64, 74, 75, 77), 'regression_exit_invalid')
        C.require(type(case['deadline_s']) is int and 0 < case['deadline_s'] <= 900, 'regression_deadline_invalid')
    return manifest


def cli_start(root, case):
    # origin: hot-update S8 plan §9.4 / L21. Long CLI waits use the shared
    # launcher; the short observer port's 55s bound never truncates a watchdog.
    launcher = os.environ.get('SIM_UPGRADE_SUPERVISOR', '')
    C.require(Path(launcher).is_absolute() and Path(launcher).is_file(), 'shared_cli_supervisor_missing')
    label = case['label']
    command = case['command']
    argv = [launcher, '--log', str(root / ('cli-' + label + '.log')), '--receipt', str(root / ('cli-' + label + '.rc')),
            '--'] + command['argv']
    spec = dict(argv=argv, cwd=command['cwd'], timeout_s=command['timeout_s'])
    owner = S.call(root, dict(op='spawn', id='cli-' + label, spec=spec))
    C.publish(root / ('cli-' + label + '.json'), dict(command=command, owner=owner))
    return label


def cli_wait(root, label, deadline):
    path = root / ('cli-' + label + '.rc')
    while time.monotonic() < deadline:
        if path.exists() and path.read_text().strip():
            status = S.call(root, dict(op='census'))['cli-' + label]
            if status['native_rc'] is not None:
                rc = int(path.read_text().strip())
                check(rc == status['native_rc'], 'cli_supervisor_native_status_changed')
                return rc
        time.sleep(.02)  # Receipt poll; no readiness or completion is inferred.
    raise C.EvidenceError('regression_cli_deadline_exceeded', 4)


def observe(root, fixture, manifest, case, deadline):
    while time.monotonic() < deadline:
        nonce = secrets.token_hex(16)
        sample = C.sample(root, fixture['case_id'], nonce)
        begin = C.boottime_ns()
        row = C.observed_command(root, manifest['observer'], dict(version=1, op='regression_query',
            drill=manifest['drill'], label=case['label'], nonce=nonce, run_id=fixture['run_id']))
        end = C.boottime_ns()
        public_evidence(row)
        C.publish(root / ('regression-' + case['label'] + '-' + nonce + '.json'), row)
        wanted = {'B2': ('staged',), 'B3': ('activating', 'serving_new', 'migrating'), 'B6': ('rolled_back',)}
        if case['label'] not in wanted or sample['phase'] in wanted[case['label']]:
            return row, sample, nonce, begin, end
        # Only the pre-stage phases may be polled. Missing the actual staged
        # window is INCOMPLETE; a completed log is never retroactive acceptance.
        C.require(case['label'] != 'B2' or sample['phase'] == 'prepared', 'staged_window_not_observed', 4)
        time.sleep(.02)  # Poll the actual action; no estimated stage timer.
    raise C.EvidenceError('staged_observation_deadline_exceeded', 4)


def run_fixture(drill, manifest, fixture_path, evidence, cases):
    fixture = F.manifest_read(fixture_path)
    C.require(fixture['arm'] == 'V2', 'regression_fixture_arm_wrong')
    root = Path(evidence)
    reader, writer = os.pipe()
    previous = {}
    started = False
    error = None
    try:
        F.start(root, fixture, reader)
        started = True
        C.publish(root / 'regression-manifest.json', manifest)
        for case in cases:
            deadline = time.monotonic() + case['deadline_s']
            # B2 observes an in-flight staged action. B5 joins that SAME CLI;
            # issuing another upgrade to obtain a verdict would violate dedup.
            if drill == '33' and case['label'] in ('B3', 'B5'):
                check(case['command'] == manifest['cases'][0]['command'], 'rollback_wait_reissued_upgrade')
                cli = 'B2'
            else:
                cli = cli_start(root, case)
            rc = None if drill == '33' and case['label'] in ('B2', 'B3') else cli_wait(root, cli, deadline)
            if rc is not None:
                check(rc == case['want_rc'], 'regression_native_cli_mismatch')
            row, sample, nonce, begin, end = observe(root, fixture, manifest, case, deadline)
            validate(drill, case['label'], row, sample, rc, case['expect'], nonce, begin, end, previous)
            previous[case['label']] = row
        # Natural terminal ACK and two independent history drains precede stop.
        F.drain(root, fixture)
        C.judge(root, fixture)
    except BaseException as caught:
        error = caught
    finally:
        os.close(reader)
        try:
            if started:
                F.stop(root, fixture)
        except BaseException as caught:
            if error is None:
                error = caught
        finally:
            os.close(writer)
            if (root / 'supervisor.json').exists():
                S.wait_empty(root)
    if error:
        raise error
    return dict(rc=0, verdict='GREEN', drill=drill, cases=list(previous))


def run(drill, path, evidence):
    manifest = manifest_read(path, drill)
    if drill == '33':
        return run_fixture(drill, manifest, manifest['fixture'], evidence, manifest['cases'])
    results = [run_fixture(drill, manifest, manifest['fixture'][case['label']], Path(evidence) / case['label'], [case])
               for case in manifest['cases']]
    return dict(rc=0, verdict='GREEN', drill=drill, cases=[item['cases'][0] for item in results])


def main():
    try:
        result = run(sys.argv[2], sys.argv[3], sys.argv[4])
    except C.EvidenceError as error:
        result = dict(rc=error.rc, verdict=C.VERDICTS[error.rc], reason=str(error))
    except (S.SupervisionError, OSError, ValueError, KeyError, TypeError, IndexError):
        result = dict(rc=2, verdict='SETUP-RED', reason='regression_contract_unavailable')
    print(__import__('json').dumps(result, sort_keys=True))
    return result['rc']


if __name__ == '__main__':
    sys.exit(main())
# S8_REGRESSION_PYTHON_END
S8_REGRESSION_PY
}

# The driver port is used by drill 33 without sourcing its legacy setup body.
if [ "${1:-}" = --regression-driver ]; then
    regression_v2 "$2"
    exit "$?"
fi
regression_v1_setup() {
. "$HERE/lib/log.sh"; . "$HERE/lib/docker.sh"; . "$HERE/lib/tether.sh"; . "$HERE/lib/assert.sh"
. "$HERE/lib/secrets.sh"
. "$HERE/drills/lib/agentyaml.sh"; . "$HERE/drills/lib/ident.sh"
. "$HERE/drills/lib/ingress.sh"; . "$HERE/drills/lib/artifact.sh"
. "$HERE/drills/lib/upgradecfg.sh"
SIM="${SIM:-$HERE/simcluster}"
SID=lab; PIN=135790
CA=/usr/local/share/ca-certificates/tether-sim-ca.crt
VALIDSHA=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef   # valid 64-hex, wrong value
WRONGSHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa   # valid 64-hex, deliberately != the real ART_SHA — the #28-FLIPPED sha-gate probe
# EXT-REVIEW-B5 (lint rule `combined-signal-trap`): a single `trap … EXIT INT TERM` is WRONG for INT/TERM —
# a POSIX handler RETURNS to the next command, so a Ctrl-C would reap the artifact and then RESUME the
# upgrade steps below (racing cmd_drill's own nuke). drill_install_traps registers EXIT separately and makes
# INT/TERM exit 128+signo without resuming. Cleanup body unchanged.
_cleanup() { artifact_down 2>/dev/null; }
drill_install_traps _cleanup
CTL() { "$SIM" ctl -- "$@"; }

# labeled operator config: add broker.upgrade.url_allow (install.sh writes NO upgrade block — spike-confirmed).
# Uses python3-yaml so it works regardless of the existing broker.yaml shape; broker.yaml is root-owned (dexec
# defaults to root). This is a DEPLOYMENT config step (Mandate ③), not compensating for tether.
# NB: dexec runs `docker exec` WITHOUT -i, so a heredoc stdin does NOT reach the container. Pass the edit as a
# python3 -c program (one arg). $INSTANCE is interpolated by the drill shell (double-quoted); the code uses
# single quotes only, so there is no quote clash.
# Bodies extracted VERBATIM to drills/lib/upgradecfg.sh (shared with drill 33 — upgrade-safety
# follow-up #2); these thin shells keep 31's call sites and probe wiring unchanged.
_allow_artifact() { upgradecfg_allow_broker brk1 _broker_up; }
# Full Mandate-③ rationale (why yaml is the only lever, sim-user 0600 shape, -artifact vs -mirror prefix
# split) lives with the body in drills/lib/upgradecfg.sh.
_allow_artifact_agent() { upgradecfg_allow_agent agt1 _pre_agt1_owner; }
# after a restart, is-active is NOT enough — the broker must be FUNCTIONALLY ready (leader elected) before
# session create works, or it fails "no active session". Poll the leader view.
_broker_up() { "$SIM" exec brk1 -- tether cluster status --json 2>/dev/null | jq -e '.leader_id=="brk1"' >/dev/null 2>&1; }

# #28-FLIPPED 3-point preconditions: make `sha256_mismatch` the ONLY wall the (a) probe can hit — the request
# must fetch+trust (else download_failed), be broker-whitelisted (else url_not_allowed), and be ONLINE+owner
# (else node_offline/not_owner). With all three, the wrong-sha upgrade can only fail at the agent's digest check.
_pre_reachable() { dexec agt1 -- curl -sf --max-time 8 --cacert "$CA" "$ART_URL" -o /dev/null; }   # (i) fetch+trust
_pre_broker_allows() { dexec brk1 -- grep -q "$INSTANCE-artifact" /etc/tether/broker.yaml; }        # (ii) broker whitelisted
_pre_agt1_owner() { "$SIM" exec brk1 -- runuser -u tether -- tether admin nodes 2>&1 | grep -E 'agt1' | grep -q ONLINE; }  # (iii) ONLINE (broker-authoritative)
_n3_notowner() {   # a joined MEMBER (not owner) is refused node upgrade
    CTLH mem login -s "$SID" --pin "$PIN" --broker nats://brk1:4222 >/dev/null 2>&1
    CTLH mem node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA" 2>&1 | grep -qiE 'not_owner|owner-only|not the owner'
}
# --- Arm FLEET (external-review M4): --all enumeration/dispatch/--timeout, INDEPENDENT of #28's blocked success ---
# `node upgrade --all`'s target set is listOnlineNIDs (node.go:341) = node.list.req filtered to status==ONLINE —
# the SAME RPC + ONLINE filter `node ls` uses (node.go:34). So the ONLINE-only `node ls --json` IS the --all
# target set. assert_ok runs these in a FUNCTION-inheriting subshell (never sh -c).
_ls_online_nids()   { "$SIM" ctl -- node ls --json 2>/dev/null | jq -r '.nodes[]?|select(.status=="ONLINE")|.nid' 2>/dev/null; }
_ls_has_online()    { _ls_online_nids | grep -qx "$1"; }
_ls_not_online()    { ! _ls_online_nids | grep -qx "$1"; }
_ls_offline_stale() { "$SIM" ctl -- node ls -a --json 2>/dev/null | jq -e --arg n "$1" '.nodes[]?|select(.nid==$n and (.status=="OFFLINE" or .status=="STALE"))' >/dev/null 2>&1; }

}

# Keep helpers above ARM and claims inside V1 for both static scanners.
if [ "${1:-}" = --arm ]; then ARM=${2:-}; fi
ARM=${ARM:-V2}
case "${ARM:?}" in
    V1)
        regression_v1_setup
drill_begin "31-node-upgrade-fleet (#28 FIXED: agent allow-list configurable + GREEN broker/agent negatives)"
"$SIM" nuke >/dev/null 2>&1 || true
assert_ok "up 1 broker + 2 agents + 1 ctl"          "$SIM" up --brokers 1 --agents 2 --ctl 1
assert_ok "init brk1 (N=1 cluster)"                  "$SIM" init brk1
# broker.upgrade.url_allow config + restart FIRST — a broker restart AFTER ctl login would invalidate the ctl's
# session (70/72 never restart the broker, so they don't hit this). Do it before session create.
assert_ok "SETUP broker.upgrade.url_allow += artifact prefix (labeled operator config) + broker restart" \
          _allow_artifact
assert_ok "session lab + ctl login (owner)"          "$SIM" session "$SID" --pin "$PIN"
assert_ok "agent-join agt1"                          "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
assert_ok "agent-join agt2"                          "$SIM" agent-join agt2 --session "$SID" --pin "$PIN"
# S0-artifact: serve tether-next as a real https release tarball + host-computed sha (sets ART_URL/ART_SHA).
# Call DIRECTLY (not via assert_ok — its command-sub subshell would discard the ART_URL/ART_SHA globals).
artifact_up "$INSTANCE" "$HERE/vendor/tether-next" "tether-next.tar.gz" || die "31: artifact_up failed"
assert_ok "SETUP artifact serving tether-next (ART_URL/ART_SHA populated)" \
          sh -c "[ -n '$ART_URL' ] && [ -n '$ART_SHA' ]"
assert_ok "SETUP inject instance CA into agt1 trust store (so the fetch is trusted — isolates #28)" \
          ingress_trust_inject agt1
log "31: ART_URL=$ART_URL ART_SHA=$ART_SHA"
# A URL the BROKER whitelists (the `-mirror/` 2nd prefix in _allow_artifact) but the AGENT does NOT (its
# url_allow lists only `-artifact/`): it PASSES the broker gate and is refused at the AGENT's local allow-list.
# The (b) non-vacuity probe + F3's config-abort trigger both use it. Its host need never resolve — the agent
# rejects at the url gate BEFORE any fetch.
AGENT_DENIED_URL="https://$INSTANCE-mirror/x.tar.gz"
# DBG BEFORE the agent allow-list is configured: still the old wall (url_not_allowed_local) — harmless, and it
# proves the flip below is caused by _allow_artifact_agent, not a pre-existing state. Uses the REAL sha, so it
# MUST run before _allow_artifact_agent — afterwards this exact call would pass every gate and RE-EXEC agt1.
log "31-DBG pre-config upgrade (real sha, agent UNconfigured) >>> $("$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "$ART_SHA" 2>&1 | tr '\n' '~')"

# labeled deployment config (Mandate ③): wire the agent's OWN upgrade allow-list. MUST come after the DBG line
# above (real-sha) and before any owner-side upgrade with the real sha, or it would trigger a real re-exec.
assert_ok "SETUP agt1 agent.upgrade.url_allow += artifact prefix (labeled operator config) + agent restart + ONLINE" \
          _allow_artifact_agent

# ── Arm #28 — FIXED (R15): the agent allow-list is now configurable; the CONFIGURED URL passes the agent gate ──
assert_ok "P28-i  precondition: agt1 can FETCH+TRUST the artifact URL (so the flip reaches the sha gate, not download_failed)"  _pre_reachable
assert_ok "P28-ii precondition: broker WHITELISTED the URL (so the request forwards, not broker url_not_allowed)"               _pre_broker_allows
assert_ok "P28-iii precondition: agt1 ONLINE + owner=caller (rules out node_offline/not_owner)"                                 _pre_agt1_owner
assert_refuses "#28 FLIPPED (R15): the CONFIGURED artifact URL PASSES the agent local allow-list — it now fails on the WRONG sha (sha256_mismatch), NOT url_not_allowed_local (was: unconfigurable → always url_not_allowed_local)" \
               "sha256_mismatch" \
               "$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "$WRONGSHA"
assert_refuses "#28 non-vacuity: the agent allow-list is LIVE, not disabled — a broker-whitelisted-but-agent-denied URL (VALID sha) is still refused url_not_allowed_local AT THE AGENT gate (broker-side url_not_allowed would MISS this sig → FAIL LOUD)" \
               "url_not_allowed_local" \
               "$SIM" ctl -- node upgrade agt1 --url "$AGENT_DENIED_URL" --sha256 "$VALIDSHA"

# ── GREEN negatives (independently reachable today) ──────────────────────────────────────────────────
assert_refuses "N1 broker url_not_allowed (off-allowlist URL + VALID sha → not the sha gate)" \
               "url_not_allowed([^_]|\$)" \
               "$SIM" ctl -- node upgrade agt1 --url "https://evil.invalid/x.tar.gz" --sha256 "$VALIDSHA"
assert_refuses "N2 broker sha256_invalid (malformed sha, not 64-lowercase-hex)" \
               "sha256_invalid|sha256|hex" \
               "$SIM" ctl -- node upgrade agt1 --url "$ART_URL" --sha256 "not-a-valid-sha"
assert_ok "N3 not_owner (a joined member, not the session owner, is refused node upgrade)"  _n3_notowner

# ── Arm FLEET (external-review M4) — --all enumeration + dispatch + --timeout (independent of #28) ─────
# F1 — ONLINE enumeration baseline: both agents are in the --all target set (ONLINE).
assert_ok "F1 --all target set (node ls --json ONLINE filter) includes agt1 (ONLINE)"  _ls_has_online agt1
assert_ok "F1b --all target set includes agt2 (ONLINE) — a real 2-node fleet"           _ls_has_online agt2
# F2 — OFFLINE enumeration exclusion: stop agt2's daemon → it drops OUT of the --all target set (listOnlineNIDs
# only picks ONLINE), while agt1 stays. This is the exact exclusion --all applies at dispatch enumeration.
assert_ok "F2-setup stop agt2 daemon → make it OFFLINE/STALE"  "$SIM" exec agt2 -- systemctl stop tether-agent
assert_ok "F2 agt2 goes OFFLINE/STALE in the broker view"     poll_until 45 3 "agt2 OFFLINE/STALE" -- _ls_offline_stale agt2
assert_ok "F2b OFFLINE agt2 is EXCLUDED from the --all target set (listOnlineNIDs ONLINE-only)"  _ls_not_online agt2
assert_ok "F2c ONLINE agt1 stays in the --all target set"     _ls_has_online agt1
# F3 — --all DISPATCHES to the ONLINE target + config-abort: targets only agt1 (agt2 excluded), dispatches, and
# aborts on url_not_allowed_local (a CONFIG error — node.go:221/318 isConfigError aborts the fleet). Trigger is
# AGENT_DENIED_URL (broker-allowed, agent-denied — since #28 is FIXED, $ART_URL would now PASS the agent gate and
# re-exec, so it can no longer be the abort trigger). Proves the fleet enumerated+dispatched (not a no-op) at the
# AGENT gate, aborted, and never dispatched to the OFFLINE agt2. VALID sha so it isn't short-circuited pre-forward.
_allcfg=$("$SIM" ctl -- node upgrade --all --url "$AGENT_DENIED_URL" --sha256 "$VALIDSHA" 2>&1); _allcfg_rc=$?
log "31-DBG --all(agent-denied) rc=$_allcfg_rc out=$(printf '%s' "$_allcfg" | tr '\n' '~')"
assert_ok "F3 --all dispatched to ONLINE agt1 → aborted on the agent's url_not_allowed_local gate (isConfigError aborts the fleet; reached an agent, not a no-op)"  sh -c '[ "$1" -ne 0 ] && printf "%s" "$2" | grep -q "url_not_allowed_local"' _ "$_allcfg_rc" "$_allcfg"
assert_ok "F3b --all did NOT dispatch to the OFFLINE agt2 (enumerated out — the abort names agt1, never agt2)"  sh -c '! printf "%s" "$1" | grep -q "agt2"' _ "$_allcfg"
# F4 — --timeout threading under the CANARY contract (upgrade-safety 增量, node.go runUpgradeAll): the
# FIRST node (lexicographic → agt1) is the canary; ANY canary dispatch failure — transient included —
# ABORTS the fleet with the rest untouched. So `--all --timeout 0` no longer skip-continues across every
# node (the pre-upgrade-safety contract this arm used to pin): the canary's immediately-expired context
# (deadline exceeded = codeless transient) must abort with "canary agt1 failed, 1 node(s) untouched" and
# agt2 must never be dispatched. Transient SKIP-continue still exists but only for the post-canary rest —
# deploy-tier not re-provable here without a healthy canary re-exec (that is drill 33's ground; hermetic:
# test/p10/upgrade_all_test.go TestUpgradeAllSkipsOfflineContinuesNext).
assert_ok "F4-setup restart agt2 daemon"                      "$SIM" exec agt2 -- systemctl start tether-agent
assert_ok "F4-setup2 agt2 back ONLINE (2-node fleet again)"   poll_until 45 3 "agt2 ONLINE" -- _ls_has_online agt2
_online_n=$(_ls_online_nids | grep -c .)
_allto=$("$SIM" ctl -- node upgrade --all --timeout 0 --url "$ART_URL" --sha256 "$ART_SHA" 2>&1); _allto_rc=$?
log "31-DBG --all --timeout 0 rc=$_allto_rc online_n=$_online_n out=$(printf '%s' "$_allto" | tr '\n' '~')"
assert_ok "F4a --all --timeout 0 announces the canary hold-back ('canary: agt1' with the rest held) — proves 2-node enumeration reached the canary split"  sh -c "[ '$_online_n' -ge 2 ] && printf '%s' \"\$1\" | grep -q 'canary: agt1'" _ "$_allto"
assert_ok "F4b canary transient failure ABORTS the fleet non-zero: rc!=0 AND 'canary agt1 failed, 1 node(s) untouched' (a canary that cannot even dispatch must hold the fleet — upgrade-safety plan §5)"  sh -c '[ "$1" -ne 0 ] && printf "%s" "$2" | grep -q "canary agt1 failed, 1 node(s) untouched"' _ "$_allto_rc" "$_allto"
# origin: external review F6 — enumerating two FAILURE states let a regression through: if the fleet
# wrongly dispatched agt2 after the canary aborted, the SUCCESS line `✔ lab/agt2: staged …` matched
# neither "skipped" nor "failed" and the assertion passed. "Untouched" means NO agt2 line of ANY kind.
assert_ok "F4c agt2 was NEVER dispatched (NO line mentioning agt2 at all — a success line must fail this too, not just skipped/failed)"  sh -c '! printf "%s" "$1" | grep -q "agt2"' _ "$_allto"
not_covered "31 (external-review M4, honest — rescoped after drill 33 took single-node ownership): a SUCCESSFUL fleet-wide --all rollout (healthy canary COMMITTED + fan-out + two-node summary)" \
    "DELIBERATELY not exercised here: a real fleet fan-out needs a healthy canary re-exec first, which would REWRITE the shared binary + re-home the 2-node fleet mid-drill, polluting the enumeration/dispatch/timeout arms. Single-node success/PID/version/rollback/--wait is OWNED by drill 33-node-upgrade-success; what remains unowned is only the fleet-wide fan-out after a committed canary. This drill covers the CONTROL surface: agent allow-list flip + non-vacuity (#28 FIXED), ENUMERATION (F1/F2), DISPATCH + config-abort (F3), --timeout + canary-abort (F4)." gap

        ;;
    V2)
        drill_begin "31-node-upgrade-fleet v2"
        regression_v2 31
        _reg_rc=$?
        case "$_reg_rc" in
            0|1) assert_ok "v2 regression receipts (native rc $_reg_rc)" test "$_reg_rc" -eq 0 ;;
            2) assert_setup "v2 regression adapters available" test "$_reg_rc" -eq 0 ;;
            3) product_red "v2 regression product oracle failed; evidence retained" ;;
            4) not_covered "v2 regression observation incomplete" "native interval/receipt missing" runtime-guard ;;
            *) setup_fail "invalid regression driver rc $_reg_rc" ;;
        esac
        ;;
    *)
        drill_begin "31-node-upgrade-fleet"
        setup_fail "unknown regression arm '$ARM'"
        ;;
esac
drill_end
