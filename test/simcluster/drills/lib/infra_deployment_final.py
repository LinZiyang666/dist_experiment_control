"""Compose final originals, measured controls, S7 lanes and S5 raw evidence.

No 'finished' boolean or remote FD number proves SS custody. The original S5
judge runs first in its custodian and again against the local raw inputs and
actual sealed descriptors. Missing inherited custody remains a named NOT_RUN;
P32's later independent host oracle must also retain those descriptors.
origin: hot-update S9 plan §9.1 / S5 §6.6; S9 INSTRUMENTS-5.
"""
import hashlib
from pathlib import Path
import time

import infra_composer_contract as contract
import infra_native as native
import infra_owner_plan as plans
import hot_update_infra_workload as workload

CONTROLS = ('resize', 'signal', 'keepalive_lost_ack', 'input', 'output',
            'ps_victim', 'kill_victim', 'revoke_alice')
FACTS = {'resize': {'seq', 'ack', 'cols', 'rows', 'observed_cols', 'observed_rows'},
         'signal': {'seq', 'ack', 'signal', 'observed_signal', 'target', 'observed_target'},
         'keepalive_lost_ack': {'request_digest', 'sent_ns', 'effect_ns', 'reply_cut_ns'},
         'input': {'sent_sha256', 'applied_sha256', 'sent_bytes', 'applied_bytes'},
         'output': {'produced_sha256', 'received_sha256', 'produced_bytes', 'received_bytes'},
         'ps_victim': {'task_id', 'observed_task_id', 'target', 'observed_target'},
         'kill_victim': {'target', 'waited_target', 'wait_status', 'exited'},
         'revoke_alice': {'revision', 'applied_revision', 'revoked', 'replayed_bytes'}}


def need(ok, reason):
    plans.need(ok, 'readers.final', reason)


def admit(value):
    need(type(value) is dict and set(value) == {'business', 'controls', 'phase_lanes', 'flow'}, 'final_sources_required')
    flow = value['flow']
    need(type(flow) is dict and set(flow) == {'custodian', 'evidence', 'logs'}, 'original_s5_custodian_required')
    need(type(value['phase_lanes']) is dict and (not value['phase_lanes'] or
         set(value['phase_lanes']) == {'writer_lost_ack', 'reader_partial_sync', 'commit_rename', 'terminal_courier'}),
         'complete_phase_lane_sources_required')
    need(type(flow['logs']) is dict and set(flow['logs']) == {'broker', 'agent', 'client', 'target'},
         'complete_s5_log_sources_required')
    for spec in (value['business'], value['controls'], flow['custodian'], *flow['logs'].values(),
                 *value['phase_lanes'].values()):
        contract.spec(spec)
    plans.path(flow['evidence'], 'readers.final')
    return value


def control(event, started_ns, ended_ns):
    need(type(event) is dict and set(event) == {'op', 'at_ns', 'facts'} and
         event['op'] in CONTROLS and contract.probe.uint(event['at_ns'], 1) and
         started_ns <= event['at_ns'] <= ended_ns, 'measured_control_event_required')
    operation, facts = event['op'], event['facts']
    need(type(facts) is dict and set(facts) == FACTS[operation], 'complete_control_facts_required')
    if operation in ('resize', 'signal'):
        need(contract.probe.uint(facts['seq'], 1) and facts['seq'] == facts['ack'], 'committed_control_receipt_required')
    if operation == 'resize':
        need(all(contract.probe.uint(facts[key], 1) and facts[key] <= 65535 for key in ('cols', 'rows')) and
             (facts['cols'], facts['rows']) == (facts['observed_cols'], facts['observed_rows']), 'target_geometry_mismatch')
    elif operation == 'signal':
        need(contract.probe.uint(facts['signal'], 1) and facts['signal'] == facts['observed_signal'] and
             native.process(facts['target']) == native.process(facts['observed_target']), 'target_signal_effect_missing')
    elif operation == 'keepalive_lost_ack':
        need(contract.probe.digest(facts['request_digest']) and
             started_ns <= facts['sent_ns'] <= facts['effect_ns'] <= facts['reply_cut_ns'] <= event['at_ns'],
             'keepalive_three_way_proof_required')
    elif operation in ('input', 'output'):
        a, b = ('sent', 'applied') if operation == 'input' else ('produced', 'received')
        need(contract.probe.uint(facts[a + '_bytes'], 1) and facts[a + '_bytes'] == facts[b + '_bytes'] and
             contract.probe.digest(facts[a + '_sha256']) and facts[a + '_sha256'] == facts[b + '_sha256'],
             'control_bytes_mismatch')
    elif operation == 'ps_victim':
        need(type(facts['task_id']) is str and facts['task_id'] and facts['task_id'] == facts['observed_task_id'] and
             native.process(facts['target']) == native.process(facts['observed_target']), 'victim_ps_identity_mismatch')
    elif operation == 'kill_victim':
        need(facts['exited'] is True and type(facts['wait_status']) is int and facts['wait_status'] != 0 and
             native.process(facts['target']) == native.process(facts['waited_target']), 'victim_native_exit_missing')
    else:
        need(contract.probe.uint(facts['revision'], 1) and facts['revision'] == facts['applied_revision'] and
             facts['revoked'] is True and facts['replayed_bytes'] == 0, 'actual_alice_revocation_required')
    return dict(op=operation, subject='ss-alice' if operation == 'revoke_alice' else
        'victim' if operation in ('ps_victim', 'kill_victim') else 'pty', at_ns=event['at_ns'],
        effect=True, producer='control-target-reader')


def source(plan, ctx, spec, arguments, timeout):
    _, raw = native.invoke(plan, ctx, 'readers.final', spec,
        ['--context', str(Path(ctx['evidence']) / 'context.json'), *arguments], timeout)
    return workload.decode(raw)


def local_flow(root, nonce):
    import hot_update_flow_probe as probe
    root = Path(root)
    document = probe.read_json(root / 'evidence.json')
    need(document.get('nonce') == nonce and document.get('flows') and
         any(row.get('protocol') == 'ss' for row in document['flows']), 'complete_s5_raw_evidence_required')
    # This is deliberately the ORIGINAL oracle. It checks public raw captures,
    # byte ledgers and sealed memfds itself. Opening a remote FD integer here
    # fails; a prior remote native-zero receipt cannot waive this second proof.
    try:
        return probe.judge(root, nonce, 'unchanged')
    except probe.ContinuityError:
        need(False, 's5_native_rc_1')
    except (probe.EvidenceError, OSError, ValueError, KeyError, TypeError):
        need(False, 'original_s5_raw_or_descriptor_custody_required')


def collect(plan, config, ctx, arguments, timeout):
    sources = config.get('final_sources')
    admit(sources)
    # Preserve the custodian's nonzero native result before any final rows can
    # be emitted. invoke still records the original native status and raw hash.
    rc, raw = native.invoke(plan, ctx, 'readers.final', sources['flow']['custodian'], arguments,
                            min(timeout, 120), allowed=(0, 1, 2, 4))
    need(rc == 0, 's5_native_rc_' + str(rc))
    result = workload.decode(raw)
    need(type(result) is dict, 'original_s5_custodian_reply_required')
    need(result.get('rc') == 0, 's5_native_rc_' + str(result.get('rc', 2)))
    root = Path(ctx['evidence'])
    manifest = contract.read(root.parent / 'manifest.json')
    trace = contract.read(root.parent / 'fixture/trace.json')
    need(manifest['nonce'] == ctx['nonce'] and trace['nonce'] == ctx['nonce'] and
         trace['manifest_sha256'] == contract.probe.manifest_digest(manifest), 'final_original_binding')
    business = source(plan, ctx, sources['business'], [], min(timeout, 120))
    from infra_owner_ports import business as public_business
    public_business(business)
    for name, row in business.items():
        original = manifest['originals'][name]
        need(row['identity'] == original['identity'] and row['carrier'] == original['carrier'] and
             row['accepts'] == 1 and row['redials'] == 0 and row['tx'] == row['rx'] and
             row['tx_sha256'] == row['rx_sha256'], 'final_original_or_bytes_changed_' + name)
        need(row.get('native_rc') == 0 and row['fin'] == [True, True], 'final_native_settlement_required_' + name)
    batch = source(plan, ctx, sources['controls'], [], min(timeout, 120))
    need(type(batch) is dict and set(batch) == {'context_sha256', 'events'} and
         batch['context_sha256'] == contract.sha(root / 'context.json'), 'control_context_binding')
    events = batch['events']
    need(type(events) is list and len(events) == len(CONTROLS) and {r['op'] for r in events} == set(CONTROLS),
         'eight_actual_controls_required')
    controls = [control(event, trace['t0_ns'], time.monotonic_ns()) for event in events]
    signal = next(event for event in events if event['op'] == 'signal')
    need(signal['facts']['target'] == manifest['originals']['pty']['identity']['child'], 'control_not_original_pty')
    ps = next(event for event in events if event['op'] == 'ps_victim')
    killed = next(event for event in events if event['op'] == 'kill_victim')
    need(ps['facts']['target'] == killed['facts']['target'] and
         all(killed['facts']['target'] != manifest['originals'][kind]['identity']['child'] for kind in ('pty', 'exec')),
         'victim_not_separate_original')
    need(business['ss-alice'].get('revoked') is True, 'final_alice_not_revoked')
    lanes = [source(plan, ctx, spec, ['events'], min(timeout, 120)) for spec in sources['phase_lanes'].values()]
    need({r['lane'] for r in lanes} == set(manifest['phase_lanes']) and len(lanes) == len(manifest['phase_lanes']),
         'all_original_phase_lanes_required')
    flow = sources['flow']
    need(Path(flow['evidence']) == root / 'flow', 'original_s5_evidence_root_required')
    for name, spec in flow['logs'].items():
        need(name in ('broker', 'agent', 'client', 'target'), 'public_s5_log_name_required')
        # Logs are opaque raw bytes; they are never decoded then re-serialized.
        # Each source is admitted before spawn and retains native status.
        _, log = native.invoke(plan, ctx, 'readers.final', spec, [], min(timeout, 120))
        need(len(log) <= 32 << 20, 'bounded_s5_log_required')
        from infra_deployment_plan import publish
        publish(root / 'flow' / (name + '-log-digest.json'), dict(sha256=hashlib.sha256(log).hexdigest(), length=len(log)))
        destination = root / 'flow' / (name + '.raw.log')
        with destination.open('xb') as stream:
            stream.write(log)
    local_flow(flow['evidence'], ctx['nonce'])
    return dict(business=business, controls=controls, phase_lanes=lanes)
