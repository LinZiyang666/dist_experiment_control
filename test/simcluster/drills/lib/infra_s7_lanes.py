#!/usr/bin/env -S python3 -I -B
"""Export phase bindings/events from the original S7 custodian.

Bindings select a cohort that is already running, retaining its actual NUID,
ctl and entry nonce. Events are never generated from the planned hop times.
A writer lost-ACK lane additionally needs the injector's real three-way proof;
an observed append alone cannot establish that its reply was cut.
origin: hot-update S9 plan §9.1 / P19/P32; S9 INSTRUMENTS-4.
"""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import infra_composer_contract as contract
import infra_owner_plan as plans
import hot_update_infra_probe as probe

LANES = ('writer_lost_ack', 'reader_partial_sync', 'commit_rename', 'terminal_courier')


def original_rows(original, snapshot):
    plans.need(not snapshot.get('failures'), 's7.phase_lanes', 'original_custody_failed')
    _, rows = probe.s7_checkpoint(snapshot, original)
    plans.need(bool(rows), 's7.phase_lanes', 'original_events_required')
    return rows


def binding(originals, snapshot, cohort):
    matched = [row for row in originals if row['cohort'] == cohort]
    plans.need(len(matched) == 1, 's7.phase_lanes', 'one_original_cohort_required')
    original_rows(matched[0], snapshot)
    return dict(transfer_id=matched[0]['transfer_id'])


def project(lane, hop, cut_ns, original, snapshot, proof=None):
    plans.need(lane in LANES and type(hop) is str and hop.startswith('hop-') and
               probe.uint(cut_ns, 1), 's7.phase_lanes', 'observed_cut_required')
    own = original_rows(original, snapshot)
    plans.need(any(row['boottime_ns'] <= cut_ns for row in own) and
               any(row['boottime_ns'] >= cut_ns for row in own), 's7.phase_lanes', 'original_did_not_cross_cut')
    sent = effect = reply_cut = False
    if lane == 'writer_lost_ack':
        plans.need(type(proof) is dict and set(proof) == {'sent', 'effect', 'reply_cut'} and
                   all(proof[key] is True for key in proof), 's7.phase_lanes', 'lost_ack_three_way_proof_required')
        plans.need(any(row['kind'] == 'append' and row.get('actual_ack_seq', 0) > 0 for row in own),
                   's7.phase_lanes', 'original_append_ack_required')
        sent, effect, reply_cut = proof['sent'], proof['effect'], proof['reply_cut']
    elif lane == 'reader_partial_sync':
        plans.need(any(row['kind'] == 'synced' and 0 < row.get('durable_offset', 0) <
                       row.get('fetched_offset', 0) for row in own), 's7.phase_lanes', 'partial_reader_sync_required')
    elif lane == 'commit_rename':
        plans.need(any(row['kind'] == 'primitive_exit' and row.get('outcome') == 'landed' for row in own) and
                   any(row['kind'] == 'directory_sync' and row.get('parent_synced') is True for row in own),
                   's7.phase_lanes', 'durable_rename_required')
    else:
        plans.need(any(row['kind'] == 'history' and row.get('native_outcome') == 0 for row in own),
                   's7.phase_lanes', 'native_terminal_history_required')
    # Keep the exact S7 rows, including their producer labels and timestamps.
    # The independent P32 judge revalidates the complete event wire schema.
    events = [row for row in snapshot['events'] if row['transfer_id'] == original['transfer_id']]
    return dict(lane=lane, hop=hop, events=events, sent=sent, effect=effect, reply_cut=reply_cut)


def read_originals(ctx):
    from infra_transfer_fixture import paths, call, custody_call
    import transfer_fixture as upstream
    import hot_update_infra_workload as workload
    manifest, evidence = paths(ctx)
    ready_path = Path(ctx['evidence']) / 'ready.json'
    retained = contract.read(ready_path) if ready_path.exists() else None
    response = (custody_call if retained is not None else call)(manifest, evidence, 'sample')
    plans.need(response.get('native_rc') == 0 and response.get('snapshot') == str(evidence / 'snapshot.json'),
               's7.phase_lanes', 'original_sample_failed')
    snapshot = contract.read(evidence / 'snapshot.json')
    plans.need(snapshot.get('manifest_sha256') == contract.sha(manifest), 's7.phase_lanes', 'manifest_changed')
    if retained is not None:
        # Terminal success can outlive the native ctl PID. Preserve READY's
        # original identity and compare it with the custodian's frozen owner;
        # a new /proc read would either fail or inspect a reused PID.
        plans.need(retained.get('schema') == 'infra-cohort-v1' and retained.get('phase') == 'ready' and
                   retained.get('producer') == 'composer' and
                   retained.get('s7_manifest_sha256') == snapshot['manifest_sha256'] and
                   retained.get('s7') == dict(manifest=str(manifest), evidence=str(evidence)),
                   's7.phase_lanes', 'ready_original_binding')
        originals = retained['transfers']
        plans.need(len(originals) == 4 and {row['cohort'] for row in originals} == set(probe.COHORTS),
                   's7.phase_lanes', 'four_ready_originals_required')
        for original in originals:
            original_rows(original, snapshot)
        return originals, snapshot
    value = upstream.read_manifest(manifest)
    originals = []
    for transfer in value['transfers']:
        owner = snapshot['processes'][transfer['ctl_producer']]
        row = dict(transfer, ctl=workload.process_identity(owner['pid'], transfer['transfer_id']))
        original_rows(row, snapshot)
        originals.append(row)
    return originals, snapshot


def spec(lane, cohort, hop='hop-0'):
    plans.need(lane in LANES and cohort in probe.COHORTS, 's7.phase_lanes', 'lane_cohort_required')
    python = str(Path('/usr/bin/python3').resolve())
    # Include imported consumers as well as the command trampoline. The supply
    # closure checker must see a changed upstream event decoder before launch.
    sources = [contract.pin(path) for path in sorted(plans.LIB.iterdir())
               if path.is_file() and path.suffix in ('.py', '.sh')]
    return dict(argv=[python, '-I', '-B', str(Path(__file__).resolve()), '--lane', lane,
                     '--cohort', cohort, '--hop', hop], sha256=contract.sha(python), inputs=sources)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('binding', 'events'))
    parser.add_argument('--context', required=True, type=Path)
    parser.add_argument('--lane', required=True, choices=LANES)
    parser.add_argument('--cohort', required=True, choices=probe.COHORTS)
    parser.add_argument('--hop', default='hop-0')
    parser.add_argument('--lost-ack-proof', type=Path)
    args = parser.parse_args()
    try:
        from infra_owner_ports import context
        ctx = context(args.context)
        originals, snapshot = read_originals(ctx)
        result = binding(originals, snapshot, args.cohort)
        if args.operation == 'events':
            root = Path(ctx['evidence']).parent
            manifest = contract.read(root / 'manifest.json')
            trace = contract.read(root / 'fixture/trace.json')
            plans.need(manifest['s7_manifest_sha256'] == snapshot['manifest_sha256'] and
                       trace['manifest_sha256'] == probe.manifest_digest(manifest) and
                       trace['nonce'] == manifest['nonce'] and
                       manifest['phase_lanes'].get(args.lane) == result['transfer_id'],
                       's7.phase_lanes', 'original_trace_binding')
            cuts = [row for row in trace['samples'] if row['hop'] == args.hop and row['phase'] == 'cutting']
            plans.need(len(cuts) == 1 and cuts[0]['producer'] == 'kernel-and-target-reader',
                       's7.phase_lanes', 'one_observed_cut_required')
            proof = None
            if args.lane == 'writer_lost_ack':
                plans.need(args.lost_ack_proof is not None, 's7.phase_lanes', 'lost_ack_injector_receipt_required')
                proof = contract.read(args.lost_ack_proof)
                plans.need(set(proof) == {'context_sha256', 'transfer_id', 'hop', 'sent', 'effect', 'reply_cut'} and
                           proof['context_sha256'] == contract.sha(args.context) and
                           proof['transfer_id'] == result['transfer_id'] and proof['hop'] == args.hop,
                           's7.phase_lanes', 'lost_ack_receipt_binding')
                proof = {key: proof[key] for key in ('sent', 'effect', 'reply_cut')}
            original = next(row for row in originals if row['transfer_id'] == result['transfer_id'])
            result = project(args.lane, args.hop, cuts[0]['at_ns'], original, snapshot, proof)
        sys.stdout.buffer.write(contract.canonical(result))
        return 0
    except (ValueError, OSError, KeyError, TypeError):
        print('NOT_RUN s7.phase_lanes: original_custody_or_phase_proof_unavailable', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
