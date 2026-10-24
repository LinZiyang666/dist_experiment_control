"""Independent synthetic observations for the infrastructure oracle controls.

origin: S10 internal review MAKETEST-GATES F7 / S9 plan §9.2
"""
import copy
import hashlib
from pathlib import Path
import sys

LIB = Path(__file__).resolve().parents[1] / 'drills' / 'lib'
sys.path.insert(0, str(LIB))
import hot_update_infra_probe as probe

def process(number, generation=1, image=None):
    return dict(BootID='hermetic-observer-boot', PID=number, StartTicks=number * 10,
                InvocationID='invocation-' + str(number), ExeDevice=1, ExeInode=number * 100 + generation,
                ExeSHA256=image or hashlib.sha256(str(generation).encode()).hexdigest(), Cgroup='/hermetic')


def socket_row(number, owner=None):
    return dict(NetNS=1, Device=0, Inode=number, Cookie=number, Local='[127.0.0.1]:' + str(10000 + number),
                Remote='[127.0.0.2]:443', Owner=owner or process(70))


def attachment(number, generation=1):
    return dict(Participant='participant-' + str(number), Generation=generation, ServerID='server-' + str(generation),
                CID=generation, URL='wss://node.example:443', Raw=socket_row(number * 100 + generation),
                Ingress=process(500 + generation), HealthGeneration=generation)


def accepted(row):
    raw = copy.deepcopy(row['Raw'])
    raw.update(Local=row['Raw']['Remote'], Remote=row['Raw']['Local'], Owner=copy.deepcopy(row['Ingress']))
    raw['Cookie'] += 10000
    raw['Inode'] += 10000
    return raw


def event(t, kind, at, offset=1):
    return dict(schema=1, kind=kind, process_start=t['ctl']['StartTicks'], boot_id=t['ctl']['BootID'],
                pid=t['ctl']['PID'], generation=1, cut_id='hop-0', transfer_id=t['transfer_id'],
                entry_nonce=t['entry_nonce'], object_version=t['object_version'], leg='leg1', phase='payload',
                event_seq=offset, boottime_ns=at, durable_offset=offset, producer=t['ctl_producer'])


def complete_model(handoff_file, component='nats'):
    """Independent expected model: three hosts, nine hops, all original IDs."""
    t0 = 10_000_000_000
    image_a, image_b = probe.sha(LIB / 'hot_update_infra.sh'), probe.sha(LIB / 'hot_update_infra_probe.py')
    originals = {}
    for i, name in enumerate(probe.ORIGINALS, 1):
        identity = dict(application_nonce='original-' + name)
        if name in ('pty', 'exec'):
            identity.update(ctl=process(20 + i), child=process(40 + i), task_id='task-' + name)
        else:
            identity['socket'] = socket_row(i)
        originals[name] = dict(identity=identity, carrier='carrier-original')
    transfers = []
    for i, name in enumerate(('small-push', 'small-pull', 'large-push', 'large-pull')):
        transfers.append(dict(cohort=name, transfer_id='transfer-' + name, entry_nonce='NUID-' + name,
            session_id='session-' + name, bucket='bucket-' + name, object_version='object-' + name,
            ctl_producer='ctl' + str(i), ctl=process(100 + i), started_ns=t0 - 1,
            size=66549440 if name.startswith('small') else 1 << 30, rate_limit=1024))
    peers = {a['Participant']: a for a in (attachment(1), attachment(2))}
    hops = []
    for stage, action in enumerate(('upgrade', 'rollback', 'upgrade')):
        for host in ('brk1', 'brk2', 'brk3'):
            hops.append(dict(id='hop-' + str(len(hops)), host=host, component=component, action=action,
                             target_sha256=image_a if stage == 1 else image_b, participants=list(peers)))
    source = dict(path=str(handoff_file), sha256=probe.sha(handoff_file))
    manifest = dict(schema='infra-fixture-v1', mode='hermetic', nonce='ab' * 16, seconds=6, profile='cycle',
        carrier_transition='unchanged', originals=originals, attachments=peers, transfers=transfers,
        hops=hops, tls_lanes=list(probe.TLS_LANES) if component == 'caddy' else [],
        s7_handoff=dict(accepted_by='lead', schema='transfer-v1', files={
            name: copy.deepcopy(source) for name in ('executable', 'source', 'schema', 'positive', 'negative')}),
        s7_manifest_sha256='cd' * 32, s7_reader_digest='ef' * 32,
        phase_lanes=dict(writer_lost_ack=transfers[0]['transfer_id'], reader_partial_sync=transfers[1]['transfer_id'],
                         commit_rename='extra-commit', terminal_courier='extra-terminal'))
    trace = dict(schema='infra-trace-v1', nonce=manifest['nonce'], manifest_sha256=probe.manifest_digest(manifest),
                 t0_ns=t0, producer='independent-observer',
                 complete=True, tail_complete=True, slots=[], effects=[], samples=[], controls=[], tls=[],
                 flow_native_rc=0, s7_final=dict(native_rc=0, manifest_sha256='cd' * 32,
                 reader_digest='ef' * 32, oracle_input_sha='12' * 32), phase_lanes=[])
    for ordinal in range(60):
        scheduled, logical = t0 + ordinal * 100_000_000, manifest['nonce'] + '-' + str(ordinal)
        kind = ('node_ls', 'ps', 'read', 'write', 'short_exec')[ordinal % 5]
        trace['slots'].append(dict(ordinal=ordinal, logical_id=logical, kind=kind, scheduled_ns=scheduled,
            dispatched_ns=scheduled + 1, terminal_ns=scheduled + 10, native_rc=0, validated=True, attempts=[dict(
            logical_id=logical, ordinal=1, phase='request', started_mono_ns=scheduled + 2,
            finished_mono_ns=scheduled + 9, sent=True, **{'class': 'ok'}, backoff_ns=0)]))
        if kind in ('write', 'short_exec'):
            trace['effects'].append(dict(logical_id=logical, effect='applied'))
        for lane in manifest['tls_lanes']:
            accepted_tls = socket_row(900 + ordinal)
            local, remote = '127.0.0.9:' + str(20000 + ordinal), '127.0.0.2:443'
            accepted_tls.update(Local=remote, Remote=local)
            trace['tls'].append(dict(ordinal=ordinal, lane=lane, scheduled_ns=scheduled,
                started_ns=scheduled + 1, terminal_ns=scheduled + 10, success=True, attempts=1,
                accepted=accepted_tls, local=local, remote=remote, bypass=False))
    running = {host: process(200 + i, image=image_a) for i, host in enumerate(('brk1', 'brk2', 'brk3'))}
    current_peers = copy.deepcopy(peers)
    for hop in hops:
        before = copy.deepcopy(running[hop['host']])
        current = process(300 + int(hop['id'].split('-')[1]), image=hop['target_sha256'])
        for phase in ('before', 'moving', 'cutting', 'verifying', 'returning', 'retired'):
            index = len(trace['samples']) + 1
            at = t0 + index * 100_000_000
            business = {}
            for name, base in originals.items():
                business[name] = dict(identity=copy.deepcopy(base['identity']), accepts=1, redials=0,
                    tx=index * 100, rx=index * 100, tx_sha256='12' * 32, rx_sha256='12' * 32,
                    fin=[False, False], carrier=base['carrier'])
                if name == 'ss-alice' and index >= 2:
                    business[name]['revoked'] = True
            if phase in ('moving', 'returning'):
                for i, participant in enumerate(current_peers, 1):
                    current_peers[participant] = attachment(i, current_peers[participant]['Generation'] + 1)
                    if phase == 'returning' and component == 'caddy':
                        current_peers[participant]['Ingress'] = copy.deepcopy(current)
            snapshot = dict(setup=[], failures=[], processes={t['ctl_producer']: dict(pid=t['ctl']['PID'],
                process_start=t['ctl']['StartTicks'], boot_id=t['ctl']['BootID'], running_sha=t['ctl']['ExeSHA256'])
                for t in transfers}, events=[event(t, 'synced', at, index * 1024) for t in transfers])
            prefix = [dict(transfer_id=t['transfer_id'], at_ns=at - 1, before_repair_ns=at,
                expected=[dict(index=0, length=index * 1024, sha256='34' * 32)],
                actual=[dict(index=0, length=index * 1024, sha256='34' * 32)]) for t in transfers]
            row = dict(hop=hop['id'], phase=phase, at_ns=at, producer='kernel-and-target-reader',
                business=business, s7=snapshot, prefix_reads=prefix, attachments=copy.deepcopy(current_peers),
                accepted={p: accepted(a) for p, a in current_peers.items()})
            if phase == 'retired':
                running[hop['host']] = current
                row.update(before=before, current=current, temporary=[process(400 + index)],
                    census=dict(producer='kernel-reader', complete=True, generation=index,
                        processes=copy.deepcopy(list(running.values())), live_business=12, old_owner_pending=0),
                    return_verified=True, root_revision=index, transaction_revision=index,
                    started_ns=at - 500_000_000, finished_ns=at, main_ready=socket_row(901, current))
            trace['samples'].append(row)
    for name in ('resize', 'signal', 'keepalive_lost_ack', 'input', 'output', 'ps_victim', 'kill_victim', 'revoke_alice'):
        trace['controls'].append(dict(op=name, subject='ss-alice' if name == 'revoke_alice' else
            'victim' if name in ('ps_victim', 'kill_victim') else 'pty', at_ns=t0 + 1,
            producer='control-target-reader', effect=True))
    trace['final_business'] = copy.deepcopy(trace['samples'][-1]['business'])
    for row in trace['final_business'].values():
        row.update(fin=[True, True], native_rc=0)
    trace['progress'] = [copy.deepcopy({k: sample[k] for k in ('at_ns', 'producer', 'business', 's7')})
                         for sample in trace['samples']]
    at = trace['samples'][2]['at_ns']
    for name, tid in manifest['phase_lanes'].items():
        t = next((transfer for transfer in transfers if transfer['transfer_id'] == tid), dict(transfers[0], transfer_id=tid))
        kind = {'writer_lost_ack': 'append', 'reader_partial_sync': 'synced',
                'commit_rename': 'primitive_exit', 'terminal_courier': 'history'}[name]
        first, second = event(t, kind, at - 1), event(t, 'freeze', at + 1, 2)
        if name == 'writer_lost_ack':
            first['actual_ack_seq'] = 10
        elif name == 'reader_partial_sync':
            first.update(durable_offset=10, fetched_offset=20)
        elif name == 'commit_rename':
            first['outcome'] = 'landed'
            second.update(kind='directory_sync', parent_synced=True)
        else:
            first['native_outcome'] = 0
        trace['phase_lanes'].append(dict(lane=name, hop='hop-0', events=[first, second],
                                        sent=True, effect=True, reply_cut=True))
    return manifest, trace


