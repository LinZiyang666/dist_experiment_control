"""Observed JS group faults and original compound injector windows.

Monitor reads use the actual node namespace and verify varz's PID against P02.
Network installation retains the existing fault library. N2 topology changes
and compound injectors need real pinned deployment commands: a partition does
not reduce the configured replica count, and a successful command is not a hit.
origin: hot-update S9 plan §3.2 / §4.2 / §5.3 / §5.4; S9 INSTRUMENTS-5.
"""
import hashlib
from pathlib import Path
import time
from urllib.parse import urlsplit

import infra_composer_contract as contract
import infra_native as native
import infra_owner_plan as plans
import hot_update_infra_workload as workload

NETWORK = ('nats-n2', 'nats-lost-quorum', 'nats-stream-lag', 'nats-consumer-lag')
COMPOUND = ('after-ldm-reply-cut', 'acme-late-write-final-copy-over-2s',
            'main-failure-controller-death-shadow-over-120s')


def need(ok, reason):
    plans.need(ok, 'faults', reason)


def receipt(point, evidence, active):
    return dict(point=point, active=active, hits=len(evidence), sent=False, effect=False,
        reply_cut=False, digest=hashlib.sha256(contract.canonical(evidence)).hexdigest() if evidence else None)


def peers(cluster):
    need(type(cluster) is dict and type(cluster.get('leader', '')) is str,
         'complete_group_view_required')
    # Stock JSz omits meta replicas on followers and omits replicas for R1.
    # Absence is unknown, not an empty (and therefore healthy) replica roster.
    if 'replicas' not in cluster:
        return None
    need(type(cluster['replicas']) is list, 'complete_group_view_required')
    names = []
    for row in cluster['replicas']:
        need(type(row) is dict and type(row.get('name')) is str and bool(row['name']) and
             type(row.get('current')) is bool and type(row.get('offline', False)) is bool and
             contract.probe.uint(row.get('lag', 0)), 'complete_peer_view_required')
        names.append(row['name'])
    need(len(set(names)) == len(names) and cluster.get('leader', '') not in names,
         'distinct_group_members_required')
    return cluster['replicas']


def network_status(point, documents):
    need(point in NETWORK and type(documents) is list and 0 < len(documents) <= 3, 'actual_monitor_views_required')
    evidence, sizes = [], set()
    for doc in documents:
        need(type(doc) is dict and type(doc.get('server_id')) is str and doc['server_id'] and
             not doc.get('disabled', False) and type(doc.get('account_details')) is list and
             all(contract.probe.uint(doc.get(key)) for key in ('streams', 'consumers', 'total')),
             'complete_js_view_required')
        meta = doc.get('meta_cluster')
        need(type(meta) is dict and contract.probe.uint(meta.get('cluster_size'), 2), 'actual_meta_membership_required')
        size = meta['cluster_size']; sizes.add(size)
        need(size in (2, 3), 'supported_meta_membership_required')
        replicas = peers(meta)
        need(replicas is None or len(replicas) <= size, 'meta_membership_conflict')
        if point == 'nats-n2' and size == 2:
            evidence.append(dict(server_id=doc['server_id'], meta=meta))
        if point == 'nats-lost-quorum' and replicas is not None and not meta.get('leader') and \
                sum(not row['current'] for row in replicas) >= size // 2 + 1:
            evidence.append(dict(server_id=doc['server_id'], meta=meta))
        streams = [stream for account in doc['account_details'] for stream in account.get('stream_detail', [])]
        # JSInfo.Total counts accounts, not streams (stock 2.14.5/2.14.6).
        need(len(streams) == doc['streams'] and len(doc['account_details']) == doc['total'], 'truncated_stream_view')
        consumers = []
        for stream in streams:
            need(type(stream.get('name')) is str and type(stream.get('consumer_detail')) is list,
                 'complete_consumer_view_required')
            cluster = stream.get('cluster')
            replicas = peers(cluster)
            if point == 'nats-stream-lag' and replicas is not None and any(not p['current'] or p.get('lag', 0) > 0 for p in replicas):
                evidence.append(dict(server_id=doc['server_id'], stream=stream['name'], cluster=cluster))
            if point == 'nats-lost-quorum' and replicas and not cluster.get('leader') and \
                    len(replicas) == stream.get('config', {}).get('num_replicas') and \
                    sum(p['current'] for p in replicas) < len(replicas) // 2 + 1:
                evidence.append(dict(server_id=doc['server_id'], stream=stream['name'], cluster=cluster))
            for consumer in stream['consumer_detail']:
                need(type(consumer.get('name')) is str and bool(consumer['name']), 'consumer_identity_required')
                replicas = peers(consumer.get('cluster'))
                if point == 'nats-consumer-lag' and replicas is not None and any(not p['current'] or p.get('lag', 0) > 0 for p in replicas):
                    evidence.append(dict(server_id=doc['server_id'], stream=stream['name'],
                                         consumer=consumer['name'], cluster=consumer['cluster']))
                if point == 'nats-lost-quorum' and replicas and not consumer['cluster'].get('leader') and \
                        len(replicas) == consumer.get('config', {}).get('num_replicas') and \
                        sum(p['current'] for p in replicas) < len(replicas) // 2 + 1:
                    evidence.append(dict(server_id=doc['server_id'], stream=stream['name'],
                                         consumer=consumer['name'], cluster=consumer['cluster']))
                consumers.append(consumer)
        need(len(consumers) == doc['consumers'], 'truncated_consumer_view')
    need(len(sizes) == 1, 'inconsistent_meta_membership')
    return receipt(point, evidence, bool(evidence))


def compound_status(point, events, now_ns):
    need(point in COMPOUND and type(events) is list and len(events) <= 4096, 'original_injector_events_required')
    digests = set()
    for event in events:
        need(type(event) is dict and set(event) == {'point', 'request_digest', 'sent_ns', 'effect_ns',
             'reply_cut_ns', 'facts'} and event['point'] == point and
             contract.probe.digest(event['request_digest']) and event['request_digest'] not in digests and
             all(contract.probe.uint(event[key], 1) for key in ('sent_ns', 'effect_ns', 'reply_cut_ns')) and
             event['sent_ns'] <= event['effect_ns'] <= event['reply_cut_ns'] <= now_ns,
             'original_request_effect_reply_cut_required')
        digests.add(event['request_digest'])
        facts = event['facts']
        if point == 'after-ldm-reply-cut':
            need(set(facts) == {'ldm_applied_ns'} and facts['ldm_applied_ns'] == event['effect_ns'],
                 'actual_ldm_effect_required')
        elif point == 'acme-late-write-final-copy-over-2s':
            need(set(facts) == {'warm_copy_finished_ns', 'old_writer_exit_ns', 'late_write_ns', 'copy_started_ns', 'copy_finished_ns',
                 'written_sha256', 'copied_sha256'} and
                 all(contract.probe.uint(facts[key], 1) for key in ('warm_copy_finished_ns', 'old_writer_exit_ns',
                     'late_write_ns', 'copy_started_ns', 'copy_finished_ns')) and
                 event['effect_ns'] <= facts['warm_copy_finished_ns'] < facts['late_write_ns'] <=
                 facts['old_writer_exit_ns'] <= facts['copy_started_ns'] < facts['copy_finished_ns'] <= now_ns and
                 facts['copy_finished_ns'] - facts['copy_started_ns'] > 2_000_000_000 and
                 contract.probe.digest(facts['written_sha256']) and facts['written_sha256'] == facts['copied_sha256'],
                 'late_write_and_final_copy_window_required')
        else:
            need(set(facts) == {'main_failure_ns', 'controller_death_ns', 'main_wait', 'controller_wait', 'shadow', 'samples'} and
                 event['effect_ns'] == facts['main_failure_ns'] <= facts['controller_death_ns'],
                 'main_failure_and_controller_death_required')
            shadow = native.process(facts['shadow'])
            for key, stamp in (('main_wait', 'main_failure_ns'), ('controller_wait', 'controller_death_ns')):
                waited = facts[key]
                need(type(waited) is dict and set(waited) == {'process', 'at_ns', 'wait_status', 'reaped'} and
                     native.process(waited['process']) != shadow and waited['at_ns'] == facts[stamp] and
                     waited['reaped'] is True and type(waited['wait_status']) is int and waited['wait_status'] != 0,
                     'original_native_failure_wait_required')
            need(facts['main_wait']['process'] != facts['controller_wait']['process'], 'distinct_failed_owners_required')
            samples = facts['samples']
            need(type(samples) is list and 122 <= len(samples) <= 4096 and
                 all(type(s) is dict and set(s) == {'at_ns', 'process', 'serving'} and
                     contract.probe.uint(s['at_ns'], 1) and s['process'] == shadow and s['serving'] is True for s in samples),
                 'continuous_original_shadow_samples_required')
            need(facts['controller_death_ns'] <= samples[0]['at_ns'] and samples[-1]['at_ns'] <= now_ns and
                 samples[-1]['at_ns'] - samples[0]['at_ns'] > 120_000_000_000 and
                 all(0 < b['at_ns'] - a['at_ns'] <= 1_000_000_000 for a, b in zip(samples, samples[1:])),
                 'shadow_over_120s_required')
    result = receipt(point, events, bool(events))
    if events:
        result.update(sent=True, effect=True, reply_cut=True)
    return result


def monitor(plan, config, ctx, node):
    url = config.get('nats_monitors', {}).get(node)
    need(type(url) is str, 'node_monitor_precondition_required')
    parsed = urlsplit(url)
    need(parsed.scheme == 'http' and parsed.hostname in ('127.0.0.1', '::1') and parsed.port and
         not parsed.username and not parsed.password and parsed.path in ('', '/') and
         not parsed.query and not parsed.fragment, 'local_public_monitor_required')
    before = native.physical(plan, config, node, 'unit-process', unit=config['nodes'][node]['units']['nats'])
    # The bounded remote program is a read, not an operator-created monitor or
    # credential. Old N1 without a monitor remains unsupported for these faults.
    script = '''import json,sys,urllib.request
base=sys.argv[1].rstrip('/')
def read(path):
 with urllib.request.urlopen(base+path,timeout=3) as response:
  raw=response.read((4<<20)+1)
  if len(raw)>4<<20: raise ValueError('monitor bound')
  return json.loads(raw)
print(json.dumps(dict(varz=read('/varz'),js=read('/jsz?accounts=true&streams=true&consumers=true&config=true&raft=true&limit=4096'))))
'''
    raw = native.remote(plan, config, node, ['/usr/bin/python3', '-I', '-B', '-c', script, url])
    value = workload.decode(raw)
    after = native.physical(plan, config, node, 'process', pid=before['process']['PID'])
    need(before['process'] == after and value['varz']['pid'] == after['PID'] and
         value['varz']['server_id'] == value['js']['server_id'], 'monitor_not_executing_main')
    return value['js']


def status(plan, config, ctx, point):
    need(point in NETWORK + COMPOUND, 'declared_fault_required')
    if point in NETWORK:
        return network_status(point, [monitor(plan, config, ctx, node) for node in ctx['nodes']])
    source = config.get('fault_events', {}).get(point)
    need(type(source) is dict and 'argv' in source, 'original_injector_export_required_' + point)
    _, raw = native.invoke(plan, ctx, 'faults.' + point + '.status', source,
        ['--context', str(Path(ctx['evidence']) / 'context.json')], 10)
    batch = workload.decode(raw)
    need(type(batch) is dict and set(batch) == {'context_sha256', 'point', 'events'} and
         batch['context_sha256'] == contract.sha(Path(ctx['evidence']) / 'context.json') and
         batch['point'] == point, 'injector_context_binding')
    return compound_status(point, batch['events'], time.monotonic_ns())


def mechanism(plan, config, ctx, point, operation, arguments, timeout):
    need(point in NETWORK + COMPOUND and operation in ('install', 'heal'), 'declared_fault_action_required')
    spec = config.get('fault_actions', {}).get(point, {}).get(operation)
    if spec is None and point in NETWORK[1:]:
        from infra_native_actions import network_spec
        spec = network_spec(plan, point, operation, arguments)
        _, raw = native.invoke(plan, ctx, 'faults.' + point + '.' + operation, spec, [], timeout)
        from infra_owner_ports import public
        return public('faults.' + point + '.' + operation, workload.decode(raw))
    need(type(spec) is dict and 'argv' in spec, 'native_mechanism_required_' + point)
    # Only the actual pinned sim transport may execute a supplied topology or
    # compound action. A JSON receipt generator cannot claim installation.
    need(spec['argv'] and spec['argv'][0] == config['sim']['path'], 'real_deployment_transport_required')
    native.invoke(plan, ctx, 'faults.' + point + '.' + operation, spec, [], timeout)
    if point == 'nats-n2':
        observed = status(plan, config, ctx, point)
        need(observed['active'] == (operation == 'install'), 'topology_postcondition_required')
    if operation == 'install':
        return dict(point=point, installed=True, nodes=ctx['nodes'], at_ns=time.monotonic_ns(),
                    mechanism='native:' + point)
    return dict(point=point, healed=True, topology='quorum', nodes=ctx['nodes'], at_ns=time.monotonic_ns())
