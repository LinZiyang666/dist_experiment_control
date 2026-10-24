#!/usr/bin/env -S python3 -I -B
"""Hermetic adversarial controls for the S9 fixture, never deployment receipts.

Synthetic protocol records exercise the same schema/judge used by LIVE. No
binary is advertised as an S7/S9 capable product and no deployment is started.
origin: hot-update S9 plan §9.2 / §12 P32 / T76 / T77 / M30
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import shutil
import sys
import tempfile
import threading
import time
import unittest

LIB = Path(__file__).resolve().parents[1] / 'drills' / 'lib'
sys.path.insert(0, str(LIB))
import hot_update_infra_probe as probe
import hot_update_infra_workload as workload


# The expected observations are shared fixtures, separate from their assertions.
# origin: S10 internal review MAKETEST-GATES F7
sys.path.insert(0, str(Path(__file__).resolve().parent))
from hot_update_infra_probe_fixtures import process, socket_row, attachment, accepted, event, complete_model


class InfraProbeTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='tether-infra-oracle-')
        self.addCleanup(self.directory.cleanup)
        self.handoff = Path(self.directory.name) / 'synthetic-handoff'
        self.handoff.write_bytes(b'hermetic schema model, not a production handoff\n')
        self.manifest, self.trace = complete_model(self.handoff)

    # origin: hot-update S9 plan §9.2 / T76
    def test_infra_complete_trace_passes(self):
        for component in ('nats', 'caddy'):
            with self.subTest(component=component):
                m, trace = complete_model(self.handoff, component)
                result = probe.judge(m, trace)
                self.assertEqual(result['native_rc'], 0, result)
                self.assertEqual(result['hops'], 9)
                self.assertEqual(result['slots'], 60)

    # origin: hot-update S9 plan §9.2 / T77 / M30
    def test_infra_missing_slot_or_original_fails(self):
        def sample(trace):
            return trace['samples'][1]
        def retired(trace):
            return trace['samples'][5]
        def url_only_move(manifest, trace):
            new = sample(trace)['attachments']['participant-1']
            new['Raw'] = copy.deepcopy(manifest['attachments']['participant-1']['Raw'])
            sample(trace)['accepted']['participant-1'] = accepted(new)
        mutations = {
            'empty-original-set': lambda m, t: m.update(originals={}),
            'missing-slot': lambda m, t: t['slots'].pop(0),
            'duplicate-slot': lambda m, t: t['slots'].append(copy.deepcopy(t['slots'][0])),
            'late-dispatch': lambda m, t: t['slots'][0].update(dispatched_ns=t['slots'][0]['scheduled_ns'] + 6_000_000_000),
            'clock-started-budget': lambda m, t: t['slots'][0].update(terminal_ns=t['slots'][0]['scheduled_ns'] + 6_000_000_000),
            'hidden-auth-attempt': lambda m, t: t['slots'][0].update(attempts=[]),
            'duplicate-write-effect': lambda m, t: t['effects'].append(copy.deepcopy(t['effects'][0])),
            'missing-write-effect': lambda m, t: t['effects'].pop(0),
            'automatic-redial': lambda m, t: sample(t)['business']['tcp-active'].update(redials=1),
            'replaced-original-fd': lambda m, t: sample(t)['business']['tcp-active']['identity']['socket'].update(Cookie=999),
            'accepted-twice': lambda m, t: sample(t)['business']['ssh'].update(accepts=2),
            'replaced-pty-child': lambda m, t: sample(t)['business']['pty']['identity']['child'].update(PID=999),
            'carrier-rebuild': lambda m, t: sample(t)['business']['ss-bob'].update(carrier='replacement'),
            'repaired-prefix-hides-loss': lambda m, t: sample(t)['prefix_reads'][0].update(actual=[]),
            'prefix-read-after-repair': lambda m, t: sample(t)['prefix_reads'][0].update(before_repair_ns=1),
            'prefix-not-read': lambda m, t: sample(t)['prefix_reads'].pop(0),
            'disk-only-sha': lambda m, t: t['samples'][-1]['current'].update(ExeSHA256='ff' * 32),
            'old-pid-still-alive': lambda m, t: retired(t)['census']['processes'].append(copy.deepcopy(retired(t)['before'])),
            'temporary-still-alive': lambda m, t: retired(t)['census']['processes'].append(copy.deepcopy(retired(t)['temporary'][0])),
            'pending-old-owner': lambda m, t: retired(t)['census'].update(old_owner_pending=1),
            'journal-disagrees': lambda m, t: retired(t).update(root_revision=1),
            'url-only-move': url_only_move,
            'wrong-accepted-pid': lambda m, t: sample(t)['accepted']['participant-1'].update(Owner=process(999)),
            'missing-data-nc': lambda m, t: sample(t)['attachments'].pop('participant-2'),
            'half-trace': lambda m, t: t.update(tail_complete=False),
            'missing-hop': lambda m, t: t['samples'].__delitem__(slice(6, 12)),
            'wrong-final-nonce': lambda m, t: t['final_business']['ssh']['identity'].update(application_nonce='replacement'),
            'fin-missing': lambda m, t: t['final_business']['tcp-half-close'].update(fin=[True, False]),
            'control-no-effect': lambda m, t: t['controls'][0].update(effect=False),
            'kill-original-task': lambda m, t: t['controls'][6].update(subject='exec'),
            'only-final-sha': lambda m, t: t.update(phase_lanes=[]),
            'lost-ack-not-hit': lambda m, t: t['phase_lanes'][0].update(reply_cut=False),
            's7-reader-changed': lambda m, t: t['s7_final'].update(reader_digest='aa' * 32),
        }
        only = os.environ.get('INFRA_MUTATION_CASE')
        for name, edit in mutations.items():
            if only and name != only:
                continue
            with self.subTest(mutation=name):
                manifest, trace = copy.deepcopy(self.manifest), copy.deepcopy(self.trace)
                edit(manifest, trace)
                try:
                    result = probe.judge(manifest, trace)
                except (probe.EvidenceError, ValueError, KeyError, TypeError):
                    continue
                self.assertEqual(result['native_rc'], 1, (name, result))

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_transfer_early_completion_is_incomplete(self):
        tid = self.manifest['transfers'][0]['transfer_id']
        self.trace['samples'][1]['s7']['events'].append(event(self.manifest['transfers'][0], 'finish',
                                                           self.trace['samples'][1]['at_ns']))
        result = probe.judge(self.manifest, self.trace)
        self.assertEqual(result['native_rc'], 4, result)
        self.assertIn('original_transfer_not_live:' + tid, result['incomplete'])

    # origin: hot-update S9 plan §9.1 / T77
    def test_continuous_progress_spans_long_phases_and_checks_both_directions(self):
        manifest, trace = complete_model(self.handoff)
        manifest['seconds'] = 30
        t0 = trace['t0_ns']
        for row in trace['samples']:
            row['at_ns'] = t0 + 6_000_000_000 + (row['at_ns'] - t0) * 4
            for event_row in row['s7']['events']:
                event_row['boottime_ns'] = row['at_ns']
            for prefix in row['prefix_reads']:
                prefix.update(at_ns=row['at_ns'] - 1, before_repair_ns=row['at_ns'])
            if row['phase'] == 'retired':
                row.update(started_ns=row['at_ns'] - 500_000_000, finished_ns=row['at_ns'])
        # Phase markers move with their declared cut; slot/effect rows remain
        # absolute and complete rather than reusing the shorter denominator.
        for lane in trace['phase_lanes']:
            for event_row in lane['events']:
                event_row['boottime_ns'] = t0 + 6_000_000_000 + (event_row['boottime_ns'] - t0) * 4
        for ordinal in range(60, 300):
            row = copy.deepcopy(trace['slots'][ordinal % 60])
            delta = (ordinal - row['ordinal']) * 100_000_000
            logical = manifest['nonce'] + '-' + str(ordinal)
            row.update(ordinal=ordinal, logical_id=logical)
            for key in ('scheduled_ns', 'dispatched_ns', 'terminal_ns'):
                row[key] += delta
            for attempt in row['attempts']:
                attempt.update(logical_id=logical)
                attempt['started_mono_ns'] += delta
                attempt['finished_mono_ns'] += delta
            trace['slots'].append(row)
            if row['kind'] in ('write', 'short_exec'):
                trace['effects'].append(dict(logical_id=logical, effect='applied'))
        # The first phase lasts over six seconds. A target observer continues
        # reading while the component operation has no new phase boundary.
        trace['samples'][0]['at_ns'] = t0 + 1
        trace['samples'][0]['s7']['events'] = [event(t, 'synced', t0 + 1, 1) for t in manifest['transfers']]
        for prefix in trace['samples'][0]['prefix_reads']:
            prefix.update(at_ns=t0 + 1, before_repair_ns=t0 + 1)
        trace['progress'] = []
        for second in range(1, 30):
            at = t0 + second * 1_000_000_000
            before = max((s for s in trace['samples'] if s['at_ns'] <= at), key=lambda s: s['at_ns'])
            after = next((s for s in trace['samples'] if s['at_ns'] > at), before)
            row = copy.deepcopy({k: before[k] for k in ('at_ns', 'producer', 'business', 's7')})
            row['at_ns'] = at
            fraction = (at - before['at_ns']) / (after['at_ns'] - before['at_ns']) if after is not before else 0
            for name in row['business']:
                for direction in ('tx', 'rx'):
                    low, high = before['business'][name][direction], after['business'][name][direction]
                    row['business'][name][direction] = low + int((high - low) * fraction)
            for index, transfer in enumerate(manifest['transfers']):
                low = before['s7']['events'][index]['durable_offset']
                high = after['s7']['events'][index]['durable_offset']
                row['s7']['events'][index] = event(transfer, 'synced', at, low + int((high - low) * fraction))
            trace['progress'].append(row)
        trace['manifest_sha256'] = probe.manifest_digest(manifest)
        self.assertEqual(probe.judge(manifest, trace)['native_rc'], 0)
        for observation in trace['samples'] + trace['progress']:
            observation['business']['tcp-active']['rx'] = 0
        result = probe.judge(manifest, trace)
        self.assertEqual(result['native_rc'], 1)
        self.assertIn('flow_stall:tcp-active', result['failures'])

    # origin: hot-update S9 plan §9.1 / T77
    def test_original_launch_age_consumes_fixed_rate_budget(self):
        transfer = self.manifest['transfers'][0]
        transfer['rate_limit'] = 6654944
        transfer['started_ns'] = 1
        self.trace['manifest_sha256'] = probe.manifest_digest(self.manifest)
        with self.assertRaisesRegex(probe.EvidenceError, 'original_transfer_budget_expired'):
            probe.judge(self.manifest, self.trace)

    # origin: hot-update S9 plan §2.3 / T77
    def test_stock_server_requires_exit_even_when_executable_changes(self):
        retired = self.trace['samples'][-1]
        retired['current'].update({k: retired['before'][k] for k in ('BootID', 'PID', 'StartTicks')})
        retired['census']['processes'][-1] = copy.deepcopy(retired['current'])
        result = probe.judge(self.manifest, self.trace)
        self.assertEqual(result['native_rc'], 1)
        self.assertIn('old_server_process_alive', result['failures'])

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_original_nuid_and_ctl_are_required_at_every_phase(self):
        for field in ('pid', 'process_start', 'boot_id'):
            trace = copy.deepcopy(self.trace)
            trace['samples'][20]['s7']['events'][0][field] = 999 if field != 'boot_id' else 'foreign-boot'
            with self.assertRaisesRegex(probe.EvidenceError, 's7_event_incarnation'):
                probe.judge(self.manifest, trace)
        for field in ('entry_nonce', 'object_version'):
            trace = copy.deepcopy(self.trace)
            trace['samples'][20]['s7']['events'][0][field] = 'replacement'
            with self.assertRaises(probe.EvidenceError):
                probe.judge(self.manifest, trace)
            trace = copy.deepcopy(self.trace)
            trace['phase_lanes'][0]['events'][0][field] = 'replacement'
            self.assertEqual(probe.judge(self.manifest, trace)['native_rc'], 1)
        for field in ('pid', 'process_start', 'boot_id', 'running_sha'):
            trace = copy.deepcopy(self.trace)
            trace['samples'][20]['s7']['processes']['ctl0'][field] = 999 if field in ('pid', 'process_start') else 'replacement'
            with self.assertRaises(probe.EvidenceError):
                probe.judge(self.manifest, trace)

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_census_unknown_and_status_projection_fail_setup(self):
        for edit in (lambda t: t.update(producer='product-status'),
                     lambda t: t['samples'][5]['census'].update(complete=False),
                     lambda t: t['samples'][5]['census'].update(processes=[]),
                     lambda t: t['samples'][5]['census'].update(producer='status')):
            trace = copy.deepcopy(self.trace)
            edit(trace)
            with self.assertRaises(probe.EvidenceError):
                probe.judge(self.manifest, trace)

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_reload_retains_process_and_existing_attachments(self):
        manifest, trace = complete_model(self.handoff, 'caddy')
        manifest['profile'] = 'reload'
        manifest['hops'] = manifest['hops'][:1]
        manifest['hops'][0].update(action='reload', target_sha256=trace['samples'][5]['before']['ExeSHA256'])
        trace['samples'] = trace['samples'][:6]
        trace['progress'] = trace['progress'][:6]
        trace['manifest_sha256'] = probe.manifest_digest(manifest)
        for sample in trace['samples']:
            sample['attachments'] = copy.deepcopy(manifest['attachments'])
            sample['accepted'] = {p: accepted(a) for p, a in manifest['attachments'].items()}
        retired = trace['samples'][5]
        retired.update(current=copy.deepcopy(retired['before']), config_before='aa' * 32, config_after='bb' * 32)
        retired['census']['processes'] = [copy.deepcopy(retired['current'])]
        retired['main_ready']['Owner'] = copy.deepcopy(retired['current'])
        # Progress remains observed until the end; this short reload model has
        # a one-second scheduling window rather than nine binary hops.
        manifest['seconds'] = 1
        trace['slots'] = trace['slots'][:10]
        trace['effects'] = trace['effects'][:4]
        trace['tls'] = trace['tls'][:60]
        trace['manifest_sha256'] = probe.manifest_digest(manifest)
        self.assertEqual(probe.judge(manifest, trace)['native_rc'], 0)
        retired['config_after'] = retired['config_before']
        self.assertEqual(probe.judge(manifest, trace)['native_rc'], 1)

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_fresh_tls_requires_every_lane_first_attempt_and_main_pid(self):
        for edit in (lambda t: t['tls'].pop(0), lambda t: t['tls'][0].update(success=False),
                     lambda t: t['tls'][0].update(attempts=2),
                     lambda t: t['samples'][5]['main_ready'].update(Owner=process(999)),
                     lambda t: t['samples'][5]['census'].update(live_business=0)):
            manifest, trace = complete_model(self.handoff, 'caddy')
            edit(trace)
            self.assertEqual(probe.judge(manifest, trace)['native_rc'], 1)

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_closed_fields_and_reader_sources_reject_drift(self):
        self.assertEqual(set(probe.SCHEMA_FIELDS), set(probe.SCHEMA_PRODUCERS))
        manifest = copy.deepcopy(self.manifest)
        manifest['unknown_field'] = 'canary'
        with self.assertRaises(probe.EvidenceError):
            probe.validate_manifest(manifest)
        trace = copy.deepcopy(self.trace)
        trace['slots'][0]['unknown_field'] = 'canary'
        with self.assertRaises(probe.EvidenceError):
            probe.judge(self.manifest, trace)
        source = Path(self.directory.name) / 'reader.py'
        source.write_text('print("original-reader")\n')
        spec = dict(argv=[sys.executable, '-I', '-B', str(source)], sha256=probe.sha(sys.executable),
                    inputs=[dict(path=str(source), sha256=probe.sha(source))])
        self.assertEqual(workload.command(spec), spec['argv'])
        source.write_text('print("status-instead-of-kernel-reader")\n')
        with self.assertRaises(probe.EvidenceError):
            workload.command(spec)
        spec['inputs'] = []
        with self.assertRaises(probe.EvidenceError):
            workload.command(spec)

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_return_attachments_must_belong_to_current_main(self):
        manifest, trace = complete_model(self.handoff, 'caddy')
        retired = trace['samples'][5]
        new = retired['attachments']['participant-1']
        new['Ingress'] = process(999)
        retired['accepted']['participant-1'] = accepted(new)
        self.assertEqual(probe.judge(manifest, trace)['native_rc'], 1)

    # origin: hot-update S9 plan §5.1 / T76 / T77
    def test_canonical_redirect_requires_exact_kernel_translation(self):
        manifest, trace = complete_model(self.handoff, 'caddy')
        def mapping(accepted_row, local, remote):
            return dict(producer='kernel-conntrack-reader', generation=1, netns=accepted_row['NetNS'],
                        original_local=local, original_remote=remote,
                        translated_local=accepted_row['Remote'], translated_remote=accepted_row['Local'])
        tls = trace['tls'][0]
        tls['accepted']['Local'] = '127.0.0.2:8443'
        tls['accepted']['Remote'] = '127.0.0.3:22000'
        tls['translation'] = mapping(tls['accepted'], tls['local'], tls['remote'])
        moving = trace['samples'][1]
        participant = 'participant-1'
        raw = moving['attachments'][participant]['Raw']
        moving['accepted'][participant]['Local'] = '127.0.0.2:8443'
        moving['translations'] = {participant: mapping(moving['accepted'][participant], raw['Local'], raw['Remote'])}
        self.assertEqual(probe.judge(manifest, trace)['native_rc'], 0)
        for field, value in (('original_local', 'unrelated'), ('original_remote', 'unrelated'),
                             ('translated_local', 'unrelated'), ('translated_remote', 'unrelated'), ('netns', 999)):
            with self.subTest(field=field):
                bad = copy.deepcopy(trace)
                bad['tls'][0]['translation'][field] = value
                self.assertEqual(probe.judge(manifest, bad)['native_rc'], 1)
        for edit in (lambda t: t['tls'][0].pop('translation'),
                     lambda t: t['samples'][1].pop('translations')):
            bad = copy.deepcopy(trace)
            edit(bad)
            self.assertEqual(probe.judge(manifest, bad)['native_rc'], 1)
        tls['translation']['producer'] = 'product-status'
        with self.assertRaises(probe.EvidenceError):
            probe.judge(manifest, trace)

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_same_pid_exec_retires_only_prior_executable(self):
        manifest, trace = complete_model(self.handoff, 'support')
        for index, hop in enumerate(manifest['hops']):
            retired = trace['samples'][index * 6 + 5]
            before, current = retired['before'], retired['current']
            if index >= 3:
                before = copy.deepcopy(trace['samples'][(index - 3) * 6 + 5]['current'])
                retired['before'] = before
            old_current = copy.deepcopy(current)
            current.update(PID=before['PID'], StartTicks=before['StartTicks'], InvocationID=before['InvocationID'])
            retired['census']['processes'] = [p for p in retired['census']['processes'] if p != old_current]
            retired['census']['processes'].append(copy.deepcopy(current))
            # Each host's independent census must describe the current support
            # image rather than stale synthetic server incarnations.
            retired['census']['processes'] = [copy.deepcopy(current)]
        self.assertEqual(probe.judge(manifest, trace)['native_rc'], 0)
        trace['samples'][5]['census']['processes'].append(copy.deepcopy(trace['samples'][5]['before']))
        self.assertEqual(probe.judge(manifest, trace)['native_rc'], 1)

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_handoff_and_fixed_pacing_fail_before_requests(self):
        for edit in (lambda m: m['s7_handoff'].update(accepted_by='pending'),
                     lambda m: m['s7_handoff']['files']['negative'].update(sha256='00' * 32),
                     lambda m: m['transfers'][0].update(rate_limit=1 << 30),
                     lambda m: m['hops'].pop(0), lambda m: m.update(attachments={}),
                     lambda m: m.update(originals={})):
            manifest = copy.deepcopy(self.manifest)
            edit(manifest)
            with self.assertRaises(probe.EvidenceError):
                probe.validate_manifest(manifest)
        # A synthetic schema receipt never authorizes a LIVE keeper. Missing
        # executable/schema custody is a setup error before requests launch.
        live = copy.deepcopy(self.manifest)
        live['mode'] = 'live'
        with self.assertRaises((probe.EvidenceError, KeyError)):
            probe.validate_manifest(live)

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_isolated_cli_preserves_native_red_and_sanitizes_errors(self):
        supervisor = workload.Supervisor(os.environ['SIM_UPGRADE_SUPERVISOR'])
        manifest = Path(self.directory.name) / 'manifest.json'
        trace = Path(self.directory.name) / 'trace.json'
        manifest.write_text(json.dumps(self.manifest))
        self.trace['slots'].pop(0)
        trace.write_text(json.dumps(self.trace))
        argv = [sys.executable, '-I', '-B', str(LIB / 'hot_update_infra_probe.py'), 'judge',
                '--manifest', str(manifest), '--trace', str(trace)]
        rc, output = supervisor.run(argv, 10)
        self.assertEqual(rc, 1, output)
        self.assertEqual(json.loads(output)['native_rc'], 1)
        trace.write_text('{"secret-canary": 1, "secret-canary": 2}')
        rc, output = supervisor.run(argv, 10)
        self.assertEqual(rc, 2, output)
        self.assertNotIn(b'secret-canary', output)

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_shared_supervisor_native_status_timeout_and_fd_lifetime(self):
        supervisor = workload.Supervisor(os.environ['SIM_UPGRADE_SUPERVISOR'])
        fd_before = len(list(Path('/proc/self/fd').iterdir()))
        for round in range(5):
            rc, output = supervisor.run([sys.executable, '-I', '-B', '-c', 'print("native"); raise SystemExit(7)'], 5)
            self.assertEqual((rc, output.strip()), (7, b'native'))
        rc, _ = supervisor.run([sys.executable, '-I', '-B', '-c', 'import time; time.sleep(20)'], .1)
        self.assertEqual(rc, 124)
        self.assertEqual(len(list(Path('/proc/self/fd').iterdir())), fd_before)

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_cli_sink_requires_exact_close_and_all_attempts(self):
        root = Path(self.directory.name)
        client = root / 'hermetic-attempt-client.py'
        client.write_text('''import json, os, sys, time
logical = os.environ['TETHER_ATTEMPT_LOGICAL_ID']
fd = int(os.environ['TETHER_ATTEMPT_RECEIPT_FD'])
assert fd > 2
start = time.monotonic_ns()
row = dict(logical_id=logical, ordinal=1, phase='result', started_mono_ns=start,
           finished_mono_ns=time.monotonic_ns(), sent=True, backoff_ns=0)
row['class'] = 'ok'
close = dict(kind='observer_close', logical_id=logical, attempt_count=1, overflow=False)
os.write(fd, (json.dumps(row) + '\\n' + json.dumps(close) + '\\n').encode())
os.close(fd)
print(logical)
''')
        spec, receipt = root / 'command.json', root / 'attempts.jsonl'
        spec.write_text(json.dumps(dict(argv=[sys.executable, '-I', '-B', str(client), '{logical_id}'],
                                        sha256=probe.sha(sys.executable), mode='cli',
                                        inputs=[dict(path=str(client), sha256=probe.sha(client))])))
        logical = self.manifest['nonce'] + '-0'
        rc, output = workload.Supervisor(os.environ['SIM_UPGRADE_SUPERVISOR']).run([
            sys.executable, '-I', '-B', str(LIB / 'hot_update_infra_workload.py'), 'exec-client',
            '--spec', str(spec), '--receipt', str(receipt), '--logical-id', logical], 5)
        self.assertEqual((rc, output.strip()), (0, logical.encode()))
        self.assertEqual(len(workload.attempt_rows(receipt, logical)), 1)
        original = receipt.read_bytes()
        for value in (original.splitlines()[0] + b'\n', original[:-1],
                      original.replace(b'"attempt_count": 1', b'"attempt_count": 2'),
                      original.replace(b'"overflow": false', b'"overflow": true')):
            receipt.write_bytes(value)
            with self.assertRaises(probe.EvidenceError):
                workload.attempt_rows(receipt, logical)

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_raw_tls_handshake_has_one_attempt_and_kernel_owner(self):
        root = Path(self.directory.name)
        key, certificate = root / 'tls.key', root / 'tls.crt'
        supervisor = workload.Supervisor(os.environ['SIM_UPGRADE_SUPERVISOR'])
        # The generated key stays in the private temporary directory; no
        # external artifact, production secret, network or root setup is used.
        openssl = shutil.which('openssl')
        self.assertIsNotNone(openssl, 'hermetic TLS selftest requires the local openssl tool')
        rc, _ = supervisor.run([openssl, 'req', '-x509', '-newkey', 'rsa:2048',
            '-nodes', '-keyout', str(key), '-out', str(certificate), '-days', '1', '-subj', '/CN=localhost',
            '-addext', 'subjectAltName=DNS:localhost'], 10)
        self.assertEqual(rc, 0)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certificate, key)
        owner = workload.process_identity(os.getpid(), 'hermetic-tls-reader')
        reader_file = root / 'accepted.json'
        reader = dict(argv=[sys.executable, '-I', '-B', '-c',
            'import pathlib,sys,time; p=pathlib.Path(sys.argv[1]); end=time.monotonic()+2\n'
            'while not p.exists() and time.monotonic()<end: time.sleep(.001)\nprint(p.read_text())', str(reader_file)],
            sha256=probe.sha(sys.executable))
        errors = []
        fd_before = len(list(Path('/proc/self/fd').iterdir()))
        for round in range(6):
            reader_file.unlink(missing_ok=True)
            with socket.socket() as listener:
                listener.bind(('127.0.0.1', 0))
                listener.listen(1)
                listener.settimeout(3)
                def serve_tls():
                    try:
                        with listener.accept()[0] as raw, context.wrap_socket(raw, server_side=True) as secure:
                            workload.publish(reader_file, dict(accepted=workload.socket_identity(secure.fileno(), owner)))
                            secure.settimeout(3)
                            try:
                                secure.recv(1)
                            except ConnectionResetError:
                                # A handshake-only client does not consume
                                # session tickets. Its close can reset this
                                # read after the successful handshake receipt.
                                pass
                    except (OSError, ValueError) as error:
                        errors.append(type(error).__name__)
                worker = threading.Thread(target=serve_tls)
                worker.start()
                config = dict(address='127.0.0.1', port=listener.getsockname()[1], server_name='localhost',
                              ca_file=str(certificate), reader=reader)
                try:
                    if round == 5:
                        wrong = copy.deepcopy(reader)
                        wrong['argv'][4] = wrong['argv'][4].replace('import pathlib,sys,time;', 'import json,pathlib,sys,time;').replace(
                            'print(p.read_text())', 'row=json.loads(p.read_text()); row["accepted"]["Remote"]="[127.0.0.1]:1"; print(json.dumps(row))')
                        with self.assertRaisesRegex(probe.EvidenceError, 'fresh_tls_tuple_binding'):
                            workload.fresh_tls('external4', round, time.monotonic_ns(), dict(config, reader=wrong), supervisor, 2)
                        continue
                    row = workload.fresh_tls('external4', round, time.monotonic_ns(), config, supervisor, 2)
                finally:
                    worker.join(timeout=4)
                self.assertFalse(worker.is_alive())
                self.assertTrue(row['success'], row)
                self.assertEqual(row['attempts'], 1)
                self.assertEqual(row['accepted']['Owner'], owner)
        self.assertFalse(errors, errors)
        self.assertEqual(len(list(Path('/proc/self/fd').iterdir())), fd_before)
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen(1)
            listener.settimeout(.02)
            config['port'] = listener.getsockname()[1]
            row = workload.fresh_tls('external4', 0, time.monotonic_ns() - 10_000_000_000, config, supervisor, .1)
            self.assertFalse(row['success'])
            with self.assertRaises(socket.timeout):
                with listener.accept()[0]:
                    pass
        # An immediately refused physical entrance remains failed and cannot
        # be signed by a later successful logical request or a second dial.
        row = workload.fresh_tls('external4', 0, time.monotonic_ns(), config, supervisor, .2)
        self.assertFalse(row['success'])
        self.assertEqual(row['attempts'], 1)

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_kernel_socket_cookie_and_incarnation_are_observed(self):
        fd_before = len(list(Path('/proc/self/fd').iterdir()))
        for round in range(5):
            owner = workload.process_identity(os.getpid(), 'hermetic-reader')
            with socket.socket() as listener, socket.socket() as client:
                listener.bind(('127.0.0.1', 0))
                listener.listen(1)
                client.connect(listener.getsockname())
                with listener.accept()[0] as server:
                    raw, accepted_row = workload.socket_identity(client.fileno(), owner), workload.socket_identity(server.fileno(), owner)
                    self.assertNotEqual(raw['Cookie'], accepted_row['Cookie'])
                    self.assertEqual(raw['Remote'], accepted_row['Local'])
                    self.assertEqual(raw['Local'], accepted_row['Remote'])
                    self.assertEqual(raw['Cookie'], int.from_bytes(client.getsockopt(socket.SOL_SOCKET, 57, 8), sys.byteorder))
                    self.assertEqual(workload.socket_identity(client.fileno(), owner), raw)
        self.assertEqual(len(list(Path('/proc/self/fd').iterdir())), fd_before)

    # origin: hot-update S9 plan §9.1 / §9.5 / T76 / T77
    def test_absolute_scheduler_keeps_all_slots_and_joins(self):
        root = Path(self.directory.name)
        protocol = root / 'hermetic-control.py'
        protocol.write_text('''import json, sys, time
kind, logical = sys.argv[1:]
start = time.monotonic_ns()
rc = 7 if kind == 'write' else 0
attempt = dict(logical_id=logical, ordinal=1, phase='request', started_mono_ns=start,
               finished_mono_ns=time.monotonic_ns(), sent=True, backoff_ns=0)
attempt['class'] = 'ok' if rc == 0 else 'terminal'
print(json.dumps(dict(logical_id=logical, native_rc=rc, attempts=[attempt])))
sys.exit(rc)
''')
        reader = dict(argv=[sys.executable, '-I', '-B', '-c', 'print("[]")'], sha256=probe.sha(sys.executable))
        progress = dict(argv=[sys.executable, '-I', '-B', '-c', 'print("{}")'], sha256=probe.sha(sys.executable))
        self.manifest.update(seconds=1, deployment=dict(commands={kind: dict(argv=[sys.executable, '-I', '-B',
            str(protocol), kind, '{logical_id}'], sha256=probe.sha(sys.executable),
            inputs=[dict(path=str(protocol), sha256=probe.sha(protocol))]) for kind in probe.KINDS},
            readers={'effects': reader, 'sample': reader, 'final': reader, 'progress': progress}, tls={}))
        fixture = workload.Fixture(self.manifest, root / 'scheduler', os.environ['SIM_UPGRADE_SUPERVISOR'])
        fixture.thread.start()
        fixture.thread.join(timeout=15)
        self.assertFalse(fixture.thread.is_alive())
        # No S7 live keeper is supplied to this scheduler unit. Missing custody
        # stays a sticky setup failure; the request scheduler still runs every
        # slot and preserves each command's native status.
        self.assertEqual(fixture.failure, 'progress_custody_failed')
        self.assertTrue(fixture.trace['complete'])
        self.assertTrue(fixture.trace['tail_complete'])
        rows = sorted(fixture.trace['slots'], key=lambda r: r['ordinal'])
        self.assertEqual([r['ordinal'] for r in rows], list(range(10)))
        self.assertEqual([r['scheduled_ns'] for r in rows], [fixture.trace['t0_ns'] + i * 100_000_000 for i in range(10)])
        self.assertEqual([r['native_rc'] for r in rows], [0, 0, 0, 7, 0] * 2)
        fixture.close()


if __name__ == '__main__':
    unittest.main()
