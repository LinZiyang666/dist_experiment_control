#!/usr/bin/env python3
"""Deployment-boundary doubles for the r9d drill 85/86/39.NI/CI arm controls.

origin: S9 FIX-INFRAVERBS (r9d finding 5); hot-update S9 plan §9.1 / §9.3 / P33.

Nothing here is a product, fixture or oracle implementation. The REAL fixture
library (drills/lib/hot_update_infra*.{sh,py}) runs every verb; these programs
stand where a live deployment would answer it:

  composer modes  keeper/control (the P19/P30 live requiredset composer), the
                  five request clients, the sample/progress/effects/final/node
                  readers, the TLS accepted-socket reader and the closed
                  fault/exercise/config instruments of vendor/infra-fixture.json;
  s7 / flow       imported by the sandbox's drills/lib/transfer_fixture.py and
                  hot_update_flow_probe.py stubs (other owners' oracles).

Every answer is a deterministic function of the frozen manifest and the
emitted samples, so a separate process (node reader, sample reader, final
reader) agrees with the others without shared mutable state. Knobs in
$INFRA_ARM_FAKE/knobs.json perturb exactly one observation; each knob that is
consulted leaves hit.<knob> so a negative can prove its fault was reached.
Composer instruments append to $INFRA_ARM_FAKE/calls (same record format as
the $SIM double). No docker, ssh, systemd, installed tether or ~/.tether.
"""
import hashlib
import json
import os
from pathlib import Path
import sys
import time

FAKE = Path(os.environ.get('INFRA_ARM_FAKE', '/nonexistent'))
PHASES = ('before', 'moving', 'cutting', 'verifying', 'returning', 'retired')
ORIGINALS = ('ssh', 'tcp-active', 'tcp-idle', 'tcp-half-close', 'tcp-reverse', 'ss-alice', 'ss-bob', 'pty', 'exec')
COHORTS = ('small-push', 'small-pull', 'large-push', 'large-pull')
NO_ROLL = ('lifetime-store-lock', 'exact-release-join', 'responsibility-permission-gates', 'invalid-config-cert',
           'legacy-capability')
HALTING = ('post-ldm-recovery', 'shadow-recovery')
# One second of the real ten-per-second schedule (plus six TLS lanes for Caddy
# hops) per run: the oracle's 5s request budget is real, so the hermetic load
# this double puts on a shared host must stay small enough never to trip it.
SECONDS = 1
SIZE = {'small': 66549440, 'large': 1 << 30}
EXERCISES = {'store-double-open': ('second-launch', 'legacy-owner'),
             'exact-release-join': ('mismatched-release', 'exact-release'),
             'caddy-responsibility-gates': ('uid-override', 'policy-override', 'late-upgrade', 'live-quic',
                                            'long-connection', 'custom-module', 'external-storage')}


def knob(name, default=None):
    path = FAKE / 'knobs.json'
    values = json.loads(path.read_text()) if path.exists() else {}
    if name in values:
        (FAKE / ('hit.' + name)).touch()
        return values[name]
    return default


def record(tool, *args):
    with open(FAKE / 'calls', 'a') as calls:
        calls.write('\x1f'.join(['%d' % time.time_ns(), tool] + [str(a) for a in args]) + '\n')


def sha(data):
    return hashlib.sha256(data if isinstance(data, bytes) else data.encode()).hexdigest()


def process(name, image=None, pid=None, inode=None):
    number = int(sha(name)[:6], 16) % 900000 + 1000
    pid = pid or number
    return dict(BootID='hermetic-boot', PID=pid, StartTicks=pid * 10 + 1, InvocationID='inv-' + name,
                ExeDevice=1, ExeInode=inode or number * 100 + 1, ExeSHA256=image or sha(name), Cgroup='/hermetic/' + name)


def sock(name, owner, local=None, remote=None):
    number = int(sha(name)[:6], 16) % 900000 + 1
    return dict(NetNS=1, Device=0, Inode=number, Cookie=number + 7, Local=local or '[127.0.0.1]:%d' % (10000 + number % 50000),
                Remote=remote or '[127.0.0.2]:443', Owner=owner)


def evidence():
    return Path((FAKE / 'ev').read_text().strip())


def profile():
    return json.loads((FAKE / 'profile.json').read_text())


def manifest():
    return json.loads((evidence() / 'manifest.json').read_text())


def t0():
    return json.loads((evidence() / 'observer/trace.json').read_text())['t0_ns']


def at_of(m, index, phase):
    """Synthetic phase clock: monotone, strictly inside [t0, end] for any hop count."""
    slots = len(m['hops']) * len(PHASES) + 1
    return t0() + (index * len(PHASES) + PHASES.index(phase) + 1) * (SECONDS * 1_000_000_000 - 1_000_000) // slots


def amount(at):
    return (at - t0()) // 1_000_000 + 1


def business(m, at, final=False):
    rows = {}
    for name, base in m['originals'].items():
        count = amount(at)
        rows[name] = dict(identity=base['identity'], accepts=1, redials=1 if knob('progress.redial') == name else 0,
                          tx=count, rx=count, tx_sha256=sha(str(count)), rx_sha256=sha(str(count)), carrier=base['carrier'])
        if name == 'ss-alice':
            rows[name]['revoked'] = True
        if final:
            rows[name].update(fin=[True, False] if knob('final.fin_missing') == name else [True, True], native_rc=0)
    return rows


def server(host, component, index, m):
    """Chain incarnation after hop index (or the initial stock incarnation)."""
    if index is None:
        image = profile()['initial_sha256'] if component != 'support' else sha('support-initial')
        return process(component + '-' + host + '-init', image)
    hop = m['hops'][index]
    if component == 'support':
        base = process('support-' + host)
        return process('support-' + host, hop['target_sha256'], pid=base['PID'], inode=base['ExeInode'] + index + 1)
    return process(component + '-' + host + '-' + str(index), hop['target_sha256'])


def emitted():
    path = FAKE / 'emitted.jsonl'
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def chain_current(m, host, component, upto=None):
    done = {row['hop'] for row in emitted() if row['phase'] == 'retired'}
    index = None
    for i, hop in enumerate(m['hops']):
        if (upto is None or i < upto) and hop['host'] == host and hop['component'] == component and hop['id'] in done \
                and hop['action'] != 'reload':
            index = i
    return server(host, component, index, m)


def attachment(participant, generation, ingress):
    raw = sock('%s-%d' % (participant, generation), process('agent-' + participant))
    return dict(Participant=participant, Generation=generation, ServerID='srv-%d' % generation, CID=generation,
                URL='wss://brk1.example:443', Raw=raw, Ingress=ingress, HealthGeneration=generation)


def accepted(row):
    socket = sock(row['Raw']['Local'] + '-accepted', row['Ingress'], local=row['Raw']['Remote'], remote=row['Raw']['Local'])
    return socket


def sample(hop_id, phase):
    m = manifest()
    index = next(i for i, h in enumerate(m['hops']) if h['id'] == hop_id)
    hop, at = m['hops'][index], at_of(m, index, phase)
    if profile()['profile'] in HALTING and (index > 0 or PHASES.index(phase) > 2) and not knob('sample.no_halt'):
        # A halted hop shows no later phase until the product recovers it.
        deadline = time.monotonic() + 90
        while not (FAKE / 'recovered').exists() and time.monotonic() < deadline:
            time.sleep(.05)
    step = PHASES.index(phase)
    generation = 1 + 2 * index + (step >= 1) + (step >= 4)
    if knob('sample.frozen_attachment') == hop_id and step >= 1:
        generation = 1 + 2 * index
    current = server(hop['host'], hop['component'], index, m)
    before = chain_current(m, hop['host'], hop['component'], upto=index)
    if hop['action'] == 'reload':
        current = before
    main = chain_current(m, hop['host'], 'caddy', upto=index) if hop['component'] == 'caddy' else process('caddy-ingress')
    ingress = process('shadow-%d' % index) if hop['component'] == 'caddy' and 1 <= step <= 3 else (
        current if hop['component'] == 'caddy' and step >= 4 else main)
    if hop['action'] == 'reload':
        ingress = main
    peers = {p: attachment(p, generation, ingress) for p in m['attachments']}
    durable = 1 << 20  # covers every synced offset the S7 stand-in can report
    row = dict(hop=hop_id, phase=phase, at_ns=at, producer='kernel-and-target-reader', business=business(m, at),
               prefix_reads=[dict(transfer_id=t['transfer_id'], at_ns=at - 1, before_repair_ns=at,
                                  expected=[dict(index=0, length=durable, sha256=sha('prefix'))],
                                  actual=[dict(index=0, length=durable, sha256=sha('prefix'))]) for t in m['transfers']],
               attachments=peers, accepted={p: accepted(a) for p, a in peers.items()})
    if phase == 'retired':
        others = [chain_current(m, h['host'], h['component'], upto=index) for h in m['hops']
                  if (h['host'], h['component']) != (hop['host'], hop['component'])]
        census = [current] + others + [process('broker-' + hop['host'])]
        if knob('sample.old_alive') == hop_id:
            census.append(before)
        row.update(before=before, current=current, temporary=[process('temp-%d' % index)],
                   census=dict(producer='kernel-reader', complete=True, generation=index + 1, processes=census,
                               live_business=12, old_owner_pending=0),
                   return_verified=True, root_revision=index + 1, transaction_revision=index + 1,
                   started_ns=at - 2000, finished_ns=at - 1, main_ready=sock('main-%d' % index, current))
        if hop['action'] == 'reload':
            row.update(config_before=sha('config-initial'), config_after=staged('valid'))
    with open(FAKE / 'emitted.jsonl', 'a') as output:
        output.write(json.dumps(dict(hop=hop_id, phase=phase)) + '\n')
    return row


def staged(variant):
    path = FAKE / ('config-' + variant)
    return path.read_text().strip() if path.exists() else sha('config-initial')


def node(name, component):
    m = manifest() if (evidence() / 'manifest.json').exists() else None
    state = profile()
    current = chain_current(m, name, component) if m else server(name, component, None, m)
    if knob('node.restarted') == name and (FAKE / 'restart-armed').exists():
        current = process(component + '-' + name + '-restarted', current['ExeSHA256'])
    if knob('node.stuck') == name and m:
        current = server(name, component, None, m)
    if knob('node.bootstrap_restart') == name and (FAKE / ('managed.' + name)).exists():
        current = process(component + '-' + name + '-bootstrap-restart', current['ExeSHA256'])
    legacy = state['profile'] == 'legacy-capability'
    previous = Path(os.environ['HERE']) / 'vendor/tether-prev'
    broker = process('broker-' + name, sha(previous.read_bytes()) if legacy and not knob('node.legacy_sha') else None)
    if knob('node.broker_reexec') == name and (FAKE / 'restart-armed').exists():
        broker = process('broker-' + name + '-reexec', broker['ExeSHA256'])
    temporary = [process('temp-left')] if knob('node.temporary') == name and (FAKE / 'restart-armed').exists() else []
    config = None
    if component == 'caddy':
        reloaded = m and any(h['action'] == 'reload' and h['id'] in {r['hop'] for r in emitted() if r['phase'] == 'retired'}
                             for h in m['hops'])
        config = staged('valid') if reloaded else sha('config-initial')
        if knob('node.config_changed') == name and (FAKE / 'restart-armed').exists():
            config = sha('config-drifted')
    managed = (FAKE / ('managed.' + name)).exists() and not legacy and not knob('node.unmanaged')
    serving = not (knob('node.not_serving') == name and (FAKE / 'restart-armed').exists())
    # An old owner the kernel still runs: only the retirement census can see it.
    lingering = [server(name, component, None, m)] if knob('node.census_old') == name and m else []
    return dict(producer='kernel-reader', node=name, component=component, at_ns=time.monotonic_ns(), server=current,
                invocation=dict(user='root', restart='on-failure', managed=managed), config=config, serving=serving,
                temporary=temporary, broker=broker, infra_capable=not legacy or bool(knob('node.legacy_capable')),
                census=dict(producer='kernel-reader', complete=True, generation=1,
                            processes=[current, broker] + temporary + lingering, live_business=12, old_owner_pending=0))


def keeper(args):
    options = dict(zip(args[0::2], args[1::2]))
    root = Path(options['--evidence'])
    (FAKE / 'ev').write_text(str(root.parent))
    (FAKE / 'profile.json').write_text(json.dumps(dict(profile=options['--profile'], component=options['--component'],
                                                        topology=options['--topology'],
                                                        initial_sha256=options['--initial-sha256'])))
    record('supply', 'keeper', options['--component'], options['--topology'], options['--profile'], options['--nodes'])
    if knob('keeper.exit') == 'prepare':
        return 3
    publish(root / 'prepared.json', dict(schema='infra-cohort-v1', phase='prepared', producer='composer',
                                        component=options['--component'], topology=options['--topology'],
                                        profile=options['--profile'], initial_sha256=options['--initial-sha256']))
    deadline = time.monotonic() + 300
    while not (root / '.launch').exists():
        if (root / '.stop').exists() or time.monotonic() > deadline:
            return int(knob('keeper.stop_rc', 0))
        time.sleep(.02)
    if knob('keeper.exit') == 'launch':
        return 3
    launch(root, options['--profile'])
    while not (root / '.stop').exists() and time.monotonic() < deadline:
        time.sleep(.02)
    return int(knob('keeper.stop_rc', 0))


def publish(path, value):
    pending = path.with_name('.' + path.name + '.pending')
    pending.write_text(json.dumps(value, sort_keys=True))
    pending.replace(path)


def launch(root, name):
    originals = {}
    for index, original in enumerate(ORIGINALS):
        identity = dict(application_nonce='original-' + original)
        if original in ('pty', 'exec'):
            identity.update(ctl=process('ctl-' + original), child=process('child-' + original), task_id='task-' + original)
        else:
            identity['socket'] = sock('original-' + original, process('client-' + original))
        originals[original] = dict(identity=identity, carrier='carrier-original')
    now = time.monotonic_ns()
    transfers = [dict(cohort=cohort, transfer_id='transfer-' + cohort, entry_nonce='NUID-' + cohort,
                      session_id='session-' + cohort, bucket='bucket-' + cohort, object_version='object-' + cohort,
                      ctl_producer='ctl%d' % i, ctl=process('transfer-ctl-' + cohort), started_ns=now,
                      size=SIZE[cohort.split('-')[0]], rate_limit=1024) for i, cohort in enumerate(COHORTS)]
    s7 = root / 's7'
    (s7 / 'evidence').mkdir(parents=True)
    streams = [dict(t, object_stream='OBJ_' + t['cohort'].replace('-', '_'), history_stream='HIST_' +
                    t['cohort'].replace('-', '_')) for t in transfers]
    (s7 / 'manifest.json').write_text(json.dumps(dict(mode='live', transfers=streams), sort_keys=True))
    (root / 'flow').mkdir()
    lib = Path(os.environ['HERE']) / 'drills/lib'
    lanes = {} if name in NO_ROLL else dict(writer_lost_ack=transfers[0]['transfer_id'],
                                            reader_partial_sync=transfers[1]['transfer_id'],
                                            commit_rename='extra-commit', terminal_courier='extra-terminal')
    publish(root / 'ready.json', dict(
        schema='infra-cohort-v1', phase='ready', producer='composer', seconds=SECONDS, originals=originals,
        attachments={p: attachment(p, 1, process('caddy-ingress')) for p in ('participant-1', 'participant-2')},
        transfers=transfers, s7=dict(manifest=str(s7 / 'manifest.json'), evidence=str(s7 / 'evidence')),
        flow=dict(evidence=str(root / 'flow'), nonce='ab' * 16),
        s7_manifest_sha256=sha((s7 / 'manifest.json').read_bytes()),
        s7_reader_digest=sha((lib / 'transfer_fixture.py').read_bytes()), phase_lanes=lanes))


def control(args):
    options = dict(zip(args[0:2:2], args[1:2:2]))
    root, verb = Path(options['--evidence']), args[2]
    record('supply', 'control', verb)
    (root / ('.' + verb)).touch()
    return 0


def client(kind, logical):
    started = time.monotonic_ns()
    fd = int(os.environ['TETHER_ATTEMPT_RECEIPT_FD'])
    attempt = dict(logical_id=logical, ordinal=1, phase='request', started_mono_ns=started,
                   finished_mono_ns=time.monotonic_ns(), sent=True, backoff_ns=0, **{'class': 'ok'})
    close = dict(kind='observer_close', logical_id=logical, attempt_count=1, overflow=False)
    os.write(fd, (json.dumps(attempt) + '\n' + json.dumps(close) + '\n').encode())
    os.close(fd)
    if kind in ('write', 'short_exec'):
        with open(FAKE / 'effects.jsonl', 'a') as output:
            output.write(json.dumps(dict(logical_id=logical, effect='applied')) + '\n')
    print(json.dumps({'nodes': []}) if kind == 'node_ls' else json.dumps({'processes': []}) if kind == 'ps'
          else 'ok ' + logical if kind in ('read', 'short_exec') else '')
    return 0


def final():
    m = manifest()
    at = t0() + SECONDS * 1_000_000_000 - 1
    controls = [dict(op=op, subject='ss-alice' if op == 'revoke_alice' else 'victim' if op in ('ps_victim', 'kill_victim')
                     else 'pty', at_ns=t0() + 1, producer='control-target-reader', effect=True)
                for op in ('resize', 'signal', 'keepalive_lost_ack', 'input', 'output', 'ps_victim', 'kill_victim',
                           'revoke_alice')]
    lanes = []
    if m['phase_lanes']:
        cut = at_of(m, 0, 'cutting')
        kinds = dict(writer_lost_ack='append', reader_partial_sync='synced', commit_rename='primitive_exit',
                     terminal_courier='history')
        for lane, tid in m['phase_lanes'].items():
            base = next((t for t in m['transfers'] if t['transfer_id'] == tid), dict(m['transfers'][0], transfer_id=tid))
            first, second = s7_event(base, kinds[lane], cut - 1, 1), s7_event(base, 'freeze', cut + 1, 2)
            first.update(dict(writer_lost_ack=dict(actual_ack_seq=10), reader_partial_sync=dict(durable_offset=10,
                         fetched_offset=20), commit_rename=dict(outcome='landed'), terminal_courier=dict(native_outcome=0))[lane])
            if lane == 'commit_rename':
                second.update(kind='directory_sync', parent_synced=True)
            lanes.append(dict(lane=lane, hop='hop-0', events=[first, second], sent=True, effect=True, reply_cut=True))
    return dict(business=business(m, at, final=True), controls=controls, phase_lanes=lanes)


def s7_event(transfer, kind, at, offset):
    ctl = transfer['ctl']
    return dict(schema=1, kind=kind, process_start=ctl['StartTicks'], boot_id=ctl['BootID'], pid=ctl['PID'],
                generation=1, cut_id='hop-0', transfer_id=transfer['transfer_id'], entry_nonce=transfer['entry_nonce'],
                object_version=transfer['object_version'], leg='leg1', phase='payload', event_seq=offset,
                boottime_ns=at, durable_offset=offset, producer=transfer['ctl_producer'])


def s7_snapshot(manifest_path):
    # The custody ledger is a function of the transfer clock alone, never of
    # when the observer happened to ask: a sample whose mapped time is later
    # than its read must not see less durable progress than an earlier row.
    transfers = json.loads(Path(manifest_path).read_text())['transfers']
    end, events = t0() + SECONDS * 1_000_000_000, []
    for t in transfers:
        steps = min(100, (end - t['started_ns']) // 100_000_000 + 1)
        events += [s7_event(t, 'synced', t['started_ns'] + k * 100_000_000, 1024 * (k + 1)) for k in range(steps)]
        if knob('s7.finish') == t['cohort']:
            events.append(s7_event(t, 'finish', t['started_ns'] + 1, 1))
    return dict(setup=[], failures=[], events=events, processes={t['ctl_producer']: dict(
        pid=t['ctl']['PID'], process_start=t['ctl']['StartTicks'], boot_id=t['ctl']['BootID'],
        running_sha=t['ctl']['ExeSHA256']) for t in transfers})


def s7_judge(manifest_path, root, digest):
    spec = json.loads(Path(manifest_path).read_text())
    inputs = {}
    for t in spec['transfers']:
        for key in ('object_stream', 'history_stream'):
            inputs[t[key]] = dict(stream=t[key], rows=[])
            (Path(root) / ('raw-' + t[key] + '.json')).write_text(json.dumps(inputs[t[key]]))
    rc = int(knob('s7.judge_rc', 0))
    result = dict(native_rc=rc, manifest_sha256=sha(Path(manifest_path).read_bytes()), reader_digest=digest,
                  oracle_input_sha=sha(json.dumps(inputs, sort_keys=True)))
    (Path(root) / 'verdict.json').write_text(json.dumps(result))
    return result


def instrument(kind, args):
    options = dict(zip(args[0::2], args[1::2]))
    name = options.get('--point') or options.get('--scenario') or options.get('--variant')
    record('supply', kind, name, *(['cleanup'] if '--cleanup' in args else []))
    if knob(kind + '.fail') == name:
        return 1
    if kind == 'fault-install':
        (FAKE / 'restart-armed').touch()
        print(json.dumps(dict(point=name, installed=True, nodes=options['--nodes'].split(), at_ns=time.monotonic_ns(),
                              mechanism='hermetic-' + name)))
    elif kind == 'fault-status':
        hit = not knob('fault.not_hit')
        print(json.dumps(dict(point=name, active=not knob('fault.inactive'), hits=1 if hit else 0, sent=hit, effect=hit,
                              reply_cut=hit, digest=sha(name) if hit else None)))
    elif kind == 'fault-heal':
        topology = options.get('--recovery-topology') or profile()['topology']
        if options.get('--recovery-topology'):
            topology = knob('heal.topology', topology)
        nodes = {'single': ['brk1'], 'sole-voter': ['brk1'], 'pair': ['brk1', 'brk2'],
                 'quorum': ['brk1', 'brk2', 'brk3']}[topology]
        print(json.dumps(dict(point=name, healed=True, topology=topology, nodes=nodes, at_ns=time.monotonic_ns())))
    elif kind == 'exercise':
        (FAKE / 'restart-armed').touch()
        rows = []
        for row in EXERCISES[name]:
            if knob('exercise.drop') == row:
                continue
            outcome = 'joined' if row == 'exact-release' else 'deferred' if name == 'caddy-responsibility-gates' else 'refused'
            rows.append(dict(name=row, outcome=knob('exercise.outcome.' + row, outcome), code='infra_' + row.replace('-', '_'),
                             forced_close=bool(knob('exercise.forced.' + row))))
        holder = chain_current(manifest(), 'brk1', profile()['component']) if name == 'store-double-open' else None
        print(json.dumps(dict(scenario=name, rows=rows, holder=holder, retired=not knob('exercise.unretired'))))
    elif kind == 'config':
        digest = sha('config-' + name)
        (FAKE / ('config-' + name)).write_text(digest)
        print(json.dumps(dict(variant=name, staged=True, sha256=knob('config.sha', digest))))
    return 0


def main(argv):
    mode, args = argv[0], argv[1:]
    if mode == 'keeper':
        return keeper(args)
    if mode == 'control':
        return control(args)
    if mode == 'client':
        return client(*args)
    if mode in ('fault-install', 'fault-status', 'fault-heal', 'exercise', 'config'):
        return instrument(mode, args)
    if mode == 'tls-reader':
        accepted_row = sock('tls-' + args[0], process('caddy-main'), local=args[1], remote=args[0])
        print(json.dumps(dict(accepted=accepted_row)))
        return 0
    if knob('reader.fail') == mode:
        return 3
    if mode == 'sample':
        value = sample(*args)
    elif mode == 'progress':
        # A remote reader maps its clock into the window; under fan-out load a
        # late final read must not leave [t0, end] (that is the reader's job).
        m, at = manifest(), min(time.monotonic_ns(), t0() + SECONDS * 1_000_000_000 - 1)
        value = dict(at_ns=at, producer='kernel-and-target-reader', business=business(m, at))
    elif mode == 'effects':
        path = FAKE / 'effects.jsonl'
        value = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        if knob('effects.duplicate') and value:
            value.append(value[-1])
    elif mode == 'final':
        value = final()
    elif mode == 'node':
        value = node(*args)
    else:
        return 64
    print(json.dumps(value))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
