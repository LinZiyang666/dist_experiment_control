#!/usr/bin/env -S python3 -I -B
"""Foreground S9 fixture and supervised observer-local control scheduler.

serve --manifest FILE --evidence DIR --supervisor ABSOLUTE_EXE owns requests
until the fixed schedule and late-effect tail finish. checkpoint/finish/stop
are private Unix-socket verbs, not product upgrades. Each checkpoint invokes
the predeclared independent reader, which returns the kernel/target/S7 sample;
the fixture never turns Status into a sample or restarts a completed original.

The deployment section has commands for the five KINDS, each with {argv, sha256}
and a {logical_id} argument placeholder; sample/progress/effects/final readers
and an S7 {manifest, evidence}. Readers return JSON, not human log scraping.
S7 remains a foreground sealed-custody keeper started through infra_spawn by
the deployment caller. Its sample and final judge are called through the same
supervisor. The flow section selects the original S5 evidence and nonce.
No producer pause, cut, policy rotation, service restart or nft write is a verb.
origin: hot-update S9 plan §9.1 / §9.5 / §12 P32
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hot_update_infra_probe as probe


def publish(path, value):
    temporary = path.with_name('.' + path.name + '.pending')
    with temporary.open('w', encoding='utf-8') as output:
        json.dump(value, output, sort_keys=True, allow_nan=False)
        output.write('\n')
        output.flush()
        os.fsync(output.fileno())
    temporary.chmod(0o600)
    temporary.replace(path)


def command(spec, logical=None):
    probe.fields(spec, 'command')
    argv = spec['argv']
    probe.require(isinstance(argv, list) and argv and all(isinstance(a, str) for a in argv)
                  and Path(argv[0]).is_absolute() and os.access(argv[0], os.X_OK)
                  and probe.sha(argv[0]) == spec['sha256'], 'reader_or_command_bytes')
    inputs = spec.get('inputs', [])
    for source in inputs:
        probe.fields(source, 'file')
        probe.require(Path(source['path']).is_absolute() and probe.sha(source['path']) == source['sha256'],
                      'reader_source_bytes_changed')
    probe.require(all(a in {i['path'] for i in inputs} for a in argv[1:]
                      if a.endswith('.py') and Path(a).is_absolute()), 'reader_source_binding_missing')
    probe.require('/.tether/' not in ' '.join(argv), 'production_home_forbidden')
    if logical is not None:
        probe.require(any('{logical_id}' in a for a in argv), 'stable_request_id_required')
    return [a.replace('{logical_id}', logical) if logical is not None else a for a in argv]


class Supervisor:
    """All children, including one-shot Python adapters, enter FIX-ORPHAN."""
    def __init__(self, path):
        self.path = Path(path)
        probe.require(self.path.is_absolute() and os.access(self.path, os.X_OK), 'shared_supervisor_required')

    def run(self, argv, timeout, *, log=None, track=None):
        # origin: hot-update S9 plan §9.5 / L22. Killing this process triggers
        # the shared launcher's lease/pidfd watcher; no independent PGID reaper.
        child = subprocess.Popen([str(self.path), '--'] + argv, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL)
        if track is not None:
            # A long phase reader must not outlive stop: close() terminates it.
            track.append(child)
        try:
            output, _ = child.communicate(timeout=max(.001, timeout))
        except subprocess.TimeoutExpired:
            child.terminate()
            try:
                output, _ = child.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                output, _ = child.communicate(timeout=5)
            if log:
                Path(log).write_bytes(output)
            return 124, output
        if log:
            Path(log).write_bytes(output)
        return child.returncode, output


def decode(raw):
    probe.require(len(raw) <= probe.MAX_BYTES, 'reader_output_overflow')
    return json.loads(raw, object_pairs_hook=probe.unique_object,
                      parse_constant=lambda _: probe.require(False, 'nonfinite_reader_json'))


def process_identity(pid, invocation):
    """Pin the executable FD while hashing, then recheck PID/start and inode.

    InvocationID is obtained from the independent unit reader, not guessed
    from PID. Missing /proc permissions or a racing exec is SETUP2.
    """
    base = Path('/proc') / str(pid)
    fields = (base / 'stat').read_text().rsplit(')', 1)[1].split()
    probe.require(fields[0] not in ('Z', 'X'), 'kernel_process_dead')
    start = int(fields[19])
    fd = os.open(base / 'exe', os.O_RDONLY | os.O_CLOEXEC)
    try:
        info = os.fstat(fd)
        digest = hashlib.sha256()
        offset = 0
        while True:
            part = os.pread(fd, 1 << 20, offset)
            if not part:
                break
            digest.update(part)
            offset += len(part)
        after = os.stat(base / 'exe')
        probe.require((after.st_dev, after.st_ino) == (info.st_dev, info.st_ino)
                      and int((base / 'stat').read_text().rsplit(')', 1)[1].split()[19]) == start,
                      'kernel_process_raced')
        row = dict(BootID=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                   PID=pid, StartTicks=start, InvocationID=invocation,
                   ExeDevice=info.st_dev, ExeInode=info.st_ino, ExeSHA256=digest.hexdigest(),
                   Cgroup=(base / 'cgroup').read_text().strip())
        probe.process(row)
        return row
    finally:
        os.close(fd)


def socket_identity(fd, owner):
    """The producer must hold the actual FD, not fabricate a cookie from inode."""
    with socket.socket(fileno=os.dup(fd)) as raw:
        info = os.fstat(raw.fileno())
        local, remote = raw.getsockname(), raw.getpeername()
        cookie = int.from_bytes(raw.getsockopt(socket.SOL_SOCKET, 57, 8), sys.byteorder)
        row = dict(NetNS=os.stat('/proc/self/ns/net').st_ino, Device=info.st_dev,
                   Inode=info.st_ino, Cookie=cookie, Local=address(local),
                   Remote=address(remote), Owner=owner)
        probe.socket(row)
        return row


def address(value):
    return ('[' + value[0] + ']' if ':' in value[0] else value[0]) + ':' + str(value[1])


def fresh_tls(lane, ordinal, scheduled, config, supervisor, deadline):
    """Exactly one physical handshake. An openssl retry cannot hide a gap.

    The observer opens canonical443 itself, with one TLS handshake and ordinary
    hostname/CA verification. The reader maps this still-open tuple to P02's
    accepted kernel socket. It cannot change the handshake outcome or retry it.
    """
    started = time.monotonic_ns()
    expires = scheduled + int(deadline * 1_000_000_000)
    def remaining():
        value = (expires - time.monotonic_ns()) / 1_000_000_000
        if value <= 0:
            raise TimeoutError('fresh_tls_deadline')
        return value
    row = dict(ordinal=ordinal, lane=lane, scheduled_ns=scheduled, started_ns=started,
               terminal_ns=started, success=False, attempts=1)
    context = ssl.create_default_context(cafile=config['ca_file'])
    family = socket.AF_INET6 if lane.endswith('6') else socket.AF_INET
    # Addresses are frozen numeric topology inputs. DNS cannot consume another
    # handshake opportunity or redirect an IPv6 lane onto IPv4.
    socket.inet_pton(family, config['address'])
    try:
        with socket.socket(family) as raw:
            raw.settimeout(remaining())
            if config.get('source_address'):
                raw.bind((config['source_address'], 0))
            raw.connect((config['address'], config.get('port', 443)))
            raw.settimeout(remaining())
            with context.wrap_socket(raw, server_hostname=config['server_name']) as secure:
                local, remote = secure.getsockname(), secure.getpeername()
                rc, output = supervisor.run(command(config['reader']) + [address(local), address(remote)], remaining())
                probe.require(rc == 0, 'accepted_socket_reader_failed')
                receipt = decode(output)
                row['accepted'] = receipt['accepted']
                row.update(local=address(local), remote=address(remote))
                if 'translation' in receipt:
                    row['translation'] = receipt['translation']
                # origin: hot-update S9 plan §5.1 / T77. A reader must identify
                # this held socket, not an unrelated healthy main connection.
                probe.socket(row['accepted'])
                probe.require(probe.connection_binding(row['accepted'], row['local'], row['remote'],
                              row.get('translation')), 'fresh_tls_tuple_binding')
                if receipt.get('bypass'):
                    row.update(bypass=True, main=receipt['main'])
                row['success'] = True
    except (OSError, ssl.SSLError):
        # The first failure remains a sample. No retry, alternate port or
        # successful logical request can overwrite this physical entrance gap.
        pass
    row['terminal_ns'] = time.monotonic_ns()
    return row


def attempt_rows(path, logical):
    raw = Path(path).read_bytes()
    probe.require(len(raw) <= probe.MAX_BYTES and raw.endswith(b'\n'), 'attempt_sink_truncated')
    rows = [decode(line) for line in raw.splitlines()]
    closes = [r for r in rows if r.get('kind') == 'observer_close']
    attempts = [r for r in rows if r.get('kind') != 'observer_close']
    probe.require(len(closes) == 1 and closes[0] == dict(kind='observer_close', logical_id=logical,
                  attempt_count=len(attempts), overflow=False) and 0 < len(attempts) <= 128,
                  'attempt_sink_close_missing')
    return sorted(attempts, key=lambda r: r['ordinal'])


def exec_client(spec, logical, receipt):
    """Open the production attempt sink after supervision, then exec in place.

    The shared launcher intentionally does not forward arbitrary inherited FDs.
    Opening here and execing preserves its child identity and lease rather than
    adding an unsupervised CLI child. The sink is private and never stdout.
    """
    argv = command(probe.read_json(spec), logical)
    fd = os.open(receipt, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.set_inheritable(fd, True)
    environment = dict(os.environ, TETHER_ATTEMPT_RECEIPT_FD=str(fd), TETHER_ATTEMPT_LOGICAL_ID=logical)
    os.execve(argv[0], argv, environment)


class Fixture:
    def __init__(self, manifest, evidence, supervisor):
        self.manifest = probe.validate_manifest(manifest)
        self.root, self.supervisor = Path(evidence), Supervisor(supervisor)
        probe.require(self.root.is_absolute() and not (self.root / 'trace.json').exists(), 'fresh_evidence_required')
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.root.chmod(0o700)
        self.deployment = manifest['deployment']
        probe.fields(self.deployment, 'deployment')
        probe.require(set(self.deployment['commands']) == set(probe.KINDS)
                      and set(self.deployment['tls']) == set(manifest['tls_lanes']), 'deployment_commands')
        probe.require(set(self.deployment['readers']) == {'sample', 'progress', 'effects', 'final'},
                      'independent_readers_required')
        for spec in list(self.deployment['commands'].values()) + list(self.deployment['readers'].values()) + [c['reader'] for c in self.deployment['tls'].values()]:
            command(spec)
        for kind, spec in self.deployment['commands'].items():
            probe.require(spec.get('mode') == 'cli' or manifest.get('mode') == 'hermetic', 'production_cli_sink_required')
            publish(self.root / (kind + '-command.json'), spec)
        for config in self.deployment['tls'].values():
            probe.fields(config, 'tls_config')
            # Only a hermetic control may dial a loopback test port; its judge
            # result is then labelled evidence_scope=hermetic, never live.
            probe.require(config.get('port', 443) == 443 or manifest['mode'] == 'hermetic', 'canonical_port_required')
        self.lock, self.stop_event = threading.RLock(), threading.Event()
        self.failure = None
        # origin: S9 FIX-INFRAVERBS. observe arms one block of declared hops;
        # the watcher records each physical phase while the product command runs.
        self.watch, self.watch_children, self.watch_failures, self.observed = None, [], [], []
        self.trace = dict(schema=probe.TRACE_SCHEMA, nonce=manifest['nonce'], producer='independent-observer',
                          manifest_sha256=probe.manifest_digest(manifest),
                          t0_ns=time.monotonic_ns(), slots=[], effects=[], samples=[], progress=[], controls=[], tls=[],
                          complete=False, tail_complete=False)
        self.thread = threading.Thread(target=self.schedule, name='infra-absolute-slots')
        self.progress_thread = threading.Thread(target=self.observe_progress, name='infra-progress')

    def save(self):
        with self.lock:
            publish(self.root / 'trace.json', self.trace)

    def reader(self, kind, *arguments):
        rc, raw = self.supervisor.run(command(self.deployment['readers'][kind]) + list(arguments), 10)
        probe.require(rc == 0, 'independent_reader_failed')
        return decode(raw)

    def s7(self, verb):
        spec = self.deployment['s7']
        probe.fields(spec, 's7_binding')
        script = Path(__file__).with_name('transfer_fixture.py')
        timeout = 30
        if verb == 'judge':
            import transfer_fixture
            actual = transfer_fixture.read_manifest(spec['manifest'])
            if actual.get('consumer') == 'infra-transfer-v1':
                from infra_transfer_fixture import completion_seconds
                timeout = max(completion_seconds(t) for t in actual['transfers']) + 30
        rc, raw = self.supervisor.run([sys.executable, '-I', '-B', str(script), verb,
                                      '--manifest', spec['manifest'], '--evidence', spec['evidence']], timeout)
        result = decode(raw)
        if verb == 'sample':
            # Native failures stay sticky; sample may return 1 on bad bytes.
            probe.require(rc in (0, 1), 's7_sample_unavailable')
            return probe.read_json(result['snapshot'])
        probe.require(result.get('native_rc') == rc, 's7_native_status_changed')
        return result

    def slot(self, ordinal, scheduled):
        logical = self.manifest['nonce'] + '-' + str(ordinal)
        kind = probe.KINDS[ordinal % len(probe.KINDS)]
        dispatched = time.monotonic_ns()
        limit = probe.budgets()['HOT_UPDATE_REQUEST_S']
        spec = self.deployment['commands'][kind]
        receipt = self.root / (logical + '-attempts.jsonl')
        argv = command(spec, logical)
        if spec.get('mode') == 'cli':
            argv = [sys.executable, '-I', '-B', str(Path(__file__).resolve()), 'exec-client',
                    '--spec', str(self.root / (kind + '-command.json')), '--logical-id', logical,
                    '--receipt', str(receipt)]
        rc, raw = self.supervisor.run(argv, (scheduled + limit * 1_000_000_000 - dispatched) / 1_000_000_000)
        terminal = time.monotonic_ns()
        attempts, validated = [], True
        try:
            # A command adapter owns controlbudget attempt receipts. No invented
            # success attempt if the sink is missing or an auth failure is hidden.
            if spec.get('mode') == 'cli':
                attempts = attempt_rows(receipt, logical)
                if kind in ('node_ls', 'ps'):
                    result = decode(raw)
                    key = 'nodes' if kind == 'node_ls' else 'processes'
                    probe.require(isinstance(result.get(key), list) and not result.get('error'), 'read_result_invalid')
                elif kind in ('read', 'short_exec'):
                    probe.require(raw.strip() == spec['expected_output'].replace('{logical_id}', logical).encode(),
                                  'original_read_or_exec_effect_missing')
            else:
                result = decode(raw)
                attempts = result['attempts']
                probe.require(result['logical_id'] == logical and result['native_rc'] == rc,
                              'control_result_binding')
        except (ValueError, KeyError, TypeError):
            # Preserve the real native result and every receipt already read;
            # invalid stdout or custody is a separate failed validation.
            validated = False
        row = dict(ordinal=ordinal, logical_id=logical, kind=kind, scheduled_ns=scheduled,
                   dispatched_ns=dispatched, terminal_ns=terminal, native_rc=rc, attempts=attempts, validated=validated)
        with self.lock:
            self.trace['slots'].append(row)

    def schedule(self):
        budget = probe.budgets()
        count = self.manifest['seconds'] * budget['HOT_UPDATE_PROBE_RATE']
        t0 = self.trace['t0_ns']
        self.progress_thread.start()
        try:
            # Include all six TLS lanes in the concurrency budget. Queuing is
            # charged from scheduled, never from when a worker becomes free.
            workers = (1 + len(self.deployment['tls'])) * budget['HOT_UPDATE_PROBE_RATE'] * (budget['HOT_UPDATE_REQUEST_S'] + 1)
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = []
                for ordinal in range(count):
                    scheduled = t0 + ordinal * 100_000_000
                    if self.stop_event.wait(max(0, (scheduled - time.monotonic_ns()) / 1_000_000_000)):
                        break
                    futures.append(pool.submit(self.slot, ordinal, scheduled))
                    for lane, config in self.deployment['tls'].items():
                        futures.append(pool.submit(self.tls_slot, lane, ordinal, scheduled, config))
                for future in futures:
                    future.result()
            with self.lock:
                self.trace['complete'] = len(self.trace['slots']) == count and not self.stop_event.is_set()
            # Keep the effect reader alive beyond the last request's deadline;
            # late duplicate effects are never erased by a successful first reply.
            self.stop_event.wait(budget['HOT_UPDATE_REQUEST_S'])
            with self.lock:
                self.trace['effects'] = self.reader('effects')
                self.trace['tail_complete'] = not self.stop_event.is_set()
        except (OSError, ValueError, KeyError, TypeError):
            self.failure = 'scheduler_or_custody_failed'
        finally:
            self.progress_thread.join(timeout=45)
            if self.progress_thread.is_alive():
                self.failure = 'progress_join_failed'
            self.save()

    def observe_progress(self):
        """Read targets and sealed custody without waiting for a phase change.

        This changes no product lifetime or pacing. Late/failed readers leave
        gaps in evidence and therefore cannot sign the five/ten-second bounds.
        """
        try:
            t0 = self.trace['t0_ns']
            for ordinal in range(self.manifest['seconds']):
                due = t0 + ordinal * 1_000_000_000
                if self.stop_event.wait(max(0, (due - time.monotonic_ns()) / 1_000_000_000)):
                    break
                row = self.reader('progress')
                probe.fields(row, 'progress')
                row['s7'] = self.s7('sample')
                with self.lock:
                    self.trace['progress'].append(row)
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
            self.failure = 'progress_custody_failed'

    def tls_slot(self, lane, ordinal, scheduled, config):
        row = fresh_tls(lane, ordinal, scheduled, config, self.supervisor,
                        probe.budgets()['HOT_UPDATE_REQUEST_S'])
        with self.lock:
            self.trace['tls'].append(row)

    def checkpoint(self, hop, phase):
        probe.require(hop in {h['id'] for h in self.manifest['hops']} and phase in probe.PHASES,
                      'undeclared_checkpoint')
        # The independent reader provides ACK-prefix-before-repair and every
        # original identity. S7 data is obtained directly from the custody keeper.
        row = self.reader('sample', hop, phase)
        probe.require(row['hop'] == hop and row['phase'] == phase, 'checkpoint_binding')
        row['s7'] = self.s7('sample')
        with self.lock:
            self.trace['samples'].append(row)
        self.save()
        return dict(native_rc=0, samples=len(self.trace['samples']))

    def observe(self, hops):
        """Arm the next declared block of hops in schedule order; never a reset.

        origin: S9 FIX-INFRAVERBS. The product command runs in the drill's
        foreground, so phase samples come from this watcher, not from caller
        checkpoints between phases that the caller cannot see.
        """
        declared = [h['id'] for h in self.manifest['hops']]
        done = len(self.observed)
        probe.require(isinstance(hops, list) and bool(hops) and hops == declared[done:done + len(hops)]
                      and (self.watch is None or not self.watch.is_alive()), 'undeclared_observation')
        self.observed.extend(hops)
        self.watch = threading.Thread(target=self.watch_block, args=(list(hops),), name='infra-hop-watch')
        self.watch.start()
        return dict(native_rc=0, observed=len(self.observed))

    def watch_block(self, hops):
        # The independent reader answers once the phase is physically observed.
        # A phase still absent after the component budget is a product-side gap
        # recorded for the oracle; reader failure is lost custody (SETUP).
        budget = probe.budgets()['HOT_UPDATE_REGISTER_DEADLINE_S'] + 10
        try:
            for hop in hops:
                for phase in probe.PHASES:
                    rc, raw = self.supervisor.run(command(self.deployment['readers']['sample']) + [hop, phase],
                                                  budget, track=self.watch_children)
                    if self.stop_event.is_set():
                        return
                    if rc == 124:
                        self.watch_failures.append('phase_not_observed:' + hop + ':' + phase)
                        return
                    probe.require(rc == 0, 'independent_reader_failed')
                    row = decode(raw)
                    probe.require(row['hop'] == hop and row['phase'] == phase, 'checkpoint_binding')
                    row['s7'] = self.s7('sample')
                    with self.lock:
                        self.trace['samples'].append(row)
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
            self.watch_failures.append('watch_custody_failed')
        finally:
            self.save()

    def snapshot(self, progress, join):
        """Persist the trace for a scoped oracle; optionally await evidence."""
        deadline = time.monotonic() + 30
        while (len(self.trace['progress']) < progress and self.failure is None
               and self.thread.is_alive() and time.monotonic() < deadline):
            time.sleep(.05)
        if join and self.watch is not None:
            self.watch.join(timeout=join)
        self.save()
        with self.lock:
            return dict(native_rc=0, samples=len(self.trace['samples']), progress=len(self.trace['progress']),
                        observed=list(self.observed), watching=bool(self.watch and self.watch.is_alive()),
                        watch_failures=list(self.watch_failures), failure=self.failure)

    def finish(self):
        self.thread.join(timeout=self.manifest['seconds'] + 30)
        probe.require(not self.thread.is_alive() and self.failure is None, 'fixture_join_failed')
        if self.watch is not None:
            self.watch.join(timeout=probe.budgets()['HOT_UPDATE_REGISTER_DEADLINE_S'] + 20)
        probe.require((self.watch is None or not self.watch.is_alive())
                      and 'watch_custody_failed' not in self.watch_failures, 'watch_join_failed')
        final = self.reader('final')
        with self.lock:
            self.trace.update(final_business=final['business'], controls=final['controls'], phase_lanes=final['phase_lanes'])
            self.trace['s7_final'] = self.s7('judge')
            spec = self.deployment['flow']
            probe.fields(spec, 'flow_binding')
            # origin: hot-update S9 plan §9.1 / T77. The S5 default is replaced;
            # every normal infra cut explicitly chooses unchanged instead.
            rc, _ = self.supervisor.run([sys.executable, '-I', '-B',
                str(Path(__file__).with_name('hot_update_flow_probe.py')), 'judge',
                '--evidence', spec['evidence'], '--nonce', spec['nonce'],
                '--carrier-transition', self.manifest['carrier_transition']], 30)
            self.trace['flow_native_rc'] = rc
        self.save()
        return probe.judge(self.manifest, self.trace)

    def close(self):
        self.stop_event.set()
        for child in list(self.watch_children):
            if child.poll() is None:
                child.terminate()
        if self.watch is not None:
            self.watch.join(timeout=60)
        self.thread.join(timeout=60)
        probe.require(not self.thread.is_alive() and not (self.watch and self.watch.is_alive()), 'scheduler_stop_join')
        self.save()


def unix_path(directory, operation):
    """Bind/connect through the directory FD: sun_path is 108 bytes, and a
    deep private evidence root must not turn into an unreachable fixture.
    origin: S9 FIX-INFRAVERBS (drill evidence roots exceed sun_path)."""
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        operation('/proc/self/fd/%d/fixture.sock' % fd)
    finally:
        os.close(fd)


def serve(args):
    bound_manifest = probe.sha(args.manifest)
    fixture = Fixture(probe.read_json(args.manifest), args.evidence, args.supervisor)
    path = args.evidence / 'fixture.sock'
    with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as server:
        unix_path(args.evidence, server.bind)
        path.chmod(0o600)
        server.listen(4)
        fixture.save()
        fixture.thread.start()
        ready = dict(native_rc=0, fixture_pid=os.getpid(), manifest_sha256=bound_manifest)
        publish(args.evidence / 'ready.json', ready)
        print(json.dumps(ready), flush=True)
        try:
            done = False
            while not done:
                with server.accept()[0] as peer:
                    peer.settimeout(10)
                    request = decode(peer.recv(65536))
                    probe.require(request['manifest_sha256'] == bound_manifest == probe.sha(args.manifest), 'manifest_changed')
                    verb = request['verb']
                    if verb == 'checkpoint':
                        result = fixture.checkpoint(request['hop'], request['phase'])
                    elif verb == 'observe':
                        result = fixture.observe(request['hops'])
                    elif verb == 'snapshot':
                        result = fixture.snapshot(int(request['progress']), int(request['join']))
                    elif verb == 'finish':
                        result = fixture.finish()
                    elif verb == 'stop':
                        result, done = dict(native_rc=0), True
                    else:
                        raise probe.EvidenceError('unknown_fixture_verb')
                    peer.send(json.dumps(result, sort_keys=True).encode())
        finally:
            fixture.close()
            path.unlink(missing_ok=True)


def request(evidence, manifest, payload, timeout):
    """One private socket request; the scheduler binds it to the frozen manifest."""
    path = Path(evidence) / 'fixture.sock'
    deadline = time.monotonic() + 10
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(.02)
    with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as peer:
        peer.settimeout(timeout)
        unix_path(Path(evidence), peer.connect)
        peer.send(json.dumps(dict(payload, manifest_sha256=probe.sha(manifest))).encode())
        return decode(peer.recv(probe.MAX_BYTES))


def call(args):
    timeout = probe.read_json(args.manifest)['seconds'] + 120 if args.command == 'finish' else 180
    result = request(args.evidence, args.manifest, dict(verb=args.command, hop=args.hop, phase=args.phase), timeout)
    print(json.dumps(result, sort_keys=True))
    return result['native_rc']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('serve', 'checkpoint', 'finish', 'stop', 'wait-start', 'exec-client'))
    parser.add_argument('--manifest', type=Path)
    parser.add_argument('--evidence', type=Path)
    parser.add_argument('--supervisor')
    parser.add_argument('--hop')
    parser.add_argument('--phase')
    parser.add_argument('--spec', type=Path)
    parser.add_argument('--logical-id')
    parser.add_argument('--receipt', type=Path)
    args = parser.parse_args()
    if args.command == 'exec-client':
        probe.require(args.spec is not None and args.logical_id and args.receipt is not None, 'client_arguments')
        exec_client(args.spec, args.logical_id, args.receipt)
        return 2
    probe.require(args.manifest is not None and args.evidence is not None, 'fixture_arguments')
    if args.command == 'serve':
        probe.require(args.supervisor is not None, 'shared_supervisor_required')
        serve(args)
        return 0
    if args.command == 'wait-start':
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if (args.evidence / 'ready.json').exists():
                ready = probe.read_json(args.evidence / 'ready.json')
                probe.require(ready['manifest_sha256'] == probe.sha(args.manifest)
                              and (args.evidence / 'fixture.sock').exists(), 'startup_binding')
                os.kill(ready['fixture_pid'], 0)
                return ready['native_rc']
            time.sleep(.02)
        raise probe.EvidenceError('startup_deadline')
    return call(args)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        # Only the closed custody code (or an exception type) crosses: never
        # reader bytes, paths or keys. A bare generic code hid which seam broke.
        reason = str(error) if isinstance(error, probe.EvidenceError) else type(error).__name__
        print(json.dumps(dict(native_rc=2, verdict='SETUP-RED', failures=['fixture_custody_failed', reason[:80]])))
        sys.exit(2)
