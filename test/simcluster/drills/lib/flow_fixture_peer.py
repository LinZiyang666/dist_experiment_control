#!/usr/bin/env python3
"""Container-only finite duplex workload and private descriptor custodian.

No reconnect is attempted. A target observes its own receive timestamps;
clients observe theirs. Socket owners stay alive through final collection.
Private captures and actual client Seal attempts are kept in sealed memfds;
the product's independent Seal ledger cannot be inferred from this ciphertext.
"""
import argparse
import array
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import select
import socket
import struct
import subprocess
import sys
import threading
import time
from types import SimpleNamespace


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


# origin: hot-update S10 plan §6 / §10.2 P12; lead L15.
# The shared launcher owns descendants, including after a worker-thread exec.
# READY supplies the actual target PID; the launcher's PID is not a socket owner.
def start_supervised(argv, **options):
    supervisor = os.environ.get("SIM_UPGRADE_SUPERVISOR", "")
    if not os.path.isabs(supervisor) or not os.access(supervisor, os.X_OK):
        raise ValueError("shared process supervisor is required")
    read_fd, write_fd = os.pipe()
    child = None
    try:
        child = subprocess.Popen([supervisor, "--ready-fd", str(write_fd), "--", *map(str, argv)],
                                 pass_fds=(write_fd,), **options)
        os.close(write_fd)
        write_fd = -1
        with os.fdopen(read_fd, "rb", buffering=0) as ready:
            read_fd = -1
            data = bytearray()
            deadline = time.monotonic() + 10
            while not data.endswith(b"\n"):
                if not select.select([ready], [], [], max(0, deadline-time.monotonic()))[0]:
                    raise ValueError("supervisor READY deadline")
                part = ready.read(1)
                if not part or len(data) >= 65536:
                    raise ValueError("supervisor READY incomplete")
                data.extend(part)
            child.census = json.loads(data)
            child.target_pid = child.census["target"]["pid"]
        return child
    except BaseException:
        if child is not None:
            child.terminate()
            child.wait(timeout=10)
            for stream in (child.stdin, child.stdout, child.stderr):
                if stream is not None:
                    stream.close()
        raise
    finally:
        for fd in (read_fd, write_fd):
            if fd >= 0:
                os.close(fd)


def run_supervised(argv, input=None, timeout=30, **options):
    if input is not None:
        options["stdin"] = subprocess.PIPE
    child = start_supervised(argv, **options)
    try:
        stdout, stderr = child.communicate(input, timeout=timeout)
    except BaseException:
        # Let the shared reaper finish; killing only the launcher would lose
        # its final native cleanup status and could strand remote ownership.
        child.terminate()
        child.communicate(timeout=10)
        raise
    return subprocess.CompletedProcess(argv, child.returncode, stdout, stderr)


probe = module("hot_update_flow_probe")
observation = module("flow_observation")


PUBLIC_ERRORS = frozenset(('connection_refused', 'connection_reset', 'broken_pipe', 'socket_timeout',
                          'peer_observation_failed', 'sealed_key_absent', 'application_redial_refused',
                          'original_ssh_not_established', 'name_resolution_failed'))


def public_error(error):
    # origin: hot-update S5 plan §6.6 / §9.4 / DRILL-RUN. Never serialize
    # exception text: socket/key parsing errors can contain private inputs.
    if isinstance(error, ValueError):
        return {'sealed key absent': 'sealed_key_absent', 'application redial refused': 'application_redial_refused',
                'original SSH did not establish': 'original_ssh_not_established'}.get(str(error), 'peer_observation_failed')
    return {socket.gaierror: 'name_resolution_failed', ConnectionRefusedError: 'connection_refused', ConnectionResetError: 'connection_reset',
            BrokenPipeError: 'broken_pipe', TimeoutError: 'socket_timeout'}.get(type(error), 'peer_observation_failed')


def public_observations(owner):
    # The failure receipt contains only control revisions, phases and counts.
    # No Data/Digest/Nonce field leaves sealed custody, even on an error path.
    # origin: hot-update S5 plan §6.6 / §9.4 / DRILL-RUN
    records = getattr(owner, 'observations', None)
    if records is None:
        return []
    kinds = {'ss-grant', 'ss-revoke-mark', 'ss-closed', 'final-cut', 'flow-retirement'}
    result = [dict(kind=e['kind'], at=e['at'], values=e['values']) for e in records.events
              if e['source'] == 'agt1' and e['kind'] in kinds][-128:]
    # origin: hot-update S5 plan §6.6 / §9.4 / DRILL-RUN 75.C. Poison
    # remains terminal; its fixed cause and counts locate the broken observer.
    failure = getattr(records, 'failure', None)
    if isinstance(failure, dict) and failure.get('source') in ('agt1','brk1','brk2','brk3') and failure.get('criterion') in observation.CUSTODY_FAILURES and isinstance(failure.get('values'),list) and len(failure['values'])==3 and all(type(value) is int and value>=0 for value in failure['values']):
        result.append(dict(kind='custody-failure', **failure))
    return result


def publish(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, allow_nan=False) + "\n")
    temporary.replace(path)


def sealed(data):
    fd = os.memfd_create("s5-private", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    try:
        with os.fdopen(os.dup(fd), "wb", closefd=True) as stream:
            stream.write(data)
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS, fcntl.F_SEAL_SEAL | fcntl.F_SEAL_SHRINK |
                    fcntl.F_SEAL_GROW | fcntl.F_SEAL_WRITE)
        return fd
    except BaseException:
        os.close(fd)
        raise


def exact(stream, length, quantum=0, pressure_until=0):
    chunks = bytearray()
    while len(chunks) < length:
        limited = quantum and (not pressure_until or time.monotonic() < pressure_until)
        data = stream.recv(min(length-len(chunks), quantum) if limited else length-len(chunks))
        if not data:
            raise EOFError
        chunks.extend(data)
        if limited and (not pressure_until or time.monotonic() < pressure_until):
            # origin: hot-update S5 plan §9.1 / DRILL-RUN. Small actual
            # reads free TCP capacity incrementally, exposing partial writes
            # instead of only alternating full writes with zero-byte waits.
            time.sleep(.001)
    return bytes(chunks)


def payload(name, seq, size):
    seed = hashlib.sha256((name + ":" + str(seq)).encode()).digest()
    return (seed * ((size + 31) // 32))[:size]


def payload_size(seq, size, burst=False):
    return size if not burst or 20 <= seq < 532 else 128


def declaration(name, nonce, count, size, burst=False):
    digest, total = hashlib.sha256(), 0
    for seq in range(count):
        raw = probe.frame(nonce, seq, payload(name, seq, payload_size(seq, size, burst)))
        digest.update(raw)
        total += len(raw)
    return dict(records=count, bytes=total, sha256=digest.hexdigest())


def tcp_owner(pid, nonce, ancestors=False):
    # origin: hot-update S5 plan §9.1 / S4 pid-1 census lesson. Pipes identify
    # the SSH channel; /proc independently identifies its actual TCP owner.
    while pid > 1:
        root = Path('/proc') / str(pid)
        inodes = set()
        for family in ('tcp','tcp6'):
            for line in (root/'net'/family).read_text().splitlines()[1:]:
                fields=line.split()
                if fields[3] in ('01','08'):
                    inodes.add(int(fields[9]))
        for path in (root/'fd').iterdir():
            try:
                if path.stat().st_ino in inodes and os.readlink(path).startswith('socket:'):
                    return probe.identity(pid,int(path.name),nonce)
            except FileNotFoundError:
                continue
        if not ancestors:
            break
        fields=(root/'stat').read_text().rsplit(')',1)[1].split()
        pid=int(fields[1])
    raise probe.EvidenceError('original SSH socket unavailable')


def ssh_command(spec, remote, nonce):
    # A single SSH process and channel; OpenSSH is never restarted or retried.
    return ['/usr/bin/ssh','-T','-o','PreferredAuthentications=password','-o','NumberOfPasswordPrompts=1',
            '-o','StrictHostKeyChecking=no','-o','UserKnownHostsFile=/dev/null','-o','ConnectTimeout=5',
            '-p',str(spec['port']),spec.get('ssh_user', 'sim')+'@'+spec['host'],
            'python3','-I','-B',remote+'/flow_fixture_peer.py','stdio','--root',remote,'--nonce',nonce]


def supply_fd(entry, target):
    # origin: hot-update S5 plan §6.6 / T48. The private SS target tuple and
    # master travel in the same sealed descriptor, never a JSON control bundle.
    if len(target) != 6:
        raise ValueError('invalid private supply')
    password=entry['password'].encode()
    first=hashlib.md5(password).digest()
    master=first+hashlib.md5(first+password).digest()
    host=entry['server'].encode()
    return sealed(struct.pack('!HH',len(host),entry['port'])+host+target+master)


class SSHStream:
    def __init__(self, child):
        self.child=child

    def sendall(self,data):
        cursor=0
        while cursor<len(data):
            cursor+=os.write(self.child.stdin.fileno(),data[cursor:])

    def recv(self,length):
        return os.read(self.child.stdout.fileno(),length)

    def shutdown(self,_):
        self.close()

    def close(self):
        if self.child.poll() is None:
            self.child.terminate()
        status = self.child.wait(timeout=10)
        self.child.stdin.close(); self.child.stdout.close()
        if status == 125:
            raise ValueError("SSH descendant cleanup failed")


class SS:
    """Independent client: record at the primitive, including failed writes."""
    def __init__(self, stream, master, read_quantum=0):
        self.stream, self.master = stream, master
        self.read_quantum = read_quantum
        self.pressure_until = 0
        self.salt = os.urandom(32)
        self.key = probe.ss_subkey(master, self.salt)
        self.tx, self.rx = 0, 0
        self.rx_key = None
        self.up, self.down = bytearray(self.salt), bytearray()
        self.calls, self.partials = [], []
        stream.sendall(self.salt)

    def seal(self, data):
        nonce = self.tx.to_bytes(12, "little")
        self.calls.append(dict(subkey_sha256=probe.digest(self.key), nonce=nonce.hex()))
        wire = probe.xor_stream(self.key, nonce, data)
        self.tx += 1
        return wire + probe.aead_tag(self.key, nonce, wire)

    def send(self, data):
        wire = bytearray()
        for offset in range(0, len(data), 0x3fff):
            chunk = data[offset:offset + 0x3fff]
            wire.extend(self.seal(struct.pack("!H", len(chunk))) + self.seal(chunk))
        self.up.extend(wire)
        # origin: hot-update S5 plan §9.1 / DRILL-RUN. Batch the already
        # sealed application frame: individual 16 KiB writes fit a Linux TCP
        # skb even with a small send buffer and hide the required short n.
        cursor = 0
        while cursor < len(wire):
            attempt = len(wire) - cursor
            try:
                n = self.stream.send(wire[cursor:], socket.MSG_DONTWAIT) if isinstance(self.stream, socket.socket) else self.stream.send(wire[cursor:])
            except BlockingIOError:
                select.select([], [self.stream], [], .1)
                continue
            if n == 0:
                raise OSError("zero socket write")
            if 0 < n < attempt:
                self.partials.append(dict(attempt_len=attempt, written=n,
                                          suffix_bytes=attempt-n, seal_calls=sum(
                                              call['nonce']==(self.tx-1).to_bytes(12,'little').hex() and
                                              call['subkey_sha256']==probe.digest(self.key) for call in self.calls)))
            cursor += n

    def receive(self):
        if self.rx_key is None:
            salt = exact(self.stream, 32, self.read_quantum, self.pressure_until)
            self.down.extend(salt)
            self.rx_key = probe.ss_subkey(self.master, salt)
        wire = exact(self.stream, 18, self.read_quantum, self.pressure_until)
        self.down.extend(wire)
        length = int.from_bytes(probe.open_aead(self.rx_key, self.rx.to_bytes(12, "little"), wire), "big")
        self.rx += 1
        if not 0 < length <= 0x3fff:
            raise ValueError("invalid SS length")
        wire = exact(self.stream, length + 16, self.read_quantum, self.pressure_until)
        self.down.extend(wire)
        clear = probe.open_aead(self.rx_key, self.rx.to_bytes(12, "little"), wire)
        self.rx += 1
        return clear


def connect_socket(host, port, burst=False):
    stream = socket.socket()
    try:
        # origin: hot-update S5 plan §9.1 / DRILL-RUN. Loopback's large
        # MSS can absorb whole cipher records even under receiver pressure.
        # Negotiate ordinary Ethernet-sized segments before connecting, so
        # the actual product writer can expose a positive short suffix.
        if burst:
            stream.setsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG, 1200)
        stream.settimeout(5)
        stream.connect((host, port))
        stream.settimeout(None)
        return stream
    except BaseException:
        stream.close()
        raise


def settled_socket_identity(stream, original):
    # A complete natural FIN may remove the kernel tuple. Its application
    # descriptor must still be the same OFD in the same original process.
    # Call only after exact finite bytes and the final peer EOF were checked.
    # origin: hot-update S5 plan lead C1 / §3.4 / DRILL-RUN
    stat = os.fstat(stream.fileno())
    if (stat.st_dev,stat.st_ino,stat.st_mode & 0o170000) != (original["dev"],original["ino"],original["type"]):
        raise probe.EvidenceError("original socket or application nonce absent")
    process = probe.identity(os.getpid())
    if any(process[key] != original[key] for key in ("pid","boot_id","start_ticks")):
        raise probe.EvidenceError("process changed during identity collection")
    return dict(original)

class Flow:
    def __init__(self, peer, name, stream, spec, crypto=None, identity_reader=None):
        self.peer, self.name, self.stream, self.spec, self.crypto = peer, name, stream, spec, crypto
        self.lock = threading.Lock()
        self.sent, self.received, self.events, self.identities = [], [], [], []
        self.start = time.monotonic()
        self.fault = None
        self.end = None
        self.done = threading.Event()
        self.tx_done = threading.Event()
        self.drain = threading.Event()
        self.fin_at = None
        self.silent = threading.Event()
        self.accepts = 1
        self.threads = []
        self.identity_reader = identity_reader or (lambda: probe.identity(os.getpid(),self.stream.fileno(),self.peer.nonce))
        self.snapshot_identity()

    def snapshot_identity(self):
        try:
            # A fully FIN-settled TCP has no kernel peer tuple any longer.
            # Its still-owned application OFD must keep the ORIGINAL inode;
            # only the complete natural-FIN cohort may use this terminal view.
            # Normal hot-cut identities always use the live tuple reader.
            # origin: hot-update S5 plan lead C1 / §3.4 / DRILL-RUN
            if self.spec.get("finite_fin") and self.fin_at is not None and self.tx_done.is_set() and \
                    [event["kind"] for event in self.events] == ["eof"] and \
                    len(self.sent) == self.spec["down" if self.peer.role == "target" else "up"]["records"] and \
                    len(self.received) == self.spec["up" if self.peer.role == "target" else "down"]["records"]:
                value = settled_socket_identity(self.stream, self.identities[0])
            else:
                value=self.identity_reader()
        except (OSError,probe.EvidenceError) as error:
            if not self.identities:
                raise
            # Losing the owner is sticky evidence, never a substituted socket
            # or an assertion that the old descriptor still exists.
            # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.A.
            # Preserve the failing census guard without publishing arbitrary
            # exception text, which can contain paths or private material.
            criteria = {'original socket or application nonce absent':'socket_absent',
                        'original socket tuple unavailable':'tuple_unavailable',
                        'original SSH socket unavailable':'ssh_socket_unavailable',
                        'process changed during identity collection':'process_changed'}
            failure = dict(kind='os_error',errno=error.errno) if isinstance(error,OSError) else dict(
                kind=criteria.get(str(error),'identity_unavailable'))
            value=dict(self.identities[-1],owner_lost=True,identity_failure=failure)
        self.identities.append(value)

    def run(self):
        for task in (self.send, self.receive):
            thread = threading.Thread(target=task)
            thread.start()
            self.threads.append(thread)

    def event(self, kind):
        with self.lock:
            self.events.append(dict(kind=kind, at=time.monotonic()))

    def send(self):
        direction = "down" if self.peer.role == "target" else "up"
        count = self.spec[direction]["records"]
        path = self.peer.root / (self.name + ".sent")
        drain = None
        try:
            with path.open("xb", buffering=0) as output:
                batch = []
                for seq in range(count):
                    while self.silent.is_set() and not self.peer.stop.is_set():
                        time.sleep(.01)
                    if self.peer.stop.is_set():
                        return
                    # Finishing drains the PREDECLARED tail; it never changes
                    # the expected totals to excuse an observed loss.
                    if not self.peer.finish.is_set() and not self.drain.is_set():
                        time.sleep(.001 if self.spec.get("burst") and 20 <= seq < 532 else
                                   .5 if self.spec["class"] == "slow" else
                                   .25 if self.spec["class"] == "half-close" and direction == "down" else
                                   1 / self.spec.get("supply", {}).get("rate", 10))
                    else:
                        # The predeclared tails end together. Eagerly draining
                        # fast raw streams creates silence while AEAD drains.
                        # This changes pacing, never the finite byte contract.
                        # origin: hot-update S5 plan §9.1 / DRILL-RUN
                        if drain is None:
                            remaining = count-seq
                            drain = (time.monotonic(),seq,remaining,
                                     min(self.spec.get("supply", {}).get("drain_seconds", 15), remaining*.1))
                        started,first,remaining,duration = drain
                        delay = started+duration*(seq-first+1)/remaining-time.monotonic()
                        if delay > 0:
                            time.sleep(delay)
                    size = payload_size(seq, self.spec["size"], self.spec.get("burst", False))
                    raw = probe.frame(self.peer.nonce, seq, payload(self.name + "." + direction, seq, size))
                    batch.append((seq, size, raw))
                    # origin: hot-update S5 plan §9.1 / DRILL-RUN. Four
                    # predeclared frames exceed TCP GSO's full-write quantum;
                    # a single 32 KiB frame can still be accepted atomically.
                    if self.crypto and self.spec.get("burst") and 20 <= seq < 532 and seq % 4 != 3:
                        continue
                    raw = b"".join(row[2] for row in batch)
                    if self.crypto:
                        self.crypto.send(raw)
                    else:
                        self.stream.sendall(raw)
                    output.write(raw)
                    with self.lock:
                        for index, length, record in batch:
                            self.sent.append(dict(seq=index, length=length,
                                                  sha256=probe.digest(record[probe.HEADER.size:]), at=time.monotonic()))
                    batch = []
                # Natural FIN follows the complete PREDECLARED saturation tail.
                # origin: hot-update S5 plan lead C1 / §3.4 / DRILL-RUN
                if self.spec.get("finite_fin") or self.spec["class"] == "half-close" and direction == "up":
                    self.stream.shutdown(socket.SHUT_WR)
                    self.fin_at = time.monotonic()
        except (OSError, EOFError, ValueError, probe.ContinuityError):
            self.event("rst")
        finally:
            self.tx_done.set()

    def receive(self):
        pending = bytearray()
        pressure_started = False
        try:
            with (self.peer.root / (self.name + ".received")).open("xb", buffering=0) as output:
                while not self.peer.stop.is_set():
                    if self.spec.get("burst") and not self.peer.finish.is_set():
                        # A paced original receiver creates real pressure,
                        # while every record still advances within five seconds.
                        if self.crypto and len(self.received) >= 18 and not pressure_started:
                            # Each original cipher receiver holds one bounded
                            # first-burst window. Incidental scheduling must
                            # not decide which actor obtains the partial prior.
                            # origin: hot-update S5 plan §9.1 / DRILL-RUN
                            pressure_started = True
                            # The quantum was formerly retained through all
                            # 16 MiB of the burst, starving the raw sender for
                            # over five seconds despite continuous SS reads.
                            # Bound only this artificial pressure experiment;
                            # retain every byte and the actual partial prior.
                            # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.L
                            self.crypto.pressure_until = time.monotonic()+4
                            time.sleep(3)
                        else:
                            time.sleep((.02 if self.crypto else .01) if 20 <= len(self.received) < 532 else .001)
                    elif self.crypto and self.peer.finish.is_set():
                        # Drain the same finite ledger after the pressure
                        # experiment; retain its key, nonce and original FD.
                        self.crypto.read_quantum = 0
                    data = self.crypto.receive() if self.crypto else self.stream.recv(65536)
                    if not data:
                        self.event("eof")
                        break
                    output.write(data)
                    pending.extend(data)
                    while len(pending) >= probe.HEADER.size:
                        length = probe.HEADER.unpack_from(pending)[3]
                        if length > probe.MAX_PAYLOAD:
                            raise ValueError("frame length")
                        end = probe.HEADER.size + length
                        if len(pending) < end:
                            break
                        row = probe.parse_frames(bytes(pending[:end]), self.peer.nonce)[0]
                        del pending[:end]
                        with self.lock:
                            self.received.append(dict(row, at=time.monotonic()))
                        wanted = self.spec["up" if self.peer.role == "target" else "down"]["records"]
                        if len(self.received) == wanted:
                            self.end = time.monotonic()
                            if self.spec["class"] != "half-close" or self.peer.role == "client":
                                self.done.set()
                            # Continue observing the original socket. Returning
                            # here would hide duplicated trailing frames/RST.
                if self.end is None:
                    self.end = time.monotonic()
                self.done.set()
        except ConnectionResetError:
            self.event("rst")
            self.end = time.monotonic()
            self.done.set()
        except (OSError, EOFError):
            self.event("eof" if not pending else "rst")
            self.end = time.monotonic()
            self.done.set()
        except (ValueError, probe.ContinuityError):
            self.event("rst")
            self.end = time.monotonic()
            self.done.set()

    def snapshot(self):
        # Keep the owner alive and its socket open for final identity checks.
        self.snapshot_identity()
        now = time.monotonic()
        window = dict(start=self.start, fault=self.fault if self.fault is not None else now,
                      end=getattr(self.peer,'observation_end',None) or now, finish_deadline=self.peer.deadline)
        with self.lock:
            value = dict(sent=list(self.sent), received=list(self.received), events=list(self.events),
                         identities=list(self.identities), window=window, completed_at=self.end,
                         target_accepts=self.accepts, fin_at=self.fin_at)
        publish(self.peer.root / (self.name + ".json"), value)
        return value


# origin: hot-update S5 plan §9.2 / DRILL-RUN. The product removes a
# delivered terminal marker after registering. Observe its real atomic file
# locally; host transport latency must not turn successful delivery into absence.
def watch_marker(owner, path):
    while not owner.stop.is_set():
        try:
            value = json.loads(path.read_text())
            if value.get('state') == 'rolled_back':
                owner.rollback_marker = dict(at=time.monotonic(), marker={key:value[key] for key in
                    ('state','generation','upgrade_id','new_sha','prev_sha')})
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        owner.stop.wait(.005)


class Peer:
    def __init__(self, root, nonce, role, listen_host="0.0.0.0", listen_port=0):
        self.root, self.nonce, self.role = root, nonce, role
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.stop, self.finish = threading.Event(), threading.Event()
        self.flows, self.keys, self.specs = {}, {}, {}
        self.short_specs, self.short_counts = {}, {}
        self.private_fds = []
        self.ssh_identities = []
        self.deadline = time.monotonic() + 660
        self.listener = None
        self.open_listener = None
        self.workers = []
        self.listen_address = (listen_host, listen_port)
        self.lock = threading.Lock()
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.server.bind(str(root / "peer.sock"))
        self.server.listen(16)
        os.chmod(root / "peer.sock", 0o600)

    def target(self):
        self.listener = socket.socket()
        # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.R.
        # Match the ordinary MSS used on the cipher side before the SYN. A
        # loopback-sized segment with our bounded sender induces delayed ACK
        # stalls instead of sustained pressure on the original product writer.
        self.listener.setsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG, 1200)
        self.listener.bind(self.listen_address)
        self.listener.listen(64)
        self.open_listener = socket.socket()
        self.open_listener.bind(("0.0.0.0", 0))
        self.open_listener.listen(8)
        publish(self.root / "ready.json", dict(port=self.listener.getsockname()[1],
                open_port=self.open_listener.getsockname()[1], identity=probe.identity(os.getpid())))
        def accept():
            while not self.stop.is_set():
                ready, _, _ = select.select((self.listener, self.open_listener), (), (), .2)
                for listener in ready:
                    stream, _ = listener.accept()
                    thread = threading.Thread(target=self.accept_one, args=(stream,))
                    thread.start()
                    self.workers.append(thread)
        thread = threading.Thread(target=accept)
        thread.start()
        self.workers.append(thread)

    def accept_one(self, stream):
        try:
            stream.settimeout(5)
            header = bytearray()
            while not header.endswith(b"\n") and len(header) < 80:
                header.extend(exact(stream, 1))
            name = header.decode().strip()
            if name in self.short_specs:
                # These separately declared short connections exercise actual
                # CLOSE/retirement between B's cuts; original survivors stay open.
                # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
                expected = bytes.fromhex(self.short_specs[name])
                with self.lock:
                    self.short_counts[name] = self.short_counts.get(name, 0) + 1
                if exact(stream, len(expected)) != expected or stream.recv(1):
                    raise ValueError('short_flow_bytes')
                stream.sendall(expected)
                stream.shutdown(socket.SHUT_WR)
                stream.close()
                return
            if not re.fullmatch(r"[a-z0-9-]+", name) or name not in self.specs:
                raise ValueError("foreign flow")
            stream.settimeout(None)
            if self.specs[name].get('burst'):
                # Match the client's bounded send quantum. An autotuned large
                # target send buffer waits for a large low-watermark drain and
                # can hold sendall for >5s despite steady receiver progress.
                # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.L
                stream.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
                stream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            with self.lock:
                if name in self.flows:
                    self.flows[name].accepts += 1
                    stream.close()
                    return
                flow = Flow(self, name, stream, self.specs[name])
                self.flows[name] = flow
            flow.run()
        except (OSError, EOFError, ValueError):
            stream.close()

    def connect(self, spec):
        name = spec["id"]
        if name in self.flows:
            raise ValueError("application redial refused")
        if name == 'ssh':
            env=dict(os.environ,SSH_ASKPASS=str(self.root/'flow_fixture_askpass.sh'),
                     SSH_ASKPASS_REQUIRE='force',DISPLAY='s5-fixture')
            with (self.root/'ssh.stderr').open('xb') as errors:
                child=start_supervised(ssh_command(spec,spec.get('ssh_root', str(self.root)),self.nonce),stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,stderr=errors,env=env)
            stream=SSHStream(child)
            deadline=time.monotonic()+5
            while True:
                try:
                    tcp_owner(child.target_pid,self.nonce)
                    break
                except (OSError,probe.EvidenceError):
                    if time.monotonic()>=deadline or child.poll() is not None:
                        stream.close()
                        raise ValueError('original SSH did not establish')
                    time.sleep(.01)
            flow=Flow(self,name,stream,spec,identity_reader=lambda: tcp_owner(child.target_pid,self.nonce))
            self.flows[name]=flow
            flow.run()
            return
        stream = connect_socket(spec["host"], spec["port"], spec.get("burst", False))
        if spec.get("burst"):
            stream.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
            # origin: hot-update S5 plan §9.1 / DRILL-RUN. Incremental
            # reads create actual short writes without a tiny receive window
            # forcing delayed TCP window updates on otherwise normal flows.
            stream.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
            stream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        crypto = None
        if spec["protocol"] == "ss":
            if spec["key"] not in self.keys:
                raise ValueError("sealed key absent")
            data = probe.sealed_bytes(self.keys[spec["key"]])
            master = data[-32:]
            # Pace an original cipher record over several actor poll intervals;
            # releasing a whole frame per poll can yield only full/zero writes.
            # origin: hot-update S5 plan §9.1 / DRILL-RUN
            crypto = SS(stream, master, read_quantum=128 if spec.get("burst") else 0)
            target = b"\x01" + data[-38:-32]
            crypto.send(target + name.encode() + b"\n")
        else:
            stream.sendall(name.encode() + b"\n")
        flow = Flow(self, name, stream, spec, crypto)
        self.flows[name] = flow
        flow.run()

    def command(self, request, rights=()):
        command = request["command"]
        if command == "key":
            if len(rights) != 1:
                raise ValueError("sealed custody missing")
            probe.sealed_bytes(rights[0])
            self.keys[request["name"]] = rights[0]
            return dict(installed=True)
        if command == "configure":
            # origin: hot-update S10 plan §3.2 / §10.2 P12.
            # A live source is immutable. Reconfigure cannot forgive missing
            # records or move the end fence after a cut.
            if self.specs or self.flows:
                raise ValueError("flow declarations already sealed")
            for row in request["flows"]:
                if "supply" in row:
                    module("fullstack_flow").validate_source(row, self.nonce)
            self.specs = {row["id"]: row for row in request["flows"]}
            if len(self.specs) != len(request["flows"]):
                raise ValueError("duplicate flow declaration")
            if any("supply" in row for row in self.specs.values()):
                self.deadline = time.monotonic() + max(row["supply"]["ceiling_seconds"]
                    for row in self.specs.values()) + 45
            self.short_specs = request.get('short_flows', {})
            if request.get('watch_marker'):
                if self.role != 'target' or hasattr(self, 'marker_worker'):
                    raise ValueError('invalid_marker_observer')
                self.marker_worker = threading.Thread(target=watch_marker, args=(self, Path('/home/sim/.local/bin/.tether-upgrade.json')))
                self.marker_worker.start()
                self.workers.append(self.marker_worker)
            if 'ssh' in self.specs:
                # Only synthetic application declarations, never SS state.
                row=self.specs['ssh']
                publish(self.root/'ssh-spec.json',{k:row[k] for k in ('id','class','size','up','down','supply') if k in row})
            return dict(configured=True)
        if command == "drain":
            # origin: hot-update S10 plan §3.2 / §10.2 P12.
            # Arm every original sender before any peer waits for its receiver.
            # This consumes only the immutable predeclared tail.
            self.finish.set()
            if self.role == "target":
                (self.root / "ssh-finish").touch()
            return dict(draining=True)
        if command == "start":
            for row in request["flows"]:
                if "supply" in row and row != self.specs.get(row["id"]):
                    raise ValueError("original start declaration changed")
                self.connect(row)
            return dict(started=True)
        if command == "open":
            self.connect(request["flow"])
            return dict(opened=True)
        if command == 'short-flow':
            data = bytes.fromhex(request['payload'])
            with connect_socket(request['host'], request['port']) as stream:
                stream.settimeout(5)
                stream.sendall(request['name'].encode()+b'\n'+data)
                stream.shutdown(socket.SHUT_WR)
                if exact(stream, len(data)) != data or stream.recv(1):
                    raise ValueError('short_flow_bytes')
            return dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        # origin: hot-update S5 plan lead C1 / §3.4 / DRILL-RUN.
        # The excess socket stays original while product FIFO admission waits.
        if command == "capacity-open":
            if hasattr(self, "capacity_pending"):
                raise ValueError("application redial refused")
            self.capacity_pending = connect_socket(request["host"], request["port"])
            self.capacity_pending.sendall(request["name"].encode()+b"\n"+bytes.fromhex(request["payload"]))
            self.capacity_pending.shutdown(socket.SHUT_WR)
            self.capacity_identity = probe.identity(os.getpid(), self.capacity_pending.fileno(), self.nonce)
            return dict(identity=self.capacity_identity)
        if command == "capacity-recover":
            stream = self.capacity_pending
            stream.settimeout(5)
            data = bytes.fromhex(request["payload"])
            if exact(stream, len(data)) != data or stream.recv(1):
                raise ValueError("capacity recovery bytes")
            identity = settled_socket_identity(stream, self.capacity_identity)
            if identity != self.capacity_identity:
                raise ValueError("capacity recovery identity")
            stream.close()
            return dict(identity=identity, bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        if command == "capacity-exec":
            token = self.nonce + "-capacity"
            result = run_supervised(["tether", "exec", "agt1", "--", "sh", "-c",
                'printf "%s\\n" "$1" >> "$2"; printf "%s\\n" "$1"', "capacity", token,
                str(self.root / "capacity-exec-ledger")], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
            # Preserve the native class and only the exact public error enum.
            # A timeout, generic reset, or other terminal error cannot count.
            capacity = result.returncode == 70 and result.stdout == b"" and result.stderr.strip() in (
                b"error: exec: workload: replay capacity exhausted",)
            return dict(native_rc=result.returncode, error_type="workload.ErrBackpressure" if capacity else None,
                        success=result.returncode == 0 and result.stdout == (token+"\n").encode() and not result.stderr)
        if command == "capacity-exec-ledger":
            path = self.root / "capacity-exec-ledger"
            return dict(lines=path.read_text().splitlines() if path.exists() else [])
        if command == "drain-saturation":
            names = request["names"]
            deadline = time.monotonic()+45
            for name in names:
                self.flows[name].drain.set()
            for name in names:
                for thread in self.flows[name].threads:
                    thread.join(timeout=max(0, deadline-time.monotonic()))
                if any(thread.is_alive() for thread in self.flows[name].threads):
                    raise ValueError("saturation FIN deadline")
            return dict(flows={name:self.flows[name].snapshot() for name in names})
        if command == "retire-saturation":
            for name in request["names"]:
                flow = self.flows[name]
                if flow.fin_at is None or [e["kind"] for e in flow.events] != ["eof"] or any(thread.is_alive() for thread in flow.threads):
                    raise ValueError("saturation before FIN")
                flow.stream.close()
                del self.flows[name]
            return dict(retired=len(request["names"]))
        if command == "product-receipt":
            return observation.product_receipt(self,request['label'],request)
        if command == 'rollback-marker':
            return dict(observation=getattr(self, 'rollback_marker', None))
        if command == "mark":
            if self.role=='target':
                (self.root/'ssh-mark').touch()
            for flow in self.flows.values():
                if flow.fault is None:
                    flow.fault = time.monotonic()
            return dict(marked=True, observed_at=time.monotonic())
        if command == "mark-opening":
            # Only the original pending OPEN uses the second fault window.
            # Never overwrite other flows' image-cut observations.
            # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.A
            flow = self.flows.get("opening")
            if flow is None or len(flow.sent) < 20 or len(flow.received) < 20:
                raise ValueError("opening_duplex_samples_missing")
            flow.fault = time.monotonic()
            return dict(marked=True, observed_at=flow.fault)
        if command == "silent":
            for flow in self.flows.values():
                if flow.name.startswith("alice"):
                    flow.silent.set()
            return dict(silent=True)
        if command == "snapshot":
            # A pending OPEN needs the actual Accept census, not megabytes of
            # unrelated byte ledgers. Full snapshots still own continuity.
            # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
            if request.get('accepts_only'):
                return dict(identity=probe.identity(os.getpid()),
                            flows={name:dict(target_accepts=flow.accepts) for name,flow in self.flows.items()})
            flows={name:flow.snapshot() for name,flow in self.flows.items()}
            if self.role=='target' and (self.root/'ssh.json').exists():
                row=probe.read_json(self.root/'ssh.json')
                try:
                    value=tcp_owner(row['identities'][0]['pid'],self.nonce,ancestors=True)
                except (OSError,probe.EvidenceError):
                    if not self.ssh_identities:
                        raise
                    value=dict(self.ssh_identities[-1],owner_lost=True)
                self.ssh_identities.append(value)
                row['identities']=list(self.ssh_identities)
                flows['ssh']=row
            return dict(identity=probe.identity(os.getpid()), flows=flows, short_accepts=dict(self.short_counts))
        if command == "finish":
            self.finish.set()
            if self.role=='target':
                (self.root/'ssh-finish').touch()
            deadline = time.monotonic() + 45
            for flow in self.flows.values():
                if not flow.name.startswith("alice"):
                    flow.done.wait(max(0, deadline-time.monotonic()))
                    flow.tx_done.wait(max(0, deadline-time.monotonic()))
            if self.role == 'target' and 'ssh' in self.specs:
                # The SSH child writes independently; its last published trace
                # must contain both tails before the root observer captures it.
                while time.monotonic() < deadline:
                    if (self.root/'ssh.json').exists():
                        row=probe.read_json(self.root/'ssh.json')
                        if len(row['sent']) >= self.specs['ssh']['down']['records'] and len(row['received']) >= self.specs['ssh']['up']['records']:
                            break
                    time.sleep(.01)
            # End the finite observation once, before later census/collection.
            # Continue capturing bytes, identities and sticky EOF/RST events;
            # any extra or missing record still fails the unchanged judge.
            # origin: hot-update S5 plan §9.1 / T47 / DRILL-RUN
            if not hasattr(self,'observation_end'):
                self.observation_end = time.monotonic()
            return self.command(dict(command="snapshot"))
        if command == "judge":
            return judge_product(self,request)
        if command == "cleanup":
            self.stop.set()
            if hasattr(self, "capacity_pending"):
                self.capacity_pending.close()
            if self.role=='target':
                (self.root/'ssh-cleanup').touch()
            for flow in self.flows.values():
                try:
                    flow.stream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                flow.stream.close()
            return dict(cleaned=True)
        raise ValueError("unknown peer command")

    def run(self):
        if self.role == "target":
            self.target()
        else:
            publish(self.root / "ready.json", dict(identity=probe.identity(os.getpid())))
        try:
            while not self.stop.is_set():
                with self.server.accept()[0] as connection:
                    rights = []
                    try:
                        message = bytearray()
                        while not message.endswith(b"\n"):
                            chunk, control, flags, _ = connection.recvmsg(1 << 20, socket.CMSG_SPACE(16))
                            for level, kind, data in control:
                                if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                                    values = array.array("i")
                                    values.frombytes(data[:len(data)-len(data)%values.itemsize])
                                    rights.extend(values)
                            if not chunk or flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC):
                                raise ValueError("truncated custody message")
                            message.extend(chunk)
                            if len(message) > 32 << 20:
                                raise ValueError("checkpoint message too large")
                        value = self.command(json.loads(message), rights)
                    except observation.CustodyError as error:
                        # origin: hot-update S5 plan §9.4 / DRILL-RUN. The
                        # observer's fixed criterion identifies the failed
                        # oracle without publishing any private event fields.
                        for fd in rights:
                            os.close(fd)
                        value = dict(rc=2, criterion=str(error), public_observations=public_observations(self))
                    except (OSError, ValueError, KeyError, probe.EvidenceError) as error:
                        for fd in rights:
                            os.close(fd)
                        value = dict(rc=2, criterion=public_error(error))
                    # origin: hot-update S5 plan §9.1 / DRILL-RUN. A full
                    # ledger exceeds SO_SNDBUF's per-packet limit. The caller
                    # already joins packets through the final newline.
                    response = json.dumps(value).encode()+b"\n"
                    for offset in range(0, len(response), 32768):
                        connection.sendall(response[offset:offset+32768])
        finally:
            self.stop.set()
            for flow in self.flows.values():
                for thread in flow.threads:
                    thread.join(timeout=2)
            for thread in self.workers:
                thread.join(timeout=2)
            if self.listener:
                self.listener.close()
            if self.open_listener:
                self.open_listener.close()
            for fd in self.keys.values():
                os.close(fd)
            for fd in self.private_fds:
                os.close(fd)
            if hasattr(self,'observations'):
                self.observations.close()
            self.server.close()
            (self.root / "peer.sock").unlink(missing_ok=True)


def call(root, request, fd=None):
    with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as stream:
        stream.settimeout(60)
        stream.connect(str(root / "peer.sock"))
        control = [] if fd is None else [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [fd]))]
        # origin: hot-update S5 plan §9.1 / DRILL-RUN. Request ledgers
        # grow too; transfer private custody once, then complete the same RPC.
        message = json.dumps(request).encode()+b"\n"
        for offset in range(0, len(message), 32768):
            stream.sendmsg([message[offset:offset+32768]], control if offset == 0 else [])
        data = bytearray()
        while not data.endswith(b"\n"):
            chunk = stream.recv(1 << 20)
            if not chunk:
                raise ValueError("keeper disappeared")
            data.extend(chunk)
        return json.loads(data)


def judge_product(peer,request):
    document=probe.read_json(peer.root/'evidence.json')
    original=document['flows']
    # Native original raw loss keeps rc1 even if custody is unavailable. Crypto
    # validation follows that independent raw judge, and can never bless loss.
    document['flows']=[row for row in original if row['protocol']=='raw']
    publish(peer.root/'evidence.json',document)
    def run():
        # origin: hot-update S10 plan §6 / §10.2 P12.
        # Run the same pure oracle in its descriptor custodian. Forwarding
        # private FDs through an argv-based launcher would lose sealed custody.
        try:
            value = probe.judge(peer.root, peer.nonce, request['carrier_transition'])
            return dict(value, rc=0)
        except probe.ContinuityError as error:
            return dict(rc=1, verdict="ASSERT-FAIL", criterion=str(error))
        except (probe.EvidenceError, OSError, ValueError, KeyError, TypeError,
                AttributeError, IndexError, OverflowError, struct.error):
            return dict(rc=2, verdict="SETUP-RED", criterion="invalid_evidence")
    # Validate actual raw bytes before optional carrier/crypto metadata. The
    # historical exporter cannot supply those private surfaces, but its real
    # original EOF/RST loss must keep the native assertion status.
    try:
        for row in document['flows']:
            for direction in ('up','down'):
                record=row['directions'][direction]
                sent=probe.check_capture(peer.root,record['sent'],record['expected'],peer.nonce)[0]
                received=probe.check_capture(peer.root,record['received'],record['expected'],peer.nonce)[0]
                probe.require(sent==received,'duplex_bytes')
    except probe.ContinuityError as exc:
        document['flows']=original;publish(peer.root/'evidence.json',document)
        return dict(rc=1,verdict='ASSERT-FAIL',criterion=str(exc))
    result=run()
    document['flows']=original
    publish(peer.root/'evidence.json',document)
    if result['rc']!=0:
        return result
    try:
        if not hasattr(peer,'observations'):
            raise observation.CustodyError('product_seal_observer_unavailable' if any(f.crypto for f in peer.flows.values()) else 'domain_observations_unavailable')
        records=peer.observations
        observation.fetch(records,Path('/run/tether-drill-custody')/peer.nonce,'agt1')
        product_calls=records.nonce_ledger()
        all_calls=product_calls+sum((flow.crypto.calls for flow in peer.flows.values() if flow.crypto),[])
        pairs=[(row['subkey_sha256'],row['nonce']) for row in all_calls]
        observation.require(len(pairs)==len(set(pairs)),'nonce_reuse')
        for fd in peer.private_fds:
            os.close(fd)
        peer.private_fds=[]
        def private(data):
            fd=sealed(data);peer.private_fds.append(fd);return {'fd':fd}
        document['product_attempts']=private(json.dumps(all_calls).encode())
        for row in original:
            binding=observation.bind_legacy_original(records,peer,row) if request.get('profile')=='legacy-broker' else observation.bind_original(records,peer,row,request['carrier_transition'])
            row['carrier_before']=binding['before'];row['carrier_after']=binding['after']
            if 'endpoint_id' in binding:
                row['endpoint_id']=binding['endpoint_id'];row['flow_id']=binding['flow_id']
            else:
                row['legacy_stream']=binding['legacy_stream']
        for row in original:
            if row['protocol']!='ss':
                continue
            flow=peer.flows.get(row['id'])
            observation.require(flow and flow.crypto and flow.crypto.rx_key,'ss_original_owner_unavailable')
            crypto=flow.crypto
            down_calls=[e for e in records.rows('ss-seal','agt1') if e['digest']==hashlib.sha256(crypto.rx_key).digest()]
            observation.require(down_calls,'product_seal_observer_unavailable')
            actor=down_calls[0]['actor']
            observation.require(crypto.partials,'client_partial_cipher_prior_unavailable')
            partial=observation.payload_partial(records,down_calls,actor)
            key=private(crypto.master)['fd']
            row['ss']=dict(key_fd=key,directions={
                'up':dict(file=private(bytes(crypto.up)),actual_seals=private(json.dumps(crypto.calls).encode()),
                    plaintext_prefix_bytes=8+len(row['id']),partial=crypto.partials[0]),
                'down':dict(file=private(bytes(crypto.down)),actual_seals=private(json.dumps([
                    dict(subkey_sha256=e['digest'].hex(),nonce=e['nonce'].hex()) for e in down_calls]).encode()),
                    plaintext_prefix_bytes=0,partial=partial)})
        publish(peer.root/'evidence.json',document)
        # The live domain receipt was consumed at collect, before owners close.
        observation.require(getattr(peer,'observation_checkpoints',{}).get('collect'),'domain_observations_unavailable')
        return run()
    except (OSError,ValueError,KeyError) as exc:
        if isinstance(exc,observation.CustodyError) and str(exc)=='nonce_reuse':
            return dict(rc=1,verdict='ASSERT-FAIL',criterion='nonce_reuse')
        return dict(rc=2,verdict='SETUP-RED',criterion=str(exc) if isinstance(exc,observation.CustodyError) else 'private_custody_failed')


# origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.C
# An empty replica view is a pre-business readiness observation. Malformed
# material stays a hard error and never becomes a subscription retry.
def install_supply(root, name, supply, node="agt1"):
    import yaml
    target, body = supply[:6], supply[6:]
    proxies = yaml.safe_load(body).get("proxies") or []
    selected = [p for p in proxies if p.get("name") == node and p.get("type") == "ss"]
    if not selected:
        return dict(rc=2, criterion="subscription_node_not_ready")
    if len(selected) != 1 or selected[0]["cipher"] != "chacha20-ietf-poly1305":
        raise ValueError("SS supply unavailable")
    entry = selected[0]
    # EVP_BytesToKey uses the literal Clash password, not its base64
    # decoding. Secret fields never enter the RPC's JSON envelope.
    fd = supply_fd(entry, target)
    try:
        result = call(root, dict(command="key", name=name), fd)
    finally:
        os.close(fd)
    result.update(host=entry["server"], port=entry["port"])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("serve", "call", "key", "kernel", "stdio"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--nonce")
    parser.add_argument("--role", choices=("target", "client"))
    parser.add_argument("--name")
    parser.add_argument("--node", default="agt1")
    parser.add_argument("--listen-host", default="0.0.0.0")
    parser.add_argument("--listen-port", type=int, default=0)
    parser.add_argument("--unit",choices=("tether-agent","tether-broker"),default="tether-agent")
    args = parser.parse_args()
    try:
        if args.command == "serve":
            Peer(args.root, args.nonce, args.role, args.listen_host, args.listen_port).run()
            return 0
        if args.command=='stdio':
            stdio(args.root,args.nonce)
            return 0
        if args.command == "call":
            result = call(args.root, json.load(sys.stdin))
        elif args.command == "key":
            result = install_supply(args.root, args.name, sys.stdin.buffer.read(), args.node)
        else:
            result = run_supervised(["systemctl", "show", args.unit, "-p", "MainPID", "--value"],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if result.returncode != 0:
                raise ValueError("unit census failed")
            pid = int(result.stdout)
            result = kernel(pid,marker=args.unit=="tether-agent")
        print(json.dumps(result, sort_keys=True))
        return result.get("rc", 0)
    except (OSError, ValueError, KeyError, TypeError, probe.EvidenceError):
        print('{"rc":2,"criterion":"peer_observation_failed"}')
        return 2


def kernel(pid,marker=True):
    """PID 1 is an ancestor stop, never a census failure or an exit proof."""
    before = probe.identity(pid)
    root = Path("/proc") / str(pid)
    fds, connections = [], []
    for path in (root / "fd").iterdir():
        try:
            link = os.readlink(path)
            value = path.stat()
        except FileNotFoundError:
            continue  # Descriptor closed during a live census, not zero.
        fds.append(dict(fd=int(path.name), link=link, dev=value.st_dev, ino=value.st_ino))
    for name in ("tcp", "tcp6"):
        for line in (root / "net" / name).read_text().splitlines()[1:]:
            fields = line.split()
            if any(fd["ino"] == int(fields[9]) and fd["link"].startswith("socket:") for fd in fds):
                connections.append(dict(family=name, state=fields[3], ino=int(fields[9])))
    if before != probe.identity(pid):
        raise ValueError("census PID reuse")
    marker_path = Path("/home/sim/.local/bin/.tether-upgrade.json")
    marker = json.loads(marker_path.read_text()) if marker and marker_path.exists() else None
    return dict(identity=before, running_sha=hashlib.sha256((root / "exe").read_bytes()).hexdigest(),
                uid=int((root / "status").read_text().split("Uid:")[1].split()[0]),
                marker=marker, fds=fds, connections=connections, at=time.monotonic())


def stdio(root,nonce):
    class Pipes:
        def recv(self,length): return os.read(0,length)
        def sendall(self,data):
            cursor=0
            while cursor<len(data): cursor+=os.write(1,data[cursor:])
    owner=SimpleNamespace(root=root,nonce=nonce,role='target',stop=threading.Event(),
                          finish=threading.Event(),deadline=time.monotonic()+660)
    spec=probe.read_json(root/'ssh-spec.json')
    if "supply" in spec:
        module("fullstack_flow").validate_source(spec, nonce)
        owner.deadline = time.monotonic() + spec["supply"]["ceiling_seconds"] + 45
    flow=Flow(owner,'ssh',Pipes(),spec,identity_reader=lambda: probe.identity(os.getpid()))
    flow.run()
    try:
        while not (root/'ssh-cleanup').exists():
            if (root/'ssh-finish').exists(): owner.finish.set()
            if (root/'ssh-mark').exists() and flow.fault is None: flow.fault=time.monotonic()
            if owner.finish.is_set() and flow.done.is_set() and flow.tx_done.is_set() and not hasattr(owner,'observation_end'):
                owner.observation_end=time.monotonic()
            flow.snapshot()
            time.sleep(.02)
    finally:
        owner.stop.set()


if __name__ == "__main__":
    sys.exit(main())
