#!/usr/bin/env python3
"""Hold original target SYNs in the isolated agent netns until its native cut.

DROP introduces a retransmit interval longer than the original connect budget.
NFQUEUE retains the actual packet and FD; accepting that packet at the cut does
not redial, extend a deadline, or alter any business traffic.
origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
"""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import select
import socket
import struct
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
import flow_observation as observation


def attribute(kind, data):
    length = len(data)+4
    return struct.pack('=HH', length, kind)+data+bytes((-length) % 4)


class Queue:
    def __init__(self, number):
        self.number, self.sequence = number, 0
        self.socket = socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, 12)
        self.socket.bind((0, 0))
        self.socket.connect((0, 0))
        self.socket.settimeout(2)
        # Linux UAPI: NFNL_SUBSYS_QUEUE / NFQNL_MSG_CONFIG; queue BIND.
        self.send(2, attribute(1, struct.pack('!BBH', 1, 0, 0)), ack=True)
        self.send(2, attribute(2, struct.pack('!IB', 0, 1)), ack=True)

    def send(self, kind, data, ack=False):
        self.sequence += 1
        body=struct.pack('!BBH', 0, 0, self.number)+data
        header=struct.pack('=IHHII', len(body)+16, 0x300+kind, 5 if ack else 1,
                           self.sequence, self.socket.getsockname()[0])
        self.socket.send(header+body)
        if ack:
            reply=self.socket.recv(65536)
            length, kind, _, sequence, _=struct.unpack_from('=IHHII', reply)
            if length < 20 or kind != 2 or sequence != self.sequence:
                raise ValueError('queue_configuration_receipt')
            status,=struct.unpack_from('=i',reply,16)
            if status:
                raise OSError(-status, 'queue_configuration_failed')

    def receive(self):
        raw=self.socket.recv(65536)
        ids=[]
        offset=0
        while offset+16 <= len(raw):
            length, kind, _, _, _=struct.unpack_from('=IHHII',raw,offset)
            if length < 20 or offset+length > len(raw):
                raise ValueError('queue_packet_truncated')
            if kind == 2:
                status,=struct.unpack_from('=i',raw,offset+16)
                if status:
                    raise OSError(-status,'queue_verdict_failed')
            elif kind == 0x300:
                cursor=offset+20
                while cursor+4 <= offset+length:
                    size, field=struct.unpack_from('=HH',raw,cursor)
                    if size < 4 or cursor+size > offset+length:
                        raise ValueError('queue_attribute_truncated')
                    if field & 0x3fff == 1:
                        if size != 11:
                            raise ValueError('queue_packet_header')
                        ids.append(struct.unpack_from('!I',raw,cursor+4)[0])
                    cursor += (size+3) & ~3
            offset += (length+3) & ~3
        return ids

    def accept(self, ids):
        for identifier in ids:
            self.send(1, attribute(2,struct.pack('!II',1,identifier)))

    def close(self):
        self.socket.close()


def publish(root, name, value):
    path=root/name
    temporary=path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value)+'\n')
    temporary.replace(path)


def capture_open_prior(records, root, peer_root):
    # All original state is read in the target netns, before releasing the
    # queued SYN. The native extracted artifact has already started validation;
    # no download time spends the original connector budget. Neither socket nor deadline is substituted.
    # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
    import flow_fixture_peer as peer
    pid = int(subprocess.check_output(['systemctl','show','tether-agent','-p','MainPID','--value']))
    kernel = peer.kernel(pid)
    target = peer.call(peer_root, dict(command='snapshot', accepts_only=True))
    pending = [event for event in records.live_flows().values()
               if event['values'][3:5] == [2,1] and event['values'][14] == 1]
    syn = {row['ino'] for row in kernel['connections'] if row['state'] == '02'}
    pending = [event for event in pending if event['values'][6] in syn]
    if len(pending) != 1 or target['flows'].get('opening'):
        return False
    _, partial = records.partial()
    event = pending[0]
    publish(root, 'open-prior.json', dict(at=event['at'], endpoint=event['endpoint'].hex(),
            flow=event['flow'].hex(), values=event['values'], kernel=kernel, target=target, ss=partial))
    return True


# Start the original OPEN after real extraction, while native hash/smoke/reader
# checks still precede PREPARE. Download/extraction time is not a connect fault;
# including it consumed the unchanged two-second connect deadline before exec.
# This only observes the product's executable staging file; it never delays it.
# origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.A
def wait_staged(gate_url, binary_dir=Path('/home/sim/.local/bin')):
    before = {path.stat().st_ino for path in binary_dir.glob('.tether-upgrade-*/tether')}
    request = urllib.request.Request(gate_url+'/release', method='POST')
    with urllib.request.urlopen(request, timeout=1) as response:
        if response.status != 204:
            raise ValueError('artifact_release_refused')
    deadline = time.monotonic()+5
    while time.monotonic() < deadline:
        for path in binary_dir.glob('.tether-upgrade-*/tether'):
            try:
                info = path.stat()
            except FileNotFoundError:
                continue
            if info.st_ino not in before and info.st_mode & 0o111 and info.st_size > 0:
                return dict(dev=info.st_dev, ino=info.st_ino, size=info.st_size, observed_at=time.monotonic())
        time.sleep(.001)
    raise TimeoutError('candidate_extraction_not_observed')


def watch(root, number, gate_url=None, peer_root=None):
    queue=Queue(number)
    records=observation.Records()
    pending=[]
    try:
        observation.fetch(records, root, 'agt1')
        baseline=len(records.events)
        publish(root,'open-fault-ready.json',dict(pid=os.getpid(),queue=number))
        deadline=time.monotonic()+15
        while time.monotonic() < deadline:
            while select.select([queue.socket],[],[],0)[0]:
                pending.extend(queue.receive())
            observation.fetch(records,root,'agt1')
            if pending and peer_root and not (root/'open-prior.json').exists():
                capture_open_prior(records, root, peer_root)
            cuts=[event for event in records.events[baseline:] if event['kind']=='final-cut']
            if cuts:
                if not pending:
                    raise ValueError('cut_without_original_syn')
                # The native sealed cut is the only release trigger. No host
                # log polling, fixed sleep, or successful upgrade can release it.
                queue.accept(pending)
                subprocess.run(['iptables','-F','FLOWOPEN'],check=True)
                publish(root,'open-fault-released.json',dict(original_packets=len(pending),
                        cut_boot_ns=cuts[0]['at'],released_at=time.monotonic()))
                return
        raise TimeoutError('original_cut_not_observed')
    finally:
        records.close()
        queue.close()


def fail_first_commit(root, queue, ids, started):
    """Bound only the actual first NATS SYN, after durable candidate boot."""
    marker = json.loads(Path('/home/sim/.local/bin/.tether-upgrade.json').read_text())
    if marker.get('state') != 'pending' or marker.get('boot_count', 0) < 1:
        raise ValueError('candidate_boot_not_persisted')
    pid = int(subprocess.check_output(['systemctl', 'show', 'tether-agent', '-p', 'MainPID', '--value']))
    process = Path('/proc') / str(pid)
    uid = int((process/'status').read_text().split('Uid:')[1].split()[0])
    binary = Path('/home/sim/.local/bin')
    if uid == 0 or binary.stat().st_uid != uid:
        raise ValueError('candidate_directory_owner')
    with (process/'exe').open('rb') as image:
        if hashlib.file_digest(image, 'sha256').hexdigest() != marker['new_sha']:
            raise ValueError('candidate_image_not_running')
    mode = binary.stat().st_mode & 0o777
    # Record restoration responsibility before changing the original directory.
    publish(root, 'watchdog-mode.json', dict(mode=oct(mode)[2:]))
    binary.chmod(0o555)
    if subprocess.call(['runuser', '-u', 'sim', '--', 'test', '-w', str(binary)]) != 1:
        raise ValueError('candidate_directory_still_writable')
    if time.monotonic() - started >= 2:
        raise TimeoutError('nats_fault_exceeded_two_seconds')
    queue.accept(ids)
    subprocess.run(['iptables', '-F', 'FLOWNATS'], check=True)
    elapsed = (time.monotonic() - started) * 1000
    if elapsed > 2000:
        raise TimeoutError('nats_fault_exceeded_two_seconds')
    publish(root, 'watchdog-released.json', dict(nats_block_ms=elapsed, mode=oct(mode)[2:],
            candidate_pid=pid, candidate_uid=uid, boot_count=marker['boot_count']))


def watch_first_commit(root, number):
    # origin: hot-update S5 plan §9.2 / T48 / DRILL-RUN. Artifact staging is
    # not a blocked NATS connection. Start the unchanged two-second fault
    # budget when its actual original SYN enters the queue, then change mode
    # locally before releasing that packet to the first real register.
    queue = Queue(number)
    try:
        publish(root, 'watchdog-ready.json', dict(pid=os.getpid(), queue=number))
        if not select.select([queue.socket], [], [], 15)[0]:
            raise TimeoutError('candidate_nats_syn_not_observed')
        started = time.monotonic()
        ids = queue.receive()
        if not ids:
            raise ValueError('candidate_nats_syn_missing')
        fail_first_commit(root, queue, ids, started)
    finally:
        subprocess.run(['iptables', '-F', 'FLOWNATS'], check=True)
        queue.close()


def selftest():
    # Real isolated-netns control: the queued original SYN must stay pending
    # until accepted, then connect on that same socket before its two seconds.
    queue=Queue(75)
    listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen()
    client=socket.socket();client.setblocking(False)
    identity=os.fstat(client.fileno()).st_ino
    port=listener.getsockname()[1]
    rule=['iptables','-A','OUTPUT','-p','tcp','-d','127.0.0.1','--dport',str(port),
          '--syn','-j','NFQUEUE','--queue-num','75']
    subprocess.run(rule,check=True)
    try:
        begin=time.monotonic()
        assert client.connect_ex(('127.0.0.1',port))==errno.EINPROGRESS
        ids=queue.receive();assert ids
        readable,writable,_=select.select([listener],[client],[],.05)
        assert not readable and not writable
        queue.accept(ids)
        listener.settimeout(2)
        target,_=listener.accept()
        try:
            assert time.monotonic()-begin < 2
            assert os.fstat(client.fileno()).st_ino==identity
            assert client.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)==0
            client.sendall(b'original-syn')
            assert target.recv(12)==b'original-syn'
        finally:
            target.close()
    finally:
        remove=rule.copy();remove[1]='-D'
        subprocess.run(remove,check=True)
        client.close();listener.close();queue.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=('arm','watch','verify','selftest', 'watchdog-arm', 'watchdog-watch', 'watchdog-verify', 'wait-staged'))
    parser.add_argument('--root',type=Path)
    parser.add_argument('--queue',type=int,default=75)
    parser.add_argument('--gate-url')
    parser.add_argument('--peer-root',type=Path)
    args=parser.parse_args()
    if args.command=='wait-staged':
        print(json.dumps(wait_staged(args.gate_url)));return
    if args.command=='selftest':
        selftest();return
    if args.command=='watch':
        watch(args.root,args.queue,args.gate_url,args.peer_root);return
    if args.command=='watchdog-watch':
        watch_first_commit(args.root,args.queue);return
    if args.command=='watchdog-verify':
        deadline = time.monotonic()+2
        path = args.root/'watchdog-released.json'
        while not path.exists():
            if time.monotonic() >= deadline:
                raise TimeoutError('first_commit_fault_not_observed')
            time.sleep(.01)
        result=json.loads(path.read_text())
        assert 0 <= result['nats_block_ms'] <= 2000 and result['candidate_uid'] > 0
        print(json.dumps(result,sort_keys=True))
        return
    if args.command=='verify':
        result=json.loads((args.root/'open-fault-released.json').read_text())
        assert result['original_packets'] > 0 and result['cut_boot_ns'] > 0
        print(json.dumps(result,sort_keys=True))
        return
    watchdog = args.command=='watchdog-arm'
    names = ('watchdog-ready.json', 'watchdog-released.json', 'watchdog-mode.json') if watchdog else ('open-fault-ready.json','open-fault-released.json')
    for name in names:
        (args.root/name).unlink(missing_ok=True)
    child=subprocess.Popen([sys.executable,'-I','-B',__file__,'watchdog-watch' if watchdog else 'watch','--root',str(args.root),
                            '--queue',str(args.queue)]+(['--peer-root',str(args.peer_root)] if args.peer_root else []),start_new_session=True,
                           stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    deadline=time.monotonic()+5
    while not (args.root/names[0]).exists():
        if child.poll() is not None or time.monotonic() >= deadline:
            child.terminate();child.wait(timeout=2)
            raise ValueError('syn_fault_not_ready')
        time.sleep(.01)


if __name__=='__main__':
    main()
