#!/usr/bin/env python3
"""Private product custody; only bounded sealed memfds cross keeper boundaries.

The decoder rejects gaps, truncation and poisoned batches before constructing
any public receipt. Keys, nonce bytes, TLS identities and cipher suffixes stay
in this process or inherited sealed descriptors, never JSON/ordinary files.
"""
import argparse
import array
import fcntl
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
import threading
sys.path.insert(0, str(Path(__file__).resolve().parent))
import infra_native_custody as infrastructure

SEALS = fcntl.F_SEAL_SEAL | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_GROW | fcntl.F_SEAL_WRITE
MAX_BATCH = 1 << 20
MAX_CUSTODY = 256 << 20
# Fit below Linux's SCM_RIGHTS limit and the unchanged custody FD budget.
# Each page still obtains a fresh poison-checked product barrier. Small pages
# repeatedly paid the producer's poll interval during a pending OPEN deadline.
# origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
TRANSFER_FDS = 128
KINDS = {'ss-seal','ss-write','ss-state','ss-binding','ss-revoke-mark','ss-closed','ss-grant',
         'flow-state','flow-retirement','carrier-negotiated','carrier-prefix','resume','prepare',
         'final-cut','domain-cut','domain-file','reaper','register-applied','commit-write-failed',
         'mark-ready','watchdog-current','domain-live','flow-socket','legacy-binding','flow-capacity',
         'runtime-sample','transfer-v1','store-before-replay','infra-attachment','infra-prefix'}

# Fixed public diagnostics keep the poison oracle unchanged. No arbitrary
# exception text or private batch field crosses the keeper boundary.
# origin: hot-update S5 plan §6.6 / §9.4 / DRILL-RUN
CUSTODY_FAILURES={'custody_io_failure','custody_broken','custody_sequence_gap','custody_capacity',
    'custody_descriptor_capacity','custody_archive_truncated','custody_header','custody_identity_changed',
    'custody_replayed_incarnation','custody_kind','custody_trailing','custody_event_size','custody_unsealed',
    'custody_size','custody_truncated','custody_producer_poison','custody_barrier_timeout','custody_barrier_transport',
    'custody_reply_invalid'}
PRODUCER_FAILURES={2:'custody_queue_overflow',3:'custody_send_failure',4:'custody_event_invalid',
    5:'custody_clock_failure',6:'custody_memfd_failure',7:'custody_write_failure',8:'custody_seal_failure',
    9:'custody_close_failure',10:'custody_flush_overflow',11:'custody_flush_timeout',
    12:'custody_control_failure',13:'custody_transfer_poison',14:'custody_ofd_failure',
    16:'custody_runtime_poison',17:'custody_sample_request_failure',18:'custody_sample_reply_failure'}
CUSTODY_FAILURES.update(PRODUCER_FAILURES.values())
CUSTODY_FAILURES.update({'custody_runtime_schema','custody_runtime_identity','custody_runtime_unknown',
    'custody_runtime_payload','custody_runtime_nonce','custody_runtime_ack','custody_runtime_stale',
    'custody_runtime_sequence_gap','custody_runtime_identity_changed'})

RUNTIME_UNKNOWN_COUNTERS = 0x7fc0

class CustodyError(ValueError):
    pass

def require(value, criterion):
    if not value:
        raise CustodyError(criterion)

# Both retaining judges and archive-only keepers validate this schema. Unknown
# business counts are explicit, while process identity, G/FD and time are known.
# origin: hot-update S10 plan §5.3 / §10.2 P06
def decode_runtime_sample(event):
    require(event['kind'] == 'runtime-sample' and event['domain'] == 'agent', 'custody_runtime_schema')
    values, payload = event['values'], event['data']
    require(len(values) == 16 and values[0] == 1 and 0 < event['at'] < 1 << 63 and
            all(type(value) is int and 0 <= value < 1 << 64 for value in values) and
            all(value < 1 << 63 for value in values[6:15]), 'custody_runtime_schema')
    require(0 < values[1] < 1 << 31 and all(values[i] > 0 for i in (2,4,5,15)) and values[4] < 1 << 63 and
            values[5] < 1 << 31 and event['digest'] != bytes(32) and
            all(event[key] == bytes(16) for key in ('endpoint','flow','actor')), 'custody_runtime_identity')
    require(event['nonce'] != bytes(12), 'custody_runtime_nonce')
    require(2 <= len(payload) <= 32768, 'custody_runtime_payload')
    unknown, = struct.unpack_from('<H', payload)
    require(unknown & ~RUNTIME_UNKNOWN_COUNTERS == 0 and
            all(not (unknown & 1 << i) or value == 0 for i,value in enumerate(values)), 'custody_runtime_unknown')
    offset, text = 2, []
    for _ in range(5):
        require(offset + 2 <= len(payload), 'custody_runtime_payload')
        length, = struct.unpack_from('<H', payload, offset)
        offset += 2
        require(length <= 256 and offset + length <= len(payload), 'custody_runtime_payload')
        try:
            text.append(payload[offset:offset + length].decode('utf-8'))
        except UnicodeError:
            raise CustodyError('custody_runtime_payload') from None
        offset += length
    require(offset == len(payload), 'custody_runtime_payload')
    alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:[]'
    require(all(value and all(char in alphabet for char in value) for value in text[:3]) and
            all(char in alphabet for char in text[3]), 'custody_runtime_identity')
    require(len(text[4]) == 64 and text[4] != '0' * 64 and
            all(char in '0123456789abcdef' for char in text[4]), 'custody_runtime_identity')
    return dict(values=values, unknown=unknown, boot_id=text[0], instance_id=text[1], pidns=text[2],
                cut_id=text[3], scope_digest=text[4], nonce=event['nonce'], exe_sha=event['digest'], at=event['at'])

def runtime_sample_request(nonce, scope):
    require(type(nonce) is bytes and len(nonce) == 12 and nonce != bytes(12) and
            type(scope) is bytes and len(scope) == 32 and scope != bytes(32), 'custody_runtime_nonce')
    return b'TDS1' + nonce + scope

def validate_runtime_sample_ack(packet, event, sealed_sequence, requested_at, after_sequence=0):
    sample = decode_runtime_sample(event)
    require(len(packet) == 72 and packet[:4] == b'TDSA', 'custody_runtime_ack')
    ordinal, watermark, poison = struct.unpack_from('<QQQ', packet, 48)
    require(packet[4:16] == sample['nonce'] and packet[16:48].hex() == sample['scope_digest'] and
            ordinal == sample['values'][15] and 0 < watermark == sealed_sequence and poison == 0,
            'custody_runtime_ack')
    require(sample['at'] > requested_at and ordinal > after_sequence, 'custody_runtime_stale')
    return sample

def sealed(data):
    fd = os.memfd_create('s5-observation', os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    try:
        require(os.write(fd, data) == len(data), 'custody_short_write')
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS, SEALS)
        return fd
    except BaseException:
        os.close(fd)
        raise

def read_sealed(fd):
    require(fcntl.fcntl(fd, fcntl.F_GET_SEALS) & SEALS == SEALS, 'custody_unsealed')
    length = os.fstat(fd).st_size
    require(0 < length <= MAX_BATCH, 'custody_size')
    data = os.pread(fd, length, 0)
    require(len(data) == length, 'custody_truncated')
    return data

def receive(connection):
    raw, ancillary, flags, _ = connection.recvmsg(MAX_BATCH, socket.CMSG_SPACE(TRANSFER_FDS * 4))
    fds = []
    for level, kind, content in ancillary:
        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
            values = array.array('i')
            values.frombytes(content[:len(content)-len(content)%values.itemsize])
            fds.extend(values)
    if flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC):
        for fd in fds:
            os.close(fd)
        raise CustodyError('custody_message_truncated')
    for fd in fds:
        os.set_inheritable(fd, False)
    return raw, fds

def decode(fd, retain_events=True, runtime_samples=None):
    data = read_sealed(fd)
    require(len(data) >= 76 and data[:8] == b'TDRILL1\0', 'custody_header')
    identity, incarnation = data[8:40], data[40:56]
    sequence, broken, count = struct.unpack_from('<QQI', data, 56)
    require(broken == 0, PRODUCER_FAILURES.get(broken,'custody_broken'))
    require(sequence > 0 and count <= 16, 'custody_broken')
    offset, events = 76, []
    def take(n):
        nonlocal offset
        require(offset+n <= len(data), 'custody_truncated')
        result, offset = data[offset:offset+n], offset+n
        return result
    for _ in range(count):
        try:
            kind = take(take(1)[0]).decode('ascii')
            domain = take(take(1)[0]).decode('ascii')
        except UnicodeError:
            raise CustodyError('custody_kind') from None
        domains = {'ss','flow','tunnel','agent','exec','pty','proxy'}
        valid = ((kind == 'runtime-sample' and domain == 'agent') or
                 (kind == 'transfer-v1' and domain == 'transfer') or
                 (kind == 'store-before-replay' and domain == 'store') or
                 (kind in infrastructure.DOMAINS and domain == infrastructure.DOMAINS[kind]) or
                 (kind in KINDS - {'runtime-sample','transfer-v1','store-before-replay','infra-attachment','infra-prefix'} and domain in domains))
        require(valid, 'custody_kind')
        detailed = retain_events or kind in ('runtime-sample','transfer-v1',*infrastructure.DOMAINS)
        if detailed:
            endpoint, flow, actor, digest, nonce = take(16), take(16), take(16), take(32), take(12)
            at, *values = struct.unpack('<17Q', take(136))
        else:
            # Keep S5's archive-only path cheap for cipher/flow traffic. Its
            # discarded objects cannot hide a runtime/transfer schema failure.
            take(92)
            at, = struct.unpack_from('<Q', take(136))
        length, = struct.unpack('<I', take(4))
        require(length <= 32768 and at > 0, 'custody_event_size')
        payload = take(length)
        if detailed:
            event = dict(kind=kind, domain=domain, endpoint=endpoint, flow=flow, actor=actor,
                         digest=digest, nonce=nonce, at=at, values=values, data=payload)
        if kind == 'runtime-sample':
            sample = decode_runtime_sample(event)
            if runtime_samples is not None:
                runtime_samples.append(sample)
        elif kind == 'transfer-v1':
            # Preserve the upstream real schema on the shared channel. Its
            # reader remains the authority; malformed mixed frames poison here.
            import transfer_observation
            try:
                require(hashlib.sha256(payload).digest() == digest, 'custody_runtime_payload')
                row = json.loads(payload, object_pairs_hook=transfer_observation.unique_object)
                transfer_observation.validate_event(row)
                require(at >= row['boottime_ns'], 'custody_event_size')
            except (ValueError, UnicodeError):
                raise CustodyError('custody_event_size') from None
        if kind in infrastructure.DOMAINS:
            infrastructure.fragment(event)
        if retain_events or kind in infrastructure.DOMAINS:
            events.append(event)
    require(offset == len(data), 'custody_trailing')
    return identity, incarnation, sequence, events

class Records:
    def __init__(self):
        self.events, self.sequences, self.identities, self.fds = [], {}, {}, []
        self.frames, self.pending, self.archive_fds = [], [], []
        self.pending_bytes=0
        self.bytes, self.broken = 0, False
        self.failure = None
        self.current_incarnations = {}
        self.runtime_sequences = {}

    def add(self, fd, source, retain_events=True):
        try:
            runtime_samples = []
            identity, incarnation, seq, rows = decode(fd, retain_events, runtime_samples)
            require(self.identities.get(source, identity) == identity, 'custody_identity_changed')
            key = (source, incarnation)
            require(self.current_incarnations.get(source,incarnation)==incarnation or key not in self.sequences, 'custody_replayed_incarnation')
            self.current_incarnations[source]=incarnation
            require(seq == self.sequences.get(key, 0)+1, 'custody_sequence_gap')
            # A missing runtime event can leave batch ordinals intact. Keep
            # only its tiny cursor/identity in the archive-only keeper, rather
            # than rebuilding S5's discarded semantic event ledger.
            # origin: hot-update S10 plan §5.3 / §10.2 P06
            previous, binding = self.runtime_sequences.get(key, (0, None))
            for sample in runtime_samples:
                current = (tuple(sample['values'][1:4]), sample['boot_id'], sample['instance_id'],
                           sample['pidns'], sample['exe_sha'])
                require(sample['values'][15] == previous + 1, 'custody_runtime_sequence_gap')
                require(binding is None or binding == current, 'custody_runtime_identity_changed')
                previous, binding = sample['values'][15], current
            require(self.bytes + os.fstat(fd).st_size <= MAX_CUSTODY, 'custody_capacity')
            self.identities[source], self.sequences[key] = identity, seq
            self.bytes += os.fstat(fd).st_size
            if runtime_samples:
                self.runtime_sequences[key] = previous, binding
            for row in rows:
                row['source'] = source
                if row['kind'] in infrastructure.DOMAINS:
                    row['sequence'], row['incarnation'] = seq, incarnation
            self.events.extend(rows)
            if self.pending and self.pending_bytes+os.fstat(fd).st_size>MAX_BATCH:
                self.compact()
            self.frames.append((fd,0,os.fstat(fd).st_size))
            self.pending.append(len(self.frames)-1);self.pending_bytes+=os.fstat(fd).st_size
            self.fds.append(fd)
            if len(self.pending)>=128:
                self.compact()
        except BaseException as error:
            self.broken = True
            reason = str(error) if isinstance(error, CustodyError) else 'custody_io_failure'
            self.failure = dict(source=source, criterion=reason, values=[self.bytes,len(self.frames),len(self.fds)])
            if fd in self.fds:
                self.fds.remove(fd)
            os.close(fd)
            raise

    def compact(self):
        # Preserve exact product batch bytes in an immutable sealed archive.
        # Absolute frame cursors survive compaction; fd count stays bounded.
        require(len(self.archive_fds)<768,'custody_descriptor_capacity')
        data=b''.join(os.pread(self.frames[i][0],self.frames[i][2],self.frames[i][1]) for i in self.pending)
        require(len(data)==self.pending_bytes,'custody_archive_truncated')
        archived=sealed(data);self.archive_fds.append(archived);self.fds.append(archived)
        offset=0
        for index in self.pending:
            fd,_,length=self.frames[index];self.frames[index]=(archived,offset,length);offset+=length
            self.fds.remove(fd);os.close(fd)
        self.pending=[];self.pending_bytes=0

    def close(self):
        for fd in self.fds:
            os.close(fd)
        self.fds.clear()

    def rows(self, kind, source=None):
        require(not self.broken, 'custody_broken')
        return [e for e in self.events if e['kind'] == kind and (source is None or e['source'] == source)]

    def ready(self, source='agt1'):
        require(not self.broken and self.identities.get(source) and self.events, 'custody_unavailable')

    def nonce_ledger(self):
        calls = self.rows('ss-seal', 'agt1')
        require(calls, 'product_seal_observer_unavailable')
        pairs = [(e['digest'], e['nonce']) for e in calls]
        require(len(set(pairs)) == len(pairs), 'nonce_reuse')
        return [dict(subkey_sha256=e['digest'].hex(), nonce=e['nonce'].hex()) for e in calls]

    def live_flows(self):
        latest = {}
        for e in self.rows('flow-state','agt1'):
            latest[e['flow']] = e
        return latest

    def domains(self, kernel):
        self.ready()
        cuts = {e['domain']: e for e in self.rows('domain-cut','agt1')}
        live = {e['domain']: e for e in self.rows('domain-live','agt1')}
        require(set(live) == {'exec','pty'}, 'live_domain_observations_unavailable')
        require(not cuts or set(cuts)=={'exec','pty','flow','ss'} and len({e['values'][0] for e in cuts.values()})==1, 'domain_cut_mixed')
        result = {}
        for domain in ('exec','pty'):
            require(all(e['values'][1]>0 for e in self.rows('domain-file','agt1') if e['domain']==domain),'domain_file_identity_unavailable')
            files = {(e['values'][0],e['values'][1]) for e in self.rows('domain-file','agt1') if e['domain']==domain}
            owned = [f for f in kernel['fds'] if (f['dev'],f['ino']) in files]
            unique = {(f['dev'],f['ino']) for f in owned}
            result[domain] = dict(fds=len(unique), dups=len(owned)-len(unique),
                                  reservation=live[domain]['values'][2], replay=live[domain]['values'][3])
        flows = list(self.live_flows().values())
        active = [e for e in flows if e['values'][14]]
        require(all(e['values'][6]>0 and len(e['data'])==32 and struct.unpack_from('<QQ',e['data'],16)[1]>0 for e in active),'flow_replay_identity_unavailable')
        originals = {(e['values'][5], e['values'][6]) for e in self.rows('flow-state','agt1') if e['values'][6]}
        originals.update((e['values'][3],e['values'][4]) for e in self.rows('legacy-binding','agt1'))
        for e in self.rows('flow-state','agt1'):
            if len(e['data'])==32:
                dev,ino=struct.unpack_from('<QQ',e['data'],16)
                if ino:
                    originals.add((dev,ino))
        actual = [f for f in kernel['fds'] if (f['dev'],f['ino']) in originals]
        result['flow'] = dict(fds=len({(f['dev'],f['ino']) for f in actual}),
            dups=len(actual)-len({(f['dev'],f['ino']) for f in actual}), reservation=sum(e['values'][12] for e in active),
            replay=sum(e['values'][13] for e in active))
        # Closed actors can leave Prepared or faulty cleanup dup FDs. Preserve
        # historical original identities and let the live kernel census count
        # them; a terminal record is never proof that its OFD vanished.
        sockets=set()
        for e in self.events:
            if e['source']=='agt1' and e['kind'] in ('ss-state','ss-binding','ss-closed') and len(e['data'])>=56:
                values=struct.unpack_from('<4Q',e['data'],24)
                sockets.update((values[i],values[i+1]) for i in (0,2) if values[i+1])
        actual=[f for f in kernel['fds'] if (f['dev'],f['ino']) in sockets]
        result['ss']=dict(fds=len(set((f['dev'],f['ino']) for f in actual)),
            dups=len(actual)-len({(f['dev'],f['ino']) for f in actual}),reservation=0,replay=0)
        require(sum(r['reservation'] for r in result.values()) <= 256<<20, 'domain_budget_exceeded')
        return result

    def partial(self):
        self.nonce_ledger()
        cuts = self.rows('final-cut','agt1')
        prior = cuts[0]['at'] if cuts else (1<<64)-1
        matches = [e for e in self.rows('ss-write','agt1') if e['values'][4]==1 and
                   0 < e['values'][1] < e['values'][0] and e['at'] < prior and
                   e['values'][2]==len(e['data'])==e['values'][0]-e['values'][1]]
        require(matches, 'product_partial_cipher_observer_unavailable')
        e=matches[0]
        seals=[c for c in self.rows('ss-seal','agt1') if c['actor']==e['actor'] and c['at']<=e['at']]
        require(seals and e['actor']!=bytes(16), 'partial_without_seal')
        return e, dict(attempt_len=e['values'][0], n=e['values'][1], cipher_suffix_bytes=len(e['data']),
                       seal_calls=len(seals),application_redials=0)

    def fresh_grant(self):
        rows=self.rows('ss-grant','agt1')
        require(rows and rows[-1]['values'][3]==1 and rows[-1]['values'][4]==2, 'fresh_ss_grant_unavailable')
        e=rows[-1]
        cuts=self.rows('final-cut','agt1')
        require(not cuts or e['at']>cuts[-1]['at'], 'stale_ss_grant')
        return dict(generation=e['values'][0],epoch=e['values'][1],applied_revision=e['values'][2])

    def reaper(self, sources=('brk1',)):
        rows=[row for source in sources for row in self.rows('reaper',source)]
        require(rows and all(e['values'][2]==3 and e['values'][3:5]==[1,1] for e in rows), 'live_reaper_receipt_unavailable')
        return dict(running=True,ticks=len(rows),default_dwell=3)

    def legacy(self):
        rows=self.rows('carrier-negotiated','agt1')
        require(rows and all(e['data']==b'' for e in rows), 'legacy_alpn_not_empty')
        prefixes=self.rows('carrier-prefix','agt1')
        require(prefixes and all(e['data'] and e['digest'] in {r['digest'] for r in rows} for e in prefixes), 'legacy_prefix_unavailable')
        require(all(len(e['data'])>=12 and e['data'][0]==0 and e['data'][1] in (0,1,2,3) and not e['data'].startswith(b'tether-flow/1') for e in prefixes), 'legacy_flow_header')
        return dict(negotiated_alpn='',raw_prefix_observed=True,flow_header=False,sessions=len(rows))

def broker_flow_domain(records,kernel,source='brk1'):
    records.ready(source)
    rows=records.rows('flow-state',source)
    require(rows,'broker_flow_observation_unavailable')
    latest={e['flow']:e for e in rows};active=[e for e in latest.values() if e['values'][14]]
    require(all(e['values'][6]>0 and len(e['data'])==32 and struct.unpack_from('<QQ',e['data'],16)[1]>0 for e in active),'broker_flow_identity_unavailable')
    identities={(e['values'][5],e['values'][6]) for e in rows if e['values'][6]}
    for e in rows:
        if len(e['data'])==32:
            dev,ino=struct.unpack_from('<QQ',e['data'],16)
            if ino:identities.add((dev,ino))
    actual=[f for f in kernel['fds'] if (f['dev'],f['ino']) in identities]
    unique={(f['dev'],f['ino']) for f in actual}
    result=dict(fds=len(unique),dups=len(actual)-len(unique),reservation=sum(e['values'][12] for e in active),replay=sum(e['values'][13] for e in active))
    require(result['reservation']<=256<<20,'broker_flow_budget_exceeded')
    return {'flow':result}

def fetch(records, root, source):
    # Each source has an independent cursor; resending a batch is a sequence gap.
    if not hasattr(records,'cursors'):
        records.cursors={}
    cursor=records.cursors.get(source,0)
    while True:
        with socket.socket(socket.AF_UNIX,socket.SOCK_SEQPACKET) as conn:
            conn.settimeout(3);conn.connect(str(root/(source+'.sock')))
            conn.send(json.dumps(dict(cursor=cursor)).encode())
            raw,fds=receive(conn)
        reply=json.loads(raw)
        if reply.get('broken') is not False:
            for fd in fds:
                os.close(fd)
            records.broken=True
            # origin: hot-update S5 plan §9.4 / DRILL-RUN 75.C. Preserve
            # only the keeper's fixed guard and public counts, never event data.
            failure=reply.get('failure')
            if isinstance(failure,dict) and failure.get('source')==source and failure.get('criterion') in CUSTODY_FAILURES and isinstance(failure.get('values'),list) and len(failure['values'])==3 and all(type(value) is int and value>=0 for value in failure['values']):
                records.failure=failure
            raise CustodyError('custody_broken')
        try:
            require(reply.get('cursor')==cursor+len(fds),'custody_cursor')
            while fds:
                records.add(fds.pop(0),source)
        finally:
            for fd in fds:
                os.close(fd)
        cursor=reply['cursor'];records.cursors[source]=cursor
        if reply['complete']:
            break
    return records

def keeper(root, node, inherited):
    path=root/(node+'.sock')
    records=Records()
    # Descriptor intake must continue while a private page copy is blocked.
    # Only intake mutates archives; a one-frame sealed borrow keeps compaction
    # from retiring its backing FD while the API copies outside this lock.
    # origin: hot-update S5 plan §6.6 / §9.1 / T48 / DRILL-RUN 75.C
    condition=threading.Condition()
    stop=threading.Event()
    barrier=dict(revision=0,waiting=False)
    infra_barrier=infrastructure.Barrier(records,node)
    with socket.socket(socket.AF_UNIX,socket.SOCK_SEQPACKET) as server, socket.socket(fileno=inherited) as product:
        server.bind(str(path));os.chmod(path,0o660);server.listen(8)
        product.setblocking(False)
        def fail(reason):
            if records.failure is None:
                records.failure=dict(source=node,criterion=reason,values=[records.bytes,len(records.frames),len(records.fds)])
            records.broken=True
            condition.notify_all()
        def ingest():
            while not stop.is_set():
                try:
                    if not select.select([product],[],[],.02)[0]:
                        continue
                    raw,fds=receive(product)
                except (OSError,ValueError):
                    with condition:
                        if not stop.is_set():fail('custody_io_failure')
                    return
                with condition:
                    if raw==b'TDC1' and len(fds)==1:
                        if records.broken:
                            os.close(fds[0])
                        else:
                            # The keeper owns exact sealed bytes, not a second semantic
                            # ledger. Retaining every event here creates GC pauses
                            # that starve the strict producer queue at capacity.
                            # Full decoding remains mandatory in the downstream judge.
                            # origin: hot-update S5 plan §6.6 / §9.1 / DRILL-RUN 75.C
                            try:records.add(fds[0],node,retain_events=False)
                            except (OSError,ValueError):
                                records.broken=True
                                condition.notify_all()
                        continue
                    for fd in fds:os.close(fd)
                    try:
                        if not fds and infra_barrier.accept(raw):
                            condition.notify_all();continue
                    except ValueError:
                        fail('custody_reply_invalid');continue
                    if raw==b'TDOK' and not fds and barrier['waiting']:
                        barrier['revision']+=1
                        barrier['waiting']=False
                        condition.notify_all()
                    else:
                        fail('custody_producer_poison' if raw==b'TDBR' else 'custody_reply_invalid')
        intake=threading.Thread(target=ingest,name='custody-intake')
        intake.start()
        try:
            while True:
                if not select.select([server],[],[],1)[0]:continue
                with server.accept()[0] as conn:
                    request=json.loads(conn.recv(4096));cursor=request['cursor']
                    with condition:
                        require(type(cursor) is int and 0<=cursor<=len(records.frames),'custody_cursor')
                        # One fresh acknowledgement after this request remains
                        # mandatory; an unsolicited or prior ACK cannot release it.
                        if not records.broken:
                            revision=barrier['revision'];barrier['waiting']=True
                            try:
                                packet=infra_barrier.begin(request)
                                product.send(packet or b'TDP1')
                                if not condition.wait_for(lambda:records.broken or (infra_barrier.ack is not None if packet else barrier['revision']>revision),timeout=5 if packet else 2):
                                    fail('custody_barrier_timeout')
                            except OSError:
                                fail('custody_barrier_transport')
                            barrier['waiting']=False
                        end=min(cursor+TRANSFER_FDS,len(records.frames))
                    group=[]
                    try:
                        for index in range(cursor,end):
                            with condition:
                                if records.broken:break
                                fd,offset,length=records.frames[index]
                                borrowed=os.dup(fd)
                            try:
                                data=os.pread(borrowed,length,offset)
                                require(len(data)==length,'custody_archive_truncated')
                                group.append(sealed(data))
                            finally:
                                os.close(borrowed)
                        with condition:
                            reply=dict(cursor=cursor+len(group),complete=cursor+len(group)==len(records.frames),broken=records.broken,failure=records.failure)
                        if 'infrastructure' in request:reply['infrastructure']=infra_barrier.ack
                        control=[] if not group else [(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',group))]
                        conn.sendmsg([json.dumps(reply).encode()],control)
                    finally:
                        for fd in group:os.close(fd)
        finally:
            stop.set()
            product.shutdown(socket.SHUT_RDWR)
            intake.join()
            records.close();path.unlink(missing_ok=True)

def launch(root,node,command):
    require(command and Path(command[0]).is_absolute() and os.access(command[0],os.X_OK),'observer_absolute_artifact_required')
    producer,consumer=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
    producer.set_inheritable(True)
    config=sealed(b'TDCFG1\0\0'+struct.pack('<Q',producer.fileno())+os.urandom(16))
    os.set_inheritable(config,True)
    subprocess.Popen([sys.executable,'-I','-B',__file__,'keeper','--root',str(root),'--node',node,'--fd',str(consumer.fileno())],
                     pass_fds=(consumer.fileno(),),stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    consumer.close()
    env={k:v for k,v in os.environ.items() if k not in ('TETHER_TEST_DRILL_OBSERVER','TETHER_TEST_DRILL_CUSTODY_FD')}
    env.update(TETHER_TEST_DRILL_OBSERVER='v1',TETHER_TEST_DRILL_CUSTODY_FD=str(config))
    os.execve(command[0],command,env)

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=('launch','keeper'));p.add_argument('--root',required=True,type=Path)
    p.add_argument('--node',required=True);p.add_argument('--fd',type=int)
    argv=sys.argv[1:];split=argv.index('--') if '--' in argv else len(argv)
    product=argv[split+1:]
    a=p.parse_args(argv[:split])
    require(a.node in ('agt1','brk1','brk2','brk3'),'observer_node')
    if a.command=='keeper':
        keeper(a.root,a.node,a.fd)
    else:
        launch(a.root,a.node,product)

if __name__=='__main__':
    try:
        main()
    except (OSError,ValueError,KeyError):
        sys.exit(2)

def payload_partial(records, down_calls, actor):
    cuts=records.rows('final-cut','agt1')
    writes=[e for e in records.rows('ss-write','agt1') if e['actor']==actor and e['values'][4]==1 and
        0<e['values'][1]<e['values'][0] and len(e['data'])==e['values'][0]-e['values'][1] and
        (not cuts or e['at']<cuts[0]['at'])]
    require(writes,'partial_cipher_prior_unavailable')
    partial=writes[0]
    payload=[e for e in down_calls if e['digest']==partial['digest'] and e['nonce']==partial['nonce'] and e['at']<=partial['at']]
    require(len(payload)==1 and partial['values'][5]+partial['values'][0]==partial['values'][6] and
        partial['values'][6]==payload[0]['values'][0]+34,'partial_payload_seal_binding')
    return dict(attempt_len=partial['values'][0],written=partial['values'][1],
                suffix_bytes=len(partial['data']),seal_calls=len(payload))


def bind_legacy_original(records,peer,row):
    records.legacy()
    if row['protocol']=='ss':
        business=peer.flows.get(row['id'])
        require(business and business.crypto and business.crypto.rx_key,'original_ss_binding_unavailable')
        digest=hashlib.sha256(business.crypto.rx_key).digest()
        seals=[e for e in records.rows('ss-seal','agt1') if e['digest']==digest]
        require(seals and len({e['actor'] for e in seals})==1,'original_ss_actor_changed')
        states=[e for e in records.rows('ss-state','agt1') if e['actor']==seals[0]['actor'] and len(e['data'])==88]
        require(states and len({e['data'][56:] for e in states})==1,'legacy_ss_socket_changed')
        identity=states[0]['data'][56:]
    else:
        original=row['identities']['target'][0]
        require('tuple' in original,'original_tuple_unavailable')
        identity=hashlib.sha256('\x00'.join(original['tuple']).encode()).digest()
    bindings=[e for e in records.rows('legacy-binding','agt1') if e['data']==identity]
    require(bindings and len({(e['digest'],tuple(e['values'][:5])) for e in bindings})==1 and
            bindings[0]['values'][4]>0 and bindings[0]['digest']!=bytes(32),'legacy_original_carrier_changed')
    value=bindings[0]
    # Legacy has no flow-v1 identity. Record its real stream and generation.
    public=f"{value['values'][0]}:{value['values'][1]}:{value['values'][2]}"
    return dict(before=public,after=public,legacy_stream=value['values'][2])


def bind_original(records, peer, row, transition):
    records.ready()
    if row['protocol']=='ss':
        business=peer.flows.get(row['id'])
        require(business and business.crypto and business.crypto.rx_key,'original_ss_binding_unavailable')
        digest=hashlib.sha256(business.crypto.rx_key).digest()
        seals=[e for e in records.rows('ss-seal','agt1') if e['digest']==digest]
        require(seals and len({e['actor'] for e in seals})==1,'original_ss_actor_changed')
        states=[e for e in records.rows('ss-state','agt1') if e['actor']==seals[0]['actor'] and e['flow']!=bytes(16)]
        require(states and len({e['flow'] for e in states})==1,'original_ss_flow_changed')
        flow_id=states[0]['flow']
    else:
        identity=row['identities']['target'][0]
        require('tuple' in identity,'original_tuple_unavailable')
        digest=hashlib.sha256('\x00'.join(identity['tuple']).encode()).digest()
        sockets=[e for e in records.rows('flow-socket','agt1') if e['digest']==digest]
        require(sockets and len({e['flow'] for e in sockets})==1,'original_business_binding_unavailable')
        flow_id=sockets[0]['flow']
    states=[e for e in records.rows('flow-state','agt1') if e['flow']==flow_id]
    # OPEN can be observed before the connector allocates its original FD.
    # Only that unbound Opening phase may omit identity; all bound receipts,
    # including terminal records, must keep exactly the original fingerprint.
    bound=[e for e in states if e['values'][6]>0]
    require(bound and len({e['endpoint'] for e in states})==1 and
            all(e['values'][6]>0 or e['values'][3]==1 for e in states) and
            len({tuple(e['values'][5:8]) for e in bound})==1,'original_flow_fd_changed')
    endpoint=states[0]['endpoint']
    require(endpoint!=bytes(16),'original_flow_fd_missing')
    resumes=[e for e in records.rows('resume','agt1') if e['endpoint']==endpoint]
    require(resumes and all(e['digest']!=bytes(32) for e in resumes),'original_carrier_binding_unavailable')
    before,after=resumes[0],resumes[-1]
    if transition=='unchanged':
        require(before['digest']==after['digest'] and before['values'][:2]==after['values'][:2],'original_carrier_changed')
    else:
        require(before['digest']!=after['digest'] and after['values'][1]>before['values'][1],'original_carrier_not_changed')
    require(states[-1]['values'][:2]==after['values'][:2],'original_carrier_generation_mismatch')
    # Public evidence contains only IDs and generations; actual TLS identities
    # have already been compared inside the private keeper.
    return dict(endpoint_id=endpoint.hex(),flow_id=flow_id.hex(),
                before=f"{endpoint.hex()}:{before['values'][0]}:{before['values'][1]}",
                after=f"{endpoint.hex()}:{after['values'][0]}:{after['values'][1]}")


def product_receipt(peer, label, request):
    """Consume custody in the client keeper; return only IDs/counts/decisions."""
    if not hasattr(peer,'observations'):
        peer.observations=Records()
        peer.observation_checkpoints={}
    records=peer.observations
    root=Path('/run/tether-drill-custody')/peer.nonce
    fetch(records,root,'agt1');records.ready()
    if label in ('allocation','ready-pending','watchdog','collect','capacity-full','capacity-refused','capacity-released') and request.get('profile')!='legacy-broker':
        sources = request.get('broker_kernels', {'brk1': request.get('broker_kernel')})
        require(sources and all(node in ('brk1','brk2','brk3') for node in sources), 'broker_owner_unavailable')
        # The elected leader runs the reaper independently of allocation home.
        # All three real observers are installed before N3 business starts.
        # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.C
        reaper_sources = ('brk1','brk2','brk3') if request.get('profile') == 'capacity-32' else tuple(sources)
        for source in set(sources) | set(reaper_sources):
            fetch(records, root, source)
    result=dict(nonce=peer.nonce,custody_complete=True)
    # origin: hot-update S5 plan lead C1 / §3.4 / DRILL-RUN.
    # Bind every saturation reservation to its ORIGINAL target tuple. An
    # aggregate byte count cannot hide a killed survivor or another owner.
    if label in ('capacity-full','capacity-refused','capacity-released'):
        originals = {}
        for name,row in request['target']['flows'].items():
            if not name.startswith('saturation-'):
                continue
            identity = row['identities'][0]
            digest = hashlib.sha256('\x00'.join(identity['tuple']).encode()).digest()
            bindings = [e for e in records.rows('flow-socket','agt1') if e['digest'] == digest]
            require(bindings and len({e['flow'] for e in bindings}) == 1, 'capacity_original_binding_missing')
            originals[name] = bindings[0]['flow']
        require(len(originals) == 32 and len(set(originals.values())) == 32, 'capacity_original_count')
        live = records.live_flows()
        require(all(value in live for value in originals.values()), 'capacity_original_state_missing')
        states = [live[value] for value in originals.values()]
        domains = records.domains(request['kernel'])
        used = sum(value['reservation'] for value in domains.values())
        if label == 'capacity-released':
            require(all(e['values'][3] == 5 and e['values'][12] == 0 and e['values'][14:16] == [0,0]
                        for e in states) and used == 0, 'capacity_original_not_settled')
            result.update(settled_originals=32, reservation=used)
        else:
            require(all(e['values'][3] == 3 and e['values'][12] == 8<<20 and e['values'][14:16] == [1,1]
                        for e in states) and used == 256<<20, 'capacity_original_killed_or_unreserved')
            require(domains['exec']['reservation'] == domains['pty']['reservation'] == 0, 'capacity_other_domain_reserved')
            result.update(active_flows=32, reservation=used)
            if label == 'capacity-refused':
                pending = peer.capacity_identity['tuple']
                # origin: hot-update S5 plan lead C1 / §9.1. Native broker receipts
                # already use the peer's tuple order; a second reversal loses the binding.
                digest = hashlib.sha256('\x00'.join(pending).encode()).digest()
                refusals = []
                for source in sources:
                    bindings = {e['flow'] for e in records.rows('flow-socket',source) if e['digest'] == digest}
                    refusals += [e for e in records.rows('flow-capacity',source) if e['flow'] in bindings and
                                 e['values'][:3] == [10,256<<20,256<<20] and e['values'][4] > 0]
                require(refusals and len({(e['source'],tuple(e['values'][3:5])) for e in refusals}) == 1,
                        'capacity_native_refusal_missing')
                result.update(flow_refusals=len(refusals), reason=10)
    elif label=='partial-ready':
        # origin: hot-update S5 plan §9.1 / DRILL-RUN. These are actual
        # original actor/client write counts, never projected from throughput.
        ready, client_counts, product_counts = [], [], []
        for flow in peer.flows.values():
            if flow.crypto is None:
                continue
            crypto = flow.crypto
            calls = [e for e in records.rows('ss-seal','agt1') if crypto.rx_key and e['digest'] == hashlib.sha256(crypto.rx_key).digest()]
            client_counts.append(len(crypto.partials))
            product_counts.append(sum(0 < e['values'][1] < e['values'][0] for e in records.rows('ss-write','agt1') if calls and e['actor'] == calls[0]['actor']))
            try:
                require(calls and crypto.partials, 'partial_cipher_prior_unavailable')
                payload_partial(records, calls, calls[0]['actor'])
                ready.append(True)
            except CustodyError:
                ready.append(False)
        result.update(partial_ready=bool(ready) and all(ready), client_short_writes=client_counts, product_short_writes=product_counts)
    elif label=='open-partial':
        _,partial=records.partial()
        local_prior = root/'open-prior.json'
        if local_prior.exists():
            prior = json.loads(local_prior.read_text())
            # Match the netns census to the native sealed event, rather than
            # trusting a declared FD or a later already-connected snapshot.
            # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
            pending = [event for event in records.rows('flow-state','agt1')
                       if event['at'] == prior['at'] and event['flow'].hex() == prior['flow'] and
                          event['endpoint'].hex() == prior['endpoint'] and event['values'] == prior['values']]
            kernel, target = prior['kernel'], prior['target']
        else:
            pending=list(records.live_flows().values())
            kernel, target=request['kernel'], request['target']
        pending=[e for e in pending if e['values'][3]==2 and e['values'][4]==1 and e['values'][14]==1]
        syn={r['ino'] for r in kernel['connections'] if r['state']=='02'}
        pending=[e for e in pending if e['values'][6] in syn]
        require(len(pending)==1 and not target['flows'].get('opening'), 'original_open_binding_unavailable')
        peer.open_prior=pending[0]
        result.update(open=dict(original_fd=True,phase='EINPROGRESS',syn_block_observed=True,target_accepts=0,
            endpoint_id=pending[0]['endpoint'].hex(),flow_id=pending[0]['flow'].hex()),ss=partial)
    elif label=='open-completed':
        prior=getattr(peer,'open_prior',None)
        require(prior is not None,'original_open_prior_unavailable')
        now=records.live_flows().get(prior['flow'])
        require(now is not None and now['values'][4]==2 and now['values'][5:8]==prior['values'][5:8], 'opening_fd_changed')
        require(request['target']['flows'].get('opening',{}).get('target_accepts')==1,'opening_target_accept_count')
        result.update(original_fd=True,completed=True,application_redials=0)
    elif label in ('revoked','revoke-races'):
        names=['alice'] if label=='revoked' else ['alice','alice-final-cut','alice-resume']
        grant=records.fresh_grant()
        live=records.live_flows();victims=[]
        for name in names:
            flow=peer.flows.get(name)
            require(flow is not None and flow.crypto is not None and flow.crypto.rx_key is not None, 'victim_original_crypto_binding')
            digest=hashlib.sha256(flow.crypto.rx_key).digest()
            seals=[e for e in records.rows('ss-seal','agt1') if e['digest']==digest]
            require(seals,'victim_product_binding')
            actor=seals[0]['actor']
            states=[e for e in records.events if e['source']=='agt1' and e['actor']==actor and e['kind'] in ('ss-binding','ss-state','ss-revoke-mark','ss-closed')]
            require(states and states[-1]['kind']=='ss-closed','victim_not_closed')
            binding=next((e['flow'] for e in states if e['flow']!=bytes(16)),None)
            current=live.get(binding)
            require(current is not None and current['values'][14:16]==[0,0] and current['values'][12]==0,'victim_replay_not_zero')
            sockets={(current['values'][5],current['values'][6])}
            # A closed logical owner can still leak a Prepared replay dup.
            # Match its historical ring OFD against the live kernel census.
            # origin: hot-update S5 plan §6.5 / §9.1 / DRILL-OBSERVER
            for e in records.rows('flow-state','agt1'):
                if e['flow']==binding and len(e['data'])==32:
                    dev,ino=struct.unpack_from('<QQ',e['data'],16)
                    if ino:
                        sockets.add((dev,ino))
            for e in states:
                if len(e['data'])>=56:
                    values=struct.unpack_from('<4Q',e['data'],24)
                    sockets.update((values[i],values[i+1]) for i in (0,2) if values[i+1])
            require(not any((f['dev'],f['ino']) in sockets for f in request['kernel']['fds']), 'victim_dup_not_zero')
            marks=[e for e in states if e['kind']=='ss-revoke-mark']
            require(marks and any(e['at']>=marks[-1]['at'] and e['values'][2]==grant['applied_revision'] for e in records.rows('ss-grant','agt1')),'revoke_revision_unavailable')
            victims.append(dict(name=name,flow_id=binding.hex(),fds=0,dups=0,replay=0,reservation=0))
        require(len({v['flow_id'] for v in victims})==len(names),'revoke_victims_not_distinct')
        bob=peer.flows.get('bob')
        offset=time.monotonic()-time.clock_gettime(time.CLOCK_BOOTTIME)
        last_mark=max(e['at'] for e in records.rows('ss-revoke-mark','agt1'))/1e9+offset
        require(bob is not None and bob.crypto is not None and len(bob.received)>=20 and
                any(sample['at']>=last_mark for sample in bob.received),'survivor_progress_unavailable')
        result.update(victims=victims,fresh_ss_grant=grant,survivor_samples=len(bob.received))
    elif label=='legacy-transport':
        legacy=records.legacy()
        require(peer.flows and all(len(f.sent)>=20 and len(f.received)>=20 for f in peer.flows.values()), 'legacy_samples_unavailable')
        offset=time.monotonic()-time.clock_gettime(time.CLOCK_BOOTTIME)
        require(all(e['at']/1e9+offset<=min(f.start for f in peer.flows.values()) for e in records.rows('carrier-negotiated','agt1')),'legacy_negotiated_after_business')
        public={str(e['values'][0]):e['values'][1] for e in records.rows('legacy-binding','agt1')}
        require(public,'legacy_session_generations_unavailable')
        result.update(carrier_generations=public,alpn=legacy['negotiated_alpn'],raw_prefix_verified=legacy['raw_prefix_observed'],
            flow_header_seen=legacy['flow_header'],before_first_business=True,original_connections=len(peer.flows),
            samples_each_direction=min(min(len(f.sent),len(f.received)) for f in peer.flows.values()))
    elif label in ('allocation','ready-pending','watchdog','collect'):
        result['domains']=records.domains(request['kernel'])
        if request.get('profile')!='legacy-broker':
            # origin: hot-update S5 plan §9.1 / DRILL-RUN. SQL names the
            # real owner after N3 leader placement. Enforce each node's budget
            # against its own sealed records and kernel, never brk1 by habit.
            kernels = request.get('broker_kernels', {'brk1': request.get('broker_kernel')})
            require(kernels and all(value is not None for value in kernels.values()), 'broker_kernel_observation_unavailable')
            domains = {node: broker_flow_domain(records, kernel, node) for node, kernel in kernels.items()}
            result['broker_domains_by_node'] = domains
            result['broker_domains'] = {'flow': {field: sum(value['flow'][field] for value in domains.values())
                                               for field in ('fds','dups','reservation','replay')}}
        if request.get('profile')!='legacy-broker':
            result['reaper']=records.reaper(reaper_sources)
        if request.get('profile') == 'capacity-32':
            require(sum(value['reservation'] for value in result['domains'].values()) <= 224<<20,
                    'normal_capacity_headroom_missing')
        result['seal_attempts']=len(records.nonce_ledger())
        result['duplicate_nonces']=0
        if label!='collect':
            result['fresh_ss_grant']=records.fresh_grant()
        if label in ('ready-pending','watchdog'):
            failures=records.rows('commit-write-failed','agt1')
            require(failures and failures[0]['values'][1:3]==[1,1], 'watchdog_permission_prior_unavailable')
            require(any(e['at']>=failures[0]['at'] for e in records.rows('mark-ready','agt1')),'watchdog_mark_ready_unavailable')
            cuts=records.rows('final-cut','agt1')
            require(cuts,'watchdog_first_cut_unavailable')
            registers=[e for e in records.rows('register-applied','agt1') if cuts[0]['at']<e['at']<=failures[0]['at']]
            require(len(registers)==1,'watchdog_full_register_count')
            result.update(permission_denied=True,commit_write_failed=True,watchdog_armed=True,
                          mark_ready=True,full_register_count=len(registers))
            if label=='watchdog':
                automatic=records.rows('watchdog-current','agt1')
                require(automatic and automatic[-1]['values'][2]==1 and automatic[-1]['values'][1]>=120_000_000_000,'watchdog_automatic_deadline_unavailable')
                require(len(cuts)>=2 and cuts[-1]['values'][0]>cuts[0]['values'][0],'watchdog_second_current_cut_unavailable')
                result.update(elapsed_s=automatic[-1]['values'][1]/1e9,automatic=True,second_cut_current=True,
                    generation_before=cuts[0]['values'][0],generation_after=cuts[-1]['values'][0])
    elif label=='active-progress':
        prior=peer.observation_checkpoints.get('hop-1') or peer.observation_checkpoints.get('baseline')
        require(prior,'private_progress_prior_unavailable')
        live=records.live_flows()
        changed=[e for key,e in live.items() if key in prior['flows'] and
                 e['values'][8]>prior['flows'][key]['values'][8] and e['values'][9]>prior['flows'][key]['values'][9]]
        require(changed,'offset_base_not_advanced')
        states=records.rows('ss-state','agt1')
        actors={e['actor']:e for e in states}
        require(any(key in prior['actors'] and e['data'][:24]!=prior['actors'][key]['data'][:24] for key,e in actors.items()), 'nonce_not_advanced')
        retired={e['endpoint']:e for e in records.rows('flow-retirement','agt1')}
        require(any(e['endpoint'] in prior['retired'] and e['values'][:3]!=prior['retired'][e['endpoint']]['values'][:3] for e in retired.values()), 'retirement_not_advanced')
        result.update(offset_advanced=True,base_advanced=True,nonce_advanced=True,retirement_advanced=True)
    if request.get('profile') == 'capacity-32' and label in ('baseline','hop-1'):
        result['domains'] = records.domains(request['kernel'])
        require(sum(value['reservation'] for value in result['domains'].values()) <= 224<<20,
                'normal_capacity_headroom_missing')
    peer.observation_checkpoints[label]=dict(flows=records.live_flows(),actors={e['actor']:e for e in records.rows('ss-state','agt1')},
                                            retired={e['endpoint']:e for e in records.rows('flow-retirement','agt1')})
    return result
