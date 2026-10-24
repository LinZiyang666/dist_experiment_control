"""Decode S9 public fragments on the existing sealed product custody channel.

No SDK lookup or producer pause is available here. TDIA certifies a newly
sampled original roster; prefix rows remain the first actual Tx/Step gate read.
origin: hot-update S9 plan §2.3 / §4.3 / §9.1; NC-EXPORTS consumer handoff.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import struct
import time

DOMAINS = {'infra-attachment': 'infra', 'infra-prefix': 'store'}
MAX_ROW = 4 << 20


def need(ok, reason):
    from flow_observation import require
    require(ok, 'custody_infrastructure_' + reason)


def fragment(event):
    values = event['values']
    need(event['kind'] in DOMAINS and event['domain'] == DOMAINS[event['kind']], 'kind')
    need(len(values) == 16 and all(type(v) is int and 0 <= v < 1 << 64 for v in values), 'fragment')
    offset, total, step = values[:3]
    need(0 < total <= MAX_ROW and 0 <= offset < total and 0 < len(event['data']) <= 32768 and
         offset + len(event['data']) <= total and not any(values[3:]) and step < 1 << 32 and
         (event['kind'] != 'infra-attachment' or step == 0), 'fragment')
    need(event['actor'] != bytes(16) and event['digest'] != bytes(32) and event['at'] > 0 and
         event['endpoint'] == event['flow'] == bytes(16) and event['nonce'] == bytes(12), 'identity')


def reassemble(events):
    """Join each emitter independently; never splice different reads."""
    from hot_update_infra_probe import unique_object
    result, pending = [], {}
    for event in events:
        fragment(event)
        offset, total, step = event['values'][:3]
        key = (event.get('source'), event.get('incarnation'), event['kind'], event['actor'], step, total, event['digest'])
        owner = key[:5]
        if owner not in pending:
            need(offset == 0, 'fragment_gap')
            pending[owner] = (key, bytearray())
        expected, data = pending[owner]
        need(key == expected and offset == len(data), 'fragment_gap')
        data.extend(event['data'])
        if len(data) == total:
            need(hashlib.sha256(data).digest() == event['digest'], 'digest')
            try:
                row = json.loads(bytes(data).decode('utf-8'), object_pairs_hook=unique_object,
                                 parse_constant=lambda _: need(False, 'json'))
            except (ValueError, UnicodeError):
                need(False, 'json')
            need(type(row) is dict, 'json')
            result.append(dict(kind=event['kind'], actor=event['actor'], step=step, at=event['at'], row=row))
            del pending[owner]
    need(not pending, 'fragment_truncated')
    return result


def validate_ack(packet, challenge, sealed_sequence, before_sequence, events):
    need(len(packet) == 32 and packet[:4] == b'TDIA' and packet[4:16] == challenge and
         len(challenge) == 12 and challenge != bytes(12), 'ack')
    watermark, failed = struct.unpack_from('<QQ', packet, 16)
    need(failed == 0 and before_sequence < watermark == sealed_sequence, 'ack')
    rows = reassemble([e for e in events if e['kind'] == 'infra-attachment'])
    need(0 < len(rows) <= 128 and len({r['actor'] for r in rows}) == len(rows), 'roster')
    need(all(r['row'].get('Participant') == r['actor'].hex() for r in rows), 'roster')
    return rows


def chunks(rows):
    need(type(rows) is list, 'chunks')
    previous = -1
    for row in rows:
        need(type(row) is dict and set(row) == {'index', 'length', 'sha256'} and
             type(row['index']) is int and previous < row['index'] < 1 << 64 and
             type(row['length']) is int and 0 < row['length'] < 1 << 64 and
             type(row['sha256']) is str and re.fullmatch('[0-9a-f]{64}', row['sha256']), 'chunks')
        previous = row['index']


def prefixes(rows, tx, step, transfer_ids, clock_offset):
    need(type(tx) is str and re.fullmatch('[0-9a-f]{32}', tx) and tx != '0'*32 and
         type(step) is int and 0 <= step < 1 << 32 and type(clock_offset) is int, 'binding')
    need(transfer_ids and len(set(transfer_ids)) == len(transfer_ids), 'originals')
    selected = {}
    for event in rows:
        if event['kind'] != 'infra-prefix' or event['actor'].hex() != tx or event['step'] != step:
            continue
        row = event['row']
        need(set(row) == {'transfer_id', 'at_ns', 'before_repair_ns', 'expected', 'actual'}, 'prefix_fields')
        tid = row['transfer_id']
        if tid not in transfer_ids:
            continue
        chunks(row['expected']); chunks(row['actual'])
        need(row['expected'] and [r['index'] for r in row['expected']] == list(range(len(row['expected']))) and
             type(row['at_ns']) is int and type(row['before_repair_ns']) is int and
             0 < row['at_ns'] <= row['before_repair_ns'] <= event['at'] < 1 << 64, 'prefix_clock')
        need(all(r['index'] < len(row['expected']) for r in row['actual']), 'chunks')
        # Preserve an incomplete first read even if a later replay repaired it.
        # The independent P32 equality oracle must see that original loss.
        if tid not in selected:
            mapped = dict(row, at_ns=row['at_ns'] + clock_offset,
                          before_repair_ns=row['before_repair_ns'] + clock_offset)
            need(0 < mapped['at_ns'] <= mapped['before_repair_ns'], 'clock')
            selected[tid] = mapped
    need(set(selected) == set(transfer_ids), 'prefix_requiredset')
    return [selected[tid] for tid in transfer_ids]


class Barrier:
    """Runs under the keeper condition; the intake alone mutates Records."""
    def __init__(self, records, source):
        self.records, self.source, self.pending, self.ack = records, source, None, None

    def begin(self, request):
        value = request.get('infrastructure')
        if value is None:
            return None
        need(type(value) is dict and set(value) == {'challenge', 'remaining_ns'}, 'request')
        challenge, remaining = value['challenge'], value['remaining_ns']
        need(type(challenge) is str and re.fullmatch('[0-9a-f]{24}', challenge) and challenge != '0'*24 and
             type(remaining) is int and 0 < remaining <= 5_000_000_000, 'request')
        before = self.records.sequences.get((self.source, self.records.current_incarnations.get(self.source)), 0)
        self.pending = (bytes.fromhex(challenge), before, len(self.records.events),
                        self.records.current_incarnations.get(self.source))
        self.ack = None
        return b'TDI1' + bytes.fromhex(challenge) + struct.pack('<Q', remaining)

    def accept(self, packet):
        if not packet.startswith(b'TDIA'):
            return False
        need(self.pending is not None and self.ack is None, 'unsolicited_ack')
        nonce, before, start, incarnation = self.pending
        current = self.records.current_incarnations.get(self.source)
        need(current is not None and (incarnation is None or current == incarnation), 'incarnation')
        validate_ack(packet, nonce, self.records.sequences[(self.source, current)], before,
                     self.records.events[start:])
        self.ack = dict(packet=packet.hex(), identity=self.records.identities[self.source].hex(),
                        incarnation=current.hex(), before_sequence=before,
                        sequence=self.records.sequences[(self.source, current)])
        return True


def collect(root, source, budget_ns=5_000_000_000):
    """Fetch exact sealed pages through one fresh TDIA, then a bounded drain."""
    import flow_observation as flow
    need(type(source) is str and re.fullmatch('[a-zA-Z0-9_.-]+', source), 'source')
    need(type(budget_ns) is int and 0 < budget_ns <= 5_000_000_000, 'request')
    records, cursor, acknowledgement = flow.Records(), 0, None
    nonce = os.urandom(12)
    deadline = time.monotonic_ns() + budget_ns
    try:
        while True:
            remaining = deadline - time.monotonic_ns()
            need(remaining > 0, 'timeout')
            request = dict(cursor=cursor)
            if acknowledgement is None:
                request['infrastructure'] = dict(challenge=nonce.hex(), remaining_ns=remaining)
            with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as connection:
                connection.settimeout(remaining / 1e9)
                connection.connect(str(Path(root) / (source + '.sock')))
                connection.send(json.dumps(request).encode())
                raw, fds = flow.receive(connection)
            try:
                reply = json.loads(raw)
                need(reply.get('broken') is False and reply.get('cursor') == cursor + len(fds), 'keeper')
                if acknowledgement is None:
                    acknowledgement = reply.get('infrastructure')
                    need(type(acknowledgement) is dict and set(acknowledgement) ==
                         {'packet', 'identity', 'incarnation', 'before_sequence', 'sequence'}, 'ack')
                while fds:
                    records.add(fds.pop(0), source)
            finally:
                for fd in fds: os.close(fd)
            cursor = reply['cursor']
            if reply.get('complete') is True:
                break
        identity, incarnation = records.identities.get(source), records.current_incarnations.get(source)
        need(identity is not None and incarnation is not None and identity.hex() == acknowledgement['identity'] and
             incarnation.hex() == acknowledgement['incarnation'], 'identity')
        before, watermark = acknowledgement['before_sequence'], acknowledgement['sequence']
        need(type(before) is int and type(watermark) is int and records.sequences[(source, incarnation)] >= watermark, 'ack')
        fresh = [e for e in records.events if e.get('incarnation') == incarnation and
                 before < e.get('sequence', 0) <= watermark]
        roster = validate_ack(bytes.fromhex(acknowledgement['packet']), nonce, watermark, before, fresh)
        all_rows = reassemble([e for e in records.events if e['kind'] in DOMAINS])
        return dict(roster=roster, rows=all_rows, boot_ns=time.clock_gettime_ns(time.CLOCK_BOOTTIME))
    finally:
        records.close()
