#!/usr/bin/env -S python3 -I -B
"""Hermetic owner boundary for the composer tests; no product operations.

Only target receipts are doubled. Four real foreground processes must be alive
before ready can answer. Knobs deliberately remove a receipt or terminate an
owner. Nothing emits GREEN; P32 and the real composer validate the projection.
origin: hot-update S9 plan §9.1 / S9 COMPOSER contract controls.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('operation')
    parser.add_argument('--model', required=True)
    parser.add_argument('--context', required=True)
    parser.add_argument('--evidence')
    args, extra = parser.parse_known_args()
    context = json.loads(Path(args.context).read_text())
    root = Path(context['evidence'])
    knobs = json.loads((root / 'knobs.json').read_text()) if (root / 'knobs.json').exists() else {}
    if args.operation == 'prepare':
        return int(knobs.get('prepare_rc', 0))
    if args.operation == 'keeper':
        directory = Path(args.evidence)
        (directory / 'pid').write_text(str(os.getpid()))
        while not (directory / 'stop').exists():
            if knobs.get('exit_owner') == directory.name:
                return 9
            (directory / 'heartbeat').write_text(str(time.monotonic_ns()))
            time.sleep(.02)
        return int(knobs.get('stop_rc', 0))
    if args.operation == 'stop':
        Path(args.evidence, 'stop').touch()
        return 0
    if args.operation == 'ready':
        deadline = time.monotonic() + 5
        for name in ('flow', 'pty', 'exec', 'transfers'):
            while not (root / name / 'pid').exists():
                if time.monotonic() > deadline:
                    return 2
                time.sleep(.01)
            os.kill(int((root / name / 'pid').read_text()), 0)
        row = json.loads(Path(args.model).read_text())
        row['seconds'] = context['seconds']
        if knobs.get('missing_original'):
            row['originals'].pop(knobs['missing_original'])
        if knobs.get('missing_transfer'):
            row['transfers'] = row['transfers'][:-1]
        if knobs.get('new_rate'):
            row['transfers'][0]['rate_limit'] += 1
        if knobs.get('new_manifest'):
            row['s7_manifest_sha256'] = 'f' * 64
        if knobs.get('redial'):
            row['originals']['ssh']['identity'] = {}
        print(json.dumps(row))
        return 0
    if args.operation == 'client':
        logical = extra[0]
        fd = int(os.environ['TETHER_ATTEMPT_RECEIPT_FD'])
        now = time.monotonic_ns()
        row = dict(logical_id=logical, ordinal=1, phase='request', started_mono_ns=now,
                   finished_mono_ns=now, sent=True, backoff_ns=0, **{'class': 'ok'})
        os.write(fd, (json.dumps(row) + '\n' + json.dumps(dict(kind='observer_close', logical_id=logical,
                 attempt_count=1, overflow=False)) + '\n').encode())
        print('ok ' + logical)
        return 0
    if args.operation == 'reader':
        print(json.dumps(dict(context_nonce=context['nonce'], arguments=extra)))
        return 0
    return 2


if __name__ == '__main__':
    raise SystemExit(main())
