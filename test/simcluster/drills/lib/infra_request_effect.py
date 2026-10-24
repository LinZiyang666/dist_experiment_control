#!/usr/bin/env -S python3 -I -B
"""Public target effect for S9 write/short-exec attempts.

Every actual invocation appends one durable row. There is deliberately no
harness deduplication: a duplicate product execution must remain visible to
the independent exactly-once oracle. No credential or arbitrary payload is
accepted. The root is pre-provisioned in the disposable target namespace.
origin: hot-update S9 plan §9.1 / T76/T77; S9 INSTRUMENTS-2.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import sys


def effect(root, logical, kind):
    if not re.fullmatch(r'[0-9a-f]{32}-[0-9]{1,8}', logical) or kind not in ('write', 'short_exec'):
        raise ValueError('effect_identity')
    root = Path(root)
    info = root.lstat()
    if not root.is_absolute() or root.is_symlink() or not root.is_dir() or info.st_mode & 0o077:
        raise ValueError('effect_private_root')
    directory = os.open(root, os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        fd = os.open('effects.jsonl', os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
                     0o600, dir_fd=directory)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            data = (json.dumps(dict(logical_id=logical, effect='applied'), sort_keys=True) + '\n').encode()
            while data:
                count = os.write(fd, data)
                if count <= 0:
                    raise OSError('effect_short_write')
                data = data[count:]
            os.fsync(fd)
            os.fsync(directory)
        finally:
            os.close(fd)
    finally:
        os.close(directory)
    if kind == 'short_exec':
        print(logical)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=('write', 'short_exec'))
    parser.add_argument('--root', required=True)
    parser.add_argument('--logical-id', required=True)
    args = parser.parse_args()
    try:
        effect(args.root, args.logical_id, args.kind)
        return 0
    except (OSError, ValueError):
        print('infra effect: target input or durable write failed', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
