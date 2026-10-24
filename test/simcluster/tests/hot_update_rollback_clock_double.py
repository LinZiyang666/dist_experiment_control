#!/usr/bin/env python3
"""Interpreter-clock double for drill 38's embedded PY38 runner (r9d D-11 arm controls only).

origin: hot-update S8 plan §10 / §11.2 P27; r9d D-11.

The drill pipes its REAL heredoc program to "$S8_PYTHON" -I -B - <args>; the
driver points S8_PYTHON at a two-line wrapper that execs this bootstrap instead.
It records the SHA-256 of the exact program text it received (the driver compares
it with the heredoc it extracts from the drill file, so nothing else can run in
its place), replaces only time.monotonic/time.sleep with a virtual clock, and
then executes that text as __main__. The family-A arms' two 300-second
calibrations therefore run their real sampling loop over 600 virtual seconds;
the clock is a deployment boundary, the loop and its deadline checks are not.
"""
import hashlib
import json
import os
import sys
import time


class VirtualClock:
    def __init__(self, start):
        self.t = float(start)
        self.slept = 0.0
        self.sleeps = 0

    def monotonic(self):
        return self.t

    def sleep(self, seconds):
        if seconds < 0:
            raise ValueError('negative sleep')
        self.t += seconds
        self.slept += seconds
        self.sleeps += 1


def main():
    program = sys.stdin.read()
    clock = VirtualClock(1000.0)
    sys.modules['r9d38_clock'] = clock
    with open(os.environ['R9D38_TRACE'], 'a') as stream:
        stream.write(json.dumps(dict(event='program', sha256=hashlib.sha256(program.encode()).hexdigest(),
                                     argv=sys.argv[1:]), sort_keys=True) + '\n')
    time.monotonic = clock.monotonic
    time.sleep = clock.sleep
    sys.argv = ['-', *sys.argv[1:]]
    try:
        exec(compile(program, '<stdin>', 'exec'), {'__name__': '__main__', '__builtins__': __builtins__})
    finally:
        with open(os.environ['R9D38_TRACE'], 'a') as stream:
            stream.write(json.dumps(dict(event='clock', slept=clock.slept, sleeps=clock.sleeps, t=clock.t),
                                    sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
