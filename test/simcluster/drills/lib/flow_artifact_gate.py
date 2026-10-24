#!/usr/bin/env python3
"""Serve the real artifact, holding its final byte until the cut/SYN observer is armed.

The receiver stages almost the entire unchanged artifact before starting the
original connector's fixed deadline. Extraction is observed before OPEN; TLS uses the existing instance certificate.
origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
"""
import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import ssl
import time


def copy_gated(source, destination, gate):
    size = source.seek(0, 2)
    source.seek(0)
    remaining = size - 1
    if remaining < 0:
        raise ValueError('empty_artifact')
    while remaining:
        chunk = source.read(min(65536, remaining))
        if not chunk:
            raise ValueError('truncated_artifact')
        destination.write(chunk)
        remaining -= len(chunk)
    destination.flush()
    (gate / 'waiting').touch()
    deadline = time.monotonic() + 10
    while not (gate / 'release').exists():
        if time.monotonic() >= deadline:
            raise TimeoutError('artifact_gate_not_released')
        time.sleep(.001)
    destination.write(source.read(1))
    destination.flush()


class Handler(SimpleHTTPRequestHandler):
    def copyfile(self, source, destination):
        if self.path == '/candidate.tar.gz':
            copy_gated(source, destination, self.server.gate)
        else:
            super().copyfile(source, destination)

    def do_POST(self):
        if self.path != '/release' or not (self.server.gate/'waiting').is_file():
            self.send_error(409)
            return
        (self.server.gate/'release').touch()
        self.send_response(204)
        self.end_headers()

    def log_message(self, *_):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('serve', 'wait', 'release', 'verify'))
    parser.add_argument('--root', type=Path, default=Path('/run/flow-artifact-gate'))
    parser.add_argument('--cert')
    parser.add_argument('--key')
    args = parser.parse_args()
    if args.command == 'verify':
        if not (args.root/'release').is_file():
            raise ValueError('original_open_prior_not_released')
        return
    if args.command == 'release':
        if not (args.root / 'waiting').is_file():
            raise ValueError('artifact_not_staged')
        (args.root / 'release').touch()
        return
    if args.command == 'wait':
        deadline = time.monotonic() + 5
        while not (args.root / 'waiting').is_file():
            if time.monotonic() >= deadline:
                raise TimeoutError('artifact_not_staged')
            time.sleep(.01)
        return
    args.root.mkdir(mode=0o700)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(args.cert, args.key)
    server = ThreadingHTTPServer(('0.0.0.0', 443), partial(Handler, directory='/artifacts'))
    server.gate = args.root
    server.socket = context.wrap_socket(server.socket, server_side=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
