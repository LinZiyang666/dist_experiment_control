#!/usr/bin/env -S python3 -I -B
"""S9 finite mixed-flow admission on the existing S5/S10 real peer.

Normal/SSH/SS sources use S10's duration declaration. Idle and asymmetric
half-close sources retain S5's byte contracts. Upstream's supplied-source
deadline assumes every source has supply, so admit the supplied subset first
and install the two finite special classes in that SAME configure operation.
No reconfiguration or additional connect is accepted after this initial seal.
origin: hot-update S9 plan §9.1 / LQ3; S9 INSTRUMENTS-2.
"""
import importlib.util
from pathlib import Path
import sys
import time


def implementation():
    spec = importlib.util.spec_from_file_location('infra_original_flow_peer', Path(__file__).with_name('flow_fixture_peer.py'))
    peer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(peer)
    original_peer, original_flow = peer.Peer, peer.Flow

    class InfraPeer(original_peer):
        def command(self, request, rights=()):
            # origin: hot-update S9 plan §9.1; S9 INSTRUMENTS-3.
            # Resolve public carrier IDs inside the existing sealed observer.
            # A peer PID, URL or application nonce is not a flow carrier ID.
            if request['command'] == 'infra-bindings':
                observation = peer.observation
                records = observation.Records()
                observation.fetch(records, Path('/run/tether-drill-custody') / self.nonce, 'agt1')
                records.ready()
                result = {}
                for name, flow in self.flows.items():
                    if name not in request['target']['flows']:
                        raise ValueError('original target flow absent')
                    row = dict(id=name, protocol=flow.spec['protocol'], identities=dict(
                        target=request['target']['flows'][name]['identities']))
                    result[name] = observation.bind_original(records, self, row, 'unchanged')
                if not result:
                    raise ValueError('original carrier binding absent')
                return result
            if request['command'] != 'configure':
                return super().command(request, rights)
            rows = request['flows']
            special = [row for row in rows if 'supply' not in row]
            if not special:
                return super().command(request, rights)
            if self.specs or self.flows or len({row['id'] for row in rows}) != len(rows):
                raise ValueError('flow declarations already sealed or duplicated')
            for row in special:
                idle = row['id'] == 'tcp-1' and row['class'] == 'idle' and all(row[d]['records'] == 0 for d in ('up', 'down'))
                half = row['id'] == 'half-close' and row['class'] == 'half-close' and row['up']['records'] == 20 and row['down']['records'] > 0
                if not (idle or half):
                    raise ValueError('unknown mixed finite flow')
                for direction in ('up', 'down'):
                    expected = peer.declaration(row['id'] + '.' + direction, self.nonce,
                                                row[direction]['records'], row['size'])
                    if row[direction] != expected:
                        raise ValueError('mixed finite declaration changed')
            supplied = [row for row in rows if 'supply' in row]
            if not supplied:
                raise ValueError('mixed flow duration source absent')
            result = super().command(dict(request, flows=supplied), rights)
            self.specs.update({row['id']: row for row in special})
            return result

    class IdleFlow(original_flow):
        def run(self):
            super().run()
            if self.spec['class'] == 'idle':
                # Keep the actual receive worker/OFD alive after its empty
                # declared byte range, just like the existing broker profile.
                self.end = self.start
                self.done.set()

    peer.Peer, peer.Flow = InfraPeer, IdleFlow
    return peer


if __name__ == '__main__':
    raise SystemExit(implementation().main())
