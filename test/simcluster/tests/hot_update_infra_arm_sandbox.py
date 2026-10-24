#!/usr/bin/env python3
"""Sandbox construction for the r9d drill 85/86/39.NI/CI arm controls.

origin: S9 FIX-INFRAVERBS (r9d finding 5); hot-update S9 plan §9.1 / §9.3 / P33.

build() lays out one private sandbox: symlinks to the real drill and shell
libraries, byte-identical COPIES of the real fixture library (so its own
imports resolve inside the sandbox), the deployment-boundary doubles ($SIM,
$DOCKER, vendor verifiers, release-source supplier, poisoned deployment tools),
stand-ins for other owners' oracles (S7 transfer_fixture, S5 flow probe) and
the composer supply vendor/infra-fixture.json whose instruments are
tests/hot_update_infra_deployment_double.py. Loopback serves the six canonical
TLS lanes on loopback ports for arms with Caddy hops (a hermetic binding: the
library labels that FINAL evidence_scope=hermetic). Nothing here is a product,
fixture or oracle implementation.
"""
import hashlib
import json
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import threading

SIM = Path(__file__).resolve().parents[1]
DOUBLE = Path(__file__).resolve().with_name('hot_update_infra_deployment_double.py')
PY = '/usr/bin/python3'
LINKS = ('lib/log.sh', 'lib/assert.sh', 'lib/docker.sh', 'lib/tether.sh', 'lib/secrets.sh', 'lib/stage.sh',
         'drills/lib/cluster.sh', 'drills/lib/logs.sh', 'drills/lib/hot_update_budget.sh',
         'drills/lib/ingress.sh', 'drills/lib/artifact.sh', 'drills/lib/upgradecfg.sh', 'drills/lib/hot_update_pty.sh',
         'drills/85-hot-update-nats.sh', 'drills/86-hot-update-caddy.sh', 'drills/39-hot-update-legacy-n1.sh')
LIBRARY = ('hot_update_infra.sh', 'hot_update_infra_verbs.py', 'hot_update_infra_scope.py',
           'hot_update_infra_workload.py', 'hot_update_infra_probe.py', 'transfer_observation.py')
POISONED = ('docker', 'ssh', 'scp', 'sudo', 'systemctl', 'journalctl', 'tether', 'nsenter')
VERSIONS = {'nats': ('nats-server', '2.14.5', '2.14.6'), 'caddy': ('caddy', '2.7.5', '2.7.6')}
FAULTS = ('nats-n2', 'nats-lost-quorum', 'nats-stream-lag', 'nats-consumer-lag', 'after-ldm-reply-cut',
          'acme-late-write-final-copy-over-2s', 'main-failure-controller-death-shadow-over-120s')
LANES = ('external4', 'external6', 'output4', 'output6', 'hairpin4', 'hairpin6')

COMMON = r'''# Shared by the shell doubles. Never sourced by a drill.
F=${INFRA_ARM_FAKE:?infra arm double requires its private directory}
rec() { { printf '%s\037%s' "$(date +%s%N)" "$1"; shift; for _a in "$@"; do printf '\037%s' "$_a"; done; printf '\n'; } >> "$F/calls"; }
# rule <tool> <joined args> : TAB rows <tool> <glob> <rc> <nth|-> <output|->; first live row answers.
rule() {
    R_RC=0; R_OUT=-; _ri=0; _rchosen=0
    [ -f "$F/rules" ] || return 0
    while IFS='	' read -r _rt _rp _rc _rn _ro; do
        _ri=$((_ri + 1))
        [ "$_rt" = "$1" ] || continue
        case "$2" in $_rp) ;; *) continue ;; esac
        _seen=$(( $(cat "$F/rule.$_ri" 2>/dev/null || echo 0) + 1 ))
        echo "$_seen" > "$F/rule.$_ri"
        [ "$_rchosen" = 0 ] || continue
        if [ "$_rn" = - ] || [ "$_rn" = "$_seen" ]; then _rchosen=$_ri; R_RC=$_rc; R_OUT=$_ro; fi
    done < "$F/rules"
    [ "$_rchosen" = 0 ] || : > "$F/hit.$_rchosen"
}
'''

SIM_DOUBLE = r'''#!/bin/sh
# $SIM double: topology verbs, cluster status probes, product CLI and helper execs.
. "${INFRA_ARM_FAKE:?}/common.sh"
rec sim "$@"
rule sim "$*"
case "${1:-}" in
nuke|init|grow) exit "$R_RC" ;;
up) [ "$R_RC" = 0 ] || exit "$R_RC"; [ "${2:-}" = --brokers ] && printf '%s\n' "$3" > "$F/brokers"; exit 0 ;;
status)
    [ "$R_RC" = 0 ] || exit "$R_RC"
    _leader=brk1; [ "$R_OUT" = - ] || _leader=$R_OUT
    printf '{"is_leader_view":true,"leader_id":"%s"}\n' "$_leader"; exit 0 ;;
exec)
    _node=${2:?}; shift 2; [ "${1:-}" = -- ] && shift
    case "${1:-}" in
    tether)
        [ "$*" = 'tether cluster status --json' ] || exit 97
        _n=$(cat "$F/brokers" 2>/dev/null || echo 0); _i=1; _sep=
        printf '{"leader_id":"brk1","nodes":['
        while [ "$_i" -le "$_n" ]; do printf '%s{"node_id":"brk%s","phase":"VOTER"}' "$_sep" "$_i"; _sep=,; _i=$((_i + 1)); done
        printf ']}\n'; exit 0 ;;
    curl) printf '{"meta_cluster":{"cluster_size":2}}\n'; exit 0 ;;
    runuser)
        : > "$F/restart-armed"
        case "$*" in
        *' status --tx '*|*' --status '*)
            if [ "$R_OUT" != - ]; then printf '%s\n' "$R_OUT"; exit "$R_RC"; fi
            /usr/bin/python3 -I -c 'import json,sys; print(json.dumps(dict(Tx=list(bytes.fromhex(sys.argv[1])), Step=1, Phase=11, Revision=1, Code="infra_recovery_required", Detail="", Before={}, Current={}, Census={}, Proof=[1]*32)))' "$TX32"
            exit "$R_RC" ;;
        *' recover '*|*' --recover '*) : > "$F/recovered" ;;
        *--to-version*)
            # sample.no_halt: the product only answers once a hop has retired.
            _w=0
            while grep -q no_halt "$F/knobs.json" && ! grep -q '"retired"' "$F/emitted.jsonl" 2>/dev/null && [ "$_w" -lt 300 ]; do
                sleep 0.1; _w=$((_w + 1))
            done ;;
        esac
        if [ "$R_OUT" != - ]; then printf '%s\n' "$R_OUT"; exit "$R_RC"; fi
        case "$R_RC:$*" in
        75:*) echo 'infra upgrade deferred: infra_not_ready' ;;
        74:*) echo "infra upgrade halted: infra_recovery_required tx=$TX32; inspect: tether admin infra status --tx $TX32" ;;
        64:*--reload-config*) echo 'infra upgrade refused: infra_config_invalid' ;;
        64:*) echo 'infra upgrade refused: infra_unsupported' ;;
        *) echo "infra product command double (rc $R_RC)" ;;
        esac
        exit "$R_RC" ;;
    /usr/local/libexec/tether-infra-helper)
        # Bootstrap is not an upgrade effect: it never arms the effect knobs.
        [ "${2:-}" = bootstrap ] && [ "$R_RC" = 0 ] && : > "$F/managed.$_node"
        printf 'infra helper double %s (rc %s)\n' "${2:-}" "$R_RC"; exit "$R_RC" ;;
    esac
    rec unexpected sim-exec "$_node" "$@"; exit 97 ;;
esac
rec unexpected sim "$@"
exit 97
'''

TOOL_DOUBLE = r'''#!/bin/sh
# Generic recording double for supply verifiers and the release-source supplier.
. "${INFRA_ARM_FAKE:?}/common.sh"
_tool=$1; shift
rec "$_tool" "$@"
rule "$_tool" "$*"
exit "$R_RC"
'''

DOCKER_DOUBLE = r'''#!/bin/sh
# $DOCKER double: only sim_log_snapshot's `exec <ctr> sh -c <reader> _ <stream> <program>`.
. "${INFRA_ARM_FAKE:?}/common.sh"
if [ "${1:-}" = exec ] && [ "${3:-}" = sh ] && [ "${4:-}" = -c ] && [ "${6:-}" = _ ]; then
    _node=${2#sim-${INSTANCE:?}-}
    rec logs "$_node" "$7"
    rule logs "$_node $7"
    [ "$R_RC" = 0 ] || exit "$R_RC"
    # A second read is the final window: an append, optionally carrying a panic.
    _raw=bG9nCg==; _len=4
    if [ -e "$F/log.$_node.$7" ]; then _raw=bG9nCm1vcmUK; _len=9; [ "$R_OUT" = panic ] && { _raw=bG9nCnBhbmljOiBib29tCg==; _len=16; }; fi
    : > "$F/log.$_node.$7"
    printf '{"metadata":{"inode":7,"length":%s,"read_ok":true},"raw":"%s"}\n' "$_len" "$_raw"; exit 0
fi
rec unexpected docker "$@"
exit 97
'''

POISON = r'''#!/bin/sh
. "${INFRA_ARM_FAKE:?}/common.sh"
rec poison "${0##*/}" "$@"
exit 126
'''

UPGRADE_ARTIFACTS_DOUBLE = r'''# Hermetic double of lib/upgrade-artifacts.sh (release-source supplier).
sim_upgrade_artifacts() { "$INFRA_ARM_FAKE/tool" artifacts "$@"; }
'''

VERIFIER_DOUBLE = r'''import os, sys
os.execv('/bin/sh', ['/bin/sh', os.path.join(os.environ['INFRA_ARM_FAKE'], 'tool'), '@NAME@', *sys.argv[1:]])
'''

# Other owners' oracles behind the composer: S7's keeper/judge and S5's flow judge.
TRANSFER_STUB = r'''"""Sandbox stand-in for S7's drills/lib/transfer_fixture.py (r9d infra arm double)."""
import hashlib, importlib.util, json, os, sys
from pathlib import Path
_spec = importlib.util.spec_from_file_location('infra_arm_double', os.environ['INFRA_ARM_DOUBLE'])
_double = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_double)
def reader_digest():
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
def read_manifest(path):
    value = json.loads(Path(path).read_text())
    if value.get('mode') != 'live':
        raise ValueError('live_s7_fixture_required')
    return value
if __name__ == '__main__':
    verb, options = sys.argv[1], dict(zip(sys.argv[2::2], sys.argv[3::2]))
    if verb == 'sample':
        path = Path(options['--evidence']) / ('snapshot-%d.json' % os.getpid())
        path.write_text(json.dumps(_double.s7_snapshot(options['--manifest'])))
        print(json.dumps(dict(native_rc=0, snapshot=str(path))))
        sys.exit(0)
    result = _double.s7_judge(options['--manifest'], options['--evidence'], reader_digest())
    print(json.dumps(result))
    sys.exit(result['native_rc'])
'''

FLOW_STUB = r'''"""Sandbox stand-in for S5's drills/lib/hot_update_flow_probe.py (r9d infra arm double)."""
import importlib.util, os, sys
class EvidenceError(ValueError):
    pass
class ContinuityError(EvidenceError):
    pass
def _defect():
    spec = importlib.util.spec_from_file_location('infra_arm_double', os.environ['INFRA_ARM_DOUBLE'])
    double = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(double)
    return double.knob('flow.defect')
def judge(root, nonce, carrier_transition='replaced'):
    if _defect():
        raise ContinuityError('hermetic flow continuity defect')
    return dict(verdict='GREEN')
if __name__ == '__main__':
    sys.exit(1 if _defect() else 0)
'''


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class Loopback:
    """Canonical-lane stand-in: one TLS listener per family, one thread per handshake.

    The throwaway key lives in the run's own sandbox, so no interrupted run can
    leave key material behind; it is never a product CA or certificate.
    """
    def __init__(self, directory):
        self.key, self.cert = Path(directory) / 'loopback-key.pem', Path(directory) / 'loopback-cert.pem'
        subprocess.run([shutil.which('openssl') or '/usr/bin/openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(self.key), '-out', str(self.cert), '-days', '1', '-subj', '/CN=localhost',
                        '-addext', 'subjectAltName=DNS:localhost'], check=True, capture_output=True, timeout=60)
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.context.load_cert_chain(self.cert, self.key)
        self.stop, self.listeners, self.ports, self.threads = threading.Event(), [], {}, []
        for family, address in ((socket.AF_INET, '127.0.0.1'), (socket.AF_INET6, '::1')):
            listener = socket.socket(family)
            listener.bind((address, 0))
            listener.listen(512)
            listener.settimeout(.2)
            self.ports[address] = listener.getsockname()[1]
            self.listeners.append(listener)
            self.threads.append(threading.Thread(target=self.accept, args=(listener,), daemon=True))
            self.threads[-1].start()

    def accept(self, listener):
        while not self.stop.is_set():
            try:
                raw = listener.accept()[0]
            except socket.timeout:
                continue
            threading.Thread(target=self.serve, args=(raw,), daemon=True).start()

    def serve(self, raw):
        try:
            raw.settimeout(10)
            with self.context.wrap_socket(raw, server_side=True) as secure:
                while secure.recv(1024):
                    pass
        except (OSError, ssl.SSLError):
            pass
        finally:
            raw.close()

    def close(self):
        self.stop.set()
        for thread in self.threads:
            thread.join(5)
        for listener in self.listeners:
            listener.close()


def binding(root, loopback):
    # A tiny pinned launcher keeps each byte-bound exec cheap: the library
    # hashes argv[0] on every command, and hashing an 8 MiB interpreter ten
    # times a second would measure this sandbox, not the fixture.
    launcher = root / 'vendor/infra-composer-double'
    write(launcher, '#!/bin/sh\nexec %s -I -B %s "$@"\n' % (PY, DOUBLE))

    def spec(*args, **extra):
        return dict(argv=[str(launcher)] + list(args), sha256=sha(launcher),
                    inputs=[dict(path=str(DOUBLE), sha256=sha(DOUBLE))], **extra)
    handoff = root / 'vendor/s7-handoff.txt'
    handoff.write_text('hermetic handoff stand-in, not a lead-accepted S7 receipt\n')
    files = {k: dict(path=str(handoff), sha256=sha(handoff)) for k in ('executable', 'source', 'schema', 'positive', 'negative')}
    tls = {lane: dict(address='127.0.0.1' if lane.endswith('4') else '::1', server_name='localhost', ca_file=str(loopback.cert),
                      reader=spec('tls-reader'), port=loopback.ports['127.0.0.1' if lane.endswith('4') else '::1'])
           for lane in LANES} if loopback else {}
    return dict(schema='infra-binding-v1', mode='hermetic' if loopback else 'live',
                handoff=dict(accepted_by='lead', schema='transfer-v1', files=files),
                cohort=dict(keeper=spec('keeper'), control=spec('control')), tls=tls,
                commands={k: spec('client', k, '{logical_id}', mode='cli', expected_output='ok {logical_id}')
                          for k in ('node_ls', 'ps', 'read', 'write', 'short_exec')},
                readers={k: spec(k) for k in ('sample', 'progress', 'effects', 'final', 'node')},
                faults={p: dict(install=spec('fault-install'), status=spec('fault-status'), heal=spec('fault-heal')) for p in FAULTS},
                exercises={s: spec('exercise') for s in ('store-double-open', 'exact-release-join', 'caddy-responsibility-gates')},
                configs={v: spec('config') for v in ('valid', 'invalid-config', 'invalid-cert')})


def write(path, text, mode=0o755):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    path.chmod(mode)


def build(base, faults, loopback):
    """Lay out the sandbox under base; returns (root, fake, poison)."""
    root, fake, poison = base / 'repo/test/simcluster', base / 'fake', base / 'poison'
    for relative in LINKS:
        (root / relative).parent.mkdir(parents=True, exist_ok=True)
        (root / relative).symlink_to(SIM / relative)
    for name in LIBRARY:
        # Copies, not links: the library resolves its own directory, so its
        # S7/S5 imports reach the stand-ins below. The bytes stay the real ones.
        shutil.copy2(SIM / 'drills/lib' / name, root / 'drills/lib' / name)
        assert sha(root / 'drills/lib' / name) == sha(SIM / 'drills/lib' / name), name
    write(root / 'drills/lib/transfer_fixture.py', TRANSFER_STUB, 0o644)
    write(root / 'drills/lib/hot_update_flow_probe.py', FLOW_STUB, 0o644)
    write(fake / 'common.sh', COMMON, 0o644)
    write(fake / 'tool', TOOL_DOUBLE)
    write(fake / 'docker', DOCKER_DOUBLE)
    for name in POISONED:
        write(poison / name, POISON)
    write(root / 'simcluster', SIM_DOUBLE)
    write(root / 'lib/upgrade-artifacts.sh', UPGRADE_ARTIFACTS_DOUBLE, 0o644)
    write(root / 'lib/infra-artifact.py', VERIFIER_DOUBLE.replace('@NAME@', 'artifact'), 0o644)
    write(root / 'lib/infra-image-manifest.py', VERIFIER_DOUBLE.replace('@NAME@', 'image'), 0o644)
    for binary, old, new in VERSIONS.values():
        for alias, version in (('prev', old), ('next', new)):
            write(root / 'vendor' / (binary + '-' + alias), 'hermetic %s %s\n' % (binary, version), 0o644)
            write(root / 'vendor/infra' / (binary + '-' + version), 'hermetic %s %s\n' % (binary, version), 0o644)
    for generation in ('previous', 'current'):
        write(root / 'vendor/infra' / ('tether-infra-helper-' + generation), 'hermetic helper ' + generation + '\n', 0o644)
    write(root / 'vendor/tether-prev', 'hermetic tether-prev v0.6.0\n', 0o644)
    rules = [value for kind, value in faults if kind == 'rule']
    (fake / 'rules').write_text(''.join('\t'.join(r) + '\n' for r in rules))
    (fake / 'knobs.json').write_text(json.dumps(dict(value for kind, value in faults if kind == 'knob')))
    if ('nobinding', None) not in faults:
        (root / 'vendor/infra-fixture.json').write_text(json.dumps(binding(root, loopback)))
    for directory in (root / 'evidence', base / 'home', base / 'tmp'):
        directory.mkdir(parents=True)
    return root, fake, poison
