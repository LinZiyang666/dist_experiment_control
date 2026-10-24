#!/bin/sh
# Hermetic supply failures and release selection use scratch Git objects and
# tiny Go programs only. No release executable or deployment driver is run.
# origin: S8 internal review rv-n1 F3/F4/F6
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
[ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] || {
    printf 'artifact supply tests: shared process supervisor is required\n' >&2; exit 2;
}
exec "$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B - "$HERE" "${1:-all}" <<'PYTEST'
import importlib.util, json, os, pathlib, subprocess, sys, tempfile, unittest
from unittest import mock

HERE = pathlib.Path(sys.argv[1])
LIB = HERE/'lib/upgrade-artifacts.sh'
source = LIB.read_text().split("<<'PYARTIFACTS'\n", 1)[1].split('\nPYARTIFACTS', 1)[0]
supplier = importlib.util.module_from_spec(importlib.util.spec_from_loader('supplier', loader=None))
exec(compile(source, str(LIB), 'exec'), supplier.__dict__)


def command(argv, **kwargs):
    return subprocess.run(argv, check=True, capture_output=True, **kwargs)


def commit(repo, tag, parent='', content='package main\nfunc main() {}\n'):
    # Import objects in an isolated repository; never commit/check out a tree.
    stream = b'commit refs/heads/main\ncommitter Test <test@example.invalid> 1 +0000\ndata 4\nmsg\n'
    if parent:
        stream += b'from '+parent.encode()+b'\n'
    for name, body in {'go.mod': 'module github.com/LinZiyang666/tether\n\ngo 1.26.0\n',
                       'cmd/tether/main.go': content}.items():
        raw = body.encode()
        stream += b'M 100644 inline '+name.encode()+b'\ndata '+str(len(raw)).encode()+b'\n'+raw
    stream += b'\n'
    if tag:
        stream += b'reset refs/tags/'+tag.encode()+b'\nfrom refs/heads/main\n\n'
    command(['git', '-C', str(repo), 'fast-import', '--quiet'], input=stream)
    command(['git', '-C', str(repo), 'symbolic-ref', 'HEAD', 'refs/heads/main'])
    command(['git', '-C', str(repo), 'read-tree', '--reset', '-u', 'HEAD'])
    return supplier.git(repo, 'rev-parse', 'HEAD').decode().strip()


class ArtifactSupplyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='artifact-supply-')
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.repo, self.vendor = self.root/'repo', self.root/'vendor'
        self.repo.mkdir()
        self.vendor.mkdir()
        command(['git', '-C', str(self.repo), 'init', '-q'])
        self.fixed = commit(self.repo, 'v0.6.0')

    def shell(self, script, *args):
        return subprocess.run(['sh', '-c', script, 'supply-control', str(HERE), str(self.repo),
                               str(self.vendor), *args], capture_output=True, text=True)

    def stage_fixture(self, failure):
        files = {'.gitignore': 'bin/\nvendor/\n', 'test/simcluster/lib/stage-caddy.sh': 'sim_stage_caddy() { [ "$FAIL" != caddy ]; }\n',
                 'test/simcluster/lib/stage-infra.sh': 'sim_verify_infra() { [ "$FAIL" != infra ]; }\n',
                 'bin/tether': 'stale candidate from an earlier build\n',
                 'scripts/install.sh': 'inert installer fixture\n',
                 'vendor/nats-server': "#!/bin/sh\nprintf 'nats-server: v2.14.0\\n'\n",
                 'vendor/nats': 'inert cached CLI\n', 'vendor/nk': 'inert cached CLI\n'}
        for name, body in files.items():
            path = self.repo/name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body)
            path.chmod(0o700)
        vendor = self.repo/'vendor'
        (vendor/'infra/fullstack').mkdir(parents=True, exist_ok=True)
        for name in ('tether-source.json', 'infra/fullstack/catalog.json'):
            (vendor/name).write_text('stale success receipt')
        if failure in ('curl', 'tar', 'partial-tar', 'nats-copy', 'partial-nats-copy', 'nats-version'):
            (vendor/'nats-server').unlink()
        if failure in ('nats-install', 'partial-nats-install'):
            (vendor/'nats').unlink()
        if failure in ('nk-install', 'partial-nk-install'):
            (vendor/'nk').unlink()
        script = r'''
. "$1/lib/stage.sh"
SIM_REPO=$2 SIM_VENDOR=$2/vendor SIM_TAG=control FAIL=$4
sim_nats_version() { printf 'v2.14.0\n'; }
make() {
    [ "$FAIL" != build ] || return 42
    out="$SIM_REPO/bin/tether"
    for arg do case "$arg" in BIN=*) out=${arg#BIN=} ;; VERSION=*) version=${arg#VERSION=} ;; esac; done
    if [ "$FAIL" = next-build ] && [ "$version" = v0.0.0-simcluster-next ]; then return 42; fi
    [ "$FAIL" != missing-output ] || return 0
    if [ "$FAIL" = missing-current-output ] && [ "$version" = v0.0.0-simcluster ]; then return 0; fi
    if [ "$FAIL" = changed-source ]; then printf changed >> "$SIM_REPO/cmd/tether/main.go"; fi
    [ "$FAIL" != same-output ] || version=same-output
    printf '%s\n' "$version" > "$out" || return 1
    chmod +x "$out" || return 1
    case "$FAIL:$version" in partial-build:v0.0.0-simcluster|partial-next-build:v0.0.0-simcluster-next) return 42 ;; esac
}
cp() {
    case "$FAIL:$2" in partial-copy:*/tether|partial-next-copy:*/tether-next|partial-nats-copy:*/nats-server) command cp "$@"; return 43 ;; esac
    case "$FAIL:$2" in copy:*/tether|next-copy:*/tether-next|installer-copy:*/install.sh|nats-copy:*/nats-server) return 43 ;; esac
    command cp "$@"
}
curl() { [ "$FAIL" != curl ]; }
tar() {
    [ "$FAIL" != tar ] || return 44
    mkdir -p "$4/nats-server-v2.14.0-linux-amd64" || return 1
    if [ "$FAIL" = nats-version ]; then
        printf '#!/bin/sh\nprintf "nats-server: v2.14.0\\n"\nexit 45\n' > "$4/nats-server-v2.14.0-linux-amd64/nats-server"
    else
        printf '#!/bin/sh\nprintf "nats-server: v2.14.0\\n"\n' > "$4/nats-server-v2.14.0-linux-amd64/nats-server"
    fi
    chmod +x "$4/nats-server-v2.14.0-linux-amd64/nats-server" || return 1
    [ "$FAIL" != partial-tar ]
}
go() {
    case "$FAIL:$*" in
        nats-install:*natscli*|nk-install:*nkeys*) return 46 ;;
        partial-nats-install:*natscli*) printf partial > "$GOBIN/nats"; chmod +x "$GOBIN/nats"; return 46 ;;
        partial-nk-install:*nkeys*) printf partial > "$GOBIN/nk"; chmod +x "$GOBIN/nk"; return 46 ;;
    esac
    command go "$@"
}
sim_stage_prev() { [ "$FAIL" != previous ]; }
sim_stage_dynamic_prev() { [ "$FAIL" != dynamic ]; }
sim_stage_flow_legacy() { [ "$FAIL" != legacy ]; }
sim_write_tether_source() {
    printf success > "$SIM_VENDOR/tether-source.json"
    [ "$FAIL" != receipt ]
}
sim_stage_fullstack() {
    printf success > "$SIM_VENDOR/infra/fullstack/catalog.json"
    case "$FAIL" in swapped-current) printf swapped > "$SIM_VENDOR/tether" ;; swapped-next) printf swapped > "$SIM_VENDOR/tether-next" ;; esac
    [ "$FAIL" != fullstack ]
}
sim_stage_binaries_supervised
'''
        return vendor, self.shell(script, failure)

    # origin: S8 internal review rv-n1 F3
    def test_StageFailureInvalidatesSuccessReceipts(self):
        for failure in ('build', 'next-build', 'partial-build', 'partial-next-build', 'copy', 'next-copy',
                        'partial-copy', 'partial-next-copy', 'missing-output', 'missing-current-output', 'same-output', 'changed-source', 'swapped-current', 'swapped-next',
                        'curl', 'tar', 'partial-tar', 'nats-copy', 'partial-nats-copy', 'nats-version',
                        'nats-install', 'partial-nats-install', 'nk-install', 'partial-nk-install',
                        'installer-copy', 'caddy', 'previous', 'dynamic', 'legacy', 'infra', 'receipt', 'fullstack'):
            with self.subTest(failure=failure):
                vendor, result = self.stage_fixture(failure)
                self.assertNotEqual(result.returncode, 0, failure+' returned success: '+result.stdout+result.stderr)
                self.assertFalse((vendor/'tether-source.json').exists(), failure+' left source success')
                self.assertFalse((vendor/'infra/fullstack/catalog.json').exists(), failure+' left catalog success')

    # origin: S8 internal review rv-n1 F3
    def test_StagePublishesOnlyFreshCurrentAndNext(self):
        vendor, result = self.stage_fixture('none')
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assertEqual((vendor/'tether').read_text(), 'v0.0.0-simcluster\n')
        self.assertEqual((vendor/'tether-next').read_text(), 'v0.0.0-simcluster-next\n')
        self.assertTrue((vendor/'tether-source.json').exists())
        self.assertTrue((vendor/'infra/fullstack/catalog.json').exists())

    # origin: S8 internal review rv-n1 F4
    def test_HistoricalProductsIgnoreOuterSourceOverlay(self):
        replacement, overlay = self.root/'replacement.go', self.root/'overlay.json'
        replacement.write_text('invalid overlaid production source\n')
        supplier.publish(overlay, {'Replace': {str(self.repo/'cmd/tether/main.go'): str(replacement)}})
        for name in ('tether-prev', 'tether-dynamic-prev', 'tether-flow-legacy', 'upgrade-wire-probe-old'):
            with self.subTest(component=name):
                probe = None
                if name == 'upgrade-wire-probe-old':
                    probe = self.root/'probe.go'
                    probe.write_text('package main\nfunc main() {}\n')
                clean = supplier.stage(self.repo, self.vendor, 'v0.6.0', name, probe=probe)
                # Force an actual archive rebuild, not a warm-cache identity-only check.
                (self.vendor/name).unlink()
                with mock.patch.dict(os.environ, GOFLAGS='-overlay='+str(overlay)):
                    overlaid = supplier.stage(self.repo, self.vendor, 'v0.6.0', name, probe=probe)
                    self.assertEqual(overlaid['dirty_diff_sha256'], supplier.EMPTY_DIFF)
                    self.assertNotIn('-overlay', overlaid['goflags'])
                    self.assertEqual(overlaid['binary_sha256'], clean['binary_sha256'])

    # origin: S8 internal review rv-n1 F4
    def test_WireManifestBindsProbeSupply(self):
        commit(self.repo, 'v0.7.0', self.fixed, 'package main\nfunc main() { println("newer") }\n')
        probe = self.repo/'test/hotupdate/wireprobe/main.go'
        probe.parent.mkdir(parents=True)
        probe.write_text('package main\nfunc main() {}\n')
        probes = {}
        for ref, label, component in [('v0.6.0', 'old', 'tether-prev'), ('v0.7.0', 'new', 'tether-wire-new')]:
            supplier.stage(self.repo, self.vendor, ref, component)
            name = 'upgrade-wire-probe-'+label
            probes[name] = supplier.stage(self.repo, self.vendor, ref, name, probe=probe)
        fixture = {}
        for b in '01':
            for q in ('00', '01', '10', '11'):
                cell = 'Q'+q+'B'+b
                agent = '/opt/s8/'+('old' if q[1] == '0' else 'new')+'/tether'
                broker = '/opt/s8/'+('old' if b == '0' else 'new')+'/tether'
                fixture[cell] = dict(values=dict(HOME_CELL='/tmp/'+cell, NATS_URL='nats://127.0.0.1:4222',
                                                 NID='node', PID='100', NONCE='a'*32),
                                     agent_argv=[agent, 'agent', '--session', 'lab', '--nid', 'node', '--nats-url', 'nats://127.0.0.1:4222'],
                                     broker_argv=[broker, 'serve', '--config', '/tmp/broker.yaml'])
        source, out = self.root/'fixture.json', self.root/'wire-supply.json'
        supplier.publish(source, fixture)
        # This scratch pin certifies only the supplier mechanism, never v0.6.0.
        with mock.patch.object(supplier, 'FIXED_COMMIT', self.fixed):
            supplier.wire_manifest(self.repo, self.vendor, 'v0.7.0', source, out)
        doc = supplier.read_json(out)
        self.assertIn('probes', doc)
        self.assertEqual(doc['probes'], probes)
        self.assertEqual(len(doc['rows']), 40)
        self.assertEqual(doc['execution'], 'NOT_RUN')

    # origin: S8 internal review rv-n1 F6
    def test_PreviousSelectionKeepsFixedAndDynamicArtifactsTogether(self):
        newer = commit(self.repo, 'v0.7.0', self.fixed, 'package main\nfunc main() { println("newer") }\n')
        commit(self.repo, '', newer, 'package main\nfunc main() { println("development") }\n')
        supplier.stage(self.repo, self.vendor, 'v0.6.0', 'tether-prev')
        supplier.stage(self.repo, self.vendor, 'v0.7.0', 'tether-dynamic-prev')
        script = r'''
. "$1/lib/stage.sh"
SIM_REPO=$2 SIM_VENDOR=$3 SIM_STAGE_LIB=$1/lib
[ "$SIM_FLOW_LEGACY_TAG" = v0.6.0 ] || exit 10
[ "$SIM_FLOW_LEGACY_COMMIT" = a3431a12be3cf947cc797c28fea93382fa68472d ] || exit 11
# Scratch commits prove selection/caching, not the production historical pin.
SIM_FLOW_LEGACY_COMMIT=$4
sim_select_previous fixed || exit 12
[ "$SIM_PREVIOUS_TAG" = v0.6.0 ] && [ "$SIM_PREVIOUS_BINARY" = "$SIM_VENDOR/tether-prev" ] &&
    [ "$SIM_PREVIOUS_SOURCE" = "$SIM_VENDOR/tether-prev-source.json" ] || exit 13
sim_prev_source_ok "$SIM_PREVIOUS_TAG" || exit 14
sim_select_previous dynamic || exit 15
[ "$SIM_PREVIOUS_TAG" = v0.7.0 ] && [ "$SIM_PREVIOUS_BINARY" = "$SIM_VENDOR/tether-dynamic-prev" ] &&
    [ "$SIM_PREVIOUS_SOURCE" = "$SIM_VENDOR/tether-dynamic-prev-source.json" ] || exit 16
sim_dynamic_prev_source_ok "$SIM_PREVIOUS_TAG" || exit 17
if sim_prev_source_ok "$SIM_PREVIOUS_TAG"; then exit 18; fi
if sim_dynamic_prev_source_ok v0.6.0; then exit 19; fi
if sim_select_previous unknown; then exit 20; fi
[ -z "$SIM_PREVIOUS_TAG$SIM_PREVIOUS_BINARY$SIM_PREVIOUS_SOURCE" ] || exit 21
'''
        for dirty in (False, True):
            if dirty:
                (self.repo/'cmd/tether/main.go').write_text('dirty worktree source\n')
            with self.subTest(dirty=dirty):
                result = self.shell(script, self.fixed)
                self.assertEqual(result.returncode, 0, result.stdout+result.stderr)


suite = unittest.defaultTestLoader.loadTestsFromTestCase(ArtifactSupplyTest)
if sys.argv[2] != 'all':
    suite = unittest.TestSuite(t for t in suite if t._testMethodName == sys.argv[2])
    if not suite.countTestCases():
        raise ValueError('unknown artifact supply test')
sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
PYTEST
