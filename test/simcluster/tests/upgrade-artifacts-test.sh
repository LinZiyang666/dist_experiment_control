#!/bin/sh
# origin: hot-update S8 plan §7.3 / §11.2 P22 / S8.P22
# Real tiny Go programs are supplier mechanism fixtures. They certify archive,
# cache and process identity predicates only, never S8 decoders or continuity.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
[ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] || {
    printf 'artifact tests: shared process supervisor is required\n' >&2; exit 2;
}
exec "$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B - "$HERE" "${1:-all}" <<'PYTEST'
import importlib.util, json, os, pathlib, shutil, subprocess, sys, tempfile, time, unittest

HERE = pathlib.Path(sys.argv[1])
LIB = HERE/'lib/upgrade-artifacts.sh'
source = LIB.read_text().split("<<'PYARTIFACTS'\n", 1)[1].split('\nPYARTIFACTS', 1)[0]
module = importlib.util.module_from_spec(importlib.util.spec_from_loader('artifact_supplier', loader=None))
exec(compile(source, str(LIB), 'exec'), module.__dict__)


def command(argv, **kwargs):
    return subprocess.run(argv, check=True, capture_output=True, **kwargs)


def add_commit(repo, files, tag, parent=None):
    # fast-import writes scratch object graphs without committing/checking out
    # the engineer's worktree. Every source ref is peeled and actually archived.
    stream = b'commit refs/heads/main\ncommitter Test <test@example.invalid> 1 +0000\ndata 4\nmsg\n'
    if parent:
        stream += b'from '+parent.encode()+b'\n'
    for path, content in files.items():
        content = content.encode()
        stream += b'M 100644 inline '+path.encode()+b'\ndata '+str(len(content)).encode()+b'\n'+content
    stream += b'\n'
    stream += b'reset refs/tags/'+tag.encode()+b'\nfrom refs/heads/main\n\n'
    command(['git', '-C', str(repo), 'fast-import', '--quiet', '--force'], input=stream)
    return module.git(repo, 'rev-parse', tag).decode().strip()


class ArtifactSupplyControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix='s8-artifact-controls-')
        cls.root = pathlib.Path(cls.temporary.name)
        cls.repo, cls.vendor = cls.root/'repo', cls.root/'vendor'
        cls.repo.mkdir()
        cls.vendor.mkdir()
        command(['git', '-C', str(cls.repo), 'init', '-q'])
        main = '''package main
import ("fmt"; "os"; "time")
func main() {
 if len(os.Args)>1 && os.Args[1]=="hold" { for { time.Sleep(time.Second) } }
 fmt.Println("supplier mechanism fixture A")
 fmt.Println("HOME="+os.Getenv("HOME"))
}
'''
        cls.a = add_commit(cls.repo, {'go.mod': 'module github.com/LinZiyang666/tether\n\ngo 1.26.0\n',
                                     'cmd/tether/main.go': main,
                                     'internal/proto/version.go': 'package proto\nvar ReleaseVersion="fixture"\n'}, 'v0.5.0')
        cls.b = add_commit(cls.repo, {'cmd/tether/main.go': main.replace('fixture A', 'fixture B')}, 'v0.6.0', cls.a)
        cls.label = add_commit(cls.repo, {'internal/proto/version.go': 'package proto\nvar ReleaseVersion="new-label"\n'}, 'v0.7.0', cls.b)
        cls.doc = module.stage(cls.repo, cls.vendor, 'v0.5.0', 'fixture')
        cls.expected, _ = module.expected_source(cls.repo, 'v0.5.0', 'fixture')
        cls.expected['build_package'] = './cmd/tether'

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def test_ArtifactCacheRequiresCompleteBuildProvenance(self):
        binary, receipt = self.vendor/'fixture', self.vendor/'fixture-source.json'
        good = receipt.read_bytes()
        self.assertTrue(module.cache_ok(binary, receipt, self.expected))
        # Missing every required field individually must turn a real warm cache
        # invalid, including fields the old tag/commit/SHA-only checker ignored.
        try:
            for field in self.doc:
                bad = dict(self.doc)
                del bad[field]
                module.publish(receipt, bad)
                self.assertFalse(module.cache_ok(binary, receipt, self.expected), 'accepted missing '+field)
            for field in ('tree_digest', 'archive_sha256', 'toolchain', 'goflags', 'dirty_diff_sha256', 'build_flags', 'commit', 'tag'):
                bad = dict(self.doc)
                bad[field] = 'forged'
                module.publish(receipt, bad)
                self.assertFalse(module.cache_ok(binary, receipt, self.expected), 'accepted forged '+field)
            bad = dict(self.doc)
            bad['schema_version'] = 2.0
            module.publish(receipt, bad)
            self.assertFalse(module.cache_ok(binary, receipt, self.expected), 'accepted noninteger schema')
            receipt.write_text('{"schema_version":1,"schema_version":2}')
            self.assertFalse(module.cache_ok(binary, receipt, self.expected))
        finally:
            receipt.write_bytes(good)
        self.assertTrue(module.cache_ok(binary, receipt, self.expected))

    def test_ArtifactCacheRejectsSwappedBytesAndPermissions(self):
        binary = self.vendor/'fixture'
        good = binary.read_bytes()
        try:
            binary.chmod(0o644)
            self.assertFalse(module.cache_ok(binary, self.vendor/'fixture-source.json', self.expected))
            binary.chmod(0o755)
            binary.write_bytes(good+b'forged')
            self.assertFalse(module.cache_ok(binary, self.vendor/'fixture-source.json', self.expected))
        finally:
            binary.write_bytes(good)
            binary.chmod(0o755)
        self.assertTrue(module.cache_ok(binary, self.vendor/'fixture-source.json', self.expected))

    def test_ArtifactRebuildRepairsIncompleteCache(self):
        name = 'rebuild-fixture'
        doc = module.stage(self.repo, self.vendor, 'v0.5.0', name)
        receipt = self.vendor/(name+'-source.json')
        del doc['toolchain']
        module.publish(receipt, doc)
        repaired = module.stage(self.repo, self.vendor, 'v0.5.0', name)
        self.assertIn('toolchain', repaired)
        self.assertEqual(repaired['toolchain'], module.build_info(self.vendor/name)['toolchain'])
        # A subsequent warm-cache hit does not rewrite its build timestamp.
        self.assertEqual(module.stage(self.repo, self.vendor, 'v0.5.0', name), repaired)

    def test_ArtifactOverlayBuildsChangedSourceAndInvalidatesCache(self):
        replacement, overlay = self.root/'replacement.go', self.root/'overlay.json'
        original = module.git(self.repo, 'show', 'v0.5.0:cmd/tether/main.go').decode()
        replacement.write_text(original.replace('fixture A', 'fixture OVERLAY'))
        module.publish(overlay, {'Replace': {str(self.repo/'cmd/tether/main.go'): str(replacement)}})
        previous = os.environ.get('GOFLAGS')
        os.environ['GOFLAGS'] = '-overlay='+str(overlay)
        try:
            doc = module.stage(self.repo, self.vendor, 'v0.5.0', 'overlay-fixture')
            self.assertNotEqual(doc['dirty_diff_sha256'], module.EMPTY_DIFF)
            self.assertIn(b'fixture OVERLAY', command([str(self.vendor/'overlay-fixture')]).stdout)
            replacement.write_text(original.replace('fixture A', 'fixture CHANGED'))
            rebuilt = module.stage(self.repo, self.vendor, 'v0.5.0', 'overlay-fixture')
            self.assertNotEqual(rebuilt['dirty_diff_sha256'], doc['dirty_diff_sha256'])
            self.assertNotEqual(rebuilt['binary_sha256'], doc['binary_sha256'])
            self.assertIn(b'fixture CHANGED', command([str(self.vendor/'overlay-fixture')]).stdout)
        finally:
            if previous is None:
                os.environ.pop('GOFLAGS', None)
            else:
                os.environ['GOFLAGS'] = previous

    def test_ArtifactFailureInvalidatesBothStaleFiles(self):
        for ref, pin in [('missing-release', ''), ('v0.5.0', module.FIXED_COMMIT)]:
            name = 'failure-fixture'
            binary, receipt = self.vendor/name, self.vendor/(name+'-source.json')
            binary.write_text('stale')
            receipt.write_text('{}')
            with self.assertRaises((ValueError, subprocess.CalledProcessError)):
                module.stage(self.repo, self.vendor, ref, name, pin)
            self.assertFalse(binary.exists())
            self.assertFalse(receipt.exists())
        bad = add_commit(self.repo, {'cmd/tether/main.go': 'syntax error\n'}, 'v0.8.0', self.label)
        with self.assertRaises(subprocess.CalledProcessError):
            module.stage(self.repo, self.vendor, bad, 'compile-failure')
        self.assertFalse((self.vendor/'compile-failure').exists())
        self.assertFalse((self.vendor/'compile-failure-source.json').exists())
        self.assertEqual(module.git(self.repo, 'worktree', 'list', '--porcelain').count(b'worktree '), 1)

    def test_UpgradePairRequiresImplementationChange(self):
        for a, b, reason in [('v0.5.0', 'v0.5.0', 'distinct source commits'), ('v0.6.0', 'v0.7.0', 'no implementation change')]:
            (self.vendor/'upgrade-pair.json').write_text('stale')
            with self.assertRaisesRegex(ValueError, reason):
                module.pair(self.repo, self.vendor, a, b)
            self.assertFalse((self.vendor/'upgrade-pair.json').exists())
        inactive = add_commit(self.repo, {'cmd/tether/variant_darwin.go': 'package main\nconst variant="inactive"\n'}, 'v0.7.1', self.b)
        with self.assertRaisesRegex(ValueError, 'compiled implementation is identical'):
            module.pair(self.repo, self.vendor, 'v0.6.0', inactive)
        self.assertFalse((self.vendor/'upgrade-pair.json').exists())
        # Distinct real code alone is insufficient to announce S8 capability.
        with self.assertRaisesRegex(ValueError, 'decoder advertisement incomplete'):
            module.pair(self.repo, self.vendor, 'v0.5.0', 'v0.6.0')
        self.assertFalse((self.vendor/'upgrade-pair.json').exists())

    def test_PreviousArtifactRequiresObservedExecution(self):
        for round in range(5):
            self.exercise_previous_execution(round)

    def exercise_previous_execution(self, round):
        copied = self.root/('copied-'+str(round))
        shutil.copyfile(self.vendor/'fixture', copied)
        copied.chmod(0o755)
        receipt = self.root/('execution-'+str(round)+'.json')
        # Staged-only source evidence cannot substitute for actual execution.
        module.publish(receipt, self.doc)
        with self.assertRaises((KeyError, ValueError)):
            module.execution_ok(self.repo, self.vendor, 'fixture', 'v0.5.0', copied, receipt)
        argv = [str(copied), 'hold']
        child = subprocess.Popen(argv)
        try:
            doc = module.observe(self.repo, self.vendor, 'fixture', 'v0.5.0', child.pid, copied, argv, receipt)
            self.assertGreater(doc['start_ticks'], 0)
            self.assertEqual(doc['running_sha256'], self.doc['binary_sha256'])
            self.assertEqual(module.execution_ok(self.repo, self.vendor, 'fixture', 'v0.5.0', copied, receipt), doc)
            with self.assertRaisesRegex(ValueError, 'argv differs'):
                module.observe(self.repo, self.vendor, 'fixture', 'v0.5.0', child.pid, copied, [str(copied), 'fake'], receipt)
            self.assertFalse(receipt.exists())
            # Identical bytes at a second inode, with a forged argv[0], still
            # cannot vouch for execution of the required copied path.
            other = subprocess.Popen(argv, executable=str(self.vendor/'fixture'))
            try:
                with self.assertRaisesRegex(ValueError, 'running executable differs'):
                    module.observe(self.repo, self.vendor, 'fixture', 'v0.5.0', other.pid, copied, argv, receipt)
                self.assertFalse(receipt.exists())
            finally:
                other.kill()
                other.wait()
            # A different live Go executable must not vouch for the old bytes.
            with self.assertRaises(ValueError):
                module.observe(self.repo, self.vendor, 'fixture', 'v0.5.0', os.getppid(), copied, argv, receipt)
            self.assertFalse(receipt.exists())
            with self.assertRaises((ValueError, FileNotFoundError)):
                module.observe(self.repo, self.vendor, 'fixture', 'v0.5.0', 2147483647, copied, argv, receipt)
            self.assertFalse(receipt.exists())
        finally:
            child.kill()
            child.wait()
        self.assertFalse(pathlib.Path('/proc/'+str(child.pid)).exists())

    def test_ArtifactVersionProbeUsesSandboxHome(self):
        for round in range(5):
            output = module.version_probe(self.vendor/'fixture')
            home = next(line[5:] for line in output.splitlines() if line.startswith('HOME='))
            self.assertTrue(home.startswith(tempfile.gettempdir()+'/s8-version-probe-'))
            self.assertNotEqual(home, os.environ.get('HOME'))
            self.assertFalse(pathlib.Path(home).exists())

    def test_ArtifactSupplierRequiresSharedSupervisor(self):
        env = dict(os.environ, SIM_REPO=str(self.repo), SIM_VENDOR=str(self.vendor))
        env.pop('SIM_UPGRADE_SUPERVISOR', None)
        result = subprocess.run(['sh', '-c', '. "$1"; sim_upgrade_artifacts verify v0.5.0 fixture', 'supervisor-control', str(LIB)], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn('shared process supervisor is required', result.stderr)
        # Sourcing libraries is inert: no child, archive or artifact publication
        # occurs until the caller invokes an explicitly supervised operation.
        untouched = self.root/'source-inert'
        untouched.mkdir()
        command(['sh', '-c', '. "$1"; . "$2"', 'source-control', str(LIB), str(HERE/'lib/stage.sh')], cwd=untouched, env=env)
        self.assertEqual(list(untouched.iterdir()), [])

    def test_WireProbeRequiresActualOwnedSource(self):
        with self.assertRaisesRegex(ValueError, 'P24 wire probe source is missing'):
            module.stage(self.repo, self.vendor, 'v0.5.0', 'upgrade-wire-probe-old', probe=self.root/'missing.go')
        self.assertFalse((self.vendor/'upgrade-wire-probe-old').exists())

    def test_WireManifestPreservesEightQuadrants(self):
        # This is the actual integration test, ready to run against real old/new
        # schemas when P24 hands back its probe. No fake probe substitutes for it.
        real_repo = HERE.parent.parent
        probe = real_repo/'test/hotupdate/wireprobe/main.go'
        if not probe.is_file():
            self.skipTest('NOT_RUN pending P24: test/hotupdate/wireprobe/main.go')
        old = module.stage(real_repo, self.vendor, module.FIXED_TAG, 'tether-prev', module.FIXED_COMMIT)
        new = module.stage(real_repo, self.vendor, 'HEAD', 'tether-wire-new')
        for ref, label in [(module.FIXED_TAG, 'old'), ('HEAD', 'new')]:
            module.stage(real_repo, self.vendor, ref, 'upgrade-wire-probe-'+label, probe=probe)
        fixture = {}
        for b in '01':
            for q in ('00', '01', '10', '11'):
                cell = 'Q'+q+'B'+b
                ap = '/opt/s8/'+('old' if q[1] == '0' else 'new')+'/tether'
                bp = '/opt/s8/'+('old' if b == '0' else 'new')+'/tether'
                fixture[cell] = dict(values=dict(HOME_CELL='/tmp/s8/'+cell+'/home', NATS_URL='nats://127.0.0.1:4222', NID='node-'+cell, PID='101', NONCE=('%032x'%len(fixture))),
                                     agent_argv=[ap, 'agent', '--session', 's8-fixture', '--nid', 'node-'+cell, '--nats-url', 'nats://127.0.0.1:4222'],
                                     broker_argv=[bp, 'serve', '--config', '/tmp/s8/'+cell+'/broker.yaml'])
        fixture_path, out = self.root/'wire-fixture.json', self.root/'wire-manifest.json'
        module.publish(fixture_path, fixture)
        rows = module.wire_manifest(real_repo, self.vendor, 'HEAD', fixture_path, out)
        self.assertEqual(len(rows), 40)
        self.assertEqual(len({(r['cell'], r['verb']) for r in rows}), 40)
        fields = {'cell', 'verb', 'argv', 'ctl_path', 'ctl_sha', 'agent_path', 'agent_sha', 'broker_path', 'broker_sha', 'probe_path', 'probe_sha', 'source_commit', 'expected_effect'}
        for r in rows:
            self.assertEqual(set(r), fields)
            for role, index in [('ctl', 1), ('agent', 2), ('broker', 4)]:
                bit = r['cell'][index]
                self.assertEqual(r[role+'_path'], '/opt/s8/'+('old' if bit == '0' else 'new')+'/tether')
                self.assertEqual(r[role+'_sha'], (old if bit == '0' else new)['binary_sha256'])
            argv = r['argv']
            self.assertNotIn('transfer', argv)
            if r['verb'] == 'kill':
                self.assertEqual(argv[0], r['probe_path'])
                self.assertIn('--signal', argv)
                self.assertEqual(argv[-1], '15')
            else:
                self.assertEqual(argv[0], r['ctl_path'])
                self.assertIn('--home', argv)
                self.assertIn('--nats-url', argv)
        companions = module.read_json(out)['companions']
        for cell, row in companions.items():
            self.assertEqual(row['agent_argv'], fixture[cell]['agent_argv'])
            self.assertEqual(row['broker_argv'], fixture[cell]['broker_argv'])
            self.assertIn('pull', row['transfer_pull'])
            self.assertIn('rm', row['expose_cleanup'])
        fixture.pop('Q00B0')
        module.publish(fixture_path, fixture)
        with self.assertRaisesRegex(ValueError, 'all eight cells'):
            module.wire_manifest(real_repo, self.vendor, 'HEAD', fixture_path, out)
        self.assertFalse(out.exists())


selection = sys.argv[2]
suite = unittest.defaultTestLoader.loadTestsFromTestCase(ArtifactSupplyControls)
if selection != 'all':
    suite = unittest.TestSuite(t for t in suite if t._testMethodName == selection)
    if suite.countTestCases() == 0:
        raise ValueError('unknown artifact test selection')
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() else 1)
PYTEST
