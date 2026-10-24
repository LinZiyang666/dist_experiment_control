# lib/upgrade-artifacts.sh — source archives, build identities and execution evidence.
# origin: hot-update S8 plan §6.1 / §7.3 / §11.2 P22
# Caller supplies SIM_REPO, SIM_VENDOR and SIM_UPGRADE_SUPERVISOR. The last is an
# executable adapter to the shared FIX-ORPHAN helper, accepting: -- argv... .
# Its inherited stdio/cwd/env and native exit status must be preserved. Refusing
# an absent adapter prevents an orphan-prone fallback during long Go builds.
sim_upgrade_artifacts() (
    [ -n "${SIM_UPGRADE_SUPERVISOR:-}" ] && [ -x "$SIM_UPGRADE_SUPERVISOR" ] || {
        printf 'artifact supplier: shared process supervisor is required\n' >&2; exit 2;
    }
    exec "$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B - "$SIM_REPO" "$SIM_VENDOR" "$@" <<'PYARTIFACTS'
import hashlib, io, json, os, pathlib, re, shlex, shutil, subprocess, sys, tarfile, tempfile
from datetime import datetime, timezone

FIXED_TAG = 'v0.6.0'
FIXED_COMMIT = 'a3431a12be3cf947cc797c28fea93382fa68472d'
EMPTY_DIFF = hashlib.sha256(b'').hexdigest()
SCHEMA = 2


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def run(argv, cwd=None, env=None):
    # The enclosing shell entered FIX-ORPHAN before this interpreter started;
    # compilers and all detached descendants stay in that supervisor's lineage.
    return subprocess.check_output(argv, cwd=cwd, env=env, stderr=sys.stderr)


def git(repo, *args):
    return run(['git', '-C', str(repo), *args])


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def file_sha(path):
    with open(path, 'rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate metadata field')
        result[key] = value
    return result


def read_json(path):
    return json.loads(pathlib.Path(path).read_text(), object_pairs_hook=unique)


def publish(path, record):
    path = pathlib.Path(path)
    # Publish the receipt last. A torn or missing receipt can only force rebuild.
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=path.name+'.')
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(record, f, sort_keys=True)
            f.write('\n')
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def identity(repo, ref):
    require(not ref.startswith('-'), 'invalid source reference')
    commit = git(repo, 'rev-parse', '--verify', ref+'^{commit}').decode().strip()
    require(re.fullmatch('[0-9a-f]{40}', commit), 'invalid source commit')
    tree = git(repo, 'rev-parse', commit+'^{tree}').decode().strip()
    listing = git(repo, 'ls-tree', '-rz', '--full-tree', commit)
    archive = git(repo, 'archive', '--format=tar', commit)
    return dict(commit=commit, tree=tree, tree_digest=sha(listing), archive_sha256=sha(archive)), archive


def flags(version):
    # Match the release Makefile's real build rather than merely relabeling bytes.
    return ['-trimpath', '-ldflags', '-s -w -X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion='+version]


# origin: S8 internal review rv-n1 F4
# Only candidate/mechanism builds inherit mutation overlays. Historical product
# and old-schema probe inputs must always come from the archived release.
def historical_source(name):
    return name in ('tether-prev', 'tether-dynamic-prev', 'tether-flow-legacy', 'upgrade-wire-probe-old')


def build_env(historical=False):
    result = dict(os.environ)
    toolchain = result.get('SIM_ARTIFACT_GOTOOLCHAIN', 'auto')
    require(toolchain == 'auto' or re.fullmatch(r'go[0-9]+\.[0-9]+\.[0-9]+(?:\+auto)?', toolchain), 'invalid requested toolchain')
    result.update(CGO_ENABLED='0', GOOS='linux', GOARCH='amd64', GOTOOLCHAIN=toolchain, GOWORK='off')
    # Preserve nested-build overlays. Adding buildvcs=false keeps an archive from
    # picking up an unrelated enclosing .git directory.
    goflags = result.get('GOFLAGS', '')
    if historical:
        tokens = shlex.split(goflags)
        require('-overlay' not in tokens, 'use one explicit -overlay=path')
        goflags = ' '.join(json.dumps(t) if any(c.isspace() for c in t) else t
                           for t in tokens if not t.startswith('-overlay='))
    result['GOFLAGS'] = (goflags+' -buildvcs=false').strip()
    return result


def overlay_inputs(repo):
    tokens = shlex.split(os.environ.get('GOFLAGS', ''))
    paths = [token[len('-overlay='):] for token in tokens if token.startswith('-overlay=')]
    require(len(paths) <= 1 and '-overlay' not in tokens, 'use one explicit -overlay=path')
    if not paths:
        return {}, EMPTY_DIFF
    overlay = pathlib.Path(paths[0])
    if not overlay.is_absolute():
        overlay = repo/overlay
    doc = read_json(overlay)
    require(set(doc) == {'Replace'} and type(doc['Replace']) is dict, 'invalid source overlay')
    replacements, digest = {}, hashlib.sha256(b's8-source-overlay-v1\0')
    for name, replacement in sorted(doc['Replace'].items()):
        require(type(name) is str and type(replacement) is str, 'invalid source overlay mapping')
        source = pathlib.Path(name)
        if not source.is_absolute():
            source = repo/source
        relative = source.resolve().relative_to(repo.resolve())
        target = pathlib.Path(replacement) if replacement else None
        if target is not None and not target.is_absolute():
            target = repo/target
        content = target.read_bytes() if target else b''
        for value in (str(relative).encode(), b'replace' if target else b'delete', content):
            digest.update(len(value).to_bytes(8, 'big'))
            digest.update(value)
        replacements[str(relative)] = str(target.resolve()) if target else ''
    return replacements, digest.hexdigest()


def rebase_overlay(repo, root, env, probe=None):
    replacements, _ = overlay_inputs(repo)
    if not replacements:
        return
    rebased = {}
    for relative, target in replacements.items():
        destination = root/relative
        if probe and (repo/relative).resolve() == probe.resolve():
            destination = root/'cmd/upgrade-wire-probe/main.go'
        rebased[str(destination)] = target
    path = root/'source-overlay.json'
    publish(path, {'Replace': rebased})
    tokens = [token for token in shlex.split(env['GOFLAGS']) if not token.startswith('-overlay=')]
    # Go's quoted flag splitter accepts this JSON-compatible quoting. Rebase
    # source paths too: preserving GOFLAGS alone silently misses archive builds.
    env['GOFLAGS'] = ' '.join(json.dumps(token) if any(c.isspace() for c in token) else token
                            for token in tokens+['-overlay='+str(path)])


def build_info(path):
    raw = run(['go', 'version', '-m', str(path)]).decode()
    lines = raw.splitlines()
    require(lines and re.search(r': go[0-9]+\.[0-9]+', lines[0]), 'missing actual build toolchain')
    settings = {}
    for line in lines[1:]:
        fields = line.strip().split('\t')
        if len(fields) == 2 and fields[0] == 'build' and '=' in fields[1]:
            key, value = fields[1].split('=', 1)
            # go version -m uses Go string quoting for settings containing
            # spaces; JSON decoding handles the emitted escaped linker string.
            settings[key] = json.loads(value) if value.startswith('"') else value
    return dict(toolchain=lines[0].rsplit(': ', 1)[1], settings=settings)


# Parse JSON rather than matching fragments: a forged tag in an unrelated field
# must never vouch for the binary actually executed by the broker unit.
def cache_ok(binary, receipt, expected):
    try:
        doc = read_json(receipt)
        require(type(doc['schema_version']) is int and doc['schema_version'] == SCHEMA, 'invalid cache schema')
        for key, value in expected.items():
            require(doc[key] == value, 'cache identity mismatch: '+key)
        require(os.access(binary, os.X_OK) and doc['binary_sha256'] == file_sha(binary), 'cache binary mismatch')
        actual = build_info(binary)
        require(doc['toolchain'] == actual['toolchain'], 'cache toolchain mismatch')
        require(doc['build_info'] == actual, 'cache buildinfo mismatch')
        require(actual['settings'].get('GOOS') == 'linux' and actual['settings'].get('GOARCH') == 'amd64'
                and actual['settings'].get('CGO_ENABLED') == '0', 'cache target mismatch')
        require(actual['settings'].get('-trimpath') == 'true', 'cache flags mismatch')
        # -trimpath omits linker flags from Go buildinfo. Their exact argv is
        # mandatory in expected metadata; embedded target/trimpath/toolchain
        # are checked independently rather than inventing a buildinfo field.
        require(doc['built_at'] and datetime.fromisoformat(doc['built_at']).tzinfo, 'missing build timestamp')
        require(doc['source_archive'] and file_sha(doc['source_archive']) == doc['archive_sha256'], 'missing source archive')
        return True
    except (OSError, KeyError, ValueError, TypeError, subprocess.CalledProcessError):
        return False


def expected_source(repo, ref, name, pin='', version=''):
    require(re.fullmatch('[a-zA-Z0-9_-]+', name), 'invalid artifact name')
    source, archive = identity(repo, ref)
    require(not pin or source['commit'] == pin, 'historical release commit differs from fixed pin')
    historical = historical_source(name)
    env = build_env(historical)
    _, dirty = ({}, EMPTY_DIFF) if historical else overlay_inputs(repo)
    # Explicit commits need usable release identities too. The label derives
    # from real source, while pair's compiled proof forbids linker-only variants.
    release = version or (ref if re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?', ref)
                          else 'v0.0.0-s8-'+source['commit'][:12])
    expected = dict(schema_version=SCHEMA, component=name, tag=ref, **source,
                    dirty_diff_sha256=dirty, goos='linux', goarch='amd64', version=release,
                    build_flags=flags(release), goflags=env['GOFLAGS'], gotoolchain=env['GOTOOLCHAIN'])
    return expected, archive


def stage(repo, vendor, ref, name, pin='', probe=None, version=''):
    vendor.mkdir(parents=True, exist_ok=True)
    binary, receipt = vendor/name, vendor/(name+'-source.json')
    # Invalidate before even resolving a ref: an unreachable/deleted pin must
    # never leave yesterday's executable available to an eager drill.
    try:
        expected, archive = expected_source(repo, ref, name, pin, version)
        if probe:
            require(probe.is_file(), 'P24 wire probe source is missing')
            expected.update(probe_source_sha256=file_sha(probe), build_package='./cmd/upgrade-wire-probe')
        else:
            expected['build_package'] = './cmd/tether'
        if cache_ok(binary, receipt, expected):
            return read_json(receipt)
        # Remove stale cache before an attempted rebuild; failure cannot leave an
        # older receipt beside an executable that the drill could accidentally use.
        binary.unlink(missing_ok=True)
        receipt.unlink(missing_ok=True)
        # Source archives remain as evidence independently of the throwaway build.
        archive_path = vendor/(name+'-'+expected['archive_sha256']+'.tar')
        archive_path.write_bytes(archive)
        with tempfile.TemporaryDirectory(prefix='s8-source-') as tmp:
            root = pathlib.Path(tmp)
            with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
                tar.extractall(root, filter='data')
            if probe:
                destination = root/'cmd/upgrade-wire-probe/main.go'
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(probe.read_bytes())
            env = build_env(historical_source(name))
            if not historical_source(name):
                rebase_overlay(repo, root, env, probe)
            actual_go = run(['go', 'env', 'GOVERSION'], cwd=root, env=env).decode().strip()
            run(['go', 'build', *expected['build_flags'], '-o', str(root/'artifact'), expected['build_package']], cwd=root, env=env)
            actual = build_info(root/'artifact')
            require(actual_go == actual['toolchain'], 'build env differs from embedded toolchain')
            shutil.copyfile(root/'artifact', binary)
            binary.chmod(0o755)
        doc = dict(expected, binary_sha256=file_sha(binary), toolchain=actual_go, build_info=actual,
                   source_archive=str(archive_path.resolve()), built_at=datetime.now(timezone.utc).isoformat())
        publish(receipt, doc)
        require(cache_ok(binary, receipt, expected), 'built artifact validation failed')
        return doc
    except Exception:
        binary.unlink(missing_ok=True)
        receipt.unlink(missing_ok=True)
        raise


def verify(repo, vendor, ref, name, pin=''):
    expected, _ = expected_source(repo, ref, name, pin)
    expected['build_package'] = './cmd/tether'
    require(cache_ok(vendor/name, vendor/(name+'-source.json'), expected), 'artifact cache is incomplete or stale')
    return read_json(vendor/(name+'-source.json'))


def implementation_digest(repo, commit):
    # Ignore version-only, docs-only and test-only commits; A/B must differ in
    # compiled responsibilities, independently of linker release strings.
    records = git(repo, 'ls-tree', '-r', commit, '--', 'cmd', 'internal').decode().splitlines()
    compiled = [line for line in records if line.endswith('.go') and not line.endswith('_test.go')
                and not line.endswith('/version.go')]
    require(compiled, 'source has no compiled implementation')
    return sha('\n'.join(compiled).encode())


def version_probe(binary):
    # Offline version dispatch still enters the real executable's bootstrap.
    # Give it its own home so no candidate can consult production user state.
    with tempfile.TemporaryDirectory(prefix='s8-version-probe-') as home:
        env = dict(os.environ)
        env.update(HOME=home, XDG_CONFIG_HOME=home+'/config', XDG_DATA_HOME=home+'/data')
        return run([str(binary), 'version'], env=env).decode()


def pair(repo, vendor, a_ref, b_ref):
    (vendor/'upgrade-pair.json').unlink(missing_ok=True)
    a_id, _ = identity(repo, a_ref)
    b_id, _ = identity(repo, b_ref)
    require(a_id['commit'] != b_id['commit'], 'S8 pair requires distinct source commits')
    a_impl = implementation_digest(repo, a_id['commit'])
    b_impl = implementation_digest(repo, b_id['commit'])
    require(a_impl != b_impl, 'S8 pair has no implementation change')
    a = stage(repo, vendor, a_ref, 'tether-s8-a')
    b = stage(repo, vendor, b_ref, 'tether-s8-b')
    require(a['binary_sha256'] != b['binary_sha256'], 'S8 pair has identical executable bytes')
    # An edit in an inactive GOOS/build-tag file must not pass merely because
    # different linker versions changed the primary artifacts. Build both with
    # one linker version and compare the actual emitted implementation bytes.
    proof_version = 'v0.0.0-s8-implementation-proof'
    pa = stage(repo, vendor, a_ref, 'tether-s8-a-implementation', version=proof_version)
    pb = stage(repo, vendor, b_ref, 'tether-s8-b-implementation', version=proof_version)
    require(pa['binary_sha256'] != pb['binary_sha256'], 'S8 pair compiled implementation is identical')
    # Actual version probes prove advertisement only. P24's nonempty decode and
    # current rollback tests provide behavior receipts; this manifest cannot.
    required = ['transfer-v1', 'upgrade-tx-v1', 'authority-incarnation-v1', 'upgrade-cut-v1', 'image-read-epochs 1', 'image-abi v1']
    versions = {}
    for label in ('a', 'b'):
        output = version_probe(vendor/('tether-s8-'+label))
        require(all(token in output.splitlines() if ' ' in token else token in output.split() for token in required), 'S8 pair decoder advertisement incomplete; pending P13-P17')
        versions[label] = output.splitlines()
    doc = dict(schema_version=1, a=a, b=b, implementation_digests=[a_impl, b_impl],
               version_probes=versions, compiled_implementation_proofs=[pa, pb], decoder_behavior='NOT_RUN', continuity='NOT_RUN')
    publish(vendor/'upgrade-pair.json', doc)
    return doc


def process_identity(pid):
    raw = pathlib.Path('/proc/'+str(pid)+'/stat').read_text()
    fields = raw[raw.rfind(')')+2:].split()
    require(fields[0] != 'Z', 'observed process is already dead')
    return dict(pid=pid, start_ticks=int(fields[19]), boot_id=pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip())


def observe(repo, vendor, name, ref, pid, copied, argv, out):
    out.unlink(missing_ok=True)
    doc = verify(repo, vendor, ref, name)
    before = process_identity(pid)
    actual_argv = pathlib.Path('/proc/'+str(pid)+'/cmdline').read_bytes().rstrip(b'\0').split(b'\0')
    require(actual_argv == [os.fsencode(arg) for arg in argv], 'executed argv differs from manifest')
    require(pathlib.Path(argv[0]).is_absolute() and os.path.samefile(argv[0], copied), 'executed path differs from copied artifact')
    running = file_sha('/proc/'+str(pid)+'/exe')
    require(os.path.samefile('/proc/'+str(pid)+'/exe', copied), 'running executable differs from copied path')
    require(running == doc['binary_sha256'] == file_sha(copied), 'source-copy-execute SHA mismatch')
    require(process_identity(pid) == before, 'process identity changed during observation')
    result = dict(schema_version=1, artifact=doc, copied_path=str(copied.resolve()), copied_sha256=file_sha(copied),
                  proc_exe=os.readlink('/proc/'+str(pid)+'/exe'), running_sha256=running, argv=argv,
                  observed_at=datetime.now(timezone.utc).isoformat(), **before)
    publish(out, result)
    return result


def execution_ok(repo, vendor, name, ref, copied, evidence):
    # Staging, copying and a caller-supplied SHA are insufficient to claim N-1.
    artifact = verify(repo, vendor, ref, name)
    doc = read_json(evidence)
    require(doc['schema_version'] == 1 and doc['artifact'] == artifact, 'execution source differs from staged source')
    require(doc['running_sha256'] == artifact['binary_sha256'] == doc['copied_sha256'] == file_sha(copied), 'execution SHA mismatch')
    require(doc['copied_path'] == str(copied.resolve()) and doc['pid'] > 0 and doc['start_ticks'] > 0
            and doc['boot_id'] and doc['proc_exe'] and doc['argv'] and doc['observed_at'], 'previous artifact was not observed executing')
    return doc


def wire_manifest(repo, vendor, new_ref, fixture, out):
    # Values come from sealed-fixture export; argv is generated, never shell-eval'd.
    # Fixture owner verifies the sealed custody and supplies eight isolated homes.
    out.unlink(missing_ok=True)
    old = verify(repo, vendor, FIXED_TAG, 'tether-prev', FIXED_COMMIT)
    new = verify(repo, vendor, new_ref, 'tether-wire-new')
    probe_source = repo/'test/hotupdate/wireprobe/main.go'
    require(probe_source.is_file(), 'P24 wire probe source is missing')
    values = read_json(fixture)
    cells = ['Q'+q+'B'+b for b in '01' for q in ('00', '01', '10', '11')]
    require(set(values) == set(cells), 'wire fixture must contain all eight cells')
    rows, companions, probes = [], {}, {}
    for cell in cells:
        spec = values[cell]
        require(set(spec) == {'values', 'agent_argv', 'broker_argv'}, 'wire fixture process argv is missing')
        v = spec['values']
        require(set(v) == {'HOME_CELL', 'NATS_URL', 'NID', 'PID', 'NONCE'}, 'wire fixture fields differ from sealed export')
        require(all(type(x) is str and x and '\0' not in x for x in v.values()), 'invalid wire fixture value')
        require(v['HOME_CELL'].startswith('/') and re.fullmatch('[0-9a-f]{32}', v['NONCE'])
                and re.fullmatch('[1-9][0-9]*', v['PID']), 'invalid wire fixture identity')
        selected = [old if bit == '0' else new for bit in (cell[1], cell[2], cell[4])]
        paths = ['/opt/s8/'+('old' if bit == '0' else 'new')+'/tether' for bit in (cell[1], cell[2], cell[4])]
        ctl, agent, broker = paths
        aa, ba = spec['agent_argv'], spec['broker_argv']
        require(type(aa) is list and type(ba) is list and all(type(x) is str and x and '\0' not in x for x in aa+ba), 'invalid process argv')
        require(len(aa) > 2 and aa[:2] == [agent, 'agent'] and '--session' in aa and '--nid' in aa and '--nats-url' in aa, 'agent ExecStart differs from quadrant')
        require(aa[aa.index('--nid')+1] == v['NID'] and aa[aa.index('--nats-url')+1] == v['NATS_URL'], 'agent ExecStart identity differs from fixture')
        require(len(ba) == 4 and ba[:3] == [broker, 'serve', '--config'] and ba[3].startswith('/'), 'broker ExecStart differs from quadrant')
        common = ['--home', v['HOME_CELL'], '--nats-url', v['NATS_URL']]
        nonce, nid = v['NONCE'], v['NID']
        probe_name = 'upgrade-wire-probe-'+('old' if cell[1] == '0' else 'new')
        probe = read_json(vendor/(probe_name+'-source.json'))
        probe_expected, _ = expected_source(repo, selected[0]['tag'], probe_name)
        probe_expected.update(build_package='./cmd/upgrade-wire-probe', probe_source_sha256=file_sha(probe_source))
        require(cache_ok(vendor/probe_name, vendor/(probe_name+'-source.json'), probe_expected), 'wire probe cache is incomplete')
        probes[probe_name] = probe
        # The probe receipt binds the same protocol source used by the selected ctl.
        require(probe['commit'] == selected[0]['commit'], 'wire probe source differs from ctl schema')
        pp = '/opt/s8/'+('old' if cell[1] == '0' else 'new')+'/upgrade-wire-probe'
        run_argv = [ctl, 'run', *common, nid, '--', '/bin/sh', '-c', 'printf "wire-pty-%s\\n" "$1"; read x; printf "%s\\n" "$x"', 'sh', nonce]
        commands = {
            'exec': [ctl, 'exec', *common, '--timeout', '30s', nid, '--', '/bin/sh', '-c', 'printf "wire-exec-%s\\n" "$1"', 'sh', nonce],
            'run': run_argv,
            'kill': [pp, *common, '--verb', 'kill', '--nid', nid, '--pid', v['PID'], '--signal', '15'],
            'expose': [ctl, 'expose', *common, '--local', '18080', '--name', 'wire-'+nonce, '--json', nid],
            'transfer': [ctl, 'push', *common, '--timeout', '120s', '/tmp/s8/'+nonce+'/source', nid+':/tmp/s8/'+nonce+'/dest']}
        effects = {'exec': 'exact nonce output and execution count=1', 'run': 'real PTY input/output/resize and terminal',
                   'kill': 'original run child exits with one terminal', 'expose': 'public-port nonce exchange and cleanup',
                   'transfer': '8MiB tier-B push/pull hash and unique terminals'}
        for verb, argv in commands.items():
            rows.append(dict(cell=cell, verb=verb, argv=argv, ctl_path=ctl, ctl_sha=selected[0]['binary_sha256'],
                             agent_path=agent, agent_sha=selected[1]['binary_sha256'], broker_path=broker, broker_sha=selected[2]['binary_sha256'],
                             probe_path=pp if verb == 'kill' else '', probe_sha=probe['binary_sha256'] if verb == 'kill' else '',
                             source_commit=selected[0]['commit'], expected_effect=effects[verb]))
        companions[cell] = dict(agent_argv=aa, broker_argv=ba, kill_start=run_argv,
                                kill_old_ctl_sigint=(cell[1] == '0'),
                                expose_cleanup=[ctl, 'expose', 'rm', *common, '--name', 'wire-'+nonce, nid],
                                transfer_pull=[ctl, 'pull', *common, '--timeout', '120s', nid+':/tmp/s8/'+nonce+'/dest', '/tmp/s8/'+nonce+'/back'])
    # The frozen P00 row remains exact. Secondary commands and full provenance
    # are separate so transfer pull/expose rm cannot disappear from execution.
    publish(out, dict(schema_version=1, rows=rows, companions=companions, old=old, new=new, probes=probes, execution='NOT_RUN'))
    return rows


def main(args):
    repo, vendor = map(lambda x: pathlib.Path(x).resolve(), args[:2])
    op, *a = args[2:]
    if op == 'stage':
        result = stage(repo, vendor, *a)
    elif op == 'verify':
        result = verify(repo, vendor, *a)
    elif op == 'probe':
        require(len(a) == 3, 'probe expects reference, component and P24 source')
        result = stage(repo, vendor, a[0], a[1], probe=pathlib.Path(a[2]))
    elif op == 'pair':
        result = pair(repo, vendor, *a)
    elif op == 'observe':
        require(len(a) >= 6, 'observe expects name/ref/PID/copy/receipt/argv')
        result = observe(repo, vendor, a[0], a[1], int(a[2]), pathlib.Path(a[3]), a[5:], pathlib.Path(a[4]))
    elif op == 'execution-ok':
        result = execution_ok(repo, vendor, a[0], a[1], pathlib.Path(a[2]), pathlib.Path(a[3]))
    elif op == 'wire':
        result = wire_manifest(repo, vendor, a[0], pathlib.Path(a[1]), pathlib.Path(a[2]))
    else:
        raise ValueError('unknown artifact operation')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except (ValueError, KeyError, IndexError, TypeError, OSError, subprocess.CalledProcessError) as error:
        print('artifact supplier: '+str(error), file=sys.stderr)
        sys.exit(2)
PYARTIFACTS
)
