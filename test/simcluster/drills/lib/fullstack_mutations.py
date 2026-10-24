#!/usr/bin/env -S python3 -I -B
"""Source-bound S10 controls and native baseline/mutant/restore receipts.

This driver never supplies product observations. The P19/P25 assembly host
owns live windows and the integration oracles. Missing adapters are NOT_RUN;
hermetic parser controls cannot sign a deployment or a continuity mutant.
"""
import argparse
import ast
from functools import lru_cache
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tokenize

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
KEYS = tuple(('M01A M01B M02E M02P M02F M02X M03E M03P M03F M03X '
              'M04A M04B M04P M05E M05P M05X M05R M06A M06B M06R M06F '
              'M07A M07B M07N M07C M08E M08P M08A M08B M08D M08X M08C M08O '
              'M09A M09B M10S M10C M11W M11I M11O M11P M11Q M11F M11S '
              'M11X M11D M11C M11L M11U M11K').split())
PHASES = ('baseline', 'mutant', 'restore')
VERDICTS = ('GREEN', 'ASSERT-FAIL', 'SETUP-RED', 'PRODUCT-RED', 'INCOMPLETE')
ROW_FIELDS = set('id defect surface source_path function predicate compatible_artifact_set role topologies window hit_event '
                 'expected_assertion integration pending_owners deploy_only'.split())
_runs = {}  # Original waiters, not archived data, authorize a native parent.
GO_TOKEN = re.compile(r'"(?:\\.|[^"\\])*"|`[^`]*`|\'(?:\\.|[^\'\\])*\'|'
                      r'//[^\n]*|/\*.*?\*/|[A-Za-z_][A-Za-z_0-9]*|\d+|[^\s]', re.S)


class ContractError(ValueError):
    pass


class Pending(RuntimeError):
    pass


def require(ok, criterion):
    if not ok:
        raise ContractError(criterion)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def is_sha(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON member')
        result[key] = value
    return result


def decode(raw):
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda _: require(False, 'nonfinite JSON'))


def load(path):
    private_path(path)
    return decode(Path(path).read_bytes())


def private_path(path):
    # A receipt or caller path must never make a read-only control touch the
    # operator's production state or select the installed tether artifact.
    require('.tether' not in Path(path).resolve().parts and '/.local/bin/tether' not in str(Path(path).resolve()),
            'production path forbidden')


def archive(root, name, value):
    # Exclusive creation retains first-attempt evidence even after a failed
    # build, native setup error or emergency supervisor cleanup.
    path = Path(root) / name
    with path.open('xb') as stream:
        stream.write(canonical(value) + b'\n')
        stream.flush()
        os.fsync(stream.fileno())
    return path


def file_at(root, relative):
    require(isinstance(relative, str) and relative, 'missing source or raw path')
    name = Path(relative)
    require(not name.is_absolute() and '..' not in name.parts, 'source or archive escape')
    path = Path(root) / name
    private_path(path)
    require(path.resolve().is_relative_to(Path(root).resolve()) and path.is_file()
            and not any(p.is_symlink() for p in [path, *path.parents] if p != Path(root).parent),
            'missing or aliased source or raw')
    return path


def read_ref(root, ref):
    require(set(ref) == {'path', 'sha', 'bytes'}, 'raw reference fields')
    raw = file_at(root, ref['path']).read_bytes()
    require(type(ref['bytes']) is int and 0 < ref['bytes'] <= 64 << 20 and
            len(raw) == ref['bytes'] and sha(raw) == ref['sha'], 'raw bytes or digest')
    return raw


def tokens(source, suffix):
    if suffix == '.py':
        # AST ignores comments and formatting, but retains literals and calls.
        return ast.dump(ast.parse(source), include_attributes=False)
    return [m.group() for m in GO_TOKEN.finditer(source)
            if not m.group().startswith(('//', '/*'))]


# Cache by complete immutable bytes, never pathname/inode: on-disk source
# mutations must invalidate the scanner even when the path remains identical.
# origin: hot-update S10 plan §8 / §12.3 / FullstackMutationManifestTest
@lru_cache(maxsize=256)
def function_body(source, function, suffix):
    if suffix == '.py':
        parts = function.split('.')
        nodes = ast.parse(source).body
        for part in parts:
            matches = [n for n in nodes if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name == part]
            require(len(matches) == 1, 'missing or ambiguous Python source binding')
            node = matches[0]
            nodes = node.body
        require(isinstance(node, ast.FunctionDef), 'binding is not a Python function')
        return ast.get_source_segment(source, node)
    matches = list(re.finditer(r'^func\s+(?:\([^()\n]*\)\s+)?' + re.escape(function) + r'\s*\(', source, re.M))
    declarations = {m.start() for m in GO_TOKEN.finditer(source) if m.group() == 'func'}
    matches = [m for m in matches if m.start() in declarations]
    require(len(matches) == 1, 'missing or ambiguous Go source binding')
    start = matches[0].start()
    opened, depth, parens = False, 0, 1
    for token in GO_TOKEN.finditer(source, matches[0].end()):
        value = token.group()
        if not opened and value in ('(', ')'):
            parens += 1 if value == '(' else -1
        if not opened and parens:
            continue
        if value == '{':
            opened, depth = True, depth + 1
        elif value == '}':
            depth -= 1
            if opened and depth == 0:
                return source[start:token.end()]
    raise ContractError('unterminated Go source binding')


def code_tokens(source, suffix, fragment=False):
    """Executable tokens only: comments vanish and every literal is one token."""
    if suffix != '.py':
        return tuple(tokens(source, '.go'))
    skipped = (tokenize.COMMENT, tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT, tokenize.ENDMARKER)
    result = []
    try:
        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            if token.type not in skipped:
                result.append(token.string)
    except (tokenize.TokenError, SyntaxError):
        # A predicate fragment must be complete Python; partial tokens could
        # degrade it to a shorter sequence that matches unrelated code.
        raise ContractError('unparsable Python ' + ('predicate' if fragment else 'source binding')) from None
    return tuple(result)


# Comments are moved verbatim when code moves (CLAUDE.md), so a predicate can
# outlive its code as prose inside the bound function (S8 replaced M06F's
# install call with a refusal whose comments still narrate the old effects). A
# substring hit in a comment, string or docstring is not a site a mutant patch
# could ever execute.
# origin: hot-update S10 plan §8 / §10.2 P20; r9d REPORT Request 4 (M06F/M09A)
@lru_cache(maxsize=256)
def binds_live_code(body, predicate, suffix):
    want, have = code_tokens(predicate, suffix, fragment=True), code_tokens(body, suffix)
    return bool(want) and any(have[i:i + len(want)] == want for i in range(len(have) - len(want) + 1))


# A row is bound only while its source file, function and predicate exist as
# executable code and its integration test, exercise and oracle still exist.
# origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
def check_binding(row, repo):
    require(set(row) == ROW_FIELDS and row['surface'] in ('product', 'harness', 'source_gate'), 'mutation row fields')
    require(row['role'] in ('agent', 'broker', 'nats', 'caddy', 'fixture', 'ctl'), 'mutation role')
    require(row['topologies'] in (['single'], ['single', 'quorum']), 'mutation cells')
    require(type(row['deploy_only']) is bool and row['pending_owners'], 'missing dependency or tier')
    for field in ('defect', 'window', 'hit_event', 'expected_assertion', 'predicate', 'compatible_artifact_set'):
        require(isinstance(row[field], str) and row[field], 'empty predicate or oracle')
    path = file_at(repo, row['source_path'])
    require(path.suffix in ('.go', '.py') and not path.name.endswith(('_test.go', '_test.py')), 'test-only mutation binding')
    require((row['surface'] == 'product') == row['source_path'].startswith('internal/'), 'mutation surface misclassified')
    body = function_body(path.read_text(), row['function'], path.suffix)
    require(row['predicate'] in body and tokens(row['predicate'], '.go'), 'predicate absent from actual function')
    require(binds_live_code(body, row['predicate'], path.suffix), 'predicate survives only in a comment or literal')
    integration = row['integration']
    require(set(integration) == {'path', 'test', 'exercise', 'oracle_path', 'oracle'}, 'integration binding fields')
    test_source = file_at(repo, integration['path']).read_text()
    test_body = function_body(test_source, integration['test'], '.go')
    require('testharness.AssertNoGoroutineLeak(' in test_body and 'testharness.AssertNoFDLeak(' in test_body
            and 'round < 5' in test_body and integration['exercise'] + '(' in test_body,
            'missing real integration leak anchors')
    oracle_source = file_at(repo, integration['oracle_path']).read_text()
    function_body(oracle_source, integration['oracle'], '.go')


# origin: hot-update S10 plan §8 / §10.2 P20 / FullstackMutationManifestTest
def validate_manifest(value, repo=REPO):
    require(set(value) == {'schema', 'required_children', 'expected_native', 'mutations'}, 'mutation manifest fields')
    require(value['schema'] == 'fullstack-mutations-v1' and value['required_children'] == list(KEYS)
            and value['expected_native'] == [0, 1, 0], '50 exact subcases or native chain changed')
    rows = value['mutations']
    require([r['id'] for r in rows] == list(KEYS), 'missing, duplicate or replaced mutation')
    for row in rows:
        try:
            check_binding(row, repo)
        except ContractError as error:
            # A source move in another stage turned every drill 100 arm
            # SETUP-RED with no hint of which of the 50 rows went stale.
            # origin: r9d REPORT Request 4
            raise ContractError(row['id'] + ': ' + str(error)) from None
    return value


def manifest():
    return validate_manifest(load(HERE / 'fullstack_mutations.json'))


def overlay_flags(inherited, mapping, output):
    flags, combined = [], {}
    for flag in shlex.split(inherited):
        require(flag != '-overlay', 'GOFLAGS overlay must use equals')
        if flag.startswith('-overlay='):
            prior = load(flag.split('=', 1)[1])
            require(set(prior) == {'Replace'} and isinstance(prior['Replace'], dict), 'inherited overlay fields')
            for source, target in prior['Replace'].items():
                require(Path(source).is_absolute() and isinstance(target, str) and Path(target).is_absolute()
                        and source not in combined, 'inherited overlay path or duplicate')
                combined[source] = target
        else:
            flags.append(flag)
    require(not set(mapping) & set(combined), 'overlay source already mutated')
    combined.update(mapping)
    path = archive(output, 'overlay.json', {'Replace': combined})
    flags.append('-overlay=' + str(path.resolve()))
    # GOFLAGS is inherited by all nested builders, not just the outer test.
    return shlex.join(flags), sha(path.read_bytes())


# Owner patches are exact source handoffs, not executable commands or oracle
# edits. A token-equivalent/comment-only patch cannot certify a behavioral kill.
# origin: hot-update S10 plan §8 / §10.2 P20 / only-changing-expected
def prepare_patch(row, patch, repo, output, inherited=''):
    require(set(patch) == {'schema', 'mutation_id', 'source_sha', 'before', 'after'} and
            patch['schema'] == 'fullstack-mutation-patch-v1' and patch['mutation_id'] == row['id'], 'patch fields or identity')
    source_path = file_at(repo, row['source_path'])
    raw, suffix = source_path.read_bytes(), source_path.suffix
    source = raw.decode()
    require(sha(raw) == patch['source_sha'], 'patch source drift')
    before, after = patch['before'], patch['after']
    body = function_body(source, row['function'], suffix)
    require(isinstance(before, str) and isinstance(after, str) and before and after and
            source.count(before) == body.count(before) == 1 and row['predicate'] in before, 'patch misses bound predicate')
    changed = source.replace(before, after, 1)
    require(tokens(before, suffix if suffix == '.go' else '.go') != tokens(after, '.go') and
            tokens(source, suffix) != tokens(changed, suffix), 'only expected, comments or formatting changed')
    # M10 may remove a launch or probe, never alter its independent schedule,
    # required-cell denominator or judge. The permitted sites are exact calls.
    if row['id'] == 'M10S':
        require(before == row['predicate'] and re.fullmatch(
                r"None if request\.slot\.id == '[A-Za-z0-9_./-]+' else self\.launch\(request\)", after),
                'scheduler mutation edits expected or omits more than one slot')
    if row['id'] == 'M10C':
        # Until P19/P25 supplies the actual predeclared probe dispatch, the
        # before_step seam is a source binding only and cannot sign a kill.
        raise Pending('NOT_RUN pending S10 P19/P25 exact targeted-cut probe sender')
    output = Path(output)
    private_path(output)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    (output / 'source.before').write_bytes(raw)
    mutant = output / ('source.after' + suffix)
    mutant.write_text(changed)
    flags, overlay_digest, harness = inherited, '', ''
    if row['surface'] == 'product':
        flags, overlay_digest = overlay_flags(inherited, {str(source_path.resolve()): str(mutant.resolve())}, output)
    else:
        # Python cannot use Go overlays. Copy its real import closure so the
        # host can select the isolated harness without touching the candidate.
        harness = str((output / 'harness').resolve())
        shutil.copytree(Path(repo) / 'test/simcluster/drills/lib', harness,
                        ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
        Path(harness, source_path.name).write_text(changed)
    result = dict(schema='fullstack-mutation-source-v1', mutation_id=row['id'], source_sha=sha(raw),
                  mutant_source_sha=sha(changed.encode()), patch_digest=sha(canonical(patch)),
                  goflags=flags, overlay_digest=overlay_digest, harness_root=harness)
    archive(output, 'source.json', result)
    return result


def oracle_digest(row, repo=REPO):
    binding = row['integration']
    source = file_at(repo, binding['oracle_path']).read_text()
    return sha(function_body(source, binding['oracle'], '.go').encode())


def check_builds(row, receipt, source, phase, root):
    builds = receipt['builds']
    require(isinstance(builds, list) and builds, 'missing build and executing identity')
    required = {'outer', 'product', 'nested'} if row['surface'] == 'product' else {'harness'}
    require(len(builds) == len(required) and {b['kind'] for b in builds} == required, 'overlay did not enter nested build')
    for build in builds:
        require(set(build) == {'kind', 'role', 'goflags', 'overlay_digest', 'source_sha', 'artifact', 'executing'}, 'build fields')
        require(build['kind'] in required and build['role'] == row['role'], 'build role or surface')
        artifact, executing = read_ref(root, build['artifact']), read_ref(root, build['executing'])
        require(artifact == executing, 'running bytes differ from built artifact')
        expected_source = source['mutant_source_sha'] if phase == 'mutant' else source['source_sha']
        require(build['source_sha'] == expected_source, 'executed source binding mismatch')
        if row['surface'] == 'product' and phase == 'mutant':
            require(build['goflags'] == source['goflags'] and build['overlay_digest'] == source['overlay_digest']
                    and any(f.startswith('-overlay=') for f in shlex.split(build['goflags'])), 'overlay did not enter nested build')
        elif row['surface'] == 'product':
            require(build['overlay_digest'] != source['overlay_digest'], 'mutant overlay survived restore')


# Compile/setup/refusal/timeout and a red from another domain are not kills.
# The hit must precede this assertion inside the exact admitted nonempty cut.
# origin: hot-update S10 plan §8 / §10.2 P20 / red-in-wrong-window
def check_phase(row, receipt, source, phase, cell, root, native_rc):
    require(set(receipt) == {'schema', 'mutation_id', 'phase', 'surface', 'topology', 'tier', 'artifact_set',
            'source_sha', 'dirty_digest', 'patch_digest', 'manifest_digest', 'schedule', 'oracle_digest',
            'reader_digest', 'native_rc', 'verdict', 'assertion', 'hit', 'builds', 'raw', 'supervision'}, 'receipt fields')
    require(receipt['schema'] == 'fullstack-mutation-run-v1' and receipt['mutation_id'] == row['id'] and
            receipt['phase'] == phase and receipt['surface'] == row['surface'] and receipt['topology'] == cell,
            'receipt from another mutation or cell')
    require(receipt['tier'] in ('hermetic', 'deploy', 'source_gate') and
            (not row['deploy_only'] or receipt['tier'] == 'deploy'), 'hermetic receipt cannot sign managed infra')
    for field in ('source_sha', 'dirty_digest', 'patch_digest', 'manifest_digest', 'oracle_digest', 'reader_digest'):
        require(is_sha(receipt[field]), 'missing candidate or reader digest')
    require(receipt['source_sha'] == source['source_sha'] and receipt['patch_digest'] == source['patch_digest'], 'receipt source or patch drift')
    expected = 1 if phase == 'mutant' else 0
    require(type(native_rc) is int and native_rc == receipt['native_rc'] == expected and
            receipt['verdict'] == VERDICTS[expected], 'native result is not 0->1->0 assertion chain')
    require(receipt['assertion'] == (row['expected_assertion'] if expected else ''), 'red at another assertion')
    log = read_ref(root, receipt['raw']).decode()
    require(log and (not expected or row['expected_assertion'] in log), 'missing native assertion text')
    require(not any(s in log for s in ('NOT_RUN pending', 'SETUP-RED', 'build failed', 'panic: test timed out',
                                      'not implemented (S10', 'unknown flag:')), 'setup or timeout masquerades as kill')
    read_ref(root, receipt['schedule'])
    supervision = decode(read_ref(root, receipt['supervision']))
    require(set(supervision) == {'native_rc', 'joined', 'remaining'} and supervision['native_rc'] == native_rc
            and supervision['joined'] is True and supervision['remaining'] == [], 'supervision cleanup did not join')
    hit = decode(read_ref(root, receipt['hit']))
    require(set(hit) == {'mutation_id', 'role', 'window', 'event', 'cut_id', 'generation', 'pid', 'start_ticks',
                       'boot_id', 'start_ns', 'at_ns', 'end_ns', 'assertion_ns', 'pending', 'accepted'}, 'hit fields')
    require((hit['mutation_id'], hit['role'], hit['window'], hit['event']) ==
            (row['id'], row['role'], row['window'], row['hit_event']), 'red in wrong window or domain')
    require(all(type(hit[k]) is int and hit[k] > 0 for k in ('generation', 'pid', 'start_ticks', 'start_ns',
                    'at_ns', 'end_ns', 'assertion_ns', 'pending')) and hit['boot_id'] and hit['cut_id']
            and hit['start_ns'] <= hit['at_ns'] <= hit['assertion_ns'] <= hit['end_ns']
            and hit['accepted'] is True, 'empty, expired or unaccepted mutation window')
    check_builds(row, receipt, source, phase, root)
    return receipt


def check_triplet(row, root, cell, expected_manifest=None, repo=REPO):
    root = Path(root)
    private_path(root)
    source = load(root / 'mutation/source.json')
    patch = load(root / 'patch.json')
    original = (root / 'mutation/source.before').read_bytes()
    changed = (root / ('mutation/source.after' + Path(row['source_path']).suffix)).read_bytes()
    require(original == file_at(repo, row['source_path']).read_bytes(), 'source archive differs from actual candidate')
    require(source['schema'] == 'fullstack-mutation-source-v1' and source['mutation_id'] == row['id'] and
            sha(original) == source['source_sha'] and sha(changed) == source['mutant_source_sha'] and
            sha(canonical(patch)) == source['patch_digest'], 'source archive tampered')
    require(original.decode().count(patch['before']) == 1 and
            row['predicate'] in patch['before'] and patch['before'] in
            function_body(original.decode(), row['function'], Path(row['source_path']).suffix) and
            original.decode().replace(patch['before'], patch['after'], 1).encode() == changed and
            tokens(original.decode(), Path(row['source_path']).suffix) !=
            tokens(changed.decode(), Path(row['source_path']).suffix), 'only changing expected or unchanged source')
    if row['surface'] == 'product':
        overlay = load(root / 'mutation/overlay.json')
        require(sha((root / 'mutation/overlay.json').read_bytes()) == source['overlay_digest'] and
                overlay['Replace'].get(str(file_at(repo, row['source_path']).resolve())) ==
                str((root / 'mutation/source.after.go').resolve()), 'overlay bytes or source missing')
    rows = []
    for phase in PHASES:
        native = load(root / phase / 'waiter.json')
        require(set(native) == {'native_rc'}, 'native waiter fields')
        receipt = load(root / phase / 'receipt.json')
        rows.append(check_phase(row, receipt, source, phase, cell, root / phase, native['native_rc']))
    for field in ('source_sha', 'dirty_digest', 'patch_digest', 'manifest_digest', 'oracle_digest', 'reader_digest',
                  'schedule', 'artifact_set', 'tier'):
        require(rows[0][field] == rows[1][field] == rows[2][field], 'only expected, candidate or oracle changed between phases')
    sealed = (root / 'sealed-schedule.json').read_bytes()
    require(sealed and all(read_ref(root / phase, r['schedule']) == sealed for phase, r in zip(PHASES, rows)),
            'observed launches resealed the independent expected schedule')
    require(rows[0]['artifact_set'] == row['compatible_artifact_set'] and
            (expected_manifest is None or rows[0]['manifest_digest'] == expected_manifest), 'artifact set or manifest drift')
    require(rows[0]['oracle_digest'] == oracle_digest(row, repo), 'receipt oracle differs from actual integration assertion')
    for kind in {b['kind'] for b in rows[0]['builds']}:
        artifacts = [next(b for b in r['builds'] if b['kind'] == kind)['artifact']['sha'] for r in rows]
        # An outer test/host may not link the affected production package.
        # Its flags still carry the overlay, while real product/nested bytes
        # (or the selected Python harness) must actually differ.
        require(artifacts[0] == artifacts[2] and (kind == 'outer' or artifacts[0] != artifacts[1]),
                'mutant bytes absent or restore artifact changed')
    return dict(schema='fullstack-mutation-chain-v1', mutation_id=row['id'], topology=cell,
                native_rc=1, chain=[0, 1, 0], receipts=[sha(canonical(r)) for r in rows])


def join_children(children, catalog, native_waiter=None):
    required = {(r['id'], c) for r in catalog['mutations'] for c in r['topologies']}
    require(isinstance(children, list), 'control child receipts required')
    seen = set()
    for child in children:
        require(set(child) == {'mutation_id', 'topology', 'root'}, 'child receipt fields')
        key = child['mutation_id'], child['topology']
        require(key in required and key not in seen, 'duplicate or unexpected mutation cell')
        row = next(r for r in catalog['mutations'] if r['id'] == key[0])
        result = check_triplet(row, child['root'], key[1], sha(canonical(catalog)))
        # P32 supplies its original waiter capability when children ran in
        # separate shell processes. An archive cannot attest its own statuses.
        original = native_waiter(child) if native_waiter is not None else _runs.get(str(Path(child['root']).resolve()))
        if original is None:
            raise Pending('NOT_RUN pending S10 P32 original mutation child waiter Join capability')
        require(original == result, 'native waiter differs from archived child chain')
        seen.add(key)
    if seen != required:
        raise Pending('NOT_RUN pending P19/P25 mutation cells: ' + ','.join(k + '/' + c for k, c in sorted(required - seen)))
    return 1  # Native ASSERT-FAIL children retain the native parent join.


# Use P05's shared launcher, including for this driver's outer shell. The host
# receives inert input; evidence never chooses a command, cwd or executable.
# origin: hot-update S10 plan §6 / L15 / §8
def execute_triplet(row, cell, patch, root, host, supervisor, judge, inherited='', expectations_path=None):
    if expectations_path is None:
        raise Pending('NOT_RUN pending S10 P17/P19 presealed mutation schedule and required-cell ledger')
    expectations = load(expectations_path)
    require(set(expectations) == {'schema', 'mutation_id', 'cells'} and
            expectations['schema'] == 'fullstack-mutation-expectations-v1' and
            expectations['mutation_id'] == row['id'] and
            [c['topology'] for c in expectations['cells']] == row['topologies'], 'presealed required-cell ledger')
    expected = next(c for c in expectations['cells'] if c['topology'] == cell)
    require(set(expected) == {'topology', 'schedule'}, 'expected cell fields')
    sealed = read_ref(Path(expectations_path).parent, expected['schedule'])
    sys.path.insert(0, str(HERE))
    from fullstack_flow import source_pinned_tools
    for path in (root, host, supervisor, judge):
        private_path(path)
    try:
        paths, owned = source_pinned_tools((host, supervisor, judge))
    except (OSError, RuntimeError, ValueError):
        raise Pending('NOT_RUN pending S10 P19/P25 source-pinned assemblycmd mutation adapter; ' + ','.join(row['pending_owners'])) from None
    try:
        root = Path(root).resolve()
        require(not root.is_relative_to(REPO), 'mutation receipts must be outside repository')
        root.mkdir(mode=0o700, parents=True, exist_ok=False)
        (root / 'sealed-schedule.json').write_bytes(sealed)
        archive(root, 'sealed-cells.json', expectations)
        archive(root, 'patch.json', patch)
        source = prepare_patch(row, patch, REPO, root / 'mutation', inherited)
        for phase in PHASES:
            directory = root / phase
            directory.mkdir(mode=0o700)
            request = dict(schema='fullstack-mutation-request-v1', mutation=row, phase=phase, topology=cell,
                           source=source, manifest_digest=sha(canonical(manifest())), evidence_root=str(directory),
                           goflags=source['goflags'] if phase == 'mutant' else inherited,
                           harness_root=source['harness_root'] if phase == 'mutant' else str(HERE),
                           expected_schedule_sha=sha(sealed), required_cells=row['topologies'])
            path = archive(directory, 'request.json', request)
            home = directory / 'home'
            home.mkdir(mode=0o700)
            env = {k: v for k, v in os.environ.items() if not k.startswith(('_TETHER_', 'TETHER_'))}
            env.update(GOFLAGS=request['goflags'], HOME=str(home), XDG_STATE_HOME=str(directory / 'state'))
            # The launcher owns cancellation and descendant cleanup. The
            # original Popen waiter is always joined, even on driver failure.
            with (directory / 'host.stdout').open('xb') as stdout, (directory / 'host.stderr').open('xb') as stderr:
                with subprocess.Popen([paths[1], '--', paths[0], '--fullstack-mutation-request', str(path)],
                                      cwd=root, env=env, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr) as child:
                    try:
                        code = child.wait()
                    except BaseException:
                        child.terminate()
                        child.wait()
                        raise
            archive(directory, 'waiter.json', {'native_rc': code})
            if not (directory / 'receipt.json').is_file():
                raise Pending('NOT_RUN pending S10 P19/P25 mutation receipt adapter; native rc retained')
            check_phase(row, load(directory / 'receipt.json'), source, phase, cell, directory, code)
        result = check_triplet(row, root, cell, sha(canonical(manifest())))
        archive(root, 'chain.json', result)
        _runs[str(root)] = result
        return 1
    finally:
        for fd in owned:
            os.close(fd)


def execute_arm(row, patch, root, host, supervisor, judge, inherited='', expectations_path=None):
    root = Path(root).resolve()
    private_path(root)
    require(not root.is_relative_to(REPO), 'mutation receipts must be outside repository')
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    for cell in row['topologies']:
        execute_triplet(row, cell, patch, root / cell, host, supervisor, judge, inherited, expectations_path)
    return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='op', required=True)
    sub.add_parser('validate')
    sub.add_parser('catalog')
    for op in ('run', 'run-arm', 'check'):
        p = sub.add_parser(op)
        p.add_argument('--mutation', choices=KEYS, required=True)
        if op != 'run-arm':
            p.add_argument('--topology', choices=('single', 'quorum'), required=True)
        p.add_argument('--root', required=True)
        if op in ('run', 'run-arm'):
            p.add_argument('--patch', required=True)
            p.add_argument('--expectations', required=True)
    p = sub.add_parser('join')
    p.add_argument('--children', required=True)
    args = parser.parse_args()
    catalog = manifest()
    if args.op == 'catalog':
        print(json.dumps(catalog, sort_keys=True))
    elif args.op == 'join':
        return join_children(load(args.children), catalog)
    elif args.op in ('run', 'run-arm', 'check'):
        row = next(r for r in catalog['mutations'] if r['id'] == args.mutation)
        if args.op != 'run-arm':
            require(args.topology in row['topologies'], 'inapplicable mutation cell')
        if args.op == 'check':
            print(json.dumps(check_triplet(row, args.root, args.topology, sha(canonical(catalog))), sort_keys=True))
            return 1
        require(os.environ.get('SIM_FULLSTACK_MUTATION_SUPERVISED') == '1', 'shared P05 outer supervisor required')
        runner = execute_arm if args.op == 'run-arm' else execute_triplet
        positional = (row, load(args.patch), args.root) if args.op == 'run-arm' else (row, args.topology, load(args.patch), args.root)
        return runner(*positional,
                               os.environ.get('SIM_FULLSTACK_HOST', ''),
                               os.environ.get('SIM_UPGRADE_SUPERVISOR', ''),
                               os.environ.get('SIM_FULLSTACK_JUDGE', ''), os.environ.get('GOFLAGS', ''), args.expectations)
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Pending as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(4)
    except (ContractError, OSError, ValueError, KeyError, TypeError) as error:
        print('fullstack mutation SETUP-RED: invalid control binding or receipt: ' + str(error), file=sys.stderr)
        raise SystemExit(2)
    except Exception as error:
        # Python exits 1 on an uncaught exception, and 1 is exactly the status
        # drill 100 maps to the expected ASSERT-FAIL kill. A crash (a receipt
        # deeper than the decoder's recursion limit, an owner patch that is not
        # Python, ...) proves nothing about the mutant, so it is SETUP.
        # origin: hot-update S10 plan §8 / L9; r9d REPORT Request 4
        print('fullstack mutation SETUP-RED: unclassified driver failure: ' + type(error).__name__, file=sys.stderr)
        raise SystemExit(2)
