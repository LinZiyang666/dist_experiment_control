#!/usr/bin/env python3
"""Deployment-boundary programs for the r9d drill 99/100 arm controls.

origin: hot-update S10 plan §9 / §10.2 P19 / P20; r9d D-11.

Nothing here is a product, fixture or judge implementation. Four modes:

  shim     Interposed by the sandbox's `python3` PATH entry for exactly the two
           drill calls (`fullstack_fixture.py run`, `fullstack_mutations.py
           run-arm`). It loads the REAL module file, doubles only the seams the
           arm controls are allowed to double (the P25 source-pinned tool
           rebuild and, for 99, the P10 candidate source snapshot), records every
           failed `require` criterion so a negative can prove WHICH guard turned
           it red, and then executes the module's own `if __name__ == '__main__'`
           statement verbatim. Exceptions are not caught: an uncaught exception
           exits exactly as the real file would.
  host99   The Go assembly host double: replays the evidence document the
           control chose into the evidence root, exits with the chosen status.
  judge99  The P01 evidence judge double: replays the chosen judge document.
  host100  The P19/P25 mutation assembly double: writes one phase receipt with
           its raw files for the request it was handed and exits with the chosen
           phase status. Defects are explicit scenario knobs.

The doubles never execute an argv taken from evidence and never touch docker,
ssh, systemd, the installed tether or ~/.tether.
"""
import ast
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys


def record(reason):
    path = os.environ.get('NV_FS_CRITERIA')
    if path:
        with open(path, 'a', encoding='utf-8') as out:
            out.write(str(reason).replace('\n', ' ') + '\n')


def instrument(module):
    """Record the criterion of every failed require; behaviour is unchanged."""
    original = module.require

    def require(ok, *args, **kwargs):
        if not ok:
            record(args[0] if args else next(iter(kwargs.values()), 'unnamed criterion'))
        return original(ok, *args, **kwargs)
    module.require = require


def main_guard(target):
    tree = ast.parse(target.read_text(encoding='utf-8'))
    guards = []
    for node in tree.body:
        test = getattr(node, 'test', None)
        if (isinstance(node, ast.If) and isinstance(test, ast.Compare) and isinstance(test.left, ast.Name)
                and test.left.id == '__name__' and len(test.comparators) == 1
                and getattr(test.comparators[0], 'value', None) == '__main__'):
            guards.append(node)
    if len(guards) != 1:
        raise SystemExit(96)
    return compile(ast.Module(body=guards, type_ignores=[]), str(target), 'exec')


def shim(argv):
    flags = []
    while argv and argv[0] in ('-I', '-B'):
        flags.append(argv.pop(0))
    # Only the exact drill call shapes are interposed; anything else is a
    # sandbox wiring error, never a silently different module invocation.
    if flags != ['-I', '-B'] or len(argv) < 2:
        return 96
    target = Path(argv[0]).resolve()
    if (target.name, argv[1]) not in (('fullstack_fixture.py', 'run'), ('fullstack_mutations.py', 'run-arm')):
        return 96
    sys.path.insert(0, str(target.parent))
    import fullstack_flow
    if os.environ.get('NV_FS_PINNED') == 'double':
        expected = (os.environ.get('SIM_FULLSTACK_HOST', ''), os.environ.get('SIM_UPGRADE_SUPERVISOR', ''),
                    os.environ.get('SIM_FULLSTACK_JUDGE', ''))

        def source_pinned_tools(paths):
            # The P25 rebuild of assemblycmd/supervise/evidencecmd is the only
            # thing replaced: the drill's own three tool paths pass through.
            paths = tuple(paths)
            if paths != expected:
                raise ValueError('assembly tool population')
            record('source_pinned_tools double')
            return list(paths), []
        fullstack_flow.source_pinned_tools = source_pinned_tools
    name = target.stem
    spec = importlib.util.spec_from_file_location(name, target)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    instrument(module)
    snapshot_path = os.environ.get('NV_FS_SNAPSHOT')
    if name == 'fullstack_fixture' and snapshot_path:
        real_supplier = module.artifact_supplier

        def artifact_supplier():
            # Real P10 supplier module (join_execution and its stamp checks
            # run unchanged); only the whole-repository source archive is
            # replaced by the control's declared candidate identity.
            supplier = real_supplier()
            instrument(supplier)
            snapshot = json.loads(Path(snapshot_path).read_text())

            def source_snapshot(repo):
                if Path(repo).resolve() != target.parents[4]:
                    raise ValueError('source snapshot of another tree')
                return snapshot, b''
            supplier.source_snapshot = source_snapshot
            return supplier
        module.artifact_supplier = artifact_supplier
    code = main_guard(target)
    sys.argv = [str(target)] + argv[1:]
    module.__dict__['__name__'] = '__main__'
    exec(code, module.__dict__)
    return 0


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def host99(argv):
    if len(argv) != 4 or argv[0] != '--profile' or argv[2] != '--arm':
        return 96
    scenario = json.loads((Path(argv[1]).parent / 'scenario.json').read_text())
    if argv[3] != scenario['arm']:
        return 96
    (Path(scenario['root']) / 'evidence.json').write_bytes(canonical(scenario['evidence']))
    return scenario['host_rc']


def judge99(argv):
    if len(argv) != 5 or argv[:2] != ['judge', '--profile'] or argv[3] != '--evidence':
        return 96
    scenario = json.loads((Path(argv[2]).parent / 'scenario.json').read_text())
    if argv[4] != scenario['root']:
        return 96
    sys.stdout.buffer.write(canonical(scenario['judge']))
    return scenario['judge_rc']


def ref(directory, name, raw):
    (Path(directory) / name).write_bytes(raw)
    return dict(path=name, sha=hashlib.sha256(raw).hexdigest(), bytes=len(raw))


def host100(argv):
    if len(argv) != 2 or argv[0] != '--fullstack-mutation-request':
        return 96
    scenario = json.loads(Path(os.environ['NV_FS_SCENARIO']).read_text())
    request = json.loads(Path(argv[1]).read_text())
    row, phase, cell, source = request['mutation'], request['phase'], request['topology'], request['source']
    defect = scenario.get('defect', '')
    directory = Path(request['evidence_root'])
    expected = 1 if phase == 'mutant' else 0
    native = expected
    if phase == 'mutant' and defect == 'surviving-mutant':
        native = 0
    exit_code = native
    if phase == 'mutant' and defect == 'native-translated':
        exit_code = 3
    assertion = row['expected_assertion'] if native else ''
    if phase == 'mutant' and defect == 'other-assertion':
        assertion = scenario['other_assertion']
    log = ('phase %s cell %s native %d\n' % (phase, cell, native)).encode()
    if assertion:
        log += ('FAIL %s\n' % assertion).encode()
    schedule = (Path.cwd() / 'sealed-schedule.json').read_bytes()
    window, event = row['window'], row['hit_event']
    if defect == 'wrong-window':
        window = scenario['other_window']
    hit = dict(mutation_id=row['id'], role=row['role'], window=window, event=event, cut_id='cut-' + phase,
               generation=7, pid=4242, start_ticks=99, boot_id='boot-r9d', start_ns=10, at_ns=20, end_ns=40,
               assertion_ns=30, pending=1, accepted=True)
    kinds = ['outer', 'product', 'nested'] if row['surface'] == 'product' else ['harness']
    builds = []
    for kind in kinds:
        goflags = request['goflags']
        overlay = source['overlay_digest'] if phase == 'mutant' else ''
        if phase == 'mutant' and defect == 'overlay-missing':
            goflags, overlay = '', ''
        mutated = phase == 'mutant' and defect != 'mutant-bytes-absent'
        if phase == 'restore' and defect == 'restore-changed':
            mutated = True
        payload = ('%s artifact for %s %s\n' % ('mutant' if mutated else 'original', kind, row['id'])).encode()
        artifact = ref(directory, 'artifact-' + kind, payload)
        executing = ref(directory, 'executing-' + kind, payload)
        builds.append(dict(kind=kind, role=row['role'], goflags=goflags, overlay_digest=overlay,
                           source_sha=source['mutant_source_sha'] if phase == 'mutant' else source['source_sha'],
                           artifact=artifact, executing=executing))
    tier = 'deploy' if row['deploy_only'] else ('source_gate' if row['surface'] == 'source_gate' else 'hermetic')
    if defect == 'hermetic-tier':
        tier = 'hermetic'
    receipt = dict(schema='fullstack-mutation-run-v1', mutation_id=row['id'], phase=phase, surface=row['surface'],
                   topology=scenario['claimed_cell'] if defect == 'other-cell' else cell, tier=tier,
                   artifact_set=row['compatible_artifact_set'], source_sha=source['source_sha'],
                   dirty_digest=scenario['dirty_digest'], patch_digest=source['patch_digest'],
                   manifest_digest=request['manifest_digest'], schedule=ref(directory, 'schedule.json', schedule),
                   oracle_digest=scenario['oracle_digest'], reader_digest=scenario['reader_digest'],
                   native_rc=native, verdict=('GREEN', 'ASSERT-FAIL')[native], assertion=assertion,
                   hit=ref(directory, 'hit.json', canonical(hit)), builds=builds, raw=ref(directory, 'native.log', log),
                   supervision=ref(directory, 'supervision.json',
                                   canonical(dict(native_rc=exit_code, joined=True, remaining=[]))))
    raw = canonical(receipt)
    if phase == 'baseline' and defect == 'malformed-receipt':
        # A broken host may emit a document the driver cannot even parse
        # (here: nesting deeper than the JSON decoder's recursion limit).
        raw = b'[' * 200000 + b']' * 200000
    (directory / 'receipt.json').write_bytes(raw)
    return exit_code


def main():
    mode, argv = (sys.argv[1], sys.argv[2:]) if len(sys.argv) > 1 else ('', [])
    modes = dict(shim=shim, host99=host99, judge99=judge99, host100=host100)
    if mode not in modes:
        return 96
    return modes[mode](argv)


if __name__ == '__main__':
    sys.exit(main())
