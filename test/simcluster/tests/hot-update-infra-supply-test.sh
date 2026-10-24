#!/bin/sh
# Hermetic supplier predicates only; fixture bytes are never executed.
# origin: hot-update S9 plan §3.1 / §6.4 / §9.4 / P37 / T89 / M34
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
[ "$#" -le 1 ] || { echo "usage: hot-update-infra-supply-test.sh [all|controls|installer_support|image_inputs]" >&2; exit 2; }
. "$HERE/../lib/supervisor.sh"
SUPPLY_SUPERVISOR_DIR=
trap '[ -z "$SUPPLY_SUPERVISOR_DIR" ] || rm -rf "$SUPPLY_SUPERVISOR_DIR"' EXIT
if [ -z "${SIM_UPGRADE_SUPERVISOR:-}" ]; then
    SUPPLY_SUPERVISOR_DIR=$(mktemp -d)
    SIM_UPGRADE_SUPERVISOR=$(sim_supervisor_build "$(cd "$HERE/../../.." && pwd)" "$SUPPLY_SUPERVISOR_DIR")
    export SIM_UPGRADE_SUPERVISOR
fi
[ -x "$SIM_UPGRADE_SUPERVISOR" ] || { echo "infra supply tests: shared supervisor is required" >&2; exit 2; }
"$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B - "$HERE/.." "${1:-all}" <<'PYSUPPLY'
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import stat
import struct
import subprocess
import sys
import tempfile

ROOT = Path(sys.argv[1]).resolve()
INSTALLER = ROOT.parents[1] / 'scripts/install.sh'
# Independent expected staging/build inputs, in addition to the Docker COPY
# closure. Omitting one in the supplier must fail before its own receipt is used.
# origin: hot-update S9 plan §9.4 / S9 internal review FIX-INST F6
IMAGE_BUILD_INPUTS = ('simcluster', 'remote.sh', 'local.sh', 'lib/docker.sh', 'lib/stage.sh',
                      'lib/stage-caddy.sh', 'lib/caddy-artifact.py', 'lib/stage-infra.sh',
                      'lib/infra-artifact.py', 'lib/infra-image-manifest.py', 'lib/supervisor.sh',
                      'lib/upgrade-artifacts.sh', 'lib/fullstack-artifacts.py')
IMAGE_RECEIPT = 'vendor/infra-image-inputs.json'


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def copy_sources(source):
    """Independently enumerate literal local COPY inputs, including JSON form.

    Unknown expansion/from-stage syntax is refused so a changing Dockerfile
    cannot shrink the tested input union silently. Directory members are added
    by input_rows, independently of the supplier's published manifest.
    """
    source = re.sub(r'\\\n', ' ', source)
    result = []
    for line in source.splitlines():
        match = re.match(r'^\s*COPY\s+(.+)$', line, re.I)
        if not match:
            continue
        body = match[1]
        words = json.loads(body) if body.startswith('[') else shlex.split(body)
        require(isinstance(words, list) and len(words) >= 2 and all(isinstance(word, str) for word in words), 'malformed COPY')
        while words and words[0].startswith('--'):
            require(words[0].startswith(('--chmod=', '--chown=')), 'unsupported COPY option')
            words.pop(0)
        require(len(words) >= 2, 'COPY has no source')
        for name in words[:-1]:
            path = Path(name)
            require(not path.is_absolute() and '..' not in path.parts and
                    not any(char in name for char in '$*?['), 'nonliteral/escaped COPY source')
            result.append(path.as_posix().removeprefix('./'))
    require(bool(result), 'empty COPY input union')
    return sorted(set(result))


def input_rows(context, installer, excluded=(), extra=()):
    paths = {'Dockerfile', *extra}
    for name in copy_sources((context / 'Dockerfile').read_text()):
        if name in excluded:
            continue
        path = context / name
        require(path.exists() or path.is_symlink(), 'missing COPY input: ' + name)
        paths.add(name)
        if path.is_dir() and not path.is_symlink():
            paths.update(item.relative_to(context).as_posix() for item in path.rglob('*'))
    result = []
    for name, path in [('installer', installer)] + [(name, context / name) for name in sorted(paths)]:
        info = path.lstat()
        kind = 'symlink' if stat.S_ISLNK(info.st_mode) else 'directory' if stat.S_ISDIR(info.st_mode) else 'file'
        require(kind != 'file' or stat.S_ISREG(info.st_mode), 'unsupported COPY file type')
        result.append(dict(path=name, type=kind, mode=stat.S_IMODE(info.st_mode),
                           size=info.st_size if kind == 'file' else 0,
                           sha256=digest(path) if kind == 'file' else '-',
                           link_target=os.readlink(path) if kind == 'symlink' else '-'))
    return sorted(result, key=lambda row: row['path'])


def function(text, name):
    match = re.search(r'^' + name + r'\(\) \{\n.*?^}\n', text, re.M | re.S)
    require(match is not None, 'missing installer validator: ' + name)
    return match.group()


def installer_support():
    # Invoke only real read-only validators extracted from the installer.
    # Nothing sources its dispatch, installs a file, or starts a version probe.
    source = INSTALLER.read_text()
    body = ''.join(function(source, name) for name in
                   ('infra_safe_path', 'verify_sha', 'validate_infra_elf', 'validate_infra_support'))
    body = 'set -eu\nDRY_RUN=0; VERSION=fixture; ARCH=amd64\ndie() { echo "$*" >&2; exit 1; }\n' + body
    body += '\nvalidate_infra_support "$1"\n'
    with tempfile.TemporaryDirectory(prefix='infra-support-controls-') as directory:
        root = Path(directory)
        header = bytearray(64)
        header[:7] = b'\x7fELF\x02\x01\x01'
        struct.pack_into('<HH', header, 16, 2, 62)
        names = ['tether-infra-helper', 'tether-infra-launch', 'infra-catalog.json']
        for name in names:
            (root / name).write_bytes(bytes(header) + name.encode() if name != 'infra-catalog.json' else b'{"fixture":true}\n')
            (root / name).chmod(0o755 if name != 'infra-catalog.json' else 0o644)
        manifest = ('format 1\nrelease fixture\ngoos linux\ngoarch amd64\nprotocol 1\nprevious-reader 1\n'
                    'source-sha256 ' + 'a' * 64 + '\n' +
                    ''.join('sha256 ' + digest(root / name) + ' ' + name + '\n' for name in names))
        (root / 'infra-support.manifest').write_text(manifest)

        def run():
            return subprocess.run([os.environ['SIM_UPGRADE_SUPERVISOR'], '--', 'sh', '-c', body, 'support-control', str(root)],
                                  capture_output=True, text=True, timeout=30)

        good = run()
        require(good.returncode == 0, 'valid support fixture rejected: ' + good.stderr)
        for name in names + ['infra-support.manifest']:
            path = root / name
            original = path.read_bytes()
            path.unlink()
            require(run().returncode == 1, 'missing offline support accepted: ' + name)
            path.write_bytes(original)
            path.chmod(0o755 if name in names[:2] else 0o644)
            path.write_bytes(original + b'stale')
            require(run().returncode == 1, 'stale offline input accepted: ' + name)
            path.write_bytes(original)
        for name in names[:2]:
            path = root / name
            path.chmod(0o644)
            require(run().returncode == 1, 'nonexecutable support accepted')
            path.chmod(0o777)
            require(run().returncode == 1, 'writable support accepted')
            path.chmod(0o755)
            original = path.read_bytes()
            path.write_bytes(original[:18] + b'\xb7\x00' + original[20:])
            changed = manifest.replace(hashlib.sha256(original).hexdigest(), digest(path))
            (root / 'infra-support.manifest').write_text(changed)
            require(run().returncode == 1, 'hash-valid wrong-platform support accepted')
            path.write_bytes(original)
            (root / 'infra-support.manifest').write_text(manifest)
        for changed in (manifest.replace('protocol 1', 'protocol 2'), manifest.replace('goarch amd64', 'goarch arm64'),
                        manifest + 'protocol 1\n', manifest.replace('previous-reader 1\n', '')):
            (root / 'infra-support.manifest').write_text(changed)
            require(run().returncode == 1, 'wrong/duplicate/missing support manifest field accepted')
        (root / 'infra-support.manifest').write_text(manifest)
        require(run().returncode == 0, 'support controls did not restore baseline')
    print('infra supply installer_support: PASS (read-only offline hash/platform/presence/mode controls)')


def controls():
    require(copy_sources('COPY a b /target\nCOPY ["c", "/other"]\n') == ['a', 'b', 'c'], 'COPY reader lost a source')
    for source in ('', 'COPY ../escape /target\n', 'COPY $DYNAMIC /target\n', 'COPY --from=build a /target\n'):
        try:
            copy_sources(source)
        except ValueError:
            pass
        else:
            raise ValueError('unsupported COPY source accepted')
    with tempfile.TemporaryDirectory(prefix='infra-copy-controls-') as directory:
        root = Path(directory)
        (root / 'Dockerfile').write_text('COPY inputs /target\n')
        (root / 'inputs').mkdir()
        (root / 'inputs/file').write_bytes(b'input')
        (root / 'inputs/link').symlink_to('file')
        installer = root / 'install.sh'
        installer.write_bytes(b'installer')
        good = input_rows(root, installer)
        require({row['path'] for row in good} == {'Dockerfile', 'installer', 'inputs', 'inputs/file', 'inputs/link'}, 'COPY closure incomplete')
        path = root / 'inputs/file'
        for change in ('content', 'mode', 'type'):
            original, mode = path.read_bytes(), path.stat().st_mode & 0o777
            if change == 'content':
                path.write_bytes(original + b'changed')
            elif change == 'mode':
                path.chmod(mode ^ 0o100)
            else:
                path.unlink()
                path.symlink_to('../install.sh')
            require(input_rows(root, installer) != good, 'independent input reader misses ' + change)
            path.unlink()
            path.write_bytes(original)
            path.chmod(mode)
    print('infra supply controls: PASS (COPY closure/content/type/mode)')


def image_inputs():
    path = ROOT / 'lib/infra-image-manifest.py'
    spec = importlib.util.spec_from_file_location('infra_image_manifest', path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    # Use the supplier's actual source and final-target operations. These read
    # fixture bytes only; there is no Docker build or candidate execution. The
    # receipt COPY is excluded from its own hash, as required by §9.4.
    # origin: hot-update S9 plan §9.4 / S9 internal review FIX-INST F6
    for name in ('snapshot', 'write', 'compare_report', 'verify_targets'):
        require(callable(getattr(module, name, None)), 'P26 image operation is missing: ' + name)
    require(module.BUILD_INPUTS == IMAGE_BUILD_INPUTS, 'staging/build source inventory changed')
    require(module.MANIFEST_SOURCE == IMAGE_RECEIPT, 'self-hashing receipt exclusion changed')
    dockerfile = (ROOT / 'Dockerfile').read_text()
    for token in ('tether-infra-helper', 'tether-infra-launch', 'infra-image-manifest', 'nftables', 'conntrack'):
        require(token in dockerfile, 'infra image supply missing ' + token)
    with tempfile.TemporaryDirectory(prefix='infra-image-inputs-') as directory:
        context = Path(directory)
        (context / 'Dockerfile').write_text(dockerfile)
        for name in copy_sources(dockerfile):
            if name in (IMAGE_RECEIPT, 'Dockerfile'):
                continue
            path = context / name
            path.parent.mkdir(parents=True, exist_ok=True)
            require(not path.exists(), 'duplicate COPY fixture path')
            if name == 'vendor/infra':
                path.mkdir()
                # Remote contexts carry a supplier source receipt; the local
                # checkout guard is separate from these COPY/target controls.
                build_source = dict(schema_version=1, inputs=[dict(path='build/infra-support.py',
                                    sha256=digest(INSTALLER.parent.parent / 'build/infra-support.py'))])
                (path / 'build-source.json').write_bytes(module.canonical(build_source))
                (path / 'nested').mkdir()
                (path / 'nested/catalog.json').write_bytes(b'{"fixture":true}\n')
            else:
                path.write_bytes(INSTALLER.read_bytes() if name == 'vendor/install.sh' else
                                 ('never-executed supply fixture: ' + name).encode())
                path.chmod(0o755)
        for name in IMAGE_BUILD_INPUTS:
            path = context / name
            if not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(('never-executed build fixture: ' + name).encode())
                path.chmod(0o755)
        installer = context / 'vendor/install.sh'
        manifest = module.snapshot(context)
        independent = input_rows(context, installer, excluded=(IMAGE_RECEIPT,), extra=IMAGE_BUILD_INPUTS)
        expected = [dict(path=row['path'], type=row['type'], mode=row['mode'], size=row['size'],
                         sha256=row['sha256'] if row['type'] == 'file' else '')
                    for row in independent if row['path'] != 'installer']
        require(manifest['sources'] == expected, 'manifest differs from independent COPY/build closure')
        require(not any(row['source'] == IMAGE_RECEIPT for row in manifest['copies']), 'receipt hashes itself')
        receipt = context / IMAGE_RECEIPT
        module.write(receipt, manifest)
        module.compare_report(context, receipt)
        image = context / 'image-root'
        for row in manifest['copies']:
            target = image / row['target'].lstrip('/')
            target.parent.mkdir(parents=True, exist_ok=True)
            if row['type'] == 'directory':
                target.mkdir(exist_ok=True)
            else:
                shutil.copyfile(context / row['source'], target)
            target.chmod(row['mode'])
        image_dockerfile = image / 'opt/sim/Dockerfile'
        module.verify_targets(manifest, image, image_dockerfile)

        def stale(path, changes, verify, label):
            # Preserve complete directory contents between independent defects.
            # Backups stay outside the COPY context and never become inputs.
            with tempfile.TemporaryDirectory(prefix='infra-stale-backup-') as saved:
                backup = Path(saved) / 'original'
                directory = path.is_dir()
                if directory:
                    shutil.copytree(path, backup)
                else:
                    shutil.copy2(path, backup)
                for change in changes:
                    if change == 'content':
                        if directory:
                            (path / 'extra-source').write_bytes(b'stale directory member')
                        else:
                            path.write_bytes(path.read_bytes() + b'changed')
                    elif change == 'mode':
                        path.chmod(stat.S_IMODE(path.stat().st_mode) ^ 0o100)
                    else:
                        shutil.rmtree(path) if directory else path.unlink()
                        if change == 'type':
                            path.symlink_to('/dev/null')
                    try:
                        verify()
                    except (OSError, ValueError):
                        pass
                    else:
                        raise ValueError('stale image input/target accepted: ' + label + '/' + change)
                    finally:
                        if path.is_symlink() or path.is_file():
                            path.unlink()
                        elif path.exists():
                            path.chmod(stat.S_IMODE(backup.stat().st_mode) | 0o700)
                            shutil.rmtree(path)
                        if directory:
                            shutil.copytree(backup, path)
                        else:
                            shutil.copy2(backup, path)
                    verify()

        for row in manifest['sources']:
            stale(context / row['path'], ('content', 'mode', 'type', 'missing'),
                  lambda: module.compare_report(context, receipt), row['path'])
        for row in manifest['copies']:
            changes = ('mode', 'type', 'missing') if row['type'] == 'directory' else ('content', 'mode', 'type', 'missing')
            stale(image / row['target'].lstrip('/'), changes,
                  lambda: module.verify_targets(manifest, image, image_dockerfile), row['target'])
        # Neither omitted nor invented inventory rows can certify the image.
        for section in ('sources', 'copies'):
            for change in ('missing', 'extra'):
                bad = json.loads(json.dumps(manifest))
                if change == 'missing':
                    bad[section].pop()
                else:
                    bad[section].append(dict(bad[section][0]))
                module.write(receipt, bad)
                try:
                    module.compare_report(context, receipt)
                except (OSError, ValueError):
                    pass
                else:
                    raise ValueError('changed image inventory accepted: ' + section + '/' + change)
        module.write(receipt, manifest)
        module.compare_report(context, receipt)
    print('infra supply image_inputs: PASS (%d sources and %d targets independently stale; receipt excluded)' %
          (len(manifest['sources']), len(manifest['copies'])))


def main():
    selection = sys.argv[2]
    require(selection in ('all', 'controls', 'installer_support', 'image_inputs'), 'unknown infra supply selection')
    if selection in ('all', 'controls'):
        controls()
    if selection in ('all', 'installer_support'):
        installer_support()
    if selection in ('all', 'image_inputs'):
        missing = [name for name in ('lib/stage-infra.sh', 'lib/infra-artifact.py', 'lib/infra-image-manifest.py')
                   if not (ROOT / name).is_file()]
        if missing:
            print('NOT_RUN pending P26: ' + ', '.join(missing), file=sys.stderr)
            return 2
        image_inputs()
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, TypeError, subprocess.SubprocessError) as error:
        print('infra supply tests: ' + str(error), file=sys.stderr)
        sys.exit(1)
PYSUPPLY
