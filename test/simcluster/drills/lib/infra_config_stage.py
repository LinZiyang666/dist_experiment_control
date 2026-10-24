#!/usr/bin/env -S python3 -I -B
"""Stage public configuration into the catalog actually opened by the root launcher.

The host hook uses the existing bounded transport and registered remote owner.
Its remote half is self-contained stdlib code: no installed tether, shell,
download, service restart or product-owned JSON state is used. --install is
deployment-only and requires root; hermetic tests call install_at on temp dirs.
origin: hot-update S9 plan §6 / §9.4 / P26; S9 INSTRUMENTS-4.
"""
import argparse
import hashlib
import os
from pathlib import Path
import re
import secrets
import stat
import sys

CONFIG_ROOT = '/var/lib/tether-infra/configs'
MAX_BYTES = 4 << 20


def need(ok, reason):
    if not ok:
        raise ValueError('NOT_RUN configs: ' + reason)


def directory(path):
    path = Path(path)
    need(path.is_absolute() and '..' not in path.parts, 'absolute_catalog_required')
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in path.parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
            os.close(fd)
            fd = child
        result, fd = fd, None
        return result
    finally:
        if fd is not None:
            os.close(fd)


def public_bytes(fd):
    info = os.fstat(fd)
    need(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= MAX_BYTES, 'bounded_regular_config_required')
    raw = os.pread(fd, MAX_BYTES + 1, 0)
    after = os.fstat(fd)
    need(len(raw) == info.st_size and (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns) ==
         (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns), 'config_read_raced')
    return raw


def catalog_fd(root, owner):
    fd = directory(root)
    try:
        info = os.fstat(fd)
        need(info.st_uid == owner and stat.S_IMODE(info.st_mode) == 0o700, 'private_product_catalog_required')
        return fd
    except BaseException:
        os.close(fd)
        raise


def install_at(root, source, digest, owner):
    """Publish once without replacing a conflicting existing catalog entry."""
    need(type(digest) is str and re.fullmatch('[0-9a-f]{64}', digest), 'config_digest_required')
    need(Path(source).is_absolute(), 'absolute_config_source_required')
    source_fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        raw = public_bytes(source_fd)
        need(hashlib.sha256(raw).hexdigest() == digest, 'source_sha_changed')
    finally:
        os.close(source_fd)
    fd = catalog_fd(root, owner)
    temporary = '.pending-' + secrets.token_hex(16)
    pending = False
    try:
        try:
            existing = os.open(digest, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
        except FileNotFoundError:
            existing = None
        if existing is not None:
            try:
                info = os.fstat(existing)
                need(info.st_uid == owner and info.st_nlink == 1 and stat.S_IMODE(info.st_mode) == 0o400 and
                     public_bytes(existing) == raw, 'catalog_conflict')
            finally:
                os.close(existing)
            os.fsync(fd)
            return
        target = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                         0o400, dir_fd=fd)
        pending = True
        try:
            offset = 0
            while offset < len(raw):
                n = os.write(target, raw[offset:])
                need(n > 0, 'config_write_stalled')
                offset += n
            os.fchmod(target, 0o400)
            os.fsync(target)
        finally:
            os.close(target)
        # link is no-replace even if another publisher wins after our check.
        # A competing/conflicting catalog entry is never overwritten by rename.
        os.link(temporary, digest, src_dir_fd=fd, dst_dir_fd=fd, follow_symlinks=False)
        os.unlink(temporary, dir_fd=fd)
        pending = False
        os.fsync(fd)
    finally:
        if pending:
            os.unlink(temporary, dir_fd=fd)
        os.close(fd)


def spec(source):
    import infra_composer_contract as contract
    lib = Path(__file__).resolve().parent
    python = str(Path('/usr/bin/python3').resolve())
    value = read_plan(source, contract.sha(source))
    inputs = [contract.pin(source), value['supervisor'], value['sim'], *value['configs'].values()]
    inputs += [contract.pin(path) for folder in (lib, lib.parents[1] / 'lib')
               for path in sorted(folder.iterdir()) if path.is_file() and path.suffix in ('.py', '.sh')]
    return dict(argv=[python, '-I', '-B', str(Path(__file__).resolve()), '--plan', str(Path(source).absolute()),
                     '--plan-sha256', contract.sha(source)], sha256=contract.sha(python), inputs=inputs)


def read_plan(source, digest):
    import infra_composer_contract as contract
    import infra_owner_plan as plans
    plans.pin(dict(path=str(source), sha256=digest), 'configs')
    value = contract.read(source)
    return validate_plan(value)


def validate_plan(value):
    import infra_owner_plan as plans
    # A separate staging plan keeps pins acyclic: the full owner plan pins its
    # deployment, which pins this hook. This plan must not pin either parent.
    need(type(value) is dict and set(value) == {'schema', 'supervisor', 'flow', 'sim', 'nodes',
         'observers', 'configs', 'config_root'} and value['schema'] == 'infra-config-stage-plan-v1',
         'staging_plan_required')
    plans.pin(value['supervisor'], 'configs', True)
    plans.pin(value['sim'], 'configs', True)
    need(value['config_root'] == CONFIG_ROOT and type(value['nodes']) is dict and value['nodes'] and
         set(value['nodes']) <= {'brk1', 'brk2', 'brk3'}, 'product_catalog_nodes_required')
    need(value['flow'] == dict(here=str(plans.LIB.parents[1]), sim=value['sim']), 'staging_transport_binding')
    need(type(value['observers']) is dict and set(value['observers']) == set(value['nodes']),
         'registered_observers_required')
    for row in value['observers'].values():
        need(type(row) is dict and set(row) == {'root', 'cgroup'} and
             all(type(row[key]) is str and row[key].startswith('/') and '..' not in Path(row[key]).parts
                 for key in ('root', 'cgroup')), 'registered_observer_scope_required')
    need(type(value['configs']) is dict and set(value['configs']) == {'valid', 'invalid-config', 'invalid-cert'},
         'three_public_configs_required')
    for row in value['configs'].values():
        plans.pin(row, 'configs')
    return value


def create_plan(value, destination):
    """Seal actual staging inputs before any parent deployment pins the hook.

    This creates a recipe, never a staging receipt. Validation reads every
    supplied executable/config pin; missing provisioning leaves no plan file.
    origin: hot-update S9 plan §9.4 / P26; S9 INSTRUMENTS-5.
    """
    from infra_deployment_plan import publish
    validate_plan(value)
    publish(destination, value)


def stage(plan, config, ctx, variant, source, digest):
    import infra_composer_contract as contract
    import infra_native as native
    import infra_owner_plan as plans
    from infra_owner_workloads import module
    from types import SimpleNamespace
    expected = plan.get('configs', {}).get(variant, {})
    plans.pin(expected, 'configs.' + variant)
    need(expected == dict(path=str(source), sha256=digest), 'config_argument_binding')
    need(config.get('config_root') == CONFIG_ROOT, 'product_config_catalog_required')
    need(ctx.get('nodes') and set(ctx['nodes']) <= set(config.get('nodes', {})), 'node_supply_required')
    # The source bytes are public pinned fixture input; no credential, key or
    # sealed capsule is transported in a config argument or ordinary file.
    script = Path(__file__).resolve().read_text()
    transport = module('flow_fixture').Transport(SimpleNamespace(here=Path(plan['flow']['here']),
        instance=ctx['instance'], sim=config['sim']['path']))
    config = dict(config, _context=ctx)
    for node in ctx['nodes']:
        command = ['/usr/bin/python3', '-I', '-B', '-c', script]
        # Check the actual selected root directory before copy-to has an effect.
        native.remote(plan, config, node, command + ['--check'])
        temporary = CONFIG_ROOT + '/.source-' + secrets.token_hex(16)
        try:
            transport.run('copy-to', source, node, temporary, timeout=30)
            native.remote(plan, config, node, command + ['--install', '--source', temporary,
                          '--source-sha256', digest], timeout=30)
        finally:
            native.remote(plan, config, node, ['/usr/bin/rm', '-f', '--', temporary])
        # Independent on-target readback, after the installing process exits.
        raw = native.remote(plan, config, node, ['/usr/bin/sha256sum', CONFIG_ROOT + '/' + digest])
        need(raw.split() and raw.split()[0].decode() == digest, 'product_catalog_readback_sha')
    return dict(variant=variant, staged=True, sha256=digest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--install', action='store_true')
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--source', type=Path)
    parser.add_argument('--source-sha256')
    parser.add_argument('--variant', choices=('valid', 'invalid-config', 'invalid-cert'))
    parser.add_argument('--plan', type=Path)
    parser.add_argument('--plan-sha256')
    parser.add_argument('--context', type=Path)
    parser.add_argument('--evidence')
    args = parser.parse_args()
    try:
        if args.install or args.check:
            need(os.geteuid() == 0 and args.install != args.check and args.plan is None and
                 args.context is None and args.variant is None, 'root_catalog_operation_required')
            if args.check:
                os.close(catalog_fd(CONFIG_ROOT, 0))
            else:
                install_at(CONFIG_ROOT, args.source, args.source_sha256, 0)
            return 0
        # Isolated Python excludes the script directory. The remote --check /
        # --install half exits above without importing any host-only sibling.
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import infra_owner_ports as ports
        import infra_composer_contract as contract
        need(args.plan is not None and args.context is not None and args.variant is not None,
             'staging_context_required')
        plan = read_plan(args.plan, args.plan_sha256)
        ctx = ports.context(args.context)
        ports.supervised(os.getpid(), plan['supervisor'], 'configs.' + args.variant)
        result = stage(plan, plan, ctx, args.variant, args.source, args.source_sha256)
        sys.stdout.buffer.write(contract.canonical(result))
        return 0
    except (ValueError, OSError, KeyError, TypeError):
        print('NOT_RUN configs: product_catalog_staging_failed', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
