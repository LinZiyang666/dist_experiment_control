#!/usr/bin/env -S python3 -I -B
"""Stage/verify vendor/infra-fixture.json from actual deployment owner supply.

SIM_INFRA_COMPOSER_SUPPLY names infra-composer-supply-v1 (see the contract
module). Staging never executes a product, downloads a binary, creates lead
acceptance, or fills missing workloads with a successful stub. Live staging
requires all frozen arms, all instruments and the real accepted S7 handoff.
The explicit --allow-hermetic switch is only for isolated supplier controls.

The binding is driver-side supply; it is not installed into a product image.
Its transitive source/executable/CA/input pins are checked again by P32 before
any verb. stage-infra.sh calls verify on --skip-download, so stale driver bytes
cannot bypass the release supplier by reusing a previously built image.
origin: hot-update S9 plan §9.1 / §9.4 / P26; S9 COMPOSER.
"""
import argparse
import os
from pathlib import Path
import sys
import tempfile

LIB = Path(__file__).resolve().parents[1] / 'drills/lib'
sys.path.insert(0, str(LIB))
import infra_composer_contract as contract
import hot_update_infra_workload as workload

SOURCES = ('infra_composer_contract.py', 'hot_update_infra_composer.py', 'hot_update_infra_probe.py',
           'hot_update_infra_verbs.py', 'hot_update_infra_workload.py', 'hot_update_budget.sh',
           'transfer_observation.py', 'transfer_fixture.py', 'transfer_keeper.py', 'transfer_oracle.py',
           'transfer_fixture_manifest.py', 'transfer_mutations.py', 'infra_transfer_fixture.py')
SUPPLY = 'infra-composer-supply.json'
BINDING = 'infra-fixture.json'


def publish(path, raw):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.infra-composer-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as output:
            os.fchmod(output.fileno(), 0o600)
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def binding(supply, path):
    """Pin the entire instrument closure, not just the small Python trampoline."""
    sources = [contract.pin(LIB / name) for name in SOURCES]
    sources += [contract.pin(Path(__file__).resolve()), contract.pin(path)]
    sources += list(supply['handoff']['files'].values())
    for spec in contract.all_specs(supply):
        sources += [contract.pin(Path(spec['argv'][0]).resolve())] + spec.get('inputs', [])
    sources += [contract.pin(row['ca_file']) for row in supply['tls'].values()]
    by_path = {}
    for row in sources:
        contract.file_pin(row)
        contract.require(row['path'] not in by_path or by_path[row['path']] == row, 'conflicting_pin')
        by_path[row['path']] = row
    inputs = [by_path[name] for name in sorted(by_path)]
    # Driver entry points may use different Python installations. The staged
    # instrument interpreter is the sim profile's explicit system executable;
    # verification must not silently rebind it to its caller's interpreter.
    python = str(Path('/usr/bin/python3').resolve())
    prefix = [python, '-I', '-B', str(LIB / 'hot_update_infra_composer.py'), '--supply', str(path),
              '--supply-sha256', contract.sha(path)]

    def command(*args, source=None):
        value = dict(argv=prefix + list(args), sha256=contract.sha(python), inputs=inputs)
        if source is not None:
            for name in ('mode', 'expected_output'):
                if name in source:
                    value[name] = source[name]
        return value

    value = dict(schema='infra-binding-v1', mode=supply['mode'], handoff=supply['handoff'],
        cohort={name: command(name) for name in ('keeper', 'control')},
        commands={name: command('instrument', 'commands', name, '{logical_id}', source=spec)
                  for name, spec in supply['commands'].items()},
        readers={name: command('instrument', 'readers', name) for name in supply['readers']},
        faults={name: {action: command('instrument', 'faults', name, action) for action in spec}
                for name, spec in supply['faults'].items()},
        exercises={name: command('instrument', 'exercises', name) for name in supply['exercises']},
        configs={name: command('instrument', 'configs', name) for name in supply['configs']},
        tls={name: dict(config, reader=command('instrument', 'tls', name)) for name, config in supply['tls'].items()})
    for spec in list(value['cohort'].values()) + list(value['commands'].values()) + list(value['readers'].values()):
        workload.command(spec)
    return value


def stage(source, vendor, allow_hermetic=False):
    # Remove an old binding first: a failed restage must not leave a previously
    # valid GREEN supply discoverable after the supplier reports failure.
    vendor = Path(vendor).resolve()
    (vendor / BINDING).unlink(missing_ok=True)
    contract.require(source, 'NOT_RUN pending SIM_INFRA_COMPOSER_SUPPLY and lead-accepted S7 handoff')
    supply = contract.load(source)
    contract.require(supply['mode'] == 'live' or allow_hermetic, 'live_supply_required')
    path = vendor / SUPPLY
    publish(path, contract.canonical(supply))
    value = binding(supply, path)
    publish(vendor / BINDING, contract.canonical(value))
    verify(vendor, allow_hermetic)
    return value


def verify(vendor, allow_hermetic=False):
    vendor = Path(vendor).resolve()
    value = contract.read(vendor / BINDING)
    # Verify the saved pins before computing anything from today's source.
    # Recomputing first would bless a drifted supply by updating its digest.
    for spec in value['cohort'].values():
        workload.command(spec)
    supply = contract.load(vendor / SUPPLY)
    contract.require(supply['mode'] == 'live' or allow_hermetic, 'live_supply_required')
    contract.require(value == binding(supply, vendor / SUPPLY), 'binding_changed')
    contract.require((vendor / BINDING).read_bytes() == contract.canonical(value), 'canonical_binding')
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('stage', 'verify', 'validate'):
        command = sub.add_parser(name)
        if name != 'verify':
            command.add_argument('--supply', default=os.environ.get('SIM_INFRA_COMPOSER_SUPPLY', ''))
        if name != 'validate':
            command.add_argument('--vendor', required=True)
        command.add_argument('--allow-hermetic', action='store_true')
    args = parser.parse_args()
    try:
        if args.command == 'stage':
            stage(args.supply, args.vendor, args.allow_hermetic)
        elif args.command == 'verify':
            verify(args.vendor, args.allow_hermetic)
        else:
            contract.require(args.supply, 'NOT_RUN pending SIM_INFRA_COMPOSER_SUPPLY and lead-accepted S7 handoff')
            value = contract.load(args.supply)
            contract.require(value['mode'] == 'live' or args.allow_hermetic, 'live_supply_required')
        print('infra composer supply verified')
        return 0
    except (ValueError, OSError, KeyError, TypeError):
        print('infra composer supply: SETUP-RED missing or stale live owner/S7 handoff; '
              'set SIM_INFRA_COMPOSER_SUPPLY to the accepted deployment supply', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
