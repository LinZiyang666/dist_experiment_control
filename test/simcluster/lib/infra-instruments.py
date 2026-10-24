#!/usr/bin/env -S python3 -I -B
"""Assemble host-local live instruments and freeze their product pacing.

Usage: infra-instruments.py assemble --deployment FILE --output FILE [--vendor DIR]
                             [--owner-plan FILE]
The deployment is infra-instruments-v1: handoff, transfer_manifest (a file pin), seconds (every closed arm),
owners {prepare, ready, workloads}, commands, readers, tls, faults, exercises,
configs. Executables are actual deployment instruments in the composer's
{argv,sha256,inputs} format; this tool does not supply missing implementations,
mint lead acceptance, run a product or manufacture observations. All four owner
lifecycles and every independent instrument are mandatory before publication.

--owner-plan compiles infra-owner-plan-v1 with the real local flow/PTY/exec
adapters and independently pinned native collector. It fills the owner ports
below without changing their frozen {argv,sha256,inputs} ABI. Missing native
deployment facts remain named NOT_RUN prerequisites, never successful stubs.

The optional vendor argument uses P26's existing composer staging hook. A
generated file can also be passed to sim_stage_infra via the documented
SIM_INFRA_COMPOSER_SUPPLY. Host paths and input bytes must remain unchanged.
No source rebinding after remote relocation or successful fallback is allowed.

origin: hot-update S9 plan §9.1 / §9.4 / LQ3 / P19/P26/P30; S9 INSTRUMENTS.
"""
import argparse
import copy
import importlib.util
import os
from pathlib import Path
import sys
import tempfile

LIB = Path(__file__).resolve().parents[1] / 'drills/lib'
sys.path.insert(0, str(LIB))
import infra_composer_contract as contract
import infra_transfer_fixture as transfers

TABLES = ('commands', 'readers', 'tls', 'faults', 'exercises', 'configs')


def transfer_specs(source, context):
    """Concrete S7 foreground keeper/stop instruments, not an owner double."""
    contract.file_pin(source)
    manifest = transfers.freeze(contract.read(source['path']), context)
    # Reuse the whole S7 admission reader, including isolation/canonical-field
    # checks. Previewing a template must not silently replace that validator
    # with this supplier's narrower pacing/owner projection.
    import transfer_fixture as s7
    with tempfile.NamedTemporaryFile(prefix='infra-transfer-preview-') as preview:
        preview.write(contract.canonical(manifest))
        preview.flush()
        manifest = s7.read_manifest(preview.name)
    contract.require(manifest.get('consumer') == transfers.CONSUMER, 'infra_transfer_consumer')
    inputs = [source]
    for name in ('infra_transfer_fixture.py', 'transfer_fixture.py', 'transfer_keeper.py',
                 'transfer_observation.py', 'transfer_oracle.py', 'transfer_fixture_manifest.py',
                 'transfer_mutations.py', 'hot_update_infra_verbs.py', 'hot_update_infra_probe.py',
                 'hot_update_infra_workload.py', 'hot_update_budget.sh'):
        inputs.append(contract.pin(LIB / name))
    # Every original local command must be a prebuilt, SHA-admitted product.
    # A target path or production-home launch cannot hide inside a JSON pin.
    for process in manifest['processes']:
        launch = process.get('launch', {})
        argv = launch.get('argv', [])
        home = Path(launch.get('home', ''))
        contract.require(argv and Path(argv[0]).is_absolute() and home.is_absolute() and home != Path.home()
                         and '.tether' not in home.parts and Path(argv[0]).name.startswith('tether') and
                         (Path(argv[0]).name != 'tether' or 'vendor' in Path(argv[0]).parts),
                         'transfer_product_launch')
        binary = contract.pin(argv[0])
        with open(argv[0], 'rb') as stream:
            elf = stream.read(4) == b'\x7fELF'
        contract.require(elf and os.access(argv[0], os.X_OK) and
                         binary['sha256'] in process['artifact_shas'], 'transfer_product_sha')
        if process.get('role') == 'ctl':
            transfer = next((t for t in manifest['transfers'] if t['ctl_producer'] == process['label']), None)
            index = argv.index('--rate-limit') + 1 if '--rate-limit' in argv else len(argv)
            contract.require(transfer is not None and transfer['cohort'].split('-')[1] in argv and
                             argv.count('--rate-limit') == 1 and
                             index < len(argv) and argv[index] == str(transfer['rate_limit']),
                             'transfer_product_rate')
        inputs.append(binary)
    control = manifest['control']['launch']
    argv = control['argv']
    home = Path(control['home'])
    contract.require(home.is_absolute() and home != Path.home() and '.tether' not in home.parts,
                     'transfer_control_home')
    control_inputs = [contract.pin(arg) for arg in argv[1:] if Path(arg).is_absolute() and
                      '/.tether/' not in arg and Path(arg).is_file()]
    contract.spec(dict(argv=argv, sha256=contract.sha(argv[0]), inputs=control_inputs))
    inputs += [contract.pin(argv[0])] + control_inputs
    python = str(Path('/usr/bin/python3').resolve())
    prefix = [python, '-I', '-B', str(LIB / 'infra_transfer_fixture.py')]
    return {verb: dict(argv=prefix + [verb, '--source', source['path']],
                       sha256=contract.sha(python), inputs=inputs) for verb in ('keeper', 'stop')}


def assemble(deployment):
    """Reject missing/stale ports before producing a discoverable live supply."""
    contract.require(type(deployment) is dict and set(deployment) ==
        {'schema', 'handoff', 'transfer_manifest', 'seconds', 'owners', *TABLES} and
        deployment['schema'] == 'infra-instruments-v1', 'instrument_deployment_schema')
    contract.require(type(deployment['seconds']) is dict and
        set(deployment['seconds']) == contract.profile_keys(), 'instrument_arm_set')
    owners = deployment['owners']
    contract.require(type(owners) is dict and set(owners) == {'prepare', 'ready', 'workloads'},
                     'instrument_owners')
    contract.require(set(owners['workloads']) == {'flow', 'pty', 'exec'}, 'instrument_owner_set')
    owners = copy.deepcopy(owners)
    profiles = {}
    for key, seconds in deployment['seconds'].items():
        component, topology, profile = key.split(':')
        context = dict(component=component, topology=topology, profile=profile,
                       seconds=seconds, rates=transfers.rates(seconds))
        row = dict(seconds=seconds, rates=context['rates'], **copy.deepcopy(owners))
        row['workloads']['transfers'] = transfer_specs(deployment['transfer_manifest'], context)
        profiles[key] = row
    value = dict(schema='infra-composer-supply-v1', mode='live',
        handoff=copy.deepcopy(deployment['handoff']), profiles=profiles,
        **{key: copy.deepcopy(deployment[key]) for key in TABLES})
    # Validate the actual executable/input bytes now. Neither an existing
    # output file nor a successful prior arm can supply an omitted port.
    return contract.validate(value)


def write(output, value):
    output = Path(output).absolute()
    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, pending = tempfile.mkstemp(prefix='.infra-instruments-', dir=output.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(contract.canonical(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(pending, output)
    finally:
        Path(pending).unlink(missing_ok=True)


def stage(deployment, output, vendor=None, owner_plan=None):
    output = Path(output).absolute()
    # A stale supply must not survive a refused restage. Do not remove the
    # input itself when a caller accidentally selects the same path twice.
    contract.require(output != Path(deployment).absolute() and
        (owner_plan is None or output != Path(owner_plan).absolute()), 'instrument_input_output_alias')
    output.unlink(missing_ok=True)
    if vendor is not None:
        (Path(vendor) / 'infra-fixture.json').unlink(missing_ok=True)
    declaration = contract.read(deployment)
    if owner_plan is not None:
        import infra_owner_plan
        ports = infra_owner_plan.supply(owner_plan)
        declaration.update(ports)
    value = assemble(declaration)
    write(output, value)
    if vendor is not None:
        spec = importlib.util.spec_from_file_location('instrument_composer_supplier',
            Path(__file__).with_name('infra-composer.py'))
        composer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(composer)
        composer.stage(output, vendor)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    # Keep the existing flag-only caller while providing the explicit assembly
    # entry used by P26. Both paths run the same fail-closed publication code.
    parser.add_argument('operation', nargs='?', choices=('assemble',), default='assemble')
    parser.add_argument('--deployment', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--vendor')
    parser.add_argument('--owner-plan')
    args = parser.parse_args()
    try:
        stage(args.deployment, args.output, args.vendor, args.owner_plan)
        print('live infra instrument supply assembled')
        return 0
    except (ValueError, OSError, KeyError, TypeError) as error:
        reason = str(error) if type(error) is ValueError and str(error).startswith('NOT_RUN ') else 'missing, changed or unsupported deployment instruments'
        print('infra instruments: SETUP-RED ' + reason, file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
