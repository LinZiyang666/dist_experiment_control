#!/usr/bin/env -S python3 -I -B
"""Seal acyclic deployment producer recipes and invoke them under FIX-ORPHAN.

The producer plan contains actual transport/physical inputs and pinned export
commands, never expected observations or a parent owner-plan pin. Product
exports must already exist; building these recipes does not implement them.
No executable is discovered on PATH and no remote command runs at assembly.
origin: hot-update S9 plan §9.1 / §9.4 / L22; S9 INSTRUMENTS-5.
"""
import argparse
import os
from pathlib import Path
import secrets
import stat
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import infra_composer_contract as contract
import infra_owner_plan as plans

FIELDS = {'schema', 'supervisor', 'product', 'flow', 'sim', 'physical', 'nodes', 'observers',
          'phase', 'nats_monitors', 'fault_events', 'fault_actions', 'final_sources'}


def publish(destination, value):
    """Fsync a no-replace, private immutable recipe, including repeat admission."""
    import infra_config_stage as stage
    destination = plans.path(str(destination), 'deployment')
    raw = contract.canonical(value)
    plans.need(len(raw) <= 4 << 20, 'deployment', 'bounded_plan_required')
    fd = stage.directory(destination.parent)
    temporary = '.producer-plan-' + secrets.token_hex(16)
    pending = False
    try:
        info = os.fstat(fd)
        plans.need(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700,
                   'deployment', 'private_plan_directory_required')
        try:
            saved = os.open(destination.name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=fd)
        except FileNotFoundError:
            saved = None
        if saved is not None:
            try:
                info = os.fstat(saved)
                plans.need(info.st_uid == os.geteuid() and info.st_nlink == 1 and
                    stat.S_IMODE(info.st_mode) == 0o400 and stage.public_bytes(saved) == raw,
                    'deployment', 'immutable_plan_conflict')
            finally:
                os.close(saved)
            os.fsync(fd)
            return
        target = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
                         0o400, dir_fd=fd)
        pending = True
        try:
            offset = 0
            while offset < len(raw):
                size = os.write(target, raw[offset:])
                plans.need(size > 0, 'deployment', 'plan_write_stalled')
                offset += size
            os.fchmod(target, 0o400)
            os.fsync(target)
        finally:
            os.close(target)
        os.link(temporary, destination.name, src_dir_fd=fd, dst_dir_fd=fd, follow_symlinks=False)
        os.unlink(temporary, dir_fd=fd)
        pending = False
        os.fsync(fd)
    finally:
        if pending:
            os.unlink(temporary, dir_fd=fd)
        os.close(fd)


def specifications(value):
    if type(value) is dict:
        if 'argv' in value:
            yield contract.spec(value)
        else:
            for child in value.values():
                yield from specifications(child)
    elif type(value) is list:
        for child in value:
            yield from specifications(child)


def read_plan(source, digest):
    plans.pin(dict(path=str(source), sha256=digest), 'deployment')
    value = contract.read(source)
    return validate_plan(value)


def validate_plan(value):
    plans.need(type(value) is dict and set(value) == FIELDS and
        value['schema'] == 'infra-deployment-producers-v1', 'deployment', 'producer_plan_required')
    for key in ('supervisor', 'sim', 'physical'):
        plans.pin(value[key], 'deployment', True)
    plans.product(value, 'deployment')
    plans.need(value['flow'] == dict(here=str(plans.LIB.parents[1]), sim=value['sim']),
               'deployment', 'transport_binding')
    plans.need(type(value['nodes']) is dict and value['nodes'] and
        set(value['observers']) == set(value['nodes']), 'deployment', 'registered_nodes_required')
    for binding in value['observers'].values():
        plans.need(type(binding) is dict and set(binding) == {'root', 'cgroup'}, 'deployment', 'observer_scope_required')
        for path in binding.values():
            plans.path(path, 'deployment')
            plans.need('..' not in Path(path).parts, 'deployment', 'observer_scope_required')
    for key in ('phase', 'fault_events', 'fault_actions', 'final_sources'):
        list(specifications(value[key]))
    # A wrapper's existence cannot hide a missing material producer until LIVE.
    # Admit the graph before sealing it and repeat immediately before execution.
    from infra_deployment_faults import COMPOUND
    from infra_deployment_final import admit
    plans.need(type(value['phase']) is dict and set(value['phase']) == {'events', 'root', 'transaction'},
               'deployment', 'original_phase_and_journal_exports_required')
    for source in value['phase'].values():
        contract.spec(source)
    plans.need(type(value['nats_monitors']) is dict and
        set(value['nats_monitors']) == {node for node in value['nodes'] if node.startswith('brk')},
        'deployment', 'actual_broker_monitors_required')
    plans.need(type(value['fault_events']) is dict and set(value['fault_events']) == set(COMPOUND),
               'deployment', 'all_compound_injector_exports_required')
    for source in value['fault_events'].values():
        contract.spec(source)
    plans.need(type(value['fault_actions']) is dict and set(value['fault_actions']) == {'nats-n2', *COMPOUND},
               'deployment', 'all_native_fault_mechanisms_required')
    for actions in value['fault_actions'].values():
        plans.need(type(actions) is dict and set(actions) == {'install', 'heal'}, 'deployment', 'both_native_fault_actions_required')
        for action in actions.values():
            contract.spec(action)
            plans.need(action['argv'][0] == value['sim']['path'], 'deployment', 'real_deployment_transport_required')
    admit(value['final_sources'])
    return value


def create_plan(value, destination):
    validate_plan(value)
    publish(destination, value)


def spec(source, *operation):
    source = Path(source).absolute()
    value = read_plan(source, contract.sha(source))
    python = str(Path('/usr/bin/python3').resolve())
    inputs = [contract.pin(source), value['supervisor'], value['sim'], value['physical'], value['product']['binary']]
    for executable in specifications(value):
        inputs.extend([contract.pin(executable['argv'][0]), *executable.get('inputs', [])])
    # Dynamic existing transports import siblings. Bind the real closure, so a
    # changed parser/helper invalidates admission before the next remote call.
    for directory in (plans.LIB, plans.LIB.parents[1] / 'lib'):
        inputs.extend(contract.pin(p) for p in sorted(directory.iterdir())
                      if p.is_file() and p.suffix in ('.py', '.sh'))
    by_path = {pin['path']: pin for pin in inputs}
    return dict(argv=[python, '-I', '-B', str(Path(__file__).resolve()), '--plan', str(source),
                     '--plan-sha256', contract.sha(source), *operation], sha256=contract.sha(python),
                inputs=[by_path[path] for path in sorted(by_path)])


def lane_specs(cohorts, hop, lost_ack_proof):
    import infra_s7_lanes as lanes
    plans.need(type(cohorts) is dict and set(cohorts) == set(lanes.LANES) and
        len(set(cohorts.values())) == 4 and set(cohorts.values()) == set(contract.probe.COHORTS),
        's7.phase_lanes', 'four_original_cohorts_required')
    plans.need(type(hop) is str and hop.startswith('hop-') and hop[4:].isdigit() and
               hop == 'hop-' + str(int(hop[4:])), 's7.phase_lanes', 'observed_hop_required')
    proof = plans.path(str(lost_ack_proof), 's7.phase_lanes')
    result = {lane: lanes.spec(lane, cohort, hop) for lane, cohort in cohorts.items()}
    # The runtime receipt does not exist at assembly. Pinning fabricated future
    # proof bytes would turn a planned outcome into evidence; the reader binds
    # the actual injector receipt to this context, transfer and observed hop.
    result['writer_lost_ack']['argv'] += ['--lost-ack-proof', str(proof)]
    return result


def configure(source, staging, cohorts, hop, lost_ack_proof):
    """Return native-deployment fields from sealed, already supplied inputs."""
    import infra_config_stage as stage
    from hot_update_infra_verbs import ALL_FAULTS
    return dict(phase_reader=spec(source, 'phase'),
        faults={point: {action: spec(source, 'fault', point, action)
                        for action in ('install', 'status', 'heal')} for point in ALL_FAULTS},
        controls=dict(final=spec(source, 'final')),
        phase_lanes=lane_specs(cohorts, hop, lost_ack_proof),
        config_stage=stage.spec(staging), config_root=stage.CONFIG_ROOT)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--plan-sha256', required=True)
    parser.add_argument('--context', type=Path, required=True)
    parser.add_argument('operation', choices=('phase', 'fault', 'final'))
    args, arguments = parser.parse_known_args()
    try:
        import infra_owner_ports as ports
        value = read_plan(args.plan, args.plan_sha256)
        ctx = ports.context(args.context)
        ports.supervised(os.getpid(), value['supervisor'], 'deployment')
        plans.need(os.environ.get('SIM_UPGRADE_SUPERVISOR') == value['supervisor']['path'],
                   'deployment', 'supervisor_binding')
        value['_context'] = ctx
        if args.operation == 'phase':
            import infra_deployment_phase as phase
            plans.need(len(arguments) == 2, 'deployment', 'phase_arguments')
            result = phase.read(value, value, ctx, *arguments)
        elif args.operation == 'fault':
            import infra_deployment_faults as faults
            plans.need(len(arguments) >= 2, 'deployment', 'fault_arguments')
            point, operation, *extra = arguments
            result = faults.status(value, value, ctx, point) if operation == 'status' else \
                faults.mechanism(value, value, ctx, point, operation, extra, 300 if operation == 'heal' else 120)
        else:
            import infra_deployment_final as final
            result = final.collect(value, value, ctx, arguments, 600)
        sys.stdout.buffer.write(contract.canonical(result))
        return 0
    except Exception as error:
        # Only fixed public guard labels may leave the boundary. Preserve the
        # named missing owner/precondition, never an arbitrary argv/body error.
        message = str(error) if type(error) is ValueError and str(error).startswith('NOT_RUN ') else \
            'NOT_RUN deployment: original_export_or_injector_precondition_unavailable'
        print(message, file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
