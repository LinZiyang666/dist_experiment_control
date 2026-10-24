#!/usr/bin/env -S python3 -I -B
"""S9 composer supply, separate from the independent P32 observation oracle.

A deployment owner supplies infra-composer-supply-v1, never a GREEN receipt:
{schema, mode, handoff, profiles, commands, readers, tls, faults, exercises,
 configs}. profiles has every component/topology/profile key in the verb engine.
Each profile has {seconds, rates, prepare, ready, workloads}; workloads has the
four owners below, each {keeper, stop}. All executable specs use the existing
{argv, sha256, inputs} contract. Commands additionally retain mode/expected_output.

prepare receives --context FILE; keeper/stop receive --context FILE --evidence
DIR. The independent ready reader receives --context FILE and returns the exact
infra-cohort-v1 READY schema. Other instruments receive --context FILE followed
by the verb engine's arguments. An owner may adapt existing S5/S7 fixtures; it
must retain their sealed descriptors and original invocations until stop.
No argv, environment, or JSON supply may carry SS keys or crypto state.

origin: hot-update S9 plan §9.1 / §9.4 / P19/P26/P30; S9 COMPOSER.
"""
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hot_update_infra_probe as probe
import hot_update_infra_verbs as verbs
import hot_update_infra_workload as workload

# A shared flow owner holds seven originals, but cannot stand in for a task or
# for any of the four independently identified transfers. This table is closed.
OWNERS = {'flow': ('ssh', 'tcp-active', 'tcp-idle', 'tcp-half-close', 'tcp-reverse', 'ss-alice', 'ss-bob'),
          'pty': ('pty',), 'exec': ('exec',), 'transfers': probe.COHORTS}
_HASHES = {}
READY_FIELDS = {'schema', 'phase', 'producer', 'seconds', 'originals', 'attachments', 'transfers',
                's7', 'flow', 's7_manifest_sha256', 's7_reader_digest', 'phase_lanes'}
SUPPLY_FIELDS = {'schema', 'mode', 'handoff', 'profiles', 'commands', 'readers', 'tls', 'faults', 'exercises', 'configs'}


def require(ok, code):
    probe.require(ok, 'composer_' + code)


def sha(path):
    # Validation visits shared sources many times. Cache only a stable kernel
    # incarnation; size/mtime/ctime changes force a fresh read, including an
    # in-place rewrite whose inode did not change. Never persist this cache.
    path = Path(path)
    before = path.stat()
    identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
    key = str(path)
    if key in _HASHES and _HASHES[key][0] == identity:
        return _HASHES[key][1]
    value = hashlib.sha256(path.read_bytes()).hexdigest()
    after = path.stat()
    require(identity == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns), 'hash_raced')
    _HASHES[key] = (identity, value)
    return value


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def read(path):
    path = Path(path)
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= 4 << 20, 'regular_bounded_input')
    return workload.decode(path.read_bytes())


def pin(path):
    path = Path(path).absolute()
    require(path.is_file() and not path.is_symlink(), 'regular_pin')
    return dict(path=str(path), sha256=sha(path))


def file_pin(row):
    require(isinstance(row, dict) and set(row) == {'path', 'sha256'} and probe.digest(row['sha256']), 'pin_schema')
    path = Path(row['path'])
    require(path.is_absolute() and path.is_file() and not path.is_symlink()
            and sha(path) == row['sha256'], 'pin_bytes')
    return row


def spec(value):
    require(isinstance(value, dict) and {'argv', 'sha256'} <= set(value)
            <= {'argv', 'sha256', 'inputs', 'mode', 'expected_output'}, 'spec_schema')
    argv = value['argv']
    require(isinstance(argv, list) and argv and all(isinstance(a, str) for a in argv)
            and Path(argv[0]).is_absolute() and os.access(argv[0], os.X_OK)
            and sha(argv[0]) == value['sha256'], 'executable_bytes')
    require('/.tether/' not in ' '.join(argv), 'production_home_forbidden')
    inputs = value.get('inputs', [])
    require(isinstance(inputs, list), 'input_list')
    require(all(arg in {row['path'] for row in inputs} for arg in argv[1:]
                if arg.endswith('.py') and Path(arg).is_absolute()), 'source_binding_missing')
    for row in value.get('inputs', []):
        file_pin(row)
    # A supplied adapter must select a staged binary or transport into the
    # disposable deployment. Never execute the driver's installed tether.
    executable = Path(value['argv'][0])
    require(executable.name != 'tether' or 'vendor' in executable.parts, 'installed_tether_forbidden')
    return value


def profile_keys():
    return {':'.join((component, topology, profile)) for (component, profile), row in verbs.PROFILES.items()
            for topology in row['topologies']}


def all_specs(value):
    result = list(value['commands'].values()) + list(value['readers'].values())
    for row in value['profiles'].values():
        result += [row['prepare'], row['ready']]
        for owner in row['workloads'].values():
            result += [owner['keeper'], owner['stop']]
    for row in value['faults'].values():
        result += list(row.values())
    result += list(value['exercises'].values()) + list(value['configs'].values())
    result += [row['reader'] for row in value['tls'].values()]
    return result


def validate(value):
    require(isinstance(value, dict) and set(value) == SUPPLY_FIELDS and value['schema'] == 'infra-composer-supply-v1'
            and value['mode'] in ('live', 'hermetic'), 'supply_schema')
    handoff = value['handoff']
    require(isinstance(handoff, dict) and set(handoff) == {'schema', 'accepted_by', 'files'}
            and handoff['schema'] == 'transfer-v1' and handoff['accepted_by'] == 'lead'
            and set(handoff['files']) == {'executable', 'source', 'schema', 'positive', 'negative'},
            'NOT_RUN pending lead-accepted S7 transfer-v1 handoff')
    for row in handoff['files'].values():
        file_pin(row)
    require(isinstance(value['profiles'], dict) and set(value['profiles']) == profile_keys(), 'profile_set')
    for key, row in value['profiles'].items():
        require(isinstance(row, dict) and set(row) == {'seconds', 'rates', 'prepare', 'ready', 'workloads'}, 'profile_schema')
        # Duration is frozen before launch. P32 independently requires every
        # physical cut to fall within it; a short declaration cannot hide a cut.
        require(type(row['seconds']) is int and 0 < row['seconds'] <= 4320, 'duration')
        require(isinstance(row['workloads'], dict) and set(row['workloads']) == set(OWNERS), 'required_workload_absent')
        for owner in row['workloads'].values():
            require(isinstance(owner, dict) and set(owner) == {'keeper', 'stop'}, 'workload_schema')
        require(isinstance(row['rates'], dict) and set(row['rates']) == set(probe.COHORTS), 'four_rates')
        for name, rate in row['rates'].items():
            size = 66549440 if name.startswith('small-') else 1 << 30
            require(type(rate) is int and rate > 0 and size // rate > row['seconds'], 'startup_rate_budget')
    require(set(value['commands']) == set(probe.KINDS) and
            set(value['readers']) == {'sample', 'progress', 'effects', 'final', 'node'}, 'instrument_set')
    for name, row in value['commands'].items():
        require(row.get('mode') == 'cli' and any('{logical_id}' in arg for arg in row['argv']), 'stable_cli_sink')
        require(name not in ('read', 'short_exec') or isinstance(row.get('expected_output'), str), 'expected_output')
    require(set(value['tls']) == set(probe.TLS_LANES), 'six_tls_lanes')
    for name, row in value['tls'].items():
        probe.fields(row, 'tls_config')
        require({'reader', 'address', 'server_name', 'ca_file'} <= set(row), 'tls_config')
        file_pin(pin(row['ca_file']))
        require(value['mode'] == 'hermetic' or row.get('port', 443) == 443, 'canonical_tls_port')
    for key, expected in (('faults', verbs.ALL_FAULTS), ('exercises', verbs.ALL_EXERCISES), ('configs', verbs.ALL_CONFIGS)):
        require(set(value[key]) == set(expected), 'complete_' + key)
    for row in value['faults'].values():
        require(isinstance(row, dict) and set(row) == {'install', 'status', 'heal'}, 'fault_schema')
    for row in all_specs(value):
        spec(row)
    return value


def load(path, expected=None):
    if expected is not None:
        require(probe.digest(expected) and sha(path) == expected, 'supply_changed')
    return validate(read(path))


def ready(value, context, supply):
    """Validate target facts before the keeper is allowed to publish READY.

    Reuse the independent oracle's manifest/S7 reader. Do not turn the known
    owner roster into observations: every identity below comes from the ready
    reader, and every scheduled hop comes from the frozen verb engine.
    """
    require(isinstance(value, dict) and set(value) == READY_FIELDS and value['schema'] == 'infra-cohort-v1'
            and value['phase'] == 'ready' and value['producer'] == 'composer', 'ready_schema')
    require(value['seconds'] == context['seconds'], 'ready_duration')
    for transfer in value['transfers']:
        require(transfer['rate_limit'] == context['rates'][transfer['cohort']], 'ready_rate_changed')
    hops = [dict(id='hop-' + str(i), component=c, action=a, host=h, target_sha256=t,
                 participants=sorted(value['attachments'])) for i, (c, a, h, t) in enumerate(context['hops'])]
    row = verbs.PROFILES[(context['component'], context['profile'])]
    projected = dict(schema=probe.SCHEMA, mode=supply['mode'], nonce=context['nonce'], seconds=value['seconds'],
        profile='cycle' if row['plan'] == 'cycle' else context['profile'], carrier_transition='unchanged',
        originals=value['originals'], attachments=value['attachments'], transfers=value['transfers'], hops=hops,
        tls_lanes=list(probe.TLS_LANES) if any(h['component'] == 'caddy' for h in hops) else [],
        s7_handoff=supply['handoff'], s7_manifest_sha256=value['s7_manifest_sha256'],
        s7_reader_digest=value['s7_reader_digest'], phase_lanes=value['phase_lanes'],
        deployment=dict(s7=value['s7'], flow=value['flow']))
    probe.validate_manifest(projected)
    for key, fields in (('flow', {'evidence', 'nonce'}), ('s7', {'manifest', 'evidence'})):
        require(isinstance(value[key], dict) and set(value[key]) == fields, 'ready_' + key)
        root = Path(value[key]['evidence'])
        require(root.is_absolute() and root.is_dir() and not root.is_symlink(), 'ready_evidence')
    require(sha(value['s7']['manifest']) == value['s7_manifest_sha256'], 's7_manifest_bytes')
    return value
