#!/usr/bin/env -S python3 -I -B
"""Native readers for authenticated S9 attachment and pre-repair exports.

participants.<id>.reader may be {custody:{root,source}}. repair_fence may be
{custody:{node,root,source},binding:<pinned live journal command>}. The binding
receives the actual context/hop/phase and returns {tx,step,revision}; it is not
an expected transaction in a configuration file. Existing executable recipes
remain supported. Custody installation and remote source supply are explicit.
origin: hot-update S9 plan §2.3 / §4.3 / §9.1; NC-EXPORTS instrument handoff.
"""
import argparse
import json
from pathlib import Path
import re
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import infra_native_custody as custody


def declaration(value, repair=False):
    from infra_owner_plan import need, path
    need(type(value) is dict and set(value) == ({'custody', 'binding'} if repair else {'custody'}),
         'reader', 'custody_reader_fields')
    source = value['custody']
    need(type(source) is dict and set(source) == ({'node', 'root', 'source'} if repair else {'root', 'source'}),
         'reader', 'custody_source_fields')
    path(source['root'], 'reader')
    need('..' not in Path(source['root']).parts and type(source['source']) is str and
         re.fullmatch('[a-zA-Z0-9_.-]+', source['source']), 'reader', 'custody_source_binding')
    if repair:
        from infra_composer_contract import spec
        spec(value['binding'])
    return source


def remote_read(plan, config, source, node):
    import infra_native as native
    import infra_composer_contract as contract
    import infra_owner_plan as plans
    # Verify the executing remote reader closure, as with P02's physical binary.
    # Reading a locally pinned file never certifies an installed remote copy.
    names = ('infra_native_exports.py', 'infra_native_custody.py', 'flow_observation.py',
             'hot_update_infra_probe.py', 'transfer_observation.py')
    files = [plans.LIB / name for name in names]
    sums = native.remote(plan, config, node, ['/usr/bin/sha256sum', *map(str, files)]).splitlines()
    plans.need(len(sums) == len(files) and all(line.split()[0].decode() == contract.sha(path)
        for line, path in zip(sums, files)), 'reader', 'target_custody_reader_sha')
    before = time.monotonic_ns()
    raw = native.remote(plan, config, node, ['/usr/bin/python3', '-I', '-B', str(files[0]),
        '--root', source['root'], '--source', source['source']], timeout=10)
    after = time.monotonic_ns()
    from hot_update_infra_workload import decode
    row = decode(raw)
    plans.need(type(row) is dict and set(row) == {'attachments', 'prefixes', 'boot_ns'} and
               type(row['boot_ns']) is int and row['boot_ns'] > 0 and before <= after, 'reader', 'custody_projection')
    # Choose the latest possible mapping, bounded by the completed remote read.
    # Both fence marks receive the SAME offset, retaining their measured order.
    # A later repair is never assigned a guessed future release time.
    return row, after - row['boot_ns']


def attachment(plan, config, source, participant):
    from infra_owner_plan import need
    from hot_update_infra_probe import attachment as validate
    binding = declaration(source['reader'])
    need(re.fullmatch('[0-9a-f]{32}', participant) is not None, 'reader', 'original_participant_id')
    payload, _ = remote_read(plan, config, binding, source['node'])
    rows = payload['attachments']
    need(type(rows) is list and rows and len({r['Participant'] for r in rows}) == len(rows), 'reader', 'complete_original_roster')
    for row in rows: validate(row)
    matched = [r for r in rows if r['Participant'] == participant]
    need(len(matched) == 1, 'reader', 'original_participant_missing')
    return matched[0]


def repair(plan, config, ctx, hop, phase, transfer_ids):
    import infra_native as native
    import infra_owner_plan as plans
    from hot_update_infra_workload import decode
    value = config['repair_fence']
    source = declaration(value, repair=True)
    _, raw = native.invoke(plan, ctx, 'readers.sample', value['binding'],
        ['--context', str(Path(ctx['evidence']) / 'context.json'), hop, phase], 10)
    binding = decode(raw)
    plans.need(type(binding) is dict and set(binding) == {'tx', 'step', 'revision'} and
        type(binding['revision']) is int and binding['revision'] > 0, 'reader', 'actual_journal_binding')
    payload, offset = remote_read(plan, config, source, source['node'])
    rows = []
    for event in payload['prefixes']:
        plans.need(type(event) is dict and set(event) == {'tx', 'step', 'at_ns', 'row'} and
            type(event['tx']) is str and re.fullmatch('[0-9a-f]{32}', event['tx']), 'reader', 'prefix_binding')
        rows.append(dict(kind='infra-prefix', actor=bytes.fromhex(event['tx']), step=event['step'],
                         at=event['at_ns'], row=event['row']))
    return custody.prefixes(rows, binding['tx'], binding['step'], transfer_ids, offset)


def project(value):
    from hot_update_infra_probe import attachment as validate
    attached = [validate(row['row']) for row in value['roster']]
    rows = [dict(tx=row['actor'].hex(), step=row['step'], at_ns=row['at'], row=row['row'])
            for row in value['rows'] if row['kind'] == 'infra-prefix']
    # Validate prefix fields before any public output. Validation preserves an
    # incomplete actual list so P32 can report the first loss, not fake success.
    for event in rows:
        custody.prefixes([dict(kind='infra-prefix', actor=bytes.fromhex(event['tx']), step=event['step'],
            at=event['at_ns'], row=event['row'])], event['tx'], event['step'], [event['row']['transfer_id']], 0)
    return dict(attachments=attached, prefixes=rows, boot_ns=value['boot_ns'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--source', required=True)
    args = parser.parse_args()
    try:
        value = project(custody.collect(args.root, args.source))
        sys.stdout.write(json.dumps(value, separators=(',', ':'), allow_nan=False) + '\n')
        return 0
    except (OSError, ValueError, KeyError, TypeError):
        print('NOT_RUN reader: original_infrastructure_custody_incomplete', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
