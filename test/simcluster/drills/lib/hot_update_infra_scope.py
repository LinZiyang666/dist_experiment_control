#!/usr/bin/env -S python3 -I -B
"""Independent scoped oracle for drills 85/86 and 39.NI/CI; judge --bundle FILE.

The verb engine (hot_update_infra_verbs.py) only gathers raw receipts: the
frozen manifest, the observer's trace, fresh per-node kernel observations, the
composer's fault/exercise/config receipts, the product's status and the drill's
own command status and output. This file judges one named scope from those raw
bytes. Per-hop physical rules are probe.judge_samples itself, the same code the
FINAL judge runs, so a round check cannot drift from the final one.

Native status: 0 the scope holds, 1 an observed defect, 2 missing or invalid
custody, 4 a required live cell that could not be exercised. A product CLI 75 or
64 is input to a scope, never this file's own status. No historical-loss rc3.
origin: S9 FIX-INFRAVERBS (r9d finding 5); hot-update S9 plan §5 / §7 / §9.3 / T80
"""
import argparse
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hot_update_infra_probe as probe

NODE_FIELDS = {'producer', 'node', 'component', 'at_ns', 'server', 'invocation', 'config', 'serving',
               'temporary', 'broker', 'infra_capable', 'census'}
EXERCISE_FIELDS = {'scenario', 'rows', 'holder', 'retired'}
ROW_FIELDS = {'name', 'outcome', 'code', 'forced_close'}
FAULT_STATUS_FIELDS = {'point', 'active', 'hits', 'sent', 'effect', 'reply_cut', 'digest'}
# plan §7.3 closed families: 75 safe defer, 74 halted/recovery, 64 unsupported/invalid.
DEFER_CODES = ('infra_not_enabled', 'infra_busy', 'infra_not_ready', 'infra_inventory_incomplete',
               'infra_budget_exceeded')
HALT_CODES = ('infra_result_unknown', 'infra_recovery_required')
# LQ2: every unknown or unsupported live Caddy responsibility defers with a
# reason; permission gates refuse. None may be forced closed and signed.
GATES = ('uid-override', 'policy-override', 'late-upgrade', 'live-quic', 'long-connection', 'custom-module',
         'external-storage')
STREAMS = {'broker': ('broker_slog', 'broker_panic'), 'agent': ('agent_slog', 'agent_panic')}
PANIC = re.compile(rb'(?m)^(panic: |fatal error: |goroutine [0-9]+ \[)')
TOPOLOGY = {'single': ('brk1',), 'sole-voter': ('brk1',), 'pair': ('brk1', 'brk2'), 'quorum': ('brk1', 'brk2', 'brk3')}


def require(ok, code):
    probe.require(ok, code)


def node_observation(path, node, component):
    """A kernel census of one node; Status or a unit file cannot stand in."""
    row = probe.read_json(path)
    require(isinstance(row, dict) and set(row) == NODE_FIELDS and row['producer'] == 'kernel-reader'
            and row['node'] == node and row['component'] == component and probe.uint(row['at_ns'], 1),
            'node_observation_custody')
    probe.process(row['server'])
    probe.process(row['broker'])
    invocation = row['invocation']
    require(isinstance(invocation, dict) and set(invocation) == {'user', 'restart', 'managed'}
            and all(isinstance(invocation[k], str) and invocation[k] for k in ('user', 'restart'))
            and type(invocation['managed']) is bool, 'invocation_fields')
    require((row['config'] is None or probe.digest(row['config'])) and type(row['serving']) is bool
            and type(row['infra_capable']) is bool and isinstance(row['temporary'], list), 'node_fields')
    for item in row['temporary']:
        probe.process(item)
    census = row['census']
    probe.fields(census, 'census')
    require(census.get('producer') == 'kernel-reader' and census.get('complete') is True
            and probe.uint(census.get('generation'), 1) and isinstance(census.get('processes'), list)
            and bool(census['processes']) and probe.uint(census.get('live_business'))
            and probe.uint(census.get('old_owner_pending')), 'census_unknown')
    running = {probe.process(p) for p in census['processes']}
    require(probe.process(row['server']) in running, 'server_outside_census')
    return row


def identity(process):
    return probe.process(process)


class Scope:
    def __init__(self, bundle):
        self.bundle = bundle
        self.state = bundle['state']
        self.args = bundle['arguments']
        self.root = Path(bundle['root'])
        self.failures, self.missing = [], []
        self.manifest = probe.validate_manifest(probe.read_json(bundle['manifest']))
        self.trace = probe.read_json(bundle['trace']) if bundle['snapshot'] is not None else None
        if self.trace is not None:
            require(self.trace.get('nonce') == self.manifest['nonce'] and self.trace.get('producer') ==
                    'independent-observer' and probe.uint(self.trace.get('t0_ns'), 1), 'trace_binding')
        self.observed = {node: node_observation(path, node, self.state['component'])
                         for node, path in bundle['observed'].items()}
        if bundle['scope'] != 'logs':
            require(sorted(self.observed) == sorted(self.state['nodes']), 'observation_requiredset')

    def check(self, ok, code):
        if not ok:
            self.failures.append(code)

    def command(self, rc):
        """The drill's own command status and output, read from its step log."""
        self.check(int(self.args['command_rc']) == rc, 'command_rc:%s' % self.args['command_rc'])
        log = Path(self.args['command_log'])
        require(log.parent == self.root and log.is_file(), 'command_log_custody')
        raw = log.read_bytes()
        require(len(raw) <= 1 << 20, 'command_log_overflow')
        return raw.decode(errors='replace')

    def baseline(self, node):
        source = self.state.get('baseline', {})
        require(isinstance(source, dict) and node in source, 'missing_baseline_observation')
        return node_observation(source[node], node, self.state['component'])

    def baselines(self):
        return {node: self.baseline(node) for node in self.observed}

    def rows(self):
        return self.trace['samples'] + self.trace['progress'] if self.trace else []

    def originals_intact(self):
        # The same original cohort at every read: identity, one accept, zero
        # redial and monotone bytes. Stall bounds remain FINAL's job.
        originals = self.manifest['originals']
        last = {}
        for row in sorted(self.rows(), key=lambda r: r['at_ns']):
            self.check(set(row['business']) == set(originals), 'requiredset_changed')
            for name, business in row['business'].items():
                probe.fields(business, 'business')
                base = originals.get(name)
                self.check(base is not None and business['identity'] == base['identity'] and business['accepts'] == 1
                           and business['redials'] == 0, 'original_changed:' + name)
                pair = (business['tx'], business['rx'])
                self.check(all(probe.uint(v) for v in pair) and all(a >= b for a, b in zip(pair, last.get(name, (0, 0)))),
                           'original_bytes_regressed:' + name)
                last[name] = pair

    def unchanged(self, before, label):
        for node, row in self.observed.items():
            self.check(identity(row['server']) == identity(before[node]['server']), label + ':' + node)

    def quiet(self):
        for node, row in self.observed.items():
            self.check(row['serving'] and not row['temporary'] and row['census']['old_owner_pending'] == 0,
                       'old_or_temporary_owner:' + node)

    def block(self, direction=None):
        blocks = self.state['blocks']
        require(bool(blocks), 'no_observed_block')
        block = blocks[-1]
        if direction is not None:
            self.check(block['direction'] == direction, 'block_direction')
        return block

    def watched(self, hops):
        snapshot = self.bundle['snapshot']
        require(snapshot is not None and 'watch_custody_failed' not in snapshot['watch_failures'], 'watch_custody')
        require(not snapshot['watching'], 'watch_still_running')
        self.check(not [f for f in snapshot['watch_failures'] if f.split(':')[1] in hops], 'phase_not_observed')

    def judge_hops(self, ids):
        """probe.judge_samples over the selected hops, from the attachments they began with."""
        samples = self.trace['samples']
        selected = [s for s in samples if s['hop'] in ids]
        first = next((i for i, s in enumerate(samples) if s['hop'] in ids), len(samples))
        attachments = samples[first - 1]['attachments'] if first else self.manifest['attachments']
        m = dict(self.manifest, hops=[h for h in self.manifest['hops'] if h['id'] in ids], attachments=attachments)
        t0 = self.trace['t0_ns']
        probe.judge_samples(m, selected, t0, t0 + m['seconds'] * 1_000_000_000, self.check, self.missing)
        # The node's server must run the last target of its own chain; a support
        # hop changes the helper incarnation, not the component's server.
        last = {h['host']: h for h in m['hops'] if h['component'] == self.state['component']}
        for host, hop in last.items():
            row = self.observed.get(host)
            self.check(row is not None and row['server']['ExeSHA256'] == hop['target_sha256'], 'host_not_at_target:' + hop['id'])

    def staged(self, variant):
        record = self.state['configs'].get(variant)
        require(record is not None, 'config_not_staged')
        receipt = probe.read_json(record['receipt'])
        require(set(receipt) == {'variant', 'staged', 'sha256'} and receipt['variant'] == variant
                and receipt['staged'] is True and probe.digest(receipt['sha256']), 'config_receipt')
        return receipt['sha256']

    def exercise(self, scenario, names):
        path = self.state['exercises'].get(scenario)
        require(path is not None, 'exercise_not_run')
        receipt = probe.read_json(path)
        require(isinstance(receipt, dict) and set(receipt) == EXERCISE_FIELDS and receipt['scenario'] == scenario
                and isinstance(receipt['rows'], list) and type(receipt['retired']) is bool, 'exercise_receipt')
        rows = {}
        for row in receipt['rows']:
            require(isinstance(row, dict) and set(row) == ROW_FIELDS and isinstance(row['name'], str)
                    and isinstance(row['outcome'], str) and isinstance(row['code'], str)
                    and type(row['forced_close']) is bool and row['name'] not in rows, 'exercise_row')
            rows[row['name']] = row
        if receipt['holder'] is not None:
            probe.process(receipt['holder'])
        # Missing actual attempts are an unexercised required cell, not a pass.
        for name in names:
            if name not in rows:
                self.missing.append('exercise_cell_missing:' + name)
        for row in rows.values():
            self.check(not row['forced_close'], 'forced_close:' + row['name'])
        return receipt, rows

    def fault_status(self, point):
        status = probe.read_json(self.bundle['fault_status'][point])
        require(isinstance(status, dict) and set(status) == FAULT_STATUS_FIELDS and status['point'] == point
                and all(type(status[k]) is bool for k in ('active', 'sent', 'effect', 'reply_cut'))
                and probe.uint(status['hits']) and (status['digest'] is None or probe.digest(status['digest'])),
                'fault_status_receipt')
        record = self.state['faults'].get(point)
        require(record is not None and record['healed'] is None, 'fault_not_installed')
        return status, record


def judge_baseline(s):
    progress = s.trace['progress'] if s.trace else []
    require(bool(progress), 'baseline_business_unobserved')
    s.check(not s.trace['samples'] and not s.state['blocks'], 'effect_before_baseline')
    s.originals_intact()
    for node, row in s.observed.items():
        # READY is the stock runtime at the previous adjacent version; a host
        # that is already managed or retrofitted cannot sign the stock baseline.
        s.check(row['server']['ExeSHA256'] == s.state['initial_sha256'], 'not_initial_version:' + node)
        s.check(row['invocation']['managed'] is False, 'pre_managed_stock:' + node)
    s.quiet()


def judge_bootstrap(s):
    s.command(0)
    node = s.args['node']
    require(node in s.observed, 'bootstrap_node')
    base, row = s.baseline(node), s.observed[node]
    s.check(identity(row['server']) == identity(base['server']), 'bootstrap_restarted_stock_server')
    s.check(row['invocation']['managed'] is True, 'management_not_activated')
    s.check({k: row['invocation'][k] for k in ('user', 'restart')} ==
            {k: base['invocation'][k] for k in ('user', 'restart')}, 'stock_invocation_changed')
    s.quiet()
    s.originals_intact()


def judge_round(s):
    s.command(0)
    block = s.block(s.args['direction'])
    s.check(block['nodes'] == s.args['nodes'].split() and bool(block['hops']), 'round_without_declared_hops')
    s.watched(block['hops'])
    s.judge_hops(set(block['hops']))
    s.quiet()
    s.originals_intact()


def judge_cycle(s):
    component = s.state['component']
    hops = [h for h in s.manifest['hops'] if h['component'] == component]
    observed = [h for b in s.state['blocks'] for h in b['hops']]
    s.check(len(hops) == s.args['expected_hops'] and [h['id'] for h in hops] == [h for h in observed
            if h in {x['id'] for x in hops}] and s.state['cursor'] == len(s.manifest['hops']), 'cycle_hop_count')
    s.watched(observed)
    binary = {'nats': 'nats-server', 'caddy': 'caddy'}[component]
    vendor = Path(s.bundle['here']) / 'vendor'
    final = vendor / 'infra' / (binary + '-' + s.args['final_version'])
    require(re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', s.args['final_version']) is not None and final.is_file(),
            'final_version_artifact')
    pin = probe.sha(final)
    s.check(pin == probe.sha(vendor / (binary + '-next')), 'final_version_not_adjacent_target')
    # Every observed hop is re-judged together, so continuity across rounds
    # (each host's before == its previous current) is part of the cycle.
    s.judge_hops(set(observed))
    for node, row in s.observed.items():
        s.check(row['server']['ExeSHA256'] == pin, 'not_returned_to_pin:' + node)
    s.originals_intact()


def judge_retirement(s):
    s.watched([h for b in s.state['blocks'] for h in b['hops']])
    s.quiet()
    running = {identity(p) for row in s.observed.values() for p in row['census']['processes']}
    for sample in s.trace['samples']:
        if sample['phase'] != 'retired':
            continue
        hop = next(h for h in s.manifest['hops'] if h['id'] == sample['hop'])
        if hop['action'] != 'reload':
            s.check(identity(sample['before']) not in running, 'old_owner_alive:' + hop['id'])
        s.check(all(identity(p) not in running for p in sample['temporary']), 'temporary_owner_alive:' + hop['id'])
    s.originals_intact()


def judge_logs(s):
    nodes = list(TOPOLOGY[s.state['log_topology']]) + ['agt1', 'agt2']
    for node in nodes:
        for stream in STREAMS['agent' if node.startswith('agt') else 'broker']:
            name = node + '.' + stream
            parts = []
            for phase in ('baseline', 'final'):
                directory = s.root / ('logs-' + phase)
                meta = probe.read_json(directory / (name + '.json'))
                raw = (directory / (name + '.raw')).read_bytes()
                require(meta.get('read_ok') is True and probe.uint(meta.get('inode'), 1)
                        and meta.get('length') == len(raw), 'log_snapshot_custody:' + name)
                parts.append((meta, raw))
            (base, before), (final, after) = parts
            # A fresh window is an append to the same file or journal identity;
            # rotation or truncation cannot hide a panic by shrinking the stream.
            s.check(final['inode'] == base['inode'] and after.startswith(before), 'log_window_not_append:' + name)
            s.check(PANIC.search(after[len(before):]) is None, 'panic_in_window:' + name)


def judge_refusal(s):
    text = s.command(75)
    s.check(any(code in text for code in DEFER_CODES), 'refusal_code')
    status, record = s.fault_status(s.args['point'])
    s.check(status['active'], 'fault_not_active')
    # Before effect: every node keeps the incarnation seen when the fault went in.
    window = {node: node_observation(record['window'][node], node, s.state['component']) for node in s.observed}
    s.unchanged(window, 'refusal_had_effect')
    s.quiet()
    s.originals_intact()


def judge_halted(s):
    text = s.command(74)
    s.check(any(code in text for code in HALT_CODES), 'halt_code')
    s.check(len(set(re.findall(r'(?<![0-9a-f])[0-9a-f]{32}(?![0-9a-f])', text))) == 1, 'halt_tx_guidance')
    status, _ = s.fault_status(s.args['point'])
    # A lost ACK needs the real request sent, its effect applied and the reply
    # path cut, bound to one digest; closing a socket alone is not the window.
    s.check(status['active'] and status['hits'] >= 1 and status['sent'] and status['effect'] and status['reply_cut']
            and status['digest'] is not None, 'fault_window_not_hit')
    block = s.block('upgrade')
    s.check(bool(block['hops']), 'halt_without_declared_hops')
    retired = [x for x in s.trace['samples'] if x['hop'] in block['hops'] and x['phase'] == 'retired']
    s.check(not retired, 'halt_did_not_block_progression')
    serving = sum(1 for row in s.observed.values() if row['serving'])
    s.check(serving >= len(s.observed) // 2 + 1, 'healthy_serving_path_lost')
    s.originals_intact()


def judge_recovered(s):
    s.command(0)
    tx = s.state.get('tx') or {}
    s.check(tx.get('tx') == s.args['tx'], 'recovered_other_transaction')
    status = probe.read_json(tx['status']) if tx.get('status') else {}
    s.check(isinstance(status.get('Tx'), list) and bytes(status['Tx']).hex() == s.args['tx'], 'status_receipt')
    block = s.block('upgrade')
    s.watched(block['hops'])
    s.judge_hops(set(block['hops']))
    s.quiet()
    s.originals_intact()


def judge_store_lock(s):
    receipt, rows = s.exercise('store-double-open', ('second-launch', 'legacy-owner'))
    for name in ('second-launch', 'legacy-owner'):
        row = rows.get(name)
        s.check(row is None or (row['outcome'] == 'refused' and row['code']), 'store_opened_twice:' + name)
    base = s.baseline('brk1')
    s.check(receipt['holder'] is not None and identity(receipt['holder']) == identity(base['server']),
            'lock_holder_not_stock_owner')
    s.unchanged(s.baselines(), 'store_owner_restarted')
    s.originals_intact()


def judge_join(s):
    receipt, rows = s.exercise('exact-release-join', ('mismatched-release', 'exact-release'))
    mismatched, exact = rows.get('mismatched-release'), rows.get('exact-release')
    s.check(mismatched is None or (mismatched['outcome'] == 'refused' and mismatched['code']), 'mismatched_release_joined')
    s.check(exact is None or exact['outcome'] == 'joined', 'exact_release_refused')
    s.check(receipt['retired'], 'temporary_broker_not_retired')
    s.unchanged(s.baselines(), 'join_restarted_server')
    s.quiet()
    s.originals_intact()


def judge_gates(s):
    _, rows = s.exercise('caddy-responsibility-gates', GATES)
    for name, row in rows.items():
        s.check(row['outcome'] in ('deferred', 'refused') and row['code'], 'responsibility_not_deferred:' + name)
    s.unchanged(s.baselines(), 'gate_restarted_server')
    s.quiet()
    s.originals_intact()


def legacy_observation(s):
    for check in reversed(s.state['checks']):
        if check['scope'] == 'legacy-source' and check['rc'] == 0:
            bundle = probe.read_json(check['bundle'])
            return node_observation(bundle['observed']['brk1'], 'brk1', s.state['component'])
    require(False, 'legacy_source_not_established')


def judge_legacy_source(s):
    row = s.observed['brk1']
    previous = Path(s.bundle['here']) / 'vendor/tether-prev'
    require(previous.is_file(), 'previous_release_artifact')
    s.check(row['broker']['ExeSHA256'] == probe.sha(previous), 'target_not_tether_prev')
    s.check(row['infra_capable'] is False, 'legacy_target_advertises_infra')
    s.check(row['serving'], 'legacy_target_not_serving')
    s.originals_intact()


def judge_legacy_refusal(s):
    text = s.command(64)
    s.check('infra_unsupported' in text, 'legacy_refusal_code')
    source, row = legacy_observation(s), s.observed['brk1']
    # Before effect: no broker reexec, no capability, the same component owner.
    s.check(identity(row['broker']) == identity(source['broker']), 'legacy_broker_reexecuted')
    s.check(row['infra_capable'] is False, 'legacy_target_advertises_infra')
    s.unchanged(s.baselines(), 'legacy_refusal_had_effect')
    s.check(not s.trace['samples'] and not any(b['hops'] for b in s.state['blocks']), 'legacy_refusal_observed_hops')
    s.originals_intact()


def judge_reload(s):
    s.command(0)
    staged = s.staged('valid')
    s.check(s.args['config_sha256'] == staged, 'reload_digest_not_staged')
    block = s.block('reload')
    s.check(bool(block['hops']), 'reload_without_declared_hop')
    s.watched(block['hops'])
    s.judge_hops(set(block['hops']))
    retired = [x for x in s.trace['samples'] if x['hop'] in block['hops'] and x['phase'] == 'retired']
    s.check(bool(retired) and retired[-1]['config_after'] == staged, 'reload_generation_not_staged')
    row = s.observed['brk1']
    s.check(row['config'] == staged, 'active_config_not_staged')
    s.unchanged(s.baselines(), 'reload_changed_pid')
    s.originals_intact()


def judge_invalid(s):
    text = s.command(64)
    s.check('infra_config_invalid' in text, 'invalid_config_code')
    s.check(s.args['config_sha256'] == s.staged(s.args['variant']), 'invalid_digest_not_staged')
    block = s.block('reload')
    s.check(not block['hops'], 'invalid_config_observed_hops')
    window = {node: node_observation(block['window'][node], node, s.state['component']) for node in s.observed}
    s.unchanged(window, 'invalid_config_had_effect')
    for node, row in s.observed.items():
        s.check(row['config'] == window[node]['config'], 'invalid_config_changed_generation:' + node)
    s.quiet()
    s.originals_intact()


def judge_support(s):
    s.command(0)
    block = s.block(s.args['action'])
    s.check(bool(block['hops']), 'support_without_declared_hop')
    s.watched(block['hops'])
    s.judge_hops(set(block['hops']))
    s.check(s.observed['brk1']['serving'], 'support_cut_lost_serving_path')
    s.originals_intact()


JUDGES = {'baseline': judge_baseline, 'bootstrap': judge_bootstrap, 'round': judge_round, 'cycle': judge_cycle,
          'retirement': judge_retirement, 'logs': judge_logs, 'refusal': judge_refusal, 'halted': judge_halted,
          'recovered': judge_recovered, 'store-lock': judge_store_lock, 'join': judge_join,
          'responsibility-gates': judge_gates, 'legacy-source': judge_legacy_source,
          'legacy-refusal': judge_legacy_refusal, 'reload': judge_reload, 'invalid-config-cert': judge_invalid,
          'support': judge_support}


def judge(bundle):
    scope = Scope(bundle)
    JUDGES[bundle['scope']](scope)
    rc = 1 if scope.failures else 4 if scope.missing else 0
    return dict(scope=bundle['scope'], native_rc=rc, verdict={0: 'GREEN', 1: 'ASSERT-FAIL', 4: 'INCOMPLETE'}[rc],
                failures=sorted(set(scope.failures)), incomplete=sorted(set(scope.missing)),
                evidence_scope=scope.manifest['mode'], reader_sha256=probe.sha(__file__))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('command', choices=('judge',))
    parser.add_argument('--bundle', type=Path, required=True)
    args = parser.parse_args()
    bundle = probe.read_json(args.bundle)
    require(isinstance(bundle, dict) and bundle.get('scope') in JUDGES, 'unknown_scope')
    result = judge(bundle)
    print(json.dumps(result, sort_keys=True))
    return result['native_rc']


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, TypeError, IndexError, AttributeError):
        print(json.dumps(dict(native_rc=2, verdict='SETUP-RED', failures=['invalid_scope_custody'])))
        sys.exit(2)
