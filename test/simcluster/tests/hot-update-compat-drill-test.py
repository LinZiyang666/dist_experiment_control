#!/usr/bin/env python3
"""Two-sided arm controls for drill 39's S8 compatibility arms (r9d D-11 sections).

origin: hot-update S8 plan §9.3 / §11.2 P28; r9d D-11.

The REAL drill file (drills/39-hot-update-legacy-n1.sh) runs for each arm in a
private sandbox and takes its real dispatch: `_upgrade_39_new_arm` sources the
REAL drills/lib/hot_update_broker.sh, `upgrade_39_compat "$ARM"` runs the
embedded PYCOMPAT driver as-is under the shared FIX-ORPHAN supervisor, and
`broker_native` + lib/assert.sh `drill_end` mint the native verdict. Only the
deployment boundary is a double:

  * SIM_UPGRADE_COMPAT_SUPPLIER is a recorder speaking the documented P24/P22
    port protocol (prepare/observe/await-fence/judge/stop); it writes the plan,
    execution, result and cleanup documents the live supplier would write and
    reports the retained command statuses it observes;
  * every product command is a finite local /bin/sh actor that records its
    exact product argv and exits with the scenario status;
  * for ABI-adjacent only, drills/lib/upgrade_tx_fixture.py is replaced in the
    sandbox by a double of the P26 live-fixture INTERFACE (manifest_read /
    start / collect.sample / drain / stop / `judge` CLI). The real P26 judge
    needs a live full-profile cohort; this double proves only that the driver
    starts, samples, drains, judges and stops it and keeps its native status;
  * docker/ssh/sudo/systemctl/journalctl/tether/runuser/nsenter/iptables are
    poisoned on PATH and the sandbox `simcluster` refuses to run.

Wire plans are built from the REAL frozen templates
(test/hotupdate/testdata/upgrade_wire_argv.json) and a P22-shaped wire
manifest. Each arm must reach its expected native verdict
(expected-verdicts.tsv `39-hot-update-legacy-n1.<ARM>`) with its own CASES39
CLI statuses, and every deliberately bad input must drive it to a named
non-expected verdict. The per-arm required checks below are an independent
pin of PYCOMPAT's `checks`/`wire_checks`/config obligations: dropping one from
the drill makes that arm's negative go GREEN here, i.e. this control red.

Nothing here is a compatibility, migration or deployment receipt.

Usage: python3 -I -B tests/hot-update-compat-drill-test.py --arm <ARM>|all [--results DIR] [--jobs N]
"""
import sys
from pathlib import Path

# Isolated Python must import only these repository-local responsibility modules.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from compat_drill_cases import *


def fan_out(arms, results, jobs):
    """One isolated interpreter per arm, each leaving its own native receipt.

    r9d keeps one section per arm; each section reads only its own arm's rc and
    log. Concurrency is bounded so a small runner is slower, never oversubscribed.
    """
    from concurrent.futures import ThreadPoolExecutor
    results.mkdir(parents=True, exist_ok=False)

    def one(arm):
        with open(results / (arm + '.out'), 'w') as log:
            try:
                rc = subprocess.run([sys.executable, '-I', '-B', str(Path(__file__).resolve()), '--arm', arm],
                                    stdout=log, stderr=subprocess.STDOUT, timeout=600).returncode
            except subprocess.TimeoutExpired:
                rc = 124
        (results / (arm + '.rc')).write_text(str(rc) + '\n')
        return rc

    with ThreadPoolExecutor(max_workers=jobs) as pool:
        codes = list(pool.map(one, arms))
    return 0 if codes and all(code == 0 for code in codes) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--arm', required=True)
    parser.add_argument('--results', type=Path, help='fan out one interpreter per arm; receipts land here')
    parser.add_argument('--jobs', type=int, default=max(2, min(16, (os.cpu_count() or 2) // 2)))
    args = parser.parse_args()
    if args.arm != 'all' and args.arm not in COMPAT:
        # An unknown selector must not run zero tests and pass vacuously.
        print('unknown compatibility arm selector: ' + args.arm, file=sys.stderr)
        return 2
    supervisor = os.environ.get('SIM_UPGRADE_SUPERVISOR', '')
    if not supervisor.startswith('/') or not os.access(supervisor, os.X_OK):
        # The drill and PYCOMPAT run only under the shared FIX-ORPHAN owner.
        print('compat arm controls: shared process supervisor is required (SIM_UPGRADE_SUPERVISOR)', file=sys.stderr)
        return 2
    selected = COMPAT if args.arm == 'all' else [args.arm]
    if args.results:
        return fan_out(selected, args.results, max(1, args.jobs))
    suite = unittest.TestSuite(arm_suite(arm) for arm in selected)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() and result.testsRun and not result.skipped else 1


if __name__ == '__main__':
    sys.exit(main())
