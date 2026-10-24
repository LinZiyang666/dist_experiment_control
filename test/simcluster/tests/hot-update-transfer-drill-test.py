#!/usr/bin/env python3
"""Two-sided arm controls for drills 63 and 64 (r9d-nonvacuity D-11 sections).

origin: hot-update S7 plan §10.3 / §10.5 / §10.6 / T43 / T44; r9d D-11.

The REAL drill files, the REAL driver library (drills/lib/hot_update_transfer.sh)
and the REAL transfer_mutations.py predicates run inside a private sandbox. Only
the deployment boundary is a double: TRANSFER_PROCESS_SUPERVISOR is a recorder
that executes nothing and answers with scenario-selected native statuses, and
docker/ssh/sudo/systemctl/tether are poisoned on PATH. Nothing here is a
transfer, cut, kill or deployment receipt. Each arm's oracle is driven both to
its expected verdict and, with deliberately bad input, away from it: an oracle
that cannot leave its expected verdict is the permanently-true family r9d exists
for, and one that cannot reach it is a permanently-red arm nobody can sign.

Usage: python3 -I -B tests/hot-update-transfer-drill-test.py --arm <ARM>|all
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from hot_update_transfer_doubles import *
from hot_update_transfer_cases import *

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
    parser.add_argument('--jobs', type=int, default=max(2, min(8, (os.cpu_count() or 2) // 2)))
    args = parser.parse_args()
    arms = tm.POSITIVE + list(tm.MUTATIONS)
    if args.arm != 'all' and args.arm not in arms:
        # An unknown selector must not run zero tests and pass vacuously.
        print('unknown transfer arm selector: ' + args.arm, file=sys.stderr)
        return 2
    selected = arms if args.arm == 'all' else [args.arm]
    if args.results:
        return fan_out(selected, args.results, max(1, args.jobs))
    suite = unittest.TestSuite(arm_suite(arm) for arm in selected)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() and result.testsRun else 1


if __name__ == '__main__':
    sys.exit(main())
