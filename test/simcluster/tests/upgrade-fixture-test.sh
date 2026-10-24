#!/bin/sh
# origin: hot-update S8 plan §9.0 / §11.2 P26. Hermetic oracle and supervision
# checks only; synthetic public rows never certify a product cut or S7 profile.
set -eu
HERE="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
LIB="$(CDPATH= cd -- "$HERE/../drills/lib" && pwd)"
export S8_FIXTURE_LIB="$LIB"
exec "${PYTHON3:-python3}" -I -B "$LIB/upgrade_tx_supervisor.py" run --timeout-s 55 -- "${PYTHON3_ABSOLUTE:-/usr/bin/python3}" -I -B - "$@" <<'PY'
import importlib.util
import os
from pathlib import Path
import sys
import unittest

TESTS = Path(os.environ['S8_FIXTURE_LIB']).parent.parent / 'tests'
# Explicit imports preserve the isolated interpreter and the original unittest
# selectors after responsibility splits; drill38 also imports these inputs.
# origin: hot-update S7 plan §11.4 / T50
for name in ('upgrade_fixture_evidence', 'upgrade_scenario_oracle_test',
             'upgrade_fixture_oracle_test', 'upgrade_fixture_supervision_test',
             'upgrade_fixture_mutations'):
    spec = importlib.util.spec_from_file_location(name, TESTS / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)

from upgrade_fixture_evidence import C, sealed_pair, evidence
from upgrade_scenario_oracle_test import UpgradeScenarioOracle
from upgrade_fixture_oracle_test import UpgradeFixtureOracle
from upgrade_fixture_supervision_test import UpgradeFixtureSupervision
from upgrade_fixture_mutations import mutation_checks, UpgradeCollectorSourceRoots

if __name__ == '__main__':
    if sys.argv[1:2] == ['--mutations']:
        mutation_checks(int(sys.argv[2]) if len(sys.argv)>2 else 0)
    else:
        unittest.main(testRunner=unittest.TextTestRunner(stream=sys.stdout, verbosity=2))
PY
