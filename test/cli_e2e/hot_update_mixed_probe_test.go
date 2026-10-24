//go:build linux

package cli_e2e_test

import "github.com/LinZiyang666/tether/internal/testharness"
import "path/filepath"
import "testing"

// origin: hot-update S4 internal review rv-drill DRILL-7
func TestPTYMixedProbeStrictCalibrationNeedsNoFaultMarkers(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	runPTYProbeInvariant(t, `
 mixed_suite = runpy.run_path(str(Path(sys.argv[1]) / 'test/simcluster/tests/hot-update-mixed-probe-test.py'))
 case = mixed_suite['MixedProbeTests']()
 case.setUpClass()
 case.setUp()
 try:
  calibration, env = case.fixture('strict-calibration', 'healthy')
  evidence = calibration / 'evidence'
  scheduled = case.invoke('run', '--strict', '--evidence', evidence, '--rate', 10,
                          '--duration', 2.4, '--node', 'test-node', '--nonce', mixed_suite['NONCE'], env=env)
  assert scheduled.returncode == 0, (scheduled.stdout, scheduled.stderr)
  assert json.loads((evidence / 'marks.json').read_text()) == {}
  judged = case.invoke('judge', '--strict', '--evidence', evidence,
                       '--ledger', calibration / 'ledger', '--mode', 'calibrate')
  assert judged.returncode == 0, (judged.stdout, judged.stderr)
 finally:
  case.doCleanups()
`)
}
