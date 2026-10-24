//go:build linux

package cli_e2e_test

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// The fixtures are independent synthetic controls already supplied by the
// probe's hermetic suite. No tether binary, broker, or Docker is invoked here.
func runPTYProbeInvariant(t *testing.T, program string) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("probe source path unavailable")
	}
	repo := filepath.Dir(filepath.Dir(filepath.Dir(file)))
	prelude := `import copy, json, runpy, sys, tempfile
from pathlib import Path
suite = runpy.run_path(str(Path(sys.argv[1]) / 'test/simcluster/tests/hot-update-pty-probe-test.py'))
sensor, nonce = suite['SENSOR'], suite['NONCE']
with tempfile.TemporaryDirectory(prefix='pty-invariant-') as temporary:
 root = Path(temporary)
`
	cmd := exec.Command("python3", "-I", "-B", "-c", prelude+program, repo)
	output, err := testharness.CombinedOutput(cmd)
	if err != nil {
		t.Fatalf("PTY invariant rejected: %v\n%s", err, output)
	}
}

// origin: hot-update S4 internal review rv-drill DRILL-1
func TestPTYProbeWatchdogDriverProvidesRequiredResizeReceipts(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	runPTYProbeInvariant(t, `
 source = (Path(sys.argv[1]) / 'test/simcluster/drills/36-hot-update-pty.sh').read_text()
 arm = source[source.index('\nW)'):source.index('\nC)')]
 doc = suite['complete_fixture'](root)
 doc['controls'] = [c for c, action in zip(doc['controls'], ('resize-before', 'resize-after')) if '_pty_checkpoint '+action in arm]
 sensor['write_json'](root / 'evidence.json', doc)
 result = sensor['judge'](root, nonce)
 assert result['valid'] and result['continuous'], result
 assert arm.index('_pty_checkpoint resize-before') < arm.index('_pty_window start watchdog')
 assert arm.index('_pty_checkpoint resize-after') > arm.index('fault_synblock_off agt1')
`)
}

// origin: hot-update S4 internal review rv-drill DRILL-2
func TestPTYProbeCarrierOutageBudgetAllowsDeclaredFault(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	runPTYProbeInvariant(t, `
 doc = suite['complete_fixture'](root)
 doc['windows'][0].update(kind='carrier', end=27.5)
 doc['images'] = [doc['images'][0]]
 doc['images'][0].update(terminal_count=1, exit_code=23)
 for direction in ('input', 'output'):
  for side in ('sent', 'received'):
   for row in doc[direction][side]['records'][26:]: row['at'] += 25
 for row in doc['processes']:
  if row['at'] >= 3: row['at'] += 25
 doc['controls'][-1]['at'] += 25
 sensor['write_json'](root / 'evidence.json', doc)
 result = sensor['judge'](root, nonce)
 assert result['valid'] and result['continuous'], result
 bad = copy.deepcopy(doc)
 for row in bad['output']['received']['records'][38:]: row['at'] += 6
 sensor['write_json'](root / 'evidence.json', bad)
 rejected = sensor['judge'](root, nonce)
 assert rejected['valid'] and 'business_stall_output' in rejected['reason'], rejected
 bad = copy.deepcopy(doc)
 bad['windows'][0]['end'] += 155
 for direction in ('input','output'):
  for side in ('sent','received'):
   for row in bad[direction][side]['records'][26:]: row['at'] += 155
 for row in bad['processes']:
  if row['at'] >= 28: row['at'] += 155
 bad['controls'][-1]['at'] += 155
 sensor['write_json'](root / 'evidence.json', bad)
 rejected = sensor['judge'](root, nonce)
 assert rejected['valid'] and 'business_stall_input' in rejected['reason'], rejected
`)
}

// origin: hot-update S4 internal review rv-drill DRILL-3
func TestPTYProbeDrainingReapHeldLeaderIsAllowedBeforeSettlement(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	runPTYProbeInvariant(t, `
 doc = suite['complete_fixture'](root)
 doc['windows'][0].update(kind='draining', leader_exited=True, leader_exit_at=2.4, tail_s=.25, draining_end_at=4.9)
 doc['processes'] = [r for r in doc['processes'] if r['role'] != 'leader' or r['at'] < 2.4]
 for row in doc['processes']:
  if row['role'] == 'foreground' and row['at'] >= 2.4:
   row.update(ctty=False, hup=1, foreground_pgid=-1)
   row['identity']['process_group'] = row['identity']['pid']
 leader = dict(suite['process'](4100), state='Z', session_id=4100)
 doc['resources']['after'][0]['zombies'] = [leader]
 doc['resources']['after'][0]['at'] = 3.5
 sensor['write_json'](root / 'evidence.json', doc)
 result = sensor['judge'](root, nonce)
 assert result['valid'] and result['continuous'], result
 for defect in ('foreign', 'late', 'settled'):
  bad = copy.deepcopy(doc)
  if defect == 'foreign': bad['resources']['after'][0]['zombies'][0]['start_ticks'] += 1
  if defect == 'late': bad['resources']['after'][0]['at'] = 5
  if defect == 'settled': bad['resources']['settled']['zombies'] = [leader]
  sensor['write_json'](root / 'evidence.json', bad)
  assert not sensor['judge'](root, nonce)['continuous'], defect
`)
}

// origin: hot-update S4 internal review rv-drill DRILL-4
func TestPTYProbeLegacyExporterLossRemainsBusinessEvidence(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	runPTYProbeInvariant(t, `
 suite['assembly_fixture'](root)
 trace = sensor['read_json'](root / 'ctl.trace.json')
 trace['scenario'] = 'P'
 sensor['write_json'](root / 'ctl.trace.json', trace)
 records = [json.loads(line) for line in (root / 'logs/agent_slog.log').read_text().splitlines()]
 # Old exporter started a genuine original child, then bare exec lost its
 # in-memory wait owner. The new exporter cannot emit the old child's exit.
 start = dict(time=records[0]['time'], msg='agent: run', pid='task-token', argv=[nonce])
 activated = dict(time=records[4]['time'], msg='agent: upgrade COMMITTED')
 suite['save_runtime_log'](root, [start, activated])
 result = sensor['runtime_records'](root, nonce)
 assert result['protocol'] == 'legacy' and result['exits'] == [], result
 # Keep the verifier's minimal trigger and the product's full audit shape.
 activated['msg'] = 'agent: upgrade COMMITTED (registered as the new binary)'
 suite['save_runtime_log'](root, [start, activated])
 # Exercise acquisition through the public classifier, not merely parsing.
 sensor['write_json'](root / 'agent/settled.ps.json', dict(processes=[]))
 sensor['write_json'](root / 'supply.json', dict(old_sha='a'*64,new_sha='b'*64,artifact_sha='c'*64,
                  served_sha='c'*64,pid=4000,start_ticks=40000))
 sensor['write_json'](root / 'hop-1.json', dict(pid=4000,start_ticks=40000,running_sha='b'*64,disk_sha='b'*64,
       marker=dict(state='committed',new_sha='b'*64,boot_count=1,target_sid='lab',target_nid='agt1')))
 for role in ('agent', 'ctl', 'broker'):
  (root / ('endpoint-'+role+'.version')).write_text('tether v0.6.0 (proto v1)\n')
  (root / ('endpoint-'+role+'.sha')).write_text('a'*64+'  fixture\n')
 assembled = sensor['assemble_evidence'](root, nonce)
 assert assembled['legacy_loss']['terminal_missing'] and assembled['legacy_loss']['management_missing']
 judged = sensor['judge'](root, nonce)
 assert judged['valid'] and not judged['continuous'] and 'courier_terminal' in judged['reason'], judged
 import subprocess
 cli = subprocess.run(['python3', '-I', '-B', str(suite['PROBE']), 'legacy-exporter',
        '--evidence', str(root), '--nonce', nonce], capture_output=True, text=True)
 assert cli.returncode == 1 and 'management_terminal=0' in cli.stderr, (cli.returncode, cli.stderr)
 # A missing ownership record outside P, or an unreadable stream, stays invalid.
 trace['scenario'] = 'L'
 sensor['write_json'](root / 'ctl.trace.json', trace)
 try: sensor['runtime_records'](root, nonce)
 except sensor['EvidenceError']: pass
 else: raise AssertionError('positive continuity accepted missing terminal')
 trace['scenario'] = 'P'
 sensor['write_json'](root / 'ctl.trace.json', trace)
 logs = sensor['read_json'](root / 'logs.json'); logs['read_ok'] = False
 sensor['write_json'](root / 'logs.json', logs)
 try: sensor['assemble_evidence'](root, nonce)
 except sensor['EvidenceError']: pass
 else: raise AssertionError('unreadable logs were treated as legacy loss')
`)
}

// origin: hot-update S4 internal review rv-drill V1
func TestPTYProbeLegacyDriverBracketsRefusalWithResizeReceipts(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	runPTYProbeInvariant(t, `
 source = (Path(sys.argv[1]) / 'test/simcluster/drills/36-hot-update-pty.sh').read_text()
 arm = source[source.index('\nL)'):source.index('\n*) setup_fail')]
 doc = suite['complete_fixture'](root)
 doc['windows'][0]['kind'] = 'refusal'
 doc['images'] = [dict(doc['images'][0], terminal_count=1, exit_code=23)]
 doc['controls'] = [c for c, action in zip(doc['controls'], ('resize-before', 'resize-after')) if '_pty_checkpoint '+action in arm]
 sensor['write_json'](root / 'evidence.json', doc)
 result = sensor['judge'](root, nonce)
 assert result['valid'] and result['continuous'], result
 assert arm.index('_pty_checkpoint resize-before') < arm.index('_pty_window start refusal')
 assert arm.index('_pty_checkpoint resize-after') > arm.index('_pty_window end refusal')
`)
}
