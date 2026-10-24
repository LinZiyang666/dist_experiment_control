//go:build linux

package cli_e2e_test

import "github.com/LinZiyang666/tether/internal/testharness"
import "path/filepath"
import "testing"

// origin: hot-update S4 internal review rv-drill DRILL-6
func TestPTYLegacyPendingGateRejectsNoResponderReceipt(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	runPTYProbeInvariant(t, `
 import re, subprocess
 source = (Path(sys.argv[1]) / 'test/simcluster/drills/36-hot-update-pty.sh').read_text()
 function = re.search(r'(?ms)^_pty_legacy_gate_valid\(\) \{.*?^\}', source).group(0)
 # v0.6.0 classifies agent_no_responders as rc75 too. No request reached
 # handleRunForwarded; zero Start/effect is caused by transport unreachability.
 receipt = root / 'legacy-gate.json'
 positive = dict(rc=75, spawn_lines=0, effect_absent=True, pending=True,
                 reason='agent_upgrading', candidate_unchanged=True)
 def invoke(row):
  receipt.write_text(json.dumps(row))
  return subprocess.run(['sh', '-c', function + '\n_pty_legacy_gate_valid "$1"',
                          'sh', str(receipt)], capture_output=True, text=True)
 assert invoke(positive).returncode == 0, 'real admission refusal was rejected'
 assert invoke(dict(positive, reason='agent_no_responders')).returncode != 0, 'unreachable agent was credited as an admission-gate rejection'
 for field, value in (('rc',70), ('candidate_unchanged',False), ('pending',False), ('spawn_lines',1), ('effect_absent',False)):
  assert invoke(dict(positive, **{field:value})).returncode != 0, field
 gate = re.search(r'(?ms)^_pty_legacy_gate\(\) \{.*?^\}', source).group(0)
 request = gate.index('"$PREV_BIN" run')
 assert gate.index('fault_synblock_off agt1') < request, 'admission request sent while unreachable'
 assert gate.index('chmod 0555') < gate.index('fault_synblock_off agt1')
 assert gate.index('_pty_legacy_gate_valid "$PTY_EV/legacy-gate.json"') < gate.index('chmod 0755')
 reason_line = next(line.strip() for line in gate.splitlines() if line.strip().startswith('_plg_reason=$('))
 for output, reason in (('run rejected by agent (agent_upgrading)', 'agent_upgrading'),
                        ('run failed: upgrade in progress (agent_upgrading: image switch)', 'agent_upgrading'),
                        ('run failed: the broker hit a transient storage hiccup (not a permanent failure); wait a moment and retry. (try_again: agent_upgrading: image switch in progress; retry)', 'agent_upgrading'),
                        ('run failed: an image switch is in progress on this node; retry in a few seconds. (try_again: agent_upgrading: image switch in progress; retry)', 'agent_upgrading'),
                        ('run rejected by agent (agent_no_responders)', ''),
                        ('run failed: storage hiccup (try_again)', ''),
                        ('run failed: storage hiccup (try_again: agent_no_responders: agent_upgrading)', ''),
                        ('run failed: storage hiccup (try_again: agent_upgrading_other)', '')):
  (root / 'legacy-gate.out').write_text(output+'\n')
  extracted = subprocess.run(['sh','-c', 'PTY_EV="$1"\n'+reason_line+'\nprintf "%s" "$_plg_reason"',
                             'sh',str(root)],capture_output=True,text=True)
  assert extracted.returncode == 0 and extracted.stdout == reason, (output, extracted.stdout, extracted.stderr)

`)
}
