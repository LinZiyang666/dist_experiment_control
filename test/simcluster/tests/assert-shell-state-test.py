#!/usr/bin/env python3
"""Positive and adversarial controls for assertion shell ownership.

origin: hot-update S4 plan §9.1–§9.3 / DRILL-SUBSHELL.
Fixtures are source data; no drill or deployment command is ever executed.
The behavioral tests use the real assertion library and isolated shell jobs.
"""
import importlib.util
from pathlib import Path
import subprocess
import re
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('assert_shell_state', HERE / 'assert-shell-state.py')
GUARD = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = GUARD
SPEC.loader.exec_module(GUARD)


class AssertionShellStateTests(unittest.TestCase):
    def check_fixture(self, drill, library=''):
        with tempfile.TemporaryDirectory(prefix='assert-shell-state-') as tmp:
            root = Path(tmp)
            (root / 'lib').mkdir()
            (root / 'fixture.sh').write_text(drill)
            (root / 'lib' / 'fixture.sh').write_text(library)
            return GUARD.scan(root)

    def test_artifact_globals_cannot_be_established_in_capture(self):
        findings = self.check_fixture('''
assert_setup "serve real distinct candidate" artifact_up instance binary archive
printf '%s %s' "$ART_URL" "$ART_SHA"
''', '''
artifact_up() {
    ART_URL=https://instance-artifact/archive
    ART_SHA=real-sha
}
''')
        self.assertEqual(len(findings), 1)
        self.assertIn('ART_URL', findings[0])
        self.assertIn('ART_SHA', findings[0])

    def test_fixture_job_and_globals_cannot_escape_capture(self):
        findings = self.check_fixture('''
JOB=
assert_setup "real controlling tty and one original ctl" fixture_start directory nonce binary
wait "$JOB"
printf '%s' "$EVIDENCE"
''', '''
fixture_start() {
    EVIDENCE=$1
    sleep 1 > /dev/null 2>&1 & JOB=$!
}
''')
        self.assertEqual(len(findings), 1)
        self.assertIn('exports background job JOB', findings[0])
        self.assertIn('EVIDENCE', findings[0])

    def test_quadrants_follow_transitive_supply_calls(self):
        findings = self.check_fixture('''
quadrants() {
    ART_URL=new-supply
    fixture_start directory
    EV=gate-directory
}
gate() { printf '%s %s' "$ART_URL" "$EV"; }
assert_setup "L new ctl old agent and old broker fallback quadrants" quadrants
gate
wait "$JOB"
''', '''
fixture_start() {
    sleep 1 > /dev/null 2>&1 & JOB=$!
}
''')
        self.assertEqual(len(findings), 1)
        self.assertIn('fixture_start exports background job JOB', findings[0])
        self.assertIn('EV', findings[0])

    def test_legacy_gate_keeps_job_and_cleanup_flag_in_parent(self):
        findings = self.check_fixture('''
gate() {
    upgrade legacy-gate & JOB=$!
    COMMIT_FAULT=1
    wait "$JOB"
    COMMIT_FAULT=0
}
cleanup() { test "$COMMIT_FAULT" = 0; }
assert_ok "L gate closed legacy75 and zero spawn receipts" gate
wait "$JOB"
''')
        self.assertEqual(len(findings), 1)
        self.assertIn('exports background job JOB', findings[0])
        self.assertIn('COMMIT_FAULT', findings[0])

    def test_wait_for_caller_child_cannot_run_in_capture(self):
        findings = self.check_fixture('''
finish() { wait "$JOB"; }
sleep 1 > /dev/null 2>&1 & JOB=$!
assert_setup "P retain original waitpid and raw evidence even if foreground died" finish
''')
        self.assertEqual(len(findings), 1)
        self.assertIn('waits for caller-owned JOB', findings[0])

    def test_wrapper_poll_and_all_capturing_primitives_are_inspected(self):
        for primitive, metadata in [('assert_ok', ''), ('assert_setup', ''),
                                    ('assert_refuses', '"reason"'), ('assert_bug', '"owner" "signature"')]:
            with self.subTest(primitive=primitive):
                findings = self.check_fixture(f'''
start() {{ GLOBAL=value; }}
wrapper() {{ start; }}
assertion_name_in_comment=ignored
# assert_ok "comment" wrapper
{primitive} "claim" {metadata} \\
    poll_until 1 1 "ready" -- wrapper
printf '%s' "$GLOBAL"
''')
                self.assertEqual(len(findings), 1)
                self.assertIn('start assigns externally consumed GLOBAL', findings[0])

    def test_shell_state_builtins_cannot_run_in_captured_predicate(self):
        for command in ['cd /tmp', 'trap cleanup EXIT', 'export SUPPLY=value', 'exec program',
                        '. library.sh', 'source library.sh']:
            with self.subTest(command=command):
                findings = self.check_fixture(f'''start() {{ {command}; }}
assert_ok "claim" start
''')
                self.assertEqual(len(findings), 1)
                self.assertIn('changes shell state', findings[0])

    def test_direct_establishment_and_pure_status_assertions_pass(self):
        self.assertEqual(self.check_fixture('''
artifact_up() { ART_SHA=real; }
fixture_start() { sleep 1 > /dev/null 2>&1 & JOB=$!; }
quadrants() { EV=next; artifact_up; }
gate() { upgrade & GATE_JOB=$!; wait "$GATE_JOB"; }
rc=0; artifact_up || rc=$?
assert_setup "serve real distinct candidate" test "$rc" = 0
rc=0; fixture_start || rc=$?
assert_setup "real controlling tty and one original ctl" test "$rc" = 0
rc=0; quadrants || rc=$?
assert_setup "L new ctl old agent and old broker fallback quadrants" test "$rc" = 0
rc=0; gate || rc=$?
assert_ok "L gate closed legacy75 and zero spawn receipts" test "$rc" = 0
printf '%s %s' "$ART_SHA" "$EV"
wait "$JOB"
'''), [])

    def test_remote_programs_heredocs_scratch_and_self_contained_jobs_pass(self):
        self.assertEqual(self.check_fixture('''
remote() {
    dexec node -- sh -c 'cd /tmp; trap cleanup EXIT; export X=v; exec program &'
    dexec node -- sh -c "job & pid=$!; wait"
    python3 - <<'PAYLOAD'
assert_ok "not shell code" unsafe
GLOBAL=value
job & JOB=$!
PAYLOAD
    count=1
    printf '%s' "$count"
}
other() { count=2; printf '%s' "$count"; }
joined() { job > /dev/null 2>&1 & JOB=$!; wait "$JOB"; }
assert_ok "remote state is remote" remote
assert_ok "scratch state is private to invocation" other
assert_ok "joined job belongs to this invocation" joined
'''), [])

    def test_imported_driver_predicate_cannot_hide_state_changes(self):
        with tempfile.TemporaryDirectory(prefix='assert-shell-state-import-') as tmp:
            root = Path(tmp)
            driver = root / 'driver.sh'
            driver.write_text('start() { SUPPLY=real; }\n')
            caller = root / 'caller.sh'
            caller.write_text('''. "$HERE/drills/driver.sh"
assert_setup "imported fixture" start
printf '%s' "$SUPPLY"
''')
            findings = GUARD.inspect(caller, [])
            self.assertEqual(len(findings), 1)
            self.assertIn('start assigns externally consumed SUPPLY', findings[0])

    # origin: S8 internal review s8-hermetic F9. Captured legacy setup
    # must not claim to return parent shell state.
    def test_current_pty_programs_have_zero_findings(self):
        root = HERE.parent / 'drills'
        libs = [GUARD.shell_source(path) for path in sorted((root / 'lib').glob('*.sh'))]
        for name in ['36-hot-update-pty.sh', '39-hot-update-legacy-n1.sh']:
            with self.subTest(name=name):
                self.assertEqual(GUARD.inspect(root / name, libs), [])

    def run_shell(self, program):
        with tempfile.TemporaryDirectory(prefix='assert-shell-state-runtime-') as tmp:
            prelude = f'''
set -u
. {shlex_quote(str(HERE.parent / 'lib' / 'log.sh'))}
. {shlex_quote(str(HERE.parent / 'lib' / 'assert.sh'))}
INSTANCE=drill-hermetic-shell-state
SIM_EVIDENCE_DIR={shlex_quote(tmp)}
drill_begin ownership
'''
            return subprocess.run(['sh', '-c', prelude + program], text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=10)

    def test_direct_steps_preserve_globals_real_child_wait_and_status(self):
        result = self.run_shell('''
for round in 1 2 3 4 5; do
    establish() { SUPPLY=real; (exit 23) > /dev/null 2>&1 & JOB=$!; }
    rc=0; establish || rc=$?
    assert_setup "fixture establishes shell state" test "$rc" = 0
    assert_ok "supply remains in caller" test "$SUPPLY" = real
    child_rc=0; wait "$JOB" || child_rc=$?
    assert_ok "caller reaps original child status" test "$child_rc" = 23
done
drill_end
''')
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn('verdict=GREEN', result.stdout)
        self.assertIn('pass=15', result.stdout)

    def test_each_saved_step_exposes_failure_and_resets_previous_status(self):
        # Exercise the real saved-status statements against an adversarial
        # hermetic operation. A dropped reset, dropped rc save, or always-true
        # assertion must fail without running the deployment operation itself.
        source = (HERE.parent / 'drills' / '36-hot-update-pty.sh').read_text()
        lines = source.splitlines()
        # origin: hot-update S4 plan §9.1 / T48; drill-run4 diagnostics.
        # Exercise the actual diagnostic/status adapter as well as the saved
        # rc; an undefined helper or an always-successful adapter must be red.
        helpers = source[source.index('_pty_step_run() {'):source.index('_pty_broker_ready() {')]
        pairs = []
        for i, line in enumerate(lines):
            if '_pty_step_rc=0;' not in line:
                continue
            match = re.fullmatch(r'\s*(_pty_step_rc=0; )(.+)( \|\| _pty_step_rc=\$\?)', line)
            self.assertIsNotNone(match, line)
            assertion = lines[i+1].strip()
            self.assertRegex(assertion, r'^assert_(setup|ok) ')
            operation = '_pty_step_run step' if match[2].startswith('_pty_step_run ') else 'step'
            pairs.append((match[1] + operation + match[3], assertion))
        self.assertTrue(pairs, 'no saved-status steps exercised')
        self.assertEqual(len(pairs), sum('assert_' in line and '"$_pty_step_rc"' in line for line in lines))
        for saver, assertion in pairs:
            with self.subTest(assertion=assertion):
                for status in (0, 17):
                    result = self.run_shell(f'''
PTY_EV=$SIM_EVIDENCE_DIR
{helpers}
step() {{ HELD=retained; printf 'original failure detail\\n' >&2; return {status}; }}
_pty_step_rc=23
{saver}
{assertion}
[ "${{HELD:-}}" = retained ] || exit 98
drill_end
''')
                    expected = 0 if status == 0 else (2 if assertion.startswith('assert_setup ') else 1)
                    self.assertEqual(result.returncode, expected, result.stdout)
                    if status:
                        self.assertIn('original failure detail', result.stdout)

    def test_capture_reproduces_lost_global_and_unwaitable_child(self):
        result = self.run_shell('''
for round in 1 2 3 4 5; do
    SUPPLY=before
    mutate() { SUPPLY=after; }
    assert_ok "captured mutation finishes" mutate
    [ "$SUPPLY" = before ] || exit 98
    (exit 23) > /dev/null 2>&1 & JOB=$!
    finish() { wait "$JOB"; }
    _as_capture finish
    [ "$_AS_RC" = 127 ] || exit 97
    child_rc=0; wait "$JOB" || child_rc=$?
    [ "$child_rc" = 23 ] || exit 96
done
drill_end
''')
        self.assertEqual(result.returncode, 0, result.stdout)

    # origin: S8 internal review s8-hermetic F9. Scope scratch in the
    # installer and select the externally consumed ctl in every caller.
    def test_legacy_drivers_select_control_path_before_capture(self):
        for name, selection in (("36-hot-update-pty.sh", "$1"),
                                ("39-hot-update-legacy-n1.sh", "$PREV_BIN"),
                                ("75-hot-update-flows.sh", "$FLOW_LEGACY_CTL")):
            with self.subTest(driver=name):
                source = (HERE.parent/"drills"/name).read_text()
                pattern = (r"LEGACY_CTL_BIN=" + re.escape(selection) + r"\n\s*assert_setup "
                           r"[^\n]*upgrade_legacy_stack [^\n]*" + re.escape('"'+selection+'"'))
                self.assertEqual(len(re.findall(pattern, source)), 1, "caller selection is missing or detached from installation")

    # origin: S8 internal review s8-hermetic F9. Installation may run in
    # assert_setup's capture, but selection belongs to the parent shell.
    def test_legacy_stack_keeps_caller_selection_and_snapshot_scratch(self):
        library = HERE.parent / "drills/lib/hot_update_pty.sh"
        source = library.read_text()
        stack = re.search(r"^upgrade_legacy_stack\(\) \{\n.*?^}\n", source, re.M | re.S).group()
        ctl = re.search(r"^upgrade_legacy_ctl\(\) \{\n.*?^}\n", source, re.M | re.S).group()
        with tempfile.TemporaryDirectory(prefix="legacy-stack-state-") as directory:
            root = Path(directory)
            (root/"previous").write_bytes(b"isolated source bytes")
            (root/"previous").chmod(0o700)
            (root/"source.json").write_text('{"synthetic":true}')
            script = """
root=$1; . "$2/../lib/assert.sh"
log() { :; }; err() { :; }; ok() { :; }
d() { :; }; sctl() { :; }; ctr_name() { printf '%s' "$1"; }
dexec() {
    if [ "$3" = sha256sum ]; then sha256sum "$root/previous"
    elif [ "$1" = -u ]; then printf 'ctl-selected=%s\\n' "$7"
    fi
}
INSTANCE=drill-hermetic-legacy-stack; drill_begin legacy-stack
""" + stack + ctl + """
for round in 1 2 3 4 5; do
    LEGACY_CTL_BIN=/caller/selected-ctl; _uls_node=caller-snapshot-node
    assert_setup "install remote source with private scratch" upgrade_legacy_stack "$root/previous" "$root/source.json" /requested/installer-ctl "$root"
    [ "$LEGACY_CTL_BIN" = /caller/selected-ctl ] || exit 98
    [ "$_uls_node" = caller-snapshot-node ] || exit 97
    upgrade_legacy_ctl version || exit 96
done
drill_end
"""
            result = subprocess.run(["sh", "-c", script, "legacy-control", str(root), str(HERE)],
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
            self.assertEqual(result.stdout.count('ctl-selected=/caller/selected-ctl'), 5)
            self.assertTrue((root/"legacy-source.sha").is_file())

    def test_failed_direct_setup_keeps_setup_red_and_original_description(self):
        result = self.run_shell('''
fail_step() { printf 'real failure detail\\n'; return 17; }
rc=0; fail_step || rc=$?
assert_setup "serve real distinct candidate" test "$rc" = 0
printf 'UNREACHABLE\\n'
''')
        self.assertEqual(result.returncode, 2, result.stdout)
        self.assertIn('serve real distinct candidate', result.stdout)
        self.assertIn('real failure detail', result.stdout)
        self.assertIn('verdict=SETUP-RED', result.stdout)
        self.assertNotIn('UNREACHABLE', result.stdout)

    def test_failed_direct_product_step_keeps_assert_fail(self):
        result = self.run_shell('''
fail_step() { return 23; }
rc=0; fail_step || rc=$?
assert_ok "L gate closed legacy75 and zero spawn receipts" test "$rc" = 0
drill_end
''')
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn('verdict=ASSERT-FAIL', result.stdout)
        self.assertIn('assert_fail=1', result.stdout)


def shlex_quote(value):
    import shlex
    return shlex.quote(value)


if __name__ == '__main__':
    unittest.main(verbosity=2)
