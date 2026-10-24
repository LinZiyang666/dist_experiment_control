#!/bin/sh
# The real scanner and its CLI are exercised without running installer code.
# origin: hot-update S9 plan §1 / §6.4 / P37 / T88 / M34
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
[ "$#" -le 2 ] || { echo "usage: lint-install-selftest.sh [test-name [install-script]]" >&2; exit 2; }
. "$HERE/../lib/supervisor.sh"
SELFTEST_SUPERVISOR_DIR=
trap '[ -z "$SELFTEST_SUPERVISOR_DIR" ] || rm -rf "$SELFTEST_SUPERVISOR_DIR"' EXIT
if [ -z "${SIM_UPGRADE_SUPERVISOR:-}" ]; then
    SELFTEST_SUPERVISOR_DIR=$(mktemp -d)
    SIM_UPGRADE_SUPERVISOR=$(sim_supervisor_build "$(cd "$HERE/../../.." && pwd)" "$SELFTEST_SUPERVISOR_DIR")
    export SIM_UPGRADE_SUPERVISOR
fi
[ -x "$SIM_UPGRADE_SUPERVISOR" ] || { echo "lint selftest: shared supervisor is required" >&2; exit 2; }
"$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B - "$HERE" "${1:-all}" "${2:-}" <<'PYSELFTEST'
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest

HERE = Path(sys.argv[1])
LINT = HERE / 'lint-install.sh'
INSTALLER = Path(sys.argv[3]) if sys.argv[3] else HERE.parents[2] / 'scripts/install.sh'
SCANNER = types.ModuleType('installer_scanner')
# There is one parser, the one shipped in the shell entry. No sibling imports
# or ambient PYTHONPATH are needed in the isolated interpreter.
source = LINT.read_text().split("<<'PYINSTALL'\n", 1)[1].rsplit('\nPYINSTALL', 1)[0]
exec(compile(source, str(LINT), 'exec'), SCANNER.__dict__)


class InstallerHeredocControls(unittest.TestCase):
    def cli(self, text):
        with tempfile.TemporaryDirectory(prefix='infra-install-lint-') as directory:
            path = Path(directory) / 'install.sh'
            path.write_text(text)
            before = path.read_bytes()
            result = subprocess.run([os.environ['SIM_UPGRADE_SUPERVISOR'], '--', 'sh', str(LINT), str(path)],
                                    capture_output=True, text=True, timeout=30)
            self.assertEqual(path.read_bytes(), before, 'lint changed its input')
            return result

    def test_exact_template_targetset(self):
        text = INSTALLER.read_text()
        documents = SCANNER.check(text)
        self.assertEqual(len(documents), 21)
        result = self.cli(text)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        # Missing and extra identities, count-neutral swaps and duplicates must
        # all fail even though the old >=10 nonempty guard would stay green.
        target = '$INFRA_ETC_DIR/templates/tether-infra-helper.socket'
        for bad in (text.replace(target, target + '.unknown'),
                    text.replace('$INFRA_ETC_DIR/templates/tether-infra-helper@.service', target),
                    text.replace('for _wif_component in nats caddy;', 'for _wif_component in caddy;'),
                    text.replace('_wif_unit=nats-server;', '_wif_unit=unknown;'),
                    text.replace('} > "$_dropin"', '} > "$_unknown"'),
                    text + '\ncat <<EOF\nextra banner\nEOF\n'):
            with self.subTest(kind='missing-extra-duplicate'):
                with self.assertRaises(ValueError):
                    SCANNER.check(bad)
                self.assertEqual(self.cli(bad).returncode, 1)
        # Remove the new template completely, including its redirection. A
        # source-name check by itself cannot detect that its body disappeared.
        start = text.index('    config_dest "' + target + '"')
        end = text.index('\nEOF\n', start) + len('\nEOF\n')
        bad = text[:start] + text[end:]
        self.assertEqual(self.cli(bad).returncode, 1)

    def test_multiple_heredocs_do_not_hide_first_body(self):
        for second in ("'B'", '"B"', r'\B', 'B'):
            text = 'cat << A <<' + second + '\n$(unsafe_root_command)\nA\nliteral\nB\n'
            documents = SCANNER.scan(text)
            self.assertEqual([row['delimiter'] for row in documents], ['A', 'B'])
            self.assertFalse(documents[0]['quoted'])
            self.assertTrue(SCANNER.substitutions(documents[0]['body']))
        # Mutate the actual installer: the second, inert body cannot make an
        # unsafe first body disappear. Both documents are supplied on disk.
        text = INSTALLER.read_text().replace('    cat <<EOF\n', "    cat <<EOF <<'B'\n$(unsafe_root_command)\n", 1)
        text = text.replace('\nEOF\n', '\nEOF\nliteral second body\nB\n', 1)
        result = self.cli(text)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn('command substitution', result.stderr)

    def test_only_delimiter_quotes_make_body_inert(self):
        for opener in ('cat "<<FAKE" <<REAL', "printf '%s' '<<FAKE'; cat << REAL",
                       'cat << REAL # <<\'FAKE\'', 'cat <<REAL; printf "quoted command"'):
            documents = SCANNER.scan(opener + '\n`unsafe`\nREAL\n')
            self.assertEqual(len(documents), 1)
            self.assertEqual(documents[0]['delimiter'], 'REAL')
            self.assertFalse(documents[0]['quoted'])
        for delimiter in ("'EOF'", '"EOF"', r'\EOF', "E'O'F"):
            document, = SCANNER.scan('cat <<' + delimiter + '\n$(literal)\nEOF\n')
            self.assertTrue(document['quoted'])
        # Removing a backslash-newline is not shell quote removal. The real
        # installer mutation keeps its targetset and still must reject its body.
        # origin: hot-update S9 plan §12 P37 / T88 / M34
        for delimiter in ('E\\\nOF', '\\\nEOF'):
            document, = SCANNER.scan('cat <<' + delimiter + '\n$(unsafe)\nEOF\n')
            self.assertEqual(document['delimiter'], 'EOF')
            self.assertFalse(document['quoted'])
            text = INSTALLER.read_text().replace('    cat <<EOF\n',
                                                 '    cat <<' + delimiter + '\n$(unsafe_root_command)\n', 1)
            result = self.cli(text)
            self.assertEqual(result.returncode, 1)
            self.assertIn('command substitution', result.stderr)
        text = INSTALLER.read_text().replace('    cat <<EOF\n', '    cat "<<FAKE" <<EOF\n$(unsafe_root_command)\n', 1)
        result = self.cli(text)
        self.assertEqual(result.returncode, 1)
        self.assertIn('command substitution', result.stderr)

    def test_delimiters_and_tabs_fail_closed(self):
        text = 'cat <<-END\n\tvalue\n\tEND\n'
        document, = SCANNER.scan(text)
        self.assertEqual(document['body'], '\tvalue\n')
        for bad in ('cat <<EOF\nx\n EOF\n', 'cat <<-EOF\nx\n EOF\n',
                    'cat <<EOF\nx\nEOF \n', 'cat <<EOF\nx\n', 'cat <<$EOF\nx\nEOF\n',
                    'cat <<EOF <<B\nx\nEOF\n'):
            with self.subTest(text=bad), self.assertRaises(ValueError):
                SCANNER.scan(bad)
        # A delimiter-looking line with spaces remains body and must not hide
        # the substitution that follows it.
        document, = SCANNER.scan('cat <<EOF\n EOF\n$(unsafe)\nEOF\n')
        self.assertTrue(SCANNER.substitutions(document['body']))
        document, = SCANNER.scan('cat <<EOF\ncontinued\\\nEOF\n$(unsafe)\nEOF\n')
        self.assertIn('$(unsafe)', document['body'])
        self.assertTrue(SCANNER.substitutions(document['body']))

    def test_body_escape_parity_and_continuations(self):
        for text in ('`command`', '$(command)', "'$(command)'", '"`command`"',
                     r'\\$(command)', r'\\`command`', '$\\\n(command)'):
            self.assertTrue(SCANNER.substitutions(text), repr(text))
        for text in (r'\$(literal)', r'\`literal\`', '$variable', '\\\n', r'\\\$(literal)'):
            self.assertFalse(SCANNER.substitutions(text), repr(text))

    def test_cli_rejects_broken_syntax_and_empty_input(self):
        for text in ('', 'if then\n', '# only a comment <<EOF\n'):
            self.assertNotEqual(self.cli(text).returncode, 0)
        # Unknown selection must never certify an empty test suite.

    def test_shell_strings_and_arithmetic_are_not_heredocs(self):
        text = 'value=$((1 << 2))\nprintf "%s" "<<literal"\n# <<comment\ncat <<EOF\nvalue\nEOF\n'
        document, = SCANNER.scan(text)
        self.assertEqual(document['delimiter'], 'EOF')
        for text in ('value=$(cat <<EOF\nbody\nEOF\n)\n', 'value="$(cat <<EOF\nbody\nEOF\n)"\n',
                     'value=`cat <<EOF\nbody\nEOF\n`\n', 'value="`cat <<EOF\nbody\nEOF\n`"\n',
                     'value=$((1 + $(cat <<EOF\nbody\nEOF\n)))\n'):
            with self.assertRaisesRegex(ValueError, 'nested heredoc'):
                SCANNER.scan(text)
        with self.assertRaises(ValueError):
            SCANNER.scan('cat <<`EOF`\n$(unsafe)\nEOF\n')

    def test_infra_templates_render_only_under_test_root(self):
        # Render the actual scanned bodies, with ordinary shell expansion and
        # redirected seams, to stdout only. The installer dispatch and unit
        # activation never run; the safe-body check precedes every shell spawn.
        documents = [row for row in SCANNER.check(INSTALLER.read_text()) if row['owner'] == 'write_infra_files']
        with tempfile.TemporaryDirectory(prefix='infra-template-render-') as directory:
            root = Path(directory)
            env = dict(os.environ, INFRA_RUN_DIR=str(root / 'run/tether-infra'),
                       LIBEXEC_DIR=str(root / 'usr/local/libexec'), INFRA_MAX_CONNECTIONS='4',
                       INFRA_WORKER_LIFETIME_S='15', INFRA_WORKER_STOP_S='5')
            rendered = {}
            for row in documents:
                components = ('nats', 'caddy') if '$_wif_unit' in row['target'] else ('',)
                for component in components:
                    script = 'set -eu\ncat <<' + row['delimiter'] + '\n' + row['body'] + row['delimiter'] + '\n'
                    result = subprocess.run([os.environ['SIM_UPGRADE_SUPERVISOR'], '--', 'sh', '-c', script],
                                            env=dict(env, _wif_component=component,
                                                     _wif_caps='CAP_NET_BIND_SERVICE' if component == 'caddy' else ''),
                                            capture_output=True, text=True, timeout=30)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    name = component or row['target'].rsplit('/', 1)[1]
                    rendered[name] = result.stdout
            self.assertEqual(set(rendered), {'tether-infra-helper.socket', 'tether-infra-helper@.service', 'nats', 'caddy'})
            socket = rendered['tether-infra-helper.socket']
            for field in ('ListenStream=' + env['INFRA_RUN_DIR'] + '/control.sock', 'Accept=yes', 'SocketUser=root',
                          'SocketGroup=tether', 'SocketMode=0660', 'MaxConnections=4'):
                self.assertIn(field + '\n', socket)
            worker = rendered['tether-infra-helper@.service']
            for field in ('ExecStart=' + env['LIBEXEC_DIR'] + '/tether-infra-helper worker', 'User=root',
                          'Restart=no', 'KillMode=control-group', 'RuntimeMaxSec=15s', 'TimeoutStopSec=5s'):
                self.assertIn(field + '\n', worker)
            for component in ('nats', 'caddy'):
                text = rendered[component]
                for field in ('User=tether', 'Group=tether', 'NoNewPrivileges=yes', 'Restart=always', 'RestartSec=2',
                              'ExecStart=' + env['LIBEXEC_DIR'] + '/tether-infra-launch --component=' + component):
                    self.assertIn(field + '\n', text)
                capability = 'CAP_NET_BIND_SERVICE' if component == 'caddy' else ''
                for field in ('AmbientCapabilities', 'CapabilityBoundingSet'):
                    self.assertEqual([line for line in text.splitlines() if line.startswith(field + '=')],
                                     [field + '=', field + '=' + capability])
            self.assertEqual(list(root.iterdir()), [], 'template rendering wrote or installed a file')


selection = sys.argv[2]
suite = unittest.defaultTestLoader.loadTestsFromTestCase(InstallerHeredocControls)
if selection != 'all':
    suite = unittest.TestSuite(test for test in suite if test._testMethodName == selection)
if not suite.countTestCases():
    raise SystemExit('unknown installer lint test selection')
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() else 1)
PYSELFTEST
