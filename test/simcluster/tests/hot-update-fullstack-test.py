#!/usr/bin/env -S python3 -I -B
"""Hermetic runner controls and exact S10 Python execution reconciliation.

origin: hot-update S10 plan §9.1, §10.2 P32, §12.3
Only sandbox copies of the runner and synthetic command protocols execute.
--runner-only verifies this package; the default entry also requires every
upstream Python selftest. Missing files produce NOT_RUN and native rc2.
"""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import runpy
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import unittest

SIM = Path(__file__).resolve().parents[1]
SELF = "tests/hot-update-fullstack-test.py"
# Exact full paths, rather than counts, bind the frozen §10 file union. Do not
# dynamically bless a new test by discovering it: its owner must register it.
PYTHON_SELFTESTS = {line.split()[2]: line.split()[3]
    for line in (SIM / "tests/hot-update-fullstack-test.sh").read_text().splitlines()
    if line.startswith("# selftest-owner ")}
BOUNDS = {"C1": (2400, 4800, 2048), "C3": (2400, 4800, 2048),
          "A": (6000, 12000, 4096), "B": (6000, 12000, 4096),
          "S1": (2700, 5400, 2048), "S3": (2700, 5400, 2048),
          "F1": (2700, 5400, 2048), "F3": (2700, 5400, 2048),
          "K": (1800, 3600, 1)}
FULLSTACK = "99-hot-update-fullstack"

def write(path, value, executable=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")
    if executable:
        path.chmod(0o700)

def command_for_shebang(path):
    """Honor a Python test's interpreter, including non-executable source files."""
    line = path.read_text(encoding="utf-8").splitlines()[0]
    words = shlex.split(line[2:]) if line.startswith("#!") else []
    if words[:2] == ["/usr/bin/env", "-S"]:
        words.pop(1)
    if words[:2] == ["/usr/bin/env", "python3"]:
        flags = words[2:]
    elif words and words[0] in ("/usr/bin/python3", "/usr/local/bin/python3"):
        flags = words[1:]
    else:
        raise ValueError("selftest needs a Python3 shebang: " + str(path))
    if any(flag not in ("-I", "-B") for flag in flags):
        raise ValueError("unsupported selftest shebang flags: " + str(path))
    # Isolated imports and no bytecode keep gate runs independent of a user's
    # PYTHONPATH and keep the source tree free of generated __pycache__ files.
    return words + [flag for flag in ("-I", "-B") if flag not in flags] + [str(path)]

def bounded_command(command, *, cwd, env=None, timeout=15):
    """The P05 waiter, not a Python process group, owns every descendant."""
    # origin: S10 internal review rv-honesty F6
    launcher = os.environ.get("FULLSTACK_SUPERVISE", "")
    if not Path(launcher).is_absolute() or not os.access(launcher, os.X_OK):
        raise ValueError("FULLSTACK_SUPERVISE must name the built P05 supervise launcher")
    if env is not None:
        env = dict(env, FULLSTACK_SUPERVISE=launcher)
    with subprocess.Popen([launcher, "--", *command], cwd=cwd, env=env,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as process:
        try:
            output, _ = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            # TERM cancels the launcher's context; its shared waiter reaps even
            # setsid descendants and retains native125 for emergency cleanup.
            process.terminate()
            output, _ = process.communicate()
            return process.returncode or 124, output
        return process.returncode, output

def discovered_selftests(root):
    paths = set()
    for directory in ("drills/lib", "lib", "tests"):
        for path in (root / directory).glob("*.py"):
            name = path.name
            if ((name.startswith("fullstack_") and name.endswith("_test.py")) or
                    (name.startswith("fullstack-") and name.endswith("-test.py")) or
                    name == "hot-update-fullstack-test.py"):
                paths.add(path.relative_to(root).as_posix())
    return paths

def run_all_registration_problems(source):
    loop = re.search(r"(?m)^for t in (.+); do$", source)
    names = loop[1].split() if loop else []
    problems = []
    for name in ("hot-update-fullstack-test", "hot-update-fullstack-logs-test", "lint-install"):
        if names.count(name) != 1:
            problems.append("run-all must invoke exactly once: " + name)
    return problems
class ReceiptResult(unittest.TextTestResult):
    # origin: S10 internal review rv-honesty F7
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_ids = []
    def startTest(self, test):
        self.test_ids.append(test.id().removeprefix("__main__."))
        super().startTest(test)
    def receipt(self):
        return dict(ids=self.test_ids, run=self.testsRun, skip=len(self.skipped),
                    fail=len(self.failures) + len(self.expectedFailures) + len(self.unexpectedSuccesses),
                    error=len(self.errors))

def selftest_worker(path, receipt_path):
    # Run the real __main__ entry; never discover tests in lieu of execution.
    # A print-and-exit program cannot obtain a structured unittest receipt.
    reports, original = [], unittest.TextTestRunner.run
    def run(runner, suite):
        runner.resultclass = ReceiptResult
        result = original(runner, suite)
        reports.append(result.receipt())
        return result
    unittest.TextTestRunner.run = run
    sys.argv = [str(path)]
    rc = 0
    try:
        runpy.run_path(str(path), run_name="__main__")
    except SystemExit as error:
        rc = error.code if type(error.code) is int else (0 if error.code is None else 1)
    except BaseException:
        rc = 1
        raise
    finally:
        Path(receipt_path).write_text(json.dumps(dict(reports=reports, native_rc=rc)))
    return rc

def required_test_ids(relative):
    # This source-controlled list is declared before execution, not derived
    # from whichever methods happen to survive discovery in the candidate.
    lines = (SIM / "tests/hot-update-fullstack-test.sh").read_text().splitlines()
    return {line.split()[3] for line in lines if line.startswith("# required-test " + relative + " ")}

def receipt_complete(relative, report):
    required = required_test_ids(relative)
    return (bool(required) and type(report) is dict and set(report) == {"ids", "run", "skip", "fail", "error"}
            and type(report['ids']) is list and all(type(i) is str for i in report['ids']) and len(report['ids']) == len(set(report['ids']))
            and set(report['ids']) == required and report['run'] == len(required)
            and all(type(report[k]) is int for k in ('run', 'skip', 'fail', 'error'))
            and report['skip'] == report['fail'] == report['error'] == 0)

def reconcile_selftests(root, own_rc, own_count, own_output=None, own_wall_s=None, own_command=None, own_report=None):
    """Return actual per-path receipts and fail both missing and extra entries."""
    present = discovered_selftests(root)
    expected = set(PYTHON_SELFTESTS)
    problems = ["NOT_RUN pending " + PYTHON_SELFTESTS[path] + ": " + path
                for path in sorted(expected - present)]
    problems += ["unregistered Python selftest: " + path for path in sorted(present - expected)]
    receipts = []
    for relative in sorted(present & expected):
        path = root / relative
        started = time.monotonic()
        report, command, rc, output = None, [], 2, ""
        if relative == SELF:
            rc = own_rc
            output = own_output or "Ran " + str(own_count) + " FullstackHermeticRunnerTest tests"
            command = own_command or command_for_shebang(path) + ["--runner-only"]
            report = own_report if own_report and own_report.get("run") == own_count else None
        else:
            try:
                interpreter = command_for_shebang(path)[:-1]
                with tempfile.TemporaryDirectory(prefix="s10-selftest-receipt-") as directory:
                    receipt = Path(directory) / "result.json"
                    command = interpreter + [str(Path(__file__).resolve()), "--selftest-worker", str(receipt), str(path)]
                    rc, output = bounded_command(command, cwd=root, timeout=20)
                    if receipt.exists():
                        raw = json.loads(receipt.read_text())
                        if raw['native_rc'] == rc and len(raw['reports']) == 1:
                            report = raw['reports'][0]
            except (ValueError, OSError, IndexError, KeyError, TypeError, AssertionError) as error:
                output += "\n" + str(error)  # Keep the native status if the command already ran.
        wall_s = own_wall_s if relative == SELF and own_wall_s is not None else time.monotonic() - started
        receipts.append({"path": relative, "command": command, "native_rc": rc,
                         "wall_s": wall_s,
                         "source_sha": hashlib.sha256(path.read_bytes()).hexdigest(),
                         "output": output, "tests": report,
                         "status": "VERIFIED" if rc == 0 and receipt_complete(relative, report) else "INCOMPLETE"})
        if not receipt_complete(relative, report):
            problems.append("NOT_RUN/INCOMPLETE required test IDs or outcomes: " + relative)
        if rc:
            problems.append("selftest failed: " + relative + " rc=" + str(rc))
    executed = [receipt["path"] for receipt in receipts]
    if len(executed) != len(set(executed)) or set(executed) != expected:
        problems.append("actual execution paths differ from the required selftest union")
    return receipts, problems
class RunnerSandbox:
    """A private runner copy with no deployment or product command available."""

    def __init__(self, root):
        self.root = root
        for relative in ("run-drills.sh", "lib/manifest.sh"):
            write(root / relative, (SIM / relative).read_text())
        (root / "drills/lib").mkdir(parents=True)
        write(root / "simcluster", """#!/bin/sh
case "$1" in
    check-image) exit 0 ;;
    drill)
        name=$2; arm=""
        [ "${3:-}" != --arm ] || arm=$4
        export ARM="$arm" SIM_DRILL_NAME="$name"
        printf '%s.%s\\n' "$name" "$arm" >> "$(dirname "$0")/calls"
        exec sh "$(dirname "$0")/drills/$name.sh" ;;
    *) exit 91 ;;
esac
""", True)
        # Fail closed if a future edit reaches any prohibited external command.
        for name in ("docker", "ssh", "sudo", "systemctl", "tether"):
            write(root / "bin" / name, "#!/bin/sh\necho prohibited-command >&2\nexit 99\n", True)
        # The runner's canary still opens, syncs, closes and unlinks its file,
        # but one sample suffices for local command-protocol tests. Actual
        # deployment calibration keeps its original 200/40 sample counts.
        write(root / "bin/python3", "#!/bin/sh\n"
              'if [ "${1:-}" = - ] && [ "$#" = 3 ]; then\n'
              '    case "$2" in ' + shlex.quote(str(root)) + '/logs*/.fsync-canary.*)\n'
              "        exec " + shlex.quote(sys.executable) + ' "$1" "$2" 1 ;;\n'
              "    esac\nfi\nexec " + shlex.quote(sys.executable) + ' "$@"\n', True)
        # Scheduler polling uses a fake command protocol, so hermetic tests
        # don't pay two seconds per tick. The unit ceiling still uses real
        # timeout(1), and the telemetry timer publishes a real join receipt.
        write(root / "bin/sleep", "#!" + sys.executable + "\n"
              "import os, signal, sys, time\n"
              "from pathlib import Path\n"
              "if sys.argv[1] == '60':\n"
              "    marker = Path(" + repr(str(root / "timers")) + ")\n"
              "    def stop(*_):\n"
              "        with marker.open('a') as out: out.write('joined ' + str(os.getpid()) + '\\n')\n"
              "        sys.exit(0)\n"
              "    signal.signal(signal.SIGTERM, stop)\n"
              "    with marker.open('a') as out: out.write('started ' + str(os.getpid()) + '\\n')\n"
              "    signal.pause()\n"
              "else: time.sleep(0.01 if sys.argv[1] == '2' else float(sys.argv[1]))\n", True)
        # Record the runner's signal protocol while delegating to Bash's real
        # kill builtin. This checks whole-group targeting without putting
        # Linux-only orphan adoption or /proc identity tests in run-all.
        write(root / "signals.sh", "kill() { printf '%s\\n' \"$*\" >> " +
              shlex.quote(str(root / "signals")) + '; builtin kill "$@"; }\n')
        self.env = {"PATH": str(root / "bin") + os.pathsep +
                    str(Path(sys.executable).parent) + os.pathsep + os.defpath,
                    "HOME": str(root / "home"), "LC_ALL": "C",
                    "BASH_ENV": str(root / "signals.sh")}
        (root / "home").mkdir()
        self.serial = 0

    def verdict(self, name, rc=0):
        verdict = ("GREEN", "ASSERT-FAIL", "SETUP-RED", "PRODUCT-RED", "INCOMPLETE")[rc]
        return ("printf 'DRILL-VERDICT verdict=" + verdict + " rc=" + str(rc) +
                " assert_fail=" + str(int(rc == 1)) + " setup_red=" + str(int(rc == 2)) +
                " product_red=" + str(int(rc == 3)) + " not_covered=" + str(int(rc == 4)) +
                " nc_gap=" + str(int(rc == 4)) + " nc_guard=0 pass=1 -- " + name + "\\n'\n" +
                'if [ -n "${ARM:-}" ]; then printf "DRILL-ARM arm=%s of=%s\\n" "$ARM" "$SIM_DRILL_NAME"; fi\n' +
                "exit " + str(rc) + "\n")

    def fullstack(self, body=None):
        arms = list(BOUNDS)
        text = "#!/bin/sh\n# arms: " + " ".join(arms) + "\n"
        for key, values in (("fixture", {arm: "N1" for arm in arms}),
                            ("grows", {arm: 0 for arm in arms}),
                            ("worst", {arm: BOUNDS[arm][0] for arm in arms}),
                            ("forgoes", {arm: "-" for arm in arms})):
            text += "# " + key + ": " + " ".join(arm + "=" + str(values[arm]) for arm in arms) + "\n"
        write(self.root / "drills" / (FULLSTACK + ".sh"), text + (body or self.verdict(FULLSTACK)))
        self.map = {"unit_ceilings": [{"unit": FULLSTACK + "." + arm, "worst_s": worst,
                                     "ceiling_s": cap, "minimum_s": minimum}
                                    for arm, (worst, cap, minimum) in BOUNDS.items()]}
        self.save_map()

    def save_map(self):
        write(self.root / "drills/lib/fullstack_manifest.json", json.dumps(self.map))

    def legacy(self, worst=2000):
        write(self.root / "drills/old.sh", "#!/bin/sh\n# arms: A\n# worst: A=" + str(worst) +
              "\n" + self.verdict("old"))
        write(self.root / "drills/plain.sh", "#!/bin/sh\n" + self.verdict("plain"))

    def run(self, *units, flags=(), timeout=15):
        self.serial += 1
        logdir = self.root / ("logs" + str(self.serial))
        rc, output = bounded_command(["bash", str(self.root / "run-drills.sh"), "--skip-preflight",
                                      "--no-lpt", "--no-retry", "--no-attribute", "--logdir",
                                      str(logdir), *flags, *units],
                                     cwd=self.root, env=self.env, timeout=timeout)
        return rc, output, logdir

# The class name is the exact P32 acceptance name. Every test invokes the real
# parser/runner copy or wrapper; generated fixtures cannot certify a deployment.
# origin: hot-update S10 plan §10.2 P32 / FullstackHermeticRunnerTest
class FullstackHermeticRunnerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="fullstack-hermetic-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.runner = RunnerSandbox(self.root)

    def assert_run(self, expected_rc, *units, flags=()):
        rc, output, logdir = self.runner.run(*units, flags=flags)
        self.assertEqual(rc, expected_rc, output)
        return output, logdir
    def test_exact_ceilings_apply_to_unfiltered_default_and_live_grow(self):
        self.runner.fullstack()
        self.runner.legacy()
        for flags in ((), ("--live-grow",)):
            with self.subTest(flags=flags):
                _, logs = self.assert_run(0, flags=flags)
                expected = {FULLSTACK + "." + arm: str(cap) for arm, (_, cap, _) in BOUNDS.items()}
                expected.update({"old.A": "2700", "plain": "2700"})
                actual = {path.stem: path.read_text().strip() for path in logs.glob("*.tmo")}
                self.assertEqual(actual, expected)
                self.assertIn("RUN-COMPLETE", (logs / "progress.tsv").read_text())
    def test_global_timeout_cannot_widen_legacy_units(self):
        self.runner.legacy()
        for flag in ("--drill-timeout", "--drill-timeout="):
            args = (flag, "12000") if flag.endswith("timeout") else (flag + "12000",)
            output, _ = self.assert_run(2, "old.A", flags=args)
            self.assertIn("legacy hard cap", output)
            self.assertFalse((self.root / "calls").exists())
        _, logs = self.assert_run(0, "old.A", flags=("--drill-timeout", "900"))
        self.assertEqual((logs / "old.A.tmo").read_text().strip(), "900")
        self.runner.legacy(worst=60)
        _, logs = self.assert_run(0, "old.A")
        self.assertEqual((logs / "old.A.tmo").read_text().strip(), "120")
    def test_ceiling_map_and_worst_header_fail_closed_before_launch(self):
        self.runner.fullstack()
        original = json.loads(json.dumps(self.runner.map))
        mutations = {
            "missing-map": lambda value: value.pop("unit_ceilings"),
            "missing-unit": lambda value: value["unit_ceilings"].pop(),
            "unknown-unit": lambda value: value["unit_ceilings"][0].update(unit="old.A"),
            "duplicate-unit": lambda value: value["unit_ceilings"].append(value["unit_ceilings"][0]),
            "wrong-cap": lambda value: value["unit_ceilings"][0].update(ceiling_s=12000),
            "wrong-worst": lambda value: value["unit_ceilings"][0].update(worst_s=2401),
            "minimum-too-small": lambda value: value["unit_ceilings"][0].update(minimum_s=2047),
            "minimum-above-cap": lambda value: value["unit_ceilings"][0].update(minimum_s=4801),
            "boolean-seconds": lambda value: value["unit_ceilings"][0].update(ceiling_s=True),
            "float-seconds": lambda value: value["unit_ceilings"][0].update(ceiling_s=4800.0),
            "extra-field": lambda value: value["unit_ceilings"][0].update(waiver=True),
        }
        for name, mutate in mutations.items():
            with self.subTest(mutation=name):
                self.runner.map = json.loads(json.dumps(original))
                mutate(self.runner.map)
                self.runner.save_map()
                if name in ("missing-map", "missing-unit", "wrong-cap"):
                    output, _ = self.assert_run(2, FULLSTACK + ".A")
                else:
                    # Row controls exercise the production reader directly;
                    # the integration cases above bind its failure to launch.
                    rc, output = bounded_command(["sh", "-c",
                        '. "$1"; manifest_fullstack_ceilings "$2" "$3"', "manifest-reader",
                        str(self.root / "lib/manifest.sh"),
                        str(self.root / "drills/lib/fullstack_manifest.json"),
                        str(self.root / "drills" / (FULLSTACK + ".sh"))], cwd=self.root)
                    self.assertEqual(rc, 2, output)
                self.assertIn("invalid fullstack ceilings", output)
                self.assertFalse((self.root / "calls").exists())
        path = self.root / "drills/lib/fullstack_manifest.json"
        valid_json = json.dumps(original)
        for malformed in ("", "[]", '{"unit_ceilings":[],"unit_ceilings":[]}',
                          '{"unit_ceilings":NaN}', "{broken",
                          valid_json[:-1] + ', "unit_ceilings":' +
                          json.dumps(original["unit_ceilings"]) + "}",
                          valid_json.replace('"worst_s": 2400',
                                             '"worst_s": 1, "worst_s": 2400', 1)):
            write(path, malformed)
            self.assert_run(2, FULLSTACK + ".A")
        path.unlink()
        self.assert_run(2, FULLSTACK + ".A")
        self.runner.map = original
        self.runner.save_map()
        drill = self.root / "drills" / (FULLSTACK + ".sh")
        saved = drill.read_text()
        for broken in (saved.replace("C1=2400", "C1=2401"),
                       saved.replace("# arms: C1", "# arms: C1 C1"),
                       saved + "# worst: A=6000\n"):
            write(drill, broken)
            self.assert_run(2, FULLSTACK + ".A")
        write(drill, saved)
        self.assert_run(0, FULLSTACK + ".A")
    def test_explicit_ceiling_cannot_exclude_the_transfer_minimum(self):
        self.runner.fullstack()
        output, _ = self.assert_run(2, FULLSTACK + ".A", flags=("--drill-timeout", "2700"))
        self.assertIn("actual transfer minimum", output)
        self.assertFalse((self.root / "calls").exists())
        _, logs = self.assert_run(0, FULLSTACK + ".C1", flags=("--drill-timeout", "2500"))
        self.assertEqual((logs / (FULLSTACK + ".C1.tmo")).read_text().strip(), "2500")
    def test_acceptance_rejects_retry_and_attribution_in_either_order(self):
        self.runner.fullstack()
        for flags in (("--retry",), ("--no-retry", "--retry")):
            output, _ = self.assert_run(2, FULLSTACK + ".A", flags=flags)
            self.assertIn("requires --no-retry --no-attribute", output)
        # The default attribution flag cannot silently run a diagnostic arm.
        runner = self.root / "run-drills.sh"
        command = ["bash", str(runner), "--skip-preflight", "--no-retry", "--logdir",
                   str(self.root / "unsafe"), FULLSTACK + ".A"]
        rc, output = bounded_command(command, cwd=self.root, env=self.runner.env)
        self.assertEqual(rc, 2, output)
        self.assertFalse((self.root / "calls").exists())
    def test_diagnostic_result_cannot_replace_first_native_red(self):
        body = "echo 'container not running'\n" + self.runner.verdict(FULLSTACK, 2)
        self.runner.fullstack(body)
        _, first = self.assert_run(1, FULLSTACK + ".A")
        before = {path.name: path.read_bytes() for path in first.iterdir() if path.is_file()}
        self.runner.fullstack()
        self.assert_run(0, FULLSTACK + ".A")
        self.assertEqual(before, {path.name: path.read_bytes() for path in first.iterdir() if path.is_file()})
        row = next(line.split("\t") for line in (first / "rollup.tsv").read_text().splitlines()
                   if line.startswith(FULLSTACK + ".A\t"))
        self.assertEqual((row[1], row[2], row[11]), ("SETUP-RED", "2", "1"))
        self.assertFalse(list(first.glob("*.attempt*")))
        self.assertEqual(len((self.root / "calls").read_text().splitlines()), 2)
        self.assertFalse(list(first.glob(".fsync-canary.*")))
        if (self.root / "timers").exists():
            events = [line.split() for line in (self.root / "timers").read_text().splitlines()]
            self.assertEqual({pid for event, pid in events if event == "started"},
                             {pid for event, pid in events if event == "joined"})
    def test_timeout_joins_protocol_child_and_preserves_infra_abort(self):
        # Five real local process lifecycles. The child acknowledges TERM and
        # is joined by its owner before timeout returns; no kernel identity
        # assertions or product resources belong in this portable selftest.
        units = []
        for round_number in range(5):
            with self.subTest(round=round_number):
                marker = self.root / ("joined" + str(round_number))
                child = self.root / "child.py"
                write(child, "import signal, time\n"
                      "signal.signal(signal.SIGTERM, lambda *_: exit(0))\n"
                      "print('ready', flush=True)\n"
                      "while True: time.sleep(1)\n")
                owner = self.root / "owner.py"
                write(owner, "import signal, subprocess, sys\n"
                      "child = subprocess.Popen([sys.executable, sys.argv[1]], stdout=subprocess.PIPE, text=True)\n"
                      "def stop(*_):\n"
                      "    child.wait(timeout=2)\n"
                      "    open(sys.argv[2], 'w').write(str(child.returncode))\n"
                      "    sys.exit(0)\n"
                      "signal.signal(signal.SIGTERM, stop)\n"
                      "assert child.stdout.readline().strip() == 'ready'\n"
                      "print('container not running', flush=True)\n"
                      "signal.pause()\n")
                unit = "wedge" + str(round_number)
                units.append(unit)
                write(self.root / "drills" / (unit + ".sh"), "#!/bin/sh\nexec " +
                      shlex.join([sys.executable, str(owner), str(child), str(marker)]) + "\n")
        _, logs = self.assert_run(5, *units, flags=("--drill-timeout", "1"))
        for round_number, unit in enumerate(units):
            with self.subTest(round=round_number):
                marker = self.root / ("joined" + str(round_number))
                self.assertEqual(marker.read_text(), "0")
                self.assertTrue((logs / (unit + ".timeout")).exists())
                self.assertEqual((logs / (unit + ".rc")).read_text().strip(), "124")
                self.assertIn(unit + "\tINFRA-ABORT\t124", (logs / "rollup.tsv").read_text())
                self.assertFalse(list(logs.glob("*.runpid")))
                self.assertFalse(list(logs.glob("*.attempt*")))
        self.assertTrue((self.root / "timers").exists())
        events = [line.split() for line in (self.root / "timers").read_text().splitlines()]
        started = {pid for event, pid in events if event == "started"}
        self.assertTrue(started, "telemetry timer was never exercised")
        self.assertEqual(started, {pid for event, pid in events if event == "joined"})
        groups = [line for line in (self.root / "signals").read_text().splitlines()
                  if re.fullmatch(r"-KILL -- -[1-9][0-9]*", line)]
        self.assertEqual(len(groups), 5, "timeout did not retire every supervised process group")
    def test_python_inventory_reconciles_actual_execution_both_ways(self):
        markers = self.root / "executed"
        for relative in PYTHON_SELFTESTS:
            classes = {}
            for identity in sorted(required_test_ids(relative)):
                cls, name = identity.rsplit('.', 1)
                classes.setdefault(cls, []).append(name)
            body = "#!/usr/bin/env python3\nimport unittest\nfrom pathlib import Path\n"
            for cls, names in classes.items():
                body += "class " + cls + "(unittest.TestCase):\n"
                body += ''.join("    def " + name + "(self): self.assertTrue(True)\n" for name in names)
            body += "with Path(" + repr(str(markers)) + ").open('a') as out: out.write(" + repr(relative + "\n") + ")\n"
            write(self.root / relative, body + "unittest.main()\n")
        own_command = [sys.executable, str(self.root / SELF), "--receipts", "protocol.json"]
        own_report = dict(ids=sorted(required_test_ids(SELF)), run=len(required_test_ids(SELF)), skip=0, fail=0, error=0)
        receipts, problems = reconcile_selftests(self.root, 0, own_report['run'], own_wall_s=0.25,
            own_command=own_command, own_report=own_report)
        self.assertEqual(problems, [])
        own = next(item for item in receipts if item["path"] == SELF)
        self.assertEqual(own["wall_s"], 0.25)
        self.assertEqual(own["command"], own_command)
        self.assertEqual({item["path"] for item in receipts}, set(PYTHON_SELFTESTS))
        self.assertTrue(markers.exists(), "Python selftest command was never executed")
        self.assertEqual(set(markers.read_text().splitlines()), set(PYTHON_SELFTESTS) - {SELF})
        self.assertTrue(all(item["native_rc"] == 0 for item in receipts))
        self.assertTrue(all(item["command"][-1] == str(self.root / item["path"])
                            for item in receipts if item["path"] != SELF))
        missing = self.root / "drills/lib/fullstack_tasks_test.py"
        missing.unlink()
        write(self.root / "drills/lib/fullstack_unregistered_test.py", "#!/usr/bin/env python3\nprint('extra')\n")
        _, problems = reconcile_selftests(self.root, 0, 10)
        self.assertTrue(any("NOT_RUN pending P13" in problem for problem in problems))
        self.assertTrue(any("unregistered Python selftest" in problem for problem in problems))
        self.assertTrue(any("actual execution paths" in problem for problem in problems))
    def test_python_failure_bad_shebang_and_zero_tests_stay_red(self):
        for relative in PYTHON_SELFTESTS:
            write(self.root / relative, "#!/usr/bin/env python3\nprint('executed')\n")
        bad = self.root / "drills/lib/fullstack_tasks_test.py"
        for body, expected in (("#!/bin/sh\nexit 0\n", 2),
                               ("#!/usr/bin/env python3\nprint('native failure'); exit(7)\n", 7),
                               ("#!/usr/bin/env python3\n", 0)):
            write(bad, body)
            receipts, problems = reconcile_selftests(self.root, 0, 10)
            receipt = next(item for item in receipts if item["path"] == bad.relative_to(self.root).as_posix())
            self.assertEqual(receipt["native_rc"], expected)
            self.assertEqual(receipt["status"], "INCOMPLETE")
            self.assertTrue(problems)
        _, problems = reconcile_selftests(self.root, 0, 0)
        self.assertTrue(any(SELF in problem for problem in problems))
    def test_shell_entry_honors_python_shebang(self):
        self.assertEqual((SIM / SELF).read_text().splitlines()[0],
                         "#!/usr/bin/env -S python3 -I -B")
        wrapper = self.root / "tests/hot-update-fullstack-test.sh"
        write(wrapper, (SIM / "tests/hot-update-fullstack-test.sh").read_text())
        target = wrapper.with_suffix(".py")
        write(target, "#!/usr/bin/env python3\nprint('python-entry-reached'); exit(7)\n", True)
        rc, output = bounded_command(["sh", str(wrapper)], cwd=self.root)
        self.assertEqual(rc, 7, output)
        self.assertIn("python-entry-reached", output)
        write(target, "#!/definitely/missing/python3\nprint('unreachable')\n", True)
        rc, output = bounded_command(["sh", str(wrapper)], cwd=self.root)
        self.assertNotEqual(rc, 0, output)
        self.assertNotIn("unreachable", output)
    def test_run_all_keeps_every_entry_and_propagates_fullstack_failure(self):
        # Run the real dispatcher on protocol fixtures, preserving the entire
        # existing timer suite's entry list without executing those timers.
        source = (SIM / "tests/run-all.sh").read_text()
        loop = re.search(r"(?m)^for t in (.+); do$", source)
        self.assertIsNotNone(loop)
        names = loop[1].split()
        self.assertEqual(len(names), len(set(names)))
        self.assertEqual(names.count("lint-install"), 1)
        # run-all remains owned by upstream S8 P32/S9 P37 until handoff. The
        # concrete S10 additions are exercised only in this temporary fixture;
        # main checks the real dispatcher and reports pending registration.
        additions = [name for name in ("hot-update-fullstack-test", "hot-update-fullstack-logs-test")
                     if name not in names]
        names += additions
        source = source[:loop.start(1)] + " ".join(names) + source[loop.end(1):]
        self.assertEqual(run_all_registration_problems(source), [])
        tests = self.root / "dispatch"
        write(tests / "run-all.sh", source)
        names += re.findall(r'"\$HERE/([A-Za-z0-9_.-]+)\.sh"', source)
        for name in names:
            shell = "#!/bin/bash\nset -o pipefail\n" if name == "hot-update-fullstack-test" else "#!/bin/sh\n"
            write(tests / (name + ".sh"), shell + "printf '%s\\n' " + shlex.quote(name) +
                  " >> " + shlex.quote(str(self.root / "dispatched")) + "\nexit 0\n")
        rc, output = bounded_command(["sh", str(tests / "run-all.sh")], cwd=self.root)
        self.assertEqual(rc, 0, output)
        self.assertEqual((self.root / "dispatched").read_text().splitlines(), names)
        write(tests / "hot-update-fullstack-test.sh", "#!/bin/bash\nset -o pipefail\nexit 7\n")
        rc, output = bounded_command(["sh", str(tests / "run-all.sh")], cwd=self.root)
        self.assertEqual(rc, 1, output)
        self.assertIn("FAIL", output)
    def test_run_all_registration_controls_reject_omissions_and_duplicates(self):
        entries = ["hot-update-fullstack-test", "hot-update-fullstack-logs-test", "lint-install"]
        source = "for t in " + " ".join(entries) + "; do\n"
        self.assertEqual(run_all_registration_problems(source), [])
        for name in entries:
            for changed in (source.replace(name, "", 1), source.replace(name, name + " " + name, 1)):
                self.assertEqual(run_all_registration_problems(changed),
                                 ["run-all must invoke exactly once: " + name])
class FullstackReceiptIntegrityTest(unittest.TestCase):
    # origin: S10 internal review rv-honesty F6
    def test_escaped_descendant_cannot_return_success(self):
        import ctypes
        libc = ctypes.CDLL(None)
        previous = ctypes.c_int()
        self.assertEqual(libc.prctl(37, ctypes.byref(previous), 0, 0, 0), 0)
        self.assertEqual(libc.prctl(36, 1, 0, 0, 0), 0)
        code = ("import subprocess,sys; p=subprocess.Popen([sys.executable,'-c',"
                "'import time; time.sleep(30)'],start_new_session=True,stdin=subprocess.DEVNULL,"
                "stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); print(p.pid,flush=True)")
        try:
            rc, out = bounded_command([sys.executable, '-c', code], cwd=SIM, timeout=5)
            pid = int(out.splitlines()[0])
            alive = Path('/proc/' + str(pid)).exists()
            if alive:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
            self.assertNotEqual(rc, 0, 'runner returned native_rc=0 with a live setsid descendant')
            self.assertEqual(rc, 125, 'emergency cleanup must preserve native125')
            self.assertFalse(alive, 'launcher returned before descendant was reaped')
        finally:
            libc.prctl(36, previous.value, 0, 0, 0)

    # origin: S10 internal review rv-honesty F7
    def test_fully_skipped_selftests_are_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for relative in PYTHON_SELFTESTS:
                write(root / relative, '#!/usr/bin/env python3\nimport unittest\n'
                      '@unittest.skip("not wired")\nclass Pending(unittest.TestCase):\n'
                      '    def test_product(self): self.fail("must actually run")\nunittest.main()\n')
            receipts, problems = reconcile_selftests(root, 0, 10)
            zero = [r for r in receipts if r['path'] != SELF and 'skipped=1' in r['output']]
            self.assertEqual(len(zero), len(PYTHON_SELFTESTS) - 1)
            self.assertTrue(problems, 'all eleven fully skipped unittest files accepted with native_rc=0 and problems=[]')

    # origin: S10 internal review rv-honesty F6
    def test_timeout_and_owner_sigkill_reap_escaped_descendants(self):
        for outcome in ('timeout', 'sigkill'):
            with self.subTest(outcome=outcome):
                code = ("import subprocess,sys,os,signal; p=subprocess.Popen([sys.executable,'-c',"
                        "'import time; time.sleep(30)'],start_new_session=True,stdout=subprocess.DEVNULL,"
                        "stderr=subprocess.DEVNULL); print(p.pid,flush=True); " +
                        ("signal.pause()" if outcome == 'timeout' else "os.kill(os.getpid(),signal.SIGKILL)"))
                rc, output = bounded_command([sys.executable, '-c', code], cwd=SIM, timeout=1)
                pid = int(output.splitlines()[0])
                self.assertNotEqual(rc, 0, output)
                self.assertFalse(Path('/proc/' + str(pid)).exists(), output)

    # origin: S10 internal review rv-honesty F7
    def test_empty_and_mixed_skip_programs_cannot_verify(self):
        relative = 'drills/lib/fullstack_tasks_test.py'
        for body in ('print("one line")', 'import unittest\nclass Mixed(unittest.TestCase):\n'
                     '    def test_pass(self): self.assertTrue(True)\n'
                     '    @unittest.skip("not wired")\n    def test_skip(self): self.fail()\nunittest.main()'):
            with self.subTest(body=body), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                write(root / relative, '#!/usr/bin/env python3\n' + body + '\n')
                receipts, problems = reconcile_selftests(root, 0, 10)
                self.assertEqual(receipts[0]['native_rc'], 0)
                self.assertEqual(receipts[0]['status'], 'INCOMPLETE')
                self.assertTrue(any('required test IDs or outcomes: ' + relative in p for p in problems))
                report = receipts[0]['tests']
                if report:
                    self.assertEqual((report['run'], report['skip'], report['fail'], report['error']), (2, 1, 0, 0))
    # origin: S10 internal review rv-honesty F6
    def test_python_owner_death_joins_inner_launcher_before_driver_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = str(Path(directory) / 'pids')
            target = ("import os,sys,signal; from pathlib import Path; "
                      "Path(sys.argv[1]).write_text(str(os.getpid())+' '+str(os.getppid())); "
                      "os.kill(int(sys.argv[2]),signal.SIGKILL); signal.pause()")
            owner = ("import runpy,sys,os; m=runpy.run_path(sys.argv[1],run_name='owner'); "
                     "m['bounded_command']([sys.executable,'-c',sys.argv[2],sys.argv[3],str(os.getpid())],cwd='.',timeout=5)")
            driver = ("import subprocess,sys,time; from pathlib import Path\n"
                      "p=subprocess.Popen([sys.executable,'-c',sys.argv[1],*sys.argv[2:]],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)\n"
                      "assert p.wait(timeout=5)==-9\n"
                      "ids=Path(sys.argv[-1]).read_text().split(); end=time.monotonic()+5\n"
                      "while any(Path('/proc/'+i).exists() for i in ids) and time.monotonic()<end: time.sleep(.01)\n"
                      "assert not any(Path('/proc/'+i).exists() for i in ids), 'inner owner left descendants'\n")
            rc, output = bounded_command([sys.executable, '-c', driver, owner, str(Path(__file__).resolve()), target, marker], cwd=SIM)
            self.assertEqual(rc, 0, output)
    # origin: S10 internal review rv-honesty F7
    def test_required_ids_and_each_outcome_count_are_independent(self):
        good = dict(ids=sorted(required_test_ids(SELF)), run=len(required_test_ids(SELF)), skip=0, fail=0, error=0)
        self.assertTrue(receipt_complete(SELF, good))
        for key, value in (('ids', good['ids'][:-1]), ('run', 0), ('skip', 1), ('fail', 1), ('error', 1)):
            with self.subTest(field=key):
                self.assertFalse(receipt_complete(SELF, dict(good, **{key: value})))

def main():
    if len(sys.argv) == 4 and sys.argv[1] == "--selftest-worker":
        return selftest_worker(Path(sys.argv[3]), sys.argv[2])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner-only", action="store_true")
    parser.add_argument("--receipts", type=Path)
    parser.add_argument("tests", nargs="*")
    args = parser.parse_args()
    names = args.tests or ["FullstackHermeticRunnerTest", "FullstackReceiptIntegrityTest"]
    if args.tests and not args.runner_only:
        parser.error("filtered tests require --runner-only; full reconciliation is unfiltered")
    suite = unittest.defaultTestLoader.loadTestsFromNames(names, module=sys.modules[__name__])
    raw = io.StringIO()
    started = time.monotonic()
    result = unittest.TextTestRunner(verbosity=2, stream=raw, resultclass=ReceiptResult).run(suite)
    own_wall_s = time.monotonic() - started
    sys.stderr.write(raw.getvalue())
    rc = 0 if result.wasSuccessful() and result.testsRun and not result.skipped else 1
    if args.runner_only:
        return rc
    own_command = [sys.executable, "-I", "-B", str(Path(__file__).resolve()), *sys.argv[1:]]
    receipts, problems = reconcile_selftests(SIM, rc, result.testsRun, raw.getvalue(), own_wall_s, own_command, result.receipt())
    registration = run_all_registration_problems((SIM / "tests/run-all.sh").read_text())
    problems += ["NOT_RUN pending S8 P32 / S9 P37 dispatcher handoff: " + problem
                 for problem in registration]
    for receipt in receipts:
        print("SELFTEST\t" + receipt["path"] + "\trc=" + str(receipt["native_rc"]))
    if args.receipts:
        with args.receipts.open("w", encoding="utf-8") as output:
            json.dump({"tier": "hermetic", "receipts": receipts, "problems": problems}, output, indent=2)
    for problem in problems:
        print(problem, file=sys.stderr)
    return rc or (2 if problems else 0)

if __name__ == "__main__":
    sys.exit(main())
