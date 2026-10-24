#!/usr/bin/env python3
"""Hermetic controls for the H0 probe; these never execute tether or Docker.

Synthetic oracle mutations plus real local producer/pipe behavior validate the
sensor, not the hot-update feature. The deploy drill owns real binary evidence.
"""

import errno
import json
import os
from pathlib import Path
import runpy
import signal
import subprocess
import sys
import tempfile
import threading
import time


PROBE = Path(__file__).resolve().parents[1] / "drills/lib/hot_update_process_probe.py"
LOGS = PROBE.with_name("logs.sh")
NONCE = "abcdef0123456789abcdef0123456789"
SENSOR = runpy.run_path(str(PROBE))


def wait_for(predicate, description, timeout=8):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.01)  # polling a real predicate, not assuming readiness
    raise AssertionError(f"timed out waiting for {description}")


def read_json(path):
    return json.loads(path.read_text())


def producer_arm(close_pipes):
    with tempfile.TemporaryDirectory(prefix="hot-update-producer-test-") as tmp:
        root = Path(tmp) / "work"
        child = subprocess.Popen(
            [sys.executable, "-u", str(PROBE), "producer", "--root", str(root),
             "--nonce", NONCE, "--period", "0.01", "--lifetime", "15"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        captured = {"stdout": [], "stderr": []}
        errors = []
        stop_reading = threading.Event()
        lock = threading.Lock()

        def drain(name, pipe):
            try:
                while not stop_reading.is_set():
                    line = pipe.readline()
                    if not line:
                        break
                    if not line.startswith(b"HUPROBE "):
                        raise AssertionError(f"unexpected producer output: {line!r}")
                    with lock:
                        captured[name].append(json.loads(line[len(b"HUPROBE "):]))
            except Exception as exc:
                errors.append(exc)
            finally:
                pipe.close()

        threads = [threading.Thread(target=drain, args=(name, pipe), daemon=True)
                   for name, pipe in (("stdout", child.stdout), ("stderr", child.stderr))]
        for thread in threads:
            thread.start()

        try:
            def ready():
                with lock:
                    return all(len(v) >= 10 for v in captured.values()) and (root / "identity.json").exists()
            wait_for(ready, "real producer output on both pipes")
            ident = read_json(root / "identity.json")
            assert ident["pid"] == child.pid and ident["start_ticks"] > 0 and ident["boot_id"]
            assert ident["nonce"] == NONCE
            if close_pipes:
                stop_reading.set()
                for thread in threads:
                    thread.join(timeout=2)
                    assert not thread.is_alive(), "local reader did not close its pipe"
                def both_broken():
                    recs = SENSOR["records"](root / "ledger.jsonl", NONCE, live=True)
                    return {r["stream"] for r in recs if r.get("kind") == "pipe_error"
                            and r.get("errno") == errno.EPIPE} == {"stdout", "stderr"}
                wait_for(both_broken, "EPIPE ledger on both original pipes")
                assert child.poll() is None, "producer must survive pipe loss for independent source evidence"

            subprocess.run([sys.executable, str(PROBE), "snapshot", "--root", str(root), "--nonce", NONCE],
                           check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            subprocess.run([sys.executable, str(PROBE), "stop", "--root", str(root), "--nonce", NONCE],
                           check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            assert child.wait(timeout=5) == 23
            for thread in threads:
                thread.join(timeout=2)
                assert not thread.is_alive(), "fixture drain thread leaked"
            assert not errors, errors
            final = read_json(root / "final.json")
            assert final["reason"] == "stop" and final["exit_code"] == 23
            source = [json.loads(line) for line in (root / "ledger.jsonl").read_text().splitlines()]
            for stream in captured:
                expected = [r for r in source if r.get("kind") == "data" and r.get("stream") == stream]
                assert len(expected) == final["count"] and len(expected) >= 10
                assert [r["seq"] for r in expected] == list(range(len(expected)))
                assert captured[stream] == expected[:len(captured[stream])]
                if close_pipes:
                    assert len(captured[stream]) < len(expected)
                else:
                    assert captured[stream] == expected
            assert any(r.get("kind") == "pipe_error" for r in source) is close_pipes
        finally:
            # This is fixture cleanup for this Popen child only.
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
            for thread in threads:
                thread.join(timeout=2)
                assert not thread.is_alive(), "fixture cleanup left a drain thread"


def cleanup_controls():
    # The alias exercises only the cleanup argv predicate with a child this
    # test owns; it never runs a tether binary or claims product behavior.
    with tempfile.TemporaryDirectory(prefix="hot-update-cleanup-test-") as tmp:
        root = Path(tmp)
        alias = root / "tether"
        alias.symlink_to(sys.executable)
        pidfile = root / "ctl.pid"
        for label, executable, verb, nonce, accepted in (
            ("original", alias, "exec", NONCE, True),
            ("wrong binary", Path(sys.executable), "exec", NONCE, False),
            ("wrong verb", alias, "run", NONCE, False),
            ("wrong nonce", alias, "exec", "0" * 32, False),
        ):
            child = subprocess.Popen([str(executable), "-c", "import time; time.sleep(15)", verb, nonce])
            try:
                pidfile.write_text(str(child.pid))
                result = subprocess.run([sys.executable, str(PROBE), "stop-ctl", "--pidfile", str(pidfile),
                                         "--nonce", NONCE], capture_output=True, timeout=5)
                assert (result.returncode == 0) is accepted, (label, result.stderr)
                if accepted:
                    assert child.wait(timeout=3) == -signal.SIGTERM
                else:
                    assert child.poll() is None, f"cleanup killed {label} child"
            finally:
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=3)
        # Drill 35 arm C1 runs the previous release as `tether-prev`: the
        # caller names it explicitly; the default name refuses it and an
        # explicit name refuses the plain `tether` alias.
        prev = root / "tether-prev"
        prev.symlink_to(sys.executable)
        for label, executable, ctl_name, accepted in (
            ("prev under its own name", prev, "tether-prev", True),
            ("prev under the default name", prev, None, False),
            ("tether under the prev name", alias, "tether-prev", False),
        ):
            child = subprocess.Popen([str(executable), "-c", "import time; time.sleep(15)", "exec", NONCE])
            try:
                pidfile.write_text(str(child.pid))
                cmd = [sys.executable, str(PROBE), "stop-ctl", "--pidfile", str(pidfile), "--nonce", NONCE]
                if ctl_name is not None:
                    cmd += ["--ctl-name", ctl_name]
                result = subprocess.run(cmd, capture_output=True, timeout=5)
                assert (result.returncode == 0) is accepted, (label, result.stderr)
                if accepted:
                    assert child.wait(timeout=3) == -signal.SIGTERM
                else:
                    assert child.poll() is None, f"cleanup killed {label} child"
            finally:
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=3)
        # Missing/not-yet-created pidfiles are an idempotent cleanup case.
        pidfile.unlink()
        subprocess.run([sys.executable, str(PROBE), "stop-ctl", "--pidfile", str(pidfile),
                        "--nonce", NONCE], check=True, timeout=5)
        # A real owned process that ignores TERM must still be reaped; poll a
        # ready marker so the test cannot send TERM before SIG_IGN is installed.
        ready = root / "ignore-term-ready"
        code = ("import pathlib,signal,sys,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                "pathlib.Path(sys.argv[1]).write_text('ready'); time.sleep(15)")
        child = subprocess.Popen([str(alias), "-c", code, str(ready), "exec", NONCE])
        try:
            wait_for(ready.exists, "TERM-ignoring cleanup fixture ready")
            pidfile.write_text(str(child.pid))
            subprocess.run([sys.executable, str(PROBE), "stop-ctl", "--pidfile", str(pidfile),
                            "--nonce", NONCE], check=True, timeout=6)
            assert child.wait(timeout=3) == -signal.SIGKILL
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=3)
        # Exercise cleanup-only producer termination, including the nonce
        # refusal, with independent owned local resources.
        producer_root = root / "producer"
        child = subprocess.Popen([sys.executable, str(PROBE), "producer", "--root", str(producer_root),
                                  "--nonce", NONCE, "--lifetime", "15"],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            wait_for((producer_root / "identity.json").exists, "cleanup producer identity")
            cmd = [sys.executable, str(PROBE), "stop", "--terminate", "--root", str(producer_root), "--nonce"]
            refused = subprocess.run(cmd + ["0" * 32], capture_output=True, timeout=5)
            assert refused.returncode != 0 and child.poll() is None
            assert not (producer_root / "stop").exists(), "wrong nonce changed producer state"
            subprocess.run(cmd + [NONCE], check=True, timeout=6)
            assert child.wait(timeout=3) in (23, -signal.SIGTERM)
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=3)


def log_mapping_controls():
    # Run the REAL helper command against a temporary filesystem mapping.
    # dexec only rewrites the two container-root prefixes; no Docker is called.
    with tempfile.TemporaryDirectory(prefix="hot-update-log-map-") as tmp:
        root = Path(tmp)
        envfile = root / "agent.env"
        home = root / "home/sim"
        for sid in ("lab", "custom-session"):
            sink = home / ".tether/agent" / sid
            sink.mkdir(parents=True)
            (sink / "agent.boot.err").write_text(f"{sid}-old\n{sid}-boot\n")
            (sink / "agent.log").write_text("WRONG-SLOG-STREAM\n")
        bridge = root / "bridge.py"
        bridge.write_text("""import os, subprocess, sys
args = sys.argv[1:]
assert args[:4] == ['agt1', '--', 'sh', '-c'], args
args[4] = args[4].replace('/etc/tether/agent.env', os.environ['TEST_AGENT_ENV']).replace('/home/sim', os.environ['TEST_AGENT_HOME'])
sys.exit(subprocess.run(args[2:]).returncode)
""")
        shell = '. "$TEST_LOGS"\ndexec() { python3 "$TEST_BRIDGE" "$@"; }\nsim_agent_panic_sink_dump agt1 "$1"\n'
        env = dict(os.environ, TEST_LOGS=str(LOGS), TEST_BRIDGE=str(bridge),
                   TEST_AGENT_ENV=str(envfile), TEST_AGENT_HOME=str(home))
        for sid in ("lab", "custom-session"):
            envfile.write_text(f"SID={sid}\n")
            result = subprocess.run(["sh", "-c", shell, "_", "1"], env=env, capture_output=True, text=True)
            assert result.returncode == 0 and result.stdout == f"{sid}-boot\n", result
        envfile.unlink()  # default SID without an env file
        result = subprocess.run(["sh", "-c", shell, "_", "1"], env=env, capture_output=True, text=True)
        assert result.returncode == 0 and result.stdout == "lab-boot\n", result
        (home / ".tether/agent/lab/agent.boot.err").unlink()
        result = subprocess.run(["sh", "-c", shell, "_", "1"], env=env, capture_output=True, text=True)
        assert result.returncode != 0 and not result.stdout, "missing boot sink reported success"
        result = subprocess.run(["sh", "-c", shell, "_", "x;exit 0"], env=env, capture_output=True, text=True)
        assert result.returncode == 2 and not result.stdout, "line count was not validated"


# origin: hot-update S3 plan §8.2. These are sensor controls with owned local
# children. They do not model the product's same-PID exec implementation.
def agent_cleanup_controls():
    fixture = Path(__file__).parent / "testdata/process-agent-cleanup.py"
    runpy.run_path(str(fixture))["agent_cleanup_controls"](SENSOR, NONCE, wait_for)


def descriptor_controls():
    before = SENSOR["fdcensus"](os.getpid())
    pipe = os.pipe()
    replay = os.memfd_create("tether-replay", os.MFD_CLOEXEC)
    handoff = os.memfd_create("tether-handoff", os.MFD_CLOEXEC)
    pidfd = os.pidfd_open(os.getpid())
    try:
        after = SENSOR["fdcensus"](os.getpid())
        for kind, delta in (("pipe", 2), ("replay", 1), ("handoff", 1), ("pidfd", 1)):
            assert after["counts"][kind] == before["counts"][kind] + delta, kind
        assert after["unsafe"] == before["unsafe"]
        os.set_inheritable(replay, True)
        unsafe = SENSOR["fdcensus"](os.getpid())["unsafe"]
        assert len(unsafe) == len(before["unsafe"]) + 1, "non-CLOEXEC replay was missed"
    finally:
        for fd in (*pipe, replay, handoff, pidfd):
            os.close(fd)
    assert SENSOR["fdcensus"](os.getpid())["counts"] == before["counts"], "sensor fixture leaked fds"


def shell_functions(path, names):
    import re
    source = Path(path).read_text()
    result = []
    for name in names:
        match = re.search(r"^" + re.escape(name) + r"\(\) \{[^\n]*\}\s*$", source, re.M)
        if match is None:
            match = re.search(r"^" + re.escape(name) + r"\(\) \{\n.*?^\}", source, re.M | re.S)
        assert match, f"predicate missing: {path}:{name}"
        result.append(match[0])
    return "\n".join(result) + "\n"


def drill_predicate_controls(arm):
    drills = PROBE.parent.parent
    path = drills / ("39-hot-update-legacy-n1.sh" if arm in ("X", "Y") else "34-hot-update-process.sh")
    with tempfile.TemporaryDirectory(prefix="hot-update-predicate-") as tmp:
        root = Path(tmp)
        env = dict(os.environ, H0_EVIDENCE=str(root), H0_NONCE=NONCE, PROBE=str(PROBE),
                   HOPS="1", DEFERRED="0", ARM=arm, H0_CURSOR="0", H0_BOOT_GENERATION="0", H0_OLD_SHA="old",
                   H0_NEW_SHA="new", H0_PID_BEFORE="4300", H0_START_BEFORE="start",
                   MARKER="marker", DST="dst", PREV="prev", H0_REMOTE_PROBE="probe", SID="lab",
                   NOHUP_PIDFILE=f"/tmp/hup-agent-{NONCE}.pid")
        def run(names, stubs, expected=True, label="", extra=None, invoke=None):
            script = "set -u\n" + shell_functions(path, names) + stubs + "\n" + (invoke or names[0]) + "\n"
            result = subprocess.run(["sh", "-c", script], env=env | (extra or {}), capture_output=True, text=True)
            assert (result.returncode == 0) == expected, (arm, label, result.returncode, result.stdout, result.stderr)

        if arm in ("A", "G", "U", "N", "W", "F"):
            SENSOR["synthetic_evidence"](root, NONCE)
            SENSOR["synthetic_hotupdate"](root, NONCE, 2 if arm in ("G", "W") else 1)
            # Use the real wrapper at the real deploy floor (20), with a
            # scaled synthetic producer instead of weakening --minimum.
            for directory in ("before", "post", "final"):
                for file in (root / directory).glob("*"):
                    if file.name in ("ledger.jsonl", "stdout", "stderr"):
                        recs = SENSOR["records"](file, NONCE, transcript=file.name in ("stdout", "stderr"))
                        expanded = []
                        for r in recs:
                            for i in range(4):
                                expanded.append(r | {"seq": r["seq"]*4+i, "at_ns": r["at_ns"]*4+i})
                        SENSOR["write_records"](file, expanded, file.name in ("stdout", "stderr"))
                for name, key in (("snapshot.json", "data_count"), ("snapshot-start.json", "data_count"), ("final.json", "count")):
                    file = root / directory / name
                    if file.exists():
                        doc = read_json(file); doc[key] *= 4; file.write_text(json.dumps(doc))
            doc = read_json(root / "hotupdate.json")
            if arm == "W":
                env["HOPS"] = "2"
                doc["hops"][0]["marker"]["state"] = "pending"
                doc["hops"][1].update(prev_consumed=True, disk_sha="a"*64)
                doc["hops"][1]["marker"].update(state="rolled_back", resource_phase="redispatched", detail="register deadline exceeded")
                lost = read_json(root / "before/ps.json"); lost["processes"][0]["status"] = "LOST"
                (root / "lost-ps.json").write_text(json.dumps(lost))
                (root / "final/ctl-outcome.json").write_text('{"finished":true,"exit_code":74}')
            elif arm == "G":
                env["HOPS"] = "2"
            elif arm == "F":
                env["DEFERRED"] = "1"; doc["audit"]["deferred_lines"] = 1
            (root / "hotupdate.json").write_text(json.dumps(doc))
            run(["_hotupdate"], "", label="positive")
            key, value = {"A": ("exec_lines", 2), "G": ("restored_lines", 1), "U": ("forbidden_lines", 1),
                          "N": ("reliable_lines", 0), "W": ("restored_lines", 1), "F": ("deferred_lines", 0)}[arm]
            doc["audit"][key] = value
            (root / "hotupdate.json").write_text(json.dumps(doc))
            run(["_hotupdate"], "", False, "audit mutation")
        if arm in ("A", "G", "U", "N", "F", "X", "Y"):
            stubs = '''
_snapshot() { printf '{"alive":true,"finished":false,"data_count":%s}\n' "$NV_SOURCE"; }
python3() { printf '{"stdout":%s,"stderr":%s}\n' "$NV_STDOUT" "$NV_STDERR"; }
_check() { _advanced_by 20 '{"stdout":20,"stderr":20}'; }
'''
            # _advanced_by needs arguments: append a wrapper AFTER extracting it.
            text = shell_functions(path, ["_advanced_by"])
            for key in (None, "NV_SOURCE", "NV_STDOUT", "NV_STDERR"):
                values = {"NV_SOURCE":"40", "NV_STDOUT":"40", "NV_STDERR":"40"}
                if key: values[key] = "39"
                result = subprocess.run(["sh", "-c", text+stubs+"\n_check"], env=env | values, capture_output=True)
                assert (result.returncode == 0) == (key is None), (arm, "progress", key)
        if arm == "A":
            lines = ("staged", "waiting for re-register (deadline 120s)", "upgrade COMMITTED")
            for missing in (None, *lines, "broker"):
                (root / "upgrade-1.stdout").write_text("\n".join(line for line in lines if line != missing))
                run(["_upgrade_transcript"], '\nsim_broker_slog_grep() { test "$NV_BROKER" = yes; }\n',
                    missing is None, str(missing), {"HOT_UPDATE_REGISTER_DEADLINE_S":"120", "NV_BROKER":"no" if missing == "broker" else "yes"},
                    invoke="_upgrade_transcript 1")
            final = read_json(root / "final/ps.json")
            for mutation in ("positive", "RUNNING", "wrong exit", "empty", "duplicate"):
                import copy
                doc = copy.deepcopy(final)
                if mutation == "RUNNING": doc["processes"][0]["status"] = "RUNNING"
                if mutation == "wrong exit": doc["processes"][0]["exit_code"] = -1
                if mutation == "empty": doc["processes"] = []
                if mutation == "duplicate": doc["processes"] *= 2
                (root / "terminal-input.json").write_text(json.dumps(doc))
                run(["_final_ps_exited"], '\nSIM=sim_stub\nsim_stub() { cat "$H0_EVIDENCE/terminal-input.json"; }\n', mutation == "positive", mutation)
        if arm == "N":
            for sha in ("wanted", "wrong"):
                run(["_nohup_original"], '\n_agent_pid_nohup() { echo 4300; }\n_running_sha() { echo "$NV_SHA"; }\nsha256sum() { echo wanted; }\n',
                    sha == "wanted", sha, {"NV_SHA":sha, "HERE":str(root)})
            stubs = '''
dexec() {
    case "$*" in
        *" cat "*) printf '%b' "$NV_PIDS" ;;
        *systemctl*) printf '%s\n' "$NV_MAIN" ;;
        *identity*) printf '{"argv":["%s","agent","--session","lab","--nid","agt1"]}\n' "$NV_EXE" ;;
        *) return 90 ;;
    esac
}
'''
            for key, value in ((None, None), ("NV_PIDS", ""), ("NV_PIDS", "4300\\n4301\\n"),
                               ("NV_MAIN", "4300"), ("NV_EXE", "unrelated")):
                values = {"NV_PIDS":"4300\\n", "NV_MAIN":"0", "NV_EXE":"dst"}
                if key: values[key] = value
                run(["_agent_pid_nohup"], stubs, key is None, str((key,value)), values)
        if arm == "W":
            stubs = '''
dexec() {
    case "$*" in
        *"cat marker"*) printf '%s\n' "$NV_MARKER" ;;
        *sha256sum*) printf '%s  dst\n' "$NV_DISK" ;;
        *"test ! -e prev"*) test "$NV_PREV" = gone ;;
        *) return 90 ;;
    esac
}
_agent_pid() { printf '%s\n' "$NV_PID"; }
_agent_start() { printf '%s\n' "$NV_START"; }
_running_sha() { printf '%s\n' "$NV_EXE"; }
'''
            # origin: hot-update S5 plan §7.1 / T48 / DRILL-RUN 34.W.
            # Keep exact +1/+2 and resource counts for both old and new boot
            # owners; missing marker never means a fresh S5 owner is zero.
            for boot in (0, 1):
                pending = {"state":"pending", "resource_phase":"active", "generation":boot+1, "imported_resources":1}
                for key, value in ((None, None), ("generation", boot), ("generation", boot+2), ("imported_resources", 0), ("imported_resources", 2)):
                    values = {"H0_BOOT_GENERATION":str(boot), "NV_MARKER":json.dumps(pending | ({key:value} if key else {})), "NV_EXE":"new"}
                    run(["_w_pending"], stubs+'\n_record_observation() { return 0; }\n', key is None, "pending "+str((boot,key,value)), values)
                rolled = {"state":"rolled_back", "detail":"register deadline exceeded", "resource_phase":"redispatched", "generation":boot+2}
                for key, value in ((None,None), ("generation",boot+1), ("generation",boot+3)):
                    values = {"H0_BOOT_GENERATION":str(boot), "NV_MARKER":json.dumps(rolled | ({key:value} if key else {})), "NV_DISK":"old", "NV_PREV":"gone", "NV_PID":"4300", "NV_START":"start", "NV_EXE":"old"}
                    run(["_w_rolled_back"], stubs, key is None, "rollback "+str((boot,key,value)), values)
            marker = {"state":"rolled_back", "detail":"register deadline exceeded", "resource_phase":"redispatched", "generation":2}
            mutations = [(None,None)] + [(k,v) for k,v in (("state","committed"),("detail","boot budget exhausted"),
                         ("resource_phase","active"),("generation",1))]
            for key,value in mutations:
                values={"NV_MARKER":json.dumps(marker | ({key:value} if key else {})), "NV_DISK":"old", "NV_PREV":"gone", "NV_PID":"4300", "NV_START":"start", "NV_EXE":"old"}
                run(["_w_rolled_back"], stubs, key is None, str(key), values)
            for key,value in (("NV_DISK","new"),("NV_PREV","present"),("NV_PID","4301"),("NV_START","restart"),("NV_EXE","new")):
                values={"NV_MARKER":json.dumps(marker), "NV_DISK":"old", "NV_PREV":"gone", "NV_PID":"4300", "NV_START":"start", "NV_EXE":"old"}
                values[key]=value
                run(["_w_rolled_back"], stubs, False, key, values)
        if arm in ("F", "X"):
            stubs = '''
dexec() {
    case "$*" in
        *sha256sum*) printf '%s  dst\n' "$NV_DISK" ;;
        *"test ! -e marker"*) test "$NV_MARKER" = absent ;;
        *) return 90 ;;
    esac
}
_agent_pid() { printf '%s\n' "$NV_PID"; }
_agent_start() { printf '%s\n' "$NV_START"; }
_running_sha() { printf '%s\n' "$NV_EXE"; }
_stream_advancing() { test "$NV_ADVANCE" = yes; }
'''
            for key,value in ((None,None),("NV_DISK","new"),("NV_MARKER","pending"),("NV_PID","4301"),("NV_EXE","new"),("NV_ADVANCE","no")):
                values={"NV_DISK":"old", "NV_MARKER":"absent", "NV_PID":"4300", "NV_START":"start", "NV_EXE":"old", "NV_ADVANCE":"yes"}
                if key: values[key]=value
                run(["_deferred_left_disk_untouched" if arm == "F" else "_unchanged"], stubs, key is None, str(key), values)
        if arm == "F":
            provisioning_controls(path)
            for reason in ("remote_fs_unsafe_cwd", "remote_fs_spawn_timeout", "", "ok"):
                (root / "wedge.out").write_text(reason)
                run(["_probe_wedged"], "", reason == "remote_fs_unsafe_cwd", reason)
            for wire, reason, log, census in ((True,True,True,True),(False,True,True,True),(True,False,True,True),(True,True,False,True),(True,True,True,False)):
                (root / "deferred.out").write_text(("agent_rejected:upgrade_deferred " if wire else "") + ("remote_fs_probe_in_flight" if reason else "pty_busy") + (" (spawnsafe: probes in flight=1, retained=1)" if census else " (spawnsafe: probes in flight=0, retained=0)"))
                run(["_deferred_reason"], "\nsim_agent_slog_grep() { test \"$NV_LOG\" = yes; }\n", wire and reason and log and census,
                    str((wire,reason,log,census)), {"NV_LOG":"yes" if log else "no"})
            for rc in (124, 0, 1, 127):
                run(["_statfs_blocks"], '\nSIM=sim_stub\nsim_stub() { return "$NV_RC"; }\n', rc == 124, str(rc), {"NV_RC":str(rc), "HOT_UPDATE_SAFE_POINT_S":"5", "MNT":"/mnt/hung"})
        if arm == "P":
            mixed_wiring_controls(path)
            for event in ("cut", "activated"):
                for log, mark in ((True,True),(False,True),(True,False)):
                    run(["_mixed_mark_seen"], '\nsim_agent_slog_grep() { test "$2" = "agent: image $NV_EVENT" && test "$3" = 0 && test "$NV_LOG" = yes; }\ndexec() { case "$*" in *" mark --evidence /tmp/mixed-$H0_NONCE-p1 --name $NV_EVENT") test "$NV_MARK" = yes;; *) return 1;; esac; }\n',
                        log and mark, (event,log,mark), {"MIXED_REMOTE":"remote", "NV_EVENT":event, "NV_LOG":"yes" if log else "no", "NV_MARK":"yes" if mark else "no"},
                        invoke="_mixed_mark_seen " + event)
            run(["_mixed_started"], "\ndexec() { return 1; }\n", False, "no start receipt")
            run(["_mixed_started"], "\ndexec() { return 0; }\n", True, "start receipt")
            for seen in ("", "cut", "cut activated", "activated"):
                stubs = '\nP1_CUT_SEEN=0\n_mixed_mark_seen() { case " $NV_SEEN " in *" $1 "*) return 0;; *) return 1;; esac; }\n'
                run(["_mixed_switch_observed"], stubs, seen == "cut activated", seen, {"NV_SEEN":seen})
            for readable, parser in ((True,True),(False,True),(True,False)):
                run(["_mixed_paths"], '\nsim_agent_slog_since() { test "$2" = 0 && test "$NV_READ" = yes; }\npython3() { test "$NV_PARSE" = yes && printf "{}\\n"; }\n',
                    readable and parser, (readable,parser), {"P1_NONCE":NONCE,"NV_READ":"yes" if readable else "no", "NV_PARSE":"yes" if parser else "no"},
                    invoke='mkdir -p "$H0_EVIDENCE/p1"; _mixed_paths')
            stubs = '\nMIXED=mixed\npython3() { cat "$NV_JSON"; }\n'
            # The real scheduler is owned by MIXEDPROBE; these test the shell
            # integration never accepts a starved/misclassified JSON receipt.
            text = shell_functions(path, ["_mixed_judge"])
            for mode in ("calibrate", "upgrade"):
                good = dict(request_failures=0,connect_failures=0,rate=10,dup_side_effects=0,
                            rc0_without_effect=0,stale_offline_failures=0,in_window=1,configured_rate=10,
                            successful=dict(read=1,write=1,short=1), writes_in_window=1,
                            effect_without_rc0=0,unrequested_effects=0,agent_upgrading=0,starts_in_window=0)
                keys = ("request_failures","connect_failures","rate") if mode=="calibrate" else (
                    "dup_side_effects","rc0_without_effect","request_failures","connect_failures","stale_offline_failures","in_window",
                    "rate","effect_without_rc0","unrequested_effects","writes_in_window","successful.read","successful.write","successful.short","agent_upgrading")
                for key in (None,*keys):
                    import copy
                    doc=copy.deepcopy(good)
                    if key and key.startswith("successful."):
                        doc["successful"][key.split(".")[1]] = 0
                    elif key:
                        doc[key] = 8 if key=="rate" else 0 if key in ("in_window","writes_in_window") else 1
                    (root / "input.json").write_text(json.dumps(doc)); (root / "p").mkdir(exist_ok=True)
                    result=subprocess.run(["sh","-c",text+stubs+f"\n_mixed_judge p {mode}"], env=env | {"NV_JSON":str(root/"input.json")}, capture_output=True)
                    assert (result.returncode==0)==(key is None),(mode,key,result.stderr)
        if arm == "Y":
            for mutation in ("positive", "wrong SHA", "wrong tag", "offline"):
                nodes = {"nodes":[{"nid":"agt1", "status":"OFFLINE" if mutation=="offline" else "ONLINE",
                                   "release_version":"wrong" if mutation=="wrong tag" else "v0.6.0"}]}
                (root / "nodes.json").write_text(json.dumps(nodes))
                stubs = '\nSIM=sim_stub\nsim_stub() { cat "$H0_EVIDENCE/nodes.json"; }\n_agent_pid() { echo 4300; }\n_running_sha() { echo "$NV_SHA"; }\nsha256sum() { echo wanted; }\n'
                run(["_prev_online"], stubs, mutation == "positive", mutation,
                    {"NV_SHA":"wrong" if mutation=="wrong SHA" else "wanted", "PREV_TAG":"v0.6.0", "HERE":str(root)})
        if arm in ("X", "Y"):
            (root / "vendor").mkdir(exist_ok=True)
            (root / "vendor/tether-prev").write_text("previous")
            (root / "vendor/tether").write_text("current")
            import hashlib
            digest=hashlib.sha256(b"previous").hexdigest()
            stubs='''
sim_prev_source_ok() { test "$NV_SOURCE" = yes; }
d() { return 0; }
ctr_name() { printf '%s\n' "$1"; }
dexec() {
    case "$*" in
        *sha256sum*) printf '%s  prev\n' "$NV_SHA" ;;
        *version*) printf '%b' "$NV_VERSION" ;;
        *chmod*) return 0 ;;
        *) return 90 ;;
    esac
}
'''
            for key,value in ((None,None),("NV_SOURCE","no"),("NV_SHA","wrong"),("NV_VERSION","tether wrong\\na\\nb\\n"),
                              ("NV_VERSION","tether v0.6.0\\na\\nb\\nimage-abi v1\\n")):
                values={"HERE":str(root),"PREV_TAG":"v0.6.0","PREV_BIN":"prev","NV_SOURCE":"yes","NV_SHA":digest,"NV_VERSION":"tether v0.6.0\\na\\nb\\n"}
                if key: values[key]=value
                run(["_prev_die_gate"],stubs,key is None,str(key),values)
    print(f"drill {arm}: real shell predicates rejected every injected bad receipt")


def artifact_controls():
    import hashlib
    import tarfile
    with tempfile.TemporaryDirectory(prefix="hot-update-artifact-") as tmp:
        root=Path(tmp); (root/"backend").mkdir(); (root/"binary").write_bytes(b"owned fixture bytes")
        shell='''
set -eu
. "$TEST_ARTIFACT"
ART_BASE_URL=https://fixture-artifact
ctr_name() { printf backend; }
d() { test "$1" = cp; cp "$2" "$TEST_ROOT/backend/${3##*/}"; }
dexec() { test "$3" = sha256sum; sha256sum "$TEST_ROOT/backend/${4##*/}"; }
artifact_add "$TEST_ROOT/binary" second.tar.gz
printf '%s %s\n' "$ART2_URL" "$ART2_SHA"
'''
        env=dict(os.environ,TEST_ROOT=str(root),TEST_ARTIFACT=str(PROBE.with_name("artifact.sh")))
        result=subprocess.run(["sh","-c",shell],env=env,capture_output=True,text=True)
        assert result.returncode==0,result.stderr
        tar=root/"backend/second.tar.gz"
        assert result.stdout.strip()==f"https://fixture-artifact/second.tar.gz {hashlib.sha256(tar.read_bytes()).hexdigest()}"
        with tarfile.open(tar) as archive:
            assert archive.getnames()==["tether"] and archive.extractfile("tether").read()==b"owned fixture bytes"
        for mutation in (shell.replace('sha256sum "$TEST_ROOT/backend/${4##*/}"','printf wrong'),
                         shell.replace('second.tar.gz','../escape.tar.gz'),
                         shell.replace('cp "$2" "$TEST_ROOT/backend/${3##*/}"','return 1')):
            result=subprocess.run(["sh","-c",mutation],env=env,capture_output=True)
            assert result.returncode!=0,"artifact_add accepted corrupt/missing/unsafe supply"


def mixed_wiring_controls(path):
    # origin: hot-update S3 plan §8.1; DR-5. These are the deploy caller's
    # obligations; the hermetic scheduler cannot prove its caller awaits it.
    source = path.read_text().split("\nP)\n", 1)[1].split("\nW)\n", 1)[0]
    assert "--until-mark activated --tail 10 --max-duration 180" in source
    assert "--mark upgrade@" not in source and "sleep 10" not in source
    assert source.index('-- _mixed_started') < source.index('_upgrade 1 &') < source.index('-- _mixed_switch_observed')
    assert '_mixed_judge p1 upgrade cut activated' in source
    assert 'H0_UPGRADE_RC=$(cat "$H0_EVIDENCE/upgrade-1.rc")' in source
    assert '_mixed_paths' in source


def provisioning_controls(path):
    # origin: hot-update S3 plan §8.1; DR-2. Execute the actual setup slice
    # with inert supply stubs: local-only boot and restart-after-work are red.
    source = path.read_text()
    start = source.index("# Machine supply")
    end = source.index('log "calibration budgets:', start)
    setup = source[start:end]
    with tempfile.TemporaryDirectory(prefix="hot-update-order-") as tmp:
        root = Path(tmp)
        (root / "vendor").mkdir()
        (root / "vendor/tether-next").write_text("#!/bin/sh\necho tether next\n")
        (root / "vendor/tether-next").chmod(0o700)
        stubs = r'''
ARM=F; INSTANCE=test; SID=lab; PIN=123; SIM=simstub
AGENT_UNIT=tether-agent; NOHUP_PIDFILE=; H0_EVIDENCE="$HERE"; PROBE="$TEST_PROBE"
dexec() { test "$1 $2 $3 $4" = 'agt1 -- /proc/4300/exe version'; printf 'tether fixture (proto v1)\nimage-domains exec-v1 pty-v1 flow-v1 ss-v1\nimage-abi v1\n'; }
simstub() { :; }
assert_setup() { shift; "$@"; }
setup_fail() { exit 91; }
artifact_up() { :; }
upgradecfg_allow_broker() { :; }
ingress_trust_inject() { :; }
_mount_hangfs() { echo mount; }
_statfs_healthy() { echo healthy; }
upgradecfg_allow_agent() { echo restart; }
_install_probe() { :; }
poll_until() { :; }
_agent_pid() { echo 4300; }
_agent_start() { echo start; }
_running_sha() { echo old; }
sim_agent_slog_cursor() { echo 0; }
_launch_producer() { echo producer; }
_stream_advancing() { :; }
_record_observation() { :; }
'''
        result = subprocess.run(["sh", "-c", stubs + setup], env=os.environ | {"HERE":str(root), "TEST_PROBE":str(PROBE)}, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        assert result.stdout.splitlines() == ["mount", "healthy", "restart", "producer"], result.stdout


def activation_record_controls():
    # origin: hot-update S3 plan §3.2 and §8.2; DR-1. These records are also
    # asserted by the real Go logger, so parser and producer cannot drift apart.
    golden = Path(__file__).with_name("testdata") / "agent-activation-records.txt"
    with tempfile.TemporaryDirectory(prefix="activation-record-") as tmp:
        log = Path(tmp) / "agent.log"
        for line in golden.read_text().splitlines():
            # shim_ms/import_ms are Duration.Milliseconds() truncations: 0 is a real sub-millisecond
            # sample (the first real 34.G run imported generation 2 in <1 ms), so only the three
            # fields that are never zero on a real switch are zero-defects.
            for defect in (None, "missing", "malformed", "duplicate field", "duplicate record", "bare", *(
                    "zero " + key for key in ("generation", "tasks", "freeze_window_ms"))):
                value = line
                if defect == "missing": value = value.replace(" import_ms=4", "")
                elif defect == "malformed": value = value.replace("import_ms=4", "import_ms=4oops")
                elif defect == "duplicate field": value = value.replace("import_ms=4", "import_ms=4 import_ms=5")
                elif defect == "duplicate record": value += "\n" + value
                elif defect == "bare": value = SENSOR["slog_record"](line)["msg"]
                elif defect and defect.startswith("zero "):
                    import re
                    value = re.sub(defect[5:] + r"=\d+", defect[5:] + "=0", value)
                log.write_text(value + "\n")
                result = subprocess.run([sys.executable, "-B", str(PROBE), "freeze-window", "--log", str(log)], capture_output=True, text=True)
                assert (result.returncode == 0) == (defect is None), (line, defect, result.stdout, result.stderr)
                if defect is None:
                    assert json.loads(result.stdout) == [dict(generation=1,tasks=1,freeze_window_ms=12,shim_ms=2,import_ms=4)]
            for key in ("shim_ms", "import_ms"):
                import re
                log.write_text(re.sub(key + r"=\d+", key + "=0", line) + "\n")
                result = subprocess.run([sys.executable, "-B", str(PROBE), "freeze-window", "--log", str(log)], capture_output=True, text=True)
                assert result.returncode == 0 and json.loads(result.stdout)[0][key] == 0, (line, "sub-millisecond " + key, result.stdout, result.stderr)
        for json_out in (False, True):
            rows = [dict(msg="agent: exec", argv=[NONCE], reliable=True),
                    dict(msg="agent: exec", argv=[NONCE], reliable=False),
                    dict(msg="agent: exec", argv=[NONCE]),
                    dict(msg="agent: exec", argv=["foreign"], reliable=True),
                    dict(msg=SENSOR["CUT"], generation=1), dict(msg=SENSOR["DEFERRED"]),
                    dict(msg="agent: image abandoned")]
            if json_out:
                text = "\n".join(json.dumps(r) for r in rows)
            else:
                text = "\n".join(" ".join(k + "=" + json.dumps(v if not isinstance(v, list) else str(v)) for k,v in r.items()) for r in rows)
            text += "\n" + golden.read_text().splitlines()[int(json_out)]
            audit = SENSOR["log_audit"](text, NONCE)
            assert audit == dict(exec_lines=3,reliable_lines=1,legacy_lines=2,cut_lines=1,restored_lines=1,deferred_lines=1,forbidden_lines=1), audit
        for line in ('msg="agent: exec" msg="agent: exec"', '{"msg":"agent: exec","msg":"agent: exec"}'):
            try:
                SENSOR["slog_record"](line)
            except SENSOR["EvidenceError"]:
                continue
            raise AssertionError("duplicate record field accepted")


def source_literal_controls():
    # Fail closed in a partial S3 worktree too. The CUT/BOOT package must emit
    # these phrases before the merged tree can earn a green hermetic receipt.
    repo = PROBE.parents[4]
    pins = ((repo/"internal/workload/replay_linux.go", 'MemfdCreate("tether-replay"'),
            (repo/"internal/handoff/image_linux.go", 'MemfdCreate("tether-handoff"'))
    for path, literal in pins:
        assert literal in path.read_text(), f"source literal changed: {path}: {literal}"
        assert literal not in path.read_text().replace(literal, 'MemfdCreate("renamed"'), "rename control escaped"
    # origin: S8 internal review s8-hermetic F1. DeferredText moved with
    # image policy; scanning both owners keeps source/log drift visible.
    roots = (repo/"internal/agent", repo/"internal/agentupgrade")
    source = "\n".join(p.read_text() for root in roots for p in root.glob("*.go") if not p.name.endswith("_test.go"))
    # A Go string literal that STARTS with the phrase: the activation line carries its fields inside
    # the message (fmt.Sprintf) so they stay `key=value` under the JSON log format too, which the
    # judge's decoded activation fields depend on; the cut/deferred lines use slog attributes.
    missing = [SENSOR[k] for k in ("ACTIVATED","CUT","DEFERRED") if '"'+SENSOR[k] not in source]
    assert not missing, f"S3 source log literals missing: {missing}"
    for key in ("ACTIVATED","CUT","DEFERRED"):
        literal='"'+SENSOR[key]
        assert literal not in source.replace(literal, '"renamed"'), "rename control did not remove the pin"


# origin: hot-update S5 plan §9.5 / DRILL-RUN 35.A run257.
def carrier_recovery_controls(path=None):
    path = Path(path) if path is not None else PROBE.parent.parent / '35-hot-update-exec-carrier.sh'
    source = path.read_text()
    for label, budget in (('A1',60),('A2',90),('B1',90),('C1.1',90)):
        assert f'assert_setup "{label} recovery: agt1 ONLINE" poll_until {budget} 2 ' in source, 'recovery deadline changed'
    assert 'assert_ok "A1 original child and its stream still advancing after the nats-server restart" _stream_advancing' in source
    import re
    functions = []
    for name in ('_online', '_agt1_online', '_fault_begin'):
        match = re.search(r'^' + re.escape(name) + r'\(\)[ \t]*\{[^\n]*\}\s*$', source, re.M)
        if match is None:
            match = re.search(r'^' + re.escape(name) + r'\(\)[ \t]*\{\n.*?^\}', source, re.M | re.S)
        assert match, f'carrier predicate missing: {path}:{name}'
        functions.append(match[0])
    code = '\n'.join(functions) + '\n'
    with tempfile.TemporaryDirectory(prefix='carrier-registration-control-') as directory:
        root = Path(directory)
        old = 'time=2026-10-11T21:23:25.491Z level=INFO msg="agent: registered"\n'
        cases = [
            ('stale ONLINE', '', True, None, 1),
            ('fresh reconnect', 'time=2026-10-11T21:23:36.884Z level=INFO msg="agent: re-registered after reconnect" reconciled=0\n', True, None, 0),
            ('fresh session', 'time=2026-10-11T21:24:04.226Z level=INFO msg="agent: registered" reconciled=0\n', True, None, 0),
            ('fresh JSON', '{"time":"2026-10-11T21:24:04.226Z","msg":"agent: registered","reconciled":0}\n', True, None, 0),
            ('offline with fresh registration', 'msg="agent: registered"\n', False, None, 1),
            ('lookalike message', 'msg="agent: registered later diagnostic"\n', True, None, 1),
            ('different message field', 'msg="unrelated" detail="agent: registered"\n', True, None, 1),
            ('missing cursor', 'msg="agent: registered"\n', True, '', 2),
            ('zero cursor', 'msg="agent: registered"\n', True, '0', 2),
            ('malformed cursor', 'msg="agent: registered"\n', True, 'bad', 2),
        ]
        cases = [(*row, 'nats-restart') for row in cases] + [
            ('broker-only existing NATS session', '', True, None, 0, 'broker-restart'),
            ('broker-only offline', 'msg="agent: registered"\n', False, None, 1, 'broker-restart'),
            ('unknown fault fails closed', 'msg="agent: registered"\n', True, None, 1, 'unknown'),
        ]
        stubs = r'''
_ls_json() { cat "$FIXTURE/nodes.json"; }
_snapshot_count() { printf '20\n'; }
python3() { printf '{"stdout":20,"stderr":20}\n'; }
_now_ns() { printf '1791753810000000000\n'; }
log() { :; }
setup_fail() { exit 2; }
sim_agent_slog_cursor() {
    [ "$1" = agt1 ] || return 1
    if [ "$CURSOR_OVERRIDE" = native ]; then wc -c < "$FIXTURE/agent.log"; else printf '%s\n' "$CURSOR_OVERRIDE"; fi
}
sim_agent_slog_grep() {
    [ "$1" = agt1 ] || return 1
    tail -c +$(( $3 + 1 )) "$FIXTURE/agent.log" | grep -qE "$2"
}
_fault_begin "$FAULT_KIND"
printf '%s' "$ADDITION" >> "$FIXTURE/agent.log"
_agt1_online
'''
        for label, addition, online, cursor, expected, fault_kind in cases:
            (root/'agent.log').write_text(old)
            (root/'nodes.json').write_text(json.dumps(dict(nodes=[dict(nid='agt1',status='ONLINE' if online else 'OFFLINE')])))
            result = subprocess.run(['sh','-c','set -u\n'+code+stubs],
                env=dict(os.environ,FIXTURE=str(root),L_EV=str(root),L_NONCE='carrier-control',PROBE='unused',
                    FAULT_KIND=fault_kind,CURSOR_OVERRIDE='native' if cursor is None else cursor,ADDITION=addition),
                capture_output=True,text=True)
            assert result.returncode == expected, (label,result.returncode,expected,result.stdout,result.stderr)
            if cursor is None:
                assert (root/(fault_kind+'.registration.cursor')).read_text().strip() == str(len(old.encode())), 'fault cursor was not retained'
    print('Carrier recovery controls passed: fresh native registration, exact source, fail-closed cursor, unchanged deadlines/progress')

def main():
    if sys.argv[1:] == ["--carrier-recovery-only"]:
        carrier_recovery_controls()
        return
    if len(sys.argv) == 3 and sys.argv[1] == "--arm":
        drill_predicate_controls(sys.argv[2])
        return
    subprocess.run([sys.executable, str(PROBE), "selftest"], check=True)
    producer_arm(False)
    producer_arm(True)
    cleanup_controls()
    log_mapping_controls()
    agent_cleanup_controls()
    descriptor_controls()
    artifact_controls()
    for arm in ("A", "G", "U", "N", "P", "W", "F", "X", "Y"):
        drill_predicate_controls(arm)
    carrier_recovery_controls()
    source_literal_controls()
    activation_record_controls()
    print("H0 probe controls: synthetic oracles, healthy/closed real pipes, pidfd cleanup and log mapping passed; no tether/Docker run")


if __name__ == "__main__":
    main()
