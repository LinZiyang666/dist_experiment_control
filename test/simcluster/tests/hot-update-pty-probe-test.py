#!/usr/bin/env python3
"""Hermetic oracle controls and a real two-PTY transport fixture.

Image/marker/log receipts are synthetic judge controls, not deploy receipts.
The private Python proxy never resolves installed tether.
"""
import copy
import datetime
import fcntl
import json
import os
from pathlib import Path
import runpy
import re
import select
import signal
import subprocess
import sys
import tempfile
import termios
import time
import tty
import unittest

PROBE = Path(__file__).resolve().parents[1] / "drills/lib/hot_update_pty_probe.py"
NONCE = "abcdef0123456789abcdef0123456789"
SENSOR = runpy.run_path(str(PROBE))


def process(pid, parent=4000):
    return dict(pid=pid, ppid=parent, boot_id="fixture-boot", start_ticks=pid*10)


def byte_blob(root, name, ids=range(50), times=None):
    chunks = [SENSOR["frame"](NONCE, seq, SENSOR["payload"](seq)) for seq in ids]
    raw, offset, rows = b"".join(chunks), 0, []
    for i, (seq, chunk) in enumerate(zip(ids, chunks)):
        rows.append(dict(id=seq, offset=offset, length=len(chunk),
                         sha256=SENSOR["sha"](chunk), at=times[i] if times else i*.1))
        offset += len(chunk)
    (root / name).write_bytes(raw)
    return dict(file=name, length=len(raw), sha256=SENSOR["sha"](raw), records=rows)


def complete_fixture(root):
    """All fields required by §12.1, independently backed by byte files."""
    root.mkdir(parents=True, exist_ok=True)
    inp = dict(sent=byte_blob(root, "ctl.sent"), received=byte_blob(root, "remote.received"))
    out = dict(sent=byte_blob(root, "remote.sent"), received=byte_blob(root, "ctl.received"))
    inp.update(applied=inp["sent"]["length"], accepted=inp["sent"]["length"],
               checkpoints=[dict(at=1, applied=100, accepted=200), dict(at=4, applied=500, accepted=600)])
    out.update(base=out["sent"]["length"], end=out["sent"]["length"],
               checkpoints=[dict(at=1, base=100, end=200), dict(at=4, base=500, end=600)])
    samples = []
    for i in range(10):
        for role, pid in (("leader", 4100), ("foreground", 4200), ("ctl", 4300)):
            samples.append(dict(role=role, identity=process(pid), at=i*.5, session_id=4100 if role != "ctl" else 4300,
                                foreground_pgid=4200 if role != "ctl" else 4300,
                                ctty=True, alive=True, hup=0, early_eof=False, nonce=NONCE))
    image = dict(identity=process(4000), sha256="a"*64, generation=0, phase="committed",
                 spawn_count=1, terminal_count=0, exit_code=0)
    master = dict(dev=1, ino=2, rdev=3, mount_id=4, pty_index=5)
    before = dict(owner=process(4000), counts=dict(master=1, replay=2, handoff=0, pidfd=1),
                  masters=[master], wrongfd=[], zombies=[], foreground_alive=True)
    settled = dict(owner=process(4000), counts=dict(master=0, replay=0, handoff=0, pidfd=0),
                   masters=[], wrongfd=[], zombies=[], foreground_alive=False)
    cursors = []
    for i, stream in enumerate(SENSOR["STREAMS"]):
        content = ("\n".join(f"{word} nonce={NONCE}" for word in ("started", "activation", "exit"))+"\n").encode() if stream == "agent_slog" else b""
        (root / (stream + ".log")).write_bytes(content)
        cursors.append(dict(stream=stream, inode=i+1, inode_before=i+1, before=0, after=len(content), read_ok=True,
                            file=stream+".log", length=len(content), sha256=SENSOR["sha"](content),
                            required={event: [event, NONCE] for event in ("started", "activation", "exit")} if stream == "agent_slog" else []))
    return dict(version=1, nonce=NONCE, images=[image, dict(image, identity=copy.deepcopy(image["identity"]), generation=1, sha256="b"*64, terminal_count=1, exit_code=23)],
                processes=samples, windows=[dict(start=2.5, end=3, fault_observed=True)],
                input=inp, output=out, controls=[dict(op="resize", seq=i+1, ack=i+1, at=at,
                    cols=cols, rows=24, observed_cols=cols, observed_rows=24) for i, (at, cols) in enumerate(((2,80),(4,100)))],
                ctl=dict(pid=4300, start_ticks=43000, wait_status=23 << 8, reaped=True),
                resources=dict(before=before, after=[copy.deepcopy(before)], settled=settled),
                logs=dict(cursors=cursors, read_ok=True), oracle=dict(valid=False, continuous=False, reason="unjudged"))


# origin: S8 internal review s8-hermetic F2. The extracted adapter must
# validate the same synthetic kernel/provenance inputs as the complete driver.
def legacy_functions(root):
    library = PROBE.with_name("hot_update_pty.sh").read_text()
    functions = []
    for name in ("upgrade_legacy_exec_required", "upgrade_legacy_assert", "upgrade_legacy_observation"):
        match = re.search(r"^" + name + r"\(\) \{\n.*?^}\n", library, re.M | re.S)
        assert match, "missing real legacy dependency " + name
        functions.append(match.group())
    old, new = "a"*64, "b"*64
    (root/"legacy-source.sha").write_text(old+"\n")
    (root/"legacy-target.sha").write_text(new+"\n")
    (root/"legacy-source.json").write_text(json.dumps(dict(commit="c"*40, tag="v0.6.0")))
    (root/"legacy-command.rc").write_text("0\n")
    for role, pid in (("agent", 4000), ("broker", 5000)):
        for phase in ("before", "after"):
            sha = new if (role, phase) == ("agent", "after") else old
            (root/f"legacy-{role}-{phase}.json").write_text(json.dumps(dict(
                pid=pid, start_ticks=pid*10, boot_id="fixture-boot", running_sha=sha)))
    for phase in ("before", "after"):
        (root/f"legacy-ctl-{phase}.sha").write_text(old+"  /isolated/old-ctl\n")
    return "".join(functions)


def assembly_fixture(root):
    """Independent raw files plus real-shaped collector/runtime sidecars."""
    doc = complete_fixture(root)
    ident = process(4300)
    SENSOR["write_json"](root/"ctl.identity.json", dict(nonce=NONCE, identity=ident, started_monotonic=1000))
    SENSOR["write_json"](root/"ctl.wait.json", doc["ctl"])
    samples = doc["processes"]
    for row in samples:
        if row["role"] == "foreground":
            row.update(cols=80 if row["at"] < 3 else 100, rows=24)
    SENSOR["write_json"](root/"ctl.trace.json", dict(nonce=NONCE, started_monotonic=1000,
        started_wall=1700000000, scenario="A", samples=[r for r in samples if r["role"] == "ctl"], input=doc["input"]["sent"], output=doc["output"]["received"]))
    SENSOR["write_json"](root/"remote.trace.json", dict(nonce=NONCE, started_monotonic=1000,
        samples=[r for r in samples if r["role"] != "ctl"], input=doc["input"]["received"], output=doc["output"]["sent"]))
    SENSOR["write_json"](root/"windows.json", dict(nonce=NONCE, windows=doc["windows"]))
    (root/"agent").mkdir()
    for name, image, resources, at in (("before",doc["images"][0],doc["resources"]["before"],1001),
            ("after",doc["images"][1],doc["resources"]["after"][0],1004),
            ("settled",doc["images"][1],doc["resources"]["settled"],1005)):
        SENSOR["write_json"](root/"agent"/(name+".json"), dict(nonce=NONCE, identity=image["identity"],
            sha256=image["sha256"], at=at, marker=dict(generation=image["generation"],state=image["phase"]), resources=resources))
    SENSOR["write_json"](root/"agent/settled.ps.json", dict(processes=[dict(pid="task-token", argv=[NONCE],status="EXITED",exit_code=23)]))
    (root/"checkpoints").mkdir()
    for i, control in enumerate(doc["controls"]):
        SENSOR["write_json"](root/"checkpoints"/(str(i)+".json"), dict(nonce=NONCE,local_resize=dict(nonce=NONCE,
            cols=control["cols"],rows=control["rows"],at=control["at"])))
    def stamp(at):
        return datetime.datetime.fromtimestamp(1700000000+at, datetime.timezone.utc).isoformat()
    state = dict(pid="task-token",generation=1, input_applied=100,input_accepted=200,
        input_fin=False,input_ended=False,output_base=100,output_end=200,output_fin=False,
        control_seq=3,ka_seq=2,ka_deadline_boot_ns=555,revoked=False,master_closed=False)
    records = [dict(time=stamp(.1),msg="agent: run reliable=true started",pid="task-token",
                    request_id_hash="c"*64,os_pid=4100,start_time_ticks=41000),
        dict(time=stamp(2),msg="agent: pty control committed",pid="task-token",seq=1,op="resize",outcome="ok"),
        dict(state,time=stamp(2.6),msg="agent: pty image state",phase="cut"),
        dict(state,time=stamp(2.8),msg="agent: pty image state",phase="activated",input_applied=150,output_end=250),
        dict(time=stamp(2.8),msg="agent: image activated generation=1 tasks=1"),
        dict(time=stamp(4),msg="agent: pty control committed",pid="task-token",seq=4,op="resize",outcome="ok"),
        dict(time=stamp(4.95),msg="agent: run reliable=true exited",pid="task-token",code=23,signal=0,
             input_applied=doc["input"]["applied"],input_accepted=doc["input"]["accepted"],input_fin=True,input_ended=True,
             output_base=doc["output"]["base"]-10,output_end=doc["output"]["end"],output_fin=True),
        dict(time=stamp(4.99),msg="agent: run reliable=true settled",pid="task-token",code=23,signal=0,
             input_applied=doc["input"]["applied"],input_accepted=doc["input"]["accepted"],
             output_base=doc["output"]["base"],output_end=doc["output"]["end"])]
    raw = "\n".join(json.dumps(row) for row in records)+"\n"
    (root/"logs").mkdir()
    (root/"logs/agent_slog.log").write_text(raw)
    cursor = next(r for r in doc["logs"]["cursors"] if r["stream"] == "agent_slog")
    cursor.update(file="logs/agent_slog.log",length=len(raw.encode()), after=len(raw.encode()), sha256=SENSOR["sha"](raw.encode()))
    SENSOR["write_json"](root/"logs.json", doc["logs"])
    return doc


def carrier_fixture(root):
    """Real-shaped C1 receipts with unchanged agent generation and PTY owner."""
    doc = assembly_fixture(root)
    trace = SENSOR["read_json"](root/"ctl.trace.json")
    trace["scenario"] = "C1"
    SENSOR["write_json"](root/"ctl.trace.json",trace)
    windows = SENSOR["read_json"](root/"windows.json")
    windows["windows"][0]["kind"] = "carrier"
    SENSOR["write_json"](root/"windows.json",windows)
    for name in ("after","settled"):
        snapshot = SENSOR["read_json"](root/("agent/"+name+".json"))
        snapshot.update(sha256=doc["images"][0]["sha256"],marker=dict(generation=0,state="none"))
        SENSOR["write_json"](root/("agent/"+name+".json"),snapshot)
    records = [json.loads(line) for line in (root/"logs/agent_slog.log").read_text().splitlines()]
    records[2].update(phase="carrier-lost",generation=0)
    records[3].update(phase="carrier-restored",generation=0)
    records[4]["msg"] = "agent: rebuilding session"
    save_runtime_log(root,records)
    return records


def save_runtime_log(root, records, text=False):
    raw = "\n".join((" ".join(k+"="+json.dumps(v) if isinstance(v, str) else
            k+"="+str(v).lower() for k,v in row.items()) if text else json.dumps(row)) for row in records)+"\n"
    (root/"logs/agent_slog.log").write_text(raw)
    logs = SENSOR["read_json"](root/"logs.json")
    cursor = next(c for c in logs["cursors"] if c["stream"] == "agent_slog")
    cursor.update(length=len(raw.encode()), after=len(raw.encode()), sha256=SENSOR["sha"](raw.encode()))
    SENSOR["write_json"](root/"logs.json", logs)


def corrupt(root, doc, name):
    if name in ("drop", "duplicate", "reorder"):
        ids = list(range(50))
        if name == "drop":
            ids.pop(30)
        elif name == "duplicate":
            ids.insert(30, 30)
        else:
            ids[30], ids[31] = ids[31], ids[30]
        doc["output"]["received"] = byte_blob(root, "ctl.received", ids)
    elif name == "sourceledger_as_received":
        doc["output"]["received"] = copy.deepcopy(doc["output"]["sent"])
        (root / "ctl.received").write_bytes(b"actual receiver lost the stream")
    elif name == "empty_window":
        doc["windows"] = []
    elif name == "fault_not_observed":
        doc["windows"][0]["fault_observed"] = False
    elif name == "empty_cut":
        doc["windows"][0].update(start=2.51, end=2.59)
    elif name == "too_few_prefault":
        doc["windows"][0]["start"] = 1.5
    elif name == "no_post_progress":
        doc["windows"][0]["end"] = 5
    elif name == "fake_pid":
        doc["processes"][-3]["identity"]["pid"] += 1
    elif name == "fake_foreground":
        doc["processes"][-2]["foreground_pgid"] += 1
    elif name == "fake_ctty":
        doc["processes"][-1]["ctty"] = False
    elif name == "early_hup":
        doc["processes"][-2]["hup"] = 1
    elif name == "early_eof":
        doc["processes"][-2]["early_eof"] = True
    elif name == "dead_job":
        doc["processes"][-2]["alive"] = False
    elif name == "old_aq":
        doc["input"]["applied"] -= 1
    elif name == "backward_aq":
        doc["input"]["checkpoints"][-1].update(applied=50, accepted=60)
    elif name == "old_output":
        doc["output"]["base"] -= 1
    elif name == "wrongfd":
        doc["resources"]["after"][0]["wrongfd"] = ["/dev/pts/42"]
    elif name == "wrong_master":
        doc["resources"]["after"][0]["masters"][0]["pty_index"] += 1
    elif name == "replay_leak":
        doc["resources"]["settled"]["counts"]["replay"] = 1
    elif name == "handoff_leak":
        doc["resources"]["after"][0]["counts"]["handoff"] = 1
    elif name == "zombie":
        doc["resources"]["settled"]["zombies"] = [process(4200)]
    elif name == "wrong_log_stream":
        cursor = next(c for c in doc["logs"]["cursors"] if c["stream"] == "broker_panic")
        agent = next(c for c in doc["logs"]["cursors"] if c["stream"] == "agent_slog")
        (root / cursor["file"]).write_bytes(b"panic: actual broker failure\n")
        for field in ("file", "length", "sha256", "after"):
            cursor[field] = agent[field]
    elif name == "unreadable_logs":
        doc["logs"]["read_ok"] = False
    elif name == "empty_logs":
        cursor = next(c for c in doc["logs"]["cursors"] if c["stream"] == "agent_slog")
        (root / cursor["file"]).write_bytes(b"")
        cursor.update(length=0, after=0, sha256=SENSOR["sha"](b""))
    elif name == "rotated_logs":
        doc["logs"]["cursors"][0]["before"] = 100
    elif name == "panic":
        cursor = doc["logs"]["cursors"][1]
        content = b"panic: lost task\n"
        (root / cursor["file"]).write_bytes(content)
        cursor.update(length=len(content), after=len(content), sha256=SENSOR["sha"](content))
    elif name == "missing_waitpid":
        doc["ctl"].pop("wait_status")
    elif name == "not_reaped":
        doc["ctl"]["reaped"] = False
    elif name == "wrong_ctl":
        doc["ctl"]["pid"] += 1
    elif name == "wrong_exit":
        doc["ctl"]["wait_status"] = 70 << 8
    elif name == "old_sha":
        doc["images"][1]["sha256"] = doc["images"][0]["sha256"]
    elif name == "agent_restart":
        doc["images"][1]["identity"]["start_ticks"] += 1
    elif name == "two_spawns":
        doc["images"][1]["spawn_count"] = 2
    elif name == "missing_terminal":
        doc["images"][1]["terminal_count"] = 0
    elif name == "resize_no_effect":
        doc["controls"][1]["observed_cols"] = 80
    elif name == "stale_control":
        doc["controls"][1]["ack"] = 1
    elif name == "no_resize":
        doc["controls"] = []
    elif name == "stall":
        for row in doc["output"]["received"]["records"][30:]:
            row["at"] += 5.1
    else:
        raise AssertionError(name)


INVALID_NEGATIVES = {"sourceledger_as_received", "wrong_log_stream", "empty_window", "fault_not_observed", "missing_waitpid", "old_sha", "unreadable_logs", "empty_logs", "rotated_logs"}

NEGATIVES = ("drop", "duplicate", "reorder", "sourceledger_as_received", "empty_window", "fault_not_observed", "empty_cut",
             "too_few_prefault", "no_post_progress", "fake_pid", "fake_foreground", "fake_ctty",
             "early_hup", "early_eof", "dead_job", "old_aq", "backward_aq", "old_output",
             "wrongfd", "wrong_master", "replay_leak", "handoff_leak", "zombie", "unreadable_logs",
             "empty_logs", "rotated_logs", "wrong_log_stream", "panic", "missing_waitpid", "not_reaped", "wrong_ctl",
             "wrong_exit", "old_sha", "agent_restart", "two_spawns", "missing_terminal",
             "resize_no_effect", "stale_control", "no_resize", "stall")


def fake_transport(argv):
    """Two independent PTYs; this private proxy is deliberately not tether."""
    root = Path(argv[0])
    scenario = argv[1] if len(argv) > 1 else "A"
    if scenario == "L":
        import threading
        stop = threading.Event()
        thread = threading.Thread(target=lambda:stop.wait(30),daemon=True)
        thread.start()
        tty.setraw(0, termios.TCSANOW)
        deadline = time.monotonic()+10
        while not (root/"attached.json").exists():
            if time.monotonic()>deadline: raise RuntimeError("private proxy tracer did not release")
            time.sleep(.001)
        stop.set()
        thread.join(timeout=1)
        (root / "proxy.attached").touch()
    master, slave = os.openpty()
    tty.setraw(slave, termios.TCSANOW)
    read_end, write_end = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(read_end)
        os.close(master)
        os.setsid()
        fcntl.ioctl(slave, termios.TIOCSCTTY, 0)
        os.tcsetpgrp(slave, os.getpgrp())
        for fd in range(3):
            os.dup2(slave, fd)
        if slave > 2:
            os.close(slave)
        os.write(write_end, b"ready")
        os.close(write_end)
        os.execv(sys.executable, [sys.executable, "-I", "-B", str(PROBE), "trace",
                  "--role", "remote", "--evidence", str(root), "--nonce", NONCE, "--scenario", scenario, "--period", ".01", "--timeout", "15"])
    os.close(write_end)
    os.close(slave)
    assert os.read(read_end, 5) == b"ready"
    os.close(read_end)
    (root / "proxy.identity.json").write_text(json.dumps(SENSOR["identity"](os.getpid())))
    def resize_remote(*_):
        size = fcntl.ioctl(0, termios.TIOCGWINSZ, bytes(8))
        fcntl.ioctl(master, termios.TIOCSWINSZ, size)
    signal.signal(signal.SIGWINCH, resize_remote)
    resize_remote()
    waited, status = 0, None
    try:
        while not waited:
            for fd in select.select([0, master], [], [], .01)[0]:
                try:
                    chunk = os.read(fd, 32768)
                except OSError as exc:
                    if exc.errno != 5 or fd != master:
                        raise
                    chunk = b""
                if not chunk and fd == 0 and scenario == "C2":
                    SENSOR["write_json"](root / "proxy.local-eof.json", dict(identity=SENSOR["identity"](os.getpid()), observed=True))
                if chunk:
                    target, offset = (master if fd == 0 else 1), 0
                    while offset < len(chunk):
                        offset += os.write(target, chunk[offset:])
            waited, status = os.waitpid(pid, os.WNOHANG)
        while select.select([master], [], [], 0)[0]:
            try:
                chunk = os.read(master, 32768)
            except OSError:
                break
            if not chunk:
                break
            os.write(1, chunk)
    finally:
        os.close(master)
        if not waited:
            os.killpg(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
    return os.waitstatus_to_exitcode(status)


class PTYProbeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="hot-update-pty-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def invoke(self, command, *extra):
        return subprocess.run([sys.executable, "-I", "-B", str(PROBE), command,
                "--evidence", str(self.root), "--nonce", NONCE, *map(str, extra)],
                capture_output=True, text=True, timeout=20)

    def save(self, doc):
        SENSOR["write_json"](self.root / "evidence.json", doc)

    # origin: hot-update S4 plan §8 T48 / §9.1.
    def test_complete_duplex_trace_passes(self):
        self.save(complete_fixture(self.root))
        result = self.invoke("continuity")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(result.stdout)["oracle"]["continuous"])
        self.assertIn(b"\r\n", (self.root / "ctl.received").read_bytes())
        self.assertIn(b"\x04\x03", (self.root / "ctl.sent").read_bytes())

    # origin: hot-update S4 plan §8 T48. Each defect has complete byte files;
    # an unrelated missing file cannot accidentally reject a broken fixture.
    def test_discontinuities_and_invalid_evidence_fail_closed(self):
        for name in NEGATIVES:
            with self.subTest(name=name):
                doc = complete_fixture(self.root)
                corrupt(self.root, doc, name)
                self.save(doc)
                result = self.invoke("continuity")
                self.assertEqual(result.returncode, 2 if name in INVALID_NEGATIVES else 1,
                                 (name, result.stdout, result.stderr))

    # origin: hot-update S4 plan §9.3 / T48. Same oracle/rc for positive,
    # product failure and invalid setup; no raw failure gets wrapped GREEN.
    def test_legacy_exporter_preserves_native_continuity_result(self):
        for name, expected in (("positive", 0), ("early_hup", 1), ("empty_window", 2), ("unreadable_logs", 2), ("missing_waitpid", 2)):
            with self.subTest(name=name):
                doc = complete_fixture(self.root)
                if name != "positive":
                    corrupt(self.root, doc, name)
                self.save(doc)
                positive, negative = self.invoke("continuity"), self.invoke("legacy-exporter")
                self.assertEqual(positive.returncode, expected)
                self.assertEqual(negative.returncode, expected)
                self.assertEqual(positive.stdout, negative.stdout)
                self.assertEqual("HOT_UPDATE_LEGACY_PTY_EXPORTER output=" in negative.stderr, expected == 1)

        # origin: hot-update S5 plan §9.3 / T48 / DRILL-RUN 39.P.
        # The real v0.6 bare-exec marker predates image generations. Its
        # missing field is valid only for the independently bound legacy fault.
        before = dict(identity=dict(pid=4000), sha256="a"*64, marker=None)
        (self.root/"agent").mkdir(exist_ok=True)
        SENSOR["write_json"](self.root/"agent/before.json", before)
        (self.root/"endpoint-agent.sha").write_text("a"*64+"  /proc/4000/exe\n")
        (self.root/"endpoint-agent.version").write_text("tether v0.6.0 (proto v2)\nlinux/amd64\ngo1.26.8\n")
        snapshot = dict(identity=dict(pid=4000), sha256="b"*64, marker=dict(
            state="committed", prev_sha="a"*64, new_sha="b"*64,
            target_sid="lab", target_nid="agt1"))
        self.assertEqual(SENSOR["snapshot_generation"](self.root, snapshot, legacy_bare_exec=True), 0)
        for mode in ("reliable", "wrong_prev", "wrong_current", "wrong_pid", "wrong_sid", "wrong_nid",
                     "imported", "resource_phase", "null_generation", "pending", "retained_before",
                     "capable_source", "other_legacy_version"):
            with self.subTest(marker=mode):
                invalid = copy.deepcopy(snapshot)
                SENSOR["write_json"](self.root/"agent/before.json", before)
                (self.root/"endpoint-agent.version").write_text("tether v0.6.0 (proto v2)\nlinux/amd64\ngo1.26.8\n")
                if mode == "wrong_prev": invalid["marker"]["prev_sha"] = "c"*64
                if mode == "wrong_current": invalid["sha256"] = "c"*64
                if mode == "wrong_pid": invalid["identity"]["pid"] = 4001
                if mode == "wrong_sid": invalid["marker"]["target_sid"] = "other"
                if mode == "wrong_nid": invalid["marker"]["target_nid"] = "agt2"
                if mode == "imported": invalid["marker"]["imported_resources"] = 1
                if mode == "resource_phase": invalid["marker"]["resource_phase"] = "active"
                if mode == "null_generation": invalid["marker"]["generation"] = None
                if mode == "pending": invalid["marker"]["state"] = "pending"
                if mode == "retained_before":
                    SENSOR["write_json"](self.root/"agent/before.json", dict(before, marker=dict(generation=0)))
                if mode == "capable_source":
                    (self.root/"endpoint-agent.version").write_text("tether fixture\nimage-domains exec-v1 pty-v1 flow-v1 ss-v1\nimage-abi v1\n")
                if mode == "other_legacy_version":
                    (self.root/"endpoint-agent.version").write_text("tether v0.7.0 (proto v2)\nlinux/amd64\ngo1.26.8\n")
                with self.assertRaises(SENSOR["EvidenceError"]):
                    SENSOR["snapshot_generation"](self.root, invalid, legacy_bare_exec=mode != "reliable")

    # origin: hot-update S4 plan §9.1–§9.3 / T48; drill-run4 diagnostics.
    def test_continuity_failure_prints_reason_for_business_and_invalid_evidence(self):
        for defect,expected in (("early_hup",1),("empty_window",2)):
            with self.subTest(defect=defect):
                doc=complete_fixture(self.root)
                corrupt(self.root,doc,defect)
                self.save(doc)
                result=self.invoke("continuity")
                self.assertEqual(result.returncode,expected)
                self.assertIn(json.loads(result.stdout)["oracle"]["reason"],result.stderr)
                self.assertIn("HOT_UPDATE_PTY_CONTINUITY_FAILED:",result.stderr)

    # origin: hot-update S4 plan §9.3 / T48. An actually fixed legacy path
    # must be detected as continuous, rather than fail for nonexistent ACKs.
    def test_legacy_byte_continuity_does_not_invent_reliable_protocol_receipts(self):
        for name in ("healthy", "hup", "drop", "unproved_path"):
            with self.subTest(name=name):
                doc = complete_fixture(self.root)
                doc["input"].update(protocol="legacy", applied=0, accepted=0)
                doc["output"].update(base=0, end=0)
                for row in doc["input"]["checkpoints"]:
                    row.update(applied=0, accepted=0)
                for row in doc["output"]["checkpoints"]:
                    row.update(base=0, end=0)
                doc["ctl"].update(path="legacy", pty_version=0)
                for row in doc["controls"]:
                    row.update(seq=0, ack=0)
                for snapshot in [doc["resources"]["before"], *doc["resources"]["after"]]:
                    snapshot["counts"]["replay"] = 0
                if name == "hup":
                    corrupt(self.root, doc, "early_hup")
                elif name == "drop":
                    corrupt(self.root, doc, "drop")
                elif name == "unproved_path":
                    doc["ctl"].pop("pty_version")
                self.save(doc)
                positive, negative = self.invoke("continuity"), self.invoke("legacy-exporter")
                expected = 0 if name == "healthy" else 2 if name == "unproved_path" else 1
                self.assertEqual(positive.returncode, expected, (name, positive.stdout, positive.stderr))
                self.assertEqual(negative.returncode, expected)
                self.assertEqual(positive.stdout, negative.stdout)
                self.assertEqual("HOT_UPDATE_LEGACY_PTY_EXPORTER output=" in negative.stderr, expected == 1)

    # origin: hot-update S4 plan §9.2 / T48. W deliberately blackholes the
    # carrier for 120s; its unchanged ctl has a fixed 180s recovery budget.
    def test_watchdog_window_requires_real_blackhole_and_bounded_recovery(self):
        for name in ("watchdog", "normal", "short_blackhole", "no_rollback", "over_recovery"):
            with self.subTest(name=name):
                doc = complete_fixture(self.root)
                gap = 181 if name == "over_recovery" else 120
                for direction in ("input", "output"):
                    for side in ("sent", "received"):
                        for row in doc[direction][side]["records"][26:]:
                            row["at"] += gap
                for row in doc["processes"]:
                    if row["at"] >= 3:
                        row["at"] += gap
                doc["controls"][-1]["at"] += gap
                doc["windows"][0].update(kind="upgrade" if name == "normal" else "watchdog",
                    end=2.5+gap, blackhole_s=119 if name == "short_blackhole" else 120)
                original = copy.deepcopy(doc["images"][0])
                original.update(generation=2, terminal_count=1, exit_code=23,
                                phase="committed" if name == "no_rollback" else "rolled_back")
                doc["images"][-1].update(phase="active", terminal_count=0, exit_code=0)
                doc["images"].append(original)
                self.save(doc)
                result = self.invoke("continuity")
                self.assertEqual(result.returncode, 0 if name == "watchdog" else
                                 2 if name in ("short_blackhole", "no_rollback") else 1, (name, result.stdout, result.stderr))

    # origin: hot-update S4 plan §9.2 / T48. Intentional draining is narrowly
    # tied to a measured leader exit; unrelated HUP/TTY/group defects stay red.
    def test_draining_receipt_does_not_exempt_unrelated_terminal_failures(self):
        for defect in ("valid","missing_exit","short_tail","extra_hup","early_ctty","wrong_group","early_eof"):
            with self.subTest(defect=defect):
                doc=complete_fixture(self.root)
                doc["windows"][0].update(kind="draining",leader_exited=True,leader_exit_at=2.4,tail_s=.25,draining_end_at=4.9)
                doc["processes"]=[r for r in doc["processes"] if r["role"] != "leader" or r["at"]<2.4]
                for row in doc["processes"]:
                    if row["role"] == "foreground" and row["at"]>=2.4:
                        row.update(ctty=False,hup=1,foreground_pgid=-1)
                        row["identity"]["process_group"]=row["identity"]["pid"]
                last=next(r for r in reversed(doc["processes"]) if r["role"] == "foreground")
                if defect=="missing_exit": del doc["windows"][0]["leader_exit_at"]
                elif defect=="short_tail": doc["windows"][0]["tail_s"] = .1
                elif defect=="extra_hup": last["hup"] = 2
                elif defect=="early_ctty": next(r for r in doc["processes"] if r["role"] == "foreground")["ctty"] = False
                elif defect=="wrong_group": last["identity"]["process_group"] += 1
                elif defect=="early_eof": last["early_eof"] = True
                self.save(doc)
                result=self.invoke("continuity")
                self.assertEqual(result.returncode,0 if defect=="valid" else 2 if defect in ("missing_exit","short_tail") else 1,
                    (defect,result.stdout,result.stderr))

    # origin: hot-update S4 plan §9.2 / T48; drill-run3 C2. /proc polling
    # observes an exit after the kernel's HUP; the last live leader sample is
    # the independent lower bound, rather than a guessed timing tolerance.
    def test_draining_exit_observation_bounds_kernel_hangup(self):
        for defect in ("valid", "forged_lower_bound", "early_hup", "extra_hup"):
            with self.subTest(defect=defect):
                doc = complete_fixture(self.root)
                doc["windows"][0].update(kind="draining", leader_exited=True, leader_exit_at=2.4,
                    leader_exit_lower_at=2, tail_s=.25, draining_end_at=4.9)
                doc["processes"] = [r for r in doc["processes"] if r["role"] != "leader" or r["at"] <= 2]
                foreground = [r for r in doc["processes"] if r["role"] == "foreground"]
                for row in foreground:
                    row["identity"]["process_group"] = row["identity"]["pid"]
                # Last live leader at 2; HUP at 2.3; /proc observes exit at 2.4.
                foreground[5]["at"] = 2.3
                for row in foreground[5:]:
                    row.update(ctty=False, hup=1, foreground_pgid=-1)
                if defect == "forged_lower_bound": doc["windows"][0]["leader_exit_lower_at"] = 0
                elif defect == "early_hup": foreground[3]["hup"] = 1
                elif defect == "extra_hup": foreground[-1]["hup"] = 2
                self.save(doc)
                result = self.invoke("continuity")
                self.assertEqual(result.returncode, 0 if defect == "valid" else 2 if defect == "forged_lower_bound" else 1,
                    result.stdout+result.stderr)

    # origin: hot-update S4 plan §9.2 / T48; drill-run3 C2. Exercise the
    # assembler too: a judge-only fixture cannot catch an omitted lower bound.
    def test_draining_assembly_keeps_last_live_leader_bound(self):
        assembly_fixture(self.root)
        trace = SENSOR["read_json"](self.root/"ctl.trace.json")
        trace["scenario"] = "C2"
        SENSOR["write_json"](self.root/"ctl.trace.json", trace)
        remote = SENSOR["read_json"](self.root/"remote.trace.json")
        remote["samples"] = [r for r in remote["samples"] if r["role"] != "leader" or r["at"] <= 2]
        foreground = [r for r in remote["samples"] if r["role"] == "foreground"]
        foreground[5]["at"] = 2.3
        for row in foreground:
            row["identity"]["process_group"] = row["identity"]["pid"]
            if row["at"] >= 2.3: row.update(ctty=False, hup=1, foreground_pgid=-1)
        SENSOR["write_json"](self.root/"remote.trace.json", remote)
        windows = SENSOR["read_json"](self.root/"windows.json")
        windows["windows"][0].update(kind="draining", leader_exited=True, leader_exit_at=2.4, tail_s=.25)
        SENSOR["write_json"](self.root/"windows.json", windows)
        (self.root/"canonical.sent").write_bytes(b"\x04\x03")
        SENSOR["write_json"](self.root/"canonical.remote.json", dict(nonce=NONCE, identity=process(4200), eof=True, sigint=1))
        SENSOR["write_json"](self.root/"fin.local.json", dict(nonce=NONCE))
        SENSOR["write_json"](self.root/"fin.remote.json", dict(nonce=NONCE, tail_s=.3, source_ended_monotonic=1004.9))
        rows = [json.loads(line) for line in (self.root/"logs/agent_slog.log").read_text().splitlines()]
        for row in rows:
            if row["msg"] in ("agent: run reliable=true exited", "agent: run reliable=true settled"):
                row["input_applied"] += 2
                row["input_accepted"] += 2
        save_runtime_log(self.root, rows)
        result = self.invoke("trace", "--role", "assemble")
        self.assertEqual(result.returncode, 0, result.stderr)
        envelope = json.loads(result.stdout)
        self.assertEqual(envelope["windows"][0]["leader_exit_lower_at"], 2)
        self.assertEqual(self.invoke("continuity").returncode, 0)

    # origin: hot-update S4 plan §8 T48. Hash metadata cannot replace reads.
    def test_missing_bytes_fields_and_invalid_counts_are_setup_errors(self):
        doc = complete_fixture(self.root)
        for key in SENSOR["TOP_KEYS"]:
            with self.subTest(key=key):
                broken = copy.deepcopy(doc)
                broken.pop(key)
                self.save(broken)
                self.assertEqual(self.invoke("continuity").returncode, 2)
        for value in (-1, True, 1.5, "0"):
            broken = copy.deepcopy(doc)
            broken["input"]["applied"] = value
            self.save(broken)
            self.assertEqual(self.invoke("continuity").returncode, 2)
        self.save(doc)
        (self.root / "ctl.received").write_bytes(b"forged")
        self.assertEqual(self.invoke("continuity").returncode, 2)
        (self.root / "ctl.received").unlink()
        self.assertEqual(self.invoke("continuity").returncode, 2)

    # origin: hot-update S4 plan §9.1 / T48. Real kernel proof of two cttys,
    # duplex bytes and waitpid, separate from synthetic image receipts.
    def test_real_controlling_tty_duplex_bytes_and_original_waitpid(self):
        runner = [sys.executable, "-I", "-B", str(PROBE), "trace", "--role", "ctl",
                  "--evidence", str(self.root), "--nonce", NONCE, "--ctl-binary", sys.executable,
                  "--period", ".01", "--timeout", "15", "--", "-I", "-B", str(Path(__file__).resolve()),
                  "--proxy", str(self.root)]
        child = subprocess.Popen(runner, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic()+10
            while True:
                self.assertIsNone(child.poll(), "ctl exited before progress")
                self.assertLess(time.monotonic(), deadline, "duplex progress did not arrive")
                sent, received = self.root / "ctl.sent", self.root / "ctl.received"
                if sent.exists() and received.exists() and sent.stat().st_size > 12000 and received.stat().st_size > 12000:
                    break
                time.sleep(.01)
            checkpoint = self.invoke("trace", "--role", "checkpoint", "--name", "cut", "--cols", 101, "--rows", 37)
            self.assertEqual(checkpoint.returncode, 0, checkpoint.stderr)
            while True:
                self.assertLess(time.monotonic(), deadline, "remote resize effect did not arrive")
                observation = json.loads((self.root / "remote.observation.json").read_text())
                if (observation["cols"], observation["rows"]) == (101, 37):
                    break
                time.sleep(.01)
            (self.root / "finish").touch()
            stdout, stderr = child.communicate(timeout=5)
            self.assertEqual(child.returncode, 0, (stdout, stderr))
        finally:
            if child.poll() is None:
                (self.root / "finish").touch()
                try:
                    child.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
            child.communicate()
        local = json.loads((self.root / "proxy.identity.json").read_text())
        remote = json.loads((self.root / "remote.identity.json").read_text())["identity"]
        ctl = json.loads((self.root / "ctl.identity.json").read_text())["identity"]
        outcome = json.loads((self.root / "ctl.wait.json").read_text())
        resize = json.loads((self.root / "resize.receipt.json").read_text())
        self.assertEqual((resize["cols"], resize["rows"]), (101, 37))
        self.assertEqual(local["pid"], ctl["pid"])
        self.assertEqual(local["start_ticks"], ctl["start_ticks"])
        self.assertNotEqual(local["tty_nr"], remote["tty_nr"])
        for ident in (local, remote):
            self.assertEqual(ident["session_id"], ident["pid"])
            self.assertEqual(ident["foreground_pgid"], ident["pid"])
            self.assertNotEqual(ident["tty_nr"], 0)
        self.assertEqual(outcome, dict(pid=ctl["pid"], start_ticks=ctl["start_ticks"], wait_status=23 << 8, reaped=True))
        self.assertEqual((self.root / "ctl.sent").read_bytes(), (self.root / "remote.received").read_bytes())
        self.assertEqual((self.root / "remote.sent").read_bytes(), (self.root / "ctl.received").read_bytes())
        self.assertGreater(len(json.loads((self.root / "ctl.trace.json").read_text())["input"]["records"]), 40)

    # origin: hot-update S4 plan §9.2 / 36.L / T48. The original process
    # cannot reach its attach action before release of the kernel ioctl entry.
    def test_legacy_pre_attach_barrier_holds_original_ctl(self):
        for round_number in range(5):
            root = self.root/str(round_number)
            root.mkdir()
            runner = [sys.executable,"-I","-B",str(PROBE),"trace","--role","ctl","--scenario","L",
                      "--evidence",str(root),"--nonce",NONCE,"--ctl-binary",sys.executable,
                      "--period",".01","--timeout","15","--","-I","-B",str(Path(__file__).resolve()),"--proxy",str(root),"L"]
            child = subprocess.Popen(runner, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            def wait_for(predicate):
                deadline = time.monotonic()+5
                while not predicate():
                    if child.poll() is not None:
                        self.fail(str(child.communicate()))
                    self.assertLess(time.monotonic(),deadline,"pre-attach action timed out")
                    time.sleep(.005)
            try:
                wait_for(lambda:(root/"before-attach.json").exists())
                barrier = SENSOR["read_json"](root/"before-attach.json")
                self.assertEqual((barrier["syscall"],barrier["fd"],barrier["request"]),("ioctl",0,termios.TCSETS))
                self.assertFalse((root/"proxy.attached").exists())
                self.assertEqual(SENSOR["identity"](barrier["identity"]["pid"])["start_ticks"],barrier["identity"]["start_ticks"])
                (root/"attach.release").touch()
                wait_for(lambda:(root/"proxy.attached").exists())
                wait_for(lambda:(root/"ctl.sent").exists() and (root/"ctl.sent").stat().st_size>5000)
                (root/"finish").touch()
                stdout,stderr = child.communicate(timeout=5)
                self.assertEqual(child.returncode,0,(stdout,stderr))
                self.assertEqual(SENSOR["read_json"](root/"ctl.wait.json")["wait_status"],23<<8)
            finally:
                if child.poll() is None:
                    (root/"attach.release").touch()
                    (root/"finish").touch()
                    try: child.communicate(timeout=5)
                    except subprocess.TimeoutExpired: child.kill(); child.communicate()

    # origin: hot-update S4 plan §9.2 / C2 / T48. Five real double-PTY
    # rounds distinguish remote canonical EOF/SIGINT from local logical FIN.
    def test_canonical_controls_and_local_fin_keep_real_terminals(self):
        for round_number in range(5):
            root = self.root/str(round_number)
            root.mkdir()
            runner = [sys.executable,"-I","-B",str(PROBE),"trace","--role","ctl","--scenario","C2",
                      "--evidence",str(root),"--nonce",NONCE,"--ctl-binary",sys.executable,
                      "--period",".01","--timeout","15","--","-I","-B",str(Path(__file__).resolve()),"--proxy",str(root),"C2"]
            child = subprocess.Popen(runner, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            def wait_for(predicate):
                deadline = time.monotonic()+5
                while not predicate():
                    if child.poll() is not None: self.fail(str(child.communicate()))
                    self.assertLess(time.monotonic(),deadline,"canonical action timed out")
                    time.sleep(.005)
            def phase(expected):
                path = root/"canonical.remote.json"
                return path.exists() and SENSOR["read_json"](path)["phase"] == expected
            try:
                wait_for(lambda:(root/"ctl.sent").exists() and (root/"ctl.sent").stat().st_size>5000)
                (root/"canonical.request").touch()
                wait_for(lambda:phase("ready"))
                (root/"canonical.eof").touch()
                wait_for(lambda:phase("eof"))
                (root/"canonical.sigint").touch()
                wait_for(lambda:phase("complete"))
                effect = SENSOR["read_json"](root/"canonical.remote.json")
                self.assertTrue(effect["eof"])
                self.assertEqual(effect["sigint"],1)
                self.assertFalse(effect["lflag"] & termios.ICANON)
                (root/"canonical.complete").touch()
                wait_for(lambda:(root/"ctl.sent").stat().st_size>12000)
                (root/"finish").touch()
                stdout,stderr = child.communicate(timeout=5)
                self.assertEqual(child.returncode,0,(stdout,stderr))
                self.assertEqual((root/"canonical.sent").read_bytes(),b"\x04\x03")
                self.assertEqual((root/"ctl.sent").read_bytes(),(root/"remote.received").read_bytes())
                self.assertEqual((root/"ctl.received").read_bytes(),(root/"remote.sent").read_bytes())
                self.assertTrue(SENSOR["read_json"](root/"proxy.local-eof.json")["observed"])
                self.assertGreater(SENSOR["read_json"](root/"proxy.local-eof.json")["identity"]["tty_nr"],0)
                self.assertGreater(SENSOR["read_json"](root/"fin.remote.json")["tail_s"],.2)
                # origin: hot-update S4 internal review rv-drill DRILL-3.
                # The final live-slave sample must fit the measured tail bound.
                trace = SENSOR["read_json"](root/"remote.trace.json")
                fin = SENSOR["read_json"](root/"fin.remote.json")
                self.assertGreaterEqual(fin["source_ended_monotonic"],
                    trace["started_monotonic"] + trace["samples"][-1]["at"])
                self.assertEqual(SENSOR["read_json"](root/"ctl.wait.json")["wait_status"],23<<8)
            finally:
                if child.poll() is None:
                    (root/"finish").touch()
                    try: child.communicate(timeout=5)
                    except subprocess.TimeoutExpired: child.kill(); child.communicate()

    # origin: hot-update S4 plan §9.1 / T48. Inspect owner while alive.
    def test_census_distinguishes_master_index_and_wrong_slave(self):
        master, slave = os.openpty()
        try:
            first = SENSOR["census"](os.getpid())
            self.assertEqual(first["counts"]["master"], 1)
            self.assertEqual(len(first["wrongfd"]), 1, first["wrongfd"])
            second_master, second_slave = os.openpty()
            try:
                second = SENSOR["census"](os.getpid())
                self.assertEqual(second["counts"]["master"], 2)
                self.assertEqual(len({m["pty_index"] for m in second["masters"]}), 2)
            finally:
                os.close(second_master)
                os.close(second_slave)
        finally:
            os.close(master)
            os.close(slave)
        self.assertEqual(SENSOR["census"](os.getpid())["counts"]["master"], 0)

    # origin: hot-update S4 plan §9.1 / T48. Concurrent mixed requests can
    # create other PTYs, but duplicate references to the original are leaks.
    def test_census_baseline_tracks_original_objects_and_duplicate_references(self):
        first, first_slave = os.openpty()
        second, second_slave = None, None
        try:
            baseline = SENSOR["census"](os.getpid())
            second, second_slave = os.openpty()
            current = SENSOR["census"](os.getpid(), baseline=baseline)
            self.assertEqual(current["counts"]["master"], 1)
            self.assertEqual(current["masters"], baseline["masters"])
            duplicate = os.dup(first)
            try:
                self.assertEqual(SENSOR["census"](os.getpid(), baseline=baseline)["counts"]["master"], 2)
                os.set_inheritable(duplicate, True)
                self.assertTrue(any(r.get("reason") == "inherited" for r in SENSOR["census"](os.getpid())["wrongfd"]))
            finally:
                os.close(duplicate)
            os.close(first)
            first = None
            self.assertEqual(SENSOR["census"](os.getpid(), baseline=baseline)["counts"]["master"], 0)
        finally:
            for fd in (first, first_slave, second, second_slave):
                if fd is not None:
                    os.close(fd)

    # origin: hot-update S4 plan §9.2 / C2. Five real controlling sessions:
    # original leader exits, original foreground alone retains >200ms of tail.
    def test_draining_leader_leaves_original_foreground_tail(self):
        import ctypes
        libc = ctypes.CDLL(None)
        self.assertEqual(libc.prctl(36,1,0,0,0),0)  # Subreaper owns the orphan's real waitpid.
        try:
            for round_number in range(5):
                root = self.root/str(round_number)
                root.mkdir()
                leader, master = os.forkpty()
                if leader == 0:
                    os.execv(sys.executable,[sys.executable,"-I","-B",str(PROBE),"trace","--role","leader",
                        "--scenario","C2","--evidence",str(root),"--nonce",NONCE,"--period",".01","--timeout","10"])
                os.set_blocking(master,False)
                foreground, leader_reaped, foreground_reaped = None,False,False
                received = b""
                try:
                    deadline=time.monotonic()+5
                    while not (root/"remote.trace.json").exists():
                        self.assertLess(time.monotonic(),deadline,"foreground never started")
                        if select.select([master],[],[],.01)[0]:
                            received += os.read(master,32768)
                    foreground=SENSOR["read_json"](root/"remote.identity.json")["identity"]
                    self.assertNotEqual(leader,foreground["pid"])
                    self.assertEqual(foreground["foreground_pgid"],foreground["pid"])
                    (root/"draining").touch()
                    while True:
                        waited,status=os.waitpid(leader,os.WNOHANG)
                        if waited:
                            leader_reaped=True
                            self.assertEqual(os.waitstatus_to_exitcode(status),23)
                            break
                        self.assertLess(time.monotonic(),deadline,"leader never entered draining")
                        time.sleep(.01)
                    before=(root/"remote.sent").stat().st_size
                    until=time.monotonic()+.25
                    while time.monotonic()<until:
                        if select.select([master],[],[],.01)[0]:
                            received += os.read(master,32768)
                    self.assertGreater((root/"remote.sent").stat().st_size,before)
                    self.assertEqual(SENSOR["valid_identity"](SENSOR["identity"](foreground["pid"])),SENSOR["valid_identity"](foreground))
                    os.write(master,SENSOR["frame"](NONCE,0,b"STOP"))
                    while True:
                        waited,status=os.waitpid(foreground["pid"],os.WNOHANG)
                        if waited:
                            foreground_reaped=True
                            self.assertEqual(os.waitstatus_to_exitcode(status),23)
                            break
                        self.assertLess(time.monotonic(),deadline,"foreground never finished")
                        if select.select([master],[],[],.01)[0]:
                            try:
                                received += os.read(master,32768)
                            except OSError:
                                pass
                    remote=SENSOR["read_json"](root/"remote.trace.json")
                    self.assertTrue(remote["normal_stop"])
                    self.assertEqual(remote["hup"],1)
                    self.assertGreaterEqual(len(SENSOR["parse_frames"](received,NONCE,complete=False)),20)
                finally:
                    os.close(master)
                    for pid,reaped in ((leader,leader_reaped),(foreground["pid"] if foreground else None,foreground_reaped)):
                        if pid and not reaped:
                            try:
                                os.kill(pid,signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                            try:
                                os.waitpid(pid,0)
                            except ChildProcessError:
                                pass
        finally:
            self.assertEqual(libc.prctl(36,0,0,0,0),0)

    # origin: hot-update S4 plan §9.3 / T48. Post-fault collection retains
    # death and original waitpid instead of requiring the old ctl to be alive.
    def test_post_fault_checkpoint_keeps_original_reaped_ctl_receipt(self):
        for round_number in range(5):
            root=self.root/str(round_number)
            root.mkdir()
            read_end,write_end=os.pipe()
            pid=os.fork()
            if pid==0:
                os.close(write_end)
                os.read(read_end,1)
                os._exit(23)
            os.close(read_end)
            ident=SENSOR["identity"](pid)
            os.write(write_end,b"1")
            os.close(write_end)
            waited,status=os.waitpid(pid,0)
            self.assertEqual(waited,pid)
            SENSOR["write_json"](root/"ctl.identity.json",dict(nonce=NONCE,identity=ident,started_monotonic=time.monotonic()))
            wait=dict(pid=pid,start_ticks=ident["start_ticks"],wait_status=status,reaped=True)
            SENSOR["write_json"](root/"ctl.wait.json",wait)
            (root/"ctl.sent").write_bytes(b"actual input")
            (root/"ctl.received").write_bytes(b"actual interrupted output")
            result=subprocess.run([sys.executable,"-I","-B",str(PROBE),"trace","--role","checkpoint","--name","post-fault",
                "--evidence",str(root),"--nonce",NONCE],capture_output=True,text=True,timeout=5)
            self.assertEqual(result.returncode,0,result.stderr)
            checkpoint=SENSOR["read_json"](root/"post-fault.json")
            self.assertTrue(checkpoint["ctl_reaped"])
            self.assertEqual(checkpoint["identity"],ident)
            self.assertEqual(SENSOR["read_json"](root/"ctl.wait.json"),wait)
            self.assertEqual(os.waitstatus_to_exitcode(wait["wait_status"]),23)

    # origin: hot-update S4 internal review rv-drill DRILL-4.
    def test_legacy_missing_exit_keeps_measured_original_ctl_timeout(self):
        for round_number in range(5):
            root = self.root / str(round_number)
            root.mkdir()
            runner = [sys.executable, "-I", "-B", str(PROBE), "trace", "--role", "ctl", "--scenario", "P",
                      "--evidence", str(root), "--nonce", NONCE, "--ctl-binary", sys.executable,
                      "--timeout", "1", "--", "-c", "import time; time.sleep(30)"]
            result = subprocess.run(runner, capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            original = SENSOR["read_json"](root / "ctl.identity.json")["identity"]
            outcome = SENSOR["read_json"](root / "ctl.wait.json")
            self.assertEqual((outcome["pid"], outcome["start_ticks"]),
                             (original["pid"], original["start_ticks"]))
            self.assertTrue(outcome["reaped"])
            self.assertTrue(outcome["timed_out"])
            self.assertTrue(os.WIFSIGNALED(outcome["wait_status"]))
            observed = outcome["timeout_observation"]
            self.assertEqual(observed["nonce"], NONCE)
            self.assertGreaterEqual(observed["elapsed_s"], observed["deadline_s"])
            with self.assertRaises(FileNotFoundError):
                SENSOR["identity"](original["pid"])

    # origin: hot-update S4 plan §9.1 / T48. This stubs only the log reader,
    # with real byte files and metadata; production logs.sh is never invoked.
    def test_log_collection_requires_readable_unrotated_complete_snapshots(self):
        library = PROBE.with_name("hot_update_pty.sh")
        here = PROBE.parents[2]
        for name in ("valid", "legacy_component_growth", "journal_vacuum", "unreadable", "rotated", "truncated", "missing_api"):
            with self.subTest(name=name):
                root = self.root / name
                root.mkdir()
                for phase in ("before", "after"):
                    folder = root / "logs" / phase
                    folder.mkdir(parents=True)
                    for stream in SENSOR["STREAMS"]:
                        raw = b"prefix\n" + (b"new lines\n" if phase == "after" else b"")
                        if name == "truncated" and phase == "after":
                            raw = b"new lines\n"
                        meta = dict(inode=1, length=len(raw), read_ok=True)
                        if name == "legacy_component_growth" and stream == "broker_slog":
                            raw += b"old"
                            meta.update(length=len(raw),parts=[dict(path="current",dev=1,inode=1,length=len(raw)-3),dict(path="legacy",dev=1,inode=2,length=3)])
                        if name == "journal_vacuum" and stream == "broker_panic":
                            meta.update(journal_identity="boot-unit",journal_cursors=["A" if phase == "before" else "B"])
                        if name == "unreadable" and phase == "after":
                            meta["read_ok"] = False
                        if name == "rotated" and phase == "after":
                            meta["inode"] = 2
                        (folder / (stream+".raw")).write_bytes(raw)
                        (folder / (stream+".json")).write_text(json.dumps(meta))
                if name != "valid":
                    (root / "logs.json").write_text(json.dumps(dict(read_ok=True, cursors=[])))
                script = 'HERE=$1; PTY_EVIDENCE=$2; PTY_AGENT_NODE=agt1; . "$3"; '
                if name != "missing_api":
                    script += 'sim_log_snapshot() { test -r "$3" && test -r "$4"; }; '
                if name == "missing_api":
                    script += 'unset -f sim_log_snapshot; '
                script += 'pty_collect_logs after'
                result = subprocess.run(["sh", "-c", script, "test", str(here), str(root), str(library)],
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode == 0, name in ("valid","legacy_component_growth"), (name, result.stderr))
                if name in ("valid","legacy_component_growth"):
                    cursors = json.loads((root / "logs.json").read_text())["cursors"]
                    self.assertEqual(len(cursors), 4)
                    for cursor in cursors:
                        legacy = name == "legacy_component_growth" and cursor["stream"] == "broker_slog"
                        self.assertEqual((cursor["before"],cursor["after"]),(10,20) if legacy else (7,17))
                    self.assertEqual((root / "logs/agent_slog.log").read_bytes(), b"new lines\n")
                else:
                    self.assertFalse((root / "logs.json").exists())

    # origin: hot-update S4 plan §9.1 / T48. Run logs.sh's actual snapshot
    # reader against isolated files and cursor responses, never Docker or home.
    def test_log_snapshot_preserves_bytes_and_rejects_read_rotation_failures(self):
        driver = self.root/"snapshot-driver.py"
        driver.write_text("""import builtins,json,os,subprocess,sys
from pathlib import Path
program,stream,root,defect=sys.argv[1:]
original_open,original_stat,original_read=builtins.open,os.stat,Path.read_text
mapping={
 '/var/log/tether/broker.log':'broker.log', '/var/log/tether/broker.err':'broker.err',
 '/home/sim/.tether/agent/lab/agent.log':'agent.log',
 '/home/sim/.tether/agent/lab/agent.boot.err':'agent.boot.err'}
def route(path):
 return str(Path(root)/mapping[str(path)]) if str(path) in mapping else path
def opened(path,*a,**kw):
 if defect=='unreadable' and str(path) in mapping: raise PermissionError('fixture unreadable')
 return original_open(route(path),*a,**kw)
def stated(path,*a,**kw):
 value=original_stat(route(path),*a,**kw)
 if defect=='rotated' and str(path) in mapping:
  fields=list(value); fields[1]+=1; return os.stat_result(fields)
 return value
def read(path,*a,**kw):
 if str(path)=='/etc/machine-id': return 'fixture-machine'
 if str(path)=='/proc/sys/kernel/random/boot_id': return 'fixture-boot'
 return original_read(path,*a,**kw)
calls=0
def journal(argv,**kw):
 global calls
 calls+=1
 if defect=='journal_unreadable': raise subprocess.CalledProcessError(1,argv)
 cursor='cursor-B' if defect=='journal_vacuum' and calls>1 else 'cursor-A'
 row=dict(__CURSOR=cursor,MESSAGE=[0,13,10,255])
 return subprocess.CompletedProcess(argv,0,stdout=(json.dumps(row)+'\\n').encode(),stderr=b'')
builtins.open,os.stat,Path.read_text,subprocess.run=opened,stated,read,journal
sys.argv=['snapshot',stream,'lab']
exec(compile(program,'logs-snapshot','exec'))
""")
        streams = dict(broker_slog=b"current\x00\xff\r\nlegacy\n",agent_slog=b"agent\x00\xff\r\n",
                       agent_panic=b"boot\r\n",broker_panic=b"\x00\r\n\xff\n")
        for name, data in (("broker.log",b"current\x00\xff\r\n"),("broker.err",b"legacy\n"),
                           ("agent.log",streams["agent_slog"]),("agent.boot.err",streams["agent_panic"])):
            (self.root/name).write_bytes(data)
        library = PROBE.with_name("logs.sh")
        script = '. "$1"; driver=$2; root=$3; defect=$4; stream=$5; '
        script += 'dexec() { _stream=$7; for _argument do _program=$_argument; done; python3 "$driver" "$_program" "$_stream" "$root" "$defect"; }; '
        script += 'sim_log_snapshot "$stream" isolated "$root/raw" "$root/meta"'
        for defect, stream in [("valid",k) for k in streams]+[("unreadable","agent_slog"),("rotated","agent_slog"),
                ("journal_unreadable","broker_panic"),("journal_vacuum","broker_panic")]:
            with self.subTest(defect=defect,stream=stream):
                (self.root/"meta").write_text('{"read_ok":true}')
                result = subprocess.run(["sh","-c",script,"test",str(library),str(driver),str(self.root),defect,stream],
                                        capture_output=True,text=True,timeout=5)
                if defect != "valid":
                    self.assertNotEqual(result.returncode,0,result.stderr)
                    self.assertFalse((self.root/"meta").exists())
                    continue
                self.assertEqual(result.returncode,0,result.stderr)
                self.assertEqual((self.root/"raw").read_bytes(),streams[stream])
                meta = json.loads((self.root/"meta").read_text())
                self.assertTrue(meta["read_ok"])
                self.assertEqual(meta["length"],len(streams[stream]))
                self.assertGreater(meta["inode"],0)
                if stream == "broker_panic":
                    self.assertEqual(meta["journal_cursors"],["cursor-A"])
                elif stream == "agent_slog":
                    self.assertEqual(meta["inode"],(self.root/"agent.log").stat().st_ino)

    # origin: hot-update S4 plan §12.1 / T48. The assembler must read the
    # four actual byte files, original waitpid, remote geometry and audit lines.
    def test_envelope_assembly_keeps_independent_runtime_receipts(self):
        assembly_fixture(self.root)
        result = self.invoke("trace", "--role", "assemble")
        self.assertEqual(result.returncode, 0, result.stderr)
        doc = json.loads(result.stdout)
        self.assertEqual(doc["ctl"]["wait_status"], 23 << 8)
        self.assertEqual(len(doc["local_resize"]), 2)
        self.assertNotIn("ack", doc["local_resize"][0])
        self.assertEqual(self.invoke("continuity").returncode, 0)

    # origin: hot-update S5 plan §7.1 / T48 / DRILL-RUN 36.A/W/C.
    # A marker-free S5 process already owns generation one. Bind that fact
    # to its running artifact, keeping exact +1 transitions and stale rejection.
    def test_fresh_flow_owner_generation_is_bound_to_running_artifact(self):
        for mode in ("image", "carrier", "wrong_sha", "wrong_pid", "malformed_sha", "missing_version", "stale_cut"):
            with self.subTest(mode=mode):
                root = self.root / mode
                records = carrier_fixture(root) if mode == "carrier" else None
                if records is None:
                    assembly_fixture(root)
                    records = [json.loads(line) for line in (root/"logs/agent_slog.log").read_text().splitlines()]
                before = SENSOR["read_json"](root/"agent/before.json")
                before["marker"] = None
                SENSOR["write_json"](root/"agent/before.json", before)
                (root/"endpoint-agent.version").write_text("tether fixture (proto v1)\nimage-domains exec-v1 pty-v1 flow-v1 ss-v1\nimage-abi v1\n")
                (root/"endpoint-agent.sha").write_text(("c" if mode == "wrong_sha" else "a")*64+"  /proc/"+("4001" if mode == "wrong_pid" else "4000")+"/exe\n")
                if mode == "malformed_sha":
                    (root/"endpoint-agent.sha").write_text("a"*64+"  /proc/4000/exe ignored\n")
                if mode == "missing_version": (root/"endpoint-agent.version").unlink()
                for record in records:
                    if record.get("msg") == "agent: pty image state": record["generation"] += 1
                    elif record.get("msg", "").startswith("agent: image activated"):
                        record["msg"] = record["msg"].replace("generation=1", "generation=2")
                for name in ("after", "settled"):
                    snapshot = SENSOR["read_json"](root/"agent"/(name+".json"))
                    if mode == "carrier": snapshot["marker"] = None
                    else: snapshot["marker"]["generation"] += 1
                    SENSOR["write_json"](root/"agent"/(name+".json"), snapshot)
                if mode == "stale_cut":
                    for record in records:
                        if record.get("msg") == "agent: pty image state": record["generation"] = 1
                save_runtime_log(root, records)
                result = subprocess.run([sys.executable, "-I", "-B", str(PROBE), "trace", "--role", "assemble", "--evidence", str(root), "--nonce", NONCE], capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0 if mode in ("image", "carrier") else 2, result.stderr)
                if mode in ("image", "carrier"):
                    doc = json.loads(result.stdout)
                    self.assertEqual(doc["images"][0]["generation"], 1)
                    judged = subprocess.run([sys.executable, "-I", "-B", str(PROBE), "continuity", "--evidence", str(root), "--nonce", NONCE], capture_output=True, text=True, timeout=5)
                    self.assertEqual(judged.returncode, 0, judged.stderr)
                    if mode == "image":
                        receipt = SENSOR["current_receipts"](root, NONCE, "queued")
                        self.assertEqual(sorted(receipt["pairs"]), [2])

    # origin: hot-update S4 plan §9.3 / S5 T48 / DRILL-RUN 39.P.
    # The legacy pin includes lost output; a few paced bytes may all drain
    # before exec. Establish real finite kernel traffic and a read barrier.
    def test_legacy_cut_has_declared_pending_output(self):
        source = (PROBE.parents[1]/"36-hot-update-pty.sh").read_text()
        body = source[source.index("_pty_upgrade_and_finish() {"):source.index("# 39.P imports")]
        log = self.root/"calls"
        script = r'''ARM=P; PTY_NEW_SHA=bbbb; PTY_EV=.; CALLS=$1
_pty_step_run() { "$@"; }
_pty_step_ok() { [ "$1" = 0 ]; }
assert_setup() { shift; "$@"; }
assert_ok() { shift; "$@"; }
_pty_checkpoint() { printf 'checkpoint %s\n' "$1" >> "$CALLS"; }
_pty_window() { printf 'window %s\n' "$1" >> "$CALLS"; }
_pty_upgrade() { printf 'upgrade\n' >> "$CALLS"; }
_pty_record_hop() { printf 'hop\n' >> "$CALLS"; }
poll_until() { shift 3; [ "$1" = -- ] && shift; "$@"; }
pty_fixture_finish() { printf 'finish\n' >> "$CALLS"; }
_pty_collect() { printf 'collect\n' >> "$CALLS"; }
'''
        result = subprocess.run(["sh", "-c", script+body+"\n_pty_upgrade_and_finish", "test", str(log)], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = log.read_text().splitlines()
        self.assertLess(calls.index("checkpoint queued"), calls.index("window start"))
        self.assertLess(calls.index("hop"), calls.index("checkpoint active-release"))
        self.assertLess(calls.index("checkpoint active-release"), calls.index("collect"))
        # Exercise the real remote writer on an original PTY. The bounded P
        # burst must cross the original 2 MiB pressure declaration; no source
        # bytes are invented when a partial syscall or HUP stops the writer.
        root = self.root/"pressure"; root.mkdir(); (root/"pause-input").touch()
        master, slave = os.openpty()
        def controlling_terminal():
            os.setsid(); fcntl.ioctl(0, termios.TIOCSCTTY, 0)
        child = subprocess.Popen([sys.executable, "-I", "-B", str(PROBE), "trace", "--role", "remote", "--scenario", "P", "--evidence", str(root), "--nonce", NONCE, "--period", ".001", "--timeout", "10"], stdin=slave, stdout=slave, stderr=subprocess.PIPE, preexec_fn=controlling_terminal)
        os.close(slave)
        received = b""; deadline = time.monotonic()+2
        try:
            while len(received) < 2*1024*1024 and time.monotonic() < deadline:
                if select.select([master], [], [], .01)[0]: received += os.read(master, 32768)
                if child.poll() is not None: break
            self.assertGreaterEqual(len(received), 2*1024*1024)
        finally:
            if child.poll() is None: child.send_signal(signal.SIGHUP)
            _, errors = child.communicate(timeout=5)
            os.close(master)
        self.assertEqual(child.returncode, 128+signal.SIGHUP, errors.decode())
        self.assertEqual(SENSOR["read_json"](root/"remote.trace.json")["hup"], 1)
        raw = (root/"remote.sent").read_bytes()
        self.assertEqual(raw[:len(received)], received)

    # origin: hot-update S4 plan §9.2 / T48; drill-run3 C2. C1 and C2
    # deliberately share a nonce; the started OS identity binds the task token.
    def test_management_join_selects_original_task_among_shared_nonce_rows(self):
        for defect in ("valid", "duplicate_token", "foreign_nonce", "wrong_token"):
            with self.subTest(defect=defect):
                root = self.root/defect
                assembly_fixture(root)
                ps = SENSOR["read_json"](root/"agent/settled.ps.json")
                ps["processes"].append(dict(pid="earlier-c1-token", argv=[NONCE], status="EXITED", exit_code=23))
                if defect == "duplicate_token": ps["processes"].append(copy.deepcopy(ps["processes"][0]))
                elif defect == "foreign_nonce": ps["processes"][0]["argv"] = ["another-nonce"]
                elif defect == "wrong_token": ps["processes"][0]["pid"] = "unrelated-token"
                SENSOR["write_json"](root/"agent/settled.ps.json", ps)
                result = subprocess.run([sys.executable, "-I", "-B", str(PROBE), "trace", "--role", "assemble",
                    "--evidence", str(root), "--nonce", NONCE], capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0 if defect == "valid" else 2, result.stderr)

    # origin: hot-update S4 plan §9.3 / T48. A different retained task's
    # nonce cannot replace the original legacy exporter's missing management.
    def test_legacy_management_absence_uses_original_task_token(self):
        fixture = Path(__file__).with_name("pty-nonvacuity-fixture.py")
        for defect in ("unrelated_token", "original_present"):
            with self.subTest(defect=defect):
                root = self.root/defect
                supplied = subprocess.run([sys.executable, "-I", "-B", str(fixture), str(root), NONCE, "legacy-loss"],
                    capture_output=True, text=True, timeout=5)
                self.assertEqual(supplied.returncode, 0, supplied.stderr)
                token = "task-token" if defect == "original_present" else "earlier-c1-token"
                SENSOR["write_json"](root/"agent/settled.ps.json", dict(processes=[dict(pid=token, argv=[NONCE])]))
                result = subprocess.run([sys.executable, "-I", "-B", str(PROBE), "legacy-exporter",
                    "--evidence", str(root), "--nonce", NONCE], capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 2 if defect == "original_present" else 1, result.stderr)

    # origin: hot-update S4 plan §9.2 / T48; drill-run3 W. These shapes
    # reproduce the archived empty default node list and redispatched marker.
    def test_watchdog_predicates_use_offline_selection_and_redispatched_marker(self):
        source = (PROBE.parents[1]/"36-hot-update-pty.sh").read_text()
        hop = source[source.index("_pty_hop_valid() {"):source.index("_pty_record_hop() {")]
        offline = source[source.index("_pty_node_offline() {"):source.index("_pty_blackhole() {")]
        sim = self.root/"sim-fixture"
        sim.write_text('#!/bin/sh\ncase " $* " in *" -a "*) cat "$NODE_ROWS" ;; *) printf \'%s\\n\' \'{"schema":"node_ls","schema_version":1,"nodes":[]}\' ;; esac\n')
        sim.chmod(0o700)
        for status in ("OFFLINE", "ONLINE", "STALE", "missing", "duplicate", "foreign"):
            rows = [dict(nid="agt1", status=status)]
            if status == "missing": rows = []
            elif status == "duplicate": rows = [dict(nid="agt1", status="OFFLINE")]*2
            elif status == "foreign": rows = [dict(nid="agt2", status="OFFLINE")]
            SENSOR["write_json"](self.root/"nodes.json", dict(schema="node_ls", schema_version=1, nodes=rows))
            script = 'PTY_EV=$1; SIM=$2; NODE_ROWS="$PTY_EV/nodes.json"; export NODE_ROWS; '+offline+'\n_pty_node_offline'
            result = subprocess.run(["sh", "-c", script, "test", str(self.root), str(sim)], capture_output=True, text=True)
            self.assertEqual(result.returncode == 0, status == "OFFLINE", (status, result.stderr))
        for state, phase, valid in (("pending", "active", True), ("committed", "active", True),
                ("rolled_back", "redispatched", True), ("rolled_back", "active", False),
                ("pending", "redispatched", False), ("rolled_back", "lost", False)):
            row = dict(pid=1219, start_ticks=125821450, running_sha="a"*64, disk_sha="a"*64,
                marker=dict(state=state, target_sid="lab", target_nid="agt1", boot_count=1, generation=2, resource_phase=phase))
            SENSOR["write_json"](self.root/"hop.json", row)
            result = subprocess.run(["sh", "-c", 'PTY_PID=1219; PTY_TICKS=125821450; '+hop+
                '\n_pty_hop_valid "$1" "$2" "$3"', "test", "a"*64, state, str(self.root/"hop.json")], capture_output=True, text=True)
            self.assertEqual(result.returncode == 0, valid, (state, phase, result.stderr))

    # origin: hot-update S4 plan §9.2 / T48; drill-run3 L. The old
    # reconciler cannot provision the current fixture's ping_interval/ping_max.
    def test_old_broker_quadrant_uses_current_provisioning_before_binary_swap(self):
        source = (PROBE.parents[1]/"36-hot-update-pty.sh").read_text()
        quadrant = source[source.index("_pty_legacy_quadrants() {"):source.index("_pty_finish() {")]
        sim = self.root/"sim-fixture"
        sim.write_text('''#!/bin/sh
case "$1" in
  nuke) rm -f "$FIXTURE_ROOT/old-broker-installed" ;;
  init) if [ -e "$FIXTURE_ROOT/old-broker-installed" ]; then
    printf '%s\n' 'natsconf: ping_interval, ping_max: refusing takeover' >&2; exit 1
  fi ;;
esac
''')
        sim.chmod(0o700)
        script = '''PTY_EV=$1; FIXTURE_ROOT=$1; export FIXTURE_ROOT; SIM=$2; HERE=$3; SID=lab; PIN=135790;
PTY_NONCE=abcdef0123456789abcdef0123456789; INSTANCE=hermetic;
sctl() { :; }; ctr_name() { printf '%s' "$1"; }; dexec() { :; };
d() { case "$*" in *brk1:/usr/local/bin/tether*) touch "$FIXTURE_ROOT/old-broker-installed" ;; esac; };
_pty_install_previous() { :; }; _pty_broker_ready() { :; }; _pty_agent_ready() { :; };
pty_fixture_start() { printf '%s\n' "$PTY_FIXTURE_SCENARIO" >> "$FIXTURE_ROOT/quadrants"; };
poll_until() { :; }; _pty_checkpoint() { :; }; _pty_window() { :; }; _pty_finish() { :; };
upgradecfg_allow_broker() { :; }; _pty_previous_supply() { :; }; artifact_up() { :; };
ingress_trust_inject() { :; }; upgradecfg_allow_agent() { :; };
'''+quadrant+'\n_pty_legacy_quadrants'
        result = subprocess.run(["sh", "-c", script, "test", str(self.root), str(sim), str(PROBE.parents[2])], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.root/"quadrants").read_text().splitlines(), ["L-old-agent", "L-old-broker"])

    # origin: hot-update S4 plan §9.3 / T48; drill-run3 P. Terminal loss
    # can deliver another HUP while the first HUP's finally publishes receipts.
    # A second real signal must not leave the last periodic trace over grown bytes.
    def test_remote_hangup_finalization_preserves_complete_byte_receipts(self):
        wrapper = '''import json,os,runpy,signal,sys
from pathlib import Path
from types import SimpleNamespace
sensor=runpy.run_path(sys.argv[1]); root=Path(sys.argv[2]); describe=sensor["describe"]
fired=False
def interrupt(path,nonce,arrivals):
    global fired
    if (root/"inject-hup").exists() and not fired:
        fired=True
        os.kill(os.getpid(),signal.SIGHUP)
    return describe(path,nonce,arrivals)
remote=sensor["trace_remote"]; remote.__globals__["describe"]=interrupt
sys.exit(remote(SimpleNamespace(evidence=str(root),nonce=sys.argv[3],scenario="P",period=.01,timeout=10)))
'''
        for round_number in range(5):
            root = self.root/str(round_number)
            root.mkdir()
            master, slave = os.openpty()
            def ctty():
                os.setsid()
                fcntl.ioctl(0, termios.TIOCSCTTY, 0)
            proc = subprocess.Popen([sys.executable, "-I", "-B", "-c", wrapper, str(PROBE), str(root), NONCE],
                stdin=slave, stdout=slave, stderr=subprocess.PIPE, preexec_fn=ctty)
            os.close(slave)
            try:
                deadline = time.monotonic()+5
                while not (root/"remote.trace.json").exists():
                    self.assertLess(time.monotonic(), deadline)
                    if select.select([master], [], [], .01)[0]: os.read(master, 32768)
                before = SENSOR["read_json"](root/"remote.trace.json")
                while (root/"remote.sent").stat().st_size <= before["output"]["length"]:
                    self.assertLess(time.monotonic(), deadline)
                    if select.select([master], [], [], .01)[0]: os.read(master, 32768)
                (root/"inject-hup").touch()
                os.kill(proc.pid, signal.SIGHUP)
                _, stderr = proc.communicate(timeout=5)
                self.assertEqual(proc.returncode, 129, stderr.decode())
                trace = SENSOR["read_json"](root/"remote.trace.json")
                self.assertEqual(trace["hup"], 1)
                for name in ("input", "output"):
                    SENSOR["blob_bytes"](root, trace[name])
                self.assertGreater(trace["output"]["length"], before["output"]["length"])
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=5)
                os.close(master)

    # origin: hot-update S4 plan §9.3 / T48; drill-run3 P. Closing the
    # sole master exercises the actual kernel's terminal loss and leader HUP,
    # independently of the deterministic second-signal test above.
    def test_remote_master_loss_finalizes_grown_raw_files(self):
        import ctypes
        libc = ctypes.CDLL(None)
        self.assertEqual(libc.prctl(36, 1, 0, 0, 0), 0)
        try:
            for round_number in range(5):
                root = self.root/str(round_number)
                root.mkdir()
                leader, master = os.forkpty()
                if leader == 0:
                    os.execv(sys.executable, [sys.executable, "-I", "-B", str(PROBE), "trace", "--role", "leader",
                        "--scenario", "P", "--evidence", str(root), "--nonce", NONCE, "--period", ".01", "--timeout", "10"])
                foreground = None
                try:
                    deadline = time.monotonic()+5
                    while not (root/"remote.trace.json").exists():
                        self.assertLess(time.monotonic(), deadline)
                        if select.select([master], [], [], .01)[0]: os.read(master, 32768)
                    foreground = SENSOR["read_json"](root/"remote.identity.json")["identity"]["pid"]
                    before = SENSOR["read_json"](root/"remote.trace.json")
                    while (root/"remote.sent").stat().st_size <= before["output"]["length"]:
                        self.assertLess(time.monotonic(), deadline)
                        if select.select([master], [], [], .01)[0]: os.read(master, 32768)
                    os.close(master)
                    master = None
                    os.waitpid(leader, 0)
                    leader = None
                    try:
                        os.waitpid(foreground, 0)
                    except ChildProcessError:
                        pass  # The original leader may have reaped it before HUP.
                    foreground = None
                    trace = SENSOR["read_json"](root/"remote.trace.json")
                    self.assertFalse(trace["normal_stop"])
                    for name in ("input", "output"):
                        SENSOR["blob_bytes"](root, trace[name])
                    self.assertGreater(trace["output"]["length"], before["output"]["length"])
                finally:
                    if master is not None: os.close(master)
                    for pid in (leader, foreground):
                        if pid is not None:
                            try: os.kill(pid, signal.SIGKILL)
                            except ProcessLookupError: pass
                            try: os.waitpid(pid, 0)
                            except ChildProcessError: pass
        finally:
            self.assertEqual(libc.prctl(36, 0, 0, 0, 0), 0)

    # origin: hot-update S4 plan §9.3 / T48. The first HUP can arrive
    # after a raw ledger write but before its timestamp/count bookkeeping.
    # Final publication alone cannot recover those missing observations.
    def test_remote_hangup_cannot_split_byte_receipt_bookkeeping(self):
        wrapper = '''import os,runpy,signal,sys
from pathlib import Path
from types import SimpleNamespace
sensor=runpy.run_path(sys.argv[1]); root=Path(sys.argv[2]); original=Path.open
fired=False
class Sink:
    def __init__(self,stream): self.stream=stream
    def __enter__(self): self.stream.__enter__(); return self
    def __exit__(self,*args): return self.stream.__exit__(*args)
    def __getattr__(self,name): return getattr(self.stream,name)
    def write(self,data):
        global fired
        count=self.stream.write(data)
        if (root/"inject-hup").exists() and not fired:
            fired=True
            os.kill(os.getpid(),signal.SIGHUP)
        return count
def opened(path,*args,**kwargs):
    stream=original(path,*args,**kwargs)
    return Sink(stream) if path.name==sys.argv[4] and args[:1]==("xb",) else stream
Path.open=opened
sys.exit(sensor["trace_remote"](SimpleNamespace(evidence=str(root),nonce=sys.argv[3],scenario="P",period=.01,timeout=10)))
'''
        for target in ("remote.sent", "remote.received"):
            for round_number in range(5):
                with self.subTest(target=target, round=round_number):
                    root = self.root/(target+str(round_number))
                    root.mkdir()
                    master, slave = os.openpty()
                    def ctty():
                        os.setsid()
                        fcntl.ioctl(0, termios.TIOCSCTTY, 0)
                    proc = subprocess.Popen([sys.executable, "-I", "-B", "-c", wrapper, str(PROBE), str(root), NONCE, target],
                        stdin=slave, stdout=slave, stderr=subprocess.PIPE, preexec_fn=ctty)
                    os.close(slave)
                    try:
                        deadline = time.monotonic()+5
                        while not (root/"remote.trace.json").exists():
                            self.assertLess(time.monotonic(), deadline)
                            if select.select([master], [], [], .01)[0]: os.read(master, 32768)
                        (root/"inject-hup").touch()
                        if target == "remote.received":
                            packet = SENSOR["frame"](NONCE, 0, SENSOR["payload"](0))
                            self.assertEqual(os.write(master, packet), len(packet))
                        _, stderr = proc.communicate(timeout=5)
                        self.assertEqual(proc.returncode, 129, stderr.decode())
                        trace = SENSOR["read_json"](root/"remote.trace.json")
                        self.assertEqual(trace["hup"], 1)
                        for name in ("input", "output"):
                            SENSOR["blob_bytes"](root, trace[name])
                        self.assertGreater(trace["input" if target == "remote.received" else "output"]["length"], 0)
                    finally:
                        if proc.poll() is None:
                            proc.kill()
                            proc.wait(timeout=5)
                        os.close(master)

    # origin: hot-update S4 plan §12.1 / T48. Byte lengths and local ioctl
    # cannot be substituted for unobserved reliable ACKs/control geometry.
    def test_envelope_assembly_rejects_missing_or_substituted_receipts(self):
        for defect in ("missing_runtime", "foreign_runtime", "foreign_ctl", "missing_ack", "missing_control", "local_ack", "remote_geometry", "source_ledger", "wrong_waitpid", "missing_exit_audit", "missing_leader", "missing_log_cursor"):
            with self.subTest(defect=defect):
                root = self.root/defect
                root.mkdir()
                assembly_fixture(root)
                records = [json.loads(line) for line in (root/"logs/agent_slog.log").read_text().splitlines()]
                if defect == "missing_runtime":
                    records = [r for r in records if r["msg"] != "agent: pty image state"]
                elif defect == "foreign_runtime":
                    records[0]["os_pid"] += 1
                elif defect == "foreign_ctl":
                    trace = SENSOR["read_json"](root/"ctl.identity.json")
                    trace["nonce"] = "0"*32
                    SENSOR["write_json"](root/"ctl.identity.json", trace)
                elif defect == "missing_ack":
                    del next(r for r in records if r["msg"] == "agent: run reliable=true exited")["input_accepted"]
                elif defect == "missing_control":
                    records.pop(1)
                elif defect == "local_ack":
                    local = SENSOR["read_json"](root/"checkpoints/0.json")
                    local["local_resize"]["cols"] = 1
                    SENSOR["write_json"](root/"checkpoints/0.json", local)
                elif defect == "remote_geometry":
                    remote = SENSOR["read_json"](root/"remote.trace.json")
                    for row in remote["samples"]:
                        row["cols"] = 1
                    SENSOR["write_json"](root/"remote.trace.json", remote)
                elif defect == "source_ledger":
                    trace = SENSOR["read_json"](root/"ctl.trace.json")
                    trace["output"] = SENSOR["read_json"](root/"remote.trace.json")["output"]
                    SENSOR["write_json"](root/"ctl.trace.json", trace)
                elif defect == "wrong_waitpid":
                    wait = SENSOR["read_json"](root/"ctl.wait.json")
                    wait["pid"] += 1
                    SENSOR["write_json"](root/"ctl.wait.json", wait)
                elif defect == "missing_exit_audit":
                    records = [r for r in records if r["msg"] != "agent: run reliable=true exited"]
                elif defect == "missing_leader":
                    trace = SENSOR["read_json"](root/"remote.trace.json")
                    trace["samples"] = [r for r in trace["samples"] if r["role"] != "leader"]
                    SENSOR["write_json"](root/"remote.trace.json",trace)
                save_runtime_log(root, records)
                if defect == "missing_log_cursor":
                    logs = SENSOR["read_json"](root/"logs.json")
                    logs["cursors"] = [c for c in logs["cursors"] if c["stream"] != "agent_slog"]
                    SENSOR["write_json"](root/"logs.json",logs)
                SENSOR["write_json"](root/"evidence.json", dict(oracle=dict(valid=True)))
                result = subprocess.run([sys.executable,"-I","-B",str(PROBE),"trace","--role","assemble",
                    "--evidence",str(root),"--nonce",NONCE],capture_output=True,text=True,timeout=5)
                self.assertEqual(result.returncode, 2, (defect,result.stdout,result.stderr))
                self.assertFalse((root/"evidence.json").exists(), "stale successful envelope survived failed assembly")

    # origin: hot-update S4 plan §9.1 / T48; P18. The current owner is the
    # source at activation, and a fresh marker cannot excuse a stale record.
    def test_runtime_slog_sources_reject_absent_stale_and_initial_bundle(self):
        for defect in ("json", "text", "text_rounded", "absent", "stale_generation", "initial_bundle", "renewed_ka", "foreign_child"):
            with self.subTest(defect=defect):
                root = self.root/defect
                assembly_fixture(root)
                records = [json.loads(line) for line in (root/"logs/agent_slog.log").read_text().splitlines()]
                if defect == "text_rounded":
                    records[1]["time"] = datetime.datetime.fromtimestamp(1700000001.9995,datetime.timezone.utc).isoformat()
                if defect == "absent": records.pop(3)
                elif defect == "stale_generation": records[2]["generation"] = records[3]["generation"] = 0
                elif defect == "initial_bundle": records[3]["input_applied"] = 99
                elif defect == "renewed_ka": records[3]["ka_deadline_boot_ns"] += 1
                elif defect == "foreign_child": records[0]["start_time_ticks"] += 1
                save_runtime_log(root, records, text=defect in ("text","text_rounded"))
                result = subprocess.run([sys.executable,"-I","-B",str(PROBE),"trace","--role","assemble",
                    "--evidence",str(root),"--nonce",NONCE],capture_output=True,text=True,timeout=5)
                self.assertEqual(result.returncode,0 if defect in ("json","text","text_rounded") else 2, result.stderr)
                if defect in ("json","text","text_rounded"):
                    doc = json.loads(result.stdout)
                    self.assertEqual(doc["runtime_states"][1]["input_applied"],150)
                    self.assertEqual(doc["controls"][1]["seq"],4)
                    self.assertNotIn(NONCE,doc["audit_binding"]["request_id_hash"])

    # origin: hot-update S4 plan §9.2 / C1 / T48. Passive terminal telemetry
    # alone cannot prove live carrier checkpoints or a retained KA deadline.
    def test_carrier_missing_live_state_is_exposed_without_offset_inference(self):
        assembly_fixture(self.root)
        records = [json.loads(line) for line in (self.root/"logs/agent_slog.log").read_text().splitlines()]
        records = [r for r in records if r["msg"] != "agent: pty image state"]
        save_runtime_log(self.root, records)
        result = self.invoke("trace", "--role", "assemble")
        self.assertEqual(result.returncode,2,result.stdout+result.stderr)
        self.assertIn("no passive carrier offset checkpoints",result.stderr)
        self.assertFalse((self.root/"evidence.json").exists())

    # origin: hot-update S4 plan §6.1 / §9.2 / C1 / T48; P18 round 2.
    # Complete logs must bind the established carrier pair to this fault, not
    # to an image generation, another task or another reconnect attempt.
    def test_carrier_records_bind_original_generation_and_absolute_ka(self):
        defects = ("json", "text", "repeat", "missing_lost", "missing_restored", "foreign_pid",
                   "generation", "orphan_restored", "duplicate", "reverse", "outside_window", "renewed_ka",
                   "new_ka", "stale_bytes", "lost_fin", "missing_deadline")
        for defect in defects:
            with self.subTest(defect=defect):
                root = self.root/defect
                records = carrier_fixture(root)
                if defect == "repeat":
                    # A second successful rebuild in the same generation uses
                    # another pair, while the tested C1 window still binds one.
                    extra = [dict(records[2],time=records[1]["time"]),dict(records[2],phase="carrier-restored",
                        time=datetime.datetime.fromtimestamp(1700000002.1,datetime.timezone.utc).isoformat())]
                    records[2:2] = extra
                elif defect == "missing_lost": records.pop(2)
                elif defect == "missing_restored": records.pop(3)
                elif defect == "foreign_pid": records[3]["pid"] = "another-task"
                elif defect == "generation": records[3]["generation"] += 1
                elif defect == "orphan_restored": records[2]["phase"] = "carrier-restored"
                elif defect == "duplicate": records.insert(3,dict(records[2]))
                elif defect == "reverse": records[3]["time"] = records[1]["time"]
                elif defect == "outside_window": records[2]["time"] = records[1]["time"]
                elif defect == "renewed_ka": records[3]["ka_deadline_boot_ns"] += 1
                elif defect == "new_ka": records[3]["ka_seq"] += 1
                elif defect == "stale_bytes": records[3]["input_applied"] = 99
                elif defect == "lost_fin": records[2]["input_fin"] = True
                elif defect == "missing_deadline": del records[2]["ka_deadline_boot_ns"]
                save_runtime_log(root,records,text=defect == "text")
                result = subprocess.run([sys.executable,"-I","-B",str(PROBE),"trace","--role","assemble",
                    "--evidence",str(root),"--nonce",NONCE],capture_output=True,text=True,timeout=5)
                valid = defect in ("json","text","repeat")
                self.assertEqual(result.returncode,0 if valid else 2,result.stdout+result.stderr)
                if valid:
                    doc = json.loads(result.stdout)
                    self.assertEqual(len(doc["images"]),1)
                    self.assertEqual(len(doc["carrier_pairs"]),2 if defect == "repeat" else 1)
                    self.assertEqual(doc["keepalive"]["deadline_boot_ns"],555)
                    judged = subprocess.run([sys.executable,"-I","-B",str(PROBE),"continuity",
                        "--evidence",str(root),"--nonce",NONCE],capture_output=True,text=True,timeout=5)
                    self.assertEqual(judged.returncode,0,judged.stdout+judged.stderr)
                else:
                    self.assertFalse((root/"evidence.json").exists())

    # origin: hot-update S4 plan §6.1 / §9.2 / C1 / T48. The readiness
    # barrier must see successful restoration before ending the fault window.
    def test_carrier_recovery_waits_for_original_successor_receipt(self):
        drill = (PROBE.parents[1]/"36-hot-update-pty.sh").read_text()
        function = drill[drill.index("_pty_rebuild_seen() {"):drill.index("_pty_node_offline() {")]
        for defect in ("valid", "start_only", "foreign", "collection_failed", "snapshot_failed"):
            with self.subTest(defect=defect):
                root = self.root/defect
                records = carrier_fixture(root)
                if defect == "start_only": records.pop(3)
                elif defect == "foreign": records[3]["pid"] = "another-task"
                save_runtime_log(root,records)
                collect = 2 if defect == "collection_failed" else 0
                snapshot = 2 if defect == "snapshot_failed" else 0
                script = """set -u
PROBE=$1
PTY_EV=$2
PTY_NONCE=$3
PTY_CURSOR=0
sim_agent_slog_count() { printf '1\n'; }
pty_fixture_collect() { return %s; }
pty_collect_logs() { return %s; }
""" % (collect,snapshot)
                result = subprocess.run(["sh","-c",script+function+"\n_pty_rebuild_seen", "carrier-test",
                    str(PROBE),str(root),NONCE],capture_output=True,text=True,timeout=5)
                self.assertEqual(result.returncode,0 if defect == "valid" else 2,result.stdout+result.stderr)

    # origin: hot-update S4 plan §6.1 / §9.1 / T48; P18 round 2.
    # Rebuilds can repeat inside an image generation; W's current-state
    # selection must still read the image cut, not the later carrier state.
    def test_carrier_and_image_records_keep_independent_pairing(self):
        for defect in ("json", "text", "exec_reset_loss", "unmatched_loss"):
            with self.subTest(defect=defect):
                root = self.root/defect
                assembly_fixture(root)
                records = [json.loads(line) for line in (root/"logs/agent_slog.log").read_text().splitlines()]
                def stamp(at):
                    return datetime.datetime.fromtimestamp(1700000000+at,datetime.timezone.utc).isoformat()
                extra = [dict(records[3],phase=phase,time=stamp(at)) for phase,at in
                         (("carrier-lost",3.2),("carrier-restored",3.3),("carrier-lost",3.4),("carrier-restored",3.5))]
                if defect == "unmatched_loss": extra.pop()
                records[5:5] = extra
                if defect == "exec_reset_loss":
                    # The old runtime's transport flag does not survive exec;
                    # the next established runtime begins its own pair.
                    records.insert(2,dict(records[2],phase="carrier-lost",generation=0,time=stamp(2.4)))
                save_runtime_log(root,records,text=defect == "text")
                result = subprocess.run([sys.executable,"-I","-B",str(PROBE),"trace","--role","assemble",
                    "--evidence",str(root),"--nonce",NONCE],capture_output=True,text=True,timeout=5)
                self.assertEqual(result.returncode,2 if defect == "unmatched_loss" else 0,result.stdout+result.stderr)
                if defect != "unmatched_loss":
                    doc = json.loads(result.stdout)
                    self.assertEqual(len(doc["images"]),2)
                    self.assertEqual(len(doc["carrier_pairs"]),2)
                    current = SENSOR["current_receipts"](root,NONCE,"queued")
                    self.assertEqual(set(current["pairs"]),{1})
                    self.assertEqual(SENSOR["read_json"](root/"queued.json")["input"]["applied"],100)

    # origin: hot-update S4 plan §3 / §9.1 / T48; P18 round 2.
    # Exit Base deliberately trails End. The successful final equality must
    # come from the later ACK commit, never a byte count or that exit snapshot.
    def test_terminal_settlement_is_required_and_proves_acknowledged_end(self):
        for defect in ("json", "text", "absent", "duplicate", "foreign", "base", "end", "input", "exit", "early", "missing_base", "missing_ack"):
            with self.subTest(defect=defect):
                root = self.root/defect
                assembly_fixture(root)
                records = [json.loads(line) for line in (root/"logs/agent_slog.log").read_text().splitlines()]
                if defect == "absent": records.pop()
                elif defect == "duplicate": records.append(dict(records[-1]))
                elif defect == "foreign": records[-1]["pid"] = "another-task"
                elif defect == "base": records[-1]["output_base"] -= 1
                elif defect == "end": records[-1]["output_end"] += 1
                elif defect == "input": records[-1]["input_applied"] -= 1
                elif defect == "exit": records[-1]["code"] += 1
                elif defect == "early": records[-1]["time"] = records[3]["time"]
                elif defect == "missing_base": del records[-1]["output_base"]
                elif defect == "missing_ack": del records[-1]["input_accepted"]
                save_runtime_log(root,records,text=defect == "text")
                result = subprocess.run([sys.executable,"-I","-B",str(PROBE),"trace","--role","assemble",
                    "--evidence",str(root),"--nonce",NONCE],capture_output=True,text=True,timeout=5)
                valid = defect in ("json","text")
                self.assertEqual(result.returncode,0 if valid else 2,result.stdout+result.stderr)
                if valid:
                    doc = json.loads(result.stdout)
                    self.assertLess(doc["terminal_receipt"]["output_base"],doc["terminal_receipt"]["output_end"])
                    self.assertEqual(doc["output"]["base"],doc["settlement_receipt"]["output_base"])
                    self.assertEqual(doc["output"]["base"],doc["output"]["end"])
                else:
                    self.assertFalse((root/"evidence.json").exists())

    # origin: hot-update S4 plan §9.2 / C2 / T48. Kernel-effect receipts
    # cannot substitute for FIN telemetry, or for another foreground's effects.
    def test_canonical_fin_envelope_rejects_missing_or_foreign_effects(self):
        for defect in ("valid", "keys", "eof", "sigint", "fin", "tail", "foreign"):
            with self.subTest(defect=defect):
                doc = complete_fixture(self.root)
                doc["scenario"] = "C2"
                (self.root/"canonical.sent").write_bytes(b"\x04\x03")
                actions = dict(nonce=NONCE, sent=dict(file="canonical.sent",length=2,sha256=SENSOR["sha"](b"\x04\x03")),
                    remote=dict(nonce=NONCE,identity=process(4200),eof=True,sigint=1),
                    local_fin=dict(nonce=NONCE),remote_fin=dict(nonce=NONCE,tail_s=.3))
                doc["canonical_actions"] = actions
                doc["input"]["applied"] += 2
                doc["input"]["accepted"] += 2
                doc["terminal_receipt"] = dict(input_fin=True,input_ended=True,output_fin=True)
                if defect == "keys":
                    (self.root/"canonical.sent").write_bytes(b"\x03\x04")
                    actions["sent"]["sha256"] = SENSOR["sha"](b"\x03\x04")
                elif defect == "eof": actions["remote"]["eof"] = False
                elif defect == "sigint": actions["remote"]["sigint"] = 0
                elif defect == "fin": doc["terminal_receipt"]["input_fin"] = False
                elif defect == "tail": actions["remote_fin"]["tail_s"] = .1
                elif defect == "foreign": actions["remote"]["identity"]["pid"] += 1
                self.save(doc)
                result = self.invoke("continuity")
                self.assertEqual(result.returncode,0 if defect == "valid" else 2 if defect == "foreign" else 1,result.stdout+result.stderr)

    # origin: hot-update S4 plan §9.2 / 36.L / T48. Fallback quadrants
    # prove compatibility only; the hot-update oracle still rejects no fault.
    def test_fallback_quadrant_retains_old_endpoint_and_selected_legacy_path(self):
        for defect in ("valid", "new_endpoints", "missing_version", "no_legacy_start", "hot_update_claim"):
            with self.subTest(defect=defect):
                root = self.root/defect
                doc = assembly_fixture(root)
                records = [json.loads(line) for line in (root/"logs/agent_slog.log").read_text().splitlines()]
                records = [r for r in records if r["msg"] not in ("agent: pty image state", "agent: pty control committed", "agent: run reliable=true settled")]
                records[0].update(msg="agent: run",argv=[NONCE])
                if defect == "no_legacy_start": records[0]["argv"] = ["another-nonce"]
                records[-1].update(msg="agent: run exit")
                save_runtime_log(root, records)
                for role in ("ctl","agent","broker"):
                    version = "tether fixture (proto v1)\nlinux/amd64 go-fixture\nimage-abi v1\n"
                    if defect == "new_endpoints" or role != "agent":
                        version = version.replace("image-abi", "image-domains exec-v1 pty-v1\nimage-abi")
                    (root/("endpoint-"+role+".version")).write_text(version)
                    (root/("endpoint-"+role+".sha")).write_text("d"*64+"  /private-fixture\n")
                if defect == "missing_version": (root/"endpoint-agent.version").unlink()
                windows = SENSOR["read_json"](root/"windows.json")
                windows["windows"][0]["kind"] = "compatibility"
                SENSOR["write_json"](root/"windows.json",windows)
                result = subprocess.run([sys.executable,"-I","-B",str(PROBE),"trace","--role","assemble",
                    "--evidence",str(root),"--nonce",NONCE],capture_output=True,text=True,timeout=5)
                self.assertEqual(result.returncode,0 if defect in ("valid","hot_update_claim") else 2,result.stderr)
                if result.returncode == 0:
                    # Reliable replay ownership is absent in this old endpoint.
                    envelope = SENSOR["read_json"](root/"evidence.json")
                    for snapshot in [envelope["resources"]["before"], *envelope["resources"]["after"]]:
                        snapshot["counts"]["replay"] = 0
                    SENSOR["write_json"](root/"evidence.json",envelope)
                    command = ["continuity"] if defect == "hot_update_claim" else ["trace","--role","compatibility"]
                    judgment = subprocess.run([sys.executable,"-I","-B",str(PROBE),*command,
                        "--evidence",str(root),"--nonce",NONCE],capture_output=True,text=True,timeout=5)
                    self.assertEqual(judgment.returncode,2 if defect == "hot_update_claim" else 0,judgment.stdout+judgment.stderr)

    # origin: hot-update S4 plan §9.2 / 36.W. Pending/active/rollback are
    # current passive receipts, not final envelopes with invented waitpid/logs.
    def test_watchdog_current_state_rejects_old_bundle_missing_fields_and_new_ka(self):
        drill=PROBE.parents[1]/"36-hot-update-pty.sh"
        text=drill.read_text()
        function=text[text.index("_pty_current_state() {"):text.index("_pty_active_progress() {")]
        for defect in ("valid","old_bundle","missing_state","new_ka","stale_ack","foreign_ctl","foreign_nonce","empty_output"):
            with self.subTest(defect=defect):
                root=self.root/defect
                root.mkdir()
                receipts=[]
                for name,applied,end in (("queued",10,30),("active",15,40),("rollback",15,40)):
                    row=dict(version=1,nonce=NONCE,protocol="reliable-pty-v1",ctl_identity=process(4300),
                        generation=1 if name=="queued" else 2,phase="activated" if name=="rollback" else "cut",
                        input=dict(applied=applied,accepted=20),output=dict(base=20,end=end),
                        controls=[dict(op="keepalive",seq=3,ack=3,deadline=555)])
                    receipts.append((name,row))
                rollback=receipts[-1][1]
                if defect=="old_bundle": rollback["input"]["applied"]=10; rollback["output"]["end"]=30
                elif defect=="missing_state": del rollback["input"]["accepted"]
                elif defect=="new_ka": rollback["controls"][0]["deadline"]+=1
                elif defect=="stale_ack": rollback["controls"][0]["ack"]-=1
                elif defect=="foreign_ctl": rollback["ctl_identity"]["pid"]+=1
                elif defect=="foreign_nonce": rollback["nonce"]="0"*32
                elif defect=="empty_output": receipts[0][1]["output"]["end"]=20
                for name,row in receipts:
                    SENSOR["write_json"](root/(name+".json"),row)
                result=subprocess.run(["sh","-c",'PTY_EV=$1; '+function+'\n_pty_current_state',"test",str(root)],
                                      capture_output=True,text=True,timeout=5)
                self.assertEqual(result.returncode==0,defect=="valid",(defect,result.stderr))
                if defect!="valid":
                    self.assertIn("PTY current state failed",result.stderr)

    # origin: hot-update S4 plan §9.2 / T48; drill-run4 W. A fast local
    # consumer otherwise ACKs all output before the first image cut.
    def test_watchdog_output_backpressure_keeps_original_duplex_bytes(self):
        runner = [sys.executable,"-I","-B",str(PROBE),"trace","--role","ctl","--scenario","W",
                  "--evidence",str(self.root),"--nonce",NONCE,"--ctl-binary",sys.executable,
                  "--period",".01","--timeout","20","--","-I","-B",str(Path(__file__).resolve()),
                  "--proxy",str(self.root),"W"]
        child = subprocess.Popen(runner,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        def wait_for(predicate):
            deadline=time.monotonic()+5
            while not predicate():
                self.assertIsNone(child.poll(),str(child.communicate() if child.poll() is not None else "early exit"))
                self.assertLess(time.monotonic(),deadline,"output backpressure observation timed out")
                time.sleep(.01)
        try:
            wait_for(lambda:(self.root/"ctl.received").exists() and (self.root/"ctl.received").stat().st_size>5000)
            (self.root/"pause-output").touch()
            wait_for(lambda:(self.root/"output-paused.json").exists())
            paused=SENSOR["read_json"](self.root/"output-paused.json")
            before=(self.root/"ctl.received").stat().st_size
            (self.root/"pause-input").touch()
            wait_for(lambda:(self.root/"remote.sent").stat().st_size>before+10000)
            self.assertEqual((self.root/"ctl.received").stat().st_size,before)
            self.assertEqual(paused["nonce"],NONCE)
            self.assertEqual(paused["received"],before)
            (self.root/"pause-output").unlink()
            wait_for(lambda:(self.root/"ctl.received").stat().st_size>before+256*1024)
            # Eight bounded large frames create backlog; the periodic stream
            # must then return to its normal size while input remains paused.
            rows=SENSOR["parse_frames"]((self.root/"ctl.received").read_bytes(),NONCE,complete=False)
            first_large=next(r["id"] for r in rows if r["length"]>32768)
            wait_for(lambda:SENSOR["parse_frames"]((self.root/"ctl.received").read_bytes(),NONCE,complete=False)[-1]["id"]>=first_large+12)
            rows=SENSOR["parse_frames"]((self.root/"ctl.received").read_bytes(),NONCE,complete=False)
            self.assertEqual(sum(r["length"]>32768 for r in rows),8)
            (self.root/"pause-input").unlink()
            (self.root/"finish").touch()
            out,err=child.communicate(timeout=5)
            self.assertEqual(child.returncode,0,(out,err))
            self.assertEqual((self.root/"remote.sent").read_bytes(),(self.root/"ctl.received").read_bytes())
            self.assertEqual((self.root/"ctl.sent").read_bytes(),(self.root/"remote.received").read_bytes())
            self.assertEqual(SENSOR["read_json"](self.root/"ctl.wait.json")["wait_status"],23<<8)
        finally:
            (self.root/"pause-output").unlink(missing_ok=True)
            (self.root/"pause-input").unlink(missing_ok=True)
            (self.root/"finish").touch()
            try: child.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill(); child.communicate()

    # origin: hot-update S4 plan §9.2 / T48; drill-run4 W. Setup waits
    # must observe the original barrier and a measured backlog before cut.
    def test_watchdog_queued_setup_requires_original_barrier_and_output_backlog(self):
        source=PROBE.with_name("hot_update_pty.sh").read_text()
        segment=source[source.index('    if [ "$_pty_checkpoint_name" = queued ]; then'):
                       source.index('    elif [ "$_pty_checkpoint_name" = active-release ]; then')]
        scripts=re.findall(r"python3 -I -B -c '\n(.*?)\n' ",segment,re.S)
        self.assertEqual(len(scripts),2)
        # Shorten only these acquisition polling budgets in the hermetic
        # fixture; production still uses its real five/ten-second deadlines.
        barrier,backlog=[code.replace("monotonic()+5","monotonic()+.1").replace("monotonic()+10","monotonic()+.1") for code in scripts]
        for defect in ("valid","missing_barrier","foreign_barrier","empty_backlog"):
            with self.subTest(defect=defect):
                root=self.root/defect; root.mkdir()
                if defect!="missing_barrier":
                    SENSOR["write_json"](root/"output-paused.json",dict(nonce="0"*32 if defect=="foreign_barrier" else NONCE,received=123))
                (root/"remote.sent").write_bytes(bytes(123 if defect=="empty_backlog" else 123+128*1024))
                first=subprocess.run([sys.executable,"-I","-B","-c",barrier,str(root),NONCE],capture_output=True,text=True,timeout=2)
                self.assertEqual(first.returncode==0,defect in ("valid","empty_backlog"),first.stderr)
                if first.returncode==0:
                    second=subprocess.run([sys.executable,"-I","-B","-c",backlog,str(root),first.stdout.strip()],capture_output=True,text=True,timeout=2)
                    self.assertEqual(second.returncode==0,defect=="valid",second.stderr)

    # origin: hot-update S4 plan §9.1 / T48; drill-run4 W/L. Assertions
    # run in subshells, so steps must execute in the parent and retain stderr.
    def test_saved_step_reemits_failure_without_losing_parent_state(self):
        source=(PROBE.parents[1]/"36-hot-update-pty.sh").read_text()
        functions=source[source.index("_pty_step_run() {"):source.index("_pty_broker_ready() {")]
        script='PTY_EV=$1; '+functions+'\nexercise() { CHANGED=yes; printf "receipt diagnostic\\n" >&2; return 7; }; '
        script+='_pty_step_rc=0; _pty_step_run exercise || _pty_step_rc=$?; test "$CHANGED" = yes || exit 99; '
        script+='v=$(_pty_step_ok "$_pty_step_rc" 2>&1); rc=$?; printf "%s\\n" "$v"; exit "$rc"'
        result=subprocess.run(["sh","-c",script,"test",str(self.root)],capture_output=True,text=True)
        self.assertEqual(result.returncode,7,result.stdout+result.stderr)
        self.assertIn("receipt diagnostic",result.stdout)
        self.assertIn("exercise",result.stdout)
        logs=list((self.root/"diagnostics").glob("*.out"))
        self.assertEqual(len(logs),1)
        self.assertIn("receipt diagnostic",logs[0].read_text())

    # origin: hot-update S4 plan §9.2 / T48; drill-run4 L. Old ctl rc70
    # must remain red against the frozen legacy75 assertion, with its receipt.
    def test_legacy_gate_exit_class_failure_is_visible_and_stays_red(self):
        source=(PROBE.parents[1]/"36-hot-update-pty.sh").read_text()
        function=source[source.index("_pty_legacy_gate_valid() {"):source.index("_pty_legacy_gate() {")]
        SENSOR["write_json"](self.root/"gate.json",dict(rc=70,reason="agent_upgrading",spawn_lines=0,
            effect_absent=True,pending=True,candidate_unchanged=True))
        result=subprocess.run(["sh","-c",function+'\n_pty_legacy_gate_valid "$1"',"test",str(self.root/"gate.json")],
                              capture_output=True,text=True)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('"rc":70',result.stdout+result.stderr)
        self.assertIn("agent_upgrading",result.stdout+result.stderr)

    # origin: hot-update S4 plan §12.1 / T48. Losing the passive receipt
    # source cannot leave yesterday's A/Q or KA snapshot available to a guard.
    def test_failed_runtime_recollection_removes_stale_current_state(self):
        path=self.root/"runtime.receipts.json"
        path.write_text('{"nonce":"old", "input":{"applied":123}}')
        library=PROBE.with_name("hot_update_pty.sh")
        script='HERE=$1; PTY_EVIDENCE=$2; PTY_CTL_NODE=isolated; PTY_AGENT_NODE=isolated; PTY_REMOTE_ROOT=isolated; '
        script+='d() { return 0; }; ctr_name() { printf isolated; }; dexec() { return 1; }; . "$3"; pty_fixture_collect; test ! -e "$PTY_EVIDENCE/runtime.receipts.json"'
        result=subprocess.run(["sh","-c",script,"test",str(PROBE.parents[2]),str(self.root),str(library)],
                              capture_output=True,text=True,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertFalse(path.exists())

    # origin: hot-update S4 plan §9.3 / T48. Exercise the real assertion adapter,
    # with the real probe and synthetic oracle controls, without running a drill.
    # origin: S8 internal review s8-hermetic F2. Restoring the extracted
    # dependencies must not bypass source, original-role or actual-exec proof.
    def test_legacy_execution_proof_rejects_missing_and_mismatched_inputs(self):
        functions = legacy_functions(self.root)
        paths = list(self.root.glob("legacy-*"))
        originals = {path: path.read_bytes() for path in paths}
        def judge():
            return subprocess.run(["sh", "-c", functions+'\nupgrade_legacy_exec_required "$1" agent',
                                   "legacy-proof", str(self.root)], capture_output=True, text=True, timeout=5)
        self.assertEqual(judge().returncode, 0)
        for path in paths:
            with self.subTest(missing=path.name):
                path.unlink()
                try:
                    result = judge()
                    self.assertEqual(result.returncode, 2, result.stdout+result.stderr)
                    self.assertIn("unproven", result.stderr)
                finally:
                    path.write_bytes(originals[path])
        path = self.root/"legacy-agent-after.json"
        for key, value in (("pid", 4001), ("start_ticks", 40001), ("boot_id", "another-boot"),
                           ("running_sha", "a"*64)):
            with self.subTest(field=key):
                row = json.loads(originals[path]); row[key] = value
                path.write_text(json.dumps(row))
                self.assertEqual(judge().returncode, 2)
                path.write_bytes(originals[path])
        self.assertEqual(judge().returncode, 0)

    # origin: S8 internal review s8-hermetic F2. The current adapter requires
    # a reviewed pin; missing provenance remains a native setup failure.
    def test_unpinned_legacy_signature_cannot_become_product_red(self):
        drill = PROBE.parents[1]/"39-hot-update-legacy-n1.sh"
        source = drill.read_text()
        functions = legacy_functions(self.root) + source[source.index("_pty_legacy_exporter() {"):source.index("\n\n_capture_logs()")]
        self.save(complete_fixture(self.root))
        doc = SENSOR["read_json"](self.root/"evidence.json")
        corrupt(self.root,doc,"early_hup")
        self.save(doc)
        assertion = PROBE.parents[2]/"lib/assert.sh"
        log = assertion.with_name("log.sh")
        script = 'HERE=$1; PTY_EV=$2; PROBE=$3; PTY_NONCE=$4; . "$5"; . "$6"; PTY_LEGACY_SIGNATURE=UNPINNED; '
        script += '_pty_continuity() { python3 "$PROBE" continuity --evidence "$PTY_EV" --nonce "$PTY_NONCE"; }; '
        script += functions+'\nINSTANCE=drill-hermetic-legacy; drill_begin hermetic-legacy; _pty_legacy_assertion; drill_end'
        result = subprocess.run(["sh","-c",script,"test",str(PROBE.parents[2]),str(self.root),str(PROBE),NONCE,str(log),str(assertion)],
                                capture_output=True,text=True,timeout=5)
        self.assertEqual(result.returncode, 2, result.stdout+result.stderr)
        self.assertIn("verdict=SETUP-RED",result.stdout+result.stderr)
        self.assertIn((self.root/"legacy-exporter.out").read_text().strip(),result.stdout+result.stderr)
        self.assertEqual((self.root/"continuity_oracle.rc").read_text().strip(),"1")
        self.assertEqual((self.root/"legacy_exporter.rc").read_text().strip(),"1")
        self.assertEqual(len((self.root/"legacy-exporter.out").read_text().splitlines()),1)
        self.assertTrue(json.loads((self.root/"legacy-exporter.json").read_text()))

    # origin: hot-update S4 plan §9.3 / T48. Only one reviewed categorical
    # signature earns native rc3; S4 treated a different shape, rc0 and rc2 as ASSERT-FAIL.
    # origin: S8 internal review s8-hermetic F2. S8 separates invalid
    # observations (SETUP-RED) from an unexpectedly fixed exporter (ASSERT-FAIL).
    def test_pinned_legacy_signature_preserves_native_three_way_verdict(self):
        source=(PROBE.parents[1]/"39-hot-update-legacy-n1.sh").read_text()
        functions=legacy_functions(self.root)+source[source.index("_pty_legacy_exporter() {"):source.index("\n\n_capture_logs()")]
        doc=complete_fixture(self.root)
        corrupt(self.root,doc,"early_hup")
        self.save(doc)
        line=self.invoke("legacy-exporter").stderr.strip()
        pattern=re.escape(line).replace("pid=4200","pid=[0-9]+").replace("nonce="+NONCE,"nonce=[0-9a-f]{32}")
        assertion=PROBE.parents[2]/"lib/assert.sh"
        log=assertion.with_name("log.sh")
        for defect,expected in (("reviewed",3),("different",2),("fixed",1),("invalid",2),("invalid_observation",2),("invalid_continuity_signature",2)):
            with self.subTest(defect=defect):
                doc=complete_fixture(self.root)
                if defect=="invalid": doc["windows"]=[]
                elif defect!="fixed": corrupt(self.root,doc,"early_hup")
                self.save(doc)
                pin=pattern.replace("hup=1","hup=2") if defect=="different" else pattern
                script='PTY_EV=$1; PROBE=$2; PTY_NONCE=$3; . "$4"; . "$5"; PTY_LEGACY_SIGNATURE=$6; '
                script+='_pty_continuity() { python3 "$PROBE" continuity --evidence "$PTY_EV" --nonce "$PTY_NONCE"; }; '
                if defect=="invalid_continuity_signature":
                    script+="""INVALID_LINE=$7; _pty_continuity() { printf '%s\\n' "$INVALID_LINE"; return 2; }; """
                if defect=="invalid_observation":
                    script+="""INVALID_LINE=$7; python3() { if [ "$2" = legacy-exporter ]; then printf '%s\\n' "$INVALID_LINE" >&2; return 2; fi; command python3 "$@"; }; """
                script+=functions+'\nINSTANCE=drill-hermetic-pin; drill_begin hermetic-pin; _pty_legacy_exporter >/dev/null; printf "%s\\n" "$?" > "$PTY_EV/predicate.rc"; _pty_legacy_assertion; drill_end'
                result=subprocess.run(["sh","-c",script,"test",str(self.root),str(PROBE),NONCE,str(log),str(assertion),pin,line],
                                      capture_output=True,text=True,timeout=5)
                self.assertEqual(result.returncode,expected,result.stdout+result.stderr)
                self.assertIn("verdict="+({3:"PRODUCT-RED",2:"SETUP-RED",1:"ASSERT-FAIL"}[expected]),result.stdout+result.stderr)
                self.assertEqual((self.root/"predicate.rc").read_text().strip(),"1" if defect=="reviewed" else "0" if defect=="fixed" else "2")
                if defect in ("fixed","invalid"):
                    self.assertFalse((self.root/"legacy-exporter.out").exists())
                    self.assertFalse((self.root/"legacy_exporter.rc").exists())

    # origin: hot-update S4 drill-run2 (first real run of 36.A/W/C/L and 39.P).
    # census walks the REAL /proc, which always contains pid 1; every fixture
    # here fed it synthetic identities, so the foreground comparison that
    # validated each child (pid 1 included) crashed only on a real host.
    def test_census_scans_real_proc_including_pid_one(self):
        me = SENSOR["identity"](os.getpid())
        snapshot = SENSOR["census"](os.getpid(), me, None)
        self.assertTrue(snapshot["foreground_alive"])
        self.assertEqual(SENSOR["valid_identity"](snapshot["owner"]), SENSOR["valid_identity"](me))
        other = dict(me, start_ticks=me["start_ticks"]+1)
        self.assertFalse(SENSOR["census"](os.getpid(), other, None)["foreground_alive"])

    # origin: hot-update S4 plan §9.1 / T48.
    def test_public_protocol_selftest(self):
        result = self.invoke("selftest")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("PASS", result.stdout)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--proxy"]:
        sys.exit(fake_transport(sys.argv[2:]))
    unittest.main()
