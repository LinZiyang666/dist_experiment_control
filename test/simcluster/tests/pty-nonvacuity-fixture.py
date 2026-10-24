#!/usr/bin/env python3
"""Independent raw files and passive runtime records for shell predicate controls.

These are hermetic fixtures, never deployment receipts. The production parser
must join the original child/ctl identities and verify the log snapshot itself.
origin: hot-update S4 plan §9.1 / §9.2 / T48.
"""
import datetime
import hashlib
import json
from pathlib import Path
import runpy
import sys

root, nonce, mode = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
sensor = runpy.run_path(str(Path(__file__).resolve().parents[1]/"drills/lib/hot_update_pty_probe.py"))

def save(name, value):
    path = root/name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value)+"\n")

def identity(pid, parent):
    return dict(pid=pid, ppid=parent, start_ticks=pid*10, boot_id="fixture-boot", tty_nr=1)

# origin: hot-update S4 internal review rv-drill DRILL-2..4 / DRILL-5.
# Keep the second writer's independent raw-file fixture boundary as well as
# the passive runtime fixtures below. These documents remain synthetic.
if mode.startswith(("carrier-", "draining-", "legacy-")) or mode == "raw-carrier":
    import copy
    suite = runpy.run_path(str(Path(__file__).with_name("hot-update-pty-probe-test.py")))
    if nonce != suite["NONCE"]:
        raise SystemExit("independent probe fixture requires its bound nonce")
    if mode == "raw-carrier":
        suite["carrier_fixture"](root)
        receipt = sensor["read_json"](root/"ctl.identity.json")
        receipt["identity"]["tty_nr"] = 1
        save("ctl.identity.json", receipt)
    elif mode.startswith("carrier-"):
        doc = suite["complete_fixture"](root)
        doc["windows"][0].update(kind="carrier", end=27.5)
        doc["images"] = [dict(doc["images"][0], terminal_count=1, exit_code=23)]
        for direction in ("input", "output"):
            for side in ("sent", "received"):
                for row in doc[direction][side]["records"][26:]:
                    row["at"] += 25
        for row in doc["processes"]:
            if row["at"] >= 3:
                row["at"] += 25
        doc["controls"][-1]["at"] += 25
        if mode == "carrier-unexplained":
            for row in doc["output"]["received"]["records"][38:]:
                row["at"] += 6
        elif mode == "carrier-over-recovery":
            doc["windows"][0]["end"] += 155
            for direction in ("input", "output"):
                for side in ("sent", "received"):
                    for row in doc[direction][side]["records"][26:]:
                        row["at"] += 155
            for row in doc["processes"]:
                if row["at"] >= 28:
                    row["at"] += 155
            doc["controls"][-1]["at"] += 155
        elif mode == "carrier-unobserved":
            doc["windows"][0]["fault_observed"] = False
        elif mode == "carrier-normal-stall":
            doc["windows"][0]["kind"] = "refusal"
        elif mode != "carrier-bounded":
            raise SystemExit("unknown carrier control")
        save("evidence.json", doc)
    elif mode.startswith("draining-"):
        doc = suite["complete_fixture"](root)
        doc["windows"][0].update(kind="draining", leader_exited=True,
            leader_exit_at=2.4, tail_s=.25, draining_end_at=4.9)
        doc["processes"] = [r for r in doc["processes"] if r["role"] != "leader" or r["at"] < 2.4]
        for row in doc["processes"]:
            if row["role"] == "foreground" and row["at"] >= 2.4:
                row.update(ctty=False, hup=1, foreground_pgid=-1)
                row["identity"]["process_group"] = row["identity"]["pid"]
        leader = dict(suite["process"](4100), state="Z", session_id=4100)
        census = doc["resources"]["after"][0]
        census.update(zombies=[copy.deepcopy(leader)], at=3.5)
        if mode == "draining-foreign":
            census["zombies"][0]["start_ticks"] += 1
        elif mode == "draining-late":
            census["at"] = 5
        elif mode == "draining-before-exit":
            census["at"] = 2.3
        elif mode == "draining-settled":
            doc["resources"]["settled"]["zombies"] = [leader]
        elif mode == "draining-wrong-session":
            census["zombies"][0]["session_id"] += 1
        elif mode == "draining-no-timestamp":
            del census["at"]
        elif mode != "draining-held":
            raise SystemExit("unknown draining control")
        save("evidence.json", doc)
    else:
        suite["assembly_fixture"](root)
        trace = sensor["read_json"](root/"ctl.trace.json")
        trace["scenario"] = "P"
        save("ctl.trace.json", trace)
        rows = [json.loads(line) for line in (root/"logs/agent_slog.log").read_text().splitlines()]
        start = dict(time=rows[0]["time"], msg="agent: run", pid="task-token", argv=[nonce])
        activation = dict(time=rows[4]["time"], msg="agent: upgrade COMMITTED (registered as the new binary)")
        suite["save_runtime_log"](root, [start, activation])
        save("agent/settled.ps.json", dict(processes=[]))
        save("supply.json", dict(old_sha="a"*64, new_sha="b"*64, artifact_sha="c"*64,
            served_sha="c"*64, pid=4000, start_ticks=40000))
        save("hop-1.json", dict(pid=4000, start_ticks=40000, running_sha="b"*64, disk_sha="b"*64,
            marker=dict(state="committed", new_sha="b"*64, boot_count=1, target_sid="lab", target_nid="agt1")))
        for role in ("agent", "ctl", "broker"):
            (root/("endpoint-"+role+".version")).write_text("tether v0.6.0 (proto v1)\n")
            (root/("endpoint-"+role+".sha")).write_text("a"*64+"  fixture\n")
        sensor["assemble_evidence"](root, nonce)
        if mode == "legacy-positive-missing":
            trace["scenario"] = "L"
            save("ctl.trace.json", trace)
        elif mode == "legacy-unreadable":
            doc = sensor["read_json"](root/"logs.json")
            doc["read_ok"] = False
            save("logs.json", doc)
        elif mode == "legacy-no-replacement":
            doc = sensor["read_json"](root/"hop-1.json")
            doc["running_sha"] = "a"*64
            save("hop-1.json", doc)
        elif mode == "legacy-no-waitpid":
            doc = sensor["read_json"](root/"evidence.json")
            del doc["ctl"]["wait_status"]
            save("evidence.json", doc)
        elif mode == "legacy-false-absence":
            save("agent/settled.ps.json", dict(processes=[dict(pid="task-token", argv=[nonce])]))
        elif mode == "legacy-not-old":
            doc = sensor["read_json"](root/"evidence.json")
            for endpoint in doc["fallback"]["endpoints"]:
                endpoint["old"] = False
            save("evidence.json", doc)
        elif mode != "legacy-loss":
            raise SystemExit("unknown legacy control")
    raise SystemExit(0)

root.mkdir(parents=True, exist_ok=True)
ctl = identity(4300, 1)
remote = []
for i in range(30):
    for role, pid, parent in (("leader",4100,771),("foreground",4200,4100)):
        remote.append(dict(role=role,identity=identity(pid,parent),at=i*.1,
                           foreground_pgid=4200,ctty=True,alive=True,hup=0,early_eof=False,nonce=nonce))
save("remote.trace.json",dict(nonce=nonce,samples=remote))
save("ctl.identity.json",dict(nonce=nonce,identity=ctl))
save("ctl.trace.json",dict(nonce=nonce,started_wall=1700000000,
                           samples=[dict(identity=ctl)]))
save("agent/before.json",dict(identity=identity(771,1),marker=dict(generation=0)))
for name in ("ctl.sent","ctl.received","remote.sent","remote.received"):
    (root/name).write_bytes(b"".join(sensor["frame"](nonce,i,sensor["payload"](i)) for i in range(30)))
save("windows.json",dict(nonce=nonce,windows=[dict(kind="carrier",start=.5,end=3)]))

def stamp(at):
    return datetime.datetime.fromtimestamp(1700000000+at,datetime.timezone.utc).isoformat()

state = dict(pid="original-task",input_applied=30,input_accepted=100,input_fin=False,input_ended=False,
             output_base=10,output_end=50,output_fin=False,control_seq=7,ka_seq=7,
             ka_deadline_boot_ns=900,revoked=False,master_closed=False)
records = [dict(time=stamp(.1),msg="agent: run reliable=true started",pid="original-task",
                request_id_hash="c"*64,os_pid=4100,start_time_ticks=41000)]
if mode == "watchdog":
    for generation, phase, at, applied, end in ((1,"cut",1,30,50),(1,"activated",1.2,30,50),
                                               (2,"cut",2,80,120),(2,"activated",2.2,80,120)):
        records.append(dict(state,generation=generation,phase=phase,time=stamp(at),
                            msg="agent: pty image state",input_applied=applied,output_end=end))
else:
    records.append(dict(state,generation=0,phase="carrier-lost",time=stamp(1),msg="agent: pty image state"))
    if mode != "missing-restored":
        records.append(dict(state,generation=0,phase="carrier-restored",time=stamp(2),
                            msg="agent: pty image state",pid="foreign-task" if mode == "foreign-token" else "original-task"))
    records.append(dict(time=stamp(1.1),msg="agent: rebuilding session"))
raw = ("\n".join(json.dumps(row) for row in records)+"\n").encode()
(root/"logs").mkdir(exist_ok=True)
cursors = []
for stream in sensor["STREAMS"]:
    data = raw if stream == "agent_slog" else b""
    (root/"logs"/(stream+".log")).write_bytes(data)
    cursors.append(dict(stream=stream,file="logs/"+stream+".log",length=len(data),
                        sha256=hashlib.sha256(data).hexdigest(),read_ok=True,
                        inode=1,inode_before=1,before=0,after=len(data)))
save("logs.json",dict(read_ok=True,cursors=cursors))
if mode == "watchdog":
    sensor["current_receipts"](root,nonce,"rollback")
