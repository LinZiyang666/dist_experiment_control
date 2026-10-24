#!/usr/bin/env -S python3 -I -B
"""Adversarial S10 P15 protocol receipts; no deployment or process is started.

FullstackInfraCollectorTest validates actual collector code against complete
static S9 shapes. P22/P25 retain real-binary/socket/old-exit lifecycle receipts.
"""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

# -I removes the script directory. Explicit sibling resolution keeps the
# registered hermetic entry independent of PYTHONPATH and the caller's cwd.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import fullstack_infra as infra


def ident(number, width=32):
    return [number] * width


def process(number):
    return {"BootID": "host-boot", "PID": 200 + number, "StartTicks": 1000 + number,
            "InvocationID": f"{number:032x}", "ExeDevice": 4, "ExeInode": 800 + number,
            "ExeSHA256": ident(number), "Cgroup": f"/system.slice/infra-{number}.service"}


def original(number):
    return {"key": {"role": "ctl", "host": "agt1", "unit": f"original-{number}", "sid": "s10-small-push",
                    "nid": "a0", "domain": f"workload-{number}", "uid": 1100},
            "boot_id": "client-boot", "pidns": "pid:[42]", "instance_id": f"original-{number}",
            "pid": 1000 + number, "start_ticks": 5000 + number}


def socket(owner, number, local, remote):
    return {"NetNS": 10, "Device": 9, "Inode": 10000 + number, "Cookie": 20000 + number,
            "Local": local, "Remote": remote, "Owner": copy.deepcopy(owner)}


def seal_census(value):
    value["Digest"] = list(hashlib.sha256(infra.canonical_s9(dict(value, Digest=ident(0)), "Census")).digest())
    return value


def census(processes, sockets):
    pids = {p["PID"]: copy.deepcopy(p) for p in processes}
    return seal_census({"Complete": True, "Generation": 1, "Processes": [pids[k] for k in sorted(pids)],
                        "Sockets": copy.deepcopy(sockets), "LiveBusiness": len(sockets),
                        "OldOwnerPending": 0, "Digest": ident(0)})


def physical(participant, generation, caddy, server, wss=True):
    client = process(20 + participant)
    number = generation * 100 + participant * 4
    local = f"10.0.0.2:{40000 + participant}"
    remote = "10.0.0.1:443" if wss else "10.0.0.1:4222"
    raw = socket(client, number, local, remote)
    accepted = socket(caddy if wss else server, number + 1, remote, local)
    upstream = socket(caddy, number + 2, f"10.0.0.1:{45000 + participant}", "10.0.0.3:4222") if wss else None
    server_accepted = socket(server, number + 3, "10.0.0.3:4222", upstream["Local"]) if wss else accepted
    observed = census([client, caddy, server], [accepted, server_accepted] if wss else [accepted])
    attachment = {"Participant": ident(participant, 16), "Generation": generation, "ServerID": f"NATS-{server['PID']}",
                  "CID": generation * 100 + participant, "URL": "wss://broker.example:443" if wss else "nats://broker.example:4222",
                  "Raw": raw, "Ingress": copy.deepcopy(caddy if wss else server), "HealthGeneration": generation}
    return {"translation": None, "attachment": attachment, "accepted": accepted, "upstream": upstream,
            "server_accepted": server_accepted, "server_id": attachment["ServerID"],
            "cid": attachment["CID"], "census": observed}


def artifact(number, kind):
    return {"Kind": kind, "Version": f"2.7.{number}", "GOOS": "linux", "GOARCH": "amd64",
            "ArchiveSHA256": ident(number + 10), "BinarySHA256": ident(number), "CatalogSHA256": ident(12)}


def raw_ref():
    return {"path": "/isolated/raw/s9.json", "sha": "d" * 64, "bytes": 123, "schema": "s9-physical-v1"}


def probe(kind, number, caddy):
    url = {"tls": "https://broker.example:443", "wss": "wss://broker.example:443",
           "ssh": "ssh://broker.example:2222", "tcp": "tcp://broker.example:9000"}[kind]
    port = 443 if kind in ("tls", "wss") else 2222 if kind == "ssh" else 9000
    client = process(40 + number)
    raw = socket(client, 900 + number * 2, f"10.0.0.2:{50000 + number}", f"10.0.0.1:{port}")
    accepted = socket(caddy, 901 + number * 2, raw["Remote"], raw["Local"])
    return {"translation": None, "id": f"probe-{number}", "kind": kind, "started_boot_ns": 180 + number, "finished_boot_ns": 200 + number,
            "attempts": 1, "rc": 0, "authenticated": kind == "wss", "hinted": False, "url": url,
            "tls_name": "broker.example" if port == 443 else "", "certificate_sha256": "a" * 64 if port == 443 else "",
            "route_generation": 2, "raw": raw, "accepted": accepted, "census": census([caddy], [accepted])}


def fixture(component=2, wss=True, action=1, topology="single"):
    old_caddy, new_caddy, old_nats, new_nats = process(1), process(2), process(3), process(4)
    target, current = (old_caddy, new_caddy) if component == 2 else (old_nats, new_nats)
    if action == 3:
        current = target
    plan = {"Tx": ident(9, 16), "Step": 1, "Domain": ident(8, 16), "Component": component, "Action": action,
            "Candidate": artifact(current["ExeSHA256"][0], 2 if component == 2 else 1),
            "Previous": artifact(target["ExeSHA256"][0], 2 if component == 2 else 1), "Before": copy.deepcopy(target),
            "ConfigSHA256": ident(5), "Store": {name: 1 for name in ("Device", "Inode", "MountID", "LockDevice", "LockInode")},
            "PolicyRevision": 1}
    cp = {"Cohort": ident(7, 16), "Assets": {"Revision": 4, "Accounts": ident(5), "Assets": ident(6), "Participants": ident(7), "Complete": True},
          "Transfers": {"Version": 1, "SourceSHA256": ident(5), "SchemaSHA256": ident(6), "InvocationSet": ident(7),
                        "ReceiptSet": ident(8), "ConsumerSet": ident(9), "Complete": True}, "Proof": ident(8)}
    bindings = [{"workload_id": "original-" + leg, "leg": leg, "participant": ident(i, 16), "original": original(i)}
                for i, leg in enumerate(("flow", "sender", "receiver"), 1)]
    def snapshot(done):
        caddy = current if done and component == 2 else old_caddy
        server = current if done and component == 1 else old_nats
        generation = 3 if done and action != 3 else 1
        physical_rows = [physical(i, generation, caddy, server, wss) for i in range(1, 4)]
        observed = census([caddy, server], [sock for row in physical_rows for sock in row["census"]["Sockets"]])
        status = {"Tx": ident(9, 16), "Step": 1, "Phase": 9 if done else 3, "Revision": 5 if done else 1,
                  "Code": "", "Detail": "preserved native detail", "Before": copy.deepcopy(target),
                  "Current": copy.deepcopy(current if done else target), "Census": observed, "Proof": ident(7)}
        stamp = {"process": original(90), "tx_id": bytes(plan["Tx"]).hex(), "cut_id": "actual-cut",
                 "direction": "reverse" if action == 2 else "forward", "query_nonce": "fresh-after" if done else "fresh-before",
                 "scope_digest": "sealed-domain", "generation": 1, "revision": status["Revision"], "sequence": 5 if done else 1,
                 "at_boot_ns": 300 if done else 100}
        stamp["process"]["boot_id"] = "host-boot"
        probes = [probe(kind, i, caddy) for i, kind in enumerate(("tls", "wss", "ssh", "tcp"))] if done else []
        owners = [] if action == 3 else [{"role": "caddy" if component == 2 else "nats", "process": copy.deepcopy(target),
                                         "exited": done, "source": "held-pidfd", "observed_boot_ns": stamp["at_boot_ns"]}]
        return {"schema": "fullstack-infra-v1", "tier": "hermetic", "stage": "completed" if done else "before", "stamp": stamp,
                "plan": copy.deepcopy(plan), "checkpoint": copy.deepcopy(cp), "bindings": copy.deepcopy(bindings),
                "attachments": [row["attachment"] for row in physical_rows], "physical": physical_rows, "status": status,
                "executing": copy.deepcopy(current if done else target), "probes": probes, "owners": owners, "raw": [raw_ref()],
                "outbound": [], "signals": [], "events": []}
    before, after = snapshot(False), snapshot(True)
    def event(kind, at, evidence):
        return {"Version": 1, "Tx": plan["Tx"], "Step": plan["Step"], "Kind": kind, "BootID": "host-boot", "BootNS": at,
                "Subject": plan["Domain"], "Generation": 1, "Evidence": evidence}
    if action != 3:
        out_caddy = process(5) if component == 2 else old_caddy
        out_server = process(6) if component == 1 and topology == "quorum" else current if component == 1 else old_nats
        after["outbound"] = [physical(i, 2, out_caddy, out_server, wss) for i in range(1, 4)]
        after["events"] = [event("attachment_changed", 210 + i, row["census"]["Digest"])
                           for i, row in enumerate(after["outbound"])]
        ops = [5, 6] if component == 1 and topology == "quorum" else [6] if component == 1 else [7]
        for i, op in enumerate(ops):
            receipt = {"Tx": plan["Tx"], "Sequence": i + 1, "Revision": i + 1, "Op": op, "Applied": True,
                       "RequestDigest": ident(30 + i), "Journal": ident(40 + i), "Current": target,
                       "Census": before["status"]["Census"], "Code": ""}
            after["signals"].append({"receipt": receipt, "observed_boot_ns": 230 + i})
            after["events"].append(event("signal_applied", 230 + i, receipt["RequestDigest"]))
        after["events"] += [event("old_exit", 245, after["status"]["Census"]["Digest"]),
                            event("owner_retired", 260, after["status"]["Census"]["Digest"])]
    after["events"] += [event("return_verified", 270, after["status"]["Census"]["Digest"]),
                        event("completed", 290, after["status"]["Proof"])]
    owners = [{"role": row["role"], "process": row["process"]} for row in after["owners"]]
    slots = [{"id": p["id"], "kind": p["kind"], "scheduled_boot_ns": 150 + i,
              "deadline_boot_ns": 290, "url": p["url"]} for i, p in enumerate(after["probes"])]
    return before, after, bindings, owners, slots


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def check(f, topology="single"):
    return infra.validate_cut(encode(f[0]), encode(f[1]), *f[2:], topology)


# origin: hot-update S10 plan §10.2 P15 / FullstackInfraCollectorTest.
class FullstackInfraCollectorTest(unittest.TestCase):
    # origin: hot-update S10 plan §5.1 / §5.4 / §10.2 P15.
    def test_complete_s9_cohorts_preserve_all_physical_fields_and_raw(self):
        for component, wss, topology, action in ((2, True, "single", 1), (2, True, "quorum", 2),
                                                (1, True, "quorum", 1), (1, False, "single", 2), (2, True, "single", 3)):
            with self.subTest(component=component, wss=wss, topology=topology, action=action):
                f = fixture(component, wss, action, topology)
                for row in (f[0], f[1]):
                    row["checkpoint"]["Assets"]["Complete"] = False
                    row["checkpoint"]["Assets"]["Accounts"] = ident(0)
                    row["checkpoint"]["Transfers"]["Complete"] = False
                result = check(f, topology)
                self.assertEqual(result["physical_participants"], 3)
                self.assertEqual(result["tier"], "hermetic")
                value = infra.record(encode(f[1]))
                self.assertEqual(value["status"], f[1]["status"])
                self.assertEqual(value["physical"], f[1]["physical"])
                self.assertEqual(value["raw"], f[1]["raw"])
                self.assertNotIn("verdict", result, "protocol checks cannot sign E1 native GREEN")

    # origin: hot-update S10 plan §5.4 / §10.2 P15 / mandatory WSS control.
    def test_wss_and_each_js_leg_must_traverse_the_cut_node(self):
        for component in (1, 2):
            for participant in range(3):
                with self.subTest(component=component, participant=participant):
                    f = fixture(component)
                    before = f[0]
                    row = physical(participant + 1, 1, process(6) if component == 2 else process(1),
                                   process(6) if component == 1 else process(3))
                    before["physical"][participant] = row
                    before["attachments"][participant] = row["attachment"]
                    with self.assertRaisesRegex(infra.ContinuityError, "did not traverse"):
                        check(f)

        for component in (1, 2):
            f = fixture(component)
            row = physical(1, 3, process(9) if component == 2 else process(1),
                           process(9) if component == 1 else process(3))
            f[1]["physical"][0] = row
            f[1]["attachments"][0] = row["attachment"]
            with self.subTest(return_component=component), self.assertRaisesRegex(infra.ContinuityError, "missed the current"):
                check(f)

    # origin: hot-update S10 plan §5.4 / §10.2 P15 / mandatory URL control.
    def test_url_and_new_cid_cannot_replace_a_new_raw_socket(self):
        for keep in (("Cookie",), ("Device", "Inode"), ("Cookie", "Device", "Inode")):
            f = fixture()
            row = f[1]["physical"][0]
            for field in keep:
                row["attachment"]["Raw"][field] = f[0]["physical"][0]["attachment"]["Raw"][field]
            row["attachment"]["URL"] = "wss://a-new-label.example:443"
            with self.subTest(keep=keep), self.assertRaisesRegex(infra.ContinuityError, "without a physical NC move"):
                check(f)

    # origin: hot-update S10 plan §5.4 / §8 M08C / §10.2 P15 / mandatory old-Caddy control.
    def test_old_temporary_caddy_and_support_workers_require_held_pidfd_exit(self):
        for role in ("caddy", "temporary-caddy", "nats", "helper", "launcher"):
            f = fixture()
            old = {"role": role, "process": process(7)}
            f[3].append(old)
            f[1]["owners"].append(dict(old, exited=False, source="held-pidfd", observed_boot_ns=300))
            with self.subTest(role=role), self.assertRaisesRegex(infra.ContinuityError, "still alive"):
                check(f)
        f = fixture()
        f[1]["owners"][0]["exited"] = False
        with self.assertRaisesRegex(infra.ContinuityError, "still alive"):
            check(f)
        f = fixture()
        f[1]["status"]["Census"]["Processes"].append(f[0]["plan"]["Before"])
        f[1]["status"]["Census"]["Processes"].sort(key=lambda p: p["PID"])
        seal_census(f[1]["status"]["Census"])
        with self.assertRaisesRegex(infra.ContinuityError, "current census"):
            check(f)
        f = fixture()
        f[1]["owners"][0]["source"] = "pid-is-missing"
        with self.assertRaisesRegex(infra.ContinuityError, "still alive"):
            check(f)

    # origin: hot-update S10 plan §8 M07N/M07C / §10.2 P15.
    def test_disk_only_completion_and_same_pid_exec_cannot_sign_binary_cut(self):
        for component in (1, 2):
            f = fixture(component)
            f[1]["executing"] = copy.deepcopy(f[0]["executing"])
            f[1]["status"]["Current"] = copy.deepcopy(f[0]["executing"])
            with self.subTest(component=component), self.assertRaisesRegex(infra.ContinuityError, "old executing process"):
                check(f)
        f = fixture()
        f[1]["executing"]["PID"] = f[0]["executing"]["PID"]
        f[1]["executing"]["StartTicks"] = f[0]["executing"]["StartTicks"]
        f[1]["status"]["Current"] = copy.deepcopy(f[1]["executing"])
        with self.assertRaisesRegex(infra.ContinuityError, "old executing process"):
            check(f)
        f = fixture()
        f[1]["executing"]["ExeSHA256"] = ident(50)
        with self.assertRaisesRegex(infra.ContinuityError, "executing image"):
            check(f)

    # origin: hot-update S10 plan §5.4 / §10.2 P15.
    def test_canonical_first_handshake_authenticated_wss_and_no_hint_are_required(self):
        changes = (("url", "https://broker.example:8443", 0, "canonical probe slot"),
                   ("attempts", 2, 0, "first attempt"), ("rc", 1, 0, "first attempt"),
                   ("authenticated", False, 1, "not authenticated"), ("tls_name", "wrong.example", 0, "canonical 443"))
        for field, value, index, message in changes:
            f = fixture()
            f[1]["probes"][index][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(infra.ContinuityError, message):
                check(f)
        f = fixture()
        f[1]["probes"][1]["hinted"] = True
        with self.assertRaisesRegex(infra.PendingEvidence, "no-hint"):
            check(f)
        f = fixture()
        p = f[1]["probes"][0]
        p["raw"]["Remote"] = "10.0.0.1:8443"
        p["accepted"]["Local"] = "10.0.0.1:8443"
        p["census"]["Sockets"][0]["Local"] = "10.0.0.1:8443"
        seal_census(p["census"])
        with self.assertRaisesRegex(infra.ContinuityError, "bypassed canonical 443"):
            check(f)

    # origin: hot-update S10 plan §5.4 / §10.2 P15.
    def test_probe_denominator_clock_and_original_binding_never_shrink(self):
        for name in ("bindings", "probes", "owners", "physical"):
            f = fixture()
            f[1][name].pop()
            with self.subTest(name=name), self.assertRaises(infra.EvidenceError):
                check(f)
        f = fixture()
        f[1]["probes"][0]["finished_boot_ns"] = 400
        with self.assertRaises(infra.EvidenceError):
            check(f)
        f = fixture()
        f[1]["bindings"][0]["original"]["pid"] += 1
        with self.assertRaisesRegex(infra.ContinuityError, "binding changed"):
            check(f)
        f = fixture()
        f[1]["stamp"]["query_nonce"] = f[0]["stamp"]["query_nonce"]
        with self.assertRaisesRegex(infra.ContinuityError, "stale cut stamp"):
            check(f)
        f = fixture()
        f[1]["stamp"]["process"]["boot_id"] = "different-host-boot"
        with self.assertRaisesRegex(infra.EvidenceError, "cross-host BOOTTIME"):
            check(f)
        f = fixture(action=2)
        f[1]["stamp"]["direction"] = "forward"
        with self.assertRaisesRegex(infra.EvidenceError, "current stamp"):
            check(f)

    # origin: hot-update S10 plan §5.4 / §10.2 P15.
    def test_physical_join_requires_caddy_upstream_nats_session_and_complete_census(self):
        for field, value, message in (("server_id", "other", "serverID/cid"), ("cid", 0, "serverID/cid"),
                                      ("upstream", None, "Caddy to NATS")):
            f = fixture()
            f[1]["physical"][0][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(infra.EvidenceError, message):
                check(f)
        f = fixture()
        f[1]["physical"][0]["accepted"]["Owner"] = process(8)
        with self.assertRaisesRegex(infra.ContinuityError, "owner mismatch"):
            check(f)
        f = fixture()
        f[1]["physical"][0]["census"]["Complete"] = False
        seal_census(f[1]["physical"][0]["census"])
        with self.assertRaises(infra.PendingEvidence):
            check(f)
        f = fixture()
        f[1]["physical"][0]["census"]["Digest"] = ident(2)
        with self.assertRaisesRegex(infra.EvidenceError, "digest mismatch"):
            check(f)

    # origin: hot-update S10 plan §5.1 / §10.2 P15 / full native DTO preservation.
    def test_strict_native_dto_closure_rejects_loss_aliases_duplicates_and_bad_widths(self):
        for path, value in ((["status", "Unknown"], 1), (["status", "Phase"], 12), (["status", "Tx"], [1]),
                            (["physical", 0, "attachment", "Raw", "Cookie"], True), (["stamp", "process", "key", "uid"], -1)):
            f = fixture()
            target = f[1]
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            with self.subTest(path=path), self.assertRaises(infra.EvidenceError):
                check(f)
        f = fixture()
        del f[1]["status"]["Detail"]
        with self.assertRaises(infra.EvidenceError):
            check(f)
        raw = encode(fixture()[1])
        for bad in (raw.replace(b'"Phase":9', b'"Phase":9,"Phase":9'), raw + b'{}',
                    raw.replace(b'"Phase":9', b'"Phase":NaN'), raw.replace(b'"Detail":', b'"detail":')):
            with self.assertRaises(infra.EvidenceError):
                infra.record(bad)

    # origin: hot-update S10 plan §5.4 / §10.2 P15 / H0 before READY.
    def test_h0_checks_every_discovered_connect_url_before_ready(self):
        f = fixture()
        base = f[1]["probes"][1]
        urls = ["wss://broker.example:443", "wss://discovered.example:443"]
        rows = []
        for url in urls:
            rows.append({"url": url, "trusted": True, "tls_name": url.split("//")[1].split(":")[0],
                         "certificate_sha256": "a" * 64, "route_generation": 1, "finished_boot_ns": 80,
                         "raw": base["raw"], "accepted": base["accepted"], "census": base["census"], "translation": None})
        h0 = {"schema": "fullstack-infra-h0-v1", "initial_urls": urls[:1], "discovered_urls": urls[1:], "endpoints": rows, "raw": raw_ref()}
        infra.validate_h0(h0, urls, 100)
        for field, value in (("trusted", False), ("certificate_sha256", ""), ("tls_name", "other"), ("finished_boot_ns", 100)):
            bad = copy.deepcopy(h0)
            bad["endpoints"][1][field] = value
            with self.subTest(field=field), self.assertRaises(infra.EvidenceError):
                infra.validate_h0(bad, urls, 100)
        bad = copy.deepcopy(h0)
        bad["endpoints"].pop()
        with self.assertRaises(infra.PendingEvidence):
            infra.validate_h0(bad, urls, 100)
        with self.assertRaisesRegex(infra.EvidenceError, "route set mismatch"):
            infra.validate_h0(h0, urls[:1], 100)

    # origin: hot-update S10 plan §5.4 / S9 L1/L2 / §10.2 P15.
    def test_n1_never_ldm_and_n3_moves_physical_cohort_before_ldm(self):
        f = fixture(1, topology="quorum")
        check(f, "quorum")
        with self.assertRaisesRegex(infra.ContinuityError, "must not use LDM"):
            check(f, "single")
        for fault in ("before-move", "stayed-on-node", "missing-outbound", "missing-signal", "wrong-event-digest", "same-outbound-inode"):
            f = fixture(1, topology="quorum")
            if fault == "before-move":
                f[1]["signals"][0]["observed_boot_ns"] = 205
                signal = next(e for e in f[1]["events"] if e["Kind"] == "signal_applied")
                signal["BootNS"] = 205
                f[1]["events"].sort(key=lambda e: e["BootNS"])
            elif fault == "stayed-on-node":
                f[1]["outbound"][0] = physical(1, 2, process(1), process(3))
            elif fault == "missing-outbound":
                f[1]["outbound"].pop()
            elif fault == "missing-signal":
                f[1]["signals"].pop(0)
            elif fault == "same-outbound-inode":
                for name in ("Device", "Inode"):
                    f[1]["outbound"][0]["attachment"]["Raw"][name] = f[0]["physical"][0]["attachment"]["Raw"][name]
            else:
                f[1]["events"][0]["Evidence"] = ident(99)
            with self.subTest(fault=fault), self.assertRaises(infra.EvidenceError):
                check(f, "quorum")

        f = fixture(1)
        f[1]["physical"][0] = f[1]["outbound"][0]
        f[1]["attachments"][0] = f[1]["outbound"][0]["attachment"]
        with self.assertRaisesRegex(infra.ContinuityError, "kept the outbound"):
            check(f)

    # origin: hot-update S10 plan §5.4 / S9 canonical nft redirect / §10.2 P15.
    def test_warm_canonical_443_joins_independent_conntrack_translation(self):
        p = probe("tls", 0, process(5))
        slot = {"id": p["id"], "kind": "tls", "scheduled_boot_ns": 150, "deadline_boot_ns": 290, "url": p["url"]}
        p["accepted"]["Local"] = "10.0.0.1:8443"
        p["census"]["Sockets"][0]["Local"] = "10.0.0.1:8443"
        seal_census(p["census"])
        with self.assertRaisesRegex(infra.PendingEvidence, "kernel NAT"):
            infra.proof_probe(p, slot)
        p["translation"] = {"netns": 10, "original_local": p["raw"]["Local"], "original_remote": p["raw"]["Remote"],
                            "reply_local": p["accepted"]["Local"], "reply_remote": p["accepted"]["Remote"],
                            "route_generation": 2, "rules_digest": "b" * 64, "raw": raw_ref()}
        infra.proof_probe(p, slot)
        for name, value in (("reply_remote", "10.0.0.2:60000"), ("route_generation", 1), ("netns", 11)):
            bad = copy.deepcopy(p)
            bad["translation"][name] = value
            with self.subTest(name=name), self.assertRaises(infra.ContinuityError):
                infra.proof_probe(bad, slot)

    # origin: hot-update S10 plan §11.2 / §10.2 P15 / P22/P25 lifecycle adapter.
    def test_archive_preserves_bytes_and_cleanup_keeps_sticky_failure(self):
        raw = encode(fixture()[1])
        with tempfile.TemporaryDirectory(prefix="fullstack-infra-") as root:
            collector = infra.InfraCollector(root, "hermetic")
            for _ in range(5):
                ref = collector.collect(raw)
                self.assertEqual(Path(ref["path"]).read_bytes(), raw)
                self.assertEqual(ref["sha"], hashlib.sha256(raw).hexdigest())
            with self.assertRaisesRegex(infra.EvidenceError, "Close must precede Join"):
                collector.join()
            collector.close()
            self.assertEqual(len(collector.join()), 5)
            with self.assertRaisesRegex(infra.EvidenceError, "closed"):
                collector.collect(raw)
        with tempfile.TemporaryDirectory(prefix="fullstack-infra-") as root:
            collector = infra.InfraCollector(root, "hermetic")
            collector.total = infra.ARCHIVE_LIMIT - len(raw) + 1
            with self.assertRaisesRegex(infra.EvidenceError, "archive ceiling"):
                collector.collect(raw)
            self.assertEqual(list(Path(root).iterdir()), [])
            collector.close()
            with self.assertRaisesRegex(infra.EvidenceError, "archive ceiling"):
                collector.join()
        bad_shape = fixture()[1]
        bad_shape["attachments"][0].pop("Participant")
        bad_upstream = fixture()[1]
        bad_upstream["physical"][0]["upstream"] = 1
        for bad in (b'{}', encode(dict(fixture()[1], tier="deploy")), encode(bad_shape), encode(bad_upstream)):
            with tempfile.TemporaryDirectory(prefix="fullstack-infra-") as root:
                collector = infra.InfraCollector(root, "hermetic")
                with self.assertRaises(infra.EvidenceError):
                    collector.collect(bad)
                with self.assertRaises(infra.EvidenceError):
                    collector.collect(raw)
                collector.close()
                with self.assertRaises(infra.EvidenceError):
                    collector.join()


if __name__ == "__main__":
    unittest.main()
