#!/usr/bin/env python3
"""The sole S8 live fixture command contract, version 1.

Manifest ports are absolute argv/cwd/timeout_s command descriptors. Actors
create the actual SSH/TCP/PTY/exec/transfer/SS/remote-fs/control workload; the
observe, admissions and drain ports are independent read-only adapters to the
product and S7 keeper. They consume one JSON request on stdin and emit one
bounded public response. They must not repair state or reconnect business IO.

The runner passes --lifeline-fd at start and retains its only write end until
stop. Its death cleans all fixture descendants. Missing adapters are SETUP-RED,
never a reduced profile. judge reads files only and leaves native results intact.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import sys
import time

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("upgrade_tx_collect", HERE / "upgrade_tx_collect.py")
collect = sys.modules.get(spec.name)
if collect is None:
    collect = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = collect
    spec.loader.exec_module(collect)
supervisor = collect.supervisor


def manifest_read(path):
    value = collect.read(path)
    keys = {"version", "source_commit", "artifact_sha", "run_id", "arm", "case_id", "role", "topology",
            "actors", "ports", "domains", "expect", "faults", "deadline_s", "transfer_budget"}
    collect.require(set(value) == keys and type(value["version"]) is int and value["version"] == 1, "manifest_schema_invalid")
    for name in ("source_commit", "artifact_sha", "run_id", "arm", "role", "topology"):
        schema = collect.read(HERE / "upgrade_tx_schema.json")["properties"][name]
        collect.schema_check(value[name], schema, where="manifest." + name)
    for name in ("case_id",):
        collect.require(isinstance(value[name], str) and re.fullmatch("[A-Za-z0-9_.-]{1,128}", value[name]), "manifest_case_invalid")
    collect.require(type(value["deadline_s"]) is int and 0 < value["deadline_s"] <= 14400, "manifest_deadline_invalid")
    collect.require(isinstance(value["domains"], list) and set(value["domains"]) <= collect.DOMAINS
                    and len(set(value["domains"])) == len(value["domains"]) and value["domains"], "manifest_domains_invalid")
    collect.require(set(value["ports"]) == {"observe", "admissions", "drain"}, "required_observer_port_missing")
    for command in value["ports"].values():
        supervisor.check_spec(command, paths=False)
    actors = value["actors"]
    collect.require(isinstance(actors, list) and len(actors) <= 128, "manifest_actors_invalid")
    ids = set()
    for actor in actors:
        collect.require(set(actor) == {"id", "family", "resource_ids", "command"}
                        and actor["id"] not in ids and re.fullmatch("[A-Za-z0-9_.-]{1,128}", actor["id"]), "manifest_actor_identity_invalid")
        ids.add(actor["id"])
        collect.require(actor["family"] in collect.PROFILE and isinstance(actor["resource_ids"], list)
                        and actor["resource_ids"] and len(set(actor["resource_ids"])) == len(actor["resource_ids"]), "manifest_actor_resources_invalid")
        supervisor.check_spec(actor["command"], paths=False)
    # origin: hot-update S8 plan §9.0 / §10. Starting a subset is a setup
    # defect even on a fault arm; no command may silently select a smaller load.
    collect.require({family: sum(actor["family"] == family for actor in actors) for family in collect.PROFILE} == collect.PROFILE,
                    "full_profile_actor_missing")
    resource_ids = [resource_id for actor in actors for resource_id in actor["resource_ids"]]
    collect.require(len(resource_ids) == len(set(resource_ids)), "actor_resource_reused")
    expected = value["expect"]
    collect.require(set(expected) == {"phase", "outcome", "native_rc", "completion", "hit_required", "a6"}
                    and type(expected["native_rc"]) is int and 0 <= expected["native_rc"] <= 255
                    and all(type(expected[name]) is bool for name in ("completion", "hit_required", "a6")), "manifest_expectation_invalid")
    collect.require(not (expected["a6"] and expected["hit_required"]), "fault_arm_cannot_sign_normal_calibration")
    budget = value["transfer_budget"]
    collect.require(set(budget) == {"rate_bytes_s", "cut_budget_s", "drain_budget_s", "margin_s", "size_bytes"}
                    and all(type(item) is int and item > 0 for item in budget.values()), "transfer_budget_invalid")
    collect.require(budget["size_bytes"] >= budget["rate_bytes_s"] * (budget["cut_budget_s"] + 2 * budget["drain_budget_s"] + budget["margin_s"]),
                    "original_transfer_will_expire_before_drain")
    collect.require(isinstance(value["faults"], list), "manifest_faults_invalid")
    fault_ids = set()
    for fault in value["faults"]:
        collect.require(set(fault) == {"id", "mechanism", "syscall", "errno", "target_resource", "arm", "release"}
                        and fault["id"] not in fault_ids and re.fullmatch("[A-Za-z0-9_.-]{1,128}", fault["id"]), "manifest_fault_invalid")
        fault_ids.add(fault["id"])
        for op in ("arm", "release"):
            supervisor.check_spec(fault[op], paths=False)
    if expected["hit_required"]:
        collect.require(value["case_id"] in fault_ids, "fault_case_not_declared")
    return value


def start(root, manifest, lifeline_fd):
    root = Path(root)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    collect.require(not list(root.iterdir()), "evidence_directory_not_empty", 1)
    for name in ("samples", "admissions", "faults"):
        (root / name).mkdir(mode=0o700)
    collect.publish(root / "manifest.json", manifest)
    supervisor.start(root, lifeline_fd, manifest["deadline_s"])
    try:
        actors = {}
        for actor in manifest["actors"]:
            actors[actor["id"]] = supervisor.call(root, dict(op="spawn", id=actor["id"], spec=actor["command"]))
        nonce = secrets.token_hex(16)
        begin = collect.boottime_ns()
        begin_unix = time.time_ns()
        row = collect.observed_command(root, manifest["ports"]["observe"], dict(version=1, op="start_observation", nonce=nonce,
                       run_id=manifest["run_id"], case=manifest["case_id"], original_set_digest="0" * 64, actors=actors))
        end = collect.boottime_ns()
        end_unix = time.time_ns()
        collect.schema_check(row)
        collect.require(row["nonce"] == nonce and begin <= row["observed_begin_ns"] <= row["observed_end_ns"] <= end
                        and begin_unix <= row["observed_begin_unix_ns"] <= row["observed_end_unix_ns"] <= end_unix,
                        "start_observation_not_fresh", 1)
        for key in ("source_commit", "artifact_sha", "run_id", "arm", "role", "topology"):
            collect.require(row[key] == manifest[key], "start_provenance_mismatch", 1)
        collect.require(set(row["domains"]) == set(manifest["domains"]), "start_domain_not_observed")
        counts = {family: sum(item["family"] == family for item in row["resources"]) for family in collect.PROFILE}
        collect.require(counts == collect.PROFILE, "start_original_profile_not_observed")
        by_id = {item["id"]: item for item in row["resources"]}
        for actor in manifest["actors"]:
            collect.require(all(resource_id in by_id and by_id[resource_id]["family"] == actor["family"]
                                for resource_id in actor["resource_ids"]), "started_actor_original_resource_missing")
        census = supervisor.call(root, dict(op="census"))
        collect.require(all(item["alive"] and item["native_rc"] is None for item in census.values()), "original_actor_exited_before_seal")
        originals = collect.seal(row, collect.digest(manifest))
        collect.validate_transfer_budget(row, manifest["transfer_budget"])
        collect.publish(root / "originals.json", originals)
        # The initial row is retained as a baseline, never as a completed cut.
        collect.publish(root / "start-observation.json", row)
        return dict(rc=0, verdict="GREEN", run_id=manifest["run_id"], original_set_digest=originals["original_set_digest"])
    except BaseException:
        supervisor.call(root, dict(op="stop"))
        supervisor.wait_empty(root)
        raise


def drain(root, manifest):
    root = Path(root)
    collect.require(not (root / "drains.json").exists(), "drain_already_recorded", 1)
    originals = collect.read(root / "originals.json")
    rows = []
    for ordinal in range(2):
        if ordinal:
            # sleep-fixture: the real two-second quiet interval detects late
            # terminal/history effects after all couriers/writers have joined.
            time.sleep(2)
        nonce = secrets.token_hex(16)
        begin = collect.boottime_ns()
        row = collect.observed_command(root, manifest["ports"]["drain"], dict(version=1, op="drain", ordinal=ordinal,
                          nonce=nonce, run_id=manifest["run_id"], original_set_digest=originals["original_set_digest"]))
        end = collect.boottime_ns()
        collect.validate_drain(row, originals, collect.unknown_lane(manifest), settled=manifest["expect"]["phase"] == "abandoned")
        collect.require(row["nonce"] == nonce and begin <= row["at_ns"] <= end, "drain_not_fresh", 1)
        rows.append(row)
        collect.publish(root / ("drain-" + str(ordinal) + ".json"), row)
    collect.publish(root / "drains.json", rows)
    return dict(rc=0, verdict="GREEN", independent_drains=2)


def stop(root, manifest):
    # Release injected environment before ending custody. Stopping cannot
    # create a drain receipt or erase a late product effect from judge's files.
    faults_spec = importlib.util.spec_from_file_location("upgrade_tx_faults", HERE / "upgrade_tx_faults.py")
    faults = importlib.util.module_from_spec(faults_spec)
    faults_spec.loader.exec_module(faults)
    release_errors = []
    for path in (Path(root) / "faults").glob("*.json"):
        if path.name.endswith(".arm.json"):
            continue
        row = collect.read(path)
        if row["released_ns"] == 0:
            try:
                faults.release(root, manifest, row["id"])
            except (collect.EvidenceError, supervisor.SupervisionError, OSError, ValueError):
                release_errors.append(row["id"])
    if not (Path(root) / "supervisor-empty.json").exists():
        supervisor.call(root, dict(op="stop"))
    empty = supervisor.wait_empty(root)
    collect.require(not release_errors, "fault_environment_release_failed")
    return dict(rc=0, verdict="GREEN", empty=empty["empty"], remaining=empty["remaining"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("start", "sample", "drain", "judge", "stop"))
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--nonce")
    parser.add_argument("--lifeline-fd", type=int)
    args = parser.parse_args()
    manifest = manifest_read(args.manifest)
    if args.command == "start":
        collect.require(args.lifeline_fd is not None, "runner_lifeline_required")
        result = start(args.evidence, manifest, args.lifeline_fd)
    else:
        stored = collect.read(args.evidence / "manifest.json")
        collect.require(stored == manifest, "manifest_changed_after_start", 1)
        if args.command == "sample":
            row = collect.sample(args.evidence, manifest["case_id"], args.nonce or secrets.token_hex(16))
            result = dict(rc=0, verdict="GREEN", nonce=row["nonce"], sequence=row["sequence"])
        elif args.command == "drain":
            result = drain(args.evidence, manifest)
        elif args.command == "judge":
            result = collect.judge(args.evidence, manifest)
        else:
            result = stop(args.evidence, manifest)
    print(json.dumps(result, sort_keys=True))
    return result["rc"]


if __name__ == "__main__":
    try:
        sys.exit(main())
    except collect.EvidenceError as error:
        print(json.dumps(dict(rc=error.rc, verdict=collect.VERDICTS[error.rc], reason=str(error))))
        sys.exit(error.rc)
    except (supervisor.SupervisionError, OSError, ValueError, KeyError, TypeError):
        print('{"rc":2,"verdict":"SETUP-RED","reason":"fixture_contract_unavailable"}')
        sys.exit(2)
