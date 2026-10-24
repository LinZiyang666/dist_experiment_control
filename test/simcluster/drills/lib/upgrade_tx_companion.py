"""Independent native companion selection and receipt verification for S8.

This reader follows immutable input hashes and supervised native output; a
proof label or a passing unrelated suite never certifies a scenario.
"""
import hashlib
import importlib.util
import os
from pathlib import Path
import re
import shutil

from upgrade_tx_evidence import (
    EvidenceError, HERE, boottime_ns, decode, evidence_file, index, publish,
    read, require, schema_definition, supervisor,
)

def mutation_tools():
    spec = importlib.util.spec_from_file_location("upgrade_tx_faults", HERE / "upgrade_tx_faults.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def companion_selection(contract, payloads, files):
    # origin: hot-update S8 plan §9.0 / F8. The selected files must be files
    # the named Go reader actually opens. An unrelated passing suite cannot
    # validate an extra JSON file that was never consumed by that reader.
    paths = [item["path"] for item in files]
    arm, family = contract["arm"], contract["family"]
    expected = []
    for payload in payloads:
        seal = payload.get("Seal", payload)
        round_number = payload.get("Round", seal.get("Round", 0))
        require(type(round_number) is int and 0 <= round_number < 5, "scenario_round_invalid")
        prefix = "p23/round-%d/" % round_number
        if family == "R":
            name = prefix + "rollback/" + contract["role"] + "-" + arm
            require(payload.get("Role") == contract["role"] and payload.get("Point") == arm,
                    "scenario_selected_cell_foreign")
        elif family == "H":
            lane = {"H-root": "root-broker", "H-user": "user-broker", "H-sibling": "multi-uid",
                    "H-readonly": "root-broker"}.get(arm)
            require(lane is not None, "scenario_live_reader_missing", 4)
            name = prefix + "readonly/" + lane
            require(payload.get("Case") == lane, "scenario_selected_cell_foreign")
        elif family == "W":
            name = "round-%d/%s" % (round_number, arm)
            if "OriginalDigest" in payload:
                name += ".original"
        elif family == "M":
            name = "mutants/" + arm
        elif arm == "C-driver":
            name = prefix + "driver/" + contract["role"]
        elif arm in {"C-late-writer", "C-history-rollback"}:
            entry = "watchdog" if arm == "C-late-writer" else "rollback"
            name = prefix + "late/broker-" + entry
            require(payload.get("Entry") == entry and payload.get("Role") == "broker", "scenario_selected_cell_foreign")
        elif arm == "C-gc":
            name = "round-%d/grant-gc" % round_number
            require(payload.get("Cause") == "gc", "scenario_selected_cell_foreign")
        elif family == "I":
            name = "round-%d/interactions/%s-%s-known-true" % (round_number, contract["case_id"], contract["point"])
            require(payload.get("Interaction") == contract["case_id"] and payload.get("CurrentKnown") is True,
                    "scenario_selected_cell_foreign")
        else:
            # G, C-reopen and C-owner-dead need dedicated P23/P25 live readers;
            # their similarly named unit tests cannot sign a fixture receipt.
            raise EvidenceError("scenario_live_reader_missing", 4)
        expected.append("companion-data/" + name + ".json")
    require(paths == expected and len(paths) == (2 if family == "W" else 1), "scenario_verifier_input_not_consumed")


def companion_command(root, contract, shared, go):
    expression = "^" + contract["test"] + "$"
    if contract["family"] == "M":
        expression += "/^" + contract["arm"] + "(#\\d+)?$"
    return [shared, "--", go, "test", "-json", "-count=1", "-tags=hotupdate_integration",
            "-timeout=90s", "./test/hotupdate", "-run", expression, "-args",
            "-upgrade-transaction-evidence=" + str(Path(root).resolve() / "companion-data")]


def companion_corpus(root):
    directory = Path(root) / "companion-data"
    files = []
    for path in sorted(directory.rglob("*.json")):
        relative = str(path.relative_to(root))
        reference = dict(path=relative, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        evidence_file(root, reference)
        files.append(reference)
    require(files and len(files) <= 32768, "scenario_verifier_corpus_missing")
    return files


def companion_sources():
    source = HERE.parents[3]
    return {str(p.relative_to(source)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted((source / "test/hotupdate").glob("transaction*_test.go"))}


def produce_companion(root, manifest, contract, shared, timeout_s):
    """Run the actual P23/P25 reader and publish its native handback.

    Product operation adapters write only scenario-input.json plus raw native
    files under companion-data/. They cannot supply PASS events or proof names.
    Existing unpartitioned Go readers require their complete corpus; missing
    cells stay INCOMPLETE until P23/P25 supply single-cell entry points.
    """
    if contract["family"] == "A":
        return
    root = Path(root)
    request = read(root / "scenario-input.json")
    schema_definition(request, "companion_input")
    originals = read(root / "originals.json")
    require(all(request[key] == manifest[key] for key in ("run_id", "arm"))
            and request["original_set_digest"] == originals["original_set_digest"], "scenario_input_foreign")
    payloads = [evidence_file(root, item) for item in request["files"]]
    companion_selection(contract, payloads, request["files"])
    corpus = companion_corpus(root)
    go = shutil.which("go")
    require(go and Path(shared).is_absolute() and os.access(shared, os.X_OK), "scenario_verifier_unavailable")
    command = companion_command(root, contract, shared, go)
    source = HERE.parents[3]
    verifier_sources = companion_sources()
    begin = boottime_ns()
    result = supervisor.call(root, dict(op="run", spec=dict(argv=command, cwd=str(source), timeout_s=min(timeout_s, 90))))
    end = boottime_ns()
    # Persist failures too. A compiler failure, missing cell or unrelated
    # assertion must not turn into a killed-mutant receipt.
    invocation = dict(command=command, cwd=str(source), corpus=corpus, sources=verifier_sources,
                      process=result, begin_ns=begin, end_ns=end)
    publish(root / "companion-invocation.json", invocation)
    require(companion_corpus(root) == corpus and companion_sources() == verifier_sources, "scenario_verifier_inputs_changed")
    require(result["native_rc"] == 0, "scenario_verifier_native_failure", 4)
    native_events = [decode(line) for line in result["stdout"].splitlines() if line.strip()]
    events = [dict(Action=e["Action"], Test=e["Test"]) for e in native_events
              if e["Action"] in {"run", "pass", "fail", "skip"} and e.get("Test")]
    common = {key: manifest[key] for key in ("version", "run_id", "arm", "source_commit", "artifact_sha")}
    common.update(original_set_digest=originals["original_set_digest"], evidence_run_id=request["evidence_run_id"],
                  test=contract["test"], files=request["files"], proofs=contract["proofs"].split(","))
    validation = dict(common, native_rc=result["native_rc"], begin_ns=begin, end_ns=end, process=result, events=events)
    validation["invocation"] = dict(path="companion-invocation.json",
                                     sha256=hashlib.sha256((root / "companion-invocation.json").read_bytes()).hexdigest())
    publish(root / "companion-validation.json", validation)
    reference = dict(path="companion-validation.json", sha256=hashlib.sha256((root / "companion-validation.json").read_bytes()).hexdigest())
    companion = dict(common, rc=int(contract["expected_native"]), validation=reference)
    publish(root / "scenario-companion.json", companion)
    scenario_companion(root, manifest, originals, contract)


def mutation_owner_binding(build, execution, manifest, payload):
    # origin: hot-update S8 plan §8.3 / F8. Running a second copy of the right
    # binary elsewhere cannot certify the runtime named by the P25 census.
    if build["recipe"]["build"] != "python":
        owner = payload.get("Proof", {}).get("Observation", {}).get("census", {}).get("stamp", {}).get("producer", {})
        require(execution["owner"] == {key: owner.get(key) for key in ("boot_id", "pid", "start_ticks")}
                and manifest["artifact_sha"] == build["binary_sha"], "mutation_running_owner_foreign")


def mutation_evidence(root, manifest, contract, payload, validation):
    envelope = read(Path(root) / "runner-envelope.json")
    tools = mutation_tools()
    build = tools.validate_mutation_build(envelope["mutation"], contract)
    require(build["patch_digest"] == envelope["mutation_patch_digest"]
            and payload.get("SourceDigest") == build["source_digest"] and payload.get("BinarySHA") == build["binary_sha"],
            "scenario_mutation_build_foreign")
    execution = read(Path(root) / "mutation-execution.json")
    schema_definition(execution, "mutation_execution")
    require(execution["arm"] == manifest["arm"] and execution["run_id"] == manifest["run_id"]
            and execution["source_digest"] == build["source_digest"] and execution["binary_sha"] == build["binary_sha"]
            and execution["patch_digest"] == build["patch_digest"] and payload.get("ExecutedPath") == execution["executed_path"]
            and execution["end_ns"] > execution["begin_ns"] > 0,
            "scenario_mutation_not_running")
    tools.validate_mutation_execution(build, execution, envelope["mutation"])
    mutation_owner_binding(build, execution, manifest, payload)
    # TestTransactionMutantReceipts asserts the Mxx prefix of the independent
    # Go oracle. Require its actual selected subtest, not just parent PASS.
    selected = contract["test"] + "/" + contract["arm"]
    events = validation["events"]
    require(sum(e["Action"] == "pass" and re.fullmatch(re.escape(selected) + r"(#\d+)?", e["Test"]) is not None
                for e in events) == 5, "scenario_mutation_oracle_not_reached", 4)
    return build


def scenario_companion(root, manifest, originals, contract):
    """Read the P23/P25 native verifier handback; never run it inside judge.

    scenario-companion.json indexes the actual Go evidence files and the
    supervised verifier receipt. The verifier runs the existing P23/P25 test
    on those bytes before handback; its complete Go JSON event stream, native
    status and exact input hashes must agree. This avoids reimplementing the
    Go current-export/readonly/crash oracles as weaker Python boolean checks.
    A labels-only proof list, test source or historical PASS is not evidence.
    """
    companion = read(Path(root) / "scenario-companion.json")
    schema_definition(companion, "scenario_companion")
    for key in ("run_id", "arm", "source_commit", "artifact_sha"):
        require(companion[key] == manifest[key], "scenario_companion_foreign")
    require(companion["original_set_digest"] == originals["original_set_digest"], "scenario_companion_foreign")
    proofs = companion["proofs"]
    require(len(proofs) == len(set(proofs)) and set(proofs) == set(contract["proofs"].split(",")), "scenario_companion_proof_missing", 4)
    require(companion["test"] == contract["test"], "scenario_companion_test_foreign")
    require(companion["rc"] == int(contract["expected_native"]), "scenario_native_rc_not_observed", 4)
    files = companion["files"]
    require(len({item["path"] for item in files}) == len(files), "scenario_companion_file_reused")
    payloads = [evidence_file(root, item) for item in files]
    # P23 embeds Seal; P25 fault cells keep the same seal in a .original file.
    # The test seal uses Go's exported names; its neutral upgradetx payload
    # retains the frozen snake_case JSON tags.
    seals = [item["Seal"] if "Seal" in item else item for item in payloads
             if isinstance(item, dict) and ("Seal" in item or "OriginalDigest" in item)]
    require(seals, "scenario_original_companion_missing")
    original_ids = {(item["domain"], item["id"]) for item in originals["resources"]}
    for seal in seals:
        require(seal.get("SourceCommit") == manifest["source_commit"]
                and (contract["family"] == "M" or seal.get("BinarySHA") == manifest["artifact_sha"])
                and seal.get("RunID") == companion["evidence_run_id"], "scenario_original_companion_foreign")
        resources = seal.get("Original", {}).get("expected", [])
        require(len(resources) == len(original_ids)
                and {(item.get("domain"), item.get("id")) for item in resources} == original_ids,
                "scenario_original_companion_roster_changed")
        original_map = index(originals["resources"])
        require(all(item.get("owner") == original_map[(item["domain"], item["id"])]["origin_owner"] for item in resources),
                "scenario_original_companion_owner_changed")
    for payload in payloads:
        require(isinstance(payload, dict) and payload.get("RunID", payload.get("Seal", {}).get("RunID")) == companion["evidence_run_id"], "scenario_payload_foreign")
        if "Arm" in payload:
            require(payload["Arm"] == manifest["arm"], "scenario_payload_foreign")
    if contract["family"] == "M":
        require(len(payloads) == 1 and payloads[0].get("Mutant") == manifest["arm"]
                and type(payloads[0].get("NativeRC")) is int and payloads[0]["NativeRC"] == companion["rc"]
                and payloads[0]["BinarySHA"] != seals[0].get("BinarySHA"), "scenario_mutant_not_executed")
    validation = evidence_file(root, companion["validation"])
    schema_definition(validation, "companion_validation")
    require(all(validation[key] == companion[key] for key in
                ("run_id", "arm", "source_commit", "artifact_sha", "original_set_digest", "evidence_run_id", "test", "files", "proofs")),
            "scenario_validation_foreign")
    require(validation["native_rc"] == 0, "scenario_validation_not_passed", 4)
    require(validation["begin_ns"] > 0 and validation["end_ns"] > validation["begin_ns"], "scenario_validation_interval_invalid")
    process = validation["process"]
    require(process["native_rc"] == validation["native_rc"] and process["owner"]["pid"] > 0
            and process["owner"]["start_ticks"] > 0, "scenario_validation_process_invalid")
    # Read the captured native stdout, rather than trusting a second, possibly
    # edited pass list. Package-level events do not stand in for the named Test.
    native_events = [decode(line) for line in process["stdout"].splitlines() if line.strip()]
    for event in native_events:
        schema_definition(event, "go_test_event")
    require(not any(event["Action"] in {"fail", "skip"} for event in native_events), "scenario_validation_native_failure", 4)
    actions = [dict(Action=event["Action"], Test=event.get("Test", "")) for event in native_events
               if event["Action"] in {"run", "pass", "fail", "skip"} and event.get("Test")]
    require(actions == validation["events"], "scenario_validation_stdout_changed")
    events = validation["events"]
    wanted = validation["test"]
    require(any(event["Action"] == "run" and event["Test"] == wanted for event in events)
            and sum(event["Action"] == "pass" and event["Test"] == wanted for event in events) == 1
            and not any(event["Action"] in {"fail", "skip"} for event in events)
            and events[-1]["Action"] == "pass" and events[-1]["Test"] == wanted,
            "scenario_validation_test_not_passed", 4)
    invocation = evidence_file(root, validation["invocation"])
    require(set(invocation) == {"command", "cwd", "corpus", "sources", "process", "begin_ns", "end_ns"},
            "scenario_invocation_schema_invalid")
    argv = invocation["command"]
    require(len(argv) == 13 and all(isinstance(arg, str) for arg in argv)
            and Path(argv[0]).is_absolute() and Path(argv[2]).is_absolute()
            and argv == companion_command(root, contract, argv[0], argv[2])
            and invocation["cwd"] == str(HERE.parents[3]), "scenario_verifier_command_unbound")
    require(invocation["process"] == process and invocation["begin_ns"] == validation["begin_ns"]
            and invocation["end_ns"] == validation["end_ns"] and invocation["sources"] == companion_sources(),
            "scenario_verifier_receipt_unbound")
    require(all(item in invocation["corpus"] for item in files), "scenario_verifier_input_unbound")
    for item in invocation["corpus"]:
        evidence_file(root, item)
    if contract["family"] == "M":
        mutation_evidence(root, manifest, contract, payloads[0], validation)
    return companion
