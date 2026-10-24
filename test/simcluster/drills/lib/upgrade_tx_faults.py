#!/usr/bin/env python3
"""Real environment injection; product effects and hit proofs stay independent.

arm/release consume only predeclared command descriptors. No phase marker signs
a syscall hit: collect compares the product/kernel reader's actual interval,
errno, owner incarnation, cut and resource against the injection receipt.
kernel is a Linux backend for candidate kill/stop, prlimit and unsealed transport
truncation. Namespace mounts/network/ptrace and authority/leader/competing CLI
use explicit P25 adapters with the same protocol; absence never means success.
"""
import argparse
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import resource
import signal
import sys
import tarfile

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("upgrade_tx_collect", HERE / "upgrade_tx_collect.py")
collect = sys.modules.get(spec.name)
if collect is None:
    collect = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = collect
    spec.loader.exec_module(collect)
supervisor = collect.supervisor
MECHANISMS = {"candidate_exit", "syscall_delay", "prlimit", "transport_truncate", "ptrace", "namespace_readonly",
              "tmpfs_quota", "noexec", "network_namespace", "leader_transfer", "revoke_recreate", "competing_cli"}


def mutation_recipe(row):
    # origin: hot-update S8 plan §8.3 / F8. A label and arbitrary patch digest
    # cannot identify a product defect. Each arm fixes the complete edit.
    recipe = collect.decode(row["mutation"])
    collect.require(set(recipe) == {"file", "symbol", "before", "after", "build", "oracle"}
                    and recipe["oracle"] == row["arm"] + ":" and recipe["before"] != recipe["after"],
                    "mutation_recipe_invalid")
    path = Path(recipe["file"])
    collect.require(not path.is_absolute() and ".." not in path.parts and
                    ((str(path).startswith("internal/") and path.suffix == ".go" and not path.name.endswith("_test.go")
                      and recipe["build"] == "./cmd/tether") or
                     (str(path).startswith("test/simcluster/drills/lib/") and path.suffix == ".py"
                      and recipe["build"] == "python")), "mutation_product_target_invalid")
    return recipe


def mutated_source(source, recipe):
    # Require the edit inside the named function, not just somewhere in a
    # file with that name (comments and another symbol cannot sign the arm).
    pattern = (r"(?m)^def (\w+)\(" if recipe["file"].endswith(".py")
               else r"(?m)^func (?:\([^\n]+?\) )?(\w+)\(")
    matches = list(re.finditer(pattern, source))
    spans = [(match.start(), matches[n + 1].start() if n + 1 < len(matches) else len(source))
             for n, match in enumerate(matches) if match[1] == recipe["symbol"]]
    collect.require(len(spans) == 1, "mutation_symbol_missing")
    begin, end = spans[0]
    body = source[begin:end]
    collect.require(body.count(recipe["before"]) == 1, "mutation_anchor_changed")
    return source[:begin] + body.replace(recipe["before"], recipe["after"], 1) + source[end:]


def source_archive(path, expected_sha):
    raw = Path(path).read_bytes()
    collect.require(hashlib.sha256(raw).hexdigest() == expected_sha, "mutation_archive_changed")
    files = {}
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        for member in archive:
            name = Path(member.name)
            collect.require(not name.is_absolute() and ".." not in name.parts and ".git" not in name.parts,
                            "mutation_archive_path_invalid")
            if member.isdir():
                continue
            collect.require(member.isfile() and member.name not in files and member.size <= collect.MAX_BYTES,
                            "mutation_archive_member_invalid")
            files[member.name] = archive.extractfile(member).read()
    collect.require("go.mod" in files and files, "mutation_archive_root_missing")
    return files


def source_inventory(files):
    return {name: hashlib.sha256(raw).hexdigest() for name, raw in sorted(files.items())}


def require_compiled_change(binary_sha, baseline_sha):
    collect.require(binary_sha != baseline_sha, "mutation_not_in_compiled_output", 4)


def build_mutation(root, row, artifact, go, shared):
    """Build one declared edit from P22's archive, outside every checkout.

    This never patches a checkout, test oracle, journal or original roster.
    The unmodified collector later verifies this receipt and independently
    observes the running image; a successful build is not a killed mutant.
    """
    root = Path(root).resolve()
    collect.require(not root.is_relative_to(HERE.parents[3]) and not root.exists(), "mutation_build_root_invalid")
    recipe = mutation_recipe(row)
    files = source_archive(artifact["source_archive"], artifact["archive_sha256"])
    before = source_inventory(files)
    baseline_files = dict(files)
    files[recipe["file"]] = mutated_source(files[recipe["file"]].decode(), recipe).encode()
    after = source_inventory(files)
    root.mkdir(parents=True, mode=0o700)
    source = root / "source"
    for directory, inputs in ((source, files), (root / "baseline", baseline_files)):
        for name, raw in inputs.items():
            path = directory / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
    output = root / "candidate"
    baseline_output = root / "baseline-candidate"
    if recipe["build"] == "python":
        # The script is the mutated subject. Its interpreter remains trusted;
        # the command's argv and the script bytes must both be attested later.
        output.write_bytes(files[recipe["file"]])
        baseline_output.write_bytes(baseline_files[recipe["file"]])
        process, command = None, []
        baseline_process, baseline_command = None, []
    else:
        command = [shared, "--", go, "-C", str(source), "build", "-trimpath", "-o", str(output), recipe["build"]]
        baseline_command = [shared, "--", go, "-C", str(root / "baseline"), "build", "-trimpath", "-o", str(baseline_output), recipe["build"]]
        processes = []
        for argv in (baseline_command, command):
            process = supervisor.run(argv, 150)
            collect.require(process["native_rc"] == 0, "mutation_build_failed", 4)
            processes.append(process)
        baseline_process, process = processes
    output.chmod(0o500)
    record = dict(version=1, arm=row["arm"], recipe=recipe, artifact=artifact,
                  before=before, after=after, source_digest=collect.digest(after),
                  patch_digest=collect.digest(dict(recipe=recipe, before=before[recipe["file"]], after=after[recipe["file"]])),
                  binary_sha=hashlib.sha256(output.read_bytes()).hexdigest(), command=command, process=process)
    record.update(baseline_sha=hashlib.sha256(baseline_output.read_bytes()).hexdigest(),
                  baseline_command=baseline_command, baseline_process=baseline_process)
    require_compiled_change(record["binary_sha"], record["baseline_sha"])
    collect.publish(root / "mutation-build.json", record)
    return record


def validate_mutation_build(path, row):
    root = Path(path).resolve().parent
    record = collect.read(path)
    collect.require(set(record) == {"version", "arm", "recipe", "artifact", "before", "after", "source_digest",
                                   "patch_digest", "binary_sha", "command", "process",
                                   "baseline_sha", "baseline_command", "baseline_process"}
                    and type(record["version"]) is int and record["version"] == 1 and record["arm"] == row["arm"],
                    "mutation_build_schema_invalid")
    recipe = mutation_recipe(row)
    collect.require(record["recipe"] == recipe, "mutation_wrong_symbol_or_edit")
    files = source_archive(record["artifact"]["source_archive"], record["artifact"]["archive_sha256"])
    before = source_inventory(files)
    files[recipe["file"]] = mutated_source(files[recipe["file"]].decode(), recipe).encode()
    after = source_inventory(files)
    actual = {str(p.relative_to(root / "source")): p.read_bytes() for p in (root / "source").rglob("*") if p.is_file()}
    baseline = {str(p.relative_to(root / "baseline")): p.read_bytes() for p in (root / "baseline").rglob("*") if p.is_file()}
    collect.require(record["before"] == before and record["after"] == after and source_inventory(actual) == after
                    and source_inventory(baseline) == before and before != after
                    and record["source_digest"] == collect.digest(after), "mutation_source_not_built")
    collect.require(record["patch_digest"] == collect.digest(dict(recipe=recipe, before=before[recipe["file"]], after=after[recipe["file"]])),
                    "mutation_patch_not_bound")
    collect.require(record["binary_sha"] == hashlib.sha256((root / "candidate").read_bytes()).hexdigest()
                    and record["binary_sha"] != record["artifact"]["binary_sha256"], "mutation_binary_unchanged")
    collect.require(record["baseline_sha"] == hashlib.sha256((root / "baseline-candidate").read_bytes()).hexdigest()
                    and record["binary_sha"] != record["baseline_sha"], "mutation_baseline_not_distinct")
    if recipe["build"] == "python":
        collect.require((root / "candidate").read_bytes() == files[recipe["file"]] and record["command"] == []
                        and (root / "baseline-candidate").read_bytes() == (root / "baseline" / recipe["file"]).read_bytes()
                        and record["process"] is None and record["baseline_command"] == [] and record["baseline_process"] is None,
                        "mutation_script_not_built")
    else:
        argv = record["command"]
        collect.require(len(argv) == 10 and argv[1:2] == ["--"] and argv[3:] ==
                        ["-C", str(root / "source"), "build", "-trimpath", "-o", str(root / "candidate"), recipe["build"]]
                        and all(Path(argv[i]).is_absolute() for i in (0, 2)), "mutation_build_command_changed")
        baseline_argv = list(argv)
        baseline_argv[4], baseline_argv[8] = str(root / "baseline"), str(root / "baseline-candidate")
        collect.require(record["baseline_command"] == baseline_argv, "mutation_baseline_flags_changed")
        collect.schema_definition(record["process"], "verifier_process")
        collect.schema_definition(record["baseline_process"], "verifier_process")
        collect.require(record["process"]["native_rc"] == record["baseline_process"]["native_rc"] == 0,
                        "mutation_build_not_successful", 4)
    return record


def validate_mutation_execution(build, execution, build_path):
    if build["recipe"]["build"] == "python":
        subject = Path(build_path).resolve().parent / "source" / build["recipe"]["file"]
        collect.require(execution["argv"][1:4] == ["-I", "-B", str(subject)]
                        and hashlib.sha256(subject.read_bytes()).hexdigest() == build["binary_sha"],
                        "mutation_script_not_executed", 4)
    else:
        collect.require(execution["executable_sha"] == build["binary_sha"], "mutation_binary_not_executed", 4)


def observe_mutation(root, manifest, row, build_path):
    # origin: hot-update S8 plan §8.3 / F8. The adapter supplies only a PID
    # incarnation. Read /proc ourselves while it is live; a supplied running
    # SHA (or a command that merely mentions the candidate) is not execution.
    root = Path(root)
    build = validate_mutation_build(build_path, row)
    owner = collect.read(root / "mutation-owner.json")
    collect.schema_definition(owner, "process_owner")
    collect.require(supervisor.same_process(owner), "mutation_owner_not_live", 4)
    begin = collect.boottime_ns()
    proc = Path("/proc") / str(owner["pid"])
    executable = hashlib.sha256((proc / "exe").read_bytes()).hexdigest()
    argv = (proc / "cmdline").read_bytes().decode().rstrip("\0").split("\0")
    validate_mutation_execution(build, dict(argv=argv, executable_sha=executable), build_path)
    collect.require(supervisor.same_process(owner), "mutation_owner_changed", 4)
    result = dict(version=1, arm=row["arm"], run_id=manifest["run_id"], owner=owner,
                  source_digest=build["source_digest"], binary_sha=build["binary_sha"],
                  patch_digest=build["patch_digest"], executable_sha=executable, argv=argv,
                  executed_path=(str(Path(build_path).resolve().parent / "source" / build["recipe"]["file"])
                                 if build["recipe"]["build"] == "python" else os.readlink(proc / "exe")),
                  begin_ns=begin, end_ns=collect.boottime_ns())
    collect.publish(root / "mutation-execution.json", result)
    return result


def declared(manifest, case_id):
    values = [row for row in manifest["faults"] if row["id"] == case_id]
    collect.require(len(values) == 1, "fault_case_not_declared")
    row = values[0]
    collect.require(row["mechanism"] in MECHANISMS and type(row["errno"]) is int and 0 <= row["errno"] <= 4095,
                    "fault_mechanism_invalid")
    return row


def action(root, fault, operation, target, restore=None):
    payload = dict(version=1, op=operation, mechanism=fault["mechanism"], target=target, restore=restore)
    result = supervisor.call(root, dict(op="run", spec=fault[operation], input=payload))
    collect.require(result["native_rc"] == 0, "fault_injector_native_failure", 4)
    ack = collect.decode(result["stdout"])
    collect.require(set(ack) == {"version", "applied", "mechanism", "target", "restore"}
                    and type(ack["version"]) is int and ack["version"] == 1 and ack["applied"] is True
                    and ack["mechanism"] == fault["mechanism"] and ack["target"] == target, "fault_environment_ack_missing")
    restore_value = ack["restore"]
    collect.require(restore_value is None or (isinstance(restore_value, list) and len(restore_value) == 2
                    and all(type(value) is int for value in restore_value))
                    or restore_value == {"irreversible": True}, "fault_restore_not_public")
    return ack["restore"]


def arm(root, manifest, case_id):
    root = Path(root)
    fault = declared(manifest, case_id)
    path = root / "faults" / (case_id + ".json")
    collect.require(not path.exists(), "fault_already_armed", 1)
    nonce = __import__("secrets").token_hex(16)
    row = collect.sample(root, case_id, nonce)
    resources = [item for item in row["resources"] if item["id"] == fault["target_resource"]]
    collect.require(len(resources) == 1, "fault_target_not_in_scope")
    target = dict(owner=resources[0]["current_owner"], resource_id=resources[0]["id"], cut_id=row["cut_id"],
                  tx_id=row["tx_id"], generation=row["generation"])
    begin = collect.boottime_ns()
    record = dict(version=1, id=case_id, mechanism=fault["mechanism"], syscall=fault["syscall"], errno=fault["errno"],
                  target=target, restore=None, armed_ns=begin, applied_ns=0, released_ns=0)
    # origin: hot-update S8 plan §8.2 / §9.0.
    # Durable intent precedes the environment side effect. A failed ACK cannot
    # make an injected namespace/limit disappear from stop's cleanup inventory.
    collect.publish(path, record)
    record["restore"] = action(root, fault, "arm", target)
    record["applied_ns"] = collect.boottime_ns()
    supervisor.write_json(path, record)
    return dict(rc=0, verdict="GREEN", case=case_id, applied=True, hit_proven=False)


def release(root, manifest, case_id):
    root = Path(root)
    fault = declared(manifest, case_id)
    path = root / "faults" / (case_id + ".json")
    row = collect.read(path)
    collect.require(row["released_ns"] == 0, "fault_already_released", 1)
    action(root, fault, "release", row["target"], row["restore"])
    row["released_ns"] = collect.boottime_ns()
    # The immutable arm receipt stays separate; only the environment lifecycle
    # projection changes. Neither file can create a kernel hit observation.
    collect.publish(path.with_name(case_id + ".arm.json"), collect.read(path))
    supervisor.write_json(path, row)
    return dict(rc=0, verdict="GREEN", case=case_id, released=True)


def kernel(request, limit, fd_number, truncate_bytes):
    collect.require(sys.platform == "linux" and request["version"] == 1, "native_fault_backend_required")
    owner = request["target"]["owner"]
    identity = {key: owner[key] for key in ("pid", "start_ticks", "boot_id")}
    mechanism, op = request["mechanism"], request["op"]
    collect.require(op in {"arm", "release"}, "fault_operation_invalid")
    if op == "release" and mechanism == "candidate_exit":
        same = supervisor.same_process(identity)
        zombie = same and Path(f"/proc/{owner['pid']}/stat").read_text().rsplit(")", 1)[1].split()[0] == "Z"
        collect.require(not same or zombie, "candidate_death_not_observed", 4)
        return dict(version=1, applied=True, mechanism=mechanism, target=request["target"], restore={"irreversible": True})
    # origin: hot-update S8 plan §8.2 / M19. Never signal a recycled PID or
    # infer the current owner from an old process-name match.
    collect.require(supervisor.same_process(identity), "fault_target_incarnation_changed", 4)
    restore = request["restore"]
    if mechanism in {"candidate_exit", "syscall_delay"}:
        fd = supervisor.pidfd_open(owner["pid"])
        try:
            collect.require(supervisor.same_process(identity), "fault_target_incarnation_changed", 4)
            if op == "arm":
                number = signal.SIGKILL if mechanism == "candidate_exit" else signal.SIGSTOP
                signal.pidfd_send_signal(fd, number)
                if mechanism == "candidate_exit":
                    restore = {"irreversible": True}
            elif mechanism == "syscall_delay":
                signal.pidfd_send_signal(fd, signal.SIGCONT)
        finally:
            os.close(fd)
    elif mechanism == "prlimit":
        if op == "arm":
            collect.require(type(limit) is int and limit > 0, "prlimit_soft_limit_required")
            previous = resource.prlimit(owner["pid"], resource.RLIMIT_NOFILE)
            collect.require(limit <= previous[0], "prlimit_must_reduce_soft_limit")
            resource.prlimit(owner["pid"], resource.RLIMIT_NOFILE, (limit, previous[1]))
            restore = list(previous)
        else:
            collect.require(isinstance(restore, list) and len(restore) == 2, "prlimit_restore_missing")
            resource.prlimit(owner["pid"], resource.RLIMIT_NOFILE, tuple(restore))
    elif mechanism == "transport_truncate":
        if op == "release":
            collect.require(restore == {"irreversible": True}, "transport_injection_not_recorded")
            return dict(version=1, applied=True, mechanism=mechanism, target=request["target"], restore=restore)
        collect.require(fd_number is not None and truncate_bytes is not None
                        and fd_number >= 3 and truncate_bytes >= 0, "transport_truncate_spec_required")
        path = Path(f"/proc/{owner['pid']}/fd/{fd_number}")
        collect.require("memfd:" in os.readlink(path), "transport_must_be_memfd")
        fd = os.open(path, os.O_RDWR | os.O_CLOEXEC)
        try:
            collect.require(supervisor.same_process(identity), "fault_target_incarnation_changed", 4)
            os.ftruncate(fd, truncate_bytes)
            restore = {"irreversible": True}
        finally:
            os.close(fd)
    else:
        raise collect.EvidenceError("external_fault_backend_required", 4)
    return dict(version=1, applied=True, mechanism=mechanism, target=request["target"], restore=restore)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("arm", "release", "kernel", "build-mutation"))
    parser.add_argument("--case")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--fd", type=int)
    parser.add_argument("--truncate-bytes", type=int)
    parser.add_argument("--go")
    parser.add_argument("--shared-supervisor")
    args = parser.parse_args()
    if args.command == "build-mutation":
        collect.require(args.case and args.manifest and args.evidence and args.go and args.shared_supervisor,
                        "mutation_build_arguments_missing")
        artifact = collect.read(args.manifest)
        with (HERE / "upgrade_tx_arms.tsv").open() as stream:
            import csv
            rows = [r for r in csv.DictReader((s for s in stream if not s.startswith("#")), delimiter="\t") if r["arm"] == args.case]
        collect.require(len(rows) == 1 and rows[0]["family"] == "M", "mutation_arm_not_declared")
        result = build_mutation(args.evidence, rows[0], artifact, args.go, args.shared_supervisor)
    elif args.command == "kernel":
        result = kernel(collect.decode(sys.stdin.buffer.read(collect.MAX_BYTES + 1)), args.limit, args.fd, args.truncate_bytes)
    else:
        collect.require(args.case and args.manifest and args.evidence, "fault_cli_contract_missing")
        manifest = collect.read(args.manifest)
        collect.require(manifest == collect.read(args.evidence / "manifest.json"), "manifest_changed_after_start", 1)
        result = arm(args.evidence, manifest, args.case) if args.command == "arm" else release(args.evidence, manifest, args.case)
    print(json.dumps(result, sort_keys=True))
    return result.get("rc", 0)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except collect.EvidenceError as error:
        print(json.dumps(dict(rc=error.rc, verdict=collect.VERDICTS[error.rc], reason=str(error))))
        sys.exit(error.rc)
    except (supervisor.SupervisionError, OSError, ValueError, KeyError, TypeError):
        print('{"rc":2,"verdict":"SETUP-RED","reason":"fault_contract_unavailable"}')
        sys.exit(2)
