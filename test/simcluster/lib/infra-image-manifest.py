#!/usr/bin/env python3
"""Bind every Docker COPY source and final target without a self-hashing receipt.

The Dockerfile is the authority for the target set. Unknown COPY/ADD syntax,
symlinks and missing inputs fail shut. create/check run on the host; targets
and report run in the built image (or a private filesystem test root).
Requires Python 3.12+ (the Ubuntu 24.04 sim profile).
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import stat
import subprocess
import tempfile


MANIFEST_SOURCE = "vendor/infra-image-inputs.json"
MANIFEST_TARGET = "/opt/sim/infra-image-inputs.json"
# These scripts determine staging/build behavior even when they are not copied.
BUILD_INPUTS = ("simcluster", "remote.sh", "local.sh", "lib/docker.sh", "lib/stage.sh",
                "lib/stage-caddy.sh", "lib/caddy-artifact.py", "lib/stage-infra.sh",
                "lib/infra-artifact.py", "lib/infra-image-manifest.py", "lib/supervisor.sh",
                "lib/upgrade-artifacts.sh", "lib/fullstack-artifacts.py")


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate manifest field")
        result[key] = value
    return result


def read_manifest(path):
    info = Path(path).lstat()
    if not stat.S_ISREG(info.st_mode) or not 1 <= info.st_size <= 4 << 20:
        raise ValueError("image manifest must be a bounded regular file")
    value = json.loads(Path(path).read_bytes(), object_pairs_hook=unique)
    if not isinstance(value, dict) or set(value) != {"schema_version", "sources", "copies"} or type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("image manifest schema mismatch")
    if Path(path).read_bytes() != canonical(value):
        raise ValueError("image manifest is not canonical")
    return value


def relative(name):
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or str(path) != name.rstrip("/") or any(char in name for char in "*?[$\\"):
        raise ValueError("COPY source must be a literal relative path")
    return path


def absolute(name):
    path = PurePosixPath(name)
    if not path.is_absolute() or ".." in path.parts or str(path) != name.rstrip("/") or str(path) == "/":
        raise ValueError("COPY destination must be a literal absolute path")
    return path


def copies(dockerfile):
    result, manifests = [], 0
    # origin: hot-update S9 plan §9.4 / T56. Reverse-scan the actual COPY
    # declarations; a hand-maintained inventory cannot prove its own completeness.
    text = Path(dockerfile).read_text().replace("\\\n", " ")
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split(None, 1)
        instruction, args = fields[0], fields[1] if len(fields) == 2 else ""
        if instruction.upper() == "ONBUILD":
            raise ValueError("ONBUILD is outside the verified COPY contract")
        if instruction.upper() == "ADD":
            raise ValueError("ADD is outside the verified COPY contract")
        if instruction.upper() != "COPY":
            continue
        mode = None
        if args.startswith("--chmod="):
            option, args = args.split(None, 1)
            if not re.fullmatch(r"--chmod=0[0-7]{3}", option):
                raise ValueError("COPY mode must be literal octal")
            mode = int(option.split("=")[1], 8)
        if args.startswith("--"):
            raise ValueError("unsupported COPY option")
        fields = json.loads(args) if args.startswith("[") else shlex.split(args)
        if len(fields) != 2 or not all(isinstance(field, str) for field in fields):
            raise ValueError("COPY must have one source and one destination")
        source, target = fields
        relative(source)
        absolute(target)
        if source == MANIFEST_SOURCE:
            if target != MANIFEST_TARGET or mode not in (None, 0o644):
                raise ValueError("image receipt must use the fixed excluded target")
            manifests += 1
            continue
        result.append((source, target, mode))
    if manifests != 1 or not result:
        raise ValueError("Dockerfile needs one receipt COPY and nonempty inputs")
    return result


def metadata(path):
    info = Path(path).lstat()
    if stat.S_ISDIR(info.st_mode):
        return {"type": "directory", "mode": stat.S_IMODE(info.st_mode), "size": 0, "sha256": ""}
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("COPY input/target is not a regular file or directory: " + str(path))
    with Path(path).open("rb") as source:
        sha = hashlib.file_digest(source, "sha256").hexdigest()
    return {"type": "file", "mode": stat.S_IMODE(info.st_mode), "size": info.st_size, "sha256": sha}


def require_parents(root, name):
    current = Path(root)
    for part in PurePosixPath(name).parts[:-1]:
        current /= part
        if not stat.S_ISDIR(current.lstat().st_mode):
            raise ValueError("COPY input/target ancestor is not a real directory")


def snapshot(context):
    context = Path(context)
    verify_build_source(context)
    sources, rows, targets = {}, [], set()
    def source(name):
        require_parents(context, name)
        value = metadata(context / name)
        sources[name] = {"path": name, **value}
        return value
    source("Dockerfile")
    if (context / ".dockerignore").exists():
        # Ignored COPY inputs cannot be honestly supplied. The current context
        # has no ignore file; adding one requires teaching this closed parser.
        raise ValueError(".dockerignore requires explicit verified input support")
    for name in BUILD_INPUTS:
        source(name)
    for name, target, mode in copies(context / "Dockerfile"):
        info = source(name.rstrip("/"))
        if info["type"] == "directory":
            if (context / MANIFEST_SOURCE).is_relative_to(context / name):
                raise ValueError("receipt cannot be included through a directory COPY")
            paths = [context / name] + sorted((context / name).rglob("*"))
            if len(paths) == 1:
                raise ValueError("COPY directory must not be empty")
            pairs = [(path.relative_to(context).as_posix(), (PurePosixPath(target) / path.relative_to(context / name)).as_posix()) for path in paths]
        else:
            if target.endswith("/"):
                target += PurePosixPath(name).name
            pairs = [(name, target)]
        for origin, destination in pairs:
            value = source(origin)
            if destination in targets:
                raise ValueError("overlapping COPY destination")
            targets.add(destination)
            rows.append({"source": origin, "target": destination,
                         **value, "mode": value["mode"] if mode is None else mode})
    return {"schema_version": 1, "sources": [sources[name] for name in sorted(sources)],
            "copies": sorted(rows, key=lambda row: row["target"])}


def verify_build_source(context):
    """When the checkout is present, stale source cannot hide behind staged bytes."""
    receipt = context / "vendor/infra/build-source.json"
    info = receipt.lstat()
    if not stat.S_ISREG(info.st_mode) or not 1 <= info.st_size <= 4 << 20:
        raise ValueError("build source receipt must be a bounded regular file")
    value = json.loads(receipt.read_bytes(), object_pairs_hook=unique)
    if set(value) != {"schema_version", "inputs"} or type(value["schema_version"]) is not int or value["schema_version"] != 1 or not value["inputs"]:
        raise ValueError("missing build source identity")
    repo = context.parent.parent
    if not (repo / "go.mod").is_file():
        return  # Remote context carries the supplier's receipt, not another checkout.
    names = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "--",
                                     "Makefile", "build/goreleaser.yaml", "build/infra-support.py", "go.mod", "go.sum", "scripts/install.sh", "cmd", "internal"],
                                    cwd=repo, timeout=10, text=True).splitlines()
    if sorted(set(names)) != [row["path"] for row in value["inputs"]]:
        raise ValueError("staged source input set changed; restage before building")
    for row in value["inputs"]:
        name = relative(row["path"]).as_posix()
        require_parents(repo, name)
        if row != {"path": name, **metadata(repo / name)}:
            raise ValueError("stale staged build source: " + name)


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".infra-image-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(canonical(value))
            output.flush()
            os.fchmod(output.fileno(), 0o644)
            os.fsync(output.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def verify_targets(value, root, dockerfile):
    declared = copies(dockerfile)
    rows = value["copies"]
    seen = set()
    for row in rows:
        if set(row) != {"source", "target", "type", "mode", "size", "sha256"} or row["target"] in seen:
            raise ValueError("duplicate/invalid image target row")
        relative(row["source"])
        target = absolute(row["target"])
        seen.add(row["target"])
        require_parents(root, target.as_posix().lstrip("/"))
        if metadata(Path(root) / target.as_posix().lstrip("/")) != {key: row[key] for key in ("type", "mode", "size", "sha256")}:
            raise ValueError("stale image target: " + row["target"])
    sources = {row["path"]: row for row in value["sources"]}
    if len(sources) != len(value["sources"]):
        raise ValueError("duplicate image source row")
    # Every declared source/target pair must be represented, including directory
    # contents. Neither a missing nor an invented target can sign image readiness.
    expected = []
    for source, target, mode in declared:
        origin = source.rstrip("/")
        if origin not in sources:
            raise ValueError("COPY source missing from image receipt")
        is_directory = sources[origin]["type"] == "directory"
        names = sorted(name for name in sources if name == origin or (is_directory and name.startswith(origin + "/")))
        for name in names:
            destination = (PurePosixPath(target) / PurePosixPath(name).relative_to(origin)).as_posix() if is_directory else target
            if not is_directory and destination.endswith("/"):
                destination += PurePosixPath(origin).name
            value_row = sources[name]
            expected.append({"source": name, "target": destination,
                             **{key: value_row[key] for key in ("type", "mode", "size", "sha256")},
                             "mode": value_row["mode"] if mode is None else mode})
    if sorted(expected, key=lambda row: row["target"]) != rows:
        raise ValueError("Docker COPY and image target inventory disagree")
    docker_row = sources.get("Dockerfile")
    if docker_row != {"path": "Dockerfile", **metadata(dockerfile)}:
        raise ValueError("Dockerfile identity mismatch")


def compare_report(context, report):
    expected = snapshot(context)
    if read_manifest(report) != expected:
        raise ValueError("stale image COPY/build inputs; restage and rebuild")
    return expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("create", "check"):
        child = sub.add_parser(command)
        child.add_argument("context")
        child.add_argument("manifest")
    child = sub.add_parser("targets")
    child.add_argument("manifest")
    child.add_argument("--root", default="/")
    child.add_argument("--dockerfile", default="/opt/sim/Dockerfile")
    child = sub.add_parser("report")
    child.add_argument("manifest")
    args = parser.parse_args()
    try:
        if args.command == "create":
            write(args.manifest, snapshot(args.context))
        elif args.command == "check":
            compare_report(args.context, args.manifest)
        else:
            value = read_manifest(args.manifest)
            verify_targets(value, getattr(args, "root", "/"), getattr(args, "dockerfile", "/opt/sim/Dockerfile"))
            if args.command == "report":
                print(canonical(value).decode(), end="")
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        parser.exit(2, f"infra-image-manifest: {error}\n")


if __name__ == "__main__":
    main()
