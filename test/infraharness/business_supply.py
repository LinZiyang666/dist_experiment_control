#!/usr/bin/env python3
"""Stage six genuine inputs for runLive, without authorizing a deployment.

The workload and its entire source closure are explicit P32 supplies. Missing
native workload/physical witnesses remain SETUP-RED; schema doubles and the
container deployment recipe cannot stand in for the executable receipt adapter.
origin: S9 internal review INFRA-SUITE-SUPPLIES F6
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

SPEC = importlib.util.spec_from_file_location("s9_release", Path(__file__).with_name("release_supply.py"))
SUPPLY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUPPLY)


def workload(source, destination):
    manifest = SUPPLY.read_json(source)
    if set(manifest) != {"Version", "Root", "Entry", "Files"} or manifest["Version"] != 1 or not manifest["Files"]:
        raise ValueError("P32: executable workload source closure required")
    origin = Path(manifest["Root"])
    if not origin.is_absolute() or origin.resolve() != origin:
        raise ValueError("P32: canonical workload source root required")
    seen = set()
    paths = []
    for row in manifest["Files"]:
        if row["Path"] in seen:
            raise ValueError("P32: duplicate workload source path")
        seen.add(row["Path"])
        paths.append(SUPPLY.verify(origin, row))
    if manifest["Entry"] not in seen:
        raise ValueError("P32: workload entry is outside source custody")
    # The caller supplies an executable adapter for --scenario/--supply/--evidence,
    # not a pre-generated business receipt. Never publish a historical receipt
    # into the fresh evidence directory or claim that input staging ran a cut.
    if not manifest["Entry"].endswith(".py"):
        raise ValueError("P32: runLive requires its Python executable adapter")
    destination.mkdir(mode=0o700)
    for row, incoming in zip(manifest["Files"], paths):
        target = SUPPLY.relative(destination, row["Path"])
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(incoming, target)
        if SUPPLY.regular(target) != row["SHA256"]:
            raise ValueError("P32: workload source changed during staging")
        target.chmod(0o755 if row["Path"] == manifest["Entry"] else 0o600)
    return SUPPLY.relative(destination, manifest["Entry"]), manifest


def stock(repo, origin, destination):
    artifact = SUPPLY.module(repo / "test/simcluster/lib/infra-artifact.py", "s9_stock")
    entries = artifact.catalog(repo / "internal/infraartifact/testdata/catalog.json", repo / "scripts/install.sh")
    destination.mkdir(mode=0o700)
    result = []
    for entry in entries:
        record = artifact.official_record(entry)
        archive = origin / entry["ArchiveName"]
        checksums = origin / record["checksums"]
        if SUPPLY.regular(archive) != entry["ArchiveSHA256"] or SUPPLY.regular(checksums) != entry["ChecksumSHA256"]:
            raise ValueError("P26: official stock archive/checksum digest mismatch")
        if artifact.CADDY.archive_checksum(checksums, entry["ArchiveName"]) != entry["OfficialChecksum"] or artifact.CADDY.digest(archive, entry["ChecksumAlgorithm"]) != entry["OfficialChecksum"]:
            raise ValueError("P26: official stock checksum algorithm/binding mismatch")
        name = ("nats-" if entry["Kind"] == 1 else "caddy-") + entry["Version"]
        path = destination / name
        if artifact.extract_binary(archive, entry["BinaryMember"], path) != entry["BinarySHA256"]:
            raise ValueError("P26: extracted official executable mismatch")
        result.append({"Kind": name, "Path": str(path), "SHA256": entry["BinarySHA256"], "Version": entry["Version"], "Race": False})
    return result


def stage(repo, root, origin, workload_source):
    entry, sources = workload(workload_source, root / "workload")
    artifacts = stock(repo, origin, root / "stock")
    with SUPPLY.offline(repo, root):
        binary = root / "tether-race"
        SUPPLY.race_binary(repo, binary, "tether")
        schema = subprocess.check_output(["go", "run", "./test/infraharness/supplymeta"], cwd=repo, timeout=120, text=True).strip()
        if len(schema) != 64 or any(c not in "0123456789abcdef" for c in schema):
            raise ValueError("P19: typed observation schema unavailable")
        builder = SUPPLY.module(repo / "build/infra-support.py", "s9_business_source")
        inputs, source_sha = builder.source_inputs(repo, "tether")
    source = SUPPLY.write(root, "business-source.json", {"Version": 1, "TetherSourceSHA256": source_sha,
        "TetherInputs": inputs, "Workload": sources, "ProducerSHA256": SUPPLY.regular(Path(__file__))})
    artifacts = [{"Kind": "tether", "Path": str(binary), "SHA256": SUPPLY.regular(binary), "Version": "current", "Race": True},
                 *artifacts, {"Kind": "workload", "Path": str(entry), "SHA256": SUPPLY.regular(entry), "Version": "current", "Race": False}]
    # Keep Go fixtureSupply field order. runLive's canonical decoder rejects
    # duplicate/unknown keys as well as a differently ordered JSON record.
    value = {"Version": 1, "Root": str(root), "Workload": str(entry), "SourceSHA256": source["SHA256"],
             "SchemaSHA256": schema, "Artifacts": artifacts}
    path = root / "business-supply.json"
    path.write_text(json.dumps(value, separators=(",", ":")))
    path.chmod(0o600)
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--stock", required=True, type=Path)
    parser.add_argument("--workload-source", type=Path)
    args = parser.parse_args()
    try:
        if args.workload_source is None:
            raise ValueError("P32: live --scenario/--supply/--evidence workload and source closure required; deployment graph/schema doubles cannot certify business")
        root = args.output.absolute()
        temporary = Path(tempfile.gettempdir())
        if not root.is_relative_to(temporary) or root == temporary or root.resolve() != root:
            raise ValueError("private canonical temporary output root required")
        if root.exists() and list(root.iterdir()):
            raise ValueError("fresh empty output required; refusing stale business supply")
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        root.chmod(0o700)
        stage(args.repo.absolute(), root, args.stock.absolute(), args.workload_source.absolute())
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        parser.exit(2, "SETUP-RED: " + str(error) + "\n")


if __name__ == "__main__":
    main()
