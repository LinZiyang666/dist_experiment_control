#!/usr/bin/env python3
"""Controls use genuine prebuilt history; no control receipt signs a live cut."""
import json
from pathlib import Path
import shutil
import sys
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_inputs as inputs
import release_supply as supply


def rejected(fn):
    try:
        fn()
    except (ValueError, OSError, inputs.subprocess.SubprocessError):
        return
    raise AssertionError("historical producer accepted a corrupt source identity")


def main():
    repo, origin, root = map(Path, sys.argv[1:])
    copied = root / "copied"
    inputs.historical(repo, origin, copied)
    for name in inputs.HISTORICAL:
        old = supply.read_json(origin / (name + "-source.json"))
        new = inputs.verify_previous(repo, copied, name)
        expected = dict(old, source_archive=new["source_archive"])
        assert new == expected and Path(new["source_archive"]).parent == copied
        assert supply.regular(copied / name) == old["binary_sha256"]
    # Mutation controls exercise the same P22 verifier as the actual importer.
    # Rehashing a modified binary in a copied manifest cannot move the pinned
    # git tree/archive/buildinfo or the fixed v0.6.0 identity.
    name = "tether-prev"
    path = copied / (name + "-source.json")
    original = path.read_bytes()
    for field, value in (("commit", "f" * 40), ("tag", "v0.6.1"),
                         ("tree_digest", "f" * 64), ("archive_sha256", "f" * 64),
                         ("binary_sha256", "f" * 64)):
        altered = json.loads(original)
        altered[field] = value
        path.write_bytes(supply.canonical(altered))
        rejected(lambda: inputs.verify_previous(repo, copied, name))
        path.write_bytes(original)
    archive = Path(json.loads(original)["source_archive"])
    with archive.open("ab") as output:
        output.write(b"changed source archive")
    rejected(lambda: inputs.verify_previous(repo, copied, name))
    source_archive = Path(supply.read_json(origin / (name + "-source.json"))["source_archive"])
    shutil.copyfile(source_archive, archive)
    inputs.verify_previous(repo, copied, name)
    copy_bound = supply.copy_bound
    def changed_after_copy(source, record, target):
        copy_bound(source, record, target)
        if target.name == "tether-prev":
            with target.open("ab") as output:
                output.write(b"changed after verified copy")
    with mock.patch.object(supply, "copy_bound", side_effect=changed_after_copy):
        rejected(lambda: inputs.historical(repo, origin, root / "copy-race", names=("tether-prev",)))
    # A valid P22 tether input has no helper/launcher. No bridge to S9 support
    # archives is allowed to infer those entries or use a linker-only variant.
    assert not (copied / "tether-infra-helper").exists()
    assert not (copied / "tether-infra-launch").exists()
    recursive = root / "recursive-input"
    recursive.mkdir()
    rejected(lambda: inputs.copy_tree(recursive, recursive / "output"))
    assert not (recursive / "output").exists(), "copy created its own recursive input"
    guard = supply.module(repo / "test/simcluster/lib/infra-image-manifest.py", "source_control_guard")
    names = inputs.subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "--",
        "Makefile", "build/goreleaser.yaml", "build/infra-support.py", "go.mod", "go.sum", "scripts/install.sh", "cmd", "internal"],
        cwd=repo, text=True).splitlines()
    current = {"schema_version": 1, "inputs": [{"path": name, **guard.metadata(repo / name)} for name in sorted(set(names))]}
    receipt = root / "build-source.json"
    receipt.write_bytes(supply.canonical(current))
    inputs.verify_current_source(repo, receipt)
    for defect in ("missing", "mode", "digest"):
        altered = json.loads(supply.canonical(current))
        if defect == "missing":
            altered["inputs"].pop()
        elif defect == "mode":
            altered["inputs"][0]["mode"] ^= 0o100
        else:
            altered["inputs"][0]["sha256"] = "f" * 64
        receipt.write_bytes(supply.canonical(altered))
        rejected(lambda: inputs.verify_current_source(repo, receipt))
    # Check the private context assembler's own source comparison, even when
    # its output is not nested under a checkout (guard's remote-context case).
    context = root / "context"
    context.mkdir()
    vendor = context / "vendor"
    shutil.copytree(copied, vendor)
    (vendor / "infra").mkdir()
    supply.write(vendor / "infra", "build-source.json", {"schema_version": 1, "inputs": []})
    # Yesterday's copied success pointer must disappear even when today's
    # independently validated source set or one COPY dependency is incomplete.
    supply.write(vendor, "infra-image-inputs.json", {"schema_version": 1, "sources": [], "copies": []})
    rejected(lambda: inputs.context(repo, vendor, origin, root / "incomplete-context"))
    assert not (root / "incomplete-context/vendor/infra-image-inputs.json").exists()
    print("historical custody controls passed")


if __name__ == "__main__":
    main()
