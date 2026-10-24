#!/usr/bin/env python3
"""Import genuine S1/P22 historical inputs and assemble a private COPY context.

The shared P22 verifier is authoritative for tag, tree, archive and buildinfo.
No historical binary is executed or rebuilt here. A previous tether release
does not supply the separate S9 helper/launcher generation. Missing image inputs
remain an error, with the actual COPY source named, before publishing a receipt.
origin: S9 internal review INFRA-SUITE-SUPPLIES F4 / hot-update S9 plan §9.4
"""
import argparse
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_supply as supply

FIXED_TAG = "v0.6.0"
FIXED_COMMIT = "a3431a12be3cf947cc797c28fea93382fa68472d"
HISTORICAL = ("tether-prev", "tether-dynamic-prev", "tether-flow-legacy")


def verify_previous(repo, vendor, name):
    if name not in HISTORICAL:
        raise ValueError("S1/P22: unsupported historical component")
    record = supply.read_json(vendor / (name + "-source.json"))
    tag = record["tag"]
    if name != "tether-dynamic-prev" and (tag != FIXED_TAG or record["commit"] != FIXED_COMMIT):
        raise ValueError("S1/P22: fixed previous source differs from v0.6.0")
    # The shell entry reuses expected_source/cache_ok, including git archive,
    # embedded buildinfo and the actual source archive. It has no fallback build.
    script = '''set -eu
SIM_REPO=$1 SIM_VENDOR=$2
export SIM_REPO SIM_VENDOR
. "$SIM_REPO/test/simcluster/lib/stage.sh"
if [ "$3" = tether-dynamic-prev ]; then
    expected=$(sim_dynamic_prev_tag)
    [ "$expected" = "$4" ] || exit 2
fi
sim_release_source_ok "$3" "$4" "$5"
'''
    # Resolve the workspace's already selected toolchain before closing network
    # access. A generic go shim may try to re-verify an otherwise cached toolchain
    # with GOSUMDB=off; the verifier must use the actual compiler's bin directory.
    goroot = subprocess.check_output(["go", "env", "GOROOT"], cwd=repo, timeout=30, text=True).strip()
    environment = dict(os.environ, GOPROXY="off", GOSUMDB="off", GOWORK="off",
                       PATH=goroot + "/bin:/usr/bin:/bin")
    subprocess.run(["sh", "-c", script, "s9-previous-input", str(repo), str(vendor), name,
                    tag, FIXED_COMMIT if name != "tether-dynamic-prev" else ""],
                   env=environment, check=True, timeout=90)
    binary = vendor / name
    if supply.regular(binary) != record["binary_sha256"]:
        raise ValueError("S1/P22: historical executable changed during verification")
    archive = Path(record["source_archive"])
    if not archive.is_absolute() or supply.regular(archive) != record["archive_sha256"]:
        raise ValueError("S1/P22: historical source archive changed during verification")
    return record


def historical(repo, origin, destination, names=HISTORICAL):
    # Validate the entire requested set first. Publication of one genuine input
    # must not hide the absence of a different historical role in the context.
    records = [(name, verify_previous(repo, origin, name)) for name in names]
    destination.mkdir(mode=0o700, parents=True, exist_ok=True)
    for name, record in records:
        source = origin / name
        supply.copy_bound(origin, supply.pin(origin, source), destination / name)
        (destination / name).chmod(0o755)
        archive = Path(record["source_archive"])
        target = destination / (name + "-" + record["archive_sha256"] + ".tar")
        supply.copy_bound(archive.parent, supply.pin(archive.parent, archive), target)
        target.chmod(0o600)
        # Only the custody location changes. The immutable source/build identity
        # remains the exact P22 record and is independently verified again below.
        copied = dict(record, source_archive=str(target))
        supply.write(destination, name + "-source.json", copied)
        verify_previous(repo, destination, name)
    return records


def copy_tree(source, destination):
    # Otherwise the freshly created output becomes another input on the first
    # directory walk, recursively copying itself before any receipt validation.
    if destination.absolute().is_relative_to(source.absolute()):
        raise ValueError("P26: image output must be outside its input tree")
    if source.is_symlink() or not source.is_dir():
        raise ValueError("P26: real input directory required")
    destination.mkdir(mode=source.stat().st_mode & 0o777, parents=True, exist_ok=True)
    for path in sorted(source.iterdir()):
        target = destination / path.name
        if path.is_symlink():
            raise ValueError("P26: image input contains a symlink")
        if path.is_dir():
            copy_tree(path, target)
        else:
            supply.copy_bound(source, supply.pin(source, path), target)
            target.chmod(path.stat().st_mode & 0o777)


def context(repo, vendor, previous, root):
    guard = supply.module(repo / "test/simcluster/lib/infra-image-manifest.py", "s9_copy_guard")
    original = repo / "test/simcluster"
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    copy_tree(vendor, root / "vendor")
    # A copied old success pointer must not survive any later source, historical
    # or COPY refusal. Only the final independent checks may publish this name.
    (root / guard.MANIFEST_SOURCE).unlink(missing_ok=True)
    # Historical receipts are imported independently, never inferred from a
    # current archive's version. Remove only these private copied destinations.
    for name in HISTORICAL:
        (root / "vendor" / name).unlink(missing_ok=True)
        (root / "vendor" / (name + "-source.json")).unlink(missing_ok=True)
    historical(repo, previous, root / "vendor")
    names = {"Dockerfile", *guard.BUILD_INPUTS}
    names.update(name.rstrip("/") for name, _, _ in guard.copies(original / "Dockerfile")
                 if not name.startswith("vendor/"))
    for name in sorted(names):
        source, target = supply.relative(original, name), supply.relative(root, name)
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            copy_tree(source, target)
        else:
            supply.copy_bound(original, supply.pin(original, source), target)
            target.chmod(source.stat().st_mode & 0o777)
    # A copied remote context must not bypass the local source comparison merely
    # because context.parent.parent is no longer this checkout.
    verify_current_source(repo, root / "vendor/infra/build-source.json")
    missing = [name for name, _, _ in guard.copies(root / "Dockerfile") if not (root / name).exists()]
    if missing:
        raise ValueError("P26: missing COPY inputs: " + ", ".join(missing))
    # Check the real catalog/official/support manifests before a complete COPY
    # receipt can exist. The guard alone proves bytes, not their release origin.
    artifact = supply.module(original / "lib/infra-artifact.py", "s9_context_artifact")
    artifact.verify_official(root / "vendor", root / "vendor/install.sh")
    artifact.verify_support(root / "vendor")
    receipt = root / guard.MANIFEST_SOURCE
    guard.write(receipt, guard.snapshot(root))
    guard.compare_report(root, receipt)
    return receipt


def verify_current_source(repo, receipt):
    guard = supply.module(repo / "test/simcluster/lib/infra-image-manifest.py", "s9_source_guard")
    source_record = supply.read_json(receipt)
    tracked = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "--",
        "Makefile", "build/goreleaser.yaml", "build/infra-support.py", "go.mod", "go.sum", "scripts/install.sh", "cmd", "internal"],
        cwd=repo, timeout=30, text=True).splitlines()
    if source_record.get("schema_version") != 1 or [row["path"] for row in source_record["inputs"]] != sorted(set(tracked)):
        raise ValueError("P26: complete current build-source input set required")
    for row in source_record["inputs"]:
        path = supply.relative(repo, row["path"])
        supply.regular(path)
        if row != {"path": row["path"], **guard.metadata(path)}:
            raise ValueError("P26: stale current build-source input: " + row["path"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("historical", "context"))
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--previous-vendor", required=True, type=Path)
    parser.add_argument("--vendor", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        root = args.output.absolute()
        if root.resolve() != root or root in (Path("/"), Path("/tmp")) or root.exists() and any(root.iterdir()):
            raise ValueError("P26: fresh canonical private output required")
        if args.command == "historical":
            historical(args.repo.absolute(), args.previous_vendor.absolute(), root)
        elif args.vendor is None:
            raise ValueError("P26: pre-staged official/support/CLI vendor inputs required")
        else:
            context(args.repo.absolute(), args.vendor.absolute(), args.previous_vendor.absolute(), root)
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        parser.exit(2, "SETUP-RED: " + str(error) + "\n")


if __name__ == "__main__":
    main()
