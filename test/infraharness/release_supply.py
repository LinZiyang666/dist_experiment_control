#!/usr/bin/env python3
"""Offline staging for the S9 integration consumers.

Build only the current workspace. Historical support is an explicit prebuilt
release.json input, never a ref, rebuild or version-only variant. This program
stages input custody; it does not issue runtime or retirement receipts.
Run under the shared testharness supervisor when called by a test.
origin: S9 internal review INFRA-SUITE-SUPPLIES F4 / F6
"""
import argparse
import contextlib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile

NAMES = ("tether-infra-helper", "tether-infra-launch", "tether")
LIMIT = 512 << 20


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate supply key")
        result[key] = value
    return result


def read_json(path):
    regular(path, limit=4 << 20)
    return json.loads(Path(path).read_bytes(), object_pairs_hook=unique)


def regular(path, limit=LIMIT):
    path = Path(path)
    # A private output cannot make an input symlink or symlinked ancestor safe.
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError("supply path contains a symlink")
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= limit:
        raise ValueError("supply input must be a bounded regular file")
    with path.open("rb") as incoming:
        return hashlib.file_digest(incoming, "sha256").hexdigest()


def relative(root, name):
    path = Path(name)
    if not name or path.is_absolute() or path.as_posix() != name or ".." in path.parts or "\\" in name:
        raise ValueError("supply path must stay below its private root")
    return Path(root) / path


def pin(root, path):
    return {"Path": Path(path).relative_to(root).as_posix(), "SHA256": regular(path)}


def verify(root, record):
    if set(record) != {"Path", "SHA256"} or not re.fullmatch(r"[0-9a-f]{64}", record["SHA256"]):
        raise ValueError("invalid supplied file binding")
    path = relative(root, record["Path"])
    if regular(path) != record["SHA256"]:
        raise ValueError("supplied bytes differ from their digest")
    return path


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


@contextlib.contextmanager
def offline(repo, root):
    # Query cache locations before isolating HOME. No product home or credential
    # is inherited, and missing cached dependencies cannot cause a download.
    selected = json.loads(subprocess.check_output(["go", "env", "-json", "GOROOT", "GOPATH", "GOCACHE"], cwd=repo, timeout=30))
    environment = {"PATH": selected["GOROOT"] + "/bin:/usr/bin:/bin", "HOME": str(root),
                   "GOPATH": selected["GOPATH"], "GOCACHE": selected["GOCACHE"], "GOTOOLCHAIN": "local",
                   "GOPROXY": "off", "GOSUMDB": "off", "GOFLAGS": os.environ.get("GOFLAGS", ""),
                   "PYTHONDONTWRITEBYTECODE": "1", "SIM_VERSION": "v0.0.0-current"}
    previous = dict(os.environ)
    os.environ.clear()
    os.environ.update(environment)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(previous)


def race_binary(repo, output, name):
    subprocess.run(["go", "build", "-race", "-trimpath", "-o", str(output), "./cmd/" + name],
                   cwd=repo, env=dict(os.environ, CGO_ENABLED="1", GOOS="linux", GOARCH="amd64"), check=True, timeout=480)
    # Go's output mode follows the builder's umask. Release executable modes
    # are fixed independently of that host setting, including inside the tar.
    output.chmod(0o755)
    verify_race(repo, output, name)


def verify_race(repo, path, name=None):
    regular(path)
    info = subprocess.check_output(["go", "version", "-m", str(path)], cwd=repo, timeout=30, text=True)
    if "\tbuild\t-race=true\n" not in info:
        raise ValueError("inner executable lacks race instrumentation")
    if name is not None and "\tpath\tgithub.com/LinZiyang666/tether/cmd/" + name + "\n" not in info:
        raise ValueError("race executable is not the real product entry")


def write(root, name, value):
    path = root / name
    data = canonical(value)
    with path.open("xb") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())
    path.chmod(0o600)
    return pin(root, path)


def current(repo, root):
    supplier = module(repo / "test/simcluster/lib/infra-artifact.py", "s9_artifact")
    catalog = repo / "internal/infraartifact/testdata/catalog.json"
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True, timeout=10).strip()
    archive = root / "tether_0.0.0-current_linux_amd64.tar.gz"
    with offline(repo, root):
        record, binaries = supplier.make_support_archive(repo, archive, "current", commit, catalog)
        race, race_records = {}, []
        for name in NAMES:
            destination = root / (name + "-race")
            race_binary(repo, destination, name)
            inputs, source_sha = supplier.source_inputs(repo, name)
            race[name] = destination
            race_records.append({"component": name, "source_inputs": inputs, "source_sha256": source_sha,
                                 "binary_sha256": regular(destination), "protocol_version": 1, "allowed_previous_reader": 1})
        # Both archives use the common release manifest generator. Executing a
        # version command is never used as a substitute for archive custody.
        support = root / "race-support.manifest"
        support.write_bytes(supplier.support_manifest().manifest_bytes("v0.0.0-current", "amd64", race_records[:2], regular(catalog)))
        race_archive = root / "tether_0.0.0-current_race_linux_amd64.tar.gz"
        with tarfile.open(race_archive, "w:gz") as package:
            for name, path in race.items():
                package.add(path, arcname=name, recursive=False)
            package.add(catalog, arcname="infra-catalog.json", recursive=False)
            package.add(support, arcname="infra-support.manifest", recursive=False)
        record["race_binaries"] = race_records
    source = write(root, "source.json", record)
    sums = root / "SHA256SUMS"
    sums.write_text("".join(regular(path) + "  " + path.name + "\n" for path in (archive, race_archive)))
    release = {"Version": "v0.0.0-current", "Archive": pin(root, archive), "Checksums": pin(root, sums),
               "Source": source, "RaceArchive": pin(root, race_archive),
               "Binaries": [{"Name": name, "SHA256": regular(binaries[name]), "Race": pin(root, race[name])} for name in NAMES]}
    write(root, "release.json", release)
    return release


# origin: S9 internal review INFRA-SUITE-SUPPLIES F4
def copy_bound(source_root, record, target):
    incoming = verify(source_root, record)
    shutil.copyfile(incoming, target)
    # A supplied file can change after admission. Hash the bytes actually
    # copied, rather than allowing the earlier input hash to sign a new inode.
    if regular(target) != record["SHA256"]:
        raise ValueError("P26: prebuilt input changed during staging")


def import_release(repo, path, destination):
    release = read_json(path)
    if set(release) != {"Version", "Archive", "Checksums", "Source", "RaceArchive", "Binaries"} or not re.fullmatch(r"v[0-9][A-Za-z0-9.+_-]{0,127}", release["Version"]):
        raise ValueError("P26: invalid prebuilt previous release")
    if {row["Name"] for row in release["Binaries"]} != set(NAMES) or len(release["Binaries"]) != 3:
        raise ValueError("P26: previous release requires all three genuine executables")
    source_root = Path(path).parent
    files = [release[key] for key in ("Archive", "Checksums", "Source", "RaceArchive")]
    files += [row["Race"] for row in release["Binaries"]]
    # Validate everything before publishing even a partial historical supply.
    paths = [verify(source_root, row) for row in files]
    source = read_json(verify(source_root, release["Source"]))
    records = source["binaries"]
    if {row["component"] for row in records} != set(NAMES[:2]):
        raise ValueError("P26: previous source lacks both executable closures")
    binaries = {row["Name"]: row for row in release["Binaries"]}
    for row in records:
        if row["binary_sha256"] != binaries[row["component"]]["SHA256"]:
            raise ValueError("P26: previous source is not bound to its executable")
        if hashlib.sha256(canonical(row["source_inputs"])).hexdigest() != row["source_sha256"]:
            raise ValueError("P26: previous source closure digest mismatch")
    for binary in release["Binaries"]:
        verify_race(repo, verify(source_root, binary["Race"]), binary["Name"])
    sums = verify(source_root, release["Checksums"]).read_text().splitlines()
    for key in ("Archive", "RaceArchive"):
        expected = release[key]["SHA256"] + "  " + Path(release[key]["Path"]).name
        if sums.count(expected) != 1:
            raise ValueError("P26: previous archive requires exactly one checksum row")
    supplier = module(repo / "test/simcluster/lib/infra-artifact.py", "s9_previous")
    for key, race in (("Archive", False), ("RaceArchive", True)):
        archive = verify(source_root, release[key])
        with tarfile.open(archive) as package:
            members = supplier.safe_members(package)
            for binary in release["Binaries"]:
                expected = binary["Race"]["SHA256"] if race else binary["SHA256"]
                entry = members.get(binary["Name"])
                if entry is None or not entry.isfile() or entry.mode != 0o755 or not 0 < entry.size <= LIMIT:
                    raise ValueError("P26: previous archive omitted an executable")
                with package.extractfile(entry) as incoming:
                    if hashlib.file_digest(incoming, "sha256").hexdigest() != expected:
                        raise ValueError("P26: previous archive executable binding mismatch")
    destination.mkdir(mode=0o700)
    copied = {}
    for row, source_path in zip(files, paths):
        name = source_path.name
        target = destination / name
        if name in copied and copied[name] != row["SHA256"]:
            raise ValueError("P26: ambiguous previous supply basename")
        if name not in copied:
            copy_bound(source_root, row, target)
            target.chmod(0o755 if source_path in paths[4:] else 0o600)
        copied[name] = row["SHA256"]
        row["Path"] = target.relative_to(destination.parent).as_posix()
    return release


def distinct(previous, present, root):
    if previous["Version"] == present["Version"]:
        raise ValueError("P26: previous support is a version-only relabel")
    old, new = (read_json(verify(root, row["Source"])) for row in (previous, present))
    old_records = {row["component"]: row for row in old["binaries"]}
    for row in new["binaries"]:
        before = old_records[row["component"]]
        if before["source_sha256"] == row["source_sha256"] or before["binary_sha256"] == row["binary_sha256"]:
            raise ValueError("P26: previous support must have distinct executable source and bytes")


def image(repo, source, root):
    guard = module(repo / "test/simcluster/lib/infra-image-manifest.py", "s9_image")
    original = guard.snapshot(source)
    context = root / "image-context"
    context.mkdir(mode=0o700)
    # Copy the actual closed input set. A missing CLI, previous image, installer
    # or provisioner stays a supply error; none is replaced by today's tether.
    for row in sorted(original["sources"], key=lambda row: (len(Path(row["path"]).parts), row["path"])):
        target = relative(context, row["path"])
        target.parent.mkdir(parents=True, exist_ok=True)
        if row["type"] == "directory":
            target.mkdir(exist_ok=True)
        else:
            shutil.copyfile(relative(source, row["path"]), target)
        target.chmod(row["mode"])
    receipt = context / guard.MANIFEST_SOURCE
    guard.write(receipt, original)
    installed = root / "image-root"
    installed.mkdir(mode=0o700)
    for row in original["copies"]:
        target = installed / row["target"].lstrip("/")
        target.parent.mkdir(parents=True, exist_ok=True)
        if row["type"] == "directory":
            target.mkdir(exist_ok=True)
        else:
            shutil.copyfile(relative(context, row["source"]), target)
        target.chmod(row["mode"])
    guard.compare_report(context, receipt)
    guard.verify_targets(original, installed, installed / "opt/sim/Dockerfile")
    return "image-context", "image-root", pin(root, receipt)


def downloads(repo, origin, root):
    supplier = module(repo / "test/simcluster/lib/infra-artifact.py", "s9_downloads")
    records = []
    directory = root / "official-downloads"
    directory.mkdir(mode=0o700)
    for entry in supplier.catalog(repo / "internal/infraartifact/testdata/catalog.json", repo / "scripts/install.sh"):
        if (entry["Kind"], entry["Version"]) not in ((1, "2.14.6"), (2, "2.7.6")):
            continue
        checksum_name = supplier.official_record(entry)["checksums"]
        archive, checksums = origin / entry["ArchiveName"], origin / checksum_name
        if regular(archive) != entry["ArchiveSHA256"] or regular(checksums) != entry["ChecksumSHA256"]:
            raise ValueError("P26: official installer input digest mismatch")
        for url, incoming in ((entry["ArchiveURL"], archive), (entry["ChecksumURL"], checksums)):
            target = directory / incoming.name
            shutil.copyfile(incoming, target)
            if regular(target) != regular(incoming):
                raise ValueError("P26: official download changed during staging")
            records.append({"URL": url, "File": pin(root, target)})
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("current", "image", "privilege"))
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--previous", type=Path)
    parser.add_argument("--runner", type=Path)
    parser.add_argument("--runner-source", type=Path)
    parser.add_argument("--image-context", type=Path)
    parser.add_argument("--support-catalog", type=Path)
    parser.add_argument("--stock", type=Path)
    args = parser.parse_args()
    try:
        if sys.platform != "linux" or os.uname().machine != "x86_64":
            raise ValueError("unsupported: S9 integration supply requires linux/amd64")
        repo, root = args.repo.absolute(), args.output.absolute()
        if root.resolve() != root or root == Path(tempfile.gettempdir()) or root == Path("/"):
            raise ValueError("private canonical output root required")
        if root.exists() and list(root.iterdir()):
            raise ValueError("fresh empty output required; refusing stale supply")
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        root.chmod(0o700)
        if args.command == "current":
            current(repo, root)
        elif args.command == "image":
            if args.image_context is None:
                raise ValueError("P26: complete pre-staged COPY context required")
            image(repo, args.image_context, root)
        else:
            if args.previous is None or args.runner is None or args.runner_source is None or args.image_context is None or args.support_catalog is None or args.stock is None:
                raise ValueError("P26/P40: genuine previous release, installed syscall runner/source, support catalog, official stock supply and complete COPY context required")
            previous = import_release(repo, args.previous, root / "previous")
            verify_race(repo, args.runner)
            runner_source = regular(args.runner_source)
            present = current(repo, root)
            distinct(previous, present, root)
            context, installed, receipt = image(repo, args.image_context, root)
            runner, source, installer = root / "installed-runner", root / "installed-runner-source", root / "install.sh"
            shutil.copyfile(args.runner, runner)
            runner.chmod(0o755)
            shutil.copyfile(args.runner_source, source)
            if regular(source) != runner_source:
                raise ValueError("P40: runner source changed during staging")
            shutil.copyfile(repo / "scripts/install.sh", installer)
            catalog_path = root / "support-catalog.json"
            catalog_sha = regular(args.support_catalog)
            shutil.copyfile(args.support_catalog, catalog_path)
            if regular(catalog_path) != catalog_sha:
                raise ValueError("P26: support catalog changed during staging")
            catalog = pin(root, catalog_path)
            write(root, "privilege-supply.json", {"Version": 1, "Downloads": downloads(repo, args.stock.absolute(), root), "Releases": [previous, present],
                "Runner": pin(root, runner), "RunnerSource": pin(root, source), "Installer": pin(root, installer),
                "ImageContext": context, "ImageRoot": installed, "ImageReceipt": receipt, "SupportCatalog": catalog})
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, tarfile.TarError) as error:
        parser.exit(2, "SETUP-RED: " + str(error) + "\n")


if __name__ == "__main__":
    main()
