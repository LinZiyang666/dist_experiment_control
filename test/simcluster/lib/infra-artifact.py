#!/usr/bin/env python3
"""Supply official adjacent infrastructure and genuine source-built support archives.

No staged executable runs on this host. Invoke stage under lib/supervisor.sh;
integration tests consume verify results and never download/build a missing item.
The previous support ref is explicit: no released tether tag yet contains S9.
Requires Python 3.12+ (the Ubuntu 24.04 sim profile).
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import time


SPEC = importlib.util.spec_from_file_location("sim_caddy_artifact", Path(__file__).with_name("caddy-artifact.py"))
CADDY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CADDY)
# This binds the already frozen P01 catalog, rather than introducing another pin.
# origin: hot-update S9 plan §3.1 / T55
CATALOG_SHA256 = "0a7faebb53ee137a43cc12b0e2f84ccc06b29c273e6cc48bffb02c30024c344b"
SUPPORT = ("tether-infra-helper", "tether-infra-launch")
MAX_ARCHIVE = 256 << 20


def support_manifest():
    # Only source staging needs the generator. The image verifier remains
    # self-contained and independently parses the emitted closed format.
    spec = importlib.util.spec_from_file_location("infra_support_manifest", Path(__file__).resolve().parents[3] / "build/infra-support.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def read_json(path, limit=4 << 20):
    info = Path(path).lstat()
    if not stat.S_ISREG(info.st_mode) or not 1 <= info.st_size <= limit:
        raise ValueError("manifest must be a bounded regular file")
    return json.loads(Path(path).read_bytes(), object_pairs_hook=CADDY.unique_object)


def pins(installer):
    text = Path(installer).read_text()
    values = {}
    for component, name, prefix in (("nats-server", "NATS_SERVER_VERSION", "v"), ("caddy", "CADDY_VERSION", "")):
        matches = re.findall(r'^' + name + r'="\$\{TETHER_' + name + r':-(' + prefix + r'[0-9]+\.[0-9]+\.[0-9]+)\}"$', text, re.M)
        if len(matches) != 1:
            raise ValueError("installer must have exactly one " + name + " default")
        override = os.environ.get("TETHER_" + name)
        if override and override != matches[0]:
            raise ValueError("S9 supply cannot override the installer pin")
        values[component] = matches[0].removeprefix("v")
    return values


def catalog(path, installer):
    CADDY.require_target()
    if CADDY.digest(path) != CATALOG_SHA256:
        raise ValueError("infra catalog differs from the frozen S9 catalog")
    entries = read_json(path)
    selected = pins(installer)
    for component, kind in (("nats-server", 1), ("caddy", 2)):
        versions = [entry["Version"] for entry in entries if entry["Kind"] == kind]
        if len(versions) != 2 or selected[component] != max(versions):
            raise ValueError("installer pin is not the adjacent catalog's current release")
    return entries


def regular(path, mode=None):
    info = Path(path).lstat()
    if not stat.S_ISREG(info.st_mode) or not 1 <= info.st_size <= MAX_ARCHIVE:
        raise ValueError("artifact must be a bounded regular file: " + str(path))
    if mode is not None and stat.S_IMODE(info.st_mode) != mode:
        raise ValueError("artifact mode mismatch: " + str(path))
    return CADDY.digest(path)


def safe_members(package):
    return CADDY.safe_members(package)


def extract_binary(archive, member_name, destination):
    with tarfile.open(archive, "r:gz") as package:
        member = safe_members(package).get(member_name)
        if member is None or not member.isfile() or not 64 <= member.size <= CADDY.MAX_BINARY:
            raise ValueError("archive lacks its unique bounded executable member")
        with package.extractfile(member) as source, Path(destination).open("wb") as target:
            shutil.copyfileobj(source, target)
    Path(destination).chmod(0o755)
    return CADDY.binary_digest(destination, executable=True)


def publish(source, destination, mode):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    pending = CADDY.staged_copy(source, destination.parent, mode)
    try:
        os.replace(pending, destination)
        CADDY.sync_directory(destination.parent)
    finally:
        pending.unlink(missing_ok=True)


def write_json(destination, value):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".infra-receipt-", dir=destination.parent)
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(canonical(value))
            out.flush()
            os.fchmod(out.fileno(), 0o644)
            os.fsync(out.fileno())
        os.replace(name, destination)
        CADDY.sync_directory(destination.parent)
    finally:
        Path(name).unlink(missing_ok=True)


def official_record(entry):
    component = "nats-server" if entry["Kind"] == 1 else "caddy"
    return {"component": component, "version": entry["Version"],
            "binary": f"{component}-{entry['Version']}", "archive": entry["ArchiveName"],
            "checksums": component + "-" + entry["Version"] + "-checksums.txt",
            "archive_sha256": entry["ArchiveSHA256"], "binary_sha256": entry["BinarySHA256"],
            "checksum_sha256": entry["ChecksumSHA256"], "source": entry["ArchiveURL"],
            "checksum_algorithm": entry["ChecksumAlgorithm"], "official_checksum": entry["OfficialChecksum"]}


def stage_official(installer, vendor, catalog_path):
    started = time.monotonic()
    entries = catalog(catalog_path, installer)
    directory = Path(vendor) / "infra"
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o755)
    (directory / "official-source.json").unlink(missing_ok=True)
    with tempfile.TemporaryDirectory(prefix=".infra-official-", dir=directory) as temporary:
        work = Path(temporary)
        records = []
        for entry in entries:
            record = official_record(entry)
            archive, sums = work / record["archive"], work / record["checksums"]
            CADDY.download(entry["ArchiveURL"], archive)
            CADDY.download(entry["ChecksumURL"], sums)
            if regular(archive) != entry["ArchiveSHA256"] or regular(sums) != entry["ChecksumSHA256"]:
                raise ValueError("official archive/checksum list SHA256 mismatch")
            expected = CADDY.archive_checksum(sums, entry["ArchiveName"])
            if expected != entry["OfficialChecksum"] or CADDY.digest(archive, entry["ChecksumAlgorithm"]) != expected:
                raise ValueError("official checksum mismatch")
            binary = work / record["binary"]
            if extract_binary(archive, entry["BinaryMember"], binary) != entry["BinarySHA256"]:
                raise ValueError("official extracted binary SHA256 mismatch")
            for source, mode in ((binary, 0o755), (archive, 0o644), (sums, 0o644)):
                publish(source, directory / source.name, mode)
            records.append(record)
        publish(catalog_path, directory / "catalog.json", 0o644)
        selected = pins(installer)
        for component, version in selected.items():
            publish(directory / f"{component}-{version}", Path(vendor) / component, 0o755)
            previous = next(entry["Version"] for entry in entries if entry["Kind"] == (1 if component == "nats-server" else 2) and entry["Version"] != version)
            publish(directory / f"{component}-{previous}", Path(vendor) / (component + "-prev"), 0o755)
            publish(directory / f"{component}-{version}", Path(vendor) / (component + "-next"), 0o755)
        current = next(entry for entry in entries if entry["Kind"] == 2 and entry["Version"] == selected["caddy"])
        write_json(Path(vendor) / "caddy-source.json", {
            "schema_version": 1, "component": "caddy", "version": current["Version"],
            "goos": "linux", "goarch": "amd64", "installer_sha256": CADDY.digest(installer),
            "archive_sha256": current["ArchiveSHA256"], "binary_sha256": current["BinarySHA256"],
            "archive_checksum_algorithm": current["ChecksumAlgorithm"],
            "archive_checksum": current["OfficialChecksum"], "source": current["ArchiveURL"],
        })
        write_json(directory / "official-source.json", {
            "schema_version": 1, "goos": "linux", "goarch": "amd64",
            "installer_sha256": CADDY.digest(installer), "catalog_sha256": CATALOG_SHA256,
            "artifacts": records,
        })
    verify_official(vendor, installer)
    print(f"infra official cold supply wall_seconds={time.monotonic() - started:.3f}")


def verify_official(vendor, installer, binary_root=None):
    directory = Path(vendor) / "infra"
    entries = catalog(directory / "catalog.json", installer)
    value = read_json(directory / "official-source.json")
    expected = {"schema_version": 1, "goos": "linux", "goarch": "amd64",
                "installer_sha256": CADDY.digest(installer), "catalog_sha256": CATALOG_SHA256,
                "artifacts": [official_record(entry) for entry in entries]}
    if canonical(value) != canonical(expected):
        raise ValueError("official supply receipt mismatch")
    for entry, record in zip(entries, value["artifacts"]):
        binary, archive, sums = (directory / record[key] for key in ("binary", "archive", "checksums"))
        if (regular(binary, 0o755) != entry["BinarySHA256"] or CADDY.binary_digest(binary) != entry["BinarySHA256"]
                or regular(archive, 0o644) != entry["ArchiveSHA256"] or regular(sums, 0o644) != entry["ChecksumSHA256"]
                or CADDY.archive_checksum(sums, entry["ArchiveName"]) != entry["OfficialChecksum"]
                or CADDY.digest(archive, entry["ChecksumAlgorithm"]) != entry["OfficialChecksum"]):
            raise ValueError("official supplied bytes/mode/checksum mismatch")
    installed = Path(binary_root) if binary_root else Path(vendor)
    for component, version in pins(installer).items():
        if regular(installed / component, 0o755) != regular(directory / f"{component}-{version}", 0o755):
            raise ValueError("default binary differs from installer pin")
        previous = next(entry["Version"] for entry in entries if entry["Kind"] == (1 if component == "nats-server" else 2) and entry["Version"] != version)
        for suffix, selected in (("prev", previous), ("next", version)):
            if regular(Path(vendor) / (component + "-" + suffix), 0o755) != regular(directory / f"{component}-{selected}", 0o755):
                raise ValueError("adjacent binary alias mismatch")
    CADDY.verify(installed / "caddy", Path(vendor) / "caddy-source.json", installer)
    return value


def run(args, cwd):
    return subprocess.check_output(args, cwd=cwd, timeout=600, text=True)


def source_inputs(root, component):
    """Hash the executable's actual repo dependency closure, excluding version flags."""
    return support_manifest().source_inputs(root, component)


def source_tree(repo, ref, directory):
    commit = run(["git", "rev-parse", "--verify", ref + "^{commit}"], repo).strip()
    archive = directory / "source.tar"
    subprocess.run(["git", "archive", "--format=tar", "--output=" + str(archive), commit], cwd=repo, check=True, timeout=60)
    root = directory / "source"
    root.mkdir()
    # Git source archives may have symlinks; none are allowed in a build supplied
    # for privileged executables. Never create another checkout/worktree/index.
    with tarfile.open(archive) as package:
        safe_members(package)
        package.extractall(root, filter="data")
    return root, commit


def make_support_archive(root, destination, version, commit, catalog_path, tether=None):
    binaries, records = {}, []
    for component in SUPPORT:
        inputs, source_sha = source_inputs(root, component)
        output = destination.parent / (destination.name + "-" + component)
        env = dict(os.environ, CGO_ENABLED="0", GOOS="linux", GOARCH="amd64")
        # No version-only variant: build bytes follow actual source and toolchain.
        subprocess.run(["go", "build", "-trimpath", "-ldflags=-s -w", "-o", str(output), "./cmd/" + component],
                       cwd=root, env=env, check=True, timeout=600)
        records.append({"component": component, "source_sha256": source_sha,
                        "source_inputs": inputs, "binary_sha256": CADDY.binary_digest(output, executable=True),
                        "protocol_version": 1, "allowed_previous_reader": 1,
                        "buildinfo": run(["go", "version", "-m", str(output)], root)})
        binaries[component] = output
    release_version = os.environ.get("SIM_VERSION", "v0.0.0-simcluster")
    if tether is None:
        tether = destination.parent / (destination.name + "-tether")
        flags = "-s -w -X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion=" + release_version
        subprocess.run(["go", "build", "-trimpath", "-ldflags=" + flags, "-o", str(tether), "./cmd/tether"],
                       cwd=root, env=dict(os.environ, CGO_ENABLED="0", GOOS="linux", GOARCH="amd64"), check=True, timeout=600)
    regular(tether)
    binaries["tether"] = tether
    def archive_metadata(member):
        member.mode = 0o755 if member.name in binaries else 0o644
        member.uid = member.gid = 0
        member.uname = member.gname = "root"
        return member
    # The release and simulator use the same closed manifest producer. No fixture
    # fills missing archive members after packaging.
    # origin: S9 internal review rv-priv F2
    manifest = destination.parent / (destination.name + "-manifest")
    manifest.write_bytes(support_manifest().manifest_bytes(release_version, "amd64", records, regular(catalog_path)))
    with tarfile.open(destination, "w:gz") as package:
        for name, binary in binaries.items():
            package.add(binary, arcname=name, recursive=False, filter=archive_metadata)
        package.add(catalog_path, arcname="infra-catalog.json", recursive=False, filter=archive_metadata)
        package.add(manifest, arcname="infra-support.manifest", recursive=False, filter=archive_metadata)
    destination.chmod(0o644)
    return {"version": version, "source_commit": commit, "archive": destination.name,
            "archive_sha256": regular(destination), "tether_sha256": regular(tether), "binaries": records}, binaries


def stage_support(repo, vendor, previous_ref):
    CADDY.require_target()
    if not previous_ref or previous_ref.startswith("-"):
        raise ValueError("genuine previous support source required: set SIM_INFRA_PREVIOUS_REF")
    started = time.monotonic()
    repo, directory = Path(repo).resolve(), Path(vendor) / "infra"
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o755)
    (directory / "support-source.json").unlink(missing_ok=True)
    with tempfile.TemporaryDirectory(prefix=".infra-support-", dir=directory) as temporary:
        work = Path(temporary).resolve()
        previous, commit = source_tree(repo, previous_ref, work)
        for component in SUPPORT:
            if not (previous / "cmd" / component / "main.go").is_file():
                raise ValueError("previous source ref does not contain " + component)
            # Reject a version-only old build before spending compile time or
            # publishing any generation. Verification repeats this independently.
            if source_inputs(previous, component)[1] == source_inputs(repo, component)[1]:
                raise ValueError("support generations require distinct executable source: " + component)
        head = run(["git", "rev-parse", "HEAD"], repo).strip()
        records, images = [], []
        for generation, root, revision in (("previous", previous, commit), ("current", repo, head)):
            archive = work / f"tether_support_{generation}_linux_amd64.tar.gz"
            record, binaries = make_support_archive(root, archive, generation, revision, directory / "catalog.json")
            records.append(record)
            images.append(binaries)
        for old, new in zip(records[0]["binaries"], records[1]["binaries"]):
            if old["source_sha256"] == new["source_sha256"] or old["binary_sha256"] == new["binary_sha256"]:
                raise ValueError("support generations require distinct executable source and bytes: " + new["component"])
        for generation, record, binaries in zip(("previous", "current"), records, images):
            publish(work / record["archive"], directory / record["archive"], 0o644)
            for component in SUPPORT:
                publish(binaries[component], directory / (component + "-" + generation), 0o755)
                if generation == "current":
                    publish(binaries[component], Path(vendor) / component, 0o755)
            if generation == "current":
                publish(binaries["tether"], Path(vendor) / "tether", 0o755)
        write_json(directory / "support-source.json", {"schema_version": 1, "goos": "linux", "goarch": "amd64", "generations": records})
        sums = work / "SHA256SUMS"
        sums.write_text("".join(record["archive_sha256"] + "  " + record["archive"] + "\n" for record in records))
        publish(sums, directory / "SHA256SUMS", 0o644)
    verify_support(vendor)
    # Record every build input outside the Docker context. Source scanning on
    # the staging host later compares these bytes before any image can sign.
    names = run(["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", "Makefile", "build/goreleaser.yaml", "build/infra-support.py", "go.mod", "go.sum", "scripts/install.sh", "cmd", "internal"], repo).splitlines()
    inputs = []
    for name in sorted(set(names)):
        path = repo / name
        if not path.is_file() or path.is_symlink():
            raise ValueError("build source must be a regular file")
        inputs.append({"path": name, "type": "file", "mode": stat.S_IMODE(path.stat().st_mode),
                       "size": path.stat().st_size, "sha256": CADDY.digest(path)})
    write_json(directory / "build-source.json", {"schema_version": 1, "inputs": inputs})
    print(f"infra support cold supply wall_seconds={time.monotonic() - started:.3f}")


def verify_support(vendor, support_root=None, binary_root=None):
    directory = Path(vendor) / "infra"
    value = read_json(directory / "support-source.json")
    if set(value) != {"schema_version", "goos", "goarch", "generations"} or type(value["schema_version"]) is not int or value["schema_version"] != 1 or (value["goos"], value["goarch"]) != ("linux", "amd64"):
        raise ValueError("support supply schema/platform mismatch")
    generations = value["generations"]
    if len(generations) != 2 or [item["version"] for item in generations] != ["previous", "current"]:
        raise ValueError("support supply requires two ordered generations")
    for generation in generations:
        if set(generation) != {"version", "source_commit", "archive", "archive_sha256", "tether_sha256", "binaries"} or not re.fullmatch(r"[a-f0-9]{40}", generation["source_commit"]):
            raise ValueError("support generation source/schema mismatch")
        name = generation["archive"]
        if name != f"tether_support_{generation['version']}_linux_amd64.tar.gz":
            raise ValueError("support archive name mismatch")
        archive = directory / name
        if regular(archive, 0o644) != generation["archive_sha256"] or CADDY.archive_checksum(directory / "SHA256SUMS", name) != generation["archive_sha256"]:
            raise ValueError("support archive checksum mismatch")
        if generation["version"] == "current":
            installed = Path(binary_root) if binary_root else Path(vendor)
            if regular(installed / "tether", 0o755) != generation["tether_sha256"]:
                raise ValueError("release archive and staged tether differ")
        if [item["component"] for item in generation["binaries"]] != list(SUPPORT):
            raise ValueError("support archive lacks helper or launcher")
        with tarfile.open(archive) as package:
            members = safe_members(package)
            if set(members) != {"tether", *SUPPORT, "infra-catalog.json", "infra-support.manifest"}:
                raise ValueError("support release archive target set mismatch")
            for component, expected in [("tether", generation["tether_sha256"]), *[(item["component"], item["binary_sha256"]) for item in generation["binaries"]], ("infra-catalog.json", CATALOG_SHA256)]:
                member = members[component]
                if not member.isfile() or not 1 <= member.size <= CADDY.MAX_BINARY:
                    raise ValueError("invalid support archive member")
                if stat.S_IMODE(member.mode) != (0o644 if component == "infra-catalog.json" else 0o755):
                    raise ValueError("support archive member mode mismatch")
                with package.extractfile(member) as incoming:
                    if hashlib.file_digest(incoming, "sha256").hexdigest() != expected:
                        raise ValueError("support archived bytes mismatch")
            manifest = members["infra-support.manifest"]
            if not manifest.isfile() or not 1 <= manifest.size <= 4096 or stat.S_IMODE(manifest.mode) != 0o644:
                raise ValueError("invalid support manifest archive member")
            data = package.extractfile(manifest).read()
            lines = data.decode("ascii").splitlines()
            release = lines[1].removeprefix("release ") if len(lines) > 1 else ""
            sources = [{"component": item["component"], "source_sha256": item["source_sha256"]} for item in generation["binaries"]]
            expected = ["format 1", "release " + release, "goos linux", "goarch amd64", "protocol 1", "previous-reader 1",
                        "source-sha256 " + hashlib.sha256(canonical(sources)).hexdigest(),
                        *["sha256 " + item["binary_sha256"] + " " + item["component"] for item in generation["binaries"]],
                        "sha256 " + CATALOG_SHA256 + " infra-catalog.json"]
            if not re.fullmatch(r"v[0-9][A-Za-z0-9.+_-]{0,127}", release) or data != ("\n".join(expected) + "\n").encode():
                raise ValueError("support archive manifest differs from source/binary records")
        for binary in generation["binaries"]:
            if set(binary) != {"component", "source_sha256", "source_inputs", "binary_sha256", "protocol_version", "allowed_previous_reader", "buildinfo"}:
                raise ValueError("support binary schema mismatch")
            if type(binary["protocol_version"]) is not int or type(binary["allowed_previous_reader"]) is not int or (binary["protocol_version"], binary["allowed_previous_reader"]) != (1, 1):
                raise ValueError("support reader protocol mismatch")
            paths = [row["path"] for row in binary["source_inputs"]]
            if not paths or paths != sorted(set(paths)) or "go.mod" not in paths or "go.sum" not in paths or "cmd/" + binary["component"] + "/main.go" not in paths:
                raise ValueError("support source closure missing/duplicate inputs")
            if any(set(row) != {"path", "sha256"} or not re.fullmatch(r"[a-f0-9]{64}", row["sha256"]) for row in binary["source_inputs"]):
                raise ValueError("support source closure schema mismatch")
            for field in ("GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0", "-trimpath=true", "path\tgithub.com/LinZiyang666/tether/cmd/" + binary["component"]):
                if field not in binary["buildinfo"]:
                    raise ValueError("support build identity mismatch")
            if hashlib.sha256(canonical(binary["source_inputs"])).hexdigest() != binary["source_sha256"]:
                raise ValueError("support source closure digest mismatch")
            path = directory / (binary["component"] + "-" + generation["version"])
            if regular(path, 0o755) != binary["binary_sha256"] or CADDY.binary_digest(path) != binary["binary_sha256"]:
                raise ValueError("support supplied binary mismatch")
            installed = Path(support_root) if support_root else Path(vendor)
            if generation["version"] == "current" and regular(installed / binary["component"], 0o755) != binary["binary_sha256"]:
                raise ValueError("support installed binary mismatch")
    # origin: hot-update S9 plan §9.4 / M28. A new version label is not N-1.
    for old, new in zip(generations[0]["binaries"], generations[1]["binaries"]):
        if old["source_sha256"] == new["source_sha256"] or old["binary_sha256"] == new["binary_sha256"]:
            raise ValueError("support previous generation is same-source or same-binary")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    staging = sub.add_parser("stage")
    staging.add_argument("repo")
    staging.add_argument("vendor")
    staging.add_argument("--previous-ref", default="")
    official = sub.add_parser("stage-official")
    official.add_argument("installer")
    official.add_argument("vendor")
    official.add_argument("catalog")
    support = sub.add_parser("stage-support")
    support.add_argument("repo")
    support.add_argument("vendor")
    support.add_argument("previous_ref")
    checking = sub.add_parser("verify")
    checking.add_argument("vendor")
    checking.add_argument("installer")
    checking.add_argument("--official-only", action="store_true")
    checking.add_argument("--binary-root")
    checking.add_argument("--support-root")
    args = parser.parse_args()
    try:
        if args.command == "stage":
            if not args.previous_ref:
                raise ValueError("genuine previous support source required: set SIM_INFRA_PREVIOUS_REF")
            repo = Path(args.repo)
            stage_official(repo / "scripts/install.sh", args.vendor, repo / "internal/infraartifact/testdata/catalog.json")
            stage_support(repo, args.vendor, args.previous_ref)
        elif args.command == "stage-official":
            stage_official(args.installer, args.vendor, args.catalog)
        elif args.command == "stage-support":
            stage_support(args.repo, args.vendor, args.previous_ref)
        else:
            verify_official(args.vendor, args.installer, args.binary_root)
            if not args.official_only:
                verify_support(args.vendor, args.support_root, args.binary_root)
            print("infra supply verified")
    except (OSError, ValueError, KeyError, TypeError, tarfile.TarError, subprocess.SubprocessError) as error:
        parser.exit(2, f"infra-artifact: {error}\n")


if __name__ == "__main__":
    main()
