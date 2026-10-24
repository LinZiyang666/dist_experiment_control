#!/usr/bin/env python3
"""caddy-artifact.py — stage and bind the sim's Linux amd64 Caddy artifact.

Requires Python 3.9+. Downloads follow the production installer's official release/checksum policy.
No binary is executed on the staging host. The same verifier runs before Docker
build and inside the image, so interrupted binary/receipt publication fails shut.
There is deliberately no download cache. Callers serialize vendor/image staging.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import struct
import subprocess
import tarfile
import tempfile


MAX_BINARY = 256 << 20
MAX_RECEIPT = 8192
RECEIPT_KEYS = {
    "schema_version", "component", "version", "goos", "goarch",
    "installer_sha256", "archive_sha256", "binary_sha256", "source",
    "archive_checksum_algorithm", "archive_checksum",
}


def selected_version(installer):
    text = Path(installer).read_text()
    pins = re.findall(r'^CADDY_VERSION="\$\{TETHER_CADDY_VERSION:-([0-9]+\.[0-9]+\.[0-9]+)\}"$', text, re.M)
    if len(pins) != 1:
        raise ValueError("installer must contain exactly one CADDY_VERSION default")
    version = os.environ.get("TETHER_CADDY_VERSION") or pins[0]
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version) is None:
        raise ValueError("Caddy version must be one exact numeric release")
    return version


def require_target():
    if (os.environ.get("SIM_TARGET_OS", "linux"), os.environ.get("SIM_TARGET_ARCH", "amd64")) != ("linux", "amd64"):
        raise ValueError("sim tether/NATS/Caddy profile supports only linux/amd64")


def digest(path, algorithm="sha256"):
    result = hashlib.new(algorithm)
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            result.update(chunk)
    return result.hexdigest()


def binary_digest(path, executable=False):
    info = Path(path).lstat()
    if not stat.S_ISREG(info.st_mode) or not 64 <= info.st_size <= MAX_BINARY:
        raise ValueError("Caddy binary must be a bounded regular file")
    if executable and not info.st_mode & 0o111:
        raise ValueError("staged Caddy binary is not executable")
    with Path(path).open("rb") as source:
        header = source.read(64)
    if (header[:7] != b"\x7fELF\x02\x01\x01" or header[7] not in (0, 3)
            or struct.unpack_from("<H", header, 16)[0] not in (2, 3)
            or struct.unpack_from("<H", header, 18)[0] != 62):
        raise ValueError("Caddy binary is not a Linux amd64 ELF image")
    return digest(path)


def archive_checksum(path, filename):
    if Path(path).stat().st_size > 2 << 20:
        raise ValueError("Caddy checksum list is oversized")
    matches = []
    for line in Path(path).read_text().splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[1].removeprefix("*") == filename:
            if len(fields) != 2 or re.fullmatch(r"(?:[a-fA-F0-9]{64}|[a-fA-F0-9]{128})", fields[0]) is None:
                raise ValueError("malformed Caddy archive checksum entry")
            matches.append(fields[0].lower())
    if len(matches) != 1:
        raise ValueError("expected exactly one checksum entry for " + filename)
    return matches[0]


def safe_members(package):
    # origin: hot-update S9 plan §3.1 / T55. Looking up the executable
    # alone would accept an otherwise malicious archive in the supply manifest.
    seen, total = {}, 0
    for member in package.getmembers():
        path = PurePosixPath(member.name)
        if (path.is_absolute() or ".." in path.parts or "\\" in member.name
                or member.name in seen or not (member.isfile() or member.isdir())):
            raise ValueError("archive has an unsafe or duplicate member")
        total += member.size
        if total > 512 << 20 or len(seen) >= 10000:
            raise ValueError("archive expanded size/member count exceeded")
        seen[member.name] = member
    return seen


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate receipt field: " + key)
        result[key] = value
    return result


def verify(binary, receipt, installer):
    require_target()
    info = Path(receipt).lstat()
    if not stat.S_ISREG(info.st_mode) or not 1 <= info.st_size <= MAX_RECEIPT:
        raise ValueError("Caddy receipt must be a bounded regular file")
    raw = Path(receipt).read_bytes()
    value = json.loads(raw, object_pairs_hook=unique_object)
    if not isinstance(value, dict) or set(value) != RECEIPT_KEYS:
        raise ValueError("Caddy receipt has missing or unknown fields")
    version = selected_version(installer)
    expected = {
        "schema_version": 1, "component": "caddy", "version": version,
        "goos": "linux", "goarch": "amd64", "installer_sha256": digest(installer),
        "source": f"https://github.com/caddyserver/caddy/releases/download/v{version}/caddy_{version}_linux_amd64.tar.gz",
    }
    if any(type(value[key]) is not type(item) or value[key] != item for key, item in expected.items()):
        raise ValueError("Caddy receipt does not match installer/version/platform/source")
    for key in ("archive_sha256", "binary_sha256"):
        if not isinstance(value[key], str) or re.fullmatch(r"[a-f0-9]{64}", value[key]) is None:
            raise ValueError("invalid receipt SHA256: " + key)
    algorithm = value["archive_checksum_algorithm"]
    # The production installer accepts SHA256/SHA512; Caddy's release table uses SHA512.
    if not isinstance(algorithm, str) or algorithm not in ("sha256", "sha512"):
        raise ValueError("invalid archive checksum algorithm")
    checksum = value["archive_checksum"]
    length = 64 if algorithm == "sha256" else 128
    if not isinstance(checksum, str) or re.fullmatch(r"[a-f0-9]{%d}" % length, checksum) is None:
        raise ValueError("invalid official archive checksum")
    if algorithm == "sha256" and checksum != value["archive_sha256"]:
        raise ValueError("inconsistent archive SHA256 receipt")
    if binary_digest(binary, executable=True) != value["binary_sha256"]:
        raise ValueError("Caddy binary SHA256 does not match receipt")
    return value


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def staged_copy(source, directory, mode):
    fd, name = tempfile.mkstemp(prefix=".caddy-publish-", dir=directory)
    try:
        with os.fdopen(fd, "wb") as target, Path(source).open("rb") as incoming:
            shutil.copyfileobj(incoming, target)
            target.flush()
            os.fchmod(target.fileno(), mode)
            os.fsync(target.fileno())
        return Path(name)
    except BaseException:
        Path(name).unlink(missing_ok=True)
        raise


def publish(binary, receipt, installer, vendor):
    """Each rename is atomic; receipt last is the commit record, not a pair-rename claim."""
    verify(binary, receipt, installer)
    pending = []
    try:
        pending.append(staged_copy(binary, vendor, 0o755))
        pending.append(staged_copy(receipt, vendor, 0o644))
        os.replace(pending[0], Path(vendor) / "caddy")
        sync_directory(vendor)
        os.replace(pending[1], Path(vendor) / "caddy-source.json")
        sync_directory(vendor)
    finally:
        for path in pending:
            path.unlink(missing_ok=True)


def download(url, destination):
    subprocess.run(["curl", "-fsSL", "--retry", "3", "--max-time", "120", url, "-o", str(destination)],
                   check=True, timeout=600)


def stage(installer, vendor):
    require_target()
    version = selected_version(installer)
    vendor = Path(vendor)
    vendor.mkdir(parents=True, exist_ok=True)
    filename = f"caddy_{version}_linux_amd64.tar.gz"
    base = f"https://github.com/caddyserver/caddy/releases/download/v{version}"
    with tempfile.TemporaryDirectory(prefix=".caddy-stage-", dir=vendor) as work:
        work = Path(work)
        archive, sums = work / filename, work / "checksums.txt"
        download(base + "/" + filename, archive)
        download(base + f"/caddy_{version}_checksums.txt", sums)
        if archive.stat().st_size > 256 << 20:
            raise ValueError("Caddy archive is oversized")
        expected = archive_checksum(sums, filename)
        algorithm = "sha256" if len(expected) == 64 else "sha512"
        if digest(archive, algorithm) != expected:
            raise ValueError("Caddy archive checksum mismatch")
        binary = work / "caddy"
        with tarfile.open(archive, "r:gz") as package:
            safe_members(package)
            matches = [entry for entry in package.getmembers() if entry.name == "caddy"]
            if len(matches) != 1 or not matches[0].isfile() or not 64 <= matches[0].size <= MAX_BINARY:
                raise ValueError("Caddy archive must have one bounded regular caddy member")
            with package.extractfile(matches[0]) as source, binary.open("wb") as target:
                shutil.copyfileobj(source, target)
        binary.chmod(0o755)
        value = {
            "schema_version": 1, "component": "caddy", "version": version,
            "goos": "linux", "goarch": "amd64", "installer_sha256": digest(installer),
            "archive_sha256": digest(archive), "binary_sha256": binary_digest(binary),
            "archive_checksum_algorithm": algorithm, "archive_checksum": expected,
            "source": base + "/" + filename,
        }
        receipt = work / "caddy-source.json"
        receipt.write_text(json.dumps(value, sort_keys=True) + "\n")
        publish(binary, receipt, installer, vendor)
        verify(vendor / "caddy", vendor / "caddy-source.json", installer)
    print(f"staged Caddy {version} linux/amd64 sha256={value['binary_sha256']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    pin = sub.add_parser("pin")
    pin.add_argument("installer")
    staging = sub.add_parser("stage")
    staging.add_argument("installer")
    staging.add_argument("vendor")
    checking = sub.add_parser("verify")
    checking.add_argument("binary")
    checking.add_argument("receipt")
    checking.add_argument("installer")
    args = parser.parse_args()
    try:
        if args.command == "pin":
            print(selected_version(args.installer))
        elif args.command == "stage":
            stage(args.installer, args.vendor)
        else:
            value = verify(args.binary, args.receipt, args.installer)
            print(f"verified Caddy {value['version']} linux/amd64 sha256={value['binary_sha256']}")
    except (OSError, ValueError, tarfile.TarError, subprocess.SubprocessError) as error:
        parser.exit(1, f"caddy-artifact: {error}\n")


if __name__ == "__main__":
    main()
