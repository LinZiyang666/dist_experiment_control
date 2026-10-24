#!/usr/bin/env python3
"""One support-manifest producer for GoReleaser and the hermetic supply builder.

Only build inputs and executable bytes are read; no candidate executable runs.
Per-target post-build records avoid depending on GoReleaser's parallel build order.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

SUPPORT = ("tether-infra-helper", "tether-infra-launch")


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(path):
    with Path(path).open("rb") as incoming:
        return hashlib.file_digest(incoming, "sha256").hexdigest()


def source_inputs(root, component, arch="amd64"):
    """Bind the actual target's repo dependency closure, including embedded data."""
    root = Path(root).resolve()
    env = dict(os.environ, CGO_ENABLED="0", GOOS="linux", GOARCH=arch)
    raw = subprocess.check_output(["go", "list", "-deps", "-json", "./cmd/" + component],
                                  cwd=root, env=env, timeout=120, text=True)
    decoder, offset, inputs = json.JSONDecoder(), 0, {"go.mod", "go.sum"}
    while offset < len(raw):
        offset += len(raw[offset:]) - len(raw[offset:].lstrip())
        if offset == len(raw):
            break
        package, offset = decoder.raw_decode(raw, offset)
        directory = Path(package["Dir"])
        if not directory.is_relative_to(root):
            continue
        for kind in ("GoFiles", "CgoFiles", "CFiles", "HFiles", "SFiles", "EmbedFiles"):
            for name in package.get(kind, []):
                inputs.add((directory / name).relative_to(root).as_posix())
    rows = [{"path": name, "sha256": digest(root / name)} for name in sorted(inputs)]
    return rows, hashlib.sha256(canonical(rows)).hexdigest()


def manifest_bytes(release, arch, records, catalog_sha):
    # origin: S9 internal review rv-priv F2
    if not re.fullmatch(r"v[0-9][A-Za-z0-9.+_-]{0,127}", release) or arch not in ("amd64", "arm64"):
        raise ValueError("invalid support release/platform")
    if [item["component"] for item in records] != list(SUPPORT):
        raise ValueError("support manifest requires both ordered executables")
    sources, hashes = [], []
    for record in records:
        for field in ("source_sha256", "binary_sha256"):
            if not re.fullmatch(r"[0-9a-f]{64}", record[field]) or record[field] == "0" * 64:
                raise ValueError("invalid support digest")
        if (record["protocol_version"], record["allowed_previous_reader"]) != (1, 1):
            raise ValueError("unsupported reader protocol")
        sources.append({"component": record["component"], "source_sha256": record["source_sha256"]})
        hashes.append("sha256 " + record["binary_sha256"] + " " + record["component"])
    if not re.fullmatch(r"[0-9a-f]{64}", catalog_sha) or catalog_sha == "0" * 64:
        raise ValueError("invalid catalog digest")
    source_sha = hashlib.sha256(canonical(sources)).hexdigest()
    lines = ["format 1", "release " + release, "goos linux", "goarch " + arch,
             "protocol 1", "previous-reader 1", "source-sha256 " + source_sha,
             *hashes, "sha256 " + catalog_sha + " infra-catalog.json"]
    return ("\n".join(lines) + "\n").encode()


def publish(path, data):
    fd, pending = tempfile.mkstemp(prefix=".support-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(data)
            out.flush()
            os.fchmod(out.fileno(), 0o644)
            os.fsync(out.fileno())
        os.replace(pending, path)
    finally:
        Path(pending).unlink(missing_ok=True)


def post_build(root, dist, release, goos, arch, binary):
    binary, root = Path(binary).resolve(), Path(root).resolve()
    if goos != "linux" or arch not in ("amd64", "arm64") or binary.name not in SUPPORT:
        raise ValueError("unsupported support build target")
    header = binary.read_bytes()[:20]
    machine = {"amd64": b"\x3e\x00", "arm64": b"\xb7\x00"}[arch]
    if header[:7] != b"\x7fELF\x02\x01\x01" or header[18:20] != machine:
        raise ValueError("support binary has wrong ELF target")
    _, source_sha = source_inputs(root, binary.name, arch)
    record = {"component": binary.name, "source_sha256": source_sha,
              "binary_sha256": digest(binary), "protocol_version": 1,
              "allowed_previous_reader": 1, "release": release, "path": str(binary)}
    directory = (Path(dist) if dist else binary.parent.parent) / "infra-support" / (goos + "_" + arch)
    directory.mkdir(parents=True, exist_ok=True)
    # The last completed build emits the manifest from both exact binaries.
    # Archives are created after all build hooks finish; no polling or fake hash.
    with (directory / "manifest.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        publish(directory / (binary.name + ".json"), canonical(record))
        paths = [directory / (component + ".json") for component in SUPPORT]
        if not all(path.is_file() for path in paths):
            return
        records = [json.loads(path.read_bytes()) for path in paths]
        if any(item["release"] != release or digest(item["path"]) != item["binary_sha256"] for item in records):
            raise ValueError("stale support build record; use a clean distribution directory")
        catalog = root / "internal/infraartifact/testdata/catalog.json"
        publish(directory / "infra-catalog.json", catalog.read_bytes())
        publish(directory / "infra-support.manifest", manifest_bytes(release, arch, records, digest(catalog)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist")
    for name in ("root", "release", "goos", "goarch", "binary"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    post_build(args.root, args.dist, args.release, args.goos, args.goarch, args.binary)
