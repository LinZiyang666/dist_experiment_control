#!/usr/bin/env python3
"""Supply six mechanism images and join source, vendor, COPY and P04 evidence.

Stage must run under the shared FIX-ORPHAN adapter (stage.sh does this). Verify,
image and join are read-only, never build/download/repair. Selftests use private
filesystem fixtures, without compiling or starting a product executable.
"""
import argparse
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import stat
import subprocess
import tarfile
import tempfile


HERE = Path(__file__).resolve().parent
GENERATIONS = tuple("T" + str(i) for i in range(6))
ROLES = ("agent", "broker", "ctl")
DOMAINS = ("exec-v1", "pty-v1", "flow-v1", "ss-v1", "broker-image-v1", "transfer-v1")
BUNDLE = "infra/fullstack"
CATALOG = BUNDLE + "/catalog.json"
COMPAT_NAMES = ("tether-s8-a", "tether-s8-b", "tether-s8-a-implementation", "tether-s8-b-implementation",
                "tether-prev", "tether-dynamic-prev", "tether-flow-legacy")
STAMP_FIELDS = {"process", "tx_id", "cut_id", "direction", "query_nonce", "scope_digest",
                "generation", "revision", "sequence", "at_boot_ns"}


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Reuse the real suppliers and Dockerfile reverse scanner. No second pin,
# adjacent-release selection algorithm or permissive COPY parser lives here.
INFRA = load("fullstack_infra_supplier", HERE / "infra-artifact.py")
IMAGE = load("fullstack_image_supplier", HERE / "infra-image-manifest.py")
S8 = importlib.util.module_from_spec(importlib.util.spec_from_loader("fullstack_s8_supplier", loader=None))
_s8_source = (HERE / "upgrade-artifacts.sh").read_text().split("<<'PYARTIFACTS'\n", 1)[1].split("\nPYARTIFACTS", 1)[0]
exec(compile(_s8_source, str(HERE / "upgrade-artifacts.sh"), "exec"), S8.__dict__)


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def canonical(value):
    return IMAGE.canonical(value)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def read(path):
    return INFRA.read_json(path)


def file_sha(path):
    return INFRA.regular(path)


def safe_path(root, name):
    name = IMAGE.relative(name).as_posix()
    IMAGE.require_parents(root, name)
    return Path(root) / name


def source_snapshot(repo):
    """Archive actual tracked/dirty/untracked inputs, never another worktree."""
    repo = Path(repo)
    names = S8.git(repo, "ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0")
    overlays, overlay_sha = S8.overlay_inputs(repo)
    rows, contents = [], {}
    for raw in sorted(set(names) - {b""} | {os.fsencode(name) for name in overlays}):
        name = os.fsdecode(raw)
        name = IMAGE.relative(name).as_posix()
        path = repo / name
        replacement = overlays.get(name, None)
        if replacement == "" or (replacement is None and not path.exists() and not path.is_symlink()):
            continue  # A tracked deletion is also bound by the dirty diff below.
        path = safe_path(repo, name)
        info = path.lstat() if path.exists() else Path(replacement).lstat()
        require(stat.S_ISREG(info.st_mode), "source input must be a regular file")
        payload = Path(replacement).read_bytes() if replacement else path.read_bytes()
        rows.append(dict(path=name, type="file", mode=stat.S_IMODE(info.st_mode), size=len(payload), sha256=digest(payload)))
        contents[name] = payload
    require(rows, "source snapshot is empty")
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for row in rows:
            member = tarfile.TarInfo(row["path"])
            member.size, member.mode = row["size"], row["mode"]
            archive.addfile(member, io.BytesIO(contents[row["path"]]))
    head = S8.git(repo, "rev-parse", "HEAD").decode().strip()
    require(re.fullmatch(r"[0-9a-f]{40}", head), "source commit is missing")
    dirty = dict(patch_sha256=digest(S8.git(repo, "diff", "--binary", "--no-ext-diff", "--no-textconv", "HEAD", "--")),
                 overlay_sha256=overlay_sha, inputs=rows)
    return dict(source_commit=head, tree_digest=digest(canonical(rows)), dirty_digest=digest(canonical(dirty)),
                archive_sha=digest(stream.getvalue()), inputs=rows), stream.getvalue()


def flags(release):
    return S8.flags(release)


def capabilities(output, release):
    lines = output.splitlines()
    require(lines and re.fullmatch(r"tether " + re.escape(release) + r" \(proto v[0-9]+\)", lines[0]), "mechanism release probe mismatch")
    def one(prefix):
        matches = [line.removeprefix(prefix) for line in lines if line.startswith(prefix)]
        require(len(matches) == 1, "missing or duplicate decoder declaration: " + prefix)
        return matches[0]
    epoch = one("image-abi v")
    readers = one("image-read-epochs ").split()
    domains = one("image-domains ").split()
    require(epoch.isdecimal() and int(epoch) > 0 and readers and all(v.isdecimal() for v in readers), "invalid image epochs")
    require(len(readers) == len(set(readers)) and int(epoch) in list(map(int, readers)), "image cannot read its own epoch")
    require(len(domains) == len(set(domains)) and set(DOMAINS) <= set(domains), "mechanism decoder domains incomplete")
    require(all(token in output.split() for token in ("upgrade-tx-v1", "authority-incarnation-v1", "upgrade-cut-v1")), "mechanism upgrade capabilities incomplete")
    return dict(image_epoch=int(epoch), read_epochs=list(map(int, readers)), required_domains=domains)


def bundle_file(vendor, source, destination):
    path = Path(vendor) / BUNDLE / destination
    path.parent.mkdir(parents=True, exist_ok=True)
    require(stat.S_ISREG(Path(source).lstat().st_mode), "supplier input must be regular")
    shutil.copyfile(source, path)
    path.chmod(stat.S_IMODE(Path(source).stat().st_mode))
    return path.relative_to(vendor).as_posix()


def supplier_evidence(repo, vendor, a_ref=None, b_ref=None):
    """Invoke S8/S9 validation, without claiming their behavior receipts."""
    official = INFRA.verify_official(vendor, Path(repo) / "scripts/install.sh")
    support = INFRA.verify_support(vendor)
    pair = read(Path(vendor) / "upgrade-pair.json")
    require(type(pair["schema_version"]) is int and pair["schema_version"] == 1
            and pair["a"]["commit"] != pair["b"]["commit"], "genuine S8 pair source is missing")
    refs = (a_ref or pair["a"]["tag"], b_ref or pair["b"]["tag"])
    for label, ref in zip(("a", "b"), refs):
        verified = S8.verify(repo, vendor, ref, "tether-s8-" + label)
        require(verified == pair[label], "S8 pair provenance mismatch")
        impl = S8.implementation_digest(repo, verified["commit"])
        require(impl == pair["implementation_digests"][0 if label == "a" else 1], "S8 implementation digest mismatch")
        proof = pair["compiled_implementation_proofs"][0 if label == "a" else 1]
        expected, _ = S8.expected_source(repo, ref, "tether-s8-" + label + "-implementation", version="v0.0.0-s8-implementation-proof")
        expected["build_package"] = "./cmd/tether"
        require(S8.cache_ok(Path(vendor) / expected["component"], Path(vendor) / (expected["component"] + "-source.json"), expected), "S8 compiled proof is stale")
        require(read(Path(vendor) / (expected["component"] + "-source.json")) == proof, "S8 compiled proof differs from pair")
    require(pair["implementation_digests"][0] != pair["implementation_digests"][1]
            and pair["compiled_implementation_proofs"][0]["binary_sha256"] != pair["compiled_implementation_proofs"][1]["binary_sha256"], "S8 pair is a version-only variant")
    dynamic_ref = S8.run(["sh", "-c", '. "$1"; sim_prev_tag', "sh", str(HERE / "stage.sh")],
                        env=dict(os.environ, SIM_REPO=str(repo))).decode().strip()
    dynamic = S8.verify(repo, vendor, dynamic_ref, "tether-dynamic-prev")
    fixed = S8.verify(repo, vendor, S8.FIXED_TAG, "tether-prev", S8.FIXED_COMMIT)
    legacy = S8.verify(repo, vendor, S8.FIXED_TAG, "tether-flow-legacy", S8.FIXED_COMMIT)
    return dict(s8_pair=pair, dynamic_previous=dynamic, fixed_previous=fixed, flow_legacy=legacy,
                official=official, support=support)


def compatibility_records(upstream):
    pair = upstream["s8_pair"]
    return dict(zip(COMPAT_NAMES, (pair["a"], pair["b"], *pair["compiled_implementation_proofs"],
                                  upstream["fixed_previous"], upstream["dynamic_previous"], upstream["flow_legacy"])))


def stage(repo, vendor, a_ref, b_ref, version_prefix):
    vendor = Path(vendor).resolve()
    directory = vendor / BUNDLE
    directory.mkdir(parents=True, exist_ok=True)
    (vendor / CATALOG).unlink(missing_ok=True)
    require(a_ref and b_ref, "set SIM_FULLSTACK_A_REF and SIM_FULLSTACK_B_REF to genuine S8 sources")
    require(re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+-[A-Za-z0-9.-]+", version_prefix), "invalid mechanism version prefix")
    # Parent supervision owns S8's probes and every compiler descendant too.
    S8.pair(repo, vendor, a_ref, b_ref)
    upstream = supplier_evidence(repo, vendor, a_ref, b_ref)
    source, archive = source_snapshot(repo)
    # As in S8, archive_sha identifies the source archive. Product staging
    # consumes the separately hashed executable; P18 owns its stage receipt.
    archive_path = directory / "source.tar"
    archive_path.write_bytes(archive)
    archive_path.chmod(0o644)
    records = []
    with tempfile.TemporaryDirectory(prefix="fullstack-build-") as temporary:
        root = Path(temporary)
        with tarfile.open(fileobj=io.BytesIO(archive)) as package:
            package.extractall(root, filter="data")
        env = S8.build_env()
        env["GOFLAGS"] = shlex.join([v for v in shlex.split(env["GOFLAGS"]) if not v.startswith("-overlay=")])
        toolchain = S8.run(["go", "env", "GOVERSION"], cwd=root, env=env).decode().strip()
        for generation in GENERATIONS:
            release = version_prefix + "-" + generation.lower()
            binary = directory / generation / "tether"
            binary.parent.mkdir(exist_ok=True)
            argv = ["go", "build", *flags(release), "-o", str(binary), "./cmd/tether"]
            S8.run(argv, cwd=root, env=env)
            binary.chmod(0o755)
            info = S8.build_info(binary)
            require(info["toolchain"] == toolchain, "mechanism toolchain changed")
            probe = S8.version_probe(binary)
            records.append(dict(generation=generation, release=release, source_commit=source["source_commit"],
                                tree_digest=source["tree_digest"], dirty_digest=source["dirty_digest"],
                                toolchain=toolchain, flags=flags(release), goflags=env["GOFLAGS"],
                                flags_digest=digest(canonical(dict(argv=flags(release), goflags=env["GOFLAGS"],
                                                                  cgo="0", goos="linux", goarch="amd64"))),
                                archive_sha=source["archive_sha"], archive_path=BUNDLE + "/source.tar",
                                binary_path=binary.relative_to(vendor).as_posix(), binary_sha=file_sha(binary),
                                vendor_sha=file_sha(binary), build_info=info, version_probe=probe,
                                **capabilities(probe, release)))
    # Preserve release, ABI and mechanism namespaces separately. Copy the
    # supplier's archives as well as binaries so provenance survives Docker COPY.
    supplied = []
    for name in COMPAT_NAMES:
        for suffix in ("", "-source.json"):
            supplied.append(bundle_file(vendor, vendor / (name + suffix), "compatibility/" + name + suffix))
        receipt = read(vendor / (name + "-source.json"))
        supplied.append(bundle_file(vendor, receipt["source_archive"], "compatibility/" + name + ".tar"))
    supplied.append(bundle_file(vendor, vendor / "upgrade-pair.json", "compatibility/upgrade-pair.json"))
    bindings = [dict(path=path, **IMAGE.metadata(vendor / path)) for path in sorted(supplied)]
    value = dict(schema_version=1, purpose="mechanism-only", source=source, mechanisms=records,
                 roles=list(ROLES), upstream=upstream, supplied=bindings, running_evidence="NOT_RUN")
    validate_catalog(value, vendor)
    # A concurrent source edit cannot silently publish builds from two trees.
    require(source_snapshot(repo)[0] == source, "source changed during fullstack supply")
    INFRA.write_json(vendor / CATALOG, value)
    return value


def validate_catalog(value, vendor):
    require(set(value) == {"schema_version", "purpose", "source", "mechanisms", "roles", "upstream", "supplied", "running_evidence"}
            and type(value["schema_version"]) is int and value["schema_version"] == 1, "fullstack catalog schema mismatch")
    require(value["purpose"] == "mechanism-only" and value["roles"] == list(ROLES)
            and value["running_evidence"] == "NOT_RUN", "supply cannot claim running or release compatibility")
    records = value["mechanisms"]
    require([row["generation"] for row in records] == list(GENERATIONS), "all six mechanism generations are required")
    # origin: hot-update S10 plan §2.1 / FullstackArtifactSupplyTest. A/B/A
    # reuse and tether-next cannot impersonate five novel successor images.
    require(len({row["binary_sha"] for row in records}) == 6 and len({row["release"] for row in records}) == 6,
            "mechanism generations must have six distinct executable SHAs")
    source = value["source"]
    require(digest(canonical(source["inputs"])) == source["tree_digest"], "source tree digest mismatch")
    require(len({(row["toolchain"], row["goflags"]) for row in records}) == 1, "mechanism toolchain or nonstamp flags differ")
    for row in records:
        require(set(row) == {"generation", "release", "source_commit", "tree_digest", "dirty_digest", "toolchain",
                             "flags", "goflags", "flags_digest", "archive_sha", "archive_path", "binary_path",
                             "binary_sha", "vendor_sha", "build_info", "version_probe", "image_epoch",
                             "read_epochs", "required_domains"}, "mechanism row schema mismatch")
        require(row["binary_path"] == BUNDLE + "/" + row["generation"] + "/tether"
                and row["archive_path"] == BUNDLE + "/source.tar", "mechanism namespace mismatch")
        for field in ("source_commit", "tree_digest", "dirty_digest", "archive_sha"):
            require(row[field] == source[field], "mechanism source identity differs: " + field)
        require(row["flags"] == flags(row["release"]), "mechanism flags differ from release recipe")
        expected_flags = digest(canonical(dict(argv=row["flags"], goflags=row["goflags"], cgo="0", goos="linux", goarch="amd64")))
        require(expected_flags == row["flags_digest"], "build flags digest mismatch")
        require(capabilities(row["version_probe"], row["release"]) == {key: row[key] for key in ("image_epoch", "read_epochs", "required_domains")}, "decoder declaration mismatch")
        actual = INFRA.regular(safe_path(vendor, row["binary_path"]), 0o755)
        require(actual == row["binary_sha"] == row["vendor_sha"], "staged mechanism bytes mismatch")
        require(INFRA.regular(safe_path(vendor, row["archive_path"]), 0o644) == row["archive_sha"], "mechanism source archive mismatch")
        info = row["build_info"]
        require(info["toolchain"] == row["toolchain"] and info["settings"].get("GOOS") == "linux"
                and info["settings"].get("GOARCH") == "amd64" and info["settings"].get("CGO_ENABLED") == "0"
                and info["settings"].get("-trimpath") == "true", "mechanism embedded build recipe mismatch")
    expected = sorted([BUNDLE + "/compatibility/" + name + suffix for name in COMPAT_NAMES
                       for suffix in ("", "-source.json", ".tar")] + [BUNDLE + "/compatibility/upgrade-pair.json"])
    require([row["path"] for row in value["supplied"]] == expected, "compatibility supply inventory mismatch")
    for row in value["supplied"]:
        require(row == dict(path=row["path"], **IMAGE.metadata(safe_path(vendor, row["path"]))), "compatibility bundle mismatch")
    # Raw supplier receipts remain unchanged. Rewriting both a copied binary
    # and its local hash row cannot break the join to the genuine S8 source.
    for name, receipt in compatibility_records(value["upstream"]).items():
        root = Path(vendor) / BUNDLE / "compatibility"
        require(read(root / (name + "-source.json")) == receipt, "copied compatibility source receipt mismatch")
        require(file_sha(root / name) == receipt["binary_sha256"]
                and file_sha(root / (name + ".tar")) == receipt["archive_sha256"], "copied compatibility bytes differ from supplier")
    require(read(Path(vendor) / BUNDLE / "compatibility/upgrade-pair.json") == value["upstream"]["s8_pair"], "copied S8 pair differs from supplier")


def verify(repo, vendor):
    value = read(Path(vendor) / CATALOG)
    validate_catalog(value, vendor)
    # origin: hot-update S10 plan §2.1 / FullstackArtifactSupplyTest. Rebuilding
    # Docker from yesterday's vendor does not bind today's source tree.
    require(source_snapshot(repo)[0] == value["source"], "stale fullstack vendor: restage before image build")
    require(supplier_evidence(repo, vendor) == value["upstream"], "upstream supply changed")
    for row in value["mechanisms"]:
        require(S8.build_info(safe_path(vendor, row["binary_path"])) == row["build_info"], "mechanism embedded buildinfo mismatch")
    return value


def image_chain(value, vendor, context, report, root, image_id):
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", image_id), "immutable Docker image ID required")
    manifest = IMAGE.compare_report(context, report)
    IMAGE.verify_targets(manifest, root, Path(context) / "Dockerfile")
    copies = {row["source"]: row for row in manifest["copies"]}
    # origin: hot-update S10 plan §2.1 / FullstackArtifactSupplyTest. Reverse
    # scan the entire bundle; missing COPY cannot disappear from its own list.
    required = [Path(vendor) / BUNDLE] + sorted((Path(vendor) / BUNDLE).rglob("*"))
    for path in required:
        source = "vendor/" + path.relative_to(vendor).as_posix()
        require(source in copies, "fullstack COPY input missing: " + source)
        row = copies[source]
        require(row["target"] == "/opt/sim/" + path.relative_to(vendor).as_posix(), "fullstack COPY destination mismatch")
    result = []
    copy_digest = file_sha(report)
    for row in value["mechanisms"]:
        copied = copies["vendor/" + row["binary_path"]]
        require(copied["sha256"] == row["vendor_sha"], "vendor/COPY hash join failed")
        for role in ROLES:
            result.append(dict(id=row["generation"] + "-" + role, role=role, release=row["release"],
                               source_commit=row["source_commit"], toolchain=row["toolchain"], image_id=image_id,
                               tree=row["tree_digest"], dirty=row["dirty_digest"], flags=row["flags_digest"],
                               archive=row["archive_sha"], binary=row["binary_sha"], vendor=row["vendor_sha"],
                               copy_manifest=copy_digest, epoch=row["image_epoch"], read_epochs=row["read_epochs"],
                               domains=row["required_domains"]))
    return result


def stamp_matches(before, after):
    require(set(before) == STAMP_FIELDS and set(after) == STAMP_FIELDS, "kernel stamp schema mismatch")
    # The initial T0 image may have no transaction/cut yet. P04 compares those
    # fields exactly, including zero values; only the fresh query is mandatory.
    require(all(type(before[field]) is str for field in ("tx_id", "cut_id", "query_nonce", "scope_digest", "direction"))
            and before["query_nonce"] and before["direction"] in ("forward", "reverse"), "kernel stamp is incomplete")
    fixed = STAMP_FIELDS - {"sequence", "at_boot_ns"}
    require(all(before[field] == after[field] for field in fixed), "process or image changed during kernel read")
    for field in ("generation", "revision", "sequence", "at_boot_ns"):
        require(type(before[field]) is int and type(after[field]) is int and 0 <= before[field] <= after[field], "kernel sequence or time regressed")
    process = before["process"]
    require(set(process) == {"key", "boot_id", "pidns", "instance_id", "pid", "start_ticks"}, "kernel process schema mismatch")
    require(type(process["pid"]) is int and process["pid"] > 0 and type(process["start_ticks"]) is int
            and process["start_ticks"] > 0 and all(process[k] for k in ("boot_id", "pidns", "instance_id")), "kernel process identity missing")
    key = process["key"]
    require(set(key) == {"role", "host", "unit", "sid", "nid", "domain", "uid"}
            and all(key[field] for field in ("host", "unit", "domain")) and type(key["uid"]) is int
            and 0 <= key["uid"] <= 0xffffffff, "kernel runtime key missing")


def join_execution(artifact, expected_stamp, raw_ref):
    require(set(raw_ref) == {"path", "sha", "bytes", "schema"} and raw_ref["schema"] == "fullstack-kernel-identity-v1", "P04 kernel raw reference required")
    path = Path(raw_ref["path"])
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= 16 << 20, "P04 raw evidence must be bounded and regular")
    raw = path.read_bytes()
    require(type(raw_ref["bytes"]) is int and digest(raw) == raw_ref["sha"] and len(raw) == raw_ref["bytes"], "P04 raw evidence digest mismatch")
    value = json.loads(raw, object_pairs_hook=IMAGE.unique)
    require(set(value) == {"schema", "before", "after", "original", "current", "product"}
            and value["schema"] == raw_ref["schema"], "P04 kernel evidence required")
    stamp_matches(expected_stamp, value["before"])
    stamp_matches(value["before"], value["after"])
    require(value["before"]["process"]["key"]["role"] == artifact["role"], "running role differs from artifact")
    original, current = value["original"], value["current"]
    require(set(original) == {"device", "inode", "sha"} and original == current
            and type(original["device"]) is int and original["device"] >= 0
            and type(original["inode"]) is int and original["inode"] > 0, "P04 independent executable FD evidence missing")
    # origin: hot-update S10 plan §2.1 / FullstackArtifactSupplyTest. A matching
    # disk path or product claim cannot replace the independent /proc/exe hash.
    require(original["sha"] == artifact["binary"], "executing SHA differs from supplied artifact")
    product = value["product"]
    stamp_matches(value["before"], product["stamp"])
    require(product["complete"] is True and product["exe_sha"] == original["sha"], "product and kernel observations disagree")
    return dict(artifact=artifact, stamp=value["after"], running_sha=original["sha"], raw=raw_ref)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for operation in ("stage", "verify", "image", "join"):
        child = sub.add_parser(operation)
        child.add_argument("repo")
        child.add_argument("vendor")
        if operation == "stage":
            child.add_argument("--a-ref", required=True)
            child.add_argument("--b-ref", required=True)
            child.add_argument("--version-prefix", default="v0.0.0-s10")
        if operation in ("image", "join"):
            child.add_argument("context")
            child.add_argument("report")
            child.add_argument("root")
            child.add_argument("image_id")
        if operation == "join":
            child.add_argument("bindings", help="nonempty [{artifact_id,stamp,raw}] using P04 raw references")
    args = parser.parse_args()
    try:
        if args.command == "stage":
            result = stage(Path(args.repo).resolve(), args.vendor, args.a_ref, args.b_ref, args.version_prefix)
        else:
            result = verify(args.repo, args.vendor)
            if args.command in ("image", "join"):
                result = image_chain(result, args.vendor, args.context, args.report, args.root, args.image_id)
            if args.command == "join":
                artifacts = {row["id"]: row for row in result}
                bindings = read(args.bindings)
                require(type(bindings) is list and bindings, "running evidence bindings are empty")
                require(all(set(row) == {"artifact_id", "stamp", "raw"} for row in bindings), "running binding schema mismatch")
                result = [join_execution(artifacts[row["artifact_id"]], row["stamp"], row["raw"]) for row in bindings]
        print(canonical(result).decode(), end="")
    except (ValueError, KeyError, IndexError, TypeError, OSError, tarfile.TarError, subprocess.SubprocessError) as error:
        parser.exit(2, "fullstack-artifacts: " + str(error) + "\n")


if __name__ == "__main__":
    main()
