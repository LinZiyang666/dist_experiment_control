#!/usr/bin/env python3
"""Hermetic filesystem controls; no builds, downloads or product processes.

Real bytes/modes, source inventories, Docker COPY parsing and P04 JSON joins
are exercised. Synthetic supplier metadata tests wiring only, and never signs
upstream decoder, release compatibility or running-product behavior.
"""
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("fullstack_artifact_controls", HERE / "fullstack-artifacts.py")
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


# origin: hot-update S10 plan §2.1 / §10.2 P10 / FullstackArtifactSupplyTest
class FullstackArtifactSupplyTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="fullstack-artifact-controls-")
        self.addCleanup(self.temporary.cleanup)
        self.repo = Path(self.temporary.name) / "repo"
        self.context = self.repo / "test/simcluster"
        self.vendor = self.context / "vendor"
        self.root = Path(self.temporary.name) / "image"
        self.vendor.mkdir(parents=True)
        self.source_names = ["Makefile", "internal/source.go"]
        self.put(self.repo / "Makefile", b"build recipe\n")
        self.put(self.repo / "internal/source.go", b"compiled responsibility\n")
        self.environment = mock.patch.dict(os.environ, {"GOFLAGS": ""})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.git = mock.patch.object(M.S8, "git", side_effect=self.fixture_git)
        self.git.start()
        self.addCleanup(self.git.stop)
        source, archive = M.source_snapshot(self.repo)
        self.put(self.vendor / M.BUNDLE / "source.tar", archive)
        self.value = dict(schema_version=1, purpose="mechanism-only", source=source,
                          roles=list(M.ROLES), mechanisms=[], upstream={"fixture": "not product evidence"},
                          supplied=[], running_evidence="NOT_RUN")
        for generation in M.GENERATIONS:
            release = "v0.0.0-s10-" + generation.lower()
            binary_path = M.BUNDLE + "/" + generation + "/tether"
            binary = self.vendor / binary_path
            self.put(binary, ("mechanism fixture " + generation).encode(), 0o755)
            probe = ("tether " + release + " (proto v2)\nimage-domains " + " ".join(M.DOMAINS)
                     + "\nupgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1\nimage-read-epochs 1\nimage-abi v1\n")
            goflags = "-buildvcs=false"
            row = dict(generation=generation, release=release, source_commit=source["source_commit"],
                       tree_digest=source["tree_digest"], dirty_digest=source["dirty_digest"], toolchain="go1.26.8",
                       flags=M.flags(release), goflags=goflags,
                       flags_digest=M.digest(M.canonical(dict(argv=M.flags(release), goflags=goflags,
                                                             cgo="0", goos="linux", goarch="amd64"))),
                       archive_sha=source["archive_sha"], archive_path=M.BUNDLE + "/source.tar",
                       binary_path=binary_path, binary_sha=M.file_sha(binary), vendor_sha=M.file_sha(binary),
                       build_info=dict(toolchain="go1.26.8", settings={"GOOS": "linux", "GOARCH": "amd64",
                                                                     "CGO_ENABLED": "0", "-trimpath": "true"}),
                       version_probe=probe, **M.capabilities(probe, release))
            self.value["mechanisms"].append(row)
        receipts = {}
        for name in M.COMPAT_NAMES:
            root = self.vendor / M.BUNDLE / "compatibility"
            self.put(root / name, ("independent release fixture " + name).encode())
            self.put(root / (name + ".tar"), ("independent source fixture " + name).encode())
            receipt = dict(binary_sha256=M.file_sha(root / name), archive_sha256=M.file_sha(root / (name + ".tar")))
            receipts[name] = receipt
            self.put(root / (name + "-source.json"), M.canonical(receipt))
        pair = dict(a=receipts["tether-s8-a"], b=receipts["tether-s8-b"],
                    compiled_implementation_proofs=[receipts["tether-s8-a-implementation"], receipts["tether-s8-b-implementation"]])
        self.value["upstream"] = dict(s8_pair=pair, fixed_previous=receipts["tether-prev"],
                                      dynamic_previous=receipts["tether-dynamic-prev"], flow_legacy=receipts["tether-flow-legacy"])
        self.put(self.vendor / M.BUNDLE / "compatibility/upgrade-pair.json", M.canonical(pair))
        compat = [name + suffix for name in M.COMPAT_NAMES for suffix in ("", "-source.json", ".tar")]
        for name in sorted(compat + ["upgrade-pair.json"]):
            path = M.BUNDLE + "/compatibility/" + name
            self.value["supplied"].append(dict(path=path, **M.IMAGE.metadata(self.vendor / path)))
        self.save_catalog()
        self.image_id = "sha256:" + "b" * 64
        for name in M.IMAGE.BUILD_INPUTS:
            self.put(self.context / name, ("build input " + name).encode())
        self.put(self.context / "image/config", b"nonbinary COPY input\n")
        self.put(self.vendor / "infra/build-source.json", M.canonical(dict(schema_version=1,
                 inputs=[dict(path="Makefile", **M.IMAGE.metadata(self.repo / "Makefile"))])))
        self.dockerfile = self.context / "Dockerfile"
        self.put(self.dockerfile, b"FROM fixture\nCOPY vendor/infra/ /opt/sim/infra/\n"
                 b"COPY image/config /opt/sim/config\nCOPY Dockerfile /opt/sim/Dockerfile\n"
                 b"COPY vendor/infra-image-inputs.json /opt/sim/infra-image-inputs.json\n")
        self.report = self.vendor / "infra-image-inputs.json"
        self.make_image()

    def put(self, path, payload, mode=0o644):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        path.chmod(mode)

    def fixture_git(self, repo, *args):
        # Git discovery is a boundary fixture; the snapshot still reads and
        # archives the actual files, including edits, modes and untracked bytes.
        if args[0] == "ls-files":
            return ("\0".join(self.source_names) + "\0").encode()
        if args[0] == "rev-parse":
            return ("a" * 40 + "\n").encode()
        if args[0] == "diff":
            return b""
        raise AssertionError("unexpected git operation")

    def save_catalog(self):
        M.INFRA.write_json(self.vendor / M.CATALOG, self.value)

    def make_image(self):
        if self.root.exists():
            shutil.rmtree(self.root)
        manifest = M.IMAGE.snapshot(self.context)
        M.IMAGE.write(self.report, manifest)
        for row in manifest["copies"]:
            destination = self.root / row["target"].lstrip("/")
            if row["type"] == "directory":
                destination.mkdir(parents=True, exist_ok=True)
            else:
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(self.context / row["source"], destination)
            destination.chmod(row["mode"])
        return manifest

    def chain(self):
        return M.image_chain(self.value, self.vendor, self.context, self.report, self.root, self.image_id)

    def verified(self):
        with mock.patch.object(M, "supplier_evidence", return_value=self.value["upstream"]), \
             mock.patch.object(M.S8, "build_info", return_value=self.value["mechanisms"][0]["build_info"]):
            return M.verify(self.repo, self.vendor)

    def execution(self, artifact=None):
        artifact = artifact or self.chain()[3]
        key = dict(role=artifact["role"], host="agent-host", unit="runtime-one", sid="sid-one",
                   nid="nid-one", domain="domain-one", uid=1000)
        stamp = dict(process=dict(key=key, boot_id="boot-one", pidns="pid:[42]", instance_id="instance-one",
                                  pid=4242, start_ticks=1234), tx_id="tx-one", cut_id="cut-one",
                     direction="forward", query_nonce="fresh-query", scope_digest="scope-one",
                     generation=2, revision=1, sequence=3, at_boot_ns=5678)
        identity = dict(device=1, inode=567, sha=artifact["binary"])
        value = dict(schema="fullstack-kernel-identity-v1", before=copy.deepcopy(stamp), after=copy.deepcopy(stamp),
                     original=identity, current=copy.deepcopy(identity),
                     product=dict(stamp=copy.deepcopy(stamp), complete=True, exe_sha=artifact["binary"]))
        return artifact, stamp, value

    def seal_raw(self, value):
        path = Path(self.temporary.name) / "kernel-identity.json"
        raw = M.canonical(value)
        self.put(path, raw)
        return dict(path=str(path), sha=M.digest(raw), bytes=len(raw), schema="fullstack-kernel-identity-v1")

    def test_source_vendor_copy_and_kernel_join(self):
        self.assertEqual(self.verified(), self.value)
        M.validate_catalog(self.value, self.vendor)
        artifacts = self.chain()
        self.assertEqual(len(artifacts), 18)
        for role in M.ROLES:
            rows = [row for row in artifacts if row["role"] == role]
            self.assertEqual(len({row["binary"] for row in rows}), 6)
        artifact, stamp, value = self.execution()
        result = M.join_execution(artifact, stamp, self.seal_raw(value))
        self.assertEqual(result["running_sha"], artifact["binary"])
        self.assertEqual(result["artifact"]["copy_manifest"], M.file_sha(self.report))
        value["after"]["sequence"] += 1
        value["after"]["at_boot_ns"] += 1
        self.assertEqual(M.join_execution(artifact, stamp, self.seal_raw(value))["stamp"], value["after"])
        _, stamp, value = self.execution(artifact)
        for current in (stamp, value["before"], value["after"], value["product"]["stamp"]):
            current.update(generation=0, tx_id="", cut_id="")
        self.assertEqual(M.join_execution(artifact, stamp, self.seal_raw(value))["stamp"]["generation"], 0)

    def test_old_vendor_rebake_rejects_changed_source(self):
        self.put(self.repo / "internal/source.go", b"new compiled responsibility\n")
        # Docker's inputs and targets remain perfectly valid. Only the source
        # arrow exposes this real stale-vendor failure.
        self.chain()
        with self.assertRaisesRegex(ValueError, "stale fullstack vendor"):
            self.verified()

    def test_source_snapshot_includes_modes_untracked_and_overlay_bytes(self):
        original = M.source_snapshot(self.repo)[0]
        self.put(self.repo / "untracked.go", b"untracked compilation input")
        self.source_names.append("untracked.go")
        changed = M.source_snapshot(self.repo)[0]
        self.assertNotEqual(original["tree_digest"], changed["tree_digest"])
        self.assertNotEqual(original["dirty_digest"], changed["dirty_digest"])
        (self.repo / "untracked.go").chmod(0o755)
        self.assertNotEqual(changed, M.source_snapshot(self.repo)[0])
        replacement = Path(self.temporary.name) / "overlay-source.go"
        replacement.write_bytes(b"actual nested-build mutant bytes")
        with mock.patch.object(M.S8, "overlay_inputs", return_value=({"internal/source.go": str(replacement)}, "c" * 64)):
            overlaid, archive = M.source_snapshot(self.repo)
            self.assertNotEqual(original["dirty_digest"], overlaid["dirty_digest"])
            with M.tarfile.open(fileobj=M.io.BytesIO(archive)) as package:
                self.assertEqual(package.extractfile("internal/source.go").read(), replacement.read_bytes())
        (self.repo / "untracked.go").unlink()
        self.assertNotEqual(changed, M.source_snapshot(self.repo)[0])
        shutil.rmtree(self.repo / "internal")
        self.assertFalse(any(row["path"].startswith("internal/") for row in M.source_snapshot(self.repo)[0]["inputs"]))

    def test_copy_omission_cannot_hide_in_self_generated_manifest(self):
        missing = M.BUNDLE + "/compatibility/tether-dynamic-prev.tar"
        # A self-consistent Dockerfile/report/image can all omit a required
        # archive. The S10 reverse inventory must still refuse it.
        lines = ["FROM fixture", "COPY Dockerfile /opt/sim/Dockerfile",
                 "COPY image/config /opt/sim/config",
                 "COPY vendor/infra-image-inputs.json /opt/sim/infra-image-inputs.json"]
        for path in sorted((self.vendor / "infra").rglob("*")):
            if path.is_file() and path.relative_to(self.vendor).as_posix() != missing:
                name = path.relative_to(self.vendor).as_posix()
                lines.append("COPY vendor/" + name + " /opt/sim/" + name)
        self.put(self.dockerfile, ("\n".join(lines) + "\n").encode())
        self.make_image()
        with self.assertRaisesRegex(ValueError, "COPY input missing"):
            self.chain()

    def test_changed_image_bytes_modes_and_build_inputs_reject(self):
        for name, action in (("copied bytes", lambda: self.put(self.root / "opt/sim/config", b"stale")),
                             ("copied mode", lambda: (self.root / "opt/sim/config").chmod(0o600)),
                             ("build input", lambda: self.put(self.context / "local.sh", b"forgotten rebuild"))):
            with self.subTest(name=name):
                self.make_image()
                action()
                with self.assertRaises(ValueError):
                    self.chain()

    def test_generations_are_distinct_and_complete(self):
        for name in ("ABA", "five images", "different toolchain", "different flags", "role omission"):
            with self.subTest(name=name):
                bad = copy.deepcopy(self.value)
                if name == "ABA":
                    bad["mechanisms"][5]["binary_sha"] = bad["mechanisms"][1]["binary_sha"]
                elif name == "five images":
                    bad["mechanisms"].pop()
                elif name == "different toolchain":
                    bad["mechanisms"][2]["toolchain"] = "go1.26.0"
                elif name == "different flags":
                    bad["mechanisms"][2]["flags"].append("-race")
                else:
                    bad["roles"].pop()
                with self.assertRaises(ValueError):
                    M.validate_catalog(bad, self.vendor)

    def test_five_successors_cannot_reuse_bytes(self):
        first, last = self.value["mechanisms"][1], self.value["mechanisms"][5]
        binary = self.vendor / last["binary_path"]
        self.put(binary, (self.vendor / first["binary_path"]).read_bytes(), 0o755)
        last["binary_sha"] = last["vendor_sha"] = first["binary_sha"]
        with self.assertRaisesRegex(ValueError, "distinct executable SHAs"):
            M.validate_catalog(self.value, self.vendor)

    def test_copied_release_cannot_be_replaced_with_next(self):
        path = self.vendor / M.BUNDLE / "compatibility/tether-dynamic-prev"
        self.put(path, b"same-source next posing as adjacent release")
        for row in self.value["supplied"]:
            if row["path"] == path.relative_to(self.vendor).as_posix():
                row.update(M.IMAGE.metadata(path))
        with self.assertRaisesRegex(ValueError, "differ from supplier"):
            M.validate_catalog(self.value, self.vendor)

    def test_missing_schema_archive_decoder_or_recipe_rejects(self):
        for field in self.value["mechanisms"][0]:
            bad = copy.deepcopy(self.value)
            del bad["mechanisms"][0][field]
            with self.subTest(missing=field), self.assertRaises((ValueError, KeyError)):
                M.validate_catalog(bad, self.vendor)
        bad = copy.deepcopy(self.value)
        bad["mechanisms"][0]["unknown_field"] = "cannot silently disappear"
        with self.assertRaisesRegex(ValueError, "row schema mismatch"):
            M.validate_catalog(bad, self.vendor)
        for name, change in (("missing decoder", lambda row: row.update(required_domains=["exec-v1"])),
                             ("wrong target", lambda row: row["build_info"]["settings"].update(GOOS="darwin")),
                             ("wrong archive", lambda row: row.update(archive_sha="c" * 64))):
            bad = copy.deepcopy(self.value)
            change(bad["mechanisms"][0])
            with self.subTest(name=name), self.assertRaises(ValueError):
                M.validate_catalog(bad, self.vendor)
        bad = copy.deepcopy(self.value)
        bad["supplied"].pop()
        with self.assertRaisesRegex(ValueError, "inventory mismatch"):
            M.validate_catalog(bad, self.vendor)

    def test_binary_and_archive_swaps_or_symlinks_reject(self):
        binary = self.vendor / self.value["mechanisms"][0]["binary_path"]
        for mode in (0o644, 0o777):
            binary.chmod(mode)
            with self.assertRaises(ValueError):
                M.validate_catalog(self.value, self.vendor)
        binary.chmod(0o755)
        original = binary.read_bytes()
        binary.write_bytes(b"tether-next cannot replace T0")
        with self.assertRaisesRegex(ValueError, "bytes mismatch"):
            M.validate_catalog(self.value, self.vendor)
        binary.write_bytes(original)
        binary.unlink()
        binary.symlink_to(self.vendor / self.value["mechanisms"][1]["binary_path"])
        with self.assertRaises(ValueError):
            M.validate_catalog(self.value, self.vendor)

    def test_changed_source_archive_rejects(self):
        archive = self.vendor / self.value["mechanisms"][0]["archive_path"]
        archive.write_bytes(b"archive from another source tree")
        with self.assertRaisesRegex(ValueError, "source archive mismatch"):
            M.validate_catalog(self.value, self.vendor)

    def test_image_requires_immutable_id(self):
        for image_id in ("tether-sim:dev", "", "sha256:missing"):
            with self.subTest(image_id=image_id), self.assertRaisesRegex(ValueError, "image ID"):
                M.image_chain(self.value, self.vendor, self.context, self.report, self.root, image_id)

    def test_disk_sha_cannot_replace_executing_sha(self):
        artifact, stamp, value = self.execution()
        old_sha = self.chain()[0]["binary"]
        value["original"]["sha"] = old_sha
        value["current"]["sha"] = old_sha
        # Disk bytes and product claim both match T1, but the independent P04
        # FD still identifies T0. Neither claim can turn the old process green.
        self.assertEqual(value["product"]["exe_sha"], artifact["binary"])
        with self.assertRaisesRegex(ValueError, "executing SHA differs"):
            M.join_execution(artifact, stamp, self.seal_raw(value))

    def test_exec_aba_stale_nonce_and_wrong_role_reject(self):
        artifact, stamp, good = self.execution()
        for field in ("generation", "cut_id", "query_nonce", "direction", "process", "sequence", "at_boot_ns"):
            bad = copy.deepcopy(good)
            if field in ("generation", "sequence", "at_boot_ns"):
                bad["after"][field] -= 1
            elif field == "process":
                bad["after"][field]["start_ticks"] += 1
            else:
                bad["after"][field] += "-stale"
            with self.subTest(field=field), self.assertRaises(ValueError):
                M.join_execution(artifact, stamp, self.seal_raw(bad))
        wrong = dict(artifact, role="broker")
        with self.assertRaisesRegex(ValueError, "role differs"):
            M.join_execution(wrong, stamp, self.seal_raw(good))

    def test_running_evidence_requires_independent_fd_and_raw_digest(self):
        artifact, stamp, good = self.execution()
        for name, change in (("disk-only", lambda v: v.update(schema="disk-sha-v1")),
                             ("missing fd", lambda v: v["original"].pop("inode")),
                             ("reopened image", lambda v: v["current"].update(inode=999)),
                             ("product claim", lambda v: v["product"].update(exe_sha="c" * 64)),
                             ("incomplete", lambda v: v["product"].update(complete=False))):
            bad = copy.deepcopy(good)
            change(bad)
            with self.subTest(name=name), self.assertRaises(ValueError):
                M.join_execution(artifact, stamp, self.seal_raw(bad))
        reference = self.seal_raw(good)
        reference["sha"] = "c" * 64
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            M.join_execution(artifact, stamp, reference)
        reference = self.seal_raw(good)
        link = Path(self.temporary.name) / "symlink-raw.json"
        link.symlink_to(reference["path"])
        reference["path"] = str(link)
        with self.assertRaisesRegex(ValueError, "bounded and regular"):
            M.join_execution(artifact, stamp, reference)
        path = self.vendor / M.CATALOG
        path.write_bytes(b'{"schema_version":1,"schema_version":1}')
        with self.assertRaisesRegex(ValueError, "duplicate"):
            M.read(path)

    def test_stage_uses_real_suppliers_and_fails_before_build_without_pair(self):
        with mock.patch.object(M.S8, "pair", side_effect=ValueError("real supplier refusal")) as pair:
            with self.assertRaisesRegex(ValueError, "real supplier refusal"):
                M.stage(self.repo, self.vendor, "source-a", "source-b", "v0.0.0-s10")
            pair.assert_called_once_with(self.repo, self.vendor.resolve(), "source-a", "source-b")
            self.assertFalse((self.vendor / M.CATALOG).exists(), "partial supply left an old success catalog")
        with mock.patch.object(M.S8, "run", side_effect=AssertionError("selftest must not build")):
            self.save_catalog()
            with self.assertRaisesRegex(ValueError, "genuine S8"):
                M.stage(self.repo, self.vendor, "", "", "v0.0.0-s10")
            self.assertFalse((self.vendor / M.CATALOG).exists(), "missing pair left an old success catalog")

    def test_supplier_ports_verify_independent_source_and_compiled_proofs(self):
        a = dict(tag="a-ref", commit="a" * 40)
        b = dict(tag="b-ref", commit="b" * 40)
        proofs = [dict(binary_sha256="1" * 64), dict(binary_sha256="2" * 64)]
        pair = dict(schema_version=1, a=a, b=b, implementation_digests=["a-implementation", "b-implementation"],
                    compiled_implementation_proofs=proofs)
        def read_port(path):
            if Path(path).name == "upgrade-pair.json":
                return pair
            return proofs[0 if Path(path).name.startswith("tether-s8-a") else 1]
        def verify_port(repo, vendor, ref, name, pin=""):
            if name.startswith("tether-s8-"):
                return a if name.endswith("-a") else b
            return dict(tag=ref, component=name, pin=pin)
        with mock.patch.object(M, "read", side_effect=read_port), \
             mock.patch.object(M.INFRA, "verify_official", return_value={"catalog": "official"}) as official, \
             mock.patch.object(M.INFRA, "verify_support", return_value={"archives": "support"}) as support, \
             mock.patch.object(M.S8, "verify", side_effect=verify_port) as verify, \
             mock.patch.object(M.S8, "implementation_digest", side_effect=lambda repo, commit: commit[0] + "-implementation"), \
             mock.patch.object(M.S8, "expected_source", side_effect=lambda repo, ref, name, version: (dict(component=name), b"")), \
             mock.patch.object(M.S8, "cache_ok", return_value=True) as cache, \
             mock.patch.object(M.S8, "run", return_value=b"v1.0.0\n"):
            result = M.supplier_evidence(self.repo, self.vendor)
            self.assertEqual(result["official"], {"catalog": "official"})
            self.assertEqual(result["support"], {"archives": "support"})
            official.assert_called_once_with(self.vendor, self.repo / "scripts/install.sh")
            support.assert_called_once_with(self.vendor)
            self.assertEqual([call.args[3] for call in verify.call_args_list],
                             ["tether-s8-a", "tether-s8-b", "tether-dynamic-prev", "tether-prev", "tether-flow-legacy"])
            self.assertEqual(result["dynamic_previous"]["tag"], "v1.0.0")
            self.assertEqual(result["fixed_previous"]["pin"], M.S8.FIXED_COMMIT)
            proofs[1]["binary_sha256"] = proofs[0]["binary_sha256"]
            with self.assertRaisesRegex(ValueError, "version-only"):
                M.supplier_evidence(self.repo, self.vendor)
            proofs[1]["binary_sha256"] = "2" * 64
            cache.return_value = False
            with self.assertRaisesRegex(ValueError, "compiled proof is stale"):
                M.supplier_evidence(self.repo, self.vendor)

    def test_stamp_requires_complete_runtime_identity(self):
        _, good, _ = self.execution()
        for name, change in (("empty nonce", lambda v: v.update(query_nonce="")),
                             ("nontext cut", lambda v: v.update(cut_id=0)),
                             ("wrong direction", lambda v: v.update(direction="mixed")),
                             ("unknown field", lambda v: v.update(unknown=True)),
                             ("boolean sequence", lambda v: v.update(sequence=True)),
                             ("missing process", lambda v: v["process"].update(pid=0)),
                             ("missing runtime", lambda v: v["process"]["key"].update(unit="")),
                             ("negative uid", lambda v: v["process"]["key"].update(uid=-1))):
            bad = copy.deepcopy(good)
            change(bad)
            with self.subTest(name=name), self.assertRaises(ValueError):
                M.stamp_matches(bad, copy.deepcopy(bad))

    def test_staging_refusal_invalidates_catalog_without_build(self):
        # Use the actual S9 missing-supervisor refusal. It happens before any
        # download/compiler; no successful supplier behavior is stubbed here.
        stage = HERE / "stage.sh"
        env = dict(os.environ, SIM_REPO=str(HERE.parents[2]), SIM_VENDOR=str(self.vendor),
                   SIM_UPGRADE_SUPERVISOR=str(Path(self.temporary.name) / "missing-supervisor"))
        result = subprocess.run(["sh", "-c", '. "$1"; sim_stage_binaries_supervised', "sh", str(stage)],
                                env=env, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("shared process supervisor", result.stderr)
        self.assertFalse((self.vendor / M.CATALOG).exists(), "upstream refusal left an old success catalog")
        for function in ("sim_stage_fullstack", "sim_verify_fullstack"):
            result = subprocess.run(["sh", "-c", '. "$1"; "$2"', "sh", str(stage), function],
                                    env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
            self.assertIn("shared process supervisor", result.stderr)


if __name__ == "__main__":
    unittest.main()
