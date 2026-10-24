#!/usr/bin/env python3
"""Real filesystem/rename/CLI/source-build checks with fake curl and Docker.

The ELF fixture is synthetic, never executed. Caddy TLS/WSS readiness and original
SSH continuity require the real simcluster drill; these tests do not claim either.
Support archives use real repository source and Go builds; no built binary runs.
"""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shutil
import struct
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock

SIM = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("caddy_artifact", SIM / "lib/caddy-artifact.py")
ART = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ART)
VERSION = "2.7.6"  # fixture pin, not the production installer's source of truth
NAME = f"caddy_{VERSION}_linux_amd64.tar.gz"


def stage_inputs(entry):
    """Discover the stage wrapper's repository-local shell and Python inputs."""
    repo = SIM.parents[1]
    pending, inputs = [entry], set()
    while pending:
        path = pending.pop()
        if path in inputs:
            continue
        source = (repo / path).read_text()
        inputs.add(path)
        # Follow both sourced libraries and invoked scripts: copying only the
        # entry wrapper leaves its supervised branch unable to reach the verifier.
        # origin: S9 internal review ledgers4-b F1
        for name in re.findall(r'^\s*(?:\.|source)\s+"\$SIM_REPO/([^"\s]+\.sh)"', source, re.M):
            pending.append(Path(name))
        for name in re.findall(r'\$SIM_REPO/([^"\s]+\.py)"', source):
            pending.append(Path(name))
        if path.suffix == ".py":
            for name in re.findall(r'with_name\("([^"/]+\.py)"\)', source):
                pending.append(path.with_name(name))
            for name in re.findall(r'"([^"\s]+/[^"\s]+\.py)"', source):
                pending.append(Path(name))
    return inputs


def elf(label):
    header = bytearray(64)
    header[:7] = b"\x7fELF\x02\x01\x01"
    struct.pack_into("<HH", header, 16, 2, 62)
    return bytes(header) + label


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="caddy-artifact-test-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.vendor = self.root / "vendor"
        self.vendor.mkdir()
        self.installer = self.root / "install.sh"
        self.installer.write_text('CADDY_VERSION="${TETHER_CADDY_VERSION:-2.7.6}"\nNATS_SERVER_VERSION="${TETHER_NATS_SERVER_VERSION:-v2.14.6}"\n')
        self.bin = self.root / "bin"
        self.bin.mkdir()
        curl = self.bin / "curl"
        curl.write_text('''#!/usr/bin/env python3
import os, pathlib, sys
args = sys.argv[1:]
url = next(x for x in args if x.startswith('https://'))
with open(os.environ['FAKE_CALLS'], 'a') as out: out.write(url+'\\n')
expected = os.environ['FAKE_BASE']
if url == expected+'/'+os.environ['FAKE_NAME']: source = os.environ['FAKE_ARCHIVE']
elif url == expected+'/caddy_'+os.environ['FAKE_VERSION']+'_checksums.txt': source = os.environ['FAKE_SUMS']
else: raise SystemExit('unexpected URL; network forbidden')
pathlib.Path(args[args.index('-o')+1]).write_bytes(pathlib.Path(source).read_bytes())
if os.environ.get('FAKE_FAIL'): raise SystemExit(22)
''')
        curl.chmod(0o755)
        self.archive, self.sums = self.root / "archive.tar.gz", self.root / "checksums"
        self.calls = self.root / "calls"
        env = dict(os.environ)
        # Operator release choices cannot select a different fixture or supply
        # the prerequisite whose absence the production negative control checks.
        # origin: S9 internal review FIX-CADDYART F1
        for key in ("TETHER_CADDY_VERSION", "TETHER_NATS_SERVER_VERSION", "SIM_TARGET_OS", "SIM_TARGET_ARCH",
                    "SIM_INFRA_PREVIOUS_REF", "SIM_VERSION"):
            env.pop(key, None)
        env.update(PATH=str(self.bin)+os.pathsep+env["PATH"],
                   FAKE_BASE=f"https://github.com/caddyserver/caddy/releases/download/v{VERSION}",
                   FAKE_NAME=NAME, FAKE_VERSION=VERSION, FAKE_ARCHIVE=str(self.archive),
                   FAKE_SUMS=str(self.sums), FAKE_CALLS=str(self.calls), DOCKER_SUDO="0")
        self.env = env
        self.environ = mock.patch.dict(os.environ, env, clear=True)
        self.environ.start()
        self.addCleanup(self.environ.stop)
        self.package()

    def package(self, payload=None, duplicate=False, symlink=False):
        if payload is None:
            payload = elf(b"fixture-caddy")
        with tarfile.open(self.archive, "w:gz") as archive:
            info = tarfile.TarInfo("caddy")
            if symlink:
                info.type, info.linkname = tarfile.SYMTYPE, "/bin/sh"
            else:
                info.size = len(payload)
            archive.addfile(info, None if symlink else io.BytesIO(payload))
            if duplicate:
                archive.addfile(info, io.BytesIO(payload))
        self.sums.write_text(f"{hashlib.sha512(self.archive.read_bytes()).hexdigest()}  {NAME}\n")
        return payload

    def stage_fixture(self):
        repo = self.root / "repo"
        (repo / "scripts").mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self.installer, repo / "scripts/install.sh")
        # The production wrapper owns its sourcing graph. Discover it each time
        # so a new source edge cannot leave the copied fixture on an older path.
        # origin: S9 internal review ledgers4-b F1
        for name in stage_inputs(Path("test/simcluster/lib/stage-caddy.sh")):
            destination = repo / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(SIM.parents[1] / name, destination)
        return repo

    def run_stage(self, repo, **environment):
        # Exercise the sourced stage wrapper even under `if`, which suppresses sh's set -e.
        # All critical operations must therefore propagate failure explicitly.
        command = 'set -eu; sim_stage_log() { echo "$*" >&2; }; . "$1"; if sim_stage_caddy; then exit 0; else exit 1; fi'
        return subprocess.run(["sh", "-c", command, "fixture", str(repo / "test/simcluster/lib/stage-caddy.sh")],
                              env=dict(os.environ, SIM_REPO=str(repo), SIM_VENDOR=str(self.vendor), **environment),
                              capture_output=True, text=True, timeout=10)

    def stage(self, success=True):
        repo = self.stage_fixture()
        # These original single-artifact controls cover override/checksum/rename
        # semantics, including versions outside S9's frozen adjacent catalog.
        # Their children remain under this suite's real shared supervisor. The
        # full supplier and genuine previous-source guard have separate controls.
        # origin: S9 internal review FIX-CADDYART F1
        result = self.run_stage(repo, SIM_UPGRADE_SUPERVISOR="")
        self.assertEqual(result.returncode == 0, success, result.stdout+result.stderr)
        self.assertFalse(list(self.vendor.glob(".caddy-*")), "temporary files leaked")
        return result

    def support_fixture(self):
        """Import two real, source-distinct support generations into scratch Git."""
        repo = self.stage_fixture()
        upstream = SIM.parents[1]
        # Use the toolchain already selected for this tree, then forbid downloads.
        # GOSUMDB=off alone prevents the PATH Go shim verifying that toolchain.
        toolchain = subprocess.check_output(["go", "env", "GOROOT"], cwd=upstream, text=True, timeout=30).strip()
        env = dict(os.environ, CGO_ENABLED="0", GOOS="linux", GOARCH="amd64", GOPROXY="off", GOSUMDB="off",
                   GOTOOLCHAIN="local", PATH=str(Path(toolchain) / "bin") + os.pathsep + os.environ["PATH"])
        raw = subprocess.check_output(["go", "list", "-deps", "-json", "./cmd/tether-infra-helper",
                                       "./cmd/tether-infra-launch", "./cmd/tether"],
                                      cwd=upstream, env=env, text=True, timeout=120)
        decoder, offset, names = json.JSONDecoder(), 0, {"go.mod", "go.sum", "build/infra-support.py"}
        while offset < len(raw):
            offset += len(raw[offset:]) - len(raw[offset:].lstrip())
            if offset == len(raw):
                break
            package, offset = decoder.raw_decode(raw, offset)
            directory = Path(package["Dir"])
            if directory.is_relative_to(upstream):
                for kind in ("GoFiles", "CgoFiles", "CFiles", "HFiles", "SFiles", "EmbedFiles"):
                    names.update((directory / name).relative_to(upstream).as_posix()
                                 for name in package.get(kind, []))
        for name in names:
            destination = repo / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(upstream / name, destination)
        current = {name: (repo / name).read_bytes() for name in sorted(names)}
        previous = dict(current)
        # Alter executable diagnostics, preserving the real worker/launcher and
        # reader protocol. Comments or release ldflags would be a fake old image:
        # production independently requires different closure AND binary hashes.
        # origin: hot-update S9 plan §9.4 / M28; S9 internal review FIX-CADDYART F1
        for name, diagnostic in (("cmd/tether-infra-helper/main.go", b'"infra_internal"'),
                                 ("cmd/tether-infra-launch/main.go", b'"infra_launch_rejected"')):
            self.assertIn(diagnostic, current[name], "real support entry diagnostic changed")
            previous[name] = current[name].replace(diagnostic, diagnostic[:-1] + b'_previous_fixture"')
        subprocess.run(["git", "init", "-q", str(repo)], check=True, env=env, timeout=10)
        stream = bytearray()
        for generation, files in (("previous", previous), ("current", current)):
            stream.extend(b"commit refs/heads/main\ncommitter Fixture <fixture@example.invalid> 1 +0000\n")
            stream.extend(b"data 8\nfixture\n")
            if generation == "current":
                stream.extend(b"from refs/tags/fixture-previous\n")
            for name, body in files.items():
                stream.extend(b"M 100644 inline " + name.encode() + b"\ndata " + str(len(body)).encode() + b"\n" + body + b"\n")
            stream.extend(b"\nreset refs/tags/fixture-" + generation.encode() + b"\nfrom refs/heads/main\n\n")
        # Plumbing creates isolated fixture objects; no project index, commit,
        # checkout or other worktree is changed, and no operator ref is required.
        subprocess.run(["git", "fast-import", "--quiet"], input=stream, cwd=repo, env=env, check=True, timeout=30)
        subprocess.run(["git", "symbolic-ref", "HEAD", "refs/heads/main"], cwd=repo, env=env, check=True, timeout=10)
        previous_ref = subprocess.check_output(["git", "rev-parse", "fixture-previous^{commit}"],
                                               cwd=repo, env=env, text=True, timeout=10).strip()
        self.assertRegex(previous_ref, r"^[a-f0-9]{40}$")
        (self.vendor / "infra").mkdir()
        shutil.copyfile(upstream / "internal/infraartifact/testdata/catalog.json", self.vendor / "infra/catalog.json")
        return repo, previous_ref, env

    # origin: S9 internal review FIX-CADDYART F1
    def test_support_fixture_builds_genuine_previous_without_operator_ref(self):
        self.assertNotIn("SIM_INFRA_PREVIOUS_REF", os.environ)
        repo, previous_ref, env = self.support_fixture()
        supplier = repo / "test/simcluster/lib/infra-artifact.py"
        result = subprocess.run([os.environ["SIM_UPGRADE_SUPERVISOR"], "--", "python3", "-I", "-B", str(supplier),
                                 "stage-support", str(repo), str(self.vendor), previous_ref],
                                env=env, capture_output=True, text=True, timeout=180)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        spec = importlib.util.spec_from_file_location("fixture_infra_artifact", supplier)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        receipt = module.verify_support(self.vendor)
        old, new = receipt["generations"]
        self.assertEqual(old["source_commit"], previous_ref)
        current_ref = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, env=env, text=True, timeout=10).strip()
        self.assertEqual(new["source_commit"], current_ref)
        self.assertNotEqual(current_ref, previous_ref)
        for before, after in zip(old["binaries"], new["binaries"]):
            self.assertNotEqual(before["source_sha256"], after["source_sha256"])
            self.assertNotEqual(before["binary_sha256"], after["binary_sha256"])
            self.assertEqual(after["component"], before["component"])
            self.assertEqual((before["protocol_version"], after["protocol_version"]), (1, 1))
        self.assertFalse(self.calls.exists(), "support fixture attempted an official download")
        self.assertFalse(list((self.vendor / "infra").glob(".infra-support-*")), "support build temporary files leaked")
        # The same-source and unknown-ref paths must still fail before publishing
        # a receipt. A real previous fixture cannot weaken production refusal.
        for ref, error in (("", "genuine previous support source required"),
                           (current_ref, "distinct executable source"), ("missing-fixture-ref", "rev-parse")):
            with self.subTest(ref=ref):
                rejected = self.root / ("rejected-" + ref)
                result = subprocess.run([os.environ["SIM_UPGRADE_SUPERVISOR"], "--", "python3", "-I", "-B", str(supplier),
                                         "stage-support", str(repo), str(rejected), ref],
                                        env=env, capture_output=True, text=True, timeout=60)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn(error, result.stderr)
                self.assertFalse((rejected / "infra/support-source.json").exists())

    # origin: S9 internal review FIX-CADDYART F1
    def test_supervised_stage_refuses_missing_previous_source_before_effect(self):
        self.stage()
        original = {name: (self.vendor / name).read_bytes() for name in ("caddy", "caddy-source.json")}
        calls = self.calls.read_bytes()
        result = self.run_stage(self.stage_fixture())
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("infra-artifact: genuine previous support source required: set SIM_INFRA_PREVIOUS_REF", result.stderr)
        self.assertEqual(self.calls.read_bytes(), calls, "missing previous source reached a download")
        self.assertEqual({name: (self.vendor / name).read_bytes() for name in original}, original)
        self.assertFalse((self.vendor / "infra").exists(), "missing source published infrastructure")

    # origin: S9 internal review FIX-CADDYART F1
    def test_entry_propagates_supervisor_refusal(self):
        refusing, calls = self.root / "refusing-supervisor", self.root / "supervisor-calls"
        refusing.write_text('#!/bin/sh\nprintf \'%s\\n\' "$@" > "$SUPERVISOR_CALLS"\nexit 67\n')
        refusing.chmod(0o755)
        selected = "ArtifactTests.test_stage_fixture_copies_every_sourced_input"
        result = subprocess.run(["sh", str(SIM / "tests/caddy-artifact-test.sh"), selected],
                                env=dict(os.environ, SIM_UPGRADE_SUPERVISOR=str(refusing), SUPERVISOR_CALLS=str(calls)),
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 67, result.stdout + result.stderr)
        self.assertEqual(calls.read_text().splitlines(),
                         ["--", "python3", "-I", "-B", str(SIM / "tests/caddy_artifact_test.py"), selected])
        self.assertFalse(self.calls.exists(), "refused supervisor started fixture children")

    # origin: S9 internal review FIX-CADDYART F1
    def test_entry_requires_executable_supervisor(self):
        selected = "ArtifactTests.test_stage_fixture_copies_every_sourced_input"
        result = subprocess.run(["sh", str(SIM / "tests/caddy-artifact-test.sh"), selected],
                                env=dict(os.environ, SIM_UPGRADE_SUPERVISOR=str(self.root / "absent-supervisor")),
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("Caddy artifact tests: shared supervisor is required", result.stderr)
        self.assertFalse(self.calls.exists(), "missing supervisor started fixture children")

    # origin: S9 internal review ledgers4-b F1
    def test_stage_fixture_copies_every_sourced_input(self):
        repo = self.stage_fixture()
        inputs = stage_inputs(Path("test/simcluster/lib/stage-caddy.sh"))
        self.assertGreater(len(inputs), 1, "stage input graph is empty")
        for name in inputs:
            with self.subTest(name=str(name)):
                self.assertTrue((repo / name).is_file(), "stage fixture omitted " + str(name))
                self.assertEqual((repo / name).read_bytes(), (SIM.parents[1] / name).read_bytes())

    # origin: S9 internal review ledgers4-b F1
    def test_missing_sourced_library_fails_setup_loudly(self):
        repo = self.stage_fixture()
        libraries = stage_inputs(Path("test/simcluster/lib/stage-caddy.sh"))
        libraries = sorted(name for name in libraries if name.suffix == ".sh" and name.name != "stage-caddy.sh")
        self.assertTrue(libraries, "missing-library control has no sourced library")
        for name in libraries:
            with self.subTest(name=str(name)):
                original = (repo / name).read_bytes()
                (repo / name).unlink()
                try:
                    # Source failure precedes use of this non-executable path;
                    # the control cannot accidentally start a real supplier.
                    result = self.run_stage(repo, SIM_UPGRADE_SUPERVISOR=str(self.root / "absent-supervisor"))
                    self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertIn(str(repo / name), result.stderr)
                    self.assertRegex(result.stderr, r"cannot open|No such file")
                    self.assertFalse(self.calls.exists(), "missing library reached a download")
                    self.assertFalse(list(self.vendor.iterdir()), "missing library published an artifact")
                finally:
                    (repo / name).write_bytes(original)

    def verify(self):
        return ART.verify(self.vendor / "caddy", self.vendor / "caddy-source.json", self.installer)

    def test_stage_and_receipt(self):
        self.stage()
        receipt = self.verify()
        self.assertEqual(receipt["archive_sha256"], ART.digest(self.archive))
        self.assertEqual(receipt["archive_checksum_algorithm"], "sha512")
        self.assertEqual(receipt["archive_checksum"], hashlib.sha512(self.archive.read_bytes()).hexdigest())
        self.assertEqual(receipt["installer_sha256"], ART.digest(self.installer))
        self.assertEqual((self.vendor / "caddy").stat().st_mode & 0o777, 0o755)
        self.assertEqual((self.vendor / "caddy-source.json").stat().st_mode & 0o777, 0o644)
        self.assertEqual(len(self.calls.read_text().splitlines()), 2)
        # There is no cache: both supplied files are fetched and verified again.
        self.stage()
        self.assertEqual(len(self.calls.read_text().splitlines()), 4)

    def test_checksum_exact_unique_entry(self):
        sha = ART.digest(self.archive)
        for content in ("", f"{sha} {NAME}.extra\n", f"{sha} {NAME}\n{sha} {NAME}\n",
                        f"{sha} {NAME}\n{'0'*64} {NAME}\n", f"z{sha[1:]} {NAME}\n",
                        f"{sha} {NAME} extra\n", f"{'0'*64} {NAME}\n"):
            with self.subTest(content=content):
                self.sums.write_text(content)
                self.stage(False)
                self.assertFalse((self.vendor / "caddy").exists())
        self.sums.write_text(f"{'1'*64} {NAME}.extra\n{sha.upper()} *{NAME}\n")
        self.stage()
        self.assertEqual(self.verify()["archive_checksum_algorithm"], "sha256")

    def test_wrong_artifact_or_download_preserves_old_pair(self):
        self.stage()
        old = [(self.vendor / name).read_bytes() for name in ("caddy", "caddy-source.json")]
        for option in ({"payload": b"not-elf"}, {"payload": elf(b"arm")[:18]+b"\xb7\x00"+elf(b"arm")[20:]},
                       {"duplicate": True}, {"symlink": True}):
            with self.subTest(option=option):
                self.package(**option)
                self.stage(False)
                self.assertEqual([(self.vendor / name).read_bytes() for name in ("caddy", "caddy-source.json")], old)
        self.package()
        with mock.patch.dict(os.environ, FAKE_FAIL="1"):
            self.stage(False)
        self.assertEqual([(self.vendor / name).read_bytes() for name in ("caddy", "caddy-source.json")], old)

    def test_pin_platform_and_input_binding(self):
        self.stage()
        self.assertEqual(ART.selected_version(self.installer), VERSION)
        for key, value in (("SIM_TARGET_OS", "darwin"), ("SIM_TARGET_ARCH", "arm64"),
                           ("TETHER_CADDY_VERSION", "2.7.6\n3.0.0"), ("TETHER_CADDY_VERSION", "3.0.0")):
            with self.subTest(key=key, value=value), mock.patch.dict(os.environ, {key: value}):
                with self.assertRaises(ValueError): self.verify()
        self.installer.write_text(self.installer.read_text()+"# modified\n")
        with self.assertRaisesRegex(ValueError, "installer"): self.verify()
        self.installer.write_text(self.installer.read_text()+f'CADDY_VERSION="${{TETHER_CADDY_VERSION:-{VERSION}}}"\n')
        with self.assertRaisesRegex(ValueError, "exactly one"): ART.selected_version(self.installer)

    def test_receipt_strict_and_binary_revalidation(self):
        self.stage()
        receipt_path = self.vendor / "caddy-source.json"
        original = receipt_path.read_text()
        for content in (original[:-2]+',"version":"2.7.6"}\n', "[]", "x"*8193,
                        original.replace('"schema_version": 1', '"schema_version": true'),
                        original.replace('"goarch": "amd64"', '"goarch": "arm64"')):
            with self.subTest(content=content[:80]):
                receipt_path.write_text(content)
                with self.assertRaises(ValueError): self.verify()
        receipt_path.write_text(original)
        binary = self.vendor / "caddy"
        binary.write_bytes(elf(b"changed"))
        with self.assertRaisesRegex(ValueError, "SHA256"): self.verify()
        binary.unlink()
        binary.symlink_to(self.archive)
        with self.assertRaisesRegex(ValueError, "regular"): self.verify()

    def next_pair(self):
        self.package(payload=elf(b"next-fixture-caddy"))
        other = self.root / "next"
        other.mkdir()
        ART.stage(self.installer, other)
        return other

    def test_publish_rename_keeps_open_old_inode(self):
        self.stage()
        other = self.next_pair()
        with (self.vendor / "caddy").open("rb") as old:
            old_inode, old_bytes = os.fstat(old.fileno()).st_ino, old.read()
            ART.publish(other / "caddy", other / "caddy-source.json", self.installer, self.vendor)
            old.seek(0)
            self.assertEqual(old.read(), old_bytes)
            self.assertNotEqual(old_inode, (self.vendor / "caddy").stat().st_ino)
        self.assertEqual(self.verify()["binary_sha256"], ART.digest(other / "caddy"))

    def test_receipt_last_interruption_is_detected(self):
        self.stage()
        other = self.next_pair()
        real_replace = ART.os.replace
        old_receipt = (self.vendor / "caddy-source.json").read_bytes()
        calls = []
        def interrupt(source, destination):
            self.assertEqual(Path(source).parent, self.vendor)
            calls.append(Path(destination).name)
            if len(calls) == 2: raise OSError("receipt publication interrupted")
            real_replace(source, destination)
        with mock.patch.object(ART.os, "replace", side_effect=interrupt):
            with self.assertRaisesRegex(OSError, "interrupted"):
                ART.publish(other / "caddy", other / "caddy-source.json", self.installer, self.vendor)
        self.assertEqual(calls, ["caddy", "caddy-source.json"])
        self.assertEqual((self.vendor / "caddy-source.json").read_bytes(), old_receipt)
        with self.assertRaisesRegex(ValueError, "SHA256"): self.verify()
        self.assertFalse(list(self.vendor.glob(".caddy-*")))

    def test_build_rejects_bad_pair_before_docker(self):
        self.stage()
        sim = self.root / "sim"
        shutil.copytree(SIM / "lib", sim / "lib")
        shutil.copyfile(SIM / "simcluster", sim / "simcluster")
        shutil.copytree(self.vendor, sim / "vendor")
        shutil.copyfile(self.installer, sim / "vendor/install.sh")
        (sim / "vendor/tether").write_text("unused fixture")
        (sim / "vendor/tether").chmod(0o755)
        calls = self.root / "docker-calls"
        docker = self.bin / "docker"
        docker.write_text('''#!/usr/bin/env python3
import json, os, sys
with open(os.environ['DOCKER_CALLS'], 'a') as out: out.write(json.dumps(sys.argv[1:])+'\\n')
if sys.argv[1] == 'build': pass
elif sys.argv[-2:] == ['/usr/local/bin/nats-server', '--version']: print('nats-server: v2.14.6')
elif sys.argv[-2:] == ['/usr/local/bin/caddy', 'version']: print('v2.7.6 fixture')
else: raise SystemExit('unexpected Docker operation; real Docker forbidden')
''')
        docker.chmod(0o755)
        def build():
            return subprocess.run(["bash", str(sim / "simcluster"), "build"],
                                  env=dict(os.environ, DOCKER=str(docker), DOCKER_CALLS=str(calls)),
                                  capture_output=True, text=True, timeout=10)
        result = build()
        # The unmodified production CLI must reject this Caddy-only fixture at
        # its independent full-infra guard before any Docker operation. Keeping
        # this negative arm prevents the scoped Caddy unit arm below certifying
        # an incomplete S9 image as buildable.
        # origin: S9 internal review FIX-CADDYART F1
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("invalid staged infra supply", result.stderr)
        self.assertFalse(calls.exists(), "incomplete infra reached Docker")
        # Scope the existing synthetic Caddy controls to their original unit:
        # full-infra and COPY validation have independent supply tests. Remove
        # only their prebuild commands and postbuild image census from this
        # private cmd_build copy; Caddy checks and original assertions stay intact.
        script = (sim / "simcluster").read_text()
        begin, end = script.index("cmd_build() {\n"), script.index("\n}\n", script.index("cmd_build() {\n"))
        body = script[begin:end]
        for guard in ('    python3 "$HERE/lib/infra-artifact.py" verify "$HERE/vendor" "$HERE/vendor/install.sh" || die "invalid staged infra supply"\n',
                      '    python3 "$HERE/lib/infra-image-manifest.py" create "$HERE" "$HERE/vendor/infra-image-inputs.json" || die "incomplete image COPY inputs"\n',
                      '    check_image_or_die\n'):
            self.assertEqual(body.count(guard), 1, "independent image guard changed")
            body = body.replace(guard, "")
        script = script[:begin] + body + script[end:]
        (sim / "simcluster").write_text(script)
        result = build()
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        invocation = json.loads(calls.read_text().splitlines()[0])
        self.assertEqual(invocation[:3], ["build", "--platform", "linux/amd64"])
        self.assertIn("TETHER_CADDY_VERSION="+VERSION, invocation)
        calls.unlink()
        (sim / "vendor/caddy").write_bytes(elf(b"interrupted new image"))
        result = build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid staged Caddy artifact", result.stderr)
        self.assertFalse(calls.exists(), "Docker invoked before Caddy verification")
        # Negative control: remove the actual prebuild check. Same corrupt fixture
        # reaches fake Docker and succeeds, proving the preceding refusal is wired.
        script = (sim / "simcluster").read_text()
        begin = script.index('    python3 "$HERE/lib/caddy-artifact.py" verify')
        end = script.index('    _caddy_want=', begin)
        (sim / "simcluster").write_text(script[:begin]+script[end:])
        mutant = build()
        self.assertEqual(mutant.returncode, 0, mutant.stdout+mutant.stderr)
        self.assertTrue(calls.exists(), "negative control did not exercise build")

    def test_control_and_tunnel_addresses_are_independent(self):
        script = '''set -u
warn() { printf '%s\\n' "$*" >&2; }
dexec() { printf 'exec %s\\n' "$*" >> "$EVENTS"; }
d() { test "$1" = cp || return 3; printf 'copy\\n' >> "$EVENTS"; cp "$2" "$CAPTURE"; }
ctr_name() { printf 'fake-%s' "$1"; }
sctl() { printf '%s\\n' "$*" >> "$EVENTS"; }
poll_until() { return 0; }
. "$HELPER"
agent_provision_yaml "$@"
'''
        cases = [("nats://brk1:4222", [], "brk1:7000", 0),
                 ("wss://brk1:443/nats", ["45", "brk1:7000"], "brk1:7000", 0),
                 ("wss://brk1:443/nats", [], None, 2),
                 ("wss://[::1]:443/nats", ["45", "[::1]:7000"], "[::1]:7000", 0)]
        for address in ("brk1.example.:1", "192.0.2.1:65535", "[2001:db8::1]:7000",
                        "[::ffff:192.0.2.1]:7000", "[fe80::1%eth0]:7000"):
            cases.append(("wss://brk1:443/nats", ["45", address], address, 0))
        for address in ("tls://brk1:7000", "brk1:7000\nproxy: false", "brk1:7000\r",
                        "brk1: 7000", "brk1:0", "brk1:65536", "brk1:-1", "brk1:+1",
                        "brk1:http", "brk1:7000/path", ":7000", "a..b:7000", "-brk1:7000",
                        "256.1.1.1:7000", "[::1:7000", "::1:7000", "[gg::1]:7000",
                        "[1:2:3:4:5:6:7:8:9]:7000", "[127.0.0.1]:7000", "[::1]%eth0:7000",
                        "[fe80::1%]:7000", "[fe80::1%a/b]:7000", "[::1]:7000#x"):
            cases.append(("wss://brk1:443/nats", ["45", address], None, 2))
        for index, (url, extra, tunnel, rc) in enumerate(cases):
            with self.subTest(url=url, extra=extra):
                capture, events = self.root / f"yaml{index}", self.root / f"events{index}"
                result = subprocess.run(["sh", "-c", script, "fixture", "agt1", "lab", url, "open", *extra],
                                        env=dict(os.environ, HELPER=str(SIM / "drills/lib/agentyaml.sh"),
                                                 CAPTURE=str(capture), EVENTS=str(events)),
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, rc, result.stdout+result.stderr)
                if rc == 0:
                    # JSON string scalars are a YAML subset; do not just grep the
                    # invalid bare IPv6 document. The Go loader test independently
                    # decodes the complete generated file with production yaml.v3.
                    lines = capture.read_text().splitlines()
                    self.assertEqual(len(lines), 4)
                    decoded = {}
                    for line in lines:
                        key, value = line.split(": ", 1)
                        self.assertNotIn(key, decoded)
                        self.assertTrue(value.startswith('"') and value.endswith('"'))
                        decoded[key] = json.loads(value)
                    self.assertEqual(decoded, {"broker_url": url, "session": "lab",
                                               "nid": "agt1", "tunnel_addr": tunnel})
                    self.assertTrue(events.exists(), "positive did not reach provisioning")
                else:
                    self.assertFalse(capture.exists())
                    self.assertFalse(events.exists(), "invalid address reached a remote operation")


    def test_explicit_override_and_rejected_stage_targets(self):
        version = "2.8.0"
        name = f"caddy_{version}_linux_amd64.tar.gz"
        self.sums.write_text(f"{ART.digest(self.archive)} {name}\n")
        with mock.patch.dict(os.environ, TETHER_CADDY_VERSION=version, FAKE_VERSION=version,
                             FAKE_NAME=name, FAKE_BASE=f"https://github.com/caddyserver/caddy/releases/download/v{version}"):
            self.stage()
            self.assertEqual(self.verify()["version"], version)
        with self.assertRaises(ValueError): self.verify()
        old = (self.vendor / "caddy-source.json").read_bytes()
        for key, value in (("SIM_TARGET_OS", "darwin"), ("SIM_TARGET_ARCH", "arm64"),
                           ("TETHER_CADDY_VERSION", "2.7.6/other")):
            with mock.patch.dict(os.environ, {key: value}):
                before = self.calls.read_bytes()
                self.stage(False)
                self.assertEqual(self.calls.read_bytes(), before, "invalid target attempted download")
                self.assertEqual((self.vendor / "caddy-source.json").read_bytes(), old)

    def test_failure_before_rename_preserves_pair(self):
        self.stage()
        other = self.next_pair()
        old = [(self.vendor / n).read_bytes() for n in ("caddy", "caddy-source.json")]
        with mock.patch.object(ART.os, "replace", side_effect=OSError("rename refused")):
            with self.assertRaisesRegex(OSError, "refused"):
                ART.publish(other / "caddy", other / "caddy-source.json", self.installer, self.vendor)
        self.assertEqual([(self.vendor / n).read_bytes() for n in ("caddy", "caddy-source.json")], old)
        self.verify()
        self.assertFalse(list(self.vendor.glob(".caddy-*")))

    def test_caddy_provisioning_failure_and_routes(self):
        # Only command routing/propagation is real here. CA, Docker, systemctl,
        # Caddy validate and WSS request results are stubs, not readiness evidence.
        script = r'''set -u
INSTANCE=fixture
warn() { echo "$*" >&2; }
die() { echo "$*" >&2; exit 2; }
ctr_name() { printf 'sim-fixture-%s' "$1"; }
_secrets_node_dir() { printf '%s' "$CERTS"; }
secrets_mint_ingress() { printf 'mint %s\n' "$*" >> "$EVENTS"; }
d() {
    printf 'docker %s\n' "$*" >> "$EVENTS"
    if [ "$1" = inspect ]; then [ "$FAIL_AT" = sidecar ]; return; fi
    [ "$1" = cp ] || return 3
    [ "$FAIL_AT" != copy ] || return 7
    case "$3" in */Caddyfile) cp "$2" "$CAPTURE" ;; esac
}
dexec() {
    printf 'exec %s\n' "$*" >> "$EVENTS"
    case "$*" in
      *sha256sum*) printf '%s  executable\n' "$EXPECTED_SHA" ;;
      *'/exe version') printf 'v2.7.6 fixture\n' ;;
    esac
}
sctl() {
    printf 'systemctl %s\n' "$*" >> "$EVENTS"
    case "$*" in *'show caddy'*) printf '123\n';; esac
    [ "$FAIL_AT" != start ] || return 8
}
poll_until() {
    printf 'poll %s\n' "$*" >> "$EVENTS"
    while [ "$1" != -- ]; do shift; done
    shift
    "$@"
}
. "$HELPER"
if caddy_prepare brk1; then :; else exit 11; fi
if caddy_start brk1; then :; else exit 12; fi
caddy_probe_wss ctl1 brk1 || exit 13
caddy_runtime_receipt brk1 "$EXPECTED_SHA"
'''
        for fail, expected in (("", 0), ("sidecar", 11), ("copy", 11), ("start", 12)):
            with self.subTest(fail=fail):
                events, capture = self.root / f"events-{fail}", self.root / f"config-{fail}"
                result = subprocess.run(["sh", "-c", script], capture_output=True, text=True, timeout=5,
                                        env=dict(os.environ, HELPER=str(SIM / "drills/lib/caddy.sh"), CERTS=str(self.root),
                                                 EVENTS=str(events), CAPTURE=str(capture), FAIL_AT=fail, EXPECTED_SHA="a"*64))
                self.assertEqual(result.returncode, expected, result.stdout+result.stderr)
                operations = events.read_text()
                if expected == 0:
                    config = capture.read_text()
                    self.assertLess(config.index("handle /sub/*"), config.index("reverse_proxy 127.0.0.1:8222"))
                    self.assertIn("reverse_proxy 127.0.0.1:8090", config)
                    self.assertIn("chmod 0600 /etc/tether/caddy/key.pem", operations)
                    self.assertIn("caddy validate --config /etc/tether/Caddyfile --adapter caddyfile", operations)
                    self.assertIn("systemctl brk1 enable --now caddy", operations)
                    self.assertIn("systemctl brk1 is-active --quiet caddy", operations)
                    self.assertIn("tether node ls --json --nats-url wss://brk1:443/nats", operations)
                    self.assertIn("CADDY_RUNTIME node=brk1 pid=123 sha256="+"a"*64, result.stdout)
                elif fail in ("copy", "sidecar"):
                    self.assertFalse(capture.exists())
                    self.assertNotIn("systemctl", operations)
                else:
                    self.assertNotIn("poll", operations)
                    self.assertNotIn("node ls", operations)


if __name__ == "__main__":
    unittest.main(verbosity=2)
