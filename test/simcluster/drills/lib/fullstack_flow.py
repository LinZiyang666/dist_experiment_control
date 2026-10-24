#!/usr/bin/env python3
"""Predeclared original flow supply; P17 alone owns control request scheduling.

P11/P19 provide start_peer(source, role), returning an already supervised owner
with call(request), read(relative_path), cancel(), join(), and a ready receipt.
join returns the shared supervisor's native status, including cleanup's 125.
Both peers use source["nonce"], not the enclosing cohort nonce, for their frames.
Each source carries the P00 runtime Key plus explicit realm/NATS/WSS and public
endpoint bindings. Provisioning, expose and SSH credentials belong to P11 and
must finish before READY. No install, service restart or CLI fallback lives here.

The selftest uses real local keepers and TCP streams. It signs supply and keeper
lifecycle only; it does not sign SSH, product cuts or remote lease supervision.
"""
import argparse
import hashlib
import fcntl
import shutil
import tempfile
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time


def peer_module():
    spec = importlib.util.spec_from_file_location("flow_peer", Path(__file__).with_name("flow_fixture_peer.py"))
    peer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(peer)
    return peer


def require(value, reason):
    if not value:
        raise ValueError(reason)


# origin: hot-update S10 plan §3.2 / §10.2 P12.
# Burst records consume sequence numbers ahead of the paced clock. Reserve
# those too, so even the SS source cannot go quiet before the declared ceiling.
def source_records(ceiling_seconds, rate, burst=False):
    for value in (ceiling_seconds, rate):
        require(type(value) in (int, float) and math.isfinite(value) and value > 0,
                "invalid flow supply duration or rate")
    return math.ceil(ceiling_seconds * rate) + 32 + (512 if burst else 0)


def declare_source(peer, name, nonce, ceiling_seconds, rate=10, size=128,
                   protocol="raw", burst=False, drain_seconds=15):
    require(isinstance(name, str) and re.fullmatch(r"[a-z][a-z0-9-]{0,60}", name), "invalid flow name")
    require(isinstance(nonce, str) and re.fullmatch(r"[0-9a-f]{32}", nonce), "invalid flow nonce")
    require(protocol in ("raw", "ss") and type(burst) is bool, "invalid flow protocol")
    require(type(size) is int and 0 < size <= 32768, "invalid flow payload size")
    require(type(drain_seconds) in (int, float) and math.isfinite(drain_seconds)
            and 0 <= drain_seconds <= 15, "invalid flow drain duration")
    count = source_records(ceiling_seconds, rate, burst)
    return dict(id=name, protocol=protocol, **{"class": "normal"}, size=size, burst=burst,
                declared_at=time.monotonic(),
                supply=dict(ceiling_seconds=ceiling_seconds, rate=rate, tail_records=32,
                            burst_records=512 if burst else 0, drain_seconds=drain_seconds),
                up=peer.declaration(name+".up", nonce, count, size, burst),
                down=peer.declaration(name+".down", nonce, count, size, burst))


def validate_source(spec, nonce):
    # Only public source metadata belongs in the manifest. SS keys/ciphertext
    # stay with the existing sealed descriptor custodian, never JSON evidence.
    require(set(spec) <= {"id", "protocol", "class", "size", "burst", "declared_at", "supply",
                         "up", "down", "host", "port", "key", "ssh_user", "ssh_root"},
            "unknown or private flow source field")
    supply = spec["supply"]
    require(set(supply) == {"ceiling_seconds", "rate", "tail_records", "burst_records", "drain_seconds"},
            "invalid flow supply fields")
    peer = peer_module()
    expected = declare_source(peer, spec["id"], nonce, supply["ceiling_seconds"], supply["rate"],
                              spec["size"], spec.get("protocol", "raw"), spec.get("burst", False),
                              supply["drain_seconds"])
    require(supply == expected["supply"] and all(spec[d] == expected[d] for d in ("up", "down")),
            "flow source differs from predeclared supply")


def original_sources(peer, nonce, ceiling_seconds, bindings, rate=10):
    """Bind the four originals to P11's actual runtime and route receipts."""
    require(set(bindings) == {"a0", "a1", "a2", "a3"}, "original runtime set incomplete")
    result = []
    for runtime in sorted(bindings):
        binding = bindings[runtime]
        key, route = binding["key"], binding["route"]
        require(set(route) <= {"realm", "nats", "wss", "client_host", "target_root", "client_root",
                              "host", "port", "ssh_user"}, "unknown or private route field")
        require(set(key) == {"role", "host", "unit", "sid", "nid", "domain", "uid"}
                and key["role"] == "agent" and key["nid"] == runtime,
                "flow runtime key differs from P00")
        require(type(key["uid"]) is int and key["uid"] > 0 and
                all(isinstance(key[k], str) and key[k] for k in set(key)-{"uid"}), "empty runtime binding")
        require(all(isinstance(route[k], str) and route[k] for k in
                    ("realm", "nats", "wss", "client_host", "target_root", "client_root", "host"))
                and type(route["port"]) is int and 0 < route["port"] <= 65535,
                "flow route binding incomplete")
        name = "tcp" if runtime in ("a0", "a1") else "ssh"
        # origin: hot-update S10 plan §3.2 / §10.2 P12.
        # Identical payload names on separate runtimes still need distinct
        # nonces: otherwise swapping two complete streams could pass both hashes.
        source_nonce = hashlib.sha256((nonce+"/"+runtime).encode()).hexdigest()[:32]
        spec = declare_source(peer, name, source_nonce, ceiling_seconds, rate)
        spec.update(host=route["host"], port=route["port"])
        if name == "ssh":
            require(isinstance(route.get("ssh_user"), str) and route["ssh_user"], "SSH runtime user missing")
            spec.update(ssh_user=route["ssh_user"], ssh_root=route["target_root"])
        result.append(dict(runtime=runtime, nonce=source_nonce, key=dict(key), route=dict(route), spec=spec))
    # origin: S10 internal review rv3-assembly F4
    # Numeric UIDs, unit names and installation paths belong to a host. Two
    # fresh hosts normally allocate the same first service UIDs independently.
    require(len({s["key"]["sid"] for s in result}) == 4, "original runtimes share an installation domain")
    for field in ("unit", "domain", "uid"):
        require(len({(s["key"]["host"], s["key"][field]) for s in result}) == 4,
                "original runtimes share an installation domain")
    return result


# origin: S10 internal review rv3-assembly F1
def source_pinned_tools(paths):
    """Independently rebuild the fixed command packages before READY.

    Callers run inside P05's outer shared supervisor (the 99 shell and Go
    hermetic tests both do). Each short build has its original Popen waiter.
    Supplied digests/Go build metadata never choose trusted executable bytes.
    The sealed anonymous images prevent pathname replacement after comparison.
    """
    repo = Path(__file__).resolve().parents[4]
    packages = ('test/hotupdate/assemblycmd', 'test/fullstackharness/cmd/supervise',
                'test/hotupdate/evidencecmd')
    require(len(paths) == len(packages), 'assembly tool population')
    require(all(Path(p).is_absolute() and Path(p).is_file() and not Path(p).is_symlink()
                and os.access(p, os.X_OK) for p in paths), 'assembly tool path')
    if not (repo / packages[0]).is_dir():
        raise RuntimeError('NOT_RUN pending P25 source-pinned assemblycmd')
    go = shutil.which('go')
    require(go is not None, 'candidate toolchain unavailable')
    env = {k: v for k, v in os.environ.items() if k not in ('GOFLAGS', 'GOWORK', 'GOENV', 'CGO_ENABLED')}
    env.update(GOFLAGS='', GOWORK='off', GOENV='off', CGO_ENABLED='0')
    owned, result = [], []
    try:
        with tempfile.TemporaryDirectory(prefix='fullstack-tool-verify-') as temporary:
            for index, (path, package) in enumerate(zip(paths, packages)):
                output = Path(temporary) / str(index)
                with subprocess.Popen([go, 'build', '-trimpath', '-buildvcs=false', '-o', str(output), './'+package],
                                      cwd=repo, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as build:
                    try:
                        code = build.wait(timeout=120)
                    except BaseException:
                        build.terminate(); build.wait()
                        raise
                require(code == 0, 'candidate command rebuild failed')
                payload = output.read_bytes()
                require(Path(path).read_bytes() == payload, 'assembly tool differs from current candidate source')
                fd = os.memfd_create('fullstack-verified-tool', os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
                owned.append(fd)
                with os.fdopen(os.dup(fd), 'wb') as image:
                    image.write(payload)
                os.fchmod(fd, 0o500)
                fcntl.fcntl(fd, fcntl.F_ADD_SEALS, fcntl.F_SEAL_WRITE | fcntl.F_SEAL_GROW |
                            fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_SEAL)
                result.append('/proc/'+str(os.getpid())+'/fd/'+str(fd))
        return result, owned
    except BaseException:
        for fd in owned:
            os.close(fd)
        raise



class FlowCohort:
    """Own both keeper handles through capture, Stop and unconditional Join."""
    def __init__(self, root, nonce, sources, start_peer):
        self.root, self.nonce, self.start_peer = Path(root), nonce, start_peer
        self.peer = peer_module()
        # JSON copies detach declarations from mutable caller dictionaries.
        self.sources = json.loads(json.dumps(sources))
        require(self.sources and len({s["runtime"] for s in self.sources}) == len(self.sources),
                "empty or duplicate source set")
        for source in self.sources:
            require(isinstance(source["runtime"], str) and re.fullmatch(r"[a-z][a-z0-9-]{0,60}", source["runtime"]),
                    "invalid flow runtime name")
            validate_source(source["spec"], source.get("nonce", nonce))
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.declaration = json.dumps(dict(nonce=nonce, sources=self.sources), sort_keys=True).encode()
        with (self.root / "sources.json").open("xb") as stream:
            stream.write(self.declaration)
        self.digest = hashlib.sha256(self.declaration).hexdigest()
        self.owners, self.before = {}, {}
        self.phase, self.cleanup_errors = "new", None

    def unchanged(self):
        require((self.root / "sources.json").read_bytes() == self.declaration and
                json.dumps(dict(nonce=self.nonce, sources=self.sources), sort_keys=True).encode() == self.declaration,
                "original source declarations changed")

    def start(self):
        require(self.phase == "new", "original cohort already started")
        self.unchanged()
        try:
            for source in self.sources:
                for role in ("target", "client"):
                    # P11 must return ownership before waiting for peer READY;
                    # a partial startup still has a waiter to join.
                    owner = self.start_peer(source, role)
                    self.owners[source["runtime"], role] = owner
                    owner.call(dict(command="configure", flows=[source["spec"]]))
            for source in self.sources:
                self.owners[source["runtime"], "client"].call(dict(command="start", flows=[source["spec"]]))
            self.phase = "running"
            # Read-only readiness may be sampled again; the original connect
            # is never retried. READY needs nonempty duplex business evidence.
            deadline = time.monotonic() + 10
            while True:
                rows = self.sample("starting")
                if all(rows[source["runtime"]+"/"+role]["flows"].get(source["spec"]["id"], {}).get(field)
                       for source in self.sources for role in ("client", "target") for field in ("sent", "received")):
                    self.before = rows
                    break
                require(time.monotonic() < deadline, "original duplex READY missing")
                time.sleep(.01)
        except BaseException:
            self.close()
            raise

    def sample(self, label):
        require(self.phase == "running", "original cohort is not running")
        self.unchanged()
        rows = {}
        for key, owner in self.owners.items():
            value = owner.call(dict(command="snapshot"))
            require(value.get("rc", 0) == 0, "flow snapshot failed")
            require(all(not row["events"] and row["target_accepts"] == 1
                        for row in value["flows"].values()), "original flow interrupted before seal")
            rows["/".join(key)] = value
        # Filename is fixed; caller labels are data, never executable paths.
        self.peer.publish(self.root / "snapshot.json", dict(label=label, sources_sha256=self.digest, peers=rows))
        return rows

    def drain(self):
        require(self.phase == "running", "original drain before start")
        self.unchanged()
        # Arm both ends before waiting. Serial finish RPCs would spend an entire
        # timeout waiting for the other original sender's still-paced tail.
        for owner in self.owners.values():
            owner.call(dict(command="drain"))
        snapshots = {key: owner.call(dict(command="finish")) for key, owner in self.owners.items()}
        for source in self.sources:
            runtime, spec = source["runtime"], source["spec"]
            for role in ("client", "target"):
                key = runtime, role
                row = snapshots[key]["flows"][spec["id"]]
                require(row["target_accepts"] == 1 and not row["events"], "original flow interrupted or redialed")
                self.peer.probe.check_identity(row["identities"], True)
                folder = self.root / runtime / role
                folder.mkdir(parents=True, exist_ok=True)
                self.peer.publish(folder / "ledger.json", row)
                for suffix in ("sent", "received"):
                    (folder / suffix).write_bytes(self.owners[key].read(spec["id"]+"."+suffix))
            for direction, sender, receiver in (("up", "client", "target"), ("down", "target", "client")):
                sent = (self.root / runtime / sender / "sent").read_bytes()
                received = (self.root / runtime / receiver / "received").read_bytes()
                wanted = spec[direction]
                require(sent == received and len(sent) == wanted["bytes"] and
                        hashlib.sha256(sent).hexdigest() == wanted["sha256"], "original duplex bytes differ")
                frames = self.peer.probe.parse_frames(sent, source.get("nonce", self.nonce))
                require(len(frames) == wanted["records"] and
                        [row["seq"] for row in frames] == list(range(wanted["records"])), "original sequence differs")
                for role, ledger in ((sender, "sent"), (receiver, "received")):
                    actual = snapshots[runtime, role]["flows"][spec["id"]][ledger]
                    require(len(actual) == len(frames) and all(all(a[k] == f[k] for k in
                            ("seq", "length", "sha256")) for a, f in zip(actual, frames)), "original ledger differs")
        self.peer.publish(self.root / "drain.json", dict(sources_sha256=self.digest, complete=True))
        return snapshots

    # origin: hot-update S10 plan §6 / §10.2 P12.
    # Error receipts are sticky even after emergency cleanup succeeds. Continue
    # through every owner so one dead remote collector cannot strand its peers.
    def close(self):
        if self.cleanup_errors is not None:
            require(not self.cleanup_errors, "original flow cleanup failed")
            return
        errors = []
        for key, owner in self.owners.items():
            try:
                result = owner.call(dict(command="cleanup"))
                require(result.get("cleaned") is True and result.get("rc", 0) == 0, "remote cleanup failed")
            except Exception:
                # P11 transports also expose SetupError. Any failed RPC must
                # still cancel this owner and reach every original waiter.
                errors.append("/".join(key)+":remote_cleanup_failed")
                try:
                    owner.cancel()
                except Exception:
                    errors.append("/".join(key)+":cancel_failed")
        for key, owner in self.owners.items():
            try:
                status = owner.join()
                if status != 0:
                    errors.append("/".join(key)+":native_rc_"+str(status))
            except Exception:
                errors.append("/".join(key)+":join_failed")
        self.cleanup_errors, self.phase = errors, "closed"
        self.peer.publish(self.root / "cleanup.json", dict(complete=not errors, errors=errors,
                                                          sources_sha256=self.digest))
        require(not errors, "original flow cleanup failed")


class LocalPeer:
    """Hermetic transport boundary: same keeper/RPC/waiter, no product claims."""
    def __init__(self, peer, root, nonce, role):
        self.peer, self.root = peer, Path(root)
        self.root.mkdir(mode=0o700, parents=True)
        self.log = (self.root / "keeper.log").open("xb")
        try:
            self.child = peer.start_supervised([sys.executable, "-I", "-B", str(Path(peer.__file__)),
                "serve", "--root", self.root, "--nonce", nonce, "--role", role],
                stdout=self.log, stderr=self.log)
        except BaseException:
            self.log.close()
            raise
        self.joined = False

    def ready(self):
        deadline = time.monotonic() + 10
        while not (self.root / "ready.json").exists():
            require(self.child.poll() is None and time.monotonic() < deadline, "keeper READY failed")
            time.sleep(.005)
        return self.peer.probe.read_json(self.root / "ready.json")

    def call(self, request):
        self.ready()
        value = self.peer.call(self.root, request)
        require(value.get("rc", 0) == 0, "peer RPC failed")
        return value

    def read(self, name):
        return (self.root / name).read_bytes()

    def cancel(self):
        if self.child.poll() is None:
            self.child.terminate()

    def join(self):
        started = time.monotonic_ns()
        try:
            status = self.child.wait(timeout=10)
            self.joined = True
            return status
        finally:
            self.join_ns = time.monotonic_ns() - started
            self.log.close()


def expect_failure(operation, criterion):
    try:
        operation()
    except (OSError, ValueError, subprocess.SubprocessError):
        return
    raise AssertionError(criterion)


def selftest(root):
    """Exercise actual keeper lifetimes plus adversarial transport boundaries."""
    peer, root, nonce = peer_module(), Path(root), os.urandom(16).hex()
    # The independent arithmetic must not call source_records: otherwise a
    # 2400-record mutation could corrupt both the producer and its oracle.
    spec = declare_source(peer, "tcp", nonce, 901, drain_seconds=0)
    require(spec["up"]["records"] == 9042, "flow supply exhausted before full schedule")
    require(source_records(10800, 10, True) == 108544, "SS burst exhausts scheduled supply")
    validate_source(spec, nonce)
    bad = json.loads(json.dumps(spec)); bad["up"]["records"] = 2400
    expect_failure(lambda: validate_source(bad, nonce), "short source accepted")
    for value in (0, -1, float("nan"), float("inf"), True):
        expect_failure(lambda: source_records(value, 10), "invalid supply accepted")
    expect_failure(lambda: declare_source(peer, "../escape", nonce, 901), "unsafe source name accepted")
    expect_failure(lambda: declare_source(peer, "tcp", "invalid", 901), "invalid nonce accepted")
    expect_failure(lambda: validate_source(dict(spec, master="private"), nonce), "private source field accepted")

    # Exercise the real legacy entry point with P12's scheduler switch. A
    # restored unconditional 600-second collector calls start and fails here.
    legacy_spec = importlib.util.spec_from_file_location("flow_fixture", Path(__file__).with_name("flow_fixture.py"))
    legacy = importlib.util.module_from_spec(legacy_spec); legacy_spec.loader.exec_module(legacy)
    from types import SimpleNamespace
    class NoScheduler:
        closed = False
        def run(self, *args, **kwargs):
            raise AssertionError("second scheduler provisioned")
        def start(self, *args, **kwargs):
            raise AssertionError("second 600s scheduler started")
        def close(self):
            self.closed = True
    fixture = legacy.Fixture(SimpleNamespace(root=root / "legacy", nonce=nonce, profile="normal",
        arm="A", request_scheduler=False), transport=NoScheduler())
    fixture.phase, fixture.specs = "prepared", [dict(id="tcp")]
    fixture.remote_call = lambda *a, **kw: dict(started=True)
    fixture.start()
    require(fixture.request_job is None and not fixture.jobs, "duplicate request scheduler")
    fixture.phase = "new"
    attempted = []
    def failed_cleanup(node, command):
        attempted.append(node)
        raise legacy.SetupError("remote_cleanup_failed")
    fixture.remote_call = failed_cleanup
    try:
        fixture.cleanup()
    except legacy.SetupError:
        pass
    else:
        raise AssertionError("legacy remote cleanup error swallowed")
    require(attempted == ["ctl1", "agt1"] and fixture.transport.closed, "cleanup stopped before all owners joined")
    require(peer.probe.read_json(fixture.root / "cleanup-capture.json")["errors"] ==
            ["ctl1_remote_cleanup_failed", "agt1_remote_cleanup_failed"], "legacy cleanup lost failure receipt")
    transport = legacy.Transport(SimpleNamespace(here=Path("."), instance="hermetic", sim="unused"))
    natural = peer.start_supervised([sys.executable, "-I", "-B", "-c", "import time; time.sleep(.2)"],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    transport.children.append(natural)
    transport.close()
    require(natural.returncode == 0, "natural collector exit was interrupted")

    bindings = {name: dict(key=dict(role="agent", host="host-"+str(i//2), unit="unit-"+name,
        sid="session-"+name, nid=name, domain="/opt/"+name, uid=1000+i%2),
        route=dict(realm="isolated-realm", nats="nats://isolated:4222", wss="wss://isolated",
                   client_host="client-host", target_root="/tmp/"+name, client_root="/tmp/client-"+name,
                   host="public-host", port=20000+i, ssh_user="user-"+name))
        for i, name in enumerate(("a0", "a1", "a2", "a3"))}
    originals = original_sources(peer, nonce, 901, bindings)
    require([s["spec"]["id"] for s in originals] == ["tcp", "tcp", "ssh", "ssh"], "original protocol mapping changed")
    require(len({s["nonce"] for s in originals}) == 4 and
            len({s["spec"]["up"]["sha256"] for s in originals}) == 4, "original runtime streams alias")
    ssh = originals[2]["spec"]
    argv = peer.ssh_command(ssh, ssh["ssh_root"], nonce)
    require("user-a2@public-host" in argv and "/tmp/a2" in argv, "SSH runtime binding ignored")
    expect_failure(lambda: original_sources(peer, nonce, 901, {"a0": bindings["a0"]}), "missing runtimes accepted")
    bindings["a3"]["key"]["domain"] = bindings["a2"]["key"]["domain"]
    expect_failure(lambda: original_sources(peer, nonce, 901, bindings), "shared binary domain accepted")

    owners, evidence = [], []
    for scenario in ("normal", "partial-start", "cleanup-error"):
        started = time.monotonic_ns()
        base = root / scenario
        target = LocalPeer(peer, base / "target", nonce, "target")
        owners.append(target)
        cohort = None
        try:
            ready = target.ready()
            source = dict(runtime="a0", spec=dict(spec, host="127.0.0.1", port=ready["port"]))
            def start_peer(source, role):
                if role == "target":
                    return target
                child = LocalPeer(peer, base / "client", nonce, "client")
                owners.append(child)
                if scenario == "partial-start":
                    # Real target exists; the second keeper rejects its first
                    # configure. The cohort must still stop and join both.
                    child.ready()
                    child.call(dict(command="configure", flows=[spec]))
                return child
            cohort = FlowCohort(base / "evidence", nonce, [source], start_peer)
            if scenario == "partial-start":
                expect_failure(cohort.start, "partial startup unexpectedly passed")
                require(all(owner.joined for owner in owners), "partial startup lost waiter")
                continue
            cohort.start()
            startup_ns = time.monotonic_ns() - started
            before = cohort.sample("before")
            if scenario == "cleanup-error":
                # Lose the real control endpoint while the original sockets
                # and keepers still exist. Cleanup must retain the RPC error.
                (target.root / "peer.sock").unlink()
                expect_failure(cohort.close, "remote cleanup error swallowed")
                expect_failure(cohort.close, "repeat cleanup erased prior error")
                receipt = peer.probe.read_json(cohort.root / "cleanup.json")
                require(not receipt["complete"] and "a0/target:remote_cleanup_failed" in receipt["errors"],
                        "cleanup evidence lost original remote error")
                continue
            io_started = time.monotonic_ns()
            snapshots = cohort.drain()
            io_ns = time.monotonic_ns() - io_started
            for role in ("client", "target"):
                row = snapshots["a0", role]["flows"]["tcp"]
                require(len(row["sent"]) == 9042 and len(row["received"]) == 9042,
                        "real source ended at legacy 2400 records")
                first = before["a0/"+role]["identity"]
                require(snapshots["a0", role]["identity"] == first, "keeper process changed")
            settle_started = time.monotonic_ns()
            cohort.close()
            cohort.close()
            evidence.append(dict(records=9042, directions=2, source_sha256=cohort.digest,
                cost_ns=dict(startup=startup_ns, io=io_ns, cut=0, settle=time.monotonic_ns()-settle_started,
                             supervisor_join=sum(owner.join_ns for owner in owners))))
        finally:
            for owner in owners:
                if not owner.joined:
                    owner.cancel()
                    owner.join()
    require(all(owner.joined and owner.child.poll() is not None for owner in owners), "flow keeper not reaped")
    for owner in owners:
        require(not Path("/proc", str(owner.child.target_pid)).exists(), "keeper survived supervisor join")
    print(json.dumps(dict(tier="hermetic", supply=evidence, scenarios=3, joined=len(owners))))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("selftest",))
    parser.add_argument("--root", required=True, type=Path)
    args = parser.parse_args()
    selftest(args.root)
    return 0


if __name__ == "__main__":
    sys.exit(main())
