#!/usr/bin/env python3
"""Initial S10 runtime supply and READY roster, never an upgrade driver.

P19 transports this host tool through P05's remote lease/cgroup launcher. P10
supplies a verified RawRef and the frozen P00 Artifact; no artifact is built or
downloaded here. Initial installation is exclusive and never writes previous
images, selectors, journals, identity keys, or transaction state. Product
bootstrap must establish the four session identities before inspect can pass.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import pwd
import re
import ssl
import stat
import subprocess
import sys
import threading
import time


def flow_module():
    spec = importlib.util.spec_from_file_location('fullstack_flow', Path(__file__).with_name('fullstack_flow.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ProvisionError(ValueError):
    pass


class CommandError(ProvisionError):
    def __init__(self, code):
        self.code = 128 - code if code < 0 else code
        super().__init__('supervised command failed rc=' + str(self.code))


def require(ok, reason):
    if not ok:
        raise ProvisionError(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def read_json(path):
    def pairs(rows):
        result = {}
        for key, value in rows:
            require(key not in result, "duplicate provision field")
            result[key] = value
        return result
    return json.loads(Path(path).read_bytes(), object_pairs_hook=pairs,
                      parse_constant=lambda _: require(False, "nonfinite provision field"))


def write_new(path, raw, mode=0o600):
    # Exclusive writes make a partial setup visible instead of repairing it on
    # a second invocation. The run owner must retain that failed setup receipt.
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode), "wb") as out:
        out.write(raw)
        out.flush()
        os.fsync(out.fileno())


def absolute(value):
    require(isinstance(value, str) and re.fullmatch(r"/[A-Za-z0-9_./-]+", value)
            and ".." not in Path(value).parts, "unsafe provision path")
    return Path(value)


def runtime_rows():
    """The frozen §3.1 population, independent of observed ONLINE rows."""
    result = []
    for i, tier in enumerate(("small-push", "small-pull", "large-push", "large-pull")):
        name, sid = "a" + str(i), "s10-" + tier
        home, domain = "/home/sim-" + name, "/opt/s10/" + name
        result.append(dict(runtime=name, host="agt1" if i < 2 else "agt2",
                           unit="tether-agent-" + name + ".service", sid=sid, nid=name,
                           user="sim-" + name, home=home, domain=domain,
                           binary=domain + "/tether", config=home + "/.tether/agent/" + sid + "/agent.yaml",
                           ctl_home=home + "/ctl"))
    return result


# origin: hot-update S10 plan §3.1 / §10.2 P11 / FullstackProvisionTest
def validate_manifest(manifest):
    require(set(manifest) == {"version", "nonce", "topology", "runtimes", "artifact", "supply",
                              "broker_url", "tunnel_addr"}, "provision manifest fields")
    require(type(manifest["version"]) is int and manifest["version"] == 1, "provision version")
    require(re.fullmatch(r"[0-9a-f]{32}", manifest["nonce"]) is not None, "provision nonce")
    require(manifest["topology"] in ("single", "n3"), "provision topology")
    rows = manifest["runtimes"]
    require(rows == runtime_rows(), "four independent runtime mappings required")
    artifact = manifest["artifact"]
    # These are P00 fields, not a second artifact/supply protocol. P10 signs the
    # source/vendor/COPY arrows; this owner joins its bytes to the installed file.
    fields = {"id", "role", "release", "source_commit", "toolchain", "image_id", "tree", "dirty",
              "flags", "archive", "binary", "vendor", "copy_manifest", "epoch", "read_epochs", "domains"}
    require(set(artifact) == fields and artifact["role"] == "agent", "P00 agent artifact required")
    for field in ("tree", "dirty", "flags", "archive", "binary", "vendor", "copy_manifest"):
        require(re.fullmatch(r"[0-9a-f]{64}", artifact[field]) is not None, "artifact digest missing")
    supply = manifest["supply"]
    require(set(supply) == {"path", "sha", "bytes", "schema"}, "P10 RawRef required")
    absolute(supply["path"])
    require(re.fullmatch(r"[0-9a-f]{64}", supply["sha"]) is not None and
            type(supply["bytes"]) is int and supply["bytes"] > 0 and supply["schema"], "P10 supply reference missing")
    require(re.fullmatch(r"(?:nats|wss)://[A-Za-z0-9.-]+:[0-9]+(?:/nats)?", manifest["broker_url"])
            is not None, "explicit broker route required")
    require(re.fullmatch(r"[A-Za-z0-9.-]+:[0-9]+", manifest["tunnel_addr"]) is not None,
            "explicit tunnel route required")
    return manifest


class SetupFence:
    """Durable, nonce-bound refusal of fixture writes once READY is sealed."""
    def __init__(self, root, nonce):
        self.root = absolute(str(root))
        require(re.fullmatch(r"[0-9a-f]{32}", nonce) is not None, "provision nonce")
        self.nonce = nonce

    def before_effect(self):
        # origin: hot-update S10 plan §2.1 / §10.2 P11: no restart/chown rescue.
        require(not os.path.lexists(self.root / "READY.json"), "fixture mutation after READY")

    def seal(self, receipt):
        self.before_effect()
        require(receipt["nonce"] == self.nonce, "READY nonce mismatch")
        write_new(self.root / "READY.json", canonical(receipt) + b"\n")


class Commands:
    """Every actual child enters the merged P05 launcher; never PATH tether."""
    def __init__(self, supervisor):
        self.supervisor = absolute(str(supervisor))
        require(self.supervisor.is_file() and os.access(self.supervisor, os.X_OK), "shared supervisor required")

    def run(self, argv, timeout=30):
        require(argv and absolute(argv[0]), "absolute command required")
        with subprocess.Popen([str(self.supervisor), "--", *argv], stdin=subprocess.DEVNULL,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE) as child:
            try:
                output, _ = child.communicate(timeout=timeout)
            except BaseException:
                child.terminate()
                child.communicate(timeout=15)
                raise
            if child.returncode != 0:
                raise CommandError(child.returncode)
            return output.decode("utf-8", errors="strict")


def properties(text):
    result = {}
    for line in text.splitlines():
        key, sep, value = line.partition("=")
        require(sep and key not in result, "incomplete effective unit properties")
        result[key] = value
    return result


def file_identity(path):
    path = Path(path)
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC), "rb") as source:
        before = os.fstat(source.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1, "shared or nonregular runtime file")
        sha = hashlib.file_digest(source, "sha256").hexdigest()
        after = os.fstat(source.fileno())
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                "runtime file changed during inspection")
    return dict(path=str(path), device=before.st_dev, inode=before.st_ino, sha=sha,
                uid=before.st_uid, mode=stat.S_IMODE(before.st_mode))


def can_read(path, uid, groups):
    """Check the service identity, not the root provisioning process's access."""
    path = Path(path)
    for entry in (path, *path.parents):
        info = entry.stat()
        shift = 6 if info.st_uid == uid else 3 if info.st_gid in groups else 0
        needed = 4 if entry == path else 1
        if uid != 0 and not ((info.st_mode >> shift) & needed):
            return False
    return True


# origin: hot-update S10 plan §5.4 / §10.2 P11 wrongcert
def verify_certificate(cert, key, ca, hostname, expected_sha, uid, groups):
    require(digest(Path(cert).read_bytes()) == expected_sha, "certificate artifact mismatch")
    require(can_read(cert, uid, groups) and can_read(key, uid, groups), "service cannot read certificate")
    require(Path(key).stat().st_mode & 0o007 == 0, "private key is world accessible")
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(cert, key)
    client_context = ssl.create_default_context(cafile=ca)
    ci, co, si, so = (ssl.MemoryBIO() for _ in range(4))
    client = client_context.wrap_bio(ci, co, server_hostname=hostname)
    server = server_context.wrap_bio(si, so, server_side=True)
    complete = set()
    # A real TLS handshake in memory verifies CA, validity, SAN and key pairing
    # without binding a public port or printing any private material.
    for _ in range(16):
        for name, endpoint in (("client", client), ("server", server)):
            try:
                endpoint.do_handshake()
                complete.add(name)
            except ssl.SSLWantReadError:
                pass
        for outgoing, incoming in ((co, si), (so, ci)):
            data = outgoing.read()
            if data:
                incoming.write(data)
        if len(complete) == 2:
            return dict(hostname=hostname, certificate_sha=expected_sha, uid=uid, verified=True)
    raise ProvisionError("certificate handshake incomplete")


def agent_config(row, manifest):
    # Strict YAML accepts JSON string scalars; no bootstrap PIN or identity key
    # is copied here. Real product bootstrap owns those identities.
    return "".join(key + ": " + json.dumps(value) + "\n" for key, value in
                   (("broker_url", manifest["broker_url"]), ("session", row["sid"]),
                    ("nid", row["nid"]), ("tunnel_addr", manifest["tunnel_addr"]))) + \
        "proxy:\n  allow_private_destinations: true\n"


def agent_unit(row, observer, decoder, supervisor, custody):
    for path in (observer, decoder, supervisor, custody):
        absolute(str(path))
    return ("[Unit]\nDescription=tether fullstack runtime " + row["runtime"] + "\n"
            "After=network-online.target\nWants=network-online.target\n[Service]\nType=simple\n"
            "User=" + row["user"] + "\nGroup=" + row["user"] + "\n"
            "Environment=HOME=" + row["home"] + "\nExecStart=/usr/bin/python3 -I -B " + str(observer) +
            " launch --script " + str(decoder) + " --supervisor " + str(supervisor) +
            " --root " + str(custody) + " --node " + row["runtime"] + " -- " + row["binary"] +
            " agent --session " + row["sid"] + " --nid " + row["nid"] +
            "\nRestart=always\nRestartSec=2\n[Install]\nWantedBy=multi-user.target\n")


class HostProvision:
    """Run inside a disposable host through P05; constructing has no effects."""
    def __init__(self, manifest, host, state, commands):
        self.manifest = validate_manifest(manifest)
        require(host in ("agt1", "agt2"), "unknown runtime host")
        self.host, self.commands = host, commands
        self.rows = [row for row in manifest["runtimes"] if row["host"] == host]
        self.fence = SetupFence(state, manifest["nonce"])

    def prepare(self, binary, observer, decoder):
        self.fence.before_effect()
        supply = self.manifest["supply"]
        raw = Path(supply["path"]).read_bytes()
        require(len(raw) == supply["bytes"] and digest(raw) == supply["sha"], "P10 supply bytes mismatch")
        source = file_identity(binary)
        binary_bytes = Path(binary).read_bytes()
        require(source["sha"] == digest(binary_bytes) == self.manifest["artifact"]["binary"], "initial binary differs from P10")
        operator = pwd.getpwnam("sim")
        # Public roster metadata is 0600. Traversable parents let each agent
        # reach only its owned custody directory without joining the operator
        # group that can read the other runtime's sealed observation channel.
        self.fence.root.mkdir(mode=0o755, parents=True, exist_ok=True)
        os.chown(self.fence.root, 0, 0)
        self.fence.root.chmod(0o755)
        write_new(self.fence.root / "PREPARING.json", canonical(self.manifest))
        for row in self.rows:
            self.fence.before_effect()
            try:
                account = pwd.getpwnam(row["user"])
            except KeyError:
                self.commands.run(["/usr/sbin/useradd", "--create-home", "--user-group", "--home-dir", row["home"],
                                   "--shell", "/bin/sh", row["user"]])
                account = pwd.getpwnam(row["user"])
            require(account.pw_dir == row["home"] and account.pw_uid > 0, "runtime account mismatch")
            for directory in (row["domain"], row["ctl_home"], row["home"] + "/.tether",
                              row["home"] + "/.tether/agent", str(Path(row["config"]).parent)):
                path = Path(directory)
                path.mkdir(mode=0o700, parents=True, exist_ok=True)
                require(not path.is_symlink(), "runtime directory is a symlink")
                os.chown(path, account.pw_uid, account.pw_gid)
                path.chmod(0o700)
            write_new(row["binary"], binary_bytes, 0o700)
            write_new(row["config"], agent_config(row, self.manifest).encode())
            for path in (row["binary"], row["config"]):
                os.chown(path, account.pw_uid, account.pw_gid)
            custody = self.fence.root / row["runtime"]
            custody.mkdir(mode=0o2770)
            os.chown(custody, account.pw_uid, operator.pw_gid)
            custody.chmod(0o2770)
            unit = agent_unit(row, observer, decoder, self.commands.supervisor, custody)
            write_new(Path("/etc/systemd/system") / row["unit"], unit.encode(), 0o644)
        self.commands.run(["/usr/bin/systemctl", "daemon-reload"])
        write_new(self.fence.root / "PREPARED.json", canonical(dict(nonce=self.fence.nonce, host=self.host)))

    def start(self):
        self.fence.before_effect()
        require(read_json(self.fence.root / "PREPARED.json") == dict(nonce=self.fence.nonce, host=self.host),
                "host not prepared")
        write_new(self.fence.root / "STARTED.json", canonical(dict(nonce=self.fence.nonce)))
        for row in self.rows:
            self.fence.before_effect()
            self.commands.run(["/usr/bin/systemctl", "start", row["unit"]])

    def inspect(self):
        result = []
        for row in self.rows:
            actual = inspect_runtime(row, self.fence.root, self.commands)
            require(actual['config']['sha'] == digest(agent_config(row, self.manifest).encode()),
                    'runtime config differs from provision manifest')
            result.append(actual)
        return result


def inspect_runtime(row, state, commands):
    account = pwd.getpwnam(row["user"])
    require(account.pw_dir == row["home"] and account.pw_uid > 0, "runtime account mismatch")
    props = properties(commands.run(["/usr/bin/systemctl", "show", row["unit"],
                                    "--property=User,Group,MainPID,ActiveState,SubState,InvocationID,ControlGroup,Restart"]))
    require(props["User"] == row["user"] and props["Group"] == row["user"] and
            props["ActiveState"] == "active" and props["SubState"] == "running" and
            props["Restart"] == "always" and props["InvocationID"] and props["ControlGroup"], "runtime unit not ready")
    custody_root = state / row['runtime']
    service = read_json(custody_root / (row['runtime'] + '.service.json'))
    require(service['pid'] == int(props['MainPID']) and service['start_ticks'] == int(
            Path('/proc/' + str(service['pid']) + '/stat').read_text().rsplit(')', 1)[1].split()[19]),
            'runtime service owner changed')
    supervised = read_json(custody_root / (row['runtime'] + '.product.json'))
    pid = supervised['target']['pid']
    require(pid > 1, "runtime PID unavailable")
    proc = Path("/proc") / str(pid)
    before = (proc / "stat").read_text().rsplit(")", 1)[1].split()[19]
    require(int(before) == supervised['target']['start_ticks'] and os.readlink(proc / 'ns/pid') ==
            supervised['target']['pid_namespace'], 'runtime supervised target changed')
    require(all(int(uid) == account.pw_uid for uid in
                next(line for line in (proc / "status").read_text().splitlines() if line.startswith("Uid:")).split()[1:]),
            "runtime effective UID mismatch")
    binary = file_identity(row["binary"])
    with (proc / "exe").open("rb") as exe:
        running_sha = hashlib.file_digest(exe, "sha256").hexdigest()
        running_stat = os.fstat(exe.fileno())
    require((running_stat.st_dev, running_stat.st_ino, running_sha) ==
            (binary["device"], binary["inode"], binary["sha"]), "runtime executing image differs")
    command = (proc / "cmdline").read_bytes().split(b"\0")
    require(command[:-1] == [s.encode() for s in (row["binary"], "agent", "--session", row["sid"], "--nid", row["nid"])],
            "runtime SID/NID command mismatch")
    require(before == (proc / "stat").read_text().rsplit(")", 1)[1].split()[19], "runtime PID changed")
    require(any(line.split(':', 2)[-1] == props['ControlGroup'] for line in (proc / 'cgroup').read_text().splitlines()),
            'runtime cgroup differs from unit')
    socket_path = state / row["runtime"] / (row["runtime"] + ".sock")
    require(stat.S_ISSOCK(socket_path.stat().st_mode), "runtime observer unavailable")
    config = file_identity(row["config"])
    require(binary["uid"] == config["uid"] == account.pw_uid and config["mode"] == 0o600, "runtime ownership mismatch")
    return dict(runtime=row["runtime"], key=dict(role="agent", host=row["host"], unit=row["unit"], sid=row["sid"],
                nid=row["nid"], domain=row["domain"], uid=account.pw_uid), home=row["home"], ctl_home=row["ctl_home"],
                process=dict(pid=pid, start_ticks=int(before), boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                             pidns=os.readlink(proc / "ns/pid")), invocation_id=props["InvocationID"],
                cgroup=props["ControlGroup"], binary=binary, config=config, running_sha=running_sha,
                custody=str(socket_path))


# origin: hot-update S10 plan §3.1 / §10.2 P11 two-SID/shared-binary controls
def ready_roster(manifest, observed, online):
    validate_manifest(manifest)
    require(len(observed) == 4 and {r["runtime"] for r in observed} == {"a0", "a1", "a2", "a3"},
            "READY requires every runtime")
    expected_online = {(row["sid"], row["nid"]) for row in runtime_rows()}
    require(len(online) == 4 and {(r["sid"], r["nid"]) for r in online if r["status"] == "ONLINE"} == expected_online,
            "four independently registered sessions required")
    identities, files, accounts, homes = set(), set(), set(), set()
    for row in runtime_rows():
        actual = next(r for r in observed if r["runtime"] == row["runtime"])
        key = actual["key"]
        require({k: key[k] for k in ("host", "unit", "sid", "nid", "domain")} ==
                {k: row[k] for k in ("host", "unit", "sid", "nid", "domain")} and key["role"] == "agent"
                and type(key["uid"]) is int and key["uid"] > 0, "READY target mismatch")
        require(actual["home"] == row["home"] and actual["ctl_home"] == row["ctl_home"], "READY HOME mismatch")
        process = actual["process"]
        identity = (key["host"], process["boot_id"], process["pidns"], process["pid"], process["start_ticks"])
        require(all(identity) and type(process["pid"]) is int and process["pid"] > 1 and
                type(process["start_ticks"]) is int and process["start_ticks"] > 0 and identity not in identities,
                "shared or unknown runtime process")
        identities.add(identity)
        for kind in ("binary", "config"):
            item = actual[kind]
            unique = (key["host"], item["device"], item["inode"])
            require(item["path"] == row[kind] and unique not in files, "shared runtime pathname or inode")
            require(type(item['device']) is int and item['device'] >= 0 and type(item['inode']) is int
                    and item['inode'] > 0 and item['uid'] == key['uid'], 'runtime file identity unknown')
            files.add(unique)
        require(actual['config']['mode'] == 0o600 and actual['binary']['mode'] == 0o700,
                'runtime file permissions changed')
        require(actual["running_sha"] == actual["binary"]["sha"] == manifest["artifact"]["binary"],
                "READY running SHA mismatch")
        account = (key["host"], key["uid"])
        require(account not in accounts and actual["ctl_home"] not in homes, "shared runtime account or ctl HOME")
        accounts.add(account)
        homes.add(actual["ctl_home"])
        require(actual["invocation_id"] and actual["cgroup"] and actual["custody"].endswith(
                "/" + row["runtime"] + "/" + row["runtime"] + ".sock"), "READY observer or unit identity missing")
    receipt = dict(version=1, nonce=manifest["nonce"], manifest_sha=digest(canonical(manifest)),
                   runtimes=observed, required=[r["key"] for r in observed])
    receipt["digest"] = digest(canonical(receipt))
    return receipt


def log_roster(ready, brokers):
    """P09's exact public log DTO, including each unit's prelogger stream."""
    streams = []
    require(brokers and len({row['host'] for row in brokers}) == len(brokers), 'broker log roster missing')
    for row in ready['runtimes']:
        key = row['key']
        base = dict(host=key['host'], unit=key['unit'], sid=key['sid'], home=row['home'],
                    boot_id=row['process']['boot_id'], invocation_id=row['invocation_id'])
        for kind in ('agent-slog', 'agent-boot-err', 'prelogger-journal'):
            streams.append(dict(base, id=row['runtime'] + '/' + kind, source_kind=kind))
    for row in brokers:
        require(set(row) == {'host', 'unit', 'home', 'boot_id', 'invocation_id'} and all(row.values()),
                'broker log identity incomplete')
        for kind in ('broker-slog', 'broker-journal', 'prelogger-journal'):
            streams.append(dict(row, sid='', id=row['host'] + '/' + kind, source_kind=kind))
    result = dict(version=1, streams=streams)
    result['digest'] = digest(canonical(result))
    return result


# origin: hot-update S10 plan §6 / §10.2 P11-P12 seam.
class PeerOwner:
    """Retain the local waiter and remote heartbeat until the keeper is reaped."""
    def __init__(self, transport, local, node, remote, cgroup, argv, user=None):
        self.transport, self.node, self.remote = transport, node, str(absolute(str(remote)))
        self.stop, self.joined, self.status = threading.Event(), False, None
        self.lease_error = False
        local.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.log = (local / 'supervisor.log').open('xb')
        self.pulse = None
        try:
            command = ['fullstack-exec', node, str(absolute(cgroup))]
            if user is not None:
                command = ['fullstack-exec-user', node, str(absolute(cgroup)), user]
            self.child = flow_module().peer_module().start_supervised(
                transport.prefix + command + list(map(str, argv)),
                stdin=subprocess.PIPE, stdout=self.log, stderr=self.log)
            os.set_blocking(self.child.stdin.fileno(), False)
            self.pulse = threading.Thread(target=self.heartbeat)
            self.pulse.start()
        except BaseException:
            if hasattr(self, 'child'):
                self.cancel()
                self.join()
            self.log.close()
            raise

    def heartbeat(self):
        try:
            while not self.stop.is_set():
                try:
                    os.write(self.child.stdin.fileno(), b'.')
                except BlockingIOError:
                    # An unread pipe must not trap Join; P05's remote deadline
                    # will retire the target if the transport stops forwarding.
                    pass
                except BrokenPipeError:
                    break  # Normal remote exit can precede local waitpid.
                if self.stop.wait(.25):
                    break
        except OSError:
            self.lease_error = True
        finally:
            self.child.stdin.close()

    def ready(self):
        deadline = time.monotonic() + 10
        while True:
            status = self.child.poll()
            if status is not None:
                raise CommandError(status or 2)
            raw = self.transport.run('fullstack-peer-read', self.node, self.remote, 'ready.json', allowed=(0, 1))
            if raw:
                self.receipt = json.loads(raw)
                require(self.receipt['identity']['pid'] > 1, 'peer READY identity missing')
                return self.receipt
            require(time.monotonic() < deadline, 'peer READY deadline')
            time.sleep(.01)

    def call(self, request):
        self.ready()
        value = self.transport.json('fullstack-peer-call', self.node, self.remote,
                                    data=canonical(request), timeout=65, allowed=(0, 2))
        require(value.get('rc', 0) == 0, 'peer RPC failed')
        return value

    def read(self, name):
        require(re.fullmatch(r'[a-z][a-z0-9-]{0,60}\.(sent|received)', name), 'invalid peer ledger name')
        return self.transport.run('fullstack-peer-read', self.node, self.remote, name)

    def cancel(self):
        # Close the remote lease, rather than only killing the docker client.
        self.stop.set()
        if self.pulse is not None and self.pulse.ident is not None:
            self.pulse.join(timeout=2)
            require(not self.pulse.is_alive(), 'peer heartbeat did not join')
        elif hasattr(self, 'child'):
            self.child.stdin.close()

    def join(self):
        if self.joined:
            return self.status
        forced = False
        try:
            try:
                status = self.child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                forced = True
                self.cancel()
                status = self.child.wait(timeout=15)
            self.status = 128 - status if status < 0 else status
            if (forced or self.lease_error) and self.status == 0:
                self.status = 125
            self.joined = True
            return self.status
        finally:
            self.cancel()
            self.log.close()


class FlowPeers:
    """P11's factory for P12. P19 supplies registered endpoint cgroups/binds.

    endpoints[(runtime, role)] contains cgroup, user, home, listen_host and listen_port.
    Target binds describe the private listener; source.route describes its
    public exposed endpoint. Product expose/SSH provisioning precedes start.
    """
    def __init__(self, transport, root, manifest, observed, online, endpoints):
        self.roster = json.loads(canonical(ready_roster(manifest, observed, online)))
        self.nonce, self.transport, self.root = manifest['nonce'], transport, Path(root)
        self.endpoints = {key: dict(value) for key, value in endpoints.items()}
        self.started, self.remote_roots = set(), set()
        require(set(endpoints) == {(r['runtime'], role) for r in observed for role in ('target', 'client')},
                'peer endpoint registrations incomplete')
        self.fence = SetupFence(root, self.nonce)
        self.flow = flow_module()

    def cohort(self, sources):
        return self.flow.FlowCohort(self.root / 'cohort', self.nonce, sources, self.start_peer)

    def start_peer(self, source, role):
        self.fence.before_effect()
        require(role in ('target', 'client'), 'invalid peer role')
        key = source['runtime'], role
        require(key in self.endpoints and key not in self.started, 'peer registration missing or already started')
        runtime = next(r for r in self.roster['runtimes'] if r['runtime'] == source['runtime'])
        require(source['key'] == runtime['key'], 'peer runtime differs from inspected roster')
        nonce = source['nonce']
        require(nonce == digest((self.nonce + '/' + source['runtime']).encode())[:32], 'peer source nonce differs')
        self.flow.validate_source(source['spec'], nonce)
        route, endpoint = source['route'], self.endpoints[key]
        node = runtime['key']['host'] if role == 'target' else route['client_host']
        remote = absolute(route[role + '_root'])
        require(remote.name and remote != Path('/'), 'peer root required')
        require((node, remote) not in self.remote_roots, 'peer roots are shared')
        require(set(endpoint) == {'cgroup', 'user', 'home', 'listen_host', 'listen_port'} and
                re.fullmatch(r'[a-z_][a-z0-9_-]*', endpoint['user']) and absolute(endpoint['home']) and
                isinstance(endpoint['listen_host'], str) and endpoint['listen_host'] and
                type(endpoint['listen_port']) is int and 0 <= endpoint['listen_port'] <= 65535,
                'peer listener registration incomplete')
        local = self.root / source['runtime'] / role
        self.started.add(key)  # A failed first launch never authorizes redial.
        self.remote_roots.add((node, remote))
        setup = PeerOwner(self.transport, local / 'prepare', node, remote, endpoint['cgroup'],
                          ['/bin/mkdir', '-m', '700', str(remote)])
        status = setup.join()
        if status:
            raise CommandError(status)
        for name in ('flow_fixture_peer.py', 'fullstack_flow.py', 'flow_observation.py',
                     'hot_update_flow_probe.py', 'flow_fixture_askpass.sh'):
            self.transport.run('copy-to', Path(__file__).with_name(name), node, str(remote / name))
        supervisor = Commands(os.environ.get('SIM_UPGRADE_SUPERVISOR', '')).supervisor
        self.transport.run('copy-to', supervisor, node, str(remote / 'supervise'))
        # SSH stdio must enter the same private root as its keeper. Docker copy
        # creates root-owned files; fix custody before READY, never on a cut.
        ownership = PeerOwner(self.transport, local / 'ownership', node, remote, endpoint['cgroup'],
                              ['/bin/chown', '-R', endpoint['user'], str(remote)])
        status = ownership.join()
        if status:
            raise CommandError(status)
        # The copied supervisor also owns SSH children spawned by the peer.
        # Return the owner before waiting for READY so partial starts get Join.
        return PeerOwner(self.transport, local, node, remote, endpoint['cgroup'],
                         ['/usr/bin/env', 'SIM_UPGRADE_SUPERVISOR=' + str(remote / 'supervise'), 'HOME=' + endpoint['home'],
                          '/usr/bin/python3', '-I', '-B', str(remote / 'flow_fixture_peer.py'), 'serve',
                          '--root', str(remote), '--nonce', nonce, '--role', role,
                          '--listen-host', endpoint['listen_host'], '--listen-port', str(endpoint['listen_port'])],
                         user=endpoint['user'])


def certificate_main(argv):
    parser = argparse.ArgumentParser(description='Verify actual service certificate access and TLS identity')
    for name in ('cert', 'key', 'ca', 'hostname', 'sha', 'user'):
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args(argv)
    account = pwd.getpwnam(args.user)
    result = verify_certificate(args.cert, args.key, args.ca, args.hostname, args.sha,
                                account.pw_uid, os.getgrouplist(args.user, account.pw_gid))
    print(json.dumps(result, sort_keys=True))
    return 0


def main():
    if sys.argv[1:2] == ['certificate']:
        return certificate_main(sys.argv[2:])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("prepare", "start", "inspect", "seal"))
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--host", required=True)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--supervisor", required=True, type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--observer", type=Path)
    parser.add_argument("--decoder", type=Path)
    parser.add_argument("--observed", type=Path)
    parser.add_argument("--online", type=Path)
    args = parser.parse_args()
    host = HostProvision(read_json(args.manifest), args.host, args.state, Commands(args.supervisor))
    if args.operation == "prepare":
        host.prepare(args.binary, args.observer, args.decoder)
    elif args.operation == "start":
        host.start()
    elif args.operation == "inspect":
        print(json.dumps(host.inspect(), sort_keys=True))
    else:
        receipt = ready_roster(host.manifest, read_json(args.observed), read_json(args.online))
        # Re-read this host immediately before sealing; submitted historical
        # observations cannot authorize a stale PID or a replaced on-disk image.
        current = host.inspect()
        require(all(row in receipt["runtimes"] for row in current), "READY local inspection changed")
        host.fence.seal(receipt)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except CommandError as error:
        print(str(error), file=sys.stderr)
        sys.exit(error.code)
    except (OSError, ValueError, KeyError, TypeError, ssl.SSLError):
        print("fullstack provision: setup failed", file=sys.stderr)
        sys.exit(2)
