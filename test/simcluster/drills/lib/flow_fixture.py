#!/usr/bin/env python3
"""Live drill-75 coordinator. Only process-boundary transport is replaceable.

The main drill shell owns this keeper's PID. The keeper owns container probes,
their sockets, and private descriptor custody until receipts have been saved.
Unknown observations are SETUP-RED; successful ciphertext never supplies a
missing product Seal attempt, grant revision, or domain reservation receipt.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import threading
import time


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


peer = load("flow_fixture_peer")
probe = peer.probe
PROFILES = ("normal", "capacity-32", "silent-revoke", "legacy-broker", "legacy-exporter")
EVENTS = {"prepare": "tunnel: flow handoff prepared", "resume": "tunnel: flow carrier resumed",
          "final-cut": "agent: image final cut", "carrier-restored": "tunnel: flow carrier resumed"}


class SetupError(Exception):
    pass


def require(value, criterion):
    if not value:
        raise SetupError(criterion)


def safe_label(value):
    require(isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9-]{0,60}", value), "invalid_label")
    return value


def record_message(line):
    """Accept JSON slog or Go text slog, never a diagnostic substring."""
    if line.startswith("{"):
        value = json.loads(line)
        require(isinstance(value.get("msg"), str), "invalid_log_record")
        return value
    match = re.search(r'(?:^| )msg="([^"\n]+)"(?: |$)', line)
    if not match:
        # Restored carriers use the Go default handler on stderr. The four
        # native streams retain those accepted RESUME records as well as slog.
        # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN
        match = re.match(r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} INFO '
                         r'(tunnel: flow carrier resumed|tunnel: flow handoff prepared)(?: |$)', line)
        if not match:
            return None
    value = dict(msg=match[1])
    for field in ("generation", "owner_generation", "carrier_generation", "mode", "applied_revision", "epoch", "freeze_window_ms"):
        match = re.search(r'(?:^| )'+field+r'=(\d+)(?: |$)', line)
        if match:
            value[field] = int(match[1])
    return value


def freeze_values(records):
    # origin: hot-update S5 plan §9.1 / S4 L4. The existing stable activation
    # message is a calibration receipt; 500ms never widens the 5s byte budget.
    values=[]
    for row in records:
        if row['msg'].startswith('agent: image activated '):
            match=re.fullmatch(r'agent: image activated generation=(\d+) tasks=(\d+) freeze_window_ms=(\d+) shim_ms=(\d+) import_ms=(\d+)',row['msg'])
            require(match is not None,'freeze_calibration_fields_missing')
            values.append(int(match.group(3))/1000)
    return values


def log_suffix(before_meta, before, after_meta, after):
    # origin: hot-update S5 plan §9.1 / S4 four-stream cursor discipline.
    for meta, raw in ((before_meta, before), (after_meta, after)):
        require(meta.get("read_ok") is True and type(meta.get("inode")) is int and meta["inode"] > 0
                and meta.get("length") == len(raw), "log_read_incomplete")
    require(before_meta["inode"] == after_meta["inode"], "log_rotated")
    if "parts" in before_meta or "parts" in after_meta:
        old, new = before_meta["parts"], after_meta["parts"]
        require([(p["path"], p["dev"], p["inode"]) for p in old] ==
                [(p["path"], p["dev"], p["inode"]) for p in new], "log_component_rotated")
        offsets, suffixes = [0, 0], []
        for first, last in zip(old, new):
            chunks = [raw[offset:offset+part["length"]] for raw, offset, part in
                      zip((before, after), offsets, (first, last))]
            require(chunks[1].startswith(chunks[0]) and len(chunks[1]) == last["length"], "log_prefix_lost")
            suffixes.append(chunks[1][len(chunks[0]):])
            offsets = [offset+part["length"] for offset, part in zip(offsets, (first, last))]
        require(offsets == [len(before), len(after)], "log_partition_incomplete")
        suffix = b"".join(suffixes)
    else:
        require(after.startswith(before), "log_prefix_lost")
        suffix = after[len(before):]
    if "journal_cursors" in before_meta or "journal_cursors" in after_meta:
        old, new = before_meta["journal_cursors"], after_meta["journal_cursors"]
        require(before_meta["journal_identity"] == after_meta["journal_identity"] and new[:len(old)] == old,
                "journal_cursor_lost")
    return suffix


class Transport:
    def __init__(self, args):
        self.prefix = ["sh", str(args.here / "drills/lib/flow_fixture_transport.sh"),
                       str(args.here), args.instance, args.sim]
        self.children = []

    def run(self, *args, data=None, timeout=30, allowed=(0,)):
        value = peer.run_supervised(self.prefix + list(map(str, args)), input=data,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        # Keep command identity without private input, arguments or error text.
        # A native status alone cannot locate a failed deploy boundary.
        operation = str(args[0]) if args else "unknown"
        verb = str(args[1]) if operation == "ctl" and len(args) > 1 else ""
        boundary = operation + ("_" + verb if verb else "")
        # origin: hot-update S5 plan §9.4 / DRILL-RUN. The CLI's fixed
        # expose code classifies readiness/storage failures. Keep only known
        # codes; its free-form detail may contain private request material.
        if operation == "ctl" and verb == "expose":
            # origin: hot-update S10 plan §6 / P11-P12 seam. The shared
            # launcher appends its own fixed failure diagnostic after stderr.
            code = re.search(rb"(?m)\(([a-z_:]+)\)[ \t]*$", value.stderr)
            if code and code[1] in {b"agent_no_responders", b"home_catching_up", b"try_again",
                    b"node_offline", b"node_not_found", b"store_error", b"name_taken",
                    b"agent_rejected:home_catching_up", b"agent_rejected:try_again", b"agent_rejected:frpc_failed"}:
                boundary += "_"+code[1].decode().replace(":", "_")
        if operation == "policy":
            # Only fixed provisioning phases leave the captured stderr.
            # origin: hot-update S5 plan §9.4 / DRILL-RUN 75.B
            phases = re.findall(rb"(?m)^FLOW_POLICY_PHASE=(provision|allowlist)$", value.stderr)
            if phases:
                boundary += "_"+phases[-1].decode()
            result = re.findall(rb"(?m)^FLOW_POLICY_RESULT=(success|start-limit-hit|exit-code|signal|timeout|resources|dependency|unavailable)$", value.stderr)
            if result:
                boundary += "_"+result[-1].decode().replace("-", "_")
        require(value.returncode in allowed, "transport_native_rc_"+str(value.returncode)+"_"+boundary)
        return value.stdout

    def json(self, *args, data=None, timeout=30, allowed=(0,)):
        value = json.loads(self.run(*args, data=data, timeout=timeout, allowed=allowed))
        require(isinstance(value, dict), "remote_observation_failed")
        return value

    def start(self, stdout, stderr, *args):
        # Only collectors are started here; the product's daemon is untouched.
        with stdout.open("wb") as out, stderr.open("wb") as err:
            child = peer.start_supervised(self.prefix + list(map(str, args)), stdout=out, stderr=err)
        self.children.append(child)
        return child

    def close(self):
        errors = []
        for child in self.children:
            try:
                # Peer cleanup ACK precedes thread/socket retirement. Give the
                # original waiter its natural exit before emergency teardown.
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                errors.append("collector_did_not_exit")
                child.terminate()
        for child in self.children:
            try:
                status = child.wait(timeout=10)
                if status != 0:
                    errors.append(status)
            except subprocess.TimeoutExpired:
                errors.append("join_timeout")
        require(not errors, "transport_cleanup_failed")


class Fixture:
    def __init__(self, args, transport=None):
        self.args, self.root, self.nonce = args, args.root, args.nonce
        require(args.profile in PROFILES and re.fullmatch("[0-9a-f]{32}", args.nonce), "invalid_fixture_contract")
        self.transport = transport or Transport(args)
        self.remote = "/tmp/s5-flow-"+self.nonce
        self.remote_script = self.remote+"/flow_fixture_peer.py"
        self.specs, self.snapshots, self.jobs, self.events, self.consumed = [], {}, [], [], set()
        self.stream_events = {}
        self.phase, self.races, self.race_errors = "new", [], []
        self.request_job = None
        self.product_observer_started = False
        self.observer = None
        self.observer_errors = []
        self.lock = threading.RLock()
        self.shutdown = threading.Event()
        self.event_baseline = None
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)

    def remote_call(self, node, command, **fields):
        raw = json.dumps(dict(command=command, **fields)).encode()
        result = self.transport.json("input", node, "python3", "-I", "-B", self.remote_script,
                                     "call", "--root", self.remote, data=raw, timeout=65,
                                     allowed=(0,1,2) if command == "judge" else (0,2))
        # origin: hot-update S5 plan §9.4 / DRILL-RUN. Retain the fixed
        # observer criterion instead of flattening every failed oracle to rc2.
        if command == "product-receipt" and result.get("rc") == 2:
            criterion = result.get("criterion", "remote_observation_failed")
            require(isinstance(criterion, str) and re.fullmatch(r"[a-z][a-z_0-9]*", criterion), "remote_observation_failed")
            if 'public_observations' in result:
                peer.publish(self.root / ('failure-'+safe_label(fields['label'])+'.json'), result['public_observations'])
            raise SetupError("product_receipt_"+criterion)
        # Fixed peer error classes locate failed socket setup without exposing
        # addresses, key material, or arbitrary exception messages.
        # origin: hot-update S5 plan §9.4 / DRILL-RUN
        if result.get("rc") == 2 and result.get("criterion") in peer.PUBLIC_ERRORS:
            raise SetupError("peer_"+result["criterion"])
        require(command == "judge" or result.get("rc", 0) == 0, "remote_observation_failed")
        return result

    def kernel(self):
        return self.transport.json("exec", "agt1", "python3", "-I", "-B", self.remote_script,
                                   "kernel", "--root", self.remote)

    def log_snapshot(self, phase):
        folder = self.root / "logs" / phase
        folder.mkdir(parents=True, exist_ok=True)
        # origin: hot-update S5 plan §9.1 / DRILL-RUN. Independent streams
        # keep their own cursors; serial transport overhead delays cut faults.
        def read_stream(stream):
            node = "brk1" if stream.startswith("broker") else "agt1"
            raw, meta = folder / (stream+".raw"), folder / (stream+".json")
            self.transport.run("logs", stream, node, raw, meta)
            return stream, probe.read_json(meta), raw.read_bytes()
        with ThreadPoolExecutor(max_workers=4) as workers:
            tasks = [workers.submit(read_stream, stream) for stream in
                     ("broker_slog", "broker_panic", "agent_slog", "agent_panic")]
            return [task.result() for task in tasks]

    def read_events(self):
        with self.lock:
            current = self.log_snapshot("current")
            original = {stream: (meta, raw) for stream, meta, raw in self.logs_before}
            for stream, meta, raw in current:
                suffix = log_suffix(*original[stream], meta, raw)
                if stream not in ("agent_slog", "agent_panic"):
                    continue
                rows = []
                for line in suffix.decode().splitlines():
                    record = record_message(line)
                    if record is not None:
                        rows.append(record)
                # Keep event IDs stable when another source grows after a
                # barrier was consumed; concatenating full streams shifts IDs.
                prior = self.stream_events.get(stream, [])
                require(rows[:len(prior)] == prior, "event_prefix_lost")
                self.events.extend(rows[len(prior):])
                self.stream_events[stream] = rows
            peer.publish(self.root / "events.json", dict(nonce=self.nonce, records=self.events))
            return list(self.events)

    def wait_event(self, event, seconds):
        require(event in (*EVENTS, "candidate-boot-persisted", "watchdog-rollback"), "unknown_barrier")
        deadline = time.monotonic()+float(seconds)
        require(0 < float(seconds) <= 150, "invalid_barrier_deadline")
        minimum_seq = self.event_baseline or 0
        while not self.shutdown.is_set():
            if event == "candidate-boot-persisted":
                observation = self.kernel()
                marker = observation["marker"]
                if marker and marker["state"] == "pending" and marker["boot_count"] > 0 and observation["running_sha"] == marker["new_sha"]:
                    peer.publish(self.root / (event+".json"), observation)
                    return observation
            else:
                records = self.read_events()
                for seq, record in enumerate(records):
                    if (event, seq) in self.consumed:
                        continue
                    if event == "watchdog-rollback":
                        valid = record["msg"] == "agent: image re-dispatched" and "generation" in record
                        if valid:
                            observation = self.kernel()
                            captured = self.remote_call('agt1', 'rollback-marker').get('observation')
                            marker = observation['marker'] or (captured or {}).get('marker')
                            pending = probe.read_json(self.root / 'pending-marker.json')
                            before = self.snapshots['before']['agent']
                            valid = (marker and marker.get('state') == 'rolled_back' and
                                     marker.get('upgrade_id') == pending['upgrade_id'] and
                                     marker.get('new_sha') == pending['new_sha'] and
                                     marker.get('generation', 0) > pending['generation'] and
                                     observation['identity'] == before['identity'] and
                                     observation['running_sha'] == before['running_sha'])
                            if valid:
                                self.rollback_marker = marker
                                peer.publish(self.root / "watchdog-rollback.json", dict(agent=observation, terminal_observation=captured))
                    else:
                        valid = record["msg"] == EVENTS[event]
                        require(not valid or type(record.get("generation" if event == "final-cut" else "carrier_generation")) is int,
                                "barrier_fields_missing")
                        if valid:
                            # origin: hot-update S5 plan §9.1 / T48. Initial
                            # business binding cannot trigger an upgrade race.
                            # Pin this floor before another checkpoint advances it.
                            valid = seq >= minimum_seq
                            if event == "resume":
                                # Reconnecting the original owner during
                                # PREPARE is not the candidate's activation.
                                # origin: hot-update S5 plan §9.1 / T48
                                valid = valid and record.get("mode") == 4 and record.get("owner_generation", 0) > 1
                    if valid:
                        self.consumed.add((event, seq))
                        receipt = dict(event=event, event_seq=seq+1, record=record, observed_at=time.monotonic())
                        peer.publish(self.root / (event+"-event.json"), receipt)
                        if event == "final-cut" and self.observer is None:
                            for node in ("ctl1", "agt1"):
                                self.remote_call(node, "mark")
                        return receipt
            require(time.monotonic() < deadline, "barrier_not_observed_"+event)
            time.sleep(.02)
        raise SetupError("collector_stopped")

    def prepare(self):
        require(self.phase == "new", "fixture_already_prepared")
        self.transport.run("policy", timeout=120)
        self.logs_before = self.log_snapshot("before")
        for node, role in (("agt1", "target"), ("ctl1", "client")):
            self.transport.run("exec", node, "mkdir", "-m", "700", self.remote)
            self.transport.run("exec", node, "chown", "sim:sim", self.remote)
            for name in ("flow_fixture_peer.py", "flow_fixture_askpass.sh", "flow_observation.py", "hot_update_flow_probe.py", "hot_update_mixed_probe.py", "hot_update_budget.sh"):
                self.transport.run("copy-to", self.args.here / "drills/lib" / name, node, self.remote+"/"+name)
            self.transport.run('exec',node,'chmod','0755',self.remote+'/flow_fixture_askpass.sh')
            self.jobs.append(self.transport.start(self.root / (role+".stdout"), self.root / (role+".stderr"),
                "exec" if role=='target' else "exec-user", node, "python3", "-I", "-B", self.remote_script, "serve", "--root", self.remote,
                "--nonce", self.nonce, "--role", role))
        deadline = time.monotonic()+15
        while True:
            try:
                ready = self.transport.json("exec", "agt1", "cat", self.remote+"/ready.json")
                self.remote_call("ctl1", "snapshot")
                break
            except (SetupError, json.JSONDecodeError):
                require(time.monotonic() < deadline, "peer_start_deadline")
                time.sleep(.05)
        self.product_observer_started=True
        pressure = self.transport.run("observe-start", self.args.profile, self.nonce, timeout=60)
        # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN.
        # A successful adapter invocation alone does not prove that the
        # real disposable namespace acquired the short-write network fault.
        if self.args.profile != 'legacy-exporter':
            require(any(re.fullmatch(rb'\{"cipher_pressure_loopback_mtu":\s*1500\}',line)
                        for line in pressure.splitlines()),'cipher_short_write_network_fault_unavailable')
            peer.publish(self.root/'cipher-pressure.json',dict(loopback_mtu=1500))
        port = ready["port"]
        open_port = ready["open_port"]
        ip = self.transport.run("ip", "agt1").decode().strip()
        require(re.fullmatch(r"\d+\.\d+\.\d+\.\d+", ip) and type(port) is int and 0 < port <= 65535
                and type(open_port) is int and 0 < open_port <= 65535 and port != open_port, "invalid_open_target")
        self.transport.run("ctl", "expose", "agt1", "--local", port, "--name", "flow-data")
        public = self.transport.json("ctl", "expose", "explain", "flow-data", "--json")
        # OPEN gets its own listener/expose, so the SYN fault never suppresses
        # target connects for ten-per-second requests or existing survivors.
        self.transport.run("ctl", "expose", "agt1", "--local", open_port, "--name", "flow-open")
        opening = self.transport.json("ctl", "expose", "explain", "flow-open", "--json")
        self.transport.run('ctl','expose','agt1','--local','22','--name','flow-ssh')
        ssh=self.transport.json('ctl','expose','explain','flow-ssh','--json')
        # L/F exercise the frozen historical raw protocol, whose bridge closes
        # both directions at first EOF. Declare duplex traffic before starting
        # that business; A/B/C/R/W retain the >8 MiB half-close tail oracle.
        # origin: hot-update S5 plan §1 / §9.1 / §9.3 / DRILL-RUN 75.L
        tail = "legacy-duplex" if self.args.profile in ("legacy-broker", "legacy-exporter") else "half-close"
        names = ["tcp-"+str(n) for n in range(4)] + [tail, "bob", "alice", "ssh"]
        if self.args.profile == "silent-revoke":
            names += ["alice-final-cut", "alice-resume"]
        # origin: hot-update S5 plan §9.1 / DRILL-RUN. Create the complete
        # initial keyset while OFF: a ready ACK for an older empty keyset does
        # not authorize subscriptions awaiting the cluster reaper's next tick.
        subscriptions = {}
        for name in names:
            if name == "bob" or name.startswith("alice"):
                out = self.transport.run("ctl", "proxy", "sub", "create", "--name", name).decode()
                tokens = re.findall(r"/sub/([A-Za-z0-9._-]+)", out)
                require(len(tokens) == 1, "subscription_supply_missing")
                subscriptions[name] = tokens[0]
        self.transport.run("ctl", "proxy", "on", "--yes")
        subscription_deadline = self.wait_proxy_ready()
        for name in names+["opening"]:
            kind = "half-close" if name == "half-close" else "slow" if name == "capacity-0" else "normal"
            protocol = "ss" if name == "bob" or name.startswith("alice") else "raw"
            size = 32768 if kind == "half-close" else 128
            up = 20 if kind == "half-close" else 2400
            down = 1024 if kind == "half-close" else 2400
            spec = dict(id=name, protocol=protocol, **{"class": kind}, size=size,
                        declared_at=time.monotonic(), host=public["home_broker"], port=public["public_port"],
                        up=peer.declaration(name+".up", self.nonce, up, size),
                        down=peer.declaration(name+".down", self.nonce, down, size))
            if protocol == "ss":
                # origin: hot-update S5 plan §9.1 / DRILL-RUN. Small paced
                # writes cannot fill the 8 MiB carrier credit and produce a
                # genuine pending cipher suffix. Totals are fixed before start.
                spec.update(burst=True, size=32768,
                            up=peer.declaration(name+".up", self.nonce, 2400, 32768, burst=True),
                            down=peer.declaration(name+".down", self.nonce, 2400, 32768, burst=True))
            if name == "opening":
                spec.update(host=opening["home_broker"], port=opening["public_port"])
            if name=='ssh':
                spec.update(host=ssh['home_broker'],port=ssh['public_port'])
            if protocol == "ss":
                key_name = "bob" if name == "bob" else name
                tokens = [subscriptions[key_name]]
                # Pipe private YAML straight to the remote descriptor keeper.
                # It is never persisted to an ordinary file or echoed in steps.
                key = self.subscription_key(key_name, tokens[0], ip, port, subscription_deadline)
                spec.update(key=key_name, host=key["host"], port=key["port"])
            self.specs.append(spec)
        self.short_flows = {name: hashlib.sha256((self.nonce+name).encode()).hexdigest()
                            for name in ('short-before', 'short-between')} if self.args.arm == 'B' else {}
        # Both phases are declared before first business; capacity never
        # borrows a normal request's success oracle or changes its totals.
        # origin: hot-update S5 plan lead C1 / §3.4 / DRILL-RUN
        self.saturation_specs = []
        if self.args.profile == "capacity-32":
            template = next(row for row in self.specs if row["id"] == "tcp-0")
            for index in range(32):
                name = "saturation-"+str(index)
                self.saturation_specs.append(dict(template, id=name, finite_fin=True,
                    up=peer.declaration(name+".up", self.nonce, 600, 128),
                    down=peer.declaration(name+".down", self.nonce, 600, 128)))
            self.short_flows["capacity-recovery"] = hashlib.sha256((self.nonce+"capacity-recovery").encode()).hexdigest()
            peer.publish(self.root/"capacity-declarations.json", dict(nonce=self.nonce,
                flows=self.saturation_specs, reservation=256<<20, normal_limit=224<<20))
        self.remote_call("agt1", "configure", flows=self.specs+self.saturation_specs,
                         short_flows=self.short_flows, watch_marker=self.args.arm=='W')
        peer.publish(self.root / 'short-declarations.json', self.short_flows)
        # Public declarations contain totals and classes only. No subscription
        # body, target parser or crypto state enters a host JSON receipt.
        public_specs = [{k: row[k] for k in ("id", "protocol", "class", "declared_at", "up", "down")}
                        for row in self.specs]
        peer.publish(self.root / "declarations.json", dict(nonce=self.nonce, flows=public_specs))
        # origin: hot-update S5 plan §9.1 / DRILL-RUN. --local PORT
        # exposes 127.0.0.1:PORT; the NIC address belongs only to SS's target.
        peer.publish(self.root / "fixture.json", dict(nonce=self.nonce, profile=self.args.profile, open=dict(ip="127.0.0.1", port=open_port)))
        self.phase = "prepared"
        self.checkpoint("before")
        return dict(prepared=True)

    def wait_proxy_ready(self):
        # origin: hot-update S5 plan §9.1 / DRILL-RUN. The directive reply
        # precedes the applied ready ACK; an empty subscription is a setup race.
        deadline = time.monotonic() + 15
        while True:
            status = self.transport.json("ctl", "proxy", "status", "--json")
            # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.C.
            # The NATS view can precede the HTTP replica. Keep only public
            # readiness facts; subscriber tokens and supply never enter logs.
            observed_at = time.monotonic()
            peer.publish(self.root / 'proxy-readiness.json',dict(at=observed_at,deadline=deadline,
                enabled=status.get('enabled'),nodes=[{key:row[key] for key in
                ('nid','status','ready','public_port','home_broker','ready_reason') if key in row}
                for row in status.get('nodes',[])]))
            if status.get("enabled") is True and any(
                    row.get("nid") == "agt1" and row.get("ready") is True for row in status.get("nodes", [])):
                return deadline
            require(observed_at < deadline, "proxy_listener_not_ready")
            time.sleep(.05)

    def subscription_key(self, name, token, ip, port, deadline):
        # Cluster status and the HTTP replica converge independently. Observe
        # the actual subscription before business, inside the SAME readiness
        # budget. Retry only its typed empty-node view, never authentication.
        # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.C
        attempts = 0
        while True:
            body = self.transport.run("input", "brk1", "curl", "-fsS", "--max-time", "6", "--config", "-",
                data=('url = "http://127.0.0.1:8090/sub/'+token+'"\n').encode())
            key = self.transport.json("input", "ctl1", "python3", "-I", "-B", self.remote_script,
                "key", "--root", self.remote, "--name", name,
                data=socket.inet_aton(ip)+port.to_bytes(2,'big')+body, allowed=(0,2))
            attempts += 1
            peer.publish(self.root / 'subscription-readiness.json',dict(name=name,attempts=attempts,
                at=time.monotonic(),deadline=deadline,ready=key.get('rc',0)==0,
                criterion=key.get('criterion') if key.get('criterion')=='subscription_node_not_ready' else None))
            if key.get("rc", 0) == 0:
                return key
            require(key.get("criterion") == "subscription_node_not_ready", "subscription_supply_invalid")
            require(time.monotonic() < deadline, "subscription_replica_not_ready")
            time.sleep(.05)

    # origin: hot-update S5 plan lead C1 / §3.4 / DRILL-RUN.
    def saturation(self):
        require(self.phase == "prepared" and self.args.profile == "capacity-32", "capacity_phase_order")
        names = [row["id"] for row in self.saturation_specs]
        require(len(names) == 32 and not self.request_job, "capacity_cohort_count")
        before, allocation = self.kernel(), self.sql("capacity-before")
        self.remote_call("ctl1", "start", flows=self.saturation_specs)
        deadline = time.monotonic()+20
        while True:
            snapshots = {role:self.remote_call(node, "snapshot") for node,role in (("ctl1","client"),("agt1","target"))}
            if all(set(doc["flows"]) == set(names) and all(len(doc["flows"][name][direction]) >= 20
                for name in names for direction in ("sent","received")) for doc in snapshots.values()):
                break
            require(time.monotonic() < deadline, "capacity_baseline_missing")
            time.sleep(.02)
        full = self.product_receipt("capacity-full", self.kernel(), snapshots["target"])
        require(full.get("active_flows") == 32 and full.get("reservation") == 256<<20, "capacity_full_budget_missing")
        peer.publish(self.root/"capacity-full.json", full)
        spec = self.specs[0]
        pending = self.remote_call("ctl1", "capacity-open", name="capacity-recovery",
            host=spec["host"], port=spec["port"], payload=self.short_flows["capacity-recovery"])
        refusals = [self.remote_call("ctl1", "capacity-exec") for _ in range(2)]
        require(all(row.get("native_rc") == 70 and row.get("error_type") == "workload.ErrBackpressure"
                    and row.get("success") is False for row in refusals), "capacity_exec_refusal_type")
        require(self.remote_call("agt1", "capacity-exec-ledger")["lines"] == [], "capacity_exec_side_effect")
        refused = self.product_receipt("capacity-refused", self.kernel(), self.remote_call("agt1", "snapshot"))
        require(refused.get("flow_refusals",0) >= 1 and refused.get("reason") == 10 and
                refused.get("active_flows") == 32 and refused.get("reservation") == 256<<20, "capacity_flow_refusal_type")
        peer.publish(self.root/"capacity-refused.json", dict(refused, exec_refusals=refusals, pending=pending))
        with ThreadPoolExecutor(max_workers=2) as workers:
            tasks = {role:workers.submit(self.remote_call,node,"drain-saturation",names=names)
                     for node,role in (("ctl1","client"),("agt1","target"))}
            final = {role:task.result() for role,task in tasks.items()}
        folder = self.root/"saturation"
        folder.mkdir()
        for node,role in (("ctl1","client"),("agt1","target")):
            (folder/role).mkdir()
            for name in names:
                for suffix in ("sent","received","json"):
                    self.transport.run("copy-from",node,self.remote+"/"+name+"."+suffix,folder/role/(name+"."+suffix))
        self.check_saturation_ledgers(folder, self.saturation_specs, snapshots, final)
        recovery = self.remote_call("ctl1", "capacity-recover", payload=self.short_flows["capacity-recovery"])
        require(recovery.get("identity") == pending.get("identity") and recovery.get("bytes") == 32 and
                recovery.get("sha256") == hashlib.sha256(bytes.fromhex(self.short_flows["capacity-recovery"])).hexdigest(),
                "capacity_recovery_socket_changed")
        require(self.remote_call("agt1", "snapshot")["short_accepts"].get("capacity-recovery") == 1, "capacity_recovery_accept_count")
        released = self.product_receipt("capacity-released", self.kernel(), self.remote_call("agt1", "snapshot"))
        require(released.get("reservation") == 0 and released.get("settled_originals") == 32, "capacity_reservation_not_released")
        success = self.remote_call("ctl1", "capacity-exec")
        require(success.get("native_rc") == 0 and success.get("success") is True, "capacity_exec_recovery_failed")
        require(self.remote_call("agt1", "capacity-exec-ledger")["lines"] == [self.nonce+"-capacity"], "capacity_exec_recovery_ledger")
        after = self.kernel()
        require(after["identity"] == before["identity"] and after["running_sha"] == before["running_sha"] and
                after.get("marker") == before.get("marker") and self.sql("capacity-after") == allocation, "capacity_cluster_changed")
        peer.publish(self.root/"capacity-recovery.json", dict(socket=recovery, exec=success, released=released,
                     original_identity=after["identity"], running_sha=after["running_sha"], allocation_unchanged=True))
        for node in ("ctl1","agt1"):
            require(self.remote_call(node,"retire-saturation",names=names).get("retired") == 32, "capacity_retirement_incomplete")
        self.capacity_complete = True
        # The normal cut's identity and SQL baseline belongs AFTER natural FIN,
        # with the same product processes, topology, allocation and image.
        self.checkpoint("before")
        return dict(saturation_complete=True, flow_refusals=refused["flow_refusals"], exec_refusals=len(refusals), recovered=True)

    def check_saturation_ledgers(self, folder, specs, before, final):
        require(len(specs) == 32, "capacity_final_cohort_count")
        for spec in specs:
            name = spec["id"]
            for role in ("client","target"):
                row = final[role]["flows"][name]
                require(row["target_accepts"] == 1 and row.get("fin_at") is not None and
                        [event["kind"] for event in row["events"]] == ["eof"], "capacity_natural_fin_missing")
                probe.check_identity(before[role]["flows"][name]["identities"]+row["identities"], True)
                require(row["fin_at"] >= max(item["at"] for item in row["sent"]), "capacity_fin_before_final_ledger")
            for direction,sender,receiver in (("up","client","target"),("down","target","client")):
                wanted = spec[direction]
                sent = (folder/sender/(name+".sent")).read_bytes()
                received = (folder/receiver/(name+".received")).read_bytes()
                require(sent == received and len(sent) == wanted["bytes"] and probe.digest(sent) == wanted["sha256"], "capacity_final_bytes_changed")
                parsed = probe.parse_frames(sent, self.nonce)
                require(len(parsed) == wanted["records"] and [row["seq"] for row in parsed] == list(range(wanted["records"])), "capacity_final_sequence_changed")
                sender_ledger = final[sender]["flows"][name]["sent"]
                require(len(sender_ledger) == len(parsed) and all(all(actual[key] == observed[key] for key in ("seq","length","sha256"))
                    for actual,observed in zip(parsed,sender_ledger)), "capacity_final_sender_ledger")
                ledger = final[receiver]["flows"][name]["received"]
                require(len(ledger) == len(parsed) and all(all(actual[key] == observed[key] for key in ("seq","length","sha256"))
                    for actual,observed in zip(parsed,ledger)), "capacity_final_receiver_ledger")
                eof = final[receiver]["flows"][name]["events"][0]["at"]
                require(eof >= max(row["at"] for row in ledger), "capacity_eof_before_final_ledger")
        peer.publish(folder/"ledgers.json",dict(flows=32, directions=64, natural_fin=True, final=final))

    def start(self):
        require(self.phase == "prepared", "fixture_start_order")
        require(self.args.profile != "capacity-32" or getattr(self,"capacity_complete",False), "normal_before_capacity_recovery")
        self.remote_call("ctl1", "start", flows=[row for row in self.specs if row["id"] != "opening"])
        if self.args.arm == 'B':
            self.short_flow('short-before')
        # origin: hot-update S10 plan §3.2 / §10.2 P12.
        # P17 owns the fullstack schedule. Legacy drills keep their collector;
        # the fullstack adapter must explicitly turn it off before business.
        if getattr(self.args, "request_scheduler", True):
            self.transport.run("exec", "agt1", "mkdir", "-p", "/tmp/hot-update-mixed-"+self.nonce)
            self.transport.run("exec", "agt1", "chown", "sim:sim", "/tmp/hot-update-mixed-"+self.nonce)
            self.request_job = self.transport.start(self.root / "requests.stdout", self.root / "requests.stderr", "exec-user", "ctl1",
                "python3", "-I", "-B", self.remote+"/hot_update_mixed_probe.py", "run", "--evidence", self.remote+"/requests",
                "--rate", "10", "--until-mark", "finish", "--tail", "2", "--max-duration", "600",
                "--node", "agt1", "--nonce", self.nonce)
            self.jobs.append(self.request_job)
        self.phase = "running"
        return dict(started=True)

    def short_flow(self, name):
        # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN. The server must
        # accept exactly the predeclared connection and echo its exact bytes.
        spec = next(row for row in self.specs if row['id'] == 'tcp-0')
        data = self.short_flows[name]
        result = self.remote_call('ctl1', 'short-flow', name=name, host=spec['host'],
                                  port=spec['port'], payload=data)
        require(result == dict(bytes=len(bytes.fromhex(data)), sha256=hashlib.sha256(bytes.fromhex(data)).hexdigest()),
                'short_flow_bytes_changed')
        target = self.remote_call('agt1', 'snapshot')
        require(target.get('short_accepts', {}).get(name) == 1, 'short_flow_accept_count')
        peer.publish(self.root / (name+'.json'), result)
        return result

    def baseline(self):
        require(self.phase == "running", "baseline_before_start")
        deadline = time.monotonic()+20
        while True:
            ctl, target = self.remote_call("ctl1", "snapshot"), self.remote_call("agt1", "snapshot")
            names = [row["id"] for row in self.specs if row["id"] != "opening"]
            complete = all(name in doc["flows"] and len(doc["flows"][name]["sent"]) >= 20 and
                           len(doc["flows"][name]["received"]) >= 20 for doc in (ctl, target) for name in names)
            if complete and self.args.profile != "legacy-exporter" and any(row.get("protocol") == "ss" for row in self.specs):
                # origin: hot-update S5 plan §9.1 / DRILL-RUN. A cut may
                # begin only after the required actual partial writes exist.
                prior = self.product_receipt("partial-ready", self.kernel(), target)
                peer.publish(self.root / "partial-ready.json", prior)
                complete = prior.get("partial_ready") is True
            if complete:
                self.remote_call("ctl1", "silent")
                self.remote_call("agt1", "silent")
                self.request_mark("cohort")
                result = self.checkpoint("baseline")
                self.start_observer()
                return result
            require(time.monotonic() < deadline, "baseline_duplex_samples_missing")
            time.sleep(.05)

    def carrier_baseline(self):
        # OPEN has no application bytes before its original EINPROGRESS cut.
        # Its FD proof spans that cut; its duplex samples span the later real
        # carrier loss, after both original endpoints have made progress.
        # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN 75.A
        require(self.phase == "running", "carrier_baseline_before_start")
        deadline = time.monotonic()+5
        while True:
            snapshots = [self.remote_call(node, "snapshot") for node in ("ctl1", "agt1")]
            if all("opening" in doc["flows"] and
                   all(len(doc["flows"]["opening"][direction]) >= 20 for direction in ("sent", "received"))
                   for doc in snapshots):
                marks = {node:self.remote_call(node, "mark-opening") for node in ("ctl1", "agt1")}
                peer.publish(self.root/"opening-carrier-baseline.json", dict(observers=marks))
                return dict(marked=True)
            require(time.monotonic() < deadline, "opening_duplex_samples_missing")
            time.sleep(.02)

    def start_observer(self):
        # origin: hot-update S5 plan §9.1 / T48. B/C/W do not synchronously
        # wait for final-cut. Observe their actual log barrier while traffic is
        # running, rather than label a later checkpoint as the freeze boundary.
        minimum_seq = self.event_baseline or 0
        observed = set()
        def watch():
            try:
                while not self.shutdown.is_set():
                    for seq, record in enumerate(self.read_events()):
                        if seq < minimum_seq or seq in observed or record['msg'] != EVENTS['final-cut']:
                            continue
                        require(type(record.get('generation')) is int, 'barrier_fields_missing')
                        marks = {node:self.remote_call(node,'mark') for node in ('ctl1','agt1')}
                        peer.publish(self.root / ('cut-'+str(seq+1)+'.json'),
                            dict(event_seq=seq+1,generation=record['generation'],observers=marks))
                        observed.add(seq)
                    self.shutdown.wait(.02)
            except (SetupError, OSError, ValueError, KeyError, subprocess.SubprocessError):
                self.observer_errors.append('cut_observation_failed')
        self.observer = threading.Thread(target=watch)
        self.observer.start()

    def sql(self, label):
        # Intentionally omit keys/token: allocation identity uses a digest.
        query = "SELECT sid,nid,name,port,home_broker,epoch,state FROM port_allocations ORDER BY sid,nid,name;"
        raw = self.transport.run("exec", "brk1", "runuser", "-u", "tether", "--", "sqlite3", "-json",
                                 "/var/lib/tether/tether.db", query)
        rows = json.loads(raw)
        require(isinstance(rows, list) and rows, "sql_allocation_receipt_empty")
        peer.publish(self.root / ("sql-"+label+".json"), rows)
        return rows

    def broker_kernels(self, label):
        # origin: hot-update S5 plan §9.1 / DRILL-RUN. Observe the exact SQL
        # allocation owners; cluster growth may place every listener on brk3.
        rows = self.snapshots.get(label, {}).get('sql') or self.sql(label)
        owners = sorted({row['home_broker'] for row in rows})
        require(owners and all(node in ('brk1','brk2','brk3') for node in owners), 'broker_owner_unavailable')
        return {node: self.transport.json('exec', node, 'python3', '-I', '-B', '/tmp/flow_fixture_peer.py',
                'kernel', '--root', '/tmp', '--unit', 'tether-broker') for node in owners}

    def product_receipt(self, label, observation, target):
        deadline=time.monotonic()+(5 if self.product_observer_started and label in ('revoked','revoke-races','open-completed','ready-pending','watchdog') else 0)
        while True:
            try:
                value=self.remote_call('ctl1','product-receipt',label=label,kernel=observation,
                                       target=target,profile=self.args.profile,
                                       **({'broker_kernels':self.broker_kernels(label)} if self.product_observer_started and self.args.profile not in ('legacy-exporter','legacy-broker') and label in ('allocation','ready-pending','watchdog','collect','capacity-full','capacity-refused','capacity-released') else {}))
                break
            except SetupError:
                if time.monotonic()>=deadline:
                    raise
                time.sleep(.1)
                observation=self.kernel();target=self.remote_call('agt1','snapshot')
        require(value.get('custody_complete') is True,'required_product_observation_unavailable')
        return value

    def checkpoint(self, label):
        label = safe_label(label)
        if label == 'active-progress' and self.args.arm == 'B':
            self.short_flow('short-between')
        # origin: hot-update S5 plan §9.1 / DRILL-RUN. Independent read-only
        # censuses share the observation window; serial docker round trips
        # can consume the pending connector's entire original deadline.
        with ThreadPoolExecutor(max_workers=4) as workers:
            observations = [workers.submit(self.kernel),
                            workers.submit(self.remote_call, "ctl1", "snapshot", accepts_only=label=='open-partial'),
                            workers.submit(self.remote_call, "agt1", "snapshot", accepts_only=label=='open-partial'),
                            workers.submit(self.sql, label)]
            observation, ctl, target, sql = [task.result() for task in observations]
        receipt = dict(nonce=self.nonce, agent=observation, client=ctl, target=target, sql=sql)
        self.snapshots[label] = receipt
        peer.publish(self.root / ("checkpoint-"+label+".json"), receipt)
        if label == "before":
            return dict(checkpoint=label)
        before = self.snapshots["before"]["agent"]
        require(observation["identity"] == before["identity"], "agent_identity_changed")
        if label.startswith("hop-") or label in ("current-rollback", "legacy-fault"):
            require(observation["marker"] and observation["marker"]["state"] == "committed", "hop_not_committed")
            reference = self.snapshots["before"] if label in ("hop-1", "legacy-fault") else self.snapshots.get("hop-1", self.snapshots["before"])
            require(observation["running_sha"] != reference["agent"]["running_sha"],
                    "hop_running_sha_unchanged")
            require(observation["running_sha"] == observation["marker"]["new_sha"], "hop_wrong_running_image")
        if label == "refusal-unchanged":
            require(observation["running_sha"] == before["running_sha"] and observation["marker"] == before["marker"], "legacy_refusal_changed_image")
            for node in ('ctl1','agt1'):
                self.remote_call(node,'mark')
        if label == "legacy-fault":
            records = self.read_events()
            # A bare historical exporter cannot emit an S5 image-cut record.
            # The successor's durable commit plus /proc SHA/PID/start ticks
            # prove exec; an invocation or a changed on-disk path cannot.
            seqs = [i+1 for i, row in enumerate(records) if row["msg"] == "agent: upgrade COMMITTED (registered as the new binary)"]
            require(seqs, "legacy_exec_event_missing")
            for node in ('ctl1','agt1'):
                self.remote_call(node,'mark')
            peer.publish(self.root / "legacy-fault.json", dict(nonce=self.nonce, valid_fault=True, observed=True,
                event_seq=seqs[-1], before=dict(**before["identity"], running_sha=before["running_sha"]),
                after=dict(**observation["identity"], running_sha=observation["running_sha"]), marker=observation["marker"]))
        if label == "open-partial":
            failure = None
            try:
                value=self.product_receipt(label,observation,target)
            except SetupError as error:
                value=None
                failure=error
            if value is not None:
                peer.publish(self.root/'open-partial.json',value)
                return dict(checkpoint=label)
            opening = [row for row in observation["connections"] if row["state"] == "02"]
            # SO_ERROR is NEVER read by this observer. Kernel SYN_SENT proves
            # a pending connect, but cannot identify the product's cipher cursor.
            peer.publish(self.root / "open-partial.json", dict(nonce=self.nonce, kernel_syn_sent=opening,
                missing_observations=["product_partial_cipher_write", "product_seal_attempts"]))
            if self.product_observer_started and failure is not None:
                raise failure
            raise SetupError("product_partial_cipher_observer_unavailable")
        if label in ("revoked", "revoke-races", "ready-pending", "watchdog", "active-progress", "allocation", "legacy-transport"):
            self.read_events()
            failure=None
            try:
                value=self.product_receipt(label,observation,target)
            except SetupError as error:
                value=None
                failure=error
            if value is not None:
                if label=='allocation':
                    require(receipt['sql']==self.snapshots['before']['sql'],'allocation_changed')
                    value['allocation_unchanged']=True
                if label=='legacy-transport':
                    value['broker_running_sha']=probe.read_json(self.root/'legacy-broker.json')['running_sha']
                if label=='ready-pending':
                    self.watchdog_ready=value
                if label=='watchdog':
                    prior=getattr(self,'watchdog_fault',None)
                    require(prior is not None,'watchdog_fault_receipt_unavailable')
                    pending=probe.read_json(self.root/'pending-marker.json')
                    require(receipt['sql']==self.snapshots['before']['sql'],'allocation_changed')
                    value.update(prior,agent_uid=observation['uid'],pending=pending['state']=='pending',
                        final_marker=self.rollback_marker['state'],final_running_sha=observation['running_sha'],
                        original_running_sha=before['running_sha'],reaper_running=value['reaper']['running'],
                        allocation_unchanged=True,fresh_ss_grant=bool(value['fresh_ss_grant']),ss_authority_available=True)
                peer.publish(self.root/(label+'.json'),value)
                self.event_baseline=len(self.read_events())
                return dict(checkpoint=label)
            # Preserve all observations before refusing an unobservable prior.
            missing = {"revoked": ["victim_flow_binding", "domain_replay_zero", "applied_revoke_revision"],
                "revoke-races": ["victim_flow_binding", "domain_replay_zero", "applied_revoke_revision"],
                "ready-pending": ["fresh_ss_grant", "domain_budget"],
                "watchdog": ["fresh_ss_grant", "domain_budget"],
                "active-progress": ["product_nonce_progress", "retirement_progress"],
                "allocation": ["domain_budget", "live_reaper_receipt"],
                "legacy-transport": ["negotiated_alpn", "raw_carrier_prefix"]}[label]
            peer.publish(self.root / (label+"-missing.json"), dict(missing_observations=missing))
            if label == "watchdog":
                pending = probe.read_json(self.root / "pending-marker.json")
                peer.publish(self.root / "watchdog.json", dict(nonce=self.nonce, agent_uid=observation["uid"],
                    pending=pending.get("state") == "pending", generation_before=pending.get("generation"),
                    generation_after=observation["marker"].get("generation"), final_marker=observation["marker"].get("state"),
                    final_running_sha=observation["running_sha"], original_running_sha=before["running_sha"],
                    missing_observations=missing))
            raise failure if failure is not None else SetupError("required_product_observation_unavailable")
        if label in ('baseline','hop-1','hop-2','before','open-completed') and self.args.profile!='legacy-exporter':
            try:
                value=self.product_receipt(label,observation,target)
            except SetupError:
                if label=='open-completed':
                    raise
            else:
                peer.publish(self.root/(label+'-product.json'),value)
        self.event_baseline = len(self.read_events())
        return dict(checkpoint=label)

    def revoke(self, name):
        safe_label(name)
        self.transport.run("ctl", "proxy", "sub", "revoke", name)
        peer.publish(self.root / ("revoke-"+name+".json"), dict(name=name, command_rc=0, observed_at=time.monotonic()))
        return dict(revoked=name)

    def arm_revoke(self, events):
        require(events == ["prepare", "final-cut", "resume"], "invalid_revoke_barriers")
        for event, name in zip(events, ("alice", "alice-final-cut", "alice-resume")):
            def race(event=event, name=name):
                try:
                    barrier = self.wait_event(event, 5)
                    self.revoke(name)
                    peer.publish(self.root / ("race-"+event+".json"), barrier)
                except (SetupError, OSError, ValueError):
                    self.race_errors.append(event)
            task = threading.Thread(target=race)
            task.start()
            self.races.append(task)
        return dict(armed=events)

    def finish(self):
        require(self.phase == "running", "finish_before_start")
        # Both original endpoints drain together. A sequential finish would
        # spend the first side's deadline waiting for the second side's tail.
        with ThreadPoolExecutor(max_workers=2) as workers:
            tasks=[workers.submit(self.remote_call,node,'finish') for node in ('ctl1','agt1')]
            for task in tasks:
                task.result()
        for task in self.races:
            task.join(timeout=6)
        require(not self.race_errors, "revoke_barrier_failed")
        self.request_mark("finish")
        # origin: hot-update S5 plan §9.1 / T48. Copying a still-running
        # collector would discard late refusals or side effects during drain.
        require(self.request_job is not None, "request_collector_missing")
        require(self.request_job.wait(timeout=20) == 0, "request_collector_native_failure")
        self.checkpoint("final")
        last = self.snapshots["final"]["agent"]
        peer.publish(self.root / "identity-final.json", dict(**last["identity"], running_sha=last["running_sha"]))
        self.phase = "finished"
        return dict(finished=True)

    def request_mark(self, name):
        # The mark process reads the ctl's own monotonic origin. Host and agent
        # clocks are never subtracted to manufacture a switch intersection.
        self.transport.run("exec", "ctl1", "python3", "-I", "-B", self.remote+"/hot_update_mixed_probe.py",
                           "mark", "--evidence", self.remote+"/requests", "--name", name)

    def request_evidence(self):
        # origin: hot-update S5 plan §9.1 / T48. Reuse the side-effect judge,
        # then pass the full observed schedule to the common five-second judge.
        folder = self.root / "requests"
        metadata = probe.read_json(folder / "run.json")
        require(metadata["nonce"] == self.nonce, "request_cohort_nonce_changed")
        child = peer.run_supervised([sys.executable, "-I", "-B", str(Path(__file__).with_name("hot_update_mixed_probe.py")),
            "judge", "--evidence", str(folder), "--ledger", str(self.root / "request-ledger"),
            "--mode", "upgrade", "--window-start", "cohort", "--window-end", "finish"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        require(child.returncode in (0, 1), "request_evidence_incomplete")
        summary = json.loads(child.stdout)
        mixed = load("hot_update_mixed_probe")
        accepted = mixed.accepted(summary, "upgrade", 10) and summary["configured_rate"] == 10 \
            and summary["refusals"] == 0 and summary["over_5s"] == 0
        peer.publish(self.root / "requests.json", dict(rc=0 if accepted else 1, summary=summary))
        samples = [json.loads(line) for line in (folder / "samples.jsonl").read_text().splitlines()]
        samples.sort(key=lambda row: row["seq"])
        return dict(rate=metadata["rate"], window=dict(start=0, end=metadata["duration_s"]),
            samples=[dict(scheduled=row["seq"]/10, start=row["t_start"], end=row["t_end"], ok=row["rc"] == 0)
                     for row in samples]), accepted

    def collect(self):
        require(self.phase == "finished", "collect_before_finish")
        require(not self.observer_errors, "cut_observation_failed")
        (self.root / "logs.json").unlink(missing_ok=True)
        current = self.log_snapshot("after")
        original = {stream: (meta, raw) for stream, meta, raw in self.logs_before}
        receipts = []
        for stream, meta, raw in current:
            suffix = log_suffix(*original[stream], meta, raw)
            destination = self.root / "logs" / (stream+".log")
            destination.write_bytes(suffix)
            receipts.append(dict(stream=stream, read_ok=True, inode=meta["inode"], length=len(suffix),
                                 sha256=probe.digest(suffix), file=str(destination.relative_to(self.root))))
        peer.publish(self.root / "logs.json", dict(read_ok=True, cursors=receipts))
        peer.publish(self.root / "freeze.json", dict(windows=freeze_values(self.read_events())))
        for node, role in (("agt1", "target"), ("ctl1", "client")):
            folder = self.root / role
            folder.mkdir(exist_ok=True)
            for row in self.specs:
                if row["id"] not in self.snapshots["final"][role]["flows"]:
                    continue
                for suffix in ("sent", "received", "json"):
                    name = row["id"]+"."+suffix
                    self.transport.run("copy-from", node, self.remote+"/"+name, folder/name)
        self.transport.run("copy-from", "ctl1", self.remote+"/requests", self.root / "requests")
        self.transport.run("copy-from", "agt1", "/tmp/hot-update-mixed-"+self.nonce+"/ledger", self.root / "request-ledger")
        self.phase = "collected"
        if self.args.profile=='legacy-exporter':
            # A historical exporter cannot sign product crypto. Only retained
            # native raw loss may reach F's negative control; GREEN still needs
            # custody in the common judge below.
            return dict(collected=True,legacy_negative_control=True)
        try:
            product=self.product_receipt('collect',self.kernel(),self.snapshots['final']['target'])
        except SetupError as error:
            product=None
            # origin: hot-update S5 plan §9.4 / DRILL-RUN. Keep the failed
            # public guard; aggregate missing-domain labels cannot locate it.
            failed_criterion = str(error)
            if not re.fullmatch(r"[a-z][a-z_0-9]*", failed_criterion):
                failed_criterion = "collection_observation_failed"
        if product is not None:
            peer.publish(self.root/'product-census.json',product)
            return dict(collected=True)
        peer.publish(self.root / "collect-missing.json", dict(criterion=failed_criterion,
            missing_observations=["product_seal_attempts", "domain_reservations"]))
        return dict(rc=2, verdict="SETUP-RED", criterion="required_product_observation_unavailable")

    def continuity(self):
        require(self.phase == "collected", "judge_before_collection")
        requests, requests_accepted = self.request_evidence()
        records = self.read_events()
        carriers = [row["carrier_generation"] for row in records if row["msg"] == EVENTS["resume"]]
        if self.args.profile=='legacy-broker':
            prior=probe.read_json(self.root/'legacy-transport.json')
            require(prior.get('carrier_generations'),'legacy_session_generations_unavailable')
        elif self.args.profile!='legacy-exporter':
            require(carriers, "carrier_identity_unavailable")
        evidence = dict(version=1, nonce=self.nonce, flows=[], requests=requests,
                        freeze_windows=freeze_values(records))
        for spec in self.specs:
            name = spec["id"]
            if name.startswith("alice") or name not in self.snapshots["final"]["client"]["flows"]:
                continue
            observations = {}
            for role in ("client", "target"):
                observations[role] = probe.read_json(self.root / role / (name+".json"))
                # SSH's stdio trace cannot inspect its privileged sshd socket.
                # The root target keeper supplies that independent /proc view.
                if name=='ssh':
                    observations[role]['identities']=self.snapshots['final'][role]['flows'][name]['identities']
            directions = {}
            for direction, sender, receiver in (("up", "client", "target"), ("down", "target", "client")):
                sent, received = observations[sender], observations[receiver]
                directions[direction] = dict(expected=spec[direction],
                    sent=dict(file=sender+"/"+name+".sent", ledger=sent["sent"], window=sent["window"]),
                    received=dict(file=receiver+"/"+name+".received", ledger=received["received"], window=received["window"]),
                    window=received["window"], completed_at=received["completed_at"], events=received["events"],
                    eof_expected=spec["class"] == "half-close" and direction == "up")
            identities = {role: observations[role]["identities"] for role in ("client", "target")}
            identities["agent"] = [row["agent"]["identity"] for row in self.snapshots.values()]
            identities["ctl"] = [row["client"]["identity"] for row in self.snapshots.values()]
            evidence["flows"].append(dict(id=name, protocol=spec["protocol"], **({"ss":{}} if spec["protocol"]=="ss" else {}), **{"class": spec["class"]}, declared_at=spec["declared_at"],
                identities=identities, accept_count=observations["target"]["target_accepts"], redials=0,
                carrier_before=str(prior['carrier_generations'].get(str(spec['port']),'') if self.args.profile=='legacy-broker' else carriers[0] if carriers else ''), carrier_after=str(prior['carrier_generations'].get(str(spec['port']),'') if self.args.profile=='legacy-broker' else carriers[-1] if carriers else ''), directions=directions))
        peer.publish(self.root / "evidence.json", evidence)
        # The keeper's filesystem must contain the same public raw inputs; no
        # private ciphertext is copied or re-encoded to reach a judge child.
        self.transport.run("copy-to", self.root / "evidence.json", "ctl1", self.remote+"/evidence.json")
        for role in ("client", "target"):
            self.transport.run("copy-to", self.root / role, "ctl1", self.remote+"/"+role)
        # origin: hot-update S5 plan §9.1 / T48 / DRILL-RUN. Docker cp
        # assigns root ownership; parent runs retain umask 077. The sim keeper
        # needs ownership of these public inputs, with their modes preserved.
        self.transport.run("exec", "ctl1", "chown", "-R", "sim:sim",
                           self.remote+"/evidence.json", self.remote+"/client", self.remote+"/target")
        # Run the original common judge in the remote keeper. Its descriptor
        # namespace stays intact across subprocess judges and until cleanup.
        result = self.remote_call("ctl1", "judge", carrier_transition="unchanged" if self.args.arm == "L" else "replaced",profile=self.args.profile)
        if result.get("rc") != 1 and not requests_accepted:
            return dict(rc=1, verdict="ASSERT-FAIL", criterion="request_side_effect_or_latency_failure")
        if result.get("rc") == 1:
            failures = [(name, flow) for name, flow in self.snapshots["final"]["client"]["flows"].items()
                        if any(event["kind"] in ("eof", "rst") for event in flow["events"])]
            if failures:
                name, flow = failures[0]
                kinds = [event["kind"] for event in flow["events"]]
                peer.publish(self.root / "legacy-loss.json", dict(original_socket=True, application_redials=0,
                    failure="reset" if "rst" in kinds else "early_eof", samples_before=len(flow["received"])))
        return result

    def cleanup(self):
        if self.phase == "cleaned":
            require(not getattr(self, "cleanup_errors", []), "fixture_cleanup_failed")
            return dict(cleaned=True)
        errors = []
        self.shutdown.set()
        if self.observer is not None:
            self.observer.join(timeout=35)
        # A failed prior still needs its original bytes and live FD census.
        # Capture before shutdown and label it incomplete; cleanup cannot mint
        # a finish declaration or replace a native setup/product verdict.
        if self.phase != "new" or self.jobs:
            try:
                peer.publish(self.root / "cleanup-agent.json", self.kernel())
                self.log_snapshot("cleanup")
            except (SetupError, OSError, ValueError, subprocess.SubprocessError):
                errors.append("agent_or_log_read_failed")
            for node, role in (("ctl1", "client"), ("agt1", "target")):
                try:
                    value = self.remote_call(node, "snapshot")
                    peer.publish(self.root / ("cleanup-"+role+".json"), value)
                    folder = self.root / ("cleanup-"+role)
                    folder.mkdir(exist_ok=True)
                    for name in value["flows"]:
                        safe_label(name)
                        for suffix in ("sent", "received", "json"):
                            self.transport.run("copy-from", node, self.remote+"/"+name+"."+suffix, folder/(name+"."+suffix))
                except (SetupError, OSError, ValueError, subprocess.SubprocessError):
                    errors.append(role+"_capture_failed")
            # origin: hot-update S5 plan §9.1 / DRILL-RUN. Killing docker
            # exec's host client does not kill the remote request collector.
            # End its own schedule and reap it before closing the transport.
            if self.request_job is not None and self.request_job.poll() is None:
                try:
                    self.request_mark("finish")
                    if self.request_job.wait(timeout=20) != 0:
                        errors.append("request_cleanup_failed")
                except (SetupError, OSError, subprocess.SubprocessError):
                    errors.append("request_cleanup_failed")
            peer.publish(self.root / "cleanup-capture.json", dict(status="INCOMPLETE", errors=errors))
        for node in ("ctl1", "agt1"):
            try:
                self.remote_call(node, "cleanup")
            except (SetupError, OSError, ValueError, subprocess.SubprocessError):
                errors.append(node+"_remote_cleanup_failed")
        for task in self.races:
            task.join(timeout=6)
        if self.product_observer_started:
            try:
                self.transport.run("observe-stop",self.args.profile,self.nonce,timeout=60)
                self.product_observer_started=False
            except (SetupError, OSError, ValueError, subprocess.SubprocessError):
                errors.append("product_observer_cleanup_failed")
        try:
            self.transport.close()
        except (SetupError, OSError, ValueError, subprocess.SubprocessError):
            errors.append("transport_cleanup_failed")
        if self.observer is not None and self.observer.is_alive():
            errors.append("cut_observer_did_not_stop")
        if any(task.is_alive() for task in self.races):
            errors.append("race_observer_did_not_stop")
        self.cleanup_errors = errors
        # A cleanup capture never certifies a finished observation, even when
        # all owners close successfully. Keep the legacy INCOMPLETE label.
        peer.publish(self.root / "cleanup-capture.json", dict(status="INCOMPLETE", errors=errors,
                                                             cleanup_complete=not errors))
        self.phase = "cleaned"
        require(not errors, "fixture_cleanup_failed")
        return dict(cleaned=True)

    def command(self, argv):
        require(argv, "empty_command")
        command, arguments = argv[0], argv[1:]
        if command in ("prepare", "saturation", "start", "baseline", "finish", "collect", "continuity", "cleanup"):
            require(not arguments, "invalid_command_arity")
            return getattr(self, command)()
        if command == "carrier-baseline":
            require(not arguments, "invalid_command_arity")
            return self.carrier_baseline()
        if command == 'watchdog-fault':
            require(len(arguments)==1,'invalid_watchdog_fault_arity')
            elapsed=float(arguments[0]);require(0<=elapsed<=2000,'watchdog_nats_fault_over_budget')
            self.watchdog_fault=dict(nats_block_ms=elapsed,mode_restored=True)
            return dict(recorded=True)
        if command == "open":
            require(len(arguments) <= 1 and self.phase == "running", "invalid_open_phase")
            if arguments:
                staged = self.transport.json('exec', 'agt1', 'python3', '-I', '-B', '/tmp/flow_open_fault.py',
                                             'wait-staged', '--gate-url', arguments[0])
                peer.publish(self.root/'open-staged.json', staged)
            return self.remote_call("ctl1", "open", flow=next(row for row in self.specs if row["id"] == "opening"))
        if command in ("checkpoint", "revoke"):
            require(len(arguments) == 1, "invalid_command_arity")
            return getattr(self, command)(arguments[0])
        if command == "wait-event":
            require(len(arguments) == 2, "invalid_command_arity")
            return self.wait_event(*arguments)
        if command == "arm-revoke":
            return self.arm_revoke(arguments)
        raise SetupError("unknown_command")


@contextmanager
def collector_address(root):
    # origin: hot-update S5 plan §9.1 / DRILL-RUN. sockaddr_un is bounded
    # independently of evidence paths. Resolve through an owned directory FD
    # while retaining the private socket inode and its normal permissions.
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        yield "/proc/self/fd/" + str(directory) + "/fixture.sock"
    finally:
        os.close(directory)


def call(root, argv):
    deadline = time.monotonic()+10
    while True:
        with collector_address(root) as address, socket.socket(socket.AF_UNIX) as connection:
            try:
                connection.settimeout(180)
                connection.connect(address)
                connection.sendall(json.dumps(argv).encode()+b"\n")
                data = bytearray()
                while not data.endswith(b"\n"):
                    chunk = connection.recv(65536)
                    require(chunk, "collector_lost")
                    data.extend(chunk)
                return json.loads(data)
            except (FileNotFoundError, ConnectionRefusedError):
                require(time.monotonic() < deadline, "collector_start_deadline")
                time.sleep(.02)


def serve(args):
    fixture = Fixture(args)
    with collector_address(args.root) as address, socket.socket(socket.AF_UNIX) as server:
        server.bind(address)
        os.chmod(args.root / "fixture.sock", 0o600)
        server.listen(8)
        try:
            while not fixture.shutdown.is_set():
                with server.accept()[0] as connection:
                    try:
                        with connection.makefile("rb") as stream:
                            argv = json.loads(stream.readline(1 << 20))
                        result = fixture.command(argv)
                    except SetupError as exc:
                        result = dict(rc=2, verdict="SETUP-RED", criterion=str(exc))
                    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
                        result = dict(rc=2, verdict="SETUP-RED", criterion="collector_observation_failed")
                    connection.sendall(json.dumps(result, sort_keys=True).encode()+b"\n")
        finally:
            fixture.cleanup()
            (args.root / "fixture.sock").unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="mode", required=True)
    commands.add_parser("selftest")
    command = commands.add_parser("serve")
    for flag in ("root", "here"):
        command.add_argument("--"+flag, type=Path, required=True)
    for flag in ("nonce", "profile", "arm", "sim", "instance"):
        command.add_argument("--"+flag, required=True)
    command = commands.add_parser("call")
    command.add_argument("--root", type=Path, required=True)
    command.add_argument("argv", nargs="+")
    args = parser.parse_args()
    try:
        if args.mode == "serve":
            serve(args)
            return 0
        if args.mode == "selftest":
            nonce = "00"*16
            raw = probe.frame(nonce, 0, b"selftest")
            require(probe.parse_frames(raw, nonce)[0]["length"] == 8, "frame_selftest")
            fd = peer.sealed(b"custody")
            try:
                require(probe.sealed_bytes(fd) == b"custody", "sealed_custody_selftest")
            finally:
                os.close(fd)
            # The frozen oracle's independent RFC vectors and adversarial
            # fixtures are the selftest, not an encrypt/decrypt round trip.
            tests = Path(__file__).resolve().parents[2] / "tests/hot-update-flow-probe-test.py"
            control = peer.run_supervised([sys.executable,"-I","-B",str(tests)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            require(control.returncode == 0, "independent_oracle_controls_failed")
            control = peer.run_supervised([sys.executable,"-I","-B",str(Path(__file__).with_name("hot_update_mixed_probe.py")),"selftest"],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            require(control.returncode == 0, "request_oracle_controls_failed")
            result = dict(selftest=True)
        else:
            result = call(args.root, args.argv)
        print(json.dumps(result, sort_keys=True))
        return result.get("rc", 0)
    except (SetupError, OSError, ValueError, KeyError):
        print('{"rc":2,"verdict":"SETUP-RED","criterion":"collector_unavailable"}')
        return 2


if __name__ == "__main__":
    sys.exit(main())
