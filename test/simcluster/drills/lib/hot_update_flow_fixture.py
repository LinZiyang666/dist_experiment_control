#!/usr/bin/env python3
"""Callable live flow fixture, sharing the existing S5 peer and byte oracles.

serve/call preserve explicit parent PID custody. Source the shell wrapper for
the drill-75 verbs; prepare starts a service in the main shell and cleanup
waits that exact PID. Environment verbs address an already started service.
No fixture command issues a product reload, replays bytes or replaces sockets.
"""
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def implementation():
    # origin: hot-update S6 plan §10.1. One original-socket oracle also serves 75.
    path = Path(__file__).with_name("flow_fixture.py")
    if not path.is_file():
        raise ValueError("missing_live_fixture")
    return load("flow_fixture")


# origin: hot-update S6 plan §10.1. Extend lifecycle seams, not the S5 private
# codec/oracle. A broker cut leaves the agent image unchanged; its checkpoint
# must not demand the agent-upgrade marker used by 75.
def broker_fixture_class(module):
    class BrokerFixture(module.Fixture):
        def prepare(self):
            result = super().prepare()
            for row in self.specs:
                if row["id"] == "tcp-1":
                    row["class"] = "idle"
                    for direction in ("up", "down"):
                        row[direction] = module.peer.declaration(row["id"] + "." + direction, self.nonce, 0, row["size"])
                if row["id"] == "tcp-2":
                    row["class"] = "slow"
            self.remote_call("agt1", "configure", flows=self.specs)
            public = [{key: row[key] for key in ("id", "protocol", "class", "declared_at", "up", "down")} for row in self.specs]
            module.peer.publish(self.root / "declarations.json", dict(nonce=self.nonce, flows=public))
            return result

        def baseline(self):
            module.require(self.phase == "running", "baseline_before_start")
            deadline = time.monotonic() + 20
            while True:
                documents = [self.remote_call(node, "snapshot") for node in ("ctl1", "agt1")]
                complete = True
                for spec in self.specs:
                    if spec["id"] == "opening":
                        continue
                    for index, document in enumerate(documents):
                        flow = document["flows"].get(spec["id"])
                        complete = complete and flow is not None and (
                            not flow["sent"] and not flow["received"] and not flow["events"] if spec["class"] == "idle"
                            else len(flow["sent"]) >= 20 and len(flow["received"]) >= 20)
                        if spec["class"] == "half-close" and index == 1 and flow is not None:
                            complete = complete and any(event["kind"] == "eof" for event in flow["events"])
                if complete:
                    for node in ("ctl1", "agt1"):
                        self.remote_call(node, "silent")
                    self.request_mark("cohort")
                    return self.checkpoint("baseline")
                module.require(time.monotonic() < deadline, "baseline_duplex_samples_missing")
                time.sleep(.02)

        def start_observer(self):
            # The broker phase watcher owns cut marks for this profile. An
            # agent image-cut record would never occur on a broker-only roll.
            return None

        def checkpoint(self, label):
            label = module.safe_label(label)
            delegated = "broker-" + label if label.startswith("hop-") or label == "current-rollback" else label
            result = super().checkpoint(delegated)
            receipt = self.snapshots[delegated]
            if delegated != label:
                self.snapshots[label] = receipt
                module.peer.publish(self.root / ("checkpoint-" + label + ".json"), receipt)
            broker = self.transport.json("exec", os.environ.get("FLOW_BROKER_NODE", "brk1"), "python3", "-I", "-B",
                                         "/tmp/flow_fixture_peer.py", "kernel", "--root", "/tmp", "--unit", "tether-broker")
            module.peer.publish(self.root / ("broker-" + label + ".json"), broker)
            if label == "before":
                self.broker_before = broker
            else:
                before = self.snapshots["before"]["agent"]
                module.require(receipt["agent"]["identity"] == before["identity"]
                               and receipt["agent"]["running_sha"] == before["running_sha"], "broker_only_changed_agent")
                module.require(receipt["sql"] == self.snapshots["before"]["sql"], "broker_cut_rehomed_allocation")
                module.require(broker["identity"] == self.broker_before["identity"], "broker_pid_changed")
            result["checkpoint"] = label
            return result

        def command(self, argv):
            if argv == ["broker-mark"]:
                module.require(self.phase == "running", "broker_mark_before_start")
                return dict(marked=True, observers=[self.remote_call(node, "mark") for node in ("ctl1", "agt1")])
            return super().command(argv)

    return BrokerFixture


def peer_main(argv):
    module = load("flow_fixture_peer")
    original_flow = module.Flow

    class IdleFlow(original_flow):
        def run(self):
            super().run()
            if self.spec["class"] == "idle":
                # Zero predeclared records are already complete. Keep the
                # receive worker and original socket live through final census.
                self.end = self.start
                self.done.set()

    module.Flow = IdleFlow
    previous = sys.argv
    try:
        sys.argv = [sys.argv[0]] + argv
        return module.main()
    finally:
        sys.argv = previous


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "peer":
        return peer_main(argv[1:])
    module = implementation()
    if not argv:
        raise ValueError("missing_fixture_command")
    if argv[0] in ("selftest", "serve", "call"):
        # The service is this process, so the caller can wait its exact PID.
        previous = sys.argv
        previous_handler = signal.getsignal(signal.SIGTERM)
        try:
            if argv[0] == "serve":
                if "--profile" in argv and argv[argv.index("--profile") + 1] == "broker":
                    module.PROFILES += ("broker",)
                    module.Fixture = broker_fixture_class(module)
                    original_transport = module.Transport

                    class BrokerTransport(original_transport):
                        def start(self, stdout, stderr, *arguments):
                            values = list(arguments)
                            if "serve" in values and any(str(value).endswith("/flow_fixture_peer.py") for value in values):
                                node = values[1]
                                index = next(i for i, value in enumerate(values) if str(value).endswith("/flow_fixture_peer.py"))
                                destination = str(Path(values[index]).with_name("hot_update_flow_fixture.py"))
                                self.run("copy-to", Path(__file__).resolve(), node, destination)
                                values[index:index + 1] = [destination, "peer"]
                            return super().start(stdout, stderr, *values)

                    module.Transport = BrokerTransport
                def stop(_number, _frame):
                    raise SystemExit(2)
                signal.signal(signal.SIGTERM, stop)
            sys.argv = [previous[0]] + argv
            return module.main()
        finally:
            sys.argv = previous
            signal.signal(signal.SIGTERM, previous_handler)
    # Environment verbs address an explicitly started service. Starting it in
    # a short-lived CLI would orphan PID custody; the sourced shell owns that.
    root = Path(os.environ["FLOW_EV"]).resolve()
    if argv[0] == "prepare":
        if len(argv) != 2 or argv[1] not in module.PROFILES + ("broker",):
            raise ValueError("invalid_fixture_prepare")
        argv = ["prepare"]
    result = module.call(root, argv)
    print(json.dumps(result, sort_keys=True))
    return result.get("rc", 0)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
        # Never expose a subscription body or private descriptor through errors.
        print('{"rc":2,"verdict":"SETUP-RED","criterion":"fixture_unavailable"}')
        sys.exit(2)
