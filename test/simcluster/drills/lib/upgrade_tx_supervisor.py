#!/usr/bin/env python3
"""External fixture subreaper; every Python fixture command uses this owner.

The runner keeps the sole lifeline writer. EOF, a deadline or normal stop kills
and reaps the complete adopted tree, including double-fork/setsid descendants.
The UNIX control socket never replaces the lifeline. Linux is implemented here;
other hosts require P21's native backend rather than pretending group kill proves
an empty tree. Protocol version 1 is also the integration port for that backend.
"""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time

VERSION = 1
MAX_BYTES = 16 * 1024 * 1024
_owners = {}


class SupervisionError(RuntimeError):
    pass


def require(value, reason):
    if not value:
        raise SupervisionError(reason)


def identity(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return dict(pid=pid, start_ticks=int(fields[19]),
                    boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip())
    except FileNotFoundError:
        return None


def same_process(row):
    return identity(row["pid"]) == row


def pidfd_open(pid):
    if hasattr(os, "pidfd_open"):
        return os.pidfd_open(pid)
    # Some Python builds omit os.pidfd_open while the host libc/kernel support
    # it. Keep incarnation-safe signals without depending on the Python build.
    libc = ctypes.CDLL(None, use_errno=True)
    require(hasattr(libc, "pidfd_open"), "native_pidfd_backend_required")
    fd = libc.pidfd_open(pid, 0)
    if fd < 0:
        number = ctypes.get_errno()
        raise OSError(number, os.strerror(number))
    return fd


def children():
    # origin: hot-update S8 plan §9.1 / M21. Subreaper adoption survives setsid
    # and intermediate-parent death; argv/cwd/UID scans cannot prove lineage.
    result = set()
    for task in Path("/proc/self/task").iterdir():
        try:
            result.update(map(int, (task / "children").read_text().split()))
        except FileNotFoundError:
            continue
    return result


class InheritedWaits:
    """Wait only the shell's initial children, before any command owns status."""

    # origin: S10 internal review FIX-SUPERVISE F1
    # POSIX exec can transfer a pipe-backed here-document writer to this
    # adapter. Its status belongs here, not to the outer launcher's exit census.
    # Snapshot before spawning; waiting on any later Popen would steal its sole
    # native status and could erase a genuine escaped-descendant failure.
    def __init__(self):
        self.pending = {pid: identity(pid) for pid in children()}
        self.stop = threading.Event()
        self.fault = None
        self.census = []
        self.worker = threading.Thread(target=self.maintain)
        self.worker.start()

    def reap(self):
        for pid, row in list(self.pending.items()):
            if row is None or not same_process(row):
                del self.pending[pid]
                continue
            try:
                waited, _ = os.waitpid(pid, os.WNOHANG)
                if waited:
                    del self.pending[pid]
            except ChildProcessError:
                del self.pending[pid]

    def maintain(self):
        try:
            while not self.stop.is_set():
                self.reap()
                self.stop.wait(.01)
        except OSError as error:
            self.fault = error

    def join(self):
        self.stop.set()
        self.worker.join()
        self.reap()
        if self.fault is not None:
            raise self.fault
        if self.pending:
            # The command's own supervisor is already joined. Every remaining
            # child is inherited: clean it, retaining native125 for live duty.
            for row in self.pending.values():
                if row is not None and same_process(row):
                    fields = Path(f"/proc/{row['pid']}/stat").read_text().rsplit(")", 1)[1].split()
                    self.census.append(dict(row, state=fields[0]))
            reap_tree()
            return False
        return True


def reap_tree():
    killed = []
    deadline = time.monotonic() + 10
    while True:
        pids = children()
        if not pids:
            return dict(version=VERSION, empty=True, remaining=[], killed=killed)
        require(time.monotonic() < deadline, "supervisor_tree_not_empty")
        for pid in pids:
            try:
                waited, _ = os.waitpid(pid, os.WNOHANG)
                if waited:
                    continue
                row = identity(pid)
                if row is not None and same_process(row):
                    # pidfd binds the signal to this incarnation, even if exit
                    # and PID reuse race the census-to-signal interval.
                    fd = pidfd_open(pid)
                    try:
                        if same_process(row):
                            signal.pidfd_send_signal(fd, signal.SIGKILL)
                            if row not in killed:
                                killed.append(row)
                    finally:
                        os.close(fd)
            except (ProcessLookupError, ChildProcessError):
                continue
        time.sleep(0.01)  # Bounded kill/reap poll, never a readiness barrier.


def write_json(path, value):
    path = Path(path)
    fd, name = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(name, 0o600)
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def receive(sock):
    raw = bytearray()
    while not raw.endswith(b"\n"):
        chunk = sock.recv(65536)
        require(chunk, "supervisor_short_frame")
        raw.extend(chunk)
        require(len(raw) <= MAX_BYTES, "supervisor_frame_overflow")
    return json.loads(raw)


def call(root, request):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(60)
        client.connect(str(Path(root) / "supervisor.sock"))
        client.sendall(json.dumps(dict(version=VERSION, **request)).encode() + b"\n")
        result = receive(client)
    require(result.get("ok") is True, result.get("error", "supervisor_refused"))
    return result["result"]


def check_spec(spec, paths=True):
    require(isinstance(spec, dict) and set(spec) <= {"argv", "cwd", "timeout_s"}, "invalid_command_spec")
    argv = spec.get("argv")
    require(isinstance(argv, list) and argv and all(isinstance(x, str) and x for x in argv), "invalid_argv")
    require(Path(argv[0]).is_absolute() and (not paths or Path(argv[0]).is_file()), "executable_path_required")
    require(type(spec.get("timeout_s")) in (int, float) and 0 < spec["timeout_s"] <= 55,
            "invalid_command_timeout")
    require(Path(spec.get("cwd", "")).is_absolute(), "command_cwd_required")
    # Production state cannot be selected by an inherited HOME. Product actors
    # still receive explicit sandbox config paths through their declared argv.
    forbidden = ("/.local/bin/tether", "/.tether")
    require(not any(part in arg for part in forbidden for arg in argv), "production_path_forbidden")
    return argv


def spawn(spec, root, stdin=subprocess.DEVNULL, capture=False):
    argv = check_spec(spec)
    env = dict(os.environ)
    for key in list(env):
        if key.startswith(("_TETHER_", "TETHER_")):
            del env[key]
    env["HOME"] = str(Path(root) / "home")
    env["XDG_STATE_HOME"] = str(Path(root) / "state")
    output = subprocess.PIPE if capture else subprocess.DEVNULL
    return subprocess.Popen(argv, cwd=spec["cwd"], env=env, stdin=stdin,
                            stdout=output, stderr=subprocess.DEVNULL, start_new_session=True)


def execute(spec, root, lifeline, data, stdin_text=None):
    # origin: hot-update S8 plan §9.1 / L21. Short observers and injectors are
    # supervised too; their grandchildren can otherwise outlive a CLI timeout.
    with tempfile.TemporaryFile() as input_file:
        raw_input = stdin_text.encode() if stdin_text is not None else json.dumps(data).encode() + b"\n"
        require(len(raw_input) <= MAX_BYTES, "command_input_overflow")
        input_file.write(raw_input)
        input_file.seek(0)
        child = spawn(spec, root, stdin=input_file, capture=True)
        row = identity(child.pid)
        raw = bytearray()
        deadline = time.monotonic() + spec["timeout_s"]
        os.set_blocking(child.stdout.fileno(), False)
        try:
            while True:
                ready, _, _ = select.select([lifeline, child.stdout], [], [], 0.05)
                if lifeline in ready:
                    require(os.read(lifeline, 1), "runner_lifeline_closed")
                if child.stdout in ready:
                    chunk = os.read(child.stdout.fileno(), 65536)
                    raw.extend(chunk)
                    require(len(raw) <= MAX_BYTES, "command_output_overflow")
                    if not chunk and child.poll() is not None:
                        break
                require(time.monotonic() < deadline, "command_timeout")
            return dict(native_rc=child.wait(), stdout=raw.decode(), owner=row,
                        executable_sha=hashlib.sha256(Path(spec["argv"][0]).read_bytes()).hexdigest())
        finally:
            child.stdout.close()
            if child.poll() is None:
                child.kill()
            child.wait()


def serve(root, lifeline, ready_fd, deadline_s):
    require(sys.platform == "linux", "native_supervisor_backend_required")
    libc = ctypes.CDLL(None, use_errno=True)
    require(libc.prctl(36, 1, 0, 0, 0) == 0, "subreaper_setup_failed")
    root = Path(root)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    for name in ("home", "state"):
        (root / name).mkdir(mode=0o700, exist_ok=True)
    actors = {}
    socket_path = root / "supervisor.sock"
    owner = identity(os.getpid())
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    termination = False
    def stop_signal(_number, _frame):
        nonlocal termination
        termination = True
    signal.signal(signal.SIGTERM, stop_signal)
    signal.signal(signal.SIGINT, stop_signal)
    try:
        server.bind(str(socket_path))
        os.chmod(socket_path, 0o600)
        server.listen(8)
        write_json(root / "supervisor.json", dict(version=VERSION, owner=owner))
        os.write(ready_fd, b"R")
        os.close(ready_fd)
        deadline = time.monotonic() + deadline_s
        while not termination and time.monotonic() < deadline:
            ready, _, _ = select.select([lifeline, server], [], [], 0.05)
            if lifeline in ready and not os.read(lifeline, 1):
                break
            if server not in ready:
                continue
            with server.accept()[0] as client:
                client.settimeout(5)
                try:
                    request = receive(client)
                    require(request.get("version") == VERSION, "supervisor_version")
                    op = request.get("op")
                    if op == "spawn":
                        label = request["id"]
                        require(label not in actors, "actor_already_started")
                        child = spawn(request["spec"], root)
                        actors[label] = (child, identity(child.pid))
                        result = actors[label][1]
                    elif op == "run":
                        result = execute(request["spec"], root, lifeline, request["input"], request.get("stdin_text"))
                    elif op == "census":
                        result = {label: dict(owner=row, alive=same_process(row), native_rc=child.poll())
                                  for label, (child, row) in actors.items()}
                    elif op == "stop":
                        termination, result = True, dict(stopping=True)
                    else:
                        raise SupervisionError("unknown_supervisor_operation")
                    reply = dict(ok=True, result=result)
                except (SupervisionError, OSError, ValueError, KeyError, TypeError):
                    reply = dict(ok=False, error="supervisor_operation_failed")
                    # EOF during a command must propagate to whole-tree cleanup.
                    if select.select([lifeline], [], [], 0)[0]:
                        termination = True
                try:
                    client.sendall(json.dumps(reply).encode() + b"\n")
                except OSError:
                    pass
    finally:
        server.close()
        receipt = reap_tree()
        receipt["owner"] = owner
        receipt["actors"] = [row for _child, row in actors.values()]
        write_json(root / "supervisor-empty.json", receipt)
        if socket_path.exists():
            socket_path.unlink()


def start(root, lifeline_fd, deadline_s=600):
    require(0 < deadline_s <= 14400, "invalid_supervision_deadline")
    require(sys.platform == "linux", "native_supervisor_backend_required")
    require(not (Path(root) / "supervisor.json").exists(), "supervisor_evidence_already_used")
    # The runner provides a pipe/FIFO whose only writer it retains. Copying a
    # writer into an actor would mask runner death, so only this reader crosses.
    require(stat.S_ISFIFO(os.fstat(lifeline_fd).st_mode), "lifeline_pipe_required")
    import fcntl
    require(fcntl.fcntl(lifeline_fd, fcntl.F_GETFL) & os.O_ACCMODE == os.O_RDONLY,
            "lifeline_read_end_required")
    reader, writer = os.pipe()
    with (Path(root) / "supervisor.log").open("xb") as log:
        child = subprocess.Popen([sys.executable, "-I", "-B", __file__, "serve", "--root", str(root),
                                  "--lifeline-fd", str(lifeline_fd), "--ready-fd", str(writer),
                                  "--deadline-s", str(deadline_s)],
                                 pass_fds=(lifeline_fd, writer), stdin=subprocess.DEVNULL,
                                 stdout=log, stderr=log, start_new_session=True)
    os.close(writer)
    try:
        require(select.select([reader], [], [], 5)[0] and os.read(reader, 1) == b"R", "supervisor_not_ready")
    except BaseException:
        if child.poll() is None:
            child.terminate()
        child.wait(timeout=12)
        raise
    finally:
        os.close(reader)
    _owners[str(Path(root))] = child
    return child


def wait_empty(root, timeout_s=12):
    path = Path(root) / "supervisor-empty.json"
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            result = json.loads(path.read_bytes())
            require(result.get("empty") is True and result.get("remaining") == [], "supervisor_postcondition_failed")
            require(result["owner"] == json.loads((Path(root) / "supervisor.json").read_bytes())["owner"],
                    "supervisor_receipt_identity_changed")
            require(all(not same_process(row) for row in result["killed"] + result["actors"]),
                    "supervisor_surviving_identity")
            owner = _owners.pop(str(Path(root)), None)
            if owner is not None:
                require(owner.wait(timeout=timeout_s) == 0, "supervisor_exit_failed")
            elif same_process(result["owner"]):
                fd = pidfd_open(result["owner"]["pid"])
                try:
                    require(select.select([fd], [], [], timeout_s)[0], "supervisor_did_not_exit")
                finally:
                    os.close(fd)
            return result
        time.sleep(0.01)  # Receipt poll after kill/reap; never creates success.
    raise SupervisionError("supervisor_empty_receipt_missing")


def run(argv, timeout_s=55, data=None, stdin_text=None):
    """Bounded one-shot command using the same external ownership protocol."""
    with tempfile.TemporaryDirectory(prefix="s8-supervise-") as temporary:
        root = Path(temporary)
        reader, writer = os.pipe()
        child = start(root, reader, timeout_s + 15)
        os.close(reader)
        try:
            return call(root, dict(op="run", spec=dict(argv=argv, cwd=str(Path.cwd()), timeout_s=timeout_s),
                                   input=data, stdin_text=stdin_text))
        finally:
            os.close(writer)
            wait_empty(root)
            child.wait(timeout=12)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("serve", "run"))
    parser.add_argument("--root", type=Path)
    parser.add_argument("--lifeline-fd", type=int)
    parser.add_argument("--ready-fd", type=int)
    parser.add_argument("--deadline-s", type=float, default=600)
    parser.add_argument("--timeout-s", type=float, default=55)
    args, remainder = parser.parse_known_args()
    if args.command == "serve":
        serve(args.root, args.lifeline_fd, args.ready_fd, args.deadline_s)
        return 0
    argv = remainder[1:] if remainder[:1] == ["--"] else remainder
    inherited = InheritedWaits()
    try:
        stdin_text = sys.stdin.read(MAX_BYTES + 1)
        require(len(stdin_text.encode()) <= MAX_BYTES, "command_input_overflow")
        result = run(argv, args.timeout_s, stdin_text=stdin_text)
    finally:
        inherited_empty = inherited.join()
    sys.stdout.write(result["stdout"])
    if not inherited_empty:
        print("supervisor: inherited live descendants required cleanup", file=sys.stderr)
        for row in inherited.census:
            print(f"inherited descendant pid={row['pid']} start_ticks={row['start_ticks']} state={row['state']}",
                  file=sys.stderr)
        return 125
    return result["native_rc"]


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SupervisionError as error:
        print(json.dumps(dict(rc=2, verdict="SETUP-RED", reason=str(error))))
        sys.exit(2)
    except (OSError, ValueError, KeyError, TypeError):
        print('{"rc":2,"verdict":"SETUP-RED","reason":"supervision_failed"}')
        sys.exit(2)
