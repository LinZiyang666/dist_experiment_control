#!/usr/bin/env python3
"""Wrap an isolated sim unit before business and retain observer ownership.

S10's service owner joins both shared supervisors; the product target execs at
its recorded PID. The legacy entry retains the older unit's direct-exec shape.

Only a public cohort identifier and artifact paths appear in unit text. The
custody descriptor and socket pair are minted inside the wrapper, never env
secrets or stored private captures. Historical exporters stay uninstrumented.
"""
import argparse
from pathlib import Path
import re
import pwd
import os
import subprocess
import sys
import array
import importlib.util
import json
import select
import socket
import struct
import time
import signal


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def observation(script):
    spec = importlib.util.spec_from_file_location('private_flow_observation', script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def private_name(node):
    require(node in ('agt1', 'brk1', 'brk2', 'brk3', 'a0', 'a1', 'a2', 'a3'), 'observer runtime')


# origin: hot-update S10 plan §5.3 / §10.2 P11. P16 receives the original
# seqpacket consumer exactly once. No JSON projection can discard S7/P06 fields.
def custody_keeper(root, node, inherited):
    private_name(node)
    path = root / (node + '.sock')
    with socket.socket(fileno=inherited) as endpoint, \
            socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as server:
        server.bind(str(path))
        os.chmod(path, 0o660)
        server.listen(1)
        try:
            with server.accept()[0] as client:
                require(client.recv(32) == b'fullstack-custody-v1', 'custody request')
                rights = array.array('i', [endpoint.fileno()])
                client.sendmsg([b'fullstack-custody-v1'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)])
                endpoint.close()
                # P16 closes the sole consumer before closing this control
                # connection. Keeping it open also keeps the original waiter
                # alive for the service/remote supervision census.
                require(client.recv(1) == b'', 'custody control payload')
        finally:
            path.unlink(missing_ok=True)


def open_custody(root, node):
    """P19/P16 own both returned sockets and close endpoint before control."""
    private_name(node)
    control = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    received = []
    try:
        control.settimeout(5)
        control.connect(str(Path(root) / (node + '.sock')))
        control.sendall(b'fullstack-custody-v1')
        raw, ancillary, flags, _ = control.recvmsg(32, socket.CMSG_SPACE(16 * 4), socket.MSG_CMSG_CLOEXEC)
        unexpected = False
        # Parse all rights before rejecting any ancillary item. Even a later
        # rights item owns installed descriptors when an earlier item is bad.
        # origin: S10 internal review rv3-assembly F7
        for level, kind, data in ancillary:
            if (level, kind) == (socket.SOL_SOCKET, socket.SCM_RIGHTS):
                fds = array.array('i')
                fds.frombytes(data[:len(data) - len(data) % fds.itemsize])
                received.extend(fds)
            else:
                unexpected = True
        require(not unexpected, 'custody ancillary')
        require(not flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC) and raw == b'fullstack-custody-v1'
                and len(received) == 1, 'custody descriptor receipt')
        # Keep the raw FD owned until socket construction succeeds: ENOTSOCK
        # otherwise removes the only reference from the rejection cleanup.
        endpoint = socket.socket(fileno=received[-1])
        received.pop()
        try:
            require(endpoint.family == socket.AF_UNIX and endpoint.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE)
                    == socket.SOCK_SEQPACKET, 'custody socket type')
            endpoint.getpeername()
            control.settimeout(None)
            return endpoint, control
        except BaseException:
            endpoint.close()
            raise
    except BaseException:
        for fd in received:
            os.close(fd)
        control.close()
        raise


def legacy_launch(root, node, script, supervisor, command):
    private_name(node)
    require(command and Path(command[0]).is_absolute() and os.access(command[0], os.X_OK), 'observer product artifact')
    require(supervisor.is_absolute() and os.access(supervisor, os.X_OK), 'shared supervisor required')
    codec = observation(script)
    producer, consumer = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    config = codec.sealed(b'TDCFG1\0\0' + struct.pack('<Q', producer.fileno()) + os.urandom(16))
    child = None
    reader, writer = os.pipe()
    try:
        # The consumer is stdin, which the P05 launcher already forwards. No
        # private FD number is guessed across its shared subreaper boundary.
        child = subprocess.Popen([str(supervisor), '--ready-fd', str(writer),
                                  '--receipt', str(root / (node + '.supervisor.rc')), '--',
                                  sys.executable, '-I', '-B', str(Path(__file__).resolve()),
                                  'legacy-keeper', '--root', str(root),
                                  '--node', node, '--script', str(script)],
                                 stdin=consumer, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                 pass_fds=(writer,))
        consumer.close()
        os.close(writer)
        writer = -1
        require(select.select([reader], [], [], 5)[0], 'observer supervisor readiness timeout')
        with os.fdopen(reader, 'rb') as ready:
            reader = -1
            receipt = json.loads(ready.readline())
        require(receipt['target']['pid'] > 1 and receipt['target']['start_ticks'] > 0, 'observer supervisor census')
        (root / (node + '.supervisor.json')).write_text(json.dumps(receipt, sort_keys=True))
        deadline = time.monotonic() + 5
        while not (root / (node + '.sock')).exists():
            require(child.poll() is None and time.monotonic() < deadline, 'observer keeper readiness failed')
            time.sleep(0.01)  # Only waits for bind; never infers product readiness.
        env = {k: v for k, v in os.environ.items()
               if k not in ('TETHER_TEST_DRILL_OBSERVER', 'TETHER_TEST_DRILL_CUSTODY_FD')}
        env.update(TETHER_TEST_DRILL_OBSERVER='v1', TETHER_TEST_DRILL_CUSTODY_FD=str(config))
        producer.set_inheritable(True)
        os.set_inheritable(config, True)
        # The unit remains the original product PID. Its supervised keeper has
        # a pidfd deathwatch on this PID; P25 must Join/census it at final Stop.
        os.execve(command[0], command, env)
    finally:
        producer.close()
        consumer.close()
        os.close(config)
        for fd in (reader, writer):
            if fd >= 0:
                os.close(fd)
        if child is not None:
            if child.poll() is None:
                child.terminate()
            code = child.wait(timeout=15)
            if code != 0:
                raise subprocess.CalledProcessError(code, ['shared-supervisor'])


def exec_product(script, command):
    """Only the supervised target execs; its parent retains every waiter."""
    require(command and Path(command[0]).is_absolute(), 'observer product command')
    codec = observation(script)
    channel = os.dup(0)
    os.set_inheritable(channel, True)
    config = codec.sealed(b'TDCFG1\0\0' + struct.pack('<Q', channel) + os.urandom(16))
    os.set_inheritable(config, True)
    # The inherited transport is no longer ordinary product stdin. Retain it
    # only at its explicit private FD, never accidentally in a workload child.
    with open(os.devnull, 'rb') as empty:
        os.dup2(empty.fileno(), 0)
    env = {k: v for k, v in os.environ.items()
           if k not in ('TETHER_TEST_DRILL_OBSERVER', 'TETHER_TEST_DRILL_CUSTODY_FD')}
    env.update(TETHER_TEST_DRILL_OBSERVER='v1', TETHER_TEST_DRILL_CUSTODY_FD=str(config))
    try:
        os.execve(command[0], command, env)
    finally:
        os.close(config)
        os.close(channel)


def supervised_child(supervisor, root, label, argv, stdin, children):
    reader, writer = os.pipe()
    try:
        child = subprocess.Popen([str(supervisor), '--ready-fd', str(writer),
                                  '--receipt', str(root / (label + '.rc')), '--', *argv],
                                 stdin=stdin, pass_fds=(writer,))
        children.append(child)  # Register before any fallible readiness read.
        os.close(writer)
        writer = -1
        require(select.select([reader], [], [], 5)[0], 'observer supervisor readiness timeout')
        with os.fdopen(reader, 'rb') as ready:
            reader = -1
            receipt = json.loads(ready.readline())
        require(receipt['target']['pid'] > 1 and receipt['target']['start_ticks'] > 0, 'observer supervisor census')
        with (root / (label + '.json')).open('x') as out:
            json.dump(receipt, out, sort_keys=True)
        return child
    finally:
        for fd in (reader, writer):
            if fd >= 0:
                os.close(fd)


# origin: hot-update S10 plan §6 / §10.2 P11. A same-PID exec cannot carry a
# Python Popen waiter into Go. Keep the service owner alive and supervise both
# product and keeper independently; readiness records the real product PID.
def launch(root, node, script, supervisor, command):
    private_name(node)
    require(command and Path(command[0]).is_absolute() and os.access(command[0], os.X_OK), 'observer product artifact')
    require(supervisor.is_absolute() and os.access(supervisor, os.X_OK), 'shared supervisor required')
    producer, consumer = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    children, stopping = [], False
    def stop(_number, _frame):
        nonlocal stopping
        stopping = True
    previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        args = [sys.executable, '-I', '-B', str(Path(__file__).resolve())]
        keeper = supervised_child(supervisor, root, node + '.supervisor',
                                  args + ['keeper', '--root', str(root), '--node', node, '--script', str(script)],
                                  consumer, children)
        consumer.close()
        deadline = time.monotonic() + 5
        while not (root / (node + '.sock')).exists():
            require(not stopping and keeper.poll() is None and time.monotonic() < deadline, 'observer keeper readiness failed')
            time.sleep(0.01)
        product = supervised_child(supervisor, root, node + '.product',
                                   args + ['exec-product', '--script', str(script), '--', *command], producer, children)
        producer.close()
        service = dict(pid=os.getpid(), start_ticks=int(Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19]))
        with (root / (node + '.service.json')).open('x') as out:
            json.dump(service, out, sort_keys=True)
        while product.poll() is None and not stopping:
            # Observation failure invalidates its receipt, not the original
            # business lifetime. P19 sees the native keeper rc immediately;
            # retain it for Join without killing a still-serving product.
            keeper.poll()
            time.sleep(0.02)
    finally:
        producer.close()
        consumer.close()
        # Always join both original waiters, including partial startup. A
        # collector error remains nonzero even after emergency reaping succeeds.
        try:
            join_supervised_children(children)
        finally:
            # Handler restoration has its own finally: even a failed waiter
            # cannot retain the service owner's shutdown handlers.
            for sig, handler in previous.items():
                signal.signal(sig, handler)


# origin: S10 internal review rv3-assembly F5
def join_supervised_children(children):
    codes = []
    for child in reversed(children):
        try:
            if child.poll() is None:
                child.terminate()
            codes.append(child.wait(timeout=15))
        except BaseException:
            # Preserve the cleanup failure even when emergency reaping works.
            # Wake a stopped supervisor so its shared subreaper can finish;
            # SIGKILL is only the final bounded fallback, never a green receipt.
            codes.append(125)
            try:
                child.send_signal(signal.SIGCONT)
                child.terminate()
                codes.append(child.wait(timeout=15))
            except BaseException:
                try:
                    child.kill()
                    codes.append(child.wait(timeout=15))
                except BaseException:
                    codes.append(125)
    if any(code != 0 for code in codes):
        code = 125 if 125 in codes else next(code for code in codes if code != 0)
        raise subprocess.CalledProcessError(code, ['shared-supervisor'], output=tuple(codes))



def runtime_main(argv):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('operation', choices=('launch', 'legacy-launch', 'keeper', 'legacy-keeper', 'exec-product'))
    p.add_argument('--root', type=Path)
    p.add_argument('--node')
    p.add_argument('--script', required=True, type=Path)
    p.add_argument('--supervisor', type=Path)
    split = argv.index('--') if '--' in argv else len(argv)
    args = p.parse_args(argv[:split])
    if args.operation == 'exec-product':
        exec_product(args.script, argv[split + 1:])
    elif args.operation == 'keeper':
        custody_keeper(args.root, args.node, 0)
    elif args.operation == 'legacy-keeper':
        observation(args.script).keeper(args.root, args.node, 0)
    elif args.operation == 'legacy-launch':
        legacy_launch(args.root, args.node, args.script, args.supervisor, argv[split + 1:])
    else:
        launch(args.root, args.node, args.script, args.supervisor, argv[split + 1:])
    return 0


def main():
    if sys.argv[1:2] in (['launch'], ['legacy-launch'], ['keeper'], ['legacy-keeper'], ['exec-product']):
        return runtime_main(sys.argv[1:])
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--node',required=True,choices=('agt1','brk1','brk2','brk3'))
    p.add_argument('--nonce',required=True)
    p.add_argument('--script',required=True,type=Path)
    p.add_argument('--supervisor',type=Path,default=Path('/usr/local/bin/tether-test-supervise'))
    p.add_argument('--ready-file',type=Path)
    p.add_argument('--initial-only',action='store_true')
    a=p.parse_args()
    if not re.fullmatch('[0-9a-f]{32}',a.nonce) or not a.script.is_absolute():
        return 2
    # origin: hot-update S10 plan §2.1 / §10.2 P11. An old flow fixture must
    # not restart a unit after S10 has handed that unit to the product driver.
    if a.ready_file is not None and os.path.lexists(a.ready_file):
        return 2
    unit='tether-agent.service' if a.node=='agt1' else 'tether-broker.service'
    # Read the actual unit, including legacy supply's earlier drop-in. There
    # must be exactly one effective ExecStart after reset lines are applied.
    text=subprocess.check_output([str(a.supervisor),'--','/usr/bin/systemctl','cat',unit],text=True)
    command=None
    for line in text.splitlines():
        if line.startswith('ExecStart='):
            command=line[len('ExecStart='):] or None
    if not command or not command.startswith('/') or 'flow_observation.py' in command:
        return 2
    root=Path('/run/tether-drill-custody')/a.nonce
    root.mkdir(mode=0o700,parents=True,exist_ok=True)
    # Agent/ctl use sim; broker retains tether. The isolated custody directory
    # uses the operator group and setgid so broker-created socket inodes are
    # readable only by those operators. Resolve IDs, never assume uid1000.
    operator=pwd.getpwnam('sim')
    os.chown(root,operator.pw_uid,operator.pw_gid)
    root.chmod(0o2770)
    dropin=Path('/etc/systemd/system')/(unit+'.d')/'zz-flow-observer.conf'
    dropin.parent.mkdir(parents=True,exist_ok=True)
    mode = ' launch' if a.initial_only else ' legacy-launch'
    dropin.write_text('[Service]\nSupplementaryGroups=sim\nExecStart=\nExecStart=/usr/bin/python3 -I -B '+str(Path(__file__).resolve())+
        mode+' --script '+str(a.script)+' --supervisor '+str(a.supervisor)+
        ' --root '+str(root)+' --node '+a.node+' -- '+command+'\n')
    subprocess.run([str(a.supervisor),'--','/usr/bin/systemctl','daemon-reload'],check=True)
    if not a.initial_only:
        subprocess.run([str(a.supervisor),'--','/usr/bin/systemctl','restart',unit],check=True)
    return 0

if __name__=='__main__':
    try:
        sys.exit(main())
    except subprocess.CalledProcessError as error:
        sys.exit(error.returncode if error.returncode > 0 else 128 - error.returncode)
    except (OSError,ValueError,KeyError,TypeError,subprocess.SubprocessError):
        sys.exit(2)
