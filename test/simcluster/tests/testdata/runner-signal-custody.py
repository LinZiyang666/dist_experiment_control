"""Bounded scratch-runner cancellation through the shared process custodian."""
import importlib.util
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


# origin: S10 internal review RUNALL-A F1 / hot-update S10 plan §6; lead L15.
def exercise_runner_signal(simroot, root):
    spec = importlib.util.spec_from_file_location(
        "flow_peer", simroot / "drills/lib/flow_fixture_peer.py")
    peer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(peer)

    # Install owner cancellation before starting the scratch tree. The shared
    # helper owns the subreaper, pidfd census and every descendant wait duty.
    def cancelled(signum, frame):
        raise InterruptedError("scratch owner cancelled")

    signal.signal(signal.SIGTERM, cancelled)
    signal.signal(signal.SIGINT, cancelled)
    child = peer.start_supervised(
        ["bash", str(root / "run-drills.sh"), "--skip-preflight", "--no-retry",
         "--no-attribute", "--logdir", str(root / "log"), "00-slow"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    fd = -1
    try:
        fd = os.pidfd_open(child.target_pid)
        time.sleep(1)  # Preserve the original early-TERM counterexample window.
        signal.pidfd_send_signal(fd, signal.SIGTERM)
        # Signal the pinned runner directly: its foreground fsync canary may
        # be in D state. Keep its original waiter until the kernel returns;
        # a deadline is a failure, never evidence that killing reaped it.
        stdout, stderr = child.communicate(timeout=60)
        assert child.returncode == 143, (child.returncode, stderr.decode())
    finally:
        if fd >= 0:
            os.close(fd)
        if child.poll() is None:
            child.terminate()
        child.communicate(timeout=15)


if __name__ == "__main__":
    # Match the process-probe entry's Linux interpreter selection. Some local
    # Python builds omit pidfds despite a capable kernel; never skip cleanup.
    if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        if Path(sys.executable).resolve() == Path("/usr/bin/python3").resolve():
            raise RuntimeError("scratch runner requires Linux Python with pidfd support")
        os.execv("/usr/bin/python3", ["/usr/bin/python3", "-I", "-B", __file__, *sys.argv[1:]])
    exercise_runner_signal(Path(sys.argv[1]), Path(sys.argv[2]))
