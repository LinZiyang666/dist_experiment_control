"""Owned Bash startup and pidfd cleanup controls; never execute tether."""
import json
import signal
import subprocess
import tempfile
from pathlib import Path


# origin: hot-update S3 plan §8.2. These are sensor controls with owned local
# children. They do not model the product's same-PID exec implementation.
def agent_cleanup_controls(SENSOR, NONCE, wait_for):
    import copy
    import types
    with tempfile.TemporaryDirectory(prefix="hot-update-agent-cleanup-") as tmp:
        root = Path(tmp)
        # bash reads an owned script named agent; its argv has the daemon shape.
        # No tether executable is launched, and no external process is signalled.
        # origin: S10 internal review RUNALL-A F2. /proc cmdline can be
        # empty during exec; readiness pins the intended image before census.
        ready = root / "ready"
        (root / "agent").write_text("printf ready > ready\nread -r -t 15 unused\n")
        for mutation in ("positive", "duplicate pid", "wrong nonce", "reused pid", "wrong argv"):
            argv = ["tether", "agent", "--session", "lab", "--nid", "agt1"]
            if mutation == "wrong argv":
                argv[-1] = "agt2"
            ready.unlink(missing_ok=True)
            child = subprocess.Popen(argv, executable="/bin/bash", cwd=root, stdin=subprocess.PIPE)
            try:
                wait_for(ready.exists, "owned Bash image ready")
                ident = SENSOR["agent_identity"](child.pid)
                assert ident["pid"] == child.pid and ident["start_ticks"] > 0
                assert ident["argv"] == argv and len(ident["exe_sha"]) == 64, (ident["argv"], argv)
                baseline = copy.deepcopy(ident)
                if mutation == "reused pid":
                    ident["start_ticks"] += 1
                identity = root / "identity.json"
                identity.write_text(json.dumps(ident))
                pidfile = root / f"hup-agent-{NONCE}.pid"
                pidfile.write_text(f"{child.pid}\n" * (2 if mutation == "duplicate pid" else 1))
                args = types.SimpleNamespace(identity=identity, pidfile=pidfile, session="lab",
                                             nonce="0"*32 if mutation == "wrong nonce" else NONCE)
                accepted = True
                try:
                    SENSOR["stop_agent"](args)
                except SENSOR["EvidenceError"]:
                    accepted = False
                assert accepted == (mutation == "positive"), mutation
                if accepted:
                    assert child.wait(timeout=3) == -signal.SIGTERM
                else:
                    assert child.poll() is None, f"agent cleanup signalled {mutation}"
                    assert SENSOR["same_process"](baseline, SENSOR["process_identity"](child.pid))
            finally:
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=3)
                child.stdin.close()


