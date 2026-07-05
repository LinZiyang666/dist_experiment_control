#!/usr/bin/env python3
# pty-confirm.py — fail-loud pty feeder for tether's typed-confirm cluster ops.
#
# Several destructive cluster ops (cluster init --from-existing, force-single --online,
# recovery recover) call confirmTypedNodeID, which prints `type "<id>" to confirm: ` and
# REFUSES when stdin is os.Stdin and not a TTY (cmd/tether/cluster_offline.go:396). There is
# NO --yes / env escape for these call sites (allowMachineEscape=false), so automation MUST
# drive them through a real pty. This helper allocates a pty, runs the command with the pty as
# its controlling terminal, waits for the confirm prompt, types the answer exactly once, then
# streams the rest of the output and propagates the child's exit code.
#
# FAIL-LOUD by design: if the confirm prompt is not seen within the timeout, we DO NOT blindly
# type the answer (blind-typing an irreversible one-way migration when the prompt drifted is
# dangerous). We kill the child and exit non-zero so the caller notices.
#
# Usage: pty-confirm.py <answer> [--timeout SECONDS] [--prompt SUBSTR] -- <cmd> [args...]
import os
import pty
import select
import sys
import time

PROMPT_DEFAULT = "to confirm:"  # verified stable substring of `type %q to confirm: `


def main() -> int:
    argv = sys.argv[1:]
    if not argv:
        sys.stderr.write("pty-confirm: usage: <answer> [--timeout N] [--prompt S] -- <cmd...>\n")
        return 2
    answer = argv[0]
    argv = argv[1:]
    timeout = 60.0
    prompt = PROMPT_DEFAULT
    while argv and argv[0].startswith("--"):
        if argv[0] == "--":
            argv = argv[1:]
            break
        if argv[0] == "--timeout":
            timeout = float(argv[1]); argv = argv[2:]
        elif argv[0] == "--prompt":
            prompt = argv[1]; argv = argv[2:]
        else:
            sys.stderr.write("pty-confirm: unknown flag %s\n" % argv[0])
            return 2
    if not argv:
        sys.stderr.write("pty-confirm: no command after --\n")
        return 2

    cmd = argv
    pid, fd = pty.fork()
    if pid == 0:  # child: exec the command with the pty as controlling tty
        os.execvp(cmd[0], cmd)
        os._exit(127)  # unreachable on success

    # parent: watch output for the prompt, type the answer once, then pump.
    buf = b""
    answered = False
    deadline = time.monotonic() + timeout
    prompt_b = prompt.encode()
    try:
        while True:
            if not answered and time.monotonic() > deadline:
                sys.stderr.write(
                    "\npty-confirm: FAIL — prompt %r not seen within %.0fs; refusing to blind-type. "
                    "child killed.\n" % (prompt, timeout)
                )
                try:
                    os.kill(pid, 9)
                except ProcessLookupError:
                    pass
                os.waitpid(pid, 0)
                return 3
            r, _, _ = select.select([fd], [], [], 0.5)
            if fd in r:
                try:
                    chunk = os.read(fd, 4096)
                except OSError:
                    break  # pty closed (child exited)
                if not chunk:
                    break
                sys.stdout.buffer.write(chunk)
                sys.stdout.buffer.flush()
                if not answered:
                    buf += chunk
                    if prompt_b in buf:
                        os.write(fd, (answer + "\n").encode())
                        answered = True
                        buf = b""
    finally:
        pass

    _, status = os.waitpid(pid, 0)
    if not answered:
        sys.stderr.write("pty-confirm: WARNING — child exited before the confirm prompt appeared.\n")
    return os.waitstatus_to_exitcode(status)


if __name__ == "__main__":
    sys.exit(main())
