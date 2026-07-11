#!/usr/bin/env python3
# pty-run.py — S0-pty: fail-loud interactive pty DRIVER for `tether run` (generalizes pty-confirm.py).
#
# pty-confirm.py drives a single typed-confirm prompt. This drives a full interactive session: it
# allocates a pty of a CHOSEN size (so the far side's `stty size` reflects a real window), runs the
# command with that pty as its controlling terminal, then executes an ORDERED --step list — expect a
# substring, send input, send raw bytes (e.g. Ctrl-C), resize, or EOF — before propagating the child's
# exit code. A ctl user *has* a terminal in production, so this is the SIM supplying the terminal the
# env is responsible for (Mandate ③/④) — NOT compensating for a tether gap. It only feeds stdin / reads
# stdout of the unchanged product `tether run`; ptmx permission is never the thing under test (the
# deploy value is the real CROSS-CONTAINER byte-stream / signal / resize).
#
# FAIL-LOUD: an `expect` not seen within --idle-timeout SIGKILLs the child, drains, and exits 3 (a
# wedged `tether run` never hangs the drill; a missed marker never silently proceeds). The DRILL must
# additionally wrap the whole invocation in `timeout <N>s` as an outer watchdog.
#
# RACE-FREE WINSIZE: openpty() + TIOCSWINSZ the SLAVE to R×C BEFORE execve, then fork; the child
# setsid()+TIOCSCTTY()+dup2()s the slave. This guarantees `tether run`'s terminalSize()
# (term.GetSize(os.Stdout.Fd()), cmd/tether/run.go) reads R×C, never the 80×24 fallback.
#
# ECHO-vs-OUTPUT: the remote pty is in cooked mode and echoes typed input, so an `expect` token equal
# to a keystroke would match the echo, not the command output. The CALLER must key every marker on
# SHELL-COMPUTED output (e.g. `send:printf 'R%sK\n' O` → output `ROK`, input has `R%sK`; or on
# `stty size` output `40 132` ≠ input `stty size`). pty-run.py only substring-matches captured output,
# advancing a search cursor past each matched expect so a later step cannot re-match earlier output.
#
# Usage (steps applied IN ARGV ORDER):
#   pty-run.py [--rows R=40] [--cols C=132] [--idle-timeout S=20] \
#              --step '<verb>[:<arg>]' [--step ...] -- <cmd> [args...]
#   expect:<substr>[:<secs>]  wait until <substr> appears in NEW output within <secs> (default
#                             --idle-timeout), else FAIL(3). The optional trailing :<secs> gives a
#                             per-step deadline — SHORT for a post-Ctrl-C prompt (a still-blocked
#                             shell must fail fast), LONG for the first output after a ~15s attach.
#   send:<text>       write <text>+"\n" to the master
#   sendraw:<hex>     write raw hex bytes to the master (sendraw:03 = Ctrl-C)
#   ctrlc             alias for sendraw:03
#   resize:<R>x<C>    TIOCSWINSZ the master to R×C, then kill(child, SIGWINCH)
#   sleep:<secs>      pump output for <secs> (bounded)
#   eof               close the master write side (send EOF to the child's stdin)
import fcntl
import os
import select
import signal
import struct
import sys
import termios
import time

DEF_ROWS, DEF_COLS = 40, 132
DEF_IDLE = 20.0


def _winsz(fd, rows, cols):
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


def _parse(argv):
    rows, cols, idle = DEF_ROWS, DEF_COLS, DEF_IDLE
    steps = []
    while argv and argv[0] != "--":
        a = argv[0]
        if a == "--rows":
            rows = int(argv[1]); argv = argv[2:]
        elif a == "--cols":
            cols = int(argv[1]); argv = argv[2:]
        elif a == "--idle-timeout":
            idle = float(argv[1]); argv = argv[2:]
        elif a == "--step":
            spec = argv[1]; argv = argv[2:]
            verb, _, arg = spec.partition(":")
            steps.append((verb, arg))
        else:
            sys.stderr.write("pty-run: unknown flag %r\n" % a); sys.exit(2)
    # reject BOTH a missing `--` delimiter (argv empty) AND a delimiter with no command after it
    # (argv == ["--"], len 1) — the latter otherwise returns an empty cmd whose child cmd[0] raises a
    # bare IndexError traceback instead of this clean fail-loud exit 2 (external review R2-F2).
    if not argv or len(argv) == 1:
        sys.stderr.write("pty-run: no command after --\n"); sys.exit(2)
    return rows, cols, idle, steps, argv[1:]


def main() -> int:
    rows, cols, idle, steps, cmd = _parse(sys.argv[1:])

    master, slave = os.openpty()
    # size the pty BEFORE fork/exec so the child's very first terminalSize() reads R×C.
    _winsz(slave, rows, cols)
    pid = os.fork()
    if pid == 0:  # child: become a session leader with the slave as controlling tty, then exec.
        os.close(master)
        os.setsid()
        try:
            fcntl.ioctl(slave, termios.TIOCSCTTY, 0)
        except OSError:
            pass
        os.dup2(slave, 0); os.dup2(slave, 1); os.dup2(slave, 2)
        if slave > 2:
            os.close(slave)
        try:
            os.execvp(cmd[0], cmd)
        except OSError as e:
            # exec FAILED (e.g. command not found): the child must NOT unwind into the parent's Python and
            # print a traceback — exit 127 (shell "command not found" convention), fail-loud and stable.
            sys.stderr.write("pty-run: exec %r failed: %s\n" % (cmd[0], e))
        os._exit(127)  # reached only if execvp raised (on success execvp never returns)

    os.close(slave)
    captured = bytearray()
    cursor = 0  # expect only scans captured[cursor:]

    def pump(timeout):
        # returns bytes read, b'' on idle, None when the pty closes (child exited).
        r, _, _ = select.select([master], [], [], timeout)
        if master not in r:
            return b""
        try:
            chunk = os.read(master, 4096)
        except OSError:
            return None
        if not chunk:
            return None
        captured.extend(chunk)
        sys.stdout.buffer.write(chunk); sys.stdout.buffer.flush()
        return chunk

    def die(msg, rc):
        sys.stderr.write("\npty-run: FAIL — %s\n" % msg)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            os.waitpid(pid, 0)
        except ChildProcessError:
            pass
        return rc

    closed = False
    try:
        for verb, arg in steps:
            # S1-11: `eof` closes the master (master=-1); it must be the TERMINAL step. Any fd-touching verb
            # after it would crash on the closed fd — select.select([-1],…) → ValueError, os.write(-1,…) →
            # OSError[Errno 9] — an uncaught traceback instead of a clean FAIL. A step after eof is a drill
            # authoring error → fail loud. (eof-as-last-step is fine: this guard only fires on a NEXT step.)
            if master < 0:
                return die("step %r after eof: master is already closed (eof must be the last step)" % verb, 3)
            if verb == "expect":
                # optional trailing :<secs> per-step deadline (a marker never contains ':' + digits).
                exp_to = idle
                head, sep, tail = arg.rpartition(":")
                if sep and tail.isdigit():
                    arg = head
                    exp_to = float(tail)
                target = arg.encode()
                deadline = time.monotonic() + exp_to
                while captured.find(target, cursor) < 0:
                    if time.monotonic() > deadline:
                        return die("expect %r not seen within %.0fs" % (arg, exp_to), 3)
                    got = pump(0.25)
                    if got is None:
                        if captured.find(target, cursor) < 0:
                            return die("child exited before expect %r" % arg, 3)
                        closed = True
                        break
                idx = captured.find(target, cursor)
                if idx >= 0:
                    cursor = idx + len(target)
            elif verb == "send":
                os.write(master, (arg + "\n").encode())
            elif verb == "sendraw":
                os.write(master, bytes.fromhex(arg))
            elif verb == "ctrlc":
                os.write(master, b"\x03")
            elif verb == "resize":
                rs, _, cs = arg.partition("x")
                _winsz(master, int(rs), int(cs))
                try:
                    os.kill(pid, signal.SIGWINCH)
                except ProcessLookupError:
                    pass
            elif verb == "sleep":
                end = time.monotonic() + float(arg)
                while time.monotonic() < end:
                    if pump(min(0.25, max(0.0, end - time.monotonic()))) is None:
                        closed = True
                        break
            elif verb == "eof":
                os.close(master)
                master = -1
            else:
                return die("unknown step verb %r" % verb, 2)
            if closed:
                break
        # drain remaining output until the child exits (bounded by idle after last activity).
        idle_deadline = time.monotonic() + idle
        drained_alive = False  # True if we stop draining while the child may still be running (silent-but-alive)
        while not closed and master >= 0:
            got = pump(0.5)
            if got is None:
                break  # pty closed → the child has exited; the terminal waitpid reaps at once
            if got:
                idle_deadline = time.monotonic() + idle
            elif time.monotonic() > idle_deadline:
                drained_alive = True  # went silent but never closed the pty — may still be wedged/running
                break
    finally:
        pass

    # S1-07: a silent-but-alive (wedged) `tether run` would make the terminal blocking waitpid hang forever.
    # Mirror die()'s self-protection — SIGKILL + reap — so pty-run FAILS LOUD on its own timeline (the drill's
    # outer `timeout` is a backstop, not the only net). A drained_alive child reaps to a non-zero exit → RED.
    if drained_alive:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    _, status = os.waitpid(pid, 0)
    return os.waitstatus_to_exitcode(status)


if __name__ == "__main__":
    sys.exit(main())
