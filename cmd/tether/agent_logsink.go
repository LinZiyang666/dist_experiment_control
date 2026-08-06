package main

// agent_logsink.go (h1 F) — the agent daemon's log-sink resolution and its
// process-owned panic destination.
//
// WHY THE AGENT DEFAULTS TO A FILE AND serve DOES NOT
//
// serve's binary default stays stderr: dev runs and the embedded-NATS test
// harness must keep printing where the operator is looking, and a DEPLOYED
// broker gets its path from install.sh's broker.yaml. The agent has no
// equivalent channel — the fleet's agents were launched as
// `setsid nohup … >> agent.log 2>&1` and self-upgrade via syscall.Exec, so
// their argv and their fds are frozen for the life of the host. The BINARY
// DEFAULT is the only thing that can reach them at the release hop, which is
// exactly why the cap has to be there and not in a launcher.
//
// WHY dup2 AND NOT "just rename the log file"
//
// Those frozen agents hold fd 1/2 open on the very file the new rotating
// writer manages. After the first rotation the inherited fd follows
// agent.log → .1 → .2 → an UNLINKED inode: every future panic/stacktrace
// (which Go writes to raw fd 2, bypassing slog entirely) lands somewhere
// nobody can open. Renaming the managed file only relocates the problem, so
// instead the agent RE-POINTS its own fd 2 at a dedicated boot.err whose size
// is bounded at startup. The panic destination becomes a property of the
// process, for every launch style (nohup, user unit, future systemd), with zero
// per-host action.

import (
	"io"
	"os"
	"path/filepath"

	"log/slog"

	"github.com/LinZiyang666/tether/internal/logrotate"
	"github.com/spf13/cobra"
)

// resolveAgentLogSink returns the slog sink and the path fd 2 should be
// re-pointed at ("" ⇒ leave fd 2 alone, i.e. the explicit stderr opt-out).
// Precedence: --log-file flag > agent.yaml log_file > default
// <home>/agent/<sid>/agent.log.
func resolveAgentLogSink(cmd *cobra.Command, home, sid, flagVal string, ay agentYAML) (io.Writer, string) {
	path := pickFlagOrYaml(cmd, "log-file", flagVal, ay.LogFile)
	if path == "" {
		path = filepath.Join(home, "agent", sid, "agent.log")
	}
	if path == "-" {
		return os.Stderr, ""
	}
	// Best-effort: a missing session dir is normal on a fresh install.
	_ = os.MkdirAll(filepath.Dir(path), 0o700)
	w := resolveLogSink(path, ay.LogMaxSizeMB, ay.LogMaxBackups)
	if rw, ok := w.(*logrotate.Writer); ok {
		return rw, filepath.Join(filepath.Dir(rw.Path()), "agent.boot.err")
	}
	// resolveLogSink degraded to stderr — fd 2 is already the destination.
	return w, ""
}

// bootErrMaxBytes caps the panic sink. Two files are kept (live + ".1"), so
// the sink's total on-disk footprint is bounded by ~2× this plus whatever ONE
// process writes to fd 2 after opening it.
//
// 1 MiB is generous for the content: this file receives panics, runtime
// stacktraces, and pre-logger boot output — a few KB per crash. The bound
// exists for the FAILURE mode, not the normal one.
const bootErrMaxBytes = 1 << 20

// redirectStderrTo dup2's fd 2 onto a SIZE-BOUNDED boot.err so panics,
// runtime stacktraces and any library that writes to raw stderr land in a
// process-owned file. Best-effort: a failure leaves fd 2 as it was (worst
// case: today's behavior) and is reported through the logger, never fatal —
// an agent that refuses to start over its panic sink would be a worse outage
// than one whose panics go where they went before.
//
// # WHY THE CAP IS APPLIED AT OPEN, NOT BY A ROTATING WRITER
//
// origin: docs/reviews/h1-external-review.md F4. The first cut opened this
// O_APPEND with no bound at all, which contradicted h1's own goal ("every
// tether log is capped in-process") in the one place it matters most: a
// crash-looping agent under systemd or a supervisor re-appends a stacktrace
// per restart, forever, and re-creates the disk-exhaustion failure h1 exists
// to remove.
//
// It cannot be fixed with a logrotate.Writer, because the Go runtime writes
// panics to fd 2 DIRECTLY — no Go-level writer is in that path, which is the
// entire reason this file exists. So the bound is applied where a Go writer
// still can act: at OPEN. Restart is the unit of growth for a crash loop, and
// every restart passes through here.
//
// Resulting hard bound, across unlimited restarts: live ≤ cap at open time,
// plus one retained predecessor, plus one process's own fd-2 output. A single
// process that scribbles unboundedly on fd 2 is NOT covered — nothing in-band
// could cover it — but no tether code does, and a panic writes once and dies.
func redirectStderrTo(path string, logger *slog.Logger) {
	rotateBootErrIfLarge(path, logger)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		logger.Warn("agent: could not open panic sink; stderr left as inherited", "path", path, "err", err)
		return
	}
	if err := dup2Stderr(f); err != nil {
		logger.Warn("agent: could not re-point stderr at the panic sink", "path", path, "err", err)
		_ = f.Close()
		return
	}
	// f's own fd is now redundant (fd 2 is the live duplicate) but closing it
	// is still correct: the description stays alive through fd 2.
	_ = f.Close()
	logger.Info("agent: panic sink armed", "path", path)
}

// rotateBootErrIfLarge keeps the panic sink bounded across restarts: when the
// live file has reached the cap, it becomes ".1" (replacing any previous one)
// and the next open starts empty. Every failure here is deliberately silent
// except for a debug line — an unrotatable sink must never stop the agent
// from arming fd 2, since an oversized sink is still better than a panic that
// goes nowhere.
func rotateBootErrIfLarge(path string, logger *slog.Logger) {
	st, err := os.Stat(path)
	if err != nil || st.Size() < bootErrMaxBytes {
		return
	}
	if err := os.Rename(path, path+".1"); err != nil {
		// Could not rotate (read-only dir, cross-device): truncate instead, so
		// the file cannot keep growing. Same anti-incident rule as
		// internal/logrotate — NO failure mode may leave an unbounded file.
		if f, terr := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0o600); terr == nil {
			_, _ = f.WriteString("tether: panic sink truncated (rotate failed: " + err.Error() + ")\n")
			_ = f.Close()
		}
		return
	}
	logger.Debug("agent: rotated panic sink", "path", path, "cap_bytes", bootErrMaxBytes)
}
