package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/LinZiyang666/tether/internal/logrotate"
)

// logging.go (B5 OPS#8) — structured-log construction shared by `serve` and `agent`.
//
// Byte-equivalence anchor: newLogger("info", false) MUST be identical to the prior
// slog.New(slog.NewTextHandler(os.Stderr, nil)) — slog's default level IS Info, so
// &slog.HandlerOptions{Level: LevelInfo} produces byte-identical output. A golden test pins it.

// newLogger builds the broker/agent logger from --log-level / --log-json. An unparseable level is
// a usage error (exit 64) — never a silent default, so a typo'd `--log-level debg` fails loudly
// instead of quietly staying at info. level is case-insensitive (debug/info/warn/error).
func newLogger(level string, jsonOut bool) (*slog.Logger, error) {
	return newLoggerTo(level, jsonOut, os.Stderr)
}

// newLoggerTo is newLogger with an explicit sink (h1 F): serve/agent resolve a
// size-capped logrotate.Writer and pass it here. newLogger(level, json) stays
// the byte-equivalence anchor — same handler, same options, os.Stderr.
func newLoggerTo(level string, jsonOut bool, w io.Writer) (*slog.Logger, error) {
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(level)); err != nil {
		return nil, usageErr("--log-level %q is not one of debug/info/warn/error", level)
	}
	opts := &slog.HandlerOptions{Level: lv}
	var h slog.Handler
	if jsonOut {
		h = slog.NewJSONHandler(w, opts)
	} else {
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h), nil
}

// resolveLogSink turns a resolved (path, sizeMB, backups) triple into the
// io.Writer newLoggerTo writes through (h1 F). "" or "-" mean stderr —
// explicitly, so a deployment can opt back out. A file sink NEVER fails the
// command: logrotate.Open degrades to stderr and keeps retrying, because a
// broker that refuses to boot over its log file is a worse outage than a
// broker logging to stderr.
func resolveLogSink(path string, maxMB, backups int) io.Writer {
	if path == "" || path == "-" {
		return os.Stderr
	}
	if maxMB <= 0 {
		maxMB = logrotate.DefaultMaxMB
	}
	if backups <= 0 {
		backups = logrotate.DefaultBackups
	}
	w, err := logrotate.Open(path, int64(maxMB)<<20, backups, 0o600)
	if err != nil {
		// Unreachable TODAY: logrotate.Open never returns an error (a
		// birth-degraded Writer spills to stderr, prints its own DEGRADED
		// line, and keeps retrying). Kept as future-proofing — if Open ever
		// grows a real error path this degrades loudly instead of nil-deref.
		fmt.Fprintf(os.Stderr, "tether: log file %s unusable (%v); logging to stderr\n", path, err)
		return os.Stderr
	}
	// #75 breadcrumb: once slog goes to the file, journal (where the unit
	// routes stderr) would otherwise show NOTHING — the live incident read
	// exactly like that ambiguity, "is the file sink working or is the
	// config dead?". One stderr line at boot answers it from journalctl
	// alone. Deliberately stderr, not the returned sink: printing it into
	// the file it announces would miss the one place operators look.
	// Phrased as the CONFIGURED sink, not a success claim: logrotate.Open
	// never fails (a birth-degraded Writer spills to stderr and retries),
	// and its own DEGRADED line — printed just above when applicable — is
	// the authority on whether bytes actually reach the file.
	fmt.Fprintf(os.Stderr, "tether: log sink %s (cap %dMB x %d backups)\n", path, maxMB, backups)
	return w
}
