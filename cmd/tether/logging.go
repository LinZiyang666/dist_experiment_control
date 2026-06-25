package main

import (
	"log/slog"
	"os"
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
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(level)); err != nil {
		return nil, usageErr("--log-level %q is not one of debug/info/warn/error", level)
	}
	opts := &slog.HandlerOptions{Level: lv}
	var h slog.Handler
	if jsonOut {
		h = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	return slog.New(h), nil
}
