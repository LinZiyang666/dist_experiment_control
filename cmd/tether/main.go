package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "tether",
		Short:         "Tether distributed node control",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newVersionCmd())
	root.AddCommand(newServeCmd())
	root.AddCommand(newAgentCmd())
	root.AddCommand(newAdminCmd())
	root.AddCommand(newSessionCmd())
	root.AddCommand(newLoginCmd())
	root.AddCommand(newLogoutCmd())
	root.AddCommand(newCtxCmd())
	root.AddCommand(newExecCmd())
	root.AddCommand(newPsCmd())
	root.AddCommand(newRunCmd())
	root.AddCommand(newExposeCmd())
	root.AddCommand(newProxyCmd())
	root.AddCommand(newHistoryCmd())
	root.AddCommand(newNodeCmd())
	root.AddCommand(newPushCmd())
	root.AddCommand(newPullCmd())
	root.AddCommand(newClusterCmd())
	root.AddCommand(newAlertCmd())
	return root
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// First line via proto.VersionLine — a frozen cross-version seam
			// (deployed agents' smoke gates parse it; see its doc comment),
			// not a printf to be reworded here.
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"%s\n%s/%s\n%s\n",
				proto.VersionLine(),
				runtime.GOOS, runtime.GOARCH, runtime.Version())
			return nil
		},
	}
}

func main() {
	// origin: upgrade-safety external review F2. The boot half of the
	// upgrade state machine must run BEFORE Cobra parsing and before any
	// config/YAML/logger step: a staged binary that regresses on flag
	// parsing or strict YAML decoding exits on those paths, and if the boot
	// check lived after them the boot budget would never be consumed — the
	// supervisor would relaunch the same broken binary forever with the
	// rollback machinery never engaging. Recognition is deliberately
	// pre-parse and conservative (argv shape only); the check itself is a
	// no-op without a pending marker next to the executable.
	if isAgentDaemonInvocation(os.Args) {
		agent.BootUpgradeCheck(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	}
	// ExecuteContext + signal.NotifyContext so Ctrl-C / SIGTERM tear
	// down running subcommands (`tether ps`, `history --follow`,
	// `exec`, `session rm`, etc.) instead of waiting for each
	// command's per-call timeout. Audit shard 04 F1: bare Execute()
	// gave subcommands `cmd.Context() == Background()`, so signal
	// handling was a no-op everywhere. The signal-aware ctx is
	// observed by every cobra command via cmd.Context().
	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := newRootCmd().ExecuteContext(ctx); err != nil {
		renderTerminalError(os.Stderr, err)
		// B2 item 3: classify the terminal error into a sysexits-style code so a monitor can
		// tell "broker unreachable" (69) from "bad arg" (64) from "permission" (77) from an
		// unclassified tether-side fault (70). 0 still means success; only the nonzero value is
		// now informative. (cluster status / exec / run os.Exit() before reaching here.)
		// stop() explicitly: os.Exit does not run deferred calls, so the `defer stop()` above would
		// never fire on any non-zero exit. Harmless for a dying process, but a `defer` that provably
		// cannot run on the path it was written for is a claim the code does not keep.
		stop()
		//nolint:gocritic // exitAfterDefer flags the SHAPE (a defer and an os.Exit in one function),
		// which is still here. The substance it warns about is not: stop() runs on the line above, so
		// the cleanup the `defer` promises actually happens on this path too. Restructuring into
		// `os.Exit(run())` would satisfy the shape, but cobra's ExecuteContext already owns this frame
		// and the exit code is derived from its error -- the wrapper would move the same two lines
		// somewhere else and gain nothing.
		os.Exit(classifyExit(err))
	}
}

// isAgentDaemonInvocation recognizes `tether agent ...` BY ARGV SHAPE ONLY —
// it must work before Cobra parses anything (external review F2: the whole
// point is to consume boot budget even when parsing itself is what the
// staged binary breaks on). Conservative exclusions: help output and the
// install/uninstall service paths never touch the upgrade marker (the smoke
// gate's own `version` probe is already excluded by args[1] != "agent").
func isAgentDaemonInvocation(args []string) bool {
	if len(args) < 2 || args[1] != "agent" {
		return false
	}
	for _, arg := range args[2:] {
		name, val, hasVal := strings.Cut(arg, "=")
		switch name {
		case "--help", "-h", "--install-user-service", "--uninstall":
			// external re-review F10: honor the bool-flag forms Cobra
			// accepts — `--flag=true` is set, `--flag=false` is not. An
			// unparsable value is treated as set (conservative: Cobra will
			// reject it before RunE, and a non-daemon misread only skips a
			// boot-budget tick; the reverse misread would burn budget on a
			// service-install invocation).
			if !hasVal {
				return false
			}
			if b, err := strconv.ParseBool(val); err != nil || b {
				return false
			}
		case "help":
			if !hasVal {
				return false
			}
		}
	}
	return true
}

// renderTerminalError is the main sink's stderr print, factored out so it is testable and so a QUIET
// error (a machine-output command's non-zero exit, e.g. `doctor --json`) does NOT append prose to a
// stream a caller merges with stdout (`... --json 2>&1 | jq`). Quiet errors carry their failure on
// stdout (the JSON) + the exit code; a stderr line would only corrupt the parse.
func renderTerminalError(w io.Writer, err error) {
	if err == nil || errorIsQuiet(err) {
		return
	}
	_, _ = fmt.Fprintln(w, "error:", err)
}
