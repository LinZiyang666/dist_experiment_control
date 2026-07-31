package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"syscall"

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
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"tether %s (proto v%d)\n%s/%s\n%s\n",
				proto.ReleaseVersion, proto.ProtoVersion,
				runtime.GOOS, runtime.GOARCH, runtime.Version())
			return nil
		},
	}
}

func main() {
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
