package main

import (
	"context"
	"fmt"
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
		fmt.Fprintln(os.Stderr, "error:", err)
		// B2 item 3: classify the terminal error into a sysexits-style code so a monitor can
		// tell "broker unreachable" (69) from "bad arg" (64) from "permission" (77) from an
		// unclassified tether-side fault (70). 0 still means success; only the nonzero value is
		// now informative. (cluster status / exec / run os.Exit() before reaching here.)
		os.Exit(classifyExit(err))
	}
}
