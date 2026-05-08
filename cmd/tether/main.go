package main

import (
	"fmt"
	"os"
	"runtime"

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
	return root
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintf(cmd.OutOrStdout(),
				"tether %s (proto v%d)\n%s/%s\n%s\n",
				proto.ReleaseVersion, proto.ProtoVersion,
				runtime.GOOS, runtime.GOARCH, runtime.Version())
			return nil
		},
	}
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
