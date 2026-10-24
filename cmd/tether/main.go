package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/spf13/cobra"
)

// brokerBoot owns the one envelope transferred before Cobra can fail. The
// serve validates through this owner; Finish is the preactivation exit
// funnel, including invocations whose argv never reaches serve.
// origin: hot-update S6 plan §4.1 / T03
var brokerBoot *brokerimage.BootImage

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "tether",
		Short:         "Tether distributed node control",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newVersionCmd(), newServeCmd())
	root.AddCommand(newBrokerImagePreflightCmd(), newAgentCmd())
	root.AddCommand(newAdminCmd())
	root.AddCommand(newSessionCmd())
	root.AddCommand(newWhoamiCmd())
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
	// origin: hot-update S7 plan §7 / T34
	root.AddCommand(newPushCmd(), newPullCmd(), newTransferCmd(transferCLI))
	root.AddCommand(newClusterCmd())
	// origin: hot-update S6 plan §5 / lead L3; S8 plan §5.2 / LQ1 (explicit root stage)
	root.AddCommand(newBrokerCmd(), upgradecli.NewUpgradeCommand(upgradeStagePorts()))
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
			// The image-ABI line is LAST and append-only (proto.ImageABILine):
			// the smoke gate of an older agent reads the first line only, the
			// hot-update candidate gate of a newer one reads this one.
			// origin: hot-update S5 plan §5.2; hot-update S4 plan §5.2 / T46. Domains extend reader
			// capability while the original ABI remains the final output line.
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"%s\n%s/%s\n%s\n%s\n",
				proto.VersionLine(),
				runtime.GOOS, runtime.GOARCH, runtime.Version(), strings.Join(proto.ImageVersionLines(), "\n"))
			return nil
		},
	}
}

func main() {
	img := agentboot.Begin(slog.New(slog.NewTextHandler(os.Stderr, nil)), agent.LineageEnvKeys()...)
	// origin: hot-update S6 plan §4.1 / T03
	// Dispatch from the validated role before the agent's non-daemon guard.
	// Wrong argv and a missing chooser hint must still reach broker salvage.
	restored, bootErr := agentboot.TakeRole(brokerimage.ImageRole)
	if restored != nil {
		brokerBoot, bootErr = brokerimage.Boot(restored)
		if brokerBoot == nil {
			_ = restored.Close()
			if bootErr == nil {
				bootErr = fmt.Errorf("broker: dispatched image has no boot owner")
			}
		}
	}
	if brokerBoot != nil && bootErr == nil {
		bootErr = brokerBoot.CheckInvocation(os.Args)
	}
	// origin: hot-update S3 plan §1.2. Claim carried descriptors before any
	// env consumer or goroutine, even when argv no longer names a daemon.
	if restored == nil && img != nil && img.Phase() == agentboot.PhaseHeld && !isAgentDaemonInvocation(os.Args) {
		img.Abandon(slog.New(slog.NewTextHandler(os.Stderr, nil)), agentboot.CrashWindowNonDaemon, "dispatched image reached a non-daemon invocation")
	}
	// origin: upgrade-safety external review F2. The boot half of the
	// upgrade state machine must run BEFORE Cobra parsing and before any
	// config/YAML/logger step: a staged binary that regresses on flag
	// parsing or strict YAML decoding exits on those paths, and if the boot
	// check lived after them the boot budget would never be consumed — the
	// supervisor would relaunch the same broken binary forever with the
	// rollback machinery never engaging. Recognition is deliberately
	// pre-parse and conservative (argv shape only); the check itself is a
	// no-op without a pending marker next to the executable.
	if restored == nil && isAgentDaemonInvocation(os.Args) {
		agent.BootUpgradeCheck(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	}
	// ExecuteContext + signal.NotifyContext so Ctrl-C / SIGTERM tear
	// down running subcommands (`tether ps`, `history --follow`,
	// `exec`, `session rm`, etc.) instead of waiting for each
	// command's per-call timeout. Audit shard 04 F1: bare Execute()
	// gave subcommands `cmd.Context() == Background()`, so signal
	// handling was a no-op everywhere. The signal-aware ctx is
	// observed by every cobra command via cmd.Context().
	ctx, stop := signal.NotifyContext(context.Background(), // ctx-root: CLI process signal root
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	err := bootErr
	if err == nil {
		err = newRootCmd().ExecuteContext(ctx)
	}
	if brokerBoot != nil {
		// A successful redispatch never returns. Errors retain the current
		// boot owner; Finish must not replace ACTIVE state with the held cut.
		if finishErr := brokerBoot.Finish(err); finishErr != nil {
			err = finishErr
		}
	} else if restored == nil {
		agent.FinishBoot(err)
	}
	if err != nil {
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
		// origin: S7 internal review rv-drill F1: keep the native result through the final custody barrier.
		os.Exit(drillobserve.FinishNativeExit(classifyExit(err)))
	}
	drillobserve.FinishNativeExit(0)
}

// The pre-parse daemon chooser lives with the upgrade boot logic it feeds
// (internal/upgradecli/daemon_argv.go, comments moved verbatim). These names keep
// main's call site and the agentboot wiring scanner on isAgentDaemonInvocation.
// origin: hot-update S8 plan §11.2 P19 (main slim move)
var (
	isAgentDaemonInvocation   = upgradecli.AgentDaemonInvocation
	agentValueTakingFlags     = upgradecli.AgentValueTakingFlags
	agentNonDaemonSubcommands = upgradecli.AgentNonDaemonSubcommands
)

// upgradeExit maps a transaction error into the exit taxonomy: S8 codes use
// the transaction policy (an accepted deferral is 74, never 75), and older
// refusal codes keep their registered classes and hints.
// origin: hot-update S8 plan §7.1 / §11.2 P19
func upgradeExit(verb string, err error) error {
	var failure *upgradetx.Error
	if err == nil || !errors.As(err, &failure) {
		return err
	}
	if _, s8 := upgradewire.ErrorExit(failure.Code); s8 || failure.Code == upgradetx.CodeNotWired {
		return &ExitError{Class: upgradecli.ExitCode(err), Code: failure.Code, Err: fmt.Errorf("%s: %w", verb, err)}
	}
	return brokerErrorMessage(verb, failure.Code, failure.Detail)
}

// upgradeStagePorts binds the root stage to this process's home, the exit
// taxonomy and the isolated version probe.
// origin: hot-update S8 plan §5.2 / LQ1 / §11.2 P19
func upgradeStagePorts() upgradecli.StagePorts {
	return upgradecli.StagePorts{Exit: upgradeExit, Probe: probeUpgradeImage,
		ControlDir: func(key upgradetx.Key) (string, error) { return upgradecli.ControlDirFor(key, cli.DefaultHome()) }}
}

// probeUpgradeImage is the isolated, bounded `version` probe of exact bytes.
func probeUpgradeImage(path string) (string, agentimage.Capabilities, error) {
	return agentimage.ProbeCapabilities(path, spawnsafe.RunVersionProbe)
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
