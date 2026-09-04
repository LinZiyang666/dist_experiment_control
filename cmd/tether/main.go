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

// agentNonDaemonSubcommands are the `tether agent <name>` forms that are NOT the
// long-running daemon. Kept as a table so the reconciliation test can walk the real
// command tree and require every child to appear here or in its explicit daemon-like
// list — a new subcommand added without a decision would otherwise silently start
// consuming the daemon's boot budget.
// argvHasStart reports whether `--start` is set in argv, honouring the bool-flag
// spellings cobra accepts. `--start=false` is NOT set; an unparsable value is treated as
// set, which is the conservative direction here — a daemon misread as a subcommand loses
// its boot budget and cannot commit an upgrade, while the reverse only spends one tick.
func argvHasStart(rest []string) bool {
	for _, a := range rest {
		name, val, hasVal := strings.Cut(a, "=")
		if name != "--start" {
			continue
		}
		if !hasVal {
			return true
		}
		if b, err := strconv.ParseBool(val); err != nil || b {
			return true
		}
	}
	return false
}

// agentValueTakingFlags are the `tether agent` flags whose SEPARATED spelling
// (`--nid gpu1`) consumes the next argv token. origin: prerelease audit round 2,
// J2.
//
// A table rather than a read of the real flag set, because this runs BEFORE cobra
// parses anything — that is the entire premise of isAgentDaemonInvocation (external
// review F2: the boot-budget tick must be spent even when parsing is what the
// staged binary breaks on). The drift risk a hand-kept table carries is answered by
// TestAgentValueTakingFlagsMatchTheRealCommand, which walks the actual command and
// fails on any flag added, removed or changed between bool and value.
var agentValueTakingFlags = map[string]bool{
	"--nats-url":          true,
	"--session":           true,
	"--nid":               true,
	"--pin":               true,
	"--tunnel-addr":       true,
	"--upgrade-url-allow": true,
	"--log-file":          true,
	"--log-level":         true,
}

// agentSubcommandToken returns the first token cobra would treat as a SUBCOMMAND
// NAME, skipping flags and the values they consume. origin: prerelease audit round
// 2, J2.
//
// This is cobra's stripFlags reasoning reproduced on raw argv, which is what makes
// `tether agent --log-level debug doctor` (a subcommand) and `tether agent --nid
// doctor` (the daemon, with a flag value that happens to read like one) come out
// differently — the first version treated both as the daemon by looking only at
// args[2].
//
// An UNKNOWN flag is assumed not to take a value. That is the conservative
// direction: it can only make a token look like a subcommand, and a daemon misread
// as a subcommand loses one boot-budget tick, while the reverse spends the budget
// that has to prove a staged binary boots.
func agentSubcommandToken(rest []string) (string, bool) {
	for i := 0; i < len(rest); i++ {
		a := rest[i]
		if a == "--" {
			// Everything after the terminator is positional.
			if i+1 < len(rest) {
				return rest[i+1], true
			}
			return "", false
		}
		if !strings.HasPrefix(a, "-") {
			return a, true
		}
		if name, _, hasVal := strings.Cut(a, "="); !hasVal && agentValueTakingFlags[name] {
			i++ // this flag eats the token behind it
		}
	}
	return "", false
}

var agentNonDaemonSubcommands = map[string]bool{
	"doctor": true,
	"join":   true,
	"config": true,
	"help":   true,
}

func isAgentDaemonInvocation(args []string) bool {
	if len(args) < 2 || args[1] != "agent" {
		return false
	}
	// A SUBCOMMAND IS NOT THE DAEMON. origin: prerelease audit
	// cli-serve-agent-cluster/L4-F3. `tether agent doctor`, `tether agent join` and
	// `tether agent config refresh` all reached this function's `return true`, so each
	// of them consumed a boot-budget tick belonging to the daemon's upgrade state
	// machine — a `tether agent doctor` run during a rollback could spend the budget
	// that was meant to prove the new binary boots.
	//
	// The predicate deliberately looks at the first POSITIONAL token, not at args[2].
	// `tether agent --nid doctor` is the daemon with a flag VALUE that happens to be a
	// subcommand name, and excluding it would be the same class of mistake in reverse.
	//
	// POSITION IS NOT THE TEST — origin: prerelease audit round 2, J2. The first
	// version only inspected args[2], so `tether agent --log-level debug doctor` —
	// which cobra genuinely routes to the doctor subcommand, because stripFlags
	// consumes `--log-level`'s value and finds `doctor` behind it — was still read as
	// the daemon and still spent a boot-budget tick belonging to the upgrade state
	// machine. That is the very defect L4-F3 set out to close, closed for one spelling
	// and left open for its sibling.
	if tok, found := agentSubcommandToken(args[2:]); found && agentNonDaemonSubcommands[tok] {
		// `agent join --start` IS the daemon: it runs the agent in-process, in the
		// foreground, for the life of the host. Excluding it stopped
		// agent.BootUpgradeCheck from running on that launch shape, so every
		// `node upgrade` against such an agent became structurally unable to commit and
		// was force-rolled-back at the register deadline (round 2, J1).
		//
		// Scanned across all of args[2:] rather than checked positionally, because the
		// flag can appear anywhere after the subcommand and in either spelling cobra
		// accepts.
		if tok == "join" && argvHasStart(args[2:]) {
			return true
		}
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
