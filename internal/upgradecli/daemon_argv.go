package upgradecli

import (
	"strconv"
	"strings"
)

// The pre-parse daemon chooser moved here verbatim from cmd/tether/main.go
// (hot-update S8 plan §11.2 P19 slim move). main keeps the original names as
// aliases, so its call sites and the agentboot wiring scanner still see
// isAgentDaemonInvocation exactly where they did.

// AgentDaemonInvocation (main: isAgentDaemonInvocation) recognizes `tether agent ...` BY ARGV SHAPE ONLY —
// it must work before Cobra parses anything (external review F2: the whole
// point is to consume boot budget even when parsing itself is what the
// staged binary breaks on). Conservative exclusions: help output and the
// install/uninstall service paths never touch the upgrade marker (the smoke
// gate's own `version` probe is already excluded by args[1] != "agent").

// AgentNonDaemonSubcommands (main: agentNonDaemonSubcommands) are the `tether agent <name>` forms that are NOT the
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

// AgentValueTakingFlags (main: agentValueTakingFlags) are the `tether agent` flags whose SEPARATED spelling
// (`--nid gpu1`) consumes the next argv token. origin: prerelease audit round 2,
// J2.
//
// A table rather than a read of the real flag set, because this runs BEFORE cobra
// parses anything — that is the entire premise of isAgentDaemonInvocation (external
// review F2: the boot-budget tick must be spent even when parsing is what the
// staged binary breaks on). The drift risk a hand-kept table carries is answered by
// TestAgentValueTakingFlagsMatchTheRealCommand, which walks the actual command and
// fails on any flag added, removed or changed between bool and value.
var AgentValueTakingFlags = map[string]bool{
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
		if name, _, hasVal := strings.Cut(a, "="); !hasVal && AgentValueTakingFlags[name] {
			i++ // this flag eats the token behind it
		}
	}
	return "", false
}

var AgentNonDaemonSubcommands = map[string]bool{
	"doctor": true,
	"join":   true,
	"config": true,
	"help":   true,
}

func AgentDaemonInvocation(args []string) bool {
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
	if tok, found := agentSubcommandToken(args[2:]); found && AgentNonDaemonSubcommands[tok] {
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
