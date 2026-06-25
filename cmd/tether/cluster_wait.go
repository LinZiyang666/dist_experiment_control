package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// cluster_wait.go (B5 OPS#5 + OPS#13) — `cluster status --watch`, the shared convergence poller,
// and the `cluster wait <node> --phase` verb. All read-only (poll OpClusterStatus); none mutates.

// minWatchInterval is the floor for --watch / --wait polling. Each socket frame can take up to
// ~observePollWindow (2s) because StatusReport runs the live health scatter-gather, so a sub-2s
// interval would just queue blocking calls.
const minWatchInterval = 2 * time.Second

// watchClusterStatus repaints the operator status every interval until Ctrl-C. A transient socket
// error mid-watch is printed to stderr and the loop CONTINUES (a monitor, not a gate). On a TTY it
// clears the screen each frame; otherwise it prints a separator. --json emits JSONL (one object
// per line, no ANSI). Exits 0 on Ctrl-C (the per-frame health exit code is NOT applied — a watch
// is interactive, not a cron probe).
func watchClusterStatus(cmd *cobra.Command, socketPath string, asJSON bool, interval time.Duration) error {
	if interval < minWatchInterval {
		return usageErr("--watch interval must be ≥ %s (each frame can take ~2s via the live health poll)", minWatchInterval)
	}
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	isTTY := term.IsTerminal(int(os.Stdout.Fd()))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if asJSON {
			// JSONL: no screen clear, one object per line.
		} else if isTTY {
			_, _ = fmt.Fprint(cmd.OutOrStdout(), "\033[H\033[2J") // cursor home + clear
		} else {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "--- %s ---\n", time.Now().Format(time.RFC3339))
		}
		rep, err := fetchClusterStatusReport(socketPath)
		if err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "watch: %v (retrying)\n", err)
		} else if asJSON {
			// BLK-1 (Stage-C): JSONL — one COMPACT object per frame (not the one-shot's pretty
			// MarshalIndent), so `jq -c` / a line-reader parses each frame.
			if b, merr := json.Marshal(rep); merr == nil {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), string(b))
			}
		} else {
			renderClusterStatusReport(cmd, rep, false)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// phaseGone is the pseudo-phase for `cluster wait --phase GONE` (the node is absent from the
// roster — the retire/remove terminal state, which has no roster phase string).
const phaseGone = "GONE"

// newClusterWaitCmd is `tether cluster wait <node> --phase <PHASE>` — block until a node reaches a
// phase (or GONE), with a timeout. Read-only and idempotent; timeout / Ctrl-C → exit 75 (transient,
// "not converged yet — safe to retry"), a failure-terminal phase → immediate non-zero.
func newClusterWaitCmd(socketPath *string) *cobra.Command {
	var phase string
	var timeout, interval time.Duration
	cmd := &cobra.Command{
		Use:   "wait <node-id>",
		Short: "Block until a node reaches a phase (or GONE), with a timeout",
		Example: "  tether cluster wait brk-b --phase VOTER --timeout 2m\n" +
			"  tether cluster wait brk-b --phase GONE   # after drain --retire",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if phase == "" {
				return usageErr("--phase is required (e.g. VOTER, CATCHING_UP, RETIRING, or GONE)")
			}
			node := args[0]
			pred := func(n *adminsock.ClusterNodeStatus, _ *adminsock.ClusterStatusReport) (bool, string) {
				if phase == phaseGone {
					return n == nil, "" // converged once absent from the roster
				}
				if n == nil {
					return false, "" // not in roster yet — keep waiting
				}
				if n.Phase == phase {
					return true, ""
				}
				// VOTER_ADD_FAILED is terminal when waiting for VOTER (don't burn the timeout).
				if phase == "VOTER" && n.Phase == "VOTER_ADD_FAILED" {
					return false, "node entered VOTER_ADD_FAILED (the AddVoter raft op failed); see `tether cluster status`"
				}
				return false, ""
			}
			return waitForConverge(cmd, *socketPath, node, pred, timeout, interval)
		},
	}
	cmd.Flags().StringVar(&phase, "phase", "", "target phase: VOTER | CATCHING_UP | JOIN_VERIFIED_PENDING_VOTER | DRAINING | RETIRING | VOTER_ADD_FAILED | GONE")
	cmd.Flags().DurationVar(&timeout, "timeout", 2*time.Minute, "give up after this long (exit 75)")
	cmd.Flags().DurationVar(&interval, "interval", minWatchInterval, "poll interval (≥2s)")
	return cmd
}

// waitForConverge polls OpClusterStatus until pred reports converged, a failure-terminal reason, a
// timeout, or Ctrl-C. pred(node, report) gets the target's roster row (nil if absent) + the full
// report; returns (done, failReason). A transient fetch error is retried (the cluster may be
// mid-election). timeout / cancel → exit 75; a failReason → a plain error (exit 70 unless the
// caller wraps it).
func waitForConverge(cmd *cobra.Command, socketPath, node string, pred func(*adminsock.ClusterNodeStatus, *adminsock.ClusterStatusReport) (bool, string), timeout, interval time.Duration) error {
	if interval < minWatchInterval {
		interval = minWatchInterval
	}
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	deadline := time.Time{}
	if timeout > 0 {
		deadline = nowFunc().Add(timeout)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		rep, err := fetchClusterStatusReport(socketPath)
		if err == nil {
			var row *adminsock.ClusterNodeStatus
			for i := range rep.Nodes {
				if rep.Nodes[i].NodeID == node {
					row = &rep.Nodes[i] // first match (a roster + orphan-voter append can list it twice)
					break
				}
			}
			done, fail := pred(row, rep)
			if fail != "" {
				return fmt.Errorf("cluster wait %s: %s", node, fail)
			}
			if done {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s converged\n", node)
				return nil
			}
		} else {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "wait: %v (retrying)\n", err)
		}
		if !deadline.IsZero() && !nowFunc().Before(deadline) {
			return &ExitError{Class: exitTransient, Err: fmt.Errorf("cluster wait %s: not converged after %s (safe to retry)", node, timeout)}
		}
		select {
		case <-ctx.Done():
			return &ExitError{Class: exitTransient, Err: fmt.Errorf("cluster wait %s: interrupted before convergence", node)}
		case <-ticker.C:
		}
	}
}

// nowFunc is time.Now, indirected so a test can drive the timeout deterministically.
var nowFunc = time.Now
