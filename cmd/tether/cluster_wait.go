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

// cluster_wait.go (B5 OPS#5 + OPS#13) — `cluster status --watch` + the shared convergence poller
// waitForConverge (used by `transfer-leader --wait` / per-op --wait). C8 deleted the standalone
// `cluster wait` verb; these are read-only (poll OpClusterStatus); none mutates.

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
				return fmt.Errorf("cluster converge %s: %s", node, fail)
			}
			if done {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s converged\n", node)
				return nil
			}
		} else {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "wait: %v (retrying)\n", err)
		}
		if !deadline.IsZero() && !nowFunc().Before(deadline) {
			return &ExitError{Class: exitTransient, Err: fmt.Errorf("cluster converge %s: not converged after %s (safe to retry)", node, timeout)}
		}
		select {
		case <-ctx.Done():
			return &ExitError{Class: exitTransient, Err: fmt.Errorf("cluster converge %s: interrupted before convergence", node)}
		case <-ticker.C:
		}
	}
}

// nowFunc is time.Now, indirected so a test can drive the timeout deterministically.
var nowFunc = time.Now

// settleDefaultInterval is the poll gap for `cluster status --settle`. Each fetch already blocks up
// to ~observePollWindow (2s) via the live health scatter-gather, so a 1s gap between polls paces the
// loop without adding meaningful latency to detecting a settle.
const settleDefaultInterval = 1 * time.Second

// settleClusterStatus (D3) is the OPT-IN debounce for `cluster status`. A voter restart produces a
// REAL, brief DEGRADED (exit 1) window — the restarting voter is genuinely unreachable / catching up
// for a few seconds, so the instantaneous exit code honestly flaps 0→1→0. The default one-shot
// reports that transient faithfully (it is real; masking it by default would be dishonest). --settle
// lets a MONITOR say "give a benign restart up to <dur> to clear before you trust a DEGRADED verdict":
//
//   - HEALTHY_HA (0) at any poll        → return immediately (the transient cleared / never happened).
//   - QUORUM_LOST (2) / FORCE_SINGLE (3) → return immediately. These are NEVER benign restart blips;
//     debouncing them into "transient" would hide a real outage — exactly what D3 warns against.
//   - DEGRADED (1)                       → keep polling until it clears OR the window elapses. If it is
//     STILL DEGRADED after <dur>, the degradation is SUSTAINED, not a restart blip: return exit 1
//     honestly. A permanently NOT-HA (N=2) cluster therefore waits the full window then still exits 1.
//
// It renders only the FINAL report; the caller owns render + os.Exit(rep.ExitCode). Testable via the
// fetchClusterStatusReport + nowFunc seams.
func settleClusterStatus(cmd *cobra.Command, socketPath string, settle, interval time.Duration) (*adminsock.ClusterStatusReport, error) {
	if interval <= 0 {
		interval = settleDefaultInterval
	}
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	deadline := nowFunc().Add(settle)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var lastRep *adminsock.ClusterStatusReport
	var lastErr error
	for {
		rep, err := fetchClusterStatusReport(socketPath)
		if err == nil {
			lastRep, lastErr = rep, nil
			// A terminal (non-transient) verdict surfaces at once; a DEGRADED verdict may be a
			// restart blip, so fall through to the deadline check and keep polling.
			if rep.ExitCode != 1 {
				return rep, nil
			}
		} else {
			// A transient socket error mid-settle (mid-election, momentary unbind) is retried; a
			// monitor asked us to wait out benign blips, and a dropped frame is one.
			lastErr = err
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "settle: %v (retrying)\n", err)
		}
		if !nowFunc().Before(deadline) {
			// Window elapsed. A DEGRADED that never cleared is a SUSTAINED degradation — return it so
			// the caller exits 1 honestly. If every poll errored, surface the last transport error.
			if lastRep != nil {
				return lastRep, nil
			}
			return nil, lastErr
		}
		select {
		case <-ctx.Done():
			if lastRep != nil {
				return lastRep, nil
			}
			return nil, &ExitError{Class: exitTransient, Err: fmt.Errorf("cluster status --settle: interrupted before the health verdict settled")}
		case <-ticker.C:
		}
	}
}
