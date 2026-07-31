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
		// THREE-way, and the first arm doing nothing is the point: --json emits neither a screen
		// clear nor a separator, because BLK-1 (Stage-C) makes each frame one parseable JSON object
		// per line and B5's external review already had to fix a violation of it once.
		//
		// A `switch` rather than if/else-if/else: the earlier if/else-if form needed an EMPTY BLOCK
		// for the --json arm, revive's empty-block flagged it, and collapsing the three arms to two
		// (`if !asJSON && isTTY {clear} else {separator}`) silently moved --json into the separator
		// arm — re-introducing that exact BLOCKER as a side effect of a lint cleanup. A switch says
		// "three cases, one of them deliberately empty" without an empty block for a linter to
		// object to.
		switch {
		case asJSON:
			// JSONL: no clear, no separator — the line must be the object.
		case isTTY:
			_, _ = fmt.Fprint(cmd.OutOrStdout(), "\033[H\033[2J") // cursor home + clear
		default:
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "--- %s ---\n", time.Now().Format(time.RFC3339))
		}
		rep, err := fetchClusterStatusReport(socketPath)
		switch {
		case err != nil && asJSON:
			// EVERY FRAME IS A LINE, including a failed one. origin: line-2 closure verification §6 B2.
			// This used to write the retry notice to stderr and nothing at all to stdout, so a JSONL
			// consumer saw a silent GAP: no object, no error, just a frame that never arrived. The
			// contract the comment above states ("each frame is one parseable JSON object per line") was
			// therefore true only while the socket was healthy — which is the half of the time nobody
			// needs the contract.
			//
			// The notice still goes to stderr for a human watching, and now a parseable error object goes
			// to stdout for the machine.
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "watch: %v (retrying)\n", err)
			emitWatchFrameError(cmd, "fetch", err)
		case err != nil:
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "watch: %v (retrying)\n", err)
		case asJSON:
			// BLK-1 (Stage-C): JSONL — one COMPACT object per frame (not the one-shot's pretty
			// MarshalIndent), so `jq -c` / a line-reader parses each frame.
			b, merr := json.Marshal(rep)
			if merr != nil {
				// Same rule as above: a marshal failure used to drop the frame silently (`if merr == nil`
				// with no else), which is the one outcome a stream consumer cannot distinguish from "the
				// cluster is fine and quiet".
				emitWatchFrameError(cmd, "marshal", merr)
				break
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), string(b))
		default:
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

// watchFrameError is what a failed `--watch --json` frame looks like on stdout.
//
// origin: line-2 closure verification §6 B2. A named type rather than an inline map so the shape is
// greppable and can be pinned by a test — a stream contract that only exists inside a fmt.Fprintf is a
// contract nobody can assert.
//
// `stage` distinguishes the two ways a frame can fail to appear: "fetch" (the admin socket did not answer)
// and "marshal" (it answered with something unserialisable).
//
// THIS STREAM IS HETEROGENEOUS BY DESIGN, and the discriminator is the same one the rest of the machine-JSON
// surface uses (jsonout.go: "a monitor keys on (schema, schema_version)"):
//
//	line has "schema":"cluster_status_watch_error"  → this type; the frame did NOT arrive
//	line has "view":"ctl-nats"|"offline"            → adminsock.ClusterStatusReport; a real frame
//
// origin: line-2 external review, 疑惑 #1. The reviewer asked whether the error object also needs
// schema_version. It does, and it was the only machine-JSON payload in the CLI without one — a strict
// decoder that negotiated a version had no way to negotiate this line, so its only safe move on a transient
// socket error was to exit. Both keys are non-omitempty: their stable presence IS the discriminator.
//
// TRAP, stated because the near-miss is silent: a status frame carries `errors` (PLURAL, broker-side
// problems folded into a report that DID arrive) and this type carries `error` (SINGULAR, no report at all).
// `jq 'select(.error)'` selects only frame failures; `jq 'select(.errors | length > 0)'` selects degraded
// frames. Confusing them reads a healthy-but-degraded cluster as a dead socket, or vice versa.
type watchFrameError struct {
	Schema        string `json:"schema"`         // "cluster_status_watch_error"
	SchemaVersion int    `json:"schema_version"` // 1
	Error         string `json:"error"`
	Stage         string `json:"stage"`
	At            string `json:"at"`
}

const (
	watchFrameErrorSchema        = "cluster_status_watch_error"
	watchFrameErrorSchemaVersion = 1
)

func emitWatchFrameError(cmd *cobra.Command, stage string, err error) {
	b, merr := json.Marshal(watchFrameError{
		Schema:        watchFrameErrorSchema,
		SchemaVersion: watchFrameErrorSchemaVersion,
		Error:         err.Error(),
		Stage:         stage,
		At:            time.Now().UTC().Format(time.RFC3339Nano),
	})
	if merr != nil {
		// Cannot happen for three strings, and if it somehow did, a hand-built line is still better than a
		// dropped frame — which is the entire point of this function. The discriminator keys are carried
		// here too: a fallback line that a versioned decoder cannot classify is a dropped frame with extra
		// steps.
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "{\"schema\":%q,\"schema_version\":%d,\"error\":%q,\"stage\":%q}\n",
			watchFrameErrorSchema, watchFrameErrorSchemaVersion, err.Error(), stage)
		return
	}
	_, _ = fmt.Fprintln(cmd.OutOrStdout(), string(b))
}
