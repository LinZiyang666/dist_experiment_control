package infracli

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/spf13/cobra"
)

// Closed classes are separate from free-form diagnostics. Unknown host codes
// become an explicit infra_internal diagnostic rather than an accidental 0.
// origin: hot-update S9 plan §7.3 / T53 / M26
var ExitClasses = map[string]int{
	proto.CodeInfraUnsupported: 64, proto.CodeInfraArtifactInvalid: 64,
	proto.CodeInfraConfigInvalid: 64, proto.CodeInfraIncompatible: 64,
	proto.CodeInfraPermissionDenied: 77, proto.CodeInfraPolicyStale: 77,
	proto.CodeInfraNotEnabled: 75, proto.CodeInfraBusy: 75,
	proto.CodeInfraNotReady: 75, proto.CodeInfraInventoryIncomplete: 75,
	proto.CodeInfraBudgetExceeded: 75, proto.CodeInfraResultUnknown: 74,
	proto.CodeInfraRecoveryRequired: 74, proto.CodeInfraInternal: 70,
}

var Hints = map[string]string{
	proto.CodeInfraUnsupported:         "Infra hot cut requires linux/amd64, systemd, pidfd, cgroup and nft capabilities, and an installed component adapter.",
	proto.CodeInfraArtifactInvalid:     "Select the catalog component version and its binary SHA256; stage the approved artifact first.",
	proto.CodeInfraConfigInvalid:       "Check component operands and the digest of the managed staged Caddy configuration.",
	proto.CodeInfraIncompatible:        "Choose a component compatible with the actual current store and configuration.",
	proto.CodeInfraPermissionDenied:    "Use an authorized account identity or the local admin socket owner; policy rotation requires local root.",
	proto.CodeInfraPolicyStale:         "Re-authorize under the current installed policy; old tickets are revoked.",
	proto.CodeInfraNotEnabled:          "As local root, run tether-infra-helper bootstrap after staging the approved support artifacts.",
	proto.CodeInfraBusy:                "Query the pending transaction and finish its retained responsibilities before starting another cut.",
	proto.CodeInfraNotReady:            "Inspect the failed readiness barrier; restore health before retrying.",
	proto.CodeInfraInventoryIncomplete: "Obtain a complete current process, socket, asset and participant census before retrying.",
	proto.CodeInfraBudgetExceeded:      "Inspect the original transaction; its remaining budget cannot be renewed by retrying the effect.",
	proto.CodeInfraResultUnknown:       "The effect may have happened. Query the original transaction before any recovery; do not start a replacement.",
	proto.CodeInfraRecoveryRequired:    "The cut retains responsibilities. Query status, then recover the same transaction from its journal.",
	proto.CodeInfraInternal:            "Inspect the infra invariant or IO failure with the transaction identity; do not infer completion.",
}

const BootstrapGuide = `Manage NATS/Caddy through the broker-local admin socket, including single mode.
Stage the approved helper/launcher artifacts, then as local root run:
  tether-infra-helper bootstrap
Inspect pending and retiring transactions with tether admin infra status --tx <id>.
Support updates use the fixed tether-infra-helper update-support or rollback-support
entry, once supplied by the installed helper. Never replace selectors by hand.
`

const RotationGuide = `After account/CA rotation on every surviving broker, synchronize the installed
infra policy revision, key_id and trust references before accepting new infra
requests. Run tether admin infra rotate-policy as local root for the fixed installed
policy input and same-domain CAS. Old tickets must be revoked; pending transactions
remain read-only until explicitly re-authorized. This command refuses unsupported
until the installed management protocol provides the root policy binding.
`

func Failure(code, detail, tx, phase string) error {
	class, ok := ExitClasses[code]
	if !ok {
		code, class = proto.CodeInfraInternal, 70
		detail = "unrecognized external infra code"
	}
	msg := code
	if tx != "" {
		msg += " tx=" + tx
	}
	if phase != "" {
		msg += " phase=" + phase
	}
	if detail != "" {
		msg += ": " + detail
	}
	msg += "; " + Hints[code]
	if tx != "" {
		// Keep one hex Tx per command output. The placeholder is deliberately
		// used in the hint: drill tooling extracts the command's sole identity.
		msg += " Run tether admin infra status --tx <id> (or cluster upgrade --component <component> --status <id>); recover that same ID only."
	}
	return &ExitError{Class: class, Code: code, Err: fmt.Errorf("%s", msg)}
}

// Render never promotes a nonterminal phase to completed. Query success is
// distinct from the result of a requested cut/recovery, as in the S8 CLI.
// The wire exposes only InfraReport: absent private physical fields remain
// zero in the Status document, never invented from a disk or health snapshot.
// origin: hot-update S9 plan §7.2 / §7.3 / T53
func Render(out io.Writer, r proto.InfraCommand, resp *proto.InfraUpgradeResp) error {
	if resp == nil {
		return Failure(proto.CodeInfraInternal, "missing infra reply", r.TxID, "unknown")
	}
	if resp.Report == nil && r.Op == "status" && resp.Code == proto.CodeInfraResultUnknown {
		resp = &proto.InfraUpgradeResp{OK: true, Report: &proto.InfraReport{TxID: r.TxID, Phase: "unknown", Code: resp.Code}}
	}
	if resp.Report == nil {
		code := resp.Code
		if code == "" {
			code = proto.CodeInfraResultUnknown
		}
		return Failure(code, "infra reply has no transaction report", r.TxID, "unknown")
	}
	s, err := statusProjection(r.TxID, resp.Report)
	if err != nil {
		return err
	}
	if r.Op == "status" {
		// A successfully observed unknown/halted/deferred record is rc0.
		// Permission/transport refusals without an observation remain errors.
		if !resp.OK && resp.Code != "" && resp.Code != proto.CodeInfraResultUnknown && resp.Code != proto.CodeInfraRecoveryRequired {
			return Failure(resp.Code, "infra status query refused", r.TxID, resp.Report.Phase)
		}
		return json.NewEncoder(out).Encode(s)
	}
	code := resp.Code
	if code == "" {
		code = resp.Report.Code
	}
	if code != "" {
		return Failure(code, "infra operation did not complete", r.TxID, resp.Report.Phase)
	}
	if !resp.OK || s.Phase != c.Completed {
		code = proto.CodeInfraRecoveryRequired
		if s.Phase == c.Deferred {
			code = proto.CodeInfraNotReady
		}
		return Failure(code, "infra operation retained responsibilities", r.TxID, resp.Report.Phase)
	}
	if s.Step == 0 || s.Revision == 0 || s.Current.ExeSHA256 == (c.Digest{}) || s.Proof == (c.Digest{}) || s.Census.OldOwnerPending != 0 {
		return Failure(proto.CodeInfraRecoveryRequired, "completed reply lacks running-image or retirement evidence", r.TxID, resp.Report.Phase)
	}
	_, err = fmt.Fprintf(out, "infra tx=%s phase=completed running_sha256=%s\n", r.TxID, resp.Report.RunningSHA256)
	return err
}

func statusProjection(tx string, r *proto.InfraReport) (c.Status, error) {
	s := c.Status{Step: r.Step, Revision: r.Revision, Code: r.Code}
	if !hexValue(tx, 16) || r.TxID != tx {
		return s, Failure(proto.CodeInfraInternal, "infra reply names another transaction", tx, "unknown")
	}
	b, _ := hex.DecodeString(tx)
	copy(s.Tx[:], b)
	if r.Phase == "unknown" {
		s.Phase = 0
	} else {
		phases := []string{"prepared", "staged", "ready", "moving", "cutting", "verifying", "returning", "retiring", "completed", "deferred", "halted"}
		for i, phase := range phases {
			if phase == r.Phase {
				s.Phase = c.Phase(i + 1)
			}
		}
		if s.Phase == 0 {
			return s, Failure(proto.CodeInfraInternal, "unknown infra phase token", tx, "unknown")
		}
	}
	if r.Code != "" {
		if _, ok := ExitClasses[r.Code]; !ok {
			return s, Failure(proto.CodeInfraInternal, "unknown infra status code", tx, r.Phase)
		}
	}
	for _, field := range []struct {
		text   string
		digest *c.Digest
	}{{r.RunningSHA256, &s.Current.ExeSHA256}, {r.Proof, &s.Proof}} {
		if field.text == "" {
			continue
		}
		if !hexValue(field.text, 32) {
			return s, Failure(proto.CodeInfraInternal, "malformed infra evidence digest", tx, r.Phase)
		}
		b, _ := hex.DecodeString(field.text)
		copy(field.digest[:], b)
	}
	s.Census.LiveBusiness, s.Census.OldOwnerPending = r.LiveBusiness, r.OldOwnerPending
	return s, nil
}

// ExitError carries an explicit exit class through the error chain. The central classifier
// honors it before any heuristic, so a class set at the SOURCE (positive evidence) always wins.
type ExitError struct {
	Class int
	Err   error
	// Quiet suppresses the main sink's human-readable "error: ..." stderr line. Set by commands whose
	// FAILURE is already carried on stdout as MACHINE output (e.g. `cluster doctor --json`, whose JSON
	// has summary.fatal): a prose line to stderr would corrupt a caller that MERGES the streams —
	// `... --json 2>&1 | jq` gets the JSON followed by "error: ..." and fails to parse it (the exact
	// R11 deploy-tier failure, drill 52 B3/55c). The non-zero exit + the JSON already convey the
	// failure, so the prose is redundant AND harmful there.
	Quiet bool
	// Code is the WIRE code this error was built from ("download_http_status", "not_owner", …), carried
	// structurally so a caller can branch on it without reading the prose. Empty for errors that have no
	// wire code (a NATS transport failure, a decode error, a CLI usage error).
	//
	// origin: line-2 external review 疑惑 #2. `node upgrade --all`'s isTransientError/isConfigError
	// classified by strings.Contains(err.Error(), code) — the exact practice this file's own comment 10
	// lines below forbids ("The classifier never string-sniffs prose for a class"). The failure it invites
	// is not theoretical: broker prose is operator-facing and gets reworded, and a hint that happens to
	// mention a code name flips a fleet rollout between "skip this node" and "abort the fleet". Setting
	// the code at the source makes the branch positive evidence.
	//
	// The agent_rejected: wrapper is STRIPPED before this is set, so a caller matches the underlying code
	// exactly and does not have to know which errors travelled wrapped.
	Code string
}

func (e *ExitError) Error() string { return e.Err.Error() }
func (e *ExitError) Unwrap() error { return e.Err }

// renderAdminRuntime prints the human table for `admin runtime`. -1 counts (a platform where
// /proc/self is unreadable) render as "n/a" so an operator never mistakes an unmeasurable value
// for a real zero.
func RenderAdminRuntime(cmd *cobra.Command, rep *adminsock.RuntimeReport) error {
	w := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(w, "goroutines : %d\n", rep.Goroutines)
	_, _ = fmt.Fprintf(w, "threads    : %d\n", rep.Threads)
	_, _ = fmt.Fprintf(w, "open_fds   : %s\n", intOrNA(rep.OpenFDs))
	_, _ = fmt.Fprintf(w, "rss        : %s\n", rssHuman(rep.RSSBytes))
	_, _ = fmt.Fprintf(w, "uptime     : %s\n", time.Duration(rep.UptimeSeconds*float64(time.Second)).Round(time.Second))
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	// SLOWEST is maxDur, not lastDur: a pass that wedged once and then recovered is exactly what an
	// operator is looking for here, and a last-tick-only column would have already forgotten it.
	// OVERRUNS says how many of those completions took longer than the pass's own interval.
	_, _ = fmt.Fprintln(tw, "\nRECONCILER\tINTERVAL\tAUTHORITY\tLAST_TICK\tRUNS\tSKIPS\tSLOWEST\tOVERRUNS\tLAST_ERR")
	now := time.Now()
	for _, r := range rep.Reconcilers {
		last := "never"
		if !r.LastTick.IsZero() {
			last = HumaneAge(now.Sub(r.LastTick)) + " ago"
		}
		slowest := "-"
		if r.MaxDurMS > 0 {
			slowest = (time.Duration(r.MaxDurMS) * time.Millisecond).String()
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d\t%s\t%d\t%s\n",
			r.Name, time.Duration(r.IntervalMS)*time.Millisecond, dashIfEmptyAdmin(r.Authority),
			last, r.Runs, r.Skips, slowest, r.Overruns, dashIfEmptyAdmin(r.LastErr))
	}
	if len(rep.Reconcilers) == 0 {
		_, _ = fmt.Fprintln(tw, "(no reconcilers registered)")
	}

	// The cluster loops own their own tickers, so they are NOT registry passes and get their own table.
	// Reading it: a periodic loop (CADENCE set) whose ITERS is 0, or whose LAST_ITER stopped advancing,
	// is wedged or died on its first line. An event-driven loop (CADENCE "-") with ITERS 0 has simply
	// had nothing to do. Before this existed the only signal was STARTED, which a loop that returned
	// immediately also has.
	//
	// ITERS is LIVENESS — completed iterations — for every row including alert-webhook. It is NOT
	// "alerts delivered": a POST the endpoint rejects is still a completed iteration. Delivery outcome is
	// the separate ALERT_WEBHOOK table below (external review B2-4; this legend previously said
	// "delivered", which is what made the two readings collide).
	if len(rep.ClusterLoops) > 0 {
		_, _ = fmt.Fprintln(tw, "\nCLUSTER_LOOP\tCADENCE\tSTARTED\tITERS\tLAST_ITER")
		for _, l := range rep.ClusterLoops {
			cadence := "-"
			if l.CadenceMS > 0 {
				cadence = (time.Duration(l.CadenceMS) * time.Millisecond).String()
			}
			started, lastIter := "never", "never"
			if !l.StartedAt.IsZero() {
				started = HumaneAge(now.Sub(l.StartedAt)) + " ago"
			}
			// nil means the loop has never completed an iteration. For a loop with a CADENCE that is the
			// dead signal; for an event-driven one (cadence "-") it just means nothing has happened.
			if l.LastIter != nil && !l.LastIter.IsZero() {
				lastIter = HumaneAge(now.Sub(*l.LastIter)) + " ago"
			}
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n", l.Name, cadence, started, l.Iterations, lastIter)
		}
	}

	// external review B2-4: the alert webhook's DELIVERY OUTCOME, printed next to the loop table rather
	// than inside it. ITERS above answers "did the consumer consume"; these answer "did the endpoint take
	// it". They used to be the same number, which meant a live poster against a 401 endpoint printed
	// ITERS 0 — the value that means DEAD in the row above.
	//
	// ACCEPTED+REJECTED tracks the alert-webhook row's ITERS. DROPS is counted at enqueue, so it is
	// deliberately outside that sum: a dropped event never reached the loop.
	if w := rep.AlertWebhook; w != nil {
		_, _ = fmt.Fprintln(tw, "\nALERT_WEBHOOK\tACCEPTED\tREJECTED\tDROPPED_QUEUE_FULL")
		_, _ = fmt.Fprintf(tw, "delivery\t%d\t%d\t%d\n", w.Accepted, w.Rejected, w.Drops)
		if w.Rejected > 0 && w.Accepted == 0 {
			_, _ = fmt.Fprintln(tw, "\t(the consumer is ALIVE and the ENDPOINT is refusing every POST — check the URL/token, not the broker)")
		}
	}
	return tw.Flush()
}

func intOrNA(n int) string {
	if n < 0 {
		return "n/a"
	}
	return strconv.Itoa(n)
}

func dashIfEmptyAdmin(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// rssHuman renders a byte count as a compact MiB string; -1 → "n/a".
func rssHuman(b int64) string {
	if b < 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1f MiB", float64(b)/(1024*1024))
}

func HumaneAge(d time.Duration) string {
	switch {
	case d < time.Second:
		return "<1s"
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
}

func StripPrefix(s, prefix string) (string, bool) {
	if strings.HasPrefix(s, prefix) {
		return s[len(prefix):], true
	}
	return "", false
}

// printNotSafeBanner is the non-green floor (rejection #5): no "done/safe/secure/complete" wording.
func PrintNotSafeBanner(w io.Writer, node string) {
	_, _ = fmt.Fprintf(w, "\nSECURITY: credentials are NOT yet rotated — %s can still authenticate. The cluster is NOT SAFE\n", node)
	_, _ = fmt.Fprintf(w, "  against this node until you complete the rotation above and clear %s.\n", "manual:credrot:"+node)
	_, _ = fmt.Fprintf(w, "  Track: `tether alert ls` / `tether cluster status`.\n")
}

// Run lifecycle failures may arrive after the child has started. Keep those
// distinct from brokerCodeExitClasses, whose entries describe admission refusals.
// origin: hot-update S4 plan §2.2 / T49
var RunFailureExitClasses = map[string]int{
	"pty_input_overflow": 74,
}
