package main

import (
	"encoding/json"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/infracli"
	"github.com/spf13/cobra"
)

// defaultAdminSocket matches architecture A.3 / I.2b. Operators can
// override via --socket; the default works for the install.sh-shaped
// production deployment.
const defaultAdminSocket = "/var/run/tether/admin.sock"

func newAdminCmd() *cobra.Command {
	var socketPath string

	root := &cobra.Command{
		Use:   "admin",
		Short: "Administrative subcommands (broker-local Unix socket)",
		Long: `Admin commands talk to the broker's local /var/run/tether/admin.sock
(architecture I.2b). They MUST be run on the broker host as a user
with read+write access to the socket file (mode 0600 — typically the
'tether' system user or root).`,
	}
	root.PersistentFlags().StringVar(&socketPath, "socket", defaultAdminSocket,
		"path to the broker admin Unix socket")

	root.AddCommand(infracli.AdminCommand(func() string { return socketPath }, infracli.AdminPorts{Call: infracli.CallAdmin, Exit: func(err error) error { return err }}))
	root.AddCommand(newAdminSessionsCmd(&socketPath))
	root.AddCommand(newAdminNodesCmd(&socketPath))
	root.AddCommand(newAdminAuditCmd(&socketPath))
	root.AddCommand(newAdminEventsCmd(&socketPath))
	root.AddCommand(newAdminEvictCmd(&socketPath))
	root.AddCommand(newAdminRuntimeCmd(&socketPath))
	root.AddCommand(newAdminSessionAllowCmd(&socketPath))
	return root
}

// newAdminEventsCmd (#30) tails the broker's H.1 `events` stream — the operator's reader for
// sys.events. Members can subscribe to sys.events over NATS; the operator (root on the broker host)
// uses this admin-socket verb instead, and gets HISTORY (the stream is persisted 30d/1GiB) plus
// kind/time filters. There is deliberately no --follow: the admin socket is one-shot by design
// (protocol.go), and the operator need is a point-in-time "what did the cluster just emit"; re-run
// with --since for a fresh window (broker-ops.md documents a poll-loop for a live tail).
func newAdminEventsCmd(socketPath *string) *cobra.Command {
	var (
		n      int
		since  time.Duration
		kind   string
		asJSON bool
	)
	cmd := &cobra.Command{
		Use:   "events",
		Short: "Tail the broker's operational sys.events (topology/rehome/proxy/session/disk kinds)",
		Long: `Read the H.1 'events' JetStream stream — the operator-facing sys.events feed
(session_created/destroyed, member_joined, agent_registered/evicted,
agent_roster_stale, disk_pressure, tetherd_restarted, and the operational
proxy_*/nats_topology_*/rehome/grow-cutover kinds). Reaches the broker over the
same root-only 0600 admin socket as 'admin runtime'/'alert raise'. The payloads
are secret-free by construction (never a PSK/token). Filter with --kind and
--since; -n bounds how many most-recent matching events to return.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			req := adminsock.Request{Op: adminsock.OpEvents, N: n, EventKind: kind}
			if since > 0 {
				req.Since = since.String()
			}
			resp, err := callAdmin(*socketPath, req)
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("admin: broker rejected: %s", resp.Error)
			}
			if asJSON {
				// R11 discipline: on success ONLY the machine JSON reaches stdout; the error paths
				// above return to main() (stderr), so `admin events --json 2>&1 | jq` stays clean.
				return emitJSON(cmd.OutOrStdout(), adminEventsJSON{Schema: "admin_events", SchemaVersion: 1, Events: normSlice(resp.Events), Truncated: resp.Truncated})
			}
			return renderAdminEvents(cmd, resp.Events, resp.Truncated)
		},
	}
	cmd.Flags().IntVarP(&n, "n", "n", 50, "number of most-recent (matching) events to tail")
	cmd.Flags().DurationVar(&since, "since", 0, "only events newer than this (e.g. 1h, 30m); 0 = no time bound")
	cmd.Flags().StringVar(&kind, "kind", "", "only events of this type (e.g. proxy_keyset_changed); empty = all kinds")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable machine JSON schema (default: human text)")
	return cmd
}

// renderAdminEvents prints the human table for `admin events`: TIME, TYPE, then the raw body JSON so
// the operator can inspect kind-specific fields without the CLI mirroring every event shape.
func renderAdminEvents(cmd *cobra.Command, events []adminsock.AuditEntry, truncated bool) error {
	out := cmd.OutOrStdout()
	if len(events) == 0 {
		if truncated {
			// A truncated EMPTY tail is not "no events" — the scan cap or request deadline was hit
			// before any match was found. Say so, so the operator narrows the query instead of
			// concluding the stream is empty (N-5).
			_, _ = fmt.Fprintln(out, "(no events returned — results truncated at the scan cap or request deadline; narrow with --since/--kind)")
			return nil
		}
		_, _ = fmt.Fprintln(out, "(no events)")
		return nil
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "TIME\tTYPE\tFIELDS")
	for _, e := range events {
		kind, _ := e.Body["type"].(string)
		if kind == "" {
			kind = "-"
		}
		body, _ := json.Marshal(e.Body)
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\n", e.Ts.Format(time.RFC3339), kind, string(body))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if truncated {
		// N-5: never hand back a silently-partial tail. This is result metadata on stdout, not an error.
		// %d is the number RETURNED; older messages beyond it were NOT examined (scan cap or deadline).
		_, _ = fmt.Fprintf(out, "(results truncated: showing the newest %d matching events; older messages "+
			"were NOT examined — the scan cap or request deadline was hit. Narrow with --since/--kind)\n", len(events))
	}
	return nil
}

func newAdminRuntimeCmd(socketPath *string) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "runtime",
		Short: "Show this broker's live process runtime (goroutines/threads/fds/rss/uptime + reconciler last-tick)",
		Long: `Runtime introspection for the LIVE broker daemon (R13). Reports the in-process
goroutine count (runtime.NumGoroutine — the real value, NOT the OS thread count,
which does not move when parked goroutines leak), the OS thread count, open fd
count, resident-set size, process uptime, and each periodic reconciler's last
tick. Diagnose a goroutine/fd leak by polling this and watching a count climb;
diagnose a stalled reconciler by watching its last_tick stop advancing.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpRuntime})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("admin: broker rejected: %s", resp.Error)
			}
			rep := resp.Runtime
			if rep == nil {
				return fmt.Errorf("broker: runtime reply missing")
			}
			if asJSON {
				// R11 discipline: on success ONLY the machine JSON reaches stdout (the failure
				// paths above return errors that main() prints to stderr and emit nothing here),
				// so `admin runtime --json 2>&1 | jq` never sees prose mixed into the JSON stream.
				return emitJSON(cmd.OutOrStdout(), rep)
			}
			return renderAdminRuntime(cmd, rep)
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable machine JSON schema (default: human text)")
	return cmd
}

// Runtime rendering lives in the CLI presentation leaf with its original rationale.
var renderAdminRuntime = infracli.RenderAdminRuntime

func newAdminSessionsCmd(socketPath *string) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "sessions",
		Short: "List all sessions in the broker SQLite",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpSessions})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("admin: broker rejected: %s", resp.Error)
			}
			if asJSON {
				return emitJSON(cmd.OutOrStdout(), adminSessionsJSON{Schema: "admin_sessions", SchemaVersion: 1, Sessions: normSlice(resp.Sessions)})
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "SID\tNAME\tSTATE\tOWNER\tCREATED")
			for _, s := range resp.Sessions {
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
					s.SID, s.Name, s.State, shortFP(s.OwnerFP), s.CreatedAt.Format(time.RFC3339))
			}
			if len(resp.Sessions) == 0 {
				_, _ = fmt.Fprintln(tw, "(no sessions)")
			}
			return tw.Flush()
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable machine JSON schema (default: human text)")
	return cmd
}

func newAdminNodesCmd(socketPath *string) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "nodes",
		Short: "List all nodes (sid, nid, status, heartbeat age, version)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpNodes})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("admin: broker rejected: %s", resp.Error)
			}
			if asJSON {
				return emitJSON(cmd.OutOrStdout(), adminNodesJSON{Schema: "admin_nodes", SchemaVersion: 1, Nodes: normSlice(resp.Nodes)})
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "SESSION\tNODE\tSTATE\tHEARTBEAT\tPROTO\tRELEASE")
			now := time.Now()
			for _, n := range resp.Nodes {
				age := "-"
				if !n.LastHeartbeatAt.IsZero() {
					age = humaneAge(now.Sub(n.LastHeartbeatAt))
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\n",
					n.SID, n.NID, n.Status, age, n.ProtoVersion, n.ReleaseVersion)
			}
			if len(resp.Nodes) == 0 {
				_, _ = fmt.Fprintln(tw, "(no nodes registered)")
			}
			return tw.Flush()
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable machine JSON schema (default: human text)")
	return cmd
}

func newAdminAuditCmd(socketPath *string) *cobra.Command {
	var n int
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "audit <sid>",
		Short: "Tail the per-session audit history (last N entries)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{
				Op:  adminsock.OpAudit,
				SID: args[0],
				N:   n,
			})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("admin: broker rejected: %s", resp.Error)
			}
			if asJSON {
				return emitJSON(cmd.OutOrStdout(), adminAuditJSON{Schema: "admin_audit", SchemaVersion: 1, Audit: normSlice(resp.Audit)})
			}
			for _, e := range resp.Audit {
				body, _ := json.Marshal(e.Body)
				_, _ = fmt.Fprintf(cmd.OutOrStdout(),
					"#%d  %s  %s  %s\n",
					e.Seq, e.Ts.Format(time.RFC3339), e.Subject, string(body))
			}
			if len(resp.Audit) == 0 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "(no audit entries)")
			}
			return nil
		},
	}
	cmd.Flags().IntVarP(&n, "n", "n", 50, "number of entries to tail (most recent)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable machine JSON schema (default: human text)")
	cmd.ValidArgsFunction = func(c *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		return cli.CompleteAdminSessions(*socketPath, toComplete)
	}
	return cmd
}

func newAdminEvictCmd(socketPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "evict <sid> <nid>",
		Short: "Remove an agent's provisioning + node row, broadcast eviction",
		Long: `Architecture P9: deletes agent_provisioning row + nodes row, then
broadcasts sys.events{type:agent_evicted}. A live agent subscribed
to sys.events shuts down within ~1s; an offline agent will be
denied at next CONNECT (no longer provisioned).`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{
				Op:  adminsock.OpEvict,
				SID: args[0],
				NID: args[1],
			})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return fmt.Errorf("admin: broker rejected: %s", resp.Error)
			}
			r := resp.Evict
			if r == nil {
				return fmt.Errorf("broker: evict reply missing")
			}
			// When neither row was present, evict is a no-op. Surface that
			// distinctly so operators don't mistake "(false false false)"
			// for a partial success.
			if !r.NodeRowDeleted && !r.AgentProvDeleted {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(),
					"nothing to evict: nid=%s not bound to sid=%s (no node row, no provisioning row)\n",
					r.NID, r.SID)
				return fmt.Errorf("not_found")
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"evicted sid=%s nid=%s (node=%v provisioning=%v broadcast=%v)\n",
				r.SID, r.NID, r.NodeRowDeleted, r.AgentProvDeleted, r.BroadcastedEvicted)
			return nil
		},
	}
	cmd.ValidArgsFunction = func(c *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		switch len(args) {
		case 0:
			return cli.CompleteAdminSessions(*socketPath, toComplete)
		case 1:
			return cli.CompleteAdminNodes(*socketPath, args[0], toComplete)
		default:
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
	}
	return cmd
}

// adminCallTimeout picks the admin-socket client read timeout for an op. Most admin ops reply promptly,
// so they take the client's 5s default (returned as 0). The SYNCHRONOUS `cluster drain` is the exception
// (external review M-6): it BLOCKS on the broker up to drainNotice (~30s) awaiting data-plane convergence,
// and a 5s client deadline would fire FIRST — surfacing that convergence wait as a bare socket timeout
// (exit 69, "broker unreachable") instead of the intended dataplane_not_converged (exit 75, retry-later).
// 60s clears drainNotice with margin for the convergence poll + the reply; `cluster retire` needs no bump
// because it starts a resumable op and returns immediately (its wait is a separate client-side poll).
func adminCallTimeout(op string) time.Duration {
	if op == adminsock.OpClusterDrain {
		return 60 * time.Second
	}
	return 0 // adminsock.Client default (5s)
}

// callAdmin is a var seam so tests can drive the admin CLI commands without a live socket (e.g. the
// R11 2>&1-cleanliness test for `admin runtime --json`). Production always uses the real client.
var callAdmin = func(socketPath string, req adminsock.Request) (*adminsock.Response, error) {
	c := &adminsock.Client{Path: socketPath, Timeout: adminCallTimeout(req.Op)}
	resp, err := c.Call(req)
	if err != nil {
		// Transport failure (socket missing / broker down / EOF) = service unavailable (69),
		// NOT an internal fault — a monitor must distinguish "broker is down" from "bad reply".
		return nil, unavailErr("admin socket %s: %w", socketPath, err)
	}
	return resp, nil
}

// shortFP truncates a SHA256 fp prefix for column display.
func shortFP(fp string) string {
	if len(fp) <= 16 {
		return fp
	}
	return fp[:16] + "…"
}

var humaneAge = infracli.HumaneAge

// newAdminSessionAllowCmd administers WHO MAY CREATE A SESSION.
//
// origin: prerelease audit round 2. `session create` used to admit anybody who could
// reach the control plane — which is the public internet by design — so becoming an
// "activated member", and from there a provisioned agent, cost a stranger two requests.
//
// On the ADMIN SOCKET, which is root-only 0600 on the broker host: admitting a
// fingerprint is an act of whoever runs the broker. On upgrade every fingerprint that
// already owns a session is admitted automatically — ONCE, by the broker at boot
// (seedSessionCreators), not by the migration, which is why a later upgrade cannot
// resurrect a revocation. So no existing deployment needs operator action; a FRESH broker
// starts empty and the first user is admitted here.
func newAdminSessionAllowCmd(socketPath *string) *cobra.Command {
	var list, remove bool
	var note string
	cmd := &cobra.Command{
		Use:   "session-allow [<fingerprint>]",
		Short: "Admit (or remove, or list) the identities allowed to create sessions",
		Long: `Only an admitted fingerprint may run 'tether session create'.

A user finds their own fingerprint with 'tether whoami' — and a user who tries
'tether session create' without being admitted is told it in the refusal, so
either way the string to paste here comes from them.

On upgrade every fingerprint that already owned a session was admitted
automatically, so an existing deployment needs nothing. A FRESH broker starts
with an EMPTY list: admit the first operator here, on the broker host.

Removing a fingerprint does NOT touch the sessions it already owns — revoking
the ability to create is not the same as deleting somebody's work.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// FLAG COMBINATIONS THAT DO NOTHING ARE USAGE ERRORS, not silent no-ops —
			// origin: increment 2 internal review, adminsock-cli/L10-F8. `--list --remove`
			// listed, and `--remove --note x` removed and dropped the note; in both cases
			// the operator's intent was half-executed without a word.
			if list && (remove || note != "") {
				return usageErr("admin session-allow --list takes neither --remove nor --note")
			}
			if remove && note != "" {
				return usageErr("admin session-allow --remove takes no --note (nothing survives the removal to carry it)")
			}
			switch {
			case list:
				if len(args) != 0 {
					return usageErr("admin session-allow --list takes no fingerprint")
				}
				resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpSessionCreators})
				if err != nil {
					return err
				}
				if resp.Error != "" {
					return fmt.Errorf("admin: broker rejected: %s", resp.Error)
				}
				out := cmd.OutOrStdout()
				if len(resp.Creators) == 0 {
					_, _ = fmt.Fprintln(out, "no identity may create a session on this broker.\n"+
						"Admit one with: tether admin session-allow <fingerprint>")
					return nil
				}
				for _, c := range resp.Creators {
					_, _ = fmt.Fprintf(out, "%s  added_at=%s by=%s", c.FP, c.AddedAt, c.AddedBy)
					if c.Note != "" {
						_, _ = fmt.Fprintf(out, "  note=%q", c.Note)
					}
					_, _ = fmt.Fprintln(out)
				}
				return nil
			case len(args) != 1:
				return usageErr("admin session-allow needs a fingerprint (or --list)")
			}
			op := adminsock.OpSessionAllow
			if remove {
				op = adminsock.OpSessionDeny
			}
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: op, FP: args[0], Note: note})
			if err != nil {
				return err
			}
			if !resp.OK {
				return fmt.Errorf("admin: broker rejected: %s", resp.Error)
			}
			// A QUALIFIED OUTCOME LEADS, it does not follow. The broker sets Error while
			// OK when it cannot honestly say "admitted"/"removed" — the fingerprint was
			// already there, or a replicated DELETE cannot report whether it deleted
			// anything. Printing "removed <fp>" and then a parenthetical that contradicts
			// it reads as success with a footnote, which is how a typo looked like a
			// revocation. origin: prerelease audit increment 2 internal review,
			// adminsock-cli/L10-F3 ≡ admission-product/L8-F6 ≡ admission-enforcement/L9-F9.
			if resp.Error != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", args[0], resp.Error)
				return nil
			}
			verb := "admitted"
			if remove {
				verb = "removed"
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s %s\n", verb, args[0])
			return nil
		},
	}
	cmd.Flags().BoolVar(&list, "list", false, "list the admitted fingerprints")
	cmd.Flags().BoolVar(&remove, "remove", false, "remove the fingerprint instead of admitting it")
	cmd.Flags().StringVar(&note, "note", "", "free text recorded alongside the admission")
	return cmd
}
