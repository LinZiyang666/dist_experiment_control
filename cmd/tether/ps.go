package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// psRequestTimeout is the wall-clock budget for one `tether ps` RPC.
// Default 15 s — long enough that a momentarily-busy broker (large
// SQLite scan after `tether ps -a` on a populous session) gets a
// chance to reply, short enough that an operator never sits there
// wondering whether the command hung. Override via TETHER_PS_TIMEOUT
// (Go time.Duration syntax — "200ms", "5s") for diagnostics or fast
// behavioral tests; invalid values are ignored and the default
// stands.
const defaultPsRequestTimeout = 15 * time.Second

func psRequestTimeout() time.Duration {
	if s := os.Getenv("TETHER_PS_TIMEOUT"); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			return d
		}
	}
	return defaultPsRequestTimeout
}

func newPsCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		showAll bool
		asJSON  bool
	)
	cmd := &cobra.Command{
		Use:   "ps",
		Short: "List managed processes in the active session",
		Long: `tether ps — list processes AND exposed ports in the active session
(TETHER_SESSION env or current_session file). Default view shows active
processes — both RUNNING (live) and LOST (RUNNING row whose owning
node is OFFLINE, derived at read time); pass -a to also include
EXITED processes. Architecture F.8 — unified view.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := connectCtl(cmd, "ps", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return err
			}
			defer nc.Close()

			// -a widens BOTH sections (h1 A1): EXITED processes and
			// FREED/REVOKED port history. The default reply is live-only —
			// FREED history is unbounded and once pushed the reply past
			// NATS max_payload.
			body, err := json.Marshal(proto.PsReq{IncludeExited: showAll, IncludeFreedPorts: showAll})
			if err != nil {
				return fmt.Errorf("ps: marshal req: %w", err)
			}
			timeout := psRequestTimeout()
			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()
			msg, err := nc.RequestWithContext(ctx,
				proto.SubjCtrlPs(id.PublicKey, sid), body)
			if err != nil {
				// Three distinct diagnostics: no-responder (broker
				// down / sub missing — NATS replies immediately),
				// timeout (broker alive but slow — deadline fired),
				// other (permission, conn closed, …). A blanket
				// "timed out" wrap misdirects operators on the
				// no-responder case.
				if errors.Is(err, nats.ErrNoResponders) {
					return fmt.Errorf("ps: no responders for %s: %w "+
						"(broker is not running, or its subscription "+
						"to ctrl.by.<actor>.s.<sid>.ps.req is gone — "+
						"check `tether serve` logs)",
						proto.SubjCtrlPs(id.PublicKey, sid), err)
				}
				if errors.Is(err, context.DeadlineExceeded) {
					// h1 A3: the old hint blamed "the processes table may be
					// unusually large" — provably wrong in the 2026-08-04
					// incident (the reply was oversize and silently dropped;
					// h1 made that a typed reply_too_large error instead).
					// A timeout here now means the broker never answered at
					// all: restarting, overloaded, or wedged.
					return fmt.Errorf("ps: request timed out after %s: %w "+
						"(the broker accepted the connection but never "+
						"replied — it may be restarting or overloaded; "+
						"check `tether alert ls` and the broker log, or "+
						"raise TETHER_PS_TIMEOUT)", timeout, err)
				}
				return fmt.Errorf("ps: request failed: %w", err)
			}
			var resp proto.PsResp
			if err := json.Unmarshal(msg.Data, &resp); err != nil {
				return err
			}
			if resp.Code != "" {
				return brokerErrorMessage("ps", resp.Code, resp.Error)
			}

			out := cmd.OutOrStdout()
			if asJSON {
				// Honor -a/showAll exactly as text (RUNNING/LOST by default, +EXITED with -a;
				// ALLOCATED ports by default). --json changes rendering, not selection.
				procs := make([]proto.PsEntry, 0, len(resp.Processes))
				for _, p := range resp.Processes {
					if showAll || p.Status == "RUNNING" || p.Status == "LOST" {
						procs = append(procs, p)
					}
				}
				ports := make([]proto.PsPortEntry, 0, len(resp.Ports))
				for _, p := range resp.Ports {
					if showAll || p.State == "ALLOCATED" {
						ports = append(ports, p)
					}
				}
				if err := emitJSON(out, psJSON{
					Schema: "ps", SchemaVersion: 1,
					Processes: normSlice(procs), Ports: normSlice(ports),
					ProcsTotal: resp.ProcsTotal, ProcsTruncated: resp.ProcsTruncated,
					PortsTotal: resp.PortsTotal, PortsTruncated: resp.PortsTruncated,
				}); err != nil {
					return err
				}
				withBanner(nc, id.PublicKey, true) // suppressed under --json
				return nil
			}

			now := time.Now()

			_, _ = fmt.Fprintln(out, "PROCESSES")
			tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "  PID\tNODE\tSTATE\tEXIT\tSTARTED\tCMD")
			anyProc := false
			for _, p := range resp.Processes {
				// Default view shows active processes — both
				// RUNNING (storage rows) and LOST (broker-derived
				// from RUNNING + OFFLINE node). `-a` adds EXITED.
				if !showAll && p.Status != "RUNNING" && p.Status != "LOST" {
					continue
				}
				anyProc = true
				exit := "-"
				if p.Status == "EXITED" {
					exit = fmt.Sprintf("%d", p.ExitCode)
				}
				_, _ = fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%s\n",
					p.PID, p.NID, p.Status, exit,
					humanizeAgo(now, p.StartedAt),
					argvToCmd(p.Argv))
			}
			if !anyProc {
				_, _ = fmt.Fprintln(tw, "  (none)")
			}
			if err := tw.Flush(); err != nil {
				return err
			}
			// h1 A1: a capped reply must SAY it is partial. The broker returns
			// at most its server cap (500) per section and reports the
			// uncapped total alongside.
			if resp.ProcsTruncated {
				_, _ = fmt.Fprintf(out, "  (truncated: %d of %d process rows returned — server cap)\n",
					len(resp.Processes), resp.ProcsTotal)
			}

			if err := renderPortsTable(out, resp.Ports, showAll, now); err != nil {
				return err
			}
			if resp.PortsTruncated {
				// origin: h1 external review F7. The earlier wording promised
				// "live rows are never omitted", which the 500-row cap cannot
				// guarantee: the live-first sort keeps history from displacing
				// live rows, but a session with MORE than the cap in ALLOCATED
				// rows (the 14000-14999 band allows it) still loses some. Say
				// what is actually true.
				_, _ = fmt.Fprintf(out, "  (truncated: %d of %d port rows returned — live rows are listed first; "+
					"a session with more than %d live rows can still have some omitted)\n",
					len(resp.Ports), resp.PortsTotal, len(resp.Ports))
			}
			withBanner(nc, id.PublicKey, false) // D8b §10.3: severe-alert banner to stderr (stdout stays parseable)
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable machine JSON schema (default: human text)")
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().BoolVarP(&showAll, "all", "a", false,
		"include EXITED processes (default: active only — RUNNING + LOST)")
	return cmd
}

// renderPortsTable renders the PORTS section of `tether ps`. B4: a HOME column appears ONLY when
// some shown port carries a home (cluster mode); a single broker leaves every HomeBroker empty so
// the table is byte-identical to pre-B4 (scripts parsing the columns are unaffected). Extracted
// so the byte-stability is golden-testable without the full NATS RunE.
func renderPortsTable(out io.Writer, ports []proto.PsPortEntry, showAll bool, now time.Time) error {
	showHome := false
	for _, p := range ports {
		if (p.State == "ALLOCATED" || showAll) && p.HomeBroker != "" {
			showHome = true
			break
		}
	}
	_, _ = fmt.Fprintln(out, "\nPORTS")
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	if showHome {
		_, _ = fmt.Fprintln(tw, "  NAME\tNODE\tLOCAL\tPUBLIC\tSTATE\tHOME\tCREATED")
	} else {
		_, _ = fmt.Fprintln(tw, "  NAME\tNODE\tLOCAL\tPUBLIC\tSTATE\tCREATED")
	}
	anyPort := false
	for _, p := range ports {
		if p.State != "ALLOCATED" && !showAll {
			continue
		}
		anyPort = true
		if showHome {
			_, _ = fmt.Fprintf(tw, "  %s\t%s\t:%d\t:%d\t%s\t%s\t%s\n",
				p.Name, p.NID, p.LocalPort, p.Port, p.State,
				psHomeCell(p), humanizeAgo(now, p.CreatedAt))
		} else {
			_, _ = fmt.Fprintf(tw, "  %s\t%s\t:%d\t:%d\t%s\t%s\n",
				p.Name, p.NID, p.LocalPort, p.Port, p.State,
				humanizeAgo(now, p.CreatedAt))
		}
	}
	if !anyPort {
		_, _ = fmt.Fprintln(tw, "  (none)")
	}
	return tw.Flush()
}

// psHomeCell renders the HOME column for one port (B4). Un-homed rows show "-"; a homed row
// shows the broker, and "(moved)" once it has failed over (epoch>0). The raw epoch stays out of
// the human table — it is available in `--json` and `expose explain`.
func psHomeCell(p proto.PsPortEntry) string {
	if p.HomeBroker == "" {
		return "-"
	}
	if p.Epoch > 0 {
		return p.HomeBroker + " (moved)"
	}
	return p.HomeBroker
}

func humanizeAgo(now, then time.Time) string {
	d := now.Sub(then)
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

func argvToCmd(argv []string) string {
	if len(argv) == 0 {
		return ""
	}
	out := argv[0]
	for _, a := range argv[1:] {
		out += " " + a
	}
	return out
}
