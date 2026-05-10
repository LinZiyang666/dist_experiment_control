package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"
)

// newHistoryCmd implements `tether history` (architecture H.2 / P7).
//
// Replays audit messages from the active session's history-<sid>
// JetStream stream via an EPHEMERAL consumer. Shapes:
//
//   tether history             # all entries from oldest, then exit
//   tether history -n 50       # last 50 entries
//   tether history --follow    # tail (stream new audit msgs as they land)
//   tether history --kind call # filter to call|proc|port
//
// All consumers are ephemeral per H.2 — Ctrl-C disposes them. Owners
// who need durable replay can build their own NATS consumer manually;
// v1 CLI doesn't expose --durable.
func newHistoryCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		lastN   int
		follow  bool
		kind    string
	)
	cmd := &cobra.Command{
		Use:   "history",
		Short: "Replay audit history for the active session",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			if kind != "" && kind != "call" && kind != "proc" && kind != "port" {
				return fmt.Errorf("--kind must be one of: call | proc | port (got %q)", kind)
			}

			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return connectError("history", natsURL, err)
			}
			defer nc.Close()

			js, err := jetstream.New(nc)
			if err != nil {
				return fmt.Errorf("history: jetstream: %w", err)
			}

			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			streamName := jsstream.HistoryStreamName(sid)
			stream, err := js.Stream(ctx, streamName)
			if err != nil {
				return fmt.Errorf("history: stream %s: %w", streamName, err)
			}

			cfg := jetstream.OrderedConsumerConfig{}
			if kind != "" {
				cfg.FilterSubjects = []string{
					fmt.Sprintf("%s.s.%s.audit.%s", proto.SubjectPrefix, sid, kind),
				}
			}

			out := cmd.OutOrStdout()
			switch {
			case follow:
				cfg.DeliverPolicy = jetstream.DeliverNewPolicy
				cons, err := stream.OrderedConsumer(ctx, cfg)
				if err != nil {
					return fmt.Errorf("history: consumer: %w", err)
				}
				return runHistoryFollow(ctx, cons, out)

			case lastN > 0 && kind == "":
				// No filter — the LastSeq - N + 1 short-cut is
				// safe because every stream message counts.
				info, err := stream.Info(ctx)
				if err != nil {
					return fmt.Errorf("history: stream info: %w", err)
				}
				start := uint64(1)
				if info.State.Msgs > uint64(lastN) {
					start = info.State.LastSeq - uint64(lastN) + 1
				}
				cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
				cfg.OptStartSeq = start
				cons, err := stream.OrderedConsumer(ctx, cfg)
				if err != nil {
					return fmt.Errorf("history: consumer: %w", err)
				}
				return runHistorySnapshot(ctx, cons, out, 250*time.Millisecond)

			case lastN > 0:
				// Filter present — must scan the FILTERED stream from
				// start and ring-buffer the last N matches; the
				// unfiltered LastSeq doesn't tell us where the
				// Nth-from-last filtered match begins.
				cfg.DeliverPolicy = jetstream.DeliverAllPolicy
				cons, err := stream.OrderedConsumer(ctx, cfg)
				if err != nil {
					return fmt.Errorf("history: consumer: %w", err)
				}
				return runHistoryFilteredTail(ctx, cons, out, lastN, 250*time.Millisecond)

			default:
				// No -n, no --follow → replay everything matching.
				cons, err := stream.OrderedConsumer(ctx, cfg)
				if err != nil {
					return fmt.Errorf("history: consumer: %w", err)
				}
				return runHistorySnapshot(ctx, cons, out, 250*time.Millisecond)
			}
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().IntVarP(&lastN, "lines", "n", 0,
		"only show the last N entries (0 = from oldest); ignored with --follow")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false,
		"after printing the snapshot, keep printing new audit msgs as they land (Ctrl-C to stop)")
	cmd.Flags().StringVar(&kind, "kind", "",
		"filter by audit kind: call | proc | port (default: all)")
	return cmd
}

// runHistorySnapshot reads until a contiguous idle window (no new
// msgs for `idle`) signals "snapshot end". This avoids the seq-stop
// trap when the consumer has a subject filter (the seq numbers it
// sees are sparse and don't approach the stream's LastSeq).
func runHistorySnapshot(ctx context.Context, cons jetstream.Consumer, out io.Writer, idle time.Duration) error {
	it, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("history: messages: %w", err)
	}
	defer it.Stop()

	type item struct {
		subject string
		data    []byte
	}
	ch := make(chan item, 16)
	doneCh := make(chan error, 1)
	go func() {
		for {
			msg, err := it.Next()
			if err != nil {
				doneCh <- err
				close(ch)
				return
			}
			b := make([]byte, len(msg.Data()))
			copy(b, msg.Data())
			ch <- item{subject: msg.Subject(), data: b}
		}
	}()

	idleT := time.NewTimer(idle)
	defer idleT.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case it, ok := <-ch:
			if !ok {
				return nil
			}
			printAuditEntry(out, it.subject, it.data)
			if !idleT.Stop() {
				select {
				case <-idleT.C:
				default:
				}
			}
			idleT.Reset(idle)
		case <-idleT.C:
			return nil
		}
	}
}

// runHistoryFilteredTail walks every filtered message from the
// stream's beginning, keeps a ring buffer of the last n, and prints
// them in arrival order on idle (or end-of-stream).
//
// Why a ring buffer: when --kind is present the consumer's
// FilterSubjects yields only matching messages, but their seq numbers
// are sparse (call/proc/port interleave on a single stream). The
// "OptStartSeq = LastSeq - N + 1" short-cut over-truncates because
// filtered messages between [LastSeq-N+1, LastSeq] are fewer than N
// (e.g. with the typical 1 call : 2 proc ratio per exec, asking for
// N=100 inside the last 100 stream messages yields only ~33 calls).
//
// Memory cost is O(n) on the ring + O(1) per message scanned. For
// v1 expected volumes (history-<sid> in the thousands of messages,
// n typically <= a few hundred), the unfiltered scan is comfortable.
// Architectures expecting millions of pre-filter messages would want
// a JetStream subject-specific seq API instead — not in v1.
func runHistoryFilteredTail(ctx context.Context, cons jetstream.Consumer, out io.Writer, n int, idle time.Duration) error {
	it, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("history: messages: %w", err)
	}
	defer it.Stop()

	type item struct{ subject string; data []byte }
	ring := make([]item, 0, n)
	add := func(it item) {
		if len(ring) < n {
			ring = append(ring, it)
			return
		}
		// Slide-and-overwrite: the oldest goes, the newest takes
		// the tail. Cheaper than a true ring buffer for n in the
		// CLI-typical range and easier to read.
		copy(ring, ring[1:])
		ring[len(ring)-1] = it
	}

	ch := make(chan item, 16)
	doneCh := make(chan struct{})
	go func() {
		for {
			msg, err := it.Next()
			if err != nil {
				close(doneCh)
				return
			}
			b := make([]byte, len(msg.Data()))
			copy(b, msg.Data())
			ch <- item{subject: msg.Subject(), data: b}
		}
	}()

	idleT := time.NewTimer(idle)
	defer idleT.Stop()
	flush := func() {
		for _, it := range ring {
			printAuditEntry(out, it.subject, it.data)
		}
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return ctx.Err()
		case it := <-ch:
			add(it)
			if !idleT.Stop() {
				select {
				case <-idleT.C:
				default:
				}
			}
			idleT.Reset(idle)
		case <-idleT.C:
			flush()
			return nil
		case <-doneCh:
			flush()
			return nil
		}
	}
}

// runHistoryFollow keeps printing as new audit msgs arrive. Returns
// when ctx ends (Ctrl-C). Doesn't fall back to the snapshot's idle
// trick — follow is explicitly indefinite.
func runHistoryFollow(ctx context.Context, cons jetstream.Consumer, out io.Writer) error {
	it, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("history: messages: %w", err)
	}
	defer it.Stop()

	go func() {
		<-ctx.Done()
		it.Stop()
	}()

	for {
		msg, err := it.Next()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		printAuditEntry(out, msg.Subject(), msg.Data())
	}
}

// printAuditEntry prints one audit message as one line, choosing
// fields per the kind. Pretty-printing intentionally minimal — the
// goal is grep-friendly, not a full audit reader. The full JSON is
// available by `tether history --kind call | jq .` style pipes.
func printAuditEntry(w io.Writer, subject string, data []byte) {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		_, _ = fmt.Fprintf(w, "[malformed audit: %v] %s\n", err, string(data))
		return
	}
	kind := strFromMap(raw, "kind")
	auditKind := lastSubjectToken(subject) // "call" / "proc" / "port"
	ts := strFromMap(raw, "ts")
	if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		ts = t.Local().Format("15:04:05")
	}
	switch auditKind {
	case "call":
		_, _ = fmt.Fprintf(w, "%s  CALL  %s/%s  fp=%s  verb=%s  ok=%v  %s\n",
			ts, strFromMap(raw, "session"), strFromMap(raw, "node"),
			short(strFromMap(raw, "actor_fp")), strFromMap(raw, "verb"),
			raw["ok"], strFromMap(raw, "error"))
	case "proc":
		_, _ = fmt.Fprintf(w, "%s  PROC  %s/%s  pid=%s  kind=%s  rc=%v  %s\n",
			ts, strFromMap(raw, "session"), strFromMap(raw, "node"),
			short(strFromMap(raw, "pid")), kind, raw["rc"],
			strFromMap(raw, "cmd"))
	case "port":
		_, _ = fmt.Fprintf(w, "%s  PORT  %s/%s  port=%v  name=%s  kind=%s\n",
			ts, strFromMap(raw, "session"), strFromMap(raw, "node"),
			raw["port"], strFromMap(raw, "name"), kind)
	default:
		_, _ = fmt.Fprintf(w, "%s  %s  %s\n", ts, auditKind, string(data))
	}
}

func strFromMap(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func short(fp string) string {
	if strings.HasPrefix(fp, "SHA256:") && len(fp) > 14 {
		return fp[:14] + "…"
	}
	return fp
}

func lastSubjectToken(subj string) string {
	parts := strings.Split(subj, ".")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}
