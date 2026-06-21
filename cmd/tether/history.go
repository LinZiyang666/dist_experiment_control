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

// historyBounds is the snapshot bounds for a non-follow `tether
// history`, derived purely from the stream's state. Factored out of
// newHistoryCmd because the arithmetic here (the -n start sequence and
// the known/pending mapping) is the part most prone to regress, and it
// is unit-testable without standing up NATS.
type historyBounds struct {
	startSeq    uint64 // OptStartSeq for the -n unfiltered consumer
	useStartSeq bool   // true → DeliverByStartSequence at startSeq
	known       bool   // true → pending bounds the read deterministically
	pending     uint64 // backlog upper bound (only consulted when known)
}

// historyReplayBounds maps (kind, lastN) + stream state to the replay
// bounds. known is true for the unfiltered paths (every stream message
// is delivered) and for any totally-empty stream; it is false for a
// non-empty --kind filter (matching count can't be read up front, so
// the drain leans on per-message NumPending + the rolling idle guard).
func historyReplayBounds(kind string, lastN int, streamMsgs, lastSeq uint64) historyBounds {
	known := kind == "" || streamMsgs == 0
	switch {
	case lastN > 0 && kind == "":
		start := uint64(1)
		pending := streamMsgs
		if streamMsgs > uint64(lastN) {
			start = lastSeq - uint64(lastN) + 1
			pending = uint64(lastN)
		}
		return historyBounds{startSeq: start, useStartSeq: true, known: known, pending: pending}
	case lastN > 0:
		// filtered -n: ring-buffer the last n matching; size unknown.
		return historyBounds{known: known, pending: 0}
	default:
		// full, or filtered without -n.
		return historyBounds{known: known, pending: streamMsgs}
	}
}

// newHistoryCmd implements `tether history` (architecture H.2 / P7).
//
// Replays audit messages from the active session's history-<sid>
// JetStream stream via an EPHEMERAL consumer. Shapes:
//
//	tether history             # all entries from oldest, then exit
//	tether history -n 50       # last 50 entries
//	tether history --follow    # tail (stream new audit msgs as they land)
//	tether history --kind call # filter to call|proc|port
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
			if kind != "" && kind != "call" && kind != "proc" && kind != "port" && kind != "transfer" {
				return fmt.Errorf("--kind must be one of: call | proc | port | transfer (got %q)", kind)
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

			if follow {
				cfg.DeliverPolicy = jetstream.DeliverNewPolicy
				cons, err := stream.OrderedConsumer(ctx, cfg)
				if err != nil {
					return fmt.Errorf("history: consumer: %w", err)
				}
				return runHistoryFollow(ctx, cons, out)
			}

			// Backlog size comes from the STREAM, never from the ordered
			// consumer: orderedConsumer.Info() returns
			// ErrOrderedConsumerNotCreated before the first read, so it
			// cannot be used to detect an empty backlog up front. The
			// per-message NumPending (filter-aware) drives the drain stop
			// once messages flow; this stream count only tells us up front
			// whether there is anything to replay and bounds the read.
			info, err := stream.Info(ctx)
			if err != nil {
				return fmt.Errorf("history: stream info: %w", err)
			}
			b := historyReplayBounds(kind, lastN, info.State.Msgs, info.State.LastSeq)

			switch {
			case b.useStartSeq:
				// -n, no filter: the LastSeq - N + 1 short-cut is safe
				// because every stream message counts.
				cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
				cfg.OptStartSeq = b.startSeq
				cons, err := stream.OrderedConsumer(ctx, cfg)
				if err != nil {
					return fmt.Errorf("history: consumer: %w", err)
				}
				return runHistorySnapshot(ctx, cons, out, b.known, b.pending)

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
				return runHistoryFilteredTail(ctx, cons, out, lastN, b.known, b.pending)

			default:
				// No -n, no --follow → replay everything matching.
				cons, err := stream.OrderedConsumer(ctx, cfg)
				if err != nil {
					return fmt.Errorf("history: consumer: %w", err)
				}
				return runHistorySnapshot(ctx, cons, out, b.known, b.pending)
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
		"filter by audit kind: call | proc | port | transfer (default: all)")
	return cmd
}

// histItem is one audit message handed from the reader goroutine to
// the drain loop. `drained` is true when JetStream reported this as
// the last not-yet-delivered message matching the consumer's filter
// (NumPending == 0) — the authoritative "snapshot complete" signal.
type histItem struct {
	subject string
	data    []byte
	drained bool
}

// historyFirstMsgGrace is the idleGuard's quiet-window: how long a
// drain tolerates NO delivery before acting on the silence. It is the
// anti-hang backstop, never the completion signal, so it is
// deliberately generous (>> realistic broker RTT) — a slow first
// message from a far/WSS broker is never mistaken for end-of-backlog.
// On the fast (common) path the per-message drained flag or the known
// count bound completes the replay long before this fires; the grace
// only matters when delivery genuinely stalls, where it ends an
// unknown-size scan as drained or marks a known-size replay incomplete.
const historyFirstMsgGrace = 5 * time.Second

// feedHistory is the single reader goroutine: it pumps messages off
// the consumer onto ch, tagging each with whether it drained the
// backlog (NumPending == 0). It selects on ctx.Done() when sending so
// it can never leak if the drain loop has already returned and stopped
// reading ch (the caller cancels its derived ctx on return).
func feedHistory(ctx context.Context, it jetstream.MessagesContext, ch chan<- histItem) {
	defer close(ch)
	for {
		msg, err := it.Next()
		if err != nil {
			return
		}
		b := make([]byte, len(msg.Data()))
		copy(b, msg.Data())
		drained := false
		if meta, e := msg.Metadata(); e == nil && meta != nil && meta.NumPending == 0 {
			drained = true
		}
		select {
		case ch <- histItem{subject: msg.Subject(), data: b, drained: drained}:
		case <-ctx.Done():
			return
		}
	}
}

// idleGuard is the anti-hang backstop for both drain loops: a rolling
// "no item for `grace`" timer. The FAST completion signals are the
// per-message drained flag and (when the size is known) the count
// bound; the idleGuard only fires when neither has, i.e. delivery has
// gone quiet for `grace`. Two cases need it:
//   - UNKNOWN size (--kind: matching count can't be read up front) on a
//     live stream whose per-filter NumPending never settles to 0 — the
//     idle is the terminator once it quiesces.
//   - KNOWN size where delivery STALLS mid-backlog (a broker/tunnel
//     reconnect, or a stream that shrank under us) so neither drained
//     nor seen>=pending is ever reached — without the idle the loop
//     would wait on ctx (Ctrl-C) forever; the old code's 250ms idle
//     used to bound exactly this.
//
// `grace` is deliberately generous (>> realistic broker RTT), so before
// the first item it doubles as a first-message guard that a slow remote
// first message can't trip, and it never truncates a backlog that is
// still actively flowing (the timer is rolled on every item). It is
// always armed; the empty-backlog case is short-circuited by the caller
// (known && pending==0) before the drain runs, so an empty stream never
// waits on it.
type idleGuard struct {
	t     *time.Timer
	grace time.Duration
}

func newIdleGuard(grace time.Duration) idleGuard {
	return idleGuard{t: time.NewTimer(grace), grace: grace}
}

func (g *idleGuard) C() <-chan time.Time {
	if g.t == nil {
		return nil
	}
	return g.t.C
}

func (g *idleGuard) reset() {
	if g.t == nil {
		return
	}
	if !g.t.Stop() {
		select {
		case <-g.t.C:
		default:
		}
	}
	g.t.Reset(g.grace)
}

func (g *idleGuard) stop() {
	if g.t != nil {
		g.t.Stop()
	}
}

// tryRecvHist non-blockingly pulls one buffered item. Used when the
// idle guard fires to honor an item that raced in just ahead of the
// timer before declaring the backlog drained — select picks a ready
// case at random, so without this peek a buffered first/next item
// could be silently dropped. ok=false covers both "nothing buffered"
// and "channel closed"; either way the caller stops.
func tryRecvHist(ch <-chan histItem) (histItem, bool) {
	select {
	case it, ok := <-ch:
		return it, ok
	default:
		return histItem{}, false
	}
}

// errHistoryIncomplete reports that a KNOWN-size replay ended before
// reaching its deterministic bound — delivery stalled (broker/tunnel
// reconnect, or the iterator errored) after `seen` of `pending`
// entries. We surface this as a non-nil error rather than returning a
// silent partial/empty snapshot: a truncated known replay is exactly
// the user-visible failure this fix exists to remove, so it must never
// masquerade as success.
func errHistoryIncomplete(seen, pending uint64) error {
	return fmt.Errorf("history: incomplete replay — delivered %d of %d entries before the broker went quiet; retry, or use --follow", seen, pending)
}

// runHistorySnapshot replays the consumer's existing backlog and exits
// when it is drained. Completion is decided by JetStream's filter-aware
// NumPending and (for a known backlog) the stream-derived count — NOT a
// wall-clock idle window, which the old "250ms of quiet == done"
// heuristic used and which truncated replays against any broker whose
// first-message latency exceeded the window. The always-armed idleGuard
// is only an anti-hang backstop (see its doc), and for a known backlog
// a stall before the bound is reported as an error, never silent
// success.
//
// known/pending come from the STREAM (see newHistoryCmd), not the
// ordered consumer, whose Info() is unusable before the first read.
// When known and the backlog is empty we return without touching the
// consumer at all.
func runHistorySnapshot(ctx context.Context, cons jetstream.Consumer, out io.Writer, known bool, pending uint64) error {
	if known && pending == 0 {
		return nil // empty backlog — nothing to replay
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	it, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("history: messages: %w", err)
	}
	defer it.Stop()

	ch := make(chan histItem, 16)
	go feedHistory(cctx, it, ch)
	return drainHistorySnapshot(cctx, ch, out, known, pending, historyFirstMsgGrace)
}

// drainHistorySnapshot is the testable core of runHistorySnapshot. It
// prints histItems until the backlog is complete, then returns:
//   - SUCCESS (nil) when a `drained` item arrives (per-message
//     NumPending == 0), OR `seen` reaches a KNOWN `pending` bound, OR —
//     for an UNKNOWN backlog only — delivery quiesces / the consumer
//     ends (the natural end of a --kind scan whose count we can't know).
//   - INCOMPLETE (errHistoryIncomplete) when a KNOWN backlog stops short
//     of its bound (idle stall or consumer end before seen>=pending):
//     never a silent partial success.
//   - ctx.Err() on cancellation.
//
// The idleGuard is an always-armed anti-hang backstop (see its doc); it
// is never the success path for a known backlog.
func drainHistorySnapshot(ctx context.Context, ch <-chan histItem, out io.Writer, known bool, pending uint64, grace time.Duration) error {
	idle := newIdleGuard(grace)
	defer idle.stop()
	var seen uint64
	// complete reports whether a just-processed item finishes the
	// backlog: the per-message drained flag, or the known count bound.
	complete := func(it histItem) bool { return it.drained || (known && seen >= pending) }
	// endShort maps a non-cancellation termination to success/incomplete:
	// a known backlog that stopped before its bound is incomplete.
	endShort := func() error {
		if known && seen < pending {
			return errHistoryIncomplete(seen, pending)
		}
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-idle.C():
			// Delivery went quiet for `grace`. Honor an item that raced
			// in ahead of the timer before deciding the outcome.
			if it, ok := tryRecvHist(ch); ok {
				printAuditEntry(out, it.subject, it.data)
				seen++
				if complete(it) {
					return nil
				}
				idle.reset()
				continue
			}
			return endShort()
		case it, ok := <-ch:
			if !ok {
				return endShort()
			}
			printAuditEntry(out, it.subject, it.data)
			seen++
			if complete(it) {
				return nil
			}
			idle.reset()
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
func runHistoryFilteredTail(ctx context.Context, cons jetstream.Consumer, out io.Writer, n int, known bool, pending uint64) error {
	if known && pending == 0 {
		return nil // no matching backlog — nothing to replay
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	it, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("history: messages: %w", err)
	}
	defer it.Stop()

	ch := make(chan histItem, 16)
	go feedHistory(cctx, it, ch)
	return drainHistoryFilteredTail(cctx, ch, out, n, known, pending, historyFirstMsgGrace)
}

// drainHistoryFilteredTail is the testable core of runHistoryFilteredTail.
// Same completion contract as drainHistorySnapshot (per-message drained
// / known count bound / unknown-quiesce success / known-stall →
// incomplete error / ctx.Err) but it ring-buffers the last n matching
// items and flushes them on every return — including the incomplete
// case, so the user sees what did arrive alongside the error. The real
// --kind path is always unknown-size, so the rolling idleGuard is what
// bounds a live-but-quiescing filtered stream here.
func drainHistoryFilteredTail(ctx context.Context, ch <-chan histItem, out io.Writer, n int, known bool, pending uint64, grace time.Duration) error {
	ring := make([]histItem, 0, n)
	add := func(it histItem) {
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
	flush := func() {
		for _, it := range ring {
			printAuditEntry(out, it.subject, it.data)
		}
	}

	idle := newIdleGuard(grace)
	defer idle.stop()
	var seen uint64
	complete := func(it histItem) bool { return it.drained || (known && seen >= pending) }
	endShort := func() error {
		flush()
		if known && seen < pending {
			return errHistoryIncomplete(seen, pending)
		}
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return ctx.Err()
		case <-idle.C():
			if it, ok := tryRecvHist(ch); ok {
				add(it)
				seen++
				if complete(it) {
					flush()
					return nil
				}
				idle.reset()
				continue
			}
			return endShort()
		case it, ok := <-ch:
			if !ok {
				return endShort()
			}
			add(it)
			seen++
			if complete(it) {
				flush()
				return nil
			}
			idle.reset()
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

	// cctx lets the watcher goroutine exit on a NON-ctx Next() error
	// (heartbeat miss, connection drop) too — without it the watcher
	// would block on the parent ctx until process exit.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-cctx.Done()
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
	case "transfer":
		_, _ = fmt.Fprintf(w, "%s  XFER  %s/%s  verb=%s  kind=%s  tier=%s  bytes=%v  path=%s  %s\n",
			ts, strFromMap(raw, "session"), strFromMap(raw, "node"),
			strFromMap(raw, "verb"), kind, strFromMap(raw, "tier"),
			raw["bytes"], strFromMap(raw, "path"), strFromMap(raw, "error"))
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
