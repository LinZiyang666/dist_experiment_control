// Broker glue for the architecture I.2b admin Unix socket. The
// adminsock package owns the wire protocol + filesystem permissions;
// this file plugs the broker's SQLite + JetStream + sys.events
// publisher into the adminsock.Backend hooks.
package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/jsstream"
)

// adminAuditTail is the AuditTail hook for the admin endpoint:
// returns the last n messages from history-<sid> in time order
// (oldest → newest), each decoded into AuditEntry. Returns
// "history_unavailable" when JetStream isn't wired or the stream
// doesn't exist (e.g. for a sid the broker has never seen).
//
// We use a stream Info() to find the stream's first/last sequence,
// then GetMsg per sequence backwards from LastSeq. That keeps the
// implementation off OrderedConsumer (which has its own state +
// cleanup) and matches `tether history -n N` semantics: "the most
// recent N regardless of subject filter".
func (b *Broker) adminAuditTail(ctx context.Context, sid string, n int) ([]adminsock.AuditEntry, error) {
	if b.js == nil {
		return nil, fmt.Errorf("history_unavailable: broker has no JetStream")
	}
	if n <= 0 {
		n = 50
	}
	if n > adminTailMaxN {
		n = adminTailMaxN // L1: clamp an operator typo (`-n 999999999`) before it sizes a slice / a seq range
	}

	streamName := jsstream.HistoryStreamName(sid)
	stream, err := b.js.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("history_unavailable: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("history_unavailable: stream info: %w", err)
	}
	last := info.State.LastSeq
	first := info.State.FirstSeq
	if last == 0 || last < first {
		return nil, nil
	}

	want := uint64(n)
	startSeq := uint64(1)
	if last > want-1 {
		startSeq = last - want + 1
	}
	if startSeq < first {
		startSeq = first
	}

	out := make([]adminsock.AuditEntry, 0, last-startSeq+1)
	for seq := startSeq; seq <= last; seq++ {
		raw, err := stream.GetMsg(ctx, seq)
		if err != nil {
			// Gaps happen when retention drops a message between
			// our seq-range computation and the GetMsg. Skip and
			// keep going — the admin tail is best-effort.
			continue
		}
		var body map[string]any
		_ = json.Unmarshal(raw.Data, &body)
		out = append(out, adminsock.AuditEntry{
			Subject: raw.Subject,
			Seq:     seq,
			Ts:      raw.Time,
			Body:    body,
		})
	}
	// Defensive: stream order is monotonic by sequence, but if a
	// gap-skip reordered anything, sort by seq so the operator
	// sees a clean tail.
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// eventsMaxScan bounds how many messages adminEventsTail walks backward through the `events`
// stream when a filter (--kind/--since) is set. sys.events are operational (topology/rehome/
// proxy/session-lifecycle/disk) — low-frequency, not per-request — so 5000 covers a lot of
// history; the cap is a safety valve against a pathological high-volume stream, not the norm.
const eventsMaxScan = 5000

// adminTailMaxN clamps the operator-supplied -n on the admin audit/events tails at the
// server trust boundary (external review L1). The Unix socket is root/operator-only, so
// this is not a remote exploit, but a typo like `-n 999999999` would otherwise size a
// slice (and, for audit, a seq range) to that value and can OOM the live broker. The
// scans are already bounded to eventsMaxScan, so a larger request buys nothing.
const adminTailMaxN = eventsMaxScan

// adminEventsTail is the EventsTail hook (#30): the last n messages of the H.1 `events` stream,
// oldest → newest, each decoded into AuditEntry, optionally filtered to type==kind and to messages
// newer than now-since. Returns "events_unavailable" when JetStream isn't wired or the stream
// doesn't exist yet.
//
// Same Info()+GetMsg-backwards strategy as adminAuditTail (no OrderedConsumer state to clean up).
// With a filter we walk backward COLLECTING up to n matches (so `--kind proxy_keyset_changed`
// returns the most recent n of THAT kind, not "the matches among the last n"), stopping at the
// since cutoff, at FirstSeq, or after eventsMaxScan messages. The events stream is secret-free by
// construction of its producers (rehome_events.go: hand-built allow-listed scalar maps); this
// reader adds no field, so it cannot introduce one.
// The returned bool is TRUNCATED (external review N-5): true when the walk stopped EARLY — at the
// eventsMaxScan cap, or because the request ctx deadline/cancel fired mid-scan — so older matching
// messages were NOT examined and the tail is a partial answer, not a complete one. It is deliberately
// false for the two COMPLETE stops (the --since cutoff and collecting n matches): those return every
// message the request asked for. Callers surface it so an operator is never handed a silently-partial
// tail. Truncation is NOT an error (a partial answer with a marker beats breaking every busy-stream
// script), so err stays nil in the truncated case.
func (b *Broker) adminEventsTail(ctx context.Context, n int, since time.Duration, kind string) ([]adminsock.AuditEntry, bool, error) {
	if b.js == nil {
		return nil, false, fmt.Errorf("events_unavailable: broker has no JetStream")
	}
	if n <= 0 {
		n = 50
	}
	if n > adminTailMaxN {
		n = adminTailMaxN // L1: clamp before `make([]…, 0, n)` — an operator typo must not size the alloc
	}

	stream, err := b.js.Stream(ctx, jsstream.EventsStreamName)
	if err != nil {
		return nil, false, fmt.Errorf("events_unavailable: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("events_unavailable: stream info: %w", err)
	}
	last := info.State.LastSeq
	first := info.State.FirstSeq
	if last == 0 || last < first {
		return nil, false, nil
	}

	var cutoff time.Time
	if since > 0 {
		cutoff = b.cfg.Now().Add(-since)
	}

	out := make([]adminsock.AuditEntry, 0, n)
	scanned := 0
	truncated := false
	for seq := last; seq >= first; seq-- {
		if scanned >= eventsMaxScan {
			truncated = true // hit the scan cap before a natural stop — older messages were never examined
			break
		}
		scanned++
		raw, err := stream.GetMsg(ctx, seq)
		if err != nil {
			if ctx.Err() != nil {
				// The request deadline/cancel fired MID-scan (a pre-cancelled ctx instead errors earlier at
				// js.Stream/Info and never reaches this loop): every remaining GetMsg would fail too, so stop
				// rather than burn the scan budget, and report the tail as PARTIAL instead of the silent
				// partial the old unconditional `continue` produced. NOTE: this mid-scan branch has no
				// hermetic unit test — a pre-cancelled ctx is caught at Stream/Info (the only structurally
				// buildable fixture), and a genuine mid-loop cancel is timing-fragile against embedded JS;
				// recorded as infeasible-as-specced (release-readiness-followups-plan §3 C1), like the A2 case.
				truncated = true
				break
			}
			// Benign retention/compaction gap between the Info() range and this GetMsg — skip and keep
			// going (the admin tail is best-effort; mirrors adminAuditTail).
			continue
		}
		// The stream is time-ordered by sequence, so once we cross below the cutoff every
		// earlier message is older too — stop the backward walk. This is a COMPLETE stop (we
		// returned every message inside the --since window), not truncation.
		if !cutoff.IsZero() && raw.Time.Before(cutoff) {
			break
		}
		var body map[string]any
		_ = json.Unmarshal(raw.Data, &body)
		if kind != "" {
			t, _ := body["type"].(string)
			if t != kind {
				continue
			}
		}
		out = append(out, adminsock.AuditEntry{
			Subject: raw.Subject,
			Seq:     seq,
			Ts:      raw.Time,
			Body:    body,
		})
		if len(out) >= n {
			break // COMPLETE: collected the n most-recent matches the request asked for.
		}
	}
	// We collected newest → oldest; present oldest → newest like adminAuditTail.
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, truncated, nil
}

// pubAgentEvicted is the PubAgentEvicted hook for the admin
// endpoint. Pubs sys.events{type:agent_evicted, sid, nid} so a
// live agent subscribing to sys.events can self-shutdown within
// the architecture P9 "1s 内下线" budget. Best-effort: failure to
// publish doesn't fail the underlying admin call (the SQLite rows
// are already gone; agent will at worst notice on next reconnect
// when the broker rejects its CONNECT).
func (b *Broker) pubAgentEvicted(sid, nid string) {
	b.pubSysEvent("agent_evicted", map[string]any{
		"sid": sid, "nid": nid,
	})
}
