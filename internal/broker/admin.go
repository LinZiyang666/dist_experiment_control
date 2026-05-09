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

