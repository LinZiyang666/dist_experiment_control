// proc_delivery_image.go — the courier outbox across a same-PID image
// switch (hot-update S2). The outbox is a plain retry table: exporting it
// is a full copy under c.mu, importing it is an install into an EMPTY
// courier before its delivery loop starts. Nothing is frozen: the courier
// keeps delivering until exec — an event delivered and cleared between
// export and exec is simply re-delivered once by the next image, which the
// broker handles idempotently (§22.4 item 2 ⑨). The per-pid ordering
// deliverRound derives from Parked/pending is carried verbatim.
// Package-level functions and a two-method adapter type on purpose: Agent's
// method budget (125) is full.
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §3.5.
package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/proto"
)

// CourierImage adapts the agent's courier to agentimage's exporter and
// importer; the courier itself stays private.
type CourierImage struct{ c *procCourier }

// NewCourierImage returns the agent's courier as an image domain (S3 wires
// it into agentimage.Owners / Target).
func NewCourierImage(a *Agent) CourierImage { return CourierImage{a.courier} }

// ExportOutbox copies every pending event with its retry bookkeeping.
func (ci CourierImage) ExportOutbox() (agentimage.CourierState, error) {
	return exportCourierState(ci.c), nil
}

// Check is the pure half of the import: every refusal ImportOutbox could
// make from the state alone, asked before any image domain is installed
// (S2 internal review I5).
func (ci CourierImage) Check(st agentimage.CourierState) error {
	_, err := decodeCourierState(st)
	return err
}

// ImportOutbox installs an exported outbox into an empty courier.
func (ci CourierImage) ImportOutbox(st agentimage.CourierState) error {
	return importCourierState(ci.c, st)
}

func exportCourierState(c *procCourier) agentimage.CourierState {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := agentimage.CourierState{Version: agentimage.CourierStateVersion}
	keys := make([]string, 0, len(c.pending))
	for key := range c.pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		ev := c.pending[key]
		out := agentimage.CourierEvent{PID: ev.pid, Kind: string(ev.kind), Payload: append(json.RawMessage(nil), ev.payload...),
			EnqueuedAtUnixNano: ev.enqueuedAt.UnixNano(), Fails: ev.fails, Silent: ev.silent, Parked: ev.parked, RC: ev.rc,
			StartTimeTicks: ev.startTimeTicks}
		if !ev.next.IsZero() {
			out.NextUnixNano = ev.next.UnixNano()
		}
		if !ev.endedAt.IsZero() {
			out.EndedAtUnixNano = ev.endedAt.UnixNano()
		}
		if !ev.startedAt.IsZero() {
			out.StartedAtUnixNano = ev.startedAt.UnixNano()
		}
		st.Events = append(st.Events, out)
	}
	return st
}

// importCourierState installs a decoded outbox into an empty courier.
// Backoff instants are reset to "now" (the wall clock may have moved under
// a long deferral, and the first round after a switch should retry
// everything once); fails/silent/parked keep their values so the
// old-broker signature and the per-pid ordering survive.
func importCourierState(c *procCourier, st agentimage.CourierState) error {
	pending, err := decodeCourierState(st)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) != 0 {
		return errors.New("courier import: outbox is not empty")
	}
	for key, ev := range pending {
		c.pending[key] = ev
	}
	return nil
}

// decodeCourierState validates the shape (again — the pure check is the
// importer's belt), then re-decodes every payload and checks it against the
// derived fields the register snapshot reads: a payload that says rc=3
// beside a row that says rc=4 would let the next register settle the wrong
// exit. It touches no courier, so Check can run it before any install.
func decodeCourierState(st agentimage.CourierState) (map[string]*pendingProcEvent, error) {
	if err := st.Validate(); err != nil {
		return nil, err
	}
	pending := make(map[string]*pendingProcEvent, len(st.Events))
	now := time.Now()
	for _, in := range st.Events {
		ev := &pendingProcEvent{pid: in.PID, kind: procEventKind(in.Kind), payload: append([]byte(nil), in.Payload...),
			enqueuedAt: time.Unix(0, in.EnqueuedAtUnixNano), next: now, fails: in.Fails, silent: in.Silent, parked: in.Parked,
			rc: in.RC, startTimeTicks: in.StartTimeTicks}
		if in.EndedAtUnixNano != 0 {
			ev.endedAt = time.Unix(0, in.EndedAtUnixNano).UTC()
		}
		if in.StartedAtUnixNano != 0 {
			ev.startedAt = time.Unix(0, in.StartedAtUnixNano).UTC()
		}
		switch ev.kind {
		case procEventStarted:
			var body proto.ProcStartedEvent
			if err := json.Unmarshal(ev.payload, &body); err != nil || body.PID != ev.pid ||
				body.StartTimeTicks != ev.startTimeTicks || !body.StartedAt.Equal(ev.startedAt) {
				return nil, fmt.Errorf("courier import: started event %s disagrees with its payload", ev.pid)
			}
		case procEventExit:
			var body proto.ProcExitEvent
			if err := json.Unmarshal(ev.payload, &body); err != nil || body.PID != ev.pid ||
				body.ExitCode != ev.rc || !body.EndedAt.Equal(ev.endedAt) {
				return nil, fmt.Errorf("courier import: exit event %s disagrees with its payload", ev.pid)
			}
		default:
			return nil, fmt.Errorf("courier import: unknown kind %q", in.Kind)
		}
		pending[courierKey(ev.pid, ev.kind)] = ev
	}
	return pending, nil
}
