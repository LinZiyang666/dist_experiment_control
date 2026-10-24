// courier.go — the proc-event courier's outbox as bundle data.
//
// The courier lives in internal/agent (an unexported type); this package
// defines only the wire-neutral state it exports and the pure validation an
// importer runs before touching the courier. The two adapters
// (CourierExporter/CourierImporter) are implemented in internal/agent.
package agentimage

import (
	"encoding/json"
	"fmt"

	"github.com/LinZiyang666/tether/internal/procrequest"
)

const (
	// CourierStateVersion is the schema epoch of CourierState.
	CourierStateVersion = 1
	// MaxCourierEvents bounds the outbox (internal/agent pins it equal to
	// its own courierMaxPending).
	MaxCourierEvents = 4096
	// MaxCourierBytes bounds the total payload bytes carried.
	MaxCourierBytes = 8 << 20
)

// Courier event kinds, as the courier names them on the wire subject.
const (
	CourierEventStarted = "started"
	CourierEventExit    = "exit"
)

// CourierEvent is one pending proc.started / proc.exit delivery with its
// retry bookkeeping. Payload is the exact wire body; the derived fields are
// what the register snapshot and receipt logic read without decoding it.
type CourierEvent struct {
	PID                string          `json:"pid"`
	Kind               string          `json:"kind"`
	Payload            json.RawMessage `json:"payload"`
	EnqueuedAtUnixNano int64           `json:"enqueued_at_unix_nano"`
	NextUnixNano       int64           `json:"next_unix_nano,omitempty"`
	Fails              int             `json:"fails,omitempty"`
	Silent             int             `json:"silent,omitempty"`
	Parked             bool            `json:"parked,omitempty"`
	RC                 int             `json:"rc,omitempty"`
	EndedAtUnixNano    int64           `json:"ended_at_unix_nano,omitempty"`
	StartedAtUnixNano  int64           `json:"started_at_unix_nano,omitempty"`
	StartTimeTicks     int64           `json:"start_time_ticks,omitempty"`
}

// CourierState is the exported outbox.
type CourierState struct {
	Version int            `json:"version"`
	Events  []CourierEvent `json:"events,omitempty"`
}

// CourierExporter is implemented by the agent's courier.
type CourierExporter interface {
	ExportOutbox() (CourierState, error)
}

// CourierImporter is implemented by the agent's courier. Check is the pure
// half (payload/field agreement) asked before anything is installed;
// ImportOutbox installs into an empty outbox.
type CourierImporter interface {
	Check(CourierState) error
	ImportOutbox(CourierState) error
}

// MaxCourierSilent is the most consecutive silent (connected-timeout)
// attempts an entry may carry: past it the courier parks an exit, so a
// larger figure is one no courier produced (internal/agent pins it equal to
// its courierParkAfter).
const MaxCourierSilent = 3

// Validate is the pure shape check of an outbox.
func (s CourierState) Validate() error {
	if s.Version != CourierStateVersion {
		return fmt.Errorf("courier state version %d, want %d", s.Version, CourierStateVersion)
	}
	if len(s.Events) > MaxCourierEvents {
		return fmt.Errorf("%d courier events exceed %d", len(s.Events), MaxCourierEvents)
	}
	seen := make(map[string]bool, len(s.Events))
	var bytes int
	for _, ev := range s.Events {
		if !procrequest.ValidToken(ev.PID) {
			return fmt.Errorf("courier event without a valid pid")
		}
		switch ev.Kind {
		case CourierEventStarted, CourierEventExit:
		default:
			return fmt.Errorf("courier event %s has kind %q", ev.PID, ev.Kind)
		}
		key := ev.PID + "/" + ev.Kind
		if seen[key] {
			return fmt.Errorf("courier event %s listed twice", key)
		}
		seen[key] = true
		if !json.Valid(ev.Payload) {
			return fmt.Errorf("courier event %s payload is not JSON", key)
		}
		bytes += len(ev.Payload)
		if ev.Parked && ev.Kind != CourierEventExit {
			return fmt.Errorf("courier event %s: only exits park", key)
		}
		if ev.Fails < 0 || ev.Silent < 0 || ev.Silent > MaxCourierSilent || ev.EnqueuedAtUnixNano <= 0 {
			return fmt.Errorf("courier event %s has invalid bookkeeping", key)
		}
	}
	if bytes > MaxCourierBytes {
		return fmt.Errorf("courier payloads total %d bytes, exceed %d", bytes, MaxCourierBytes)
	}
	return nil
}
