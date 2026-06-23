// alert_forward.go is the D8b (§10) BUILD-AND-PROVE mechanism file for forwarding alert
// signals/acks from any broker to the leader's replicated alert store over the §4.1 forwarder.
// The leader-side handlers (VerbAlertSignal / VerbAlertAck) live in cluster_forward.go's
// dispatchForward; this file holds the originating-side seam + wire payloads.
//
// Everything here is INERT in production (cutover=D9): serve.go never calls AttachAlertSink,
// so b.alertSink stays nil and the disk monitor surfaces pressure only via the existing
// pubSysEvent (byte-identical).
//
// EXCLUDED from the TestD8ProductionWiresNoCluster guard scan: this is where the
// `b.alertSink =` write lives.
package broker

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// AlertSignalPayload forwards a per-node alert raise/clear decision to the leader (D8b §10.2;
// currently disk_pressure). The leader reads the current ACTIVE state and commits a raise or
// clear only on a transition.
type AlertSignalPayload struct {
	Kind     string `json:"kind"`
	Node     string `json:"node"`
	Active   bool   `json:"active"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

// AlertAckPayload forwards a cluster-level ack to the leader (D8b §10.1).
type AlertAckPayload struct {
	DedupKey string `json:"dedup_key"`
	AckedBy  string `json:"acked_by"`
}

// planAlertSignal is the leader-side decision for a forwarded VerbAlertSignal (§10.2). It
// reads the CURRENT ACTIVE state and returns a raise/clear command ONLY on a genuine
// transition (a nil command → no raft write, so a broker's every-tick re-assert is free and a
// dropped clear self-heals). It VALIDATES kind/severity against the 0009 enum first (review
// m1): an out-of-enum value returns a nil command (no write, no FSM poison) rather than
// failing the CHECK on every replica. Extracted from dispatchForward so it is unit-testable.
func planAlertSignal(db *sql.DB, p AlertSignalPayload, now time.Time) (*cluster.Command, error) {
	if !cluster.ValidAlertKind(p.Kind) || !cluster.ValidAlertSeverity(p.Severity) {
		return nil, nil // defense-in-depth: never bake an out-of-enum row (would brick on replay)
	}
	key := cluster.DedupKeyNode(p.Kind, p.Node)
	active, err := cluster.IsAlertActive(db, key)
	if err != nil {
		return nil, err
	}
	switch {
	case p.Active && !active:
		id := fmt.Sprintf("%s@%d", key, now.UnixNano())
		return cluster.PlanAlertRaise(id, p.Kind, p.Severity, key, p.Message, now)
	case !p.Active && active:
		return cluster.PlanAlertClear(key, now)
	default:
		return nil, nil // no transition
	}
}

// AttachAlertSink wires the D8b disk-pressure forward seam (TEST/HARNESS ONLY). After this
// call the disk monitor's every-tick signalDiskAlert forwards the local disk state to the
// leader as a VerbAlertSignal keyed disk_pressure:<selfID>. Best-effort: a lost signal is
// re-asserted next tick (the leader-side transition gate makes the re-assert free). Call
// before Run. Production never calls it.
func (b *Broker) AttachAlertSink(fwd *Forwarder) {
	b.alertSink = func(active bool) {
		p := AlertSignalPayload{
			Kind:     cluster.AlertKindDiskPressure,
			Node:     b.selfID,
			Active:   active,
			Severity: cluster.AlertSeverityInfo,
			Message:  "broker " + b.selfID + " disk usage is above threshold — free space or expand the volume",
		}
		payload, err := json.Marshal(p)
		if err != nil {
			return
		}
		_ = fwd.Forward(VerbAlertSignal, "", payload) // best-effort; level-triggered re-assert self-heals
	}
}
