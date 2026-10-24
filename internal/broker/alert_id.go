package broker

import (
	"time"

	"github.com/LinZiyang666/tether/internal/brokercontrol"
)

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM

func newAlertID(dedupKey string, now time.Time) string {
	return brokercontrol.NewAlertID(dedupKey, now)
}
