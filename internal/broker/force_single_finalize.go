package broker

import (
	"time"

	"github.com/LinZiyang666/tether/internal/brokercontrol"
)

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM
const opForceSingleFinalizeBudget = brokercontrol.OpForceSingleFinalizeBudget

func parseOpTimestamp(s string) (time.Time, bool) { return brokercontrol.ParseOpTimestamp(s) }
