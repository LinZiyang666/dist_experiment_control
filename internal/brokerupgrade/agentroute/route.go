// origin: hot-update S8 plan §12.1; S8 BROKERUPGRADE-SLIM responsibility extraction
package agentroute

import (
	"database/sql"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
)

// AgentRoute is one authenticated upgrade-tx request. Actor is the subject's
// NKey, ActorFP its fingerprint; Request is the broker's own private-inbox
// request to the agent's forwarded subject and returns the raw reply body.
type AgentRoute struct {
	DB                       *sql.DB
	SID, NID, Actor, ActorFP string
	Reply                    string
	Body                     []byte
	Timeout                  time.Duration
	Request                  func(subject string, body []byte, timeout time.Duration) ([]byte, error)
	// Received is when the request was taken off the subscription (zero: now).
	// The forward deadline is Received+Timeout, whatever admission spent.
	// origin: S8 BROKER-EXPORTER R-9
	Received   time.Time
	SignSample func(rollcontrol.Sample) (rollcontrol.Sample, error)
	VerifyRoll func(rollcontrol.Request) error
}
