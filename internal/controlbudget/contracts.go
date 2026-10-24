// Package controlbudget defines neutral admission deadlines and attempt observations.
// origin: hot-update S6 plan §2.1 / §12.1 P00
package controlbudget

// Deadline is an absolute CLOCK_BOOTTIME timestamp in nanoseconds; zero is invalid.
type Deadline int64

// AttemptPhase is closed: producers share one vocabulary across admission and cuts.
type AttemptPhase string

const (
	PhaseDNS           AttemptPhase = "dns"
	PhaseProxy         AttemptPhase = "proxy"
	PhaseTLS           AttemptPhase = "tls"
	PhaseWS            AttemptPhase = "ws"
	PhaseConnect       AttemptPhase = "connect"
	PhaseInboxProbe    AttemptPhase = "inbox_probe"
	PhaseInboxFallback AttemptPhase = "inbox_fallback"
	PhaseDiscovery     AttemptPhase = "discovery"
	PhaseBanner        AttemptPhase = "banner"
	PhaseProbe         AttemptPhase = "probe"
	PhaseCreate        AttemptPhase = "create"
	PhaseRequest       AttemptPhase = "request"
	PhaseBackoff       AttemptPhase = "backoff"
	PhaseCancel        AttemptPhase = "cancel"
	PhaseResult        AttemptPhase = "result"
	PhaseFreeze        AttemptPhase = "freeze"
	PhaseExec          AttemptPhase = "exec"
	PhaseImport        AttemptPhase = "import"
	PhaseAuth          AttemptPhase = "auth"
	PhaseReady         AttemptPhase = "ready"
	PhaseMaintenance   AttemptPhase = "maintenance"
)

type Attempt struct {
	LogicalID      string       `json:"logical_id"`
	Ordinal        uint32       `json:"ordinal"`
	Phase          AttemptPhase `json:"phase"`
	StartedMonoNS  int64        `json:"started_mono_ns"`
	FinishedMonoNS int64        `json:"finished_mono_ns"`
	Sent           bool         `json:"sent"`
	Class          string       `json:"class"`
	BackoffNS      int64        `json:"backoff_ns"`
}

type Observer interface {
	Begin(logicalID string, phase AttemptPhase) (Attempt, error)
	Record(Attempt) error
	Close() error
}
