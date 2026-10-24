package upgradebudget

import "time"

// Privileged IPC has one bounded read, action and worker lifetime. Keep these
// beside the other upgrade budgets so IPC and drill admission cannot drift.
// origin: S7 internal review GATES-PROD F3 / hot-update S9 plan §10.2 / T83
const (
	InfraReadBudget   = 2 * time.Second
	InfraActionBudget = 10 * time.Second
	InfraWorkerBudget = 15 * time.Second
	InfraMaxWorkers   = 4
)
