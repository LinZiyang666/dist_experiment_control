package workload

// ProcessState is the versioned process-table part of an image handoff. Its
// owner is the original parent process, not merely another process with access
// to the same task IDs. Streams, admission, permissions and event delivery have
// separate owners and must join the same runtime cut before this is activated.
type ProcessState struct {
	Version             int      `json:"version"`
	OwnerPID            int      `json:"owner_pid"`
	OwnerStartTimeTicks int64    `json:"owner_start_time_ticks"`
	BootID              string   `json:"boot_id"`
	Records             []Record `json:"records"`
}

const processStateVersion = 1
