package ptysession

import (
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Image is the version-1 terminal domain. Its fields are never omitted: only
// the enclosing runtime's PTY pointer is optional for a pure exec image. It
// holds no descriptor numbers, live owners, or reusable connection health proof.
// origin: hot-update S4 plan §5.1
type Image struct {
	Version                 int                      `json:"version"`
	Identity                pty.TerminalIdentity     `json:"identity"`
	Observation             pty.TerminalObservation  `json:"observation"`
	Input                   workload.ReplayState     `json:"input"`
	Output                  workload.ReplayState     `json:"output"`
	InputFIN                bool                     `json:"input_fin"`
	InputEnded              bool                     `json:"input_ended"`
	InputError              string                   `json:"input_error"`
	OutputError             string                   `json:"output_error"`
	Pending                 []byte                   `json:"pending"`
	Discarding              bool                     `json:"discarding"`
	SourceEnded             bool                     `json:"source_ended"`
	MasterClosed            bool                     `json:"master_closed"`
	ControlSeq              uint64                   `json:"control_seq"`
	ControlDigest           string                   `json:"control_digest"`
	ControlError            string                   `json:"control_error"`
	KAIntervalMS            int                      `json:"ka_interval_ms"`
	KADeadlineBootNS        int64                    `json:"ka_deadline_boot_ns"`
	ExpiredSinceBootNS      int64                    `json:"expired_since_boot_ns"`
	RecoveryNotBeforeBootNS int64                    `json:"recovery_not_before_boot_ns"`
	NextJudgeBootNS         int64                    `json:"next_judge_boot_ns"`
	LastKASeq               uint64                   `json:"last_ka_seq"`
	AuthorityDeadlineBootNS int64                    `json:"authority_deadline_boot_ns"`
	Revoked                 string                   `json:"revoked"`
	CleanupPending          bool                     `json:"cleanup_pending"`
	CleanupMembers          []workload.SessionMember `json:"cleanup_members"`
}

// Tombstone retains final byte and control responsibility after all terminal
// descriptors and replay reservations have gone. ACK need not equal Accepted
// when input ended with an error; settling must not erase that diagnosis.
type Tombstone struct {
	InputACK      uint64 `json:"input_ack"`
	InputAccepted uint64 `json:"input_accepted"`
	InputFIN      bool   `json:"input_fin"`
	InputEnded    bool   `json:"input_ended"`
	InputError    string `json:"input_error"`
	ControlSeq    uint64 `json:"control_seq"`
	ControlDigest string `json:"control_digest"`
	ControlError  string `json:"control_error"`
}
