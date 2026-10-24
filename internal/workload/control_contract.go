package workload

import "syscall"

// Control is valid only during WithControl's callback. Its signal authority
// must not escape the Manager guard or outlive the retained process identity.
// origin: hot-update S4 plan §2.3 / §3.4
type Control struct {
	Identity Identity
	Exit     *Exit
	Signal   func(syscall.Signal) error
}

// SessionMember retains the identity that cleanup must verify again before
// signalling; a recycled PID cannot inherit another task's cleanup obligation.
type SessionMember struct {
	Identity  Identity `json:"identity"`
	SessionID int      `json:"session_id"`
}
