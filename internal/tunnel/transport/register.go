package transport

import "errors"

// ErrRegisterDenied wraps every broker DENY reply. errors.Is(err,
// ErrRegisterDenied) is true for a *DenyError; the reconnect loop then
// classifies the Reason as terminal (stop) or transient (retry).
var ErrRegisterDenied = errors.New("tunnel: broker denied REGISTER")

// DenyError carries the broker's DENY reason string for terminal-vs-
// transient classification (see denyIsTransient).
type DenyError struct{ Reason string }

func (e *DenyError) Error() string { return "tunnel: broker denied REGISTER: " + e.Reason }

func (e *DenyError) Is(target error) bool { return target == ErrRegisterDenied }

// denyIsTransient reports whether a DENY reason is worth retrying. Only
// the broker's two non-authoritative reasons are transient: a public-port
// bind race (the old listener is mid-teardown) and a broker-side store
// fault (try_again, see broker.tunnelTokenLookup). Every other reason —
// including any UNKNOWN one from a newer broker — is treated terminal:
// the safe default is to stop rather than hammer a broker that
// authoritatively refused us.
func DenyIsTransient(reason string) bool {
	switch reason {
	case "public_port_bind_failed", "try_again":
		return true
	default:
		return false
	}
}
