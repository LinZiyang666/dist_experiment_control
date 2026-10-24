//go:build !linux

package ingressswitch

import (
	"crypto/tls"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

// Unsupported platforms retain their cold install/build path. No rule, socket
// mark or runtime hot-cut capability is inferred from successful compilation.
// origin: hot-update S9 plan lead LQ1 / §5.2 / P07
func NewLinux() (Backend, error) { return nil, infracontract.ErrUnsupported }

type TLSAttempt struct {
	StartedNS  int64
	FinishedNS int64
	Success    bool
	Mark       uint32
}

func DialPrimary(_ string, _ *tls.Config, _ infracontract.Deadline, _ infracontract.Stop) (*tls.Conn, TLSAttempt, error) {
	return nil, TLSAttempt{}, infracontract.ErrUnsupported
}

func DialCanonical(_ string, _ *tls.Config, _ infracontract.Deadline, _ infracontract.Stop) (*tls.Conn, TLSAttempt, error) {
	return nil, TLSAttempt{}, infracontract.ErrUnsupported
}
