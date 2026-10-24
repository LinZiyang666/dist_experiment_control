//go:build !linux && !darwin

package xfersend

import (
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// Source custody requires descriptor identity and access flags. Platforms
// without the POSIX leaf must refuse rather than seal an unverified source.
// origin: hot-update S7 plan §2.2 / §11.1 / T19
func New(Config) (Sender, error) {
	return nil, handoff.ErrUnsupported
}

// Import refuses before adopting an original source on unsupported platforms.
// origin: hot-update S7 plan §9 / T35
func Import(Config, xfercontract.SenderState) (Sender, error) {
	return nil, handoff.ErrUnsupported
}

// Inline has no sender to read on unsupported platforms.
// origin: hot-update S7 plan §9 / FIX-INLINE
func Inline(Sender, xfercontract.Deadline) (xfercontract.SourceSeal, []byte, error) {
	return xfercontract.SourceSeal{}, nil, handoff.ErrUnsupported
}
