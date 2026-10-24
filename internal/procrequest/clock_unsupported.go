//go:build !linux

package procrequest

import "github.com/LinZiyang666/tether/internal/handoff"

func currentBootID() (string, error) { return "", handoff.ErrUnsupported }
func bootNow() (int64, error)        { return 0, handoff.ErrUnsupported }
