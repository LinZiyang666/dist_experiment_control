//go:build !linux

package infralaunch

import "github.com/LinZiyang666/tether/internal/infracontract"

func inheritedLock(*Lock, infracontract.Process) error { return infracontract.ErrUnsupported }
