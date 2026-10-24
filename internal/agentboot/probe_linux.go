//go:build linux

package agentboot

import (
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"os/exec"
)

// RunVersionProbe retains the bootstrap API while the neutral supervisor owns
// the isolated candidate process tree. Broker preflight can use that leaf
// without importing the agent image or its sealed SS owner.
// origin: hot-update S6 plan §3.1 / §9.3 / T06
func RunVersionProbe(cmd *exec.Cmd) error { return spawnsafe.RunVersionProbe(cmd) }
