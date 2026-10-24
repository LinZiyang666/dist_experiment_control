// capability_probe.go bounds and SHA-pins candidate capability probes. The
// caller supplies the isolated process supervisor without a reverse dependency.
// origin: hot-update S4 plan §5.2 / T46.
package agentimage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

// VersionProbeTimeout preserves the candidate execution budget.
// upgradeSmokeTimeout bounds the smoke gate's `version` probe. A healthy
// static binary answers in milliseconds; 5s absorbs a cold page cache.
const VersionProbeTimeout = 5 * time.Second

// VersionOutputLimit preserves the candidate capture budget.
// smokeOutputCap bounds how much of the candidate's stdout we will hold
// (external review 疑惑 2): a hostile/broken candidate spraying output must
// not OOM the agent. The real line is <100 bytes.
const VersionOutputLimit = 64 * 1024

// ProbeCapabilities binds bounded version output to stable executable bytes.
func ProbeCapabilities(binPath string, runProbe func(*exec.Cmd) error) (string, Capabilities, error) {
	before, err := SHA256OfFile(binPath)
	if err != nil {
		return "", Capabilities{}, fmt.Errorf("hash candidate before version probe: %w", err)
	}
	// ctx-root: the isolated version probe owns its bounded process lifetime.
	ctx, cancel := context.WithTimeout(context.Background(), VersionProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binPath, "version")
	// Bound output as well as execution. The isolated supervisor kills and
	// reaps the probe group on success, refusal and timeout without reaping
	// any of the agent's business children (hot-update S3 plan §2.1 / RS-3).
	output := NewVersionOutput(VersionOutputLimit)
	cmd.Stdout = output
	cmd.WaitDelay = 2 * time.Second
	if err := runProbe(cmd); err != nil {
		return "", Capabilities{}, fmt.Errorf("exec `version` on the staged binary: %w", err)
	}
	after, err := SHA256OfFile(binPath)
	if err != nil || before != after {
		return "", Capabilities{}, errors.New("candidate changed during version probe")
	}
	if output.Overflowed() {
		return "", Capabilities{}, errors.New("candidate version output exceeds limit")
	}
	release, capabilities, err := ParseCapabilities(output.String())
	capabilities.SHA256 = before
	return release, capabilities, err
}

// VersionOutput captures declarations without permitting unbounded memory.
// capWriter drops bytes past n; version and domain declarations must fit
// within the bounded capture or the candidate is refused.
type VersionOutput struct {
	w        *bytes.Buffer
	n        int
	overflow bool
}

func (c *VersionOutput) Write(p []byte) (int, error) {
	if len(p) > c.n-c.w.Len() {
		c.overflow = true
	}
	if room := c.n - c.w.Len(); room > 0 {
		if len(p) > room {
			c.w.Write(p[:room])
		} else {
			c.w.Write(p)
		}
	}
	return len(p), nil
}

// NewVersionOutput captures bounded version output and records truncation.
func NewVersionOutput(limit int) *VersionOutput {
	return &VersionOutput{w: &bytes.Buffer{}, n: limit}
}

// String returns the captured prefix of the version output.
func (c *VersionOutput) String() string { return c.w.String() }

// Overflowed reports whether any output exceeded the capture bound.
func (c *VersionOutput) Overflowed() bool { return c.overflow }
