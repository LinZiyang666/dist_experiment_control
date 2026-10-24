//go:build linux && amd64

// Copy into internal/infrahost. Exercises the real durable transition functions.
package infrahost

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// origin: S9 privilege review — successful exec changes only executable identity.
// origin: S9 internal review rv-priv F8
func TestLaunchAckAdmitsObservedStockExec(t *testing.T) {
	d, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	launcher := c.Process{BootID: "boot", PID: 123, StartTicks: 456, InvocationID: "unit", Cgroup: "/system.slice/nats-server.service", ExeDevice: 1, ExeInode: 2, ExeSHA256: c.Digest{1}}
	artifact := c.Artifact{Kind: c.NATSBinary, Version: "2.14.6", GOOS: "linux", GOARCH: "amd64", BinarySHA256: c.Digest{2}}
	s := HostState{Step: 1, Permit: c.Digest{3}, LaunchCandidate: artifact, LaunchConfig: c.Digest{4}, LaunchDeadline: d}
	r := c.HostRequest{Op: c.LaunchClaim, Step: 1, Candidate: artifact, Config: s.LaunchConfig, Payload: s.Permit, Expected: launcher}
	if err := validateTransition(s, r); err != nil {
		t.Fatal("claim rejected:", err)
	}
	applyState(&s, r, c.HostReceipt{Applied: true, Current: launcher})
	r.Op = c.LaunchAck
	r.Expected.ExeInode = 3
	r.Expected.ExeSHA256 = artifact.BinarySHA256
	if err := validateTransition(s, r); err != nil {
		t.Fatalf("same PID/startticks/invocation stock exec rejected after claim: %v", err)
	}
}
