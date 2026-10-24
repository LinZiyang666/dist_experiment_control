//go:build linux && amd64

// Copy into internal/infrahost. Only the actual census reducer runs.
package infrahost

import (
	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"testing"
	"time"
)

// origin: S9 internal review rv-infra F04
func TestLaunchAckAcceptsSameProcessAfterStockExec(t *testing.T) {
	dl, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	launcher := c.Process{BootID: "boot", PID: 100, StartTicks: 1, InvocationID: "unit", ExeDevice: 1, ExeInode: 2, ExeSHA256: c.Digest{1}, Cgroup: "/system.slice/nats-server.service"}
	server := launcher
	server.ExeInode, server.ExeSHA256 = 3, c.Digest{2}
	artifact := c.Artifact{Kind: c.NATSBinary, Version: "2.14.6", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: c.Digest{1}, BinarySHA256: server.ExeSHA256, CatalogSHA256: c.Digest{3}}
	state := HostState{Step: 1, Permit: c.Digest{4}, LaunchCandidate: artifact, LaunchConfig: c.Digest{5}, LaunchDeadline: dl, LaunchClaimed: true, LaunchOwner: launcher}
	r := c.HostRequest{Op: c.LaunchAck, Step: 1, Payload: state.Permit, Candidate: artifact, Config: state.LaunchConfig, Expected: server}
	if err := validateTransition(state, r); err != nil {
		t.Fatalf("exact same PID/startticks exec cannot acknowledge stock identity: %v", err)
	}
	for _, mutate := range []func(*c.Process){
		func(p *c.Process) { p.BootID = "foreign-boot" },
		func(p *c.Process) { p.PID++ },
		func(p *c.Process) { p.StartTicks++ },
		func(p *c.Process) { p.InvocationID = "foreign-invocation" },
		func(p *c.Process) { p.Cgroup = "/foreign-unit" },
		func(p *c.Process) { p.ExeSHA256 = c.Digest{9} },
		func(p *c.Process) { *p = launcher },
	} {
		bad := r
		mutate(&bad.Expected)
		if err := validateTransition(state, bad); err == nil {
			t.Fatal("Ack accepted a different incarnation or pre-exec identity")
		}
	}
}
