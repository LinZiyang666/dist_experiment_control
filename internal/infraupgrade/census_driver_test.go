//go:build linux && amd64

// Copy into internal/infraupgrade. The NATS recovery cases reuse that package's
// existing state-table fixture; no server, subprocess or privileged effect runs.
package infraupgrade

import (
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
)

func reviewProcess() c.Process {
	return c.Process{BootID: "boot", PID: 100, StartTicks: 1, InvocationID: "00000000000000000000000000000001", ExeDevice: 1, ExeInode: 2, ExeSHA256: c.Digest{1}, Cgroup: "/system.slice/caddy.service"}
}

// origin: S9 internal review rv-infra F07
func TestCaddyDrainAllowsSealedEmptyListener(t *testing.T) {
	old := reviewProcess()
	// P02 assembleCensus retains LISTEN/state=10 in Sockets, with zero pending
	// responsibility when its receive queue and all accepted/conntrack sets are empty.
	census := c.Census{Complete: true, Generation: 1, Digest: c.Digest{1}, Processes: []c.Process{old}, Sockets: []c.Socket{
		{NetNS: 1, Device: 1, Inode: 8, Cookie: 9, Local: "0.0.0.0:443", Remote: "0.0.0.0:0", Owner: old},
	}}
	if !caddyCensus(census) {
		t.Fatal("invalid fixture")
	}
	if !caddyDrained(census, old) {
		t.Fatal("sealed empty LISTEN socket prevents reaching graceful stop")
	}
}

type reviewCensusHost struct {
	census    c.Census
	requested c.ID
	sequence  *infrahost.CensusSequence
}

func (h *reviewCensusHost) Do(c.HostRequest, c.Deadline, c.Stop) (c.HostReceipt, error) {
	return c.HostReceipt{}, c.ErrUnsupported
}
func (h *reviewCensusHost) Inspect(id c.ID, _ c.Deadline) (c.Census, error) {
	h.requested = id
	if h.sequence != nil {
		// Exercise the same trusted producer used by identityBackend. The review
		// fixture originally hard-coded the producer's broken fixed generation;
		// retaining that fake would require removing the driver's rollback guard.
		observed := h.census
		observed.Digest = c.Digest{}
		data, err := json.Marshal(observed)
		if err != nil {
			return c.Census{}, err
		}
		observed.Digest = sha256.Sum256(data)
		return h.sequence.Observe(observed)
	}
	return h.census, nil
}

// origin: S9 internal review rv-infra F05
func TestNATSPhysicalInspectionUsesDomain(t *testing.T) {
	dl, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h := &reviewCensusHost{census: c.Census{Complete: true, Generation: 1, Digest: c.Digest{1}, Processes: []c.Process{reviewProcess()}}}
	d := &natsDriver{deps: c.Dependencies{Host: h}}
	r := &natsRun{plan: c.Plan{Tx: c.ID{1}, Domain: c.ID{2}}, deadline: dl}
	if _, err := d.inspect(r); err != nil {
		t.Fatal(err)
	}
	if h.requested != r.plan.Domain {
		t.Fatalf("physical Inspect receives Tx %x instead of Domain %x", h.requested, r.plan.Domain)
	}
}

// origin: S9 internal review rv-infra F06
func TestCaddyPhysicalGenerationCanProgress(t *testing.T) {
	dl, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h := &reviewCensusHost{sequence: &infrahost.CensusSequence{}, census: c.Census{Complete: true, Generation: 7, Digest: c.Digest{1}, Processes: []c.Process{reviewProcess()}}}
	d := &caddyDriver{deps: c.Dependencies{Host: h}}
	x := &CaddyTransition{Plan: c.Plan{Domain: c.ID{2}}}
	if _, err := d.inspect(x, dl); err != nil {
		t.Fatal(err)
	}
	// identityBackend keeps its CensusScope.Generation fixed across Inspect calls.
	h.census.Processes = append(h.census.Processes, c.Process{BootID: "boot", PID: 101, StartTicks: 2, InvocationID: "00000000000000000000000000000002", ExeDevice: 1, ExeInode: 3, ExeSHA256: c.Digest{2}, Cgroup: "/system.slice/warm.service"})
	h.census.Digest = c.Digest{2}
	if _, err := d.inspect(x, dl); err != nil {
		t.Fatalf("physical scope generation stayed fixed when warm started: %v", err)
	}
}

// origin: S9 internal review rv-infra F08
func TestNATSRecoverAfterControllerReplacement(t *testing.T) {
	f := newNATSFixture(t, false)
	f.fail["recovery.verify"] = 1
	status, err := f.driver.Advance(f.plan, f.deadline, nil)
	if err == nil || status.Phase != c.Halted || f.oldAlive || !f.started {
		t.Fatalf("fault not reached: phase=%v err=%v", status.Phase, err)
	}
	replacement, err := NewNATS(f.dependencies())
	if err != nil {
		t.Fatal(err)
	}
	status, err = replacement.Recover(f.plan, f.deadline, nil)
	if err != nil || status.Phase != c.Completed {
		t.Fatalf("replacement cannot recover durable transaction: phase=%v err=%v", status.Phase, err)
	}
}

// origin: S9 internal review rv-infra F08
func TestNATSRecoverHasBoundedFreshActionBudget(t *testing.T) {
	f := newNATSFixture(t, false)
	f.fail["recovery.verify"] = 1
	status, err := f.driver.Advance(f.plan, f.deadline, nil)
	if err == nil || status.Phase != c.Halted {
		t.Fatal("fault not reached")
	}
	// Deterministically model the original handover deadline having elapsed.
	d := f.driver.(*natsDriver)
	d.run.deadline = 1
	fresh, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	status, err = d.Recover(f.plan, fresh, nil)
	if err != nil || status.Phase != c.Completed {
		t.Fatalf("recover still uses expired handover budget: phase=%v err=%v", status.Phase, err)
	}
}

// origin: S9 internal review rv-infra F06
func TestCaddyPhysicalCensusRejectsReplayAndConflictingGeneration(t *testing.T) {
	dl, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h := &reviewCensusHost{sequence: &infrahost.CensusSequence{}, census: c.Census{Complete: true, Generation: 7, Processes: []c.Process{reviewProcess()}}}
	d := &caddyDriver{deps: c.Dependencies{Host: h}}
	x := &CaddyTransition{Plan: c.Plan{Domain: c.ID{2}}}
	first, err := d.inspect(x, dl)
	if err != nil {
		t.Fatal(err)
	}
	// A natural close changes the trusted producer's responsibility set.
	h.census.OldOwnerPending = 1
	latest, err := d.inspect(x, dl)
	if err != nil || latest.Generation <= first.Generation {
		t.Fatalf("producer did not advance: %v", err)
	}
	h.sequence = nil
	for _, replay := range []c.Census{first, func() c.Census { v := latest; v.Digest[0]++; return v }()} {
		h.census = replay
		if _, err := d.inspect(x, dl); err == nil {
			t.Fatal("stale or conflicting physical observation was accepted")
		}
	}
	h.census = latest
	if _, err := d.inspect(x, dl); err != nil {
		t.Fatal("identical current observation was rejected")
	}
}
