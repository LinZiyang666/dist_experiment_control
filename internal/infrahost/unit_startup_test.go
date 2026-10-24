//go:build linux

package infrahost

import (
	"crypto/sha256"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

type unitTestState struct {
	mu         sync.Mutex
	f          StartupFence
	casError   error
	badReceipt bool
	badDigest  bool
}

func (s *unitTestState) Load(_ infracontract.Component, _ infracontract.Deadline) (StartupFence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f, nil
}
func (s *unitTestState) CompareAndSwap(before, after StartupFence, r infracontract.HostRequest, _ infracontract.Deadline) (infracontract.HostReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.casError != nil {
		return infracontract.HostReceipt{}, s.casError
	}
	if !reflect.DeepEqual(before, s.f) {
		return infracontract.HostReceipt{}, ErrUnitNotReady
	}
	s.f = after
	if s.badReceipt {
		return infracontract.HostReceipt{}, nil
	}
	encoded, err := infracontract.EncodeRequest(r)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	receipt := infracontract.HostReceipt{Tx: r.Tx, Sequence: r.Sequence, Revision: after.Revision, Op: r.Op, Applied: true, Journal: infracontract.Digest{7}, RequestDigest: infracontract.Digest(sha256.Sum256(encoded))}
	if s.badDigest {
		receipt.RequestDigest[0]++
	}
	return receipt, nil
}

func unitTestFence(t *testing.T) (StartupFence, infracontract.HostRequest) {
	t.Helper()
	f := StartupFence{Tx: infracontract.ID{1}, Domain: infracontract.ID{2}, Step: 1, Component: infracontract.Caddy,
		Revision: 1, Closed: true, Deadline: unitTestDeadline(t, time.Second), OldExited: true, StorageFinalized: infracontract.Digest{3}}
	r := infracontract.HostRequest{Version: 1, Op: infracontract.ReleaseStart, Tx: f.Tx, Domain: f.Domain, Step: f.Step, Component: f.Component, Sequence: 1, Payload: f.StorageFinalized,
		PolicyRevision: 1, KeyID: "fixture-key", HostBootID: "fixture-boot", Nonce: infracontract.ID{4}, RemainingNS: uint64(time.Second), ExpectedJournal: infracontract.Digest{5}, Config: infracontract.Digest{6},
		Expected: unitTestProcess(t, os.Getpid()), Candidate: infracontract.Artifact{Kind: infracontract.CaddyBinary, Version: "2.7.6", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: infracontract.Digest{7}, BinarySHA256: infracontract.Digest{8}, CatalogSHA256: infracontract.Digest{9}}}
	r.HostBootID = r.Expected.BootID
	return f, r
}

func testUnitStartupFence(t *testing.T) {
	f, r := unitTestFence(t)
	if _, err := ClaimStartup(f); !errors.Is(err, ErrUnitNotReady) {
		t.Fatal("closed durable fence permits restart")
	}
	for _, defect := range []string{"old-writer", "storage", "different-storage", "tx", "domain", "step", "revision", "expired", "boots"} {
		broken, request := f, r
		switch defect {
		case "old-writer":
			broken.OldExited = false
		case "storage":
			broken.StorageFinalized = infracontract.Digest{}
		case "different-storage":
			request.Payload = infracontract.Digest{4}
		case "tx":
			request.Tx[1]++
		case "domain":
			request.Domain[1]++
		case "step":
			request.Step++
		case "revision":
			broken.Revision = 0
		case "expired":
			broken.Deadline = 1
		case "boots":
			broken.Boots = upgradebudget.BootBudget
		}
		got, err := ReleaseStartup(broken, request)
		if err == nil || !reflect.DeepEqual(got, broken) {
			t.Errorf("unsafe release %s changed fence", defect)
		}
	}
	released, err := ReleaseStartup(f, r)
	if err != nil || released.Closed || released.Deadline != f.Deadline {
		t.Fatal("release changed original budget or failed")
	}
	for boot := uint32(0); boot < upgradebudget.BootBudget; boot++ {
		claimed, err := ClaimStartup(released)
		if err != nil || claimed.Boots != boot+1 || claimed.Deadline != f.Deadline {
			t.Fatal("claim did not debit fixed boot budget")
		}
		released = claimed
	}
	if got, err := ClaimStartup(released); !errors.Is(err, ErrUnitBudget) || !reflect.DeepEqual(got, released) {
		t.Fatal("restart minted a fourth boot")
	}
	released.Boots, released.Deadline = 0, 1
	if _, err := ClaimStartup(released); !errors.Is(err, ErrUnitBudget) {
		t.Fatal("expired permit reopened startup")
	}
}

func testUnitHostEffects(t *testing.T) {
	newHost := func() (infracontract.Host, *unitTestManager, *unitTestState, *unitTestPhysical, infracontract.HostRequest) {
		f, r := unitTestFence(t)
		m := &unitTestManager{}
		policies := []UnitPolicy{unitFixture(UnitCaddy, false), unitFixture(UnitCaddyWarm, false), unitFixture(UnitCaddy, true)}
		for _, p := range policies {
			if !p.Legacy {
				m.units[p.Target] = p.Expected
			}
		}
		state := &unitTestState{f: f}
		physical := &unitTestPhysical{processes: []infracontract.Process{unitTestProcess(t, os.Getpid())}}
		h, err := NewUnitHost(UnitDependencies{Manager: m, State: state, Physical: physical, Policies: policies, Retire: physical.Do})
		if err != nil {
			t.Fatal(err)
		}
		r.Expected = physical.processes[0]
		return h, m, state, physical, r
	}
	for _, op := range []infracontract.HostOp{infracontract.FenceStart, infracontract.StartWarm, infracontract.ReleaseStart, infracontract.RetireWarm} {
		h, m, state, physical, r := newHost()
		r.Op = op
		if op == infracontract.FenceStart {
			r.BeforeCensus = infracontract.Digest{8}
		}
		if op == infracontract.RetireWarm {
			r.Candidate, r.Config = infracontract.Artifact{}, infracontract.Digest{}
			state.f.Warm = r.Expected
			state.f.ReturnProof, r.ReturnProof = infracontract.Digest{8}, infracontract.Digest{8}
			state.f.Retirement, _ = physical.Inspect(r.Tx, state.f.Deadline)
			r.WarmCensus = state.f.Retirement.Digest
		}
		receipt, err := h.Do(r, state.f.Deadline, nil)
		if err != nil || !receipt.Applied || receipt.Op != op {
			t.Fatalf("op %d: receipt=%+v err=%v", op, receipt, err)
		}
		switch op {
		case infracontract.FenceStart:
			if !state.f.Closed || len(m.started) != 0 || !reflect.DeepEqual(m.fenced, []UnitTarget{UnitCaddy}) {
				t.Fatal("fence starts business or fails to replace direct entry")
			}
		case infracontract.StartWarm:
			if !state.f.WarmPending || !reflect.DeepEqual(m.started, []UnitTarget{UnitCaddyWarm}) {
				t.Fatal("warm intent not persisted")
			}
			r.Sequence++
			if _, err := h.Do(r, state.f.Deadline, nil); err == nil || len(m.started) != 1 {
				t.Fatal("pending warm job submitted twice")
			}
		case infracontract.ReleaseStart:
			if state.f.Closed || !reflect.DeepEqual(m.started, []UnitTarget{UnitCaddy}) {
				t.Fatal("release did not submit main start")
			}
		case infracontract.RetireWarm:
			if len(m.started) != 0 || len(physical.signals) != 1 || !reflect.DeepEqual(physical.signals[0], r) {
				t.Fatal("retirement used unit name instead of exact physical signal")
			}
		}
	}
	for _, defect := range []string{"cas-failure", "cas-stub", "receipt-digest", "job-lost", "old-owner-pending", "return-proof", "warm-census", "warm-identity", "unknown-op", "unsafe-unit"} {
		h, m, state, physical, r := newHost()
		r.Op = infracontract.ReleaseStart
		switch defect {
		case "cas-failure":
			state.casError = ErrUnitUnknown
		case "cas-stub":
			state.badReceipt = true
		case "receipt-digest":
			state.badDigest = true
		case "job-lost":
			m.startError = ErrUnitUnknown
		case "unknown-op":
			r.Op = infracontract.SignalStop
		case "unsafe-unit":
			m.units[UnitCaddy].EnvironmentFiles = "/tmp/operator.env"
		default:
			r.Op = infracontract.RetireWarm
			r.Candidate, r.Config = infracontract.Artifact{}, infracontract.Digest{}
			state.f.Warm = r.Expected
			state.f.ReturnProof, r.ReturnProof = infracontract.Digest{8}, infracontract.Digest{8}
			state.f.Retirement, _ = physical.Inspect(r.Tx, state.f.Deadline)
			r.WarmCensus = state.f.Retirement.Digest
			switch defect {
			case "old-owner-pending":
				state.f.Retirement.OldOwnerPending = 1
			case "return-proof":
				r.ReturnProof[1]++
			case "warm-census":
				r.WarmCensus[1]++
			case "warm-identity":
				r.Expected.StartTicks++
			}
		}
		if _, err := h.Do(r, state.f.Deadline, nil); err == nil {
			t.Errorf("unsafe host action %s accepted", defect)
		}
		if defect != "job-lost" && (len(m.started) != 0 || len(physical.signals) != 0) {
			t.Errorf("guard %s allowed physical effect", defect)
		}
		if defect == "job-lost" && state.f.Closed {
			t.Error("lost start reply erased durable permit intent")
		}
	}
	if _, err := NewUnitHost(UnitDependencies{}); !errors.Is(err, infracontract.ErrContractStub) {
		t.Fatal("missing real backend accepted")
	}
}

func testUnitStartCallback(t *testing.T) {
	state := &unitTestState{}
	f, r := unitTestFence(t)
	state.f = f
	r.Op = infracontract.ReleaseStart
	var worker sync.Mutex
	callbackDone := make(chan struct{})
	started := make(chan struct{})
	m := &systemdManager{run: func(args []string, d infracontract.Deadline, _ infracontract.Stop) ([]byte, error) {
		if args[2] == "show" {
			return unitFixtureBytes(UnitCaddy, unitFixture(UnitCaddy, false).Expected), nil
		}
		// A start job's launcher calls back into the same authorization owner.
		// Waiting for job completion before releasing that owner deadlocks.
		go func() {
			close(started)
			worker.Lock()
			defer worker.Unlock()
			current, _ := state.Load(infracontract.Caddy, d)
			claimed, err := ClaimStartup(current)
			if err == nil {
				claimRequest := r
				claimRequest.Op, claimRequest.Sequence = infracontract.LaunchClaim, r.Sequence+1
				claimRequest.Nonce[1]++
				_, err = state.CompareAndSwap(current, claimed, claimRequest, d)
			}
			if err != nil {
				t.Error(err)
			}
			close(callbackDone)
		}()
		for _, arg := range args {
			if arg == "--no-block" {
				return nil, nil
			}
		}
		select {
		case <-callbackDone:
			return nil, nil
		case <-time.After(50 * time.Millisecond):
			return nil, ErrUnitUnknown
		}
	}}
	physical := &unitTestPhysical{processes: []infracontract.Process{unitTestProcess(t, os.Getpid())}}
	h, err := NewUnitHost(UnitDependencies{Manager: m, State: state, Physical: physical, Policies: []UnitPolicy{unitFixture(UnitCaddy, false)}, Retire: physical.Do})
	if err != nil {
		t.Fatal(err)
	}
	worker.Lock()
	_, err = h.Do(r, f.Deadline, nil)
	worker.Unlock()
	select {
	case <-started:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("start was never submitted: %v", err)
	}
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("launch callback did not join")
	}
	if err != nil {
		t.Fatalf("start waited for its own launch callback: %v", err)
	}
	if state.f.Boots != 1 {
		t.Fatal("launch callback did not claim the durable permit")
	}
}

// origin: hot-update S9 plan §12 P05 / L: real child and socket, five complete rounds.
func TestInfraUnitLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUnit(t)
	}
	testharness.AssertNoGoroutineLeak(t, "unit bootstrap x5", beforeG)
	testharness.AssertNoFDLeak(t, "unit bootstrap x5", beforeFD, 0)
}

func exerciseUnitCommand(t *testing.T) {
	t.Helper()
	t.Setenv("TETHER_INFRA_TOOL_CANARY", "canary-unit-tool-7628")
	stop := make(chan struct{})
	started := make(chan struct{})
	joined := make(chan struct{})
	go func() { defer close(joined); <-started; close(stop) }()
	start := time.Now()
	_, err := runUnitCommand("/bin/cat", nil, unitTestDeadline(t, time.Second), stop, func(cmd *exec.Cmd) error {
		inputR, inputW, err := os.Pipe()
		if err != nil {
			close(started)
			return err
		}
		defer func() { _ = inputR.Close() }()
		defer func() { _ = inputW.Close() }()
		cmd.Stdin = inputR
		wait, err := testharness.StartProcess(cmd)
		close(started)
		if err != nil {
			return err
		}
		return wait()
	})
	<-joined
	if !errors.Is(err, ErrUnitBudget) || time.Since(start) >= 500*time.Millisecond {
		t.Fatalf("stop did not interrupt and join systemd action: %v", err)
	}
	start = time.Now()
	output, err := runUnitCommand("/usr/bin/env", nil, unitTestDeadline(t, time.Second), nil, testharness.RunProcess)
	if err != nil || strings.Contains(string(output), "canary") || strings.Contains(string(output), "HOME=") || !strings.Contains(string(output), "LANG=C\n") {
		t.Fatal("privileged tool inherits an unaudited environment")
	}
	if time.Since(start) >= 500*time.Millisecond {
		t.Fatal("successful systemd action did not cancel and join its watcher")
	}
}
