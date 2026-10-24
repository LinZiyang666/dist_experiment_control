//go:build linux

package transferharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

type processIdentity struct {
	PID   int
	Start uint64
}

// Sample the external owner too: a direct cmd.Start would otherwise satisfy
// the child PID/hash oracle while abandoning it on fatal test-runner exits.
// origin: S7 internal review rv-lifecycle F7
func captureSupervisedCensus(t *testing.T, p *Process, sha string) []processIdentity {
	t.Helper()
	live, err := p.SampleIdentity()
	if err != nil || live.ProcessStart == 0 || live.BootID == "" || live.RunningSHA != sha {
		t.Fatalf("missing live kernel process identity: %+v: %v", live, err)
	}
	child, parent, err := readProcessIdentity(live.PID)
	if err != nil || child.Start != live.ProcessStart || parent <= 1 || parent == os.Getpid() {
		t.Fatalf("process has no external supervisor: child=%+v parent=%d err=%v", child, parent, err)
	}
	owner, _, err := readProcessIdentity(parent)
	if err != nil || owner.Start == 0 {
		t.Fatalf("supervisor has no live kernel identity: %+v: %v", owner, err)
	}
	return []processIdentity{child, owner}
}

func readProcessIdentity(pid int) (processIdentity, int, error) {
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return processIdentity{}, 0, err
	}
	end := strings.LastIndexByte(string(body), ')')
	fields := strings.Fields(string(body)[end+1:])
	if end < 0 || len(fields) < 20 {
		return processIdentity{}, 0, errors.New("incomplete process identity")
	}
	parent, parentErr := strconv.Atoi(fields[1])
	start, startErr := strconv.ParseUint(fields[19], 10, 64)
	return processIdentity{PID: pid, Start: start}, parent, errors.Join(parentErr, startErr)
}

// Include zombies, and fail on unreadable evidence. Absence or a different
// start time proves this incarnation retired; a numeric PID alone cannot.
// origin: S7 internal review rv-lifecycle F7
func assertSupervisedCensusRetired(t *testing.T, census []processIdentity) {
	t.Helper()
	for _, previous := range census {
		current, _, err := readProcessIdentity(previous.PID)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || current == previous {
			t.Errorf("supervised process survived join or census failed: %+v: %v", previous, err)
		}
	}
}

// Real: a runner, its child and both external supervisors. Every launch goes
// through the transfer adapter; ordinary Stop and cancellation must join all
// recorded PID/start pairs before the oracle permits emergency cleanup.
// Fatal panic/timeout/SIGKILL and escaped double-fork behavior additionally use
// the shared supervisor's independent retained-runner fault suite.
// origin: S7 internal review rv-lifecycle F7
func TestTransferHarnessStopJoinsLiveProcessTree(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for _, action := range []string{"stop", "cancel"} {
		t.Run(action, func(t *testing.T) {
			for round := 0; round < 5; round++ {
				exerciseTransferProcessTree(t, action)
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "transfer process tree", g)
	testharness.AssertNoFDLeak(t, "transfer process tree", fds, 0)
}

func exerciseTransferProcessTree(t *testing.T, action string) {
	t.Helper()
	if SharedSupervisor == nil {
		t.Fatal("required transfer harness has no shared process supervisor")
	}
	h := New(t, SharedSupervisor(t))
	ctx, cancel := context.WithCancel(t.Context())
	defer func() {
		cancel()
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	}()
	spec := privateProbeBinary(t, h)
	spec.Args = []string{"-test.run=^TestTransferHarnessProcessTreeProbe$", "-test.count=1"}
	spec.Env = []string{"TRANSFER_HARNESS_TREE=root"}
	p, err := h.Start(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	var census []processIdentity
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
		body, err := os.ReadFile(p.Receipt().StdoutPath)
		return err == nil && json.Unmarshal(body, &census) == nil && len(census) == 4
	}) {
		t.Fatal("supervised tree did not publish all four process identities")
	}
	seen := make(map[int]bool)
	for _, id := range census {
		current, _, err := readProcessIdentity(id.PID)
		if err != nil || current != id || id.Start == 0 || seen[id.PID] {
			t.Fatalf("missing or aliased live tree identity: %+v: %v", id, err)
		}
		seen[id.PID] = true
	}
	if !seen[p.Receipt().PID] || seen[os.Getpid()] {
		t.Fatal("tree census omitted the original runner or claimed its caller")
	}
	done := make(chan error, 1)
	if action == "cancel" {
		cancel()
	}
	go func() {
		if action == "stop" {
			done <- p.Stop()
			return
		}
		<-p.done
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		// Cancellation is only emergency cleanup after the oracle has failed;
		// it cannot turn a no-op Stop mutation into a successful tree join.
		cancel()
		<-done
		t.Fatal("supervised tree did not join after " + action)
	}
	assertSupervisedCensusRetired(t, census)
	receipt, err := p.Wait(t.Context())
	if err == nil || receipt.NativeRC != -1 {
		t.Fatalf("killed runner lost its native signal outcome: %+v: %v", receipt, err)
	}
}

// origin: S7 internal review rv-lifecycle F7
func TestTransferHarnessProcessTreeProbe(t *testing.T) {
	role := os.Getenv("TRANSFER_HARNESS_TREE")
	if role != "root" && role != "leaf" {
		return
	}
	if role == "leaf" {
		if err := json.NewEncoder(os.Stdout).Encode(os.Getpid()); err != nil {
			t.Fatal(err)
		}
		<-time.NewTimer(time.Hour).C
		return
	}
	h := New(t, SharedSupervisor(t))
	spec := privateProbeBinary(t, h)
	spec.Args = []string{"-test.run=^TestTransferHarnessProcessTreeProbe$", "-test.count=1"}
	spec.Env = []string{"TRANSFER_HARNESS_TREE=leaf"}
	p, err := h.Start(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
		body, err := os.ReadFile(p.Receipt().StdoutPath)
		var pid int
		return err == nil && json.Unmarshal(body, &pid) == nil && pid == p.Receipt().PID
	}) {
		t.Fatal("supervised leaf did not publish its PID")
	}
	census := captureSupervisedCensus(t, p, spec.ArtifactSHA)
	root, parent, err := readProcessIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	owner, _, err := readProcessIdentity(parent)
	if err != nil {
		t.Fatal(err)
	}
	census = append(census, root, owner)
	if err := json.NewEncoder(os.Stdout).Encode(census); err != nil {
		t.Fatal(err)
	}
	<-time.NewTimer(time.Hour).C
}
