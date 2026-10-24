//go:build linux

package agentimage

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

func flowSSMetadata() (*tunnel.ClientImage, *ssproxy.Image) {
	id := strings.Repeat("1", 32)
	return &tunnel.ClientImage{Version: flow.Version, StateRef: "flow-state." + id,
			Flows: flow.Image{Version: flow.Version, StateRef: "flow-state." + id}},
		&ssproxy.Image{Version: ssproxy.Version, StateRef: "ss-state." + id}
}

func zeroSSGrant() ssproxy.Grant { return ssproxy.Grant{} }

// Real: original SS listener and key table, frozen SS exporter, Runtime and
// handoff.Prepare. No P0 success substitute is used: an unimplemented owner
// fails this test and its remaining assertions are reported pending that owner.
// origin: hot-update S5 plan §5.3 / T33
func TestBundleFinalReexportIncludesFlowSSRevoke(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseBundleFinalReexportIncludesFlowSSRevoke(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "final-flow-SS-reexport", beforeG)
	testharness.AssertNoFDLeak(t, "final-flow-SS-reexport", beforeFD, 0)
}

func exerciseBundleFinalReexportIncludesFlowSSRevoke(t *testing.T) {
	f := newBundleFixture(t, "final-reexport")
	ss := ssproxy.New(testharness.SilentLog())
	if _, err := ss.Start(0, []ssproxy.Key{{KeyID: "retained-test-material", Secret: "private-test-material"}}); err != nil {
		t.Fatal(err)
	}
	defer ss.Stop()
	// Retain SS listener/material responsibility and a real bidirectional flow.
	client, peerAddress := liveImageFlowPeer(t, execsession.SharedReplayBudget(f.rt), nil)
	awaitPeerCarrierClose := imageCarrierCloseBarrier(t, peerAddress)
	cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, SS: ss, Flow: client, Lineage: Lineage{Generation: 2}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatalf("real flow/SS cut unavailable: %v", err)
	}
	defer func() {
		if !cut.done {
			_ = cut.Thaw()
		}
	}()
	// Freeze joins the client carrier. Its real server peer is another
	// goroutine in this process and must close before an exact FD baseline.
	// Otherwise a correct asynchronous peer close looks like a leaked cut.
	// origin: hot-update S5 plan §8 / T33 / DRILL-RUN FD census.
	awaitPeerCarrierClose()
	before := cut.Bundle()
	if before.Flow == nil || before.SS == nil {
		t.Fatal("retained domain absent from initial cut")
	}
	if len(before.Flow.Flows.Records) != 1 {
		t.Fatal("flow precondition: original business owner is absent")
	}
	beforeFlowState := stateHash(t, cut.resources, before.Flow.StateRef)
	beforeState := stateHash(t, cut.resources, before.SS.StateRef)
	previous := append([]*os.File(nil), cut.snapshots[len(cut.snapshots)-1].Owned...)
	if len(previous) == 0 {
		t.Fatal("SS export produced no owned sealed state")
	}
	if err := client.RevokeFlow(before.Flow.Flows.Records[0].ID, flow.ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	if err := ss.RevokeAll("revoked"); err != nil {
		t.Fatal(err)
	}
	checked := false
	packed, err := cut.PrepareChecked(func(b Bundle) error {
		checked = true
		if b.Flow == nil || b.SS == nil || strings.Join(b.RequiredDomains(), ",") != "exec-v1,flow-v1,ss-v1" {
			return errors.New("retained domain lost in final cut")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := packed.Close(); err != nil {
		t.Fatal(err)
	}
	if !checked {
		t.Fatal("final-cut compatibility check was bypassed")
	}
	if stateHash(t, cut.resources, cut.Bundle().SS.StateRef) == beforeState {
		t.Fatal("final re-export reused SS state from before the late revoke")
	}
	if stateHash(t, cut.resources, cut.Bundle().Flow.StateRef) == beforeFlowState {
		t.Fatal("final re-export reused flow state from before the late revoke")
	}
	for _, file := range previous {
		if _, err := file.Stat(); err == nil {
			t.Fatal("replaced sealed snapshot remains open")
		}
	}
	if err := validateCutResources(cut.Bundle(), cut.resources); err != nil {
		t.Fatal(err)
	}
	// Every retry is bounded in descriptor ownership, including a failed check.
	for attempt := 0; attempt < 5; attempt++ {
		fdBefore, beforeTargets := imageDescriptorCensus(t)
		retainedFlow := stateHash(t, cut.resources, cut.Bundle().Flow.StateRef)
		retainedSS := stateHash(t, cut.resources, cut.Bundle().SS.StateRef)
		reject := errors.New("reader refuses final cut")
		if p, err := cut.PrepareChecked(func(Bundle) error { return reject }); p != nil || !errors.Is(err, reject) {
			t.Fatal("failed final check replaced the retained cut")
		}
		if after, afterTargets := imageDescriptorCensus(t); after != fdBefore {
			t.Fatalf("failed re-export changed descriptor count: before=%d after=%d before_targets=%v after_targets=%v", fdBefore, after, beforeTargets, afterTargets)
		}
		if stateHash(t, cut.resources, cut.Bundle().Flow.StateRef) != retainedFlow || stateHash(t, cut.resources, cut.Bundle().SS.StateRef) != retainedSS {
			t.Fatal("failed final check invalidated the retained cut")
		}
	}
}

// A real SS exporter closes its prior snapshot before the final reader check.
// The composed owner must keep that cut usable through repeated refusals.
// origin: hot-update S5 plan §5.3 / T33
func TestFinalReexportRefusalKeepsSealedCut(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFinalReexportRefusal(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "refused-domain-reexport", beforeG)
	testharness.AssertNoFDLeak(t, "refused-domain-reexport", beforeFD, 0)
}

func exerciseFinalReexportRefusal(t *testing.T) {
	f := newBundleFixture(t, "refused-reexport")
	ss := ssproxy.New(testharness.SilentLog())
	if _, err := ss.Start(0, []ssproxy.Key{{KeyID: "retained-test-material", Secret: "private-test-material"}}); err != nil {
		t.Fatal(err)
	}
	defer ss.Stop()
	cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, SS: ss}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cut.Thaw() }()
	digest := stateHash(t, cut.resources, cut.Bundle().SS.StateRef)
	reject := errors.New("reader refuses final cut")
	for attempt := 0; attempt < 5; attempt++ {
		beforeFD := testharness.FDCount()
		if packed, err := cut.PrepareChecked(func(Bundle) error { return reject }); packed != nil || !errors.Is(err, reject) {
			t.Fatal("final reader refusal was ignored")
		}
		if stateHash(t, cut.resources, cut.Bundle().SS.StateRef) != digest {
			t.Fatal("reader refusal invalidated the previous sealed cut")
		}
		if testharness.FDCount() != beforeFD {
			t.Fatal("reader refusal leaked a replacement sealed file")
		}
	}
	previous := append([]*os.File(nil), cut.snapshots[len(cut.snapshots)-1].Owned...)
	packed, err := cut.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if err := packed.Close(); err != nil {
		t.Fatal(err)
	}
	for _, file := range previous {
		if _, err := file.Stat(); err == nil {
			t.Fatal("successful replacement retained the old sealed file")
		}
	}
}

// Tunnel route state is borrowed by its root snapshot. It still counts as
// private state and must survive exporter replacement with the composed cut.
// origin: hot-update S5 plan §3.4 / §5.3 / T33
func TestComposedSnapshotRetainsBorrowedEndpointState(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseRetainedEndpointState(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "retained-endpoint-state", beforeG)
	testharness.AssertNoFDLeak(t, "retained-endpoint-state", beforeFD, 0)
}

func exerciseRetainedEndpointState(t *testing.T) {
	root := imageSealedFile(t, []byte("root state"))
	route := imageSealedFile(t, []byte("private route"))
	raw, peer := imageSocketPair(t)
	snapshot := &sealedstate.Snapshot{Owned: []*os.File{root}, Resources: []handoff.Resource{
		{ID: "flow-state." + strings.Repeat("1", 32), Kind: "sealed-state-v1", Files: map[string]*os.File{"state": root}},
		{ID: "flow-endpoint." + strings.Repeat("2", 32), Kind: "sealed-state-v1", Files: map[string]*os.File{"state": route}},
		{ID: "flow." + strings.Repeat("3", 32), Kind: "flow-v1", Files: map[string]*os.File{"raw": raw}},
	}}
	retained, err := retainDomainSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = retained.Close() }()
	if len(retained.Owned) != 2 || retained.Resources[2].Files["raw"] != raw {
		t.Fatal("retention omitted endpoint state or duplicated a business owner")
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := route.Close(); err != nil {
		t.Fatal(err)
	}
	for i, payload := range []string{"root state", "private route"} {
		file := retained.Resources[i].Files["state"]
		data := make([]byte, len(payload))
		if _, err := file.ReadAt(data, 0); err != nil || string(data) != payload {
			t.Fatal("exporter replacement invalidated retained private state")
		}
		rawConn, err := file.SyscallConn()
		if err != nil {
			t.Fatal(err)
		}
		flags, checkErr := 0, error(nil)
		if err := rawConn.Control(func(fd uintptr) { flags, checkErr = unix.FcntlInt(fd, unix.F_GETFD, 0) }); err != nil || checkErr != nil || flags&unix.FD_CLOEXEC == 0 {
			t.Fatal("retained sealed state lacks CLOEXEC")
		}
	}
	if err := retained.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Write([]byte("live")); err != nil {
		t.Fatal("sealed cleanup closed the borrowed business socket")
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(peer, got); err != nil || string(got) != "live" {
		t.Fatal("original business socket stopped communicating")
	}
	// A failure after the first duplicate must release that partial ownership
	// without closing any borrowed business file or leaving a candidate cut.
	partial := imageSealedFile(t, []byte("partial state"))
	bad := &sealedstate.Snapshot{Owned: []*os.File{partial}, Resources: []handoff.Resource{
		{ID: "first", Kind: "sealed-state-v1", Files: map[string]*os.File{"state": partial}},
		{ID: "second", Kind: "sealed-state-v1", Files: map[string]*os.File{"state": nil}},
	}}
	beforeFD := testharness.FDCount()
	if candidate, err := retainDomainSnapshot(bad); candidate != nil || !errors.Is(err, sealedstate.ErrInvalid) {
		t.Fatal("incomplete sealed snapshot was retained")
	}
	if testharness.FDCount() != beforeFD-1 {
		t.Fatal("failed snapshot retention leaked a partial duplicate")
	}
}

// Admission into a retained flow domain selects one receiving-node budget;
// explicit disagreement must be refused before any importer takes a file.
// origin: hot-update S5 plan §3.4 / §5.3 / T30
func TestBundleImportSharesNodeReplayBudget(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseTargetReplayBudget(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "target-node-replay-budget", beforeG)
	testharness.AssertNoFDLeak(t, "target-node-replay-budget", beforeFD, 0)
}

func exerciseTargetReplayBudget(t *testing.T) {
	for _, source := range []string{"implicit", "runtime", "flow", "both"} {
		t.Run(source, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final subtest process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			f := newBundleConfig(t, "target-budget")
			target := Target{Runtime: f.cfg}
			budget := &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}
			if source == "runtime" || source == "both" {
				target.Runtime.SharedReplayBudget = budget
			}
			if source == "flow" || source == "both" {
				target.Flow.Budget = budget
			}
			resolved, err := shareTargetReplayBudget(target)
			if err != nil {
				t.Fatal(err)
			}
			if source == "implicit" {
				budget = resolved.Runtime.SharedReplayBudget
			}
			if budget == nil || resolved.Runtime.SharedReplayBudget != budget || resolved.Flow.Budget != budget {
				t.Fatal("target copied or split the node reservation owner")
			}
			if budget.Limit != workload.DefaultReplayBudget {
				t.Fatal("target replaced the receiving node limit")
			}
			f.rt, err = execsession.New(resolved.Runtime)
			if err != nil {
				t.Fatal(err)
			}
			if execsession.SharedReplayBudget(f.rt) != budget {
				t.Fatal("runtime accessor returned a different reservation owner")
			}
			log, err := workload.NewReplay(resolved.Flow.Budget, workload.DefaultReplayCapacity)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = log.Close() }()
			if execsession.SharedReplayBudget(f.rt).Used() != workload.DefaultReplayCapacity || f.rt.ReplayBudgetUsed() != 0 {
				t.Fatal("runtime and flow did not observe the same node accounting")
			}
		})
	}
	f := newBundleFixture(t, "conflicting-budget")
	cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cut.Thaw() }()
	b := cut.Bundle()
	b.Flow, _ = flowSSMetadata()
	for _, target := range []Target{
		{Runtime: execsession.Config{Manager: f.manager, SharedReplayBudget: &workload.ReplayBudget{}}, Flow: tunnel.FlowConfig{Budget: &workload.ReplayBudget{}}},
		{Runtime: execsession.Config{Manager: f.manager, ReplayBudget: 1}},
		{Runtime: execsession.Config{Manager: f.manager, SharedReplayBudget: &workload.ReplayBudget{Limit: -1}}},
		{Runtime: execsession.Config{Manager: f.manager}, Flow: tunnel.FlowConfig{Budget: &workload.ReplayBudget{Limit: 1}}},
	} {
		src := &targetBudgetFileSource{}
		imp, salvage, err := Import(b, nil, src, target)
		if imp != nil || !errors.Is(err, ErrBundle) || salvage == nil || src.takes != 0 || len(salvage.Resources) != 0 {
			t.Fatal("conflicting target accounting was not refused before import")
		}
	}
}

type targetBudgetFileSource struct{ takes int }

func (s *targetBudgetFileSource) TakeFile(string, string) (*os.File, error) {
	s.takes++
	return nil, errors.New("unexpected file transfer")
}

// A private state digest is an oracle only; bytes are never printed or written
// to an ordinary file. It observes the real sealed FD Prepare will duplicate.
func stateHash(t *testing.T, resources []handoff.Resource, id string) [32]byte {
	t.Helper()
	for _, resource := range resources {
		if resource.ID == id {
			file := resource.Files["state"]
			info, err := file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() <= 0 || info.Size() > sealedstate.MaxStateBytes {
				t.Fatal("invalid private-state size")
			}
			hash := sha256.New()
			if _, err := io.Copy(hash, io.NewSectionReader(file, 0, info.Size())); err != nil {
				t.Fatal(err)
			}
			var digest [32]byte
			copy(digest[:], hash.Sum(nil))
			return digest
		}
	}
	t.Fatal("state resource absent")
	return [32]byte{}
}

// Real: memfd state, a live socket pair, the composition resource merge and
// handoff's descriptor duplication. This isolates resource responsibility;
// it does not replace any flow/SS runtime or claim their codec round-trip.
// origin: hot-update S5 plan §5.3 / T33
func TestComposedSnapshotResourcesRemainComplete(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseComposedSnapshotResources(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "composed-snapshots", beforeG)
	testharness.AssertNoFDLeak(t, "composed-snapshots", beforeFD, 0)
}

func exerciseComposedSnapshotResources(t *testing.T) {
	f := newBundleFixture(t, "snapshot-resources")
	cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !cut.done {
			_ = cut.Thaw()
		}
	}()
	b := cut.Bundle()
	b.Flow, b.SS = flowSSMetadata()
	b.Flow.Flows.StateRef = "flow-state." + strings.Repeat("3", 32)
	raw, peer := imageSocketPair(t)
	flowState := imageSealedFile(t, []byte("flow private state"))
	innerFlowState := imageSealedFile(t, []byte("inner flow private state"))
	ssState := imageSealedFile(t, []byte("SS private state"))
	flowSnapshot := &sealedstate.Snapshot{Owned: []*os.File{flowState, innerFlowState}, Resources: []handoff.Resource{
		{ID: b.Flow.StateRef, Kind: "flow-state", Files: map[string]*os.File{"state": flowState}},
		{ID: b.Flow.Flows.StateRef, Kind: "flow-state", Files: map[string]*os.File{"state": innerFlowState}},
		{ID: "flow." + strings.Repeat("2", 32), Kind: "flow", Files: map[string]*os.File{"raw": raw}},
	}}
	ssSnapshot := &sealedstate.Snapshot{Owned: []*os.File{ssState}, Resources: []handoff.Resource{
		{ID: b.SS.StateRef, Kind: "ss-state", Files: map[string]*os.File{"state": ssState}},
	}}
	snapshots := []*sealedstate.Snapshot{flowSnapshot, ssSnapshot}
	resources, err := mergeDomainResources(cut.resources, snapshots)
	if err != nil {
		t.Fatal(err)
	}
	if len(resources) != 5 || resources[0].ID != calibrationResourceID || len(resources[0].Files) != 1 || resources[0].Files["state"] == nil {
		t.Fatalf("merge dropped a domain or calibration custody: resources=%d", len(resources))
	}
	if err := validateCutResources(b, resources); err != nil {
		t.Fatal(err)
	}
	for _, lost := range []string{b.Flow.StateRef, b.Flow.Flows.StateRef, b.SS.StateRef} {
		var incomplete []handoff.Resource
		for _, resource := range resources {
			if resource.ID != lost {
				incomplete = append(incomplete, resource)
			}
		}
		if err := validateCutResources(b, incomplete); err == nil {
			t.Fatal("missing domain state was accepted")
		}
	}
	data, err := Encode(b)
	if err != nil {
		t.Fatal(err)
	}
	packed, err := handoff.Prepare(handoff.State{Role: Role, Instance: b.Instance, Generation: b.Lineage.Generation, Data: data}, resources)
	if err != nil {
		t.Fatal(err)
	}
	if err := packed.Close(); err != nil {
		t.Fatal(err)
	}
	for _, file := range []*os.File{flowState, innerFlowState, ssState, raw} {
		if _, err := file.Stat(); err != nil {
			t.Fatal("Prepared closed a borrowed original")
		}
	}
	if err := closeSnapshots(snapshots); err != nil {
		t.Fatal(err)
	}
	for _, file := range []*os.File{flowState, innerFlowState, ssState} {
		if _, err := file.Stat(); err == nil {
			t.Fatal("cut-owned state descriptor not closed")
		}
	}
	if err := closeSnapshots(snapshots); err != nil {
		t.Fatal("snapshot close is not idempotent")
	}
	if _, err := raw.Write([]byte("still-live")); err != nil {
		t.Fatal("snapshot cleanup closed the original business FD")
	}
	got := make([]byte, len("still-live"))
	if _, err := io.ReadFull(peer, got); err != nil || !bytes.Equal(got, []byte("still-live")) {
		t.Fatal("original business socket stopped communicating")
	}
}

func imageSealedFile(t *testing.T, data []byte) *os.File {
	t.Helper()
	fd, err := unix.MemfdCreate("image-test-state", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(fd), "image-state")
	t.Cleanup(func() { _ = file.Close() })
	if _, err := file.Write(data); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL); err != nil {
		t.Fatal(err)
	}
	return file
}

func imageSocketPair(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	a, b := os.NewFile(uintptr(pair[0]), "original-business"), os.NewFile(uintptr(pair[1]), "business-peer")
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return a, b
}

// Even an otherwise valid local runtime cut must not manufacture a successful
// runtime-only re-export when its retained flow/SS owner was omitted.
// origin: hot-update S5 plan §5.2 / T33
func TestFinalReexportRequiresRetainedDomainOwners(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFinalReexportRequiresOwners(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "final-cut-domain-owners", beforeG)
	testharness.AssertNoFDLeak(t, "final-cut-domain-owners", beforeFD, 0)
}

func exerciseFinalReexportRequiresOwners(t *testing.T) {
	f := newBundleFixture(t, "missing-domain-owner")
	cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !cut.done {
			_ = cut.Thaw()
		}
	}()
	for _, which := range []string{"flow", "SS"} {
		previous := cut.bundle
		flow, ss := flowSSMetadata()
		if which == "flow" {
			cut.bundle.Flow = flow
		} else {
			cut.bundle.SS = ss
		}
		if p, err := cut.Prepare(); p != nil || !errors.Is(err, ErrBundle) {
			if p != nil {
				_ = p.Close()
			}
			t.Fatalf("runtime-only final re-export accepted omitted %s owner", which)
		}
		cut.bundle = previous
	}
}

// Public error text cannot relay private parser values from an owner. Error
// identity remains available for fail-closed classification via errors.Is.
// origin: hot-update S5 plan §6.6
func TestDomainDeferralRedactsPrivateOwnerErrors(t *testing.T) {
	secret := privateImageOwnerError{detail: "private-master-salt-target"}
	got := domainDeferral("restore SS", secret)
	if !errors.Is(got, secret) || strings.Contains(got.Error(), secret.Error()) {
		t.Fatal("domain error exposed private state or lost error identity")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		if strings.Contains(fmt.Sprintf(format, got), secret.detail) {
			t.Fatal("formatted domain error exposed private state")
		}
	}
	for _, tc := range []struct {
		cause  error
		reason string
	}{
		{ssproxy.ErrStateUnrepresentable, ReasonSSStateUnrepresentable},
		{ssproxy.ErrCapacity, ReasonImageTooLarge},
		{sealedstate.ErrCapacity, ReasonImageTooLarge},
		{sealedstate.ErrUnsupported, ReasonPlatformUnsupported},
	} {
		var deferred *DeferredError
		if !errors.As(domainDeferral("freeze SS", tc.cause), &deferred) || deferred.Reason != tc.reason {
			t.Fatal("semantic and capacity refusal reasons were conflated")
		}
	}
}

// A real TLS/yamux endpoint with one original target socket. The test itself
// exchanges bytes with the accepted target, so no reconnection-capable echo
// fixture can hide loss of ownership across the cut.
func liveImageFlow(t *testing.T, budget *workload.ReplayBudget) *tunnel.Client {
	t.Helper()
	return liveImageFlowWithAuthority(t, budget, nil)
}

func liveImageFlowWithAuthority(t *testing.T, budget *workload.ReplayBudget, authorize func(tunnel.AuthRequest) error) *tunnel.Client {
	t.Helper()
	client, _ := liveImageFlowPeer(t, budget, authorize)
	return client
}

func liveImageFlowPeer(t *testing.T, budget *workload.ReplayBudget, authorize func(tunnel.AuthRequest) error) (*tunnel.Client, string) {
	t.Helper()
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		t.Fatal(err)
	}
	local, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })
	localPort := local.Addr().(*net.TCPAddr).Port
	lookup := func(req tunnel.AuthRequest) error {
		if req.Allocation.SID != "image-session" || req.Allocation.NID != "image-node" || req.Allocation.Epoch != 1 {
			return errors.New("authority mismatch")
		}
		return nil
	}
	cfg := tunnel.FlowConfig{Budget: budget,
		InstanceID: "final-reexport", BootID: boot, OwnerGen: 1,
		MaxEndpoints: tunnel.MaxEndpoints, MaxPendingFDs: flow.MaxPendingFDs,
		TokenLookup: lookup, Authorize: func(req tunnel.AuthRequest) (tunnel.Authority, error) {
			if err := lookup(req); err != nil {
				return tunnel.Authority{}, err
			}
			if authorize != nil {
				if err := authorize(req); err != nil {
					return tunnel.Authority{}, err
				}
			}
			return tunnel.Authority{Enabled: true, Allocation: req.Allocation, Generation: 1, ProxyEpoch: 1, AppliedRevision: 1}, nil
		}}
	// Probe the frozen runtime API before opening a carrier. This is a real
	// refusal while P1 is a compile-stub, never a fixture-success fallback.
	client := tunnel.NewClient("", "image-session", "image-node", func(int) (int, error) { return localPort, nil }, testharness.SilentLog())
	if err := client.SetFlowConfig(cfg); err != nil {
		t.Fatalf("real flow configuration unavailable: %v", err)
	}
	cert, pins := imageFlowCertificate(t)
	var server *tunnel.Server
	var address string
	for attempt := 0; attempt < 16; attempt++ {
		address = imageVacantTCPAddress(t)
		server = tunnel.NewServerWithCert(address, "127.0.0.1", func(sid, nid string, port int, hash string, epoch int64) error {
			digest := sha256.Sum256([]byte("image-flow-token"))
			if sid != "image-session" || nid != "image-node" || hash != fmt.Sprintf("%x", digest) || epoch != 1 {
				return errors.New("token mismatch")
			}
			return nil
		}, &cert, testharness.SilentLog())
		brokerCfg := cfg
		brokerCfg.Budget = &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}
		if err := server.SetFlowConfig(brokerCfg); err != nil {
			t.Fatal(err)
		}
		err = server.Start(t.Context())
		if err == nil {
			break
		}
		server.Close()
		if !errors.Is(err, unix.EADDRINUSE) {
			t.Fatal(err)
		}
	}
	if err != nil {
		t.Fatal("could not bind the flow control fixture")
	}
	t.Cleanup(server.Close)
	client.Start(t.Context())
	publicPort := 0
	for attempt := 0; attempt < 16; attempt++ {
		_, port, splitErr := net.SplitHostPort(imageVacantTCPAddress(t))
		if splitErr != nil {
			t.Fatal(splitErr)
		}
		publicPort, _ = strconv.Atoi(port)
		err = client.OpenHome(t.Context(), publicPort, localPort, "image-flow-token", address, 1, pins)
		if err == nil {
			break
		}
		client.Close(publicPort)
		if !errors.Is(err, unix.EADDRINUSE) {
			t.Fatal(err)
		}
	}
	if err != nil {
		t.Fatal("could not bind the flow public fixture")
	}
	t.Cleanup(func() { client.Close(publicPort) })
	public, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)), bundleTestBudget)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = public.Close() })
	if err := public.SetDeadline(time.Now().Add(bundleTestBudget)); err != nil {
		t.Fatal(err)
	}
	if err := local.(*net.TCPListener).SetDeadline(time.Now().Add(bundleTestBudget)); err != nil {
		t.Fatal(err)
	}
	target, err := local.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })
	if err := target.SetDeadline(time.Now().Add(bundleTestBudget)); err != nil {
		t.Fatal(err)
	}
	message := []byte("original-flow-duplex")
	if _, err := public.Write(message); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(message))
	if _, err := io.ReadFull(target, got); err != nil || !bytes.Equal(got, message) {
		t.Fatal("forward flow precondition failed")
	}
	if _, err := target.Write(message); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(public, got); err != nil || !bytes.Equal(got, message) {
		t.Fatal("reverse flow precondition failed")
	}
	// No further Accept is performed by the fixture; a redial cannot get its
	// reply and cannot masquerade as the preserved original owner.
	return client, address
}

func imageVacantTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func imageFlowCertificate(t *testing.T) (tls.Certificate, proto.CertPins) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}, proto.CertPins{Current: tunnel.CertFingerprint(leaf)}
}

// A per-domain size check accepts two individually valid 32 MiB snapshots.
// The composition must reject their sum before Prepared duplicates any files.
// origin: hot-update S5 plan §3.4 / §5.3
func TestComposedSealedStateBoundCountsAllDomains(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseComposedSealedStateBound(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "composed-sealed-bound", beforeG)
	testharness.AssertNoFDLeak(t, "composed-sealed-bound", beforeFD, 0)
}

func exerciseComposedSealedStateBound(t *testing.T) {
	// Sparse memfds keep this a size/ownership test, not a heap load test.
	var snapshots []*sealedstate.Snapshot
	for domain := 0; domain < 2; domain++ {
		fd, err := unix.MemfdCreate("image-bound-state", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
		if err != nil {
			t.Fatal(err)
		}
		file := os.NewFile(uintptr(fd), "state")
		t.Cleanup(func() { _ = file.Close() })
		if err := unix.Ftruncate(fd, sealedstate.MaxStateBytes); err != nil {
			t.Fatal(err)
		}
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL); err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, &sealedstate.Snapshot{Owned: []*os.File{file}})
	}
	if _, err := mergeDomainResources(nil, snapshots[:1]); err != nil {
		t.Fatal("single snapshot at the bound was refused")
	}
	var deferred *DeferredError
	if _, err := mergeDomainResources(nil, snapshots); !errors.As(err, &deferred) || deferred.Reason != ReasonImageTooLarge {
		t.Fatal("combined sealed-state overflow was accepted")
	}
	if err := closeSnapshots(snapshots); err != nil {
		t.Fatal(err)
	}
}

func metadataEndpoints(count int) []tunnel.EndpointImage {
	endpoints := make([]tunnel.EndpointImage, count)
	for i := range endpoints {
		id := flow.EndpointID{byte(i + 1)}
		endpoints[i] = tunnel.EndpointImage{ID: id, OwnerGen: 1, StateRef: fmt.Sprintf("flow-endpoint.%x", id)}
	}
	return endpoints
}

type privateImageOwnerError struct{ detail string }

func (e privateImageOwnerError) Error() string { return e.detail }

// origin: hot-update S5 internal review rv-image-2
func TestBundleFlowCapacitySeparatesActiveAndTerminalRecords(t *testing.T) {
	f := newBundleFixture(t, "capacity-bundle")
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Lineage: Lineage{Generation: 2}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(frozen.Thaw)
	for _, tc := range []struct {
		active, terminal int
		valid            bool
	}{{0, 33, true}, {1, flow.MaxTerminalMetadata, true}, {flow.MaxFlows, flow.MaxTerminalMetadata, true}, {flow.MaxFlows + 1, 0, false}, {0, flow.MaxTerminalMetadata + 1, false}} {
		b := frozen.Bundle()
		b.Flow, _ = flowSSMetadata()
		ep := tunnel.EndpointImage{ID: flow.EndpointID{1}, OwnerGen: 1, StateRef: b.Flow.StateRef}
		for i := 0; i < tc.active+tc.terminal; i++ {
			id := flow.ID{1}
			binary.BigEndian.PutUint64(id[8:], uint64(i+1))
			phase := flow.FlowPhaseTerminal
			if i < tc.active {
				phase = flow.FlowPhaseOpening
			}
			b.Flow.Flows.Records = append(b.Flow.Flows.Records, flow.RecordImage{ID: id, OpenSeq: uint64(i + 1), Phase: phase})
			ep.FlowIDs = append(ep.FlowIDs, id)
		}
		b.Flow.Endpoints = []tunnel.EndpointImage{ep}
		if err := b.Validate(); (err == nil) != tc.valid {
			t.Fatalf("active=%d terminal=%d valid=%v: %v", tc.active, tc.terminal, tc.valid, err)
		}
	}
}

func imageDescriptorTargets(t *testing.T) map[string]string {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	targets := make(map[string]string)
	for _, entry := range entries {
		if target, err := os.Readlink("/proc/self/fd/" + entry.Name()); err == nil {
			targets[entry.Name()] = target
		}
	}
	return targets
}

// Capture identities before closing the enumerating directory. A second
// ReadDir can miss the transient descriptor responsible for an exact-count
// failure. This keeps the same count (including the directory itself), and
// records vanished entries rather than silently dropping their identities.
// origin: hot-update S5 plan §8 / T33 / DRILL-RUN FD census.
func imageDescriptorCensus(t *testing.T) (int, map[string]string) {
	t.Helper()
	directory, err := os.Open("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = directory.Close() }()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		t.Fatal(err)
	}
	targets := make(map[string]string, len(entries))
	for _, entry := range entries {
		target, err := os.Readlink("/proc/self/fd/" + entry.Name())
		if err != nil {
			targets[entry.Name()] = "closed during census"
			continue
		}
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil {
			targets[entry.Name()] = target + " (closed during identity check)"
			continue
		}
		targets[entry.Name()] = fmt.Sprintf("%s dev=%d ino=%d", target, stat.Dev, stat.Ino)
	}
	return len(entries), targets
}

// Capture the peer's original kernel descriptor while duplex traffic proves it
// is live. Count equality becomes meaningful only after that exact FD retires;
// neither repeated count sampling nor a delay proves this ownership boundary.
// origin: hot-update S5 plan §8 / T33 / DRILL-RUN FD census.
func imageCarrierCloseBarrier(t *testing.T, address string) func() {
	t.Helper()
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	local := fmt.Sprintf("%08X:%04X", binary.LittleEndian.Uint32(net.ParseIP(host).To4()), port)
	data, err := os.ReadFile("/proc/self/net/tcp")
	if err != nil {
		t.Fatal(err)
	}
	var targets []string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 10 && fields[1] == local && fields[3] == "01" {
			targets = append(targets, "socket:["+fields[9]+"]")
		}
	}
	if len(targets) != 1 {
		t.Fatalf("expected one original peer carrier, got %d", len(targets))
	}
	var descriptors []string
	for fd, target := range imageDescriptorTargets(t) {
		if target == targets[0] {
			descriptors = append(descriptors, "/proc/self/fd/"+fd)
		}
	}
	if len(descriptors) != 1 {
		t.Fatalf("peer carrier descriptor owners=%d", len(descriptors))
	}
	return func() {
		t.Helper()
		deadline := time.NewTimer(bundleTestBudget)
		defer deadline.Stop()
		poll := time.NewTicker(time.Millisecond)
		defer poll.Stop()
		for {
			target, err := os.Readlink(descriptors[0])
			if errors.Is(err, os.ErrNotExist) || err == nil && target != targets[0] {
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-poll.C:
			case <-deadline.C:
				t.Fatal("original peer carrier did not close after Freeze")
			}
		}
	}
}
