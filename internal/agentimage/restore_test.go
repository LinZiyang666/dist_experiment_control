//go:build linux

// restore_test.go — the late-failure path of Restore/Import: a bundle that
// passed every validation, whose runtime was built and whose process table
// was installed, and then a domain owner refuses (spawn verdicts or courier
// outbox). importDomains must hand the runtime's descriptors back through
// Runtime.Detach AND release the freshly restored table through
// Manager.Discard (pidfds), leaving the child, the exporter's table and the
// exporter's budget exactly as they were. bundle_test.go's rejection table
// stops before RestoreProcesses; this is the half after it.
// origin: hot-update S2 internal review, resources lane.
package agentimage

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// refusingCourier is a CourierImporter whose owner is not ready AT INSTALL
// time although its pure Check passed (the agent's courier refuses an outbox
// when its own pending set is non-empty — a race the pre-check cannot see).
type refusingCourier struct{ calls int }

func (c *refusingCourier) Check(CourierState) error { return nil }

func (c *refusingCourier) ImportOutbox(CourierState) error {
	c.calls++
	return errors.New("courier: outbox not empty")
}

// TestRestoreLateFailureDetachesRuntimeAndReleasesTable imports a good
// bundle into a fresh table with a courier that refuses: the runtime's
// descriptors come back as salvage (open, the exported ones), the fresh
// table is empty and unfrozen with its process handles released (fd count
// back to baseline), the child is alive, the shared exporter table and
// budget are unchanged, no goroutine was started, and the exporter can thaw
// and drain the task afterwards.
func TestRestoreLateFailureDetachesRuntimeAndReleasesTable(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newBundleFixture(t, "late-instance")
	hLive, pidLive := f.create(t, "live", []string{"/bin/sh", "-c", "printf partial; exec sleep 30"}, nil)
	f.awaitRunning(t, hLive)
	courier := &courierStub{state: courierEvents()}
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Courier: courier, Lineage: Lineage{Generation: 3}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	b := frozen.Bundle()
	if len(b.Processes.Records) != 1 || b.Processes.Records[0].ID != pidLive {
		t.Fatalf("bundle rows: %+v", b.Processes.Records)
	}
	osPID := b.Processes.Records[0].OSPID
	expectedFiles := 0
	for _, ti := range b.Runtime.Tasks {
		expectedFiles += len(ti.Files)
	}
	usedBefore := f.rt.ReplayBudgetUsed()
	tableBefore := fmt.Sprintf("%+v", f.manager.Snapshot())
	baselineG := runtime.NumGoroutine()
	fdBefore := testharness.FDCount()

	src := newBundleDupSource(t, frozen.resourcesForTest())
	fresh := &workload.Manager{}
	cfg := f.cfg
	cfg.Manager = fresh
	refusing := &refusingCourier{}
	imported, salvage, err := Import(b, b.Processes.Records, src, Target{Runtime: cfg, Courier: refusing})
	if err == nil || imported != nil {
		t.Fatalf("import with a refusing courier succeeded: %+v", imported)
	}
	if refusing.calls != 1 {
		t.Fatalf("courier asked %d times, want 1 (the table must have been installed first)", refusing.calls)
	}
	if salvage == nil || salvage.State.Generation != 3 || salvage.State.Instance != "late-instance" {
		t.Fatalf("salvage envelope: %+v", salvage)
	}
	got := 0
	for _, res := range salvage.Resources {
		for name, file := range res.Files {
			if _, err := file.Stat(); err != nil {
				t.Fatalf("salvaged %s/%s closed: %v", res.ID, name, err)
			}
			got++
		}
	}
	if got != expectedFiles {
		t.Fatalf("salvage returned %d files, the cut exported %d", got, expectedFiles)
	}
	// The fresh table: installed, then released without reaping — no rows,
	// not frozen, and the pidfd RestoreProcesses opened is gone.
	if rows := fresh.Snapshot(); len(rows) != 0 || fresh.Frozen() {
		t.Fatalf("fresh table after the late failure: rows=%d frozen=%t", len(rows), fresh.Frozen())
	}
	if err := salvage.Close(); err != nil {
		t.Fatal(err)
	}
	src.Close()
	if now := testharness.FDCount(); now != fdBefore {
		t.Fatalf("late failure leaked or closed descriptors: fd=%d before=%d", now, fdBefore)
	}
	if err := syscall.Kill(osPID, 0); err != nil {
		t.Fatalf("child gone after the late failure: %v", err)
	}
	if now := fmt.Sprintf("%+v", f.manager.Snapshot()); now != tableBefore {
		t.Fatalf("shared exporter table changed:\n%s\n%s", tableBefore, now)
	}
	if used := f.rt.ReplayBudgetUsed(); used != usedBefore {
		t.Fatalf("exporter budget moved %d → %d", usedBefore, used)
	}
	if g := runtime.NumGoroutine(); g > baselineG+2 {
		t.Fatalf("goroutines %d > baseline %d after a refused import", g, baselineG)
	}
	// The child is still waitable by whoever restores next: here the
	// exporter simply thaws and finishes it.
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	_ = f.manager.Signal(pidLive, syscall.SIGKILL)
	out, reply := f.drain(t, hLive)
	if out != "partial" || reply.Signal != int(syscall.SIGKILL) {
		t.Fatalf("live task after the late failure: out=%q reply=%+v", out, reply)
	}
}

// origin: hot-update S2 internal review I1 / import-V1 — a refused import
// must hand back something the previous image can be re-dispatched with:
// the Salvage carries the bundle (decoded, or the dispatched bytes when
// they could not be decoded) and Prepare packs it again under Role with the
// salvaged descriptors; and no rendering of the Salvage carries the
// registry key.
func TestSalvageRedispatchesTheSameBundleWithoutRenderingTheKey(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newBundleFixture(t, "salvage-instance")
	h, _ := f.create(t, "live", []string{"/bin/sh", "-c", "exec sleep 30"}, nil)
	f.awaitRunning(t, h)
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Courier: &courierStub{state: courierEvents()}, Lineage: Lineage{Generation: 7}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	b := frozen.Bundle()
	secret := base64.StdEncoding.EncodeToString(b.Runtime.Registry.Secret)
	leaks := func(label string, s *Salvage) {
		t.Helper()
		for verb, text := range map[string]string{"%v": fmt.Sprintf("%v", s), "%+v": fmt.Sprintf("%+v", s), "%s": fmt.Sprintf("%s", s),
			"%#v": fmt.Sprintf("%#v", s), "value %+v": fmt.Sprintf("%+v", *s), "State %+v": fmt.Sprintf("%+v", s.State)} {
			if strings.Contains(text, secret) || strings.Contains(text, `"secret"`) {
				t.Fatalf("%s: %s rendering of the salvage carries the registry key: %q", label, verb, text)
			}
			// A summary is bounded; a struct dump of the raw bytes (%#v
			// without Format renders them as hex — the key, spelled
			// differently) is not. Mutation M34 (Format removed) survived
			// the substring check alone.
			if len(text) > 256 {
				t.Fatalf("%s: %s rendering of the salvage is %d bytes, not a summary: %.120q…", label, verb, len(text), text)
			}
		}
		if s.State.Data != nil {
			t.Fatalf("%s: salvage envelope still carries the raw bundle", label)
		}
	}
	// Stage-2 refusal on the in-process path: a declared file is absent.
	src := newBundleDupSource(t, frozen.resourcesForTest())
	id := "exec-task." + b.Runtime.Tasks[0].PID
	_ = src.files[id]["stderr-replay"].Close()
	delete(src.files[id], "stderr-replay")
	fresh := &workload.Manager{}
	cfg := f.cfg
	cfg.Manager = fresh
	imported, salvage, err := Import(b, b.Processes.Records, src, Target{Runtime: cfg, Courier: &courierStub{}})
	if err == nil || imported != nil {
		t.Fatal("import with a missing file succeeded")
	}
	leaks("stage-2", salvage)
	if salvage.Bundle() == nil || salvage.Bundle().Instance != "salvage-instance" {
		t.Fatalf("stage-2 salvage lost the bundle: %v", salvage)
	}
	prepared, err := salvage.Prepare(handoff.Resource{ID: "control", Kind: "pipe", Data: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("re-dispatch from the stage-2 salvage: %v", err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if err := salvage.Close(); err != nil {
		t.Fatal(err)
	}
	// Decode failure through the real Restore entry point: the bytes could
	// not be read, so the very bytes are carried for the previous image.
	garbage := &handoff.Restored{State: handoff.State{Role: Role, Instance: "salvage-instance", Generation: 7, Data: []byte(`{"version":1,"newer_field":true,"registry":{"secret":"` + secret + `"}}`)}}
	imported2, salvage2, err := Restore(garbage, Target{Runtime: cfg})
	if err == nil || imported2 != nil {
		t.Fatal("garbage decoded")
	}
	leaks("decode-failure", salvage2)
	if garbage.State.Data != nil {
		t.Fatal("Restore left the raw bundle in the source envelope")
	}
	if salvage2.Bundle() != nil {
		t.Fatal("a decode failure cannot produce a decoded bundle")
	}
	prepared2, err := salvage2.Prepare()
	if err != nil {
		t.Fatalf("re-dispatch from the decode-failure salvage: %v", err)
	}
	if err := prepared2.Close(); err != nil {
		t.Fatal(err)
	}
	// Nothing dispatched at all: Prepare says so.
	if _, err := (&Salvage{State: handoff.State{Role: Role, Instance: "x", Generation: 1}}).Prepare(); err == nil {
		t.Fatal("an empty salvage re-dispatched nothing")
	}
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	_ = f.manager.Signal(b.Runtime.Tasks[0].PID, syscall.SIGKILL)
	f.drain(t, h)
}

// origin: hot-update S2 internal review I5 — every refusal the courier and
// spawn importers can make from the data is asked BEFORE any domain is
// installed, so a refused import never leaves verdicts or an outbox behind.
func TestImportPreChecksDomainsBeforeInstalling(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newBundleFixture(t, "precheck-instance")
	h, _ := f.create(t, "live", []string{"/bin/sh", "-c", "exec sleep 30"}, nil)
	f.awaitRunning(t, h)
	probe := &bundleProbe{}
	policy := bundleSpawnPolicy(t, "/precheck-nfs", probe, time.Now)
	if _, err := policy.Prepare([]string{"/bin/true"}, "/precheck-nfs", "/usr/bin", nil, false); err != nil {
		t.Fatal(err)
	}
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Spawn: policy, Courier: &courierStub{state: courierEvents()}, Lineage: Lineage{Generation: 2}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	b := frozen.Bundle()
	for _, tc := range []struct {
		name   string
		target func(*Target)
	}{
		{name: "courier-check-refuses", target: func(tgt *Target) { tgt.Courier = &courierStub{checkErr: errors.New("payload disagrees")} }},
		{name: "spawn-policy-not-fresh", target: func(tgt *Target) {
			busy := bundleSpawnPolicy(t, "/precheck-nfs", &bundleProbe{}, time.Now)
			_, _ = busy.Prepare([]string{"/bin/true"}, "/precheck-nfs", "/usr/bin", nil, false) // it decided something
			tgt.Spawn = busy
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			src := newBundleDupSource(t, frozen.resourcesForTest())
			fresh := &workload.Manager{}
			cfg := f.cfg
			cfg.Manager = fresh
			policy2 := bundleSpawnPolicy(t, "/precheck-nfs", &bundleProbe{}, time.Now)
			courier2 := &courierStub{}
			tgt := Target{Runtime: cfg, Spawn: policy2, Courier: courier2}
			tc.target(&tgt)
			imported, salvage, err := Import(b, b.Processes.Records, src, tgt)
			if err == nil || imported != nil {
				t.Fatalf("%s: imported", tc.name)
			}
			if len(salvage.Resources) != 0 {
				t.Fatalf("%s: descriptors were taken before the pre-checks: %d", tc.name, len(salvage.Resources))
			}
			if rows := fresh.Snapshot(); len(rows) != 0 {
				t.Fatalf("%s: process table installed", tc.name)
			}
			if courier2.imported != nil {
				t.Fatalf("%s: courier installed", tc.name)
			}
			if err := policy2.ImportReady(); tgt.Spawn == policy2 && err != nil {
				t.Fatalf("%s: verdicts installed into the fresh policy", tc.name)
			}
		})
	}
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	_ = f.manager.Signal(b.Runtime.Tasks[0].PID, syscall.SIGKILL)
	f.drain(t, h)
}

// origin: hot-update S2 internal review, import lane missed-coverage 1 —
// the two shared-table refusals of Restore: an in-process switch over a
// table that is not frozen, and over a frozen table whose rows are not the
// bundle's. Both refuse before any descriptor is taken and hand back a
// salvage that re-dispatches (the decoded bundle survives the row check;
// the not-frozen refusal happens before it, so only the envelope is there
// and Prepare says so).
func TestRestoreRefusesSharedTableNotFrozenOrDiffering(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newBundleFixture(t, "shared-instance")
	h, _ := f.create(t, "live", []string{"/bin/sh", "-c", "exec sleep 30"}, nil)
	f.awaitRunning(t, h)
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Courier: &courierStub{state: courierEvents()}, Lineage: Lineage{Generation: 4}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	b := frozen.Bundle()
	data, err := encode(b)
	if err != nil {
		t.Fatal(err)
	}
	restored := func(data []byte) *handoff.Restored {
		return &handoff.Restored{State: handoff.State{Role: Role, Instance: b.Instance, Generation: 4, Data: append([]byte(nil), data...)}}
	}
	shared := f.cfg // Manager is the exporter's (frozen) table
	fdBefore := testharness.FDCount()
	// Rows differ: the bundle claims another OS pid for the live row.
	forged := b
	forged.Processes.Records = append([]workload.Record(nil), b.Processes.Records...)
	forged.Processes.Records[0].OSPID++
	forgedData, err := encode(forged)
	if err != nil {
		t.Fatal(err)
	}
	imported, salvage, err := Restore(restored(forgedData), Target{Runtime: shared})
	if err == nil || imported != nil || !errors.Is(err, ErrBundle) || !strings.Contains(err.Error(), "differs from the bundle") {
		t.Fatalf("differing rows accepted: %v", err)
	}
	if len(salvage.Resources) != 0 || salvage.Bundle() == nil {
		t.Fatalf("row refusal took descriptors or lost the bundle: %v", salvage)
	}
	if prepared, err := salvage.Prepare(); err != nil {
		t.Fatalf("re-dispatch after the row refusal: %v", err)
	} else if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if got := testharness.FDCount(); got != fdBefore {
		t.Fatalf("descriptors after the row refusal: %d vs %d", got, fdBefore)
	}
	// Not frozen: thaw the cut, then try the intact bundle over the live table.
	calibrationFile := frozen.resources[len(frozen.resources)-1].Files["state"]
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	// Thaw releases exactly the calibration snapshot; the refusal below
	// must still leave every business descriptor untouched.
	// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility.
	if _, err := calibrationFile.Stat(); err == nil || testharness.FDCount() != fdBefore-1 {
		t.Fatal("thaw did not release exactly calibration custody")
	}
	fdBefore = testharness.FDCount()
	imported, salvage, err = Restore(restored(data), Target{Runtime: shared})
	if err == nil || imported != nil || !errors.Is(err, ErrBundle) || !strings.Contains(err.Error(), "not frozen") {
		t.Fatalf("unfrozen shared table accepted: %v", err)
	}
	if len(salvage.Resources) != 0 || salvage.Bundle() != nil {
		t.Fatalf("not-frozen refusal took descriptors or carried a bundle it never checked: %v", salvage)
	}
	if _, err := salvage.Prepare(); err == nil {
		t.Fatal("a salvage without a bundle re-dispatched something")
	}
	if got := testharness.FDCount(); got != fdBefore {
		t.Fatalf("descriptors after the not-frozen refusal: %d vs %d", got, fdBefore)
	}
	_ = f.manager.Signal(b.Runtime.Tasks[0].PID, syscall.SIGKILL)
	f.drain(t, h)
}

// origin: hot-update S3 plan §1.4 P4 — a refused import took some of a
// task's files (the Salvage) and left the rest inside the handoff bundle;
// the re-dispatch must merge both halves under the ONE task resource, which
// handoff.Prepare would otherwise refuse as a duplicate ID.
func TestSalvagePrepareMergesRestoredRemainder(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newBundleFixture(t, "merge-instance")
	h, _ := f.create(t, "live", []string{"/bin/sh", "-c", "exec sleep 30"}, nil)
	f.awaitRunning(t, h)
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Courier: &courierStub{state: courierEvents()}, Lineage: Lineage{Generation: 2}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	b := frozen.Bundle()
	id := "exec-task." + b.Runtime.Tasks[0].PID
	// Stage 2 refusal after some files were taken: swap the two output
	// replay memfds (same shape, different kernel identity), so the import
	// takes the files before them and stops at the first mismatched one.
	src := newBundleDupSource(t, frozen.resourcesForTest())
	files := src.files[id]
	files["stdout-replay"], files["stderr-replay"] = files["stderr-replay"], files["stdout-replay"]
	fresh := &workload.Manager{}
	cfg := f.cfg
	cfg.Manager = fresh
	imported, salvage, err := Import(b, b.Processes.Records, src, Target{Runtime: cfg, Courier: &courierStub{}})
	if err == nil || imported != nil {
		t.Fatal("import with a missing file succeeded")
	}
	if len(salvage.Resources) != 1 || salvage.Resources[0].ID != id || len(salvage.Resources[0].Files) == 0 {
		t.Fatalf("salvage does not hold the taken half: %+v", salvage.Resources)
	}
	// The untaken half, as handoff.Restored.Borrow would report it.
	untaken := handoff.Resource{ID: id, Kind: salvage.Resources[0].Kind, Data: salvage.Resources[0].Data, Files: map[string]*os.File{}}
	for name, file := range src.files[id] {
		if _, taken := salvage.Resources[0].Files[name]; !taken {
			untaken.Files[name] = file
		}
	}
	if len(untaken.Files) == 0 {
		t.Fatal("fixture: the import took every file, nothing left to merge")
	}
	prepared, err := salvage.Prepare(untaken)
	if err != nil {
		t.Fatalf("Prepare with the borrowed remainder: %v", err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	// Two owners of the same file name is refused, not silently dropped.
	dup := handoff.Resource{ID: id, Kind: untaken.Kind, Data: untaken.Data, Files: map[string]*os.File{}}
	for name, file := range salvage.Resources[0].Files {
		dup.Files[name] = file
	}
	if _, err := salvage.Prepare(dup); err == nil {
		t.Fatal("a second owner of a salvaged file was accepted")
	}
	if err := salvage.Close(); err != nil {
		t.Fatal(err)
	}
	src.Close()
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	_ = f.manager.Signal(b.Runtime.Tasks[0].PID, syscall.SIGKILL)
	f.drain(t, h)
}
