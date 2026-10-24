package upgradecut

import (
	"errors"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"golang.org/x/sys/unix"
)

// The exporter and importer share one resource shape. A reader can skip
// neither a second adjunct nor a mislabelled one, and the file set must be
// exactly the referenced lock: an extra descriptor has no importer.
// origin: hot-update S8 plan §6.1 / §11.2 P17 / TestPrivateAdjunctCodecClosure
func TestAdjunctHandoffResourceIsClosed(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAdjunctHandoffResource(t)
	}
	testharness.AssertNoGoroutineLeak(t, "adjunct handoff resource", beforeG)
	testharness.AssertNoFDLeak(t, "adjunct handoff resource", beforeFD, 0)
}

func exerciseAdjunctHandoffResource(t *testing.T) {
	t.Helper()
	lock, err := os.CreateTemp(t.TempDir(), "lease-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Close() }()
	other, err := os.CreateTemp(t.TempDir(), "other-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	state := brokerFixtureState()
	state.LockFD = 3
	resource, err := Pack(state, map[uint32]*os.File{3: lock})
	if err != nil || resource.ID != ResourceID || resource.Kind != Version || len(resource.Files) != 1 || resource.Files["aux-3"] != lock {
		t.Fatalf("pack: %+v %v", resource, err)
	}
	got, ok, err := Find([]handoff.Resource{{ID: "sealed-state", Kind: "sealed-state-v1"}, resource})
	if err != nil || !ok || !reflect.DeepEqual(got, state) {
		t.Fatalf("find did not return the packed adjunct: %+v %t %v", got, ok, err)
	}
	if _, ok, err := Find([]handoff.Resource{{ID: "sealed-state", Kind: "sealed-state-v1"}}); ok || err != nil {
		t.Fatal("legacy dispatch without an adjunct was not absent")
	}
	for _, refused := range []map[uint32]*os.File{nil, {}, {2: lock}, {3: lock, 4: other}, {3: nil}} {
		if _, err := Pack(state, refused); err == nil {
			t.Fatalf("pack accepted auxiliary set %v", refused)
		}
	}
	invalid := state
	invalid.Authorities = nil
	if _, err := Pack(invalid, map[uint32]*os.File{3: lock}); err == nil {
		t.Fatal("pack bypassed the closed codec")
	}
	mutate := func(edit func(*handoff.Resource)) []handoff.Resource {
		copy := handoff.Resource{ID: resource.ID, Kind: resource.Kind, Data: append([]byte(nil), resource.Data...), Files: map[string]*os.File{}}
		for name, file := range resource.Files {
			copy.Files[name] = file
		}
		edit(&copy)
		return []handoff.Resource{copy}
	}
	for name, resources := range map[string][]handoff.Resource{
		"duplicate adjunct":    {resource, resource},
		"kind under other ID":  mutate(func(r *handoff.Resource) { r.ID = "upgrade-cut-v2" }),
		"ID with other kind":   mutate(func(r *handoff.Resource) { r.Kind = "sealed-state-v1" }),
		"missing lock file":    mutate(func(r *handoff.Resource) { r.Files = map[string]*os.File{} }),
		"wrong lock index":     mutate(func(r *handoff.Resource) { r.Files = map[string]*os.File{"aux-0": lock} }),
		"extra descriptor":     mutate(func(r *handoff.Resource) { r.Files["aux-4"] = other }),
		"noncanonical adjunct": mutate(func(r *handoff.Resource) { r.Data = append([]byte(" "), r.Data...) }),
		"optional adjunct": mutate(func(r *handoff.Resource) {
			r.Data = []byte(strings.Replace(string(r.Data), `"required":true`, `"required":false`, 1))
		}),
	} {
		if _, ok, err := Find(resources); !ok || err == nil {
			t.Fatalf("%s: importer accepted or skipped the adjunct (present=%t err=%v)", name, ok, err)
		}
	}
}

// The broker's adjunct names every session's incarnation. Comparison with the
// rows read after OpenCurrent is exact: no partial success, no recreation.
// origin: hot-update S8 plan §6.1 / §6.2 / §11.2 P17
func TestAdjunctAuthorityMapMatchesCurrentRowsExactly(t *testing.T) {
	expected := brokerFixtureState().Authorities
	if err := MatchAuthorities(expected, []Authority{expected[1], expected[0]}); err != nil {
		t.Fatalf("same authority map in another order refused: %v", err)
	}
	if err := MatchAuthorities([]Authority{}, nil); err != nil {
		t.Fatal("explicit idle broker refused")
	}
	recreated := []Authority{expected[0], {SID: expected[1].SID, Incarnation: strings.Repeat("e", 64)}}
	for name, actual := range map[string][]Authority{
		"recreated session": recreated,
		"missing session":   expected[:1],
		"extra session":     append(append([]Authority(nil), expected...), Authority{SID: "session-z", Incarnation: strings.Repeat("f", 64)}),
		"duplicate row":     {expected[0], expected[0]},
		"malformed row":     {expected[0], {SID: expected[1].SID, Incarnation: "not-a-digest"}},
	} {
		var refusal *upgradetx.Error
		if err := MatchAuthorities(expected, actual); !errors.As(err, &refusal) || refusal.Code != upgradetx.CodeSessionIncarnationMismatch {
			t.Fatalf("%s: authority mismatch not refused with the frozen code: %v", name, err)
		}
	}
}

// A cut adopted after a same-PID exec was produced by this very process. Any
// other PID, start tick or boot (including PID reuse) is another process.
// origin: hot-update S8 plan §2.1 / §5.1 / §11.2 P17
func TestAdjunctOwnerMustBeThisProcess(t *testing.T) {
	if runtime.GOOS != "linux" {
		if err := SameProcess(upgradetx.Owner{PID: os.Getpid()}); err == nil {
			t.Fatal("a host without a process witness accepted adoption")
		}
		return
	}
	self := thisProcessOwner(t)
	if err := SameProcess(self); err != nil {
		t.Fatalf("own identity refused: %v", err)
	}
	dashless := self
	dashless.BootID = strings.ReplaceAll(self.BootID, "-", "")
	if err := SameProcess(dashless); err != nil {
		t.Fatalf("canonical hex boot ID refused: %v", err)
	}
	for name, edit := range map[string]func(*upgradetx.Owner){
		"another PID":        func(o *upgradetx.Owner) { o.PID++ },
		"reused PID":         func(o *upgradetx.Owner) { o.StartTicks++ },
		"previous boot":      func(o *upgradetx.Owner) { o.BootID = strings.Repeat("0", 32) },
		"unknown start tick": func(o *upgradetx.Owner) { o.StartTicks = 0 },
	} {
		owner := self
		edit(&owner)
		if err := SameProcess(owner); err == nil {
			t.Fatalf("%s: adopted a cut from another process", name)
		}
	}
}

func thisProcessOwner(t *testing.T) upgradetx.Owner {
	t.Helper()
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		t.Fatal(err)
	}
	line := string(stat)
	fields := strings.Fields(line[strings.LastIndexByte(line, ')')+1:])
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return upgradetx.Owner{BootID: strings.TrimSpace(string(boot)), PID: os.Getpid(), StartTicks: ticks, InstanceID: strings.Repeat("9", 32)}
}

// Custody adopts once, releases an adopted lease only to an ACTIVE importer
// (exactly once), re-packs the same OFD for a pristine redispatch without
// unlocking it, and keeps refused custody final until terminal Close.
// origin: hot-update S8 plan §5.1 / §8.1 / M08 / §11.2 P17
func TestAdjunctCustodyReleasesOnlyAnActiveAdoptedLease(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the inherited domain OFD and /proc witness are Linux contracts")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAdjunctCustody(t)
	}
	testharness.AssertNoGoroutineLeak(t, "adjunct custody", beforeG)
	testharness.AssertNoFDLeak(t, "adjunct custody", beforeFD, 0)
}

func exerciseAdjunctCustody(t *testing.T) {
	t.Helper()
	var none *Custody
	if none.Take(nil) != nil || none.Release(true) != nil || none.State() != nil || none.Close() != nil {
		t.Fatal("legacy dispatch custody is not inert")
	}
	if resources, err := none.Resources(); resources != nil || err != nil {
		t.Fatal("legacy dispatch re-packed an adjunct")
	}
	state, original := acquireFixtureLease(t)
	duplicate, err := unix.FcntlInt(original.FD(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		_ = original.Close()
		t.Fatal(err)
	}
	if err := original.Close(); err != nil { // the exporting wrapper ends with exec
		t.Fatal(err)
	}
	inherited := os.NewFile(uintptr(duplicate), "inherited-lease")
	custody := &Custody{state: state}
	takes := 0
	take := func(string, string) (*os.File, error) { takes++; return inherited, nil }
	if err := custody.Take(take); err != nil || custody.Take(take) != nil || takes != 1 {
		t.Fatalf("adoption was not exactly once: takes=%d err=%v", takes, err)
	}
	if custody.Release(false) != nil {
		t.Fatal("a pristine importer released the adopted lease")
	}
	resources, err := custody.Resources()
	if err != nil || len(resources) != 1 || resources[0].ID != ResourceID {
		t.Fatalf("pristine re-pack: %+v %v", resources, err)
	}
	carried := resources[0].Files[AuxiliaryName(state.LockFD)]
	if got, ok, err := Find(resources); err != nil || !ok || !reflect.DeepEqual(got, state) || !sameOFD(t, carried, custody.lease.FD()) {
		CloseResources(resources)
		t.Fatal("re-pack did not carry the adopted OFD with its adjunct")
	}
	CloseResources(resources)
	lease := custody.Release(true)
	if lease == nil || custody.Release(true) != nil || custody.Close() != nil {
		t.Fatal("active importer did not receive the lease exactly once")
	}
	// A partial activation may retry after the commit owner took the lease:
	// the descriptor already left the source, so the retry must not take again.
	if err := custody.Take(take); err != nil || takes != 1 {
		t.Fatalf("activation retry re-took a released lease: takes=%d err=%v", takes, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	// Refused custody stays final and is never released as a reservation.
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = write.Close() }()
	refused := &Custody{state: fixtureState()}
	if err := refused.Take(func(string, string) (*os.File, error) { return read, nil }); err == nil {
		t.Fatal("a pipe was adopted as a lease")
	}
	if refused.Take(func(string, string) (*os.File, error) { t.Error("refused take retried"); return nil, nil }) == nil || refused.Release(true) != nil {
		t.Fatal("refused custody retried or was released")
	}
	if err := refused.Close(); err != nil {
		t.Fatal(err)
	}
}

// sameOFD proves two descriptors share one open file description (offset).
func sameOFD(t *testing.T, file *os.File, fd uintptr) bool {
	t.Helper()
	if _, err := unix.Seek(int(fd), 3, 0); err != nil {
		t.Fatal(err)
	}
	offset, err := unix.Seek(int(file.Fd()), 0, 1)
	_, _ = unix.Seek(int(fd), 0, 0)
	return err == nil && offset == 3
}

// A take callback that loses the file confers nothing; one that hands back a
// non-lease descriptor keeps it as refused custody for salvage.
// origin: hot-update S8 plan §5.1 / M08 / §11.2 P17
func TestAdjunctTakeLeaseFromHandoffSource(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAdjunctTakeLease(t)
	}
	testharness.AssertNoGoroutineLeak(t, "adjunct take from handoff", beforeG)
	testharness.AssertNoFDLeak(t, "adjunct take from handoff", beforeFD, 0)
}

func exerciseAdjunctTakeLease(t *testing.T) {
	t.Helper()
	state := fixtureState()
	if lease, err := TakeLease(state, nil); lease != nil || err == nil {
		t.Fatal("nil source fabricated custody")
	}
	lease, err := TakeLease(state, func(id, name string) (*os.File, error) {
		if id != ResourceID || name != AuxiliaryName(state.LockFD) {
			t.Errorf("took %s/%s", id, name)
		}
		return nil, errors.New("private-source-detail")
	})
	if lease != nil || err == nil || strings.Contains(err.Error(), "private-source-detail") {
		t.Fatalf("missing lease produced custody or leaked diagnostics: %v", err)
	}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = write.Close() }()
	lease, err = TakeLease(state, func(string, string) (*os.File, error) { return read, nil })
	var unsettled *upgradetx.Error
	if lease == nil || !errors.As(err, &unsettled) || unsettled.Code != upgradetx.CodeUnsettled {
		if lease != nil {
			_ = lease.Close()
		}
		t.Fatalf("non-lease descriptor lost its custody: %v", err)
	}
	// The taken wrapper was retired; the retained duplicate still names the pipe.
	if _, err := write.Write([]byte("x")); err != nil {
		_ = lease.Close()
		t.Fatal("refused custody closed the taken descriptor")
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := write.Write([]byte("x")); err == nil {
		t.Fatal("terminal cleanup kept a reader alive")
	}
}
