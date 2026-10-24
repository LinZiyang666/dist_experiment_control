//go:build linux

package brokerimage

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"golang.org/x/sys/unix"
)

const (
	brokerCutPhase    = "BROKER_UPGRADE_CUT_PHASE"
	brokerCutScenario = "BROKER_UPGRADE_CUT_SCENARIO"
	brokerCutDir      = "BROKER_UPGRADE_CUT_DIR"
	brokerCutTest     = "TestBrokerBootAdmitsUpgradeCutOutsidePrivateClosure"
)

// A real same-PID exec carries the S8 adjunct and its domain lease beside the
// broker image. Boot verifies it before the private closure and keeps it out
// of that closure (nine private kinds, delta=∅); Import adopts the lease while
// pristine; the authority comparison after OpenCurrent precedes StartAuth via
// Config.NewControl; and a refused adjunct consumes nothing.
// origin: hot-update S8 plan §5.1 / §6.1 / §6.2 / §11.2 P17
func TestBrokerBootAdmitsUpgradeCutOutsidePrivateClosure(t *testing.T) {
	switch os.Getenv(brokerCutPhase) {
	case "before":
		brokerUpgradeCutBefore(t, os.Getenv(brokerCutScenario), os.Getenv(brokerCutDir))
		return
	case "after":
		brokerUpgradeCutAfter(t, os.Getenv(brokerCutScenario), os.Getenv(brokerCutDir))
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseBrokerUpgradeCut(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker upgrade cut x5", beforeG)
	testharness.AssertNoFDLeak(t, "broker upgrade cut x5", beforeFD, 0)
}

func exerciseBrokerUpgradeCut(t *testing.T) {
	for _, scenario := range []string{"activate", "recreated", "repack", "foreign-owner", "wrong-target", "extra-file"} {
		dir, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=^"+brokerCutTest+"$", "-test.count=1")
		cmd.Env = replaceTestEnv(replaceTestEnv(replaceTestEnv(os.Environ(), brokerCutPhase, "before"), brokerCutScenario, scenario), brokerCutDir, dir)
		output, err := testharness.CombinedOutput(cmd)
		if err != nil || !strings.Contains(string(output), "broker upgrade cut receipt "+scenario+" ok") {
			t.Fatalf("%s: same-PID adjunct exec failed: %v\n%s", scenario, err, output)
		}
	}
}

func brokerUpgradeCutBefore(t *testing.T, scenario, dir string) {
	f := newImportFixture(t)
	refreshBootFixture(t, f)
	key, lease := brokerUpgradeDomain(t, dir, true)
	state := brokerUpgradeCutState(t, key, f.bundle.ExporterSHA256)
	switch scenario {
	case "foreign-owner":
		state.Owner.StartTicks++ // this PID after reuse is another process
	case "wrong-target":
		state.TargetSHA = strings.Repeat("e", 64)
	}
	duplicate, err := unix.FcntlInt(lease.FD(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		t.Fatal(err)
	}
	borrowed := os.NewFile(uintptr(duplicate), "exported-lease")
	adjunct, err := upgradecut.Pack(state, map[uint32]*os.File{state.LockFD: borrowed})
	if err != nil {
		t.Fatal(err)
	}
	if scenario == "extra-file" {
		adjunct.Files["aux-9"] = f.peer // a descriptor the importer has no owner for
	}
	data, err := Encode(f.bundle)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := handoff.Prepare(handoff.State{Role: ImageRole, Instance: f.bundle.InstanceID, Generation: f.bundle.Generation, Data: data}, append(f.resources, adjunct))
	if err != nil {
		t.Fatal(err)
	}
	env := replaceTestEnv(os.Environ(), brokerCutPhase, "after")
	err = prepared.Exec("/proc/self/exe", []string{os.Args[0], "-test.run=^" + brokerCutTest + "$", "-test.count=1"}, env)
	t.Fatalf("same-PID exec returned: %v", err)
}

func brokerUpgradeCutAfter(t *testing.T, scenario, dir string) {
	restored, err := handoff.Restore()
	if err != nil || restored == nil {
		t.Fatalf("restore: %v", err)
	}
	defer func() { _ = restored.Close() }()
	// The new image registers the same deployment again before using a Key.
	key, _ := brokerUpgradeDomain(t, dir, false)
	image, err := Boot(restored)
	switch scenario {
	case "foreign-owner", "wrong-target", "extra-file":
		if err == nil || image != nil {
			t.Fatalf("broker admitted a %s adjunct", scenario)
		}
		// Refusal is pristine: nothing was taken and the reservation survives.
		if !brokerBorrowed(restored, upgradecut.ResourceID) || !brokerDomainHeld(t, dir, key) {
			t.Fatal("refused adjunct consumed the lease")
		}
		fmt.Printf("broker upgrade cut receipt %s ok\n", scenario)
		return
	}
	if err != nil {
		t.Fatalf("boot refused a valid adjunct: %v", err)
	}
	state, lease := image.UpgradeCut()
	if state == nil || state.Attempt.Key != key || lease != nil {
		t.Fatal("boot did not retain the verified adjunct as pristine custody")
	}
	events := []string{}
	var server *sleepingServer
	actual := state.Authorities
	if scenario == "recreated" {
		actual = []upgradecut.Authority{{SID: state.Authorities[0].SID, Incarnation: strings.Repeat("f", 64)}}
	}
	cfg := Config{Mode: ModeSingle, Deadline: controlbudget.Deadline(1 << 62),
		NewStore: func(storage.StoreStamp, *os.File, controlbudget.Deadline) (StoreOwner, error) {
			events = append(events, "store")
			return &testStore{}, nil
		},
		// The broker's authority check rides the existing seam between
		// OpenCurrent and StartAuth; a mismatch revokes before any business.
		NewControl: func(StoreOwner, controlbudget.Deadline) (ControlOwner, error) {
			if err := upgradecut.MatchAuthorities(state.Authorities, actual); err != nil {
				return nil, err
			}
			return &testControl{events: &events}, nil
		},
		ImportServer: func(_ tunnel.ServerImportConfig, _ tunnel.ServerImage, source tunnel.FileSource) (tunnel.ServerImported, tunnel.Salvage, error) {
			file, err := source.TakeFile("raw.fixture")
			server = &sleepingServer{file: file, events: &events}
			return server, tunnel.Salvage{}, err
		}}
	imported, err := image.Import(cfg)
	if err != nil {
		t.Fatalf("pristine import with adjunct: %v", err)
	}
	if brokerBorrowed(restored, upgradecut.ResourceID) || !brokerDomainHeld(t, dir, key) {
		t.Fatal("import did not adopt the lease before activation")
	}
	if _, lease := image.UpgradeCut(); lease != nil {
		t.Fatal("a pristine import released its lease")
	}
	switch scenario {
	case "repack":
		before := brokerLeaseHolders(t, dir, key)
		prepared, err := image.Repack()
		if err != nil {
			t.Fatalf("pristine repack after adoption: %v", err)
		}
		// The redispatch carries the adopted OFD onward with its adjunct.
		if held := brokerLeaseHolders(t, dir, key); before != 1 || held != 2 {
			_ = prepared.Close()
			t.Fatalf("repack carried %d lease holders (custody %d), want custody plus one", held, before)
		}
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
		if !brokerDomainHeld(t, dir, key) {
			t.Fatal("repack released the reservation")
		}
		if err := image.Close(); err != nil {
			t.Fatal(err)
		}
		if brokerDomainHeld(t, dir, key) {
			t.Fatal("terminal close kept the reservation")
		}
	case "activate", "recreated":
		slot := &testSlot{events: &events}
		err = imported.Activate(slot, cfg.Deadline)
		if scenario == "recreated" {
			var mismatch *upgradetx.Error
			if !errors.Is(err, ErrActive) || !errors.As(err, &mismatch) || mismatch.Code != upgradetx.CodeSessionIncarnationMismatch || strings.Contains(strings.Join(events, ","), "auth") {
				t.Fatalf("recreated authority reached business: %v %v", err, events)
			}
		} else if err != nil || strings.Join(events, ",") != "store,install,auth,server-active,business" {
			t.Fatalf("activation: %v %v", err, events)
		}
		// Either way the import holds ACTIVE responsibility; the commit
		// owner (or the revocation owner) now receives the lease once.
		_, lease = image.UpgradeCut()
		if lease == nil || lease.DomainID() != key.DomainID {
			t.Fatal("active import did not transfer the adopted lease")
		}
		if _, again := image.UpgradeCut(); again != nil {
			t.Fatal("lease transferred twice")
		}
		if err := lease.Close(); err != nil {
			t.Fatal(err)
		}
		if brokerDomainHeld(t, dir, key) {
			t.Fatal("final owner close kept the reservation")
		}
		if server != nil {
			_ = server.file.Close()
		}
		if err := image.Finish(nil); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Printf("broker upgrade cut receipt %s ok\n", scenario)
}

// The exporter's prepare graph admits exactly the leaf export, the broker's own
// state and at most the closed adjunct. Any other unexported resource would be
// a descriptor with no owner on the receiving side.
// origin: hot-update S8 plan §6.1 / §11.2 P17
func TestBrokerPrepareAdmitsOnlyTheClosedUpgradeCut(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseBrokerPrepareExtras(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker prepare extras x5", beforeG)
	testharness.AssertNoFDLeak(t, "broker prepare extras x5", beforeFD, 0)
}

func exerciseBrokerPrepareExtras(t *testing.T) {
	files := make([]*os.File, 4)
	for i := range files {
		file, err := os.CreateTemp(t.TempDir(), "prepare-")
		if err != nil {
			t.Fatal(err)
		}
		files[i] = file
		defer func() { _ = file.Close() }()
	}
	bundle := Bundle{StateResource: "broker-state.fixture"}
	exported := []handoff.Resource{{ID: "lock", Kind: "store-lock-v1", Files: map[string]*os.File{"lock": files[0]}}, {ID: "raw", Kind: "raw-socket-v1", Files: map[string]*os.File{"raw": files[1]}}}
	state := handoff.Resource{ID: bundle.StateResource, Kind: "sealed-state-v1", Files: map[string]*os.File{"state": files[2]}}
	cut := brokerUpgradeCutState(t, upgradetx.Key{DomainID: strings.Repeat("7", 32), Component: upgradetx.ComponentBroker, ConfigPath: "/fixture/broker.yaml"}, strings.Repeat("a", 64))
	adjunct, err := upgradecut.Pack(cut, map[uint32]*os.File{cut.LockFD: files[3]})
	if err != nil {
		t.Fatal(err)
	}
	malformed := adjunct
	malformed.Data = []byte(`{"kind":"upgrade-cut-v1","required":false}`)
	for _, tc := range []struct {
		name     string
		supplied []handoff.Resource
		extra    int
	}{
		{"legacy state only", []handoff.Resource{exported[0], exported[1], state}, 1},
		{"state and adjunct", []handoff.Resource{exported[1], adjunct, exported[0], state}, 2},
		{"adjunct without state", []handoff.Resource{exported[0], exported[1], adjunct}, 0},
		{"malformed adjunct", []handoff.Resource{exported[0], exported[1], state, malformed}, 0},
		{"export omitted", []handoff.Resource{exported[0], state, adjunct}, 0},
		{"unowned resource", []handoff.Resource{exported[0], exported[1], state, {ID: "other", Kind: "sealed-state-v1", Files: map[string]*os.File{"state": files[3]}}}, 0},
		{"state also exported", []handoff.Resource{exported[0], exported[1], state}, -1},
	} {
		index := exported
		if tc.extra < 0 {
			index = append(append([]handoff.Resource(nil), exported...), state)
		}
		extra, err := prepareExtras(bundle, tc.supplied, index)
		if tc.extra <= 0 {
			if err == nil {
				t.Fatalf("%s: prepare graph accepted %d extras", tc.name, len(extra))
			}
			continue
		}
		if err != nil || len(extra) != tc.extra {
			t.Fatalf("%s: extras=%d err=%v", tc.name, len(extra), err)
		}
	}
}

func brokerUpgradeDomain(t *testing.T, dir string, acquire bool) (upgradetx.Key, upgradelock.Lease) {
	t.Helper()
	binary := filepath.Join(dir, "tether")
	if acquire {
		if err := os.WriteFile(binary, []byte("broker fixture image"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	key := upgradetx.Key{Component: upgradetx.ComponentBroker, ConfiguredNID: "broker-a", ConfigPath: filepath.Join(dir, "broker.yaml"), UID: uint32(os.Getuid())}
	id, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: binary, ControlDir: dir, DeploymentID: "broker-upgrade-cut",
		Participants: []upgradelock.Participant{{Key: key, ServiceID: "broker-fixture"}}})
	if err != nil {
		t.Fatal(err)
	}
	key.DomainID = id
	if !acquire {
		return key, nil
	}
	lease, err := upgradelock.Acquire(t.Context(), key, "")
	if err != nil {
		t.Fatal(err)
	}
	return key, lease
}

func brokerUpgradeCutState(t *testing.T, key upgradetx.Key, running string) upgradecut.State {
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
	ticks, err := strconv.ParseUint(strings.Fields(line[strings.LastIndexByte(line, ')')+1:])[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	owner := upgradetx.Owner{BootID: strings.TrimSpace(string(boot)), PID: os.Getpid(), StartTicks: ticks, InstanceID: strings.Repeat("8", 32)}
	return upgradecut.State{
		Attempt:   upgradetx.Handle{Key: key, TxID: strings.Repeat("3", 32), CutID: strings.Repeat("4", 32), Generation: 2, Revision: 1},
		SourceSHA: running, TargetSHA: running, Owner: owner,
		Authorities: []upgradecut.Authority{{SID: "session-a", Incarnation: strings.Repeat("c", 64)}},
		Scope: upgradetx.Scope{ID: strings.Repeat("6", 32), Participants: []upgradetx.Key{key},
			Domains:  []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainUpgrade},
			Expected: []upgradetx.Resource{}},
	}
}

func brokerBorrowed(restored *handoff.Restored, id string) bool {
	for _, resource := range restored.Borrow() {
		if resource.ID == id {
			return true
		}
	}
	return false
}

// brokerLeaseHolders counts this process's descriptors naming the lease inode.
func brokerLeaseHolders(t *testing.T, dir string, key upgradetx.Key) int {
	t.Helper()
	named, err := os.Stat(filepath.Join(dir, key.DomainID+".lease"))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if info, err := os.Stat("/proc/self/fd/" + entry.Name()); err == nil && os.SameFile(info, named) {
			count++
		}
	}
	return count
}

// An independent OFD detects both an early unlock and a leaked reservation.
func brokerDomainHeld(t *testing.T, dir string, key upgradetx.Key) bool {
	t.Helper()
	contender, err := os.OpenFile(filepath.Join(dir, key.DomainID+".lease"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = contender.Close() }()
	err = unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err != nil && !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatal(err)
	}
	return err != nil
}
