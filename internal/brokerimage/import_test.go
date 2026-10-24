//go:build linux

package brokerimage

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"golang.org/x/sys/unix"
)

// The tunnel callback is a sleeping-owner contract double. FD operations,
// socket identity, original business bytes and independent flock are real.
// This tests P02 orchestration; P07/P08/P22 integration remains separately owned.
type importFixture struct {
	bundle    Bundle
	resources []handoff.Resource
	source    *handofftest.DupSource
	raw       *os.File
	peer      *os.File
	lockPath  string
	config    Config
	server    *sleepingServer
	store     *testStore
	control   *testControl
	events    []string
}

func newImportFixture(t *testing.T) *importFixture {
	t.Helper()
	f := &importFixture{bundle: fixtureBundle(t)}
	f.bundle.Args = []string{os.Args[0], "-test.run=TestBrokerBootRoleOnce"}
	f.lockPath = filepath.Join(t.TempDir(), "tether.lock")
	lock, err := os.OpenFile(f.lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.raw = os.NewFile(uintptr(pair[0]), "original-raw")
	f.peer = os.NewFile(uintptr(pair[1]), "independent-peer")
	listenerFD, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	listener := os.NewFile(uintptr(listenerFD), "original-listener")
	if err = unix.Bind(listenerFD, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	if err = unix.Listen(listenerFD, 8); err != nil {
		t.Fatal(err)
	}
	var st unix.Stat_t
	if err = unix.Fstat(listenerFD, &st); err != nil {
		t.Fatal(err)
	}
	cookie, err := unix.GetsockoptUint64(listenerFD, unix.SOL_SOCKET, unix.SO_COOKIE)
	if err != nil {
		t.Fatal(err)
	}
	f.bundle.Listeners = []listenerimage.Descriptor{{ID: "listener.fixture", Network: "tcp4", Address: "127.0.0.1", Device: st.Dev, Inode: st.Ino, Cookie: cookie, Family: unix.AF_INET, SockType: unix.SOCK_STREAM, Nonblock: true, CloseOnExec: true}}
	f.resources = []handoff.Resource{
		{ID: f.bundle.LockResource, Kind: "store-lock-v1", Files: map[string]*os.File{"lock": lock}},
		{ID: "listener.fixture", Kind: "listener-v1", Files: map[string]*os.File{"listener": listener}},
		{ID: "raw.fixture", Kind: "raw-socket-v1", Files: map[string]*os.File{"raw": f.raw}},
	}
	state := fixturePrivate(f.bundle)
	for _, res := range f.resources {
		for name, file := range res.Files {
			identity, err := handoff.ResourceFileIdentity(file)
			if err != nil {
				t.Fatal(err)
			}
			purpose := "raw"
			owner := "server"
			if res.ID == f.bundle.LockResource {
				purpose = "store-lock"
				owner = "store"
			}
			if res.ID == "listener.fixture" {
				purpose = "listener"
				owner = "control"
			}
			state.Resources = append(state.Resources, ResourceOwnership{ID: res.ID, Kind: res.Kind, Name: name, Purpose: purpose, Identity: identity, Owner: owner, CutID: state.Runtime.CutID})
		}
	}
	sort.Slice(state.Resources, func(i, j int) bool {
		return state.Resources[i].ID+"/"+state.Resources[i].Name < state.Resources[j].ID+"/"+state.Resources[j].Name
	})
	private, err := EncodePrivateState(state)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := sealedstate.New(f.bundle.StateResource, private)
	if err != nil {
		t.Fatal(err)
	}
	f.resources = append(f.resources, snapshot.Resources...)
	t.Cleanup(func() {
		_ = snapshot.Close()
		for _, res := range f.resources {
			for _, file := range res.Files {
				_ = file.Close()
			}
		}
		_ = f.peer.Close()
	})
	f.source = handofftest.DupResources(t, f.resources)
	f.store = &testStore{}
	f.control = &testControl{events: &f.events}
	f.config = Config{Mode: f.bundle.Mode, Deadline: controlbudget.Deadline(1 << 62), NewStore: func(storage.StoreStamp, *os.File, controlbudget.Deadline) (StoreOwner, error) {
		f.events = append(f.events, "store")
		return f.store, nil
	}, NewControl: func(StoreOwner, controlbudget.Deadline) (ControlOwner, error) {
		f.events = append(f.events, "control")
		return f.control, nil
	}}
	f.config.ImportServer = func(_ tunnel.ServerImportConfig, _ tunnel.ServerImage, source tunnel.FileSource) (tunnel.ServerImported, tunnel.Salvage, error) {
		file, err := source.TakeFile("raw.fixture")
		if err != nil {
			return nil, tunnel.Salvage{}, err
		}
		f.server = &sleepingServer{file: file, events: &f.events}
		return f.server, tunnel.Salvage{}, nil
	}
	return f
}

type flatFaultSource struct {
	source        *handofftest.DupSource
	ordinal, fail int
}

func (s *flatFaultSource) TakeFile(ref string) (*os.File, error) {
	s.ordinal++
	id, name, explicit := strings.Cut(ref, "/")
	if !explicit {
		for _, resource := range s.source.Borrow() {
			if resource.ID == id {
				if len(resource.Files) != 1 {
					return nil, ErrImage
				}
				for fileName := range resource.Files {
					name = fileName
				}
			}
		}
	}
	file, err := s.source.TakeFile(id, name)
	if s.ordinal == s.fail {
		return file, errors.New("injected Take failure")
	}
	return file, err
}
func closeSalvage(s tunnel.Salvage) {
	for _, res := range s.Resources {
		for _, file := range res.Files {
			if file != nil {
				_ = file.Close()
			}
		}
	}
}
func fileCount(resources []handoff.Resource) int {
	n := 0
	for _, resource := range resources {
		n += len(resource.Files)
	}
	return n
}

type sleepingServer struct {
	file        *os.File
	events      *[]string
	detaches    int
	detachError error
	detachExtra []handoff.Resource
}

func (s *sleepingServer) Server() *tunnel.Server { return nil }
func (s *sleepingServer) Activate(controlbudget.Deadline) error {
	*s.events = append(*s.events, "server-active")
	return nil
}
func (s *sleepingServer) DetachPristine() (tunnel.Salvage, error) {
	s.detaches++
	resources := []handoff.Resource{{ID: "raw.fixture", Kind: "raw-socket-v1", Files: map[string]*os.File{"raw": s.file}}}
	resources = append(resources, s.detachExtra...)
	return tunnel.Salvage{Resources: resources}, s.detachError
}

type testStore struct{}

func (*testStore) Quiesce(controlbudget.Deadline) error { return nil }
func (*testStore) CloseForImage(controlbudget.Deadline) (storage.StoreSeal, error) {
	return storage.StoreSeal{}, errors.New("unused contract method")
}
func (*testStore) ReopenCurrent(storage.StoreStamp, controlbudget.Deadline) (StoreOwner, error) {
	return nil, errors.New("unused contract method")
}
func (*testStore) Stamp() (storage.StoreStamp, error) {
	return storage.StoreStamp{}, errors.New("unused contract method")
}
func (*testStore) LockResource() (handoff.Resource, error) {
	return handoff.Resource{}, errors.New("unused contract method")
}

type testControl struct{ events *[]string }

func (*testControl) FenceTransfers(controlbudget.Deadline) (uint32, error) { return 0, nil }
func (*testControl) ResumeTransfers() error                                { return nil }
func (*testControl) Fence(controlbudget.Deadline) error                    { return nil }
func (*testControl) JoinStoreUsers(controlbudget.Deadline) error           { return nil }
func (*testControl) StopTransport(controlbudget.Deadline) error            { return nil }
func (c *testControl) StartAuth(controlbudget.Deadline) error {
	*c.events = append(*c.events, "auth")
	return nil
}
func (c *testControl) StartBusiness(controlbudget.Deadline) error {
	*c.events = append(*c.events, "business")
	return nil
}
func (*testControl) StopCurrent(controlbudget.Deadline) error { return nil }

type testSlot struct {
	store   StoreOwner
	control ControlOwner
	server  tunnel.ServerImported
	events  *[]string
}

func (s *testSlot) Install(store StoreOwner, control ControlOwner, server tunnel.ServerImported) error {
	s.store, s.control, s.server = store, control, server
	*s.events = append(*s.events, "install")
	return nil
}
func (*testSlot) StopCurrent(controlbudget.Deadline) error {
	return errors.New("unused contract method")
}

// origin: hot-update S6 plan §4.2 / T04
func TestBrokerPartialTakeSalvage(t *testing.T) { exerciseBrokerPartialTake(t) }
func exerciseBrokerPartialTake(t *testing.T) {
	t.Helper()
	for fail := 1; fail <= 4; fail++ {
		t.Run(string(rune('0'+fail)), func(t *testing.T) {
			f := newImportFixture(t)
			source := &flatFaultSource{source: f.source, fail: fail}
			image, salvage, err := Import(f.config, f.bundle, source)
			defer closeSalvage(salvage)
			if err == nil || image != nil || source.ordinal != fail {
				t.Fatalf("nth Take did not refuse at %d: image=%T err=%v calls=%d", fail, image, err, source.ordinal)
			}
			if fileCount(salvage.Resources) != fail {
				t.Fatalf("lost nonnil error file at Take %d: salvage=%d", fail, fileCount(salvage.Resources))
			}
			merged, err := handoff.MergeResources(f.source.Borrow(), salvage.Resources)
			if err != nil {
				t.Fatal(err)
			}
			if fileCount(merged) != fileCount(f.resources) {
				t.Fatal("taken+untaken does not conserve the complete table")
			}
			if handoff.ValidateResourceOwners(merged) != nil {
				t.Fatal("salvage has aliased ownership")
			}
			assertOriginalResourcesSurvive(t, f)
			if fail == 4 && (f.server == nil || f.server.detaches != 1) {
				t.Fatal("successful inner import was not detached on outer failure")
			}
			if len(f.events) != 0 {
				t.Fatalf("pristine import started constructors: %v", f.events)
			}
		})
	}
}
func assertOriginalResourcesSurvive(t *testing.T, f *importFixture) {
	t.Helper()
	if _, err := f.peer.Write([]byte("original socket witness")); err != nil {
		t.Fatalf("pristine path shut down the raw OFD: %v", err)
	}
	buffer := make([]byte, 128)
	n, err := f.raw.Read(buffer)
	if err != nil || string(buffer[:n]) != "original socket witness" {
		t.Fatalf("original raw bytes lost: %q %v", buffer[:n], err)
	}
	contender, err := os.OpenFile(f.lockPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = contender.Close() }()
	if err = unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("pristine path released original flock: %v", err)
	}
}

// origin: hot-update S6 plan §4.2 / T04
func TestBrokerImportLeakCycles(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerPartialTake(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker partial Take", beforeG)
	testharness.AssertNoFDLeak(t, "broker partial Take", beforeFD, 0)
}

// origin: hot-update S6 plan §4.2 / T49
func TestBrokerConstructorFailureIsActive(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil", true: "partial"}[partial], func(t *testing.T) {
			f := newImportFixture(t)
			calls := 0
			f.config.NewStore = func(storage.StoreStamp, *os.File, controlbudget.Deadline) (StoreOwner, error) {
				calls++
				if partial {
					return f.store, errors.New("partial writer")
				}
				return nil, errors.New("unknown writer")
			}
			image, salvage, err := Import(f.config, f.bundle, &flatFaultSource{source: f.source})
			if err != nil {
				t.Fatal(err)
			}
			defer closeSalvage(salvage)
			slot := &testSlot{events: &f.events}
			if err = image.Activate(slot, f.config.Deadline); !errors.Is(err, ErrActive) {
				t.Fatalf("constructor failure lost ACTIVE responsibility: %v", err)
			}
			if _, err = image.DetachPristine(); !errors.Is(err, ErrActive) {
				t.Fatal("failed opener returned to pristine")
			}
			if err = image.Activate(slot, f.config.Deadline); !errors.Is(err, ErrActive) || calls != 1 {
				t.Fatal("failed opener retried construction")
			}
			private, privateErr := ImportedPrivateState(image)
			if privateErr != nil || !private.Runtime.Active {
				t.Fatal("partial constructor lost private ACTIVE marker")
			}
			store, _, _, active := CurrentOwners(image)
			if !active || (partial && (store != f.store || slot.store != f.store)) {
				t.Fatal("partial constructor owner discarded")
			}
			resources, err := ImportedResources(image)
			if err != nil {
				t.Fatal(err)
			}
			defer closeSalvage(resources)
		})
	}
}

// origin: hot-update S6 plan §4.2 / §8.1 / T49
func TestBrokerActivationOrdersAuthorityBeforeBusiness(t *testing.T) {
	f := newImportFixture(t)
	image, _, err := Import(f.config, f.bundle, &flatFaultSource{source: f.source})
	if err != nil {
		t.Fatal(err)
	}
	slot := &testSlot{events: &f.events}
	if err = image.Activate(slot, f.config.Deadline); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(f.events, []string{"store", "control", "install", "auth", "server-active", "business"}) {
		t.Fatalf("unsafe activation order: %v", f.events)
	}
	resources, err := ImportedResources(image)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSalvage(resources)
	if _, err = image.DetachPristine(); !errors.Is(err, ErrActive) {
		t.Fatal("active business detached as pristine")
	}
}

// origin: hot-update S6 plan §4.1 / T27
func TestBrokerImportRejectsCrossResourceAlias(t *testing.T) {
	f := newImportFixture(t)
	// Make a malicious source return the raw socket as the listener descriptor.
	// The original descriptor proof must reject it after inner import succeeds.
	source := &aliasSource{normal: &flatFaultSource{source: f.source}, raw: f.raw}
	image, salvage, err := Import(f.config, f.bundle, source)
	defer closeSalvage(salvage)
	if err == nil || image != nil || f.server == nil || f.server.detaches != 1 {
		t.Fatalf("cross-resource identity accepted: %T %v", image, err)
	}
	if fileCount(salvage.Resources) != 4 {
		t.Fatal("alias refusal lost a taken descriptor")
	}
	assertOriginalResourcesSurvive(t, f)
}

type aliasSource struct {
	normal *flatFaultSource
	raw    *os.File
}

func (s *aliasSource) TakeFile(ref string) (*os.File, error) {
	if strings.HasPrefix(ref, "listener.fixture") {
		raw, err := s.raw.SyscallConn()
		if err != nil {
			return nil, err
		}
		var fd int
		var dupErr error
		if err = raw.Control(func(original uintptr) { fd, dupErr = unix.FcntlInt(original, unix.F_DUPFD_CLOEXEC, 3) }); err != nil || dupErr != nil {
			return nil, errors.Join(err, dupErr)
		}
		return os.NewFile(uintptr(fd), "aliased-listener"), nil
	}
	return s.normal.TakeFile(ref)
}

// origin: hot-update S6 plan §4.2 / T04
func TestBrokerImportSalvagesUnexpectedInnerFiles(t *testing.T) {
	f := newImportFixture(t)
	extra, err := os.CreateTemp(t.TempDir(), "unexpected-inner-state")
	if err != nil {
		t.Fatal(err)
	}
	f.config.ImportServer = func(tunnel.ServerImportConfig, tunnel.ServerImage, tunnel.FileSource) (tunnel.ServerImported, tunnel.Salvage, error) {
		return nil, tunnel.Salvage{Resources: []handoff.Resource{{ID: "unexpected-state", Kind: "fixture", Files: map[string]*os.File{"state": extra}}}}, errors.New("inner import failed")
	}
	image, salvage, err := Import(f.config, f.bundle, &flatFaultSource{source: f.source})
	defer closeSalvage(salvage)
	if err == nil || image != nil {
		t.Fatalf("unexpected inner state was accepted: %T %v", image, err)
	}
	found := false
	for _, resource := range salvage.Resources {
		if resource.ID == "unexpected-state" && resource.Files["state"] == extra {
			found = true
		}
	}
	if !found {
		t.Fatal("inner error receipt silently dropped its nonnil file")
	}
	if _, err = extra.Stat(); err != nil {
		t.Fatal("refusal closed a taken inner file")
	}
}

// origin: hot-update S6 plan §4.2 / T04
func TestBrokerPristineDetachTransfersOwnershipOnce(t *testing.T) {
	f := newImportFixture(t)
	image, _, err := Import(f.config, f.bundle, &flatFaultSource{source: f.source})
	if err != nil {
		t.Fatal(err)
	}
	state, err := ImportedPrivateState(image)
	if err != nil || state.Runtime.Generation != f.bundle.Generation || len(state.Control.HomeBaselines) != 1 {
		t.Fatalf("private projection changed: %v", err)
	}
	salvage, err := image.DetachPristine()
	if err != nil {
		t.Fatal(err)
	}
	defer closeSalvage(salvage)
	if fileCount(salvage.Resources) != fileCount(f.resources) || f.source.Remaining() != 0 {
		t.Fatal("pristine detach lost files")
	}
	if _, err = image.DetachPristine(); err == nil {
		t.Fatal("pristine resources acquired a second owner")
	}
	if err = image.Activate(&testSlot{events: &f.events}, f.config.Deadline); err == nil {
		t.Fatal("detached owner activated")
	}
	if f.server.detaches != 1 {
		t.Fatal("sleeping owner detached more than once")
	}
	assertOriginalResourcesSurvive(t, f)
}

// origin: hot-update S6 plan §4.1 / T04 / T27
func TestBrokerImportRejectsChangedLockFingerprint(t *testing.T) {
	f := newImportFixture(t)
	replacement, err := os.CreateTemp(t.TempDir(), "foreign-lock")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replacement.Close() }()
	source := &changedLockSource{normal: &flatFaultSource{source: f.source}, lock: f.bundle.LockResource, replacement: replacement}
	image, salvage, err := Import(f.config, f.bundle, source)
	defer closeSalvage(salvage)
	if image != nil {
		held, _ := ImportedResources(image)
		defer closeSalvage(held)
	}
	if err == nil || image != nil {
		t.Fatal("wrong lock OFD admitted before ACTIVE")
	}
	if fileCount(salvage.Resources) != 2 {
		t.Fatal("wrong lock refusal lost its returned file")
	}
	assertOriginalResourcesSurvive(t, f)
}

type changedLockSource struct {
	normal      *flatFaultSource
	lock        string
	replacement *os.File
}

func (s *changedLockSource) TakeFile(ref string) (*os.File, error) {
	if ref == s.lock {
		return s.replacement, nil
	}
	return s.normal.TakeFile(ref)
}

// origin: hot-update S6 plan §4.1 / T04 / T27
func TestBrokerImportRejectsChangedListenerCookie(t *testing.T) {
	f := newImportFixture(t)
	f.bundle.Listeners[0].Cookie++
	image, salvage, err := Import(f.config, f.bundle, &flatFaultSource{source: f.source})
	defer closeSalvage(salvage)
	if image != nil {
		held, _ := ImportedResources(image)
		defer closeSalvage(held)
	}
	if err == nil || image != nil {
		t.Fatal("wrong original listener cookie admitted")
	}
	if f.server == nil || f.server.detaches != 1 {
		t.Fatal("outer listener refusal did not detach successful server import")
	}
	assertOriginalResourcesSurvive(t, f)
}

// origin: hot-update S6 plan §4.2 / T04
func TestBrokerMultiFileResourceKeepsMetadata(t *testing.T) {
	f := newImportFixture(t)
	first, err := os.CreateTemp(t.TempDir(), "first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.CreateTemp(t.TempDir(), "second")
	if err != nil {
		t.Fatal(err)
	}
	resource := handoff.Resource{ID: "opaque.multi", Kind: "fixture-resource", Data: []byte(`{"label":"opaque"}`), Files: map[string]*os.File{"first": first, "second": second}}
	data, _, err := sealedstate.Read(borrowedState{f.resources}, f.bundle.StateResource, "state", sealedstate.MaxStateBytes)
	if err != nil {
		t.Fatal(err)
	}
	private, err := DecodePrivateState(data)
	if err != nil {
		t.Fatal(err)
	}
	for name, file := range resource.Files {
		identity, err := handoff.ResourceFileIdentity(file)
		if err != nil {
			t.Fatal(err)
		}
		private.Resources = append(private.Resources, ResourceOwnership{ID: resource.ID, Kind: resource.Kind, Data: resource.Data, Name: name, Purpose: "fixture", Identity: identity, Owner: "control", CutID: private.Runtime.CutID})
	}
	sort.Slice(private.Resources, func(i, j int) bool {
		return private.Resources[i].ID+"/"+private.Resources[i].Name < private.Resources[j].ID+"/"+private.Resources[j].Name
	})
	data, err = EncodePrivateState(private)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := sealedstate.New(f.bundle.StateResource, data)
	if err != nil {
		t.Fatal(err)
	}
	for i := range f.resources {
		if f.resources[i].ID == f.bundle.StateResource {
			f.resources[i] = snapshot.Resources[0]
		}
	}
	f.resources = append(f.resources, resource)
	f.source = handofftest.DupResources(t, f.resources)
	image, _, err := Import(f.config, f.bundle, &flatFaultSource{source: f.source})
	if err != nil {
		t.Fatal(err)
	}
	salvage, err := image.DetachPristine()
	if err != nil {
		t.Fatal(err)
	}
	defer closeSalvage(salvage)
	found := false
	for _, got := range salvage.Resources {
		if got.ID == resource.ID {
			found = true
			if len(got.Files) != 2 || !reflect.DeepEqual(got.Data, resource.Data) {
				t.Fatal("multi-file metadata changed")
			}
		}
	}
	if !found || fileCount(salvage.Resources) != 6 {
		t.Fatal("multi-file resource lost a file")
	}
}

// origin: hot-update S6 plan §2.1 / §4.2 / T04
func TestBrokerExpiredImportNeverTakesFiles(t *testing.T) {
	f := newImportFixture(t)
	source := &flatFaultSource{source: f.source}
	now, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	f.config.Deadline = controlbudget.Deadline(now - 1)
	image, salvage, err := Import(f.config, f.bundle, source)
	if err == nil || image != nil || source.ordinal != 0 || fileCount(salvage.Resources) != 0 {
		t.Fatal("expired cut took ownership")
	}
	f.config.Deadline = controlbudget.Deadline(1 << 62)
	image, _, err = Import(f.config, f.bundle, source)
	if err != nil {
		t.Fatal(err)
	}
	if err = image.Activate(&testSlot{events: &f.events}, controlbudget.Deadline(now-1)); err == nil {
		t.Fatal("expired activation began construction")
	}
	if len(f.events) != 0 {
		t.Fatal("expired activation invoked side effects")
	}
	salvage, err = image.DetachPristine()
	if err != nil {
		t.Fatal("expiry before constructor incorrectly crossed ACTIVE")
	}
	defer closeSalvage(salvage)
}

// origin: hot-update S6 plan §4.2 / T04
func TestBrokerPartialDetachErrorKeepsSalvage(t *testing.T) {
	f := newImportFixture(t)
	extra, err := os.CreateTemp(t.TempDir(), "partial-detach-state")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = extra.Close() }()
	original := f.config.ImportServer
	f.config.ImportServer = func(config tunnel.ServerImportConfig, image tunnel.ServerImage, source tunnel.FileSource) (tunnel.ServerImported, tunnel.Salvage, error) {
		imported, salvage, err := original(config, image, source)
		f.server.detachError = errors.New("partial detach")
		f.server.detachExtra = []handoff.Resource{{ID: "partial-detach", Kind: "fixture", Files: map[string]*os.File{"state": extra}}}
		return imported, salvage, err
	}
	f.bundle.Listeners[0].Cookie++ // Refusal occurs after successful sleeping import.
	image, salvage, err := Import(f.config, f.bundle, &flatFaultSource{source: f.source})
	defer closeSalvage(salvage)
	if err == nil || image == nil {
		t.Fatal("partial detach lost its remaining owner")
	}
	found := false
	for _, resource := range salvage.Resources {
		if resource.ID == "partial-detach" && resource.Files["state"] == extra {
			found = true
		}
	}
	if !found {
		t.Fatal("partial detach error dropped its returned file")
	}
	if fileCount(salvage.Resources) != 5 {
		t.Fatal("partial detach error lost taken files")
	}
	assertOriginalResourcesSurvive(t, f)
}
