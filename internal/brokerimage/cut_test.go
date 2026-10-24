//go:build linux

package brokerimage

import (
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// These exercise P01's orchestration against simulated leaf contracts, using
// real sealed memfds and handoff duplicates. P26/P28 own real SQLite/Node/socket
// integration and must not treat these contract tests as those runtime receipts.
type cutEvents struct {
	mu    sync.Mutex
	items []string
	fail  string
}

func (e *cutEvents) call(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.items = append(e.items, name)
	if name == e.fail {
		return errors.New("injected owner failure")
	}
	return nil
}

func (e *cutEvents) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.items...)
}

type cutStore struct {
	events   *cutEvents
	stamp    storage.StoreStamp
	lock     *os.File
	seal     storage.StoreSeal
	reopen   StoreOwner
	openErr  error
	openN    int
	closed   bool
	terminal bool
	openHook func()
}

func (s *cutStore) Stamp() (storage.StoreStamp, error) {
	return cloneStamp(s.stamp), s.events.call("store.stamp")
}
func (s *cutStore) Quiesce(controlbudget.Deadline) error { return s.events.call("store.quiesce") }
func (s *cutStore) Resume() error                        { return s.events.call("store.resume") }
func (s *cutStore) CloseForImage(controlbudget.Deadline) (storage.StoreSeal, error) {
	for _, event := range []string{"raft.close", "transport.join", "bolt.close", "cursor.final", "checkpoint", "readers.close", "writer.close"} {
		if err := s.events.call(event); err != nil {
			failed := s.seal
			failed.Writer = storage.CloseUnknown
			return failed, err
		}
	}
	s.closed = true
	return s.seal, nil
}
func (s *cutStore) ReopenCurrent(stamp storage.StoreStamp, _ controlbudget.Deadline) (StoreOwner, error) {
	s.openN++
	_ = s.events.call("store.reopen")
	if s.openHook != nil {
		s.openHook()
	}
	if !s.closed || !reflect.DeepEqual(stamp, s.seal.Stamp) {
		return nil, errors.New("reopen did not use final current stamp")
	}
	return s.reopen, s.openErr
}
func (s *cutStore) LockResource() (handoff.Resource, error) {
	return handoff.Resource{ID: "broker.lock", Kind: "broker-lock", Files: map[string]*os.File{"lock": s.lock}}, nil
}

type cutControl struct {
	events        *cutEvents
	name          string
	busy          uint32
	stopped       bool
	stopHook      func() error
	transportHook func() error
	authHook      func() error
}

func (c *cutControl) FenceTransfers(controlbudget.Deadline) (uint32, error) {
	return c.busy, c.events.call(c.name + ".transfers.fence")
}
func (c *cutControl) ResumeTransfers() error { return c.events.call(c.name + ".transfers.resume") }
func (c *cutControl) Fence(controlbudget.Deadline) error {
	return c.events.call(c.name + ".fence")
}
func (c *cutControl) JoinStoreUsers(controlbudget.Deadline) error {
	return c.events.call(c.name + ".users.join")
}
func (c *cutControl) StopTransport(controlbudget.Deadline) error {
	if c.transportHook != nil {
		if err := c.transportHook(); err != nil {
			return err
		}
	}
	return c.events.call(c.name + ".transport.stop")
}
func (c *cutControl) StartAuth(controlbudget.Deadline) error {
	if c.authHook != nil {
		if err := c.authHook(); err != nil {
			return err
		}
	}
	return c.events.call(c.name + ".auth")
}
func (c *cutControl) StartBusiness(controlbudget.Deadline) error {
	return c.events.call(c.name + ".business")
}
func (c *cutControl) StopCurrent(controlbudget.Deadline) error {
	if c.stopHook != nil {
		if err := c.stopHook(); err != nil {
			return err
		}
	}
	if err := c.events.call(c.name + ".stop"); err != nil {
		return err
	}
	c.stopped = true
	return nil
}

type cutKeeper struct {
	events       *cutEvents
	id           string
	file         *os.File
	closed       bool
	invalidFlags bool
}

func (k *cutKeeper) Descriptor() listenerimage.Descriptor {
	return listenerimage.Descriptor{ID: k.id, Network: "contract", Address: "contract", Nonblock: true, CloseOnExec: !k.invalidFlags}
}
func (k *cutKeeper) View() net.Listener { return nil }
func (k *cutKeeper) Pause(controlbudget.Deadline) error {
	return k.events.call(k.id + ".pause")
}
func (k *cutKeeper) Resume() error {
	if k.closed {
		return errors.New("revoked listener resumed")
	}
	return k.events.call(k.id + ".resume")
}
func (k *cutKeeper) Export() (listenerimage.Descriptor, handoff.Resource, error) {
	if k.closed {
		return listenerimage.Descriptor{}, handoff.Resource{}, errors.New("revoked listener exported")
	}
	return k.Descriptor(), handoff.Resource{ID: k.id, Kind: "listener", Files: map[string]*os.File{"listener": k.file}}, nil
}
func (k *cutKeeper) Detach() (*os.File, error) { return k.file, nil }
func (k *cutKeeper) Close() error {
	if k.closed {
		return nil
	}
	k.closed = true
	return k.file.Close()
}

type cutServer struct {
	events  *cutEvents
	version uint64
	last    *os.File
	state   *sealedstate.Snapshot
	exportN int
	thawed  bool
	offset  uint64
}

func (s *cutServer) Export() (tunnel.ServerImage, []handoff.Resource, error) {
	s.exportN++
	snapshot, err := sealedstate.New("tunnel.state", []byte{byte(s.version)})
	if err != nil {
		return tunnel.ServerImage{}, nil, err
	}
	s.last = snapshot.Owned[0]
	err = s.events.call("server.export")
	if err != nil {
		_ = snapshot.Close()
	} else {
		old := s.state
		s.state = snapshot
		_ = old.Close()
	}
	return tunnel.ServerImage{Schema: TunnelStateRecord, SchemaVersion: 1, Generation: s.version, StateResource: "tunnel.state",
		Flows: flow.Image{Records: []flow.RecordImage{{RxApplied: s.offset}}}}, snapshot.Resources, err
}
func (s *cutServer) Thaw(controlbudget.Deadline) error {
	if err := s.events.call("server.thaw"); err != nil {
		return err
	}
	s.thawed = true
	return s.state.Close()
}
func (s *cutServer) Discard() error {
	return errors.Join(s.events.call("server.discard"), s.state.Close())
}

type cutFixture struct {
	events  *cutEvents
	store   *cutStore
	control *cutControl
	server  *cutServer
	keeper  *cutKeeper
	cfg     Config
}

func newCutFixture(t *testing.T) *cutFixture {
	t.Helper()
	events := &cutEvents{}
	lock := cutFile(t, "lock")
	migrations, err := storage.SupportedMigrations()
	if err != nil {
		t.Fatal(err)
	}
	store := &cutStore{events: events, lock: lock, stamp: storage.StoreStamp{
		Identity: storage.DBIdentity{Path: "/contract/current.db", DataDir: "/contract", LockPath: "/contract/tether.lock", Device: 1, Inode: 2},
		Schema:   storage.SchemaStamp{Applied: migrations, Supported: migrations, Digest: migrationDigest(migrations), ObjectDigest: strings.Repeat("b", 64)},
		Journal:  "wal", Synchronous: 2, AppliedIndex: 7, AppliedTerm: 2,
	}}
	store.seal = storage.StoreSeal{Stamp: cloneStamp(store.stamp), Writer: storage.CloseClosed, ReadOnly: storage.CloseClosed,
		Bolt: storage.CloseClosed, Transport: storage.CloseClosed, Raft: storage.CloseClosed}
	control := &cutControl{events: events, name: "old"}
	server := &cutServer{events: events, version: 1}
	t.Cleanup(func() { _ = server.state.Close() })
	keeper := &cutKeeper{events: events, id: "public.listener", file: cutFile(t, "listener")}
	f := &cutFixture{events: events, store: store, control: control, server: server, keeper: keeper}
	f.cfg = Config{Mode: ModeSingle, Store: store, Control: control, Server: &tunnel.Server{},
		Listeners: []listenerimage.Keeper{keeper}, Deadline: futureCutDeadline(t),
		FreezeServer: func(*tunnel.Server, controlbudget.Deadline) (tunnel.ServerFrozen, error) {
			return server, events.call("server.freeze")
		}}
	return f
}

func cutFile(t *testing.T, name string) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}

func futureCutDeadline(t *testing.T) controlbudget.Deadline {
	t.Helper()
	now, err := procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	return controlbudget.Deadline(now + int64(time.Minute))
}

func freezeFixture(t *testing.T, f *cutFixture) Cut {
	t.Helper()
	cut, err := Freeze(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cut.Discard() })
	return cut
}

func exportFixture(t *testing.T, f *cutFixture, cut Cut) (Bundle, []handoff.Resource) {
	t.Helper()
	seal, err := f.store.CloseForImage(f.cfg.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	bundle, resources, err := cut.Export(seal)
	if err != nil {
		t.Fatal(err)
	}
	return bundle, resources
}

func prepareFixture(t *testing.T, cut Cut, bundle Bundle, resources []handoff.Resource) (*handoff.Prepared, Bundle) {
	t.Helper()
	if bundle.Generation == 0 {
		bundle.Generation = 2
	}
	bundle.InstanceID, bundle.BootID = "instance", "boot"
	bundle.ExporterSHA256, bundle.CandidateSHA256 = strings.Repeat("a", 64), strings.Repeat("b", 64)
	bundle.PreviousSHA256, bundle.ConfigDigest = bundle.ExporterSHA256, strings.Repeat("c", 64)
	bundle.Args, bundle.StateResource = []string{"tether", "serve"}, "broker.state"
	snapshot, err := sealedstate.New(bundle.StateResource, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	resources = append(resources, snapshot.Resources...)
	prepared, err := cut.Prepare(bundle, resources)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prepared.Close() })
	return prepared, bundle
}

// origin: hot-update S6 plan §3.2 / T09 / §12.1 P01
func TestBrokerShutdownReceiptOrder(t *testing.T) {
	f := newCutFixture(t)
	cut := freezeFixture(t, f)
	if f.server.exportN != 0 || f.store.closed {
		t.Fatal("Freeze exported a preliminary seal or closed the store outside Run")
	}
	bundle, resources := exportFixture(t, f, cut)
	_, _ = prepareFixture(t, cut, bundle, resources)
	want := []string{"store.stamp", "old.transfers.fence", "old.fence", "public.listener.pause", "store.quiesce", "old.users.join", "server.freeze",
		"raft.close", "transport.join", "bolt.close", "cursor.final", "checkpoint", "readers.close", "writer.close", "server.export", "server.export"}
	if got := f.events.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("final seal order:\n got %v\nwant %v", got, want)
	}
	// Every independently unsafe phase/census is rejected before final export.
	for _, name := range []string{"writer", "reader", "bolt", "transport", "raft", "writers", "readers", "rpc", "backup", "checkpoint", "floor", "raft-log", "raft-config", "schema", "journal", "synchronous"} {
		t.Run(name, func(t *testing.T) {
			f := newCutFixture(t)
			if name == "raft-log" {
				f.store.stamp.RaftLastIndex = 10
			}
			if name == "raft-config" {
				f.store.stamp.RaftConfigIndex = 10
			}
			cut := freezeFixture(t, f)
			seal := cloneSeal(f.store.seal)
			switch name {
			case "writer":
				seal.Writer = storage.CloseOpen
			case "reader":
				seal.ReadOnly = storage.CloseStopped
			case "bolt":
				seal.Bolt = storage.CloseUnknown
			case "transport":
				seal.Transport = storage.CloseOpen
			case "raft":
				seal.Raft = storage.CloseStopped
			case "writers":
				seal.DriverWriters = 1
			case "readers":
				seal.DriverReaders = 1
			case "rpc":
				seal.ActiveRPCs = 1
			case "backup":
				seal.ActiveBackups = 1
			case "checkpoint":
				seal.CheckpointBusy = 1
			case "floor":
				seal.Stamp.AppliedIndex--
			case "raft-log", "raft-config":
				// The final receipt's zero metadata regresses the independent
				// pre-cut Raft floor even though its SQL command floor is intact.
			case "schema":
				seal.Stamp.Schema.Digest = "different"
			case "journal":
				seal.Stamp.Journal = "DELETE"
			case "synchronous":
				seal.Stamp.Synchronous = 1
			}
			if _, _, err := cut.Export(seal); !errors.Is(err, ErrStoreClose) || f.server.exportN != 0 {
				t.Fatalf("unsafe seal accepted or exporter called: err=%v exports=%d", err, f.server.exportN)
			}
			if err := cut.Thaw(f.cfg.Deadline); !errors.Is(err, ErrStoreClose) || f.store.openN != 0 {
				t.Fatalf("unknown close reopened/resumed: err=%v opens=%d", err, f.store.openN)
			}
		})
	}
	t.Run("close-error", func(t *testing.T) {
		f := newCutFixture(t)
		cut := freezeFixture(t, f)
		f.events.fail = "writer.close"
		seal, closeErr := f.store.CloseForImage(f.cfg.Deadline)
		if closeErr == nil {
			t.Fatal("close failure was not exercised")
		}
		if _, _, err := cut.Export(seal); !errors.Is(err, ErrStoreClose) {
			t.Fatalf("failed Close became a final seal: %v", err)
		}
	})
	for _, mode := range []Mode{ModeSingle, ModeSoleVoter, ModeQuorum} {
		t.Run(fmt.Sprintf("mode-%d", mode), func(t *testing.T) {
			f := newCutFixture(t)
			f.cfg.Mode = mode
			if mode != ModeSingle {
				f.store.seal.Role = storage.RoleReceipt{Term: 2, ConfigIndex: 1,
					VoterDigest: strings.Repeat("d", 64), ObserverClosed: true, LastRunningIsLeader: mode == ModeSoleVoter}
			}
			cut := freezeFixture(t, f)
			_, _ = exportFixture(t, f, cut)
		})
	}
	for _, badRole := range []string{"observer-open", "quorum-leader", "missing-term", "missing-config", "missing-voters"} {
		t.Run(badRole, func(t *testing.T) {
			f := newCutFixture(t)
			f.cfg.Mode = ModeQuorum
			cut := freezeFixture(t, f)
			seal := cloneSeal(f.store.seal)
			seal.Role = storage.RoleReceipt{Term: 2, ConfigIndex: 1, VoterDigest: strings.Repeat("d", 64), ObserverClosed: true}
			switch badRole {
			case "observer-open":
				seal.Role.ObserverClosed = false
			case "quorum-leader":
				seal.Role.LastRunningIsLeader = true
			case "missing-term":
				seal.Role.Term = 0
			case "missing-config":
				seal.Role.ConfigIndex = 0
			case "missing-voters":
				seal.Role.VoterDigest = ""
			}
			if _, _, err := cut.Export(seal); !errors.Is(err, ErrStoreClose) || f.server.exportN != 0 {
				t.Fatalf("unsafe role allowed a final seal: %v", err)
			}
		})
	}
}

// origin: hot-update S6 plan §6.2 / T25 / §12.1 P01
func TestBrokerPreparedListenerRevokeCannotResurrectEndpoint(t *testing.T) {
	for _, when := range []string{"before-prepare", "after-prepare"} {
		t.Run(when, func(t *testing.T) {
			f := newCutFixture(t)
			cut := freezeFixture(t, f)
			bundle, resources := exportFixture(t, f, cut)
			lockRevoked := false
			if err := RevokeResources(cut, []string{"broker.lock"}, func() error {
				lockRevoked = true
				return nil
			}); err == nil || lockRevoked {
				t.Fatal("late revoke was allowed to release the inherited store lock")
			}
			info, err := f.keeper.file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			var prepared *handoff.Prepared
			if when == "after-prepare" {
				prepared, bundle = prepareFixture(t, cut, bundle, resources)
				if countFileAliases(t, info) != 2 {
					t.Fatal("Prepared listener alias was not actually created")
				}
			}
			if err := RevokeResources(cut, []string{f.keeper.id}, func() error {
				f.server.version++ // Simulated leaf fence changes its projection.
				return f.keeper.Close()
			}); err != nil {
				t.Fatal(err)
			}
			if countFileAliases(t, info) != 0 {
				t.Fatal("revoked listener still has an original or Prepared alias")
			}
			validated := false
			if err := ExecCommit(cut, prepared, func(Bundle) error {
				validated = true
				return errors.New("unexpected validation")
			}, "/missing", bundle.Args, []string{}); !errors.Is(err, ErrCutChanged) || validated {
				t.Fatalf("stale Prepared reached exec validation: %v validated=%v", err, validated)
			}
			fresh, rows, err := cut.Export(f.store.seal)
			if err != nil {
				t.Fatal(err)
			}
			if len(fresh.Listeners) != 0 || fresh.Tunnel.Generation != 2 {
				t.Fatal("preliminary listener/fence resurrected")
			}
			for _, row := range rows {
				if row.ID == f.keeper.id {
					t.Fatal("revoked listener resource was re-exported")
				}
			}
			_, _ = prepareFixture(t, cut, fresh, rows)
			if countFileAliases(t, info) != 0 {
				t.Fatal("next Prepared resurrected revoked listener")
			}
		})
	}
}

func countFileAliases(t *testing.T, info os.FileInfo) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		actual, err := os.Stat("/proc/self/fd/" + entry.Name())
		if err == nil && os.SameFile(info, actual) {
			count++
		}
	}
	return count
}

// origin: hot-update S6 plan §4.1 / §6.2 / §12.1 P01
func TestBrokerCutRejectsAliasAndStaleExport(t *testing.T) {
	f := newCutFixture(t)
	cut := freezeFixture(t, f)
	bundle, resources := exportFixture(t, f, cut)
	if !reflect.DeepEqual(bundle.RequiredDomains, []string{Domain, "flow-v1"}) {
		t.Fatal("retained flow responsibility omitted from reader domains")
	}
	alias := copyResources(resources)
	alias = append(alias, handoff.Resource{ID: "second.owner", Kind: "raw", Files: map[string]*os.File{"raw": f.keeper.file}})
	if err := validateResourceGraph(alias); err == nil {
		t.Fatal("cross-ID original alias accepted")
	}
	// A private fence changed after Export without changing resource IDs. The
	// final Prepare must re-export and require compatibility to be checked again.
	f.server.version++
	bundle.Generation, bundle.InstanceID, bundle.BootID = 2, "instance", "boot"
	bundle.ExporterSHA256, bundle.CandidateSHA256 = strings.Repeat("a", 64), strings.Repeat("b", 64)
	bundle.PreviousSHA256, bundle.ConfigDigest = bundle.ExporterSHA256, strings.Repeat("c", 64)
	bundle.Args, bundle.StateResource = []string{"tether", "serve"}, "broker.state"
	snapshot, err := sealedstate.New(bundle.StateResource, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = snapshot.Close() }()
	if _, err := cut.Prepare(bundle, append(resources, snapshot.Resources...)); !errors.Is(err, ErrCutChanged) {
		t.Fatalf("final Prepare used stale projection: %v", err)
	}
}

// origin: hot-update S6 plan §3.2 / §5 / §12.1 P01
func TestBrokerCutFailureKeepsOwner(t *testing.T) {
	for _, point := range []string{"old.transfers.fence", "old.fence", "public.listener.pause", "store.quiesce", "old.users.join", "server.freeze"} {
		t.Run(point, func(t *testing.T) {
			f := newCutFixture(t)
			f.events.fail = point
			cut, err := Freeze(f.cfg)
			if err == nil || cut != nil {
				t.Fatalf("failed fence did not unwind: cut=%v err=%v", cut, err)
			}
			if f.store.closed || f.server.exportN != 0 {
				t.Fatal("failed freeze closed store or exported a seal")
			}
			if _, err := f.keeper.file.Stat(); err != nil {
				t.Fatal("failed cut closed a borrowed listener")
			}
		})
	}
	f := newCutFixture(t)
	f.control.busy = 1
	cut, err := Freeze(f.cfg)
	var deferred *DeferredError
	if !errors.As(err, &deferred) || deferred.Reason != "transfers_in_flight" || cut != nil {
		t.Fatalf("busy census not deferred: %v", err)
	}
	if got := f.events.snapshot(); !reflect.DeepEqual(got, []string{"store.stamp", "old.transfers.fence", "old.transfers.resume"}) {
		t.Fatalf("busy transfer touched another owner: %v", got)
	}
	f = newCutFixture(t)
	f.control.busy, f.events.fail = 1, "old.transfers.resume"
	cut, err = Freeze(f.cfg)
	if err == nil || cut == nil {
		t.Fatal("failed rollback discarded its recovery owner")
	}
	f.events.fail = ""
	if err := cut.Thaw(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S6 plan §2.2 / §4.2 / §12.1 P01
func TestBrokerCutPartialExportClosesPrivate(t *testing.T) {
	f := newCutFixture(t)
	cut := freezeFixture(t, f)
	_, _ = exportFixture(t, f, cut)
	previous := f.server.last
	f.events.fail = "server.export"
	if _, _, err := cut.Export(f.store.seal); err == nil {
		t.Fatal("partial export did not fail")
	}
	if _, err := f.server.last.Stat(); err == nil {
		t.Fatal("non-nil failed export leaked its private file")
	}
	if _, err := previous.Stat(); err != nil {
		t.Fatal("failed replacement closed the prior cut")
	}
	f.events.fail = ""
	_, rows, err := cut.Export(f.store.seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := previous.Stat(); err == nil {
		t.Fatal("successful replacement retained the old private file")
	}
	if err := cut.Discard(); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		for _, file := range row.Files {
			_, err := file.Stat()
			if (row.Kind == "sealed-state-v1") != (err != nil) {
				t.Fatalf("Discard confused private and borrowed ownership: kind=%s err=%v", row.Kind, err)
			}
		}
	}
}

// origin: hot-update S6 plan §12.1 P01 / §11 leak discipline
func TestBrokerCutLeakCycles(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCutFailure(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker cut failure cycles", beforeG)
	testharness.AssertNoFDLeak(t, "broker cut failure cycles", beforeFD, 0)
}

func exerciseCutFailure(t *testing.T) {
	t.Helper()
	f := newCutFixture(t)
	slot, _ := runtimeFixture(t, f)
	f.store.reopen = &cutStore{events: f.events, stamp: cloneStamp(f.store.stamp), lock: f.store.lock}
	f.cfg.NewControl = func(StoreOwner, controlbudget.Deadline) (ControlOwner, error) {
		return &cutControl{events: f.events, name: "current"}, nil
	}
	cut := freezeFixture(t, f)
	if err := BindRuntime(cut, slot); err != nil {
		t.Fatal(err)
	}
	bundle, resources := exportFixture(t, f, cut)
	prepared, bundle := prepareFixture(t, cut, bundle, resources)
	// The syscall is real; a missing absolute path makes it return without any
	// secondary process. Prepared copies must be closed while originals survive.
	if err := ExecCommit(cut, prepared, func(Bundle) error { return nil },
		"/nonexistent-broker-image-contract-test", bundle.Args, []string{}); err == nil {
		t.Fatal("missing exec path unexpectedly succeeded")
	}
	if err := cut.Thaw(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
	if err := slot.StopCurrent(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
	if f.server.last != nil {
		if _, err := f.server.last.Stat(); err == nil {
			t.Fatal("Discard leaked final private state")
		}
	}
}

// origin: hot-update S6 plan §4.1 / §6.2 / §12.1 P01
func TestBrokerCutExecCommitSerializesRevoke(t *testing.T) {
	f := newCutFixture(t)
	cut := freezeFixture(t, f)
	bundle, resources := exportFixture(t, f, cut)
	prepared, bundle := prepareFixture(t, cut, bundle, resources)
	c := cut.(*frozenCut)
	f.control.transportHook = func() error {
		if !c.mu.TryLock() {
			return errors.New("transport join held the cut latch")
		}
		c.mu.Unlock()
		return nil
	}
	entered, release, attempted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	execDone, revokeDone := make(chan error, 1), make(chan error, 1)
	go func() {
		execDone <- ExecCommit(cut, prepared, func(Bundle) error {
			close(entered)
			<-release
			return errors.New("injected final validation failure")
		}, "/missing", bundle.Args, []string{})
	}()
	select {
	case <-entered:
	case err := <-execDone:
		t.Fatalf("exec failed before final validation: %v", err)
	}
	// There is no other latch contender yet. Checking before launching revoke
	// proves ExecCommit owns the latch, rather than observing revoke's brief hold.
	if c.mu.TryLock() {
		c.mu.Unlock()
		close(release)
		<-execDone
		t.Fatal("ExecCommit did not hold the cut latch")
	}
	go func() {
		close(attempted)
		revokeDone <- RevokeResources(cut, []string{f.keeper.id}, func() error { return f.keeper.Close() })
	}()
	<-attempted
	close(release)
	if err := <-execDone; err == nil {
		t.Fatal("final validation failure lost")
	}
	if err := <-revokeDone; err != nil {
		t.Fatal(err)
	}
	if c.prepared != nil {
		t.Fatal("late revoke left the returned Prepared live")
	}
}

type cutObserver struct {
	attempts []controlbudget.Attempt
	fail     bool
}

func (o *cutObserver) Begin(id string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	now, err := procrequest.BootNow()
	return controlbudget.Attempt{LogicalID: id, Phase: phase, Ordinal: uint32(len(o.attempts) + 1), StartedMonoNS: now}, err
}
func (o *cutObserver) Record(attempt controlbudget.Attempt) error {
	o.attempts = append(o.attempts, attempt)
	if o.fail {
		return errors.New("injected observation overflow")
	}
	return nil
}
func (o *cutObserver) Close() error { return nil }

// origin: hot-update S6 plan §8.3 / §12.1 P01
func TestBrokerCutObservationsFailClosed(t *testing.T) {
	f := newCutFixture(t)
	observer := &cutObserver{fail: true}
	f.cfg.Observe = observer
	cut, err := Freeze(f.cfg)
	if err == nil || cut == nil {
		t.Fatal("observation failure silently lost or discarded the fenced owner")
	}
	if len(observer.attempts) != 1 || observer.attempts[0].Phase != controlbudget.PhaseFreeze || observer.attempts[0].FinishedMonoNS < observer.attempts[0].StartedMonoNS {
		t.Fatalf("freeze observation missing: %v", observer.attempts)
	}
	observer.fail = false
	if err := cut.Thaw(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(observer.attempts))
	for i, attempt := range observer.attempts {
		got[i] = fmt.Sprint(attempt.Phase)
	}
	if !reflect.DeepEqual(got, []string{"freeze", "auth", "ready"}) {
		t.Fatalf("phase observations: %v", got)
	}
}

// origin: hot-update S6 plan §3.2 / §6.2 / T09 / T25
func TestBrokerFinalProjectionUsesCurrentResourceIdentities(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerFinalProjection(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker final projection", g)
	testharness.AssertNoFDLeak(t, "broker final projection", fd, 0)
}
func exerciseBrokerFinalProjection(t *testing.T) {
	t.Helper()
	f := newCutFixture(t)
	cut := freezeFixture(t, f)
	_, _ = exportFixture(t, f, cut)
	old := f.server.last
	projected := 0
	prepared, err := PrepareFinal(cut, func(bundle Bundle, resources []handoff.Resource) (Bundle, []handoff.Resource, error) {
		projected++
		if f.server.exportN != 2 {
			t.Fatal("projection preceded final leaf export")
		}
		if _, err := old.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("final projection retained obsolete private file")
		}
		for _, resource := range resources {
			for _, file := range resource.Files {
				if _, err := handoff.ResourceFileIdentity(file); err != nil {
					t.Fatal("projection received a replaced file", err)
				}
			}
		}
		bundle.Generation = 2
		bundle.InstanceID, bundle.BootID = "instance", "boot"
		bundle.ExporterSHA256, bundle.CandidateSHA256 = strings.Repeat("a", 64), strings.Repeat("b", 64)
		bundle.PreviousSHA256, bundle.ConfigDigest = bundle.ExporterSHA256, strings.Repeat("c", 64)
		bundle.Args, bundle.StateResource = []string{"tether", "serve"}, "broker.final-state"
		state, err := sealedstate.New(bundle.StateResource, []byte{1})
		if err != nil {
			return bundle, resources, err
		}
		return bundle, append(resources, state.Resources...), nil
	})
	if err != nil || prepared == nil || projected != 1 {
		t.Fatalf("final projection: calls=%d err=%v", projected, err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cut.Discard(); err != nil {
		t.Fatal(err)
	}
}

type decoratedTestCut struct{ Cut }

func (c decoratedTestCut) ImageCut() Cut { return c.Cut }

// origin: hot-update S6 plan §3.2 / T01
func TestBrokerRuntimeBindingSurvivesCutDecoration(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerDecoratedCut(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "decorated current cut", g)
	testharness.AssertNoFDLeak(t, "decorated current cut", fd, 0)
}
func exerciseBrokerDecoratedCut(t *testing.T) {
	t.Helper()
	f := newCutFixture(t)
	f.cfg.NewControl = func(StoreOwner, controlbudget.Deadline) (ControlOwner, error) {
		return &cutControl{events: f.events, name: "new"}, nil
	}
	f.store.reopen = &cutStore{events: f.events, stamp: f.store.stamp, lock: f.store.lock, seal: f.store.seal}
	cut := decoratedTestCut{freezeFixture(t, f)}
	slot, err := NewRuntimeSlot(func(StoreOwner, controlbudget.Deadline) error { return nil }, func(tunnel.ServerImported, controlbudget.Deadline) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := BindRuntime(cut, slot); err != nil {
		t.Fatal("decorator lost current slot", err)
	}
	if err := MarkStoreClosing(cut); err != nil {
		t.Fatal("decorator lost store-close responsibility", err)
	}
	_, _ = exportFixture(t, f, cut)
	if err := cut.Thaw(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S6 plan §4.2 / T09 / T25
func TestBrokerFinalizerRefusalRetainsPrivateCloser(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerFinalizerRefusal(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "finalizer refusal custody", g)
	testharness.AssertNoFDLeak(t, "finalizer refusal custody", fd, 0)
}
func exerciseBrokerFinalizerRefusal(t *testing.T) {
	t.Helper()
	f := newCutFixture(t)
	cut := freezeFixture(t, f)
	_, _ = exportFixture(t, f, cut)
	var state *os.File
	prepared, err := PrepareFinal(cut, func(bundle Bundle, resources []handoff.Resource) (Bundle, []handoff.Resource, error) {
		bundle.Store.AppliedIndex++
		snapshot, err := sealedstate.New("broker.refused-state", []byte{1})
		if err != nil {
			t.Fatal(err)
		}
		state = snapshot.Owned[0]
		return bundle, append(resources, snapshot.Resources...), nil
	})
	if !errors.Is(err, ErrCutChanged) || prepared != nil {
		t.Fatal("accepted changed final store projection", err)
	}
	if _, err := state.Stat(); err != nil {
		t.Fatal("refusal silently released retained state", err)
	}
	if err := cut.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, err := state.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("refused finalizer lost private terminal closer")
	}
}
