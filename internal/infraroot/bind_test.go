package infraroot

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infralaunch"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/ingressswitch"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// These record providers model before-effect rejections only. No synthetic
// census is used to sign a kernel/systemd/stock success. Stage, journal replay,
// and resource custody below exercise real files and OFDs in private directories.
type rootFacts struct {
	validate atomic.Int32
	deny     atomic.Bool
}

func (f *rootFacts) Validate(c.HostRequest, infrahost.HostState, c.Deadline, c.Stop) error {
	f.validate.Add(1)
	if f.deny.Load() {
		return c.ErrIncomplete
	}
	return nil
}
func (*rootFacts) Inspect(c.ID, c.Deadline) (c.Census, error) { return c.Census{}, c.ErrIncomplete }
func (*rootFacts) Resolve(infrahost.JournalEvent, c.Deadline, c.Stop) (c.HostReceipt, error) {
	return c.HostReceipt{}, c.ErrIncomplete
}

type rootSource struct {
	path  string
	calls atomic.Int32
}

func (s *rootSource) Open(c.Artifact, c.Digest, c.Deadline) (*os.File, error) {
	s.calls.Add(1)
	return os.Open(s.path)
}

type rootRecords struct{ calls atomic.Int32 }

func (r *rootRecords) Load(c.Digest) (ingressswitch.Change, error) {
	r.calls.Add(1)
	return ingressswitch.Change{}, c.ErrIncomplete
}

type rejectingIngress struct{}

func (rejectingIngress) Read(c.Deadline, c.Stop) (ingressswitch.Snapshot, error) {
	return ingressswitch.Snapshot{}, c.ErrIncomplete
}
func (rejectingIngress) CompareAndSwap(ingressswitch.Snapshot, ingressswitch.Snapshot, c.Deadline, c.Stop) (ingressswitch.Snapshot, error) {
	return ingressswitch.Snapshot{}, c.ErrIncomplete
}

type rejectingEvidence struct{}

func (rejectingEvidence) Probe(c.Process, bool, c.Deadline, c.Stop) (ingressswitch.ProbeProof, error) {
	return ingressswitch.ProbeProof{}, c.ErrIncomplete
}
func (rejectingEvidence) Seal(ingressswitch.Snapshot, c.Process, c.Deadline, c.Stop) (ingressswitch.Seal, error) {
	return ingressswitch.Seal{}, c.ErrIncomplete
}
func (rejectingEvidence) Census(c.ID, c.Deadline) (c.Census, error) {
	return c.Census{}, c.ErrIncomplete
}

type rejectingUnits struct{}

func (rejectingUnits) Read(infrahost.UnitTarget, c.Deadline, c.Stop) (infrahost.EffectiveUnit, error) {
	return infrahost.EffectiveUnit{}, c.ErrIncomplete
}
func (rejectingUnits) ReadHelper(c.Process, c.Deadline, c.Stop) (infrahost.EffectiveUnit, error) {
	return infrahost.EffectiveUnit{}, c.ErrIncomplete
}
func (rejectingUnits) Reload(c.Deadline, c.Stop) error                      { return c.ErrIncomplete }
func (rejectingUnits) Start(infrahost.UnitTarget, c.Deadline, c.Stop) error { return c.ErrIncomplete }
func (rejectingUnits) Fence(infrahost.UnitTarget, c.Deadline, c.Stop) error { return c.ErrIncomplete }

type rejectingState struct{ calls atomic.Int32 }

func (s *rejectingState) Load(c.Component, c.Deadline) (infrahost.StartupFence, error) {
	s.calls.Add(1)
	return infrahost.StartupFence{}, c.ErrIncomplete
}
func (*rejectingState) CompareAndSwap(infrahost.StartupFence, infrahost.StartupFence, c.HostRequest, c.Deadline) (c.HostReceipt, error) {
	return c.HostReceipt{}, c.ErrIncomplete
}

type rootFiles struct {
	accepts atomic.Int32
	reject  atomic.Bool
}

func (f *rootFiles) Accept(c.HostRequest, c.Peer, []*os.File, c.Deadline, c.Stop) error {
	f.accepts.Add(1)
	if f.reject.Load() {
		return infrapriv.ErrPermission
	}
	return nil
}
func (*rootFiles) Reply(c.HostRequest, c.HostReceipt, c.Peer, c.Deadline, c.Stop) ([]*os.File, error) {
	return nil, c.ErrIncomplete
}

type rootFixture struct {
	deps                                   Dependencies
	root                                   *Root
	key                                    ed25519.PrivateKey
	peer                                   c.Peer
	source                                 *rootSource
	facts                                  *rootFacts
	records                                *rootRecords
	state                                  *rejectingState
	files                                  *rootFiles
	launchCalls, policyCalls, storageCalls atomic.Int32
}

func rootDeadline(t *testing.T) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func digest(s string) c.Digest { return sha256.Sum256([]byte(s)) }
func privateDirectory(t *testing.T) *os.File {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}
func knownProcess(boot string) c.Process {
	return c.Process{BootID: boot, PID: 2147483000, StartTicks: 99,
		InvocationID: strings.Repeat("a", 32), ExeDevice: 1, ExeInode: 2,
		ExeSHA256: digest("before executable"), Cgroup: "/tether-infra-hermetic-absent"}
}
func fixturePolicy() (infrapriv.Policy, ed25519.PrivateKey) {
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{31}, ed25519.SeedSize))
	uid, gid := uint32(os.Geteuid()), uint32(os.Getegid())
	// The frozen policy requires a nonroot service identity even for tests.
	if uid == 0 {
		uid = 1000
	}
	if gid == 0 {
		gid = 1000
	}
	p := infrapriv.Policy{Version: 1, HostBootID: "root-test-boot", Domain: c.ID{2}, Revision: 1,
		AccountPublicKey: key.Public().(ed25519.PublicKey), TrustSHA256: digest("trust"), ServiceUID: uid, ServiceGID: gid,
		Keys: []infrapriv.PolicyKey{{KeyID: "account", PublicKey: key.Public().(ed25519.PublicKey), UID: uid, GID: gid, Scope: infrapriv.AccountAuthority}}}
	return p, key
}
func unitPolicy() infrahost.UnitPolicy {
	u := infrahost.EffectiveUnit{
		Name: "nats-server.service", LoadState: "loaded", UnitFileState: "enabled", NeedDaemonReload: "no",
		FragmentPath: "/etc/systemd/system/nats-server.service", User: "tether", Group: "tether",
		DynamicUser: "no", Type: "simple", Restart: "always", RestartUSec: "2s", RuntimeMaxUSec: "infinity",
		NoNewPrivileges: "yes", KillMode: "control-group",
		Exec: [7]string{2: "{ path=/usr/local/libexec/tether-infra-launch ; argv[]=/usr/local/libexec/tether-infra-launch --component nats ; flags= }"},
	}
	return infrahost.UnitPolicy{Target: infrahost.UnitNATS, Expected: u}
}

func newRootFixture(t *testing.T) *rootFixture {
	t.Helper()
	p, key := fixturePolicy()
	journalDirectory := privateDirectory(t)
	path := journalDirectory.Name()
	if err := journalDirectory.Close(); err != nil {
		t.Fatal(err)
	}
	if err := infrahost.InitializeJournal(path, p.HostBootID, p.Domain, infrahost.HostState{}, rootDeadline(t)); err != nil {
		t.Fatal(err)
	}
	data := []byte("immutable root-owned candidate bytes")
	candidate := filepath.Join(t.TempDir(), "candidate")
	if err := os.WriteFile(candidate, data, 0600); err != nil {
		t.Fatal(err)
	}
	f := &rootFixture{key: key, source: &rootSource{path: candidate}, facts: &rootFacts{}, records: &rootRecords{}, state: &rejectingState{}, files: &rootFiles{}}
	f.peer = c.Peer{PID: int32(os.Getpid()), UID: p.ServiceUID, GID: p.ServiceGID, BootID: p.HostBootID, StartTicks: 1}
	f.deps = Dependencies{Policy: p, JournalPath: path,
		Scope: infrahost.CensusScope{Domain: p.Domain, Generation: 1, Before: []c.Process{knownProcess(p.HostBootID)},
			Cgroups: []string{"/tether-infra-hermetic-absent"}, Addresses: []string{"127.0.0.1:443"}},
		Source: f.source, Facts: f.facts, Manager: rejectingUnits{}, UnitState: f.state, UnitPolicies: []infrahost.UnitPolicy{unitPolicy()},
		Ingress: rejectingIngress{}, Changes: f.records, Evidence: rejectingEvidence{},
		Storage: infrahost.ACMEConfig{Root: privateDirectory(t), Initial: privateDirectory(t), UID: os.Geteuid(), GID: os.Getegid(),
			Exit: func(c.Process, c.Deadline, c.Stop) (c.Census, error) { return c.Census{}, c.ErrIncomplete }},
		StorageRevision: func(c.HostRequest, c.Deadline) (uint64, error) { f.storageCalls.Add(1); return 0, c.ErrIncomplete },
		ResolvePolicy: func(c.Digest, c.Deadline, c.Stop) (infrapriv.Policy, error) {
			f.policyCalls.Add(1)
			return infrapriv.Policy{}, c.ErrIncomplete
		},
		PersistPolicy: func(infrapriv.Policy, infrapriv.Policy, c.Deadline, c.Stop) error { return c.ErrIncomplete }, Files: f.files,
		Launch: infralaunch.Bindings{
			Authorize: func(c.HostRequest, c.Deadline, c.Stop) (infralaunch.Selection, error) {
				f.launchCalls.Add(1)
				return infralaunch.Selection{}, c.ErrIncomplete
			},
			StoreCensus: func(c.Store, c.Deadline, c.Stop) (c.Census, error) { return c.Census{}, c.ErrIncomplete },
			Process:     func(c.Process, c.Deadline) (c.Process, error) { return c.Process{}, c.ErrIncomplete },
			Inspect:     func(c.ID, c.Deadline) (c.Census, error) { return c.Census{}, c.ErrIncomplete },
		},
	}
	root, err := New(f.deps)
	if err != nil {
		t.Fatal(err)
	}
	f.root = root
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	return f
}

func (f *rootFixture) request(t *testing.T, op c.HostOp) c.HostRequest {
	t.Helper()
	_, head, err := f.root.journal.Read(rootDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	var desc c.HostOpDescriptor
	for _, row := range c.HostOpDescriptors() {
		if row.Op == op {
			desc = row
		}
	}
	r := c.HostRequest{Version: 1, Op: op, Component: desc.Components[0], Tx: c.ID{1}, Step: 1,
		Domain: f.deps.Policy.Domain, Sequence: 1, PolicyRevision: 1, KeyID: "account", HostBootID: f.deps.Policy.HostBootID,
		Nonce: c.ID{1}, RemainingNS: uint64(time.Second)}
	if op == c.ReleaseStart {
		r.Component = c.Caddy
	}
	kind := c.CaddyBinary
	if r.Component == c.NATS {
		kind = c.NATSBinary
	}
	if r.Component == c.HostSupport {
		kind = c.HelperBinary
	}
	data, err := os.ReadFile(f.source.path)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]any{
		"Expected": knownProcess(r.HostBootID), "ExpectedJournal": head,
		"Candidate": c.Artifact{Kind: kind, Version: "candidate", GOOS: "linux", GOARCH: "amd64",
			ArchiveSHA256: digest("archive"), BinarySHA256: sha256.Sum256(data), CatalogSHA256: digest("catalog")},
		"Config": digest("config"), "Payload": digest("payload"), "BeforeCensus": digest("before"),
		"WarmCensus": digest("warm"), "ReturnProof": digest("return"),
	}
	for _, name := range desc.Required {
		reflect.ValueOf(&r).Elem().FieldByName(name).Set(reflect.ValueOf(values[name]))
	}
	encoded, err := c.SigningBytes(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Signature = ed25519.Sign(f.key, encoded)
	return r
}

// origin: hot-update S9 plan §10.3 / T91 / M36
func TestInfraRootBindsEveryHostOperation(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		if _, err := New(Dependencies{}); !errors.Is(err, c.ErrUnsupported) {
			t.Fatal(err)
		}
		return
	}
	expected := []string{"", "*infrahost.identityBackend", "*infrahost.Backend", "*infraroot.storageUnits", "*infrahost.Backend",
		"*infrahost.identityBackend", "*infrahost.identityBackend", "*infrahost.identityBackend", "*infraroot.storageUnits",
		"*ingressswitch.Switch", "*infrahost.ACMEStorage", "*infraroot.storageUnits", "*infraroot.storageUnits",
		"*infrahost.Backend", "*infrapriv.PolicyBackend", "*infrahost.Backend", "*infralaunch.Backend", "*infralaunch.Backend", "*infraroot.ingressReader"}
	for op := c.Inspect; op <= c.VerifyIngress; op++ {
		t.Run(fmt.Sprint(op), func(t *testing.T) {
			f := newRootFixture(t)
			if f.root.bindings[0] != nil {
				t.Fatal("operation zero has a handler")
			}
			h, ok := f.root.bindings[op].(*operation)
			if !ok || h.op != op || reflect.TypeOf(h.backend).String() != expected[op] || h.HostJournal() != f.root.journal {
				t.Fatalf("operation %d has a missing, wrong, or unjournaled backend", op)
			}
			// Reaching a real leaf's own refusal is required in addition to the
			// runtime type check. No dangerous host action is permitted here.
			r := f.request(t, op)
			_, err := h.Do(r, rootDeadline(t), nil)
			if errors.Is(err, c.ErrContractStub) {
				t.Fatalf("operation %d reached a stub", op)
			}
			if op == c.Stage {
				if err != nil || f.source.calls.Load() != 1 {
					t.Fatalf("real stage did not copy the slot: %v", err)
				}
			} else if err == nil {
				t.Fatalf("operation %d accepted incomplete physical evidence", op)
			}
			switch op {
			case c.FenceStart, c.StartWarm, c.RetireWarm:
				if f.state.calls.Load() != 1 {
					t.Fatal("unit backend was not reached")
				}
			case c.ReleaseStart:
				if f.state.calls.Load() != 1 || f.storageCalls.Load() != 0 {
					t.Fatal("ACME release ran before validating the closed unit permit")
				}
			case c.SwitchIngress:
				if f.records.calls.Load() != 1 {
					t.Fatal("ingress backend was not reached")
				}
			case c.RotatePolicy:
				if f.policyCalls.Load() != 1 {
					t.Fatal("policy backend was not reached")
				}
			case c.LaunchClaim, c.LaunchAck:
				if f.launchCalls.Load() != 1 {
					t.Fatal("launch backend was not reached")
				}
			}
			other := r
			other.Op = c.Inspect
			if op == c.Inspect {
				other.Op = c.Stage
			}
			if _, err := h.Do(other, rootDeadline(t), nil); !errors.Is(err, c.ErrUnsupported) {
				t.Fatal("wrong handler accepted a foreign operation", err)
			}
		})
	}
	// All effects, including physical signals and launch handshakes, must first
	// resolve their proof operands at the root boundary; type presence alone
	// cannot authorize a signed but unresolved census or configuration digest.
	f := newRootFixture(t)
	f.facts.deny.Store(true)
	for op := c.Inspect; op <= c.VerifyIngress; op++ {
		if _, err := f.root.bindings[op].Do(f.request(t, op), rootDeadline(t), nil); !errors.Is(err, c.ErrIncomplete) {
			t.Fatal("unresolved proof reached a backend", op, err)
		}
	}
	// A valid return digest cannot turn an absent incarnation into permission
	// to signal a newly reused unit or PID. This PID is outside Linux's PID range.
	if receipt, err := retireWarm(f.request(t, c.RetireWarm), rootDeadline(t), nil); err == nil || receipt.Applied {
		t.Fatal("warm retirement accepted an unpinned incarnation", err)
	}
}

// origin: hot-update S9 plan §2.1 / T91 / M36
func TestInfraRootRejectsIncompleteComposition(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		if _, err := New(Dependencies{}); !errors.Is(err, c.ErrUnsupported) {
			t.Fatal(err)
		}
		return
	}
	f := newRootFixture(t)
	for _, field := range []string{"JournalPath", "Source", "Facts", "Manager", "UnitState", "UnitPolicies", "Ingress", "Changes", "Evidence", "StorageRevision", "ResolvePolicy", "PersistPolicy", "Files"} {
		t.Run(field, func(t *testing.T) {
			d := f.deps
			v := reflect.ValueOf(&d).Elem().FieldByName(field)
			v.Set(reflect.Zero(v.Type()))
			if root, err := New(d); root != nil || !errors.Is(err, c.ErrIncomplete) {
				t.Fatalf("missing %s admitted: %v", field, err)
			}
		})
	}
	// Typed nil is as absent as nil, including interfaces whose methods would
	// otherwise panic only after durable journal intent had been published.
	d := f.deps
	d.Source = (*rootSource)(nil)
	if root, err := New(d); root != nil || !errors.Is(err, c.ErrIncomplete) {
		t.Fatal("typed nil admitted", err)
	}
	before := testharness.FDCount()
	for round := 0; round < 5; round++ {
		d := f.deps
		d.Launch.Authorize = nil
		if root, err := New(d); root != nil || !errors.Is(err, c.ErrIncomplete) {
			t.Fatal("missing launch binding admitted", err)
		}
	}
	if after := testharness.FDCount(); after != before {
		t.Fatal("partial construction leaked owned descriptors", before, after)
	}
}

// origin: hot-update S9 plan §6.1 / §12 P40 lifecycle
func TestInfraRootLifecycle(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		if _, err := New(Dependencies{}); !errors.Is(err, c.ErrUnsupported) {
			t.Fatal(err)
		}
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRoot(t)
	}
	testharness.AssertNoGoroutineLeak(t, "root worker and admission custody", beforeG)
	testharness.AssertNoFDLeak(t, "root journal storage and launch custody", beforeFD, 0)
}

func exerciseRoot(t *testing.T) {
	t.Helper()
	f := newRootFixture(t)
	defer func() { _ = f.deps.Storage.Root.Close(); _ = f.deps.Storage.Initial.Close() }()
	r := f.request(t, c.Stage)
	if _, err := f.root.host.Do(r, rootDeadline(t), nil); !errors.Is(err, infrahost.ErrPermission) {
		t.Fatal("signature bypassed socket admission", err)
	}
	files := &requestFiles{root: f.root}
	if err := files.Accept(r, f.peer, nil, rootDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	defer files.close()
	receipt, err := f.root.host.Do(r, rootDeadline(t), nil)
	if err != nil || !receipt.Applied || receipt.Journal == (c.Digest{}) || receipt.Revision < 3 || f.source.calls.Load() != 1 {
		t.Fatalf("real copy was not durably journaled: %+v %v", receipt, err)
	}
	copyReceipt, err := f.root.host.Do(r, rootDeadline(t), nil)
	if err != nil || !reflect.DeepEqual(receipt, copyReceipt) || f.source.calls.Load() != 1 {
		t.Fatal("receipt replay repeated the copy", err)
	}
	files.close()
	if _, err := f.root.host.Do(r, rootDeadline(t), nil); !errors.Is(err, infrahost.ErrPermission) {
		t.Fatal("connection admission survived teardown", err)
	}
	f.files.reject.Store(true)
	rejected := &requestFiles{root: f.root}
	if err := rejected.Accept(r, f.peer, nil, rootDeadline(t), nil); !errors.Is(err, infrapriv.ErrPermission) {
		t.Fatal(err)
	}
	rejected.close()
	if len(f.root.authority.peers) != 0 {
		t.Fatal("refused input kept authority custody")
	}
	// Even failure before P03 worker admission owns and closes its accepted FD.
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(t.TempDir(), "socket"), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	client, err := net.DialUnix("unix", nil, listener.Addr().(*net.UnixAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	accepted, err := listener.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.root.ServeConn(accepted, nil, rootDeadline(t), nil); err == nil {
		t.Fatal("missing shared limiter admitted a worker")
	}
	if _, err := accepted.Write([]byte("closed")); err == nil {
		t.Fatal("rejected accepted FD remained open")
	}
	if err := f.root.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.root.Close(); err != nil {
		t.Fatal("root close was not idempotent", err)
	}
	if _, err := f.root.host.Do(r, rootDeadline(t), nil); !errors.Is(err, infrahost.ErrStopped) {
		t.Fatal("closed worker accepted a request", err)
	}
	// Constructor inputs are borrowed, not silently closed by Root.Close.
	if _, err := f.deps.Storage.Root.Stat(); err != nil {
		t.Fatal("root closed caller directory", err)
	}
}

type waitingUnits struct {
	rejectingUnits
	start func(c.Deadline) error
}

func (m waitingUnits) Read(target infrahost.UnitTarget, _ c.Deadline, _ c.Stop) (infrahost.EffectiveUnit, error) {
	if target != infrahost.UnitNATS {
		return infrahost.EffectiveUnit{}, c.ErrIncomplete
	}
	return unitPolicy().Expected, nil
}
func (m waitingUnits) Start(_ infrahost.UnitTarget, d c.Deadline, _ c.Stop) error { return m.start(d) }

type releaseState struct {
	fence infrahost.StartupFence
	root  *Root
}

func (s *releaseState) Load(c.Component, c.Deadline) (infrahost.StartupFence, error) {
	return s.fence, nil
}
func (s *releaseState) CompareAndSwap(before, after infrahost.StartupFence, r c.HostRequest, d c.Deadline) (c.HostReceipt, error) {
	if !reflect.DeepEqual(before, s.fence) || after.Closed {
		return c.HostReceipt{}, c.ErrIncomplete
	}
	_, head, err := s.root.journal.Read(d)
	if err != nil {
		return c.HostReceipt{}, err
	}
	data, err := c.EncodeRequest(r)
	if err != nil {
		return c.HostReceipt{}, err
	}
	s.fence = after
	return c.HostReceipt{Tx: r.Tx, Sequence: r.Sequence, Op: r.Op, Applied: true, Revision: after.Revision, Journal: head, RequestDigest: sha256.Sum256(data)}, nil
}

// The manager actually waits for another worker to finish a journaled claim;
// an instantaneous fake Start would never exercise the circular-wait seam.
// The claim deliberately refuses missing stock provenance, and cannot launch a
// process. Real stock exec/ACK and privileged systemd remain P28/P31 receipts.
// origin: hot-update S9 plan §6.1 / T91 / M36
func TestInfraRootLaunchHandshakeDoesNotHoldJournalLock(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		if _, err := New(Dependencies{}); !errors.Is(err, c.ErrUnsupported) {
			t.Fatal(err)
		}
		return
	}
	f := newRootFixture(t)
	defer func() { _ = f.deps.Storage.Root.Close(); _ = f.deps.Storage.Initial.Close() }()
	r := f.request(t, c.ReleaseStart)
	r.Component, r.Candidate.Kind = c.NATS, c.NATSBinary
	if err := f.root.Close(); err != nil {
		t.Fatal(err)
	}
	directory := privateDirectory(t)
	f.deps.JournalPath = directory.Name()
	if err := directory.Close(); err != nil {
		t.Fatal(err)
	}
	initial := infrahost.HostState{StartupFence: true}
	initial.Slots[c.NATSBinary].Active = r.Candidate
	if err := infrahost.InitializeJournal(f.deps.JournalPath, r.HostBootID, r.Domain, initial, rootDeadline(t)); err != nil {
		t.Fatal(err)
	}
	launchKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{32}, ed25519.SeedSize))
	f.deps.Policy.Keys = append(f.deps.Policy.Keys, infrapriv.PolicyKey{KeyID: "launch", Scope: infrapriv.LaunchAuthority,
		PublicKey: launchKey.Public().(ed25519.PublicKey), UID: f.peer.UID, GID: f.peer.GID})
	state := &releaseState{fence: infrahost.StartupFence{Tx: r.Tx, Domain: r.Domain, Step: r.Step, Component: c.NATS,
		Revision: 1, Closed: true, OldExited: true, StorageFinalized: r.Payload, Deadline: rootDeadline(t)}}
	f.deps.UnitState = state
	joined := make(chan error, 1)
	completed := atomic.Bool{}
	f.deps.Manager = waitingUnits{start: func(d c.Deadline) error {
		claim := r
		claim.Op, claim.KeyID, claim.Sequence, claim.Nonce = c.LaunchClaim, "launch", 2, c.ID{2}
		_, claim.ExpectedJournal, _ = f.root.journal.Read(d)
		claim.Signature = nil
		data, err := c.SigningBytes(claim)
		if err != nil {
			return err
		}
		claim.Signature = ed25519.Sign(launchKey, data)
		admission := &requestFiles{root: f.root}
		if err := admission.Accept(claim, f.peer, nil, d, nil); err != nil {
			return err
		}
		go func() {
			defer admission.close()
			_, err := f.root.host.Do(claim, d, nil)
			joined <- err
		}()
		left, err := controlbudget.Remaining(d)
		if err != nil {
			return err
		}
		timer := time.NewTimer(left)
		defer timer.Stop()
		select {
		case err := <-joined:
			completed.Store(true)
			if err == nil {
				return errors.New("unexpected stock launch acceptance")
			}
			return c.ErrIncomplete
		case <-timer.C:
			return c.ErrIncomplete
		}
	}}
	root, err := New(f.deps)
	if err != nil {
		t.Fatal(err)
	}
	f.root, state.root = root, root
	defer func() { _ = root.Close() }()
	_, r.ExpectedJournal, err = root.journal.Read(rootDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	r.Signature = nil
	data, err := c.SigningBytes(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Signature = ed25519.Sign(f.key, data)
	admission := &requestFiles{root: root}
	if err := admission.Accept(r, f.peer, nil, rootDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	defer admission.close()
	if _, err := root.host.Do(r, rootDeadline(t), nil); err == nil {
		t.Fatal("refused start was reported successful")
	}
	if !completed.Load() || f.launchCalls.Load() != 1 {
		t.Fatal("start waited on a claim blocked by the journal/dispatch lock")
	}
}
