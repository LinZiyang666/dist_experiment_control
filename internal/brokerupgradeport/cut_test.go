package brokerupgradeport

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradecompat"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// fixedJournal is a read-only journal holding one record: the runtime adapter
// only ever loads (the engine is the sole writer).
type fixedJournal struct {
	mu   sync.Mutex
	view upgradetx.View
}

func (j *fixedJournal) Load(_ upgradetx.Key, tx string) (upgradetx.View, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.view.Historical.Grant.Handle.TxID == "" || tx != "" && tx != j.view.Historical.Grant.Handle.TxID {
		return upgradetx.View{}, refusal(upgradetx.CodeGrantUnknown, "test transaction unknown")
	}
	return j.view, nil
}

var errReadOnlyJournal = errors.New("test journal is read-only")

func (j *fixedJournal) Prepare(upgradetx.Key, upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	return upgradetx.Grant{}, errReadOnlyJournal
}
func (j *fixedJournal) Start(upgradetx.Grant, upgradetx.Fence) (upgradetx.Record, error) {
	return upgradetx.Record{}, errReadOnlyJournal
}
func (j *fixedJournal) Apply(upgradetx.Handle, upgradetx.Event) (upgradetx.Record, error) {
	return upgradetx.Record{}, errReadOnlyJournal
}
func (j *fixedJournal) PrepareReverse(upgradetx.Fence, upgradetx.ReverseRequest) (upgradetx.Grant, error) {
	return upgradetx.Grant{}, errReadOnlyJournal
}
func (j *fixedJournal) Archive(upgradetx.Handle, upgradetx.DeathProof) (upgradetx.View, error) {
	return upgradetx.View{}, errReadOnlyJournal
}
func (j *fixedJournal) CloseWindow(upgradetx.Handle) (upgradetx.Metadata, error) {
	return upgradetx.Metadata{}, errReadOnlyJournal
}

// activationDispatched recognizes Activate's armed reply.
func activationDispatched(err error) bool {
	var typed *upgradetx.Error
	return errors.As(err, &typed) && typed.Code == upgradetx.CodeUnsettled && typed.Detail == ReasonActivationDispatched && typed.Accepted
}

// fileLease is a reservation over a real file descriptor, so the adjunct's
// lease duplicate is a real OFD the test can inspect.
type fileLease struct {
	file   *os.File
	closed bool
}

func (l *fileLease) FD() uintptr      { return l.file.Fd() }
func (l *fileLease) DomainID() string { return strings.Repeat("d", 32) }
func (l *fileLease) Close() error     { l.closed = true; return l.file.Close() }

// cutFixture is one started broker attempt with published source/target pins,
// a session store with a real creation identity, and an attached Broker port
// whose Arm models the S6 store cut through the four hooks.
type cutFixture struct {
	dir                    string
	key                    upgradetx.Key
	owner                  upgradetx.Owner
	grant                  upgradetx.Grant
	sourcePath, targetPath string
	journal                *fixedJournal
	db                     *sql.DB
	facts                  BrokerFacts
	runtime                *brokerRuntime
	leases                 []*fileLease
	arm                    func(controlbudget.Deadline) error
	control                func(string) error
	// running is the /proc image (the source until a test plays the target);
	// candidate overrides the probed declarations of the staged target.
	running   string
	candidate func(sha string) upgradecompat.Reader
}

func publishPin(t *testing.T, dir, body string) (string, string) {
	t.Helper()
	sum := sha256.Sum256([]byte(body))
	sha := hex.EncodeToString(sum[:])
	path := filepath.Join(dir, sha)
	if err := os.WriteFile(path, []byte(body), 0o500); err != nil {
		t.Fatal(err)
	}
	return path, sha
}

func brokerReaderFor(sha, release string) upgradecompat.Reader {
	return upgradecompat.Reader{SHA256: sha, Release: release, Component: upgradetx.ComponentBroker, Epoch: 1, ReadEpochs: []uint64{1},
		Domains:  []string{"exec-v1", "flow-v1", "broker-image-v1", "transfer-v1", "upgrade-cut-v1"},
		Features: []string{"upgrade-tx-v1", "authority-incarnation-v1", "upgrade-cut-v1"}}
}

var testPINHash = sync.OnceValues(func() (string, error) { return auth.HashPIN("2468") })

func newCutFixture(t *testing.T) *cutFixture {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("broker boot identity requires Linux")
	}
	f := &cutFixture{dir: t.TempDir(), journal: &fixedJournal{}}
	f.key = upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: strings.Repeat("d", 32), ConfigPath: "/etc/tether/broker.yaml"}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		// origin: S10 internal review NO-SKIP-AS-PASS F3
		t.Fatalf("SETUP-RED: NOT_RUN pending S8 P10: boot identity unavailable: %v", err)
	}
	f.owner = upgradetx.Owner{BootID: strings.TrimSpace(string(boot)), PID: os.Getpid(), StartTicks: 7, InstanceID: strings.Repeat("a", 32)}
	var sourceSHA, targetSHA string
	f.sourcePath, sourceSHA = publishPin(t, f.dir, "broker image A")
	f.targetPath, targetSHA = publishPin(t, f.dir, "broker image B")
	f.db = testharness.OpenDB(t)
	hash, err := testPINHash()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(f.db, "lab", "lab", "owner-fp", hash, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	authorities, err := SessionAuthorities(f.db)
	if err != nil || len(authorities) != 1 {
		t.Fatalf("fixture authority map: %+v %v", authorities, err)
	}
	f.facts = BrokerFacts{Store: StoreIdentity{Path: "/var/lib/tether/state.db", Device: 2049, Inode: 77}, Listeners: []string{"admin.listener"},
		Tunnel: true, Transfers: true, Trackers: []TrackerEntry{{SID: "lab", TransferID: "push-1", EntryNonce: "nonce-1"}}, Serving: true}
	handle := upgradetx.Handle{Key: f.key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 9}
	source := upgradetx.Handle{Generation: 1}
	rows, err := censusRows(BrokerFacts{Store: f.facts.Store, Listeners: f.facts.Listeners, Tunnel: true, Trackers: f.facts.Trackers, Authorities: authorities},
		f.owner, source.Generation, "source")
	if err != nil {
		t.Fatal(err)
	}
	scope, err := upgradetx.CanonicalScope(upgradetx.Scope{ID: strings.Repeat("5", 32), Participants: []upgradetx.Key{f.key}, Domains: allDomains(), Expected: rows})
	if err != nil {
		t.Fatal(err)
	}
	scopeDigest, _ := upgradetx.ScopeDigest(scope)
	f.grant = upgradetx.Grant{Handle: handle, Issuer: f.owner, Mode: upgradetx.ModeHot, SourceRelease: "v1", TargetRelease: "v2",
		SourceSHA: sourceSHA, TargetSHA: targetSHA, ScopeDigest: scopeDigest, CapabilityDigest: strings.Repeat("c", 64), ActorFP: "actor"}
	attempt := upgradetx.Attempt{Handle: handle, Direction: upgradetx.DirectionForward, SourceSHA: sourceSHA, TargetSHA: targetSHA, SourceGeneration: 1,
		Phase: upgradetx.PhaseFreezing, CutStage: upgradetx.CutPristine, Outcome: upgradetx.OutcomePending, Scope: scope}
	f.journal.view = upgradetx.View{Historical: upgradetx.Record{Version: upgradetx.RecordVersion, Key: f.key, Revision: handle.Revision, Grant: f.grant,
		Forward: attempt, EffectiveSHA: sourceSHA, RecordedPreviousSHA: sourceSHA},
		Metadata: upgradetx.Metadata{TxID: handle.TxID, Window: upgradetx.WindowOpen, Pins: []string{f.sourcePath, f.targetPath}}}
	f.running = sourceSHA
	host := RuntimeHost{Key: f.key, Journal: f.journal,
		Owner:      func() (upgradetx.Owner, error) { return f.owner, nil },
		RunningSHA: func() (string, error) { return f.running, nil },
		Source:     func(sha string) (upgradecompat.Reader, error) { return brokerReaderFor(sha, "v1"), nil },
		Reader: func(path string) (upgradecompat.Reader, error) {
			sha, err := upgradeartifact.FileSHA(path)
			if f.candidate != nil {
				return f.candidate(sha), err
			}
			return brokerReaderFor(sha, "v2"), err
		},
		Lease: func(_ context.Context, _ upgradetx.Key) (upgradelock.Lease, error) {
			file, err := os.CreateTemp(f.dir, "lease-*")
			if err != nil {
				return nil, err
			}
			lease := &fileLease{file: file}
			f.leases = append(f.leases, lease)
			return lease, nil
		}}
	f.runtime = NewRuntime(host).(*brokerRuntime)
	Attach(f.runtime, BrokerPort{
		Facts: func() (BrokerFacts, error) { return f.facts, nil },
		DB:    func() *sql.DB { return f.db },
		Arm: func(d controlbudget.Deadline) error {
			if f.arm == nil {
				return errors.New("no S6 cut in this fixture")
			}
			return f.arm(d)
		},
		Control: func(n string) error {
			if f.control == nil {
				return errors.New("no control probe in this fixture")
			}
			return f.control(n)
		}})
	return f
}

// prepareCut runs the source half up to the armed activation: preflight,
// freeze (reservation), the final current export.
func (f *cutFixture) prepareCut(t *testing.T) upgradeport.Cut {
	t.Helper()
	if _, err := f.runtime.Preflight(t.Context(), f.grant, upgradeartifact.Receipt{}); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if _, err := f.runtime.Freeze(f.grant.Handle); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	cut, err := f.runtime.ExportCurrent(f.grant.Handle)
	if err != nil {
		t.Fatalf("current export: %v", err)
	}
	return cut
}

// An armed broker cut changes exactly four inputs of the S6 store cut: its
// readers (the immutable target slot and the published source slot), the
// adjunct sealed after the S6 freeze (this transaction, the store's complete
// authority map and a duplicate of the held reservation), the extra resource
// every final Prepare carries, and the exec path. Activate reports the armed
// cut as dispatched only when the S6 cut armed; a refused cut is an accepted
// unknown, and the reload's return releases the reservation exactly once.
// No plan outlives its reload: the next S6 cut sees the defaults again.
// origin: hot-update S8 plan §4.1 / §5.1 / §6.1 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11
func TestBrokerStoreCutCarriesTheSealedAdjunct(t *testing.T) {
	t.Run("armed", func(t *testing.T) {
		f := newCutFixture(t)
		cut := f.prepareCut(t)
		var sealed upgradecut.State
		f.arm = func(controlbudget.Deadline) error {
			candidate, previous, expected := CutReaders(f.runtime, "/canonical/tether", "/canonical/tether.prev")
			if candidate != f.targetPath || previous != f.sourcePath || expected != "" {
				t.Errorf("S6 preflight readers: %s %s %q", candidate, previous, expected)
			}
			if len(CutResources(f.runtime)) != 0 {
				t.Error("an adjunct travelled before the S6 freeze sealed it")
			}
			if err := SealCut(f.runtime, f.db, nil); err != nil {
				t.Errorf("seal after the S6 freeze: %v", err)
			}
			resources := CutResources(f.runtime)
			state, found, err := upgradecut.Find(resources)
			if !found || err != nil || len(resources) != 1 || resources[0].Files[upgradecut.AuxiliaryName(state.LockFD)] == nil {
				t.Errorf("final Prepare carries no sealed adjunct: %+v %v", resources, err)
			}
			sealed = state
			if got := CutPath(f.runtime, "/canonical/tether"); got != f.targetPath {
				t.Errorf("S6 exec path %s", got)
			}
			return nil
		}
		_, err := f.runtime.Activate(cut)
		if !activationDispatched(err) {
			t.Fatalf("an armed cut was not dispatched: %v", err)
		}
		want, _ := SessionAuthorities(f.db)
		if sealed.Attempt != cut.Attempt || sealed.SourceSHA != f.grant.SourceSHA || sealed.TargetSHA != f.grant.TargetSHA ||
			sealed.Owner != f.owner || upgradecut.MatchAuthorities(want, sealed.Authorities) != nil || len(sealed.Scope.Expected) != len(cut.Scope.Expected) {
			t.Fatalf("adjunct does not carry this cut: %+v", sealed)
		}
		if attempt, dispatched := Returned(f.runtime); attempt != cut.Attempt || !dispatched || len(f.leases) != 1 || !f.leases[0].closed {
			t.Fatalf("returned reload: attempt=%+v dispatched=%t, reservation released=%t", attempt, dispatched, f.leases[0].closed)
		}
		if candidate, _, _ := CutReaders(f.runtime, "/canonical/tether", "/canonical/tether.prev"); candidate != "/canonical/tether" ||
			CutPath(f.runtime, "/x") != "/x" || CutResources(f.runtime) != nil {
			t.Fatal("a returned plan reached the next S6 cut")
		}
		if _, dispatched := Returned(f.runtime); dispatched {
			t.Fatal("a second return settled the same cut again")
		}
	})
	t.Run("refused", func(t *testing.T) {
		f := newCutFixture(t)
		cut := f.prepareCut(t)
		f.arm = func(controlbudget.Deadline) error { return errors.New("transfers_in_flight") }
		_, err := f.runtime.Activate(cut)
		var typed *upgradetx.Error
		if activationDispatched(err) || !errors.As(err, &typed) || typed.Code != upgradetx.CodeUnsettled || !typed.Accepted {
			t.Fatalf("a refused S6 cut reported dispatch or nothing accepted: %v", err)
		}
		if _, err := f.runtime.Freeze(f.grant.Handle); err == nil {
			t.Fatal("a second reservation started while the first cut is still armed")
		}
		// The engine journals this refusal itself (Activate's reply); the
		// returned reload only releases the reservation.
		if _, dispatched := Returned(f.runtime); dispatched || !f.leases[0].closed {
			t.Fatalf("refused cut: dispatched=%t, reservation released=%t", dispatched, f.leases[0].closed)
		}
	})
	t.Run("thaw", func(t *testing.T) {
		f := newCutFixture(t)
		_ = f.prepareCut(t)
		if err := f.runtime.Thaw(f.grant.Handle); err != nil || !f.leases[0].closed {
			t.Fatalf("thaw kept the reservation: %v", err)
		}
		if _, err := f.runtime.Activate(upgradeport.Cut{Attempt: f.grant.Handle}); activationDispatched(err) || err == nil {
			t.Fatalf("a thawed cut activated: %v", err)
		}
	})
}
