package upgradestore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func newFixture(t *testing.T) (*Store, upgradetx.PrepareRequest) {
	t.Helper()
	key := upgradetx.Key{DomainID: "installation-domain", Component: upgradetx.ComponentAgent, SID: "session", ConfiguredNID: "node", ConfigPath: "/isolated/agent.yaml", UID: 1001}
	owner := upgradetx.Owner{BootID: "boot", PID: 1234, StartTicks: 5678, InstanceID: "instance"}
	dir := t.TempDir()
	s, err := Open(Options{Path: filepath.Join(dir, "upgrade-state.json"), GuardPath: filepath.Join(dir, key.DomainID+".guard"), Key: key, Owner: owner})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1800000000, 0)
	s.now = func() time.Time { return now }
	req := upgradetx.PrepareRequest{ClientNonce: "client-nonce", ActorFP: "actor", Mode: upgradetx.ModeHot, SourceRelease: "v1", TargetRelease: "v2", SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), CapabilityDigest: strings.Repeat("c", 64), Scope: upgradetx.Scope{ID: strings.Repeat("1", 32), Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}, Expected: []upgradetx.Resource{{Domain: upgradetx.DomainExec, ID: "original-process", Origin: "request", Owner: owner, Generation: 1, State: "running", Custody: "source"}}}}
	return s, req
}

func reopen(t *testing.T, s *Store) *Store {
	t.Helper()
	next, err := Open(s.options)
	if err != nil {
		t.Fatal(err)
	}
	next.now = s.now
	return next
}

func prepare(t *testing.T, s *Store, req upgradetx.PrepareRequest) upgradetx.Grant {
	t.Helper()
	g, err := s.Prepare(s.options.Key, req)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func start(t *testing.T, s *Store, g upgradetx.Grant) upgradetx.Record {
	t.Helper()
	r, err := s.Start(g, upgradetx.Fence{Owner: s.options.Owner, RunningSHA: g.SourceSHA})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var txErr *upgradetx.Error
	if !errors.As(err, &txErr) || txErr.Code != code {
		t.Fatalf("error = %v, want %s", err, code)
	}
}

// origin: hot-update S8 plan §3.1 / §9.1 / M10
func TestStartNeverRecreatesExpiredGrant(t *testing.T) {
	for _, name := range []string{"expired", "unknown", "boot", "pid", "start_ticks", "instance", "gc", "tampered", "accepted"} {
		t.Run(name, func(t *testing.T) {
			s, req := newFixture(t)
			g := prepare(t, s, req)
			original := g
			var accepted upgradetx.Record
			if name == "accepted" {
				accepted = start(t, s, g)
			}
			s = reopen(t, s)
			want := upgradetx.CodeGrantUnknown
			switch name {
			case "expired", "gc", "accepted":
				then := time.Unix(0, g.ExpiresAt)
				s.now = func() time.Time { return then }
				want = upgradetx.CodeGrantExpired
			case "unknown":
				g.Handle.TxID = strings.Repeat("0", 32)
			case "boot":
				s.options.Owner.BootID = "new-boot"
			case "pid":
				s.options.Owner.PID++
			case "start_ticks":
				s.options.Owner.StartTicks++
			case "instance":
				s.options.Owner.InstanceID = "new-instance"
			case "tampered":
				g.ActorFP = "another-actor"
			}
			if name == "gc" {
				req.ClientNonce = "new-nonce"
				prepare(t, s, req) // Runs production GC, not a hand-edited tombstone.
				s = reopen(t, s)
				want = upgradetx.CodeGrantUnknown
			}
			before, err := os.ReadFile(s.options.Path)
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.Start(g, upgradetx.Fence{Owner: s.options.Owner, RunningSHA: g.SourceSHA})
			if name == "accepted" {
				if err != nil || !reflect.DeepEqual(got, accepted) {
					t.Fatalf("accepted retry recreated record: %v", err)
				}
			} else {
				requireCode(t, err, want)
				if got.Version != 0 {
					t.Fatal("refusal returned an execution record")
				}
			}
			after, err := os.ReadFile(s.options.Path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("Start retry changed durable evidence")
			}
			if name == "gc" {
				_, err = s.Load(s.options.Key, original.Handle.TxID)
				requireCode(t, err, upgradetx.CodeGrantUnknown)
			}
		})
	}
}

// origin: hot-update S8 plan §3.1 / target-issued grant bindings
func TestPrepareBindsNonceScopeAndParentDeadline(t *testing.T) {
	s, req := newFixture(t)
	g := prepare(t, s, req)
	if g.ExpiresAt != s.now().Add(upgradebudget.ComponentHandover).UnixNano() || g.Handle.TxID == req.ClientNonce {
		t.Fatal("grant is not target-issued with absolute TTL")
	}
	if g.Handle.Generation <= req.Scope.Expected[0].Generation {
		t.Fatal("grant reused an existing resource generation")
	}
	got := prepare(t, reopen(t, s), req)
	if got != g {
		t.Fatal("reopened prepare did not deduplicate")
	}
	req.Scope.Domains[0], req.Scope.Domains[1] = req.Scope.Domains[1], req.Scope.Domains[0]
	if prepare(t, s, req) != g {
		t.Fatal("scope order changed identity")
	}
	for _, change := range []func(*upgradetx.PrepareRequest){
		func(r *upgradetx.PrepareRequest) { r.ActorFP = "foreign" },
		func(r *upgradetx.PrepareRequest) { r.TargetSHA = r.SourceSHA },
		func(r *upgradetx.PrepareRequest) { r.Mode = upgradetx.ModeLegacyInterrupting },
		func(r *upgradetx.PrepareRequest) { r.CapabilityDigest = r.SourceSHA },
		func(r *upgradetx.PrepareRequest) { r.Scope.ID = "other-scope" },
	} {
		candidate := req
		change(&candidate)
		_, err := s.Prepare(s.options.Key, candidate)
		requireCode(t, err, upgradetx.CodeRejected)
	}
	s, req = newFixture(t)
	req.ParentTxID, req.ParentSlot = strings.Repeat("d", 32), "host/agent"
	_, err := s.Prepare(s.options.Key, req)
	requireCode(t, err, upgradetx.CodeDeferred)
	deadline := s.now().Add(upgradebudget.FullStackRollout).UnixNano()
	held := true
	s.options.ParentReservation = func(parent, slot string) (int64, error) {
		if parent != req.ParentTxID || slot != req.ParentSlot || !held {
			return 0, errors.New("reservation absent")
		}
		return deadline, nil
	}
	g = prepare(t, s, req)
	if g.ExpiresAt != deadline {
		t.Fatal("parent deadline was not inherited")
	}
	held = false
	_, err = s.Start(g, upgradetx.Fence{Owner: s.options.Owner, RunningSHA: g.SourceSHA})
	requireCode(t, err, upgradetx.CodeDeferred)
	held = true
	req.ClientNonce, req.ExpiresAt = "overlong", deadline+1
	_, err = s.Prepare(s.options.Key, req)
	requireCode(t, err, upgradetx.CodeRejected)
}

// origin: hot-update S8 plan §3.1 / corrupt-as-idle mutation
func TestJournalCorruptionNeverMeansIdle(t *testing.T) {
	for _, name := range []string{"truncated", "checksum", "version", "duplicate", "nested_duplicate", "unknown", "case", "trailing", "key", "action", "combination", "metadata", "symlink", "legacy"} {
		t.Run(name, func(t *testing.T) {
			s, req := newFixture(t)
			g := prepare(t, s, req)
			start(t, s, g)
			data, err := os.ReadFile(s.options.Path)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "truncated":
				data = data[:len(data)/2]
			case "checksum":
				data = bytes.Replace(data, []byte("actor"), []byte("xxxxx"), 1)
			case "duplicate":
				data = append([]byte(`{"version":2,`), data[1:]...)
			case "nested_duplicate":
				data = bytes.Replace(data, []byte(`"uid":1001`), []byte(`"uid":1001,"uid":1001`), 1)
			case "unknown":
				data = append([]byte(`{"unrecognized":0,`), data[1:]...)
			case "case":
				data = bytes.Replace(data, []byte(`"version"`), []byte(`"Version"`), 1)
			case "trailing":
				data = append(data, []byte(`{}`)...)
			case "legacy":
				data = []byte(`{"state":"committed","prev_sha":"old","new_sha":"new"}`)
			case "symlink":
				other := s.options.Path + ".other"
				if err := os.Rename(s.options.Path, other); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(other, s.options.Path); err != nil {
					t.Fatal(err)
				}
			default:
				var d diskFile
				if err := json.Unmarshal(data, &d); err != nil {
					t.Fatal(err)
				}
				switch name {
				case "version":
					d.SchemaVersion++
				case "key":
					d.Entries[0].Key.UID++
				case "action":
					d.Entries[0].Current.Actions = []upgradetx.Action{{ID: "unbound", State: upgradetx.ActionDurable}}
				case "combination":
					d.Entries[0].Current.Forward.Phase = upgradetx.PhaseCompleted
				case "metadata":
					d.Entries[0].Metadata = nil
				}
				data = marshalFixture(t, d)
			}
			if name != "symlink" {
				if err := os.WriteFile(s.options.Path, data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			_, err = Open(s.options)
			want := upgradetx.CodeStateCorrupt
			if name == "legacy" {
				want = upgradetx.CodeDeferred
			}
			requireCode(t, err, want)
			_, err = s.Start(g, upgradetx.Fence{Owner: s.options.Owner, RunningSHA: g.SourceSHA})
			requireCode(t, err, want)
		})
	}
}

func marshalFixture(t *testing.T, d diskFile) []byte {
	t.Helper()
	d.Checksum = ""
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	d.Checksum = digestBytes(data)
	data, err = json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// origin: hot-update S8 plan §3.1 / file-sync and directory-sync mutations
func TestJournalSyncFailureNeverPublishesAcceptance(t *testing.T) {
	for _, stage := range []string{"file", "directory"} {
		for _, fault := range []syscall.Errno{syscall.ENOSPC, syscall.EROFS} {
			t.Run(fmt.Sprintf("%s/%s", stage, fault.Error()), func(t *testing.T) {
				s, req := newFixture(t)
				g := prepare(t, s, req)
				before, err := os.ReadFile(s.options.Path)
				if err != nil {
					t.Fatal(err)
				}
				hits := 0
				s.beforeSync = func(kind string) error {
					if kind == stage {
						hits++
						return fault
					}
					return nil
				}
				out, err := s.Start(g, upgradetx.Fence{Owner: s.options.Owner, RunningSHA: g.SourceSHA})
				requireCode(t, err, upgradetx.CodeUnsettled)
				if hits != 1 || out.Version != 0 {
					t.Fatal("failed durability published acceptance")
				}
				s = reopen(t, s)
				after, err := os.ReadFile(s.options.Path)
				if err != nil {
					t.Fatal("journal was deleted after failure")
				}
				if stage == "file" {
					if !bytes.Equal(before, after) {
						t.Fatal("failed file sync replaced prior journal")
					}
					_, err := s.Load(s.options.Key, g.Handle.TxID)
					requireCode(t, err, upgradetx.CodeGrantUnknown)
				} else {
					view, err := s.Load(s.options.Key, g.Handle.TxID)
					if err != nil || view.Historical.Grant != g || view.Observation.OwnerKnown {
						t.Fatal("post-rename recovery evidence disappeared or became proof")
					}
				}
				entries, err := os.ReadDir(filepath.Dir(s.options.Path))
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if strings.HasPrefix(entry.Name(), ".upgrade-state-") {
						t.Fatal("temporary descriptor path leaked")
					}
				}
			})
		}
	}
}

// origin: hot-update S8 plan §3.1 / LQ2 retention and pending protection
func TestJournalHistoryRetainsCountAgeAndPending(t *testing.T) {
	s, req := newFixture(t)
	g := prepare(t, s, req)
	r := start(t, s, g)
	err := s.locked(func(d *diskFile, e *diskEntry) error {
		for i := 0; i < retainedHistory+4; i++ {
			archive := r
			archive.Grant.Handle.TxID = fmt.Sprintf("%032x", i+1)
			archive.Forward.Handle = archive.Grant.Handle
			archive.Forward.Phase, archive.Forward.Outcome = upgradetx.PhaseRejected, upgradetx.OutcomeRejected
			m := upgradetx.Metadata{TxID: archive.Grant.Handle.TxID, Window: upgradetx.WindowClosed, ArchivedAt: s.now().Add(-historyAge - time.Hour).UnixNano()}
			if i == 0 {
				archive.Forward.Phase, archive.Forward.Outcome = upgradetx.PhaseRecoveryRequired, upgradetx.OutcomeUnknown
			}
			if i == 1 {
				m.ArchivedAt = s.now().UnixNano()
			}
			if i == 2 {
				m.Pins = []string{g.SourceSHA}
			}
			e.Archives = append(e.Archives, archive)
			e.Metadata = append(e.Metadata, m)
		}
		collect(e, s.now().UnixNano())
		return s.write(d, true)
	})
	if err != nil {
		t.Fatal(err)
	}
	s = reopen(t, s)
	for _, i := range []int{1, 2, 3, 5, retainedHistory + 4} {
		if _, err := s.Load(s.options.Key, fmt.Sprintf("%032x", i)); err != nil {
			t.Fatalf("protected history %d: %v", i, err)
		}
	}
	_, err = s.Load(s.options.Key, fmt.Sprintf("%032x", 4))
	requireCode(t, err, upgradetx.CodeGrantUnknown)
	if _, err := s.Load(s.options.Key, g.Handle.TxID); err != nil {
		t.Fatal("GC removed pending current")
	}
	old := r.Grant
	old.Handle.TxID = fmt.Sprintf("%032x", 4)
	_, err = s.Start(old, upgradetx.Fence{Owner: s.options.Owner, RunningSHA: g.SourceSHA})
	requireCode(t, err, upgradetx.CodeGrantUnknown)
}

// origin: hot-update S8 plan §3 / §11.2 P02 / file FD five-round anchor
func TestJournalConcurrentWritersReleaseDescriptors(t *testing.T) {
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		exerciseConcurrentJournal(t)
	}
	testharness.AssertNoFDLeak(t, "journal guards and sync failures", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "journal writers", beforeG)
}

func exerciseConcurrentJournal(t *testing.T) {
	t.Helper()
	s, req := newFixture(t)
	const writers = 8
	stores := make([]*Store, writers)
	for i := range stores {
		stores[i] = reopen(t, s)
	}
	grants := make(chan upgradetx.Grant, writers)
	errors := make(chan error, writers)
	var wg sync.WaitGroup
	for _, writer := range stores {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g, err := writer.Prepare(writer.options.Key, req)
			if err != nil {
				errors <- err
			} else {
				grants <- g
			}
		}()
	}
	wg.Wait()
	close(grants)
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	var expected upgradetx.Grant
	for g := range grants {
		if expected.Handle.TxID == "" {
			expected = g
		}
		if g != expected {
			t.Fatal("concurrent prepare issued multiple execution IDs")
		}
	}
	r := start(t, s, expected)
	s.beforeSync = func(string) error { return syscall.ENOSPC }
	a := actionFixture(r, upgradetx.ActionPublish)
	_, err := s.Apply(attempt(r).Handle, upgradetx.Event{Action: a})
	requireCode(t, err, upgradetx.CodeUnsettled)
	view, err := reopen(t, s).Load(s.options.Key, expected.Handle.TxID)
	if err != nil || !reflect.DeepEqual(view.Historical, r) {
		t.Fatal("failed concurrent write destroyed accepted record")
	}
}

// origin: hot-update S8 plan §3.1 / §5.1 per-component namespace
func TestSharedJournalKeepsComponentKeysSeparate(t *testing.T) {
	s, req := newFixture(t)
	s.options.Path = filepath.Join(filepath.Dir(s.options.Path), ".tether-upgrade.json")
	g := prepare(t, s, req)
	first := start(t, s, g)
	options := s.options
	options.Key.ConfiguredNID, options.Key.SID, options.Key.UID = "sibling", "other-session", 2002
	sibling, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	sibling.now = s.now
	req.Scope.Participants = []upgradetx.Key{options.Key}
	other := start(t, sibling, prepare(t, sibling, req))
	if other.Key == first.Key {
		t.Fatal("sibling did not receive its own current")
	}
	view, err := reopen(t, s).Load(s.options.Key, g.Handle.TxID)
	if err != nil || !reflect.DeepEqual(view.Historical, first) {
		t.Fatal("sibling overwrote current")
	}
	_, err = sibling.Load(options.Key, first.Grant.Handle.TxID)
	requireCode(t, err, upgradetx.CodeGrantUnknown)
}

// origin: hot-update S8 plan §2.1 / §3.1 / scope must be explicit and sealed
func TestPrepareRejectsIncompleteScopeAndIdentity(t *testing.T) {
	for _, change := range []func(*upgradetx.PrepareRequest){
		func(r *upgradetx.PrepareRequest) { r.Mode = "" },
		func(r *upgradetx.PrepareRequest) { r.SourceSHA = "unknown" },
		func(r *upgradetx.PrepareRequest) { r.ClientNonce = "" },
		func(r *upgradetx.PrepareRequest) { r.ParentSlot = "unbound" },
		func(r *upgradetx.PrepareRequest) { r.Scope.Participants = nil },
		func(r *upgradetx.PrepareRequest) { r.Scope.Participants[0].SID = "" },
		func(r *upgradetx.PrepareRequest) { r.Scope.Domains, r.Scope.Expected = nil, nil },
		func(r *upgradetx.PrepareRequest) { r.Scope.Domains = append(r.Scope.Domains, r.Scope.Domains[0]) },
		func(r *upgradetx.PrepareRequest) { r.Scope.Expected[0].Owner.StartTicks = 0 },
		func(r *upgradetx.PrepareRequest) { r.Scope.Expected[0].Domain = "unobserved" },
		func(r *upgradetx.PrepareRequest) { r.Scope.Expected = append(r.Scope.Expected, r.Scope.Expected[0]) },
	} {
		s, req := newFixture(t)
		change(&req)
		_, err := s.Prepare(s.options.Key, req)
		requireCode(t, err, upgradetx.CodeRejected)
		if _, err := os.Stat(s.options.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("invalid prepare wrote a journal")
		}
	}
	s, _ := newFixture(t)
	options := s.options
	options.Key.Component = upgradetx.ComponentCaddy
	_, err := Open(options)
	requireCode(t, err, upgradetx.CodeRejected)
}

// origin: hot-update S8 plan §3.1 / LQ2 never evict pending for space
func TestJournalCapacityDefersWithoutDeletingPending(t *testing.T) {
	s, req := newFixture(t)
	g := prepare(t, s, req)
	r := start(t, s, g)
	if err := s.locked(func(d *diskFile, e *diskEntry) error {
		for i := 0; i < maxGrants; i++ {
			pending := preparedGrant{Grant: g, Scope: r.Forward.Scope}
			pending.Grant.Handle.TxID = fmt.Sprintf("%032x", i+1)
			pending.Grant.ClientNonce = fmt.Sprintf("reserved-%d", i)
			e.Grants = append(e.Grants, pending)
		}
		return s.write(d, true)
	}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.options.Path)
	if err != nil {
		t.Fatal(err)
	}
	req.ClientNonce = "capacity-contender"
	_, err = s.Prepare(s.options.Key, req)
	requireCode(t, err, upgradetx.CodeDeferred)
	after, err := os.ReadFile(s.options.Path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("capacity pressure discarded existing evidence")
	}
	view, err := s.Load(s.options.Key, g.Handle.TxID)
	if err != nil || !reflect.DeepEqual(view.Historical, r) {
		t.Fatal("capacity pressure changed pending execution")
	}
}

// origin: hot-update S8 plan §2.1 / §3.1; S9 plan §8 / T44
func TestJournalRegistersInfrastructureComponents(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInfrastructureJournal(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infrastructure component journal", g)
	testharness.AssertNoFDLeak(t, "infrastructure component journal", fd, 0)
}

func exerciseInfrastructureJournal(t *testing.T) {
	t.Helper()
	for _, component := range []upgradetx.Component{upgradetx.ComponentNATS, upgradetx.ComponentCaddy} {
		s, req := newFixture(t)
		key := s.options.Key
		key.Component, key.SID = component, ""
		s.options.Key = key
		s = reopen(t, s)
		req.Scope.Participants = []upgradetx.Key{key}
		g := prepare(t, s, req)
		if reopenedGrant := prepare(t, reopen(t, s), req); reopenedGrant != g || g.Handle.Key != key {
			t.Fatal("reopen changed the infrastructure grant or its key")
		}
		r := start(t, s, g)
		view, err := reopen(t, s).Load(key, g.Handle.TxID)
		if err != nil || !reflect.DeepEqual(view.Historical, r) {
			t.Fatalf("infrastructure journal failed to reopen: %v", err)
		}
		before, err := os.ReadFile(s.options.Path)
		if err != nil {
			t.Fatal(err)
		}
		for _, foreign := range []upgradetx.Component{upgradetx.ComponentBroker, upgradetx.ComponentNATS, upgradetx.ComponentCaddy, "unknown"} {
			if foreign == component {
				continue
			}
			other := key
			other.Component = foreign
			_, err = s.Prepare(other, req)
			requireCode(t, err, upgradetx.CodeRejected)
			_, err = s.Load(other, g.Handle.TxID)
			requireCode(t, err, upgradetx.CodeRejected)
			stolen := g
			stolen.Handle.Key = other
			_, err = s.Start(stolen, upgradetx.Fence{Owner: s.options.Owner, RunningSHA: g.SourceSHA})
			requireCode(t, err, upgradetx.CodeGrantUnknown)
			options := s.options
			options.Key = other
			_, err = Open(options)
			if foreign == "unknown" {
				requireCode(t, err, upgradetx.CodeRejected)
			} else {
				requireCode(t, err, upgradetx.CodeStateCorrupt)
			}
		}
		after, err := os.ReadFile(s.options.Path)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("foreign component changed the durable journal")
		}
	}
}

// origin: hot-update S8 plan §2.1 / §3.1; S9 plan §8 / T44
func TestJournalRejectsMalformedInfrastructureKeys(t *testing.T) {
	for _, component := range []upgradetx.Component{upgradetx.ComponentNATS, upgradetx.ComponentCaddy} {
		for _, defect := range []string{"unknown", "sid", "node", "long_node", "relative", "unclean", "long_path", "path_control"} {
			t.Run(string(component)+"/"+defect, func(t *testing.T) {
				s, _ := newFixture(t)
				options := s.options
				options.Key.Component, options.Key.SID = component, ""
				switch defect {
				case "unknown":
					options.Key.Component = "unknown"
				case "sid":
					options.Key.SID = "agent-session"
				case "node":
					options.Key.ConfiguredNID = ""
				case "long_node":
					options.Key.ConfiguredNID = strings.Repeat("n", 257)
				case "relative":
					options.Key.ConfigPath = "config"
				case "unclean":
					options.Key.ConfigPath = "/managed/../config"
				case "long_path":
					options.Key.ConfigPath = "/" + strings.Repeat("p", 4096)
				case "path_control":
					options.Key.ConfigPath = "/managed/config\n"
				}
				_, err := Open(options)
				requireCode(t, err, upgradetx.CodeRejected)
				// The same validator also guards scope and disk records. An
				// Open-only restriction cannot protect a reopened journal.
				if validKey(options.Key) {
					t.Fatal("codec accepted an invalid infrastructure key")
				}
			})
		}
	}
}

// origin: hot-update S8 plan §5.1 / guard must have a stable independent inode
func TestJournalGuardCannotAliasJournal(t *testing.T) {
	s, req := newFixture(t)
	prepare(t, s, req)
	if err := os.Remove(s.options.GuardPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(s.options.Path, s.options.GuardPath); err != nil {
		t.Fatal(err)
	}
	_, err := Open(s.options)
	requireCode(t, err, upgradetx.CodeStateCorrupt)
}

// origin: hot-update S8 plan §3.1 / §3.3 / legacy fields never sign v2 completion
func TestLegacyProjectionNeverBecomesCompletion(t *testing.T) {
	s, req := newFixture(t)
	legacy := legacyProjection{State: "committed", PrevSHA: req.SourceSHA, NewSHA: req.TargetSHA, PrevVersion: "v0", NewVersion: "v1", TargetSID: s.options.Key.SID, TargetNID: s.options.Key.ConfiguredNID, UpgradeID: "old-id", BootCount: 2, ResourcePhase: "active"}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.options.Path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	s.options.AllowLegacyMigration = true
	s = reopen(t, s)
	_, err = s.Load(s.options.Key, "old-id")
	requireCode(t, err, upgradetx.CodeGrantUnknown)
	g := prepare(t, s, req)
	start(t, s, g)
	data, err = os.ReadFile(s.options.Path)
	if err != nil {
		t.Fatal(err)
	}
	var d diskFile
	if err := strictJSON(data, &d); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(d.legacyProjection, legacy) || d.Entries[0].Current.Forward.Control.Known {
		t.Fatal("legacy projection was dropped or promoted into current proof")
	}
	legacy.State = "pending"
	data, err = json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.options.Path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Open(s.options)
	requireCode(t, err, upgradetx.CodeDeferred)
}
