package infrahost

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
)

type testAuthority struct {
	key  ed25519.PrivateKey
	deny atomic.Bool
}

func (a *testAuthority) Authorize(r infracontract.HostRequest, _ infracontract.Deadline) (infracontract.Peer, error) {
	if a.deny.Load() || r.KeyID != "current" || r.PolicyRevision != 7 || infracontract.VerifyRequest(r, a.key.Public().(ed25519.PublicKey)) != nil {
		return infracontract.Peer{}, ErrPermission
	}
	return infracontract.Peer{PID: int32(os.Getpid()), UID: uint32(os.Geteuid()), BootID: "test-boot", StartTicks: 101}, nil
}

type testSource struct{ path string }

func (s testSource) Open(_ infracontract.Artifact, _ infracontract.Digest, _ infracontract.Deadline) (*os.File, error) {
	return os.Open(s.path)
}

type testHost struct {
	journal  *Journal
	do       func(infracontract.HostRequest, infracontract.Deadline, infracontract.Stop) (infracontract.HostReceipt, error)
	inspect  func(infracontract.ID, infracontract.Deadline) (infracontract.Census, error)
	resolve  func(JournalEvent, infracontract.Deadline, infracontract.Stop) (infracontract.HostReceipt, error)
	validate func(infracontract.HostRequest, HostState, infracontract.Deadline, infracontract.Stop) error
	calls    atomic.Int32
}

func (h *testHost) Validate(r infracontract.HostRequest, s HostState, d infracontract.Deadline, stop infracontract.Stop) error {
	if h.validate != nil {
		return h.validate(r, s, d, stop)
	}
	return nil
}

func (h *testHost) HostJournal() *Journal { return h.journal }
func (h *testHost) Do(r infracontract.HostRequest, d infracontract.Deadline, s infracontract.Stop) (infracontract.HostReceipt, error) {
	h.calls.Add(1)
	if h.do != nil {
		return h.do(r, d, s)
	}
	return infracontract.HostReceipt{Applied: true, Current: r.Expected}, nil
}
func (h *testHost) Inspect(tx infracontract.ID, d infracontract.Deadline) (infracontract.Census, error) {
	if h.inspect != nil {
		return h.inspect(tx, d)
	}
	return infracontract.Census{Complete: true, Generation: 1, Digest: digestOf("census"), Processes: []infracontract.Process{testProcess()}}, nil
}
func (h *testHost) Resolve(e JournalEvent, d infracontract.Deadline, s infracontract.Stop) (infracontract.HostReceipt, error) {
	if h.resolve != nil {
		return h.resolve(e, d, s)
	}
	return infracontract.HostReceipt{}, ErrHalted
}

type hostFixture struct {
	journal   *Journal
	worker    *worker
	backend   *Backend
	authority *testAuthority
	effects   *testHost
	path      string
	artifact  infracontract.Artifact
}

func digestOf(s string) infracontract.Digest { return sha256.Sum256([]byte(s)) }
func testProcess() infracontract.Process {
	return infracontract.Process{BootID: "test-boot", PID: 41, StartTicks: 111, InvocationID: "unit-1", ExeDevice: 2, ExeInode: 3, ExeSHA256: digestOf("old"), Cgroup: "/sandbox/unit"}
}
func testDeadline(t *testing.T) infracontract.Deadline {
	t.Helper()
	d, err := controlbudget.After(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func newHostFixture(t *testing.T) *hostFixture {
	t.Helper()
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("hot root profile requires linux/amd64")
	}
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	authority := &testAuthority{key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{19}, ed25519.SeedSize))}
	initial := HostState{}
	for kind := infracontract.NATSBinary; kind <= infracontract.LauncherBinary; kind++ {
		initial.Slots[kind].Active = infracontract.Artifact{Kind: kind, Version: "previous", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: digestOf("previous archive"), BinarySHA256: digestOf("old"), CatalogSHA256: digestOf("catalog")}
	}
	initial.Slots[infracontract.LauncherBinary].Active.BinarySHA256 = digestOf("old launcher")
	if err := InitializeJournal(path, "test-boot", infracontract.ID{2}, initial, testDeadline(t)); err != nil {
		t.Fatal(err)
	}
	j, err := OpenJournal(path, "test-boot", infracontract.ID{2}, authority)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("sandbox executable contents")
	source := filepath.Join(t.TempDir(), "candidate")
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	a := infracontract.Artifact{Kind: infracontract.CaddyBinary, Version: "2.7.6", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: digestOf("archive"), BinarySHA256: sha256.Sum256(data), CatalogSHA256: digestOf("catalog")}
	effects := &testHost{journal: j}
	b, err := NewBackend(j, testSource{source}, effects)
	if err != nil {
		t.Fatal(err)
	}
	var bindings infracontract.HostBindings
	for op := infracontract.Inspect; op <= infracontract.VerifyIngress; op++ {
		bindings[op] = effects
	}
	for _, op := range []infracontract.HostOp{infracontract.Stage, infracontract.Promote, infracontract.UpdateSupport, infracontract.RecoverHost} {
		bindings[op] = b
	}
	w, err := NewWorker(bindings)
	if err != nil {
		t.Fatal(err)
	}
	f := &hostFixture{j, w.(*worker), b, authority, effects, path, a}
	t.Cleanup(func() { _ = f.worker.Close(); _ = f.journal.Close() })
	return f
}
func (f *hostFixture) request(t *testing.T, op infracontract.HostOp, seq uint64) infracontract.HostRequest {
	t.Helper()
	_, head, err := f.journal.Read(testDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	component := infracontract.Caddy
	if op == infracontract.SignalLDM || op == infracontract.SignalStop {
		component = infracontract.NATS
	}
	if op == infracontract.UpdateSupport || op == infracontract.RotatePolicy {
		component = infracontract.HostSupport
	}
	r := infracontract.HostRequest{Version: 1, Op: op, Component: component, Tx: infracontract.ID{1}, Step: 1, Domain: infracontract.ID{2}, Sequence: seq, PolicyRevision: 7, KeyID: "current", HostBootID: "test-boot", Nonce: infracontract.ID{byte(seq), byte(seq >> 8)}, RemainingNS: uint64(time.Second)}
	a := f.artifact
	if component == infracontract.NATS {
		a.Kind = infracontract.NATSBinary
	}
	if component == infracontract.HostSupport {
		a.Kind = infracontract.HelperBinary
	}
	values := map[string]interface{}{"Expected": testProcess(), "ExpectedJournal": head, "Candidate": a, "Config": digestOf("config"), "BeforeCensus": digestOf("before"), "WarmCensus": digestOf("warm"), "ReturnProof": digestOf("return"), "Payload": digestOf("payload")}
	for _, desc := range infracontract.HostOpDescriptors() {
		if desc.Op != op {
			continue
		}
		for _, name := range desc.Required {
			reflect.ValueOf(&r).Elem().FieldByName(name).Set(reflect.ValueOf(values[name]))
		}
	}
	return f.sign(t, r)
}
func (f *hostFixture) sign(t *testing.T, r infracontract.HostRequest) infracontract.HostRequest {
	t.Helper()
	r.Signature = nil
	b, err := infracontract.SigningBytes(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Signature = ed25519.Sign(f.authority.key, b)
	return r
}
func (f *hostFixture) run(t *testing.T, r infracontract.HostRequest) infracontract.HostReceipt {
	t.Helper()
	receipt, err := f.worker.Do(r, testDeadline(t), nil)
	if err != nil {
		t.Fatalf("op %d: %v", r.Op, err)
	}
	return receipt
}

// origin: hot-update S9 plan §2.2 / §6.1 / T09
// origin: S10 internal review ARCH-REDS R12
func TestInfraHostEffectCAS(t *testing.T) {
	t.Run("local action budget only shrinks", func(t *testing.T) {
		d, err := controlbudget.After(time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		bounded, err := actionDeadline(infracontract.HostRequest{RemainingNS: uint64(time.Hour)}, d)
		if err != nil {
			t.Fatal(err)
		}
		left, err := controlbudget.Remaining(bounded)
		if err != nil || left > hostActionLimit {
			t.Fatal("action budget exceeded root cap", left, err)
		}
		short, err := actionDeadline(infracontract.HostRequest{RemainingNS: uint64(time.Second)}, bounded)
		if err != nil || short > bounded {
			t.Fatal("signed remaining budget extended", err)
		}
		if _, err := actionDeadline(infracontract.HostRequest{RemainingNS: uint64(time.Hour)}, 1); err == nil {
			t.Fatal("expired local deadline renewed")
		}
	})
	t.Run("result persistence keeps original absolute deadline", func(t *testing.T) {
		f := newHostFixture(t)
		r := f.request(t, infracontract.SwitchIngress, 1)
		clock := newJournalClock(t, f.journal)
		d := clock.deadline(30 * time.Millisecond)
		entered := false
		f.effects.do = func(r infracontract.HostRequest, action infracontract.Deadline, stop infracontract.Stop) (infracontract.HostReceipt, error) {
			im, err := f.journal.load()
			if err != nil {
				t.Fatal(err)
			}
			intent := im.Events[len(im.Events)-1]
			if intent.Phase != EffectIntent || intent.Deadline != d || action != d {
				t.Fatal("effect did not enter with the durable original deadline")
			}
			entered = true
			clock.advance(30 * time.Millisecond)
			select {
			case <-stop:
			case <-time.After(3 * time.Second):
				t.Fatal("expired action did not stop at the effect barrier")
			}
			return infracontract.HostReceipt{Applied: true, Current: r.Expected}, nil
		}
		if _, err := f.worker.Do(r, d, nil); !controlbudget.IsExpired(err) {
			t.Fatal("expired worker gained a new commit budget", err)
		}
		if !entered {
			t.Fatal("deadline expired before the intended effect barrier")
		}
		im, err := f.journal.load()
		if err != nil {
			t.Fatal(err)
		}
		last := im.Events[len(im.Events)-1]
		if last.Phase != EffectIntent || last.Deadline != d {
			t.Fatal("late reply changed the original deadline or claimed completion")
		}
	})
	t.Run("complete immutable bindings", func(t *testing.T) {
		f := newHostFixture(t)
		for op := infracontract.Inspect; op <= infracontract.VerifyIngress; op++ {
			b := f.worker.bindings
			b[op] = nil
			if _, err := NewWorker(b); !errors.Is(err, infracontract.ErrContractStub) {
				t.Fatalf("missing op %d accepted: %v", op, err)
			}
		}
		b := f.worker.bindings
		b[0] = f.effects
		if _, err := NewWorker(b); !errors.Is(err, infracontract.ErrContractStub) {
			t.Fatal("zero slot accepted")
		}
		b = f.worker.bindings
		var absent *testHost
		b[infracontract.SignalStop] = absent
		if _, err := NewWorker(b); !errors.Is(err, infracontract.ErrContractStub) {
			t.Fatal("typed nil accepted")
		}
		b = f.worker.bindings
		h, err := NewWorker(b)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = h.(*worker).Close() }() // Close joins and always returns nil.
		b[infracontract.SwitchIngress] = nil
		if _, err := h.Do(f.request(t, infracontract.SwitchIngress, 1), testDeadline(t), nil); err != nil {
			t.Fatal("caller changed copied table", err)
		}
	})
	t.Run("lost reply survives reopen", func(t *testing.T) {
		f := newHostFixture(t)
		r := f.request(t, infracontract.SwitchIngress, 1)
		first := f.run(t, r)
		j, err := OpenJournal(f.path, "test-boot", infracontract.ID{2}, f.authority)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = j.Close() }() // Releases only this reopened journal's directory descriptor.
		b := f.worker.bindings
		adapter := &testHost{journal: j}
		for _, op := range []infracontract.HostOp{infracontract.Stage, infracontract.Promote, infracontract.RecoverHost, infracontract.UpdateSupport} {
			b[op] = adapter
		}
		h, err := NewWorker(b)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = h.(*worker).Close() }() // Close joins and always returns nil.
		got, err := h.Do(r, testDeadline(t), nil)
		if err != nil || !reflect.DeepEqual(first, got) || f.effects.calls.Load() != 1 {
			t.Fatalf("replay changed receipt/effect: %+v %v", got, err)
		}
		changed := r
		changed.Payload = digestOf("late unflip")
		changed = f.sign(t, changed)
		if _, err := h.Do(changed, testDeadline(t), nil); !errors.Is(err, ErrCAS) {
			t.Fatal("sequence reuse accepted", err)
		}
	})
	t.Run("late unflip and nonce cannot pass current CAS", func(t *testing.T) {
		f := newHostFixture(t)
		first := f.request(t, infracontract.SwitchIngress, 1)
		late := f.request(t, infracontract.SwitchIngress, 2)
		f.run(t, first)
		if _, err := f.worker.Do(late, testDeadline(t), nil); !errors.Is(err, ErrCAS) {
			t.Fatal("late unflip dispatched", err)
		}
		fresh := f.request(t, infracontract.SwitchIngress, 2)
		fresh.Nonce = first.Nonce
		fresh = f.sign(t, fresh)
		if _, err := f.worker.Do(fresh, testDeadline(t), nil); !errors.Is(err, ErrCAS) {
			t.Fatal("nonce replay accepted", err)
		}
		if f.effects.calls.Load() != 1 {
			t.Fatal("rejected effect ran")
		}
	})
	t.Run("authority stays current on replay", func(t *testing.T) {
		for _, mutation := range []string{"boot", "domain", "key", "revision", "signature"} {
			t.Run(mutation, func(t *testing.T) {
				f := newHostFixture(t)
				r := f.request(t, infracontract.SwitchIngress, 1)
				switch mutation {
				case "boot":
					r.HostBootID = "other"
					r.Expected.BootID = "other"
				case "domain":
					r.Domain = infracontract.ID{9}
				case "key":
					r.KeyID = "revoked"
				case "revision":
					r.PolicyRevision = 6
				case "signature":
					r.Signature[0] ^= 1
				}
				if mutation != "signature" {
					r = f.sign(t, r)
				}
				if _, err := f.worker.Do(r, testDeadline(t), nil); !errors.Is(err, ErrPermission) {
					t.Fatalf("unauthorized %s: %v", mutation, err)
				}
				if f.effects.calls.Load() != 0 {
					t.Fatal("unauthorized effect")
				}
			})
		}
		f := newHostFixture(t)
		r := f.request(t, infracontract.SwitchIngress, 1)
		f.run(t, r)
		f.authority.deny.Store(true)
		if _, err := f.worker.Do(r, testDeadline(t), nil); !errors.Is(err, ErrPermission) {
			t.Fatal("revoked replay accepted", err)
		}
	})
	t.Run("ambiguous effects are resolved without repeating", func(t *testing.T) {
		f := newHostFixture(t)
		f.effects.do = func(infracontract.HostRequest, infracontract.Deadline, infracontract.Stop) (infracontract.HostReceipt, error) {
			return infracontract.HostReceipt{}, errors.New("SECRET-CANARY-after-effect")
		}
		r := f.request(t, infracontract.SwitchIngress, 1)
		receipt, err := f.worker.Do(r, testDeadline(t), nil)
		if !errors.Is(err, ErrHalted) || receipt.Applied {
			t.Fatal("unknown signed success", err)
		}
		if _, err := f.worker.Do(r, testDeadline(t), nil); !errors.Is(err, ErrHalted) {
			t.Fatal("unknown replay dispatched", err)
		}
		data, err := os.ReadFile(filepath.Join(f.path, "journal.json"))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("SECRET-CANARY")) {
			t.Fatal("backend error persisted")
		}
		f.effects.resolve = func(e JournalEvent, _ infracontract.Deadline, _ infracontract.Stop) (infracontract.HostReceipt, error) {
			return infracontract.HostReceipt{Applied: true, RequestDigest: e.RequestDigest, Current: e.Request.Expected}, nil
		}
		recover := f.request(t, infracontract.RecoverHost, 2)
		recover.Payload = receipt.RequestDigest
		recover = f.sign(t, recover)
		f.run(t, recover)
		if _, err := f.worker.Do(r, testDeadline(t), nil); err != nil {
			t.Fatal("resolved receipt unavailable", err)
		}
		if f.effects.calls.Load() != 1 {
			t.Fatal("recovery repeated original effect")
		}
	})
	t.Run("start reply waits for launcher without journal lock", func(t *testing.T) {
		f := newHostFixture(t)
		entered, claimed := make(chan struct{}), make(chan struct{})
		f.effects.do = func(r infracontract.HostRequest, _ infracontract.Deadline, s infracontract.Stop) (infracontract.HostReceipt, error) {
			if r.Op == infracontract.StartWarm {
				close(entered)
				select {
				case <-claimed:
				case <-s:
					return infracontract.HostReceipt{}, ErrStopped
				}
			}
			return infracontract.HostReceipt{Applied: true, Current: r.Expected}, nil
		}
		start := f.request(t, infracontract.StartWarm, 1)
		done := make(chan error, 1)
		go func() { _, err := f.worker.Do(start, testDeadline(t), nil); done <- err }()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("start never entered")
		}
		claim := f.request(t, infracontract.LaunchClaim, 2)
		_, err := f.worker.Do(claim, testDeadline(t), nil)
		close(claimed)
		startErr := <-done
		if err != nil || startErr != nil {
			t.Fatalf("start/claim dependency deadlocked: %v / %v", err, startErr)
		}
		ack := f.request(t, infracontract.LaunchAck, 3)
		// origin: S9 internal review rv-infra F04 — journal identity remains
		// stable across exec, while a pre-exec Ack must remain a refusal.
		if _, err := f.worker.Do(ack, testDeadline(t), nil); !errors.Is(err, ErrPermission) {
			t.Fatal("pre-exec identity was acknowledged", err)
		}
		ack.Expected.ExeInode++
		ack.Expected.ExeSHA256 = ack.Candidate.BinarySHA256
		ack = f.sign(t, ack)
		f.run(t, ack)
	})
	t.Run("canceled admission has no effect", func(t *testing.T) {
		f := newHostFixture(t)
		stop := make(chan struct{})
		close(stop)
		if _, err := f.worker.Do(f.request(t, infracontract.SwitchIngress, 1), testDeadline(t), stop); !errors.Is(err, ErrStopped) {
			t.Fatal(err)
		}
		if f.effects.calls.Load() != 0 {
			t.Fatal("canceled effect")
		}
	})
	t.Run("every dispatched effect has a durable exact intent", func(t *testing.T) {
		f := newHostFixture(t)
		f.effects.do = func(r infracontract.HostRequest, d infracontract.Deadline, _ infracontract.Stop) (infracontract.HostReceipt, error) {
			im, err := f.journal.load()
			if err != nil {
				return infracontract.HostReceipt{}, err
			}
			last := im.Events[len(im.Events)-1]
			if last.Phase != EffectIntent || !reflect.DeepEqual(last.Request, r) || last.Deadline != d || last.Holder.PID <= 0 {
				t.Error("effect preceded its durable bound intent")
			}
			return infracontract.HostReceipt{Applied: true, Current: r.Expected}, nil
		}
		f.run(t, f.request(t, infracontract.SwitchIngress, 1))
	})
	t.Run("launch permit cannot authorize a different target or second claim", func(t *testing.T) {
		f := newHostFixture(t)
		f.run(t, f.request(t, infracontract.StartWarm, 1))
		for _, change := range []string{"candidate", "config", "payload", "step", "ack before claim"} {
			r := f.request(t, infracontract.LaunchClaim, 2)
			switch change {
			case "candidate":
				r.Candidate.BinarySHA256 = digestOf("foreign target")
			case "config":
				r.Config = digestOf("foreign config")
			case "payload":
				r.Payload = digestOf("foreign permit")
			case "step":
				r.Step++
			case "ack before claim":
				r = f.request(t, infracontract.LaunchAck, 2)
			}
			r = f.sign(t, r)
			if _, err := f.worker.Do(r, testDeadline(t), nil); !errors.Is(err, ErrPermission) {
				t.Fatalf("%s accepted: %v", change, err)
			}
		}
		f.run(t, f.request(t, infracontract.LaunchClaim, 2))
		if _, err := f.worker.Do(f.request(t, infracontract.LaunchClaim, 3), testDeadline(t), nil); !errors.Is(err, ErrPermission) {
			t.Fatal("second claim admitted", err)
		}
		r := f.request(t, infracontract.LaunchAck, 3)
		r.Expected.PID++
		r = f.sign(t, r)
		if _, err := f.worker.Do(r, testDeadline(t), nil); !errors.Is(err, ErrPermission) {
			t.Fatal("foreign launcher ack admitted", err)
		}
	})
	t.Run("domain release needs independent zero-owner evidence", func(t *testing.T) {
		f := newHostFixture(t)
		receipt := f.run(t, f.request(t, infracontract.SwitchIngress, 1))
		f.effects.resolve = func(e JournalEvent, _ infracontract.Deadline, _ infracontract.Stop) (infracontract.HostReceipt, error) {
			return infracontract.HostReceipt{Applied: true, RequestDigest: e.RequestDigest, Census: infracontract.Census{Complete: true, Generation: 3, Digest: digestOf("retirement")}}, nil
		}
		r := f.request(t, infracontract.RecoverHost, 2)
		r.Payload = receipt.RequestDigest
		r.ReturnProof = digestOf("return proof")
		r = f.sign(t, r)
		f.run(t, r)
		s, _, err := f.journal.Read(testDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if s.Tx != (infracontract.ID{}) || s.Retirement != r.ReturnProof {
			t.Fatal("domain did not retire")
		}
		next := f.request(t, infracontract.SwitchIngress, 1)
		next.Tx = infracontract.ID{9}
		next.Nonce = infracontract.ID{9}
		next = f.sign(t, next)
		f.run(t, next)
	})
	t.Run("unknown result text never becomes protocol data", func(t *testing.T) {
		f := newHostFixture(t)
		f.effects.do = func(infracontract.HostRequest, infracontract.Deadline, infracontract.Stop) (infracontract.HostReceipt, error) {
			return infracontract.HostReceipt{Applied: true, Code: "SECRET-CANARY-result"}, nil
		}
		if _, err := f.worker.Do(f.request(t, infracontract.SwitchIngress, 1), testDeadline(t), nil); !errors.Is(err, ErrHalted) {
			t.Fatal("unknown code accepted", err)
		}
		data, err := os.ReadFile(filepath.Join(f.path, "journal.json"))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("SECRET-CANARY")) {
			t.Fatal("result text leaked")
		}
	})
	t.Run("signed digests still require root verified records", func(t *testing.T) {
		for _, op := range []infracontract.HostOp{infracontract.Stage, infracontract.Promote, infracontract.UpdateSupport} {
			t.Run(string(rune('0'+op)), func(t *testing.T) {
				f := newHostFixture(t)
				seq := uint64(1)
				if op != infracontract.Stage {
					stage := f.request(t, infracontract.Stage, seq)
					seq++
					if op == infracontract.UpdateSupport {
						stage.Component = infracontract.HostSupport
						stage.Candidate.Kind = infracontract.HelperBinary
						stage = f.sign(t, stage)
					}
					f.run(t, stage)
					if op == infracontract.Promote {
						f.run(t, f.request(t, infracontract.FenceStart, seq))
						seq++
					}
				}
				f.effects.validate = func(infracontract.HostRequest, HostState, infracontract.Deadline, infracontract.Stop) error {
					return ErrPermission
				}
				if _, err := f.worker.Do(f.request(t, op, seq), testDeadline(t), nil); !errors.Is(err, ErrPermission) {
					t.Fatal("signed but unresolved root record accepted", err)
				}
				s, _, err := f.journal.Read(testDeadline(t))
				if err != nil {
					t.Fatal(err)
				}
				if s.Slots[infracontract.CaddyBinary].Active.BinarySHA256 != digestOf("old") || s.Slots[infracontract.HelperBinary].Active.BinarySHA256 != digestOf("old") {
					t.Fatal("unvalidated selector applied")
				}
			})
		}
	})
}

// origin: hot-update S9 plan §6.1 / §9.5 / P04 lifecycle
func TestInfraHostLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseHost(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra host lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "infra host lifecycle", beforeFD, 0)
}

func exerciseHost(t *testing.T) {
	t.Helper()
	f := newHostFixture(t)
	entered, exited := make(chan struct{}), make(chan struct{})
	f.effects.do = func(_ infracontract.HostRequest, _ infracontract.Deadline, s infracontract.Stop) (infracontract.HostReceipt, error) {
		close(entered)
		<-s
		close(exited)
		return infracontract.HostReceipt{}, ErrStopped
	}
	r := f.request(t, infracontract.SwitchIngress, 1)
	done := make(chan error, 1)
	go func() { _, err := f.worker.Do(r, testDeadline(t), nil); done <- err }()
	<-entered
	if err := f.worker.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
	default:
		t.Fatal("Close returned before backend joined")
	}
	if err := <-done; !errors.Is(err, ErrHalted) {
		t.Fatal(err)
	}
	if _, err := f.worker.Do(r, testDeadline(t), nil); !errors.Is(err, ErrStopped) {
		t.Fatal("closed worker admitted", err)
	}
	if err := f.journal.Close(); err != nil {
		t.Fatal(err)
	}
	// Read-only concurrent observations also share the application cap.
	g := newHostFixture(t)
	gate := make(chan struct{})
	var enteredCount sync.WaitGroup
	enteredCount.Add(4)
	var doneCount sync.WaitGroup
	g.effects.inspect = func(infracontract.ID, infracontract.Deadline) (infracontract.Census, error) {
		enteredCount.Done()
		<-gate
		return infracontract.Census{}, nil
	}
	for n := 0; n < 4; n++ {
		doneCount.Add(1)
		go func() { defer doneCount.Done(); _, _ = g.worker.Inspect(infracontract.ID{1}, testDeadline(t)) }()
	}
	enteredCount.Wait()
	if _, err := g.worker.Inspect(infracontract.ID{1}, testDeadline(t)); !errors.Is(err, ErrBusy) {
		t.Fatal("fifth worker admitted", err)
	}
	close(gate)
	doneCount.Wait()
	_ = g.worker.Close()
	_ = g.journal.Close()
}
