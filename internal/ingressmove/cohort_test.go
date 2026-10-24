package ingressmove

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// These isolated tables exercise the registry, not P02's production kernel
// census or P14's cancellation implementation. The real NC exercise lives in the
// sibling move_test.go; cross-package kernel/transport signing remains P30's obligation.
// origin: hot-update S9 plan §4.5 / T34 / M17
func TestIngressCohortSealsLateConnections(t *testing.T) {
	t.Run("independent_data_nc_and_missing_prepare_ack", func(t *testing.T) {
		f := newCohortFixture(t, "control", "data", "consumer", "auth")
		cp := f.seal(t)
		proofs := f.prepares(c.RouteOutbound, f.away)
		for _, p := range proofs[:3] {
			mustInstall(t, f, cp, p)
		}
		f.connections[3].installErr = errors.New("prepare reply cut")
		if err := f.registry.Install(cp, proofs[3], deadline(t)); err == nil {
			t.Fatal("lost prepare ACK certified Installed")
		}
		if _, err := f.registry.Move(cp, deadline(t), nil); err == nil {
			t.Fatal("moved without every Installed ACK")
		}
		for _, p := range f.connections {
			if p.attachCalls != 0 {
				t.Fatal("partial prepare caused an attachment effect")
			}
		}
		ledger, _ := f.registry.Ledger(c.RouteOutbound)
		if len(ledger.Installed) != 3 {
			t.Fatal("data/consumer/auth participant omitted from Installed ledger")
		}
		f.connections[3].installErr = nil
		mustInstall(t, f, cp, proofs[3])
		move := mustMove(t, f, cp, c.RouteOutbound)
		if len(move.Attachments) != 4 {
			t.Fatal("independent NC missing from move census")
		}
		mustBind(t, f, cp, proofs, move)
	})
	t.Run("late_data_nc_after_zero_census", func(t *testing.T) {
		f := newCohortFixture(t, "control", "data")
		cp := f.seal(t)
		if _, err := f.registry.Admit(c.ID{9}, "data", deadline(t)); err == nil {
			t.Fatal("late dial permitted before successor installation")
		}
		proofs := f.prepares(c.RouteOutbound, f.away)
		for _, p := range proofs {
			mustInstall(t, f, cp, p)
		}
		move := mustMove(t, f, cp, c.RouteOutbound)
		mustBind(t, f, cp, proofs, move)
		// The previous census was genuinely complete/zero in this isolated
		// producer. Admission now changes that set and invalidates its authority.
		a, err := f.registry.Admit(c.ID{9}, "data", deadline(t))
		if err != nil || a.Target != f.away {
			t.Fatalf("late dial not restricted to successor: %v", err)
		}
		late := f.add(t, a, f.away)
		if _, err = f.registry.Move(cp, deadline(t), nil); err == nil {
			t.Fatal("old zero census accepted after late registration")
		}
		if _, err = f.registry.Bind(cp, binds(proofs, move), deadline(t)); err == nil {
			t.Fatal("old Bind census accepted after late registration")
		}
		if err = f.registry.Release(cp, deadline(t)); err == nil {
			t.Fatal("late join escaped release barrier")
		}
		fresh := f.seal(t)
		if fresh == cp || fresh.Cohort != cp.Cohort || fresh.Assets.Revision <= cp.Assets.Revision || fresh.Assets.Participants == cp.Assets.Participants {
			t.Fatal("latejoin did not revise the exact participant seal")
		}
		if _, err = f.registry.Move(fresh, deadline(t), nil); err == nil {
			t.Fatal("late participant inherited another NC's Installed ACK")
		}
		p := prepare(f.plan, late.current, c.RouteOutbound, f.away)
		mustInstall(t, f, fresh, p)
		move = mustMove(t, f, fresh, c.RouteOutbound)
		proofs = append(proofs, p)
		mustBind(t, f, fresh, proofs, move)
		if len(move.Attachments) != 3 {
			t.Fatal("late independent dataNC omitted")
		}
	})
	t.Run("old_inflight_admission_and_target_tamper", func(t *testing.T) {
		f := newCohortFixture(t, "control")
		old, err := f.registry.Admit(c.ID{7}, "data", deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		cp := f.seal(t)
		p := f.prepares(c.RouteOutbound, f.away)[0]
		mustInstall(t, f, cp, p)
		conn := &tableConnection{current: attachment(7, 1, f.home)}
		// Its independent physical producer can see this real candidate NC;
		// admission must reject it even when observation itself is complete.
		f.witness.connections[c.ID{7}] = conn
		if err = f.registry.Register(old, conn, deadline(t)); err == nil {
			t.Fatal("pre-seal in-flight dial rejoined old ingress")
		}
		a, err := f.registry.Admit(c.ID{7}, "data", deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if err = f.registry.Register(a, conn, deadline(t)); err == nil {
			t.Fatal("late NC admitted on old ingress")
		}
		a.Target = f.home
		if err = f.registry.Register(a, conn, deadline(t)); err == nil {
			t.Fatal("caller altered successor restriction")
		}
	})
	t.Run("admission_during_census_invalidates_result", func(t *testing.T) {
		f := newCohortFixture(t, "control", "data")
		cp := f.seal(t)
		proofs := f.prepares(c.RouteOutbound, f.away)
		for _, p := range proofs {
			mustInstall(t, f, cp, p)
		}
		f.witness.duringCensus = func() {
			a, err := f.registry.Admit(c.ID{8}, "consumer", deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			f.add(t, a, f.away)
		}
		if _, err := f.registry.Move(cp, deadline(t), nil); err == nil {
			t.Fatal("concurrent admission did not invalidate in-flight census")
		}
	})
	t.Run("concurrent_install_retains_deadline_but_invalidates_ack", func(t *testing.T) {
		f := newCohortFixture(t, "control", "data")
		cp := f.seal(t)
		proofs := f.prepares(c.RouteOutbound, f.away)
		mustInstall(t, f, cp, proofs[0])
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		f.connections[1].installHook = func(d c.Deadline) {
			close(entered)
			left, err := controlbudget.Remaining(d)
			if err != nil {
				return
			}
			timer := time.NewTimer(left)
			defer timer.Stop()
			select {
			case <-release:
			case <-timer.C:
			}
		}
		var releaseOnce sync.Once
		defer releaseOnce.Do(func() { close(release) })
		go func() { done <- f.registry.Install(cp, proofs[1], deadline(t)) }()
		<-entered
		a, err := f.registry.Admit(c.ID{9}, "data", deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		f.add(t, a, f.away)
		releaseOnce.Do(func() { close(release) })
		if err = <-done; err == nil {
			t.Fatal("Install kept old revision across concurrent data admission")
		}
		f.connections[1].installHook = nil
		fresh := f.seal(t)
		snapshot, err := f.registry.Snapshot(fresh, deadline(t))
		if err != nil || len(snapshot) != 3 {
			t.Fatal("route producer omitted late independent NC")
		}
		snapshot[0].CID = 0
		if _, err = f.registry.Snapshot(fresh, deadline(t)); err != nil {
			t.Fatal("Snapshot exposed mutable registry state")
		}
		if _, err = f.registry.Snapshot(cp, deadline(t)); err == nil {
			t.Fatal("stale route producer snapshot accepted")
		}
		mustInstall(t, f, fresh, proofs[1])
	})
	t.Run("complete_census_and_empty_registry_fail_closed", func(t *testing.T) {
		f := newCohortFixture(t)
		if _, err := f.registry.Seal(f.plan, deadline(t)); err == nil {
			t.Fatal("empty NC enumeration certified a seal")
		}
		if _, err := New(nil, f.registry.verify); err == nil {
			t.Fatal("missing physical producer accepted")
		}
		if _, err := New(f.witness, nil); err == nil {
			t.Fatal("unsigned cohort accepted")
		}
		f = newCohortFixture(t, "data")
		for _, defect := range []string{"unknown_nc", "incomplete", "zero_generation", "zero_digest"} {
			t.Run(defect, func(t *testing.T) {
				f.witness.defect = defect
				if _, err := f.registry.Seal(f.plan, deadline(t)); err == nil {
					t.Fatal("unknown/incomplete census certified a seal")
				}
			})
		}
		f.witness.defect = ""
		cp := f.seal(t)
		if cp.Assets.Complete || cp.Transfers.Complete {
			t.Fatal("participant seal falsely certified P18/P19 assets")
		}
		if repeated := f.seal(t); repeated != cp {
			t.Fatal("unchanged Seal minted another cohort")
		}
		changedPlan := f.plan
		changedPlan.Step++
		if _, err := f.registry.Seal(changedPlan, deadline(t)); err == nil {
			t.Fatal("different step reused active cohort")
		}
		if err := f.registry.Remove(f.connections[0].current.Participant, 1, deadline(t)); err == nil {
			t.Fatal("task completion erased sealed attachment responsibility")
		}
		if err := f.registry.Release(cp, deadline(t)); err != nil {
			t.Fatal(err)
		}
		if err := f.registry.Remove(f.connections[0].current.Participant, 2, deadline(t)); err == nil {
			t.Fatal("wrong generation removed NC")
		}
		if err := f.registry.Remove(f.connections[0].current.Participant, 1, deadline(t)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("deadline_and_observation_identity", func(t *testing.T) {
		f := newCohortFixture(t, "control")
		if _, err := f.registry.Seal(f.plan, 0); err == nil {
			t.Fatal("zero deadline admitted")
		}
		if _, err := f.registry.Admit(c.ID{9}, "farewell-capability", deadline(t)); err == nil {
			t.Fatal("unknown dial responsibility admitted")
		}
		if _, err := f.registry.Admit(c.ID{}, "data", deadline(t)); err == nil {
			t.Fatal("anonymous participant admitted")
		}
		for _, field := range []string{"Participant", "Generation", "ServerID", "CID", "URL", "Raw", "Ingress", "HealthGeneration"} {
			t.Run(field, func(t *testing.T) {
				current := f.connections[0].current
				reflect.ValueOf(&f.connections[0].current).Elem().FieldByName(field).SetZero()
				if _, err := f.registry.Seal(f.plan, deadline(t)); err == nil {
					t.Fatal("unannounced/unknown attachment sealed")
				}
				f.connections[0].current = current
			})
		}
	})
	t.Run("n1_future_target_cannot_admit_unverified_dial", func(t *testing.T) {
		f := newCohortFixture(t, "control")
		cp := f.seal(t)
		target := f.away
		target.Ingress = c.Process{}
		target.Before = f.plan.Before
		target.Candidate = c.Artifact{Kind: c.NATSBinary, Version: "candidate", GOOS: "linux", GOARCH: "amd64", BinarySHA256: c.Digest{2}, ArchiveSHA256: c.Digest{2}, CatalogSHA256: c.Digest{2}}
		p := prepare(f.plan, f.connections[0].current, c.RouteOutbound, target)
		mustInstall(t, f, cp, p)
		if _, err := f.registry.Admit(c.ID{8}, "data", deadline(t)); err == nil {
			t.Fatal("future N1 candidate treated as running successor")
		}
	})
	t.Run("registration_rejects_every_unknown_identity_field", func(t *testing.T) {
		paths := []string{"Generation", "ServerID", "CID", "URL", "HealthGeneration", "Raw.NetNS", "Raw.Device", "Raw.Inode", "Raw.Cookie", "Raw.Local", "Raw.Remote"}
		for _, owner := range []string{"Ingress", "Raw.Owner"} {
			for _, field := range []string{"BootID", "PID", "StartTicks", "InvocationID", "ExeDevice", "ExeInode", "ExeSHA256", "Cgroup"} {
				paths = append(paths, owner+"."+field)
			}
		}
		for _, path := range paths {
			t.Run(path, func(t *testing.T) {
				f := newCohortFixture(t, "control")
				a, err := f.registry.Admit(c.ID{2}, "data", deadline(t))
				if err != nil {
					t.Fatal(err)
				}
				conn := &tableConnection{current: attachment(2, 1, f.home)}
				zeroField(&conn.current, path)
				f.witness.connections[a.Participant] = conn
				if err = f.registry.Register(a, conn, deadline(t)); err == nil {
					t.Fatal("unknown attachment identity admitted")
				}
			})
		}
	})
	t.Run("mixed_transport_targets_are_per_participant", func(t *testing.T) {
		f := newCohortFixture(t, "control", "data")
		cp := f.seal(t)
		proofs := f.prepares(c.RouteOutbound, f.away)
		wss := f.away
		wss.URL = "wss://survivor.example:443"
		proofs[1].Target = wss
		for _, proof := range proofs {
			mustInstall(t, f, cp, proof)
		}
		move := mustMove(t, f, cp, c.RouteOutbound)
		if move.Attachments[0].URL != f.away.URL || move.Attachments[1].URL != wss.URL {
			t.Fatal("one target replaced another NC's transport profile")
		}
		mustBind(t, f, cp, proofs, move)
		if _, err := f.registry.Admit(c.ID{9}, "data", deadline(t)); err == nil {
			t.Fatal("mixed-profile late dial guessed a controlNC URL")
		}
		unknown := wss
		unknown.NodeID = "uninstalled"
		if _, err := f.registry.AdmitTo(c.ID{9}, "data", unknown, deadline(t)); err == nil {
			t.Fatal("late dial selected an uninstalled target")
		}
		a, err := f.registry.AdmitTo(c.ID{9}, "data", wss, deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		late := f.add(t, a, wss)
		fresh := f.seal(t)
		if _, err = f.registry.Move(fresh, deadline(t), nil); err == nil {
			t.Fatal("mixed-profile late participant inherited an Installed ACK")
		}
		proof := prepare(f.plan, late.current, c.RouteOutbound, wss)
		mustInstall(t, f, fresh, proof)
		proofs = append(proofs, proof)
		move = mustMove(t, f, fresh, c.RouteOutbound)
		mustBind(t, f, fresh, proofs, move)
	})
}

func zeroField(value *c.Attachment, path string) {
	field := reflect.ValueOf(value).Elem()
	for _, name := range strings.Split(path, ".") {
		field = field.FieldByName(name)
	}
	field.SetZero()
}

type cohortFixture struct {
	registry             *Registry
	witness              *tableWitness
	connections          []*tableConnection
	plan                 c.Plan
	home, away, returned c.RouteTarget
}

func newCohortFixture(t *testing.T, roles ...string) *cohortFixture {
	t.Helper()
	f := &cohortFixture{plan: c.Plan{Tx: c.ID{50}, Domain: c.ID{51}, Step: 1, PolicyRevision: 7, Component: c.NATS, Action: c.Upgrade, Before: process(1)}}
	f.home = c.RouteTarget{URL: "nats://127.0.0.1:4222", NodeID: "home", Component: c.NATS, Ingress: process(1)}
	f.away = c.RouteTarget{URL: "nats://127.0.0.1:4223", NodeID: "survivor", Component: c.NATS, Ingress: process(2)}
	f.returned = c.RouteTarget{URL: f.home.URL, NodeID: "home", Component: c.NATS, Ingress: process(3)}
	f.witness = &tableWitness{connections: make(map[c.ID]*tableConnection)}
	var err error
	f.registry, err = New(f.witness, func(plan c.Plan, p c.RouteProof, _ c.Deadline) error {
		if plan != f.plan || !bytes.Equal(p.Signature, bytes.Repeat([]byte{17}, ed25519.SignatureSize)) {
			return errors.New("route authority rejected")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, role := range roles {
		a, e := f.registry.Admit(c.ID{byte(i + 1)}, role, deadline(t))
		if e != nil {
			t.Fatal(e)
		}
		f.add(t, a, f.home)
	}
	return f
}

func (f *cohortFixture) add(t *testing.T, a Admission, target c.RouteTarget) *tableConnection {
	t.Helper()
	p := &tableConnection{current: attachment(a.Participant[0], 1, target)}
	f.witness.mu.Lock()
	f.witness.connections[a.Participant] = p
	f.witness.mu.Unlock()
	if err := f.registry.Register(a, p, deadline(t)); err != nil {
		t.Fatal(err)
	}
	f.connections = append(f.connections, p)
	return p
}
func (f *cohortFixture) seal(t *testing.T) c.Checkpoint {
	t.Helper()
	cp, err := f.registry.Seal(f.plan, deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	return cp
}
func (f *cohortFixture) prepares(direction c.RouteDirection, target c.RouteTarget) []c.RouteProof {
	var proofs []c.RouteProof
	for _, conn := range f.connections {
		proofs = append(proofs, prepare(f.plan, conn.current, direction, target))
	}
	return proofs
}
func deadline(t testing.TB) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(upgradebudget.RequestLatency)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func process(id byte) c.Process {
	return c.Process{BootID: "sandbox-boot", PID: int32(id), StartTicks: uint64(id), InvocationID: "sandbox-invocation", ExeDevice: 1, ExeInode: uint64(id), ExeSHA256: c.Digest{id}, Cgroup: "/sandbox"}
}
func attachment(id byte, generation uint64, target c.RouteTarget) c.Attachment {
	return c.Attachment{Participant: c.ID{id}, Generation: generation, ServerID: target.NodeID, CID: uint64(id)*100 + generation, URL: target.URL, Ingress: target.Ingress, HealthGeneration: 1, Raw: c.Socket{NetNS: 1, Device: 1, Inode: uint64(id)*1000 + generation, Cookie: uint64(id)*1000 + generation, Local: "127.0.0.1:31000", Remote: target.URL, Owner: process(10)}}
}
func prepare(plan c.Plan, old c.Attachment, direction c.RouteDirection, target c.RouteTarget) c.RouteProof {
	return c.RouteProof{Purpose: c.RoutePrepare, Direction: direction, Target: target, Version: 1, Tx: plan.Tx, Participant: old.Participant, Old: old, HomeDigest: c.Digest{8}, AuthorityRevision: 9, PolicyRevision: plan.PolicyRevision, RemainingNS: uint64(upgradebudget.BrokerMaintenanceTTL), Nonce: c.ID{old.Participant[0], byte(direction), byte(old.Generation)}, KeyID: "sandbox-key", Signature: bytes.Repeat([]byte{17}, ed25519.SignatureSize)}
}
func binds(proofs []c.RouteProof, move c.MoveResult) []c.RouteProof {
	result := make([]c.RouteProof, 0, len(proofs))
	for _, p := range proofs {
		p.Purpose = c.RouteBind
		for _, a := range move.Attachments {
			if a.Participant == p.Participant {
				p.New = a
			}
		}
		result = append(result, cloneProof(p))
	}
	return result
}
func mustInstall(t *testing.T, f *cohortFixture, cp c.Checkpoint, p c.RouteProof) {
	t.Helper()
	if err := f.registry.Install(cp, p, deadline(t)); err != nil {
		t.Fatal(err)
	}
}
func mustMove(t *testing.T, f *cohortFixture, cp c.Checkpoint, direction c.RouteDirection) c.MoveResult {
	t.Helper()
	var result c.MoveResult
	var err error
	if direction == c.RouteOutbound {
		result, err = f.registry.Move(cp, deadline(t), nil)
	} else {
		result, err = f.registry.Return(cp, deadline(t), nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func mustBind(t *testing.T, f *cohortFixture, cp c.Checkpoint, proofs []c.RouteProof, move c.MoveResult) {
	t.Helper()
	result, err := f.registry.Bind(cp, binds(proofs, move), deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Complete || result.LiveBusiness == 0 || result.OldOwnerPending != 0 {
		t.Fatal("invalid actual responsibility census")
	}
}

type tableConnection struct {
	mu                                   sync.Mutex
	current                              c.Attachment
	installErr, bindErr, attachErr       error
	installReply, bindReply              *c.Attachment
	attachMode                           string
	attachCalls, bindCalls, installCalls int
	attachDeadline                       c.Deadline
	installHook                          func(c.Deadline)
}

func (p *tableConnection) Observe(c.Deadline) (c.Attachment, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current, nil
}
func (p *tableConnection) Install(_ c.RouteProof, d c.Deadline) (c.Attachment, error) {
	if p.installHook != nil {
		p.installHook(d)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.installCalls++
	if p.installReply != nil {
		return *p.installReply, p.installErr
	}
	return p.current, p.installErr
}
func (p *tableConnection) Attach(target c.RouteTarget, d c.Deadline, _ c.Stop) (c.Attachment, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attachCalls++
	p.attachDeadline = d
	old := p.current
	p.current = attachment(old.Participant[0], old.Generation+1, target)
	switch p.attachMode {
	case "pool_only":
		p.current.Raw = old.Raw
	case "same_cid":
		p.current.ServerID, p.current.CID = old.ServerID, old.CID
	case "old_generation":
		p.current.Generation = old.Generation
	case "wrong_ingress":
		p.current.Ingress = old.Ingress
	case "forwarded":
		p.current.Participant = c.ID{99}
	case "unchanged":
		p.current = old
	}
	return p.current, p.attachErr
}
func (p *tableConnection) Bind(_ c.RouteProof, _ c.Deadline) (c.Attachment, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bindCalls++
	if p.bindReply != nil {
		return *p.bindReply, p.bindErr
	}
	return p.current, p.bindErr
}

type tableWitness struct {
	mu           sync.Mutex
	connections  map[c.ID]*tableConnection
	defect       string
	pending      uint32
	duringCensus func()
}

func (w *tableWitness) Verify(a c.Attachment, _ c.RouteTarget, _ c.Deadline) error {
	w.mu.Lock()
	p := w.connections[a.Participant]
	defect := w.defect
	w.mu.Unlock()
	if defect == "verify_failure" {
		return ErrIdentity
	}
	if p == nil {
		return c.ErrIncomplete
	}
	observed, _ := p.Observe(0)
	if observed != a {
		return ErrIdentity
	}
	return nil
}
func (w *tableWitness) Census(_ c.Plan, attachments []c.Attachment, _ c.Deadline) (c.Census, error) {
	w.mu.Lock()
	hook := w.duringCensus
	w.duringCensus = nil
	defect, pending := w.defect, w.pending
	connections := make(map[c.ID]*tableConnection, len(w.connections))
	for id, p := range w.connections {
		connections[id] = p
	}
	w.mu.Unlock()
	if hook != nil {
		hook()
	}
	if defect == "unknown_nc" || len(attachments) != len(connections) {
		return c.Census{}, c.ErrIncomplete
	}
	for _, a := range attachments {
		p := connections[a.Participant]
		if p == nil {
			return c.Census{}, c.ErrIncomplete
		}
		got, _ := p.Observe(0)
		if got != a {
			return c.Census{}, ErrIdentity
		}
	}
	result := c.Census{Complete: true, Generation: 1, LiveBusiness: uint32(len(connections)), OldOwnerPending: pending, Digest: c.Digest{1}}
	if defect == "incomplete" {
		result.Complete = false
	}
	if defect == "zero_generation" {
		result.Generation = 0
	}
	if defect == "zero_digest" {
		result.Digest = c.Digest{}
	}
	return result, nil
}

// Keep this helper clock-derived: the original absolute hold must survive a
// lost prepare reply; wall-clock sleeps cannot renew it on the next attempt.
func waitDeadline(t *testing.T, d c.Deadline) {
	t.Helper()
	left, err := controlbudget.Remaining(d)
	if err == nil {
		timer := time.NewTimer(left + time.Millisecond)
		defer timer.Stop()
		<-timer.C
	}
}
