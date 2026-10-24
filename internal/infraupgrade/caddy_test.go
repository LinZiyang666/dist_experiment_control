package infraupgrade

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// These injected owners test the leaf's actual ordering and evidence admission.
// They do not sign kernel, stock-Caddy, systemd or ACME integration receipts.
// P29 owns those independent real-process tests; no P00 stub is made successful.
type caddyFixture struct {
	t             *testing.T
	plan          c.Plan
	warm, main    c.Process
	now           int64
	verifyDelay   int64
	asyncStart    bool
	realWait      bool
	startDelay    int
	warmCountdown int
	mainCountdown int
	driver        *caddyDriver
	status        c.Status
	saved         CaddyTransition
	key           ed25519.PrivateKey
	oldLive       bool
	warmLive      bool
	mainLive      bool
	fenced        bool
	finalized     bool
	redirect      bool
	moved         bool
	returned      bool
	reloaded      bool
	seal          uint64
	attachments   []c.Attachment
	prepares      []c.RouteProof
	trace         []string
	requests      []c.HostRequest
	replies       map[uint64]c.HostReceipt
	requestBytes  map[uint64][]byte
	applied       map[c.HostOp]int
	lost          c.HostOp
	fault         string
	after         func(c.HostOp)
	censusHook    func(*c.Census)
	signHook      func(*c.HostRequest)
	bindHook      func([]c.RouteProof)
	observation   []c.Observation
}

func caddyDigest(n byte) c.Digest { return sha256.Sum256([]byte{n}) }
func caddyID(n byte) c.ID         { return c.ID{n} }
func caddyTestProcess(n byte, sha c.Digest) c.Process {
	return c.Process{BootID: "test-boot", PID: int32(n), StartTicks: uint64(n) * 100,
		InvocationID: fmt.Sprintf("invocation-%d", n), ExeDevice: 1, ExeInode: uint64(n),
		ExeSHA256: sha, Cgroup: fmt.Sprintf("/test/%d", n)}
}

func newCaddyFixture(t *testing.T, action c.Action) *caddyFixture {
	t.Helper()
	old := c.Artifact{Kind: c.CaddyBinary, Version: "2.7.5", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: caddyDigest(1), BinarySHA256: caddyDigest(2), CatalogSHA256: caddyDigest(3)}
	newArtifact := old
	newArtifact.Version, newArtifact.BinarySHA256, newArtifact.ArchiveSHA256 = "2.7.6", caddyDigest(4), caddyDigest(5)
	if action == c.Rollback {
		old, newArtifact = newArtifact, old
	}
	if action == c.Reload {
		newArtifact = old
	}
	h := &caddyFixture{t: t, now: int64(time.Second), oldLive: true,
		key: ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)), replies: make(map[uint64]c.HostReceipt),
		requestBytes: make(map[uint64][]byte), applied: make(map[c.HostOp]int)}
	h.plan = c.Plan{Tx: caddyID(1), Step: 1, Domain: caddyID(2), Component: c.Caddy, Action: action,
		Candidate: newArtifact, Previous: old, Before: caddyTestProcess(10, old.BinarySHA256),
		ConfigSHA256: caddyDigest(6), Store: c.Store{Device: 1, Inode: 2, MountID: 3, LockDevice: 4, LockInode: 5}, PolicyRevision: 1}
	h.warm, h.main = caddyTestProcess(20, newArtifact.BinarySHA256), caddyTestProcess(30, newArtifact.BinarySHA256)
	h.main.Cgroup = h.plan.Before.Cgroup
	if action == c.Reload {
		h.main = h.plan.Before
	}
	for i := byte(1); i <= 2; i++ {
		h.attachments = append(h.attachments, c.Attachment{Participant: caddyID(i + 10), Generation: 1,
			ServerID: "same-upstream", CID: uint64(i), URL: "wss://example.test:443", HealthGeneration: 1,
			Ingress: h.plan.Before, Raw: c.Socket{NetNS: 1, Device: 1, Inode: uint64(i), Cookie: uint64(i),
				Local: "127.0.0.1:10000", Remote: "127.0.0.1:443", Owner: caddyTestProcess(40+i, caddyDigest(40))}})
	}
	h.newDriver()
	return h
}

func (h *caddyFixture) deps() c.Dependencies {
	return c.Dependencies{Host: caddyTestHost{h}, Artifacts: caddyTestArtifacts{h}, Ready: caddyTestReady{h},
		Cohort: caddyTestCohort{h}, Routes: caddyTestRoutes{h}, Tx: caddyTestTransaction{h}, Observe: caddyTestObserver{h}}
}

func (h *caddyFixture) newDriver() {
	h.t.Helper()
	driver, err := NewCaddy(h.deps())
	if err != nil {
		h.t.Fatal(err)
	}
	h.driver = driver.(*caddyDriver)
	h.driver.now = func() (int64, error) { return h.now, nil }
	if !h.realWait {
		h.driver.wait = func(stop c.Stop) error {
			select {
			case <-stop:
				return errors.New("infra_budget_exceeded")
			default:
			}
			h.now += int64(time.Second)
			return nil
		}
	}
}

func (h *caddyFixture) deadline() c.Deadline { return c.Deadline(h.now + int64(10*time.Minute)) }
func (h *caddyFixture) event(event string)   { h.trace = append(h.trace, event) }

func (h *caddyFixture) census() c.Census {
	out := c.Census{Complete: true, Generation: uint64(len(h.trace) + 1), LiveBusiness: 9, Digest: caddyDigest(byte(len(h.trace) + 1))}
	if h.oldLive {
		out.Processes = append(out.Processes, h.plan.Before)
	}
	if h.warmLive {
		out.Processes = append(out.Processes, h.warm)
	}
	if h.mainLive && h.main != h.plan.Before {
		out.Processes = append(out.Processes, h.main)
	}
	if h.moved && !h.returned && h.fault == "old_pending" {
		out.OldOwnerPending = 1
	}
	if h.returned && h.fault == "warm_pending" {
		out.OldOwnerPending = 1
	}
	if (h.moved && !h.returned && h.fault == "old_socket") || (h.returned && h.fault == "warm_socket") {
		owner := h.plan.Before
		if h.returned {
			owner = h.warm
		}
		out.Sockets = []c.Socket{{Owner: owner, NetNS: 1, Device: 1, Inode: 999, Cookie: 999}}
	}
	if h.censusHook != nil {
		h.censusHook(&out)
	}
	return out
}

type caddyTestHost struct{ h *caddyFixture }

func (a caddyTestHost) AuthorizeCaddy(_ c.Plan, r c.HostRequest, _ c.Deadline) (c.HostRequest, error) {
	r.KeyID, r.Nonce = "fixture-key", caddyID(byte(r.Sequence))
	if a.h.signHook != nil {
		a.h.signHook(&r)
	}
	data, err := c.SigningBytes(r)
	if err != nil {
		return c.HostRequest{}, err
	}
	r.Signature = ed25519.Sign(a.h.key, data)
	return r, nil
}

func (a caddyTestHost) Inspect(_ c.ID, _ c.Deadline) (c.Census, error) {
	if a.h.warmCountdown > 0 {
		a.h.warmCountdown--
		a.h.warmLive = a.h.warmCountdown == 0
	}
	if a.h.mainCountdown > 0 {
		a.h.mainCountdown--
		a.h.mainLive = a.h.mainCountdown == 0
	}
	a.h.event("census")
	return a.h.census(), nil
}

func (a caddyTestHost) Do(r c.HostRequest, _ c.Deadline, _ c.Stop) (c.HostReceipt, error) {
	h := a.h
	if err := c.VerifyRequest(r, h.key.Public().(ed25519.PublicKey)); err != nil {
		return c.HostReceipt{}, err
	}
	data, err := c.EncodeRequest(r)
	if err != nil {
		return c.HostReceipt{}, err
	}
	if cached, ok := h.replies[r.Sequence]; ok {
		if !slices.Equal(h.requestBytes[r.Sequence], data) {
			h.t.Fatal("lost reply recovery changed the signed request")
		}
		h.event("cached")
		return cached, nil
	}
	h.requests = append(h.requests, r)
	h.event(fmt.Sprintf("host:%d", r.Op))
	current := h.plan.Before
	switch r.Op {
	case c.Inspect, c.Stage:
	case c.FenceStart:
		h.fenced = true
	case c.StartWarm:
		h.warmLive, current = true, h.warm
		if h.startDelay > 0 {
			h.warmLive, h.warmCountdown = false, h.startDelay
		}
	case c.SwitchIngress:
		h.redirect = r.Expected == h.plan.Before && !h.reloaded
		if h.plan.Action != c.Reload && r.Expected == h.main {
			h.redirect = false
		}
	case c.Promote:
		if !h.fenced || !h.moved {
			h.t.Fatal("promotion preceded the startup fence or outbound migration")
		}
	case c.SignalGraceful:
		if r.Expected != h.plan.Before {
			h.t.Fatal("graceful signal targeted the wrong invocation")
		}
		h.oldLive = false
	case c.FinalizeStorage:
		if h.oldLive || !h.fenced {
			h.t.Fatal("final ACME delta preceded old exit or lost its fence")
		}
		h.finalized = true
	case c.ReleaseStart:
		if !h.finalized {
			h.t.Fatal("startup released before final current storage")
		}
		h.mainLive, current = true, h.main
		if h.startDelay > 0 {
			h.mainLive, h.mainCountdown = false, h.startDelay
		}
	case c.RetireWarm:
		if r.Expected != h.warm || !h.returned || r.ReturnProof == (c.Digest{}) {
			h.t.Fatal("warm retirement lacked its invocation or return proof")
		}
		if h.fault != "keep_warm" {
			h.warmLive = false
		}
	default:
		h.t.Fatalf("unexpected host operation %d", r.Op)
	}
	h.applied[r.Op]++
	if h.asyncStart && (r.Op == c.StartWarm || r.Op == c.ReleaseStart) {
		current = c.Process{}
	}
	receipt := c.HostReceipt{Tx: r.Tx, Sequence: r.Sequence, Revision: r.Sequence,
		Op: r.Op, Applied: true, RequestDigest: sha256.Sum256(data), Journal: caddyDigest(byte(r.Sequence + 80)),
		Current: current, Census: h.census()}
	h.replies[r.Sequence], h.requestBytes[r.Sequence] = receipt, data
	if h.after != nil {
		h.after(r.Op)
	}
	if h.lost == r.Op {
		h.lost = 0
		return c.HostReceipt{}, errors.New("infra_result_unknown")
	}
	if h.fault == "wrong_receipt" {
		receipt.RequestDigest = caddyDigest(99)
	}
	return receipt, nil
}

type caddyTestArtifacts struct{ h *caddyFixture }

func (a caddyTestArtifacts) Resolve(_ c.ArtifactKind, version, _, _ string) (c.Artifact, error) {
	if version == a.h.plan.Candidate.Version {
		return a.h.plan.Candidate, nil
	}
	return a.h.plan.Previous, nil
}
func (a caddyTestArtifacts) Verify(c.Artifact, c.Deadline, c.Stop) error {
	a.h.event("verify")
	a.h.now += a.h.verifyDelay
	return nil
}
func (a caddyTestArtifacts) Compatible(c.Artifact, c.Artifact, c.Digest, c.Store, c.Deadline) error {
	a.h.event("compatible")
	if a.h.fault == "bad_config" {
		return errors.New("infra_config_invalid")
	}
	if a.h.fault == "secret_error" {
		return errors.New("private-key-canary must not escape")
	}
	return nil
}

type caddyTestReady struct{ h *caddyFixture }

func (a caddyTestReady) Read(p c.Plan, _ c.Deadline, _ c.Stop) (c.Readiness, error) {
	a.h.event("ready")
	running := p.Before
	if a.h.fault == "main_hits_shadow" && p.Before == a.h.main {
		running = a.h.warm
	}
	return c.Readiness{Complete: true, Running: running, Auth: caddyDigest(71), WSS: caddyDigest(72)}, nil
}
func (a caddyTestReady) ValidateCaddyReload(c.Plan, c.Deadline, c.Stop) error {
	if a.h.fault == "root_reload" {
		return c.ErrUnsupported
	}
	return nil
}
func (a caddyTestReady) ReloadCaddy(_ c.Plan, expected c.Process, _ c.Deadline, _ c.Stop) (c.Digest, error) {
	if expected != a.h.plan.Before || !a.h.moved {
		a.h.t.Fatal("reload lost its invocation or preceded migration")
	}
	a.h.event("load")
	a.h.reloaded = true
	return a.h.plan.ConfigSHA256, nil
}

type caddyTestTransaction struct{ h *caddyFixture }

func (a caddyTestTransaction) Acquire(p c.Plan, _ c.Deadline) (c.Status, error) {
	a.h.status = c.Status{Tx: p.Tx, Step: p.Step, Phase: c.Prepared, Revision: 1, Before: p.Before}
	return a.h.status, nil
}
func (a caddyTestTransaction) CompareAndSwap(s c.Status, phase c.Phase, proof c.Digest, _ c.Deadline) (c.Status, error) {
	if s.Revision != a.h.status.Revision {
		return c.Status{}, errors.New("infra_busy")
	}
	s.Revision++
	s.Phase, s.Proof = phase, proof
	a.h.status = s
	a.h.event(fmt.Sprintf("phase:%d", phase))
	return s, nil
}
func (a caddyTestTransaction) Load(c.ID, c.Deadline) (c.Status, error) { return a.h.status, nil }
func (a caddyTestTransaction) LoadCaddy(c.Plan, c.Deadline) (CaddyTransition, error) {
	return caddyClone(a.h.t, a.h.saved), nil
}
func (a caddyTestTransaction) StoreCaddy(_ c.Plan, x CaddyTransition, _ c.Deadline) error {
	a.h.saved = caddyClone(a.h.t, x)
	return nil
}
func caddyClone(t *testing.T, x CaddyTransition) CaddyTransition {
	t.Helper()
	data, err := json.Marshal(x)
	if err != nil {
		t.Fatal(err)
	}
	var clone CaddyTransition
	if err := json.Unmarshal(data, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

type caddyTestObserver struct{ h *caddyFixture }

func (a caddyTestObserver) Record(o c.Observation) error {
	if err := c.ValidateObservation(o); err != nil {
		return err
	}
	a.h.observation = append(a.h.observation, o)
	return nil
}

type caddyTestCohort struct{ h *caddyFixture }

func (a caddyTestCohort) Seal(c.Plan, c.Deadline) (c.Checkpoint, error) {
	a.h.seal++
	a.h.event("seal")
	return c.Checkpoint{Cohort: caddyID(byte(a.h.seal)), Proof: caddyDigest(byte(a.h.seal)), Assets: c.AssetSeal{Complete: true}}, nil
}
func (a caddyTestCohort) Install(_ c.Checkpoint, _ c.RouteProof, _ c.Deadline) error {
	a.h.event("install")
	if a.h.fault == "missing_install" {
		return c.ErrIncomplete
	}
	return nil
}
func (a caddyTestCohort) Move(c.Checkpoint, c.Deadline, c.Stop) (c.MoveResult, error) {
	a.h.event("move")
	a.h.moved = true
	if a.h.fault == "partial_move" {
		return a.move(a.h.warm), c.ErrIncomplete
	}
	return a.move(a.h.warm), nil
}
func (a caddyTestCohort) Return(c.Checkpoint, c.Deadline, c.Stop) (c.MoveResult, error) {
	a.h.event("return")
	if a.h.fault == "return_failed" {
		return c.MoveResult{}, c.ErrIncomplete
	}
	a.h.returned = true
	return a.move(a.h.main), nil
}
func (a caddyTestCohort) move(target c.Process) c.MoveResult {
	for i := range a.h.attachments {
		attachment := &a.h.attachments[i]
		attachment.Generation++
		attachment.HealthGeneration++
		attachment.Raw.Cookie += 10
		attachment.Raw.Inode += 10
		attachment.CID += 10
		attachment.Ingress = target
	}
	return c.MoveResult{Census: a.h.census(), Attachments: slices.Clone(a.h.attachments)}
}
func (a caddyTestCohort) Bind(_ c.Checkpoint, proofs []c.RouteProof, _ c.Deadline) (c.Census, error) {
	a.h.event("cohort_bind")
	for i, proof := range proofs {
		if !reflect.DeepEqual(proof.New, a.h.attachments[i]) {
			a.h.t.Fatal("cohort received an invented attachment")
		}
	}
	return a.h.census(), nil
}
func (a caddyTestCohort) Release(c.Checkpoint, c.Deadline) error {
	a.h.event("release")
	return nil
}

type caddyTestRoutes struct{ h *caddyFixture }

func (a caddyTestRoutes) Prepare(_ c.Plan, _ c.Checkpoint, direction c.RouteDirection, _ c.Deadline) ([]c.RouteProof, error) {
	a.h.event("prepare")
	target := a.h.warm
	if direction == c.RouteReturn {
		target = a.h.main
	}
	var proofs []c.RouteProof
	for _, old := range a.h.attachments {
		proofs = append(proofs, c.RouteProof{Version: 1, Purpose: c.RoutePrepare, Direction: direction,
			Tx: a.h.plan.Tx, Participant: old.Participant, Old: old, Target: c.RouteTarget{URL: old.URL,
				NodeID: "local", Component: c.Caddy, Ingress: target}, HomeDigest: caddyDigest(9),
			AuthorityRevision: 1, PolicyRevision: 1, RemainingNS: uint64(8 * time.Second),
			Nonce: caddyID(byte(old.Generation + 70)), KeyID: "route-key", Signature: make([]byte, 64)})
	}
	a.h.prepares = proofs
	return proofs, nil
}
func (a caddyTestRoutes) Bind(_ c.Plan, moved c.MoveResult, _ c.RouteDirection, _ c.Deadline) ([]c.RouteProof, error) {
	a.h.event("route_bind")
	proofs := slices.Clone(a.h.prepares)
	for i := range proofs {
		proofs[i].Purpose, proofs[i].New = c.RouteBind, moved.Attachments[i]
	}
	if a.h.bindHook != nil {
		a.h.bindHook(proofs)
	}
	return proofs, nil
}

// origin: hot-update S9 plan §5.2 / §5.3 / §5.4 / T22 / M12
func TestCaddyDriverRetiresAllOldResponsibilities(t *testing.T) {
	t.Run("finite_HTTP_finishes_naturally", func(t *testing.T) {
		h := newCaddyFixture(t, c.Upgrade)
		pending := 4
		h.censusHook = func(census *c.Census) {
			if h.moved && h.oldLive && pending > 0 {
				pending--
				census.OldOwnerPending = 1
			}
		}
		status, err := h.driver.Advance(h.plan, h.deadline(), nil)
		if err != nil || status.Phase != c.Completed || pending != 0 {
			t.Fatalf("finite HTTP did not drain within the original budget: %v", err)
		}
	})
	t.Run("nonblocking_start_job", func(t *testing.T) {
		h := newCaddyFixture(t, c.Upgrade)
		h.asyncStart = true
		h.startDelay = 2
		status, err := h.driver.Advance(h.plan, h.deadline(), nil)
		if err != nil || status.Phase != c.Completed || status.Current != h.main || h.applied[c.ReleaseStart] != 1 || h.applied[c.StartWarm] != 1 {
			t.Fatalf("nonblocking start was treated as runtime readiness or repeated: %v", err)
		}
	})
	for _, action := range []c.Action{c.Upgrade, c.Rollback, c.Reload} {
		t.Run(fmt.Sprintf("action_%d", action), func(t *testing.T) {
			h := newCaddyFixture(t, action)
			status, err := h.driver.Advance(h.plan, h.deadline(), nil)
			if err != nil || status.Phase != c.Completed {
				t.Fatalf("handover: phase=%v cursor=%v err=%v", status.Phase, h.saved.Cursor, err)
			}
			if h.warmLive || (!h.reloaded && h.oldLive) || !h.returned || status.Census.LiveBusiness != 9 || status.Census.OldOwnerPending != 0 || status.Current != h.main {
				t.Fatal("completion left old obligations or ended original business")
			}
			if action == c.Reload {
				if h.applied[c.SignalGraceful] != 0 || h.applied[c.ReleaseStart] != 0 || !h.reloaded || status.Current != h.plan.Before {
					t.Fatal("reload changed process identity or did not load config")
				}
			} else if h.applied[c.SignalGraceful] != 1 || h.applied[c.FinalizeStorage] != 1 || h.applied[c.ReleaseStart] != 1 {
				t.Fatal("binary handover skipped an old-exit/current-storage/start barrier")
			}
			caddyAssertOrder(t, h.trace, "prepare", "install", fmt.Sprintf("host:%d", c.SwitchIngress), "move", "route_bind", "cohort_bind", "return", fmt.Sprintf("host:%d", c.RetireWarm), fmt.Sprintf("phase:%d", c.Completed))
			if status.Proof != h.saved.Journal || h.saved.Pending != nil || h.saved.Cursor != caddyFinished {
				t.Fatal("completion disagrees with the root journal or retains a request")
			}
		})
	}
	for _, fault := range []string{"old_pending", "old_socket", "warm_pending", "warm_socket", "keep_warm", "main_hits_shadow", "return_failed", "missing_install", "wrong_receipt", "bad_config"} {
		t.Run(fault, func(t *testing.T) {
			h := newCaddyFixture(t, c.Upgrade)
			h.fault = fault
			status, err := h.driver.Advance(h.plan, h.deadline(), nil)
			if err == nil || status.Phase == c.Completed || h.applied[c.RetireWarm] != 0 && fault != "keep_warm" {
				t.Fatalf("unsafe retirement accepted: phase=%v err=%v", status.Phase, err)
			}
			if slices.Contains([]string{"old_pending", "old_socket", "missing_install", "bad_config"}, fault) && h.applied[c.SignalGraceful] != 0 {
				t.Fatal("old main was stopped before complete responsibility retirement")
			}
		})
	}
}

func caddyAssertOrder(t *testing.T, trace []string, events ...string) {
	t.Helper()
	for _, event := range events {
		index := slices.Index(trace, event)
		if index < 0 {
			t.Fatalf("missing or reordered barrier %q in %v", event, trace)
		}
		trace = trace[index+1:]
	}
}
