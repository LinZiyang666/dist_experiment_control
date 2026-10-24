package infraupgrade

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

type natsFixture struct {
	t                                       *testing.T
	driver                                  c.Driver
	plan                                    c.Plan
	deadline                                c.Deadline
	current                                 c.Process
	status                                  c.Status
	seal                                    c.AssetSeal
	transfer                                c.TransferSeal
	checkpoint                              c.Checkpoint
	oldAlive, started, single, ldmExits     bool
	transfersHeld, recoveryHeld, cohortHeld bool
	verified, rebound, returned             bool
	finalCensus                             bool
	defect                                  string
	loseReply                               c.HostOp
	fail                                    map[string]int
	hook                                    func(string)
	events                                  []string
	requests                                []c.HostRequest
	effects                                 map[c.HostOp]int
	receipts                                map[uint64]c.HostReceipt
	beforeReads                             int
	prepared                                map[c.RouteDirection][]c.RouteProof
	transitionJournal                       *FileTransactionJournal
	transitionBytes                         []byte
}

func newNATSFixture(t *testing.T, single bool) *natsFixture {
	t.Helper()
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("NATS hot cut requires linux/amd64; cross-build is not a runtime receipt")
	}
	old := c.Process{BootID: "test-boot", PID: 401, StartTicks: 17, InvocationID: "old", ExeDevice: 2, ExeInode: 10, ExeSHA256: c.Digest{10}, Cgroup: "/test/old"}
	artifact := c.Artifact{Kind: c.NATSBinary, Version: "2.14.6", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: c.Digest{11}, BinarySHA256: c.Digest{12}, CatalogSHA256: c.Digest{13}}
	previous := artifact
	previous.Version, previous.BinarySHA256 = "2.14.5", old.ExeSHA256
	f := &natsFixture{t: t, single: single, oldAlive: true, fail: map[string]int{}, effects: map[c.HostOp]int{}, receipts: map[uint64]c.HostReceipt{}, prepared: map[c.RouteDirection][]c.RouteProof{}}
	f.plan = c.Plan{Tx: c.ID{1}, Step: 1, Domain: c.ID{2}, Component: c.NATS, Action: c.Upgrade,
		Candidate: artifact, Previous: previous, Before: old, ConfigSHA256: c.Digest{14}, PolicyRevision: 1,
		Store: c.Store{Device: 2, Inode: 21, MountID: 1, LockDevice: 2, LockInode: 22}}
	f.current = old
	f.current.PID, f.current.StartTicks, f.current.InvocationID, f.current.ExeInode, f.current.ExeSHA256 = 402, 18, "new", 12, artifact.BinarySHA256
	f.seal = c.AssetSeal{Complete: true, Revision: 1, Accounts: c.Digest{15}, Assets: c.Digest{16}, Participants: c.Digest{17}}
	f.transfer = c.TransferSeal{Complete: true, Version: 1, SourceSHA256: c.Digest{18}, SchemaSHA256: c.Digest{19}, InvocationSet: c.Digest{20}, ReceiptSet: c.Digest{21}, ConsumerSet: c.Digest{22}}
	f.checkpoint = c.Checkpoint{Cohort: c.ID{3}, Assets: f.seal, Transfers: f.transfer, Proof: c.Digest{23}}
	var err error
	f.deadline, err = controlbudget.After(upgradebudget.ComponentHandover)
	if err != nil {
		t.Fatal(err)
	}
	f.driver, err = NewNATS(f.dependencies())
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *natsFixture) dependencies() c.Dependencies {
	return c.Dependencies{Host: &natsFixtureHost{f}, Artifacts: natsFixtureArtifacts{f}, Ready: natsFixtureReady{f}, Cohort: natsFixtureCohort{f}, Routes: natsFixtureRoutes{f}, Transfers: natsFixtureTransfers{f}, Recovery: natsFixtureRecovery{f}, Tx: natsFixtureTx{f}, Observe: natsFixtureObserver{f}}
}

func (f *natsFixture) event(name string) error {
	f.events = append(f.events, name)
	if f.hook != nil {
		f.hook(name)
	}
	if f.fail[name] > 0 {
		f.fail[name]--
		return errors.New("private-adapter-canary")
	}
	return nil
}

func (f *natsFixture) before(t *testing.T, first, second string) {
	t.Helper()
	a, b := slices.Index(f.events, first), slices.Index(f.events, second)
	if a < 0 || b < 0 || a >= b {
		t.Fatalf("required order %s < %s: %v", first, second, f.events)
	}
}

func (f *natsFixture) noSignals(t *testing.T) {
	t.Helper()
	for _, request := range f.requests {
		if request.Op == c.SignalLDM || request.Op == c.SignalStop {
			t.Fatalf("unprepared signal: %v", request.Op)
		}
	}
}

func (f *natsFixture) census() c.Census {
	result := c.Census{Complete: true, Generation: 1, LiveBusiness: 4, Digest: c.Digest{24}}
	if f.oldAlive {
		result.Processes = append(result.Processes, f.plan.Before)
	}
	if f.started {
		result.Processes = append(result.Processes, f.current)
	}
	if f.defect == "incomplete_census" && f.started {
		result.Complete = false
	}
	if f.defect == "empty_final_census" && f.returned {
		result.Processes = nil
	}
	if f.defect == "pending_after_return" && f.finalCensus {
		result.OldOwnerPending = 1
	}
	if f.defect == "old_socket" && f.returned {
		result.Sockets = []c.Socket{{Owner: f.plan.Before, Cookie: 2, Inode: 3}}
	}
	return result
}

type natsFixtureHost struct{ f *natsFixture }

func (h *natsFixtureHost) Do(request c.HostRequest, _ c.Deadline, _ c.Stop) (c.HostReceipt, error) {
	f := h.f
	f.requests = append(f.requests, request)
	// Use the real canonical codec to catch forbidden/missing host operands.
	signed := request
	signed.KeyID = "test-authority"
	bytes, err := c.SigningBytes(signed)
	if err != nil {
		f.t.Fatalf("malformed host intent %v: %v", request.Op, err)
	}
	if prior, ok := f.receipts[request.Sequence]; ok {
		if prior.RequestDigest != c.Digest(sha256.Sum256(bytes)) {
			f.t.Fatal("changed request reused a sequence")
		}
		return prior, nil
	}
	name := map[c.HostOp]string{c.Inspect: "inspect", c.Stage: "stage", c.FenceStart: "fence", c.Promote: "promote", c.SignalLDM: "ldm", c.SignalStop: "stop", c.ReleaseStart: "release_start"}[request.Op]
	if name == "" {
		f.t.Fatalf("unexpected host operation: %v", request.Op)
	}
	if err := f.event("host." + name); err != nil {
		return c.HostReceipt{}, err
	}
	if request.Expected != f.plan.Before {
		f.t.Fatal("signal target was refreshed from MainPID")
	}
	f.effects[request.Op]++
	if request.Op == c.SignalStop || request.Op == c.SignalLDM && f.ldmExits {
		if f.defect != "old_alive" {
			f.oldAlive = false
		}
	}
	if request.Op == c.ReleaseStart {
		if f.oldAlive {
			f.t.Fatal("second store owner started before old exit")
		}
		f.started = true
	}
	if request.Op == c.Inspect && f.started {
		f.finalCensus = true
	}
	receipt := c.HostReceipt{Tx: request.Tx, Op: request.Op, Sequence: request.Sequence, Revision: request.Sequence,
		Applied: true, RequestDigest: c.Digest(sha256.Sum256(bytes)), Journal: c.Digest{byte(request.Sequence + 30)}, Current: f.current, Census: f.census()}
	if !f.started {
		receipt.Current = f.plan.Before
	}
	if f.defect == "changed_initial_runtime" && request.Op == c.Inspect {
		receipt.Current.StartTicks++
	}
	if request.Op == c.Stage {
		switch f.defect {
		case "receipt_tx":
			receipt.Tx[0]++
		case "receipt_sequence":
			receipt.Sequence++
		case "receipt_op":
			receipt.Op = c.Promote
		case "receipt_journal":
			receipt.Journal = c.Digest{}
		case "receipt_digest":
			receipt.RequestDigest = c.Digest{}
		case "receipt_applied":
			receipt.Applied = false
		}
	}
	f.receipts[request.Sequence] = receipt
	if request.Op == f.loseReply {
		f.loseReply = 0
		return c.HostReceipt{}, errors.New("reply lost after actual effect")
	}
	return receipt, nil
}

func countEvent(events []string, name string) int {
	count := 0
	for _, event := range events {
		if event == name {
			count++
		}
	}
	return count
}

func (h *natsFixtureHost) Inspect(_ c.ID, _ c.Deadline) (c.Census, error) {
	if err := h.f.event("host.census"); err != nil {
		return c.Census{}, err
	}
	return h.f.census(), nil
}

type natsFixtureArtifacts struct{ f *natsFixture }

func (a natsFixtureArtifacts) Resolve(c.ArtifactKind, string, string, string) (c.Artifact, error) {
	return c.Artifact{}, errors.New("driver must use pinned plan")
}
func (a natsFixtureArtifacts) Verify(_ c.Artifact, _ c.Deadline, _ c.Stop) error {
	return a.f.event("artifact.verify")
}
func (a natsFixtureArtifacts) Compatible(_, _ c.Artifact, config c.Digest, store c.Store, _ c.Deadline) error {
	if store != a.f.plan.Store || config != a.f.plan.ConfigSHA256 {
		a.f.t.Fatal("compatibility did not use current store/config")
	}
	return a.f.event("artifact.compatible")
}

type natsFixtureReady struct{ f *natsFixture }

func (a natsFixtureReady) Read(_ c.Plan, _ c.Deadline, _ c.Stop) (c.Readiness, error) {
	f := a.f
	name := "ready.after"
	if !f.started {
		f.beforeReads++
		name = fmt.Sprintf("ready.before.%d", f.beforeReads)
	}
	if err := f.event(name); err != nil {
		return c.Readiness{}, err
	}
	r := c.Readiness{Complete: true, Topology: c.Digest{25}, Seal: f.seal, Auth: c.Digest{26}, WSS: c.Digest{27}, Groups: c.Digest{28}, Running: f.plan.Before}
	if !f.single {
		r.MetaLeader = "off-host"
	}
	if f.started {
		r.Running = f.current
	}
	switch f.defect {
	case "late_data_NC":
		if f.beforeReads == 2 {
			r.Seal.Participants[0]++
		}
	case "rebuilt_consumer":
		if f.beforeReads == 2 {
			r.Seal.Assets[0]++
		}
	case "missing_auth":
		r.Auth = c.Digest{}
	case "missing_WSS":
		r.WSS = c.Digest{}
	case "missing_topology":
		r.Topology = c.Digest{}
	case "missing_groups":
		r.Groups = c.Digest{}
	case "final_health":
		if f.returned {
			r.Groups = c.Digest{}
		}
	case "wrong_runtime_SHA":
		if f.started {
			r.Running.ExeSHA256 = f.plan.Before.ExeSHA256
		}
	case "reused_old_PID":
		if f.started {
			r.Running.PID, r.Running.StartTicks = f.plan.Before.PID, f.plan.Before.StartTicks
		}
	}
	return r, nil
}

type natsFixtureCohort struct{ f *natsFixture }

func (a natsFixtureCohort) Seal(_ c.Plan, _ c.Deadline) (c.Checkpoint, error) {
	a.f.cohortHeld = true
	checkpoint := a.f.checkpoint
	if a.f.defect == "partial_cohort" {
		return c.Checkpoint{}, errors.New("partial seal without receipt")
	}
	if a.f.defect == "changed_checkpoint" {
		checkpoint.Assets.Revision++
	}
	return checkpoint, a.f.event("cohort.seal")
}
func (a natsFixtureCohort) Install(_ c.Checkpoint, proof c.RouteProof, _ c.Deadline) error {
	return a.f.event(fmt.Sprintf("install.%s.%d", natsDirectionName(proof.Direction), proof.Participant[0]))
}
func (a natsFixtureCohort) Move(_ c.Checkpoint, _ c.Deadline, _ c.Stop) (c.MoveResult, error) {
	return a.moved(c.RouteOutbound)
}
func (a natsFixtureCohort) Return(_ c.Checkpoint, _ c.Deadline, _ c.Stop) (c.MoveResult, error) {
	return a.moved(c.RouteReturn)
}
func (a natsFixtureCohort) moved(direction c.RouteDirection) (c.MoveResult, error) {
	name := "cohort.move"
	if direction == c.RouteReturn {
		name = "cohort.return"
	}
	if err := a.f.event(name); err != nil {
		return c.MoveResult{}, err
	}
	result := c.MoveResult{Census: a.f.census()}
	for _, intent := range a.f.prepared[direction] {
		if a.f.defect == "return_missing_attachment" && direction == c.RouteReturn && intent.Participant[0] == 2 {
			continue
		}
		if a.f.defect == "broker_local_pending" && direction == c.RouteOutbound && intent.Participant[0] == 3 {
			continue
		}
		next := intent.Old
		next.Generation++
		next.CID++
		next.Raw.Inode++
		next.Raw.Cookie++
		next.URL = intent.Target.URL
		next.Ingress = a.f.current
		if a.f.defect == "unchanged_raw" {
			next.Raw = intent.Old.Raw
		}
		result.Attachments = append(result.Attachments, next)
	}
	return result, nil
}
func (a natsFixtureCohort) Bind(_ c.Checkpoint, proofs []c.RouteProof, _ c.Deadline) (c.Census, error) {
	f := a.f
	direction := proofs[0].Direction
	if err := f.event("cohort.bind." + natsDirectionName(direction)); err != nil {
		return c.Census{}, err
	}
	if direction == c.RouteReturn {
		f.returned = true
	}
	result := f.census()
	if f.defect == "pending_owner" || f.defect == "broker_local_pending" && direction == c.RouteOutbound {
		result.OldOwnerPending = 1
	}
	return result, nil
}
func (a natsFixtureCohort) Release(_ c.Checkpoint, _ c.Deadline) error {
	if err := a.f.event("cohort.release"); err != nil {
		return err
	}
	a.f.cohortHeld = false
	return nil
}

type natsFixtureRoutes struct{ f *natsFixture }

func (a natsFixtureRoutes) Prepare(_ c.Plan, _ c.Checkpoint, direction c.RouteDirection, _ c.Deadline) ([]c.RouteProof, error) {
	f := a.f
	if err := f.event("routes.prepare." + natsDirectionName(direction)); err != nil {
		return nil, err
	}
	var proofs []c.RouteProof
	participants := byte(2)
	if f.defect == "broker_local_pending" {
		participants = 3
	}
	for i := byte(1); i <= participants; i++ {
		attachment := c.Attachment{Participant: c.ID{i}, Generation: 1, HealthGeneration: 1, CID: uint64(i), ServerID: "server", URL: "nats://old", Ingress: f.plan.Before,
			Raw: c.Socket{Owner: f.plan.Before, Cookie: uint64(i), Inode: uint64(i)}}
		proofs = append(proofs, c.RouteProof{Version: 1, Tx: f.plan.Tx, Participant: c.ID{i}, Purpose: c.RoutePrepare, Direction: direction,
			Target: c.RouteTarget{URL: "nats://successor", NodeID: "survivor", Component: c.NATS, Before: f.plan.Before, Candidate: f.plan.Candidate, Ingress: f.current},
			Old:    attachment, HomeDigest: c.Digest{29}, AuthorityRevision: 1, PolicyRevision: 1, RemainingNS: uint64(upgradebudget.BrokerMaintenanceTTL), Nonce: c.ID{i, byte(direction)}, KeyID: "route-key", Signature: []byte{1}})
	}
	switch f.defect {
	case "empty_cohort":
		proofs = nil
	case "duplicate_participant":
		proofs[1] = proofs[0]
	case "future_CID":
		proofs[0].New.CID = 5
	}
	f.prepared[direction] = proofs
	return proofs, nil
}
func (a natsFixtureRoutes) Bind(_ c.Plan, moved c.MoveResult, direction c.RouteDirection, _ c.Deadline) ([]c.RouteProof, error) {
	f := a.f
	if err := f.event("routes.bind." + natsDirectionName(direction)); err != nil {
		return nil, err
	}
	var proofs []c.RouteProof
	for _, attachment := range moved.Attachments {
		for _, intent := range f.prepared[direction] {
			if intent.Participant == attachment.Participant {
				intent.Purpose, intent.New = c.RouteBind, attachment
				proofs = append(proofs, intent)
			}
		}
	}
	switch f.defect {
	case "forwarded_ACK":
		proofs[1].New = proofs[0].New
	case "wrong_nonce":
		proofs[0].Nonce[0]++
	case "wrong_target":
		proofs[0].Target.NodeID = "unprepared"
	case "renewed_lease":
		proofs[0].RemainingNS++
	}
	return proofs, nil
}
func natsDirectionName(direction c.RouteDirection) string {
	if direction == c.RouteOutbound {
		return "outbound"
	}
	return "return"
}

type natsFixtureTransfers struct{ f *natsFixture }

func (a natsFixtureTransfers) Fence(_ c.ID, _ c.Deadline) (c.TransferSeal, error) {
	a.f.transfersHeld = true
	seal := a.f.transfer
	if a.f.defect == "partial_transfer" {
		return c.TransferSeal{}, errors.New("partial fence without receipt")
	}
	if a.f.defect == "incomplete_transfer" {
		seal.Complete = false
	}
	if a.f.defect == "changed_transfer" {
		seal.ReceiptSet[0]++
	}
	return seal, a.f.event("transfer.fence")
}
func (a natsFixtureTransfers) Rebind(seal c.TransferSeal, _ c.Deadline) error {
	if !a.f.verified || seal != a.f.transfer {
		a.f.t.Fatal("transfer rebound before original ACK verification")
	}
	if err := a.f.event("transfer.rebind"); err != nil {
		return err
	}
	a.f.rebound = true
	return nil
}
func (a natsFixtureTransfers) Release(_ c.TransferSeal, _ c.Deadline) error {
	if err := a.f.event("transfer.release"); err != nil {
		return err
	}
	a.f.transfersHeld = false
	return nil
}

type natsFixtureRecovery struct{ f *natsFixture }

func (a natsFixtureRecovery) Seal(_ c.Plan, _ c.Deadline) (c.Checkpoint, error) {
	a.f.recoveryHeld = true
	if a.f.defect == "partial_recovery" {
		return c.Checkpoint{}, errors.New("partial checkpoint without receipt")
	}
	return a.f.checkpoint, a.f.event("recovery.seal")
}
func (a natsFixtureRecovery) VerifyBeforeReplay(checkpoint c.Checkpoint, _ c.Deadline, _ c.Stop) error {
	// Verification also authorizes aborting a pre-cut seal while the original
	// server is still serving. Keep the post-signal restart assertion below;
	// this branch cannot authorize a verification between stop and restart.
	// origin: S9 internal review s9-p18 F1
	if !a.f.started && a.f.oldAlive && a.f.effects[c.SignalStop] == 0 && a.f.effects[c.SignalLDM] == 0 {
		if checkpoint != a.f.checkpoint || !a.f.recoveryHeld {
			a.f.t.Fatal("pre-cut ACK oracle did not read the retained checkpoint")
		}
		if err := a.f.event("recovery.verify"); err != nil {
			return err
		}
		a.f.verified = true
		return nil
	}
	if checkpoint != a.f.checkpoint || !a.f.started {
		a.f.t.Fatal("ACK oracle did not read original checkpoint after restart")
	}
	if err := a.f.event("recovery.verify"); err != nil {
		return err
	}
	a.f.verified = true
	return nil
}
func (a natsFixtureRecovery) Release(_ c.Checkpoint, _ c.Deadline) error {
	if !a.f.started && !a.f.verified {
		a.f.t.Fatal("pre-cut repair opened before original ACK verification")
	}
	if a.f.started && (!a.f.verified || !a.f.rebound) {
		a.f.t.Fatal("repair opened before ACK oracle and transfer rebind")
	}
	if err := a.f.event("recovery.release"); err != nil {
		return err
	}
	a.f.recoveryHeld = false
	return nil
}

type natsFixtureTx struct{ f *natsFixture }

func (a natsFixtureTx) Acquire(p c.Plan, _ c.Deadline) (c.Status, error) {
	if err := a.f.event("tx.acquire"); err != nil {
		return c.Status{}, err
	}
	a.f.status = c.Status{Tx: p.Tx, Step: p.Step, Before: p.Before, Current: p.Before, Phase: c.Prepared, Revision: 1}
	return a.f.status, nil
}
func (a natsFixtureTx) Load(_ c.ID, _ c.Deadline) (c.Status, error) {
	return a.f.status, a.f.event("tx.load")
}
func (a natsFixtureTx) CompareAndSwap(prior c.Status, next c.Phase, proof c.Digest, _ c.Deadline) (c.Status, error) {
	if prior.Revision != a.f.status.Revision {
		return c.Status{}, errors.New("revision conflict")
	}
	name := fmt.Sprintf("tx.phase.%d", next)
	if next == c.Completed {
		name = "tx.completed"
	}
	if err := a.f.event(name); err != nil {
		return c.Status{}, err
	}
	prior.Phase, prior.Proof, prior.Revision = next, proof, prior.Revision+1
	if next == c.Staged {
		switch a.f.defect {
		case "CAS_proof":
			prior.Proof[0]++
		case "CAS_revision":
			prior.Revision--
		case "CAS_phase":
			prior.Phase = c.Completed
		}
	}
	a.f.status = prior
	return prior, nil
}

type natsFixtureObserver struct{ f *natsFixture }

func (a natsFixtureObserver) Record(observation c.Observation) error {
	if observation.Evidence == (c.Digest{}) || observation.BootNS <= 0 {
		return c.ErrIncomplete
	}
	return a.f.event("observe." + observation.Kind)
}

// Keep a canary assertion in the common status oracle as well as the failure
// paths: adapter diagnostics must never become a public Detail/Error payload.
func natsAssertSanitized(t *testing.T, status c.Status, err error) {
	t.Helper()
	if strings.Contains(fmt.Sprint(status, err), "private-adapter-canary") {
		t.Fatal("private adapter diagnostics escaped")
	}
}
