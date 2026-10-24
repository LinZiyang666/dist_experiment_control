package cli

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetransport"
	"github.com/nats-io/nats.go"
)

// ClusterRollIO keeps the driver independent of Cobra and credential storage.
// Each callback must honor ctx and join its own transport before returning. The
// broker remains the authority for holder CAS, maintenance ACKs and the role-edge
// fence: two health reads cannot prove that no leader ABA occurred between them.
// origin: hot-update S6 plan §5 / T38
type ClusterRollIO struct {
	Health  func(context.Context) ([]proto.ClusterHealthResp, error)
	Trigger func(context.Context, *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error)
}

// ClusterRollStep carries product scope separately from the host-presence fact.
// Voters is the configured set shown to the operator; live probes must agree.
type ClusterRollStep struct {
	Step                                   clusterupgrade.Step
	Scope                                  clusterupgrade.PlanScope
	Voters                                 []string
	ToVersion, ExpectSHA256, Holder, CutID string
	PollInterval                           time.Duration
}

// PlanClusterRoll supplements version planning with measured running bytes.
// Same-release rebuilds must still enter reload, and absent image status cannot
// certify completion. Agent presence is never rewritten to implement scope.
// origin: hot-update S6 plan §5 / T45
// A release string is never a skip witness (S8 plan §4.2 / IL14): without the
// expected SHA no broker is skippable, and a co-located agent's release alone
// can never mark its host done — its leg must run and prove itself.
func PlanClusterRoll(nodes []clusterupgrade.Node, target string, scope clusterupgrade.PlanScope, sha string, health []proto.ClusterHealthResp) clusterupgrade.Plan {
	copyNodes := append([]clusterupgrade.Node(nil), nodes...)
	for i := range copyNodes {
		matches := 0
		for _, h := range health {
			if sha != "" && h.NodeID == copyNodes[i].ID && BrokerImageComplete(h.BrokerImage, sha, 0, "") {
				matches++
			}
		}
		if matches != 1 {
			copyNodes[i].BrokerVer = "" // unknown/old running bytes must not become a version-only skip
		}
		if copyNodes[i].Agent == clusterupgrade.AgentPresent {
			copyNodes[i].AgentVer = ""
		}
	}
	return clusterupgrade.ComputeWithScope(copyNodes, target, scope)
}

// BrokerImageComplete reads only current runtime status, never the staged file
// or an armed reply. CutID is required after a real cut; an exact already-running
// image can be accepted without creating a new generation.
// origin: hot-update S6 plan §5 / T45
func BrokerImageComplete(s *proto.BrokerImageStatus, sha string, generation uint64, cutID string) bool {
	if s == nil || !validBrokerSHA(sha) || !strings.EqualFold(s.RunningSHA256, sha) || s.Generation == 0 || (generation != 0 && s.Generation != generation) || (cutID != "" && s.CutID != cutID) {
		return false
	}
	if s.Phase != "serving" { // control_ready is still inside the maintenance window
		return false
	}
	switch s.Mode {
	case "single", "sole-voter", "quorum":
		return true
	default:
		return false
	}
}

func validBrokerSHA(sha string) bool {
	b, err := hex.DecodeString(sha)
	return err == nil && len(b) == 32
}

// RollBrokerStep re-resolves leader and voters on EVERY step, including a step
// planned as a follower and a same-version candidate. Unsafe reload is sent once;
// an unknown reply switches to status queries for the rest of the SAME deadline.
// origin: hot-update S6 plan §5 / T38 / T45
func RollBrokerStep(ctx context.Context, io ClusterRollIO, cfg ClusterRollStep) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if io.Health == nil || io.Trigger == nil || !validBrokerSHA(cfg.ExpectSHA256) || cfg.CutID == "" {
		return errors.New("incomplete broker roll configuration")
	}
	switch cfg.Scope {
	case clusterupgrade.ScopeWholeHost, clusterupgrade.ScopeBrokerDaemon:
	default:
		return errors.New("unknown upgrade scope")
	}
	health, err := io.Health(ctx)
	if err != nil {
		return fmt.Errorf("fresh broker topology: %w", err)
	}
	leader, target, voters, err := rollTopology(health, cfg.Step.NodeID, cfg.Voters)
	if err != nil {
		return err
	}
	// Authenticated pre-S6 health is positive legacy evidence. A missing or
	// malformed current image is never a timeout-based downgrade.
	// origin: hot-update S6 internal review rv-cluster-8
	legacy := LegacyBrokerRoll(health)
	if !legacy && cfg.Holder == "" {
		return errors.New("incomplete broker roll holder")
	}
	trigger := func(ctx context.Context, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
		if legacy {
			req.Holder, req.Scope, req.CutID, req.Maintenance = "", "", "", nil
		}
		return io.Trigger(ctx, req)
	}

	for _, voter := range voters {
		if !legacy && (voter.BrokerImage == nil || voter.BrokerImage.Generation == 0) {
			return errors.New("maintenance_observer_unsupported: broker image status is unavailable")
		}
	}
	leader, target, err = transferBrokerRollLeader(ctx, io, cfg, health, legacy, trigger, leader, target, len(voters))
	if err != nil {
		return err
	}

	// A new health read is not a substitute for the broker's continuous role
	// observer; the reload request's holder/cut bind the server-side final guard.
	if !legacy && (target.BrokerImage == nil || target.BrokerImage.Generation == 0) {
		return errors.New("maintenance_observer_unsupported: broker image status is unavailable")
	}
	before := uint64(0)
	if target.BrokerImage != nil {
		before = target.BrokerImage.Generation
	}
	if before == ^uint64(0) {
		return errors.New("broker generation exhausted")
	}
	maintenance, err := prepareBrokerRollMaintenance(ctx, cfg, legacy, trigger, leader, target, before)
	if err != nil {
		return err
	}

	reloadCut := cfg.CutID
	if !legacy && maintenance == nil {
		reloadCut = ""
	}
	resp, triggerErr := trigger(ctx, &proto.ClusterUpgradeReq{Op: "reload", TargetNode: target.NodeID, ToVersion: cfg.ToVersion, ExpectSHA256: cfg.ExpectSHA256, Holder: cfg.Holder, Scope: string(cfg.Scope), CutID: reloadCut, Maintenance: maintenance})
	generation, cut := before+1, cfg.CutID
	if !legacy && maintenance == nil {
		generation, cut = before, ""
	}
	if triggerErr == nil {
		if resp == nil {
			return errors.New("missing broker reload reply")
		}
		if !resp.OK && !resp.AlreadyAtVersion {
			return &BrokerReloadError{Code: resp.Code, Detail: resp.Error}
		}
		if resp.AlreadyAtVersion {
			generation, cut = before, "" // still query SHA; same release alone proves nothing
		} else {
			if !legacy && (resp.NextGeneration == 0 || resp.NextGeneration <= before) {
				return errors.New("reload did not report the next broker generation")
			}
			if !legacy {
				generation = resp.NextGeneration
			}
		}
	}
	err = pollBrokerRoll(ctx, cfg.PollInterval, func() (bool, error) {
		return brokerRollComplete(ctx, io, cfg, generation, cut, legacy), nil
	})
	if err != nil && triggerErr != nil {
		return &BrokerReloadUnknownError{Err: err}
	}
	if err == nil && maintenance != nil {
		maintenance.Phase = brokermaint.MaintEnded
		ended, e := trigger(ctx, &proto.ClusterUpgradeReq{Op: "maintenance", TargetNode: leader.NodeID, Holder: cfg.Holder, Scope: string(cfg.Scope), CutID: cfg.CutID, Maintenance: maintenance})
		if e != nil {
			return e
		}
		if ended == nil || !ended.OK {
			return errors.New("maintenance end did not commit")
		}
	}
	return err
}

// A cut may briefly have no responder or leader. Completion is a predicate:
// keep querying inside the original deadline, never resend the unsafe reload.
// origin: hot-update S6 plan §5 / T45
func brokerRollComplete(ctx context.Context, io ClusterRollIO, cfg ClusterRollStep, generation uint64, cut string, legacy bool) bool {
	health, err := io.Health(ctx)
	if err != nil {
		return false
	} // a cut may briefly have no responder; keep querying, never reload again
	leader, target, _, err := rollTopology(health, cfg.Step.NodeID, cfg.Voters)
	if err != nil {
		return false
	}
	caughtUp := target.AppliedIndex >= leader.AppliedIndex || leader.AppliedIndex-target.AppliedIndex <= 64
	complete := BrokerImageComplete(target.BrokerImage, cfg.ExpectSHA256, generation, cut)
	if legacy {
		complete = BrokerImageComplete(target.BrokerImage, cfg.ExpectSHA256, 0, "") ||
			target.BrokerImage == nil && proto.SameRelease(target.ReleaseVersion, cfg.ToVersion)
	}
	return target.IsVoter && caughtUp && complete
}

// The maintenance ACK must bind this exact reload before the unsafe request.
// origin: hot-update S6 plan §7.1 / T40 / T45
func prepareBrokerRollMaintenance(ctx context.Context, cfg ClusterRollStep, legacy bool, trigger func(context.Context, *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error), leader, target proto.ClusterHealthResp, before uint64) (*brokermaint.Record, error) {
	if !legacy && !BrokerImageComplete(target.BrokerImage, cfg.ExpectSHA256, 0, "") {
		response, err := trigger(ctx, &proto.ClusterUpgradeReq{Op: "maintenance", TargetNode: leader.NodeID, TransferTo: target.NodeID, Holder: cfg.Holder, Scope: string(cfg.Scope), CutID: cfg.CutID})
		if err != nil {
			return nil, fmt.Errorf("maintenance preparation result unknown: %w", err)
		}
		if response == nil || !response.OK || response.Maintenance == nil {
			return nil, errors.New("maintenance_observer_unsupported: required observers did not install the hold")
		}
		record := response.Maintenance
		if record.Phase != brokermaint.MaintActive || record.CutID != cfg.CutID || record.Holder != cfg.Holder || record.Home != target.NodeID || record.Generation != before {
			return nil, errors.New("maintenance record does not bind this reload")
		}
		return record, nil
	}
	return nil, nil
}

// Transfer and confirmation share the step deadline and always re-read topology.
// origin: hot-update S6 plan §5 / T38
func transferBrokerRollLeader(ctx context.Context, io ClusterRollIO, cfg ClusterRollStep, health []proto.ClusterHealthResp, legacy bool, trigger func(context.Context, *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error), leader, target proto.ClusterHealthResp, voterCount int) (proto.ClusterHealthResp, proto.ClusterHealthResp, error) {
	if leader.NodeID == target.NodeID && voterCount > 1 {
		// Acquiring the holder commits a real Raft command. Its leader can
		// answer health before any follower applies that command; absence of a
		// caught-up destination in that sample is not a permanent refusal.
		// Re-read the entire topology under this step's original deadline.
		// origin: hot-update S6 plan §5 / T38 / HARDGATE-B request 3
		var destination string
		var err error
		leader, target, destination, err = waitBrokerRollDestination(ctx, io, cfg, health, legacy)
		if err != nil {
			return leader, target, fmt.Errorf("wait for caught-up voter: %w", err)
		}
		if destination != "" {
			resp, err := trigger(ctx, &proto.ClusterUpgradeReq{Op: "transfer-leader", TargetNode: target.NodeID, TransferTo: destination, Holder: cfg.Holder, Scope: string(cfg.Scope)})
			if err != nil {
				return leader, target, fmt.Errorf("leadership transfer result unknown: %w", err)
			}
			if resp == nil || !resp.OK {
				return leader, target, errors.New("leadership transfer refused")
			}
			// VerifyLeader on the new leader AND a reachable old follower are needed.
			// Membership drift is terminal, rather than a reason to select another set.
			if err := pollBrokerRoll(ctx, cfg.PollInterval, func() (bool, error) {
				fresh, err := io.Health(ctx)
				if errors.Is(err, ErrIncompleteBrokerHealth) {
					return false, nil
				}
				if err != nil {
					return false, err
				}
				l, old, _, err := rollTopology(fresh, target.NodeID, cfg.Voters)
				if errors.Is(err, errBrokerTopologyNotReady) {
					return false, nil
				}
				if err != nil {
					return false, err
				}
				leader, target = l, old
				return l.NodeID == destination && !old.WritableLeaderConfirmed && !old.LeaderContactStale && old.LeaderID == destination, nil
			}); err != nil {
				return leader, target, fmt.Errorf("verify leadership transfer: %w", err)
			}
		}
	}
	return leader, target, nil
}

// A fresh sample supplies both the leader cursor and its candidate voters. A
// saved cursor would authorize a still-lagging follower when another commit
// advances the leader during polling. Membership/capability drift is terminal;
// only an incomplete census, election edge or ordinary apply lag can be retried.
// origin: hot-update S6 plan §5 / T38 / HARDGATE-B request 3
func waitBrokerRollDestination(ctx context.Context, io ClusterRollIO, cfg ClusterRollStep, health []proto.ClusterHealthResp, legacy bool) (leader, target proto.ClusterHealthResp, destination string, err error) {
	first := true
	err = pollBrokerRoll(ctx, cfg.PollInterval, func() (bool, error) {
		if !first {
			fresh, err := io.Health(ctx)
			if errors.Is(err, ErrIncompleteBrokerHealth) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			health = fresh
		}
		first = false
		if err := ctx.Err(); err != nil {
			return false, err
		}
		var voters []proto.ClusterHealthResp
		var err error
		leader, target, voters, err = rollTopology(health, cfg.Step.NodeID, cfg.Voters)
		if errors.Is(err, errBrokerTopologyNotReady) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		for _, voter := range voters {
			if !legacy && (voter.BrokerImage == nil || voter.BrokerImage.Generation == 0) {
				return false, errors.New("maintenance_observer_unsupported: broker image status is unavailable")
			}
		}
		if leader.NodeID != target.NodeID {
			// An independently elected survivor needs no forced transfer, but
			// the old target must also confirm it is following that survivor.
			return !target.WritableLeaderConfirmed && target.LeaderID == leader.NodeID, nil
		}
		for _, voter := range voters {
			if voter.NodeID != target.NodeID && !voter.LeaderContactStale && voter.AppliedIndex >= leader.AppliedIndex {
				destination = voter.NodeID
				return true, nil
			}
		}
		return false, nil
	})
	return
}

// A complete census can temporarily have no verified leader at the transfer
// edge. Retry that election observation; membership drift remains terminal.
// origin: hot-update S6 plan §5 / T38 / internal review rv-cluster-10
var errBrokerTopologyNotReady = errors.New("broker leader or target is not ready")

func rollTopology(health []proto.ClusterHealthResp, targetID string, expected []string) (proto.ClusterHealthResp, proto.ClusterHealthResp, []proto.ClusterHealthResp, error) {
	var leader, target proto.ClusterHealthResp
	var voters []proto.ClusterHealthResp
	seen := map[string]bool{}
	leaders := 0
	for _, h := range health {
		if h.NodeID == "" || seen[h.NodeID] {
			return leader, target, nil, errors.New("ambiguous broker topology")
		}
		seen[h.NodeID] = true
		if h.NodeID == targetID {
			target = h
		}
		if h.IsVoter {
			voters = append(voters, h)
		}
		if h.WritableLeaderConfirmed {
			leader = h
			leaders++
		}
	}
	sort.Slice(voters, func(i, j int) bool { return voters[i].NodeID < voters[j].NodeID })
	want := append([]string(nil), expected...)
	sort.Strings(want)
	if len(want) == 0 || len(want) != len(voters) {
		return leader, target, nil, errors.New("configured voter set changed or is unknown")
	}
	for i, h := range voters {
		if h.NodeID != want[i] {
			return leader, target, nil, errors.New("configured voter set changed")
		}
	}
	if leaders != 1 || !leader.IsVoter || target.NodeID == "" || target.LeaderContactStale {
		return leader, target, nil, errBrokerTopologyNotReady
	}
	return leader, target, voters, nil
}

func pollBrokerRoll(ctx context.Context, interval time.Duration, step func() (bool, error)) error {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	timer := time.NewTicker(interval)
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		done, err := step()
		if err != nil || done {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// CallBrokerAdmin preserves the CLI transport edge while its transaction
// reply ownership and cancellation live with the control-plane transport.
// origin: hot-update S8 plan §7.1; S8 internal review activation-and-route-migration F7
func CallBrokerAdmin(ctx context.Context, path string, req adminsock.Request) (*adminsock.Response, error) {
	return upgradetransport.CallBrokerAdmin(ctx, path, req)
}

// ReloadLocalBroker sends OpReload only once. Deferred transfers and census read
// errors remain refusal errors; an armed response is followed by runtime reads.
// The broker owns candidate/prev reader validation before arming the cut.
// origin: hot-update S6 plan §5 / T45
func ReloadLocalBroker(ctx context.Context, path, version, sha string) (*proto.BrokerImageStatus, error) {
	if !validBrokerSHA(sha) {
		return nil, errors.New("expect-sha256 must be a 64-digit SHA256")
	}
	ctx, cancel := context.WithTimeout(ctx, upgradebudget.ComponentHandover)
	defer cancel()
	before, err := CallBrokerAdmin(ctx, path, adminsock.Request{Op: adminsock.OpRuntime})
	if err != nil {
		return nil, err
	}
	if before.Code != "" || before.Error != "" || before.Runtime == nil || before.Runtime.BrokerImage == nil {
		return nil, errors.New("broker runtime image is unavailable")
	}
	old := before.Runtime.BrokerImage
	// A missing lineage must fail before the unsafe request. Zero generation
	// cannot be used as a guessed predecessor for completion queries.
	// origin: hot-update S6 plan §5 / T45
	if old.Generation == 0 {
		return nil, errors.New("broker runtime generation is unavailable")
	}
	if old.Generation == ^uint64(0) {
		return nil, errors.New("broker generation exhausted")
	}
	resp, sentErr := CallBrokerAdmin(ctx, path, adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: version, ExpectSHA256: sha})
	generation, cut := old.Generation+1, ""
	if sentErr == nil {
		if !resp.OK && !resp.AlreadyAtVersion {
			return nil, &BrokerReloadError{Code: resp.Code, Detail: resp.Error}
		}
		if resp.AlreadyAtVersion {
			generation = old.Generation
		} else {
			if resp.NextGeneration <= old.Generation || resp.BrokerImage == nil || resp.BrokerImage.CutID == "" {
				return nil, errors.New("reload did not report a broker cut and next generation")
			}
			generation, cut = resp.NextGeneration, resp.BrokerImage.CutID
		}
	}
	var status *proto.BrokerImageStatus
	err = pollBrokerRoll(ctx, 0, func() (bool, error) {
		status = localBrokerRollStatus(ctx, path)
		return BrokerImageComplete(status, sha, generation, cut), nil
	})
	if err != nil {
		return nil, &BrokerReloadUnknownError{Err: err}
	}
	return status, err
}

// A temporarily unavailable admin view is an incomplete status sample. The
// caller polls with its existing deadline after sending reload exactly once.
// origin: hot-update S6 plan §5 / T45
func localBrokerRollStatus(ctx context.Context, path string) *proto.BrokerImageStatus {
	r, err := CallBrokerAdmin(ctx, path, adminsock.Request{Op: adminsock.OpRuntime})
	if err != nil || r.Code != "" || r.Error != "" || r.Runtime == nil {
		return nil
	}
	return r.Runtime.BrokerImage
}

// QueryBrokerRollHealth collects authenticated runtime replies inside one RPC
// budget. Subscribe/flush/decode failures are errors, never an empty safe census.
// It honors the connection's private inbox prefix and joins subscription cleanup.
// origin: hot-update S6 plan §5 / T38
func QueryBrokerRollHealth(ctx context.Context, nc *nats.Conn, actor string) ([]proto.ClusterHealthResp, error) {
	return QueryBrokerRollHealthForVoters(ctx, nc, actor, nil)
}

func QueryBrokerRollHealthForVoters(ctx context.Context, nc *nats.Conn, actor string, expected []string) ([]proto.ClusterHealthResp, error) {
	ctx, cancel := context.WithTimeout(ctx, upgradebudget.RequestLatency)
	defer cancel()
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushWithContext(ctx); err != nil {
		return nil, err
	}
	if last := nc.LastError(); last != nil && strings.Contains(last.Error(), inbox) {
		return nil, fmt.Errorf("broker health inbox refused: %w", last)
	}
	if err := nc.PublishRequest(proto.SubjCtrlClusterHealth(actor), inbox, nil); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var health []proto.ClusterHealthResp
	for {
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			if ctx.Err() != nil {
				if len(health) > 0 && len(expected) == 0 && errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return health, nil
				}
				if len(health) > 0 && len(expected) > 0 {
					return nil, ErrIncompleteBrokerHealth
				}
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("broker health probe: %w", err)
		}
		if msg.Header.Get("Status") == "503" {
			return nil, nats.ErrNoResponders
		}
		var h proto.ClusterHealthResp
		if err := json.Unmarshal(msg.Data, &h); err != nil {
			return nil, fmt.Errorf("decode broker health: %w", err)
		}
		if h.NodeID == "" || seen[h.NodeID] {
			return nil, errors.New("ambiguous broker health reply")
		}
		seen[h.NodeID] = true
		health = append(health, h)
		complete := len(expected) > 0
		for _, id := range expected {
			if !seen[id] {
				complete = false
			}
		}
		if complete {
			return health, nil
		}
	}
}

// BrokerReloadError retains the broker's refusal namespace for CLI exit mapping.
// A successful census with live transfers is a transient deferral; a census read
// failure is a store error and must never be translated into the same outcome.
// origin: hot-update S6 plan §5 / T45
type BrokerReloadError struct{ Code, Detail string }

func (e *BrokerReloadError) Error() string {
	return fmt.Sprintf("broker reload refused: %s %s", e.Code, e.Detail)
}

// BrokerReloadUnknownError distinguishes a sent mutation from a pre-send
// refusal. Automation must query its outcome rather than replay the reload.
// origin: hot-update S6 plan §8.2 / T36 / T45
type BrokerReloadUnknownError struct{ Err error }

func (e *BrokerReloadUnknownError) Error() string {
	return fmt.Sprintf("control_result_unknown: reload sent; status not confirmed: %v", e.Err)
}
func (e *BrokerReloadUnknownError) Unwrap() error { return e.Err }

// Explicit old-version health permits the established cold roll, with exactly
// the old seven signed fields. Silence and missing current status are not proof.
// origin: hot-update S6 plan §9.2 / rv-cluster-8
func LegacyBrokerRoll(health []proto.ClusterHealthResp) bool {
	for _, h := range health {
		if h.IsVoter && h.SchemaVersion > 0 && h.SchemaVersion < 8 && h.BrokerImage == nil {
			return true
		}
	}
	return false
}

// A partial census is retriable during an election; a positive changed
// configuration, duplicate identity or signature error remains terminal.
// origin: hot-update S6 internal review rv-cluster-10
var ErrIncompleteBrokerHealth = errors.New("broker health census incomplete")
