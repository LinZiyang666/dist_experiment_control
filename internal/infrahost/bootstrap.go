// Package infrahost implements the closed infrastructure host effects. The
// worker owns authorization and the journal; these backends never accept an
// arbitrary unit name, command, or configuration from a socket request.
package infrahost

import (
	"crypto/sha256"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infraunit"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// The host API retains the leaf's exact types and error identities. Policy
// parsing owns no host effects and cannot import the worker that consumes it.
// origin: hot-update S9 plan §10.1 / S9-SLIM
const BootstrapCommand = infraunit.BootstrapCommand

var (
	ErrUnitInvalid    = infraunit.ErrUnitInvalid
	ErrUnitPermission = infraunit.ErrUnitPermission
	ErrUnitNotEnabled = infraunit.ErrUnitNotEnabled
	ErrUnitNotReady   = infraunit.ErrUnitNotReady
	ErrUnitBudget     = infraunit.ErrUnitBudget
	ErrUnitUnknown    = infraunit.ErrUnitUnknown
)

type UnitTarget = infraunit.UnitTarget

const (
	UnitNATS          = infraunit.UnitNATS
	UnitCaddy         = infraunit.UnitCaddy
	UnitNATSWarm      = infraunit.UnitNATSWarm
	UnitCaddyWarm     = infraunit.UnitCaddyWarm
	UnitHelperSocket  = infraunit.UnitHelperSocket
	UnitHelperService = infraunit.UnitHelperService
)

func UnitName(target UnitTarget) (string, error) { return infraunit.UnitName(target) }

type EffectiveUnit = infraunit.EffectiveUnit
type UnitPolicy = infraunit.UnitPolicy

func ParseEffectiveUnit(target UnitTarget, data []byte) (EffectiveUnit, error) {
	return infraunit.ParseEffectiveUnit(target, data)
}

func ValidateEffectiveUnit(unit EffectiveUnit, policy UnitPolicy) error {
	return infraunit.ValidateEffectiveUnit(unit, policy)
}

func ManagedUnitDropIn(component infracontract.Component) ([]byte, error) {
	return infraunit.ManagedUnitDropIn(component)
}

type UnitManager interface {
	Read(UnitTarget, infracontract.Deadline, infracontract.Stop) (EffectiveUnit, error)
	ReadHelper(infracontract.Process, infracontract.Deadline, infracontract.Stop) (EffectiveUnit, error)
	Reload(infracontract.Deadline, infracontract.Stop) error
	Start(UnitTarget, infracontract.Deadline, infracontract.Stop) error
	Fence(UnitTarget, infracontract.Deadline, infracontract.Stop) error
}

// BootstrapSetup is composed by P40 from P03/P04/P07. Preflight must observe
// linux/amd64, systemd, pidfd/cgroup/nft/IPv6 and trusted policy/files before
// effects. Initialize is idempotent and must reject damaged existing keys.
// Neither Initialize nor InstallManagement may touch a business unit or store.
type BootstrapSetup interface {
	Preflight(infracontract.Deadline, infracontract.Stop) (BootstrapEnvironment, error)
	Initialize(infracontract.Deadline, infracontract.Stop) error
	InstallManagement(infracontract.Deadline, infracontract.Stop) error
	VerifyHelper(infracontract.Deadline, infracontract.Stop) (HelperVerification, error)
}

type BootstrapEnvironment struct {
	UID                                                              uint32
	Systemd, PidFD, Cgroup, NFT, IPv6, PolicyTrusted, FilesInstalled bool
}

type HelperVerification struct {
	Version uint16
	Running infracontract.Process
	Catalog infracontract.Digest
}

type BootstrapDependencies struct {
	Manager  UnitManager
	Setup    BootstrapSetup
	Physical infracontract.Host
	Policies []UnitPolicy
}

type BootstrapStage uint8

const (
	FilesInstalled        BootstrapStage = 1
	SocketActive          BootstrapStage = 2
	HelperVersionVerified BootstrapStage = 3
	EffectiveUnitVerified BootstrapStage = 4
	RuntimeReady          BootstrapStage = 5
)

type BootstrapReceipt struct {
	Stage         BootstrapStage
	Helper        infracontract.Process
	HelperCatalog infracontract.Digest
	Business      []infracontract.Process
}

func unitBudget(deadline infracontract.Deadline, stop infracontract.Stop) error {
	return infraunit.CheckBudget(deadline, stop)
}

// Bootstrap activates only the management socket. A submitted start job does
// not prove activation, and bootstrap cannot certify business runtime readiness.
// origin: hot-update S9 plan §6.2 / §6.4 / T13
func Bootstrap(deps BootstrapDependencies, deadline infracontract.Deadline, stop infracontract.Stop) (BootstrapReceipt, error) {
	var receipt BootstrapReceipt
	if deps.Manager == nil || deps.Setup == nil || deps.Physical == nil {
		return receipt, infracontract.ErrContractStub
	}
	if err := unitBudget(deadline, stop); err != nil {
		return receipt, err
	}
	if err := unitPlatform(); err != nil {
		return receipt, err
	}
	environment, err := deps.Setup.Preflight(deadline, stop)
	if err != nil {
		return receipt, err
	}
	if environment.UID != 0 || !environment.PolicyTrusted {
		return receipt, ErrUnitPermission
	}
	if !environment.Systemd || !environment.PidFD || !environment.Cgroup || !environment.NFT || !environment.IPv6 {
		return receipt, infracontract.ErrUnsupported
	}
	if !environment.FilesInstalled {
		return receipt, ErrUnitNotEnabled
	}
	// Audit all four fixed targets before keygen/reload/start, not just MainPID.
	if err := auditBootstrapUnits(deps, deadline, stop); err != nil {
		return receipt, err
	}
	before, err := deps.Physical.Inspect(infracontract.ID{}, deadline)
	if err != nil || !validBusinessCensus(before) {
		return receipt, infracontract.ErrIncomplete
	}
	receipt.Stage = FilesInstalled
	receipt.Business = append([]infracontract.Process(nil), before.Processes...)
	for _, effect := range []func(infracontract.Deadline, infracontract.Stop) error{
		deps.Setup.Initialize, deps.Setup.InstallManagement, deps.Manager.Reload,
	} {
		if err := unitBudget(deadline, stop); err != nil {
			return receipt, err
		}
		if err := effect(deadline, stop); err != nil {
			return receipt, err
		}
	}
	if err := auditBootstrapUnits(deps, deadline, stop); err != nil {
		return receipt, err
	}
	if err := deps.Manager.Start(UnitHelperSocket, deadline, stop); err != nil {
		return receipt, err
	}
	if err := awaitManagementSocket(deps.Manager, deadline, stop); err != nil {
		return receipt, err
	}
	receipt.Stage = SocketActive
	helper, err := deps.Setup.VerifyHelper(deadline, stop)
	if err != nil || helper.Version != 1 || helper.Catalog == (infracontract.Digest{}) || !unitProcessKnown(helper.Running) {
		return receipt, ErrUnitNotReady
	}
	receipt.Helper, receipt.HelperCatalog = helper.Running, helper.Catalog
	receipt.Stage = HelperVersionVerified
	if err := auditBootstrapUnits(deps, deadline, stop); err != nil {
		return receipt, err
	}
	actualHelper, err := deps.Manager.ReadHelper(helper.Running, deadline, stop)
	if err != nil {
		return receipt, err
	}
	for _, policy := range deps.Policies {
		if policy.Target == UnitHelperService {
			if err := ValidateEffectiveUnit(actualHelper, policy); err != nil {
				return receipt, err
			}
		}
	}
	after, err := deps.Physical.Inspect(infracontract.ID{}, deadline)
	if err != nil || !validBusinessCensus(after) || !infraunit.SameProcesses(before.Processes, after.Processes) {
		return receipt, infracontract.ErrIncomplete
	}
	if err := unitBudget(deadline, stop); err != nil {
		return receipt, err
	}
	receipt.Stage = EffectiveUnitVerified
	return receipt, nil
}

func awaitManagementSocket(manager UnitManager, deadline infracontract.Deadline, stop infracontract.Stop) error {
	for {
		if err := unitBudget(deadline, stop); err != nil {
			return ErrUnitNotEnabled
		}
		socket, err := manager.Read(UnitHelperSocket, deadline, stop)
		if err != nil {
			return err
		}
		// origin: hot-update S9 plan §6.4 / T13 — job acceptance is not activation.
		if socket.ActiveState == "active" && socket.SubState == "listening" {
			return nil
		}
		if socket.ActiveState == "failed" {
			return ErrUnitNotEnabled
		}
		remaining, err := controlbudget.Remaining(deadline)
		if err != nil {
			return ErrUnitNotEnabled
		}
		pause := time.NewTimer(min(remaining, 10*time.Millisecond))
		select {
		case <-stop:
			pause.Stop()
			return ErrUnitBudget
		case <-pause.C:
		}
	}
}

func auditBootstrapUnits(deps BootstrapDependencies, deadline infracontract.Deadline, stop infracontract.Stop) error {
	if len(deps.Policies) != 4 {
		return ErrUnitInvalid
	}
	seen := [7]bool{}
	for _, policy := range deps.Policies {
		switch policy.Target {
		case UnitNATS, UnitCaddy, UnitHelperSocket, UnitHelperService:
		default:
			return ErrUnitInvalid
		}
		if seen[policy.Target] {
			return ErrUnitInvalid
		}
		seen[policy.Target] = true
		if err := unitBudget(deadline, stop); err != nil {
			return err
		}
		unit, err := deps.Manager.Read(policy.Target, deadline, stop)
		if err != nil {
			return err
		}
		if err := ValidateEffectiveUnit(unit, policy); err != nil {
			return err
		}
	}
	return nil
}

func unitProcessKnown(p infracontract.Process) bool   { return infraunit.ProcessKnown(p) }
func validBusinessCensus(c infracontract.Census) bool { return infraunit.BusinessCensus(c) }

// VerifyBootstrapRuntime is called only after the non-root driver's actual
// auth/WSS/asset readiness. Files, socket activation and helper version alone
// must not advertise InfraUpgradeVersion or certify a running business image.
// origin: hot-update S9 plan §2.3 / §6.4 / T13
func VerifyBootstrapRuntime(receipt BootstrapReceipt, ready infracontract.Readiness) (BootstrapReceipt, error) {
	if receipt.Stage != EffectiveUnitVerified || !unitProcessKnown(receipt.Helper) || receipt.HelperCatalog == (infracontract.Digest{}) ||
		!ready.Complete || !ready.Seal.Complete || ready.Seal.Revision == 0 ||
		ready.Seal.Accounts == (infracontract.Digest{}) || ready.Seal.Assets == (infracontract.Digest{}) ||
		ready.Seal.Participants == (infracontract.Digest{}) || ready.Topology == (infracontract.Digest{}) ||
		ready.Auth == (infracontract.Digest{}) || ready.WSS == (infracontract.Digest{}) || !unitProcessKnown(ready.Running) {
		return receipt, ErrUnitNotReady
	}
	receipt.Stage = RuntimeReady
	return receipt, nil
}

// StartupFence is a projection of P04's durable journal, not a second journal.
// P04 must fsync every CompareAndSwap before the associated external effect.
// P06's LaunchClaim persists ClaimStartup's increment before executing stock.
type StartupFence struct {
	Tx, Domain       infracontract.ID
	Step             uint32
	Component        infracontract.Component
	Revision         uint64
	Closed           bool
	Deadline         infracontract.Deadline
	Boots            uint32
	OldExited        bool
	StorageFinalized infracontract.Digest
	Warm             infracontract.Process
	WarmPending      bool
	ReturnProof      infracontract.Digest
	Retirement       infracontract.Census
}

func fenceMatches(f StartupFence, r infracontract.HostRequest) bool {
	return f.Tx == r.Tx && f.Domain == r.Domain && f.Step == r.Step && f.Component == r.Component &&
		f.Revision != 0 && f.Tx != (infracontract.ID{}) && f.Domain != (infracontract.ID{})
}

// ReleaseStartup preserves the original boot deadline and refuses a snapshot
// copied while the old ACME writer could still mutate the current directory.
// origin: hot-update S9 plan §5.3 / T13
func ReleaseStartup(f StartupFence, r infracontract.HostRequest) (StartupFence, error) {
	if !fenceMatches(f, r) || !f.Closed || !f.OldExited ||
		f.StorageFinalized == (infracontract.Digest{}) || f.StorageFinalized != r.Payload {
		return f, ErrUnitNotReady
	}
	if err := unitBudget(f.Deadline, nil); err != nil || f.Boots >= upgradebudget.BootBudget {
		return f, ErrUnitBudget
	}
	f.Closed = false
	f.Revision++
	return f, nil
}

// ClaimStartup never renews authority on a systemd Restart=always retry. Even
// a release written before a failing boot cannot manufacture a fourth boot.
// origin: hot-update S9 plan §5.3 / §6.2 / T13
func ClaimStartup(f StartupFence) (StartupFence, error) {
	if f.Closed || f.Revision == 0 || f.Tx == (infracontract.ID{}) || f.Domain == (infracontract.ID{}) ||
		(f.Component != infracontract.NATS && f.Component != infracontract.Caddy) {
		return f, ErrUnitNotReady
	}
	if err := unitBudget(f.Deadline, nil); err != nil || f.Boots >= upgradebudget.BootBudget {
		return f, ErrUnitBudget
	}
	f.Boots++
	f.Revision++
	return f, nil
}

type UnitStateStore interface {
	Load(infracontract.Component, infracontract.Deadline) (StartupFence, error)
	CompareAndSwap(StartupFence, StartupFence, infracontract.HostRequest, infracontract.Deadline) (infracontract.HostReceipt, error)
}

type UnitDependencies struct {
	Manager  UnitManager
	State    UnitStateStore
	Physical infracontract.Host
	Policies []UnitPolicy
	// Retire is P02's exact pidfd action, composed by P40 after the P04
	// zero-responsibility check below. It receives the original signed request;
	// rewriting its Op to SignalGraceful would invalidate the canonical binding.
	Retire func(infracontract.HostRequest, infracontract.Deadline, infracontract.Stop) (infracontract.HostReceipt, error)
}

type unitHost struct{ deps UnitDependencies }

func NewUnitHost(deps UnitDependencies) (infracontract.Host, error) {
	if deps.Manager == nil || deps.State == nil || deps.Physical == nil || deps.Retire == nil {
		return nil, infracontract.ErrContractStub
	}
	// Copy the policy slice so a caller cannot mutate an audited target after
	// construction. No policy record contains a map or caller-controlled argv.
	deps.Policies = append([]UnitPolicy(nil), deps.Policies...)
	seen := [7][2]bool{}
	for _, policy := range deps.Policies {
		mode := 0
		if policy.Legacy {
			mode = 1
		}
		if _, err := UnitName(policy.Target); err != nil || seen[policy.Target][mode] {
			return nil, ErrUnitInvalid
		}
		seen[policy.Target][mode] = true
		if err := ValidateEffectiveUnit(policy.Expected, policy); err != nil {
			return nil, err
		}
	}
	return &unitHost{deps: deps}, nil
}

func (h *unitHost) Inspect(id infracontract.ID, deadline infracontract.Deadline) (infracontract.Census, error) {
	return h.deps.Physical.Inspect(id, deadline)
}

// Do is registered only for FenceStart/StartWarm/ReleaseStart/RetireWarm.
// P04 owns request authorization/replay and must never hold its journal lock
// while this method submits a job or P06 makes a nested LaunchClaim.
// origin: hot-update S9 plan §6.1 / §10.3 / T13
func (h *unitHost) Do(r infracontract.HostRequest, deadline infracontract.Deadline, stop infracontract.Stop) (infracontract.HostReceipt, error) {
	var receipt infracontract.HostReceipt
	if err := unitBudget(deadline, stop); err != nil {
		return receipt, err
	}
	if err := unitPlatform(); err != nil {
		return receipt, err
	}
	requestBytes, err := infracontract.EncodeRequest(r)
	if err != nil {
		return receipt, ErrUnitInvalid
	}
	if r.Component != infracontract.NATS && r.Component != infracontract.Caddy {
		return receipt, ErrUnitInvalid
	}
	switch r.Op {
	case infracontract.FenceStart, infracontract.StartWarm, infracontract.ReleaseStart, infracontract.RetireWarm:
	// origin: hot-update S9 plan §10.2 / HOSTOP-SWITCHES
	case infracontract.Inspect, infracontract.Stage, infracontract.Promote, infracontract.SignalLDM,
		infracontract.SignalStop, infracontract.SignalGraceful, infracontract.SwitchIngress,
		infracontract.FinalizeStorage, infracontract.RecoverHost, infracontract.RotatePolicy,
		infracontract.UpdateSupport, infracontract.LaunchClaim, infracontract.LaunchAck, infracontract.VerifyIngress:
		return receipt, ErrUnitInvalid
	default:
		return receipt, ErrUnitInvalid
	}
	before, err := h.deps.State.Load(r.Component, deadline)
	if err != nil {
		return receipt, err
	}
	if !fenceMatches(before, r) {
		return receipt, ErrUnitNotReady
	}
	after := before
	target := UnitNATS
	if r.Component == infracontract.Caddy {
		target = UnitCaddy
	}
	switch r.Op {
	case infracontract.FenceStart:
		if !unitProcessKnown(r.Expected) {
			return receipt, infracontract.ErrIncomplete
		}
		census, err := h.deps.Physical.Inspect(r.Domain, deadline) // origin: S9 internal review rv-infra F05
		if err != nil || !validBusinessCensus(census) || !infraunit.ContainsProcess(census, r.Expected) {
			return receipt, infracontract.ErrIncomplete
		}
		if err := h.auditCurrent(target, deadline, stop); err != nil {
			return receipt, err
		}
		after.Closed = true
		after.Revision++
	case infracontract.StartWarm:
		if !before.Closed || before.WarmPending || before.Warm != (infracontract.Process{}) {
			return receipt, ErrUnitNotReady
		}
		if err := unitBudget(before.Deadline, stop); err != nil {
			return receipt, err
		}
		if r.Component == infracontract.NATS {
			// S9 NATS uses real surviving servers, never a sibling store opener.
			return receipt, infracontract.ErrUnsupported
		}
		target = UnitCaddyWarm
		after.WarmPending = true
		after.Revision++
	case infracontract.ReleaseStart:
		after, err = ReleaseStartup(before, r)
		if err != nil {
			return receipt, err
		}
	case infracontract.RetireWarm:
		c := before.Retirement
		if !unitProcessKnown(before.Warm) || r.Expected != before.Warm ||
			r.ReturnProof == (infracontract.Digest{}) || r.ReturnProof != before.ReturnProof ||
			!validBusinessCensus(c) || !infraunit.ContainsProcess(c, before.Warm) || c.OldOwnerPending != 0 ||
			r.WarmCensus != c.Digest {
			return receipt, ErrUnitNotReady
		}
		// P02 revalidates the expected pidfd. A reused unit name is never a
		// retirement identity; this cannot accidentally stop a successor.
		return h.deps.Retire(r, deadline, stop)
	case infracontract.Inspect, infracontract.Stage, infracontract.Promote, infracontract.SignalLDM,
		infracontract.SignalStop, infracontract.SignalGraceful, infracontract.SwitchIngress,
		infracontract.FinalizeStorage, infracontract.RecoverHost, infracontract.RotatePolicy,
		infracontract.UpdateSupport, infracontract.LaunchClaim, infracontract.LaunchAck, infracontract.VerifyIngress:
		// Unreachable: the admission switch above refuses these before any state
		// load. They are named, not defaulted, so a new HostOp must be classified
		// here as well instead of silently falling through to the CAS below.
		return receipt, ErrUnitInvalid
	}
	if r.Op != infracontract.FenceStart {
		if err := h.audit(target, deadline, stop); err != nil {
			return receipt, err
		}
	}
	receipt, err = h.deps.State.CompareAndSwap(before, after, r, deadline)
	if err != nil {
		return receipt, err
	}
	if !receipt.Applied || receipt.Tx != r.Tx || receipt.Sequence != r.Sequence || receipt.Op != r.Op ||
		receipt.Revision == 0 || receipt.Journal == (infracontract.Digest{}) || receipt.RequestDigest != infracontract.Digest(sha256.Sum256(requestBytes)) {
		return receipt, ErrUnitUnknown
	}
	if r.Op == infracontract.FenceStart {
		// The durable fence comes first. Replacing ExecStart and reloading the
		// manager then makes Restart=always pass through P06's LaunchClaim.
		if err := h.deps.Manager.Fence(target, deadline, stop); err != nil {
			receipt.Applied, receipt.Code = false, "infra_result_unknown"
			return receipt, ErrUnitUnknown
		}
		if err := h.audit(target, deadline, stop); err != nil {
			return receipt, err
		}
		return receipt, nil
	}
	// Intent is durable before job submission. A lost job reply stays unknown;
	// only P04's replay/census may resolve it, never an automatic second Start.
	if err := h.deps.Manager.Start(target, deadline, stop); err != nil {
		receipt.Applied = false
		receipt.Code = "infra_result_unknown"
		return receipt, ErrUnitUnknown
	}
	return receipt, nil
}

func (h *unitHost) auditCurrent(target UnitTarget, deadline infracontract.Deadline, stop infracontract.Stop) error {
	unit, err := h.deps.Manager.Read(target, deadline, stop)
	if err != nil {
		return err
	}
	for _, policy := range h.deps.Policies {
		if policy.Target == target && ValidateEffectiveUnit(unit, policy) == nil {
			return nil
		}
	}
	return ErrUnitInvalid
}

func (h *unitHost) audit(target UnitTarget, deadline infracontract.Deadline, stop infracontract.Stop) error {
	for _, policy := range h.deps.Policies {
		if policy.Target == target && !policy.Legacy {
			unit, err := h.deps.Manager.Read(target, deadline, stop)
			if err != nil {
				return err
			}
			return ValidateEffectiveUnit(unit, policy)
		}
	}
	return ErrUnitInvalid
}
