// Package infraunit parses and validates the effective systemd unit policy.
// Host authorization, journal ownership and systemd effects remain in infrahost.
// origin: hot-update S9 plan §10.1 / S9-SLIM
package infraunit

import (
	"errors"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
)

const BootstrapCommand = "/usr/local/libexec/tether-infra-helper bootstrap"

var (
	ErrUnitInvalid    = errors.New("infra_config_invalid")
	ErrUnitPermission = errors.New("infra_permission_denied")
	ErrUnitNotEnabled = errors.New("infra_not_enabled: run " + BootstrapCommand)
	ErrUnitNotReady   = errors.New("infra_not_ready")
	ErrUnitBudget     = errors.New("infra_budget_exceeded")
	ErrUnitUnknown    = errors.New("infra_result_unknown")
)

// UnitTarget is a policy identity, not a caller-controlled systemd name.
type UnitTarget uint8

const (
	UnitNATS          UnitTarget = 1
	UnitCaddy         UnitTarget = 2
	UnitNATSWarm      UnitTarget = 3
	UnitCaddyWarm     UnitTarget = 4
	UnitHelperSocket  UnitTarget = 5
	UnitHelperService UnitTarget = 6
)

func UnitName(target UnitTarget) (string, error) {
	switch target {
	case UnitNATS:
		return "nats-server.service", nil
	case UnitCaddy:
		return "caddy.service", nil
	case UnitNATSWarm:
		return "tether-infra-nats-warm.service", nil
	case UnitCaddyWarm:
		return "tether-infra-caddy-warm.service", nil
	case UnitHelperSocket:
		return "tether-infra-helper.socket", nil
	case UnitHelperService:
		// A template has no loadable incarnation. This audit instance is read
		// only; the actual socket worker is audited again after peer verification.
		return "tether-infra-helper@audit.service", nil
	default:
		return "", ErrUnitInvalid
	}
}

// EffectiveUnit contains the complete audited surface, including properties
// which can bypass a seemingly safe User= or replace the launcher's arguments.
// Exec entries are Condition/StartPre/Start/StartPost/Reload/Stop/StopPost, in
// that order, using the Ex properties so privilege prefixes remain visible.
// The policy and observation are memory-only; they are not a new wire codec.
type EffectiveUnit struct {
	Name, LoadState, UnitFileState, ActiveState, SubState                                        string
	FragmentPath, DropInPaths, NeedDaemonReload                                                  string
	User, Group, SupplementaryGroups, DynamicUser                                                string
	Type, Restart, RestartUSec, RuntimeMaxUSec                                                   string
	Exec                                                                                         [7]string
	Environment, EnvironmentFiles, PassEnvironment, UnsetEnvironment                             string
	ExecSearchPath, WorkingDirectory, RootDirectory, RootImage                                   string
	AmbientCapabilities, CapabilityBoundingSet, NoNewPrivileges                                  string
	BindPaths, BindReadOnlyPaths, ReadWritePaths                                                 string
	ConfigurationDirectory, StateDirectory, RuntimeDirectory                                     string
	LoadCredential, LoadCredentialEncrypted, SetCredential, SetCredentialEncrypted               string
	PAMName, Sockets, KillMode                                                                   string
	Requires, Wants, Requisite, BindsTo, PartOf, Conflicts, Before, After, Triggers, TriggeredBy string
	Accept, Listen, SocketUser, SocketGroup, SocketMode, DirectoryMode, MaxConnections           string
}

type UnitPolicy struct {
	Target   UnitTarget
	Legacy   bool          // Only the original direct stock main may run as root/on-failure.
	Expected EffectiveUnit // Supplied by root-owned policy, never HostRequest.
}

// Unit/bootstrap evidence shares these predicates with its host adapter. Keep
// the exact physical set checks in the unit leaf alongside effective policy.
// origin: hot-update S9 plan §6.4 / WIRE-2 R11 (host budget)
func ProcessKnown(p infracontract.Process) bool {
	return p.BootID != "" && p.PID > 0 && p.StartTicks > 0 && p.InvocationID != "" &&
		p.ExeDevice != 0 && p.ExeInode != 0 && p.ExeSHA256 != (infracontract.Digest{}) && p.Cgroup != ""
}

func BusinessCensus(c infracontract.Census) bool {
	if !c.Complete || c.Generation == 0 || c.Digest == (infracontract.Digest{}) || len(c.Processes) == 0 {
		return false
	}
	for i, p := range c.Processes {
		if !ProcessKnown(p) {
			return false
		}
		for _, previous := range c.Processes[:i] {
			if p.BootID == previous.BootID && p.PID == previous.PID {
				return false
			}
		}
	}
	return true
}

func SameProcesses(before, after []infracontract.Process) bool {
	if len(before) != len(after) {
		return false
	}
	for _, old := range before {
		found := false
		for _, current := range after {
			found = found || old == current
		}
		if !found {
			return false
		}
	}
	return true
}

func CheckBudget(deadline infracontract.Deadline, stop infracontract.Stop) error {
	select {
	case <-stop:
		return ErrUnitBudget
	default:
	}
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return ErrUnitBudget
	}
	return nil
}

func ContainsProcess(c infracontract.Census, p infracontract.Process) bool {
	for _, current := range c.Processes {
		if current == p {
			return true
		}
	}
	return false
}
