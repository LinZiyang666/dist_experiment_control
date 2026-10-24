//go:build linux

package infraunit

import (
	"bytes"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

type unitProperty struct {
	name  string
	value *string
}

// The same closed list drives both show's filter and the required-field parser.
// Missing fields (including Ex properties on older systemd) are unsupported,
// rather than falling back to the lossy ordinary ExecStart representation.
func unitProperties(u *EffectiveUnit) []unitProperty {
	return []unitProperty{
		{"Id", &u.Name}, {"LoadState", &u.LoadState}, {"UnitFileState", &u.UnitFileState},
		{"ActiveState", &u.ActiveState}, {"SubState", &u.SubState},
		{"FragmentPath", &u.FragmentPath}, {"DropInPaths", &u.DropInPaths}, {"NeedDaemonReload", &u.NeedDaemonReload},
		{"User", &u.User}, {"Group", &u.Group}, {"SupplementaryGroups", &u.SupplementaryGroups}, {"DynamicUser", &u.DynamicUser},
		{"Type", &u.Type}, {"Restart", &u.Restart}, {"RestartUSec", &u.RestartUSec}, {"RuntimeMaxUSec", &u.RuntimeMaxUSec},
		{"ExecConditionEx", &u.Exec[0]}, {"ExecStartPreEx", &u.Exec[1]}, {"ExecStartEx", &u.Exec[2]},
		{"ExecStartPostEx", &u.Exec[3]}, {"ExecReloadEx", &u.Exec[4]}, {"ExecStopEx", &u.Exec[5]}, {"ExecStopPostEx", &u.Exec[6]},
		{"Environment", &u.Environment}, {"EnvironmentFiles", &u.EnvironmentFiles},
		{"PassEnvironment", &u.PassEnvironment}, {"UnsetEnvironment", &u.UnsetEnvironment},
		{"ExecSearchPath", &u.ExecSearchPath}, {"WorkingDirectory", &u.WorkingDirectory},
		{"RootDirectory", &u.RootDirectory}, {"RootImage", &u.RootImage},
		{"AmbientCapabilities", &u.AmbientCapabilities}, {"CapabilityBoundingSet", &u.CapabilityBoundingSet},
		{"NoNewPrivileges", &u.NoNewPrivileges}, {"BindPaths", &u.BindPaths},
		{"BindReadOnlyPaths", &u.BindReadOnlyPaths}, {"ReadWritePaths", &u.ReadWritePaths},
		{"ConfigurationDirectory", &u.ConfigurationDirectory}, {"StateDirectory", &u.StateDirectory},
		{"RuntimeDirectory", &u.RuntimeDirectory}, {"LoadCredential", &u.LoadCredential},
		{"LoadCredentialEncrypted", &u.LoadCredentialEncrypted}, {"SetCredential", &u.SetCredential},
		{"SetCredentialEncrypted", &u.SetCredentialEncrypted}, {"PAMName", &u.PAMName},
		{"Sockets", &u.Sockets}, {"KillMode", &u.KillMode},
		{"Requires", &u.Requires}, {"Wants", &u.Wants}, {"Requisite", &u.Requisite}, {"BindsTo", &u.BindsTo},
		{"PartOf", &u.PartOf}, {"Conflicts", &u.Conflicts}, {"Before", &u.Before}, {"After", &u.After},
		{"Triggers", &u.Triggers}, {"TriggeredBy", &u.TriggeredBy},
		{"Accept", &u.Accept}, {"Listen", &u.Listen}, {"SocketUser", &u.SocketUser}, {"SocketGroup", &u.SocketGroup},
		{"SocketMode", &u.SocketMode}, {"DirectoryMode", &u.DirectoryMode}, {"MaxConnections", &u.MaxConnections},
	}
}

func propertiesFor(target UnitTarget, u *EffectiveUnit) []unitProperty {
	properties := unitProperties(u)
	var result []unitProperty
	for _, p := range properties {
		isSocket := p.name == "Accept" || p.name == "Listen" || p.name == "SocketUser" ||
			p.name == "SocketGroup" || p.name == "SocketMode" || p.name == "DirectoryMode" || p.name == "MaxConnections"
		isCommon := p.name == "Id" || p.name == "LoadState" || p.name == "UnitFileState" ||
			p.name == "ActiveState" || p.name == "SubState" || p.name == "FragmentPath" || p.name == "DropInPaths" ||
			p.name == "NeedDaemonReload" || p.name == "Requires" || p.name == "Wants" || p.name == "Requisite" ||
			p.name == "BindsTo" || p.name == "PartOf" || p.name == "Conflicts" || p.name == "Before" ||
			p.name == "After" || p.name == "Triggers" || p.name == "TriggeredBy"
		if isCommon || (target == UnitHelperSocket && isSocket) || (target != UnitHelperSocket && !isSocket) {
			result = append(result, p)
		}
	}
	return result
}

// ParseEffectiveUnit never includes property values in its errors. Environment
// and credential properties can contain a seed even when the unit is rejected.
// origin: hot-update S9 plan §5.3 / T12
func ParseEffectiveUnit(target UnitTarget, data []byte) (EffectiveUnit, error) {
	var unit EffectiveUnit
	if _, err := UnitName(target); err != nil {
		return unit, err
	}
	if len(data) == 0 || len(data) > 64<<10 || !utf8.Valid(data) || bytes.ContainsAny(data, "\x00\r") || data[len(data)-1] != '\n' {
		return unit, infracontract.ErrIncomplete
	}
	properties := propertiesFor(target, &unit)
	seen := make([]bool, len(properties))
	for _, line := range strings.Split(string(data[:len(data)-1]), "\n") {
		name, value, ok := strings.Cut(line, "=")
		if !ok || strings.Contains(value, "[unprintable]") {
			return EffectiveUnit{}, ErrUnitInvalid
		}
		index := -1
		for i, property := range properties {
			if property.name == name {
				index = i
				break
			}
		}
		if index < 0 || seen[index] {
			return EffectiveUnit{}, ErrUnitInvalid
		}
		seen[index] = true
		*properties[index].value = value
	}
	for _, present := range seen {
		if !present {
			return EffectiveUnit{}, infracontract.ErrUnsupported
		}
	}
	for i, command := range unit.Exec {
		if command == "" {
			continue
		}
		normalized, err := normalizeUnitExec(command)
		if err != nil {
			return EffectiveUnit{}, err
		}
		unit.Exec[i] = normalized
	}
	return unit, nil
}

func normalizeUnitExec(command string) (string, error) {
	// systemd v256 systemctl-show.c prints Ex command flags separately. Do
	// not strip those flags with the timestamps: '+' is fully-privileged.
	parts := strings.Split(command, " ; ")
	if len(parts) != 9 || !strings.HasPrefix(parts[0], "{ path=") ||
		!strings.HasPrefix(parts[1], "argv[]=") || !strings.HasPrefix(parts[2], "flags=") ||
		!strings.HasPrefix(parts[3], "start_time=[") || !strings.HasSuffix(parts[3], "]") ||
		!strings.HasPrefix(parts[4], "stop_time=[") || !strings.HasSuffix(parts[4], "]") ||
		!strings.HasPrefix(parts[5], "pid=") || !strings.HasPrefix(parts[6], "code=") ||
		!strings.HasPrefix(parts[7], "status=") || parts[8] != "}" {
		// Actual show output has no separator before its final closing brace.
		if len(parts) != 8 || !strings.HasSuffix(parts[7], " }") {
			return "", ErrUnitInvalid
		}
		return normalizeUnitExec(strings.TrimSuffix(command, " }") + " ; }")
	}
	if strings.Contains(strings.Join(parts[:3], ""), "\n") {
		return "", ErrUnitInvalid
	}
	return strings.Join(parts[:3], " ; ") + " }", nil
}

func cleanUnitPath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && !strings.ContainsAny(path, " \t\n\r\x00$%{};\\\"'")
}

func safeUnitExec(command string, target UnitTarget, legacy bool) bool {
	parts := strings.Split(strings.TrimSuffix(command, " }"), " ; ")
	if len(parts) != 3 || !strings.HasPrefix(parts[0], "{ path=") ||
		!strings.HasPrefix(parts[1], "argv[]=") || parts[2] != "flags=" {
		return false
	}
	path := strings.TrimPrefix(parts[0], "{ path=")
	argv := strings.TrimPrefix(parts[1], "argv[]=")
	if !cleanUnitPath(path) || strings.ContainsAny(argv, "\n\r\t\x00$%{};\\\"'") ||
		!strings.HasPrefix(argv, path+" ") {
		return false
	}
	if target == UnitHelperService {
		return path == "/usr/local/libexec/tether-infra-helper" && argv == path+" worker"
	}
	if legacy {
		return (target == UnitCaddy && path == "/usr/local/bin/caddy") ||
			(target == UnitNATS && path == "/usr/local/bin/nats-server")
	}
	return path == "/usr/local/libexec/tether-infra-launch"
}

// ValidateEffectiveUnit compares the manager's merged configuration with the
// root-approved full surface. Templates alone miss additive capabilities,
// privileged Exec prefixes, environment injection and later drop-ins.
// origin: hot-update S9 plan §5.3 / §6.2 / T12
func ValidateEffectiveUnit(unit EffectiveUnit, policy UnitPolicy) error {
	name, err := UnitName(policy.Target)
	if err != nil || unit.Name != name || policy.Expected.Name != name || unit.LoadState != "loaded" ||
		unit.NeedDaemonReload != "no" || !cleanUnitPath(unit.FragmentPath) {
		return ErrUnitInvalid
	}
	switch unit.UnitFileState {
	case "enabled", "enabled-runtime", "static", "indirect", "generated", "transient":
	default:
		return ErrUnitNotEnabled
	}
	if policy.Legacy && policy.Target != UnitNATS && policy.Target != UnitCaddy {
		return ErrUnitInvalid
	}
	if unit.Environment != "" || unit.EnvironmentFiles != "" || unit.PassEnvironment != "" || unit.UnsetEnvironment != "" ||
		unit.ExecSearchPath != "" || unit.RootDirectory != "" || unit.RootImage != "" || unit.PAMName != "" ||
		unit.LoadCredential != "" || unit.LoadCredentialEncrypted != "" || unit.SetCredential != "" || unit.SetCredentialEncrypted != "" {
		return ErrUnitInvalid
	}
	if policy.Target == UnitHelperSocket {
		if unit.Accept != "yes" || unit.SocketUser != "root" ||
			(unit.SocketMode != "0600" && unit.SocketMode != "0660") ||
			(unit.DirectoryMode != "0700" && unit.DirectoryMode != "0750") || unit.MaxConnections != "4" ||
			!strings.HasSuffix(unit.Listen, " (Stream)") || !cleanUnitPath(strings.TrimSuffix(unit.Listen, " (Stream)")) {
			return ErrUnitInvalid
		}
		if !managementUnitDependenciesSafe(unit) {
			return ErrUnitInvalid
		}
	} else {
		if unit.Type != "simple" || unit.DynamicUser != "no" || unit.SupplementaryGroups != "" ||
			!safeUnitExec(unit.Exec[2], policy.Target, policy.Legacy) {
			return ErrUnitInvalid
		}
		// A hidden pre/post/reload/stop hook executes even when MainPID is safe.
		for i, command := range unit.Exec {
			if i != 2 && command != "" {
				return ErrUnitInvalid
			}
		}
		if policy.Target == UnitHelperService {
			if unit.User != "root" || unit.Group != "root" || unit.Restart != "no" || !managementUnitDependenciesSafe(unit) {
				return ErrUnitInvalid
			}
		} else if policy.Legacy {
			if policy.Target == UnitCaddy && ((unit.User != "" && unit.User != "root") ||
				(unit.Group != "" && unit.Group != "root") || unit.Restart != "on-failure") {
				return ErrUnitInvalid
			}
			if policy.Target == UnitNATS && (unit.User != "tether" || unit.Restart != "always" || unit.RestartUSec != "2s") {
				return ErrUnitInvalid
			}
		} else {
			if unit.User != "tether" || unit.Group != "tether" || unit.NoNewPrivileges != "yes" || unit.RuntimeMaxUSec != "infinity" {
				return ErrUnitInvalid
			}
			warm := policy.Target == UnitNATSWarm || policy.Target == UnitCaddyWarm
			if (warm && unit.Restart != "no") || (!warm && (unit.Restart != "always" || unit.RestartUSec != "2s")) {
				return ErrUnitInvalid
			}
			caps := ""
			if policy.Target == UnitCaddy {
				caps = "cap_net_bind_service"
			}
			// origin: hot-update S9 plan §5.3 / T12 — equality, not contains.
			if unit.AmbientCapabilities != caps || unit.CapabilityBoundingSet != caps {
				return ErrUnitInvalid
			}
		}
	}
	// ActiveState/SubState are runtime observations. All configuration fields,
	// dependency edges and exact fragment/drop-in lists remain in the comparison.
	expected := policy.Expected
	unit.ActiveState, unit.SubState = "", ""
	expected.ActiveState, expected.SubState = "", ""
	if unit != expected {
		return ErrUnitInvalid
	}
	return nil
}

func managementUnitDependenciesSafe(unit EffectiveUnit) bool {
	for _, edges := range []string{unit.Requires, unit.Wants, unit.Requisite, unit.BindsTo,
		unit.PartOf, unit.Conflicts, unit.Before, unit.After, unit.Triggers, unit.TriggeredBy} {
		for _, name := range strings.Fields(edges) {
			if name == "nats-server.service" || name == "caddy.service" || name == "tether-broker.service" ||
				name == "tether-infra-nats-warm.service" || name == "tether-infra-caddy-warm.service" {
				return false
			}
		}
	}
	return true
}

// ManagedUnitDropIn clears both accumulating capability lists before setting
// the sole bind capability. daemon-reload does not alter a currently live PID.
// P25 installs these exact bytes; P04 must persist the fence before loading it.
// origin: hot-update S9 plan §5.3 / T12
func ManagedUnitDropIn(component infracontract.Component) ([]byte, error) {
	var name, capabilities string
	switch component {
	case infracontract.NATS:
		name = "nats"
	case infracontract.Caddy:
		name, capabilities = "caddy", "CAP_NET_BIND_SERVICE"
	case infracontract.HostSupport:
		return nil, ErrUnitInvalid
	default:
		return nil, ErrUnitInvalid
	}
	return []byte("# tether-infra owned v1\n[Service]\nUser=tether\nGroup=tether\nSupplementaryGroups=\n" +
		"AmbientCapabilities=\nCapabilityBoundingSet=\nAmbientCapabilities=" + capabilities + "\nCapabilityBoundingSet=" + capabilities + "\n" +
		"NoNewPrivileges=yes\nExecStart=\nExecStart=/usr/local/libexec/tether-infra-launch --component=" + name + "\n" +
		"Restart=always\nRestartSec=2\nRuntimeMaxSec=infinity\n"), nil
}

// PropertyNames uses the parser's required fields for the manager's show filter.
// A missing Ex property must remain unsupported rather than weakening the audit.
// origin: hot-update S9 plan §5.3 / T12
func PropertyNames(target UnitTarget) []string {
	var unit EffectiveUnit
	var names []string
	for _, property := range propertiesFor(target, &unit) {
		names = append(names, property.name)
	}
	return names
}
