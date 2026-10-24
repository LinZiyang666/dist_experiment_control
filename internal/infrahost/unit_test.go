//go:build linux

package infrahost

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

func unitFixture(target UnitTarget, legacy bool) UnitPolicy {
	name, _ := UnitName(target)
	u := EffectiveUnit{
		Name: name, LoadState: "loaded", UnitFileState: "enabled", ActiveState: "active", SubState: "running",
		FragmentPath: "/etc/systemd/system/" + name, NeedDaemonReload: "no",
		User: "tether", Group: "tether", DynamicUser: "no", Type: "simple",
		Restart: "always", RestartUSec: "2s", RuntimeMaxUSec: "infinity", NoNewPrivileges: "yes", KillMode: "control-group",
		Exec: [7]string{2: "{ path=/usr/local/libexec/tether-infra-launch ; argv[]=/usr/local/libexec/tether-infra-launch --component nats ; flags= }"},
	}
	if target == UnitCaddy {
		u.AmbientCapabilities, u.CapabilityBoundingSet = "cap_net_bind_service", "cap_net_bind_service"
		u.Exec[2] = strings.ReplaceAll(u.Exec[2], "nats", "caddy")
	}
	if target == UnitNATSWarm || target == UnitCaddyWarm {
		u.Restart = "no"
		u.Exec[2] = "{ path=/usr/local/libexec/tether-infra-launch ; argv[]=/usr/local/libexec/tether-infra-launch --component caddy --warm ; flags= }"
	}
	if legacy {
		u.Group, u.NoNewPrivileges = "", "no"
		u.AmbientCapabilities = ""
		u.CapabilityBoundingSet = "cap_chown cap_dac_override cap_net_bind_service"
		if target == UnitCaddy {
			u.User, u.Restart, u.RestartUSec = "", "on-failure", "100ms"
			u.Exec[2] = "{ path=/usr/local/bin/caddy ; argv[]=/usr/local/bin/caddy run --config /etc/tether/Caddyfile --adapter caddyfile ; flags= }"
		} else {
			u.Exec[2] = "{ path=/usr/local/bin/nats-server ; argv[]=/usr/local/bin/nats-server -c /etc/tether/nats.d/nats.conf ; flags= }"
		}
	}
	if target == UnitHelperService {
		u.User, u.Group, u.Restart, u.RuntimeMaxUSec = "root", "root", "no", "15s"
		u.Exec[2] = "{ path=/usr/local/libexec/tether-infra-helper ; argv[]=/usr/local/libexec/tether-infra-helper worker ; flags= }"
		u.UnitFileState = "static"
		u.FragmentPath = "/etc/systemd/system/tether-infra-helper@.service"
	}
	if target == UnitHelperSocket {
		u = EffectiveUnit{Name: name, LoadState: "loaded", UnitFileState: "enabled", ActiveState: "inactive", SubState: "dead",
			FragmentPath: "/etc/systemd/system/" + name, NeedDaemonReload: "no", Accept: "yes",
			Listen: "/run/tether-infra/control.sock (Stream)", SocketUser: "root", SocketGroup: "tether", SocketMode: "0660",
			DirectoryMode: "0750", MaxConnections: "4", Triggers: "tether-infra-helper@.service"}
	}
	return UnitPolicy{Target: target, Legacy: legacy, Expected: u}
}

// This independent mapping is intentionally separate from the production
// required-property list: deleting one audit field must break fixture parsing.
func unitFixtureBytes(target UnitTarget, u EffectiveUnit) []byte {
	records := []struct{ key, value string }{
		{"Id", u.Name}, {"LoadState", u.LoadState}, {"UnitFileState", u.UnitFileState}, {"ActiveState", u.ActiveState}, {"SubState", u.SubState},
		{"FragmentPath", u.FragmentPath}, {"DropInPaths", u.DropInPaths}, {"NeedDaemonReload", u.NeedDaemonReload},
		{"Requires", u.Requires}, {"Wants", u.Wants}, {"Requisite", u.Requisite}, {"BindsTo", u.BindsTo}, {"PartOf", u.PartOf},
		{"Conflicts", u.Conflicts}, {"Before", u.Before}, {"After", u.After}, {"Triggers", u.Triggers}, {"TriggeredBy", u.TriggeredBy},
	}
	if target == UnitHelperSocket {
		records = append(records, []struct{ key, value string }{
			{"Accept", u.Accept}, {"Listen", u.Listen}, {"SocketUser", u.SocketUser}, {"SocketGroup", u.SocketGroup},
			{"SocketMode", u.SocketMode}, {"DirectoryMode", u.DirectoryMode}, {"MaxConnections", u.MaxConnections},
		}...)
	} else {
		records = append(records, []struct{ key, value string }{
			{"User", u.User}, {"Group", u.Group}, {"SupplementaryGroups", u.SupplementaryGroups}, {"DynamicUser", u.DynamicUser},
			{"Type", u.Type}, {"Restart", u.Restart}, {"RestartUSec", u.RestartUSec}, {"RuntimeMaxUSec", u.RuntimeMaxUSec},
			{"Environment", u.Environment}, {"EnvironmentFiles", u.EnvironmentFiles}, {"PassEnvironment", u.PassEnvironment}, {"UnsetEnvironment", u.UnsetEnvironment},
			{"ExecSearchPath", u.ExecSearchPath}, {"WorkingDirectory", u.WorkingDirectory}, {"RootDirectory", u.RootDirectory}, {"RootImage", u.RootImage},
			{"AmbientCapabilities", u.AmbientCapabilities}, {"CapabilityBoundingSet", u.CapabilityBoundingSet}, {"NoNewPrivileges", u.NoNewPrivileges},
			{"BindPaths", u.BindPaths}, {"BindReadOnlyPaths", u.BindReadOnlyPaths}, {"ReadWritePaths", u.ReadWritePaths},
			{"ConfigurationDirectory", u.ConfigurationDirectory}, {"StateDirectory", u.StateDirectory}, {"RuntimeDirectory", u.RuntimeDirectory},
			{"LoadCredential", u.LoadCredential}, {"LoadCredentialEncrypted", u.LoadCredentialEncrypted},
			{"SetCredential", u.SetCredential}, {"SetCredentialEncrypted", u.SetCredentialEncrypted},
			{"PAMName", u.PAMName}, {"Sockets", u.Sockets}, {"KillMode", u.KillMode},
		}...)
		for i, key := range []string{"ExecConditionEx", "ExecStartPreEx", "ExecStartEx", "ExecStartPostEx", "ExecReloadEx", "ExecStopEx", "ExecStopPostEx"} {
			command := u.Exec[i]
			if command != "" {
				command = strings.TrimSuffix(command, " }") + " ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0 }"
			}
			records = append(records, struct{ key, value string }{key, command})
		}
	}
	var data strings.Builder
	for _, record := range records {
		data.WriteString(record.key + "=" + record.value + "\n")
	}
	return []byte(data.String())
}

// origin: hot-update S9 plan §5.3 / T12 / M07
func TestInfraEffectiveUnitRejectsOverrides(t *testing.T) {
	for _, target := range []UnitTarget{UnitNATS, UnitCaddy, UnitCaddyWarm, UnitHelperSocket, UnitHelperService} {
		t.Run("valid-"+unitTestName(target), func(t *testing.T) {
			p := unitFixture(target, false)
			got, err := ParseEffectiveUnit(target, unitFixtureBytes(target, p.Expected))
			if err != nil || got != p.Expected {
				t.Fatalf("full effective property parse: err=%v equal=%v", err, got == p.Expected)
			}
			if err := ValidateEffectiveUnit(got, p); err != nil {
				t.Fatalf("approved effective unit rejected: %v", err)
			}
		})
	}
	for _, target := range []UnitTarget{UnitNATS, UnitCaddy} {
		p := unitFixture(target, true)
		if err := ValidateEffectiveUnit(p.Expected, p); err != nil {
			t.Fatalf("original stock unit rejected: %v", err)
		}
	}
	policy := unitFixture(UnitCaddy, false)
	mutations := []struct {
		name   string
		change func(*EffectiveUnit)
	}{
		{"accumulated-ambient", func(u *EffectiveUnit) { u.AmbientCapabilities += " cap_sys_admin" }},
		{"accumulated-bounding", func(u *EffectiveUnit) { u.CapabilityBoundingSet += " cap_dac_override" }},
		{"plus-exec", func(u *EffectiveUnit) { u.Exec[2] = strings.Replace(u.Exec[2], "flags=", "flags=fully-privileged", 1) }},
		{"bang-exec", func(u *EffectiveUnit) { u.Exec[2] = strings.Replace(u.Exec[2], "flags=", "flags=no-setuid", 1) }},
		{"minus-exec", func(u *EffectiveUnit) { u.Exec[2] = strings.Replace(u.Exec[2], "flags=", "flags=ignore-failure", 1) }},
		{"environment-file", func(u *EffectiveUnit) { u.EnvironmentFiles = "/tmp/operator.env (ignore_errors=yes)" }},
		{"environment-canary", func(u *EffectiveUnit) { u.Environment = "SECRET=canary-unit-seed-7842" }},
		{"pass-environment", func(u *EffectiveUnit) { u.PassEnvironment = "LD_PRELOAD" }},
		{"dynamic-user", func(u *EffectiveUnit) { u.DynamicUser = "yes" }},
		{"supplementary-root", func(u *EffectiveUnit) { u.SupplementaryGroups = "root" }},
		{"root-user", func(u *EffectiveUnit) { u.User = "root" }},
		{"root-group", func(u *EffectiveUnit) { u.Group = "root" }},
		{"credential", func(u *EffectiveUnit) { u.SetCredential = "secret:canary-unit-seed-7842" }},
		{"root-image", func(u *EffectiveUnit) { u.RootImage = "/tmp/operator.img" }},
		{"pam", func(u *EffectiveUnit) { u.PAMName = "operator" }},
		{"search-path", func(u *EffectiveUnit) { u.ExecSearchPath = "/tmp" }},
		{"privileges", func(u *EffectiveUnit) { u.NoNewPrivileges = "no" }},
		{"restart-policy", func(u *EffectiveUnit) { u.Restart = "on-failure" }},
		{"restart-delay", func(u *EffectiveUnit) { u.RestartUSec = "100ms" }},
		{"runtime-timer", func(u *EffectiveUnit) { u.RuntimeMaxUSec = "120s" }},
		{"unloaded", func(u *EffectiveUnit) { u.LoadState = "not-found" }},
		{"disabled", func(u *EffectiveUnit) { u.UnitFileState = "disabled" }},
		{"masked", func(u *EffectiveUnit) { u.UnitFileState = "masked" }},
		{"stale-manager", func(u *EffectiveUnit) { u.NeedDaemonReload = "yes" }},
	}
	for i, key := range []string{"condition", "pre", "start", "post", "reload", "stop", "stop-post"} {
		if i == 2 {
			continue
		}
		mutations = append(mutations, struct {
			name   string
			change func(*EffectiveUnit)
		}{
			"hook-" + key, func(u *EffectiveUnit) {
				u.Exec[i] = "{ path=/usr/bin/true ; argv[]=/usr/bin/true privileged-hook ; flags=fully-privileged }"
			},
		})
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			u := policy.Expected
			mutation.change(&u)
			// Also reject unsafe root policy input: comparing two equally unsafe
			// values must not bypass the capability, hook or env safety guards.
			p := policy
			p.Expected = u
			if err := ValidateEffectiveUnit(u, p); err == nil || strings.Contains(err.Error(), "canary") {
				t.Fatalf("unsafe effective unit accepted or secret disclosed: %v", err)
			}
		})
	}
	t.Run("all-static-fields", func(t *testing.T) {
		value := reflect.ValueOf(&policy.Expected).Elem()
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if field.Name == "ActiveState" || field.Name == "SubState" || field.Type.Kind() != reflect.String {
				continue
			}
			u := policy.Expected
			reflect.ValueOf(&u).Elem().Field(i).SetString("unapproved-" + field.Name)
			if err := ValidateEffectiveUnit(u, policy); err == nil {
				t.Errorf("effective field %s is not audited", field.Name)
			}
		}
	})
	t.Run("management-dependencies", func(t *testing.T) {
		for _, target := range []UnitTarget{UnitHelperSocket, UnitHelperService} {
			p := unitFixture(target, false)
			p.Expected.Wants = "caddy.service"
			if err := ValidateEffectiveUnit(p.Expected, p); err == nil {
				t.Fatal("socket activation may start live business through Wants")
			}
		}
	})
	t.Run("strict-observation", func(t *testing.T) {
		data := unitFixtureBytes(UnitCaddy, policy.Expected)
		bad := [][]byte{nil, bytes.Repeat([]byte{'x'}, (64<<10)+1), data[:len(data)-1], append(bytes.Clone(data), '\x00'),
			append(bytes.Clone(data), 0xff), append(bytes.Clone(data), []byte("User=tether\n")...),
			append(bytes.Clone(data), []byte("UnknownField=safe\n")...),
			bytes.Replace(data, []byte("User=tether"), []byte("User=[unprintable]"), 1),
			bytes.Replace(data, []byte("flags="), []byte("ignore_errors=no"), 1),
			bytes.Replace(data, []byte("status=0 }"), []byte("status=0 } { path=/bin/sh ; argv[]=/bin/sh ; flags= }"), 1),
		}
		for i, body := range bad {
			if _, err := ParseEffectiveUnit(UnitCaddy, body); err == nil {
				t.Errorf("malformed observation %d accepted", i)
			}
		}
		for _, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
			body := bytes.Replace(data, []byte(line+"\n"), nil, 1)
			if _, err := ParseEffectiveUnit(UnitCaddy, body); err == nil {
				t.Errorf("missing property %s accepted", strings.SplitN(line, "=", 2)[0])
			}
		}
	})
	t.Run("drop-in-clears-additive-capabilities", func(t *testing.T) {
		for _, component := range []infracontract.Component{infracontract.NATS, infracontract.Caddy} {
			data, err := ManagedUnitDropIn(component)
			if err != nil {
				t.Fatal(err)
			}
			for _, property := range []string{"AmbientCapabilities", "CapabilityBoundingSet"} {
				caps := "CAP_SYS_ADMIN CAP_DAC_OVERRIDE"
				for _, line := range strings.Split(string(data), "\n") {
					if v, ok := strings.CutPrefix(line, property+"="); ok {
						if v == "" {
							caps = ""
						} else {
							caps = strings.TrimSpace(caps + " " + v)
						}
					}
				}
				expected := ""
				if component == infracontract.Caddy {
					expected = "CAP_NET_BIND_SERVICE"
				}
				if caps != expected {
					t.Fatalf("%s accumulated unapproved capabilities: %s", property, caps)
				}
			}
			if !strings.Contains(string(data), "ExecStart=\nExecStart=/usr/local/libexec/tether-infra-launch --component=") {
				t.Fatal("drop-in does not reset the stock ExecStart before launcher replacement")
			}
		}
		if _, err := ManagedUnitDropIn(infracontract.HostSupport); !errors.Is(err, ErrUnitInvalid) {
			t.Fatal("unknown component can render a privileged unit")
		}
	})
	t.Run("trusted-drop-in-replacement", testManagedDropInReplacement)
	t.Run("actual-helper-instance", func(t *testing.T) {
		process := unitTestProcess(t, os.Getpid())
		process.Cgroup = "/system.slice/tether-infra-helper@23-local.service"
		called := 0
		m := &systemdManager{run: func(args []string, _ infracontract.Deadline, _ infracontract.Stop) ([]byte, error) {
			called++
			if args[len(args)-1] != "tether-infra-helper@23-local.service" {
				t.Fatal("helper audit did not bind the peer's actual instance")
			}
			u := unitFixture(UnitHelperService, false).Expected
			u.Name = args[len(args)-1]
			return unitFixtureBytes(UnitHelperService, u), nil
		}}
		u, err := m.ReadHelper(process, unitTestDeadline(t, time.Second), nil)
		policy := unitFixture(UnitHelperService, false)
		if err != nil || ValidateEffectiveUnit(u, policy) != nil {
			t.Fatal("actual socket worker audit rejected")
		}
		for _, cgroup := range []string{"/system.slice/caddy.service", "/system.slice/tether-infra-helper@.service", "/system.slice/tether-infra-helper@unsafe$(id).service", "/system.slice/tether-infra-helper@new\n.service"} {
			process.Cgroup = cgroup
			if _, err := m.ReadHelper(process, unitTestDeadline(t, time.Second), nil); err == nil {
				t.Fatal("untrusted helper identity can address arbitrary units")
			}
		}
		if called != 1 {
			t.Fatal("invalid helper identity reached systemctl")
		}
	})
	var output unitOutput
	if n, err := output.Write(make([]byte, 64<<10)); n != 64<<10 || err != nil {
		t.Fatal("exact output bound rejected")
	}
	if _, err := output.Write([]byte{1}); !errors.Is(err, infracontract.ErrIncomplete) || !output.oversized {
		t.Fatal("truncated census accepted")
	}
}

func testManagedDropInReplacement(t *testing.T) {
	data, err := ManagedUnitDropIn(infracontract.Caddy)
	if err != nil {
		t.Fatal(err)
	}
	for _, defect := range []string{"none", "repeat", "operator-file", "symlink-file", "symlink-directory", "writable-directory", "writable-file", "wrong-owner", "missing-marker"} {
		t.Run(defect, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0700); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(root, "90-tether-infra.conf")
			outside := filepath.Join(t.TempDir(), "outside")
			outsideContent := "# tether-infra owned v1\noperator-original"
			if err := os.WriteFile(outside, []byte(outsideContent), 0600); err != nil {
				t.Fatal(err)
			}
			body, owner := data, uint32(os.Geteuid())
			switch defect {
			case "operator-file":
				err = os.WriteFile(file, []byte("# operator configuration\n"), 0644)
			case "symlink-file":
				err = os.Symlink(outside, file)
			case "symlink-directory":
				parentPath := t.TempDir()
				alias := filepath.Join(parentPath, "unit.d")
				err = os.Symlink(root, alias)
				if err != nil {
					t.Fatal(err)
				}
				parent, err := unix.Open(parentPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = unix.Close(parent) }() // Read-only directory descriptors.
				if child, err := openTrustedUnitChild(parent, "unit.d", owner); err == nil {
					_ = unix.Close(child)
					t.Fatal("symlink directory opened without nofollow protection")
				}
				return
			case "writable-directory":
				err = os.Chmod(root, 0777)
			case "writable-file":
				err = os.WriteFile(file, data, 0666)
				if err == nil {
					err = os.Chmod(file, 0666)
				}
			case "wrong-owner":
				owner++
			case "missing-marker":
				body = []byte("[Service]\nUser=root\n")
			}
			if err != nil {
				t.Fatal(err)
			}
			fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if err != nil {
				t.Fatal(err)
			}
			// Read-only directory descriptor; replaceManagedDropIn syncs its own effects.
			defer func() { _ = unix.Close(fd) }()
			err = replaceManagedDropIn(fd, body, owner)
			if defect != "none" && defect != "repeat" {
				if err == nil {
					t.Fatal("unowned or replaceable unit target accepted")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if defect == "repeat" {
					if err := replaceManagedDropIn(fd, data, owner); err != nil {
						t.Fatal(err)
					}
				}
				got, err := os.ReadFile(file)
				if err != nil || !bytes.Equal(got, data) {
					t.Fatal("final installed drop-in bytes changed")
				}
				info, err := os.Stat(file)
				if err != nil || info.Mode().Perm() != 0644 {
					t.Fatal("drop-in mode changed")
				}
			}
			got, err := os.ReadFile(outside)
			if err != nil || string(got) != outsideContent {
				t.Fatal("replacement wrote through a symlink")
			}
			entries, err := os.ReadDir(root)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), ".tether-infra-") {
					t.Fatal("replacement leaked temporary unit file")
				}
			}
		})
	}
}

func unitTestName(target UnitTarget) string {
	name, err := UnitName(target)
	if err != nil {
		return "invalid"
	}
	return name
}

// origin: hot-update S9 plan §5.3 / T12 — show is an untrusted text boundary.
func FuzzInfraEffectiveUnit(f *testing.F) {
	p := unitFixture(UnitCaddy, false)
	f.Add(unitFixtureBytes(UnitCaddy, p.Expected))
	f.Add([]byte("ExecStartEx={ path=/bin/sh ; argv[]=/bin/sh ; flags=fully-privileged }\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		unit, err := ParseEffectiveUnit(UnitCaddy, data)
		if err == nil {
			roundtrip, err := ParseEffectiveUnit(UnitCaddy, unitFixtureBytes(UnitCaddy, unit))
			if err != nil || roundtrip != unit {
				t.Fatal("accepted property observation cannot roundtrip")
			}
		}
	})
}
