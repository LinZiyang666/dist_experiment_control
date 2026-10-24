package infraroot

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/ingressswitch"
)

// bootstrap initializes the management plane before asking the strict runtime
// constructor to open it. Existing policy, seeds and journal are reused exactly;
// partial or corrupt authority is never silently replaced by newly generated keys.
// origin: S9 internal review rv-infra F01
func (i *installation) bootstrap(d c.Deadline, stop c.Stop) (infrahost.BootstrapReceipt, error) {
	var receipt infrahost.BootstrapReceipt
	if err := installedBudget(d, stop); err != nil {
		return receipt, err
	}
	before, err := i.before(d)
	if err != nil {
		return receipt, err
	}
	units, templates, err := i.bootstrapInputs(d, stop)
	if err != nil {
		return receipt, err
	}
	state, err := i.files.Directory(stateDirectory, true, 0700, uint32(os.Getegid()))
	if err != nil {
		return receipt, err
	}
	if err := state.Close(); err != nil {
		return receipt, err
	}
	unlock, err := i.files.Lock(stateDirectory + "/bootstrap.lock")
	if err != nil {
		return receipt, err
	}
	defer unlock()
	for _, path := range []string{credentialDirectory, runtimeDirectory, stateDirectory + "/records", stateDirectory + "/acme", stateDirectory + "/locks", stateDirectory + "/configs"} {
		mode, group := uint32(0700), uint32(os.Getegid())
		if path == credentialDirectory || path == runtimeDirectory || path == stateDirectory+"/locks" {
			mode, group = 0750, i.gid
		}
		dir, err := i.files.Directory(path, true, mode, group)
		if err != nil {
			return receipt, err
		}
		if err := dir.Close(); err != nil {
			return receipt, err
		}
	}
	policy, err := i.initializePolicy()
	if err != nil {
		return receipt, err
	}
	if err := i.initializeRecords(policy, units, before, d); err != nil {
		return receipt, err
	}
	receipt.Stage, receipt.Business = infrahost.FilesInstalled, before
	snapshot, err := i.ingress.Read(d, stop)
	if err != nil {
		return receipt, err
	}
	if snapshot.Generation == 0 {
		digest := sha256.Sum256(append([]byte("tether-infra-baseline-v1\n"), policy.Domain[:]...))
		if _, err := i.ingress.CompareAndSwap(snapshot, ingressswitch.Snapshot{Version: 1, Generation: 1, Digest: digest}, d, stop); err != nil {
			return receipt, err
		}
	}
	if err := i.installManagementTemplates(templates); err != nil {
		return receipt, err
	}
	if err := i.activateManagement(d, stop); err != nil {
		return receipt, err
	}
	after, err := i.before(d)
	if err != nil || !sameInstalledProcesses(before, after) {
		return receipt, c.ErrIncomplete
	}
	if err := i.files.Replace(stateDirectory+"/enabled", []byte("tether-infra-enabled-v1\n"), 0600, uint32(os.Getegid())); err != nil {
		return receipt, err
	}
	receipt.Stage = infrahost.SocketActive
	// Helper runtime/version and business readiness remain separate observations;
	// enabling a socket does not manufacture EffectiveUnitVerified/RuntimeReady.
	return receipt, nil
}

// bootstrapInputs validates all support and unit bytes before authority is created.
func (i *installation) bootstrapInputs(d c.Deadline, stop c.Stop) ([]infrahost.UnitPolicy, map[string][]byte, error) {
	for _, path := range []string{"/usr/local/libexec/tether-infra-helper", "/usr/local/libexec/tether-infra-launch"} {
		binary, err := i.files.Open(path, 0)
		if err != nil {
			return nil, nil, err
		}
		var header [20]byte
		_, err = binary.ReadAt(header[:], 0)
		err = errors.Join(err, binary.Close())
		if err != nil || !bytes.Equal(header[:7], []byte{0x7f, 'E', 'L', 'F', 2, 1, 1}) || header[18] != 62 || header[19] != 0 {
			return nil, nil, c.ErrUnsupported
		}
	}
	var units []infrahost.UnitPolicy
	for _, target := range []infrahost.UnitTarget{infrahost.UnitNATS, infrahost.UnitCaddy} {
		policy, err := i.businessPolicy(target, d, stop)
		if err != nil {
			return nil, nil, err
		}
		units = append(units, policy)
		managed := managedPolicy(policy)
		if managed != policy {
			units = append(units, managed)
		}
	}
	// Validate every template before creating authority or changing the manager.
	templates := make(map[string][]byte)
	for _, name := range []string{"tether-infra-helper.socket", "tether-infra-helper@.service"} {
		data, err := i.files.Read("/etc/tether/infra/templates/"+name, 64<<10)
		if err != nil || !validManagementTemplate(name, data) {
			return nil, nil, infrapriv.ErrPermission
		}
		templates[name] = data
	}
	return units, templates, nil
}

// initializeRecords reuses durable authority and the original domain census on retry.
func (i *installation) initializeRecords(policy infrapriv.Policy, units []infrahost.UnitPolicy, before []c.Process, d c.Deadline) error {
	journal, err := infrahost.OpenJournal(i.files.Path(stateDirectory), i.boot, policy.Domain, bootstrapAuthority{})
	if err == nil {
		if err := journal.Close(); err != nil {
			return err
		}
	} else {
		// Missing journal is the fresh/partial bootstrap case. Existing invalid
		// bytes must fail closed, rather than being treated as an empty transaction.
		if _, statErr := os.Lstat(i.files.Path(stateDirectory + "/journal.json")); !errors.Is(statErr, os.ErrNotExist) {
			return err
		}
		initial, err := i.initial(d)
		if err != nil {
			return err
		}
		if err := infrahost.InitializeJournal(i.files.Path(stateDirectory), i.boot, policy.Domain, initial, d); err != nil {
			return err
		}
	}
	record := installedRecord{Version: 1, Purpose: "tether-infra-installed-v1", Units: units, Scope: infrahost.CensusScope{
		Domain: policy.Domain, Generation: 1, Before: before,
		Cgroups:   []string{"/system.slice/nats-server.service", "/system.slice/caddy.service", "/system.slice/tether-infra-caddy-warm.service"},
		Addresses: []string{"127.0.0.1:4222", "127.0.0.1:8222", "0.0.0.0:443", "[::]:443", "0.0.0.0:8443", "[::]:8443"},
	}}
	// Re-bootstrap preserves the original scope/identities. A changed host profile
	// requires explicit reconcile, never overwriting an in-flight domain's basis.
	var existing installedRecord
	if err := i.files.Decode(stateDirectory+"/installed.json", &existing); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	} else if err := i.files.Encode(stateDirectory+"/installed.json", record, 0600, uint32(os.Getegid())); err != nil {
		return err
	}
	return nil
}

// installManagementTemplates publishes only the validated management units.
func (i *installation) installManagementTemplates(templates map[string][]byte) error {
	systemd, err := i.files.Directory("/etc/systemd/system", true, 0755, 0)
	if err != nil {
		return err
	}
	if err := systemd.Close(); err != nil {
		return err
	}
	for name, data := range templates {
		target := "/etc/systemd/system/" + name
		old, err := i.files.Read(target, 64<<10)
		if err == nil && !bytes.Equal(old, data) {
			return infrapriv.ErrPermission
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if errors.Is(err, os.ErrNotExist) {
			if err := i.files.Replace(target, data, 0644, uint32(os.Getegid())); err != nil {
				return err
			}
		}
	}
	wants, err := i.files.Directory("/etc/systemd/system/sockets.target.wants", true, 0755, 0)
	if err != nil {
		return err
	}
	err = linkInstalledSocket(wants)
	if errors.Is(err, os.ErrExist) {
		actual, readErr := os.Readlink(i.files.Path("/etc/systemd/system/sockets.target.wants/tether-infra-helper.socket"))
		if readErr != nil || actual != "../tether-infra-helper.socket" {
			err = infrapriv.ErrPermission
		} else {
			err = nil
		}
	}
	if err == nil {
		err = wants.Sync()
	}
	err = errors.Join(err, wants.Close())
	if err != nil {
		return err
	}
	return nil
}

// activateManagement verifies the asynchronous socket job under the original budget.
func (i *installation) activateManagement(d c.Deadline, stop c.Stop) error {
	if err := i.manager.Reload(d, stop); err != nil {
		return err
	}
	// Exactly one fixed management target is started; no business start/stop can
	// be introduced by the bootstrap loop or by a caller-supplied unit string.
	if err := i.manager.Start(infrahost.UnitHelperSocket, d, stop); err != nil {
		return err
	}
	// systemd's job submission is asynchronous. Observe listening within the
	// original bootstrap budget instead of mistaking a submitted job for readiness.
	for {
		socket, err := i.manager.Read(infrahost.UnitHelperSocket, d, stop)
		if err != nil {
			return err
		}
		if socket.ActiveState == "active" && socket.SubState == "listening" {
			break
		}
		if err := installedBudget(d, stop); err != nil {
			return err
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-stop:
			timer.Stop()
			return infrapriv.ErrBudget
		case <-timer.C:
		}
	}
	for _, target := range []infrahost.UnitTarget{infrahost.UnitHelperSocket, infrahost.UnitHelperService} {
		unit, err := i.manager.Read(target, d, stop)
		if err != nil {
			return err
		}
		fragment := "/etc/systemd/system/tether-infra-helper@.service"
		if target == infrahost.UnitHelperSocket {
			fragment = "/etc/systemd/system/tether-infra-helper.socket"
			if unit.Listen != infrapriv.DefaultSocketPath+" (Stream)" || unit.SocketGroup != "tether" || unit.SocketMode != "0660" {
				return infrapriv.ErrPermission
			}
		}
		if unit.FragmentPath != fragment || unit.DropInPaths != "" {
			return infrapriv.ErrPermission
		}
		policy := infrahost.UnitPolicy{Target: target, Expected: unit}
		if err := infrahost.ValidateEffectiveUnit(unit, policy); err != nil {
			return err
		}
	}
	return nil
}

type bootstrapAuthority struct{}

func (bootstrapAuthority) Authorize(c.HostRequest, c.Deadline) (c.Peer, error) {
	return c.Peer{}, infrapriv.ErrPermission
}

func (i *installation) initializePolicy() (_ infrapriv.Policy, resultErr error) {
	dir, err := i.files.Directory(credentialDirectory, false, 0, 0)
	if err != nil {
		return infrapriv.Policy{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	if err := infrapriv.InitializeCredentialStore(dir, i.files.Owner(), i.gid); err != nil {
		return infrapriv.Policy{}, err
	}
	local, err := infrapriv.ReadCredentialStore(dir, infrapriv.LocalInfra, i.files.Owner(), i.gid)
	if err != nil {
		return infrapriv.Policy{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, local.Close()) }()
	launch, err := infrapriv.ReadCredentialStore(dir, infrapriv.LocalLaunch, i.files.Owner(), i.gid)
	if err != nil {
		return infrapriv.Policy{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, launch.Close()) }()
	policy, err := i.policy()
	if errors.Is(err, os.ErrNotExist) {
		policy = infrapriv.Policy{Version: 1, HostBootID: i.boot, Revision: 1, AccountPublicKey: local.PublicKey(), TrustSHA256: sha256.Sum256(local.PublicKey()), ServiceUID: i.uid, ServiceGID: i.gid,
			Keys: []infrapriv.PolicyKey{{KeyID: "local-infra-v1", PublicKey: local.PublicKey(), UID: i.uid, GID: i.gid, Scope: infrapriv.LocalAuthority}, {KeyID: "local-launch-v1", PublicKey: launch.PublicKey(), UID: i.uid, GID: i.gid, Scope: infrapriv.LaunchAuthority}}}
		if _, err := rand.Read(policy.Domain[:]); err != nil {
			return policy, err
		}
		data, err := infrapriv.EncodePolicy(policy)
		if err != nil {
			return policy, err
		}
		if err := i.files.Replace(stateDirectory+"/policy.json", data, 0600, uint32(os.Getegid())); err != nil {
			return policy, err
		}
	} else if err != nil {
		return policy, err
	}
	var keyID string
	for _, key := range policy.Keys {
		if key.Scope == infrapriv.LaunchAuthority && bytes.Equal(key.PublicKey, launch.PublicKey()) {
			keyID = key.KeyID
		}
	}
	if keyID == "" {
		return policy, infrapriv.ErrPolicyStale
	}
	identity := infrapriv.LaunchIdentity{Version: 1, Domain: policy.Domain, PolicyRevision: policy.Revision, KeyID: keyID, HostBootID: policy.HostBootID, ServiceGID: i.gid}
	if err := i.files.Encode(credentialDirectory+"/launch.json", identity, 0640, i.gid); err != nil {
		return policy, err
	}
	return policy, nil
}

func sameInstalledProcesses(a, b []c.Process) bool {
	if len(a) != len(b) {
		return false
	}
	for _, p := range a {
		if !slices.Contains(b, p) {
			return false
		}
	}
	return true
}

func validManagementTemplate(name string, data []byte) bool {
	if !strings.HasPrefix(string(data), "# tether-infra owned v1\n") {
		return false
	}
	var want string
	if name == "tether-infra-helper.socket" {
		want = "[Unit]\nDescription=tether infrastructure management socket\n[Socket]\nListenStream=/run/tether-infra/control.sock\nAccept=yes\nSocketUser=root\nSocketGroup=tether\nSocketMode=0660\nDirectoryMode=0750\nMaxConnections=4\nService=tether-infra-helper@.service\n[Install]\nWantedBy=sockets.target"
	} else {
		want = "[Unit]\nDescription=tether infrastructure management worker\n[Service]\nType=simple\nUser=root\nGroup=root\nExecStart=/usr/local/libexec/tether-infra-helper worker\nStandardInput=socket\nStandardOutput=socket\nStandardError=journal\nRestart=no\nRuntimeMaxSec=15s\nKillMode=control-group\nTimeoutStopSec=5s"
	}
	var lines []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n") == want
}

func (i *installation) businessPolicy(target infrahost.UnitTarget, d c.Deadline, stop c.Stop) (infrahost.UnitPolicy, error) {
	name, err := infrahost.UnitName(target)
	if err != nil {
		return infrahost.UnitPolicy{}, err
	}
	data, err := i.files.Read("/etc/systemd/system/"+name, 64<<10)
	if err != nil || !validBusinessUnit(target, data) {
		return infrahost.UnitPolicy{}, infrapriv.ErrPermission
	}
	unit, err := i.manager.Read(target, d, stop)
	if err != nil {
		return infrahost.UnitPolicy{}, err
	}
	legacy := unit.DropInPaths == ""
	policy := infrahost.UnitPolicy{Target: target, Legacy: legacy, Expected: unit}
	if !legacy {
		component := c.NATS
		if target == infrahost.UnitCaddy {
			component = c.Caddy
		}
		expected, _ := infrahost.ManagedUnitDropIn(component)
		path := "/etc/systemd/system/" + name + ".d/90-tether-infra.conf"
		actual, err := i.files.Read(path, 64<<10)
		if err != nil || !bytes.Equal(actual, expected) || unit.DropInPaths != path {
			return policy, infrapriv.ErrPermission
		}
	}
	return policy, infrahost.ValidateEffectiveUnit(unit, policy)
}

func validBusinessUnit(target infrahost.UnitTarget, data []byte) bool {
	section := ""
	seen := make(map[string]bool)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") {
			if line != "[Unit]" && line != "[Service]" && line != "[Install]" {
				return false
			}
			section = line
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key = section + key
		if !ok || seen[key] {
			return false
		}
		seen[key] = true
		switch key {
		case "[Unit]Description":
			if value == "" {
				return false
			}
		case "[Unit]After", "[Unit]Wants":
			if value != "network-online.target" {
				return false
			}
		case "[Service]Type":
			if value != "simple" {
				return false
			}
		case "[Service]User":
			if target != infrahost.UnitNATS || value != "tether" {
				return false
			}
		case "[Service]ExecStart":
			want := "/usr/local/bin/nats-server -c /etc/tether/nats.d/nats.conf"
			if target == infrahost.UnitCaddy {
				want = "/usr/local/bin/caddy run --config /etc/tether/Caddyfile --adapter caddyfile"
			}
			if value != want {
				return false
			}
		case "[Service]Restart":
			if target == infrahost.UnitNATS && value != "always" || target == infrahost.UnitCaddy && value != "on-failure" {
				return false
			}
		case "[Service]RestartSec":
			if target != infrahost.UnitNATS || value != "2" {
				return false
			}
		case "[Install]WantedBy":
			if value != "multi-user.target" {
				return false
			}
		default:
			return false
		}
	}
	return seen["[Service]ExecStart"] && seen["[Service]Restart"]
}

func managedPolicy(policy infrahost.UnitPolicy) infrahost.UnitPolicy {
	if !policy.Legacy {
		return policy
	}
	policy.Legacy = false
	unit := &policy.Expected
	name := "nats"
	capabilities := ""
	if policy.Target == infrahost.UnitCaddy {
		name, capabilities = "caddy", "cap_net_bind_service"
	}
	unit.User, unit.Group, unit.NoNewPrivileges = "tether", "tether", "yes"
	unit.SupplementaryGroups, unit.AmbientCapabilities, unit.CapabilityBoundingSet = "", capabilities, capabilities
	unit.Exec[2] = "{ path=/usr/local/libexec/tether-infra-launch ; argv[]=/usr/local/libexec/tether-infra-launch --component=" + name + " ; flags= }"
	unit.Restart, unit.RestartUSec, unit.RuntimeMaxUSec = "always", "2s", "infinity"
	unit.DropInPaths = filepath.Join("/etc/systemd/system", unit.Name+".d", "90-tether-infra.conf")
	return policy
}
