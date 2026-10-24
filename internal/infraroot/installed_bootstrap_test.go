//go:build linux && amd64

package infraroot

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/ingressswitch"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Only systemd/nft are namespace fixtures. Credentials, pinned directory walks,
// journal persistence and strict worker construction use their real owners.
type installedManager struct {
	units   map[infrahost.UnitTarget]infrahost.EffectiveUnit
	starts  []infrahost.UnitTarget
	reloads int
	start   func(infrahost.UnitTarget) error
}

func (m *installedManager) Read(target infrahost.UnitTarget, _ c.Deadline, _ c.Stop) (infrahost.EffectiveUnit, error) {
	return m.units[target], nil
}
func (*installedManager) ReadHelper(c.Process, c.Deadline, c.Stop) (infrahost.EffectiveUnit, error) {
	return infrahost.EffectiveUnit{}, c.ErrIncomplete
}
func (m *installedManager) Reload(c.Deadline, c.Stop) error { m.reloads++; return nil }
func (m *installedManager) Start(target infrahost.UnitTarget, _ c.Deadline, _ c.Stop) error {
	m.starts = append(m.starts, target)
	if m.start != nil {
		return m.start(target)
	}
	if target != infrahost.UnitHelperSocket {
		return errors.New("bootstrap started a business unit")
	}
	u := m.units[target]
	u.ActiveState, u.SubState = "active", "listening"
	m.units[target] = u
	return nil
}
func (*installedManager) Fence(infrahost.UnitTarget, c.Deadline, c.Stop) error {
	return errors.New("bootstrap fenced a business unit")
}

type installedIngress struct {
	snapshot ingressswitch.Snapshot
	writes   int
}

func (n *installedIngress) Read(c.Deadline, c.Stop) (ingressswitch.Snapshot, error) {
	return n.snapshot, nil
}
func (n *installedIngress) CompareAndSwap(before, after ingressswitch.Snapshot, _ c.Deadline, _ c.Stop) (ingressswitch.Snapshot, error) {
	if before != n.snapshot {
		return ingressswitch.Snapshot{}, c.ErrIncomplete
	}
	n.snapshot = after
	n.writes++
	return after, nil
}

func installedFixture(t *testing.T) (*installation, *installedManager, *installedIngress) {
	t.Helper()
	base := t.TempDir()
	write := func(name, contents string, mode os.FileMode) {
		t.Helper()
		path := filepath.Join(base, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), mode); err != nil {
			t.Fatal(err)
		}
	}
	header := make([]byte, 20)
	copy(header, []byte{0x7f, 'E', 'L', 'F', 2, 1, 1})
	header[18] = 62
	for _, name := range []string{"helper", "launch"} {
		write("usr/local/libexec/tether-infra-"+name, string(header), 0755)
	}
	write("etc/tether/infra/templates/tether-infra-helper.socket", "# tether-infra owned v1\n[Unit]\nDescription=tether infrastructure management socket\n[Socket]\nListenStream=/run/tether-infra/control.sock\nAccept=yes\nSocketUser=root\nSocketGroup=tether\nSocketMode=0660\nDirectoryMode=0750\nMaxConnections=4\nService=tether-infra-helper@.service\n[Install]\nWantedBy=sockets.target\n", 0644)
	write("etc/tether/infra/templates/tether-infra-helper@.service", "# tether-infra owned v1\n[Unit]\nDescription=tether infrastructure management worker\n[Service]\nType=simple\nUser=root\nGroup=root\nExecStart=/usr/local/libexec/tether-infra-helper worker\nStandardInput=socket\nStandardOutput=socket\nStandardError=journal\nRestart=no\nRuntimeMaxSec=15s\nKillMode=control-group\nTimeoutStopSec=5s\n", 0644)
	m := &installedManager{units: make(map[infrahost.UnitTarget]infrahost.EffectiveUnit)}
	for _, target := range []infrahost.UnitTarget{infrahost.UnitNATS, infrahost.UnitCaddy} {
		name, _ := infrahost.UnitName(target)
		command := "/usr/local/bin/nats-server -c /etc/tether/nats.d/nats.conf"
		user, restart, delay := "tether", "always", "2s"
		if target == infrahost.UnitCaddy {
			command, user, restart, delay = "/usr/local/bin/caddy run --config /etc/tether/Caddyfile --adapter caddyfile", "", "on-failure", "100ms"
		}
		body := "[Unit]\nDescription=Original stock service\nAfter=network-online.target\n[Service]\nType=simple\nExecStart=" + command + "\nRestart=" + restart + "\n"
		if target == infrahost.UnitNATS {
			body += "User=tether\nRestartSec=2\n"
		}
		body += "[Install]\nWantedBy=multi-user.target\n"
		write("etc/systemd/system/"+name, body, 0644)
		m.units[target] = infrahost.EffectiveUnit{Name: name, LoadState: "loaded", UnitFileState: "enabled", ActiveState: "active", SubState: "running", FragmentPath: "/etc/systemd/system/" + name, NeedDaemonReload: "no", User: user, DynamicUser: "no", Type: "simple", Restart: restart, RestartUSec: delay, RuntimeMaxUSec: "infinity", NoNewPrivileges: "no", KillMode: "control-group", CapabilityBoundingSet: "cap_chown cap_dac_override cap_net_bind_service", Exec: [7]string{2: "{ path=" + strings.Fields(command)[0] + " ; argv[]=" + command + " ; flags= }"}}
	}
	m.units[infrahost.UnitHelperSocket] = infrahost.EffectiveUnit{Name: "tether-infra-helper.socket", LoadState: "loaded", UnitFileState: "enabled", ActiveState: "inactive", SubState: "dead", FragmentPath: "/etc/systemd/system/tether-infra-helper.socket", NeedDaemonReload: "no", Accept: "yes", Listen: "/run/tether-infra/control.sock (Stream)", SocketUser: "root", SocketGroup: "tether", SocketMode: "0660", DirectoryMode: "0750", MaxConnections: "4", Triggers: "tether-infra-helper@.service"}
	u := unitPolicy().Expected
	u.Name = "tether-infra-helper@audit.service"
	u.FragmentPath = "/etc/systemd/system/tether-infra-helper@.service"
	u.User, u.Group, u.Restart, u.RuntimeMaxUSec, u.UnitFileState = "root", "root", "no", "15s", "static"
	u.Exec[2] = "{ path=/usr/local/libexec/tether-infra-helper ; argv[]=/usr/local/libexec/tether-infra-helper worker ; flags= }"
	m.units[infrahost.UnitHelperService] = u
	write("root/.local/share/caddy/account-key", "private test writer data", 0600)
	if err := os.Chmod(filepath.Join(base, "root/.local/share/caddy"), 0700); err != nil {
		t.Fatal(err)
	}
	files, err := openInstalledFiles(base, uint32(os.Geteuid()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := files.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	})
	i := &installation{files: files, manager: m, uid: uint32(os.Geteuid()), gid: uint32(os.Getegid()), boot: "installed-test-boot", census: infrahost.ReadCensus, process: infrahost.ReadProcess}
	i.before = func(c.Deadline) ([]c.Process, error) {
		p := knownProcess(i.boot)
		p.Cgroup = "/system.slice/nats-server.service"
		return []c.Process{p}, nil
	}
	i.initial = func(c.Deadline) (infrahost.HostState, error) { return infrahost.HostState{}, nil }
	n := &installedIngress{}
	i.ingress = n
	return i, m, n
}

// origin: S9 internal review rv-infra F01
func TestInstalledBootstrapInitializesBeforeOpeningWorker(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInstalledBootstrap(t)
	}
	testharness.AssertNoGoroutineLeak(t, "installed bootstrap worker lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "installed bootstrap record custody", beforeFD, 0)
}
func exerciseInstalledBootstrap(t *testing.T) {
	t.Helper()
	i, m, n := installedFixture(t)
	defer closeTestResource(t, i.files)
	if root, err := i.worker(); root != nil || !errors.Is(err, infrapriv.ErrNotEnabled) {
		t.Fatal("fresh installation opened an enabled worker", err)
	}
	for _, path := range []string{"policy.json", "journal.json", "enabled"} {
		if _, err := os.Stat(i.files.Path(stateDirectory + "/" + path)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("worker initialization changed persistent authority", path, err)
		}
	}
	bootstrap := &Root{stop: make(chan struct{}), initialize: i.bootstrap}
	defer closeTestResource(t, bootstrap)
	receipt, err := bootstrap.Bootstrap(rootDeadline(t), nil)
	if err != nil || receipt.Stage != infrahost.SocketActive {
		t.Fatalf("fresh bootstrap cannot initialize management: stage=%d err=%v", receipt.Stage, err)
	}
	if !reflect.DeepEqual(m.starts, []infrahost.UnitTarget{infrahost.UnitHelperSocket}) || n.writes != 1 || m.reloads != 1 {
		t.Fatal("wrong bootstrap effects", m.starts, n.writes, m.reloads)
	}
	policy, err := i.files.Read(stateDirectory+"/policy.json", c.MaxRequestBytes)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := i.worker()
	if err != nil {
		t.Fatal("enabled installation cannot compose real worker", err)
	}
	// origin: S9 internal review rv-infra F03 — a sent reply alone cannot
	// release the last helper reference. An absent Ack consumes the original
	// custody budget; cancellation joins the same wait without inventing success.
	claim := c.HostRequest{Op: c.LaunchClaim, Tx: c.ID{1}, Step: 1, Component: c.NATS, Payload: c.Digest{1}}
	deadline, err := controlbudget.After(30 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := awaitLaunchAck(worker.journal, claim, deadline, nil); !controlbudget.IsExpired(err) {
		t.Fatal("missing Ack released custody before its deadline", err)
	}
	stop := make(chan struct{})
	close(stop)
	if err := awaitLaunchAck(worker.journal, claim, rootDeadline(t), stop); !errors.Is(err, infrahost.ErrStopped) {
		t.Fatal("custody cancellation was not joined", err)
	}
	closeTestResource(t, worker)
	if _, err := bootstrap.Bootstrap(rootDeadline(t), nil); err != nil {
		t.Fatal("bootstrap retry failed", err)
	}
	again, err := i.files.Read(stateDirectory+"/policy.json", c.MaxRequestBytes)
	if err != nil || !bytes.Equal(policy, again) || n.writes != 1 {
		t.Fatal("bootstrap rotated authority or rewrote ingress baseline", err)
	}
	// A partial first installation may have committed policy/seeds but not its
	// genesis journal. Strict worker open must refuse it; explicit bootstrap is
	// still able to initialize that missing journal under the original authority.
	if err := os.Remove(i.files.Path(stateDirectory + "/journal.json")); err != nil {
		t.Fatal(err)
	}
	if root, err := i.worker(); err == nil || root != nil {
		t.Fatal("worker initialized a missing journal")
	}
	if _, err := bootstrap.Bootstrap(rootDeadline(t), nil); err != nil {
		t.Fatal("existing policy prevented missing-journal initialization", err)
	}
	worker, err = i.worker()
	if err != nil {
		t.Fatal("partial bootstrap did not recover", err)
	}
	closeTestResource(t, worker)
	again, err = i.files.Read(stateDirectory+"/policy.json", c.MaxRequestBytes)
	if err != nil || !bytes.Equal(policy, again) {
		t.Fatal("partial bootstrap changed authority", err)
	}
	if err := i.files.Replace(stateDirectory+"/policy.json", []byte("damaged"), 0600, i.gid); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Bootstrap(rootDeadline(t), nil); err == nil {
		t.Fatal("damaged authority silently regenerated")
	}
	if root, err := i.worker(); err == nil || root != nil {
		t.Fatal("damaged authority opened a worker")
	}
}

// closeTestResource keeps cleanup failures visible while allowing the remaining owners to close.
func closeTestResource(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil {
		t.Error(err)
	}
}
