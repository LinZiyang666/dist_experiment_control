//go:build linux

package infrahost

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
)

type unitTestManager struct {
	units          [7]EffectiveUnit
	started        []UnitTarget
	fenced         []UnitTarget
	reloaded       int
	inactive       bool
	startError     error
	helperOverride *EffectiveUnit
	start          func(UnitTarget) error
}

func (m *unitTestManager) Read(target UnitTarget, d infracontract.Deadline, stop infracontract.Stop) (EffectiveUnit, error) {
	if err := unitBudget(d, stop); err != nil {
		return EffectiveUnit{}, err
	}
	return m.units[target], nil
}

func (m *unitTestManager) ReadHelper(_ infracontract.Process, d infracontract.Deadline, stop infracontract.Stop) (EffectiveUnit, error) {
	if m.helperOverride != nil {
		return *m.helperOverride, nil
	}
	return m.Read(UnitHelperService, d, stop)
}
func (m *unitTestManager) Reload(d infracontract.Deadline, stop infracontract.Stop) error {
	if err := unitBudget(d, stop); err != nil {
		return err
	}
	m.reloaded++
	return nil
}
func (m *unitTestManager) Start(target UnitTarget, d infracontract.Deadline, stop infracontract.Stop) error {
	if err := unitBudget(d, stop); err != nil {
		return err
	}
	m.started = append(m.started, target)
	if m.startError != nil {
		return m.startError
	}
	if m.start != nil {
		if err := m.start(target); err != nil {
			return err
		}
	}
	if target == UnitHelperSocket && !m.inactive {
		m.units[target].ActiveState, m.units[target].SubState = "active", "listening"
	}
	return nil
}
func (m *unitTestManager) Fence(target UnitTarget, d infracontract.Deadline, stop infracontract.Stop) error {
	if err := unitBudget(d, stop); err != nil {
		return err
	}
	m.fenced = append(m.fenced, target)
	m.units[target] = unitFixture(target, false).Expected
	return m.Reload(d, stop)
}

type unitTestSetup struct {
	environment         BootstrapEnvironment
	initialize, install int
	verification        HelperVerification
	verify              func() error
	error               error
}

func (s *unitTestSetup) Preflight(infracontract.Deadline, infracontract.Stop) (BootstrapEnvironment, error) {
	return s.environment, s.error
}
func (s *unitTestSetup) Initialize(infracontract.Deadline, infracontract.Stop) error {
	s.initialize++
	return s.error
}
func (s *unitTestSetup) InstallManagement(infracontract.Deadline, infracontract.Stop) error {
	s.install++
	return s.error
}
func (s *unitTestSetup) VerifyHelper(infracontract.Deadline, infracontract.Stop) (HelperVerification, error) {
	if s.verify != nil {
		if err := s.verify(); err != nil {
			return HelperVerification{}, err
		}
	}
	return s.verification, s.error
}

type unitTestPhysical struct {
	processes []infracontract.Process
	read      func() []infracontract.Process
	inspected int
	error     error
	signals   []infracontract.HostRequest
}

func (p *unitTestPhysical) Inspect(infracontract.ID, infracontract.Deadline) (infracontract.Census, error) {
	p.inspected++
	processes := append([]infracontract.Process(nil), p.processes...)
	if p.read != nil {
		processes = p.read()
	}
	return infracontract.Census{Complete: true, Generation: uint64(p.inspected), Processes: processes,
		LiveBusiness: 1, Digest: infracontract.Digest(sha256.Sum256([]byte(fmt.Sprint(processes))))}, p.error
}
func (p *unitTestPhysical) Do(r infracontract.HostRequest, _ infracontract.Deadline, _ infracontract.Stop) (infracontract.HostReceipt, error) {
	p.signals = append(p.signals, r)
	if r.Op != infracontract.RetireWarm || len(p.processes) != 1 || r.Expected != p.processes[0] {
		return infracontract.HostReceipt{}, ErrUnitNotReady
	}
	return infracontract.HostReceipt{Tx: r.Tx, Sequence: r.Sequence, Op: r.Op, Applied: true}, p.error
}

func unitTestDeadline(t *testing.T, duration time.Duration) infracontract.Deadline {
	t.Helper()
	d, err := controlbudget.After(duration)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func unitTestProcess(t *testing.T, pid int) infracontract.Process {
	t.Helper()
	root := "/proc/" + strconv.Itoa(pid)
	stat, err := os.ReadFile(root + "/stat")
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(stat)[strings.LastIndexByte(string(stat), ')')+1:])
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(root + "/exe")
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.ReadFile(root + "/exe")
	if err != nil {
		t.Fatal(err)
	}
	cgroup, err := os.ReadFile(root + "/cgroup")
	if err != nil {
		t.Fatal(err)
	}
	identity := info.Sys().(*syscall.Stat_t)
	return infracontract.Process{BootID: strings.TrimSpace(string(boot)), PID: int32(pid), StartTicks: ticks,
		InvocationID: "supervised-fixture-" + strconv.Itoa(pid) + "-" + strconv.FormatUint(ticks, 10),
		ExeDevice:    uint64(identity.Dev), ExeInode: identity.Ino, ExeSHA256: infracontract.Digest(sha256.Sum256(exe)), Cgroup: strings.TrimSpace(string(cgroup))}
}

func bootstrapFixture(t *testing.T) (BootstrapDependencies, *unitTestManager, *unitTestSetup, *unitTestPhysical) {
	t.Helper()
	m := &unitTestManager{}
	var policies []UnitPolicy
	for _, target := range []UnitTarget{UnitNATS, UnitCaddy, UnitHelperSocket, UnitHelperService} {
		policy := unitFixture(target, target == UnitNATS || target == UnitCaddy)
		policies = append(policies, policy)
		m.units[target] = policy.Expected
	}
	p := unitTestProcess(t, os.Getpid())
	s := &unitTestSetup{environment: BootstrapEnvironment{UID: 0, Systemd: true, PidFD: true, Cgroup: true, NFT: true, IPv6: true, PolicyTrusted: true, FilesInstalled: true},
		verification: HelperVerification{Version: 1, Running: p, Catalog: infracontract.Digest{1}}}
	physical := &unitTestPhysical{processes: []infracontract.Process{p}}
	return BootstrapDependencies{Manager: m, Setup: s, Physical: physical, Policies: policies}, m, s, physical
}

// origin: hot-update S9 plan §6.2 / §6.4 / T13 / M07
func TestInfraBootstrapLeavesBusinessRunning(t *testing.T) {
	t.Run("inactive-socket-live-business", exerciseUnit)
	t.Run("refuses-before-effects", func(t *testing.T) {
		for _, defect := range []string{"uid", "policy", "systemd", "pidfd", "cgroup", "nft", "ipv6", "files", "masked", "disabled", "override", "duplicate-target", "missing-target", "unknown-process", "empty-census", "missing-setup"} {
			t.Run(defect, func(t *testing.T) {
				deps, m, s, p := bootstrapFixture(t)
				switch defect {
				case "uid":
					s.environment.UID = 1001
				case "policy":
					s.environment.PolicyTrusted = false
				case "systemd":
					s.environment.Systemd = false
				case "pidfd":
					s.environment.PidFD = false
				case "cgroup":
					s.environment.Cgroup = false
				case "nft":
					s.environment.NFT = false
				case "ipv6":
					s.environment.IPv6 = false
				case "files":
					s.environment.FilesInstalled = false
				case "masked":
					m.units[UnitHelperSocket].UnitFileState = "masked"
				case "disabled":
					m.units[UnitHelperSocket].UnitFileState = "disabled"
				case "override":
					m.units[UnitCaddy].Exec[0] = "operator hook"
				case "duplicate-target":
					deps.Policies[1] = deps.Policies[0]
				case "missing-target":
					deps.Policies = deps.Policies[:3]
				case "unknown-process":
					p.processes[0].StartTicks = 0
				case "empty-census":
					p.processes = nil
				case "missing-setup":
					deps.Setup = nil
				}
				receipt, err := Bootstrap(deps, unitTestDeadline(t, time.Second), nil)
				if err == nil || receipt.Stage != 0 || s.initialize != 0 || s.install != 0 || m.reloaded != 0 || len(m.started) != 0 {
					t.Fatalf("before-effect guard bypassed: stage=%d err=%v effects=%d/%d/%d/%v", receipt.Stage, err, s.initialize, s.install, m.reloaded, m.started)
				}
			})
		}
	})
	t.Run("activation-and-version-failures", func(t *testing.T) {
		for _, defect := range []string{"inactive", "active-not-listening", "version", "catalog", "helper-identity", "helper-failure", "worker-override", "start-failure", "business-replaced"} {
			t.Run(defect, func(t *testing.T) {
				deps, m, s, p := bootstrapFixture(t)
				switch defect {
				case "inactive":
					m.inactive = true
				case "active-not-listening":
					m.inactive = true
					m.units[UnitHelperSocket].ActiveState = "active"
				case "version":
					s.verification.Version = 0
				case "catalog":
					s.verification.Catalog = infracontract.Digest{}
				case "helper-identity":
					s.verification.Running.InvocationID = ""
				case "helper-failure":
					s.verify = func() error { return errors.New("reply lost") }
				case "worker-override":
					unsafe := m.units[UnitHelperService]
					unsafe.EnvironmentFiles = "/tmp/instance-specific.env"
					m.helperOverride = &unsafe
				case "start-failure":
					m.startError = errors.New("job refused")
				case "business-replaced":
					s.verify = func() error { p.processes[0].ExeInode++; return nil }
				}
				receipt, err := Bootstrap(deps, unitTestDeadline(t, 30*time.Millisecond), nil)
				if err == nil || receipt.Stage >= EffectiveUnitVerified || !reflect.DeepEqual(m.started, []UnitTarget{UnitHelperSocket}) {
					t.Fatalf("false-ready management: stage=%d err=%v started=%v", receipt.Stage, err, m.started)
				}
			})
		}
	})
	t.Run("runtime-needs-independent-proofs", func(t *testing.T) {
		deps, _, _, _ := bootstrapFixture(t)
		receipt, err := Bootstrap(deps, unitTestDeadline(t, time.Second), nil)
		if err != nil || receipt.Stage != EffectiveUnitVerified {
			t.Fatalf("bootstrap: stage=%d err=%v", receipt.Stage, err)
		}
		ready := infracontract.Readiness{Complete: true, Running: receipt.Business[0], Topology: infracontract.Digest{1}, Auth: infracontract.Digest{2}, WSS: infracontract.Digest{3},
			Seal: infracontract.AssetSeal{Complete: true, Revision: 1, Accounts: infracontract.Digest{4}, Assets: infracontract.Digest{5}, Participants: infracontract.Digest{6}}}
		if got, err := VerifyBootstrapRuntime(receipt, ready); err != nil || got.Stage != RuntimeReady {
			t.Fatalf("runtime readiness: %v", err)
		}
		for _, defect := range []string{"bootstrap-stage", "helper", "helper-catalog", "complete", "running", "topology", "auth", "wss", "seal", "revision", "accounts", "assets", "participants"} {
			r, proof := receipt, ready
			switch defect {
			case "bootstrap-stage":
				r.Stage = FilesInstalled
			case "helper":
				r.Helper = infracontract.Process{}
			case "helper-catalog":
				r.HelperCatalog = infracontract.Digest{}
			case "complete":
				proof.Complete = false
			case "running":
				proof.Running = infracontract.Process{}
			case "topology":
				proof.Topology = infracontract.Digest{}
			case "auth":
				proof.Auth = infracontract.Digest{}
			case "wss":
				proof.WSS = infracontract.Digest{}
			case "seal":
				proof.Seal.Complete = false
			case "revision":
				proof.Seal.Revision = 0
			case "accounts":
				proof.Seal.Accounts = infracontract.Digest{}
			case "assets":
				proof.Seal.Assets = infracontract.Digest{}
			case "participants":
				proof.Seal.Participants = infracontract.Digest{}
			}
			if got, err := VerifyBootstrapRuntime(r, proof); err == nil || got.Stage == RuntimeReady {
				t.Errorf("missing %s signed runtime-ready", defect)
			}
		}
	})
	t.Run("persistent-startup-fence-and-boot-budget", testUnitStartupFence)
	t.Run("unit-jobs-and-exact-retirement", testUnitHostEffects)
	t.Run("nonblocking-start-callback", testUnitStartCallback)
	stop := make(chan struct{})
	close(stop)
	deps, m, _, _ := bootstrapFixture(t)
	if _, err := Bootstrap(deps, unitTestDeadline(t, time.Second), stop); !errors.Is(err, ErrUnitBudget) || len(m.started) != 0 {
		t.Fatal("cancelled bootstrap had effects")
	}
	if _, err := runSystemctl(nil, 0, nil); !errors.Is(err, ErrUnitBudget) {
		t.Fatal("invalid deadline may spawn systemctl")
	}
}

func exerciseUnit(t *testing.T) {
	t.Helper()
	// The live business is a supervised real process with one original pipe.
	// Its identity and byte flow are read again after management activation.
	cmd := exec.Command("/bin/cat") // The shared supervisor and returned waiter own this live child.
	inputR, inputW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inputW.Close() }()
	outputR, outputW, err := os.Pipe()
	if err != nil {
		_ = inputR.Close()
		t.Fatal(err)
	}
	defer func() { _ = outputR.Close() }()
	cmd.Stdin, cmd.Stdout = inputR, outputW
	wait, err := testharness.StartProcess(cmd)
	// A started child holds its own copies of these ends; the parent's copies
	// are released here and their close has nothing to report.
	_ = inputR.Close()
	_ = outputW.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		if err := wait(); err != nil && !strings.Contains(err.Error(), "signal: killed") {
			t.Errorf("business join: %v", err)
		}
	}()
	before := unitTestProcess(t, cmd.Process.Pid)
	echo := func(message string) {
		if err := outputR.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(inputW, message); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(message))
		if _, err := io.ReadFull(outputR, got); err != nil || string(got) != message {
			t.Fatalf("original business pipe interrupted: %v", err)
		}
	}
	echo("before-bootstrap\n")
	deps, m, s, p := bootstrapFixture(t)
	p.read = func() []infracontract.Process { return []infracontract.Process{unitTestProcess(t, cmd.Process.Pid)} }
	var listener net.Listener
	joined := make(chan struct{})
	path := filepath.Join(t.TempDir(), "control.sock")
	m.start = func(target UnitTarget) error {
		if target != UnitHelperSocket {
			return ErrUnitInvalid
		}
		if listener != nil {
			return nil
		}
		var err error
		listener, err = net.Listen("unix", path)
		if err != nil {
			return err
		}
		go func() {
			defer close(joined)
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				_, _ = io.Copy(conn, conn)
				_ = conn.Close()
			}
		}()
		return nil
	}
	defer func() {
		if listener != nil {
			_ = listener.Close()
			<-joined
		}
	}()
	s.verify = func() error {
		conn, err := net.DialTimeout("unix", path, time.Second)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }() // The probe's verdict is the echoed bytes, not the close.
		if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
			return err
		}
		const probe = "protocol=1 catalog=verified\n"
		if _, err := io.WriteString(conn, probe); err != nil {
			return err
		}
		got := make([]byte, len(probe))
		if _, err := io.ReadFull(conn, got); err != nil {
			return err
		}
		if string(got) != probe {
			return ErrUnitNotReady
		}
		return nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		receipt, err := Bootstrap(deps, unitTestDeadline(t, time.Second), nil)
		if err != nil || receipt.Stage != EffectiveUnitVerified || !reflect.DeepEqual(receipt.Business, []infracontract.Process{before}) {
			t.Fatalf("bootstrap changed live business: %v", err)
		}
		echo("after-bootstrap-" + strconv.Itoa(attempt) + "\n")
	}
	if after := unitTestProcess(t, cmd.Process.Pid); before != after {
		t.Fatal("bootstrap replaced original live business incarnation")
	}
	if !reflect.DeepEqual(m.started, []UnitTarget{UnitHelperSocket, UnitHelperSocket}) || len(m.fenced) != 0 {
		t.Fatal("bootstrap operated on a business unit")
	}
	exerciseUnitCommand(t)
}
