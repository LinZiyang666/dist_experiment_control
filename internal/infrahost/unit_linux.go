//go:build linux

package infrahost

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infraunit"
	"golang.org/x/sys/unix"
)

func unitPlatform() error {
	if runtime.GOARCH != "amd64" {
		return infracontract.ErrUnsupported
	}
	return nil
}

type systemdManager struct {
	run func([]string, infracontract.Deadline, infracontract.Stop) ([]byte, error)
}

// NewSystemdManager does not launch a process or mutate the host. Bootstrap's
// root preflight supplies kernel/tool/policy support validation before effects.
func NewSystemdManager() (UnitManager, error) {
	if err := unitPlatform(); err != nil {
		return nil, err
	}
	return &systemdManager{run: runSystemctl}, nil
}

func (m *systemdManager) Read(target UnitTarget, deadline infracontract.Deadline, stop infracontract.Stop) (EffectiveUnit, error) {
	name, err := UnitName(target)
	if err != nil {
		return EffectiveUnit{}, err
	}
	return m.readNamedUnit(target, name, deadline, stop)
}

func (m *systemdManager) readNamedUnit(target UnitTarget, name string, deadline infracontract.Deadline, stop infracontract.Stop) (EffectiveUnit, error) {
	names := infraunit.PropertyNames(target)
	data, err := m.run([]string{"--no-pager", "--full", "show", "--all", "--property=" + strings.Join(names, ","), name}, deadline, stop)
	if err != nil {
		return EffectiveUnit{}, err
	}
	return ParseEffectiveUnit(target, data)
}

func (m *systemdManager) ReadHelper(process infracontract.Process, deadline infracontract.Deadline, stop infracontract.Stop) (EffectiveUnit, error) {
	name := filepath.Base(process.Cgroup)
	if !unitProcessKnown(process) || !strings.HasPrefix(name, "tether-infra-helper@") ||
		!strings.HasSuffix(name, ".service") || len(name) > 255 || name == "tether-infra-helper@.service" {
		return EffectiveUnit{}, ErrUnitPermission
	}
	for _, ch := range name {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || strings.ContainsRune("@._:-\\", ch) {
			continue
		}
		return EffectiveUnit{}, ErrUnitPermission
	}
	unit, err := m.readNamedUnit(UnitHelperService, name, deadline, stop)
	if err != nil {
		return EffectiveUnit{}, err
	}
	if unit.Name != name {
		return EffectiveUnit{}, ErrUnitPermission
	}
	// Only the instance name is projected to the fixed audit identity. All
	// effective overrides/dependencies still compare against the trusted policy.
	unit.Name, _ = UnitName(UnitHelperService)
	return unit, nil
}

func (m *systemdManager) Reload(deadline infracontract.Deadline, stop infracontract.Stop) error {
	_, err := m.run([]string{"--no-pager", "daemon-reload"}, deadline, stop)
	return err
}

func (m *systemdManager) Start(target UnitTarget, deadline infracontract.Deadline, stop infracontract.Stop) error {
	switch target {
	case UnitNATS, UnitCaddy, UnitCaddyWarm, UnitHelperSocket:
	default:
		return ErrUnitInvalid
	}
	name, err := UnitName(target)
	if err != nil {
		return err
	}
	// origin: hot-update S9 plan §6.1 / T13 — waiting for the job deadlocks
	// when its launcher calls back into the worker to claim the same permit.
	_, err = m.run([]string{"--no-pager", "--no-block", "start", name}, deadline, stop)
	return err
}

func (m *systemdManager) Fence(target UnitTarget, deadline infracontract.Deadline, stop infracontract.Stop) error {
	var component infracontract.Component
	switch target {
	case UnitNATS:
		component = infracontract.NATS
	case UnitCaddy:
		component = infracontract.Caddy
	default:
		return ErrUnitInvalid
	}
	if err := unitBudget(deadline, stop); err != nil {
		return err
	}
	name, _ := UnitName(target)
	data, err := ManagedUnitDropIn(component)
	if err != nil {
		return err
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return ErrUnitPermission
	}
	for _, part := range []string{"etc", "systemd", "system"} {
		next, openErr := openTrustedUnitChild(fd, part, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return ErrUnitPermission
		}
		fd = next
	}
	defer func() { _ = unix.Close(fd) }()
	directory := name + ".d"
	if err := unix.Mkdirat(fd, directory, 0755); err != nil && !errors.Is(err, unix.EEXIST) {
		return ErrUnitPermission
	}
	child, err := openTrustedUnitChild(fd, directory, 0)
	if err != nil {
		return ErrUnitPermission
	}
	defer func() { _ = unix.Close(child) }()
	if err := replaceManagedDropIn(child, data, 0); err != nil {
		return err
	}
	if err := unix.Fsync(fd); err != nil {
		return ErrUnitUnknown
	}
	if err := m.Reload(deadline, stop); err != nil {
		return err
	}
	return nil
}

func openTrustedUnitChild(parent int, name string, owner uint32) (int, error) {
	fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, ErrUnitPermission
	}
	if err := trustedUnitDirectory(fd, owner); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

func trustedUnitDirectory(fd int, owner uint32) error {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != owner || st.Mode&0022 != 0 {
		return ErrUnitPermission
	}
	return nil
}

// The dirfd anchors every read/write/rename. An operator's same-named drop-in
// is not ours to replace; a symlink must not turn retrofit into a write elsewhere.
// origin: hot-update S9 plan §5.3 / §6.4 / T12
func replaceManagedDropIn(dirfd int, data []byte, owner uint32) error {
	const name = "90-tether-infra.conf"
	const marker = "# tether-infra owned v1\n"
	if err := trustedUnitDirectory(dirfd, owner); err != nil {
		return err
	}
	if !bytes.HasPrefix(data, []byte(marker)) {
		return ErrUnitInvalid
	}
	existing, err := unix.Openat(dirfd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err == nil {
		file := os.NewFile(uintptr(existing), name)
		var st unix.Stat_t
		statErr := unix.Fstat(existing, &st)
		prefix := make([]byte, len(marker))
		_, readErr := io.ReadFull(file, prefix)
		closeErr := file.Close()
		if statErr != nil || readErr != nil || closeErr != nil || st.Mode&unix.S_IFMT != unix.S_IFREG ||
			st.Uid != owner || st.Mode&0022 != 0 || string(prefix) != marker {
			return ErrUnitPermission
		}
	} else if !errors.Is(err, unix.ENOENT) {
		return ErrUnitPermission
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return ErrUnitUnknown
	}
	temporary := ".tether-infra-" + hex.EncodeToString(nonce[:])
	fd, err := unix.Openat(dirfd, temporary, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0644)
	if err != nil {
		return ErrUnitPermission
	}
	defer func() { _ = unix.Unlinkat(dirfd, temporary, 0) }()
	file := os.NewFile(uintptr(fd), temporary)
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return ErrUnitUnknown
	}
	if err := file.Chmod(0644); err != nil {
		_ = file.Close()
		return ErrUnitUnknown
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return ErrUnitUnknown
	}
	if err := file.Close(); err != nil {
		return ErrUnitUnknown
	}
	if err := unix.Renameat(dirfd, temporary, dirfd, name); err != nil {
		return ErrUnitUnknown
	}
	if err := unix.Fsync(dirfd); err != nil {
		return ErrUnitUnknown
	}
	return nil
}

type unitOutput struct {
	mu        sync.Mutex
	data      []byte
	oversized bool
}

func (w *unitOutput) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(data) > (64<<10)-len(w.data) {
		w.oversized = true
		return 0, infracontract.ErrIncomplete
	}
	w.data = append(w.data, data...)
	return len(data), nil
}

func runSystemctl(args []string, deadline infracontract.Deadline, stop infracontract.Stop) ([]byte, error) {
	return runUnitCommand("/usr/bin/systemctl", args, deadline, stop, func(cmd *exec.Cmd) error { return cmd.Run() })
}

// commandRun is an in-package test seam for the shared process supervisor. The
// only production caller supplies the fixed absolute systemctl path above.
func runUnitCommand(path string, args []string, deadline infracontract.Deadline, stop infracontract.Stop, commandRun func(*exec.Cmd) error) ([]byte, error) {
	if err := unitBudget(deadline, stop); err != nil {
		return nil, err
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, ErrUnitBudget
	}
	// ctx-root: one bounded systemd operation owns cancellation and joins its watcher and child.
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	defer cancel()
	joined := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-stop:
			cancel()
		case <-done:
		case <-ctx.Done():
		}
	}()
	defer func() { close(done); <-joined }()
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C", "SYSTEMD_COLORS=0", "SYSTEMD_PAGER=cat"}
	cmd.Dir = "/"
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = time.Second
	var output unitOutput
	cmd.Stdout, cmd.Stderr = &output, io.Discard
	err = commandRun(cmd) // Run joins the child and stdout pump on every return path.
	if ctx.Err() != nil {
		return nil, ErrUnitBudget
	}
	if output.oversized {
		return nil, infracontract.ErrIncomplete
	}
	if err != nil {
		// systemctl diagnostics can quote environment/credential values.
		return nil, ErrUnitUnknown
	}
	return output.data, nil
}
