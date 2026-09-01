// Package spawnexec isolates a potentially wedged target exec from the Go
// runtime that owns the agent. A goroutine around exec.Cmd.Start is not enough:
// if the child blocks inside execve, the parent Start can remain in a runtime
// state that prevents stop-the-world GC, freezing every timer and heartbeat.
package spawnexec

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

const helperEnv = "_TETHER_SPAWN_EXEC_HELPER"

// Dispatch helper mode from the imported package itself. Prepare uses
// /proc/self/exe, which is cmd/tether in production but a package-specific
// *.test binary under go test. Requiring every possible caller to remember a
// TestMain hook caused unhooked E2E test binaries to recursively run their full
// suites until the host exhausted memory (2026-09-01). Package initialization
// runs before application/test initialization, so this is the single closed
// dispatch boundary for every binary that can call Prepare.
func init() {
	if os.Getenv(helperEnv) != "1" {
		return
	}
	code, ok := maybeRun()
	if ok {
		os.Exit(code)
	}
}

type spec struct {
	Path string   `json:"path"`
	Args []string `json:"args"`
	// Do not use omitempty: nil means inherit the helper environment, while an
	// explicitly empty slice means launch with an empty environment.
	Env []string `json:"env"`
	Dir string   `json:"dir,omitempty"`
}

type status struct {
	Ready bool   `json:"ready,omitempty"`
	Error string `json:"error,omitempty"`
}

const globalWedgeCeiling = 64

// Handshake owns the two private pipes between the agent and helper.
type Handshake struct {
	specR, specW     *os.File
	statusR, statusW *os.File
	target           spec
	once             sync.Once
	stateMu          sync.Mutex
	canceled         bool
}

// Prepare wraps target in a local /proc/self/exe helper. The returned command
// inherits target's stdio but not its Path/Dir/Env; those cross the private spec
// pipe and are applied only inside the helper process.
func Prepare(target *exec.Cmd) (*exec.Cmd, *Handshake, error) {
	if target == nil || target.Path == "" || len(target.Args) == 0 {
		return nil, nil, fmt.Errorf("spawnexec: incomplete target")
	}
	specR, specW, err := os.Pipe()
	if err != nil {
		return nil, nil, fmt.Errorf("spawnexec: spec pipe: %w", err)
	}
	statusR, statusW, err := os.Pipe()
	if err != nil {
		_ = specR.Close()
		_ = specW.Close()
		return nil, nil, fmt.Errorf("spawnexec: status pipe: %w", err)
	}
	h := &Handshake{specR: specR, specW: specW, statusR: statusR, statusW: statusW}
	helperEnv := stripHelperEnv(os.Environ())
	helperEnv = append(helperEnv, helperEnvKey()+"=1")
	helper := &exec.Cmd{
		Path:       "/proc/self/exe",
		Args:       []string{"tether-spawn-exec-helper"},
		Env:        helperEnv,
		Stdin:      target.Stdin,
		Stdout:     target.Stdout,
		Stderr:     target.Stderr,
		ExtraFiles: []*os.File{specR, statusW}, // child fd 3, fd 4
	}
	h.target = spec{Path: target.Path, Args: append([]string(nil), target.Args...),
		Env: cloneAndStripHelperEnv(target.Env), Dir: target.Dir}
	return helper, h, nil
}

// target is assigned by Prepare; kept outside the serialized wire struct so the
// handshake itself owns all launch state.
func (h *Handshake) targetSpec() spec { return h.target }

// Start launches the local helper and waits until the helper reports that the
// dangerous target Start either succeeded or failed. Cancel interrupts only
// this wait; the helper/target process group remains owned by the caller.
func (h *Handshake) Start(cmd *exec.Cmd) error {
	if err := cmd.Start(); err != nil {
		h.closeAll()
		return err
	}
	_ = h.specR.Close()
	_ = h.statusW.Close()
	if err := json.NewEncoder(h.specW).Encode(h.targetSpec()); err != nil {
		_ = h.specW.Close()
		_ = h.statusR.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("spawnexec: write helper spec: %w", err)
	}
	_ = h.specW.Close()
	dec := json.NewDecoder(io.LimitReader(h.statusR, 64<<10))
	var st status
	err := dec.Decode(&st)
	if err == nil && st.Error != "" {
		_ = h.statusR.Close()
		_ = cmd.Wait()
		return fmt.Errorf("spawnexec: target start: %s", st.Error)
	}
	if err == nil && !st.Ready {
		err = fmt.Errorf("invalid pre-exec status")
	}
	if err == nil {
		st = status{}
		err = dec.Decode(&st)
		if errors.Is(err, io.EOF) {
			// fd 4 is close-on-exec. EOF after the ready record therefore proves
			// that the helper was replaced by the requested target.
			err = nil
		}
	}
	_ = h.statusR.Close()
	if err != nil {
		if !h.wasCanceled() && cmd.Process != nil {
			_ = cmd.Wait()
		}
		return fmt.Errorf("spawnexec: helper handshake: %w", err)
	}
	if st.Error != "" {
		_ = cmd.Wait() // helper reported failure and exits immediately
		return fmt.Errorf("spawnexec: target start: %s", st.Error)
	}
	return nil
}

// Cancel makes a blocked Start return without touching the helper process.
// The watchdog's reaper then Waits for that process and keeps its wedge slot.
func (h *Handshake) Cancel() {
	h.stateMu.Lock()
	h.canceled = true
	h.stateMu.Unlock()
	h.once.Do(func() {
		_ = h.specW.Close()
		_ = h.statusR.Close()
	})
}

func (h *Handshake) wasCanceled() bool {
	h.stateMu.Lock()
	defer h.stateMu.Unlock()
	return h.canceled
}

func (h *Handshake) closeAll() {
	h.once.Do(func() {
		_ = h.specR.Close()
		_ = h.specW.Close()
		_ = h.statusR.Close()
		_ = h.statusW.Close()
	})
}

// maybeRun turns the current process into the private spawn helper. It is
// intentionally package-private and is called only by init above: distributing
// this responsibility to application mains or test TestMain hooks is unsafe.
func maybeRun() (int, bool) {
	if os.Getenv(helperEnvKey()) != "1" {
		return 0, false
	}
	_ = os.Unsetenv(helperEnvKey()) // a target that is itself tether must not recurse
	specFile := os.NewFile(3, "spawnexec-spec")
	statusFile := os.NewFile(4, "spawnexec-status")
	if specFile == nil || statusFile == nil {
		return 127, true
	}
	defer func() { _ = specFile.Close() }()
	defer func() { _ = statusFile.Close() }()
	var sp spec
	if err := json.NewDecoder(io.LimitReader(specFile, 16<<20)).Decode(&sp); err != nil {
		_ = json.NewEncoder(statusFile).Encode(status{Error: "decode spec: " + err.Error()})
		return 127, true
	}
	// syscall.Exec does not run defers. Close the consumed private spec pipe now
	// so fd 3 cannot leak into a long-lived target process.
	_ = specFile.Close()
	if sp.Path == "" || len(sp.Args) == 0 {
		_ = json.NewEncoder(statusFile).Encode(status{Error: "incomplete target spec"})
		return 127, true
	}
	// Acquire before Chdir: cwd itself may be a symlink-hidden remote path and
	// wedge in the kernel. The cross-restart ceiling must cover that operation
	// as well as the final execve.
	slotFD, err := acquireGlobalWedgeSlot()
	if err != nil {
		_ = json.NewEncoder(statusFile).Encode(status{Error: err.Error()})
		return 127, true
	}
	defer func() { _ = syscall.Close(slotFD) }()
	if sp.Dir != "" {
		if err := os.Chdir(sp.Dir); err != nil {
			_ = json.NewEncoder(statusFile).Encode(status{Error: "chdir: " + err.Error()})
			return 127, true
		}
	}
	// No second fork is allowed here. The previous implementation called
	// exec.Cmd.Start from this Go helper. A wedged target left both a full helper
	// and its fork child resident; after agent restarts those orphans accumulated
	// without bound and caused the 2026-09-01 host-wide OOM. Instead this process
	// becomes the target directly. fd 4 and the global slot are CLOEXEC: their EOF
	// / release is the success acknowledgement; on failure Exec returns and we
	// report the error.
	syscall.CloseOnExec(int(statusFile.Fd()))
	if err := json.NewEncoder(statusFile).Encode(status{Ready: true}); err != nil {
		return 127, true
	}
	env := sp.Env
	if env == nil {
		env = os.Environ()
	}
	env = stripHelperEnv(env)
	if err := syscall.Exec(sp.Path, sp.Args, env); err != nil {
		_ = json.NewEncoder(statusFile).Encode(status{Error: err.Error()})
		return 127, true
	}
	// syscall.Exec cannot return on success.
	return 127, true
}

func helperEnvKey() string { return helperEnv }

func cloneAndStripHelperEnv(env []string) []string {
	if env == nil {
		return nil
	}
	out := make([]string, 0, len(env))
	return append(out, stripHelperEnv(env)...)
}

func stripHelperEnv(env []string) []string {
	prefix := helperEnvKey() + "="
	out := make([]string, 0, len(env))
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			out = append(out, entry)
		}
	}
	return out
}

// acquireGlobalWedgeSlot bounds abandoned pre-exec helpers across agent
// restarts. Each abstract AF_UNIX address is a kernel-owned lease. The socket
// is CLOEXEC, so a successful target exec releases it immediately; a wedged
// exec retains it until the kernel operation/process actually disappears.
func acquireGlobalWedgeSlot() (int, error) {
	name := "tether-spawnexec-" + strconv.Itoa(os.Getuid())
	return acquireWedgeSlot(name, globalWedgeCeiling)
}

func acquireWedgeSlot(name string, ceiling int) (int, error) {
	for i := 0; i < ceiling; i++ {
		fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_DGRAM, 0)
		if err != nil {
			return -1, fmt.Errorf("global wedge slot socket: %w", err)
		}
		syscall.CloseOnExec(fd)
		addr := &syscall.SockaddrUnix{Name: "\x00" + name + "-" + strconv.Itoa(i)}
		if err := syscall.Bind(fd, addr); err == nil {
			return fd, nil
		} else if !errors.Is(err, syscall.EADDRINUSE) {
			_ = syscall.Close(fd)
			return -1, fmt.Errorf("global wedge slot bind: %w", err)
		}
		_ = syscall.Close(fd)
	}
	return -1, fmt.Errorf("global wedge ceiling reached (%d)", ceiling)
}
