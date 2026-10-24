// tether-infra-helper has no business bootstrap or reexec dispatch. It accepts
// only fixed management commands and one systemd-activated Unix stream.
// origin: hot-update S9 plan §6.1 / T92
package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/infraroot"
)

func main() {
	if err := sanitizeInherited(); err != nil {
		code, rc := publicError(err)
		_, _ = fmt.Fprintln(os.Stderr, code)
		os.Exit(rc)
	}
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// Accept=yes carries the connected stream in 0/1/2, so no other inherited FD
// or environment entry is authority. Adopt/duplicate the stream only AFTER this
// cleanup, before any directory open could reuse an untrusted descriptor number.
// Standard-library operations keep this entry buildable on the cold platforms.
// origin: hot-update S9 plan §6.1 / T92
func sanitizeInherited() error {
	pid, fds, invocation := os.Getenv("LISTEN_PID"), os.Getenv("LISTEN_FDS"), os.Getenv("INVOCATION_ID")
	if pid != "" && pid != strconv.Itoa(os.Getpid()) {
		return infrapriv.ErrPermission
	}
	if fds != "" && fds != "0" {
		return infrapriv.ErrPermission
	}
	if invocation != "" {
		decoded, err := hex.DecodeString(invocation)
		if err != nil || len(decoded) != 16 {
			return infrapriv.ErrPermission
		}
	}
	os.Clearenv()
	if invocation != "" {
		if err := os.Setenv("INVOCATION_ID", invocation); err != nil {
			return infrapriv.ErrPermission
		}
	}
	if runtime.GOOS != "linux" {
		return nil
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return infrapriv.ErrPermission
	}
	for _, entry := range entries {
		fd, err := strconv.ParseUint(entry.Name(), 10, 64)
		if err != nil {
			return infrapriv.ErrPermission
		}
		if fd > 2 {
			// ReadDir's own descriptor has already closed. Closing a vanished
			// entry is harmless; no root worker goroutine exists at this point.
			if file := os.NewFile(uintptr(fd), "infra-unapproved-inherited-fd"); file != nil {
				_ = file.Close()
			}
		}
	}
	return nil
}

func run(args []string, out, diagnostic io.Writer) int {
	// Fresh bootstrap must not try to open the enabled runtime first.
	// origin: S9 internal review rv-infra F01
	if len(args) == 1 && args[0] == "bootstrap" {
		return runWith(args, out, diagnostic, infraroot.OpenBootstrap)
	}
	return runWith(args, out, diagnostic, infraroot.OpenInstalled)
}

func runWith(args []string, out, diagnostic io.Writer, open func() (*infraroot.Root, error)) int {
	if len(args) != 1 || (args[0] != "version" && args[0] != "worker" && args[0] != "bootstrap") {
		_, _ = fmt.Fprintln(diagnostic, "infra_config_invalid")
		return 64
	}
	if args[0] == "version" {
		_, err := fmt.Fprintln(out, "tether-infra-helper protocol 1")
		if err != nil {
			return 74
		}
		return 0
	}
	// Platform refusal precedes policy opens, FD adoption and every host effect.
	// origin: hot-update S9 plan lead LQ1 / T92
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		_, _ = fmt.Fprintln(diagnostic, "infra_unsupported")
		return 64
	}
	if os.Geteuid() != 0 {
		_, _ = fmt.Fprintln(diagnostic, "infra_permission_denied")
		return 77
	}
	root, err := open()
	if root == nil && err == nil {
		err = c.ErrContractStub
	}
	if err == nil {
		defer func() { _ = root.Close() }()
		if args[0] == "bootstrap" {
			deadline, deadlineErr := controlbudget.After(infrapriv.WorkerBudget)
			err = deadlineErr
			if err == nil {
				_, err = root.Bootstrap(deadline, nil)
			}
		} else {
			err = serveActivated(root)
		}
	}
	if err != nil {
		// Adapter diagnostics can contain private paths/keys. Only the closed
		// public code crosses stderr; pending installation cannot exit zero.
		code, rc := publicError(err)
		_, _ = fmt.Fprintln(diagnostic, code)
		return rc
	}
	return 0
}

func publicError(err error) (string, int) {
	switch {
	case errors.Is(err, c.ErrUnsupported):
		return "infra_unsupported", 64
	case errors.Is(err, infrapriv.ErrPermission):
		return "infra_permission_denied", 77
	case errors.Is(err, c.ErrIncomplete):
		return "infra_inventory_incomplete", 75
	case errors.Is(err, infrapriv.ErrNotEnabled):
		return "infra_not_enabled", 75
	case errors.Is(err, infrapriv.ErrBusy):
		return "infra_busy", 75
	case errors.Is(err, infrapriv.ErrPolicyStale):
		return "infra_policy_stale", 77
	case errors.Is(err, infrapriv.ErrBudget), controlbudget.IsExpired(err):
		return "infra_budget_exceeded", 75
	default:
		return "infra_internal", 70
	}
}

func serveActivated(root *infraroot.Root) error {
	// Accept=yes workers inherit the connected stream as stdin (and often
	// stdout). LISTEN_FDS instead denotes Accept=no and must not be interpreted
	// as permission to create a listener or accept an arbitrary descriptor.
	if value := os.Getenv("LISTEN_PID"); value != "" && value != strconv.Itoa(os.Getpid()) {
		return infrapriv.ErrPermission
	}
	if value := os.Getenv("LISTEN_FDS"); value != "" && value != "0" {
		return infrapriv.ErrPermission
	}
	conn, err := net.FileConn(os.Stdin)
	if err != nil {
		return infrapriv.ErrPermission
	}
	stream, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		return infrapriv.ErrPermission
	}
	defer func() { _ = stream.Close() }()
	if _, err := infrapriv.PeerCredentials(stream); err != nil {
		return err
	}
	// No handoff/environment variable participates in root dispatch. Clearing
	// inherited environment also prevents privileged child configuration leaks.
	os.Clearenv()
	limit, err := infrapriv.NewWorkerLimit("/run/tether-infra")
	if err != nil {
		return err
	}
	defer func() { _ = limit.Close() }()
	deadline, err := controlbudget.After(infrapriv.WorkerBudget)
	if err != nil {
		return err
	}
	return root.ServeConn(stream, limit, deadline, nil)
}
