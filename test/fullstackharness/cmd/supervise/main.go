// supervise is a test-only launcher for Python/shell and container-side fixtures.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/test/fullstackharness"
)

func main() { os.Exit(run()) }

type launcherSignal syscall.Signal

func (s launcherSignal) Error() string          { return "supervise: owner requested a signal" }
func (s launcherSignal) Signal() syscall.Signal { return syscall.Signal(s) }

// Retain the actual signal as the cancellation cause. NotifyContext discards
// it, so TERM used to close the lease and SIGKILL the target before its handler
// could join business children and return its native143.
// origin: S10 internal review FIX-SUPERVISE-2 F2
func launcherContext() (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(context.Background()) // ctx-root: test launcher signal lifetime.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case value := <-signals:
			// origin: hot-update S10 lint L3a / forcetypeassert
			if sig, ok := value.(syscall.Signal); ok {
				cancel(launcherSignal(sig))
			} else {
				cancel(errors.New("supervise: unsupported owner signal"))
			}
		case <-ctx.Done():
		}
	}()
	return ctx, func() {
		signal.Stop(signals)
		cancel(nil)
		<-done
	}
}

// Return through the cleanup stack before main exits the process.
// origin: hot-update S10 plan §6 / LINT-s10
func run() (code int) {
	leaseFD := flag.Int("lease-fd", -1, "remote heartbeat pipe FD; send '.' until Join")
	leaseTimeout := flag.Duration("lease-timeout", 5*time.Second, "maximum time without a remote heartbeat")
	readyFD := flag.Int("ready-fd", -1, "FD for one JSON launcher/target census receipt")
	cgroup := flag.String("cgroup", "", "already registered kernel cgroup path; required with a remote lease")
	logPath := flag.String("log", "", "tee child stdout and stderr to this private file")
	receiptPath := flag.String("receipt", "", "write the final native status after process cleanup")
	flag.Parse()
	if flag.NArg() == 0 || *leaseFD < -1 || *readyFD < -1 || (*leaseFD >= 0 && *leaseFD == *readyFD) {
		_, _ = fmt.Fprintln(os.Stderr, "supervise: command and distinct lease/READY descriptors required")
		return 126
	}
	// Open evidence before starting anything. An unwritable path must not leave
	// a running child, and the final receipt includes the shared reaper's 125.
	// origin: hot-update S6 plan §10.1 / T58; DRILL-ASSETS / supervisor evidence protocol.
	var receipt, log *os.File
	if *receiptPath != "" {
		var err error
		receipt, err = os.OpenFile(*receiptPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "supervise: receipt setup failed")
			return 126
		}
		defer func() {
			_, writeErr := fmt.Fprintln(receipt, code)
			syncErr, closeErr := receipt.Sync(), receipt.Close()
			if writeErr != nil || syncErr != nil || closeErr != nil {
				_, _ = fmt.Fprintln(os.Stderr, "supervise: receipt cleanup failed")
				if code == 0 {
					code = 126
				}
			}
		}()
	}
	if *logPath != "" {
		var err error
		log, err = os.OpenFile(*logPath, os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "supervise: log setup failed")
			return 126
		}
		defer func() {
			syncErr, closeErr := log.Sync(), log.Close()
			if syncErr != nil || closeErr != nil {
				_, _ = fmt.Fprintln(os.Stderr, "supervise: log cleanup failed")
				if code == 0 {
					code = 126
				}
			}
		}()
		if receipt != nil {
			logInfo, logErr := log.Stat()
			receiptInfo, receiptErr := receipt.Stat()
			if logErr != nil || receiptErr != nil || os.SameFile(logInfo, receiptInfo) {
				_, _ = fmt.Fprintln(os.Stderr, "supervise: log and receipt must be distinct files")
				return 126
			}
		}
		if err := log.Truncate(0); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "supervise: log setup failed")
			return 126
		}
	}
	options := fullstackharness.LauncherOptions{LeaseTimeout: *leaseTimeout, Cgroup: *cgroup}
	if *leaseFD >= 0 {
		options.Lease = os.NewFile(uintptr(*leaseFD), "fullstack-owner-lease")
	}
	if *readyFD >= 0 {
		ready := os.NewFile(uintptr(*readyFD), "fullstack-process-ready")
		defer func() {
			if err := ready.Close(); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, "supervise: READY descriptor cleanup failed")
				if code == 0 {
					code = 126
				}
			}
		}()
		options.Ready = ready
	}
	// The launcher owns cancellation; the shared reaper still outlives SIGKILL.
	ctx, stop := launcherContext()
	cmd := exec.Command(flag.Arg(0), flag.Args()[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if log != nil {
		// Tee rather than replace the inherited streams: existing callers still
		// receive business output while the driver retains one combined log.
		cmd.Stdout, cmd.Stderr = io.MultiWriter(os.Stdout, log), io.MultiWriter(os.Stderr, log)
	}
	if *leaseFD == 0 {
		cmd.Stdin = nil // A remote stdin lease cannot also be business input.
	}
	joinInherited, err := watchInheritedChildren()
	if err != nil {
		stop()
		_, _ = fmt.Fprintln(os.Stderr, "supervise: inherited wait setup failed")
		return 126
	}
	err = fullstackharness.RunLauncher(ctx, cmd, options)
	inheritedErr := joinInherited()
	if inheritedErr != nil {
		_, _ = fmt.Fprintln(os.Stderr, inheritedErr) // Only PID/start-tick wait diagnostics; no command payload.
	}
	err = errors.Join(err, inheritedErr)
	stop()
	if err != nil {
		// Never print argv/env or command payloads in a supervision diagnostic.
		// A nonzero command status alone says nothing about cleanup. Only the
		// shared reaper/inherited waiter can certify a failed process census.
		// origin: S10 internal review FIX-SUPERVISE-2 F1
		var supervision interface{ SupervisorExitCode() int }
		var exit *exec.ExitError
		switch {
		case errors.As(err, &supervision):
			_, _ = fmt.Fprintf(os.Stderr, "supervise: process cleanup failed with status %d\n", supervision.SupervisorExitCode())
		case errors.As(err, &exit):
			_, _ = fmt.Fprintf(os.Stderr, "supervise: command exited with status %d\n", fullstackharness.ProcessExitCode(err))
		default:
			_, _ = fmt.Fprintln(os.Stderr, "supervise: launcher setup or ownership failed")
		}
	}
	return fullstackharness.ProcessExitCode(err)
}
