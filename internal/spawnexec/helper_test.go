package spawnexec

import (
	"bytes"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func TestHelperStartsTargetAndPreservesOutputAndExit(t *testing.T) {
	target := exec.Command("/bin/sh", "-c", "printf helper-ok; exit 7")
	helper, handshake, err := Prepare(target)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	helper.Stdout = &out
	helper.Stderr = &out
	if err := handshake.Start(helper); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	err = helper.Wait()
	ee, ok := err.(*exec.ExitError)
	if !ok || ee.ExitCode() != 7 {
		t.Fatalf("Wait=%v, want exit 7", err)
	}
	if got := out.String(); got != "helper-ok" {
		t.Fatalf("output=%q", got)
	}
}

func TestHelperReportsTargetStartFailure(t *testing.T) {
	target := &exec.Cmd{Path: "/definitely/not/a/tether-binary", Args: []string{"missing"}}
	helper, handshake, err := Prepare(target)
	if err != nil {
		t.Fatal(err)
	}
	err = handshake.Start(helper)
	if err == nil || !strings.Contains(err.Error(), "no such file") {
		t.Fatalf("start error=%v, want target ENOENT", err)
	}
}

func TestHelperDoesNotLeakPrivateModeIntoTarget(t *testing.T) {
	target := exec.Command("/bin/sh", "-c", "test -z \"$_TETHER_SPAWN_EXEC_HELPER\" && test \"$KEEP\" = yes && test ! -e /proc/self/fd/3")
	// An RPC caller can supply an explicit environment, including a collision
	// with our private key. It must neither trigger recursion nor reach target.
	target.Env = []string{helperEnv + "=1", "KEEP=yes"}
	helper, handshake, err := Prepare(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake.Start(helper); err != nil {
		t.Fatal(err)
	}
	if err := helper.Wait(); err != nil {
		t.Fatalf("private helper marker leaked to target: %v", err)
	}
}

func TestHelperExecsTargetInPlace(t *testing.T) {
	// Regression for the 2026-09-01 host OOM: the old helper used cmd.Start,
	// retaining a full Go helper plus a fork child for every wedged exec. The
	// target must now replace the helper and therefore keep the same PID.
	target := exec.Command("/bin/sh", "-c", "printf %s $$")
	helper, handshake, err := Prepare(target)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	helper.Stdout = &out
	if err := handshake.Start(helper); err != nil {
		t.Fatal(err)
	}
	pid := helper.Process.Pid
	if err := helper.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(out.String()); got != strconv.Itoa(pid) {
		t.Fatalf("target pid=%q helper pid=%d: helper forked instead of execing in place", got, pid)
	}
}

func TestHelperPreservesExplicitEmptyEnvironment(t *testing.T) {
	target := exec.Command("/usr/bin/env")
	target.Env = []string{}
	helper, handshake, err := Prepare(target)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	helper.Stdout = &out
	if err := handshake.Start(helper); err != nil {
		t.Fatal(err)
	}
	if err := helper.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "" {
		t.Fatalf("explicit empty environment became inherited: %q", got)
	}
}

func TestGlobalWedgeCeilingSurvivesProcessLifetimeBoundaries(t *testing.T) {
	// The leases live in the kernel's abstract AF_UNIX namespace rather than in
	// an Agent/Policy object. Filling every address models abandoned helpers left
	// by previous agent processes; the next helper must fail closed.
	const ceiling = 4
	name := "tether-spawnexec-test-" + strconv.Itoa(os.Getpid())
	acquire := func() (int, error) { return acquireWedgeSlot(name, ceiling) }
	fds := make([]int, 0, ceiling)
	defer func() {
		for _, fd := range fds {
			_ = syscall.Close(fd)
		}
	}()
	for len(fds) < ceiling {
		fd, err := acquire()
		if err != nil {
			t.Fatalf("acquire slot %d: %v", len(fds), err)
		}
		fds = append(fds, fd)
	}
	if fd, err := acquire(); err == nil {
		_ = syscall.Close(fd)
		t.Fatal("pre-exec helper escaped the global wedge ceiling")
	} else if !strings.Contains(err.Error(), "global wedge ceiling reached") {
		t.Fatalf("ceiling error=%v", err)
	}

	if err := syscall.Close(fds[len(fds)-1]); err != nil {
		t.Fatal(err)
	}
	fds = fds[:len(fds)-1]
	fd, err := acquire()
	if err != nil {
		t.Fatalf("released kernel lease was not reusable: %v", err)
	}
	if err := syscall.Close(fd); err != nil {
		t.Fatalf("close reacquired slot: %v", err)
	}
}
