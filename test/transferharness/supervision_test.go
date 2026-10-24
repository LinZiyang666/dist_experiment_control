package transferharness

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: S7 internal review rv-lifecycle F7
func TestTransferHarnessSharedSupervisorIsInstalled(t *testing.T) {
	if SharedSupervisor == nil {
		t.Fatal("required transfer harness has no shared process supervisor")
	}
	exerciseSupervisedTransferLifecycle(t)
}

// Real: each control is a separately supervised copy of this test binary.
// Simulated: missing custody and a completion channel without a spawned child.
// The native failure must survive the adapter; a skipped required exercise
// would exit zero and make this control fail.
// origin: S7 internal review rv-lifecycle F7
func TestTransferHarnessSupervisionControls(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for _, control := range []string{"missing-adapter", "no-process"} {
		t.Run(control, func(t *testing.T) {
			for round := 0; round < 5; round++ {
				exerciseTransferSupervisionControl(t, control)
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "transfer supervision controls", g)
	testharness.AssertNoFDLeak(t, "transfer supervision controls", fds, 0)
}

func exerciseTransferSupervisionControl(t *testing.T, control string) {
	t.Helper()
	// The control's outer safety owner is not the factory being disabled in
	// the victim runner. Even a failing mutant must join its actual process.
	h := New(t, sharedProcessSupervisor{})
	defer func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	}()
	spec := privateProbeBinary(t, h)
	spec.Args = []string{"-test.run=^TestTransferHarnessSupervisionControlProbe$", "-test.count=1"}
	spec.Env = []string{"TRANSFER_SUPERVISION_CONTROL=" + control}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	p, err := h.Start(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := p.Wait(ctx)
	if err == nil || receipt.NativeRC != 1 {
		t.Fatalf("%s control did not fail natively: rc=%d err=%v", control, receipt.NativeRC, err)
	}
	body, readErr := os.ReadFile(receipt.StdoutPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	want := "required transfer harness has no shared process supervisor"
	if control == "no-process" {
		want = "supervised process did not publish its real PID"
	}
	if !strings.Contains(string(body), want) {
		t.Fatalf("%s control failed for the wrong reason: %s", control, body)
	}
}

// origin: S7 internal review rv-lifecycle F7
func TestTransferHarnessSupervisionControlProbe(t *testing.T) {
	switch os.Getenv("TRANSFER_SUPERVISION_CONTROL") {
	case "missing-adapter":
		SharedSupervisor = nil
	case "no-process":
		SharedSupervisor = func(*testing.T) Supervisor { return completionWithoutProcess{} }
	default:
		return
	}
	exerciseSupervisedTransferLifecycle(t)
}

type completionWithoutProcess struct{}

func (completionWithoutProcess) Launch(*exec.Cmd) (Child, error) {
	done := make(chan Exit, 1)
	done <- Exit{NativeRC: 75}
	close(done)
	// Syntactically complete custody still cannot prove that a child started.
	return Child{PID: os.Getpid(), Done: done, Stop: func() error { return nil }}, nil
}

func privateProbeBinary(t *testing.T, h *Harness) Spec {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	candidate := filepath.Join(h.Root, "harness-probe")
	if err := os.WriteFile(candidate, data, 0o700); err != nil {
		t.Fatal(err)
	}
	sha, err := FileSHA(candidate)
	if err != nil {
		t.Fatal(err)
	}
	return Spec{Binary: candidate, ArtifactSHA: sha}
}
