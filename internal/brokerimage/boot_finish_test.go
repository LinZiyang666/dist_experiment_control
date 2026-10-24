//go:build linux

// Real: dispatcher test process -> compiled tether -> previous test reader,
// all under one PID with sealed broker private state and original descriptors.
// The previous reader's probe is simulated by boot_test.go; store/control ACTIVE
// recovery remains outside this pristine bootstrap test.
package brokerimage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
)

func init() {
	if os.Getenv("BROKER_FINISH_PHASE") != "receiver" || len(os.Args) < 2 || os.Args[1] == "version" || os.Args[1] == PreflightProbeCommand {
		return
	}
	fmt.Println("BROKER_FINISH_RECEIVER_ENTERED")
	agentboot.Begin(nil)
	restored, err := agentboot.TakeRole(ImageRole)
	if err != nil || restored == nil {
		fmt.Fprintln(os.Stderr, "previous reader lost broker envelope")
		os.Exit(2)
	}
	boot, err := Boot(restored)
	if err != nil || boot.CheckInvocation(os.Args) != nil || boot.Bundle().Generation != 8 {
		fmt.Fprintln(os.Stderr, "previous reader lost sealed identity/argv", err)
		os.Exit(2)
	}
	if _, hint := os.LookupEnv("_TETHER_HANDOFF_ROLE"); hint {
		os.Exit(2)
	}
	borrowed := restored.Borrow()
	if err = boot.Close(); err != nil {
		os.Exit(2)
	}
	for _, resource := range borrowed {
		for _, file := range resource.Files {
			if _, err := file.Stat(); err == nil {
				fmt.Fprintln(os.Stderr, "Finish receiver retained a held descriptor")
				os.Exit(2)
			}
		}
	}
	fmt.Printf("BROKER_FINISH_RECEIPT pid=%d generation=8\n", os.Getpid())
	os.Exit(0)
}

// origin: hot-update S6 plan §4.1 / T03
func TestBrokerBootFinishUsesSealedArgs(t *testing.T) {
	if os.Getenv("BROKER_FINISH_PHASE") == "dispatcher" {
		fixture := newImportFixture(t)
		candidate := os.Getenv("BROKER_FINISH_CANDIDATE")
		sha, err := hashArtifact(candidate)
		if err != nil {
			t.Fatal(err)
		}
		fixture.bundle.CandidateSHA256 = sha
		fixture.bundle.Args = []string{candidate, "serve", "--config", os.Getenv("BROKER_FINISH_CONFIG")}
		refreshBootFixture(t, fixture)
		data, err := Encode(fixture.bundle)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := handoff.Prepare(handoff.State{Role: ImageRole, Instance: fixture.bundle.InstanceID, Generation: fixture.bundle.Generation, Data: data}, fixture.resources)
		if err != nil {
			t.Fatal(err)
		}
		env := replaceTestEnv(os.Environ(), "BROKER_FINISH_PHASE", "receiver")
		if err = prepared.Exec(candidate, []string{candidate, "version"}, env); err != nil {
			t.Fatal(err)
		}
		return
	}
	exerciseBrokerBootFinish(t, false)
}

// origin: hot-update S6 plan §4.1 / T03
func TestBrokerBootFinishRejectsForeignPrevious(t *testing.T) {
	exerciseBrokerBootFinish(t, true)
}

func exerciseBrokerBootFinish(t *testing.T, foreignPrevious bool) {
	t.Helper()
	dir := t.TempDir()
	candidate := filepath.Join(dir, "tether-fixture")
	build := exec.Command("go", "build", "-race", "-o", candidate, "./cmd/tether")
	build.Dir = "../.."
	// GOFLAGS is inherited so production mutations reach this inner build too.
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build candidate: %v\n%s", err, output)
	}
	input, err := os.Open(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	previous, err := os.OpenFile(candidate+".prev", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		_ = input.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(previous, input)
	if err := errors.Join(copyErr, previous.Close(), input.Close()); err != nil {
		t.Fatal(err)
	}
	if foreignPrevious {
		file, err := os.OpenFile(candidate+".prev", os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := file.WriteString("foreign previous artifact")
		if err := errors.Join(writeErr, file.Close()); err != nil {
			t.Fatal(err)
		}
	}
	config := filepath.Join(dir, "broker.yaml")
	if err := os.WriteFile(config, []byte("fixture-config"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBrokerBootFinishUsesSealedArgs$")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	cmd.Env = replaceTestEnv(os.Environ(), "BROKER_FINISH_PHASE", "dispatcher")
	cmd.Env = append(cmd.Env, "BROKER_FINISH_CANDIDATE="+candidate, "BROKER_FINISH_CONFIG="+config, "HOME="+dir)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitErr := cmd.Wait()
	if foreignPrevious {
		if waitErr == nil || strings.Contains(output.String(), "BROKER_FINISH_RECEIVER_ENTERED") {
			t.Fatalf("unvalidated previous artifact executed: %v\n%s", waitErr, &output)
		}
		return
	}
	if waitErr != nil {
		t.Fatalf("real bootstrap redispatch: %v\n%s", waitErr, &output)
	}
	want := fmt.Sprintf("BROKER_FINISH_RECEIPT pid=%d generation=8", cmd.Process.Pid)
	if !strings.Contains(output.String(), want) {
		t.Fatalf("same-PID sealed redispatch receipt missing: %s", &output)
	}
}

// Rebuild the sealed projection when a fixture changes its public lineage. The
// private validator is intentionally strict; keeping stale fixture Args would
// reject before the bootstrap guard under test ever runs.
func refreshBootFixture(t *testing.T, fixture *importFixture) {
	t.Helper()
	data, _, err := sealedstate.Read(borrowedState{fixture.resources}, fixture.bundle.StateResource, "state", sealedstate.MaxStateBytes)
	if err != nil {
		t.Fatal(err)
	}
	state, err := DecodePrivateState(data)
	if err != nil {
		t.Fatal(err)
	}
	state.Runtime = runtimeState(fixture.bundle, state.Runtime.CutID)
	encoded, err := EncodePrivateState(state)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := sealedstate.New(fixture.bundle.StateResource, encoded)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	for i, resource := range fixture.resources {
		if resource.ID == fixture.bundle.StateResource {
			fixture.resources[i] = snapshot.Resources[0]
			return
		}
	}
	t.Fatal("private fixture resource missing")
}

// origin: hot-update S6 plan §4.1 / §4.2 / T03
func TestBrokerBootFinishRefusalKeepsActiveOwner(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerBootFinishRefusal(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker Finish refusal", beforeG)
	testharness.AssertNoFDLeak(t, "broker Finish refusal", beforeFD, 0)
}

func exerciseBrokerBootFinishRefusal(t *testing.T) {
	t.Helper()
	fixture := newImportFixture(t)
	image := &imported{active: true}
	boot := &BootImage{bundle: fixture.bundle, imported: image}
	cause := errors.New("post-constructor failure")
	if err := boot.Finish(cause); !errors.Is(err, ErrActive) || !errors.Is(err, cause) || errors.Is(err, ErrImage) {
		t.Fatalf("ACTIVE Finish permitted pristine cleanup: %v", err)
	}
	if err := boot.Finish(nil); err != nil {
		t.Fatalf("completed ACTIVE stop was treated as rollback: %v", err)
	}
	if !reflect.DeepEqual(boot.Bundle(), fixture.bundle) {
		t.Fatal("Finish rewound the current lineage")
	}
	if err := boot.CheckInvocation([]string{"wrong"}); err == nil {
		t.Fatal("wrong invocation was accepted")
	}
	if err := boot.CheckInvocation(fixture.bundle.Args); err != nil {
		t.Fatal("sealed invocation was rejected")
	}
	if _, err := fixture.raw.Stat(); err != nil {
		t.Fatal("ACTIVE refusal closed its original raw descriptor")
	}
}

// origin: hot-update S6 plan §4.1 / T03
func TestBrokerBootConfigBytesRejectsAmbiguousVectors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broker.yaml")
	if err := os.WriteFile(path, []byte("broker: {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"tether", "serve", "--config"}, {"tether", "serve", "--config=" + path, "--config=" + path}, {"tether", "serve", "--config=" + path + ".missing"}} {
		if _, err := BootConfigBytes(args); err == nil {
			t.Fatal("ambiguous/unreadable sealed config was accepted")
		}
	}
	for _, args := range [][]string{{"tether", "serve", "--config", path}, {"tether", "serve", "--config=" + path}} {
		if data, err := BootConfigBytes(args); err != nil || string(data) != "broker: {}\n" {
			t.Fatalf("sealed config bytes changed: %v", err)
		}
	}
	if err := os.WriteFile(path, make([]byte, MaxBundleBytes+1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := BootConfigBytes([]string{"tether", "serve", "--config", path}); err == nil {
		t.Fatal("oversized sealed config was accepted")
	}
}
