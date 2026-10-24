// Real same-PID Prepared.Exec with a parent-owned KCMP_FILE witness.
// The fixture restores one regular file; it does not certify a product rollout.
package fullstackharness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
	"golang.org/x/sys/unix"
)

const ofdHandoffMode = "_TETHER_TEST_OFD_HANDOFF_MODE"
const ofdHandoffRoot = "_TETHER_TEST_OFD_HANDOFF_ROOT"

type ofdHandoffReceipt struct {
	Stamp         contract.Stamp
	ResourceID    string
	FD            int
	Position      int64
	Flags         uint64
	Device, Inode uint64
}

// origin: S10 internal review rv-harness F6
func TestOFDWitnessFollowsPreparedExec(t *testing.T) {
	if mode := os.Getenv(ofdHandoffMode); mode != "" {
		runOFDHandoff(t, mode)
		return
	}
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseOFDPreparedExec(t)
	}
	testharness.AssertNoGoroutineLeak(t, "OFD Prepared.Exec lifecycle", g)
	testharness.AssertNoFDLeak(t, "OFD Prepared.Exec lifecycle", f, 0)
}

func exerciseOFDPreparedExec(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOFDWitnessFollowsPreparedExec$", "-test.count=1")
	cmd.Env = ofdHandoffEnvironment("before", t.TempDir())
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := input.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	}()
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := output.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	}()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	wait, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		_ = wait()
		if t.Failed() {
			t.Log(stderr.String())
		}
	}()
	decoder := json.NewDecoder(output)
	var before, after ofdHandoffReceipt
	if err := decoder.Decode(&before); err != nil {
		t.Fatal(err)
	}
	w, err := NewOFDWitness(before.Stamp, before.FD)
	if err != nil {
		t.Fatalf("SETUP: remote KCMP_FILE witness: %v", err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := w.Check(ctx, before.Stamp, before.FD); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Write([]byte{'E'}); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&after); err != nil {
		t.Fatal(err)
	}
	if before.Stamp.Process != after.Stamp.Process || after.Stamp.Generation != before.Stamp.Generation+1 ||
		after.Stamp.CutID == before.Stamp.CutID || before.ResourceID != after.ResourceID || before.FD == after.FD {
		t.Fatalf("handoff did not migrate the same resource in the same PID: before=%+v after=%+v", before, after)
	}
	if before.Device != after.Device || before.Inode != after.Inode || before.Position != after.Position || before.Flags != after.Flags || after.Flags&unix.O_CLOEXEC == 0 {
		t.Fatalf("handoff changed inode, offset or flags: before=%+v after=%+v", before, after)
	}
	proof, err := w.Check(ctx, after.Stamp, after.FD)
	if err != nil {
		t.Fatalf("real Prepared.Exec lost original OFD: %v", err)
	}
	if proof.OriginalFD != before.FD || proof.CurrentFD != after.FD || proof.Flags != after.Flags {
		t.Fatalf("OFD provenance lost after migration: %+v", proof)
	}
	if _, err := w.Check(ctx, after.Stamp, after.FD); err != nil {
		t.Fatalf("new binding not retained: %v", err)
	}
	if _, err := input.Write([]byte{'D'}); err != nil {
		t.Fatal(err)
	}
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	t.Logf("same-PID handoff pid=%d ticks=%d generation=%d->%d fd=%d->%d witness=%d", before.Stamp.Process.PID,
		before.Stamp.Process.StartTicks, before.Stamp.Generation, after.Stamp.Generation, before.FD, after.FD, proof.WitnessFD)
}

func runOFDHandoff(t *testing.T, mode string) {
	t.Helper()
	root := os.Getenv(ofdHandoffRoot)
	if mode == "before" {
		file, err := os.Create(filepath.Join(root, "original"))
		if err != nil {
			t.Fatal(err)
		}
		// origin: hot-update S10 lint L3a / errcheck
		defer func() {
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		}()
		if _, err := file.WriteString("original OFD data"); err != nil {
			t.Fatal(err)
		}
		before := readOFDHandoffReceipt(t, fixtureStamp(t), file)
		if err := json.NewEncoder(os.Stdout).Encode(before); err != nil {
			t.Fatal(err)
		}
		readOFDHandoffFence(t, 'E')
		data, err := json.Marshal(before)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := handoff.Prepare(handoff.State{Role: "agent", Instance: before.Stamp.Process.InstanceID,
			Generation: before.Stamp.Generation + 1, Data: data}, []handoff.Resource{{ID: before.ResourceID,
			Kind: "ofd-witness", Files: map[string]*os.File{"data": file}}})
		if err != nil {
			t.Fatal(err)
		}
		// origin: hot-update S10 lint L3a / errcheck
		defer func() {
			if err := prepared.Close(); err != nil {
				t.Error(err)
			}
		}()
		err = prepared.Exec(os.Args[0], []string{os.Args[0], "-test.run=^TestOFDWitnessFollowsPreparedExec$", "-test.count=1"}, ofdHandoffEnvironment("after", root))
		t.Fatalf("Prepared.Exec returned: %v", err)
	}
	if mode != "after" {
		t.Fatalf("unknown OFD handoff mode: %s", mode)
	}
	restored, err := handoff.Restore()
	if err != nil || restored == nil {
		t.Fatalf("restore: %v", err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := restored.Close(); err != nil {
			t.Error(err)
		}
	}()
	var before ofdHandoffReceipt
	if err := json.Unmarshal(restored.State.Data, &before); err != nil {
		t.Fatal(err)
	}
	file, err := restored.TakeFile(before.ResourceID, "data")
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	if again, err := handoff.Restore(); err != nil || again != nil {
		t.Fatal("handoff dispatch was not consumed")
	}
	next := before.Stamp
	next.Generation = restored.State.Generation
	next.CutID = "prepared-exec-cut"
	next.Sequence++
	next.AtBootNS++
	if err := checkKernelProcess(next.Process); err != nil {
		t.Fatal(err)
	}
	after := readOFDHandoffReceipt(t, next, file)
	if err := json.NewEncoder(os.Stdout).Encode(after); err != nil {
		t.Fatal(err)
	}
	readOFDHandoffFence(t, 'D')
}

func readOFDHandoffReceipt(t *testing.T, stamp contract.Stamp, file *os.File) ofdHandoffReceipt {
	t.Helper()
	fd := int(file.Fd()) // Regular-file fixture; no net.Conn flag conversion.
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		t.Fatal(err)
	}
	position, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	flags, err := ReadDescriptorFlags(os.Getpid(), fd)
	if err != nil {
		t.Fatal(err)
	}
	return ofdHandoffReceipt{Stamp: stamp, ResourceID: "original-resource", FD: fd, Position: position,
		Flags: flags, Device: stat.Dev, Inode: stat.Ino}
}

func readOFDHandoffFence(t *testing.T, expected byte) {
	t.Helper()
	var value [1]byte
	if _, err := io.ReadFull(os.Stdin, value[:]); err != nil || value[0] != expected {
		t.Fatalf("handoff fence %q: %q %v", expected, value, err)
	}
}

func ofdHandoffEnvironment(mode, root string) []string {
	var env []string
	for _, entry := range handoff.CleanEnvironment(os.Environ()) {
		if !strings.HasPrefix(entry, ofdHandoffMode+"=") && !strings.HasPrefix(entry, ofdHandoffRoot+"=") &&
			!strings.HasPrefix(entry, "_TETHER_TEST_PROCESS_SUPERVISOR=") && !strings.HasPrefix(entry, "GORACE=") {
			env = append(env, entry)
		}
	}
	return append(env, ofdHandoffMode+"="+mode, ofdHandoffRoot+"="+root, "GORACE=atexit_sleep_ms=0")
}
