//go:build darwin

package cli_e2e_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/execclient"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

func init() {
	// realstack_race_test.go is Linux-only. Darwin's actual race setting still
	// has to reach the inner product build, independently of cross-compilation.
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "-race" && setting.Value == "true" {
				raceEnabled = true
			}
		}
	}
}
func agentProcessAttributes() *syscall.SysProcAttr { return &syscall.SysProcAttr{Setpgid: true} }
func identityStillAlive(id ptyIdentity) bool {
	out, err := testharness.Output(exec.Command("ps", "-o", "lstart=", "-p", strconv.Itoa(id.PID)))
	return err == nil && id.Ticks != "" && strings.TrimSpace(string(out)) == id.Ticks
}

// origin: hot-update S4 plan §8.3 T44 / L6. This is an actual Darwin test:
// neither a cross-build nor Linux execution can provide this receipt.
func TestLegacyRunDarwinTerminalLifecycle(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	buildTether(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	const rounds = 5
	for round := 0; round < rounds; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			s := newRealStack(t, false)
			nc := connectPTYPeer(t, s, s.url)
			client, err := execclient.ProbePTY(t.Context(), nc, proto.SubjCmdBy(s.sid, s.ctlID.PublicKey, s.nid, proto.ProcessResumeVerb))
			if client != nil {
				_ = client.Close()
			}
			if !errors.Is(err, execclient.ErrUnsupported) {
				t.Fatalf("Darwin advertised reliable PTY: client=%v err=%v", client, err)
			}
			f := newPTYFixture(t)
			ctl := startTerminalCtl(t, s, s.bin, s.url, f.argv("tail")...)
			f.identity(t)
			ctl.expect(t, []byte("READY\n"))
			payload := ptyPayload(round)
			ctl.frame(t, payload)
			want := append([]byte("READY\n"), ptyEcho(payload)...)
			ctl.expect(t, want)
			ctl.resize(t, 137, 53)
			if string(f.receipt(t, "size")) != "53 137" {
				t.Fatal("Darwin resize returned success without changing the terminal")
			}
			ctl.frame(t, []byte("EXIT"))
			_ = f.receipt(t, "leader-exited")
			_ = f.receipt(t, "tail-child")
			var tailIdentity ptyIdentity
			if err := json.Unmarshal(f.receipt(t, "tail-child-identity"), &tailIdentity); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if identityStillAlive(tailIdentity) {
					_ = syscall.Kill(tailIdentity.PID, syscall.SIGKILL)
				}
			})
			// sleep-fixture: exceed the historical 200 ms truncation timer before
			// releasing a descendant that still owns the original slave.
			time.Sleep(300 * time.Millisecond)
			ctl.alive(t)
			f.gate(t, "late-tail")
			want = append(want, []byte("TAIL-OUT\nTAIL-ERR\n")...)
			ctl.wait(t, 23)
			ctl.expect(t, want)
			f.once(t)
			if !bytes.Equal(ctl.out.bytes(), want) {
				t.Fatal("Darwin legacy tail was truncated")
			}
			requirePTYReliable(t, s, 0)
			cancelFixture := newPTYFixture(t)
			cancelCtl := startTerminalCtl(t, s, s.bin, s.url, cancelFixture.argv("duplex")...)
			cancelFixture.identity(t)
			cancelCtl.expect(t, []byte("READY\n"))
			if err := cancelCtl.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			select {
			case <-cancelCtl.done:
			case <-time.After(10 * time.Second):
				t.Fatal("Darwin legacy Close failed to wake terminal I/O")
			}
			cancelCtl.restored(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "Darwin legacy terminal lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "Darwin legacy terminal lifecycle", beforeFD, 0)
}
