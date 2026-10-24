//go:build linux

package ptysession

import (
	"bytes"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Real: bounded memfd replay and exact retained overlap. Controlled: no kernel
// borrowers, so capacity can be examined without scheduling a slave reader.
// origin: hot-update S4 plan §3.2 / T11
func TestPtyInputCreditAndOverlap(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		t.Run("credit", func(t *testing.T) { exerciseInputCredit(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "input credit", g0)
	testharness.AssertNoFDLeak(t, "input credit", fd0, 0)
}

func exerciseInputCredit(t *testing.T) {
	now := int64(1)
	a := tableActor(t, &now)
	payload := inputPayload(upgradebudget.PTYInputBytes)
	enqueueActor(t, a, payload, false)
	s := a.InputStatus()
	if s.Applied != 0 || s.Accepted != uint64(len(payload)) || s.Window != 0 {
		t.Fatalf("credit confused with applied ACK: %+v", s)
	}
	if got, err := a.AcceptInput(s.Accepted, []byte("x"), true); !errors.Is(err, workload.ErrBackpressure) || got != s {
		t.Fatalf("full request mutated FIN/credit: %+v %v", got, err)
	}
	for _, tc := range []struct {
		seq  uint64
		data []byte
	}{
		{uint64(len(payload) + 1), nil}, {math.MaxUint64, []byte("xx")}, {0, []byte("wrong")}, {uint64(len(payload) - 2), []byte("bad-overlap")},
	} {
		if _, err := a.AcceptInput(tc.seq, tc.data, false); !errors.Is(err, ErrConflict) {
			t.Fatalf("accepted conflict %+v: %v", tc, err)
		}
	}
	if _, err := a.AcceptInput(0, payload[:100], false); err != nil {
		t.Fatal(err)
	}
	// Applied prefixes are unavailable; comparing them would read reused bytes.
	if err := a.cfg.Input.Ack(100); err != nil {
		t.Fatal(err)
	}
	before := a.InputStatus()
	if got, err := a.AcceptInput(before.Accepted, bytes.Repeat([]byte{'x'}, 101), true); !errors.Is(err, workload.ErrBackpressure) || got != before {
		t.Fatalf("partial-capacity request changed Q or FIN: %+v %v", got, err)
	}

	retry := append(bytes.Repeat([]byte{0xff}, 100), payload[100:200]...)
	if _, err := a.AcceptInput(0, retry, false); err != nil {
		t.Fatalf("compared an applied prefix: %v", err)
	}
	if _, err := a.AcceptInput(uint64(len(payload)-5), append(append([]byte(nil), payload[len(payload)-5:]...), []byte("suffix")...), true); err != nil {
		t.Fatal(err)
	}
	s = a.InputStatus()
	if !s.FIN || s.Ended || s.Accepted != uint64(len(payload)+6) || s.Applied != 100 {
		t.Fatalf("FIN endpoint: %+v", s)
	}
	if _, err := a.AcceptInput(s.Accepted, []byte("new"), false); !errors.Is(err, ErrConflict) {
		t.Fatal("extended FIN")
	}
	if _, err := a.AcceptInput(s.Accepted-1, nil, true); !errors.Is(err, ErrConflict) {
		t.Fatal("moved FIN")
	}
	// A retryable control does not wait for A to catch Q, even with input full.
	r := ControlRequest{Seq: 1, Op: "resize", Cols: 80, Rows: 24}
	digest, _ := controlDigest(r)
	a.mu.Lock()
	receipt := a.commitControlLocked(r, digest, nil)
	a.mu.Unlock()
	if got, err := a.ApplyControl(r); err != nil || got != receipt {
		t.Fatalf("input blocked control: %+v %v", got, err)
	}
}

// Real: canonical, raw and custom VEOF line disciplines. A logical FIN remains
// queued across same-PID exec; each original slave proves output and no HUP.
// origin: hot-update S4 plan §3.2 / T05
func TestPtyLogicalFINIsNotTerminalEOF(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if mode := os.Getenv(actorImageMode); mode != "" {
		runActorImage(t, mode, "fin")
		return
	}
	requirePTYImplementation(t)
	t.Run("atomic-FIN-credit", exerciseInputCredit)
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		for _, discipline := range []string{"canonical", "raw", "custom"} {
			t.Run(discipline, func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				runActorExecParent(t, "fin-"+discipline)
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "logical terminal FIN", g0)
	testharness.AssertNoFDLeak(t, "logical terminal FIN", fd0, 0)
}

// finChild verifies the actual byte VEOF separately from logical FIN. It first
// leaves the writer blocked in raw mode, then tests the requested discipline.
func finChild(discipline string) string {
	setup := "tty.setraw(0)\n"
	if discipline != "raw" {
		setup = "attrs=termios.tcgetattr(0)\nattrs[3] |= termios.ICANON\nattrs[3] &= ~termios.ECHO\nattrs[6][termios.VEOF] = b'\\x04'\n"
		if discipline == "custom" {
			setup += "attrs[6][termios.VEOF] = b'\\x05'\n"
		}
		setup += "termios.tcsetattr(0,termios.TCSANOW,attrs)\n"
	}
	return `signal.signal(signal.SIGHUP,lambda *_: mark('hup'))
tty.setraw(0)
mark('ready')
wait('drain')
data=b''
while len(data)<65536: data+=os.read(0,65536-len(data))
mark('received',data)
os.write(1,b'AFTER_FIN')
wait('veof')
` + setup + `mark('discipline-ready')
data=os.read(0,100)
mark('veof-result',data)
wait('exit')
`
}

func requireFileBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("file %s got=%x want=%x err=%v", filepath.Base(path), got, want, err)
	}
}
