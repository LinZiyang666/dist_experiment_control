//go:build linux

package drillobserve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// The standalone test must exercise the startup-only contract in a real child,
// rather than installing a hook after init or treating missing evidence as green.
// origin: S10 internal review POLISH-A F2 / hot-update S10 plan §5.3
func exerciseCustodyRecording(t *testing.T) {
	t.Helper()
	config, socket, receiver := nativeExitPair(t, true)
	defer func() { _ = config.Close(); _ = socket.Close(); _ = unix.Close(receiver) }()
	var content [32]byte
	if _, err := config.ReadAt(content[:], 0); err != nil {
		t.Fatal(err)
	}
	reader := TransferCustodyReader{Identity: sha256.Sum256(content[:])}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCustodyRecordedStream$", "-test.count=1")
	cmd.Env = append(StripEnvironment(os.Environ()), EnableEnv+"=v1", CustodyEnv+"=3", "TETHER_TEST_OBSERVER_RECORDING=unit")
	cmd.ExtraFiles = []*os.File{config, socket}
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = input.Close() }()
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	join, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = join() }()
	if err := unix.SetsockoptTimeval(receiver, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &unix.Timeval{Sec: 3}); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	read := func() bool {
		t.Helper()
		packet, control := make([]byte, 4), make([]byte, unix.CmsgSpace(4))
		n, cn, flags, _, err := unix.Recvmsg(receiver, packet, control, unix.MSG_CMSG_CLOEXEC)
		for err == unix.EINTR {
			n, cn, flags, _, err = unix.Recvmsg(receiver, packet, control, unix.MSG_CMSG_CLOEXEC)
		}
		if err != nil || n != 4 || flags&^(unix.MSG_CMSG_CLOEXEC) != 0 {
			t.Fatalf("recorded custody receive: n=%d flags=%d err=%v", n, flags, err)
		}
		if string(packet) == "TDOK" && cn == 0 {
			return true
		}
		if string(packet) != "TDC1" {
			t.Fatalf("recorded custody packet is not sealed data: %q", packet)
		}
		messages, err := unix.ParseSocketControlMessage(control[:cn])
		if err != nil || len(messages) != 1 {
			t.Fatal("recorded custody rights missing")
		}
		rights, err := unix.ParseUnixRights(&messages[0])
		if err != nil || len(rights) != 1 {
			t.Fatal("recorded custody requires one descriptor")
		}
		fd := rights[0]
		defer func() { _ = unix.Close(fd) }()
		seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
		if err != nil || seals&custodySeals != custodySeals {
			t.Fatal("recorded custody is mutable")
		}
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil || stat.Size < 76 || stat.Size > 1<<20 {
			t.Fatal("recorded custody has invalid size")
		}
		raw := make([]byte, stat.Size)
		if n, err := unix.Pread(fd, raw, 0); err != nil || n != len(raw) {
			t.Fatal("recorded custody read incomplete")
		}
		events, err := reader.Accept(raw)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			counts[event.Kind+"/"+event.Domain]++
		}
		return false
	}
	for counts["carrier-prefix/tunnel"] == 0 {
		if read() {
			t.Fatal("recorded custody acknowledged before a drain")
		}
	}
	if err := unix.Sendmsg(receiver, []byte("TDP1"), nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	// Each read blocks on the next custody record; stop at the drain acknowledgement.
	for drained := false; !drained; {
		drained = read()
	}
	for _, key := range []string{"ss-seal/ss", "ss-write/ss", "flow-state/flow", "resume/tunnel", "domain-cut/exec", "domain-cut/pty", "domain-cut/flow", "domain-cut/ss", "domain-live/exec", "domain-live/pty", "ss-state/ss", "flow-retirement/flow", "reaper/proxy", "ss-grant/ss", "carrier-negotiated/tunnel", "carrier-prefix/tunnel"} {
		if counts[key] != 1 {
			t.Fatalf("recorded custody %s count=%d, want 1", key, counts[key])
		}
	}
	if len(counts) != 16 || reader.Sequence() < 2 {
		t.Fatal("recorded custody has extra events or no fresh sealed drain")
	}
	if _, err := input.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := join(); err != nil {
		t.Fatalf("recording child failed: %v: %s", err, output.String())
	}
	// An explicitly requested recording must still fail when its live startup
	// evidence is missing. Only the ordinary package entry supplies a fixture.
	missing := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCustodyRecordedStream$", "-test.count=1")
	missing.Env = append(StripEnvironment(os.Environ()), "TETHER_TEST_OBSERVER_RECORDING=unit")
	out, err := testharness.CombinedOutput(missing)
	if err == nil || !bytes.Contains(out, []byte("SETUP-RED: NOT_RUN pending S10 P06: recording requires inherited custody")) {
		t.Fatalf("missing recording evidence did not fail SETUP-RED: %v: %s", err, out)
	}
}
