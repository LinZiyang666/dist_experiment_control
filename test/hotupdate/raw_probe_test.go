//go:build linux

package hotupdate_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

const rawProbeHeaderBytes = 61 // session16, direction1, sequence8, length4, digest32

type rawProbeRecord struct {
	Session   [16]byte
	Direction byte
	Sequence  uint64
	Length    uint32
	Digest    [32]byte
}

// The sender declares the entire finite trace before a fault. The receiver's
// expected ledger is never reconstructed from the receiver's observed bytes.
// origin: hot-update S5 plan §8 P19 / §9.1
func rawProbeTrace(session [16]byte, direction byte, count int) ([]rawProbeRecord, [][]byte) {
	var ledger []rawProbeRecord
	var wire [][]byte
	for seq := 0; seq < count; seq++ {
		payload := bytes.Repeat([]byte{byte(seq), direction, 0, 0xff}, 17+seq%7)
		r := rawProbeRecord{Session: session, Direction: direction, Sequence: uint64(seq),
			Length: uint32(len(payload)), Digest: sha256.Sum256(payload)}
		body := make([]byte, rawProbeHeaderBytes+len(payload))
		copy(body[:16], session[:])
		body[16] = direction
		binary.BigEndian.PutUint64(body[17:25], r.Sequence)
		binary.BigEndian.PutUint32(body[25:29], r.Length)
		copy(body[29:61], r.Digest[:])
		copy(body[61:], payload)
		ledger, wire = append(ledger, r), append(wire, body)
	}
	return ledger, wire
}

type rawProbeReceiver struct {
	expected          []rawProbeRecord
	identity          hotUpdateSocketIdentity
	pending           []byte
	next              int
	sticky            error
	eof               bool
	started, progress time.Time
	maxGap            time.Duration
}

func newRawProbeReceiver(ledger []rawProbeRecord, identity hotUpdateSocketIdentity, now time.Time) *rawProbeReceiver {
	return &rawProbeReceiver{expected: append([]rawProbeRecord(nil), ledger...), identity: identity,
		started: now, progress: now}
}

func (r *rawProbeReceiver) fail(reason string) error {
	if r.sticky == nil {
		r.sticky = errors.New("raw probe: " + reason)
	}
	return r.sticky
}

// Feed consumes arbitrary TCP segmentation, including n+EOF. A mismatch is
// sticky: later correct bytes cannot heal a drop, redial, reset or early EOF.
func (r *rawProbeReceiver) Feed(p []byte, readErr error, identity hotUpdateSocketIdentity, now time.Time) error {
	if r.sticky != nil {
		return r.sticky
	}
	if identity != r.identity {
		return r.fail("original socket identity changed")
	}
	if r.eof && (len(p) != 0 || readErr != io.EOF) {
		return r.fail("data after EOF")
	}
	r.pending = append(r.pending, p...)
	for len(r.pending) >= rawProbeHeaderBytes {
		length := binary.BigEndian.Uint32(r.pending[25:29])
		if length == 0 || length > 32768 {
			return r.fail("invalid record length")
		}
		total := rawProbeHeaderBytes + int(length)
		if len(r.pending) < total {
			break
		}
		if r.next >= len(r.expected) {
			return r.fail("undeclared trailing record")
		}
		var observed rawProbeRecord
		copy(observed.Session[:], r.pending[:16])
		observed.Direction = r.pending[16]
		observed.Sequence = binary.BigEndian.Uint64(r.pending[17:25])
		observed.Length = length
		copy(observed.Digest[:], r.pending[29:61])
		// Checking only the embedded digest lets a corrupt sender sign its own
		// output. Both header and actual payload must match the sender ledger.
		if observed != r.expected[r.next] || sha256.Sum256(r.pending[61:total]) != r.expected[r.next].Digest {
			return r.fail("independent sender ledger mismatch")
		}
		if gap := now.Sub(r.progress); gap > r.maxGap {
			r.maxGap = gap
		}
		r.progress = now
		r.next++
		r.pending = r.pending[total:]
	}
	if readErr != nil {
		if readErr != io.EOF {
			return r.fail("business read aborted")
		}
		r.eof = true
		if r.next != len(r.expected) || len(r.pending) != 0 {
			return r.fail("EOF before declared final byte")
		}
	}
	return nil
}

func (r *rawProbeReceiver) Finish(now time.Time, maxPause time.Duration, requireEOF bool) error {
	if r.sticky != nil {
		return r.sticky
	}
	if len(r.expected) == 0 || r.next != len(r.expected) || len(r.pending) != 0 {
		return r.fail("incomplete finite trace")
	}
	if requireEOF && !r.eof {
		return r.fail("missing final EOF")
	}
	// Include silence after the last progress; a final packet does not erase
	// a stall continuing to the declared observation endpoint.
	if trailing := now.Sub(r.progress); trailing > r.maxGap {
		r.maxGap = trailing
	}
	if r.maxGap > maxPause {
		return r.fail("maximum business pause exceeded")
	}
	return nil
}

func readRawProbe(c *net.TCPConn, ledger []rawProbeRecord) error {
	identity, err := hotUpdateTCPIdentity(c)
	if err != nil {
		return err
	}
	r := newRawProbeReceiver(ledger, identity, time.Now())
	buf := make([]byte, 47) // splits both headers and payloads over real reads
	for {
		n, readErr := c.Read(buf)
		current, err := hotUpdateTCPIdentity(c)
		if err != nil {
			return err
		}
		if err := r.Feed(buf[:n], readErr, current, time.Now()); err != nil {
			return err
		}
		if readErr != nil {
			return r.Finish(time.Now(), 5*time.Second, true)
		}
	}
}

func writeRawProbe(c *net.TCPConn, wire [][]byte) error {
	for _, record := range wire {
		for len(record) > 0 {
			end := min(len(record), 31)
			n, err := c.Write(record[:end])
			record = record[n:]
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrNoProgress
			}
		}
	}
	return c.CloseWrite()
}

// origin: hot-update S5 plan §8 P19 / §9.1
// origin: S7 internal review TRANSFER-MAKETEST F3
func TestRawProbeDetectsDroppedAndDuplicatedBytes(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRawProbe(t)
	}
	testharness.AssertNoGoroutineLeak(t, "independent duplex raw probe", beforeG)
	testharness.AssertNoFDLeak(t, "independent duplex raw probe", beforeFD, 0)

	// The shared artifact fixture must execute the exact built image. Two real
	// builds label A/B, as required by the later rollback receipts.
	a := buildHotUpdateArtifact(t, "v0.0.0-s5-fixture-a")
	b := buildHotUpdateArtifact(t, "v0.0.0-s5-fixture-b")
	if a.SHA256 == b.SHA256 || a.TarSHA256 == b.TarSHA256 || a.SourceDigest != b.SourceDigest {
		t.Fatal("artifact A/B identity or shared source receipt is invalid")
	}
	for _, artifact := range []hotUpdateArtifact{a, b} {
		p := startHotUpdateProcess(t, artifact.Path, hotUpdateEnvironment(t), "version")
		select {
		case <-p.Done:
		case <-time.After(10 * time.Second):
			t.Fatal("real version process did not exit")
		}
		// S6 appends its reader domain without removing any of the S5 readers.
		// origin: hot-update S6 plan §9.1 / T46
		// S7 appends transfer-v1 the same way, still before the unchanged final
		// ABI line; the literal stays independent of proto so dropping or
		// reordering any earlier reader domain is still caught here.
		// origin: hot-update S7 plan §6.2 / P01
		// S7 §9 requires the actual capable reader; S8 §6.1 appends its
		// domain, epoch and feature lines while retaining every earlier domain
		// and the final ABI line. Pin that full suffix independently of proto.
		if p.err != nil || !bytes.HasSuffix(p.Output.Bytes(), []byte("image-domains exec-v1 pty-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1 upgrade-cut-v1\nimage-read-epochs 1\nimage-features upgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1\nimage-abi v1\n")) {
			t.Fatalf("private real artifact failed its domain/ABI probe: err=%v output=%q", p.err, p.Output.Bytes())
		}
		info, err := debugBuildArtifact(artifact.Path)
		if err != nil || info != hotUpdateRaceEnabled() {
			t.Fatal("nested artifact race instrumentation differs from harness")
		}
		t.Logf("artifact source=%s source_digest=%s binary_sha=%s tar_sha=%s race=%t",
			artifact.SourceCommit, artifact.SourceDigest, artifact.SHA256, artifact.TarSHA256, artifact.Race)
	}
}

func exerciseRawProbe(t *testing.T) {
	t.Helper()
	var session [16]byte
	if _, err := rand.Read(session[:]); err != nil {
		t.Fatal(err)
	}
	forward, fw := rawProbeTrace(session, 0, 24)
	reverse, rw := rawProbeTrace(session, 1, 24)
	cases := []struct {
		name string
		wire [][]byte
		bad  bool
	}{
		{"complete-duplex", fw, false},
		{"dropped-record", append(append([][]byte(nil), fw[:7]...), fw[8:]...), true},
		{"duplicated-record", append(append([][]byte(nil), fw[:8]...), fw[7:]...), true},
		{"missing-tail", fw[:len(fw)-1], true},
		{"trailing-record", append(append([][]byte(nil), fw...), fw[0]), true},
	}
	corrupt := bytes.Join(fw, nil)
	corrupt[rawProbeHeaderBytes+3] ^= 1
	cases = append(cases, struct {
		name string
		wire [][]byte
		bad  bool
	}{"corrupt-byte", [][]byte{corrupt}, true})
	original := bytes.Join(fw, nil)
	droppedByte := append(append([]byte(nil), original[:rawProbeHeaderBytes+2]...), original[rawProbeHeaderBytes+3:]...)
	duplicatedByte := append(append([]byte(nil), original[:rawProbeHeaderBytes+3]...), original[rawProbeHeaderBytes+2:]...)
	for _, tc := range []struct {
		name string
		body []byte
	}{{"dropped-byte", droppedByte}, {"duplicated-byte", duplicatedByte}} {
		cases = append(cases, struct {
			name string
			wire [][]byte
			bad  bool
		}{tc.name, [][]byte{tc.body}, true})
	}
	reordered := append([][]byte(nil), fw...)
	reordered[4], reordered[5] = reordered[5], reordered[4]
	cases = append(cases, struct {
		name string
		wire [][]byte
		bad  bool
	}{"reordered-record", reordered, true})
	for _, tc := range cases {
		client, server := hotUpdateTCPPair(t)
		// Exactly one Accept above; neither direction may replace this pair.
		serverResult := make(chan error, 1)
		go func() {
			writeErr := writeRawProbe(server, rw)
			readErr := readRawProbe(server, forward)
			serverResult <- errors.Join(writeErr, readErr)
		}()
		writeErr := writeRawProbe(client, tc.wire)
		reverseErr := readRawProbe(client, reverse)
		forwardErr := <-serverResult
		_ = client.Close()
		_ = server.Close()
		if writeErr != nil || reverseErr != nil {
			t.Fatalf("%s: duplex setup failed", tc.name)
		}
		if (forwardErr != nil) != tc.bad {
			t.Fatalf("%s: independent ledger verdict error=%v, want rejected=%t", tc.name, forwardErr, tc.bad)
		}
	}
	identity := hotUpdateSocketIdentity{Dev: 1, Ino: 2, Type: 3}
	now := time.Now()
	for _, fault := range []string{"changed-socket", "changed-session", "reset", "early-eof", "partial-tail", "missing-eof", "trailing-silence", "initial-silence"} {
		r := newRawProbeReceiver(forward, identity, now)
		body := bytes.Join(fw, nil)
		observed := identity
		readErr := error(io.EOF)
		when := now.Add(time.Millisecond)
		switch fault {
		case "changed-socket":
			observed.Ino++
		case "changed-session":
			body[0] ^= 1
		case "reset":
			readErr = errors.New("injected reset")
		case "early-eof":
			body = nil
		case "partial-tail":
			body = body[:len(body)-1]
		case "missing-eof":
			readErr = nil
		case "initial-silence":
			when = now.Add(6 * time.Second)
		}
		err := r.Feed(body, readErr, observed, when)
		finish := when.Add(time.Millisecond)
		if fault == "trailing-silence" {
			finish = when.Add(6 * time.Second)
		}
		if err == nil {
			err = r.Finish(finish, 5*time.Second, true)
		}
		if err == nil {
			t.Fatalf("%s: oracle accepted faulty trace", fault)
		}
		if followup := r.Feed(bytes.Join(fw, nil), io.EOF, identity, finish); followup == nil {
			t.Fatalf("%s: rejected trace lost sticky failure", fault)
		}
	}
	// n+EOF is a legal final read, and the declared ledger is immutable.
	r := newRawProbeReceiver(forward, identity, now)
	forward[0].Sequence++
	if err := r.Feed(bytes.Join(fw, nil), io.EOF, identity, now); err != nil {
		t.Fatal(fmt.Errorf("valid final n+EOF: %w", err))
	}
	if err := r.Finish(now, 5*time.Second, true); err != nil {
		t.Fatal(err)
	}
	// Retain the actual syscall n and resend exactly its unwritten suffix on
	// the original OFD, as P20/P21 do after a cut. The peer has not read yet.
	client, server := hotUpdateTCPPair(t)
	identityBefore, err := hotUpdateTCPIdentity(client)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("blocked-original-business-socket\x00"), 32768)
	n, _ := hotUpdateBlockedWrite(t, client, body)
	received := make(chan []byte, 1)
	go func() { got, _ := io.ReadAll(server); received <- got }()
	if _, err := io.Copy(client, bytes.NewReader(body[n:])); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got := <-received
	identityAfter, err := hotUpdateTCPIdentity(client)
	_ = client.Close()
	_ = server.Close()
	if err != nil || identityAfter != identityBefore || !bytes.Equal(got, body) {
		t.Fatal("real partial write suffix lost bytes, duplicated bytes or replaced original socket")
	}
}
