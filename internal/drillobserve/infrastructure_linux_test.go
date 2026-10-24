//go:build linux

package drillobserve

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

func infrastructureHook(t *testing.T, hook func(Event)) {
	t.Helper()
	previous := Hook
	infrastructureSources.Lock()
	rows, broken := infrastructureSources.Rows, infrastructureSources.Broken
	infrastructureSources.Rows = make(map[[16]byte]func(int64) error)
	infrastructureSources.Broken = false
	infrastructureSources.Unlock()
	Hook = hook
	t.Cleanup(func() {
		Hook = previous
		infrastructureSources.Lock()
		infrastructureSources.Rows, infrastructureSources.Broken = rows, broken
		infrastructureSources.Unlock()
	})
}

// This uses a real seqpacket channel, SCM_RIGHTS and sealed memfds. The test
// installs custody in process; the existing startup-enabler controls separately
// prove that an ordinary caller cannot install this channel in a release.
// origin: hot-update S9 plan §2.2 / §2.3 / §9.1; s9-instr4 Requests 1–2
func TestInfrastructureCustodyChallengeSealsCompleteRoster(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("challenge", func(t *testing.T) { exerciseInfrastructureCustody(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "infrastructure custody", goroutines)
	testharness.AssertNoFDLeak(t, "infrastructure custody", fds, 0)
}

func exerciseInfrastructureCustody(t *testing.T) {
	_, socket, peer := custodyPair(t)
	custodian := &custody{channel: int(socket.Fd()), queue: make(chan pending, queueDepth), exitDone: make(chan struct{})}
	infrastructureHook(t, custodian.record)
	go custodian.writeLoop()
	defer func() {
		if err := unix.Shutdown(peer, unix.SHUT_RDWR); err != nil {
			t.Error(err)
		}
		select {
		case <-custodian.exitDone:
		case <-time.After(3 * time.Second):
			t.Error("custody reader outlived its endpoint")
		}
	}()
	RegisterInfrastructure(c.ID{1}, func(c.Deadline) error {
		return RecordInfrastructure(InfrastructureAttachmentKind, c.ID{1}, 0, []byte(`{"Participant":"original-a"}`))
	})
	RegisterInfrastructure(c.ID{2}, func(c.Deadline) error {
		return RecordInfrastructure(InfrastructureAttachmentKind, c.ID{2}, 0, []byte(`{"Participant":"original-b"}`))
	})
	var nonce [12]byte
	nonce[0] = 7
	request := append([]byte("TDI1"), nonce[:]...)
	request = binary.LittleEndian.AppendUint64(request, uint64(time.Second))
	if err := unix.Sendmsg(peer, request, nil, nil, unix.MSG_NOSIGNAL); err != nil {
		t.Fatal(err)
	}
	var sealed uint64
	for {
		poll := []unix.PollFd{{Fd: int32(peer), Events: unix.POLLIN}}
		if _, err := unix.Poll(poll, 3000); err != nil || poll[0].Revents&unix.POLLIN == 0 {
			t.Fatal("fresh infrastructure challenge was not acknowledged")
		}
		var body [64]byte
		oob := make([]byte, unix.CmsgSpace(4))
		n, oobn, flags, _, err := unix.Recvmsg(peer, body[:], oob, 0)
		if err != nil || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
			t.Fatal("incomplete custody packet")
		}
		if string(body[:n]) == "TDC1" {
			messages, err := unix.ParseSocketControlMessage(oob[:oobn])
			if err != nil || len(messages) != 1 {
				t.Fatal("missing sealed descriptor")
			}
			fds, err := unix.ParseUnixRights(&messages[0])
			if err != nil || len(fds) != 1 {
				t.Fatal("ambiguous sealed descriptor")
			}
			file := os.NewFile(uintptr(fds[0]), "infrastructure-receipt")
			data, readErr := io.ReadAll(io.NewSectionReader(file, 0, MaxInfrastructureReceipt))
			seals, sealErr := unix.FcntlInt(file.Fd(), unix.F_GET_SEALS, 0)
			closeErr := file.Close()
			if readErr != nil || sealErr != nil || closeErr != nil || seals&custodySeals != custodySeals || len(data) < 76 || string(data[:8]) != "TDRILL1\x00" {
				t.Fatal("receipt lacked immutable custody")
			}
			sealed = binary.LittleEndian.Uint64(data[56:64])
			if binary.LittleEndian.Uint64(data[64:72]) != 0 || binary.LittleEndian.Uint32(data[72:76]) != 2 || !bytes.Contains(data, []byte(`{"Participant":"original-a"}`)) || !bytes.Contains(data, []byte(`{"Participant":"original-b"}`)) {
				t.Fatal("challenge did not seal both original rows")
			}
			continue
		}
		if n != 32 || string(body[:4]) != "TDIA" || !bytes.Equal(body[4:16], nonce[:]) || sealed == 0 || binary.LittleEndian.Uint64(body[16:24]) != sealed || binary.LittleEndian.Uint64(body[24:32]) != 0 || oobn != 0 {
			t.Fatal("challenge reply preceded its complete sealed roster")
		}
		break
	}
}

// origin: hot-update S9 plan §2.2 / §2.3; s9-instr4 Requests 1–2
func TestInfrastructureObservationBoundsAndMissingSources(t *testing.T) {
	var events []Event
	infrastructureHook(t, func(e Event) { events = append(events, e) })
	d, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(SampleInfrastructure(d), ErrInfrastructureObservation) {
		t.Fatal("empty roster signed complete")
	}
	RegisterInfrastructure(c.ID{1}, func(c.Deadline) error { return errors.New("private-error-canary") })
	if err := SampleInfrastructure(d); err != ErrInfrastructureObservation {
		t.Fatal("missing source or private callback error escaped")
	}
	UnregisterInfrastructure(c.ID{1})
	for i := 0; i < maxInfrastructureSources; i++ {
		RegisterInfrastructure(c.ID{byte(i), 1}, func(c.Deadline) error { return nil })
	}
	RegisterInfrastructure(c.ID{1, 2}, func(c.Deadline) error { return nil })
	if !errors.Is(SampleInfrastructure(d), ErrInfrastructureObservation) {
		t.Fatal("source overflow became a partial successful roster")
	}
	data := bytes.Repeat([]byte("x"), MaxData+7)
	if err := RecordInfrastructure(InfrastructurePrefixKind, c.ID{1}, 1, data); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	if len(events) != 2 || events[0].Digest != digest || events[1].Digest != digest || events[0].Values[0] != 0 || events[1].Values[0] != MaxData || events[0].Values[1] != uint64(len(data)) || events[1].Values[1] != uint64(len(data)) || !bytes.Equal(append(events[0].Data, events[1].Data...), data) {
		t.Fatal("bounded export truncated or lost fragment custody")
	}
	if !validCustodyVocabulary(InfrastructurePrefixKind, "store") || !validCustodyVocabulary(InfrastructureAttachmentKind, "infra") || validCustodyVocabulary(InfrastructurePrefixKind, "infra") {
		t.Fatal("infrastructure custody vocabulary drift")
	}
	if err := RecordInfrastructure(InfrastructurePrefixKind, c.ID{1}, 1, make([]byte, MaxInfrastructureReceipt+1)); !errors.Is(err, ErrInfrastructureObservation) {
		t.Fatal("oversized receipt was accepted")
	}
}
