//go:build linux

package drillobserve

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"golang.org/x/sys/unix"
)

// origin: hot-update S7 plan §11.3 / T41 / T47
func TestTransferLifecycleNoLeaks(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferCustody(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer custody", goroutines)
	testharness.AssertNoFDLeak(t, "transfer custody", fds, 0)
}

func exerciseTransferCustody(t *testing.T) {
	t.Helper()
	for _, ref := range []xfercontract.ResourceRef{
		{ResourceID: "transfer.1", Name: "source", Kind: "xfer-source-v1"}, {ResourceID: "transfer.1", Name: "tmp", Kind: "xfer-tmp-v1"}, {ResourceID: "transfer.1", Name: "parent", Kind: "xfer-parent-v1"},
	} {
		func() {
			file, err := os.CreateTemp(t.TempDir(), "original")
			if ref.Name == "parent" {
				if err == nil {
					_ = file.Close()
				}
				file, err = os.Open(t.TempDir())
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = file.Close() }()
			w, err := CaptureTransferOFD(file, ref)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = w.Close() }()
			pair := transferSocketPair(t)
			defer func() { _ = unix.Close(pair[0]); _ = unix.Close(pair[1]) }()
			if w.Send(-1) != ErrTransferCustody {
				t.Fatal("invalid transport fabricated custody")
			}
			process, err := TransferProcessIdentity(1, "cut.1")
			if err != nil || w.Compare(os.Getpid(), process.ProcessStart, int(file.Fd())) != nil {
				t.Fatal("refused send destroyed old owner")
			}
			oldActive := active
			active = &custody{channel: pair[0]}
			publishErr := PublishTransferOFD(w)
			active = oldActive
			if publishErr != nil {
				t.Fatal("custody did not recover after refused send")
			}
			keeper, err := ReceiveTransferOFD(pair[1])
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = keeper.Close() }()
			if keeper.Compare(os.Getpid(), process.ProcessStart, int(file.Fd())) != nil {
				t.Fatal("keeper lost original OFD")
			}
			witnessFD := w.file.Fd()
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := unix.FcntlInt(witnessFD, unix.F_GETFD, 0); err != unix.EBADF {
				t.Fatal("retired witness descriptor still open")
			}
			if keeper.Compare(os.Getpid(), process.ProcessStart, int(file.Fd())) != nil {
				t.Fatal("retiring exporter closed current keeper")
			}
		}()
	}

	// Terminal-pending observations remain visible through a fresh drain.
	// Closing the keeper poisons and joins the writer instead of hiding a tail.
	o := transferSample(t)
	o.Kind, o.Phase = xfercontract.ObserveAcknowledge, xfercontract.PhaseTerminalPending
	e, err := EncodeTransfer(o)
	if err != nil {
		t.Fatal(err)
	}
	testTransferFreshDrain(t, e)
	oldHook, oldActive := Hook, active
	defer func() { Hook, active = oldHook, oldActive }()
	c := &custody{queue: make(chan pending, 1), channel: -1}
	Hook, active = c.record, c
	observer := NewTransferObserver[xfercontract.Observation](TransferProcess{ProcessStart: o.ProcessStart, BootID: o.BootID, PID: o.PID, Generation: o.Generation, CutID: o.CutID})
	observer(o)
	queued := <-c.queue
	decoded, err := DecodeTransfer(*queued.event)
	if err != nil || decoded.Phase != string(xfercontract.PhaseTerminalPending) {
		t.Fatal("terminal pending disappeared")
	}
	// The queued copy must outlive the producer's reusable data.
	o.Phase = xfercontract.PhaseHistoryConfirmed
	observer(o)
	if decoded.Phase != string(xfercontract.PhaseTerminalPending) {
		t.Fatal("producer mutated retained observation")
	}
	observer(o)
	if !c.broken.Load() || c.failure.Load() != 2 {
		t.Fatal("full queue did not poison custody")
	}
	poisonCustody(c, 3)
	if c.failure.Load() != 2 {
		t.Fatal("later failure replaced original overflow")
	}
	c = &custody{queue: make(chan pending, 1), channel: -1}
	Hook, active = c.record, c
	observer = NewTransferObserver[xfercontract.Observation](TransferProcess{ProcessStart: o.ProcessStart, BootID: o.BootID, PID: o.PID, Generation: o.Generation, CutID: o.CutID})
	bad := o
	bad.Dropped = 1
	observer(bad)
	if !c.broken.Load() || c.failure.Load() != 13 {
		t.Fatal("typed poison left channel clean")
	}
	c.send(nil)
	if c.failure.Load() != 13 {
		t.Fatal("send failure replaced typed poison")
	}
	file, err := os.CreateTemp(t.TempDir(), "publish-failure")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	w, err := CaptureTransferOFD(file, xfercontract.ResourceRef{ResourceID: "transfer.1", Name: "source", Kind: "xfer-source-v1"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	active = nil
	if PublishTransferOFD(w) != ErrTransferCustody {
		t.Fatal("missing startup channel fabricated publication")
	}
	c = &custody{channel: -1}
	active = c
	if PublishTransferOFD(w) != ErrTransferCustody || !c.broken.Load() || c.failure.Load() != 14 {
		t.Fatal("failed witness publication did not poison startup custody")
	}
	if w.Compare(os.Getpid(), w.Receipt().ProcessStart, int(file.Fd())) != nil {
		t.Fatal("failed publication mutated original OFD")
	}

	for _, defect := range []string{"unsealed", "wrong-version", "missing-resource", "wrong-type", "extra-rights", "truncated-rights", "partial-closure"} {
		exerciseTransferRejectedCustody(t, defect)
	}
}

func exerciseTransferRejectedCustody(t *testing.T, defect string) {
	t.Helper()
	before := testharness.FDCount()
	func() {
		pair := transferSocketPair(t)
		defer func() { _ = unix.Close(pair[0]); _ = unix.Close(pair[1]) }()
		metadataFD, err := unix.MemfdCreate("transfer-rejected-custody", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = unix.Close(metadataFD) }()
		file, err := os.CreateTemp(t.TempDir(), "source")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = file.Close() }()
		process, err := TransferProcessIdentity(1, "cut.1")
		if err != nil {
			t.Fatal(err)
		}
		ref := xfercontract.ResourceRef{ResourceID: "transfer.1", Name: "source", Kind: "xfer-source-v1"}
		r := TransferOFDReceipt{Schema: TransferSchemaVersion, ProcessStart: process.ProcessStart, BootID: process.BootID, PID: process.PID, Resource: transferResource{ResourceID: ref.ResourceID, Name: ref.Name, Kind: ref.Kind}, WitnessDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", OwnedFDs: 1, PeakFDs: 2}
		data, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(data, &fields) != nil {
			t.Fatal("receipt fixture")
		}
		resource, err := transferObject(reflect.ValueOf(transferResource{ResourceID: ref.ResourceID, Name: ref.Name, Kind: ref.Kind}))
		if err != nil {
			t.Fatal(err)
		}
		fields["resource"], _ = json.Marshal(resource)
		switch defect {
		case "wrong-version":
			fields["schema"] = json.RawMessage("2")
		case "missing-resource":
			delete(resource, "name")
			fields["resource"], _ = json.Marshal(resource)
		}
		data, err = json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := unix.Write(metadataFD, data); err != nil {
			t.Fatal(err)
		}
		if defect != "unsealed" {
			if _, err := unix.FcntlInt(uintptr(metadataFD), unix.F_ADD_SEALS, custodySeals); err != nil {
				t.Fatal(err)
			}
		}
		fds := []int{metadataFD, int(file.Fd())}
		switch defect {
		case "wrong-type":
			fds[1] = pair[1]
		case "extra-rights":
			fds = append(fds, int(file.Fd()))
		case "truncated-rights":
			for len(fds) < 32 {
				fds = append(fds, int(file.Fd()))
			}
		case "partial-closure":
			fds = fds[:1]
		}
		if unix.Sendmsg(pair[0], []byte("TDO1"), unix.UnixRights(fds...), nil, unix.MSG_NOSIGNAL) != nil {
			t.Fatal("negative custody send")
		}
		keeper, err := ReceiveTransferOFD(pair[1])
		if keeper != nil {
			_ = keeper.Close()
		}
		if !errors.Is(err, ErrTransferCustody) || keeper != nil {
			t.Fatalf("%s custody returned usable witness", defect)
		}
	}()
	if after := testharness.FDCount(); after != before {
		t.Fatalf("%s rejection leaked descriptors: %d -> %d", defect, before, after)
	}
}

// origin: hot-update S7 plan §10.4 / T41
func TestTransferCustodyCloseAndTruncationPoison(t *testing.T) {
	for _, scenario := range []string{"close", "truncated", "drain-poison", "legacy-drain"} {
		t.Run(scenario, func(t *testing.T) {
			pair := transferSocketPair(t)
			defer func() { _ = unix.Close(pair[0]); _ = unix.Close(pair[1]) }()
			c := &custody{channel: pair[0], identity: sha256.Sum256([]byte("fixture")), incarnation: [16]byte{1}, queue: make(chan pending, queueDepth)}
			joined := make(chan struct{})
			go func() { defer close(joined); c.writeLoop() }()
			defer func() {
				_ = unix.Shutdown(pair[1], unix.SHUT_RDWR)
				select {
				case <-joined:
				case <-time.After(time.Second):
					t.Error("custody writer did not join")
				}
			}()
			if scenario == "close" {
				_ = unix.Shutdown(pair[1], unix.SHUT_RDWR)
			} else {
				request := bytes.Repeat([]byte{1}, 21)
				if scenario == "drain-poison" {
					poisonCustody(c, 2)
					request, _ = TransferDrainRequest([16]byte{9})
				}
				if scenario == "legacy-drain" {
					request = []byte("TDP1")
				}
				if unix.Sendmsg(pair[1], request, nil, nil, unix.MSG_NOSIGNAL) != nil {
					t.Fatal("request")
				}
				if scenario == "drain-poison" || scenario == "legacy-drain" {
					deadline := time.Now().Add(time.Second)
					acked := false
					for !acked && time.Now().Before(deadline) {
						if n, _ := unix.Poll([]unix.PollFd{{Fd: int32(pair[1]), Events: unix.POLLIN}}, 50); n == 0 {
							continue
						}
						packet, fds, err := receiveTransferPacket(pair[1])
						for _, fd := range fds {
							_ = unix.Close(fd)
						}
						if err != nil {
							t.Fatal(err)
						}
						if string(packet) == "TDC1" {
							continue
						}
						if scenario == "drain-poison" {
							if len(packet) != 36 || binary.LittleEndian.Uint64(packet[28:]) != 2 || ValidateTransferDrainACK(packet, [16]byte{9}, binary.LittleEndian.Uint64(packet[20:])) == nil {
								t.Fatal("poison drain fabricated clean ACK")
							}
						} else if string(packet) != "TDOK" {
							t.Fatal("legacy drain protocol changed")
						}
						acked = true
					}
					if !acked {
						t.Fatal("fresh/legacy drain ACK missing")
					}
					return
				}
			}
			select {
			case <-joined:
			case <-time.After(time.Second):
				t.Fatal("channel loss did not terminate custody writer")
			}
			if !c.broken.Load() || c.failure.Load() != 12 {
				t.Fatal("close/truncation did not poison custody")
			}
		})
	}
}

// Only sealed startup authority can extend an exec allowlist. Matching FD
// numbers, unsealed bytes or a changed channel never establish that authority.
// origin: S7 internal review INTEG-M F4
func TestTransferExecDescriptorsRequireSealedStartupAuthority(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferExecAuthority(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer exec custody authority", g)
	testharness.AssertNoFDLeak(t, "transfer exec custody authority", fd, 0)
}

func exerciseTransferExecAuthority(t *testing.T) {
	t.Helper()
	previous := active
	active = nil
	defer func() { active = previous }()
	if fds, err := TransferExecDescriptors(); err != nil || len(fds) != 0 {
		t.Fatal("ordinary release acquired private inheritance")
	}
	pair := transferSocketPair(t)
	defer func() { _ = unix.Close(pair[0]); _ = unix.Close(pair[1]) }()
	data := append([]byte("TDCFG1\x00\x00"), make([]byte, 24)...)
	binary.LittleEndian.PutUint64(data[8:16], uint64(pair[0]))
	create := func(sealed bool) int {
		t.Helper()
		fd, err := unix.MemfdCreate("transfer-exec-authority", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := unix.Write(fd, data); err != nil {
			_ = unix.Close(fd)
			t.Fatal(err)
		}
		if sealed {
			if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, custodySeals); err != nil {
				_ = unix.Close(fd)
				t.Fatal(err)
			}
		}
		return fd
	}
	sealed, unsealed := create(true), create(false)
	defer func() { _ = unix.Close(sealed); _ = unix.Close(unsealed) }()
	active = &custody{fd: sealed, channel: pair[0], identity: sha256.Sum256(data)}
	if fds, err := TransferExecDescriptors(); err != nil || !reflect.DeepEqual(fds, []int{sealed, pair[0]}) {
		t.Fatalf("valid startup custody omitted: %v %v", fds, err)
	}
	for _, defect := range []string{"identity", "channel", "unsealed"} {
		original := &custody{fd: active.fd, channel: active.channel, identity: active.identity}
		switch defect {
		case "identity":
			active.identity[0] ^= 1
		case "channel":
			active.channel = pair[1]
		case "unsealed":
			active.fd = unsealed
		}
		if fds, err := TransferExecDescriptors(); err == nil || len(fds) != 0 {
			t.Fatalf("%s custody authorized inherited descriptors", defect)
		}
		active = original
	}
}
