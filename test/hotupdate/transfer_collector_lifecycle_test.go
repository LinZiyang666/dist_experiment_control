//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/drillobserve/receipt"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/transferharness"
	"golang.org/x/sys/unix"
)

// Real: a supervised process with a pinned live identity, exit/waitpid, procfs
// and sealed memfd. Simulated: only its custody record and procfs error class.
// origin: S7 internal review FIX-TIERA2 F3 / hot-update S7 plan §10.4 / T46
func TestTransferCollectorAcceptsLateBatchWithoutHidingSurvivors(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLateTransferBatch(t)
	}
	testharness.AssertNoGoroutineLeak(t, "late transfer custody", g)
	testharness.AssertNoFDLeak(t, "late transfer custody", fd, 0)
}

func exerciseLateTransferBatch(t *testing.T) {
	t.Helper()
	h := transferharness.New(t, transferSupervisor{})
	defer func() { transferMust(t, h.Close()) }()
	binaryPath := filepath.Join(h.Root, "interpreter")
	transferCopy(t, "/usr/bin/python3", binaryPath)
	s := &transferStack{t: t, h: h, collectors: make(map[string]*transferCollector)}
	release := filepath.Join(h.Root, "exit")
	p := s.launch("ctl", binaryPath, h.Root, []string{"-c", "import pathlib,sys,time\nwhile not pathlib.Path(sys.argv[1]).exists(): time.sleep(.001)", release}, true)
	c := s.collectors["ctl"]
	defer c.close()
	pinned := p.Receipt()
	row := x.Observation{Kind: x.ObserveControl, Phase: x.PhasePrepared, Leg: "ctl", TransferID: "original", EntryNonce: "entry",
		PID: pinned.PID, ProcessStart: pinned.ProcessStart, BootID: pinned.BootID, Generation: 1, EventSeq: 1, BoottimeNS: transferBoottime(t)}
	// EACCES while the original producer lives must remain SETUP-RED. The
	// bounded join must not kill it or convert that survivor to retirement.
	if transferProducerRetired(c, row, os.ErrPermission) || p.Receipt().NativeRC != -1 {
		t.Fatal("a live producer's inaccessible descriptors were hidden")
	}
	transferMust(t, os.WriteFile(release, nil, 0600))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done, err := p.Wait(ctx)
	transferMust(t, err)
	if done.NativeRC != 0 {
		t.Fatalf("producer exit rc=%d", done.NativeRC)
	}
	cell := transferCell{"b", "pull", "js", "source-pinned"}
	if transferCtlExitAllowed(cell, done, false) == nil {
		t.Fatal("an early successful exit hid the missing live cut")
	}
	transferMust(t, transferCtlExitAllowed(cell, done, true))
	cell.window = "cleanup-pending"
	transferMust(t, transferCtlExitAllowed(cell, done, false))
	failed := done
	failed.NativeRC = 75
	if transferCtlExitAllowed(cell, failed, false) == nil {
		t.Fatal("an early failed exit stood in for cleanup completion")
	}
	event, err := drillobserve.EncodeTransfer(row)
	transferMust(t, err)
	fd := sealedTransferBatch(t, c.identity, event)
	c.mu.Lock()
	c.accept(fd)
	c.mu.Unlock()
	transferMust(t, unix.Close(fd))
	rows, err := c.snapshot()
	transferMust(t, err)
	if len(rows) != 1 || rows[0] != row {
		t.Fatal("late first batch lost its original process evidence")
	}
	transferMust(t, c.checkNoReopen(row, nil))
	for _, procErr := range []error{os.ErrNotExist, os.ErrPermission} {
		if !transferProducerRetired(c, row, procErr) {
			t.Fatal("joined original producer's descriptors did not retire")
		}
		foreign := row
		foreign.ProcessStart++
		if transferProducerRetired(c, foreign, procErr) {
			t.Fatal("another process incarnation stood in for retirement")
		}
	}
	if transferProducerRetired(c, row, errors.New("unrelated procfs failure")) {
		t.Fatal("an unrelated procfs failure was suppressed")
	}
}

func sealedTransferBatch(t *testing.T, identity [32]byte, event drillobserve.Event) int {
	t.Helper()
	data := append([]byte("TDRILL1\x00"), identity[:]...)
	data = append(data, make([]byte, 16)...)
	data[40] = 1 // One nonzero custody incarnation, sequence one, no poison.
	data = binary.LittleEndian.AppendUint64(data, 1)
	data = binary.LittleEndian.AppendUint64(data, 0)
	data = append(data, receipt.EncodeEvents([]drillobserve.Event{event})...)
	fd, err := unix.MemfdCreate("late-transfer-batch", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	transferMust(t, err)
	_, err = unix.Write(fd, data)
	transferMust(t, err)
	_, err = unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL)
	transferMust(t, err)
	return fd
}

// The two sealed batches and ACKs arrive in legal wire order while both waiters
// remain idle. Finishing the first ACK at consumption would compare it to batch 2.
// origin: S7 internal review TAGGED-SUITE-SWEEP-A F2
func TestTransferDrainACKKeepsItsReceiveWatermark(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferDrainWatermark(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer drain receive watermark", g)
	testharness.AssertNoFDLeak(t, "transfer drain receive watermark", fd, 0)
}

func exerciseTransferDrainWatermark(t *testing.T) {
	t.Helper()
	c := newTransferCollector(t)
	c.running = true
	go c.read()
	defer func() { c.close(); transferMust(t, c.child.Close()); transferMust(t, c.config.Close()) }()
	for sequence := uint64(1); sequence <= 2; sequence++ {
		sendTransferDrainBatch(t, c, sequence)
		nonce := [16]byte{byte(sequence)}
		ack := transferDrainPacket(nonce, sequence, 0)
		transferMust(t, drillobserve.ValidateTransferDrainACK(ack, nonce, sequence))
		transferMust(t, unix.Sendmsg(int(c.child.Fd()), ack, nil, nil, unix.MSG_NOSIGNAL))
	}
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.drains) == 2 || c.err != nil
	}) {
		t.Fatal("collector did not receive both sealed drain barriers")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	transferMust(t, c.err)
	for sequence := uint64(1); sequence <= 2; sequence++ {
		nonce := [16]byte{byte(sequence)}
		ack, received := c.takeDrain(nonce)
		if !received || ack.err != nil {
			t.Fatalf("fresh ACK rejected after the reader accepted a later batch: received=%v err=%v", received, ack.err)
		}
		transferMust(t, drillobserve.ValidateTransferDrainACK(ack.packet, nonce, sequence))
		if _, received := c.takeDrain(nonce); received {
			t.Fatal("one drain ACK was consumed twice")
		}
	}
	// A second ACK cannot reuse the last sealed watermark, even with a new
	// nonce; keeping the first ACK valid must never excuse a missing batch.
	nonce := [16]byte{3}
	c.acceptDrain(nonce, transferDrainPacket(nonce, 2, 0))
	rejected, received := c.takeDrain(nonce)
	if !received || rejected.err == nil || c.err == nil {
		t.Fatal("a drain without a newly sealed batch hid incomplete custody")
	}
}

func sendTransferDrainBatch(t *testing.T, c *transferCollector, sequence uint64) {
	t.Helper()
	data := append([]byte("TDRILL1\x00"), c.identity[:]...)
	data = append(data, make([]byte, 16)...)
	data[40] = 1
	data = binary.LittleEndian.AppendUint64(data, sequence)
	data = binary.LittleEndian.AppendUint64(data, 0)
	data = binary.LittleEndian.AppendUint32(data, 0)
	fd, err := unix.MemfdCreate("drain-transfer-batch", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	transferMust(t, err)
	defer func() { transferMust(t, unix.Close(fd)) }()
	_, err = unix.Write(fd, data)
	transferMust(t, err)
	_, err = unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL)
	transferMust(t, err)
	transferMust(t, unix.Sendmsg(int(c.child.Fd()), []byte("TDC1"), unix.UnixRights(fd), nil, unix.MSG_NOSIGNAL))
}

func transferDrainPacket(nonce [16]byte, sequence, poison uint64) []byte {
	ack := append([]byte("TDA2"), nonce[:]...)
	ack = binary.LittleEndian.AppendUint64(ack, sequence)
	return binary.LittleEndian.AppendUint64(ack, poison)
}

// Each waiter consumes the exact validation made at receive time. A later
// legal batch may advance the reader before that waiter's next poll.
// origin: S7 internal review TAGGED-SUITE-SWEEP-A F2 / hot-update S7 plan §10.4
type transferDrainResult struct {
	packet []byte
	err    error
}

// Both helpers run under c.mu, shared with batch acceptance. Finish stays strict
// about poison, the current watermark and a newly sealed batch for every drain.
func (c *transferCollector) acceptDrain(nonce [16]byte, packet []byte) {
	err := c.reader.Finish(nonce, packet)
	if err != nil {
		// TDA2 carries the producer's first poison cause, as a batch does.
		// origin: S7 internal review FIX-TIERA2 F3 / hot-update S7 plan §10.4 / T41
		err = fmt.Errorf("transfer drain ACK (producer poison cause=%d): %w", binary.LittleEndian.Uint64(packet[28:36]), err)
		c.err = errors.Join(c.err, err)
	}
	if _, exists := c.drains[nonce]; exists {
		// A replay cannot replace an earlier result, including a rejected ACK.
		c.err = errors.Join(c.err, drillobserve.ErrTransferCustody)
		return
	}
	c.drains[nonce] = transferDrainResult{packet: bytes.Clone(packet), err: err}
}

func (c *transferCollector) takeDrain(nonce [16]byte) (transferDrainResult, bool) {
	ack, received := c.drains[nonce]
	delete(c.drains, nonce)
	return ack, received
}

// A receive-time success cannot overwrite rejection or excuse poisoned later
// custody. The cached nonce is one-use even when another fresh batch arrives.
// origin: S7 internal review TAGGED-SUITE-SWEEP-A F2
func TestTransferDrainACKRetainsPoisonAndOneUse(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferDrainRejections(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer drain rejection", g)
	testharness.AssertNoFDLeak(t, "transfer drain rejection", fd, 0)
}

func exerciseTransferDrainRejections(t *testing.T) {
	t.Helper()
	for _, mode := range []string{"nonce", "poison", "stale", "future", "replay", "late-poison"} {
		t.Run(mode, func(t *testing.T) {
			c := newTransferCollector(t)
			defer c.close()
			nonce := [16]byte{1}
			batch := func(sequence, poison uint64) []byte {
				data := append([]byte("TDRILL1\x00"), c.identity[:]...)
				data = append(data, make([]byte, 16)...)
				data[40] = 1
				data = binary.LittleEndian.AppendUint64(data, sequence)
				data = binary.LittleEndian.AppendUint64(data, poison)
				return binary.LittleEndian.AppendUint32(data, 0)
			}
			_, err := c.reader.Accept(batch(1, 0))
			transferMust(t, err)
			ack := transferDrainPacket(nonce, 1, 0)
			switch mode {
			case "nonce":
				ack[4]++
			case "poison":
				ack[28] = 1
			case "stale":
				ack[20] = 0
			case "future":
				ack[20] = 2
			}
			c.acceptDrain(nonce, ack)
			switch mode {
			case "replay":
				_, err := c.reader.Accept(batch(2, 0))
				transferMust(t, err)
				c.acceptDrain(nonce, transferDrainPacket(nonce, 2, 0))
			case "late-poison":
				_, err := c.reader.Accept(batch(2, 1))
				if err == nil {
					t.Fatal("poisoned later batch was accepted")
				}
				c.err = errors.Join(c.err, err)
			}
			cached, received := c.takeDrain(nonce)
			if !received || c.err == nil {
				t.Fatal("poison or replay disappeared from collector custody")
			}
			if mode == "replay" || mode == "late-poison" {
				if cached.err != nil || !bytes.Equal(cached.packet, ack) {
					t.Fatal("later custody replaced the original ACK's result")
				}
			} else if cached.err == nil {
				t.Fatal("invalid ACK was cached as success")
			}
			if _, received := c.takeDrain(nonce); received {
				t.Fatal("rejected ACK was consumed twice")
			}
		})
	}
}
