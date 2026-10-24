//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/transferharness"
	server "github.com/nats-io/nats-server/v2/server"
	"golang.org/x/sys/unix"
)

// Custody comes from the existing sealed P28 protocol, with its own sequence
// and poison checks. Product logs are diagnostic only, never transfer receipts.
// origin: hot-update S7 plan §10.4 / T35 / T46
type transferCollector struct {
	t             *testing.T
	fd            int
	child, config *os.File
	identity      [32]byte
	mu            sync.Mutex
	rows          []x.Observation
	witnesses     map[string]*drillobserve.TransferOFDWitness
	err           error
	stop, done    chan struct{}
	once          sync.Once
	running       bool
	drains        map[[16]byte]transferDrainResult
	reader        drillobserve.TransferCustodyReader
	incarnation   []byte
	sequences     map[string]*drillobserve.TransferSequence
	process       *transferharness.Process
	lastSHA       string
}

func newTransferCollector(t *testing.T) *transferCollector {
	t.Helper()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	transferMust(t, err)
	c := &transferCollector{t: t, fd: pair[0], child: os.NewFile(uintptr(pair[1]), "transfer-custody"),
		stop: make(chan struct{}), done: make(chan struct{}), witnesses: make(map[string]*drillobserve.TransferOFDWitness),
		drains: make(map[[16]byte]transferDrainResult), sequences: make(map[string]*drillobserve.TransferSequence)}
	fd, err := unix.MemfdCreate("transfer-observer-config", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		_ = unix.Close(pair[0])
		_ = c.child.Close()
		t.Fatal(err)
	}
	c.config = os.NewFile(uintptr(fd), "transfer-observer-config")
	var config [32]byte
	copy(config[:8], "TDCFG1\x00\x00")
	binary.LittleEndian.PutUint64(config[8:16], 3)
	_, err = rand.Read(config[16:])
	transferMust(t, err)
	_, err = c.config.Write(config[:])
	transferMust(t, err)
	_, err = unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL)
	transferMust(t, err)
	c.identity = sha256.Sum256(config[:])
	c.reader.Identity = c.identity
	return c
}
func (c *transferCollector) files() []*os.File { return []*os.File{c.child, c.config} }
func (c *transferCollector) environment() []string {
	return []string{"S7_TRANSFER_OBSERVER=v1", "S7_TRANSFER_CUSTODY_FD=4"}
}
func (c *transferCollector) started() {
	// Pin identity before ctl can exit; its first sealed batch may arrive only
	// after waitpid. Only a genuine incarnation change needs a fresh proc read.
	// origin: S7 internal review FIX-TIERA2 F3 / hot-update S7 plan §10.4 / T46
	r, err := c.process.SampleIdentity()
	transferMust(c.t, err)
	c.lastSHA = r.RunningSHA
	c.running = true
	_ = c.child.Close()
	_ = c.config.Close()
	go c.read()
}
func (c *transferCollector) read() {
	defer close(c.done)
	for {
		select {
		case <-c.stop:
			return
		default:
		}
		var header [36]byte
		n, _, err := unix.Recvfrom(c.fd, header[:], unix.MSG_PEEK|unix.MSG_DONTWAIT)
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
			_, _ = unix.Poll([]unix.PollFd{{Fd: int32(c.fd), Events: unix.POLLIN}}, 20)
			continue
		}
		if err != nil || n == 0 {
			return
		}
		if string(header[:min(n, 4)]) == "TDO1" {
			w, err := drillobserve.ReceiveTransferOFD(c.fd)
			c.mu.Lock()
			if err != nil {
				c.err = errors.Join(c.err, err)
			} else {
				r := w.Receipt()
				key := fmt.Sprintf("%d/%s/%s", r.PID, r.Resource.ResourceID, r.Resource.Name)
				if _, exists := c.witnesses[key]; exists {
					_ = w.Close()
				} else {
					c.witnesses[key] = w
				}
			}
			c.mu.Unlock()
			continue
		}
		body, oob := make([]byte, 64), make([]byte, unix.CmsgSpace(4*4))
		n, on, flags, _, err := unix.Recvmsg(c.fd, body, oob, unix.MSG_CMSG_CLOEXEC|unix.MSG_DONTWAIT)
		var fds []int
		messages, parseErr := unix.ParseSocketControlMessage(oob[:on])
		for _, msg := range messages {
			rights, e := unix.ParseUnixRights(&msg)
			parseErr = errors.Join(parseErr, e)
			fds = append(fds, rights...)
		}
		c.mu.Lock()
		if err != nil || parseErr != nil || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
			c.err = errors.Join(c.err, drillobserve.ErrTransferCustody)
		} else if n == 36 && string(body[:4]) == "TDA2" && len(fds) == 0 {
			var nonce [16]byte
			copy(nonce[:], body[4:20])
			c.acceptDrain(nonce, body[:n])
		} else if n == 4 && string(body[:4]) == "TDC1" && len(fds) == 1 {
			c.accept(fds[0])
		} else {
			c.err = errors.Join(c.err, drillobserve.ErrTransferCustody)
		}
		c.mu.Unlock()
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
	}
}
func (c *transferCollector) accept(fd int) {
	seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	const required = unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	var st unix.Stat_t
	if err != nil || seals&required != required || unix.Fstat(fd, &st) != nil || st.Size < 76 || st.Size > 1<<20 {
		c.err = errors.Join(c.err, drillobserve.ErrTransferCustody)
		return
	}
	data := make([]byte, st.Size)
	if n, err := unix.Pread(fd, data, 0); err != nil || n != len(data) {
		c.err = drillobserve.ErrTransferCustody
		return
	}
	if len(c.incarnation) != 0 && !bytes.Equal(c.incarnation, data[40:56]) {
		// A producer cannot erase a dropped sequence by claiming a new image.
		// Reset only after the independently pinned /proc executable changed.
		r, err := c.process.SampleIdentity()
		if err != nil || r.RunningSHA == c.lastSHA {
			c.err = drillobserve.ErrTransferCustody
			return
		}
		c.lastSHA = r.RunningSHA
		c.reader = drillobserve.TransferCustodyReader{Identity: c.identity}
	}
	c.incarnation = bytes.Clone(data[40:56])
	events, err := c.reader.Accept(data)
	if err != nil {
		// The frame carries only the producer's numeric poison cause. Name it:
		// a queue overflow, a refused descriptor send and a sequence gap have
		// different owners, yet all surfaced as one indistinct SETUP-RED2.
		// origin: S7 internal review FIX-TIERA2 F3 / hot-update S7 plan §10.4 / T41
		c.err = errors.Join(c.err, fmt.Errorf("sealed transfer batch (producer poison cause=%d sequence=%d): %w",
			binary.LittleEndian.Uint64(data[64:72]), binary.LittleEndian.Uint64(data[56:64]), err))
		return
	}
	for _, event := range events {
		if event.Kind != drillobserve.TransferEventKind {
			continue
		}
		o, err := drillobserve.DecodeTransfer(event)
		if err != nil {
			c.err = errors.Join(c.err, err)
			continue
		}
		key := fmt.Sprintf("%d/%d", o.PID, o.Generation)
		seq := c.sequences[key]
		if seq == nil {
			seq = new(drillobserve.TransferSequence)
			c.sequences[key] = seq
		}
		if _, err := seq.Accept(event); err != nil {
			c.err = errors.Join(c.err, fmt.Errorf("transfer ordinal kind=%s generation=%d ordinal=%d: %w", o.Kind, o.Generation, o.EventSeq, err))
			continue
		}
		// Both are the frozen Observation field closure; the P28 decoder's
		// private nominal type is deliberately not an importable second DTO.
		// Copy by exact field name: a JSON round trip silently dropped every
		// tagged Attempt field (logical_id, started/finished_mono_ns), so no
		// attempt-based boundary could ever validate.
		// origin: S7 FIX-CUT collector attempt evidence / hot-update S7 plan §10.4
		row, err := transferObservationRow(o)
		if err != nil {
			c.err = errors.Join(c.err, err)
		} else {
			c.rows = append(c.rows, row)
		}
	}
}

// transferObservationRow is the collector's only decoded-record conversion.
func transferObservationRow(decoded any) (x.Observation, error) {
	var row x.Observation
	err := copyTransferObservation(reflect.ValueOf(&row).Elem(), reflect.ValueOf(decoded))
	return row, err
}

// copyTransferObservation requires identical field names, order and kinds at
// every level; any drift in either frozen closure rejects the receipt.
// origin: S7 FIX-CUT collector attempt evidence / hot-update S7 plan §10.4
func copyTransferObservation(dst, src reflect.Value) error {
	if src.Kind() != reflect.Struct || dst.Kind() != reflect.Struct || src.NumField() != dst.NumField() {
		return drillobserve.ErrTransferCustody
	}
	for i := 0; i < dst.NumField(); i++ {
		from, to := src.Field(i), dst.Field(i)
		if dst.Type().Field(i).Name != src.Type().Field(i).Name || from.Kind() != to.Kind() {
			return drillobserve.ErrTransferCustody
		}
		if to.Kind() == reflect.Struct {
			if err := copyTransferObservation(to, from); err != nil {
				return err
			}
			continue
		}
		if !from.CanConvert(to.Type()) {
			return drillobserve.ErrTransferCustody
		}
		to.Set(from.Convert(to.Type()))
	}
	return nil
}

func (c *transferCollector) snapshot() ([]x.Observation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]x.Observation(nil), c.rows...), c.err
}

func (c *transferCollector) drain() error {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	request, err := drillobserve.TransferDrainRequest(nonce)
	if err != nil {
		return err
	}
	if err := unix.Sendmsg(c.fd, request, nil, nil, unix.MSG_NOSIGNAL); err != nil {
		return err
	}
	end := time.NewTimer(5 * time.Second)
	defer end.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		c.mu.Lock()
		ack, received := c.takeDrain(nonce)
		err = ack.err
		poison := c.err
		c.mu.Unlock()
		if received || poison != nil {
			return errors.Join(err, poison)
		}
		select {
		case <-end.C:
			return errors.New("fresh transfer drain ACK missing")
		case <-tick.C:
		}
	}
}
func (c *transferCollector) compareOFDs(row x.Observation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ref := range []x.ResourceRef{row.Source, row.Tmp, row.Parent} {
		if ref.ResourceID == "" {
			continue
		}
		w := c.witnesses[fmt.Sprintf("%d/%s/%s", row.PID, ref.ResourceID, ref.Name)]
		if w == nil {
			return errors.New("original transfer OFD custody missing")
		}
		fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", row.PID))
		if err != nil {
			return err
		}
		matched := false
		for _, f := range fds {
			fd, err := strconv.Atoi(f.Name())
			if err != nil {
				return err
			}
			if w.Compare(row.PID, row.ProcessStart, fd) == nil {
				matched = true
				break
			}
		}
		if !matched {
			return errors.New("original transfer OFD was closed or reopened (kernel comparison)")
		}
	}
	return nil
}

func (c *transferCollector) close() {
	c.once.Do(func() {
		close(c.stop)
		if c.running {
			<-c.done
		} else {
			_ = c.child.Close()
			_ = c.config.Close()
		}
		_ = unix.Close(c.fd)
		for _, w := range c.witnesses {
			if err := w.Close(); err != nil {
				c.t.Error(err)
			}
		}
	})
}

// Separate perturbations have different independent identity assertions; a
// reconnect log cannot stand in for a same-store server restart or S5 carrier.
// origin: hot-update S7 plan §10.1 / T46
// origin: S7 internal review FIX-TIERA2 F1
// origin: S7 internal review FIX-TIERA2 F2
// origin: S7 internal review FIX-TIERA2 F3
// origin: S7 internal review FIX-TIERA2 F4
// origin: S7 internal review CUT-CAPACITY F5–F6
func TestTransferCarrierAndJSRestartReceiptsAreDistinct(t *testing.T) {
	// origin: hot-update S10 lead decision D-A
	for _, mode := range testharness.CandidateBuildModes() {
		t.Run(mode, func(t *testing.T) {
			a := buildTransferArtifacts(t)
			requireTransferIntegration(t, a)
			g, fd := runtime.NumGoroutine(), testharness.FDCount()
			// The blocking parent joins all five rounds before the direct leak checks.
			// In particular, five real 120s watchdog rounds cannot run serially inside
			// the lead's 600s per-test partition. Fixture concurrency is bounded below.
			// origin: hot-update S7 plan lead L17 / §11.3
			t.Run("rounds", func(t *testing.T) {
				for round := 0; round < 5; round++ {
					t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
						t.Parallel()
						exerciseTransferRestart(t, a)
					})
				}
			})
			testharness.AssertNoGoroutineLeak(t, "transfer restart boundaries", g)
			if testharness.FDCount() > fd {
				files, err := os.ReadDir("/proc/self/fd")
				if err == nil {
					for _, file := range files {
						name, err := os.Readlink("/proc/self/fd/" + file.Name())
						if err == nil {
							t.Logf("restart remaining fd=%s target=%s", file.Name(), name)
						}
					}
				}
			}
			testharness.AssertNoFDLeak(t, "transfer restart boundaries", fd, 0)

		})
	}
}
func exerciseTransferRestart(t *testing.T, a transferArtifacts) {
	for _, kind := range []string{"carrier", "data-connection", "same-store-js"} {
		for _, cell := range transferCells("js") {
			// Same-store JS covers every E4 boundary. Attachment replacement
			// needs an established, nonempty byte leg; prepare-before-dial and
			// inline tier A have no dedicated OBJ data socket to disconnect.
			if kind != "same-store-js" && cell.window != "synced-prefix" || kind == "data-connection" && cell.tier == "a" {
				continue
			}
			t.Run(kind+"/"+cell.name(), func(t *testing.T) {
				t.Parallel()
				s := newTransferRestartStack(t, a)
				if kind == "carrier" && transferStructuralNA(cell) == "" {
					routeTransferThroughCarrier(t, s)
				}
				s.requireCapable()
				runTransferCell(t, s, cell, func(release func()) {
					cutStart := transferBoottime(t)
					s.interruptTransport(kind)
					cutEnd := transferBoottime(t)
					if kind == "same-store-js" && cell.tier == "b" && cell.direction == "push" && cell.window == "prepare-accepted" {
						// Keep the original ctl alive for diagnosis. The sealed Sync
						// timestamp must meet cutStart+10s; late progress stays red.
						// origin: S7 internal review FIX-PUSHWIN F1 / hot-update S7 plan §10.3 / M04
						release()
						classifyTransferPrepareProgress(t, s, cutStart, cutEnd)
					}
				})
				if kind == "carrier" {
					transferRequireCarriedLegs(t, s, int64(transferCellSize(cell)))
				}
			})
		}
	}
}

type transferPrepareProgress struct {
	prepared, reconciled, reaccepted, chunk, metadata, syncEntered, synced int64
}

// Only sealed rows for the original prepared owner can classify a missed
// window. A successful upload ACK cannot substitute for that owner's Sync.
// origin: S7 internal review FIX-PUSHWIN F1 / hot-update S7 plan §1.2 / §10.3 / M04
func transferPrepareProgressTiming(start, end int64, rows []x.Observation) (transferPrepareProgress, error) {
	var timing transferPrepareProgress
	var before x.Observation
	for _, row := range rows {
		if row.Leg == "agent" && row.Kind == x.ObserveControl && row.Phase == x.PhasePrepared && row.BoottimeNS <= start {
			before = row
			break
		}
	}
	if start <= 0 || end < start || before.PID == 0 || before.TransferID == "" || before.EntryNonce == "" || before.ObjectVersion == "" {
		return timing, errors.New("prepare progress has no original cut identity")
	}
	timing.prepared = before.BoottimeNS
	first := func(dst *int64, at int64) {
		if at > 0 && (*dst == 0 || at < *dst) {
			*dst = at
		}
	}
	for _, row := range rows {
		if row.TransferID != before.TransferID || row.BoottimeNS <= start {
			continue
		}
		if row.EntryNonce != before.EntryNonce || row.ObjectVersion != before.ObjectVersion || row.BootID != before.BootID {
			return timing, errors.New("prepare progress changed original identity or clock")
		}
		a := row.Attempt
		if row.Leg == "ctl" && row.Kind == x.ObserveAttempt && a.Sent && a.Class == "ok" && a.Phase == "request" &&
			a.StartedMonoNS > 0 && a.FinishedMonoNS >= a.StartedMonoNS && a.FinishedMonoNS <= row.BoottimeNS {
			switch a.LogicalID {
			case before.TransferID + "-prepare":
				first(&timing.reaccepted, a.FinishedMonoNS)
			case before.TransferID + "-status":
				first(&timing.reconciled, a.FinishedMonoNS)
			}
		}
		if row.Leg == "ctl" && row.ActualACKSeq > 0 {
			switch row.Kind {
			case x.ObserveResolve:
				first(&timing.chunk, row.BoottimeNS)
			case x.ObserveFinalize:
				first(&timing.metadata, row.BoottimeNS)
			case x.ObserveAppend, x.ObserveFetched, x.ObserveSynced, x.ObserveReaderStop, x.ObservePrimitiveEnter, x.ObservePrimitiveExit, x.ObserveDirectorySync, x.ObserveFreeze, x.ObserveImport, x.ObserveControl, x.ObserveAcknowledge, x.ObserveReserve, x.ObserveFinish, x.ObserveCallbackEnter, x.ObserveCallbackJoin, x.ObserveLedgerSync, x.ObserveHistory, x.ObserveRetirement, x.ObserveInstall, x.ObserveRollback, x.ObserveAttempt, x.ObserveDrain:
				// These observations are not remote chunk or metadata acknowledgments.
			}
		}
		if row.PID == before.PID && row.ProcessStart == before.ProcessStart && row.Kind == x.ObserveSynced {
			if row.FileSynced && row.DurableOffset > 0 {
				first(&timing.synced, row.BoottimeNS)
			} else if !row.FileSynced {
				first(&timing.syncEntered, row.BoottimeNS)
			}
		}
	}
	if timing.reconciled > timing.reaccepted {
		timing.reconciled = 0 // No status reconciliation preceded the accepted prepare.
	}
	if timing.reaccepted < end || timing.chunk < timing.reaccepted || timing.metadata < timing.chunk ||
		timing.syncEntered < timing.metadata || timing.synced < timing.syncEntered {
		return timing, errors.New("prepare progress lacks ordered prepare, upload and durable Sync evidence")
	}
	return timing, nil
}

func classifyTransferPrepareProgress(t *testing.T, s *transferStack, start, end int64) {
	t.Helper()
	if start == 0 || end == 0 {
		return // Setup failed before an actual restart; there is no timing claim.
	}
	var rows []x.Observation
	var timing transferPrepareProgress
	var timingErr error
	// Diagnose eventual Sync while the ctl is alive. Acceptance uses the sealed
	// time against the original cut, never the duration of this diagnostic tail.
	testharness.WaitFor(t, 90*time.Second, 10*time.Millisecond, func() bool {
		rows = nil
		for _, collector := range s.collectors {
			current, err := collector.snapshot()
			transferMust(t, err)
			rows = append(rows, current...)
		}
		timing, timingErr = transferPrepareProgressTiming(start, end, rows)
		return timingErr == nil || s.ctl.Receipt().NativeRC != -1
	})
	if timingErr != nil {
		t.Errorf("NOT_RUN prepare progress timing: %v", timingErr)
		return
	}
	evidence := fmt.Sprintf("cut_start_ns=%d cut_end_ns=%d sealed_timing_ns=%+v restart=%s prepare_after_cut=%s upload=%s first_sync=%s sync_syscall=%s size=9437184 rate_bps=1048576 budget=%s",
		start, end, timing, time.Duration(end-start), time.Duration(timing.reaccepted-start), time.Duration(timing.metadata-timing.reaccepted),
		time.Duration(timing.synced-start), time.Duration(timing.synced-timing.syncEntered), upgradebudget.TransferProgress)
	// origin: hot-update S10 lead decision D-A / T46 durable-progress SLO
	if !testharness.ReleaseBudgetRun(t) {
		t.Logf("race functional prepare progress (no SLO verdict): %s", evidence)
		return
	}
	path := fmt.Sprintf("/proc/%d/exe", s.agent.Receipt().PID)
	receipt, buildErr := testharness.CheckReleaseBudget(path, 0, upgradebudget.TransferProgress)
	transferMust(t, buildErr)
	t.Logf("release budget receipt: path=%s sha256=%s toolchain=%s race=%t CGO_ENABLED=%s measured=%s budget=%s", receipt.Path, receipt.SHA256, receipt.Toolchain, receipt.Race, receipt.CGOEnabled, time.Duration(timing.synced-start), upgradebudget.TransferProgress)
	// Every measured duration also passes the shared checker; the pure timing
	// classifier cannot accidentally turn a late sample into an SLO verdict.
	_, budgetErr := testharness.CheckReleaseBudget(path, time.Duration(timing.synced-start), upgradebudget.TransferProgress)
	if budgetErr != nil || transferPrepareProgressExceeded(timing, start) {
		t.Errorf("not_covered: push prepare-accepted completed-object receiver misses the cut-anchored durable-progress budget; %s", evidence)
		return
	}
	t.Log(evidence)
}

func transferPrepareProgressExceeded(timing transferPrepareProgress, start int64) bool {
	return time.Duration(timing.synced-start) > upgradebudget.TransferProgress
}

// origin: S7 internal review FIX-PUSHWIN F1 / hot-update S7 plan §10.3 / M04
func TestTransferPrepareProgressRequiresCutAnchoredSync(t *testing.T) {
	start, end := int64(time.Second), int64(3*time.Second)
	row := x.Observation{PID: 7, ProcessStart: 9, BootID: "boot", TransferID: "id", EntryNonce: "nonce", ObjectVersion: "version"}
	rows := make([]x.Observation, 6)
	for i := range rows {
		rows[i] = row
		rows[i].Leg, rows[i].BoottimeNS = "ctl", int64(time.Duration(i+4)*time.Second)
	}
	rows[0].Leg, rows[0].Kind, rows[0].Phase, rows[0].BoottimeNS = "agent", x.ObserveControl, x.PhasePrepared, start-1
	rows[1].Kind, rows[1].Attempt = x.ObserveAttempt, x.Attempt{LogicalID: "id-prepare", Sent: true, Class: "ok", Phase: "request", StartedMonoNS: end, FinishedMonoNS: rows[1].BoottimeNS}
	rows[2].Kind, rows[2].ActualACKSeq = x.ObserveResolve, 1
	rows[3].Kind, rows[3].ActualACKSeq = x.ObserveFinalize, 2
	rows[4].Leg, rows[4].Kind = "agent", x.ObserveSynced
	rows[5].Leg, rows[5].Kind, rows[5].FileSynced, rows[5].DurableOffset = "agent", x.ObserveSynced, true, 131072
	for _, defect := range []string{"healthy", "boundary", "late", "unsynced", "zero-prefix", "new-owner", "new-object", "new-boot", "missing-meta"} {
		t.Run(defect, func(t *testing.T) {
			input := append([]x.Observation(nil), rows...)
			switch defect {
			case "boundary":
				input[5].BoottimeNS = start + int64(upgradebudget.TransferProgress)
			case "late":
				input[5].BoottimeNS = start + int64(upgradebudget.TransferProgress) + 1
			case "unsynced":
				input[5].FileSynced = false
			case "zero-prefix":
				input[5].DurableOffset = 0
			case "new-owner":
				input[5].PID++
			case "new-object":
				input[5].ObjectVersion = "replacement"
			case "new-boot":
				input[5].BootID = "replacement"
			case "missing-meta":
				input[3].ActualACKSeq = 0
			}
			timing, err := transferPrepareProgressTiming(start, end, input)
			valid := defect == "healthy" || defect == "boundary" || defect == "late"
			if (err == nil) != valid || valid && transferPrepareProgressExceeded(timing, start) != (defect == "late") {
				t.Fatalf("%s: timing=%+v err=%v", defect, timing, err)
			}
		})
	}
}
func (s *transferStack) interruptTransport(kind string) {
	s.t.Helper()
	before, err := s.ns.Connz(&server.ConnzOptions{Subscriptions: true})
	transferMust(s.t, err)
	switch kind {
	case "same-store-js":
		id, store := s.ns.ID(), s.options.StoreDir
		streams := transferStreamIdentities(s.t, s.js)
		if s.gate == nil {
			s.t.Fatal("same-store restart needs the gated restart-lane fixture")
		}
		// Fence new CONNECT handshakes before Shutdown clears s.sys, then
		// stop the server the way SIGTERM does: established connections
		// receive "409 Server Shutdown" for their pending pulls first.
		// origin: S7 internal review CUT-CAPACITY F5
		s.gate.fence(s.t)
		s.ns.Shutdown()
		s.ns.WaitForShutdown()
		if late := s.gate.lateAnswers(); late != 0 {
			s.t.Fatalf("server processed %d CONNECTs after the fence, racing Shutdown's auth-callout teardown", late)
		}
		// The gate owns the client port, so the restarted server on the same
		// store takes a fresh one; rebinding the old one could lose to a
		// parallel fixture's ephemeral bind.
		s.options.Port = -1
		s.startJS()
		s.gate.reopen(s.ns.Addr().String())
		if s.ns.ID() == id || s.options.StoreDir != store {
			s.t.Fatal("JS restart did not replace server on original store")
		}
		if !testharness.WaitFor(s.t, 5*time.Second, 20*time.Millisecond, s.nc.IsConnected) {
			s.t.Fatal("oracle did not reconnect after JS restart")
		}
		transferCheckStreamIdentities(s.t, streams, transferStreamIdentities(s.t, s.js))
	case "data-connection":
		// Control and data connections deliberately share the session client
		// name. A producer's explicit server/client-ID mapping is required;
		// matching a name or disconnecting every session client is not evidence
		// that the dedicated reader/writer attachment was the affected socket.
		// origin: hot-update S7 plan §10.3 / T46
		mapped := make(map[uint64]bool)
		for _, role := range []string{"agent", "ctl"} {
			rows, err := s.collectors[role].snapshot()
			transferMust(s.t, err)
			for _, row := range rows {
				if row.ServerIdentity != s.ns.ID() || row.ConnectionIdentity == "" || (row.Kind != x.ObserveFetched && row.Kind != x.ObserveAppend) {
					continue
				}
				// ConnectionIdentity must identify the actual server-side CID.
				// An opaque reader-window token cannot identify a NATS socket.
				cid, err := strconv.ParseUint(row.ConnectionIdentity, 10, 64)
				if err != nil {
					continue
				}
				for _, c := range before.Conns {
					if c.Cid == cid {
						mapped[cid] = true
					}
				}
			}
		}
		if len(mapped) == 0 {
			s.t.Fatal("NOT_RUN pending P28/P29 connection mapping: no custody data-operation receipt maps to a live NATS server/client ID")
		}
		for cid := range mapped {
			transferMust(s.t, s.ns.DisconnectClientByID(cid))
		}
	case "carrier":
		// TCP sockets of the S5 carrier are unrelated to NATS connection IDs.
		// Destroy only the independently identified established carrier socket.
		transferBreakCarrier(s.t, s)
	default:
		s.t.Fatalf("unknown transfer interruption %q", kind)
	}
}

// A decoded sealed record keeps every Attempt field, including the tagged
// logical_id and BOOTTIME bounds. Their silent loss had made every attempt-
// based boundary (grant-reply-lost, inline-send-unconfirmed) unprovable.
// origin: S7 FIX-CUT collector attempt evidence / hot-update S7 plan §10.4 / T41
func TestTransferCollectorKeepsAttemptEvidence(t *testing.T) {
	sent := x.Observation{Kind: x.ObserveReserve, ProcessStart: 11, BootID: "boot", PID: 7, Generation: 2, CutID: "tracker.2",
		TransferID: "id", EntryNonce: "nonce", ObjectVersion: "version", Leg: "broker", Phase: x.PhaseCommitReserved, EventSeq: 3, BoottimeNS: 40,
		GrantNonce: "grant", OriginalDeadline: x.BootDeadline{BootID: "boot", Deadline: 900},
		Attempt: x.Attempt{LogicalID: "xfer-grant-id", Ordinal: 2, Phase: "request", StartedMonoNS: 30, FinishedMonoNS: 35, Sent: true, Class: "reply", BackoffNS: 7}}
	event, err := drillobserve.EncodeTransfer(sent)
	transferMust(t, err)
	decoded, err := drillobserve.DecodeTransfer(event)
	transferMust(t, err)
	row, err := transferObservationRow(decoded)
	transferMust(t, err)
	if !reflect.DeepEqual(row, sent) {
		t.Fatalf("collector changed sealed evidence:\nsent=%+v\nrow=%+v", sent.Attempt, row.Attempt)
	}
	var drifted struct{ Kind, Phase string }
	if copyTransferObservation(reflect.ValueOf(&drifted).Elem(), reflect.ValueOf(decoded)) == nil {
		t.Fatal("collector accepted a drifted observation closure")
	}
}
