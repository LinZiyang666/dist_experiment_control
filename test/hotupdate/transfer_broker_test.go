//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/transferharness"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §10.1 / T36
func TestTransferBrokerExecKeepsTrackerBeforeReaper(t *testing.T) {
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
						exerciseTransferBrokerExec(t, a)
					})
				}
			})
			testharness.AssertNoGoroutineLeak(t, "transfer broker binary boundaries", g)
			testharness.AssertNoFDLeak(t, "transfer broker binary boundaries", fd, 0)

		})
	}
}
func exerciseTransferBrokerExec(t *testing.T, a transferArtifacts) {
	for _, topology := range []string{"single", "sole-voter", "quorum"} {
		for _, cell := range transferCells("broker") {
			t.Run(topology+"/"+cell.name(), func(t *testing.T) {
				t.Parallel()
				s := newTransferTopology(t, a, topology)
				s.requireCapable()
				runTransferCell(t, s, cell, func(func()) { s.brokerCut(false); s.brokerCut(true) })
				rows, err := s.collectors["broker"].snapshot()
				transferMust(t, err)
				transferMust(t, validateTransferReaper(rows))
			})
		}
	}
}
func requireTransferIntegration(t *testing.T, a transferArtifacts) {
	t.Helper()
	s := newTransferStack(t, a)
	defer s.close()
	s.requireCapable()
}

// Broker cuts use the authorized transaction route and challenge completion
// on the original handle. The kernel process identity is independent evidence.
// origin: S8 internal review activation-and-route-migration F3
func (s *transferStack) brokerCut(original bool) {
	s.t.Helper()
	before, err := s.broker.SampleIdentity()
	transferMust(s.t, err)
	path, expected := s.a.b, s.a.shaB
	if original {
		path, expected = s.a.a, s.a.shaA
	}
	pending := s.startBrokerTransaction(path, expected)
	s.awaitTransactionStart(pending)
	s.waitBrokerTransaction(pending.tx)
	after, err := s.broker.SampleIdentity()
	transferMust(s.t, err)
	transferMust(s.t, validateTransferProcess(before, after, expected))
}

// No success receipt is inferred from dev/ino, artifact path or eventual SHA.
// origin: hot-update S7 plan §10.2 / T35 / M08–M12
func validateTransferProcess(before, after transferharness.Receipt, expected string) error {
	if before.PID <= 0 || before.ProcessStart == 0 || before.BootID == "" || expected == "" ||
		before.PID != after.PID || before.ProcessStart != after.ProcessStart || before.BootID != after.BootID || after.RunningSHA != expected {
		return errors.New("original process identity/running artifact changed")
	}
	return nil
}
func validateTransferBoundary(cell transferCell, o x.Observation, size int64) error {
	if o.TransferID == "" || o.EntryNonce == "" || o.PID <= 0 || o.ProcessStart == 0 || o.BootID == "" || o.EventSeq == 0 ||
		o.Generation == 0 || o.OriginalDeadline.Deadline <= 0 || o.OriginalDeadline.BootID != o.BootID || !transferWindowMatches(cell.window, o) {
		return errors.New("missing original boundary identity, deadline or live responsibility")
	}
	if !drillobserve.TransferPendingBoundary(cell.window, o) {
		return errors.New("pending boundary has no exact producer or sent reply-loss witness")
	}
	if (cell.tier == "b") != (o.ObjectVersion != "") {
		return errors.New("object version does not match the negotiated tier")
	}
	if cell.window == "receiver-write" || cell.window == "Sync-pending" || cell.window == "synced-prefix" {
		if o.DurableOffset <= 0 || o.DurableOffset >= size || o.FetchedOffset < o.DurableOffset || o.HashStateDigest == "" || o.Tmp.ResourceID == "" || o.Parent.ResourceID == "" {
			return errors.New("receiver boundary has no nonempty durable prefix or original FD closure")
		}
		if cell.tier == "b" && (o.Index == 0 || o.LastConsumedStreamSeq == 0) {
			return errors.New("durable prefix lacks physical chunk sequence")
		}
	}
	// A primitive window follows the whole sealed payload's durable Sync (for
	// Tier A, its single inline Sync) while the original tmp and parent are
	// still held; an earlier record names a byte window, not a primitive one.
	// origin: S7 FIX-TIERA2 / hot-update S7 plan §3 / §10.2 / T35
	if (cell.window == "pre-primitive" || cell.window == "post-primitive-pre-outcome") &&
		(o.DurableOffset != size || o.Tmp.ResourceID == "" || o.Parent.ResourceID == "") {
		return errors.New("primitive boundary does not follow the whole durable payload in its original tmp")
	}
	return nil
}
func validateTransferReaper(rows []x.Observation) error {
	installed, pass, reaped := make(map[uint64]bool), false, false
	for _, row := range rows {
		if row.Kind == x.ObserveInstall && row.InstalledBeforeReaper {
			installed[row.Generation] = true
		}
		if row.Kind == x.ObserveCallbackEnter {
			if !installed[row.Generation] {
				return errors.New("reaper ran before imported live tracker installation")
			}
			pass = true
		}
		if row.Kind == x.ObserveRetirement && row.RetirementDigest != "" {
			reaped = true
		}
	}
	if !pass || !reaped {
		return errors.New("reaper protection is vacuous: no pass and real retirement")
	}
	return nil
}

// The observer reopens the destination independently for each sample, then
// reads that one pinned inode. It never concatenates two versions across opens.
// origin: hot-update S7 plan §10.4 / T35 / M10B
type transferTargetWatch struct {
	mu         sync.Mutex
	err        error
	samples    int
	stop, done chan struct{}
	once       sync.Once
}

func startTransferTargetWatch(t *testing.T, path string, old, body []byte) *transferTargetWatch {
	t.Helper()
	w := &transferTargetWatch{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			data, err := os.ReadFile(path)
			w.mu.Lock()
			w.samples++
			if err != nil || !bytes.Equal(data, old) && !bytes.Equal(data, body) {
				w.err = errors.New("destination exposed absent, partial or mixed content")
			}
			w.mu.Unlock()
			select {
			case <-w.stop:
				return
			case <-tick.C:
			}
		}
	}()
	return w
}
func (w *transferTargetWatch) check() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.samples == 0 {
		return errors.New("destination observer was empty")
	}
	return w.err
}
func (w *transferTargetWatch) close() { w.once.Do(func() { close(w.stop); <-w.done }) }

type transferRawWatch struct {
	t          *testing.T
	s          *transferStack
	r          *transferRun
	mu         sync.Mutex
	rows       map[string]map[uint64]transferharness.RawMessage
	created    map[string]time.Time
	err        error
	sdk        bool
	sdkMu      sync.Mutex
	sdkErr     error
	sdkCancel  context.CancelFunc
	sdkDone    chan struct{}
	chunks     *transferRawChunkRead
	stop, done chan struct{}
}

func startTransferRawWatch(t *testing.T, s *transferStack, r *transferRun) *transferRawWatch {
	w := startTransferRawCensus(t, s, r)
	w.startObjectReads()
	return w
}

// startTransferRawCensus scans the raw streams only; a live-cut cell attaches
// its object readers after the cut with startObjectReads.
func startTransferRawCensus(t *testing.T, s *transferStack, r *transferRun) *transferRawWatch {
	w := &transferRawWatch{t: t, s: s, r: r, rows: make(map[string]map[uint64]transferharness.RawMessage), created: make(map[string]time.Time), stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			_ = w.scan()
			select {
			case <-w.stop:
				return
			case <-tick.C:
			}
		}
	}()
	return w
}
func (w *transferRawWatch) close() {
	close(w.stop)
	if w.sdkCancel != nil {
		w.sdkCancel()
		<-w.sdkDone
	}
	if w.chunks != nil {
		w.chunks.stop()
	}
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	mergeTransferRawChunks(w)
}
func (w *transferRawWatch) scan() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	ctx, cancel := context.WithTimeout(w.t.Context(), time.Second)
	defer cancel()
	names := []string{jsstream.HistoryStreamName(w.s.sid)}
	if w.r.cell.tier == "b" {
		names = append(names, "OBJ_"+proto.XferBucketName(w.s.sid))
	}
	for _, name := range names {
		stream, err := w.s.js.Stream(ctx, name)
		if err != nil {
			return err
		}
		rows, info, err := transferharness.ScanStream(ctx, stream, 0)
		if err != nil {
			return err
		}
		if at, ok := w.created[name]; ok && !at.Equal(info.Created) {
			w.err = errors.New("stream was recreated during continuation")
			return w.err
		}
		w.created[name] = info.Created
		if w.rows[name] == nil {
			w.rows[name] = make(map[uint64]transferharness.RawMessage)
		}
		for _, row := range rows {
			if row.Absent {
				continue
			}
			if old, ok := w.rows[name][row.Sequence]; ok && (old.Subject != row.Subject || !bytes.Equal(old.Data, row.Data)) {
				w.err = errors.New("physical JetStream sequence changed content")
			}
			w.rows[name][row.Sequence] = row
		}
	}
	w.sdkMu.Lock()
	defer w.sdkMu.Unlock()
	return errors.Join(w.err, w.sdkErr)
}
func (w *transferRawWatch) verify() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	mergeTransferRawChunks(w)
	if w.err != nil {
		return w.err
	}
	var history []transferharness.RawMessage
	for _, row := range w.rows[jsstream.HistoryStreamName(w.s.sid)] {
		if row.Subject == proto.SubjAuditTransfer(w.s.sid) {
			history = append(history, row)
		}
	}
	if err := validateTransferHistory(history, w.r.id, w.r.boundary.EntryNonce, transferDigest(w.r.body), int64(len(w.r.body))); err != nil {
		return err
	}
	observations, err := w.s.collectors["broker"].snapshot()
	if err != nil {
		return err
	}
	if err := validateTransferHistoryReceipts(history, observations, w.r.id); err != nil {
		return err
	}
	var effects []x.Observation
	for _, role := range []string{"agent", "ctl"} {
		rows, err := w.s.collectors[role].snapshot()
		if err != nil {
			return err
		}
		effects = append(effects, rows...)
	}
	if err := validateTransferEffects(effects, w.r.id); err != nil {
		return err
	}
	if w.r.cell.tier == "b" {
		w.sdkMu.Lock()
		defer w.sdkMu.Unlock()
		if w.sdkErr != nil {
			return w.sdkErr
		}
		if !w.sdk {
			return errors.New("original object was never independently readable through SDK Get")
		}
		var chunks []transferharness.RawMessage
		for _, row := range w.rows["OBJ_"+proto.XferBucketName(w.s.sid)] {
			if strings.Contains(row.Subject, ".C.") {
				chunks = append(chunks, row)
			}
		}
		return validateTransferChunks(chunks, w.r.boundary.ObjectVersion, w.r.body)
	}
	return nil
}
func validateTransferChunks(rows []transferharness.RawMessage, version string, body []byte) error {
	if version == "" || len(rows) == 0 {
		return errors.New("missing physical original object chunks")
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Sequence < rows[j].Sequence })
	var data []byte
	for i, row := range rows {
		get := func(key string) string {
			values := row.Headers[key]
			if len(values) != 1 {
				return ""
			}
			return values[0]
		}
		if !strings.HasSuffix(row.Subject, ".C."+version) || get(proto.TransferHeaderVersion) != "1" || get(proto.TransferHeaderIndex) != strconv.Itoa(i) ||
			get(proto.TransferHeaderOffset) != strconv.Itoa(len(data)) || get(proto.TransferHeaderLength) != strconv.Itoa(len(row.Data)) || get(proto.TransferHeaderSHA256) != transferDigest(row.Data) {
			return errors.New("physical chunks changed NUID, duplicated an index or lost a receipt")
		}
		data = append(data, row.Data...)
	}
	if !bytes.Equal(data, body) {
		return errors.New("physical chunk bytes differ from the original source")
	}
	return nil
}
func validateTransferHistory(rows []transferharness.RawMessage, id, nonce, digest string, size int64) error {
	starts, terminals := 0, 0
	for _, row := range rows {
		var record struct {
			TransferID     string `json:"transfer_id"`
			EntryNonce     string `json:"entry_nonce"`
			Kind           string `json:"kind"`
			SHA256         string `json:"sha256"`
			Bytes          int64  `json:"bytes"`
			TerminalID     string `json:"terminal_id"`
			OutcomeUnknown bool   `json:"outcome_unknown"`
		}
		if err := json.Unmarshal(row.Data, &record); err != nil {
			return err
		}
		if record.TransferID != id {
			continue
		}
		if record.EntryNonce != nonce {
			return errors.New("history changed original entry nonce")
		}
		switch record.Kind {
		case "start":
			starts++
		case "complete":
			terminals++
			if record.TerminalID == "" || record.SHA256 != digest || record.Bytes != size || record.OutcomeUnknown {
				return errors.New("terminal does not describe receiver content")
			}
		default:
			return errors.New("opposite or unknown physical transfer terminal")
		}
	}
	if starts != 1 || terminals != 1 {
		return fmt.Errorf("physical history start=%d terminal=%d; need exactly one of each", starts, terminals)
	}
	return nil
}

// Match physical canonical bytes and their sequence against the original
// publisher receipt, rather than treating any terminal's existence as an ACK.
// origin: hot-update S7 plan §5.2 / T35 / M12
func validateTransferHistoryReceipts(history []transferharness.RawMessage, observations []x.Observation, id string) error {
	matched := 0
	for _, raw := range history {
		var record struct {
			TransferID string `json:"transfer_id"`
		}
		if err := json.Unmarshal(raw.Data, &record); err != nil {
			return err
		}
		if record.TransferID != id {
			continue
		}
		exact := false
		for _, row := range observations {
			if row.TransferID == id && row.Kind == x.ObserveHistory && row.HistorySeq == raw.Sequence && row.CanonicalDigest == transferDigest(raw.Data) {
				exact = true
			}
		}
		if !exact {
			return errors.New("physical canonical history has no exact publisher ACK")
		}
		matched++
	}
	if matched != 2 {
		return errors.New("exact START and TERMINAL publisher receipts missing")
	}
	return nil
}
func validateTransferEffects(rows []x.Observation, id string) error {
	entered, landed, synced := 0, 0, false
	for _, row := range rows {
		if row.TransferID != id {
			continue
		}
		if row.Kind == x.ObservePrimitiveEnter {
			entered++
		}
		if row.Kind == x.ObservePrimitiveExit && row.Outcome == x.OutcomeLanded && row.Errno == 0 {
			landed++
		}
		if row.Kind == x.ObserveDirectorySync && row.ParentSynced {
			synced = true
		}
	}
	if entered != 1 || landed != 1 || !synced {
		return errors.New("receiver did not perform exactly one durable filesystem primitive")
	}
	return nil
}
func (s *transferStack) verifyCalls(id string) error {
	s.wireMu.Lock()
	defer s.wireMu.Unlock()
	ids := make(map[string]string)
	for _, msg := range s.wire {
		var r struct {
			CallID     string `json:"call_id"`
			TransferID string `json:"transfer_id"`
		}
		if err := json.Unmarshal(msg.Data, &r); err != nil {
			return err
		}
		if r.TransferID != id {
			return errors.New("original ctl created a second transfer ID")
		}
		if call, ok := ids[msg.Subject]; ok && call != r.CallID {
			return errors.New("retry changed the original logical call ID")
		}
		ids[msg.Subject] = r.CallID
	}
	if len(ids) == 0 {
		return errors.New("no original ctl request identity was observed")
	}
	return nil
}
func transferStreamIdentities(t *testing.T, js jetstream.JetStream) map[string]time.Time {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	out := make(map[string]time.Time)
	list := js.ListStreams(ctx)
	for info := range list.Info() {
		out[info.Config.Name] = info.Created
	}
	transferMust(t, list.Err())
	if len(out) == 0 {
		t.Fatal("JS stream identity census empty")
	}
	return out
}
func transferCheckStreamIdentities(t *testing.T, before, after map[string]time.Time) {
	t.Helper()
	for name, stamp := range before {
		if !after[name].Equal(stamp) {
			t.Fatalf("JS restart recreated stream %s", name)
		}
	}
}

// The finalizer must describe each pending responsibility before calling the
// port that can acknowledge it. Later complete bytes cannot create that record.
// origin: S7 internal review INTEG-O F5
func TestTransferSettlementBoundariesPrecedeAcknowledgement(t *testing.T) {
	a := buildTransferArtifacts(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseTransferSettlementBoundaries(t, a)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "transfer pending settlement boundaries", g)
	testharness.AssertNoFDLeak(t, "transfer pending settlement boundaries", fd, 0)
}

func exerciseTransferSettlementBoundaries(t *testing.T, a transferArtifacts) {
	for _, direction := range []string{"push", "pull"} {
		t.Run(direction, func(t *testing.T) {
			s := newTransferStack(t, a)
			s.requireCapable()
			r := startTransferRun(t, s, transferCell{"b", direction, "broker", "synced-prefix"})
			defer r.watch.close()
			r.boundary = awaitTransferBoundary(t, s, r)
			r.id = r.boundary.TransferID
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			result, err := s.ctl.Wait(ctx)
			if err != nil || result.NativeRC != 0 {
				t.Fatalf("original transfer did not complete: native_rc=%d err=%v %s", result.NativeRC, err, s.output(s.ctl))
			}
			awaitTransferRetired(t, s, r.id)
			transferMust(t, s.collectors["broker"].drain())
			rows, err := s.collectors["broker"].snapshot()
			transferMust(t, err)
			transferMust(t, validateTransferSettlementBoundaries(rows, r.id))
		})
	}
}

func validateTransferSettlementBoundaries(rows []x.Observation, id string) error {
	var terminal, cleanup, history, retired x.Observation
	for _, row := range rows {
		if row.TransferID != id {
			continue
		}
		if row.Kind == x.ObserveFinish && row.Phase == x.PhaseTerminalPending && row.HistorySeq == 0 && row.CanonicalDigest != "" && terminal.EventSeq == 0 {
			terminal = row
		}
		if terminal.EventSeq != 0 && row.Kind == x.ObserveHistory && row.CanonicalDigest == terminal.CanonicalDigest && row.HistorySeq > 0 {
			history = row
		}
		if row.Kind == x.ObserveFinish && row.Phase == x.PhaseCleanupPending && row.RetirementDigest == "" && cleanup.EventSeq == 0 {
			cleanup = row
		}
		if row.Kind == x.ObserveRetirement && row.Phase == x.PhaseSettled && row.RetirementDigest != "" {
			retired = row
		}
	}
	if terminal.EventSeq == 0 || history.EventSeq <= terminal.EventSeq {
		return errors.New("terminal pending boundary was not emitted before its exact history ACK")
	}
	if cleanup.EventSeq <= history.EventSeq || retired.EventSeq <= cleanup.EventSeq || cleanup.CanonicalDigest != terminal.CanonicalDigest {
		return errors.New("cleanup pending boundary was not emitted between history ACK and retirement")
	}
	for _, row := range []x.Observation{terminal, cleanup} {
		if row.OriginalDeadline.Deadline <= 0 || row.OriginalDeadline.BootID != row.BootID || row.Revision == 0 || !row.AdmissionClosed {
			return errors.New("pending boundary lost its original deadline, durable revision or closed gate")
		}
	}
	if terminal.OriginalDeadline != cleanup.OriginalDeadline {
		return errors.New("settlement boundary refreshed the original deadline")
	}
	return nil
}

// A phase label or a successful reserve send does not prove a lost reply or
// the pending work of its producer. Require the exact qualifying record.
// origin: S7 internal review INTEG-O F5
func TestTransferPendingBoundaryRequiresProducerProof(t *testing.T) {
	for _, window := range []string{"grant-reply-lost", "terminal-pre-history-ACK", "cleanup-pending"} {
		t.Run(window, func(t *testing.T) {
			row := transferOracleObservation()
			row.CanonicalDigest = strings.Repeat("a", 64)
			row.AdmissionClosed, row.HistorySeq, row.RetirementDigest = true, 0, ""
			switch window {
			case "grant-reply-lost":
				row.Kind, row.Phase, row.GrantNonce = x.ObserveReserve, x.PhaseCommitReserved, "grant"
				row.Attempt = x.Attempt{LogicalID: "reserve", Ordinal: 1, Phase: "request", StartedMonoNS: 1, FinishedMonoNS: 2, Sent: true}
			case "terminal-pre-history-ACK":
				row.Kind, row.Phase = x.ObserveControl, x.PhaseTerminalPending
			case "cleanup-pending":
				row.Kind, row.Phase, row.HistorySeq = x.ObserveControl, x.PhaseCleanupPending, 2
			}
			cell := transferCell{"b", "push", "broker", window}
			if validateTransferBoundary(cell, row, 100) == nil {
				t.Fatal("boundary accepted a phase label without the producer's exact pending proof")
			}
			if window == "grant-reply-lost" {
				row.Attempt.Class = "reply"
			} else {
				row.Kind = x.ObserveFinish
			}
			transferMust(t, validateTransferBoundary(cell, row, 100))
		})
	}
}
