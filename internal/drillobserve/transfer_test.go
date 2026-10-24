//go:build linux

package drillobserve

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"golang.org/x/sys/unix"
)

func transferSample(t *testing.T) xfercontract.Observation {
	t.Helper()
	p, err := TransferProcessIdentity(3, "cut.3")
	if err != nil {
		t.Fatal(err)
	}
	return xfercontract.Observation{
		Kind: xfercontract.ObserveSynced, ProcessStart: p.ProcessStart, BootID: p.BootID,
		PID: p.PID, Generation: p.Generation, CutID: p.CutID, TransferID: "transfer.1", EntryNonce: "entry.1",
		ObjectVersion: "nuid.1", Leg: "receiver", Phase: xfercontract.PhaseReceiving, EventSeq: 1, BoottimeNS: 100,
		Index: 2, Offset: 65536, Length: 32768, SHA256: strings.Repeat("a", 64), ExpectedSubjectSeq: 5,
		ExpectedStreamSeq: 7, ActualACKSeq: 9, AttachmentGeneration: 3, FetchedOffset: 98304, DurableOffset: 65536,
		LastConsumedStreamSeq: 9, HashStateDigest: strings.Repeat("b", 64), ConsumerIdentity: "consumer.1",
		ConnectionIdentity: "conn.1", ServerIdentity: "server.1", StreamIdentity: "stream.1", PONGConfirmed: true,
		ConsumerDeleted: true, Primitive: xfercontract.PrimitiveRename, Outcome: xfercontract.OutcomeLanded,
		Errno: 5, ParentSynced: true, Tmp: xfercontract.ResourceRef{ResourceID: "transfer.1", Name: "tmp", Kind: "xfer-tmp-v1"},
		Parent: xfercontract.ResourceRef{ResourceID: "transfer.1", Name: "parent", Kind: "xfer-parent-v1"}, Source: xfercontract.ResourceRef{ResourceID: "transfer.1", Name: "source", Kind: "xfer-source-v1"},
		OFDWitnessDigest: strings.Repeat("c", 64), AdmissionClosed: true, GrantNonce: "grant.1",
		CanonicalDigest: strings.Repeat("d", 64), StopNonce: "stop.1", OriginalDeadline: xfercontract.BootDeadline{BootID: p.BootID, Deadline: 1000},
		Revision: 11, PrimaryRevision: 12, OutboxRevision: 13, FileSynced: true, DirectorySynced: true,
		RaftIndex: 14, HistorySeq: 15, PublisherCheckpoint: 16, RetirementDigest: strings.Repeat("e", 64),
		ReaderSHA: strings.Repeat("f", 64), Active: true, InstalledBeforeReaper: true,
		Attempt: controlbudget.Attempt{LogicalID: "logical.1", Ordinal: 1, Phase: controlbudget.PhaseRequest, StartedMonoNS: 10, FinishedMonoNS: 20, Sent: true, Class: "ok", BackoffNS: 3}, NativeOutcome: 75,
	}
}

func transferRepack(t *testing.T, e Event, edit func(map[string]json.RawMessage)) Event {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(e.Data, &fields); err != nil {
		t.Fatal(err)
	}
	edit(fields)
	data, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	e.Data, e.Digest = data, sha256.Sum256(data)
	return e
}

// origin: hot-update S7 plan §10.4 / T41
func TestTransferObservationSchemaAndPoison(t *testing.T) {
	o := transferSample(t)
	testTransferContractProjection(t, o)
	e, err := EncodeTransfer(o)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeTransfer(e)
	if err != nil || !reflect.DeepEqual(transferProductValue(t, got), o) {
		t.Fatalf("typed schema lost evidence: got=%v err=%v", transferProductValue(t, got) == o, err)
	}
	if e.Kind != "transfer-v1" || e.Domain != "transfer" || TransferSetupRC != 2 {
		t.Fatal("schema or setup classification drift")
	}
	var raw map[string]json.RawMessage
	if json.Unmarshal(e.Data, &raw) != nil || len(raw) != 60 {
		t.Fatal("v1 field inventory drift")
	}
	for _, key := range []string{"process_start", "boot_id", "pid", "generation", "cut_id", "transfer_id", "entry_nonce", "object_version", "leg", "phase", "event_seq", "boottime_ns"} {
		if raw[key] == nil {
			t.Fatalf("missing required envelope field %s", key)
		}
	}
	// Remove EVERY top-level field, including legitimate zero values. Only
	// checking positive identifiers would let missing false/zero evidence pass.
	for key := range raw {
		t.Run("missing-"+key, func(t *testing.T) {
			bad := transferRepack(t, e, func(m map[string]json.RawMessage) { delete(m, key) })
			if _, err := DecodeTransfer(bad); !errors.Is(err, ErrTransferCustody) {
				t.Fatal("missing field became zero evidence")
			}
		})
		t.Run("null-"+key, func(t *testing.T) {
			bad := transferRepack(t, e, func(m map[string]json.RawMessage) { m[key] = json.RawMessage("null") })
			if _, err := DecodeTransfer(bad); !errors.Is(err, ErrTransferCustody) {
				t.Fatal("null field became zero evidence")
			}
		})
	}
	for _, key := range []string{"tmp", "parent", "source", "original_deadline", "attempt"} {
		var nested map[string]json.RawMessage
		if json.Unmarshal(raw[key], &nested) != nil {
			t.Fatal("nested schema")
		}
		for name := range nested {
			t.Run("nested-"+key+"-"+name, func(t *testing.T) {
				for _, replacement := range []json.RawMessage{nil, json.RawMessage("null")} {
					bad := transferRepack(t, e, func(m map[string]json.RawMessage) {
						var inner map[string]json.RawMessage
						_ = json.Unmarshal(m[key], &inner)
						if replacement == nil {
							delete(inner, name)
						} else {
							inner[name] = replacement
						}
						m[key], _ = json.Marshal(inner)
					})
					if _, err := DecodeTransfer(bad); !errors.Is(err, ErrTransferCustody) {
						t.Fatal("missing nested evidence accepted")
					}
				}
			})
		}
	}
	for _, change := range []struct{ key, value string }{
		{"event_seq", "-1"}, {"event_seq", "1.5"}, {"event_seq", "18446744073709551616"}, {"boottime_ns", "0"},
		{"process_start", "0"}, {"pid", "0"}, {"kind", `"unknown"`}, {"phase", `"unknown"`}, {"offset", "-1"},
		{"dropped", "1"}, {"sha256", `"private/path"`}, {"transfer_id", `"/private/path"`}, {"durable_offset", "98305"},
		{"future_field", "true"}, {"schema", "2"},
	} {
		bad := transferRepack(t, e, func(m map[string]json.RawMessage) { m[change.key] = json.RawMessage(change.value) })
		if _, err := DecodeTransfer(bad); !errors.Is(err, ErrTransferCustody) {
			t.Fatalf("invalid %s accepted", change.key)
		}
	}
	for _, data := range [][]byte{append(append([]byte(nil), e.Data...), []byte("{}")...), append([]byte(`{"pid":1,`), e.Data[1:]...), e.Data[:len(e.Data)-1]} {
		bad := e
		bad.Data, bad.Digest = data, sha256.Sum256(data)
		if _, err := DecodeTransfer(bad); err == nil {
			t.Fatal("duplicate/trailing/truncated JSON accepted")
		}
	}
	oversize := e
	oversize.Data = append(append([]byte(nil), e.Data...), bytes.Repeat([]byte(" "), MaxData)...)
	oversize.Digest = sha256.Sum256(oversize.Data)
	if _, err := DecodeTransfer(oversize); err == nil {
		t.Fatal("oversize raw evidence accepted")
	}
	bad := e
	bad.Digest[0] ^= 1
	if _, err := DecodeTransfer(bad); err == nil {
		t.Fatal("damaged custody digest accepted")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		if strings.Contains(fmt.Sprintf(format, e), "transfer.1") {
			t.Fatal("private event formatting escaped redaction")
		}
	}
	if data, err := json.Marshal(e); err != nil || string(data) != `{"redacted":true}` {
		t.Fatal("private event JSON escaped redaction")
	}

	testTransferSequenceAndAdapter(t, o)
	testTransferBatchPoison(t, e)
	testTransferFreshDrain(t, e)
}

func testTransferSequenceAndAdapter(t *testing.T, o xfercontract.Observation) {
	t.Helper()
	for _, ordinal := range []uint64{1, 3, 0} {
		var stream TransferSequence
		e, _ := EncodeTransfer(o)
		if _, err := stream.Accept(e); err != nil {
			t.Fatal(err)
		}
		bad := o
		bad.EventSeq = ordinal
		badEvent, _ := EncodeTransfer(bad)
		if _, err := stream.Accept(badEvent); !errors.Is(err, ErrTransferCustody) {
			t.Fatal("lost/reordered event did not poison reader")
		}
		o.EventSeq = 2
		good, _ := EncodeTransfer(o)
		o.EventSeq = 1
		if _, err := stream.Accept(good); err == nil {
			t.Fatal("later good record cleared poison")
		}
	}
	oldHook := Hook
	defer func() { Hook = oldHook }()
	Hook = nil
	if NewTransferObserver[xfercontract.Observation](TransferProcess{}) != nil {
		t.Fatal("ordinary release allocated an observer")
	}
	var events []Event
	Hook = func(e Event) { events = append(events, e) }
	observer := NewTransferObserver[xfercontract.Observation](TransferProcess{ProcessStart: o.ProcessStart, BootID: o.BootID, PID: o.PID, Generation: o.Generation, CutID: o.CutID})
	var joined sync.WaitGroup
	for i := 0; i < 32; i++ {
		joined.Add(1)
		go func() { defer joined.Done(); observer(o) }()
	}
	joined.Wait()
	if len(events) != 32 {
		t.Fatal("concurrent producer lost event")
	}
	var stream TransferSequence
	for _, e := range events {
		if _, err := stream.Accept(e); err != nil {
			t.Fatal("concurrent ordinal order changed")
		}
	}
	cut := o
	cut.CutID = "cut.4"
	observer(cut)
	if len(events) != 33 {
		t.Fatal("next cut restarted process sequence")
	}
	if got, err := stream.Accept(events[32]); err != nil || got.CutID != "cut.4" {
		t.Fatal("next cut tag lost within process generation")
	}
	bad := o
	bad.ProcessStart++
	observer(bad)
	observer(o)
	if len(events) != 34 || events[33].Kind != TransferPoisonKind {
		t.Fatal("bad producer did not irreversibly poison stream")
	}
	var captured []Event
	Hook = func(e Event) { captured = append(captured, e) }
	observer = NewTransferObserver[xfercontract.Observation](TransferProcess{ProcessStart: o.ProcessStart, BootID: o.BootID, PID: o.PID, Generation: o.Generation, CutID: o.CutID})
	bad = o
	bad.Dropped = 1
	observer(bad)
	observer(o)
	if len(captured) != 1 || captured[0].Kind != TransferPoisonKind {
		t.Fatal("invalid producer silently vanished")
	}
}

func transferTestBatch(e Event) ([]byte, [32]byte) {
	identity := sha256.Sum256([]byte("sealed-fixture-custody"))
	data := append([]byte("TDRILL1\x00"), identity[:]...)
	data = append(data, bytes.Repeat([]byte{7}, 16)...)
	data = binary.LittleEndian.AppendUint64(data, 1)
	data = binary.LittleEndian.AppendUint64(data, 0)
	return append(data, encodeEvents([]Event{e})...), identity
}

// Independently constructed actor ports share one process ordinal stream.
// Per-port counters looked valid alone but poisoned a merged custody reader.
// origin: S7 internal review INTEG-M F1
func TestTransferProcessObserversShareOrdinalStream(t *testing.T) {
	oldHook := Hook
	transferProcessObservers.Lock()
	oldObservers := transferProcessObservers.byGeneration
	transferProcessObservers.byGeneration = make(map[uint64]func(transferObservation))
	transferProcessObservers.Unlock()
	defer func() {
		Hook = oldHook
		transferProcessObservers.Lock()
		transferProcessObservers.byGeneration = oldObservers
		transferProcessObservers.Unlock()
	}()
	Hook = nil
	if ProcessTransferObserver[xfercontract.Observation](1, "sender") != nil {
		t.Fatal("ordinary release allocated a process observer")
	}
	var events []Event
	Hook = func(e Event) { events = append(events, e) }
	sender := ProcessTransferObserver[xfercontract.Observation](3, "sender")
	receiver := ProcessTransferObserver[xfercontract.Observation](3, "receiver")
	o := transferSample(t)
	o.Leg, o.CutID = "", ""
	sender(o)
	receiver(o)
	if len(events) != 2 {
		t.Fatal("one production port lost custody")
	}
	var sequence TransferSequence
	for i, event := range events {
		row, err := sequence.Accept(event)
		if err != nil || row.EventSeq != uint64(i+1) || row.DurableOffset != o.DurableOffset || row.OriginalDeadline != (transferDeadline{BootID: o.OriginalDeadline.BootID, Deadline: int64(o.OriginalDeadline.Deadline)}) {
			t.Fatalf("shared process sequence or original producer evidence changed: event=%d err=%v", i, err)
		}
		if row.Leg != []string{"sender", "receiver"}[i] {
			t.Fatal("process observer changed responsibility")
		}
	}
	if ProcessTransferObserver[xfercontract.Observation](3, "invalid/leg") != nil || len(events) != 3 || events[2].Kind != TransferPoisonKind {
		t.Fatal("cached observer accepted invalid responsibility")
	}
}

// Callback entry precedes the real request return; its finish time is unknown.
// Requiring a completed attempt here poisoned every production courier stream.
// origin: S7 internal review INTEG-M F1
func TestTransferCallbackEntryKeepsUnfinishedAttempt(t *testing.T) {
	o := transferSample(t)
	o.Kind = xfercontract.ObserveCallbackEnter
	o.Attempt.FinishedMonoNS = 0
	event, err := EncodeTransfer(o)
	if err != nil {
		t.Fatalf("actual callback entry refused before its request returned: %v", err)
	}
	decoded, err := DecodeTransfer(event)
	if err != nil || decoded.Attempt.FinishedMonoNS != 0 {
		t.Fatal("callback entry manufactured a request completion")
	}
	for _, kind := range []xfercontract.ObservationKind{xfercontract.ObserveAttempt, xfercontract.ObserveCallbackJoin} {
		bad := o
		bad.Kind = kind
		if _, err := EncodeTransfer(bad); err == nil {
			t.Fatal("finished callback accepted without a finish timestamp")
		}
	}
	o.Attempt.FinishedMonoNS = o.Attempt.StartedMonoNS - 1
	if _, err := EncodeTransfer(o); err == nil {
		t.Fatal("callback entry accepted an impossible finish timestamp")
	}
}

// A lifecycle adapter records the executable actually mapped by this process,
// including a deleted source pathname; its requested target is not evidence.
// origin: S7 internal review INTEG-M F1
func TestTransferProcessInstallRecordsRunningReader(t *testing.T) {
	oldHook := Hook
	transferProcessObservers.Lock()
	oldObservers := transferProcessObservers.byGeneration
	transferProcessObservers.byGeneration = make(map[uint64]func(transferObservation))
	transferProcessObservers.Unlock()
	defer func() {
		Hook = oldHook
		transferProcessObservers.Lock()
		transferProcessObservers.byGeneration = oldObservers
		transferProcessObservers.Unlock()
	}()
	var event Event
	Hook = func(e Event) { event = e }
	o := transferSample(t)
	o.Kind, o.ReaderSHA = xfercontract.ObserveInstall, ""
	ProcessTransferObserver[xfercontract.Observation](3, "tracker")(o)
	row, err := DecodeTransfer(event)
	file, openErr := os.Open("/proc/self/exe")
	if openErr != nil {
		t.Fatal(openErr)
	}
	hash := sha256.New()
	_, hashErr := io.Copy(hash, file)
	closeErr := file.Close()
	if err != nil || hashErr != nil || closeErr != nil || row.ReaderSHA != fmt.Sprintf("%x", hash.Sum(nil)) {
		t.Fatal("installed transfer owner omitted its actual running reader")
	}
}

func testTransferBatchPoison(t *testing.T, e Event) {
	t.Helper()
	data, identity := transferTestBatch(e)
	r := TransferCustodyReader{Identity: identity}
	events, err := r.Accept(data)
	if err != nil || len(events) != 1 || !bytes.Equal(events[0].Data, e.Data) {
		t.Fatal("custody framing lost observation")
	}
	for _, scenario := range []string{"identity", "poison", "sequence", "count", "truncated", "extra", "incarnation", "event-poison"} {
		t.Run(scenario, func(t *testing.T) {
			bad := append([]byte(nil), data...)
			switch scenario {
			case "identity":
				bad[8] ^= 1
			case "poison":
				bad[64] = 2
			case "sequence":
				bad[56] = 3
			case "count":
				bad[72] = maxBatch + 1
			case "truncated":
				bad = bad[:len(bad)-1]
			case "extra":
				bad = append(bad, 1)
			case "incarnation":
				clear(bad[40:56])
			case "event-poison":
				bad, _ = transferTestBatch(Event{Kind: TransferPoisonKind, Domain: "transfer"})
			}
			r := TransferCustodyReader{Identity: identity}
			if _, err := r.Accept(bad); !errors.Is(err, ErrTransferCustody) {
				t.Fatal("custody defect did not become SETUP2")
			}
			if _, err := r.Accept(data); err == nil {
				t.Fatal("good batch cleared custody poison")
			}
		})
	}
	if _, err := r.Accept(data); err == nil {
		t.Fatal("replayed sealed batch accepted")
	}
	many := make([]Event, maxBatch+1)
	for i := range many {
		many[i] = e
	}
	for _, events := range [][]Event{many, {{Kind: "other-domain", Domain: "flow", Data: make([]byte, MaxData+1)}}} {
		bad := append(append([]byte(nil), data[:72]...), encodeEvents(events)...)
		r := TransferCustodyReader{Identity: identity}
		if _, err := r.Accept(bad); err == nil {
			t.Fatal("custody count/payload bound bypassed")
		}
	}
}

func transferSocketPair(t *testing.T) [2]int {
	t.Helper()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	return pair
}

func testTransferFreshDrain(t *testing.T, e Event) {
	t.Helper()
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
			t.Error("custody writer failed to join on channel close")
		}
	}()
	c.record(e)
	nonce := [16]byte{5}
	request, err := TransferDrainRequest(nonce)
	if err != nil || unix.Sendmsg(pair[1], request, nil, nil, unix.MSG_NOSIGNAL) != nil {
		t.Fatal("drain request failed")
	}
	reader := TransferCustodyReader{Identity: c.identity}
	readACK := func() []byte {
		var ack []byte
		deadline := time.Now().Add(time.Second)
		for ack == nil && time.Now().Before(deadline) {
			if ready, err := unix.Poll([]unix.PollFd{{Fd: int32(pair[1]), Events: unix.POLLIN}}, 50); err != nil || ready == 0 {
				continue
			}
			data, fds, err := receiveTransferPacket(pair[1])
			if err != nil {
				t.Fatal(err)
			}
			if string(data) == "TDC1" && len(fds) == 1 {
				batch, readErr := readTransferSealed(fds[0], 76+maxBatch*(MaxData+328))
				for _, fd := range fds {
					_ = unix.Close(fd)
				}
				if readErr != nil {
					t.Fatal(readErr)
				}
				if _, err := reader.Accept(batch); err != nil {
					t.Fatal(err)
				}
			} else {
				for _, fd := range fds {
					_ = unix.Close(fd)
				}
				ack = data
			}
		}
		return ack
	}
	ack := readACK()
	pendingDrain := reader
	if reader.Finish(nonce, ack) != nil {
		t.Fatal("fresh product drain ACK missing or invalid")
	}
	wrongNonce, wrongSequence, poison := append([]byte(nil), ack...), append([]byte(nil), ack...), append([]byte(nil), ack...)
	wrongNonce[4] ^= 1
	wrongSequence[20] ^= 1
	poison[28] = 2
	for _, bad := range [][]byte{nil, []byte("TDOK"), ack[:len(ack)-1], wrongNonce, wrongSequence, poison} {
		r := pendingDrain
		if r.Finish(nonce, bad) != ErrTransferCustody {
			t.Fatal("missing/stale/truncated ACK signed success")
		}
		if r.Finish(nonce, ack) == nil {
			t.Fatal("late ACK cleared missing-ACK poison")
		}
	}
	nextNonce := [16]byte{6}
	request, err = TransferDrainRequest(nextNonce)
	if err != nil || unix.Sendmsg(pair[1], request, nil, nil, unix.MSG_NOSIGNAL) != nil {
		t.Fatal("second fresh drain request failed")
	}
	nextACK := readACK()
	if reader.Finish(nextNonce, nextACK) != nil {
		t.Fatal("second fresh product drain failed")
	}
	if reader.Finish(nextNonce, nextACK) != ErrTransferCustody {
		t.Fatal("one ACK closed both quiet-window drains")
	}
	if _, err := TransferDrainRequest([16]byte{}); err == nil {
		t.Fatal("unchallenged drain request accepted")
	}
}

// origin: hot-update S7 plan §10.4 / T47 / M09
func TestTransferObserverOFDWitnessRejectsReopen(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "source")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if _, err := file.Write([]byte("original-source-bytes")); err != nil {
		t.Fatal(err)
	}
	ref := xfercontract.ResourceRef{ResourceID: "transfer.1", Name: "source", Kind: "xfer-source-v1"}
	w, err := CaptureTransferOFD(file, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	process, err := TransferProcessIdentity(3, "cut.3")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Compare(os.Getpid(), process.ProcessStart, int(file.Fd())); err != nil {
		t.Fatal("kernel OFD evidence unavailable (SETUP2)")
	}
	originalOffset, err := file.Seek(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFL, 0)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := os.OpenFile(file.Name(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if _, err := reopened.Seek(originalOffset, 0); err != nil {
		t.Fatal(err)
	}
	before, _ := file.Stat()
	after, _ := reopened.Stat()
	if !os.SameFile(before, after) {
		t.Fatal("mutation did not reopen same inode")
	}
	if err := w.Compare(os.Getpid(), process.ProcessStart, int(reopened.Fd())); !errors.Is(err, ErrTransferCustody) {
		t.Fatal("same inode and offset reopen passed kernel OFD oracle")
	}
	if offset, _ := file.Seek(0, 1); offset != originalOffset {
		t.Fatal("oracle changed original offset")
	}
	if currentFlags, _ := unix.FcntlInt(file.Fd(), unix.F_GETFL, 0); currentFlags != flags {
		t.Fatal("oracle changed original flags")
	}
	if w.Compare(os.Getpid(), process.ProcessStart+1, int(file.Fd())) == nil || w.Compare(os.Getpid(), process.ProcessStart, -1) == nil {
		t.Fatal("PID reuse/invalid descriptor accepted")
	}
	pair := transferSocketPair(t)
	defer func() { _ = unix.Close(pair[0]); _ = unix.Close(pair[1]) }()
	if err := w.Send(pair[0]); err != nil {
		t.Fatal(err)
	}
	keeper, err := ReceiveTransferOFD(pair[1])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = keeper.Close() }()
	if flags, err := unix.FcntlInt(keeper.file.Fd(), unix.F_GETFD, 0); err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatal("received witness can escape exec custody")
	}
	if keeper.Receipt() != w.Receipt() || keeper.Receipt().OwnedFDs != 1 || keeper.Receipt().PeakFDs != 2 {
		t.Fatal("keeper lost witness/resource peak")
	}
	if keeper.Compare(os.Getpid(), process.ProcessStart, int(file.Fd())) != nil || keeper.Compare(os.Getpid(), process.ProcessStart, int(reopened.Fd())) == nil {
		t.Fatal("SCM_RIGHTS lost independent original OFD custody")
	}
	witnessFD := w.file.Fd()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(witnessFD, unix.F_GETFD, 0); err != unix.EBADF {
		t.Fatal("retired witness descriptor still open")
	}
	if w.Compare(os.Getpid(), process.ProcessStart, int(file.Fd())) == nil || w.Send(pair[0]) == nil {
		t.Fatal("closed witness fabricated kernel proof")
	}
	if err := keeper.Compare(os.Getpid(), process.ProcessStart, int(file.Fd())); err != nil {
		t.Fatal("closing exporter destroyed keeper's OFD")
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if keeper.Compare(os.Getpid(), process.ProcessStart, int(reopened.Fd())) == nil {
		t.Fatal("reopen adopted after original owner close")
	}
}

func transferProductValue(t *testing.T, value transferObservation) xfercontract.Observation {
	t.Helper()
	var product xfercontract.Observation
	if err := copyTransferValue(reflect.ValueOf(&product).Elem(), reflect.ValueOf(value)); err != nil {
		t.Fatal(err)
	}
	return product
}

// The adapter may erase Go enum names, never a field, name or integer width.
// origin: hot-update S7 plan §10.4 / §11.1 / T41
func testTransferContractProjection(t *testing.T, o xfercontract.Observation) {
	t.Helper()
	for _, value := range []any{nil, 1, map[string]any{}, &o} {
		if _, err := EncodeTransfer(value); err != ErrTransferCustody {
			t.Fatal("untyped input entered custody schema")
		}
	}
	original := reflect.ValueOf(o)
	for _, defect := range []string{"extra-field", "renamed-field", "scalar-kind"} {
		fields := make([]reflect.StructField, 0, original.NumField()+1)
		for i := 0; i < original.NumField(); i++ {
			fields = append(fields, original.Type().Field(i))
		}
		switch defect {
		case "extra-field":
			fields = append(fields, reflect.StructField{Name: "FutureEvidence", Type: reflect.TypeFor[bool]()})
		case "renamed-field":
			fields[0].Name = "FutureKind"
		case "scalar-kind":
			fields[1].Type = reflect.TypeFor[int64]()
		}
		value := reflect.New(reflect.StructOf(fields)).Elem()
		for i := 0; i < original.NumField(); i++ {
			value.Field(i).Set(original.Field(i).Convert(value.Field(i).Type()))
		}
		if _, err := EncodeTransfer(value.Interface()); err != ErrTransferCustody {
			t.Fatalf("%s contract drift entered custody schema", defect)
		}
	}
	for _, kind := range []xfercontract.ObservationKind{
		xfercontract.ObserveAppend, xfercontract.ObserveResolve, xfercontract.ObserveFinalize, xfercontract.ObserveFetched,
		xfercontract.ObserveSynced, xfercontract.ObserveReaderStop, xfercontract.ObservePrimitiveEnter, xfercontract.ObservePrimitiveExit,
		xfercontract.ObserveDirectorySync, xfercontract.ObserveFreeze, xfercontract.ObserveImport, xfercontract.ObserveControl,
		xfercontract.ObserveAcknowledge, xfercontract.ObserveReserve, xfercontract.ObserveFinish, xfercontract.ObserveCallbackEnter,
		xfercontract.ObserveCallbackJoin, xfercontract.ObserveLedgerSync, xfercontract.ObserveHistory, xfercontract.ObserveRetirement,
		xfercontract.ObserveInstall, xfercontract.ObserveRollback, xfercontract.ObserveAttempt, xfercontract.ObserveDrain,
	} {
		value := o
		value.Kind = kind
		if _, err := EncodeTransfer(value); err != nil {
			t.Fatalf("frozen kind %s rejected", kind)
		}
	}
	for _, phase := range []xfercontract.Phase{xfercontract.PhaseOpen, xfercontract.PhasePrepared, xfercontract.PhaseSending, xfercontract.PhaseReceiving, xfercontract.PhaseCommitReserved, xfercontract.PhaseDecided, xfercontract.PhaseTerminalPending, xfercontract.PhaseHistoryConfirmed, xfercontract.PhaseCleanupPending, xfercontract.PhaseSettled} {
		value := o
		value.Phase = phase
		if _, err := EncodeTransfer(value); err != nil {
			t.Fatalf("frozen phase %s rejected", phase)
		}
	}
}
