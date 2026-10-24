//go:build linux

// Real: replay memfds, bounded partial Append, private codec and FD salvage.
// Stubbed: FileSource transfer failures only. Runtime freeze/restore is real.
// No carrier, authority lookup or same-PID exec is exercised here.
package flow

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 plan §3.1 / §3.2 / §8 T31
func TestFlowImagePendingAndRetirementSurvive(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseFlowImage(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "flow private image", goroutines)
	testharness.AssertNoFDLeak(t, "flow private image", fds, 0)
}

func exerciseFlowImage(t *testing.T) {
	t.Helper()
	budget := &workload.ReplayBudget{}
	replay, err := workload.NewReplay(budget, workload.DefaultReplayCapacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(replay.Close)
	if n, err := replay.Append(make([]byte, workload.DefaultReplayCapacity-3)); err != nil || int64(n) != workload.DefaultReplayCapacity-3 {
		t.Fatalf("fill replay: %d %v", n, err)
	}
	read := []byte("partial-source-append")
	n, err := replay.Append(read)
	if n != 3 || !errors.Is(err, workload.ErrBackpressure) {
		t.Fatalf("partial Append premise missing: %d %v", n, err)
	}
	records := flowImageFixture(t)
	setPrivateField(&records[0], "tx_pending", sealedstate.Field{Bytes: bytes.Clone(read[n:])})
	setPrivateField(&records[0], "tx_pending_error", sealedstate.Field{Bytes: []byte("backpressure")})
	setPrivateField(&records[0], "tx_end", sealedstate.Field{U64: uint64(replay.Snapshot().End)})
	encoded, err := encodeFlowState(records)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeFlowState(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var flow, retirement map[string]sealedstate.Field
	for _, rec := range decoded {
		if rec.Kind == sealedstate.KindFlow {
			flow = recordFields(rec)
		} else {
			retirement = recordFields(rec)
		}
	}
	if !bytes.Equal(flow["tx_pending"].Bytes, read[3:]) {
		t.Fatalf("TxPending lost partial Append suffix: %q", flow["tx_pending"].Bytes)
	}
	if !bytes.Equal(flow["rx_pending"].Bytes, []byte("unapplied-tail")) {
		t.Fatalf("RxPending lost successful-short-write suffix: %q", flow["rx_pending"].Bytes)
	}
	if !bytes.Equal(flow["raw_read_pending"].Bytes, []byte("read-before-append")) {
		t.Fatal("RawReadPending was lost")
	}
	if retirement["retired_through"].U64 != 7 || retirement["next_open_seq"].U64 != 13 ||
		!bytes.Equal(retirement["holes"].Bytes, []byte{0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 11}) {
		t.Fatal("retirement watermark or holes were lost")
	}
	if flow["rx_applied"].U64 != 19 || flow["rx_received"].U64 != 33 {
		t.Fatal("received was mistaken for applied")
	}
	if flow["tx_pending_error"].Type != sealedstate.FieldUTF8 || string(flow["tx_pending_error"].Bytes) != "backpressure" {
		t.Fatal("partial Append error receipt was lost")
	}
	projected, err := imageFromRecords(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if projected.ReservedBytes != workload.DefaultReplayCapacity {
		t.Fatalf("reservation counts retained bytes: %d", projected.ReservedBytes)
	}
	if len(projected.Records) != 1 || len(projected.Retirement) != 1 || !reflect.DeepEqual(projected.Retirement[0].Holes, []uint64{9, 11}) {
		t.Fatal("public projection lost retirement")
	}
}

func flowImageFixture(t *testing.T) []sealedstate.Record {
	t.Helper()
	build := func(kind string) sealedstate.Record {
		rec := sealedstate.Record{Kind: kind}
		for _, desc := range sealedstate.RecordDescriptors {
			if desc.Kind != kind {
				continue
			}
			for _, fd := range desc.Fields {
				f := sealedstate.Field{Name: fd.Name, Type: fd.Type}
				if fd.Type > sealedstate.FieldBool && fd.Width > 0 {
					f.Bytes = make([]byte, fd.Width)
				}
				rec.Fields = append(rec.Fields, f)
			}
		}
		return rec
	}
	flow := build(sealedstate.KindFlow)
	id := ID{1}
	for _, x := range []struct {
		name string
		f    sealedstate.Field
	}{
		{"id", sealedstate.Field{Bytes: id[:]}}, {"endpoint_id", sealedstate.Field{Bytes: []byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}},
		{"open_seq", sealedstate.Field{U64: 12}}, {"phase", sealedstate.Field{U64: uint64(FlowPhaseOpening)}},
		{"tx_pending", sealedstate.Field{Bytes: []byte("partial-append")}}, {"raw_read_pending", sealedstate.Field{Bytes: []byte("read-before-append")}},
		{"rx_pending", sealedstate.Field{Bytes: []byte("unapplied-tail")}}, {"rx_applied", sealedstate.Field{U64: 19}}, {"rx_received", sealedstate.Field{U64: 33}},
		{"replay_ref", sealedstate.Field{Bytes: []byte(flowResourceRef(id))}},
	} {
		setPrivateField(&flow, x.name, x.f)
	}
	retirement := build(sealedstate.KindRetirement)
	for _, x := range []struct {
		name string
		f    sealedstate.Field
	}{
		{"id", sealedstate.Field{Bytes: []byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}}, {"endpoint_id", sealedstate.Field{Bytes: []byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}},
		{"next_open_seq", sealedstate.Field{U64: 13}}, {"retired_through", sealedstate.Field{U64: 7}},
		{"holes", sealedstate.Field{Bytes: []byte{0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 11}}},
	} {
		setPrivateField(&retirement, x.name, x.f)
	}
	return []sealedstate.Record{flow, retirement}
}

func setPrivateField(rec *sealedstate.Record, name string, value sealedstate.Field) {
	for i, f := range rec.Fields {
		if f.Name == name {
			value.Name = name
			value.Type = f.Type
			rec.Fields[i] = value
			return
		}
	}
	panic("fixture field missing")
}

// origin: hot-update S5 plan §12.1 / §5.1 / T31
func TestFlowImageCodecRejectsIncompleteAndMalformedState(t *testing.T) {
	valid := flowImageFixture(t)
	data, err := encodeFlowState(valid)
	if err != nil {
		t.Fatal(err)
	}
	for end := 0; end < len(data); end++ {
		if _, err := decodeFlowState(data[:end]); err == nil {
			t.Fatalf("accepted truncated cut at %d", end)
		}
	}
	for _, mutation := range []struct {
		name   string
		change func([]sealedstate.Record) []sealedstate.Record
	}{
		{"missing-tx", func(rs []sealedstate.Record) []sealedstate.Record {
			for i, f := range rs[0].Fields {
				if f.Name == "tx_pending" {
					rs[0].Fields = append(rs[0].Fields[:i], rs[0].Fields[i+1:]...)
					break
				}
			}
			return rs
		}},
		{"duplicate", func(rs []sealedstate.Record) []sealedstate.Record {
			rs[0].Fields[len(rs[0].Fields)-1] = rs[0].Fields[0]
			return rs
		}},
		{"unknown", func(rs []sealedstate.Record) []sealedstate.Record { rs[0].Fields[0].Name = "unknown"; return rs }},
		{"case-alias", func(rs []sealedstate.Record) []sealedstate.Record { rs[0].Fields[0].Name = "Bind_nonce"; return rs }},
		{"type", func(rs []sealedstate.Record) []sealedstate.Record { rs[0].Fields[0].Type = 255; return rs }},
		{"phase-overflow", func(rs []sealedstate.Record) []sealedstate.Record {
			setPrivateField(&rs[0], "phase", sealedstate.Field{U64: 257})
			return rs
		}},
		{"rx-gap", func(rs []sealedstate.Record) []sealedstate.Record {
			setPrivateField(&rs[0], "rx_received", sealedstate.Field{U64: 34})
			return rs
		}},
		{"holes-unsorted", func(rs []sealedstate.Record) []sealedstate.Record {
			setPrivateField(&rs[1], "holes", sealedstate.Field{Bytes: []byte{0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 9}})
			return rs
		}},
		{"bool", func(rs []sealedstate.Record) []sealedstate.Record {
			setPrivateField(&rs[0], "revoked", sealedstate.Field{U64: 2})
			return rs
		}},
		{"unknown-kind", func(rs []sealedstate.Record) []sealedstate.Record { rs[0].Kind = "Flow"; return rs }},
		{"duplicate-id", func(rs []sealedstate.Record) []sealedstate.Record { return append(rs, rs[0]) }},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			malformed := mutation.change(flowImageFixture(t))
			if _, err := encodeFlowState(malformed); err == nil {
				t.Fatal("accepted malformed private cut")
			}
			if _, err := decodeFlowState(flowTestWire(malformed)); err == nil {
				t.Fatal("decoded malformed private cut")
			}
		})
	}
	if _, err := decodeFlowState(append(bytes.Clone(data), 0)); err == nil {
		t.Fatal("accepted trailing bytes")
	}
	wrongVersion := bytes.Clone(data)
	wrongVersion[5]++
	if _, err := decodeFlowState(wrongVersion); err == nil {
		t.Fatal("accepted unknown version")
	}
	huge := bytes.Clone(data)
	binary.BigEndian.PutUint32(huge[6:10], mathMaxUint32())
	if _, err := decodeFlowState(huge); err == nil {
		t.Fatal("accepted unbounded record count")
	}
	// Independent envelope/header expectation, not a decode-encode self proof.
	if !bytes.Equal(data[:10], []byte{'T', 'F', 'C', 'S', 0, 1, 0, 0, 0, 2}) {
		t.Fatalf("private header drifted: %x", data[:10])
	}
}
func mathMaxUint32() uint32 { return ^uint32(0) }

// origin: hot-update S5 plan §5.1 / §5.3
func TestFlowImageSalvageKeepsEveryTakenDescriptor(t *testing.T) {
	before := testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseFlowImageSalvage(t) })
	}
	testharness.AssertNoFDLeak(t, "flow image salvage", before, 0)
}
func exerciseFlowImageSalvage(t *testing.T) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(r.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(w.Close)
	records := flowImageFixture(t)
	im, err := imageFromRecords(records)
	if err != nil {
		t.Fatal(err)
	}
	second := im.Records[0]
	second.ID = ID{3}
	second.ReplayRef = flowResourceRef(second.ID)
	im.Records = append(im.Records, second)
	src := &flowImageSource{files: map[string]*os.File{im.Records[0].ReplayRef + "/replay": r}, fail: second.ReplayRef + "/replay", failedFile: w}
	salvage, err := takeFlowResources(im, records, src)
	if err == nil || len(salvage) != 2 || salvage[0].Files["replay"] != r || salvage[1].Files["replay"] != w {
		t.Fatalf("lost taken descriptor on later failure: %v %v", salvage, err)
	}
	if src.calls != 2 {
		t.Fatalf("unexpected Take count: %d", src.calls)
	}
	if _, err := r.Stat(); err != nil {
		t.Fatalf("salvage closed original: %v", err)
	}
	if _, err := w.Stat(); err != nil {
		t.Fatalf("rejected transferred FD was closed: %v", err)
	}
}

type flowImageSource struct {
	files      map[string]*os.File
	fail       string
	failedFile *os.File
	calls      int
}

func (s *flowImageSource) TakeFile(id, name string) (*os.File, error) {
	s.calls++
	key := id + "/" + name
	if key == s.fail {
		f := s.failedFile
		s.failedFile = nil
		return f, errors.New("fixture transfer rejected")
	}
	f := s.files[key]
	delete(s.files, key)
	if f == nil {
		return nil, errors.New("fixture missing descriptor")
	}
	return f, nil
}

// origin: hot-update S5 plan §5.1 / T31
func TestFlowImageRejectsPublicPrivateMismatchBeforeBusinessTake(t *testing.T) {
	im, err := imageFromRecords(flowImageFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	im.StateRef = "flow-state.00000000000000000000000000000001"
	for _, change := range []func(*Image){
		func(im *Image) { im.ReservedBytes-- }, func(im *Image) { im.Records[0].RxApplied = im.Records[0].RxReceived + 1 },
		func(im *Image) { im.Records[0].ReplayRef = "flow:bad" }, func(im *Image) { im.Retirement[0].Holes = []uint64{9, 9} },
	} {
		mutated := im
		mutated.Records = append([]RecordImage(nil), im.Records...)
		mutated.Retirement = append([]RetirementImage(nil), im.Retirement...)
		change(&mutated)
		src := &flowImageSource{}
		if _, _, err := Restore(Config{Budget: &workload.ReplayBudget{}}, mutated, src); !errors.Is(err, sealedstate.ErrInvalid) || src.calls != 0 {
			t.Fatalf("invalid public cut touched source: %v calls=%d", err, src.calls)
		}
	}
	other := im
	other.Records = append([]RecordImage(nil), im.Records...)
	other.Records[0].TxEnd++
	if equalFlowImages(im, other) {
		t.Fatal("public/private tx accounting mismatch accepted")
	}
	if _, _, err := (&Frozen{}).Export(); !errors.Is(err, sealedstate.ErrInvalid) {
		t.Fatalf("empty owner export: %v", err)
	}
}

// Real runtime and sealed-state ownership, including replacement and both
// lifecycle exits. The receiver remains live while every cut file is checked.
// origin: hot-update S5 plan §12.3 / T31
func TestFlowImageFrozenRuntimeRoundTrip(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowImageRuntimeRoundTrip(t)
	}
	testharness.AssertNoGoroutineLeak(t, "flow runtime image", goroutines)
	testharness.AssertNoFDLeak(t, "flow runtime image", fds, 0)
}

func exerciseFlowImageRuntimeRoundTrip(t *testing.T) {
	t.Helper()
	exerciseFlowImageConcurrentExport(t)
	for _, discard := range []bool{false, true} {
		budget := &workload.ReplayBudget{}
		m, err := New(Config{Budget: budget})
		if err != nil {
			t.Fatal(err)
		}
		defer func(closeFunc func() error) { _ = closeFunc() }(m.Close)
		frozen, err := m.Freeze(time.Now().Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		defer func(closeFunc func() error) { _ = closeFunc() }(frozen.Discard)
		_, old, err := frozen.Export()
		if err != nil {
			t.Fatal(err)
		}
		oldFile := old.Owned[0]
		im, snapshot, err := frozen.Export()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := oldFile.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("re-export retained the previous cut file")
		}
		src := handofftest.DupResources(t, snapshot.Resources)
		restored, salvage, err := Restore(Config{Budget: budget}, im, src)
		if err != nil {
			t.Fatalf("restore empty frozen manager: %v salvage=%d", err, len(salvage))
		}
		defer func(closeFunc func() error) { _ = closeFunc() }(restored.Close)
		if err := restored.ActivateRaw(); err != nil {
			t.Fatal(err)
		}
		stateFile := snapshot.Owned[0]
		if discard {
			err = frozen.Discard()
		} else {
			err = frozen.Thaw()
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stateFile.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("lifecycle exit retained the cut file")
		}
	}
}

// Concurrent exports share borrowed resources, but each sealed cut must have
// exactly one lifecycle owner. Discard must close every successful export.
// origin: hot-update S5 plan §5.3 / T31
func exerciseFlowImageConcurrentExport(t *testing.T) {
	t.Helper()
	m, err := New(Config{Budget: &workload.ReplayBudget{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(m.Close)
	frozen, err := m.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(frozen.Discard)
	type result struct {
		file *os.File
		err  error
	}
	results := make(chan result, 8)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < cap(results); worker++ {
		workers.Go(func() {
			<-start
			im, snapshot, err := frozen.Export()
			var file *os.File
			if err == nil {
				for _, res := range snapshot.Resources {
					if res.ID == im.StateRef {
						file = res.Files["state"]
					}
				}
			}
			results <- result{file, err}
		})
	}
	close(start)
	workers.Wait()
	if err := frozen.Discard(); err != nil {
		t.Fatal(err)
	}
	for worker := 0; worker < cap(results); worker++ {
		r := <-results
		if r.err != nil || r.file == nil {
			t.Fatalf("concurrent export lost its cut: file=%v err=%v", r.file != nil, r.err)
		}
		if _, err := r.file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("concurrent export escaped lifecycle cleanup")
		}
	}
}

// origin: hot-update S5 plan §5.1 / §5.3
func TestFlowImageResourcesFormExactClosure(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(r.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(w.Close)
	im, err := imageFromRecords(flowImageFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	good := []handoff.Resource{{ID: im.Records[0].ReplayRef, Kind: "flow-v1", Files: map[string]*os.File{"replay": r}}}
	if err := validateCutResources(im, good); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]handoff.Resource{
		nil,
		{{ID: good[0].ID, Kind: "flow-v1", Files: map[string]*os.File{}}},
		{{ID: good[0].ID, Kind: "flow-v1", Files: map[string]*os.File{"replay": nil}}},
		{{ID: good[0].ID, Kind: "flow-v1", Files: map[string]*os.File{"replay": r, "raw": w}}},
		{{ID: good[0].ID, Kind: "flow-v1", Data: []byte(`{"pending":"secret"}`), Files: good[0].Files}},
		{{ID: good[0].ID, Kind: "file", Files: good[0].Files}},
		{good[0], good[0]},
	} {
		if err := validateCutResources(im, bad); err == nil {
			t.Fatal("accepted incomplete, duplicated or public-secret resource closure")
		}
	}
}

// An independent wire writer permits malformed fields the product encoder
// correctly refuses. Decoder assertions must not merely test encoder guards.
func flowTestWire(records []sealedstate.Record) []byte {
	data := []byte{'T', 'F', 'C', 'S', 0, 1}
	data = binary.BigEndian.AppendUint32(data, uint32(len(records)))
	name := func(s string) {
		data = binary.BigEndian.AppendUint16(data, uint16(len(s)))
		data = append(data, []byte(s)...)
	}
	for _, rec := range records {
		name(rec.Kind)
		data = binary.BigEndian.AppendUint16(data, uint16(len(rec.Fields)))
		for _, field := range rec.Fields {
			name(field.Name)
			data = append(data, field.Type)
			value := field.Bytes
			if field.Type <= sealedstate.FieldBool {
				value = binary.BigEndian.AppendUint64(nil, field.U64)
				if field.Type == sealedstate.FieldBool {
					value = value[7:]
				}
			}
			data = binary.BigEndian.AppendUint32(data, uint32(len(value)))
			data = append(data, value...)
		}
	}
	return data
}

// origin: hot-update S5 plan §5.1 / §5.3 / T31
func TestFlowImageSealedMismatchNeverTakesBusinessDescriptors(t *testing.T) {
	records := flowImageFixture(t)
	data, err := encodeFlowState(records)
	if err != nil {
		t.Fatal(err)
	}
	ref := "flow-state.00000000000000000000000000000001"
	snapshot, err := sealedstate.New(ref, data)
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(snapshot.Close)
	im, err := imageFromRecords(records)
	if err != nil {
		t.Fatal(err)
	}
	im.StateRef = ref
	im.Records[0].TxEnd++
	src := handofftest.DupResources(t, snapshot.Resources)
	manager, salvage, err := Restore(Config{Budget: &workload.ReplayBudget{}}, im, src)
	if manager != nil || !errors.Is(err, sealedstate.ErrInvalid) || len(salvage) != 1 || salvage[0].Files["state"] == nil {
		t.Fatalf("mismatched cut crossed business boundary or lost sealed FD: manager=%v salvage=%v err=%v", manager, salvage, err)
	}
	for _, res := range salvage {
		for _, file := range res.Files {
			_ = file.Close()
		}
	}
}

// origin: hot-update S6 plan §6.3 / T24
func TestFlowRestoredDirectionIsPristineOnly(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowRestoredDirection(t)
	}
	testharness.AssertNoGoroutineLeak(t, "restored role direction", g)
	testharness.AssertNoFDLeak(t, "restored role direction", fd, 0)
}
func exerciseFlowRestoredDirection(t *testing.T) {
	t.Helper()
	// The enclosing broker/agent role supplies the direction; a carried frame
	// cannot select it. The sealed flow record deliberately has no role field.
	r := &record{open: OpenRequest{Direction: FlagDirection}}
	m := &Manager{state: &managerState{frozen: true, pristine: true, records: map[ID]*record{{1}: r}}}
	if err := RestoreDirection(m, 0); err != nil || r.open.Direction != 0 {
		t.Fatal("broker role retained agent direction", err)
	}
	if err := RestoreDirection(m, FlagDirection); err != nil || r.open.Direction != FlagDirection {
		t.Fatal("agent role lost direction", err)
	}
	if err := RestoreDirection(m, FlagDirection|2); err == nil {
		t.Fatal("accepted unknown direction flags")
	}
	for _, state := range []struct{ closed, frozen, pristine bool }{{true, true, true}, {false, false, true}, {false, true, false}} {
		m.state.closed, m.state.frozen, m.state.pristine = state.closed, state.frozen, state.pristine
		if err := RestoreDirection(m, 0); !errors.Is(err, ErrFrozen) || r.open.Direction != FlagDirection {
			t.Fatal("changed a non-pristine direction", err)
		}
	}
	if err := RestoreDirection(nil, 0); err == nil {
		t.Fatal("accepted nil owner")
	}
}
