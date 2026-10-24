package flow

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"math"
	"os"
	"slices"
	"sort"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/workload"
)

// A cut's lifecycle owner installs the replacement under its freeze guard and
// closes the previous cut-owned files only after success. Thaw/Discard close
// the installed snapshot; business files in Resources remain borrowed.
// origin: hot-update S5 plan §5.3
type flowImageSnapshotOwner interface {
	replaceImageSnapshot(*sealedstate.Snapshot) error
}

// Export builds public accounting from the same private records it encodes.
// There is no independent manifest snapshot that could disagree with pending
// bytes or retirement evidence. Errors never include private field contents.
// origin: hot-update S5 plan §5.1 / §12.1 / T31
func (f *Frozen) Export() (Image, *sealedstate.Snapshot, error) {
	if f == nil || f.state == nil {
		return Image{}, nil, sealedstate.ErrInvalid
	}
	// Projection, sealing and ownership replacement share the lifecycle guard;
	// Thaw cannot reactivate borrowed files while this cut is still encoding.
	// origin: hot-update S5 plan §5.3 / T31
	f.state.mu.Lock()
	defer f.state.mu.Unlock()
	if f.state.released {
		return Image{}, nil, ErrClosed
	}
	state, err := snapshotFlowCutLocked(f.state)
	if err != nil {
		return Image{}, nil, err
	}
	im, err := imageFromRecords(state.Records)
	if err != nil {
		return Image{}, nil, err
	}
	if err := validateCutResources(im, state.Resources); err != nil {
		return Image{}, nil, err
	}
	data, err := encodeFlowState(state.Records)
	if err != nil {
		return Image{}, nil, err
	}
	var cut ID
	if _, err := rand.Read(cut[:]); err != nil {
		return Image{}, nil, err
	}
	im.StateRef = "flow-state." + hex.EncodeToString(cut[:])
	snapshot, err := sealedstate.New(im.StateRef, data)
	if err != nil {
		return Image{}, nil, err
	}
	snapshot.Resources = append(snapshot.Resources, state.Resources...)
	owner, ok := any(f.state).(flowImageSnapshotOwner)
	if !ok {
		_ = snapshot.Close()
		return Image{}, nil, sealedstate.ErrInvalid
	}
	if err := owner.replaceImageSnapshot(snapshot); err != nil {
		_ = snapshot.Close()
		return Image{}, nil, err
	}
	return im, snapshot, nil
}

// Restore validates the entire private/public cut before taking any business
// descriptor. Every transferred file enters salvage before its next predicate;
// a bad second socket must not lose the first socket or the sealed state file.
// No pump, DNS lookup, connect or authority operation occurs here.
// origin: hot-update S5 plan §5.1 / §5.3 / §12.1
func Restore(cfg Config, im Image, src rawsocket.FileSource) (*Manager, []handoff.Resource, error) {
	if err := validatePublicImage(im); err != nil {
		return nil, nil, err
	}
	if cfg.Budget == nil || (cfg.Budget.Limit != 0 && cfg.Budget.Limit != workload.DefaultReplayBudget) ||
		cfg.MaxFlows < 0 || cfg.MaxFlows > MaxFlows || cfg.MaxPendingFDs < 0 || cfg.MaxPendingFDs > MaxPendingFDs ||
		(cfg.MaxFlows > 0 && activeImageFlows(im) > cfg.MaxFlows) || src == nil {
		return nil, nil, sealedstate.ErrInvalid
	}
	data, file, err := sealedstate.Read(src, im.StateRef, "state", sealedstate.MaxStateBytes)
	var salvage []handoff.Resource
	if file != nil {
		salvage = append(salvage, handoff.Resource{ID: im.StateRef, Kind: "sealed-state-v1", Files: map[string]*os.File{"state": file}})
	}
	if err != nil {
		return nil, salvage, err
	}
	records, err := decodeFlowState(data)
	if err != nil {
		return nil, salvage, err
	}
	projected, err := imageFromRecords(records)
	if err != nil {
		return nil, salvage, err
	}
	projected.StateRef = im.StateRef
	if !equalFlowImages(im, projected) {
		return nil, salvage, sealedstate.ErrInvalid
	}
	resources, err := takeFlowResources(im, records, src)
	salvage = append(salvage, resources...)
	if err != nil {
		return nil, salvage, err
	}
	m, _, err := restoreCutState(cfg, sealedstate.CutState{Records: records, Resources: resources})
	// The hook must leave every resource frozen and borrowed on failure, including
	// any wrapper it constructed before rejecting a later resource. Salvage owns
	// this complete list, rather than a hook's possibly incomplete error suffix.
	if err != nil {
		return nil, salvage, err
	}
	if err := RetainImportMetadata(m, salvage[0]); err != nil {
		_, detachErr := m.Detach()
		if detachErr != nil {
			return nil, salvage, detachErr
		}
		return nil, salvage, err
	}
	return m, nil, nil
}

func flowResourceRef(id ID) string { return "flow." + hex.EncodeToString(id[:]) }

// References have a single canonical spelling. Rejecting aliases prevents
// two records from claiming the same resource through different strings and
// keeps private addresses out of the public resource namespace.
// origin: hot-update S5 plan §5.1
func validStateRef(ref string) bool {
	if len(ref) != len("flow-state.")+32 || ref[:len("flow-state.")] != "flow-state." {
		return false
	}
	b, err := hex.DecodeString(ref[len("flow-state."):])
	return err == nil && hex.EncodeToString(b) == ref[len("flow-state."):] && !bytes.Equal(b, make([]byte, 16))
}

// Public validation runs before Read/Take: malformed bookkeeping must not
// consume a descriptor the bootstrap owner still needs for redispatch.
// Domain reservation is full log capacity, including logs with Base == End;
// another domain's shared Used value cannot establish this image's charge.
// origin: hot-update S5 plan §3.4 / §5.3
func validatePublicImage(im Image) error {
	if im.Version != Version || !validStateRef(im.StateRef) || len(im.Records) > MaxFlows+MaxTerminalMetadata || activeImageFlows(im) > MaxFlows || len(im.Records)-activeImageFlows(im) > MaxTerminalMetadata || len(im.Retirement) > 128 || im.ReservedBytes < 0 {
		return sealedstate.ErrInvalid
	}
	ids := make(map[ID]bool)
	var reserved int64
	for _, r := range im.Records {
		if r.ID == (ID{}) || ids[r.ID] || r.OpenSeq == 0 || r.Phase < FlowPhaseOpening || r.Phase > FlowPhaseTerminal ||
			r.TxBase > r.TxEnd || r.TxEnd-r.TxBase > uint64(workload.DefaultReplayCapacity) || r.RxApplied > r.RxReceived || r.RxReceived-r.RxApplied > MaxDataBytes ||
			(r.RawRef != "" && r.RawRef != flowResourceRef(r.ID)) || (r.ReplayRef != "" && r.ReplayRef != flowResourceRef(r.ID)) {
			return sealedstate.ErrInvalid
		}
		ids[r.ID] = true
		if r.ReplayRef != "" {
			reserved += workload.DefaultReplayCapacity
		}
	}
	if reserved != im.ReservedBytes {
		return sealedstate.ErrInvalid
	}
	endpoints := make(map[EndpointID]bool)
	for _, r := range im.Retirement {
		if r.EndpointID == (EndpointID{}) || endpoints[r.EndpointID] || !validRetirement(r) {
			return sealedstate.ErrInvalid
		}
		endpoints[r.EndpointID] = true
	}
	return nil
}

// Active slots exclude retained terminal receipts, including at custom limits.
// origin: hot-update S5 internal review rv-image-2
func activeImageFlows(im Image) int {
	active := 0
	for _, record := range im.Records {
		if record.Phase != FlowPhaseTerminal {
			active++
		}
	}
	return active
}

// Holes are retired sequences above the contiguous watermark, strictly sorted
// and below the next assignable sequence. Accepting duplicate/unordered holes
// would permit runtime canonicalization to silently drop retirement evidence.
// origin: hot-update S5 plan §3.2 / T31
func validRetirement(r RetirementImage) bool {
	if r.NextOpenSeq == 0 || r.RetiredThrough >= r.NextOpenSeq || len(r.Holes) > MaxRetirementHoles {
		return false
	}
	prev := r.RetiredThrough
	for _, h := range r.Holes {
		if h <= prev || h >= r.NextOpenSeq {
			return false
		}
		prev = h
	}
	return true
}

// The descriptor table is the only schema. All fields, including empty pending
// buffers and false receipt decisions, are required and copied into the codec.
// origin: hot-update S5 plan §12.1 / T31
func checkedFlowRecords(records []sealedstate.Record) ([]sealedstate.Record, error) {
	if len(records) > MaxFlows+MaxTerminalMetadata+128 {
		return nil, sealedstate.ErrCapacity
	}
	out := make([]sealedstate.Record, 0, len(records))
	seen := make(map[string]bool)
	counts := make(map[string]int)
	active, terminal := 0, 0
	for _, rec := range records {
		if rec.Kind != sealedstate.KindFlow && rec.Kind != sealedstate.KindRetirement {
			return nil, sealedstate.ErrInvalid
		}
		var desc *sealedstate.RecordDescriptor
		for i := range sealedstate.RecordDescriptors {
			if sealedstate.RecordDescriptors[i].Kind == rec.Kind {
				desc = &sealedstate.RecordDescriptors[i]
				break
			}
		}
		if desc == nil || len(rec.Fields) != len(desc.Fields) {
			return nil, sealedstate.ErrInvalid
		}
		fields := make(map[string]sealedstate.Field, len(rec.Fields))
		for _, field := range rec.Fields {
			if _, exists := fields[field.Name]; exists {
				return nil, sealedstate.ErrInvalid
			}
			fields[field.Name] = field
		}
		copyRec := sealedstate.Record{Kind: rec.Kind}
		for _, fd := range desc.Fields {
			field, ok := fields[fd.Name]
			if !ok || field.Type != fd.Type || !validPrivateField(field, fd) {
				return nil, sealedstate.ErrInvalid
			}
			field.Bytes = bytes.Clone(field.Bytes)
			copyRec.Fields = append(copyRec.Fields, field)
		}
		id := fields["id"].Bytes
		key := rec.Kind + string(id)
		if bytes.Equal(id, make([]byte, 16)) || seen[key] {
			return nil, sealedstate.ErrInvalid
		}
		seen[key] = true
		counts[rec.Kind]++
		// Terminal receipts preserve retirement proof without reserving an active slot.
		// origin: hot-update S5 internal review rv-image-2
		if rec.Kind == sealedstate.KindFlow {
			if fields["phase"].U64 == uint64(FlowPhaseTerminal) {
				terminal++
			} else {
				active++
			}
		}
		if active > MaxFlows || terminal > MaxTerminalMetadata || counts[sealedstate.KindRetirement] > 128 {
			return nil, sealedstate.ErrCapacity
		}
		out = append(out, copyRec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return bytes.Compare(recordFields(out[i])["id"].Bytes, recordFields(out[j])["id"].Bytes) < 0
	})
	return out, nil
}

// Fixed-width primitives use Width rather than MaxBytes (which is zero for
// IDs/nonces/salts). Numeric and byte representations are mutually exclusive;
// accepting a second representation would hide state the codec cannot retain.
// origin: hot-update S5 plan §12.1
func validPrivateField(f sealedstate.Field, d sealedstate.FieldDescriptor) bool {
	if f.Type <= sealedstate.FieldBool {
		return len(f.Bytes) == 0 && (f.Type != sealedstate.FieldBool || f.U64 <= 1) && (f.Type != sealedstate.FieldNonnegativeI64 || f.U64 <= math.MaxInt64)
	}
	return f.U64 == 0 && (d.Width == 0 || len(f.Bytes) == d.Width) && (d.Width != 0 || len(f.Bytes) <= d.MaxBytes) && (f.Type != sealedstate.FieldUTF8 || utf8.Valid(f.Bytes))
}
func recordFields(r sealedstate.Record) map[string]sealedstate.Field {
	fields := make(map[string]sealedstate.Field, len(r.Fields))
	for _, f := range r.Fields {
		fields[f.Name] = f
	}
	return fields
}

func imageFromRecords(records []sealedstate.Record) (Image, error) {
	checked, err := checkedFlowRecords(records)
	if err != nil {
		return Image{}, err
	}
	im := Image{Version: Version}
	for _, record := range checked {
		f := recordFields(record)
		if record.Kind == sealedstate.KindRetirement {
			r := RetirementImage{EndpointID: EndpointID(f["endpoint_id"].Bytes), NextOpenSeq: f["next_open_seq"].U64, RetiredThrough: f["retired_through"].U64}
			if len(f["holes"].Bytes)%8 != 0 {
				return Image{}, sealedstate.ErrInvalid
			}
			for b := f["holes"].Bytes; len(b) > 0; b = b[8:] {
				r.Holes = append(r.Holes, binary.BigEndian.Uint64(b[:8]))
			}
			if !validRetirement(r) {
				return Image{}, sealedstate.ErrInvalid
			}
			im.Retirement = append(im.Retirement, r)
			continue
		}
		// Narrowing before validation would accept e.g. phase=257 as Opening.
		if f["phase"].U64 < uint64(FlowPhaseOpening) || f["phase"].U64 > uint64(FlowPhaseTerminal) || f["fin_flags"].U64 > math.MaxUint8 ||
			f["close_mode"].U64 > uint64(CloseModeRevoke) || f["close_reason"].U64 > uint64(ReasonStateInvalid) || f["open_reason"].U64 > uint64(ReasonStateInvalid) ||
			f["connect_state"].U64 > uint64(rawsocket.ConnectFailed) || f["target_port"].U64 > math.MaxUint16 || f["raw_type"].U64 > math.MaxUint32 {
			return Image{}, sealedstate.ErrInvalid
		}
		r := RecordImage{ID: ID(f["id"].Bytes), OpenSeq: f["open_seq"].U64, Intent: [32]byte(f["intent"].Bytes), Phase: Phase(f["phase"].U64),
			TxBase: f["tx_base"].U64, TxEnd: f["tx_end"].U64, RxApplied: f["rx_applied"].U64, RxReceived: f["rx_received"].U64,
			TxFIN: f["tx_fin"].U64, RxFIN: f["rx_fin"].U64, FINFlags: uint8(f["fin_flags"].U64), RawRef: string(f["raw_ref"].Bytes), ReplayRef: string(f["replay_ref"].Bytes)}
		// [Applied,Received) must carry exactly that suffix, including after a
		// successful short Write followed by an error. No progress is fabricated.
		// origin: hot-update S5 plan §3.1 / T31
		if r.RxApplied > r.RxReceived || r.RxReceived-r.RxApplied != uint64(len(f["rx_pending"].Bytes)) ||
			(f["tx_source_ended"].U64 != 0 && r.TxFIN != r.TxEnd) || (f["rx_close_write"].U64 != 0 && (r.RxApplied != r.RxFIN || r.RxReceived != r.RxFIN)) {
			return Image{}, sealedstate.ErrInvalid
		}
		im.Records = append(im.Records, r)
		if r.ReplayRef != "" {
			im.ReservedBytes += workload.DefaultReplayCapacity
		}
	}
	// StateRef is generated after projection. The same validation is applied to
	// both public inputs and private-derived metadata, using a legal dummy ID.
	im.StateRef = "flow-state.00000000000000000000000000000001"
	if err := validatePublicImage(im); err != nil {
		return Image{}, err
	}
	im.StateRef = ""
	return im, nil
}

func equalFlowImages(a, b Image) bool {
	if a.Version != b.Version || a.StateRef != b.StateRef || a.ReservedBytes != b.ReservedBytes || len(a.Records) != len(b.Records) || len(a.Retirement) != len(b.Retirement) {
		return false
	}
	// Manifest ordering has no semantic meaning. Canonical projection is sorted
	// by ID; compare keyed values so a JSON round trip cannot change ownership.
	ar, br := append([]RecordImage(nil), a.Records...), append([]RecordImage(nil), b.Records...)
	sort.Slice(ar, func(i, j int) bool { return bytes.Compare(ar[i].ID[:], ar[j].ID[:]) < 0 })
	sort.Slice(br, func(i, j int) bool { return bytes.Compare(br[i].ID[:], br[j].ID[:]) < 0 })
	for i := range ar {
		if ar[i] != br[i] {
			return false
		}
	}
	for _, x := range a.Retirement {
		found := false
		for _, y := range b.Retirement {
			if x.EndpointID == y.EndpointID {
				found = x.NextOpenSeq == y.NextOpenSeq && x.RetiredThrough == y.RetiredThrough && slices.Equal(x.Holes, y.Holes)
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// A ref names one resource with separate raw/replay files. Exact closure keeps
// unexpected descriptors out of a cut and rejects public copies of private
// state. The original files remain borrowed until the runtime hook takes over.
// origin: hot-update S5 plan §5.1 / §5.3
func validateCutResources(im Image, resources []handoff.Resource) error {
	expected := make(map[string]map[string]bool)
	for _, r := range im.Records {
		if r.RawRef != "" {
			if expected[r.RawRef] == nil {
				expected[r.RawRef] = make(map[string]bool)
			}
			expected[r.RawRef]["raw"] = true
		}
		if r.ReplayRef != "" {
			if expected[r.ReplayRef] == nil {
				expected[r.ReplayRef] = make(map[string]bool)
			}
			expected[r.ReplayRef]["replay"] = true
		}
	}
	if len(resources) != len(expected) {
		return sealedstate.ErrInvalid
	}
	seen := make(map[string]bool)
	files := make(map[*os.File]bool)
	for _, res := range resources {
		names, ok := expected[res.ID]
		// Data is empty: pending buffers/tuples/receipts are already in sealed state.
		if !ok || seen[res.ID] || res.Kind != "flow-v1" || len(res.Data) != 0 || len(res.Files) != len(names) {
			return sealedstate.ErrInvalid
		}
		seen[res.ID] = true
		for name, file := range res.Files {
			if !names[name] || file == nil || files[file] {
				return sealedstate.ErrInvalid
			}
			files[file] = true
		}
	}
	return nil
}

// FileSource may return both a file and an error after transferring ownership.
// Both ordinary successes and rejected transfers must survive a later failure;
// the source retains every file this loop has not requested yet.
// origin: hot-update S5 plan §5.3
func takeFlowResources(im Image, records []sealedstate.Record, src rawsocket.FileSource) ([]handoff.Resource, error) {
	fields := make(map[ID]map[string]sealedstate.Field)
	for _, r := range records {
		if r.Kind == sealedstate.KindFlow {
			f := recordFields(r)
			fields[ID(f["id"].Bytes)] = f
		}
	}
	var resources []handoff.Resource
	for _, r := range im.Records {
		if r.RawRef == "" && r.ReplayRef == "" {
			continue
		}
		res := handoff.Resource{ID: flowResourceRef(r.ID), Kind: "flow-v1", Files: make(map[string]*os.File)}
		resources = append(resources, res)
		for _, name := range []string{"raw", "replay"} {
			if (name == "raw" && r.RawRef == "") || (name == "replay" && r.ReplayRef == "") {
				continue
			}
			var file *os.File
			var err error
			if name == "raw" {
				f := fields[r.ID]
				file, err = rawsocket.TakeImageFile(src, res.ID, name, rawsocket.Fingerprint{Dev: f["raw_dev"].U64, Ino: f["raw_ino"].U64, Type: uint32(f["raw_type"].U64)})
			} else {
				file, err = src.TakeFile(res.ID, name)
			}
			if file != nil {
				res.Files[name] = file
			}
			if err != nil {
				return resources, err
			}
			if file == nil {
				return resources, sealedstate.ErrInvalid
			}
		}
	}
	return resources, nil
}

// Private wire: TFCS, u16 version, u32 record count; each record uses u16 kind
// length/kind/u16 field count, then u16 name length/name/u8 type/u32 value
// length/value. Numeric values are big endian. Descriptor names are wire IDs.
// Lengths are checked against the remaining input before any allocation.
// origin: hot-update S5 plan §12.1
func encodeFlowState(records []sealedstate.Record) ([]byte, error) {
	records, err := checkedFlowRecords(records)
	if err != nil {
		return nil, err
	}
	if _, err := imageFromRecords(records); err != nil {
		return nil, err
	}
	var b bytes.Buffer
	b.WriteString("TFCS")
	_ = binary.Write(&b, binary.BigEndian, uint16(Version))
	_ = binary.Write(&b, binary.BigEndian, uint32(len(records)))
	writeName := func(s string) { _ = binary.Write(&b, binary.BigEndian, uint16(len(s))); b.WriteString(s) }
	for _, rec := range records {
		writeName(rec.Kind)
		_ = binary.Write(&b, binary.BigEndian, uint16(len(rec.Fields)))
		for _, f := range rec.Fields {
			writeName(f.Name)
			b.WriteByte(f.Type)
			value := f.Bytes
			if f.Type <= sealedstate.FieldBool {
				value = make([]byte, 8)
				binary.BigEndian.PutUint64(value, f.U64)
				if f.Type == sealedstate.FieldBool {
					value = value[7:]
				}
			}
			if len(value) > sealedstate.MaxStateBytes-b.Len()-4 {
				return nil, sealedstate.ErrCapacity
			}
			_ = binary.Write(&b, binary.BigEndian, uint32(len(value)))
			b.Write(value)
		}
	}
	return b.Bytes(), nil
}

func decodeFlowState(data []byte) ([]sealedstate.Record, error) {
	if len(data) > sealedstate.MaxStateBytes {
		return nil, sealedstate.ErrCapacity
	}
	if len(data) < 10 || string(data[:4]) != "TFCS" || binary.BigEndian.Uint16(data[4:6]) != Version {
		return nil, sealedstate.ErrInvalid
	}
	count := binary.BigEndian.Uint32(data[6:10])
	data = data[10:]
	if count > MaxFlows+MaxTerminalMetadata+128 {
		return nil, sealedstate.ErrCapacity
	}
	take := func(n int) ([]byte, error) {
		if n < 0 || n > len(data) {
			return nil, sealedstate.ErrInvalid
		}
		b := data[:n]
		data = data[n:]
		return b, nil
	}
	readName := func() (string, error) {
		b, e := take(2)
		if e != nil {
			return "", e
		}
		b, e = take(int(binary.BigEndian.Uint16(b)))
		return string(b), e
	}
	var records []sealedstate.Record
	for range count {
		kind, err := readName()
		if err != nil {
			return nil, err
		}
		b, err := take(2)
		if err != nil {
			return nil, err
		}
		fields := int(binary.BigEndian.Uint16(b))
		if fields > 64 {
			return nil, sealedstate.ErrInvalid
		}
		rec := sealedstate.Record{Kind: kind}
		for range fields {
			name, err := readName()
			if err != nil {
				return nil, err
			}
			b, err := take(5)
			if err != nil {
				return nil, err
			}
			typ, size := b[0], binary.BigEndian.Uint32(b[1:])
			if uint64(size) > uint64(len(data)) {
				return nil, sealedstate.ErrInvalid
			}
			value, err := take(int(size))
			if err != nil {
				return nil, err
			}
			f := sealedstate.Field{Name: name, Type: typ}
			if typ <= sealedstate.FieldBool {
				width := 8
				if typ == sealedstate.FieldBool {
					width = 1
				}
				if len(value) != width {
					return nil, sealedstate.ErrInvalid
				}
				if width == 1 {
					f.U64 = uint64(value[0])
				} else {
					f.U64 = binary.BigEndian.Uint64(value)
				}
			} else {
				f.Bytes = bytes.Clone(value)
			}
			rec.Fields = append(rec.Fields, f)
		}
		records = append(records, rec)
	}
	if len(data) != 0 {
		return nil, sealedstate.ErrInvalid
	}
	checked, err := checkedFlowRecords(records)
	if err != nil {
		return nil, err
	}
	if _, err := imageFromRecords(checked); err != nil {
		return nil, err
	}
	return checked, nil
}
