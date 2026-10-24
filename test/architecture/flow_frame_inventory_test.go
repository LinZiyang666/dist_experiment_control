package architecture

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/flow"
)

// The binary ledger is separate from proto's JSON inventory. Golden bytes are
// assembled from the frozen layout, never by the encoder they judge.
// origin: hot-update S5 plan §2.2 / §10.1 / T49.
// gate-control: TestFlowFrameInventoryControls
func TestFlowFrameInventoryMatchesCodec(t *testing.T) {
	if err := checkFlowFrameShape(); err != nil {
		t.Fatal(err)
	}
	t.Run("ledger", func(t *testing.T) {
		assertBinaryInventory(t, "testdata/flow_frame_inventory.txt", flowFrameInventory())
	})
	for _, f := range flowFrameVectors() {
		t.Run(fmt.Sprintf("kind-%d-flags-%d-mode-%d", f.Kind, f.Flags, resumeVectorMode(f)), func(t *testing.T) {
			golden := flowFrameGolden(f)
			var out bytes.Buffer
			if err := flow.EncodeFrame(&out, f); err != nil {
				t.Fatalf("real frame encoder required (pending P2): %v", err)
			}
			if !bytes.Equal(out.Bytes(), golden) {
				t.Fatal("encoder differs from independent header/body vector")
			}
			got, err := flow.DecodeFrame(bytes.NewReader(golden))
			if err != nil {
				t.Fatal(err)
			}
			gotBody := got.Body
			got.Body = f.Body
			if !reflect.DeepEqual(got, f) || !bytes.Equal(gotBody, f.Body) {
				t.Fatal("decoder lost header/body state")
			}
		})
	}
}

func flowFrameInventory() []string {
	rows := []string{
		"header.Version\t0\tu8\t1", "header.Kind\t1\tu8\t1", "header.Flags\t2\tu16be\t2",
		"header.BodyLen\t4\tu32be\t4", "header.FlowID\t8\tid16\t16", "header.CarrierGen\t24\tu64be\t8", "header.Offset\t32\tu64be\t8",
		"body.OPEN\t74", "body.OPEN_ACK\t75", "body.RESUME\tprefix=106+ticket+2+entries*106", "body.RESUME_OK\trequest+owner+ticket+retirement+entries+authority",
		"body.REJECT\t18", "body.DATA\t1..32768", "body.ACK\t0", "body.FIN\t0", "body.CLOSE\t35",
		"flag.direction\t1\tall", "flag.fin-receipt\t2\tACK", "flag.close-receipt\t4\tCLOSE",
	}
	for i, name := range []string{"OPEN", "OPEN_ACK", "RESUME", "RESUME_OK", "REJECT", "DATA", "ACK", "FIN", "CLOSE"} {
		rows = append(rows, fmt.Sprintf("kind.%s\t%d", name, i+1))
	}
	for i, name := range []string{"initial", "reconnect", "prepare", "activate", "abort", "bind"} {
		rows = append(rows, fmt.Sprintf("resume.mode.%s\t%d", name, i+1))
	}
	rows = append(rows, "resume.scope.endpoint\t0", "resume.scope.flow\t1", "version\t1", "header.bytes\t40", "body.max\t32768")

	// Fixed body prefixes have their own offsets. Variable sections retain
	// symbolic offsets so adding a ticket never renumbers an existing field ID.
	for _, layout := range []struct {
		kind   string
		names  []string
		widths []int
		types  []string
	}{
		{"OPEN", []string{"EndpointID", "OpenSeq", "Intent", "BindNonce", "TargetPort"}, []int{16, 8, 32, 16, 2}, []string{"id16", "u64be", "digest32", "id16", "u16be"}},
		{"OPEN_ACK", []string{"EndpointID", "OpenSeq", "Intent", "BindNonce", "Status", "Reason"}, []int{16, 8, 32, 16, 1, 2}, []string{"id16", "u64be", "digest32", "id16", "u8", "u16be"}},
		{"RESUME", []string{"Scope", "Mode", "PageIndex", "PageCount", "RequestID", "EndpointID", "OwnerGen", "NextOwnerGen", "CutID", "ImageDigest", "TargetPort", "TicketLen"}, []int{1, 1, 2, 2, 16, 16, 8, 8, 16, 32, 2, 2}, []string{"u8", "u8", "u16be", "u16be", "id16", "id16", "u64be", "u64be", "id16", "digest32", "u16be", "u16be"}},
		{"RESUME_OK", []string{"RequestID", "EndpointID", "OwnerGen", "CarrierGen", "ImageDigest", "GrantDigest", "Verdict", "Reason", "TicketLen"}, []int{16, 16, 8, 8, 32, 32, 1, 2, 2}, []string{"id16", "id16", "u64be", "u64be", "digest32", "digest32", "u8", "u16be", "u16be"}},
		{"REJECT", []string{"RequestID", "Reason"}, []int{16, 2}, []string{"id16", "u16be"}},
		{"CLOSE", []string{"Mode", "Reason", "FinalEnd0", "FinalEnd1", "Applied0", "Applied1"}, []int{1, 2, 8, 8, 8, 8}, []string{"u8", "u16be", "u64be", "u64be", "u64be", "u64be"}},
		{"resume-entry", []string{"FlowID", "OpenSeq", "Intent", "TxBase", "TxEnd", "RxApplied", "RxReceived", "TxFIN", "RxFIN", "FINFlags", "OpenStatus"}, []int{16, 8, 32, 8, 8, 8, 8, 8, 8, 1, 1}, []string{"id16", "u64be", "digest32", "u64be", "u64be", "u64be", "u64be", "u64be", "u64be", "u8", "u8"}},
		{"authority", []string{"Enabled", "Generation", "ProxyEpoch", "AppliedRevision", "KeyCount"}, []int{1, 8, 8, 8, 2}, []string{"u8", "nonnegative-i64be", "nonnegative-i64be", "u64be", "u16be"}},
	} {
		offset := 0
		for i, name := range layout.names {
			rows = append(rows, fmt.Sprintf("body.%s.%s\t%d\t%s\t%d", layout.kind, name, offset, layout.types[i], layout.widths[i]))
			offset += layout.widths[i]
		}
	}
	rows = append(rows,
		"body.RESUME.Ticket\t106\tbytes<=512\tTicketLen", "body.RESUME.EntryCount\t106+TicketLen\tu16be\t2", "body.RESUME.Entries\t108+TicketLen\tresume-entry\tEntryCount*106",
		"body.RESUME_OK.Ticket\t117\tbytes<=512\tTicketLen", "body.RESUME_OK.PageIndex\t117+TicketLen\tu16be\t2", "body.RESUME_OK.PageCount\t119+TicketLen\tu16be\t2", "body.RESUME_OK.NextOpenSeq\t121+TicketLen\tu64be\t8", "body.RESUME_OK.RetiredThrough\t129+TicketLen\tu64be\t8", "body.RESUME_OK.HoleCount\t137+TicketLen\tu16be\t2", "body.RESUME_OK.Holes\t139+TicketLen\tu64be[]\tHoleCount*8", "body.RESUME_OK.EntryCount\t139+TicketLen+HoleCount*8\tu16be\t2", "body.RESUME_OK.Entries\t141+TicketLen+HoleCount*8\tresume-entry\tEntryCount*106", "body.RESUME_OK.Authority\t141+TicketLen+HoleCount*8+EntryCount*106\tauthority\tvariable",
		"body.authority.KeyIDLen\t27+previous-keys\tu16be\t2", "body.authority.KeyID\t29+previous-keys\tUTF8<=128\tKeyIDLen", "body.authority.SecretLen\t29+previous-keys+KeyIDLen\tu16be\t2", "body.authority.Secret\t31+previous-keys+KeyIDLen\tbytes<=512\tSecretLen",
		"resume.pages.max\t64", "resume.aggregate.max\t2097152", "resume.ticket.max\t512", "authority.key-id.max\t128", "authority.secret.max\t512")
	for i, name := range []string{"malformed", "offset_gap", "offset_expired", "intent_conflict", "unknown_flow", "retired", "revoked", "stale_epoch", "owner_conflict", "capacity", "unauthorized", "deadline", "unsupported", "io_failure", "nonce_exhausted", "state_invalid"} {
		rows = append(rows, fmt.Sprintf("reason.%s\t%d", name, i+1))
	}
	sort.Strings(rows)
	return rows
}

// Reject both missing and stale rows. An updater may append schema, but may not
// silently rewrite a field ID to bless incompatible bytes from an old carrier.
func assertBinaryInventory(t *testing.T, path string, current []string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("binary inventory pending P27: %s: %v; required rows:\n%s", path, err, strings.Join(current, "\n"))
	}
	var rows []string
	for _, row := range strings.Split(string(raw), "\n") {
		if row != "" && !strings.HasPrefix(row, "#") {
			rows = append(rows, row)
		}
	}
	if err := compareInventoryRows(rows, current); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

func compareInventoryRows(ledger, current []string) error {
	a, b := slices.Clone(ledger), slices.Clone(current)
	sort.Strings(a)
	sort.Strings(b)
	if len(a) == 0 || !slices.Equal(a, b) {
		return fmt.Errorf("inventory field deletion, addition, tag/type/number drift or duplicate row")
	}
	return nil
}

func checkFlowFrameShape() error {
	typ := reflect.TypeOf(flow.Frame{})
	expected := []struct {
		name string
		typ  reflect.Type
	}{
		{"Version", reflect.TypeOf(uint8(0))}, {"Kind", reflect.TypeOf(flow.Kind(0))}, {"Flags", reflect.TypeOf(uint16(0))},
		{"BodyLen", reflect.TypeOf(uint32(0))}, {"FlowID", reflect.TypeOf(flow.ID{})}, {"CarrierGen", reflect.TypeOf(uint64(0))}, {"Offset", reflect.TypeOf(uint64(0))}, {"Body", reflect.TypeOf([]byte{})},
	}
	if typ.NumField() != len(expected) {
		return fmt.Errorf("frame fields drifted")
	}
	for i, field := range expected {
		got := typ.Field(i)
		if got.Name != field.name || got.Type != field.typ {
			return fmt.Errorf("frame field %s type/order drifted", field.name)
		}
	}
	if flow.Version != 1 || flow.HeaderBytes != 40 || flow.MaxDataBytes != 32768 || flow.MaxControlBytes != 32768 {
		return fmt.Errorf("frame bounds drifted")
	}
	kinds := []flow.Kind{flow.KindOpen, flow.KindOpenACK, flow.KindResume, flow.KindResumeOK, flow.KindReject, flow.KindData, flow.KindACK, flow.KindFIN, flow.KindClose}
	for i, kind := range kinds {
		if kind != flow.Kind(i+1) {
			return fmt.Errorf("frame kind %d wire number drifted", i+1)
		}
	}
	if flow.FlagDirection != 1 || flow.FlagFINReceipt != 2 || flow.FlagCloseReceipt != 4 {
		return fmt.Errorf("frame flags drifted")
	}
	modes := []flow.ResumeMode{flow.ResumeModeInitial, flow.ResumeModeReconnect, flow.ResumeModePrepare, flow.ResumeModeActivate, flow.ResumeModeAbort, flow.ResumeModeBind}
	for i, mode := range modes {
		if mode != flow.ResumeMode(i+1) {
			return fmt.Errorf("resume mode wire number drifted")
		}
	}

	reasons := []flow.Reason{flow.ReasonMalformed, flow.ReasonOffsetGap, flow.ReasonOffsetExpired, flow.ReasonIntentConflict, flow.ReasonUnknownFlow, flow.ReasonRetired, flow.ReasonRevoked, flow.ReasonStaleEpoch, flow.ReasonOwnerConflict, flow.ReasonCapacity, flow.ReasonUnauthorized, flow.ReasonDeadline, flow.ReasonUnsupported, flow.ReasonIOFailure, flow.ReasonNonceExhausted, flow.ReasonStateInvalid}
	for i, reason := range reasons {
		if reason != flow.Reason(i+1) {
			return fmt.Errorf("frame reason wire number drifted")
		}
	}
	if flow.MaxTicketBytes != 512 || flow.MaxResumePages != 64 || flow.MaxResumeBytes != 2<<20 || flow.MaxKeyIDBytes != 128 || flow.MaxKeySecretBytes != 512 {
		return fmt.Errorf("resume/key bounds drifted")
	}
	if flow.ResumeScopeEndpoint != 0 || flow.ResumeScopeFlow != 1 {
		return fmt.Errorf("resume scope drifted")
	}
	return nil
}

func flowFrameGolden(f flow.Frame) []byte {
	b := make([]byte, 40+len(f.Body))
	b[0], b[1] = f.Version, byte(f.Kind)
	binary.BigEndian.PutUint16(b[2:4], f.Flags)
	binary.BigEndian.PutUint32(b[4:8], f.BodyLen)
	copy(b[8:24], f.FlowID[:])
	binary.BigEndian.PutUint64(b[24:32], f.CarrierGen)
	binary.BigEndian.PutUint64(b[32:40], f.Offset)
	copy(b[40:], f.Body)
	return b
}

func flowFrameVectors() []flow.Frame {
	var vectors []flow.Frame
	add := func(kind flow.Kind, flags uint16, body []byte) {
		f := flow.Frame{Version: 1, Kind: kind, Flags: flags, BodyLen: uint32(len(body)), CarrierGen: 7, Body: body}
		if kind != flow.KindResume && kind != flow.KindResumeOK && kind != flow.KindReject {
			f.FlowID[0] = 3
		}
		if kind == flow.KindData || kind == flow.KindACK || kind == flow.KindFIN {
			f.Offset = 11
		}
		vectors = append(vectors, f)
	}
	for _, direction := range []uint16{0, 1} {
		open := make([]byte, 74)
		open[0] = 1
		binary.BigEndian.PutUint64(open[16:24], 1)
		intent := append([]byte("tether-flow-open-v1\n"), open[:16]...)
		id := make([]byte, 16)
		id[0] = 3
		intent = append(intent, id...)
		intent = append(intent, open[16:24]...)
		intent = append(intent, 0, 80)
		digest := sha256.Sum256(intent)
		copy(open[24:56], digest[:])
		open[56] = 2
		binary.BigEndian.PutUint16(open[72:74], 80)
		add(flow.KindOpen, direction, open)
		ack := append(slices.Clone(open[:72]), 1, 0, 0)
		add(flow.KindOpenACK, direction, ack)
		for _, status := range []byte{2, 3} {
			other := slices.Clone(ack)
			other[72] = status
			if status == 3 {
				other[74] = 1
			}
			add(flow.KindOpenACK, direction, other)
		}
		reject := make([]byte, 18)
		reject[0] = 1
		binary.BigEndian.PutUint16(reject[16:], 1)
		add(flow.KindReject, direction, reject)
		add(flow.KindData, direction, []byte{0, 1, 255})
		add(flow.KindACK, direction, nil)
		add(flow.KindACK, direction|2, nil)
		add(flow.KindFIN, direction, nil)
		for mode := byte(0); mode <= 3; mode++ {
			closeBody := make([]byte, 35)
			closeBody[0] = mode
			if mode > 0 {
				closeBody[2] = 14
			}
			add(flow.KindClose, direction, closeBody)
			add(flow.KindClose, direction|4, slices.Clone(closeBody))
		}
		for mode := byte(1); mode <= 6; mode++ {
			b := bytes.Repeat([]byte{0}, 108)
			b[1] = mode
			b[5] = 1
			b[6] = 1
			b[22] = 1
			b[45] = 1
			b[102] = 0
			b[103] = 80
			if mode >= 3 && mode <= 5 {
				b[53] = 2
				b[54] = 3
				b[70] = 4
			}
			if mode == 6 {
				b[0] = 1
				// Flow bind contains the existing journal identity and offsets.
				entry := make([]byte, 106)
				entry[0] = 3
				entry[23] = 1
				copy(entry[24:56], digest[:])
				entry[105] = 2
				b[107] = 1
				b = append(b, entry...)
			}
			add(flow.KindResume, direction, b)
			if mode == 1 {
				vectors[len(vectors)-1].CarrierGen = 0
			}
			if mode == 6 {
				vectors[len(vectors)-1].FlowID[0] = 3
			}
		}
		// Empty accepted page with a complete empty authority snapshot.
		b := make([]byte, 168)
		b[0] = 1
		b[16] = 1
		b[39] = 1
		b[47] = 7
		b[112] = 1
		b[120] = 1
		b[128] = 1
		add(flow.KindResumeOK, direction, b)
	}
	return vectors
}
func resumeVectorMode(f flow.Frame) byte {
	if f.Kind == flow.KindResume {
		return f.Body[1]
	}
	return 0
}

// Controls use the same comparison as the on-disk ledger. Each defect is a
// separate case so a lost header field cannot hide behind a changed kind.
// origin: hot-update S5 plan §10.1 / T49.
func TestFlowFrameInventoryControls(t *testing.T) {
	if err := checkFlowFrameShape(); err != nil {
		t.Fatal(err)
	}
	rows := flowFrameInventory()
	if err := compareInventoryRows(rows, rows); err != nil {
		t.Fatal(err)
	}
	for i, row := range rows {
		t.Run(row, func(t *testing.T) {
			deleted := append(slices.Clone(rows[:i]), rows[i+1:]...)
			if err := compareInventoryRows(rows, deleted); err == nil {
				t.Fatal("deleted binary field accepted")
			}
			drift := slices.Clone(rows)
			drift[i] += "-drift"
			if err := compareInventoryRows(rows, drift); err == nil {
				t.Fatal("binary field number/type drift accepted")
			}
		})
	}
	// Pin the independent serializer as well: it must retain flags and offsets.
	f := flow.Frame{Version: 1, Kind: flow.KindACK, Flags: 3, CarrierGen: 0x0102030405060708, Offset: 0x1112131415161718}
	got := flowFrameGolden(f)
	want := []byte{1, 7, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 17, 18, 19, 20, 21, 22, 23, 24}
	if !bytes.Equal(got, want) {
		t.Fatal("independent header offsets/flags drifted")
	}
}

// Refusal uses the real decoder as well as the real encoder. A ledger can pin
// offsets but cannot certify that untrusted flags, modes or lengths are refused.
// origin: hot-update S5 plan §2.2 / §10.1 / T49.
func TestFlowFrameInventoryRejectsInvalidWire(t *testing.T) {
	f := flow.Frame{Version: 1, Kind: flow.KindData, BodyLen: 1, CarrierGen: 1, Body: []byte{1}}
	f.FlowID[0] = 1
	var encoded bytes.Buffer
	if err := flow.EncodeFrame(&encoded, f); err != nil {
		t.Fatalf("real frame codec required (pending P2): %v", err)
	}
	golden := flowFrameGolden(f)
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"unknown-version", func(b []byte) { b[0] = 2 }},
		{"unknown-kind", func(b []byte) { b[1] = 255 }},
		{"unknown-flag", func(b []byte) { b[2] = 128 }},
		{"fin-receipt-on-data", func(b []byte) { b[3] = 2 }},
		{"close-receipt-on-data", func(b []byte) { b[3] = 4 }},
		{"oversize-body", func(b []byte) { binary.BigEndian.PutUint32(b[4:8], 32769) }},
		{"offset-overflow", func(b []byte) { binary.BigEndian.PutUint64(b[32:40], ^uint64(0)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broken := slices.Clone(golden)
			tc.mutate(broken)
			if _, err := flow.DecodeFrame(bytes.NewReader(broken)); err == nil {
				t.Fatal("invalid wire accepted")
			}
		})
	}
	for _, vector := range flowFrameVectors() {
		if vector.Kind != flow.KindResume {
			continue
		}
		for _, offset := range []int{0, 1} {
			broken := slices.Clone(vector.Body)
			broken[offset] = 255
			mutant := vector
			mutant.Body = broken
			if err := flow.EncodeFrame(&bytes.Buffer{}, mutant); err == nil {
				t.Fatal("unknown RESUME scope/mode encoded")
			}
			if _, err := flow.DecodeFrame(bytes.NewReader(flowFrameGolden(mutant))); err == nil {
				t.Fatal("unknown RESUME scope/mode decoded")
			}
		}
	}
}
