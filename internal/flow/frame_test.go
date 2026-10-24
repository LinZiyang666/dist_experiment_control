package flow

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"os"
	"strings"
	"testing"
)

// Real: pure codec and independently packed binary vectors. No sockets, carriers,
// ownership, or multipart authorization are simulated by these tests.
// origin: hot-update S5 plan §2.2 / T01
func TestFlowFrameGoldenAndMalformed(t *testing.T) {
	goldens := flowFrameGoldens(t)
	kinds := make(map[byte]bool)
	for name, wire := range goldens {
		t.Run(name, func(t *testing.T) {
			want := frameFromGolden(wire)
			var encoded bytes.Buffer
			if err := EncodeFrame(&encoded, want); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded.Bytes(), wire) {
				t.Fatalf("encoded bytes differ from independent golden: got %x want %x", encoded.Bytes(), wire)
			}
			decoded, err := DecodeFrame(bytes.NewReader(wire))
			if err != nil {
				t.Fatal(err)
			}
			assertFrameEqual(t, decoded, want)
		})
		kinds[wire[1]] = true
	}
	if len(kinds) != 9 {
		t.Fatalf("goldens cover %d kinds, want nine", len(kinds))
	}
	// The wire numbers must remain fixed even if all switch cases are renamed.
	for i, kind := range []Kind{KindOpen, KindOpenACK, KindResume, KindResumeOK, KindReject, KindData, KindACK, KindFIN, KindClose} {
		if kind != Kind(i+1) {
			t.Fatalf("kind number %d changed to %d", i+1, kind)
		}
	}
	for name, wire := range malformedFlowFrames(goldens) {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeFrame(bytes.NewReader(wire)); err == nil {
				t.Fatal("decoder accepted malformed frame")
			}
			if len(wire) >= 40 && len(wire) == 40+int(binary.BigEndian.Uint32(wire[4:8])) {
				var dst bytes.Buffer
				if err := EncodeFrame(&dst, frameFromGolden(wire)); err == nil {
					t.Fatal("encoder accepted malformed frame")
				}
				if dst.Len() != 0 {
					t.Fatal("encoder wrote bytes before rejecting malformed frame")
				}
			}
		})
	}
	for name, wire := range goldens {
		for end := 0; end < len(wire); end++ {
			if _, err := DecodeFrame(bytes.NewReader(wire[:end])); err == nil {
				t.Fatalf("%s truncation at %d accepted", name, end)
			}
		}
	}
	// A stream is not an EOF-delimited record: reading the next header early
	// would block a live carrier, or lose its next frame.
	stream := bytes.NewReader(append(bytes.Clone(goldens["data"]), goldens["ack"]...))
	for _, name := range []string{"data", "ack"} {
		got, err := DecodeFrame(stream)
		if err != nil {
			t.Fatal(err)
		}
		assertFrameEqual(t, got, frameFromGolden(goldens[name]))
	}
	if stream.Len() != 0 {
		t.Fatal("decoder left frame bytes unread")
	}
	for _, size := range []int{1, 32768} {
		f := frameFromGolden(goldens["data"])
		f.Body = bytes.Repeat([]byte{0xa5}, size)
		f.BodyLen = uint32(size)
		f.Offset = math.MaxUint64 - uint64(size)
		var dst bytes.Buffer
		if err := EncodeFrame(&dst, f); err != nil {
			t.Fatal(err)
		}
		got, err := DecodeFrame(&dst)
		if err != nil {
			t.Fatal(err)
		}
		assertFrameEqual(t, got, f)
	}

	for _, name := range []string{"open", "open_ack", "resume_1", "resume_6", "resume_ok_1", "reject", "data", "ack", "fin", "close"} {
		f := frameFromGolden(goldens[name])
		f.Flags = 0
		var dst bytes.Buffer
		if err := EncodeFrame(&dst, f); err != nil {
			t.Fatalf("%s without receipt/direction: %v", name, err)
		}
		got, err := DecodeFrame(&dst)
		if err != nil {
			t.Fatal(err)
		}
		assertFrameEqual(t, got, f)
	}
	for mode := byte(0); mode <= 3; mode++ {
		f := frameFromGolden(goldens["close"])
		f.Body[0] = mode
		if mode == 0 {
			clear(f.Body[1:3])
			copy(f.Body[19:35], f.Body[3:19])
		}
		var dst bytes.Buffer
		if err := EncodeFrame(&dst, f); err != nil {
			t.Fatalf("valid CLOSE mode %d: %v", mode, err)
		}
	}
	for reason := uint16(1); reason <= 16; reason++ {
		f := frameFromGolden(goldens["reject"])
		binary.BigEndian.PutUint16(f.Body[16:18], reason)
		var dst bytes.Buffer
		if err := EncodeFrame(&dst, f); err != nil {
			t.Fatalf("valid reason %d: %v", reason, err)
		}
	}
	// Empty pages and maximum tickets are valid, even without flow entries.
	for _, name := range []string{"resume_1", "resume_ok_1"} {
		f := frameFromGolden(goldens[name])
		at := 104
		suffix := []byte{0, 0}
		if name == "resume_ok_1" {
			at = 115
			suffix = make([]byte, 51)
			binary.BigEndian.PutUint16(suffix[2:4], 1)
			binary.BigEndian.PutUint64(suffix[4:12], 1)
		}
		for _, size := range []int{0, 512} {
			f.Body = append(bytes.Clone(goldens[name][40:40+at]), byte(size>>8), byte(size))
			f.Body = append(f.Body, bytes.Repeat([]byte{'t'}, size)...)
			f.Body = append(f.Body, suffix...)
			f.BodyLen = uint32(len(f.Body))
			var dst bytes.Buffer
			if err := EncodeFrame(&dst, f); err != nil {
				t.Fatalf("%s empty page ticket=%d: %v", name, size, err)
			}
			got, err := DecodeFrame(&dst)
			if err != nil {
				t.Fatal(err)
			}
			assertFrameEqual(t, got, f)
		}
	}
	f := frameFromGolden(goldens["data"])
	f.BodyLen--
	var dst bytes.Buffer
	if err := EncodeFrame(&dst, f); err == nil || dst.Len() != 0 {
		t.Fatal("encoder failed to reject BodyLen mismatch before writing")
	}
}

// origin: hot-update S5 plan §2.2 / T01 / T25
func TestFlowFrameRejectsHeaderBeforeBodyRead(t *testing.T) {
	goldens := flowFrameGoldens(t)
	for name, mutate := range map[string]func([]byte){

		"version":          func(p []byte) { p[0] = 2 },
		"open_length":      func(p []byte) { p[1] = 1; binary.BigEndian.PutUint32(p[4:8], 73); clear(p[32:40]) },
		"open_ack_length":  func(p []byte) { p[1] = 2; binary.BigEndian.PutUint32(p[4:8], 74); clear(p[32:40]) },
		"resume_length":    func(p []byte) { p[1] = 3; binary.BigEndian.PutUint32(p[4:8], 107); clear(p[32:40]) },
		"resume_ok_length": func(p []byte) { p[1] = 4; binary.BigEndian.PutUint32(p[4:8], 167); clear(p[32:40]) },
		"reject_length":    func(p []byte) { p[1] = 5; binary.BigEndian.PutUint32(p[4:8], 17); clear(p[32:40]) },
		"ack_length":       func(p []byte) { p[1] = 7; binary.BigEndian.PutUint32(p[4:8], 1) },
		"fin_length":       func(p []byte) { p[1] = 8; binary.BigEndian.PutUint32(p[4:8], 1) },
		"close_length":     func(p []byte) { p[1] = 9; binary.BigEndian.PutUint32(p[4:8], 34); clear(p[32:40]) },
		"zero_flow":        func(p []byte) { clear(p[8:24]) },
		"empty_data":       func(p []byte) { clear(p[4:8]) },
		"unknown_kind":     func(p []byte) { p[1] = 255 },
		"unknown_flags":    func(p []byte) { p[2] = 128 },
		"control_oversize": func(p []byte) { p[1] = 3; binary.BigEndian.PutUint32(p[4:8], 32769); clear(p[32:40]) },
		"oversize":         func(p []byte) { binary.BigEndian.PutUint32(p[4:8], 32769) },
		"length_overflow":  func(p []byte) { binary.BigEndian.PutUint32(p[4:8], math.MaxUint32) },
		"offset_overflow":  func(p []byte) { binary.BigEndian.PutUint64(p[32:40], math.MaxUint64) },
	} {
		t.Run(name, func(t *testing.T) {
			p := bytes.Clone(goldens["data"][:40])
			mutate(p)
			r := &headerOnlyReader{header: bytes.NewReader(p)}
			if _, err := DecodeFrame(r); err == nil {
				t.Fatal("decoder accepted invalid header")
			}
			if r.bodyReads != 0 {
				t.Fatalf("invalid header caused %d body reads", r.bodyReads)
			}
		})
	}
}

// origin: hot-update S5 plan §2.2 / T01
func TestFlowFrameWriterPreservesPartialProgressAndErrors(t *testing.T) {
	wire := flowFrameGoldens(t)["data"]
	f := frameFromGolden(wire)
	w := &frameTestWriter{maximum: 3}
	if err := EncodeFrame(w, f); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(w.Bytes(), wire) {
		t.Fatal("partial writes dropped or duplicated bytes")
	}
	failure := errors.New("fixture write failure")
	for _, tc := range []struct {
		name    string
		maximum int
		fail    error
		want    error
	}{
		{"zero_progress", 0, nil, io.ErrShortWrite},
		{"partial_with_error", 3, failure, failure},
		{"invalid_negative_count", -1, nil, io.ErrShortWrite},
		{"invalid_large_count", 41, nil, io.ErrShortWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &frameTestWriter{maximum: tc.maximum, failure: tc.fail}
			if err := EncodeFrame(w, f); !errors.Is(err, tc.want) {
				t.Fatalf("write error = %v, want %v", err, tc.want)
			}
		})
	}
}

func flowFrameGoldens(t testing.TB) map[string][]byte {
	t.Helper()
	data, err := os.ReadFile("testdata/frame-v1-golden.txt")
	if err != nil {
		t.Fatal(err)
	}
	rows := make(map[string][]byte)
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatal("invalid golden row")
		}
		wire, err := hex.DecodeString(fields[1])
		if err != nil {
			t.Fatal(err)
		}
		if len(wire) < 40 || len(wire) != 40+int(binary.BigEndian.Uint32(wire[4:8])) || rows[fields[0]] != nil {
			t.Fatal("invalid golden shape")
		}
		rows[fields[0]] = wire
	}
	if len(rows) != 16 {
		t.Fatalf("golden rows = %d, want 16", len(rows))
	}
	return rows
}

// Literal offsets and types intentionally do not depend on the production codec.
func frameFromGolden(wire []byte) Frame {
	f := Frame{Version: wire[0], Kind: Kind(wire[1]), Flags: binary.BigEndian.Uint16(wire[2:4]), BodyLen: binary.BigEndian.Uint32(wire[4:8]), CarrierGen: binary.BigEndian.Uint64(wire[24:32]), Offset: binary.BigEndian.Uint64(wire[32:40]), Body: bytes.Clone(wire[40:])}
	copy(f.FlowID[:], wire[8:24])
	return f
}
func assertFrameEqual(t testing.TB, got, want Frame) {
	t.Helper()
	if got.Version != want.Version || got.Kind != want.Kind || got.Flags != want.Flags || got.BodyLen != want.BodyLen || got.FlowID != want.FlowID || got.CarrierGen != want.CarrierGen || got.Offset != want.Offset || !bytes.Equal(got.Body, want.Body) {
		t.Fatal("decoded frame differs from independent header/body fields")
	}
}

type headerOnlyReader struct {
	header    *bytes.Reader
	bodyReads int
}

func (r *headerOnlyReader) Read(p []byte) (int, error) {
	if r.header.Len() == 0 {
		r.bodyReads++
		return 0, errors.New("unexpected body read")
	}
	return r.header.Read(p)
}

type frameTestWriter struct {
	bytes.Buffer
	maximum int
	failure error
}

func (w *frameTestWriter) Write(p []byte) (int, error) {
	n := w.maximum
	if n < 0 || n > len(p) && n > 3 {
		return n, w.failure
	}
	if n > len(p) {
		n = len(p)
	}
	_, _ = w.Buffer.Write(p[:n])
	return n, w.failure
}

func malformedFlowFrames(g map[string][]byte) map[string][]byte {
	rows := make(map[string][]byte)
	patch := func(name, seed string, at int, data ...byte) {
		p := bytes.Clone(g[seed])
		copy(p[at:], data)
		rows[name] = p
	}
	patch("version", "data", 0, 2)
	for _, kind := range []byte{0, 10, 255} {
		patch("kind_"+hex.EncodeToString([]byte{kind}), "data", 1, kind)
	}
	patch("flags_unknown", "data", 2, 0x80, 0)
	patch("fin_receipt_on_data", "data", 3, 2)
	patch("close_receipt_on_ack", "ack", 3, 4)
	patch("fin_receipt_on_close", "close", 3, 2)
	patch("offset_on_control", "open", 39, 1)
	patch("offset_overflow", "data", 32, 255, 255, 255, 255, 255, 255, 255, 255)
	patch("body_over_limit", "data", 4, 0, 0, 0x80, 1)
	patch("body_length_overflow", "data", 4, 255, 255, 255, 255)
	for _, name := range []string{"open", "open_ack", "reject", "close"} {
		p := append(bytes.Clone(g[name]), 0)
		binary.BigEndian.PutUint32(p[4:8], uint32(len(p)-40))
		rows[name+"_trailing"] = p
	}
	for _, name := range []string{"data", "ack", "fin", "close", "open", "open_ack"} {
		p := bytes.Clone(g[name])
		clear(p[8:24])
		rows[name+"_zero_flow"] = p
	}
	patch("open_zero_seq", "open", 56, 0, 0, 0, 0, 0, 0, 0, 0)
	patch("open_zero_port", "open", 112, 0, 0)
	patch("open_ack_status", "open_ack", 112, 4)
	patch("open_ack_success_reason", "open_ack", 113, 0, 7)
	patch("reject_zero_reason", "reject", 56, 0, 0)
	patch("reject_unknown_reason", "reject", 56, 0, 17)
	p := bytes.Clone(g["close"])
	clear(p[40:43])
	rows["close_normal_unapplied_tail"] = p
	patch("close_mode", "close", 40, 4)
	patch("close_zero_error_reason", "close", 41, 0, 0)
	patch("close_applied_past_end", "close", 59, 255, 255, 255, 255, 255, 255, 255, 255)
	patch("resume_scope", "resume_1", 40, 2)
	patch("resume_mode", "resume_1", 41, 7)
	patch("resume_zero_mode", "resume_1", 41, 0)
	patch("resume_endpoint_bind", "resume_1", 41, 6)
	patch("resume_flow_nonbind", "resume_6", 41, 1)
	patch("resume_endpoint_flowid", "resume_1", 8, 1)
	patch("resume_zero_pages", "resume_1", 44, 0, 0)
	patch("resume_pages_over_limit", "resume_1", 44, 0, 65)
	patch("resume_page_index", "resume_1", 42, 0, 1)
	patch("resume_ticket_over_limit", "resume_1", 144, 2, 1)
	patch("resume_entry_count", "resume_1", 149, 255, 255)
	patch("resume_entry_flags", "resume_1", 255, 4)
	patch("resume_entry_status", "resume_1", 256, 4)
	patch("resume_entry_base_past_end", "resume_1", 207, 255, 255, 255, 255, 255, 255, 255, 255)
	patch("resume_entry_pending_overflow", "resume_1", 231, 255, 255, 255, 255, 255, 255, 255, 255)
	patch("resume_entry_fin_mismatch", "resume_1", 239, 0, 0, 0, 0, 0, 0, 0, 10)
	patch("resume_ok_verdict", "resume_ok_1", 152, 4)
	patch("resume_ok_reason", "resume_ok_1", 153, 0, 7)
	patch("resume_ok_generation_negative", "resume_ok_1", 299, 128)
	patch("resume_ok_epoch_negative", "resume_ok_1", 307, 128)
	patch("resume_ok_key_count", "resume_ok_1", 323, 255, 255)
	patch("resume_ok_key_id_utf8", "resume_ok_1", 327, 255)
	for _, name := range []string{"resume_1", "resume_ok_1"} {
		p := append(bytes.Clone(g[name]), 0)
		binary.BigEndian.PutUint32(p[4:8], uint32(len(p)-40))
		rows[name+"_trailing"] = p
	}

	patch("open_ack_zero_seq", "open_ack", 56, 0, 0, 0, 0, 0, 0, 0, 0)
	patch("open_ack_zero_status", "open_ack", 112, 0)
	patch("open_ack_rejected_zero_reason", "open_ack", 112, 3, 0, 0)
	patch("resume_zero_port", "resume_1", 142, 0, 0)
	patch("resume_entry_zero_seq", "resume_1", 167, 0, 0, 0, 0, 0, 0, 0, 0)
	patch("resume_entry_applied_past_received", "resume_1", 223, 0, 0, 0, 0, 0, 0, 0, 7)
	patch("resume_entry_rxfin_before_received", "resume_1", 247, 0, 0, 0, 0, 0, 0, 0, 5)
	patch("resume_entry_zero_status", "resume_1", 256, 0)
	patch("resume_ok_zero_verdict", "resume_ok_1", 152, 0)
	patch("resume_ok_rejected_zero_reason", "resume_ok_3", 153, 0, 0)
	patch("resume_ok_zero_pages", "resume_ok_1", 162, 0, 0)
	patch("resume_ok_page_index", "resume_ok_1", 160, 0, 1)
	patch("resume_ok_zero_nextseq", "resume_ok_1", 164, 0, 0, 0, 0, 0, 0, 0, 0)
	patch("resume_ok_retired_past_nextseq", "resume_ok_1", 172, 0, 0, 0, 0, 0, 0, 0, 10)
	patch("resume_ok_hole_count_over_limit", "resume_ok_1", 180, 4, 1)
	patch("resume_ok_hole_before_watermark", "resume_ok_1", 182, 0, 0, 0, 0, 0, 0, 0, 2)
	patch("resume_ok_hole_past_nextseq", "resume_ok_1", 182, 0, 0, 0, 0, 0, 0, 0, 10)
	patch("resume_ok_enabled", "resume_ok_1", 298, 2)
	// Build complete over-limit fields, rather than relying on truncation to
	// reject them after a broken size guard has already accepted the length.
	replaceSized := func(name, seed string, at, oldSize, newSize int) {
		p := append(bytes.Clone(g[seed][:at]), byte(newSize>>8), byte(newSize))
		p = append(p, bytes.Repeat([]byte{'x'}, newSize)...)
		p = append(p, g[seed][at+2+oldSize:]...)
		binary.BigEndian.PutUint32(p[4:8], uint32(len(p)-40))
		rows[name] = p
	}
	replaceSized("resume_ticket_complete_over_limit", "resume_1", 144, 3, 513)
	replaceSized("resume_ok_ticket_complete_over_limit", "resume_ok_1", 155, 3, 513)
	replaceSized("resume_ok_id_complete_over_limit", "resume_ok_1", 325, 3, 129)
	replaceSized("resume_ok_secret_complete_over_limit", "resume_ok_1", 330, 4, 513)
	replaceSized("resume_ok_empty_id", "resume_ok_1", 325, 3, 0)
	replaceSized("resume_ok_empty_secret", "resume_ok_1", 330, 4, 0)
	for _, field := range []string{"id", "seq"} {
		p := append(bytes.Clone(g["resume_1"]), g["resume_1"][151:257]...)
		binary.BigEndian.PutUint16(p[149:151], 2)
		if field == "id" {
			binary.BigEndian.PutUint64(p[273:281], 8)
		} else {
			p[257] ^= 1
		}
		binary.BigEndian.PutUint32(p[4:8], uint32(len(p)-40))
		rows["resume_duplicate_"+field] = p
	}
	p = bytes.Clone(g["resume_1"])
	clear(p[151:167])
	rows["resume_entry_zero_id"] = p
	p = append(bytes.Clone(g["resume_ok_1"][:190]), g["resume_ok_1"][182:190]...)
	p = append(p, g["resume_ok_1"][190:]...)
	binary.BigEndian.PutUint16(p[180:182], 2)
	binary.BigEndian.PutUint32(p[4:8], uint32(len(p)-40))
	rows["resume_ok_duplicate_hole"] = p
	p = append(bytes.Clone(g["resume_ok_1"]), g["resume_ok_1"][325:]...)
	binary.BigEndian.PutUint16(p[323:325], 2)
	binary.BigEndian.PutUint32(p[4:8], uint32(len(p)-40))
	rows["resume_ok_duplicate_key"] = p

	p = bytes.Clone(g["resume_1"])
	p[40] = 2
	p[8] = 1
	rows["resume_unknown_scope_with_flow"] = p
	// Isolate each offset guard: a wrapped subtraction can look like a small
	// pending window, while a large legitimate window also needs a matching FIN.
	p = bytes.Clone(g["resume_1"])
	binary.BigEndian.PutUint64(p[223:231], math.MaxUint64)
	clear(p[231:239])
	rows["resume_entry_applied_wraps_credit"] = p
	p = bytes.Clone(g["resume_1"])
	binary.BigEndian.PutUint64(p[231:239], 32773)
	binary.BigEndian.PutUint64(p[247:255], 32773)
	rows["resume_entry_pending_complete_over_limit"] = p
	for _, which := range []string{"zero_nextseq", "retired_past_nextseq"} {
		p = append(bytes.Clone(g["resume_ok_1"][:180]), 0, 0)
		p = append(p, g["resume_ok_1"][190:]...)
		if which == "zero_nextseq" {
			clear(p[164:172])
		} else {
			binary.BigEndian.PutUint64(p[172:180], 10)
		}
		binary.BigEndian.PutUint32(p[4:8], uint32(len(p)-40))
		rows["resume_ok_no_holes_"+which] = p
	}
	p = bytes.Clone(g["resume_ok_1"][:180])
	binary.BigEndian.PutUint64(p[164:172], 2000)
	p = append(p, 4, 1)
	for seq := uint64(3); seq < 1028; seq++ {
		p = binary.BigEndian.AppendUint64(p, seq)
	}
	p = append(p, g["resume_ok_1"][190:]...)
	binary.BigEndian.PutUint32(p[4:8], uint32(len(p)-40))
	rows["resume_ok_complete_holes_over_limit"] = p
	p = bytes.Clone(g["resume_ok_1"][:323])
	p = append(p, 1, 1)
	for key := uint16(0); key < 257; key++ {
		id := []byte{'k', byte('a' + key/26), byte('a' + key%26)}
		p = append(p, 0, 3)
		p = append(p, id...)
		p = append(p, 0, 1, 's')
	}
	binary.BigEndian.PutUint32(p[4:8], uint32(len(p)-40))
	rows["resume_ok_complete_keys_over_limit"] = p
	rows["empty_data"] = bytes.Clone(g["data"][:40])
	clear(rows["empty_data"][4:8])
	return rows
}
