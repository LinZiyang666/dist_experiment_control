package flow

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"unicode/utf8"
)

// Malformed errors are deliberately independent of payloads: RESUME_OK can carry
// authority secrets, so neither validation nor transport errors quote wire bytes.
var errMalformedFrame = errors.New("flow: malformed frame")

// EncodeFrame validates the complete frame before writing any bytes. A caller
// cannot accidentally publish a valid header followed by an invalid control body.
// origin: hot-update S5 plan §2.2 / T01
func EncodeFrame(w io.Writer, f Frame) error {
	if err := validateFrameHeader(f); err != nil {
		return err
	}
	if uint64(len(f.Body)) != uint64(f.BodyLen) || !validFrameBody(f) {
		return errMalformedFrame
	}
	var header [HeaderBytes]byte
	header[0], header[1] = f.Version, byte(f.Kind)
	binary.BigEndian.PutUint16(header[2:4], f.Flags)
	binary.BigEndian.PutUint32(header[4:8], f.BodyLen)
	copy(header[8:24], f.FlowID[:])
	binary.BigEndian.PutUint64(header[24:32], f.CarrierGen)
	binary.BigEndian.PutUint64(header[32:40], f.Offset)
	if err := writeFrameBytes(w, header[:]); err != nil {
		return err
	}
	return writeFrameBytes(w, f.Body)
}

// DecodeFrame consumes exactly one frame, leaving the next frame in the stream.
// Body lengths and DATA arithmetic are checked before allocation or body reads;
// a hostile header cannot turn a bounded frame into an unbounded allocation.
// origin: hot-update S5 plan §2.2 / T01 / T25
func DecodeFrame(r io.Reader) (Frame, error) {
	var header [HeaderBytes]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}
	f := Frame{
		Version: header[0], Kind: Kind(header[1]),
		Flags:      binary.BigEndian.Uint16(header[2:4]),
		BodyLen:    binary.BigEndian.Uint32(header[4:8]),
		CarrierGen: binary.BigEndian.Uint64(header[24:32]),
		Offset:     binary.BigEndian.Uint64(header[32:40]),
	}
	copy(f.FlowID[:], header[8:24])
	if err := validateFrameHeader(f); err != nil {
		return Frame{}, err
	}
	if f.BodyLen != 0 {
		f.Body = make([]byte, int(f.BodyLen))
		if _, err := io.ReadFull(r, f.Body); err != nil {
			return Frame{}, err
		}
	}
	if !validFrameBody(f) {
		return Frame{}, errMalformedFrame
	}
	return f, nil
}

func writeFrameBytes(w io.Writer, p []byte) error {
	for len(p) != 0 {
		n, err := w.Write(p)
		if n < 0 || n > len(p) {
			return io.ErrShortWrite
		}
		p = p[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// Header validation is shared by both directions so a local sender cannot emit
// a frame that the remote parser would reject. Fixed sizes are checked here,
// before a body allocation; nested counts are checked against that bounded body.
// origin: hot-update S5 plan §2.2 / T01 / T25
func validateFrameHeader(f Frame) error {
	if f.Version != Version || f.BodyLen > MaxControlBytes {
		return errMalformedFrame
	}
	allowed := FlagDirection
	switch f.Kind {
	case KindOpen:
		if f.BodyLen != 74 {
			return errMalformedFrame
		}
	case KindOpenACK:
		if f.BodyLen != 75 {
			return errMalformedFrame
		}
	case KindResume:
		if f.BodyLen < 108 {
			return errMalformedFrame
		}
	case KindResumeOK:
		if f.BodyLen < 168 {
			return errMalformedFrame
		}
	case KindReject:
		if f.BodyLen != 18 {
			return errMalformedFrame
		}
	case KindData:
		// Subtraction avoids wrap before a caller can dispatch DATA to a socket.
		// origin: hot-update S5 plan §3.1 / T01 / T25
		if f.BodyLen == 0 || f.BodyLen > MaxDataBytes || f.Offset > math.MaxUint64-uint64(f.BodyLen) {
			return errMalformedFrame
		}
	case KindACK:
		allowed |= FlagFINReceipt
		if f.BodyLen != 0 {
			return errMalformedFrame
		}
	case KindFIN:
		if f.BodyLen != 0 {
			return errMalformedFrame
		}
	case KindClose:
		allowed |= FlagCloseReceipt
		if f.BodyLen != 35 {
			return errMalformedFrame
		}
	default:
		return errMalformedFrame
	}
	// Receipt bits are kind-specific, not a global mask shared by all frames.
	// origin: hot-update S5 plan §2.2 / T01
	if f.Flags & ^allowed != 0 {
		return errMalformedFrame
	}
	if f.Kind != KindData && f.Kind != KindACK && f.Kind != KindFIN && f.Offset != 0 {
		return errMalformedFrame
	}
	switch f.Kind {
	case KindOpen, KindOpenACK, KindData, KindACK, KindFIN, KindClose:
		if f.FlowID == (ID{}) {
			return errMalformedFrame
		}
	case KindResume, KindResumeOK, KindReject:
		// Endpoint controls have no flow; flow bind replies may carry its ID.
	default:
		return errMalformedFrame
	}
	return nil
}

func validReason(reason uint16, failed bool) bool {
	return reason <= uint16(ReasonStateInvalid) && (reason != 0) == failed
}

// Body checks describe a single wire record. Socket authority, intent hashes,
// carrier generation, and multi-page atomicity need the owner journal and are
// deliberately left to the dispatcher; the codec never grants ownership.
// origin: hot-update S5 plan §2.2 / §3
func validFrameBody(f Frame) bool {
	b := f.Body
	switch f.Kind {
	case KindOpen:
		return binary.BigEndian.Uint64(b[16:24]) != 0 && binary.BigEndian.Uint16(b[72:74]) != 0
	case KindOpenACK:
		status := b[72]
		return binary.BigEndian.Uint64(b[16:24]) != 0 && status >= 1 && status <= 3 && validReason(binary.BigEndian.Uint16(b[73:75]), status == 3)
	case KindResume:
		return validResumeBody(f)
	case KindResumeOK:
		return validResumeOKBody(b)
	case KindReject:
		return validReason(binary.BigEndian.Uint16(b[16:18]), true)
	case KindData, KindACK, KindFIN:
		return true
	case KindClose:
		// A normal close cannot claim an undelivered tail is settled. Abort and
		// revoke receipts preserve the smaller applied offsets explicitly.
		// origin: hot-update S5 plan §3.3 / T01
		if b[0] == byte(CloseModeNormal) && (binary.BigEndian.Uint64(b[19:27]) != binary.BigEndian.Uint64(b[3:11]) || binary.BigEndian.Uint64(b[27:35]) != binary.BigEndian.Uint64(b[11:19])) {
			return false
		}
		return b[0] <= byte(CloseModeRevoke) && validReason(binary.BigEndian.Uint16(b[1:3]), b[0] != byte(CloseModeNormal)) &&
			binary.BigEndian.Uint64(b[19:27]) <= binary.BigEndian.Uint64(b[3:11]) &&
			binary.BigEndian.Uint64(b[27:35]) <= binary.BigEndian.Uint64(b[11:19])
	default:
		return false
	}
}

// The cursor bounds every variable field before slicing. Counts consume the
// declared body exactly; extra bytes cannot become a second interpretation of a
// ticket, entries, or authority. Aggregate pages/digests belong to the carrier.
// origin: hot-update S5 plan §2.2 / T01 / T25
type frameCursor struct {
	data []byte
	pos  int
	bad  bool
}

func (c *frameCursor) take(n int) []byte {
	if n < 0 || n > len(c.data)-c.pos {
		c.bad = true
		return nil
	}
	p := c.data[c.pos : c.pos+n]
	c.pos += n
	return p
}
func (c *frameCursor) u16() uint16 {
	p := c.take(2)
	if p == nil {
		return 0
	}
	return binary.BigEndian.Uint16(p)
}
func (c *frameCursor) sized(maximum int) []byte {
	n := int(c.u16())
	if c.bad || n > maximum {
		c.bad = true
		return nil
	}
	return c.take(n)
}
func (c *frameCursor) complete() bool { return !c.bad && c.pos == len(c.data) }

func validPages(index, count uint16) bool {
	return count <= MaxResumePages && index < count
}

// The 104-byte fixed prefix ends at TargetPort. TicketLen and EntryCount follow
// it, making the smallest request 108 bytes. No wire cursor depends on native
// struct padding or machine word width.
// origin: hot-update S5 plan §2.2 / T01
func validResumeBody(f Frame) bool {
	b := f.Body
	scope, mode := ResumeScope(b[0]), ResumeMode(b[1])
	if scope != ResumeScopeEndpoint && scope != ResumeScopeFlow {
		return false
	}
	if mode < ResumeModeInitial || mode > ResumeModeBind || !validPages(binary.BigEndian.Uint16(b[2:4]), binary.BigEndian.Uint16(b[4:6])) {
		return false
	}
	// A bind must refer to a flow; endpoint modes cannot smuggle a flow header.
	if (scope == ResumeScopeFlow) != (mode == ResumeModeBind) || (scope == ResumeScopeEndpoint) != (f.FlowID == (ID{})) {
		return false
	}
	if binary.BigEndian.Uint16(b[102:104]) == 0 {
		return false
	}
	c := frameCursor{data: b, pos: 104}
	c.sized(MaxTicketBytes)
	return validResumeEntries(&c) && c.complete()
}

// RESUME_OK always has an authority suffix, including on empty pages. A key
// list is a set of IDs, while different IDs may intentionally alias a secret.
// The codec checks only this page; grant digests bind the complete snapshot.
// origin: hot-update S5 plan §2.2 / §4.3 / T01
func validResumeOKBody(b []byte) bool {
	verdict := ResumeVerdict(b[112])
	if verdict < ResumeVerdictAccepted || verdict > ResumeVerdictRejected || !validReason(binary.BigEndian.Uint16(b[113:115]), verdict == ResumeVerdictRejected) {
		return false
	}
	c := frameCursor{data: b, pos: 115}
	c.sized(MaxTicketBytes)
	index, count := c.u16(), c.u16()
	if c.bad || !validPages(index, count) {
		return false
	}
	watermarks := c.take(16)
	if watermarks == nil || binary.BigEndian.Uint64(watermarks[8:]) >= binary.BigEndian.Uint64(watermarks[:8]) {
		return false
	}
	holes := int(c.u16())
	if c.bad || holes > MaxRetirementHoles {
		return false
	}
	// Holes are retired sequences beyond the contiguous watermark and before
	// the next assignable sequence. Duplicates would miscount bounded history.
	seen := make(map[uint64]bool, holes)
	for i := 0; i < holes; i++ {
		p := c.take(8)
		if p == nil {
			return false
		}
		seq := binary.BigEndian.Uint64(p)
		if seq <= binary.BigEndian.Uint64(watermarks[8:]) || seq >= binary.BigEndian.Uint64(watermarks[:8]) || seen[seq] {
			return false
		}
		seen[seq] = true
	}
	if !validResumeEntries(&c) {
		return false
	}
	authority := c.take(25)
	if authority == nil || authority[0] > 1 || binary.BigEndian.Uint64(authority[1:9]) > math.MaxInt64 || binary.BigEndian.Uint64(authority[9:17]) > math.MaxInt64 {
		return false
	}
	keys := int(c.u16())
	// The node key ceiling also bounds the duplicate-check map before allocation.
	if c.bad || keys > 256 {
		return false
	}
	ids := make(map[string]bool, keys)
	for i := 0; i < keys; i++ {
		id := c.sized(MaxKeyIDBytes)
		if c.bad || len(id) == 0 || !utf8.Valid(id) || ids[string(id)] {
			return false
		}
		ids[string(id)] = true
		secret := c.sized(MaxKeySecretBytes)
		if c.bad || len(secret) == 0 {
			return false
		}
	}
	return c.complete()
}

// The 106-byte entry records TxBase/TxEnd and RxApplied/RxReceived separately:
// conflating accepted bytes with applied bytes would lose the pending suffix.
// FINs may precede DATA, so RxFIN can be greater than RxReceived; TxFIN fixes End.
// origin: hot-update S5 plan §2.2 / §3.1 / §3.3 / T01
func validResumeEntries(c *frameCursor) bool {
	const entryBytes = 106
	count := int(c.u16())
	if c.bad || count > (len(c.data)-c.pos)/entryBytes {
		return false
	}
	ids := make(map[ID]bool, count)
	seqs := make(map[uint64]bool, count)
	for i := 0; i < count; i++ {
		p := c.take(entryBytes)
		if p == nil {
			return false
		}
		var id ID
		copy(id[:], p[:16])
		seq := binary.BigEndian.Uint64(p[16:24])
		base, end := binary.BigEndian.Uint64(p[56:64]), binary.BigEndian.Uint64(p[64:72])
		applied, received := binary.BigEndian.Uint64(p[72:80]), binary.BigEndian.Uint64(p[80:88])
		if id == (ID{}) || seq == 0 || ids[id] || seqs[seq] || base > end || applied > received || received-applied > MaxDataBytes || p[104]&^byte(3) != 0 || p[105] < 1 || p[105] > 3 {
			return false
		}
		if p[104]&1 != 0 && binary.BigEndian.Uint64(p[88:96]) != end {
			return false
		}
		if p[104]&2 != 0 && binary.BigEndian.Uint64(p[96:104]) < received {
			return false
		}
		ids[id], seqs[seq] = true, true
	}
	return !c.bad
}
