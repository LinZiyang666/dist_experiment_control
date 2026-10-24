package flowstate

import (
	"encoding/binary"

	"github.com/LinZiyang666/tether/internal/flow"
)

type ResumeRequest struct {
	Scope       flow.ResumeScope
	Mode        flow.ResumeMode
	RequestID   [16]byte
	Endpoint    flow.EndpointID
	Owner, Next uint64
	Cut         [16]byte
	Digest      [32]byte
	Target      uint16
	Ticket      []byte
	Entries     []flow.State
}

type ResumeResponse struct {
	RequestID           [16]byte
	Endpoint            flow.EndpointID
	Owner, Carrier      uint64
	Digest, GrantDigest [32]byte
	Verdict             flow.ResumeVerdict
	Reason              flow.Reason
	Ticket              []byte
	Page, Pages         uint16
	Retirement          flow.RetirementImage
	Entries             []flow.State
	Authority           Authority
}

// Cursor helpers never allocate from an unvalidated count. Frame.Decode imposes
// the outer 32 KiB bound; these helpers also serve the private image adapter.
type wireCursor struct {
	b   []byte
	bad bool
}

func (c *wireCursor) take(n int) []byte {
	if n < 0 || n > len(c.b) {
		c.bad = true
		return make([]byte, max(0, min(n, 128)))
	}
	b := c.b[:n]
	c.b = c.b[n:]
	return b
}

func (c *wireCursor) u16() uint16 { b := c.take(2); return binary.BigEndian.Uint16(b) }

func (c *wireCursor) u64() uint64 { b := c.take(8); return binary.BigEndian.Uint64(b) }

func appendEntries(b []byte, entries []flow.State) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(entries)))
	for _, e := range entries {
		b = append(b, e.ID[:]...)
		b = binary.BigEndian.AppendUint64(b, e.OpenSeq)
		b = append(b, e.Intent[:]...)
		for _, v := range []uint64{e.TxBase, e.TxEnd, e.RxApplied, e.RxReceived, e.TxFIN, e.RxFIN} {
			b = binary.BigEndian.AppendUint64(b, v)
		}
		// Wire entries carry FIN existence only. Local CloseWrite/receipt bits
		// remain in the image and cannot become unknown wire flags on reconnect.
		// origin: hot-update S5 plan §2.2 / T12
		b = append(b, e.FINFlags&3, e.OpenStatus)
	}
	return b
}

func readEntries(c *wireCursor) []flow.State {
	n := int(c.u16())
	if n > flow.MaxFlows || n > len(c.b)/106 {
		c.bad = true
		return nil
	}
	out := make([]flow.State, n)
	for i := range out {
		e := &out[i]
		copy(e.ID[:], c.take(16))
		e.OpenSeq = c.u64()
		copy(e.Intent[:], c.take(32))
		e.TxBase = c.u64()
		e.TxEnd = c.u64()
		e.RxApplied = c.u64()
		e.RxReceived = c.u64()
		e.TxFIN = c.u64()
		e.RxFIN = c.u64()
		flags := c.take(2)
		e.FINFlags = flags[0]
		e.OpenStatus = flags[1]
	}
	return out
}

func EncodeResumeRequest(r ResumeRequest) []byte {
	b := []byte{byte(r.Scope), byte(r.Mode), 0, 0, 0, 1}
	b = append(b, r.RequestID[:]...)
	b = append(b, r.Endpoint[:]...)
	b = binary.BigEndian.AppendUint64(b, r.Owner)
	b = binary.BigEndian.AppendUint64(b, r.Next)
	b = append(b, r.Cut[:]...)
	b = append(b, r.Digest[:]...)
	b = binary.BigEndian.AppendUint16(b, r.Target)
	b = binary.BigEndian.AppendUint16(b, uint16(len(r.Ticket)))
	b = append(b, r.Ticket...)
	return appendEntries(b, r.Entries)
}

func DecodeResumeRequest(body []byte) (ResumeRequest, error) {
	c := wireCursor{b: body}
	r := ResumeRequest{}
	prefix := c.take(6)
	if len(prefix) != 6 {
		return r, ErrFlowState
	}
	r.Scope = flow.ResumeScope(prefix[0])
	r.Mode = flow.ResumeMode(prefix[1])
	if binary.BigEndian.Uint16(prefix[2:4]) != 0 || binary.BigEndian.Uint16(prefix[4:6]) != 1 {
		return r, ErrFlowState
	}
	copy(r.RequestID[:], c.take(16))
	copy(r.Endpoint[:], c.take(16))
	r.Owner = c.u64()
	r.Next = c.u64()
	copy(r.Cut[:], c.take(16))
	copy(r.Digest[:], c.take(32))
	r.Target = c.u16()
	n := int(c.u16())
	if n > flow.MaxTicketBytes {
		return r, ErrFlowState
	}
	r.Ticket = append([]byte(nil), c.take(n)...)
	r.Entries = readEntries(&c)
	if c.bad || len(c.b) != 0 || r.Scope > flow.ResumeScopeFlow || r.Mode < flow.ResumeModeInitial || r.Mode > flow.ResumeModeBind || r.Owner == 0 || r.RequestID == ([16]byte{}) {
		return r, ErrFlowState
	}
	return r, nil
}

func EncodeResumeResponse(r ResumeResponse) []byte {
	b := append([]byte(nil), r.RequestID[:]...)
	b = append(b, r.Endpoint[:]...)
	b = binary.BigEndian.AppendUint64(b, r.Owner)
	b = binary.BigEndian.AppendUint64(b, r.Carrier)
	b = append(b, r.Digest[:]...)
	b = append(b, r.GrantDigest[:]...)
	b = append(b, byte(r.Verdict))
	b = binary.BigEndian.AppendUint16(b, uint16(r.Reason))
	b = binary.BigEndian.AppendUint16(b, uint16(len(r.Ticket)))
	b = append(b, r.Ticket...)
	b = binary.BigEndian.AppendUint16(b, r.Page)
	b = binary.BigEndian.AppendUint16(b, r.Pages)
	b = binary.BigEndian.AppendUint64(b, r.Retirement.NextOpenSeq)
	b = binary.BigEndian.AppendUint64(b, r.Retirement.RetiredThrough)
	b = binary.BigEndian.AppendUint16(b, uint16(len(r.Retirement.Holes)))
	for _, h := range r.Retirement.Holes {
		b = binary.BigEndian.AppendUint64(b, h)
	}
	b = appendEntries(b, r.Entries)
	enabled := byte(0)
	if r.Authority.Enabled {
		enabled = 1
	}
	b = append(b, enabled)
	b = binary.BigEndian.AppendUint64(b, uint64(r.Authority.Generation))
	b = binary.BigEndian.AppendUint64(b, uint64(r.Authority.ProxyEpoch))
	b = binary.BigEndian.AppendUint64(b, r.Authority.AppliedRevision)
	b = binary.BigEndian.AppendUint16(b, uint16(len(r.Authority.Keys)))
	for _, k := range r.Authority.Keys {
		b = binary.BigEndian.AppendUint16(b, uint16(len(k.ID)))
		b = append(b, k.ID...)
		b = binary.BigEndian.AppendUint16(b, uint16(len(k.Secret)))
		b = append(b, k.Secret...)
	}
	return b
}

func DecodeResumeResponse(body []byte) (ResumeResponse, error) {
	c := wireCursor{b: body}
	r := ResumeResponse{}
	copy(r.RequestID[:], c.take(16))
	copy(r.Endpoint[:], c.take(16))
	r.Owner = c.u64()
	r.Carrier = c.u64()
	copy(r.Digest[:], c.take(32))
	copy(r.GrantDigest[:], c.take(32))
	r.Verdict = flow.ResumeVerdict(c.take(1)[0])
	r.Reason = flow.Reason(c.u16())
	n := int(c.u16())
	if n > flow.MaxTicketBytes {
		return r, ErrFlowState
	}
	r.Ticket = append([]byte(nil), c.take(n)...)
	r.Page = c.u16()
	r.Pages = c.u16()
	r.Retirement.EndpointID = r.Endpoint
	r.Retirement.NextOpenSeq = c.u64()
	r.Retirement.RetiredThrough = c.u64()
	n = int(c.u16())
	if n > flow.MaxRetirementHoles || n > len(c.b)/8 {
		return r, ErrFlowState
	}
	for range n {
		r.Retirement.Holes = append(r.Retirement.Holes, c.u64())
	}
	r.Entries = readEntries(&c)
	enabled := c.take(1)[0]
	r.Authority.Enabled = enabled == 1
	r.Authority.Generation = int64(c.u64())
	r.Authority.ProxyEpoch = int64(c.u64())
	r.Authority.AppliedRevision = c.u64()
	n = int(c.u16())
	if n > 256 {
		return r, ErrFlowState
	}
	for range n {
		idLen := int(c.u16())
		if idLen > flow.MaxKeyIDBytes {
			return r, ErrFlowState
		}
		id := string(c.take(idLen))
		secretLen := int(c.u16())
		if secretLen > flow.MaxKeySecretBytes {
			return r, ErrFlowState
		}
		r.Authority.Keys = append(r.Authority.Keys, AuthorizedKey{ID: id, Secret: append([]byte(nil), c.take(secretLen)...)})
	}
	if c.bad || len(c.b) != 0 || enabled > 1 || r.Pages < 1 || r.Pages > flow.MaxResumePages || r.Page >= r.Pages || r.Verdict < 1 || r.Verdict > 3 || r.Authority.Generation < 0 || r.Authority.ProxyEpoch < 0 {
		return r, ErrFlowState
	}
	return r, nil
}
