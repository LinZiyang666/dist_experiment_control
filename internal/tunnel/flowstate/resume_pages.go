package flowstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"
	"sync/atomic"

	"github.com/LinZiyang666/tether/internal/flow"
)

var ErrFlowState = errors.New("tunnel: invalid flow owner state")

// Authority pages have one digest over the complete snapshot; no page grants
// partial attachment or keys. Only keys need splitting at the 32-flow bound.
func SplitResumeResponse(full ResumeResponse) ([]ResumeResponse, error) {
	if len(full.Authority.Keys) > 256 || len(full.Entries) > flow.MaxFlows {
		return nil, ErrFlowState
	}
	full.GrantDigest = ResponseDigest(full)
	current := full
	current.Authority.Keys = nil
	if len(EncodeResumeResponse(current)) > flow.MaxControlBytes {
		return nil, ErrFlowState
	}
	pages := []ResumeResponse{}
	for _, key := range full.Authority.Keys {
		if len(key.ID) > flow.MaxKeyIDBytes || len(key.Secret) > flow.MaxKeySecretBytes {
			return nil, ErrFlowState
		}
		candidate := current
		candidate.Authority.Keys = append(append([]AuthorizedKey(nil), current.Authority.Keys...), key)
		if len(EncodeResumeResponse(candidate)) > flow.MaxControlBytes {
			pages = append(pages, current)
			current = full
			current.Entries = nil
			current.Authority.Keys = []AuthorizedKey{key}
		} else {
			current = candidate
		}
	}
	pages = append(pages, current)
	if len(pages) > flow.MaxResumePages {
		return nil, ErrFlowState
	}
	for i := range pages {
		pages[i].Page = uint16(i)
		pages[i].Pages = uint16(len(pages))
	}
	return pages, nil
}

func ReadResumePages(first flow.Frame, next func() (flow.Frame, error), budget *atomic.Int64) (ResumeResponse, error) {
	var out ResumeResponse
	var reserved int64
	if budget != nil {
		defer func() { budget.Add(-reserved) }()
	}
	pages := make(map[uint16]ResumeResponse)
	digests := make(map[uint16][32]byte)
	frame := first
	aggregate := 0
	for {
		if frame.Version != flow.Version || frame.Kind != flow.KindResumeOK || frame.FlowID != (flow.ID{}) || frame.Flags != 0 || int(frame.BodyLen) != len(frame.Body) {
			return out, ErrFlowState
		}
		response, err := DecodeResumeResponse(frame.Body)
		if err != nil {
			return out, err
		}
		if len(pages) == 0 {
			out = response
			out.Entries = nil
			out.Authority.Keys = nil
		}
		if frame.CarrierGen != response.Carrier || response.Verdict != out.Verdict || response.Reason != out.Reason || !slices.Equal(response.Retirement.Holes, out.Retirement.Holes) || response.RequestID != out.RequestID || response.Endpoint != out.Endpoint || response.Owner != out.Owner || response.Carrier != out.Carrier || response.Digest != out.Digest || response.GrantDigest != out.GrantDigest || response.Pages != out.Pages || response.Authority.Enabled != out.Authority.Enabled || response.Authority.Generation != out.Authority.Generation || response.Authority.ProxyEpoch != out.Authority.ProxyEpoch || response.Authority.AppliedRevision != out.Authority.AppliedRevision || response.Retirement.NextOpenSeq != out.Retirement.NextOpenSeq || response.Retirement.RetiredThrough != out.Retirement.RetiredThrough || !bytes.Equal(response.Ticket, out.Ticket) {
			return out, ErrFlowState
		}
		digest := sha256.Sum256(frame.Body)
		if old, ok := digests[response.Page]; ok {
			if old != digest {
				return out, ErrFlowState
			}
		} else {
			size := len(frame.Body)
			aggregate += size
			if aggregate > flow.MaxResumeBytes {
				return out, ErrFlowState
			}
			if budget != nil {
				if budget.Add(int64(size)) > flow.MaxResumeBytes {
					budget.Add(-int64(size))
					return out, ErrFlowState
				}
				reserved += int64(size)
			}
			pages[response.Page] = response
			digests[response.Page] = digest
		}
		if len(pages) == int(out.Pages) {
			break
		}
		frame, err = next()
		if err != nil {
			return out, err
		}
	}
	ids := make(map[flow.ID]bool)
	keys := make(map[string]bool)
	for page := uint16(0); page < out.Pages; page++ {
		p, ok := pages[page]
		if !ok {
			return out, ErrFlowState
		}
		for _, e := range p.Entries {
			if ids[e.ID] {
				return out, ErrFlowState
			}
			ids[e.ID] = true
			out.Entries = append(out.Entries, e)
		}
		for _, key := range p.Authority.Keys {
			if keys[key.ID] {
				return out, ErrFlowState
			}
			keys[key.ID] = true
			out.Authority.Keys = append(out.Authority.Keys, key)
		}
	}
	if len(out.Entries) > flow.MaxFlows || len(out.Authority.Keys) > 256 || out.GrantDigest != ResponseDigest(out) {
		return out, ErrFlowState
	}
	return out, nil
}

func ResponseDigest(r ResumeResponse) [32]byte {
	r.GrantDigest = [32]byte{}
	r.Page = 0
	r.Pages = 1
	return sha256.Sum256(EncodeResumeResponse(r))
}
