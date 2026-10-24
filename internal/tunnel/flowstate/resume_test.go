package flowstate

import (
	"bytes"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/LinZiyang666/tether/internal/flow"
)

// origin: hot-update S5 plan §4.3 / T13; hot-update S6 plan §9.3 / TUNNEL-SLIM
func exerciseResumeAuthorityPages(t *testing.T) {
	full := ResumeResponse{RequestID: [16]byte{1}, Endpoint: flow.EndpointID{2}, Owner: 1, Carrier: 2, Verdict: flow.ResumeVerdictAccepted, Pages: 1, Authority: Authority{Enabled: true, Generation: 1, AppliedRevision: 9}, Retirement: flow.RetirementImage{NextOpenSeq: 4, RetiredThrough: 1, Holes: []uint64{3}}}
	for i := 0; i < 128; i++ {
		full.Authority.Keys = append(full.Authority.Keys, AuthorizedKey{ID: fmt.Sprint(i), Secret: bytes.Repeat([]byte{byte(i)}, 512)})
	}
	pages, err := SplitResumeResponse(full)
	if err != nil || len(pages) < 2 {
		t.Fatalf("authority did not page: %v", err)
	}
	decode := func(input []ResumeResponse, budget *atomic.Int64, changeCarrier bool) (ResumeResponse, error) {
		index := 0
		next := func() (flow.Frame, error) {
			if index >= len(input) {
				return flow.Frame{}, io.ErrUnexpectedEOF
			}
			page := input[index]
			body := EncodeResumeResponse(page)
			index++
			gen := page.Carrier
			if changeCarrier && index > 1 {
				gen++
			}
			return flow.Frame{Version: flow.Version, Kind: flow.KindResumeOK, CarrierGen: gen, BodyLen: uint32(len(body)), Body: body}, nil
		}
		first, _ := next()
		return ReadResumePages(first, next, budget)
	}
	var budget atomic.Int64
	got, err := decode(pages, &budget, false)
	if err != nil || len(got.Authority.Keys) != 128 || budget.Load() != 0 {
		t.Fatalf("complete grant failed or leaked reservation: %v", err)
	}
	for _, variant := range []string{"holes", "verdict", "carrier", "digest", "missing", "duplicate"} {
		input := append([]ResumeResponse(nil), pages...)
		switch variant {
		case "holes":
			input[1].Retirement.Holes = []uint64{2}
		case "verdict":
			input[1].Verdict = flow.ResumeVerdictRejected
		case "digest":
			input[1].GrantDigest[0]++
		case "missing":
			input = input[:len(input)-1]
		case "duplicate":
			altered := input[0]
			altered.Authority.Keys = append([]AuthorizedKey(nil), altered.Authority.Keys...)
			altered.Authority.Keys[0].ID = "conflicting"
			input = append(input[:1], append([]ResumeResponse{altered}, input[1:]...)...)
		}
		if _, err = decode(input, &budget, variant == "carrier"); err == nil {
			t.Fatalf("partial authority accepted %s page", variant)
		}
		if budget.Load() != 0 {
			t.Fatal("rejected grant retained node reservation")
		}
	}
	budget.Store(flow.MaxResumeBytes - 1)
	if _, err = decode(pages, &budget, false); err == nil || budget.Load() != flow.MaxResumeBytes-1 {
		t.Fatal("authority exceeded node budget or released another owner")
	}
}

// origin: hot-update S5 plan §4.3 / T13; hot-update S6 plan §9.3 / TUNNEL-SLIM
func TestResumeAuthorityPagesMatchGrantAndBudget(t *testing.T) {
	for round := 0; round < 5; round++ {
		exerciseResumeAuthorityPages(t)
	}
}
