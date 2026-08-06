package proto

import (
	"encoding/json"
	"strings"
	"testing"
)

// ps_truncation_test.go — the wire shape of PsResp's truncation surface.
//
// origin: docs/reviews/h1-external-review.md, the "疑惑/低风险残留" list.
//
// The struct's doc comment used to justify the four new fields with "an
// untruncated reply marshals byte-identical to v0.4.7". That is false:
// *_Total is assigned unconditionally in handlePsReq, and `omitempty` drops
// only the ZERO value, so every reply that lists at least one row carries
// `"procs_total":N` on the wire.
//
// SCOPE — what these tests do and do NOT back. They see the STRUCT only. The
// "assigned unconditionally in handlePsReq" half is a claim about the broker
// and is pinned by internal/broker/ps_totals_test.go instead; a mutation that
// made the handler send totals only when a cap bit left every test in THIS
// file green, which is how that gap was found. Keep the two together: this
// file owns the wire shape, that one owns the handler.
//
// The N-1 window is satisfied regardless — by ADDITIVITY with a legal zero
// value, not by byte-identity — and these tests pin that distinction so the
// comment cannot drift back. A wrong justification is worse than none: it
// hands the next person weighing a wire change a guarantee they believe was
// tested.
func TestPsRespTruncationFieldsAreAdditiveNotInvisible(t *testing.T) {
	t.Run("a populated untruncated reply still carries the totals", func(t *testing.T) {
		b, err := json.Marshal(PsResp{
			Processes:  []PsEntry{{PID: "01h", NID: "lab-1", Status: "RUNNING"}},
			ProcsTotal: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), `"procs_total":1`) {
			t.Fatalf("procs_total vanished from a non-empty reply: %s\n"+
				"If this ever passes, `omitempty` is no longer the only thing gating these keys and "+
				"the comment on PsResp needs re-deriving", b)
		}
	})

	t.Run("byte-identity holds only for an empty reply", func(t *testing.T) {
		b, err := json.Marshal(PsResp{})
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"procs_total", "procs_truncated", "ports_total", "ports_truncated"} {
			if strings.Contains(string(b), key) {
				t.Fatalf("empty PsResp emitted %q (%s) — the ONE case where the old "+
					"byte-identity claim does hold would be broken too", key, b)
			}
		}
	})

	t.Run("an old broker's reply decodes as not-truncated, which is the correct reading", func(t *testing.T) {
		// Exactly what a pre-h1 broker puts on the wire: no truncation keys.
		var got PsResp
		if err := json.Unmarshal([]byte(`{"processes":[{"pid":"01h","nid":"lab-1","status":"RUNNING"}]}`), &got); err != nil {
			t.Fatal(err)
		}
		if got.ProcsTruncated || got.PortsTruncated {
			t.Fatal("a pre-h1 reply must decode as NOT truncated — a broker that cannot truncate " +
				"has, in fact, not truncated anything")
		}
		if got.ProcsTotal != 0 || got.PortsTotal != 0 {
			t.Fatalf("totals from an old broker must stay zero, got procs=%d ports=%d",
				got.ProcsTotal, got.PortsTotal)
		}
		// The zero value must not read as "0 rows exist" when rows were listed:
		// the ctl decides what to print from *_Truncated, never from *_Total
		// alone. This asserts the input the ctl's renderer is entitled to.
		if len(got.Processes) != 1 {
			t.Fatalf("the rows themselves must survive the skew, got %d", len(got.Processes))
		}
	})
}
