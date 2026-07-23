package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

const mib = 1024 * 1024

// TestTierAInlineCeilingNeverWidenedByAMissingMeasurement is the second half of #67 face B. The
// pre-fix code clamped only `if caps.MaxPayload > 0`, so a FAILED probe (zero-value CapsResp,
// MaxPayload=0) silently RAISED the inline ceiling back to the 8 MiB design default and moved the
// tier-A/B boundary with nobody told. A missing measurement must never widen a budget.
func TestTierAInlineCeilingNeverWidenedByAMissingMeasurement(t *testing.T) {
	small := capsProbe{Status: capsOK, Resp: proto.CapsResp{OK: true, MaxPayload: 2 * mib}}
	wantSmall := int64(2*mib/2 - 1024)

	if got := tierAInlineCeiling(0, small); got != wantSmall {
		t.Fatalf("an authoritative small max_payload must clamp: got %d want %d", got, wantSmall)
	}
	// The connection's own max_payload is ground truth and is ALWAYS available on a connected conn.
	if got := tierAInlineCeiling(2*mib, capsProbe{Status: capsUndetermined, Err: errors.New("timeout")}); got != wantSmall {
		t.Fatalf("a failed probe must still be clamped by the connection's own max_payload: got %d want %d", got, wantSmall)
	}
	// The regression itself: probe failed AND we have no conn measurement -> fall back to the design
	// ceiling, never above it.
	if got := tierAInlineCeiling(0, capsProbe{Status: capsUndetermined, Err: errors.New("timeout")}); got != cliTierAMaxBytes {
		t.Fatalf("with no measurement at all the ceiling must be the design default %d, got %d", cliTierAMaxBytes, got)
	}
	// A refused probe carries no capability information, so its (zero) MaxPayload must be ignored
	// rather than treated as authoritative.
	refused := capsProbe{Status: capsRefused, Resp: proto.CapsResp{Code: "not_a_member"}}
	if got := tierAInlineCeiling(2*mib, refused); got != wantSmall {
		t.Fatalf("a refused probe must not contribute a bogus zero: got %d want %d", got, wantSmall)
	}
	// The tightest of several known measurements wins.
	both := capsProbe{Status: capsOK, Resp: proto.CapsResp{OK: true, MaxPayload: 16 * mib}}
	if got := tierAInlineCeiling(2*mib, both); got != wantSmall {
		t.Fatalf("the TIGHTEST known measurement must win: got %d want %d", got, wantSmall)
	}
}

// TestChooseTierNeverInventsACapabilityClaim is the #67 face-B headline: only an AUTHORITATIVE answer
// may produce a "this broker has no JetStream" refusal. Anything less proceeds and lets the broker —
// the single adjudicator — answer.
func TestChooseTierNeverInventsACapabilityClaim(t *testing.T) {
	big := int64(12_000_000) // > the 8 MiB tier-A ceiling => tier B territory

	for _, tc := range []struct {
		name  string
		probe capsProbe
		kills string
	}{
		{"probe timed out", capsProbe{Status: capsUndetermined, Err: errors.New("nats: timeout")},
			"the optimistic path for a transport failure — the pre-#67 code refused here with an invented claim"},
		{"broker declined to answer (says nothing about JetStream)",
			capsProbe{Status: capsRefused, Resp: proto.CapsResp{Code: "not_a_member"}},
			"the optimistic path for a non-authoritative refusal; note transferGate re-checks membership authoritatively"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tier, _, err := chooseTier(big, tc.probe, 0)
			if err != nil {
				t.Fatalf("a non-authoritative probe must NOT produce a local refusal, got %v — kills: %s", err, tc.kills)
			}
			if tier != "b" {
				t.Fatalf("want tier b (let the broker adjudicate), got %q", tier)
			}
			if note := tc.probe.warning(); note == "" {
				t.Fatal("proceeding on a guess must be announced on stderr")
			}
		})
	}

	// Authoritative "no JetStream" IS entitled to refuse — a genuine single-broker deployment without
	// JetStream must still get an honest permanent error.
	authoritative := capsProbe{Status: capsOK, Resp: proto.CapsResp{OK: true, JetStreamReady: false, MaxPayload: 16 * mib}}
	_, _, err := chooseTier(big, authoritative, 16*mib)
	if err == nil {
		t.Fatal("an AUTHORITATIVE no-JetStream answer must still refuse; otherwise the honest permanent case is lost")
	}
	if !strings.Contains(err.Error(), "reports JetStream is not available") {
		t.Fatalf("the refusal must attribute the claim to the broker, not assert it; got %v", err)
	}
	// The max_payload clause is misdirection for a file that can never travel inline.
	if strings.Contains(err.Error(), "max_payload") {
		t.Fatalf("a %d-byte file can never fit inline, so raising max_payload cannot help and must not "+
			"be offered — that unconditional advice was half of #67 face B; got %v", big, err)
	}
	if warn := authoritative.warning(); warn != "" {
		t.Fatalf("an authoritative answer needs no warning, got %q", warn)
	}

	// ...but when the file COULD fit inline, raising max_payload genuinely is an alternative, so it
	// is offered. This is the one case the pre-#67 message was accidentally right about.
	small := int64(3 * mib)
	tinyPayload := capsProbe{Status: capsOK, Resp: proto.CapsResp{OK: true, JetStreamReady: false, MaxPayload: 1 * mib}}
	_, _, err = chooseTier(small, tinyPayload, 1*mib)
	if err == nil {
		t.Fatal("a file above the (tiny) inline budget with no JetStream must be refused")
	}
	if !strings.Contains(err.Error(), "max_payload") {
		t.Fatalf("here raising max_payload WOULD let the file travel inline, so it must be offered; got %v", err)
	}

	// A file that fits inline is tier A and must never be blocked by a failed probe.
	tier, _, err := chooseTier(1024, capsProbe{Status: capsUndetermined, Err: errors.New("boom")}, 0)
	if err != nil || tier != "a" {
		t.Fatalf("a small file must go tier A regardless of the probe: tier=%q err=%v", tier, err)
	}
}

// TestCapsProbeWarningsAreHonest pins that the two warnings say what was actually established — in
// particular that a REFUSED probe says nothing about JetStream.
func TestCapsProbeWarningsAreHonest(t *testing.T) {
	undet := capsProbe{Status: capsUndetermined, Err: errors.New("nats: timeout")}
	if w := undet.warning(); !strings.Contains(w, "could not read broker capabilities") || !strings.Contains(w, "timeout") {
		t.Fatalf("the undetermined warning must name the real cause; got %q", w)
	}
	ref := capsProbe{Status: capsRefused, Resp: proto.CapsResp{Code: "session_not_found_or_deleting"}}
	w := ref.warning()
	if !strings.Contains(w, "session_not_found_or_deleting") {
		t.Fatalf("the refused warning must surface the broker's own code; got %q", w)
	}
	if !strings.Contains(w, "says nothing about JetStream") {
		t.Fatalf("the refused warning must explicitly NOT be read as a JetStream verdict — that "+
			"conflation is #67 face B; got %q", w)
	}
	for _, w := range []string{undet.warning(), ref.warning()} {
		if strings.Contains(w, "has no JetStream") || strings.Contains(w, "bump nats max_payload") {
			t.Fatalf("the deleted #67 literals must not come back through the warnings; got %q", w)
		}
	}
}

// TestTransferRefusalCarriesExitClassWithoutChangingText pins G67 step 6. Two things must BOTH hold,
// and they pull in opposite directions:
//
//   - the transient refusal must exit 75 so a monitor retries instead of filing a tether bug (before
//     this wiring the map entry existed but nothing consulted it, so it exited the unclassified 70 —
//     caught on the deploy tier, not by any unit test);
//   - the human text must be BYTE-IDENTICAL to the pre-G67 format, because
//     drills/61-transfer-edges greps the literal `code=<X>` token and is GREEN. Routing these through
//     brokerErrorMessage would render `<verb> failed: ... (code)` and break it.
func TestTransferRefusalCarriesExitClassWithoutChangingText(t *testing.T) {
	for _, tc := range []struct {
		code      string
		wantClass int
		why       string
	}{
		{"jetstream_not_ready", exitTransient, "the whole point of the #67 split: a bounded-retried transient stall is retryable, not a tether bug"},
		{"too_many_in_flight", exitTransient, "the broker already words it 'retry shortly'"},
		{"transfer_id_in_flight", exitTransient, "ditto"},
		{"bucket_create_failed", exitInternal, "the PERMANENT half must NOT become retryable"},
		{"some_unmapped_code", exitInternal, "unmapped codes keep the pre-G67 default"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			err := transferRefusalErr(tc.code, "push (tier B) refused at prepare: code=%s %s", tc.code, "detail")
			var ee *ExitError
			if !errors.As(err, &ee) {
				t.Fatalf("a transfer refusal must carry an exit class; got %T", err)
			}
			if ee.Class != tc.wantClass {
				t.Fatalf("code %q -> class %d, want %d (%s)", tc.code, ee.Class, tc.wantClass, tc.why)
			}
			want := "push (tier B) refused at prepare: code=" + tc.code + " detail"
			if got := err.Error(); got != want {
				t.Fatalf("the text must be byte-identical to the pre-G67 format (drill 61 greps `code=<X>`):\n got %q\nwant %q", got, want)
			}
		})
	}
}

// TestClassifyCapsRespThreeWay closes internal-review B1's surviving mutation: with the classification
// folded into the RPC wrapper, flipping `!resp.OK` to capsOK left every Go test green — and that flip
// ALONE reinstates #67 face B, because a broker that merely DECLINED to answer would be treated as
// authoritative and chooseTier would then refuse with a permanent capability claim the broker never
// made. Each row names the mutation it kills.
func TestClassifyCapsRespThreeWay(t *testing.T) {
	for _, tc := range []struct {
		name  string
		resp  proto.CapsResp
		err   error
		want  capsStatus
		kills string
	}{
		{"transport failure is UNDETERMINED, never a JetStream verdict",
			proto.CapsResp{}, errors.New("nats: timeout"), capsUndetermined,
			"dropping the err arm — the zero-value CapsResp would then look authoritative, which IS #67 face B"},
		{"a broker that declined (OK=false) is REFUSED, not authoritative",
			proto.CapsResp{OK: false, Code: "not_a_member"}, nil, capsRefused,
			"flipping the !resp.OK arm to capsOK — the single mutation that reinstates face B"},
		{"OK=false with JetStreamReady set is STILL only refused",
			proto.CapsResp{OK: false, Code: "store_error", JetStreamReady: true}, nil, capsRefused,
			"trusting fields on a reply the broker marked not-OK"},
		{"an affirmative answer is AUTHORITATIVE",
			proto.CapsResp{OK: true, JetStreamReady: true, MaxPayload: 16 * mib}, nil, capsOK,
			"the authoritative arm — without it a real no-JetStream broker could never be reported honestly"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyCapsResp(tc.resp, tc.err)
			if got.Status != tc.want {
				t.Fatalf("status=%v want %v — this row exists to kill: %s", got.Status, tc.want, tc.kills)
			}
			if tc.want != capsOK && got.warning() == "" {
				t.Fatal("a non-authoritative probe must announce that it is proceeding on a guess")
			}
		})
	}
}

// TestWireCodeLiteralsAreTheCrossPackageContract closes the other half of B1: the broker constant and
// this package's exit-class map are joined by nothing but two independently-written string literals,
// so renaming the constant left BOTH packages green while `jetstream_not_ready` silently reverted to
// exit 70. The literal is the wire contract; pin it on both sides.
func TestWireCodeLiteralsAreTheCrossPackageContract(t *testing.T) {
	for code, wantClass := range map[string]int{
		"jetstream_not_ready":    exitTransient,
		"tier_b_store_too_small": exitUsage,
		"jetstream_unavailable":  exitUsage,
		"broker_restarting":      exitTransient,
	} {
		if got := brokerCodeExitClass(code); got != wantClass {
			t.Fatalf("wire code %q maps to exit %d, want %d — if the BROKER renamed its constant, this "+
				"map silently stops matching and the code falls back to the unclassified 70", code, got, wantClass)
		}
	}
}
