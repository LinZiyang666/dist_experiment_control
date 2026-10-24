package upgradecli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: hot-update S8 plan §2 / §7.1 / P18.
func TestUpgradeViewSeparatesHistoryAndObservation(t *testing.T) {
	view := transactionView()
	view.Historical.Forward.Phase = upgradetx.PhaseCompleted
	view.Historical.Forward.Outcome = upgradetx.OutcomeSuccess
	view.Historical.Revision = 10
	view.Observation.Current.Outcome = upgradetx.OutcomeUnknown
	view.Observation.OwnerKnown = false
	view.Observation.Nonce = "fresh"
	view.Observation.Census.Known = false
	view.Metadata.Window = upgradetx.WindowClosed
	view.Code = upgradetx.CodeUnsettled
	view.Detail = "owner cannot be observed"
	view.Historical.Reason = "historical completion"
	var output bytes.Buffer
	if err := Render(&output, view, true); err != nil {
		t.Fatal(err)
	}
	var got upgradetx.View
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, view) {
		t.Fatalf("JSON projected away current facts: %+v", got)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(output.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"historical", "metadata", "observation", "code", "detail"} {
		if _, ok := fields[name]; !ok {
			t.Fatalf("missing JSON field %s", name)
		}
	}
	output.Reset()
	if err := Render(&output, view, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Historical:", "phase=completed outcome=success", "Observation:", "phase=migrating outcome=unknown owner_known=false", "Census: known=false complete=false", "control_ready=false resources_migrated=false old_generation_released=false", "window=closed", "owner cannot be observed", "historical completion"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("text missing %q: %s", want, output.String())
		}
	}
	view.Historical.Grant.Mode = upgradetx.ModeLegacyInterrupting
	output.Reset()
	if err := Render(&output, view, false); err != nil || !strings.Contains(output.String(), "legacy_interrupting non_hot") {
		t.Fatalf("legacy mode not visible: %s %v", output.String(), err)
	}
}

// origin: hot-update S8 plan §7.1 / P18 — accepted unknown is never rc75.
func TestUpgradeExitCodePreservesAcceptance(t *testing.T) {
	for _, row := range []struct {
		code     string
		accepted bool
		rc       int
	}{
		{upgradetx.CodeRejected, false, 64}, {upgradetx.CodeUnsupportedComponent, false, 64},
		{upgradetx.CodeWindowClosed, false, 64}, {upgradetx.CodeGrantUnknown, false, 64}, {upgradetx.CodeGrantExpired, false, 64},
		{"bad_request", false, 64}, {"legacy_busy", false, 64},
		{upgradetx.CodeDeferred, false, 75}, {upgradetx.CodeDeferred, true, 74}, {"try_again", false, 75}, {"try_again", true, 74},
		{"not_owner", false, 77}, {"not_owner", true, 77}, {upgradetx.CodeSessionIncarnationMismatch, true, 77},
		{upgradetx.CodeUnsettled, true, 74}, {upgradetx.CodeStateCorrupt, false, 74}, {upgradetx.CodeNotWired, false, 74}, {"future_code", false, 74},
	} {
		err := fmt.Errorf("wrapped: %w", &upgradetx.Error{Code: row.code, Accepted: row.accepted})
		if got := ExitCode(err); got != row.rc {
			t.Fatalf("%s accepted=%t: got%d want%d", row.code, row.accepted, got, row.rc)
		}
	}
	if ExitCode(nil) != 0 || ExitCode(errors.New("transport lost")) != 74 {
		t.Fatal("nil or unclassified error lost its exit policy")
	}
}

type failingOutput struct {
	err            error
	writes, failAt int
}

func (w *failingOutput) Write(data []byte) (int, error) {
	w.writes++
	if w.writes >= w.failAt {
		return 0, w.err
	}
	return len(data), nil
}

// origin: hot-update S8 plan §7.1 / P18.
func TestUpgradeRenderPropagatesWriteFailure(t *testing.T) {
	view := transactionView()
	view.Historical.Grant.Mode = upgradetx.ModeLegacyInterrupting
	view.Detail = "diagnostic"
	sentinel := errors.New("output closed")
	for _, asJSON := range []bool{false, true} {
		for _, failAt := range []int{1, 2, 3} {
			if asJSON && failAt != 1 {
				continue
			}
			output := &failingOutput{err: sentinel, failAt: failAt}
			if err := Render(output, view, asJSON); !errors.Is(err, sentinel) {
				t.Fatalf("write failure lost: json=%t failAt=%d err=%v", asJSON, failAt, err)
			}
		}
	}
}
