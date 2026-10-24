package agentupgrade

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: hot-update S8 plan §6.1; S8 review rv-n1 F5; S8 FIX-AGENTWIRE.
// The candidate policy decides every positive ABI from both readers' declared
// epochs. A numeric ceiling refused a real future reader for the wrong reason
// (its own ABI number) and never saw a candidate that declares it cannot read
// this binary's epoch; both must carry the epoch admission reason.
func TestCandidateABIEpochsDecideWithoutNumericCeiling(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	exec := []string{"exec-v1"}
	for _, tc := range []struct {
		name      string
		candidate agentimage.Capabilities
		empty     bool
		reason    string // "" accepts; otherwise the epoch reason or "abi"
	}{
		{"adjacent_future_reader", agentimage.Capabilities{ABI: 2, ReadEpochs: []uint64{1, 2}, Domains: exec}, false, "image_epoch_unreadable"},
		{"undeclared_future_reader", agentimage.Capabilities{ABI: 2, Domains: exec}, false, "image_epoch_unreadable"},
		{"two_epochs_ahead", agentimage.Capabilities{ABI: 3, ReadEpochs: []uint64{2, 3}, Domains: exec}, false, "image_epoch_distance"},
		{"current_reader_excludes_us", agentimage.Capabilities{ABI: 1, ReadEpochs: []uint64{2}, Domains: exec}, false, "image_epoch_unreadable"},
		{"current_reader", agentimage.Capabilities{ABI: 1, ReadEpochs: []uint64{1}, Domains: exec}, false, ""},
		{"legacy_same_epoch_reader", agentimage.Capabilities{ABI: 1, Domains: exec}, false, ""},
		{"bare_exec_empty_image", agentimage.Capabilities{}, true, ""},
		{"bare_exec_live_image", agentimage.Capabilities{}, false, "abi"},
		{"negative_abi", agentimage.Capabilities{ABI: -1}, true, "abi"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckCandidateABI(logger, func() bool { return tc.empty }, func() []string { return exec }, tc.candidate)
			if tc.reason == "" {
				if err != nil {
					t.Fatalf("compatible reader refused: %v", err)
				}
				return
			}
			if !errors.Is(err, agentimage.ErrABIUnsupported) {
				t.Fatalf("unsafe reader admitted: %v", err)
			}
			var admission *upgradetx.Error
			if tc.reason == "abi" {
				if errors.As(err, &admission) {
					t.Fatalf("ABI shape refusal claimed an epoch decision: %v", admission.Detail)
				}
				return
			}
			if !errors.As(err, &admission) || admission.Detail != tc.reason {
				t.Fatalf("refusal is not the reader-epoch decision %q: %v", tc.reason, err)
			}
		})
	}
}

// origin: hot-update S8 plan §7.1; S8 FIX-FIXTURES R2; S8 FIX-AGENTWIRE. The
// exact reason crosses the engine's public projection only from the closed
// runtime/admission vocabulary; paths, identities and adapter text never do,
// and acceptance still upgrades a refusal to unsettled without losing it.
func TestPublicRefusalCarriesOnlyClosedReasons(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		accepted bool
		code     string
		detail   string
	}{
		{"busy_reason", &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "legacy_session_busy"}, false, upgradetx.CodeDeferred, "legacy_session_busy"},
		{"cut_in_progress", &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: agentupgradeport.ReasonUpgradeInProgress}, false, upgradetx.CodeDeferred, agentupgradeport.ReasonUpgradeInProgress},
		{"epoch_reason", &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: "image_epoch_unreadable"}, false, upgradetx.CodeRejected, "image_epoch_unreadable"},
		{"accepted_keeps_reason", &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: agentupgradeport.ReasonImageABIUnsupported}, true, upgradetx.CodeUnsettled, agentupgradeport.ReasonImageABIUnsupported},
		{"private_text", &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "open /home/operator/.tether/secret: denied"}, false, upgradetx.CodeDeferred, "upgrade port operation failed"},
		{"untyped", errors.New("salt=00ff private"), false, upgradetx.CodeDeferred, "upgrade preflight failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got *upgradetx.Error
			if !errors.As(publicError(tc.err, tc.accepted), &got) || got.Code != tc.code || got.Detail != tc.detail {
				t.Fatalf("public projection = %+v, want %s %q", got, tc.code, tc.detail)
			}
		})
	}
}
