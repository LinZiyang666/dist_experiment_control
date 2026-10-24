package main

import (
	"errors"
	"testing"

	"github.com/LinZiyang666/tether/internal/infracli"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// Dynamic projection exemptions must name a real finite producer and its
// actual CLI class. Exercise those production seams with independent expected
// classes: copying the producer's classifier would hide a changed policy.
// origin: S7 internal review ledgers6 F1 (error-code unresolved-sites)
func TestDynamicUpgradeRefusalsKeepExitClasses(t *testing.T) {
	t.Run("transaction", func(t *testing.T) {
		classes := map[string]int{
			upgradetx.CodeUnsupportedComponent: 64, upgradetx.CodeGrantUnknown: 64,
			upgradetx.CodeGrantExpired: 64, upgradetx.CodeRejected: 64,
			upgradetx.CodeWindowClosed: 64, upgradetx.CodeDeferred: 75,
			upgradetx.CodeNotWired: 74, upgradetx.CodeUnsettled: 74,
			upgradetx.CodeStateCorrupt: 74, upgradetx.CodeSessionIncarnationMismatch: 77,
			proto.CodeNotOwner: 77,
		}
		// The local CLI boundary deliberately treats missing wiring as unknown
		// (TestUpgradeClientBoundaryRefusals), while the wire registry calls it
		// an admission refusal. Pin both existing contracts; reconciling that
		// S8 policy is outside the S7 projection-site repair.
		if class, known := upgradewire.ErrorExit(upgradetx.CodeNotWired); !known || class != 75 {
			t.Fatal("missing-wiring wire refusal class changed")
		}
		for code, class := range classes {
			for _, accepted := range []bool{false, true} {
				response := upgradewire.ErrorResponse(&upgradetx.Error{Code: code, Accepted: accepted})
				wantCode, wantClass := code, class
				if accepted && class != 77 {
					wantCode, wantClass = upgradetx.CodeUnsettled, 74
				}
				if response.Code != wantCode {
					t.Fatalf("%s accepted=%t: producer returned %s, want %s", code, accepted, response.Code, wantCode)
				}
				failure := &upgradetx.Error{Code: response.Code, Detail: response.Error, Accepted: accepted}
				if got := classifyExit(upgradeExit("upgrade", failure)); got != wantClass {
					t.Fatalf("%s accepted=%t: CLI exit=%d, want %d", code, accepted, got, wantClass)
				}
			}
		}
		for _, failure := range []error{errors.New("opaque failure"), &upgradetx.Error{Code: "unrecognized_upgrade_code"}} {
			response := upgradewire.ErrorResponse(failure)
			if response.Code != upgradetx.CodeUnsettled || classifyExit(upgradeExit("upgrade", &upgradetx.Error{Code: response.Code})) != 74 {
				t.Fatal("unknown transaction failure lost its unsettled exit")
			}
		}
	})
	t.Run("infra", func(t *testing.T) {
		classes := map[string]int{
			proto.CodeInfraUnsupported: 64, proto.CodeInfraArtifactInvalid: 64,
			proto.CodeInfraConfigInvalid: 64, proto.CodeInfraIncompatible: 64,
			proto.CodeInfraPermissionDenied: 77, proto.CodeInfraPolicyStale: 77,
			proto.CodeInfraNotEnabled: 75, proto.CodeInfraBusy: 75,
			proto.CodeInfraNotReady: 75, proto.CodeInfraInventoryIncomplete: 75,
			proto.CodeInfraBudgetExceeded: 75, proto.CodeInfraResultUnknown: 74,
			proto.CodeInfraRecoveryRequired: 74, proto.CodeInfraInternal: 70,
		}
		if len(infracli.ExitClasses) != len(classes) {
			t.Fatalf("infra classifier has %d codes, want the %d-code S9 family", len(infracli.ExitClasses), len(classes))
		}
		for code, class := range classes {
			if got, ok := infracli.ExitClasses[code]; !ok || got != class {
				t.Fatalf("%s: registered exit=%d present=%t, want %d", code, got, ok, class)
			}
			if got := proto.InfraCode(errors.New(code)); got != code {
				t.Fatalf("%s: error producer returned %s", code, got)
			}
			report := proto.InfraStatusReport(c.Status{Code: code})
			if report.Code != code {
				t.Fatalf("%s: status projection returned %s", code, report.Code)
			}
			if got := classifyExit(brokerErrorMessage("infra", report.Code, "refused")); got != class {
				t.Fatalf("%s: CLI exit=%d, want %d", code, got, class)
			}
		}
		if proto.InfraCode(nil) != "" || proto.InfraCode(errors.New("unknown_host_failure")) != proto.CodeInfraInternal {
			t.Fatal("empty/unknown infra error projection changed")
		}
	})
}

// The old install producer was replaced by a grant-required refusal. Keeping
// its wire classes is required by architecture §21.2 while an N-1 peer can
// still send them; a same-named S8 detail is not a live Code producer.
// origin: S7 internal review ledgers6 F3 (error-code missing-emitter-keys)
func TestLegacyUpgradeCodesKeepNMinusOneClasses(t *testing.T) {
	for code, want := range map[string]int{
		"download_failed": 75, "download_http_retryable": 75,
		"download_http_status": 64, "download_too_large": 64,
		"image_abi_unsupported": 64, "install_failed": 64,
		"self_path": 64, "sha256_mismatch": 64, "sha256_required": 64,
		"smoke_failed": 64, "upgrade_in_progress": 75, "url_not_allowed_local": 64,
	} {
		if got, ok := brokerCodeExitClasses[code]; !ok || got != want {
			t.Errorf("N-1 code %s has exit=%d present=%t, want %d", code, got, ok, want)
		}
		if got := classifyExit(brokerErrorMessage("upgrade", code, "refused")); got != want {
			t.Errorf("N-1 code %s reaches CLI exit=%d, want %d", code, got, want)
		}
		if staleClassificationKeys[code] == "" {
			t.Errorf("N-1 code %s lost its specific compatibility obligation", code)
		}
	}
}
