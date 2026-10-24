package main

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: S7 internal review rv-n1compat F6
func TestTransferLegacyRefusalsKeepNativeExitClasses(t *testing.T) {
	for code, want := range map[string]int{
		"try_again": 75, "forward_failed": 75, "broker_forward_failed": 75, "too_many_in_flight": 75,
		"agent_no_responders": 75, "leader_unavailable": 75, "home_catching_up": 75, "jetstream_not_ready": 75,
		"broker_restarting": 75, "cluster_not_ready": 75, "path_race": 75, "agent_upgrading": 75, "transfer_id_in_flight": 75,
		"path_outside_roots": 77, "not_a_member": 77, "not_owner": 77, "not_owner_or_creator": 77, "not_allowed": 77,
		"path_not_absolute": 64, "path_not_found": 64, "path_parent_missing": 64, "not_a_regular_file": 64, "dst_exists": 64,
		"too_large": 64, "transfer_unknown": 64, "tier_invalid": 64, "size_mismatch": 64, "sha_mismatch": 64,
		"bucket_unknown": 64, "tier_b_store_too_small": 64, "jetstream_unavailable": 64, "node_offline": 64,
		"node_not_found": 64, "session_not_found": 64, "verb_mismatch": 64, "request_invalid": 64, "transfer_disabled": 64,
		"actor_invalid": 70, "subject_malformed": 70, "marshal": 70, "json_parse": 70, "agent_malformed_resp": 70,
		"store_error": 70, "internal_error": 70, "reply_too_large": 70, "io_error": 70, "object_get_failed": 70,
		"object_put_failed": 70, "bucket_create_failed": 70, "bucket_bind_failed": 70,
	} {
		t.Run(code, func(t *testing.T) {
			d, _ := controlbudget.After(time.Second)
			owner, err := xferclient.NewLegacy(xferclient.LegacyConfig{TransferID: "original", PutOnce: func(x.Deadline) (x.LegacyResult, error) {
				return x.LegacyResult{Kind: "failed", Code: code, Error: "determinate peer refusal"}, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := owner.Stop(d); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			_, err = owner.Run(d)
			if got := classifyExit(transferLeafError(transferCLI, err)); got != want {
				t.Fatalf("leaf -> command -> main: got %d want %d: %v", got, want, err)
			}
			if got := brokerCodeExitClass(code); got != want {
				t.Fatalf("native wire table differs: got %d want %d", got, want)
			}
		})
	}
	for _, tc := range []struct {
		phase      x.Phase
		unresolved bool
		want       int
	}{{"", false, 64}, {x.PhaseSending, false, 75}, {x.PhaseSending, true, 74}, {"", true, 74}} {
		err := &xferclient.Error{Code: x.CodeBudgetExceeded, TransferID: "original", Phase: tc.phase, Unresolved: tc.unresolved}
		if got := classifyExit(transferLeafError(transferCLI, err)); got != tc.want {
			t.Fatalf("budget phase=%s unresolved=%v exit=%d want=%d", tc.phase, tc.unresolved, got, tc.want)
		}
	}
}
