//go:build linux && infrabusiness_integration

package infrabusiness_test

import (
	"github.com/LinZiyang666/tether/internal/proto"
	"testing"
)

// origin: S9 internal review rv-business F4
func TestBusinessOracleComputesRawDurableProgress(t *testing.T) {
	for name, mutate := range map[string]func(*businessReceipt){
		"frozen_sixty_seconds": func(r *businessReceipt) {
			scaleTransferClock(r, 10)
			for n := range r.Transfers {
				row := &r.Transfers[n]
				row.Durable, row.PreviousDurable, row.MaxSilenceNS = 32768, 32768, 0
				row.PrefixSHA256, row.RawPrefixSHA256 = digest([]byte("frozen")), digest([]byte("frozen"))
				for k := range row.Checkpoints {
					row.Checkpoints[k].Durable, row.Checkpoints[k].PrefixSHA256 = 32768, row.RawPrefixSHA256
				}
			}
		},
		"raw_stall_overrides_zero_summary": func(r *businessReceipt) { scaleTransferClock(r, 20) },
		"no_per_hop_progress": func(r *businessReceipt) {
			for n := range r.Transfers {
				row := &r.Transfers[n]
				row.Durable, row.PreviousDurable = 32768, 32768
				row.PrefixSHA256, row.RawPrefixSHA256 = digest([]byte("frozen")), digest([]byte("frozen"))
				for k := range row.Checkpoints {
					row.Checkpoints[k].Durable, row.Checkpoints[k].PrefixSHA256 = 32768, row.RawPrefixSHA256
				}
			}
		},
		"wrong_boot":            func(r *businessReceipt) { r.Transfers[0].Checkpoints[0].BootID = "reboot" },
		"missing_raw":           func(r *businessReceipt) { r.Transfers[0].Checkpoints = nil },
		"missing_leading_edge":  func(r *businessReceipt) { r.Transfers[0].Checkpoints[0].AtNS = r.Hops[0].CutNS - 1 },
		"missing_trailing_edge": func(r *businessReceipt) { r.Transfers[8].Checkpoints[0].AtNS = r.Hops[0].EndNS - 1 },
		"raw_summary_conflict":  func(r *businessReceipt) { r.Transfers[len(r.Transfers)-1].Checkpoints[0].Durable++ },
		"unsynced_raw":          func(r *businessReceipt) { r.Transfers[0].Checkpoints[0].Synced = false },
		"recovery_without_progress": func(r *businessReceipt) {
			for _, n := range []int{4, 8} {
				r.Transfers[n].Durable, r.Transfers[n].PreviousDurable = r.Transfers[0].Durable, r.Transfers[0].Durable
				r.Transfers[n].PrefixSHA256, r.Transfers[n].RawPrefixSHA256 = r.Transfers[0].PrefixSHA256, r.Transfers[0].RawPrefixSHA256
				r.Transfers[n].Checkpoints[0].Durable, r.Transfers[n].Checkpoints[0].PrefixSHA256 = r.Transfers[0].Durable, r.Transfers[0].PrefixSHA256
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := transferControls()
			originals, err := transferOriginals(r)
			if err != nil {
				t.Fatal(err)
			}
			if err := checkTransferProgress(r, originals); err != nil {
				t.Fatal("positive raw control: ", err)
			}
			mutate(&r)
			resealTransferEvidence(&r)
			if checkTransferProgress(r, originals) == nil || checkTransfers(r) == nil {
				t.Fatal("accepted contradictory or stalled raw durable progress")
			}
		})
	}
}

// origin: S9 internal review rv-business F4
func TestBusinessOracleRequiresFinalAndPhaseContent(t *testing.T) {
	for name, mutate := range map[string]func(*businessReceipt){
		"missing_final":    func(r *businessReceipt) { r.TransferFinals = r.TransferFinals[1:] },
		"missing_terminal": func(r *businessReceipt) { r.TransferFinals[0].History.Terminal = nil },
		"duplicate_start": func(r *businessReceipt) {
			f := &r.TransferFinals[0]
			f.History.Start = append(f.History.Start, f.History.Start[0])
		},
		"incomplete_history_scan": func(r *businessReceipt) { r.TransferFinals[0].History.Complete = false },
		"wrong_terminal_bytes": func(r *businessReceipt) {
			r.TransferFinals[0].History.Terminal[0].Bytes = []byte(`{"kind":"complete"}`)
		},
		"wrong_hash":              func(r *businessReceipt) { r.TransferFinals[0].SHA256 = digest([]byte("wrong")) },
		"wrong_bytes":             func(r *businessReceipt) { r.TransferFinals[0].Bytes-- },
		"final_consumer_missing":  func(r *businessReceipt) { r.TransferFinals[0].Consumers = r.TransferFinals[0].Consumers[1:] },
		"consumer_still_present":  func(r *businessReceipt) { r.TransferFinals[0].Consumers[0].Absent = false },
		"missing_commit_lane":     func(r *businessReceipt) { r.TransferPhases = append(r.TransferPhases[:3], r.TransferPhases[4:]...) },
		"coverage_labels_only":    func(r *businessReceipt) { r.TransferPhases = nil },
		"phase_same_invocation":   func(r *businessReceipt) { r.TransferPhases[3].Original = r.TransferFinals[0].Original },
		"phase_did_not_cross_cut": func(r *businessReceipt) { r.TransferPhases[3].Before.AtNS = r.Hops[0].CutNS + 1 },
		"rename_not_observed":     func(r *businessReceipt) { r.TransferPhases[3].After.Renamed = false },
		"parent_not_synced":       func(r *businessReceipt) { r.TransferPhases[3].After.ParentSynced = false },
		"terminal_not_delivered":  func(r *businessReceipt) { r.TransferPhases[4].After.Delivered = false },
		"terminal_bytes_changed":  func(r *businessReceipt) { r.TransferPhases[4].After.Terminal.Bytes = []byte("different") },
		"no_pending_ack":          func(r *businessReceipt) { r.TransferPhases[0].Before.ACKed = r.TransferPhases[0].Before.Published },
		"no_partial_write":        func(r *businessReceipt) { r.TransferPhases[1].Before.Synced = r.TransferPhases[1].Before.Written },
		"no_synced_progress":      func(r *businessReceipt) { r.TransferPhases[2].After.Synced = r.TransferPhases[2].Before.Synced },
	} {
		t.Run(name, func(t *testing.T) {
			r := transferControls()
			if err := checkTransfers(r); err != nil {
				t.Fatal("positive final/phase control: ", err)
			}
			mutate(&r)
			// These are internally consistent bad observations, not checksum damage.
			resealTransferEvidence(&r)
			if checkTransfers(r) == nil {
				t.Fatal("accepted missing or contradictory final/phase content")
			}
		})
	}
}

// origin: S9 internal review rv-business F5
func TestBusinessOracleFreezesFourTransferQuadrants(t *testing.T) {
	for _, name := range []string{"four_quadrants", "all_small", "all_large", "duplicate_quadrant"} {
		t.Run(name, func(t *testing.T) {
			r := transferControls()
			for n := range r.Required {
				id := &r.Required[n]
				if id.Role != "transfer" {
					continue
				}
				switch name {
				case "all_small":
					id.Size = 66549440
				case "all_large":
					id.Size = 1 << 30
				case "duplicate_quadrant":
					id.Direction = "push"
				}
			}
			var sealed []businessIdentity
			for n := range r.Required {
				id := &r.Required[n]
				if id.Role != "transfer" {
					continue
				}
				budget, err := proto.TransferBudget("b", id.Size, id.RateLimitBPS)
				if err != nil {
					t.Fatal(err)
				}
				id.DeadlineNS = id.StartedNS + int64(budget)
				sealed = append(sealed, *id)
			}
			r.TransferSealSHA256 = contentDigest(sealed)
			_, err := transferOriginals(r)
			if name == "four_quadrants" && err != nil {
				t.Fatal(err)
			}
			if name != "four_quadrants" && err == nil {
				t.Fatal("accepted a missing transfer quadrant")
			}
		})
	}
	for _, name := range []string{"sealed_after_cut", "changed_seal"} {
		t.Run(name, func(t *testing.T) {
			r := transferControls()
			if name == "sealed_after_cut" {
				r.TransferSealNS = r.Hops[0].CutNS + 1
			} else {
				r.TransferSealSHA256 = digest([]byte("changed"))
			}
			if _, err := transferOriginals(r); err == nil {
				t.Fatal("accepted an unsealed or post-cut transfer manifest")
			}
		})
	}

	for _, name := range []string{"session", "bucket", "direction", "transfer_id", "changed_size_direction_assignment"} {
		t.Run("later_hop_"+name, func(t *testing.T) {
			r := transferControls()
			switch name {
			case "changed_size_direction_assignment":
				for n := range r.Transfers {
					if r.Transfers[n].Size == 66549440 {
						r.Transfers[n].Direction = "push"
					} else {
						r.Transfers[n].Direction = "pull"
					}
				}
			case "session":
				r.Transfers[12].Session = "replacement"
			case "bucket":
				r.Transfers[12].Bucket = "replacement"
			case "direction":
				r.Transfers[12].Direction = "pull"
			case "transfer_id":
				r.Transfers[12].TransferID = "replacement"
			}
			resealTransferEvidence(&r)
			if checkTransfers(r) == nil {
				t.Fatal("accepted a changed frozen tuple")
			}
		})
	}
}

func resealTransferEvidence(r *businessReceipt) {
	for n := range r.Transfers {
		r.Transfers[n].ReceiptEvidence = transferDigest(r.Transfers[n])
	}
	for n := range r.TransferFinals {
		r.TransferFinals[n].ReceiptEvidence = finalDigest(r.TransferFinals[n])
	}
	for n := range r.TransferPhases {
		if r.TransferPhases[n].Final.Original.TransferID != "" {
			r.TransferPhases[n].Final.ReceiptEvidence = finalDigest(r.TransferPhases[n].Final)
		}
		r.TransferPhases[n].ReceiptEvidence = phaseDigest(r.TransferPhases[n])
	}
}
func scaleTransferClock(r *businessReceipt, factor int64) {
	r.WindowNS *= factor
	for n := range r.Hops {
		h := &r.Hops[n]
		h.StartNS *= factor
		h.CutNS *= factor
		h.EndNS *= factor
	}
	for n := range r.Transfers {
		for k := range r.Transfers[n].Checkpoints {
			r.Transfers[n].Checkpoints[k].AtNS *= factor
		}
	}
	scaleFinal := func(f *transferFinalReceipt) {
		f.CompletedNS *= factor
		for n := range f.Checkpoints {
			f.Checkpoints[n].AtNS *= factor
		}
		for n := range f.Consumers {
			f.Consumers[n].UnsubNS *= factor
			f.Consumers[n].PONGNS *= factor
			f.Consumers[n].DeleteNS *= factor
		}
	}
	for n := range r.TransferFinals {
		scaleFinal(&r.TransferFinals[n])
	}
	for n := range r.TransferPhases {
		l := &r.TransferPhases[n]
		l.Before.AtNS *= factor
		l.After.AtNS *= factor
		scaleFinal(&l.Final)
	}
}
