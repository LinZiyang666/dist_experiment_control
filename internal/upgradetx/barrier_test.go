package upgradetx

import "testing"

// origin: S8 internal review rv3-leaves F4 F5
func TestCompletionBarrierPreservesHistoryAndCurrentCustody(t *testing.T) {
	for _, direction := range []Direction{DirectionForward, DirectionReverse} {
		for _, arm := range []string{"late-admission", "terminal", "unfinished", "missing", "origin", "retained", "old-owner", "stale-resources", "future-control", "revival", "pristine-observation"} {
			t.Run(string(direction)+"/"+arm, func(t *testing.T) {
				a, o := completionFixture(direction)
				a.Control.Stamp.Sequence--
				a.Control.Census.Stamp = a.Control.Stamp
				switch arm {
				case "late-admission":
					row := a.Resources.Census.Resources[0]
					row.ID, row.Origin = "late-request", "target-admission"
					a.Resources.Census.Resources = append(a.Resources.Census.Resources, row)
					a.Released.Census.Resources = append(a.Released.Census.Resources, row)
					o.Census.Resources = append(o.Census.Resources, row)
				case "terminal":
					a.Released.Census.Resources[0].State, a.Released.Census.Resources[0].Custody = "terminal", "released"
					o.Census.Resources[0] = a.Released.Census.Resources[0]
				case "unfinished":
					o.Census.Admitting = 1
				case "missing":
					o.Census.Resources = nil
				case "origin":
					o.Census.Resources[0].Origin = "replacement"
				case "retained":
					o.Census.Resources[0].State, o.Census.Resources[0].Custody, o.Census.Resources[0].Generation = "terminal", "retained", 1
				case "old-owner":
					o.Census.OldOwners = []Owner{sourceOwner()}
				case "stale-resources":
					a.Resources.Stamp.Sequence--
					a.Resources.Census.Stamp = a.Resources.Stamp
				case "future-control":
					a.Control.Stamp.Sequence += 2
					a.Control.Census.Stamp = a.Control.Stamp
				case "revival":
					a.Control.Census.Resources[0].State, a.Control.Census.Resources[0].Custody = "terminal", "released"
				}
				o.Current = cloneAttempt(a)
				if arm == "pristine-observation" {
					o.Current.CutStage = CutPristine
				}
				err := ValidateCompletionBarrier(a, o, o.Census)
				if arm == "late-admission" || arm == "terminal" {
					if err != nil {
						t.Fatalf("legitimate custody progress refused: %v", err)
					}
				} else {
					assertUnsettled(t, err)
				}
			})
		}
	}
}

// The final running sample is taken before the admission gate's snapshot, so
// a live resource may settle in between. Only that monotone step to terminal
// custody may separate the two censuses; any other difference between the
// sample and the gated snapshot is still an unproven barrier, in both the
// pure comparison and the completion barrier that consumes it.
// origin: S8 BROKER-EXPORTER F1 (S8 FIX-DESCRIBE finding F1); plan §4.2
func TestCompletionBarrierAcceptsOnlyTerminalSettlementSinceTheRunningSample(t *testing.T) {
	cases := []struct {
		arm     string
		allowed bool
		mutate  func(earlier, later *Census)
	}{
		{"unchanged", true, func(*Census, *Census) {}},
		{"settled", true, func(_, later *Census) {
			later.Resources[0].State, later.Resources[0].Custody = "terminal", "retained"
		}},
		{"released", true, func(earlier, later *Census) {
			earlier.Resources[0].State, earlier.Resources[0].Custody = "terminal", "retained"
			later.Resources[0].State, later.Resources[0].Custody = "terminal", "released"
		}},
		{"resumed", false, func(earlier, _ *Census) {
			earlier.Resources[0].State, earlier.Resources[0].Custody = "terminal", "retained"
		}},
		{"released-reopened", false, func(earlier, later *Census) {
			earlier.Resources[0].State, earlier.Resources[0].Custody = "terminal", "released"
			later.Resources[0].State, later.Resources[0].Custody = "terminal", "retained"
		}},
		{"custody-only", false, func(_, later *Census) { later.Resources[0].Custody = "retained" }},
		{"appeared", false, func(_, later *Census) {
			row := later.Resources[0]
			row.ID, row.Origin = "late-request", "unadmitted"
			later.Resources = append(later.Resources, row)
		}},
		{"vanished", false, func(_, later *Census) { later.Resources = nil }},
		{"origin", false, func(_, later *Census) { later.Resources[0].Origin = "replacement" }},
		{"owner", false, func(_, later *Census) { later.Resources[0].Owner = sourceOwner() }},
		{"generation", false, func(_, later *Census) {
			later.Resources[0].Generation = 1
			later.Resources[0].State, later.Resources[0].Custody = "terminal", "retained"
		}},
		{"stamp", false, func(_, later *Census) { later.Stamp.Nonce = testID(999) }},
		{"admitting", false, func(_, later *Census) { later.Admitting = 1 }},
		{"old-owner", false, func(_, later *Census) { later.OldOwners = []Owner{sourceOwner()} }},
		{"duplicate", false, func(earlier, later *Census) {
			earlier.Resources = append(earlier.Resources, earlier.Resources[0])
			later.Resources = append(later.Resources, later.Resources[0])
		}},
	}
	for _, direction := range []Direction{DirectionForward, DirectionReverse} {
		for _, c := range cases {
			t.Run(string(direction)+"/"+c.arm, func(t *testing.T) {
				a, o := completionFixture(direction)
				earlier, later := cloneReceipt(Receipt{Census: o.Census}).Census, cloneReceipt(Receipt{Census: o.Census}).Census
				c.mutate(&earlier, &later)
				if got := CensusProgressed(earlier, later); got != c.allowed {
					t.Fatalf("CensusProgressed = %t, want %t", got, c.allowed)
				}
				o.Census = earlier
				err := ValidateCompletionBarrier(a, o, later)
				if c.allowed && err != nil {
					t.Fatalf("settlement after the running sample refused completion: %v", err)
				}
				if !c.allowed {
					assertUnsettled(t, err)
				}
			})
		}
	}
}
