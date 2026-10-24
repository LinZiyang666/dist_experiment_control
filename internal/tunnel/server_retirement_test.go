package tunnel

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: S8 internal review P13-COHORT F2; hot-update S8 plan §4.2
func TestImportedAllocationRetirementRequiresItsSealedFenceAndJoinedOwners(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseImportedAllocationRetirement(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "imported allocation retirement x5", g)
	testharness.AssertNoFDLeak(t, "imported allocation retirement x5", fds, 0)
}

func exerciseImportedAllocationRetirement(t *testing.T) {
	t.Helper()
	info := SessionInfo{PublicPort: 4100, SID: "lab", NID: "agent", TokenHash: hashToken("original-token")}
	key := sessionFenceKeyFor(info.PublicPort, info.SID, info.NID, info.TokenHash)
	for _, fault := range []string{"none", "unsealed", "unsealed-close", "live-close", "wrong-token", "inflight", "endpoint", "listener", "corrupt-fence"} {
		t.Run(fault, func(t *testing.T) {
			s := &Server{sessions: map[int]*serverSession{}, closedAllocation: map[sessionFenceKey]bool{key: true}, inflightByAllocation: map[sessionFenceKey]int{}}
			record, err := snapshotServerFences(s)
			if err != nil {
				t.Fatal(err)
			}
			s.importedImage = &serverImportedImage{state: serverStateSnapshot{Records: []sealedstate.Record{record}}}
			want := info
			switch fault {
			case "unsealed":
				s.importedImage = nil
			case "live-close":
				s.closedAllocation[key] = false
			case "unsealed-close":
				for i := range s.importedImage.state.Records[0].Fields {
					if s.importedImage.state.Records[0].Fields[i].Name == "closed_allocation" {
						s.importedImage.state.Records[0].Fields[i].Bytes = serverMapBytes(map[string]uint64{})
					}
				}
			case "wrong-token":
				want.TokenHash = hashToken("replacement-token")
			case "inflight":
				s.inflightByAllocation[key] = 1
			case "endpoint":
				s.flowState = &serverFlowState{endpoints: map[int]*flowEndpoint{4100: {allocation: Allocation{Port: 4100, SID: info.SID, NID: info.NID, TokenHash: info.TokenHash}}}}
			case "listener":
				s.sessions[4100] = &serverSession{}
			case "corrupt-fence":
				for i := range s.importedImage.state.Records[0].Fields {
					if s.importedImage.state.Records[0].Fields[i].Name == "closed_allocation" {
						s.importedImage.state.Records[0].Fields[i].Bytes = []byte("invalid fence")
					}
				}
			}
			if got := ServerAllocationReleased(s, want); got != (fault == "none") {
				t.Fatalf("retirement=%t for %s; missing ownership is not a terminal proof", got, fault)
			}
		})
	}
}
