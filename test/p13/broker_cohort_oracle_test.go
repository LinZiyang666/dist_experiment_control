//go:build linux && p13_integration

package p13_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"syscall"

	"github.com/LinZiyang666/tether/internal/listenerimage"
)

// Keep synthetic oracle inputs separate from real cohort scheduling; these
// values test the consumer and never claim a successful product cut.
// origin: hot-update S6 plan §11 T48 / S10 plan §10.1
func brokerCohortOracleInputs() (brokerCohortReceipt, brokerCohortReceipt, []brokerCohortSample) {
	rc, emptyDigest := 0, ""
	before := brokerCohortReceipt{Claim: "original-cohort", TestOrArm: "T48", Command: "sandbox broker reload", NativeRC: &rc, Verdict: "GREEN",
		PID: 42, StartTicks: 73, SourceSHA: strings.Repeat("a", 40), ArtifactSHA: strings.Repeat("b", 64), PreviousSHA: strings.Repeat("b", 64),
		WallSeconds: 1, PhaseIntersections: json.RawMessage("[]"), MutationDigest: &emptyDigest, StdoutPath: "stdout.log", StderrPath: "stderr.log", LogMapping: json.RawMessage("{}")}
	before.OracleInputs.FlowLedger = "flows.jsonl"
	before.Counters = brokerCohortCounters{Generation: 1, RunningSHA: strings.Repeat("b", 64), AgentPID: 21, AgentTicks: 31, AgentSHA: strings.Repeat("c", 64)}
	before.Counters.Listeners = []listenerimage.Descriptor{{ID: "public.listener", Device: 1, Inode: 2, Cookie: 3, Nonblock: true, CloseOnExec: true}}
	for i, protocol := range []string{"ssh", "tcp", "tcp", "tcp", "tcp", "ss"} {
		stream := brokerCohortStream{ID: fmt.Sprintf("stream-%d", i), Protocol: protocol, PublicPID: 100 + i, PublicFD: 3, TargetPID: 200 + i, TargetFD: 4,
			AppPID: 300 + i, AppTicks: uint64(400 + i), TargetAccepts: 1, FinalSequence: 40, HalfClosed: i == 1}
		if protocol == "ssh" {
			stream.AppSHA = strings.Repeat("e", 64)
		}
		if protocol == "ss" {
			stream.SSHeaders = 1
		}
		if stream.HalfClosed {
			stream.HalfCloseSequence, stream.FinalSequence = 20, 600
		}
		before.Counters.Streams = append(before.Counters.Streams, stream)
	}
	after := before
	after.PreviousSHA = before.Counters.RunningSHA
	after.Counters.Generation, after.Counters.RunningSHA = 2, strings.Repeat("d", 64)
	after.ArtifactSHA = after.Counters.RunningSHA
	after.Counters.CutBootNS, after.Counters.ReadyBootNS = 1000, 2000
	before.Counters.PendingOpen = brokerCohortOpening{Phase: "connecting", AgentPID: before.Counters.AgentPID, AgentFD: 7, ClientPID: 50, ClientFD: 8, Opened: 900,
		AgentSocket: brokerCohortSocket{Inode: 11, Mode: syscall.S_IFSOCK}, ClientSocket: brokerCohortSocket{Inode: 12, Mode: syscall.S_IFSOCK}, BrokerSocket: brokerCohortSocket{Inode: 13, Mode: syscall.S_IFSOCK}}
	after.Counters.PendingOpen = before.Counters.PendingOpen
	after.Counters.PendingOpen.Phase = "connected"
	after.Counters.PendingOpen.TargetAccepts = 1
	after.Counters.PendingOpen.Started = 1100
	after.Counters.PendingOpen.Finished = 2100
	after.Counters.PendingOpen.PayloadSHA = fmt.Sprintf("%x", sha256.Sum256([]byte("tether-broker-original-connecting-open-v1")))
	after.Counters.ExpectedSlots = 1
	after.Counters.Connects = []brokerCohortConnect{{Slot: 0, StartedBootNS: 900, FinishedBootNS: 2100, NativeRC: &rc}}
	var samples []brokerCohortSample
	for _, stream := range after.Counters.Streams {
		for direction := uint8(0); direction < 2; direction++ {
			end := stream.FinalSequence
			if stream.HalfClosed && direction == 0 {
				end = stream.HalfCloseSequence
			}
			for sequence := uint64(0); sequence < end; sequence++ {
				ns := int64(10 + sequence*10)
				if sequence >= 20 {
					ns += after.Counters.ReadyBootNS
				}
				samples = append(samples, brokerCohortSample{ID: stream.ID, Direction: direction, Sequence: sequence,
					PayloadSHA: brokerCohortPayloadSHA(stream.ID, direction, sequence), StartedBootNS: ns, FinishedBootNS: ns + 1})
			}
			fin := after.Counters.ReadyBootNS + int64(end)*10 + 10
			if stream.HalfClosed && direction == 0 {
				fin = after.Counters.CutBootNS - 10
			}
			samples = append(samples, brokerCohortSample{ID: stream.ID, Direction: direction, Sequence: end,
				StartedBootNS: fin, FinishedBootNS: fin + 1, EOF: true})
		}
	}
	return before, after, samples
}
