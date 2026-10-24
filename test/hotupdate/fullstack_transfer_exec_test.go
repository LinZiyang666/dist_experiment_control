//go:build linux && hotupdate_integration

package hotupdate_test

// Real: workspace-built ctl/agent/broker, file-store JetStream, custody and OFD
// readers. The small binary witnesses exercise transfer mechanisms; the frozen
// C0/C1 geometry is checked separately and is executed by drill 99, not relabeled
// as a sub-600-second integration run. Missing product supply fails closed.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/fullstackharness"
	"github.com/nats-io/nats.go"
)

// Separate supply from controls so unavailable S7/S8 wiring cannot hide a
// runnable contract regression. A filtered controls run is not a live receipt.
// origin: hot-update S10 plan §3.3 / §10.2 P23 / §11.2
func TestFullstackTransferCohortsRespectOriginalBudget(t *testing.T) {
	t.Run("contracts", fullstackTransferBudgetControls)
	t.Run("required-cells", fullstackTransferRequiredCuts)
	var artifacts transferArtifacts
	artifactDir := t.TempDir()
	if !t.Run("supply", func(t *testing.T) {
		artifacts = buildFullstackTransferArtifacts(t, artifactDir)
		s := newTransferStack(t, artifacts)
		s.requireCapable()
	}) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	started := time.Now()
	live := false
	t.Run("rounds", func(t *testing.T) {
		live = true
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseTransferCohort(t, artifacts)
			})
		}
	})
	if !live {
		return
	} // A controls-only selection is not a five-cycle receipt.
	testharness.AssertNoGoroutineLeak(t, "fullstack transfer cohorts", g)
	testharness.AssertNoFDLeak(t, "fullstack transfer cohorts", fd, 0)
	t.Logf("five-cycle including supervisor join=%s; full-size C0/C1 belongs to 99", time.Since(started))
}

// Five late-frame windows overlap; serializing them would exceed the frozen
// 600s partition. Product owners stay alive until both fresh drains and scans.
// origin: hot-update S10 plan §5.4 / §10.2 P23 / §11.2
func TestFullstackTransferSealsLateTerminal(t *testing.T) {
	t.Run("contracts", fullstackTransferSealControls)
	t.Run("required-cells", fullstackTransferRequiredCuts)
	var artifacts transferArtifacts
	artifactDir := t.TempDir()
	if !t.Run("supply", func(t *testing.T) {
		artifacts = buildFullstackTransferArtifacts(t, artifactDir)
		s := newTransferStack(t, artifacts)
		s.requireCapable()
	}) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	started := time.Now()
	live := false
	t.Run("rounds", func(t *testing.T) {
		live = true
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseTransferEndSeal(t, artifacts)
			})
		}
	})
	if !live {
		return
	} // A controls-only selection is not a five-cycle receipt.
	testharness.AssertNoGoroutineLeak(t, "fullstack transfer end seal", g)
	testharness.AssertNoFDLeak(t, "fullstack transfer end seal", fd, 0)
	t.Logf("five-cycle including default dedupe window and supervisor join=%s", time.Since(started))
}

type fullstackTransferCut struct{ topology, cohort, trajectory, leg, direction, component, action, runtime string }

func (c fullstackTransferCut) name() string {
	return strings.Join([]string{c.topology, c.cohort, c.trajectory, c.leg, c.runtime, c.direction, c.component, c.action}, "/")
}

// Keep the frozen trajectory, including mixed vectors and the infra-only pin.
// Expanding each step across all four original calls and three topologies keeps
// a missing supplier visible; a same-store restart cannot satisfy a binary cut.
// origin: S10 internal review rv3-assembly F6 / S10 plan §2 / §3.3 / P23
func fullstackTransferCuts() []fullstackTransferCut {
	steps := []struct{ cohort, trajectory, leg, components, actions string }{
		{"C0", "R1", "leg1", "agent broker nats caddy", "forward forward forward forward"},
		{"C0", "R2", "leg2", "caddy nats broker agent", "reverse reverse reverse reverse"},
		{"C0", "R3", "leg2", "agent broker nats caddy", "reupgrade reupgrade reupgrade reupgrade"},
		{"C1", "R4", "leg1", "caddy nats agent broker", "reverse reverse forward forward"},
		{"C1", "R5", "leg2", "agent broker nats caddy", "forward forward forward forward"},
		{"C1", "R6", "leg2", "caddy nats agent broker", "reverse reverse forward forward"},
		{"C1", "R7", "leg2", "nats caddy", "forward forward"},
	}
	var cells []fullstackTransferCut
	for _, topology := range []string{"single", "sole-voter", "n3"} {
		for runtime, direction := range []string{"push", "pull", "push", "pull"} {
			for _, step := range steps {
				actions := strings.Fields(step.actions)
				for i, component := range strings.Fields(step.components) {
					cells = append(cells, fullstackTransferCut{topology, step.cohort, step.trajectory, step.leg, direction, component, actions[i], fmt.Sprintf("a%d", runtime)})
				}
			}
		}
	}
	return cells
}

// Each cell fails at its actual missing supplier, without suppressing the
// independent runnable mechanism rounds. Their two-artifact isolated cuts do
// not certify the ordered fullstack trajectory or novel T1..T5 supply.
// origin: S10 internal review rv3-assembly F6 / S10 plan §3.3 / P23
func fullstackTransferRequiredCuts(t *testing.T) {
	for _, cell := range fullstackTransferCuts() {
		t.Run(cell.name(), func(t *testing.T) {
			if cell.topology != "single" {
				t.Fatalf("SETUP-RED: NOT_RUN pending S10 P23 ordered clustered cohort supplier and P25 live assembly: %s needs per-hop original-origin receipts across the full trajectory", cell.name())
			}
			if cell.component == "nats" || cell.component == "caddy" {
				t.Fatalf("SETUP-RED: NOT_RUN pending S10 P15 physical transfer ingress/retirement receipts and P25 live assembly: %s needs actual infra binary cuts on the original attachment", cell.name())
			}
			if cell.component == "agent" {
				t.Fatalf("SETUP-RED: NOT_RUN pending S10 P23 ordered same-ID cohort supplier and P18 native CLI adapter: %s needs the full authorized trajectory", cell.name())
			}
			t.Fatalf("SETUP-RED: NOT_RUN pending S10 P25 live assembly and P23 ordered same-ID cohort supplier: %s needs immutable T1..T5 supply and full component completion; isolated S6 broker exec is a separate mechanism witness", cell.name())
		})
	}
}

// An independent literal pins every ordered step and role; deletion, duplicate,
// reordering or moving re-upgrade onto C1 must be rejected before product IO.
// origin: S10 internal review rv3-assembly F6 / S10 plan §2 / §3.3 / P23
func fullstackTransferDenominatorControls(t *testing.T) {
	want := []string{
		"C0/R1/leg1/agent/forward", "C0/R1/leg1/broker/forward", "C0/R1/leg1/nats/forward", "C0/R1/leg1/caddy/forward",
		"C0/R2/leg2/caddy/reverse", "C0/R2/leg2/nats/reverse", "C0/R2/leg2/broker/reverse", "C0/R2/leg2/agent/reverse",
		"C0/R3/leg2/agent/reupgrade", "C0/R3/leg2/broker/reupgrade", "C0/R3/leg2/nats/reupgrade", "C0/R3/leg2/caddy/reupgrade",
		"C1/R4/leg1/caddy/reverse", "C1/R4/leg1/nats/reverse", "C1/R4/leg1/agent/forward", "C1/R4/leg1/broker/forward",
		"C1/R5/leg2/agent/forward", "C1/R5/leg2/broker/forward", "C1/R5/leg2/nats/forward", "C1/R5/leg2/caddy/forward",
		"C1/R6/leg2/caddy/reverse", "C1/R6/leg2/nats/reverse", "C1/R6/leg2/agent/forward", "C1/R6/leg2/broker/forward",
		"C1/R7/leg2/nats/forward", "C1/R7/leg2/caddy/forward",
	}
	cells := fullstackTransferCuts()
	if len(cells) != 312 {
		t.Fatalf("layer/trajectory denominator lost cells: got %d want 312", len(cells))
	}
	seen := make(map[string]bool)
	for _, topology := range []string{"single", "sole-voter", "n3"} {
		for runtime, direction := range []string{"push", "pull", "push", "pull"} {
			var got []string
			for _, cell := range cells {
				if cell.topology == topology && cell.direction == direction && cell.runtime == fmt.Sprintf("a%d", runtime) {
					if seen[cell.name()] {
						t.Fatal("duplicate fullstack transfer cell")
					}
					seen[cell.name()] = true
					got = append(got, strings.Join([]string{cell.cohort, cell.trajectory, cell.leg, cell.component, cell.action}, "/"))
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s/%s lost frozen ordered sender/receiver trajectory: %v", topology, direction, got)
			}
		}
	}
}

// Controls below use fabricated evidence ONLY to exercise rejection predicates.
// They do not count as M02X/M03X/M05X/M08X/M11X production mutation receipts.
// origin: hot-update S10 plan §8 / §10.2 P23
func fullstackTransferBudgetControls(t *testing.T) {
	t.Run("layer-trajectory-denominator", fullstackTransferDenominatorControls)
	t.Run("cohort-and-image-freshness", func(t *testing.T) {
		row := x.Observation{TransferID: "C0", Kind: x.ObserveSynced, FileSynced: true,
			DurableOffset: 65536, HashStateDigest: strings.Repeat("a", 64), Generation: 1, EventSeq: 100}
		previous := map[string]bool{"C0": true}
		if fullstackTransferBoundaryIsNew(row, previous, "synced-prefix") {
			t.Fatal("C1 reused a retained C0 progress boundary")
		}
		next := row
		next.TransferID, next.Generation, next.EventSeq = "C1", 2, 1
		if !fullstackTransferBoundaryIsNew(next, previous, "synced-prefix") || !fullstackTransferOwnerIsNewer(next, row) || fullstackTransferOwnerIsNewer(row, next) {
			t.Fatal("new cohort or current image lost to an old event sequence")
		}
	})
	t.Run("frozen-cohorts", func(t *testing.T) {
		for _, cohort := range []string{"C0", "C1"} {
			rows, err := fullstackharness.TransferCohortPlan(cohort)
			transferMust(t, err)
			if len(rows) != 4 {
				t.Fatal("cohort lost one of four independent runtimes")
			}
			for i, row := range rows {
				size, rate, seconds := int64(66549440), int64(65536), int64(2092)
				if i >= 2 {
					size, rate, seconds = 1<<30, 1<<20, 2108
				}
				direction := "push"
				if i%2 == 1 {
					direction = "pull"
				}
				want := fullstackharness.TransferWorkload{Runtime: fmt.Sprintf("a%d", i), Cohort: cohort,
					Direction: direction, Tier: "B", Size: size, RateLimit: rate, BudgetNS: seconds * int64(time.Second)}
				if row != want {
					t.Fatalf("frozen cohort geometry changed: got %+v want %+v", row, want)
				}
				budget, err := proto.TransferBudget("b", row.Size, row.RateLimit)
				transferMust(t, err)
				if int64(budget) != row.BudgetNS {
					t.Fatal("fixture budget differs from actual product admission")
				}
			}
		}
		if _, err := fullstackharness.TransferCohortPlan("C2"); err == nil {
			t.Fatal("unregistered replacement cohort accepted")
		}
		if _, err := proto.TransferBudget("b", 1<<30, (1<<20)-1); err == nil {
			t.Fatal("original maximum budget was clamped instead of refused")
		}
	})
	t.Run("status", func(t *testing.T) {
		original := fullstackTransferStatusFixture()
		current := original
		current.Budget.RemainingNS -= int64(time.Second)
		current.DurableOffset += 32768
		current.ChunkIndex++
		current.LastConsumedStreamSeq++
		transferMust(t, fullstackCheckTransferStatus(original, current))
		for _, tc := range []struct {
			name string
			edit func(*x.Status)
		}{
			{"renew-start", func(s *x.Status) { s.Budget.StartedAt = s.Budget.StartedAt.Add(time.Second) }},
			{"renew-effective-budget", func(s *x.Status) { s.Budget.EffectiveBudgetNS++ }},
			{"renew-remaining", func(s *x.Status) { s.Budget.RemainingNS = original.Budget.RemainingNS + 1 }},
			{"replace-origin", func(s *x.Status) { s.Origin.Generation++ }},
			{"M05X-new-entry", func(s *x.Status) { s.Identity.EntryNonce = "replacement" }},
			{"replace-NUID", func(s *x.Status) { s.Identity.ObjectVersion = "replacement" }},
			{"unknown", func(s *x.Status) { s.State = x.StatusUnknown }},
			{"unsealed", func(s *x.Status) { s.Budget.SourceSealed = false }},
			{"M02X-prefix-lost", func(s *x.Status) { s.DurableOffset = 0 }},
			{"physical-sequence-lost", func(s *x.Status) { s.LastConsumedStreamSeq = 0 }},
			{"unconfirmed-terminal", func(s *x.Status) { s.State = x.StatusTerminal }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				bad := current
				tc.edit(&bad)
				if fullstackCheckTransferStatus(original, bad) == nil {
					t.Fatal("altered original transfer status accepted")
				}
			})
		}
	})
	t.Run("checkpoint", func(t *testing.T) {
		before, after := fullstackTransferCheckpointFixture()
		transferMust(t, fullstackCheckTransferCheckpoint(before, after))
		for _, tc := range []struct {
			name string
			edit func(*x.Observation)
		}{
			{"M02X-sync-prefix", func(o *x.Observation) { o.DurableOffset = 0 }},
			{"M03X-offset-plus-one", func(o *x.Observation) { o.DurableOffset++ }},
			{"M03X-offset-minus-one", func(o *x.Observation) { o.DurableOffset-- }},
			{"hash-state", func(o *x.Observation) { o.HashStateDigest = "different" }},
			{"sequence", func(o *x.Observation) { o.LastConsumedStreamSeq++ }},
			{"new-tmp", func(o *x.Observation) { o.Tmp.ResourceID = "replacement" }},
			{"new-parent", func(o *x.Observation) { o.Parent.ResourceID = "replacement" }},
			{"new-process", func(o *x.Observation) { o.ProcessStart++ }},
			{"new-deadline", func(o *x.Observation) { o.OriginalDeadline.Deadline++ }},
			{"same-generation", func(o *x.Observation) { o.Generation = before.Generation }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				bad := after
				tc.edit(&bad)
				if fullstackCheckTransferCheckpoint(before, bad) == nil {
					t.Fatal("altered frozen receiver checkpoint accepted")
				}
			})
		}
	})
	t.Run("full-observation-projection", func(t *testing.T) {
		row, _ := fullstackTransferCheckpointFixture()
		event, err := drillobserve.EncodeTransfer(row)
		transferMust(t, err)
		projected, err := fullstackharness.DecodeTransferObservation(event.Data, "sealed-batch/transfer")
		transferMust(t, err)
		if !reflect.DeepEqual(row, projected.Value) || !bytes.Equal(projected.Raw, event.Data) {
			t.Fatal("S7 observation fields or original bytes were discarded")
		}
		poison := bytes.Replace(event.Data, []byte(`"durable_offset":65536`), []byte(`"durable_offset":65536,"durable_offset":65537`), 1)
		if bytes.Equal(poison, event.Data) {
			t.Fatal("duplicate-field control missed the actual wire field")
		}
		if _, err := fullstackharness.DecodeTransferObservation(poison, "sealed-batch/poison"); err == nil {
			t.Fatal("duplicate checkpoint field accepted")
		}
	})
	t.Run("M05X-tracker-population", func(t *testing.T) {
		s := fullstackTransferStatusFixture()
		row := x.LedgerRow{Identity: s.Identity, Origin: s.Origin, EntryNonce: s.Identity.EntryNonce,
			Revision: 1, BudgetSeal: s.Budget}
		state := x.TrackerState{Schema: x.StateSchema, Generation: 1, Entries: []x.LedgerRow{row}}
		transferMust(t, fullstackharness.ValidateTransferTrackerState(state, []x.Status{s}))
		state.Entries = append(state.Entries, row)
		if fullstackharness.ValidateTransferTrackerState(state, []x.Status{s, s}) == nil {
			t.Fatal("duplicate permit/entry accepted by the tracker projection")
		}
	})
	t.Run("M05X-original-calls", func(t *testing.T) {
		request := &nats.Msg{Subject: "original.prepare.req", Data: []byte(`{"transfer_id":"transfer","call_id":"original"}`)}
		s := &transferStack{wire: []*nats.Msg{request, request}}
		transferMust(t, fullstackCheckTransferCalls(s, "transfer"))
		for _, body := range []string{`{"transfer_id":"replacement","call_id":"original"}`, `{"transfer_id":"transfer","call_id":"replacement"}`} {
			s.wire = []*nats.Msg{request, {Subject: request.Subject, Data: []byte(body)}}
			if fullstackCheckTransferCalls(s, "transfer") == nil {
				t.Fatal("replacement permit/creation invocation accepted")
			}
		}
	})
}

func fullstackTransferSealControls(t *testing.T) {
	t.Run("bounded-wire-frames", func(t *testing.T) {
		for _, frame := range []string{"PING\r\n", "PUB history reply 2\r\n{}\r\n", "HPUB history reply 12 14\r\nNATS/1.0\r\n\r\n{}\r\n", "MSG reply 1 2\r\n{}\r\n"} {
			got, _, err := fullstackTransferFrame(bufio.NewReader(strings.NewReader(frame)))
			transferMust(t, err)
			if string(got) != frame {
				t.Fatal("relay changed original wire bytes")
			}
		}
		for _, frame := range []string{"PUB history reply -1\r\n", "PUB history reply 12582913\r\n", "PUB history reply 2\r\n{", "MSG reply 1 2\r\n{}xx"} {
			if _, _, err := fullstackTransferFrame(bufio.NewReader(strings.NewReader(frame))); err == nil {
				t.Fatal("malformed publication frame accepted")
			}
		}
	})
	t.Run("once-primitive", func(t *testing.T) {
		rows := fullstackTransferEffectFixture()
		transferMust(t, fullstackCheckTransferEffects(rows, "transfer"))
		for _, tc := range []struct {
			name string
			rows []x.Observation
		}{
			{"M11X-duplicate-enter", append(append([]x.Observation(nil), rows...), rows[0])},
			{"M11X-duplicate-landed", append(append([]x.Observation(nil), rows...), rows[1])},
			{"missing-directory-sync", rows[:2]},
			{"wrong-sync-owner", func() []x.Observation { r := append([]x.Observation(nil), rows...); r[2].PID++; return r }()},
			{"sync-before-primitive", func() []x.Observation {
				r := append([]x.Observation(nil), rows...)
				r[2].BoottimeNS = r[0].BoottimeNS
				return r
			}()},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if fullstackCheckTransferEffects(tc.rows, "transfer") == nil {
					t.Fatal("missing or repeated durable receiver primitive accepted")
				}
			})
		}
	})
	t.Run("two-fresh-seals", func(t *testing.T) {
		first := fullstackTransferSeal{nonce: "one", at: 1, cursor: 10, acks: map[string]uint64{"agent": 8, "broker": 9}, history: []byte("canonical physical history")}
		second := fullstackTransferSeal{nonce: "two", at: 2, cursor: 10, acks: map[string]uint64{"agent": 10, "broker": 11}, history: bytes.Clone(first.history)}
		transferMust(t, fullstackCheckTransferSeals(first, second))
		for _, tc := range []struct {
			name string
			edit func(*fullstackTransferSeal)
		}{
			{"replayed-nonce", func(s *fullstackTransferSeal) { s.nonce = first.nonce }},
			{"replayed-ACK", func(s *fullstackTransferSeal) { s.acks["agent"] = 8 }},
			{"missing-owner", func(s *fullstackTransferSeal) { delete(s.acks, "broker") }},
			{"late-opposite-terminal", func(s *fullstackTransferSeal) { s.history = append(s.history, []byte("failed")...) }},
			{"cursor-reversed", func(s *fullstackTransferSeal) { s.cursor-- }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				bad := second
				bad.acks = map[string]uint64{"agent": 10, "broker": 11}
				bad.history = bytes.Clone(second.history)
				tc.edit(&bad)
				if fullstackCheckTransferSeals(first, bad) == nil {
					t.Fatal("unsealed or changed final history accepted")
				}
			})
		}
	})
	t.Run("M08X-consumer-retirement", func(t *testing.T) {
		transferMust(t, fullstackCheckTransferConsumers([]string{"old"}, nil))
		for _, consumers := range [][]string{{"old"}, {"replacement"}} {
			if fullstackCheckTransferConsumers([]string{"old"}, consumers) == nil {
				t.Fatal("one remaining transfer consumer accepted after retirement")
			}
		}
		if fullstackCheckTransferConsumers(nil, nil) == nil {
			t.Fatal("zero-consumer lifecycle certified without a live consumer")
		}
	})
}

type fullstackHeldTransferFrame struct {
	bytes    []byte
	reply    string
	conn     net.Conn
	writeMu  *sync.Mutex
	accepted time.Time
}

type fullstackTransferRelay struct {
	listener    net.Listener
	mu          sync.Mutex
	wg          sync.WaitGroup
	accepted    chan struct{}
	connections []net.Conn
	first, late *fullstackHeldTransferFrame
	err         error
}

// The fixture inserts this TCP relay BEFORE starting any business. Moving the
// embedded server's listener here is supply, not a claimed S9 cut. Once a
// transfer starts there is no process replacement or owner repair by the test.
// origin: hot-update S10 plan §5.4 / §6 / P23
func fullstackLateTransferRelay(t *testing.T, s *transferStack) *fullstackTransferRelay {
	t.Helper()
	address := strings.TrimPrefix(s.url, "nats://")
	s.ns.Shutdown()
	s.ns.WaitForShutdown()
	s.options.Port = -1
	s.startJS()
	listener, err := net.Listen("tcp", address)
	transferMust(t, err)
	r := &fullstackTransferRelay{listener: listener, accepted: make(chan struct{})}
	upstream, subject := strings.TrimPrefix(s.ns.ClientURL(), "nats://"), proto.SubjAuditTransfer(s.sid)
	go func() {
		defer close(r.accepted)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := net.DialTimeout("tcp", upstream, time.Second)
			if err != nil {
				_ = client.Close()
				r.mu.Lock()
				r.err = err
				r.mu.Unlock()
				return
			}
			r.mu.Lock()
			r.connections = append(r.connections, client, server)
			r.mu.Unlock()
			r.pipe(client, server, subject)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-r.accepted
		r.mu.Lock()
		for _, conn := range r.connections {
			_ = conn.Close()
		}
		r.mu.Unlock()
		r.wg.Wait()
	})
	// The oracle's PONG proves only its own attachment. Wait for the actual
	// broker ingress and original agent to restore their subscriptions too.
	// origin: S7 internal review INTEG-M F6
	if !testharness.WaitFor(t, 10*time.Second, 10*time.Millisecond, func() bool {
		return s.nc.FlushTimeout(100*time.Millisecond) == nil && transferProductionSubscriptionsReady(s)
	}) {
		t.Fatal("SETUP-RED2: pre-business NATS relay did not become ready")
	}
	s.requireCapable()
	return r
}

func (r *fullstackTransferRelay) pipe(client, server net.Conn, subject string) {
	writeMu := new(sync.Mutex)
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		defer func() { _ = server.Close() }()
		reader := bufio.NewReader(client)
		for {
			frame, fields, err := fullstackTransferFrame(reader)
			if err != nil {
				return
			}
			if len(fields) >= 4 && (fields[0] == "PUB" || fields[0] == "HPUB") && fields[1] == subject {
				payload := frame[bytes.Index(frame, []byte("\r\n"))+2 : len(frame)-2]
				if fields[0] == "HPUB" {
					n, err := strconv.Atoi(fields[len(fields)-2])
					if err != nil || n < 0 || n > len(payload) {
						r.mu.Lock()
						r.err = errors.New("invalid transfer HPUB header")
						r.mu.Unlock()
						return
					}
					payload = payload[n:]
				}
				var record struct {
					Kind string `json:"kind"`
				}
				if json.Unmarshal(payload, &record) == nil && record.Kind == "complete" && r.hold(frame, fields[2], server, writeMu) {
					continue
				}
			}
			writeMu.Lock()
			_, err = server.Write(frame)
			writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	go func() {
		defer r.wg.Done()
		defer func() { _ = client.Close() }()
		reader := bufio.NewReader(server)
		for {
			frame, fields, err := fullstackTransferFrame(reader)
			if err != nil {
				return
			}
			if len(fields) >= 4 && fields[0] == "MSG" {
				r.mu.Lock()
				if r.first != nil && fields[1] == r.first.reply {
					var ack struct {
						Sequence uint64          `json:"seq"`
						Error    json.RawMessage `json:"error"`
					}
					payload := frame[bytes.Index(frame, []byte("\r\n"))+2 : len(frame)-2]
					if json.Unmarshal(payload, &ack) != nil || ack.Sequence == 0 || len(ack.Error) != 0 {
						r.err = errors.New("original held terminal was not accepted")
					} else {
						r.first.accepted = time.Now()
					}
					r.mu.Unlock()
					continue // Lose the real ACK; never fabricate a response.
				}
				r.mu.Unlock()
			}
			if _, err := client.Write(frame); err != nil {
				return
			}
		}
	}()
}

func (r *fullstackTransferRelay) hold(frame []byte, reply string, server net.Conn, writeMu *sync.Mutex) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	held := &fullstackHeldTransferFrame{bytes: bytes.Clone(frame), reply: reply, conn: server, writeMu: writeMu}
	if r.first == nil {
		r.first = held
		return true
	}
	if r.late != nil {
		return false
	}
	r.late = held
	// Both original and retry have actually left the publisher's socket while
	// history still reports absence. Accept one, retain the other past expiry.
	r.first.writeMu.Lock()
	_, r.err = r.first.conn.Write(r.first.bytes)
	r.first.writeMu.Unlock()
	return true
}

func (r *fullstackTransferRelay) pending(t *testing.T) fullstackHeldTransferFrame {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	transferMust(t, r.err)
	if r.first == nil || r.late == nil || r.first.accepted.IsZero() {
		t.Fatal("SETUP-RED2: NOT_RUN pending S7 P12/P14/P15/P28 two sent terminal attempts and real acceptance ACK")
	}
	for _, frame := range []*fullstackHeldTransferFrame{r.first, r.late} {
		if !bytes.Contains(frame.bytes, []byte("Nats-Expected-Last-Subject-Sequence:")) {
			t.Fatal("terminal attempt has no frozen subject-sequence fence")
		}
	}
	late := *r.late
	late.accepted = r.first.accepted
	return late
}

func (r *fullstackTransferRelay) releaseLate(t *testing.T, nc *nats.Conn, frame fullstackHeldTransferFrame) {
	t.Helper()
	if time.Since(frame.accepted) < 125*time.Second {
		t.Fatal("late publication did not cross default dedupe expiry")
	}
	sub, err := nc.SubscribeSync(frame.reply)
	transferMust(t, err)
	defer func() { transferMust(t, sub.Unsubscribe()) }()
	transferMust(t, nc.FlushTimeout(time.Second))
	frame.writeMu.Lock()
	_, err = frame.conn.Write(frame.bytes)
	frame.writeMu.Unlock()
	transferMust(t, err)
	response, err := sub.NextMsg(5 * time.Second)
	transferMust(t, err)
	var ack struct {
		Error *struct {
			Code int `json:"err_code"`
		} `json:"error"`
	}
	transferMust(t, json.Unmarshal(response.Data, &ack))
	if ack.Error == nil || ack.Error.Code != 10071 {
		t.Fatal("late terminal publication escaped the original subject-sequence fence")
	}
	t.Logf("late terminal: elapsed=%s frame_sha=%s rejection_sha=%s", time.Since(frame.accepted), transferDigest(frame.bytes), transferDigest(response.Data))
}

func fullstackTransferFrame(reader *bufio.Reader) ([]byte, []string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, nil, err
	}
	fields, frame := strings.Fields(line), []byte(line)
	if len(fields) >= 3 && (fields[0] == "PUB" || fields[0] == "HPUB" || fields[0] == "MSG" || fields[0] == "HMSG") {
		size, err := strconv.Atoi(fields[len(fields)-1])
		if err != nil || size < 0 || size > 12<<20 {
			return nil, nil, errors.New("invalid NATS transfer frame size")
		}
		body := make([]byte, size+2)
		if _, err := io.ReadFull(reader, body); err != nil {
			return nil, nil, err
		}
		if !bytes.HasSuffix(body, []byte("\r\n")) {
			return nil, nil, errors.New("invalid NATS transfer frame terminator")
		}
		frame = append(frame, body...)
	}
	return frame, fields, nil
}

func fullstackTransferStatusFixture() x.Status {
	return x.Status{State: x.StatusLive, Phase: x.PhaseReceiving,
		Identity: x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer",
			EntryNonce: "entry", IntentDigest: strings.Repeat("a", 64), ObjectVersion: "original-NUID"},
		Origin: x.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1},
		Budget: x.BudgetSeal{StartedAt: time.Unix(100, 0).UTC(), Size: 66549440, RateLimitBPS: 65536,
			EffectiveBudgetNS: int64(2092 * time.Second), RemainingNS: int64(2000 * time.Second), SourceSealed: true},
		DurableOffset: 65536, ChunkIndex: 2, LastConsumedStreamSeq: 7}
}

func fullstackTransferCheckpointFixture() (x.Observation, x.Observation) {
	before := x.Observation{Kind: x.ObserveFreeze, PID: 123, ProcessStart: 8, BootID: "boot", Generation: 1,
		TransferID: "transfer", EntryNonce: "entry", ObjectVersion: "original-NUID", Leg: "receiver", Phase: x.PhaseReceiving,
		EventSeq: 10, BoottimeNS: 100, Index: 2, FetchedOffset: 65536, DurableOffset: 65536, LastConsumedStreamSeq: 7,
		HashStateDigest: strings.Repeat("b", 64), FileSynced: true,
		Tmp:              x.ResourceRef{ResourceID: "tmp-id", Name: "tmp", Kind: x.ResourceTmp},
		Parent:           x.ResourceRef{ResourceID: "parent-id", Name: "parent", Kind: x.ResourceParent},
		OriginalDeadline: x.BootDeadline{BootID: "boot", Deadline: 1000000}}
	after := before
	after.Kind, after.Generation, after.CutID, after.EventSeq, after.BoottimeNS = x.ObserveImport, 2, "cut", 1, 101
	return before, after
}

func fullstackTransferEffectFixture() []x.Observation {
	row := x.Observation{Kind: x.ObservePrimitiveEnter, PID: 123, ProcessStart: 8, BootID: "boot", TransferID: "transfer",
		EntryNonce: "entry", GrantNonce: "grant", BoottimeNS: 100, Primitive: x.PrimitiveRename, Outcome: x.OutcomeNotEntered}
	rows := []x.Observation{row, row, row}
	rows[1].Kind, rows[1].Outcome, rows[1].BoottimeNS = x.ObservePrimitiveExit, x.OutcomeLanded, 101
	rows[2].Kind, rows[2].ParentSynced, rows[2].BoottimeNS = x.ObserveDirectorySync, true, 102
	return rows
}
