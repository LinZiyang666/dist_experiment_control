//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/fullstackharness"
	"github.com/LinZiyang666/tether/test/transferharness"
	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"
)

// Build with the production observation adapter; importer faults remain inert
// unless a test explicitly arms one. P23 exercises successful cuts.
// GOFLAGS is inherited by the real nested build so product mutations cannot
// accidentally affect only the outer test binary. Both artifacts are private.
// origin: hot-update S10 plan §2.1 / §8 / §10.2 P23
func buildFullstackTransferArtifacts(t *testing.T, artifactDir string) transferArtifacts {
	t.Helper()
	started := time.Now()
	root, err := filepath.Abs("../..")
	transferMust(t, err)
	dir := testharness.GuardProcessDirectory(t, artifactDir)
	a := transferArtifacts{a: filepath.Join(dir, "original"), b: filepath.Join(dir, "candidate")}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-race", "-trimpath", "-o", a.a, "./cmd/tether")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOFLAGS="+transferFaultOverlay(t, root, dir))
	// The shared S7 overlay predates the receipt leaf move. Keep its real
	// boundary predicates on that same DTO without editing the S7-owned file.
	// origin: S10 internal review TAGGED-SUITE-SWEEP-B F2
	observer := filepath.Join(dir, "cut-internal-drillobserve-transfer.go")
	observerSource, err := os.ReadFile(observer)
	transferMust(t, err)
	observerSource = bytes.ReplaceAll(observerSource, []byte("transferAttempt{}"), []byte("receipt.TransferAttempt{}"))
	observerSource = bytes.ReplaceAll(observerSource, []byte("var a transferAttempt"), []byte("var a receipt.TransferAttempt"))
	transferMust(t, os.WriteFile(observer, observerSource, 0600))
	// The read-side observation still formats CID after writer code moved out.
	reader := filepath.Join(dir, "internal-jsstream-object_resume.go")
	readerSource, err := os.ReadFile(reader)
	transferMust(t, err)
	if !bytes.Contains(readerSource, []byte("\"strconv\"")) {
		readerSource = bytes.Replace(readerSource, []byte("\"math\""), []byte("\"math\"\n\"strconv\""), 1)
	}
	transferMust(t, os.WriteFile(reader, readerSource, 0600))
	if output, err := fullstackharness.CombinedOutput(cmd); err != nil {
		t.Fatalf("SETUP-RED2: workspace product build: %v\n%s", err, output)
	}
	body, err := os.ReadFile(a.a)
	transferMust(t, err)
	transferMust(t, os.WriteFile(a.b, append(body, []byte("\nS10 transfer mechanism candidate\n")...), 0700))
	a.shaA, err = transferharness.FileSHA(a.a)
	transferMust(t, err)
	a.shaB, err = transferharness.FileSHA(a.b)
	transferMust(t, err)
	if a.shaA == a.shaB {
		t.Fatal("mechanism artifacts must have different executable digests")
	}
	cmd = exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	cmd.Dir = root
	output, err := fullstackharness.Output(cmd)
	transferMust(t, err)
	a.source, a.dirty = strings.TrimSpace(string(output)), hotUpdateSourceDigest(t, root)
	a.archiveA, a.archiveB = transferArchive(t, a.a), transferArchive(t, a.b)
	t.Logf("startup build=%s source=%s tree=%s artifact_A=%s artifact_B=%s", time.Since(started), a.source, a.dirty, a.shaA, a.shaB)
	return a
}

// C1 is admitted only after C0's exact terminal, original ctl wait, consumer
// retirement and two fresh seals. Every run starts a real worker/consumer/tmp.
// This small mechanism witness does not change TransferCohortPlan's slow rates.
// origin: hot-update S10 plan §3.3 / §4.2 / §11.2 P23
func exerciseTransferCohort(t *testing.T, a transferArtifacts) {
	t.Helper()
	for _, cell := range []transferCell{{direction: "push", layer: "agent"}, {direction: "pull", layer: "agent"},
		{direction: "push", layer: "broker"}, {direction: "pull", layer: "broker"}} {
		t.Run(cell.layer+"/"+cell.direction, func(t *testing.T) {
			t.Parallel()
			s := newTransferStack(t, a)
			cut := s.agentCut
			if cell.layer == "broker" {
				cut = s.brokerCut
			}
			seen := make(map[string]bool)
			for _, cohort := range []string{"C0", "C1"} {
				t.Run(cohort, func(t *testing.T) {
					// Attribute a refused cut to this cohort, while keeping the
					// original stack and bucket alive across the exact C0/C1 join.
					previous := s.t
					s.t = t
					defer func() { s.t = previous }()
					r, original, raw, _ := fullstackStartTransfer(t, s, cell.direction, "sender-chunk-pending")
					r.cell.layer = cell.layer
					defer r.watch.close()
					defer raw.close()
					if seen[r.id] || seen[r.boundary.EntryNonce] || seen[r.boundary.ObjectVersion] {
						t.Fatal("relay reused a prior invocation, entry or object version")
					}
					for _, value := range []string{r.id, r.boundary.EntryNonce, r.boundary.ObjectVersion} {
						seen[value] = true
					}
					sender := fullstackAwaitTransferSender(t, s, r, 0)
					cutStarted := time.Now()
					cut(false)
					current := fullstackReadTransferStatus(t, s, r.id, original)
					transferMust(t, fullstackCheckTransferStatus(original, current))
					advanced := fullstackAwaitTransferSender(t, s, r, sender.Offset+int64(sender.Length))
					if advanced.PID != sender.PID || advanced.ProcessStart != sender.ProcessStart || advanced.BootID != sender.BootID || advanced.ObjectVersion != sender.ObjectVersion {
						t.Fatal("sender continued under a replacement identity")
					}
					// The reverse must carry progress made by the active candidate.
					// A stale initial bundle can preserve the old checksum and still
					// lose newly synced bytes, so sample again before reversing.
					r.cell.window = "synced-prefix"
					r.boundary = fullstackAwaitTransferProgress(t, s, r, current.DurableOffset)
					consumers := fullstackTransferConsumers(t, s)
					if len(consumers) == 0 {
						t.Fatal("receiver cut has no original live consumer")
					}
					r.cutOwner = fullstackLatestTransferOwner(t, s.collectors[cell.layer], r.id)
					cut(true)
					awaitTransferContinuation(t, s, r)
					if cell.layer == "agent" {
						fullstackCheckImportedPrefix(t, s, r)
					}
					// The third cut belongs to this original invocation, before it
					// settles. C1 cannot replace C0's same-ID re-upgrade obligation.
					// origin: S10 internal review rv3-assembly F6 / S10 plan §3.3
					r.boundary = fullstackAwaitTransferProgress(t, s, r, r.boundary.DurableOffset)
					r.cutOwner = fullstackLatestTransferOwner(t, s.collectors[cell.layer], r.id)
					cut(false)
					awaitTransferContinuation(t, s, r)
					transferMust(t, fullstackCheckTransferStatus(original, fullstackReadTransferStatus(t, s, r.id, original)))
					t.Logf("cohort=%s layer=%s direction=%s three-cuts=%s", cohort, cell.layer, cell.direction, time.Since(cutStarted))
					fullstackFinishTransfer(t, s, r, original, raw, consumers)
					first := fullstackDrainTransfer(t, s, r.id)
					second := fullstackDrainTransfer(t, s, r.id)
					transferMust(t, fullstackCheckTransferSeals(first, second))
					// Completed ctl custody has no further producer. Join its reader
					// before the next launch replaces the map entry.
					s.collectors["ctl"].close()
					delete(s.collectors, "ctl")
				})
				if t.Failed() {
					return // A failed original must never be retried as cohort C1.
				}
			}
		})
	}
}

// The default 120s history window is deliberately not shortened. Late producer
// attempts must be observed and fenced before the second seal; waiting alone
// cannot certify that an old frame was actually rejected.
// origin: hot-update S10 plan §5.4 / §10.2 P23 / M08X/M11X
func exerciseTransferEndSeal(t *testing.T, a transferArtifacts) {
	t.Helper()
	for _, direction := range []string{"push", "pull"} {
		t.Run(direction, func(t *testing.T) {
			t.Parallel()
			s := newTransferStack(t, a)
			relay := fullstackLateTransferRelay(t, s)
			r, original, raw, consumers := fullstackStartTransfer(t, s, direction, "synced-prefix")
			defer r.watch.close()
			defer raw.close()
			fullstackFinishTransfer(t, s, r, original, raw, consumers)
			stream, err := s.js.Stream(t.Context(), jsstream.HistoryStreamName(s.sid))
			transferMust(t, err)
			info, err := stream.Info(t.Context())
			transferMust(t, err)
			if info.Config.Duplicates != 120*time.Second {
				t.Fatal("late-terminal witness did not use the real default dedupe window")
			}
			beforeLate := fullstackDrainTransfer(t, s, r.id)
			late := relay.pending(t)
			wait := time.NewTimer(time.Until(late.accepted.Add(125 * time.Second))) // sleep-fixture: real default history dedupe expiry.
			defer wait.Stop()
			select {
			case <-wait.C:
			case <-t.Context().Done():
				t.Fatal(t.Context().Err())
			}
			relay.releaseLate(t, s.nc, late)
			first := fullstackDrainTransfer(t, s, r.id)
			transferMust(t, fullstackCheckTransferSeals(beforeLate, first))
			second := fullstackDrainTransfer(t, s, r.id)
			transferMust(t, fullstackCheckTransferSeals(first, second))
			transferMust(t, raw.scan())
			transferMust(t, raw.verify())
			transferMust(t, r.watch.check())
		})
	}
}

func fullstackStartTransfer(t *testing.T, s *transferStack, direction, window string) (*transferRun, x.Status, *transferRawWatch, []string) {
	t.Helper()
	started := time.Now()
	previous := make(map[string]bool)
	for _, collector := range s.collectors {
		rows, err := collector.snapshot()
		transferMust(t, err)
		for _, row := range rows {
			previous[row.TransferID] = true
		}
	}
	cell := transferCell{"b", direction, "agent", window}
	r := startTransferRun(t, s, cell)
	t.Cleanup(r.watch.close)
	if !testharness.WaitFor(t, 20*time.Second, 5*time.Millisecond, func() bool {
		for _, collector := range s.collectors {
			rows, err := collector.snapshot()
			transferMust(t, err)
			for _, row := range rows {
				if fullstackTransferBoundaryIsNew(row, previous, window) {
					r.boundary = row
					return true
				}
			}
		}
		if receipt := s.ctl.Receipt(); receipt.NativeRC != -1 {
			t.Fatalf("INCOMPLETE: original ctl exited before %s: native_rc=%d %s", window, receipt.NativeRC, s.output(s.ctl))
		}
		return false
	}) {
		t.Fatal("NOT_RUN pending S7 P28/P29: no new nonempty transfer boundary")
	}
	r.id = r.boundary.TransferID
	transferMust(t, validateTransferBoundary(cell, r.boundary, int64(len(r.body))))
	r.cutOwner = fullstackLatestTransferOwner(t, s.collectors["agent"], r.id)
	original := fullstackReadTransferStatus(t, s, r.id, x.Status{})
	if original.Budget.Size != int64(len(r.body)) || original.Budget.RateLimitBPS != 1<<20 ||
		original.Identity.EntryNonce != r.boundary.EntryNonce || original.Identity.ObjectVersion != r.boundary.ObjectVersion {
		t.Fatal("original status does not bind the actual source and receiver")
	}
	budget, err := proto.TransferBudget("b", int64(len(r.body)), 1<<20)
	transferMust(t, err)
	if original.Budget.EffectiveBudgetNS != int64(budget) {
		t.Fatal("mechanism witness silently changed the production byte budget")
	}
	owner := "agent"
	if (direction == "pull") == (window == "synced-prefix") {
		owner = "ctl"
	}
	transferMust(t, s.collectors[owner].compareOFDs(r.boundary))
	raw := startTransferRawWatch(t, s, r)
	consumers := fullstackTransferConsumers(t, s)
	if window == "synced-prefix" && len(consumers) == 0 {
		raw.close()
		t.Fatal("SETUP-RED2: no live transfer consumer at the nonzero Sync prefix")
	}
	t.Logf("startup and nonempty I/O=%s transfer=%s direction=%s window=%s", time.Since(started), r.id, direction, window)
	return r, original, raw, consumers
}

func fullstackReadTransferStatus(t *testing.T, s *transferStack, id string, original x.Status) x.Status {
	t.Helper()
	identity, err := cli.EnsureIdentity(s.home)
	transferMust(t, err)
	// origin: S7 internal review INTEG-M F5 — status replies use authenticated private custody.
	opts := append(natsinbox.InboxConnectOptions(identity.PublicKey), nats.Name(cli.CtlNameForSession(s.sid)), nats.Nkey(identity.PublicKey,
		func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(identity.Seed, nonce) }))
	nc, err := nats.Connect(s.url, opts...)
	transferMust(t, err)
	defer nc.Close()
	actorFP, err := auth.FingerprintFromActor(identity.PublicKey)
	transferMust(t, err)
	// Discovery still carries authenticated subject scope; subsequent reads
	// retain the original owner's complete binding instead of rediscovering it.
	// origin: S7 internal review INTEG-M F5
	lookup := x.Identity{SID: s.sid, NID: s.nid, ActorFP: actorFP, TransferID: id}
	origin := original.Origin
	if original.Identity.TransferID != "" {
		lookup = original.Identity
	} else {
		lookup, origin = fullstackOriginalTransferAuthority(t, s, lookup)
	}
	subject, request, err := proto.EncodeTransferRequest(identity.PublicKey, x.Request{Op: x.OpStatus,
		CallID: fullstackTransferNonce(t), Identity: lookup, Origin: origin})
	transferMust(t, err)
	message, err := nc.Request(subject, request, 5*time.Second)
	transferMust(t, err)
	response, err := proto.DecodeTransferResponse(x.OpStatus, "push", message.Data)
	transferMust(t, err)
	if response.Code != "" || response.Error != "" {
		t.Fatalf("original-owner status refused: code=%s error=%s", response.Code, response.Error)
	}
	if response.Status == nil || response.Status.State == x.StatusUnknown {
		t.Fatal("SETUP-RED2: NOT_RUN pending S7 P25/P27 original-owner status")
	}
	if original.Identity.TransferID == "" {
		original = *response.Status
	}
	t.Logf("original-owner status state=%s phase=%s agent_bound=%t entry_bound=%t version_bound=%t origin_bound=%t budget_started=%t budget_size=%d budget_effective=%d budget_remaining=%d", response.Status.State, response.Status.Phase,
		original.Identity.AgentInstance != "", original.Identity.EntryNonce != "", original.Identity.ObjectVersion != "",
		original.Origin.BrokerID != "" && original.Origin.InstanceID != "" && original.Origin.BootID != "" && original.Origin.Generation > 0,
		!original.Budget.StartedAt.IsZero(), original.Budget.Size, original.Budget.EffectiveBudgetNS, original.Budget.RemainingNS)
	// Preserve the actual status object's wire bytes. CLI presentation omits
	// empty terminal/code/error fields, while the frozen wire DTO requires them.
	projected, err := fullstackharness.DecodeTransferStatus(message.Data, "nats-status/"+id, original)
	transferMust(t, err)
	return projected.Value
}

func fullstackLatestTransferOwner(t *testing.T, c *transferCollector, id string) x.Observation {
	t.Helper()
	rows, err := c.snapshot()
	transferMust(t, err)
	var result x.Observation
	for _, row := range rows {
		if row.TransferID == id && fullstackTransferOwnerIsNewer(row, result) {
			result = row
		}
	}
	if result.PID == 0 || result.EntryNonce == "" {
		t.Fatal("SETUP-RED2: NOT_RUN pending S7 P28 transfer owner observation")
	}
	return result
}

func fullstackAwaitTransferProgress(t *testing.T, s *transferStack, r *transferRun, offset int64) x.Observation {
	t.Helper()
	var result x.Observation
	if !testharness.WaitFor(t, 10*time.Second, 5*time.Millisecond, func() bool {
		for _, role := range []string{"agent", "ctl"} {
			rows, err := s.collectors[role].snapshot()
			transferMust(t, err)
			for _, row := range rows {
				if row.TransferID == r.id && row.Kind == x.ObserveSynced && row.FileSynced &&
					row.DurableOffset > offset && row.DurableOffset < int64(len(r.body)) && row.HashStateDigest != "" {
					result = row
					return true
				}
			}
		}
		return false
	}) {
		t.Fatal("no nonempty candidate receiver progress before reverse cut")
	}
	return result
}

// A sender's pinned source and the object writer's ACK are separate original
// records. Join them by process and transfer identity instead of manufacturing
// a combined event or counting metadata/courier activity as byte progress.
// origin: hot-update S10 plan §3.3 / §5.4 / P23
func fullstackAwaitTransferSender(t *testing.T, s *transferStack, r *transferRun, offset int64) x.Observation {
	t.Helper()
	role := "ctl"
	if r.cell.direction == "pull" {
		role = "agent"
	}
	var ack, source x.Observation
	if !testharness.WaitFor(t, 10*time.Second, 5*time.Millisecond, func() bool {
		rows, err := s.collectors[role].snapshot()
		transferMust(t, err)
		for _, row := range rows {
			if row.TransferID != r.id || row.EntryNonce != r.boundary.EntryNonce || row.ObjectVersion != r.boundary.ObjectVersion {
				continue
			}
			if row.Source.ResourceID != "" {
				source = row
			}
			end := row.Offset + int64(row.Length)
			if (row.Kind == x.ObserveAppend || row.Kind == x.ObserveResolve) && row.ActualACKSeq > 0 && row.Length > 0 && end > offset && end < int64(len(r.body)) {
				ack = row
			}
		}
		return ack.PID > 0 && source.PID == ack.PID && source.ProcessStart == ack.ProcessStart && source.BootID == ack.BootID
	}) {
		t.Fatal("SETUP-RED2: NOT_RUN pending S7 P08/P28 original source OFD and nonempty sender ACK")
	}
	transferMust(t, s.collectors[role].compareOFDs(source))
	return ack
}

// Compare frozen checkpoints, not arbitrary pre/post samples: legitimate
// progress between samples would otherwise hide an off-by-one import.
// origin: hot-update S10 plan §5.4 / §8 M02X/M03X
func fullstackCheckImportedPrefix(t *testing.T, s *transferStack, r *transferRun) {
	t.Helper()
	if r.cell.direction == "pull" {
		transferMust(t, s.collectors["ctl"].compareOFDs(r.boundary))
		return // The original ctl owns pull's receiver; its FD never migrates.
	}
	rows, err := s.collectors["agent"].snapshot()
	transferMust(t, err)
	for i := len(rows) - 1; i >= 0; i-- {
		imported := rows[i]
		if imported.TransferID != r.id || imported.Kind != x.ObserveImport || imported.Generation <= r.cutOwner.Generation {
			continue
		}
		for j := i - 1; j >= 0; j-- {
			frozen := rows[j]
			if frozen.TransferID == r.id && frozen.Kind == x.ObserveFreeze && frozen.DurableOffset > 0 {
				transferMust(t, fullstackCheckTransferCheckpoint(frozen, imported))
				transferMust(t, s.collectors["agent"].compareOFDs(frozen))
				return
			}
		}
	}
	t.Fatal("SETUP-RED2: NOT_RUN pending S7 P06/P07/P28 exact receiver Freeze/Import checkpoint observations")
}

// Retained C0 evidence cannot authorize C1, and an exec resets event sequence.
// origin: hot-update S10 plan §3.3 / §5.1 / P23
func fullstackTransferBoundaryIsNew(row x.Observation, previous map[string]bool, window string) bool {
	return row.TransferID != "" && !previous[row.TransferID] && transferWindowMatches(window, row)
}

func fullstackTransferOwnerIsNewer(row, old x.Observation) bool {
	return row.Generation > old.Generation || row.Generation == old.Generation && row.EventSeq > old.EventSeq
}

func fullstackFinishTransfer(t *testing.T, s *transferStack, r *transferRun, original x.Status, raw *transferRawWatch, consumers []string) {
	t.Helper()
	started := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	result, err := s.ctl.Wait(ctx)
	if err != nil || result.NativeRC != 0 {
		t.Fatalf("original ctl did not settle: native_rc=%d err=%v %s", result.NativeRC, err, s.output(s.ctl))
	}
	transferMust(t, validateTransferProcess(r.before, result, r.before.RunningSHA))
	body, err := os.ReadFile(r.target)
	transferMust(t, err)
	if !bytes.Equal(body, r.body) {
		t.Fatal("original receiver content lost, repeated or shifted bytes")
	}
	awaitTransferRetired(t, s, r.id)
	current := fullstackReadTransferStatus(t, s, r.id, original)
	if current.State != x.StatusTerminal || current.Phase != x.PhaseSettled || current.CleanupPending {
		t.Fatal("terminal history did not retire cleanup responsibility")
	}
	transferMust(t, fullstackCheckTransferConsumers(consumers, fullstackTransferConsumers(t, s)))
	var effects []x.Observation
	for _, role := range []string{"agent", "ctl"} {
		rows, err := s.collectors[role].snapshot()
		transferMust(t, err)
		effects = append(effects, rows...)
	}
	transferMust(t, fullstackCheckTransferEffects(effects, r.id))
	transferMust(t, raw.scan())
	transferMust(t, raw.verify())
	transferMust(t, r.watch.check())
	transferMust(t, fullstackCheckTransferCalls(s, r.id))
	t.Logf("settle=%s original_ctl_pid=%d ticks=%d native_rc=%d", time.Since(started), result.PID, result.ProcessStart, result.NativeRC)
}

func fullstackTransferConsumers(t *testing.T, s *transferStack) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := s.js.Stream(ctx, "OBJ_"+proto.XferBucketName(s.sid))
	transferMust(t, err)
	lister := stream.ConsumerNames(ctx)
	var names []string
	for name := range lister.Name() {
		names = append(names, name)
	}
	transferMust(t, lister.Err())
	slices.Sort(names)
	return names
}

// Independent read-only status queries have fresh CallIDs. They do not mint
// business identities and must not be confused with a retried creation call.
// origin: hot-update S10 plan §3.3 / §8 M05X
func fullstackCheckTransferCalls(s *transferStack, id string) error {
	s.wireMu.Lock()
	defer s.wireMu.Unlock()
	ids := make(map[string]string)
	for _, message := range s.wire {
		_, _, _, _, ctrlOp, ctrl := proto.ParseCtrlTransferRequest(message.Subject)
		_, _, _, evOp, ev := proto.ParseEvTransferRequest(message.Subject)
		if ctrl && ctrlOp == x.OpStatus || ev && evOp == x.OpStatus {
			continue
		}
		var request struct {
			CallID     string `json:"call_id"`
			TransferID string `json:"transfer_id"`
		}
		if err := json.Unmarshal(message.Data, &request); err != nil {
			return err
		}
		if request.TransferID != id || request.CallID == "" {
			return errors.New("original transfer invocation was replaced")
		}
		if old, ok := ids[message.Subject]; ok && old != request.CallID {
			return errors.New("creation or mutation retry replaced its original CallID")
		}
		ids[message.Subject] = request.CallID
	}
	if len(ids) == 0 {
		return errors.New("original transfer request population is empty")
	}
	return nil
}

type fullstackTransferSeal struct {
	nonce   string
	at      int64
	cursor  uint64
	acks    map[string]uint64
	history []byte
}

func fullstackDrainTransfer(t *testing.T, s *transferStack, id string) fullstackTransferSeal {
	t.Helper()
	seal := fullstackTransferSeal{at: time.Now().UnixNano(), acks: make(map[string]uint64)}
	for _, role := range []string{"agent", "broker"} {
		nonce, sequence := fullstackDrainTransferOwner(t, s.collectors[role])
		seal.nonce += role + ":" + nonce + "/"
		seal.acks[role] = sequence
	}
	stream, err := s.js.Stream(t.Context(), jsstream.HistoryStreamName(s.sid))
	transferMust(t, err)
	rows, info, err := transferharness.ScanStream(t.Context(), stream, 0)
	transferMust(t, err)
	seal.cursor = info.State.LastSeq
	slices.SortFunc(rows, func(a, b transferharness.RawMessage) int {
		if a.Sequence < b.Sequence {
			return -1
		}
		if a.Sequence > b.Sequence {
			return 1
		}
		return 0
	})
	for _, row := range rows {
		if row.Absent || row.Subject != proto.SubjAuditTransfer(s.sid) {
			continue
		}
		var record struct {
			TransferID string `json:"transfer_id"`
		}
		transferMust(t, json.Unmarshal(row.Data, &record))
		if record.TransferID == id {
			encoded, err := json.Marshal(row)
			transferMust(t, err)
			seal.history = append(seal.history, encoded...)
			seal.history = append(seal.history, '\n')
		}
	}
	if len(seal.history) == 0 {
		t.Fatal("fresh complete scan has no original transfer history")
	}
	return seal
}

// Use the actual challenge and sealed-batch watermark. The custody protocol
// does not promise an ObserveDrain event in the transfer domain.
// origin: hot-update S10 plan §5.4 / §10.2 P23
func fullstackDrainTransferOwner(t *testing.T, c *transferCollector) (string, uint64) {
	t.Helper()
	var nonce [16]byte
	_, err := rand.Read(nonce[:])
	transferMust(t, err)
	request, err := drillobserve.TransferDrainRequest(nonce)
	transferMust(t, err)
	transferMust(t, unix.Sendmsg(c.fd, request, nil, nil, unix.MSG_NOSIGNAL))
	var sequence uint64
	if !testharness.WaitFor(t, 5*time.Second, 5*time.Millisecond, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		transferMust(t, c.err)
		ack, received := c.takeDrain(nonce)
		if !received {
			return false
		}
		transferMust(t, ack.err)
		sequence = binary.LittleEndian.Uint64(ack.packet[20:28])
		return true
	}) {
		t.Fatal("fresh transfer drain has no nonce-bound sealed batch ACK")
	}
	return hex.EncodeToString(nonce[:]), sequence
}

func fullstackTransferNonce(t *testing.T) string {
	t.Helper()
	var nonce [16]byte
	_, err := rand.Read(nonce[:])
	transferMust(t, err)
	return hex.EncodeToString(nonce[:])
}

func fullstackCheckTransferStatus(original, current x.Status) error {
	wire := proto.TransferStatusResp{State: current.State, Phase: current.Phase, Identity: proto.Identity(current.Identity),
		Origin: proto.Origin(current.Origin), Budget: proto.BudgetSeal(current.Budget), DurableOffset: current.DurableOffset,
		ChunkIndex: current.ChunkIndex, LastConsumedStreamSeq: current.LastConsumedStreamSeq, CleanupPending: current.CleanupPending,
		Code: current.Code, Error: current.Error}
	if current.Terminal != nil {
		terminal := proto.TerminalReceipt(*current.Terminal)
		wire.Terminal = &terminal
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return err
	}
	_, err = fullstackharness.DecodeTransferStatus(data, "status-control", original)
	return err
}

func fullstackCheckTransferCheckpoint(before, after x.Observation) error {
	if before.Kind != x.ObserveFreeze || after.Kind != x.ObserveImport || before.DurableOffset <= 0 ||
		before.HashStateDigest == "" || before.LastConsumedStreamSeq == 0 || before.Index == 0 ||
		before.Tmp.ResourceID == "" || before.Parent.ResourceID == "" || before.PID <= 0 || before.BootID == "" ||
		before.OriginalDeadline.Deadline <= 0 || after.CutID == "" || after.BoottimeNS <= before.BoottimeNS ||
		after.Generation != before.Generation+1 || after.PID != before.PID || after.ProcessStart != before.ProcessStart ||
		after.BootID != before.BootID || after.TransferID != before.TransferID || after.EntryNonce != before.EntryNonce ||
		after.ObjectVersion != before.ObjectVersion || after.OriginalDeadline != before.OriginalDeadline {
		return errors.New("receiver import lost original identity or boot-bound deadline")
	}
	if after.DurableOffset != before.DurableOffset || after.Index != before.Index ||
		after.LastConsumedStreamSeq != before.LastConsumedStreamSeq || after.HashStateDigest != before.HashStateDigest ||
		after.Tmp != before.Tmp || after.Parent != before.Parent {
		return errors.New("receiver import changed exact synced prefix, hash or original resource")
	}
	return nil
}

func fullstackCheckTransferEffects(rows []x.Observation, id string) error {
	if err := validateTransferEffects(rows, id); err != nil {
		return err
	}
	var enter, landed, synced x.Observation
	for _, row := range rows {
		if row.TransferID != id {
			continue
		}
		switch row.Kind {
		case x.ObservePrimitiveEnter:
			enter = row
		case x.ObservePrimitiveExit:
			landed = row
		case x.ObserveDirectorySync:
			if row.ParentSynced {
				synced = row
			}
		}
	}
	if enter.PID <= 0 || enter.EntryNonce == "" || landed.PID != enter.PID || synced.PID != enter.PID ||
		landed.ProcessStart != enter.ProcessStart || synced.ProcessStart != enter.ProcessStart ||
		landed.BootID != enter.BootID || synced.BootID != enter.BootID || landed.EntryNonce != enter.EntryNonce ||
		synced.EntryNonce != enter.EntryNonce || landed.GrantNonce != enter.GrantNonce ||
		landed.BoottimeNS <= enter.BoottimeNS || synced.BoottimeNS <= landed.BoottimeNS {
		return errors.New("directory durability does not follow the same receiver primitive")
	}
	return nil
}

func fullstackCheckTransferSeals(first, second fullstackTransferSeal) error {
	if first.nonce == "" || first.nonce == second.nonce || second.nonce == "" || second.at <= first.at ||
		second.cursor < first.cursor || len(first.history) == 0 || !bytes.Equal(first.history, second.history) ||
		len(first.acks) == 0 || len(first.acks) != len(second.acks) {
		return errors.New("late terminal changed history or replayed the final seal")
	}
	for owner, sequence := range first.acks {
		if sequence == 0 || second.acks[owner] <= sequence {
			return errors.New("second drain lacks a fresh owner ACK sequence")
		}
	}
	return nil
}

func fullstackCheckTransferConsumers(before, after []string) error {
	if len(before) == 0 || len(after) != 0 {
		return fmt.Errorf("transfer consumer lifecycle is incomplete: live=%d retired_remaining=%d", len(before), len(after))
	}
	return nil
}
