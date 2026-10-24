//go:build linux && hotupdate_integration

package hotupdate_test

// The live-cut barrier parks the producer of a declared boundary at its own
// observation, so a cut happens while that responsibility provably exists.
// The reply-drop witness discards an actual reply at its requester's receive
// edge. Both are test overlays; ordinary releases have neither.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/transferharness"
	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"
)

// transferPatchSource applies exact, unique anchors on top of any prior
// overlay replacement of the same production file.
func transferPatchSource(t *testing.T, root, dir string, replacements map[string]string, relative string, edits [][2]string, suffix string) {
	t.Helper()
	path := filepath.Join(root, relative)
	read := path
	if prior := replacements[path]; prior != "" {
		read = prior
	}
	data, err := os.ReadFile(read)
	transferMust(t, err)
	source, err := transferOverlayEdits(string(data), edits)
	if err != nil {
		t.Fatalf("transfer overlay anchor moved in %s: %v", relative, err)
	}
	name := filepath.Join(dir, "cut-"+strings.ReplaceAll(relative, "/", "-"))
	transferMust(t, os.WriteFile(name, []byte(source+suffix), 0600))
	replacements[path] = name
}

// Every edit must still select exactly one current construction site. A move
// must fail before compiling an artifact with a silently missing fault port.
// origin: S7 internal review TAGGED-SUITE-SWEEP-A F1
func transferOverlayEdits(source string, edits [][2]string) (string, error) {
	for _, edit := range edits {
		if strings.Count(source, edit[0]) != 1 {
			return "", fmt.Errorf("missing or ambiguous anchor: %s", edit[0])
		}
		source = strings.Replace(source, edit[0], edit[1], 1)
	}
	return source, nil
}

// The producer parks after its record entered custody and outside the
// observer's ordinal lock, so other producers keep their streams. Locks that
// its caller holds stay held: the harness cuts around the parked owner and
// releases it afterwards. An abandoned hold poisons custody rather than
// passing quietly. The predicate mirrors validateTransferBoundary; the harness
// still validates the exact parked record independently.
// origin: S7 FIX-CUT deterministic live-cut barrier / hot-update S7 plan §10.2 / T35–T38 / T46
const transferCutHoldOverlay = `
func init() { receipt.TransferCutHold = transferCutHold }

// Identity belongs to the original kernel process, not its later exit record.
// Park only during test startup, before main/RunE creates any business budget
// or transfer ID, so even a slow independent executable hash precedes exit.
// origin: S7 internal review FIX-TIERA2 F3 / hot-update S7 plan §10.4 / T46
func init() {
 if Hook == nil || len(os.Args) < 2 || (os.Args[1] != "push" && os.Args[1] != "pull") { return }
 root := os.Getenv("S7_TRANSFER_FAULT_ROOT")
 if root == "" { return }
 transferHoldWrite(root+"/ctl-identity-entered", "ready\n")
 until := time.Now().Add(90*time.Second)
 for {
  if _, err := os.Stat(root+"/ctl-identity-release"); err == nil { return }
  if time.Now().After(until) { transferPoison(Hook); return }
  time.Sleep(time.Millisecond)
 }
}

func transferCutHold(o transferObservation, poison func()) {
	if o.Kind == "reserve" && o.GrantNonce != "" && o.Attempt == (transferAttempt{}) {
		transferReserveMemory.Lock()
		transferReserveMemory.rows[o.TransferID] = o
		transferReserveMemory.Unlock()
	}
	root := os.Getenv("S7_TRANSFER_FAULT_ROOT")
	if root == "" {
		return
	}
	data, err := os.ReadFile(root + "/hold-arm")
	if err != nil {
		return
	}
	f := strings.Fields(string(data))
	if len(f) != 5 {
		return
	}
	size, err := strconv.ParseInt(f[4], 10, 64)
	if err != nil || !strings.Contains(","+f[2]+",", ","+transferHoldRole()+",") || !transferHoldWindow(f[1], f[3], size, o) {
		return
	}
	if os.Rename(root+"/hold-arm", root+"/hold-"+f[0]+"-taken") != nil {
		return
	}
	now, _ := transferNow()
	transferHoldWrite(root+"/hold-"+f[0]+"-entered", fmt.Sprintf("%d %d %d %d %d\n", o.PID, o.ProcessStart, o.Generation, o.EventSeq, now))
	limit := time.Now().Add(transferCutHoldLimit)
	for {
		if _, err := os.Stat(root + "/hold-" + f[0] + "-release"); err == nil {
			break
		}
		if time.Now().After(limit) {
			poison()
			break
		}
		time.Sleep(time.Millisecond)
	}
	now, _ = transferNow()
	transferHoldWrite(root+"/hold-"+f[0]+"-woke", fmt.Sprintf("%d\n", now))
}

var transferReserveMemory = struct {
	sync.Mutex
	rows map[string]transferObservation
}{rows: map[string]transferObservation{}}

func transferHoldWrite(path, body string) {
	if os.WriteFile(path+".tmp", []byte(body), 0600) == nil {
		_ = os.Rename(path+".tmp", path)
	}
}

func transferHoldRole() string {
	if len(os.Args) < 2 {
		return ""
	}
	switch os.Args[1] {
	case "serve":
		return "broker"
	case "agent":
		return "agent"
	case "push", "pull":
		return "ctl"
	}
	return ""
}

func transferHoldWindow(window, tier string, size int64, o transferObservation) bool {
	if o.TransferID == "" || o.EntryNonce == "" || o.OriginalDeadline.Deadline <= 0 || o.OriginalDeadline.BootID != o.BootID ||
		(tier == "b") != (o.ObjectVersion != "") || !TransferPendingBoundary(window, o) {
		return false
	}
	receiver := o.DurableOffset > 0 && o.DurableOffset < size && o.FetchedOffset >= o.DurableOffset && o.HashStateDigest != "" &&
		o.Tmp.ResourceID != "" && o.Parent.ResourceID != "" && (tier != "b" || o.Index > 0 && o.LastConsumedStreamSeq > 0)
	// Park a primitive window only after the whole payload's durable Sync
	// (Tier A's single inline Sync), with the original tmp and parent held.
	primitive := o.DurableOffset == size && o.Tmp.ResourceID != "" && o.Parent.ResourceID != ""
	switch window {
	case "prepare-accepted":
		return o.Kind == "control" && o.Phase == "prepared"
	case "source-pinned":
		return o.Source.ResourceID != "" && o.Phase == "sending"
	case "sender-chunk-pending":
		return o.Kind == "append" && o.ActualACKSeq == 0 && o.Length > 0
	case "inline-send-unconfirmed":
		return o.Kind == "attempt" && o.Attempt.Sent && o.ObjectVersion == ""
	case "receiver-write":
		return receiver && o.Kind == "fetched" && o.FetchedOffset > o.DurableOffset
	case "Sync-pending":
		return receiver && o.Kind == "synced" && !o.FileSynced
	case "synced-prefix":
		return receiver && o.Kind == "synced" && o.FileSynced
	case "grant-reply-lost":
		return o.Kind == "reserve" && o.GrantNonce != "" && o.Attempt.Sent
	case "pre-primitive":
		return primitive && o.Kind == "primitive_enter" && o.Outcome == "not_entered"
	case "post-primitive-pre-outcome":
		return primitive && o.Kind == "primitive_exit" && o.Outcome == "landed" && o.CanonicalDigest == ""
	case "terminal-pre-history-ACK":
		return o.Phase == "terminal_pending" && o.CanonicalDigest != "" && o.HistorySeq == 0
	case "cleanup-pending":
		return o.Phase == "cleanup_pending" && o.RetirementDigest == ""
	}
	return false
}

// TransferReplyDropped is the drop witness of a requester whose actual reply
// was discarded at its own receive edge. It extends this process's durable
// reserve record with the real sent attempt and emits it on that record's
// generation stream. A grant this process never reserved emits nothing.
func TransferReplyDropped(transferID, grantNonce string, attempt any) bool {
	return transferReserveAttempt("reserve", transferID, grantNonce, attempt)
}

// TransferLinkReplyLost records the requester's own final outcome: a grant
// link attempt that actually ended without a reply. A drop that let the reply
// through cannot produce it, however its earlier witness was labelled.
func TransferLinkReplyLost(transferID, grantNonce string, attempt any) bool {
	return transferReserveAttempt("attempt", transferID, grantNonce, attempt)
}

func transferReserveAttempt(kind, transferID, grantNonce string, attempt any) bool {
	transferReserveMemory.Lock()
	rec, ok := transferReserveMemory.rows[transferID]
	transferReserveMemory.Unlock()
	var a transferAttempt
	if !ok || grantNonce == "" || rec.GrantNonce != grantNonce || copyTransferValue(reflect.ValueOf(&a).Elem(), reflect.ValueOf(attempt)) != nil || !a.Sent {
		return false
	}
	transferProcessObservers.Lock()
	emit := transferProcessObservers.byGeneration[rec.Generation]
	transferProcessObservers.Unlock()
	if emit == nil {
		return false
	}
	w := rec
	w.Kind, w.Phase, w.Attempt, w.EventSeq, w.BoottimeNS = kind, "commit_reserved", a, 0, 0
	if kind == "reserve" {
		w.Attempt.Class = "reply"
	}
	emit(w)
	return true
}
`

// The pull receiver is the ctl: its reserve reply carries the grant. The
// actual reply reaches the receive edge, is discarded once, and the requester
// waits out its own real reply budget before reporting a sent-unknown attempt.
// origin: S7 FIX-CUT grant-reply drop witness / hot-update S7 plan §4 / §10.2 / T13
// The boundary is recorded at that receive edge, before waiting out the reply
// budget. Tier B already spent time on both byte legs; waiting another five
// seconds before emitting the boundary made it unobservable inside 20 seconds.
// origin: S7 internal review FIX-TIERA2 F4 / hot-update S7 plan §10.2 / T46
const transferCtlReplyDropOverlay = `
var transferDroppedGrants = struct {
	sync.Mutex
	watch map[string]func(string, controlbudget.Attempt)
}{watch: map[string]func(string, controlbudget.Attempt){}}

func transferDropGrantReply(ctx context.Context, subject string, data []byte) bool {
	root := os.Getenv("S7_TRANSFER_FAULT_ROOT")
	if root == "" || !strings.HasSuffix(subject, ".reserve.req") {
		return false
	}
	var reply map[string]any
	if json.Unmarshal(data, &reply) != nil {
		return false
	}
	grant, _ := reply["grant"].(map[string]any)
	identity, _ := grant["identity"].(map[string]any)
	nonce, _ := grant["grant_nonce"].(string)
	id, _ := identity["transfer_id"].(string)
	if nonce == "" || id == "" || os.Rename(root+"/fault-grant-reply", root+"/fault-grant-reply.entered") != nil {
		return false
	}
	transferDroppedGrants.Lock()
	watch := transferDroppedGrants.watch[id]
	delete(transferDroppedGrants.watch, id)
	transferDroppedGrants.Unlock()
	if watch != nil {
		if recorder, ok := ctx.Value(transferDropAttemptKey{}).(*transferAttemptRecorder); ok {
			a := recorder.transferCurrent
			a.Sent, a.Class = true, "reply"
			a.FinishedMonoNS, _ = controlbudget.Now()
			watch(nonce, a)
		}
	}
	return true
}

type transferDropAttemptKey struct{}

func (t *TransferTransport) TransferWatchGrantDrop(id string, watch func(string, controlbudget.Attempt)) {
 transferDroppedGrants.Lock()
 transferDroppedGrants.watch[id] = watch
 transferDroppedGrants.Unlock()
}

`

const transferCtlWitnessOverlay = `
// Test overlay: the client's drop witness carries the transport's actual sent
// attempt, the grant the dropped reply contained and the original deadline.
func transferWatchGrantDrop(c *Client, request x.Request) {
 if c.cfg.Observe == nil || request.Op != x.OpReserve { return }
 port, ok := c.cfg.Transport.(interface{ TransferWatchGrantDrop(string, func(string, x.Attempt)) })
 if !ok { return }
 port.TransferWatchGrantDrop(c.identity.TransferID, func(nonce string, witness x.Attempt) {
  c.cfg.Observe(x.Observation{Kind: x.ObserveReserve, Phase: x.PhaseCommitReserved, TransferID: c.identity.TransferID,
   EntryNonce: c.identity.EntryNonce, ObjectVersion: c.identity.ObjectVersion, GrantNonce: nonce,
   OriginalDeadline: x.BootDeadline{BootID: c.bootID, Deadline: c.deadline}, Attempt: witness})
 })
}

`

// transferCutBarrierOverlay installs the barrier and both drop edges. The
// push receiver is the agent: the broker delivers the durable grant on its
// link and the agent's actual reply is discarded at the broker's edge.
// origin: S7 FIX-CUT / hot-update S7 plan §10.2 / T35–T38 / T46
func transferCutBarrierOverlay(t *testing.T, root, dir string, replacements map[string]string) {
	t.Helper()
	// GATE-SLIMOBS moved the ordinal-stamping observer into the pure receipt
	// leaf; the custody/process half (and its file IO) stays in drillobserve.
	// The receipt leaf therefore only exposes the park point outside its own
	// ordinal lock, and the drillobserve adapter installs the barrier there.
	// Patching only drillobserve left every barrier cell failing at overlay
	// construction ("anchor moved") on the integrated tree.
	// origin: S7 FIX-TIERA2 / S7 FIX-CUT live-cut barrier after GATE-SLIMOBS
	transferPatchSource(t, root, dir, replacements, "internal/drillobserve/receipt/transfer.go", [][2]string{
		{"\t\thook(e)\n", "\t\thook(e)\n\t\tif TransferCutHold != nil {\n\t\t\tmu.Unlock()\n\t\t\tTransferCutHold(o, func() { transferPoison(hook, poison) })\n\t\t\tmu.Lock()\n\t\t}\n"}}, `
// Test overlay: the drillobserve adapter installs the live-cut barrier, which
// parks the producer after its record entered custody, outside the ordinal lock.
var TransferCutHold func(TransferObservation, func())
`)
	transferPatchSource(t, root, dir, replacements, "internal/drillobserve/transfer.go", [][2]string{
		{"\t\"sync\"\n\n", "\t\"sync\"\n\t\"fmt\"\n\t\"os\"\n\t\"strconv\"\n\t\"strings\"\n\t\"time\"\n\n"}},
		// The attempt codec moved into drillobserve/receipt; alias it only inside
		// this overlay instead of keeping a stale alias in production.
		// origin: S10 internal review TAGGED-SUITE-SWEEP-A request 1
		"\ntype transferAttempt = receipt.TransferAttempt\n"+transferCutHoldSource(transferCutHoldCuts))
	transferPatchSource(t, root, dir, replacements, "internal/cli/transfer_conn.go", [][2]string{
		{"\t\"errors\"\n", "\t\"errors\"\n\t\"encoding/json\"\n\t\"os\"\n\t\"strings\"\n"},
		{"reply, err = sub.NextMsgWithContext(ctx)", "reply, err = sub.NextMsgWithContext(ctx)\n\tif err == nil && transferDropGrantReply(ctx, subject, reply.Data) {\n\t\t<-ctx.Done()\n\t\treply, err = nil, ctx.Err()\n\t}"},
		{`request  controlbudget.Attempt`, `request  controlbudget.Attempt
 transferCurrent controlbudget.Attempt`},
		{`return r.observer.Begin(id, phase)`, `a, err := r.observer.Begin(id, phase); r.transferCurrent = a; return a, err`},
		{`return controlbudget.Attempt{LogicalID: id, Ordinal: r.ordinal, Phase: phase, StartedMonoNS: now}, err`, `a := controlbudget.Attempt{LogicalID: id, Ordinal: r.ordinal, Phase: phase, StartedMonoNS: now}; r.transferCurrent = a; return a, err`},
		{`transferRequestAttempt(requestCtx, nc, subject, payload)`, `transferRequestAttempt(context.WithValue(requestCtx, transferDropAttemptKey{}, recorder), nc, subject, payload)`}},
		transferCtlReplyDropOverlay)
	transferPatchSource(t, root, dir, replacements, "internal/xferclient/status.go", [][2]string{
		{"response, attempt, err := c.cfg.Transport.Request(request, limit)\n\t\tc.observeAttempt(attempt)\n\t\tif err != nil {", "transferWatchGrantDrop(c, request)\n\t\tresponse, attempt, err := c.cfg.Transport.Request(request, limit)\n\t\tc.observeAttempt(attempt)\n\t\tif err != nil {"}},
		transferCtlWitnessOverlay)
	// The agent may already land with the grant it received, so the broker's
	// witness is emitted at the drop itself; it then waits out its real budget.
	transferPatchSource(t, root, dir, replacements, "internal/xferserve/link.go", [][2]string{
		{"\t\"github.com/LinZiyang666/tether/internal/controlbudget\"\n", "\t\"github.com/LinZiyang666/tether/internal/controlbudget\"\n\t\"github.com/LinZiyang666/tether/internal/drillobserve\"\n"},
		{"msg, err := sub.NextMsgWithContext(ctx)", "msg, err := sub.NextMsgWithContext(ctx)\n\tif err == nil && link.Op == proto.TransferLinkGrant && link.Grant != nil && transferDropLinkReply() {\n\t\tlost := attempt\n\t\tlost.FinishedMonoNS, _ = controlbudget.Now()\n\t\tlost.Class = \"reply\"\n\t\tdrillobserve.TransferReplyDropped(link.Identity.TransferID, link.Grant.GrantNonce, lost)\n\t\t<-ctx.Done()\n\t\tmsg, err = nil, ctx.Err()\n\t}"},
		{"\t\tattempt.Class = \"reply\"\n", "\t\tattempt.Class = \"reply\"\n\t\tif link.Op == proto.TransferLinkGrant && link.Grant != nil {\n\t\t\tdrillobserve.TransferLinkReplyLost(link.Identity.TransferID, link.Grant.GrantNonce, attempt)\n\t\t}\n"}}, `
func transferDropLinkReply() bool {
	root := os.Getenv("S7_TRANSFER_FAULT_ROOT")
	return root != "" && os.Rename(root+"/fault-grant-reply", root+"/fault-grant-reply.entered") == nil
}
`)
}

// transferWindowProducers names the processes whose own record proves each
// boundary. Direction decides which endpoint sends and which receives.
// origin: S7 FIX-CUT / hot-update S7 plan §10.2 / §10.4
func transferWindowProducers(cell transferCell) []string {
	sender, receiver := "ctl", "agent"
	if cell.direction == "pull" {
		sender, receiver = "agent", "ctl"
	}
	switch cell.window {
	case "prepare-accepted":
		return []string{"agent"}
	case "source-pinned", "sender-chunk-pending":
		return []string{sender}
	case "inline-send-unconfirmed":
		return []string{"ctl", "agent"}
	case "receiver-write", "Sync-pending", "synced-prefix", "pre-primitive", "post-primitive-pre-outcome":
		return []string{receiver}
	case "grant-reply-lost":
		if cell.direction == "pull" {
			return []string{"ctl"}
		}
		return []string{"broker"}
	case "terminal-pre-history-ACK", "cleanup-pending":
		return []string{"broker"}
	default:
		return nil
	}
}

// A cut of another process happens while the producer is parked. When the
// only producer is the process being cut, its own Freeze needs the owner the
// producer holds: the harness then releases at cut entry and says so.
func transferHoldRoles(cell transferCell) (roles []string, cutEntryRelease bool) {
	producers := transferWindowProducers(cell)
	for _, role := range producers {
		if role != cell.layer {
			roles = append(roles, role)
		}
	}
	if len(roles) == 0 {
		return producers, true
	}
	return roles, false
}

// transferHold is the harness half of one armed barrier.
type transferHold struct {
	t                   *testing.T
	root, seq           string
	cutEntryRelease     bool
	released            bool
	pid                 int
	start, gen, ordinal uint64
	enteredNS, wokeNS   int64
	releasedNS          int64
}

var transferHoldSequence atomic.Uint64

func transferBoottime(t *testing.T) int64 {
	t.Helper()
	var now unix.Timespec
	transferMust(t, unix.ClockGettime(unix.CLOCK_BOOTTIME, &now))
	return now.Nano()
}

func transferWriteAtomic(t *testing.T, path, body string) {
	t.Helper()
	transferMust(t, os.WriteFile(path+".tmp", []byte(body), 0600))
	transferMust(t, os.Rename(path+".tmp", path))
}

// armTransferHold arms exactly one boundary before the original ctl starts.
func armTransferHold(t *testing.T, root string, cell transferCell, size int64) *transferHold {
	t.Helper()
	roles, cutEntry := transferHoldRoles(cell)
	if len(roles) == 0 {
		t.Fatalf("live-cut barrier has no producer for %s", cell.window)
	}
	h := &transferHold{t: t, root: root, seq: strconv.FormatUint(transferHoldSequence.Add(1), 10), cutEntryRelease: cutEntry}
	transferWriteAtomic(t, filepath.Join(root, "hold-arm"), fmt.Sprintf("%s %s %s %s %d\n", h.seq, cell.window, strings.Join(roles, ","), cell.tier, size))
	return h
}

func (h *transferHold) path(suffix string) string {
	return filepath.Join(h.root, "hold-"+h.seq+"-"+suffix)
}

// await returns the exact parked record from its producer's sealed custody.
// A phase-matching row from another producer or ordinal cannot stand in.
func (h *transferHold) await(s *transferStack, r *transferRun) x.Observation {
	t := h.t
	t.Helper()
	if !testharness.WaitFor(t, 20*time.Second, 5*time.Millisecond, func() bool {
		body, err := os.ReadFile(h.path("entered"))
		if err == nil {
			h.pid, h.start, h.gen, h.ordinal, h.enteredNS, err = parseTransferHoldEntry(string(body))
			transferMust(t, err)
			return true
		}
		// A command's native refusal is a more useful dependency diagnostic than
		// spending the remaining live-window budget waiting for nonexistent IO.
		// Cleanup follows the exact receipt the original ctl completed with.
		if receipt := s.ctl.Receipt(); receipt.NativeRC != -1 && !transferCtlMayComplete(r.cell, receipt) {
			t.Fatalf("INCOMPLETE: original ctl exited before %s: native_rc=%d %s", r.cell.window, receipt.NativeRC, s.output(s.ctl))
		}
		return false
	}) {
		t.Fatalf("NOT_RUN pending P28/P29 integration: no nonempty production boundary %s; completed/missed windows cannot certify a cut", r.cell.window)
	}
	var found x.Observation
	if !testharness.WaitFor(t, 5*time.Second, 5*time.Millisecond, func() bool {
		for _, c := range s.collectors {
			// The parked record lives in its producer's own custody. Another
			// stream (a cleanup cell's ctl may already have exited and lost
			// its identity sample) cannot hold it, nor fail this lookup.
			// origin: S7 FIX-TIERA2 — T35 tier-a cleanup-pending "PID recycled during sample"
			if c.process == nil || c.process.Receipt().PID != h.pid {
				continue
			}
			rows, err := c.snapshot()
			transferMust(t, err)
			for _, row := range rows {
				if row.PID == h.pid && row.ProcessStart == h.start && row.Generation == h.gen && row.EventSeq == h.ordinal {
					found = row
					return true
				}
			}
		}
		return false
	}) {
		t.Fatal("parked producer record is missing from its sealed custody")
	}
	return found
}

// Only the broker's cleanup window follows the original ctl's own successful
// completion; every earlier boundary still needs the original ctl alive.
func transferCtlMayComplete(cell transferCell, receipt transferharness.Receipt) bool {
	return cell.window == "cleanup-pending" && receipt.NativeRC == 0
}

// transferCtlAcrossCut samples the original ctl while the boundary is held. A
// cleanup cell's ctl may already have completed; its own receipt then binds the
// same PID/start/artifact and its native outcome, which the caller still
// requires to be success. After a release, completion may race the sample; a
// cleanup cell's ctl may also exit between the sample's two procfs reads,
// before its supervisor settles it, so the same allowance applies there.
// origin: S7 FIX-CUT final T46 run round-3 cleanup-pending "PID recycled during sample"
func transferCtlAcrossCut(t *testing.T, s *transferStack, cell transferCell, released bool) transferharness.Receipt {
	t.Helper()
	accept := func(done transferharness.Receipt) transferharness.Receipt {
		if err := transferCtlExitAllowed(cell, done, released); err != nil {
			t.Fatalf("%v: native_rc=%d %s", err, done.NativeRC, s.output(s.ctl))
		}
		return done
	}
	if done := s.ctl.Receipt(); done.NativeRC != -1 {
		return accept(done)
	}
	after, err := s.ctl.SampleIdentity()
	if err != nil {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		// A failed command can disappear before the supervisor records it too.
		// Join for its real outcome, then reject an early exit explicitly; no
		// missing procfs entry grants permission to accept a premature exit.
		// origin: S7 internal review FIX-TIERA2 F3 / hot-update S7 plan §10.4 / T46
		if done, waitErr := s.ctl.Wait(ctx); waitErr == nil || done.NativeRC != -1 {
			return accept(done)
		}
	}
	transferMust(t, err)
	return after
}

// release proves the producer stayed parked until now and resumed afterwards.
func (h *transferHold) release() {
	t := h.t
	t.Helper()
	if h.released {
		return
	}
	if _, err := os.Stat(h.path("woke")); err == nil {
		t.Fatal("live-cut barrier did not hold its producer: it resumed before the harness released it")
	}
	h.releasedNS = transferBoottime(t)
	transferWriteAtomic(t, h.path("release"), "release\n")
	h.released = true
	if !testharness.WaitFor(t, 10*time.Second, 2*time.Millisecond, func() bool {
		_, err := os.Stat(h.path("woke"))
		return err == nil
	}) {
		t.Fatal("parked producer did not resume after its barrier release")
	}
	woke, err := transferHoldResumed(h.path("woke"), h.releasedNS, h.enteredNS)
	transferMust(t, err)
	h.wokeNS = woke
}

// releaseQuietly unparks a failed cell's producer before fixture teardown.
func (h *transferHold) releaseQuietly() {
	if !h.released {
		_ = os.WriteFile(h.path("release"), []byte("release\n"), 0600)
		h.released = true
	}
}

// Arming a reply drop is part of the grant-reply-lost cell itself. The single
// fault is consumed by the first matching reply at the requester's edge.
func armTransferReplyDrop(t *testing.T, root string, cell transferCell) {
	t.Helper()
	if cell.window == "grant-reply-lost" {
		transferWriteAtomic(t, filepath.Join(root, "fault-grant-reply"), "armed\n")
	}
}

// transferRequestCensus counts the ctl's original reserve requests by logical
// call. It keeps only the call identity, never grants or capsules.
type transferRequestCensus struct {
	mu    sync.Mutex
	calls map[string]int
}

func watchTransferRequests(t *testing.T, s *transferStack, cell transferCell) *transferRequestCensus {
	t.Helper()
	c := &transferRequestCensus{calls: make(map[string]int)}
	if cell.window != "grant-reply-lost" {
		return c
	}
	subject, err := proto.SubjCtrlTransferRequestWildcard(proto.TransferOpReserve)
	transferMust(t, err)
	sub, err := s.nc.Subscribe(subject, func(m *nats.Msg) {
		var v struct {
			CallID string `json:"call_id"`
		}
		if json.Unmarshal(m.Data, &v) != nil || v.CallID == "" {
			return
		}
		c.mu.Lock()
		c.calls[v.CallID]++
		c.mu.Unlock()
	})
	transferMust(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	transferMust(t, s.nc.FlushTimeout(time.Second))
	return c
}

func (c *transferRequestCensus) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.calls))
	for id, n := range c.calls {
		out[id] = n
	}
	return out
}

// transferCutHoldCuts is the most authorized cuts any cell runs while its
// producer stays parked: an agent or broker cut and its return (A→B→A).
const transferCutHoldCuts = 2

// transferCutHoldBound is how long a parked producer may wait for its release
// before it poisons custody. A parked producer is released at the first cut's
// journaled freeze intent or after every cut, so the bound must cover N whole
// authorized cuts. Each is bounded by product budgets: the target-issued grant
// lives ComponentHandover from prepare (stage and start must use it), then the
// Start crosses the safe-point wait, the local freeze window and the reply
// drain, a deferral is retried once after RollbackCutRetry, and the wait/
// completion is one more control request. The former 90s literal was set for
// the legacy dispatch; two authorized cuts measured 60–90s under the 220-cell
// load and poisoned producers that were still legitimately parked. This
// bounds only the hermetic park: it grants no transfer or transaction any
// byte lifetime, and no product budget changes.
// origin: S7 internal review CUT-CAPACITY F4 / hot-update S7 plan §1.2 / §10.2
func transferCutHoldBound(cuts int) time.Duration {
	perCut := upgradebudget.ComponentHandover + upgradebudget.SafePointWait + upgradebudget.LocalFreezeWindow +
		upgradebudget.ReplyDrain + upgradebudget.RollbackCutRetry + upgradebudget.RequestLatency
	return upgradebudget.RequestLatency + time.Duration(cuts)*perCut
}

// transferCutHoldSource is the barrier overlay with its park bound declared
// from the budget SSOT; the overlay text names no literal duration.
func transferCutHoldSource(cuts int) string {
	return transferCutHoldOverlay + fmt.Sprintf("\nconst transferCutHoldLimit = time.Duration(%d)\n", int64(transferCutHoldBound(cuts)))
}

// The released producer's bound must outlast every legitimately parked cut and
// still be the only park limit in the overlay the binaries are built from.
// origin: S7 internal review CUT-CAPACITY F4 / hot-update S7 plan §1.2
func TestTransferProducerParkCoversEveryAuthorizedCut(t *testing.T) {
	if strings.Count(transferCutHoldOverlay, "limit := time.Now().Add(") != 1 || strings.Count(transferCutHoldOverlay, "limit := time.Now().Add(transferCutHoldLimit)") != 1 {
		t.Fatal("producer park uses a literal bound instead of the derived transferCutHoldLimit")
	}
	for _, cuts := range []int{1, transferCutHoldCuts, 3} {
		source := transferCutHoldSource(cuts)
		_, value, ok := strings.Cut(source, "const transferCutHoldLimit = time.Duration(")
		if !ok {
			t.Fatal("producer park bound is not declared")
		}
		value, _, _ = strings.Cut(value, ")")
		bound, err := strconv.ParseInt(value, 10, 64)
		// Each cut needs at least its grant lifetime and its freeze safe point.
		floor := time.Duration(cuts) * (upgradebudget.ComponentHandover + upgradebudget.SafePointWait + upgradebudget.RequestLatency)
		if err != nil || time.Duration(bound) < floor || time.Duration(bound) != transferCutHoldBound(cuts) {
			t.Fatalf("%d authorized cuts outlive their producer park: %s %v", cuts, value, err)
		}
	}
}
