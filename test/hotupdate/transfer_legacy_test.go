//go:build linux && hotupdate_integration

package hotupdate_test

// Native harness and oracle controls are separate from P33's four historical
// binary quadrants. They cannot certify legacy interoperability or product cuts.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/transferharness"
	"github.com/nats-io/nats.go"
)

// origin: S7 internal review INTEG-O F2
func TestTransferTopologyStartsPrivateCluster(t *testing.T) {
	a := buildTransferArtifacts(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, topology := range []string{"sole-voter", "quorum"} {
			t.Run(fmt.Sprintf("round-%d/%s", round, topology), func(t *testing.T) { exerciseTransferClusterTopology(t, a, topology) })
		}
	}
	testharness.AssertNoGoroutineLeak(t, "transfer clustered topology", g)
	testharness.AssertNoFDLeak(t, "transfer clustered topology", fd, 0)
}

func exerciseTransferClusterTopology(t *testing.T, a transferArtifacts, topology string) {
	t.Helper()
	s := newTransferTopology(t, a, topology)
	defer s.close()
	r, err := (&adminsock.Client{Path: s.admin, Timeout: 5 * time.Second}).Call(adminsock.Request{Op: adminsock.OpClusterStatus})
	transferMust(t, err)
	voters := 1
	if topology == "quorum" {
		voters = 3
	}
	if !r.OK || r.Cluster == nil || r.Cluster.LeaderID == "" || len(r.Cluster.Nodes) != voters {
		t.Fatalf("real %s cluster missing: %+v", topology, r)
	}
	if s.cluster == nil || s.cluster.Replicas != voters || len(s.cluster.Servers) != voters {
		t.Fatal("requested cluster was replaced with a standalone fixture")
	}
	rows, err := s.cluster.Census()
	transferMust(t, err)
	if len(rows) != voters {
		t.Fatal("process census omitted a real voter")
	}
	for i, row := range rows {
		t.Logf("topology=%s node=%s pid=%d process_start=%d boot_id=%s running_sha=%s", topology, s.cluster.Peers[i].ID, row.PID, row.ProcessStart, row.BootID, row.RunningSHA)
	}
	stream, err := s.js.Stream(t.Context(), jsstream.HistoryStreamName(s.sid))
	transferMust(t, err)
	info, err := stream.Info(t.Context())
	transferMust(t, err)
	if info.Config.Replicas != voters || voters == 3 && (info.Cluster == nil || len(info.Cluster.Replicas) != 2) {
		t.Fatalf("history is not real R%d: %+v", voters, info)
	}
	if voters == 3 && (s.cluster.Peers[s.originIndex].ID == r.Cluster.LeaderID || s.originIndex == s.ingressIndex || s.url == s.ns.ClientURL()) {
		t.Fatal("N3 origin must be a follower with separate ctl ingress")
	}
}

// Original transfer bytes, kernel custody and exact history use the same
// independent oracles as T36. The leader change occurs inside that invocation.
// origin: S7 internal review INTEG-O F2 / hot-update S7 plan §6.3 / T39
func TestTransferClusterLeaderChangeKeepsOriginalInvocation(t *testing.T) {
	a := buildTransferArtifacts(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { t.Parallel(); exerciseTransferClusterLeaderChange(t, a) })
		}
	})
	testharness.AssertNoGoroutineLeak(t, "original clustered transfer", g)
	testharness.AssertNoFDLeak(t, "original clustered transfer", fd, 0)
}
func exerciseTransferClusterLeaderChange(t *testing.T, a transferArtifacts) {
	for _, topology := range []string{"sole-voter", "quorum"} {
		for _, direction := range []string{"push", "pull"} {
			t.Run(topology+"/"+direction, func(t *testing.T) {
				t.Parallel()
				s := newTransferTopology(t, a, topology)
				s.requireCapable()
				runTransferCell(t, s, transferCell{"b", direction, "js", "synced-prefix"}, func(func()) {
					id, origin := transferClusterOriginalAuthority(t, s)
					// drill-pin: xferclient.Client.callID preserves tid + "-status".
					before := transferClusterRequest(t, s, x.Request{Op: x.OpStatus, CallID: id.TransferID + "-status", Identity: id, Origin: origin})
					if before.Code != "" || before.Status == nil || before.Status.State != x.StatusLive {
						t.Fatalf("original live entry missing before leader change: %+v", before)
					}
					if topology == "quorum" {
						transferClusterChangeLeader(t, s)
					}
					after := transferClusterRequest(t, s, x.Request{Op: x.OpStatus, CallID: id.TransferID + "-status", Identity: x.Identity{SID: id.SID, NID: id.NID, ActorFP: id.ActorFP, TransferID: id.TransferID}})
					if after.Code != "" || after.Status == nil || after.Status.Identity != id || after.Status.Origin != origin || after.Status.Budget.StartedAt != before.Status.Budget.StartedAt {
						t.Fatalf("leader change replaced original authority/budget: %+v", after)
					}
				})
			})
		}
	}
}

// Retiring the original follower cannot create a replacement permit or turn an
// unreachable owner into an authoritative unknown. Membership changes use the
// real retire controller, with its production drain/catch-up timers untouched.
// origin: S7 internal review INTEG-O F2 / hot-update S7 plan §6.1 / T39
func TestTransferClusterOriginRemovalRefusesReplacement(t *testing.T) {
	a := buildTransferArtifacts(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { t.Parallel(); exerciseTransferClusterOriginRemoval(t, a) })
		}
	})
	testharness.AssertNoGoroutineLeak(t, "removed original transfer origin", g)
	testharness.AssertNoFDLeak(t, "removed original transfer origin", fd, 0)
}
func exerciseTransferClusterOriginRemoval(t *testing.T, a transferArtifacts) {
	s := newTransferTopology(t, a, "quorum")
	s.requireCapable()
	r := startTransferRun(t, s, transferCell{"b", "push", "js", "source-pinned"})
	defer r.watch.close()
	r.boundary = awaitTransferBoundary(t, s, r)
	id, origin := transferClusterOriginalAuthority(t, s)
	intent := x.Intent{Verb: "push", Tier: "a", Path: filepath.Join(s.h.Root, "issued-only"), SHA256: transferDigest(nil)}
	issued := x.Request{Op: x.OpPermit, CallID: "issued-only", Identity: x.Identity{SID: id.SID, NID: id.NID, TransferID: "issued-only", AgentInstance: id.AgentInstance}, Intent: &intent}
	before := transferClusterRequest(t, s, issued)
	if before.Code != "" || before.Permit == nil || before.Permit.Origin != origin {
		t.Fatalf("original issued-only permit missing: %+v", before)
	}
	leader := transferClusterChangeLeader(t, s)
	p := s.cluster.Peers[leader]
	reply, err := (&adminsock.Client{Path: p.Admin, Timeout: 5 * time.Second}).Call(adminsock.Request{Op: adminsock.OpClusterRetire, NodeID: origin.BrokerID, Confirmed: true})
	transferMust(t, err)
	if !reply.OK || reply.OpID == "" {
		t.Fatalf("NOT_RUN pending cluster retirement owner: code=%s error=%s", reply.Code, reply.Error)
	}
	if !testharness.WaitFor(t, 60*time.Second, 20*time.Millisecond, func() bool {
		view, err := (&adminsock.Client{Path: p.Admin, Timeout: 5 * time.Second}).Call(adminsock.Request{Op: adminsock.OpClusterStatus})
		if err != nil || !view.OK || view.Cluster == nil || len(view.Cluster.Nodes) != 2 {
			return false
		}
		for _, row := range view.Cluster.Nodes {
			if row.NodeID == origin.BrokerID {
				return false
			}
		}
		return true
	}) {
		t.Fatal("NOT_RUN pending cluster retirement owner: original follower did not leave committed roster")
	}
	transferMust(t, s.broker.Stop())
	after := transferClusterRequest(t, s, issued)
	if after.Permit == nil || *after.Permit != *before.Permit {
		t.Fatalf("removed origin replaced/lost immutable permit: code=%s error=%s", after.Code, after.Error)
	}
	missing := transferClusterRequest(t, s, x.Request{Op: x.OpStatus, CallID: "unreachable-origin", Identity: id, Origin: origin})
	if missing.Code != x.CodeOwnerRecovering || missing.Status != nil && missing.Status.State == x.StatusUnknown {
		t.Fatalf("unreachable origin supplied false absence: %+v", missing)
	}
	issued.Identity.TransferID, issued.CallID = "new-after-loss", "new-after-loss"
	fresh := transferClusterRequest(t, s, issued)
	if fresh.Code != x.CodeOwnerRecovering || fresh.Permit != nil {
		t.Fatalf("origin loss authorized replacement work: %+v", fresh)
	}
}

func transferClusterOriginalAuthority(t *testing.T, s *transferStack) (x.Identity, x.Origin) {
	t.Helper()
	s.wireMu.Lock()
	defer s.wireMu.Unlock()
	for _, message := range s.wire {
		var request struct {
			Continuation *proto.TransferContinuation `json:"continuation"`
		}
		if json.Unmarshal(message.Data, &request) != nil || request.Continuation == nil {
			continue
		}
		id, origin := x.Identity(request.Continuation.Identity), x.Origin(request.Continuation.Origin)
		transferMust(t, proto.ValidateTransferWire(proto.Identity(id)))
		if s.cluster == nil || origin.BrokerID != s.cluster.Peers[s.originIndex].ID {
			t.Fatal("original request did not name the selected real follower")
		}
		return id, origin
	}
	t.Fatal("original transfer supplied no retained authority")
	return x.Identity{}, x.Origin{}
}
func transferClusterRequest(t *testing.T, s *transferStack, req x.Request) x.Response {
	t.Helper()
	id, err := cli.EnsureIdentity(s.home)
	transferMust(t, err)
	options := append(natsinbox.InboxConnectOptions(id.PublicKey), nats.Nkey(id.PublicKey, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(id.Seed, nonce) }), nats.Name(cli.CtlNameForSession(s.sid)), nats.NoReconnect())
	nc, err := nats.Connect(s.url, options...)
	transferMust(t, err)
	defer nc.Close()
	subject, data, err := proto.EncodeTransferRequest(id.PublicKey, req)
	transferMust(t, err)
	msg, err := nc.Request(subject, data, 5*time.Second)
	transferMust(t, err)
	response, err := proto.DecodeTransferResponse(req.Op, "push", msg.Data)
	transferMust(t, err)
	return response
}
func transferClusterChangeLeader(t *testing.T, s *transferStack) int {
	t.Helper()
	leader, err := s.cluster.Leader()
	transferMust(t, err)
	target := s.ingressIndex
	if leader == target {
		target = (leader + 1) % 3
		if target == s.originIndex {
			target = (target + 1) % 3
		}
	}
	p := s.cluster.Peers[leader]
	r, err := (&adminsock.Client{Path: p.Admin, Timeout: 5 * time.Second}).Call(adminsock.Request{Op: adminsock.OpClusterTransfer, NodeID: s.cluster.Peers[target].ID})
	transferMust(t, err)
	if !r.OK {
		t.Fatalf("real leader transfer refused: code=%s error=%s", r.Code, r.Error)
	}
	if !testharness.WaitFor(t, 5*time.Second, 20*time.Millisecond, func() bool { got, err := s.cluster.Leader(); return err == nil && got == target }) {
		t.Fatal("real leader transfer did not converge")
	}
	return target
}

// No test may substitute single for a requested Raft topology. The existing
// S6 cluster fixture owns its process creation and currently has no transfer
// session/custody attachment interface. Keep this dependency explicit.
// This transfer-specific fixture supplies that attachment independently, with
// private binary/store/authority paths and the original custody readers.
// origin: hot-update S7 plan §10.1 / T36 / §11.3
func newTransferTopology(t *testing.T, a transferArtifacts, topology string) *transferStack {
	t.Helper()
	switch topology {
	case "single", "sole-voter", "quorum":
		return newTransferStackInTopology(t, a, topology)
	default:
		t.Fatalf("unknown transfer topology %q", topology)
		return nil
	}
}

// origin: hot-update S7 plan §11.3 / lead L16
func TestTransferBinaryHarnessNoLeaks(t *testing.T) {
	a := buildTransferArtifacts(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseTransferBinaryHarness(t, a) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer binary harness", g)
	testharness.AssertNoFDLeak(t, "transfer binary harness", fd, 0)
}
func exerciseTransferBinaryHarness(t *testing.T, a transferArtifacts) {
	t.Helper()
	// Partial launch must release custody without waiting on an unstarted loop.
	custody := newTransferCollector(t)
	custody.close()
	h := transferharness.New(t, transferSupervisor{})
	path := filepath.Join(h.Root, "product")
	transferCopy(t, a.a, path)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, args := range [][]string{{"version"}, {"transfer", "status"}} {
		p, err := h.Start(ctx, transferharness.Spec{Binary: path, ArtifactSHA: a.shaA, Args: args,
			Env: []string{"HOME=" + h.Root, "GORACE=atexit_sleep_ms=0"}})
		transferMust(t, err)
		r, err := p.Wait(ctx)
		if len(args) == 1 {
			transferMust(t, err)
			out, err := os.ReadFile(r.StdoutPath)
			transferMust(t, err)
			if r.NativeRC != 0 || !bytes.Contains(out, []byte("image-abi v1")) {
				t.Fatalf("actual product version/ABI missing: native_rc=%d %s", r.NativeRC, out)
			}
		} else if err == nil || r.NativeRC != 64 {
			t.Fatalf("native CLI usage refusal was translated: native_rc=%d err=%v", r.NativeRC, err)
		}
	}
	transferMust(t, h.Close())
}

// These are oracle controls, not M08–M13 product mutation receipts. Synthetic
// inputs prove each predicate rejects its defect before blocked producers land.
// origin: hot-update S7 plan §10.5 / T35–T38 / M08–M13
func TestTransferBinaryOracleControls(t *testing.T) {
	// origin: S7 internal review TAGGED-SUITE-SWEEP-A F1
	t.Run("overlay-anchors", func(t *testing.T) {
		edits := [][2]string{{"original", "current"}, {"current", "instrumented"}}
		got, err := transferOverlayEdits("original", edits)
		transferMust(t, err)
		if got != "instrumented" {
			t.Fatalf("sequential overlay edits lost an earlier patch: %q", got)
		}
		for _, source := range []string{"moved", "original original", "original current"} {
			if _, err := transferOverlayEdits(source, edits); err == nil {
				t.Fatalf("missing or ambiguous overlay anchor accepted: %q", source)
			}
		}
		// Later adapters must read the earlier replacement rather than the
		// product file; otherwise two valid patches silently lose the first.
		root, dir := t.TempDir(), t.TempDir()
		path := filepath.Join(root, "owner.go")
		transferMust(t, os.WriteFile(path, []byte("original"), 0600))
		replacements := make(map[string]string)
		transferPatchSource(t, root, dir, replacements, "owner.go", edits[:1], " suffix")
		transferPatchSource(t, root, dir, replacements, "owner.go", edits[1:], "")
		data, err := os.ReadFile(replacements[path])
		transferMust(t, err)
		if string(data) != "instrumented suffix" {
			t.Fatalf("composed overlay lost instrumentation: %q", data)
		}
	})
	t.Run("original-call", func(t *testing.T) {
		original := &nats.Msg{Subject: "original-prepare", Data: []byte(`{"transfer_id":"id","call_id":"prepare"}`)}
		s := &transferStack{wire: []*nats.Msg{original, original}}
		transferMust(t, s.verifyCalls("id"))
		for _, next := range []string{`{"transfer_id":"replacement","call_id":"prepare"}`, `{"transfer_id":"id","call_id":"replacement"}`} {
			s.wire = []*nats.Msg{original, {Subject: original.Subject, Data: []byte(next)}}
			if s.verifyCalls("id") == nil {
				t.Fatal("replacement transfer or logical call identity accepted")
			}
		}
		s.wire = nil
		if s.verifyCalls("id") == nil {
			t.Fatal("empty request identity census accepted")
		}
	})
	t.Run("matrix", func(t *testing.T) {
		for _, layer := range []string{"agent", "broker", "js"} {
			cells := transferCells(layer)
			if len(cells) != 44 {
				t.Fatalf("matrix lost cells: %d", len(cells))
			}
			seen := make(map[string]bool)
			for _, c := range cells {
				if seen[c.name()] || c.layer != layer {
					t.Fatal("duplicate or wrong-layer cell")
				}
				seen[c.name()] = true
			}
			if !seen["tier-a/push/inline-send-unconfirmed"] || seen["tier-a/push/sender-chunk-pending"] {
				t.Fatal("tier A must replace only its structural OBJ cell")
			}
			// Plan §10.2: only the tier A receiver data cells are structurally
			// N/A; every primitive/grant/settlement window and all of tier B
			// stay applicable and must reach their own cut.
			// origin: S7 FIX-TIERA2 / hot-update S7 plan §10.2 / T35
			notApplicable := 0
			for _, c := range cells {
				if transferStructuralNA(c) == "" {
					continue
				}
				notApplicable++
				if c.tier != "a" || c.window != "receiver-write" && c.window != "Sync-pending" && c.window != "synced-prefix" {
					t.Fatalf("%s is applicable but marked structurally N/A", c.name())
				}
			}
			if notApplicable != 6 {
				t.Fatalf("structural N/A set has %d cells, want the 6 tier A receiver data cells", notApplicable)
			}
		}
	})
	// A primitive boundary is the whole payload's durable Sync held in its
	// original tmp/parent; a partial prefix or a released FD is not one.
	// origin: S7 FIX-TIERA2 / hot-update S7 plan §3 / §10.2 / T35
	t.Run("primitive-boundary", func(t *testing.T) {
		for _, tier := range []string{"a", "b"} {
			for _, window := range []string{"pre-primitive", "post-primitive-pre-outcome"} {
				row := transferOracleObservation()
				row.Kind, row.Outcome, row.DurableOffset = x.ObservePrimitiveEnter, x.OutcomeNotEntered, 100
				if window == "post-primitive-pre-outcome" {
					row.Kind, row.Outcome = x.ObservePrimitiveExit, x.OutcomeLanded
				}
				if tier == "a" {
					row.ObjectVersion = ""
				}
				cell := transferCell{tier, "push", "agent", window}
				transferMust(t, validateTransferBoundary(cell, row, 100))
				for _, mutate := range []func(*x.Observation){func(o *x.Observation) { o.DurableOffset = 99 }, func(o *x.Observation) { o.DurableOffset = 0 },
					func(o *x.Observation) { o.Tmp = x.ResourceRef{} }, func(o *x.Observation) { o.Parent = x.ResourceRef{} }} {
					bad := row
					mutate(&bad)
					if validateTransferBoundary(cell, bad, 100) == nil {
						t.Fatalf("%s/%s accepted a primitive boundary before the whole durable payload", tier, window)
					}
				}
			}
		}
	})
	t.Run("process", func(t *testing.T) {
		before := transferharness.Receipt{PID: 7, ProcessStart: 11, BootID: "boot", RunningSHA: "A"}
		after := before
		after.RunningSHA = "B"
		transferMust(t, validateTransferProcess(before, after, "B"))
		for _, mutate := range []func(*transferharness.Receipt){func(r *transferharness.Receipt) { r.PID++ }, func(r *transferharness.Receipt) { r.ProcessStart++ }, func(r *transferharness.Receipt) { r.BootID = "other" }, func(r *transferharness.Receipt) { r.RunningSHA = "A" }} {
			bad := after
			mutate(&bad)
			if validateTransferProcess(before, bad, "B") == nil {
				t.Fatal("replacement process/artifact accepted")
			}
		}
	})
	t.Run("checkpoint", func(t *testing.T) {
		cell := transferCell{"b", "push", "agent", "synced-prefix"}
		before := transferOracleObservation()
		transferMust(t, validateTransferBoundary(cell, before, 100))
		for _, mutate := range []func(*x.Observation){func(o *x.Observation) { o.EntryNonce = "" }, func(o *x.Observation) { o.DurableOffset = 0 }, func(o *x.Observation) { o.DurableOffset = 100 }, func(o *x.Observation) { o.FileSynced = false }, func(o *x.Observation) { o.Tmp = x.ResourceRef{} }, func(o *x.Observation) { o.OriginalDeadline.Deadline = 0 }} {
			bad := before
			mutate(&bad)
			if validateTransferBoundary(cell, bad, 100) == nil {
				t.Fatal("empty or unconfirmed transfer boundary accepted")
			}
		}
		after := before
		after.Kind = x.ObserveImport
		after.Generation++
		after.EventSeq++
		after.BoottimeNS++
		after.CutID = "cut"
		after.DurableOffset++
		after.Index++
		after.LastConsumedStreamSeq++
		transferMust(t, validateTransferContinuation(cell, before, before, transferharness.Receipt{}, []x.Observation{after}))
		for _, tc := range []struct {
			name   string
			mutate func(*x.Observation)
		}{
			{"M08_lost_checkpoint", func(o *x.Observation) { o.DurableOffset = 0 }},
			{"M08_wrong_sequence", func(o *x.Observation) { o.LastConsumedStreamSeq = 0 }},
			{"M08_hash_ahead", func(o *x.Observation) { o.DurableOffset = before.DurableOffset; o.HashStateDigest = "different" }},
			{"M11_reset_deadline", func(o *x.Observation) { o.OriginalDeadline.Deadline++ }},
			{"M11_no_import", func(o *x.Observation) { o.Kind = x.ObserveSynced }},
			{"M01_new_object", func(o *x.Observation) { o.ObjectVersion = "new" }},
			{"M07_new_entry", func(o *x.Observation) { o.EntryNonce = "new" }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				bad := after
				tc.mutate(&bad)
				if validateTransferContinuation(cell, before, before, transferharness.Receipt{}, []x.Observation{bad}) == nil {
					t.Fatal("mutated continuation accepted")
				}
			})
		}
		// A broker import is checked against the tracker's state. The original
		// receiver still has to advance, but need not import an image itself.
		brokerCell := cell
		brokerCell.layer = "broker"
		tracker := before
		tracker.PID = 8
		tracker.DurableOffset, tracker.Index, tracker.LastConsumedStreamSeq = 0, 0, 0
		tracker.HashStateDigest = ""
		tracker.OriginalDeadline.Deadline = 2000
		installed := tracker
		installed.Kind, installed.CutID = x.ObserveInstall, "broker-cut"
		installed.Generation++
		installed.BoottimeNS++
		progress := after
		progress.Kind = x.ObserveSynced
		transferMust(t, validateTransferContinuation(brokerCell, before, tracker, transferharness.Receipt{}, []x.Observation{progress, installed}))
		if validateTransferContinuation(brokerCell, before, tracker, transferharness.Receipt{}, []x.Observation{progress}) == nil {
			t.Fatal("receiver progress hid missing tracker import")
		}
	})
	t.Run("history", func(t *testing.T) {
		start := []byte(`{"transfer_id":"id","entry_nonce":"nonce","kind":"start"}`)
		terminal := []byte(`{"transfer_id":"id","entry_nonce":"nonce","kind":"complete","terminal_id":"terminal","sha256":"digest","bytes":100}`)
		rows := []transferharness.RawMessage{{Sequence: 1, Data: start}, {Sequence: 2, Data: terminal}}
		transferMust(t, validateTransferHistory(rows, "id", "nonce", "digest", 100))
		acks := []x.Observation{{Kind: x.ObserveHistory, TransferID: "id", HistorySeq: 1, CanonicalDigest: transferDigest(start)}, {Kind: x.ObserveHistory, TransferID: "id", HistorySeq: 2, CanonicalDigest: transferDigest(terminal)}}
		transferMust(t, validateTransferHistoryReceipts(rows, acks, "id"))
		acks[1].CanonicalDigest = "opposite"
		if validateTransferHistoryReceipts(rows, acks, "id") == nil {
			t.Fatal("boolean terminal existence accepted instead of exact history receipt")
		}
		for _, tc := range []struct {
			name string
			rows []transferharness.RawMessage
		}{
			{"M12_duplicate", append(append([]transferharness.RawMessage(nil), rows...), transferharness.RawMessage{Sequence: 3, Data: terminal})},
			{"M12_opposite", []transferharness.RawMessage{{Sequence: 1, Data: start}, {Sequence: 2, Data: bytes.Replace(terminal, []byte("complete"), []byte("failed"), 1)}}},
			{"M12_missing_start", rows[1:]},
			{"M12_wrong_content", []transferharness.RawMessage{{Sequence: 1, Data: start}, {Sequence: 2, Data: bytes.Replace(terminal, []byte("digest"), []byte("other"), 1)}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if validateTransferHistory(tc.rows, "id", "nonce", "digest", 100) == nil {
					t.Fatal("nonexact history accepted")
				}
			})
		}
	})
	t.Run("primitive", func(t *testing.T) {
		rows := []x.Observation{{TransferID: "id", Kind: x.ObservePrimitiveEnter}, {TransferID: "id", Kind: x.ObservePrimitiveExit, Outcome: x.OutcomeLanded}, {TransferID: "id", Kind: x.ObserveDirectorySync, ParentSynced: true}}
		transferMust(t, validateTransferEffects(rows, "id"))
		for _, bad := range [][]x.Observation{rows[:2], rows[1:], append(append([]x.Observation(nil), rows...), rows[0]), append(append([]x.Observation(nil), rows...), rows[1])} {
			if validateTransferEffects(bad, "id") == nil {
				t.Fatal("missing durability or repeated primitive accepted")
			}
		}
	})
	t.Run("chunks", func(t *testing.T) {
		body := []byte("complete body")
		row := transferharness.RawMessage{Sequence: 7, Subject: "$O.bucket.C.version", Data: body, Headers: map[string][]string{
			proto.TransferHeaderVersion: {"1"}, proto.TransferHeaderIndex: {"0"}, proto.TransferHeaderOffset: {"0"}, proto.TransferHeaderLength: {fmt.Sprint(len(body))}, proto.TransferHeaderSHA256: {transferDigest(body)}}}
		transferMust(t, validateTransferChunks([]transferharness.RawMessage{row}, "version", body))
		for _, field := range []string{proto.TransferHeaderIndex, proto.TransferHeaderOffset, proto.TransferHeaderLength, proto.TransferHeaderSHA256} {
			t.Run(field, func(t *testing.T) {
				encoded, _ := json.Marshal(row)
				var bad transferharness.RawMessage
				transferMust(t, json.Unmarshal(encoded, &bad))
				bad.Headers[field] = []string{"wrong"}
				if validateTransferChunks([]transferharness.RawMessage{bad}, "version", body) == nil {
					t.Fatal("invalid physical receipt accepted")
				}
			})
		}
		if validateTransferChunks([]transferharness.RawMessage{row, row}, "version", body) == nil {
			t.Fatal("duplicate physical chunk accepted")
		}
	})
	t.Run("reaper", func(t *testing.T) {
		rows := []x.Observation{{Kind: x.ObserveInstall, Generation: 2, InstalledBeforeReaper: true}, {Kind: x.ObserveCallbackEnter, Generation: 2}, {Kind: x.ObserveRetirement, Generation: 2, RetirementDigest: "proof"}}
		transferMust(t, validateTransferReaper(rows))
		for _, bad := range [][]x.Observation{rows[1:], {rows[1], rows[0], rows[2]}, {rows[0], rows[2]}, rows[:2]} {
			if validateTransferReaper(bad) == nil {
				t.Fatal("late install or stopped reaper accepted")
			}
		}
	})
	t.Run("rollback", func(t *testing.T) {
		before := transferOracleObservation()
		before.Kind = x.ObserveInstall
		before.Active = true
		before.PrimaryRevision = 2
		before.OutboxRevision = 2
		before.CanonicalDigest = "canonical"
		after := before
		after.Kind = x.ObserveRollback
		after.Generation++
		after.EventSeq++
		after.CutID = "rollback"
		transferMust(t, validateTransferRollback([]x.Observation{before, after}, true))
		for _, mutate := range []func(*x.Observation){func(o *x.Observation) { o.Generation-- }, func(o *x.Observation) { o.OutboxRevision = 1 }, func(o *x.Observation) { o.CanonicalDigest = "other" }, func(o *x.Observation) { o.OriginalDeadline.Deadline++ }, func(o *x.Observation) { o.DurableOffset = 0 }} {
			bad := after
			mutate(&bad)
			if validateTransferRollback([]x.Observation{before, bad}, true) == nil {
				t.Fatal("stale rollback state accepted")
			}
		}
		advanced := before
		advanced.Kind, advanced.EventSeq = x.ObserveSynced, before.EventSeq+1
		advanced.DurableOffset++
		after.EventSeq = advanced.EventSeq + 1
		after.BoottimeNS = advanced.BoottimeNS + 1
		after.DurableOffset = advanced.DurableOffset
		transferMust(t, validateTransferRollbackAdvance(advanced, []x.Observation{after}))
		after.DurableOffset--
		if validateTransferRollbackAdvance(advanced, []x.Observation{after}) == nil {
			t.Fatal("rollback discarded candidate progress without rejection")
		}
		if validateTransferRollbackAdvance(advanced, nil) == nil {
			t.Fatal("missing fresh rollback accepted")
		}
	})
}
func transferOracleObservation() x.Observation {
	return x.Observation{Kind: x.ObserveSynced, Phase: x.PhaseReceiving, PID: 7, ProcessStart: 11, BootID: "boot", Generation: 1, EventSeq: 1, BoottimeNS: 1,
		TransferID: "id", EntryNonce: "nonce", ObjectVersion: "version", DurableOffset: 10, FetchedOffset: 20, Index: 1, LastConsumedStreamSeq: 9, HashStateDigest: "hash", FileSynced: true,
		OriginalDeadline: x.BootDeadline{BootID: "boot", Deadline: 1000}, Tmp: x.ResourceRef{ResourceID: "tmp"}, Parent: x.ResourceRef{ResourceID: "parent"}}
}

// origin: hot-update S7 plan §10.4 / T35 / M10B
func TestTransferDestinationObserverRejectsTransientPartialWrite(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferDestinationObserver(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer independent target observer", g)
	testharness.AssertNoFDLeak(t, "transfer independent target observer", fd, 0)
}
func exerciseTransferDestinationObserver(t *testing.T) {
	path := filepath.Join(t.TempDir(), "target")
	old, body := []byte("old complete"), []byte("new complete")
	transferMust(t, os.WriteFile(path, old, 0600))
	w := startTransferTargetWatch(t, path, old, body)
	defer w.close()
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool { return w.check() == nil }) {
		t.Fatal("target observer did not sample original")
	}
	transferMust(t, os.WriteFile(path, body[:3], 0600))
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool { return w.check() != nil }) {
		t.Fatal("observer missed partial destination")
	}
	transferMust(t, os.WriteFile(path, body, 0600))
	if err := w.check(); err == nil || !strings.Contains(err.Error(), "partial") {
		t.Fatal(errors.New("final repair erased the transient partial-write evidence"))
	}
}
