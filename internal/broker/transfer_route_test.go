package broker

// Real routed NATS and three Raft/SQLite owners exercise the broker adapters.
// Census/probe callbacks supply explicit backend facts in adapter tests only.
// T39 consumes actual business handlers; missing P16/P22/P24 wiring is a refusal,
// never a replacement handler or a fabricated successful controller.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/clusterharness"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"golang.org/x/time/rate"
)

var transferRouteOperations = []string{"probe", "permit", "status", "source", "reserve", "terminal", "cancel", "census"}

func transferRouteSubject(op, tid string) string {
	return proto.SubjectPrefix + ".ctrl.by.actor.s.lab.node.node.transfer." + tid + "." + op + ".req"
}

func transferRouteDeadline(t *testing.T, budget time.Duration) controlbudget.Deadline {
	t.Helper()
	d, err := controlbudget.After(budget)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// origin: hot-update S7 plan §6.3 / T39
func TestTransferBroadcastClassificationKeepsClosedOperationSet(t *testing.T) {
	for _, op := range transferRouteOperations {
		for _, tid := range []string{"*", "original-id"} {
			for _, subject := range []string{transferRouteSubject(op, tid), proto.SubjectPrefix + ".s.*.ev.node.*.transfer." + tid + "." + op + ".req"} {
				if !isBroadcastClusterSubject(subject) {
					t.Errorf("original-owner operation would be queue-routed: %s", subject)
				}
			}
		}
	}
	for _, subject := range []string{transferRouteSubject("*", "*"), transferRouteSubject("unknown", "*"), proto.SubjectPrefix + ".ctrl.by.*.s.*.status.req", proto.SubjectPrefix + ".ctrl.by.*.session.create.req"} {
		if isBroadcastClusterSubject(subject) {
			t.Errorf("unregistered operation was broadcast: %s", subject)
		}
	}
}

// origin: hot-update S7 plan §6.1 / §6.3 / T39
func TestTransferOriginRoutingIgnoresCurrentHome(t *testing.T) {
	db := openBrokerFixtureDB(t)
	seedHomedNode(t, db, "lab", "node", "new-server", "new-home")
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: "original-follower", clusterMode: true}
	origin := xfercontract.Origin{BrokerID: b.selfID, InstanceID: "original-instance", BootID: "original-boot", Generation: 7}
	if b.transferHomeGate("lab", "node") || !transferOriginGate(b, origin) {
		t.Fatal("rehome redirected the accepted transfer away from its original follower")
	}
	for _, id := range []string{"new-home", "ingress", ""} {
		b.selfID = id
		if transferOriginGate(b, origin) {
			t.Fatalf("nonowner %q answered original origin", id)
		}
	}
	b.selfID = origin.BrokerID
	if transferOriginGate(b, xfercontract.Origin{}) {
		t.Fatal("missing origin was treated as local absence authority")
	}
	origin.BootID = "lost-boot"
	if !transferOriginGate(b, origin) {
		t.Fatal("the original broker must answer a lost boot with recovery evidence")
	}
	if !transferOriginGate(&Broker{}, origin) || transferOriginGate(nil, origin) || transferOriginGate(&Broker{}, xfercontract.Origin{}) {
		t.Fatal("cluster peer selection disabled the single controller or accepted a missing broker")
	}
}

// origin: hot-update S7 plan §6.1 / T39
func TestTransferContinuationKeepsStaleWithoutBypassingAuthority(t *testing.T) {
	db := openBrokerFixtureDB(t)
	seedHomedNode(t, db, "lab", "node", "server", "home")
	if _, err := db.Exec(`INSERT INTO members(sid,pubkey_fp,role,via) VALUES('lab','actor','owner','pin')`); err != nil {
		t.Fatal(err)
	}
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}}
	for _, state := range []node.State{node.StateOnline, node.StateStale, node.StateOffline} {
		if _, err := db.Exec(`UPDATE nodes SET status=? WHERE sid='lab' AND nid='node'`, state); err != nil {
			t.Fatal(err)
		}
		for _, accepted := range []bool{false, true} {
			got := transferNodeGate(b, "lab", "node", accepted)
			allowed := state == node.StateOnline || (accepted && state == node.StateStale)
			if (got == "") != allowed {
				t.Fatalf("state=%s accepted=%t gate=%q", state, accepted, got)
			}
		}
	}
	if code := transferNodeGate(b, "lab", "missing", true); code != "node_not_found" {
		t.Fatalf("missing node gate=%q", code)
	}
	// The liveness exception is composed after authority, not before it.
	// The existing gate with an empty NID checks session and member revocation.
	if _, err := db.Exec(`UPDATE nodes SET status='STALE'; DELETE FROM members WHERE pubkey_fp='actor'`); err != nil {
		t.Fatal(err)
	}
	if code := b.transferGate("lab", "actor", ""); code != "not_a_member" {
		t.Fatalf("STALE continuation bypassed revoked membership: %q", code)
	}
	if _, err := db.Exec(`UPDATE sessions SET state='DELETING' WHERE sid='lab'`); err != nil {
		t.Fatal(err)
	}
	if code := b.transferGate("lab", "actor", ""); code != "session_not_found_or_deleting" {
		t.Fatalf("STALE continuation restored ordinary authority after session removal: %q", code)
	}
}

// origin: hot-update S7 plan §8.2 / T39
func TestTransferAuditJoinUsesOriginalDeadline(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferAuditJoin(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer audit deadline join", beforeG)
	testharness.AssertNoFDLeak(t, "transfer audit deadline join", beforeFD, 0)
}

func exerciseTransferAuditJoin(t *testing.T) {
	t.Helper()
	b := &Broker{cfg: Config{Logger: silentLogger()}, cl: &clusterRuntime{}}
	var calls, acknowledgements atomic.Int32
	b.attachTransferAuditSinkWith(func([]byte) error { calls.Add(1); return nil })
	entered, release := make(chan struct{}), make(chan struct{})
	releaseCallback := sync.OnceFunc(func() { close(release) })
	// Hold the post-forward callback: joining only the network request or the
	// Raft ACK would let this backend borrower survive the image cut.
	b.transferAuditSink(schema.AuditTransfer{Kind: "complete", TransferID: "original"}, func() {
		close(entered)
		<-release
		acknowledgements.Add(1)
	})
	defer func() { releaseCallback(); b.WaitTransferAudit() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("audit callback did not enter")
	}
	start := time.Now()
	joined := make(chan error, 1)
	deadline := transferRouteDeadline(t, 30*time.Millisecond)
	go func() { joined <- JoinTransferAudit(b, deadline) }()
	select {
	case err := <-joined:
		if !controlbudget.IsExpired(err) {
			t.Fatalf("blocked completion was falsely joined: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		// Release and join even the unbounded-Wait mutant before failing, so
		// the regression receipt does not itself leak a waiter or borrower.
		releaseCallback()
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Fatal("audit join did not return after its borrower exited")
		}
		t.Fatal("audit join ignored the cut's original deadline")
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("join ignored original deadline: %v", elapsed)
	}
	b.transferAuditSink(schema.AuditTransfer{Kind: "failed"}, func() { acknowledgements.Add(1) })
	if calls.Load() != 1 || acknowledgements.Load() != 0 {
		t.Fatal("fenced callback entered or retired its ledger")
	}
	if err := b.transferAuditForwardSync(nil); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("synchronous ledger forward bypassed the join fence: %v", err)
	}
	// A refused cut can restore this binding. The old completion is still
	// joined at cleanup; reopening does not discard it or reset a WaitGroup.
	ResumeTransferAudit(b)
	if err := b.transferAuditForwardSync(nil); err != nil || calls.Load() != 2 {
		t.Fatalf("retained owner could not resume: calls=%d err=%v", calls.Load(), err)
	}
	oldBinding := b.cl
	b.cl = &clusterRuntime{}
	releaseCallback()
	b.WaitTransferAudit()
	if err := oldBinding.transferAuditUsers.Fence(transferRouteDeadline(t, 30*time.Millisecond)); err != nil {
		t.Fatalf("late completion decremented a replacement binding instead of its original owner: %v", err)
	}
	if err := JoinTransferAudit(b, transferRouteDeadline(t, 30*time.Millisecond)); err != nil {
		t.Fatalf("old completion corrupted the new callback admission: %v", err)
	}
	if err := JoinTransferAudit(b, 0); !errors.Is(err, controlbudget.ErrInvalidDeadline) {
		t.Fatalf("zero deadline authorized join: %v", err)
	}
}

// origin: hot-update S7 plan §6.3 / §8.2 / T39
func TestTransferClusterForwardUsesOneDeadline(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferForwardDeadline(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer forward deadline", beforeG)
	testharness.AssertNoFDLeak(t, "transfer forward deadline", beforeFD, 0)
}

func exerciseTransferForwardDeadline(t *testing.T) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	entered, release := make(chan struct{}), make(chan struct{})
	sub, err := nc.Subscribe(proto.SubjClusterApply(VerbTransferCensus), func(msg *nats.Msg) {
		close(entered)
		<-release
		_ = msg.Respond([]byte(`{"status":"ok"}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { close(release); _ = sub.Unsubscribe() })
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	fwd := NewForwarderWithLimit(nc, time.Second, 0)
	fwd.limiter = rate.NewLimiter(10, 1)
	if !fwd.limiter.Allow() {
		t.Fatal("limiter token premise failed")
	}
	start := time.Now()
	err = forwardTransferRPC(fwd, transferForwardRequest{}, transferRouteDeadline(t, 125*time.Millisecond))
	if err == nil || time.Since(start) > 250*time.Millisecond {
		t.Fatalf("limiter and network acquired separate budgets: elapsed=%v err=%v", time.Since(start), err)
	}
	select {
	case <-entered:
	default:
		t.Fatal("deadline test never sent a real NATS request after waiting for its token")
	}
	if err := forwardTransferRPC(fwd, transferForwardRequest{}, 0); !errors.Is(err, controlbudget.ErrInvalidDeadline) {
		t.Fatalf("zero deadline sent a request: %v", err)
	}
}

// origin: hot-update S7 plan §6.3 / §8.2 / T39
func TestTransferForwardDispatchRejectsUnprovenAndLateReplies(t *testing.T) {
	origin := xfercontract.Origin{BrokerID: "original", InstanceID: "instance", BootID: "boot", Generation: 1}
	cfg := TransferClusterForwardConfig{Origin: origin}
	cfg.Census = func(xfercontract.Origin, controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
		return xfercontract.LedgerCensus{Origin: origin, Ready: true, Complete: true, Digest: "complete",
			Rows: []xfercontract.LedgerRow{{Settlement: &xfercontract.SettlementCapsule{Signature: "must-stay-local"}}}}, nil
	}
	good := transferForwardRequest{Origin: origin, BudgetNS: int64(time.Second)}
	for _, control := range []struct {
		name string
		env  forwardEnvelope
		req  transferForwardRequest
		code string
	}{
		{"foreign-boot", forwardEnvelope{Verb: VerbTransferCensus}, transferForwardRequest{Origin: xfercontract.Origin{BrokerID: origin.BrokerID}, BudgetNS: good.BudgetNS}, xfercontract.CodeOwnerRecovering},
		{"zero-budget", forwardEnvelope{Verb: VerbTransferCensus}, transferForwardRequest{Origin: origin}, "bad_request"},
		{"renewed-budget", forwardEnvelope{Verb: VerbTransferCensus}, transferForwardRequest{Origin: origin, BudgetNS: int64(transferForwardBudget + 1)}, "bad_request"},
		{"forward-reqid", forwardEnvelope{Verb: VerbTransferCensus, ReqID: "raft-key"}, good, "bad_request"},
		{"unknown-verb", forwardEnvelope{Verb: "unknown"}, good, "bad_request"},
		{"census-with-patch", forwardEnvelope{Verb: VerbTransferCensus}, transferForwardRequest{Origin: origin, Patch: &xfercontract.ConfigPatch{}, BudgetNS: good.BudgetNS}, "bad_request"},
		{"probe-with-patch", forwardEnvelope{Verb: VerbTransferProbe}, transferForwardRequest{Origin: origin, Patch: &xfercontract.ConfigPatch{}, BudgetNS: good.BudgetNS}, "bad_request"},
		{"config-with-origin", forwardEnvelope{Verb: VerbTransferBucketConfig}, transferForwardRequest{Origin: origin, Patch: &xfercontract.ConfigPatch{}, BudgetNS: good.BudgetNS}, "bad_request"},
		{"config-without-patch", forwardEnvelope{Verb: VerbTransferBucketConfig}, transferForwardRequest{BudgetNS: good.BudgetNS}, "bad_request"},
	} {
		t.Run(control.name, func(t *testing.T) {
			if reply := dispatchTransferForward(cfg, control.env, control.req); reply.Status != fwdStatusError || reply.Code != control.code {
				t.Fatalf("unproven request was acknowledged: %+v", reply)
			}
		})
	}
	reply := dispatchTransferForward(cfg, forwardEnvelope{Verb: VerbTransferCensus}, good)
	if reply.Status != fwdStatusOK || reply.Census == nil {
		t.Fatalf("complete backend proof was not forwarded: %+v", reply)
	}
	raw, err := json.Marshal(reply)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "must-stay-local") || strings.Contains(string(raw), "Rows") || strings.Contains(string(raw), "Settlement") {
		t.Fatalf("census serialized ledger authentication: %s", raw)
	}
	entered := false
	cfg.Probe = func(deadline controlbudget.Deadline) (xfercontract.ProbeResult, error) {
		entered = true
		// This adversarial backend reports success after its real deadline.
		// A caller's timeout cannot become a receiver's capability proof.
		for {
			if _, err := controlbudget.Remaining(deadline); err != nil {
				break
			}
			runtime.Gosched()
		}
		return xfercontract.ProbeResult{InstanceID: origin.InstanceID}, nil
	}
	reply = dispatchTransferForward(cfg, forwardEnvelope{Verb: VerbTransferProbe}, transferForwardRequest{Origin: origin, BudgetNS: int64(time.Millisecond)})
	if !entered || reply.Status != fwdStatusError || reply.Probe != nil {
		t.Fatalf("expired backend reply became a capability seal: %+v", reply)
	}
}

// origin: hot-update S7 plan §8.2 / T39
func TestTransferClusterForwardJoinRetainsBlockedOwner(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferClusterJoin(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer cluster callback join", beforeG)
	testharness.AssertNoFDLeak(t, "transfer cluster callback join", beforeFD, 0)
}

func exerciseTransferClusterJoin(t *testing.T) {
	t.Helper()
	owner := &TransferClusterForward{}
	if !owner.users.Begin() {
		t.Fatal("initial callback admission failed")
	}
	defer owner.users.End()
	start := time.Now()
	if err := JoinTransferClusterForward(owner, transferRouteDeadline(t, 10*time.Millisecond)); !controlbudget.IsExpired(err) {
		t.Fatalf("blocked callback was falsely joined: %v", err)
	}
	if time.Since(start) > 250*time.Millisecond || owner.users.Begin() {
		t.Fatal("join was unbounded or reopened future callback admission")
	}
	ResumeTransferClusterForward(owner)
	if !owner.users.Begin() {
		t.Fatal("refused cut lost the original callback owner")
	}
	owner.users.End()
}

type transferRouteRefusal struct{ code, message string }

func (e transferRouteRefusal) Error() string        { return e.message }
func (e transferRouteRefusal) TransferCode() string { return e.code }

type transferRouteRefusingBucket struct {
	err    error
	called func()
}

func (b transferRouteRefusingBucket) Apply(xfercontract.ConfigPatch, controlbudget.Deadline) (xfercontract.BucketSeal, error) {
	if b.called != nil {
		b.called()
	}
	return xfercontract.BucketSeal{}, b.err
}
func (b transferRouteRefusingBucket) Freeze(controlbudget.Deadline) (xfercontract.ConfigWriterState, error) {
	return xfercontract.ConfigWriterState{}, b.err
}
func (b transferRouteRefusingBucket) Stop(controlbudget.Deadline) error { return b.err }

// origin: hot-update S7 plan lead LQ3 / §2.1 / T39
func TestTransferConfigForwardPreservesPublicHorizonRefusal(t *testing.T) {
	detail := "transfer_storage_horizon: required Duplicates >= 2228s and MaxAge = 0 or >= 2228s"
	refusal := transferRouteRefusal{code: xfercontract.CodeStorageHorizon, message: detail}
	cfg := TransferClusterForwardConfig{Buckets: transferRouteRefusingBucket{err: refusal}}
	req := transferForwardRequest{Patch: &xfercontract.ConfigPatch{Bucket: "xfer-lab"}, BudgetNS: int64(time.Second)}
	reply := dispatchTransferForward(cfg, forwardEnvelope{Verb: VerbTransferBucketConfig}, req)
	if reply.Code != xfercontract.CodeStorageHorizon || reply.Error != detail || reply.Seal != nil {
		t.Fatalf("cross-broker horizon refusal lost its code or required limits: %+v", reply)
	}
	cfg.Buckets = transferRouteRefusingBucket{err: errors.New("private store diagnostic")}
	reply = dispatchTransferForward(cfg, forwardEnvelope{Verb: VerbTransferBucketConfig}, req)
	if reply.Code != xfercontract.CodeResultUnknown || reply.Error != "" {
		t.Fatalf("unclassified backend diagnostic crossed the public refusal boundary: %+v", reply)
	}
	cfg.Buckets = transferRouteRefusingBucket{err: transferRouteRefusal{message: "private store diagnostic"}}
	reply = dispatchTransferForward(cfg, forwardEnvelope{Verb: VerbTransferBucketConfig}, req)
	if reply.Code != xfercontract.CodeResultUnknown || reply.Error != "" {
		t.Fatalf("empty refusal code exposed a backend diagnostic: %+v", reply)
	}
}

// origin: hot-update S7 plan §6.3 / T39
func TestTransferClusterForwardRejectsMalformedProofs(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferMalformedProofs(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer malformed forwarding proof", beforeG)
	testharness.AssertNoFDLeak(t, "transfer malformed forwarding proof", beforeFD, 0)
}

func exerciseTransferMalformedProofs(t *testing.T) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	var raw atomic.Value
	raw.Store([]byte(`{"status":"ok"}`))
	sub, err := nc.Subscribe(proto.SubjClusterApplyWildcard, func(msg *nats.Msg) { _ = msg.Respond(raw.Load().([]byte)) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	fwd := NewForwarderWithLimit(nc, time.Second, 0)
	origin := xfercontract.Origin{BrokerID: "original", InstanceID: "instance", BootID: "boot", Generation: 1}
	wrong := origin
	wrong.BrokerID = "foreign"
	for _, reply := range []transferForwardReply{
		{Status: "unknown"},
		{Status: fwdStatusOK},
		{Status: fwdStatusOK, Code: xfercontract.CodeResultUnknown, Seal: &xfercontract.BucketSeal{Bucket: "xfer-lab", ConfigDigest: "digest"}},
		{Status: fwdStatusOK, Error: "unknown", Seal: &xfercontract.BucketSeal{Bucket: "xfer-lab", ConfigDigest: "digest"}},
		{Status: fwdStatusOK, Census: &transferCensusProof{Origin: wrong, Ready: true, Complete: true, Digest: "digest"}},
		{Status: fwdStatusOK, Census: &transferCensusProof{Origin: origin, Ready: false, Complete: true, Digest: "digest"}},
		{Status: fwdStatusOK, Census: &transferCensusProof{Origin: origin, Ready: true, Complete: false, Digest: "digest"}},
		{Status: fwdStatusOK, Census: &transferCensusProof{Origin: origin, Ready: true, Complete: true}},
		{Status: fwdStatusOK, Census: &transferCensusProof{Origin: origin, Ready: true, Complete: true, Digest: "digest", Code: xfercontract.CodeResultUnknown}},
		{Status: fwdStatusOK, Seal: &xfercontract.BucketSeal{Bucket: "foreign", ConfigDigest: "digest"}},
		{Status: fwdStatusOK, Seal: &xfercontract.BucketSeal{Bucket: "xfer-lab"}},
		{Status: fwdStatusOK, Probe: &xfercontract.ProbeResult{InstanceID: "foreign"}},
		{Status: fwdStatusOK, Probe: &xfercontract.ProbeResult{InstanceID: origin.InstanceID, Code: xfercontract.CodeResultUnknown}},
		{Status: fwdStatusOK, Probe: &xfercontract.ProbeResult{InstanceID: origin.InstanceID, Error: "unavailable"}},
		{Status: fwdStatusOK, Seal: &xfercontract.BucketSeal{Bucket: "xfer-lab", ConfigDigest: "digest"}, Census: &transferCensusProof{Origin: origin, Ready: true, Complete: true, Digest: "digest"}},
		{Status: fwdStatusOK, Seal: &xfercontract.BucketSeal{Bucket: "xfer-lab", ConfigDigest: "digest"}, Probe: &xfercontract.ProbeResult{InstanceID: origin.InstanceID}},
		{Status: fwdStatusOK, Census: &transferCensusProof{Origin: origin, Ready: true, Complete: true, Digest: "digest"}, Probe: &xfercontract.ProbeResult{InstanceID: origin.InstanceID}},
	} {
		encoded, err := json.Marshal(reply)
		if err != nil {
			t.Fatal(err)
		}
		raw.Store(encoded)
		if _, err := (TransferOwnerResolver{Forwarder: fwd}).Census(origin, transferRouteDeadline(t, time.Second)); err == nil {
			t.Fatalf("malformed or foreign census was accepted: %s", encoded)
		}
		if _, err := ForwardTransferBucketConfig(fwd, xfercontract.ConfigPatch{Bucket: "xfer-lab"}, transferRouteDeadline(t, time.Second)); err == nil {
			t.Fatalf("malformed or foreign config seal was accepted: %s", encoded)
		}
		if _, err := ProbeTransferBroker(fwd, origin, transferRouteDeadline(t, time.Second)); err == nil {
			t.Fatalf("malformed or foreign probe was accepted: %s", encoded)
		}
	}
	encoded, err := json.Marshal(transferForwardReply{Status: fwdStatusOK, Probe: &xfercontract.ProbeResult{}})
	if err != nil {
		t.Fatal(err)
	}
	raw.Store(encoded)
	if _, err := ProbeTransferBroker(fwd, xfercontract.Origin{BrokerID: origin.BrokerID}, transferRouteDeadline(t, time.Second)); err == nil {
		t.Fatal("directed discovery accepted a missing broker instance")
	}
	raw.Store([]byte(`{`))
	if err := forwardTransferRPC(fwd, transferForwardRequest{Origin: origin}, transferRouteDeadline(t, time.Second)); err == nil {
		t.Fatal("malformed JSON became a successful owner response")
	}
}

// origin: hot-update S7 plan §2.1 / §6.3 / T39
func TestTransferConfigForwardNeverAppliesOnFollower(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferFollowerConfig(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer follower config refusal", beforeG)
	testharness.AssertNoFDLeak(t, "transfer follower config refusal", beforeFD, 0)
}

func exerciseTransferFollowerConfig(t *testing.T) {
	t.Helper()
	c := newTransferRouteCluster(t)
	probes := make([]clusterharness.LeaderProbe, len(c.nodes))
	for i, n := range c.nodes {
		probes[i] = n
	}
	_, err := clusterharness.WithLeader(t, probes, 5*time.Second, func(leader int) error {
		follower, ingress := (leader+1)%3, (leader+2)%3
		var entered atomic.Int32
		owner, err := SubscribeTransferClusterForward(c.conns[follower], TransferClusterForwardConfig{
			Node: c.nodes[follower], Origin: c.origins[follower],
			Buckets: transferRouteRefusingBucket{err: errors.New("must never enter"), called: func() { entered.Add(1) }},
		})
		if err != nil {
			return err
		}
		defer func() {
			if err := StopTransferClusterForward(owner, transferRouteDeadline(t, time.Second)); err != nil {
				t.Error(err)
			}
		}()
		if err := c.conns[follower].FlushTimeout(time.Second); err != nil {
			return err
		}
		fwd := NewForwarderWithLimit(c.conns[ingress], time.Second, 0)
		_, err = ForwardTransferBucketConfig(fwd, xfercontract.ConfigPatch{Bucket: "xfer-lab"}, transferRouteDeadline(t, 40*time.Millisecond))
		if !errors.Is(err, context.DeadlineExceeded) || entered.Load() != 0 {
			return fmt.Errorf("follower replied or entered the config writer: err=%v entered=%d", err, entered.Load())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §6.3 / T39
func TestTransferN3RoutingUsesOriginalFollowerOwner(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferBusinessRouting(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer cold/hot original routing", beforeG)
	testharness.AssertNoFDLeak(t, "transfer cold/hot original routing", beforeFD, 0)
}

func exerciseTransferBusinessRouting(t *testing.T) {
	t.Helper()
	exerciseTransferProductionRouting(t)
}
