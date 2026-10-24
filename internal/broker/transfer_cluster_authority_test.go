package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferserve"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// origin: S7 internal review s7-p9m-open F1
func TestTransferClusterPermitKeepsOriginalWindowAcrossLeaderAndRemoval(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseOriginalClusterPermit(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "retained cluster permits", g)
	testharness.AssertNoFDLeak(t, "retained cluster permits", fd, 0)
}

func exerciseOriginalClusterPermit(t *testing.T) {
	c := newContinuationCluster(t)
	leader := c.leader(t)
	original, next := (leader+1)%3, (leader+2)%3
	permit, _ := c.permit(t, original, "issued-only")
	live, intent := c.permit(t, original, "live-transfer")
	graph := c.brokers[original].transferRuntime.Load()
	before, err := graph.Tracker.Admit(live, intent, transferRouteDeadline(t, time.Second))
	requireWiring(t, err)
	aggregate, err := c.brokers[next].transferRuntime.Load().Assembly.Owners.Census(x.Origin{}, transferRouteDeadline(t, 5*time.Second))
	requireWiring(t, err)
	found := false
	for _, row := range aggregate.Rows {
		found = found || row.Identity == live.Identity && row.Origin == live.Origin
	}
	if !aggregate.Ready || !aggregate.Complete || !found {
		t.Fatal("zero-origin collection census lost the original follower row")
	}

	// No tracker was admitted: the signed permit itself must retain origin.
	requireWiring(t, c.nodes[leader].LeadershipTransferToServer(c.nodes[next].SelfID(), fmt.Sprintf("127.0.0.1:%d", 7400+next)))
	if got := c.leader(t); got != next {
		t.Fatalf("leader=%d, want %d", got, next)
	}
	replayed, _ := c.permit(t, next, "issued-only")
	if replayed != permit {
		t.Fatal("new home replaced original issuer, nonce or creation window")
	}
	partial := x.Request{Op: x.OpStatus, CallID: "read-only", Identity: x.Identity{SID: "lab", NID: "node", TransferID: "live-transfer"}}
	resolved, ok := resolveTransferStatusOrigin(c.brokers[next], c.brokers[next].transferRuntime.Load(), partial, transferRouteDeadline(t, time.Second))
	if !ok || resolved.Identity != live.Identity || resolved.Origin != live.Origin {
		t.Fatal("origin-less status lost the live original owner")
	}
	after, err := graph.Tracker.Status(x.StatusRequest{CallID: "same-entry", Identity: resolved.Identity, Origin: resolved.Origin}, transferRouteDeadline(t, time.Second))
	requireWiring(t, err)
	if after.State != x.StatusLive || after.Budget.StartedAt != before.Budget.StartedAt {
		t.Fatal("leader change reset the live original entry")
	}
	requireWiring(t, c.nodes[next].RemoveServer(c.nodes[original].SelfID()))
	remaining := *c.transferRouteCluster
	remaining.nodes = nil
	for i, n := range c.nodes {
		if i != original {
			remaining.nodes = append(remaining.nodes, n)
		}
	}
	routePropose(t, &remaining, []string{"DELETE FROM cluster_nodes WHERE node_id='" + c.nodes[original].SelfID() + "'"})
	replayed, _ = c.permit(t, next, "issued-only")
	if replayed != permit {
		t.Fatal("removed origin lost its issued permit")
	}
	// Closing the original ingress must make absence/status/barrier unknown,
	// while the immutable permit remains discoverable from durable authority.
	requireWiring(t, stopBrokerTransfers(c.brokers[original]))
	c.conns[original].Close()
	replayed, _ = c.permit(t, next, "issued-only")
	if replayed != permit {
		t.Fatal("unreachable origin minted a replacement permit")
	}
	refused, _ := c.callPermit(t, next, "new-after-loss")
	if refused.Permit != nil || refused.Code == "" {
		t.Fatal("removed unreachable origin was treated as complete creation census")
	}
	if _, err := c.brokers[next].transferRuntime.Load().Assembly.Owners.SessionCensus("lab", transferRouteDeadline(t, 200*time.Millisecond)); err == nil {
		t.Fatal("unreachable removed origin disappeared from session settlement")
	}
}

// A retained session-owner adapter must follow the installed graph after a
// tracker cut. Foreign rows never borrow the handler's local mutation ports.
// origin: S7 internal review s7-p9term-request F2 / hot-update S7 plan §6.4 / §8.2
func TestTransferClusterSettlementResolvesCurrentOriginalOwner(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseCurrentSettlementOwner(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "current original settlement owner", g)
	testharness.AssertNoFDLeak(t, "current original settlement owner", fd, 0)
}

func exerciseCurrentSettlementOwner(t *testing.T) {
	c := newContinuationCluster(t)
	leader := c.leader(t)
	original, next := (leader+1)%3, (leader+2)%3
	b := c.brokers[original]
	graph := b.transferRuntime.Load()
	owners, ok := any(graph.Assembly.Owners).(xferauth.OriginalSettlementOwners)
	if !ok {
		t.Fatal("production session owners do not resolve original settlement mutation ports")
	}
	assertCurrent := func(current *BrokerTransfers) {
		t.Helper()
		tracker, finalizer, err := owners.SettlementOwner(graph.Origin, transferRouteDeadline(t, time.Second))
		requireWiring(t, err)
		if tracker != current.Tracker || finalizer != current.Finalizer {
			t.Fatal("retained session-owner adapter returned an obsolete graph")
		}
		if _, ok := tracker.(xferauth.TrackerAbandoner); !ok {
			t.Fatal("original tracker lost its serialized abandonment port")
		}
	}
	assertCurrent(graph)
	permit, intent := c.permit(t, original, "owner-cut")
	_, err := graph.Tracker.Admit(permit, intent, transferRouteDeadline(t, time.Second))
	requireWiring(t, err)
	requireWiring(t, c.nodes[leader].LeadershipTransferToServer(c.nodes[next].SelfID(), fmt.Sprintf("127.0.0.1:%d", 7400+next)))
	if c.leader(t) != next {
		t.Fatal("leadership did not move")
	}
	assertCurrent(graph)
	foreign := c.brokers[next].transferRuntime.Load().Assembly.Owners
	remote, ok := any(foreign).(xferauth.OriginalSettlementOwners)
	if !ok {
		t.Fatal("new leader does not resolve original settlement mutation ports")
	}
	tracker, finalizer, err := remote.SettlementOwner(graph.Origin, transferRouteDeadline(t, time.Second))
	if err == nil || tracker != nil || finalizer != nil {
		t.Fatal("foreign original borrowed the new leader's local settlement ports")
	}

	// The merged TERM consumer must use those ports too, rather than falling
	// back to the foreign handler's local tracker for its cancellation.
	// origin: hot-update S7 plan §6.4 / FIX-TERM request 2
	barrierErr := c.brokers[next].transferRuntime.Load().Settlement.Settle("lab", false, transferRouteDeadline(t, 5*time.Second))
	var pending *xferauth.PendingError
	routed := false
	if errors.As(barrierErr, &pending) {
		for _, p := range pending.Transfers {
			routed = routed || p.TransferID == permit.Identity.TransferID && p.Reason == "original_owner_unavailable"
		}
	}
	if !routed {
		t.Fatalf("production barrier did not refuse foreign mutation authority: %v", barrierErr)
	}
	frozen, err := graph.Tracker.Freeze(transferRouteDeadline(t, time.Second))
	requireWiring(t, err)
	state, resources, err := frozen.Export()
	requireWiring(t, err)
	if len(resources) != 0 {
		t.Fatal("tracker unexpectedly exported business descriptors")
	}
	requireWiring(t, suspendBrokerTransfers(b, transferRouteDeadline(t, 5*time.Second)))
	tracker, finalizer, err = owners.SettlementOwner(graph.Origin, transferRouteDeadline(t, time.Second))
	if err == nil || tracker != nil || finalizer != nil {
		t.Fatal("uninstalled original graph still returned mutation authority")
	}
	js, err := jetstream.New(c.conns[original])
	requireWiring(t, err)
	current, err := serveTransferFactoryState(b, js, transferRouteDeadline(t, 5*time.Second), &state)
	requireWiring(t, err)
	requireWiring(t, InstallBrokerTransfers(b, current, transferRouteDeadline(t, 5*time.Second)))
	requireWiring(t, startBrokerTransferForward(b, c.conns[original], current))
	if current.Tracker == graph.Tracker || current.Finalizer == graph.Finalizer {
		t.Fatal("rebuild reused retired mutation owners")
	}
	assertCurrent(current)
	tracker, _, err = owners.SettlementOwner(graph.Origin, transferRouteDeadline(t, time.Second))
	requireWiring(t, err)
	status, err := tracker.Status(x.StatusRequest{Identity: permit.Identity, Origin: permit.Origin}, transferRouteDeadline(t, time.Second))
	requireWiring(t, err)
	if status.State != x.StatusLive || status.Identity != permit.Identity {
		t.Fatal("current original tracker lost the retained transfer")
	}
}

// origin: S7 internal review s7-p9m-open F5
func TestTransferClusterConfigHandoffRetiresOriginalWriter(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseClusterConfigHandoff(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "cluster configuration handoff", g)
	testharness.AssertNoFDLeak(t, "cluster configuration handoff", fd, 0)
}

func exerciseClusterConfigHandoff(t *testing.T) {
	c := newContinuationCluster(t)
	leader := c.leader(t)
	old := c.brokers[leader].transferRuntime.Load().Buckets
	d := transferRouteDeadline(t, 5*time.Second)
	first, err := old.Apply(x.ConfigPatch{Bucket: "xfer-handoff"}, d)
	requireWiring(t, err)
	next := (leader + 1) % 3
	requireWiring(t, c.nodes[leader].LeadershipTransferToServer(c.nodes[next].SelfID(), fmt.Sprintf("127.0.0.1:%d", 7400+next)))
	if c.leader(t) != next {
		t.Fatal("leadership did not transfer")
	}
	seal, err := c.brokers[next].transferRuntime.Load().Buckets.Apply(x.ConfigPatch{Bucket: "xfer-handoff", Duplicates: first.Duplicates + time.Minute}, transferRouteDeadline(t, 5*time.Second))
	requireWiring(t, err)
	if seal.ConfigDigest == first.ConfigDigest {
		t.Fatal("successor did not actually change config")
	}
	if _, err := old.Apply(x.ConfigPatch{Bucket: "xfer-handoff"}, transferRouteDeadline(t, time.Second)); err == nil {
		t.Fatal("original writer remained active after cross-leader handoff")
	}
}

type blockedConfigJS struct {
	jetstream.JetStream
	enabled          atomic.Bool
	entered, release chan struct{}
	unknown          bool
	late             jetstream.StreamConfig
}

func (j *blockedConfigJS) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if j.enabled.CompareAndSwap(true, false) {
		j.late = cfg
		close(j.entered)
		<-j.release
		if j.unknown {
			return nil, context.DeadlineExceeded
		}
	}
	return j.JetStream.UpdateStream(ctx, cfg)
}

// origin: S7 internal review s7-p9m-open F5 / rv-jsnats F1
func TestTransferClusterConfigHandoffWaitsForInflightUpdate(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseInflightConfigHandoff(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "inflight cluster configuration handoff", g)
	testharness.AssertNoFDLeak(t, "inflight cluster configuration handoff", fd, 0)
}

func exerciseInflightConfigHandoff(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint(unknown), func(t *testing.T) {
			var wrappers [3]*blockedConfigJS
			c := newContinuationCluster(t, func(i int, js jetstream.JetStream) jetstream.JetStream {
				wrappers[i] = &blockedConfigJS{JetStream: js, entered: make(chan struct{}), release: make(chan struct{}), unknown: unknown}
				return wrappers[i]
			})
			leader := c.leader(t)
			old := c.brokers[leader].transferRuntime.Load().Buckets
			seal, err := old.Apply(x.ConfigPatch{Bucket: "xfer-inflight"}, transferRouteDeadline(t, 5*time.Second))
			requireWiring(t, err)
			j := wrappers[leader]
			j.enabled.Store(true)
			patch := x.ConfigPatch{Bucket: "xfer-inflight", Duplicates: seal.Duplicates + time.Minute}
			result := make(chan error, 1)
			go func() { _, err := old.Apply(patch, transferRouteDeadline(t, 10*time.Second)); result <- err }()
			select {
			case <-j.entered:
			case <-time.After(time.Second):
				t.Fatal("UPDATE did not enter")
			}
			defer func() {
				select {
				case <-j.release:
				default:
					close(j.release)
				}
				<-result
			}()
			next := (leader + 1) % 3
			requireWiring(t, c.nodes[leader].LeadershipTransferToServer(c.nodes[next].SelfID(), fmt.Sprintf("127.0.0.1:%d", 7400+next)))
			if c.leader(t) != next {
				t.Fatal("leadership did not transfer")
			}
			patch.Duplicates += time.Minute
			if _, err := c.brokers[next].transferRuntime.Load().Buckets.Apply(patch, transferRouteDeadline(t, 200*time.Millisecond)); err == nil {
				t.Fatal("successor wrote while original UPDATE was inflight")
			}
			close(j.release)
			err = <-result
			result <- err // cleanup joins the completed exercise on all exits.
			if unknown && err == nil {
				t.Fatal("injected UPDATE did not stay sent-unknown")
			}
			if !unknown {
				requireWiring(t, err)
			}
			_, err = c.brokers[next].transferRuntime.Load().Buckets.Apply(patch, transferRouteDeadline(t, 5*time.Second))
			if unknown {
				c.pendingConfig.Store(true)
				if err == nil {
					t.Fatal("sent-unknown UPDATE authorized cross-leader handoff")
				}
				// A real late request reaches JetStream only after the refusal.
				_, err = c.js.UpdateStream(t.Context(), j.late)
				requireWiring(t, err)
			} else {
				requireWiring(t, err)
			}
		})
	}
}

// origin: S7 internal review s7-p9m-open F2
func TestTransferClusterRevokedReceiverUsesRestrictedReconnect(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseClusterSettlementReconnect(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "cluster settlement reconnect", g)
	testharness.AssertNoFDLeak(t, "cluster settlement reconnect", fd, 0)
}

func exerciseClusterSettlementReconnect(t *testing.T) {
	c := newContinuationCluster(t)
	original := (c.leader(t) + 1) % 3
	graph := c.brokers[original].transferRuntime.Load()
	identity, _, err := xferserve.LoadIdentity(filepath.Join(filepath.Dir(c.brokers[original].cfg.DBPath), "xfer-continuation"), graph.Origin.BrokerID)
	requireWiring(t, err)
	defer identity.Issuer.Wipe()
	permits, err := xferauth.NewPermitAuthority(filepath.Join(t.TempDir(), "permits"), graph.Origin, identity.Issuer, time.Now)
	requireWiring(t, err)
	fp, _ := auth.FingerprintFromActor(c.actor)
	empty := sha256.Sum256(nil)
	intent := x.Intent{Verb: "push", Tier: "a", Path: "destination", SHA256: hex.EncodeToString(empty[:])}
	permit, err := permits.IssueFor(x.Identity{SID: "lab", NID: "node", ActorFP: fp, AgentInstance: "agent", TransferID: "settlement"}, intent, transferRouteDeadline(t, time.Second))
	requireWiring(t, err)
	_, err = graph.Tracker.Admit(permit, intent, transferRouteDeadline(t, time.Second))
	requireWiring(t, err)
	_, _, err = graph.Link.Request(x.Request{Op: x.OpPrepare, CallID: "prepare", Identity: permit.Identity, Origin: permit.Origin, Intent: &intent, Permit: &permit}, transferRouteDeadline(t, 5*time.Second))
	requireWiring(t, err)
	var capsule x.SettlementCapsule
	select {
	case capsule = <-c.capsules:
	case <-time.After(time.Second):
		t.Fatal("prepare issued no capsule")
	}
	_, _, err = graph.Link.Request(x.Request{Op: x.OpPrepare, CallID: "prepare-replay", Identity: permit.Identity, Origin: permit.Origin, Intent: &intent, Permit: &permit}, transferRouteDeadline(t, 5*time.Second))
	requireWiring(t, err)
	select {
	case repeated := <-c.capsules:
		if repeated != capsule {
			t.Fatal("prepare replay replaced original settlement capsule")
		}
	case <-time.After(time.Second):
		t.Fatal("prepare replay issued no retained capsule")
	}
	ingress := (original + 1) % 3
	censuses, err := c.brokers[ingress].transferRuntime.Load().Assembly.Owners.SessionCensus("lab", transferRouteDeadline(t, 5*time.Second))
	requireWiring(t, err)
	found := false
	for _, census := range censuses {
		for _, row := range census.Rows {
			if row.Identity == permit.Identity {
				found = true
				if census.Origin != permit.Origin || row.CanonicalStart == nil || row.Settlement != nil {
					t.Fatal("session census omitted exact original evidence or exposed a capsule")
				}
			}
		}
	}
	if !found {
		t.Fatal("session owners omitted a foreign original row")
	}
	routePropose(t, c.transferRouteCluster, []string{"DELETE FROM members WHERE sid='lab'", "UPDATE sessions SET state='DELETING' WHERE sid='lab'"})
	witness, err := c.conns[ingress].SubscribeSync("$SYS._INBOX.>")
	requireWiring(t, err)
	defer func() { _ = witness.Unsubscribe() }()
	requireWiring(t, c.conns[ingress].FlushTimeout(time.Second))
	// Actual CONNECT consumes the production callout on a different ingress.
	// The ordinary role is revoked; the original receiver settles one entry
	// with its same NKey and persisted capsule, and receives a real status reply.
	url := c.conns[ingress].ConnectedUrl()
	options := []nats.Option{nats.NoReconnect(), nats.Nkey(c.agent, c.agentKey.Sign), nats.CustomInboxPrefix(auth.InboxPrefixFor(c.agent))}
	if ordinary, err := nats.Connect(url, append(options, nats.Name("tether-agent:lab:node"))...); err == nil {
		ordinary.Close()
		t.Fatal("revoked receiver established an ordinary CONNECT")
	}
	capBytes, _ := json.Marshal(capsule)
	restricted, err := nats.Connect(url, append(options, nats.Name("transfer-settlement:lab:node"), nats.Token(string(capBytes)))...)
	requireWiring(t, err)
	defer restricted.Close()
	statusSubject, err := proto.SubjCtrlTransferRequest(c.agent, "lab", "node", "settlement", x.OpStatus)
	requireWiring(t, err)
	statusBody, _ := json.Marshal(proto.TransferStatusReq{CallID: "settlement-status", Identity: proto.Identity(permit.Identity), Origin: proto.Origin(permit.Origin)})
	statusMsg, err := restricted.Request(statusSubject, statusBody, 5*time.Second)
	requireWiring(t, err)
	var status proto.TransferStatusResp
	requireWiring(t, proto.DecodeTransferWire(statusMsg.Data, &status))
	if status.State != proto.StatusState(x.StatusLive) || status.Identity != proto.Identity(permit.Identity) || status.Code != "" {
		t.Fatalf("restricted status lost original entry: %+v", status)
	}
	// Observe the exact JWT that the real server consumed, keeping the full
	// permission equality assertion from the original counterexample.
	var granted *jwt.UserClaims
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		m, err := witness.NextMsg(time.Until(until))
		if err != nil {
			t.Fatal("real CONNECT JWT was not observed:", err)
		}
		response, err := jwt.DecodeAuthorizationResponseClaims(string(m.Data))
		if err != nil || response.Error != "" {
			continue
		}
		u, err := jwt.DecodeUserClaims(response.Jwt)
		if err == nil {
			granted = u
			break
		}
	}
	if granted == nil {
		t.Fatal("real CONNECT granted no user JWT")
	}
	want, _ := json.Marshal(auth.PermissionsForTransferSettlement(c.agent, "lab", "node", "settlement"))
	got, _ := json.Marshal(granted.Permissions)
	if string(got) != string(want) {
		t.Fatal("restricted reconnect restored business permissions")
	}
	violations := make(chan error, 8)
	restricted.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { violations <- err })
	for _, target := range []string{
		proto.SubjCmdForwarded("lab", "node", proto.TransferLinkVerb),
		"$O.xfer-lab.C.forbidden",
		proto.SubjClusterApply("transfer-bucket-config"),
	} {
		requireWiring(t, restricted.Publish(target, []byte("forbidden")))
		requireWiring(t, restricted.FlushTimeout(time.Second))
		select {
		case err := <-violations:
			if err == nil || !strings.Contains(err.Error(), "Permissions Violation") {
				t.Fatalf("unexpected ACL response: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("restricted receiver regained a business or cluster publish grant")
		}
	}
	// A valid signature from a connecting party's issuer is not issuer trust.
	// Refusal must happen before a cross-origin lookup, not only at its far end.
	rogue, err := nkeys.CreateAccount()
	requireWiring(t, err)
	defer rogue.Wipe()
	foreign, err := xferauth.SignCapsule(rogue, capsule)
	requireWiring(t, err)
	var untrustedLookups atomic.Int32
	spy, err := c.conns[ingress].Subscribe(proto.SubjectPrefix+".cluster.transfer-continuation", func(m *nats.Msg) {
		var req struct {
			Operation string               `json:"operation"`
			Capsule   *x.SettlementCapsule `json:"capsule"`
		}
		if json.Unmarshal(m.Data, &req) == nil && req.Operation == "settle" && req.Capsule != nil && req.Capsule.Issuer == foreign.Issuer {
			untrustedLookups.Add(1)
		}
	})
	requireWiring(t, err)
	defer func() { _ = spy.Unsubscribe() }()
	requireWiring(t, c.conns[ingress].FlushTimeout(time.Second))
	nonce := []byte("untrusted-issuer-proof")
	sig, _ := c.agentKey.Sign(nonce)
	_, err = c.brokers[ingress].transferRuntime.Load().Assembly.Reconnect.Verify(foreign, x.NKeyProof{NKey: c.agent, Nonce: nonce, Signature: sig}, transferRouteDeadline(t, time.Second))
	if err == nil {
		t.Fatal("untrusted issuer granted settlement")
	}
	requireWiring(t, c.conns[ingress].FlushTimeout(time.Second))
	if untrustedLookups.Load() != 0 {
		t.Fatal("untrusted issuer reached original-origin settlement lookup")
	}
	bad := capsule
	bad.Nonce += "forged"
	badBytes, _ := json.Marshal(bad)
	if forged, err := nats.Connect(url, append(options, nats.Name("transfer-settlement:lab:node"), nats.Token(string(badBytes)))...); err == nil {
		forged.Close()
		t.Fatal("forged capsule authenticated")
	}
	requireWiring(t, stopBrokerTransfers(c.brokers[original]))
	c.conns[original].Close()
	if lost, err := nats.Connect(url, append(options, nats.Name("transfer-settlement:lab:node"), nats.Token(string(capBytes)))...); err == nil {
		lost.Close()
		t.Fatal("unreachable origin authenticated receiver")
	}
}
