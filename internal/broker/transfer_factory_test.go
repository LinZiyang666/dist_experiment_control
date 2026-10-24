package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/LinZiyang666/tether/test/clusterharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// The unavailable receiver is an adversarial environment, not a successful
// replacement worker. Routing must return the explicit refusal and retain the
// real ledger entry; this fixture cannot certify receiver effects or cleanup.
type factoryUnavailablePorts struct{}

func (factoryUnavailablePorts) Resolve(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
	return x.ReceiverOutcome{}, errors.New("receiver unavailable")
}
func (factoryUnavailablePorts) Retire(x.Identity, x.RetirementProof, x.Deadline) (x.CleanupReceipt, error) {
	return x.CleanupReceipt{}, errors.New("receiver retirement unavailable")
}
func (factoryUnavailablePorts) Request(x.Request, x.Deadline) (x.Response, x.Attempt, error) {
	return x.Response{}, x.Attempt{}, xferauth.ControlRefusal(x.CodePayloadUnsupported)
}
func (factoryUnavailablePorts) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return nil, errors.New("no data attachment")
}
func (factoryUnavailablePorts) CloseData(x.Identity, x.Deadline) error {
	return errors.New("no data attachment")
}

type factoryCapacity struct {
	mu   sync.Mutex
	next int
	held map[x.ReservationID]bool
}

func (c *factoryCapacity) Reserve(x.Reservation) (x.ReservationID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.held == nil {
		c.held = map[x.ReservationID]bool{}
	}
	c.next++
	id := x.ReservationID(fmt.Sprint(c.next))
	c.held[id] = true
	return id, nil
}
func (c *factoryCapacity) Release(id x.ReservationID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.held[id] {
		return errors.New("unknown reservation")
	}
	delete(c.held, id)
	return nil
}

type factoryOwners struct{ b *Broker }

func (o factoryOwners) Census(origin x.Origin, d x.Deadline) (x.LedgerCensus, error) {
	current := o.b.transferRuntime.Load()
	if current == nil || origin != (x.Origin{}) && origin != current.Origin {
		return x.LedgerCensus{}, errors.New("owner unavailable")
	}
	census, err := current.Ledger.Scan(d)
	census.Origin = current.Origin
	return census, err
}
func (o factoryOwners) SessionCensus(_ string, d x.Deadline) ([]x.LedgerCensus, error) {
	c, err := o.Census(x.Origin{}, d)
	return []x.LedgerCensus{c}, err
}

func exerciseTransferProductionRouting(t *testing.T) {
	t.Helper()
	c := newTransferRouteCluster(t)
	// A separate real JS backend keeps routing/authority failure independent of
	// JS placement. All three controllers still share physical exact history.
	nc, err := nats.Connect(testharness.StartJSNATS(t))
	requireWiring(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	requireWiring(t, err)
	actor := freshUserActor(t)
	fp, err := auth.FingerprintFromActor(actor)
	requireWiring(t, err)
	probes := make([]clusterharness.LeaderProbe, len(c.nodes))
	for i := range probes {
		probes[i] = c.nodes[i]
	}
	leader, err := clusterharness.WithLeader(t, probes, 5*time.Second, func(int) error { return nil })
	requireWiring(t, err)
	original := (leader + 1) % 3
	ingress, _, err := natsinbox.Connect(c.conns[(original+1)%3].ConnectedUrl(), actor, nil)
	requireWiring(t, err)
	defer ingress.Close()
	// Real Raft commands seed authority on every follower. Rehoming later changes
	// this replicated location while the retained transfer stays at its origin.
	sqls := []string{
		"INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES('lab','lab','" + fp + "','pin')",
		"INSERT INTO members(sid,pubkey_fp,role,via) VALUES('lab','" + fp + "','owner','pin')",
	}
	for i := range c.brokers {
		sqls = append(sqls, fmt.Sprintf("INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES('broker-%d','broker-%d','pub-%d','server-%d','127.0.0.1:7400','127.0.0.1:6222','127.0.0.1:7000','host','sha','VOTER','2026-10-12T00:00:00Z')", i, i, i, i))
	}
	for _, sid := range []string{"lab", "lab-hot"} {
		if sid != "lab" {
			sqls = append(sqls, "INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES('lab-hot','lab-hot','"+fp+"','pin')", "INSERT INTO members(sid,pubkey_fp,role,via) VALUES('lab-hot','"+fp+"','owner','pin')")
		}
		sqls = append(sqls, fmt.Sprintf("INSERT INTO nodes(sid,nid,nats_server,status) VALUES('%s','node','server-%d','ONLINE')", sid, original))
	}

	routePropose(t, c, sqls)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // ctx-root: bounded embedded history fixture.
	defer cancel()
	requireWiring(t, jsstream.EnsureHistoryStream(ctx, js, "lab", 1))
	requireWiring(t, jsstream.EnsureHistoryStream(ctx, js, "lab-hot", 1))
	permits := make([]*xferauth.PermitAuthority, 3)
	for i, b := range c.brokers {
		root := t.TempDir()
		b.cfg.DBPath = filepath.Join(root, "state.db")
		setBrokerJS(b, js)
		issuer, err := nkeys.CreateUser()
		requireWiring(t, err)
		defer issuer.Wipe()
		permits[i], err = xferauth.NewPermitAuthority(filepath.Join(root, "permits"), c.origins[i], issuer, time.Now)
		requireWiring(t, err)
		base := x.Base{Clock: func() (string, int64, error) { ns, err := controlbudget.Now(); return "boot", ns, err }, UTC: time.Now, Capacity: &factoryCapacity{}}
		authority, err := xferbucket.NewAuthority(x.ConfigWriterProof{BrokerID: c.origins[i].BrokerID, InstanceID: c.origins[i].InstanceID, BootID: "boot", Generation: 1, Nonce: fmt.Sprint("config-", i)}, nil)
		requireWiring(t, err)
		buckets, err := xferbucket.New(x.BucketConfig{Base: base, JS: js, Authority: authority, Limits: x.StorageLimits{DiskBytes: 8 << 30, MaxPayload: nc.MaxPayload(), MaxMsgSize: nc.MaxPayload()}})
		requireWiring(t, err)
		cfg := xfercontrol.AssemblyConfig{Base: base, Origin: c.origins[i], Root: filepath.Join(root, "v1"), FallbackRoot: filepath.Join(root, "outbox"), Permits: permits[i], Transport: factoryUnavailablePorts{}, Receiver: factoryUnavailablePorts{}, Objects: factoryUnavailablePorts{}, Buckets: buckets, Owners: factoryOwners{b}, SessionOwners: factoryOwners{b}}
		b.cfg.TransferFactory = NewBrokerTransferFactory(func(*Broker, jetstream.JetStream, controlbudget.Deadline) (xfercontrol.AssemblyConfig, error) {
			return cfg, nil
		})
		requireWiring(t, startBrokerTransfers(ctx, b, c.conns[i]))
		defer func() {
			requireWiring(t, stopBrokerTransfers(b))
			if b.imageControl != nil {
				b.imageControl.cancel()
			}
		}()
		// The assembled legacy finalizer must reach the actual storage binding;
		// an absent stream is a backend refusal, never a missing-port fallback.
		// origin: hot-update S7 plan §5.3 / T29 / INTEG-R assembly regression
		legacy := b.transferRuntime.Load().Finalizer.(x.LegacyOrphanFinalizer)
		_, legacyErr := legacy.RetireLegacyOrphan(x.LegacyOrphanCandidate{Bucket: "xfer-absent", Stream: "OBJ_xfer-absent", StreamCreated: "original", Name: "orphan", NUID: "original", MetadataSequence: 1, MetadataDigest: "original"}, transferRouteDeadline(t, time.Second))
		if !errors.Is(legacyErr, jetstream.ErrStreamNotFound) {
			t.Fatalf("assembled legacy retirement did not reach real storage: %v", legacyErr)
		}
		var state, member string
		requireWiring(t, b.cfg.DB.QueryRow("SELECT state FROM sessions WHERE sid='lab'").Scan(&state))
		requireWiring(t, b.cfg.DB.QueryRow("SELECT pubkey_fp FROM members WHERE sid='lab'").Scan(&member))
		if state != "ACTIVE" || member != fp {
			t.Fatalf("replicated authority state=%q member=%q want=%q", state, member, fp)
		}
	}
	id := x.Identity{SID: "lab", NID: "node", ActorFP: fp, AgentInstance: "agent", TransferID: "original"}
	intent := x.Intent{Verb: "pull", Tier: "b", Path: "target", ChunkSize: 32768}
	permit, err := permits[original].IssueFor(id, intent, transferRouteDeadline(t, time.Second))
	requireWiring(t, err)
	graph := c.brokers[original].transferRuntime.Load()
	_, err = graph.Tracker.Admit(permit, intent, transferRouteDeadline(t, time.Second))
	requireWiring(t, err)
	id = permit.Identity
	who, err := graph.Bindings.Authority.(xferauth.ControlAuthority).Principal(actor, id, transferRouteDeadline(t, time.Second))
	requireWiring(t, err)
	if who.FP != fp || who.Role != x.RoleCTL {
		t.Fatalf("fixture principal = %+v", who)
	}
	source := proto.SourceSeal{Size: 4, SHA256: strings.Repeat("a", 64), ChunkSize: 32768, Chunks: 1, SourceFingerprint: "original-source"}
	request := func(op x.Operation, body any, target any) {
		t.Helper()
		subject, err := proto.SubjCtrlTransferRequest(actor, id.SID, "node", id.TransferID, op)
		requireWiring(t, err)
		data, err := json.Marshal(body)
		requireWiring(t, err)
		msg, err := ingress.Request(subject, data, time.Second)
		if err != nil {
			t.Fatalf("%s request: %v", op, err)
		}
		requireWiring(t, proto.DecodeTransferWire(msg.Data, target))
	}
	for _, binding := range []string{"cold", "hot"} {
		if binding == "hot" {
			id = x.Identity{SID: "lab-hot", NID: "node", ActorFP: fp, AgentInstance: "agent", TransferID: "original-hot"}
			permit, err = permits[original].IssueFor(id, intent, transferRouteDeadline(t, time.Second))
			requireWiring(t, err)
			_, err = graph.Tracker.Admit(permit, intent, transferRouteDeadline(t, time.Second))
			requireWiring(t, err)
			id = permit.Identity
			for i, b := range c.brokers {
				for _, sub := range b.imageControl.subs {
					requireWiring(t, sub.Unsubscribe())
				}
				b.imageControl.subs = nil
				b.imageControl.joins = nil
				requireWiring(t, subscribeBrokerImageBusiness(b, c.conns[i]))
				requireWiring(t, c.conns[i].FlushTimeout(time.Second))
			}
		}
		// Only current-home creation remains ONLINE-only. Continuation belongs to
		// the original follower even after another broker owns the node's home.
		routePropose(t, c, []string{fmt.Sprintf("UPDATE nodes SET nats_server='server-%d',status='STALE' WHERE nid='node'", leader)})
		var status proto.TransferStatusResp
		// Repeated reads make queue misrouting observable against the installed
		// handlers even when a random queue's first delivery hits the owner.
		for attempt := 0; attempt < 12; attempt++ {
			request(x.OpStatus, proto.TransferStatusReq{CallID: "repeat", Identity: proto.Identity(id), Origin: proto.Origin(permit.Origin)}, &status)
			if status.State != x.StatusLive || x.Identity(status.Identity) != id {
				t.Fatalf("%s repeated status lost owner: %+v", binding, status)
			}
		}
		request(x.OpStatus, proto.TransferStatusReq{CallID: "status", Identity: proto.Identity(id), Origin: proto.Origin(permit.Origin)}, &status)
		if status.State != x.StatusLive || x.Origin(status.Origin) != permit.Origin || x.Identity(status.Identity) != id {
			t.Fatalf("%s status lost original follower: %+v", binding, status)
		}
		request(x.OpSource, proto.TransferSourceReq{CallID: "source", Identity: proto.Identity(id), Origin: proto.Origin(permit.Origin), Source: source}, &status)
		if status.State != x.StatusLive || status.Code != "" {
			t.Fatalf("%s accepted STALE source refused: %+v", binding, status)
		}
		var reserved proto.TransferReserveResp
		request(x.OpReserve, proto.TransferReserveReq{CallID: "reserve", Identity: proto.Identity(id), Origin: proto.Origin(permit.Origin), ReceiverInstance: actor}, &reserved)
		if reserved.Code != "" || reserved.Grant == nil || x.Identity(reserved.Grant.Identity) != id {
			t.Fatalf("%s reserve lost original grant: %+v", binding, reserved)
		}
		request(x.OpCancel, proto.TransferCancelReq{CallID: "cancel", Identity: proto.Identity(id), Origin: proto.Origin(permit.Origin), Reason: x.CancelUser}, &status)
		if status.Code != x.CodeResultUnknown {
			t.Fatalf("%s unavailable receiver was treated as settlement: %+v", binding, status)
		}
		var probe proto.TransferProbeResp
		request(x.OpProbe, proto.TransferProbeReq{CallID: "probe", SID: id.SID, NID: "node"}, &probe)
		if probe.Code != x.CodePayloadUnsupported {
			t.Fatalf("%s probe fabricated agent capability: %+v", binding, probe)
		}
		var issued proto.TransferPermitResp
		request(x.OpPermit, proto.TransferPermitReq{CallID: "permit", Intent: proto.Intent(intent), TransferID: id.TransferID, AgentInstance: id.AgentInstance}, &issued)
		if issued.Code != "node_offline" || issued.Permit != nil {
			t.Fatalf("%s STALE creation admitted: %+v", binding, issued)
		}
		request(x.OpStatus, proto.TransferStatusReq{CallID: "unresolved", Identity: proto.Identity(id), Origin: proto.Origin(permit.Origin)}, &status)
		if status.State != x.StatusLive || status.Code != x.CodeResultUnknown || x.Identity(status.Identity) != id {
			t.Fatalf("cancel ambiguity lost retained identity: %+v", status)
		}
		var refused proto.TransferReserveResp
		request(x.OpReserve, proto.TransferReserveReq{CallID: "late-reserve", Identity: proto.Identity(id), Origin: proto.Origin(permit.Origin), ReceiverInstance: actor}, &refused)
		if refused.Code != x.CodeResultUnknown || refused.Grant != nil {
			t.Fatalf("unresolved cancellation reopened grant admission: %+v", refused)
		}
		// A conflicting terminal must be refused by the same original controller,
		// never acknowledged simply because a terminal-shaped payload arrived.
		var terminal proto.TransferTerminalResp
		wrong := *reserved.Grant
		wrong.GrantNonce = "other"
		request(x.OpTerminal, proto.TransferTerminalReq{CallID: "terminal", Identity: proto.Identity(id), Origin: proto.Origin(permit.Origin), Outcome: proto.ReceiverOutcome{Grant: wrong, Primitive: x.PrimitiveRename, Outcome: x.OutcomeNotEntered}}, &terminal)
		if terminal.Receipt != nil || terminal.Code == "" {
			t.Fatalf("%s conflicting terminal acknowledged: %+v", binding, terminal)
		}
		{
			// The negative receiver outcome is now the exact retained grant. A real
			// history receipt, not a shaped error, proves the tracker/history seam.
			request(x.OpTerminal, proto.TransferTerminalReq{CallID: "terminal", Identity: proto.Identity(id), Origin: proto.Origin(permit.Origin), Outcome: proto.ReceiverOutcome{Grant: *reserved.Grant, Primitive: x.PrimitiveRename, Outcome: x.OutcomeNotEntered}}, &terminal)
			if terminal.Code != "" || terminal.Receipt == nil || terminal.Receipt.Kind != "failed" || terminal.Receipt.HistoryStream != "history-"+id.SID || terminal.Receipt.HistorySeq == 0 {
				_, cause := graph.Tracker.Finish(id, x.ReceiverOutcome{Grant: x.CommitGrant{Identity: x.Identity(reserved.Grant.Identity), GrantNonce: reserved.Grant.GrantNonce, ReceiverInstance: reserved.Grant.ReceiverInstance, TerminalID: reserved.Grant.TerminalID}, Primitive: x.PrimitiveRename, Outcome: x.OutcomeNotEntered}, transferRouteDeadline(t, time.Second))
				t.Fatalf("exact terminal was not confirmed in real history: %+v; tracker: %v", terminal, cause)
			}
		}
		census, err := (TransferOwnerResolver{Forwarder: NewForwarderWithLimit(c.conns[leader], time.Second, 0)}).Census(permit.Origin, transferRouteDeadline(t, time.Second))
		requireWiring(t, err)
		if !census.Ready || !census.Complete || census.Origin != permit.Origin || len(census.Entries) != map[string]int{"cold": 1, "hot": 2}[binding] {
			t.Fatalf("%s directed census lost retained responsibility: %+v", binding, census)
		}
	}
	// Revocation still wins over STALE continuation. This must run against the
	// installed handler, not just the extracted liveness predicate.
	routePropose(t, c, []string{"DELETE FROM members"})
	var denied proto.TransferStatusResp
	request(x.OpStatus, proto.TransferStatusReq{CallID: "revoked", Identity: proto.Identity(id), Origin: proto.Origin(permit.Origin)}, &denied)
	if denied.Code != x.CodeNotOwner {
		t.Fatalf("revocation bypassed by retained entry: %+v", denied)
	}
}

func routePropose(t *testing.T, c *transferRouteCluster, queries []string) {
	t.Helper()
	probes := make([]clusterharness.LeaderProbe, len(c.nodes))
	for i := range probes {
		probes[i] = c.nodes[i]
	}
	var applied uint64
	_, err := clusterharness.WithLeader(t, probes, 5*time.Second, func(i int) error {
		err := c.nodes[i].Propose(func(*sql.DB) (*cluster.Command, error) {
			cmd := cluster.NewCommand(cluster.OpReconcileBatch)
			for _, q := range queries {
				cmd.Body = append(cmd.Body, cluster.Statement{SQL: q})
			}
			return cmd, nil
		})
		if err == nil {
			err = c.nodes[i].RODB().QueryRow("SELECT value FROM cluster_meta WHERE key='applied_index'").Scan(&applied)
		}
		return err
	})
	requireWiring(t, err)
	// Barrier replication makes the expected SQL authority observable on all nodes.
	for _, n := range c.nodes {
		mustWait(t, 5*time.Second, "replicated transfer authority", func() bool {
			var got uint64
			err := n.RODB().QueryRow("SELECT value FROM cluster_meta WHERE key='applied_index'").Scan(&got)
			return err == nil && got >= applied
		})
	}
}
