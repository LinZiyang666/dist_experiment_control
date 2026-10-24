package clusterroll

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgraderoll"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
)

// ClusterPorts retains the caller's authenticated transport and cancellation.
// Trigger performs one signed targeted exchange; the driver never retries an
// unsafe mutation merely because its reply was lost.
// origin: hot-update S8 plan §3.1 / §7.1; S8 internal review cluster-roll F4
type ClusterPorts struct {
	Trigger       func(context.Context, *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error)
	Health        func(context.Context) ([]proto.ClusterHealthResp, error)
	ActorFP, Home string
	Output        io.Writer
	Agent         func(context.Context, upgradetx.Key, []byte) ([]byte, error)
	SID           string
	Requested     *clusterupgrade.Plan
}

// AgentExchange binds the signed agent route to the caller's authenticated
// connection and request budget, keeping transport policy in the roll driver.
// origin: hot-update S8 plan §3.1 / §7.1 / P19
func AgentExchange(nc *nats.Conn, actor string, seed []byte) func(context.Context, upgradetx.Key, []byte) ([]byte, error) {
	return func(ctx context.Context, key upgradetx.Key, body []byte) ([]byte, error) {
		if rollcontrol.IsRequest(body) {
			request, err := rollcontrol.Decode(body)
			if err != nil {
				return nil, err
			}
			body, err = rollcontrol.SignAgentRequest(seed, request)
			if err != nil {
				return nil, err
			}
		}
		requestCtx, cancel := context.WithTimeout(ctx, upgradebudget.RequestLatency)
		defer cancel()
		reply, err := nc.RequestWithContext(requestCtx, proto.SubjCmdBy(key.SID, actor, key.ConfiguredNID, "upgrade-tx"), body)
		if err != nil {
			return nil, err
		}
		return reply.Data, nil
	}
}

type clusterClient struct {
	ports  ClusterPorts
	parent upgradetx.Grant
	plan   upgradetx.Plan
	ctx    context.Context
}
type rollJournal struct{ client *clusterClient }

func (c *clusterClient) call(ctx context.Context, target string, r rollcontrol.Request) (rollcontrol.Response, error) {
	r.Parent = c.parent
	raw, err := json.Marshal(r)
	if err != nil {
		return rollcontrol.Response{}, err
	}
	response, err := c.ports.Trigger(ctx, &proto.ClusterUpgradeReq{Op: "upgrade-tx", TargetNode: target, ActorFP: c.ports.ActorFP, Holder: c.parent.Handle.TxID, Upgrade: raw})
	if err != nil {
		return rollcontrol.Response{}, rollcontrol.Unknown("roll reply was lost; resume the original parent")
	}
	if response == nil {
		return rollcontrol.Response{}, rollcontrol.Unknown("roll route returned no reply")
	}
	var out rollcontrol.Response
	if err := upgradewire.StrictDecode(response.Upgrade, &out); err != nil {
		return out, rollcontrol.Unknown("invalid roll reply for " + r.Op + ": " + response.Code + ": " + err.Error())
	}
	if out.Code != "" {
		return out, &upgradetx.Error{Code: out.Code, Detail: out.Detail, Accepted: out.Accepted}
	}
	if !response.OK {
		return out, rollcontrol.Unknown("roll route refused without a transaction result")
	}
	return out, nil
}
func (c *clusterClient) coordinator() string {
	return strings.TrimSuffix(c.parent.Handle.Key.ConfiguredNID, "@roll")
}
func (c *clusterClient) component(node string) upgradecli.Remote {
	return upgradecli.Remote{Exchange: func(ctx context.Context, r upgradewire.Request) (upgradewire.Response, error) {
		raw, err := upgradewire.EncodeRequest(r)
		if err != nil {
			return upgradewire.Response{}, &upgradecli.NotSentError{Err: err}
		}
		response, err := c.ports.Trigger(ctx, &proto.ClusterUpgradeReq{Op: "upgrade-tx", TargetNode: node, ActorFP: c.ports.ActorFP, Holder: c.parent.Handle.TxID, Upgrade: raw})
		if err != nil {
			return upgradewire.Response{}, err
		}
		if response == nil {
			return upgradewire.Response{}, rollcontrol.Unknown("component reply is missing")
		}
		if len(response.Upgrade) == 0 {
			return upgradewire.Response{}, &upgradetx.Error{Code: response.Code, Detail: response.Error}
		}
		return upgradewire.DecodeResponse(response.Upgrade)
	}}
}
func (j rollJournal) Load(key upgradetx.Key, tx string) (upgradetx.View, error) {
	c := j.client
	if key != c.parent.Handle.Key || tx != c.parent.Handle.TxID {
		return upgradetx.View{}, rejected("parent lookup differs from the recorded grant")
	}
	out, err := c.call(c.ctx, c.coordinator(), rollcontrol.Request{Op: "roll.load"})
	return out.View, err
}
func (j rollJournal) Apply(h upgradetx.Handle, event upgradetx.Event) (upgradetx.Record, error) {
	out, err := j.client.call(j.client.ctx, j.client.coordinator(), rollcontrol.Request{Op: "roll.apply", Expected: h, Event: event})
	return out.View.Historical, err
}
func (j rollJournal) SealPlan(p upgradetx.Plan) (upgradetx.Plan, error) {
	out, err := j.client.call(j.client.ctx, j.client.coordinator(), rollcontrol.Request{Op: "roll.seal", Plan: p})
	return out.Plan, err
}
func (j rollJournal) LoadPlan(h upgradetx.Handle) (upgradetx.Plan, error) {
	out, err := j.client.call(j.client.ctx, j.client.coordinator(), rollcontrol.Request{Op: "roll.plan", Expected: h})
	return out.Plan, err
}
func (j rollJournal) CompletePlan(h upgradetx.Handle, o upgradetx.Observation) (upgradetx.View, error) {
	c := j.client
	samples, err := c.samples(c.ctx, c.plan, o.Nonce)
	if err != nil {
		return upgradetx.View{}, err
	}
	out, err := c.call(c.ctx, c.coordinator(), rollcontrol.Request{Op: "roll.complete", Expected: h, Nonce: o.Nonce, Samples: samples})
	return out.View, err
}
func (c *clusterClient) samples(ctx context.Context, p upgradetx.Plan, nonce string) ([]rollcontrol.Sample, error) {
	var steps []upgradetx.Step
	for _, step := range p.Steps {
		if step.Kind == upgradetx.StepBrokerCut || step.Kind == upgradetx.StepAgentCut {
			steps = append(steps, step)
		}
	}
	out := make([]rollcontrol.Sample, len(steps))
	errs := make([]error, len(steps))
	var joined sync.WaitGroup
	for i, step := range steps {
		joined.Go(func() {
			reply, err := c.stepCall(ctx, step, rollcontrol.Request{Op: "roll.sample", Plan: p, Step: step, Nonce: nonce})
			out[i], errs[i] = reply.Sample, err
		})
	}
	joined.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c *clusterClient) leader(ctx context.Context) (string, error) {
	health, err := c.ports.Health(ctx)
	if err != nil {
		return "", err
	}
	id := ""
	for _, row := range health {
		if row.WritableLeaderConfirmed {
			if id != "" {
				return "", rollcontrol.Unknown("multiple writable leaders")
			}
			id = row.NodeID
		}
	}
	if id == "" {
		return "", rollcontrol.Unknown("no verified writable leader")
	}
	return id, nil
}
func (c *clusterClient) Reserve(ctx context.Context, p upgradetx.Plan) error {
	leader, err := c.leader(ctx)
	if err != nil {
		return err
	}
	if _, err = c.call(ctx, leader, rollcontrol.Request{Op: "roll.reserve_cluster", Plan: p}); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, step := range p.Steps {
		if (step.Kind != upgradetx.StepBrokerCut && step.Kind != upgradetx.StepAgentCut) || seen[step.Participant.DomainID] {
			continue
		}
		seen[step.Participant.DomainID] = true
		if _, err = c.stepCall(ctx, step, rollcontrol.Request{Op: "roll.reserve", Plan: p}); err != nil {
			return err
		}
	}
	return nil
}
func (c *clusterClient) Check(ctx context.Context, p upgradetx.Plan, nonce string) (upgraderoll.FleetProof, error) {
	samples, err := c.samples(ctx, p, nonce)
	if err != nil {
		return upgraderoll.FleetProof{}, err
	}
	out, err := c.call(ctx, c.coordinator(), rollcontrol.Request{Op: "roll.fleet", Nonce: nonce, Samples: samples})
	return out.Fleet.Proof(), err
}
func (c *clusterClient) Renew(ctx context.Context, h upgradetx.Handle, deadline int64) error {
	if h != c.plan.Parent || deadline != c.plan.Deadline || time.Now().UnixNano() >= deadline {
		return rollcontrol.Unknown("original parent deadline expired")
	}
	leader, err := c.leader(ctx)
	if err != nil {
		return err
	}
	out, err := c.ports.Trigger(ctx, &proto.ClusterUpgradeReq{Op: "renew-lock", TargetNode: leader, Holder: h.TxID})
	if err != nil {
		return err
	}
	if out == nil || !out.OK {
		return rollcontrol.Unknown("original holder renewal was refused")
	}
	return nil
}

// LeaseRenewInterval keeps the production cadence tied to the cluster lease.
// Command tests compress only this courier timer so HALT/join is observed while
// the original driver process remains alive; the lease TTL is never changed.
// origin: hot-update S8 plan §6.3 / L21; S8 internal review upgrade-maketest-2 F1
var LeaseRenewInterval = cluster.LockLeaseRenewInterval

func (c *clusterClient) RenewalInterval() time.Duration { return LeaseRenewInterval }
func (c *clusterClient) Release(ctx context.Context, p upgradetx.Plan, s upgradetx.Step, n string) (upgradetx.Receipt, error) {
	target := route(s.Grant)
	op := "roll.release_host"
	if s.Ordinal == uint32(len(p.Steps)) {
		op = "roll.release_cluster"
		leader, err := c.leader(ctx)
		if err != nil {
			return upgradetx.Receipt{}, err
		}
		target = leader
	}
	var samples []rollcontrol.Sample
	if op == "roll.release_cluster" {
		var err error
		samples, err = c.samples(ctx, p, n)
		if err != nil {
			return upgradetx.Receipt{}, err
		}
	}
	if op == "roll.release_host" && s.Participant.Component == upgradetx.ComponentAgent {
		out, err := c.stepCall(ctx, s, rollcontrol.Request{Op: op, Plan: p, Step: s, Nonce: n})
		return out.Receipt, err
	}
	out, err := c.call(ctx, target, rollcontrol.Request{Op: op, Plan: p, Step: s, Nonce: n, Samples: samples})
	return out.Receipt, err
}
func (c *clusterClient) Artifact(ctx context.Context, g upgradetx.Grant) (upgradeartifact.Receipt, error) {
	for _, s := range c.plan.Steps {
		if s.Grant == g {
			out, err := c.stepCall(ctx, s, rollcontrol.Request{Op: "roll.stage", Plan: c.plan, Step: s})
			return out.Artifact, err
		}
	}
	return upgradeartifact.Receipt{}, rollcontrol.Unknown("artifact grant is outside the sealed plan")
}
func (c *clusterClient) Converge(ctx context.Context, s upgradetx.Step, n string) (upgradetx.Receipt, error) {
	leader, err := c.leader(ctx)
	if err != nil {
		return upgradetx.Receipt{}, err
	}
	response, err := c.call(ctx, leader, rollcontrol.Request{Op: "roll.home", Plan: c.plan, Step: s, Nonce: n})
	return response.Receipt, err
}
func (c *clusterClient) ObserveParent(ctx context.Context, p upgradetx.Plan, n string) (upgradetx.Observation, error) {
	samples, err := c.samples(ctx, p, n)
	if err != nil {
		return upgradetx.Observation{}, err
	}
	out, err := c.call(ctx, c.coordinator(), rollcontrol.Request{Op: "roll.observe", Nonce: n, Samples: samples})
	return out.Observation, err
}

// ClusterTransport is also useful to hermetic product harnesses: they exercise
// the same signed route, durable journal and holder adapter as the command.
func ClusterTransport(ctx context.Context, p ClusterPorts, parent upgradetx.Grant, plan upgradetx.Plan) (upgraderoll.PlanStore, upgraderoll.Endpoint, error) {
	c := &clusterClient{ctx: ctx, ports: p, parent: parent, plan: plan}
	journal := rollJournal{client: c}
	components := clusterComponents{client: c}
	endpoint, err := upgraderoll.BindEndpoint(journal, components, c, plan.Parent)
	if err != nil {
		return nil, nil, err
	}
	return journal, artifactEndpoint{Endpoint: endpoint, client: c}, nil
}

type artifactEndpoint struct {
	upgraderoll.Endpoint
	client *clusterClient
}

func (e artifactEndpoint) VerifyArtifact(ctx context.Context, _ upgradetx.Plan, r upgradeartifact.Receipt) error {
	verified, err := e.client.Artifact(ctx, r.Grant)
	if err != nil {
		return err
	}
	if verified != r {
		return rollcontrol.Unknown("publication differs from the original grant")
	}
	return nil
}

type clusterComponents struct{ client *clusterClient }

func (c clusterComponents) Prepare(ctx context.Context, key upgradetx.Key, r upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	return c.client.componentKey(key).Prepare(ctx, key, r)
}
func (c clusterComponents) Query(ctx context.Context, key upgradetx.Key, tx, n string) (upgradetx.View, error) {
	for {
		v, err := c.client.componentKey(key).Query(ctx, key, tx, n)
		if err == nil {
			return v, nil
		}
		if !transientQuery(err) && (key.Component != upgradetx.ComponentAgent || !lostAgentQuery(err)) {
			return v, err
		}
		select {
		case <-ctx.Done():
			return v, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
func (c clusterComponents) Start(ctx context.Context, g upgradetx.Grant, r upgradeartifact.Receipt) (upgradetx.View, error) {
	if g.Handle.Key.Component == upgradetx.ComponentAgent {
		return c.client.componentKey(g.Handle.Key).Start(ctx, g, r)
	}
	return c.client.startBroker(ctx, g, r)
}

func route(g upgradetx.Grant) string { node, _, _ := strings.Cut(g.ParentSlot, ":"); return node }
func (c *clusterClient) routeKey(key upgradetx.Key) string {
	for _, step := range c.plan.Steps {
		if step.Participant == key {
			return route(step.Grant)
		}
	}
	return ""
}

// Only the original parent journal operations are exposed by this adapter.
// Parent creation/acceptance are explicit signed orchestration operations;
// an image reverse or death archive cannot be inferred for bookkeeping.
func (j rollJournal) Prepare(upgradetx.Key, upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	return upgradetx.Grant{}, rejected("parent creation requires the signed snapshot operation")
}
func (j rollJournal) Start(upgradetx.Grant, upgradetx.Fence) (upgradetx.Record, error) {
	return upgradetx.Record{}, rejected("parent acceptance requires the signed snapshot operation")
}
func (j rollJournal) PrepareReverse(upgradetx.Fence, upgradetx.ReverseRequest) (upgradetx.Grant, error) {
	return upgradetx.Grant{}, rejected("parent bookkeeping has no image reverse")
}
func (j rollJournal) Archive(upgradetx.Handle, upgradetx.DeathProof) (upgradetx.View, error) {
	return upgradetx.View{}, rejected("a component death proof cannot archive an aggregate parent")
}
func (j rollJournal) CloseWindow(upgradetx.Handle) (upgradetx.Metadata, error) {
	return upgradetx.Metadata{}, rejected("parent bookkeeping has no image rollback window")
}
func (c clusterComponents) Rollback(ctx context.Context, f upgradetx.Fence, r upgradetx.ReverseRequest) (upgradetx.View, error) {
	return c.client.componentKey(f.Current.Key).Rollback(ctx, f, r)
}
func (c clusterComponents) Cancel(ctx context.Context, h upgradetx.Handle) (upgradetx.View, error) {
	return c.client.componentKey(h.Key).Cancel(ctx, h)
}
func (c clusterComponents) Archive(ctx context.Context, h upgradetx.Handle) (upgradetx.View, error) {
	return c.client.componentKey(h.Key).Archive(ctx, h)
}
func (c clusterComponents) CloseWindow(ctx context.Context, h upgradetx.Handle) (upgradetx.View, error) {
	return c.client.componentKey(h.Key).CloseWindow(ctx, h)
}
