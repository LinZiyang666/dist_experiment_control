package infraattach

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracompose"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// ServiceConfig carries the deployment's real leaf roots and public selector
// resolver. Recovery is deliberately replaced at composition by this broker's
// own producer gate; an unrelated gate cannot certify its ACK-prefix recovery.
// Capability must observe the installed helper/unit/runtime, not a build flag.
// BrokerInfra.Pending must read the durable shared domain; a cached status
// projection cannot authorize a concurrent image cut after controller restart.
// origin: hot-update S9 plan §2.1 / §7.1 / WIRE-2 R10
type ServiceConfig struct {
	Dependencies c.Dependencies
	Composition  *infracompose.Config
	Plan         func(proto.InfraCommand) (c.Plan, error)
	Capability   func() *proto.InfraCapability
}

type serviceRuntime struct {
	c.Service
	mu      sync.Mutex
	active  uint32
	current *c.Status
}

// The service keeps only the last public observation in memory. Durable plans
// and pending recovery remain in the registered S8 transaction and root journal.
func (r *serviceRuntime) remember(status c.Status) {
	if status.Tx == (c.ID{}) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.current = &status
}

func (r *serviceRuntime) Start(p c.Plan, d c.Deadline, stop c.Stop) (c.Status, error) {
	r.mu.Lock()
	r.active++
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.active--; r.mu.Unlock() }()
	s, err := r.Service.Start(p, d, stop)
	r.remember(s)
	return s, err
}
func (r *serviceRuntime) Resume(id c.ID, d c.Deadline, stop c.Stop) (c.Status, error) {
	r.mu.Lock()
	r.active++
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.active--; r.mu.Unlock() }()
	s, err := r.Service.Resume(id, d, stop)
	r.remember(s)
	return s, err
}
func (r *serviceRuntime) Status(id c.ID, d c.Deadline) (c.Status, error) {
	s, err := r.Service.Status(id, d)
	if err == nil {
		r.remember(s)
	}
	return s, err
}

func servicePending(o *Broker) bool {
	if o == nil || o.service == nil {
		return false
	}
	r := o.service
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active != 0 || r.current != nil && r.current.Phase != c.Completed && r.current.Phase != c.Deferred
}

// InfraBindings is the single local/remote Service instance. A legacy or partial
// descriptor has no backend, and therefore cannot advertise an upgrade version.
func InfraBindings(o *Broker) *adminsock.InfraBindings {
	if o == nil || o.service == nil {
		return nil
	}
	config := o.cfg.Infra.Service
	return &adminsock.InfraBindings{Service: o.service, Plan: config.Plan, Capability: func() *proto.InfraCapability {
		if !o.Admitted() {
			return nil
		}
		return config.Capability()
	}}
}

func BindAdmin(o *Broker, backend adminsock.Backend) adminsock.Backend {
	backend.Infra = InfraBindings(o)
	return backend
}

func serviceSnapshot(o *Broker) (*proto.InfraCapability, *c.Status) {
	bindings := InfraBindings(o)
	if bindings == nil {
		return nil, nil
	}
	r := o.service
	r.mu.Lock()
	var status *c.Status
	if r.current != nil {
		copy := *r.current
		status = &copy
	}
	r.mu.Unlock()
	return bindings.Capability(), status
}

func SetClusterHealth(o *Broker, response *proto.ClusterHealthResp) {
	capability, status := serviceSnapshot(o)
	proto.SetInfraClusterHealth(response, capability, status)
}
func SetRuntimeReport(o *Broker, response *adminsock.RuntimeReport) {
	capability, status := serviceSnapshot(o)
	adminsock.SetInfraRuntimeReport(response, capability, status)
}

// InfraHandler is a direct, exact-host subscriber in both cold and hot tables.
// Queue delay is charged to the signed attempt budget. A request cannot select
// a different actor's reply root, legacy reload, or HostSupport operation.
// origin: hot-update S9 plan §7.1 / T47 / WIRE-2 R14
func InfraHandler(o *Broker, target string, account func() string, now func() time.Time, stop c.Stop) nats.MsgHandler {
	return func(msg *nats.Msg) {
		received := time.Now()
		request, err := proto.DecodeInfraUpgradeRequest(msg.Data)
		if err != nil {
			return
		}
		node, ok := proto.ParseClusterInfra(msg.Subject)
		prefix := auth.InboxPrefixFor(request.Actor)
		if !ok || node != target || account == nil || now == nil ||
			proto.VerifyInfraUpgradeRequest(request, account(), request.Actor, target, prefix, msg.Reply, now()) != nil {
			return
		}
		issued, _ := time.Parse(time.RFC3339Nano, request.IssuedAt) // The verifier already checked canonical time.
		elapsed := max(time.Since(received), now().Sub(issued))
		remaining := time.Duration(request.Request.RemainingNS) - elapsed
		deadline, err := controlbudget.After(remaining)
		response := &proto.InfraUpgradeResp{Code: proto.CodeInfraBudgetExceeded, Error: proto.CodeInfraBudgetExceeded}
		if err == nil && remaining > 0 {
			response = adminsock.ExecuteInfra(InfraBindings(o), request.Request, deadline, stop)
		}
		data, err := json.Marshal(response)
		if err == nil {
			_ = msg.Respond(data)
		}
	}
}

// ContextStop keeps construction-only subscription tables safe before Run binds
// its control lifetime. Live Run always supplies the root cancellation channel.
func ContextStop(ctx context.Context) c.Stop {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

// QueueBusiness keeps exact-host infra control direct in both cold and restored
// subscription loops. All established broadcast/liveness exclusions still apply.
// origin: hot-update S9 plan §7.1 / WIRE-2 R14
func QueueBusiness(clustered bool, subject string, broadcast bool) bool {
	return clustered && !broadcast && !strings.HasPrefix(subject, proto.SubjClusterPrefix+".infra.") && !strings.HasSuffix(subject, ".register.req") && !strings.HasSuffix(subject, ".heartbeat")
}

// TrackSubscription retains the callback join beside its owned subscription.
// A hot retirement must join callbacks before releasing their borrowed store.
func TrackSubscription(sub *nats.Subscription, subs *[]*nats.Subscription, joins *[]<-chan struct{}) {
	done := make(chan struct{})
	sub.SetClosedHandler(func(string) { close(done) })
	*subs = append(*subs, sub)
	*joins = append(*joins, done)
}
