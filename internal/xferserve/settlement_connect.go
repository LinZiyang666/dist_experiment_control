package xferserve

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
)

const continuationAuthoritySubject = proto.SubjectPrefix + ".cluster.transfer-continuation"

// These broker-only requests carry private authentication, never public census
// rows. The original origin performs exact capsule lookup; the connecting
// broker receives only a least-privilege grant. Retire names one writer epoch.
type authorityRequest struct {
	Operation string               `json:"operation"`
	Origin    x.Origin             `json:"origin"`
	BudgetNS  int64                `json:"budget_ns"`
	Head      *configHead          `json:"head,omitempty"`
	Capsule   *x.SettlementCapsule `json:"capsule,omitempty"`
	Proof     *x.NKeyProof         `json:"proof,omitempty"`
	SID       string               `json:"sid,omitempty"`
}
type authorityReply struct {
	Code    string             `json:"code,omitempty"`
	Grant   *x.SettlementGrant `json:"grant,omitempty"`
	Members []x.Origin         `json:"members,omitempty"`
	Census  *x.LedgerCensus    `json:"census,omitempty"`
}

func decodeClosed(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	var extra json.RawMessage
	if d.Decode(&extra) != io.EOF {
		return xferauth.ErrDenied
	}
	return nil
}

func authorityRPC(nc *nats.Conn, req authorityRequest, d x.Deadline) (authorityReply, error) {
	ctx, cancel, err := authorityContext(d)
	if err != nil {
		return authorityReply{}, err
	}
	defer cancel()
	if nc == nil || !nc.IsConnected() {
		return authorityReply{}, xferauth.ControlRefusal(x.CodeOwnerRecovering)
	}
	end, _ := ctx.Deadline()
	req.BudgetNS = int64(time.Until(end))
	body, err := json.Marshal(req)
	if err != nil {
		return authorityReply{}, err
	}
	msg, err := nc.RequestWithContext(ctx, continuationAuthoritySubject, body)
	if err != nil {
		return authorityReply{}, xferauth.ControlRefusal(x.CodeResultUnknown)
	}
	var resp authorityReply
	if decodeClosed(msg.Data, &resp) != nil || resp.Code != "" {
		return resp, xferauth.ControlRefusal(x.CodeResultUnknown)
	}
	return resp, nil
}

type settlementReconnect struct {
	users     brokerimage.StoreAdmission
	local     x.SettlementVerifier
	owners    Owners
	directory *Directory
	issuer    string
	conn      func() *nats.Conn
}

func (v *settlementReconnect) Verify(c x.SettlementCapsule, p x.NKeyProof, d x.Deadline) (x.SettlementGrant, error) {
	// Remote verification borrows the original owner's bus and directory too.
	// Joining only local verification can close those handles beneath a live
	// auth callout during an image cut.
	// origin: hot-update S7 plan §8.2 / T31
	if !v.users.Begin() {
		return x.SettlementGrant{}, xferauth.ErrStopped
	}
	defer v.users.End()
	issuer := v.issuer
	if v.directory != nil {
		var err error
		issuer, err = v.directory.issuer(c.Origin, d)
		if err != nil {
			return x.SettlementGrant{}, xferauth.ErrDenied
		}
	} else if c.Origin != v.owners.Origin {
		return x.SettlementGrant{}, xferauth.ErrDenied
	}
	trusted, err := xferauth.NewIssuerVerifier([]string{issuer})
	if err != nil || c.Issuer != issuer || trusted.Verify(c, p) != nil {
		return x.SettlementGrant{}, xferauth.ErrDenied
	}
	if c.Origin == v.owners.Origin {
		return v.local.Verify(c, p, d)
	}
	resp, err := authorityRPC(v.conn(), authorityRequest{Operation: "settle", Origin: c.Origin, Capsule: &c, Proof: &p}, d)
	if err != nil || resp.Grant == nil {
		return x.SettlementGrant{}, xferauth.ErrDenied
	}
	g := *resp.Grant
	if g.Identity != c.Identity || g.Origin != c.Origin || g.EntryNonce != c.Identity.EntryNonce || g.ReceiverNkey != c.ReceiverNkey || g.OwnerGeneration != c.Origin.Generation {
		return x.SettlementGrant{}, xferauth.ErrDenied
	}
	return g, nil
}
func (v *settlementReconnect) Stop(d x.Deadline) error {
	if err := v.users.Fence(d); err != nil {
		return err
	}
	return v.local.Stop(d)
}

// CurrentSettlement resolves the currently installed graph for EACH auth
// callout, including callouts installed before business and same-PID imports.
// Missing/currently retiring graphs fail closed; no cached ordinary grant is
// treated as settlement authentication.
// origin: hot-update S7 plan §6.4 / §8.2 / T31
func CurrentSettlement(current func() x.SettlementVerifier) x.SettlementVerifier {
	return currentSettlement{current: current}
}

type currentSettlement struct{ current func() x.SettlementVerifier }

func (v currentSettlement) Verify(c x.SettlementCapsule, p x.NKeyProof, d x.Deadline) (x.SettlementGrant, error) {
	if verifier := v.current(); verifier != nil {
		return verifier.Verify(c, p, d)
	}
	return x.SettlementGrant{}, xferauth.ErrDenied
}
func (currentSettlement) Stop(x.Deadline) error { return nil } // This borrowed view owns no graph.

// ClusterService's admission is joined with the existing cluster forward
// owner. It creates no waiter goroutine and never borrows an old SQL binding
// after that owner's cut fence has completed.
type ClusterService struct {
	users brokerimage.StoreAdmission
	nc    ClusterConnection
	sub   *nats.Subscription
}

// ClusterConnection borrows only subscription and drain operations. Dialer
// callbacks, including resolver contexts, remain with the connection owner.
// origin: S7 internal review GATES-PROD F1
type ClusterConnection interface {
	Subscribe(string, nats.MsgHandler) (*nats.Subscription, error)
	FlushTimeout(time.Duration) error
}

func (a *Assembly) StartCluster(nc ClusterConnection) (*ClusterService, error) {
	if a.Directory == nil {
		return nil, nil
	}
	s := &ClusterService{nc: nc}
	var err error
	s.sub, err = nc.Subscribe(continuationAuthoritySubject, func(msg *nats.Msg) {
		var req authorityRequest
		if msg.Reply == "" || decodeClosed(msg.Data, &req) != nil || (req.Origin.BrokerID != a.Owners.Origin.BrokerID && req.Operation != "roster") {
			return
		}
		if !s.users.Begin() {
			return
		}
		defer s.users.End()
		// Leader verification borrows the node/store too. Admission must cover
		// that first read, not just the subsequent census/config dispatch.
		// origin: hot-update S7 plan §8.2 / T39
		if req.Operation == "roster" && (a.Host.Leader == nil || !a.Host.Leader()) {
			return
		}
		resp := authorityReply{Code: x.CodeResultUnknown}
		if (req.Origin == a.Owners.Origin || req.Operation == "roster") && req.BudgetNS > 0 && req.BudgetNS <= int64(5*time.Second) {
			d, err := controlbudget.After(time.Duration(req.BudgetNS))
			if err == nil {
				switch req.Operation {
				case "session-census":
					if req.Head == nil && req.Capsule == nil && req.Proof == nil {
						census, err := a.Owners.Census(req.Origin, d)
						if err == nil && census.Ready && census.Complete && census.Code == "" {
							rows := census.Rows
							census.Rows = nil
							for _, row := range rows {
								if req.SID == "" || row.Identity.SID == req.SID {
									row.Settlement = nil
									census.Rows = append(census.Rows, row)
								}
							}
							resp = authorityReply{Census: &census}
						}
					}
				case "roster":
					if req.Origin == (x.Origin{}) && req.Head == nil && req.Capsule == nil && req.Proof == nil && a.Host.Peers != nil {
						members, err := a.Host.Peers(d)
						if err == nil && len(members) > 0 {
							resp = authorityReply{Members: members}
						}
					}
				case "retire":
					if req.Head != nil && req.Capsule == nil && req.Proof == nil {
						if w, ok := a.Local.(*clusterWriter); ok {
							_, err = w.retire(req.Head, d)
							if err == nil {
								resp.Code = ""
							}
						}
					}
				case "settle":
					if req.Head == nil && req.Capsule != nil && req.Proof != nil {
						grant, err := a.Reconnect.local.Verify(*req.Capsule, *req.Proof, d)
						if err == nil {
							resp = authorityReply{Grant: &grant}
						}
					}
				}
			}
		}
		data, _ := json.Marshal(resp)
		// Responses contain only public scope or a fixed refusal, never capsules.
		_ = msg.Respond(data)
	})
	return s, err
}

func clusterRoster(nc *nats.Conn, d x.Deadline) ([]x.Origin, error) {
	r, err := authorityRPC(nc, authorityRequest{Operation: "roster"}, d)
	if err != nil || len(r.Members) == 0 || r.Grant != nil {
		return nil, xferauth.ControlRefusal(x.CodeOwnerRecovering)
	}
	return r.Members, nil
}

// The deletion barrier needs canonical history/cleanup proof, rather than only
// the public GC protection count. This directed broker-only response omits all
// capsules and cannot be reached through the ctl census/status ACL.
// origin: hot-update S7 plan §6.4 / T31 / LQ1
func (a *Directory) sessionCensus(origin x.Origin, sid string, d x.Deadline) (x.LedgerCensus, error) {
	r, err := authorityRPC(a.js.Conn(), authorityRequest{Operation: "session-census", Origin: origin, SID: sid}, d)
	if err != nil || r.Census == nil || r.Census.Origin != origin || !r.Census.Ready || !r.Census.Complete || r.Census.Code != "" {
		return x.LedgerCensus{}, xferauth.ControlRefusal(x.CodeOwnerRecovering)
	}
	for _, row := range r.Census.Rows {
		if sid != "" && row.Identity.SID != sid || row.Origin != origin || row.Settlement != nil {
			return x.LedgerCensus{}, xferauth.ControlRefusal(x.CodeOwnerRecovering)
		}
	}
	return *r.Census, nil
}

func probeRoster(h Host, peers func(x.Deadline) ([]x.Origin, error), d x.Deadline) error {
	if h.Probe == nil {
		return xferauth.ControlRefusal(x.CodeOwnerRecovering)
	}
	members, err := peers(d)
	if err != nil {
		return err
	}
	for _, m := range members {
		p, err := h.Probe(m, d)
		if err != nil || p.Version != x.Version || p.HistoryGuardVersion != x.Version || p.BucketConfigVersion != x.Version {
			return xferauth.ControlRefusal(x.CodeOwnerRecovering)
		}
	}
	return nil
}

func (s *ClusterService) Join(d x.Deadline) error {
	if s == nil {
		return nil
	}
	return s.users.Fence(d)
}
func (s *ClusterService) Resume() {
	if s != nil {
		s.users.Resume()
	}
}
func (s *ClusterService) Stop(d x.Deadline) error {
	if s == nil {
		return nil
	}
	if err := s.Join(d); err != nil {
		return err
	}
	if s.sub == nil {
		return nil
	}
	err := s.sub.Unsubscribe()
	left, deadlineErr := controlbudget.Remaining(d)
	if deadlineErr != nil {
		return errors.Join(err, deadlineErr)
	}
	return errors.Join(err, s.nc.FlushTimeout(left))
}

// ForwardCensus augments the original ledger with issued-but-not-admitted
// permits, retaining the public protection set on the existing census RPC.
func ForwardCensus(current func() Owners) func(x.Origin, x.Deadline) (x.LedgerCensus, error) {
	return func(origin x.Origin, d x.Deadline) (x.LedgerCensus, error) {
		o := current()
		if origin != o.Origin {
			return x.LedgerCensus{}, xferauth.ControlRefusal(x.CodeOwnerRecovering)
		}
		return o.Census(origin, d)
	}
}

func MemberOrigins(node *cluster.Node, d x.Deadline) ([]x.Origin, error) {
	if node == nil {
		return nil, xferauth.ControlRefusal(x.CodeOwnerRecovering)
	}
	if _, err := controlbudget.Remaining(d); err != nil {
		return nil, err
	}
	config, err := node.RaftConfiguration()
	if err != nil {
		return nil, err
	}
	var out []x.Origin
	for _, member := range config {
		if member.Voter {
			out = append(out, x.Origin{BrokerID: member.NodeID})
		}
	}
	return out, nil
}

func LocalCensus(origin x.Origin, ledger x.LedgerPort, a *Assembly, requested x.Origin, d x.Deadline) (x.LedgerCensus, error) {
	if a != nil {
		return a.Owners.Census(requested, d)
	}
	c, err := ledger.Scan(d)
	if err == nil && c.Origin == (x.Origin{}) {
		c.Origin = origin
	}
	return c, err
}

// origin: S7 internal review GATES-PROD F1
func ProbeOrigin(current func() x.Origin, nc interface{ MaxPayload() int64 }) func(x.Deadline) (x.ProbeResult, error) {
	return func(d x.Deadline) (x.ProbeResult, error) {
		if _, err := controlbudget.Remaining(d); err != nil {
			return x.ProbeResult{}, err
		}
		o := current()
		if o == (x.Origin{}) {
			return x.ProbeResult{}, xferauth.ControlRefusal(x.CodeOwnerRecovering)
		}
		return x.ProbeResult{Version: x.Version, HistoryGuardVersion: x.Version, BucketConfigVersion: x.Version, InstanceID: o.InstanceID, MaxPayload: nc.MaxPayload(), MaxMsgSize: nc.MaxPayload()}, nil
	}
}
