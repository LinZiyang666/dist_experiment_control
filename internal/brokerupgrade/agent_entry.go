// The broker's agent-upgrade entries: the old `upgrade` wire and the S8
// `upgrade-tx` forwarding hop. Both cross the same legacy policy before one
// byte reaches an agent; the Broker owner keeps only subject admission/replies.
// origin: hot-update S8 plan §7.1 / §7.2 / §11.2 P15
package brokerupgrade

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/agentroll"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/agentroute"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
)

var sha256HexRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

// LegacyRefusal is one old-wire refusal. Audit is empty only where the
// historical handler replied without an audit row (an unparseable body).
type LegacyRefusal struct{ Code, Detail, Audit string }

// CheckLegacyAgentRequest runs the old envelope's gates after ownership
// admission. The gates mirror architecture J.4 § "tetherd 侧硬拒条件" +
// § "安全约束": sha256 must be 64 lowercase hex chars (sanity; agent does the
// real verification post-download), the URL must start with one of the
// mandatory allowlist prefixes, and request proto_version must equal the
// broker's ProtoVersion. It always refuses: the old envelope grants neither an
// interrupting migration nor a fenced cut.
func CheckLegacyAgentRequest(body []byte, allow []string, db *sql.DB, sid, nid string) LegacyRefusal {
	var req proto.UpgradeReq
	if err := json.Unmarshal(body, &req); err != nil {
		return LegacyRefusal{Code: "json_parse", Detail: err.Error()}
	}
	// origin: upgrade-safety N-1 window — exact equality (architecture §21.1
	// site #2): a cross-epoch upgrade is the one permitted compatibility
	// break and goes through reinstall, never this verb. See §21.4.
	// Keyed `Code:` fields keep every emitted code visible to the error-code
	// scanner once this leaf is one of its roots (§12.2 item 3).
	if req.ProtoVersion != proto.ProtoVersion {
		return LegacyRefusal{Code: "proto_bump_requires_reinstall", Audit: "proto_bump_requires_reinstall",
			Detail: fmt.Sprintf("broker proto=%d, request proto=%d", proto.ProtoVersion, req.ProtoVersion)}
	}
	if !sha256HexRE.MatchString(strings.ToLower(req.SHA256)) {
		return LegacyRefusal{Code: "sha256_invalid", Detail: "expected 64 lowercase hex chars", Audit: "sha256_invalid"}
	}
	if !URLAllowed(req.URL, allow) {
		return LegacyRefusal{Code: "url_not_allowed", Detail: req.URL, Audit: "url_not_allowed"}
	}
	if CloneFamilyConflict(db, sid, nid) {
		return LegacyRefusal{Code: "clone_family_upgrade_unsupported", Audit: "clone_family_upgrade_unsupported",
			Detail: "remote upgrade is disabled for a credential family that has issued lease names; rebuild the shared image instead"}
	}
	// origin: hot-update S8 plan §7.2 / S8 internal review rv-n1 F1
	// The old envelope grants neither interrupting migration nor a fenced cut.
	// Refuse before sending even one byte to the agent's download/exec handler.
	code, detail := LegacyAgentRefusal(db, sid, nid)
	if code == "try_again" {
		return LegacyRefusal{Code: "try_again", Detail: detail, Audit: detail}
	}
	return LegacyRefusal{Code: proto.CodeBadRequest, Detail: detail, Audit: detail}
}

// The public broker entry points retain their signatures while target policy
// belongs to the agent route leaf, independently of broker cut orchestration.
// origin: hot-update S8 plan §12.1; S8 BROKERUPGRADE-SLIM
func LegacyAgentRefusal(db *sql.DB, sid, nid string) (string, string) {
	return agentroute.LegacyAgentRefusal(db, sid, nid)
}

func CloneFamilyConflict(db *sql.DB, sid, target string) bool {
	return agentroute.CloneFamilyConflict(db, sid, target)
}

func URLAllowed(url string, allow []string) bool {
	return agentroute.URLAllowed(url, allow)
}

func AgentTransactionCapable(db *sql.DB, sid, nid string) bool {
	return agentroute.AgentTransactionCapable(db, sid, nid)
}

func PlanAgentTransactionCapability(db *sql.DB, sid, nid string, capabilities []string) (*cluster.Command, error) {
	return agentroute.PlanAgentTransactionCapability(db, sid, nid, capabilities)
}

// AgentRoute preserves the broker entry's request type at the route owner.
type AgentRoute = agentroute.AgentRoute

// RouteAgentTransaction returns the reply for the actor. Every refusal happens
// before the forwarded publish: a legacy (non-advertising) agent is subject to
// the same busy64/defer75 policy as the old wire, and a capable agent receives
// only the strict request with the broker-stamped actor. The target agent then
// repeats key, actor and ownership checks against its own journal.
// The returned code is the reply's own code ("" for an answered success) and
// feeds only the audit row; the body is relayed byte-for-byte.
// origin: hot-update S8 plan §2.2 / §7.1 / §7.2 / §11.2 P15
func RouteAgentTransaction(r AgentRoute) ([]byte, string) {
	body, code, err := routeAgentTransaction(r)
	if err != nil {
		return encodeAgentResponse(upgradewire.ErrorResponse(err))
	}
	return body, code
}

// AgentTransactionReply renders an ownership/node admission refusal in the
// transaction envelope, or routes the admitted request. The admission code is
// already in the public registry; it is not remapped to an S8 code a new ctl
// would classify differently.
func AgentTransactionReply(admitted bool, denialCode, denialDetail string, r AgentRoute) ([]byte, string) {
	if !admitted {
		return encodeAgentResponse(upgradewire.Response{Code: denialCode, Error: denialDetail})
	}
	return RouteAgentTransaction(r)
}

// AuditAgentTransaction mirrors the old verb's audit rule: every admitted
// request and its ownership/node refusals are audited; malformed subjects and
// missing sessions carry no identity worth a row.
func AuditAgentTransaction(admitted bool, denialCode string) bool {
	return admitted || denialCode == "not_owner" || denialCode == "node_not_found" || denialCode == "node_offline"
}

func NATSRequest(nc *nats.Conn) func(string, []byte, time.Duration) ([]byte, error) {
	return agentroute.NATSRequest(nc)
}

func RememberAgentTransaction(read *sql.DB, write func(func(*sql.DB) (*cluster.Command, error)) error, sid, nid string, capabilities []string) error {
	return agentroute.RememberAgentTransaction(read, write, sid, nid, capabilities)
}

func routeAgentTransaction(r AgentRoute) ([]byte, string, error) {
	forward, err := admitAgentTransaction(r)
	if err != nil {
		return nil, "", err
	}
	return forward.relay(r, r.Timeout)
}

// agentForward is one admitted request: the forwarded subject and strict
// envelope, and whether a lost reply leaves no accepted effect (read).
// origin: S8 BROKER-EXPORTER R-9 (admission split from the relay)
type agentForward struct {
	subject  string
	envelope []byte
	read     bool
	query    bool
	roll     *rollcontrol.Request
}

// admitAgentTransaction runs every pre-send refusal: private inbox, strict
// decoding, session/actor binding and the latest capability record. It reads
// the store but sends nothing, so it stays on the caller's goroutine.
func admitAgentTransaction(r AgentRoute) (agentForward, error) {
	if rollcontrol.IsRequest(r.Body) {
		return admitAgentRoll(r)
	}
	// A full private View may answer this request. The shared `_INBOX` space is
	// readable by any connection; only the actor's own private root may get it.
	if r.Actor == "" || !strings.HasPrefix(r.Reply, auth.InboxPrefixFor(r.Actor)+".") {
		return agentForward{}, refusal(upgradetx.CodeRejected, "upgrade transactions require the actor's private reply inbox")
	}
	request, err := upgradewire.DecodeRequest(r.Body, upgradetx.ComponentAgent)
	if err != nil {
		return agentForward{}, err
	}
	actor := request.Prepare.ActorFP
	if request.Op == upgradewire.OpStart {
		actor = request.Grant.ActorFP
	}
	if request.Op == upgradewire.OpRollback {
		actor = request.Reverse.ActorFP
	}
	// The subject authenticates the session; a request for another session's
	// agent, or carrying another actor, never reaches the forwarded subject.
	if request.Key.SID != r.SID || actor != "" && actor != r.ActorFP {
		return agentForward{}, &upgradetx.Error{Code: proto.CodeNotOwner, Detail: "upgrade actor or target is not authorized"}
	}
	// Parent-owned Starts must still hold the original replicated reservation.
	// A prepared agent grant never creates permission to bypass cluster exclusion.
	// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §6.3
	if request.Op == upgradewire.OpStart && request.Grant.ParentTxID != "" {
		if err := agentroll.ParentHeld(r.DB, request.Grant.ParentTxID); err != nil {
			return agentForward{}, err
		}
	}
	// Capability is the agent's latest authenticated advertisement. Without it
	// the target is legacy and the shared policy refuses: busy is permanent,
	// an unfenced idle observation is deferred. A capable source's candidate
	// reader set (capable->legacy refusal) is proven by the agent's own
	// preflight against the actual artifact; the broker cannot read it.
	if !AgentTransactionCapable(r.DB, r.SID, r.NID) {
		code, detail := LegacyAgentRefusal(r.DB, r.SID, r.NID)
		if code == "try_again" {
			return agentForward{}, refusal(upgradetx.CodeDeferred, detail)
		}
		return agentForward{}, refusal(upgradetx.CodeRejected, detail)
	}
	envelope, err := upgradewire.EncodeForwarded(request, r.ActorFP)
	if err != nil {
		return agentForward{}, err
	}
	// A describe, like Prepare and Query, has no effect to settle.
	// origin: S8 FIX-DESCRIBE (P19 request R1)
	read := request.Op == upgradewire.OpPrepare || request.Op == upgradewire.OpQuery || request.Op == upgradewire.OpDescribe
	return agentForward{subject: proto.SubjCmdForwarded(r.SID, r.NID, SignedTransactionOp), envelope: envelope, read: read, query: request.Op == upgradewire.OpQuery}, nil
}

// relay sends the admitted envelope and classifies the agent's answer. It is
// the only part of a route that waits on the network, bounded by timeout.
func (f agentForward) relay(r AgentRoute, timeout time.Duration) ([]byte, string, error) {
	if timeout <= 0 {
		// The request's own deadline passed before it was sent: nothing reached
		// the agent, so even a mutation is a plain, retryable deferral.
		return nil, "", refusal(upgradetx.CodeDeferred, "agent transaction deadline expired before forwarding")
	}
	var body []byte
	var err error
	if f.query {
		body, err = requestAgentQuery(r, f.subject, f.envelope, timeout)
	} else {
		body, err = r.Request(f.subject, f.envelope, timeout)
	}
	if errors.Is(err, nats.ErrNoResponders) {
		return nil, "", refusal(upgradetx.CodeDeferred, "agent transaction route is not subscribed")
	}
	if err == nil && f.roll != nil {
		return agentroll.Response(rollRoute(r), *f.roll, body)
	}
	var response upgradewire.Response
	if err == nil {
		response, err = upgradewire.DecodeResponse(body)
	}
	if err != nil {
		// A lost Prepare/Query reply leaves no accepted effect: the same client
		// nonce returns the same inert grant. A lost mutation reply may follow an
		// accepted effect and must be queried, never retried as a new request.
		if f.read {
			return nil, "", refusal(upgradetx.CodeDeferred, "agent transaction reply is unavailable")
		}
		return nil, "", accepted(refusal(upgradetx.CodeUnsettled, "agent transaction result is unknown; query the original transaction"))
	}
	return body, response.Code, nil
}

// A query can reach the old subscriber after Start's accepted reply while its
// cut gate drops dispatch. Exec then loses that reply. Waiting the mutation
// forward timeout spends an inline transfer's entire original byte budget.
// Only this read may retry: retain the exact envelope and one absolute control
// deadline, with short attempts so the imported subscriber can answer promptly.
// Start/cancel/rollback remain sent-unknown after one send; typed refusals and
// malformed replies still return unchanged to the caller.
// origin: S7 internal review CONTINUATION-VICTIM F1 / S7 plan §1.2 / §7
func requestAgentQuery(r AgentRoute, subject string, envelope []byte, timeout time.Duration) ([]byte, error) {
	deadline := time.Now().Add(min(timeout, upgradebudget.RequestLatency))
	// Admission and executor scheduling consume this same query window.
	// A long mutation-forward timeout cannot renew time spent before relay.
	if !r.Received.IsZero() {
		if control := r.Received.Add(upgradebudget.RequestLatency); control.Before(deadline) {
			deadline = control
		}
	}
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nats.ErrTimeout
		}
		body, err := r.Request(subject, envelope, min(remaining, 500*time.Millisecond))
		if !errors.Is(err, nats.ErrTimeout) {
			return body, err
		}
	}
}

func encodeAgentResponse(response upgradewire.Response) ([]byte, string) {
	encoded, err := upgradewire.EncodeResponse(response)
	if err != nil {
		return []byte(`{"code":"` + upgradetx.CodeUnsettled + `"}`), upgradetx.CodeUnsettled
	}
	return encoded, response.Code
}

func WithControlProbe(nc *nats.Conn, heartbeat func(*nats.Msg)) func(*nats.Msg) {
	return agentroute.WithControlProbe(nc, heartbeat)
}

func RelayControlProbe(nc *nats.Conn, msg *nats.Msg) {
	agentroute.RelayControlProbe(nc, msg)
}
