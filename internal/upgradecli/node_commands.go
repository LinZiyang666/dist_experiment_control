package upgradecli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// NodeSession is one authenticated ctl connection supplied by cmd. Request is
// a single NATS request from the actor's private inbox (the broker refuses any
// other reply root for upgrade-tx); it returns *NotSentError when nothing
// received the request. Close releases the connection.
type NodeSession struct {
	SID, Actor, ActorFP string
	Request             func(ctx context.Context, subject string, body []byte) ([]byte, error)
	Close               func()
}

// NodePorts are the cmd-owned edges of the node transaction commands: the
// private-inbox connection, exit classification and the target description.
// Parsing of the shared --home/--nats-url flags stays with the command tree.
// origin: hot-update S8 plan §7.1 / §11.2 P19
type NodePorts struct {
	Open        func(cmd *cobra.Command, verb, home, natsURL string) (*NodeSession, error)
	Exit        func(verb string, err error) error
	DefaultHome string
	Describe    func(ctx context.Context, s *NodeSession, nid string) (Target, error)
	Fetch       func(string, time.Duration) ([]byte, error)
	Probe       ProbeImage
}

// RequestFunc is one ctl NATS request/reply under the caller's context (an
// alias of the legacy leaf's identical port).
type RequestFunc = func(ctx context.Context, subject string, body []byte) ([]byte, error)

// NewNodeSession wraps one ctl request port. A no-responders reply proves
// the request reached no subscriber, which is the only transport failure a
// mutation may report as not sent.
func NewNodeSession(sid, actor, actorFP string, request RequestFunc, closeFn func()) *NodeSession {
	return &NodeSession{SID: sid, Actor: actor, ActorFP: actorFP, Close: closeFn,
		Request: func(ctx context.Context, subject string, body []byte) ([]byte, error) {
			reply, err := request(ctx, subject, body)
			if errors.Is(err, nats.ErrNoResponders) {
				return nil, &NotSentError{Err: err}
			}
			return reply, err
		}}
}

// DescribeNode is the production node describer. The target agent answers on
// the same authenticated upgrade-tx route as every other S8 operation: the
// broker admits only the session owner and the agent's latest register must
// have advertised the transaction capability, so a legacy or unregistered
// agent refuses here, before the ctl downloads any candidate.
// origin: hot-update S8 plan §2.1 / §7.1; S8 FIX-DESCRIBE (P19 request R1)
func DescribeNode(ctx context.Context, s *NodeSession, nid string) (Target, error) {
	selector := upgradetx.Key{Component: upgradetx.ComponentAgent, SID: s.SID, ConfiguredNID: nid}
	target, _, err := NodeEndpoint(s, nid, upgradebudget.RequestLatency).DescribeTarget(ctx, selector)
	return target, err
}

// NodeEndpoint routes the frozen envelope to the agent's upgrade-tx verb
// through the broker. The broker applies ownership and the legacy policy
// before any byte reaches the agent; the agent re-checks key and actor.
func NodeEndpoint(s *NodeSession, nid string, timeout time.Duration) Remote {
	subject := proto.SubjCmdBy(s.SID, s.Actor, nid, "upgrade-tx")
	return Remote{Exchange: func(ctx context.Context, request upgradewire.Request) (upgradewire.Response, error) {
		body, err := upgradewire.EncodeRequest(request)
		if err != nil {
			return upgradewire.Response{}, &NotSentError{Err: err}
		}
		rctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		reply, err := s.Request(rctx, subject, body)
		if err != nil {
			return upgradewire.Response{}, err
		}
		return upgradewire.DecodeResponse(reply)
	}}
}

// NodeTransactionCommands returns the independent sibling verbs of
// `node upgrade`. Each takes the NID positionally and the original TxID.
func NodeTransactionCommands(p NodePorts) []*cobra.Command {
	short := map[string]string{
		VerbStatus:   "Query one upgrade transaction with a fresh observation (rc0 even when unknown)",
		VerbWait:     "Wait for one upgrade transaction to settle (unknown or timeout is 74)",
		VerbRollback: "Reverse one upgrade transaction from its current state",
		VerbCancel:   "Cancel one upgrade transaction that has no activation effect",
		VerbArchive:  "Archive one upgrade transaction whose owner the target proved dead",
	}
	var out []*cobra.Command
	for _, verb := range []string{VerbStatus, VerbWait, VerbRollback, VerbCancel, VerbArchive} {
		out = append(out, nodeVerbCommand(p, verb, short[verb]))
	}
	return out
}

func nodeVerbCommand(p NodePorts, verb, short string) *cobra.Command {
	var home, natsURL, txID string
	var timeout time.Duration
	var asJSON bool
	cmd := &cobra.Command{Use: "upgrade-" + verb + " <nid>", Short: short, Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := p.Open(cmd, "upgrade-"+verb, home, natsURL)
			if err != nil {
				return err
			}
			defer s.Close()
			nid := args[0]
			err = RunVerb(cmd.Context(), VerbRequest{Verb: verb, Home: home, TxID: txID, ActorFP: s.ActorFP,
				Bind: nodeBinding(s.SID, nid), Endpoint: NodeEndpoint(s, nid, timeout), Timeout: timeout, JSON: asJSON, Out: cmd.OutOrStdout()})
			return p.Exit("node upgrade-"+verb, err)
		}}
	cmd.Flags().StringVar(&txID, "tx-id", "", "the original target-issued transaction ID (required)")
	cmd.Flags().StringVar(&home, "home", p.DefaultHome, "tether home dir (holds the grants this ctl prepared)")
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().DurationVar(&timeout, "timeout", 2*time.Minute, "absolute wait budget, including transport time")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the private transaction View as JSON")
	_ = cmd.MarkFlagRequired("tx-id")
	return cmd
}

// nodeBinding refuses a recalled grant for another session or node: the
// positional NID is the operator's statement of intent and must match.
func nodeBinding(sid, nid string) func(upgradetx.Key) error {
	return func(key upgradetx.Key) error {
		if key.Component != upgradetx.ComponentAgent || key.SID != sid || key.ConfiguredNID != nid {
			return rejected("transaction belongs to another session, node or role")
		}
		return nil
	}
}

// RunNodeUpgrade is the S8 half of `node upgrade <nid>`: prepare-only,
// resume with an artifact receipt, resume-wait, or the explicit interrupting
// first migration. The default --url/--sha256 form stays on the legacy route.
// origin: hot-update S8 plan §5.2 / §7.1 / §7.2 / §11.2 P19
func RunNodeUpgrade(cmd *cobra.Command, p NodePorts, nid, home, natsURL string, opts UpgradeOptions, timeout time.Duration) error {
	if err := ValidateUpgrade(upgradetx.ComponentAgent, opts); err != nil {
		return p.Exit("node upgrade", err)
	}
	s, err := p.Open(cmd, "upgrade", home, natsURL)
	if err != nil {
		return err
	}
	defer s.Close()
	ctx, out := cmd.Context(), cmd.OutOrStdout()
	endpoint := NodeEndpoint(s, nid, timeout)
	switch {
	case opts.Interrupting:
		// The verb carries the label: a legacy refusal's hint replaces its detail.
		return p.Exit("node upgrade --interrupting [legacy_interrupting non_hot]", interrupting(ctx, s, nid, opts, timeout, out))
	case opts.PrepareOnly:
		grant, err := prepareNode(ctx, p, s, nid, home, opts, endpoint)
		if err == nil {
			_, err = fmt.Fprintf(out, "prepared %s/%s: tx_id=%s source=%s target=%s grant=%s\n",
				s.SID, nid, grant.Handle.TxID, grant.SourceSHA, grant.TargetSHA, opts.GrantOut)
		}
		return p.Exit("node upgrade --prepare-only", err)
	case opts.ArtifactReceipt != "":
		receipt, err := ReadArtifactReceipt(opts.ArtifactReceipt)
		if err == nil {
			err = nodeBinding(s.SID, nid)(receipt.Grant.Handle.Key)
		}
		if err == nil {
			var view upgradetx.View
			view, err = Resume(ctx, endpoint, home, opts.TxID, receipt)
			err = renderOutcome(out, view, false, err)
		}
		return p.Exit("node upgrade --tx-id", err)
	default:
		// --tx-id alone resumes observation of the original only; it can
		// never reissue Start, so a lost reply is settled by waiting.
		err = RunVerb(ctx, VerbRequest{Verb: VerbWait, Home: home, TxID: opts.TxID, Bind: nodeBinding(s.SID, nid),
			Endpoint: endpoint, Timeout: timeout, Out: out})
		return p.Exit("node upgrade --tx-id", err)
	}
}

func prepareNode(ctx context.Context, p NodePorts, s *NodeSession, nid, home string, opts UpgradeOptions, endpoint Remote) (upgradetx.Grant, error) {
	describe := p.Describe
	if describe == nil {
		return upgradetx.Grant{}, &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "target description port is unavailable"}
	}
	// Describe first: an undescribable target refuses before any download.
	target, err := describe(ctx, s, nid)
	if err != nil {
		return upgradetx.Grant{}, err
	}
	if err := nodeBinding(s.SID, nid)(target.Key); err != nil {
		return upgradetx.Grant{}, err
	}
	candidate, err := FetchCandidate(upgradetx.ComponentAgent, opts.URL, opts.SHA256, p.Fetch, p.Probe)
	if err != nil {
		return upgradetx.Grant{}, err
	}
	defer func() { _ = candidate.Close() }()
	req, err := BuildPrepare(target, candidate, s.ActorFP, time.Now())
	if err != nil {
		return upgradetx.Grant{}, err
	}
	grant, err := endpoint.Prepare(ctx, target.Key, req)
	if err != nil {
		return upgradetx.Grant{}, err
	}
	if err := RememberGrant(home, grant); err != nil {
		return grant, err
	}
	return grant, WriteGrant(opts.GrantOut, grant)
}

// interrupting sends the explicit first-migration request on the existing
// node upgrade subject — the same bytes an N-1 ctl sends, so a new ctl gains
// no route an old one lacked. The broker's shared legacy policy decides; any
// accepted dispatch is reported non_hot and unsettled, never hot success.
// origin: hot-update S8 plan §7.1 / §7.2 / §11.2 P19
func interrupting(ctx context.Context, s *NodeSession, nid string, opts UpgradeOptions, timeout time.Duration, out interface{ Write([]byte) (int, error) }) error {
	body, _ := json.Marshal(proto.UpgradeReq{URL: opts.URL, SHA256: opts.SHA256, ProtoVersion: proto.ProtoVersion})
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	reply, err := s.Request(rctx, proto.SubjCmdBy(s.SID, s.Actor, nid, "upgrade"), body)
	if err != nil {
		var notSent *NotSentError
		if errors.As(err, &notSent) {
			return &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "legacy_interrupting non_hot: no upgrade route answered"}
		}
		return LegacyInterruptingDispatched()
	}
	var resp proto.UpgradeResp
	if err := json.Unmarshal(reply, &resp); err != nil {
		return LegacyInterruptingDispatched()
	}
	if !resp.OK {
		return &upgradetx.Error{Code: resp.Code, Detail: "legacy_interrupting non_hot refused: " + resp.Error}
	}
	_, _ = fmt.Fprintf(out, "legacy_interrupting non_hot: %s/%s dispatched; this is not a hot upgrade\n", s.SID, nid)
	return LegacyInterruptingDispatched()
}
