package upgradecli

import (
	"context"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/spf13/cobra"
)

// BrokerPorts are the cmd-owned edges of the local broker transaction verbs.
// Call is one admin-socket round trip under the caller's deadline; the socket's
// Unix permissions are the authorization, so no NATS identity is involved.
// origin: hot-update S8 plan §5.2 / §7.1 / §11.2 P19
type BrokerPorts struct {
	Call        func(ctx context.Context, socket string, req adminsock.Request) (*adminsock.Response, error)
	Exit        func(verb string, err error) error
	DefaultHome string
	// Describe answers upgrade.describe for the local daemon, including the
	// candidate staged at its registered canonical path (DescribeBroker).
	// origin: S8 FIX-DESCRIBE (P19 request R1)
	Describe func(ctx context.Context, socket string) (Target, *upgradeport.CandidateDescription, error)
	// Actor names who prepared a grant over the admin socket: the local ctl
	// identity's fingerprint. The socket's Unix permissions stay the
	// authorization; the name only attributes the recorded grant.
	Actor  func(home string) (string, error)
	Socket func(cmd *cobra.Command) string
}

// IdentityActor adapts the ctl's home identity loader to BrokerPorts.Actor:
// the grant is attributed to the local identity's fingerprint, and a missing
// or unreadable identity refuses before anything is sent.
// origin: S8 FIX-DESCRIBE (P19 request R1)
func IdentityActor[I any](load func(home string) (I, error), fingerprint func(I) string) func(string) (string, error) {
	return func(home string) (string, error) {
		id, err := load(home)
		if err != nil {
			return "", err
		}
		return fingerprint(id), nil
	}
}

// DescribeBroker is the production broker describer: the local daemon answers
// upgrade.describe over the same admin socket as every other broker verb.
// origin: S8 FIX-DESCRIBE (P19 request R1)
func DescribeBroker(p BrokerPorts) func(context.Context, string) (Target, *upgradeport.CandidateDescription, error) {
	return func(ctx context.Context, socket string) (Target, *upgradeport.CandidateDescription, error) {
		return AdminEndpoint(p, socket).DescribeTarget(ctx, upgradetx.Key{Component: upgradetx.ComponentBroker})
	}
}

// AdminEndpoint carries the frozen envelope inside the admin request. Every
// transport failure after the dial is treated as possibly delivered; the
// admin dispatcher answers fenced intake with a typed deferral instead.
func AdminEndpoint(p BrokerPorts, socket string) Remote {
	return Remote{Exchange: func(ctx context.Context, request upgradewire.Request) (upgradewire.Response, error) {
		if p.Call == nil {
			return upgradewire.Response{}, &NotSentError{Err: fmt.Errorf("admin transport is unavailable")}
		}
		resp, err := p.Call(ctx, socket, adminsock.Request{Op: request.Op, Upgrade: &request})
		if err != nil {
			return upgradewire.Response{}, err
		}
		if resp.Upgrade == nil {
			// A legacy-shaped reply means the daemon does not dispatch the
			// transaction envelope at all: nothing was accepted.
			return upgradewire.Response{Code: upgradetx.CodeDeferred, Error: "broker does not serve upgrade transactions"}, nil
		}
		return *resp.Upgrade, nil
	}}
}

func brokerBinding(key upgradetx.Key) error {
	if key.Component != upgradetx.ComponentBroker {
		return rejected("transaction does not belong to a broker")
	}
	return nil
}

// BrokerTransactionCommands returns `broker upgrade wait|rollback|cancel|archive`.
// `status --tx-id` and `reload --tx-id/--prepare-only` extend the existing verbs.
func BrokerTransactionCommands(p BrokerPorts) []*cobra.Command {
	short := map[string]string{
		VerbWait:     "Wait for one broker upgrade transaction to settle (unknown or timeout is 74)",
		VerbRollback: "Reverse one broker upgrade transaction from its current state",
		VerbCancel:   "Cancel one broker upgrade transaction that has no activation effect",
		VerbArchive:  "Archive one broker upgrade transaction whose owner the broker proved dead",
	}
	var out []*cobra.Command
	for _, verb := range []string{VerbWait, VerbRollback, VerbCancel, VerbArchive} {
		var home, txID string
		var timeout time.Duration
		var asJSON bool
		cmd := &cobra.Command{Use: verb, Short: short[verb], Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				return p.Exit("broker upgrade "+verb, RunBrokerVerb(cmd, p, verb, home, txID, timeout, asJSON))
			}}
		cmd.Flags().StringVar(&txID, "tx-id", "", "the original target-issued transaction ID (required)")
		cmd.Flags().StringVar(&home, "home", p.DefaultHome, "tether home dir (holds the grants this ctl prepared)")
		cmd.Flags().DurationVar(&timeout, "timeout", 2*time.Minute, "absolute wait budget, including transport time")
		cmd.Flags().BoolVar(&asJSON, "json", false, "emit the private transaction View as JSON")
		_ = cmd.MarkFlagRequired("tx-id")
		out = append(out, cmd)
	}
	return out
}

// RunBrokerVerb executes one --tx-id verb over the local admin socket.
func RunBrokerVerb(cmd *cobra.Command, p BrokerPorts, verb, home, txID string, timeout time.Duration, asJSON bool) error {
	ctx := cmd.Context()
	if verb == VerbStatus {
		// A status read has the admin request budget, not a wait budget.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return RunVerb(ctx, VerbRequest{Verb: verb, Home: home, TxID: txID, Bind: brokerBinding,
		Endpoint: AdminEndpoint(p, p.Socket(cmd)), Timeout: timeout, JSON: asJSON, Out: cmd.OutOrStdout()})
}

// RunBrokerReload is the S8 half of `broker upgrade reload`: prepare-only
// keeps the existing --to-version/--expect-sha256 first-call contract; a resume
// reads version and SHA from the original grant and accepts nothing else.
// origin: hot-update S8 plan §5.2 / §7.1 / §11.2 P19
func RunBrokerReload(cmd *cobra.Command, p BrokerPorts, home string, opts UpgradeOptions, timeout time.Duration) error {
	if err := ValidateUpgrade(upgradetx.ComponentBroker, opts); err != nil {
		return p.Exit("broker upgrade reload", err)
	}
	ctx, out, socket := cmd.Context(), cmd.OutOrStdout(), p.Socket(cmd)
	endpoint := AdminEndpoint(p, socket)
	switch {
	case opts.PrepareOnly:
		grant, err := brokerPrepare(ctx, p, socket, home, opts, endpoint)
		if err == nil {
			_, err = fmt.Fprintf(out, "prepared broker: tx_id=%s source=%s target=%s grant=%s\n",
				grant.Handle.TxID, grant.SourceSHA, grant.TargetSHA, opts.GrantOut)
		}
		return p.Exit("broker upgrade reload --prepare-only", err)
	case opts.ArtifactReceipt != "":
		receipt, err := ReadArtifactReceipt(opts.ArtifactReceipt)
		if err == nil {
			err = brokerBinding(receipt.Grant.Handle.Key)
		}
		if err == nil {
			var view upgradetx.View
			view, err = Resume(ctx, endpoint, home, opts.TxID, receipt)
			err = renderOutcome(out, view, false, err)
		}
		return p.Exit("broker upgrade reload --tx-id", err)
	default:
		return p.Exit("broker upgrade reload --tx-id", RunVerb(ctx, VerbRequest{Verb: VerbWait, Home: home, TxID: opts.TxID,
			Bind: brokerBinding, Endpoint: endpoint, Timeout: timeout, Out: out}))
	}
}

// brokerPrepare needs two facts only the broker knows: its registered Key,
// running source image and original scope, and the reader declarations of the
// candidate (the frozen first call names only version and SHA, never a local
// file). The broker describes both — the candidate is the one staged at its
// registered canonical path, the S6 staging precondition — and the operator's
// --to-version/--expect-sha256 must name exactly those bytes. Every gap is a
// pre-acceptance refusal, never a grant built from guesses.
// origin: hot-update S8 plan §5.2 / §11.2 P19 (request R1); S8 FIX-DESCRIBE
func brokerPrepare(ctx context.Context, p BrokerPorts, socket, home string, opts UpgradeOptions, endpoint Remote) (upgradetx.Grant, error) {
	if p.Describe == nil || p.Actor == nil {
		return upgradetx.Grant{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: upgradewire.DescriptionUnavailable + ": the broker description port is unavailable"}
	}
	target, staged, err := p.Describe(ctx, socket)
	if err != nil {
		return upgradetx.Grant{}, err
	}
	if err := brokerBinding(target.Key); err != nil {
		return upgradetx.Grant{}, err
	}
	if staged == nil {
		return upgradetx.Grant{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "candidate_declarations_unavailable: no candidate is staged at the broker's canonical path"}
	}
	if staged.SHA != opts.ExpectSHA256 || staged.Release != opts.ToVersion {
		return upgradetx.Grant{}, rejected("the staged candidate does not match --to-version/--expect-sha256")
	}
	actor, err := p.Actor(home)
	if err != nil {
		return upgradetx.Grant{}, err
	}
	req, err := BuildPrepare(target, Candidate{SHA: staged.SHA, Release: staged.Release, CapabilityDigest: staged.CapabilityDigest}, actor, time.Now())
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
