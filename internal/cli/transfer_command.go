package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferclient"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// TransferCommandError keeps presentation independent of cmd/tether's exit
// wrapper while preserving the leaf's class and stable machine code.
// origin: hot-update S7 plan §7 / T34 / INTEG-D
type TransferCommandError struct {
	Class int
	Code  string
	Err   error
}

func (e *TransferCommandError) Error() string { return e.Err.Error() }
func (e *TransferCommandError) Unwrap() error { return e.Err }

const (
	exitUsage       = 64
	exitUnavailable = 69
	exitInternal    = 70
	exitUnsettled   = 74
	exitTransient   = 75
	exitNoPerm      = 77
)

type RemoteTransferSpec struct{ Node, Path string }

func usageErr(format string, args ...any) error {
	return &TransferCommandError{Class: exitUsage, Err: fmt.Errorf(format, args...)}
}
func transferRefusalErr(code, format string, args ...any) error {
	return &TransferCommandError{Class: xferclient.ExitCode(&xferclient.Error{Code: code}), Code: code, Err: fmt.Errorf(format, args...)}
}
func NewTransferID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

type TransferCommandPorts struct {
	Client   xferclient.Factory
	Select   func(*xferclient.Config, controlbudget.Deadline) (bool, error)
	Open     func(*cobra.Command, string, string, string) (xferclient.Config, func() error, error)
	Receiver func(string, bool) (xfercontract.AtomicPort, error)
	Classify func(error) (string, int)
}

func transferCompositionError() error {
	return errors.New("transfer CLI: required factory is unavailable")
}

// NewTransferStatusCommand adds only the frozen read-only query. It cannot prepare,
// acquire a permit, reopen an object or mint a new business transfer ID.
// origin: hot-update S7 plan §7 / T34
func NewTransferStatusCommand(ports TransferCommandPorts) *cobra.Command {
	root := &cobra.Command{Use: "transfer", Short: "Inspect transfer continuation state"}
	var home, natsURL, node string
	var machine bool
	status := &cobra.Command{
		Use: "status <transfer-id>", Short: "Query the original owner's transfer state (read-only)",
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.ExactArgs(1)(cmd, args); err != nil {
				return usageErr("transfer status: %v", err)
			}
			if !validTransferToken(args[0]) || !validTransferToken(node) {
				return usageErr("transfer status requires a transfer ID and --node without subject separators")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) (resultErr error) {
			sid := ReadCurrentSession(home)
			if sid == "" {
				return usageErr("no active session — run `tether login -s <sid>` first")
			}
			if ports.Open == nil || ports.Classify == nil {
				return transferCompositionError()
			}
			cfg, closeOwner, err := ports.Open(cmd, home, natsURL, sid)
			if err != nil {
				return TransferLeafError(ports, err)
			}
			defer finishTransferCleanup(&resultErr, closeOwner)
			deadline, err := controlbudget.After(5 * time.Second)
			if err != nil {
				return err
			}
			cfg.Identity.SID, cfg.Identity.NID, cfg.Identity.TransferID = sid, node, args[0]
			resp, _, err := cfg.Transport.Request(xfercontract.Request{
				Op: xfercontract.OpStatus, CallID: NewTransferID(), Identity: cfg.Identity,
			}, deadline)
			if err != nil {
				return TransferLeafError(ports, err)
			}
			if resp.Code != "" {
				return transferRefusalErr(resp.Code, "transfer status refused: code=%s %s", resp.Code, resp.Error)
			}
			if resp.Status == nil {
				return &TransferCommandError{Class: exitUnsettled, Err: errors.New("transfer status: missing authoritative response")}
			}
			if resp.Status.Identity.SID != sid || resp.Status.Identity.NID != node || resp.Status.Identity.TransferID != args[0] {
				return &TransferCommandError{Class: exitUnsettled, Err: errors.New("transfer status: response identity mismatch")}
			}
			return RenderTransferStatus(cmd, *resp.Status, machine)
		},
	}
	status.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return usageErr("transfer status flags: %v", err) })
	status.Flags().StringVar(&home, "home", DefaultHome(), "tether home dir")
	status.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	status.Flags().StringVar(&node, "node", "", "node that owns the transfer (required)")
	status.Flags().BoolVar(&machine, "json", false, "emit transfer state as JSON")
	root.AddCommand(status)
	return root
}

// Subject tokens are checked before loading identity or issuing a query; an
// embedded wildcard must not broaden the authenticated node/transfer scope.
// origin: hot-update S7 plan §6.2 / T34
func validTransferToken(token string) bool {
	return token != "" && !strings.ContainsAny(token, ".*>/\\ \t\r\n\x00")
}

// RunTransferCommand owns parameter/path presentation and exactly one business
// identity. Probe, permit, byte work, reconciliation and legacy selection belong
// to the leaf invocation, so no retry closure in this package can mint another ID.
// origin: hot-update S7 plan §7 / T34
func RunTransferCommand(cmd *cobra.Command, ports TransferCommandPorts, home, natsURL, verb, localPath string, spec RemoteTransferSpec, force bool, timeout time.Duration) (resultErr error) {
	rate, err := cmd.Flags().GetInt64("rate-limit")
	if err != nil || rate < 0 {
		return usageErr("--rate-limit must be a nonnegative integer number of bytes per second")
	}
	if timeout <= 0 || !validTransferToken(spec.Node) {
		return usageErr("transfer requires a valid node and a positive --timeout")
	}
	sid := ReadCurrentSession(home)
	if sid == "" {
		return usageErr("no active session — run `tether login -s <sid>` first")
	}
	if ports.Client == nil || ports.Open == nil || ports.Classify == nil || (verb == "pull" && ports.Receiver == nil) {
		return transferCompositionError()
	}
	localAbs, err := filepath.Abs(localPath)
	if err != nil {
		return usageErr("local path: %v", err)
	}
	// The provisional pull deadline and the explicit ctl bound share one local
	// clock. The engine seals and tightens its rate/size budget from original start.
	deadline, err := controlbudget.After(min(timeout, proto.XferTierBMaxBudget))
	if err != nil {
		return err
	}
	cfg, closeOwner, err := ports.Open(cmd, home, natsURL, sid)
	if err != nil {
		return TransferLeafError(ports, err)
	}
	defer finishTransferCleanup(&resultErr, closeOwner)
	cfg.Identity.SID, cfg.Identity.NID, cfg.Identity.TransferID = sid, spec.Node, NewTransferID()
	cfg.Intent = xfercontract.Intent{Verb: verb, Path: spec.Path, Force: force, RateLimitBPS: rate}
	// The command owns the pin through negotiation and legacy execution; a
	// constructed v1 invocation takes custody and retires it through Stop.
	// origin: hot-update S10 plan §0 / LINT-L2 errcheck
	sourceTransferred := false
	if verb == "push" {
		cfg.Source, err = os.Open(localAbs)
		if err != nil {
			return usageErr("local source: %v", err)
		}
		defer func() {
			if !sourceTransferred {
				finishTransferCleanup(&resultErr, cfg.Source.Close)
			}
		}()
		st, err := cfg.Source.Stat()
		if err != nil || !st.Mode().IsRegular() || st.Size() > proto.XferMaxBytes {
			return usageErr("local source must be a regular file of at most %d bytes", proto.XferMaxBytes)
		}
		cfg.Intent.Size = st.Size()
	} else {
		cfg.Destination, err = ports.Receiver(localAbs, force)
		if err != nil {
			return TransferLeafError(ports, err)
		}
		defer finishTransferCleanup(&resultErr, func() error { return stopTransferOwner(cfg.Destination) })
	}
	if ports.Select != nil {
		legacy, selectErr := ports.Select(&cfg, deadline)
		if selectErr != nil {
			return TransferLeafError(ports, selectErr)
		}
		if legacy {
			if verb == "push" {
				if f := cmd.Flags().Lookup("timeout"); f == nil || !f.Changed {
					if bound, boundErr := controlbudget.After(LegacyPhaseTimeout(false, cfg.Intent.Size, timeout)); boundErr == nil {
						deadline = min(deadline, bound)
					}
				}
			}
			return runLegacyTransferCommand(cmd, cfg, localAbs, spec, force, deadline)
		}
	}
	invocation, err := ports.Client(cfg)
	if err != nil {
		return TransferLeafError(ports, err)
	}
	sourceTransferred = cfg.Source != nil
	defer finishTransferCleanup(&resultErr, func() error { return stopTransferOwner(invocation) })
	// origin: hot-update S7 plan §7 / T34. The command context expresses
	// cancellation intent, while the leaf retains the original outcome/grant.
	canceled := make(chan struct{})
	stopCancel := context.AfterFunc(cmd.Context(), func() {
		defer close(canceled)
		cancelDeadline, deadlineErr := controlbudget.After(5 * time.Second)
		if deadlineErr == nil {
			_, _ = invocation.Cancel(xfercontract.CancelUser, cancelDeadline)
		}
	})
	defer func() {
		if !stopCancel() {
			<-canceled
		}
	}()
	receipt, err := invocation.Run(deadline)
	if err != nil {
		return fmt.Errorf("transfer %s: %w; query `tether transfer status %s --node %s`",
			cfg.Identity.TransferID, TransferLeafError(ports, err), cfg.Identity.TransferID, spec.Node)
	}
	if !ValidTransferReceipt(receipt) {
		return &TransferCommandError{Class: exitUnsettled, Err: fmt.Errorf("transfer %s: result unresolved; query `tether transfer status %s --node %s`", cfg.Identity.TransferID, cfg.Identity.TransferID, spec.Node)}
	}
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "transfer_id=%s kind=%s bytes=%d sha256=%s outcome_unknown=%t\n", cfg.Identity.TransferID, receipt.Kind, receipt.Bytes, receipt.SHA256, receipt.OutcomeUnknown); err != nil {
		return err
	}
	if receipt.OutcomeUnknown {
		return transferRefusalErr(xfercontract.CodeResultUnknown, "transfer %s: outcome unknown (code=%s)", cfg.Identity.TransferID, receipt.Code)
	}
	if receipt.Kind == "failed" {
		return transferRefusalErr(receipt.Code, "transfer %s failed: code=%s", cfg.Identity.TransferID, receipt.Code)
	}
	return nil
}

// TransferLeafError forwards the engine's phase-aware classifier. An arbitrary
// error or an unknown class cannot be laundered into successful completion.
// origin: hot-update S7 plan §7 / T34
func TransferLeafError(ports TransferCommandPorts, err error) error {
	if ports.Classify == nil {
		return &TransferCommandError{Class: exitInternal, Err: err}
	}
	code, class := ports.Classify(err)
	if class != exitUsage && class != exitUnavailable && class != exitInternal && class != exitUnsettled && class != exitTransient && class != exitNoPerm {
		class = exitUnsettled
	}
	return &TransferCommandError{Class: class, Code: code, Err: err}
}

// LegacyPhaseTimeout is the bound an explicitly negotiated cap0 push of
// `size` bytes gets. Without an explicit --timeout it mirrors the cap0
// broker's own budget for THIS transfer (both legs at the declared size,
// floor included) plus a two-minute margin, so the ctl is never tighter than
// the broker on any size and a stalled 12 MB upload no longer waits out the
// 2 GiB worst case (drill 67 measured 37 minutes). An explicit --timeout wins.
// A capable transfer instead seals its own size/rate budget in the engine.
// origin: simcluster-speed 0a / internal review round 1 R4-F6; hot-update S7 plan §9 / P31
func LegacyPhaseTimeout(explicit bool, size int64, flagValue time.Duration) time.Duration {
	if explicit {
		return flagValue
	}
	return proto.XferBudget("b", size, proto.XferPushLegs) + 2*time.Minute
}

// History proof is the presentation floor, not a substitute for the leaf's
// exact receipt comparison. Outcome-unknown can never authorize complete.
// origin: hot-update S7 plan §6.1 / T34
func ValidTransferReceipt(receipt xfercontract.TerminalReceipt) bool {
	return receipt.TerminalID != "" && receipt.CanonicalDigest != "" && receipt.ReqID != "" &&
		receipt.HistoryStream != "" && receipt.HistorySeq > 0 &&
		(receipt.Kind == "failed" || (receipt.Kind == "complete" && !receipt.OutcomeUnknown))
}

// transferStatusView is a presentation schema, deliberately without settlement
// capsules. The neutral leaf state has no JSON tags on Status; spelling this
// view keeps the CLI's machine schema stable independently of private state.
// origin: hot-update S7 plan §6.1 / T34
// Identity, Origin and Budget retain the frozen tagged P00 value types.
type transferStatusView struct {
	State                 xfercontract.StatusState      `json:"state"`
	Phase                 xfercontract.Phase            `json:"phase"`
	Identity              xfercontract.Identity         `json:"identity"`
	Origin                xfercontract.Origin           `json:"origin"`
	Budget                xfercontract.BudgetSeal       `json:"budget"`
	DurableOffset         int64                         `json:"durable_offset"`
	ChunkIndex            uint32                        `json:"chunk_index"`
	LastConsumedStreamSeq uint64                        `json:"last_consumed_stream_seq"`
	Terminal              *xfercontract.TerminalReceipt `json:"terminal,omitempty"`
	CleanupPending        bool                          `json:"cleanup_pending"`
	Code                  string                        `json:"code,omitempty"`
	Error                 string                        `json:"error,omitempty"`
}

func RenderTransferStatus(cmd *cobra.Command, status xfercontract.Status, machine bool) error {
	// origin: hot-update S7 plan §6.1 / T34
	// A missing state is a lost answer, never authoritative absence.
	if status.State != xfercontract.StatusUnknown && status.State != xfercontract.StatusLive && status.State != xfercontract.StatusTerminal {
		return &TransferCommandError{Class: exitUnsettled, Err: errors.New("transfer status: invalid or missing state")}
	}
	if (status.State == xfercontract.StatusTerminal && (status.Terminal == nil || !ValidTransferReceipt(*status.Terminal))) ||
		(status.State != xfercontract.StatusTerminal && status.Terminal != nil) {
		return &TransferCommandError{Class: exitUnsettled, Err: errors.New("transfer status: inconsistent terminal proof")}
	}
	view := transferStatusView{State: status.State, Phase: status.Phase, Identity: status.Identity, Origin: status.Origin,
		Budget: status.Budget, DurableOffset: status.DurableOffset, ChunkIndex: status.ChunkIndex,
		LastConsumedStreamSeq: status.LastConsumedStreamSeq, Terminal: status.Terminal, CleanupPending: status.CleanupPending,
		Code: status.Code, Error: status.Error}
	if machine {
		return json.NewEncoder(cmd.OutOrStdout()).Encode(view)
	}
	_, err := fmt.Fprintf(cmd.OutOrStdout(), "transfer_id=%s node=%s state=%s phase=%s durable_offset=%d chunk_index=%d cleanup_pending=%t",
		status.Identity.TransferID, status.Identity.NID, status.State, status.Phase, status.DurableOffset, status.ChunkIndex, status.CleanupPending)
	if err == nil && status.Terminal != nil {
		_, err = fmt.Fprintf(cmd.OutOrStdout(), " kind=%s code=%s outcome_unknown=%t", status.Terminal.Kind, status.Terminal.Code, status.Terminal.OutcomeUnknown)
	}
	if err == nil {
		_, err = fmt.Fprintln(cmd.OutOrStdout())
	}
	return err
}

// The explicit legacy branch retains its separate result type through stdout.
// origin: hot-update S7 plan §7 / §9 / INTEG-D
func runLegacyTransferCommand(cmd *cobra.Command, cfg xferclient.Config, local string, _ RemoteTransferSpec, _ bool, d controlbudget.Deadline) (resultErr error) {
	if cfg.Destination != nil {
		if err := cfg.Destination.Stop(d); err != nil {
			return err
		}
	}
	owner, err := NewLegacyTransfer(cfg, local)
	if err != nil {
		return TransferLeafError(TransferCommandPorts{Classify: func(e error) (string, int) { return xferclient.ErrorCode(e), xferclient.ExitCode(e) }}, err)
	}
	defer finishTransferCleanup(&resultErr, func() error { return StopLegacyTransfer(owner) })
	result, err := owner.Run(d)
	if err != nil {
		return &TransferCommandError{Class: xferclient.ExitCode(err), Code: xferclient.ErrorCode(err), Err: err}
	}
	_, err = fmt.Fprintf(cmd.OutOrStdout(), "transfer_id=%s kind=%s bytes=%d sha256=%s best_effort=true\n", cfg.Identity.TransferID, result.Kind, result.Bytes, result.SHA256)
	return err
}

// SessionRemovalOptions requests only the removal scope from a new broker.
// An old broker sees its existing member role and ignores the marker for an
// already joined member. No failed authentication is treated as version proof.
// origin: S7 internal review rv-n1compat F4 / hot-update S7 plan LQ1
func SessionRemovalOptions(sid string) []nats.Option {
	return []nats.Option{nats.Name(CtlNameForSession(sid)), nats.Token(auth.SessionRemovalToken)}
}

// Cleanup failures remain visible without erasing the operation's typed refusal.
// origin: hot-update S10 plan §0 / LINT-L2 errcheck
func finishTransferCleanup(result *error, cleanup func() error) {
	*result = errors.Join(*result, cleanup())
}

// Every command byte owner gets the same fresh, bounded cleanup deadline.
// origin: hot-update S10 plan §0 / LINT-L2 errcheck
func stopTransferOwner(owner interface {
	Stop(controlbudget.Deadline) error
}) error {
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return err
	}
	return owner.Stop(d)
}
