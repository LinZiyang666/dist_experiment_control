package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/execclient"
	"github.com/LinZiyang666/tether/internal/infraattach"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// ctl_connect.go — the ONE ctl/cli connect path with cluster broker auto-failover (v0.4.5 阶段4 fix).
// Every ctl-over-NATS subcommand connects through connectCtl instead of the bare
// ResolveNATSURLFromHome+ConnectNATSWithNkey+connectError triple, so failover, the operator-override
// invariant, and the opportunistic discovery refresh live in exactly one place.

const (
	// ctlRefreshTTL bounds how often a File-source command re-fetches the signed HTTP manifest (tier-2).
	ctlRefreshTTL = 10 * time.Minute
	// ctlManifestFetchTimeout bounds the post-connect manifest fetch so a slow/absent route adds at most
	// this to a command, once per TTL.
	ctlManifestFetchTimeout = 2500 * time.Millisecond
)

// connectCtl resolves the dial set, connects with nats.Connect failover, and (only on the expandable,
// OOB-pinned broker_url path) opportunistically refreshes the discovery cache from the signed manifest.
// base is the human single URL (error text); dial is the comma-separated failover list (== base on every
// non-expandable path → byte-equivalent to today). The two extra opts are added ONLY when dial != base.
func connectCtl(cmd *cobra.Command, verb, home, natsURL string, id *cli.Identity, name nats.Option) (*nats.Conn, error) {
	return connectCtlOpts(cmd, verb, home, natsURL, id, name)
}

// connectCtlOpts is connectCtl with caller-supplied NATS options (e.g. nats.Token(pin) for a first-time
// login PIN join). Same source-aware failover: expand ONLY on the persisted broker_url default (never an
// explicit --nats-url/--broker flag nor $TETHER_NATS_URL), and opportunistically refresh the discovery
// cache from the signed manifest on every eligible connect — including a bootstrap-only pin whose dial has
// not expanded yet (the refresh is gated on source eligibility, NOT on dial!=base, so tier-2 can warm).
func connectCtlOpts(cmd *cobra.Command, verb, home, natsURL string, id *cli.Identity, extra ...nats.Option) (resultNC *nats.Conn, resultErr error) {
	if id == nil {
		return nil, errors.New("ctl: nil identity")
	}
	hadAdmission := cmd.Context().Value(ctlAdmissionKey{}) != nil
	a, err := ensureCtlAdmission(cmd)
	if err != nil {
		return nil, err
	}
	if !hadAdmission {
		defer func() {
			finishCtlAdmission(a, &resultErr)
			if resultErr != nil && resultNC != nil {
				resultNC.Close()
				resultNC = nil
			}
		}()
	}
	base := natsURL // already resolved by the caller's ResolveNATSURLFromHome (single human URL)
	// --broker is login's alias of --nats-url; both pin single. An explicit flag or $TETHER_NATS_URL means
	// the source is NOT the persisted broker_url default → no expansion, no refresh.
	flagChanged := cmd.Flags().Changed("nats-url") || cmd.Flags().Changed("broker")
	expandable := !flagChanged && os.Getenv(cli.DefaultBrokerURLEnv) == ""
	dial := cli.DialFor(flagChanged, base, home, time.Now())
	opts := append([]nats.Option(nil), extra...)
	// All ctl commands share this invocation's registered raw owner. The
	// independent S7 data attachment inherits it through its own role token.
	// origin: hot-update S9 plan §4.5 / T51 / M25
	if config, ok := infraattach.FromContext(cmd.Context()); ok {
		opts = append(opts, infraattach.Configure(config))
	}
	// h1 D2: route async NATS errors through OUR handler instead of nats.go's
	// default (which prints straight to stderr — catastrophic under `run`'s
	// raw-mode terminal). A permissions violation on a `.ka` keepalive
	// publish additionally flips kaPubDenied so the pump stops: that is the
	// new-ctl + old-broker skew (the v0.4.7-minted JWT has no .ka grant).
	opts = append(opts, nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		if err == nil {
			return
		}
		if strings.Contains(err.Error(), "Permissions Violation") && strings.Contains(err.Error(), ".ka") {
			kaPubDenied.Store(true)
			return // silent: the degraded mode IS the old behavior (no liveness contract)
		}
		// A run connection carries the user's raw terminal. Reconnect CONNECT
		// timeouts reach this callback even while the reliable session is still
		// recovering, and stderr commonly names the same tty as stdout. The run
		// owner reports any final failure through main after restoring termios;
		// asynchronous carrier diagnostics cannot be inserted into its bytes.
		// This is connection-scoped, so queued callbacks cannot leak through a
		// short raw-mode flag window or race terminal restoration.
		// origin: hot-update S4 plan §7.2 / §7.3 / T35; PRODUCT-R4 arm C1
		if verb == "run" {
			return
		}
		fmt.Fprintf(os.Stderr, "\r\ntether: nats async error: %v\r\n", err)
	}))
	if dial != base {
		// DontRandomize honors the floor-last order. ConnectWithin supplies the
		// remaining dial budget, including every endpoint and proxy handshake.
		// origin: hot-update S6 plan §8.2 / T30
		opts = append(opts, nats.DontRandomize())
	}
	// Connection-level bounded retry (hot-update S1 §1.5): an identity that
	// has authenticated for this session before may retry a transient CONNECT
	// failure — the broker or nats-server restarting under it — within
	// ctlAuthRetryBudget. A fresh identity (first login, rotated key) gets
	// exactly one attempt, so a wrong PIN fails fast. Which attempt this is
	// is printed so an operator can tell a black window from a hang.
	sid, activated := cli.SessionFromCtlName(ctlNameOf(opts))
	eligible := connectRetryEligible(verb, home, sid, activated, id.Fingerprint)
	nc, err := connectWithAuthRetryWithin(cmd.Context(), a.deadline, a.logicalID, controlbudget.WithoutPhases(a.observer, controlbudget.PhaseConnect), eligible,
		func(deadline controlbudget.Deadline) (*nats.Conn, error) {
			return cli.ConnectWithin(dial, id, deadline, a.observer, opts...)
		},
		func(attempt int, err error, next time.Duration) {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "tether: %s: broker not ready (%v); retrying in %s (attempt %d; identity previously authenticated for session %s)\n",
				verb, err, next, attempt, sid)
		})
	if err != nil {
		return nil, connectError(verb, base, err)
	}
	if activated {
		_ = cli.WriteAuthReceipt(home, sid, id.Fingerprint) // best-effort; the command already succeeded
	}
	if expandable {
		if err = refreshCtlEndpointsWithin(cmd.Context(), a, nc, home, base, id.PublicKey); err != nil {
			nc.Close()
			return nil, err
		}
	}
	return nc, nil
}

// refreshCtlEndpointsWithin reserves time for the primary request and health
// gate. Both best-effort discovery transports share this one short window.
// origin: hot-update S6 internal review rv-ctl-10 / plan §8.2
func refreshCtlEndpointsWithin(ctx context.Context, a *ctlAdmission, nc *nats.Conn, home, base, actor string) error {
	const refreshWindow = 500 * time.Millisecond
	const primaryReserve = time.Second
	remaining, err := controlbudget.Remaining(a.deadline)
	if err != nil || remaining < refreshWindow+primaryReserve || ctx.Err() != nil {
		if a.observer == nil {
			return nil
		}
		attempt, err := a.observer.Begin(a.logicalID, controlbudget.PhaseDiscovery)
		if err != nil {
			return err
		}
		attempt.FinishedMonoNS, err = controlbudget.Now()
		attempt.Class = "skipped"
		return errors.Join(err, a.observer.Record(attempt))
	}
	refreshCtx, cancel := context.WithTimeout(ctx, refreshWindow)
	defer cancel()
	return observeCtlPhase(a, controlbudget.PhaseDiscovery, func() error {
		refreshCtlEndpoints(cli.WithControlObservation(refreshCtx, a.logicalID, a.observer), nc, home, base, actor)
		return nil
	})
}

// origin: hot-update S8 internal review FIX-AGENTSLIM.
func connectRetryEligible(verb, home, sid string, activated bool, fingerprint string) bool {
	return cli.ConnectRetryEligible(verb, home, sid, activated, fingerprint)
}

// ctlAuthRetryBudget is the complete admission cap, including all dial attempts.
// origin: hot-update S6 plan §8.2 / T33
const ctlAuthRetryBudget = 5 * time.Second

// ctlNameOf reads the nats.Name an option list would set, the same way
// natsinbox does: apply the options to a throwaway Options value.
func ctlNameOf(opts []nats.Option) string {
	o := nats.GetDefaultOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		_ = opt(&o)
	}
	return o.Name
}

// origin: hot-update S8 internal review FIX-AGENTSLIM.
func transientConnectErr(err error) bool { return agentupgrade.TransientConnectError(err) }

// refreshCtlEndpoints is the tier-2 opportunistic discovery refresh: best-effort, TTL-gated, and
// PIN-gated. It funnels a signed manifest through the agent's single adopt authority (AdoptDecision)
// against the EXISTING pin — a forged/foreign/expired/rollback manifest is rejected and leaves the cache
// bytes UNCHANGED (no poison).
//
// G3 #17 — two changes to escape the "floor single-point" of the shipped version:
//   - 改法二 (primary): pull the signed manifest from the ACTUALLY-CONNECTED broker over the live conn
//     (SubjCtrlClusterRoster), so discovery converges from whichever survivor a transparent failover
//     landed on — not the one pinned floor/bootstrap host. HTTP bootstrap is kept as a secondary fallback.
//   - 改法一: the FloorURL==base gate B is REMOVED. A signed manifest self-authenticates against the OOB
//     pin, so WHICH broker delivered it is cryptographically irrelevant. On the accepted path FloorURL is
//     migrated to base so DialFor's FloorURL==base expansion gate self-heals after an operator re-point.
//
// 门A (PIN) stays ABSOLUTE and structural: a nil/empty pin ⇒ return BEFORE any consume — never TOFU-pin
// off a network responder's self-claimed AccountPub (AdoptDecision would otherwise TOFU the roster path).
func refreshCtlEndpoints(ctx context.Context, nc *nats.Conn, home, base, actor string) {
	ce, err := cli.ReadClusterEndpoints(home)
	if err != nil || ce == nil || ce.PinAccountPub == "" {
		return
	}
	if ce.FetchedAt != "" {
		if t, perr := time.Parse(time.RFC3339, ce.FetchedAt); perr == nil && time.Since(t) < ctlRefreshTTL {
			return
		}
	}
	// templateURL = the STABLE cached FloorURL (fallback base) so DialURLs scheme/port/path templating is
	// consistent regardless of which survivor answered (homogeneous-port assumption; only host varies).
	tmpl := ce.FloorURL
	if tmpl == "" {
		tmpl = base
	}
	// Nothing to refresh FROM (no live conn AND no bootstrap URL) → return WITHOUT touching FetchedAt: we
	// never attempted a fetch, so we must not throttle the next eligible connect.
	if nc == nil && ce.BootstrapURL == "" {
		return
	}
	prev := agent.RosterState{
		Pin: ce.PinAccountPub, RosterGen: ce.RosterGen, SeedGen: ce.SeedGen,
		Roster: ce.Roster, Seeds: ce.Seeds,
	}
	next := prev
	accepted := false
	adopt := func(m *proto.ClusterManifest) {
		if m == nil {
			return
		}
		if n2, ok := agent.AdoptDecision(next, m.Roster, m.Seeds, tmpl, time.Now()); ok {
			next = n2 // accumulate — AdoptDecision's monotone gate keeps the higher generation
			accepted = true
		}
	}
	// 改法二 primary: pull from the actually-connected survivor (escapes the floor/bootstrap single point).
	adopt(fetchManifestOverNATS(ctx, nc, actor))
	// External-review F1: if the NATS pull did not advance EITHER generation (a cached-but-stale roster-pull
	// in the ≥30s manifest window, or nil / rejected — note the roster and seed gens advance independently,
	// so a manifest can bump seed but not roster), consult the HTTP bootstrap too so a FRESHER manifest is
	// not masked by the stale one. Both candidates flow through the SAME AdoptDecision monotone gate — the
	// higher generation wins, a rollback/foreign one is still rejected (no poison). Only when NATS advanced
	// BOTH gens (fully fresh) do we skip HTTP, preserving 改法二's "no bootstrap dependency" property.
	if (next.RosterGen <= prev.RosterGen || next.SeedGen <= prev.SeedGen) && ce.BootstrapURL != "" {
		fctx, cancel := context.WithTimeout(ctx, ctlManifestFetchTimeout)
		defer cancel()
		httpM, _ := clusterroster.FetchManifest(fctx, ce.BootstrapURL, 0)
		adopt(httpM)
	}
	// Stage-C M2: we ATTEMPTED a refresh — advance FetchedAt to throttle it to at most once per ctlRefreshTTL,
	// so a FAILING/non-advancing refresh does NOT re-incur the (possibly multi-second) fetch on every ctl
	// command; it retries once per TTL, like a successful one.
	ce.FetchedAt = time.Now().UTC().Format(time.RFC3339)
	if accepted {
		ce.RosterGen = next.RosterGen
		ce.Roster = next.Roster
		ce.SeedGen = next.SeedGen
		ce.Seeds = next.Seeds
		ce.FloorURL = base // 改法一: self-heal the DialFor expansion gate after a re-point (accepted path only)
	}
	// reject/no-progress → roster/seed/FloorURL fields untouched (no poison); only FetchedAt advanced.
	_ = cli.WriteClusterEndpoints(home, ce)
}

// fetchManifestOverNATS pulls the signed cluster manifest from the connected broker over the live conn
// (G3 #17 改法二). Returns nil on ANY request failure — ErrNoResponders (an old broker that never wired
// the responder), a TIMEOUT (a cluster-mode-but-unsigned broker that is subscribed but silent, or a new
// ctl whose JWT was minted by an OLD broker lacking the cluster-roster.req pub-allow so the publish is
// refused), or a bad reply — so the caller falls back to the HTTP bootstrap. nil is NOT an error, it is
// "this broker cannot answer over NATS". The reply is verified downstream by AdoptDecision against the
// OOB pin (this never trusts it blindly).
func fetchManifestOverNATS(ctx context.Context, nc *nats.Conn, actor string) *proto.ClusterManifest {
	if nc == nil || actor == "" {
		return nil
	}
	rctx, cancel := context.WithTimeout(ctx, ctlManifestFetchTimeout)
	defer cancel()
	msg, err := cli.RequestControlWithin(rctx, nc, proto.SubjCtrlClusterRoster(actor), nil, "", nil)
	if err != nil || msg == nil || len(msg.Data) == 0 {
		return nil
	}
	var m proto.ClusterManifest
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		return nil
	}
	return &m
}

// ctlAdmission lives on the command context without imposing that context's
// admission deadline on a long-running task. Only true start ends admission.
// origin: hot-update S6 plan §8.2 / T30 / T33
type ctlAdmissionKey struct{}
type ctlAdmission struct {
	deadline    controlbudget.Deadline
	logicalID   string
	observer    controlbudget.Observer
	closed      bool
	settled     bool
	diagnostics io.Writer
}

func beginCtlAdmission(cmd *cobra.Command) (*ctlAdmission, error) {
	now, err := controlbudget.Now()
	if err != nil {
		return nil, err
	}
	duration := upgradebudget.RequestLatency
	if d, ok := cmd.Context().Deadline(); ok && time.Until(d) < duration {
		duration = time.Until(d)
	}
	if duration <= 0 {
		return nil, context.DeadlineExceeded
	}
	deadline := controlbudget.Deadline(now + int64(duration))
	logicalID, err := cli.AttemptLogicalID(proc.NewPID())
	if err != nil {
		return nil, err
	}
	observer, err := cli.AttemptSinkFromEnv(deadline)
	if err != nil {
		return nil, err
	}
	// The operation budget also owns result reporting. Preserve the original
	// sink bound while reserving its final slice from all network attempts.
	// origin: hot-update S6 plan §8.2 / T30 / EXITS-2-3
	workDeadline := deadline - controlbudget.Deadline(min(cli.ControlResultReserve, duration/10))
	a := &ctlAdmission{deadline: workDeadline, logicalID: logicalID, diagnostics: cmd.ErrOrStderr()}
	// An unset receipt FD returns a nil *AttemptSink. Converting it directly
	// to Observer would create a non-nil interface and panic on the first Begin.
	// origin: hot-update S6 plan §8.3 / T35
	if observer != nil {
		a.observer = cli.BindControlObserver(observer, a.logicalID)
	}
	cmd.SetContext(context.WithValue(cmd.Context(), ctlAdmissionKey{}, a))
	return a, nil
}

func ensureCtlAdmission(cmd *cobra.Command) (*ctlAdmission, error) {
	if a, ok := cmd.Context().Value(ctlAdmissionKey{}).(*ctlAdmission); ok {
		return a, nil
	}
	// Other ctl verbs share the bounded connect path; their command owner closes
	// the sink when adopting admission. The four S6 RunE consumers start it first.
	return beginCtlAdmission(cmd)
}

func finishCtlAdmission(a *ctlAdmission, result *error) {
	if a == nil || a.closed {
		return
	}
	a.closed = true
	if a.observer != nil {
		attempt, err := a.observer.Begin(a.logicalID, controlbudget.PhaseResult)
		if err == nil {
			attempt.FinishedMonoNS, err = controlbudget.Now()
			attempt.Class = "success"
			if *result != nil {
				attempt.Class = "error"
			}
			err = errors.Join(err, a.observer.Record(attempt))
		}
		receiptErr := errors.Join(err, a.observer.Close())
		// origin: hot-update S6 internal review rv-ctl-5
		// Instrumentation still reports failure, but cannot replace a remote
		// status whose task and terminal ACK have already settled.
		if a.settled {
			if receiptErr != nil && a.diagnostics != nil {
				_, _ = fmt.Fprintf(a.diagnostics, "tether: attempt receipt failed: %v\n", receiptErr)
			}
		} else {
			*result = errors.Join(*result, receiptErr)
		}
	}
}

func ctlAdmissionConfig(cmd *cobra.Command) execclient.AdmissionConfig {
	a, _ := cmd.Context().Value(ctlAdmissionKey{}).(*ctlAdmission)
	if a == nil {
		return execclient.AdmissionConfig{}
	}
	return execclient.AdmissionConfig{Deadline: a.deadline, LogicalID: a.logicalID, Observer: a.observer}
}

func ctlAdmissionContext(ctx context.Context, deadline controlbudget.Deadline) (context.Context, context.CancelFunc, error) {
	now, err := controlbudget.Now()
	if err != nil {
		return nil, nil, err
	}
	remaining := time.Duration(int64(deadline) - now)
	if deadline <= 0 || remaining <= 0 {
		return nil, nil, context.DeadlineExceeded
	}
	child, cancel := context.WithTimeout(ctx, remaining)
	return child, cancel, nil
}

// Observe phase completion even on failure. Raw errors never enter the
// receipt sink: only the bounded, secret-free class vocabulary does.
// origin: hot-update S6 plan §8.3 / T35
func observeCtlPhase(a *ctlAdmission, phase controlbudget.AttemptPhase, call func() error) error {
	operationErr, observationErr := observeCtlPhaseOutcome(a, phase, call)
	if observationErr == nil {
		return operationErr
	}
	return errors.Join(operationErr, observationErr)
}

// Keep receipt failures distinct from transport failures: an unavailable sink
// must stop admission even when the underlying CONNECT error admits retry.
// origin: hot-update S6 plan §8.3 / T35
func observeCtlPhaseOutcome(a *ctlAdmission, phase controlbudget.AttemptPhase, call func() error) (error, error) {
	if a == nil || a.observer == nil {
		return call(), nil
	}
	attempt, err := a.observer.Begin(a.logicalID, phase)
	if err != nil {
		return nil, err
	}
	operationErr := call()
	finished, clockErr := controlbudget.Now()
	attempt.FinishedMonoNS = finished
	// Record the wait actually consumed, including an interrupted backoff.
	if phase == controlbudget.PhaseBackoff {
		attempt.BackoffNS = max(0, finished-attempt.StartedMonoNS)
	}
	attempt.Class = "success"
	if operationErr != nil {
		attempt.Class = "error"
	}
	recordErr := a.observer.Record(attempt)
	return operationErr, errors.Join(clockErr, recordErr)
}

// Each retry passes the same absolute deadline into the deep connector.
// A receipt permits retry; it never permits authentication or suppresses a
// final authorization refusal. Fresh login/PIN always gets one attempt.
// origin: hot-update S6 plan §8.2 / T33
func connectWithAuthRetryWithin(ctx context.Context, deadline controlbudget.Deadline, logicalID string, observer controlbudget.Observer, eligible bool,
	attempt func(controlbudget.Deadline) (*nats.Conn, error), report func(int, error, time.Duration)) (*nats.Conn, error) {
	backoff := 250 * time.Millisecond
	var last error
	for n := 1; ; n++ {
		remainingCtx, cancel, err := ctlAdmissionContext(ctx, deadline)
		if err != nil {
			if last != nil {
				return nil, last
			}
			return nil, err
		}
		if remainingCtx.Err() != nil {
			cancel()
			if last != nil {
				return nil, last
			}
			return nil, remainingCtx.Err()
		}
		cancel()
		a := &ctlAdmission{deadline: deadline, logicalID: logicalID, observer: observer}
		var nc *nats.Conn
		var dialErr error
		var observationErr error
		err, observationErr = observeCtlPhaseOutcome(a, controlbudget.PhaseConnect, func() error {
			nc, dialErr = attempt(deadline)
			return dialErr
		})
		if observationErr != nil {
			if nc != nil {
				nc.Close()
			}
			return nil, errors.Join(err, observationErr)
		}
		if err == nil {
			return nc, nil
		}
		if nc != nil {
			nc.Close()
		}
		// origin: hot-update S6 internal review rv-ctl-6
		// A budget-truncated attempt carries less evidence than the preceding
		// callout refusal. Keep the refusal for the operator and exit classifier.
		var timed net.Error
		truncated := controlbudget.IsExpired(err) || errors.Is(err, nats.ErrTimeout) || errors.As(err, &timed) && timed.Timeout()
		if last == nil || !truncated || !errors.Is(last, nats.ErrAuthorization) {
			last = err
		}
		if !eligible || !transientConnectErr(err) {
			return nil, last
		}
		now, clockErr := controlbudget.Now()
		if clockErr != nil {
			return nil, errors.Join(err, clockErr)
		}
		if now+int64(backoff) >= int64(deadline) {
			return nil, last
		}
		if report != nil {
			report(n, err, backoff)
		}
		waitErr := observeCtlPhase(a, controlbudget.PhaseBackoff, func() error {
			timer := time.NewTimer(backoff)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		})
		if waitErr != nil {
			return nil, errors.Join(last, waitErr)
		}
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

// Read-only requests may be retried independently. Re-entering an entire
// RunE would recreate process handles and replay unsafe operations.
// origin: hot-update S6 plan §8.2 / T36
func requestCtlWithin(cmd *cobra.Command, nc *nats.Conn, subject string, body []byte, cap time.Duration) (*nats.Msg, error) {
	a, err := ensureCtlAdmission(cmd)
	if err != nil {
		return nil, err
	}
	deadline := a.deadline
	now, err := controlbudget.Now()
	if err != nil {
		return nil, err
	}
	if cap > 0 && now+int64(cap) < int64(deadline) {
		deadline = controlbudget.Deadline(now + int64(cap))
	}
	var msg *nats.Msg
	err = cli.DoPhaseWithin(cmd.Context(), cli.RetryConfig{Deadline: deadline, Family: cli.RetryReadOnly, LogicalID: a.logicalID, Observer: controlbudget.WithoutPhases(a.observer, controlbudget.PhaseResult)}, controlbudget.PhaseRequest, func(remaining time.Duration) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), remaining)
		defer cancel()
		var requestErr error
		msg, requestErr = cli.RequestControlWithin(ctx, nc, subject, body, a.logicalID, controlbudget.WithoutPhases(a.observer, controlbudget.PhaseRequest))
		return requestErr
	})
	return msg, err
}
