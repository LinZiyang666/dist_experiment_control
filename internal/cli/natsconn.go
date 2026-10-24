package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraattach"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/proto"
	"os"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/proxydial"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// proxyDialTimeout bounds a single proxy dial (proxy hop + CONNECT/SOCKS
// handshake) for the ctl's normal connect path. Generous because the broker is
// typically remote and reached through a local proxy.
const proxyDialTimeout = 10 * time.Second

// DevNoAuthEnv switches the CLI off nkey CONNECT.
//
// Set TETHER_DEV_NO_AUTH=1 to make `tether session/login/exec/ps`
// connect anonymously to a vanilla `nats-server` (no auth_callout, no
// `Nkeys` config). This exists for local laptop demos / debugging only.
//
// Security: with this set, NATS does not pin the JWT `by.<actor>`
// segment, so the broker treats the actor in subjects as a routing
// label, not a proven identity (P3 trust degrades to P2-era).
// Application-layer membership/owner checks still run, but anyone with
// network access to the broker can spoof another actor's fingerprint.
// Never enable in production.
const DevNoAuthEnv = "TETHER_DEV_NO_AUTH"

// ConnectNATSWithNkey opens a NATS connection using the identity's user
// nkey for the CONNECT challenge. Callers add `nats.Name(...)` (carrying
// role + session — see internal/authcallout.parseRole) and optionally
// `nats.Token(pin)` for first-time PIN join via `opts`.
//
// With auth_callout enabled on the broker side (architecture B.2 / E.2),
// the CONNECT triggers a server→broker auth callout. NATS pins the
// user's nkey into the issued JWT permissions, so subject `by.<actor>`
// segments published from this conn are unforgeable.
//
// When DevNoAuthEnv is non-empty, this function omits nats.Nkey() and
// falls back to anonymous CONNECT — see DevNoAuthEnv doc.
func ConnectNATSWithNkey(url string, id *Identity, opts ...nats.Option) (*nats.Conn, error) {
	if id == nil {
		return nil, fmt.Errorf("cli: nil identity")
	}
	deadline, err := controlbudget.After(proxyDialTimeout)
	if err != nil {
		return nil, err
	}
	return ConnectWithin(url, id, deadline, nil, opts...)
}

// ConnectWithin assembles the authenticated connection with one absolute
// deadline. The scoped observer supplied by RunE also reaches deep attempts.
// Bounded options follow all caller options, so stale dialers/timeouts cannot
// reset admission. FinishWithin joins transport admission before returning.
// origin: hot-update S6 plan §8.2 / §8.3 / T30 / T35 / §12.1 P23
func ConnectWithin(url string, id *Identity, deadline controlbudget.Deadline, observer controlbudget.Observer, opts ...nats.Option) (nc *nats.Conn, err error) {
	if id == nil {
		return nil, errors.New("cli: nil identity")
	}
	_, err = controlbudget.Remaining(deadline)
	if err != nil {
		return nil, err
	}
	all := append(ctlConnOptions(), opts...)
	caller := nats.GetDefaultOptions()
	for _, option := range all {
		if err = option(&caller); err != nil {
			return nil, err
		}
	}
	config, _ := infraattach.Configuration(caller.CustomDialer)
	if os.Getenv(DevNoAuthEnv) == "" {
		seed := append([]byte(nil), id.Seed...)
		all = append(all, nats.Nkey(id.PublicKey, func(nonce []byte) ([]byte, error) {
			kp, e := nkeys.FromSeed(seed)
			if e != nil {
				return nil, e
			}
			defer kp.Wipe()
			return kp.Sign(nonce)
		}))
	}
	// Proxy-aware dial: when a proxy env var is set, route the NATS TCP dial
	// through it (HTTP CONNECT / SOCKS5) and skip nats.go's local DNS so the
	// hostname reaches the proxy for remote resolution. Direct and NO_PROXY
	// dials still carry the original cancellable admission budget.
	// NATS inbox admission owns CONNECT, including when an infrastructure
	// dialer wraps the proxy and hides its optional ownership hook.
	// origin: S7 internal review MAKETEST-TRANSFER F1; S6 plan §8.3
	popts, err := proxydial.OptionsWithin(proxydial.OSEnv, deadline, controlbudget.WithoutPhases(observer, controlbudget.PhaseConnect))
	if err != nil {
		return nil, err
	}
	all = append(all, popts...)
	// Options supplied by callers cannot expand a bound or enable background
	// initial reconnects after this synchronous admission helper returns.
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, err
	}
	// The proxy ConnectedCB safety net would detach after the first private
	// CONNECT, before inbox refusal can require a second dial. This owner joins
	// synchronously after negotiation, keeping both dials on the same budget.
	// origin: hot-update S6 plan §8.2 / T30 / T34
	all = append(all, nats.Timeout(remaining), nats.RetryOnFailedConnect(false), func(o *nats.Options) error {
		o.ConnectedCB = caller.ConnectedCB
		return nil
	})
	// PRIVATE reply inbox, plus the CONNECT marker that tells the callout to grant it
	// instead of the shared pre-cutover `_INBOX.>`. auth_callout derives the same prefix
	// from this connection's nkey, so replies — exec output, the print-once /sub token —
	// are no longer readable by every other connection on the account.
	// origin: prerelease audit proto-auth-acl/L1-F1.
	//
	// natsinbox.Connect rather than a bare option append, because the inbox now lives
	// under its own root and a PRE-CUTOVER broker grants no reach to it at all. Against
	// one of those this probes once and falls back to the shared space, which is the
	// {old broker × new ctl} rollback quadrant. Silence here is deliberate: a ctl prints
	// command output, and a line about inbox roots on every command against an older
	// broker would be noise the user cannot act on. The agent, which is long-lived and
	// whose operator CAN act on it, logs it.
	admissionOptions := all
	client, all, err := infraattach.Prepare(all, deadline, config)
	if err != nil {
		return nil, err
	}
	if os.Getenv(DevNoAuthEnv) != "" {
		nc, err = natsinbox.ConnectTransportWithin(url, all, deadline, observer)
	} else {
		nc, _, err = natsinbox.ConnectWithin(url, id.PublicKey, all, deadline, observer)
	}
	err = proxydial.FinishWithin(admissionOptions, err)
	if err == nil {
		err = infraattach.Finish(nc, deadline)
	}
	if err != nil {
		err = errors.Join(err, client.Close(deadline))
	}
	// origin: hot-update S6 plan §8.2 / T50 / COHORT-COLLECTOR
	// During delayed callout admission, nats-server may send its keepalive
	// PING before the CONNECT PONG. nats.go closes that incomplete handshake.
	// It is a retryable missing PONG, while every other protocol/pin error
	// remains terminal. The RunE's receipt eligibility and deadline still gate
	// retries; this supplies no old-version evidence and cannot downgrade exec.
	if err != nil && strings.TrimSpace(err.Error()) == "nats: expected 'PONG', got 'PING'" {
		err = errors.Join(nats.ErrTimeout, err)
	}
	if err == nil {
		_, err = controlbudget.Remaining(deadline)
	}
	if err != nil && nc != nil {
		nc.Close()
		nc = nil
	}
	return nc, err
}

// BindControlObserver keeps deep helpers on the RunE operation's ID even
// when their frozen signatures do not carry a logical ID separately.
// origin: hot-update S6 plan §8.3 / T30
func BindControlObserver(observer controlbudget.Observer, logicalID string) controlbudget.Observer {
	if observer == nil {
		return nil
	}
	return controlObserver{Observer: observer, logicalID: logicalID}
}

type controlObserver struct {
	controlbudget.Observer
	logicalID string
}

func (o controlObserver) Begin(_ string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	return o.Observer.Begin(o.logicalID, phase)
}

// ctlConnOptions are the reconnect options every ctl connection carries.
//
// IgnoreAuthErrorAbort (hot-update S1): nats.go permanently CLOSES a
// connection after seeing the same auth error twice in a row while
// reconnecting. During a nats-server restart the broker — the only
// auth_callout responder — comes back a moment after the server does, so a
// long-lived ctl command (`exec` mid-stream, `history --follow`) reconnecting
// into that window collected two `Authorization Violation`s and died with
// ErrConnectionClosed instead of finishing. The agent has carried this option
// since gotcha #89 for the same reason. The cost, documented in usage §9.13.2:
// a ctl whose membership is revoked mid-command keeps reconnecting until its
// --timeout or Ctrl-C; the broker still refuses every request with 77.
// Initial authentication stays fail-fast (this only governs reconnects).
func ctlConnOptions() []nats.Option {
	return []nats.Option{nats.MaxReconnects(-1), nats.IgnoreAuthErrorAbort()}
}

// ResolveNATSURLFromHome implements the broker-URL precedence chain
// the ctl uses on every command. flagVal is whatever cobra parsed
// (cobra default unless the user passed --nats-url); flagChanged is
// Cmd.Flags().Changed("nats-url"). Returns the URL the caller should
// hand to ConnectNATSWithNkey.
//
// Precedence (high → low):
//  1. explicit --nats-url flag (operator override always wins)
//  2. $TETHER_NATS_URL env var (per-shell override)
//  3. ~/.tether/broker_url file (persistent default written by `tether login --broker`)
//  4. flagVal (cobra default — typically nats://127.0.0.1:4222)
//
// Call this in every ctl subcommand's RunE before connecting.
func ResolveNATSURLFromHome(flagVal string, flagChanged bool, home string) string {
	if flagChanged {
		return flagVal
	}
	if u := ReadDefaultBrokerURL(home); u != "" {
		return u
	}
	return flagVal
}

// CtlNameUnactivated is the connection-Name a CLI uses before activating
// a session. The broker auth_callout decodes this and grants the
// "unactivated" permission template (architecture B.2).
const CtlNameUnactivated = "tether-cli"

// CtlNameForSession is the connection-Name a CLI uses to activate a
// session. The broker auth_callout extracts <sid> and grants the
// "activated member" permission template after a membership check (or
// PIN verify if a Token is also presented).
func CtlNameForSession(sid string) string { return "tether-cli:" + sid }

// AgentName is the connection-Name an agent uses on CONNECT. The
// auth_callout handler parses this format ("tether-agent:<sid>:<nid>")
// to bind the connecting nkey to the (sid, nid) registered in
// agent_provisioning (architecture E.2 / K.1). Real agents always
// connect with this Name + their nkey; the broker's auth_callout
// branch verifies the binding before issuing a JWT.
func AgentName(sid, nid string) string { return "tether-agent:" + sid + ":" + nid }

// origin: hot-update S6 plan §8.2 / T30
// fetchAlertsStrict pulls the active alert set from any one broker (queue-group). It returns a
// real error on no-responder / timeout / malformed reply — for the EXPLICIT `alert ls` command,
// which must NOT show a false "no active alerts" when the store could not be queried (F4).
func FetchAlertsWithin(ctx context.Context, nc *nats.Conn, actor string, timeout time.Duration) ([]proto.AlertView, error) {
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	msg, err := RequestControlWithin(requestCtx, nc, proto.SubjCtrlAlertLs(actor), nil, "", nil)
	if err != nil {
		return nil, fmt.Errorf("alert ls: query: %w (no broker answered the alert store)", err)
	}
	var resp proto.AlertLsResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("alert ls: malformed reply: %w", err)
	}
	return resp.Alerts, nil
}

// origin: hot-update S6 plan §8.2 / T36
// ackAlert sends a cluster-level ack for dedupKey (one broker forwards it to the leader). It is
// FAIL-CLOSED (F3): only the exact reply "ok" is success — a broker "error: …" (e.g. no leader
// accepted the ack), an empty/malformed reply, or a transport failure all return a non-nil
// error, so an operator/script never treats an un-committed ack as acknowledged.
func AckAlertWithin(ctx context.Context, nc *nats.Conn, actor, dedupKey string, timeout time.Duration) (string, error) {
	body, err := json.Marshal(proto.AlertAckReq{DedupKey: dedupKey})
	if err != nil {
		return "", err
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	msg, err := RequestControlWithin(requestCtx, nc, proto.SubjCtrlAlertAck(actor), body, "", nil)
	if err != nil {
		var transportErr *ControlRequestError
		if !errors.As(err, &transportErr) || !transportErr.Sent {
			return "", fmt.Errorf("alert ack: request: %w", err)
		}
		// A sent mutation is never retried by the helper. Keep the shared wire
		// code/detail so automation cannot mistake a lost commit reply for safety.
		return "", fmt.Errorf("%w: alert ack result unavailable: %w", ErrControlResultUnknown, err)
	}
	reply := string(msg.Data)
	if reply != "ok" {
		if reply == "" {
			reply = "(empty reply)"
		}
		return reply, fmt.Errorf("alert ack not accepted: %s", reply)
	}
	return reply, nil
}

// RequestControlWithin records the actual publish boundary rather than
// guessing that a timeout means a request was sent. The private inbox comes
// from the authenticated connection, so this also preserves the ACL marker.
// origin: hot-update S6 plan §8.2 / §8.3 / T30 / T36
func RequestControlWithin(ctx context.Context, nc *nats.Conn, subject string, body []byte, logicalID string, observer controlbudget.Observer) (_ *nats.Msg, resultErr error) {
	if observation, ok := ctx.Value(controlObservationKey{}).(controlObservation); observer == nil && ok {
		observer, logicalID = observation.observer, observation.logicalID
	}

	var attempt controlbudget.Attempt
	defer func() {
		if resultErr != nil {
			resultErr = &ControlRequestError{Err: resultErr, Sent: attempt.Sent}
		}
	}()

	if observer != nil {
		var err error
		attempt, err = observer.Begin(logicalID, controlbudget.PhaseRequest)
		if err != nil {
			return nil, err
		}
		defer func() {
			finished, clockErr := controlbudget.Now()
			attempt.FinishedMonoNS = finished
			attempt.Class = "success"
			if resultErr != nil {
				attempt.Class = "error"
			}
			resultErr = errors.Join(resultErr, clockErr, observer.Record(attempt))
		}()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, sub.Unsubscribe()) }()
	if err := sub.SetPendingLimits(8, 8*proto.ProcessRequestMaxBytes); err != nil {
		return nil, err
	}
	if err := nc.PublishRequest(subject, inbox, body); err != nil {
		return nil, err
	}
	attempt.Sent = true
	msg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return nil, err
	}
	if len(msg.Data) == 0 && msg.Header.Get("Status") == "503" {
		return nil, nats.ErrNoResponders
	}
	return msg, nil
}

// WithControlObservation carries the operation observer through helper APIs
// that already accept a caller context, without giving it to data-plane owners.
// origin: hot-update S6 plan §8.3 / T30
func WithControlObservation(ctx context.Context, logicalID string, observer controlbudget.Observer) context.Context {
	return context.WithValue(ctx, controlObservationKey{}, controlObservation{logicalID: logicalID, observer: observer})
}

type controlObservationKey struct{}
type controlObservation struct {
	logicalID string
	observer  controlbudget.Observer
}

// ControlRequestError records transport evidence for unsafe callers; a
// pre-publish failure cannot imply an unknown committed effect.
// origin: hot-update S6 plan §8.2 / T36
type ControlRequestError struct {
	Err  error
	Sent bool
}

func (e *ControlRequestError) Error() string { return e.Err.Error() }
func (e *ControlRequestError) Unwrap() error { return e.Err }
