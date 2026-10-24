package natsinbox

import (
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/nats-io/nats.go"
)

// natsinbox — the private reply-inbox connect options, in their own package.
//
// IT IS NOT IN internal/auth, and the reason is a layering rule rather than taste:
// internal/cluster imports internal/auth for the join-PoP verify, and L-2 requires the
// raft layer to stay NATS-free. auth may pull in nats-io/nkeys (crypto); the moment it
// pulls in the nats.go CLIENT, internal/cluster transitively does too and
// test/architecture's layering table goes red. That distinction is the whole point of
// that row, and this package exists to preserve it.

// InboxConnectOptions is the ONLY supported way to opt a connection into the private
// per-identity inbox. It returns the prefix option and the marker together, because a
// connection that sets one without the other is broken:
//
//   - prefix without marker → the callout hands it the pre-cutover `_INBOX.>` grant,
//     which does not cover auth.InboxRoot at all, so every subscription to its own
//     inbox is REFUSED and every request times out;
//   - marker without prefix → it asks for the deep grant and then subscribes shallow,
//     which its grant does not cover.
//
// Both halves are pinned by test/architecture's inbox pairing gate.
//
// THE FAILURE IS LOUD, NOT SILENT — origin: prerelease audit increment 2 internal
// review, doc-truth/L12-F1 + marker-channel/F3 + pairing-sweep/F3, which measured it.
// Four places in this repository (this comment among them) used to claim a half-wired
// connection "times out with nothing in the logs". It does not: the server refuses the
// SUBSCRIBE, sends `-ERR 'Permissions Violation for Subscription to "…"'`, logs it
// server-side, and nats.go surfaces it on the async error handler. That is precisely
// why Connect below can detect the condition at all — and it is worth stating plainly,
// because the false claim was itself the argument for guarding this with a source-level
// gate instead of a runtime check.
func InboxConnectOptions(pubKey string) []nats.Option {
	return []nats.Option{
		nats.CustomInboxPrefix(auth.InboxPrefixFor(pubKey)),
		nats.UserInfo(auth.InboxCapableMarker, ""),
	}
}

// Connect dials NATS with the private inbox and falls back to the pre-cutover shared
// `_INBOX` space when the server on the other end will not grant it. It reports whether
// the fallback happened so the caller can say so in its own voice.
//
// WHY A FALLBACK EXISTS AT ALL. auth.InboxRoot is a separate top-level subject, not a
// subtree of `_INBOX` (auth.InboxPrefixFor records the measurement that forced that).
// The one N-1 quadrant this costs is {old broker × new client}: a pre-cutover broker's
// auth_callout mints `Sub _INBOX.>` and nothing else, so this client's subscription to
// its own inbox is refused and — without this function — every request it made would
// time out. Rolling the broker back while agents are already upgraded is a real
// operation on a real fleet, so it gets a real answer instead of a documented caveat.
//
// THE PROBE IS ORDERED, NOT TIMED, and that is the only reason it is cheap enough to do
// on every connection. nats-server processes a client's inbound protocol messages in
// order on one connection, so the `-ERR` for a refused SUB is written before the PONG
// that answers the PING that Flush sends. nats.go's readLoop processes them in that same
// order and sets nc.LastError() synchronously in processPermissionsViolation, BEFORE it
// queues the async callback. So by the time Flush returns, a refusal is already
// observable — no sleep, no polling, no timing assumption beyond "the server answers our
// PING at all", which Flush already requires.
//
// The cost is one PING/PONG round trip per connection. For an agent (one long-lived
// connection) that is unmeasurable; for a one-shot ctl command it is a single extra RTT
// on a connection that is about to make at least one request anyway.
func Connect(url, pubKey string, opts []nats.Option) (nc *nats.Conn, fellBack bool, err error) {
	inspect, err := inspectConnectOptions(opts)
	if err != nil {
		return nil, false, err
	}
	prefix := auth.InboxPrefixFor(pubKey)
	deep := privateConnectOptions(pubKey, opts, inspect.AsyncErrorCB)

	nc, err = nats.Connect(url, deep...)
	if err != nil {
		return nil, false, err
	}
	refused, _, probeErr := probeInbox(nc, prefix, flushBudget(inspect.Timeout))
	// A flush that does not complete says nothing about the grant. Treat it as
	// usable and let the caller's own request timeouts speak: falling back here
	// would silently downgrade a healthy connection to the shared inbox on nothing
	// more than a slow link. Within instead returns the failed admission.
	if probeErr != nil || !refused {
		return nc, false, nil //nolint:nilerr // The legacy API leaves probe failures to caller request timeouts; only a proved refusal authorizes fallback.
	}
	// The peer will not grant this identity its own inbox. Fall back to exactly the
	// connection a pre-cutover client would have made: no prefix, no marker.
	nc.Close()
	nc, err = nats.Connect(url, opts...)
	if err != nil {
		return nil, false, err
	}
	return nc, true, nil
}

// ConnectWithin admits both the private inbox and its N-1 fallback under one
// absolute deadline. The caller supplies proxydial.OptionsWithin with the same
// deadline to bound DNS, multi-URL dialing and TLS/WSS below nats.Connect.
// Admission does not install a timer on the returned, long-lived connection.
// origin: hot-update S6 plan §8.2 / T34
func ConnectWithin(url, pubKey string, opts []nats.Option, deadline controlbudget.Deadline,
	observer controlbudget.Observer) (result *nats.Conn, fallback bool, resultErr error) {
	// The composite owner ends admission after private negotiation, including a
	// proved legacy re-dial. An asynchronous first-CONNECT callback is too early.
	// origin: hot-update S6 internal review rv-ctl-14
	defer func() {
		inspect, err := inspectConnectOptions(opts)
		if err == nil {
			if dialer, ok := inspect.CustomDialer.(interface{ FinishConnect(error) error }); ok {
				resultErr = dialer.FinishConnect(resultErr)
			}
		}
		if resultErr != nil && result != nil {
			result.Close()
			result = nil
		}
	}()
	if _, err := inboxRemaining(deadline); err != nil {
		return nil, false, err
	}
	inspect, err := inspectConnectOptions(opts)
	if err != nil {
		return nil, false, err
	}
	deep := privateConnectOptions(pubKey, opts, inspect.AsyncErrorCB)
	nc, err := connectInboxWithin(url, deep, inspect.Timeout, deadline, observer, controlbudget.PhaseConnect)
	if err != nil {
		return nil, false, err
	}
	attempt, err := beginInboxAttempt(observer, controlbudget.PhaseInboxProbe)
	if err != nil {
		nc.Close()
		return nil, false, err
	}
	remaining, err := inboxRemaining(deadline)
	refused := false
	sent := false
	if err == nil {
		// Unlike the legacy probe, there is no 500ms floor: every round trip
		// consumes only the operation's remaining admission time.
		refused, sent, err = probeInbox(nc, auth.InboxPrefixFor(pubKey), min(remaining, flushBudget(inspect.Timeout)))
		if err == nil {
			_, err = inboxRemaining(deadline)
		}
	}
	class := inboxAttemptClass(err)
	if err == nil && refused {
		class = "refused"
	}
	err = finishInboxAttempt(observer, attempt, sent, class, err)
	if err != nil {
		nc.Close()
		return nil, false, err
	}
	if !refused {
		return nc, false, nil
	}
	// A completed flush plus the exact subject refusal is the only downgrade
	// proof. Timeout, disconnect and unrelated permissions cannot grant fallback.
	nc.Close()
	nc, err = connectInboxWithin(url, opts, inspect.Timeout, deadline, observer, controlbudget.PhaseInboxFallback)
	if err != nil {
		return nil, false, err
	}
	return nc, true, nil
}

// inspectConnectOptions discovers the caller's async error handler and dial timeout
// WITHOUT connecting, so the wrapper can chain to the handler rather than replace it.
// Options are plain functions over nats.Options; applying them to a throwaway value
// is the only way to read what a caller configured.
// Applying options to a fresh value also keeps our appended options out of the
// caller's backing slice, which may be reused by later connection attempts.
func inspectConnectOptions(opts []nats.Option) (nats.Options, error) {
	inspect := nats.GetDefaultOptions()
	for _, option := range opts {
		if option != nil {
			if err := option(&inspect); err != nil {
				return nats.Options{}, err
			}
		}
	}
	return inspect, nil
}

func privateConnectOptions(pubKey string, opts []nats.Option, previous nats.ErrHandler) []nats.Option {
	prefix := auth.InboxPrefixFor(pubKey)
	deep := append([]nats.Option(nil), opts...)
	deep = append(deep, InboxConnectOptions(pubKey)...)
	return append(deep, nats.ErrorHandler(func(c *nats.Conn, sub *nats.Subscription, err error) {
		if err != nil && isInboxRefusal(err.Error(), prefix) {
			// Deliberately NOT forwarded to the caller's handler: this connection is
			// about to be replaced, and a "permissions violation" line the operator can
			// do nothing about is exactly the noise that trains people to ignore the
			// handler. The fallback is reported through the return value instead.
			return
		}
		if previous != nil {
			previous(c, sub, err)
		}
	}))
}

// probeInbox separates a proved refusal from a failed round trip. LastError is
// updated synchronously before the matching PONG; the async callback is not a
// barrier and can run after the connection has already been replaced.
// origin: hot-update S6 plan §8.2 / T34
func probeInbox(nc *nats.Conn, prefix string, budget time.Duration) (refused, sent bool, err error) {
	sub, err := nc.SubscribeSync(prefix + ".>")
	if err != nil {
		return false, false, err
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushTimeout(budget); err != nil {
		// A flush that does not complete says nothing about the grant. Falling
		// back would silently downgrade a healthy connection to a shared inbox
		// on nothing more than a slow link. The legacy API lets its caller's
		// request timeouts speak; Within returns the failed admission instead.
		return false, true, err
	}
	return isInboxRefusal(lastErrText(nc), prefix), true, nil
}

// isInboxRefusal matches the one server message that means "you may not subscribe to
// your own inbox". It is matched on the SUBJECT as well as the phrase so an unrelated
// permissions violation elsewhere on the connection cannot trigger a fallback.
func isInboxRefusal(msg, prefix string) bool {
	if msg == "" {
		return false
	}
	return strings.Contains(msg, "Permissions Violation for Subscription") && strings.Contains(msg, prefix)
}

func lastErrText(nc *nats.Conn) string {
	if err := nc.LastError(); err != nil {
		return err.Error()
	}
	return ""
}

// flushBudget bounds the probe's round trip. The caller's dial timeout is the right
// scale — a caller that is willing to wait 1s to connect is not willing to wait 5s to
// probe — but it is clamped so neither an unset nor an enormous value ends up here.
func flushBudget(dial time.Duration) time.Duration {
	const (
		lo = 500 * time.Millisecond
		hi = 5 * time.Second
	)
	switch {
	case dial < lo:
		return lo
	case dial > hi:
		return hi
	default:
		return dial
	}
}
