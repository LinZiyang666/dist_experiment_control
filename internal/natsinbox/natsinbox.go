package natsinbox

import (
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
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
	// Discover the caller's async error handler and dial timeout WITHOUT connecting, so
	// the wrapper below can chain to the handler rather than replace it. Options are
	// plain functions over nats.Options; applying them to a throwaway value is the only
	// way to read what a caller configured.
	inspect := nats.GetDefaultOptions()
	for _, o := range opts {
		if o == nil {
			continue
		}
		if e := o(&inspect); e != nil {
			return nil, false, e
		}
	}
	prevErrCB := inspect.AsyncErrorCB

	prefix := auth.InboxPrefixFor(pubKey)
	var mu sync.Mutex
	refused := false

	deep := make([]nats.Option, 0, len(opts)+3)
	deep = append(deep, opts...)
	deep = append(deep, InboxConnectOptions(pubKey)...)
	deep = append(deep, nats.ErrorHandler(func(c *nats.Conn, sub *nats.Subscription, aerr error) {
		if aerr != nil && isInboxRefusal(aerr.Error(), prefix) {
			mu.Lock()
			refused = true
			mu.Unlock()
			// Deliberately NOT forwarded to the caller's handler: this connection is
			// about to be replaced, and a "permissions violation" line the operator can
			// do nothing about is exactly the noise that trains people to ignore the
			// handler. The fallback is reported through the return value instead.
			return
		}
		if prevErrCB != nil {
			prevErrCB(c, sub, aerr)
		}
	}))

	nc, err = nats.Connect(url, deep...)
	if err != nil {
		return nil, false, err
	}
	if inboxUsable(nc, prefix, flushBudget(inspect.Timeout)) {
		mu.Lock()
		ok := !refused
		mu.Unlock()
		if ok {
			return nc, false, nil
		}
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

// inboxUsable subscribes to this connection's own inbox and reports whether the server
// accepted it. See Connect for why a single Flush is sufficient synchronisation.
func inboxUsable(nc *nats.Conn, prefix string, budget time.Duration) bool {
	sub, err := nc.SubscribeSync(prefix + ".>")
	if err != nil {
		return false
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushTimeout(budget); err != nil {
		// A flush that does not complete says nothing about the grant. Treat it as
		// usable and let the caller's own request timeouts speak: falling back here
		// would silently downgrade a healthy connection to the shared inbox on nothing
		// more than a slow link.
		return true
	}
	return !isInboxRefusal(lastErrText(nc), prefix)
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
