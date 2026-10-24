package agentupgrade

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// IsAuthFailure detects nats-server auth-rejection messages. These come
// across as plain `error` strings (not typed) — match on the substrings
// nats-server emits for the relevant cases (auth_callout deny, expired
// JWT, account mismatch). We deliberately keep the substring set small;
// anything else is treated as transient and retried.
func IsAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, needle := range []string{
		"Authorization Violation",
		"authorization violation",
		"nats: Authorization",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// AttributeDialFailure re-dials each URL of a MULTI-URL pool separately, once, to find
// out whether any of them actually refused the credential.
//
// It returns the auth error when one is found, and nil otherwise (leaving the caller's
// original error in place). nats.Connect's pooled error is the last one it happened to
// see, so a genuine authorization violation on one server is routinely reported as
// ErrNoServers — and every decision downstream that asks "was this an auth failure"
// then answers no.
//
// ONLY on an already-failed pooled connect, and only when there is more than one URL:
// the healthy path never reaches it, and a single-URL pool has nothing to disambiguate.
//
// THE GATE IS "not already an auth error", NOT "is ErrNoServers" — origin:
// prerelease audit round 2, AC-1. The first version keyed on ErrNoServers and
// carried a `// already attributed` comment that was simply false for every
// other error. nats.go only rewrites the pooled error to ErrNoServers when the
// last dial failed with "connection refused" (nats.go@v1.52.0 connect()); the
// ordinary down-broker outcome — host unreachable, firewall DROP, partition —
// is `i/o timeout`, and on that the function returned nil on its first line and
// the denial stayed hidden exactly as it was before AC-F3. It was also
// order-dependent: the same fleet attributed correctly or not depending on
// where in the pool the denying broker happened to sit.
//
// `opts` must be the PROBE option set (probeConnOptions), never the live one.
func AttributeDialFailure(dial string, poolErr error, opts []nats.Option, log *slog.Logger, maxProbes int) error {
	if IsAuthFailure(poolErr) {
		return nil // already attributed: the pooled error IS the denial
	}
	urls := strings.Split(dial, ",")
	if len(urls) < 2 {
		return nil
	}
	// BOUNDED, and it says so when it truncates. Each probe that reaches a live
	// broker costs a full auth_callout decision — an argon2id verify against the
	// broker's process-wide PIN budget — so an agent flapping against a large
	// roster must not turn one failed connect into an unbounded fan-out of them.
	if len(urls) > maxProbes {
		log.Warn("agent: attributing a pooled dial failure over only the first brokers of a larger pool",
			"probed", maxProbes, "pool", len(urls),
			"why", "each probe costs the broker a full auth decision")
		urls = urls[:maxProbes]
	}
	for _, u := range urls {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		nc, err := nats.Connect(u, opts...)
		if err == nil {
			// It works in isolation; nothing to attribute. Close it rather than
			// returning it — the caller's retry owns connection construction.
			nc.Close()
			continue
		}
		if IsAuthFailure(err) {
			return err
		}
	}
	return nil
}

// ProbeConnOptions derives a THROWAWAY option set from a live one.
// origin: prerelease audit round 2, AC-5.
//
// nats.Option values are applied in order and later ones overwrite earlier
// ones, so appending is how a field is un-set. Four things must go:
//
//   - the lifecycle callbacks, because they close over this session's
//     generation and would run armFailClosed / armRedialWatchdog on behalf of a
//     connection nobody will ever use;
//   - reconnection, because a probe that succeeds is closed immediately and a
//     probe that fails must report that, not retry into the background;
//   - RetryOnFailedConnect, for the same reason — it turns "refused" into a
//     connection object that reports its error later, which is precisely the
//     signal this probe exists to read synchronously;
//   - the connect timeout is shortened, since one failed pooled connect now
//     costs several of these in series.
//
// The identity options are deliberately KEPT. An attribution probe that does
// not present the credential proves nothing about whether the credential is
// refused, which is the only question being asked.
func ProbeConnOptions(live []nats.Option, timeout time.Duration) []nats.Option {
	o := append([]nats.Option(nil), live...)
	return append(o,
		nats.ReconnectHandler(nil),
		nats.DisconnectErrHandler(nil),
		nats.ClosedHandler(nil),
		nats.DiscoveredServersHandler(nil),
		nats.NoReconnect(),
		nats.RetryOnFailedConnect(false),
		nats.Timeout(timeout),
	)
}

// WaitBackoff shares cancellable, capped retry growth across CONNECT and REGISTER.
// origin: hot-update S8 internal review FIX-AGENTSLIM.
// waitBackoff sleeps out one retry interval and doubles it, capped at max.
//
// Extracted from connectNATS' three identical copies — origin: prerelease audit round
// 2. AC-2 added a fourth wait site and pushed the function's maintainability index to
// 18; the repo's precedent for that (the `session` intake grouping) is to EXTRACT rather
// than add an exemption, because a maintidx exemption is permanent and an extraction is
// the thing the number was asking for.
//
// The pointer is the point: every caller's next wait must be longer than this one, and
// three hand-written copies of "double it unless it is already at the cap" is exactly
// the shape where one of them quietly stops doubling.
func WaitBackoff(ctx context.Context, backoff *time.Duration, max time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(*backoff):
	}
	if *backoff < max {
		*backoff *= 2
		if *backoff > max {
			*backoff = max
		}
	}
	return nil
}

// AppendIdentityOptions binds the configured credential to the current routing
// name while keeping lifecycle callbacks with their agent session owner.
// origin: hot-update S8 internal review FIX-AGENTSLIM.
func AppendIdentityOptions(opts []nats.Option, sid, nid string, identity *cli.Identity, pin string) []nats.Option {
	if identity == nil {
		// Anonymous fallback: name with `/` separators is intentional
		// — it does NOT match parseRole's `tether-agent:<sid>:<nid>`
		// format, so a misconfigured prod broker (auth_callout ON,
		// Identity nil) fails CONNECT immediately rather than landing
		// on an unintended role.
		opts = append(opts, nats.Name(fmt.Sprintf("tether-agent/%s/%s", sid, nid)))
		return opts
	}

	id := identity
	seed := append([]byte(nil), id.Seed...)
	sigCB := func(nonce []byte) ([]byte, error) {
		kp, err := nkeys.FromSeed(seed)
		if err != nil {
			return nil, fmt.Errorf("agent: nkey from seed: %w", err)
		}
		defer kp.Wipe()
		return kp.Sign(nonce)
	}
	opts = append(opts,
		// The CONNECT name carries the ROUTING name: auth_callout mints
		// PermissionsForAgent from exactly this string, so an instance running
		// under a lease must present the lease name or its JWT would grant the
		// basename's subjects instead of its own. The three-field grammar is
		// unchanged — a fourth segment would fold into parseRole's SplitN and
		// be rejected by ValidateNID on any older broker, turning a rolling
		// upgrade into a hard auth denial.
		nats.Name(cli.AgentName(sid, nid)),
		nats.Nkey(id.PublicKey, sigCB),
		// THE PRIVATE REPLY INBOX IS NOT SET HERE — see auth.InboxPrefixFor. The register
		// reply carries the tunnel token and every subscriber PSK, so it must not land on
		// a subject every other connection on the account may subscribe to
		// (origin: prerelease audit proto-auth-acl/L1-F1) — but the inbox now lives under
		// its own top-level root, which a PRE-CUTOVER broker grants no reach to at all.
		// So the options are added at the DIAL, by natsinbox.Connect, which probes once
		// and falls back to the shared space when the broker is older than this agent.
		// That is the {old broker × new agent} quadrant, and on a real fleet it is what a
		// broker rollback looks like from six already-upgraded machines.
	)
	if pin != "" {
		opts = append(opts, nats.Token(pin))
	}
	return opts
}

// TransientConnectError classifies failures that a broker restart can clear.
// origin: hot-update S8 internal review FIX-AGENTSLIM.
// transientConnectErr reports a CONNECT failure that a restart can clear.
// Under the shipped timeouts (nats.go DefaultTimeout 2 s == nats-server
// AUTH_TIMEOUT 2 s) an absent auth_callout responder surfaces as an i/o
// timeout, not as "Authorization Violation"; only the expanded dial path
// (Timeout 3 s) sees the -ERR. ErrNoServers covers nats-server itself being
// down. TLS errors, "nkeys not supported" and every other -ERR are not
// transient and are returned at once.
func TransientConnectError(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, nats.ErrNoServers) || errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrAuthorization) || strings.Contains(err.Error(), "Authorization Violation")
}
