// Register waits preserve a captured writer/authority identity across retries.
// Snapshot and settlement callbacks keep courier state inside its owning package.
// origin: hot-update S8 plan §3.3 / §4.2 / §12.1 / P14.
package agentupgrade

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradetx"

	"github.com/nats-io/nats.go"
)

type RegisterPort struct {
	Request                         proto.NodeRegisterReq
	Subject                         string
	Logger                          *slog.Logger
	Timeout, RetryInitial, RetryMax time.Duration
	Marshal                         func(proto.NodeRegisterReq) ([]byte, error)
	Snapshot                        func(*proto.NodeRegisterReq) func(proto.NodeRegisterResp)
	Authority                       func([]byte) error
	Connected                       func(string, string, proto.NodeRegisterResp)
	Revoke                          func(string)
	SessionGone                     error
	WaitBackoff                     func(context.Context, *time.Duration, time.Duration) error
}

func Register(ctx context.Context, nc *nats.Conn, p RegisterPort) (proto.NodeRegisterResp, error) {
	req, subject := p.Request, p.Subject
	backoff := p.RetryInitial
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return proto.NodeRegisterResp{}, err
		}

		// A retry may follow an automatic reconnect to a different NATS server.
		// Bind both the request and the identity reply to this attempt's route.
		settle := p.Snapshot(&req)
		req.ServerID = nc.ConnectedServerName()
		routeURL := nc.ConnectedUrl()
		payload, err := p.Marshal(req)
		if err != nil {
			return proto.NodeRegisterResp{}, fmt.Errorf("agent: marshal register: %w", err)
		}
		reqCtx, cancel := context.WithTimeout(ctx, p.Timeout)
		msg, err := nc.RequestWithContext(reqCtx, subject, payload)
		cancel()

		if err == nil {
			var resp proto.NodeRegisterResp
			switch {
			case json.Unmarshal(msg.Data, &resp) != nil:
				// Garbled reply — treat as transient (broker bug or
				// concurrent partial deploy). Retry.
				p.Logger.Warn("agent: register reply parse failed; retrying",
					"attempt", attempt)
			case resp.OK:
				if err := p.Authority(msg.Data); err != nil {
					p.Revoke(upgradetx.CodeSessionIncarnationMismatch)
					return proto.NodeRegisterResp{}, err
				}
				p.Connected(req.ServerID, routeURL, resp)
				if attempt > 1 {
					p.Logger.Info("agent: register succeeded after retry",
						"attempts", attempt)
				}
				// A contested reply carries no reconciliation or accepted processes.
				// origin: hot-update S8 plan §4.2 / M05.
				if resp.Lease == nil {
					settle(resp)
				}

				return resp, nil
			case resp.Code == proto.CodeLeaderUnavailable:
				// audit M2 / write-forward F3: a leadership race (raft failover / election) is
				// TRANSIENT, not a config error — retry with backoff instead of exiting the agent
				// process. break exits the switch and falls through to the backoff sleep below.
				p.Logger.Warn("agent: register hit a transient leader failover; retrying",
					"attempt", attempt, "next_backoff", backoff)
			case resp.Code == proto.CodeReplyTooLarge:
				// h1 A2 (plan critique-4 MAJOR): an oversize register REPLY is a
				// broker-side bug, not this agent's config. Pre-h1 the oversize
				// reply was silently dropped and register self-healed via the
				// timeout/retry path; letting it fall into the authoritative-reject
				// default below would instead EXIT the agent — converting a
				// self-healing condition into fleet-node death. Keep the
				// self-healing shape: retry with backoff.
				p.Logger.Warn("agent: register reply exceeded broker max_payload (broker bug); retrying",
					"attempt", attempt, "next_backoff", backoff, "detail", resp.Error)
			case resp.Code == proto.CodeSessionIncarnationMismatch:
				// origin: S8 internal review FIX-AGENTRACE-B F1 / hot-update S8 plan §6.2.
				// The guarded authority writer refused this agent's inherited
				// incarnation before any replacement-session write: the session it
				// served was deleted and recreated. That is an authority loss like a
				// successful reply naming another incarnation (the Authority arm
				// above), so the old data plane is revoked here, before the caller
				// sees the error, and the rejection is final (never retried).
				p.Revoke(resp.Code)
				return proto.NodeRegisterResp{}, fmt.Errorf("agent: register rejected (code=%s): %s", resp.Code, resp.Error)
			case resp.Code == proto.CodeSessionNotFound || resp.Code == proto.CodeSessionNotFoundOrDeleting:
				// origin: hot-update S3 plan §3.3. A deletion in the exec window
				// may lose its event; the register authority must revoke it too.
				p.Revoke(resp.Code)
				return proto.NodeRegisterResp{}, fmt.Errorf("%w: %s", p.SessionGone, resp.Code)
			default:
				// Authoritative reject from broker. Don't retry; the operator
				// must fix config (proto, nid uniqueness, etc.).
				return proto.NodeRegisterResp{}, fmt.Errorf("agent: register rejected (code=%s): %s",
					resp.Code, resp.Error)
			}
		} else {
			// Parent ctx canceled mid-request — exit cleanly.
			if ctx.Err() != nil {
				return proto.NodeRegisterResp{}, ctx.Err()
			}
			p.Logger.Warn("agent: register attempt failed; retrying",
				"attempt", attempt, "err", err, "next_backoff", backoff)
		}

		// Sleep remains cancellable and uses the same bounded growth as CONNECT.
		if err := p.WaitBackoff(ctx, &backoff, p.RetryMax); err != nil {
			return proto.NodeRegisterResp{}, err
		}
	}
}
