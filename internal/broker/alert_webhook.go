package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

// alert_webhook.go — the B6 OPS#2 alert webhook (leader-side, transition-gated POST). It is fed
// off the reconciler's COMMITTED-delta seam (alert_reconcile.go): the reconciler diffs the
// committed ACTIVE set at the top of each pass and calls Post on each genuine raised/cleared
// transition. So a hung endpoint can never stall the reconcile pass or the applyMu-held propose —
// Post is a NON-BLOCKING enqueue onto a bounded channel; a single drain goroutine owns the HTTP.
//
// Body carries only PUBLIC topology (the same fields cluster status / alert ls render) — never a
// secret. The URL is operator-trusted: http AND https are accepted (the common deployment is an
// internal http://10.x:9093 alertmanager); the load-bearing guard is rejecting userinfo (no
// secret-in-URL) + a non-HTTP scheme. Private-IP/metadata blocking is deliberately NOT done
// (the operator owns the URL — security-pragmatic).

const webhookQueueCap = 64

// webhookSchemaName / webhookSchemaVersion identify the on-the-wire alert webhook contract. A consumer
// keys off (schema, schema_version) to authenticate the payload shape and reject a forged / wrong-version
// body. Bump webhookSchemaVersion on ANY incompatible field change.
const (
	webhookSchemaName    = "tether_alert_webhook"
	webhookSchemaVersion = 1
)

// webhookPayload is the EXACT on-the-wire JSON contract POSTed for each alert transition. It is the single
// serialization SSOT (was an ad-hoc map[string]any). SECURITY INVARIANT: every field is a fixed, public,
// whitelisted key carrying only cluster topology (the same fields `cluster status` / `alert ls` render) —
// it MUST NEVER carry a secret (no PIN, nkey, seed, token, cert, account key, password). b6_webhook_test
// pins the key set to exactly this whitelist + a no-secret scan, and proves an adversarial alert string
// cannot smuggle extra keys (JSON-injection safe).
type webhookPayload struct {
	Schema        string `json:"schema"`
	SchemaVersion int    `json:"schema_version"`
	Transition    string `json:"transition"` // "raised" | "cleared"
	Kind          string `json:"kind"`
	Severity      string `json:"severity"`
	DedupKey      string `json:"dedup_key"`
	Message       string `json:"message"`
	Node          string `json:"node,omitempty"` // omitted when empty (cluster-wide alert)
	ClusterLeader string `json:"cluster_leader"`
	Ts            string `json:"ts"`
}

// webhookPayloadFor builds the wire payload from an alert transition. Stamps the schema identity so the
// receiver can verify shape + version.
func webhookPayloadFor(ev WebhookEvent) webhookPayload {
	return webhookPayload{
		Schema:        webhookSchemaName,
		SchemaVersion: webhookSchemaVersion,
		Transition:    ev.Transition,
		Kind:          ev.Kind,
		Severity:      ev.Severity,
		DedupKey:      ev.DedupKey,
		Message:       ev.Message,
		Node:          ev.Node,
		ClusterLeader: ev.ClusterLeader,
		Ts:            ev.Ts,
	}
}

// WebhookEvent is one alert transition to POST.
type WebhookEvent struct {
	Transition    string // "raised" | "cleared"
	Kind          string
	Severity      string
	DedupKey      string
	Message       string
	Node          string
	ClusterLeader string
	Ts            string
}

type webhookPoster struct {
	url    string
	client *http.Client
	ch     chan WebhookEvent
	drops  atomic.Int64
	logger *slog.Logger
}

// parseWebhookURL validates an operator-supplied webhook URL: http/https only, NO userinfo (the
// load-bearing no-secret-in-URL guard), and a non-empty host.
func parseWebhookURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("alert webhook url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("alert webhook url scheme %q not allowed (http/https only)", u.Scheme)
	}
	if u.User != nil {
		return "", fmt.Errorf("alert webhook url must not contain userinfo (no secret-in-URL)")
	}
	if u.Host == "" {
		return "", fmt.Errorf("alert webhook url has no host")
	}
	return raw, nil
}

func newWebhookPoster(rawURL string, logger *slog.Logger) (*webhookPoster, error) {
	clean, err := parseWebhookURL(rawURL)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &webhookPoster{
		url: clean,
		client: &http.Client{
			Timeout: 5 * time.Second,
			// External-review Q2: defense-in-depth against SSRF via redirect. The webhook URL is
			// operator-configured (trusted), but a compromised/misbehaving endpoint must not be able
			// to BOUNCE the POST (which carries no secret, but could be aimed at a cloud metadata /
			// private endpoint) to a DIFFERENT host. Refuse any cross-host redirect.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) > 0 && req.URL.Host != via[0].URL.Host {
					return fmt.Errorf("alert webhook: refusing cross-host redirect %s -> %s", via[0].URL.Host, req.URL.Host)
				}
				if len(via) >= 5 {
					return errors.New("alert webhook: too many redirects")
				}
				return nil
			},
		},
		ch:     make(chan WebhookEvent, webhookQueueCap),
		logger: logger,
	}, nil
}

// Post is a NON-BLOCKING enqueue: a full queue (a hung/slow endpoint backing up) drops the event
// + bumps the drop counter rather than blocking the reconcile pass.
func (p *webhookPoster) Post(ev WebhookEvent) {
	select {
	case p.ch <- ev:
	default:
		n := p.drops.Add(1)
		p.logger.Warn("alert webhook: queue full, dropping event", "drops", n, "key", ev.DedupKey, "transition", ev.Transition)
	}
}

// Run drains the queue until ctx is cancelled. A single goroutine owns the HTTP client, so a
// hung endpoint stalls only this goroutine's next send — never the reconciler.
func (p *webhookPoster) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-p.ch:
			p.deliver(ctx, ev)
		}
	}
}

func (p *webhookPoster) deliver(ctx context.Context, ev WebhookEvent) {
	buf, err := json.Marshal(webhookPayloadFor(ev))
	if err != nil {
		p.logger.Warn("alert webhook: marshal", "err", err)
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(buf))
	if err != nil {
		p.logger.Warn("alert webhook: build request", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		p.logger.Warn("alert webhook: POST failed", "err", err, "key", ev.DedupKey)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		p.logger.Warn("alert webhook: endpoint returned error status", "status", resp.StatusCode, "key", ev.DedupKey)
	}
}

// Drops returns the number of events dropped due to a full queue (test + ops counter).
func (p *webhookPoster) Drops() int64 { return p.drops.Load() }
