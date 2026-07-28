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
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
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
	// beat (B7), when non-nil, is called once per COMPLETED ITERATION — every event dequeued and put
	// through deliver, whatever the endpoint said.
	//
	// THIS FIELD HAS BEEN BOTH WAYS, AND THE SECOND WAY WAS WRONG (external review B2-4)
	// ---------------------------------------------------------------------------------
	// It originally beat unconditionally while its own comment claimed "per DELIVERED event", so a dead
	// endpoint produced a climbing counter an operator would read as "alerts are going out". Internal
	// review B7-01 caught that and moved the beat behind `deliver() == true`.
	//
	// That fix traded one wrong reading for another. Iters/LastIter are a SHARED contract — loopStat
	// documents them as per-completed-iteration liveness, and ClusterLoopInfo repeats that Iters == 0 on
	// an event-driven loop means "nothing happened". Keyed on acceptance, a perfectly live poster that
	// dequeues events and gets HTTP 401 reports iterations=0 / last_iter=null: indistinguishable from a
	// loop that never ran, in a field whose documented meaning is that it ran. The file even conceded
	// the row "is therefore not a liveness signal on its own", which is a shared type quietly losing its
	// contract in one implementation.
	//
	// One integer cannot answer both questions, so there are now two. beat is liveness. Delivery outcome
	// lives in accepted/rejected below and is reported under its own names, where "the endpoint is
	// refusing us" is a distinct fact from "the consumer stopped consuming".
	beat func()

	// outcomeMu guards accepted+rejected AS ONE STATE, and Stats reads them under it. Their sum IS the
	// published iteration count for this loop (see Stats), so an operator can tell the three states apart:
	//
	//	iterations == 0                      nothing was ever dequeued (nothing fired, or wedged)
	//	iterations > 0, rejected == 0         alerts are going out
	//	iterations > 0, accepted == 0         the consumer is alive and the ENDPOINT is refusing
	//
	// The third row is the state the pre-B2-4 shape rendered as the first one.
	//
	// WHY A MUTEX AND NOT TWO ATOMICS (external review RB2-4)
	// -------------------------------------------------------
	// They WERE two atomics, incremented before the liveness beat, while RuntimeReport read the webhook
	// counters and the loop's iteration counter in separate operations. A snapshot taken between those
	// two reads published `accepted+rejected = 1` alongside `iterations = 0` — a state the documented
	// contract says cannot exist. Reordering the writes only moves the tear to the other side; two
	// independently-published numbers cannot be made equal by ordering.
	//
	// So the two facts are now ONE state, read once. `drops` stays atomic on purpose: it is incremented
	// from the CALLER's goroutine inside Post(), which must never block on the delivery loop, and it is
	// deliberately outside the accepted+rejected identity anyway (a dropped event never became an
	// iteration).
	outcomeMu sync.Mutex
	accepted  int64
	rejected  int64
	lastIter  time.Time
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
			// ONE state transition per completed iteration (external review B2-4 then RB2-4). The
			// iteration is complete once deliver returns, whatever it returned — that is what Beat means
			// on every other loop in the set, and a shared field may not mean something else here.
			p.recordOutcome(p.deliver(ctx, ev))
		}
	}
}

// deliver POSTs one event and reports whether the endpoint ACCEPTED it (a non-error response below 400).
//
// The bool feeds the OUTCOME counters, not the liveness beat — the beat fires on every completed
// iteration regardless (external review B2-4; this sentence used to say the beat was keyed on the bool,
// which RB2-4 flagged as stale after that fix). Every failure path here is a silent `return` from the
// loop's point of view — marshal, request build, transport, and an HTTP error status are all logged and
// swallowed, which is correct for a best-effort notifier but means "the loop processed an event" and
// "an alert reached its destination" are DIFFERENT facts. An operator reading a climbing iteration
// counter must not be able to conclude the second from the first; that is what accepted/rejected are for.
func (p *webhookPoster) deliver(ctx context.Context, ev WebhookEvent) bool {
	buf, err := json.Marshal(webhookPayloadFor(ev))
	if err != nil {
		p.logger.Warn("alert webhook: marshal", "err", err)
		return false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(buf))
	if err != nil {
		p.logger.Warn("alert webhook: build request", "err", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		p.logger.Warn("alert webhook: POST failed", "err", err, "key", ev.DedupKey)
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		p.logger.Warn("alert webhook: endpoint returned error status", "status", resp.StatusCode, "key", ev.DedupKey)
		return false
	}
	return true
}

// Drops returns the number of events dropped due to a full queue (test + ops counter).
func (p *webhookPoster) Drops() int64 { return p.drops.Load() }

// webhookLoopName is the loopSet key for the poster's drain goroutine. It is a constant because
// RuntimeReport has to recognise that ROW to publish its iteration count from the poster's coherent
// snapshot (RB2-4) — a second string literal there would silently stop matching after a rename and the
// tear would come back with no test failing.
const webhookLoopName = "alert-webhook"

// recordOutcome closes out one completed iteration: the delivery verdict and the liveness beat happen
// as a single state transition, so no reader can observe one without the other.
//
// The beat is called OUTSIDE outcomeMu, deliberately. Holding a lock across a caller-supplied callback
// would (a) make any Stats() reader block for as long as that callback runs, and (b) create an
// outcomeMu -> loopSet-mutex ordering for no benefit — because the published equality is NOT achieved by
// serialising the two writes. It is achieved by there being only ONE counter pair: Stats derives the
// iteration count from accepted+rejected, so a reader landing between the increment and the beat sees a
// consistent pair either way. The beat's remaining job is LastIter.
func (p *webhookPoster) recordOutcome(delivered bool) {
	p.outcomeMu.Lock()
	if delivered {
		p.accepted++
	} else {
		p.rejected++
	}
	p.lastIter = time.Now()
	p.outcomeMu.Unlock()
	if p.beat != nil {
		p.beat()
	}
}

// Stats snapshots the delivery outcome, derived iteration count, and completion timestamp in ONE read
// of ONE state (external review RB2-4 / second re-review SRB2-4).
//
// The second return value is `Accepted + Rejected` BY CONSTRUCTION rather than by convention, and the
// third was written in the same critical section. RuntimeReport uses all three for the alert_webhook
// block and row, so neither the count nor LastIter can tear against the outcome.
//
// Drops is counted at Post() on the ENQUEUE side and is deliberately outside that sum: a dropped event
// never reached the loop, so counting it as an iteration would claim work that never happened.
func (p *webhookPoster) Stats() (adminsock.AlertWebhookStats, uint64, time.Time) {
	p.outcomeMu.Lock()
	accepted, rejected, lastIter := p.accepted, p.rejected, p.lastIter
	p.outcomeMu.Unlock()
	return adminsock.AlertWebhookStats{
		Accepted: accepted,
		Rejected: rejected,
		Drops:    p.drops.Load(),
	}, uint64(accepted + rejected), lastIter
}
