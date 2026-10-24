package broker

import (
	"log/slog"

	"github.com/LinZiyang666/tether/internal/brokercontrol"
)

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM
const webhookQueueCap = brokercontrol.WebhookQueueCap

type webhookPayload = brokercontrol.WebhookPayload

func webhookPayloadFor(ev WebhookEvent) webhookPayload { return brokercontrol.WebhookPayloadFor(ev) }

type WebhookEvent = brokercontrol.WebhookEvent
type webhookPoster = brokercontrol.WebhookPoster

func parseWebhookURL(raw string) (string, error) { return brokercontrol.ParseWebhookURL(raw) }
func newWebhookPoster(rawURL string, logger *slog.Logger) (*webhookPoster, error) {
	return brokercontrol.NewWebhookPoster(rawURL, logger)
}

const webhookLoopName = brokercontrol.WebhookLoopName
