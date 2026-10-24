package broker

import "github.com/LinZiyang666/tether/internal/brokercontrol"

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM

type AlertReconcilerConfig = brokercontrol.AlertReconcilerConfig
type AlertReconciler = brokercontrol.AlertReconciler

const jsDownThreshold = brokercontrol.JsDownThreshold

func NewAlertReconciler(cfg AlertReconcilerConfig) *AlertReconciler {
	return brokercontrol.NewAlertReconciler(cfg)
}
