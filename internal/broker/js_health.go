package broker

import "github.com/LinZiyang666/tether/internal/brokercontrol"

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM
const jsErrCodeUnavailable = brokercontrol.JsErrCodeUnavailable

func classifyJSUnavailable(err error) bool { return brokercontrol.ClassifyJSUnavailable(err) }
