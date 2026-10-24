package broker

import "github.com/LinZiyang666/tether/internal/brokercontrol"

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM
type rehomedExpose = brokercontrol.RehomedExpose

const codeDataplaneNotConverged = brokercontrol.CodeDataplaneNotConverged

type ErrDataPlaneNotConverged = brokercontrol.ErrDataPlaneNotConverged
