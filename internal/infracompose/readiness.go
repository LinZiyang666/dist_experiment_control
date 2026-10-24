package infracompose

import c "github.com/LinZiyang666/tether/internal/infracontract"

// A single Dependencies value must still dispatch readiness by responsibility.
// The NATS probe intentionally refuses Caddy, and /load remains a separate
// authenticated admin operation rather than an implicit Host fallback.
// origin: hot-update S9 plan §3.2 / §5.1 / P22
type componentReadiness struct {
	nats c.ReadyProbe
	CaddyReadiness
}

func (r componentReadiness) Read(p c.Plan, d c.Deadline, stop c.Stop) (c.Readiness, error) {
	switch p.Component {
	case c.NATS:
		return r.nats.Read(p, d, stop)
	case c.Caddy:
		return r.CaddyReadiness.Read(p, d, stop)
	case c.HostSupport:
		return c.Readiness{}, c.ErrUnsupported
	default:
		return c.Readiness{}, c.ErrUnsupported
	}
}
