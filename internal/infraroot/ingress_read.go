package infraroot

import (
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/ingressswitch"
)

type ingressReader struct {
	switcher *ingressswitch.Switch
	facts    infrahost.HostFacts
}

func (r *ingressReader) Inspect(id c.ID, d c.Deadline) (c.Census, error) {
	return r.facts.Inspect(id, d)
}
func (r *ingressReader) Do(q c.HostRequest, d c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	return ingressswitch.VerifyCurrent(r.switcher, r.facts.Inspect, q, d, stop)
}
