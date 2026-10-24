package infrarepair

import (
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"sort"
)

// RetireUnknownProducers includes dynamically registered original transfers,
// rather than only the static broker roots. A successful store read proves an
// effect, not that a timed-out sender can no longer issue a late write. Each
// retained producer must run its own real transport retirement before re-Seal.
// origin: hot-update S9 plan §4.3 / T40 / T42; NC-EXPORTS ACK registration
func RetireUnknownProducers(g *Gate, d c.Deadline, stop c.Stop) error {
	if err := admission(d, stop); err != nil {
		return err
	}
	if g == nil {
		return c.ErrIncomplete
	}
	g.mu.Lock()
	if !g.closed || g.active != 0 {
		g.mu.Unlock()
		return ErrUnknown
	}
	names := make([]string, 0, len(g.unknown))
	for name := range g.unknown {
		names = append(names, name)
	}
	g.mu.Unlock()
	sort.Strings(names)
	for _, name := range names {
		if err := g.RetireUnknown(name, d, stop); err != nil {
			return err
		}
	}
	return nil
}
