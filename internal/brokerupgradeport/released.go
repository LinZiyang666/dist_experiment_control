package brokerupgradeport

import "github.com/LinZiyang666/tether/internal/upgradetx"

// Keep every original resource visible even when a hard revoke removed it
// during the final cut. Only the importer may project a positively proven,
// identity-bound terminal disposition; absence or an unbound callback cannot.
// origin: hot-update S8 plan §4.2 / §8.3 M06; S8 internal review P13-COHORT F2
func (p *brokerRuntime) releasedRows(stamp upgradetx.Stamp, rows []upgradetx.Resource, view upgradetx.View, started bool) []upgradetx.Resource {
	port := p.port.Load()
	if !started || p.host.Inherited == nil || !upgradetx.SameCut(p.host.Inherited.Attempt, stamp.Attempt) || port == nil || port.Released == nil {
		return rows
	}
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		seen[string(row.Domain)+"\x00"+row.ID] = true
	}
	for _, row := range active(view.Historical).Scope.Expected {
		if seen[string(row.Domain)+"\x00"+row.ID] || row.Domain != upgradetx.DomainBrokerRuntime || !port.Released(string(row.Domain), row.ID, row.Origin) {
			continue
		}
		row.Owner, row.Generation = stamp.Producer, stamp.Attempt.Generation
		row.State, row.Custody = "terminal", "released"
		rows = append(rows, row)
	}
	return rows
}
