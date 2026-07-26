package brokermetrics

import (
	"bytes"
	"strings"
	"testing"
)

// External review (gate quality #5). The three audit-loss series and the
// cluster_loops export were added with no test pinning their NAME, their TYPE,
// or that the value reaches the exposition at all. A scraper contract that only
// "compiles" is not a contract: renaming a series, or leaving it wired to a
// field nothing sets, breaks every dashboard silently.
func TestAuditLossSeriesContract(t *testing.T) {
	var buf bytes.Buffer
	Render(&buf, Snapshot{
		ClusterMode:            true,
		AuditTruncationLoss:    7,
		AuditLagExceeded:       11,
		AuditDeletedStreamLoss: 13,
	})
	out := buf.String()

	for _, tc := range []struct {
		name  string
		value string
	}{
		{"tether_broker_audit_truncation_loss_total", "7"},
		{"tether_broker_audit_lag_exceeded_total", "11"},
		{"tether_broker_audit_deleted_stream_loss_total", "13"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(out, "# TYPE "+tc.name+" counter") {
				t.Errorf("%s is not exported as TYPE counter; the _total suffix is the Prometheus "+
					"counter convention and rate()/increase() depend on the declared type", tc.name)
			}
			if !strings.Contains(out, tc.name+" "+tc.value) {
				t.Errorf("%s did not carry its snapshot value %s — the field is exported but not wired",
					tc.name, tc.value)
			}
		})
	}
}

// TestAuditLossSeriesAlwaysPresentInClusterMode: emitting 0 beats omitting the
// series, because a missing series and a zero one look identical to an alerting
// rule only if the rule was written defensively — and most are not.
func TestAuditLossSeriesAlwaysPresentInClusterMode(t *testing.T) {
	var buf bytes.Buffer
	Render(&buf, Snapshot{ClusterMode: true})
	out := buf.String()
	for _, name := range []string{
		"tether_broker_audit_truncation_loss_total",
		"tether_broker_audit_lag_exceeded_total",
		"tether_broker_audit_deleted_stream_loss_total",
	} {
		if !strings.Contains(out, name+" 0") {
			t.Errorf("%s is absent at zero; a series that appears only after the first loss cannot be "+
				"alerted on before it matters", name)
		}
	}
}
