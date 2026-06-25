package broker

import "testing"

// TestF10ScrubIsBestEffortNotGuarantee — External-review F10: the scrub redacts secret-SHAPED keys
// but a secret VALUE under an innocent key survives. This pins the documented limitation so the
// "best-effort" wording stays honest (and a future allowlist/value-pattern upgrade has a baseline).
func TestF10ScrubIsBestEffortNotGuarantee(t *testing.T) {
	body := map[string]any{
		"token":   "SECRET-A",             // secret-shaped key → redacted
		"message": "login token=SECRET-B", // innocent key, secret value → NOT redacted (the limitation)
	}
	got := scrubAuditBody(body)
	if got["token"] != "[redacted]" {
		t.Fatalf("a secret-shaped key MUST be redacted, got %v", got["token"])
	}
	if got["message"] != "login token=SECRET-B" {
		t.Fatalf("F10: an innocent key's value is NOT scrubbed (best-effort only) — wording must reflect this; got %v", got["message"])
	}
}
