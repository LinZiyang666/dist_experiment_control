package schema

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"
)

func cap0Row(kind, verb, tier string) AuditTransfer {
	rec := AuditTransfer{V: AuditSchemaVersion, Kind: kind, Verb: verb, Ts: time.Unix(1_700_000_000, 0).UTC(),
		Session: "lab", Node: "lab-1", ActorNkey: "UACTOR", ActorFp: "fp", TransferID: "tid-cap0", Path: "/dst", Tier: tier,
		EntryNonce: "NONCE", BestEffort: true}
	if kind != "start" {
		rec.Bytes = 4
		rec.TerminalID = TransferTerminalID(rec.Session, rec.TransferID, rec.EntryNonce)
	}
	return rec
}

// A cap0 row retains the broker's admission nonce and one terminal slot, but no
// receiver authority: it validates on the exact-history route without ever
// presenting an agent, intent or object identity, and a partial row of either
// shape is still rejected before it can reach history.
// origin: hot-update S7 plan §4 / §5.2 / §9 / L12 (S7 integration oplegacy F2)
func TestCanonicalTransferAuditAcceptsBestEffortCap0WithoutV1Authority(t *testing.T) {
	for _, rec := range []AuditTransfer{
		cap0Row("start", "push", "a"), cap0Row("complete", "push", "a"), cap0Row("failed", "push", "b"),
		cap0Row("start", "pull", "?"), cap0Row("complete", "pull", "b"), cap0Row("failed", "pull", "a"),
	} {
		canonical, err := CanonicalTransferAudit(rec)
		if err != nil || !TransferContinuation(rec) {
			t.Fatalf("%s/%s/%s cap0 row rejected: %v", rec.Verb, rec.Kind, rec.Tier, err)
		}
		id := canonical.Identity
		if id.AgentInstance != "" || id.IntentDigest != "" || id.ObjectVersion != "" || id.EntryNonce != "NONCE" ||
			!bytes.Contains(canonical.Bytes, []byte(`"best_effort":true`)) {
			t.Fatalf("cap0 canonical identity certified v1 authority or lost its mark: %+v", id)
		}
		if _, _, err := TransferAuditPayload(rec); err != nil {
			t.Fatalf("the Raft envelope refused a valid cap0 row: %v", err)
		}
	}
	for name, change := range map[string]func(*AuditTransfer){
		"agent authority":       func(r *AuditTransfer) { r.AgentInstance = "agent" },
		"intent authority":      func(r *AuditTransfer) { r.IntentDigest = "intent" },
		"object authority":      func(r *AuditTransfer) { r.ObjectVersion = "object" },
		"no admission nonce":    func(r *AuditTransfer) { r.EntryNonce = "" },
		"wrong terminal slot":   func(r *AuditTransfer) { r.TerminalID = strings.Repeat("0", 64) },
		"missing terminal ID":   func(r *AuditTransfer) { r.TerminalID = "" },
		"unresolved terminal":   func(r *AuditTransfer) { r.Tier = "?" },
		"unknown tier":          func(r *AuditTransfer) { r.Tier = "c" },
		"unknown verb":          func(r *AuditTransfer) { r.Verb = "copy" },
		"no actor":              func(r *AuditTransfer) { r.ActorFp = "" },
		"false outcome unknown": func(r *AuditTransfer) { r.OutcomeUnknown = true },
		"unmarked identity":     func(r *AuditTransfer) { r.BestEffort = false },
	} {
		rec := cap0Row("complete", "pull", "b")
		change(&rec)
		if _, err := CanonicalTransferAudit(rec); err == nil {
			t.Fatalf("%s: malformed cap0 row validated", name)
		}
		if _, _, err := TransferAuditPayload(rec); err == nil {
			t.Fatalf("%s: malformed cap0 row reached the Raft envelope", name)
		}
	}
	for name, rec := range map[string]AuditTransfer{
		"push START unresolved tier": cap0Row("start", "push", "?"),
		"START with terminal slot":   func() AuditTransfer { r := cap0Row("start", "pull", "?"); r.TerminalID = "x"; return r }(),
		"lone best-effort mark": {V: AuditSchemaVersion, Kind: "failed", Verb: "push", Ts: time.Unix(1, 0), Session: "lab",
			Node: "lab-1", ActorFp: "fp", TransferID: "tid", Tier: "a", BestEffort: true},
	} {
		if !TransferContinuation(rec) {
			t.Fatalf("%s: a marked row would fall through to legacy bounded loss", name)
		}
		if _, _, err := TransferAuditPayload(rec); err == nil {
			t.Fatalf("%s: malformed row reached the Raft envelope", name)
		}
	}
}

// Binding never mints: an empty nonce leaves the pre-S7 bytes untouched, and a
// complete and a failed decision of one admission compete for one slot.
// origin: hot-update S7 plan §4 / L12 (S7 integration oplegacy F2)
func TestBindLegacyTerminalUsesTheAdmissionSlot(t *testing.T) {
	plain := cap0Row("complete", "push", "a")
	plain.EntryNonce, plain.TerminalID, plain.BestEffort = "", "", false
	if got := BindLegacyTerminal(plain, ""); !reflect.DeepEqual(got, plain) || TransferContinuation(got) {
		t.Fatalf("an empty admission nonce changed a pre-S7 terminal: %+v", got)
	}
	complete := BindLegacyTerminal(plain, "NONCE")
	failed := plain
	failed.Kind, failed.Code = "failed", "late"
	failed = BindLegacyTerminal(failed, "NONCE")
	if complete.TerminalID == "" || complete.TerminalID != failed.TerminalID || !complete.BestEffort || complete.EntryNonce != "NONCE" {
		t.Fatalf("opposite decisions of one admission did not share its terminal slot: %+v %+v", complete, failed)
	}
	if _, err := CanonicalTransferAudit(complete); err != nil {
		t.Fatalf("a bound terminal is not canonical: %v", err)
	}
	if other := BindLegacyTerminal(plain, "OTHER"); other.TerminalID == complete.TerminalID {
		t.Fatal("a different admission reused the terminal slot")
	}
}
