package architecture

// legacy_gates_without_controls.go — the DRAINING LEDGER for TestEveryGateNamesItsControl.
//
// Every entry is a gate file CLAUDE.md's table names that carried no `// gate-control:` anchor when
// the rule landed (2026-09-01). Most of them DO have a self-check test already (the G2 discipline was
// real); what they lack is the one-line pointer that says which test it is — and a few (the
// evidence gates around spawn, the structural budget) have controls spread over several tests with
// no single one to point at, which is itself worth a look when the entry is drained. Draining an
// entry is: read the file, find (or write) the positive/negative control, add the anchor above it,
// delete the line here, lower the cap.
//
// Plan B4 named four small gates to anchor on the day (docs_layout, nolint_directive, ci_workflow,
// simcluster_log_oracle). The first landing did three — docs_layout and nolint_directive by pointing
// at their EXISTING self-checks, ci_workflow by gaining a parser control — and left
// simcluster_log_oracle in this ledger while the header said "four" (internal review L5-F9 / L6-F8).
// The log oracle got its predicates and control in the same review and left the ledger; the 12 below
// are the remainder. Every new gate of the increment carries an anchor and was never here.
//
// KEYED by gate FILE — the unit is one anchor per file. Only ever REMOVED; a stale entry (the file
// gained an anchor, or left the table) FAILS the gate.

const legacyGatesWithoutControlsCap = 12

var legacyGatesWithoutControls = map[string]bool{
	"cmd/tether/command_tree_inventory_test.go":      true,
	"internal/auth/acl_reconcile_test.go":            true,
	"internal/proto/wire_inventory_test.go":          true,
	"test/architecture/dataplane_lifetime_test.go":   true,
	"test/architecture/gate_registry_test.go":        true,
	"test/architecture/layering_test.go":             true,
	"test/architecture/spawn_exec_isolation_test.go": true,
	"test/architecture/spawn_stall_evidence_test.go": true,
	"test/architecture/structural_budget_test.go":    true,
	"test/architecture/tls_verify_pairing_test.go":   true,
	"test/concurrency/helpers_test.go":               true,
	"test/determinism/enum_switch_default_test.go":   true,
}
