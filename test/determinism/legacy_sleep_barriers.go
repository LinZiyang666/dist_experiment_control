package determinism

// legacy_sleep_barriers.go — the DRAINING LEDGER for TestReadinessHelpersDoNotSleepAsABarrier.
//
// Every entry is a startBroker / startAgent / seedSession helper that, on 2026-09-01, still used a
// bare time.Sleep as its readiness barrier (docs/testing-standards.md T1; parallel-flake-rootcause
// root cause 3). They are not rewritten here — root-cause analysis attributed no flake to a sleep
// OUTSIDE these helpers and a 13-suite rewrite would be churn without an incident — but they are
// frozen: the next person who touches one of these helpers replaces the sleep with the observable
// the test actually depends on (testharness.WaitNodeOnline; a short nc.Request against the broker's
// subject; a DB predicate — NOT WaitConnect, which only proves NATS accepts connections) and deletes
// the line here, or marks it `// sleep-fixture: <why>` if the sleep is genuinely the fixture.
//
// ON THE NUMBER 19: the plan estimated 21 from an awk over function bodies; the AST scan excludes
// sleeps inside polling loops and func literals, which is the correct reading of "barrier", and finds
// 19. test/p8's startBrokerManual / startAgentManual deliberately control start/stop ordering and are
// expected to stay here longest.
//
// KEYED "<path>: <func>". Only ever REMOVED (legacySleepBarriersCap); a stale entry FAILS the gate.

// S6 TESTFIX replaces five startup sleeps with subscription/registration barriers.
// S7 MAKETEST-TRANSFER replaced three more with real readiness barriers
// (cli_e2e startBroker, p4 startAgentSecure, p7 startAgent).
// origin: hot-update S10 lint L3a — the unused upgrade startup helper was removed.
const legacySleepBarriersCap = 10

var legacySleepBarriers = map[string]bool{
	"test/chaos/chaos_harness_test.go: startBrokerExplicitCfg": true,
	"test/concurrency/helpers_test.go: startBrokerNoTunnel":    true,
	"test/p10/upgrade_e2e_test.go: startAgentWithUpgrade":      true,
	"test/p4/exec_e2e_test.go: startAgent":                     true,
	"test/p7/audit_e2e_test.go: startBroker":                   true,
	"test/p8/reconcile_e2e_test.go: startAgent":                true,
	"test/p8/reconcile_e2e_test.go: startAgentManual":          true,
	"test/p8/reconcile_e2e_test.go: startBroker":               true,
	"test/p8/reconcile_e2e_test.go: startBrokerManual":         true,
	"test/p9/admin_e2e_test.go: startAgent":                    true,
}
