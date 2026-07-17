# S6–S8 (G-B) external review tasklist

Scope: independently review every unstaged/untracked change in the S6+S8 deploy-tier batch against
`CLAUDE.md`, the simcluster Mandate, `docs/simcluster-coverage-roadmap.md`, the architecture/operations
contracts and the finalized S6–S8 plan. The internal review is a lead index only, never evidence.
Review the final worktree, add only independent review tests/artifacts, and stage every file at handoff.

## Boundary, authority and traceability

- [x] Reconstruct the exact cached/uncached/untracked boundary and ensure the review does not silently inherit
  staged content or omit any concurrent worktree change.
- [x] Read the project requirements/architecture, distributed-broker requirements/architecture, cluster
  operator docs, simcluster Mandate, S6/S8 roadmap cells and the complete S6–S8 plan.
- [x] Map each new drill (`22`, `40`–`43`, `90`–`93`) to its roadmap/plan acceptance cells, open questions,
  Stage-B disposition and claimed gotcha/inventory ownership; flag scope substitutions and unowned deferrals.
- [x] Review the new gotcha ledger entries for numeric collisions, source/behavior accuracy, reproducibility,
  severity, signature pin and flip criteria; reconcile comments/plan/review claims with the ledger SSOT.
- [x] Review the untracked plan and internal-review artifacts themselves for unresolved contradictions,
  knowingly pending work, unsupported completion claims and release-gate omissions.

## Cross-cutting harness and shell safety

- [x] Run `sh -n`/`dash -n`, whitespace checks and ShellCheck if available on every new drill.
- [x] Audit interpreter compatibility, quoting, command substitution, pipelines, redirections, exit-code capture,
  `set -u` behavior and functions invoked through nested `sh -c`.
- [x] Audit every `assert_ok`, `assert_refuses`, `assert_bug`, `poll_until`, manual `_as_pass`, warning and
  NOT-COVERED branch for false-green behavior, swallowed prerequisite failure or over-broad signature.
- [x] Verify setup failures, leader/follower discovery failures, mutation failures and recovery failures gate all
  dependent assertions; diagnostic commands must not mutate or accidentally satisfy their own oracle.
- [x] Verify each destructive arm is isolated, orders preconditions before mutation, cleans up safely, and does
  not let state from an earlier arm contaminate later claims.
- [x] Verify provisioning-only `[env]` actions stay on the harness side of the Mandate; identify any manual
  cluster lifecycle, DB/config rewrite or data-plane repair that compensates for product behavior.
- [x] Verify time-window assertions use bounded polling and causal before/after evidence rather than fixed-sleep
  optimism, stale snapshots, weak counts or unrelated status text.

## S6 drill-specific review

- [x] `22`: verify healthy gates, within-dwell/peer-alive protection, total branch classification, online commit,
  protected-write control, MainPID discrimination, restart-triggered #35 and no hidden split-brain path.
- [x] `40`: verify drain/abort foreground evidence, #31 two-outcome handling, real retire operation identity,
  terminal-state requirement, genuine replicated write/read control and honest #37-family exposure.
- [x] `41`: verify peer-presence refusals, plan zero-write, retire-to-N=1 causality, blocked-reason classification,
  standalone config/post-restart persistence and session/tier-B survival after JetStream reset.
- [x] `42`: verify returning-node cold-start diagnostic, dead/alive diagnose controls, join provenance, O_EXCL on
  intact state, resnapshot/rejoin ordering, permissions and no vacuous roster/phase assertions.
- [x] `43`: verify pre-migration serving precondition and the selected `(a)/(b)/(c)` disposition, check-only
  zero-write proof, machine-confirm controls, live DB/JS/data-plane survival, rollback byte restoration and
  operator-vs-product boundary.

## S8 drill-specific review

- [x] `90`: verify clean alert baselines, leader/non-leader paths, severe banner causality, ack-vs-clear,
  dedup/persistence, quorum-live refusal, below-quorum handling and disk-pressure timing/#39 honesty.
- [x] `91`: verify no intermediate publish between generations, strictly increasing generation, exact roster
  membership, retire-commit-before-seed-drop, force-single survivor convergence and cli failover/anchor claims.
- [x] `92`: verify survivor/control-health preconditions, TFence window measurement, eventual READ-ONLY control,
  destructive `--ack-alerts` semantics, force-single/JS-503 banner causality, data-plane corroboration and
  documented recovery without harness compensation.
- [x] `93`: verify observability YAML round-trip, leader/follower metric rows, HTTP method/status/body contracts,
  webhook exact schema/transition/no-secret whitelist, card/JSON same-report taxonomy, log-json, watch TTY,
  readyz degraded state and all-down exit classification.

## Independent verification and handoff

- [x] Build focused local adversarial probes for the highest-risk shell/oracle paths, including failed setup,
  failed poll, missing JSON fields, warning-only recovery and absent/extra webhook keys.
- [x] Inspect sim server connectivity/resources without changing production state; synchronize through the
  documented remote driver and verify local/server hashes before execution.
- [x] Run the nine new drills with safe isolation/concurrency, retain complete logs/exit summaries, classify each
  RED as harness, infrastructure or product behavior, and retry only signature-matched infrastructure flakes.
- [x] Scan runtime artifacts/logs for secrets and verify no persistent simcluster instance/process residue.
- [x] Run proportional repository tests/lint/document checks required by any shared surfaces touched or newly
  exposed by the review.
- [x] Write a final external-review report beginning with `Pass` or `Fail`, with findings, doubts, evidence,
  coverage gaps and release disposition; never equate harness GREEN with product acceptance automatically.
- [x] Re-read the final diff/report, close every checkbox truthfully, run `git add -A`, and verify there is no
  unstaged/untracked content and the cached scope is complete.
