# S3–S5 external review tasklist

> External reviewer working checklist, 2026-07-12. Scope is every unstaged and untracked
> file present when this review started. Internal review results are treated as leads only;
> every conclusion below requires independent source inspection or reproduced evidence.\n> Completed 2026-07-12; detailed evidence and exceptions are in `s3-s5-external-review.md`. `shellcheck` was unavailable, and no implementation file was changed by the external reviewer.

## 1. Baseline and review contract

- [x] Read `CLAUDE.md` and identify the simcluster mandate, phase workflow, and test gates.
- [x] Inventory unstaged/untracked files and confirm that the staged diff was empty at review start.
- [x] Read the authoritative architecture/operations sections for expose, proxy, upgrade,
  cluster membership, install layout, and simcluster execution.
- [x] Read the S3–S5 plan, internal review, coverage inventory, roadmap, gotcha ledger, and
  representative prior external-review/tasklist reports; record any conflicting claims.
- [x] Freeze the exact review scope (`git status`, diff names, untracked files) and ensure the
  final report distinguishes product findings, harness findings, documentation findings,
  infrastructure flakes, and explicitly uncovered behavior.

## 2. Change-map and shell/harness integrity

- [x] Inspect all eight new drills (30/31/32/70/71/72/73/74) line by line.
- [x] Inspect shared helpers (`artifact.sh`, `cluster.sh`, `dataplane.sh`, `proxy.sh`) and every
  modified existing helper/unit/image/remote runner call site.
- [x] Inspect every untracked spike script; decide whether it is intentional durable evidence,
  misleading dead debug code, or should be excluded from the deliverable.
- [x] Run shell syntax checks on every changed/new shell script and ShellCheck where available.
- [x] Check POSIX `/bin/sh` compatibility: local-variable leakage, quoting, word splitting,
  pipelines/subshell state, `set -e` behavior, traps, command substitutions, and non-portable tools.
- [x] Check assertion plumbing for vacuous success, stale output reuse, wrong exit-code capture,
  regex overbreadth, inverted `assert_ok`/`assert_bug` semantics, and assertion-count drift.
- [x] Check bounded waits, teardown behavior, background-process containment, label isolation,
  container/volume lifecycle, and cross-drill concurrency safety.

## 3. S0 shared facilities and deployment fidelity

- [x] Verify the agent executable relocation matches the real `install.sh` layout at every unit
  generation path without masking a real ownership/upgrade defect or breaking old drills.
- [x] Verify `ingress_up` route argument parsing, TLS readiness semantics, SAN/CA negative tests,
  quoting, and backward compatibility with drill 82.
- [x] Verify artifact service freshness, immutable/read-only intent, checksums, TLS trust,
  hostname/SAN isolation, cleanup, and resistance to stale/torn artifacts.
- [x] Verify dual-version remote builds are actually version-only deltas, do not leave the normal
  binary in the wrong version, and are included correctly in remote sync/build inputs.
- [x] Verify Docker image additions and systemd unit changes reproduce production packages,
  permissions, users, paths, and restart behavior rather than repairing tether in the harness.
- [x] Verify `node_kill`/`node_stop`/`node_start` preserve the intended persistent state and cannot
  target an empty/wrong instance through quoting or argument bugs.

## 4. S3 expose drills

- [x] For drill 70, verify real end-to-end sentinel traffic, exact port refusal classification,
  allocation removal/reuse, explain/ps schema assertions, and same-allocation restart recovery.
- [x] For drill 71, independently verify the #29 preconditions and signature discriminator;
  ensure a transient eligibility/race/agent failure cannot satisfy the expected-defect arm.
- [x] Verify internal docs do not overclaim crash/drain/rehome, `--no-rebuild`, events, or HA
  coverage that the implemented drill no longer exercises.

## 5. S4 proxy/rebalance drills

- [x] For drill 72, verify owner/member authorization, one-time subscription secrecy, revocation
  controls, loopback binding, real Shadowsocks success, private-destination denial, and wrong-PSK oracle.
- [x] For drill 73, verify non-tunnel-home traffic and rehome recovery, dead-vs-survivor data-plane
  separation under quorum loss, control-write fencing, and no dependency on unreadable events.
- [x] For drill 74, verify skew construction, heaviest-home selection, return-to-voter versus
  proxy eligibility, dry-run non-mutation, manual rebalance causality, unhomed exits, and the honest
  status of the auto-rebalance path.
- [x] Check secrets in command output, process listings, temporary files, logs, subscription YAML,
  and generated proxy client invocations; verify no durable credential leakage.

## 6. S5 upgrade/install drills

- [x] For drill 31, verify broker allowlist, agent-local allowlist, URL/TLS reachability, SHA,
  ownership, `--all`, and multi-agent/fleet semantics; ensure #28 cannot pass for the wrong reason.
- [x] For drill 30, verify staged artifact identity, precondition refusals, dry-run plan/order,
  running-version (not disk-version) oracle, grow-lock #31 discriminator, and suppression of
  PID/write-continuity assertions when no upgrade occurs.
- [x] Determine whether #31 is a product defect, harness sequencing defect, or both by inspecting
  the product implementation and independently probing the live cluster state.
- [x] For drill 32, verify dry-run zero-write comprehensively, per-role install/uninstall/reinstall,
  process non-start, ownership/mode checks, and whether role loops accidentally share residue.
- [x] Verify the S5 coverage claims do not mark fleet upgrade or rolling upgrade complete when
  the success mechanisms are blocked or unexercised.

## 7. Documentation and traceability

- [x] Cross-check plan vs implementation vs internal review vs coverage inventory vs gotcha ledger
  for drill names, assertion counts, GREEN/RED meaning, gotcha numbering, signatures, and coverage.
- [x] Check user-facing operational claims against `usage.md`, cluster/runbook docs, and actual CLI
  help/source, especially #28/#29/#31 and proxy eligibility timing.
- [x] Check all source/file/line references for staleness and all claims of live execution for
  reproducible command/log evidence.
- [x] Record questions and recommendations separately from release-blocking findings.

## 8. Verification execution

- [x] Run lightweight local checks first (syntax, lint/static checks, helper self-consistency).
- [x] Run regression drills required by the shared layout/ingress changes (61/62/80/82) when the
  sim server is healthy, or document exact infrastructure blockers and obtain equivalent evidence.
- [x] Run all eight S3–S5 drills on the designated simcluster server, preferring isolated reruns for
  suspicious failures; preserve concise logs and distinguish deterministic failures from infra flakes.
- [x] Add independent regression tests/probes where an important oracle is absent or can false-green.
- [x] Re-run every changed/added test and the smallest relevant regression set after test fixes.

## 9. Closure

- [x] Re-read every task item and link each completed item to evidence or a report finding.
- [x] Write `docs/reviews/s3-s5-external-review.md` beginning with exactly `Pass` or `Fail`, with
  severity-ranked findings, doubts, questions, suggestions, test evidence, and residual risk.
- [x] Confirm no implementation code was modified by the external reviewer; only independent tests
  and review documents may be added/changed.
- [x] Add every file in the working tree to the Git index and verify the final staged status/diff.
