# G4 external review tasklist

- [x] Read `CLAUDE.md`, architecture/requirements/cluster docs, simcluster mandate, and recent external-review style.
- [x] Reconstruct the unstaged/untracked review boundary from `git status` and broad diffs.
- [x] Read G4 plan/internal review as context only, without trusting its conclusions.
- [x] Review `tether cluster add` CLI/orchestrator resume model, HALT boundaries, signing, auth/session assumptions, and stale-op behavior.
- [x] Review grow trigger wire protocol, canonical signatures, ACLs, replay gates, and broker dispatch paths.
- [x] Review grow lock/membership mutual exclusion, adaptive catch-up, seed convergence, and operation-controller invariants.
- [x] Review former-N1 clustered cutover, JS move-aside/preserve semantics, restart/probe behavior, and idempotency/crash windows.
- [x] Review `cluster init` unattended confirm and broker.yaml seam application against privilege and YAML-shape requirements.
- [x] Review NATS reconcile secrets-dir fallback and standalone-to-clustered withhold behavior, including autonomous topology reconcile effects.
- [x] Review install/systemd changes and simcluster `cmd_grow`/drill behavior for honest deploy-tier coverage.
- [x] Add or run independent focused regression tests where the review identifies high-risk behavior.
- [x] Run focused Go/shell checks plus `git diff --check`; use simcluster/server path if needed for deploy-tier confidence.
- [x] Write external review report with Pass/Fail, findings, doubts, recommendations, and verification.
- [x] Add all files to the git index after the report is complete.
