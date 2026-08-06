# H1 independent external review tasklist

Date: 2026-08-05
Reviewer: independent external reviewer
Base: `HEAD=4e2a101` on `main`; staged area initially empty
Review boundary: all tracked unstaged changes plus all untracked files present at review start
(68 tracked files and 20 untracked files before this tasklist/report are added; approximately
`+2756/-219` for the tracked diff). The internal H1 review is treated only as a source of
hypotheses and is not accepted as evidence.

## Boundary, contracts, and prior-review workflow

- [x] Freeze and re-check the exact unstaged/untracked boundary; ensure no pre-existing staged
  candidate is accidentally mixed into the review.
- [x] Read `CLAUDE.md`, the requirements/current-architecture authority chain, testing standards,
  H1 plan, internal review, and representative recent external review/tasklist artifacts.
- [x] Map every changed file and every H1 goal/non-goal to an implementation and test owner; identify
  plan promises that have no executable evidence.
- [x] Read the complete product diff independently before relying on internal-review dispositions.

## A — bounded `ps` and reply egress

- [x] Verify process/port filtering, ordering, totals, truncation, legacy `-a` behavior, JSON output,
  timeout guidance, and all zero/default compatibility cases.
- [x] Verify count/list DB access respects the RODB/cfgdb contract and that count/list skew is only
  cosmetic under concurrent writes.
- [x] Audit every broker response path, including raw/text/auth-callout exemptions, max-payload
  fallback recursion/size/log severity, closed/draining connections, and `RespondMsg`-style escapes.
- [x] Audit every ctl and non-ctl response decoder for `reply_too_large`, unknown-code, retry/fatal,
  and old/new version interoperability behavior.

## B — single-node and Raft storage GC

- [x] Verify cutoff normalization, NULL timestamp fallback, strict boundary semantics, deterministic
  key ordering, row-close/error handling, terminal-state rechecks, chunk bounds, and idle-zero-writes.
- [x] Verify single-node and cluster behavior match; leader/follower authority, catch-up gates,
  failover/replay/idempotency, partial chunk failure, retention cadence, and backlog draining are safe.
- [x] Verify new FSM operations are append-only compatible with the documented lockstep rollout and
  that migration/rollback/log-replay constraints are operationally honest.
- [x] Re-run/adversarially extend scan-error, mixed-timezone, race-with-state-change, and multi-chunk
  tests; inspect whether existing-process/port consumers can observe broken invariants after GC.

## C — durable process-event delivery

- [x] Model courier state transitions for started/exit, duplicate enqueue, PID reuse, queue cap,
  drop policy, retry/backoff, parked entries, 24h expiry, reconnect, session replacement, and shutdown.
- [x] Verify current-connection use, request deadlines, fairness/bounded work, no goroutine/timer leaks,
  and no stale-connection or wakeup race.
- [x] Verify broker ACK decisions are derived from authoritative committed state, audit events are
  idempotent enough for retries, and follower forwarding preserves node/session identity.
- [x] Verify register snapshot replay/clearance across new broker, v0.4.7 broker, missing ACK,
  accepted/reconciled/absent PIDs, lost enqueue windows, and started-then-exit ordering.
- [x] Independently revisit internal-review fixes for parked-started exit delivery, live-process
  protection, parked expiry, and old-broker demotion.

## D — ctl liveness and PTY reaping

- [x] Verify keepalive subject ACLs, pump lifecycle, legacy-broker permission handling, raw-terminal
  safety, and cancellation on all run exit/error paths.
- [x] Verify agent stamps and reaper synchronization, grace calculation, suspend semantics, live NATS
  probe, second-strike confirmation, reconnect/restamp behavior, and false-positive resistance.
- [x] Audit process-group/PID reuse defenses, PTY master close + SIGHUP ordering, lock boundaries,
  wait completion, exit code/event publication, and interaction with courier reliability.
- [x] Add or run blackhole/reconnect/race/leak tests that distinguish a silent partition from a clean
  close and prove healthy long-running interactive sessions are not reaped.

## E — reconcile backoff, proxy alerting, audit/alert publishers

- [x] Verify the backoff package for overflow, jitter bounds, minimum hold, recovery decay, clock
  injection, zero values, and concurrency assumptions.
- [x] Model every proxy tracker transition: absent/unready/ready, repeated bind failure, mixed nodes,
  offline nodes, broker restart, alert raise/clear proposal failure, 60s hysteresis, and GC of state.
- [x] Verify proxy remains ON while retries are capped/fair; identity keys cannot suppress another
  node/port and alert severity/kind/migration are consistent across storage, wire, docs, and UX.
- [x] Verify AuditPublisher and AlertReconciler only log/increment attempts on real attempts, recover
  after failure without hot loops or permanent silence, and do not create unbounded duplicate work.

## F — rotating logs and deployment surface

- [x] Audit `internal/logrotate` for size accounting, external appenders, rename/open/fsync errors,
  backup retention, permissions, concurrent writes, partial writes, reopen rate limiting, and disk-full
  behavior without recursion or silent permanent degradation.
- [x] Verify agent log path/default/disable semantics, home/session validation, symlink/path safety,
  fd ownership, `dup2` panic sink behavior across launch styles/upgrades, and platform build tags.
- [x] Verify broker/agent logger wiring preserves severity/output expectations and closes resources in
  all startup failures and shutdown paths.
- [x] Audit install script and runbooks for ownership, journald persistence, config preservation,
  rollback ordering, smoke checks, and agreement with actual flags/config defaults.

## Cross-cutting protocol, security, architecture, and tests

- [x] Audit additive wire fields, JSON tags/zero values, subject constructors, ACL parity, enum/known-op
  registries, error-code classification, wire inventory/freeze updates, migrations, CLI golden, and
  structural-budget changes.
- [x] Check context/cancellation, mutex/channel ownership, error swallowing, unbounded allocation,
  integer/time overflow, nondeterministic map iteration, SQL injection/literal construction, path
  traversal/symlink exposure, log secret leakage, and fail-open behavior across all changed code.
- [x] Review every changed/new test for vacuity, fake fixtures, timing assumptions, mutation strength,
  production-constant use, race coverage, process-based naming, and false claims in comments/docs.
- [x] Reproduce every internally reported Blocker/Major disposition and investigate its eight listed
  residuals rather than inheriting their severity or scope judgment.

## Verification and sim cluster

- [x] Run focused unit tests per workstream, then affected-package `-race` tests and leak-sensitive
  tests; classify every failure with exact test identity.
- [x] Run diff hygiene, architecture/determinism gates, vet, lint, and repository-prescribed full test
  targets without hiding exit status.
- [x] Read simcluster mandate/server instructions and inspect server state before use.
- [x] Mechanically census H1 log-path assumptions in simcluster; run the relevant hermetic suite and
  named deploy-tier drills (broker provisioning, agent-log reading, journald contract, and storage GC
  where available), recording coverage gaps honestly and leaving the server clean.
- [x] Add independent regression tests for confirmed defects only, named by tested responsibility and
  annotated with stable review origins.

## Deliverables

- [x] Re-check the review boundary for unexpected concurrent changes.
- [x] Write `docs/reviews/h1-external-review.md` beginning with `Fail` or `Pass`, with severity-ranked
  findings, evidence, uncertainties, suggestions, and exact verification results.
- [x] Mark every task above complete or explicitly blocked with evidence; do not silently skip items.
- [x] Add all files to the staging area and verify there are no unstaged or untracked files left.

## 2026-08-05 开发者大改后的独立复审追加清单

- [x] 粗读 120 文件的 staged 范围，逐项复核上一轮 F1–F7 的整改，不采信内部“已修复”结论。
- [x] 在任何审查者修改之前冻结开发者候选；记录 cached diff 指纹并确认工作树为空。
- [x] 对 register courier、proxy alert hysteresis、日志迁移、boot.err、GC op、wire 错误与 ps
  截断重新建立故障模型和独立反例。
- [x] 修复进程 LIST 失败的 false-settlement/orphan-kill，以及首次 ready tick 绕过告警迟滞。
- [x] 审计 drill 80 的身份/会话/节点前置条件，纠正错误节点目标，撤销不能成立的 #75 归因。
- [x] 新增非空单测并运行定向 race、全仓 Go 测试、gates、99 单元并行 E2E、simcluster
  hermetic suite 与真实 drill 80。
- [x] 形成以 Pass/Fail 开头的最终报告，写明疑惑、限制、问题、修复和建议。
- [x] 复核暂存边界：开发者冻结基线仍在 index；审查者所有修改及最终报告均只在工作树。
