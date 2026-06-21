PASS — D1 外审通过；未发现需要实现者返工的 High/Medium blocker。

# D1 External Review

Date: 2026-06-21
Reviewer role: external reviewer

本轮审查对象为暂存区外 D1 改动：`internal/cluster` 单节点 Raft/FSM、
`internal/storage` WAL/restore 接缝、determinism/e2e 测试接线、D1 plan/review
与架构/状态文档更新。结论：D1 代码范围守住，核心崩溃一致性与快照/恢复契约
有承重测试覆盖，新增全量测试通过。

## Review Tasklist

- [x] Scope and phase-boundary audit: confirm D1 stays limited to single-node Raft/FSM test construction and does not wire product mutators, forwarding, auth_callout, membership, tunnel, or multi-node behavior.
- [x] Raft/FSM correctness: check Apply indexing, command decoding, same-transaction `applied_index`/`applied_term`, replay idempotence, poison-entry behavior, fail-stop handling, Response/Error semantics, bootstrap/config entry off-by-one, shutdown hygiene.
- [x] Snapshot/restore correctness: check modernc online-backup Step/Finish usage, snapshot materialization, restore integrity/FK checks, migration path, in-place restore behavior, liveness reset, snapshot-index vs applied-index assertions.
- [x] WAL/storage API audit: check `OpenWAL`, read-only handles, DSN construction, migration export, journal/synchronous pragmas, foreign-key/busy-timeout behavior, and absence of P0-P13 DB behavior changes.
- [x] Crash-consistency and independent-test audit: inspect kill-9 harness and add targeted reviewer tests where the implementation relies on behavior not directly covered.
- [x] Determinism/lint/e2e audit: check raft confinement, nondeterministic-import guards, liveness-column detector self-checks, D1 e2e matrix wiring, and vacuity risks.
- [x] Documentation/report consistency: ensure CLAUDE.md, distributed-broker architecture notes, D1 plan/review, and actual implementation agree.
- [x] Verification: run focused tests and record exact commands/results; if full gates are too expensive or fail for existing reasons, state that explicitly.

## Findings

No blocking findings.

### N1 - Non-blocking: package/test comments still state the old snapshot invariant

`internal/cluster/doc.go:10` and `internal/cluster/snapshot_test.go:17-19` still
say `snapshot.Index <= applied_index`. That was the internal-review must-fix
that the implementation and architecture now correctly replace with “restart
does not lose any committed `LogCommand`” plus fail-stop on unapplied committed
commands. The tests themselves assert the corrected shape (`==` for all-command
tail and a separate barrier-gap test), so this is documentation drift, not a
behavioral defect.

### N2 - Non-blocking: CLAUDE.md still names `goleak` as the generic leak gate

`CLAUDE.md:58-60` still says race + `goleak`, while the D1 architecture update
at `docs/distributed-broker-architecture.md:300-301` correctly records the repo
convention: no `go.uber.org/goleak`, use `runtime.NumGoroutine` polling plus fd
baselines. D1 follows the corrected convention. This is process-document
wording drift and did not affect the D1 implementation.

## Reviewer Additions

Added `TestOpen_DefaultPathDoesNotEnableWAL` in
`internal/storage/storage_wal_test.go` to lock the D1 blast-radius boundary:
ordinary `storage.Open` must not enable WAL; WAL remains opt-in via `OpenWAL`
for the cluster FSM only.

## Verification

- `GOCACHE=/tmp/tether-gocache go test ./internal/cluster ./test/cluster ./internal/storage ./test/determinism` — PASS
- `GOCACHE=/tmp/tether-gocache go test -race ./internal/cluster ./test/cluster` — PASS
- `GOCACHE=/tmp/tether-gocache go test -tags e2e_matrix ./test/e2e -run TestD1Matrix -count=1 -timeout 300s` — PASS
- `GOCACHE=/tmp/tether-gocache go test ./...` — PASS outside sandbox; sandbox run failed only because localhost sockets were blocked
- `make lint` — PASS outside sandbox; sandbox run failed because Go/golangci-lint attempted to write module stat cache under read-only GOPATH
- `git diff --check` — PASS

## Re-review Result

D1 外审通过。代码层无 blocker；N1/N2 可在收尾文档清理时顺手改，不要求阻塞
本轮提交。

---

## 主进程回复（逐条处置，2026-06-21）

**结论：PASS 收悉，N1/N2 即修（不阻塞但顺手清掉文档漂移）。Reviewer 加的范围守卫测试采纳保留。**

- **N1（残留 `snapshot.Index <= applied_index` 注释）— 已修**：`internal/cluster/doc.go:10` 与 `snapshot_test.go:17-19` 的旧不变式措辞改为纠正后的"重启不丢任何已提交 LogCommand（snapshot.Index 只可能被无变更的 barrier/config entry 超过 applied_index）+ Apply fail-stop"，与实现/架构正文一致。
- **N2（CLAUDE.md 仍写 `goleak`）— 已修**：`CLAUDE.md §5`（原 :58/:60）改为"`-race` + 仓库内建 `NumGoroutine`/fd 泄漏门（**刻意不用 goleak**，见 `test/concurrency/helpers_test.go`）"，与架构 §13「泄漏门约定」一致。
- **Reviewer 加的 `TestOpen_DefaultPathDoesNotEnableWAL` — 采纳保留**：锁定 D1 爆炸半径（`storage.Open` 保持 rollback-journal，WAL 仅经 `OpenWAL`），与 D1 范围边界完全吻合，本地复跑 PASS。

**复验（修 N1/N2 + 含 reviewer 测试后）**：`make lint` 0 issues、full `make test` ALL PASS、`-race`(cluster/storage/determinism) 绿、`make e2e`(含 `TestD1Matrix`) ALL PASS。→ 推进 commit/push。
