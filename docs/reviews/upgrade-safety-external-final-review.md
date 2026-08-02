Pass

# upgrade-safety 独立外部最终审查（round 2 修复后）

日期：2026-08-01

## 结论

**放行。** 开发者大改经独立复审后曾判定 Fail（2 Major + 4 Minor，见
`upgrade-safety-external-rereview.md`）；本轮已直接修复或确认闭合 F8-F13，所有 reviewer 反例、受影响包、
race、仓库级 gates、全量测试与 18-worker 并行 E2E 均通过。最终状态未发现残留 Blocker 或 Major。

本结论按两个可审计层交付：开发者成果、修复前 reviewer 红测/tasklist 和中间 Fail 报告已经冻结在
index；审查者修复、tasklist 完成标记和本最终报告全部留在 index 外。未再次执行 `git add`，可直接用
`git diff` 查看审查者改动，用 `git diff --cached` 查看开发者/复审基线。

## Findings 处置

### F8 — Major，已修：host flock / `upgradeMu` ABBA 死锁

`installNewBinary` 的调用方已持 host flock，marker 写入不再反向获取 `upgradeMu`；commit/watchdog 继续用
`upgradeMu → host flock`，因此不存在 `host flock → upgradeMu` 的环。子进程反例
`TestUpgradeHostLockOrderDoesNotDeadlock` 从稳定 4s 超时翻绿。

### F9 — Major，已修：Darwin 路径 SHA 可借用

commit proof 从 target + BootCount + running SHA 加强为四项：target、BootCount、**本进程在 pre-Cobra boot
shim 获得的同一随机 UpgradeID**、running SHA。事务 ID 使用 128-bit `crypto/rand`，不再由 SHA 前缀和秒级
deadline 拼接；旧进程、兄弟进程、旧事务以及非 Linux 的 replaced-path fallback 均无法借用该证明。
新增链路断言同时钉住 `bootUpgradeCheck → process proof → Agent.New`，Darwin arm64 完整 binary 交叉构建通过。

### F10 — Minor，已修：Cobra bool 值形态

冻结 index 前开发者补入了 bool 值解析；裸值/`=true` 的 help、install、uninstall 均不消费 boot budget，
显式 `=false` 仍按 daemon 调用处理。表驱动反例全部通过。

### F11 — Minor，已修并收紧：marker rename 后 fsync 失败补偿

pre-flip marker 写失败会清理可见的本次 pending 与 prev，并 best-effort 同步目录；审查者进一步把 marker
清理限定为 UpgradeID 精确匹配，避免写入在 rename 前失败时误删上一笔 committed breadcrumb。注错测试已翻绿。

### F12 — Minor，已修：exec recovery 锁错误导致唯一旧进程退出

阻塞 flock 对 EINTR 重试；exec-failure recovery 无法取得 host lock 时响亮记录并保留当前旧进程，不再把
“无法检查 marker”解释成“无 pending”后 `os.Exit(1)`。watchdog 的 lock failure 也不再静默丢弃。

### F13 — Minor，已修：#28 / deploy-tier coverage 账本漂移

inventory、simcluster README 和 drill 自述已统一：#28 已修；31 drill 覆盖 allowlist flip 与 fleet control
surface；真实 PID-preserving re-exec、version/re-register、rollback 和 `--wait` deploy-tier success 仍为
NOT-COVERED，且当前无 owner。没有再把缺口错误归因于 #28 墙，也没有声称不存在的 dedicated drill 已拥有它。

## 验证证据

以下均在最终未暂存修复上运行，退出码为 0：

- reviewer 反例：F8/F9/F11/F12、共享 binary per-instance proof、same-tag fleet、legacy baseline、boot shim bool；
- `go test ./internal/agent ./cmd/tether ./test/p10 -count=1`；
- `go test -race ./internal/agent ./cmd/tether -count=1`（agent 14.096s，cmd 103.975s）；
- `make gates`（含 vet、Darwin cluster build、architecture/determinism/auth/concurrency/proto 与 lint）；
- `make test`（全包通过，包含 p1-p13、security、chaos、cluster 与 CLI E2E）；
- `make e2e-parallel`（99 units，18 workers，15/15 top-level coverage，3m24.705s，`ALL PASS`）；
- 独立 `make lint`（`0 issues`）；
- `GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -o /tmp/tether-darwin-review ./cmd/tether`；
- `sh -n test/simcluster/drills/31-node-upgrade-fleet.sh`、`git diff --check`、`git diff --cached --check`。

Darwin build 仅打印 module stat-cache 位于只读目录的 warning，构建自身成功；未将 warning 误报为产品失败。

## 疑惑、残余风险与建议

1. **deploy-tier success 仍无 owner。** 这是明确登记的 coverage gap，不是已知代码缺陷，也未被本地测试假装
   覆盖。建议在下一次生产升级前建立独立 destructive drill，至少钉住 PID、真实运行版本、re-register
   commit、same-tag、rollback 和 `--wait`。
2. **simcluster 本轮没有可运行实例。** 最终只读 `simcluster status --json` 返回
   `{"error":"no leader in instance sim"}`；服务器 resolver 仍是 `127.0.0.53` stub，DNS fidelity 前置不满足。
   因此没有使用 fake DNS，也没有把 setup 状态计算为产品 verdict。
3. **boot proof fail-closed。** 任何绕过 `cmd/tether` pre-Cobra shim、直接嵌入 `agent.New` 的新生产入口都不会
   获得 proof，pending upgrade 将不 commit 而在 deadline rollback。这个方向是安全的，但未来新增入口时必须
   显式接入 `BootUpgradeCheck`。
4. host lock 文件仍依赖 binary directory 作为信任边界并会跟随 symlink；能替换该目录内容的主体本来就能
   替换 tether binary。当前不是额外权限提升，但若将来允许低权限多租户共享可写安装目录，应改为受保护的
   runtime lock 目录并使用 no-follow 打开策略。

## 暂存边界

- **staged**：开发者大改、round-1/round-2 reviewer 测试与中间报告，共 47 个路径；其中包括审查期间并发出现
  且已保留的 `docs/deploy-tier-gotchas.md` 更新。
- **unstaged**：F8/F9/F11/F12/F13 修复与补测、tasklist 完成标记、本最终报告。
- 最终未执行任何重新暂存；上述边界是有意保留的审计界面。
