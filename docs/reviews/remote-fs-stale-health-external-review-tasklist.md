# remote-fs stale-health 独立外部审查 tasklist

> 审查对象：外审开始时当前工作树中全部暂存区外内容。内部 plan、内审报告及其绿色结果只作为待证伪假说；结论必须由权威契约、源码、独立测试或可重复运行证据重新建立。
>
> 审查边界：外审者不修改生产实现；允许新增独立测试、tasklist 与最终报告。全部项目完成后才给出 `Pass` / `Fail`，最后执行 `git add -A`。

## A. 范围、权威契约与事故链

- [x] A1. 粗读 `git status`、diff stat/name-status、本轮 plan/内审和生产差异，确认暂存区为空，建立初始风险面。
- [x] A2. 完整对账 tracked/untracked、numstat、完整 diff 与 HEAD 基线，排除漏审文件、生成物、无关漂移和隐藏的 staged 内容。
- [x] A3. 按权威链阅读 requirements、distributed-broker/deploy-tier、usage、testing standards、历史 architecture 有效章节，以及 remote-fs 原始 plan/review；提取 liveness、兼容性、资源界、错误分类和文档诚实性契约。
- [x] A4. 独立重建 #81/#82 事故链，逐条挑战本轮 plan 的前提、范围裁决、状态转换、D-state 线程预算、TTL 选择、四个 evidence 站点和内审 finding 处置。
- [x] A5. 核验变更是否保持 wire/proto/config/CLI 默认行为与 N-1 兼容；识别任何未声明的行为面或上线/回滚风险。

## B. 核心生产实现

- [x] B1. 完整审查 `mountHealth` 状态机与 `applyMounts` 继承：所有 state/result/done/decidedAt 组合、epoch swap、旧 launcher/joiner、mount generation 变化、晚到结果及 channel 关闭所有权。
- [x] B2. 审查 TTL 语义：零值/负值/注入时钟、边界相等、时钟回拨、只重探 healthy、dead sticky/self-heal、重复过期与每 mount-generation 的 goroutine 上界。
- [x] B3. 审查 `InvalidateHealthy`/`invalidateHealthy` 的锁序、原子性和作用域；验证不会在 `p.mu` 下重入、不会错误复活 unhealthy、不会遗漏或误伤挂载。
- [x] B4. 逐入口审查 `Prepare`：`--safe` 作废顺序、mount refresh、PATH/cwd/explicit argv0、local fast path、autofs、ceiling 与 outage/warning 语义，确保兼容性和 fail-fast 分类不漂移。
- [x] B5. 审查 `boundedResolveInDirs` 的 deadline evidence 接线、并发 slot、迟到 resolver、错误优先级与 FSError 映射；验证作废发生在正确分支且不会制造重试风暴。
- [x] B6. 审查 `RunStartWithCleanup` 及 `exec.startBounded` 收敛：同步 `onAbandon`、reap-before-release、pipe/PTY/session 所有权、成功/错误/timeout/ceiling 分支、panic/迟到返回与 goroutine/fd/wedge-slot 回收。
- [x] B7. 审查 agent `boundedHomeRead` 证据接线与 Home 单飞：timeout 竞态、迟到 read、重复调用、nil policy、本地/网络 Home、state store 锁链以及是否夸大 #82 已修范围。
- [x] B8. 审查 ctl 错误提示和两处 `--safe` 帮助：错误分类、退出码、换行/去重、exec/run 对称性、用户可执行建议及旧客户端/agent 组合。
- [x] B9. 审查可维护性与代码异味：导出面、命名/注释真实性、重复状态、锁内慢操作、timer 用法、复杂度、测试专用 API、跨包职责和未来新增 spawn 路径的防漏能力。

## C. 测试与架构门有效性

- [x] C1. 阅读全部新增/修改测试，逐项确认 fixture 可达、断言能观察目标终态、无恒等式/只测 helper/依赖调度巧合/资源泄漏/过程命名；对 plan 宣称的变异收据做抽样复现。
- [x] C2. 深审 `spawn_stall_evidence_test.go` 的 AST discovery、mint-site ledger、deadline-arm 判定、同函数多站点、别名/包装器、闭包、named return、锁支配与递归/跨接收者误报漏报。
- [x] C3. 深审 `safe_flag_contract_test.go` 的源码解析和文档行匹配，验证不会误取相邻字符串、注释或不同 BoolVar，也不会在 flag/文档重排后静默失明。
- [x] C4. 为高风险但现有守卫不足的路径添加职责命名的独立 reviewer 测试；优先尝试 epoch 竞态、TTL/晚到探针、safe+refresh、timeout cleanup 顺序、Home timeout 接线和 gate 绕过反例。
- [x] C5. 对新架构门做有针对性的变异验证；确认 `CLAUDE.md` 注册、gate registry、`make gates` 接线与闸门声明的覆盖面一致。
- [x] C6. 运行受影响包定向测试、重复/压力测试、`-race` 与 goroutine/fd/slot 泄漏检查；区分产品失败、测试缺陷和环境限制。

## D. 文档与运维真实性

- [x] D1. 审查 `docs/usage.md` 的 safe-mode/网络 Home 契约、flag 表、示例与实际行为一致，且未把 best-effort 写成保证。
- [x] D2. 审查 `docs/deploy-tier-gotchas.md` #81/#82 的现场证据、状态、已修/未覆盖边界、测试锚点、行号/数量和推断措辞；核对与 plan、实现、drill 相互一致。
- [x] D3. 审查 `CLAUDE.md` 新闸门登记是否准确、可机械重导、没有把测试盲区写成已覆盖。
- [x] D4. 审查 simcluster drill 62 的 shell 安全、故障注入真实性、断言顺序、cleanup、日志 oracle、可选/强制 guard、旧镜像污染与 false-green/false-red 风险。

## E. deploy-tier / simcluster 与全量验证

- [x] E1. 阅读 simcluster Mandate、cluster/device/本地运维信息；只读检查 server/instance/宿主能力和残留状态，确定最小相关 drill。
- [x] E2. 从当前源码构建并运行相关 remote-fs drill（优先 62）；记录 setup/product/assert/not-covered/cleanup，绝不让 harness 代偿产品缺陷。
- [x] E3. 对真实 D-state/FUSE/网络 Home 或 outage 横幅等无法安全覆盖的场景明确登记 NOT-COVERED、原因、风险与建议 owner。
- [x] E4. 运行 `make gates`、`make test`、`make e2e-parallel`、`make lint` 以及必要的 build/vet/diff-check；不以一条绿色命令覆盖独立红色反例。

## F. 结论与交付

- [x] F1. 汇总 Blocker/Major/Minor/建议，逐条给出文件行号、触发链、影响、证据和建议修法；单列疑惑、假设与残余风险。
- [x] F2. 生成 `docs/reviews/remote-fs-stale-health-external-review.md`，首行仅写 `Pass` 或 `Fail`，记录完整验证命令与结果。
- [x] F3. 全部 tasklist 打勾后运行 `gofmt`（仅外审新增 Go 测试）、`git diff --check`、`git add -A`，最后确认所有文件均已暂存且无暂存区外遗漏。
