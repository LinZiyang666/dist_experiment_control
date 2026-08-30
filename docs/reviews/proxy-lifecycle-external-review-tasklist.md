# proxy-lifecycle 独立外部审查 tasklist

> 审查对象：当前工作树中全部暂存区外内容。内部 plan / review 只作为待证伪假说，任何结论必须由源码、契约、独立测试或运行证据重新建立。
>
> 审查纪律：外审者不修改生产实现；允许添加独立回归测试、tasklist 与最终报告。完成全部项目后才给出 Pass / Fail，并把所有文件加入暂存区。

## A. 范围、契约与历史证据

- [x] A1. 对账 `git status`、name-status、numstat 与完整 diff，区分本增量、既存无关漂移、未跟踪文件和生成/备份文件。
- [x] A2. 按权威链复核 requirements、当前 distributed-broker/deploy-tier 契约、历史 architecture 的有效部分，以及 testing/simcluster 规范；提取 proxy 生命周期、排序、ready、fail-closed、rehome、退出清理和 N-1 约束。
- [x] A3. 独立核验事故根因链；逐条挑战 `proxy-lifecycle-plan.md` 与内审报告中的采纳、驳回、遗留议题和测试/变异声明。
- [x] A4. 审阅文档变更的真实性、权威层级、交叉引用、编号、覆盖口径与设备清单漂移；判断 `.bak` 是否属于不应上线的工件。

## B. 生产实现审查

- [x] B1. 完整审查 `ssproxy.Server` 状态机：Start/Serving/SetKeys/Stop/shutdown/acceptLoop 的单调性、幂等性、锁序、WaitGroup、listener 状态与 single-use 契约。
- [x] B2. 完整审查连接所有权：accepted/upstream/keyConns/allConns 的注册、撤销、并发 shutdown、revoke、half-close、异常返回与泄漏边界。
- [x] B3. 审查 dial/DNS 取消与 teardown 上界，验证慢 DNS、慢拨号、握手前阻塞是否会在 `p.mu` 下阻塞 heartbeat、ready 或 rehome；独立裁定内审留出的首要议题。
- [x] B4. 建模 agent proxy 状态机：`srv`、Serving、gen/epoch、home epoch、`needsReestablish`、`authoritativelyOff`、`agentExiting`、持久 footprint、dial backoff 的合法组合与转移。
- [x] B5. 逐入口审查 directive/register/reconnect/heartbeat/fail-closed/agent-exit 并发与排序；验证 stale/equal/lower-generation、token-bearing/keyset-only/nil/OFF 不会复活、误杀、假 READY 或永久变暗。
- [x] B6. 审查 ready/unready 发布语义及 single/cluster 两模式差异；验证 corpse 自愈同调用时序、失败重试、broker repair 抑制条件和端口/rehome 爆炸半径。
- [x] B7. 审查 Run defer 接线与 fail-closed timer 竞态，验证退出后无迟到重建、无 footprint 擦除、无 goroutine/fd/listener 泄漏。
- [x] B8. 审查 API/可维护性/代码异味：测试专用导出 API、注释真实性、错误分类与日志降噪、锁内日志、复杂函数预算、context 禁止面和未来子包逃逸。
- [x] B9. 复核范围边界与兼容性：不得有 proto/wire、broker 行为、依赖或 CLI 表面漂移；核验 N-1 四象限与回滚陈述。

## C. 测试有效性与独立反例

- [x] C1. 阅读所有改动/新增测试，逐断言确认 fixture 可达、终态可观察、时间预算有证伪力、非恒等式、非只测 helper、无 process-named 测试。
- [x] C2. 审查新架构门是否递归覆盖目标、不会空转、与 `make gates`/CLAUDE 注册一致，并对其关键主张做独立变异或等价反例。
- [x] C3. 针对未覆盖高风险面添加独立 reviewer 测试；优先尝试：in-flight DNS/dial teardown、EADDRINUSE/rebuild、ready 发布序列、停止者闭集接线、late directive/fail-closed/exit 交错。
- [x] C4. 运行受影响包定向测试与 `-race`；重复高并发/时间敏感用例并检查 goroutine/fd/连接 tracking。
- [x] C5. 运行 `make gates`、`make test`、`make e2e-parallel`、独立 lint/build/vet/diff-check；确认没有少跑或把 setup failure 误判为产品 verdict。

## D. deploy-tier / simcluster

- [x] D1. 读取 simcluster Mandate、设备/本地运维信息并检查 server/instance 状态；只在 fixture 真实可用时运行相关 proxy drill。
- [x] D2. 选择能验证本改动独有部署风险的最小 drill（优先 72/73 或专门复现）；记录 setup、产品断言、日志 oracle 与清理状态，绝不以 fake harness 代偿产品缺陷。
- [x] D3. 对 simcluster 无法覆盖的生命周期场景明确登记 NOT-COVERED、原因、风险和建议 owner，不把未运行写成通过。

## E. 结论与交付

- [x] E1. 汇总所有 Blocker/Major/Minor/建议，逐条给出文件行号、触发链、影响、证据和建议修法；明确疑惑与残余风险。
- [x] E2. 生成 `docs/reviews/proxy-lifecycle-external-review.md`，首行仅写 `Pass` 或 `Fail`，并记录完整验证命令与结果。
- [x] E3. 对 tasklist 全部打勾，运行 `git diff --check`，执行 `git add -A`，最后核验所有文件均已暂存且报告/测试纳入审计面。
