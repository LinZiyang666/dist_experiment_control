# upgrade-safety 独立外部复审 tasklist（round 2）

日期：2026-08-01

基线：`HEAD=a31634814aff`；round-1 成果已在 index。开发者修复位于工作树：17 个 tracked
文件修改、2 个 untracked 测试文件，约 `+751/-162`；初始 tracked unstaged diff SHA-256：
`6e89a88af4cb96687fc9497f8780b2fa9bda41f2ad3d3551dbb8601161f41f4d`。

流程约束：先独立复审开发者修复；完成复审材料后将开发者成果与复审材料加入 index；随后审查者
可直接修复残余问题，但修复与最终报告必须留在 index 外，供 staged/unstaged 对比。

## A. 边界与处置映射

- [x] 冻结 staged 与 developer-unstaged 边界，确认 round-1 reviewer 红测、开发者修复及无关工作树文件的来源。
- [x] 重读 `CLAUDE.md`、升级 plan/内审/外审及开发者逐条回复；把 F1-F7 的每项承诺映射到真实代码和测试。
- [x] 审计 `line2-plan.md`、结构预算和 nolint 账本的附带变化，确认不是为修复门禁而削弱门禁。

## B. F1 多实例/共享升级域

- [x] 检查 host lock 的路径、创建权限、symlink/no-follow、fd 生命周期、TryLock/阻塞语义、错误分类和所有 RMW 覆盖面。
- [x] 验证同进程不同 Agent、真实多进程、同 binary alias、不同 binary 同目录、锁文件删除/替换及进程崩溃后的互斥语义。
- [x] 审计 marker 的 target sid/nid/upgrade-id：旧 marker 零值兼容、target 匹配、兄弟进程 report/commit/watchdog/terminal clear 隔离。
- [x] 验证 running-image SHA 真正读取运行 inode，不被磁盘路径 rename、`/proc` 不可用、Darwin/FreeBSD 行为或测试 override 误导。
- [x] 检查共享 binary 下“目标实例 commit、所有兄弟实例下一次启动升级”的域语义是否可收敛，是否仍能错误 rollback 已在工作的兄弟实例。
- [x] 对跨 Agent install 回归做反向变异；必要时增加真实子进程或锁文件攻击测试。

## C. F2 启动 shim

- [x] 枚举真实 argv：全局 flag 前置、`agent` 后置、`help agent`、`agent help`、`--help` 组合、install/uninstall 值形态与别名，确认只在 daemon 启动消费预算。
- [x] 检查 shim logger、marker/lock 权限、rollback exec argv、递归执行及 Cobra 解析失败路径；确认无双计数或绕过。
- [x] 运行并审查真实 binary 黑盒测试的 oracle、平台依赖、构建缓存、超时和 rollback 结果。

## D. F3/F4 ctl wait 与 fleet

- [x] 复核 same-tag fail-closed 的单节点/车队退出类、输出、untouched 计数和 `--wait=false` 逃生门。
- [x] 复核 baseline `(string,error)` 的所有调用方；节点不存在、空 release、暂态 NATS、旧 agent、ONLINE/OFFLINE 时序不得 dispatch 或假提交。
- [x] 检查 release 变化仍可能来自与本次 upgrade 无关的 register/update；评估当前无 generation wire 下剩余的可接受边界。
- [x] 运行 reviewer 反例与新增 CLI 测试，并做 baseline/same-tag 判据变异。

## E. F5 冒烟与纪元

- [x] 审计冻结 version 行解析：精确 token、release 空值/空白、多行、超长输出、错误 epoch、非零退出及 stderr。
- [x] 验证 64 KiB 输出上限是真正有界、`WaitDelay`/context 能处理继承 pipe 的后代进程，不遗留进程或长期持锁。
- [x] 确认 cross-epoch 错误稳定映射为 `proto_bump_requires_reinstall`，且 disk/prev/marker 在拒绝前零变化。

## F. F6 持久化事务

- [x] 逐步核对 candidate chmod+fsync、prev、pending marker、dst flip、commit、rollback、terminal remove 的 file/dir fsync 顺序。
- [x] 检查每个 sync 失败点的返回与补偿；不得留下 pending+old dst、new dst+不可用 marker、被误删 prev 或错误 staged 回执。
- [x] 审计 sync observer 的并行安全、生产 inert 性和测试假绿风险；确认测试不只是观察调用而绕过真实 fsync。
- [x] 检查硬链接 fallback、目录 fd、只读/不支持目录 fsync平台的行为和文档支持边界。

## G. F7 文档、全局回归与 simcluster

- [x] 对齐 requirements/architecture/usage/broker-ops/inventory：升级域、UNCONFIRMED、likely rollback、#28 已修及 deploy-tier NOT-COVERED 必须一致。
- [x] 运行 `git diff --check`、受影响包、reviewer tests、`-race`、wire/architecture/determinism/auth/concurrency 门。
- [x] 运行 `make gates`、`make test`、`make e2e-parallel`、`make lint`，分别记录真实退出码。
- [x] 复核 simcluster server 状态；只有 DNS fidelity 前置满足时才运行相关 drill，不使用 fake-DNS 绕过 dead-node oracle。

## H. 结论、暂存、修复与最终报告

- [x] 形成 round-2 复审结论：无 Blocker/Major 才可放行；列明疑惑、Minor 和建议。
- [x] 将开发者修复、round-2 tasklist 与复审结论加入暂存，确认 index 外无开发者遗留内容。
- [x] 对剩余代码/测试/文档问题直接实施修复并验证，不改写已暂存开发者基线。
- [x] 形成首行为 `Pass` 或 `Fail` 的最终报告；报告与审查者修复保持 unstaged，列出 staged/unstaged 对比。
