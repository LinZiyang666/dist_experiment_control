# 克隆凭据实例增量 · 外部独立审查 tasklist

> 审查对象：工作树中全部未暂存内容（含未跟踪文件）。本清单由外部审查者在粗读修改范围后独立建立；既有 plan/内审报告只作问题索引，不作正确性证据。
>
> 判定原则：生产上线后可能暴露的正确性、安全性、兼容性、并发、恢复、运维或可观测性问题均进入结论；已有测试全绿不能替代状态机与失败路径证明。

## A. 基线、范围与权威契约

- [x] A1. 核对 `CLAUDE.md` 的权威链、外审边界、测试/归档/暂存要求，并读取相关活文档与 simcluster Mandate。
- [x] A2. 建立完整修改清单（tracked + untracked），确认仅审查暂存区外内容，检查是否夹带无关变更、过程命名或遗漏文档。
- [x] A3. 从 requirements / 当前架构 / plan 提炼可机械验证的不变量：单持有者、每进程 instance id、租约非持久、旧版本降级、I2 例外、共享状态安全。
- [x] A4. 独立核对既有两轮内审每项“已修/非缺陷/遗留”叙述，拒绝以内部结论代替源码、测试或运行证据。

## B. Wire、身份与兼容窗口

- [x] B1. 审查 instance-id 的生成、字母表/长度/熵、环境继承、exec/rollback 连续性、普通子进程隔离与错误降级。
- [x] B2. 审查租约名解析/生成/边界（长度、`-NN` 歧义、后缀耗尽、已 provisioned 名、排序、嵌套/前导零）及 agent/broker 规则一致性。
- [x] B3. 核对所有 wire 字段 additive/omitempty/零值合法、inventory/freeze 更新真实，验证 N-1 四方向的注册/告别/claim-probe/节点列表语义。
- [x] B4. 检查旧 broker、混合版本 auth_callout queue、broker rollback、旧 agent 与 malformed/空 instance-id 的 fail-safe 方向。

## C. Broker 租约裁决与并发状态机

- [x] C1. 逐路径审查 register 先后顺序：contest 必须在任何持久化、reconcile、proxy/event 副作用之前短路；uncontested 路径行为不漂移。
- [x] C2. 审查 holder/grant/probe cache 的客观语义、TTL、失效、释放、leader/进程重启与陈旧证据；验证“自己订阅”与“他人订阅”不会互判。
- [x] C3. 审查并发 challenger、offer 保留、subscribe settle、慢/静默 responder、后台 probe 覆盖、suffix 重复分配及锁粒度/HOL 阻塞。
- [x] C4. 审查后缀分配对 DB 错误、未知 provisioning、离线真设备、leased 标记、最大实例数与拒绝/降级分支的处理。
- [x] C5. 审查 graceful farewell 的防伪、乱序/重复/陈旧消息、N-1 行为、证据保留及 teardown 时间上界。
- [x] C6. 审查 cluster/leader/follower 语义：leader-local cache、forward/reconcile、raft 写边界、co-located agent、故障转移后的租约唯一性。

## D. Agent 采用、重连、资源所有权与升级

- [x] D1. 审查 routingNID 的所有读写点，确认 NATS CONNECT、register、forwarded subscriptions、proc/transfer/proxy/tunnel/roster 全部使用正确身份且连接内不可变。
- [x] D2. 审查 lease verdict 校验、采用次数上限、session rebuild、reconnect callback、拒绝/非法/空 assignment，以及 pending courier/reconcile 的提交边界。
- [x] D3. 审查 previousNID/进程 refile 的所有权证明，避免杀 incumbent、错误搬迁审计、二次 register 遗失或 cluster 模式行为分裂。
- [x] D4. 审查共享 `state.json` 的 detach/reattach、端口 replay/prune/fail-close、proxy 状态、并发写与 basename/lease 实例相互破坏。
- [x] D5. 审查 tunnel `SetNID` 的数据竞争、旧 session 退役、goroutine/FD 泄漏、阻塞 Close、重拨注册身份与端口劫持窗口。
- [x] D6. 审查 auth denial 后 dropLease 重试的 option 重建、backoff、终止条件、日志/运维提示、旧名资源清理及状态恢复。
- [x] D7. 审查 upgrade marker、boot shim/rollback、共享二进制/共享 home/flock、多实例 target lineage、显式 upgrade 与 `--all` 排除语义。

## E. Auth、存储、CLI 与运维表面

- [x] E1. 审查 auth_callout suffix fallback 的绑定/会话/fence/PIN bootstrap 顺序、权限最小化、嵌套/真实数字后缀设备与 eviction 语义。
- [x] E2. 审查 migration 0019 的前后兼容、所有 node 写/读/UPSERT 列表、snapshot/restore/cluster apply，以及 leased 标记不会被心跳/旧 agent 意外清零。
- [x] E3. 审查 node list、proxy eligibility、admin evict、audit/history/events、transfer/expose/exec/run 的租约实例可寻址性与身份归属。
- [x] E4. 审查 `node upgrade --all` 的筛选、canary、计数与输出；真实 basename 形似租约时不得误排，显式 target 行为须清楚。
- [x] E5. 对照 requirements/usage/broker ops/cluster docs，检查用户可操作流程是否完整准确（识别物理实例、重启改名、共享 home、租约耗尽、回滚、故障排查）。

## F. 测试质量与独立验证

- [x] F1. 审计所有新增/修改测试：断言是否可达、fixture 是否真实经过生产路径、是否存在恒真/假绿、时间常量与泄漏纪律是否合规。
- [x] F2. 对关键守卫做变异思维或实际变异验证，尤其 contest-before-side-effects、probe staleness、farewell ladder、state detach、tunnel retarget、upgrade lineage。
- [x] F3. 运行按包单测与必要 `-race`，独立添加最小回归测试以证明新发现；测试按职责命名并带稳定 origin。
- [x] F4. 运行 `make test`、`make gates`/`make lint` 与唯一全矩阵 `make e2e-parallel`，记录命令、rc、耗时和任何 flake。
- [x] F5. 阅读 simcluster server/资源说明，审计 drill 83 的保真度与 oracle，按需在本机 sim cluster 运行该 drill 并记录 verdict。
- [x] F6. 检查 git diff/stat、格式、生成清单、迁移连续性、shell syntax、测试命名及结构预算放宽的必要性。

## G. 报告与交付

- [x] G1. 汇总 findings，按 BLOCKER/MAJOR/MINOR 给出精确源码位置、失败场景、影响与建议；明确疑惑、覆盖缺口及非缺陷判断。
- [x] G2. 报告首行给出 `Fail` 或 `Pass`，逐项列出独立验证证据与未运行/受限项，格式对齐 `docs/reviews` 的外审习惯。
- [x] G3. 完成全部 tasklist、写入最终报告，复核报告自身引用与结论一致。
- [x] G4. 将工作树全部文件加入暂存区，核对 `git status` 仅剩 staged 内容并向用户交付。
