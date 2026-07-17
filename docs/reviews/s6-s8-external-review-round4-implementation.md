Fail

# S6-S8 外部重审 round-4 实现与复验报告

日期：2026-07-16
角色：外部审查者；特殊 stage 中对确定问题做最小修复并复验
原则：测试用于暴露 tether 问题；不靠重试、弱化断言、重启 agent、清 cache 或手工补 seeds 制造 GREEN

## 结论

本轮不再继续扩审或追加修改，但仍不能给 Pass。重大 force-single 冷启动缺陷与 40 的假红已关闭，40、41、90
均取得严格单次 GREEN；然而 91 在新镜像上仍稳定落为 ASSERT-FAIL：离线 force-single 后，发布 seeds 在 90 秒
内没有收敛为 survivor-only。该问题会让冷启动客户端继续拿到已死亡 broker endpoint，属于真实产品问题，不能
用“核心 broker 已恢复”代替，也不能写成虚假全绿。

42 的 force-single 后 broker crash-loop 已修复并越过原失败点，但 drill 后半段为了制造新的 transfer-audit，
在 `force_single_active` 状态执行 `ctl push` 被产品正确以 rc70 拒绝，最终为 SETUP-RED。它是夹具与安全策略冲突，
不是本轮新产品回归；由于用户要求当前阶段结束后停止，本轮如实保留该验证缺口，不继续改写 fixture。

## 本轮确认并修复的问题

1. **高 applied index 的 Raft 重建日志空洞。** 仅把 bootstrap config 从 index 1 移到 `applied_index+1`，
   `RecoverCluster` 仍会从 index 1 回放并报 log not found。改为从权威 SQLite 直接生成高位 `{self}` snapshot。
2. **snapshot-only store 无法通过生产启动探针。** 新 store 没有 `raft/raft.db`；Hashicorp Raft 可从 snapshot
   识别状态，但 broker 的 fail-closed 探针为避免隐式创建 store，先要求该路径存在，导致真实环境 crash-loop：
   `no raft state exists`。现同时创建空 Bolt stable/log store，并新增“resnapshot 之前直接冷启动、选主、权威写”
   回归，避免测试通过后续 resnapshot 掩盖缺陷。
3. **agent 测试数据竞争。** 测试修改包级 `rosterRefreshFailBackoff` 时后台刷新 goroutine 同时读取。改为 Agent
   实例初始化后不可变的测试 seam；四个受影响 package 的 race 门禁通过。
4. **40 的 `RAFT_REMOVED` 假红。** drill 在领导切换后把 `LDR` 固定为退役目标；目标转移领导权并被移除后，
   oracle 仍从其本地旧 DB 轮询。改为每次从当前 leader 读取 op，并收紧“旧 leader 返回”为 reachable VOTER。
   同一无重试演练随后 37/37 GREEN，且 surviving cluster 的真实控制写成功。
5. **harness 缺陷。** 修正 40 dry-run 的 SAN 身份、90 capped-store 的重复 mount（父 data-dir tmpfs 替代
   named volume，而非叠加）、42 失败诊断。断言本身未被放宽。

## 远端严格证据

全部使用 `-j1 --no-retry`，首次落地 verdict 保留。

| drill | 最终/最新证据 | 结论 |
|---|---:|---|
| 22 | GREEN，34 pass | 原严格批次已绿；本轮产品修复不涉及该路径 |
| 40 | GREEN，37 pass | 当前 leader oracle 下，mid-retire failover、terminal RETIRED、真实控制写均通过 |
| 41 | GREEN，30 pass | 连续退役、agent 迁移、N=1 standalone、Tier-B 通过 |
| 42 | SETUP-RED，26 pass | force-single 冷启动已通过；后半段 `ctl push` 被 `force_single_active` 正确拒绝 |
| 43 | GREEN，38 pass | 原严格批次已绿 |
| 90 | GREEN，49 pass | capped 3GiB fixture、disk-pressure raise/clear、below-quorum raise/clear 通过 |
| 91 | ASSERT-FAIL，34 pass | force-single 后 survivor-only seeds 在 90s 内未收敛；未重试、未手工 publish |
| 92 | GREEN，34 pass | 原严格批次已绿 |
| 93 | 未完成 post-fix 远端复跑 | 先前 fixture 修复已静态/contract 校验；本轮按停止指令不再扩跑 |

主要日志：

- `/tmp/s6-s8-external-round4-postfix-strict`
- `/tmp/s6-s8-external-round4-postfix-diagnostic`
- `/tmp/s6-s8-external-round4-final-minimal`

## 本地门禁

- `make e2e`：完整通过（含 ForceSingleRecoverRestart）。
- `go test -race ./internal/agent ./internal/broker ./internal/cluster ./internal/clusteroffline`：通过。
- force-single 外审回归：直接冷启动、选主、post-recovery 权威写通过。
- `make lint`：0 issues。
- shell syntax、9-drill lint、verdict contract：通过。
- `make test` 曾完整通过；最后一次全包复跑只有 `TestRebalanceProxySingleVoter` 在 admin socket 启动前偶发
  `connection refused`，同一测试随后 `-count=10` 全过。该测试与本轮 Raft store 修改无关，按时序 flake 记录，
  不冒充最后一次全包全绿，也不据此新增修改。

## 疑惑、问题与建议

1. 91 需要开发者继续定位：应直接读取 force-single 后 `cluster_meta.seed_endpoints/seed_generation` 与
   `cluster_nodes.public_host`，区分 offline drop-host 未命中、snapshot 恢复覆盖，还是 `seeds show` 服务不可用。
   修复时必须保留 survivor-positive + all-dead-negative oracle，禁止手工 publish 擦除失败。
2. 42 的 audit-window 正臂应改为在进入 `force_single_active` 之前生成可回放的真实 audit，或使用不违反
   emergency-mode 写保护的构造方式；不能让安全策略为了测试放行写操作。
3. 93 尚缺当前镜像的真栈复跑；它不是已观察到的重大产品故障，但在宣称完整九项 ALL GREEN 前必须补齐。
4. 本轮已按用户要求停止，不再对上述问题继续修改或复跑。开发者应从当前暂存快照继续处理 91 与 42 fixture。

## Release disposition

**Fail。** 已关闭的重大冷启动问题与通过项可以进入开发者反向审查；91 的真实 ASSERT-FAIL 未关闭前，不应宣称
qualified 或 ALL GREEN。
