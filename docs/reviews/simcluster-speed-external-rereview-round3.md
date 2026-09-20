Pass — simcluster-speed 独立外审 round 3（含授权修复的完整工作树）

2026-09-20。本轮独立审查；既有 round-2 Pass 与主进程回复均作为待证主张。
HEAD：`a3431a12be3cf947cc797c28fea93382fa68472d`。开始时 128 个文件已暂存，工作树无额外修改。
候选 staged diff SHA-256：`977a7ecc6130a94561e45593fd0616c80c841734fe2680402505112794ed7ac0`。
范围为首轮 F1–F5 的修复、后续 R1–R3 处置及相关调用链；按同名 round-3 tasklist 逐项执行。

## 最终结论

**Pass。** 本轮 tasklist 已全部完成。独立复验首轮修复后，新发现的 R3-F1（MAJOR，transfer ID 重用跨 entry 修改）、
R3-F2（MINOR，损坏 REGIME 误报已知模式），以及完整验收暴露的 R3-F3（MINOR，SQLite 测试清理竞态）均已修复并验证；
既有 silence 测试的外部 DNS 依赖一并消除。未放宽断言、删除测试或更改产品 wire 协议。

放行对象是**当前完整工作树：已暂存的开发者候选 + 暂存区外的审查者修复**。
index 保留下面的修复前 Fail 快照，不能仅凭本结论把 index 中的候选单独视为通过。
全量 `make test`、`make e2e-parallel`、最终 `make lint` 均 rc=0；gates 的全部非 lint 检查也已通过。
首次验收的失败与处理过程保留在文末。未重新运行整套 Docker drills，故本结论不新增部署性能收据。

## 修复前结论

**Fail。** 首轮五个原始触发形状已修复，但独立反例发现一条传输状态/权限 MAJOR 和一条回放证据 MINOR。
相关既有测试全部通过，不能替代下面的失败证据。

按用户本轮追加授权：先把开发者候选、此修复前报告与 tasklist 加入暂存；随后由审查者直接修复。
后续代码、测试和最终报告更新全部保持在暂存区外，便于与这个候选比较；不 commit/push。

## R3-F1 · MAJOR：校验的是旧 entry，claim 却能终结另一会话重用 ID 后的新 entry

**位置：** `internal/broker/transfer.go` 的 `handleFinalizeReq`（preview/transferGate/claim）、
`claimAbandonedPush`、`claimFinalize`；同类风险存在于 `markCommitted`、接收端事件及 watchdog/cleanup 调用方。

handler 先取得 `preview`，校验其中的 actor/session，再按字符串 transfer ID 重新查表并 claim。
这些字段在同一 entry 上不可变，不意味着 ID 永远指向同一个 entry。`putWithBlocker` 只拒绝当前在途 ID，
旧 entry 删除后相同 ID 可被重新使用。新 abandon 分支虽检查 verb/tier/committed，却未核对它仍是刚才授权的对象；
`abandonNotPush` 旁的注释只处理了“新 entry 变成 pull”，漏掉“仍是 push，但属于另一会话/创建者”。

**确定性复现：** 真 NATS、真 `handleFinalizeReq`、真 SQLite；合法 lab 创建者发 finalize。
测试占用唯一 DB connection，等 DB 的 WaitCount 增长，证明 handler 已读取旧 preview、正在 transferGate 等连接。
此时模拟另一终结路径移除旧 entry，再放入同 ID、不同 session/actor 的 tier-B push；释放 DB connection。
结果在旧 entry 为 push 和 pull 两种分支均相同：

```text
reply={OK:true Code: Error:} replacement_tracked=false replacement_finalized=true
FAIL: a finalize authorized against an old transfer claimed a different session's replacement
```

反例通过 tracker 的合法 remove/put 原语控制交错，不依赖概率碰撞；未模拟整段文件上传。
它证明 handler 会越过已验证的身份边界，写新传输的错误终态、删除新 tracker，真实接收/上传仍可能继续。
`markCommitted(id)` 及其它按旧快照验证后按 ID 修改的调用方也必须一起收敛，不能仅给 abandon 补一项 actor 比较。

**修复建议：** claim/commit 接受调用方已验证的 entry，在同一 tracker 锁内验证对象身份后再改状态；
旧 watchdog、cleanup、接收端事件及 pull finalize 同样绑定到其预览/持有的 entry。
增加实际 handler 的 DB 等待交错测试，以及同 ID/同 owner 重用也不能被旧回调终结的表驱动测试。

**处置：已修。** 三个 tracker 操作接收 expected entry，并在锁内比较对象身份；全部生产调用方传入原预览/持有的 entry。新增 2 个正式职责测试（共 6 个分支），原始独立 overlay 已由红转绿；/tmp overlay 移除三个身份守卫后 6 个分支全部变红。

## R3-F2 · MINOR：损坏的 regime.tsv 仍被报告为已知运行模式

**位置：** `test/simcluster/run-drills.sh:1164–1177`，REGIME 读取与输出。

reader 只检查第一行前两列 `REGIME:default|REGIME:live-grow`，没有检查字段数、cap/stagger 类型或多行冲突。
因此不满足既有复审所称“形状坏 → unknown”的契约。真实 runner 的独立 replay 输入结果：

| 原始 metadata | 生成的 REGIME | 退出码 |
| --- | --- | --- |
| 仅 `REGIME<TAB>live-grow` | `live-grow`，cap/stagger 为空；human summary 显示 uncapped | 0 |
| `REGIME<TAB>default<TAB>broken<TAB>-8` | `default / broken / -8` | 0 |
| 两行分别为 live-grow、default | 静默选第一行 live-grow | 0 |

**影响：** 截断、拼接或损坏的归档可被当作已确认的争用模式，回放收据与实际可知信息不符。
缺 metadata 的历史归档允许 unknown 的兼容策略本身没有问题；损坏 metadata 应采用相同的保守分类并明确原因。

**修复建议：** 要求恰一条四列记录、已知 mode、非负整数 cap/stagger；不满足则统一 `unknown - -`，
保持原始文件不变并在说明中区分“不可确认”与“已知 uncapped”。补齐空字段、非法数值、额外列、多行、缺文件和合法值的回放测试。

**处置：已修。** awk 验证完整文件恰一条四列记录、合法 mode 和非负整数；损坏时输出 unknown 并说明 missing or invalid。新增 7 种损坏输入回放测试，保留原文件字节；独立三例和 arm-aggregation 全通过，README 已同步。

## 首轮修复与其余审查面

- **F1 原始慢拒绝/慢成功：关闭。** 3 秒 open ctx 已传到可取消 adapter 锁等待和 TLS/REGISTER，安装前有 caller/Start fence。
  成功会话仍以 Start ctx 为寿命；AfterFunc 的取消传播不延伸到已成功打开的 session。
  全部生产调用方已迁移。现有报告所称“broker 总有至少 2 秒裕量”过强：state.json 写入与 NATS handler 排队不在这 3 秒 open 预算内。
  本轮确认的是首轮新增重试的边界，不声称整个 forward 在任意 I/O 卡顿下都有硬实时保证；这些既有路径未在本轮修改。
- **F2 tier-A 终结权：原始形状关闭。** tracked tier 在锁内检查，伪造 body Tier 无法切换权限；跨 entry 交错在 R3-F1 的授权修复中关闭。
- **F3 legacy 混合归档：关闭。** 独立重放旧父日志 + SRAB、缺 C，C=INFRA-ABORT、arms_missing=1、runner rc=1；显式选择也扫描完整 manifest。
- **F4 重建计数：关闭。** 独立复用真实 grow_to_3 和 grow_active，SIM stub 按生产协议尊重空 marker 环境变量；
  第二次 brk3 暂停时 marker_count=0、grow_active=1，成功后 fixture 写两行。相关 N=2 后置 guard/失败分支和 shell suite 同样通过。
- **F5 原模式保存：原始形状关闭。** live 单独写 regime.tsv、replay 保留它，三个 regime 参数与 replay 互斥；损坏输入在 R3-F2 的授权修复中关闭。
- **既有 R1/R2：** 放宽测试时序裕量和 AfterFunc 替换已落实；本轮 agent/tunnel 全包 race 通过。
- **既有 R3：已修。** silence 测试原先使用外部 DNS 名字；授权修复阶段已改为文档保留地址 `203.0.113.1`，保持“候选 broker 不同于静默 broker”的测试前提，相关 race 回归通过。

## 本轮独立验证

| 命令/实验 | 修复前结果 |
| --- | --- |
| agent/tunnel/broker 中 AddProxy、HandleExposeForwarded、OpenHome、ClaimAbandonedPush、PushCreatorFinalize、PushCommitHandoff，`-race -count=1` | 通过 |
| concurrency 的 BrokerRunCancel、AgentRunCancel、active TunnelServerClose、TunnelOpenCloseFDStable，`-race` | 通过 |
| `go test -race ./internal/tunnel ./internal/agent -count=1` | 通过，8.008 s / 56.074 s |
| `go test ./test/architecture ./test/determinism -count=1` | 通过 |
| `sh test/simcluster/tests/run-all.sh` | ALL PASS |
| 独立 transfer ID 重用 overlay 测试，`-race` | push/pull 两项失败，见 R3-F1 |
| 独立混合归档、重建计数、三种损坏 REGIME 回放 | F3/F4 修复有效；确认 R3-F2 |

复现/日志目录：`/tmp/tether-speed-r3/`，含 candidate.patch、identity-overlay.json、transfer_identity_test.go、
identity-before.log、regime-*、replay-mixed、grow-retry-adapted.log、focused.log、agent-tunnel.log、architecture.log、hermetic.log。

只读确认本机为 weilandserver（192.168.0.200），当前没有运行中的 simcluster 容器。
开发者镜像 #15 的收据在当前报告中未给出可定位的原始归档路径，本轮未找到对应 regime.tsv，故未把其“394 秒 MATCH”当作本轮独立证据。
本轮修复涉及进程内 transfer 身份和离线 metadata reader，均可由真实 handler/SQLite/NATS 与真实 runner 确定性验证；无需为这些修复重复启动完整集群。
此处不声称已重跑全部 Docker drills 或重新验证整个 simcluster-speed 性能目标。

## 授权修复与最终验收

修复前 staged 基线 SHA-256：`29887bd9cf74c745a573ba751cc423134c8de7a51a73c1dc87372ba884c74557`（130 文件）。
审查者的所有修复与本节更新均在暂存区外。R3-F1 / R3-F2 已修，既有 silence 测试的 DNS 依赖也已移除；
测试清单仅新增 2 个职责测试键，无删除或预算放宽。全部必要验收已完成，当前范围无未关闭的阻断问题。

### 已完成的修复后验证

- 新增 handler/entry 身份测试、原 tier/commit/finalize 回归、silence 测试：`-race` 通过。
- 原始独立 handler overlay：push/pull 均保留 replacement，`replacement_finalized=false`，通过。
- 去掉三个身份守卫的 overlay 变异：两个新测试的 6 个分支全部失败；生产工作树未被变异改写。
- `arm-aggregation-test.sh`：ALL PASS，含 7 种新增损坏 metadata 输入。
- `make gates` 的 vet-tags、Darwin build、全部 Go 门和完整 hermetic suite：通过；最后 lint 首次发现新增测试 3 处清理返回值未处理。
  已按现有 cleanup 写法修正并单独重跑 `make lint`，rc=0、0 issues；未把第一次 make gates 的 rc=2 写成通过。
- `make e2e-parallel`：rc=0，ALL PASS，67 个执行单元、17/17 顶层测试覆盖自检通过，用时 4m2.858s；未再串行重复全矩阵。
- 修复 R3-F3 后重跑 `make test`：rc=0，全部包通过；该目标同时执行 vet-tags 和 Darwin build。
- 最终 `make lint`：rc=0、0 issues；`git diff --check` 通过。

### R3-F3 · MINOR：全量验收暴露文件数据库清理竞态

首遍 `make test` rc=2，唯一失败为 `TestSimultaneouslyLaunchedClonesBecomeTwoDevices`：业务断言已通过，
`TempDir RemoveAll cleanup: ... directory not empty`。`test/p2/heartbeat_e2e_test.go` 的 openDB 清理仅调用 DB.Close；
Close 拒绝新查询，但已借出的连接可在稍后归还时才完成 SQLite 物理关闭。这样 WAL/journal 的收尾可与 TempDir 删除竞态。
修复保持 on-disk SQLite 测试语义，在 Close 后用有界条件轮询等 OpenConnections==0；超时仍报错，不忽略 RemoveAll 错误，
不靠固定 sleep 或移除真实存储来掩盖。原失败测试 `-race -count=20` 已全部通过，lint 也再次通过；重跑全量 `make test` rc=0。

### 最终证据与交付边界

修复后原始日志位于 `/tmp/tether-speed-r3/`：`identity-after.log`、`identity-mutant.log`、`fixes-focused.log`、
`arms-fixed.log`、`gates-final.log`、`lint-final.log`、`test-final.log`（首次失败）、`clone-cleanup.log`、
`lint-cleanup.log`、`test-final-rerun.log`（最终通过）、`e2e-final.log`。
临时日志是本机证据；正式回归测试和本报告保留在工作树，后续复验不依赖 /tmp 文件继续存在。

交付时 staged 二进制差分与 `review-baseline.patch` 逐字节一致，SHA-256 仍为上面的 `29887bd9…74557`。
修复阶段没有再次暂存；代码、独立测试、文档和最终报告更新均在 index 外，新增 `transfer_identity_test.go` 保持未跟踪。
未 commit、未 push。后续维护应继续保留 entry 身份校验与原始 metadata 校验，并为部署性能结论归档可定位的原始收据。

## 主进程复核（2026-09-20，§4 角色边界：审查者的实现改动逐处复核后才算采纳）

用户裁定"不能全盘接收，亲自检查和修改"。逐处结论：

- **R3-F1 · ACCEPTED，并延伸一处。** 复现成立且路径真实（validate-then-claim 校验的是 preview 的不可变字段，claim 却按 id 回表；
  `putWithBlocker` 只拒在途 id，终结后同 id 可重用）。三个 claim 改收 expected entry、锁内比对象身份的修法正确：调用方持有旧指针
  时新 entry 不可能拿到同一地址，指针身份是可靠判据；全部生产调用方（watchdog / ev.transfer / cleanupEntry / commit / finalize
  pull+push）都传了自己的 preview/entry。**主进程延伸**：`transfers.remove(id)` 三处（ev.transfer / finalizeTransfer / finalize.req）
  仍按 id 删——安全，但靠的是"claim 成功后 entry 仍在表里、put 拒重复 id、只有 claim 者删"这条两步论证；改为 `remove(expected)`
  锁内比身份后，tracker 只剩 `put`（拒重复）与 `get`（preview）两条 by-id 路径，论证不再需要。`TestTransferClaimsRejectReplacedEntries`
  加 `remove` 行；test 内 `copy` 变量改 `dup`（遮蔽内建）。**变异**：四个身份守卫各自单独去掉——ID1 → pull-finalize + finalize 行红，
  ID2 → push-commit + commit 行红，ID3 → push-finalize + abandon 行红，ID4（remove）→ remove 行红；四个同时去掉 7/7 红。
- **R3-F2 · ACCEPTED。** awk 整文件校验（恰一行、四列、mode 已知、cap/stagger 非负整数）与 7 种坏输入回归正确；文案 "missing or
  invalid" 同时覆盖缺失与损坏。**变异**：数值检查改恒真 → empty-cap / negative-cap / nonnumeric-stagger 行红（第一次我把条件行整个换掉
  弄坏了 awk 语法，所有 REGIME 行全 unknown——那是"语法坏"不是"守卫弱"，不算数，重做）。
- **R3-F3 · ACCEPTED，并做成结构。** 机理成立（`database/sql.Close` 只关闲置连接，借出的连接归还时才物理关闭，WAL 收尾与 TempDir
  的 RemoveAll 竞态）。审查者只修了 p2 一处；全仓同形状（文件型 SQLite 在 TempDir + 裸 `db.Close()` cleanup）另有 6 个 broker 测试文件
  7 处 `storage.OpenWAL("file:…")`——它们同样起后台 goroutine，同一条竞态在并行矩阵里迟早也会在那里出现。做成
  `testharness.CloseDBOnCleanup(t, db)` 一个 helper（Close → 有界等 `OpenConnections==0`，超时 `t.Errorf`，不吞 RemoveAll 错、不固定
  sleep），p2 与 7 处全部改用。`test/storage/pragma_test.go` 的两处文件型 open 是同步 QueryRow、无 goroutine，连接在 Close 前必已归还，
  不改。
- **R3（DNS）· ACCEPTED。** 复审 round 2 我把它交给下一个增量，理由是"改既有测试语义需要自己的审查"；用户这轮授权审查者改、并由我复核
  ——复核成立：测试要的是"避开静默 broker 的 host"，203.0.113.1 保留该前提且不再解析名字，`-race -count=8` 全过。plan §7 那行改为
  已在 round 3 完成。
- **文档**：审查者在 INDEX.md 另起了一行——INDEX 是"每个增量一行"，已并回 simcluster-speed 那一行（结论与链接都在）；#84 台账的
  "entry 身份栅栏"段落保留并补 `remove`；plan 加 §8.11。
- **复核后验证**：transfer 家族 `-race` ok（30.9 s）；p2 目标测试 `-race -count=3` ok；`internal/testharness` ok；
  arm-aggregation-test ALL PASS；四硬闸见 plan §8.11。随后 `git add -A`、commit、push（§3 step 7）。
