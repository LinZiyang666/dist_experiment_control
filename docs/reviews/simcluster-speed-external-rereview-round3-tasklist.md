# simcluster-speed 独立外审 round 3 tasklist

2026-09-20。HEAD `a3431a12be3cf947cc797c28fea93382fa68472d`。本轮开始时 128 个文件已暂存，
无未暂存/未跟踪文件；开发者修复与既有 round-2 Pass 报告均已在 index 中。
据用户“开发者修改后重审”的要求，实际对象为此候选中的首轮 F1–F5 修复、round-2 R1–R3 处置及其连带面。
已有报告和主进程回复只作待证主张。本轮独立验证结果另行记录，不沿用其 Pass。

用户追加授权：先完成复审并暂存开发者候选、tasklist 和修复前报告，再直接修复至可放行；
之后审查者的实现、测试与报告更新全部留在暂存区外，不再执行全部暂存，不 commit/push。
开始时 staged 二进制差分已保存 `/tmp/tether-speed-r3/candidate.patch`，用于核对基线漂移。

## A. 边界与契约

- [x] A1. 核对 CLAUDE.md §4.1、测试规范、首轮报告回复、既有复审报告与 tasklist；确认初始 index/工作树边界。
- [x] A2. 粗读 F1–F5 的当前实现、新增测试和回放/fixture 改动，先写本 tasklist。
- [x] A3. 核对需求/架构中的数据面寿命、传输终态、演练证据契约；确认签名迁移、所有调用方与兼容面。

## B. 产品修复与反例

- [x] B1. F1 总预算覆盖锁等待、握手、安装；迟到成功、取消优先级、慢 state 写与 handler 排队的预算边界。
- [x] B2. 握手 ctx 与 session ctx 分离；成功后取消、Start 取消、AfterFunc 回调已启动/未启动、连接/协程回收。
- [x] B3. OpenHome 安装 fence 与并发 rename/close/rehome 交错；失败不能破坏已有会话和本地端口映射。
- [x] B4. adapter 信号量所有 acquire/release 路径、取消等待、ApplyHome/RemoveProxy 串行及零值/构造路径。
- [x] B5. 重试分类：瞬态拒绝后超时/取消、无先例超时、永久拒绝、无 deadline；错误链与 CLI 语义。
- [x] B6. F2 tracked tier 与 actor/session 权限；伪造 Tier、tier-A 接收中、tier-B commit/abandon/watchdog 竞争及唯一终态。
- [x] B7. 新增测试的时序前提与判别力；核验 R1/R2 处置并判断 R3 是否需要本轮修正。

## C. Harness 与证据完整性

- [x] C1. F3 全臂/选中单臂/混合旧父日志/缺臂/损坏日志，legacy 不替代缺失证据；聚合与退出码一致。
- [x] C2. F4 grow_to_2/grow_to_3 失败、nuke 重试、后置检查失败与命令替换；完成 marker 不能早于当前 fixture 成功。
- [x] C3. F4 declared grows 与所有 fixture 调用位置、重复 fixture/runner retry、独立 grow 的计数相容。
- [x] C4. F5 live 写入/回放读取/缺失/损坏 metadata、参数互斥、目录重用、只读归档、不可写输出的失败行为。
- [x] C5. 用独立边界输入验证 shell 新守卫；运行相关 hermetic suite，检查断言并未因测试通过而空转。

## D. 验证与修复前交付

- [x] D1. 独立运行 agent/tunnel/broker 相关 -race 回归、架构/确定性与受影响 leak 门；记录真实命令和退出状态。
- [x] D2. 核对相关部署收据的实际文件与代码身份；按新增风险判断是否需要真 simcluster 单项，不把既有报告当本轮实测。
- [x] D3. 形成以 Fail/Pass 开头的 round-3 报告，写明疑惑、问题、建议、复现与未验证边界。
- [x] D4. 固化开发者候选与修复前报告/tasklist 到 index，记录 staged 内容摘要；此后不再改写基线。

## E. 授权修复与最终验收（全部留在暂存区外）

- [x] E1. 按确认的问题实施最小修复并添加职责命名回归；先证明修前失败，修后通过。
- [x] E1b. 完整 make test 暴露 p2 文件数据库清理竞态；保留真实文件数据库，在 Close 后等待 OpenConnections 归零再允许 TempDir 清理，补充重复验证。
- [x] E2. 完成相称的 race/leak、shell、架构/确定性及必要发布闸门；不重复无新增风险的测试。
- [x] E3. 更新最终报告和 tasklist，逐项闭合或如实记录边界；确认 index 不变、审查者修改均在其外，并给出最终结论。

## 修复前执行结果

A/B/C/D 的已执行项：首轮原始形状关闭；新增 R3-F1（transfer ID 重用跨 entry claim）与 R3-F2（损坏 REGIME）已独立复现。
相关 race/leak、agent/tunnel 全包、架构/确定性、hermetic suite 全通过；部署收据缺可定位原件，本轮不引用其为独立证据，
且本轮新增问题不需要 Docker 才能复现。D4 固化后开始 E；最终结果见同名 round-3 报告。

## 已固化的边界与授权修复

D4 完成：130 个文件的 staged 差分 SHA-256 为 `29887bd9cf74c745a573ba751cc423134c8de7a51a73c1dc87372ba884c74557`；
修复前报告与本 tasklist 已进入 index。此后所有改动仅写工作树。
E1 完成：三个 tracker 操作及全部调用方绑定 entry 身份，新增真实 handler 交错和同 owner 重用测试；
REGIME 严格验证完整单行四列及数值，新增 7 种损坏输入回归；silence 测试移除外部 DNS 名字。
原独立 overlay 由红转绿；去掉身份守卫的 /tmp overlay 变异使两个新增测试所有分支变红；arm-aggregation ALL PASS。

E1b 完成：原清理失败测试 `-race -count=20` 通过；全量 `make test` 重跑 rc=0。
E2 完成：最终 `make lint` rc=0；gates 中全部非 lint 检查通过，首次 lint 三项清理返回值问题已修并重跑通过；
`make e2e-parallel` rc=0、67 个执行单元 ALL PASS、用时 4m2.858s。
E3 完成：最终报告首行 Pass，适用于含审查者修复的完整工作树；index 仍保留修复前 Fail 快照且字节不变。
报告索引已补充本轮入口；所有后续实现、测试、文档更新留在暂存区外，未 commit/push。
