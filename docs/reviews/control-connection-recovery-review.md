Pass — 第一轮内审发现已修复，完整闸门通过；用户追加的第二轮内审也已完成，最终出口见[第二轮报告](control-connection-recovery-review-round2.md)，本文不代替独立外审。

# 控制连接与隧道恢复：内审与开发交付

2026-09-20。基线 HEAD `cab90d1dcafe6badc4d68a9492c2a2b2ad6cc025`；主进程为开发者，用户要求完成内审修复后停在独立外审门。初始 index/worktree 干净；本轮所有修改保留暂存区外，不提交、不部署。

入口：[plan](control-connection-recovery-plan.md) · [tasklist](control-connection-recovery-tasklist.md)。

## 1. 五项现场反馈的处置

| 项 | 结果与证据 |
| --- | --- |
| #1 大文件 push 压断控制 WSS | 真 TLS WSS、默认服务端阈值、66,549,440B 原大小、2MiB/s 下行复现旧 Get 三次 frame-too-large、14/60 RPC 失败。最终采用同连接共享四块窗口 + 实际字节上限 + 取消 UNSUB/PONG 屏障；同一正式正常测试 194 RPC 全成功、同 CID、零断重连、长度/SHA正确；64次取消测试129 RPC 全成功。 |
| #2 活 expose 重放误报 bind failed | 同配置在拨 REGISTER 前保留 slot/supervisor、存量流及后续流；不同 token/local/home/epoch、身份代与取消仍有守卫。内审又修三种 pin/在途握手竞态，真实 TLS 反例转绿。 |
| #3 单 broker 被误判 retired island | additive `connected_broker_id` 标识实际接入 broker；已验证 roster alias、nil/过期、迟到响应和全部当前别名排除。真实 Agent.Run 从不断线但停止回复的旧 NATS 迁到唯一 survivor，并通过该连接收到命令响应；错误目标 mutant 明确失败。 |
| #4 声称旧 agent 四分钟未重拨 | **原判断部分被原始证据推翻**：broker 在 EOF 后约2.7秒和5.6秒收到两个端口重新 REGISTER。补 `tunnel: reconnected` 日志；当前版本真实 broker/持久DB/稳定证书/NATS 两种恢复顺序，在同 Agent.Run 下自主恢复新 payload。历史 payload 没有观测，保持未知。 |
| #5 history bytes 科学计数 | UseNumber 保留数字词法；66549440、2^53+1、MaxInt64 输出精确；拒绝尾随 JSON/垃圾的旧行为保持，去掉 EOF 守卫的 mutant 红。 |

原始现场归档为 `/home/weiland/tether-patrol/evidence/2026-09-20/`。broker/h100/connz UTC 与其余 agent/history CDT 按 -5 小时对齐。现场 Read/WriteError 与本机 Client Closed 的文字不同；本轮复现并消除了匹配现场条件的具体 WSS 缺陷，不能声称历史每一次断线的物理原因已唯一确定。

## 2. 编排与独立内审

固定三路设计草拟→交叉质疑→主进程裁决；主进程唯一修改生产代码。三个专家独立新增行为测试并审完整候选，彼此交换反例和证据。本轮没有把内部代理称为外部审查者。

- [传输内审](control-connection-recovery-review-transport.md)：最终本路 Pass，五项发现闭合，13个独立 Test。
- [恢复内审](control-connection-recovery-review-recovery.md)：最终本路 Pass，四项发现闭合，16个独立 Test，关键路径三轮 race。
- [验证内审原报告](control-connection-recovery-review-validation.md)：保留其原始 Fail，C1 与传输 T-R1 为同一缺陷；该专家完成修后 WSS/ACL 正式测试后，生成最终回复时工具报错。**没有把其报告改签为 Pass**。主进程亲读最终进程日志，并由另外两位专家交叉核验 C1 修复与相同反例转绿；最终综合裁决由主进程承担。

### 逐项 finding 回复

| Finding | 裁决、修复与回归 |
| --- | --- |
| T-C1：每 reader 四块仍可跨 reader 累积 | 采纳。20 reader 原样可送80块；改为按 *nats.Conn 共享批次 gate。20 reader 同时暂停首字节时总投递≤4，全部恢复后各自字节正确；另一连接不受阻、排队者取消不偷 permit。 |
| T-C2：metadata 声明块数后额外尾块被忽略 | 采纳。每条 NumPending 与剩余 chunks 一致，保留消费者序号/stream序号、长度、摘要双层校验。真 stream 追加尾块、缺块、重排及坏 metadata 原反例转绿。 |
| T-C3：固定 inactive/Fetch 时间缩短既有预算 | 采纳。inactive覆盖完整剩余ctx预算加余量；Fetch继承caller deadline，不另加30s/新SDK默认heartbeat。真实11min deadline下consumer配置检查通过。 |
| T-R1 / V-C1：取消后线上旧块未排空就放下一批 | 采纳。初修真实WSS仍在约5秒出现超过20MB帧。失败/取消保持permit直到UNSUB→PONG确认排空；屏障超时由后续reader继承，成功前不发MSG.NEXT。64reader同一WSS反例转绿；真实扣留PONG/删除回复的异常清理测试证明有界Close、后续安全等待、恢复后完整。禁用继承屏障mutant红。 |
| T-R2：metadata ChunkSize不足以限制实际旧chunk | 采纳。真server写256KiB块后MaxPayload降32KiB，缺失/伪小Opts原来仍读出危险块。改用公开legacy Fetch同时限制消息与真实bytes，cap=min(5×MaxPayload,32MiB)，部分batch不能冒报完整成功；三臂同反例转绿。 |
| R1：unverified transport被快通路径当成pinned | 采纳。核验真实peer；每次握手验证结果随transport安装，不从可变desired pins推断；pin升级超越insecure在途握手时安装前重验，失败关闭结果并继续同supervisor。初始升级、跨redial证明继承、已在途握手三组真实反例均转绿。 |
| R2：adapter缓存写了未安装的stale配置 | 采纳。错误恢复旧值，成功/no-op后按实际slot的LocalPort同步，无slot清缓存。现有数据路径捕获localPort，未夸大为已证实流量损坏。 |
| R3：迟到writer覆盖新route身份 | 采纳。rosterMu内复查ncBox；入口顺序号拒绝较旧调用覆盖已采纳较新回应，覆盖same-nc自动重连；锁内不调用可能阻塞的NATS observer。特定锁交错以静态锁序证明，未冒称新增确定性调度测试。 |
| R4：expiry绕过注入时钟 | 采纳。显式传agentNow；expired-roster反例转绿。 |

额外验证纠错：旧 auth_callout/CLI pull替身仍用 store.Get，不能证明新helper权限。已只对齐这两处真实读取路径并检查Close结果，原跨sid拒绝等断言保持；两向真实auth_callout及CLI pull回归通过。

## 3. 测试收据

原始命令和日志在 `/tmp/tether-control-recovery/`。最终候选未升级依赖或放宽 server 阈值/ACL、结构预算；wire/test inventory只增，旧Background豁免从40减至39。内建 goroutine/fd 门至少五循环，无新增goleak。

| 验证 | 最终结果 / 日志 |
| --- | --- |
| 原大小 WSS + 连续取消，race | PASS59.298s；`wss-formal-final.log`。正常38.897s、194RPC0失败、max284ms；取消64reader16.053s、129RPC0失败、max271ms；零断线。采样pending仅诊断，不作为严格上界证明。 |
| 新reader核心行为，race | PASS8.054s；`object-bounded-bytes-root.log`；后增历史块与失败屏障分别PASS1.418s/5.287s，见对应日志。 |
| 恢复/identity独立测试，race×3 | PASS；`recovery-final-race-tests.log`，order补强后`recovery-route-order-race-tests.log`。 |
| 完整broker/DB/NATS重启，race | PASS7.066s；`broker-restart-full.log`；两个窗口新payload约294ms/538ms恢复。 |
| 唯一survivor实际接入，race及mutant | PASS；`silence-sole-survivor.log`；错误目的地mutant原样FAIL，旧节点22次register/survivor0次。 |
| 真auth_callout两向传输 / CLI pull，race | PASS7.112s/3.582s；`transfer-auth-final.log`、`transfer-cli-final.log`。 |
| make test | PASS（exit 0），`make-test-final.log`；初次仅清单漏登记见`make-test.log`。 |
| make e2e-parallel | ALL PASS，67个单元，4m15.613s；`make-e2e-parallel.log`；完整矩阵，未以串行矩阵替代。 |
| make gates（含make lint） | PASS（exit 0），`make-gates-close.log`；Go套件与simcluster hermetic全绿，lint 0 issues。 |

保留真实过程：初次架构/确定性检查发现新增b.cfg.DB读取绕过read facade、Background cap未同步收紧、泄漏循环使用gate不解析的selector常量、清单还缺内审新增项；已分别改只读QueryRow、cap39、等值具名循环常量及只增登记。未通过放宽门消除错误。完整make test初次仅因并行新增最后一个测试尚未登记而红，已只增登记后重跑。make gates初次正确拒绝把现场反馈错误分配到有deploy owner要求的#90–#94编号族；五项移至[证据归档](control-connection-recovery-evidence.md)，原台账留入口，未编造drill或改判owner。第二次 make gates 的 lint 报10项：测试 helper 参数顺序、切片写法和死赋值、token谓词、过时 Agent wrapper及注册处理函数复杂度；已逐项修复并跑相关race，抽取只读刷新回复职责，删除过时wrapper并将Agent方法预算127收紧至126，未新增lint豁免。`make-lint-close.log` 为0 issues；`register-refactor-race.log` 为完整/刷新真实NATS注册回归PASS。测试fixture中正常取消退出判定、atomic nc字段赋值的编译问题均修于测试，不冒报生产缺陷。

## 4. 疑惑、建议与覆盖边界

1. **现场归因边界**：归档agent没有原始DisconnectErr，已补断线错误日志；本机WSS严格复现了超帧机制，但不证明现场六次关闭只有这一种原因。下一次批准部署后可用相同文件及控制请求、CID、重注册记录核对canary，本轮没有操作现网。
2. **历史#4**：秒级REGISTER有证，历史payload仍未知；当前in-process真broker/SQLite/NATS/agent的生命周期测试不覆盖systemd/Caddy/WAN。
3. **兼容与更新范围**：对象格式与sid权限不变，identity字段optional/omitempty、签名roster未变；已做源码/wire/真实权限验证，未运行released N-1二进制混部矩阵。旧接收端agent不升级仍使用旧Get。
4. **在途传输**：维持架构的best-effort语义；session重建可以使在途失败，不承诺断点续传。ctx在handler开始执行时取，terminal身份仍取运行时nid；不是所有字段的订阅接收代不可变快照。本轮未证实跨代成功落盘，专家保留后续收紧建议，不把猜测列成已确认缺陷。原子rename之后同时到来的取消不能追溯撤销已完成写入。
5. **审查边界**：本报告是开发与内审交付，独立外审须从index重新查看全部未暂存/未跟踪内容，不能依据内审Pass直接放行。当前没有提交、暂存或部署授权动作。

## 5. 外审入口

最终门与第二轮已完成，见[第二轮内审与回复](control-connection-recovery-review-round2.md)。外审者读取CLAUDE.md、本plan/tasklist、三路原报告和本回复，再独立复验。根目录生产变更集中于对象reader与两端接线、tunnel/adapter、roster/register identity、history；测试/台账/清单同属审查范围。


用户追加（2026-09-20）：第一轮门完成后再进行第二轮独立内审与主进程修改。tasklist §D现已完成；第一轮记录保留，最终外审交付以第二轮报告为入口。
