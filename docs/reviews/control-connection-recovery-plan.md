# 控制连接与隧道恢复：定稿 plan

2026-09-20；主进程定稿。基线 `cab90d1`。本轮完成内审后停在外审门，所有修改保留在暂存区外，不提交、不发布。

## 1. 编排与证据裁决

原生 agents 替代 Workflow：固定 transport/recovery/validation 三路独立草拟、互读质疑，由主进程裁决；实现仅主进程修改，专家可新增测试。内审仍固定三路完整审查并交叉核验。草案、互评和实验原始记录在 `/tmp/tether-control-recovery/`。

现场归档：`/home/weiland/tether-patrol/evidence/2026-09-20/README.md`。connz/broker UTC 与其他 agent CDT 对齐；history 无日期的 CALL/XFER 行不能单独归因。

| 编号 | 证据与裁决 |
| --- | --- |
| 1 | 原文件 66,549,440 字节，六条关闭，pending 8–12.9 MB，未触 max_pending、slow_consumers=0。依赖源码：ObjectStore.Get 有 ordered push 流控，但服务端能合并超过客户端 8×MaxPayload 的 WSS 帧。真 TLS WSS、默认阈值、1 MiB/s 复现两次 frame-too-large、34/52 控制 RPC 超时，最终摘要正确。实验 server 原因 Client Closed 不等于现场 Write/Read Error，不能宣称逐字一致。 |
| 2 | OpenHome 在检查已有 slot 前拨 REGISTER，server 先 bind 后替换，重放自己的活端口失败。修完全相同 desired configuration 的幂等性，保留身份/epoch fence 与真冲突。 |
| 3 | silence 只比 PublicHost 与当前 URL host，同 broker 多 alias 被算为其他 voter。须识别实际接入 broker，并确认存在不同合法 voter；不能简单禁用单项 roster，已退休当前节点可能只剩一个 survivor。 |
| 4 | broker 归档证明 EOF 后约 2.7 秒、5.6 秒已接受两端口重新 REGISTER；supervisor 成功不打印 opened，“四分钟没重拨”不成立。历史 payload 未采样，保持未知。补恢复成功日志与当前版本真实重启后 payload 验证。 |
| 5 | history map[string]any 将整数变 float64；保留 JSON 数字词法，保持拒绝尾随垃圾。 |

## 2. 实现约束

### 有界对象读取

agent push、ctl pull 共用 `jsstream.GetTransferObject(ctx, js, store, bucket, name)`。GetInfo 后精确订阅 `$O.<bucket>.C.<NUID>`，ephemeral AckNone pull consumer，每次最多四条，整批交给 reader 消费后才拉下一批，末批按剩余 chunks，无预取。内审补充：窗口必须由同一 NATS connection 的所有 reader 共享；Close 释放引用，最后一位 reader 移除 gate，防重复 commit 并发叠加窗口。对象记录的 ChunkSize 超过当前 MaxPayload 时明确拒绝。内审再发现：元数据可缺失或被改小，必须有实际字节上限；使用公开 legacy JetStream PullSubscribe(Bind) + Fetch(count, Context, PullMaxBytes) 绑定既有 consumer，每批同时限制四条与 min(5×MaxPayload, 32MiB)，低于客户端 min(8×MaxPayload,64MiB) 帧上限。仍用新 JetStream API GetInfo/Create/Delete，object 格式不变。校验 metadata bucket/NUID/link/size/chunks/digest、consumer 序列连续、stream 序列递增、最终长度/摘要；保留调用方 expectedSHA 与原子 rename。Close 取消 fetch 并有界清理 consumer；inactive 至少覆盖整个 transfer deadline 加余量，空对象不建 consumer。失败或取消不能把 SDK channel/Fetch 返回当作线上数据已排空：保持批次 permit，UNSUB 后 Flush/PONG 确认同连接旧数据已排空再释放；确认超时则保留 needsBarrier，后续 reader 必须完成屏障后才能 Fetch。引用从 Get 入口建立，覆盖 constructor 与 Close 竞态；归零后新 reader 先经历 GetInfo/Create 往返才可 Fetch。取消反例在默认真实 WSS 上于约五秒积累超过20MB帧，属于内审阻断并必须回归。

否决仅拆连接：独立 data 对照仍三次 frame-too-large；共享连接 bounded Fetch(4) 正控 68.50 秒完成原文件、254 次控制 RPC 全成功且无断线。选小窗口为核心修复；不改依赖/服务端上限、不伪造 browser UA，不把采样 pending 当严格上界，不承诺任意低速网络上的固定延迟。沿用对象发布格式和权限树。

### 隧道与 silence

OpenHome 持锁、dial 前判断 public/local/token/effective address/epoch/identity 相同，含正在 redial 的 slot；保留 gen、已有流、唯一 supervisor。已有同 home 空 pins 不清信任，非空 pins 可更新；无 pins 建立的旧 fallback session 不能凭同地址升级成已验证 named home，须核验已有 transport 的真实 peer 后才可升级；握手验证状态独立于可变 desired pins，随 swap 更新。升级 pin 若超越在途 insecure 握手，swap 必须验证新 transport 的 peer，失败关闭该 transport 并由同一 supervisor 继续重拨；取消/rename fence 保留，不同配置不能伪报成功。adapter 失败不删除已有 mapping。supervisor 安装成功打印 `tunnel: reconnected`。

register response 添加 optional/omitempty `connected_broker_id`，broker 用请求 ServerID 查 `nats_server` 映射，不能填 leader 自身。agent 身份绑定 connection/server 路由，迟到旧路由回复不能覆盖新路由；roster-only 携 ServerID。N-1 用已确认 host/route 映射，未知 alias 不推断不同 broker。nil roster 暂停 silence 判定但保留持久 pin/roster，过期 roster 不用来逃离。排除当前已知 alias 后必须有实际其他 voter；验证 retired→sole survivor。

history 使用 UseNumber 且检查 EOF；覆盖 66549440、超过 2^53 整数与 malformed/trailing JSON。

## 3. 验收与交付

- 真 WSS 原大小、慢链路、控制 RPC/连接 identity/摘要；空/坏对象、缺 chunk、取消、超时、consumer 回收、五循环 goroutine/fd；agent/ctl 接线及实际权限。
- 隧道存量流/新流、重放 register/gen 不变、不同 token/home/epoch、pins、取消/rename/redial；真实服务重启后同生命周期自主恢复。明确 tunnel.Server 与完整 broker/NATS 层证据。
- 单 broker alias、unknown alias、nil/expired/N-1 roster、真实 retired→sole survivor、旧路由迟到响应、自定义 NATS server name。
- 三路内审、交叉核验、逐条回复与修复。最终 make test、make e2e-parallel、make lint；并发 race/资源门；改门/清单则 make gates。lint 串行，inventory 只增，不降低预算。
- 不修改现网；本机 WSS 组件证据不是现网 canary。simcluster 无 WSS 前端，plain NATS drill 不可冒称 #1 验证。报告保留实际失败过程和剩余边界，停在外审门。

## 第二轮内审补充裁决

S1：启动cache仅在验签/账户/schema/有效期通过后进入可推导route身份的cachedRoster；pin与generation高水位仍保留。运行中已过期的旧roster不能在收到新回复时首次种入brokerID/aliases；此前在有效期内明确确认的同连接身份可保留，用于移除旧节点后的逃离。五臂真实cache+NATS refresh反例（含自然过期）均为修前红。

T2-F1：同epoch ApplyHome与OpenHome重放共用实际TLS peer验证；无pin fallback升级须校验当前peer并保存验证证明和显式home，拒绝后续无pin降级。既有已验证transport的合法纯pin轮换仍保留存量流。该路径可经cluster init --from-existing（保留epoch0、补home_broker）真实到达。

同epoch显式home可以是当前fallback的同一有效地址，不能在仍保留旧supervisor目的地时静默保存不同地址；不同有效地址须新epoch，本路径明确拒绝。

第二轮传输调用链补强：push-commit已经从接收消息的nc创建JS，不再通过session间可变a.js判空；删除残留读，避免把能力判断系于另一会话状态。未声称已复现跨代落盘。

S2：N-1回复缺少接入ID时，只有host/route匹配唯一不同NodeID才可固化runtime brokerID；多个不同节点共享host不构成唯一身份。显式接入ID仍优先，不能被歧义host抹除；同一节点的多个别名不产生歧义。真实签名roster替换+实际session取消反例修前红。

T2-传输-F1：legacy PullSubscribe的nats.Context只覆盖subscription选项，内部ConsumerInfo读取legacy JS自身上下文。构造legacy JS时同步传nats.Context(ctx)，使Bind INFO也受整个transfer取消/预算约束；实际扣留INFO响应反例修前1秒内未进入清理。清理仍使用独立有界WithoutCancel上下文。

Bind第一修原样三轮仍红，原因是固定SDK getJSContextOpts未继承JS defaults.ctx。最终不用该无效设置：显式Bind我们刚由新API创建成功的consumer，SkipConsumerLookup避免重复INFO，AckNone对齐消费者配置；因已Bind，不会走SDK新建consumer分支。增强为“构造提前返回或真实INFO发布后取消”的双路径oracle，修前仍红，不以仅前置ctx检查掩盖在途lookup。最后window归零后新reader依靠GetInfo/Create的真实同连接往返确立旧UNSUB排空顺序。

第二轮交叉后的主进程边界复核：上述同epoch地址守卫须限定于“已经named”的slot。InitFromExisting的TunnelAddr是operator-provided，可能与旧fallback不同；首次命名升级允许同epoch新advertised地址，须验证实际peer并保持已有transport。redial在现有mu+generation守卫内同时快照当前addr和pins，之后使用新的named地址。此首次命名例外替代先前一刀切的不同地址拒绝，仍拒绝已命名home的同epoch变址。新增真实双控制地址/相同证书、关掉旧endpoint后payload恢复的反例，不能只断言缓存。
