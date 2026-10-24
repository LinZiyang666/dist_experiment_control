# 2026-09-20 现场巡检证据与处置（待外审）

归档：`weilandserver:~/tether-patrol/evidence/2026-09-20/README.md`。
完整设计、反例与验收见 [control-connection-recovery plan](control-connection-recovery-plan.md)。
以下是现场巡检来源，不冒称已有 simcluster deploy drill 覆盖；本轮不部署、不改现网。

### 用户反馈 1 — tier-B 大对象下载使共享 NATS WSS 控制连接断开

现场精确文件 66,549,440 字节，四个目标共六次关闭，server pending 8–12.9 MB，
未触 64 MiB max_pending，slow_consumers=0，journal 无相应服务重启。ObjectStore.Get 的 ordered push
有流控，但窗口可增长至 32MiB；server 合并 WSS frame 后可超过客户端 min(8×MaxPayload,64MiB)。
本机真实 TLS WSS、默认阈值、慢下行已得到明确 frame-too-large；与现场关闭原因不是逐字等同。
候选：agent push/ctl pull 同连接共享四块窗口，加实际 pull bytes cap；完整消费再下一批。
取消先 UNSUB + PONG 屏障，失败屏障由下一 reader 继承；不能仅等 SDK 返回就放行。
`internal/jsstream/object_wss_test.go` 两个真实 WSS 回归分别验证完整大文件和连续取消，
另有并发、历史大块、异常元信息、资源回收及真实 auth_callout 证明。旧 agent 的下载路径须升级才受修复保护。

### 用户反馈 2 — 自己的活 expose 在 NATS 重建后重放报 public_port_bind_failed

OpenHome 先 REGISTER 后查已有 slot，server bind 自己还占着的端口。
候选：同一 desired configuration 在拨号前保留原 slot/supervisor/字节流；不同身份/token/home/epoch仍受守卫。
空 pins 的持久重放只能保留已有信任；旧 unverified transport 升级为 pinned 必须真实核验 peer，
在途 unverified 握手被 pin 升级超过时安装前再核验，失败由同一 supervisor 继续重拨。
`internal/tunnel/open_replay_test.go` 钉住字节流与证书竞态，不能通过关闭旧隧道消除误报。

### 用户反馈 3 — 单 broker 多地址被 silence 逃逸误算成“其他 voter”

原比较仅看 PublicHost 与当前 URL；WSS 域名和 route 别名导致约30秒抖动触发无意义重建。
候选：register 的 optional connected_broker_id 使用请求 ServerID 查实际接入 node_id（非回复 leader）；
运行时绑定 connection/server/URL，锁内防迟到覆盖；nil/过期 roster 不用于逃逸，N-1未知别名保守停留。
已知当前 broker 被移除而 roster 只剩一位 survivor 仍可迁移；真实双 NATS + 同 Agent.Run + 命令响应测试证明落点，
不是仅观测一个 rebuild hook。当前 broker 的全部已知 alias 一次性排除。

### 用户反馈 4 — broker 重启后隧道恢复日志不足，曾被误判“四分钟未重拨”

**证据订正**：0.5.0 归档 broker 在 EOF 后约2.7秒与5.6秒已接受两个端口的重新 REGISTER。
agent 的 supervise 成功路径不打印 opened，故不能由 opened 缺失推断未重拨；历史 payload 状态仍未知。
候选补 `tunnel: reconnected` 日志。当前版本已由真实 tunnel 服务五次重建及完整 broker/持久 DB/稳定证书/NATS
不同恢复顺序证明，同一 agent 无重执行、无手工重开即可恢复端口 payload。此证据不追溯证明历史 payload 正常。

### 用户反馈 5 — history 将整数字节数转 float64，科学计数并丢失大整数精度

候选用 JSON UseNumber 保留数字词法并继续严格拒绝尾随 JSON/垃圾。
`cmd/tether/history_render_test.go` 验证66549440与超过2^53的精确整数及无效输入。


## 归档身份（SHA-256）

归档原文件只读，未复制或修改38MB现场数据。以下摘要可核对本轮所读版本：

```text
cf846ef827a08663c08749a42d87314ed2f8592f985db3f69fa1261d27f7494d README.md
854d782366bc286c80e158c38dff724139393ca7a208ede69eeabb508ceb3a02 connz_closed_agents.tsv
a55d29d2bf826f0cf7893297d57450d5b9aab2a882f8a9d6dabba48a5d9887e2 connz_closed_full.json
5e74a194aef406ec263dfc005f026d6c90cb2729c15dfc72109d04f2e177fb3d varz.json
2fc480046c7ec104bbc4683d7ded179fbdcff4b8f17d979e39b4aceef806c364 broker_log_2026-09-17_2120-2135Z_restart_upgrade.txt
ad55d383dbb82e9ba5912ea77b8c1ca9f40f05892275e719bd8ff2ba2a599b96 broker_log_2026-09-20_1730-1736Z_tierB_push.txt
```

初次把五项分配为#90–#94被ledger-crosscheck正确拒绝：该编号族必须有deploy矩阵中的非GREEN owner。
本轮只有明确记录层级的现场/组件/集成证据，不能编造drill ownership。五项完整保留在本文件与综合报告，
当前架构记录约束，deploy台账留可发现入口；编号没有发布，现有#25–#89及其门保持原义。
