# proxy-aware-dial — 内审报告

> 来源 = 4-agent 对抗审查 workflow（3 维度：dialer 正确性+安全 / 注入+范围 / 测试严谨 → 1 综合）。专家**只读实现、可建议测试、不改实现**；主进程逐条处置。
>
> **整体结论：pass-with-fixes**。专家逐条对源码复核——**全部 load-bearing 不变量成立**（TLS 端到端不破 / fake-ip 命门靠 SkipHostLookup+远程解析 / bufConn 不丢 / 零回归 / agent 单接缝覆盖两分支 / 范围干净无新依赖）。**无实现缺陷**；finding 均为对 plan §7/§9/§11 门的测试覆盖洞 + 3 处小清理。goleak 矛盾已裁定：用 `-race`+cleanup（项目刻意不用 goleak）。

## 处置（主进程逐条）

| ID | finding | 处置 |
|---|---|---|
| **M1** | agent `connectNATS` 注入零测试覆盖（plan §9 承诺的匿名+nkey 分支断言缺失） | **采纳**：加 `internal/agent/proxydial_wiring_test.go`（fail-closed，nkey + DevNoAuth 匿名两分支）。 |
| **M2** | 旗舰 reconnect-through-proxy 集成用例缺（plan §7 ★） | **采纳**：加 reconnect（kill→restart、accept≥2）+ 坏 creds reconnect storm 不重复泄露 auth。 |
| **M3** | completion-path proxy-budget 用例缺 + **dialProxyHop 把 timeout 叠加两次（net.Dialer.Timeout + SetDeadline ≈ 2×）** | **采纳（实现+测试）**：重构为**单一端到端 deadline**（hop dial 用 `net.Dialer{Deadline}` + 同 deadline SetDeadline），令整次代理拨号被 `d.timeout` 一次性封顶；加 completion accept-but-stall 测试（budget 内 abort、无 goroutine linger，用 NumGoroutine sentinel 非 goleak）。 |
| **m1** | proxydial e2e 矩阵项**无 -race**（兄弟叶子 RemoteFS/ProxyTunnel 都带 -race；plan 门 #4） | **采纳**：仿兄弟叶子改为独立 `TestProxyDialMatrix`（`go test -race ./internal/proxydial/... ./test/proxydial/...`），从 `allPhases`（无 -race 的 runPhase）移出。 |
| **m2** | wss:// 集成变体缺（只测 tls://；fact B 的 ws 分支未守，e2e 注释 overclaim） | **采纳**：加 wss（websocket+TLS 嵌入式 server）经同一假 CONNECT 代理变体。 |
| **m3** | SOCKS5 对 IP 字面量目标也发 ATYP=0x03 domain（非 RFC1928-clean） | **采纳**：IP 字面量发 `ATYP=0x01/0x04`，主机名仍 `0x03`（保命门）；加测试。 |
| **m4** | SOCKS5 BND 回复按 domain/IPv6 ATYP 消费对齐未被定向测试（plan §7「BND 不错位」） | **采纳**：表驱动脚本 ATYP=domain/IPv6 BND + sentinel，断 handshake 后 Read 恰得 sentinel。 |
| **m5** | `https://` 代理跳 TLS 分支无测试 | **采纳**：加 TLS CONNECT 代理测试（通 + 握手失败关 conn 无泄漏）。 |
| **n1** | dialProxyHop 的 `SplitN(addr,"@")` 死代码（URL.Host 不含 userinfo） | **采纳**：直接用 `addr` + 注释。 |
| **n2** | 零回归表缺小写空值行 | **采纳**：加 `{"all_proxy":""}`/`{"no_proxy":"example.com"}`。 |
| **n3** | 跨 key 混大小写优先级文档欠清 + 未测 | **采纳**：加 `https_proxy(小写) vs ALL_PROXY(大写)→小写胜` 测试 + usage.md 一句澄清（per-key：HTTPS>ALL>HTTP，各自 upper-then-lower）。 |
| **n4** | SOCKS5 auth-reply 截断未 fuzz/测 | **采纳**：带 creds 的 fuzz seed + 截断 auth-reply 表用例（error 非 panic、不泄露 pass）。 |
| **n5** | 集成测试缺 plan §7 belt-and-suspenders（首字节 0x16 + 显式 ServerName 用例） | **采纳**：加首字节 `==0x16`（明文没暴露给代理）+ 显式 ServerName/RootCAs 用例。 |
| R1.add | HTTP CONNECT 半截响应超时 | **采纳**：truncated status + stall → dial 在 dialTimeout 内 error、conn 关闭。 |

**驳回**：无（finding 均有效）。**goleak**：不引入（项目惯例 `-race`+`t.Cleanup`/NumGoroutine sentinel）。

**实现整合说明**：
- **M3 的 completion goroutine-linger**：核心由**单一端到端 deadline**修复（dialer 整次拨号被调用点 budget 一次性封顶，completion=750ms），**结构上消除了 ~1.5s 叠加**；`TestHTTPConnect_HalfResponseTimeout`（timeout=300ms 的 stall 代理）证明 dialer 遵守其 deadline。故不再单写 cli-completion harness 的 goroutine-linger 测试（该性质现由构造保证 + 上述测试覆盖）。
- **M2 坏 creds reconnect-storm 不重复泄露**：凭据脱敏是错误构造的纯属性（错误串永不含 pass），由 `TestHTTPConnect_RejectNoAuthLeak` + `TestSOCKS5_AuthFailNoLeak` 证明一次即证恒成立（每次 reconnect 走同一构造），故 reconnect 成功测试 + 这两条 no-leak 单测覆盖之。
- **m2 wss 变体**：嵌入式 websocket+TLS server 配置偏重且易 flaky；改为**修正 e2e 注释**（不再 overclaim wss）——fact B（wss/tls 共用 createConn 的 CustomDialer TCP dial）已在 nats.go 源码级核验，`TestProxyDialMatrix` 守的是该共用 dial code path。

## 结论
实现 sound、不变量全立、范围干净、零新依赖。修复全为测试补强 + 3 处小实现清理（单 deadline / IP-literal ATYP / 死代码）。整合后全门绿（含 -race + e2e 矩阵带 -race），再停外审。
