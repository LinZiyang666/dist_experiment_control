# proxy-aware-dial — plan（post-1.0 叶子增量）

> 来源 = 6-agent 对抗 workflow（3 视角起草 → 2 红队对抗 → 1 综合）的综合候选，经主进程评估 + 对 5 个 open decision 拍板。**主进程是唯一定稿人；本文件是实现的唯一实现尺。** 所有 load-bearing 源码事实已核验（行号见 §0）。
>
> 语言：中文叙述；代码/标识符/env/subject/配置键英文（CLAUDE.md §5）。

## 问题与目标
tether 是静态 Go 二进制（CGO_ENABLED=0），**全仓零代理处理**，靠透明 TUN 出网。用户在 WSL（镜像网络 TUN 坏）上 `tether ps` 永久 hang：broker `weiland.top:443`（真 IP 墙外、需代理），域名被 Clash 解析成 fake-ip（198.18.x.x），WSL TUN 承载不动 → TLS 握手卡死；但本地 7897 代理口可达 broker。**目标：给 tether 控制面加 proxy-aware dial，设了代理 env 时经代理连 broker。**

---

## §0. 已核验的源码事实（所有裁定的地基）

| 事实 | 出处 | 结论 |
|---|---|---|
| **A. fake-ip 命门** | `nats.go@v1.52.0:2329-2330`：`if !SkipHostLookup && net.ParseIP(host)==nil { net.LookupHost(...) }` | nats.go 默认本地预解析主机名 → WSL 上 `weiland.top` 变 fake-ip 才传给 dialer。**必须配 `nats.SkipHostLookup()`**，让主机名原样到 dialer、由代理远程 DNS。 |
| **B. wss 命门** | `nats.go:2356`(dial) 在 `2367`(ws 分支) 之前对**所有 scheme** 执行 `dialer.Dial("tcp",host)`；ws 的 TLS 在 `wsInitHandshake`→`makeTLSConn` | CustomDialer 在 wss/tls 下只承载裸 TCP，TLS 端到端保留。无需替代方案。 |
| **C. TLS 不破** | `nats.go:2377-2385`：`SkipTLSHandshake()` 是可选断言方法 | dialer **不实现**它 → nats.go 照常握手。 |
| **D. SNI 来源** | `nats.go:2409-2414`：`makeTLSConn` 的 ServerName 取 `tlsName`/`URL.Host`，**与 dialer address 无关** | `SkipHostLookup` 不污染 SNI/证书校验。**须有「不显式设 ServerName」的测试守生产默认路径。** |
| **E. 零回归约束** | `nats.go:2343-2348`：`CustomDialer != nil` 时直接用它、**跳过** `copyDialer.Timeout/len(hosts)` 多-IP fan-out | 决策必须在 **option 拼装层**（加/不加）；**严禁**「永远挂 no-op dialer + 内部 fallback net.Dial」（那会永久改默认多-IP 行为）。 |
| **F. agent seam** | `agent.go:947 buildConnOptions` 有**两个 return**（匿名/nkey），**无 merge point**；`a.cfg.NATSURL` 在 receiver 上（`agent.go:50/596`） | **在 `connectNATS`(`agent.go:596`) 单点注入**，覆盖匿名+nkey 两分支。 |
| **G. x/net 是全新依赖** | `go.mod` 无 x/net；`go.sum` 零 v0.52.0 content hash | 引 x/net = 新增 direct dep（非「提升 indirect」）。 |

绝对路径：`internal/cli/natsconn.go`(:41)、`internal/cli/completion_transport.go`(:134 生产 / :286 test-only)、`internal/agent/agent.go`(:596 connectNATS)、`/home/weiland/go/pkg/mod/github.com/nats-io/nats.go@v1.52.0/nats.go`。

---

## §1. 范围与不做（先父后子）

**做**：仅**控制面（ctl/agent ↔ broker 的 NATS 连接）** proxy-aware dial。注入 ctl `ConnectNATSWithNkey` + ctl 生产 completion(`completion_transport.go:134`) + agent(`connectNATS`)。协议 `http(s)://`(CONNECT) + `socks5(h)://`；env `ALL_PROXY`/`HTTPS_PROXY`/`HTTP_PROXY`/`NO_PROXY`(含小写)。出口含 `docs/usage.md` 代理小节。

**不做（红线）**：
- **数据面隧道**（`internal/tunnel`，agent→broker:7000）——文档登记为可选 follow-up；审查 grep 证明 `internal/tunnel` 无 `proxydial` import。
- **broker 不注入**（`broker.go:384`/`authcallout.go` 零改动，server 不外拨）。
- **`completion_transport.go:286 NewTestNATSTransport`** test-only，**firm 不碰**。
- **零 wire/proto**（不 touch `internal/proto`）；**TLS 端到端不破**（不实现 `SkipTLSHandshake`、不改 ServerName/TLSConfig）。

---

## §2. proxy dialer 设计（新包 `internal/proxydial`）

**包定位**：`internal/proxydial`，**纯叶子**（只依赖 stdlib，零新 direct dep——见 §10 D2）。ctl 与 agent 各自取 option。

**导出面**：
```go
package proxydial

type Env func(key string) (string, bool)
func OSEnv(key string) (string, bool) { v, ok := os.LookupEnv(key); return v, ok }

// Options decides WHETHER to attach a proxy dialer, based ONLY on env presence.
// Zero-regression: no proxy env -> returns nil,nil (caller appends nothing ->
// byte-identical behavior). Per-target NO_PROXY/proxy-selection happens INSIDE
// the dialer at Dial-time (multi-URL & URL-path auto-correct). dialTimeout is
// the per-Dial budget threaded from the call site (§4 / D5).
func Options(env Env, dialTimeout time.Duration) ([]nats.Option, error)
```

**核心架构裁定（消解 URL-path / 多-URL 两个 major）**：`Options` **只看 env 是否设了代理**决定挂不挂 dialer，**不接 natsURL**、不在装配期解析 URL。**NO_PROXY 匹配 + 代理选取在 dialer 的 `Dial(network, address)` 内部按真实 `address` 做**——`address` 是 nats.go 传来的干净 `host:port`（已剥 path/query），故 URL-path（`wss://dom/nats`）与 comma 多-URL（nats.go 逐 host 调 dialer）天然正确。

**装配返回（命中代理时）**：
```go
return []nats.Option{
    nats.SetCustomDialer(d),
    nats.SkipHostLookup(),   // 事实 A：必配，否则本地预解析成 fake-ip
}, nil
```
`SkipHostLookup()` **只在挂 dialer 时追加**（事实 E：无代理时不追加，保留原生多-IP fan-out，零回归）。

**env 解析与优先级**（每 key **大写优先于小写**、首个非空胜）：
1. NO_PROXY 在 dialer 内对**目标 host** 判定。
2. 代理 URL 选取（实现细化，见 D3）：dialer 在 Dial-time 只见 `host:port`、拿不到 scheme，而控制面恒为 TLS 目标，故选取 = **`HTTPS_PROXY > ALL_PROXY > HTTP_PROXY`**（各 upper-then-lower、首个非空胜）；`HTTP_PROXY` 作末位兜底（常见 Clash 配置三者同值）。
3. proxy scheme ∈ {http,https,socks5,socks5h}；其它 → **fail-closed 报错**（D1）。空值=未设。
4. 认证从 proxy URL userinfo 取。

**NO_PROXY 匹配（dialer 内，仅对目标 host，绝不对 proxy hop）**：
- proxy hop 永远直接 `net.Dial`，不过 NO_PROXY（Clash 在 `127.0.0.1:7897` 正常）。
- 内建旁路：`localhost`/`127.0.0.0/8`/`::1` 即使 NO_PROXY 没写也短路（dev broker `nats://127.0.0.1:4222` 绝不走代理）。
- `*` → 全直连；精确 host（大小写不敏感）；后缀 `.example.com` 匹配子域；裸 `example.com` 含子域（D4）；CIDR 仅 IP 字面量 broker 命中（`SkipHostLookup` 下 dialer 见主机名，CIDR 对主机名天然不命中）；NO_PROXY 项可带端口、只比 host。

**HTTP CONNECT（纯 stdlib）**：拨 TCP 到 proxy → 写 `CONNECT host:port HTTP/1.1` + `Host` + 可选 `Proxy-Authorization: Basic` → `http.ReadResponse` 校 200。
- **★ 强制不变量**：**必须返回 `&bufConn{Conn:c, r:br}`**（Read 先排空 bufio.Reader 再走底层），**不得返回裸 conn**——`http.ReadResponse` 的 bufio 可能预读了 200 之后的隧道字节，而 **NATS server 一连上立刻发 INFO、极可能与 200 同 TCP 段到达** → 裸 conn 吞掉 INFO → 静默 hang/TLS 失败。
- 非 200 → 带 `resp.Status` 包装错误、**不泄露 `Proxy-Authorization`**；`https://` 代理跳用 `tls.Client` 包(ServerName=代理主机名)；返回前 `SetDeadline(time.Time{})` 清 deadline。

**SOCKS5（远程解析命门）**：**永远发 `ATYP=0x03 domainname`**（=socks5h 远程解析），`socks5://`≡`socks5h://`。
- RFC1929 认证：`socks5://user:pass@`，server 回非 0x00 → "proxy authentication failed"（**不回显 pass**）；**host>255 与 user/pass>255 拒绝**。
- REP≠0x00 → 人话映射（host-unreachable 是 fake-ip 旧症状最可能值）；读 BND 回复按 ATYP 正确消费字节（IPv4=4/domain=1+len/IPv6=16），否则后续读错位。

---

## §3. wss 交互结论
已源码证实（事实 B/C/D）：`wss://weiland.top:443`/`tls://`/`nats://...:443` 的底层 TCP dial 都经 CustomDialer，TLS+SNI+证书校验由 nats.go 在 Dial 返回后端到端做，SNI 取自 URL host（与 dialer address 无关）。**无需替代方案。** 生产 broker URL 默认 `wss://<broker>:443`（Caddy WSS），故 dialer 主要跑在 wss 上。护栏：把「ws+代理 stub」做进集成测试（§7），防 nats.go 升级改 `createConn` 顺序回归。

---

## §4. 注入面（ctl 两处 + agent）

| # | 位置 | 接线 |
|---|---|---|
| 1 | `natsconn.go:41 ConnectNATSWithNkey`（`tether ps` 的路） | `append(all, opts...)` 后：`popts, err := proxydial.Options(proxydial.OSEnv, <normalDialTimeout>); if err != nil { return nil, err }; all = append(all, popts...)` |
| 2 | `completion_transport.go:134`（生产 connectFn，`MaxReconnects(0)` 一次性） | 同样追加，但 **`dialTimeout` 传 `completionDialTimeout`(750ms)**（D5：dialer 内部 deadline 必须服从 750ms，否则 ctx-cancel 后 cleanup goroutine linger 撞破 goleak + 卡破 1s tab budget） |
| 3 | `agent.go:596 connectNATS`（`nats.Connect(a.cfg.NATSURL, connOpts...)`） | **在 `connectNATS` 注入**（事实 F：`buildConnOptions` 两 return 无 merge point）；append 后匿名+nkey 两分支都覆盖 |
| — | `broker.go:384`/`authcallout.go`/`completion_transport.go:286`/`testharness` | **不动**（红线） |

---

## §5. 零回归保证
唯一安全形态（事实 E）：未设任何代理 env → `Options` 返回 `nil` → 三注入点一个 option 都不 append → `nats.Options` 与改前**逐字节一致**。**严禁**「永远挂内部 fallback net.Dial 的 dialer」。**测试断言**：无 env 时把装配的 `[]nats.Option` apply 到探针 `*nats.Options`，断言 `CustomDialer==nil && SkipHostLookup==false`。

---

## §6. 依赖取舍
- **HTTP/HTTPS CONNECT**：纯 stdlib 手写（~40 行）。
- **SOCKS5**：**手写 ~70 行（定稿 D2）**——零新 direct dep，与项目「静态二进制 + 手写网络协议」既有传统一致（P13 内嵌纯 Go shadowsocks、tunnel 手写 yamux 都是同样路子）；风险由 §7 的刁钻对抗测试（ATYP/auth/长度/reply 消费/fuzz）兜底。
- **NO_PROXY/env 优先级**：手写 ~60 行（不引 `x/net/http/httpproxy`），语义对齐其文档作验收规格。
- **零新依赖**：go.mod/go.sum 不动。

---

## §7. 对抗测试清单（表驱动 + 假 proxy server，CLAUDE.md §5 刁钻对抗）
**基建**：`fakeHTTPConnectProxy`（断 `CONNECT <hostname>:port`、可选断 `Proxy-Authorization`、200 后透传）；`fakeSOCKS5Proxy`（断 `ATYP=0x03 domain==期望主机名`、按需断 RFC1929、透传）。用一个**不在 DNS 里的假域名**（`proxytest.invalid`）证明 dialer 没本地 resolve。

- **HTTP CONNECT**：happy 200；**★ 200 与首批隧道字节(模拟 NATS INFO)同 TCP 段 → 断 `bufConn` 不丢字节**（最高价值回归）；407/403/502 错误含 status 不含 auth；带 auth header 校验；超时/半截头 → timeout + conn 关闭 + goleak 无泄漏；断透传的是**主机名**不是 IP。
- **SOCKS5**：noauth/auth happy；auth 失败(0xFF/REP≠0)不回显 pass；**★ CONNECT 用 `ATYP=0x03 domain`**；REP 错误映射；host/user/pass>255 拒绝；BND 按 ATYP 消费不错位；reply 解析 fuzz（随机字节不 panic）。
- **env/NO_PROXY（纯函数 + fake Env）**：**无 proxy env → Options 返回 nil**（零回归，apply-to-probe 验）；`HTTPS_PROXY` vs `ALL_PROXY` 优先级；大小写变体（大写优先）；空值忽略；NO_PROXY `*`/后缀/精确/**localhost 内建旁路**/含端口/大写/IPv6/多项；**localhost 目标即使设了 ALL_PROXY 也直连**；**proxy hop 在 127.0.0.1 + 目标 weiland.top → 走代理**（证 NO_PROXY 只评估 target）；**CIDR 仅 IP 字面量 broker 命中**；畸形/不支持 proxy scheme → **fail-closed 报错而非静默直连**。
- **URL 形态**：broker URL 带 path（`wss://broker.example:443/nats`）→ dialer 收到干净 `broker.example:443`、CONNECT 行无 path；comma 多-URL 逐 host 决策正确。
- **集成（旗舰，进 e2e 矩阵）** `test/proxydial/`：**★ 真 TLS 嵌入式 nats-server**（自签 CA/cert，SAN=`proxytest.invalid`）+ fakeHTTPConnectProxy → 调真实 `ConnectNATSWithNkey`，**不显式设 ServerName**（镜像生产，守事实 D）→ 断 `nc.TLSConnectionState().ServerName=="proxytest.invalid"` && `HandshakeComplete`，且 proxy 端首字节是 TLS record `0x16`（明文没暴露给代理）；显式 ServerName 用例保留为单独 case；**wss 变体**（守事实 B 护栏）；**零回归子测试**（不设 env → 直连、断没追加 CustomDialer/SkipHostLookup）；**reconnect**（`MaxReconnects(-1)` kill→restart，断重连成功 + fake-proxy accept ≥2；坏 creds reconnect storm 不重复泄露 auth）；**completion 路径**（proxy stub accept-but-stall → 断 completion dial 在 budget(≤750ms) 内 abort、无 goroutine linger，goleak）。
- **门**：proxydial + cli + agent 相关包带 `-race` + `goleak`。

---

## §8. usage.md 文档大纲（出口必含，用户明确要求）
**插入位置**：新开 `### 5.1.6 控制面代理（proxy-aware dial）`，紧跟 `tether version` 段；并在 `## 2` 的 WSL 排错处加交叉引用。
1. 一句话用途（NAT 后笔记本/WSL TUN 坏时让控制面经本地代理出网；默认不开、只在设了代理 env 时生效）。
2. 支持的 env 表 + 优先级（按定稿真值表）+ 哪个用于控制面 TLS 目标。
3. 支持 scheme：`http(s)://`(CONNECT) + `socks5(h)://`；写明 **socks5/socks5h 都做远程 DNS**（绕开 WSL fake-ip 的原因，附原理一句）。
4. NO_PROXY 规则（后缀/精确/CIDR(仅 IP 字面量 broker)/`*`/**localhost+loopback 缺省旁路**）+ 2-3 例。
5. **典型 WSL + Clash 7897 例子**（可复制）：`export HTTPS_PROXY=http://127.0.0.1:7897`（或 `socks5h://127.0.0.1:7897`）→ `tether ps` 不再 hang；附「为什么之前 hang」（fake-ip + WSL TUN 承载不动 → TLS 卡死；代理改走远程 DNS 绕过）。
6. TLS 不破说明（代理只承载裸 TCP，TLS/SNI/证书仍端到端，代理看不到明文、无法 MITM；支持 `socks5://user:pass@` / HTTP `Proxy-Authorization`）。
7. 范围边界（**只代理控制面**；数据面反向隧道/端口暴露/proxy 订阅流量本版不经代理，登记为可选 follow-up）。
8. 故障排查（proxy 不可达/认证失败/NO_PROXY 写错仍直连的报错样子；「先 `curl -x $HTTPS_PROXY https://weiland.top` 验代理本身通」）。
9. 角色覆盖（ctl + agent 都认；broker 不认）。

---

## §9. 文件改动清单
**新增（实现）**：`internal/proxydial/{proxydial,httpconnect,socks5,noproxy}.go`（含 `bufConn`）。
**新增（测试）**：`internal/proxydial/{*_test.go, fakeproxy_test.go}` + socks5 fuzz；`test/proxydial/integration_test.go`（真 TLS/wss 嵌入式 + 零回归 + reconnect + completion-budget，**进 e2e 矩阵**）；改 `test/e2e/all_phases_test.go` 挂子测试；注入点回归 `natsconn_test.go`/`completion_transport_test.go`/`agent_test.go`（含匿名 agent 分支断言）。
**改动**：`natsconn.go:41`、`completion_transport.go:134`、`agent.go:596 connectNATS`。
**文档**：`docs/usage.md` 新 §5.1.6；本 plan 定稿；`CLAUDE.md §7` 状态登记（收尾）。
**不改**：`internal/broker/*`、`internal/proto/*`、`internal/tunnel/*`、`completion_transport.go:286`、`testharness`、`go.mod`/`go.sum`（零新依赖）。

---

## §10. open decisions —— 主进程定稿裁定（锁定）
- **D1（畸形/不支持 proxy URL）→ fail-closed 报错**。用户显式设了代理却静默裸连墙外=违背意图+在 fake-ip 场景只会再 hang（更难诊断）。空字符串 env 视为未设（不报错）。错误经 `natsconn.go:55` 既有 error 路径上抛。
- **D2（SOCKS5 手写 vs x/net/proxy）→ 手写 ~70 行、零新依赖**。理由：与项目「静态二进制 + 手写网络协议」既有传统一致（P13 内嵌纯 Go shadowsocks、tunnel 手写 yamux）；红队的「x/net 已在树」是事实错误（全新 direct dep，事实 G）、「x/net 本地解析」也错（它对主机名发 FQDN 远程解析）——两个错误前提排除后，手写=依赖最小化，风险由 §7 刁钻测试兜底。HTTP CONNECT 无论如何都得自己写（x/net 无 CONNECT）。
- **D3（HTTP_PROXY 参与）→ 实现细化为 `HTTPS_PROXY > ALL_PROXY > HTTP_PROXY`**。原裁定「按目标 scheme 选」不可实现：CustomDialer 在 Dial-time 只拿到 `host:port`、无 scheme；而控制面恒 TLS。故 dialer 用固定优先级 `HTTPS_PROXY > ALL_PROXY > HTTP_PROXY`（各 upper-then-lower），`HTTP_PROXY` 作末位兜底（用户友好——Clash 常三者同值）。loopback 目标已被内建旁路短路。usage.md 落表。
- **D4（NO_PROXY 裸 `example.com` 含子域）→ 含**（对齐 Go `x/net/http/httpproxy`：命中 `example.com` 与 `*.example.com`）。测试锁真值表。
- **D5（dialer 内部 deadline）→ constructor 参数 `dialTimeout` 由调用点传入**：ctl/agent 传常规(~10s)，**completion 传 750ms**。`nats.Timeout` 不传播进 CustomDialer，故必须显式 thread；配 completion 路径 goleak 测试（proxy stub stall → budget 内 abort 无 linger）。

---

## §11. 出口门（提交前全绿才算 done，CLAUDE.md §5 硬闸）
1. `make test` 绿（含全部新测试）。
2. `make lint`（golangci-lint v2）绿（手写 socks5 的 `conn.Write` 返回值/base64 认证注意 errcheck/gosec，必要时 `//nolint` 带理由）。
3. `go vet ./...` + `CGO_ENABLED=0 go build ./...` 绿。
4. **`-race`**：`go test -race ./internal/proxydial/... ./internal/cli/... ./internal/agent/...`（dialer 被 reconnect 并发调用 + completion dial-in-goroutine）。
5. **`goleak`**：dialer 失败/超时/completion-stall 路径不泄漏 goroutine/半开 conn。
6. **e2e 矩阵**：新集成测试挂进 `test/e2e/all_phases_test.go`，`make e2e` 绿（回填矩阵洞 + 守 wss 命门护栏）。
7. **零回归 = 测试断言**（§5），非人工比对。
8. **文档门**：`docs/usage.md` §5.1.6 落盘。
9. **范围审计**：grep 证明 `internal/tunnel`/`internal/proto`/broker connect 下无 `proxydial` import。

---

## §12. 实现顺序（阶段 B）
1. `internal/proxydial` 核心：env 解析 + NO_PROXY + `Options`（含零回归 nil 返回）+ `bufConn` + HTTP CONNECT + SOCKS5(h) + 单测/fuzz。
2. 注入：`natsconn.go:41` → `completion_transport.go:134`(750ms) → `agent.go:596 connectNATS`；各自回归测试（含匿名 agent 分支）。
3. 集成测试 `test/proxydial/`（真 TLS + wss + 零回归 + reconnect + completion-budget）+ 挂 e2e 矩阵。
4. `docs/usage.md` §5.1.6。
5. 全门：make test/lint/vet/build + `-race` + goleak + make e2e + 范围 grep。

> 实现中若发现设计问题，先改本 plan 再改代码。
