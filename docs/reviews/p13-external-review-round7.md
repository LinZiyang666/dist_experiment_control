FAIL — *主进程已整改,见文末「## 主进程回复（round-7 整改）」。F1–F5 全部采纳并修复,你的 5 个 round-7 reviewer 测试通过(`-race`)。F6 真硬件 + F1/F4 的 owner 问题见回复。*

# P13 External Review Round 7

Date: 2026-06-10
Reviewer role: external reviewer

## Verdict

P13 暂不放行。

round-6 的十二项整改测试均已重新执行并通过，但扩大到整改后的完整
目的地策略、readiness 收敛、OFF 状态和 CLI 配置接线后，确认两项 High
安全问题和三项 Medium 行为问题。新增五项独立回归均稳定失败 10/10；
四项并发/网络路径在 `-race` 下同样失败。真实 Caddy/WSS 与真实 Clash
出口条件也仍未完成。

## Findings

### F1 - High: DNS 校验与实际拨号分离，DNS rebinding 可绕过目的地策略

`destAllowed` 先用 `net.LookupIP(host)` 校验一次解析结果
(`internal/agent/ssproxy/server.go:130-155`)，随后 `handleConn` 又把原始
hostname 交给 `net.DialTimeout` (`internal/agent/ssproxy/server.go:427-436`)。
拨号会再次解析 DNS。

因此攻击者可让策略查询返回公网地址，再让拨号查询返回
`127.0.0.1`、RFC1918、link-local 或 metadata 地址。代码注释和架构文档
声称“防 DNS-rebinding”，但当前实现恰好留下了标准 TOCTOU 绕过。

独立测试使用本地确定性 DNS：第一次 A 查询返回 `93.184.216.34`，第二次
返回 `127.0.0.1`，最终 loopback listener 被实际连接：

`internal/agent/ssproxy/destpolicy_test.go:74`
`TestExternalReviewDestinationPolicyPinsValidatedDNSResult`

建议：一次性解析、一次性校验，并用 `net.Dialer.DialContext` 直接拨经校验
的 IP，不得在校验后再次拨原始 hostname。resolver 和 dialer 应可注入，
并明确多 A/AAAA 地址的选择与失败重试策略；任何候选地址不合规时应
fail closed。

### F2 - High: “仅公网出口”仍允许 shared/benchmark/metadata 地址

当前过滤依赖 `IP.IsPrivate` 加少量类别
(`internal/agent/ssproxy/server.go:147-153`)。它不覆盖 RFC 6598
`100.64.0.0/10` 和 RFC 2544 `198.18.0.0/15`。其中
`100.100.100.200` 还是实际云环境使用的 metadata endpoint。

任意订阅链接持有者可直接指定这些 IP；这不需要 DNS rebinding。结果与
CLI、架构文档承诺的“internet-only / 拒绝 cloud metadata”不一致，并可能
暴露实例凭据或运营商/集群内部服务。

独立测试：

`internal/agent/ssproxy/destpolicy_test.go:142`
`TestExternalReviewDestinationPolicyBlocksNonPublicRanges`

建议：定义并测试完整的“可公开路由目的地”策略。不要只换成
`IsGlobalUnicast`，因为 shared/benchmark 地址仍可能满足该判断。至少显式
拒绝 IANA special-purpose ranges，并覆盖各云厂商 metadata 地址；更稳妥的
实现是维护审计过的前缀表并同时处理 IPv4-mapped IPv6。

### F3 - Medium: 当前版本的重复指令在 readiness 丢失后不会重新 ACK

broker 对 `ON + not ready` 节点会重推当前权威 pair
(`internal/broker/proxy.go:568-615`)。agent 却在 exact-equal 指令上先命中
strict-newer guard 并返回
(`internal/agent/proxy.go:101-121`)，到不了 `pubProxyReady`。

初次 ready publish 丢失、OFFLINE 清除 `proxy_ready` 后仅 heartbeat 恢复，
或其他 readiness 状态丢失时，SS 与 tunnel 可仍在运行，但节点会永久不再
进入订阅渲染。

独立测试：

`internal/agent/p13_external_review_round6_test.go:39`
`TestExternalReviewDuplicateCurrentDirectiveReacksReady`

建议：对 exact-equal、`Enabled=true` 且 `p.srv != nil` 的幂等重复指令重新
publish ready，不重建、不推进 pair；低于当前 pair 的指令仍必须拒绝。

### F4 - Medium: proxy OFF 时修改订阅仍下发 `Enabled:true` keyset

create/revoke 在事务提交后无条件调用 `pushCurrentKeyset`
(`internal/broker/proxy.go:236-270`)；该函数不检查主开关，并固定构造
`Enabled:true` (`internal/broker/proxy.go:514-528`)。这与其上方注释、
P13 plan 和“OFF 时 P13 表面惰性”的契约冲突。

它会在 OFF 状态泄露/推送最新 PSK，触发 `proxy_keyset_changed`，并可能让
本地残留 runtime 更新或尝试 bootstrap，制造 readiness 和持久化状态漂移。

独立测试：

`internal/broker/p13_external_review_round6_test.go:288`
`TestExternalReviewSubscriberChangeWhileOffDoesNotPushEnable`

建议：凭据变更与 epoch bump 可继续在同一事务提交，但 commit 后仅在
`proxy_enabled=1` 时推 keyset 和发布 keyset-changed 事件。下一次 ON 会
发送完整权威 keyset。

### F5 - Medium: 文档中的 private-destination YAML 选项不可用

`agent.Config` 有 `ProxyAllowPrivateDestinations`
(`internal/agent/agent.go:143-148`)，CLI 警告也明确要求配置
`proxy.allow_private_destinations`
(`cmd/tether/proxy.go:287-299`)。但 `agentYAML` 没有 `proxy` 字段
(`cmd/tether/agent.go:21-30`)，构造 `agent.Config` 时也没有传入该值
(`cmd/tether/agent.go:123-131`)。

`yaml.Unmarshal` 会静默忽略这个未知配置，因此运营者按文档配置后不会报错，
功能也不会生效。

独立测试：

`cmd/tether/agent_config_test.go:85`
`TestExternalReviewAgentYAMLExposesPrivateDestinationOptIn`

建议：增加带明确 YAML tag 的嵌套 proxy 配置，传入
`cfg.ProxyAllowPrivateDestinations`，并增加 parser + command wiring 测试。
若不打算支持该能力，则删除 Config 字段、CLI 文案和架构声明，不应保留一个
静默无效的安全选项。

### F6 - Exit blocker: 真实 Caddy/WSS 与 Clash 验证仍未完成

in-process P13 E2E 通过，但锁定的真实部署出口条件仍只有执行者请求 owner
裁决，没有真实 Caddy/ACME `/sub/*` 与 NATS WSS 共存证据，也没有真实 Clash
导入、连接、撤销和 OFF 验证。若不执行，必须由 owner 明确修订出口标准，
不能由代码审查隐式视为完成。

## Accepted Round-6 Fixes

原 round-6 十二项 reviewer regression 均通过 10 次复跑，包括 generation
收敛/capability/bounds、ForgetSession fence、事务化 switch/sub mutation、
fail-closed 重建、register readiness 清除、render gate、subhttp 启动失败、
salt replay cache 和 literal private-address deny。

这些修复本身可以接受；F1/F2 表明 F12 的安全目标仍未真正闭合，不否定其他
十一项整改。

## Questions

1. “private destination opt-in”是否仍是产品需求？若是，需要明确 public、
   shared、benchmark、documentation 和各云 metadata ranges 的边界。
2. OFF 状态的订阅 create/revoke 是否同意只提交 DB + epoch，不发 NATS
   directive/sys.event，留到下一次 ON 全量收敛？
3. F6 是安排真实环境验收，还是由 owner 修改已锁定的出口标准？

## Verification

- round-6 原十二项 reviewer tests: PASS，`-count=10`
- 新增五项 reviewer tests: FAIL，均稳定 `10/10`
- 四项网络/状态机回归: FAIL under `-race`
- `CGO_ENABLED=0 go build ./...`: PASS
- CGO dependency check: PASS
- `golangci-lint`: PASS，0 issues
- `git diff --check`: PASS
- `go test ./...`: FAIL；包含本轮五项新回归及既有 macOS
  `/var`、Unix socket、CLI/install baseline
- E2E matrix: P1-P8 PASS，P9/P10 既有 macOS baseline FAIL，P13 PASS

## Re-review Gate

至少完成 F1-F5 并让五项新增回归通过；F1/F2 应增加真实 dial-level 组合测试，
不能只测 helper。F6 完成真实环境验收或由 owner 明确修改出口标准后，再考虑
P13 放行。

---

## 主进程回复（round-7 整改）

日期: 2026-06-10。采纳 F1–F5 全部 finding。**5 个 round-7 reviewer 测试全部通过**(`-race`),`golangci-lint v2.5.0` 0 issues、`CGO_ENABLED=0` deps 无 cgo、P13 e2e 绿。`go test ./...` 仅余既有 macOS 基线。

### F1（High,DNS-rebinding TOCTOU 绕过目的地策略)— 已修(校验=拨号同一 IP)
根因正确:`destAllowed` 用 `LookupIP` 校验一次,`handleConn` 又把 hostname 交给 `DialTimeout` 二次解析。修复:**废弃「校验后再拨 hostname」**,改用 `net.Dialer.Control` —— Go dialer 解析后、connect 前对**每个候选 IP** 调 Control,我在其中跑 `blockedIP`,不合规即 fail-closed。故**校验的 IP 就是连接的 IP**,rebinding 的第二次解析返回 loopback 也被 Control 拒。precheck (`destAllowed`) 保留为 fast-fail。`TestExternalReviewDestinationPolicyPinsValidatedDNSResult` 通过(rebinding DNS 第一次公网、第二次 loopback,loopback listener 未被连接,A 查询≥2)。

### F2（High,shared/benchmark/metadata 仍放行)— 已修(IANA 前缀表)
`blockedIP` 在类别检查(loopback/private/link-local/multicast/unspecified)之外加**显式 IANA special-purpose 前缀表**:RFC6598 `100.64.0.0/10`(含 `100.100.100.200` 阿里云 metadata)、RFC2544 `198.18.0.0/15`、`0/8`、`192.0.0.0/24`、TEST-NET-1/2/3、`192.88.99.0/24`、`240/4`,以及 IPv6 ULA/link-local/multicast/doc/NAT64 等;并归一化 IPv4-mapped IPv6(`::ffff:127.0.0.1` 不绕过)。未用 `IsGlobalUnicast`(你指出 shared/benchmark 仍满足它)。`TestExternalReviewDestinationPolicyBlocksNonPublicRanges`(100.64.0.1 / 100.100.100.200 / 198.18.0.1)通过。回答 Q1:**private opt-in 仍是需求**(默认 deny);「可公开路由」边界即上述前缀表,已落 architecture §L + plan。

### F3（Medium,重复指令在 readiness 丢失后不 re-ACK)— 已修
`applyProxyDirective`:exact-equal 的 `Enabled=true` 指令且**仍服务**(`p.srv != nil`)⇒ 只 `pubProxyReady(true)` 重发,**不重建、不推进 pair**;低于当前 pair 仍拒。故初次 ready 丢失 / OFFLINE flap 清 `proxy_ready` 后,正常服务的节点能经心跳重回 `/sub`。`TestExternalReviewDuplicateCurrentDirectiveReacksReady` 通过(`-race`,真 NATS)。

### F4（Medium,OFF 时改订阅仍推 Enabled:true)— 已修
`pushCurrentKeyset` 开头加 `if !GetProxyEnabled(sid) return` —— OFF 时 `sub create/revoke` 仍在事务里提交凭据+epoch bump(durable),但**不推 keyset、不发 keyset_changed**(不泄露 PSK、不扰动 readiness)。下次 ON 发全量权威 keyset。`TestExternalReviewSubscriberChangeWhileOffDoesNotPushEnable` 通过。回答 Q2:**同意** —— OFF 下只提交 DB+epoch,不发 NATS directive/sys.event,留到下次 ON 全量收敛(已实现)。

### F5（Medium,private-destination YAML 选项不可用)— 已修(接线)
`agentYAML` 加 `Proxy proxyConfig` (`yaml:"proxy"`),`proxyConfig.AllowPrivateDestinations` (`yaml:"allow_private_destinations"`);`agent.Config{... ProxyAllowPrivateDestinations: ay.Proxy.AllowPrivateDestinations}` 接线。故文档/CLI 警告所述配置真正生效。`TestExternalReviewAgentYAMLExposesPrivateDestinationOptIn` 通过;malformed-yaml/pickFlagOrYaml 等不回归。

### F6（Exit blocker,真 Caddy/WSS + 真 Clash)— 仍需 owner 裁决(Q3)
in-process 全覆盖(含 dial-level DNS-rebinding 组合测试,非仅 helper)。真硬件本机 macOS 无法复现。请你裁决排期 lab 或显式修订出口标准。

### 复核请关注
- 5 个 reviewer 测试是否如期绿(本机 `-race` 已过);F1/F2 的 dial-level 组合测试(rebinding DNS server)已覆盖,非仅 helper。
- F6 出口标准处置(Q3)。
