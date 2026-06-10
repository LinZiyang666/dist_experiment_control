CONDITIONAL PASS — 代码放行；P13 阶段出口仍受原 F6 真 Caddy/WSS + 真 Clash 验证阻塞。

# P13 External Review Round 8

Date: 2026-06-10
Reviewer role: external reviewer with direct-fix authority

## Verdict

执行者对 round-7 F1-F5 的修复均可接受，原五项 reviewer regression 普通
复跑 10 次和 `-race` 均通过。

本轮扩大检查又确认四项实现问题。依用户授权，reviewer 已直接修改代码并增加
回归；修复后全部 reviewer tests 与 race gate 通过。目前没有剩余代码级
High/Medium blocker，可以放行代码。阶段出口仍不能标记完整完成，因为锁定的
真实 Caddy/WSS 与 Clash 验证尚未执行，也未见 owner 明确豁免。

## Direct Fixes

### F1 - High, fixed: IPv6 transition/special prefixes仍可绕过目的地策略

round-7 前缀表遗漏 IANA 当前 registry 中的多个 IPv6 special-purpose
range。最直接的安全绕过是 `64:ff9b::/96` NAT64 well-known prefix：订阅者
可把 `169.254.169.254` 编码为 `64:ff9b::a9fe:a9fe`，原 `blockedIP`
会把它视为普通公网 IPv6。

reviewer 已补：

- NAT64 well-known、dummy IPv6、Teredo、benchmark、deprecated ORCHID；
- 6to4、`3fff::/20` documentation、`5f00::/16` SRv6 SID；
- NAT64 metadata、6to4 loopback、IPv4-mapped metadata 回归。

代码：
`internal/agent/ssproxy/server.go`
`internal/agent/ssproxy/destpolicy_test.go`

### F2 - Medium, fixed: OFF gate仍有跨 subscription check-then-publish 窗口

`pushCurrentKeyset` 的 `GetProxyEnabled` 修复只保护顺序执行。`proxy.set` 与
`proxy.sub.*` 是不同 NATS subscriptions，callback 可并发：sub mutation
可能先读到 ON，随后 OFF 完成，再由 sub 发布 `Enabled:true`。

epoch ordering 和 broker listener kill 降低了数据面复活风险，但这仍违反
“OFF 返回后不再发送 enable/keyset”的控制面契约。

reviewer 已增加 `Broker.proxyOpMu`，把 `proxy on/off` 与
`sub create/revoke` 的 mutation + publish 区间统一串行化。

回归：
`TestExternalReviewProxyMutationsShareSerializationLock`

### F3 - Medium, fixed: 没有 tunnel adapter 仍虚假 ACK ready

`proxyStartLocked` 在 `ExposeAdapter == nil` 时仍启动本地 SS server并发布
ready。此时 broker 公网端没有 tunnel，节点却会进入订阅 YAML，形成稳定黑洞。

reviewer 已改为 fail closed：没有 adapter 时不启动 runtime，发布 unready，
不得进入渲染。

回归：
`TestExternalReviewProxyWithoutTunnelDoesNotAckReady`

### F4 - Medium, fixed: TunnelExposeAdapter共享 map 存在并发崩溃

普通 expose、expose-rm 和 P13 proxy 都在独立 goroutine 中调用同一个
`TunnelExposeAdapter`。`localFor` 原为无锁 map，Add/Remove/lookup 可并发
读写，可能被 race detector 捕获，严重时触发 `concurrent map` 进程崩溃。

reviewer 已：

- 用 `opMu` 串行化 Add/Remove 完整操作；
- 用 RWMutex 保护 `localFor`；
- 增加并发读写 race regression。

回归：
`TestExternalReviewTunnelAdapterLocalMapIsConcurrentSafe`

## Accepted Executor Fixes

1. DNS rebinding 在实际 connect candidate 上由 `Dialer.Control` 二次校验；
2. IPv4 shared/benchmark/metadata 前缀已拒绝；
3. exact-equal enabled directive 在 runtime 正常时重新 ACK ready；
4. OFF 状态的普通 sub mutation 不再推 keyset/event；
5. `proxy.allow_private_destinations` 已从 YAML 接到 `agent.Config`。

## Remaining Blocker

### F5 - Deployment exit blocker: real Caddy/WSS and Clash validation

尚缺：

- 真 Caddy + ACME 环境中 `/sub/*` 与 NATS WSS catch-all 共存；
- 真 Clash 客户端导入订阅、选节点、确认出口 IP；
- revoke 与 proxy OFF 在真实客户端上的断连/不可达验证。

这是部署验收 blocker，不是本轮剩余代码 finding。owner 可以选择安排 lab，
或明确修改/豁免锁定的 exit criteria。

## Verification

- round-7 五项 reviewer tests: PASS，`-count=10`
- 全部 `TestExternalReview*` / destination policy tests: PASS，`-count=10`
- 全部 reviewer tests: PASS under `-race`
- round-8 新增 regressions: PASS，含 `-race -count=10`
- affected package P13 tests: PASS；完整 package command 仅因
  `internal/agent` 既有 macOS `/var` vs `/private/var` baseline 退出失败
- `CGO_ENABLED=0 go build ./...`: PASS
- CGO dependency check: PASS，无输出
- `golangci-lint`: PASS，0 issues
- `git diff --check`: PASS
- `go test ./...`: P13/本轮相关包通过；失败仅为既有 macOS path/socket、
  CLI exit-64 和 install baseline
- E2E matrix: P1-P8 PASS，P9/P10 既有 macOS baseline FAIL，P13 PASS

## References

- IANA IPv4 Special-Purpose Address Space:
  https://www.iana.org/assignments/iana-ipv4-special-registry/
- IANA IPv6 Special-Purpose Address Space:
  https://www.iana.org/assignments/iana-ipv6-special-registry/

## Re-review Result

代码审查通过，不要求执行者再修改本轮 F1-F4。阶段完成状态等待 F5 的真实部署
验证或 owner 明确豁免。
