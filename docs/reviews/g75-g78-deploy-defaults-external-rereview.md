# Pass — g75-g78 部署默认值二次外审

> 2026-08-11。独立复审开发者针对首轮 F1/F8/F9/F10/F11 的回复与大改。开发者基线、二审反例和本报告的修复前版本已先暂存；随后外审者按授权直接修代码。本报告的最终更新及所有外审修补均保持 unstaged，便于与 staged 基线对比。

## 最终结论

**Pass，可以放行。** 开发者的大改正确关闭首轮 F8/F9/F10/F11，并关闭 F1 已知的 scheme、DNS 字符、loopback 与端口 0 问题。二审另发现 R1/R2 两个 parser parity 缺陷，已由外审者直接修复；普通、race、完整 gates/test、99-unit e2e、simcluster hermetic 和重建镜像后的 live drill 93 均未出现新的 assert/setup/product red。

## 修复前发现与处置

开发者已正确关闭首轮 F8/F9/F10/F11，并关闭 F1 已知的坏 scheme、坏 DNS 字符、loopback 与端口 0 问题；但 F1 的手写 NATS/listen parser 仍有两个可复现差异。两项均已在暂存边界之后修复。

### R1 — Major — 混合 WebSocket/TCP NATS pool 被 config-check 放行

`nats.Connect("nats://127.0.0.1:4222,ws://127.0.0.1:8080")` 在拨号前稳定返回 `nats.ErrMixingWebsocketSchemes`；当前 `validateNATSURL` 只逐项检查 scheme，未检查 pool transport 一致性，`--config-check` 错误打印 `config OK`。独立差分见 `TestValidateNATSURLRejectsMixedTransportLikeNATSClient` 与 cmd 配置检查子测。

**修复**：`validateNATSURL` 追踪 pool transport class，允许 `nats+tls` 或 `ws+wss` 同类组合，跨类时包装返回 nats.go 的 `ErrMixingWebsocketSchemes`。删除了可变全局 scheme map，并补同类 pool 正向测试。红→绿证据覆盖普通与 race。

### R2 — Medium — 非 IP host 可借 `%zone` 绕过 hostname 校验

`validListenHost` 遇到任意 `%` 都先截断，再把前缀当 DNS hostname；因此 `[example.com%eth0]:9090` 被放行。zone 只对 IPv6 literal 有意义，真实 bind 不会把带 zone 的 DNS 名称视作该 hostname。cmd 差分子测稳定得到错误的 `config OK`。

**修复**：仅当 `%` 前是 IPv6 literal 且 zone 非空时接受；DNS host 带 zone、空 zone 均拒绝。同时允许 resolver 合法的单尾点 absolute FQDN，避免假阴性。普通与 race 差分均转绿。

## 已独立确认的修补

- F8：runbook 不再使用空输出且 exit 0 的 journald property。
- F9：validator 合并回既有 broker 文件，结构预算 70 未放宽，定向预算测试 PASS。
- F10：New policy 与 Run/EADDRINUSE 两阶段测试均 PASS。
- F11：全新 lint cache PASS，0 issues。
- simcluster hermetic gates ALL PASS；99-unit e2e ALL PASS。
- 当前镜像与 vendor SHA 一致的 live drill 93：INCOMPLETE rc=4，64 pass、1 个既有 #42 gap、1 个已登记 webhook timing runtime-guard；无 assert/setup/product red。该次运行因 lease jitter 少于开发者记录的 65 pass，但属于 drill 自己显式分类的非产品 guard，不冒充稳定 65-pass 证据，也不构成本增量阻塞。

## 最终验证

| 命令/验证 | 结果 |
|---|---|
| targeted config-check + broker New/Run + R1/R2 正反例 | PASS |
| targeted `-race`（cmd/tether、broker、httplisten） | PASS，无 race |
| `make lint`（第二个全新 cache） | PASS，0 issues；首次曾抓到外审修补的无效初值，已修后重跑 |
| `make gates` | PASS，含 architecture/预算、determinism、cmd、auth、concurrency、proto 与 lint |
| `make test` | PASS，全部 Go 包 |
| `make e2e-parallel`（外审修补后） | ALL PASS，99 units，4m26s |
| simcluster `run-all.sh` | ALL PASS |
| live drill 93（外审修补后重建镜像） | INCOMPLETE rc=4，64 pass，0 assert/setup/product red，1 个 #42 gap + 1 个已登记 webhook lease-jitter runtime-guard |
| `git diff --check` / staged check / gofmt | PASS |

live 93 的 runtime-guard 连续两轮出现，说明 alert transition 的内存 baseline 在 lease jitter 下仍有已登记的可观测性缺口；warmup raised+cleared 与后续 cleared 已证明 send path/schema。本增量没有触碰该逻辑，不把它洗成 GREEN，也不把已分类的正交 gap 当作本轮回归。

## 疑惑与建议

1. 手写 config parser 必须持续与依赖库的 pool-level 规则做差分；只逐 URL 验证会漏掉跨项不变量。保留当前直接调用 nats.go 的反例。
2. `validListenHost` 允许 underscore 是现有部署兼容选择，而非严格 RFC hostname；建议在注释/文档保持这一点，避免未来误收紧。
3. drill 93 的 webhook runtime-guard 已有清晰根因和后续方向（持久 committed-transition cursor），建议独立增量解决；当前报告不宣称该 gap 闭合。

## 暂存边界

开发者大改、二审修复前报告/tasklist及最初反例已暂存。以下最终外审改动刻意保持 unstaged：R1/R2 生产修复、R1/R2 正向/差分测试、F10 纯 preflight 测试去网络化、tasklist 最终状态和本报告最终 Pass 更新。未再次执行 `git add`。
