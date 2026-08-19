# Pass - 克隆凭据实例最终外部审查

结论：**Pass，可以放行外审修复后的工作树版本**。开发者本轮大改并未独立闭合全部风险，暂存时的开发者版本仍判定为 Fail；在用户授权下，我修复了确认的问题并新增独立守卫。最终 `make gates`、全量 `make test`、99-unit 发布 E2E、关键路径 race 与真实共享 home/共享 binary sim drill 全绿，未发现剩余重大正确性或上线阻断问题。

## 审查范围、独立性与差异分层

- 重新阅读 `CLAUDE.md`、requirements、architecture/usage、当前 plan、历次 `docs/reviews` 和 simcluster 契约；内部报告仅作为索引，没有采信其中的“已修”或自报测试结果。
- 先逐行复核开发者新增的 7 文件 `+177/-42` 整改，再回到生产调用链、cluster writer、认证 provenance、NATS reconnect 和真实容器拓扑验证。
- 开发者版本已完整加入 index，当前 `git diff --cached` 是开发者基线（79 files，`+11317/-130`）。此后外审实现、回归测试、tasklist 和本报告全部留在暂存区外，便于用 `git diff` 单独比较。
- 完整审查面及完成状态见 `docs/reviews/cloned-credential-instances-final-review-tasklist.md`。

## 开发者版本复审发现与外审处置

### X1 - Blocker - refusal budget 终止分支实际形成零延迟永久重连

开发者把 reconnect refusal 计数到了阈值，但 terminal 分支仍调用 rebuild；`leaseRefusalBackoff(max)` 返回零等待，下一轮立刻拨号。“giving up”因此只是日志，initial register 又绕过同一预算。外审将 initial/reconnect 两条路径统一为一个 refusal state machine：旧 session 先退役、指数退避发生在 session 之间、达到阈值后保持断连直至进程重启或 parent cancellation；真实注册统一清零状态。新增初始路径 terminal guard，并复跑原 reconnect guard。

### X2 - Blocker - connection teardown 存在真实数据竞争

完整 agent race 捕获到 `finalizeConn` 的关闭 goroutine读取闭包变量 `intent`，同时 parent-cancellation 分支改写该变量。外审为关闭 goroutine冻结不可变 `closeIntent`，将超时升级决策放入独立 `escalationIntent`，保留“systemctl stop 胜过 rebuild”的原语义并消除竞态。

### X3 - High - `ConfiguredNID` 和 `PreviousNID` 仍可越过凭据族边界

仅验证 `<base>-NN` 字符串形状会把真实、独立 provision 的 `gpu-02` 当作 `gpu` 的 lease。外审让 basename 判定查询 provisioning provenance：当前名字自有 binding 时不得折叠到短 root。进一步发现进程迁移的 `PreviousNID` 是客户端输入，现已在 raft write 和 live reconcile 之前规范化，只允许从同一个已证明 credential family 搬运；跨族提示被清空。新增真实数字后缀设备和跨族 process-row source 守卫。

### X4 - High - cluster process refile 仍是假成功

开发者版本在 cluster mode 仍对 adopted process refile 返回 nil，不写数据，`ps <lease>` 与退出结算可能长期失真。外审没有新增 raft op，避免 same-proto rolling release 分叉；而是把经过校验的 process moves 作为排序、去重、全 literal、仅 RUNNING 的附加 SQL，和现有 `OpNodeRegister` identity statement 在同一个 raft command 中原子提交。晚到的非原子 cluster refile 现在显式报错，不再假成功；文档恢复“租约实例为普通 ps 地址”的契约。

### X5 - High - shared-home drill 没有共享二进制，升级所有权问题未闭合

原 drill 只共享 `.tether`，实际 binary/marker 仍是容器私有 inode，不能证明 shared-binary 行为。外审将整个 agent home 作为同一 volume，显式核对 `.tether` 与 `.local/bin/tether` 的 device/inode。由于共享 binary 与 rollback marker 天然没有诚实的 per-instance upgrade boundary，产品契约改为：只要 family 曾签发 lease，basename 与 lease row 均拒绝 remote upgrade，要求重建 source image；agent 端再加一道 lease-instance belt。无 auth binding 时对 suffix-shaped family 保守拒绝，宁可降低升级可用性，也不猜测二进制隔离性。

### X6 - Medium - cluster release 注释把本地 liveness 误称为 replicated

实现实际遵循架构：register/farewell leader-only，leader 的 local liveness row 立即 OFFLINE，follower 通过普通 sweep 老化。外审修正文档注释，避免后续维护者把 liveness 错接入 raft；原独立 cluster release 守卫保持通过。

## 独立验证证据

| 验证 | 结果 | 覆盖 |
|---|---:|---|
| focused functional guards | rc=0 | initial/reconnect refusal、cluster release/refile、数字后缀与跨族身份、shared upgrade、teardown |
| focused `go test -race`（agent/broker/proc） | rc=0 | 本轮所有关键修复 guard |
| `go test -race ./internal/agent -count=1` | rc=0，23.465s | 完整 agent 包；确认 teardown race 消失 |
| `make gates` | rc=0 | vet、Darwin cluster build、architecture、determinism、cmd/auth/concurrency/proto、lint 0 issues |
| `make test` | rc=0 | 全仓；broker 323.978s，所有 phase/cluster/security 包通过 |
| `make e2e-parallel` | ALL PASS，3m56.364s | 99 units，15/15 top-level coverage，含 `TestAllPhases` 与 D4/D5 broker 分片 |
| `./local.sh drill 84-shared-home-instances` | GREEN，rc=0 | 19 pass、0 gap；真实共享 `.tether` 与 binary inode，lease/basename upgrade 均在下载前拒绝 |
| drill lint、83/84 `bash -n` | rc=0 | verdict framing 与 shell 语法 |
| `git diff --check`、`git diff --cached --check` | rc=0 | 外审层与开发者基线均无 whitespace error |

沙箱内第一次 `make gates` 的两个 NATS fixture 因禁止本地监听而在 10 秒启动超时；同一命令在允许本地监听的审查环境重跑后全部通过，这不是产品断言失败。

## 疑惑、限制与建议

- 本轮没有等待完整 `go test -race ./internal/broker` 的数分钟全包执行；完成的是完整 agent race、关键 broker/proc focused race、全量普通测试与发布 E2E。以本次改动面看证据足够放行，但建议 CI 增加夜间全仓 race，特别覆盖 leader 切换与 register transaction。
- 无 auth-callout binding 时，broker 无法区分真实命名为 `gpu-02` 的设备与 lease-shaped row。当前 node-list/`--all` 保持非破坏性的可用性优先，remote upgrade 则采用安全优先并可能拒绝真实数字后缀设备。长期建议持久化不改变 rolling schema 契约的权威 lease provenance，消除这组相反 fallback。
- clone family remote upgrade 现在是明确不支持，而非待验证的隐式能力。若未来必须支持，需要先引入每实例独立 binary、marker 与可跨 exec 验证的 lineage；不能只放宽 broker 拒绝码。
- cluster farewell 的即时释放以 leader allocator 为权威，follower 本地 liveness 不复制而按 sweep 收敛，这是既有架构选择。若未来允许 follower 接受 register/farewell，必须重新设计该一致性边界。

## 最终判定

外审修复后的版本满足当前 requirements、集群兼容与 simcluster 验证契约，**Pass**。没有遗留重大问题需要阻断发布；上面列出的事项均是已披露的产品限制或后续工程建议。
