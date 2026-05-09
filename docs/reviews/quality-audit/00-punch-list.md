# Quality Audit — Punch List

汇总 6 个并行 audit shard 的发现。详细说明在
`01-concurrency.md` ... `06-deadcode-drift.md`。

## 总数

| Shard | Critical | High | Medium | Low/Nit | Total |
|---|---:|---:|---:|---:|---:|
| 01 Concurrency      | **1** | 5 | 7  | 2 | 15 |
| 02 Security         |   0   | 1 | 2  | 3 |  6 |
| 03 Storage/Protocol |   0   | 2 | 5  | 4 | 11 |
| 04 CLI/UX           |   0   | 4 | 8  | 5 | 17 |
| 05 Tests/Harness    | **1** | 8 | 9  | 6 | 24 |
| 06 Dead code/Drift  |   0   | 3 | 6  | 4 | 13 |
| **Total**           | **2** | **23** | **37** | **24** | **86** |

---

## Tier 1 — 必修（dogfood 前阻断）

正确性 / 数据安全 / 升级安全相关。共 **6** 项。

| # | 来源 | 问题 | 影响 |
|---|---|---|---|
| T1.1 | Concurrency F1 | tunnel server `Start` ctx-cancel 与 in-flight `handleAgent` 之间无同步 → 公网端口 leak 跨 broker 重启 → EADDRINUSE | broker 重启后 `expose` 不能用 |
| T1.2 | Concurrency F3 | `tunnel.Server.Close()` 不关 control listener；只 ctx-Done 路径关 | 测试 / shutdown 路径 listener 漏关 |
| T1.3 | Security F1 | agent upgrade `fetchURL` 用 `io.ReadAll` 无 size cap + 调 host `tar` | 恶意/失误 url 让 agent OOM；`--all` 一次干掉整 fleet |
| T1.4 | Storage F2 | `schema.AuditCall.ReqID` / `Target` 永远空（broker 三个 caller 都没传） | audit 行无法把 call→proc→port 串起来；`tether history` 输出退化 |
| T1.5 | Storage F1 | `port_allocations.token_hash` 无索引；REVOKED/FREED 行永久保留 | 长跑 broker 表膨胀，每个 frpc 连接都全表扫 |
| T1.6 | CLI F1 | `main.go` 用 `Execute()` 不是 `ExecuteContext(ctx)` | Ctrl-C 在 `tether ps` / `history --follow` / `exec` 等命令上不生效 |

## Tier 2 — 强烈建议（UX / 维护性）

不影响数据，但日常用会撞到。共 **20** 项。

### 并发与资源 (4)

| # | 来源 | 问题 |
|---|---|---|
| T2.1 | Concurrency F2 | `tunnel.Client.Open` 在 Start ctx cancel 后仍 leak session |
| T2.2 | Concurrency F4 | `killOrphanProcess` SIGKILL goroutine 无 ctx，比 agent shutdown 多活 5s |
| T2.3 | Concurrency F5 | `handleRunForwarded` / `handleExecForwarded` / `handleUpgradeForwarded` 无 ctx；agent eviction 后仍跑完，违反 P9 1s 预算 |
| T2.4 | Concurrency F10 | agent.Run 的 heartbeat ctx 与 forwarded sub ctx 不一致；evict 触发 heartbeat 关但 forwarded handler 不关 |

### CLI/UX (4)

| # | 来源 | 问题 |
|---|---|---|
| T2.5 | CLI F2 | `tether agent --install-user-service` 静默丢 `--nats-url` / `--tunnel-addr` / `--pin` / `$TETHER_HOME` |
| T2.6 | CLI F4 | `tether ps` 错误信息仍 dev-style — P11/3 漏改 |
| T2.7 | CLI F12 | `tether exec node1 ls -la` 报 "unknown shorthand flag 'l'"；run/exec 没 `SetInterspersed(false)` |
| T2.8 | CLI F9 | 远端被信号杀的 child 在本地变成 exit 255，丢失信号信息（vs ssh 的 128+sig） |

### 测试稳定性 (8 高优先 sleep-as-barrier)

| # | 来源 | 问题 |
|---|---|---|
| T2.9 | Tests F1 | `p7/audit_e2e:172` 300ms 后断言 150 audit 已落 JS — CI 慢机必 flake |
| T2.10 | Tests F2-F4 | `p7/sys_events_test` 三处 250-500ms barrier sleep |
| T2.11 | Tests F5-F6 | `p8/reconcile_e2e` 200/300/150ms 复合 barrier |
| T2.12 | Tests F8 | `p4/exec_e2e` 50ms+300ms warm-up sleep — 影响所有 phase 的 startBroker/startAgent helper |
| T2.13 | Tests F10 | `p4/exec_authcallout:148` `nats.Timeout(200ms)` per-attempt CONNECT 太紧 |
| T2.14 | Tests F11 | `p9/admin_e2e:427` 300ms register-warmup 然后测 1s evict 预算 |
| T2.15 | Tests F14 | `p10/upgrade_e2e:68` 120ms broker startup + 依赖 forwarded subscribe |
| T2.16 | Tests F19-F21 | `p6:367` / `p8:521` / `p9:411` 起的 agent stop 没注册到 `t.Cleanup`，goroutine leak on test failure |

### Doc/Comment drift (4)

| # | 来源 | 问题 |
|---|---|---|
| T2.17 | Dead code F2 | `internal/broker/expose.go:2-3` 引用不存在的 `internal/frpmgr` 包 |
| T2.18 | Dead code F3 | `internal/agent/expose.go` & `agent.go:71-76` 仍说"P6-6 will ship the real frp adapter"，其实 yamux 早就 ship 了 |
| T2.19 | Dead code F1 | `internal/cli/natsconn.go:71-78` `AgentName` godoc 说 "agent role denied"，与代码 + 所有调用点矛盾 |
| T2.20 | Dead code F6 | `tether exec --help` 说 "PTY mode lands in P5"，`tether run` 早就在那了 — 用户可见 |

---

## Tier 3 — 卫生级（可批量打扫）

37 medium + 24 low 中余下的部分。这部分按域批量处理：

- **Concurrency** F6-F12: ctx 误用 / `time.Now` 漏 / mutex 持锁过久 / disk monitor first check 时机
- **Security** F2-F6: symlink race / agent_provisioned 并发 race UX / PIN cmdline 可见 / admin 父目录 owner 校验
- **Storage** F3-F11: history stream 无 eviction / `splitDot` 边缘 / proto Parse 不校验 sid 形态 / `proc.MarkExited` 吞错 / `JoinWithPIN` 跨 conn / DSN 缺 busy_timeout 等
- **CLI** F5-F8 + F11-F17: `history` / `session create/ls` / `admin *` / `expose --local` 验证顺序 / `--n` short flag / `ctx` exit code 等
- **Tests** 13 项：余下 sleep / cleanup / per-attempt deadline
- **Dead code** F4-F13: `var _ = errors.New` / `var _ = cobra.Command{}` / phase tag 注释 / `tunnel.hashToken` vs `port.HashToken` 重复

---

## 建议执行顺序

1. **Tier 1 (6)** 先做 — 集中提交一个 "address audit Tier 1" commit，跑 e2e × 3 验稳定。
2. **Tier 2 测试稳定性 (8)** — 把 sleep-as-barrier 全替换为 polling pattern；在 `internal/testharness` 里加共用的 `waitFor(t, deadline, predicate)` helper。
3. **Tier 2 CLI/UX (4) + 并发 (4) + drift (4)** — 一个 cleanup commit。
4. **Tier 3** — 留给闲时分批，或在每次撞到具体问题时顺手修。

---

## 决策点

- 全做（Tier 1 + 2 + 3）：3 个 commit，工作量大约一天的 focused work。
- 只做 Tier 1 + Tier 2 测试稳定性（=14 项）：半天，最高 ROI。
- 只做 Tier 1（=6 项）：1-2 小时，dogfood 前的最低门槛。
