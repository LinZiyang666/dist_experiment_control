# test/ — 目录 ↔ 层次 ↔ tag ↔ 矩阵 地图

层次的定义在 `docs/testing-standards.md §零`：**层次是测试的属性，目录只是它的地址。**
这张表是 `test/` 下每个顶层目录的登记簿；`test/architecture/test_layout_map_test.go` 对它做双向对账
（目录必须登记、登记的目录必须存在、写在表里的矩阵名与 build tag 必须真实存在）。
存量的 `p*`/`d*` 目录按开发 phase 命名，是 e2e 矩阵契约的一部分（`e2e/all_phases_test.go` 字面量、
`e2e/parallel/shard.go`、账本路径键），**冻结为精确集合**：不新增、不迁移；新目录一律主题命名。

| 目录 | 层 | build tag | 怎么跑 | 守什么 |
|---|---|---|---|---|
| `architecture/` | L0 gate | — | `make gates` | 仓库结构的门：分层规则表、结构预算棘轮、TLS 配对、build-tag 对账与局部性、docs 布局、CI 发布链、闸门标准元门、hermetic 闸集对账、fuzz 语料预算、test tree 地图、ctx 站点冻结……**完整清单只在 CLAUDE.md §5 闸门表**（这里曾重复列举，落地当天就漏了五个新门——重复列举必然腐化） |
| `determinism/` | L0 gate | — | `make gates` | 测试代码自身的门：raft 确定性 lint、版本字面量 SSOT、docs wire 版本、raft 计时 / 产品计时 sleep 守卫、`// origin:` 真实性、promised-guard、命名冻结（两本递减账本 + 生产文件零账本）、测试身份清单、枚举 `default:`、泄漏断言形状与覆盖面、就绪 sleep 冻结、T3 前提（裸 `.IsLeader()`）——同上，**以 CLAUDE.md §5 为准** |
| `chaos/` | L3 | — | `TestChaosMatrix`（-race）；`go test ./test/chaos/` | 磁盘只读 / broker 重启 WAL 恢复 / NATS 中断下的 ctl+agent 行为；每个注入先 `proveInjected` 自证（2026-09-01 前无 -race、不在任何矩阵） |
| `cli_e2e/` | L3 | — | `TestTransferDefaultsMatrix`；**`TestReliableExecMatrix`** 与 `TestReliableRunMatrix`（-race；`^(TestExecReliable\|TestCtlConnect)`，S3 按 plan §9 扩展筛选纳入 node-upgrade 用例）；`go test ./test/cli_e2e/` | 从 CLI helper 进的整栈：expose 真 TCP echo、agent 重启后端口存活、exec 大输出、session 生命周期、动态补全；**真二进制层**（`realstack_harness_test.go`，Linux-only）：编译出的 `tether` 同时当 ctl 与 agent 子进程、嵌入 nats-server 在固定端口可重启、进程内 broker 保留产品默认 Stale/Offline、TCP relay 在匹配的 PUB 帧之后切断两侧（请求转发前封住回包并取得收据，再断链；只切 TCP、不伪造字节）——可靠 exec 的 14 条 `TestExecReliable*` 续接/退出码/N-1 fallback 用例与 `TestCtlConnect*` 的 callout 黑窗连接级重试都在这里（testing-standards §0.4）；S3 交付的 `node_upgrade_image_test.go` 同层覆盖 node upgrade 活任务跨 exec、代际链、busy deferral 与旧候选 ABI 拒绝（S3 plan §7 T16；不代表 deploy-tier 已验收）；**注意**：真二进制的 ctl/agent 由 harness 嵌套 `go build` 出来，对它们的变异要走 `GOFLAGS=-overlay=<json>`，`go test -overlay` 只改 harness 进程；`-race` 下二进制同样插桩（`realstack_race_test.go` 哨兵） |
| `cluster/` | L3 | — | `TestD1Matrix` / `TestD2Matrix`（-race；runner 去重后只跑一次） | raft 集群 node ops、Plan/Apply 等价、register 路径的领导权丢失窗 |
| `clusterharness/` | helper | — | 被 `d*`/`cluster` 引用 | RouteCA / WaitForCond / FreePort / **WithLeader**（T3 原语：观测→行动→再观测；`d3` 的 follower PIN 写是第一个调用方，`determinism/leader_premise_test.go` 指向它）；集群 builders **永不合并**（文件头有裁决） |
| `fullstackharness/` | helper | — | 被 S10 全栈验收（`test/hotupdate/acceptance`、drill 99 fixture）引用 | 内核身份 / OFD 读取、基于共享 `internal/testharness` 监督器的进程监督（S10 P04/P05） |
| `infrabusiness/` | L3 / L4 | `infrabusiness_integration` | `TestHotUpdateInfraBusinessMatrix` (-race, per-test 600s) | Ordinary authentication, business routing, migration and continuity across infrastructure cuts; real supply remains required. |
| `infracaddy/` | L3 / L4 | `infracaddy_integration` | `TestHotUpdateInfraCaddyMatrix` (-race, per-test 600s) | Caddy artifact, process, socket and retained-storage acceptance with supervised fixtures. |
| `infraprivilege/` | L3 / L4 | `infraprivilege_integration` | `TestHotUpdateInfraPrivilegeMatrix` (-race, per-test 600s) | Privileged root operations, caller scope, and independent process ownership receipts. |
| `infranats/` | L3 / L4 | `infranats_integration` | `TestHotUpdateInfraNATSMatrix` (-race) | Embedded NATS reply recovery and consumer custody; tagged adjacent-version, quorum and process-lock acceptance requires real supply and product bindings. |
| `infraharness/` | helper | — | 被 S9 infra 集成测试引用 | 基于共享 `internal/testharness` 监督器的 infra 进程监督：不可变进程身份、有界 Stop、deathwatch 作用域（S9 P27） |
| `transferharness/` | helper | — | 被 S7 `test/hotupdate` transfer 用例与 `test/cli_e2e` 黑盒引用 | 真实二进制启动/停止与原始 receipt 接口（S7 P29）；生命周期测试直接五轮双泄漏 |
| `concurrency/` | L3 | — | `TestRemoteFSMatrix`（`Spawnsafe\|Leak\|FDStable`）；`go test -race ./test/concurrency/` | NumGoroutine + fd 泄漏门宿主、spawnsafe 压力 |
| `d3/` | L3 | — | `TestD3Matrix`（-race） | NATS 集群配置渲染、auth_callout 集群路径、follower PIN 写 |
| `d4/` | L3 | — | `TestD4Matrix`（-race） | 转发、proc 事件、storage 在集群下 |
| `d5/` | L4 | `d5_integration` | `TestD5Matrix`（-race） | 集群 JetStream 行为套件（fanout / joint / replicas / window） |
| `d6/` | L4 | `d6_integration` | `TestD6Matrix`（-race） | 数据面 e2e（端口 home / rehome） |
| `d7/` | L4 | `d7_integration` | `TestD7Matrix`（-race） | 集群生命周期：真三节点 grow / transfer-leader / retire |
| `d8/` | L4 | `d8_integration` | `TestD8Matrix`（-race） | 分布式 transfer ‖ replicated alerts、forward churn 泄漏 |
| `d9/` | L4 | `d9_integration` | `TestD9Matrix`（-race） | 生产 cutover：seed + 迁移 + 回滚 |
| `e2e/` | L5 matrix | `e2e_matrix` | `make e2e-parallel`；定位单个用 `make e2e-one T=<Matrix>` | 唯一允许 exec `go test` 的地方；`e2e/parallel/` 是 NUMA 钉核的并行 runner，`-dedupe`（默认开）按运行时 `go list -race` 闭包 hash 折叠跨矩阵的重复单元（折与不折都打印理由），`-shuffle` 透传给每次 `go test`（可解析单元作旗、whole 单元经 `GOFLAGS` 到其子进程）；单元清单 golden 在 `e2e/parallel/testdata/`（含 whole 矩阵的包/`-run`/phase 字面量） |
| `hotupdate/` | L3 / L4 | `hotupdate_integration` | `TestHotUpdateFlowMatrix`, `TestHotUpdateIntegrationMatrix`, `TestFullstackAcceptanceMatrix`（-race；按 Test 分区） | S5–S10 reference、real image/rollback/transfer/transaction/fullstack tests；tagged/untagged 分别发现，600s 单元，整包 race 由分区并集覆盖。acceptance/evidencecmd 是子包，各自保持独立编译与生命周期。[Theme README](hotupdate/README.md). |
| `p1/` | L3 | — | `TestAllPhases` | auth seed / JWT / storage FK 三个互不相干的风险测试 |
| `p2/` | L3 | — | `TestAllPhases` | register / heartbeat / 克隆实例租约命名 / NATS 迟到时的 agent 韧性 |
| `p3/` | L3 | — | `TestAllPhases` | auth_callout 全链路 |
| `p4/` | L3 | — | `TestAllPhases` + `TestRemoteFSMatrix` | exec / ps / 远程文件系统安全 |
| `p5/` | L3 | — | `TestAllPhases` | run（PTY）模式 |
| `p6/` | L3 | — | `TestAllPhases` | expose：端口分配 + `ev.port` + agent `state.json`（**不含**真 TCP 转发，包头写明） |
| `p7/` | L3 | — | `TestAllPhases` | audit |
| `p8/` | L3 | — | `TestAllPhases` | reconcile（G.1/G.2 不变量） |
| `p9/` | L3 | — | `TestAllPhases` | admin |
| `p10/` | L3 | — | `TestAllPhases` | upgrade：状态机、install 用户级 unit |
| `p13/` | L3 | — | `TestAllPhases` + `TestProxyTunnelReconnectMatrix` | proxy 订阅、隧道掉线后的假在线恢复 |
| `proxydial/` | L3 | — | `TestProxyDialMatrix`（-race） | SOCKS5 / HTTP 代理拨号集成 |
| `security/` | L3 | — | `go test ./test/security/` | auth bypass、成员冒充 owner、PIN 暴力、transfer 的 auth_callout |
| `stackharness/` | helper + 收据 | — | 被 8 个单机套件的 `seedSession` 转发器引用；自带收据测试（`seed_test.go`），在 `make gates` 里跑；只许 `_test.go` / `test/` 树 import（`architecture/layering_test.go` 反向扫描） | 单机 stack 的产品依赖原语（`SeedSession`）；`testharness` 因 import 环放不下的东西放这里；`startBroker/startAgent` 的收敛是条件项（plan §0 A3） |
| `simcluster/` | L6 deploy-tier | — | `./local.sh drill <name>`（按需，见 `simcluster/README.md` Mandate）；hermetic 自检集 `tests/run-all.sh` | 真 Docker + systemd + 跨机 mTLS 的 drill 集（数量看 `ls drills/*.sh \| wc -l`——写死的数字第一天就错了）；**忠实暴露缺陷，绝不弥补** |
| `storage/` | L3 | — | `go test ./test/storage/` | SQLite 存储层 |

S4 的 `TestReliableRunMatrix` 已登记 `./test/cli_e2e/...` 的 `^TestRunReliable`；
`TestLeakRiskMatrix` 已有 ptysession/terminalio 等整包 race 声明。这些入口存在不等于当前候选已执行。
Darwin lifecycle 仍须在 Darwin 原生执行，cross-build 与 native 结果分列。S3/S4 历史收据见 evidence §14/§15。

**读法**：
- 想知道"改了 X 跑什么"——找 X 所在包的 L1/L2 同包测试，再找上表里守 X 的 L3/L4 目录，最后 `make e2e-parallel`。
- 想知道"这个目录为什么叫 p8"——它是主线 phase 8（reconcile）的产物；不改名的理由在 `docs/reviews/test-system-overhaul-plan.md §0 A2`。
- `TestAllPhases` 串行跑 p1–p10 与 p13 的包，是矩阵里最重的单元（e2e-parallel 给它一个宽 worker）。

`test/` 之外的 S3 测试落点：`internal/agentboot/boot_exec_test.go` 是 dispatcher test binary → 真 tether →
receiver test binary（`.prev`）的三角色同 PID harness，由 S3 plan §7 交付，规范见 `docs/testing-standards.md §0.6`；
`internal/handoff/handofftest` 是 Linux-only 的 FD 复制 helper，`DupSource` 用 `F_DUPFD_CLOEXEC` 为
`agentboot.FromCut` → `Config.BootImage` 的进程内注入提供独立描述符，规范见 §0.5。
它们不新增 `test/` 顶层目录；这里登记的是测试位置与责任；S3 历史 drill 收据见 `docs/reviews/hot-update-evidence.md §14`，S4 的该历史待验证说明已由 evidence §15.8 的实际同候选收据补齐；S5 阶段于2026-10-11按用户裁决收尾，历史 §16 的未签行与 §17/§18 的 carry 分列。

The [hotupdate theme](hotupdate/README.md) documents S5–S10 contracts and bounded audits.
Installed matrix declarations, including the infra tags and fullstack acceptance/harness matrices,
are in test/e2e/all_phases_test.go. Heavy suites are partitioned by exact top-level test at **600s**;
whole-package race coverage is the union, not an oversized single invocation. A unit above 420s must
be split or optimized. Five real 120s watchdog rounds cannot be hidden by extending the unit budget.

Each child uses shared internal/testharness subreaper/CLOEXEC lease/pidfd supervision, including nested
artifact builds and same-PID dispatchers. Setpgid/Pdeathsig alone is insufficient. Concurrency tests use
direct shared goroutine/FD assertions and ≥5 direct exercise calls; owner retirement is observed before
fixture teardown. Production mutations propagate GOFLAGS overlays into nested builds, with real
artifact/running SHA and independent raw byte/effect evidence.

38/39/85/86/99/100 live runs, the final candidate's three hard gates/separate gates/whole-race and
Darwin native remain NOT_RUN/PENDING. S9 hot support is linux/amd64 + systemd/pidfd/cgroup/nft, without
expanding other platform claims. S9 P39 and S10 P34/P35 own final evidence/coverage; directory or matrix
registration never signs product completion. Workers run targeted checks; global validation is lead-only.
