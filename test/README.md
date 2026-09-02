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
| `cli_e2e/` | L3 | — | `TestTransferDefaultsMatrix`；`go test ./test/cli_e2e/` | 从 CLI helper 进的整栈：expose 真 TCP echo、agent 重启后端口存活、exec 大输出、session 生命周期、动态补全 |
| `cluster/` | L3 | — | `TestD1Matrix` / `TestD2Matrix`（-race；runner 去重后只跑一次） | raft 集群 node ops、Plan/Apply 等价、register 路径的领导权丢失窗 |
| `clusterharness/` | helper | — | 被 `d*`/`cluster` 引用 | RouteCA / WaitForCond / FreePort / **WithLeader**（T3 原语：观测→行动→再观测；`d3` 的 follower PIN 写是第一个调用方，`determinism/leader_premise_test.go` 指向它）；集群 builders **永不合并**（文件头有裁决） |
| `concurrency/` | L3 | — | `TestRemoteFSMatrix`（`Spawnsafe\|Leak\|FDStable`）；`go test -race ./test/concurrency/` | NumGoroutine + fd 泄漏门宿主、spawnsafe 压力 |
| `d3/` | L3 | — | `TestD3Matrix`（-race） | NATS 集群配置渲染、auth_callout 集群路径、follower PIN 写 |
| `d4/` | L3 | — | `TestD4Matrix`（-race） | 转发、proc 事件、storage 在集群下 |
| `d5/` | L4 | `d5_integration` | `TestD5Matrix`（-race） | 集群 JetStream 行为套件（fanout / joint / replicas / window） |
| `d6/` | L4 | `d6_integration` | `TestD6Matrix`（-race） | 数据面 e2e（端口 home / rehome） |
| `d7/` | L4 | `d7_integration` | `TestD7Matrix`（-race） | 集群生命周期：真三节点 grow / transfer-leader / retire |
| `d8/` | L4 | `d8_integration` | `TestD8Matrix`（-race） | 分布式 transfer ‖ replicated alerts、forward churn 泄漏 |
| `d9/` | L4 | `d9_integration` | `TestD9Matrix`（-race） | 生产 cutover：seed + 迁移 + 回滚 |
| `e2e/` | L5 matrix | `e2e_matrix` | `make e2e-parallel`；定位单个用 `make e2e-one T=<Matrix>` | 唯一允许 exec `go test` 的地方；`e2e/parallel/` 是 NUMA 钉核的并行 runner，`-dedupe`（默认开）按运行时 `go list -race` 闭包 hash 折叠跨矩阵的重复单元（折与不折都打印理由），`-shuffle` 透传给每次 `go test`（可解析单元作旗、whole 单元经 `GOFLAGS` 到其子进程）；单元清单 golden 在 `e2e/parallel/testdata/`（含 whole 矩阵的包/`-run`/phase 字面量） |
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

**读法**：
- 想知道"改了 X 跑什么"——找 X 所在包的 L1/L2 同包测试，再找上表里守 X 的 L3/L4 目录，最后 `make e2e-parallel`。
- 想知道"这个目录为什么叫 p8"——它是主线 phase 8（reconcile）的产物；不改名的理由在 `docs/reviews/test-system-overhaul-plan.md §0 A2`。
- `TestAllPhases` 串行跑 p1–p10 与 p13 的包，是矩阵里最重的单元（e2e-parallel 给它一个宽 worker）。
