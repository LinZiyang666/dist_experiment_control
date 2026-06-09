# P12 plan — `tether expose --remote-port`（指定公网端口）

> 工作流：CLAUDE.md §3 阶段 A。本文件是**主进程定稿**（step 2），综合自规划 workflow 的 5 专家草稿 + 5 轮对抗互评（10 agents，`wf_651964fe-79c`）。下面的「主进程裁决」段逐条记录采纳/驳回，实现与审查以本文件为尺。

## 0. 范围与锁定决策（feature A，不变）

让操作员用 `tether expose --remote-port <N>` 请求一个**具体**公网端口，而非自动最小可用。

锁定（规划前已与用户敲定，不再 relitigate）：
- `ExposeReq` 加**可选** `RemotePort int`，`0`/缺省 = 现状（最小可用）。**不 bump proto**（纯 additive）。
- CLI flag = `--remote-port`（int），缺省时行为与今天**字节一致**。
- 请求端口必须落在配置带 `[BandLow,BandHigh]`（本 broker 14000-14999）；带外 → 新错误码 `port_out_of_band`。
- 请求端口当前有 `ALLOCATED` 行 → **硬拒** `port_taken`，**不回退**自动。
- `REVOKED`/`FREED` 行**不阻塞**（与现有 "free = 无 ALLOCATED 行" 一致）。
- **一次性**（本次 expose 用此端口），不做 name 粘性。
- desired-port 检查 + INSERT 在**同一 SQLite 事务**内（原子，防并发抢同口）。
- name 唯一性（per (sid,name) ALLOCATED）不变,且**仍最先检查**。

仓库：`github.com/LinZiyang666/tether`，Go 1.25，当前 v0.2.8，proto v1。

---

## 1. 主进程裁决（解决专家间冲突 / 必修项）

### D-1 · taken 检测机制 ✅ 采纳 concurrency lens，驳回 broker-storage 的 COUNT-then-INSERT
- **冲突**：broker-storage 提议 in-tx `SELECT COUNT(*) WHERE port=? AND state='ALLOCATED'` 再 INSERT；concurrency 证明真正的原子保证来自 **migration 0003 的部分唯一索引** `idx_port_alloc_unique_active ON port_allocations(port) WHERE state='ALLOCATED'`（已核实 `0003_port_alloc_history.sql:44`），COUNT-then-INSERT 是经典 check-then-act，仅靠 `SetMaxOpenConns(1)`（`storage.go:68`，已核实 LOAD-BEARING）才不出错，一旦该 pragma 被放松或第二个 `*sql.DB` handle 打开同文件即成 TOCTOU。
- **裁决**：**UNIQUE 索引为唯一正确性仲裁者**。desired-port 路径：band 检查（纯 Go）→ `port = desiredPort` → **跳过 findFreePort** → 跑现有 INSERT（`port.go:146-153` SQL 不变）→ 若 INSERT 触发 UNIQUE 约束失败，翻译成 `ErrPortTaken`。**不保留 COUNT 预检**（多余表面，会误导后人以为它是 guard；约束翻译给出的 `ErrPortTaken` 已足够干净）。

### D-2 · UNIQUE 失败翻译必须 `desiredPort != 0` 门控 ✅ 必修（仅 concurrency 提出）
- auto 路径（`desiredPort==0`）的 UNIQUE 失败是 **impossible-by-construction**（findFreePort 已在同一 tx 内证明端口空闲），必须作为**响亮的** wrapped `port: insert` 错误冒出，**绝不**被静默重标为 `ErrPortTaken`。由一个测试钉死（auto 路径永不返回 `ErrPortTaken`）。

### D-3 · 约束检测惯例：复用字符串匹配，不引入 typed error ✅ 采纳 test lens，驳回 concurrency 的 "typed 优先"
- 代码库已有且唯一的约束检测惯例是 `internal/session/session.go:279 isUniqueViolation()`（字符串匹配 `"UNIQUE constraint failed"`，已核实）。concurrency 推荐的 `errors.As(*sqlite.Error).Code()==2067` 虽更"干净"，但会引入**第二套惯例**。
- **裁决**：在 `package port` 内加一个 4 行 unexported `isUniqueViolation`，**镜像 session.go 的字符串匹配**（port 与 session 不同包，无法直接调用；不值得为此新建 shared 包）。加一个**真触发约束**的测试钉死该路径，防驱动改 message 时静默退化。

### D-4 · Allocate 签名：positional `desiredPort int`，驳回 Config-threading ✅
- 专家对调用点数各有错报；已核实 `port.Allocate` 真实调用点 = `internal/broker/expose.go`（1 prod）+ `test/concurrency/race_test.go`（1）+ `internal/port/port_test.go`（~14 in-package）。（grep 里 `pty.go`/`run.go` 的 `Allocate(` 是无关的 PTY 分配，不算。）
- **裁决**：用 positional `desiredPort int`，**紧跟 `localPort` 之后**（desiredPort 是 per-call **intent**，不是 Config 那种 band/clock **policy**；`cfgWithDefaults` 保持不动，调用点 auto-vs-desired 一目了然）。编辑全部 ~16 个调用点（非新签名处一律传 `0`）。两个 int 相邻、调错会**静默编译通过**，故用 `desired=0(auto)` vs `desired=specific` 两个测试抓 arg-swap。

### D-5 · CLI 端校验：软 floor/ceiling，不做 band 检查 ✅ 采纳 cli-ux，驳回 wire-api 的"零客户端校验"
- **裁决**：CLI `RunE` 加 `if remotePort < 0 || remotePort > 65535 { error }`（镜像现有 `--local` 的 `expose.go:54-56` guard，给 fat-finger 快速本地反馈）；**不**在 CLI 硬编码 14000-14999 band（band 是 broker 配置 `serveconf frp.port_range`，可改，硬编码会与重配的 broker 脱同步）。在 65535 内但带外的值（如 13000）照常上送 broker → `port_out_of_band`。

### D-6 · 校验顺序（tx 内，锁定）
1. name 唯一性 `ErrNameTaken`（`port.go:124-133`，**不变、最先**）；
2. 若 `desiredPort != 0`：band 检查 `desiredPort < low || desiredPort > high` → `ErrPortOutOfBand`（**纯 Go**，在任何 DB 写之前，无 round-trip）；
3. INSERT（`port=desiredPort`）→ UNIQUE 失败翻译为 `ErrPortTaken`（仅 `desiredPort!=0`）；
4. 否则（`desiredPort==0`）：`findFreePort` 路径**完全不变**。

→ 保证 `name_taken` 永远赢过 `port_out_of_band`/`port_taken`。

### D-7 · wire 字段（锁定 verbatim）
`ExposeReq` 在 `LocalPort`（`messages.go:391`）与 `ActorFP`（`messages.go:395`）之间加：
```go
RemotePort int `json:"remote_port,omitempty"`
```
`omitempty` 是 byte-identical-default 保证的**承重件**（缺省 0 → 整个 key 不出现 → 老 broker 收到与今天完全一致的字节）。同时扩 `ExposeResp.Code` 的 doc 注释枚举列出两个新码。**不 bump ProtoVersion**（`version.go:14` 保持 1）。

### D-8 · 跨版本静默降级 → 文档化，不工程化 ✅
- 新 ctl 带 `--remote-port` 打到**旧 broker**：未知 JSON key 被忽略 → 自动分配 → `resp.Code==''` 且 `resp.Port != 请求值`，**无报错**。因无 proto bump 无法检测。broker-storage 正确指出：这是单 broker、v0.2.8、strict same-version 部署下的 deploy-skew 边缘，不是支持的混合集群。
- **裁决**：在 `usage.md 5.9` 写**一行已知限制**，**不**加 `CapsResp.BrokerRelease` 版本门（脆弱、超范围）。同时纠正 cli-ux 草稿里"成功行打印的端口即确认"的措辞——该保证**仅同版本成立**。

### D-9 · 字段名纠正
broker-storage 草稿误写 `Config.PortBandLow/High`；实际是 `Config.BandLow`/`Config.BandHigh`（`port.go:96-97`），默认常量 `DefaultPortBandLow`/`DefaultPortBandHigh`（`port.go:49-50`）。实现按真实名。

### D-10 · 错误 hint 措辞冻结（供 test 取子串）
`error_hints.go` 在 `// Expose` 段（`local_port_invalid` 之后）加两条，并**同步**加进 `error_hints_test.go` 的封闭 allowlist 表（`TestBrokerErrorMessageRegisteredCodes`）：
- `port_taken` → `that public port is already allocated; pick another, omit --remote-port to auto-pick, or release the existing one with 'tether expose rm'.`
- `port_out_of_band` → `--remote-port must be within the broker public band (default 14000-14999); pick an in-band port or omit it to auto-pick.`
- **测试断言子串**：`port_taken` 用 `"already allocated"`，`port_out_of_band` 用 `"14000-14999"`。**避开** `name_taken` 已用的 `"expose rm"` 子串，防误配通过。
- 注：`brokerErrorMessage`（`error_hints.go:63-65`）对任何非空 Code 都有 fallback，故两码**即使忘加 hint 也会冒给用户**——hint 只是 UX，不是 surfacing 的承重件，CLI 与 broker lens 可独立合并。
- **实现期修订（Phase-C 内审 BCD-2）**：上面 `port_taken` 的冻结措辞自身含 `'tether expose rm'`，与本条「避开 `expose rm` 子串」的要求**自相矛盾**。实现采用的最终措辞结尾改为 `...or release the existing one first.`（去掉字面 `tether expose rm`），既保留 `"already allocated"` 断言子串、又不与 `name_taken` 的 `"expose rm"` 撞串。以 `error_hints.go` 实现为准。

---

## 2. 改动面（files to touch）

| 文件 | 改动 |
|---|---|
| `internal/proto/messages.go` | `ExposeReq` 加 `RemotePort int json:"remote_port,omitempty"`（D-7）；扩 `ExposeResp.Code` doc 枚举。**不**改 `version.go`。 |
| `internal/port/port.go` | 加 `ErrPortTaken`/`ErrPortOutOfBand`（`var` 块 `:88-92`）；`Allocate` 签名加 `desiredPort int`（紧跟 `localPort`）；tx 内按 D-6 分支；加 unexported `isUniqueViolation`（D-3）；UNIQUE→`ErrPortTaken` 仅 `desiredPort!=0`（D-2）。 |
| `internal/broker/expose.go` | `:138` 调用传 `req.RemotePort`；`:139-151` switch 加两 case：`errors.Is(err, port.ErrPortOutOfBand)` / `port.ErrPortTaken` → `replyExposeErr(msg, "<code>", strconv.Itoa(req.RemotePort))` + `pubAuditCall(...,false,"<code>",...)`（镜像 `name_taken`/`port_exhausted`；`strconv` 已 import `:22`）。 |
| `cmd/tether/expose.go` | `newExposeCmd` **only**（非 rm）：`remotePort int` 变量 + `IntVar(&remotePort,"remote-port",0,...)` + D-5 软 guard + marshal `:68` 加 `RemotePort: remotePort`。无 completion。 |
| `cmd/tether/error_hints.go` | 加两条 hint（D-10）。 |
| `test/concurrency/race_test.go` | `:76` Allocate 调用补 `0`；新增并发同口测试（见 §3）。 |
| `internal/port/port_test.go` | ~14 个 Allocate 调用补 `0`；新增 `TestAllocateDesiredPort` 表（见 §3）。 |
| `internal/broker/*_test.go` 或 `test/p6` | reject-path handler 测试（见 §3）。 |
| `test/p6/expose_e2e_test.go` | `runExpose` 加 remotePort 变体 + 5 个 e2e（见 §3）。 |
| `cmd/tether/error_hints_test.go` | allowlist 表加两码（D-10）。 |
| `docs/usage.md` | 4 处（§5.9 代码块/`--local` 后 bullet、`:486` 速查行、`:1461` §9.4 错误表）+ D-8 跨版本限制一行。 |

---

## 3. 测试计划（make test + make e2e + -race 全绿才算 done）

**单元 `internal/port/port_test.go`**（表驱动 `TestAllocateDesiredPort`，显式传 `tinyBand{14000,14002}`）：
- `free_granted`：`desired=14002`（带内非最小）→ `Port==14002`（证明确实"指定"而非最小巧合）。
- `allocated_taken`：先占 14000 → `desired=14000` → `errors.Is(ErrPortTaken)`，**无第二行、无回退**。
- `below_band`/`above_band`：`desired=13999`/`14003` → `ErrPortOutOfBand`，无 INSERT。
- `revoked_only_granted` / `freed_only_granted` / **`revoked_and_freed_mixed_granted`**（混合历史）→ 均授予。
- `zero_is_auto_lowest_free`：`desired=0` 且 14000 已占 → 返回 14001（auto 路径不变，抓 arg-swap）。
- `name_first`：dup (sid,name) + 一个本身被占的 desired → `errors.Is(ErrNameTaken)`（证顺序）。
- **`auto_path_never_taken`**（D-2 守卫）：auto 路径下的 UNIQUE 冲突冒为 wrapped `port: insert`，**非** `ErrPortTaken`。

**并发 `test/concurrency/race_test.go`**（紧邻 E.18）：
- `N>=20` goroutines 经 `newRaceLatch` 同时 `Allocate(... desiredPort=14001 ...)` 于同一 `*sql.DB`，显式小带 `{14000,14002}`（**不**抄 E.18 的 N-sized 带）→ **恰一个** `nil`+`Port==14001`，其余 `errors.Is(ErrPortTaken)`；`SELECT COUNT(*) WHERE port=14001 AND state='ALLOCATED' == 1`；`-race`。
- **真约束触发测试**（D-3）：制造真实 UNIQUE 冲突，断言映射到 `ErrPortTaken`，钉死字符串匹配契约。

**broker handler（reject 路径，DB-seeded harness 即可）**：
- `RemotePort` 指向 ALLOCATED 端口 → `ExposeResp.Code=="port_taken"`；带外 → `"port_out_of_band"`；
- **断言 reject 路径不向 agent forward、不发 `pubAuditPort "allocated"`**（失败在 switch 短路，先于 `:157` forward）。
- 注：成功/honored 路径 **必须放 `test/p6`**（`handleExposeReq` 成功前会 block 在 `nc.Request` agent forward `:172`，需 `startAgent`；且 `test/cli_e2e` **不在** `all_phases` 矩阵，只作补充）。

**e2e `test/p6/expose_e2e_test.go`**（在矩阵内，带 14000-14002）：
- `HonorsRequestedRemotePort`：请求 14002（14000 空闲）→ `resp.Port==14002` + DB 行。
- `RequestedPortTakenHardFails`：A auto→14000，再 B 请求 14000 → `Code=="port_taken"`、`resp.Port==0`、A 仍占 14000（无 clobber/无回退）。
- `RequestedPortOutOfBand`：13999/15000 → `port_out_of_band`。
- `RequestedPortReusableAfterRm`：A 占 14001 → rm → B 请求 14001 → 授予（证 REVOKED/FREED 不阻塞）。
- `OmittedRemotePortUnchanged`：`RemotePort=0` 仍取最小 14000。
- （可选）reject-then-free-then-regrant：desired 授予后 agent reject → 端口被 Free → 同口可再请求（证 one-shot 语义、防测试 flake：硬拒测试须打活 agent 仍 ALLOCATED 的口）。

**CLI `cmd/tether`**：
- `omitempty` byte-identical：marshal `{Name,LocalPort}` 与 `{Name,LocalPort,RemotePort:0}` 产生**无** `remote_port` key 的相同字节。
- 软 guard：`--remote-port -1` / `70000` 本地报错、**不**走 NATS。
- wire 穿透：`--remote-port 14005` → JSON 含 `"remote_port":14005`。
- `error_hints_test.go`：两码进 allowlist 表，子串 `"already allocated"` / `"14000-14999"`。

---

## 4. 实现顺序（Phase B，step 3）

1. `proto/messages.go` 加字段 + doc（先有字段，broker/CLI 才能引用）。
2. `internal/port/port.go`：sentinels + 签名 + desired 分支 + `isUniqueViolation` + 翻译门控。
3. 改全部 ~16 个 Allocate 调用点（`race_test.go`/`port_test.go` 传 `0`）→ 先让仓库编译通过。
4. `internal/broker/expose.go`：传参 + 两 switch case。
5. `cmd/tether/expose.go` + `error_hints.go`：flag + guard + marshal + hint。
6. 测试：单元 → 并发 → broker reject → p6 e2e → CLI/hints。
7. `docs/usage.md` 4 处 + 跨版本限制。
8. `make test` + `make e2e` + `make lint`（golangci v2）+ 并发 `-race` 全绿。

分支：`phase/12-expose-remote-port`。commit 留到 phase 收尾（内审 + 用户外审通过后），conventional `feat(expose): --remote-port to pin a specific public port`。

---

## 5. 已知限制 / out of scope

- **跨版本静默降级**（D-8）：新 ctl `--remote-port` 对旧 broker 无效且无报错，仅文档化。
- **不做 name 粘性**（feature B）、不做可插拔分配策略 / 子带（feature C）——本 phase 只 A。
- desired-port 授予后 agent-forward 失败 → 端口被 Free → 可再请求（**one-shot**，非 retry-sticky），有意行为，非 bug。
- audit 行不区分"操作员指定端口 vs 自动"（请求 mode 不持久化）——有意非特性。

---

## 6. 被驳回 / 降级的专家建议（留痕）

- ❌ broker-storage 的 COUNT-then-INSERT 作为正确性 guard（D-1）。
- ❌ concurrency 的 typed `errors.As(Code()==2067)` 检测（D-3，改用既有字符串匹配惯例）。
- ❌ 通过 `*port.Config` 线 desiredPort（D-4，改 positional param）。
- ❌ wire-api 的"CLI 零校验"（D-5，保留软 floor/ceiling）。
- ❌ 为静默降级加 `CapsResp` 版本门（D-8，仅文档化）。
- ⚠️ 多份草稿误报 Allocate 调用点数 / 误写 `Config.PortBandLow` 字段名——已核实纠正（D-4/D-9）。
