# HA grow/force-single/deploy 缺陷整治 — 拆分 Roadmap（G1–G7）

Date: 2026-07-05
Status: **ROADMAP（总纲，未开工）**。本文件**不是**单批 plan、**不**进入实现——它把
`docs/v0.4.5-ha-grow-ops-gotchas.md` 的 24 条 backlog（#1–#24）+ §B 终极自动化目标，按内聚子系统 +
依赖顺序 + 现网止血优先级拆成 **7 个独立叶子增量 G1–G7**。每批**开工时**各自按 CLAUDE.md §3 走
3 阶段 7 步（Workflow 对抗草拟 → 主进程定稿 `docs/reviews/g<N>-plan.md` → 实现 → 对抗内审 → 外审 →
commit），彼此不阻塞主线。**范围以本文件为总纲，精度以各批 plan 为准。**

> 来源真相：`docs/v0.4.5-ha-grow-ops-gotchas.md`（gotcha 编号 SSOT）。
> 验收网：`test/simcluster/`（每批修好，对应 signature-guarded RED drill 翻成普通 GREEN 回归，
> `grow` 的 `GREW-VIA-WORKAROUNDS` trailer 逐个掉 token——**验收标准已写好**）。
> 约束尺：`docs/distributed-broker-architecture.md`（R3「不静默 de-cluster」等不变量），
> `docs/architecture.md`（控制面/数据面分离、proto wire SSOT）。

---

## 0. 为什么拆（不适合一次做完）

三条硬约束决定必须分批：

1. **依赖链（先父后子）**。`#12`（force-single 幽灵 VOTER）是堵点：它挡死 `#20` 的
   `reconcile nats --to-standalone` 去集群化路径（"unrecognized raft role … cannot prove N=1" 拒），
   也是"成员变→客户端视图收敛"一族（`#1`/`#17`/`#11`）的前置。`#22/#23/#24`（install 权限 / unit
   Restart / cert SAN）是 grow 编排（§B `cluster add`）的部署地基——不修，编排永远得手动绕。
2. **风险异质**。`#23`=一行 `Restart=always`、`#22`=一行 install.sh chown，读一眼就能审；`#12` /
   force-single 语义、§B 的 `cluster add` 大编排要动 raft membership 状态机，外审要盯很久。混一个 PR
   会让外审无法分档对待、回归网糊成一团。
3. **止血优先级**。`#20`/`#12` 是现网 racknerd 已经流过血的（JS 静默 503 烂 5 天、名册卡着删不掉的
   幽灵 VOTER）；`#14`/`#19`/`#2`/`#18` 是改进项。优先级差一个量级。

**已修 / 已纠正（范围缩小，勿重复做）**：
- `#9`（ctl 无持久 roster）→ **已修 v0.4.6**（cli-failover 本就工作）。G3 只做剩余的 `#17`。
- `#16`（缺 `--remote`）→ **已纠正**：`cluster status --remote` 本就存在；真缺口只剩 `--homes`/`seeds
  show` 无 `--remote` 变体 + `--remote` 在 force_single 下 `exit=0`（应 exit 3）。
- `#17`（cli-failover 坏）→ **已纠正**：failover 本就工作；真缺口只剩 roster 动态发现被 `FloorURL`/
  `BootstrapURL` 单点门住（`refreshCtlEndpoints` 四重门）。
- `#6`（root-owned `tether.lock`）→ **sim 不复现**（sim 标准化 `User=tether` init）；与 `#22` 同族
  （install/init 属主），G1 顺带处理，无专属 drill。

## 1. 依赖图

```
        G1 部署面加固 (#22 #23 #24 +#6)  ── 地基，无依赖
          │                                  ├──► G4 grow 编排 cluster add (§B #3 #4 #5 #7 #8)
          │                                  └──► (G2 也受益：force-single 后的 nats 重启不再弄死 broker)
        G2 force-single 完整化 (#12 #20 #10 #15)  ── 现网止血，无硬前置
          │
          └──► G3 客户端视图收敛 (#1 #17 #11)   ── 需 #12 幽灵可删，名册才收敛到真实成员

        G5 滚动升级 cluster upgrade (#13 #14 #19)   ── 独立
        G6 容量感知 MaxBytes (#21)                   ── 独立
        G7 数据面再均衡 + 可观测/告警 (#2 #18 #16 #20③)  ── 独立（可再拆两小批）
```

**推荐推进顺序**：`G1 → G2 →`（G3/G4/G5/G6/G7 按需，其中 G4 依赖 G1、G3 依赖 G2）。G1 低风险高杠杆、
是一切编排的地基先做；G2 紧接着止现网的血；G4（`cluster add`）终极目标但工作量最大、放地基铺好之后。

---

## 2. 逐批规格

### G1 — 部署面加固（install / systemd unit / route-cert）
- **含 gotcha**：#22、#23、#24（+ #6 顺带）
- **做**：
  - `#22`（**已定稿 Option B**）：install.sh 新建 tether-owned `/etc/tether/nats.d/`、把 reconciler 的
    nats.conf 迁到那里（`/etc/tether` 本身**保持 root-owned**——root 跑的 caddy 读 `Caddyfile`，chown
    `/etc/tether` 会成 tether→root 提权面）；代码默认路径（`defaultNatsConfPath` SSOT）同步迁 `nats.d/`。
    使 in-broker C3 reconciler（`User=tether`）能原子重写 nats.conf → membership 变更后 topology 自动收敛。
    现网老机需多步迁移（建 nats.d/ + mv conf + 改 ExecStart + daemon-reload），见 broker-ops §8.6。
  - `#23` unit 层改 `Restart=on-failure`→`Restart=always` + `RestartSec=2`（durable、trigger-agnostic 兜底；
    `serve.go` 把 `context.Canceled` 当 exit 0 是 clean-exit 根因，产品级 return 点静态定位不到→**DEFERRED**，
    需 sim journal）。
  - `#24` route/tunnel leaf 证书**须带 `subjectAltName` 匹配 route-URL host**（IP-URL 用 `IP:` SAN、
    hostname 用 `DNS:`；纠正 `transport.go` 两处误导注释 + `cluster-runbook.md:63` 那句错的）：docs +
    注释 + 一个 hermetic x509 不变量测试；SAN 铸证工具 DEFERRED。
  - `#6` docs mandate（offline op 一律 `sudo -u tether`）+ offline-CLI 的 root-vs-非root-dir **WARN** guard
    （lock-only chown 有害、不做）。
- **不做**：任何 raft/membership 状态机改动、任何编排命令（那是 G2/G4）；#24 铸证工具、#23 产品 return 点。
- **依赖**：无。**风险**：#22 Option B **中**（跨 install.sh/代码默认/sim 多接缝 + 现网多步迁移），其余低。
  **现网止血**：否（但为 G2/G4 扫障）。
- **sim 验收 / 出口**：`drill 13-inbroker-reconcile-perm`（#22）从 RED **翻成普通 GREEN 回归**（断言
  `/etc/tether` **仍** root-owned + `/etc/tether/nats.d` tether-owned + User=tether 写 nats.d/ 成功）；
  `doctor` 加 #22（nats.d/ tether-owned + /etc/tether root guard）+ #23（`Restart=always` drift）检查；
  `grow` trailer **只掉 #22**（经真第二次 grow 实测；**#23/#24 token STAY**——#23 auto-recovery 与 #10
  纠缠、#24 无铸证工具，现在翻转即 mask）。**定稿细节见 `docs/reviews/g1-plan.md`。**

### G2 — force-single 完整化 + 消幽灵（现网止血）
- **含 gotcha**：#12、#20、#10、#15
- **做**：force-single 现在只热换 raft 成 `{survivor}`，是半成品；完整化为三件事：
  - `#20` **survivor 自救**：force-single 必须同时把 survivor 的 nats.conf 降回 standalone JetStream
    （去掉 `cluster{}` 块 + 完整重启 nats-server，重启断 ctl 通道→必须 detached），否则独立
    nats-server 仍 clustered、JS meta `{survivor,dead}` 永 1/2 无 quorum → 每个 JS API 503 静默腐烂。
    受 R3「不静默 de-cluster」约束，但这是 survivor 自救、非成员操作，须谨慎设计门控。
  - `#12` **消幽灵**：force-single 把被弃节点 roster `phase` 降级为非-VOTER（ejected/RETIRING），
    或 `readRosterBrokers` 按真实 raft 成员过滤，或 `recovery node remove` 放行"不在 raft config 的
    幽灵"（先判成员资格、不在 config 就跳 `RemoveServer` 直接删 roster 行，~3 行）。任一/组合，使
    幽灵可在线删除、且 `--to-standalone` 自然解锁（承接 #20）。区分两类 voter：崩了仍是成员（保留、
    会回来自愈）vs force-single 单向踢出（降级/排除）。
  - `#10`/`#15` **被弃节点冷启动 actionable**：被弃节点盘上 raft config 仍含它自己（voters=2），冷启动
    撞 N=1-clustered-JS 陷阱退 70 崩溃循环；应检测"我已被 peer force-single 踢出"（本地 config 含我、
    peer 不认）→ actionable 报错直指 rejoin 步骤，别崩溃循环、别尝试 2-voter JS。可选一条
    `cluster reabsorb <node>` 自动 wipe+rejoin。
- **不做**：`#20③` 的 JS-503 告警（→ G7）。
- **依赖**：无硬前置（G1 的 #23 会让 #20 的 nats 重启不再弄死 broker，建议 G1 先落）。**风险**：中高
  （动 membership 状态、force-single 语义、去集群化门控）。**现网止血**：✅（racknerd）。
- **sim 验收 / 出口**：`drill 20-forcesingle-natsconf`（#20）+ `drill 12-ghost-voter`（#12）从
  signature-guarded RED 翻成普通 GREEN 回归（force-single 后 nats.conf 去 `cluster{}` 块、tier-B push
  恢复；幽灵 phase 降级、三条删除路径至少一条成功）。

### G3 — 成员变 → 客户端视图自动收敛
- **含 gotcha**：#1、#17、#11（#9 已修）
- **做**：
  - `#1` membership 变化（join/retire/force-single 提交）后，leader 自动从各 broker 的 `broker.yaml`
    public_host/domain 派生 client-dialable 端点、自动 bump `seed_generation` 发布——不再手动
    `cluster seeds publish`。
  - `#17` 任一成功连接（不限 `FloorURL` broker）都用该 broker 推的签名 roster 刷新 ctl 缓存，使 roster
    动态发现不依赖单一 floor/bootstrap host（拆 `refreshCtlEndpoints` 四重门）。
  - `#11` cluster seeds 让客户端缓存"按真实 IP 直连"的 fallback，使运维/观测通道不被单点 DNS/代理链
    卡死（failover 期间还能够到幸存者）。
- **依赖**：**G2**（#12 幽灵可删后，发布名册才收敛到真实 raft 成员，否则死端点永不消失）。**风险**：中。
- **sim 验收 / 出口**：新增 drill——grow/force-single 后 `cluster seeds show` 自动含新成员且不含被弃
  节点；杀 floor broker 后 ctl 仍能从非-floor 幸存者刷新到最新 roster。（暂无现成 RED drill，G3 plan
  阶段新增。）

### G4 — grow 编排 `tether cluster add`（终极自动化目标 §B）
- **含 gotcha**：§B、#3、#4、#5、#7、#8（顺带收编 #10 的 mesh-before-joiner 顺序、#23 的 SIGHUP-reload）
- **做**：把整条 grow 序列（leader resnapshot 若需 → joiner `init` → `join` → 渲染 mesh → reset JS →
  发布 seed → rebalance proxy）收进**一条 `tether cluster add <broker>` 编排命令/向导**，全自动幂等
  可恢复：
  - `#3` auto-reconcile 在 conf 无 `cluster{}` 块时 fallback 到 `secrets-dir` 取 mTLS，让首次
    standalone→clustered grow 的 `reconcile nats --all` 能自动渲染 mesh。
  - `#4` grow 自动检测 + reset former-N1 JS store（或 `backup`→reset→`restore` 保数据），别孤儿化旧流。
  - `#5` `cluster init` 加机器可逃生的 confirm（`--yes-i-understand` 或同 resnapshot 的
    `--confirm-node-id`+env），编排不必 pty 喂 TTY。
  - `#7` catch-up deadline（`opCatchupTimeout=2min`）按快照大小/进度自适应，或 stall 后自动重试。
  - `#8` 编排里 `AddNonvoter` 后自动渲染 mesh、再判 catch-up、再 `AddVoter`（解"先 join 再 reconcile
    nats"的鸡生蛋）；`join approve` 不带阻塞 `--wait`。
- **依赖**：**G1**（#22/#23/#24 修好，编排各步才不用手动 root/SAN/SIGHUP 绕）。**风险**：高（新大编排、
  幂等恢复语义）。**现网止血**：否（是终极提升）。
- **sim 验收 / 出口**：`simcluster grow` 的手动 workaround 逐个换成 `tether cluster add` 真命令 →
  `drill 10-grow-to-3` 保持 GREEN、`drill 11-grow-gaps` 的 `GREW-VIA-WORKAROUNDS` trailer **清空**
  #3/#4/#5/#8/#10（trailer 断言全 flip，drill 11 退役或翻成 `cluster add` 幂等回归）。**`simcluster grow`
  是这条编排的可执行规格**（已跨进程跑通 N=1→2→3 全 VOTER）。

### G5 — 滚动升级编排 `tether cluster upgrade`
- **含 gotcha**：#13、#14、#19
- **做**：`tether cluster upgrade --all`——一次 install 新二进制 + 重启 broker systemd 守护 + re-exec
  同机 agent，**滚动一台一台、先 `transfer-leader` 再重启**（避 #14 的 N=2 no-quorum 写阻塞 blip），
  幂等可恢复 + 聊天室通报（#13）；`node ls -a` 对 broker 主机同显 broker+agent 两版本、同机 skew 标
  warning，让"整机升级完成"有单一可信判据（#19）。文档明确推荐生产 N≥3、N=2 仅过渡态（#14 根因）。
- **依赖**：无。**风险**：中（滚动编排 + leadership 转移时序）。
- **sim 验收 / 出口**：新增 drill——N=3 滚动 `cluster upgrade` 全程无写阻塞、每台先 transfer-leader、
  升级后三台版本一致。（G5 plan 阶段新增。）

### G6 — 容量感知 MaxBytes（小盘 broker）
- **含 gotcha**：#21
- **做**：`OBJ_xfer`（及 `events`/`history`）的 `MaxBytes` 从硬编码 8 GiB 改为**盘感知/可配**（按 broker
  `max_file_store` 或磁盘容量按比例缩放，或 agent.yaml/broker.yaml 覆盖），小盘 broker 自动收敛到更小
  per-session 上限；JetStream `max_file_store` 在 broker 侧显式渲进 nats.conf（别裸依赖 nats "75% 空闲盘"
  默认），让容量可预测可告警。**注意**：`max_file_store` 不能作为 jetstream 子键裸写（会 brick
  `reconcile`，见 sim §9 OQ-5）——渲染方式要经 sim drill 21 校验。
- **依赖**：无。**风险**：中低。**现网止血**：✅（racknerd tier-B 全废）。
- **sim 验收 / 出口**：`drill 21-smalldisk-tierb`（#21）从 RED 翻 GREEN（4g tmpfs 小盘上 tier-B push
  不再被 JS storage admission 10047 拒）。

### G7 — 数据面再均衡 + 可观测/告警
- **含 gotcha**：#2、#18、#16、#20③（可再拆：G7a 数据面 #2/#18；G7b 可观测 #16/#20③）
- **做**：
  - `#2` 每个 exit 用它自己 home broker 的 public_host 渲染（不用订阅 host），修 proxy status /
    订阅内容的 host 错标；缩短 provider interval / rebalance 后让订阅带 staleness 信号。
  - `#18` broker rejoin/回归后 leader 自动评估并触发一次 proxy 再均衡（dry-run 可预览、受命脉稳定约束），
    别让 failover 的"粘"性使分布永久倾斜。
  - `#16` 给 `cluster status --homes` / `cluster seeds show` 加 `--remote` 变体；`--remote` 在
    force_single 下应 `exit 3`（与本机 status 对齐，让监控按 exit code 抓 emergency）。
  - `#20③` JS 503 持续应触发告警（现 `alert ls` 只有 `broker_down`，JS-down 无任何主动信号——数天 JS
    全废却无告警是最大运维盲区）。
- **依赖**：无（`#20③` 概念上承 G2 的 #20，但告警是独立可观测面）。**风险**：中。
- **sim 验收 / 出口**：新增 drill——rebalance 后订阅每出口指向真实 home、broker 回归自动再均衡；
  force-single/JS-down 场景 `alert ls` 出主动告警、`--remote` force_single exit 3。（G7 plan 阶段新增。）

---

## 3. 全量 gotcha → 批 映射（24 条 + §B，无遗漏）

| gotcha | 批 | 备注 |
|---|---|---|
| #1 seed 手动发布 | G3 | |
| #2 proxy status/订阅 host 错标 | G7 | |
| #3 首次 grow reconcile 渲染不出 | G4 | |
| #4 former-N1 JS reset 手动+损失 | G4 | |
| #5 cluster init 无机器逃生 confirm | G4 | |
| #6 tether.lock root-owned | G1（顺带） | sim 不复现（标准化 User=tether init） |
| #7 catch-up deadline 太短 | G4 | |
| #8 catch-up 鸡生蛋顺序 | G4 | |
| #9 ctl 无持久 roster | — | **已修 v0.4.6** |
| #10 N=1-clustered-JS 陷阱 | G2（冷启动）+ G4（grow 顺序） | 横跨两批 |
| #11 观测通道依赖死节点 | G3 | seeds 让客户端有 IP-直连 fallback |
| #12 force-single 幽灵 VOTER | G2 | 堵点：挡死 #20 + G3 |
| #13 无 quorum-safe 滚动 broker 升级 | G5 | |
| #14 N=2 滚动重启 no-quorum blip | G5 | transfer-leader 先 |
| #15 被弃节点冷启动 STUCK | G2 | |
| #16 缺 --remote 变体 + exit code | G7 | **已纠正**：只剩 --homes/seeds + exit 3 |
| #17 roster 发现 floor 单点 | G3 | **已纠正**：failover 本就工作 |
| #18 broker 回归不自动再均衡 | G7 | |
| #19 同机 broker/agent 版本割裂 | G5 | |
| #20 force-single survivor nats.conf 滞留 clustered | G2（主体）+ G7（③告警） | 现网烂 5 天 |
| #21 硬编码 8 GiB OBJ_xfer 拒小盘 tier-B | G6 | |
| #22 install.sh 漏 chown /etc/tether | G1 | sim drill 13 |
| #23 broker 丢 nats clean-exit 永不回来 | G1 | unit Restart |
| #24 route-cert 需 SAN | G1 | |
| §B 终极自动化 `cluster add` | G4 | grow 编排总目标 |

---

## 4. 各批开工约定

- 每批**独立**按 CLAUDE.md §3 走 3 阶段 7 步：Workflow 对抗草拟（固定 agent 数、≥Opus 4.8）→ 主进程
  定稿 `docs/reviews/g<N>-plan.md` → 主进程实现 + 测试 → Workflow 对抗内审（`g<N>-review.md`）→ 用户
  外审（`g<N>-external-review.md`）→ commit（直提 main）。
- **测试纪律**：每批的 Go 单元/集成/并发测试进 `make test`/`make e2e`；**部署面批（G1/G2/G4/G6）另跑
  相关 sim drill 作为 deploy-tier 门**（`cd test/simcluster && ./remote.sh drill <name>`，一次一个）。
  修好一个 gotcha，就把对应 RED drill 从 `assert_bug` 翻成 `assert_ok`（普通 GREEN 回归），并从
  `simcluster grow` 的 trailer 移除对应 workaround token。
- **不变量约束**：G2/G4 触碰 R3「不静默 de-cluster」，plan 必须显式论证门控；wire 改动守
  `internal/proto.ProtoVersion` SSOT（多为 additive、`ProtoVersion` 不翻）。
