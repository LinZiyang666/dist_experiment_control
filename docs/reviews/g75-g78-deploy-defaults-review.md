# g75-g78 部署层默认值 — 内审报告(多专家对抗性审查 + 主进程裁决)

> 2026-08-11。审查方法:6 维并行审查(wire-n1 / backoff / broker-optout / warn-damp / install-sh /
> serveconf-visibility)→ 6 维独立对抗性验证(默认怀疑,多条经 dash 最小复现 / ubuntu:24.04 容器端到端 /
> 临时 in-package 测试驱动真实实现复现)→ 综合(Workflow 13 agents)。报告只收录验证阶段 CONFIRMED 的
> findings;主进程逐条裁决如下。**专家只读实现、只建议测试;全部实现改动由主进程完成。**
> plan:`g75-g78-deploy-defaults-plan.md`(同目录)。

## 总量与结论

BLOCKER 1 · MAJOR 6 · MINOR 12 · NIT 13;REFUTED 2 整条 + 2 子句。六维中 wire-n1 / backoff /
broker-optout 的核心不变量被验证为**干净**(N-1 双向零值字节等价、raft payload 未动、退避状态机无
边界 bug、四步闭环真实闭合);发现集中在 **warn-damp 的多源语义**、**install.sh 的环境分支诚实性**、
以及 **drill 32 的两处 subshell 丢函数 bug**(B1/M3——审查抓到了主进程自己引入且变异验证漏做的守卫)。

## 裁决与处置(全部已落地,除注明外)

| # | 严重度 | 一句话 | 裁决 | 处置 |
|---|---|---|---|---|
| B1 | BLOCKER | drill 32 uninstall 断言 `sh -c` 丢函数,无条件 rc=127 | **采纳** | 改当前 shell 具名函数 `_uninstall_then_notfound`;合并重复的 `_units_notfound`→`_units_disabled`(N10);drill 32 待 weilandserver 实跑复核 |
| M1 | MAJOR | 多源类别交替击穿 REGISTER WARN 降频(单共享 Tracker 的 class-change 契约误用于多源站点) | **采纳** | `regLog` 改 per-class 三个固定 Tracker(静态宽度有界);收 `TestRegisterReadDampingInterleavedClasses`(变异:回退共享 Tracker→红,实测 4 FAIL) |
| M2 | MAJOR | `registerReadOK` 在 parse/auth 前接线:未鉴权垃圾行伪造 recovery、重武装 WARN | **采纳** | 挪到 `tokenLookup` 成功后;收 net.Pipe 垃圾行测试(变异:挪回 read 后→红,实测 4 FAIL);broker-ops recovery 措辞随之成真 |
| M3 | MAJOR | drill 32 journald oracle 是 `[ "" = "" ]` 恒等式(同 B1 根因、相反极性) | **采纳** | 改 `_caps_match` 当前 shell 函数 + **非空守卫**;主进程承认该守卫落地时漏做变异验证(违反自家规矩),已按台账记录 |
| M4 | MAJOR | 无 systemd 宿主 banner 假称 ENABLED(#76 在环境守卫路径复活) | **采纳** | `ENABLED_UNITS` 三分支 banner(enabled / no-enable / systemd-absent 完整指引);收 `TestInstallShBrokerBannerSelfConsistent` |
| M5 | MAJOR | "operator setting respected" 早退不删自家 stale drop-in(声称尊重、实际压制) | **采纳** | 早退分支 `rm -f` 自家 drop-in + log;收 `TestInstallShJournaldStaleDropinRemoved`(连带 Mi6 spaced `=` 识别) |
| M6 | MAJOR | broker-ops pre-flip 验证指引会让新二进制对活库跑 migration | **采纳(产品化)** | 新增 `tether serve --config-check`(strict 解析 + 全校验器后、任何副作用前退出);收零副作用测试(DB 不存在→仍不存在);文档改指它;CLI golden 已更新;**集群侧不加东西**(该子句 REFUTED:AcquireDataDirLock 是 LOCK_NB 非阻塞) |
| Mi1 | MINOR | wire 冻结注释 cross-version 归因错误(forward 载荷键集冻结,无键可丢) | **采纳** | 注释重写:降级归位 OLD answering broker;补记混版残余(OLD leader reaper 缺 teardown 腿) |
| Mi2 | MINOR | cluster `proxy status` opted-out 节点整行消失,注释归因错 | **采纳(诚实呈现路线)** | 保持 render-equivalence 过滤(plan Non-goal 11);cmd 注释改为如实归因(query-shape gap 非 replication gap);usage [GAP] 改为"整行缺席"+ 判别法 |
| Mi3 | MINOR | adapter-nil / no-footprint 两臂是残留 5s 永久 WARN 洪水 | **采纳** | 两臂改 announce-once(`configWarnAnnounced`,成功 start 复位) |
| Mi4 | MINOR | 单机 opt-out register × 并发 `proxy on` 竞态泄漏无人收割的 ALLOCATED 行 | **采纳** | `freeOptOutProxyRowSingle` 取 `proxyOpMu`;收确定性锁序测试(hold 锁→free 阻塞→放锁→收割) |
| Mi5 | MINOR | regLog 无时间维度重申,Cap 是死配置 | **采纳(Due 重申门)** | `Due()` 到点的失败照常 WARN——Cap 变成"至多每 5min 重申一条"的活语义;收 `TestRegisterReadDampingReaffirmsPerCap` |
| Mi6 | MINOR | grep 漏 `SystemMaxUse = 2G`(= 两侧空白) | **采纳** | 模式改 `SystemMaxUse[[:space:]]*=` |
| Mi7 | MINOR | uninstall dry-run 输出依赖宿主 | **采纳** | dry-run 下跳过 systemd 探测、恒预览;p10 断言收紧 |
| Mi8 | MINOR | 无 systemd 宿主 uninstall 留悬空 symlink + 假 "disabled" 日志 | **采纳** | else 分支手动 rm wants/ symlink;收尾日志按 `_DISABLED` 分支如实 |
| Mi9 | MINOR | df 非数字落最大档(失败方向反了) | **采纳** | `case` 非数字→0→最小档 |
| Mi10 | MINOR | verifyClusterSeam 对 parse 错误配错补救指引(引向补 seam 循环) | **采纳** | 文案分流:"does not parse (strict since #75) — fix the named key" |
| Mi11 | MINOR | broker-ops 把 xfer 两键样例放 observability 块,就地取消注释=拒启 | **采纳** | 样例挪进 `cluster:` 块 |
| Mi12 | MINOR | 配对门不覆盖注释态 cluster 块(文档化 HA 动线) | **采纳** | 新 `TestInstallShTemplateClusterBlockParsesStrict`(bare `# cluster:` 判别,不碰散文注释;变异:注释块塞假键→红,实测 4 FAIL) |
| N1 | NIT | hint 先于 raft 提交被改写(短暂自相矛盾窗口) | **采纳** | hint 更新移到 registerNode 成功后,与列同生共死 |
| N2 | NIT | 退避窗口从拨号开始而非失败计时(首窗被拨号超时吃光) | **采纳** | `proxyDialFailedLocked` 内取失败时刻时钟(参数移除,lint 强制了这次清理) |
| N3 | NIT | reaper 注释"rotation 会 re-mint"不实(结构上不可能) | **采纳** | 注释改写为"再无人碰这行" |
| N4 | NIT | teardown Propose 失败仍发假 `freed` 事件 | **采纳** | Propose 失败 `continue`(下 tick 完整重做) |
| N5 | NIT | `proxyOptedOut` sync.Map 条目 session 删除后残留 | **采纳(仅注释)** | 上界=历史 opt-out (sid,nid) 去重数、restart 清零、无渲染影响——安全实用主义可接受;注释已说明 |
| N6/N7 | NIT | class-change WARN 字段错位 / recovery 无 class | **采纳(随 M1 顺带解决)** | per-class Tracker 后每条 WARN/recovery 天然带 `class` 属性 |
| N8 | NIT | usage() "no backtick" 注释被证伪 | **采纳** | 注释改为"backticks escaped" |
| N9 | NIT | journald 扫描面缺 /run 与 /usr/lib 层 | **采纳** | 扫描循环加两目录 |
| N11 | NIT | dry-run banner 过去式 | **驳回(仅记录)** | 与既有 banner 风格一致,不动 |
| N12 | NIT | logging.go 死分支与注释矛盾 | **采纳(注释)** | 死分支保留为 future-proofing,注释明说 unreachable-today |
| N13 | NIT | parse 错误双重包装打印两遍 | **驳回** | 纯观感;错误链 `%w` 语义正确,拆包装得不偿失 |

**REFUTED 简表复核(主进程认可全部四条驳回)**:backoff F4(Recover fold 结构不可达+注释已存在)、
backoff F5(homeEpoch 双源结构闭合;其 pinning 测试建议留给外审裁决是否加)、wire-n1 F3 第二腿
(map 残留无可观测面=N5)、serveconf F1 集群子句(AcquireDataDirLock LOCK_NB 非阻塞,第二进程立即失败)。

**审查补测试中未采纳的两条**(如实记录):homeEpoch bypass 表行与 class-不重置-梯子表行——
`TestProxyDialBackoffBypassOnNewIdentity` 已覆盖 gen/epoch/port/token 四分量,homeEpoch 分量与
class 维的显式表行是加固项,留外审裁决是否补(机制上 `proxyDialID` 含 homeEpoch、Tracker 的 class
只影响 logNow 不影响 schedule,均已由实现结构保证)。

## 变异验证记录(本轮新增守卫)

| 守卫 | 注入 | 结果 |
|---|---|---|
| serveconf 严格解析表测 | decoder 回退 `yaml.Unmarshal` | 4 案红 ✅ |
| 模板配对(正向) | install.sh 模板注野键 | 红 ✅ |
| 模板配对(反向) | 删 TLS stub | 红(编译失败形态)✅ |
| 注释态 cluster 块配对(Mi12) | 注释块塞假键 | 红 ✅ |
| seam-probe 传播 | 恢复 `lerr == nil` 吞错 | 红 ✅ |
| breadcrumb | 删 Fprintf | 红 ✅ |
| install.sh enable | 删 `enable_broker_units` 调用 | 红 ✅ |
| journald 条件写 | grep 不排注释态 / 不排自身 / 删发射 | 各红 ✅ |
| agent 退避 Due gate | 删 gate | 红 ✅ |
| agent 退避 bypass | identity 比对写死 | 红 ✅ |
| agent 退避 bypass 清零 | 删 new-identity 清零 | 红 ✅(此变异先红后修——bypass 原实现缺清零,是变异验证抓出的真缺陷) |
| agent opt-out 三守卫 | 删 gate / 删 boot 清 footprint / 删 register 接线 | 各红 ✅ |
| broker opt-out 四守卫 | 删 fold / 删 register free / 撤 reaper 查询腿 / 删 status hint | 各红 ✅ |
| wire omitempty | 去 omitempty(两字段) | 红(golden 同时抓 register 侧)✅ |
| WARN 降频(M1) | 回退单共享 Tracker | 红 ✅ |
| recovery 接线(M2) | `registerReadOK` 挪回 read 后 | 红 ✅ |
| WARN 降频恒 WARN | 绕过 Tracker | 红 ✅ |
| recovery 汇报 | 删 recovery Info | 红 ✅ |
| yaml 三态 | `*bool` 改裸 bool | 红 ✅ |
| M3 oracle(drill) | (hermetic 侧由 `_caps_match` 非空守卫承担;drill 侧变异= 改 install.sh tier 阈值,随 drill 实跑轮做) | 待 drill 轮 |

## 硬闸状态

`make lint` 0 issues;`make test` + `make e2e-parallel` 见收尾记录(整合后重跑);
结构预算/命名冻结/wire 账本/CLI golden/determinism 全绿。simcluster drill 32/93/78 待 weilandserver
实跑(改了部署栈,按 CLAUDE.md 必跑相关那三个;含对 v0.5.0 发布二进制的变异轮)。

## 遗留(不阻塞本增量,已登记)

- 审查附记:`--uninstall` 不带 `--dry-run` 时先走 `maybe_resolve_version`,离线主机 uninstall 会先死在
  release tag 解析——**先在行为,已顺手修**(`maybe_resolve_version` 对 `UNINSTALL=1` 早退,uninstall
  从不下载、不需要 release tag)。
- Mi5 的联动说明(M2 修复使"安静 broker 跨周同类事件"更依赖 Due 重申)已由 Due 门解决。
- N-1 [GAP](旧 broker × opted-out 新 agent 渲染 never-ready)与 cluster status 整行缺席 [GAP] 均已文档化。
