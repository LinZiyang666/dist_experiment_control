# R6 plan — 定案实验批（定稿）

Date: 2026-07-19 · 总纲：`docs/reviews/allgreen-remediation-roadmap.md` §4-R6 · 前置：R5
**本批铁律：只取证，不改实现。** 出口硬断言 `git diff --stat internal/ cmd/ scripts/` 为空
（唯一例外：纯观测的日志补强，须论证不改变任何控制流，且走独立提交、ledger 比对在观测改动之前的构建上做）。

## 0. 为什么必须有这一批

缺陷报告 §5 的 Q1–Q4 明写「**不得当作已定结论**」；台账里另有 7 条 `[CANDIDATE]/候选`。
把未定案的东西直接当缺陷修，会产生两类浪费：修错方向（Q1 的三个假说要的是三种不同的产品改动），
以及把「设计如此」当成缺陷去"修"（H8 已经是活教训：`gateDestructive` 拦 push 是设计，
drill 漏传 flag 才是 bug）。

## 1. 输出契约：每条必须产出五元组

**【假说 → 可证伪预测 → 实测证据（日志行号/命令输出）→ 裁定 → 归属批】**

裁定只有三种：`CONFIRMED-DEFECT` / `REFUTED` / `AS-DESIGNED`。
**禁止输出「可能 / 疑似 / 大概率」**。无法定案者必须写成
「实验设计 X 已执行、证据不足、需 Y 条件」并排入 R14——
**不得默认为缺陷，也不得默认为非缺陷**（两个方向的默认都是在替证据下结论）。

## 2. 待定案清单

### 2.1 开放问题（缺陷报告 §5）
| # | 假说 | 可证伪预测 | 采证要求 |
|---|---|---|---|
| **Q1** | A=account.nk 轮换单向砖化(blocker) / B=纯计时 / C=C3 reconciler 重渲造成反向 skew | A：恢复 gen-1 后 issuer 永不回退；B：延长窗口即恢复；C：conf issuer 与磁盘 seed 反向不一致 | **必须同采 `nats.conf` 的 issuer 与 `broker.err`**——只采其一无法区分 A/C |
| **Q2** | 被分区少数派对客户端是连接黑洞 vs 设计中的 fail-closed 拒绝 | `handler.go:102-104` 的 `fenced()`→`deny` 是设计意图 ⇒ **brk1 journal 应有 `authcallout: handle failed`**；有=候选机理②成立，无=另找 | brk1 broker journal + tether-broker 存活探针 + 路由快照 |
| **Q3** | `exec` 返回 rc=0 但进程表 30s 内从未记为 RUNNING | 若是登记路径问题，`ps -a` 全量应能看到该 ULID 但状态异常；若进程真没起，agt2 journal 应有 spawn 失败 | `ps -a` 全量 + agt2 journal + brk1 exec 登记路径 |
| **Q4** | `session create` 写已提交却报失败 | 机理三候选：①`readCommittedSession` apply-lag ②`session.go` 5s 客户端超时→rc=69 ③harness 的 `timeout 15` | **一次保留 stderr + rc 的定向复跑**即可定案（现谓词 `>/dev/null 2>&1` 把证据全丢了） |

### 2.2 候选态 gotcha（`ledger-crosscheck` 现列为 `R6-CAND`）
`#33` `#35` `#45` `#48` `#49` `#55` `#59` `#63` `#65` + `95-D`（很可能是**假缺口**：谓词硬钉
`leader_id=="brk1"`，而注入前 brk1 的 broker 已被两次干掉，N=2 下 leadership 完全可能停在 brk2）
+ `96:240` arm B 是否 source-closed。

**`#65` 特殊处理**：raft safety 级，需 **≥10 轮**统计定性，且**必须在 H6 修好后的干净地面真相上做**
（R1 已查明 D6b 此前是空绿，#65 的既有记录被污染过）。
**若判 CONFIRMED ⇒ 触发条件批 R8x，并预先声明：raft safety 缺陷未闭合前工程不得宣布全绿。**

### 2.3 「设计如此」判定表（严禁当缺陷"修"）
`gateDestructive` 拦 push/pull/run/expose/session rm（H8 已证）· force-single 必须人工确认 ·
`handler.go` 的 `fenced()`→`deny`（待 Q2 证）· #34 的 fire-gate 正确 DEFER（待复核）。

**AS-DESIGNED 项的处置**：对应 drill 改用 **`assert_refuses` 带签名正面钉住那道拒绝**
——不是绕过、不是删除、不是放宽。`kept_sites` 因此**不降反升**。

### 2.4 结构性缺口三判据裁决（总纲 §2）
`#55` → 已定 (a) 给产品加原子 switch-over 动词（R11）·
**`OQ-2` → 本批必须实测**：CLAUDE.md §5 称 weilandserver 为**专用服务器**（非共享宿主），
须实测能否在独占时段/一次性 VM 上安全构造 kernel nfsd + hard mount。
**T3 四项（宿主/构造/断言/责任人+时限）当批填满或明确填不满**——填不满即锁定 (c) 分支，
收官声明形态在此刻确定，**不许拖到 R15**。
`97 goroutine` → 已定 (a) 给产品加 runtime 自省能力（R13）· `OQ-6` → 供给缺口，R9 补。

## 3. 出口断言

1. 上述每条产出完整五元组，**零「可能/疑似」**。
2. AS-DESIGNED 项已在对应 drill 落 `assert_refuses` 签名断言。
3. `#65` 判定完成；若 CONFIRMED，R8x 已立项并写入总纲。
4. `OQ-2` 的 T3 四项**填满或明确填不满**，`coverage-boundary.md` 初稿落盘。
5. `ledger-crosscheck` 的 `R6-CAND` 行**全部转为 ok 或从台账降级/删除**。
6. **`git diff --stat internal/ cmd/ scripts/` 为空**（纯观测改动除外，且须独立提交）。

## 4. 验证
针对性单跑 40 / 52 / 62 / 95 / 96 + scratchpad 一次性取证脚本（不入库）。**不跑全套、不跑 e2e。**
