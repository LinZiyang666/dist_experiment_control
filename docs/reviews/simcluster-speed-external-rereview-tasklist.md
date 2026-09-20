# simcluster-speed 外部复审 tasklist

> 2026-09-20。角色：独立复审（CLAUDE.md §4.1）。基线：HEAD `a3431a1`；index = 首轮外审结束时 `git add -A` 的受审树
> （107 文件，含首轮报告与用户随后暂存的 CLAUDE.md §4.1）。复审范围 = index 之外的全部修改与未跟踪文件：
> 22 个已跟踪文件的工作树差分（6 个首轮已暂存文件再改：external-review.md 回复、plan §8.9、expose_test.go、
> transfer_finalize_test.go、manifest.sh、arm-aggregation-test.sh；16 个仅工作树修改）+ 2 个新文件
> （`internal/agent/tunnel_adapter_test.go`、`internal/tunnel/open_deadline_test.go`）。
> 主进程在回复里写的"已修 / 全绿 / 变异红"只作待证主张。复审期间不修改实现；tasklist 先落盘再逐项深入。
> 每项后面的括号是实际处置与结果；报告 `simcluster-speed-external-rereview.md`。

## A. 首轮 F1–F5 的逐条关闭

- [x] A1. F1 · 边界（13 个 AddProxy 实现全部改签；四个生产调用点的 ctx 与回复一致；proxy 路径 `ctx-none:` 标注、行为与改前一致）。
- [x] A2. F1 · 迟到安装（fence 之后 / 返回之前的窗口：session 已装且 handler 在 ≤3 s 内答 OK，broker 5 s 窗内一致；握手 ctx 每次
      open 都 cancel 的新行为 → 发现 `dialAndRegister` watcher 的随机分支 = **R2**，900 次 open 探测 0 次命中）。
- [x] A3. F1 · 生命周期（`sessCtx` 仍派生自 `c.ctx`；信号量三条路径配对正确；超时不 release；defer 覆盖 panic；ctx 已 done 且锁空闲
      时随机命中锁分支无害）。
- [x] A4. F1 · 回复码（有先例 deny 的切断 → `home_catching_up`，无先例 → deadline 本身；proxy 路径不经梯子，"无 deadline 不重试"
      只是对梯子的约束）。
- [x] A5. F2 · tracked tier 在锁下读；push.req 已把 tier 限定为 {a,b}（`tier_invalid`）；`fin.Tier` 只对 pull 的 audit tier 生效；
      noEntry / finalized 幂等 OK 与修前语义一致。
- [x] A6. F2 · 枚举登记为覆盖扩大；`abandon` 前缀在全仓常量中无歧义（`abandoned` 是变量）。
- [x] A7. F3 · 三种形状自造复跑：完整 post-split + 残留旧父日志 → 三臂行无 legacy；显式点已有臂 → 单行；纯旧归档 → LEGACY 一行按父行 MATCH。
- [x] A8. F4 · dash 下 env 前缀有效；`assert_setup` 的命令替换形状（96）自跑：零每-grow 行、fixture 两行；grow_to_2 写点在两 guard 后；
      失败零行；`setup_forcesingle_n2` 未改。
- [x] A9. F5 · 写点 / 不删 / 只读文件 / unknown / flag 互斥 / attribution pass 不经写点，全部核对。

## B. 独立反例、回归与假阴性

- [x] B1. 首轮两个 overlay probe 转绿（F1 fake 按新接口适配、F2 原样）；F3/F4/F5 首轮形状由 H-8 / E-3 / H-9 覆盖并转绿。
- [x] B2. 复审自造变异 R-M1（切断不报先例 deny）/ R-M2（caller ctx 不 relay）/ R-M4（信 body Tier）/ R-M5（只扫选中臂）/
      R-M6（post-check 之前写）/ R-M7（replay 删 regime.tsv）——全部红。无假阴性。
- [x] B3. 时序假设：三处裕量偏紧 = **R1**（Minor，测试常量）；fence 测试的 20 次迭代实测两种交错 6–11 / 20 ×3，都被覆盖。
- [x] B4. 回归面：tunnel / agent 全包 -race、broker transfer 家族 -race、broker 全包（无 -race 361.8 s）、d6（tag + -race）、vet-tags，全绿；
      fake 适配只改签名，断言语义未变。
- [x] B5. 结构门：Agent 方法预算 127 未放宽；inventory 只 +7 无删；无 golden / 账本放宽；两个新 `context.Background()` 站点均标注；
      `.golangci.yml` 与 index 一致。

## C. 验证、部署层与交付边界

- [x] C1. 独立复跑全部通过（见报告"验证记录"）；四硬闸数字与 rc 文件一致；e2e 首遍红归因为既有 DNS 名字拨号 = **R3**（Note）。
- [x] C2. 真栈：核对 74.SRAB（镜像 #15）原始文件；不再跑 96——A8 已覆盖其与 74 的唯一差别。
- [x] C3. 报告已写，首行 **Pass**；首轮 F1–F5 逐条关闭；R1/R2 Minor + R3 Note。
- [x] C4. 主进程处置 R1/R2 后复跑四硬闸，`git add -A` 暂存全部；不 commit、不 push（收据在报告文末）。
