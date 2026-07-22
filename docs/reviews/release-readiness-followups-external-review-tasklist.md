# 外审 tasklist — §6 release-readiness follow-ups（lane A/B/C/D）

审查范围：**暂存区外全部未暂存修改 + 未跟踪新文件**（`git diff` 53 文件 +1503/-152 与 `release-readiness-followups{-plan,}.md`、`kept-sites-selftest.sh`）。
定位：外部审查者，与主进程无关，**不独信任何内审/Stage-C 结论**（含 plan §5.1 的 Stage-C 自审、§5.3 的 deploy-tier DONE 声称）——一律当索引，逐项从代码/测试/落盘证据/实测独立复核。

- [x] 读 CLAUDE.md / plan / followups 交接文档 / 前任外审体例，重建改动边界
- [x] 全量 diff 粗读（4 段）+ 新增 untracked 文件，建立审查面（本文件）
- [x] **A1** `Node.CaughtUp()`：谓词语义（commit>0 边界 / snapshot-restart / N=1 force-single 影响 / 读序竞态方向）+ 两处 reaper 替换对称性 + 3 个新测试的非空性/判别力
- [x] **A2/A4** record-only 注释的技术声称核实（fsmMutateCh 深度 / dispatch-vs-apply / Barrier 无 apply deadline / RESIDUAL-1 skew-subtraction 论证）——注释即合约，错注释=finding
- [x] **A3** `TestXferReapGraceIsWiredInProduction` 断言顺序 load-bearing 论证 + New() 不拨号前提
- [x] **B3** `applyClusterSeam` natsConfPath 贯通：全部调用点收敛（生产 2 + 测试 8）/ 空值 fail-loud / no-thrash 幂等 / `cluster init --check`、身份 cross-check、打印命令均用新参 / golden 同步
- [x] **B1** IssuerUnverified：语义不放宽原行为（之前也不拒）/ ADVISORY-not-FATAL 相称性 / reconcile 两路径（no-wait + --wait converged）均 warn / converged 前缀保留（drill grep 兼容）/ 无 BrokerUnverified 假警报 / F1 F2 F5 测试对位
- [x] **B2** fresh-box 两步文案：exists-vs-missing 判别（os.Stat 竞态窗口）/ perr 恒非 nil / 执行前提 pin / 记录的 F3 后半 deviation 合理性
- [x] **C1** adminEventsTail truncated：四种停机分类穷尽（scan-cap/ctx-deadline=truncated；cutoff/n-satisfied=complete）/ 边界（恰 5000 条流）/ mid-scan 测试 infeasible 声称 / CLI 空 tail 文案 / JSON additive 兼容
- [x] **C2** serveconf 24h 上界：三 knob 全闭合 / 边界 24h 收、24h1s 拒 / Load() 级生效 / ProcRetention 不误伤
- [x] **C3b** unreapable gauge：`xferBucketOrphanedEverywhere` 与 `homeOwnsXferBucket` 谓词一致性（同数据源？可 drift？）/ resolveHomeForAgent 语义 / fail-quiet 方向 / gauge stale 语义 / 单机不泄漏 / Store-not-Add / metrics 单机隐藏
- [x] **C4** pruneHomeAttempts：liveKeys==seen 键同源 / 空集清空正确 / backoff 保留 / 与 homeDeliveryReset 唯一删除点的关系 / -race
- [x] **D1/D5 台账**：tsv 96/50/30/22/82 行与 gotchas #66/DOC-27 状态一致性；**已发现疑似矛盾：gotchas DOC-27 头标 CLOSED 但正文残留"本条仍 OPEN"段** — 确认并定级
- [x] **D2** drill 30 scene watcher：detach 修复正确性（$(...) pipe 继承）/ LDR 时序 / 有界循环 / kill 语义 / `_scene_sigs` 单源 / replay 不被捕获 / #66 机理叙述与 scene 证据自洽
- [x] **D3** kept-sites：quote-mask awk 正确性（不成对引号/跨行）/ **独立实测新旧 tokenizer 对全部 37 drill 计数中性** / baseline 1399 与 live 相等 / selftest 两臂判别力（手动变异）
- [x] **D4** gate-D withdraw 无行为损失；**D6** coverage-boundary H2 记录
- [x] 独立复跑硬闸：`make lint` / `make test`（-count=1）/ 触碰面 `-race` + 泄漏门 / `make e2e` / `sh test/simcluster/tests/run-all.sh`
- [x] weilandserver deploy-tier 抽查：drill 50（GREEN 声称=registry flip 依据）+ drill 30 serial（watcher 不 wedge + 谓词仍咬）
- [x] 写外审报告（Pass/Fail 开头、疑惑/问题/建议），完成后将所有文件加入暂存
