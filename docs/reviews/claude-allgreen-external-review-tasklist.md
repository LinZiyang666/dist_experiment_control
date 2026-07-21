# claude 外审 tasklist — allgreen/simcluster 整治工程（R1–R15）

审查范围：**暂存区全部文件**（`git diff --cached`）。工作树与暂存区一致，直接读文件。
定位：外部审查者，与主进程无关，**不独信任何内审/收官结论**（含 `r15-finalization.md`、`allgreen-remediation-roadmap.md`、`expected-verdicts.tsv`）——一律当索引，逐项从代码/测试/落盘证据/live 复跑独立复核。
过程中出现的 `codex` 前缀报告与测试文件**一律忽略**（非本审查范围；但记录其对硬闸的影响）。

- [x] 读 CLAUDE.md、architecture/cluster/simcluster 定位铁律、前任外审体例
- [x] 从 `git status` + `git diff --cached --stat` 重建审查边界（生产码 + 测试 + harness + docs + install.sh）
- [x] 读 roadmap/r1/r2/r6/r15 plan 与 findings、缺陷报告、coverage-boundary 作为索引（不信任结论）
- [x] 核对收官声明（G-1…G-10）与落盘证据（expected-verdicts.tsv / kept-sites.baseline.tsv / rollup）一致性
- [x] 深审 R7 周期对账注册表 + 锁 lease（一票否决不变量 / R-a 周期化破坏 / TOCTOU / leaderOnly）
- [x] 深审 R8 home 主动投递（P1 drain/retire rc 语义 / #33 / #48 / ack 通道安全）— **逐行亲自复核 gate**
- [x] 深审 R9 agentless upgrade + cluster unlock/lock-keeper（P3 / HALT 锁 / force-single 确认）
- [x] 深审 R10 DR/备份恢复（P2 restore --config / P4 文案 / P5 doctor 五态 / #53 / restore 残留清理）
- [x] 深审 R11/R12 身份凭据与接入安全（#54 skew / PIN 限速 / Q2 / webhook / adminsock / re-pin）
- [x] 深审 R13/R15（runtime 自省 / admin events / #58 orphan reap / #93 webhook baseline / P8）
- [x] 横切并发/竞态/死锁/fail-open（新 goroutine 生命周期 / 锁序 / raft apply 阻塞 / lease 时钟）
- [x] simcluster harness 审查 + **本地跑全部 hermetic 自检**（run-all / lint-* / verdict-contract / kept-sites / ledger-crosscheck / poll-reentrancy / r9d-nonvacuity）
- [x] drills 谓词诚实性抽查（谓词方向 weaken / 恒真 / runtime-guard 藏债 / 残留计数核对）
- [x] 文档订正核查（runbook §5 seam/DOC-27 / gotchas 台账 #29/#34/D1-D6）
- [x] 独立复跑三硬闸：`go test -count=1 ./...` / `make e2e` / `make lint`
- [x] 触碰面 `-race` + 仓库内建 NumGoroutine/fd 泄漏门（broker/agent/cmd/authcallout/concurrency）
- [x] weilandserver 探测 + `remote.sh --build build`（正确 binary+镜像）+ deploy-tier 针对性复跑（30/42/50/51/82/96）
- [x] 写外审报告（claude 前缀），开头 Pass/Fail 结论
- [ ] 报告完成后将所有文件加入暂存
