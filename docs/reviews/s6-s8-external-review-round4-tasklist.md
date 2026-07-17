# S6-S8 外部重审 Tasklist（round-4，特殊 stage）

> 基线：round-3 已暂存内容；对象：开发者声称整体 remediation 的未暂存修改。先独立审查并暂存快照，随后审查者切换实现者修至合格。

## A. 边界、流程与证据

- [x] A1. 固化 staged / unstaged / untracked 边界，核对 18 个修改文件与 2 个新测试的完整范围。
- [x] A2. 将开发者 round-3 回复的 B1/B2/M1/M2/m1、round-1 B1/M1-M6/m1 映射到具体 diff 和测试。
- [x] A3. 独立核查本地/远端测试声明、日志、SHA/残留证据，禁止仅采信回复中的结果表。

## B. Harness 与 runner 契约

- [x] B1. 审查五态 grammar、参数校验、计数器、优先级、退出码、重复 verdict 与自由文本安全。
- [x] B2. 审查 `assert_setup` / `setup_fail` 的终止语义、trap/cleanup 兼容性及 `set -u/-e` 行为。
- [x] B3. 审查 runner 对合法/非法/缺失/重复 verdict、rc mismatch、未知 rc、日志注入的解析。
- [x] B4. 审查 blocker 计数、owner-policy、suite exit 上限、重试资格和首跑证据保存。
- [x] B5. 审查 hermetic contract tests 是否独立、覆盖真实 runner、不会自证同一错误契约。
- [x] B6. 审查 lint 禁令的范围、误报/漏报、目标文件发现和规避方式。

## C. 九 drill 原 finding 复核

- [x] C1. 22/#35：MainPID、自动 crash-loop、多时间点、全观察窗 DRY、candidate ledger 与 gate 完整性。
- [x] C2. 40：真实 renderer 正臂、零写/.bak、ops abort/confirm/apply-plan/add-dryrun/mid-retire。
- [x] C3. 41：逐次 voter count、terminal op、N=1/to-standalone/JS reset、session 存活、tier-B >8MiB。
- [x] C4. 42：namesake 诊断、source broker/timestamp、prune/rejoin/persistence 与缺口 disposition。
- [x] C5. 43：outcome-(b)、真 live-row/data-plane、非交互 cutover、DB/conf/restart/cluster-off rollback。
- [x] C6. 90：JSON fail-closed、presence/absence、return 条件、dedup key、disk/below-quorum raise-clear。
- [x] C7. 91：D/A3/anchor locked cells、fixture 与 PRODUCT-RED signature 的因果性。
- [x] C8. 92：独立 session、自然 quorum-loss、write-path 排他、tier-B 标签、banner/recovery 同对象闭环。
- [x] C9. 93：HTTP status/body、ready 负匹配、webhook schema/transition/leader、CARD/JSON、PTY watch、exit code。

## D. SSOT 与测试执行

- [x] D1. 核对 #37/#42/#45/#46 跨 plan/ledger/review/README/inventory 的唯一映射与状态。
- [x] D2. 核对 roadmap、README、inventory、review 最终状态与实际 verdict/覆盖相符。
- [x] D3. 运行 diff/shell 静态检查、三壳 contract test、lint，并构造额外对抗性 runner/API 探针。
- [x] D4. 在需要时复跑远端精确 simcluster，并核验日志、秘密扫描和容器/进程残留。

## E. 审查快照与实现切换

- [x] E1. 形成首行 Fail/Pass 的 round-4 独立审查报告，写明问题、疑惑、建议和证据。
- [x] E2. 将 round-4 审查时点全部文件加入暂存，确认工作树干净。
- [x] E3. 切换实现者后逐项修复 harness/oracle/产品问题，新增独立测试并更新 SSOT；无法闭环的真实产品
  RED 保留为 blocker，禁止用重试、豁免或弱断言擦除。
- [x] E4. 完成本地与远端验证、逐要求完成审计，按实际证据形成 Pass/Fail 实现报告并交付开发者反向审查。
  - 最终事实：40/41/90 GREEN；42 主冷启动缺陷关闭但后半段 fixture SETUP-RED；91 survivor-only seeds
    仍 ASSERT-FAIL；93 未完成当前镜像复跑。按停止指令结束，不继续修改，报告结论保持 Fail。
