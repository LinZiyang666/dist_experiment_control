# docs/reviews — 索引

> 每个 phase 收尾（CLAUDE.md §3 step 7）在此追加一行。归档判据见该条款：
> `docs/` 顶层只放"下一次改代码时还会被读"的活文档；描述的工作一旦完成，它就是过程产物，`git mv` 进这里。
>
> 存量的 389 份**不回填**——条款从此刻起生效，回填一次并不会让下一次自动发生，而条款会。
> （389 = `git ls-files docs/reviews | grep -c '\.md$'`，即本增量之前已跟踪的份数。写出命令而不只写数字：
> 本轮闭合核验反复抓到的一类缺陷就是「公布了一个没人能重新导出的数字」，包括我自己publish 的好几个。
> 顺带三个口径都不同，容易混：顶层 `ls docs/reviews/*.md` = 368，递归 `find` = 392，已跟踪 = 389。）

| 日期 | 增量 | 一句结论 | plan |
|---|---|---|---|
| 2026-07-29 | 线二 · 质量闸门加固 | 装齐 S3 剩余闸门；实测推翻 S3 两条核心裁决（maintidx 非注释免疫、exhaustive 的 `default:` 逃生门）；lint 从 139 降到 0。**内审 59 块 + §5/§6 十四条全部处置**，其中三条影响生产行为（`node upgrade --all` 会把打错的 `--url` 继续扇给车队、incident bundle 静默丢审计正文、PTY 耗尽的 ENOSPC 落在终态支）。闭合核验推翻过我自己一次「全部完成」的结论——分母 53 是错的（真值 59），且三条我从未看见 | [line2-plan.md](line2-plan.md) · [line2-review.md](line2-review.md) · [外审](line2-external-review.md) · [复审 Pass](line2-external-rereview.md) |
| 2026-08-01 | upgrade-safety（N-1 更新规范 + 升级安全） | requirements §6.7 改写为 N-1 窗口（join 门为显式豁免）；wire 字段 append-only 账本入 gates；agent 升级状态机（冒烟纪元校验 / prev 槽 / marker / boot shim 预算 / register 提交 / 自动回退，host flock 一个升级域 + 四件套提交证明 + 有序 fsync）；ctl `--wait` fail-closed + `--all` 金丝雀。内审 30 条全处置；外审首轮 Fail（1 Blocker 6 findings）逐条修复，复审 2 Major 4 Minor 由外审者直接修复、主进程审查通过，最终 Pass。教训入档：管道 `\| tail` 吃退出码曾制造一轮假全绿 | [plan](upgrade-safety-plan.md) · [内审](upgrade-safety-review.md) · [外审](upgrade-safety-external-review.md) · [复审](upgrade-safety-external-rereview.md) · [终审 Pass](upgrade-safety-external-final-review.md) |
