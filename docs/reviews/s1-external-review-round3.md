# Pass — S1 用户平面核心旅程外部复审（round 3）

Date: 2026-07-11

本轮按 round 2 指定的窄复审条件，只审查 R2-F1/F2/F3 的整改及其回复。未发现新的 Blocker/Major/Minor；
round 2 的 **1 Major / 2 Minor 全部闭合**，S1 可放行进入提交前收尾。

## Closure matrix

| Round-2 finding | 结论 | 独立复核 |
|---|---|---|
| R2-F1：plan 残留 login snapshot 语义 | Closed | `s1-plan.md` 的 J-G.3c 已改为“重鉴权后首个、单次、非 polling node read 读取 broker 当前态”，并明确 login 本身无 snapshot 语义；与 drill、README、inventory 一致。 |
| R2-F2：`pty-run.py --` traceback | Closed | parser 同时拒绝缺 `--` 与 `--` 后无命令；独立负控三种非法形式均 rc=2、无 traceback，正常命令 rc=0。round-1 回复保留原记录并追加订正，未掩盖历史误测。 |
| R2-F3：不可定位 provenance 被称为可复核 | Closed | `s1-review.md` 诚实区分“已入库的 13 条原始产出可复核”与“runId/model 等元数据仅转录、当前不可独立再验”；新增 raw-output 文件可直接清点为 6 reviewer + 6 verifier + 1 synth。 |

## Independent verification

通过：

- `python3 test/simcluster/image/pty-run.py --` → rc=2，`no command after --`，无 traceback。
- 无参数 → rc=2；带 step 但无 delimiter → rc=2；正常 `-- /bin/sh -c 'echo hi'` → rc=0、输出 `hi`。
- 两份 Python asset 语法编译通过。
- `go test ./cmd/tether -run TestCommandTreeInventory -count=1` 通过。
- raw outputs 独立计数：13 sections = reviewer 6 / verifier 6 / synth 1。
- 本轮涉及文件定向 trailing-whitespace 扫描为 0。

未重跑 `make e2e` 或 simcluster：本轮只改 plan/provenance 文档与 pty parser 的 no-command authoring-error
分支；round 2 已明确指定本轮只需静态核对、pty 负控、command-tree focused test 和 cached diff-check。60/61/62
drill 行为没有变化，round 2 从当前 S1 实现重建后的 60=38/38、61=41/41 证据仍有效。

## Residual note

workflow 的 runId/model 元数据仍无法从已损坏会话独立重放；当前文档已明确标为“转录、非独立可再验”，不再
构成虚假 provenance 声称。入库的 13 条产出足以复核 finding 内容与 6+6+1 结构。

## Release recommendation

**放行 S1。** 外审 finding 已全部闭合；保持产品代码零 diff 边界，完成 commit/push 前按项目流程填入实际
commit 标识即可。
