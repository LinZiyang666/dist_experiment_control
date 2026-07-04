# 文档三拆一 + 文件挪动 · 外审报告

> 审查基线：`git show HEAD:docs/usage.md`（2588 行）。审查对象：`docs/usage.md`、
> `docs/broker-ops.md`、`docs/cluster.md`、四份 `docs/v2-*.md` 搬移及其全仓指针。
> 按项目约定，本报告只给 finding，不修改被审文件或代码。
>
> **最新结论：PASS（见 §9 外审二次复审）。**

## 1. 结论

**FAIL — 整改后复审。**

> 本节为首轮外审结论；执行者整改后的复审结论见 §7。

正文内容与风险警告保全通过，三册页内锚点和主体导航也通过；当前不通过来自两项执行问题：

1. §7.7 的读者边界放错册；
2. `docs/v2-*.md` 搬移后仍有一批全仓旧路径未同步。

Finding 合计：**0 BLOCKER / 2 MAJOR / 2 MINOR / 1 NIT**。

`log.md` 删除已由项目 owner 明确裁定为不在本次审查范围且应删除，本报告不对此提出 finding。

## 2. Findings

### MAJOR-1：`v2-*.md` 搬移后的全仓旧路径没有闭合

四个文件本体搬移是字节一致的（旧路径 blob 与 `docs/reviews/` 新文件 SHA-256 逐一相同），但全仓仍有
**13 处**指向已删除 `docs/v2-*.md` 的路径，分布在 6 个文件中。例如：

- `docs/reviews/v2-usability-proposals-gap.md:3`
- `docs/reviews/c8-plan.md:3,11,58`
- `docs/reviews/c7-review.md:41,108`
- `docs/reviews/quality-audit/c-mega-audit.md:71,95,150,165,167`
- `docs/reviews/c-overseer-1.md:28`
- `docs/reviews/c-external-review.md:118`

这不是可接受的历史漂移：这些路径正是被本次搬移变成悬空的，且新位置明确存在。应机械改为
`docs/reviews/v2-*.md`；其中同目录文档也可使用同级文件名，但全仓应采用一致规则。整改后以下扫描应为 0：

```text
docs/v2-(automation-program|cli-consolidation-proposal|usability-proposals-gap|usability-proposals).md
```

### MAJOR-2：§7.7 应移回 `usage.md`

`broker-ops.md:514-560` 的 §7.7 全节讲的是 agent 的 `HOME`/`PATH`/网络挂载、
`agent.yaml remote_fs`、以及普通使用者执行 `tether exec/run --safe` 时的自救与降级注意；没有 broker
部署、broker 配置或 broker 主机操作。相反，`usage.md` 已在 §3.2、§5.12、§5.13 三处暴露这些配置和
flag，却把完整故障处置指向 broker 运维册。这会让最可能遇到故障的 ctl 使用者和实验机管理员查错册。

外审批板：**把 §7.7 整节移回 `usage.md`，保留原编号**。同步两册目录、§7 跳号说明和所有
`broker-ops.md §7.7` 指针；`broker-ops.md` 如需引导，只保留不占用 §7.7 标题的跨册提示。

### MINOR-1：`usage.md` 有一处拆分后变得歧义的裸 § 引用

`usage.md:339` 的“再用 §1 join 流程长回去”实际指 `cluster-runbook.md §1`，但在使用者册中裸写
`§1` 会解析成当前册“总体心智模型”。应显式写成 `cluster-runbook.md §1` 并尽量使用链接。

### MINOR-2：内审报告的章节计数与仓库事实不符

内审报告多处声称旧文档有“87 个 numbered `###` 子节”。独立扫描结果是：

- 旧文档 `^### ` 共 72 个，其中 numbered `###` 为 **70** 个；
- 新三册 numbered `###` 合计仍为 **70** 个，编号集合相同、无重复、无缺失；
- 新三册全部 `###` 合计 71 个，因为旧 `### broker 主机` 被提升为 `## 附录 A`。

因此“1:1 落位”的实质结论成立，但“87”及据此描述的方法不可复现。应修正
`docs/reviews/docs-split-review.md` 的计数与方法说明，避免把错误审计数字带入最终记录。

### NIT-1：工作树遗留内审临时产物

`.wf_tmp/digest.txt`、`.wf_tmp/findings.json` 仍为未跟踪文件，且不在交付清单。提交前删除或明确忽略，
不要随本次文档变更入库。

## 3. 独立核验结果

### 3.1 内容保全与风险提示：PASS

- 70 个 numbered `###` 编号在旧文档与新三册间集合完全相同，均只出现一次；落点为
  `usage.md` 50 个、`broker-ops.md` 17 个、`cluster.md` 3 个。
- 逐节去空行比较：56 节字节相同；14 节有差异，逐项均为跨册指针补全或节尾分隔线，没有技术语义、
  命令、参数、阈值或告警被删改。
- 对旧文档与三册合并文本做非空行 multiset 差分，旧文档只有 32 个非空行未原样出现；逐行检查后全部是
  旧总标题/旧目录/父标题，或已被显式册名替代的裸 § 引用。未发现正文或风险提示丢失。
- 风险词计数未下降：`⚠` 1→1、`不可逆` 4→4、`破坏性` 5→6、`危险` 1→1、
  `绝对不要` 1→1、`永远不要` 1→1、`必须` 53→53、`kill -9` 1→1。
- 抽验通过：session 删除不可逆、Tier-2 quorum 操作拒绝 `--yes`、force-single 无完整性保证、
  admin socket 禁止外放、NFS D 状态不可杀、`remote_fs` 降级前删配置块、proto 跨版本必须重装等提示
  均保留，未弱化。

### 3.2 页内锚点与相对链接：PASS

按 fenced code 之外的标题生成 GitHub 风格 slug，并逐一核对页内目录：

| 文档 | 页内目录引用 | 死锚 |
|---|---:|---:|
| `usage.md` | 13 | 0 |
| `broker-ops.md` | 8 | 0 |
| `cluster.md` | 4 | 0 |

三册的相对 Markdown 文件链接分别为 21 / 8 / 8 个，目标文件全部存在。内审关于“三册页内锚点 0
死锚”的结论可复现。MAJOR-1 是代码 span/文字路径层面的全仓悬空，不会被该页内锚点脚本捕获。

### 3.3 结构导航：PASS（待 §7.7 搬回后重跑）

- `usage.md` 对缺失 §7、附录 B 有明确去向；§2/§3/§5/§8/§9 的跨册缺号在目录或正文签路。
- `broker-ops.md` 的章节父标题、目录和附录 A/B 一致。
- `cluster.md` 的目录与三个保留编号标题一致，且明确区分命令/概念与 runbook。
- 三册共用原始编号的意图已在册首说明，当前跳号不会被误判为漏章。

### 3.4 其它挪动：PASS

- 四份 `v2-*.md` 新旧内容逐文件 SHA-256 相同；问题仅为 MAJOR-1 的引用同步。
- `.gitignore` 精确命中 `/docs/devices-ops.local.md`，未扩大为目录级或通配忽略。
- `git diff --check` 通过。

## 4. 三个开放问题的外审批板

1. **`log.md`：按 owner 最新指示删除，不归档，不在本次审查范围。**
2. **`requirements.md` 的 `logs/log.md`：接受 accepted-residual。** 该引用在本次改动前已不存在，
   `requirements.md` 又明确标记为冻结历史；本次不改。内审报告应继续明确记录它，因此不能把结论表述为
   “全仓绝对 0 悬空”，只能表述为“本次新增悬空 0，另有已登记历史残余”。
3. **§7.7：拉回 `usage.md`。** 理由见 MAJOR-2。

## 5. 复审通过条件

- MAJOR-1 的 13 处旧 `docs/v2-*.md` 路径全部改到新位置；
- §7.7 搬回 `usage.md`，目录、跳号说明和跨册指针同步，三册锚点复跑仍为 0 死锚；
- `usage.md:339` 的裸 `§1` 消歧；
- 内审报告把 numbered `###` 计数改为可复现的 70；
- `.wf_tmp/` 不进入提交。

## 6. 主进程整改回复（逐条）

全部 finding 采纳并整改。未 `git add`（暂存留给复审）。

- **MAJOR-1（v2 旧路径）——已修**：采纳外审判断（这些是被本次搬移新变悬空的、新位置存在，非可接受历史漂移；推翻我内审时"冻结历史不改"的误判）。机械改 `docs/v2-*.md` → `docs/reviews/v2-*.md`，共 **13 处 / 6 文件**：`c-overseer-1.md`×1、`c7-review.md`×2、`c-external-review.md`×1、`v2-usability-proposals-gap.md`×1、`quality-audit/c-mega-audit.md`×5、`c8-plan.md`×3。**复核**：`grep 'docs/v2-(automation-program|cli-consolidation-proposal|usability-proposals-gap|usability-proposals).md'` = **0**。
- **MAJOR-2（§7.7 拉回 usage）——已修**：§7.7 整节移回 `usage.md`、**保留原编号**；在 usage 新增 `## 7. 运维 / 维护`（仅含 §7.7）+ 一条指向 broker-ops §7.1–7.6 的说明。同步：usage 目录加 §7 条目、册首"没有 §7"改为"§7 只含 §7.7"、`remote_fs.mode` 表指针 `broker-ops.md §7.7`→`§7.7`；broker-ops 删除 §7.7 标题+正文、原处留**不占 §7.7 标题**的跨册提示、目录 §7 行去掉 7.7 并加指向 usage 的提示。§7.7 正文**字节保留**，仅把内部自引 `见 usage.md §2 安装`→`见 §2 安装`。**复核**：三册锚点复跑 `usage 14 / broker-ops 8 / cluster 4`，**0 死锚**；`broker-ops.md §7.7` 指针残留 = 0；`### 7.7` 仅存在于 usage。
- **MINOR-1（`usage.md:339` 裸 `§1`）——已修**：`再用 §1 join 流程` → `再用 cluster-runbook.md §1 join 流程`。
- **MINOR-2（计数 87→70）——已修**：更正 `docs-split-review.md` 为可复现的 **70**（旧 numbered `###`=70；新三册合计 70、编号集合相同、无重复无缺失；旧 `### broker 主机` 提升为 `## 附录 A` 故新三册 `###` 总数 71）。**独立复核**：OLD=70、NEW 合计=70、集合相等、0 重复；§7.7 搬回后落点 `usage 51 / broker-ops 16 / cluster 3`。已同步删除报告中"全仓绝对 0 悬空"式表述，改为"本次新增悬空 0，另有已登记历史残余"。
- **NIT-1（`.wf_tmp/`）——已修**：删除该目录（`digest.txt` 空、`findings.json` = `[]`，确认为空 scratch）；未入暂存。
- **三个开放问题**：全部采纳外审批板——① `log.md` 删除不归档（保持现状）；② `requirements.md` 的 `logs/log.md` 接受 accepted-residual、结论措辞已收紧；③ §7.7 拉回 usage（见 MAJOR-2）。内审报告 §7 已更新为"外审已裁定"。

**复审通过条件自检**：5 条全部满足（v2 scan=0 / §7.7 已搬且锚点 0 死锚 / `§1` 已消歧 / 计数=70 / `.wf_tmp` 已删）。请复审。

## 7. 外审复审（2026-07-04）

**RE-FAIL — 仍需一次小修后复审。**

执行者对首轮 5 条复审通过条件中的 4 条闭合，且 §7.7 读者边界修正到位；但 MAJOR-1 仍有 1
处同类残留：精确旧路径的 13 处已清零，宽松全仓扫描仍命中一个 `docs/v2-{...}.md` 花括号写法：

- `docs/reviews/c-overseer-3.md:3`：
  `docs/v2-{usability-proposals,usability-proposals-gap,automation-program,cli-consolidation-proposal}.md`

这不是新的设计问题，只是 `docs/v2-*.md` 搬移后的旧路径指针漏改。应机械改成
`docs/reviews/v2-{usability-proposals,usability-proposals-gap,automation-program,cli-consolidation-proposal}.md`
（或展开为 4 个明确链接）。改完后，排除本拆分报告自身描述性文本，以下扫描应为 0：

```text
rg -n "docs/v2-" . --glob '!docs/reviews/docs-split-review.md' --glob '!docs/reviews/docs-split-external-review.md'
```

已复核通过项：

- §7.7 已整节移回 `usage.md`，`### 7.7` 仅在 `docs/usage.md:1317` 出现；`broker-ops.md`
  只保留不占编号的跨册提示。
- 旧 / 新 numbered `###` 均为 70，编号集合相同、无重复无缺失；落点为 usage 51 / broker-ops 16 /
  cluster 3。
- §7.7 正文与旧版一致；仅允许的自引用 `见 usage.md §2 安装` → `见 §2 安装` 不改变语义。
- 风险词计数不减：`⚠` 1→1、`不可逆` 4→4、`破坏性` 5→6、`危险` 1→1、`kill -9` 1→1。
- 三册页内锚点复跑 0 死锚：usage 14/14、broker-ops 8/8、cluster 4/4；相对 `.md` 链接目标也全部存在。
- `usage.md:340` 的裸 `§1` 已消歧为 `cluster-runbook.md §1`。
- `docs-split-review.md` 的 numbered `###` 计数已改为可复现的 70；残留的 “87” 只作为原错误数字的纠正说明出现。
- `.wf_tmp/` 已不存在；`git diff --check` 通过。

本轮只需修上面 1 处 `docs/v2-{...}.md` 残留；修复后无需重新争议 §7.7 或内容保全，可做窄复审。

## 8. 主进程二次整改回复（RE-FAIL → 已修）

- **RE-FAIL 唯一项（`c-overseer-3.md:3` 花括号写法）——已修**：`docs/v2-{usability-proposals,usability-proposals-gap,automation-program,cli-consolidation-proposal}.md` → `docs/reviews/v2-{...}.md`。根因：上轮我用**精确文件名正则**扫描，漏掉了 brace-expansion 写法（`docs/v2-{...}` 而非 `docs/v2-<name>`）；本轮改用**宽松前缀** `docs/v2-` 全仓扫描，确认此为唯一残留。
- **复核（外审指定扫描）**：`grep -rn 'docs/v2-' . --include=*.{md,go,sh}`（排除本报告 + `docs-split-review.md` 描述性文本）= **0**；精确文件名扫描亦 = 0。
- 未 `git add`；未触碰 §7.7、内容保全或其它已 PASS 项。请做窄复审。

## 9. 外审二次复审（2026-07-04）

**PASS — 本次文档三拆一 + 文件挪动外审通过。**

上轮唯一未闭合项已修复：

- 排除本拆分报告自身描述性文本后，`rg -n "docs/v2-" . --glob '!docs/reviews/docs-split-review.md' --glob '!docs/reviews/docs-split-external-review.md'`
  无命中。
- `docs/reviews/c-overseer-3.md:3` 已改为 `docs/reviews/v2-{usability-proposals,usability-proposals-gap,automation-program,cli-consolidation-proposal}.md`。

同时复核此前 PASS 项未回退：

- `git diff --check` 通过。
- 旧 / 新 numbered `###` 均为 70，编号集合相同、无重复、无缺失；落点为 usage 51 / broker-ops 16 / cluster 3。
- `### 7.7` 仅在 `docs/usage.md:1317`；`broker-ops.md` 未重新占用 §7.7。
- 三册页内锚点仍为 0 死锚：usage 14/14、broker-ops 8/8、cluster 4/4。
- 三册相对 `.md` 链接目标全部存在：usage 23、broker-ops 10、cluster 8。

最终裁定：

- 内容保全、安全 / 破坏性 / 不可逆提示保全：通过。
- 读者切分边界：通过（§7.7 留在 `usage.md`）。
- 跨册 / 全仓指针：通过（`requirements.md` 的 `logs/log.md` 维持 accepted-residual；`log.md` 删除不纳入审查 finding）。
- 结构导航：通过。

可进入提交前整理；本外审未执行 `git add`。
