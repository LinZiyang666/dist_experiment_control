PASS

# P14 Transfer Unrestrict External Review

Date: 2026-06-11
Reviewer role: external reviewer

## Verdict

当前结论为 **PASS / 放行**。F11 的 fail-open 多文档绕过已正确修复并通过
针对性、黑盒和全量回归；未发现剩余 High/Medium blocker。本结论覆盖下文
较早轮次的 PASS/BLOCKED。

原提交不能直接放行。全量审查确认 4 项 High、5 项 Medium 和 1 项 Low
正确性/安全问题；reviewer 已直接修复并增加独立回归。修复后未发现剩余
High/Medium blocker，P14 代码可放行。

## Review Checklist

- 架构、需求、权限边界、wire/proto、Object Store 生命周期：完成
- 配置三态、升级行为、安装默认值、失效保护：完成
- 路径规范化、软链、父目录/叶子 TOCTOU、覆盖语义：完成
- Tier A/B 尺寸、SHA、内存/磁盘上限、并发与缓存：完成
- CLI 本地落盘、事件顺序、错误收敛：完成
- 单元、集成、auth_callout、e2e matrix、race、跨平台构建：完成

## Findings And Direct Fixes

### F1 - High: 未知 YAML 字段会静默回落到全盘开放

`yaml.Unmarshal` 忽略 `allow_root`、`file_transfers` 等拼写错误。P14 默认
开放后，这不再只是“配置不生效”，而是 fail-open。

已改为 `KnownFields(true)`，并拒绝多 YAML document。未知顶层、嵌套字段和
第二 document 均有回归测试。

### F2 - High: Tier-B push 在 SHA 校验前已覆盖目标

原 `objectStoreGetAndWrite` 写 tmp 后立即 rename，调用层随后才比较 SHA。
错误对象会先落盘，再上报 `sha_mismatch`。

已改为：严格读取 prepare 声明的字节数，完成 size + SHA 校验后才提交 tmp。
wrong-SHA / wrong-size + `force=true` 回归均证明原目标保持不变。

### F3 - High: Tier-B 对象未按 prepare size 限制

成员可用很小的 `req.Size` 通过 broker，再向 Object Store 放更大对象；agent
原先无界复制到本地，绕过单传输 2 GiB 契约。

已增加 broker 非负/上限校验、agent 精确 size 限制、pull 双端流式上限和
`size_mismatch` 收敛。

### F4 - High: `O_NOFOLLOW` 只保护叶子，父目录仍可竞态重定向

验证后再次按完整字符串路径 open/rename，敌对本机进程可把父目录换成软链。

已将 canonical parent 逐级 `openat(O_DIRECTORY|O_NOFOLLOW)`，读写和清理均
相对固定的目录 fd 完成；write commit 前还会复核 parent dev/inode 仍对应原
路径。确定性 early/late parent-swap 回归均返回 `path_race`，外部目录不产生
文件。

### F5 - Medium: `--force=false` 存在检查后覆盖竞态

agent 和 ctl 都是 `Stat/Lstat` 后 `Rename`；并发创建目标可在检查后被覆盖。

已改为原子 create-if-absent 提交；并发双写测试稳定为一方成功、一方
`dst_exists`。`--force` 仍使用原子 rename-overwrite。

### F6 - Medium: 文件增长可突破 Tier A 内存和 2 GiB 上限

原逻辑只在读取前 stat，之后 `ReadAll` / Object Store Put 不设读上限。

已在 ctl push、agent pull、ctl pull 的实际 reader 上施加 `limit+1`，并校验
实际上传/下载字节数。

### F7 - Medium: CLI 在 commit 后才订阅完成事件

快速 agent 可在订阅建立前发布失败事件，CLI 随后把失败降级为
“commit acked”。

已改为 commit 前订阅并 flush。

### F8 - Medium: agent push prepare cache 无界且不过期

客户端 prepare 后不 commit 会永久留下 entry。broker watchdog 只清 broker，
不清 agent。

已加入 6 分钟过期清理和 1024 entry 上限，与 broker in-flight 上限对齐。

### F9 - Medium: `allow_roots: ["/"]` 拒绝所有路径

字符串拼接把根前缀变成 `//`。已改用 `filepath.Rel` 做结构化 containment；
`/` 现在作为普通非空 root 正确覆盖全文件系统。

### F10 - Low: Tier-A broker 未钉 8 MiB 协议上限

直接 NATS 调用可绕过 CLI tier chooser。已增加 broker 端 8 MiB 硬限制。

## Questions And Recommendations

1. `allow_roots: null` 与缺省一样表示 Open，这是已锁定设计；发布说明必须继续
   明示，避免操作员把 null 误认为关闭。
2. no-force 的 portable 原子实现依赖 hard-link 支持；Linux/APFS/ext4 正常，
   不支持 hard-link 的挂载会 fail closed。若以后正式支持 FAT/对象挂载，可增加
   Linux `renameat2(RENAME_NOREPLACE)` / Darwin `renameatx_np(RENAME_EXCL)` 分支。
3. 无 wire 变更，不要求重装；但这是默认姿态翻转，建议 release 前至少做一次
   真实旧配置升级验证：缺省配置应 Open，显式 `allow_roots: []` 应保持 Disabled。

## Verification

- `go test ./... -count=1`: PASS
- affected packages + CLI E2E under `-race`: PASS
- new reviewer regressions: PASS, `-count=10`
- Tier-B verify-before-commit regression: PASS, `-count=10`
- full `-tags e2e_matrix` matrix: PASS, 81.7s
- Darwin + linux/amd64 `CGO_ENABLED=0 go build ./...`: PASS
- `go vet ./...`: PASS
- `golangci-lint run`: PASS, 0 issues
- `go mod tidy -diff`: PASS, no diff
- `git diff --check` (staged + unstaged): PASS

Reviewer 的实现、测试和本报告保持未暂存，便于与执行者原暂存内容直接对比。

---

## 主进程回复（main-process response, 2026-06-11）

逐条评估如下；全量 gate 已复跑通过（`go test ./...` exit 0、`make e2e` 含
`TestTransferDefaultsMatrix` 81s、golangci-lint 0 issues、`go test -race ./internal/agent/
./cmd/tether/`、gofmt、go vet、darwin + `GOOS=linux` build）。

**F1–F10：全部采纳，已逐一阅读实现 + 配套回归测试，正确无误。**
- **F1（严格 YAML）采纳，但其 `KnownFields`/multi-doc 严格化引入了 2 处过度拒绝，我已在
  `cmd/tether/agent.go::loadAgentYAML` 收口**：① 空文件 / 纯注释 / 纯空白配置原被 `decode: EOF`
  拒、导致 agent 起不来——恢复容忍（零结构 → 回落 CLI flag，匹配旧 `yaml.Unmarshal`）；
  ② 结尾一行 `---`（无害空第二文档）被误判 multi-doc——改为仅拒**非空**第二文档。F1 的安全
  意图（拒未知字段 + 拒真·隐藏第二文档）完整保留，`TestLoadAgentYAMLRejectsUnknownFields`
  仍全过，新增 `TestLoadAgentYAML_ToleratesEmptyAndTrailingSeparator` 钉住容忍面。详见
  `transfer-unrestrict-review.md` 的 external review (round 3) 段。
- F2–F10：无需改动，实现与回归均正确（verify-before-commit、size 限制、openat 父目录钉定 +
  parent dev/inode 复核、linkat 原子无覆盖、流式 limit+1、commit 前订阅、push cache TTL+上限、
  `filepath.Rel` containment、broker 端 8 MiB 硬限）。

**Questions / Recommendations：**
1. **`allow_roots: null` == 缺省 == Open**：已锁定设计，采纳。`usage.md` 的「升级说明
   (v0.4.0)」对照表已明示「无 `file_transfer` / `allow_roots` 键 → 开放」；发布说明会沿用此措辞。
2. **no-force 原子依赖 hard-link**：采纳为已知约束。不支持 hard-link 的挂载 **fail-closed**
   （`link` 报错 → 操作拒绝、不覆盖），对 v0.4.0 可接受。`renameat2(RENAME_NOREPLACE)` /
   `renameatx_np(RENAME_EXCL)` 分支列为**后续增强**，非本次 blocker。
3. **发布前真实旧配置升级验证**：采纳，列为 **release 前置动作**（非代码 blocker），对应
   plan §Open-questions #3。建议在 pc732 真 broker 上各跑一次：缺省配置 → Open 可 push/pull；
   显式 `allow_roots: []` → 保持 Disabled（`transfer_disabled`）。打 v0.4.0 tag 前完成并记入
   `log.md`。

**结论**：外审 PASS 已确认；主进程对 F1 的 2 处回归收口完成、其余无异议。代码可进入收尾
（commit/push）；建议项 #3 在打 tag 前执行。

---

## 执行者回复后复审（2026-06-11）

### 结论

**BLOCKED / 暂不放行。**

发现 1 项 High 阻断问题。除该问题外，本轮未发现新的重大问题，已有全量门禁
均通过。

### F11 - High: 空中间 YAML document 可绕过多文档拒绝并回落为 Open

`cmd/tether/agent.go::loadAgentYAML` 在完成主文档解码后，只调用一次
`dec.Decode(&extra)`。当第二 document 为空时，`extra == nil`，函数随后直接
返回，不再确认输入是否已经到达 `io.EOF`。

因此以下配置会被接受：

```yaml
session: lab
nid: a100
---
# empty document
---
file_transfer:
  allow_roots: []
```

第三 document 中的显式禁用配置被完全忽略，`AllowRoots` 保持 nil，
`RootsConfigured` 变为 false，Agent 最终按 whole-filesystem Open 模式启动。
这是 F1 所防范的同类 fail-open，只是借助空中间 document 绕过。

本轮已用临时 `TETHER_HOME` 黑盒复现：CLI 未报告 YAML 错误，并输出
`file transfer: whole-filesystem reach` 后进入 NATS 连接重试。

现有测试仅覆盖“非空第二 document 应拒绝”和“单个空尾 document 应允许”，
未覆盖“空第二 document + 非空第三 document”。

### 建议修改

额外 document 检查应循环解码直至 `io.EOF`：

- 允许任意数量的空尾 document；
- 任一额外 document 非空时立即返回 multiple-documents 错误；
- 解码错误立即返回；
- 增加“空第二 document + 非空第三 document”回归测试，并可补充多个空尾
  document 的容忍测试。

### Verification

- `go test ./...`: PASS
- `go test -race ./internal/agent ./cmd/tether`: PASS
- `make e2e`: PASS
- `go vet ./...`: PASS
- `go build ./...`: PASS
- `golangci-lint run`: PASS, 0 issues
- staged + unstaged `git diff --check`: PASS
- F11 临时配置黑盒复现: FAIL as expected，确认发生 fail-open

本轮 reviewer 未修改任何代码，也未暂存文件。

---

## 主进程回复 — F11 收口（2026-06-11）

**F11 采纳，确认是我引入的 fail-open，已修复。** 根因：上一轮我为收口空文件/尾随
`---` 的过度拒绝，把额外文档检查写成**只解码一次** `dec.Decode(&extra)`；当第二文档为空
（`extra == nil`）时直接 return，没有继续扫到 `io.EOF`，于是空中间文档之后的非空文档
（含显式 `allow_roots: []`）被整段忽略 → `RootsConfigured` 退回 false → whole-FS Open。

**修复**（`cmd/tether/agent.go::loadAgentYAML`）：额外文档检查改为**循环解码直至
`io.EOF`**——
- 任意数量的**空**尾/中间文档：容忍；
- 任一**非空**额外文档：立即返回 `multiple YAML documents are not supported`；
- 解码错误：立即返回。

reviewer 给出的精确绕过配置（doc1 + 空 doc2 + 非空 doc3 `allow_roots: []`）现在直接报错、
agent **拒绝启动**（fail-closed），不再 fall-open。空文件 / 纯注释 / 单个或多个空尾 `---`
仍正常容忍。

**回归**：
- `TestLoadAgentYAMLRejectsUnknownFields` 新增用例
  `empty middle document cannot hide a later narrowing`（= reviewer 的绕过配置）→ 必须报错。
- `TestLoadAgentYAML_ToleratesEmptyAndTrailingSeparator` 新增 `multiple empty trailing docs`
  容忍用例。

**复跑 gate（全绿）**：`go test ./...` exit 0 · `go test -race ./cmd/tether/ ./internal/agent/`
· `make e2e`（e2e_matrix）81s · golangci-lint 0 issues · go vet · gofmt · darwin +
`GOOS=linux CGO_ENABLED=0` build。

请 reviewer 复审 F11 收口；其余 F1–F10 维持上轮结论不变。在 reviewer 将本报告判回 PASS
之前，主进程不 commit/push。

---

## F11 修复后最终复审（2026-06-11）

### 结论

**PASS / 放行。**

F11 修复正确：`loadAgentYAML` 现在循环解码所有额外 document 直至
`io.EOF`，空 document 只会继续扫描，任一非空额外 document 都会立即拒绝。
原绕过配置不再启动 Agent，不会回落为 whole-filesystem Open。

新增回归同时覆盖：

- 空第二 document 后隐藏非空第三 document：拒绝；
- 单个或多个空尾 document：容忍；
- 未知顶层/嵌套字段和普通非空第二 document：继续拒绝。

复审执行者本轮实际差异后，未发现新的 High/Medium 问题。F1-F11 均已收口，
P14 可以进入 commit/push。真实旧配置升级验证仍是打 v0.4.0 tag 前的发布
动作，不是代码放行 blocker。

### Final Verification

- F11 原始绕过配置黑盒复验：PASS，立即返回
  `multiple YAML documents are not supported`
- `go test ./cmd/tether -run TestLoadAgentYAML -count=50`: PASS
- `go test ./...`: PASS
- `go test -race ./internal/agent ./cmd/tether`: PASS
- `make e2e`: PASS，包含 `TestTransferDefaultsMatrix`
- `go vet ./...`: PASS
- Darwin `go build ./...`: PASS
- Linux/amd64 `CGO_ENABLED=0 go build ./...`: PASS
- `golangci-lint run`: PASS，0 issues
- gofmt 与 staged/unstaged `git diff --check`: PASS

本轮 reviewer 未修改任何代码，也未暂存文件；仅更新本审查报告。
