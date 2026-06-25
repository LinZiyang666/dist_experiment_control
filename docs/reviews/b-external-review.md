# Fail - B program external review

结论：Fail。B1-B7 这批变更还不能上线。主要阻断项是恢复失败保护顺序错误、根目录暂存了不可审查的本地二进制、在线备份可从滞后 follower 产生“成功但陈旧”的备份，以及若干运维命令会给自动化返回错误的成功/失败语义。

## Scope

- 用户要求审查“暂存区外内容”。我先核对了 `git diff` 和 `git status --short`：严格未暂存 diff 为空，只有我本轮新增的外审测试/报告尚未暂存。
- 因此本报告把当前 index 中 B1-B7 usability program 作为真实上线候选审查面：`git diff --cached --stat` 显示 162 files changed, 13554 insertions(+), 466 deletions(-)，包含 `cmd/tether`、`internal/broker`、`internal/clusteroffline`、`internal/clusterroster`、`internal/clusterspec`、文档和一个根目录 `tether` 二进制。
- 我没有信任既有内部报告；只把 `CLAUDE.md`、需求/架构文档和 `docs/reviews/*` 用作流程与风格参考。

## Tasklist

- [x] 建立审查范围：未暂存/已暂存差异、B1-B7 文件面、二进制/文档/测试变更。
- [x] 复读 `CLAUDE.md`、需求/架构文档、既有 `docs/reviews` 外审报告风格。
- [x] 审查 B1-B2：CLI/JSON/exit code/status card/机器契约。
- [x] 审查 B3-B4：拓扑、NATS 配置、expose rebuild、alert/webhook。
- [x] 审查 B5：join/apply/sign-join、quorum 与 capacity guard。
- [x] 审查 B6：备份、恢复、事故导出、诊断与 DR 失败模式。
- [x] 审查 B7：readyz、ops、roster 与运维闭环。
- [x] 跨切面审查：安全、数据一致性、崩溃恢复、文档真实性、构建产物。
- [x] 执行独立验证：focused tests、build、diff check、外审回归测试。
- [x] 形成本外审报告，并准备暂存所有文件。

## Findings

### F1 - Blocker - `restore_in_progress` fail-closed check happens after raft runtime can start

`Broker.Run` 在 cluster mode 下先构造 raft runtime，再检查 restored DB 的 `restore_in_progress` 标记：`internal/broker/broker.go:569` 调用 `buildClusterRuntime`，`internal/broker/broker.go:576` 才调用 `assertClusterDBConsistent`。而 `buildClusterRuntime` 会进入 `cluster.NewProduction`（`internal/broker/cutover.go:170`），`cluster.New` 打开 WAL DB、raft store/snapshot，并在 `internal/cluster/node.go:191` 调用 `raft.NewRaft`。

`assertClusterDBConsistent` 的注释明确说 `restore_in_progress` 是为了防止“restored DB + OLD raft log resurrect pruned peers”（`internal/broker/cutover.go:98`）。但当前顺序允许旧 raft log/FSM 在拒绝启动前接触 restored DB。也就是说，失败保护本身晚于它要防的危险。

建议：在任何 raft store/FSM/transport 构造前，用只读 DB 先检查 `restore_in_progress`。可以把该检查放在 `buildClusterRuntime` 调 `cluster.NewProduction` 之前，或在 `Run` 中先对 `cfg.DBPath` 做 preflight。需要新增崩溃恢复回归：带旧 raft log + restored DB marker 的启动不得调用 raft/FSM apply。

### F2 - Blocker - 根目录暂存了不可审查且不符合发布约束的 `tether` 二进制

当前 index 新增根目录 `tether`，大小 24M，mode `100755`。`file tether` 结果是动态链接 ELF，带 debug info，not stripped。`CLAUDE.md` 要求本项目 Go-only，二进制发布应为 `CGO_ENABLED=0` 静态构建；源码提交里也不应夹带本地构建产物。

我用当前源码运行 `CGO_ENABLED=0 go build -o /tmp/tether-review ./cmd/tether` 可以产出 statically linked ELF，说明这是误把本地 artifact 暂存，而不是源码无法静态构建。

建议：从 index 和工作区移除根目录 `tether`，把 `/tether` 加入 `.gitignore` 或 CI/pre-commit guard；发布产物走独立 release/dist 流程。

### F3 - High - 在线 backup 可由任意 follower 返回完整成功，但没有新鲜度/leader barrier

`HandleCluster` 明确把 `OpClusterBackup` 放在 leader gate 之前，注释写“ANY node serves it”（`internal/broker/clusterstatus.go:468`）。`handleBackup` 直接从本地 `node.BackupDBTo` 复制本机 RO DB 并返回 OK（`internal/broker/clusterbackup.go:44`）。

如果 operator 对一个滞后 follower 或分区 follower 执行 `cluster backup`，会得到结构完整、manifest 正常、exit 0 的 bundle，但它可能缺少 leader 已提交状态。DR 工具和 runbook 很容易把它理解成“集群备份成功”，而不是“某个节点本地视图的陈旧快照”。

建议：默认只允许 leader 执行 online backup；或者在 follower 上先做 `VerifyLeaderRead`/caught-up barrier，并把 `leader_id`、`source_node_id`、`applied_index`、freshness 状态写进 manifest 和 CLI 输出。不满足新鲜度时应非 0 退出。

### F4 - High - `cluster restore` 声称保留当前 DB，但 `.bak` 已存在时会静默跳过备份

restore 覆盖 live DB 前调用 `backupOnce(opts.DBPath)`（`internal/clusteroffline/restore.go:152`）。`backupOnce` 的语义是如果 `<db>.bak` 已存在就直接返回 nil（`internal/clusteroffline/init.go:300`）。CLI 却在恢复前告诉用户“The current DB is preserved at <db>.bak”（`cmd/tether/cluster_backup.go:84`）。

第二次 restore，或曾经执行过 init 的机器上，当前 live DB 会被删除/覆盖，但不会生成新的当前状态备份；提示信息是错的。

建议：restore 使用唯一备份名，例如 `<db>.restore-<timestamp>.bak`，或者发现 `.bak` 已存在就拒绝继续并要求显式覆盖策略。CLI 文案必须与真实保留策略一致。

### F5 - High - restore provenance 没有校验 manifest `applied_index` 与 state.db cursor 一致

`restoreProvenanceGate` 会比较 manifest 与 bundle state.db 的 name、cert fingerprint、raft/nats/tunnel/public host 等字段（`internal/clusteroffline/restore.go:224`），但没有比较 `m.AppliedIndex` 与 `ReadSelfIdentity` 从 state.db 读出的 applied index。这样可以把较新的 manifest 套在较旧的 state.db 上，恢复本身通过，日志/输出却报告较新的 bundle cursor。

建议：把 applied index 加入 manifest/state.db 一致性检查；新增测试：用新 manifest + 旧 state.db 的混合 bundle 必须拒绝。

### F6 - High - `cluster apply` 从非权威/partial status 生成运维计划

`cluster apply` 读取 status 后直接把 `rep.Nodes` 喂给 `clusterspec.Diff`（`cmd/tether/cluster_apply.go:38`），没有检查 `rep.IsLeaderView`、`rep.Partial` 或 `rep.Errors`。这些字段在 status 结构里存在，说明设计已经区分了 leader authoritative view 与 partial/stale view。

失败路径：operator 对 follower/minority/stale socket 运行 apply，得到基于不完整 voter count 或旧 leader 的 drain/add/remove 计划。计划虽然“只打印不执行”，但这是 B 计划的核心 copy-paste 运维面，错误计划本身就是风险。

建议：`cluster apply` 必须拒绝 `!IsLeaderView`、`Partial` 或 `len(Errors)>0` 的 status，并在 JSON/text 中输出可机器识别的拒绝原因。

### F7 - High - `cluster doctor` 在线检查失败后可能 exit 0

auto 模式下，`fetchClusterStatusReport` 失败但 socket 文件存在时，doctor 只向 stderr 打 warning，然后落回 offline pre-init checks（`cmd/tether/cluster_natsconf.go:199`）。`renderDoctor` 只根据 offline checks 的 fatal 数决定退出码（`cmd/tether/cluster_natsconf.go:262`）。

这会让一个 admin socket stale、daemon down、或 mid-restart 的 live 集群在 offline secrets/db checks 通过时返回 exit 0。监控或运维脚本会把“没有完成在线健康检查”误判成“cluster doctor OK”。

建议：socket 存在但在线 RPC 失败时，加入 `online_admin_socket` FATAL check，text/JSON 均保留失败原因并非 0 退出；只有 socket 不存在且用户未显式要求 online 时才可 fallback 到 offline preflight。

### F8 - High - `takeover-natsconf` 可生成语法正确但 mesh 不完整的配置

`takeover-natsconf` 只从 self 和用户手写的 `--peer` 三元组生成 peers（`cmd/tether/cluster_natsconf.go:90`）。少传一个现有 voter 时，`nats-server -t` 仍然会通过，因为它不知道 raft roster 里缺了谁；但 routes、auth users、broker static nkey permissions 会缺项。

建议：支持从当前 leader status/roster 自动渲染完整 peer set，例如 `--from-roster`；如果手写 peers 与 live voter set 不一致，应拒绝而不是生成“valid but incomplete”的 conf。

### F9 - High - `export-incident --out` 会跟随 symlink 并覆盖已有文件

CLI 写 incident bundle 时直接 `os.WriteFile(out, blob, 0o600)`（`cmd/tether/cluster_backup.go:138`）。这会跟随 symlink，也会无提示截断已有文件。事故导出常在 root/service 用户上下文运行；在可写目录中，恶意 symlink 可导致覆盖敏感文件。

建议：使用 `O_CREATE|O_EXCL|O_WRONLY`，拒绝 symlink（可用 `O_NOFOLLOW` 的平台上启用），写完 fsync 文件和目录。默认不应覆盖已有 incident bundle。

### F10 - Major - incident bundle 的“secret-scrubbed”承诺过强

scrubber 只按 audit body 的 key substring denylist 做递归清理（`internal/broker/incident.go:28`、`internal/broker/incident.go:192`）。如果秘密值出现在无害 key（如 `message`、`error`、`args` 的某个普通字段）或非 audit body 的 alert/timeline 字段里，仍会进入“可分享”的 incident bundle。

建议：把文案从绝对 “secret-scrubbed” 降级为“best-effort redaction”，或改成 schema allowlist + value-pattern redaction。至少要有覆盖“innocent key, secret value”的测试。

### F11 - Major - admin `bad_request` 和 restore abort 被映射为 internal error

`error_hints.go` 把 `bad_request` 映射到 `exitInternal` 70（`cmd/tether/error_hints.go:91`），但大量 admin bad_request 是 operator input，例如 backup 目录已存在（`internal/broker/clusterbackup.go:34`）或 malformed request。`cluster restore` typed-confirm abort 返回普通 `fmt.Errorf`（`cmd/tether/cluster_backup.go:91`），`classifyExit` 也会给 70。

我新增了外审回归 `cmd/tether/b_external_review_test.go`，当前稳定失败：

- `TestBExternalReviewBadRequestIsUsage`: got 70, want 64。
- `TestBExternalReviewRestoreAbortIsUsage`: got 70, want 64。

建议：`bad_request` 映射到 usage 64；restore abort 使用 `usageErr` 或专用 typed-confirm usage error。70 应保留给程序 bug/version skew/store corruption 这类真正 internal failure。

### F12 - Major - `cluster apply` 会打印 backend 拒绝执行的 non-voter cleanup 命令

`clusterspec.Diff` 对非 voter retire 输出 `tether cluster remove <id>`（`internal/clusterspec/spec.go:157`），但 backend `RemoveNode` 只允许 `RETIRING` 或 `VOTER_ADD_FAILED`，其他 phase 直接拒绝（`internal/broker/clusterdrain.go:186`）。

一个 stuck `CATCHING_UP` learner 或 roster/raft inconsistent 行会得到不可执行的 apply plan。对运维工具来说，打印一条 backend 必拒绝的命令比显式 REFUSED 更危险。

建议：`Diff` 需要按 phase 生成真实可执行 remediation，或者输出 REFUSED/diagnostic step，并有测试证明所有 command-shaped steps 能被 admin phase gate 接受。

### F13 - Major - offline init/restore 缺少统一 identity validation

`InitFromExisting` 只检查 SelfID/RaftAddr/DBPath/DataDir 非空，以及 raft/tunnel 的 host:port（`internal/clusteroffline/init.go:103`）。它没有调用 `proto.ValidateNID`，也没有统一拒绝控制字符/NUL/非预期文本。restore 还会信任 manifest 里的 self identity 并重新 bootstrap。

建议：offline init、backup manifest 读取、restore provenance gate 都应复用在线路径的 node id/text/address validation。至少 SelfID 必须 `proto.ValidateNID`，所有会被写入 DB 或渲染到命令/JSON 的文本字段要拒绝 NUL 和非法 UTF-8。

### F14 - Medium - signed roster seam 固化了可重放语义

`clusterroster.Verify` 只校验 account pub 与签名（`internal/clusterroster/roster.go:92`）；broker 生成 roster 时 `expires_at` 为空（`internal/broker/cluster_roster.go:57`）；测试还显式要求 expired roster 通过（`internal/clusterroster/roster_test.go:120`）。如果后续 agent 直接消费这个 seam，旧 roster 可无限期重放，把 agent 固定到已退役 broker route。

建议：在消费者上线前引入 `VerifyAt`，拒绝未知 schema version 和过期 `expires_at`；broker 签发 roster 时使用短 TTL，并让 agent 缓存只作为短期 fallback。

## Questions / Concerns

- `cluster restore` 重新 bootstrap 时使用 manifest 的旧 `RaftAddr`（`internal/clusteroffline/restore.go:178`），CLI 没有 `--raft-addr` override。若 DR 目标是“同一节点原地恢复”，这可以成立；若 runbook 允许 fresh host 恢复，则会把新单节点集群 bootstrap 到旧地址。
- Alert webhook URL 当前只做 scheme/userinfo/host 检查，并使用默认 HTTP redirect 行为。源码注释把 URL 定义为 operator-trusted，因此我没有把它列为阻断项；但如果 webhook host 被接管或重定向到 metadata/private endpoint，仍有 SSRF 面。建议至少禁止跨 host redirect，或在文档中明确风险边界。
- `cluster status --remote --json` 是否必须带 `schema_version` 需要产品口径。B2 写的是机器 JSON 稳定契约，但代码中有刻意例外；若这是对外 API，应补 schema discriminator。

## Confirmed Clean / Lower Risk Areas

- `git diff --cached --check` 通过。
- focused pure tests 通过：`internal/clusterroster`、`internal/clusterspec`、`internal/adminsock`、`internal/proto`。
- focused offline DR tests 通过：`go test ./internal/clusteroffline -run 'Test(OfflineBackup|Restore|InitFromExisting|RecoverEmitManifest)'`。
- focused CLI JSON/expose/cluster/machine/sign-join/status-card tests 通过：`go test ./cmd/tether -run 'Test(Json|ExposeJSON|Cluster|Machine|SignJoin|NoActive|Exit|StatusCard|B5)'`。
- `CGO_ENABLED=0 go build -o /tmp/tether-review ./cmd/tether` 成功，输出为 statically linked ELF；Go 只打印了 module stat cache 在只读 cache 下无法写入的 warning。

## Verification

- PASS: `git diff --cached --check`
- PASS: `env GOCACHE=/tmp/tether-go-cache go test ./internal/clusterroster ./internal/clusterspec ./internal/adminsock ./internal/proto`
- PASS: `env GOCACHE=/tmp/tether-go-cache go test ./internal/clusteroffline -run 'Test(OfflineBackup|Restore|InitFromExisting|RecoverEmitManifest)'`
- PASS: `env GOCACHE=/tmp/tether-go-cache go test ./cmd/tether -run 'Test(Json|ExposeJSON|Cluster|Machine|SignJoin|NoActive|Exit|StatusCard|B5)'`
- PASS with warning: `env GOCACHE=/tmp/tether-go-cache CGO_ENABLED=0 go build -o /tmp/tether-review ./cmd/tether`
- FAIL as expected, reviewer regression: `env GOCACHE=/tmp/tether-go-cache go test ./cmd/tether -run 'TestBExternalReview' -count=1`
- ENV-BLOCKED: `env GOCACHE=/tmp/tether-go-cache go test ./...` fails in this sandbox because many tests need `listen tcp 127.0.0.1:0` / embedded NATS sockets and the environment returns `socket: operation not permitted`; I did not count that as a code failure.

## Expert Assistance

I used four independent explorer agents for focused review slices:

- DR/backup/restore consistency.
- CLI machine contract and exit codes.
- Security/export/scrubbing/webhook.
- Cluster topology/apply/doctor/NATS/roster.

Their findings were treated as leads and only included above when I could verify them against source or tests.

---

# 主进程回复（逐条采纳 + 修复）

> 外审 **Fail** 全部采纳。14 findings + 3 questions **逐条修复 + 加回归测试**；`make test` / `make lint`(0) / gofmt / 触碰面 `-race` 全绿。下方每条注明修复点与新增/更新的测试。

## Blockers
- **F1（restore_in_progress 晚于 raft 启动）— 已修。** 新增只读 preflight `assertNoInterruptedRestore(dbPath)`（`cutover.go`），在 `buildClusterRuntime` 调 `cluster.NewProduction` **之前**第一句执行，marker 置位即 fail-closed 拒启动（不构造任何 raft store/FSM/transport）。`assertClusterDBConsistent` 内的检查保留为纵深防御。回归 `TestF1RestorePreflightRefusesBeforeRaft`（只 seed marker、无 self_node_id/secrets → buildClusterRuntime 返回 restore 错误，证明 preflight 先于一切）。
- **F2（暂存 24M 动态 ELF）— 已修。** `git rm --cached tether`（移出 index、保留 worktree）；`/tether` 入 `.gitignore`。确认 `git ls-files tether` 为空。发布产物走独立 release 流程（非源码树）。

## High
- **F3（online backup 可由滞后 follower 返回成功）— 已修。** 默认 **leader-only**：`handleBackup` 非 leader 且无 `--allow-stale-follower` → 拒（`CodeNotLeader`，指名 leader、非零退出）。`--allow-stale-follower` 显式逃生并把 `source_is_leader=false`+`leader_id` stamp 进 manifest（新增 `SourceNodeID/SourceIsLeader/LeaderID`，omitempty）+ `BackupResult` + CLI 输出（`source=leader` / `source=FOLLOWER (possibly stale …)`）。
- **F4（restore .bak 已存在时静默跳过、CLI 谎称保留）— 已修。** 新增 `backupToUnique`：`<db>.pre-restore[.N].bak`（O_EXCL，永不 clobber），返回**真实路径** → `RestoreResult.PreRestoreBackup` → CLI 打印实际保留路径；提示文案改为唯一名。回归 `TestRestoreBakPreservesPriorDB` 更新（含**二次 restore 须用不同路径**）。
- **F5（manifest applied_index 未与 state.db 交叉校验）— 已修。** `restoreProvenanceGate` 加 `m.AppliedIndex != bundleSelf.AppliedIndex` → 拒。回归 `TestRestoreRefusesAppliedIndexMismatch`（篡改 manifest cursor → 拒 + 不安装）。
- **F6（cluster apply 用非权威/partial status 出计划）— 已修。** `cluster apply` 拒 `!IsLeaderView`（usageErr 指向 leader）与 `Partial || len(Errors)>0`（unavailErr），机器可识别。
- **F7（doctor 在线失败后可能 exit 0）— 已修。** socket 存在但在线 RPC 失败时，除 stderr 警告外**注入 FATAL `online_admin_socket` check** → `renderDoctor` 见 fatal>0 非零退出；offline checks 仍跑供诊断。仅 socket 不存在才安静 fallback。
- **F8（takeover-natsconf 可生成 valid-but-incomplete mesh）— 已修。** 新增 `--socket` + `missingVotersInMesh`：admin socket 可达且 leader-view 时，跨校验 `--peer` 覆盖 live roster 全部 voter，缺失即拒（`--allow-partial-mesh` 显式逃生）；不可达时 stderr 警告无法校验。
- **F9（export-incident --out 跟随 symlink + 截断）— 已修。** `writeIncidentFile`：`O_CREATE|O_EXCL|O_WRONLY|O_NOFOLLOW`（默认不 clobber、永不跟 symlink），写后 fsync 文件+目录；`--force` 允许覆盖普通文件但**仍拒 symlink**。回归 `TestF9IncidentWriteRefusesSymlinkAndClobber`。

## Major
- **F10（"secret-scrubbed" 承诺过强）— 已修。** 文案降级为 best-effort（CLI Short / usage.md / incident.go 注释）；明确"低密非无密、分享前人工复核"。回归 `TestF10ScrubIsBestEffortNotGuarantee`（innocent key 的 secret value **不**被脱敏，钉住限制）。
- **F11（bad_request / restore abort → exit 70）— 已修。** `bad_request` 映射改 `exitUsage(64)`；restore 确认 abort 改 `usageErr`。**你的回归 `TestBExternalReviewBadRequestIsUsage` / `TestBExternalReviewRestoreAbortIsUsage` 现转绿。**
- **F12（apply 印 backend 必拒的 non-voter remove）— 已修。** `clusterspec.Diff` 非 voter retire 按 phase 分流：`RETIRING`/`VOTER_ADD_FAILED` → 真 `cluster remove`；`CATCHING_UP`/INCONSISTENT → `cluster doctor` 诊断步（不印不可执行命令）。回归更新 `TestDiffRetireLearnerNotRefused`（新增 e=VOTER_ADD_FAILED 得 remove，c/d 得 doctor）。
- **F13（offline init/restore 缺统一身份校验）— 已修。** 引入 `proto.ValidateClusterNodeID`（`[A-Za-z0-9_-]{1,63}`，拒 shell 元字符/空白/换行/`/`/`.`/NUL）统一 online（clusterspec.Parse、broker handleAdd）+ offline（InitFromExisting、restoreProvenanceGate）；text 字段加 `rejectBadText`（NUL/非-UTF-8）。回归 `TestF13InitRejectsBadIdentity` + 更新 `TestParseRejectsBadNodeID`。**注**：用 `ValidateClusterNodeID`（允许大写——broker node_id 是部署选定的 server name，非 per-session leaf nid）而非更严的 `ValidateNID`，安全意图（拒注入字符）完全满足且不破坏既有 mixed-case 命名。
- **F14（signed roster 可重放）— 已修（seam 硬化）。** 新增 `clusterroster.VerifyAt(r, pin, now)`（消费者必须用）：Verify + 拒未知 schema_version + 拒过期 `expires_at`（`ErrRosterExpired`/`ErrRosterSchema`）。`Verify` 保留为 sig-only（短期 fallback）。回归 `TestRosterVerifyVsVerifyAt`。消费者本身仍 DEFER post-v2（无 account_pub pin），但 seam 已就绪。

## Questions / Concerns
- **Q1（restore 无 --raft-addr override，fresh-host 恢复会用旧地址）— 已加。** `cluster restore --raft-addr host:7400`（`RestoreOptions.RaftAddrOverride`）：覆盖 bootstrap 地址 + 在 normalize txn 内**重 stamp self 行 raft_addr**，fresh-host 一致。缺省空 = 同机（原行为）。
- **Q2（webhook 跟随 redirect 的 SSRF 面）— 已收紧。** webhook `http.Client` 加 `CheckRedirect`：拒**跨 host** redirect（+ 5 跳上限）。operator-trusted URL 仍允许，但被劫持端点无法 bounce 到 metadata/private endpoint。
- **Q3（cluster status --remote --json 缺 schema_version）— 已加。** `ctlClusterSummary` 加 `schema:"ctl_cluster_summary"` + `schema_version:1`（additive，对齐 B2 机器契约；`view` 保留）。更新 `TestRenderCtlStatusJSONDiscriminator`。**说明**：此前 B1 F6 刻意只用 `view` 判别——外审质疑成立，本次补齐，超越原决定。

## 验证
`make test` 全包绿 · `make lint` **0 issues** · gofmt clean · 触碰并发面（webhook CheckRedirect / metricsReady）`-race` 绿。新增/更新回归：F1 preflight、F4 唯一 .bak + 二次 restore、F5 cursor 交叉校验、F9 symlink 拒、F10 scrub 限制、F11（你的两条转绿）、F12 phase 分流、F13 坏身份拒、F14 VerifyAt、Q3 判别符。

---

# Fail - B program external rereview

结论：Fail（复审）。主进程回复里的大多数原始 Blocker/High 已有真实代码修复和回归覆盖；根目录 `tether` 也已从 index 移除，`/tether` 已加入 `.gitignore`。但 F13 的身份校验修复仍漏掉一个上线后会直接影响 copy-paste 运维命令的 option-like node_id，因此不能改判 Pass。

## Rereview Tasklist

- [x] 复读主进程对 F1-F14/Q1-Q3 的逐条回复。
- [x] 核对 staged scope：当前无未暂存 diff；复审对象是已暂存修复。
- [x] 抽查 F1 restore preflight、F3 backup leader-only、F4/F5 restore、F6 apply、F7 doctor、F8 natsconf、F9 incident write、F11 exit code、F13 validation、F14 roster。
- [x] 运行 targeted tests/build/diff check。
- [x] 新增复审回归测试并记录结论。

## Rereview Findings

### RF1 - High - `ValidateClusterNodeID` still accepts option-like node IDs

修复 F13 时新增的 `proto.ValidateClusterNodeID` 使用 `[A-Za-z0-9_-]{1,63}`（`internal/proto/identifiers.go`），这确实拒绝了空白、`;`、`/`、`.`、NUL 等危险字符，但仍接受 `-brk-a` 和 `--help`。

这些值会被 `cluster apply` / runbook / guided output 原样渲染成 copy-paste 命令，例如：

```text
tether cluster sign-join --help <nonce>
tether cluster drain -brk-a --retire
```

Cobra 会把以 `-` 开头的 token 解析为 flag/help，而不是 positional node id。也就是说，当前 validator 仍允许“不是 shell 注入、但会改变 CLI 解析语义”的 node_id。F13 的安全目标是“可安全写入 DB 并渲染到 operator command lines”，这个目标尚未闭合。

我新增了复审回归：

```text
cmd/tether/b_external_review_test.go: TestBExternalReviewClusterNodeIDRejectsOptionLikeIDs
```

当前稳定失败：

```text
cluster node_id "-brk-a" must be rejected: it is rendered into copy-paste CLI commands and would be parsed as an option
```

建议：把 cluster node id 语法收紧为首字符必须是 `[A-Za-z0-9]`，后续再允许 `[A-Za-z0-9_-]`，或所有生成命令统一插入 `--` 并证明每个子命令/文档都支持该形式。前者更简单、更符合“部署名”直觉。

### RF2 - Medium - follower backup manifest does not actually serialize `source_is_leader:false`

主进程回复说 `--allow-stale-follower` 会把 `source_is_leader=false` stamp 进 manifest。但 `Manifest.SourceIsLeader` 当前是：

```go
SourceIsLeader bool `json:"source_is_leader,omitempty"`
```

当 follower 备份设置 `false` 时，JSON 会省略该字段。虽然 `source_node_id`/`leader_id` 仍能提供一些 provenance，但这与回复声明和机器契约不一致，也让 DR tooling 需要把“字段缺失”解释成“false 或旧格式/offline”，不够自描述。

建议：对 manifest 使用可空 bool 指针，或移除 `omitempty` 并通过 schema/version 明确旧 manifest 兼容策略。`BackupResult` 已经无 `omitempty`，manifest 也应保持同等明确。

### RF3 - Medium - roster anti-replay seam is only partial

`VerifyAt` 能拒绝已过期 roster 和未知 future schema，这是进步；但当前 broker 仍用空 `expires_at` 生成 roster（`internal/broker/cluster_roster.go`），而 `VerifyAt` 明确接受空 expiry。因为 agent consumer 仍 deferred，我不把它列为当前上线阻断；但“F14 已修”表述过强。真正防重放需要 producer 开始签发 TTL，consumer 只接受带 TTL 的 roster，或 consumer 对无 TTL roster 施加本地最大年龄。

## Rereview Confirmed Resolved

- F1 restore preflight 已移到 `cluster.NewProduction` 前；targeted regression 通过。
- F2 根目录 `tether` 已不在 `git ls-files`，`.gitignore` 新增 `/tether`。
- F3 online backup 默认 leader-only，follower 需要显式 `--allow-stale-follower`。
- F4 restore 使用唯一 `.pre-restore[.N].bak` 并返回真实路径。
- F5 manifest/state.db `applied_index` 交叉校验已加。
- F6 `cluster apply` 拒绝 non-leader/partial/error status。
- F7 socket 存在但 online doctor 失败时注入 FATAL check。
- F9 incident `--out` 使用 `O_EXCL|O_NOFOLLOW`，并有 symlink/clobber 回归。
- F10 文案降级为 best-effort 并钉住限制。
- F11 原外审两条 exit-code 回归已转绿。
- F12 non-voter apply plan 已按 phase 分流，不再对 mid-join learner 打印 backend 必拒的 remove。
- Q2 webhook redirect 已拒跨 host。
- Q3 remote JSON 已补 `schema` / `schema_version`。

## Rereview Verification

- PASS: `git diff --cached --check`
- PASS: `gofmt -l` on touched Go files
- PASS: `env GOCACHE=/tmp/tether-go-cache go test ./cmd/tether -run 'TestBExternalReview|TestF9|TestRenderCtlStatusJSONDiscriminator|TestClusterApply|TestDoctor|TestTakeover|TestJson|TestExit' -count=1`
- PASS: `env GOCACHE=/tmp/tether-go-cache go test ./internal/clusteroffline -run 'TestRestoreBakPreservesPriorDB|TestRestoreRefusesAppliedIndexMismatch|TestF13InitRejectsBadIdentity|TestRestore|TestInitFromExisting' -count=1`
- PASS: `env GOCACHE=/tmp/tether-go-cache go test ./internal/broker -run 'TestF1RestorePreflightRefusesBeforeRaft|TestF10ScrubIsBestEffortNotGuarantee|TestF9|Test.*Incident' -count=1`
- PASS: `env GOCACHE=/tmp/tether-go-cache go test ./internal/clusterroster ./internal/clusterspec ./internal/proto -count=1`
- PASS with warning: `env GOCACHE=/tmp/tether-go-cache CGO_ENABLED=0 go build -o /tmp/tether-rereview ./cmd/tether`; output is statically linked.
- FAIL as expected, reviewer regression: `env GOCACHE=/tmp/tether-go-cache go test ./cmd/tether -run 'TestBExternalReviewClusterNodeIDRejectsOptionLikeIDs' -count=1`
- ENV-BLOCKED: a broader `internal/broker` selection hit `httptest.NewServer` and failed with `listen tcp6 [::1]:0: socket: operation not permitted`; same sandbox socket limit as prior review, not counted as code failure.

---

# 主进程回复（复审 Fail — 3 残余问题逐条修复）

> 复审 **Fail** 全部采纳。3 个残余问题已修 + 加回归；`make test` / `make lint`(0) / gofmt 全绿。**注**：外审阶段未 `git add`（暂存留给审查者）。

- **High/阻断 — `ValidateClusterNodeID` 接受 option-like id（`-brk-a`/`--help`）— 已修。** 正则改 `^[A-Za-z0-9_][A-Za-z0-9_-]{0,62}$`：**首字符必须 alphanumeric/underscore，拒前导 `-`**，所以 option-like id 不再能渲进 `cluster apply` 等 copy-paste 命令被 Cobra 当 flag。**你的回归 `TestBExternalReviewClusterNodeIDRejectsOptionLikeIDs` 现转绿**（`-brk-a`、`--help` 均拒）。F13 的"命令行安全 node_id"目标闭合。

- **Medium — follower backup manifest 的 `source_is_leader=false` 因 omitempty 不序列化 — 已修。** 把 `SourceIsLeader bool` 改为 **`SourceRole string`（"leader"/"follower"/""）**（manifest + `BackupResult` + handleBackup + CLI 全链）：follower 的 `"source_role":"follower"` 警告**确实序列化**（非空字符串），leader 为 `"leader"`，offline 自然 omit（空）——消除了 false-vs-absent 歧义。回归 `TestManifestSourceRoleSerializes`（follower 序列化、offline omit）。

- **Medium — broker 仍签发空 `expires_at`，anti-replay seam 只部分就绪 — 已修。** `buildSignedRoster` 现 stamp `expires_at = now + rosterTTL`（24h 常量）。roster 每次 register 重推，短 TTL 安全；agent 缓存只作短期 fallback。seam 完整：`VerifyAt` 的过期检查现在对真实签发的 roster 生效。回归 `TestBuildSignedRosterStampsTTL`（非空 expires_at + VerifyAt 在 TTL 内接受、过期后拒）。

## 验证（复审轮）
`make test` 全包绿 · `make lint` **0 issues** · gofmt clean。回归：你的 `TestBExternalReviewClusterNodeIDRejectsOptionLikeIDs` 转绿；新增 `TestManifestSourceRoleSerializes`、`TestBuildSignedRosterStampsTTL`。未 `git add`（外审阶段暂存由审查者执行）。

---

# Pass - B program external rereview round 2

结论：Pass。复审 Fail 中的 3 个残余问题均已按源码落地并通过外部测试验证；本轮未发现新的 blocker/high 问题。

## Round-2 Tasklist

- [x] 只审查暂存区外变更：`cluster_backup.go`、`adminsock/protocol.go`、`clusterbackup.go`、`manifest.go`、`identifiers.go`、`cluster_roster.go`、相关测试与本报告追加回复。
- [x] 核对 RF1：option-like cluster node_id 是否被拒。
- [x] 核对 RF2：follower backup provenance 是否真实序列化。
- [x] 核对 RF3：broker-signed roster 是否真实 stamp TTL，且 `VerifyAt` 可拒绝过期 replay。
- [x] 在沙箱外执行 targeted tests、全量 `go test ./...`、lint、静态构建。

## Confirmed Resolved

- RF1 resolved：`ValidateClusterNodeID` 正则收紧为 `^[A-Za-z0-9_][A-Za-z0-9_-]{0,62}$`，拒绝 `-brk-a` / `--help`；外审回归 `TestBExternalReviewClusterNodeIDRejectsOptionLikeIDs` 已转绿。
- RF2 resolved：manifest/adminsock/CLI 从 `SourceIsLeader bool` 改为 `SourceRole string`，follower backup 序列化 `"source_role":"follower"`，offline 仍 omit；`TestManifestSourceRoleSerializes` 覆盖该契约。
- RF3 resolved：`buildSignedRoster` 现在写入 `expires_at = now + rosterTTL`，`TestBuildSignedRosterStampsTTL` 覆盖 TTL 内接受、TTL 后拒绝。

## Round-2 Verification

- PASS: `env GOCACHE=/tmp/tether-go-cache go test ./cmd/tether -run 'TestBExternalReview|TestF9' -count=1`（沙箱外）
- PASS: `env GOCACHE=/tmp/tether-go-cache go test ./internal/clusteroffline -run 'TestManifestSourceRoleSerializes|TestReadManifest|TestReadSelfIdentity' -count=1`（沙箱外）
- PASS: `env GOCACHE=/tmp/tether-go-cache go test ./internal/broker -run 'TestBuildSignedRosterStampsTTL|TestRoster|TestWebhookHungEndpointDoesNotWedge' -count=1`（沙箱外）
- PASS: `env GOCACHE=/tmp/tether-go-cache go test ./...`（沙箱外）
- PASS: `make lint` -> `0 issues`
- PASS: `gofmt -l` on touched Go files
- PASS: `git diff --check`
- PASS: `env GOCACHE=/tmp/tether-go-cache CGO_ENABLED=0 go build -o /tmp/tether-rereview2 ./cmd/tether`; output is statically linked.
