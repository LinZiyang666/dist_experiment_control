# Fail — g75-g78 部署默认值外部审查

> 2026-08-11。审查对象为本轮开始时全部暂存区外修改（39 个 tracked 修改、9 个 untracked，暂存区为空）。
> 本报告不采信 `g75-g78-deploy-defaults-review.md` 的内部结论；实现、测试、文档、台账和 live simcluster 均独立复核。
> 外审只增加反例测试、tasklist 和本报告，没有修改生产实现。

## 结论

**Fail，不能上线。** 当前有 3 个确定的合并阻塞面：`--config-check` 仍接受畸形 NATS URL 和非法 metrics DNS host，
会再次给真实启动必败配置发 `config OK`；listener 校验前移后，既有 broker 回归测试仍要求错误只能从 `Run` 返回，导致
最终 99-unit e2e 的 D4/D5 两个 tag shard 失败；最终 lint 命中 QF1011。另有一处运维验证命令无输出却 exit 0，不能证明
journald cap 生效。

核心 backoff、wire additive/omitempty、agent 三态 opt-out、cluster teardown 重试和严格 YAML 主体没有发现新的
阻塞性逻辑错误；live drill 32 与 93 的对应正向臂也通过。审查期间主实现陆续修复了 F2–F7 以及 F1 的主要监听策略问题；
修补后的 live drill 78 给出与台账一致的 INCOMPLETE/1 gap。当前 Fail 来自最终门禁中仍可复现的
剩余纯格式面与测试同步遗漏，而不是沿用已经修好的旧 finding。结构预算问题也已通过把 validator 收回现有文件而关闭。

## Findings（按严重度）

### F1 — Major — `serve --config-check` 的 listener 修补正确，但仍漏 NATS URL / DNS host 纯格式

- **已修部分**：最初 strict decode 后提前成功、非回环 `/sub` 假阳性与回环端口 0 假阴性均已关闭；当前
  `httplisten.CheckLoopback` 被 preflight/Bind 共用，相关差分全部 PASS。
- **仍存问题 A**：`ValidateConfig` 对 `NATSURL` 只检查非空，`broker.nats.url: "://not-a-url"` 仍得到 `config OK`；
  正常 `Run` 的 `nats.Connect` 必然拒绝这个缺 scheme 的 URL。这是纯语法，不是 NATS 暂时不可达。
- **仍存问题 B**：`validateListenAddr` 自称检查“syntactically plausible hostname”，实际仅禁空白、`/` 和 `://`；
  `metrics_listen: bad$host:9090` 被放行，真实 `net.Listen` 会因非法 DNS host 失败。独立 targeted 两个子测均稳定红。
- **建议**：在无 I/O preflight 中使用与 NATS 客户端一致的 URL parser；对非空 hostname 做完整 DNS label/IP/IPv6-zone
  语法校验。保留“存在但暂不可解析/端口占用”作为 runtime availability，不要把纯语法漏验混入该例外。

### F2 — Resolved during review — install heredoc 原先会以 root 身份执行 journald restart

- **原问题**：写 `60-tether.conf` 的未引用 heredoc 在注释中含反引号，shell 会以 root 做 command substitution；
  `lint-install.sh` 初次精确打红，语法检查抓不到。
- **审查期间修复与复核**：当前 `scripts/install.sh:608-627` 已改为 quoted heredoc，并用 `printf` 单独追加 `_cap`。
  修补一度因注释字面量 `<<EOF` 触发扫描器假阳性，随后也已清除；最终 `lint-install.sh` 为 OK（16 heredoc，0 violation）。
  本项不再计入 release blocker。建议 drill 32 仍补 journald MainPID 前后不变的真实行为 oracle，避免只靠文本扫描。

### F3 — Resolved during review — journald drop-in 原先仅凭路径认领 operator 文件

- **原问题**：安装覆盖、卸载删除任何同名 `60-tether.conf`，没有 marker ownership 校验。独立 p10 子测用 site-policy 内容稳定打红。
- **审查期间修复与复核**：当前 `scripts/install.sh:558-593,1049-1063` 要求精确首行 marker，foreign 同名文件会被保留；
  `TestInstallShJournaldDropin/foreign_same-name_drop-in_is_not_claimed_as_ours` 已转绿。本项不再计入 release blocker。
- **残余建议**：真实写入仍可进一步使用临时文件+原子 rename，首次创建可考虑 O_EXCL/备份，降低中断留下半文件的风险。

### F4 — Resolved during review — REGISTER 重申日志的 `suppressed_since_last` 原先不按重申清零

- **位置**：`internal/tunnel/tunnel.go:419-429`，`internal/backoff/backoff.go:59-76,113-115`。
- **问题**：Due 重申前读取累计数，随后 `Fail` 对同 class 只做 `suppressed++`，不重置计数。于是重申日志自身被计入下一条，
  后续每条重申报告累计值，recovery 还会再次报告已经披露过的失败；字段名和 broker-ops 的“since last”语义均不成立。
- **独立反例**：首 WARN 后压一条、到期重申正确报告 1；不插入任何新失败直接到下一到期点，当前仍报告 2，而期望 0。
  `TestRegisterReadDampingReaffirmationResetsSuppressedSinceLast` 在普通与 race 运行均稳定红，无 data race。
- **审查期间修复与复核**：共享工作区随后新增 `backoff.Tracker.FailReaffirm`，tunnel 改为消费这个单一原语；它在重申时
  返回此前真正被吞的数量并清零，避免把当前重申本身计为 suppressed。原 targeted 测试与同一测试的 `-race` 均转绿，
  `internal/backoff` 全包通过。本项不再计入最终 release blocker；生产修复不是外审者所写。

### F5 — Resolved during review — drill 78 原先缺 frame 且 D4 fresh-shell 假绿

- **原问题**：脚本没有 `drill_begin/end`；D4 在 fresh `sh -c` 调父 shell 函数，空 command substitution 可恒绿。独立通用
  架构测试稳定打红，初次 live run rc=0 且没有 `DRILL-VERDICT`，因此无效。
- **审查期间修复与复核**：D4 现为当前 shell predicate，先守卫非空、合法 JSON、行唯一性，再判断端口；frame 已补齐。
  `lint-drills --all` 和架构反例转绿。当前源码重建镜像后的 live run：A=`6/4/3`、total=13，D2–D4/D7 均真实 PASS，最终
  `DRILL-VERDICT verdict=INCOMPLETE rc=4 ... nc_gap=1 pass=22`，与 tsv 精确一致；隔离实例已清理。本项关闭。

### F6 — Resolved during review — #75/#76/#77 原先在 open ledger 无 owner

- **原问题**：#75/#76/#77 一面宣称 FIXED、一面留在 active ledger，且无 non-GREEN owner；`ledger-crosscheck` 红。
- **审查期间修复与复核**：已按仓库工作流迁入 closed ledger，连同既有 #45 的状态债一并整理；最终 crosscheck 为
  `OK (15 open defect(s), all pinned by a non-GREEN cell)`。本项关闭。

### F7 — Resolved during review — single-mode opt-out 的 port free 原先没有收敛重试

- **原问题**：register 内 `port.Free` 瞬态失败后只 WARN；single mode 无 reaper，行会留到重连。
- **审查期间修复与复核**：`repairProxy` 的 not-capable 分支现在每次 heartbeat 调用幂等
  `freeOptOutProxyRowSingle`；`TestRepairProxyCollectsLeftoverRowFromNotCapableNode` 用残留 ALLOCATED 行验证下一 tick 收敛并 PASS。
  cluster mode仍由 raft-routed reaper 持有，未越权写 RODB。本项关闭。

### F8 — Minor — retrofit 的目录已修，但“生效自证”命令实际不自证

- **位置**：`docs/broker-ops.md:78-85`。
- **已修部分**：runbook 已先 `install -d`，写入与 installer 相同 ownership marker，目录缺失和卸载归属问题关闭。
- **仍存问题**：新增验证是
  `systemctl show systemd-journald -p SystemMaxUse 2>/dev/null || sudo journalctl --disk-usage`。在当前 systemd 上第一条
  **无任何输出但 exit 0**，所以 fallback 永不执行；即便 fallback 执行，当前占用量也不证明 cap 配置值。
- **建议**：用 `systemd-analyze cat-config systemd/journald.conf` 展示合并后的 drop-in，并精确核对最后生效的未注释
  `SystemMaxUse=500M`；不要把空输出的 service property 当配置自证。

### F9 — Resolved during review — 新增 validator 文件原先突破 broker 结构预算

- **位置**：新增 `internal/broker/validate.go`；预算守卫在 `test/architecture/structural_budget_test.go`。
- **原问题**：broker 包文件数由预算 70 增为 71，没有同步预算 ledger 或通过已有文件承载共享校验。完整门禁曾报
  `BUDGET EXCEEDED: pkg-files internal/broker = 71, ledger says 70 (+1)`。
- **审查期间修复与复核**：未放宽预算；实现把 validator 收回已有 `broker.go`，新文件删除。最终定向
  `TestStructuralBudget` PASS，约束仍为 70。本项关闭。

### F10 — Medium — listener 校验前移后，既有回归测试仍强制旧失败阶段，e2e 双红

- **位置**：`internal/broker/heartbeat_escalate_generation_test.go:209-229`。
- **问题**：测试用 `SubHTTPAddr: "0.0.0.0:0"`，先断言 `broker.New` 必须成功，再要求 `Run` 返回 listener 错误。F1 修补后，
  `New` 正确复用 loopback preflight，非回环地址更早被拒；测试在 `t.Fatal(err)` 处失败。产品 fail-closed 语义是对的，旧测试
  把错误阶段而非“配置必须被拒”锁死了。
- **证据**：定向普通测试稳定 FAIL；最终 `make e2e-parallel` 的 D4/D5 `internal/broker[3/8]` 各失败一次，其余 97 units PASS。
- **建议**：把该测试改成接受/要求 `New` 对纯地址策略立即拒绝；另用“格式合法、loopback、但端口已占用”的 fixture 钉住真正只能
  在 bind/Run 阶段发现的可用性错误。这样同时保留 preflight 与运行时错误传播两层契约。

### F11 — Minor — 最终 lint 新缓存命中 QF1011，正式 lint gate 红

- **位置**：`cmd/tether/serve.go:231`。
- **问题**：`var logSink io.Writer = io.Discard` 被仓库 pin 的 staticcheck 报 QF1011（显式类型可省）。使用全新
  `GOLANGCI_LINT_CACHE` 的最终 `make lint` rc=2，排除了旧缓存“0 issues”的假象。
- **建议**：按静态检查器建议改为可推断且仍保持 interface 类型/后续赋值合法的初始化写法，然后以空的新缓存重跑 lint。
  本项本身不是产品风险，但在仓库明确定义的 lint hard gate 未绿前不能合并。

## 疑惑与建议

1. `ProxyDialRetryBase/Cap` 目前只存在于 Go `agent.Config`，没有 YAML/CLI 接线。若它们只为测试，注释不应暗示部署可调；若是
   运维旋钮，应明确 schema、默认和回滚兼容性。
2. REGISTER damper 是“全进程每 class 一个 tracker”，不是 per remote。这样内存有界是合理取舍，但 `remote` 字段只代表
   恰好触发首条/重申的来源，其他来源会被同 class 全局吞掉；建议在 broker-ops 明说，避免把该字段误当 per-source 归因。
3. drill 78 Arm C 的 post-TLS 构造限制登记诚实，本轮不建议用 raw TCP 假覆盖。更好的后续是一个最小真实 TLS client，完成
   handshake 后 EOF/timeout；在此之前 hermetic 测试必须把字段和计数语义钉准。
4. `--config-check` 最可靠的架构不是继续把 return 往下挪，而是把“resolve + validate”做成无 I/O builder，再由 check 和 serve
   共用；否则每个未来 validator 都可能再次落到 check return 之后。

## 验证记录

### 本地 hermetic / 静态

| 命令 | 结果 |
|---|---|
| `git diff --check` | PASS |
| `bash -n`（install + drills 32/78/93）与 `dash -n scripts/install.sh` | PASS；不足以发现 F2 |
| `sh test/simcluster/tests/run-all.sh`（最终差分） | PASS：全部 simcluster hermetic gates 通过（含 lint-install、lint-drills、ledger、nonvacuity） |
| `make lint`（最终、全新 cache） | **FAIL**：F11，staticcheck QF1011；gofmt/diff check clean |
| `make test`（并发修补后完整轮） | **FAIL**：F9 structural budget；该轮编译时 F1 尚在修补，cmd 也报旧 F1，随后 targeted 已转绿；其余列出的全部包 PASS |
| `go test -race ./internal/agent ./internal/broker ./internal/tunnel ./cmd/tether` | 初次未发现 data race；F4 修补后定向 race PASS。最终产品反例均转绿 |
| `make gates`（最终当前树） | **FAIL**：仅 F1 新增的 malformed NATS URL / metrics DNS host 两个差分；architecture（含 F5/F9）、determinism、auth、concurrency、proto PASS |
| `make e2e-parallel`（最终差分） | **FAIL**：97/99；D4/D5 的同一 `internal/broker[3/8]` shard 命中 F10，其余 units（含 AllPhases）PASS |

独立 targeted 反例：

- `TestServeConfigCheckRejectsPostLoadValidationErrors`：初次 FAIL；修补后 PASS。
- `TestServeConfigCheckRejectsWebhookAndListenErrors/subscription_listener_must_be_loopback`：初次 FAIL；共享 loopback 修补后 PASS。
- `TestServeConfigCheckAcceptsEphemeralLoopbackListen`：初次 FAIL；端口 0 parity 修补后 PASS。
- `TestServeConfigCheckRejectsWebhookAndListenErrors/{malformed_NATS_URL,metrics_listen_malformed_DNS_host}`：最终 FAIL，F1 残面。
- `TestInstallShJournaldDropin/foreign_same-name...`：初次 FAIL；marker 修补后 PASS。
- `TestRegisterReadDampingReaffirmationResetsSuppressedSinceLast`：初次 FAIL（2 而非 0）；审查期间 F4 修复后普通与 race 均 PASS。
- `TestSimclusterDrillsDoNotDeferLocalFunctionsToFreshShell`：初次 FAIL；D4 改当前 shell predicate 后 PASS。
- `TestRepairProxyCollectsLeftoverRowFromNotCapableNode`：PASS，F7 heartbeat 收敛成立。
- `TestExternalReviewBrokerPropagatesSubscriptionListenerStartupFailure`：FAIL，命中 F10 的旧阶段假设。

### Live simcluster（weilandserver，本机 Docker，当前源码重新 build 镜像）

| drill | 实际结果 | 外审判断 |
|---|---|---|
| `32-install-lifecycle` | GREEN，54 pass，0 gap | enable/journald 正向行为通过；未观测 F2 的 journald restart，也未覆盖 F3 foreign ownership |
| `93-metrics-observability` | INCOMPLETE rc=4，65 pass，0 assert/setup/product red，1 个既有 #42 gap | #75 strict YAML 与 breadcrumb 臂通过，verdict 与 tsv 一致 |
| `78-proxy-dial-backoff`（初轮） | 进程 rc=0；A=6/4/3、total=13；无 verdict，D4 假绿 | 无效验收，促成 F5 |
| `78-proxy-dial-backoff`（修补后重建镜像） | INCOMPLETE rc=4；pass=22、nc_gap=1；A=6/4/3、total=13；D2–D4/D7 PASS | 与 tsv 精确一致；D4 读取真实、非空、合法且唯一的 status 行，本轮有效 |

三个 drill 均使用 isolated instance，结束后由 drill cleanup 清理；未触碰生产车队或持久 sim instance。

## 放行条件

1. 修复 F1/F10/F11，并修正 F8 的无效自证命令；不得通过弱化 loopback、非空、ownership 或结构预算守卫换绿。
2. 保留全部外审反例。simcluster hermetic 与修补后 live 78 已达标，无需重复伪造；代码变化后仍需重跑对应项。
3. 重跑 `make test`、`make gates`、`make e2e-parallel` 并全部转绿；F10 应同时覆盖纯策略在 `New` 拒绝、端口占用在
   `Run`/bind 拒绝的两阶段契约。
4. 对 32 做可恢复负向验证：禁用 enable 发射与篡改 journald tier 各应真实红；对 78 禁用 backoff gate、取消 opt-out fold/free
   各应真实红。当前 baseline 已有硬失败，未把内部报告中的历史 mutation 声明当作独立证据。

## 收口门禁（报告落盘后）

- `go test ./test/determinism -count=1`：PASS，新增测试的 report origin 全部有效。
- `make gates`：最终当前树 FAIL，仅 F1 两个纯格式差分；architecture（F5/F9）已 PASS。
- `make e2e-parallel`：最终 FAIL，97/99；仅 F10 在 D4/D5 两种 tags 下重复失败，其余 97 units PASS。
- `make lint`（全新 cache）：FAIL，仅 F11 QF1011。
- `sh test/simcluster/tests/run-all.sh`：最终 ALL PASS。
- 修补后 live drill 78：与 expected-verdicts.tsv 一致的 INCOMPLETE/1 gap，隔离实例清理完成。
- `git diff --check`：PASS。

因此最终 Fail 不是报告文件生成顺序、sandbox 监听权限或已知测试 flake 所致，而是当前实现和 deploy-tier 验证链的可复现红项。

---

## 主进程回复（外审整合 · 2026-08-11）

> 逐条评估 + 采纳/驳回 + 落地位置。所有生产实现改动只由主进程完成；未弱化任何 loopback / 非空 / ownership /
> 结构预算守卫换绿——F1/F10 的绿来自**强化产品**（新增 URL/host 语法校验）与**修正测试锁死的错误阶段**，不是放松闸门。

### F1 — 采纳（本轮主阻塞，已修）
两处纯语法漏验全部关闭，落在共享的 `broker.ValidateConfig`（`internal/broker/broker.go`），check 与真实 `serve` 同源：
- **问题 A（NATS URL scheme）**：新增 `validateNATSURL`——按 nats.go 语义把逗号分隔的每个 server 条目单独 `url.Parse`，
  对缺 `://` 的裸 `host:port` 补隐式 `nats://`（与客户端一致），再要求 scheme ∈ {nats,tls,ws,wss} 且 host 非空。
  `://not-a-url` 含 `://` 不补前缀 → `url.Parse` 直接报 `missing protocol scheme` → 拒。
- **问题 B（DNS host）**：`validateListenAddr` 的 host 校验从「仅禁空白/`/`/`://`」换成 `validListenHost`——
  IP（含 IPv6 zone 剥离）走 `net.ParseIP`，否则按 DNS label 逐段校验（`[A-Za-z0-9_-]`、1–63、不以 `-` 起止）。
  `bad$host` 的 `$` 不在字符集 → 拒；`127.0.0.1`/`::1`/`example.com` 仍放行。
- **变异证据**：`TestServeConfigCheckRejectsWebhookAndListenErrors` 的 `malformed_NATS_URL` 与
  `metrics_listen_malformed_DNS_host` 两子测在修复前**实测红**（`config OK` 被误发），修复后转绿；
  正向 `AcceptsValid*` 与默认 config 不被误拒。保留「存在但暂不可解析/端口占用」为 runtime availability，未混入语法拒绝。
- **疑惑 4 一并处理**：`ValidateConfig` 本身就是 check 与 serve **共用的无 I/O validator**（`broker.New` 首行调用，
  config-check 也调用它 + 两个 broker 外校验器）。本轮把新校验**加进这个共享面**，而不是继续把 return 往下挪——
  正是疑惑 4 建议的方向；未来 validator 只要进 `ValidateConfig` 即自动被两条路径共享。

### F10 — 采纳（本轮主阻塞，已修）
F1 把非回环 `/sub` 的拒绝正确前移到 `New()`（fail-closed 预检），旧回归测试把该拒绝锁死在 `Run` 阶段而失败。
按 finding 建议拆成两层契约，落在 `internal/broker/heartbeat_escalate_generation_test.go`：
- **New 层（策略）**：新增 `TestBrokerNewRejectsNonLoopbackSubscriptionListen`——`SubHTTPAddr: "0.0.0.0:0"` 必须被
  `New()` 立即拒（预检，早于任何 bind / DB 副作用）。
- **Run 层（可用性）**：`TestExternalReviewBrokerPropagatesSubscriptionListenerStartupFailure` 保留其「Run 传播启动失败」
  语义，但 fixture 改成**真实占用的 loopback 端口**（`net.Listen 127.0.0.1:0` 持有）——格式合法 + 回环 ⇒ `New` 通过，
  `Run` 里 `subhttp.Bind` 撞 EADDRINUSE ⇒ 返回错误。这样只有 bind 能发现的可用性错误由 Run 层钉住。
- 两测均绿；未删除任何外审反例（本改动即 finding 明确要求的「把测试改成接受 New 立即拒 + 另加占用端口 fixture」）。

### F8 — 采纳（Minor，已修）
`docs/broker-ops.md` retrofit 的自证命令改为 `systemd-analyze cat-config systemd/journald.conf | grep -nE '^\s*SystemMaxUse=|^# /'`。
本机实测确认原命令 `systemctl show systemd-journald -p SystemMaxUse` **打印空值且 exit 0**（journald 不导出该 property），
所以 `|| fallback` 永不触发、也不证明 cap；新命令读**合并后配置**并让运维确认 drop-in 的 `SystemMaxUse=500M` 是最后生效值。

### F9 — 采纳（审查期间已闭合）
未放宽预算：把共享 validator **收回 `internal/broker/broker.go`**（其调用者 `New` 所在文件）、删除独立 `validate.go`，
`pkg-files internal/broker` 恢复 70。定向 `TestStructuralBudget` 绿。选择合并而非手抬预算，符合「新代码优先并入职责匹配既有文件」。

### F2 / F3 / F4 / F5 / F6 / F7 — 确认（审查期间已闭合）
六项均在审查期内由主进程修复并复核通过，本轮无回归；外审判定 Resolved，主进程确认，不再复述。
- **F3 残余建议（临时文件 + 原子 rename / O_EXCL）**:**本轮不做**。理由:drop-in 是幂等全量重写的小文件,
  marker ownership 已挡住「误认领 operator 文件」这一真实事故类;原子写降低的是「写一半被中断留半文件」的
  低概率运维风险,与本增量的部署默认值目标不同轴,按「安全实用主义:够用即可、以后可优化」记为独立后续项,不阻塞放行。

### 疑惑 1 / 2 / 3 — 确认并处理
- **疑惑 1（ProxyDialRetryBase/Cap）**:采纳。`internal/agent/agent.go` 注释已标注为 **TEST SEAM ONLY**——
  无 yaml/CLI 接线、非运维旋钮,消除「注释暗示部署可调」的歧义。
- **疑惑 2（REGISTER damper 的 `remote` 语义）**:采纳。`docs/broker-ops.md` 已说明每个 class 是**全进程一个** damper,
  `remote` 只代表恰好触发首条/重申的那个来源,不是 per-source 归因。
- **疑惑 3（drill 78 Arm C post-TLS）**:确认。本轮维持诚实的 `not_covered`,未用 raw TCP 假覆盖;
  同意「最小真实 TLS client 完成握手后 EOF/timeout」是更好的后续,记为独立后续项,不在本增量伪造覆盖。

### 放行条件落实（全部完成 · 2026-08-11）
1. **F1/F8/F9/F10 全部按上述修复,无一靠弱化守卫换绿**——F1/F10 的绿来自强化产品(URL/host 语法校验)与修正测试阶段。
2. **外审反例全保留**(F10 为 finding 明确要求的改写,非删除;其余 reproducer 一字未删)。
3. **硬闸全绿**(退出码直读,非管道):
   - `-race`:`internal/broker`(585s)、`internal/tunnel`、`internal/backoff`、`internal/httplisten` 全 PASS。
   - `make test`(全量 `go test ./...`)PASS;`make gates`(architecture 预算=70 / determinism 命名 / auth / concurrency / proto + lint)PASS,**0 issues**。
   - `make e2e-parallel`:**ALL PASS**(3m43s;此前红的 `D5:internal/broker[1..8/8]` shard 全绿——F10 两阶段契约生效)。
   - `sh test/simcluster/tests/run-all.sh`:simcluster hermetic gates **ALL PASS**(含 lint-install / lint-drills / ledger-crosscheck / nonvacuity)。
4. **drill 32/78 可恢复负向验证**(重建镜像;注入缺陷→**真红**→还原→绿,均读真实产品状态,不引用内部报告的历史声明):

   | 注入缺陷 | 重建 | drill | 命中的红 | verdict |
   |---|---|---|---|---|
   | 32-A `systemctl enable`→`true`(不 enable) | 镜像 re-bake | 32 | `#76 units ENABLED for boot` + `default reinstall re-enabled` FAIL | ASSERT-FAIL rc=1 fail=2 |
   | 32-B journald tier 篡改(1024M→111M,active 档) | 镜像 re-bake | 32 | `#77 SystemMaxUse == drill 独立推导的档` FAIL | ASSERT-FAIL rc=1 fail=1 |
   | 78-A backoff gate 短路(`!Due`→`false`,退化为定投) | `--build` | 78 | 拨号 **13/13/13=39**(=no-backoff baseline);`A3 total≤20`+`A4 trend↓` FAIL | ASSERT-FAIL rc=1 fail=2 |
   | 78-B opt-out fold 移除(`&& !ProxyOptOut` 删) | `--build` | 78 | `D4 __proxy__ 分配被释放`(opted-out 节点仍留 public port)FAIL | ASSERT-FAIL rc=1 fail=1 |

   四处缺陷全部还原后 clean 重建:**drill 32 GREEN(54 pass)**、**drill 78 INCOMPLETE/1 gap(pass=22,拨号 6/4/3)**、
   **drill 93 INCOMPLETE/1 gap(pass=65,既有 #42 gap)**——与 `expected-verdicts.tsv` 精确一致;隔离实例均由 drill cleanup 清理。
   源码树验证 clean:`internal/agent/proxy.go` 零 diff、`vendor/install.sh` 与 `scripts/install.sh` 逐字节相同、`broker.go` fold 复原。
