# upgrade-safety 内审报告

> 2026-08-01。5 lane 审查 → 5 lane 1:1 对抗核验 → 综合（11 agent workflow）。
> 主进程逐条处置见文末「主进程处置」节——每条 finding 的采纳/驳回与修复落点以该节为准。

## 0. 结论一句话
5 lane × 5 对抗核验后：**0 BLOCKER / 10 MAJOR / 12 MINOR / 8 NIT 存活**（跨 lane 合并去重后 30 条）；无整条 REFUTED，5 条被核验降级、若干子论断被驳（见 §3）；agent 侧状态机骨架扎实，最薄处是 ctl `--wait`/金丝雀判据与 install 路径锁纪律，S1–S2（伪 commit 窗口、install 无锁 TOCTOU）与 S3–S4（首跳 legacy NewVersion、同 tag 假 COMMITTED）建议本轮必修。

## 1. 存活 finding 逐条表

| # | 级 | 位置 | 陈述 | 失败场景 | 建议修法 | verdict |
|---|---|---|---|---|---|---|
| S1 (FM-1) | MAJOR | internal/agent/upgrade_state.go:394-398,433-436 | "是否新二进制"用磁盘 sha 判定，flip→exec 窗口（≥100-200ms）内旧进程可伪 commit | rename 后 exec 前 NATS 重连触发 re-register→marker 变 committed→新映像 boot 无预算无 watchdog（起来即崩则 NAT 后永久失联）；exec 失败则 recoverFromFailedExec 见非 pending→os.Exit(1) | commit 判据加 `m.BootCount > 0`（install 写的 marker 恒 0，现有 e2e 预置 boot_count:1 仍绿） | CONFIRMED |
| S2 (FM-2+CL-1+CL-2) | MAJOR | internal/agent/upgrade.go:91-100,492-529 + exec.go:92 | install 全段（入口门→prev Remove/Link→marker 写→rename）不持锁：与并发二次 install（每消息独立 goroutine，pending marker 下载+冒烟后才落盘）、与 deadline 时刻的 watchdog/commit 均构成 TOCTOU，违背 plan R3"marker 转换单一 owner" | 双 install 交错→prev 槽丢原始好二进制且与 marker.PrevSHA 发散→回退变 rollback_failed；watchdog 同刻交错同后果；commit 的 read→Remove 间隙可误删新 pending marker | handler 整段加专用 `upgradeInstallMu.TryLock()`（拿不到回 upgrade_in_progress），勿复用 upgradeMu（持锁 ~35s 饿死 watchdog/commit）；executeRollback 参与同一互斥 | CONFIRMED ×3 |
| S3 (W-1) | MAJOR | cmd/tether/node.go:400-401 + internal/proto/messages.go:754 | 旧 agent 的 NewVersion 是 `tether version` 首行全文（非归一化 tag，实证 v0.4.0–v0.4.7），fallback 分支走不到；messages.go 注释"empty from a pre-upgrade-safety agent"事实错误 | 本增量发布后第一次对旧车队 `node upgrade --all`：升级实际成功却等式永不成立→150s 假 ROLLED BACK→金丝雀 abort 整个车队（自己的发布路径必踩） | ctl 侧防御归一化：含空格/不以 `v` 开头视为 legacy 降级为 `""` fallback；修正注释 | CONFIRMED |
| S4 (OPS-1+W-6+FM-3) | MAJOR | cmd/tether/node.go:397-405 | `--wait` 判据 `ReleaseVersion==newVersion ∧ ONLINE` 不区分旧注册行与新 register 事件：同 tag 重发/同 release 重装首轮 3s 轮询命中旧行即假 COMMITTED | 金丝雀空洞确认后扇出可能有害产物；agent 随后 F5 回退运维全程绿灯。**裁决注**：FM/W 核验判 MINOR、OPS 核验判 MAJOR，我读码裁决 MAJOR——`startRelease` 捕获后从未与 `newVersion` 比对（:376 只作 lastSeen），同 tag 重推是本车队记录在案的标准操作，且失败方向是金丝雀门整体失效 | `newVersion == startRelease` 时显式降级告警并拒绝以此放行 `--all` 扇出 | CONFIRMED（级别冲突已裁决） |
| S5 (OPS-4+TQ-5) | MAJOR | internal/agent/upgrade.go:621-642 + cmd/tether/main.go:52 | smokeVersion 零测试（docstring 自称 "pinned by test" 为虚）；emitter/parser 是两处独立 literal，无测试跨缝钉住 `smokeVersion(真实首行)==proto.ReleaseVersion`，而这是 N-1 兼容面（旧 agent 解析新二进制） | version 首行格式改动+同步更新 TestVersionCommand 即全绿→旧 agent 解析出错误 NewVersion→全车队假 ROLLED BACK。**裁决注**：TQ 核验持 MINOR（e2e 有间接覆盖）、OPS 核验持 MAJOR，我裁 MAJOR——e2e 覆盖是 fixture 验证 fixture，跨缝方向零钉子 | 补表驱动单测（含 exit-1 对抗用例）；长期把格式串收敛为 proto 共享常量 | CONFIRMED（级别冲突已裁决） |
| S6 (OPS-3) | MAJOR | cmd/tether/node.go:382-383 | `--wait` 超时与金丝雀失败返回裸 fmt.Errorf→classifyExit 落缺省 70，usage.md 教自动化"70 重试" | 起来即崩的产物被自动化无限重推，每轮目标机崩 3 次再回退——批 A 已修的"未分类 70=假重试许可"缺陷模式在新路径复现 | 显式给类（建议 75 或至少非缺省 70） | CONFIRMED |
| S7 (OPS-5) | MAJOR | cmd/tether/node_classify_test.go:27-74 | `smoke_failed`/`upgrade_in_progress` 不在分类测试与对账表：变异"从 configUpgradeCodes 删 smoke_failed"零测试变红 | plan §10.5 点名要外审复核的"smoke_failed 全队 abort"裁决在 ctl 侧零测试；违背本仓"每条新守卫必须变异验证"纪律 | 补分类测试（wire 形状含 `agent_rejected:` 包裹，已核实） | CONFIRMED |
| S8 (TQ-1) | MAJOR | internal/agent/agent.go:640 + test/p10 | F5 武装链路（Run 挂 watchdog→timer→回退）零测试；唯一 watchdog 单测手调 watchdogRollback 绕过 arming | 变异"删 armUpgradeWatchdog 一行"全绿而 F5 生产全失守；plan §7 承诺的 F5 e2e 未交付 | 补无 broker 的 p10 e2e（见 §2） | CONFIRMED |
| S9 (TQ-3) | MAJOR | cmd/tether/node.go:406-410 | `newVersion==""` fallback 分支零测试 | 删该分支全绿；现网旧 agent + `--wait` 100% 空等假 ROLLED BACK、金丝雀错误 abort | 补 mid-poll 翻转测试（见 §2） | CONFIRMED |
| S10 (W-2) | MAJOR | internal/broker/clusterstatus.go:1117-1124,1196-1201 ↔ requirements §6.7 | 新 §6.7 全称"任何 release 层拒绝都会卡死回滚"与 join 门 release 精确相等硬拒冲突；两处错误串引用已删条文；§21.1"今日全量"盘点漏 row 3 的 release 检查与 fail-closed | 窗口内回滚被拒且错误串指向不存在的条文；盘点表误导 bump 者 | 条文追认豁免（代码注释已有现成论证）或放宽门；无论取哪边必改 :1117/:1124 错误串 + :1140-1155 + skew_test.go:93 注释 | CONFIRMED（文档契约冲突） |
| S11 (OPS-2) | MINOR | cmd/tether/node.go:369-372 | fallback 基线两向误判：初始 lookup 失败静默成 `""`→首轮假 COMMITTED；基线 dispatch 后捕获→快 re-register 时假 ROLLED BACK | 限于 plan §10.1 已声明的首跳 fallback 面，故降级 | lookup 失败重试或响亮失败；基线 dispatch 前捕获 | DOWNGRADED MAJOR→MINOR |
| S12 (FM-4+CL-5) | MINOR | internal/agent/upgrade_state.go:214-221 | watchdog 回退 exec 失败→os.Exit(1)，nohup 无 supervisor 即死，与 plan F5"nohup 三者皆闭合、不依赖 supervisor"不符 | 双重故障（register 超时 + prev exec 失败）才触发，概率极低 | watchdog 路径 exec 失败继续现进程运行，或 broker-ops §8.7 如实标 GAP | CONFIRMED ×2 |
| S13 (FM-5) | MINOR | upgrade_state.go:117 + upgrade.go:544,599 | 三处写盘无 fsync，F8"任意断点闭合"只在命名空间层成立 | 非 ext4-auto_da_alloc 上 rename 后断电→截断 dst/损坏 marker→按 idle 处理→无回退 crash-loop | 三处 close 前加 `f.Sync()`，三行 | CONFIRMED |
| S14 (CL-4) | MINOR | upgrade_state.go:439-441 | commit 落盘失败不停 watchdog，注释"will re-report on next register"失实（下次 register 晚于 deadline） | 已成功 register 的健康升级被回退（方向安全但违背注释；核验订正：不会落 rollback_failed，rename 是元数据操作） | commit 分支落盘失败也停 watchdog，或修注释 | CONFIRMED |
| S15 (FM-6+W-3+W-4) | MINOR | broker.go:1375 / broker/upgrade.go:63 / clusterstatus.go:1179 | plan §0 承诺的三站点 origin 注释一处未落站点行（site#1 论证在 broker_test.go 测试上方）；site#2 无 N-1 臂、#2/#3 无 §21.4 指路 | 未来改 site#2 为区间比较现有测试仍绿（site#1/#3 已有钉子——W-4 的 site#3 指控被驳，见 §3） | 补三行注释；site#2 在 p10 case 表加 `ProtoVersion-1` 臂；可选采纳 W-4 表驱动测试 | CONFIRMED / W-4 DOWNGRADED |
| S16 (W-5+OPS-6) | MINOR | docs/usage.md:1213 ↔ node.go:344-346 | 样例 `✔ nid1: staged (v0.4.7 → v0.5.0, …)` 代码产不出（实际无箭头左值、带 sid/ 前缀）；staged 行零断言 | 运维照文档 grep 永远 miss；plan"被 e2e 断言"未兑现 | 改样例为真实输出，或补整行格式断言 | CONFIRMED ×2 |
| S17 (W-7) | MINOR | internal/cluster/join_bundle.go:21-52 | join bundle JSON 字段（除版本两字段）不在任何机械闸门下，而 §21.2 条文覆盖"一切 wire 变更" | 改名 bundle 字段全闸门绿→已签发 bundle（含 DR rejoin）解码 fail-closed | §21.2 明写闸门边界，或 bundle 并入同款账本 | CONFIRMED |
| S18 (TQ-2) | MINOR | upgrade.go:93 | 入口门 stale-pending 放行分支零覆盖，变异 `Before→true` 全绿 | strand 窗口限当前进程存活期（重启由 decideBoot 收敛——"永久挡住"被驳，故降级） | 采纳 §2 测试 | DOWNGRADED MAJOR→MINOR |
| S19 (TQ-4) | MINOR | node_upgrade_wait_test.go:80,160 | wait 测试预置 release=目标版本，old→new 真实时序与 `Status!=ONLINE` 分支零覆盖；stub 注释"tests mutate it mid-poll"无人兑现 | "首轮 poll 即成"类退化不可见 | 复用 TQ-3 翻转模式；stub 增可切换 Status | CONFIRMED |
| S20 (TQ-6) | MINOR | upgrade.go:496,534 | copyFile 降级分支零覆盖——plan §10.2 点名专查项答案为否 | 网络盘上 prev 槽静默坏，F7 时才暴露 | 至少直测 copyFile（内容+0755） | CONFIRMED |
| S21 (TQ-7) | MINOR | upgrade_state_test.go:340-394 | commit/watchdog 只测两个串行顺序：删双方 upgradeMu.Lock 仍绿；watchdog goroutine 归零承诺无测试 | mutex 未被变异验证 | `-race` 下真并发测试 + arm 后 cancel 的 NumGoroutine 回落断言 | CONFIRMED |
| S22 (OPS-7) | MINOR | docs/broker-ops.md:735 | `### 8.7` 物理位于 `## 9` 之内（§9.10 后），编号断裂 | 读 §8 的运维找不到 8.7 | 移到 §8.6 之后 | CONFIRMED |
| S23 (CL-3) | NIT | agent.go:1016-1050 | register payload 只 marshal 一次，broker 可记假 "committed" 日志行 | 根因归置被驳（在途语义固有），仅日志措辞级 | broker 日志措辞降为 "agent-reported" | DOWNGRADED MINOR→NIT |
| S24 (FM-7+CL-6) | NIT | upgrade_state.go:449-455 | committed marker 永不清理，与 rolled_back 送达即清不对称 | 无失败场景，仅每次 register 一次读+parse | 送达后同样 Remove，或写明保留作运维痕迹 | CONFIRMED ×2 |
| S25 (W-8) | NIT | wire_inventory_test.go:54-57 | `rm + -update` 一步重建收缩账本，摩擦低于宣称 | 依赖 git diff 可见性兜底 | bootstrap 分支加"git 索引中存在则拒绝" | CONFIRMED |
| S26 (W-9) | NIT | error_hints.go:26 | hint "skips it and keeps going" 对金丝雀失真（金丝雀失败是 abort） | 排障误导 | 补半句金丝雀例外 | CONFIRMED |
| S27 (OPS-8) | NIT | node.go:467-520 | `sha256_mismatch` 两集合皆非→逐台失败继续 | "金丝雀可 skip"前提被驳（结构上不可 skip），残余面小且该码先于本增量 | 归类或 plan 写明不动理由 | DOWNGRADED MINOR→NIT |
| S28 (OPS-9) | NIT | upgrade.go:95,167 等 | ① Code 常量零引用用字面量；② --all 强制金丝雀无 RunE 测试；③ broker-ops "≈10 MiB" 漏提 tmp 副本 | 均无硬失败场景 | 酌情 | CONFIRMED |
| S29 (TQ-8) | NIT | upgrade_state.go:399 + broker.go:1460-1463 | rollback_failed 报告分支未测；broker upgrade_state 日志行（两新 wire 字段唯一消费者）删掉无测试红 | 观测信号无钉子 | 捕获 logger 断言一行 | CONFIRMED |
| S30 (TQ-9) | NIT | node.go:388 + upgrade_e2e_test.go:526 | poll 间隔 3s 硬编码不可注入；commit 测试 3s 预算在 e2e-parallel 高负载下偏紧 | flake 面 | 间隔提为 var，预算放 5s | CONFIRMED |

## 2. 专家提议的新增测试（原样保留）

**lane failure — FM-1**（`internal/agent/upgrade_state_test.go`，今日注入即红）：
```go
// origin: upgrade-safety external review FM-1 — flip→exec window: the OLD
// process image re-registers after rename(tmp,dst) but before syscall.Exec.
// Disk sha matches new_sha, yet the staged binary never booted (boot_count==0);
// reporting/committing "committed" here disarms the boot budget and watchdog.
func TestUpgradeCommitRequiresBootedStagedBinary(t *testing.T) {
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, 0, time.Now().Add(time.Minute))
	m.NewSHA = mustSHA(t, exePath)
	m.PrevSHA = mustSHA(t, upgradePrevPath(exePath))
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}
	a := testAgentFor(t, exePath)
	if state, _ := a.upgradeRegisterReport(); state == upgradeStateCommitted {
		t.Error("boot_count==0: the staged binary never booted; must not report committed")
	}
	a.commitUpgradeAfterRegister(upgradeStateCommitted)
	if got := readMarkerState(t, markerPath); got.State == upgradeStateCommitted {
		t.Error("boot_count==0: commit must not land for a never-booted staged binary")
	}
}
```

**lane failure — FM-3**（`cmd/tether/node_upgrade_wait_test.go` 风格，今日注入即红）：
```go
// origin: upgrade-safety external review FM-3 — a same-tag republish: the
// pre-upgrade node.list row already satisfies ReleaseVersion==newVersion, so
// equality alone "confirms" a commit that has not happened.
func TestWaitForUpgradeCommitSameTagIsNotInstantlyConfirmed(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	release := &atomic.Value{}
	release.Store("v0.0.9") // the STALE pre-upgrade row already carries the target tag
	stubNodeList(t, nc, "n1", release)
	old := upgradeWaitBudget
	upgradeWaitBudget = 4 * time.Second
	t.Cleanup(func() { upgradeWaitBudget = old })
	cmd, out := waitTestCmd(t)
	if err := waitForUpgradeCommit(cmd, nc, "lab", "actor-pub", "n1", "v0.0.9"); err == nil &&
		strings.Contains(out.String(), "COMMITTED") {
		t.Fatalf("same-tag target confirmed from the stale row alone: %s", out.String())
	}
}
```

**lane wire — W-1**（`cmd/tether/node_upgrade_wait_test.go`，当前红）：
```go
// A pre-upgrade-safety agent fills UpgradeForwardedResp.NewVersion with the FULL
// first line of `tether version` ("tether v0.5.0 (proto v2)") — the broker relays
// it verbatim. --wait must not require ReleaseVersion to equal that whole line,
// or a fully committed upgrade is reported ROLLED BACK and a canary aborts the fleet.
func TestWaitForUpgradeCommitToleratesLegacyUnnormalizedNewVersion(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	release := &atomic.Value{}
	release.Store("v0.5.0")
	stubNodeList(t, nc, "n1", release)
	old := upgradeWaitBudget
	upgradeWaitBudget = 8 * time.Second
	t.Cleanup(func() { upgradeWaitBudget = old })
	cmd, out := waitTestCmd(t)
	if err := waitForUpgradeCommit(cmd, nc, "lab", "actor-pub", "n1", "tether v0.5.0 (proto v2)"); err != nil {
		t.Fatalf("legacy NewVersion must not fail a committed upgrade: %v (out: %s)", err, out.String())
	}
}
```

**lane wire — W-4**（`internal/broker/join_version_gate_test.go`；注：核验证实 site#3 已有 N-1 用例，此测试的增量价值在 §21.4 指路与表驱动收口）：
```go
// origin: upgrade-safety plan §2 — N-1 window nail for the join gate
// (architecture §21.1 site #3): epoch equality is EXACT, not [N-1, N]. An
// acceptance window here is unreachable dead code until the dual-tree
// subscription of architecture §21.4 exists.
func TestJoinGateProtoEqualityIsExact(t *testing.T) {
	cases := []struct {
		name    string
		joiner  int
		wantErr bool
	}{
		{"exact_epoch_accepted", proto.ProtoVersion, false},
		{"previous_epoch_rejected", proto.ProtoVersion - 1, true},
		{"next_epoch_rejected", proto.ProtoVersion + 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := versionSkewRefusal(c.joiner, proto.ReleaseVersion, "br-nail", nil)
			if got := err != nil; got != c.wantErr {
				t.Fatalf("joiner proto=%d: refusal=%v, want %v — before widening this, read "+
					"docs/distributed-broker-architecture.md §21.4 (dual subject-tree duty)", c.joiner, got, c.wantErr)
			}
		})
	}
}
```

**lane concurrency — CL-1**（`internal/agent/upgrade_install_test.go`；核验注：helpers 已存在，`makeTarball` 需从 test/p10 内联）：
```go
// origin: upgrade-safety external review (concurrency lane) CL-1
func TestConcurrentUpgradeRequestsCannotClobberPrevSlot(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "tether")
	if err := os.WriteFile(dst, []byte("#!/bin/sh\necho 'tether v0.0.1 (proto v2)'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	origSHA := mustSHA(t, dst)
	// A slow `version` script widens the pre-marker window: the smoke gate
	// runs BEFORE the prev-slot dance, so both handlers pass the entry gate.
	slow := func(v string) []byte {
		return []byte("#!/bin/sh\nsleep 0.3\necho 'tether " + v + " (proto v2)'\n")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tb, _ := makeTarball(t, slow("v0.0."+strings.TrimPrefix(r.URL.Path, "/")))
		_, _ = w.Write(tb)
	}))
	defer srv.Close()
	a := testAgentFor(t, dst)
	a.cfg.UpgradeNoExit = true
	a.cfg.UpgradeURLAllowlist = []string{srv.URL}
	var wg sync.WaitGroup
	for _, p := range []string{"2", "3"} {
		tb, sum := makeTarball(t, slow("v0.0."+p))
		_ = tb
		body, _ := json.Marshal(proto.UpgradeForwardedReq{URL: srv.URL + "/" + p, SHA256: sum})
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.handleUpgradeForwarded(nil, &nats.Msg{Reply: "inbox", Data: body})
		}()
	}
	wg.Wait()
	// Invariant (plan §3.2 F6/F7): marker and prev slot must agree, and the
	// prev slot must still hold the pre-upgrade binary.
	m := readMarkerState(t, upgradeMarkerPath(dst))
	if m == nil {
		t.Fatal("no marker after installs")
	}
	prevSHA, err := sha256OfFile(upgradePrevPath(dst))
	if err != nil {
		t.Fatalf("prev slot unreadable after concurrent installs: %v", err)
	}
	if m.PrevSHA != prevSHA {
		t.Fatalf("marker.PrevSHA=%s but prev slot holds %s — a later rollback would be rollback_failed", m.PrevSHA, prevSHA)
	}
	if prevSHA != origSHA {
		t.Fatalf("prev slot lost the pre-upgrade binary (got %s want %s)", prevSHA, origSHA)
	}
}
```

**lane tests — TQ-1**（test/p10，无 broker，只有 watchdog 能解决 pending）：
```go
// origin: upgrade-safety plan §3.2 F5 — Run must ARM the watchdog; without a broker only it can resolve pending.
func TestUpgradeWatchdogRollsBackWithoutRegisterCommit(t *testing.T) {
	url := startNATS(t) // deliberately NO broker
	sandbox := t.TempDir()
	exePath := filepath.Join(sandbox, "tether")
	if err := os.WriteFile(exePath, []byte("NEW-BINARY"), 0o755); err != nil { t.Fatal(err) }
	if err := os.WriteFile(exePath+".prev", []byte("OLD-BINARY"), 0o755); err != nil { t.Fatal(err) }
	newSum, prevSum := sha256.Sum256([]byte("NEW-BINARY")), sha256.Sum256([]byte("OLD-BINARY"))
	marker := fmt.Sprintf(`{"state":"pending","prev_sha":%q,"new_sha":%q,"prev_version":"v0.0.1","new_version":"v0.0.2","deadline":%q,"boot_count":1,"boot_budget":3}`,
		hex.EncodeToString(prevSum[:]), hex.EncodeToString(newSum[:]), time.Now().UTC().Add(time.Second).Format(time.RFC3339))
	markerPath := filepath.Join(sandbox, ".tether-upgrade.json")
	if err := os.WriteFile(markerPath, []byte(marker), 0o644); err != nil { t.Fatal(err) }
	defer startAgentWithUpgrade(t, url, "lab", "lab-1", exePath, nil)()
	deadline := time.Now().Add(8 * time.Second)
	for {
		raw, _ := os.ReadFile(markerPath)
		var m struct{ State string `json:"state"` }
		if json.Unmarshal(raw, &m) == nil && m.State == "rolled_back" { break }
		if time.Now().After(deadline) { t.Fatalf("watchdog never rolled back; marker=%s", raw) }
		time.Sleep(50 * time.Millisecond)
	}
	if got, _ := os.ReadFile(exePath); string(got) != "OLD-BINARY" { t.Errorf("dst=%q, want restored prev", got) }
}
```

**lane tests — TQ-2**（p10；marker 必须在 agent 启动后写入）：
```go
// origin: upgrade-safety plan §3.1 — a stale pending marker (deadline passed, nobody owns it) must not block a retry.
func TestUpgradeProceedsOverStalePendingMarker(t *testing.T) {
	url := startNATS(t); db := openDB(t); pub, fp := freshUserPub(t); seedSession(t, db, "lab", fp)
	binBody := fakeTetherScript("v9.9.9-test"); tarball, sum := makeTarball(t, binBody)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(tarball) }))
	defer srv.Close()
	sandbox := t.TempDir(); exePath := filepath.Join(sandbox, "tether")
	if err := os.WriteFile(exePath, []byte("OLD"), 0o755); err != nil { t.Fatal(err) }
	defer startBroker(t, url, db, []string{srv.URL})()
	defer startAgentWithUpgrade(t, url, "lab", "lab-1", exePath, []string{srv.URL})()
	stale := fmt.Sprintf(`{"state":"pending","prev_sha":"aa","new_sha":"bb","prev_version":"v0.0.1","new_version":"v0.0.2","deadline":%q,"boot_count":0,"boot_budget":3}`,
		time.Now().UTC().Add(-time.Minute).Format(time.RFC3339))
	if err := os.WriteFile(filepath.Join(sandbox, ".tether-upgrade.json"), []byte(stale), 0o644); err != nil { t.Fatal(err) }
	nc, _ := nats.Connect(url); defer nc.Close()
	resp := upgradeRequest(t, nc, "lab", pub, "lab-1", proto.UpgradeReq{URL: srv.URL + "/tether.tar.gz", SHA256: sum, ProtoVersion: proto.ProtoVersion})
	if !resp.OK { t.Fatalf("stale pending must not block a retry; got %+v", resp) }
	if got, _ := os.ReadFile(exePath); string(got) != string(binBody) { t.Errorf("binary not replaced after stale-marker retry") }
}
```

**lane tests — TQ-3**（`cmd/tether/node_upgrade_wait_test.go`）：
```go
func TestWaitForUpgradeCommitFallsBackToAnyReleaseChange(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil { t.Fatal(err) }
	defer nc.Close()
	release := &atomic.Value{}; release.Store("v0.0.1-old")
	stubNodeList(t, nc, "n1", release)
	go func() { time.Sleep(time.Second); release.Store("v0.0.2-changed") }() // flips before the first 3s poll
	cmd, out := waitTestCmd(t)
	if err := waitForUpgradeCommit(cmd, nc, "lab", "actor-pub", "n1", ""); err != nil {
		t.Fatalf("empty NewVersion must commit on any release change: %v", err)
	}
	if !strings.Contains(out.String(), "release changed") { t.Errorf("missing fallback verdict: %s", out.String()) }
}
```

**lane ops — OPS-4**（`internal/agent/upgrade_test.go`）：
```go
// origin: upgrade-safety plan §3.1 — NewVersion must equal the new binary's ReleaseVersion (ctl --wait criterion).
func TestSmokeVersionParsesRealVersionLineFormat(t *testing.T) {
	// EXACTLY the first line cmd/tether newVersionCmd prints, rebuilt from the same SSOT constants.
	line := fmt.Sprintf("tether %s (proto v%d)", proto.ReleaseVersion, proto.ProtoVersion)
	bin := filepath.Join(t.TempDir(), "tether")
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' %q 'linux/amd64' 'go1.25.0'\n", line)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := smokeVersion(bin)
	if err != nil {
		t.Fatal(err)
	}
	if got != proto.ReleaseVersion {
		t.Fatalf("smokeVersion = %q, want %q — the --wait equality criterion is broken", got, proto.ReleaseVersion)
	}
}
```

**lane ops — OPS-5**（`cmd/tether/node_classify_test.go`）：
```go
// origin: upgrade-safety plan §4 — smoke_failed aborts the fleet; upgrade_in_progress is skipped.
func TestUpgradeSafetyCodesFleetClassification(t *testing.T) {
	// The wire shape is agent_rejected:-wrapped (internal/broker/upgrade.go:103); brokerErrorMessage strips it.
	for _, c := range []string{"smoke_failed", "agent_rejected:smoke_failed"} {
		if !isConfigError(brokerErr(c)) || isTransientError(brokerErr(c)) {
			t.Errorf("%s must abort --all (config), never be skipped", c)
		}
	}
	for _, c := range []string{"upgrade_in_progress", "agent_rejected:upgrade_in_progress"} {
		if !isTransientError(brokerErr(c)) || isConfigError(brokerErr(c)) {
			t.Errorf("%s must be skipped (transient), never abort the fleet", c)
		}
	}
}
```

另：TQ-5 的 smokeVersion 表驱动单测（含 `exit 1` 对抗用例）与 TQ-7 的并发 commit∥watchdog 测试为文字提议、无成文代码，随 S5/S21 修复时落地。

## 3. 被驳回的 finding 及驳回理由

无整条 REFUTED；以下为被核验驳回的**子论断**（宿主 finding 降级或收窄后存活于 §1）：

- W-4 的 "site#3 无 N-1 钉子、三站点变异仅 site#1 必红"：事实读漏——`join_version_gate_test.go:69` 主决策表已有 `proto one behind` 用例，放宽即红；只有 site#2 真无钉子。
- OPS-8 的核心场景前提 "金丝雀若因故 skip"：金丝雀结构上不可 skip（任何错误直接 abort），全队皆错的 sha 必被拦下 → 降 NIT。
- TQ-2 的 "stale pending 永久挡住重试"：agent 重启即由 decideBoot 收敛 + 启动时过期 pending 以 wait=0 立即触发，strand 仅限当前进程存活期 → 降 MINOR。
- CL-3 的根因归置（payload 只 marshal 一次）：即使每次重 marshal，在途请求仍产生同一假日志——是 R3 互斥固有的在途语义，且后续 register 会补送 rolled_back 纠正 → 降 NIT。
- OPS-2 的 MAJOR 定级：两个方向均只活在 `newVersion==""` fallback 分支，即 plan §10.1 已声明缓解的首跳过渡面 → 降 MINOR。
- CL-4 的 "磁盘满时回退大概率 rollback_failed"：rename(prev,dst) 是元数据操作通常成功，走的是 "restore landed but marker write failed" 分支而非 rollback_failed 终态（宿主 finding 本体成立）。
- FM-6 的 "site#1 论证完全缺失"：措辞修正——论证存在，只是落在 broker_test.go 测试注释而非 plan 指定的站点行。
- OPS-9③ 的 "≈10 MiB 措辞错误"：pending 窗口内 prev 已独占旧 inode，运维观感正确，仅漏提 tmp 副本。

## 4. 与 plan 的偏差清单（实现 ≠ plan，无论好坏）

1. plan §0"三站点只加 origin 注释钉住"：一处都没落在站点行（site#1 的论证在测试注释上方）。
2. plan §2"三站点表驱动钉死 N±1、失败信息指向 §21.4"：只有 site#1 全兑现；site#3 存量覆盖 N-1 但无 §21.4 指路；site#2 仅 `+99` 单向。
3. plan §3.1 R3"marker 状态转换收敛到单一 owner（mutex + 当前态检查）"：install 路径的 idle→pending 与 abort 改写全程在 owner 之外（S2）。
4. plan §3.1"NewVersion 归一化等式用测试钉住"：smokeVersion 零测试，docstring "pinned by test" 为虚（S5）。
5. plan §3.2 F5"nohup 三者皆闭合、回退不依赖 supervisor"：watchdog 回退 exec 失败分支 `os.Exit(1)` 依赖 supervisor（S12）。
6. plan §3.2 F8"断电打断任意步骤闭合"：全链路无 fsync，仅命名空间层成立（S13）。
7. plan §5"staged 行样例写进 usage.md 并被 e2e 断言"：样例格式代码产不出且零断言（usage.md 抄了 plan，实现漂移）（S16）。
8. plan §7 承诺的 e2e"F5 回退路径（register 阻断）"未交付；9 条变异清单不含"删 armUpgradeWatchdog"、"删 configUpgradeCodes 条目"、"三站点相等改区间（site#2）"等可存活变异（S7/S8/S15）。
9. plan §10.2 点名外审专查的 copyFile 降级分支覆盖：答案为否（S20）。
10. `internal/proto/messages.go:754` 注释"empty from a pre-upgrade-safety agent"与旧版实证行为（非空全文串）相悖（S3）。
11. 本增量改写 requirements §6.7 后，join 门的 release 硬拒、两处运维错误串、§21.1 盘点表、skew_test.go:93 注释均未随之更新，形成第 1/2 层文档与实现的四点错位（S10）。
12. 好的偏差：`upgradeWaitBudget` 提为 var 便于测试压短（plan 未要求）；install abort 时改写 marker 的自述场景注释（upgrade.go:525）比 plan 更细。

---

## 主进程处置（逐条）

| # | 处置 | 说明 |
|---|---|---|
| S1 | **采纳** | commit/report 判据加 `m.BootCount > 0`（install 写的 marker 恒 0，只有经 decideBoot 的 boot 才可能 commit），关死 flip→exec 窗口的伪 commit |
| S2 | **采纳** | install 全段加专用 `upgradeInstallMu.TryLock`（拿不到回 `upgrade_in_progress`）；install 内两处 marker 写入改走 `upgradeMu` 短临界区，与 watchdog/commit 真互斥 |
| S3 | **采纳** | ctl 侧对 NewVersion 做 legacy 归一化（含空格或不以 `v` 开头 ⇒ 视为旧 agent 整行文本，降级为 "" 走 fallback）；messages.go 注释改为实证行为 |
| S4 | **部分采纳** | 同 tag 重推时打印显著 ⚠ 并把确认降级为"staged-only"（不假报 COMMITTED），但**不** abort 扇出——同 tag 工件风险已被 sha256+冒烟兜住，硬拒会让合法的同版本重装永远不可用；报告建议的"拒绝放行"改为"告知后放行" |
| S5 | **采纳（根治版）** | 版本首行格式收敛为 `proto.VersionLine(release, proto)` 共享 helper（main.go 与冒烟测试同源）；补 smokeVersion 表驱动单测（含 exit-1 / 假前缀 / 空输出对抗例）+ 跨缝钉子 |
| S6 | **采纳（类裁 64）** | wait 超时/likely-ROLLED-BACK 属"重试同样结局、需要人换产物"，归 exitUsage(64) 而非核验建议的 75——75 会让自动化无限重推坏产物，正是 Y2 教训 |
| S7 | **采纳** | 落地报告提议的分类测试（含 `agent_rejected:` 包裹形状） |
| S8 | **采纳** | 补 F5 武装链路测试：Run + pending marker + 秒级 deadline + 不可达 NATS → watchdog 自回退、UpgradeExecFn 以恢复后的 dst 被调 |
| S9/S19 | **采纳** | wait 测试补 old→new mid-poll 翻转、`newVersion==""` fallback、`Status!=ONLINE` 分支 |
| S10 | **采纳（追认豁免）** | join 门的 release 相等硬拒是 §6.7 的**显式豁免**——joiner 是运维正在添置的机器（重装即匹配），拒绝不会卡住任何已部署节点的回滚；§6.7 补豁免句、§21.1 盘点表补 row、clusterstatus 两处错误串与 skew_test 注释改指现行条文 |
| S11 | **采纳** | baseline 改为 dispatch 前捕获并作为参数传入 waitForUpgradeCommit；lookup 失败打 warn 不再静默 |
| S12 | **采纳** | watchdog/boot 回退的 exec 失败分支不再 `os.Exit(1)`：dst 已恢复、marker 已 rolled_back，继续现进程运行（nohup 下保底不死；systemd 用户可手动重启进恢复后的 dst） |
| S13 | **采纳** | writeUpgradeMarker / extractTetherBinary / copyFile 三处 close 前加 Sync() |
| S14 | **采纳（修注释）** | 不停 watchdog 是对的（marker 仍 pending 时 watchdog 是唯一收敛者），把失实注释改为"deadline 可能先到——回退，方向安全" |
| S15 | **采纳** | 三站点行补 origin 注释；p10 的 site#2 用例表加 `ProtoVersion-1` 臂 |
| S16 | **采纳** | usage.md 样例改为代码真实输出（`✔ lab/n1: staged (→ v0.5.0, smoke ok); …`）；canary 成功测试断言 staged 行格式 |
| S17 | **采纳（文档）** | §21.2 明写 inventory 闸门边界=internal/proto 包内导出结构；join bundle 等包外 wire 由条文+审查纪律覆盖，不假称机械 |
| S18 | **采纳** | p10 补 stale-pending（deadline 已过）放行重试的测试 |
| S20 | **采纳** | copyFile 直测（内容 + 0755） |
| S21 | **采纳** | -race 下 commit ∥ watchdogRollback 真并发测试（互斥的变异验证载体） |
| S22 | **采纳** | broker-ops §8.7 移回 §8 块内 |
| S23 | **采纳** | broker 日志措辞降为 agent-reported |
| S24 | **采纳（注释）** | committed marker 保留是刻意的（运维痕迹 + 下次升级覆盖），写明 |
| S25 | **采纳（注释）** | bootstrap 分支注明依赖 git diff 兜底、收缩必须过 commit message |
| S26 | **采纳** | hint 补金丝雀例外半句 |
| S27 | **驳回** | `sha256_mismatch` 两集合皆非是**前置存量行为**（先于本增量），且核验已驳"金丝雀可 skip"前提；不在本增量动，如需归类另立小改 |
| S28① | **驳回** | wire code 用字面量而非常量是本仓既定风格（coverage gate 静态解析的前提），改常量反而制造 unresolved site |
| S28② | **驳回** | `--all` 强制金丝雀由 runUpgradeAll 的两个测试直接覆盖（该函数就是 RunE 主体） |
| S28③ | **采纳** | broker-ops §8.7 补一句下载期 tmp 副本的瞬时磁盘占用 |
| S29 | **部分采纳** | 补 upgradeRegisterReport 的 rollback_failed case；broker 日志行不上测试（log-only 观测信号，捕获 logger 的测试成本大于收益，记录在案） |
| S30 | **采纳** | poll 间隔提为 var；commit e2e 轮询预算放宽到 5s |

### 处置轮新增守卫的变异验证（第二批，M10–M14）

| # | 变异 | 载体测试 | 结果 |
|---|---|---|---|
| M10 | 删 S1 的 `BootCount == 0` 门 | `TestUpgradeRegisterReportTable/pending_bootcount_zero` | 红 ✓ |
| M11 | 整删 S2 install 互斥 | `TestUpgradeConcurrentInstallRejected` | 红 ✓（`2 OK / 0 busy`） |
| M12 | 删 Run 里的 `armUpgradeWatchdog` 调用 | `TestRunArmsWatchdogAndRollsBackAtDeadline` | 红 ✓ |
| M13 | 删 S3 legacy NewVersion 归一化 | `TestDispatchUpgradeNormalizesLegacyNewVersion` | 红 ✓ |
| M14 | 删 S4 same-tag 降级分支 | `TestWaitForUpgradeCommitSameTagDowngradesLoudly` | 红 ✓ |

### 闸门收尾（处置修复后的三道硬闸首轮红，全部处置）

| 红 | 根因 | 处置 |
|---|---|---|
| `make gates` 结构预算 | `Agent` 类型方法数 106→115（状态机 9 个新方法是真实子系统增长） | 手改 `structural_budget_golden.txt`（放宽必须手改 + commit message 说明，本行即预告） |
| `make gates` lint | gofmt 两文件未格式化 | `gofmt -w` |
| `make test` `test/security` | `TestUpgradeTarballMultipleEntriesOnlyTetherAccepted` 的 `echo ok` fixture 是冒烟门前时代产物 | 迁移为真实 version 行脚本（与 p10 fixture 迁移同类，审查 lane 漏扫了 test/security） |
| `make test` `test/d7` 30.7s 超时 | 满载 flake（单跑与终验均绿） | 观察项，不改 |
| e2e-parallel `TestRegisterProtoEqualityIsExact` | 钉子的 accept 臂没 seed session，D4/D5 集群分片模式下被 C.1 session 门先拒（普通模式恰好放行——两模式行为差异被钉子自己暴露） | 改用 `openDBWithSession`，普通/D4 两模式复验绿 |
| `make test` / e2e `TestForwardPayloadKeysAreWireFrozen` | `NodeRegisterReq` 两个新键触发 forward-envelope 冻结门 | additive 论证写进冻结清单注释后收录两键——从此对这两键的**改名**就是该门要抓的跨版本断裂 |

**过程教训（写给下一轮）**：P5 首轮"三闸全绿"的结论是**假的**——`make test 2>&1 | tail -8` 这类管道让
退出码变成 `tail` 的 0。上表六项红全部是被假绿掩盖过一轮的。跑闸门必须单独取每段退出码。

三条过程发现，如实记录：
1. M11 第一版测试走 broker 路径时变异**不红**——单 broker 对 `upgrade.req` 在**单订阅回调里串行**处理，
   hermetic 单 broker 结构上打不出并发；S2 防的真实场景是 **HA 集群**两台 broker 同时向一台 agent 转发
   （agent 侧 `dispatchForwarded` 每消息一个 goroutine）。测试改为绕过 broker 直打 forwarded 主题后变异才红。
   这同时订正 S2 的表述：单 broker 现网今天**打不出**这个竞态，它是集群面缺陷。
2. M11 第二版曾出现"错误原因的红"（agent 没有 broker 无法 register → 订阅未装 → no responders）——
   已修为 broker 在场但请求绕行，红的原因核实为计数断言（`2 OK / 0 busy`）。
3. TryLock 退化为 Lock（排队不拒绝）的变异**不红**：排队者随后被 pending 入口门拒绝，行为等价——
   互斥与入口门在此构成有效冗余，不是漏洞；TryLock 的价值是不让 loser 白等 ~35s。
