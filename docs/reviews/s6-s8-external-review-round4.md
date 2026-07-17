Fail

# S6-S8 外部重审报告（round-4，审查快照）

日期：2026-07-15
基线：round-3 已暂存内容
对象：开发者回复“整体 remediation 一次落地”后的未暂存修改

## 结论

本轮不能通过。开发者确实补上五态 producer、runner 分类、API 参数校验、两份 hermetic test，并把九个
drill 的裸 warning 迁成 `not_covered`/`product_red`；这些是有效进展。但“B1/B2、round-1 全部 finding
均 CLOSED”“NO BLOCKERS”的结论不成立：runner 不执行其宣称的完整 grammar，能把畸形/重复 verdict
判成 GREEN，并能通过自动重试把首次 `VERDICT-RC-MISMATCH` 洗成 ALL GREEN；更根本地，开发者未经
owner 授权，把 PRODUCT-RED 和 INCOMPLETE 一律定义成 exit 0，导致所有未完成 locked cell 和已复现产品
缺陷默认不阻断 landing。

九个目标 drill 都仍缺少原审查指定的核心闭环；开发者远端表本身也显示九项无一 GREEN，却被新的宽松策略
解释成“零 blocker”。这是验收口径弱化，不是 finding 关闭。本报告时点保持 Fail。

## 修改边界

开发者未暂存 diff 共 18 个已跟踪文件，699 insertions / 234 deletions，另新增：

- `test/simcluster/tests/verdict-contract-test.sh`
- `test/simcluster/tests/lint-drills.sh`

范围包含 assert harness、runner、九个目标 drill、README/roadmap/inventory/plan/review/gotcha ledger，以及
向 round-3 报告追加的开发者回复。本 tasklist 与本报告属于外审产物。

## Blockers

### B1 — 未经授权把 PRODUCT-RED/INCOMPLETE 默认改成可发布，验收口径被实质弱化

`lib/assert.sh:27-40` 和 `run-drills.sh:203-238` 把已知产品缺陷与未覆盖 locked cell 都定义为“不 fail
suite”；只要没有 harness/setup 错，九个 drill 即使全部 PRODUCT-RED/INCOMPLETE，suite 仍 exit 0 并打印
`NO BLOCKERS`。round-2 回复曾明确说 NOT-COVERED 阻断 landing；本轮没有 owner waiver、批准记录或按项
disposition，却直接废止旧规则。

严格门禁下默认应 fail closed：PRODUCT-RED 和 INCOMPLETE 都阻断；如 owner 确需临时接受，必须用显式、
可审计的命令行 waiver 分别放行，且摘要不得称为 ALL GREEN/NO BLOCKERS。把“暴露得诚实”与“允许上线”混成
一个 exit code，会让尚未实现的测试主题脊永久通过 CI。

### B2 — runner 未校验完整 grammar、唯一性和枚举，可稳定制造假 GREEN

producer 承诺每个 drill **恰好一次**输出完整行：

```text
DRILL-VERDICT verdict=<ENUM> rc=<n> assert_fail=<n> setup_red=<n> product_red=<n> not_covered=<n> pass=<n> -- <name>
```

实际 parser `run-drills.sh:140-152` 只分别 grep verdict 前缀和 `rc` 前缀，并取最后一条；它不验证完整字段、
非负计数、允许枚举、verdict/rc 对应、计数推导、` -- `、唯一性，也不保证 verdict 与 rc 来自同一完整行。
独立合成 drill 证实：缺 `rc` 的 `GREEN` 行和两条互相冲突的 verdict 行均被汇总为 GREEN，suite exit 0。
这直接推翻开发者的 B1 CLOSED 声明。

### B3 — 自动重试可洗掉首次契约 blocker

`is_flake` 在 `run-drills.sh:158-160` 把 `VERDICT-RC-MISMATCH` 纳入可重试态。合成 drill 首跑输出
GREEN/rc0 行但进程 rc1，同时带 `container not running`；重试输出合法 GREEN。runner 虽保存
`attempt1`，最终仍打印 ALL GREEN 并 exit 0。保存证据不能替代门禁：契约矛盾是 harness blocker，绝不能
因日志碰巧含 infra 关键字而自动洗白。

## Major findings

### M1 — 22/#35 没有证明自动 crash-loop，round-1 M5 仍 OPEN

实现只在固定等待前后各取一次 MainPID，并在末尾取一次 `NRestarts`；没有证明至少两次**自动**重启、每次短
生命周期、每次由 startup-JS 原因触发，也没有覆盖完整观察窗持续证明 DRY 不会进入 destructive path。
第二个 broker 的死亡仅 `warn`，无效 fixture 仍可继续。当前证据最多支持 #35 candidate，不能关闭原 finding。

### M2 — 40 仍缺 renderer 正臂和多项 locked cell，且 #45 因果签名过宽

40 仅验证缺参 refusal 与 `nats.conf` md5，不覆盖全参 issuer/nkey renderer、footer、`.bak` 集合/mtime；
OPS abort/confirm、ADD dry-run、APPLY plan、mid-retire 仍直接 `not_covered`。`op-started` 匹配包含宽泛
`retir|removed`；#45 的 shell 自行输出 `STALLED not RETIRED`，签名又接受 `stalled|not RETIRED`，任意未知状态
都可能被包装成已登记缺陷，未锁定真实失败原因。

### M3 — 41 的逐次退役、JS reset 与 tier-B 仍可假通过

两次 retirement 都只 poll voter `<3`，第一次降到 2 后第二次可真空通过；没有每次 before/after delta 和
terminal operation identity。tier-B 用 `head -c 8000000`，小于 8 MiB（8,388,608），与 round-1 finding
相同。JS store reset 仍 `... || true`，之后只断 broker active；restart 内层也可能在 restart 失败时因配置
不存在而成功。没有 `tier=b` 分类证据。

### M4 — 42 的 namesake 诊断失败仍被降级为 coverage gap

round-1 要求 returning-node namesake diagnostic 失败即 RED；实现仍 `not_covered`，且不校验 source broker、
report timestamp、`LeaderContactStale`。dead-peer diagnose 通过管道 grep，命令非零但打印目标文本也可通过；
“roster unchanged”只检查 voter count `>=1`，未比较前后 roster/N=2/leader。JS reset 仍屏蔽失败，E/F/I
仍未实现。

### M5 — 43 未实现 outcome-(b) live-data migration，rollback 不完整

drill 仍走 candidate outcome-(c)，没有真实 live rows/business object；所谓非交互 cutover 实际调用带 PTY 的
`simcluster init`。broker/DB/secrets 前置失败多处只 warn。rollback 未恢复 `nats.conf`、未以同一 sentinel
复验，cluster-off 也只看 `broker.yaml`；“broker active”不足以证明可用 standalone。核心迁移 seam 仍是
`not_covered`，round-1 M2 未关闭。

### M6 — 90 的 alert presence/absence 不是同一 JSON 快照，node scope 可错配

`_bd_absent`/`_dp_absent` 先用一次查询证明 JSON 有效，再对第二次查询执行 `! ... | jq`；第二次传输失败可被
误判为 absence，不满足同一捕获文档 fail-closed。`_dp_present` 接受 `dedup_key==$node OR kind==disk_pressure`，
其他节点任一 disk alert 即可通过。返回态只验 broker_down absent，未原子断言 `VOTER && absent`；磁盘填充
失败只 warn、restart 被屏蔽、未证明 >80%，N=2 below-quorum leg 仍未覆盖。

### M7 — 91 的三个 locked cell 未交付，fixture 还会重复 publish

D 自动 failover、A3 retire 后 seed drop、anchor torn-manifest 均仍 `not_covered`。peer death、force-single、
JS reset 多处 warning/`|| true`，不足以构成因果 fixture；drill 的诊断 publish 加断言 publish 实际执行两次，
与“一次 manual publish”要求冲突。round-1 B1 未关闭。

### M8 — 92 的自然 quorum-loss、write-path、tier-B/recovery 仍 fail-open

自然 quorum-loss 没有硬断 exit 2，只 poll 文本。`--ack-alerts` 三分支对未识别错误直接计 PASS，没有正向证明
已到达 write path；auth/not-found 之外的失败可假通过。tier-B 只断 push 失败，不锁 JS-503 原因；恢复复用
可能已删除 SID、login 失败被屏蔽，且没有同一业务对象、banner 自动清除和 `tier=b` 证据。mutex 只要任意
非零即通过。

### M9 — 93 的 HTTP/webhook/card/watch/exit-code oracle 都不满足原要求

HTTP status 与 body 来自不同 curl 请求；health assertion 还以 readyz 作为前置。webhook 只 grep sentinel，
没有 exact schema/transition，no-secret 是 blacklist 而非 key whitelist；cleared 只断日志行增长，未证同 alert、
`transition=cleared` 或 leader pin。CARD 与 JSON 分开采样，仅检查字段存在而非同一 health/exit 镜像。
WATCH 仍非 PTY 并 `not_covered`；all-down 允许“rc2 **或** 任意 rc+unreachable 文本”，没有精确 exit 2。

### M10 — lint/tests 自证了宽松契约，无法兜住上述规避

contract test 没覆盖缺字段、重复/冲突行、未知 enum、计数与 verdict 不一致、契约 mismatch+flake 重试；因此
真实 parser 的假绿未被发现。lint 只是窄 regex：九文件虽报 clean，仍有大量 `|| true`、warning prerequisite
和 bundled locked cells；它只把其他 drill 的裸 NOT-COVERED 列为 advisory，也不能证明批次语义闭环。

## Finding 闭环状态

| Finding | round-4 状态 | 依据 |
|---|---|---|
| round-3 B1 | PARTIAL / OPEN | producer 有五态行；parser 不验证完整 grammar/唯一性，可假 GREEN |
| round-3 B2 | PARTIAL / OPEN | 九 drill 已迁移计数 API；原 locked cells 大量仍缺，lint 可绕过 |
| round-3 M1/m1 | CLOSED | 缺/空 desc/sig/gotcha/cmd 已在读参前 fail-closed，三壳矩阵通过 |
| round-3 M2 | OPEN | 分类已接线，但 owner-policy 无授权且默认放行 PRODUCT-RED/INCOMPLETE |
| round-1 B1 | OPEN | 40/42/43/91/92/93 的主题脊仍可 `not_covered` 后 suite exit 0 |
| round-1 M1-M5 | OPEN | 92/41+43/93/90/22 的核心 oracle 未实现或仍可假通过 |
| round-1 M6 | PARTIAL / OPEN | 编号大体统一；SSOT 的“全部关闭/G-B landing”与实现不符 |
| round-1 m1 | OPEN | 40 full renderer 与 plan/apply/abort/mid-retire 未实现 |

## 独立验证证据

- Scope：18 个 tracked 文件，699+/234-；2 个新测试文件。
- 静态：`git diff --check` 通过；runner 用 `bash -n`，POSIX harness/drills/tests 用 `sh -n`、`dash -n` 通过。
- Contract：`sh`、`dash`、`bash tests/verdict-contract-test.sh` 均通过其现有断言。
- Lint：`sh tests/lint-drills.sh --all` 对九文件返回 0，但同时列出 legacy advisory，且上述语义漏报仍存在。
- 对抗 runner：缺 rc 的 GREEN、重复冲突 verdict 均被判 GREEN/exit0；首次 mismatch+flake、重试 GREEN 最终
  ALL GREEN/exit0，首次日志在 `/tmp/s6-s8-round4-retry/*.attempt1.*`。
- 远端：只读检查 weilandserver；当前无开发者声称运行的 durable log/attempt 文件，也无容器、sim/drill
  network 或 volume 残留。清理状态可确认，九次运行结果与 SHA 表因证据已删除而不可独立复核。

## 疑惑与建议

1. 谁以何种可审计方式批准了 PRODUCT-RED/INCOMPLETE 默认 exit 0？若没有批准，应恢复默认阻断并只允许显式
   waiver；waiver 还应打印独立 `WAIVED` 摘要。
2. 为什么报告把九项全部非 GREEN 称为“G-B landing”？若 landing 指测试实现合入而非产品发布，请在 SSOT
   区分 test-harness readiness、coverage completeness、product readiness 三个维度。
3. `not_covered` 应只允许非主题脊 follow-up；主题 locked cell 未实现应直接令验收失败，而不是自动获得
   “expected”身份。
4. 建议 parser 对整行做单一 anchored match，强制 exactly-one、枚举/rc/计数一致；任何契约错误永不重试。
5. 远端验证应保留不可变 summary、逐 drill log、attempt、命令行与 source SHA，至少保留到外审结束。

## Release disposition

Fail。可以保留参数校验与五态表达，但不得接受当前默认放行策略、“全部 finding CLOSED”或 G-B landing
stamp。先修 runner 假绿和门禁，再逐个实现原 locked cells、强化 hermetic tests，并用可留存日志完成远端复跑。
