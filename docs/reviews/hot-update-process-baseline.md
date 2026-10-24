PRODUCT-RED — 原 runner rc=3；真实 H0 legacy process 基线复现，尚未达到热更新连续性。

## 运行绑定

- 命令：`IMAGE=tether-sim:hot-update-h0 SIM_EVIDENCE_DIR=/tmp/tether-control-recovery/hot-update-process-real ./local.sh drill 34-hot-update-process`；显式 `SIM_KEEP=0`，保留原 runner 自动 teardown。
- 实际用时 61.900s；drill 原 wall=58s。冻结 image ID：`sha256:8285559d7fa2bf2cbce942c0f24323a90c9d19e5e57e1b91bb3eadc0644abcd7`。
- 旧 vendor/running SHA：`4676bb8dba25eddd0809234def9784f490ccdeba88827849ba5f22c7886b2189`。
- 新 vendor/running SHA：`b66591bf5e587f76daf3f1a69d5e1e4ec8c4e08890c8fc226165b3d4e68a9f17`。
- Source manifest：`hot-update-process-real/build-source-manifest.json`，SHA `53d1f91d01f17a2a53f146c671d387f9cd279060868df8333dd973204a4a052d`。
- 以上绑定均在真实执行前独立核对。没有 restage、修改 vendor、源码、oracle，亦未绕过 image guard；harness 前后全部 SHA 一致。root 后续源码工作不改变本次冻结产物。

## 原判词

```text
DRILL-VERDICT verdict=PRODUCT-RED rc=3 assert_fail=0 setup_red=0 product_red=1 not_covered=0 nc_gap=0 nc_guard=0 pass=17 -- 34-hot-update-process (legacy real-exec continuity; expected PRODUCT-RED)
```

分类为 **产品旧升级路径的连续性缺陷**。17 项 setup 全过，故不归因于 fixture/环境失败；未把原 red 改成 green，未补发缺失字节、重启原业务或静默放宽 oracle。

## 原身份、实际 exec 与提交

- Agent 容器 PID **1223 → 1223**；systemd ExecMainStartTimestamp 两侧均 `Sun 2026-09-20 23:12:43 UTC`。
- `/proc/1223/exe` 从旧 SHA 变为上述新 SHA，candidate 与实际 running SHA 一致。真实 `node upgrade` rc=0；新 release `v0.0.0-simcluster-next` 已 ONLINE。
- Marker `state=committed`、`boot_count=1`、`target_sid=lab`、`target_nid=agt1`；原日志保留 `23:12:47.592Z ... re-execing into new binary` 与 `23:12:48.129Z ... upgrade COMMITTED`。
- 原 producer PID **1279**，PPID **1223**，start_ticks **18144079**，boot ID `cd09dd4f-c644-431c-9291-0d23db116793`，前/后/终态一致。
- Exec 完成后两次真实快照间 source 从 **117 → 140** 推进（约 1.465s）；原 child 当时仍 alive、finished=false。最终正常 stop，source count=149，producer 记录真实终态 code **23**。

## 原输出与管理结果

| 流 | 升级前已观察记录 | 最终 source 记录 | 原 ctl 收到 | 缺失记录 | source 编码字节 | ctl 实收字节 | 未交付字节 |
|---|---:|---:|---:|---:|---:|---:|---:|
| stdout | 36 | 149 | 57 | 92 | 18,962 | 7,229 | 11,733 |
| stderr | 36 | 149 | 56 | 93 | 18,962 | 7,102 | 11,860 |

- 两条实收流各自仍是 source 的精确前缀；stdout 到 seq56，stderr 到 seq55。两个原 pipe 都在 seq57 记录 **errno32 / EPIPE**。没有从 ledger 重建任何输出；“未交付字节”是独立 ledger 意图序列与实收 transcript 的差额，并不声称这些字节曾成功写入旧 pipe。
- 原 managed task ID `01m30hfsgnrefwj24j4rykytg0`：升级前 RUNNING，升级后错误记录为 **EXITED，exit_code=-1**，同时上面的原 OS child 仍存活产出。
- 正连续性 oracle原 rc=2，原错误：`HOT_UPDATE_EVIDENCE_INVALID: original live task lost management`。
- 采样截止时原 ctl 10s 内仍未退出，保存的 `final/ctl-outcome.json` 为 `{"finished":false}`，没有交付 producer 的真实23。
- 随后的身份受限 cleanup 才结束 ctl，最终 `ctl.rc=75`；这是清理后的结果，**不能替代测量时的未退出状态，也不是原任务正常退出码**。两者均原样留存。

## 证据路径

父目录：`/tmp/tether-control-recovery/hot-update-process-real/`。

- `runner.stdout` / `runner.stderr` / `runner.rc`：原 runner 输出、判词及 rc。
- `runner-start.json` / `runner-result.json`：真实启动/结束时间、命令和被冻结输入。
- `hot-update-process.1bY49s/`：before/post/final 独立 ledger、identity、原输出、ps、marker、升级和正oracle结果。
- 同目录四份日志分别为 `broker.slog`（7,326B）、`broker.boot`（1,687B）、`agent.slog`（2,982B）、`agent.boot`（266B），均非空，不互相替代。
- `observed-result-summary.json`：独立离线核对的计数、字节、SHA和身份摘要；未改原证据。
- `evidence-sha256.json`：本轮结束时证据文件摘要。

## 资源与边界

- 原 teardown 已执行。`resource-postflight.json` 在 **2026-09-20 23:13:58 UTC** 独立确认该 instance 的 containers、network、volumes 全为空；没有再手工删其他实例资源。
- 该结果建立真实 agent 同PID exec 的进程/输出/管理缺陷基线。没有测试 Caddy/WSS、PTY、原外部SSH socket、文件传输、回滚、NATS/broker升级、500ms/5s目标或全旧映像退出。
- 下一步产品修复应保留原 child wait/输出FD和管理归属，真实 same-PID exec后导入恢复，再让原连续性 oracle通过并按正式流程将 assert_bug晋升为正断言。不得通过保留旧错误row、旁路补输出或fixture重启业务修“绿”。
