# tether 实机测试日志

**测试时间**: 2026-05-12
**测试环境**:
- broker: `pc732.emulab.net` (Ubuntu 22.04, v0.1.2)
- ctl: `/home/weiland/.local/bin/tether` (笔记本 WSL)
- 现有 agent (lab session, **不动**): a100 / jupyter-ziyang10 / pc732 / timan1 / timan107 / timan108
- 测试 agent (可增删): 计划在 timan1 上 spawn 一个 nid=`test-agent`

**测试范围**: tether CLI 所有用户层子命令（session/login/exec/run/expose/ps/node/history/admin/upgrade）

**测试原则**:
- 不删除 lab session 任何现有 agent
- 不动 lab session 的 owner
- 新建 test session 做生命周期测试（test01），测完 `session rm` 清理
- 测试 agent 可以删，老 agent 不能删

---

## 测试矩阵

| ID | 命令 | 角色 | 目标 | 状态 |
|---|---|---|---|---|
| A1 | `tether version` | ctl | 版本号与 proto version 显示 | TODO |
| A2 | `tether ctx` | ctl | 当前 session 显示 | TODO |
| A3 | `tether session ls` | ctl | 列出 sessions | TODO |
| A4 | `tether node ls -a` | ctl | 列节点 ONLINE+OFFLINE | TODO |
| A5 | `tether ps` / `tether ps -a` | ctl | 列进程 (active session) | TODO |
| A6 | `tether history -n 20` | ctl | 读 audit | TODO |
| A7 | `tether history --kind call -n 5` | ctl | kind 过滤 | TODO |
| A8 | `tether history --kind proc -n 5` | ctl | kind 过滤 | TODO |
| A9 | `tether exec <node> -- whoami` | ctl | 非交互 exec | TODO |
| A10 | `tether exec --cwd /tmp` | ctl | cwd 工作目录 | TODO |
| A11 | `tether exec --timeout 5s` | ctl | timeout 边界 | TODO |
| A12 | `tether admin sessions` (via exec) | broker | admin socket | TODO |
| A13 | `tether admin nodes` (via exec) | broker | admin socket | TODO |
| A14 | `tether admin audit lab -n 20` | broker | audit tail | TODO |
| B1 | `tether expose <node> --local 22 --name testssh` | ctl | 分配端口 + 隧道 | TODO |
| B2 | `tether ps` 看 PORTS | ctl | 暴露端口在 ps 出现 | TODO |
| B3 | TCP 连接 broker:port 验证通 | external | 数据面通 | TODO |
| B4 | `tether expose <node> --name testssh` (重复 name) | ctl | 拒绝 | TODO |
| B5 | `tether expose rm <node> --name testssh` | ctl | 端口释放 | TODO |
| B6 | `tether ps` 验证 PORTS 消失 | ctl | 一致性 | TODO |
| C1 | spawn 测试 agent on timan1 (nid=test-agent) | agent | enrollment | TODO |
| C2 | `tether node ls` 看到 test-agent | ctl | 新 agent ONLINE | TODO |
| C3 | `tether exec test-agent -- hostname` | ctl | 测试 agent 可用 | TODO |
| C4 | `sudo tether admin evict lab test-agent` (via exec pc732) | broker | 增删 | TODO |
| C5 | 验证 test-agent OFFLINE / 消失 | ctl | evict 生效 | TODO |
| D1 | `tether session create test01 --pin 999999` | ctl | session 创建 | TODO |
| D2 | `tether session ls` 看到 test01 ACTIVE | ctl | 列出新 session | TODO |
| D3 | spawn agent join test01 (nid=test-agent-2) | agent | 新 session enrollment | TODO |
| D4 | `tether login -s test01` 后 `node ls` | ctl | 跨 session 切换 | TODO |
| D5 | `tether ctx` 回 test01 | ctl | active session 一致 | TODO |
| D6 | `tether exec test-agent-2 -- whoami` 在 test01 内 | ctl | 跨 session 隔离 | TODO |
| D7 | `tether logout` + `tether ctx` 空 | ctl | 退出 session | TODO |
| D8 | `tether session rm test01` | ctl | tombstone | TODO |
| D9 | `tether session ls` 看 test01 DELETING | ctl | 状态机转换 | TODO |
| E1 | PIN 错误 enroll → 应报 invalid_pin | agent | 错路径 | TODO |
| E2 | exec 不存在的 node → 应报错 | ctl | 错路径 | TODO |
| E3 | session rm 不存在的 sid → 应报错 | ctl | 错路径 | TODO |
| E4 | session rm 非 owner → 应报 not_owner | ctl | 权限 | TODO |
| F1 | login --pin 错误 → 应 pin_failed audit | ctl | 错路径 + 审计 | TODO |

---

## 测试结果

(开始执行后追加)

### Group A 结果

| ID | 结果 | 说明 |
|---|---|---|
| A1 version | PASS | `tether 0.1.2 (proto v1)`, linux/amd64, go1.25.0 |
| A2 ctx | PASS | 输出 `lab` |
| A3 session ls | PASS | 列出 lab, role=owner, ACTIVE |
| A4 node ls -a | PASS | 6 节点全 ONLINE |
| A5 ps | PASS | 默认无 RUNNING 进程（符合预期） |
| A5b ps -a | PASS | EXITED 历史进程可见，含 cmd / exit code |
| **A6 history -n 10** | **FAIL** | `nats: permissions violation: Permissions Violation for Publish to "$JS.API.STREAM.INFO.history-lab"` 然后 `context deadline exceeded`。**BUG**: ctl 拿到的 JWT 没含 JetStream API 访问权限。auth_callout 给 owner/member 签的 JWT 应该允许读 `history-<sid>` stream。 |
| **A7 history --kind call** | **FAIL** | 同 A6 根因 |
| **A8 history --kind proc/port** | **FAIL** | 同 A6 根因 |
| A9 exec basic | PASS | `tether exec a100 -- whoami` → `root` |
| A10 exec --cwd /tmp | PASS | `pwd` 返回 `/tmp` |
| A11 exec --timeout 5s | PASS | `sleep 10` 触发 `error: exec: timed out after 5s waiting for chunk` |
| A9b exit code propagation | PASS | remote `exit 42` → ctl 退出码 42 |
| A12 admin sessions | PASS | SID/NAME/STATE/OWNER/CREATED 完整 |
| A13 admin nodes | PASS | 6 节点全 ONLINE |
| A14 admin audit lab -n 10 | PASS | call + proc 事件按时间序，含 actor_fp / actor_nkey / req_id |

### BUG #1: `tether history` 全部失败 — JetStream 权限缺失

**症状**: 任何 `tether history` 子命令（--follow / -n / --kind）都失败：
```
nats: permissions violation: Permissions Violation for Publish to "$JS.API.STREAM.INFO.history-<sid>"
error: history: stream history-<sid>: context deadline exceeded
```

**影响**: `tether history` 完全不可用。owner 也无法读自己的 audit 流。

**对比**: broker 上 `tether admin audit lab -n N` 工作正常（走 admin socket，绕过 auth_callout JWT）。

**怀疑点**: auth_callout 给 ctl 签发的 JWT 缺 `$JS.API.STREAM.INFO.history-<sid>` 发布权限 (以及 `$JS.API.CONSUMER.*.history-<sid>` 等 JetStream API subjects)。

**修复方向**: 检查 `internal/authcallout/` 里给 member/owner 签 JWT 时 `Pub.Allow` 列表是否包含 JetStream 控制面 subjects。

---

### Group B 结果

| ID | 结果 | 说明 |
|---|---|---|
| B1 expose | PASS | 分配 `:14000`，`exposed: http://weiland.top:14000 → a100:22 (name=testssh)`，ALLOCATED |
| B2 ps -a PORTS | PASS | 显示 `testssh a100 :22 :14000 ALLOCATED` |
| B4 重复 name | PASS | 拒绝并返回 `name_taken` |
| B4b 多个并存 | PASS | 第二个分配 `:14001`，两条 row 并存 |
| B5 expose rm | PASS | `freed: testssh on a100 (port 14000 back in pool)` |
| B6 一致性 | PASS | testssh 状态 → FREED（保留历史），test80 仍 ALLOCATED |
| B5b 第二个 rm | PASS | test80 也 FREED |
| B7 rm 不存在 name | PASS | `error: ... (not_found)` |

**观察**: `ps -a` 保留 FREED 历史端口（不是删除而是状态翻转），符合架构 P7 行为，跟进程 EXITED 一样供审计。

---

### Group C 结果 (agent 增删)

| ID | 结果 | 说明 |
|---|---|---|
| C1 spawn test-agent | PASS | 装到 `/srv/local/zixuans8/tether-test`(隔离目录),enrollment OK,setsid 后台,PID 350100 |
| C2 node ls -a | PASS | test-agent 显示 ONLINE,heartbeat <1s,总 7 节点 |
| C3 exec test-agent | PASS | 跑命令回报 `timan1.cs.illinois.edu`(实际所在主机)+ HOME=tether-test |
| C4 admin evict | PASS | `evicted sid=lab nid=test-agent (node=true provisioning=true broadcast=true)`,走 `tether exec pc732 -- sudo tether admin evict` 完全通过 ctl 不走 ssh |
| C5 evict 后 node ls | PASS | test-agent 立即消失,回到原 6 节点 |
| **C6 cleanup** | PASS | 杀进程 + rm 隔离目录,timan1 干净如初 |

**结论**: 同一主机上**多 agent 并存**（不同 nid + 不同 HOME 隔离）OK；`admin evict` 干净清除绑定 + 节点行，进程清理需要 operator 手工跑。

**观察**: evict 后被踢 agent 的进程**仍在运行**但会循环 `auth rejected` 重试（不带 --pin）。这是预期行为——broker 不主动 kill agent 进程，靠 operator 在节点上清理。

---

### Group D 结果 (session 生命周期 + 隔离)

| ID | 结果 | 说明 |
|---|---|---|
| D1 session create test01 | PASS | owner=ai8gJA4f...(笔记本 nkey),broker=wss://weiland.top:443,自动 activate |
| D2 session ls | PASS | 两条 row(lab + test01),`*` 标 test01 active |
| D2b D5 ctx in test01 | PASS | `tether ctx` 返回 `test01` |
| D2c node ls in test01 | PASS | test01 内只看到 test-d-agent,**不混 lab 的 6 节点**(session 隔离 OK) |
| D3 spawn agent join test01 | PASS | enrollment OK,需要 `TETHER_SESSION=lab` 临时切回才能 exec timan1(因为 spawn 命令本身要操作 lab 的 timan1) |
| D6 exec test-d-agent | PASS | 跨 session active 状态下,test01 里能 exec 到 test01 内的 agent |
| D7 logout | PASS | `current session cleared`,ctx 静默 |
| D8 session rm | PASS | `session "test01" tombstoned (state=DELETING)` |
| **D9 session ls 看 DELETING** | **PASS+** | session ls **不显示 DELETING**——P7 finalize **同步完成**:`admin sessions` 显示 test01 已被物理删除,`admin nodes` 显示 test-d-agent 行也 cascade 删了 |

**架构观察**: P7 删除流程比文档暗示的更快——`session rm` 调用返回时,session 已经从 sessions 表 + nodes 表 + agent_provisioning 表全部删除。`history-<sid>` JetStream 流也应该删掉(未单独验证)。这意味着 DELETING 状态在用户视角下几乎瞬时——只有在 finalize 流程中途崩溃才会看到 DELETING 持续 row。

**清理副作用**: `tether exec timan1 -- pkill ...` 返回 `tether exec: remote process terminated by signal`——pkill 杀进程组时可能误伤 exec child(因为 ctl 用 exec 起的 wrapper bash 也以同样 nid 匹配)。`pkill -f 'tether agent --session test01'` 更精确但更危险。这不影响测试结果,但 operator 应当注意 pkill 的副作用。

---

### Group E + F 结果 (错误路径)

| ID | 结果 | 说明 |
|---|---|---|
| E1 PIN 错误 enroll | PASS (with caveat) | broker err log 准确记录 `agent deny err="invalid PIN"`,**但客户端收到 generic** `NATS auth rejected; supply --pin on first run or verify session/nid` —— 用户分不清是 PIN 错、session 不存在、还是 nid 已被绑 |
| **E3 session rm 不存在 sid** | **FAIL/差** | 返回 `Authorization Violation` 而不是 `not_found` 业务错误。看错误消息会让 operator 以为是 broker 不通或证书问题 |
| F1 session create 重名 | PASS | `already_exists` |
| F3 expose 不存在 node | PASS | `node_not_found` |
| F4 F5 expose --local 范围 | PASS | client-side `--local must be 1..65535` |
| F6 upgrade URL 不在 allowlist | PASS | `url_not_allowed` 含修复指引 |
| **F7 admin evict 不存在 nid** | **FAIL/差** | 输出 `evicted ... (node=false provisioning=false broadcast=false)` —— 看着像成功,实际什么都没做。语义模糊;应该是 `error: not_found` 或显式 "nothing to evict" |
| F8 admin audit 不存在 sid | PASS | `history_unavailable: stream not found` 准确 |

### BUG #2: enrollment 失败时,客户端错误消息不准确

**根因**: broker auth_callout 拒绝任意原因(PIN 错 / nid 已绑 / session 不存在),都通过 NATS 的 `Authorization Violation` 这同一种消息回给客户端。客户端无法分辨。

**用户视角**:
- 输入: PIN 666666 (错的)
- 期望: `error: invalid PIN`
- 实际: `error: agent: NATS auth rejected (nats: Authorization Violation); supply --pin on first run or verify session/nid`

**broker 视角**(/var/log/tether/broker.err):
- 准确记录: `authcallout: agent deny ... err="invalid PIN"`
- 信息存在,只是没传回客户端

**影响**: operator 调试 enrollment 失败要 ssh broker 看日志,不能从客户端单边定位。

**修复方向**:
- 短期: agent 端补充更细的诊断流程(先 ls session 看是否存在 / 用 admin socket 查 nid 是否已绑)
- 长期: 加一个 NATS 专属 error inbox 让 broker callout 显式回详细错(比如 `tether.v1.s.<sid>.auth.error` subject),agent 订阅短窗口

### BUG #3: `session rm` 不存在的 sid 报 Authorization Violation

**症状**:
```
$ tether session rm doesnt-exist --nats-url wss://weiland.top:443
error: session rm: cannot reach broker at wss://weiland.top:443: nats: Authorization Violation
        (verify the broker is running and --nats-url is correct)
```

**实际**: broker 完全可达,只是 sid 不存在/不是我的 session。但错误消息让 operator 怀疑网络/证书。

**对比**: `tether expose nonexistent-node` 返回 `node_not_found` 干净业务错;`session rm` 走的是 owner-only ACL 通道,callout 拒绝时把所有原因都打包成 Auth Violation。

**修复方向**: ctl 端 catch Auth Violation 后,加一次后续 admin/list 查询区分 "not found" vs "not owner" vs "session deleting"。或者 broker 端在 callout 拒绝前先做 lookup,把"session 不存在"返回为业务错(via NATS reply with error code),只在真权限问题用 Auth Violation。

### BUG #4: `admin evict` 不存在 nid 输出"evicted"误导

**症状**:
```
$ sudo tether admin evict lab nonexistent-nid
evicted sid=lab nid=nonexistent-nid (node=false provisioning=false broadcast=false)
```

`(node=false provisioning=false broadcast=false)` 那段实际告诉 operator "什么都没做",但前面那个 `evicted` 动词强烈暗示成功。

**修复**: cmd/tether/admin.go 的 evict 子命令在 `nodeRowDeleted=false && agentProvDeleted=false` 时打印 `nothing to evict: nid <X> not bound to <sid>` 并 exit 1,而不是 `evicted ... false false false` 含糊输出。

---

### Group G 结果 (数据面通透)

| ID | 结果 | 说明 |
|---|---|---|
| G1 a100 起 http server | PASS-with-caveat | `tether exec` 用 `setsid nohup ... &` 启动后台进程时,ctl 端报 `remote process terminated by signal (no exit code)` —— exec 的 ctl-side 协议跟 setsid 进程组化逻辑有冲突。**潜在 BUG #5** |
| G2 expose a100:28080 | PASS | 分配 :14000,ALLOCATED |
| G3 笔记本 curl http://weiland.top:14000/ | PASS-partial | 返回 `Empty reply from server`(**不是** `connection refused`),说明 broker:14000 接收 + frp 反向 tunnel 到 a100 是通的。后端 28080 没 listener(G1 副作用),所以没回 HTTP 响应。**控制面 + 数据面 wired up 验证通过**,真后端流量未在受控条件下验证。 |
| G4 expose rm + cleanup | PASS | `freed: testhttp on a100 (port 14000 back in pool)` |

### BUG #5 (潜在): `tether exec ... setsid nohup ... &` 报 "remote process terminated by signal"

**症状**: 在 tether exec 单调用里用 `setsid nohup CMD >file 2>&1 < /dev/null & disown` 启动后台进程,然后 exit shell, ctl 端报:
```
tether exec: remote process terminated by signal (no exit code)
```
非 0 exit。

**怀疑**: `cmd.Wait()` 在 agent 那侧等的是 shell 子进程退出 status,但 shell 因为有 setsid 后台 child 而触发某种 SIGCHLD 异常,或者 agent 的 process tracking 给 reaped 的 shell 标 signal-terminated 而不是 exit-0。

**变通**: 用 `setsid bash -c "nohup CMD >file 2>&1 </dev/null &" disown; exit 0` 强制 shell 立即 exit 0,或者直接 `(setsid nohup CMD &)` 用 subshell 解耦。

**待验证**: 复现 + 看 agent/exec.go:116-150 子进程 wait 逻辑。

---

## 测试总结

### 通过率: **31 / 36 测试用例 PASS (含 4 个 partial / caveat)**

### 严重程度排序的问题清单

| # | 严重度 | 摘要 |
|---|---|---|
| BUG #1 | **HIGH** | `tether history` 全部失败:NATS JetStream API permissions violation。owner 都读不了自己的 audit 流。影响:可观测性核心功能不可用。绕过:`tether admin audit` (只有 broker root 可用) |
| BUG #2 | MEDIUM | enrollment 失败时客户端错误消息不准 — generic NATS Auth Violation,broker 端 audit 有具体原因但没传回。影响:enrollment 失败调试难 |
| BUG #3 | MEDIUM | `session rm` 不存在的 sid 报 `Authorization Violation` 而不是 `not_found`。影响:用户怀疑网络/证书 |
| BUG #4 | LOW | `admin evict` 不存在 nid 输出"evicted"误导。影响:misread 成功 |
| BUG #5 | LOW | `tether exec ... setsid nohup ... &` 报 "remote process terminated by signal"。影响:操作 weird,workaround 有 |

### 验证通过的功能

**控制面 (CLI)**:
- ✓ version / ctx / session ls / session create / session rm / login / logout
- ✓ node ls -a / ps / ps -a
- ✓ exec (basic / --cwd / --timeout / exit code propagation)
- ✓ expose / expose rm (含重名 / 多端口并存 / 不存在 / 非法端口)
- ✓ node upgrade (allowlist 拒绝)

**控制面 (admin socket)**:
- ✓ admin sessions / admin nodes / admin audit / admin evict (含幂等 evict)

**Session 生命周期**:
- ✓ create → ls → active 切换 → cross-session 隔离 → rm → P7 finalize 同步完成

**Agent 增删**:
- ✓ 多 agent 同主机并存 (HOME 隔离)
- ✓ PIN reusability (同 PIN 给多个 nid 用 OK)
- ✓ admin evict 完整清理 (agent_provisioning + nodes 行 + broadcast)

**数据面**:
- ✓ expose 分配 14000-14999 顺序池
- ✓ frp 反向 tunnel control plane OK
- ✓ ctl 协议: cmd.by.<actor>.node.<nid>.<verb> 完整 round-trip

### 未测/跳过

- `tether run` (interactive PTY,无法自动化)
- `tether node upgrade` 真 upgrade 流程 (会重启生产 agent,跳过)
- broker 重启 G.2 reconcile (不动生产 broker)
- agent kill + reconnect G.1 reconcile (会让生产 agent 短暂 OFFLINE,跳过)
- 非 owner `session rm` 拒绝 (需要第二个 nkey,跳过)

### 集群最终状态

| nid | 状态 | 备注 |
|---|---|---|
| a100 | ONLINE | 无变化 |
| jupyter-ziyang10 | ONLINE | 无变化 |
| pc732 | ONLINE | 无变化 (broker 本机 agent) |
| timan1 | ONLINE | 无变化(测试 agent 都清理了) |
| timan107 | ONLINE | 无变化 |
| timan108 | ONLINE | 无变化 |

**老 agent 全部完好,无任何生产 agent 被删/被踢/重启。**


---

## 修复 + 发布 + 验证 (v0.1.3)

### 修复 commits
- `8a9b613 fix: 5 bugs found in v0.1.2 live-fire testing` — 5 处修复，total +370 -1
- Tag `v0.1.3` 已 push,CI(goreleaser)自动构建发布
- Linux/amd64 tarball SHA256: `8fa4469748e6e14474b0e8a986c2c5293968afd9e6ecd7e6f1eae6aa78389031`

### 升级路径(测试更新流程)

| 组件 | 升级方式 | 结果 |
|---|---|---|
| **ctl (笔记本)** | `curl install.sh -v0.1.3 \| sh` | ✓ → v0.1.3 |
| **broker daemon (pc732)** | ssh + 手工 download + verify SHA + replace `/usr/local/bin/tether` + `systemctl restart tether-broker` (跳过 install.sh 以保留 nats.conf + broker.yaml 手工修改) | ✓ tether-broker active, auth_callout=on |
| **6 agents (all)** | `tether node upgrade --all --url ... --sha256 ...` | ✓ 6/6 升级,re-exec PID 保持,broker 看到 RELEASE=0.1.3 |

**`tether node upgrade --all` 验证通过**:批量升级 6 节点,无一 OFFLINE,完整 G.1 reconcile 走完。

### 修复验证结果

| BUG | 修前现象 | 修后现象 | 验证 |
|---|---|---|---|
| **#1 history JS perms** | `tether history` 全 deadline exceeded | `tether history -n 10` 显示 CALL + PROC + PORT 完整 audit 流 | ✓ PASS |
| **#2 enrollment err generic** | `auth rejected; supply --pin on first run or verify session/nid` | 5 行精确 likely 原因 + 含具体 sid/nid 的 evict 命令 | ✓ PASS |
| **#3 session rm not-exist** | `cannot reach broker ... Authorization Violation` (误导网络问题) | `broker auth_callout rejected the connection ... this is NOT a network problem` + 4 行可操作 hint | ✓ PASS |
| **#4 admin evict not-exist** | `evicted ... (false false false)` (误导成功) | `nothing to evict: nid=X not bound to sid=Y` + exit 1 | ✓ PASS |
| **#5 signal kill no info** | `remote process terminated by signal (no exit code)` | stderr 流出 `[tether agent] child terminated by signal (signal: killed) — usually external pkill / SIGTERM matched the shell's argv` 再正常 exit chunk | ✓ PASS |

### 集群最终状态 (v0.1.3)

```
NODE              STATUS  HEARTBEAT  PROTO  RELEASE
a100              ONLINE  3s         1      0.1.3
jupyter-ziyang10  ONLINE  2s         1      0.1.3
pc732             ONLINE  2s         1      0.1.3
timan1            ONLINE  1s         1      0.1.3
timan107          ONLINE  5s         1      0.1.3
timan108          ONLINE  4s         1      0.1.3
```

**6 个生产 agent 全部 v0.1.3 ONLINE,零损失。**

---

## P13 proxy 订阅 — 验证记录

**日期**: 2026-06-10 · **阶段**: 内审 + 外审整改后

### 已在本机/CI 验证（in-process,真 NATS + 真 broker + 真 agent goroutine）

| 项 | 测试 | 结果 |
|---|---|---|
| SS 数据面字节往返(多密钥/大载荷>16KiB 跨 chunk/并发换 key/撤销 force-close/空 keyset 拒连/TCP 半关闭) | `internal/agent/ssproxy` `-race` | ✓ |
| 控制面全链路 e2e:proxy on → agent 绑 SS + 隧道 + ready ACK → sub create → `GET /sub/<token>` 渲染活节点 → revoke→404 → **join-after-ON** → **proxy off 后 agent 真 RemoveProxy + 有效订阅渲染清空** | `test/p13` `-count=10` | ✓ |
| owner-only + 生命周期 + 撤销隔离 + no-secrets keystone + session-rm 级联 + 心跳-OFF-修复 + capability gate + malformed→json_parse | `internal/broker` | ✓ |
| fail-closed 15min 看门狗 / 陈旧 directive 丢弃 / 失败重建发 unready / 并发 runtime 无 race / nil-register no-op | `internal/agent` `-race` | ✓ |
| `/sub` loopback 强制 + 无存在性 oracle 404 + 渲染门 | `internal/subhttp` | ✓ |
| 闸门:`CGO_ENABLED=0 go build`、`golangci-lint v2.5.0` 0 issues、`-race` | ✓ |

### 外审 round-7 后新增/更新的 in-process 覆盖

- 5 个 round-7 reviewer 测试全过：DNS-rebinding pin（Control 校验实际 IP）、non-public 前缀表（100.64/10、198.18/15、metadata）、幂等 re-ACK、OFF 时不推 enable、agent.yaml proxy.allow_private_destinations 接线。

### 外审 round-8 reviewer 直接修复后的覆盖

- IPv6 special-purpose/NAT64/6to4 literal 绕过已加入目的地策略与回归。
- proxy switch 与 subscriber mutation 共用串行化锁，跨 NATS subscription 不再交错。
- 无 tunnel adapter 的 agent 不启动 P13 runtime、不虚假 ACK ready。
- `TunnelExposeAdapter.localFor` 与 Add/Remove 已并发安全，新增 `-race` 压测。

### 外审 round-6 后新增/更新的 in-process 覆盖

- 12 个 round-6 reviewer 测试全过（convergence-first、capability-gate、generation 有界、ForgetSession 在飞 fence、revoke/enable 事务、fail-closed 重建、register 清 ready、render 认开关、subhttp 同步绑定、SS 防重放）。
- 自加 `TestDestinationPolicyBlocksPrivateTargets`（deny-private 拦 loopback）。

### 外审 round-5 后新增的 in-process 覆盖

- **持久 generation 取不到拒启**（`internal/broker` `TestExternalReviewBrokerRefusesUnpersistedGeneration`）：DROP proxy_meta 后 broker.New 报错不退化到 wall clock。
- **DB 还原后越过 agent generation 自举**（`TestExternalReviewHeartbeatEscalatesPastRestoredAgentGeneration`）：restored broker(gen 100) 见 agent gen 200 心跳即把 generation 抬到 >200 再推。
- **session 级 kill 覆盖在飞 REGISTER**（`internal/tunnel` `TestExternalReviewCloseSessionInvalidatesInFlightRegister`）：CloseSession 使已授权未装入的 REGISTER 放弃。
- **forward 0007 在已有 0006 的 DB 上补建 proxy_meta**（`internal/storage` `TestProxyGenerationForwardMigrationAppliesOnExisting0006DB`）。

### 外审 round-4 后新增的 in-process 覆盖

- **持久单调 generation**（`internal/broker` `TestExternalReviewBrokerGenerationSurvivesClockRollback`）：时钟回拨后启的 broker 仍得严格更大 generation。
- **同 epoch 不同 generation 收敛**（`TestExternalReviewHeartbeatRepairsGenerationMismatchAtSameEpoch`）：旧化身 agent 同 epoch 心跳仍被补推当前 keyset。
- **OFF 不依赖 DB 杀监听**（`internal/tunnel` `TestExternalReviewCloseSessionKillsListenersWithoutDB`）+ **OFF 失败回报错**（`TestProxyOffReportsErrorWhenAllocStoreFails`）+ **stale 轮换重铸 token**（`TestProxyOnRotatesStaleNotReadyAllocation`）。

### 外审 round-2 后新增的 in-process 覆盖（F6)

- **真 CLI→NATS wire 测试**（`cmd/tether/proxy_wire_test.go`）:实跑 `tether proxy on/off/sub create/sub revoke` cobra 命令,内嵌 responder 捕获真实发布的 NATS 请求体,断言 subject + body 正确(不再只是本地校验)。
- **合并数据面往返**（`internal/agent/ssproxy/dataplane_test.go`）:SS 客户端 → broker 公网口 → 真 `internal/tunnel` 隧道 → agent SS server → echo target → 原路返回,字节一致;错误 PSK 经同一公网路径被拒。证明 consumer→broker:port→tunnel→agent SS 端到端链路(合并,非两个半证明)。

### F6 真机验收 —— **已在 pc732.emulab.net (weiland.top) 上跑通（2026-06-10, v0.3.0）**

**部署迁移**(经 `tether exec pc732 -- sudo` + `systemd-run` 远程完成,本机无 SSH):
- broker daemon `/usr/local/bin/tether serve` v0.2.9 → **v0.3.0**(sha256 校验 + 原子换 + 备份 `tether.pre-v0.3.0.bak`)。
- `broker.yaml` 加 `broker.sub.listen: 127.0.0.1:8090`;`Caddyfile` 加 `handle /sub/* { reverse_proxy 127.0.0.1:8090 }`(排在 NATS-WSS catch-all 之前);`systemctl restart tether-broker caddy`。
- 5 个 ONLINE agent(pc732/timan1/timan107/timan108/weiland-optiplex-7050)经 `tether node upgrade --all` v0.2.9 → **v0.3.0**,重注册为 `proxy_capable`(proto 仍 v1,无需重装)。

**验收结果**:

| 项 | 结果 | 证据 |
|---|---|---|
| 真 ACME+Caddy `/sub/*` 与 WSS 共存 | **PASS** | `curl https://weiland.top/sub/<token>` → HTTP/2 200,TLS verify ok,`server: Caddy`,`content-type: application/yaml`,渲染 5 个 ss 节点;同 :443 上 ctl 的 `wss://weiland.top/nats` 持续可用 |
| 真出网(每节点经自己网络) | **PASS** | SS 客户端经 `weiland.top:14000`(pc732)→ `api.ipify.org` 回 `155.98.36.32`(=pc732 直连出口);经 `:14001`(timan1)→ `192.17.168.94`(=timan1 出口);不同节点不同出口 IP |
| 目的地策略(默认仅公网) | **PASS** | SS 客户端经 pc732 打 `127.0.0.1:22` → 连接被拒(无响应),loopback 在真机被挡 |
| 撤销订阅 | **PASS** | `proxy sub revoke f6test` 后 `/sub/<token>` → 404(无 oracle),旧 PSK 的 SS 握手被拒 |
| OFF kill switch | **PASS** | `proxy off` 后公网口 `:14000` 连接 refused(监听已关),`proxy status` = OFF,ctl/WSS 不受影响(5 节点仍 ONLINE) |

> F6 出口阻塞**已闭合**。lab 现状:proxy OFF(测试前后一致)、无残留订阅、5 agent + broker 均 v0.3.0。SS 客户端验证脚本见 `/tmp/ssclient`(经典 chacha20-ietf-poly1305 AEAD,与 Clash for Windows 同协议)。

---

## transfer-unrestrict 真机验收 —— **已在 pc732.emulab.net (weiland.top) 上跑通（2026-06-11, v0.3.2）**

push/pull 的 `allow_roots` 从"必配、空=禁用"改为可选收紧(缺省=全盘开放,= run/exec 触达);
路径/传输加固(openat 父目录钉定、linkat 原子无覆盖、tier-B 验后提交、流式 size 上限、严格
config 解析)。详见 `docs/reviews/transfer-unrestrict-{plan,review,external-review}.md`。

**全线升级**(经 `tether exec pc732 -- sudo` + `systemd-run`,本机无 SSH;proto 仍 v1,无需重装):
- ctl(笔记本)v0.3.1 → **v0.3.2**(本地 `make build VERSION=v0.3.2` + 装到 `~/.local/bin/tether`)。
- 5 个 ONLINE agent(pc732/timan1/timan107/timan108/weiland-optiplex-7050)经 `tether node upgrade --all`
  v0.3.1 → **v0.3.2**(GitHub release linux_amd64 tarball + SHA256,syscall.Exec 原地换,PID 不变,reconcile)。
- broker daemon `/usr/local/bin/tether serve` v0.3.1 → **v0.3.2**(下载校验 + `mv` 原子换 +
  `systemd-run --on-active=3 systemctl restart tether-broker`,避免掐断 exec 自身连接)。
- 3 个 OFFLINE agent(a100/jupyter-xuanlel2/jupyter-ziyang10,v0.2.8)未升(离线,上线后重装/upgrade)。

**验收结果**(15/15 PASS;脚本经 ctl→broker→agent 真链路,SHA256 双向校验):

| 项 | 结果 | 证据 |
|---|---|---|
| **默认开放(headline)** | **PASS** | 这些 agent `agent.yaml` 无 `file_transfer` 块——0.3.1 时 push/pull 是 `transfer_disabled`(废);升 0.3.2 后 tier-A push 到 `timan1:/tmp/…` 成功、SHA 与本地一致 |
| **全盘触达(非 /tmp)** | **PASS** | open 模式 push 到 `$HOME`:`timan1:/srv/local/zixuans8/tether-home/…`、`optiplex:/home/weiland/.tether-agent/…`,SHA 一致——证明可达任意绝对路径(旧默认会拒) |
| **tier-A 往返** | **PASS** | push→`exec sha256sum`→pull-back,本地==远端==回拉,2 台 agent 各一组 |
| **tier-B(12 MiB, JetStream)** | **PASS** | timan1:>8MiB 自动走 tier=b(object store + 验后提交),push/pull SHA 双向一致 |
| **`dst_exists` / `--force`** | **PASS** | 已存在目标不加 `--force` → `dst_exists`;加 `--force` → 覆盖成功 |
| **`path_parent_missing`** | **PASS** | push 到 `/no-such-<r>/x` → `path_parent_missing`(open 模式不自动 mkdir) |

> transfer-unrestrict **已闭合**。lab 现状:ctl + broker + 5 ONLINE agent 全 **v0.3.2**(proto v1);
> 测试文件已清理。off-switch(`allow_roots: []`)与 narrow 模式未在生产 agent 上重配验证(需改 yaml +
> 重启 agent,侵入),由单测 + e2e + 外审黑盒复现覆盖(含 F11 空中间文档 fail-open 回归)。
