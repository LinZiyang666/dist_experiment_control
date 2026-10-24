Caddy profile PASS；额外 legacy Python ingress 冷 return 链路 FAIL（已定位 fixture netns 失配）。原 rc 分别保留，未合并成总绿。

# 供给及执行边界

- 冻结镜像 `tether-sim:hot-update-caddy`，ID `sha256:a5f3d4680edd205eee665de885e3623379d479611e766d405c8d9d5c3d1438ce`。
- Caddy **2.7.6**，binary SHA `db3bfc85bb93160f60fa6df9c3ebf2340dc11740e9a52c717d88a14c0430f229`；官方 archive SHA256 `99587cf77c595f0bf62cc23c9ea101f9879fa016c7b689d498ce054904821f22`；真实 checksum 表使用 SHA512，完整记录于 vendor/caddy-source.json 和每次smoke副本。
- tether沿用H0冻结SHA `4676bb8dba25eddd0809234def9784f490ccdeba88827849ba5f22c7886b2189`，不是root随后继续实现的新源码版本。启动前imageID、vendor SHA、receipt verifier和原simcluster check-image均通过。
- 只在root GO后启动新实例。没有restage、改vendor/产品源码/系统service、mock产品服务、重写业务输出。配置均通过真实installer公开 `--no-enable` 选项及既有供给helper；Caddy调用真实service的 `enable --now`。
- 脚本在 `/tmp/tether-control-recovery/caddy-profile-{smoke,wss-smoke,plain-diagnostic}.sh`。独立WSS脚本只拆分第二arm，不放宽前一arm失败；诊断只加观察，不改变失败oracle。

# 结果与原 rc

父目录 `/tmp/tether-control-recovery/caddy-profile-real/`。

| 执行 | 原 rc | 证据目录 | 结论 |
|---|---:|---|---|
| 两profile初次脚本 | 1 | `smoke.O0fbrZ` | plain所需“不启动Caddy”通过；额外原Python前端return可达性失败，未继续第二arm |
| 独立fresh Caddy WSS arm | 0 | `smoke.FWl17C` | 真实认证WSS、running SHA、完整PID1停启后自动恢复均通过 |
| 唯一plain诊断复验 | 1 | `smoke.PYKHVz` | 同一额外HTTPS失败，证实旧sidecar留在旧netns |

`runner.*`、`wss-runner.*`、`plain-diagnostic.*` 保留各自stdout/stderr/rc。没有静默重跑到绿，也没有把额外失败当作Caddy失败。

# Plain profile 的所需事实

- Fresh provision使用原installer `--no-enable`。正常启动和kill/return后，Caddy均为 `UnitFileState=disabled`、`ActiveState=inactive`、`SubState=dead`、`MainPID=0`、`ExecMainStartTimestampMonotonic=0`，不存在失败后伪装inactive的启动记录。
- 第一次真实broker PID1从host PID **1766872 → 1774251**，StartedAt从23:17:34到23:17:53；原认证ctl在return后恢复，Caddy未自启。
- Python ingress之前通过实例CA校验访问真实manifest得到HTTP200。额外要求“原sidecar在broker return后仍可访问HTTPS”失败，不属于“不自启Caddy”的所需断言，但原失败保留。

# 额外失败的实际反例与归类

`smoke.PYKHVz/...-plain/`：

- before：broker和Python sidecar均为 `net:[4026533535]`。
- broker kill/return后：broker变为 `net:[4026533611]`；Python sidecar原PID **1807754**、原StartedAt完全不变，仍在 `net:[4026533535]`。
- 新broker保留IP `172.18.0.2`，新netns没有443监听；其NATS/控制请求及loopback manifest已恢复。原sidecar只在旧netns监听443。
- 原ctl执行可信CA HTTPS curl，实际 **rc7 / code000 / connection refused**。Caddy依然disabled/inactive/never-start。

因此这是 **legacy同netns sidecar的机器fixture生命周期缺口**；不是Caddy抢占443，不是tether恢复失败。本轮root明确决定不扩修或再次运行该失败。

建议留到对应fixture任务：显式冷机器return后重挂legacy sidecar到新netns，或改为同机service。不得在热更新连续性测量窗口插入这类动作掩盖连接中断。

# Caddy profile 的实际事实

- 独立fresh实例，没有Python ingress sidecar。`caddy validate`通过，实际service由enabled激活。
- `tether node ls --json --nats-url wss://brk1:443/nats` 在已有真实ctl身份/会话下成功返回 `schema=node_ls`、`schema_version=1`、空nodes数组；这是认证控制请求，不是HTTP liveness。该smoke没有创建agent，空列表不被冒称为业务流。
- 重启前Caddy容器内MainPID **690**，运行 `/proc/690/exe` SHA为冻结Caddy SHA，version真实输出v2.7.6。
- broker实际PID1 stop/start：host PID **1787407 → 1794884**，StartedAt **23:19:08 → 23:19:29**。没有仅在组件内reload冒充节点reboot。
- 返回后未额外调用caddy_start/systemctl补救。Caddy自动以MainPID **81**启动，`enabled/active/running`，运行SHA不变，真实WSS认证请求再次成功。
- 前后unit状态、Caddy运行回执、WSS JSON、PID1身份和journal全部保留；相关harness前后SHA一致。

# 测试源审补充

- 新地址生成采用JSON string作为YAML scalar，host:port拒scheme/控制字符/非法端口并允许正确括号IPv6。已读正反例、副作用前拒绝；完整产品yaml.v3解析由root另一条测试负责。
- 当前 `test_caddy_provisioning_failure_and_routes` 还应明确断言发出 `systemctl brk1 enable --now caddy`。只检查is-active/WSS时，回退成start会让hermetic仍绿却破坏 `--no-enable` profile的reboot自启。已把该窄覆盖建议交root；当前实现和本次真实reboot均正确。

# 清理与不覆盖项

- **2026-09-20 23:21:54 UTC** 独立再次核对三个本轮实例：containers/networks/volumes全为空，见 `resource-postflight.json`；evidence仍保留，镜像未删除。
- `evidence-sha256.json` 记录证据摘要。
- 这是初始供给/服务profile/认证WSS/冷重启证据，不证明原外部SSH/TCP流、已有NATS/WSS会话无EOF、流seq/ACK恢复、文件传输、热更新回滚、延迟预算或全旧映像退场。
