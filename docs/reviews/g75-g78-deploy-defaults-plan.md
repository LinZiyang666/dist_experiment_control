# 部署层默认值修复 plan(定稿)— gotcha #75/#76/#77/#78

> 叶子增量(post-1.0 模式),不在线性 P 序内。2026-08-11 定稿;起草 = 4 专家草案 + 4 视角对抗批评 + 综合
> (Workflow 9 agents),主进程逐条裁决后定稿。四条互为因果:#78 的每 5s 拨号/WARN 是燃料,#75(配置静默吞 +
> sink 不可见)与 #77(journald 对小盘太宽)是油箱,#76(漏 enable)让一次重启把带病的单 broker 直接带走。
> 修复主轴 = **把"运维得记得"变成默认,并掐掉燃料**。
>
> 批评轮挖出、主进程逐一亲核确认的四条源码级硬事实,作为裁决前提:
> **F-A**:`nodeHasProxyCap`(`internal/broker/proxy.go`)对 capabilities 缺 token 的节点回退
> `isP13Capable(release)`(≥v0.2.9 即 capable)——**"摘 CapProxyV1"停不了任何东西**;
> **F-B**:`scripts/install.sh` 官方模板自 P10 起自带 `broker.tls.acme.email` 而 Go schema 无 `tls` 字段——
> **裸 `KnownFields(true)` 会 brick 每台官方装机的 broker**(主进程复核:模板 `tls:` 块在,schema 零 TLS 引用);
> **F-C**:`repairProxy` 每拍推的是 **keyset-only directive**(无 Token/PublicPort),agent 走
> `case p.srv == nil` footprint bootstrap——退避闸只能放 `proxyStartLocked` 单咽喉、按**解析后拨号参数**比对;
> **F-D**:`internal/node.RegisterInput` 是 raft payload——**不加字段**;但 `ProxyCapable` 是其**既有字段**,
> 只改其计算值合法(raft 面零 schema 改动)。

---

## 0. 有序更新(v0.5.0 → 本版;用户 2026-08-11 指示的常设约束)

现网 = racknerd 单 broker + 8 agent 全 v0.5.0。本批必须可从 v0.5.0 **in-place 有序升级**:

1. **升级顺序**:broker 先、agent 后(broker-ops §8.8);agent 走 `node upgrade --url/--sha256`。
2. **wire N-1 四象限**(本批 +2 additive 字段,零值=今日行为):
   - 新 broker × 旧 agent:无 `proxy_opt_out` 字段 → 完整参与,字节不变。
   - 旧 broker × 新 agent(opt-out):旧 broker 照推(Debug 级 keyset),agent 本地 gate 拒拨 + 退避
     → 刷屏止血成立;broker 仍渲染该节点 never-ready,如实写 [GAP]。
   - 退避/WARN 降频均为单侧本地行为,无协商。
3. **serveconf 严格化对现网升级的影响(必须写进 broker-ops 升级注意)**:racknerd 的 broker.yaml 是
   2026-08-11 手工重建的,其中后补的 `observability` 段实测未生效——若系错嵌套,升级到本版后 broker 将
   **fail-loud 拒启**(这正是 #75 要的行为,但要在升级时窗内发生)。升级步骤加一步:
   flip 二进制前先用新二进制做一次配置验证(`tether serve --config /etc/tether/broker.yaml` 手动起一次看
   报错,或直接按模板修正 yaml),再 restart。
4. **存量装机 retrofit(#76/#77 只改 install.sh 惠及不了 racknerd)**:broker-ops.md 增一节
   "existing install retrofit",给两条一次性命令:
   `systemctl enable nats-server tether-broker caddy` + 按盘写
   `/etc/systemd/journald.conf.d/60-tether.conf`(racknerd 上一轮已手工做过等效动作,写文档是为下一台)。
   **不建议**在存量机上重跑 install.sh(会覆写 broker.yaml/Caddyfile)。

---

## 1. 范围与 Non-goals(D9 裁决)

**范围**:

| gotcha | 本批做什么 |
|---|---|
| #75 | serveconf `KnownFields(true)` 严格化 + inert TLS stub + 模板↔schema 配对守卫 + `cluster.go` seam-probe 吞错修复 + file-sink stderr breadcrumb |
| #76 | install.sh broker 角色默认 `systemctl enable`(不 `--now`)+ `--no-enable` + K.0 §2 契约字句全引用面 sweep + `--uninstall` 对称 disable |
| #77 | install.sh broker 角色条件写入 journald drop-in(三档按盘推导,不覆盖运维显式设置,不自动 restart journald) |
| #78 | agent 首拨指数退避(`proxyStartLocked` 单咽喉,封顶 5min)+ broker `read REGISTER` WARN 经 `backoff.Tracker` 降频 + `proxy.participate` opt-out(ProxyOptOut 折入 ProxyCapable 计算) |
| 配套 | hermetic 测试 + drill(扩 32、扩 93、新增 78)+ 文档(含 §0 retrofit/升级注意)+ 台账 flip |

**Non-goals**(写死,越界即废):

1. 不修 WSL 宿主出站 :7000(环境侧,台账已裁定)。
2. 不做 broker 按连续拨失败**自动摘除**节点(flap 自驱逐 + 无人负责恢复;opt-out + 退避已够)。
3. 不做通用 rate-limited logger/第二套 damper(h1 E1:`internal/backoff` 是唯一 damper)。
4. 不用 LogNamespace(打断 `drills/lib/logs.sh` 流映射与 `journalctl -u` 肌肉记忆——h1 教训:挪流是本 harness 最坏输出的成因)。
5. 不引入零配置 binary 默认日志文件(h1 byte-equivalence golden 钉死)。
6. 不动已建立会话的 supervisor redial backoff(500ms→30s 已正确)。
7. 不做 `proxy.participate` 热加载(改 yaml 重启 agent 生效)。
8. **agent 角色不做 enable**:install.sh agent 角色不写任何 system unit(user unit 由 `tether agent --install-user-service` 生成,K.0 §2 禁止 installer 调 tether 二进制)。
9. 不动 broker repair/reaper 的 5s 推送节拍(M2/F1 收敛环不打孔)。
10. 不给 `nodes` 加新列、不给 heartbeat 加载荷(D7 裁决后冗余)。
11. cluster 视图的 opted-out 标签降为文档化 [GAP](不为一个 status 标签建复制机制)。
12. 不做 RNG jitter(**对"要抖动"预期的显式偏离**,理由见 D5,留待外审裁决)。
13. 不做 journald vacuum 自动化、不做 journald 配置漂移检测。

---

## 2. 决策点裁决

### D1 — serveconf 严格化:**`KnownFields(true)` fail-loud + inert TLS stub + 模板配对守卫 + seam-probe 吞错修复**

- `Load()` 改 `yaml.NewDecoder` + `dec.KnownFields(true)`;`io.EOF`(空/纯注释)→ 零值容忍;第二个非空
  document 拒绝(镜像 `loadAgentYAML`)。yaml.v3 错误自带行号与键名,`systemctl status` 5 秒可见——
  比带病运行 7 小时写 1.1GB 好。agent 侧严格解析已在产线证明可行,broker 宽松是双标。
- **必配 inert stub**(F-B 面前无可谈判):`BrokerSection` 增 `TLS TLSSection{ACME ACMESection{Email string}}`,
  注释标 ops-only、Go 不消费(caddy 消费)。驳回裸严格化:官方模板自 P10 起每一版都带 `tls.acme.email`,
  裸 strict 是 100% 复现的 fleet-brick。
- **模板↔schema 配对守卫**:从 install.sh 真 heredoc 提取(dummy 变量代入)、严格解析必过且
  `Obs.LogFile != ""`。驳回手抄 fixture(手抄必然照 schema 写,fixture 绿而真模板炸)。
- `cmd/tether/cluster.go` seam-probe 站(`if existing, lerr := serveconf.Load(configPath); lerr == nil`)
  吞错改为传播——严格化后该站会把 typo yaml 当"无 seam"→ 写 seam → 尾部 fail-closed Load 才爆且错因错位。
- WARN-不拒启驳回:WARN 落进的正是可能配错的 sink,本缺陷已实证"没人看见"。无逃生 flag(agent 侧也没有)。

### D2 — 零配置 binary 默认:**保持 stderr;file sink 成功时打 stderr breadcrumb**

- h1 golden 不动;部署层封顶三层已闭合(模板 log_file+cap → unit journal → 本批 journald cap)。
- 可见性 = `resolveLogSink` 内 file 打开成功后**原地**
  `fmt.Fprintf(os.Stderr, "tether: logging to %s (cap %dMB x %d backups)\n", ...)`(stderr→journal,
  serve/agent 两角色同享);失败降级行保留。驳回"经已构建 logger 打 Info"——file sink 时该行落进文件自身,
  恰好错过"journal 里找日志去向"的盲区(断言面与缺陷面错位的假绿)。

### D3 — #76:**broker 角色默认 `daemon-reload` + `systemctl enable`(不 `--now`)三 unit + `--no-enable` + uninstall 对称;agent 角色不动**

- 提示无法修复结构性风险:banner 里 `enable --now` 一直在,racknerd 照样漏。enable 不带 `--now` 只建
  symlink——**`pgrep tether` 必空的字面不变量原样保留**。
- 执行守卫(POSIX sh):`command -v systemctl` 且 `[ -d /run/systemd/system ]` 才 enable;systemd 在跑而
  enable 失败 → `die`;无 systemd → NOTICE 一行。enable 集合 = `nats-server tether-broker caddy`
  (caddy 现网"恰好 enabled"是运气不是设计)。
- banner:默认分支改 `daemon-reload && systemctl start …`(start 仍归 caller——这正是 CORE INVARIANT);
  `--no-enable` 分支保留完整 `enable --now` 文案。
- `--uninstall`:rm unit 前先 `systemctl disable`(容错;顺带修掉手动 enable 过再 uninstall 留悬空
  symlink 的既有隐患)+ `daemon-reload`。
- **契约 sweep 全引用面**(逐文件列入 §3):install.sh 头注释 + `:630` 附近注释 + banner、
  `docs/architecture.md` K.0 核心原则 2 与 K.5 表、`docs/broker-ops.md`、`test/p10/install_sh_test.go`
  注释、drill 32 头注释。字句:
  `generated and ENABLED for boot (symlink only), but NEVER STARTED — pgrep tether stays empty`。

### D4 — #77:**条件写入全局 drop-in `/etc/systemd/journald.conf.d/60-tether.conf`,三档按盘推导,banner 提示 restart**

- 驳回 LogNamespace 与只文档(h1 §8.8 已是文档,现网 1.8G 是它的实证)。
- 条件:`/etc/systemd/journald.conf` 与 `journald.conf.d/*.conf` 中无**未注释**的 `SystemMaxUse=` 才写,
  **检测排除自己那份**(否则第二次安装把自己当运维设置,幂等与盘扩容更新全破)。自己那份幂等覆写。
- 推导(三档表,最可读可测):`df -Pk /var/log` → <10G→200M;<40G→500M;≥40G→1G。19G 现网盘 → 500M,
  与实战兜法吻合。事实修正写进文件注释:journald 默认 min(fs 10%, 4G),**非无界、是对小盘太宽**
  (台账 #77 的"无界"表述一并订正)。
- **不自动 restart journald**(共享主机全局动作 + 老 systemd 断 stdout stream 风险):banner 追加
  `sudo systemctl restart systemd-journald` 一行;未重启前下次 reboot 生效——#76 已使 reboot 安全。
- 测试缝只留一个:`TETHER_JOURNALD_ROOT`(默认 `/etc/systemd`)。驳回多 env 缝(纯为测试开生产分支)
  与 dot-source 函数单测(install.sh 尾部无条件 dispatch,source 即执行安装)。
- `--uninstall` 只删自己署名的文件。dry-run 打印将写路径与推导值。

### D5 — #78 退避:**只落 agent 侧,`proxyStartLocked` 单咽喉,复用 `backoff.Tracker`,key 含 homeEpoch,不做 RNG jitter**

- **broker repair 一行不动**:推送是廉价 NATS publish + Debug;昂贵且刷屏的是拨号本身,掐 agent 侧一处,
  broker WARN 自然归零。
- **闸位(F-C 决定)**:`proxyStartLocked` 入口、任何副作用之前。驳回"directive 五元组"key
  (keyset-only directive 的 port=0/token="" 与失败记录永不相等 → 每拍清零重拨,退避形同虚设)与
  Token 分支闸(5s 循环走 bootstrap 分支,闸一次都不会触发)。
- 状态入 `proxyRuntime`(`p.mu` 保护,字段注释钉 Tracker 非并发安全):
  `dial *backoff.Tracker`(Policy{Base:5s, Cap:5min},`Config.ProxyDialRetryBase/Cap` 覆盖)+
  `lastFail proxyDialID{gen, epoch, port, tokenHash, homeEpoch}`(**homeEpoch 必须在**——C5 rehome 只
  bump home epoch,缺它则换家被旧失败压满 5min)。
- 记失败:`ssproxy.Start` 与 `AddProxy` 两臂 `Fail(now, class)`,`logNow` 才 WARN 否则 Debug
  (agent 自家日志同步止血);**adapter-nil 臂不进退避**(配置病非网络病,保持现 WARN)。
- 挡重试:同 `proxyDialID` 且未 Due → return(不 teardown、不 ACK、保持 unready、Debug 一行)。
  窗内热路径是 bootstrap 分支(srv==nil,无可 teardown),Token 全建路径必伴新 token=bypass——
  "窗内零 teardown/零 ACK"由测试钉死。
- bypass 白名单(ID 任一分量新 → 立即拨 + 清零):(gen,epoch) 前进(`proxy off/on` 必 bump epoch——
  **运维显式操作零延迟**)、port/token 变化(rotation/re-mint)、homeEpoch 前进(rehome)。
- 重置:拨成功 → 打 `recovered, suppressed=N` Info 后**显式重建 Tracker**(不依赖 `Recover` 的
  anti-flap floor——run<Base 不重置,显式重置让"成功后下次从 5s 起"无条件为真);权威 OFF/
  `proxyTeardownLocked(clearPersist=true)` 重置;`onNATSReconnect` 在 apply 前重置。
- 零 timer、零新 goroutine、零新 `context.Background()` 站点:下一次 heartbeat 推送就是重试时钟
  (附加延迟上界 = 一个 heartbeat)。
- **jitter 不做**(Non-goal 12):重试由 5s heartbeat 格点驱动、相位按 agent 天然离散、车队十位数量级,
  亚 5s 抖动无意义;jitter 区间断言是删掉 jitter 也不红的恒等式。**此为对预期方案的显式偏离,留外审裁决。**
- 时钟走既有 `Config.Now` seam。

### D6 — broker WARN 降频:**`handleAgent` read-REGISTER 单站点,`regLogMu + backoff.Tracker` 复用,class={eof,timeout,other},补 remote 属性**

- 驳回自造 throttle(h1 E1 钉死 backoff 是唯一 damper);驳回 per-IP key(轮换源扫描下自败,且违反
  "class 不含可变细节"红线);"不能加锁"在 60 conn/min 量级无实测依据,mutex 纳秒级。
- 站点只收 read-REGISTER WARN(唯一未鉴权互联网可触发站点);accept 已有清退守卫、bind 在鉴权后有
  自然上界——驳回多站点扩面(对臆测故障的对冲)。
- `Fail(now, class)` 的 logNow 才 WARN(携 `suppressed_since_last` 与 `remote` 属性——现网当时无法定位
  是谁在拨,该站点确无 remote,补上);成功 REGISTER → `Recover` → Info `suppressed=N`。
- **如实声明语义**:纯失败风暴下每 class 首条后静默,直到 class 切换或下次成功 REGISTER 才复述计数——
  写进 broker-ops.md,不加周期性复述(够用就好)。交错语义(成功/失败穿插的计数)由专门 hermetic 案钉死。

### D7 — opt-out:**`ProxyOptOut` 折入 `ProxyCapable` 计算 + register 单载 + 内存 hint 可见性**

- **机制**(F-A 面前 capability 摘除路线全废,F-D 面前 RegisterInput 加字段全废;折入计算是唯一同时
  合法且最小的形状):
  1. wire:`NodeRegisterReq.ProxyOptOut bool json:"proxy_opt_out,omitempty"`(additive、零值=participate、
     N-1 合法)。**只载 register**——驳回 heartbeat 双载 + `nodes` 新列 + 新 setter(`ProxyCapable` 是
     raft payload 的**既有字段**,折入计算即复制到位;并消掉双载的"两个来源冲突"悬案)。
  2. broker 折入点:`handleRegister` 的 `ProxyCapable: nodeHasProxyCap(...) && !req.ProxyOptOut`——
     `nodes.proxy_capable=0` 后,`onlineNIDs`、`repairProxy`→`nodeProxyCapable`、status 查询、
     `enableProxy` 循环**全部经既有列免费生效**(register 即生效,无 5s 窗口泄漏)。
  3. register-reply 直读两站(`proxy.go` 单机/cluster 两臂):`req.ProxyOptOut` → return nil。
  4. 存量释放(单机):`handleRegister` 中 `req.ProxyOptOut` 且存在 ALLOCATED `__proxy__` 行 →
     `CloseProxy` + `port.Free` + `freed` 事件 + `SetProxyReady(false)`。
  5. cluster 残留:`reconcileProxySession` nids 循环前置 gate——`proxy_capable=0` 且有存量行 → 按
     `reconcileProxyTeardown` 单行剧本(push OFF + CloseProxy + raft PlanFree + freed 事件 + damper 清理)
     → continue。同时闭合 rotation 路径不查 capability 的永久 rotate-mint。
  6. agent belt(对旧 broker 的 N-1 半径):`applyProxyDirective` 顶部 opted-out → Info 一次 +
     `pubProxyReady(false)` + return;boot 时 `SetProxy(nil)` 清持久 footprint。旧 broker 照推,
     agent 永不拨——刷屏止血成立,残余如实写 [GAP]。
- **可见性**:"不会 vs 不愿"用 `b.proxyOptedOut sync.Map`(register 时 set/clear,单机)+
  `ProxyNodeEntry.OptedOut bool json:"opted_out,omitempty"`;`proxyStatusNodes` 把 hint 中节点**补进列表**
  (capable=0 后原查询不出行),CLI 渲染 `opted-out`。**如实标注 hint 语义**:broker 重启后、agent 未重连
  期间显示回落为 not-capable——文档化 [GAP](broker 单独重启不掉 agent 的 NATS 连接,不能说
  "re-register 即恢复")。cluster 视图不做标签(Non-goal 11)。
- agent.yaml:`proxy.participate`(`*bool`,nil→参与——裸 bool 会把"没写"读成 opt-out);内部
  `Config.ProxyOptOut bool`(零值=参与)。
- **回滚砖警告必须落文档**:agent.yaml 是 KnownFields 严格解析,写入 `participate` 后回滚到旧二进制
  (含 `node upgrade` 自动回滚)会**拒启**——usage.md 写明"仅当该机不再会回滚到 < 本版本时才写此键",
  进风险表与 flip 记录。

### D8 — simcluster:**扩 32(#76/#77)、扩 93(#75)、新增 `drills/78-proxy-dial-backoff.sh`(#78);dry-run 断言进 hermetic,两层都要**

- **32 扩**:is-enabled×3 == enabled ∧ is-active ≠ active ∧ `pgrep -x tether` 空;`--no-enable` →
  disabled;drop-in 存在且值 == drill 内**独立复算**的三档值(不抄产品输出);预置显式 `SystemMaxUse=` →
  不写;uninstall → disabled + drop-in 净。**脚本级变异轮**:注释掉 enable/drop-in 发射点各跑一次确认红。
- **93 扩**:错嵌套 yaml(**从真模板形状变异**,非手造)→ unit 拒启,错误经 `sim_broker_panic_journal`
  含键名(启动拒绝在 boot/panic 流);恢复模板形状 yaml → journal 出现 `tether: logging to` breadcrumb
  且 broker.log 增长。93 的 1 条内联额度不动。
- **78 新 drill**(source `drills/lib/logs.sh`,oracle 门自动核):
  - **臂 A(退避)**:agent 容器 `iptables -I OUTPUT -p tcp --dport <tunnel_port> -j REJECT`(fail-fast;
    驳回 PSH-drop:TLS ClientHello 被吞后 client 侧超时行为未核实,有挂起 `p.mu` 与 false-red 双险)。
    **反空洞先行**:先断言 ≥1 次拨号 WARN 已发生(全部 ≤N 断言同此)。180s 窗按 60s 分桶:首桶 ≥2、
    末桶 ≤1(递减趋势,不钉死次数)。
  - **臂 B(运维 bypass)**:拆规则 + `proxy off && proxy on` → 60s 内新拨号(并发 drill 下 10s 脆,
    60s 对 5min 退避判别力不损)。
  - **臂 C(WARN 降频)**:ctl 循环 `nc` 60× 建连即断(establish-then-close 正是 read-REGISTER 的 EOF
    class,比 PSH-drop 更真更廉价)→ 断言 ≥1 ∧ ≤2 条 WARN。**不在 drill 断言 suppressed 行**
    (事件驱动复述在秒级风暴内不触发,强断言=对健康产品 false-red);suppressed 计数语义由 hermetic 钉。
  - **臂 D(opt-out + 恢复)**:`participate: false` 重启 → 游标断言零新拨号、`proxy status` 显示
    opted-out、ALLOCATED 行 freed;**翻回 true → 节点回池**(恢复路径坏了,opt-out 就是静默永久开关)。
  - **drill 变异**:对 v0.5.0 发布二进制整跑 → 臂 A/C/D 红、臂 B 天然绿(红绿分布本身验证判别力),
    结果记 drill 头注释(IS/IS-NOT 惯例)。成本预告:约 2× drill 时长。

### D9 — 见 §1 Non-goals。

---

## 3. 逐文件改动清单(file:symbol)

**#75**
| 文件:符号 | 改动 |
|---|---|
| `internal/serveconf/serveconf.go : Load` | strict decoder + EOF 容忍 + 第二非空 doc 拒绝 + 错误串带路径 |
| `internal/serveconf/serveconf.go : BrokerSection` | + `TLS TLSSection`(inert:`ACME.Email`;注释 ops-only, parsed-not-consumed) |
| `cmd/tether/cluster.go`(seam-probe 站) | `lerr == nil` 吞错 → 传播为命令失败 |
| `cmd/tether/logging.go : resolveLogSink` | file 打开成功 → 原地 stderr breadcrumb;签名不动 |

**#76/#77 — `scripts/install.sh`**
| 位置 | 改动 |
|---|---|
| 头注释 K.0 §2 + unit 生成处注释 + banner | 契约字句 → "generated and ENABLED for boot (symlink only), NEVER STARTED";banner 默认 `daemon-reload && start …`(+ 写了 drop-in 时 `restart systemd-journald` 行);`--no-enable` 分支保留 `enable --now` 全文 |
| argparse/usage | + `--no-enable` |
| `install_broker` → 新 `enable_broker_units` | systemd 守卫(`command -v systemctl` ∧ `/run/systemd/system`)→ `run daemon-reload` + `run systemctl enable nats-server tether-broker caddy`;失败 die;无 systemd NOTICE |
| `install_broker` → 新 `write_journald_dropin` | 排除自身的未注释 `SystemMaxUse=` 检测 + `df -Pk /var/log` 三档 + 写 `60-tether.conf`;`TETHER_JOURNALD_ROOT` 单缝 |
| `uninstall_broker` | 先 disable(容错)→ rm units → rm drop-in → daemon-reload |

**#78**
| 文件:符号 | 改动 |
|---|---|
| `internal/proto/messages.go : NodeRegisterReq` | + `ProxyOptOut bool json:"proxy_opt_out,omitempty"`(注释:零值=participate、N-1 矩阵) |
| `internal/proto/messages.go : ProxyNodeEntry` | + `OptedOut bool json:"opted_out,omitempty"` |
| `cmd/tether/agent.go : proxyConfig` | + `Participate *bool yaml:"participate"`(nil→参与),翻转接线 `Config.ProxyOptOut` |
| `internal/agent/agent.go : Config` | + `ProxyOptOut bool`、`ProxyDialRetryBase/Cap time.Duration` |
| `internal/agent/agent.go : register` | req 填 `ProxyOptOut`(capabilities 不动) |
| `internal/agent/agent.go : Run` | opted-out → `SetProxy(nil)` 清 footprint |
| `internal/agent/proxy.go : proxyRuntime` | + `dial *backoff.Tracker`、`lastFail proxyDialID{gen,epoch,port,tokenHash,homeEpoch}`(mu 保护注释) |
| `internal/agent/proxy.go : applyProxyDirective` | 顶部 opt-out gate(Info once + `pubProxyReady(false)` + return) |
| `internal/agent/proxy.go : proxyStartLocked` | 入口退避 gate(同 ID ∧ !Due → return);Start/AddProxy 两失败臂 `Fail(now,class)` + 记 lastFail + logNow 定 Warn/Debug;成功 → suppressed Info + 显式重建 Tracker |
| `internal/agent/proxy.go : proxyTeardownLocked / onNATSReconnect` | clearPersist=true / 重连 → 重置 Tracker |
| `internal/broker/broker.go : handleRegister` | `ProxyCapable: nodeHasProxyCap(...) && !req.ProxyOptOut`;`b.proxyOptedOut sync.Map` hint;opt-out 存量行 → CloseProxy + Free + freed 事件 + SetProxyReady(false) |
| `internal/broker/proxy.go : proxyDirectiveForRegister` | 两臂 `req.ProxyOptOut` → nil |
| `internal/broker/proxy_reconcile.go : reconcileProxySession` | nids 循环前置:capable=0 有存量行 → teardown 单行剧本 + continue |
| `internal/broker/proxy.go : proxyStatusNodes` | hint 节点补行 `OptedOut=true` |
| `cmd/tether/proxy.go`(single status 打印) | 渲染 `opted-out`(cluster 不动,[GAP]) |
| `internal/tunnel/tunnel.go : Server / handleAgent` | + `regLogMu sync.Mutex` + `regLog *backoff.Tracker`;read-REGISTER 站按 class{eof,timeout,other} `Fail`→logNow WARN(+`remote`+`suppressed_since_last`);成功 REGISTER `Recover`→Info |

**文档 / 契约 sweep**:`docs/architecture.md`(K.0 原则 2、K.5 表)、`docs/broker-ops.md`
(手抄命令 + journald/enable/严格化升级注意/WARN 静默语义 + **§0 的 retrofit 一节**)、`docs/usage.md`
(`proxy.participate` + **回滚砖警告** + 退避行为 + hint [GAP])、`docs/deploy-tier-gotchas.md`(flip +
#77 "无界"表述订正)、`test/simcluster/README.md`(drill 78 行 + 32/93 扩说明)、
`test/p10/install_sh_test.go` 注释、drill 32 头注释。

---

## 4. wire / 闸门影响

- **wire**:+2 字段(`NodeRegisterReq.ProxyOptOut`、`ProxyNodeEntry.OptedOut`),additive/omitempty/
  零值=今日行为;**ProtoVersion 不 bump**;inventory updater 合法追加。**raft 面零改动**
  (RegisterInput schema 不动,只改 `ProxyCapable` 计算值)。
- **闸门**:命名冻结——新测试全按被测单元命名,`// origin: deploy-tier gotcha #7x` 锚点;CLI golden
  零扰动(`--no-enable` 在 sh 不在 cobra);TLS 配对门站点数保持 4;enum 门无涉;结构预算——各包增量
  预期落量化窗内,顶到棘轮优先并入既有文件;layering 表——`internal/agent` 首次 import
  `internal/backoff`,若规则表为允许式则加一行;泄漏门——零新 goroutine/timer,tunnel/agent 测试带
  `-race` + 内建门;simcluster oracle——drill 78 source `logs.sh`;docs 布局——本 plan 在
  `docs/reviews/` 下;`context.Background()` 新站点 = 0。改闸门自身仅 wire 账本追加 → 收尾
  `make gates`(不与其它 lint 并行)。

---

## 5. 测试清单(每守卫附变异验证)

**Hermetic**

| 测试 | 断言 | 变异验证(注入 → 红) |
|---|---|---|
| `internal/serveconf/serveconf_test.go` unknown-key 表测 | 根层错嵌套 observability/`log_file` 直挂 broker/typo → error 含键名行号;合法全字段落位;空文件/纯注释放行;第二 doc 拒 | decoder 换回 `yaml.Unmarshal` → 三条 error 案红 |
| 同文件·模板配对 | 真 heredoc 提取(dummy 代入)严格解析过 ∧ `Obs.LogFile≠""` | 模板注野键 → 红;删 TLS stub → 红(双向) |
| `cmd/tether` cluster init 测试 | typo yaml → 命令失败、**不写 seam** | 恢复 `lerr == nil` → 红 |
| `cmd/tether` logging 测试扩 | file sink → **stderr 捕获**含 breadcrumb;stderr sink → stderr 零新输出;h1 golden 伴测全绿 | 删 Fprintf → 红 |
| `test/p10/install_sh_test.go` 扩 | dry-run 含 enable 行;`--no-enable` 缺席且 banner 含 `enable --now`;uninstall 含 disable+rm drop-in;journald(`TETHER_JOURNALD_ROOT` 指 tmpdir)四案:空→写入行含 `SystemMaxUse=[0-9]+M`;预置未注释值→skip 行;**预置注释态 `#SystemMaxUse=`→仍写**;自己那份已存在→仍覆写 | 逐发射点注释 → 各红;grep 不排注释 → 注释案红;不排自身 → 幂等案红 |
| `internal/agent/proxy_dial_backoff_test.go`(fake clock=`Config.Now`,恒败 adapter stub) | 同 ID keyset 重推窗内 → **零 AddProxy、零 teardown、零 ACK、保持 unready**;阶梯 5s→…→5min 封顶;epoch bump/新 token/新 port/homeEpoch 前进 → 立即拨+清零;成功→显式重置(下次从 5s);OFF/reconnect 重置;adapter-nil 不进退避;`-race`+泄漏门 | 删 Due gate → 计数红;逐删 bypass 条款 → 各红;删显式重置 → 重置案红 |
| `internal/agent` opt-out 测试扩 | register req 载 `proxy_opt_out`;yaml 三态(缺/true/false);directive gate no-op + 恰一 Info + unready;boot 清 footprint | `*bool` 改裸 bool → "缺失=参与"案红(N-1 零值灾难预演);删 gate → 红 |
| `internal/agent` 既有回归声明 | `proxy_ordering`/`proxy_reconnect`/`TestExternalReviewTransientProxyStartFailureCanRecover` 全绿不改断言 | —(不回归声明) |
| `internal/tunnel` server 测试扩(并入既有文件) | N 条 EOF 连发 → WARN ≤ 1+class 切换数,含 `remote`;**成功/失败交错案**钉 Recover 语义(suppressed 对账);`-race`+泄漏门 | 绕过 Tracker 恒 WARN → 计数红;删 remote/suppressed 属性 → 红 |
| `internal/broker` proxy 测试扩 | opt-out register:`proxy_capable=0`、无 mint、repair 零推、`onlineNIDs` 除名、存量行 freed 且**端口立刻可再分配给另一节点**、status hint 行 `OptedOut`;**翻回 participate → 回池 re-mint**;cluster reaper gate:capable=0 有行 → teardown 不 re-mint | 删 fold → capable 案红;删 register free → 端口复用案红;删 reaper gate → cluster 案红;删 hint → status 案红 |
| `internal/proto` golden 扩 | 新字段**零值字节等价**(旧 golden 不变);true 时 round-trip | 去 omitempty → 字节等价红 |

驳回 60s 真时钟 e2e(`parallel-flake-rootcause` 载明的 flake 类,假时钟已覆盖逻辑、drill 覆盖真时间)。

**simcluster**(weilandserver 本机 `./local.sh drill <name>`,改代码后必先 `./local.sh --build build`,
本批必跑 32/93/78):断言与变异法见 D8;全部 ≤N 断言配 ≥1 反空洞下限。

---

## 6. 风险与回滚

| 风险 | 缓解 | 回滚 |
|---|---|---|
| 严格化拒启存量 broker.yaml(含 racknerd 手工 yaml) | inert stub 覆盖官方模板全键 + 配对测试长效钉;错误带键名行号;broker-ops.md 升级注意(§0.3:flip 前先验配置) | 换回 N-1 二进制即宽松(配置未被改写) |
| **agent.yaml `participate` 回滚砖** | usage.md 写明"仅当不再回滚到 < 本版本才写此键";flip 记录注明 | 删该键一行即复原 |
| enable-by-default 惊扰刻意 disabled 场景 | `--no-enable` + banner 明示已 enable 清单 | `systemctl disable` 一行 |
| journald drop-in 影响整机 | 条件写入(显式设置绝不覆盖)+ 署名注释 + uninstall 删净 | 删文件 + restart journald |
| 退避拖慢真恢复(最坏 5min+1 heartbeat) | bypass 白名单覆盖全部运维显式路径与拓扑演进;Cap 可配 | agent 侧单文件 revert,无协同回滚 |
| Tracker 静默期遮蔽新故障 | class 切换必打;成功即复述计数;语义如实入 broker-ops.md | read-REGISTER 一处 revert |
| opted-out hint 在 broker 重启后缺席 | 文档化 [GAP](回落显示 not-capable,无功能影响) | 无状态残留 |
| wire 追加字段 | 零值=旧语义,N-1 双向核过 | 无需回滚(账本留痕合法) |

收尾按 gotcha 切 4 个 commit(serveconf+breadcrumb / enable / journald / 退避+降频+opt-out),一次 push——
任一条可独立 revert 不牵连 wire。

---

## 7. 验收判据(flip 条件)

- **#75 → FIXED**:错嵌套/typo 拒启且报键名行号(hermetic + drill 93 经 `sim_broker_panic_journal`);
  模板配对测试常绿;正确配置下 journal 现 breadcrumb 且 broker.log 增长;h1 golden 全程未动;
  seam-probe 站不再吞错。
- **#76 → FIXED**:drill 32 四连——is-enabled×3 enabled ∧ is-active≠active ∧ `pgrep -x tether` 空 ∧
  uninstall 对称净;`--no-enable`/dry-run 两退出口绿;K.0 §2 新字句落齐全引用面。
- **#77 → FIXED**:drill 32——drop-in 值 == 独立复算三档(19G→500M)∧ 预置显式值不覆盖 ∧ 注释态仍写入
  (hermetic)∧ uninstall 删净;banner 含 restart journald 行;台账落"非无界、对小盘太宽"事实修正。
- **#78 → FIXED**:drill 78 四臂对 fixed 绿、臂 A/C/D 对 v0.5.0 红(存证入头注释);hermetic
  退避/降频/opt-out 全表绿含变异轮;`proxy status` 渲染 opted-out;wire 账本 +2;台账注明环境侧残余
  (WSL :7000 放行)与旧 broker [GAP]。
- **整批 done**:`make test` + `make e2e-parallel` + `make lint` 全绿(退出码直取不过管道)+ 并发面
  `-race`+泄漏门 + drill 32/93/78 各绿含变异轮 + `make gates` 绿;台账 flip、INDEX.md 追行,外审通过后
  按 §6 提交。
