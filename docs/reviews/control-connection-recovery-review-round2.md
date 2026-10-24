Pass — 第二轮五项发现（含修复过程中引入的迁移回归）全部闭合；三路内审、最终完整测试与闸门通过。停在独立外审门。

# 控制连接与隧道恢复：第二轮内审与回复

2026-09-20。用户在第一轮开发/内审后追加一轮内审修改；本轮仍由开发者主进程负责，不代替独立外审。基线 HEAD `cab90d1dcafe6badc4d68a9492c2a2b2ad6cc025`，index 保持干净，修改全部在暂存区外。

入口：[plan](control-connection-recovery-plan.md)、[tasklist](control-connection-recovery-tasklist.md)、[第一轮综合回复](control-connection-recovery-review.md)、[现场证据](control-connection-recovery-evidence.md)。

## 1. 编排和审查范围

三名新代理各自先读 CLAUDE/需求/当前架构和全部修改，在阅读第一轮报告前独立写下审查面：传输生命周期、隧道/证书/恢复、系统发现与契约。固定三路均实际执行，再互相质疑。三路最终原报告：[传输](control-connection-recovery-review-round2-transfer.md)、[隧道](control-connection-recovery-review-round2-tunnel.md)、[系统](control-connection-recovery-review-round2-system.md)，均已独立签署Pass。专家只读实现并新增独占职责测试，主进程修改生产、裁决、更新文档和统一执行闸门。

第一轮 `make test`、完整 `make e2e-parallel`（67单元）、`make gates`（含lint 0 issues）已通过。第二轮起点38个文件的SHA-256保存在 `/tmp/tether-control-recovery/round2-start-sha256.txt`；独立只读审查与第一轮固定计时脚本收尾并行，第二轮生产修改在第一轮完整门通过后开始。

第二轮新增8个Test，本增量总共43个只增登记。新增审查面包含：构造/最后Close交错、实际INFO RPC取消、同连接多reader与失败屏障、重复commit及跨session对象、boot cache重新验签与自然到期、N-1共用hostname、显式身份优先、同epoch ApplyHome与OpenHome的不同入口、单节点迁入集群epoch0、实际peer证明和不可降级、现有纯pin轮换与地址变更、真实数据恢复及测试是否覆盖断言。

## 2. Findings 和开发者回复

| 编号 | 结论及证据 | 主进程处置 |
| --- | --- | --- |
| S1 / Major | boot缓存未通过签名/账户/schema/有效期仍进入cachedRoster；remember在采纳合法新roster前把旧信息固化成接入ID。五臂真实cache+NATS refresh（含运行中自然过期）均可使合法新单broker会话实际被silence取消。 | 采纳。boot仅镜像已验证roster，仍保留pin/generation高水位；首次推导身份/aliases再检查当前有效期。此前在有效期内已确认的同连接身份仍保留，避免堵死真正retired岛屿。负例及有效缓存、有效期内确认后节点移除的正例race×3绿。 |
| S2 / Major | N-1缺接入ID时多个NodeID共用hostname被按首条记为某个ID；随后合法roster删除该ID/调整剩余节点地址，现有单broker会话被当成岛屿取消。 | 采纳。仅唯一NodeID匹配才固化fallback身份，显式ID优先且不受host歧义抹除。同反例与explicit-ID正例race×3绿。 |
| T2-F1 / Major | ApplyHome同epoch只写desired pins，绕过OpenHome的实际peer验证。真实TLS反例接受foreign pin；matching升级也没保存named/pinned身份，可再用空pins降级。clusteroffline --from-existing保持epoch0的真实调用链可达。 | 采纳。两入口共用verifySessionPeer，先验证实际TLS peer再更新、保存named home。已验证transport的正常纯pin轮换仍保留；同epoch不同有效地址明确拒绝，避免ACK未真正使用的home。真实TLS正反、重拨、相邻状态机23个Test×3 race绿。旧synthetic纯pin fixture仅补其已验证前提pinned:true，不改断言。 |
| T2-传输-F1 / Medium | legacy Bind隐式ConsumerInfo不继承PullSubscribe的ctx，实际扣留INFO后cancel仍不能及时进入清理。 | 采纳，已闭合。第一版把ctx放到legacy JS构造仍红：交叉核查SDK getJSContextOpts只继承wait/pre，不继承ctx。失败日志保留，未作绿结论。改用已创建consumer的显式Bind+SkipConsumerLookup，消除冗余不可取消RPC，实际配置与AckNone保持一致。强化双路径oracle与constructor race×3 PASS5.112s；全部reader/两条真WSS PASS70.980s。 |

附带补强：push-commit已根据收到消息的nc创建JS，删除剩余的可变a.js判空依赖。未把尚未证实的跨代成功落盘列为已发生缺陷。N-1新增字段仍optional/omitempty，对象wire和ACL保持兼容。

### 首次命名地址边界：纠正本轮修复引入的回归

主进程在最终边界检查提出：同epoch不同地址一刀切拒绝，会挡住合法的single→cluster首次命名迁移。隧道专家独立核对InitFromExistingOptions与CLI `--tunnel-addr` 为operator-provided，并用同证书的第二控制listener构造真反例：此前guard直接拒绝，`round2-fallback-address-before.log` FAIL0.067s。

采纳并修正：仅已经named的home同epoch换址才拒绝；首次命名允许新的advertised地址，先验证peer，保留现有流和supervisor。下一次redial在mu/generation守卫内同时快照当前addr与pins，不能再使用旧fallback参数。正例必须关闭旧控制listener并从新地址恢复真实payload，不能仅检查缓存字段。R13旧注释同步改为代绑定的锁内快照契约。

## 3. 第二轮证据

原始命令/日志统一在 `/tmp/tether-control-recovery/`，红/绿都保留。

| 项目 | 收据 |
| --- | --- |
| invalid/expired boot cache反例 | `round2-roster-cache-before.log`，5臂-race FAIL；含自然到期，非仅手工篡改本地文件。 |
| 非唯一host反例 | `round2-roster-ambiguous-before.log` FAIL；合法签名roster替换，真实session cancel。 |
| system最终定向race×3 | `round2-system-identity-final.log` PASS6.755s，含上述反例、正例和真实sole-survivor。 |
| ApplyHome实际peer反例 | `round2-apply-home-pin-before.log`：foreign/matching两臂均红。 |
| tunnel最终23个Test×3 race | `round2-tunnel-regressions.log` PASS11.257s，覆盖真实转发、重拨、期限、rename和旧连接清理。 |
| 传输constructor资源六循环 | `round2-transfer-constructor-race.log` PASS2.349s；metadata/consumer reply两类取消，后续完整读取、consumer/subscription/window和goroutine/fd回收。 |
| Bind取消反例与首修失败 | `round2-transfer-bind-before.log` FAIL1.119s；`round2-transfer-lifecycle-after.log` 三轮仍FAIL7.351s。 |
| Bind最终原样反例和constructor，race×3 | `round2-transfer-lifecycle-final.log` PASS5.112s；双路径oracle修前`round2-transfer-bind-dual-path-before.log` FAIL1.129s。 |
| 全reader与两条真WSS，race | `round2-transfer-reader-wss-final.log` PASS70.980s；正常191 RPC零失败/max277.405ms/零断重连；64取消127 RPC零失败/max277.369ms/零断线。 |
| 首次命名地址恢复与24个相关Test，race×3 | `round2-tunnel-final.log` PASS11.274s；`round2-fallback-address-mutant.log`：仅恢复旧addr的overlay按预期FAIL8.080s，真实payload不能恢复。 |
| 最终make test | PASS，exit0；`round2-make-test-final.log`。 |
| 最终make e2e-parallel | ALL PASS，67单元、4m2.509s；`round2-make-e2e-parallel-final.log`。 |
| 最终make gates（含lint） | PASS，exit0；`round2-make-gates-final.log`，hermetic ALL PASS、make lint 0 issues。无final后缀日志属于地址边界修正前的中间候选。 |

## 4. 疑惑和覆盖边界

- 第一轮现场归因边界保持：本机真WSS复现超帧，不能唯一归因现场所有Read/WriteError；本轮未部署或运行现网canary。
- 历史#4已有秒级REGISTER证据，历史payload未知；当前恢复测试覆盖真broker/SQLite/NATS/agent，未覆盖systemd/Caddy/WAN。
- N-1以源码/wire/真实权限与缺字段回复覆盖，未执行released旧二进制混部。
- Bind取消修复须用真实请求与清理收据证明；新增测试不要求所有取消时构造都立即返回error，只要求已取消reader不交付字节、可有界回收。
- 内审Pass只构成开发者向独立外审交付的依据，外审须重新检查全部暂存区外和未跟踪文件。

## 5. 外审交付

固定三路审查和交叉核验已完成，全部finding已回复并验证。最终make test、67单元完整make e2e-parallel、make gates（含lint）全通过；43个新增测试只增登记，Agent方法预算127→126、Background豁免40→39，无放宽。`git diff --check`通过，index保持干净，全部实现/测试/文档留在暂存区外，无提交或部署。

独立外审从index审全部未暂存及未跟踪内容：reader/agent与ctl接线、tunnel/adapter、roster/register与wire字段、history，以及所有新增测试、清单/预算和本文档。先读CLAUDE.md与本轮plan/tasklist，再独立检验，不把本报告Pass当作外审结果。外审前候选文件SHA-256记录于`/tmp/tether-control-recovery/final-candidate-sha256.txt`。

用户随后提出的全栈热更新按独立新增量规划；本轮没有实现热更新，已有恢复能力与计划内无中断升级不可混称。

## 闸门执行过程记录

第二轮中间候选的完整矩阵在执行期间加入迁移地址反例，明确报出旧guard拒绝和该新增测试尚未登记两项；`round2-make-e2e-parallel.log`保留FAIL（4m39.045s），不是归因不明的抖动。已修生产并只增登记后对最终候选重跑完整矩阵。中间`round2-make-gates.log`的Go与hermetic门通过，lint对两个冗余显式接口类型声明各报两项staticcheck；改用等价类型推导，无新增豁免，最终gates重跑。
