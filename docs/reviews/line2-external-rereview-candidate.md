Fail

# 线二 · 开发者修复候选 — 独立外部复审

日期：2026-07-30

边界：上一轮全部已暂存内容之上的 20 个开发者未暂存文件，初始 `+1511/-163`，
diff SHA-256 `08df0e660fd9583059c2d942daf0727b2714eee03b94fc54b755d546dfe0d059`。
开发者写入上一轮报告的回复只作实现索引，不作修复成立的证据。

## 结论

不能放行。M3/M4/M5/M6 的修复经独立变异/测试成立，watch JSON 与结构化 wire code
改造也未发现产品级回归；但仍存在 2 个 Blocker、1 个 Major / Security 和 2 个
Medium。正常路径的 `make gates`、`make test`、race 和发布矩阵均绿，不能推翻浅克隆
CI 和安全变异的直接反证。

## Findings

### B1 — BLOCKER：冻结历史 SHA 在真实 CI 的 depth=1 checkout 中不可达

位置：

- `test/architecture/layering_test.go:440-516`
- `.github/workflows/ci.yml:20`

把 `HEAD` 换成固定 commit 修复了完整仓库中的“提交后自毁”，但 GitHub Actions 的
`actions/checkout@v4` 默认 `fetch-depth: 1`；`build-test` 随后运行 `make test`，旧 commit
不在对象库中。开发者报告的“它是未来 HEAD 的祖先，所以永久可达”漏掉了真实 CI 的
浅克隆边界。

独立复现：把候选真正提交，再用 `git clone --depth=1 file://...` 克隆。commit 数为 1，
`git cat-file` 明确显示冻结 SHA 不可达；运行精确 test，四次 `git show` 均 exit 128，
`checked=0`，稳定失败。

修复建议：至少给 CI 的 `build-test` checkout 配 `fetch-depth: 0`。门禁保留当前
fail-closed 行为；这样以后误删完整历史配置会由门禁直接抓出。

### B2 — BLOCKER：sim fake-DNS 前置守卫破坏 hermetic gate，且诚实 NXDOMAIN 在 `set -e` 下直接退出

位置：

- `test/simcluster/lib/docker.sh:19-70`
- `test/simcluster/tests/simcluster-accel-final-review-test.sh:174-225`

`assert_host_dns_says_no` 使用：

```sh
_adns_out=$(getent hosts "$_adns_probe" 2>/dev/null | head -1)
```

`simcluster` 开启 `set -euo pipefail`。在诚实 resolver 上，`getent` 对 NXDOMAIN 返回 2，
assignment 自身也返回 2，脚本在执行 `[ -z "$_adns_out" ]` 前已经退出。因此“应当通过”
的宿主反而无法运行任何 `up`。

同时，所有调用 `cmd_up` 的 hermetic 测试现在继承真实宿主 DNS；当前宿主检测到 fake-IP
时，它们在被测 signal/temp-cleanup 路径之前退出。`tests/run-all.sh` 稳定红：
`simcluster-accel-final-review-test` 3 failures。一个声明“no docker, no server”的
hermetic gate 被改成宿主状态测试，而且新增前置守卫自己没有常驻真假两向测试。

独立复现：在 PATH 前放一个始终 exit 2 的 fake `getent`，以
`set -euo pipefail` source `docker.sh` 并调用守卫，得到 rc=2 且没有到达成功分支。

修复建议：

1. command substitution 后加显式 `|| true`，让 NXDOMAIN 按数据而不是 shell control-flow 处理；
2. 非 DNS 职责的 hermetic `cmd_up` 测试显式 `SIM_ALLOW_FAKE_DNS=1`；
3. 增加专门的 hermetic DNS preflight test，用 PATH fake `getent` 固定
   NXDOMAIN→pass、合成地址→refuse、override→pass 三条。

### M1 — MAJOR / SECURITY：TLS 门禁仍把无效 callback 和不同索引对象判为安全

位置：`test/architecture/tls_verify_pairing_test.go:174-508`

按 selector base 配对修复了最初的 `client` / `unrelated` 反例，但仍有两个逃逸：

1. assignment 和 composite literal 只看 callback 字段“在场”，不看值。
   `VerifyPeerCertificate: nil` / `c.VerifyConnection = nil` 被判为已验证；Go 的 nil callback
   不执行任何验证。
2. `selectorBaseName` 对 `IndexExpr` 丢弃 index，对 `CallExpr` 丢弃参数。
   `configs[0].InsecureSkipVerify=true` 与
   `configs[1].VerifyPeerCertificate=verifyChainToCA` 被折叠到同一个 `configs` bucket。
   `factory(0)` / `factory(1)` 同理。

两种变异都替换真实 `clusterTLSConfigs` 站点、保持精确站点数为 4，精确 TLS gate 均错误
PASS。新增 synthetic self-test 甚至把 `VerifyPeerCertificate = nil` 写成
`properlyPaired`，把缺陷固化成期望。

修复建议：callback 只有值不是字面量 nil 时才算在场；base key 应使用 Go AST 的完整稳定
渲染（保留 index 和 call arguments），无法渲染时每个表达式必须是独立 bucket，不能共享
`"?"`。

### M2 — MEDIUM：HTTP transient 具名表遗漏标准定义的 421 与 425

位置：`internal/agent/upgrade.go:270-334`

408/429/500/502/503/504 的拆分成立，但 421 `Misdirected Request` 可在不同连接重试，
425 `Too Early` 的定义就是要求/允许客户端重试。当前二者仍进入
`ErrUpgradeHTTPStatus` / exit 64 / fleet abort，继续把可自行恢复的状态说成错误参数。

依据：RFC 9110 §15.5.20 明确 421 可在不同连接重试；
RFC 8470 §5.2 明确 425 用于触发 retry。

修复建议：把 421、425加入具名集合与回归表；继续保留 501 作为永久边界。

### M3 — MEDIUM：HARD-REFUSE 诊断新增两个无界 DNS lookup

位置：`internal/clusteroffline/offline.go:488-548`

原 peer probe 有 1.5s `DialTimeout`；一旦 peer 被判活，新增 advice 又调用两次
`net.LookupHost`，没有 context/timeout。恢复命令的安全拒绝现在可能被失灵 resolver
额外阻塞数十秒甚至不确定时长。诊断不能把安全判定改成无界等待。

修复建议：用 `net.Resolver.LookupHost(ctx, ...)` 和短、具名 budget；超时/错误只意味着
不追加 advice，绝不能改变原 HARD-REFUSE。

## 已独立确认成立的修复

- B1 在完整、真正提交后的非浅仓库 PASS；问题只剩 CI shallow 边界。
- M3 required build-tag ledger：删除整个 `TestD9Matrix` 后精确门禁转红。
- M4 golangci path scope：只改名 `internal/broker.(*Broker).Run` 后精确门禁转红。
- M5 非目录 JetStream store：上一轮红测转绿，只有 ENOENT no-op。
- M6 structural golden：移走 golden 后文档化 bootstrap 成功；缺 sentinel 子进程测试通过。
- watch JSONL error schema/version、`error`/`errors` discriminator 测试通过。
- `ExitError.Code` 结构化分类与 prose 对抗测试通过。

## 验证记录

| 验证 | 结果 |
|---|---|
| `git diff --check` / gofmt | PASS |
| focused `cmd/tether agent natsconf clusteroffline architecture` | PASS |
| `make gates`（完整本地仓库） | PASS，lint 0 issues |
| `make test`（完整本地仓库） | PASS |
| affected packages `-race -count=1` | PASS |
| `make e2e-parallel` | ALL PASS，15/15、99 units、3m19s |
| post-commit full-history provenance | PASS |
| post-commit depth=1 provenance | FAIL，确认 B1 |
| TLS nil callback mutation | 门禁错误 PASS，确认 M1 |
| TLS indexed-object laundering mutation | 门禁错误 PASS，确认 M1 |
| D9 deletion / scoped Run rename mutations | 正确 FAIL |
| structural golden bootstrap | PASS |
| `test/simcluster/tests/run-all.sh` | FAIL，final-review 3 failures，确认 B2 |

## 阶段裁定

开发者候选先整体加入暂存，作为不可混淆的交付快照；随后由外部审查者直接修复
B1/B2/M1/M2/M3。所有修复、回归测试和最终报告留在暂存区外，供 `git diff` 对比。
