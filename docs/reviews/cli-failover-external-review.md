# Fail - ctl/cli cluster broker auto-failover external review

Reviewer role: external reviewer. Scope: all unstaged / untracked cli-failover changes outside staging,
including `cmd/tether` ctl connection rewiring, `internal/cli` endpoint cache, `internal/clusterroster`
invite/dial helpers, agent dial-string lift, tests, and the new operational docs.

结论：Fail。Tier-1 invite seed 的主路径有基础实现，但本轮改动仍有用户可见断点：bootstrap-only
discovery invite 永远不会 warm HTTP manifest；`session create` 和 `login -s` 仍走旧的单点
`ConnectNATSWithNkey`，配置 broker 死掉时不会用已 pin 的 survivor。三条独立 reviewer regression 已复现。

## Tasklist / Review Surface

- [x] Scope census: enumerated tracked modified/deleted files and untracked additions outside staging.
- [x] Process/docs alignment: read `CLAUDE.md`, architecture/HA docs, `cli-failover-plan.md`, gotchas, and prior external review style.
- [x] Trust model review: OOB pin, discovery invite parsing, manifest verification, no HTTP/NATS TOFU.
- [x] Dial precedence review: File-only expansion, flag/env/default byte-equivalence, `TETHER_NO_DISCOVER`, floor-last builder.
- [x] CLI wiring review: checked ctl connection sites, direct legacy connects, login/session-create exceptions, and completion deferral.
- [x] Agent regression review: checked `effectiveDialURLs` lift to `clusterroster.BuildDialString`.
- [x] Persistence review: `cluster_endpoints.json` atomic write, 0600, corrupt-tolerant read, FloorURL keying.
- [x] Independent tests: added reviewer regressions in `cmd/tether/cli_failover_external_review_test.go`.
- [x] Verification: ran focused unit/reviewer tests and `git diff --check`.
- [x] Report: this file.

## Findings

### F1 - High: bootstrap-only discovery never fetches the signed manifest

Locations:
- `cmd/tether/ctl_connect.go:33` computes `dial := cli.DialFor(...)`.
- `cmd/tether/ctl_connect.go:45` only calls `refreshCtlEndpoints` when `dial != base`.
- `internal/cli/cluster_endpoints.go:180`-`186` returns exactly `base` when the cache has only `BootstrapURL` and no learned roster/seeds/invite seed yet.

Why this fails:

`tether cluster invite --bootstrap <url>` is a valid advertised path, and `ParseDiscoveryInvite` accepts
bootstrap-only invites. After `cluster pin`, the cache is OOB-pinned and has a manifest URL, but no dial
expansion yet. `DialFor` therefore returns `base`; `connectCtl` treats that as a non-expanded path and
skips refresh. Result: tier-2 discovery never warms, so a bootstrap-only user never learns the survivor
endpoints.

Reviewer regression:
- `TestCLIExternalReviewBootstrapOnlyPinWarmsManifest` fails: cache remains `Roster=nil`, `Seeds=nil`, `SeedGen=0` after a successful connect to the floor broker.

Expected fix direction:

Gate refresh on cache eligibility (`SourceFile`/not env/flag, pin present, `FloorURL==base`, `BootstrapURL!= ""`,
TTL expired), not on `dial != base`. Also consider doing the same best-effort refresh in `cluster pin` so a
bootstrap-only invite is useful immediately.

### F2 - High: `session create` still cannot fail over when the configured broker is dead

Locations:
- `cmd/tether/session.go:38` resolves the single base URL.
- `cmd/tether/session.go:43` calls `cli.ConnectNATSWithNkey(natsURL, ...)` directly instead of `connectCtl`.

Why this fails:

`session create` is a ctl-over-NATS command and was in the plan's `session.go` rewiring scope. With a persisted
`broker_url` pointing at a dead primary and a pinned cache containing a live invite seed, the command still dials
only the dead primary and fails before sending `SessionCreateReq`.

Reviewer regression:
- `TestCLIExternalReviewSessionCreateUsesFailoverDial` fails with `nats: no servers available for connection`
even though the survivor NATS server is in `cluster_endpoints.json`.

Expected fix direction:

Use the same expanded-dial path as `session ls/rm` while preserving the single human `base` for error messages
and broker_url persistence. Explicit `--nats-url` should still pin single.

### F3 - High: `login -s` still cannot fail over after logout / reactivation

Locations:
- `cmd/tether/login.go:49`-`55` direct-connects for pure auth.
- `cmd/tether/login.go:74`-`80` direct-connects for activation, including PIN join.

Why this fails:

After a user logs out, changes shells, or needs to activate a session again, `tether login -s <sid>` is the
re-entry command. It still ignores the pinned endpoint cache and dials only the dead floor broker. That leaves
a common recovery path broken even though normal commands like `ps`, `run`, and `node ls` were rewired.

Reviewer regression:
- `TestCLIExternalReviewLoginActivationUsesFailoverDial` fails with `nats: no servers available for connection`
despite a live invite seed in the cache.

Expected fix direction:

Add a `connectCtl` variant that accepts extra NATS options (`nats.Token(pin)` for first join) and use it for
login activation when the URL source is the persisted file. Keep explicit `--broker`/`--nats-url` pinned-single.

## Questions / Concerns

- `docs/reviews/cli-failover-plan.md` listed `login --account-pub` / `--bootstrap` and eager cache warm, but
  this implementation only adds `cluster pin`. Is that a deliberate scope cut? If so, the plan should be amended
  because the current text overstates the shipped surface.
- The plan also says `logout` should remove `cluster_endpoints.json` for clean cluster switch, but `cmd/tether/login.go:128`
  still only clears `current_session`. FloorURL keying prevents most stale-cache use, but same-host cluster
  reprovisioning can still keep an old pin until `cluster pin --force`.
- `ReadClusterEndpoints` ignores `SchemaVersion`. The signed roster/seed children reject future schemas, but the
  cache envelope itself does not. If future versions change `InviteSeeds` semantics, an old binary will still use them.

## Confirmed Clean / Lower Risk

- `DialFor` keeps flag/env/default and `TETHER_NO_DISCOVER` pinned single, and existing unit tests cover those paths.
- Signed roster/seeds are re-verified against the pin on every read; verify failures drop that tier.
- Discovery invite mint/parse validates account pub and seed/bootstrap schemes; SID-less discovery tokens still fail
  the agent `ParseInvite` path.
- The agent `effectiveDialURLs` change is a pure lift to `BuildDialString`; focused agent roster tests pass.

## Verification

Passing:

- `GOCACHE=/tmp/tether-go-build go test ./internal/cli ./internal/clusterroster -run 'TestDialFor|TestBuildDialString|TestDiscoveryInvite|TestSeedBundle|TestRoster' -count=1`
- `GOCACHE=/tmp/tether-go-build go test ./internal/agent -run 'Test.*Roster|TestSeed|TestManifest|TestAgentJoin|TestAgentConfigRefresh|TestAgentDoctor' -count=1`
- `GOCACHE=/tmp/tether-go-build go test ./cmd/tether -run 'TestRefreshCtlEndpointsNoHTTPTOFU|TestRefreshCtlEndpointsFloorMismatchNoop|TestDialFor|TestBuildDialString|TestDiscoveryInvite' -count=1`
- `git diff --check`

Failing reviewer regressions:

- `GOCACHE=/tmp/tether-go-build go test ./cmd/tether -run 'TestCLIExternalReview' -count=1`
  - `TestCLIExternalReviewBootstrapOnlyPinWarmsManifest`
  - `TestCLIExternalReviewSessionCreateUsesFailoverDial`
  - `TestCLIExternalReviewLoginActivationUsesFailoverDial`

Not run:

- Full `make test`, `make e2e`, and `make lint`, because deterministic reviewer regressions already make this external review a Fail.

---

## 主进程回复 (Round 1 → 全部采纳 + 修复)

所有三条 finding 成立(我之前把 `session create`/`login` 留单端点、并把 refresh gate 错绑在 `dial != base` 上)。逐条修复,reviewer 的三个 regression 现已全部 PASS;`make lint` 0 issues、`make test` 无 FAIL。

### F1 — bootstrap-only discovery never warms — FIXED
- `cmd/tether/ctl_connect.go`:把 refresh 触发从 `if dial != base` 改为**源可扩展性**判定
  (`!flagChanged && $TETHER_NATS_URL==""`,即持久 `broker_url`/默认源)。bootstrap-only pin 此时 `dial==base`
  但仍会触发 `refreshCtlEndpoints` → 拉签名 manifest → `AdoptDecision`(pin 已设、不 TOFU)→ 写回缓存。
- `cmd/tether/cluster_pin.go`:`cluster pin` 写缓存后**立即 best-effort refresh**(有 bootstrap URL 时),
  使 bootstrap-only invite 即刻可用,不必等下一次连接。
- 验证:`TestCLIExternalReviewBootstrapOnlyPinWarmsManifest` 现 PASS(`ce.Seeds!=nil`、`SeedGen==1`)。

### F2 — `session create` 不 failover — FIXED
- `cmd/tether/session.go`:`session create` 改走 `connectCtl`(与 `session ls/rm` 同路)。File 源 + 已 pin
  缓存时 failover 到 survivor;`--nats-url` 显式覆盖仍钉单;`broker_url` 持久化仍写 human `base`(不写 failover 目标)。
- 验证:`TestCLIExternalReviewSessionCreateUsesFailoverDial` 现 PASS。

### F3 — `login -s` 不 failover — FIXED
- `cmd/tether/ctl_connect.go`:新增 `connectCtlOpts`(接受额外 NATS options,如 `nats.Token(pin)` 首次 join),
  `connectCtl` 成为其薄封装。`connectCtl`/`connectCtlOpts` 现同时识别 **`--broker`**(login 的 `--nats-url` 别名)→
  显式 `--broker`/`--nats-url` 仍钉单。
- `cmd/tether/login.go`:pure-auth(site 51)→ `connectCtl`;activation(site 78,含 PIN join)→ `connectCtlOpts`。
  auth_callout 拒绝的措辞由 `connectError` 的 "Authorization Violation" 分支正确给出(非 "cannot reach broker")。
- 验证:`TestCLIExternalReviewLoginActivationUsesFailoverDial` 现 PASS;`TestLoginAcceptsExoticSidStrings` 仍 PASS。

### Q1 — plan 列了 `login --account-pub`/`--bootstrap`+eager warm,但只发了 `cluster pin` — 已修正 plan
- 这是**有意的范围收敛**:`cluster pin <invite>` 是唯一 pin 建立路径,`login --account-pub` 不再单列(冗余)。
  `cluster-failover-plan.md` 的 FILES 段已改为反映实发面;eager warm 已在 `cluster pin` 实现(见 F1)。

### Q2 — `logout` 不清缓存 — 有意,plan 已修正
- **不在 logout 清 `cluster_endpoints.json`**:① cache 按 `FloorURL=broker_url` keyed,**cluster 切换(换 broker_url)
  时旧 cache 因 FloorURL 不匹配被自动忽略**;② 签名 roster/seeds 每次读都对 pin 重验,陌生集群的 manifest **fail-closed
  丢弃**(非安全洞)。仅**同主机重供给(broker_url 不变、account 换)**需 `cluster pin --force` 重 pin(已支持)。
  不在 logout 清缓存可保「同集群 logout/login」顺滑。plan 的 logout 条目已据此修正。

### Q3 — `ReadClusterEndpoints` 忽略 envelope SchemaVersion — FIXED
- `internal/cli/cluster_endpoints.go`:`ReadClusterEndpoints` 现对 `SchemaVersion > ClusterEndpointsSchemaVersion`
  的缓存**当作不存在**(floor-only),老二进制不会误用未来语义的 `InviteSeeds`/字段。

**复验**:`go test ./cmd/tether -run 'TestCLIExternalReview' -count=1` 全 PASS;`make lint` 0 issues;`make test` 无 FAIL;
`-race ./internal/cli ./internal/clusterroster` 通过。请 round-2 re-review。

---

## Round 2 — 外部复审

**Pass。** Round-1 的三条 High finding 已关闭，未发现新的 blocking issue。

复审重点：
- F1：`connectCtlOpts` 现在按源可扩展性触发 refresh，bootstrap-only pin 在 `dial==base` 时也会 warm；`cluster pin` 写缓存后也会 best-effort refresh。
- F2：`session create` 已接入 `connectCtl`，保留 explicit `--nats-url` pinned-single 语义。
- F3：`login` pure-auth 和 activation 均接入统一连接路径，activation 通过 `connectCtlOpts` 保留 `nats.Token(pin)`；`--broker` alias 被纳入 override 检测。
- Q3：future cache envelope schema 被当作 absent，回落 floor-only。

剩余非阻塞疑问：plan §PERSISTENCE 仍有一句 "`cluster pin <invite>` or `login --account-pub`"，与后文 Q1 裁定不完全一致；不影响代码放行，但建议后续文档清一下。

Round-2 verification:
- `GOCACHE=/tmp/tether-go-build go test ./cmd/tether -run 'TestCLIExternalReview|TestRefreshCtlEndpointsNoHTTPTOFU|TestRefreshCtlEndpointsFloorMismatchNoop|TestLoginSessionWithoutPINRequiresMembershipVerification|TestLoginAcceptsExoticSidStrings' -count=1` passed.
- `GOCACHE=/tmp/tether-go-build go test ./internal/cli ./internal/clusterroster -run 'TestDialFor|TestResolveBrokerURLSource|TestClusterEndpoints|TestBuildDialString|TestDiscoveryInvite|TestSeedBundle|TestRoster' -count=1` passed.
- `GOCACHE=/tmp/tether-go-build go test ./cmd/tether ./internal/cli ./internal/clusterroster ./internal/agent -count=1` passed.
- `GOCACHE=/tmp/tether-go-build go test -race ./internal/cli ./internal/clusterroster -count=1` passed.
- `GOCACHE=/tmp/tether-go-build make lint` passed with `0 issues` (cache-write warnings only, due sandbox read-only home cache).
