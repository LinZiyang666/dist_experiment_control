PASS - Round 2 external review: proxy-aware dial can be released. The previous credential-leak finding is fixed, no new blocking issue was found, and the relevant proxy, race, build, lint, vet, and full test gates pass in this workspace.

# proxy-aware-dial — 外审报告

## Round 2 — 外部复审（当前）

### 结论

**Pass。** 这轮复审只针对执行者在上一轮 High finding 后的修改，以及 proxy-aware dial 已暂存主体作为上下文。凭据泄漏修复成立：错误路径不再回显 raw proxy env、userinfo、password 或 host；新增测试覆盖 unsupported scheme 与 parse 失败两类路径。

### Tasklist / Review Surface

- [x] 重新确认当前 git 边界：proxy-aware dial 主体已暂存，`internal/proxydial/proxydial.go` 和 `proxydial_test.go` 有后续修复增量。
- [x] 复审上一轮 High：invalid / unsupported proxy URL 的错误信息是否仍可能泄漏凭据。
- [x] 复审 proxy-aware dial 关键路径：env 选择、fail-closed、NO_PROXY 目标判定、HTTP CONNECT、SOCKS5、ctl/agent/completion 接线、e2e 矩阵接入。
- [x] 复审范围边界：broker/proto/tunnel 不应接入 proxydial；go.mod/go.sum 不应新增依赖。
- [x] 运行相关测试、race 测试、lint、build、vet、full `make test`。
- [x] 更新外审报告并暂存全部文件。

### Findings

No blocking findings remain.

上一轮 High 已关闭：

- `internal/proxydial/proxydial.go` 现在把 parse 失败和 unsupported/malformed proxy URL 分开处理。parse 失败只返回通用错误；unsupported/malformed 只报告 `scheme`，不报告 raw URL、host 或 userinfo。
- `internal/proxydial/proxydial_test.go::TestOptions_FailClosedNoCredentialLeak` 覆盖了 `socks6://alice:s3cretpass@...`、`ftp://bob:hunter2@...` 和 parse-fail URL，断言 password、username、host 都不出现在错误里。

### Verification

- `go test -race ./internal/proxydial ./internal/cli ./internal/agent ./test/proxydial -count=1` passed.
- `go test -count=1 -tags e2e_matrix -run TestProxyDialMatrix -v ./test/e2e` passed.
- `make lint` passed with `0 issues`.
- `CGO_ENABLED=0 go build ./...` passed.
- `make test` passed.
- `go vet ./...` passed.
- `git diff --check` and `git diff --cached --check` passed.
- `go mod tidy` produced no `go.mod` / `go.sum` drift.
- Scope grep found no `proxydial` import/use in `internal/broker`, `internal/proto`, `internal/tunnel`, or `cmd/tether`.

---

## Round 1 — 外部审查（用户）

**结论：Fail，建议先修 1 个安全问题再继续。**

### High — 代理 URL 校验错误会泄漏凭据

`internal/proxydial/proxydial.go:79` 在代理 URL 无效或 scheme 不支持时把原始 `raw` 用 `%q` 放进错误信息。文档同时支持带认证代理（`docs/usage.md:580`）。

所以如果用户配置了：
```
HTTPS_PROXY=socks6://user:secret@127.0.0.1:7897
```
错误会直接包含 `user:secret`。这个错误会从 CLI/agent 连接路径向外返回，**agent 场景下尤其容易进 systemd/log 文件**。

**建议修法**：不要回显原始 proxy env；或者先 parse 成 URL 后用 `u.Redacted()`，parse 失败时也不要打印 raw。还应补一个 `Options` 单测覆盖 `user:secret` 不出现在错误里。

已跑的快速验证都通过：`go test ./internal/proxydial ./test/proxydial`（含 `-race`）、`go test ./internal/cli ./internal/agent ...`、`go test -tags e2e_matrix -run TestProxyDialMatrix ./test/e2e`、`git diff --check`。未改文件、未暂存临时 proxy 改造。

---

## 主进程回复（逐条处置，2026-06-21）

### High（凭据泄漏）— 已修

**采纳，且修得比建议更严**：错误**只报 scheme**，完全不回显 raw / userinfo / host（scheme 是唯一可操作信息、永不敏感）。`internal/proxydial/proxydial.go` Options 拆两支：
- `url.Parse` 失败 → 通用错误，**不含 raw**（`"could not parse the proxy URL from the proxy env var ..."`）。
- 解析成功但 host 空 / scheme 不支持 → `"unsupported or malformed proxy URL (scheme %q) ..."`，只带 `pu.Scheme`（如 `"socks6"`），**无 host、无 userinfo、无 raw**。

> 为何不用 `u.Redacted()`：`Redacted()` 只屏蔽 password、**保留 username**（仍 `user:xxxxx@host`），且回显 host。scheme-only 是纵深更深的选择（用户名/主机也可能敏感），且仍给出可操作信息（哪个 scheme 不支持 / 格式怎么写）。

**回归测试已补**：`internal/proxydial/proxydial_test.go::TestOptions_FailClosedNoCredentialLeak`——对 `socks6://alice:s3cretpass@...`、`ftp://bob:hunter2@...`、解析失败的 `http://carol:pw@[::1` 三类，断言错误**不含** password、username、host（`s3cretpass`/`alice`/`127.0.0.1` 等均不出现）。仍 fail-closed（设了代理却配错 → 报错而非静默裸连）。

**复验**：`go test -race ./internal/proxydial/...` 绿（含新测试）；`make lint` 0 issues；`go test ./internal/cli ./internal/agent ./internal/proxydial ./test/proxydial -count=1` 绿；`CGO_ENABLED=0 go build ./...` 绿。

### 门状态（修复后 — 全绿）
`build` ✓ · `vet` ✓ · `make lint` 0 issues ✓ · full `make test` ALL PASS ✓ · `-race`(proxydial/cli/agent) ✓ · `TestProxyDialMatrix -race` (e2e) ✓

请复审放行。
