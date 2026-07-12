# S2 Simcluster Batch — Consolidated Adversarial Review (SYNTHESIZER)

> **本报告 = Stage-C step 4（对抗内审）产出**：5 视角 critic（false-green / anti-masking / source-fidelity /
> drill-craft / feasibility）+ 1 synth，全 Opus 4.8。下方「主进程裁决」= Stage-C step 5（主进程逐条评估+修复）。

---

## 主进程裁决（Stage-C step 5；逐条 accept/fix/reject + commit-pending 落盘）

**总裁定采纳内审总裁定**：三头条 gotcha（#25/#26/#27）**真暴露非擦屁股**；调试修复全为合法测试-bug 修复。
**R1 是唯一 CRITICAL 错-oracle**（TLS 负例走 curl 旁路而非产品 Go-x509 fetcher）——**全采纳并已修**。

| # | 裁决 | 落地 |
|---|---|---|
| **R1** CRITICAL | **采纳** | 82 I-NEG-SAN/CA 改走产品 `agent config refresh`（`_mk_yaml` 构造 scratch agent.yaml→坏 front），sig 钉 Go x509 `certificate is valid for .*not brk1` / `signed by unknown authority`；curl 降为可选次级。活体重验通过。 |
| **R2** | 采纳 | 82 P0 去 `\|\| true; true`，断真空-seed 签名 + `! grep nats://\|wss://`。 |
| **R3** | 采纳 | 82 C1 改 `poll_until 45` 过 30s 重签节流。 |
| **R4/R23** | 采纳 | 80 Oracle② sub 加正控 `_i2_posA/B_sub` + 恢复 `.*s\.ops`/`.*s\.lab` subject 锚 + `--count 1`。 |
| **R5** | 采纳（部分） | 80 事件 oracle 改**单行绑定** piped-grep（防三 token 落不同行）；`$WFP` fp-filter 因捕获脆弱不加，靠 ev_stop-before-Arm-R 序守卫（已注明）。 |
| **R6** | 采纳 | 81 C-base-proc 加 broker `ps` RUNNING 基线（坐实 FK-cascade divergence）。 |
| **R7** | 采纳 | 81…→82 T2 doctor 断 exit77 + `roster does NOT verify against the pinned account`（非仅 exit≠0）。 |
| **R8** | 采纳 | 82 J5 doctor 断 `well-known manifest verifies against the pin` + `! FATAL`（非裸 exit0）。 |
| **R9** | 采纳 | 81 header(f) + ledger DOC-11 + plan E 注 + inventory 全订正为 CONNECT-deny（IsActive gate = hermetic-only）。 |
| **R10** | 采纳 | 82 加 `trap … ingress_down EXIT INT TERM`（防 netns-owner 先于 sidecar 被删）。 |
| **R11** | 采纳 | 81 expose 口改 `--json \| jq .port`（不 grep 2>&1 人读行）。 |
| **R12** | 采纳 | run-drills FLAKE_SIG 加 `reaches VOTER\|did not reach VOTER`。 |
| **R13** | 采纳 | 81 evict sig → `evicted sid=lab nid=agt1.*broadcast=true`（3 处）。 |
| **R14** | 采纳 | 82 T3/T4/T5 加 `env -u TETHER_DEV_NO_AUTH`。 |
| **R15/R22** | 采纳 | 82 SETUP-27 前加 leader-health floor（`000` 归因 manifest 口非死 broker）。 |
| **R16** | 采纳 | 并入 R1（sig 钉精确 Go x509 串）。 |
| **R17** | **驳回原议、改按实测** | 审查称 2nd rm→`not_found`；**实测核实 rm 以 `tether-cli:<sid>` activated 连接（session.go:163），lab 已删→2nd rm 亦 CONNECT-deny**，rm-API `not_found` deploy 不可达=hermetic-only。E3c 改断 CONNECT-deny + 注明。 |
| **R18** | 采纳 | 删死代码 `_agt1_fp`（D1 用 md5 更强）。 |
| **R19** | 采纳 | 81/82 两处 `sleep` → `poll_until`。 |
| **R20** | 采纳 | ingress readiness 改从 broker 容器内 curl 共享 netns sidecar（不每 poll 起临时容器）。 |
| **R21** | 采纳 | 81 header(c) 与 B3 注调和（去 server-side-discriminator 子句）。 |
| **R24** | 采纳 | 台账加 DOC-15（manifest 30s 新鲜度滞后，产品 doc 缺口）。 |
| **R25** | 采纳 | 引用订正：applyClusterSeam 定义 `:880`（`:799`=调用点）；agent.go:859 打印/:1538 检测。 |

**VERIFIED HONEST（内审明确「非掩盖」）全部保留**（见文末 §VERIFIED HONEST）。所有修复已重跑活体验证（80/81/82 全绿）。
**本裁决与修复停在此**——按流程进用户外审；未 commit（外审过后主进程 commit/push）。

---

Targets: `test/simcluster/drills/{80,81,82}.sh` + harness (`drills/lib/{ident,ingress,agentyaml}.sh`,
`lib/assert.sh`, `image/ingress-proxy.py`, `image/provision-node.sh`) + `docs/reviews/s2-plan.md` +
`docs/deploy-tier-gotchas.md`. Consolidated from five independent lane critiques (false-green,
anti-masking, source-fidelity, drill-craft, feasibility). Every sig/string/file:line re-verified against
tether source under `internal/` + `cmd/tether/`; the two headline CRITICAL/MAJOR claims (curl-vs-agent
fetcher, P0 no-op) plus the drill 80/81/82 lines were re-read directly by the synthesizer.

---

## VERDICT SUMMARY

**CRITICAL false-greens hiding a CURRENTLY-LIVE tether defect: 0.** All five lanes independently agree the
three headline gotchas are **genuinely exposed, not papered over**:
- **#25** (no PIN rate-limiter) — verified absent in source (no counter/bucket/window/`ClientInformation.Host`
  read in `internal/authcallout/handler.go` or `internal/broker/authcallout.go`); drill 80 Arm R asserts a
  real `RN==10` >10 baseline (same ctl1 IP, correct PINs) and inverts `assert_ok` on the 11th success.
- **#26** (evict leaks a managed OS child) — verified `internal/adminsock/server.go handleEvict` DELETEs only
  `agent_provisioning`/`nodes` (FK cascade), no proc-kill; agent `agent_evicted` path is `cancelRun()` only
  (`internal/agent/agent.go:711-728`); the composite oracle `_leak_present = daemon-gone AND child-alive`
  (81:149-150) cannot pass for the wrong reason; the systemd cgroup counter-probe is honest.
- **#27** (manifest_listen default-off) — verified `broker.go:753` gates bind on `ManifestAddr != ""` and
  `applyClusterSeam` omits `manifest_listen`; the `000` gap (82:75-76) is asserted BEFORE the labeled
  `ingress_enable_manifest` workaround (Mandate 2 held); the enable is faithful operator provisioning (no
  `/etc/hosts` spoof, no loopback rebind, no `TETHER_DEV_NO_AUTH`, manifest still product-signed +
  agent-verified).

**CRITICAL wrong-oracle that MASKS a would-be regression: 1** — **R1** (TLS negatives run curl/OpenSSL, not
the product Go-x509 fetcher). It is green today for a *near-right* reason but structurally cannot catch an
`InsecureSkipVerify` regression in the exact code path S0-ingress exists to guard. All three source-facing
lanes rank it their top finding.

**Overall honest assessment:** the S2 greens are **genuine exposure, not accommodation** — the user's
"配合 tether 擦屁股" suspicion is largely unfounded. The debug fixes that were self-flagged (`--broker`,
`2>&1` on admin nodes, E3 sig reframe, dropped B3 discriminator) are all verified as legitimate test-bug
fixes, not weakenings-to-green. BUT the batch shipped **one CRITICAL wrong-oracle (R1)** and a cluster of
**MAJOR oracle-weakenings below the plan spec** (R2 vacuous baseline, R4/R5/R6/R7 dropped
positive-controls/anchors/DB-baselines that let a *future* regression stay green, R9 stale coverage labels)
plus **robustness/flake gaps** (R10 host-leak trap, R11 fragile port parse, R12 non-retried grow flake). The
main process must act on R1 (CRITICAL) and every MAJOR before this batch is done. Nothing here should block on
a re-run of the deploy tier except after R1/R10/R11 are fixed.

---

## CRITICAL

### R1 — 82 I-NEG-SAN / I-NEG-CA test curl's OpenSSL stack, NOT the product agent Go-x509 fetch → masks an `InsecureSkipVerify`/permissive-RootCAs regression in the exact path S0-ingress exists to guard
*(source F1, craft C1, false-green M1 — unanimous top finding)*
- **Where:** `82-agent-onboarding-invite.sh:48-56` — `_ineg_san` runs `"$SIM" exec agt1 -- curl -sS --cacert
  "$CA" https://brk1:8444/… | grep -qiE 'certificate|SSL|verify|not brk1'`; `_ineg_ca` runs
  `curl -sS https://brk1/… | grep -qiE 'self.signed|unknown|…|certificate'`. Both negatives are **curl**.
- **Why it is wrong/masking:** the load-bearing POSITIVE (J4 `_j4_refresh`, 82:47/114) correctly drives the
  product path — `agent config refresh --once` → `clusterroster.FetchManifest`, which (re-verified,
  `internal/clusterroster/fetch.go:36-38`) builds `&http.Client{Transport:&http.Transport{Proxy:…}}` with
  **no `InsecureSkipVerify` and no custom `RootCAs`** (system roots only; a bad cert surfaces as
  `clusterroster: fetch manifest: … x509: …` wrapped at `fetch.go:52`). The NEGATIVES do not exercise that
  code — curl uses OpenSSL, an independent verifier. Consequence: if the agent fetcher ever regressed to
  `InsecureSkipVerify:true` (a catastrophic TLS bypass), **J4 stays green** (valid cert), **M2 stays green**
  (curl, valid cert), and **both curl negatives stay green** (curl still rejects the bad cert) → the drill is
  fully GREEN while the product silently accepts forged fronts. This is exactly the Mandate-4 inversion: the
  sim swapped the load-bearing product-path assertion for an easier off-path one, and directly contradicts
  plan §2.2 (`s2-plan.md:188-192,530-531`) which specified the negatives run through `agent config refresh`
  matching `x509: certificate is valid for … not brk1` / `x509: certificate signed by unknown authority`.
- **Fix:** route BOTH negatives through the agent fetcher, isolated via a scratch `TETHER_HOME`:
  - I-NEG-SAN: write a throwaway `agent.yaml` with `bootstrap_url: https://brk1:8444/.well-known/tether/cluster.json`
    (the wrong-SAN front) + the real pinned `account_pub`, keep the CA trusted, then
    `assert_refuses` sig `x509: certificate is valid for .*not brk1` on `agent config refresh --once --session lab`.
  - I-NEG-CA: on agt2 (CA-un-injected) same pattern with `bootstrap_url: https://brk1/…`, sig
    `x509: certificate signed by unknown authority`.
  Keep the curl checks only as an explicitly-labeled *secondary* sanity, never the sole oracle. (This also
  subsumes **R16** — the current `certificate|SSL|verify` sig is too broad and would match an unrelated TLS
  failure such as a dead front or handshake timeout.)

---

## MAJOR

### R2 — 82 P0 baseline is a VACUOUS always-green (`… || true; true`) — it can never go RED, so "no seed bundle before first publish" asserts nothing
*(feasibility MAJOR-5, source F8, anti-mask M1)*
- **Where:** `82-…invite.sh:87-88` — `assert_ok "P0 …" sh -c "… tether cluster seeds show 2>&1 | grep -qiE
  'no seed|not published|endpoints:[[:space:]]*$' || true; true"`. Confirmed by direct read: the `sh -c` body
  ends `… || true; true`, so it **always exits 0**; `assert_ok` (`lib/assert.sh:23-28`) can never fail.
- **Why it is wrong:** if a stale instance or a regression left seeds already published before the first
  `publish` (`cmd/tether/cluster_seeds.go:104` prints the empty-endpoints line the sig *would* match), P0
  still reports GREEN — a green for no reason, precisely the mission-forbidden "green for the wrong reason."
- **Fix:** drop the `|| true; true` and assert the real signature so a pre-seeded state RED-s:
  `sh -c "\"$SIM\" exec brk1 -- runuser -u tether -- tether cluster seeds show 2>&1 | grep -qiE 'no signed seed|not published|endpoints:[[:space:]]*$'"`.
  Stronger: capture `seeds show` and assert it contains NO `nats://`/`wss://` endpoint line
  (`assert_refuses`/`! grep -q 'tether-invite'`).

### R3 — 82 C1 grow-convergence is a single-shot `config refresh`, not a poll, despite the broker's ≥30s manifest re-sign throttle → false-RED flake on a state-change arm
*(feasibility MAJOR-2, craft M2, false-green m1)*
- **Where:** `82-…invite.sh:128-129` — one `agent config refresh --once` then `[ "$G2" -gt "$G1" ]`, no
  `poll_until`. M1/M2 (82:101-103) correctly `poll_until 45 3` past the same throttle; C1 does not.
- **Why it is weak:** verified `internal/broker/cluster_manifest.go:22` (`manifestRecheckInterval = 30s`) —
  the broker serves cached manifest bytes until `nextCheckAt = signedAt + 30s`, only then re-signs to reflect
  the post-`grow brk2` roster generation. If C1's single fetch lands <30s after the last manifest touch (J5
  doctor at 82:119, or I-NEG-CA at 82:124), it reads the STALE pre-grow gen → `G2 == G1` → assert fails
  spuriously. It is only usually-green because `cmd_grow` incidentally takes >30s — an implicit timing
  dependency, not a guarantee. Gen is monotone so this is a false-RED (never a false-green), but it is the
  exact flake class `poll_until` exists to kill, and it is the only convergence arm in the batch done without
  a poll.
- **Fix:** `poll_until 45 3 "roster_gen grew past $G1" -- sh -c 'G2=$(… config refresh --once …); [ -n "$G2" ] && [ "$G2" -gt '"$G1"' ]'` (45s guarantees crossing one 30s boundary, same reasoning that makes M1/M2 safe).

### R4 — 80 Oracle ② sub-deny arms have NO positive SUB control and dropped the plan-specified cross-tenant subject anchor → a "empty member Sub.Allow" regression stays GREEN
*(craft M1, false-green m2)*
- **Where:** `80-…isolation.sh:59-60` (`_i2_ab_sub`/`_i2_ba_sub` grep bare `Permissions Violation for
  Subscription to`, no `.*s\.ops`/`.*s\.lab`); the only positive controls (I2-posA/posB, 80:97-98) are PUB.
- **Why it is weak:** verified a lab member's JWT allows sub only `tether.v2.s.lab.audit.>` + `tether.v2.sys.events`
  (`internal/auth/permissions.go:135-148`), excludes `s.ops.*` — so today the deny is real. But (1) the sig
  dropped the subject qualifier (`s2-plan.md:300`), so it matches a violation for *any* subject, not the
  cross-tenant one; and (2) there is no positive SUB control proving A *can* sub its own `s.lab.audit.>`. A
  refactor that empties member `Sub.Allow` would still produce a "Subscription to" violation on ops.audit →
  both sub arms stay GREEN while legitimate same-tenant audit subscriptions silently break. (The PUB arms are
  sound — they carry positive controls and target the cross-tenant subject.)
- **Fix:** add a positive sub control (`nats_as A "$SID_A" sub 'tether.v2.s.lab.audit.>' --timeout 3s`
  asserting NO `Permissions Violation`) and restore `.*s\.ops` / `.*s\.lab` subject anchors on all four deny
  sigs. (Subsumes **R23**: also restore the `--count 1` the plan specified, 80:59-60, so each denied sub
  returns immediately instead of waiting the full 3s timeout.)

### R5 — 80 event oracles use three INDEPENDENT greps over the whole capture (not single-event/jq bound) and dropped the plan's W-fingerprint filter → latent false-green if Arm R is ever reordered
*(craft M6, source F5, false-green m3)*
- **Where:** `80-…isolation.sh:52-53` — `_ev_pinfailed`/`_ev_joined` do `grep -q '<tok1>' && grep -q '<tok2>'
  && grep -q '<tok3>'` across `$EVCAP`; the three tokens may land on DIFFERENT event lines.
- **Why it is weak:** verified sys.events are flat single-line JSON (`internal/broker/audit.go:36-48`) and the
  field shapes are correct (`member_joined{sid,fp,via:pin,role:ctl}` `handler.go:353`; `pin_failed{sid,fp,role:ctl}`
  `:348`). It is honest TODAY only because ordering keeps the capture to W's two events (sub starts 80:127,
  after all setup joins; Arm R runs after `ev_stop` 80:140). Plan §3.1 required a `"fp":"$WFP"` filter
  (+ an E-wfp capture) which the drill dropped; the moment Arm R is reordered before `ev_stop` or any pre-sub
  event leaks, the three-independent-grep oracle over-matches → false-green.
- **Fix:** match a SINGLE event object per line, e.g. `jq -e 'select(.type=="member_joined" and .sid=="lab"
  and .via=="pin")'` per captured line, and restore the `"fp":"$WFP"` binding as belt-and-suspenders.

### R6 — 81 C-base-proc has no broker-DB-row baseline → C-brk's "FK-cascade removed the proc row" (the DIVERGENCE half of #26) is ungrounded
*(anti-mask M3, craft M4)*
- **Where:** `81-…session-rm.sh:130-131` — C-base-proc asserts only `_child_alive` (`pgrep -f 'sleep 999999'`
  on the agt1 host). C-brk (81:145-146) then asserts the post-evict broker view lacks the child. But the
  baseline never proves the broker's `processes` table held a RUNNING row for it.
- **Why it is weak:** #26's divergence claim is "OS child survives WHILE the broker DB row is FK-cascade
  deleted." Verified `tether exec agt1 -- sleep 999999` DOES register a managed proc — the agent emits
  `proc.started` (`internal/agent/exec.go`) → broker `handleProcEvent`→`recordProc` inserts into `processes`
  (`internal/broker/exec.go:150`), FK-cascaded on node delete. So the row *should* exist pre-evict. But if the
  `proc.started` event ever raced/missed, C-brk passes vacuously (row was never there) and the "cascade
  removed it = divergence" narrative is fake. Plan `s2-plan.md:432` explicitly wanted `CTL(ps) … RUNNING` in
  the baseline; the drill dropped it. (The core leak oracle `_leak_present` at 81:149-150 is sound; this only
  hardens the DB-side leg.)
- **Fix:** add a broker-view baseline before evict:
  `poll_until 15 1 "sleep 999999 RUNNING in broker ps" -- sh -c "\"$SIM\" ctl -- ps -a 2>/dev/null | grep 'sleep 999999' | grep -q RUNNING"`,
  so C-brk's absence check proves an actual FK cascade.

### R7 — 82 T2 doctor sub-oracle is masked by an incidental identity-FATAL → the intended manifest-forgery FATAL (the DOC-7 point) is never verified
*(craft M5, source F3)*
- **Where:** `82-…invite.sh:141-145` — T2's third clause is `! … agent doctor … >/dev/null 2>&1`, i.e.
  exit≠0 only.
- **Why it is masking:** verified `agent doctor` accumulates all checks (no short-circuit) and maps ANY FATAL
  to exit 77 (`cmd/tether/agent_doctor.go:42-49`). The `.tether-forge2` scratch home is joined WITHOUT
  `--start`, so `keys/agent.nk` is never minted → the **identity** check is FATAL independently of the
  manifest (`agent_doctor.go:79-84`). So T2's "doctor FATAL" is guaranteed by the missing key alone; a
  regression that removed the manifest-MITM FATAL branch (`agent_doctor.go:125`, `roster does NOT verify
  against the pinned account`) would leave T2 GREEN. (T2 is not fully masked — the `! test -e
  roster_cache.json` sub-oracle at 82:144 genuinely proves the forgery via AdoptDecision rejecting the
  DECOY-pinned manifest — so the DOC-7 core survives; only the doctor leg is vacuous.)
- **Fix:** assert doctor exit code == 77 (`internal/cli/exitcode.go:34`) AND
  `grep -q 'roster does NOT verify against the pinned account'`; keep the identity-FATAL as an explicitly
  noted co-driver, not the thing being asserted.

### R8 — 82 J5 doctor asserts exit-0 only; a WARN-downgraded (unverified) manifest still exits 0, so the "manifest verifies against the pin" green is unfounded
*(source F2)*
- **Where:** `82-…invite.sh:119-120` — `assert_ok "J5 … (manifest verifies against the pin)" AJOIN agent
  doctor --session "$SID"` (bare exit-0).
- **Why it is weak:** `cmd/tether/agent_doctor.go:122-123` maps a manifest FETCH FAILURE to **WARN**, not
  FATAL — doctor still exits 0. So exit-0 does NOT prove the manifest verified against the pin (it is also 0
  when the ingress front is momentarily down and the fetch WARNs out). The label overclaims. (Partly mitigated
  by J4/M2 proving verification separately, so not a false-green in this run — but the assertion as written
  doesn't demonstrate what it claims.)
- **Fix:** capture doctor stdout, assert exit 0 AND `grep -q 'well-known manifest verifies against the pin'`
  (`agent_doctor.go:134`) AND `! grep -q FATAL`.

### R9 — 81 E3 sig reframe is HONEST, but header guard (f) + ledger DOC-11 + plan inventory still overclaim the app-layer IsActive gate (a stale coverage-label misattribution)
*(anti-mask M2, craft M3)*
- **Where:** drill `81-…session-rm.sh` header guard **(f)** + body assertions **E3a/E3b (81:186-191)**; ledger
  `docs/deploy-tier-gotchas.md:131-136` (DOC-11); plan `s2-plan.md:656`.
- **Why it is a finding (drill is honest, labels are stale):** the sig change from
  `session_not_found_or_deleting` to `auth_callout rejected|Authorization Violation` is **correct** — verified
  after N=1 synchronous `session rm`, `session.IsActive` returns `(false,nil)` for the absent row
  (`internal/session/session.go:133-142`), so the ctl's next session-scoped CONNECT is denied at auth_callout
  (`ensureMember` → `handler.go:317-319` → generic `Authorization Violation` → `error_hints.go:147`), never
  reaching the app-layer `internal/broker/exec.go:49-55` gate that emits `session_not_found_or_deleting`. The
  old sig would have been permanently RED, so this is a legitimate test-bug fix, not a weakening-to-green. BUT
  header (f) still says the probe verifies "the REAL refusal mechanism (broker-side IsActive gate)" and DOC-11
  + the inventory still credit "IsActive gate E3a" — both now FALSE. A maintainer reading the header/ledger
  would expect the `session_not_found_or_deleting` sig and mis-flag the drill; the app-layer gate is genuinely
  **hermetic-only / unreachable at deploy tier in N=1** and NOT covered here.
- **Fix:** rewrite guard (f) to say the probe pins the auth_callout CONNECT-deny path
  (ensureMember→session-not-active, broker-side, no agent broadcast — DOC-11's core claim still holds); mark
  the `exec.go:49-55` `session_not_found_or_deleting` app-layer gate as hermetic-only, NOT deploy-covered in
  N=1. Update DOC-11 and `s2-plan.md:656` inventory to match.

### R10 — 82 has NO cleanup trap → on abnormal exit `cmd_nuke` removes the netns-owner broker BEFORE its netns-sharing ingress sidecars → broker + volumes + network leak on the shared weilandserver
*(feasibility MAJOR-1)*
- **Where:** `82-…invite.sh` has no `trap` (only the linear tail `ingress_down brk1 443/8444` at 82:177-178).
  The reap path is `cmd_drill` (`simcluster:485-492`) → `cmd_nuke` (`simcluster:468-469`) iterating
  `list_nodes` **sorted** (`lib/docker.sh:69`), giving order
  `agt1, agt2, brk1, brk1-ingress-443, brk1-ingress-8444, …` — so `brk1` (the netns provider) is `docker rm
  -f`'d FIRST while the two `--network container:brk1` sidecars still share its namespace.
- **Why it is fragile:** `rm_node` (`docker.sh:80-86`) is `d rm -f … || true`; if docker refuses to remove a
  netns provider with live sharers (version-dependent), the `|| true` swallows it, the single-pass loop never
  retries brk1, and `d network rm` then also fails (brk1 still attached) → brk1 container + `-etc`/`-lib`
  volumes + the `sim-drill-82…` network leak. Bites on SIGINT/SIGTERM or any `set -u` unbound-var abort mid-drill.
- **Fix:** add after the `SIM=` line:
  `trap 'ingress_down brk1 443 2>/dev/null; ingress_down brk1 8444 2>/dev/null' EXIT INT TERM`.
  Belt-and-braces: make `cmd_nuke`/`cmd_down` reap `sim.role=ingress` nodes FIRST so a netns-owner is never
  removed while a sharer is alive.

### R11 — 81 Arm C expose public-port parse is fragile: greps `:[0-9]{4,5}` off the 2>&1-merged human output + `head -1` → wrong port → C-base-expose false-RED and C-port vacuous-green
*(feasibility MAJOR-4)*
- **Where:** `81-…session-rm.sh:136-137` — `EXP=$("$SIM" ctl -- expose … 2>&1); PORT=$(printf '%s' "$EXP" |
  grep -oE ':[0-9]{4,5}' | tr -d ':' | head -1)`.
- **Why it is fragile:** the human line is `exposed: http://brk1:14000 → agt1:8080` (`cmd/tether/expose.go:111`);
  `head -1` gives the public port today, but the capture is 2>&1-**merged**, so any stderr/log line printed
  before `exposed:` that contains a 4-5-digit port (e.g. a transport connect to `nats://brk1:4222` — `:4222`
  matches `[0-9]{4,5}`) becomes `head -1` → `PORT=4222`. Then `_curl_probe` (81:138) hits the NATS port → the
  SENTINEL never returns → C-base-expose (81:139-140) times out (false-RED), and C-port (81:151-152) then
  passes VACUOUSLY on the wrong URL. Plan §2.1 T3 (`s2-plan.md:143`) used `expose … --json | jq .port`
  precisely to avoid this; the drill regressed to grep.
- **Fix:** `PORT=$("$SIM" ctl -- expose agt1 --local 8080 --name web --json 2>/dev/null | jq -r '.port')`
  (emitJSON path `expose.go:109`), keeping stderr off the parse.

### R12 — run-drills.sh has no grow-family wave, and 82's VOTER-timeout is NOT a retry-eligible flake signature → 82's grow flakes RED under peak concurrency and is never auto-retried
*(feasibility MAJOR-3)*
- **Where:** `run-drills.sh:84-149` fires all `drills/*.sh` concurrently (`JOBS=0` → no cap, :96) with no
  family/wave grouping, contra plan §6/§7 OQ-8 ("80/81 N=1 parallel + 82 grow serial/-j 2"). The retry gate
  `FLAKE_SIG` (`run-drills.sh:57`) does NOT match the grow message `poll_until: timed out after 150s waiting
  for: brk2 reaches VOTER` (`simcluster:215`), so `is_flake` (`run-drills.sh:132`) returns false → a grow
  flake shows RED and is never retried.
- **Why it matters:** `cmd_grow` is documented timing-sensitive at high concurrency (`simcluster:223-232`);
  adding 82 makes it an 8th concurrent grower. A `./run-drills.sh` sweep can report a false RED that a human
  must manually re-run solo — masking nothing about tether but eroding the suite's signal.
- **Fix:** add `|reaches VOTER|did not reach VOTER` to `FLAKE_SIG`, and/or teach run-drills.sh to run
  grow-family drills (`10/11/12/13/82`) in a `-j`-capped second wave (or document the mandatory
  two-invocation split so OQ-8 is enforceable, not aspirational).

---

## MINOR

### R13 — 81 B1/C1/C-sysd evict sig `evicted.*agt1|broadcast` accepts `broadcast=false`
*(anti-mask m5, source F9)*
`81:90,143,163`. Admin evict prints `evicted sid=lab nid=agt1 (node=%v provisioning=%v broadcast=%v)`
(`cmd/tether/admin.go:191`); the regex matches via `evicted.*agt1` regardless of the broadcast value, and the
`broadcast` alternative even matches `broadcast=false`. Not a false-green — B2a (agent self-exit) transitively
proves the broadcast fired (a live agent's JWT is never re-authed; only `agent_evicted` makes the daemon
self-exit, `agent.go:711-728`) — but the sig should pin it. **Fix:** `evicted sid=lab nid=agt1.*broadcast=true`.

### R14 — 82 T3/T4/T5 rely on `TETHER_DEV_NO_AUTH` being unset but never assert it
*(false-green m7, source F10)*
`82:146-154`. T5's `must be https` refusal (`internal/clusterroster/invite.go:192`) is gated on
`TETHER_DEV_NO_AUTH` unset; a leaked env value would let the http bootstrap through → `assert_refuses` RED
(fail-safe, not a false-green), but the plan's explicit env guard is unimplemented. **Fix:** `env -u
TETHER_DEV_NO_AUTH` on T3/T4/T5 (or `test -z "${TETHER_DEV_NO_AUTH:-}"` inside the container) for hermeticity.

### R15 — 82 SETUP-27 has no positive broker-health control before the `000` assert (a crashed broker also yields 000)
*(false-green m4)*
`82:41-44,73-76`. `_manifest_refused` asserts curl to `127.0.0.1:7480` → `000` = manifest listener unbound
(#27, genuine per `broker.go:753`). But a crashed broker also gives `000`. Drills 80/81 place a `cluster
status leader` floor between init and their first assertion; drill 82 goes straight from `init` to SETUP-27.
`init brk1` returning 0 implies the broker answered so the window is tiny, but a one-line positive control
(broker answers on 4222 / `cluster status` leader==brk1) right before SETUP-27 would make the `000` provably
attributable to the manifest port. **Fix:** add the leader-health floor before SETUP-27 (also addresses
**R22**: 82 setup lacks a leader-health gate after the `ingress_enable_manifest` broker restart, 82:78-89 —
add `cluster status --json | jq .leader_id==brk1` after the restart, mirroring 80/81).

### R16 — 82 I-NEG-SAN sig regex `certificate|SSL|verify` is too broad
*(false-green m5)* — folds into **R1**; when the negatives move to the agent fetcher, pin the Go message
`x509: certificate is valid for .*not brk1` specifically.

### R17 — 81 plan §3.2 E3c (`not_found` taxonomy anchor) is missing from the drill
*(source F4)*
`81:186-191` has only E3a/E3b. The plan (`s2-plan.md:456`) kept a taxonomy anchor distinct: session-scoped
call on a gone sid → CONNECT-deny (`Authorization Violation`), rm-API on an absent sid → `not_found`
(`broker/sessions.go:168`). The drill drops E3c so the `not_found`-vs-`session_not_found_or_deleting`
distinctness is never demonstrated at deploy tier. **Fix:** add `assert_refuses "E3c second rm → not_found
(distinct from CONNECT-deny)" "not_found" "$SIM" ctl -- session rm "$SID"`.

### R18 — 81 `_agt1_fp` is dead code (unverified `admin nodes --json .fp/.fingerprint` field baked in)
*(false-green m6, feasibility MINOR-6)*
`81:64` defined, never called (D1 uses `md5sum` of the on-disk `agent.nk` instead, which is a *stronger*
proof). The dead helper also implies an fp-equality oracle that isn't wired and bakes in an unexercised
`admin nodes --json` field name. **Fix:** remove it (or wire it as a second independent D1 oracle).

### R19 — fixed `sleep` instead of `poll_until` (harness flake-discipline drift)
*(feasibility MINOR-7)*
`81:135` (`python3 -m http.server 8080 …; sleep 1`) and `82:163` (`pkill -x tether …; sleep 3`) are the only
fixed sleeps in the batch; CLAUDE.md §5 mandates `poll_until`. Both are currently masked by a later
`poll_until` so low-risk, but latent flakes under load. **Fix:** replace with bounded polls
(`poll_until 5 1 "8080 bound" -- … curl`, `poll_until 5 1 "tether gone" -- ! … pgrep -x tether`).

### R20 — 82 ingress readiness spawns a throwaway container per poll iteration (concurrency-sensitive)
*(feasibility MINOR-8)*
`_ingress_front_up` (`ingress.sh:84-87`) does `d run --rm --network <net> …` on every `poll_until 20 2`
iteration; each ~1-2s spawn contends with the parallel wave → occasional readiness flake at
`SETUP-ingress start … front` (82:82). **Fix:** curl the https front from an already-running node
(`dexec ctl1 -- curl -sk …`) instead of a fresh container per attempt, or raise the budget to `poll_until 30 2`.

### R21 — 81 header guard (c) over-promises a "server-side discriminator (broker log `not provisioned`)" that B3 correctly omits
*(craft m1)*
`81:15-17` vs B3 at `81:95-100`. The B3 inline comment is correct — `isAuthFailure` gates the "NATS
auth_callout rejected" message on `Authorization Violation` only (`internal/agent/agent.go:1538-1544`), and a
non-auth failure goes to silent-retry (no such string), so B3 alone distinguishes auth-deny from network
fault. But the banner still lists a server-side discriminator the impl does not (and need not) do. **Fix:**
reconcile header (c) with the B3 comment (drop the server-side-discriminator clause).

### R22 — 82 setup lacks an explicit leader-health gate after the ingress broker restart
*(craft m2)* — merged into **R15** above.

### R23 — 80 `_i2_ab_sub`/`_i2_ba_sub` dropped `--count 1` (benign latency)
*(craft m3)* — merged into **R4** above (restore `--count 1` alongside the subject anchor + positive control).

### R24 — 82 M1/M2 wait out the 30s manifest re-sign throttle instead of registering it as a doc-gap
*(anti-mask m6)*
`82:96-103` polls 45s past `manifestRecheckInterval` (`cluster_manifest.go:22`) and orders M before J1, so the
realistic "agent joins within 30s of `seeds publish` → gets a seeds-less manifest" window is never exercised.
The 30s lag is only a code constant; `usage.md`/`cluster.md`/`broker-ops.md` carry no user-facing note (same
doc-gap family as #27, which this batch DID register). Leaning benign (AdoptDecision's generation-monotonicity
makes a stale manifest just "no update yet", not a security hole), so severity is low — but silently polling
past it slightly launders a real freshness lag. **Fix:** add a one-line DOC-n entry ("well-known manifest lags
`seeds publish` by up to 30s; undocumented"), OR turn the lag into a labeled probe (curl immediately after
publish → assert seeds absent → poll past 30s → assert seeds present). Do not simply keep the silent note.

### R25 — citation imprecisions in ledger/plan/header (mechanism correct, line attribution off)
*(source F6, F7)*
(a) `deploy-tier-gotchas.md:72-74` / `s2-plan.md:54,600` cite `applyClusterSeam (cmd/tether/cluster.go:799)` —
`:799` is the CALL site; the function is DEFINED at `:880`, and its signature writes NO `manifest_listen`
(the #27 core holds) but also no `nats_conf_path`, so the ledger over-lists what the seam writer sets. Cite
`:880` (definition) or note `:799` as the call site. (b) `81:95-97` header (c) says `agent.go:1538-1553`
"prints" the reject message — the message is PRINTED at `agent.go:859`; `1538-1553` is `isAuthFailure` (the
DETECTOR). Cite `agent.go:859` (print) + `:1538` (detector).

---

## VERIFIED HONEST (interrogated, NOT masking — recorded so this is not a rubber-stamp)

- **#25/#26/#27 headline exposure** — all three genuinely reproduced against real source (see Verdict).
- **Inverted `assert_ok` semantics** (80 Arm R, 81 C-GAP-proc) — correct; success IS the recorded gap; flips
  to RED/`assert_refuses` the day the limiter/reaper lands.
- **#26 composite leak oracle** `_leak_present = _daemon_gone && _child_alive` (81:49-53,149-150) — SOUND: a
  still-running agent fails `_daemon_gone` → RED, ruling out the "child alive because agent alive" false-green;
  the setsid-nohup deploy is `install.sh:371`'s documented #1 manual-start path (real deployment mode); the
  systemd `sleep 888888` counter-probe (81:157-165) honestly attributes cgroup reaping to systemd, not tether.
- **D1 evicted-nkey re-join** (81:107-118) — `md5(agent.nk)` before==after + `[ -n "$NKHASH0" ]` guard proves
  the SAME on-disk nkey re-provisioned (evict deletes only the broker `agent_provisioning` row;
  `EnsureAgentIdentity` loads-if-present, `internal/cli/identity.go:57-58`); a re-mint would differ → RED.
- **E3 sig reframe itself** (81:186-191) — HONEST (see R9; only the labels are stale).
- **S0-tunnel `public_host=<node>`** (`provision-node.sh:35-36` → `install.sh:529` writes `public_host: brk1`,
  a docker-DNS-resolvable name) — FAITHFUL provisioning; no `/etc/hosts` spoof; the expose data plane
  genuinely traverses the real reverse tunnel (81 C-base-expose serves a `SENTINEL81_$$` body cross-container).
- **S0-ingress does NOT weaken the fail-closed boundary** — product manifest listener is loopback-only and
  physically cannot be rebound (`internal/clustermanifest/manifest.go:19-27` `Bind` refuses non-loopback); the
  `ingress-proxy.py` sidecar only relays bytes to `127.0.0.1:7480`; no `TETHER_DEV_NO_AUTH` anywhere; the
  manifest stays broker-built + account-signed + agent-verified (J4 through `FetchManifest`+`AdoptDecision`);
  the enable is gated on the #27 gap asserted first (Mandate 2). (R1 is about the *negative* TLS oracle, not
  this boundary.)
- **Debug fixes are legitimate test-bug fixes**: `--broker` is the real arch-K.1 alias of `--nats-url`
  (`login.go:103-105`, persists `broker_url`); `2>&1` on admin nodes is immaterial (table prints to STDOUT,
  `admin.go:91`) — the real bug was the `^agt1` anchor (admin nodes is SESSION-first vs `node ls` NODE-first),
  correctly attributed to the sim; dropped B3 discriminator is genuinely redundant (agent message is
  self-discriminating). None are tether accommodation.
- **admin-socket EACCES** (81:83-85) — `nobody` on the 0600 socket yields Go's `connect: permission denied`;
  A1 positive control proves the binary+socket work.
- **Invite negatives T1/T3/T4/T5 sigs** — verified verbatim against source: `does not match the invite's pin`
  (`agent_join.go:41`), `unknown param` (`invite.go:83`), `expected scheme … version` (`invite.go:74`),
  `bootstrap url must be https` (`invite.go:192`); T1-noresidue holds (expect-check precedes writeAgentConfig).
- **Cross-drill isolation + teardown (happy path)** — each drill on its own `drill-<name>` network/volumes;
  `cmd_drill` nukes the throwaway instance after each drill; per-drill in-container leaks die with the
  container. (R10 is the abnormal-exit edge only.)
