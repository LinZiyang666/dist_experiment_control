# `test/simcluster/` — Docker Simulated-Cluster Dev Tool — Implementation Plan (FINALIZED)

> **Status:** Stage A finalized (this doc is the plan of record for Stage B).
> **Provenance:** Stage A ran a 10-agent adversarial workflow (6 dimension drafters + 3 adversarial
> critics [fidelity / simplicity / feasibility] + 1 synthesizer, all Opus 4.8). Every load-bearing
> claim below was independently verified by ≥2 critics against source (file:line cited inline). The
> main process (sole finalizer) adopted the synthesized candidate, resolved all 8 open questions
> (§9), and recorded two overrides of the candidate (§9 OQ-3, OQ-5).
> Raw candidate + critiques archived in the session scratchpad.

---

## 0. Goal & scope

Build a **persistent, parameterized, docker-based simulated cluster** under `test/simcluster/` that runs
as a **deploy-tier dev tool** on the dedicated Ubuntu server (`192.168.1.150`). Each container is one node
running **real systemd (PID1) + real `sshd` + real out-of-process `nats-server` + the real `tether` binary**
on a docker bridge with per-node IPs and persistent named volumes. The tool is a thin POSIX-sh orchestrator
that drives the **real** `tether` grow/shrink/failover flows + `docker` + in-container `systemctl`, so it
exercises the cross-process / on-disk / nats.conf-drift / install-path bug class that the hermetic
`make test` and in-process `d*_integration` suites structurally cannot reach (the 21 real-fleet failures in
`docs/v0.4.5-ha-grow-ops-gotchas.md`).

**First increment =** control tool + one base image + walking skeleton (N=1 standalone → clustered,
`node ls` + real `agent join` + `push`/`pull` round-trip) + grow to N=3 (**including a migrated-broker-grow
regression guard that forces a real InstallSnapshot** — the fidelity crux) + three live-breakage drills:
**#20** (force-single survivor nats.conf stays clustered → JetStream 503 rot), **#21** (small-disk broker:
hardcoded 8 GiB `OBJ_xfer` reservation → tier-B denied by JS storage admission), **#12** (force-single
ghost `VOTER` "three-non" deadlock). Everything else is an explicit follow-on (§1).

---

## 1. NON-GOALS

1. **Zero product change.** No new `tether` subcommand, no flag, no edit under `cmd/`/`internal/`. The tool
   orchestrates existing commands + docker + in-container `systemctl` only. (An optional build-tagged
   WSL-side secrets minter is a *fallback* — see §9 OQ-3 — and even then is a standalone dev tool, not the
   product binary and adds no subcommand.)
2. **Not in `make test` / `make e2e`.** No `e2e_matrix`/`dN_integration` build tag; **no Go `_test.go`** under
   `test/simcluster/`. `go test ./...` never sees it. It is a separate, slower, server-side tier.
3. **No ACME, no real domains, no public ingress, no Caddy in increment 1.** Client transport is plain
   `nats://` on the trusted bridge. Broker↔broker **route mTLS stays real and load-bearing**.
4. **No dependency on the box Clash / fake-ip (198.18).** Inter-container 172.x only; provisioning is fully
   offline (`install.sh --skip-download` + pre-staged binaries). Only the one-time `FROM ubuntu:24.04` pull
   may traverse the TUN (pre-pull once, then build offline).
5. **Single host, cross-process — not cross-host.** True multi-host WAN raft stays real-fleet validation.
6. **Not a load/perf/fuzz/chaos tool.** Deterministic RED/GREEN drills for *known* gotchas.
7. **v2 / `main` only** (proto v2 cannot talk to a v1 fleet).
8. **Not a CI gate; not a substitute for the real 3-node staging drill.** Local pre-flight before a release
   touches production.
9. **Linux/amd64 server only.** No macOS-ctl / Windows / arm.
10. **Deferred to follow-on increments:** `upgrade`/version-skew (#13/#19), `drain`/`retire`/`reabsorb`/
    `rebalance` as first-class verbs, colocated broker+agent nodes, cert/account rotation, the C2
    `/.well-known` HTTPS manifest + `seeds publish` + `cluster status --remote` client-convergence (#1),
    Caddy `tls-internal` + P13 subscription HTTPS, the slow-InstallSnapshot >2min catch-up (#7, needs a
    seeded multi-GB DB), and gotchas #1–#19 minus #12/#20/#21.

---

## 2. Container & base-image architecture

**One image, role decided at runtime** (`tether-sim:<ver>`). Mirrors "three roles, one binary." Role = which
systemd units the control script enables. In increment 1, broker nodes, agent nodes, and one `ctl` node are
**separate containers** (no colocated broker+agent — that reproduces #13/#19, which are follow-on).

**Provisioning — REUSE `install.sh`, do NOT hand-bake the tree (verified by all 3 critics).** The "install.sh
is unusable headless" premise is false: `--skip-download` makes `install_nats_server` and `install_caddy`
**early-return** (`scripts/install.sh:611-618`, `640-647`), and the script **starts nothing** — it *prints*
the `systemctl enable --now` sequence (`:582-593`, "generated but NOT enabled or started"), matching how
tether deliberately never runs systemctl itself. The only friction is the forced `--domain`/`--acme-email`
hard-`die` (`:460-461`), satisfied with dummy sim values. Testing a *fork* of `install.sh` (a lean
provisioner / baked tree) would reintroduce exactly the install-path/unit drift this tool exists to catch
(e.g. the `RuntimeDirectory=tether` reboot fix at `:735`).

- **Brokers:** `install.sh --role broker --skip-download --prefix /usr/local/bin --domain
  sim-<node>.tether.test --acme-email dev@sim.tether.test`, then a **tiny overlay**: (a) patch nats.conf
  client bind `host: 0.0.0.0` (cross-container clients reach `:4222`; must be preserved through every
  takeover re-render); (b) append `broker.cluster.{data_dir,raft_addr,secrets_dir,nats_conf_path}` to
  `broker.yaml`; (c) `mkdir -p /etc/tether/secrets && chmod 700`. **Never enable the caddy unit.**
- **Agents & ctl:** `install.sh --role agent --skip-download` verbatim (headless-clean, no Caddy).
- **Anti-drift tripwire (`doctor`):** grep the sim's laid-down units against `install.sh`'s
  `write_systemd_units` output so an install.sh change the sim silently forks is caught.

**Sentinel-guarded, run-ONCE provisioning** via `/etc/tether/.sim-provisioned` on the persistent
`/etc/tether` volume. Load-bearing for #20: a tether-managed `nats.conf` must persist across restarts and
change **only** on membership ops. `up`-recreate-with-same-volume must not re-provision.

**Confirmed systemd run flags** (exact set that passed the server PID1 smoke: `is-system-running`=running,
0 failed units, sshd active — verified 2026-07-04):
```
docker run -d --name sim-$CLUSTER-$NODE --hostname $NODE \
  --network sim-$CLUSTER --ip <fixed> \
  --privileged --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
  --tmpfs /run --tmpfs /run/lock \
  --stop-signal SIGRTMIN+3 \
  -v sim-$CLUSTER-$NODE-etc:/etc/tether \
  -v sim-$CLUSTER-$NODE-lib:/var/lib/tether \
  tether-sim:$VER
```
- `STOPSIGNAL SIGRTMIN+3` (Dockerfile) **and** `--stop-signal` — **mandatory**. Default SIGTERM 10s-kills PID1
  mid-write and corrupts SQLite/BoltDB, masking real bugs as sim artifacts.
- Image is **STATELESS**: no `VOLUME` directive; all mutable state lives only under the two mounts. Binary
  lives in the image layer (new release = new image tag = the canonical upgrade seam).
- Mask units that fight docker PID1/DNS: `systemd-resolved`, `systemd-networkd(.socket)`,
  `systemd-firstboot`, getty. Re-smoke (`doctor`) if this set changes.

**Network:** user-defined bridge `sim-<cluster>` on `172.28.0.0/16` (avoids default `172.17` and Clash
`198.18`). **Static per-node IPs**, deterministic map so `raft_addr=<ip>:7400` / `nats route <ip>:6222` /
`tunnel <ip>:7000` are stable across restart — sidesteps the loopback-advertise gotcha from birth.
**`node_id == hostname == nats server_name == raft ServerID == route cert CN`**, stable forever (D6's
home-bridge keys on `ConnectedServerName()`; cert_fp depends on it — never re-mint on upgrade).
**Numeric-suffix ids** (`brk1/brk2/agt1/ctl1`) so the IP map is arithmetic
(`brk<n>→172.28.1.<10+n>`, `agt<n>→172.28.2.<10+n>`).

**Baked binaries** (`vendor/`, rsync'd from WSL — §7): `tether` (WSL `make build`, CGO_ENABLED=0 amd64),
**`nats-server` pinned to the production `install.sh` version `v2.10.22`** (NOT go.mod's embedded `v2.14.0`
— running what the fleet runs is the tool's justification; single-source from
`scripts/install.sh:NATS_SERVER_VERSION`, verify `nats-server --version` at build), **`nats` CLI** (load-bearing
corroborating probe — `/jsz` + stream checks), `nk` (account/user key minting — §6). **No caddy.** `install.sh`
is `COPY`'d from `../../scripts/install.sh` at build. Also baked: `python3-minimal` + the pty helper, `jq`,
`openssl`, `ca-certificates`, `iproute2`, `curl`.

---

## 3. `simcluster` command surface

Runs on the server; POSIX sh; orchestrates real `tether` + `docker` + in-container `systemctl`.
`--instance <name>` (default `sim`) namespaces the container/volume/network prefix so **destructive drills
run on isolated throwaway instances** and never wedge the persistent cluster. ~13 verbs (the reconcile-engine
/ 18-verb framing from a draft is cut — simplicity critic O1).

| Subcommand | What it does (real commands it drives) |
|---|---|
| `build` | `docker build` the base image from `vendor/` + `install.sh`; assert `nats-server --version` == install.sh pin |
| `up --brokers N --agents M [--ctl 1]` | create net + volumes + containers, boot systemd, provision (once, sentinel-guarded); does **not** cluster |
| `init <brk>` | real `cluster init --from-existing` on a broker (pty-fed typed confirm, §5/#5), then run its printed NEXT-steps verbatim (first `reconcile nats --manual`, §4.1) → standalone→N=1 cluster |
| `grow <brk>` | real fresh-voter join: joiner DB bootstrap → `init --from-existing` → `join prepare`/`join approve` → mesh render → catch-up poll → VOTER (§4.2) |
| `force-single <survivor> --dead <n>[,<n>]` | `docker kill` peer(s), poll their `:7400/:6222/:7000` until refused + quorum-loss dwell, then real `cluster recovery force-single --online` (pty-fed). **Never restarts survivor nats-server** |
| `status [--json]` | resolve leader from `cluster status --json`.`leader_id`, proxy the leader's report + a docker table |
| `nats-conf <node> [--diff]` | inspect `/etc/tether/nats.conf` (cluster{} block present? mtime?) — the #20 probe surface |
| `exec <node> -- <cmd>` | `docker exec [-u tether] <ctr> <cmd>` — the **robust debug primitive** (works even if broker/sshd is dead) |
| `shell <node>` | `docker exec -it <ctr> bash` |
| `ssh <node>` | `ssh sim-<node>` via generated `ssh_config` (real sshd per container) |
| `ctl -- <tether…>` | run `tether` from the `ctl` container (for `push`/`pull`/`node ls`) |
| `session --agent <agt>` | establish (persist) a ctl session (`tether login -s <sid>`) to a live agent; echoes `<sid>` |
| `drill <name>` | run `drills/<name>.sh` on an isolated `--instance`; print RED/GREEN |
| `logs <node> [unit]` | `journalctl -u <unit>` inside the container |
| `down [-v]` | stop/rm the instance's containers, keep volumes; `-v` also removes volumes |
| `nuke[-all] [<node>]` | remove containers **and** volumes (clean slate) |
| `doctor` | re-run PID1 smoke (`is-system-running`==running, 0 failed), verify no default-route egress, units match install.sh, store-cap primitive, no `max_file_store` under any reconcile conf |

**State model:** the script owns **no** raft mirror. Two external SSOTs: **docker labels**
(`sim.cluster`, `sim.role`, `sim.nodeid`) = inventory/desired topology; **live `cluster status --json`**
(leader's admin socket) = raft/membership truth. Only persistent local artifact = a tiny per-instance
`cluster.env` (subnet, image tag, ids). No reconcile-engine.

**Admin-socket + user discipline (verified — critique-3 G3):** every online verb targets the leader's admin
socket. install.sh's broker.yaml sets `admin.socket` under `RuntimeDirectory=tether`; the `tctl` helper must
wire `--socket`/`--config` **explicitly** (Phase-0 verifies how the CLI locates `/run/tether/admin.sock`
before trusting a default). All mutating `tether cluster` ops run `docker exec -u tether` (matches
`User=tether`; dodges the root-owned `tether.lock` gotcha #6). A coarse `flock
/run/lock/simcluster-<instance>.lock` guards mutating subcommands.

---

## 4. Grow / shrink / failover orchestration (real `tether` command sequences)

Grounded in `cmd/tether/cluster*.go`, `internal/clusteroffline`, `internal/natsconf`,
`docs/cluster-runbook.md`, and the gotchas doc. **Three DISTINCT grow sequences** (the runbook blurs them):

### 4.1 First cutover: single standalone broker → N=1 cluster
```
tctl brk1 -u tether cluster keygen --out /etc/tether/secrets/node-ident.nk   # writes SEED to file, prints PUBKEY to stdout
tctl brk1 -u tether cluster doctor --secrets-dir /etc/tether/secrets          # preflight (HARD-FATAL on bad secrets)
sctl brk1 stop tether-broker                                                   # offline disk surgery
# typed confirm has NO machine escape (allowMachineEscape=false, cluster.go:762) → pty feeder (§5/#5):
exec brk1 -u tether python3 /opt/sim/pty-confirm.py brk1 -- \
     tether cluster init --from-existing --self-id brk1 --name simcl \
       --raft-addr 172.28.1.11:7400 --nats-route nats://172.28.1.11:6222 \
       --tunnel-addr 172.28.1.11:7000 --public-host 172.28.1.11 \
       --secrets-dir /etc/tether/secrets --node-ident-pub <pub>
# FIRST takeover MUST be --manual (standalone conf has no cluster{} block for --all to harvest, gotcha #3)
# AND MUST carry --account-issuer + --broker-nkey (cluster_natsconf.go:264 refuses without them — critique-3 G4).
# MOST ROBUST: capture init's printed NEXT-steps (cluster.go:788-808, pubkeys already substituted) and run them verbatim:
tctl brk1 -u tether cluster reconcile nats --manual --secrets-dir /etc/tether/secrets \
     --server-name brk1 --route-url nats://172.28.1.11:6222 \
     --account-issuer <account_pub> --broker-nkey <broker_pub> --conf /etc/tether/nats.conf
docker exec sim-.-brk1 nats-server -t -c /etc/tether/nats.conf   # validate-before-restart gate (mandatory after ANY conf edit)
sctl brk1 restart nats-server
sctl brk1 start tether-broker                                    # boots in cluster mode (DetectClusterMode via raft/)
wait_healthy 1     # N=1 floor: exit-1 DEGRADED is HEALTHY (no redundancy expected), JS live standalone
```
**Bonus #3 characterization:** assert `reconcile nats --all` *fails* on the not-yet-clustered conf, then `--manual`.

### 4.2 Fresh-voter grow: N → N+1
The fresh-joiner raft bootstrap is **acceptance-blocking and unproven** (critique-3 C3/G1): a fresh container
has no `raft/`; `DetectClusterMode` FATALs on `data_dir` set + `raft/` absent (`internal/broker/cutover.go:49`,
"run cluster init first; no auto-bootstrap"), and `init --from-existing` migrates an *existing* DB — there is
no fresh-bootstrap path. The walking skeleton **empirically pins** the exact ordering before any drill depends
on it. Candidate sequence:
```
# joiner: boot standalone once to CREATE/migrate its DB, then init-from-existing, then join.
sctl brk2 start tether-broker            # standalone, creates tether.db
sctl brk2 stop tether-broker
exec brk2 -u tether python3 /opt/sim/pty-confirm.py brk2 -- tether cluster init --from-existing --self-id brk2 …
sctl brk2 start tether-broker            # up as its own {self} pre-join

L=$(leader_node)
bundle=$(tctl brk2 -u tether cluster join prepare --node-id brk2 --raft-addr … --nats-route … --secrets-dir …)
op=$(tctl $L -u tether cluster join approve "$bundle")     # capture op; try WITHOUT --wait first (gotcha #8)
# gotcha #8 (catch-up needs joiner NATS meshed): render the FULL mesh, then restart joiner nats-server:
tctl $L -u tether cluster reconcile nats --all --wait --timeout 90s    # leader is clustered → --all harvests cluster{}
docker exec … nats-server -t -c /etc/tether/nats.conf
sctl brk2 restart nats-server
# former-N1 JS reset (gotcha #4): the ex-standalone JS store does NOT migrate into the clustered meta.
sctl $L stop nats-server
exec $L sh -c 'mv /var/lib/tether/jetstream /var/lib/tether/jetstream.grow-bak.$(date +%s)'   # mv not rm (forensic)
sctl $L start nats-server
wait_phase brk2 VOTER 300                 # poll status --json .nodes[].phase, NOT `ops show <opid>` (critique-3 G5)
wait_healthy 2                            # incl. STREAMS actual==target re-formed
```
**Ordering deviation policy (resolves the grow-order conflict):** try the **documented** runbook order first;
fall back to the async-approve/mesh-mid-catch-up interleave **only if it observably stalls**. Poll the
**joiner's phase** via `status --json` (verified fields), not `ops show <op-id>` (arg type + enum are
product-ambiguous: `ops show <node>` at `cluster_ops.go:125` vs `<opID>` in the join hint — critique-3 G5);
reserve `ops confirm <op-id>` (`cluster_ops.go:33`) for the BLOCKED path only. Replace every fixed `sleep`
with poll-until-condition (CLAUDE.md §7). `opCatchupTimeout`=2min confirmed
(`cluster_operation_controller.go:281`); sim DBs are tiny so the >2-min InstallSnapshot-#7 path is a
**follow-on** — do not build the STALLED→confirm retry loop now.

**EMPIRICALLY-PINNED grow sequence (Phase 2, proven N=1→2→3 on the real server).** The working `simcluster
grow <joiner>` ordering, with the gotchas it had to confront:
1. joiner: mint+distribute secrets (shared CA+account) → standalone boot once (create tether.db) → stop →
   `cluster init --from-existing` (create raft/) + broker.yaml cluster seam.
2. joiner: `join prepare` (offline) → bundle (`tether-join:v1:<b64>` — grep that prefix; it has colons).
3. leader: `join approve <bundle>` **WITHOUT `--wait`** — the joiner cannot catch up until the clustered
   JS meta forms (step 5), so `approve --wait` blocks **forever** (this was a 20-min hang). approve just
   stages the raft config change; catch-up follows once the joiner broker starts.
4. render the **FULL route mesh**: reconcile EVERY broker (existing voters + joiner) CLUSTERED with a
   `--peer` triple for every OTHER broker. A joiner brought up clustered while any peer is still standalone
   hits the **N=1-clustered-JS trap (#10)** and crash-loops (JS meta can't reach quorum). Route certs MUST
   carry SANs (§9 OQ-3).
5. bring nats into the mesh: **RESET the former-N1 JS store on the FIRST grow only** (`mv jetstream …`, #4 —
   standalone JS does not migrate); **RELOAD (SIGHUP) already-running voters** to pick up the new route —
   do **NOT** `systemctl restart` a running voter's nats-server: its tether-broker loses the connection,
   exits **cleanly**, and `Restart=on-failure` does not bring a clean exit back → the broker dies
   permanently (this silently knocked an existing VOTER back to CATCHING_UP on the 2nd grow). Restart only
   the joiner's nats (fresh) + the former-N1 (after its JS reset).
6. start the joiner's tether-broker (JS now available) → it catches up → `wait_phase <joiner> VOTER`.
Guard every command-substitution against `set -e`/`pipefail` (an unguarded `grep -c` / transient-empty
status read silently kills the script). These findings are exactly the cross-process bug class the tool
exists to surface (they are unreachable by the in-process d-suite).

### 4.3 Shrink / de-cluster / force-single (increment-1 uses only force-single; retire/drain deferred)
- **Force-single (online, the #20/#12 setup):** `docker kill` the peer container (closes all 3 ports at once,
  satisfying `--confirm-peers-dead`'s TCP-dead HARD-REFUSE), **poll `:7400/:6222/:7000` until
  connection-refused** (never a fixed sleep) plus the ~15s continuous-quorum-lost dwell, then pty-fed
  `cluster recovery force-single --online --self-id <s> --confirm-peers-dead <dead>`. **Do NOT restart the
  survivor's nats-server** — the whole point of #20 is that force-single leaves nats.conf untouched and the
  already-running nats-server keeps the stale 2-node `cluster{}` config. Observe the survivor via `docker
  exec` (robust primitive), never through a tunnel riding the NATS being disrupted.

**"Healthy N" predicate** (raft-green alone is blind to #20/#21). Evaluate against the **leader's** view
(`reachable`/`applied_lag`/`stream_actual` are leader-only-populated — verified `adminsock/protocol.go:415`):
1. every expected node `phase==VOTER`, `reachable`, `applied_lag==0` (N=1: exit-1 DEGRADED is the healthy floor);
2. **JS liveness** — a `tether push` of a small tier-A + tier-B file round-trips (the only probe that catches
   #20/#21); `curl localhost:8222/jsz` corroborates meta health;
3. topology converged (`topo_observed>=topo_desired`, empty `topo_reconcile_reason`);
4. `stream_actual==stream_target` (== `ReplicasFor(voters)`, capped 3).

Verified snake_case JSON tags (`internal/adminsock/protocol.go`, critique-3 G7): `leader_id`(:377),
`is_leader_view`(:385), `phase`/`role`(:484-485), `applied_lag`(:486), `stream_actual/target`(:491-492),
`reachable`/`reach_source`(:493-494), `topo_desired`(:394), `topo_observed`/`topo_reconcile_reason`(:521-522),
`health`∈{HEALTHY_HA|DEGRADED|QUORUM_LOST|FORCE_SINGLE}(:375), `force_single`(:253).

---

## 5. The drills (walking skeleton + grow-to-N=3 + #20/#21/#12)

**RED/GREEN harness — minimal, not a framework** (simplicity critic O2). #20 and #12 have **no green code path
today** (open backlog). Keep the ONE load-bearing idea and cut the XFAIL/XPASS/FAIL registry + promotion
machinery: a **signature-guarded** helper.
```sh
# assert_ok    "<desc>" <cmd...>                  → must exit 0 (GREEN invariant)
# assert_refuses "<desc>" "<sig-regex>" <cmd>     → must FAIL with stderr matching sig (a refusal we want KEPT)
# assert_bug   "<desc>" "<gotcha>" "<sig>" <cmd>  → runs the cmd that SHOULD succeed once fixed:
#     exit 0             → loud "APPEARS FIXED — promote to assert_ok", drill FAILS (so we notice)
#     fail & stderr=~sig → bug reproduced for the DOCUMENTED reason (expected; drill stays green)
#     fail & stderr!~sig → HARD FAIL (broke for an undocumented reason — e.g. the alert gate, not JS)
```
The `sig` guard is the defense against false-green (an alert-gate error must NOT be scored as "JS 503
reproduced"). Signatures match the **cause token** (`bucket_create_failed`, `503|10008`,
`insufficient storage|10047`, `unrecognized raft role`), broad enough to survive rewording, narrow enough to
reject the wrong cause. **Initial signatures MUST be captured from a real run**, not written from the docs
(the #12 refusal messages are currently guesses — critique-3 §4.4). Each destructive drill runs on its own
`--instance` and tears down on EXIT.

### 5.0 Walking skeleton (`00-skeleton.sh`, GREEN-only, the acceptance gate)
`up --brokers 1 --agents 1 --ctl 1` → `init brk1` (pty). Asserts: `status --json` → exactly 1 node
`phase==VOTER`, health∈{HEALTHY_HA,DEGRADED}; **a real `agent join <invite>` succeeds and `node ls` lists the
agent**; `session` establishes a ctl session; **`push` (tier-A + tier-B) + `pull` round-trip** (proves the
auth_callout + JS path end-to-end — critique-3 G2). **This gate blocks increment 1** — it empirically proves
three unproven primitives: cross-process routes-mTLS forms, `agent join`+transfer round-trips, and the
store-cap mechanism caps nats-server's free-disk.

### 5.1 Grow to N=3 (`10-grow-to-3.sh`, GREEN)
`grow brk2`, `grow brk3` (§4.2). Asserts: 3 nodes `VOTER`, health HEALTHY_HA, all reachable, lag 0; poll
`stream_actual==stream_target==ReplicasFor(3)=3`; then a **positive HA proof** — kill a *follower's*
nats-server, confirm a tier-B push still succeeds at quorum 2/3, restore.

**Migrated-broker-grow regression guard (the fidelity crux — F1 BLOCKER, kept in increment 1 per §9 OQ-6).**
The plain grow above joins via raft **log replay** — the same path the in-process `d9` suite already covers
(`test/d9/grow_migrated_leader_e2e_test.go:25-48` documents this exact blind spot; a fresh-init leader sits at
snapshot@1 so joiner+leader indices match → log replay, never InstallSnapshot). The bug that **BLOCKED the
real-machine test** (memory `project_cluster_ha_realmachine_test`: fresh joiner replaying a *migrated* leader's
log → FK-panic; grow-onto-migrated-broker gap; product fix `32b28e9` "preserve joiner identity across
InstallSnapshot") only appears with a real cross-process raft transport **and** a migrated (snapshot-first /
high-FirstIndex) leader. So one grow arm drives the N=1 leader's raft **FirstIndex above the fresh joiner's
nextIndex** (via `cluster recovery resnapshot` on the N=1 — the pc732 shape) so the join ships a real
**InstallSnapshot**, and asserts: the InstallSnapshot fired (leader log / joiner snapshot index > 1), **no
FK-panic** in the joiner's `tether-broker` journal, and **no hollow voter** (joiner `cluster_nodes` row count
== leader's, all identity rows present). **This is the single assertion the in-process suites cannot reach —
without it the grow drill is a false green** (fidelity critic G1/F1).

### 5.2 #20 — force-single survivor nats.conf stays clustered → JetStream 503 rot (`20-forcesingle-natsconf.sh`, RED)
Setup (shared `lib/setup-forcesingle.sh`): real N=2 clustered-JS + 1 agent + ctl session; **assert the JS meta
actually FORMED** (`stream_actual==target==2`) *before* killing the peer (else the 503 is a formation failure,
not quorum-loss rot — a false confirm; critique-3 G3). Baseline: tier-B push works on healthy N=2. Then
`force-single brk1 --dead brk2`.
- **GREEN positive controls** (cluster alive, only JS rots): `node ls`, `exec … echo ok`, **tier-A push works**.
- **`--ack-alerts` is mandatory** on every force-single-context transfer (verified: `gateDestructive`,
  `cmd/tether/d8_alerts.go:71`, blocks `force_single_active` *before* JetStream at `transfer.go:158/419`;
  without the flag the push fails at the alert gate and falsely "confirms" #20 — F3 BLOCKER).
- **RED assertions:** `nats.conf` **still contains a `cluster{}` block** and its **mtime is unchanged** across
  force-single (proves the lingering-conf mechanism, not an assumption; critique-3 G7);
  `assert_bug "tier-B push after force-single" "#20" "bucket_create_failed.*(503|10008)" -- push <20MiB> --ack-alerts`;
  same for a tiny pull (broker pre-creates the tier-B bucket regardless of size → push/pull asymmetry is a
  second fingerprint). Corroborate with `curl :8222/jsz`. **Do not** use unauthenticated `nats stream ls` as
  the primary probe (`:4222` is auth_callout-gated → auth violation, not 503).
- **GREEN recovery arm:** drive the documented manual workaround (strip `cluster{}` via awk → `nats-server -t`
  → `.bak` → `mv jetstream` → restart) and assert JS heals. Carries a
  `# FLIP TO PURE-REGRESSION WHEN force-single de-clusters survivor nats.conf`.

### 5.3 #21 — small-disk broker: 8 GiB OBJ_xfer reservation denies tier-B (`21-smalldisk-tierb.sh`, RED)
Model the small disk **deterministically** — the 846-GiB-free server never triggers it naturally. **Cap the
store via `--tmpfs /var/lib/tether/jetstream:size=1g`** on that broker's container (primary — §9 OQ-5;
nats-server statfs's the cap as free-disk; consumes no host-global loop devices — the shared-88-core-box
resource risk; loopback ext4 is the documented fallback). **NEVER `max_file_store` in nats.conf** (verified:
it is an unrecognized `jetstream{}` subkey → `internal/natsconf/preflight.go:65` + `preflight_test.go:71`
`Preflight` **REFUSES** it → bricks `cluster reconcile nats` — F2 BLOCKER; also not the real conf's mechanism,
gotchas #21). Size < the hardcoded 8 GiB `OBJ_xfer` `MaxBytes` (`internal/broker/transfer.go:201`) so the
**first** tier-B bucket alone deterministically hits `10047` (no near-ceiling flake). Run the broker
**clustered/force-single N=1** (the racknerd shape where #21 actually bit) so the store cap is orthogonal to
the tether-managed conf. Asserts: `assert_ok "tier-A push (no reservation)"`;
`assert_bug "tier-B push" "#21" "bucket_create_failed.*(insufficient storage|10047)" -- push <20MiB> --ack-alerts`.
`doctor` guards that no drill running `reconcile nats` has a `max_file_store` in its conf.

### 5.4 #12 — force-single ghost VOTER "three-non" deadlock (`12-ghost-voter.sh`, RED)
Shares `setup-forcesingle.sh` (after force-single, raft={brk1} but the roster still lists brk2 `phase==VOTER`).
GREEN control: assert the ghost row is visible (offline roster / status shows brk2 `phase==VOTER`,
INCONSISTENT). RED: all three online removal paths refuse today — `assert_bug` on `cluster recovery node
remove brk2 --manual` (env-escapable, `allowMachineEscape=true`, `cluster.go:531`), `cluster retire brk2`,
`cluster reconcile nats --to-standalone --confirm-single --server-name brk1`. **Capture the exact refusal
signatures from a first real run** (which gate fires first — the "need EXACTLY 1 voter" count at
`cluster_natsconf.go:170` vs the "unrecognized raft role" check at `:166` — is unproven; critique-3 §4.4).
Carries the FLIP-TO-REGRESSION marker.

---

## 6. TLS / secrets / identity + client transport

**Client transport = plain `nats://` on the trusted bridge; no Caddy in increment 1** (all 6 drafts + all 3
critics converge). auth_callout/nkey CONNECT is transport-independent (`internal/cli/natsconn.go`); the broker
never terminates TLS (Caddy is a pure external WSS sidecar). Simulating Caddy would mint a second CA and test
Caddy, not tether HA. **Broker↔broker route mTLS stays 100% real and load-bearing** (exercises gotcha #3's
harvest path and the raft `NetworkTransport`).

**Standalone auth posture (critique-3 G2):** install.sh's base nats.conf has **no `auth_callout` block**
(`:700-717`) — that block is written by `reconcile nats`/takeover, not by install.sh or single-mode serve.
So a *standalone* broker's clients connect **anonymously**; real auth_callout auth exists only **after**
`init`+`reconcile`. Consequence: the #21 broker runs **clustered/force-single N=1** (not pure standalone) so
its auth fidelity is real; the walking skeleton round-trips a real `agent join`+`push`/`pull` *after* the
cutover to prove the authenticated path.

**Secrets contract (per-broker `secrets-dir`, HARD-FATAL on violation — `internal/clusteroffline/preflight.go`):**
`cluster-ca.pem` (shared trust anchor, public), `route-cert/key.pem` (per-node CA-signed leaf; key `0600`),
`tunnel-cert/key.pem` (per-node; key `0600`), `node-ident.nk` (per-node USER seed, `0600`), `broker.nk`
(per-node bus USER seed, `0600`), `account.nk` (**shared ACCOUNT seed**, `0600`). `account.nk` MUST be an
ACCOUNT key (`IsValidPublicAccountKey` for the auth_callout issuer, `internal/auth/jwt.go:32`) — `cluster
keygen` mints only USER keys, so the account key needs `nk -gen account`; a "simplification" to `cluster
keygen` makes every client CONNECT fail cryptically. One CA + one account for the whole sim.

**Minting mechanism — see §9 OQ-3 (finalizer override).** Mint on WSL, `docker cp` per-node trees to the
volumes, then **mandatory** `chown -R tether:tether` + `chmod 0600` on every private key (`docker cp` lands
root:0644; `serve`/`SecretsPreflight` HARD-FATAL on `&0o077 != 0` — this is also the gotcha-#6 surface).
`server_name == node_id`, stable across restart/upgrade (D6 home-bridge + cert_fp depend on it — never re-mint
on upgrade).

**Client bootstrap:** agents via inline-seed invite `tether agent join
'tether-invite:v1?…&seed=nats://brk1:4222&sid=<sid>&pin=<PIN>'` (bypasses the HTTPS manifest); ctl via
`tether login --broker nats://brk1:4222 -s <sid>`. **Round-trip one real invite through `agent join` in the
walking skeleton to lock the exact query grammar** (currently assembled by hand, unverified — Phase-0 OQ-7).

---

## 7. Dev-loop (WSL build → rsync → server run) + persistence

**The binary is built only on WSL** (server has no Go): `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build
VERSION=<v>` (static, matches server amd64). A thin WSL driver `remote.sh`:
```
remote.sh <subcmd> …
  1. (cd repo && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build VERSION=$V)
  2. rsync -a test/simcluster/  weiland@192.168.1.150:~/dist_experiment_control/test/simcluster/   # WHOLE tree
     rsync -a bin/tether                     …/test/simcluster/vendor/tether
  3. ssh weiland@192.168.1.150 "cd …/test/simcluster && ./simcluster <subcmd> …"
```
**rsync the whole `test/simcluster/` tree during iteration** (it is tiny) — do NOT `git pull` the tooling on
the server (this repo commits only at phase end and forbids mid-review staging, so `git pull` gets stale
tooling every loop — critique-3 G6). Git becomes the source of record once the increment lands. `remote.sh`
is **not** a Makefile target (keeps `sim-*` off the release-gate build file). Pin `GOOS/GOARCH` explicitly.
`weiland` is in the `docker` group (added Phase-0) so steady-state is sudo-free; `DOCKER_SUDO=1` fallback
shim. `vendor/` is gitignored.

**Upgrade** (deferred to follow-on, seam only): binary is an image layer; the real fleet path is `docker cp`
new tether + `systemctl restart` the two units in place (volumes untouched) — designed, not built in
increment 1.

**Persistence = per-node named volumes** (`sim-<instance>-<node>-{etc,lib}`) decoupled from container
lifetime. `down` keeps volumes (living cluster survives dev sessions, exercises restart / #20's
nats.conf-across-restart / #4's orphaned JS store). `nuke[-all]` removes volumes. `up`-recreate-with-same-
volumes must not re-provision (sentinel). **Living cluster = default `--instance sim`; every destructive drill
uses its own throwaway `--instance`.**

---

## 8. File layout under `test/simcluster/`

```
test/simcluster/
├── README.md                 # server ops (192.168.1.150), deps, quickstart, boundary vs d*_integration, NON-GOALS
├── remote.sh                 # WSL driver: make build → rsync tree+binary → ssh server simcluster …
├── simcluster                # POSIX-sh dispatcher (runs on the server)
├── Dockerfile                # ubuntu:24.04 + systemd + sshd + nats-server(v2.10.22) + tether + nats + nk + python3
├── .gitignore                # vendor/  secrets/  *.local
├── lib/
│   ├── log.sh                # log/err/die/run
│   ├── docker.sh             # net/vol/run/label helpers, arithmetic IP map, ctr-name
│   ├── tether.sh             # tctl (-u tether, --socket), sctl, leader_node, wait_phase, wait_healthy, peer_triple
│   ├── grow.sh               # first_cutover / fresh_voter / reset_former_n1_js  (§4)
│   ├── secrets.sh            # mint (§9 OQ-3) + distribute per-node tree → volume, chown+0600
│   └── assert.sh             # assert_ok / assert_refuses / assert_bug (signature-guarded)  (§5)
├── image/
│   ├── provision-node.sh     # install.sh --skip-download + thin overlay, sentinel-guarded (§2)
│   ├── pty-confirm.py         # fail-loud pty feeder for typed-confirm ops (#5); match VERIFIED prompt substring
│   └── units/
│       └── tether-agent.service   # sim-owned agent unit (User=sim/tether; install.sh writes no system agent unit)
├── drills/
│   ├── 00-skeleton.sh        # walking skeleton (GREEN) — the acceptance gate
│   ├── 10-grow-to-3.sh       # grow N=1→3 + migrated-broker-grow InstallSnapshot regression guard (GREEN)
│   ├── lib/setup-forcesingle.sh  # shared: real N=2 clustered-JS → assert meta formed → kill peer → force-single
│   ├── 20-forcesingle-natsconf.sh # #20 RED
│   ├── 21-smalldisk-tierb.sh      # #21 RED (tmpfs-size store cap)
│   └── 12-ghost-voter.sh          # #12 RED
├── gen-secrets/              # (FALLBACK ONLY, §9 OQ-3) build-tagged WSL Go minter; //go:build simcluster_tools
│   └── main.go               #   present only if Phase-0 shows openssl/nk can't mint accepted ed25519 route certs
└── vendor/                   # GITIGNORED: tether, nats-server, nats, nk, install.sh — rsync'd from WSL
```
No Go `_test.go` here; `gen-secrets/main.go` (if it exists) is build-tagged `//go:build simcluster_tools` so
`go test ./...` / `go build ./...` in the default build context never compile it.

---

## 9. Finalizer decisions (open-question resolutions)

The 8 candidate open questions are RESOLVED here by the sole finalizer. Two are **overrides** of the candidate.

- **OQ-1 (ctl+session lifecycle) — DECIDED: dedicated `ctl` container per instance.** `session --agent <agt>`
  runs `tether login -s <sid>` **once**, persisting session credentials to the ctl container's config volume;
  each `ctl -- push/pull/node ls` is a fresh `tether` invocation via `docker exec` that reconnects with those
  stored creds. Creds are bound to the session/account, not a specific broker, so they survive the
  force-single window (reconnect to the survivor). No long-lived ctl daemon needed.
- **OQ-2 (grow ordering) — DECIDED: empirical.** Try the documented runbook order first; add the gotcha-#8
  async-approve/mesh-mid-catch-up interleave only if it observably stalls (§4.2). Do not pre-encode the
  contested interleave.
- **OQ-3 (secrets minter) — RESOLVED (implementation-confirmed): sh `openssl`+`nk` WITH SANs; NO Go minter.**
  Verified: the product has **no** full-secrets-tree generator (`cluster keygen` mints USER keys only;
  `cluster_secrets.go` only *reads*; `account.nk` needs an ACCOUNT key). `lib/secrets.sh` mints on the
  server host with `openssl` (ed25519 CA + CN=`<node_id>` route/tunnel leaves) + `nk` (`nk -gen account`
  for account.nk; `nk -gen user` for node-ident/broker); distribution is `docker cp` + `chown tether:tether`
  + `chmod 0600`. **CRITICAL correction (materialized during Phase 2 grow, exactly the RK1/W7 risk):
  route/tunnel leaves MUST carry `subjectAltName=DNS:<node_id>` (+ localhost/127.0.0.1).** The earlier
  "CN-only is fine (per test/d9)" reasoning was WRONG: test/d9's CN-only certs exercise the **raft**
  `NetworkTransport` (tether's own custom `VerifyConnection`), whereas **nats-server's cluster-route mTLS**
  (`verify: true` in nats.conf) does **standard Go x509 verification**, which since Go 1.15 REQUIRES a SAN —
  a CN-only cert fails `x509: certificate relies on legacy Common Name field, use SANs instead`, the route
  handshake closes, routes never form, and the clustered JS meta never forms (the joiner then hangs at
  CATCHING_UP). The Go-minter fallback is **not needed** — sh + SANs works. (This is a load-bearing example
  of the tool catching a real cross-process bug the in-process d9 suite structurally cannot.)
- **OQ-4 (provisioning) — DECIDED: reuse `install.sh --skip-download`, confirmed in Phase-0.** Mandate
  `install.sh --role broker --skip-download --domain sim.test --acme-email dev@sim.test` (+ overlay) and
  `--role agent` verbatim. Phase-0 `--dry-run` confirms it early-returns caddy/nats and leaves the caddy unit
  **disabled**; if it force-enables caddy, fall back to a lean provisioner byte-aligned to
  `write_systemd_units` minus caddy + the `doctor` drift tripwire. Ban all hand-baked `/etc/tether` trees.
- **OQ-5 (#21 store-cap) — OVERRIDE toward feasibility critic: `--tmpfs …/jetstream:size=1g` PRIMARY**,
  loopback ext4 the documented fallback (the candidate's pick, reinforced: no host-global loop devices on the
  shared 88-core box). Phase-0 smoke MUST confirm the cap actually constrains nats-server's free-disk
  computation before #21 depends on it.
- **OQ-6 (InstallSnapshot regression guard) — DECIDED: KEEP in increment 1.** It is the tool's raison d'être
  (the exact bug that BLOCKED the real-machine grow; the cross-process guard for fix `32b28e9`). Sequencing:
  plain grow + FK-panic/hollow-voter assertion lands first (Phase 2); the resnapshot-forced-InstallSnapshot
  arm is the last Phase-2 item. **If** resnapshot-forcing proves fiddly within a bounded spike, it drops to
  the *immediate* follow-on (documented, not silent) while plain-grow + the FK-panic/hollow-voter assertion
  stay.
- **OQ-7 (verify-before-build strings) — DECIDED: mandatory Phase-0.** Verify against source before drill
  code: the exact `confirmTypedNodeID` prompt substring (`cmd/tether/cluster_offline.go`), the real `agent
  join` invite grammar (round-trip one invite), and any `adminsock.ClusterStatusReport` JSON tag beyond the
  already-verified set (§4 list).
- **OQ-8 (run-as-tether default) — DECIDED: yes.** Daemon + nats as `User=tether` (install.sh default); all
  CLI ops `docker exec -u tether`; pre-chown the lock. Gotcha #6 is an **opt-in `REPRO_LOCK=1`** drill, never
  the accidental happy-path wedge.

---

## 10. Phased build order (de-risk first)

**Phase 0 — Prove the unproven primitives standalone (before any drill depends on them).**
(a) `install.sh --role broker --skip-download --dry-run` is headless + leaves caddy disabled (OQ-4);
(b) `--tmpfs /var/lib/tether/jetstream:size=1g` actually caps nats-server's reported free-disk under the
confirmed run-flags (OQ-5); (c) read + pin the exact `confirmTypedNodeID` prompt substring and the `agent
join` invite grammar (OQ-7); (d) `nats-server v2.10.22` matches the install.sh pin (**done** — staged
2026-07-04); (e) secrets mint spike — openssl+nk ed25519 route cert accepted by nats-server route mTLS?
(OQ-3 decider). PID1-in-docker + sshd already smoked (**done**).

**Phase 1 — Base image + secrets + walking skeleton (the acceptance gate).** Dockerfile (systemd + sshd +
nats-server + tether + nats + nk + python3, `STOPSIGNAL SIGRTMIN+3`); `remote.sh` build→rsync→ssh; secrets
mint + `secrets.sh` distribution (chown+0600); `simcluster build/up/init/exec/status/shell/ssh/down`;
`pty-confirm.py`; `provision-node.sh` (install.sh + overlay, sentinel). **Ship `00-skeleton.sh` green:** N=1
standalone→clustered, real `agent join` + `node ls` + `push`/`pull` round-trip. **Nothing proceeds until this
is green.**

**Phase 2 — Grow.** Empirically pin the fresh-joiner bootstrap ordering (§4.2, acceptance-blocking); `grow`
verb; `10-grow-to-3.sh` green (3-VOTER + stream replicas + follower-kill HA proof); then the
migrated-broker-grow regression guard (resnapshot → InstallSnapshot → no FK-panic / no hollow voter — OQ-6).

**Phase 3 — The three drills (isolated `--instance`).** `assert.sh` (signature-guarded); `setup-forcesingle.sh`
(assert JS meta formed before kill); `force-single` verb (poll-until-ports-refused, no survivor nats-server
restart); `20-forcesingle-natsconf.sh` (#20, `--ack-alerts` + cause-token sigs + nats.conf-mtime-unchanged),
`21-smalldisk-tierb.sh` (#21, tmpfs-size cap, clustered-N=1), `12-ghost-voter.sh` (#12, capture refusal sigs
from a real run). Each carries a FLIP-TO-REGRESSION marker.

**Phase 4 — Finish + guardrails.** `README.md` (ops, boundary-vs-`dN` table, NON-GOALS); `doctor` (PID1
re-smoke + no-egress + units-match-install.sh drift tripwire + no-`max_file_store`-under-reconcile guard);
`ssh_config` generation; `flock` concurrency guard. Hand to Stage C adversarial review, then external review.

---

## 11. False-green acceptance criteria (each drill states the wrong reason it could pass)

Per fidelity critic F9 — a drill without its false-green guard documented is not accepted:
- **grow-to-3:** could pass via **log-replay instead of InstallSnapshot** → guard = assert InstallSnapshot
  fired (joiner snapshot idx > 1) + no FK-panic + no hollow voter (§5.1).
- **#20:** could pass via the **alert-gate refusal** mistaken for JS 503 → guard = `--ack-alerts` +
  `bucket_create_failed.*(503|10008)` signature; and could pass via **JS never forming at N=2** → guard =
  assert `stream_actual==target==2` before the kill (§5.2).
- **#21:** could pass via **`max_file_store` bricking the takeover** (wrong mechanism) or **near-ceiling
  flake** → guard = tmpfs-size cap (never `max_file_store`) sized < 8 GiB so the first bucket alone fails
  (§5.3); `doctor` asserts no `max_file_store` under any reconcile conf.
- **#12:** could pass via a **refusal for an undocumented reason** → guard = capture the real refusal
  signature from a first run; `assert_bug` HARD-FAILs on a non-matching cause (§5.4).
