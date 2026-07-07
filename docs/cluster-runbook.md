# tether cluster — operator runbook (D7)

> Operator-facing procedures for the distributed-broker cluster lifecycle. The
> binding contract is `docs/distributed-broker-architecture.md` §8 / §17; this file
> is the copy-pasteable drill. **All cluster admin commands run on a broker host**
> (admin is strictly local — no network bypass). A non-leader broker fails fast and
> names the leader host to re-run on.

## C8 command migration (read if you used an older runbook)

The cluster CLI was consolidated in C8. Old spellings → new spellings:

| old (≤ C7) | new (C8) |
|---|---|
| `tether cluster add <id> <host:7400> <pub> [--join-token …]` | joiner: `tether cluster join prepare --node-id <id> --raft-addr <host:7400> --nats-route nats://<host:6222> --tunnel-addr <host:7000> [--public-host <host>]` → emits a bundle; leader: `tether cluster join approve <bundle> --wait` |
| `tether cluster sign-join <id> <nonce>` | folded into `tether cluster join prepare` (prepare self-signs — no separate sign-join step) |
| `tether cluster node-pub` | hidden debug command; bootstrap prereq is `tether cluster keygen --out /etc/tether/node-ident.nk` (`join prepare` derives the pubkey automatically) |
| `tether cluster wait <id> --phase VOTER` | per-operation `--wait` (e.g. `join approve … --wait`, `retire … --wait`, `transfer-leader … --wait`), or `tether cluster ops show <op-id>` / `tether cluster status --watch` |
| `tether cluster drain <n> --retire` | `tether cluster retire <n>` (plain `cluster drain <n>` is unchanged) |
| `tether cluster remove <n>` | `tether cluster recovery node remove <n> --manual` (routine path is `cluster retire`) |
| `tether cluster force-single …` | `tether cluster recovery force-single …` |
| `tether cluster recover --self-id …` | `tether cluster recovery rejoin prepare --self-id …` |
| `tether cluster restore <bundle> …` | `tether cluster recovery restore <bundle> …` |
| `tether cluster export-incident …` | `tether cluster recovery incident export …` |
| `tether cluster takeover-natsconf …` | `tether cluster reconcile nats --all --wait` |

The old top-level spellings (force-single/recover/restore/export-incident/remove/takeover-natsconf) remain as HIDDEN deprecated aliases for one release, then are deleted; `add`/`sign-join`/`wait` are deleted now.

## 0. What is a cluster, and what is quorum? (read this first.)

A tether *cluster* is several brokers that replicate one shared state via the Raft
protocol, so if one broker dies the others keep serving — that is high availability
(HA). Raft commits a write only when a **majority (quorum = ⌊voters/2⌋ + 1)**
acknowledges it: 3 voters → quorum 2, tolerates 1 loss; 5 voters → quorum 3,
tolerates 2. If live voters drop below quorum the cluster goes **read-only** (it
refuses writes) until a majority returns — this is anti-split-brain by design, not a
bug. Production HA therefore needs **at least 3 voters** (2 voters give no fault
tolerance: lose one and you are read-only). If you run a single broker you do not
need this file at all — single-broker is the fully supported default (see usage §1).

## 1. Grow the cluster (add a voter) — two-phase, prepare/approve

Admission is **two-phase**: the joining node prepares a self-signed join bundle
(carrying its node identity + raft/nats/tunnel addresses), then the leader approves it.

### 1.0 FIRST make the leader grow-ready — rebind a loopback advertise (v0.4.2; ONLINE, NOT force-single)

A broker migrated by `cluster init --from-existing` advertises whatever `broker.cluster.raft_addr`
was at init. If that was a **loopback** (`127.0.0.1:7400`) — the install default — a cross-network
voter can never dial this leader, and the grow stalls (the new voter is staged as a NON-VOTER and
never catches up — it does NOT wedge the cluster; you just `cluster recovery node remove --manual`
it and fix the address). `cluster doctor`/`status` shows a `raft_advertise` ADVISORY when self is
loopback. **The fix is ONLINE — do NOT use force-single for this** (force-single is the quorum-loss
escape hatch only; running it live is what crashed the broker once):

```
# (a) Bind the socket to all interfaces so peers can connect, then ONE clean broker restart.
#     This is a normal restart — agents reconnect on the same tunnel cert_fp (NOT a fatal-auth drop).
leader$ sudoedit /etc/tether/broker.yaml      # cluster.raft_addr: 0.0.0.0:7400   (BIND, not advertise)
leader$ sudo systemctl restart tether-broker

# (b) FIREWALL :7400 (raft) and :6222 (NATS routes) to the peer IPs BEFORE they are externally
#     reachable — any leaf that chains to the cluster CA is accepted as a peer, so a leaked route
#     leaf could otherwise join/observe raft. HARD gate, not advisory. (#24: the raft transport
#     :7400 skips hostname verification entirely and accepts a CN-only leaf; the NATS route mesh
#     :6222 does standard x509 and REQUIRES a SAN matching the route-URL host — but a same-CA leaf
#     WITH a matching SAN still joins, so the firewall, not the SAN, is the containment. See the
#     route-cert minting recipe below.)

# (c) Rebind the ADVERTISE addresses ONLINE (rewrites cluster_nodes.raft_addr + the raft Configuration
#     in place via AddVoter — an online address update, NO wipe). --route fixes the NATS-mesh twin.
leader$ sudo tether cluster set-raft-addr 155.98.36.32:7400 --route nats://155.98.36.32:6222
leader$ sudo tether cluster reconcile nats --all --wait      # re-render the mesh with the new route
```

`set-raft-addr` is idempotent (a re-run when the address already matches is a no-op) and refuses a
loopback/unspecified advertise unless `--allow-loopback` (single-host dev). It is **SELF-ONLY** — it
rebinds the LEADER's OWN advertise address (rebinding a peer from the leader would wedge the cluster,
so to readdress a follower `cluster transfer-leader` to it first, then run set-raft-addr there). For
the **one-time fix of a localhost-bootstrapped broker (pc732)**:
upgrade it to v0.4.2 first, then run (a)→(c) above. If a PRIOR failed grow left a half-added voter,
`cluster status` shows it; remove it with `cluster recovery node remove --manual <id>` before
re-growing.

```
# 0. ONCE per fresh node — mint the node-identity seed on the JOINING node. `cluster keygen`
#    is a hidden debug command; run it once on each new broker before it can join.
joiner$ tether cluster keygen --out /etc/tether/node-ident.nk      # mints the 0600 seed

# 1. On the JOINING node: prepare a self-signed join bundle. It derives its node-identity
#    public key from the seed and carries its full expose-home + NATS identity, so the new
#    voter can serve as an expose home AND be rendered into the NATS topology (external-review F4/Q2).
joiner$ tether cluster join prepare --node-id <node-id> --raft-addr <host:7400> \
          --nats-route nats://<host:6222> --tunnel-addr <host:7000> --public-host <host>
        <join-bundle>

# 2. On the LEADER: approve the bundle. --wait blocks until the new voter is admitted.
leader$ tether cluster join approve <join-bundle> --wait
        approved <node-id>

# 2a. PRE-CHECK the former-N1 node BEFORE re-rendering. Its conf is STILL standalone here, so
#     this preview prints the standalone-JS → JS-reset warning, mutating NOTHING — you learn that
#     step 3a is required before you touch anything. (Run it NOW: after step 3's `--all` renders
#     the conf clustered, this can no longer detect the transition, and `--all` itself does not
#     print the warning.)
former-N1$ sudo tether cluster reconcile nats --manual --plan

# 3. RE-RENDER nats.conf across the WHOLE cluster (one auto command renders every broker's
#    conf from the live roster). Growth grows RAFT membership; it does NOT form the NATS
#    route/auth mesh — each broker's nats.conf must list every peer's {server_name, route_url,
#    bus_nkey} (external-review F2).
leader$ sudo tether cluster reconcile nats --all --wait
#    Then bring NATS into the mesh ONE node at a time. FRESH new voters (empty JS store) just
#    restart. The former N=1 node is the EXCEPTION — do NOT plain-restart it (that orphans its
#    streams); bring it in LAST via 3a. Restart the fresh voters here:
fresh-voter$ sudo systemctl restart nats-server   # fresh voters only; one at a time

# 3a. ⚠ STANDALONE-JS → CLUSTERED-JS, the former N=1 node ONLY (fresh voters DON'T need this).
#     A broker migrated by `cluster init --from-existing` runs STANDALONE JetStream (no cluster{}
#     block — clustered JS refuses to start without configured routes, and a lone node can never
#     reach the JS meta quorum-of-2, so N=1 MUST run JS standalone). NATS does NOT migrate that
#     standalone JS state into the clustered meta: restarting with the cluster{} block IN PLACE
#     forms the meta FINE but ORPHANS every pre-existing stream (invisible to the clustered meta;
#     on-disk files linger as garbage the broker then re-creates over). Verified:
#     test/d9 TestD9Matrix/GrowInPlaceOrphansStreams.
#
#     RESET this node's JetStream store while stopped, then start it clustered LAST:
former-N1$ sudo systemctl stop nats-server
former-N1$ sudo rm -rf /var/lib/tether/jetstream      # the jetstream.store_dir from nats.conf — NOT raft/ or the DB
former-N1$ sudo systemctl start nats-server           # joins the mesh with a FRESH clustered JS meta
#     The broker re-creates its streams CLUSTERED on the next register/reconcile; reconcile then
#     raises them to R=ReplicasFor(voters). Verified end-to-end (reset + PRODUCTION rolling order,
#     new-node-first/former-N1-last, reaches working R=2): test/d9 GrowResetThenStaggeredWorks.
#
#     ⚠ DATA IMPACT — the reset DROPS ALL of this node's JetStream audit/history. There is NO
#     separate "audit stream": audit records ARE the contents of history-<sid>, and the
#     re-derivation cursor (audit_published_index) lives in SQLite and SURVIVES the `rm -rf`, so
#     the publisher backfills NOTHING. Reset: history-<sid> (incl. `tether history` + the
#     incident-export forensic bundle), the events stream, and in-flight OBJ_xfer transfers
#     (drain/finish them FIRST). Effectively NOTHING re-derives (at most the cursor-to-commit lag
#     at the moment of the wipe). For a fresh/test cluster this is fine — accept it and skip the
#     next paragraph.
#
#     TO PRESERVE audit/history across the first grow (data-preserving alternative): while the
#     node is STILL standalone (JS serving), snapshot every stream, then restore after the meta
#     forms (restores at R=1; reconcile then raises to target R):
#       former-N1$ for s in $(nats stream ls -n); do nats stream backup "$s" "/var/backups/js/$s"; done
#       former-N1$  ... stop / rm -rf jetstream / start clustered (above) ...
#       former-N1$ for d in /var/backups/js/*; do nats stream restore "$(basename "$d")" "$d"; done

# 3b. VERIFY the JetStream meta formed (the failure mode is a SILENT JS problem, so check it,
#     not just raft): `nats --server <broker> stream ls` must RETURN (not hang) on each broker.
#     NOTE: this drill is proven on the embedded nats-server (go.mod); if the production
#     nats-server (scripts/install.sh) version diverges, re-run the live grow drill first.

# 4. Verify.
leader$ tether cluster status            # the new node walks JOIN_VERIFIED_PENDING_VOTER -> CATCHING_UP -> VOTER
                                         # + every voter shows reachable (nats-health), no applied-lag
```

Half-success is visible, never silently forked: if AddVoter fails the node shows
`VOTER_ADD_FAILED`; if catch-up stalls it stays `CATCHING_UP` with a stall hint.
`cluster status` shows the stuck phase + the next command; `cluster doctor` is the
secrets/preflight check. A new leader runs a
membership reconciliation pass on startup that forward-completes a mid-add node.

### 1.1 Spread proxy load onto the new voter (optional)

The auto-rehome reaper only moves a `__proxy__` home when its current home goes
DOWN; nothing migrates a healthy proxy onto a freshly-added empty voter. After the
new node reaches VOTER, even out the proxy homes (greedy, to max−min ≤ 1):

```
leader$ tether cluster rebalance proxy --dry-run   # preview the moves
leader$ tether cluster rebalance proxy             # spread __proxy__ homes; each agent re-establishes on its new home
```

Each move briefly drops that proxy's public listener while the agent re-establishes
(self-healing). Leader-only; a failure/partial pass exits transient — re-run once the
cluster is HEALTHY-HA (re-running is idempotent).

## 2. Drain / retire a node

```
leader$ tether cluster drain <node-id>            # migrate exposes off, keep it a voter (sheds serving load)
leader$ tether cluster retire <node-id>           # drain THEN remove from the cluster (resumable; was `drain --retire`)
leader$ tether cluster drain <node-id> --abort    # cancel an in-progress drain
leader$ tether cluster drain <node-id> --now       # skip the notice period
```

Before the op, a **quorum projection** is shown: `after op: N voters, quorum=K,
tolerate F failures`. **If F==0** (e.g. retiring the 3rd of 3, or any drain at N=2)
you must **type the node_id to confirm** — `--yes` is never accepted. Retire is
**refused** while any stream this node owns is still below its target replica count
(its data is not yet redundant elsewhere).

> **Pinned exposes block silent migration.** An expose created with `tether expose
> --no-rebuild` (rebuild-OFF) is the operator's explicit "do not move this" choice, so
> `cluster drain` REFUSES to auto-migrate it and enumerates the offending ports — free or
> re-decide them, then re-run drain. Inspect an expose's home/epoch/rebuild with
> `tether expose explain <name>`; pin a new one to a specific home with `tether expose
> --on-broker <node-id>`.

> **Maintenance alerts.** Before a planned reboot, raise a cluster-wide notice from the
> broker host: `tether alert raise --severity severe --message "brk-b maintenance
> 02:00–03:00 UTC" --label brk-b` (operator-only, via the admin socket). It shows in
> `tether alert ls` + the ps/node banner cluster-wide; clear it after with `tether alert
> clear manual:brk-b`. `severe` is banner-only — it does NOT block writes (only
> `quorum_lost` / `force_single_active` hard-gate destructive ops).

### 2.1 Retire is a topology change, NOT a trust revocation (account.nk / CA rotation)

A retired node's roster + raft membership are removed immediately, but its
`account.nk` and the cluster CA are **shared and NOT rotated** — a retired node that
keeps those files can still mint JWTs / present a route cert until they are rotated.
**If you retire a node because it may be compromised, you MUST rotate** (full-fleet
re-provision):

```
# 1. Retire + power off the suspect node.
leader$ tether cluster retire <node-id>

# 2. Generate a NEW account key + a NEW cluster CA on a trusted host.
trusted$ go install github.com/nats-io/nkeys/nk@latest
trusted$ ~/go/bin/nk -gen account > account.nk.new
trusted$ ~/go/bin/nk -inkey account.nk.new -pubout
trusted$ # (re-issue a fresh cluster CA + per-node route leaf certs with your PKI of choice)
trusted$ # IMPORTANT (#24): each per-node route (and tunnel) leaf MUST carry a subjectAltName
trusted$ #   matching that node's route-URL host, or the NATS route mesh (cluster{tls{verify:true}},
trusted$ #   standard Go x509) rejects it — "x509: certificate relies on legacy Common Name field,
trusted$ #   use SANs instead" — and routes never form (new voters stall CATCHING_UP). Use a DNS SAN
trusted$ #   for a hostname route URL, an IP SAN for an IP one (e.g. nats://10.0.0.2:6222 -> IP SAN):
trusted$ #     openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
trusted$ #       -keyout route-key.pem -subj "/CN=<node>" \
trusted$ #       -addext "subjectAltName=DNS:<node>"          # or IP:<addr> for an IP route URL
trusted$ #       -addext "extendedKeyUsage=serverAuth,clientAuth" -out route.csr
trusted$ #     # then sign route.csr with the cluster CA (copying the SAN + EKU into the cert)
trusted$ #   tether's OWN raft transport skips hostname checks and would accept a CN-only leaf — do
trusted$ #   NOT be misled by that (see internal/cluster/transport.go): the nats route mesh reusing
trusted$ #   the same route-cert.pem needs the SAN. The sim's test/simcluster/lib/secrets.sh mints
trusted$ #   SAN-bearing leaves as a working reference.

# 3. Distribute the new account.nk + CA to EVERY surviving broker over a trusted
#    channel (scp, never committed; 0600). Update /etc/tether/secrets/{account.nk,cluster-ca.pem}
#    and the corresponding route-cert.pem/route-key.pem leaf files on each node.

# 4. Re-render nats.conf across the cluster (`tether cluster reconcile nats --all --wait`),
#    then rolling-restart NATS + the broker so both NATS route auth and broker auth_callout
#    load the new secrets. Old JWTs signed by the old account key expire within their TTL;
#    the new CA rejects the retired node's old route cert immediately.
```

After `cluster retire`, immediately run `tether cluster reconcile nats --all --wait`
so the retired node's NATS route/user grants are removed from generated
configuration. **Retire is not considered safe against a compromised node until
this re-render/restart and the key/CA rotation above are done.**

### 2.2 De-cluster the LONE survivor back to standalone JetStream (v0.4.2 shrink)

`cluster retire` shrinks the voter set down to ONE (it refuses the last voter — that is force-single
territory). The lone survivor is still running a CLUSTERED nats.conf (a `cluster{}` block + a
clustered-JS meta-group-of-1 it can never reach a quorum for). The FINAL downgrade step re-renders it
standalone:

```
# ONLY when exactly ONE voter remains (retire the rest FIRST). De-clustering with live peers tears the
# route mesh, so --confirm-single is mandatory and a FULL restart is required (dropping cluster{} is
# NOT SIGHUP-reloadable).
lone$ sudo tether cluster reconcile nats --to-standalone --confirm-single --server-name <self> --broker-nkey <self-bus-nkey>
# (--broker-nkey = this broker's bus nkey — derive from the broker.nk seed in secrets_dir, or read
#  cluster_nodes.bus_nkey_pub; it is NOT in broker.yaml. A multi-broker conf lists every peer's nkey so
#  --to-standalone cannot auto-pick self's — pass it explicitly. A single-broker conf auto-reads it.)
lone$ sudo systemctl restart nats-server      # FULL restart loads the standalone conf
# ⚠ clustered-JS → standalone-JS does NOT migrate in place — RESET the JS store (the command prints
#   the exact rm -rf). DATA IMPACT: drops ALL JetStream audit/history (history-<sid>, events, in-flight
#   OBJ_xfer — drain first; `nats stream backup`/`restore` to preserve). Mirror of grow §1 step 3a.
lone$ sudo systemctl stop nats-server && sudo rm -rf /var/lib/tether/jetstream && sudo systemctl start nats-server
```

Lands at "N=1 single-voter cluster, standalone JS" — the supported post-init shape. (Fully EXITING
cluster mode — clearing `broker.cluster.*`, handing the WAL back to plain `storage.Open` — is a
separate epic; it crosses the cluster.Node-owns-the-WAL invariant.)

### 2.3 Operator-command semantics (grow / shrink / protected mode) — read before edge operations

| command | valid at | who | in PROTECTED MODE (quorum-lost) | notes |
|---|---|---|---|---|
| `set-raft-addr` | any N, **self-only** | leader | **refused** (Proposes a raft write — no quorum, no commit) → force-single | rebinds the LEADER's OWN advertise; readdress a follower by `transfer-leader` to it first |
| `join approve` (grow) | N≥1 | leader | **refused** | N=1→2 allowed (the pc732 case); prereq: self-advertise reachable (§1.0) |
| `retire` / `drain` (shrink) | N≥2 (refuses the last voter) | leader | **refused** | N=2→1 forces a typed F==0 confirm |
| `reconcile nats --to-standalone` | **N=1 ONLY** (`--confirm-single`) | lone survivor | n/a (N=1 has quorum-of-1) | refuses if already standalone; FULL restart + JS reset required |
| `recovery force-single --online` (escape) | quorum-lost | **RUNNING broker** (admin socket) | **the preferred path** (no second outage) | recovers to N=1 IN-PROCESS; routine ops work again. OFFLINE disk path is the floor if the broker won't start |

**Edge cases.** (1) *Multiple brokers but you want to downgrade*: `retire` one at a time down to N=1
FIRST, THEN `reconcile nats --to-standalone` (it refuses while peers remain). (2) *One broker but you
want to upgrade*: the normal N=1→2 grow (§1.0 rebind if loopback, then join). (3) *Protected mode
(quorum-lost / read-only)*: every routine cluster op is refused — they Propose raft writes that cannot
commit without a quorum — so `cluster recovery force-single` is the escape. Prefer `--online`: it
recovers IN-PROCESS on the RUNNING survivor broker via its admin socket (no second outage, and it can
be drilled with `--dry-run` on a healthy cluster). The OFFLINE disk-surgery path stays the floor for a
broker that cannot start. After it lands you at N=1 (`force_single_active`), routine cluster ops work
again (quorum-of-1), though destructive DATA ops stay hard-gated until you regrow to HA (N≥3).

## 3. Quorum loss — the force-single escape hatch

If a majority of brokers are permanently dead the cluster goes **read-only**.
`cluster status` reports `QUORUM_LOST` (exit 2). To resume service on a survivor, force this node to a
lone single-voter cluster. There are TWO paths — **prefer ONLINE**; OFFLINE is the floor.

### 3.0 ONLINE (preferred — the survivor broker keeps RUNNING)

When the survivor's daemon is still up (its admin socket answers), recover **in-process** — no daemon
stop, no second outage. The broker hot-swaps only its raft instance to `{self}`; the data plane keeps
serving reads throughout. A two-step **arm→commit** flow gates it:

```
# 0. CONFIRM the other brokers are TRULY dead (powered off / unreachable to AGENTS), not merely
#    partitioned from you. A merely-partitioned-but-alive peer WILL split-brain.

# 1. (optional) DRILL it on the LIVE cluster — zero mutation, evaluates the gates + prints the peer probe:
survivor$ sudo tether cluster recovery force-single --online --dry-run \
            --self-id <this-node-id> --confirm-peers-dead <dead-node-id-1>,<dead-node-id-2>

# 2. Recover. The broker REFUSES unless it has been CONTINUOUSLY quorum-lost for the dwell (~15s), and
#    HARD-REFUSES if any listed peer still answers a TCP probe on its raft/nats/tunnel port (alive ->
#    split-brain). You TYPE this node's id to confirm (no --yes); the broker validates --self-id matches
#    the socket owner, swaps raft to {self} IN-PROCESS, then raises force_single_active.
survivor$ sudo tether cluster recovery force-single --online \
            --self-id <this-node-id> --confirm-peers-dead <dead-node-id-1>,<dead-node-id-2>
```

The broker never restarts; once the command returns, the node is a writable single voter
(`force_single_active`, `status` exit 3) — routine cluster ops (quorum-of-1) work again. If the admin
socket is unreachable (the broker is truly DOWN), the command prints the OFFLINE floor command below.

### 3.1 OFFLINE (the floor — for a broker that will not start)

If the survivor's daemon cannot start (crash-loop / corrupt config) so its admin socket is dead, take
the disk directly with the daemon STOPPED:

```
# 0. CONFIRM the other brokers are TRULY dead (powered off / unreachable to AGENTS),
#    not merely partitioned from you. A merely-partitioned-but-alive peer WILL split-brain.

# 1. STOP the daemon so the offline tool can take the disk (and systemd won't restart it).
survivor$ sudo systemctl mask tether-broker && sudo systemctl stop tether-broker

# 2. Inspect the on-disk state (no NATS needed).
survivor$ sudo tether cluster status --offline --db /var/lib/tether/tether.db

# 3. Force this node to a single-voter cluster. List EVERY other node_id; the tool
#    HARD-REFUSES if any of them still accepts a TCP connection on :7400 (alive ->
#    split-brain), if there is no existing raft state, or if the daemon still holds
#    the store. You must TYPE this node's id to confirm — no --yes.
survivor$ sudo tether cluster recovery force-single \
            --self-id <this-node-id> --self-addr <this-host:7400> \
            --confirm-peers-dead <dead-node-id-1>,<dead-node-id-2>

# 4. Bring the daemon back. It runs as a single voter (NO HA / NO integrity until recovered).
survivor$ sudo systemctl unmask tether-broker && sudo systemctl start tether-broker
```

`force-single` rewrites the raft configuration to `{self}` via `RecoverCluster`,
which forward-replays this node's local log into SQLite (the recovery point is the
node's last local log index — its uncommitted tail is committed by fiat, logged
loudly). The node raises a persistent `force_single_active` severe (`status` exit 3).

### 3.2 After force-single: restore the data plane (JetStream) + clean up abandoned peers (G2)

force-single restores the CONTROL plane instantly, but the DATA plane (JetStream — file transfers,
history, audit) needs one more step, and the abandoned peers must leave the client roster.

**Restore JetStream (#20).** A survivor whose nats.conf still carries a `cluster{}` block after
force-single wedges JetStream at `503` (no quorum-of-2) — *silently*, since the alert path itself rides
JetStream. `cluster status` now raises a **DATA-PLANE DEGRADED** banner when this is the case.

- **OFFLINE force-single already fixed it**: it re-renders the survivor's nats.conf to standalone
  JetStream BEFORE you restart the broker (§3.1 step 3 prints "nats.conf de-clustered to standalone").
  Just RESET the JS store as warned (it prints the `mv jetstream …` guidance) before the restart.
- **ONLINE force-single leaves the daemon running** and does NOT restart nats-server (that would blip the
  shared core-NATS control plane). De-cluster it explicitly, then restart ONLY nats-server:

  ```
  survivor$ sudo tether cluster reconcile nats --to-standalone --confirm-single --server-name <self> --broker-nkey <self-bus-nkey>
  # (--broker-nkey is this broker's bus nkey — derive from the broker.nk seed in secrets_dir, or read
  #  cluster_nodes.bus_nkey_pub; it is NOT in broker.yaml. A still-clustered conf lists every peer's nkey
  #  so --to-standalone cannot auto-pick self's — pass it explicitly)
  # detached so stopping nats-server does not cut the ctl channel mid-command:
  survivor$ sudo systemd-run --collect --unit tether-nats-restart \
              sh -c 'mv /var/lib/tether/jetstream /var/lib/tether/jetstream.clustered-bak.$(date +%s); systemctl restart nats-server'
  ```

  The abandoned peers are already pruned (below), so the N=1 voter tally passes and `--to-standalone`
  is unlocked.

**Abandoned-peer roster (#12).** force-single now PRUNES the abandoned peers from the roster automatically
(both the online and offline paths), so agents/ctl converge to N=1 immediately — the dead endpoints stop
appearing in every client's failover list. If you upgraded from an older binary and a **ghost VOTER**
lingers (a roster row still phase=VOTER but absent from the raft config — force-single moved it out of
raft, but the old code left the row), remove it explicitly on the leader:

```
leader$ sudo tether cluster recovery node remove <ghost-node-id> --manual
```

This now passes through a VOTER row that is not in the committed raft config (proven leader-side) —
deleting it cannot fork quorum (it is already out of raft). `cluster status` then shows N=1 with no
INCONSISTENT rows and no DEGRADED banner.

### Bring a recovered/returning node back

Each former peer must be **wiped** before rejoining (its divergent state cannot be
auto-merged):

```
# Daemon stopped on the returning node.
returning$ sudo systemctl mask tether-broker && sudo systemctl stop tether-broker

# Dump this node's divergent rows for forensics, THEN wipe raft/ + tether.db.
# The dump is fsync'd (0600, never overwrites a prior dump) BEFORE any wipe; if the
# dump fails, the wipe is refused. You must pass --self-id and TYPE the node_id to
# confirm (no --yes) — this proves you are wiping the intended node.
returning$ sudo tether cluster recovery rejoin prepare --self-id <returning-node-id> --dump-divergent /root/divergent-$(hostname).json

# Reinitialize this host as a clean single-voter seed so it has raft/ state before start.
# Do NOT start tether-broker between recover and this init; the daemon refuses cluster
# mode when broker.cluster.data_dir is set but raft/ is absent.
returning$ sudo tether cluster init --from-existing \
             --self-id <returning-node-id> --name <returning-name> --node-ident-pub <Uxxxx...> \
             --raft-addr <host:7400> --nats-route <host:6222> \
             --tunnel-addr <host:7000> --public-host <dns> \
             --secrets-dir /etc/tether/secrets

# Now start it and rejoin it as a clean node (section 1: `join prepare` on the returning
# node, then `join approve <bundle> --wait` on the leader).
returning$ sudo systemctl unmask tether-broker && sudo systemctl start tether-broker
returning$ tether cluster join prepare --node-id <returning-node-id> --raft-addr <host:7400> \
             --nats-route nats://<host:6222> --tunnel-addr <host:7000> --public-host <host>
leader$    tether cluster join approve <join-bundle> --wait
```

> **Drill it.** Practice force-single -> rejoin on a 3-node staging cluster before
> you need it in production (§13.12). The safety gates are the (b)/(c)/(d) hard
> preconditions + the typed confirmation, NOT the displayed peer-unreachable timer.

---

## 4. Migrate a LIVE single broker into a cluster (the D9 one-time cutover)

> **This is a one-way, flag-day migration.** proto v2 breaks the wire: a v1 agent CANNOT
> connect to a v2 broker, so EVERY agent must be reinstalled on v2 afterward. Rollback =
> restore `tether.db.bak` (returns to the v2 SINGLE broker, NOT the v1 fleet — there is no
> path back to a v1 fleet once reinstalled). Practice on staging first.

```
# 0. PROVISION the §15 secrets on the broker host BEFORE migrating (0600; FDE volume):
#    /etc/tether/secrets/{cluster-ca.pem, route-cert.pem, route-key.pem(0600),
#                         tunnel-cert.pem, tunnel-key.pem(0600), broker.nk(0600),
#                         node-ident.nk(0600), account.nk(0600)}
#    tether does NOT generate keys — you own the CA. Verify with the doctor preflight:
broker$ tether cluster doctor --secrets-dir /etc/tether/secrets
#    Missing / unreadable / world-readable private key => FATAL. FDE-absent => advisory
#    (psk_at_rest_unprotected): ensure the secrets volume is full-disk-encrypted.

# 1. STOP the broker (the migration is OFFLINE disk surgery; the daemon must not be writing).
broker$ sudo systemctl stop tether-broker

# 2. Migrate the DB + bootstrap a single-voter raft. Idempotent (re-runnable after a kill-9):
#    forward migrations 0008-0013, seed cluster_meta(applied_index=0, self_node_id) + the
#    self VOTER row (cert_fp DERIVED from the tunnel cert) + home_broker backfill, then
#    raft.BootstrapCluster({self}) LAST. The pristine pre-migration DB is kept in
#    tether.db.bak (skip-if-exists). You TYPE the node_id to confirm (no --yes).
broker$ sudo tether cluster init --from-existing \
          --self-id <node-id> --name <broker-name> --node-ident-pub <Uxxxx...> \
          --raft-addr <host:7400> --nats-route <host:6222> \
          --tunnel-addr <host:7000> --public-host <dns> \
          --secrets-dir /etc/tether/secrets

# 3. TAKE OVER nats.conf (rewrite it with the cluster directives + auth_callout, preserving
#    the install.sh websocket/jetstream + any documented tuning). Refuses fail-closed if the
#    conf has a directive tether does not recognize. Prints the before/after ownership table.
#    The auto path derives the server-name / route-url / account-issuer / per-broker bus
#    nkey from the live roster + secrets, so there are no per-broker flags to pass by hand.
broker$ sudo tether cluster reconcile nats --all --wait

# 4. Restart nats-server so the new authorization{} (cluster.apply.* ACL) is live BEFORE the
#    broker connects in cluster mode (else it fails closed: no ACL).
broker$ sudo systemctl restart nats-server
# Confirm the seeded single-voter roster while tether-broker is still stopped:
broker$ sudo tether cluster status --offline --db /var/lib/tether/tether.db

# 5. Point the broker at the cluster + start it (now a single-voter cluster, N=1).
#    In /etc/tether/broker.yaml under broker.cluster: data_dir / raft_addr / secrets_dir.
#    Cluster mode auto-loads /etc/tether/secrets/{broker.nk,account.nk}; an explicit
#    --auth-callout-seeds-dir is only needed if those seeds live somewhere else.
broker$ sudo systemctl start tether-broker

# 6. Reinstall ALL agents on v2 (the wire break forces this), then grow to N>=3 (section 1:
#    `join prepare` on each new node, `join approve <bundle> --wait` on the leader). x2 for N=3.
new-node$ tether cluster join prepare --node-id <node-2> --raft-addr <host:7400> \
            --nats-route nats://<host:6222> --tunnel-addr <host:7000> --public-host <host>
leader$   tether cluster join approve <join-bundle> --wait
```

> **Rollback** (before agents are reinstalled): `systemctl stop tether-broker`, restore
> `tether.db.bak` over `tether.db` (and `rm tether.db-wal tether.db-shm`), restore
> `nats.conf.bak.<ts>`, remove `broker.cluster.*` from broker.yaml, `systemctl start`. This
> returns to the **v2 single broker** (cluster-mode OFF) — NOT to a v1 fleet.
>
> **HA guarantee** (§17): N=1 has no redundancy; N=2 is read-survives / write-zero-fault;
> only N>=3 with JS replicas at target gives committed-0-loss HA. `cluster status` shows
> `stream-replicas actual/target` + raises `replication_degraded` until they converge.

## 5. Backup & disaster recovery (`cluster backup` / `cluster recovery restore`)

A backup is a self-describing **bundle directory** — `state.db` (a consistent copy of the
committed FSM DB: roster, ports, sessions, alerts, the applied cursor) + `manifest.json`
(identity + provenance fingerprints, **never keys/seeds/PINs** — those stay in the secrets dir).
The raft log is **node-local and intentionally NOT carried**; restore re-bootstraps a fresh
single voter. The bundle is **not a credential**: a restore *requires* the node's secrets dir.

```bash
# ONLINE backup (daemon running; any node, leader OR follower — read-only, no raft write):
tether cluster backup --out /var/backups/tether-$(date +%F)

# OFFLINE backup (daemon STOPPED):
systemctl stop tether-broker
tether cluster backup --offline --out /var/backups/tether-$(date +%F) \
    --db /var/lib/tether/tether.db --secrets-dir /etc/tether/secrets
```

Take backups on a schedule + before any destructive op (drain/retire/remove/force-single). A
single backup off ANY node is the whole committed state — you do **not** need one per node.

### 5.1 Restore (OFFLINE, IRREVERSIBLE)

Use restore to rebuild a destroyed node, or to roll the whole cluster back to a known-good
point. Restore is **offline-only**, overwrites the on-disk DB (preserving it at `<db>.bak`), and
re-bootstraps a **single voter** — you then re-grow with the §1 join flow (`cluster join prepare` / `join approve`).

```bash
# 0. On the target host, place the SAME node's secrets dir (the live tunnel cert is the
#    un-forgeable provenance anchor — a restore onto a different node's secrets is REFUSED).
# 1. Stop the daemon.
systemctl stop tether-broker
# 2. Restore. You TYPE the node_id to confirm (no --yes, never a machine escape — restore is
#    irreversible + identity-affecting). The bundle's identity must match this host's secrets.
#    Restore is RE-RUNNABLE after a kill-9: it marks restore_in_progress and the daemon REFUSES to
#    start until restore completes — do NOT start the daemon mid-restore; just re-run the line.
tether cluster recovery restore /var/backups/tether-2026-06-24 --confirm-node-id brk-a \
    --secrets-dir /etc/tether/secrets
# 3. Start the daemon. It comes up as a single-voter cluster (NO HA until you re-grow).
systemctl start tether-broker
tether cluster status            # exit 1 DEGRADED (N=1, no redundancy) until re-grown; roster = {self}
                                 # (NOT exit 3 — restore is not force-single; it clears that marker)
# 4. Re-grow to N>=3 with the §1 join flow (`cluster join prepare` / `join approve`),
#    re-rendering nats.conf with `tether cluster reconcile nats --all --wait`.
```

The restore **resets the applied cursor to 0** and **prunes the old peers from the roster** so
the restored node is a clean single-voter origin (the original membership is preserved in the
bundle's `manifest.json` for the incident record). Provenance is gated on the **live tunnel-cert
fingerprint** (== the manifest's `self_cert_fp` == the bundle's self-row `cert_fp`) + the typed
`--confirm-node-id` — a foreign or torn/edited bundle is refused before any disk mutation.

### 5.2 Full-cluster disaster recovery (all nodes lost)

```bash
# 1. On a FRESH box, restore this node's secrets dir from your secret store.
# 2. cluster recovery restore the latest bundle (§5.1) with --confirm-node-id <the original node_id>.
# 3. Start the daemon (single voter N=1), then re-grow to N>=3 with the §1 join flow
#    (`cluster join prepare` on each new node, `join approve <bundle> --wait` on the leader).
# 4. Agents reconnect + re-pin; exposes re-home onto the live broker automatically (D6).
```

### 5.3 Identity-only manifest replay (recover → re-init)

`cluster recovery rejoin prepare` (§3) can capture a node's IDENTITY into a manifest before it
wipes, so the re-init does not re-type the 9 identity flags:

```bash
# On the returning node (daemon stopped): dump forensics AND emit an identity manifest.
tether cluster recovery rejoin prepare --self-id brk-b --dump-divergent /root/divergent-brk-b.json \
    --emit-manifest /root/brk-b-ident.json --secrets-dir /etc/tether/secrets
# Re-init from the manifest (cert_fp is re-derived LIVE from this host's secrets, not replayed —
# a rotated cert still pins agents correctly). The manifest is identity-only: NO business rows.
tether cluster init --from-manifest /root/brk-b-ident.json --secrets-dir /etc/tether/secrets
# Then rejoin via the §1 join flow on the leader (`join approve <bundle> --wait`).
```

## 6. Rolling upgrade (followers-first, leader-last)

A rolling upgrade replaces the binary on each broker one at a time WITHOUT downtime, as long as
the new and old binaries speak the SAME proto version. A **proto bump (v2→v3) is a flag-day, NOT
a rolling upgrade** (the wire is incompatible — stop the whole fleet, upgrade, restart).

```bash
# 0. Confirm the target release is the SAME proto. `cluster status` shows each node's running
#    VER (a live self-report). `cluster join approve` HARD-REJECTS a joiner with a different proto.
tether cluster status                    # note the leader + every node's VER

# 1. Upgrade FOLLOWERS first, one at a time (the leader keeps serving):
#    on each follower host —
systemctl stop tether-broker
#    (swap the binary)
systemctl start tether-broker
tether cluster status --watch                     # block until it is a full voter again (was `cluster wait`)
tether cluster status                            # confirm its VER updated + REACH ok before the next

# 2. Upgrade the LEADER last. Hand off leadership FIRST so you never re-elect mid-rollout:
tether cluster transfer-leader <an-already-upgraded-follower> --wait
#    then upgrade the (now ex-leader) host as in step 1.

# 3. Verify: every node shows the new VER, REACH ok, no INCONSISTENT, health HEALTHY_HA.
tether cluster status
```

Notes:
- A mixed-release window (some nodes new, some old) is generally safe as long as the proto is
  unchanged — `cluster join approve` only WARNS on a release skew (it rejects only a proto mismatch), and a
  re-joining drained node may legitimately be older than the now-upgraded leader during a rollback.
  **CAVEAT (DB schema):** if the NEW release adds a DB migration, an upgraded node forward-migrates
  its DB and you CANNOT then roll that node back to the older binary (migrations are forward-only;
  the old binary has no downgrade path). Upgrade is one-way per node once it has migrated — keep the
  `tether.db` backup (`cluster backup`) from BEFORE the upgrade for a true rollback. Same-proto
  releases that add NO migration are freely roll-back-able.
- Watch `cluster status` STREAMS (actual/target) between steps: a node restart transiently drops a
  JS replica; wait for `replication_degraded` to clear (actual==target) before upgrading the next.
