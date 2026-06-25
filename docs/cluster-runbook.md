# tether cluster — operator runbook (D7)

> Operator-facing procedures for the distributed-broker cluster lifecycle. The
> binding contract is `docs/distributed-broker-architecture.md` §8 / §17; this file
> is the copy-pasteable drill. **All cluster admin commands run on a broker host**
> (admin is strictly local — no network bypass). A non-leader broker fails fast and
> names the leader host to re-run on.

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

## 1. Grow the cluster (add a voter) — two-phase, challenge/response

`cluster add` is a **two-phase** admission: the leader issues a single-use nonce,
the joining node signs it with its node-identity key, then the leader admits it.

```
# 0. On the JOINING node: print its node-identity public key.
joiner$ tether cluster node-pub                       # -> Uxxxx...   (or `cluster keygen --out /etc/tether/node-ident.nk`)

# 1. On the LEADER: start the add (no token yet). It prints a challenge nonce.
leader$ tether cluster add <node-id> <host:7400> <Uxxxx...>
        challenge nonce: <nonce>
        on the joining node run:  tether cluster sign-join <node-id> <nonce>

# 2. On the JOINING node: sign the nonce. It prints <nonce>:<sigHex>.
joiner$ tether cluster sign-join <node-id> <nonce>
        <nonce>:<sigHex>

# 3. On the LEADER: re-run with the token. The token call MUST carry the joiner's full
#    expose-home + NATS identity (else the new voter can hold raft votes but can never serve
#    as an expose home nor be rendered into the NATS topology — external-review F4/Q2).
leader$ tether cluster add <node-id> <host:7400> <Uxxxx...> --join-token <nonce>:<sigHex> \
          --tunnel-addr <host:7000> --cert-fp <sha256:...> --public-host <host> \
          --nats-route nats://<host>:6222
        added <node-id>

# 4. RE-RENDER nats.conf on EVERY node with the COMPLETE peer set, then restart NATS.
#    `cluster add` grows RAFT membership; it does NOT form the NATS route/auth mesh. Each
#    broker's nats.conf must list every peer's {server_name, route_url, bus_nkey}; growth
#    therefore re-runs takeover-natsconf on ALL nodes (external-review F2). Restart NATS one
#    node at a time (rolling), leader LAST, verifying `cluster status` reachability after each.
each-broker$ sudo tether cluster takeover-natsconf --secrets-dir /etc/tether/secrets \
               --server-name <self-id> --route-url nats://<self-host>:6222 \
               --account-issuer <account-public-nkey> --broker-nkey <self-broker-public-nkey> \
               --peer <other1-id>,nats://<other1-host>:6222,<other1-bus-nkey> \
               --peer <other2-id>,nats://<other2-host>:6222,<other2-bus-nkey>
each-broker$ sudo systemctl restart nats-server   # rolling; leader last

# 5. Verify.
leader$ tether cluster status            # the new node walks JOIN_VERIFIED_PENDING_VOTER -> CATCHING_UP -> VOTER
                                         # + every voter shows reachable (nats-health), no applied-lag
```

Half-success is visible, never silently forked: if AddVoter fails the node shows
`VOTER_ADD_FAILED`; if catch-up stalls it stays `CATCHING_UP` with a stall hint.
`cluster status` shows the stuck phase + the next command; `cluster doctor` is the
secrets/preflight check. A new leader runs a
membership reconciliation pass on startup that forward-completes a mid-add node.

## 2. Drain / retire a node

```
leader$ tether cluster drain <node-id>            # migrate exposes off, keep it a voter (sheds serving load)
leader$ tether cluster drain <node-id> --retire   # drain THEN remove from the cluster
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
leader$ tether cluster drain <node-id> --retire

# 2. Generate a NEW account key + a NEW cluster CA on a trusted host.
trusted$ go install github.com/nats-io/nkeys/nk@latest
trusted$ ~/go/bin/nk -gen account > account.nk.new
trusted$ ~/go/bin/nk -inkey account.nk.new -pubout
trusted$ # (re-issue a fresh cluster CA + per-node route leaf certs with your PKI of choice)

# 3. Distribute the new account.nk + CA to EVERY surviving broker over a trusted
#    channel (scp, never committed; 0600). Update /etc/tether/secrets/{account.nk,cluster-ca.pem}
#    and the corresponding route-cert.pem/route-key.pem leaf files on each node.

# 4. Re-render nats.conf on every surviving node with takeover-natsconf, then rolling-restart
#    NATS + the broker so both NATS route auth and broker auth_callout load the new secrets.
#    Old JWTs signed by the old account key expire within their TTL; the new CA
#    rejects the retired node's old route cert immediately.
```

After `drain --retire`, immediately re-run `takeover-natsconf` on every surviving
node so the retired node's NATS route/user grants are removed from generated
configuration. **Retire is not considered safe against a compromised node until
this re-render/restart and the key/CA rotation above are done.**

## 3. Quorum loss — the force-single escape hatch (OFFLINE)

If a majority of brokers are permanently dead the cluster goes **read-only**.
`cluster status` reports `QUORUM_LOST` (exit 2). To resume service on a survivor:

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
survivor$ sudo tether cluster force-single \
            --self-id <this-node-id> --self-addr <this-host:7400> \
            --confirm-peers-dead <dead-node-id-1>,<dead-node-id-2>

# 4. Bring the daemon back. It runs as a single voter (NO HA / NO integrity until recovered).
survivor$ sudo systemctl unmask tether-broker && sudo systemctl start tether-broker
```

`force-single` rewrites the raft configuration to `{self}` via `RecoverCluster`,
which forward-replays this node's local log into SQLite (the recovery point is the
node's last local log index — its uncommitted tail is committed by fiat, logged
loudly). The node raises a persistent `force_single_active` severe (`status` exit 3).

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
returning$ sudo tether cluster recover --self-id <returning-node-id> --dump-divergent /root/divergent-$(hostname).json

# Reinitialize this host as a clean single-voter seed so it has raft/ state before start.
# Do NOT start tether-broker between recover and this init; the daemon refuses cluster
# mode when broker.cluster.data_dir is set but raft/ is absent.
returning$ sudo tether cluster init --from-existing \
             --self-id <returning-node-id> --name <returning-name> --node-ident-pub <Uxxxx...> \
             --raft-addr <host:7400> --nats-route <host:6222> \
             --tunnel-addr <host:7000> --public-host <dns> \
             --secrets-dir /etc/tether/secrets

# Now start it and rejoin it as a clean node (section 1).
returning$ sudo systemctl unmask tether-broker && sudo systemctl start tether-broker
leader$    tether cluster add <returning-node-id> <host:7400> <Uxxxx...>
```

> **Drill it.** Practice force-single -> recover on a 3-node staging cluster before
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
broker$ sudo tether cluster takeover-natsconf \
          --secrets-dir /etc/tether/secrets \
          --server-name <node-id> --route-url nats://<host:6222> \
          --account-issuer <account-public-nkey> --broker-nkey <broker-public-nkey>
# --account-issuer may be read from an existing auth_callout issuer. --broker-nkey
# is auto-read only when the existing authorization block has exactly one nkey user;
# multi-broker generated configs must pass this node's --broker-nkey explicitly.

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

# 6. Reinstall ALL agents on v2 (the wire break forces this), then grow to N>=3 (section 1).
leader$ tether cluster add <node-2> <host:7400> <Uxxxx...>   # x2 for N=3
```

> **Rollback** (before agents are reinstalled): `systemctl stop tether-broker`, restore
> `tether.db.bak` over `tether.db` (and `rm tether.db-wal tether.db-shm`), restore
> `nats.conf.bak.<ts>`, remove `broker.cluster.*` from broker.yaml, `systemctl start`. This
> returns to the **v2 single broker** (cluster-mode OFF) — NOT to a v1 fleet.
>
> **HA guarantee** (§17): N=1 has no redundancy; N=2 is read-survives / write-zero-fault;
> only N>=3 with JS replicas at target gives committed-0-loss HA. `cluster status` shows
> `stream-replicas actual/target` + raises `replication_degraded` until they converge.

## 5. Backup & disaster recovery (`cluster backup` / `cluster restore`)

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
re-bootstraps a **single voter** — you then re-grow with `cluster add`.

```bash
# 0. On the target host, place the SAME node's secrets dir (the live tunnel cert is the
#    un-forgeable provenance anchor — a restore onto a different node's secrets is REFUSED).
# 1. Stop the daemon.
systemctl stop tether-broker
# 2. Restore. You TYPE the node_id to confirm (no --yes, never a machine escape — restore is
#    irreversible + identity-affecting). The bundle's identity must match this host's secrets.
#    Restore is RE-RUNNABLE after a kill-9: it marks restore_in_progress and the daemon REFUSES to
#    start until restore completes — do NOT start the daemon mid-restore; just re-run the line.
tether cluster restore /var/backups/tether-2026-06-24 --confirm-node-id brk-a \
    --secrets-dir /etc/tether/secrets
# 3. Start the daemon. It comes up as a single-voter cluster (NO HA until you re-grow).
systemctl start tether-broker
tether cluster status            # exit 1 DEGRADED (N=1, no redundancy) until re-grown; roster = {self}
                                 # (NOT exit 3 — restore is not force-single; it clears that marker)
# 4. Re-grow to N>=3 with `cluster add` (§1), re-rendering nats.conf on every node.
```

The restore **resets the applied cursor to 0** and **prunes the old peers from the roster** so
the restored node is a clean single-voter origin (the original membership is preserved in the
bundle's `manifest.json` for the incident record). Provenance is gated on the **live tunnel-cert
fingerprint** (== the manifest's `self_cert_fp` == the bundle's self-row `cert_fp`) + the typed
`--confirm-node-id` — a foreign or torn/edited bundle is refused before any disk mutation.

### 5.2 Full-cluster disaster recovery (all nodes lost)

```bash
# 1. On a FRESH box, restore this node's secrets dir from your secret store.
# 2. cluster restore the latest bundle (§5.1) with --confirm-node-id <the original node_id>.
# 3. Start the daemon (single voter N=1), then `cluster add` new nodes to re-grow to N>=3.
# 4. Agents reconnect + re-pin; exposes re-home onto the live broker automatically (D6).
```

### 5.3 Identity-only manifest replay (recover → re-init)

`cluster recover` (§3) can capture a node's IDENTITY into a manifest before it wipes, so the
re-init does not re-type the 9 identity flags:

```bash
# On the returning node (daemon stopped): dump forensics AND emit an identity manifest.
tether cluster recover --self-id brk-b --dump-divergent /root/divergent-brk-b.json \
    --emit-manifest /root/brk-b-ident.json --secrets-dir /etc/tether/secrets
# Re-init from the manifest (cert_fp is re-derived LIVE from this host's secrets, not replayed —
# a rotated cert still pins agents correctly). The manifest is identity-only: NO business rows.
tether cluster init --from-manifest /root/brk-b-ident.json --secrets-dir /etc/tether/secrets
# Then `cluster add` on the leader to rejoin (§1).
```

## 6. Rolling upgrade (followers-first, leader-last)

A rolling upgrade replaces the binary on each broker one at a time WITHOUT downtime, as long as
the new and old binaries speak the SAME proto version. A **proto bump (v2→v3) is a flag-day, NOT
a rolling upgrade** (the wire is incompatible — stop the whole fleet, upgrade, restart).

```bash
# 0. Confirm the target release is the SAME proto. `cluster status` shows each node's running
#    VER (a live self-report). `cluster add` HARD-REJECTS a joiner with a different proto.
tether cluster status                    # note the leader + every node's VER

# 1. Upgrade FOLLOWERS first, one at a time (the leader keeps serving):
#    on each follower host —
systemctl stop tether-broker
#    (swap the binary)
systemctl start tether-broker
tether cluster wait <node-id> --phase VOTER     # block until it is a full voter again
tether cluster status                            # confirm its VER updated + REACH ok before the next

# 2. Upgrade the LEADER last. Hand off leadership FIRST so you never re-elect mid-rollout:
tether cluster transfer-leader <an-already-upgraded-follower> --wait
#    then upgrade the (now ex-leader) host as in step 1.

# 3. Verify: every node shows the new VER, REACH ok, no INCONSISTENT, health HEALTHY_HA.
tether cluster status
```

Notes:
- A mixed-release window (some nodes new, some old) is generally safe as long as the proto is
  unchanged — `cluster add` only WARNS on a release skew (it rejects only a proto mismatch), and a
  re-joining drained node may legitimately be older than the now-upgraded leader during a rollback.
  **CAVEAT (DB schema):** if the NEW release adds a DB migration, an upgraded node forward-migrates
  its DB and you CANNOT then roll that node back to the older binary (migrations are forward-only;
  the old binary has no downgrade path). Upgrade is one-way per node once it has migrated — keep the
  `tether.db` backup (`cluster backup`) from BEFORE the upgrade for a true rollback. Same-proto
  releases that add NO migration are freely roll-back-able.
- Watch `cluster status` STREAMS (actual/target) between steps: a node restart transiently drops a
  JS replica; wait for `replication_degraded` to clear (actual==target) before upgrading the next.
