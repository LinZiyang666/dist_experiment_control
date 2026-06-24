# tether cluster — operator runbook (D7)

> Operator-facing procedures for the distributed-broker cluster lifecycle. The
> binding contract is `docs/distributed-broker-architecture.md` §8 / §17; this file
> is the copy-pasteable drill. **All cluster admin commands run on a broker host**
> (admin is strictly local — no network bypass). A non-leader broker fails fast and
> names the leader host to re-run on.

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
               --peer <other1-id>,nats://<other1-host>:6222,<other1-bus-nkey> \
               --peer <other2-id>,nats://<other2-host>:6222,<other2-bus-nkey>
each-broker$ sudo systemctl restart nats-server   # rolling; leader last

# 5. Verify.
leader$ tether cluster status            # the new node walks JOIN_VERIFIED_PENDING_VOTER -> CATCHING_UP -> VOTER
                                         # + every voter shows reachable (nats-health), no applied-lag
```

Half-success is visible, never silently forked: if AddVoter fails the node shows
`VOTER_ADD_FAILED`; if catch-up stalls it stays `CATCHING_UP` with a stall hint.
`cluster doctor` shows the stuck phase + the next command. A new leader runs a
membership reconciliation pass on startup that forward-completes a mid-promote node.

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

### Retire is a topology change, NOT a trust revocation (account.nk / CA rotation)

A retired node's roster + raft membership are removed immediately, but its
`account.nk` and the cluster CA are **shared and NOT rotated** — a retired node that
keeps those files can still mint JWTs / present a route cert until they are rotated.
**If you retire a node because it may be compromised, you MUST rotate** (full-fleet
re-provision):

```
# 1. Retire + power off the suspect node.
leader$ tether cluster drain <node-id> --retire

# 2. Generate a NEW account key + a NEW cluster CA on a trusted host.
trusted$ tether cluster keygen --out account.nk.new          # new account signer
trusted$ # (re-issue a fresh cluster CA + per-node leaf certs with your PKI of choice)

# 3. Distribute the new account.nk + CA to EVERY surviving broker over a trusted
#    channel (scp, never committed; 0600). Update /etc/tether/{account.nk,cluster-ca.pem}.

# 4. Rolling-restart the surviving brokers so they load the new secrets.
#    Old JWTs signed by the old account key expire within their TTL; the new CA
#    rejects the retired node's old route cert immediately.
```

`cluster status` prints "retired node credentials remain cryptographically valid
until rotation" so the operator is never misled. **Retire is not considered safe
against a compromised node until this rotation is done.**

## 3. Quorum loss — the force-single escape hatch (OFFLINE)

If a majority of brokers are permanently dead the cluster goes **read-only**.
`cluster status` reports `QUORUM_LOST` (exit 2). To resume service on a survivor:

```
# 0. CONFIRM the other brokers are TRULY dead (powered off / unreachable to AGENTS),
#    not merely partitioned from you. A merely-partitioned-but-alive peer WILL split-brain.

# 1. STOP the daemon so the offline tool can take the disk (and systemd won't restart it).
survivor$ sudo systemctl mask tether && sudo systemctl stop tether

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
survivor$ sudo systemctl unmask tether && sudo systemctl start tether
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
returning$ sudo systemctl mask tether && sudo systemctl stop tether

# Dump this node's divergent rows for forensics, THEN wipe raft/ + tether.db.
# The dump is fsync'd (0600, never overwrites a prior dump) BEFORE any wipe; if the
# dump fails, the wipe is refused. You must pass --self-id and TYPE the node_id to
# confirm (no --yes) — this proves you are wiping the intended node.
returning$ sudo tether cluster recover --self-id <returning-node-id> --dump-divergent /root/divergent-$(hostname).json

# Then rejoin it as a clean node (section 1).
returning$ sudo systemctl unmask tether && sudo systemctl start tether
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
broker$ sudo tether cluster takeover-natsconf --secrets-dir /etc/tether/secrets

# 4. Restart nats-server so the new authorization{} (cluster.apply.* ACL) is live BEFORE the
#    broker connects in cluster mode (else it fails closed: no ACL).
broker$ sudo systemctl restart nats-server
broker$ tether cluster status   # (offline ok) confirm the seeded single-voter roster

# 5. Point the broker at the cluster + start it (now a single-voter cluster, N=1).
#    In /etc/tether/broker.yaml under broker.cluster: data_dir / raft_addr / secrets_dir.
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
