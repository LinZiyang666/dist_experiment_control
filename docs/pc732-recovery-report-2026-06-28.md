# pc732 Recovery Report - 2026-06-28

## Summary

pc732 (`pc732.emulab.net`, user `lzy666`) was reachable at the host level, but the v2 broker was down. The immediate failure was `tether-broker.service`, not disk, memory, NATS reachability, or Caddy.

The broker has been restored. Current service state after the fix:

- `nats-server`: active
- `tether-broker`: active
- `caddy`: active
- `tether-agent`: active
- admin socket: `/var/run/tether/admin.sock` present and responding
- `cluster doctor`: 9 pass, 1 advisory, 0 fatal
- `cluster status`: `DEGRADED`, exit 1, expected for a single-voter N=1 broker with no HA

## Impact

While the broker was down:

- v2 agents could not use the broker normally.
- `/var/run/tether/admin.sock` did not exist.
- `tether cluster status` and admin operations could not use the live broker path.

After recovery, v2 agents re-registered successfully. Observed online nodes included `pc732`, `racknerd`, `timan1`, `timan107`, `timan108`, and `weiland-optiplex-7050`.

## Root Cause

`tether-broker.service` was failing with exit status 70. The broker log showed:

```text
broker: cluster mode requires JetStream; enable JetStream before starting HA broker
```

NATS itself was running, and `/etc/tether/nats.conf` contained `jetstream { ... }`, but the NATS JetStream metadata still reflected a clustered JetStream state. The monitoring endpoint showed a JetStream `meta_cluster` with `cluster_size: 2`.

At the same time, the offline cluster roster showed only one voter:

```text
NODE_ID  NAME   PHASE  RAFT_PING
pc732    pc732  VOTER  DOWN
```

This is the unsupported shape described in the v2 cluster runbook: a lone N=1 broker cannot keep running clustered JetStream, because a single node cannot satisfy the clustered JetStream meta quorum. N=1 must run standalone JetStream.

## Fix Applied

The repair path was to de-cluster NATS back to the supported N=1 standalone JetStream shape.

Actions taken:

1. Confirmed host health:
   - root filesystem: 27% used
   - memory: healthy
   - load: low

2. Confirmed actual deployed version:

   ```text
   tether 0.4.4 (proto v2)
   linux/amd64
   go1.25.0
   ```

3. Confirmed live failure:
   - `nats-server`: active
   - `tether-broker`: failed
   - `caddy`: active
   - `tether-agent`: active

4. Confirmed offline roster had exactly one voter: `pc732`.

5. Backed up NATS config to:

   ```text
   /var/lib/tether/recovery-bak.20260628-054302/
   ```

   Files saved:

   ```text
   nats.conf.before
   nats.conf.after
   ```

6. Edited `/etc/tether/nats.conf` to remove the `cluster { ... }` block while preserving:
   - `server_name: "pc732"`
   - `listen: "127.0.0.1:4222"`
   - `http: "127.0.0.1:8223"`
   - `jetstream { store_dir: "/var/lib/tether/jetstream" }`
   - `authorization`
   - `websocket`

7. Validated the modified config:

   ```text
   nats-server: configuration file /etc/tether/nats.conf is valid
   ```

8. Restarted services:
   - stopped `tether-broker`
   - restarted `nats-server`
   - reset failed state for `tether-broker`
   - started `tether-broker`

## Important Note On JetStream Store

The strict runbook path for clustered-to-standalone shrink includes a JetStream store reset because clustered JS state does not migrate cleanly to standalone JS in all cases.

During this recovery, the current `/var/lib/tether/jetstream` store was not removed. After the `cluster {}` block was removed and NATS was restarted, JetStream came up in standalone mode and the broker accepted it.

Post-fix monitoring showed:

- no `meta_cluster` in `/jsz`
- `ha_assets: 0`
- streams present: `events`, `history-lab`
- broker log: `broker: JetStream enabled`

So the immediate outage was fixed without dropping current JetStream audit/history data.

## Validation

Service checks after the fix:

```text
nats-server active
tether-broker active
caddy active
tether-agent active
```

Key listeners after the fix:

```text
127.0.0.1:4222   nats-server
127.0.0.1:8222   nats-server websocket internal
127.0.0.1:8223   nats-server monitoring
127.0.0.1:8090   tether subscription HTTP
*:7400           tether raft
*:17000          tether tunnel
*:443            caddy
```

Broker log after the fix:

```text
broker: JetStream enabled
broker: admin socket ready path=/var/run/tether/admin.sock
broker: ready nats=nats://127.0.0.1:4222
```

Cluster status after the fix:

```text
health: DEGRADED
exit_code: 1
leader_id: pc732
health_label: NOT-HA
```

This is expected for N=1 and means the broker is running but has no redundancy.

Cluster doctor after the fix:

```text
doctor: 9 pass, 1 advisory, 0 fatal
```

External checks:

```text
TCP 17000: open
HTTPS 443: reachable through Caddy, returned HTTP 400 for root path
```

## Remaining Risk

The broker is currently healthy but not highly available:

- N=1 tolerates 0 broker failures.
- `cluster status` will continue to report `DEGRADED` until another broker is added.
- The next operational step for HA is to grow the cluster to N>=3.

Before the next grow attempt, verify that pc732 remains in the supported N=1 shape:

```text
/etc/tether/nats.conf has jetstream{}
/etc/tether/nats.conf has no cluster{}
tether cluster doctor has no fatal checks
tether cluster status reports pc732 as leader, VOTER, stream_actual=1, stream_target=1
```
