# tether

A NAT-traversal control plane for SSH-style access and port exposure. NAT-bound agents
reverse-connect to a public broker over NATS; operators route commands to those agents
through the same bus. One Go binary, three roles (broker / agent / ctl), selected by
subcommand.

```
# install (broker host needs a domain + sudo; agent host just needs the binary)
curl -fsSL https://github.com/LinZiyang666/dist_experiment_control/releases/latest/download/install.sh | sh -s -- --role agent

# expose a local port through the broker
tether expose --name web --local-port 3000
```

- **Usage (`ctl` / `agent` — connect, run commands, transfer files, troubleshoot):** [`docs/usage.md`](docs/usage.md)
- **Broker operations (deploy / configure / maintain a public broker):** [`docs/broker-ops.md`](docs/broker-ops.md)
- **Cluster HA (`cluster` / `alert` commands + quorum concepts):** [`docs/cluster.md`](docs/cluster.md)
- **Architecture (single-broker baseline):** [`docs/architecture.md`](docs/architecture.md)
- **Distributed-broker HA (proto v2):** [`docs/distributed-broker-architecture.md`](docs/distributed-broker-architecture.md)
  and the operator runbook [`docs/cluster-runbook.md`](docs/cluster-runbook.md)

Built with `CGO_ENABLED=0` (static binary), Go 1.25. `make build` / `make test` /
`make e2e` / `make lint`.

> **Release lines:** the `main` branch is on **proto v2** (the distributed-broker HA epic)
> and is **not** wire-compatible with the deployed proto-v1 fleet. v1 patches branch from
> the latest v1 tag; deploying HA requires a coordinated v2 reinstall (broker + all agents).
