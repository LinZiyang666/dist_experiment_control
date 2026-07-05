#!/bin/sh
# provision-node.sh — lay down a node's on-disk tree ONCE, inside the container, from the REAL
# install.sh (never a fork). Driven by the control script via `docker exec` after the container
# boots; sentinel-guarded so it runs exactly once per persistent /etc/tether volume (load-bearing
# for #20: a tether-managed nats.conf must persist across restarts and change only on membership).
#
# Usage: provision-node.sh <role> <node_id> <bridge_ip>
#   role ∈ broker | agent | ctl
# Starts NOTHING (matches install.sh + tether's "never runs systemctl" discipline). The control
# script enables/starts the units after secrets are distributed.
set -eu

ROLE="${1:?role}"; NODE="${2:?node_id}"; IP="${3:?bridge_ip}"
SENTINEL=/etc/tether/.sim-provisioned
ETC=/etc/tether

if [ -f "$SENTINEL" ]; then
    echo "provision: already provisioned ($(cat "$SENTINEL")); skipping"
    exit 0
fi

case "$ROLE" in
broker)
    # REUSE the production installer headlessly: --skip-download early-returns the nats-server/caddy
    # fetch and starts nothing; dummy --domain/--acme-email satisfy its hard-die. The tether +
    # nats-server binaries are already baked at /usr/local/bin (install.sh --skip-download uses them).
    sh /opt/sim/install.sh --role broker --skip-download --prefix /usr/local/bin \
        --domain "sim-${NODE}.tether.test" --acme-email dev@sim.tether.test

    # --- thin overlay (§2), the ONLY sim-specific deltas over the real tree ---
    # (a) client bind 0.0.0.0 so cross-container clients reach :4222 (install.sh defaults to loopback).
    if [ -f "$ETC/nats.conf" ]; then
        # standalone install.sh nats.conf: set/insert a 0.0.0.0 host for the client listener.
        if grep -qE '^\s*host:' "$ETC/nats.conf"; then
            sed -i -E 's/^\s*host:.*/host: 0.0.0.0/' "$ETC/nats.conf"
        fi
    fi
    # (b) secrets dir (§15; keys land here, chowned tether:tether 0600 by the control script). The
    #     broker.yaml cluster seam is written by cmd_init/cmd_grow (NOT here — a blind append would add a
    #     duplicate top-level `broker:` key + omit nats_server_bin).
    mkdir -p "$ETC/secrets"
    chmod 700 "$ETC/secrets"
    chown -R tether:tether "$ETC/secrets" 2>/dev/null || true
    # M6/#22: DELIBERATELY leave /etc/tether ROOT-owned (exactly as install.sh does — it chowns only
    #     LIB/LOG/RUN, never ETC). That IS gotcha #22: the in-broker C3 reconciler (User=tether) then
    #     perm-denies its atomic temp write and topology never auto-converges. The sim must NOT chown it
    #     (that would MASK #22, the very defect this tool exists to catch). Grow still works because the
    #     MANUAL `reconcile nats` runs as root (the operator path); only the automatic reconcile is broken,
    #     exactly as on the fleet. `doctor` flags a root-owned /etc/tether as the reproduced-#22 tripwire.
    ;;
agent)
    # Agents onboard via the real product path: `tether agent join <invite>` binds the nkey, then a
    # sim-owned system unit runs the daemon. install.sh --role agent is broker-URL-centric (die at
    # :298) and writes a systemctl --user unit; a system unit is the clean fit under PID1 systemd.
    id -u sim >/dev/null 2>&1 || useradd -m -s /bin/bash sim
    install -o sim -g sim -m 0755 -d /home/sim/.tether
    if [ -f /opt/sim/tether-agent.service ]; then
        cp /opt/sim/tether-agent.service /etc/systemd/system/tether-agent.service
        systemctl daemon-reload
    fi
    ;;
ctl)
    # ctl only needs the binary (baked) + a home to persist `tether login` session creds.
    id -u sim >/dev/null 2>&1 || useradd -m -s /bin/bash sim
    install -o sim -g sim -m 0755 -d /home/sim/.tether
    ;;
*)
    echo "provision: unknown role '$ROLE'" >&2; exit 2 ;;
esac

echo "role=$ROLE node=$NODE ip=$IP at=$(date -u +%FT%TZ)" > "$SENTINEL"
echo "provision: done ($ROLE $NODE)"
