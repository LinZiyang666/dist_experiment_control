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
    # G1 #22: install.sh now writes the reconciler's nats.conf into the tether-owned
    # /etc/tether/nats.d/ subdir (NOT root-owned /etc/tether). Follow the product path.
    if [ -f "$ETC/nats.d/nats.conf" ]; then
        # standalone install.sh nats.conf: set/insert a 0.0.0.0 host for the client listener.
        if grep -qE '^\s*host:' "$ETC/nats.d/nats.conf"; then
            sed -i -E 's/^\s*host:.*/host: 0.0.0.0/' "$ETC/nats.d/nats.conf"
        fi
    fi
    # (b) secrets dir (§15; keys land here, chowned tether:tether 0600 by the control script). The
    #     broker.yaml cluster seam is written by cmd_init/cmd_grow (NOT here — a blind append would add a
    #     duplicate top-level `broker:` key + omit nats_server_bin).
    mkdir -p "$ETC/secrets"
    chmod 700 "$ETC/secrets"
    chown -R tether:tether "$ETC/secrets" 2>/dev/null || true
    # #22 (G1 fix landed, Option B): DELIBERATELY do NOT chown anything under /etc/tether by hand — the
    #     fix is the PRODUCT's job. install.sh now `install -d -o tether`s a dedicated /etc/tether/nats.d/
    #     subdir and writes the reconciler's nats.conf there, while /etc/tether ITSELF stays root-owned (a
    #     root-owned /etc/tether is CORRECT under Option B — the root-run caddy reads /etc/tether/Caddyfile,
    #     so a tether-owned /etc/tether would be a tether->root privesc). Post-fix invariants the sim asserts:
    #     /etc/tether root-owned (unchanged) AND /etc/tether/nats.d tether-owned (so the User=tether in-broker
    #     reconciler can now atomically rewrite its nats.conf). The sim adds NO chown: nats.d's tether
    #     ownership comes from the REAL install.sh (never a sim override — Mandate ①). The secrets/ subdir is
    #     tether-owned above. `doctor` asserts /etc/tether stays root-owned AND nats.d is tether-owned; a
    #     root-owned/missing nats.d = an un-upgraded / pre-#22-fix host. drills/13 exercises the User=tether
    #     write into nats.d/. (History: an earlier build baked a provision that chowned /etc/tether → masked
    #     #22; removed. The pre-Option-B idea of chowning /etc/tether itself was rejected for the Caddyfile
    #     privesc — verify ownership with `doctor` after any image rebuild.)
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
