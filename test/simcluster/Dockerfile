# tether-sim base image — one image, role decided at runtime (broker / agent / ctl).
# Real systemd (PID1) + real sshd + real out-of-process nats-server + the real tether binary,
# so the sim exercises the cross-process / on-disk / nats.conf-drift / install-path bug class
# the hermetic Go suites cannot reach. Build context = test/simcluster/. Binaries + install.sh
# are rsync'd into vendor/ from the WSL dev box (server has no Go); see remote.sh.
#
# Confirmed run flags (server PID1 smoke, 2026-07-04): --privileged --cgroupns=host
#   -v /sys/fs/cgroup:/sys/fs/cgroup:rw --tmpfs /run --tmpfs /run/lock --stop-signal SIGRTMIN+3
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# systemd (PID1) + sshd (real `ssh sim-<node>` debug) + the deps the control scripts/drills need.
RUN apt-get update && apt-get install -y --no-install-recommends \
        systemd systemd-sysv dbus \
        openssh-server \
        python3 \
        jq openssl iproute2 curl ca-certificates procps iputils-ping less vim-tiny \
        fuse3 libfuse2t64 python3-fusepy \
        shadowsocks-libev python3-yaml sqlite3 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
# shadowsocks-libev = `ss-local` client for drill 72's real Shadowsocks egress legs (OQ-1); the SS
# server is tether's proxy exit, stock ss-local interops (chacha20-ietf-poly1305 + evpBytesToKey MD5 +
# HKDF-SHA1 "ss-subkey"). python3-yaml parses the /sub body (Clash YAML, application/yaml — NOT SIP008
# JSON, so jq won't do). Both are ubuntu universe; if `ss-local` is unavailable at build the OQ-1
# fallback ladder (AEAD-gate + TCP-reachability) applies (s3-s5-plan §7 / §11-U3).

# Mask the units that fight docker PID1 / DNS (re-smoke via `simcluster doctor` if this set changes).
RUN systemctl mask systemd-resolved.service systemd-networkd.service systemd-networkd.socket \
        systemd-firstboot.service getty.target console-getty.service \
    && rm -f /etc/machine-id /var/lib/dbus/machine-id

# sshd: password login as sim for `simcluster ssh <node>` (dev tool on a trusted bridge; docker exec
# `shell` is the robust primary debug path and needs none of this).
RUN mkdir -p /run/sshd \
    && useradd -m -s /bin/bash sim 2>/dev/null || true \
    && echo 'sim:sim' | chpasswd \
    && sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config

# Baked binaries (rsync'd into vendor/ from WSL): production-pinned nats-server v2.10.22 (NOT go.mod's
# embedded v2.14.0 — running what the fleet runs is the tool's justification), the tether binary, and
# the nats/nk CLIs. install.sh is the REAL production installer (reused, never forked).
COPY vendor/tether       /usr/local/bin/tether
COPY vendor/nats-server  /usr/local/bin/nats-server
COPY vendor/nats         /usr/local/bin/nats
COPY vendor/nk           /usr/local/bin/nk
COPY vendor/install.sh   /opt/sim/install.sh
RUN chmod +x /usr/local/bin/tether /usr/local/bin/nats-server /usr/local/bin/nats /usr/local/bin/nk /opt/sim/install.sh

# Provisioning + typed-confirm/interactive-pty helpers (run inside the container via docker exec).
COPY image/provision-node.sh          /opt/sim/provision-node.sh
COPY image/pty-confirm.py             /opt/sim/pty-confirm.py
COPY image/pty-run.py                 /opt/sim/pty-run.py
COPY image/hangfs.py                  /opt/sim/hangfs.py
COPY image/ingress-proxy.py           /opt/sim/ingress-proxy.py
COPY image/units/tether-agent.service /opt/sim/tether-agent.service
RUN chmod +x /opt/sim/provision-node.sh /opt/sim/pty-confirm.py /opt/sim/pty-run.py /opt/sim/hangfs.py /opt/sim/ingress-proxy.py

# STOPSIGNAL SIGRTMIN+3 is mandatory: default SIGTERM 10s-kills PID1 mid-write and corrupts
# SQLite/BoltDB, masking real bugs as sim artifacts. Pair with --stop-signal at run time.
STOPSIGNAL SIGRTMIN+3

# STATELESS image: no VOLUME directive. All mutable state lives only under the two named-volume
# mounts (/etc/tether, /var/lib/tether) supplied at run time.
CMD ["/lib/systemd/systemd"]
