# tether-sim base image — one image, role decided at runtime (broker / agent / ctl).
# Real systemd (PID1) + real sshd + real out-of-process nats-server + the real tether binary,
# so the sim exercises the cross-process / on-disk / nats.conf-drift / install-path bug class
# the hermetic Go suites cannot reach. Build context = test/simcluster/. Binaries + install.sh
# live in vendor/ (staged by ./simcluster build on the server, or rsync'd in by remote.sh).
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
        jq openssl iproute2 iptables nftables conntrack libcap2-bin util-linux \
        curl ca-certificates procps iputils-ping less vim-tiny \
        fuse3 libfuse2t64 python3-fusepy \
        shadowsocks-libev python3-yaml sqlite3 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
# iptables (G-C / S0-fault-primitives, s7-s9-plan §1.2-5): the netfilter instrument drills 96/97 use to
# inject a SILENT network partition (packets DROPped, not RST) inside the target's own netns.
# MANDATE (3)/(4): supplying a machine with standard kernel tooling is PROVISIONING (the sim's job), exactly
# like docker/systemd/--privileged — a real Ubuntu server has iptables. It works around NO tether defect:
# no rule exists outside an explicit injection window, every rule is removed by an unconditional trap, and
# tether never reads netfilter. A silent partition is HARDER on tether than `docker network disconnect`
# (which only detaches the interface -> instant EHOSTUNREACH), never easier. Verified on the sim server
# 2026-07-17 (SB-FAULT): iptables v1.8.10 (nf_tables) works in --privileged containers; a --dport/--sport
# DROP pair makes the peer port hang (rc=124) while an unfiltered port on the SAME peer still connects.
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

# Baked binaries (from vendor/): the production-pinned nats-server, the tether binary, and
# the nats/nk CLIs. install.sh is the REAL production installer (reused, never forked).
#
# NO VERSION LITERAL HERE ON PURPOSE. lib/stage.sh greps NATS_SERVER_VERSION out of
# scripts/install.sh, so this image always bakes what the fleet installs; naming a version
# in this comment is how the two silently diverge, and this comment DID name a stale one
# through the whole drift that test/architecture/nats_server_pin_test.go now guards.
# Running what the fleet runs is this tool's entire justification.
COPY vendor/tether       /usr/local/bin/tether
COPY vendor/nats-server  /usr/local/bin/nats-server
COPY vendor/caddy        /usr/local/bin/caddy
COPY vendor/caddy-source.json /opt/sim/caddy-source.json
COPY vendor/nats         /usr/local/bin/nats
COPY vendor/nk           /usr/local/bin/nk
COPY vendor/install.sh   /opt/sim/install.sh
COPY lib/caddy-artifact.py /opt/sim/caddy-artifact.py
COPY vendor/nats-server-prev /opt/sim/nats-server-prev
COPY vendor/nats-server-next /opt/sim/nats-server-next
COPY vendor/caddy-prev /opt/sim/caddy-prev
COPY vendor/caddy-next /opt/sim/caddy-next
COPY vendor/tether-next /opt/sim/tether-next
COPY vendor/tether-prev /opt/sim/tether-prev
COPY vendor/tether-dynamic-prev /opt/sim/tether-dynamic-prev
COPY vendor/tether-flow-legacy /opt/sim/tether-flow-legacy
COPY vendor/tether-source.json /opt/sim/tether-source.json
COPY vendor/tether-prev-source.json /opt/sim/tether-prev-source.json
COPY vendor/tether-dynamic-prev-source.json /opt/sim/tether-dynamic-prev-source.json
COPY vendor/tether-flow-legacy-source.json /opt/sim/tether-flow-legacy-source.json
# origin: hot-update S9 plan §6.5 / §9.4 / T55/T56. These are files only:
# the product's explicit bootstrap activates management on a live stock host.
COPY vendor/tether-infra-helper /usr/local/libexec/tether-infra-helper
COPY vendor/tether-infra-launch /usr/local/libexec/tether-infra-launch
COPY vendor/infra/ /opt/sim/infra/
COPY lib/infra-artifact.py /opt/sim/infra-artifact.py
COPY lib/infra-image-manifest.py /opt/sim/infra-image-manifest.py
COPY Dockerfile /opt/sim/Dockerfile
COPY vendor/infra-image-inputs.json /opt/sim/infra-image-inputs.json
ARG TETHER_CADDY_VERSION
# Bind the image's exact COPY results, including the installer pin, before success.
RUN python3 /opt/sim/caddy-artifact.py verify /usr/local/bin/caddy /opt/sim/caddy-source.json /opt/sim/install.sh
RUN python3 /opt/sim/infra-artifact.py verify /opt/sim /opt/sim/install.sh \
        --binary-root /usr/local/bin --support-root /usr/local/libexec

# Provisioning + typed-confirm/interactive-pty helpers (run inside the container via docker exec).
COPY image/provision-node.sh          /opt/sim/provision-node.sh
COPY image/pty-confirm.py             /opt/sim/pty-confirm.py
COPY image/pty-run.py                 /opt/sim/pty-run.py
COPY image/hangfs.py                  /opt/sim/hangfs.py
COPY image/ingress-proxy.py           /opt/sim/ingress-proxy.py
COPY image/units/tether-agent.service /opt/sim/tether-agent.service
# Do not rewrite COPY modes after recording them. The complete source/target
# manifest verifies actual content and metadata; its own bytes are excluded.
RUN python3 /opt/sim/infra-image-manifest.py targets /opt/sim/infra-image-inputs.json

# STOPSIGNAL SIGRTMIN+3 is mandatory: default SIGTERM 10s-kills PID1 mid-write and corrupts
# SQLite/BoltDB, masking real bugs as sim artifacts. Pair with --stop-signal at run time.
STOPSIGNAL SIGRTMIN+3

# STATELESS image: no VOLUME directive. All mutable state lives only under the two named-volume
# mounts (/etc/tether, /var/lib/tether) supplied at run time.
CMD ["/lib/systemd/systemd"]
