# drills/lib/ident.sh — multi-identity ctl + bare-nats-impersonation + session-agnostic agent-bind fixtures.
# Sourced by drills 80/82. Depends on lib/docker.sh + lib/log.sh (poll_until) + the drill's $SIM.
#
# WHY THIS EXISTS (roadmap S2 §2.3-2.5): tether's ctl identity root is $TETHER_HOME (DefaultHome reads it,
# internal/cli/identity.go:137-142) — a full identity dir (nkey keys/default.nk + current_session +
# broker_url). Distinct $TETHER_HOME ⇒ distinct nkey (distinct fp) ⇒ a real, separate tenant identity, all
# from ONE ctl container. That is exactly how a real team runs many operators; the sim only SUPPLIES the
# per-identity home (Mandate ④), zero product-behaviour change. `nats_as` impersonates a ctl over the bare
# vendored `nats` CLI (nkey + connection-name=tether-cli:<sid>) so auth_callout issues it the <sid> JWT —
# the ONLY way to probe the protocol-layer subject ACL a real member JWT carries (a per-verb app string is
# spoofable; the JWT deny is the real boundary). Verified live on the server (spike#4 2026-07-11): the deny
# is `nats: permissions violation: Permissions Violation for Publish to "<subject>"` (pub) / `... for
# Subscription to "<subject>"` (sub). The nkey file keys/default.nk is a `SU…` user seed the nats CLI reads.
#
# `bind_agent` binds a real agent nkey via the real `--pin` first-connect + a persistent unit (as
# cmd_agent_join does) but polls ONLINE via the BROKER's own admin socket (session-agnostic ground truth),
# because cmd_agent_join's own ONLINE self-check reads the default-home ctl's `node ls`, which a multi-home
# drill has NOT logged into the tenant session (simcluster:372-376). Mandate ④: real nkey, real first
# connect, observed through the broker's authoritative admin socket — not filling a tether gap.

: "${NATS_BROKER:=brk1}"   # the broker the ctl container dials for nats_as (N=1 default)

# CTLH <tag> <tether args...> : run tether from ctl1 as user sim with TETHER_HOME=/home/sim/.tether-<tag>.
CTLH() {
    _ch_tag=$1; shift
    "$SIM" exec ctl1 -- runuser -u sim -- env HOME=/home/sim "TETHER_HOME=/home/sim/.tether-$_ch_tag" tether "$@"
}

# CTLHS <tag> <TETHER_SESSION> <tether args...> : CTLH + a per-shell TETHER_SESSION override (identity.go:153
# honours $TETHER_SESSION before the current_session file).
CTLHS() {
    _chs_tag=$1; _chs_sess=$2; shift 2
    "$SIM" exec ctl1 -- runuser -u sim -- env HOME=/home/sim \
        "TETHER_HOME=/home/sim/.tether-$_chs_tag" "TETHER_SESSION=$_chs_sess" tether "$@"
}

# nats_as <tag> <sid> <nats subcommand + args...> : run the vendored `nats` CLI from ctl1 as user sim,
# impersonating identity <tag> activated on session <sid>. auth_callout reads ConnectOptions.Nkey (the
# identity pub) + Name (tether-cli:<sid> → roleCtlActivated, handler.go:197-206) and, for an already-member,
# issues PermissionsForActivatedMember(actor,sid). Cross-session pub/sub then hits "Permissions Violation".
nats_as() {
    _na_tag=$1; _na_sid=$2; shift 2
    "$SIM" exec ctl1 -- runuser -u sim -- env HOME=/home/sim nats \
        --server "nats://$NATS_BROKER:4222" --no-context \
        --nkey "/home/sim/.tether-$_na_tag/keys/default.nk" \
        --connection-name "tether-cli:$_na_sid" "$@"
}

# nats_as_t <timeout_s> <tag> <sid> <nats args…> : nats_as wrapped in a CONTAINER-SIDE `timeout`. For an
# ALLOWED `sub` with no matching message, `nats sub` blocks indefinitely (its --timeout is the connect
# timeout, not a subscribe-duration bound), so a positive-control sub MUST be time-bounded or the drill hangs.
nats_as_t() {
    _nt_to=$1; _nt_tag=$2; _nt_sid=$3; shift 3
    "$SIM" exec ctl1 -- runuser -u sim -- env HOME=/home/sim timeout "$_nt_to" nats \
        --server "nats://$NATS_BROKER:4222" --no-context \
        --nkey "/home/sim/.tether-$_nt_tag/keys/default.nk" \
        --connection-name "tether-cli:$_nt_sid" "$@"
}

# _agent_online_admin <agt> : predicate — <agt> is ONLINE per the BROKER's own admin socket (session-
# agnostic; no ctl session needed). 60-drill:31 idiom, run as the broker's euid (tether).
_agent_online_admin() {
    # `tether admin nodes` prints to STDOUT (verified via fd separation; tether is correct). Its table is
    # `SESSION NODE STATE …` (SESSION first), so match <nid> + ONLINE without a `^` anchor.
    "$SIM" exec "$NATS_BROKER" -- runuser -u tether -- tether admin nodes 2>&1 \
        | grep -E "$1[[:space:]]" | grep -q ONLINE
}

# bind_agent <agt> <sid> <pin> [broker] : bind the agent nkey (real --pin first-connect) + a persistent
# flagless-ish unit, then poll ONLINE via the broker admin socket. Session-home-agnostic (unlike cmd_agent_join).
bind_agent() {
    _ba_agt=$1; _ba_sid=$2; _ba_pin=$3; _ba_brk=${4:-$NATS_BROKER}
    _ba_url="nats://$_ba_brk:4222"
    log "bind_agent: $_ba_agt → session $_ba_sid (broker=$_ba_url)"
    # first --pin connect binds the agent nkey as a session member + persists it to /home/sim/.tether.
    dexec -u sim "$_ba_agt" -- sh -c "HOME=/home/sim timeout 6 tether agent --session '$_ba_sid' --nid '$_ba_agt' --pin '$_ba_pin' --nats-url '$_ba_url' >/tmp/agent-bind.log 2>&1 || true"
    # persistent unit (reconnects as the now-bound member; no PIN in argv).
    dexec "$_ba_agt" -- sh -c "printf 'SID=%s\nNID=%s\nNATS_URL=%s\n' '$_ba_sid' '$_ba_agt' '$_ba_url' > /etc/tether/agent.env
cat > /etc/systemd/system/tether-agent.service <<UNIT
[Unit]
Description=tether agent (sim, bind_agent)
After=network-online.target
[Service]
Type=simple
User=sim
Environment=HOME=/home/sim
EnvironmentFile=/etc/tether/agent.env
ExecStart=/home/sim/.local/bin/tether agent --session \\\${SID} --nid \\\${NID} --nats-url \\\${NATS_URL}
Restart=on-failure
RestartSec=2
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload"
    sctl "$_ba_agt" enable --now tether-agent
    if poll_until 40 3 "$_ba_agt ONLINE in broker admin roster" -- _agent_online_admin "$_ba_agt"; then
        ok "bind_agent: $_ba_agt onboarded + ONLINE (broker-admin view)"
    else
        die "bind_agent: $_ba_agt did not reach ONLINE (check: $SIM logs $_ba_agt tether-agent)"
    fi
}
