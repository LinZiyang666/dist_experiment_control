# drills/lib/fault.sh — S0-fault-primitives (G-C / S9; s7-s9-plan §1.2-(5)).
# POSIX sh. Sourced by drills 96/97 (and 52-A7 for the tunnel redial trigger).
# Depends on lib/docker.sh (dexec/ctr_name) + lib/log.sh (log/warn/err).
#
# MANDATE (3)/(4) — these are INSTRUMENTS (cable-pull equivalents), never accommodations.
# iptables is the containerized equivalent of an operator unplugging a cable or a switch dropping a link.
# Supplying the machine with standard kernel tooling is PROVISIONING (the sim's job), exactly like docker,
# systemd, or --privileged. It works around NO tether defect: no rule exists outside an explicit injection
# window, every rule is removed by an unconditional trap, and tether never reads netfilter.
# MANDATE (4) self-check: a silent partition is HARDER on tether than `docker network disconnect`, not
# easier — it removes tether's ability to notice the failure quickly. If a drill ever needed a rule to make
# a tether command SUCCEED, that is a defect to expose, not a rule to keep.
#
# FOUR DISTINCT FAILURE SEMANTICS — pick deliberately, they are NOT interchangeable:
#   partition (iptables DROP)  packets vanish. Sockets hang. tether learns nothing until its own timeouts
#                              fire. This is a NETWORK PARTITION.
#   crash     (node_kill)      process gone, kernel RSTs. Peers learn INSTANTLY. This is a HOST DEATH.
#   freeze    (SIGSTOP)        socket still open, kernel still ACKs into a filling receive buffer, systemd
#                              Restart= never fires. This is a WEDGED-BUT-ALIVE broker.
#   syn-block (SYN-only DROP)  ESTABLISHED flows keep working; every NEW outbound connection to the port
#                              black-holes. This is a STATEFUL-MIDDLEBOX / NAT-rebind failure — the weak-NAT
#                              shape broker-ops §8.7 documents for upgrade-rollback (drill 33): the old
#                              agent's live registration survives while the re-exec'd new process can
#                              never complete its first register.
#
# THE DROP-vs-REJECT DISCRIMINATOR IS A FIRST-CLASS ASSERTION, NOT A CONVENTION.
#   fault_assert_blackholed -> connect HANGS  -> `timeout` kills it -> rc 124
#   lib/docker.sh tcp_refused -> connect REFUSED -> immediate failure -> rc 1
# They are mutually exclusive, so "we injected a partition, not an outage" is itself something the drill
# PROVES. It also fail-closes the design: if someone ever swaps the partition arm back to
# `docker network disconnect` (whose documented contract only detaches the interface -> instant
# EHOSTUNREACH -> rc 1), fault_assert_blackholed goes RED instead of silently changing the semantics.
#
# CURL UNDER A PARTITION USES A DIFFERENT EXIT CODE THAN UNDER A CRASH.
#   dp_curl_refused (dataplane.sh) asserts curl exit 7  = "failed to connect" = the process is DEAD.
#   dp_curl_blackholed (below)      asserts curl exit 28 = "operation timed out" = the packets are DROPPED.
# Inside an armed DROP window `dp_curl_refused` is WRONG and will fail for the wrong reason. Same family of
# discriminator as the 124-vs-1 rule above.
#
# PORT SEMANTICS (cut exactly what the arm is about — never more):
#   6222 NATS route      -> mesh split                | 96-D (paired with 7400)
#   7400 tether raft     -> raft replication split    | 96-D
#   4222 NATS client     -> client-side cut           | 96-D DELIBERATELY LEAVES THIS UP = the selective
#                                                     |      control source. A broker cut off from
#                                                     |      EVERYTHING obviously cannot serve; that proves
#                                                     |      nothing. Leaving 4222 up is what makes
#                                                     |      "minority is read-only" a real claim.
#   7000 reverse tunnel  -> data plane cut            | 52-A7 redial trigger; 96-F optional
#   8223 NATS monitor    -> NEVER CUT (loopback observation source)
#
# VERIFIED ON THE SIM SERVER 2026-07-17 (SB-FAULT, s7-s9-plan §12): iptables v1.8.10 (nf_tables) works in
# the --privileged containers; a --dport + --sport DROP pair makes the peer's port hang (rc=124) while an
# unfiltered port on the SAME peer still connects (rc=0), and `-F SIMFAULT` restores it immediately.

FAULT_CHAIN=SIMFAULT

# _fault_chain_ensure <node> : create the SIMFAULT chain + hook it into INPUT/OUTPUT, idempotently.
# Everything lives in ONE named chain so cleanup is a single flush and can never miss a stray rule.
_fault_chain_ensure() {
    dexec "$1" -- sh -c "
        iptables -N $FAULT_CHAIN 2>/dev/null || true
        iptables -C OUTPUT -j $FAULT_CHAIN 2>/dev/null || iptables -I OUTPUT -j $FAULT_CHAIN
        iptables -C INPUT  -j $FAULT_CHAIN 2>/dev/null || iptables -I INPUT  -j $FAULT_CHAIN
    " >/dev/null 2>&1
}

# node_ip <node> : the node's bridge IP on this instance's network.
node_ip() {
    d inspect -f "{{(index .NetworkSettings.Networks \"$(net_name)\").IPAddress}}" "$(ctr_name "$1")" 2>/dev/null
}

# fault_partition_on <node> <port> [port...] : silently blackhole <port> on <node>, symmetric and
# peer-agnostic, in <node>'s own netns. Any peer connecting to <node>:<port> HANGS.
#
# Why peer-agnostic (not per-peer-IP): the semantics are "isolate THIS node's port from everyone", so one
# INPUT --dport rule + one OUTPUT --sport rule per port is correct and does not need a live listener to
# take effect. (An earlier per-peer-IP form was wrong — measured 2026-07-17.)
# Why both --dport and --sport: without --sport, replies on an already-ESTABLISHED connection still leave
# (their sport is <port>), so route/raft would keep limping — a weaker, fake injection.
fault_partition_on() {
    _fp_node=$1; shift
    [ "$#" -ge 1 ] || { err "fault_partition_on <node> <port>... — no port given"; return 1; }
    _fault_chain_ensure "$_fp_node" || { err "fault_partition_on: could not create $FAULT_CHAIN on $_fp_node"; return 1; }
    for _fp_port in "$@"; do
        # Peer-agnostic + symmetric, in <node>'s own netns. IMPORTANT — the SIMFAULT chain is jumped from
        # BOTH the INPUT and the OUTPUT hook (see _fault_chain_ensure), and it carries BOTH `--dport <port>`
        # and `--sport <port>`. So it drops a packet whose SOURCE or DEST port is <port>, in EITHER direction.
        # That means it blackholes <port> whether the node LISTENS on it (peers connecting IN to node:<port>
        # hang: INPUT --dport / the reply's OUTPUT --sport) OR the node CONNECTS OUT to a peer:<port> (the
        # node's OUTBOUND SYN has dst-port=<port> -> OUTPUT --dport -> dropped; the peer's reply has
        # src-port=<port> -> INPUT --sport -> dropped). Both are covered. (Verified empirically: a
        # `fault_partition_on <client> <listen-port>` makes <client>-><server>:<listen-port> go from rc=1
        # refused to rc=124 hung — egress to that port IS dropped, not just ingress to a local listener.)
        dexec "$_fp_node" -- sh -c "
            iptables -A $FAULT_CHAIN -p tcp --dport $_fp_port -j DROP
            iptables -A $FAULT_CHAIN -p tcp --sport $_fp_port -j DROP
        " >/dev/null 2>&1 || { err "fault_partition_on: iptables failed on $_fp_node port $_fp_port"; return 1; }
    done
    log "fault: partitioned $_fp_node on port(s) $* (silent DROP, symmetric, peer-agnostic)"
}

# fault_partition_off <node> : remove every rule this library added on <node>. Flushing the named chain is
# enough: nothing we add lives anywhere else.
fault_partition_off() {
    dexec "$1" -- iptables -F "$FAULT_CHAIN" >/dev/null 2>&1 || true
    log "fault: healed $1 (flushed $FAULT_CHAIN)"
}

# fault_synblock_on <node> <port> : drop only OUTBOUND SYNs to <port> (drill 33's upgrade-rollback
# instrument). ESTABLISHED flows are untouched — deliberately ASYMMETRIC vs fault_partition_on: the point
# is that the still-running old agent's registration stays ONLINE while any NEW connection (the re-exec'd
# binary's first register dial) black-holes until the 120s watchdog rolls back. Lives in the SIMFAULT
# chain like every other rule so the single-flush cleanup discipline holds. `--syn` expands to
# `--tcp-flags FIN,SYN,RST,ACK SYN` (spike-verified on the sim image's nf_tables backend).
fault_synblock_on() {
    _fs_node=$1; _fs_port=$2
    [ -n "${_fs_port:-}" ] || { err "fault_synblock_on <node> <port> — no port given"; return 1; }
    _fault_chain_ensure "$_fs_node" || { err "fault_synblock_on: could not create $FAULT_CHAIN on $_fs_node"; return 1; }
    dexec "$_fs_node" -- iptables -A "$FAULT_CHAIN" -p tcp --syn --dport "$_fs_port" -j DROP >/dev/null 2>&1 \
        || { err "fault_synblock_on: iptables failed on $_fs_node port $_fs_port"; return 1; }
    log "fault: syn-blocked $_fs_node -> :$_fs_port (new connections black-hole, established flows live)"
}

# fault_synblock_off <node> : heal. Same single-flush contract as fault_partition_off.
fault_synblock_off() { fault_partition_off "$1"; }

# fault_reject_on <node> <port> : REJECT (tcp-reset) <node>'s OUTBOUND connects to <port> — a FIFTH
# failure semantic, distinct from the four in the header: the dial fails FAST (connection refused),
# like an egress firewall actively refusing the tunnel port (#78's weak analog of the live WSL
# half-open :7000 — the exact live shape lets TCP establish and kills bytes, which needs a
# middlebox this sim does not front; REJECT reproduces the "agent can never establish, retries
# forever" driver without the client-side hang risk of PSH-drop). Lives in the SIMFAULT chain so
# the single-flush cleanup contract holds.
fault_reject_on() {
    _fr_node=$1; _fr_port=$2
    [ -n "${_fr_port:-}" ] || { err "fault_reject_on <node> <port> — no port given"; return 1; }
    _fault_chain_ensure "$_fr_node" || { err "fault_reject_on: could not create $FAULT_CHAIN on $_fr_node"; return 1; }
    dexec "$_fr_node" -- iptables -A "$FAULT_CHAIN" -p tcp --dport "$_fr_port" -j REJECT --reject-with tcp-reset >/dev/null 2>&1 \
        || { err "fault_reject_on: iptables failed on $_fr_node port $_fr_port"; return 1; }
    log "fault: rejecting $_fr_node -> :$_fr_port (fast refuse — the dial-never-establishes instrument)"
}

# fault_reject_pkts <node> <port> : the packet counter on the REJECT rule — one SYN per connect()
# attempt, so this is a PRODUCT-INDEPENDENT dial-attempt counter (#78's backoff oracle: the damped
# product logs deliberately stop restating attempts, so counting log lines would under-count by
# design; the netfilter counter sees every attempt regardless).
fault_reject_pkts() {
    dexec "$1" -- iptables -L "$FAULT_CHAIN" -v -x -n 2>/dev/null \
        | awk -v port="$2" '$0 ~ "REJECT" && $0 ~ ("dpt:" port) {print $1; exit}'
}

# fault_partition_peer_on <node> <peer> <port> : DROP <node>'s traffic to/from ONE PEER's <port>,
# leaving every other peer reachable. origin: internal review F98-1 — fault_partition_on is
# deliberately PEER-AGNOSTIC (it cuts a port toward everyone), so using it for "cut the connected
# broker, recover via another voter" black-holes the failover targets too and there is no recovery
# path left to measure. The peer's container IP is resolved through docker (the sim network's own
# DNS), so this cuts exactly one edge of the mesh.
fault_partition_peer_on() {
    _fpp_node=$1; _fpp_peer=$2; _fpp_port=$3
    [ -n "${_fpp_port:-}" ] || { err "fault_partition_peer_on <node> <peer> <port> — missing args"; return 1; }
    _fpp_ip=$(d inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$(ctr_name "$_fpp_peer")" 2>/dev/null)
    [ -n "$_fpp_ip" ] || { err "fault_partition_peer_on: could not resolve $_fpp_peer's container IP"; return 1; }
    _fault_chain_ensure "$_fpp_node" || { err "fault_partition_peer_on: could not create $FAULT_CHAIN on $_fpp_node"; return 1; }
    dexec "$_fpp_node" -- sh -c "
        iptables -A $FAULT_CHAIN -p tcp -d $_fpp_ip --dport $_fpp_port -j DROP
        iptables -A $FAULT_CHAIN -p tcp -s $_fpp_ip --sport $_fpp_port -j DROP
    " >/dev/null 2>&1 || { err "fault_partition_peer_on: iptables failed on $_fpp_node"; return 1; }
    log "fault: partitioned $_fpp_node -> $_fpp_peer($_fpp_ip):$_fpp_port only (other peers still reachable)"
}

# fault_cleanup_all : unconditional teardown for the drill's single EXIT trap (R-TRAP). Flushes the chain
# on EVERY node and un-freezes anything left SIGSTOPped. Never fails the drill — cleanup failure warns.
fault_cleanup_all() {
    for _fc_n in $(list_nodes); do
        [ -n "$_fc_n" ] || continue
        node_running "$_fc_n" || continue
        dexec "$_fc_n" -- iptables -F "$FAULT_CHAIN" >/dev/null 2>&1 || true
        dexec "$_fc_n" -- sh -c 'for p in $(pgrep -x tether 2>/dev/null); do kill -CONT "$p" 2>/dev/null; done' >/dev/null 2>&1 || true
    done
    true
}

# fault_assert_blackholed <from-node> <to-node> <port> : predicate — a TCP connect from <from> to
# <to>:<port> HANGS (rc 124 under `timeout`). This is the self-proof that the rule took effect AND that
# the semantics really are "silent drop" and not "refused".
#
# rc 124 = timeout killed it (packets vanished)  -> a PARTITION.
# rc 1   = connect failed fast (RST / unreachable) -> an OUTAGE. NOT what we injected.
fault_assert_blackholed() {
    _fb_rc=0
    dexec "$1" -- timeout 3 bash -c "</dev/tcp/$2/$3" >/dev/null 2>&1 || _fb_rc=$?
    [ "$_fb_rc" = 124 ] || { warn "fault_assert_blackholed: $1 -> $2:$3 gave rc=$_fb_rc (want 124=hung); rc=1 would mean REFUSED (an outage), not a silent partition"; return 1; }
    return 0
}

# fault_assert_reachable <from-node> <to-node> <port> : predicate — a TCP connect SUCCEEDS. The selective
# control source: proves the partition is targeted rather than a total blackout, so claims like "the
# minority survives read-only" are about tether and not about us having unplugged everything.
fault_assert_reachable() {
    dexec "$1" -- timeout 3 bash -c "</dev/tcp/$2/$3" >/dev/null 2>&1
}

# dp_curl_blackholed <from-node> <url> : succeeds iff curl TIMES OUT (exit 28) — the honest "the packets
# are being dropped" data-plane oracle inside an armed partition window.
# NOT dp_curl_refused (exit 7): that is the "process is dead" oracle and will fail here for the wrong
# reason. The two exit codes are mutually exclusive discriminators (see the header).
dp_curl_blackholed() {
    _dcb_rc=0
    dexec "$1" -- curl -s -o /dev/null --max-time 6 "$2" >/dev/null 2>&1 || _dcb_rc=$?
    [ "$_dcb_rc" -eq 28 ]
}

# fault_freeze_on <node> <unit> : SIGSTOP the unit's MainPID — a wedged-but-alive process. Distinct from
# both partition and crash: the socket stays open, the kernel keeps ACKing into a filling receive buffer,
# and systemd's Restart= never fires because the process never exits.
fault_freeze_on() {
    _ff_pid=$(dexec "$1" -- systemctl show -p MainPID --value "$2" 2>/dev/null | tr -d '\r')
    [ -n "$_ff_pid" ] && [ "$_ff_pid" != 0 ] || { err "fault_freeze_on: no MainPID for $2 on $1"; return 1; }
    dexec "$1" -- kill -STOP "$_ff_pid" >/dev/null 2>&1 || { err "fault_freeze_on: kill -STOP $_ff_pid failed on $1"; return 1; }
    log "fault: froze $1/$2 (SIGSTOP pid=$_ff_pid — socket open, systemd Restart never fires)"
    printf '%s' "$_ff_pid"
}

# fault_freeze_off <node> <pid> : SIGCONT.
fault_freeze_off() {
    dexec "$1" -- kill -CONT "$2" >/dev/null 2>&1 || true
    log "fault: thawed $1 pid=$2"
}
