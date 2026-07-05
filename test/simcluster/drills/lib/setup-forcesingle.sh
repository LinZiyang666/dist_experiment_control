# drills/lib/setup-forcesingle.sh — shared setup for #20 + #12. Sourced by those drills (SIM/PIN/SID
# in scope). Brings up a REAL N=2 clustered-JS cluster + 1 agent + a ctl session, and — the
# load-bearing false-green guard (plan §5.2, critique G3) — asserts the 2-node JS meta actually
# FORMED (and tier-B works) BEFORE the peer is killed, so the post-force-single 503 is provably
# quorum-loss rot, not a meta that never formed.
setup_forcesingle_n2() {
    "$SIM" nuke >/dev/null 2>&1 || true
    assert_ok "up 2 brokers + 1 agent + 1 ctl"                 "$SIM" up --brokers 2 --agents 1 --ctl 1
    assert_ok "init brk1 (N=1)"                                 "$SIM" init brk1
    assert_ok "grow brk2 (N=2 clustered JS)"                    "$SIM" grow brk2
    assert_ok "session + ctl login"                            "$SIM" session "$SID" --pin "$PIN"
    assert_ok "agent-join agt1"                                "$SIM" agent-join agt1 --session "$SID" --pin "$PIN"
    # FALSE-GREEN GUARD: the 2-node JS meta must have FORMED (cluster_size==2) before we kill the peer.
    assert_ok "JS meta FORMED at N=2 (cluster_size==2) before kill" \
        sh -c "$SIM exec brk1 -- sh -c 'curl -s \"localhost:8223/jsz?meta=1\"' 2>/dev/null | jq -e '.meta_cluster.cluster_size==2' >/dev/null"
    assert_ok "baseline: tier-B push works on healthy N=2" \
        sh -c "$SIM exec ctl1 -- sh -c 'head -c 12000000 /dev/urandom > /tmp/base.bin' && $SIM ctl -- push /tmp/base.bin agt1:/tmp/base.bin"
}
