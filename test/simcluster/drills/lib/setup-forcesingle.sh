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
    assert_ok "baseline: build the 12 MB tier-B payload" \
        "$SIM" exec ctl1 -- sh -c 'head -c 12000000 /dev/urandom > /tmp/base.bin; test -s /tmp/base.bin'
    # G67 (#67): the contract this asserts is "tier-B is SERVABLE here", and after G67 that contract is
    # explicitly "either it works first time, or it is refused with the TRANSIENT code and retrying the
    # same command shortly works" — the exact words the refusal now tells the operator.
    #
    # This is NOT a weakened assertion and NOT the sim doing tether's job. Before G67 a post-grow stall
    # produced a TERMINAL `bucket_create_failed` with no retry vocabulary, and that still hard-fails
    # here. What is now tolerated is only the case the product documents and instructs. The tooth: a
    # refusal that is NOT honestly transient fails, and a transient refusal whose retry ALSO fails is a
    # product_red, because then the documented remedy does not work.
    _base_out=$("$SIM" ctl -- push /tmp/base.bin agt1:/tmp/base.bin 2>&1); _base_rc=$?
    if [ "$_base_rc" = 0 ]; then
        _as_pass "baseline: tier-B push works on healthy N=2 (first attempt)"
    elif printf '%s' "$_base_out" | grep -q 'code=jetstream_not_ready'; then
        log "baseline: first tier-B push was refused as TRANSIENT (G67 contract) — exercising the documented remedy: $(printf '%s' "$_base_out" | tr '\n' ' ' | tr -cd '[:print:]' | cut -c1-200)"
        # The product's promise is "retry the same command shortly. If it persists, run `tether cluster
        # status`" — a SHORT WINDOW, not "exactly one retry suffices". The first version of this branch
        # asserted a single immediate retry and fired under 7-way sim saturation (6-9 clustered-JS
        # clusters on one host); that was the fixture over-specifying relative to the product's own
        # wording, so the window is now bounded-but-plural and matches it. The TOOTH is unchanged: if the
        # documented remedy never works inside the window, it is still a product_red. The attempt count
        # is logged so a worsening shows up as a number, not as a silent pass.
        _base_try=1
        while [ "$_base_try" -lt 6 ]; do
            _base_try=$((_base_try+1))
            if "$SIM" ctl -- push /tmp/base.bin agt1:/tmp/base.bin >/dev/null 2>&1; then break; fi
            sleep 5
            if [ "$_base_try" -ge 6 ]; then _base_try=99; fi
        done
        if [ "$_base_try" != 99 ]; then
            log "baseline: the documented retry window closed the gap on attempt $_base_try (product says 'retry the same command shortly')"
            _as_pass "baseline: tier-B push works on healthy N=2 (transient refusal, then the documented retry window succeeded — G67 contract holds)"
        else
            product_red "#67 residual: the tier-B refusal said it was transient and told the operator to retry shortly, but the documented retry window (5 attempts over ~25s) NEVER succeeded on a healthy N=2 — the documented remedy does not work here"
        fi
    else
        _as_fail "baseline: tier-B push works on healthy N=2 (refused, and NOT with the transient code — a terminal refusal on a healthy cluster is the pre-G67 #67 shape)" \
            "$(printf '%s' "$_base_out" | tr '\n' ' ' | tr -cd '[:print:]' | cut -c1-300)"
    fi
}
