# lib/secrets.sh — mint + distribute the §15 per-broker secrets tree. Sourced by simcluster.
# Depends on lib/docker.sh (d, dexec, ctr_name) + lib/log.sh.
#
# OQ-3 (finalizer): minted with host openssl (ed25519) + vendored nk — no Go minter. ed25519 CA
# (CA:TRUE, keyCertSign+digitalSignature) + CA-signed leaves (keyUsage=digitalSignature,
# extKeyUsage=serverAuth+clientAuth). CRITICAL (#24): route/tunnel leaves MUST carry
# subjectAltName=DNS:<node_id> — the nats-server cluster route mesh does STANDARD Go x509 verify
# (verify:true) which REQUIRES a SAN; a CN-only cert is rejected and the mesh never forms. (tether's
# raft transport separately skips hostname verify, so CN-only satisfies raft — but NOT the nats mesh.)
# nkeys: node-ident.nk / broker.nk = USER seeds; account.nk = ACCOUNT seed (auth_callout issuer).
# CA + account are shared per instance; route/tunnel/node-ident/broker are per node.

: "${SECRETS_STASH:=$HERE/secrets}"      # gitignored host-side stash
: "${NK_BIN:=$HERE/vendor/nk}"

_secrets_shared_dir() { printf '%s/%s/_shared' "$SECRETS_STASH" "$1"; }
_secrets_node_dir()   { printf '%s/%s/%s' "$SECRETS_STASH" "$1" "$2"; }

secrets_ensure_shared() {  # mint the instance's shared CA + account once
    _es_i=$1; _es_d=$(_secrets_shared_dir "$_es_i")
    [ -f "$_es_d/cluster-ca.pem" ] && return 0
    mkdir -p "$_es_d"; chmod 700 "$_es_d"
    openssl genpkey -algorithm ed25519 -out "$_es_d/ca-key.pem" 2>/dev/null
    openssl req -x509 -new -key "$_es_d/ca-key.pem" -days 3650 -out "$_es_d/cluster-ca.pem" \
        -subj "/CN=tether-sim-ca-$_es_i" \
        -addext "basicConstraints=critical,CA:TRUE" \
        -addext "keyUsage=critical,keyCertSign,digitalSignature" 2>/dev/null
    "$NK_BIN" -gen account > "$_es_d/account.nk"
    chmod 600 "$_es_d/account.nk" "$_es_d/ca-key.pem"
    log "secrets: minted shared CA + account (instance=$_es_i)"
}

_mint_leaf() {  # _mint_leaf <shared-dir> <out-dir> <name> <cn>
    _ml_ca=$1; _ml_out=$2; _ml_nm=$3; _ml_cn=$4
    # SANs are LOAD-BEARING: nats-server's cluster route mTLS does standard Go x509 verification,
    # which (Go 1.15+) REQUIRES a SAN — a CN-only cert fails "relies on legacy Common Name field,
    # use SANs instead" and the route handshake closes (routes never form → JS meta never forms).
    # DNS:<node_id> matches the route URL host (nats://<node>:6222); localhost/127.0.0.1 for safety.
    _ml_ext="$_ml_out/$_ml_nm.ext"
    printf 'keyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:%s,DNS:localhost,IP:127.0.0.1\n' "$_ml_cn" > "$_ml_ext"
    openssl genpkey -algorithm ed25519 -out "$_ml_out/$_ml_nm-key.pem" 2>/dev/null
    openssl req -new -key "$_ml_out/$_ml_nm-key.pem" -subj "/CN=$_ml_cn" -out "$_ml_out/$_ml_nm.csr" 2>/dev/null
    openssl x509 -req -in "$_ml_out/$_ml_nm.csr" -CA "$_ml_ca/cluster-ca.pem" -CAkey "$_ml_ca/ca-key.pem" \
        -CAcreateserial -days 3650 -out "$_ml_out/$_ml_nm-cert.pem" -extfile "$_ml_ext" 2>/dev/null
    rm -f "$_ml_out/$_ml_nm.csr" "$_ml_ext"
    chmod 600 "$_ml_out/$_ml_nm-key.pem"; chmod 644 "$_ml_out/$_ml_nm-cert.pem"
}

secrets_mint_node() {  # secrets_mint_node <instance> <node>
    _mn_i=$1; _mn_n=$2
    secrets_ensure_shared "$_mn_i"
    _mn_sh=$(_secrets_shared_dir "$_mn_i"); _mn_nd=$(_secrets_node_dir "$_mn_i" "$_mn_n")
    [ -f "$_mn_nd/route-cert.pem" ] && return 0
    mkdir -p "$_mn_nd"; chmod 700 "$_mn_nd"
    _mint_leaf "$_mn_sh" "$_mn_nd" route  "$_mn_n"
    _mint_leaf "$_mn_sh" "$_mn_nd" tunnel "$_mn_n"
    "$NK_BIN" -gen user > "$_mn_nd/node-ident.nk"; chmod 600 "$_mn_nd/node-ident.nk"
    "$NK_BIN" -gen user > "$_mn_nd/broker.nk";     chmod 600 "$_mn_nd/broker.nk"
    cp "$_mn_sh/cluster-ca.pem" "$_mn_nd/cluster-ca.pem"
    cp "$_mn_sh/account.nk"     "$_mn_nd/account.nk"; chmod 600 "$_mn_nd/account.nk"
    log "secrets: minted node tree ($_mn_n)"
}

# secrets_node_ident_pub <instance> <node> : the node-ident PUBLIC key (for cluster init --node-ident-pub).
secrets_node_ident_pub() { "$NK_BIN" -inkey "$(_secrets_node_dir "$1" "$2")/node-ident.nk" -pubout 2>/dev/null; }
secrets_account_pub()    { "$NK_BIN" -inkey "$(_secrets_shared_dir "$1")/account.nk" -pubout 2>/dev/null; }
secrets_broker_pub()     { "$NK_BIN" -inkey "$(_secrets_node_dir "$1" "$2")/broker.nk" -pubout 2>/dev/null; }
# peer_triple <instance> <node> : the "server_name,route_url,bus_nkey" triple reconcile --peer wants.
peer_triple() { printf '%s,nats://%s:6222,%s' "$2" "$2" "$(secrets_broker_pub "$1" "$2")"; }

# secrets_distribute <instance> <node> : docker cp the node tree into the container + chown/chmod.
# docker cp lands root:0644; serve/SecretsPreflight HARD-FATAL on a private key with &0o077 != 0.
secrets_distribute() {
    _sd_i=$1; _sd_n=$2; _sd_nd=$(_secrets_node_dir "$_sd_i" "$_sd_n"); _sd_ctr=$(ctr_name "$_sd_n")
    dexec "$_sd_n" -- mkdir -p /etc/tether/secrets
    for f in cluster-ca.pem route-cert.pem route-key.pem tunnel-cert.pem tunnel-key.pem node-ident.nk broker.nk account.nk; do
        d cp "$_sd_nd/$f" "$_sd_ctr:/etc/tether/secrets/$f"
    done
    dexec "$_sd_n" -- sh -c 'chown -R tether:tether /etc/tether/secrets && chmod 700 /etc/tether/secrets && chmod 600 /etc/tether/secrets/*-key.pem /etc/tether/secrets/*.nk && chmod 644 /etc/tether/secrets/*-cert.pem /etc/tether/secrets/cluster-ca.pem'
    log "secrets: distributed to $_sd_n (tether:tether, keys 0600)"
}

# ─────────────────────────────────────────────────────────────────────────────────────────────────────
# G-C / S7-52 additions — CREDENTIAL ROTATION generations (s7-s9-plan §1.2-(3), §1.3).
#
# WHY A SEPARATE "GENERATION" INSTEAD OF RE-MINTING _shared. The coverage inventory (§3) makes the FIRST
# batch to land the instance CA its facility owner, and every later batch reuses it and must never re-mint
# it (drills/lib/ingress.sh:17-18 hardcodes that). Drill 52's entire reason to exist is rotating the CA.
# The reconciliation: gen1 IS `_shared` and is NEVER touched; a rotation mints `_shared-gen<N>` alongside
# it. The owner rule holds and 52 still gets a real rotation.
# (Corollary written into the plan: 52's topology MUST NOT use the ingress sidecar or artifact server —
# a gen2 rotation would break their gen1-CA trust and manufacture a FAKE RED.)

_secrets_gen_dir() { printf '%s/%s/_shared-gen%s' "$SECRETS_STASH" "$1" "$2"; }

# secrets_mint_gen <instance> <gen> : mint generation-N trust material (a NEW account.nk + a NEW CA).
# gen 1 == `_shared` and is refused outright, so the facility-owner rule can never be violated by accident.
secrets_mint_gen() {
    _mg_i=$1; _mg_g=$2
    [ "$_mg_g" != 1 ] || { err "secrets_mint_gen: generation 1 IS _shared (the instance CA facility, owned by S2) — it must never be re-minted"; return 1; }
    _mg_d=$(_secrets_gen_dir "$_mg_i" "$_mg_g")
    [ -f "$_mg_d/cluster-ca.pem" ] && return 0
    mkdir -p "$_mg_d"; chmod 700 "$_mg_d"
    openssl genpkey -algorithm ed25519 -out "$_mg_d/ca-key.pem" 2>/dev/null || return 1
    openssl req -x509 -new -key "$_mg_d/ca-key.pem" -days 3650 -out "$_mg_d/cluster-ca.pem" \
        -subj "/CN=tether-sim-ca-$_mg_i-gen$_mg_g" \
        -addext "basicConstraints=critical,CA:TRUE" \
        -addext "keyUsage=critical,keyCertSign,digitalSignature" 2>/dev/null || return 1
    "$NK_BIN" -gen account > "$_mg_d/account.nk" || return 1
    chmod 600 "$_mg_d/account.nk" "$_mg_d/ca-key.pem"
    log "secrets: minted generation-$_mg_g trust material (new account.nk + new CA) for instance=$_mg_i; gen1/_shared untouched"
}

# (secrets_remint_route_only was removed — drill 52 swaps only account.nk; the route-leaf rotation it did
# was never wired and would break the mesh. Stage-C B3.)

# secrets_mint_tunnel_only <instance> <node> : re-issue ONLY the node's tunnel leaf under the CURRENT
# generation's CA (gen1/_shared). Feeds `rotate-tunnel-cert`'s positive arm: a NEW fp for the same node.
secrets_mint_tunnel_only() {
    _mt_i=$1; _mt_n=$2
    _mt_sh=$(_secrets_shared_dir "$_mt_i"); _mt_nd=$(_secrets_node_dir "$_mt_i" "$_mt_n")
    [ -d "$_mt_nd" ] || { err "secrets_mint_tunnel_only: no node tree for $_mt_n"; return 1; }
    rm -f "$_mt_nd/tunnel-cert.pem" "$_mt_nd/tunnel-key.pem"
    _mint_leaf "$_mt_sh" "$_mt_nd" tunnel "$_mt_n" || return 1
    log "secrets: re-minted $_mt_n's tunnel leaf (new fingerprint, same CA)"
}

# EXT-REVIEW-B3: snapshot / restore a node's CURRENT tunnel leaf so a rotation drill can roll back to the
# EXACT previously-pinned generation. secrets_mint_tunnel_only OVERWRITES the leaf in place (rm + re-mint),
# so without a snapshot a "recovery" that re-pushes the stash copy would ship the NEW (unpinned) leaf and
# the fail-closed broker would stay bricked — the drill could never prove the old pin was restored.
# Snapshots live under a dedicated `.snapshots/` root that secrets_distribute's fixed file list ignores.
_secrets_snap_dir() { printf '%s/%s/.snapshots/%s-%s' "$SECRETS_STASH" "$1" "$2" "$3"; }
secrets_snapshot_tunnel() {  # <instance> <node> <slot>
    _ss_nd=$(_secrets_node_dir "$1" "$2"); _ss_sd=$(_secrets_snap_dir "$1" "$2" "$3")
    [ -f "$_ss_nd/tunnel-cert.pem" ] && [ -f "$_ss_nd/tunnel-key.pem" ] || { err "secrets_snapshot_tunnel: no current tunnel leaf for $2"; return 1; }
    mkdir -p "$_ss_sd" && cp "$_ss_nd/tunnel-cert.pem" "$_ss_sd/tunnel-cert.pem" \
        && cp "$_ss_nd/tunnel-key.pem" "$_ss_sd/tunnel-key.pem" && chmod 600 "$_ss_sd/tunnel-key.pem" || return 1
    log "secrets: snapshotted $2's current tunnel leaf as generation '$3'"
}
secrets_restore_tunnel_snapshot() {  # <instance> <node> <slot>
    _sr_nd=$(_secrets_node_dir "$1" "$2"); _sr_sd=$(_secrets_snap_dir "$1" "$2" "$3")
    [ -f "$_sr_sd/tunnel-cert.pem" ] && [ -f "$_sr_sd/tunnel-key.pem" ] || { err "secrets_restore_tunnel_snapshot: no snapshot '$3' for $2"; return 1; }
    cp "$_sr_sd/tunnel-cert.pem" "$_sr_nd/tunnel-cert.pem" && cp "$_sr_sd/tunnel-key.pem" "$_sr_nd/tunnel-key.pem" || return 1
    log "secrets: restored $2's saved previous tunnel leaf from generation '$3'"
}

# secrets_tunnel_fp <instance> <node> : the node's on-disk tunnel-cert fingerprint, computed the way the
# PRODUCT computes it — sha256 over the DER bytes (internal/tunnel/tls.go:91-94: sha256(cert.Raw), and
# cert.Raw IS the DER encoding). Drill 52 arm A3 cross-checks this against the fp tether itself reports;
# if they ever disagree that is a HARNESS bug to fix immediately, because every later fp oracle would be
# meaningless.
secrets_tunnel_fp() {
    openssl x509 -in "$(_secrets_node_dir "$1" "$2")/tunnel-cert.pem" -outform DER 2>/dev/null \
        | sha256sum 2>/dev/null | awk '{print $1}'
}

# secrets_push_file <instance> <node> <filename> : push ONE file from the host key vault into the node's
# live secrets dir with the ownership/mode the product REQUIRES.
# 0600 + tether-owned is load-bearing, not hygiene: SecretsPreflight hard-fatals on a private key with
# mode &0o077 != 0 (internal/clusteroffline/preflight.go:86-88). Forgetting the chmod would launder a real
# product finding into a fake SETUP-RED.
secrets_push_file() {
    _pf_i=$1; _pf_n=$2; _pf_f=$3
    _pf_src="$(_secrets_node_dir "$_pf_i" "$_pf_n")/$_pf_f"
    [ -f "$_pf_src" ] || { err "secrets_push_file: no such file in the vault: $_pf_src"; return 1; }
    d cp "$_pf_src" "$(ctr_name "$_pf_n")":/etc/tether/secrets/"$_pf_f" >/dev/null 2>&1 || return 1
    case "$_pf_f" in
        *-key.pem|*.nk) _pf_mode=600 ;;
        *)              _pf_mode=644 ;;
    esac
    dexec "$_pf_n" -- sh -c "chown tether:tether /etc/tether/secrets/$_pf_f && chmod $_pf_mode /etc/tether/secrets/$_pf_f" >/dev/null 2>&1 || return 1
    log "secrets: pushed $_pf_f -> $_pf_n (tether:tether, $_pf_mode)"
}
