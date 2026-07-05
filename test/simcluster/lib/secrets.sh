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
