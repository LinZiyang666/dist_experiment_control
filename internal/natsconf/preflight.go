// Package natsconf is the D9 §11 tether-takeover of /etc/tether/nats.d/nats.conf (G1 #22: a
// tether-owned subdir the User=tether reconciler can atomically rewrite). install.sh
// hand-writes that file today (host/port/jetstream/websocket, no auth/cluster); the D9
// cutover has tether REWRITE it with the cluster directives (routes mTLS + auth_callout +
// per-broker ACLs) while PRESERVING the install.sh client/websocket/jetstream bits and any
// documented tuning. The takeover is FAIL-CLOSED: if the existing conf carries a directive
// tether neither recognizes (install.sh safe set) nor generates (the cluster set) nor
// explicitly passes through (documented tuning), it REFUSES rather than silently dropping a
// hand-tuned conf. It is a leaf: NO raft / NATS-runtime imports — only the nats-server conf
// LEXER (the real parser; a line-based recognizer mis-handles quoted nkey subjects).
package natsconf

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/nats-io/nats-server/v2/conf"
)

// Bucket classifies a top-level nats.conf directive for the takeover ownership table.
type Bucket string

const (
	// InstallSafe: written by install.sh, PRESERVED verbatim into the merged conf.
	InstallSafe Bucket = "install_safe"
	// TetherGen: tether REGENERATES these from the cluster identity (Render owns them).
	TetherGen Bucket = "tether_gen"
	// OperatorAuth: the §3.4 hand-added authorization{} — READ for the cluster identity
	// (auth_callout.issuer + broker nkey), then SUPERSEDED by the generated authorization.
	OperatorAuth Bucket = "operator_auth"
	// TetherPassthrough: recognized tuning directives PRESERVED verbatim (usage.md §5.16
	// documents max_payload>=16MiB for tier-A transfer — refusing it would brick a
	// docs-compliant file-transfer broker).
	TetherPassthrough Bucket = "tether_passthrough"
)

// bucketOf maps each RECOGNIZED top-level key to its bucket. A key absent here is UNKNOWN
// ⇒ the takeover refuses (fail-closed). install.sh writes host+port (NOT a `listen` key),
// so both are InstallSafe and ClientListen is harvested from them.
var bucketOf = map[string]Bucket{
	"host":            InstallSafe,
	"port":            InstallSafe,
	"listen":          InstallSafe, // accepted if an operator used it, though install.sh uses host+port
	"jetstream":       InstallSafe,
	"websocket":       InstallSafe,
	"http":            TetherGen, // C3: loopback monitor for the reconciler observed-gen probe (round-trips own output)
	"server_name":     TetherGen,
	"cluster":         TetherGen,
	"authorization":   OperatorAuth, // read for identity, regenerated
	"max_payload":     TetherPassthrough,
	"write_deadline":  TetherPassthrough,
	"max_pending":     TetherPassthrough,
	"max_connections": TetherPassthrough,
}

// jetstreamSafeSubkeys / websocketSafeSubkeys are the ONLY subkeys the takeover PRESERVES:
// BuildMergedConf re-emits exactly these (JSStoreDir → jetstream{store_dir}; websocketBlock →
// websocket{host,port,no_tls}). jetstream{} and websocket{} are InstallSafe at the TOP level,
// but a hand-added subkey (jetstream.domain / .max_file_store, websocket.compression) would
// pass the top-level gate and then be SILENTLY DROPPED on re-render — exactly what the
// package's fail-closed contract forbids. So Preflight refuses any unrecognized subkey too
// (audit M6 / natsconf F1). install.sh writes only these, so the common path is unaffected.
var (
	jetstreamSafeSubkeys = map[string]bool{"store_dir": true}
	websocketSafeSubkeys = map[string]bool{"host": true, "port": true, "no_tls": true}
	// C3-M1: cluster{} and authorization{} are recognized at the top level (TetherGen), but the
	// reconciler now REGENERATES both blocks AUTOMATICALLY — so an unrecognized SUBKEY (a hand-set
	// cluster.no_advertise / cluster.pool_size, or auth_callout.xkey which breaks callout decryption)
	// would be silently dropped on every auto-reconcile (acceptance #3 violated for subkeys). Refuse
	// them fail-closed (the reconciler then stays STUCK, never auto-overwriting a hand-tuned block).
	clusterSafeSubkeys       = map[string]bool{"name": true, "listen": true, "routes": true, "tls": true}
	authorizationSafeSubkeys = map[string]bool{"auth_callout": true, "users": true}
	authCalloutSafeSubkeys   = map[string]bool{"issuer": true, "account": true, "auth_users": true}
)

// lowerKeys recursively lowercases every map key (nats keys are case-insensitive). Values
// are preserved; nested maps + slices of maps are descended so the accessors that read
// nested keys (auth_callout, users, store_dir) are also case-insensitive.
func lowerKeys(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[strings.ToLower(k)] = lowerValue(v)
	}
	return out
}

func lowerValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return lowerKeys(t)
	case []any:
		for i := range t {
			t[i] = lowerValue(t[i])
		}
		return t
	default:
		return v
	}
}

// Owned is one classified top-level directive (the before/after ownership table row).
type Owned struct {
	Key    string
	Bucket Bucket
}

// Ownership is the classified existing conf: the table + the raw parsed map (for the merge
// step to harvest install.sh values + the operator auth identity).
type Ownership struct {
	Entries []Owned
	Parsed  map[string]any
}

// Preflight parses the existing nats.conf, refuses if it contains an `include` (the parser
// inlines/errors on includes — a split-file conf can't be safely round-tripped by
// re-rendering) or ANY unrecognized top-level directive (accounts/leafnodes/mqtt/gateways/
// operators/…), and otherwise returns the ownership table. This is the fail-closed gate
// that prevents the takeover from silently dropping a hand-tuned conf.
func Preflight(confPath string) (*Ownership, error) {
	raw, err := os.ReadFile(confPath)
	if err != nil {
		return nil, fmt.Errorf("natsconf: read %q: %w", confPath, err)
	}
	if hasIncludeDirective(string(raw)) {
		return nil, fmt.Errorf("natsconf: %q uses `include` — split-file confs cannot be safely "+
			"taken over (the operator's intent can't be round-tripped); inline it or take over manually", confPath)
	}
	parsed, err := conf.Parse(string(raw))
	if err != nil {
		return nil, fmt.Errorf("natsconf: parse %q: %w", confPath, err)
	}
	// round-1 MAJOR: nats-server keys are CASE-INSENSITIVE (it lowercases on load) but
	// conf.Parse preserves the original case in the map, so a hand-edited `Host:`/`PORT:`/
	// `WebSocket{}` would slip past the classifier AND make ClientListen/AuthIdentity/
	// JSStoreDir miss. Normalize every key to lowercase recursively so the whole leaf is
	// case-insensitive (install.sh already writes lowercase, so this is a no-op there).
	parsed = lowerKeys(parsed)
	own := &Ownership{Parsed: parsed}
	var unknown []string
	for key := range parsed {
		b, ok := bucketOf[strings.ToLower(key)]
		if !ok {
			unknown = append(unknown, key)
			continue
		}
		own.Entries = append(own.Entries, Owned{Key: key, Bucket: b})
	}
	// Subkey fail-closed (audit M6 / natsconf F1): refuse an unrecognized subkey inside
	// jetstream{} / websocket{} too — the takeover re-emits only a fixed subset, so any other
	// subkey would be silently dropped on re-render (e.g. a hand-set JS `domain` that breaks
	// cross-cluster stream addressing).
	unknown = append(unknown, unrecognizedSubkeys(parsed, "jetstream", jetstreamSafeSubkeys)...)
	unknown = append(unknown, unrecognizedSubkeys(parsed, "websocket", websocketSafeSubkeys)...)
	// C3-M1: cluster{}/authorization{} subkeys + the nested authorization.auth_callout subkeys (xkey!)
	// are now auto-regenerated by the reconciler — fail-closed on any tether does not re-emit.
	unknown = append(unknown, unrecognizedSubkeys(parsed, "cluster", clusterSafeSubkeys)...)
	unknown = append(unknown, unrecognizedSubkeys(parsed, "authorization", authorizationSafeSubkeys)...)
	if auth, ok := parsed["authorization"].(map[string]any); ok {
		unknown = append(unknown, unrecognizedSubkeys(auth, "auth_callout", authCalloutSafeSubkeys)...)
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("natsconf: %q contains directive(s) tether does not recognize: %s — "+
			"refusing takeover (would silently drop them); remove them or take over manually",
			confPath, strings.Join(unknown, ", "))
	}
	sort.Slice(own.Entries, func(i, j int) bool { return own.Entries[i].Key < own.Entries[j].Key })
	return own, nil
}

// unrecognizedSubkeys returns the dotted names (e.g. "jetstream.domain") of any subkey inside
// the named block that is NOT in the preserved allow-list. Returns nil if the block is absent
// or not a map (e.g. `jetstream: true`). The parsed map is already lowercased.
func unrecognizedSubkeys(parsed map[string]any, block string, safe map[string]bool) []string {
	m, ok := parsed[block].(map[string]any)
	if !ok {
		return nil
	}
	var out []string
	for k := range m {
		if !safe[strings.ToLower(k)] {
			out = append(out, block+"."+k)
		}
	}
	return out
}

// hasIncludeDirective reports whether the raw conf has a top-level-ish `include` token. The
// conf lexer inlines includes (so there is no `include` KEY to classify), hence a raw scan.
// Matches `include` as a leading token on a line (after optional whitespace), the form
// nats-server accepts.
func hasIncludeDirective(raw string) bool {
	for _, line := range strings.Split(raw, "\n") {
		// round-2 MAJOR: nats-server lowercases the `include` keyword at lex time, so an
		// uppercase `Include` is still an include — match case-insensitively (else a split-
		// file conf the parser would inline slips past the fail-closed include guard).
		t := strings.ToLower(strings.TrimSpace(line))
		if strings.HasPrefix(t, "include ") || strings.HasPrefix(t, "include\t") || t == "include" {
			return true
		}
	}
	return false
}

// ServerName harvests the nats server_name directive from the live conf (a tether-generated field). The
// offline force-single de-cluster render needs it to name the lone survivor; empty if absent.
func (o *Ownership) ServerName() string {
	if s, ok := o.Parsed["server_name"].(string); ok {
		return s
	}
	return ""
}

// AuthIdentity extracts the cluster identity from the existing conf's §3.4 authorization
// block: the auth_callout.issuer (the shared account pub == AccountIssuer) and, only when
// unambiguous, the broker bus nkey. Generated cluster configs contain one user per broker,
// so a multi-user block cannot identify "this" node by position; callers must pass
// --broker-nkey explicitly in that case. Empty strings if the block is absent or ambiguous.
func (o *Ownership) AuthIdentity() (issuer, brokerNkey string) {
	auth, ok := o.Parsed["authorization"].(map[string]any)
	if !ok {
		return "", ""
	}
	if ac, ok := auth["auth_callout"].(map[string]any); ok {
		issuer, _ = ac["issuer"].(string)
	}
	var userNkeys []string
	if users, ok := auth["users"].([]any); ok {
		for _, raw := range users {
			if u, ok := raw.(map[string]any); ok {
				if nk, _ := u["nkey"].(string); nk != "" {
					userNkeys = append(userNkeys, nk)
				}
			}
		}
	}
	if len(userNkeys) == 1 {
		brokerNkey = userNkeys[0]
	}
	return issuer, brokerNkey
}

// ClusterMTLS harvests the routes-mTLS identity from the LIVE conf's cluster{} block (C3-B1): the
// reconciler does not carry the cert paths, so BuildMergedConf reads them here to render a COMPLETE
// conf (Render hard-rejects empty cert paths). Fail-closed: a cluster-mode broker's conf
// MUST already carry cluster.tls — a missing block returns an error so the reconciler stays loud
// (never renders a conf that would drop routes mTLS).
func (o *Ownership) ClusterMTLS() (ca, cert, key, listen, name string, err error) {
	cl, ok := o.Parsed["cluster"].(map[string]any)
	if !ok {
		return "", "", "", "", "", fmt.Errorf("natsconf: live conf has no cluster{} block to harvest routes mTLS from")
	}
	listen, _ = cl["listen"].(string)
	name, _ = cl["name"].(string)
	tls, ok := cl["tls"].(map[string]any)
	if !ok {
		return "", "", "", "", "", fmt.Errorf("natsconf: live conf cluster{} has no tls{} block (routes mTLS)")
	}
	ca, _ = tls["ca_file"].(string)
	cert, _ = tls["cert_file"].(string)
	key, _ = tls["key_file"].(string)
	if ca == "" || cert == "" || key == "" {
		return "", "", "", "", "", fmt.Errorf("natsconf: live conf cluster.tls missing ca_file/cert_file/key_file")
	}
	return ca, cert, key, listen, name, nil
}

// MonitorHTTP harvests the existing HTTP monitor listen (`http:`) from the live conf (C3-B3). The
// reconciler PRESERVES whatever monitor the conf already has — it never hot-adds `http:`, because
// nats-server cannot reload an http-port add (it would reject the whole SIGHUP). A monitor is
// established only at a manual takeover + restart. Empty string ⇒ no monitor configured.
func (o *Ownership) MonitorHTTP() string {
	if h, ok := o.Parsed["http"].(string); ok {
		return h
	}
	return ""
}

// JSStoreDir harvests the JetStream store_dir install.sh set (preserved across takeover).
func (o *Ownership) JSStoreDir() string {
	if js, ok := o.Parsed["jetstream"].(map[string]any); ok {
		if d, ok := js["store_dir"].(string); ok {
			return d
		}
	}
	return ""
}

// HasJetStream reports whether the live conf enables JetStream AT ALL, regardless of whether it is
// clustered. It is deliberately weaker than its two siblings below, and the weakness is the point:
// BuildMergedConf uses it to refuse rendering a conf with no jetstream{} block over one that HAS a
// jetstream key but from which no store_dir could be harvested (BUG-1). The sibling predicates both
// also inspect cluster{}, so neither can answer "would this render silently drop JetStream".
//
// Note it is true for a bare `jetstream {}` with no subkeys, which is exactly the input that made the
// defect reachable: Preflight accepts that shape (jetstream is InstallSafe at the top level and
// unrecognizedSubkeys returns nil for an empty map), so nothing upstream rejects it.
func (o *Ownership) HasJetStream() bool {
	v, ok := o.Parsed["jetstream"]
	if !ok {
		return false
	}
	// KEY PRESENCE IS NOT ENABLEMENT (internal review of B5's guard).
	//
	// nats-server accepts `jetstream: false` / `jetstream: disabled` / `jetstream: off` as an explicit
	// DISABLE. The first version of this predicate returned true for those, and because BuildMergedConf's
	// BUG-1 guard keys on it, a broker whose operator had deliberately switched JetStream off would be
	// refused a render — permanently, in the 5-second reconcile loop, with a banner advising them to add
	// a store_dir, i.e. to turn back on the subsystem they had turned off. The guard exists to stop
	// JetStream being taken away by accident; a conf that asks for no JetStream is not that.
	//
	// The bare `jetstream {}` map with no store_dir stays TRUE, which is the shape that made the original
	// defect reachable.
	switch t := v.(type) {
	case bool:
		return t
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "false", "disabled", "off", "no":
			return false
		}
		return true
	default:
		// A map (the normal `jetstream { … }` block) or anything else structured: enabled.
		return true
	}
}

// TWO FACTS, TWO PREDICATES — and the compound that used to blur them is gone (external review RB2-1)
// ----------------------------------------------------------------------------------------------------
// A nats.conf carries two INDEPENDENT facts, and every bug in this area came from a predicate that
// answered both at once:
//
//	NATS TOPOLOGY        is there a cluster{} block?     -> IsClusteredTopology / IsStandaloneTopology
//	JETSTREAM ENABLEMENT is JetStream actually ON?       -> HasJetStream (value-aware)
//
// They are independent: `jetstream: false` + `cluster{}` is a clustered NATS with JetStream switched
// off, and it is a shape an operator reaches for during a JetStream incident.
//
// WHAT THE COMPOUND PREDICATES DID, AND WHY MY FIRST DEFENCE OF THEM WAS WRONG
// ----------------------------------------------------------------------------
// `IsStandaloneJetStream`/`IsClusteredJetStream` were `keyPresence && (!)hasCluster`. When external
// review B2-1 said every JetStream decision should use one value-aware predicate, I agreed for the
// enablement guard but REFUSED to touch these two, arguing that making them value-aware would break
// force-single's de-cluster of a `jetstream: false` + `cluster{}` conf.
//
// The re-review (RB2-1) showed that argument rested on a false premise: that conf could not be
// de-clustered EITHER WAY. `IsClusteredJetStream` said true (key present), so buildStandaloneConf
// entered the de-cluster arm and then demanded a JS `store_dir` that a disabled JetStream does not
// have — failing with "source JetStream has no explicit store_dir". My rebuttal described a regression
// away from a state that never worked. The second half was wrong too: under TOPOLOGY predicates a lone
// `jetstream: false` node is standalone, so IntentPreserve keeps rendering standalone and the
// ActionRejected wedge I predicted does not arise. It only arose against the strawman of folding
// topology INTO HasJetStream, which is not what was asked for.
//
// So the compounds are deleted rather than renamed. A caller that needs both facts now writes both,
// and the second conjunct is visible at the call site instead of hidden in a predicate's name — which
// is exactly where the two destructive bugs (a de-cluster that refuses, an `rm -rf` advised to a broker
// with JetStream off) were able to hide.

// IsClusteredTopology reports whether the conf has a cluster{} block — i.e. whether this node is
// configured as a cluster member. It says NOTHING about JetStream.
func (o *Ownership) IsClusteredTopology() bool {
	_, hasCluster := o.Parsed["cluster"]
	return hasCluster
}

// IsStandaloneTopology reports whether the conf has NO cluster{} block. The exact complement of
// IsClusteredTopology, spelled out because "no cluster block" is what most call sites actually mean and
// `!IsClusteredTopology()` reads as a double negative at the point of use.
//
// This is the shape `cluster init --from-existing` leaves an N=1 broker in (clustered JS refuses to
// start without configured routes). A node in this shape that ALSO runs JetStream cannot simply be
// restarted with a cluster{} block added: NATS does not migrate standalone JS state into a clustered
// meta, so the meta forms and the streams orphan (test/d9 TestD9Matrix/GrowStandaloneRestartWedgesJS).
// That migration hazard is a JETSTREAM fact, so the sites that warn about it pair this with
// HasJetStream() rather than assuming it.
func (o *Ownership) IsStandaloneTopology() bool {
	return !o.IsClusteredTopology()
}

// ClientListen harvests the client listen address from the parsed conf: install.sh writes
// host + port (NOT a `listen` key), so this joins them; an explicit `listen` wins if present.
func (o *Ownership) ClientListen() string {
	if l, ok := o.Parsed["listen"].(string); ok && l != "" {
		return l
	}
	host, _ := o.Parsed["host"].(string)
	port := ""
	switch p := o.Parsed["port"].(type) {
	case int64:
		port = fmt.Sprintf("%d", p)
	case int:
		port = fmt.Sprintf("%d", p)
	case string:
		port = p
	}
	if host != "" && port != "" {
		return host + ":" + port
	}
	return ""
}
