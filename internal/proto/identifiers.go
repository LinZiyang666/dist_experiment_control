package proto

import (
	"fmt"
	"regexp"
)

// Identifier validation per architecture B.5 + requirements §7.4 / §7.5.
//
// sid: [a-z0-9-]{1,32}, leading [a-z], no trailing '-', not reserved,
//      no reserved prefix.
// nid: same character class; uniqueness is per-session, enforced by tetherd.
// actor: NATS user nkey public key, base32 with leading 'U', 56 chars
//        (1 prefix + 32 byte key + 2 byte CRC = 35 bytes → 56 base32 chars).

var (
	idCharset = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
	// clusterNodeCharset: first char MUST be alphanumeric/underscore (NOT '-') so an option-like
	// node_id ('-brk-a', '--help') cannot be rendered into a copy-paste CLI command and parsed as a
	// flag by Cobra (External-review re-review). 1-63 chars total.
	clusterNodeCharset = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_-]{0,62}$`)
	actorPattern       = regexp.MustCompile(`^U[A-Z2-7]{55}$`)
	// instanceCharset is DELIBERATELY NOT idCharset. idCharset is shared with
	// ValidateSID, so widening or reusing it couples two contracts that must be
	// able to evolve independently — and an instance id has different needs: it
	// is fixed-width, machine-generated, and never typed by an operator.
	// 26 lowercase base32 chars = 128 bits of crypto/rand, matching the
	// lowercase-ULID alphabet already used for pids in internal/proc.
	instanceCharset = regexp.MustCompile(`^[0-9a-z]{26}$`)
)

var sidReserved = map[string]struct{}{
	"default": {}, "system": {}, "admin": {}, "global": {}, "all": {}, "*": {},
}

var sidReservedPrefixes = []string{"system-", "ctl-"}

// ValidateSID returns nil iff s is a syntactically valid session id.
// Uniqueness is checked separately by tetherd against the sessions table.
func ValidateSID(s string) error {
	if !idCharset.MatchString(s) {
		return fmt.Errorf("sid %q: must match [a-z0-9-]{1,32}", s)
	}
	if !isLowerAlpha(s[0]) {
		return fmt.Errorf("sid %q: must start with [a-z]", s)
	}
	if s[len(s)-1] == '-' {
		return fmt.Errorf("sid %q: must not end with '-'", s)
	}
	if _, bad := sidReserved[s]; bad {
		return fmt.Errorf("sid %q: reserved", s)
	}
	for _, p := range sidReservedPrefixes {
		if len(s) >= len(p) && s[:len(p)] == p {
			return fmt.Errorf("sid %q: prefix %q is reserved", s, p)
		}
	}
	return nil
}

// ValidateNID applies the per-session node-id syntax from architecture B.5:
// `[a-z0-9-]{1,32}`. No leading-letter or no-trailing-dash rule (those are
// sid-only, per requirements §7.4). Uniqueness is per session (sid, nid) and
// enforced by tetherd.
func ValidateNID(s string) error {
	if !idCharset.MatchString(s) {
		return fmt.Errorf("nid %q: must match [a-z0-9-]{1,32}", s)
	}
	return nil
}

// ValidateInstanceID validates NodeRegisterReq.InstanceID — the per-process-run
// identifier a broker uses to tell "my own agent reconnecting" from "a second
// process presenting the same credential".
//
// It is CLIENT-SUPPLIED (like the nid in the CONNECT name), so it is validated
// fail-closed before it can reach a log line, a decision or a reply: no NUL, no
// newline, no length bomb, no NATS metacharacter. It is NOT an isolation
// boundary and NOT attestation — every clone of one image presents the same
// nkey and the same fingerprint, so an instance id is a routing and
// correctness token only.
//
// The empty string is NOT valid here; callers treat absence (a pre-feature
// agent) separately, before validating.
func ValidateInstanceID(s string) error {
	if !instanceCharset.MatchString(s) {
		return fmt.Errorf("instance_id %q: must match [0-9a-z]{26}", s)
	}
	return nil
}

// Lease-name grammar for cloned-credential instances.
//
// A nid is a per-session SLOT whose occupancy is a lease. When several
// processes present the same baked credential, the first to arrive holds the
// configured basename and the rest are assigned `<basename>-NN`.
//
// This lives in the identifier layer, not in internal/node, because three
// layers need to READ the grammar — the auth path (to authorize a suffixed
// reconnect), the agent (to know whether it is running under a lease) and the
// broker — while only the broker needs the DB-backed allocation itself.
const (
	// LeaseSuffixWidth is the zero-padded width of a lease suffix ("-02").
	//
	// Zero-padding is NOT cosmetic. Eight production sites order by nid as
	// TEXT, and the load-bearing one is the /sub proxy list rendered to every
	// subscriber, followed by the online-node ordering that drives proxy
	// allocation. Unpadded, "-10" sorts before "-2" and that document order
	// changes as soon as a fleet passes nine instances.
	LeaseSuffixWidth = 2
	// LeaseSuffixLen is the total character cost of a suffix ("-NN").
	LeaseSuffixLen = 1 + LeaseSuffixWidth
	// MaxLeaseSuffix is the highest suffix expressible at LeaseSuffixWidth.
	MaxLeaseSuffix = 99
	// FirstLeaseSuffix is where suffixing starts: the basename holder is
	// conceptually "01" and never carries a suffix, so the first contested
	// arrival gets "-02".
	FirstLeaseSuffix = 2
	// MaxLeaseBasenameLen is the longest basename that can still be suffixed
	// inside ValidateNID's 32-character budget.
	//
	// Q2: neither widen nor truncate. Widening ValidateNID is wrong because
	// idCharset is shared with ValidateSID — it would silently widen the
	// session-id contract too — and because a >32-char nid is unaddressable
	// from any N-1 broker or ctl. Truncating is worse than refusing: two
	// distinct 30-char basenames would collapse to one prefix and silently
	// merge two unrelated device families into a single lease namespace.
	MaxLeaseBasenameLen = 32 - LeaseSuffixLen
)

// SplitLeaseName decomposes a nid into its basename and lease suffix.
//
// It is strict on purpose: anything that is not exactly `<basename>-<NN>` with
// NN zero-padded to LeaseSuffixWidth and >= FirstLeaseSuffix reports
// leased=false. The verdict decides whether a node is treated as ephemeral
// (excluded from fleet upgrades, ineligible for proxy egress), and
// mis-classifying an operator's own device as ephemeral is worse than failing
// to recognize a lease.
func SplitLeaseName(nid string) (basename string, suffix int, leased bool) {
	if len(nid) < LeaseSuffixLen+1 {
		return nid, 0, false
	}
	cut := len(nid) - LeaseSuffixLen
	if nid[cut] != '-' {
		return nid, 0, false
	}
	n := 0
	for i := cut + 1; i < len(nid); i++ {
		c := nid[i]
		if c < '0' || c > '9' {
			return nid, 0, false
		}
		n = n*10 + int(c-'0')
	}
	if n < FirstLeaseSuffix {
		return nid, 0, false
	}
	return nid[:cut], n, true
}

// LeaseNameFor renders the lease name for a basename and suffix.
func LeaseNameFor(basename string, suffix int) string {
	return fmt.Sprintf("%s-%0*d", basename, LeaseSuffixWidth, suffix)
}

// BasenameOf returns the configured basename behind a nid, whether or not it
// currently carries a lease suffix.
//
// Callers that must anchor on the operator's CONFIGURED identity — the upgrade
// marker's commit proof, the agent_evicted matcher, the co-located-agent
// resolution — use this rather than the routing name, because a lease name is
// transient and comparing against it would strand a marker or miss an eviction.
func BasenameOf(nid string) string {
	base, _, _ := SplitLeaseName(nid)
	return base
}

// ValidateClusterNodeID validates a cluster broker node_id (== raft ServerID == §6.5 nats_server_id).
// Charset `[A-Za-z0-9_-]{1,63}` (External-review SEC-MAJOR-1 / F13): it rejects every injection-shaped
// character — shell metacharacters, whitespace, newlines, path separators ('/', '.'), and NUL — that
// would be dangerous when the node_id is rendered verbatim into operator command lines, a filesystem
// path, or nats.conf. It is intentionally more permissive than ValidateNID on case (a broker node_id
// is a deployment-chosen server name, not a per-session leaf nid) but equally fail-closed on danger.
func ValidateClusterNodeID(s string) error {
	if !clusterNodeCharset.MatchString(s) {
		return fmt.Errorf("cluster node_id %q: must match [A-Za-z0-9_][A-Za-z0-9_-]{0,62} "+
			"(no leading '-' / option-like ids, no shell metacharacters, whitespace, '/', '.', or NUL)", s)
	}
	return nil
}

// ValidateActorToken checks the syntactic shape of a NATS user nkey public key.
// Authoritative semantics (does this key actually exist? has it presented a
// valid signature?) are enforced by NATS during CONNECT.
func ValidateActorToken(s string) error {
	if !actorPattern.MatchString(s) {
		return fmt.Errorf("actor %q: must match U[A-Z2-7]{55}", s)
	}
	return nil
}

func isLowerAlpha(b byte) bool { return b >= 'a' && b <= 'z' }
