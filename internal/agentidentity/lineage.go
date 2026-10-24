package agentidentity

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// Process lineage and lease validation take values without depending on a running agent.
// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
// InstanceIDLen is the character length of an instance id: 16 crypto/rand bytes
// rendered as unpadded lowercase base32.
const InstanceIDLen = 26

// lowerBase32 renders bytes with the standard base32 alphabet lowercased and
// without padding, giving [0-9a-z] — subject-safe, and the same character class
// the lowercase ULIDs in internal/proc already use for pids.
var lowerBase32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// MintInstanceID returns this process's instance id, adopting an inherited
// lineage when one is present and generating a fresh one otherwise.
//
// The inherited value is validated before use: it arrives through the
// environment, which any parent process can set.
func MintInstanceID() (string, error) {
	// NO PROCESS-WIDE CACHE. A round-2 revision memoized this so a boot-time
	// caller and Agent.New would agree; that made every Agent constructed in one
	// process share an id, and two instances sharing an id read to the broker as
	// one instance reconnecting — the fan-out this increment exists to remove.
	// The boot shim no longer needs an id at all (external review F7), so the
	// question is moot and the simple reading stands.
	id, _, err := MintInstanceIDWithSource()
	return id, err
}

func MintInstanceIDWithSource() (string, bool, error) {
	v := strings.TrimSpace(os.Getenv(InstanceIDEnv))
	// CONSUME the variable: once its value is in memory it must not remain in
	// this process's environment, because managed children inherit os.Environ()
	// by default and several spawn paths deliberately leave cmd.Env nil to stay
	// byte-identical to pre-feature behaviour (remote-fs-resilience pins that).
	//
	// Unsetting here is what makes the guarantee total instead of per-call-site:
	// a child cannot inherit what the parent no longer has, so a spawn path
	// added later cannot forget the rule. Without it, a user command that starts
	// an agent (`tether run node -- bash`, then a daemon in that shell) would
	// inherit this process's lineage and the broker would read the two of them
	// as one instance reconnecting — the clone fan-out, restored through the
	// environment.
	//
	// The value is re-injected explicitly at the two syscall.Exec sites (see
	// execEnv), which is the only place it is legitimately handed forward.
	_ = os.Unsetenv(InstanceIDEnv)
	if v != "" && len(v) == InstanceIDLen && IsLowerBase32(v) {
		return v, true, nil
	}
	// A malformed inherited value is discarded rather than propagated: a bad id
	// would be rejected by the broker on every register, and silently running
	// with one is worse than starting a fresh lineage.
	id, err := NewInstanceID()
	return id, false, err
}

func NewInstanceID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("agent: mint instance id: %w", err)
	}
	return lowerBase32.EncodeToString(b[:]), nil
}

func IsLowerBase32(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			continue
		}
		return false
	}
	return true
}

// ExecEnvFor is execEnv with an explicit lineage: the re-dispatch of a held
// image uses the snapshot the bootstrap took before anything consumed the
// variables (agentboot.Image.Env), because at that point New may have failed
// between consuming them and recording them (hot-update S3 §1.7).
func ExecEnvFor(instanceID, routingNID string) []string {
	out := StripInstanceEnv(os.Environ())
	if instanceID != "" {
		out = append(out, InstanceIDEnv+"="+instanceID)
	}
	if routingNID != "" {
		out = append(out, RoutingNIDEnv+"="+routingNID)
	}
	return out
}

// LoadedLineage prefers a recorded lineage value and falls back to the ambient
// environment, which is the only source available before Agent.New has run.
func LoadedLineage(recorded *string, env string) string {
	if recorded != nil && *recorded != "" {
		return *recorded
	}
	return strings.TrimSpace(os.Getenv(env))
}

// acceptableLeaseName reports whether a broker-supplied lease name is one this
// agent may adopt.
//
// The name arrives over the network and becomes this agent's CONNECT name and
// every subject token it publishes on, so it is validated before use — the
// sibling path that restores a name from the environment already validates, and
// the LESS trusted source must not be the unchecked one. Two conditions:
// it must be a syntactically legal nid (four subject parsers re-validate it),
// and it must be a lease OF THIS AGENT'S OWN BASENAME — a broker has no business
// moving an agent onto an unrelated device's name.
func AcceptableLeaseName(basename, current, assigned string, adoptions int32) bool {
	if assigned == "" || assigned == current {
		return false
	}
	if proto.ValidateNID(assigned) != nil {
		return false
	}
	// A DIRECT LEASE OF THE CONFIGURED NAME, nothing else.
	//
	// The earlier rule compared against proto.BasenameOf(basename) so that the
	// agent would agree with a broker that folded `gpu-02` into the `gpu`
	// family. Agreeing was the wrong repair: the two sides then agreed on a name
	// in the WRONG credential family, and the disagreement simply moved to auth
	// — `gpu-03` is not covered by gpu-02's provisioning row, so the reconnect
	// is denied (external review F2). The broker no longer folds; the configured
	// name is an opaque root on both sides, and `gpu-02`'s lease is
	// `gpu-02-02`.
	base, _, leased := proto.SplitLeaseName(assigned)
	if !leased || base != basename {
		return false
	}
	// BOUND THE ADOPTIONS. Every adoption tears down and rebuilds the session,
	// so a broker that keeps assigning a different name each time would spin the
	// session loop as fast as the network allows — measured at ~700 rebuilds per
	// second, which is unbounded goroutine and fd churn dressed up as progress.
	//
	// After the cap the agent stays on the name it has and says so. That is the
	// safe direction: it is still a functioning node, and the suffix is never
	// persisted, so a restart re-competes cleanly.
	return adoptions < MaxLeaseAdoptions
}

// MaxLeaseAdoptions caps how many times one agent process will accept a new
// routing name. Three is enough for any legitimate sequence (contest, adopt,
// and one more if the assigned name was itself taken in the meantime) and small
// enough that a misbehaving or flapping broker cannot spin the session loop.
const MaxLeaseAdoptions = 3

// LeaseRefusalBackoff returns how long to wait before re-competing after a
// verdict this process cannot use, and whether the agent should stop competing
// and stay put.
//
// external review F17. A refusal retires the session (that is the F3 fix — the
// replayed subscriptions of a name this process does not hold must not survive
// the verdict), and the natural consequence is a re-connect. With nothing
// damping it, an instance meeting a full suffix space re-competes at connect
// speed forever: a full auth_callout round per attempt, from every clone in the
// image, aimed at the one broker that just told them all there is no room.
//
// The shape is exponential-with-a-floor and then a STOP. Stopping is the honest
// end state: the answer is not going to change until an operator raises
// MaxInstancesPerBasename or something else exits, and an agent that keeps
// asking is a load generator, not a retry.
func LeaseRefusalBackoff(refusals int32) (wait time.Duration, giveUp bool) {
	switch {
	case refusals <= 0:
		return 0, false
	case refusals >= MaxLeaseRefusals:
		return 0, true
	default:
		// 2s, 4s, 8s, … capped — the same shape as the register backoff, so the
		// two do not beat against each other.
		wait = time.Duration(1<<uint(refusals-1)) * 2 * time.Second
		if wait > LeaseRefusalBackoffMax {
			wait = LeaseRefusalBackoffMax
		}
		return wait, false
	}
}

// StripInstanceEnv removes the lineage variables from an environment.
//
// This is applied to the environment handed to MANAGED CHILD PROCESSES. Without
// it, a user command that itself starts a tether agent — `tether run node --
// bash`, then launching an agent in that shell — would inherit this agent's
// lineage, and the broker would read the two of them as one instance
// reconnecting and hand them a single name. That is the fan-out defect,
// restored through the environment.
func StripInstanceEnv(env []string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		if strings.HasPrefix(kv, InstanceIDEnv+"=") || strings.HasPrefix(kv, RoutingNIDEnv+"=") {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// Lineage environment names remain process-only; they must never become persisted identity.
const InstanceIDEnv = "TETHER_INSTANCE_ID"
const RoutingNIDEnv = "TETHER_ROUTING_NID"
const MaxLeaseRefusals = 5
const LeaseRefusalBackoffMax = 60 * time.Second

// Process identity readers stay with lineage and lease identity, independent
// of transaction control and runtime ownership.
// origin: hot-update S8 internal review FIX-AGENTSLIM.
// ReadBootID returns the Linux per-boot UUID, or "" on non-Linux / error.
// Used in P8's reconciliation (architecture G.1) for PID-reuse detection;
// recorded already in P2 so the column is never NULL once populated.
func ReadBootID() string {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// ReadStartTimeTicks returns /proc/<pid>/stat field 22 (the kernel-
// stamped boot tick when this process started), the third leg of the
// architecture G.1 PID-reuse triple. Returns 0 on any failure
// (non-Linux, /proc not mounted, pid disappeared between fork and
// read, etc.) — the broker treats 0 as "agent could not capture",
// which falls back to the no-triple-check accept path. Parsing the
// stat line correctly requires honoring the comm (bytes 2..end-of-")")
// because comm can contain spaces or close-parens; we slice from the
// LAST ')' so a process named "weird ) name" parses cleanly.
func ReadStartTimeTicks(osPID int) (int64, error) {
	if osPID <= 0 {
		return 0, fmt.Errorf("agent: invalid os pid %d", osPID)
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", osPID))
	if err != nil {
		return 0, err
	}
	line := string(b)
	rp := strings.LastIndexByte(line, ')')
	if rp < 0 || rp+1 >= len(line) {
		return 0, fmt.Errorf("agent: malformed /proc/%d/stat", osPID)
	}
	rest := strings.Fields(line[rp+1:])
	// rest[0] is field 3 (state); field 22 is rest[19].
	if len(rest) < 20 {
		return 0, fmt.Errorf("agent: /proc/%d/stat too short (%d fields after comm)",
			osPID, len(rest))
	}
	ticks, err := strconv.ParseInt(rest[19], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("agent: parse start_time_ticks: %w", err)
	}
	return ticks, nil
}
