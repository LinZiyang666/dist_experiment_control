package procrequest

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// Secret is the registry's HMAC key. It is a distinct type so that every
// formatted rendering of a State (%v, %+v, %s, %x, %q) prints "[redacted]"
// instead of the key: an error string, a Warn line or a debug dump that
// carried the key would let anyone who can read the agent log forge leases.
// JSON encoding is unchanged (encoding/json ignores Stringer and encodes a
// named byte slice as base64), so the sealed handoff snapshot still carries
// the real bytes — that snapshot is the ONLY place the key may travel.
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §4 item 9.
type Secret []byte

func (Secret) String() string { return "[redacted]" }

// Format implements fmt.Formatter so that even %x/%X/%q, which bypass
// Stringer for byte slices, print the redaction marker.
func (Secret) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte("[redacted]")) }

// State contains a secret. Only an authenticated, private handoff bundle may
// carry it. Restoring it does not transfer process or byte-stream ownership.
type State struct {
	Version  int           `json:"version"`
	Instance string        `json:"instance"`
	BootID   string        `json:"boot_id"`
	Secret   Secret        `json:"secret"`
	Limit    int           `json:"limit"`
	TTL      time.Duration `json:"ttl"`
	Records  []Record      `json:"records"`
}

// CurrentBootID reports the kernel boot identity every lease and every
// exported process table is bound to. Exported so the image layer can bind
// its own manifest to the same identity instead of parsing /proc itself.
func CurrentBootID() (string, error) { return currentBootID() }

// ValidToken reports whether s is a bounded [A-Za-z0-9_-] identifier — the
// shape every task/session/instance id in this table has. Exported so the
// image layer can apply the same predicate to ids it carries beside the
// table (revocation SIDs).
func ValidToken(s string) bool { return validToken(s) }

// Freeze refuses uncompleted creation. The runtime must hold its admission
// barrier until every resource domain has frozen and exported successfully.
// The refusal wraps ErrBusy and names the starting pids: a creation whose
// terminal was refused stays "starting" for good and keeps a node deferred
// until restart, and the operator needs the pid to chase it.
func (r *Registry) Freeze() (State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var starting []string
	for _, record := range r.records {
		if record.Phase == "starting" {
			starting = append(starting, record.PID)
		}
	}
	if len(starting) > 0 {
		sort.Strings(starting)
		return State{}, fmt.Errorf("%w: %d starting record(s): %s", ErrBusy, len(starting), strings.Join(starting, ","))
	}
	r.frozen = true
	state := State{Version: 1, Instance: r.instance, BootID: r.bootID,
		Secret: append(Secret(nil), r.secret...), Limit: r.limit, TTL: r.ttl}
	for _, record := range r.records {
		state.Records = append(state.Records, copyRecord(record))
	}
	sort.Slice(state.Records, func(i, j int) bool { return state.Records[i].PID < state.Records[j].PID })
	return state, nil
}

func (r *Registry) Thaw() {
	r.mu.Lock()
	r.frozen = false
	r.mu.Unlock()
}

// Frozen reports whether the table currently refuses every mutation.
func (r *Registry) Frozen() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.frozen
}

// Instance reports the identity every lease this registry signs is bound to.
func (r *Registry) Instance() string { return r.instance }

// Restore validates the whole table before returning an owner. Success is
// still frozen; only the runtime may activate it after all domains restore.
// The real boot clock is used; a runtime that shares an injected clock with
// its registry restores through RestoreWithClock.
func Restore(state State) (*Registry, error) {
	return RestoreWithClock(state, nil)
}

// RestoreWithClock is Restore with the runtime's injected boot clock (nil
// selects the real CLOCK_BOOTTIME). A restored registry that measured lease
// expiry on a different clock than the runtime measures abandoned-terminal
// grace and tombstone TTL on would silently reintroduce the two-clock
// interplay NewWithClock exists to prevent.
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §4 item 7.
func RestoreWithClock(state State, clock func() (int64, error)) (*Registry, error) {
	boot, err := currentBootID()
	if err != nil {
		return nil, err
	}
	if clock == nil {
		clock = bootNow
	}
	if state.Version != 1 || !validToken(state.Instance) || state.BootID != boot ||
		len(state.Secret) != sha256.Size || state.Limit < 1 || state.Limit > MaxLimit ||
		len(state.Records) > state.Limit || state.TTL <= 0 || state.TTL > MaxTTL {
		return nil, fmt.Errorf("process request: incompatible state")
	}
	r := &Registry{instance: state.Instance, bootID: boot, secret: append([]byte(nil), state.Secret...),
		limit: state.Limit, ttl: state.TTL, now: clock, frozen: true, records: make(map[requestKey]Record)}
	pids := make(map[string]bool)
	for _, record := range state.Records {
		// A future kind cannot be interpreted as exec: the runtime must know
		// which domain owns its resources and retained terminal receipt.
		// origin: hot-update S4 plan §5.1 / T17
		if record.Kind != "" && record.Kind != proto.ProcessKindPTY {
			return nil, fmt.Errorf("process request: unknown restored task kind")
		}
		key, err := r.validate(scopeOf(record.Handle.Lease), record.Handle)
		if err != nil || !validHex(record.IntentHash, sha256.Size) || !validToken(record.PID) ||
			pids[record.PID] || record.AttachEpoch == 0 || len(record.Inbox) == 0 || len(record.Inbox) > 512 {
			return nil, fmt.Errorf("process request: invalid restored record")
		}
		if _, exists := r.records[key]; exists {
			return nil, fmt.Errorf("process request: duplicate restored identity")
		}
		switch record.Phase {
		case "running":
			if record.Terminal != nil {
				return nil, fmt.Errorf("process request: live record has a terminal result")
			}
		case "terminal", "settled":
			if record.Terminal == nil || !validTerminal(*record.Terminal) {
				return nil, fmt.Errorf("process request: invalid restored terminal result")
			}
		default:
			return nil, fmt.Errorf("process request: unsafe restored phase")
		}
		pids[record.PID] = true
		r.records[key] = copyRecord(record)
	}
	return r, nil
}
