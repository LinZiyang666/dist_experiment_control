// Pure register authority comparisons preserve the inherited creation identity.
// origin: hot-update S8 plan §6.2 / §12.1 / P14.
package upgradeauthority

import (
	"cmp"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// RequestGuard binds a register's carried authority to its routed SID and name.
// It contains no lookup, so an old request cannot learn a replacement creation.
// origin: hot-update S8 plan §6.2 / P16
func RequestGuard(sid, nid string, req proto.NodeRegisterReq) Guard {
	return Guard{SID: sid, Incarnation: req.Incarnation, InstanceID: req.InstanceID, ConfiguredNID: nid}
}

// Compare the inherited creation identity before applying a register reply.
// An old broker's empty reply remains ordinary-wire compatible only when no
// sealed image supplied an expected incarnation. Concurrent replies cannot
// replace a first learned value or borrow another request's expected identity.
// origin: hot-update S8 plan §6.2 / M17.
func AcceptIncarnation(current *atomic.Pointer[string], expected, actual string) error {
	if expected == "" {
		expected = incarnation(current)
	}
	if expected == "" && actual == "" {
		return nil
	}
	if err := Match(expected, actual); err != nil {
		return err
	}
	value := current.Load()
	if value == nil && current.CompareAndSwap(nil, &actual) {
		return nil
	}
	return Match(incarnation(current), actual)
}

func incarnation(current *atomic.Pointer[string]) string {
	if value := current.Load(); value != nil {
		return *value
	}
	return ""
}

// CheckRegister compares both typed P13 authorities before adopting a lease.
// origin: hot-update S8 plan §6.2 / M17.
func CheckRegister(current *atomic.Pointer[string], expected string, resp proto.NodeRegisterResp) error {
	if resp.Lease != nil && resp.Lease.Incarnation != resp.Incarnation {
		return Match(resp.Incarnation, resp.Lease.Incarnation)
	}
	return AcceptIncarnation(current, expected, resp.Incarnation)
}

// These control envelopes preserve captured guards across broker forwarding.
// They are neither disk records nor image records; creation material stays local.
// origin: hot-update S8 plan §6.2 / P16
// AllocationIdentity retains the original five-column allocation fence.
type AllocationIdentity struct {
	Port      int    `json:"port"`
	SID       string `json:"sid"`
	NID       string `json:"nid"`
	Name      string `json:"name"`
	TokenHash string `json:"token_hash"`
	Authority *Guard `json:"authority,omitempty"`
}

type SessionMutation struct {
	SID       string `json:"sid"`
	Authority *Guard `json:"authority,omitempty"`
}

type ProcessExit struct {
	Pid       string    `json:"pid"`
	Sid       string    `json:"sid,omitempty"`
	ExitCode  int       `json:"exit_code"`
	EndedAt   time.Time `json:"ended_at"`
	Authority *Guard    `json:"authority,omitempty"`
}

// ExitRequest carries the admission's original guard, including missed exits.
// A legacy ordinary register retains its original envelope bytes.
// origin: hot-update S8 plan §6.2 / P16
func ExitRequest(sid, nid string, req proto.NodeRegisterReq, pid string, rc int, when time.Time) ProcessExit {
	p := ProcessExit{Pid: pid, Sid: sid, ExitCode: rc, EndedAt: when}
	if req.Incarnation != "" {
		g := RequestGuard(sid, nid, req)
		p.Authority = &g
	}
	return p
}

// MutationPredicate permits the initial legacy tombstone envelope, but never
// lets a carried proof change SID or learn an empty destructive incarnation.
// origin: hot-update S8 plan §6.2 / P16
func MutationPredicate(reader *sql.DB, p SessionMutation, literal func(string) (string, error)) (string, error) {
	if p.Authority == nil {
		return "1", nil
	}
	if p.Authority.SID != p.SID || p.Authority.Incarnation == "" {
		return "", incarnationMismatch()
	}
	return GuardPredicate(reader, *p.Authority, literal)
}

// Request IDs are pure; the leader planner retains the write boundary.
// origin: hot-update S8 plan §6.2 / P16
func ReconcileRequestID(sid, nid string, req proto.NodeRegisterReq) string {
	h := sha256.New()
	for _, s := range []string{"reconcile", sid, nid, req.BootID} {
		writeSeg(h, s)
	}
	// origin: hot-update S8 plan §6.2 / P16
	if req.Incarnation != "" {
		writeSeg(h, req.Incarnation)
		writeSeg(h, req.InstanceID)
	}
	// Total order over ALL hashed fields (not just PID/Port) so the digest is stable
	// even when the agent reports duplicate-key entries in a map-randomized order
	// (review M5: a PID/Port-only sort.Slice is unstable for equal keys, which would
	// derive different keys for the same logical request on retry).
	lps := append([]proto.LocalProcess(nil), req.LocalProcesses...)
	slices.SortStableFunc(lps, func(a, b proto.LocalProcess) int {
		return cmp.Or(strings.Compare(a.PID, b.PID), strings.Compare(a.State, b.State), cmp.Compare(a.StartTimeTicks, b.StartTimeTicks), strings.Compare(rcKey(a.RC), rcKey(b.RC)))
	})
	for _, lp := range lps {
		for _, s := range []string{lp.PID, lp.State, strconv.FormatInt(lp.StartTimeTicks, 10), rcKey(lp.RC)} {
			writeSeg(h, s)
		}
	}
	lpo := append([]proto.LocalPort(nil), req.LocalPorts...)
	slices.SortStableFunc(lpo, func(a, b proto.LocalPort) int {
		return cmp.Or(cmp.Compare(a.Port, b.Port), cmp.Compare(a.LocalPort, b.LocalPort), strings.Compare(a.Name, b.Name), strings.Compare(a.TokenHash, b.TokenHash))
	})
	for _, p := range lpo {
		for _, s := range []string{strconv.Itoa(p.Port), p.Name, strconv.Itoa(p.LocalPort), p.TokenHash} {
			writeSeg(h, s)
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// rcKey renders an optional rc for hashing/sorting: "" when nil, else the decimal.
func rcKey(rc *int) string {
	if rc == nil {
		return ""
	}
	return strconv.Itoa(*rc)
}

// writeSeg writes a NUL-terminated segment so concatenation is unambiguous (no
// "ab"+"c" == "a"+"bc" collision).
func writeSeg(h interface{ Write([]byte) (int, error) }, s string) {
	_, _ = h.Write([]byte(s))
	_, _ = h.Write([]byte{0})
}
