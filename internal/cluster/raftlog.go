package cluster

import (
	"io"
	"log"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
)

// Batch-A A15: give raft a voice.
//
// node.go set c.LogOutput = io.Discard with the comment "D1: discard raft's
// internal chatter; D3 wires logging". D3 never wired it. internal/cluster is
// ~5,300 lines with five log statements in total, so when a cluster misbehaves
// the operator has: no election trace, no heartbeat failures, no snapshot
// activity, no failed-to-contact — and `cluster export-incident` bundles alerts,
// membership and audit but no raft narrative at all. Raft is the hardest part of
// this system and was the only part with nothing to read afterwards.
//
// Volume is bounded rather than assumed harmless. raft only starts follower
// replication for servers OTHER than itself, so a force-single N=1 broker emits
// none of the repeated "failed to heartbeat" lines at all. But that is a
// property of the CURRENT configuration, not a guarantee — a lingering
// unreachable voter (this fleet has had one) would produce a steady stream at
// however fast the dial timeout plus backoff allows. Rather than reason about
// which is true on any given host, messages sharing a de-duplication key (message + its string args,
// see dedupKey) are collapsed into one line per dedupWindow with a count. The cost is bounded either way.
//
// DEBUG/TRACE stay dropped unless the operator asked for debug logging: those
// are the genuinely high-rate levels and they are not what a post-incident read
// needs.

const raftLogDedupWindow = 30 * time.Second

// raftLogBridge adapts hclog (raft's logger interface) onto slog.
type raftLogBridge struct {
	logger *slog.Logger
	debug  bool
	name   string
	with   []any

	mu   sync.Mutex
	seen map[string]*dedupState
}

type dedupState struct {
	last       time.Time
	suppressed int
}

// NewRaftLogger returns an hclog.Logger that forwards WARN and above to logger.
// When debug is true, DEBUG/INFO are forwarded as well.
func NewRaftLogger(logger *slog.Logger, debug bool) hclog.Logger {
	return &raftLogBridge{logger: logger, debug: debug, seen: map[string]*dedupState{}}
}

// dedupKeyLimit bounds the seen map. raft's message set is small and closed, but
// the key includes string args (peer ids), so a pathological configuration churn
// could otherwise grow it without bound. On overflow the map is dropped: the
// worst case is one extra line per message, which is the safe direction.
const dedupKeyLimit = 512

// dedupKey combines the message with its STRING-valued args.
//
// Batch-A review M7: keying on the message alone swallowed every unreachable
// peer after the first, because raft reports them as one constant message with
// the identity in the args ("failed to heartbeat to", peer=...). During an
// incident that reads as "one peer is down" when several are — worse than thin
// logging, it is misleading logging.
//
// Numeric args (term, index, duration) are deliberately EXCLUDED: they change on
// every occurrence, and including them would turn de-duplication off entirely,
// which is the failure mode in the other direction.
//
// External review B2: the first version type-asserted `a.(string)`, which is
// false for raft's own identity types. raft passes peers as raft.ServerAddress
// and raft.ServerID — both DEFINED string types, so the assertion missed every
// one of them and two unreachable peers collapsed onto one key. The peer
// identity is precisely the field that must separate them, so the de-duplication
// silently did the opposite of its purpose: it hid the second failing node.
//
// Matching on reflect.Kind covers any defined string type, present or future,
// without this package needing to import raft or enumerate its types.
func dedupKey(msg string, args []any) string {
	var sb strings.Builder
	sb.WriteString(msg)
	for _, a := range args {
		if s, ok := stringLike(a); ok {
			sb.WriteByte(0x1f)
			sb.WriteString(s)
		}
	}
	return sb.String()
}

// stringLike reports whether a is a string or any defined type whose underlying
// kind is string (raft.ServerAddress, raft.ServerID, ...), returning its value.
func stringLike(a any) (string, bool) {
	if s, ok := a.(string); ok {
		return s, true
	}
	if a == nil {
		return "", false
	}
	rv := reflect.ValueOf(a)
	if rv.Kind() == reflect.String {
		return rv.String(), true
	}
	// NO fmt.Stringer fallback. It was here as "cheap insurance" for identity
	// types that render through a method, and external review R3 showed what it
	// actually bought: time.Duration is a Stringer, so a `duration` argument
	// joined the key and `heartbeat timeout` became `heartbeat timeout\x1f1s`.
	// A key that varies with a timing value dedupes nothing — the exact defect
	// (M7/F-05) this dedup key was widened to fix, reintroduced from the other
	// side, and directly contradicting the comment above claiming term/index/
	// duration are deliberately excluded.
	//
	// reflect string-kind already covers raft.ServerAddress and raft.ServerID,
	// which are the identities that matter. A future non-string identity type
	// should be handled explicitly, not swept in by an interface that numeric
	// types also satisfy.
	return "", false
}

// allow reports whether this message should be emitted now, and how many
// identical messages were suppressed since the last emission.
func (b *raftLogBridge) allow(msg string) (bool, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if len(b.seen) >= dedupKeyLimit {
		b.seen = map[string]*dedupState{}
	}
	st, ok := b.seen[msg]
	if !ok {
		b.seen[msg] = &dedupState{last: now}
		return true, 0
	}
	if now.Sub(st.last) >= raftLogDedupWindow {
		n := st.suppressed
		st.last = now
		st.suppressed = 0
		return true, n
	}
	st.suppressed++
	return false, 0
}

func (b *raftLogBridge) emit(level slog.Level, msg string, args ...any) {
	if b.logger == nil {
		return
	}
	ok, suppressed := b.allow(dedupKey(msg, args))
	if !ok {
		return
	}
	all := make([]any, 0, len(b.with)+len(args)+4)
	all = append(all, b.with...)
	all = append(all, args...)
	all = append(all, "component", "raft")
	if suppressed > 0 {
		all = append(all, "suppressed_since_last", suppressed)
	}
	b.logger.Log(nil, level, msg, all...) //nolint:staticcheck // nil ctx is the documented "no context" form
}

func (b *raftLogBridge) Log(level hclog.Level, msg string, args ...any) {
	switch level {
	case hclog.Error:
		b.emit(slog.LevelError, msg, args...)
	case hclog.Warn:
		b.emit(slog.LevelWarn, msg, args...)
	case hclog.Info:
		if b.debug {
			b.emit(slog.LevelInfo, msg, args...)
		}
	case hclog.Debug, hclog.Trace:
		if b.debug {
			b.emit(slog.LevelDebug, msg, args...)
		}
	}
}

func (b *raftLogBridge) Trace(msg string, args ...any) { b.Log(hclog.Trace, msg, args...) }
func (b *raftLogBridge) Debug(msg string, args ...any) { b.Log(hclog.Debug, msg, args...) }
func (b *raftLogBridge) Info(msg string, args ...any)  { b.Log(hclog.Info, msg, args...) }
func (b *raftLogBridge) Warn(msg string, args ...any)  { b.Log(hclog.Warn, msg, args...) }
func (b *raftLogBridge) Error(msg string, args ...any) { b.Log(hclog.Error, msg, args...) }

func (b *raftLogBridge) IsTrace() bool { return b.debug }
func (b *raftLogBridge) IsDebug() bool { return b.debug }
func (b *raftLogBridge) IsInfo() bool  { return b.debug }
func (b *raftLogBridge) IsWarn() bool  { return true }
func (b *raftLogBridge) IsError() bool { return true }

func (b *raftLogBridge) ImpliedArgs() []any { return b.with }

func (b *raftLogBridge) With(args ...any) hclog.Logger {
	next := &raftLogBridge{logger: b.logger, debug: b.debug, name: b.name, seen: map[string]*dedupState{}}
	next.with = append(append([]any{}, b.with...), args...)
	return next
}

func (b *raftLogBridge) Name() string { return b.name }

func (b *raftLogBridge) Named(name string) hclog.Logger {
	n := name
	if b.name != "" {
		n = b.name + "." + name
	}
	next := &raftLogBridge{logger: b.logger, debug: b.debug, name: n, seen: map[string]*dedupState{}}
	next.with = append([]any{}, b.with...)
	return next
}

func (b *raftLogBridge) ResetNamed(name string) hclog.Logger {
	next := &raftLogBridge{logger: b.logger, debug: b.debug, name: name, seen: map[string]*dedupState{}}
	next.with = append([]any{}, b.with...)
	return next
}

func (b *raftLogBridge) SetLevel(hclog.Level) {}

func (b *raftLogBridge) GetLevel() hclog.Level {
	if b.debug {
		return hclog.Debug
	}
	return hclog.Warn
}

func (b *raftLogBridge) StandardLogger(*hclog.StandardLoggerOptions) *log.Logger {
	return log.New(io.Discard, "", 0)
}

func (b *raftLogBridge) StandardWriter(*hclog.StandardLoggerOptions) io.Writer { return io.Discard }
