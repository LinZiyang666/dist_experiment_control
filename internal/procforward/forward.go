// Package procforward is the broker's forwarding half of the reliable process
// protocol: it reads the envelope of a process-resume request (version,
// call_id, op) and re-emits the body with the authenticated actor stamped
// in, while every field it does not know about survives.
//
// Why raw overlay and not a struct round trip: decoding into
// proto.ProcessResumeReq and re-marshalling silently drops any field the
// broker's release does not know. That is exactly how the legacy exec
// forwarder loses a newer client's fields on the way to a newer agent
// (docs/reviews/hot-update-handoff-protocol.md §3.3 #1) — and the reason the
// reliable family got its own verb in the first place. A broker must be a
// pass-through for the N+1 wire; it only owns the actor stamp.
//
// The stamp deletes every key that equals "actor_fp" case-insensitively at
// the top level and inside "exec": encoding/json matches struct keys
// case-insensitively and takes the last duplicate in input order, so a
// client could otherwise smuggle "Actor_FP" past a stamp that only wrote the
// canonical key. After Stamp there is exactly one actor key in each object,
// and it is the broker's.
// origin: docs/reviews/hot-update-reliable-exec-plan.md §2.2.
package procforward

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	"github.com/LinZiyang666/tether/internal/proto"
)

// MaxRequestBytes is the broker's request body limit for this verb. It is
// the wire constant (proto.ProcessRequestMaxBytes) the client enforces
// before publishing; it matches the registry's intent-hash limit and NATS'
// default max_payload, so a bigger body could never be forwarded anyway.
const MaxRequestBytes = proto.ProcessRequestMaxBytes

var (
	ErrNotObject       = errors.New("procforward: request body is not a JSON object")
	ErrExecShape       = errors.New("procforward: exec is not a JSON object")
	ErrCallID          = errors.New("procforward: call_id is not an unsigned integer")
	ErrTooLarge        = errors.New("procforward: request body exceeds the limit")
	ErrEnvelopeKeyCase = errors.New("procforward: envelope key spelled in a case variant")
)

// envelopeKeys are the keys the broker READS to route a request; every one
// must be spelled exactly like this, because the agent's struct decoder folds
// case. actor_fp is not among them: the stamp OWNS that key and rewrites it,
// deleting every case variant at both levels, so a variant there is stripped
// rather than refused (TestStampPreservesUnknownFieldsAndOwnsTheActorKey).
var envelopeKeys = [...]string{"version", "call_id", "op", "exec"}

// Envelope is the part of a request the broker acts on. Everything else is
// opaque and preserved.
type Envelope struct {
	CallID  uint64
	Version int
	Op      string

	raw     map[string]json.RawMessage
	exec    map[string]json.RawMessage
	hasExec bool
}

// Parse reads only version, call_id and op (and whether exec is a non-null
// object). A non-object body, a non-object exec, or a call_id that is not an
// unsigned integer is an error; a missing call_id parses as 0, which the
// broker reports back so the client can tell "malformed" from "refused".
func Parse(body []byte) (*Envelope, error) {
	if len(body) > MaxRequestBytes {
		return nil, ErrTooLarge
	}
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil || raw == nil {
		return nil, ErrNotObject
	}
	if dec.More() {
		return nil, ErrNotObject
	}
	// The envelope keys are read here by their exact spelling, but the agent
	// decodes the forwarded body into a struct with encoding/json, which
	// matches keys case-insensitively. A body spelling a routing key in any
	// other case ("Exec", "OP", "Call_ID") would therefore be admitted on one
	// value and executed on another — for "Exec" the agent would even decode
	// a client-chosen exec.actor_fp the stamp never touched. Such a body has
	// no honest reading; refuse it. origin: S1 internal review protocol-V2.
	for key := range raw {
		for _, envelopeKey := range envelopeKeys {
			if key != envelopeKey && strings.EqualFold(key, envelopeKey) {
				return nil, ErrEnvelopeKeyCase
			}
		}
	}
	env := &Envelope{raw: raw}
	if v, ok := raw["call_id"]; ok {
		// json.Number also accepts a quoted "7"; require a bare number token.
		tok := bytes.TrimSpace(v)
		if len(tok) == 0 || tok[0] < '0' || tok[0] > '9' {
			return nil, ErrCallID
		}
		var n json.Number
		if err := json.Unmarshal(tok, &n); err != nil || strings.ContainsAny(n.String(), "-.eE") {
			return nil, ErrCallID
		}
		id, err := parseUint(n.String())
		if err != nil {
			return nil, ErrCallID
		}
		env.CallID = id
	}
	if v, ok := raw["version"]; ok {
		var n json.Number
		if err := json.Unmarshal(v, &n); err == nil {
			if i, err := n.Int64(); err == nil && i >= 0 && i <= 1<<31 {
				env.Version = int(i)
			}
		}
	}
	if v, ok := raw["op"]; ok {
		_ = json.Unmarshal(v, &env.Op) // a non-string op stays "" and is refused as unsupported
	}
	if v, ok := raw["exec"]; ok && string(bytes.TrimSpace(v)) != "null" {
		var exec map[string]json.RawMessage
		if err := json.Unmarshal(v, &exec); err != nil || exec == nil {
			return nil, ErrExecShape
		}
		env.exec, env.hasExec = exec, true
	}
	return env, nil
}

// Stamp writes the authenticated actor fingerprint into the top-level object
// and, when present, into exec, removing every case-variant of the key first.
// Unknown fields survive as values; keys are re-sorted by json.Marshal and
// RawMessage values are emitted compactly, so the guarantee is semantic
// (same fields, same values), not byte-for-byte.
func (e *Envelope) Stamp(actorFP string) ([]byte, error) {
	fp, err := json.Marshal(actorFP)
	if err != nil {
		return nil, err
	}
	deleteKeyFold(e.raw, "actor_fp")
	e.raw["actor_fp"] = json.RawMessage(fp)
	if e.hasExec {
		deleteKeyFold(e.exec, "actor_fp")
		e.exec["actor_fp"] = json.RawMessage(fp)
		execBody, err := json.Marshal(e.exec)
		if err != nil {
			return nil, err
		}
		e.raw["exec"] = json.RawMessage(execBody)
	}
	return json.Marshal(e.raw)
}

func deleteKeyFold(m map[string]json.RawMessage, key string) {
	for k := range m {
		if strings.EqualFold(k, key) {
			delete(m, k)
		}
	}
}

func parseUint(s string) (uint64, error) {
	if s == "" || len(s) > 20 {
		return 0, ErrCallID
	}
	var n uint64
	for _, c := range []byte(s) {
		if c < '0' || c > '9' {
			return 0, ErrCallID
		}
		d := uint64(c - '0')
		if n > (^uint64(0)-d)/10 {
			return 0, ErrCallID
		}
		n = n*10 + d
	}
	return n, nil
}
