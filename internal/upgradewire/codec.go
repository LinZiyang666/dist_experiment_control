package upgradewire

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Bound allocation before decoding. The envelope is control metadata, never an
// image or sealed state. A peer cannot turn one admin connection into an
// unbounded reader, nor hide duplicate keys inside a grant or resource census.
// origin: hot-update S8 plan §2.2 / §7 / P13
const MaxMessageBytes = 1 << 20
const MaxCollection = 4096

func wireError(detail string) error {
	return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: detail}
}

func IsOperation(op string) bool {
	switch op {
	case OpPrepare, OpStart, OpQuery, OpRollback, OpCancel, OpArchive, OpCloseWindow, OpDescribe:
		return true
	default:
		return false
	}
}

func operationFields(op string) []string {
	switch op {
	case OpPrepare:
		return []string{"prepare"}
	case OpStart:
		return []string{"grant", "artifact"}
	case OpQuery:
		return []string{"tx_id", "query_nonce"}
	case OpDescribe:
		return []string{"query_nonce"}
	case OpRollback:
		return []string{"expected", "reverse"}
	case OpCancel, OpArchive, OpCloseWindow:
		return []string{"expected"}
	default:
		return nil
	}
}

// MarshalJSON is necessary because encoding/json does not omit zero-valued
// structs. Ordinary json.Marshal must have the same per-op shape as the strict
// receiver; emitting empty grants on query would otherwise reject our own CLI.
func (r Request) MarshalJSON() ([]byte, error) { return EncodeRequest(r) }

func EncodeRequest(r Request) ([]byte, error) {
	if err := validateRequest(r); err != nil {
		return nil, err
	}
	fields := map[string]any{"tx_id": r.TxID, "query_nonce": r.QueryNonce,
		"grant": r.Grant, "artifact": r.Artifact, "prepare": r.Prepare,
		"expected": r.Expected, "reverse": r.Reverse}
	allowed := map[string]bool{}
	out := map[string]any{"op": r.Op, "key": r.Key}
	for _, name := range operationFields(r.Op) {
		allowed[name] = true
		out[name] = fields[name]
	}
	for name, value := range fields {
		if !allowed[name] && !reflect.ValueOf(value).IsZero() {
			return nil, wireError("field is not allowed for operation")
		}
	}
	body, err := json.Marshal(out)
	if err == nil {
		err = checkJSON(body)
	}
	return body, err
}

// DecodeRequest rejects presence of irrelevant fields, even an explicit null or
// empty object. Semantic checks precede endpoint invocation; store CAS remains
// responsible for issuer authenticity, expiry and current-revision comparison.
func DecodeRequest(body []byte, component upgradetx.Component) (Request, error) {
	var req Request
	if err := StrictDecode(body, &req); err != nil {
		return req, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return req, wireError("invalid request object")
	}
	allowed := map[string]bool{"op": true, "key": true}
	for _, field := range operationFields(req.Op) {
		allowed[field] = true
	}
	if len(fields) != len(allowed) {
		return Request{}, wireError("operation fields are incomplete or unexpected")
	}
	for field, value := range fields {
		if !allowed[field] || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return Request{}, wireError("operation field is not allowed")
		}
	}
	if req.Key.Component != component {
		return Request{}, wireError("transaction target role mismatch")
	}
	if err := validateRequest(req); err != nil {
		return Request{}, err
	}
	return req, nil
}

// Boundary validation establishes structural identity, not live execution
// authority. The daemon still verifies the stored grant and current fence; a
// well-formed envelope cannot create a transaction or infer absent resources.
// origin: hot-update S8 plan §2.2 / §3.1 / P13
func validateRequest(r Request) error {
	// A describe names only what a ctl can know; every other operation names
	// the exact registered Key the target issued in its grant.
	// origin: S8 FIX-DESCRIBE (P19 request R1)
	if r.Op == OpDescribe {
		if !validSelector(r.Key) || !hexValue(r.QueryNonce, 32) {
			return wireError("describe requires a component selector and a fresh nonce")
		}
		return nil
	}
	if !IsOperation(r.Op) || !validKey(r.Key) {
		return wireError("invalid operation or target key")
	}
	switch r.Op {
	case OpPrepare:
		// Only the durable legacy issuer may authorize interruption. Accepting an
		// empty mode here would let an old request bypass the shared policy.
		// origin: hot-update S8 plan §7.2 / P13
		p := r.Prepare
		if !validText(p.ClientNonce, 256) || !validText(p.ActorFP, 256) || !validText(p.SourceRelease, 128) || !validText(p.TargetRelease, 128) || p.Mode != upgradetx.ModeHot ||
			!hexValue(p.SourceSHA, 64) || !hexValue(p.TargetSHA, 64) || !hexValue(p.CapabilityDigest, 64) ||
			p.ExpiresAt <= 0 || !validScope(p.Scope, r.Key) || !validParent(p.ParentTxID, p.ParentSlot) {
			return wireError("invalid prepare authorization")
		}
	case OpStart:
		// Publication belongs to this exact grant, including actor, issuer and
		// parent slot. Matching bytes from somebody else's stage are insufficient.
		// These paths are checked lexically; this decoder never opens an artifact.
		// origin: hot-update S8 plan §5.2 / P13
		g := r.Grant
		if !validHandle(g.Handle, r.Key) || !validOwner(g.Issuer) || !validText(g.ActorFP, 256) ||
			!validText(g.ClientNonce, 256) || !validText(g.SourceRelease, 128) || !validText(g.TargetRelease, 128) || g.Mode != upgradetx.ModeHot || g.ExpiresAt <= 0 ||
			!hexValue(g.SourceSHA, 64) || !hexValue(g.TargetSHA, 64) || !hexValue(g.ScopeDigest, 64) ||
			!hexValue(g.CapabilityDigest, 64) || !validParent(g.ParentTxID, g.ParentSlot) ||
			r.Artifact.Grant != g || r.Artifact.SourceSHA != g.SourceSHA || r.Artifact.TargetSHA != g.TargetSHA ||
			!hexValue(r.Artifact.ManifestDigest, 64) || !validPath(r.Artifact.CanonicalPath) || !validPath(r.Artifact.SourcePath) || !validPath(r.Artifact.TargetPath) {
			return wireError("invalid start grant or artifact binding")
		}
	case OpQuery:
		if !hexValue(r.TxID, 32) || !hexValue(r.QueryNonce, 32) {
			return wireError("query requires transaction and fresh nonce")
		}
	case OpRollback:
		// A reverse is a separate authorization from the current image. Requiring
		// the inherited incarnation prevents a missing image expectation from
		// silently becoming an ordinary registration that learns a recreated SID.
		// origin: hot-update S8 plan §4 / §6.2 / P13
		if !validHandle(r.Expected.Current, r.Key) || !validOwner(r.Expected.Owner) ||
			!hexValue(r.Expected.RunningSHA, 64) || !hexValue(r.Expected.PreviousSHA, 64) || (r.Key.Component == upgradetx.ComponentAgent && !hexValue(r.Expected.Incarnation, 64)) ||
			!validText(r.Reverse.ClientNonce, 256) || !validText(r.Reverse.ActorFP, 256) ||
			!hexValue(r.Reverse.HistoricalTxID, 32) || !hexValue(r.Reverse.TargetSHA, 64) ||
			r.Reverse.ExpiresAt <= 0 || !validScope(r.Reverse.Scope, r.Key) {
			return wireError("invalid reverse authorization")
		}
	case OpCancel, OpArchive, OpCloseWindow:
		// The caller supplies only a CAS handle, never a death proof. Archive and
		// retirement facts must come from the target's independent observation.
		// origin: hot-update S8 plan §2.2 / §8.1 / P13
		if !validHandle(r.Expected.Current, r.Key) {
			return wireError("operation requires expected handle")
		}
	}
	return nil
}

func validPath(path string) bool {
	return validText(path, 4096) && filepath.IsAbs(path) && filepath.Clean(path) == path
}

// UID zero is a legitimate root namespace. Role-specific fields, rather than
// UID nonzero or a display node name, determine the stable store key. Alias
// resolution remains with the canonical domain registry, not the wire parser.
// origin: hot-update S8 plan §2.1 / §5.1 / P13
func validKey(k upgradetx.Key) bool {
	if !validText(k.DomainID, 256) {
		return false
	}
	if k.Component == upgradetx.ComponentAgent {
		if !validText(k.SID, 128) || !validText(k.ConfiguredNID, 128) {
			return false
		}
	} else if k.Component != upgradetx.ComponentBroker || k.SID != "" || k.ConfigPath == "" {
		return false
	}
	return k.ConfigPath == "" || (validText(k.ConfigPath, 4096) && filepath.IsAbs(k.ConfigPath) && filepath.Clean(k.ConfigPath) == k.ConfigPath)
}

// DescribeSelector is the identity a ctl can state about a component: its role
// and, for an agent, the session and configured node name. DomainID, UID and
// ConfigPath are installation facts that only the target reports, and a
// broker is addressed by its route (admin socket or signed target node), so
// its selector is the role alone.
// origin: S8 FIX-DESCRIBE (P19 request R1)
func DescribeSelector(key upgradetx.Key) upgradetx.Key {
	if key.Component == upgradetx.ComponentAgent {
		return upgradetx.Key{Component: key.Component, SID: key.SID, ConfiguredNID: key.ConfiguredNID}
	}
	return upgradetx.Key{Component: key.Component}
}

// A selector carrying any installation fact is refused rather than compared:
// a ctl-supplied DomainID or UID would otherwise read as knowledge the ctl
// cannot have, and a partially matching guess must not select a component.
func validSelector(k upgradetx.Key) bool {
	switch k.Component {
	case upgradetx.ComponentAgent:
		return validText(k.SID, 128) && validText(k.ConfiguredNID, 128) && k == DescribeSelector(k)
	case upgradetx.ComponentBroker:
		return k == DescribeSelector(k)
	case upgradetx.ComponentCaddy, upgradetx.ComponentNATS:
		return false
	default:
		return false
	}
}

// Revision participates in mutation CAS, while cut and generation bind the
// retained runtime attempt. No one of these identities substitutes for another.
// origin: hot-update S8 plan §3.3 / P13
func validHandle(h upgradetx.Handle, key upgradetx.Key) bool {
	return h.Key == key && hexValue(h.TxID, 32) && hexValue(h.CutID, 32) && h.Generation > 0 && h.Revision > 0
}
func validOwner(o upgradetx.Owner) bool {
	return validText(o.BootID, 256) && o.PID > 0 && o.StartTicks > 0 && validText(o.InstanceID, 256)
}
func validParent(id, slot string) bool {
	return id == "" && slot == "" || hexValue(id, 32) && validText(slot, 128)
}
func validText(s string, limit int) bool {
	return s != "" && len(s) <= limit && utf8.ValidString(s) && !strings.ContainsAny(s, "\x00\r\n")
}
func hexValue(s string, length int) bool {
	if len(s) != length {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// The scope is an explicit set containing the addressed participant. Unknown
// or duplicate domains cannot turn a truncated set into apparent coverage;
// retained-object ownership and complete census proofs remain engine checks.
// origin: hot-update S8 plan §2.1 / §4.2 / P13
func validScope(s upgradetx.Scope, key upgradetx.Key) bool {
	if !validText(s.ID, 256) || len(s.Participants) == 0 || len(s.Domains) == 0 {
		return false
	}
	found := false
	seenKeys := map[upgradetx.Key]bool{}
	for _, k := range s.Participants {
		if !validKey(k) || seenKeys[k] {
			return false
		}
		seenKeys[k] = true
		found = found || k == key
	}
	seen := map[upgradetx.Domain]bool{}
	for _, d := range s.Domains {
		switch d {
		case upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainPTY, upgradetx.DomainFlow, upgradetx.DomainSS,
			upgradetx.DomainTransferWorker, upgradetx.DomainTransferTracker, upgradetx.DomainBrokerStore, upgradetx.DomainBrokerRuntime,
			upgradetx.DomainAuthority, upgradetx.DomainUpgrade:
		default:
			return false
		}
		if seen[d] {
			return false
		}
		seen[d] = true
	}
	seenResources := map[string]bool{}
	for _, resource := range s.Expected {
		identity := string(resource.Domain) + "\x00" + resource.ID
		if !seen[resource.Domain] || !validText(resource.ID, 4096) || seenResources[identity] {
			return false
		}
		seenResources[identity] = true
	}
	return found
}

// StrictDecode preserves encoding/json's typed integer overflow checks while
// adding duplicate, depth and collection guards. Lowercase keys prevent its
// case-insensitive field fallback from admitting a second spelling of a field.
func StrictDecode(body []byte, target any) error {
	if err := checkJSON(body); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return wireError("invalid transaction fields")
	}
	return nil
}

// Scan the bounded bytes before decoding into any slice-bearing DTO. JSON's
// last-key-wins rule is unsafe for signed grants: validators and issuers must
// see the same value at every depth, including nested artifact grants.
// origin: hot-update S8 plan §2.2 / P13
func checkJSON(body []byte) error {
	if len(body) == 0 || len(body) > MaxMessageBytes || !utf8.Valid(body) {
		return wireError("transaction payload exceeds limits or is invalid")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	if err := scanJSON(d, 0); err != nil {
		return err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return wireError("trailing transaction payload")
	}
	return nil
}

func scanJSON(d *json.Decoder, depth int) error {
	// Depth is checked before recursive descent, independently of byte size;
	// a small adversarial envelope must not exhaust the parser stack.
	// origin: hot-update S8 plan §2.2 / P13
	if depth > 32 {
		return wireError("transaction nesting exceeds limit")
	}
	token, err := d.Token()
	if err != nil {
		return wireError("invalid transaction JSON")
	}
	if text, ok := token.(string); ok && len(text) > 4096 {
		return wireError("transaction string exceeds limit")
	}
	delim, container := token.(json.Delim)
	if !container {
		return nil
	}
	if delim != '{' && delim != '[' {
		return wireError("invalid transaction container")
	}
	seen := map[string]bool{}
	for count := 0; d.More(); count++ {
		// Count before decoding the next child so rejecting an oversized scope
		// does not first allocate that scope's entire typed collection.
		// origin: hot-update S8 plan §2.2 / P13
		if count >= MaxCollection {
			return wireError("transaction collection exceeds limit")
		}
		if delim == '{' {
			key, err := d.Token()
			if err != nil {
				return wireError("invalid transaction key")
			}
			name, ok := key.(string)
			if !ok || len(name) > 128 || seen[name] || strings.ToLower(name) != name {
				return wireError("duplicate or noncanonical transaction field")
			}
			seen[name] = true
		}
		if err := scanJSON(d, depth+1); err != nil {
			return err
		}
	}
	if _, err := d.Token(); err != nil {
		return wireError("invalid transaction container end")
	}
	return nil
}

// ReadLine bounds the socket framing before allocating a complete JSON object.
// The reader belongs to one connection; no buffered bytes survive a request.
func ReadLine(reader io.Reader) ([]byte, error) {
	line, err := bufio.NewReader(io.LimitReader(reader, MaxMessageBytes+1)).ReadBytes('\n')
	if len(line) > MaxMessageBytes {
		return nil, wireError("admin request exceeds limit")
	}
	return line, err
}

// CheckJSON lets the admin adapter reject ambiguous framing before choosing a
// legacy or transaction decoder. It does not authorize either operation.
// origin: hot-update S8 plan §2.2 / P13
func CheckJSON(body []byte) error { return checkJSON(body) }
