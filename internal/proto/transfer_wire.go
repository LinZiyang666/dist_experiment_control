package proto

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"
)

// PushPrepareReq — ctl pub on s.<sid>.cmd.by.<actor>.node.<nid>.push.req.
// One shape covers both tiers; agent reads Tier and dispatches.
//
// Tier A: InlineData populated; agent SHA-verifies, writes via tmp+rename
// into Path, replies OK; agent then publishes ev.transfer.<id>.complete
// (receiver-finalization). Size MUST equal len(InlineData).
//
// Tier B: InlineData empty; broker has already ensured bucket Bucket exists
// and the actor's session-scoped permissions allow Put (not in this struct).
// Agent validates Path against allow_roots, replies OK +
// echoes Bucket/ObjectKey/ChunkSize. ctl then ObjectStore.Puts to Bucket
// keyed by ObjectKey, then sends TransferCommitReq.
type PushPrepareReq struct {
	// Stable logical operation identity; required on capable requests only.
	// origin: hot-update S7 plan §6.2 / P01
	CallID     string `json:"call_id,omitempty"`
	TransferID string `json:"transfer_id"` // random ctl-generated id; broker echoes into audit
	Path       string `json:"path"`        // absolute remote path; under agent's allow_roots only when narrowing is configured
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`                // hex; expected digest after upload
	Force      bool   `json:"force,omitempty"`       // overwrite existing regular file
	Tier       string `json:"tier"`                  // "a" | "b" — chosen by ctl, validated by agent
	InlineData []byte `json:"inline_data,omitempty"` // tier A only (base64 over JSON)
	// Bucket / ObjectKey are BROKER-ASSIGNED and any ctl-supplied value is
	// DISCARDED — the same rule as ActorFP below. handlePushReq clears both before
	// the tier-b arm refills them, because the tracker serialises per bucket by
	// plain string compare with no session in it, so a client-chosen bucket name is
	// a cross-session denial of service (prerelease audit broker-transfer/BT-F2).
	// NOTE this does NOT apply to TransferCommitReq's fields of the same names:
	// there they are ctl-supplied and required.
	Bucket    string `json:"bucket,omitempty"`     // tier B only; broker-assigned, ctl value discarded
	ObjectKey string `json:"object_key,omitempty"` // tier B only; broker-assigned, ctl value discarded
	ChunkSize int    `json:"chunk_size,omitempty"` // tier B hint

	// ActorFP — broker-stamped at forward time (same convention as ExecReq.ActorFP).
	// ctl-supplied value is discarded.
	ActorFP string `json:"actor_fp,omitempty"`
	// All retained creation authority travels together; nil keeps the one-shot wire.
	// origin: hot-update S7 plan §6.2 / P01
	TransferVersion string                `json:"transfer_version,omitempty"`
	Continuation    *TransferContinuation `json:"continuation,omitempty"`
}

// PullPrepareReq — ctl pub on s.<sid>.cmd.by.<actor>.node.<nid>.pull.req.
// Single shape; agent decides tier based on size vs MaxInline. Broker ensures
// the per-session tier-B bucket exists before forwarding; a tier-A response
// simply leaves that shared bucket untouched.
type PullPrepareReq struct {
	// A sent-unknown prepare keeps its original operation identity.
	// origin: hot-update S7 plan §6.2 / P01
	CallID     string `json:"call_id,omitempty"`
	TransferID string `json:"transfer_id"`
	Path       string `json:"path"`            // absolute remote path; under agent's allow_roots only when narrowing is configured
	MaxInline  int64  `json:"max_inline"`      // ctl's tier-A budget (server max_payload aware)
	Force      bool   `json:"force,omitempty"` // overwrite existing local file

	ActorFP string `json:"actor_fp,omitempty"`
	// Source discovery seals the original budget, never a second invocation.
	// origin: hot-update S7 plan §6.2 / P01
	TransferVersion string                `json:"transfer_version,omitempty"`
	Continuation    *TransferContinuation `json:"continuation,omitempty"`
}

// PushPrepareResp — agent pub on the push.req reply inbox. For tier A
// this means "file is on disk + SHA verified + renamed into place"; for
// tier B it means "I validated Path + ready for you to Put".
//
// Code taxonomy for the tier-B provisioning refusals (G67 / gotcha #67). These two are the
// TRANSIENT/PERMANENT split of what used to be a single terminal code, and the distinction is the
// whole point — do not collapse them again:
//
//	jetstream_not_ready   TRANSIENT. The broker could not provision the per-session object store
//	                      because JetStream did not accept the request (a broker restart, a leader
//	                      election, or a cluster grow in flight). The broker has ALREADY retried a
//	                      bounded number of times before replying. Retrying the command shortly is
//	                      the correct operator response; ctl maps it to exit 75.
//	bucket_create_failed  PERMANENT. Provisioning failed for a reason retrying cannot fix (the JS
//	                      store is too small for the tier-B floor, replicas>1 on a non-clustered
//	                      node, ...). Its text deliberately carries NO retry vocabulary.
//	jetstream_unavailable This broker has no JetStream client at all — a configuration/boot state,
//	                      not a stall. Distinct from jetstream_not_ready, which means JetStream
//	                      exists but would not serve this request right now.
//
// Adding jetstream_not_ready is purely ADDITIVE: Code is a free-form string, the struct is
// unchanged, and ProtoVersion does NOT move. An older ctl prints an unknown code through its
// default branch exactly as it always did.
type PushPrepareResp struct {
	// Exact prepared state accompanies private prepare-only carriers.
	// origin: hot-update S7 plan §6.2 / INTEG-D carrier amendment
	Status *TransferStatusResp `json:"status,omitempty"`
	OK     bool                `json:"ok"`
	Tier   string              `json:"tier,omitempty"` // echoed for paranoia
	Code   string              `json:"code,omitempty"` // transfer_id_in_flight | dst_exists | path_outside_roots (narrow mode) | not_a_regular_file | path_parent_missing | sha_mismatch | too_large | tier_invalid | transfer_disabled (explicit allow_roots: []) | jetstream_unavailable | io_error | ...
	Error  string              `json:"error,omitempty"`
	// The top-level version must equal the retained identity before any object IO.
	// origin: hot-update S7 plan §6.2 / P01
	ObjectVersion   string                `json:"object_version,omitempty"`
	TransferVersion string                `json:"transfer_version,omitempty"`
	Continuation    *TransferContinuation `json:"continuation,omitempty"`
}

// PullPrepareResp — agent pub on the pull.req reply inbox.
//
// Tier A: InlineData populated with the entire file contents; ctl
// SHA-verifies, writes locally via tmp+rename, then sends
// SubjCtrlTransferFinalize{kind:complete}.
//
// Tier B: agent has Put the file into Bucket/ObjectKey (broker created
// the bucket synchronously between subscribe and forward); ctl Gets it,
// SHA-verifies, writes locally, then sends finalize.
type PullPrepareResp struct {
	// Exact prepared state accompanies private prepare-only carriers.
	// origin: hot-update S7 plan §6.2 / INTEG-D carrier amendment
	Status     *TransferStatusResp `json:"status,omitempty"`
	OK         bool                `json:"ok"`
	Tier       string              `json:"tier,omitempty"` // a | b
	Size       int64               `json:"size,omitempty"`
	SHA256     string              `json:"sha256,omitempty"`      // hex
	InlineData []byte              `json:"inline_data,omitempty"` // tier A only
	Bucket     string              `json:"bucket,omitempty"`      // tier B only
	ObjectKey  string              `json:"object_key,omitempty"`  // tier B only
	Code       string              `json:"code,omitempty"`        // transfer_id_in_flight | path_not_found | path_outside_roots (narrow mode) | not_a_regular_file | too_large | transfer_disabled (explicit allow_roots: []) | jetstream_unavailable | io_error | ...
	Error      string              `json:"error,omitempty"`
	// Legacy payload fields remain intact, including tier-A inline bytes.
	// origin: hot-update S7 plan §6.2 / P01
	ObjectVersion   string                `json:"object_version,omitempty"`
	TransferVersion string                `json:"transfer_version,omitempty"`
	Continuation    *TransferContinuation `json:"continuation,omitempty"`
}

// CanonicalTransferExtensionBytes preserves the old signer's bytes exactly
// for an absent/all-zero extension. Presence requires a complete versioned
// block; stripping unrecognized fields before verification is never allowed.
// origin: hot-update S7 plan §6.2 / P01
func CanonicalTransferExtensionBytes(legacy []byte, c *TransferContinuation) ([]byte, error) {
	if c == nil || *c == (TransferContinuation{}) {
		return bytes.Clone(legacy), nil
	}
	if c.Version != TransferVersion {
		return nil, errors.New("unsupported transfer extension")
	}
	if err := ValidateTransferWire(c); err != nil {
		return nil, err
	}
	extension, err := json.Marshal(c)
	if err != nil {
		return nil, errors.New("invalid transfer extension")
	}
	out := append(bytes.Clone(legacy), []byte("\n"+TransferVersion+"\n")...)
	return append(out, extension...), nil
}

// Sign only the permit's authority, never its own signature. JSON's explicit
// Identity field order supplies deterministic framing without anonymous fields.
// Issuance bounds creation only; an expired permit can still identify live work.
// origin: hot-update S7 plan §6.1–§6.2 / P01
func CanonicalCreationPermitBytes(p *CreationPermit) ([]byte, error) {
	if p == nil || p.Identity == (Identity{}) || p.Origin == (Origin{}) || p.IssuedAt.IsZero() || !p.CreateUntil.After(p.IssuedAt) ||
		p.CreateUntil.Sub(p.IssuedAt) > 5*time.Minute || !validTransferToken(p.PermitNonce) {
		return nil, errors.New("invalid transfer permit")
	}
	copy := *p
	copy.Signature = ""
	if err := ValidateTransferWire(&copy); err != nil {
		return nil, err
	}
	copy.IssuedAt, copy.CreateUntil = copy.IssuedAt.UTC(), copy.CreateUntil.UTC()
	b, err := json.Marshal(copy)
	return append([]byte("tether-transfer-permit-v1\n"), b...), err
}

// The receiver's NKey proof is verified by the auth owner in addition to this
// signature. A valid canonical capsule alone is not a bearer credential.
// origin: hot-update S7 plan §6.4 / P01
func CanonicalSettlementCapsuleBytes(c *SettlementCapsule) ([]byte, error) {
	if c == nil || c.Identity == (Identity{}) || c.Origin == (Origin{}) || ValidateActorToken(c.ReceiverNkey) != nil ||
		!validTransferToken(c.Issuer) || !validTransferToken(c.Nonce) {
		return nil, errors.New("invalid transfer settlement capsule")
	}
	copy := *c
	copy.Signature = ""
	if err := ValidateTransferWire(&copy); err != nil {
		return nil, err
	}
	b, err := json.Marshal(copy)
	return append([]byte("tether-transfer-settlement-v1\n"), b...), err
}

// A pull's eventual SourceSeal never changes these original intent bytes.
// origin: hot-update S7 plan §6.1–§6.2 / P01
func TransferIntentDigest(intent Intent) (string, error) {
	if err := ValidateTransferWire(intent); err != nil {
		return "", err
	}
	b, err := json.Marshal(intent)
	if err != nil {
		return "", errors.New("invalid transfer intent")
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// DecodeTransferWire is for capable messages and signed extensions only.
// Ordinary N-1 JSON keeps its existing permissive decoder. Decode into a fresh
// value so a refused partial payload cannot leave authority in the caller.
// origin: hot-update S7 plan §6.2 / P01
func DecodeTransferWire(data []byte, dst any) error {
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Pointer || v.IsNil() || v.Elem().Kind() != reflect.Struct || !utf8.Valid(data) {
		return errors.New("invalid transfer wire target or encoding")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if err := scanTransferJSON(d); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing transfer JSON")
	}
	if err := checkTransferJSONFields(data, v.Elem().Type()); err != nil {
		return err
	}
	tmp := reflect.New(v.Elem().Type())
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(tmp.Interface()); err != nil {
		// Decoder errors can quote arbitrary private payloads. Keep them out of logs.
		return errors.New("invalid or unknown transfer wire field")
	}
	if err := ValidateTransferWire(tmp.Interface()); err != nil {
		return err
	}
	v.Elem().Set(tmp.Elem())
	return nil
}

// Duplicate keys are ambiguous even when the final decoded value looks valid.
// The recursive scan catches nested duplicates before any signature input is built.
func scanTransferJSON(d *json.Decoder) error {
	tok, err := d.Token()
	if err != nil {
		return errors.New("invalid transfer JSON")
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	if delim != '{' && delim != '[' {
		return errors.New("invalid transfer JSON delimiter")
	}
	seen := map[string]bool{}
	for d.More() {
		if delim == '{' {
			key, err := d.Token()
			if err != nil {
				return errors.New("invalid transfer JSON key")
			}
			s, ok := key.(string)
			if !ok || seen[s] {
				return errors.New("duplicate transfer JSON key")
			}
			seen[s] = true
		}
		if err := scanTransferJSON(d); err != nil {
			return err
		}
	}
	if _, err := d.Token(); err != nil {
		return errors.New("invalid transfer JSON end")
	}
	return nil
}

// encoding/json accepts case-insensitive field aliases. Signed authority must
// use the exact manifest spelling, otherwise sid/SID can bypass duplicate-key
// detection and different readers can verify different identities.
// origin: hot-update S7 plan §6.2 / P01
func checkTransferJSONFields(data []byte, typ reflect.Type) error {
	if typ.Kind() == reflect.Pointer {
		if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
			return nil
		}
		return checkTransferJSONFields(data, typ.Elem())
	}
	if typ == reflect.TypeFor[time.Time]() {
		return nil
	}
	if typ.Kind() == reflect.Struct {
		var fields map[string]json.RawMessage
		if json.Unmarshal(data, &fields) != nil || fields == nil {
			return errors.New("invalid transfer JSON object")
		}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			name, options, _ := strings.Cut(f.Tag.Get("json"), ",")
			raw, present := fields[name]
			if !present {
				if options != "omitempty" {
					return errors.New("missing transfer wire field")
				}
				continue
			}
			if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) && f.Type.Kind() != reflect.Pointer && f.Type.Kind() != reflect.Slice {
				return errors.New("null transfer wire field")
			}
			if err := checkTransferJSONFields(raw, f.Type); err != nil {
				return err
			}
			delete(fields, name)
		}
		if len(fields) != 0 {
			return errors.New("unknown transfer wire field")
		}
	} else if typ.Kind() == reflect.Slice && typ.Elem().Kind() != reflect.Uint8 {
		var entries []json.RawMessage
		if json.Unmarshal(data, &entries) != nil {
			return errors.New("invalid transfer JSON array")
		}
		for _, raw := range entries {
			if err := checkTransferJSONFields(raw, typ.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateTransferWire checks syntactic identity and closed enums. Authority,
// phase transitions, exact receipts and subject/auth binding remain owner work.
// Empty response enums are allowed only with an explicit error code; they never
// default to unknown. Zero nested grants cover a proven pre-reserve failure.
// origin: hot-update S7 plan §6.1–§6.2 / P01
func ValidateTransferWire(value any) error {
	return validateTransferValue(reflect.ValueOf(value))
}

func validateTransferValue(v reflect.Value) error {
	if !v.IsValid() {
		return errors.New("missing transfer wire value")
	}
	if v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil
		}
		return validateTransferValue(v.Elem())
	}
	if v.CanInterface() {
		if terminal, err := validateTransferSyntax(v.Interface()); terminal || err != nil {
			return err
		}
	}
	if v.Kind() == reflect.Struct {
		if v.Type() == reflect.TypeFor[time.Time]() {
			return nil
		}
		if err := validateTransferAuthorityFields(v); err != nil {
			return err
		}
		for i := 0; i < v.NumField(); i++ {
			field, name := v.Field(i), v.Type().Field(i).Name
			if field.Kind() == reflect.String {
				s := field.String()
				if name == "CallID" && !validTransferToken(s) && (s != "" || !strings.HasSuffix(v.Type().Field(i).Tag.Get("json"), ",omitempty")) {
					return errors.New("invalid transfer call ID")
				}
				if s != "" && !validTransferText(s) {
					return errors.New("invalid transfer text")
				}
				switch name {
				case "TransferID", "EntryNonce", "ObjectVersion", "AgentInstance", "ReceiverInstance", "GrantNonce", "TerminalID", "PermitNonce":
					if s != "" && !validTransferToken(s) {
						return errors.New("invalid transfer identifier")
					}
				case "TransferVersion":
					if s != "" && s != TransferVersion {
						return errors.New("unsupported transfer version")
					}
				}
			}
			if err := validateTransferValue(field); err != nil {
				return err
			}
		}
	} else if v.Kind() == reflect.Slice {
		for i := 0; i < v.Len(); i++ {
			if err := validateTransferValue(v.Index(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func validTransferText(s string) bool {
	return s != "" && utf8.ValidString(s) && !strings.ContainsAny(s, "\x00\r\n")
}

// Version disagreement is detected before either side touches the object.
// A legacy response has neither extension nor top-level version.
// origin: hot-update S7 plan §6.2 / P01
func ValidateTransferPrepareVersion(objectVersion string, c *TransferContinuation) error {
	if c == nil {
		if objectVersion != "" {
			return errors.New(CodeTransferObjectVersionMismatch)
		}
		return nil
	}
	if objectVersion != c.Identity.ObjectVersion {
		return errors.New(CodeTransferObjectVersionMismatch)
	}
	return ValidateTransferWire(c)
}

func validateTransferLookup(call string, id Identity, origin Origin) error {
	if id.EntryNonce != "" && origin == (Origin{}) {
		return errors.New("retained status query requires its original owner")
	}
	if !validTransferToken(call) || ValidateSID(id.SID) != nil || ValidateNID(id.NID) != nil || !validTransferText(id.ActorFP) || !validTransferToken(id.TransferID) {
		return errors.New("invalid status lookup scope")
	}
	for _, v := range []string{id.AgentInstance, id.IntentDigest, id.EntryNonce, id.ObjectVersion} {
		if v != "" && !validTransferToken(v) {
			return errors.New("invalid status lookup binding")
		}
	}
	return ValidateTransferWire(origin)
}

// Reflection is also used by additive callers outside the frozen wire types.
// A same-named field with a different type must be refused before any assertion
// or nil inspection; invalid authority is an error, never a process panic.
// origin: hot-update S10 plan §0 / LINT-L2 forcetypeassert
func validateTransferAuthorityFields(v reflect.Value) error {
	if call, ok := v.Type().FieldByName("CallID"); ok && call.Tag.Get("json") == "call_id" {
		if id := v.FieldByName("Identity"); id.IsValid() {
			identity, ok := id.Interface().(Identity)
			if !ok || identity == (Identity{}) {
				return errors.New("missing transfer request identity")
			}
		}
		if origin := v.FieldByName("Origin"); origin.IsValid() {
			owner, ok := origin.Interface().(Origin)
			if !ok || owner == (Origin{}) {
				return errors.New("missing transfer request origin")
			}
		}
	}
	if field := v.FieldByName("Continuation"); field.IsValid() {
		continuation, ok := field.Interface().(*TransferContinuation)
		if !ok {
			return errors.New("invalid transfer continuation field")
		}
		if continuation != nil {
			if call := v.FieldByName("CallID"); call.IsValid() && !validTransferToken(call.String()) {
				return errors.New("missing capable transfer call ID")
			}
			if id := v.FieldByName("TransferID"); id.IsValid() && id.String() != continuation.Identity.TransferID {
				return errors.New(CodeTransferIntentConflict)
			}
		}
	}
	return nil
}

// Type-specific syntax checks are separate from recursive field traversal so
// a new wire type cannot hide an authority check in a large reflection walk.
// origin: hot-update S10 plan §0 / LINT-L2 maintidx
func validateTransferSyntax(value any) (terminal bool, err error) {
	switch x := value.(type) {
	case Identity:
		return false, validateWireIdentity(x)
	case Origin:
		return false, validateWireOrigin(x)
	case TransferProbeReq:
		return false, validateWireTransferProbeReq(x)
	case TransferCensusReq:
		return false, validateWireTransferCensusReq(x)
	case TransferPermitReq:
		return false, validateWireTransferPermitReq(x)
	case Intent:
		return false, validateWireIntent(x)
	case PushPrepareResp:
		return false, validateWirePushPrepareResp(x)
	case PullPrepareResp:
		return false, validateWirePullPrepareResp(x)
	case TransferContinuation:
		return false, validateWireTransferContinuation(x)
	case TransferReserveResp:
		return false, validateWireTransferReserveResp(x)
	case TransferTerminalResp:
		return false, validateWireTransferTerminalResp(x)
	case TransferStatusReq:
		return true, validateWireTransferStatusReq(x)
	case TransferStatusResp:
		return validateWireTransferStatusResp(x)
	case ReceiverOutcome:
		return false, validateWireReceiverOutcome(x)
	case Protection:
		return false, validateWireProtection(x)
	case TerminalReceipt:
		return false, validateWireTerminalReceipt(x)
	case StatusState:
		return false, validateWireStatusState(x)
	case Phase:
		return false, validateWirePhase(x)
	case CommitPrimitive:
		return false, validateWireCommitPrimitive(x)
	case PrimitiveOutcome:
		return false, validateWirePrimitiveOutcome(x)
	case CancelReason:
		return false, validateWireCancelReason(x)
	}
	return false, nil
}

func validateWireIntent(x Intent) error {
	if (x.Verb != "push" && x.Verb != "pull") || (x.Tier != "a" && x.Tier != "b") ||
		!validTransferText(x.Path) || x.Size < 0 || x.Size > XferMaxBytes || x.RateLimitBPS < 0 ||
		(x.ChunkSize != 0 && (x.ChunkSize < TransferMinChunkSize || x.ChunkSize > TransferMaxChunkSize)) {
		return errors.New("invalid transfer intent")
	}
	if _, err := TransferBudget(x.Tier, x.Size, x.RateLimitBPS); err != nil {
		return err
	}
	return nil
}

func validateWirePushPrepareResp(x PushPrepareResp) error {
	if err := ValidateTransferPrepareVersion(x.ObjectVersion, x.Continuation); err != nil {
		return err
	}
	return nil
}

func validateWirePullPrepareResp(x PullPrepareResp) error {
	if err := ValidateTransferPrepareVersion(x.ObjectVersion, x.Continuation); err != nil {
		return err
	}
	return nil
}

func validateWireTransferContinuation(x TransferContinuation) error {
	if x.Version != TransferVersion || x.Identity == (Identity{}) || x.Origin == (Origin{}) {
		return errors.New("invalid transfer continuation")
	}
	if x.Permit != nil && (x.Permit.Identity != x.Identity || x.Permit.Origin != x.Origin) {
		return errors.New("transfer permit identity mismatch")
	}
	if x.Settlement != nil && (x.Settlement.Identity != x.Identity || x.Settlement.Origin != x.Origin) {
		return errors.New("transfer settlement identity mismatch")
	}
	if x.Grant != nil && (x.Grant.Identity != x.Identity || !validTransferToken(x.Grant.GrantNonce) ||
		!validTransferToken(x.Grant.ReceiverInstance) || !validTransferToken(x.Grant.TerminalID)) {
		return errors.New("transfer grant identity mismatch")
	}
	return nil
}

func validateWireTransferReserveResp(x TransferReserveResp) error {
	// A source seal is evidence for the Grant it accompanies, never alone.
	// origin: hot-update S7 plan §4 / INTEG-D pull source seal
	if x.Source != nil && x.Grant == nil {
		return errors.New("transfer reserve source without a grant")
	}
	return nil
}

func validateWireTransferTerminalResp(x TransferTerminalResp) error {
	// Decision bytes without the exact receipt they name could bind a
	// courier to a record the history owner never confirmed.
	// origin: hot-update S7 plan §4 / §5.1 / INTEG-D courier canonical carrier
	if c := x.Canonical; c != nil {
		sum := sha256.Sum256(c.Bytes)
		if x.Receipt == nil || len(c.Bytes) == 0 || len(c.Bytes) > 64<<10 || c.Kind != x.Receipt.Kind ||
			c.ReqID != x.Receipt.ReqID || c.Digest != x.Receipt.CanonicalDigest || c.Digest != hex.EncodeToString(sum[:]) {
			return errors.New("transfer canonical record differs from its receipt")
		}
	}
	return nil
}

func validateWireTransferStatusReq(x TransferStatusReq) error {
	// A standalone read knows only authenticated subject scope. Missing
	// creation facts are discovered by the original-owner census.
	// origin: hot-update S7 plan §6.1 / INTEG-D
	return validateTransferLookup(x.CallID, x.Identity, x.Origin)
}

func validateWireTransferStatusResp(x TransferStatusResp) (terminal bool, err error) {
	if x.State == StatusUnknown && x.Terminal == nil {
		if x.Origin == (Origin{}) {
			return false, errors.New("unknown status has no original owner")
		}
		return true, validateTransferLookup("status", x.Identity, x.Origin)
	}
	if x.State == "" && x.Code == "" {
		return false, errors.New("missing transfer status state")
	}
	if x.State == StatusTerminal && x.Terminal == nil {
		return false, errors.New("missing transfer terminal receipt")
	}
	if x.State == StatusLive && x.Phase == "" {
		return false, errors.New("missing live transfer phase")
	}
	return false, nil
}

func validateWireReceiverOutcome(x ReceiverOutcome) error {
	if x.Outcome == "" || (x.Primitive == "" && (x.Outcome != OutcomeNotEntered || x.Grant != (CommitGrant{}))) {
		return errors.New("missing transfer receiver outcome")
	}
	return nil
}

func validateWireProtection(x Protection) error {
	if x.Phase == "" || x.Identity == (Identity{}) || x.ObjectVersion != x.Identity.ObjectVersion {
		return errors.New("invalid transfer protection")
	}
	return nil
}

func validateWireTerminalReceipt(x TerminalReceipt) error {
	if (x.Kind != "complete" && x.Kind != "failed") || (x.OutcomeUnknown && x.Kind != "failed") {
		return errors.New("invalid transfer terminal kind")
	}
	return nil
}

func validateWireStatusState(x StatusState) error {
	if x != "" && x != StatusUnknown && x != StatusLive && x != StatusTerminal {
		return errors.New("unknown transfer status state")
	}
	return nil
}

func validateWirePhase(x Phase) error {
	switch x {
	case "", PhaseOpen, PhasePrepared, PhaseSending, PhaseReceiving, PhaseCommitReserved, PhaseDecided,
		PhaseTerminalPending, PhaseHistoryConfirmed, PhaseCleanupPending, PhaseSettled:
	default:
		return errors.New("unknown transfer phase")
	}
	return nil
}

func validateWireCommitPrimitive(x CommitPrimitive) error {
	if x != "" && x != PrimitiveRename && x != PrimitiveLink {
		return errors.New("unknown transfer primitive")
	}
	return nil
}

func validateWirePrimitiveOutcome(x PrimitiveOutcome) error {
	if x != "" && x != OutcomeNotEntered && x != OutcomeInflight && x != OutcomeLanded {
		return errors.New("unknown transfer outcome")
	}
	return nil
}

func validateWireCancelReason(x CancelReason) error {
	if x != CancelUser && x != CancelDeadline && x != CancelRevoked && x != CancelStopped {
		return errors.New("unknown transfer cancel reason")
	}
	return nil
}
