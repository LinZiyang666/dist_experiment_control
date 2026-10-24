package xferauth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

// Authentication errors deliberately contain no capsule, challenge or key
// material: the callout can return them to an unauthenticated connection.
var ErrDenied = errors.New("transfer settlement authentication denied")
var ErrStopped = errors.New("transfer settlement verifier stopped")

type verifier struct {
	cfg     Config
	mu      sync.Mutex
	stopped bool
	active  int
	idle    chan struct{}
}

var _ Factory = New

// New requires both cryptographic and durable authority. Neither membership
// nor possession of a capsule establishes that its entry still needs delivery.
// origin: hot-update S7 plan §6.4 / T31
func New(cfg Config) (SettlementVerifier, error) {
	if cfg.Issuers == nil || cfg.Owners == nil {
		return nil, ErrDenied
	}
	idle := make(chan struct{})
	close(idle)
	return &verifier{cfg: cfg, idle: idle}, nil
}

func (v *verifier) Verify(c x.SettlementCapsule, p x.NKeyProof, d controlbudget.Deadline) (x.SettlementGrant, error) {
	v.mu.Lock()
	if v.stopped {
		v.mu.Unlock()
		return x.SettlementGrant{}, ErrStopped
	}
	if v.active == 0 {
		v.idle = make(chan struct{})
	}
	v.active++
	v.mu.Unlock()
	defer func() {
		v.mu.Lock()
		v.active--
		if v.active == 0 {
			close(v.idle)
		}
		v.mu.Unlock()
	}()
	if _, err := controlbudget.Remaining(d); err != nil {
		return x.SettlementGrant{}, err
	}
	if !validCapsule(c) || p.NKey != c.ReceiverNkey || v.cfg.Issuers.Verify(c, p) != nil {
		return x.SettlementGrant{}, ErrDenied
	}
	census, err := v.cfg.Owners.Census(c.Origin, d)
	if err != nil || !census.Ready || !census.Complete || census.Code != "" || census.Origin != c.Origin {
		return x.SettlementGrant{}, ErrDenied
	}
	// A full row is mandatory. An entry count, local empty cache, or an old
	// generation's protection cannot renew a retired receiver's authority.
	// origin: hot-update S7 plan §6.4 / T31
	var match *x.LedgerRow
	for i := range census.Rows {
		r := &census.Rows[i]
		if r.Identity.SID != c.Identity.SID || r.Identity.TransferID != c.Identity.TransferID {
			continue
		}
		if match != nil || r.Identity != c.Identity || r.Origin != c.Origin || r.EntryNonce != c.Identity.EntryNonce ||
			r.Settlement == nil || *r.Settlement != c || r.Revision == 0 || r.OutcomeUnknown || !settlementPhase(r.Phase) {
			return x.SettlementGrant{}, ErrDenied
		}
		match = r
	}
	if match == nil {
		return x.SettlementGrant{}, ErrDenied
	}
	if _, err := controlbudget.Remaining(d); err != nil {
		return x.SettlementGrant{}, err
	}
	return x.SettlementGrant{Identity: c.Identity, Origin: c.Origin, ReceiverNkey: c.ReceiverNkey,
		EntryNonce: c.Identity.EntryNonce, OwnerGeneration: c.Origin.Generation}, nil
}

// Stop closes admission before joining in-flight verification. A timed-out
// join keeps the fence closed; the caller still owns its dependent ports.
// origin: hot-update S7 plan §6.4 / T31
func (v *verifier) Stop(d controlbudget.Deadline) error {
	v.mu.Lock()
	v.stopped = true
	idle := v.idle
	v.mu.Unlock()
	select {
	case <-idle:
		return nil
	default:
	}
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-idle:
		return nil
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}

type issuerVerifier struct{ trusted map[string]bool }

// NewIssuerVerifier pins the configured issuer keys. A valid signature from
// a key supplied by the connecting party is not a trust decision.
// origin: hot-update S7 plan §6.4 / T31
func NewIssuerVerifier(issuers []string) (x.IssuerVerifier, error) {
	trusted := make(map[string]bool, len(issuers))
	for _, key := range issuers {
		if !nkeys.IsValidPublicAccountKey(key) && !nkeys.IsValidPublicUserKey(key) {
			return nil, ErrDenied
		}
		trusted[key] = true
	}
	if len(trusted) == 0 {
		return nil, ErrDenied
	}
	return &issuerVerifier{trusted: trusted}, nil
}

func (v *issuerVerifier) Verify(c x.SettlementCapsule, p x.NKeyProof) error {
	if !validCapsule(c) || !v.trusted[c.Issuer] || p.NKey != c.ReceiverNkey || len(p.Nonce) == 0 || len(p.Nonce) > 1024 {
		return ErrDenied
	}
	issuer, err := nkeys.FromPublicKey(c.Issuer)
	if err != nil {
		return ErrDenied
	}
	sig, err := base64.RawURLEncoding.DecodeString(c.Signature)
	if err != nil || issuer.Verify(capsuleBytes(c), sig) != nil {
		return ErrDenied
	}
	receiver, err := nkeys.FromPublicKey(p.NKey)
	if err != nil || receiver.Verify(p.Nonce, p.Signature) != nil {
		return ErrDenied
	}
	return nil
}

// SignCapsule borrows the issuer key; key ownership and sealed persistence
// remain with the caller. Signing fixes all scope fields before prepare replies.
// origin: hot-update S7 plan §6.4 / T31
func SignCapsule(issuer nkeys.KeyPair, c x.SettlementCapsule) (x.SettlementCapsule, error) {
	if issuer == nil {
		return x.SettlementCapsule{}, ErrDenied
	}
	pub, err := issuer.PublicKey()
	if err != nil {
		return x.SettlementCapsule{}, ErrDenied
	}
	c.Issuer = pub
	if !validCapsule(c) {
		return x.SettlementCapsule{}, ErrDenied
	}
	sig, err := issuer.Sign(capsuleBytes(c))
	if err != nil {
		return x.SettlementCapsule{}, ErrDenied
	}
	c.Signature = base64.RawURLEncoding.EncodeToString(sig)
	return c, nil
}

func capsuleBytes(c x.SettlementCapsule) []byte {
	c.Signature = ""
	b, _ := json.Marshal(c) // This closed value contains no fallible JSON types.
	return append([]byte("tether-transfer-settlement-v1\n"), b...)
}

func validCapsule(c x.SettlementCapsule) bool {
	i := c.Identity
	for _, value := range []string{i.SID, i.NID, i.ActorFP, i.AgentInstance, i.TransferID, i.EntryNonce,
		i.IntentDigest, c.Origin.BrokerID, c.Origin.InstanceID, c.Origin.BootID, c.Nonce} {
		if !validToken(value) {
			return false
		}
	}
	return (i.ObjectVersion == "" || validToken(i.ObjectVersion)) &&
		nkeys.IsValidPublicUserKey(c.ReceiverNkey) &&
		(nkeys.IsValidPublicAccountKey(c.Issuer) || nkeys.IsValidPublicUserKey(c.Issuer))
}

// Capsule tokens can become JWT subject components; wildcards or control
// bytes must fail before permission construction.
// origin: hot-update S7 plan §6.2 / §6.4 / T31
func validToken(s string) bool {
	return s != "" && len(s) <= 512 && !strings.ContainsAny(s, ".*>") &&
		strings.IndexFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) < 0
}

func validOrigin(o x.Origin) bool {
	return validToken(o.BrokerID) && validToken(o.InstanceID) && validToken(o.BootID)
}

// VerifyConnect decodes the private capsule and checks retained SQL authority
// within the same deadline as the original-origin lookup. Errors are opaque.
// origin: hot-update S7 plan §6.4 / T31
func VerifyConnect(db *sql.DB, verifier SettlementVerifier, req *jwt.AuthorizationRequestClaims, sid, nid string) (x.SettlementGrant, error) {
	if db == nil || verifier == nil || len(req.ConnectOptions.Token) > 16384 {
		return x.SettlementGrant{}, ErrDenied
	}
	clientNkey := req.ConnectOptions.Nkey
	var capsule x.SettlementCapsule
	decoder := json.NewDecoder(strings.NewReader(req.ConnectOptions.Token))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&capsule); err != nil {
		return x.SettlementGrant{}, ErrDenied
	}
	var trailing json.RawMessage
	if decoder.Decode(&trailing) != io.EOF || capsule.Identity.SID != sid || capsule.Identity.NID != nid || capsule.ReceiverNkey != clientNkey {
		return x.SettlementGrant{}, ErrDenied
	}
	// Deleted SQL authority is never resurrected by an old capsule; DELETING
	// remains readable specifically so retained obligations can reconnect.
	deadline, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return x.SettlementGrant{}, ErrDenied
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return x.SettlementGrant{}, ErrDenied
	}
	// ctx-none: auth_callout Handle is a NATS callback without a caller context.
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	defer cancel()
	var state string
	if err := db.QueryRowContext(ctx, `SELECT state FROM sessions WHERE sid = ?`, sid).Scan(&state); err != nil || (state != string(session.StateActive) && state != string(session.StateDeleting)) {
		return x.SettlementGrant{}, ErrDenied
	}
	signature, err := base64.RawURLEncoding.DecodeString(req.ConnectOptions.SignedNonce)
	if err != nil {
		// Match the normal CONNECT proof's accepted encodings.
		signature, err = base64.StdEncoding.DecodeString(req.ConnectOptions.SignedNonce)
		if err != nil {
			return x.SettlementGrant{}, ErrDenied
		}
	}
	grant, err := verifier.Verify(capsule, x.NKeyProof{NKey: clientNkey, Nonce: []byte(req.ClientInformation.Nonce), Signature: signature}, deadline)
	if err != nil || grant.Identity != capsule.Identity || grant.Origin != capsule.Origin || grant.ReceiverNkey != clientNkey || grant.EntryNonce != capsule.Identity.EntryNonce || grant.OwnerGeneration != capsule.Origin.Generation {
		return x.SettlementGrant{}, ErrDenied
	}
	return grant, nil
}

// origin: hot-update S7 plan §6.4 / T31; shared normal/settlement proof.
// VerifyClientNkey proves that the connecting party holds the private half of the nkey
// it presented, by checking its signature over the nonce the server issued.
//
// origin: prerelease audit increment 2 internal review, admission-enforcement/L9-F1 ≡
// red-team/RED-TEAM-REFUTE-F1, found by two lanes and reproduced end to end against a
// real nats-server + this handler.
//
// NOBODY ELSE DOES THIS. It is natural to assume the server has already checked the
// signature by the time a callout runs — this package's own doc comment said so for
// three releases — but in conf mode nats-server only verifies an nkey it has a STATIC
// `users:` entry for. server/auth.go bails out at `nkey, ok = s.nkeys[c.opts.Nkey]; if
// !ok { return false }` BEFORE reaching its Verify branch, and the deferred callout hop
// then forwards `ConnectOptions.Nkey` exactly as the client typed it
// (server/auth_callout.go fillConnectOpts). Every tether principal except the broker
// itself is callout-authenticated, so the entire identity model rests on this function.
//
// WHAT WAS REACHABLE WITHOUT IT. `clientNkey` is the actor in every permission template:
// it pins the `ctrl.by.<actor>` publish grants and it derives auth.InboxPrefixFor's
// private subtree. And the fingerprint the session-create allow-list is keyed on is
// computed from it. So knowing a victim's PUBLIC key — which sys.events hands to any
// session member in session_created{actor} — was enough to connect as that victim, read
// its private inbox (register replies carry the tunnel token and every subscriber PSK),
// and pass the admission check as that victim. The signature was never checked, so any
// bytes at all would do.
//
// NOT AN N-1 BREAK. All four production dial sites already sign: internal/cli's
// ConnectNATSWithNkey, the completion transport, the agent, and the broker's own
// connection. A client that signs has always sent `sig`, and nats-server fills
// ClientInformation.Nonce whenever `sig` is present (verified on v2.10.22 and v2.14.0),
// so the material has always been on the request — nothing here asks a client for
// anything it does not already send.
//
// EMPTY IS A REFUSAL, NOT AN EXEMPTION. A missing nonce or signature means the client
// did not authenticate as an nkey holder, which is exactly the case this exists to
// reject. It must not fall through to a permissive template: an empty jwt.Permissions is
// UNRESTRICTED in nats-server (c.perms stays nil), so failing open here would be worse
// than the hole it closes.
func VerifyClientNkey(pubKey, nonce, signedNonce string) error {
	if nonce == "" || signedNonce == "" {
		return errors.New("no signature over the server nonce")
	}
	// Same decode order as nats-server's own check (server/auth.go): raw-url first,
	// standard base64 as a fallback. A client library that picks the other encoding must
	// not be rejected here when the server itself would have accepted it.
	sig, err := base64.RawURLEncoding.DecodeString(signedNonce)
	if err != nil {
		if sig, err = base64.StdEncoding.DecodeString(signedNonce); err != nil {
			return errors.New("signature is not valid base64")
		}
	}
	kp, err := nkeys.FromPublicKey(pubKey)
	if err != nil {
		return fmt.Errorf("public key: %w", err)
	}
	if err := kp.Verify([]byte(nonce), sig); err != nil {
		return errors.New("signature does not verify against the presented nkey")
	}
	return nil
}

// CalloutConfig holds the borrowed credentials and current settlement verifier
// installed by the outer broker. The broker owns and wipes both seeds; this
// configuration is never serialized into an image or a status response.
// origin: hot-update S7 plan §6.4 / T31
type CalloutConfig struct {
	BrokerNkeySeed []byte
	AccountSeed    []byte
	Settlement     SettlementVerifier
}

// origin: hot-update S7 plan §6.4 / lead LQ1 / T31
// ConnectionRole tags the client kinds; settlement never restores ordinary membership.
type ConnectionRole int

const (
	RoleUnknown ConnectionRole = iota
	RoleCtlUnactivated
	RoleCtlActivated
	RoleAgent
	RoleTransferSettlement
	RoleSessionRemoval
)

// ParseConnectionName classifies a connection name into (role, sid, nid). sid is
// empty for unactivated CLIs; nid identifies agents and settlement receivers.
func ParseConnectionName(name string) (ConnectionRole, string, string) {
	switch {
	case strings.HasPrefix(name, "session-removal:"):
		sid := strings.TrimPrefix(name, "session-removal:")
		if proto.ValidateSID(sid) != nil {
			return RoleUnknown, "", ""
		}
		return RoleSessionRemoval, sid, ""
	case strings.HasPrefix(name, "transfer-settlement:"):
		parts := strings.Split(strings.TrimPrefix(name, "transfer-settlement:"), ":")
		if len(parts) != 2 || proto.ValidateSID(parts[0]) != nil || proto.ValidateNID(parts[1]) != nil {
			return RoleUnknown, "", ""
		}
		return RoleTransferSettlement, parts[0], parts[1]
	case name == "tether-cli":
		return RoleCtlUnactivated, "", ""
	case strings.HasPrefix(name, "tether-cli:"):
		sid := strings.TrimPrefix(name, "tether-cli:")
		if sid == "" {
			return RoleUnknown, "", ""
		}
		return RoleCtlActivated, sid, ""
	case strings.HasPrefix(name, "tether-agent:"):
		rest := strings.TrimPrefix(name, "tether-agent:")
		parts := strings.SplitN(rest, ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return RoleUnknown, "", ""
		}
		return RoleAgent, parts[0], parts[1]
	}
	return RoleUnknown, "", ""
}

// VerifyRemovalOwner preserves a reconnect path for the operator after normal
// membership is revoked. This authorizes only rm and its read-only health gate.
// origin: hot-update S7 plan §6.4 / lead LQ1 / T31
func VerifyRemovalOwner(db *sql.DB, sid, fp string) error {
	if db == nil || proto.ValidateSID(sid) != nil || fp == "" {
		return ErrDenied
	}
	// ctx-none: auth_callout CONNECT has no caller context.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var owner bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sessions s JOIN members m ON m.sid=s.sid
		WHERE s.sid=? AND s.state IN ('ACTIVE','DELETING') AND m.pubkey_fp=? AND m.role='owner')`, sid, fp).Scan(&owner)
	if err != nil || !owner {
		return ErrDenied
	}
	return nil
}

// ControlAuthority reads today's SQL authority for each operation. The node
// gate runs only after membership/revocation checks; accepted STALE work cannot
// accidentally become a revocation bypass.
// origin: hot-update S7 plan §6.1 / §6.4 / T39
type ControlAuthority struct {
	SettlementRows func(x.Identity, x.Deadline) (x.LedgerRow, error)
	SQL            func() *sql.DB
	Ledger         func() x.LedgerPort
	NodeGate       func(sid, nid string, accepted bool) string
}

type ControlRefusal string

func (e ControlRefusal) Error() string        { return string(e) }
func (e ControlRefusal) TransferCode() string { return string(e) }

func (a ControlAuthority) active(id x.Identity, d x.Deadline) (bool, error) {
	if a.SQL == nil {
		return false, ControlRefusal(x.CodeOwnerRecovering)
	}
	db := a.SQL()
	if db == nil {
		return false, ControlRefusal(x.CodeOwnerRecovering)
	}
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), remaining) // ctx-none: one bounded current-authority SQL read.
	defer cancel()
	var active bool
	err = db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sessions s JOIN members m ON m.sid=s.sid WHERE s.sid=? AND s.state='ACTIVE' AND m.pubkey_fp=?)`, id.SID, id.ActorFP).Scan(&active)
	if err != nil {
		return false, ControlRefusal(x.CodeOwnerRecovering)
	}
	return active, nil
}

func (a ControlAuthority) settlement(id x.Identity, d x.Deadline) (*x.SettlementCapsule, error) {
	if a.SettlementRows != nil {
		row, err := a.SettlementRows(id, d)
		if err != nil {
			return nil, ErrDenied
		}
		return retainedSettlement(row, id)
	}
	if a.Ledger == nil || a.Ledger() == nil {
		return nil, ErrDenied
	}
	c, err := a.Ledger().Scan(d)
	if err != nil || !c.Ready || !c.Complete || c.Code != "" {
		return nil, ErrDenied
	}
	for _, row := range c.Rows {
		if capsule, err := retainedSettlement(row, id); err == nil {
			return capsule, nil
		}
	}
	return nil, ErrDenied
}

func (a ControlAuthority) Check(id x.Identity, access x.AccessKind, d x.Deadline) error {
	if _, err := controlbudget.Remaining(d); err != nil {
		return err
	}
	active, err := a.active(id, d)
	if err != nil {
		return err
	}
	if !active {
		if access != x.AccessSettle && access != x.AccessStatus {
			return ControlRefusal(x.CodeNotMember)
		}
		if _, err := a.settlement(id, d); err != nil {
			return ControlRefusal(x.CodeNotMember)
		}
	}
	if access == x.AccessCreate || access == x.AccessContinue {
		if a.NodeGate == nil {
			return ControlRefusal(x.CodeOwnerRecovering)
		}
		if code := a.NodeGate(id.SID, id.NID, access == x.AccessContinue); code != "" {
			return ControlRefusal(code)
		}
	}
	return nil
}

// Principal binds the NATS-authenticated subject actor, including after CONNECT
// permissions have been cached. Settlement must still match retained receiver
// authority and is never reconstructed from a public status body.
func (a ControlAuthority) Principal(actor string, id x.Identity, d x.Deadline) (x.Principal, error) {
	if _, err := controlbudget.Remaining(d); err != nil {
		return x.Principal{}, err
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		return x.Principal{}, ErrDenied
	}
	p := x.Principal{NKey: actor, FP: fp, SID: id.SID, NID: id.NID, Role: x.RoleCTL}
	active, err := a.active(id, d)
	if err != nil {
		return x.Principal{}, err
	}
	if active && fp == id.ActorFP {
		return p, nil
	}
	capsule, err := a.settlement(id, d)
	if err != nil || capsule.ReceiverNkey != actor {
		return x.Principal{}, ControlRefusal(x.CodeNotOwner)
	}
	p.Role = x.RoleSettlement
	// The receiver binding is the retained grant for landed work, otherwise the
	// original agent instance. The authenticated key itself supplies ctl identity.
	p.InstanceID = id.AgentInstance
	c, err := a.Ledger().Scan(d)
	if err != nil {
		return x.Principal{}, ErrDenied
	}
	for _, row := range c.Rows {
		if row.Identity == id && row.Grant != nil {
			p.InstanceID = row.Grant.ReceiverInstance
		}
	}
	return p, nil
}

// PermitAuthority persists public signed creation authority before returning it.
// A lost reply reuses the original nonce, object version and creation window.
// The signing key is borrowed from the broker's credential owner, never stored.
// origin: hot-update S7 plan §6.1 / T26
type PermitAuthority struct {
	root   string
	origin x.Origin
	issuer nkeys.KeyPair
	utc    func() time.Time
	mu     sync.Mutex
}

func NewPermitAuthority(root string, origin x.Origin, issuer nkeys.KeyPair, utc func() time.Time) (*PermitAuthority, error) {
	if root == "" || issuer == nil || utc == nil || !validOrigin(origin) {
		return nil, ErrDenied
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	return &PermitAuthority{root: root, origin: origin, issuer: issuer, utc: utc}, nil
}

// Issue lacks session/actor scope in the frozen port. Production calls the
// explicit scoped adapter below; an unscoped call cannot mint authority.
func (p *PermitAuthority) Issue(x.Intent, string, string, x.Deadline) (x.CreationPermit, error) {
	return x.CreationPermit{}, ErrDenied
}

func (p *PermitAuthority) IssueFor(id x.Identity, intent x.Intent, d x.Deadline) (x.CreationPermit, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := controlbudget.Remaining(d); err != nil {
		return x.CreationPermit{}, err
	}
	if !validToken(id.SID) || !validToken(id.NID) || !validToken(id.ActorFP) || !validToken(id.TransferID) || !validToken(id.AgentInstance) {
		return x.CreationPermit{}, ErrDenied
	}
	digest, err := proto.TransferIntentDigest(proto.Intent(intent))
	if err != nil {
		return x.CreationPermit{}, ErrDenied
	}
	sum := sha256.Sum256([]byte(id.SID + "\n" + id.TransferID))
	path := filepath.Join(p.root, hex.EncodeToString(sum[:])+".json")
	data, err := os.ReadFile(path)
	if err == nil {
		return p.retained(data, id, digest, d)
	}
	if !os.IsNotExist(err) {
		return x.CreationPermit{}, err
	}
	id.EntryNonce, id.IntentDigest = nuid.Next(), digest
	if intent.Tier == "b" {
		id.ObjectVersion = nuid.Next()
	} else {
		id.ObjectVersion = ""
	}
	now := p.utc().UTC()
	permit := x.CreationPermit{Identity: id, Origin: p.origin, IssuedAt: now, CreateUntil: now.Add(5 * time.Minute), PermitNonce: nuid.Next()}
	body, err := permitBytes(permit)
	if err != nil {
		return x.CreationPermit{}, err
	}
	sig, err := p.issuer.Sign(body)
	if err != nil {
		return x.CreationPermit{}, ErrDenied
	}
	permit.Signature = base64.RawURLEncoding.EncodeToString(sig)
	data, err = json.Marshal(permit)
	if err != nil {
		return x.CreationPermit{}, err
	}
	file, err := os.CreateTemp(p.root, ".permit-*")
	if err != nil {
		return x.CreationPermit{}, err
	}
	name := file.Name()
	defer func() { _ = file.Close(); _ = os.Remove(name) }()
	if _, err = file.Write(data); err != nil {
		return x.CreationPermit{}, err
	}
	if err = file.Sync(); err != nil {
		return x.CreationPermit{}, err
	}
	if err = file.Close(); err != nil {
		return x.CreationPermit{}, err
	}
	// Exclusive publication also fences a second signer attached to this same
	// durable origin. A concurrent winner's bytes, not a new nonce, are returned.
	// origin: hot-update S7 plan §6.1 / T26
	if err = os.Link(name, path); err != nil {
		if !os.IsExist(err) {
			return x.CreationPermit{}, err
		}
		data, err = os.ReadFile(path)
		if err != nil {
			return x.CreationPermit{}, err
		}
		return p.retained(data, id, digest, d)
	}
	dir, err := os.Open(p.root)
	if err != nil {
		return x.CreationPermit{}, err
	}
	defer func() { _ = dir.Close() }()
	if err = dir.Sync(); err != nil {
		return x.CreationPermit{}, err
	}
	if _, err = controlbudget.Remaining(d); err != nil {
		return x.CreationPermit{}, err
	}
	return permit, nil
}

func permitBytes(p x.CreationPermit) ([]byte, error) {
	wire := proto.CreationPermit{Identity: proto.Identity(p.Identity), Origin: proto.Origin(p.Origin), IssuedAt: p.IssuedAt, CreateUntil: p.CreateUntil, PermitNonce: p.PermitNonce, Signature: p.Signature}
	return proto.CanonicalCreationPermitBytes(&wire)
}
func (p *PermitAuthority) Verify(permit x.CreationPermit) error {
	if permit.Origin != p.origin {
		return ErrDenied
	}
	body, err := permitBytes(permit)
	if err != nil {
		return ErrDenied
	}
	sig, err := base64.RawURLEncoding.DecodeString(permit.Signature)
	if err != nil {
		return ErrDenied
	}
	if err = p.issuer.Verify(body, sig); err != nil {
		return errors.Join(ErrDenied, errors.New("invalid permit signature"))
	}
	return nil
}

func (p *PermitAuthority) retained(data []byte, id x.Identity, digest string, d x.Deadline) (x.CreationPermit, error) {
	var old x.CreationPermit
	if json.Unmarshal(data, &old) != nil || p.Verify(old) != nil || old.Origin != p.origin || old.Identity.SID != id.SID || old.Identity.NID != id.NID || old.Identity.ActorFP != id.ActorFP || old.Identity.AgentInstance != id.AgentInstance || old.Identity.TransferID != id.TransferID || old.Identity.IntentDigest != digest {
		return x.CreationPermit{}, ErrDenied
	}
	// A concurrent exclusive-link winner may not have synced its directory
	// yet. This caller proves durability before returning those same bytes.
	dir, err := os.Open(p.root)
	if err != nil {
		return x.CreationPermit{}, err
	}
	defer func() { _ = dir.Close() }()
	if err = dir.Sync(); err != nil {
		return x.CreationPermit{}, err
	}
	if _, err = controlbudget.Remaining(d); err != nil {
		return x.CreationPermit{}, err
	}
	return old, nil
}
