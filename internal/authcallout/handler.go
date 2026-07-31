// Package authcallout implements the NATS auth_callout decision logic
// (architecture E.2 / E.5).
//
// Flow on each NATS CONNECT:
//
//  1. NATS server has already verified the client's nkey signature.
//  2. NATS publishes a signed AuthorizationRequestClaims JWT to
//     `$SYS.REQ.USER.AUTH`.
//  3. This handler decodes it, decides the permission template, and
//     replies with an AuthorizationResponseClaims JWT signed by the
//     account key. NATS applies the user JWT permissions for the
//     duration of the connection.
//
// Connection name (`nats.Name(...)`) carries role + session:
//
//   - "tether-cli"            — unactivated CLI
//   - "tether-cli:<sid>"      — CLI activating session <sid>
//   - "tether-agent:<sid>:<nid>" — agent in session <sid> as node <nid>
//
// Token (`nats.Token(...)`) optionally carries a PIN for first-time join.
package authcallout

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// Handler is stateful: holds the DB + the account signer for issuing JWTs.
type Handler struct {
	DB        *sql.DB
	AccountKp nkeys.KeyPair // signs response JWT (Issuer in NATS auth_callout)
	Now       func() time.Time
	Logger    *slog.Logger

	// JWTTTL is the lifetime stamped into issued user JWTs. Defaults to 24h.
	JWTTTL time.Duration

	// TargetAccount is the NATS account name (or pub) where the issued user
	// JWT places the connection. In static auth_callout mode this matches
	// Options.AuthCallout.Account. Empty = "$G" (global). Stamped into the
	// user JWT's Audience so the server can route the user into the right
	// account post-callout (see auth_callout_test.createAuthUser).
	TargetAccount string

	// EmitEvent, when non-nil, is invoked for sys.events-worthy
	// auth-callout outcomes (architecture H.1):
	//   - kind="member_joined" — PIN-bootstrap successfully added a
	//     new member row (fields: sid, fp, via=pin)
	//   - kind="pin_failed"    — PIN verify rejected (fields: sid, fp)
	//
	// The handler doesn't import broker (broker imports authcallout),
	// so the broker injects this callback that wraps its pubSysEvent.
	// Nil callback = no emission, fine for unit tests / pre-P7 builds.
	EmitEvent func(kind string, fields map[string]any)

	// LeaderContactStale, when non-nil, fences already-provisioned (no-PIN)
	// authorize decisions: if it returns true (this node lost timely leader
	// contact beyond T_fence) the handler DENIES even a valid member/agent
	// reconnect (distributed-broker §3.2/§6.2 fail-closed). nil => never fenced —
	// today's single-node behavior, the production default in D3 (no cluster.Node
	// is wired into the live broker until D9).
	LeaderContactStale func(now time.Time) bool

	// JoinMemberWrite / ProvisionAgentWrite, when non-nil, route the PIN-bootstrap
	// WRITE through the cluster FSM (leader-only Node.Propose) instead of a direct
	// local mutator. They MUST return the same typed errors as session.JoinWithPIN /
	// agentprov.ProvisionWithPIN, plus authcallout.ErrNotLeader when this broker is
	// not the leader (D3-R3: a transient deny, never a false-allow or an
	// un-replicated local write). nil => direct local mutator (production default
	// in D3). Transparent follower→leader forwarding is D4.
	JoinMemberWrite     func(sid, fp, pin string, now time.Time) error
	ProvisionAgentWrite func(sid, nid, fp, pin string, now time.Time) error

	// ClusterMode makes the seam-nil fallbacks FAIL CLOSED instead of writing (batch B, B3).
	//
	// The two seams above are set by internal/broker/authcallout.go inside a single
	// `if b.clusterMode { … }`. That one `if` is the whole enforcement of "clustered ⇒ both seams
	// wired", and nothing checked it. If it is ever missed — a third PIN write path added with its
	// own seam, a refactor that reorders the wiring past this call — the nil fallback issues
	// `ProvisionWithPIN(h.DB, …)` directly. h.DB is the READ-ONLY FSM handle in cluster mode, so
	// that write fails at the SQLite layer with `attempt to write a readonly database`, on the
	// AUTHENTICATION path, surfacing to the operator as "agents cannot join" with no hint why.
	// Worse, if a future change hands this package the FSM WRITE pool instead, the same fallback
	// succeeds and silently bypasses raft.
	//
	// Setting this alongside the seams turns both outcomes into one named refusal. It is a
	// fail-closed marker, NOT a mode switch: when it is true and a seam is missing, the handler
	// denies rather than guessing.
	ClusterMode bool

	// pinLimiter (P7/#25) throttles PIN-bootstrap brute force per client IP
	// (architecture E.6). Created once on first use; shared across every callout
	// this broker answers. See ratelimit.go for the trust-boundary rationale.
	limiterOnce sync.Once
	limiter     *pinRateLimiter
}

// pinLimiterFor returns the process-wide (per-broker) PIN rate limiter, creating
// it exactly once. Race-safe: Handle may be invoked concurrently.
func (h *Handler) pinLimiterFor() *pinRateLimiter {
	h.limiterOnce.Do(func() { h.limiter = newPinRateLimiter() })
	return h.limiter
}

// ErrNotLeader is the transient deny a clustered write seam returns when this
// broker is not the raft leader (D3-R3). The PIN bootstrap cannot complete here;
// the request is retriable (e.g. the client reconnects onto the leader). It is
// NEVER a false-allow and NEVER an un-replicated local write.
var ErrNotLeader = errors.New("not_leader: cluster write must go to the leader (retriable)")

// ErrFenced is the transient deny returned when this node has lost timely leader
// contact (> T_fence) and must fail closed (§3.2/§6.2). Retriable once contact
// is restored.
var ErrFenced = errors.New("fenced: node lost leader contact (retriable)")

// ErrPINRateLimited is the deny returned when a client IP has exceeded the E.6
// PIN-attempt budget (≤10 failed attempts / IP / minute). It is REFUSED even
// with a correct PIN — the whole point is to shut a brute-force source out of
// the PIN oracle. Retriable once the source's bucket refills. It never touches a
// legitimate already-provisioned reconnect (those skip the PIN path entirely).
var ErrPINRateLimited = errors.New("rate_limited: too many PIN attempts from your network address; retry shortly")

// ErrSeamNotWired is the fail-closed deny returned when the handler is in cluster mode but a
// PIN-bootstrap write seam is nil (batch B, B3). It is a WIRING BUG in the broker, not a client
// error.
//
// Before this existed, the same condition took the direct-mutator fallback and wrote h.DB — the
// read-only FSM handle in cluster mode — so the symptom was a bare
// `attempt to write a readonly database` surfacing on the authentication path.
//
// THE MESSAGE IS DELIBERATELY OPAQUE, and that is a decision, not laziness. Handle passes
// err.Error() straight into h.deny, which puts the text on the wire to a client that has not
// authenticated yet — the same channel batch B just pulled storage-error detail off of
// (testing-standards §S4, see storeErrDenial in internal/broker/admit.go). A message naming the
// seam would tell any anonymous connector that this broker is CLUSTERED and which internal write
// path is missing. So: the wire gets "the operator must look at this broker", the Error log gets
// which seam and why. TestSeamNotWiredKeepsDetailOffTheWire and
// TestSeamNotWiredPutsDetailInTheLog pin those as two SEPARATE propositions — batch A's M13 was
// exactly the mistake of treating them as one.
//
// It is also NOT worded as retriable, unlike ErrNotLeader/ErrFenced: retrying cannot help until an
// operator restarts the broker, and a retriable-looking string would make an agent loop forever
// against a broker that will never accept it.
var ErrSeamNotWired = errors.New("pin_bootstrap_unavailable: this broker cannot complete PIN bootstrap; the operator must check the broker log")

const defaultJWTTTL = 24 * time.Hour

// fenced reports whether this node must fail closed right now (LeaderContactStale
// wired and tripped). nil predicate => never fenced (production default in D3).
func (h *Handler) fenced() bool {
	return h.LeaderContactStale != nil && h.LeaderContactStale(h.Now())
}

// isNotLeader classifies a write-seam error as the transient not-leader case. The
// handler stays raft-free (architecture L-2: raft is confined to internal/cluster):
// the clustered seam (built by the broker/D9, which may import raft — see
// cluster.IsNotLeader) is responsible for translating raw raft.ErrNotLeader /
// raft.ErrLeadershipLost into this sentinel before returning. §6.2 / D3-R3.
func isNotLeader(err error) bool {
	return errors.Is(err, ErrNotLeader)
}

// Handle decodes the request, decides permissions, returns the response JWT.
// On any decision failure (not a member, bad PIN, malformed), returns an
// AuthorizationResponseClaims with .Error set — NATS interprets that as a
// CONNECT rejection and the client sees a clear auth error.
func (h *Handler) Handle(reqJWT string) (string, error) {
	if h.Now == nil {
		h.Now = time.Now
	}
	if h.Logger == nil {
		h.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if h.JWTTTL == 0 {
		h.JWTTTL = defaultJWTTTL
	}

	req, err := jwt.DecodeAuthorizationRequestClaims(reqJWT)
	if err != nil {
		return "", fmt.Errorf("authcallout: decode request: %w", err)
	}

	// IMPORTANT: NATS server generates a fresh ephemeral user nkey for each
	// auth_callout request and sends it as `req.UserNkey` (replay-attack
	// protection — the response JWT must be tied to that exact ephemeral pub).
	// The client's REAL identity nkey, the one the architecture B.2 by.<actor>
	// segment refers to, is in `req.ConnectOptions.Nkey`.
	//
	// We therefore use:
	//   - req.UserNkey            → the JWT subject (what NATS expects back)
	//   - req.ConnectOptions.Nkey → the actor in permission templates,
	//                                  the fingerprint key for membership checks
	jwtSubject := req.UserNkey
	clientNkey := req.ConnectOptions.Nkey

	if jwtSubject == "" || !nkeys.IsValidPublicUserKey(jwtSubject) {
		return h.deny(req, "invalid ephemeral user nkey")
	}
	if clientNkey == "" || !nkeys.IsValidPublicUserKey(clientNkey) {
		return h.deny(req, "client must present a user nkey on CONNECT")
	}

	fp, err := auth.FingerprintFromActor(clientNkey)
	if err != nil {
		return h.deny(req, "fingerprint: "+err.Error())
	}

	// clientIP is the TCP peer address nats-server stamped onto the request; it
	// keys the E.6 per-IP PIN-brute-force throttle. Client-controlled fields
	// (nkey / name / token) are NOT trusted for this — see ratelimit.go.
	clientIP := req.ClientInformation.Host

	role, sid, nid := parseRole(req.ConnectOptions.Name)
	switch role {
	case roleCtlUnactivated:
		return h.allow(req, jwtSubject, auth.PermissionsForUnactivated(clientNkey))
	case roleCtlActivated:
		if err := h.ensureMember(sid, fp, req.ConnectOptions.Token, clientIP); err != nil {
			h.Logger.Info("authcallout: ctl deny", "actor", clientNkey, "sid", sid, "err", err)
			return h.deny(req, err.Error())
		}
		h.Logger.Info("authcallout: ctl allow", "actor", clientNkey, "sid", sid)
		return h.allow(req, jwtSubject, auth.PermissionsForActivatedMember(clientNkey, sid))
	case roleAgent:
		if err := h.ensureAgentProvisioned(sid, nid, clientNkey, fp, req.ConnectOptions.Token, clientIP); err != nil {
			h.Logger.Info("authcallout: agent deny",
				"actor", clientNkey, "sid", sid, "nid", nid, "err", err)
			return h.deny(req, err.Error())
		}
		h.Logger.Info("authcallout: agent allow",
			"actor", clientNkey, "sid", sid, "nid", nid)
		return h.allow(req, jwtSubject, auth.PermissionsForAgent(sid, nid))
	case roleUnknown:
		// Named explicitly (origin: line-2 review IDG-3). The default below already denies, so the
		// behaviour is unchanged — but with only a default, exhaustive goes blind on this switch, and a
		// FIFTH role added later would land in deny() silently. For an auth decision, "silently" is the
		// part that matters: deny is the safe direction, so nobody would notice the new role never got
		// its branch until someone asked why that client cannot connect.
		fallthrough
	default:
		return h.deny(req, fmt.Sprintf("unknown role from name=%q", req.ConnectOptions.Name))
	}
}

// role tags the four valid client kinds.
type role int

const (
	roleUnknown role = iota
	roleCtlUnactivated
	roleCtlActivated
	roleAgent
)

// parseRole classifies a connection name into (role, sid, nid). sid is
// empty for unactivated CLIs; nid is non-empty only for agents.
func parseRole(name string) (role, string, string) {
	switch {
	case name == "tether-cli":
		return roleCtlUnactivated, "", ""
	case strings.HasPrefix(name, "tether-cli:"):
		sid := strings.TrimPrefix(name, "tether-cli:")
		if sid == "" {
			return roleUnknown, "", ""
		}
		return roleCtlActivated, sid, ""
	case strings.HasPrefix(name, "tether-agent:"):
		rest := strings.TrimPrefix(name, "tether-agent:")
		parts := strings.SplitN(rest, ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return roleUnknown, "", ""
		}
		return roleAgent, parts[0], parts[1]
	}
	return roleUnknown, "", ""
}

// ensureAgentProvisioned implements the two-phase agent auth flow:
//
//  1. Validate sid/nid format strictly. The connection name is
//     client-controlled — never mint PermissionsForAgent without
//     re-validating, otherwise a peer can self-declare into any
//     session by picking the right name.
//  2. Consult agent_provisioning(sid, nid):
//     - row exists with matching fp                     → allow;
//     - row exists with a different fp                  → deny (this nid
//     is taken by another agent identity; operator revokes first);
//     - row missing AND no PIN supplied                 → deny (agent
//     must run `tether agent` with --pin on first boot);
//     - row missing AND a valid PIN supplied            → ProvisionWithPIN
//     (verifies session ACTIVE + PIN, then INSERT), allow.
//
// Architecture K.1 — agent identity is per-machine, per-session, bound
// once at install/provision time and remembered by the broker thereafter.
func (h *Handler) ensureAgentProvisioned(sid, nid, clientNkey, fp, pin, clientIP string) error {
	if err := proto.ValidateSID(sid); err != nil {
		return fmt.Errorf("sid: %w", err)
	}
	if err := proto.ValidateNID(nid); err != nil {
		return fmt.Errorf("nid: %w", err)
	}
	if !nkeys.IsValidPublicUserKey(clientNkey) {
		return fmt.Errorf("agent nkey: not a valid public user key")
	}

	bound, err := agentprov.Lookup(h.DB, sid, nid)
	switch {
	case err == nil:
		if bound != fp {
			return fmt.Errorf("nid %q is bound to a different agent identity", nid)
		}
		// Even on already-provisioned re-connect, refuse if the session
		// has been tombstoned. C.1 §6 applies at every ingress.
		active, err := session.IsActive(h.DB, sid)
		if err != nil {
			return fmt.Errorf("active check: %w", err)
		}
		if !active {
			return fmt.Errorf("session %q not active", sid)
		}
		// Already-provisioned (no-PIN) local-replica read: fail closed if this
		// node lost timely leader contact (§3.2/§6.2).
		if h.fenced() {
			return ErrFenced
		}
		return nil
	case errors.Is(err, agentprov.ErrNotProvisioned):
		// fall through to PIN-bootstrap below
	default:
		return fmt.Errorf("provisioning lookup: %w", err)
	}

	if pin == "" {
		return fmt.Errorf("agent (sid=%q, nid=%q) not provisioned; first connect must supply --pin", sid, nid)
	}
	// E.6 PIN brute-force throttle: a source over its per-IP budget is refused
	// BEFORE the Argon2 verify, so even a correct PIN is rejected while the source
	// is shut out (that is the point — deny the brute-forcer the oracle).
	if h.pinRateLimited(clientIP) {
		h.Logger.Warn("authcallout: agent PIN attempt rate-limited", "sid", sid, "nid", nid, "ip", clientIP)
		return ErrPINRateLimited
	}
	// PIN-bootstrap WRITE: route through the cluster FSM (leader-only) when wired,
	// else the direct local mutator (production default in D3).
	provision := h.ProvisionAgentWrite
	if provision == nil {
		if h.ClusterMode {
			// FAIL CLOSED: see ClusterMode's doc. h.DB is the read-only FSM handle here, so the
			// direct mutator below would either fail with a bare readonly-database error on the
			// auth path or (with a different handle) bypass raft entirely.
			//
			// This log line is the ONLY place the cause is stated: ErrSeamNotWired's wire text is
			// deliberately opaque (it reaches an unauthenticated client), so an operator who does
			// not see this line has no way to tell a wiring bug from a wrong PIN.
			h.Logger.Error("authcallout: ProvisionAgentWrite seam is not wired in cluster mode — "+
				"refusing rather than writing the read-only FSM handle directly",
				"seam", "ProvisionAgentWrite", "sid", sid, "nid", nid)
			return ErrSeamNotWired
		}
		provision = func(sid, nid, fp, pin string, now time.Time) error {
			return agentprov.ProvisionWithPIN(h.DB, sid, nid, fp, pin, auth.VerifyPIN, now)
		}
	}
	switch err := provision(sid, nid, fp, pin, h.Now()); {
	case err == nil:
		h.emit("member_joined", map[string]any{
			"sid": sid, "nid": nid, "fp": fp, "via": "pin", "role": "agent",
		})
		return nil
	case isNotLeader(err):
		return ErrNotLeader // transient — never false-allow (D3-R3)
	case errors.Is(err, agentprov.ErrSessionMissing):
		return fmt.Errorf("session %q does not exist", sid)
	case errors.Is(err, agentprov.ErrSessionDeleting):
		return fmt.Errorf("session %q is being deleted", sid)
	case errors.Is(err, agentprov.ErrInvalidPIN):
		h.recordPINFailure(clientIP) // E.6: only a genuine Argon2 reject counts toward the budget
		h.emit("pin_failed", map[string]any{"sid": sid, "nid": nid, "fp": fp, "role": "agent"})
		return fmt.Errorf("invalid PIN")
	case errors.Is(err, agentprov.ErrAlreadyProvisioned):
		// Race: another agent just provisioned this nid with a different
		// fp. The caller's nkey can never win this nid; surface as the
		// same "bound to a different agent" message.
		return fmt.Errorf("nid %q is bound to a different agent identity", nid)
	default:
		return fmt.Errorf("provision: %w", err)
	}
}

func (h *Handler) ensureMember(sid, fp, pin, clientIP string) error {
	if err := proto.ValidateSID(sid); err != nil {
		return err
	}
	active, err := session.IsActive(h.DB, sid)
	if err != nil {
		return fmt.Errorf("active check: %w", err)
	}
	if !active {
		return fmt.Errorf("session %q not active", sid)
	}
	member, err := session.IsMember(h.DB, sid, fp)
	if err != nil {
		return fmt.Errorf("member check: %w", err)
	}
	if member {
		// Already-provisioned (no-PIN) local-replica read: fail closed if this
		// node lost timely leader contact (§3.2/§6.2).
		if h.fenced() {
			return ErrFenced
		}
		return nil
	}
	if pin == "" {
		return fmt.Errorf("not a member of session %q", sid)
	}
	// E.6 PIN brute-force throttle: refuse a source over its per-IP budget BEFORE
	// the Argon2 verify (a correct PIN is refused too — the source is shut out).
	if h.pinRateLimited(clientIP) {
		h.Logger.Warn("authcallout: ctl PIN attempt rate-limited", "sid", sid, "ip", clientIP)
		return ErrPINRateLimited
	}
	// PIN-bootstrap WRITE: route through the cluster FSM (leader-only) when wired,
	// else the direct local mutator (production default in D3).
	join := h.JoinMemberWrite
	if join == nil {
		if h.ClusterMode {
			// FAIL CLOSED — same reason as the provision seam above, same opaque-wire/detailed-log
			// split.
			h.Logger.Error("authcallout: JoinMemberWrite seam is not wired in cluster mode — "+
				"refusing rather than writing the read-only FSM handle directly",
				"seam", "JoinMemberWrite", "sid", sid)
			return ErrSeamNotWired
		}
		join = func(sid, fp, pin string, now time.Time) error {
			return session.JoinWithPIN(h.DB, sid, fp, pin, auth.VerifyPIN, now)
		}
	}
	if err := join(sid, fp, pin, h.Now()); err != nil {
		if isNotLeader(err) {
			return ErrNotLeader // transient — never false-allow (D3-R3)
		}
		if errors.Is(err, session.ErrInvalidPIN) {
			h.recordPINFailure(clientIP) // E.6: only a genuine Argon2 reject counts toward the budget
			h.emit("pin_failed", map[string]any{"sid": sid, "fp": fp, "role": "ctl"})
			return fmt.Errorf("invalid PIN for session %q", sid)
		}
		return fmt.Errorf("join failed: %w", err)
	}
	h.emit("member_joined", map[string]any{"sid": sid, "fp": fp, "via": "pin", "role": "ctl"})
	return nil
}

// pinRateLimited reports whether clientIP is currently over its E.6 PIN-attempt
// budget. An empty IP (nats-server did not stamp a peer address — not something
// a client can force) fails OPEN: we would rather never throttle real joiners on
// a missing address than collapse the whole fleet into one shared bucket. The
// realistic brute-force path always carries a real peer IP.
func (h *Handler) pinRateLimited(clientIP string) bool {
	if clientIP == "" {
		return false
	}
	return h.pinLimiterFor().blocked(clientIP, h.Now())
}

// recordPINFailure charges one unit of clientIP's E.6 budget for a genuine
// Argon2 reject. No-op on an empty IP (see pinRateLimited).
func (h *Handler) recordPINFailure(clientIP string) {
	if clientIP == "" {
		return
	}
	h.pinLimiterFor().recordFailure(clientIP, h.Now())
}

// emit safely calls EmitEvent if it's wired. Keeps caller code from
// having to nil-check at every event point.
func (h *Handler) emit(kind string, fields map[string]any) {
	if h.EmitEvent != nil {
		h.EmitEvent(kind, fields)
	}
}

func (h *Handler) allow(req *jwt.AuthorizationRequestClaims, userPub string, perms jwt.Permissions) (string, error) {
	uc := jwt.NewUserClaims(userPub)
	uc.Permissions = perms
	if h.JWTTTL > 0 {
		uc.Expires = h.Now().Add(h.JWTTTL).Unix()
	}
	// In NON-operator mode the user JWT's Audience names the account where
	// the user is placed (per nats-server auth_callout_test.createAuthUser:
	// "if it is not a public key, set the audience"). NATS uses this to
	// route the connection into the right account after auth. Default to
	// "$G" (global) when caller didn't specify.
	target := h.TargetAccount
	if target == "" {
		target = "$G"
	}
	if _, err := nkeys.FromPublicKey(target); err != nil {
		// target is a name (e.g. "$G"), set as Audience.
		uc.Audience = target
	}

	userJWT, err := uc.Encode(h.AccountKp)
	if err != nil {
		return "", fmt.Errorf("encode user JWT: %w", err)
	}

	resp := jwt.NewAuthorizationResponseClaims(userPub)
	resp.Audience = req.Server.ID
	resp.Jwt = userJWT
	out, err := resp.Encode(h.AccountKp)
	if err != nil {
		return "", fmt.Errorf("encode auth response: %w", err)
	}
	return out, nil
}

func (h *Handler) deny(req *jwt.AuthorizationRequestClaims, reason string) (string, error) {
	resp := jwt.NewAuthorizationResponseClaims(req.UserNkey)
	resp.Audience = req.Server.ID
	resp.Error = reason
	return resp.Encode(h.AccountKp)
}
