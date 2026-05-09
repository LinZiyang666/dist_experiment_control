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
}

const defaultJWTTTL = 24 * time.Hour

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

	role, sid, _ := parseRole(req.ConnectOptions.Name)
	switch role {
	case roleCtlUnactivated:
		return h.allow(req, jwtSubject, auth.PermissionsForUnactivated(clientNkey))
	case roleCtlActivated:
		if err := h.ensureMember(sid, fp, req.ConnectOptions.Token); err != nil {
			h.Logger.Info("authcallout: ctl deny", "actor", clientNkey, "sid", sid, "err", err)
			return h.deny(req, err.Error())
		}
		h.Logger.Info("authcallout: ctl allow", "actor", clientNkey, "sid", sid)
		return h.allow(req, jwtSubject, auth.PermissionsForActivatedMember(clientNkey, sid))
	case roleAgent:
		nid := agentNidFromName(req.ConnectOptions.Name)
		if err := h.ensureAgentProvisioned(sid, nid, clientNkey, fp, req.ConnectOptions.Token); err != nil {
			h.Logger.Info("authcallout: agent deny",
				"actor", clientNkey, "sid", sid, "nid", nid, "err", err)
			return h.deny(req, err.Error())
		}
		h.Logger.Info("authcallout: agent allow",
			"actor", clientNkey, "sid", sid, "nid", nid)
		return h.allow(req, jwtSubject, auth.PermissionsForAgent(sid, nid))
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

// agentNidFromName extracts the nid out of a `tether-agent:<sid>:<nid>`
// connection name, returning "" on any malformed input. parseRole has
// already separated sid out, but it discards nid; this re-parses just the
// nid for the agent path.
func agentNidFromName(name string) string {
	rest := strings.TrimPrefix(name, "tether-agent:")
	if rest == name {
		return ""
	}
	parts := strings.SplitN(rest, ":", 2)
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}

// ensureAgentProvisioned implements the F1 (P4 review) two-phase agent
// auth flow:
//
//  1. validate sid/nid format strictly (P3-round2 lesson: never mint
//     PermissionsForAgent off a client-controlled string without
//     re-validating);
//  2. consult agent_provisioning(sid, nid):
//     - row exists with matching fp                     → allow;
//     - row exists with a different fp                  → deny (this nid
//       is taken by another agent identity; operator revokes first);
//     - row missing AND no PIN supplied                 → deny (agent
//       must run `tether agent` with --pin on first boot);
//     - row missing AND a valid PIN supplied            → ProvisionWithPIN
//       (verifies session ACTIVE + PIN, then INSERT), allow.
//
// Architecture K.1 — agent identity is per-machine, per-session, bound
// once at install/provision time and remembered by the broker thereafter.
func (h *Handler) ensureAgentProvisioned(sid, nid, clientNkey, fp, pin string) error {
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
		return nil
	case errors.Is(err, agentprov.ErrNotProvisioned):
		// fall through to PIN-bootstrap below
	default:
		return fmt.Errorf("provisioning lookup: %w", err)
	}

	if pin == "" {
		return fmt.Errorf("agent (sid=%q, nid=%q) not provisioned; first connect must supply --pin", sid, nid)
	}
	switch err := agentprov.ProvisionWithPIN(h.DB, sid, nid, fp, pin, auth.VerifyPIN, h.Now()); {
	case err == nil:
		return nil
	case errors.Is(err, agentprov.ErrSessionMissing):
		return fmt.Errorf("session %q does not exist", sid)
	case errors.Is(err, agentprov.ErrSessionDeleting):
		return fmt.Errorf("session %q is being deleted", sid)
	case errors.Is(err, agentprov.ErrInvalidPIN):
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

func (h *Handler) ensureMember(sid, fp, pin string) error {
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
		return nil
	}
	if pin == "" {
		return fmt.Errorf("not a member of session %q", sid)
	}
	if err := session.JoinWithPIN(h.DB, sid, fp, pin, auth.VerifyPIN, h.Now()); err != nil {
		if errors.Is(err, session.ErrInvalidPIN) {
			return fmt.Errorf("invalid PIN for session %q", sid)
		}
		return fmt.Errorf("join failed: %w", err)
	}
	return nil
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
