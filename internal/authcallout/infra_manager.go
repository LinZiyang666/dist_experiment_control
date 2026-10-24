package authcallout

import (
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// WithInfraManager adds the separate account-authorized management connection
// to the normal queue. Its CONNECT token is the canonical account-signed request
// it will send, and its name selects only that request's exact target. Ordinary
// members keep their existing grants; a name or local UID carries no authority.
// The targeted responder verifies the request again at its effect boundary.
// origin: hot-update S9 plan §7.1 / T47 / WIRE-2 R14
func WithInfraManager(h *Handler, enabled func(string) bool) func(string) (string, error) {
	if h.Now == nil {
		h.Now = time.Now
	}
	if h.JWTTTL == 0 {
		h.JWTTTL = defaultJWTTTL
	}
	return func(request string) (string, error) {
		r, err := jwt.DecodeAuthorizationRequestClaims(request)
		if err != nil || !strings.HasPrefix(r.ConnectOptions.Name, "tether-infra-manager:") {
			return h.Handle(request)
		}
		actor := r.ConnectOptions.Nkey
		target := strings.TrimPrefix(r.ConnectOptions.Name, "tether-infra-manager:")
		deny := func() (string, error) { return h.deny(r, "infrastructure management authentication denied") }
		if !nkeys.IsValidPublicUserKey(r.UserNkey) || !nkeys.IsValidPublicUserKey(actor) ||
			xferauth.VerifyClientNkey(actor, r.ClientInformation.Nonce, r.ConnectOptions.SignedNonce) != nil ||
			proto.ValidateClusterNodeID(target) != nil || h.AccountKp == nil || h.fenced() || enabled == nil || !enabled(target) {
			return deny()
		}
		command, err := proto.DecodeInfraUpgradeRequest([]byte(r.ConnectOptions.Token))
		if err != nil {
			return deny()
		}
		account, err := h.AccountKp.PublicKey()
		now := time.Now()
		if h.Now != nil {
			now = h.Now()
		}
		prefix := auth.InboxPrefixFor(actor)
		if err != nil || proto.VerifyInfraUpgradeRequest(command, account, actor, target, prefix, prefix+".connect", now) != nil {
			return deny()
		}
		return h.allow(r, r.UserNkey, auth.PermissionsForInfraManager(actor, target))
	}
}
