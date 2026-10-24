package authcallout

import (
	"errors"
	"strings"

	"github.com/nats-io/jwt/v2"
)

// SystemAccount is the default system account the managed infra principals live
// in. The callout places them there through the user JWT's Audience; it never
// changes the server's system_account selection.
// origin: hot-update S9 plan §3.3 / P12 / T27
const SystemAccount = "$SYS"

// SystemInboxRoot is the private reply root of the managed $SYS principals. Each
// identity owns exactly SystemInboxRoot + "." + <its public nkey> and nothing
// shared, so one adapter can never read another identity's replies.
// natsconf.SystemPrincipalDescriptors renders the same root; T27 pins the pair.
// origin: hot-update S9 plan §3.3 / P12 / M14-08/M14-09
const SystemInboxRoot = "_INBOX.tether.infra.sys"

// SystemPrincipal is one managed $SYS user of the NATS config this callout
// answers for: the public nkey and the exact allow lists rendered for it
// (natsconf.SystemPrincipalDescriptors of the LOADED file). The callout re-issues
// these lists verbatim and never widens them.
// origin: hot-update S9 plan §3.3 / P12 / T27
type SystemPrincipal struct {
	Nkey      string
	Publish   []string
	Subscribe []string
}

// errSystemPrincipalAuthority keeps the refused detail off the unauthenticated wire.
var errSystemPrincipalAuthority = errors.New("system principal authority is invalid")

// systemPrincipal recognizes a managed $SYS identity by its nonce-PROVEN public
// nkey only. The connection name cannot select it, and an identity listed twice
// is ambiguous, so it is refused rather than resolved by position.
// origin: hot-update S9 plan §3.3 / P12 — a static auth_callout exemption is not
// reloadable (`config reload not supported for AuthCallout`), so the normal
// queue must admit the users that the reloaded $SYS block added.
func (h *Handler) systemPrincipal(provenNkey string) (SystemPrincipal, bool, error) {
	if h.SystemPrincipals == nil {
		return SystemPrincipal{}, false, nil
	}
	var found SystemPrincipal
	matches := 0
	for _, principal := range h.SystemPrincipals() {
		if principal.Nkey == provenNkey {
			found = principal
			matches++
		}
	}
	switch matches {
	case 0:
		return SystemPrincipal{}, false, nil
	case 1:
		return found, true, nil
	default:
		return SystemPrincipal{}, true, errSystemPrincipalAuthority
	}
}

// systemPrincipalPermissions copies the managed allow lists into the user JWT.
// The lists are re-checked here because the callout, not the config file, is
// what finally grants authority: every reply grant must stay under the
// identity's own private root, and no publish grant may hold a full wildcard.
// origin: hot-update S9 plan §3.3 / P12 / T28 least privilege
func systemPrincipalPermissions(principal SystemPrincipal) (jwt.Permissions, error) {
	own := SystemInboxRoot + "." + principal.Nkey + "."
	if len(principal.Publish) == 0 || len(principal.Subscribe) == 0 {
		return jwt.Permissions{}, errSystemPrincipalAuthority
	}
	for _, subject := range principal.Subscribe {
		if !strings.HasPrefix(subject, own) || !validSystemSubject(subject[len(own):]) {
			return jwt.Permissions{}, errSystemPrincipalAuthority
		}
	}
	for _, subject := range principal.Publish {
		if !validSystemSubject(subject) || strings.HasSuffix(subject, ">") || strings.HasPrefix(subject, "*") {
			return jwt.Permissions{}, errSystemPrincipalAuthority
		}
	}
	var perms jwt.Permissions
	perms.Pub.Allow = append(jwt.StringList(nil), principal.Publish...)
	perms.Sub.Allow = append(jwt.StringList(nil), principal.Subscribe...)
	return perms, nil
}

// validSystemSubject refuses empty tokens and whitespace; a full wildcard is
// only legal as the final token, where the caller decides whether it may appear.
func validSystemSubject(subject string) bool {
	if subject == "" || strings.ContainsAny(subject, " \t\r\n") {
		return false
	}
	tokens := strings.Split(subject, ".")
	for i, token := range tokens {
		if token == "" || (token == ">" && i != len(tokens)-1) || (strings.Contains(token, ">") && token != ">") || (strings.Contains(token, "*") && token != "*") {
			return false
		}
	}
	return true
}
