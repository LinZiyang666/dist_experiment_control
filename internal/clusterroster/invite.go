package clusterroster

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/nats-io/nkeys"
)

// invite.go (C2) — the OOB-delivered, UNSIGNED agent-join token. account_pub (Pin) is the trust
// anchor it pre-seeds into the consumer (closing C1's TOFU window); integrity comes from the OOB
// delivery channel (parity with handing broker_url+account_pub today). It carries ZERO secrets — no
// session PIN, no seed material, no private key (拒绝表 #2). ONE strict canonical format.

const (
	inviteScheme  = "tether-invite"
	inviteVersion = "v1"
	// devNoAuthEnv reuses the existing dev escape hatch (cli.DevNoAuthEnv) to allow plaintext
	// http/ws bootstrap in dev without a second env var. Read directly so this leaf imports no cli.
	devNoAuthEnv = "TETHER_DEV_NO_AUTH"
)

// Invite is the parsed join token.
type Invite struct {
	Pin          string // account public key — the OOB-trusted pin (pre-seeds Config.AccountPub)
	BootstrapURL string // https well-known manifest URL
	SID          string
	Seed         string // OPTIONAL inline client-dialable NATS URL (cold-start dial floor)
}

// ErrBadInvite is the sentinel for any malformed/invalid invite.
var ErrBadInvite = errors.New("clusterroster: malformed invite")

func devPlaintextAllowed() bool { return os.Getenv(devNoAuthEnv) == "1" }

// MintInvite renders an Invite to the canonical `tether-invite:v1?pin=&url=&sid=[&seed=]` token.
// Validates pin is an account public key, url is https (unless dev), seed (if present) is nats/tls/wss.
func MintInvite(in Invite) (string, error) {
	if !nkeys.IsValidPublicAccountKey(in.Pin) {
		return "", fmt.Errorf("%w: pin must be an account public key", ErrBadInvite)
	}
	if err := validateBootstrapURL(in.BootstrapURL); err != nil {
		return "", err
	}
	if strings.TrimSpace(in.SID) == "" {
		return "", fmt.Errorf("%w: sid required", ErrBadInvite)
	}
	if in.Seed != "" {
		if err := validateSeedURL(in.Seed); err != nil {
			return "", err
		}
	}
	q := url.Values{}
	q.Set("pin", in.Pin)
	q.Set("url", in.BootstrapURL)
	q.Set("sid", in.SID)
	if in.Seed != "" {
		q.Set("seed", in.Seed)
	}
	return inviteScheme + ":" + inviteVersion + "?" + q.Encode(), nil
}

// ParseInvite strictly parses a canonical invite token. Rejects a wrong scheme/version, unknown
// params, a non-account-key pin, or a non-http(s) bootstrap url.
func ParseInvite(token string) (Invite, error) {
	u, err := url.Parse(strings.TrimSpace(token))
	if err != nil {
		return Invite{}, fmt.Errorf("%w: %v", ErrBadInvite, err)
	}
	if u.Scheme != inviteScheme || u.Opaque != inviteVersion {
		return Invite{}, fmt.Errorf("%w: expected scheme %q version %q", ErrBadInvite, inviteScheme, inviteVersion)
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return Invite{}, fmt.Errorf("%w: bad query: %v", ErrBadInvite, err)
	}
	allowed := map[string]bool{"pin": true, "url": true, "sid": true, "seed": true}
	for k := range q {
		if !allowed[k] {
			return Invite{}, fmt.Errorf("%w: unknown param %q", ErrBadInvite, k)
		}
	}
	in := Invite{Pin: q.Get("pin"), BootstrapURL: q.Get("url"), SID: q.Get("sid"), Seed: q.Get("seed")}
	if !nkeys.IsValidPublicAccountKey(in.Pin) {
		return Invite{}, fmt.Errorf("%w: pin must be an account public key", ErrBadInvite)
	}
	if err := validateBootstrapURL(in.BootstrapURL); err != nil {
		return Invite{}, err
	}
	if strings.TrimSpace(in.SID) == "" {
		return Invite{}, fmt.Errorf("%w: sid required", ErrBadInvite)
	}
	if in.Seed != "" {
		if err := validateSeedURL(in.Seed); err != nil {
			return Invite{}, err
		}
	}
	return in, nil
}

// MintDiscoveryInvite renders a SID-LESS ctl discovery invite to the canonical
// `tether-invite:v1?pin=[&url=][&seed=]` token. Unlike MintInvite (agent join, SID required + carries a
// session), this is the OPERATOR→OPERATOR cold-start bootstrap a ctl uses to learn the cluster pin +
// failover endpoints. Requires a valid account-pub pin and AT LEAST ONE of bootstrap url (https, tier-2
// HTTP manifest) or seed (nats/tls/wss, tier-1 cold-start dial floor) — else it carries no endpoint and
// is useless. Carries ZERO secrets (no PIN, no seed material, no key).
func MintDiscoveryInvite(in Invite) (string, error) {
	if !nkeys.IsValidPublicAccountKey(in.Pin) {
		return "", fmt.Errorf("%w: pin must be an account public key", ErrBadInvite)
	}
	if strings.TrimSpace(in.BootstrapURL) == "" && strings.TrimSpace(in.Seed) == "" {
		return "", fmt.Errorf("%w: discovery invite needs a bootstrap url or a seed", ErrBadInvite)
	}
	if in.BootstrapURL != "" {
		if err := validateBootstrapURL(in.BootstrapURL); err != nil {
			return "", err
		}
	}
	if in.Seed != "" {
		if err := validateSeedURL(in.Seed); err != nil {
			return "", err
		}
	}
	q := url.Values{}
	q.Set("pin", in.Pin)
	if in.BootstrapURL != "" {
		q.Set("url", in.BootstrapURL)
	}
	if in.Seed != "" {
		q.Set("seed", in.Seed)
	}
	return inviteScheme + ":" + inviteVersion + "?" + q.Encode(), nil
}

// ParseDiscoveryInvite strictly parses a ctl discovery invite. Same scheme/version/param allowlist as
// ParseInvite, but SID is OPTIONAL (ignored if present — an agent-join token also parses here, its sid is
// simply unused for ctl discovery) and a bootstrap url OR seed is REQUIRED. A discovery-only invite
// (no sid) deliberately FAILS ParseInvite, so it can never be mistaken for an agent-join token.
func ParseDiscoveryInvite(token string) (Invite, error) {
	u, err := url.Parse(strings.TrimSpace(token))
	if err != nil {
		return Invite{}, fmt.Errorf("%w: %v", ErrBadInvite, err)
	}
	if u.Scheme != inviteScheme || u.Opaque != inviteVersion {
		return Invite{}, fmt.Errorf("%w: expected scheme %q version %q", ErrBadInvite, inviteScheme, inviteVersion)
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return Invite{}, fmt.Errorf("%w: bad query: %v", ErrBadInvite, err)
	}
	allowed := map[string]bool{"pin": true, "url": true, "sid": true, "seed": true}
	for k := range q {
		if !allowed[k] {
			return Invite{}, fmt.Errorf("%w: unknown param %q", ErrBadInvite, k)
		}
	}
	in := Invite{Pin: q.Get("pin"), BootstrapURL: q.Get("url"), SID: q.Get("sid"), Seed: q.Get("seed")}
	if !nkeys.IsValidPublicAccountKey(in.Pin) {
		return Invite{}, fmt.Errorf("%w: pin must be an account public key", ErrBadInvite)
	}
	if strings.TrimSpace(in.BootstrapURL) == "" && strings.TrimSpace(in.Seed) == "" {
		return Invite{}, fmt.Errorf("%w: discovery invite needs a bootstrap url or a seed", ErrBadInvite)
	}
	if in.BootstrapURL != "" {
		if err := validateBootstrapURL(in.BootstrapURL); err != nil {
			return Invite{}, err
		}
	}
	if in.Seed != "" {
		if err := validateSeedURL(in.Seed); err != nil {
			return Invite{}, err
		}
	}
	return in, nil
}

func validateBootstrapURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("%w: bad bootstrap url %q", ErrBadInvite, raw)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if devPlaintextAllowed() {
			return nil
		}
		return fmt.Errorf("%w: bootstrap url must be https (set %s=1 to allow http in dev)", ErrBadInvite, devNoAuthEnv)
	default:
		return fmt.Errorf("%w: bootstrap url scheme must be http(s), got %q", ErrBadInvite, u.Scheme)
	}
}

func validateSeedURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("%w: bad seed url %q", ErrBadInvite, raw)
	}
	switch u.Scheme {
	case "nats", "tls", "wss":
		return nil
	case "ws":
		if devPlaintextAllowed() {
			return nil
		}
		return fmt.Errorf("%w: seed url must be nats/tls/wss (set %s=1 to allow ws in dev)", ErrBadInvite, devNoAuthEnv)
	default:
		return fmt.Errorf("%w: seed url scheme must be nats/tls/wss, got %q", ErrBadInvite, u.Scheme)
	}
}
