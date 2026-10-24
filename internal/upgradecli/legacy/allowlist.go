package legacy

import (
	"fmt"
	"strings"
)

// The two upgrade URL allowlist resolvers moved verbatim from cmd/tether
// (serve.go and agent.go) with their comments; cmd keeps only the call sites
// that read cobra's Changed bit. origin: hot-update S8 plan §11.2 P19 slim move.

// DefaultUpgradeURLPrefix matches the GitHub release URL that
// install.sh and goreleaser produce; keeping it here (rather than in
// serveconf) so a self-hosted distribution can override via yaml or
// flag without recompiling. Architecture J.4: "默认
// https://github.com/<org>/tether/releases/".
const DefaultUpgradeURLPrefix = "https://github.com/LinZiyang666/dist_experiment_control/releases/"

// BrokerUpgradeAllow is the broker's precedence: explicit
// --upgrade-url-allow > yaml > built-in default. The
// built-in default points at this binary's own GitHub
// release prefix so `tether node upgrade` works out of
// the box; operators who self-host artifacts override
// via yaml or flag.
func BrokerUpgradeAllow(flagChanged bool, flagVal, yamlVal []string) []string {
	allow := flagVal
	if !flagChanged {
		switch {
		case len(yamlVal) > 0:
			allow = yamlVal
		case len(allow) == 0:
			allow = []string{DefaultUpgradeURLPrefix}
		}
	}
	return allow
}

// AgentUpgradeAllow applies #28's precedence for the agent's local upgrade-URL allowlist:
//
//	explicit --upgrade-url-allow  >  agent.yaml `upgrade.url_allow`  >  the agent's built-in default
//
// This is the same shape `tether serve` already uses for the broker's copy of the same setting. An empty
// result is NOT "deny everything": internal/agent falls back to defaultAgentURLAllowlist, so every
// pre-#28 install keeps its exact behaviour.
//
// Entries are validated, because the failure mode of a typo here is silent and confusing: urlAllowed is
// a plain strings.HasPrefix test, so "githib.com/…" or a bare hostname simply never matches, and the
// operator sees url_not_allowed_local for a URL they believe they allowed. Fail at startup instead.
func AgentUpgradeAllow(flagChanged bool, flagVal, yamlVal []string) ([]string, error) {
	allow := flagVal
	if !flagChanged {
		allow = yamlVal
	}
	for _, p := range allow {
		if !strings.HasPrefix(p, "https://") && !strings.HasPrefix(p, "http://") {
			return nil, fmt.Errorf("upgrade URL allowlist entry %q is not a URL prefix — entries must begin with "+
				"https:// (or http://). The allowlist is matched by prefix, so a non-URL entry can never match a "+
				"download URL: it would silently DISABLE upgrades rather than permit them. "+
				"Set it via --upgrade-url-allow or the `upgrade.url_allow` list in agent.yaml", p)
		}
	}
	return allow, nil
}

// origin: S10 internal review MAKETEST-GATES F4
// subURLBase derives the public origin printed in P13 subscription URLs from
// the broker's public host. https is assumed (Caddy terminates TLS on :443).
func BrokerSubURLBase(publicHost string) string {
	if publicHost == "" || publicHost == "localhost" {
		return ""
	}
	return "https://" + publicHost
}
