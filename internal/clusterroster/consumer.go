package clusterroster

import (
	"net/url"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// Pure consumer adoption and routing decisions shared by agent discovery callers.
// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
// ConsumerState is the agent's consumer discovery state — the input/output of the pure single-authority
// AdoptDecision (C2 §2.5). Both the daemon (adoptManifest, holds rosterMu + persists) and the CLI
// (config refresh / join / doctor) decide through AdoptDecision over this struct, so there is exactly
// ONE verify/adopt enforcement site.
type ConsumerState struct {
	Pin       string
	RosterGen uint64
	SeedGen   uint64
	DialURLs  []string
	SeedURLs  []string
	Roster    *proto.ClusterRoster
	Seeds     *proto.SeedBundle
}

// AdoptDecision is the ONE consumer authority (C1 roster decision table + C2 seed mirror). It verifies
// a roster and/or a seed bundle against the pin (TOFU on the FIRST roster when pin==""), enforces the
// monotone roster/seed generations, and returns the next state + whether anything was accepted. Every
// reject keeps prior state (never wedge, never empty a pool). The pin passed to VerifyAt/VerifySeedsAt
// is the PERSISTED/OOB pin — never the object's self-claimed AccountPub — except the first-TOFU roster.
// templateURL is the agent's cfg.NATSURL (roster DialURLs templating). PURE — no I/O, no lock, the
// only clock is the passed `now`.
func AdoptDecision(prev ConsumerState, r *proto.ClusterRoster, s *proto.SeedBundle, templateURL string, now time.Time) (ConsumerState, bool) {
	next := prev
	accepted := false
	pin := prev.Pin

	if r != nil {
		tofu := pin == ""
		vp := pin
		if tofu {
			vp = r.AccountPub // row 2: trust-on-first-use
		}
		// rows 2-7: sig + account pin + schema + expiry + monotone generation. A reject leaves `next`
		// at `prev` for the roster fields (prior kept).
		if err := VerifyAt(r, vp, now); err == nil && r.Generation >= prev.RosterGen {
			urls, derr := DialURLs(r, templateURL)
			if derr == nil {
				if len(urls) == 0 {
					urls = prev.DialURLs // row E: empty-set floor — keep prior, never empty the pool
				}
				if tofu {
					pin = r.AccountPub
					next.Pin = r.AccountPub
				}
				next.RosterGen = r.Generation
				next.DialURLs = urls
				next.Roster = r
				accepted = true
			}
		}
	}

	// Seeds verify against the SAME pin (possibly just TOFU-set by the roster above). A fresh agent
	// with no pin AND no roster cannot adopt seeds — it has nothing to verify them against.
	if s != nil && pin != "" {
		if err := VerifySeedsAt(s, pin, now); err == nil && s.Generation >= prev.SeedGen {
			next.SeedGen = s.Generation
			next.SeedURLs = SeedDialURLs(s)
			next.Seeds = s
			accepted = true
		}
	}
	return next, accepted
}

// RosterRequiresReconnect is the narrow proactive-move policy: a healthy connection is rebuilt only
// when another dialable VOTER exists and the signed roster either marks the current broker as leaving,
// or removes a broker that the previously accepted roster identified as current. The previous-roster
// fence prevents an unrelated DNS/route alias from looking like a removal on every refresh.
func RosterRequiresReconnect(previous, current *proto.ClusterRoster, connectedURL string) bool {
	cu, err := url.Parse(connectedURL)
	if err != nil || cu.Hostname() == "" || current == nil {
		return false
	}
	connectedHost := cu.Hostname()
	knownBefore := RosterContainsHost(previous, connectedHost)
	currentPresent := false
	currentLeaving := false
	otherVoter := false
	for _, b := range current.Brokers {
		isCurrent := RosterBrokerMatchesHost(b, connectedHost)
		if isCurrent {
			currentPresent = true
			switch b.Phase {
			case proto.RosterPhaseDraining, proto.RosterPhaseRetiring, proto.RosterPhaseAddFailed:
				currentLeaving = true
			}
			continue
		}
		// Dialability filters destinations, not identity. A cfg.NATSURL floor may legitimately connect
		// a local/dev agent to a loopback roster entry; skipping that entry before the identity check
		// makes an unchanged roster look like a removal and churns the session every refresh.
		if b.PublicHost == "" || IsUndialableHost(b.PublicHost) {
			continue
		}
		if b.Phase == proto.RosterPhaseVoter {
			otherVoter = true
		}
	}
	return otherVoter && (currentLeaving || (knownBefore && !currentPresent))
}

func RosterContainsHost(r *proto.ClusterRoster, host string) bool {
	if r == nil {
		return false
	}
	for _, b := range r.Brokers {
		if RosterBrokerMatchesHost(b, host) {
			return true
		}
	}
	return false
}

func RosterBrokerMatchesHost(b proto.RosterBroker, host string) bool {
	if strings.EqualFold(host, b.PublicHost) {
		return true
	}
	ru, err := url.Parse(b.NatsRoute)
	return err == nil && ru.Hostname() != "" && strings.EqualFold(host, ru.Hostname())
}

func UniqueRosterNodeAtHost(r *proto.ClusterRoster, host string) string {
	var id string
	for _, b := range r.Brokers {
		if !RosterBrokerMatchesHost(b, host) {
			continue
		}
		if id != "" && id != b.NodeID {
			return ""
		}
		id = b.NodeID
	}
	return id
}

func AppendRosterAliases(aliases []string, b proto.RosterBroker) []string {
	for _, host := range []string{b.PublicHost, RouteHostname(b.NatsRoute)} {
		if host == "" {
			continue
		}
		found := false
		for _, old := range aliases {
			if strings.EqualFold(old, host) {
				found = true
				break
			}
		}
		if !found {
			aliases = append(aliases, host)
		}
	}
	return aliases
}

func RouteHostname(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// RosterCurrentAt gates use of accepted rosters as current routing evidence.
// A persisted generation/pin survives expiry; address and identity evidence do not.
func RosterCurrentAt(r *proto.ClusterRoster, now time.Time) bool {
	if r == nil {
		return false
	}
	if r.ExpiresAt == "" {
		return true
	}
	exp, err := time.Parse(time.RFC3339Nano, r.ExpiresAt)
	return err == nil && now.Before(exp)
}

// Unknown aliases do not prove another broker exists. A previously identified
// ingress may, however, have disappeared from a roster containing one survivor.
func RosterVoterDestination(r *proto.ClusterRoster, host, id string, aliases []string, now time.Time) (string, bool) {
	if !RosterCurrentAt(r, now) {
		return "", false
	}
	known := id != ""
	avoid := append([]string{host}, aliases...)
	for _, b := range r.Brokers {
		if (id != "" && b.NodeID == id) || RosterBrokerMatchesHost(b, host) {
			known = true
			if id == "" {
				id = b.NodeID
			}
			avoid = AppendRosterAliases(avoid, b)
		}
	}
	if !known {
		return "", false
	}
	for _, b := range r.Brokers {
		if b.Phase != proto.RosterPhaseVoter || b.PublicHost == "" || IsUndialableHost(b.PublicHost) || (id != "" && b.NodeID == id) {
			continue
		}
		isCurrent := false
		for _, alias := range avoid {
			if RosterBrokerMatchesHost(b, alias) {
				isCurrent = true
				break
			}
		}
		if !isCurrent {
			return strings.Join(avoid, ","), true
		}
	}
	return "", false
}

// ExcludeHostFromDial drops every comma-separated dial URL whose host equals `host` (the just-silent
// island broker). If the filter would strand the agent (empty result), it returns the input
// unchanged — never trade a stuck-on-island for a stuck-on-nothing.
func ExcludeHostFromDial(csv, host string) string {
	if csv == "" || host == "" {
		return csv
	}
	parts := strings.Split(csv, ",")
	kept := make([]string, 0, len(parts))
	for _, p := range parts {
		u, err := url.Parse(strings.TrimSpace(p))
		excluded := false
		if err == nil && u.Hostname() != "" {
			for _, alias := range strings.Split(host, ",") {
				if strings.EqualFold(u.Hostname(), alias) {
					excluded = true
					break
				}
			}
		}
		if excluded {
			continue
		}
		kept = append(kept, p)
	}
	if len(kept) == 0 {
		return csv
	}
	return strings.Join(kept, ",")
}
