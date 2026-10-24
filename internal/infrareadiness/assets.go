package infrareadiness

import (
	"sort"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

type Account struct {
	ID        string
	Streams   uint32
	Consumers uint32
}

// Snapshot contains identities/configuration and placement, never data payloads.
// Complete is set only after every JS-enabled account and both LIST families
// have independently agreed on their totals and the full response limits held.
type Snapshot struct {
	Accounts []Account
	Groups   []Group
	Meta     Group
	Complete bool
}

func validateSnapshot(t Topology, s Snapshot, targetHost string) error {
	if !s.Complete || len(s.Accounts) == 0 {
		return infracontract.ErrIncomplete
	}
	if t.Standalone {
		if s.Meta.ID != "" || len(s.Meta.Members) != 0 {
			return infracontract.ErrIncomplete
		}
	} else {
		js := 0
		for _, server := range t.Servers {
			if server.JetStream {
				js++
			}
		}
		if s.Meta.Replicas != js || validateGroup(t, s.Meta, targetHost, true) != nil {
			return infracontract.ErrIncomplete
		}
	}
	accounts := make(map[string]Account)
	for _, a := range s.Accounts {
		if _, exists := accounts[a.ID]; a.ID == "" || exists {
			return infracontract.ErrIncomplete
		}
		accounts[a.ID] = a
	}
	streams, consumers := make(map[string]uint32), make(map[string]uint32)
	identities, names := make(map[string]bool), make(map[string]bool)
	for _, g := range s.Groups {
		_, accountExists := accounts[g.Account]
		key := g.Account + "\x00" + g.Stream + "\x00" + g.Consumer
		if !accountExists || g.Stream == "" || g.CreatedNS <= 0 || g.ConfigSHA256 == (infracontract.Digest{}) ||
			identities[g.ID] || names[key] || validateGroup(t, g, targetHost, false) != nil {
			return infracontract.ErrIncomplete
		}
		identities[g.ID], names[key] = true, true
		if g.Consumer == "" {
			streams[g.Account]++
		} else {
			consumers[g.Account]++
		}
	}
	for _, g := range s.Groups {
		if g.Consumer != "" && !names[g.Account+"\x00"+g.Stream+"\x00"] {
			return infracontract.ErrIncomplete
		}
	}
	for _, a := range s.Accounts {
		// Consumer omission is a topology error even when every stream is healthy.
		// origin: hot-update S9 plan §3.2 / T24 / T25 / M13
		if streams[a.ID] != a.Streams || consumers[a.ID] != a.Consumers {
			return infracontract.ErrIncomplete
		}
	}
	return nil
}

// SealDigests defines the shared configuration-fence projection for P18/P22.
// Health is deliberately excluded: placement currentness is read separately,
// whereas creation/config/group identity must remain bound through the cut.
// origin: hot-update S9 plan §3.2 / T25
func SealDigests(s Snapshot) (infracontract.Digest, infracontract.Digest, error) {
	if !s.Complete {
		return infracontract.Digest{}, infracontract.Digest{}, infracontract.ErrIncomplete
	}
	s = canonicalSnapshot(s)
	ids := make([]string, len(s.Accounts))
	for i, a := range s.Accounts {
		if a.ID == "" || (i > 0 && ids[i-1] == a.ID) {
			return infracontract.Digest{}, infracontract.Digest{}, infracontract.ErrIncomplete
		}
		ids[i] = a.ID
	}
	// Clear only live observations; all asset identity/configuration fields stay.
	for i := range s.Groups {
		s.Groups[i].Leader = ""
		s.Groups[i].Members = nil
	}
	return digest(ids), digest(s.Groups), nil
}

func canonicalSnapshot(s Snapshot) Snapshot {
	s.Accounts = append([]Account(nil), s.Accounts...)
	s.Groups = append([]Group(nil), s.Groups...)
	sort.Slice(s.Accounts, func(i, j int) bool { return s.Accounts[i].ID < s.Accounts[j].ID })
	sort.Slice(s.Groups, func(i, j int) bool { return s.Groups[i].ID < s.Groups[j].ID })
	for i := range s.Groups {
		s.Groups[i] = canonicalGroup(s.Groups[i])
	}
	s.Meta = canonicalGroup(s.Meta)
	return s
}

func canonicalGroup(g Group) Group {
	g.Members = append([]Member(nil), g.Members...)
	sort.Slice(g.Members, func(i, j int) bool { return g.Members[i].ServerID < g.Members[j].ServerID })
	return g
}
