package fullstackharness

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// RequestSchedule freezes the denominator before any request can be launched.
// EndNS is the exclusive scheduling ceiling, not the drain deadline: a request
// in the last slot still owns its original five seconds. The caller must use
// the scheduler host's clock for both bounds and every attempt receipt.
// origin: hot-update S10 plan §3.2 / §4.4 / P17
func RequestSchedule(p contract.Profile) ([]contract.Slot, error) {
	const intervalNS, latencyNS int64 = 100_000_000, 5_000_000_000
	if p.ID == "" || p.ProbeRate != 10 || p.StartNS < 0 || p.EndNS <= p.StartNS || p.EndNS > math.MaxInt64-latencyNS {
		return nil, errors.New("request schedule requires an ID, 10/s and valid absolute clock bounds")
	}
	// §9.2's largest frozen arm is 12000s. Refuse malformed bounds before
	// allocation; never truncate a ledger or silently shorten a valid arm.
	if p.EndNS-p.StartNS > 12_000_000_000_000 {
		return nil, errors.New("request schedule exceeds the frozen fullstack arm ceiling")
	}
	var targets []contract.Key
	for _, key := range p.Required {
		if key.Role == "agent" {
			targets = append(targets, key)
		}
	}
	if len(targets) != 4 {
		return nil, errors.New("request schedule requires four independent agent runtimes")
	}
	// Required also includes infrastructure. Sorting only the agent projection
	// makes a manifest reorder harmless without filtering out an offline agent.
	sort.Slice(targets, func(i, j int) bool { return targets[i].NID < targets[j].NID })
	sids, nids, domains := map[string]bool{}, map[string]bool{}, map[string]bool{}
	units, users := map[string]bool{}, map[string]bool{}
	for _, key := range targets {
		if _, err := contract.EncodeJSON("key", key); err != nil {
			return nil, fmt.Errorf("request schedule target: %w", err)
		}
		unit, user := key.Host+"\x00"+key.Unit, fmt.Sprintf("%s\x00%d", key.Host, key.UID)
		if key.SID == "" || key.NID == "" || sids[key.SID] || nids[key.NID] || domains[key.Domain] || units[unit] || users[user] {
			return nil, errors.New("request schedule targets share a session, node, domain, unit or user")
		}
		sids[key.SID], nids[key.NID], domains[key.Domain], units[unit], users[user] = true, true, true, true, true
	}
	count := (p.EndNS-p.StartNS-1)/intervalNS + 1
	if count < 4 {
		return nil, errors.New("request schedule does not cover four runtimes")
	}
	slots := make([]contract.Slot, 0, int(count))
	classes := [...]string{"read", "read", "read", "write", "short-exec"}
	for i := int64(0); i < count; i++ {
		at := p.StartNS + i*intervalNS
		slots = append(slots, contract.Slot{
			ID: fmt.Sprintf("%s/slot/%08d", p.ID, i), Class: classes[i%5],
			Target: targets[i%4], ScheduledNS: at, DeadlineNS: at + latencyNS,
		})
	}
	return slots, nil
}

// SealRequestSchedule emits P00's snake_case slot codec and the exact bytes
// digest. P19 seals these bytes in its manifest before READY; Python consumes
// them and never derives the expected population from successful launches.
// Keep the original bytes: reformatting JSON changes this evidence identity.
// origin: hot-update S10 plan §5.1 / §8 M10S / §10.3
func SealRequestSchedule(p contract.Profile) ([]byte, contract.Digest, error) {
	slots, err := RequestSchedule(p)
	if err != nil {
		return nil, contract.Digest{}, err
	}
	rows := make([]json.RawMessage, 0, len(slots))
	for _, slot := range slots {
		row, encodeErr := contract.EncodeJSON("slot", slot)
		if encodeErr != nil {
			return nil, contract.Digest{}, encodeErr
		}
		rows = append(rows, row)
	}
	data, err := json.Marshal(rows)
	if err != nil {
		return nil, contract.Digest{}, err
	}
	return data, contract.Digest(sha256.Sum256(data)), nil
}
