package acceptance

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// Resource IDs name logical responsibilities; several kernel references can
// share an ID. FD/OFD rows remain a multiset, so leaving a dup cannot compensate
// for closing the original. These classes describe observations, not image ABI
// kinds. Adapters preserve their upstream raw schemas beside this projection.
// origin: hot-update S10 plan §4.2 / P03
func resourceClass(kind string) string {
	switch kind {
	case "pipe", "pty-master", "raw-socket", "listener", "eventpoll", "eventfd", "file", "directory", "pidfd", "source", "tmp", "parent",
		"handoff-memfd", "sealed-batch", "custody-memfd", "custody-seqpacket", "lock", "old-executable":
		return "fd"
	case "child", "sole-waiter", "pty-input", "pty-output", "replay", "consumer", "temp":
		return "data"
	case "lease", "terminal-receipt", "retained-domain", "tombstone", "revocation",
		"transfer-grant", "canonical-publication", "cleanup-responsibility", "history", "courier", "gc", "no-reexec-fence":
		return "metadata"
	default:
		return ""
	}
}

func resourceState(state string) bool {
	switch state {
	case "fixed", "live", "reap-held", "terminal-unacked", "settled-unexpired",
		"expired", "collected", "abandoned", "tombstone", "revoked", "pending", "settled":
		return true
	default:
		return false
	}
}

func resourceDomain(domain string) bool {
	switch domain {
	case "control", "exec", "pty", "flow", "ss", "transfer_worker", "transfer_tracker",
		"broker_store", "broker_runtime", "authority", "upgrade", "custody", "observer", "harness":
		return true
	default:
		return false
	}
}

// A fixed observer and the current broker store lock are baseline costs. A
// caller cannot use fixed to exempt replay, a task, or an upgrade-domain lock.
func fixedResource(r contract.Resource) bool {
	if r.State != "fixed" {
		return false
	}
	switch r.Domain {
	case "control", "broker_runtime", "harness", "custody", "observer":
		return resourceClass(r.Kind) == "fd" && r.Kind != "handoff-memfd" && r.Kind != "sealed-batch" && r.Kind != "old-executable" && r.Kind != "lock"
	case "broker_store":
		return r.Kind == "lock"
	default:
		return false
	}
}

func retainedResource(r contract.Resource) bool {
	if resourceClass(r.Kind) != "metadata" {
		return false
	}
	switch r.State {
	case "settled-unexpired", "abandoned", "tombstone", "revoked", "pending", "settled":
		return true
	default:
		return false
	}
}

func resourceProblems(s contract.Sample) []string {
	var problems []string
	fdSeen := make(map[int]bool)
	metadataSeen := make(map[string]bool)
	byID := make(map[string][]contract.Resource)
	for _, r := range s.Resources {
		class := resourceClass(r.Kind)
		if r.ID == "" || class == "" || !resourceState(r.State) || !resourceDomain(r.Domain) {
			problems = append(problems, "unknown resource class, state, domain or ID")
			continue
		}
		byID[r.ID] = append(byID[r.ID], r)
		if !sameResourceOwner(r.Owner, s.Stamp) {
			problems = append(problems, "resource has a foreign process, cut or generation owner")
		}
		if r.Bytes < 0 || r.ReleaseAfterNS < 0 || r.FD < -1 {
			problems = append(problems, "negative resource quantity")
		}
		if r.FD >= 0 {
			if fdSeen[r.FD] || r.OFD == "" {
				problems = append(problems, "duplicate FD row or missing OFD witness")
			}
			fdSeen[r.FD] = true
			// Samples describe activated, serving owners. The handoff window is
			// not a license to leak inheritable descriptors into later children.
			if r.FD >= 3 && r.Flags&1 == 0 {
				problems = append(problems, "serving resource lacks CLOEXEC")
			}
		}
		if class == "fd" && r.FD < 0 {
			problems = append(problems, "kernel resource lacks its FD")
		}
		// Empty streams still reserve their full capacity. A zero-byte replay
		// row must not disguise a missed reservation or missing memfd.
		if r.Kind == "replay" && (r.Bytes == 0 || r.FD < 0) {
			problems = append(problems, "replay lacks its capacity reservation or memfd")
		}
		if class == "metadata" && (r.FD != -1 || r.OFD != "" || r.Flags != 0) {
			problems = append(problems, "retained metadata hides a kernel reference")
		}
		if class == "metadata" {
			key := r.ID + "/" + r.Kind + "/" + r.Domain
			if metadataSeen[key] {
				problems = append(problems, "duplicate retained metadata responsibility")
			}
			metadataSeen[key] = true
		}
		if (r.Domain == "exec" || r.Domain == "pty") && (r.State == "pending" || r.State == "settled") {
			problems = append(problems, "task uses a transfer-only retention state")
		}
		if r.State == "fixed" && !fixedResource(r) {
			problems = append(problems, "transient resource disguised as fixed baseline")
		}
		if r.Kind == "handoff-memfd" || r.Kind == "old-executable" {
			problems = append(problems, "activated owner retains handoff or old executable")
		}
		// origin: hot-update S10 plan §4.2 / P03
		if (r.State == "settled-unexpired" || r.State == "settled") && class != "metadata" {
			problems = append(problems, "settled responsibility retains data FD, worker or replay")
		}
		if r.State == "expired" || r.State == "collected" {
			if r.Kind != "no-reexec-fence" || r.ReleaseAfterNS != 0 {
				problems = append(problems, "collected responsibility retains more than its no-reexec fence")
			}
		}
		if r.Kind == "no-reexec-fence" && r.State != "expired" && r.State != "collected" {
			problems = append(problems, "no-reexec fence disguises an unsettled responsibility")
		}
		unresolved := (r.Domain == "transfer_worker" || r.Domain == "transfer_tracker") &&
			(r.State == "pending" || r.State == "abandoned")
		if retainedResource(r) && !unresolved &&
			(r.ReleaseAfterNS == 0 || r.ReleaseAfterNS <= s.Stamp.AtBootNS) {
			problems = append(problems, "retained metadata lacks an unexpired original deadline")
		}
	}
	if int64(len(fdSeen)) != s.FDs {
		problems = append(problems, "classified FD census does not equal the observed FD count")
	}
	consumers, temps, locks, oldImages := resourceSets(s.Resources)
	if !sameResourceIDs(consumers, s.Consumers) || !sameResourceIDs(temps, s.Temps) ||
		!sameResourceIDs(locks, s.Locks) || !sameResourceIDs(oldImages, s.OldImages) {
		problems = append(problems, "classified resource sets do not equal the observed consumer, temp, lock or image census")
	}
	for _, rows := range byID {
		problems = append(problems, taskRetentionProblems(rows)...)
	}
	return problems
}

// Set counters cannot silently omit a held descriptor/consumer row. Several
// rows may describe one temp (its data and directory FDs), hence unique IDs in
// these side sets while Resources continues to preserve reference multiplicity.
func resourceSets(rows []contract.Resource) (consumers, temps, locks, oldImages []string) {
	seen := make(map[string]bool)
	for _, r := range rows {
		key := r.Kind + "/" + r.ID
		if r.Kind == "tmp" || r.Kind == "temp" {
			key = "temp/" + r.ID
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		switch r.Kind {
		case "consumer":
			consumers = append(consumers, r.ID)
		case "tmp", "temp":
			temps = append(temps, r.ID)
		case "lock":
			locks = append(locks, r.ID)
		case "old-executable":
			oldImages = append(oldImages, r.ID)
		}
	}
	return consumers, temps, locks, oldImages
}

// A process terminal is not a settlement. Require the resources appropriate to
// each task state without incorrectly collecting its still-valid registry row.
func taskRetentionProblems(rows []contract.Resource) []string {
	var problems []string
	for _, r := range rows {
		if r.Domain != "exec" && r.Domain != "pty" {
			continue
		}
		has := func(kind string) bool { return resourceKindCount(rows, r.Domain, kind) > 0 }
		switch r.State {
		case "live":
			if !has("child") || !has("sole-waiter") {
				problems = append(problems, "live task lacks original child or sole waiter")
			}
			if r.Domain == "exec" && (resourceKindCount(rows, r.Domain, "pipe") != 3 || resourceKindCount(rows, r.Domain, "replay") != 3) {
				problems = append(problems, "live exec lacks three pipes or three replay reservations")
			}
			if r.Domain == "pty" && (!has("pty-master") || !has("pty-input") || !has("pty-output") || resourceKindCount(rows, r.Domain, "replay") != 2) {
				problems = append(problems, "live PTY lacks its master, pumps or two replay reservations")
			}
		case "reap-held":
			if !has("child") || !has("sole-waiter") || (!has("pipe") && !has("pty-master") && !has("source")) {
				problems = append(problems, "ReapHeld task lost its child, waiter or tail source")
			}
		case "terminal-unacked":
			if !has("terminal-receipt") || !has("replay") {
				problems = append(problems, "unacknowledged terminal lost exact receipt or replay")
			}
		case "settled-unexpired":
			if !has("lease") || !has("terminal-receipt") || !has("retained-domain") {
				problems = append(problems, "settled task lost original lease, terminal receipt or retained domain")
			}
		}
	}
	return problems
}

func resourceKindCount(rows []contract.Resource, domain, kind string) int {
	n := 0
	for _, r := range rows {
		if r.Domain == domain && r.Kind == kind {
			n++
		}
	}
	return n
}

func sameResourceOwner(a, b contract.Stamp) bool {
	return a.Process == b.Process && a.Generation == b.Generation && a.TxID == b.TxID &&
		a.CutID == b.CutID && a.Direction == b.Direction && a.ScopeDigest == b.ScopeDigest &&
		a.Revision > 0 && a.Revision <= b.Revision && a.Sequence > 0 && a.AtBootNS > 0 && a.AtBootNS <= b.AtBootNS
}

// Within an image, all background references must stay identical. Across an
// exec, descriptor numbers can change while the independently witnessed OFD
// and original responsibility remain the same. Observation stamps are not part
// of a kernel reference's identity, but ownership is checked separately above.
func resourceBag(rows []contract.Resource, persistentOnly, acrossExec bool) map[contract.Resource]int {
	bag := make(map[contract.Resource]int)
	for _, r := range rows {
		if retainedResource(r) || r.Kind == "no-reexec-fence" || (persistentOnly && fixedResource(r) && r.Domain != "broker_store") {
			continue
		}
		r.Owner = contract.Stamp{}
		if acrossExec {
			r.FD = -1
		}
		bag[r]++
	}
	return bag
}

func sameResourceBag(a, b []contract.Resource, persistentOnly, acrossExec bool) bool {
	return reflect.DeepEqual(resourceBag(a, persistentOnly, acrossExec), resourceBag(b, persistentOnly, acrossExec))
}

func sameResourceIDs(a, b []string) bool {
	left, right := append([]string{}, a...), append([]string{}, b...)
	sort.Strings(left)
	sort.Strings(right)
	return reflect.DeepEqual(left, right)
}

func resourceLabel(s contract.Sample) string {
	return fmt.Sprintf("%s/%s/%s generation %d sample %d", s.Stamp.Process.Key.Role,
		s.Stamp.Process.Key.Host, s.Stamp.Process.Key.Unit, s.Stamp.Generation, s.Stamp.Sequence)
}
