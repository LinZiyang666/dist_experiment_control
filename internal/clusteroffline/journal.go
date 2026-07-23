package clusteroffline

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"
)

// journal.go — the force-single RECOVERY JOURNAL (external review round-5 B1).
//
// force-single is a multi-phase, partly-irreversible disk surgery: recover the FSM → raise the
// force_single_active marker → prune the abandoned roster → rebuild+exchange the raft store → (back in the
// CLI) de-cluster nats.conf to standalone. Only the LAST phase makes the node bootable again: a clustered
// nats.conf on a lone N=1 node can never form the JetStream meta quorum, so the broker fail-closes with
// exit 70 and crash-loops forever.
//
// Before round-5 there was no record that the sequence had begun. A process death anywhere after the prune
// (SIGPIPE from a harness pipe — which is exactly how drill 91 produced this state live — but equally an
// SSH drop, OOM, power cut or operator Ctrl-C) left a node that:
//   - crash-loops (clustered conf at N=1), AND
//   - REFUSES to re-run force-single (it demands --confirm-peers-dead for every peer, and the peers were
//     already pruned away), AND
//   - cannot use `reconcile nats --to-standalone` either, because that is socket-gated on the very broker
//     that cannot start.
// i.e. an un-recoverable brick reachable by a single lost signal.
//
// The journal fixes the re-entrancy half: it is written and fsync'd BEFORE the first mutation, records the
// operator's confirmed-dead set + the phase reached, and lets a re-run FORWARD-COMPLETE the remaining
// phases without re-demanding confirmation for peers that are already gone. It is removed only after the
// final phase (the nats.conf de-cluster) has succeeded.

const journalFileName = ".force-single.journal"

// Phase names, in order. A journal at phase P means P COMPLETED.
const (
	phaseStarted     = "started"      // pre-checks passed; mutations are about to begin
	phaseRaftRebuilt = "raft_rebuilt" // FSM recovered, marker raised, roster pruned, raft store exchanged
	// (no phase constant for the de-cluster: its success DELETES the journal)
)

type forceSingleJournal struct {
	SelfID        string   `json:"self_id"`
	SelfRaftAddr  string   `json:"self_raft_addr"`
	ConfirmedDead []string `json:"confirmed_dead"`
	Phase         string   `json:"phase"`
	// JSResetEpoch (R16 A3) is a per-incident stable timestamp minted when the raft rebuild lands. The CLI
	// names the JS-store move-aside backup (jetstream.force-single-bak.<epoch>) with it so a resume reuses
	// the SAME backup path → backup-dir-first idempotent. Cleared with the journal at completion, so a later
	// incident mints a fresh epoch (a fixed name would unsafely no-op a second incident's clustered store).
	JSResetEpoch string `json:"js_reset_epoch,omitempty"`
}

// ForceSingleJSResetEpoch returns the per-incident JS-reset epoch recorded in an in-flight force-single
// journal (R16 A3), or "" if there is no journal (nothing in flight). The CLI uses it to name the survivor
// JS-store move-aside backup stably across resumes.
func ForceSingleJSResetEpoch(dataDir string) (string, error) {
	j, err := readJournal(dataDir)
	if err != nil || j == nil {
		return "", err
	}
	return j.JSResetEpoch, nil
}

func journalPath(dataDir string) string { return filepath.Join(dataDir, journalFileName) }

// readJournal returns the interrupted-run journal, or (nil, nil) when there is none.
func readJournal(dataDir string) (*forceSingleJournal, error) {
	// Round-6: O_NOFOLLOW — the data dir is tether-writable and this may run as root, so a symlink planted
	// AT the journal path must not redirect the read onto an arbitrary file.
	f, err := os.OpenFile(journalPath(dataDir), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		if errors.Is(err, syscall.ELOOP) {
			return nil, fmt.Errorf("clusteroffline: %s is a SYMLINK — refusing to follow it; remove it and inspect why it is there", journalPath(dataDir))
		}
		return nil, fmt.Errorf("clusteroffline: read force-single journal: %w", err)
	}
	b, err := io.ReadAll(f)
	_ = f.Close()
	if err != nil {
		return nil, fmt.Errorf("clusteroffline: read force-single journal: %w", err)
	}
	var j forceSingleJournal
	if err := json.Unmarshal(b, &j); err != nil {
		// A corrupt journal must not silently vanish: it is the only evidence that a rebuild was in flight.
		return nil, fmt.Errorf("clusteroffline: force-single journal at %s is corrupt (%w) — inspect it by hand; do NOT delete it until the node is known good", journalPath(dataDir), err)
	}
	return &j, nil
}

// writeJournal persists the journal DURABLY (temp + fsync + rename + parent dir fsync) so a crash cannot
// leave a torn record of an in-flight rebuild.
func writeJournal(dataDir string, j *forceSingleJournal) error {
	b, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("clusteroffline: encode force-single journal: %w", err)
	}
	// Round-6: NEVER O_CREATE|O_TRUNC a FIXED, predictable temp path. force-single is run as root per the
	// runbook, and the data dir is tether-owned — a `tether`-writable directory. A pre-planted symlink at
	// `<dataDir>/.force-single.journal.tmp` would make root's O_TRUNC open follow it and truncate ANY file
	// on the box (/etc/shadow, another service's DB): a tether→root local privilege-escalation primitive.
	// O_EXCL|O_NOFOLLOW refuses to follow a symlink and refuses to reuse an existing path; a random suffix
	// makes the name unpredictable and lets two callers not collide. The temp is always cleaned up.
	tmpf, err := os.CreateTemp(dataDir, journalFileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("clusteroffline: create force-single journal: %w", err)
	}
	tmp := tmpf.Name()
	defer func() { _ = os.Remove(tmp) }() // no-op once the rename below succeeded
	if err := tmpf.Chmod(0o600); err != nil {
		_ = tmpf.Close()
		return fmt.Errorf("clusteroffline: chmod force-single journal: %w", err)
	}
	f := tmpf
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return fmt.Errorf("clusteroffline: write force-single journal: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("clusteroffline: fsync force-single journal: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("clusteroffline: close force-single journal: %w", err)
	}
	if err := os.Rename(tmp, journalPath(dataDir)); err != nil {
		return fmt.Errorf("clusteroffline: install force-single journal: %w", err)
	}
	if d, err := os.Open(dataDir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// ClearForceSingleJournal removes the journal. It is called ONLY after the final phase (the nats.conf
// de-cluster) succeeded — i.e. once the node is actually bootable again. Round-5 B1.
func ClearForceSingleJournal(dataDir string) error {
	if err := os.Remove(journalPath(dataDir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("clusteroffline: clear force-single journal: %w", err)
	}
	if d, err := os.Open(dataDir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// InterruptedForceSingle reports the self_id of an interrupted force-single on this data dir, or "" if
// none. Callers (the broker's startup diagnostics, `cluster status`, the drills) use it to name the state
// instead of leaving the operator to decode an exit-70 crash-loop.
func InterruptedForceSingle(dataDir string) (selfID string, phase string, err error) {
	j, err := readJournal(dataDir)
	if err != nil || j == nil {
		return "", "", err
	}
	return j.SelfID, j.Phase, nil
}

// resumeConfirmation merges the operator's --confirm-peers-dead with the set recorded by an interrupted
// run — but ONLY for peers that are ALREADY GONE from the CURRENT roster.
//
// Round-5 B1 needs this because after the prune the abandoned peers no longer exist in the roster, so a
// re-run cannot name them and the plain gate would refuse to forward-complete forever. But a journalled
// confirmation must NEVER answer the liveness question for a peer that is still in the roster (round-6):
// the typed confirmation means "I, the operator, physically verified these machines are dead AT THIS
// MOMENT". A journal replays that assertion at a LATER moment — and a run that journalled `started` and
// then failed BEFORE mutating anything would otherwise leave a permanent, invisible standing confirmation.
// If the peer has since RETURNED, or is merely PARTITIONED (the exact case this gate exists for — a TCP
// probe cannot see a partitioned-but-live peer), that stale assertion is precisely how a split brain
// happens. So: peers still in the roster must be confirmed AFRESH by a human; only the already-pruned ones
// inherit, and for those there is nothing left to split.
func resumeConfirmation(current []string, j *forceSingleJournal, roster []Peer) []string {
	inRoster := make(map[string]struct{}, len(roster))
	for _, p := range roster {
		inRoster[p.NodeID] = struct{}{}
	}
	inherited := make([]string, 0, 4)
	if j != nil {
		for _, id := range j.ConfirmedDead {
			if _, still := inRoster[id]; still {
				continue // STILL a roster peer → the operator must re-confirm it now; the journal does not count
			}
			inherited = append(inherited, id)
		}
	}
	seen := make(map[string]struct{}, len(current)+len(inherited))
	out := make([]string, 0, len(current)+len(inherited))
	for _, s := range append(append([]string{}, current...), inherited...) {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
