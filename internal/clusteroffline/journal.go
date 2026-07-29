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
	"time"
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
	return writeFileDurably(dataDir, journalPath(dataDir), journalFileName, b)
}

// writeFileDurably installs b at dst via temp -> chmod -> write -> fsync(file) -> close -> rename ->
// fsync(dir). Shared by the offline journal and the ONLINE intent (EXTERNAL review B1) so both get the
// same durability AND the same symlink refusal.
//
// Round-6: NEVER O_CREATE|O_TRUNC a FIXED, predictable temp path. force-single is run as root per the
// runbook, and the data dir is tether-owned — a `tether`-writable directory. A pre-planted symlink at
// a predictable `<dataDir>/<name>.tmp` would make root's O_TRUNC open follow it and truncate ANY file
// on the box (/etc/shadow, another service's DB): a tether->root local privilege-escalation primitive.
// CreateTemp refuses to reuse an existing path and makes the name unpredictable; the temp is always
// cleaned up.
func writeFileDurably(dataDir, dst, prefix string, b []byte) error {
	tmpf, err := os.CreateTemp(dataDir, prefix+".tmp-*")
	if err != nil {
		return fmt.Errorf("clusteroffline: create %s: %w", prefix, err)
	}
	tmp := tmpf.Name()
	defer func() { _ = os.Remove(tmp) }() // no-op once the rename below succeeded
	if err := tmpf.Chmod(0o600); err != nil {
		_ = tmpf.Close()
		return fmt.Errorf("clusteroffline: chmod %s: %w", prefix, err)
	}
	if _, err := tmpf.Write(b); err != nil {
		_ = tmpf.Close()
		return fmt.Errorf("clusteroffline: write %s: %w", prefix, err)
	}
	if err := tmpf.Sync(); err != nil {
		_ = tmpf.Close()
		return fmt.Errorf("clusteroffline: fsync %s: %w", prefix, err)
	}
	if err := tmpf.Close(); err != nil {
		return fmt.Errorf("clusteroffline: close %s: %w", prefix, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("clusteroffline: install %s: %w", prefix, err)
	}
	d, err := os.Open(dataDir)
	if err != nil {
		return fmt.Errorf("clusteroffline: open parent dir for %s fsync: %w", prefix, err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("clusteroffline: fsync parent dir for %s: %w", prefix, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("clusteroffline: close parent dir for %s: %w", prefix, err)
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

// ─── ONLINE force-single intent (batch C, EXTERNAL review B1) ────────────────────────────────────
//
// The ONLINE path had the same re-entrancy hole this file was built to close for the offline one, and
// it was worse there because nothing crash-loops to make it visible.
//
// handleForceSingleCommit performs an IRREVERSIBLE raft rewrite and only THEN writes the
// force_single_active marker and the recovery epoch. If anything between those points fails — a
// leader-wait timeout, a marker propose, a crash — the node is a perfectly healthy writable N=1
// cluster with NO record that an emergency happened:
//
//   - `cluster status` shows DEGRADED-for-fault-tolerance, not FORCE_SINGLE, so the operator is not
//     told the cluster has no integrity;
//   - the ctl destructive gate is `QuorumLost || ForceSingleActive`, and the rewrite made QuorumLost
//     false, so with the marker missing the gate is FULLY OPEN on a zero-redundancy node;
//   - the error text says "re-run `cluster recovery force-single --online`", but the re-run is refused
//     at the arm gate with quorum_not_lost — the node now has leader contact, because it is its own
//     leader. The documented repair is unreachable.
//   - and the leadership-edge resume cannot help either, because ITS trigger was the very marker that
//     failed to be written.
//
// The fix is the one this file already implements for the offline path: record the INTENT on local
// disk, fsync'd, BEFORE the first irreversible step. It needs no quorum (it is a file), it carries the
// epoch so a resume re-uses the SAME one rather than minting a second, and it makes the resume trigger
// independent of any raft state the failure may have prevented.

const onlineIntentFileName = ".force-single-online.intent"

// OnlineIntent is the pre-rewrite record. Every field is needed to FORWARD-COMPLETE without asking the
// operator anything again.
type OnlineIntent struct {
	SelfID string `json:"self_id"`
	// Abandoned is the peer set the rewrite moves out of the raft configuration. Captured before the
	// rewrite because afterwards the roster may already be partly pruned.
	Abandoned []string `json:"abandoned"`
	// Epoch is minted BEFORE the rewrite and reused by every resume. Minting it during recovery is what
	// made the split-brain detector's durable input unrecoverable when the epoch write was the step
	// that failed: a later attempt would have generated a DIFFERENT value, so "did the epoch I promised
	// actually land" became unanswerable.
	Epoch string `json:"epoch"`
	// MarkedAt is the force_single_active value, baked once so every resume proposes identical bytes.
	MarkedAt string `json:"marked_at"`
}

func onlineIntentPath(dataDir string) string { return filepath.Join(dataDir, onlineIntentFileName) }

// WriteOnlineIntent fsyncs the intent before the caller performs any irreversible step. It reuses the
// same symlink-refusing, unpredictable-temp-name write as the offline journal: this runs on a
// tether-writable directory, potentially as root.
func WriteOnlineIntent(dataDir string, in OnlineIntent) error {
	if dataDir == "" {
		return fmt.Errorf("clusteroffline: online force-single intent needs a data dir")
	}
	if err := ValidateOnlineIntent(in); err != nil {
		return err
	}
	b, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("clusteroffline: encode online force-single intent: %w", err)
	}
	return writeFileDurably(dataDir, onlineIntentPath(dataDir), onlineIntentFileName, b)
}

// ReadOnlineIntent returns the in-flight intent, or (nil, nil) when there is none.
func ReadOnlineIntent(dataDir string) (*OnlineIntent, error) {
	if dataDir == "" {
		return nil, nil
	}
	f, err := os.OpenFile(onlineIntentPath(dataDir), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		if errors.Is(err, syscall.ELOOP) {
			return nil, fmt.Errorf("clusteroffline: %s is a SYMLINK — refusing to follow it", onlineIntentPath(dataDir))
		}
		return nil, fmt.Errorf("clusteroffline: read online force-single intent: %w", err)
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("clusteroffline: read online force-single intent: %w", err)
	}
	var in OnlineIntent
	if err := json.Unmarshal(b, &in); err != nil {
		return nil, fmt.Errorf("clusteroffline: decode online force-single intent: %w", err)
	}
	if err := ValidateOnlineIntent(in); err != nil {
		return nil, err
	}
	return &in, nil
}

// ValidateOnlineIntent rejects a syntactically valid but unusable record before it can mutate
// replicated emergency state. Unknown additive JSON fields remain compatible.
func ValidateOnlineIntent(in OnlineIntent) error {
	if in.SelfID == "" {
		return fmt.Errorf("clusteroffline: online force-single intent has an empty self_id")
	}
	if in.Epoch == "" {
		return fmt.Errorf("clusteroffline: online force-single intent has an empty epoch")
	}
	if _, err := time.Parse(time.RFC3339Nano, in.MarkedAt); err != nil {
		return fmt.Errorf("clusteroffline: online force-single intent has invalid marked_at %q: %w",
			in.MarkedAt, err)
	}
	seen := make(map[string]struct{}, len(in.Abandoned))
	for _, id := range in.Abandoned {
		if id == "" {
			return fmt.Errorf("clusteroffline: online force-single intent has an empty abandoned node id")
		}
		if id == in.SelfID {
			return fmt.Errorf("clusteroffline: online force-single intent abandons its own node %q", id)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("clusteroffline: online force-single intent repeats abandoned node %q", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

// ClearOnlineIntent removes the intent once every post-rewrite step is CONFIRMED landed. Absent is OK.
func ClearOnlineIntent(dataDir string) error {
	if dataDir == "" {
		return nil
	}
	if err := os.Remove(onlineIntentPath(dataDir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("clusteroffline: clear online force-single intent: %w", err)
	}
	d, err := os.Open(dataDir)
	if err != nil {
		return fmt.Errorf("clusteroffline: open parent dir to clear online force-single intent: %w", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("clusteroffline: fsync parent dir to clear online force-single intent: %w", err)
	}
	return nil
}
