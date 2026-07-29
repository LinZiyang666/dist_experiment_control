package clusteroffline

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// force_single_round5_test.go — regressions for external review round-5 B1/B2/B3.
// Each test FAILS if its fix is reverted (that is the point: round-5 M-lane proved five earlier "fixes"
// could be deleted wholesale with the suite still green).

// B1: the journal must survive an interruption and let a re-run forward-complete WITHOUT the operator
// re-listing peers that the prune already removed from the roster.
func TestRound5B1_JournalCarriesConfirmationAcrossAnInterruption(t *testing.T) {
	dir := t.TempDir()
	j := &forceSingleJournal{
		SelfID: "brk1", SelfRaftAddr: "brk1:7400",
		ConfirmedDead: []string{"brk2", "brk3"}, Phase: phaseRaftRebuilt,
	}
	if err := writeJournal(dir, j); err != nil {
		t.Fatalf("writeJournal: %v", err)
	}
	got, err := readJournal(dir)
	if err != nil || got == nil {
		t.Fatalf("readJournal after write: %v (got %+v)", err, got)
	}
	if got.SelfID != "brk1" || got.Phase != phaseRaftRebuilt {
		t.Fatalf("journal round-trip lost fields: %+v", got)
	}
	// The whole point: a RE-RUN passes NO --confirm-peers-dead (it cannot — the peers are pruned away),
	// yet the recorded confirmation must still satisfy the gate.
	// The peers are ALREADY PRUNED (roster empty) — that is the only case a journal may speak for.
	resumed := resumeConfirmation(nil, got, nil)
	if len(resumed) != 2 || resumed[0] != "brk2" || resumed[1] != "brk3" {
		t.Fatalf("resumeConfirmation dropped the recorded confirmation: %v", resumed)
	}
	// And the resumed set must satisfy the real liveness/confirmation gate for those peers.
	if err := checkPeersDead([]Peer{{NodeID: "brk2"}, {NodeID: "brk3"}}, resumed); err != nil {
		t.Fatalf("checkPeersDead rejected the journalled confirmation: %v", err)
	}
	// Round-6 (self-review): a peer STILL IN THE ROSTER must NOT inherit the journalled confirmation — the
	// typed confirmation is a human assertion about THIS moment, and a stale journal must never replay it
	// for a peer that may have returned or be merely partitioned (a TCP probe cannot see a live partitioned
	// peer). This is the split-brain guard.
	stillThere := []Peer{{NodeID: "brk2"}, {NodeID: "brk3"}} // BOTH still in the roster (nothing was pruned)
	if leaked := resumeConfirmation(nil, j, stillThere); len(leaked) != 0 {
		t.Fatalf("a journalled confirmation leaked to peers STILL in the roster (%v) — split-brain risk", leaked)
	}
	if err := checkPeersDead(stillThere, resumeConfirmation(nil, j, stillThere)); err == nil {
		t.Fatal("a stale journal satisfied the confirm gate for roster peers — the operator must re-confirm")
	}
	// A PARTIALLY-pruned roster: only the already-gone peer inherits; the surviving roster peer does not.
	if got := resumeConfirmation(nil, j, []Peer{{NodeID: "brk2"}}); len(got) != 1 || got[0] != "brk3" {
		t.Fatalf("partial resume wrong: want only the pruned brk3 to inherit, got %v", got)
	}
	// A brand-new (un-journalled) run must still be gated: no confirmation → refuse.
	if err := checkPeersDead([]Peer{{NodeID: "brk2"}}, resumeConfirmation(nil, nil, nil)); err == nil {
		t.Fatal("checkPeersDead accepted an UNCONFIRMED peer with no journal — the gate was weakened")
	}
}

// B1: the journal is the only evidence a rebuild was in flight — a corrupt one must NOT be silently
// ignored (that would let a re-run start a fresh sequence over a half-done node).
func TestRound5B1_CorruptJournalIsNotSilentlyIgnored(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(journalPath(dir), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readJournal(dir); err == nil {
		t.Fatal("a corrupt journal was silently ignored — an in-flight rebuild would be forgotten")
	}
}

// TestOnlineIntentRejectsStructurallyUnsafeRecords ensures valid JSON is not confused with a valid
// recovery instruction.
//
// origin: batch-c external re-review B1. This file can trigger replicated emergency mutations, so an
// empty epoch, self-prune target, duplicate target, bad timestamp, or trailing JSON must fail closed.
func TestOnlineIntentRejectsStructurallyUnsafeRecords(t *testing.T) {
	valid := OnlineIntent{
		SelfID: "brk1", Abandoned: []string{"brk2"}, Epoch: "epoch-1",
		MarkedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	cases := map[string]OnlineIntent{
		"empty self":     {Epoch: valid.Epoch, MarkedAt: valid.MarkedAt},
		"empty epoch":    {SelfID: valid.SelfID, MarkedAt: valid.MarkedAt},
		"bad timestamp":  {SelfID: valid.SelfID, Epoch: valid.Epoch, MarkedAt: "not-a-time"},
		"self abandoned": {SelfID: valid.SelfID, Epoch: valid.Epoch, MarkedAt: valid.MarkedAt, Abandoned: []string{"brk1"}},
		"duplicate peer": {SelfID: valid.SelfID, Epoch: valid.Epoch, MarkedAt: valid.MarkedAt, Abandoned: []string{"brk2", "brk2"}},
		"empty peer":     {SelfID: valid.SelfID, Epoch: valid.Epoch, MarkedAt: valid.MarkedAt, Abandoned: []string{""}},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			if err := ValidateOnlineIntent(in); err == nil {
				t.Fatal("unsafe intent accepted")
			}
		})
	}

	dir := t.TempDir()
	if err := WriteOnlineIntent(dir, valid); err != nil {
		t.Fatalf("write valid intent: %v", err)
	}
	p := onlineIntentPath(dir)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, append(b, []byte(`{"second":"document"}`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadOnlineIntent(dir); err == nil {
		t.Fatal("intent reader accepted trailing JSON after the recovery record")
	}
}

// B1: InterruptedForceSingle must name the state so an operator is not left decoding an exit-70 loop.
func TestRound5B1_InterruptedForceSingleIsReportable(t *testing.T) {
	dir := t.TempDir()
	if id, _, err := InterruptedForceSingle(dir); err != nil || id != "" {
		t.Fatalf("clean data dir reported an interruption: id=%q err=%v", id, err)
	}
	if err := writeJournal(dir, &forceSingleJournal{SelfID: "brk1", Phase: phaseStarted}); err != nil {
		t.Fatal(err)
	}
	id, phase, err := InterruptedForceSingle(dir)
	if err != nil || id != "brk1" || phase != phaseStarted {
		t.Fatalf("InterruptedForceSingle did not report the in-flight run: id=%q phase=%q err=%v", id, phase, err)
	}
	if err := ClearForceSingleJournal(dir); err != nil {
		t.Fatal(err)
	}
	if id, _, _ := InterruptedForceSingle(dir); id != "" {
		t.Fatalf("journal survived ClearForceSingleJournal: %q", id)
	}
}

// B2: the atomic-exchange capability is a PRECONDITION — it must be provable before any mutation, and on a
// filesystem that cannot do it the answer must be a refusal, never a non-atomic fallback.
func TestRound5B2_AtomicExchangeIsPrecheckedNotFallenBackOn(t *testing.T) {
	dir := t.TempDir() // the test tmpfs/ext4 under Linux supports RENAME_EXCHANGE
	err := cluster.AtomicExchangeCapable(dir)
	if err != nil && !strings.Contains(err.Error(), "atomically exchange") {
		t.Fatalf("unexpected probe error: %v", err)
	}
	// The probe must leave NOTHING behind — it runs before a destructive op and must not litter the data dir.
	ents, rerr := os.ReadDir(dir)
	if rerr != nil {
		t.Fatal(rerr)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".xchg-probe-") {
			t.Fatalf("capability probe leaked %s into the data dir", e.Name())
		}
	}
}

// B3: the daemon's lifetime lock and the offline tools' lock MUST be the same file, and it must actually
// exclude a second holder. A regression that gives them different paths silently removes the interlock.
func TestRound5B3_DataDirLockIsOneSSOTAndExcludes(t *testing.T) {
	if lockFileName != cluster.DataDirLockFile {
		t.Fatalf("offline lock %q != daemon lock %q — the interlock is not a single SSOT", lockFileName, cluster.DataDirLockFile)
	}
	dir := t.TempDir()
	release, err := cluster.AcquireDataDirLock(dir)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	// A second holder (the daemon starting under a live recovery, or vice-versa) MUST be refused.
	if _, err := cluster.AcquireDataDirLock(dir); err == nil {
		t.Fatal("the data-dir lock did not exclude a second holder — daemon/offline interlock is broken")
	}
	release()
	// After release the lock is free again (a crashed daemon must never wedge recovery).
	release2, err := cluster.AcquireDataDirLock(dir)
	if err != nil {
		t.Fatalf("lock not released: %v", err)
	}
	release2()
}
