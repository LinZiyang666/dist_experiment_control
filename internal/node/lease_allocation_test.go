package node

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §3 D7
//
// A lease name is allocated from the SAME namespace an operator provisions
// devices into. `foo-02` may already be a real machine somebody owns, so the
// allocator has to consult BOTH tables — nodes for live occupancy and
// agent_provisioning for operator-owned names. Skipping either one hands an
// ephemeral clone a name that already belongs to something else.
func TestLowestFreeSuffixSkipsBothLiveNodesAndProvisionedNames(t *testing.T) {
	db := openDB(t)

	// A live instance already holds -02.
	if _, err := db.Exec(
		`INSERT INTO nodes(sid, nid, status, last_heartbeat_at) VALUES (?,?,?,?)`,
		"lab", "gpu1-02", "ONLINE", time.Now().UTC()); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	// An operator owns -03 as a real device: it has a credential binding but no
	// live row at all. A nodes-only scan would hand this name away.
	if _, err := db.Exec(
		`INSERT INTO agent_provisioning(sid, nid, agent_fp) VALUES (?,?,?)`,
		"lab", "gpu1-03", "SHA256:someone-elses-device"); err != nil {
		t.Fatalf("seed provisioning: %v", err)
	}

	got, err := LowestFreeSuffix(db, "lab", "gpu1", proto.MaxLeaseSuffix)
	if err != nil {
		t.Fatalf("LowestFreeSuffix: %v", err)
	}
	if want := "gpu1-04"; got != want {
		t.Fatalf("got %q, want %q — -02 is live and -03 is an operator-owned provisioning row; "+
			"handing out either would collide with something that already exists", got, want)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §3 D7
//
// An OFFLINE row does not hold its name: reclaiming it is the whole point of
// modelling occupancy as a lease rather than a permanent registration.
//
// A lease has no agent_provisioning row of its own — that is what distinguishes
// it from a real device the operator named gpu1-02, which keeps its name through
// a reboot (see the test below). See claimedLeaseNames.
func TestLowestFreeSuffixReclaimsAnOfflineName(t *testing.T) {
	db := openDB(t)
	if _, err := db.Exec(
		`INSERT INTO nodes(sid, nid, status, last_heartbeat_at) VALUES (?,?,?,?)`,
		"lab", "gpu1-02", "OFFLINE", time.Now().UTC().Add(-time.Hour)); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	got, err := LowestFreeSuffix(db, "lab", "gpu1", proto.MaxLeaseSuffix)
	if err != nil {
		t.Fatalf("LowestFreeSuffix: %v", err)
	}
	if want := "gpu1-02"; got != want {
		t.Fatalf("got %q, want %q — an OFFLINE row must not keep holding its lease name", got, want)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §3 D7
//
// A device whose name merely SHARES A PREFIX must not block a lease. "gpu1-west"
// is not "gpu1-NN", and treating it as one would make an unrelated device's
// existence shrink another family's instance budget.
func TestLowestFreeSuffixIgnoresPrefixSharingNames(t *testing.T) {
	db := openDB(t)
	for _, nid := range []string{"gpu1-west", "gpu1-2", "gpu1-001"} {
		if _, err := db.Exec(
			`INSERT INTO nodes(sid, nid, status, last_heartbeat_at) VALUES (?,?,?,?)`,
			"lab", nid, "ONLINE", time.Now().UTC()); err != nil {
			t.Fatalf("seed node %q: %v", nid, err)
		}
	}
	got, err := LowestFreeSuffix(db, "lab", "gpu1", proto.MaxLeaseSuffix)
	if err != nil {
		t.Fatalf("LowestFreeSuffix: %v", err)
	}
	if want := "gpu1-02"; got != want {
		t.Fatalf("got %q, want %q — none of the seeded names is a genuine `gpu1-NN` lease", got, want)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §2 Q2
//
// REFUSE, never truncate. Two distinct over-long basenames would truncate to
// the same prefix and silently merge two unrelated device families into one
// lease namespace; refusing is honest and cannot corrupt anything. The refusal
// is then DEGRADED by the caller into "grant the presented name", so an
// over-long basename stays single-instance-capable rather than unusable.
func TestLowestFreeSuffixRefusesAnOverlongBasenameRatherThanTruncating(t *testing.T) {
	db := openDB(t)
	tooLong := ""
	for len(tooLong) <= proto.MaxLeaseBasenameLen {
		tooLong += "b"
	}
	_, err := LowestFreeSuffix(db, "lab", tooLong, proto.MaxLeaseSuffix)
	if err == nil {
		t.Fatalf("an over-long basename must be refused, not truncated: truncation would let two "+
			"distinct %d-char basenames collapse to one prefix", len(tooLong))
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §2 Q2
//
// The suffix space is finite by construction. Exhausting it must produce a
// clean refusal rather than a name outside the grammar.
func TestLowestFreeSuffixRefusesWhenTheSuffixSpaceIsExhausted(t *testing.T) {
	db := openDB(t)
	for n := proto.FirstLeaseSuffix; n <= 4; n++ {
		if _, err := db.Exec(
			`INSERT INTO nodes(sid, nid, status, last_heartbeat_at) VALUES (?,?,?,?)`,
			"lab", proto.LeaseNameFor("gpu1", n), "ONLINE", time.Now().UTC()); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if _, err := LowestFreeSuffix(db, "lab", "gpu1", 4); err == nil {
		t.Fatal("exhausting the configured ceiling must refuse, not wrap or overflow the grammar")
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §3.2
//
// Contest is driven by nodes.last_heartbeat_at because that value is in SQLite
// and therefore survives a broker restart and a leader election, unlike any
// in-memory holder registry. These are the three answers the adjudicator
// depends on.
func TestHeartbeatAgeAnswersFromDurableStateOnly(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC()

	// (1) No row at all — nobody holds the name.
	if _, exists, err := HeartbeatAge(db, "lab", "ghost", now); err != nil || exists {
		t.Fatalf("missing row: got exists=%v err=%v, want exists=false", exists, err)
	}

	// (2) A row written by the REPLICATED identity op carries no liveness
	// columns (PlanRegister inserts status='OFFLINE' and never touches
	// last_heartbeat_at). That must read as "no live holder" rather than as a
	// zero-age heartbeat, which would make every such name look contested.
	if _, err := db.Exec(`INSERT INTO nodes(sid, nid, status) VALUES (?,?,?)`,
		"lab", "identity-only", "OFFLINE"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, exists, err := HeartbeatAge(db, "lab", "identity-only", now); err != nil || exists {
		t.Fatalf("identity-only row: got exists=%v err=%v, want exists=false", exists, err)
	}

	// (3) A real beat gives a real age.
	if _, err := db.Exec(
		`INSERT INTO nodes(sid, nid, status, last_heartbeat_at) VALUES (?,?,?,?)`,
		"lab", "live", "ONLINE", now.Add(-3*time.Second)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	age, exists, err := HeartbeatAge(db, "lab", "live", now)
	if err != nil || !exists {
		t.Fatalf("live row: exists=%v err=%v", exists, err)
	}
	if age < 2*time.Second || age > 4*time.Second {
		t.Fatalf("age %v is not ~3s; the adjudicator compares this against LeaseGrace, so a wrong "+
			"unit or timezone here silently changes who gets suffixed", age)
	}
}

// origin: docs/reviews/cloned-credential-instances-review-round2.md B8
//
// THE OTHER HALF OF THE SAME RULE: a real device that the operator happened to
// name `<base>-NN` keeps its name across a reboot.
//
// This is the case the OFFLINE-reclaim rule gets wrong on its own. Such a
// device is admitted by the auth_callout suffix fallback — against the
// BASENAME's fingerprint — so it never acquires an agent_provisioning row of
// its own, and neither of the two original occupancy tests sees it once it goes
// OFFLINE. Its name is then handed to the next clone that shows up, and when
// the machine finishes rebooting BOTH register under it: two agents on one
// name, every command executed twice, on a box that runs exactly one agent.
//
// MUTATION: drop the `OR leased = 0` clause from claimedLeaseNames and this
// goes red.
func TestLowestFreeSuffixDoesNotReclaimARealDevicesName(t *testing.T) {
	db := openDB(t)
	if _, err := db.Exec(
		`INSERT INTO nodes(sid, nid, status, last_heartbeat_at) VALUES (?,?,?,?)`,
		"lab", "gpu1-02", "OFFLINE", time.Now().UTC().Add(-time.Hour)); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	// ITS OWN BINDING is what makes it a device rather than a lease, and it is
	// the whole reason no `nodes.leased` column (and no migration) is needed: a
	// real device reaches PIN bootstrap and provisions itself, while a lease is
	// admitted through the BASENAME's fingerprint and never gets a row here.
	// (external review F1/F9)
	if _, err := db.Exec(
		`INSERT INTO agent_provisioning(sid, nid, agent_fp) VALUES (?,?,?)`,
		"lab", "gpu1-02", "SHA256:a-real-machine"); err != nil {
		t.Fatalf("seed provisioning: %v", err)
	}
	got, err := LowestFreeSuffix(db, "lab", "gpu1", proto.MaxLeaseSuffix)
	if err != nil {
		t.Fatalf("LowestFreeSuffix: %v", err)
	}
	if got == "gpu1-02" {
		t.Fatal("a rebooting device's name was offered to a clone. When the machine comes back it " +
			"registers under gpu1-02 too, and one physical box that runs exactly one agent is now " +
			"sharing a name with a clone — the fan-out this increment exists to remove, aimed at " +
			"the one class of device it promised not to touch.")
	}
	if want := "gpu1-03"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
