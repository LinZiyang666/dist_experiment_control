package node

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// Lease allocation for cloned-credential instances — the DB-backed half. The
// name GRAMMAR (SplitLeaseName / BasenameOf / the width and budget constants)
// lives in internal/proto, because the auth path and the agent both need to
// read it while only the broker needs to allocate.
//
// WHY THERE IS NO LEASE TABLE. `nodes` already is one: it is PRIMARY KEY
// (sid, nid) with an ON CONFLICT upsert, so a name is "held" iff a live row
// exists, and node.Register's existing upsert already IS take-over. A separate
// replicated lease object would restate what `nodes` says, and introducing a
// new FSM op to maintain it would be poison-skipped by an un-upgraded replica —
// a silent per-replica fork of exactly the table that decides name uniqueness.
// Reclamation is likewise not a new GC: it is the row lifecycle that already
// exists.
//
// WHY NOTHING HERE DELETES. processes and port_allocations both declare
// FOREIGN KEY (sid, nid) REFERENCES nodes(sid, nid) ON DELETE CASCADE, with
// foreign_keys forced ON. Reclaiming a name by deleting its nodes row would
// silently destroy that instance's entire process and port history — including
// the terminal audit rows a later investigation depends on. Renaming is
// unavailable for the same reason in reverse: the FKs carry no ON UPDATE
// clause, so the default NO ACTION rejects a PK rename while children exist.
// That is why a freed basename goes to the next NEW arrival rather than
// promoting an existing suffixed instance.

// ErrLeaseUnavailable is returned when no lease name can be issued for a
// basename: either the basename is too long to carry a suffix, or every suffix
// up to the ceiling is taken.
var ErrLeaseUnavailable = fmt.Errorf("node: no lease name available for this basename")

// LowestFreeSuffix returns the lowest lease name for basename that is not
// currently claimed, scanning from proto.FirstLeaseSuffix up to maxSuffix.
//
// "Claimed" is checked against BOTH tables deliberately:
//
//   - nodes: a row that is not OFFLINE means some instance is using that name.
//   - agent_provisioning: `foo-02` is an ordinary ValidateNID-legal name that an
//     operator may already own as a real device. The lease namespace and the
//     provisioning namespace are the SAME namespace and nothing else separates
//     them, so skipping only one would hand an operator's own device's name to
//     an ephemeral clone.
func LowestFreeSuffix(db *sql.DB, sid, basename string, maxSuffix int) (string, error) {
	return LowestFreeSuffixExcept(db, sid, basename, maxSuffix, nil)
}

// LowestFreeSuffixExcept is LowestFreeSuffix with an extra caller-supplied
// exclusion, for names that are taken but not yet visible in the database.
//
// The broker needs it because a contested register deliberately writes NOTHING
// — that ordering is what protects the incumbent's row — so a name it just
// offered leaves no trace for the next challenger's scan to find. Several
// instances arriving together would otherwise all be offered the same suffix.
func LowestFreeSuffixExcept(db *sql.DB, sid, basename string, maxSuffix int, excluded func(string) bool) (string, error) {
	if len(basename) > proto.MaxLeaseBasenameLen {
		return "", ErrLeaseUnavailable
	}
	if maxSuffix > proto.MaxLeaseSuffix {
		maxSuffix = proto.MaxLeaseSuffix
	}
	taken, err := claimedLeaseNames(db, sid, basename)
	if err != nil {
		return "", err
	}
	for n := proto.FirstLeaseSuffix; n <= maxSuffix; n++ {
		name := proto.LeaseNameFor(basename, n)
		if _, bad := taken[name]; bad {
			continue
		}
		if excluded != nil && excluded(name) {
			continue
		}
		return name, nil
	}
	return "", ErrLeaseUnavailable
}

// claimedLeaseNames collects every name under basename that must not be handed
// out, from both the nodes and agent_provisioning tables.
func claimedLeaseNames(db *sql.DB, sid, basename string) (map[string]struct{}, error) {
	taken := map[string]struct{}{}
	// The '-' is already in the LIKE pattern so the scan stays tight; the exact
	// shape is re-checked by SplitLeaseName below, so a stray match is harmless.
	pattern := basename + "-%"

	// A real device that the operator named `<basename>-NN` is protected by its
	// OWN agent_provisioning row (scanned below), not by anything in this query.
	//
	// That is why the external review's F1/F9 fix matters here: a permanently
	// named gpu1-02 must reach PIN bootstrap and acquire its own binding rather
	// than being admitted through the basename's. With that binding present, an
	// OFFLINE real device keeps its name across a reboot, while an OFFLINE lease
	// — which by construction has no binding of its own — is reclaimable. No
	// column, and therefore no migration, is needed to tell them apart.
	nodeRows, err := db.Query(
		`SELECT nid FROM nodes WHERE sid = ? AND nid LIKE ? AND status <> 'OFFLINE'`,
		sid, pattern)
	if err != nil {
		return nil, fmt.Errorf("node: lease scan nodes: %w", err)
	}
	if err := collectLeaseNames(nodeRows, basename, taken); err != nil {
		return nil, err
	}

	provRows, err := db.Query(
		`SELECT nid FROM agent_provisioning WHERE sid = ? AND nid LIKE ?`,
		sid, pattern)
	if err != nil {
		return nil, fmt.Errorf("node: lease scan provisioning: %w", err)
	}
	if err := collectLeaseNames(provRows, basename, taken); err != nil {
		return nil, err
	}
	return taken, nil
}

func collectLeaseNames(rows *sql.Rows, basename string, into map[string]struct{}) error {
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var nid string
		if err := rows.Scan(&nid); err != nil {
			return fmt.Errorf("node: lease scan: %w", err)
		}
		// Only genuine `<basename>-NN` names count: a device called
		// "gpu1-west" must not block "gpu1-02".
		if base, _, leased := proto.SplitLeaseName(nid); leased && base == basename {
			into[nid] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("node: lease scan rows: %w", err)
	}
	return nil
}

// ProvisionedNIDs returns the set of nids in sid that own an agent_provisioning
// binding — the names an operator explicitly enrolled, as opposed to names a
// broker leased to a second instance of an already-enrolled credential.
//
// It is the precise test for "is this row a lease", and the reason a name-shape
// test alone is not: `gpu-01 gpu-02 gpu-03` is this project's own example fleet
// in docs/usage.md, so real single-instance devices are routinely named in the
// lease grammar. A leased instance is distinguished by owning NO binding — it is
// admitted by the auth suffix fallback against its BASENAME's fingerprint.
//
// The second return reports whether the answer is USABLE. A nil map is NOT a
// safe stand-in for "nothing is provisioned": callers ask `!provisioned[nid]`,
// and that is true for every nid against a nil map — so an unusable answer would
// mark every `<x>-NN` device as an ephemeral lease and silently drop it from
// fleet upgrades. `gpu-01 gpu-02 gpu-03` is this repo's own example fleet.
//
// Two distinct unusable cases, both real:
//   - the query failed;
//   - the session has ZERO bindings, which is the STEADY STATE on a broker
//     running without auth_callout (single mode with no seeds dir): nothing
//     ever writes agent_provisioning there, so an empty set carries no
//     information about any name.
func ProvisionedNIDs(db *sql.DB, sid string) (map[string]bool, bool) {
	rows, err := db.Query(`SELECT nid FROM agent_provisioning WHERE sid = ?`, sid)
	if err != nil {
		return nil, false
	}
	defer func() { _ = rows.Close() }()
	out := map[string]bool{}
	for rows.Next() {
		var nid string
		if err := rows.Scan(&nid); err != nil {
			return nil, false
		}
		out[nid] = true
	}
	if rows.Err() != nil {
		return nil, false
	}
	return out, len(out) > 0
}

// HeartbeatAge returns how long ago (sid, nid) last heartbeat, and whether a
// live-enough row exists to answer.
//
// This reads SQLite rather than any in-memory registry ON PURPOSE. An
// in-memory holder map is empty after a broker restart or a leader election, so
// a contest test keyed on it would pass BOTH live clones through as
// uncontested and hand them the same name in sequence — restoring the fan-out
// with no clone arrival and no death involved. last_heartbeat_at survives both
// events.
func HeartbeatAge(db *sql.DB, sid, nid string, now time.Time) (time.Duration, bool, error) {
	var last sql.NullTime
	err := db.QueryRow(
		`SELECT last_heartbeat_at FROM nodes WHERE sid = ? AND nid = ?`,
		sid, nid).Scan(&last)
	switch {
	case err == sql.ErrNoRows:
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("node: heartbeat age: %w", err)
	case !last.Valid:
		// A row written by the replicated identity op carries no liveness
		// columns (PlanRegister inserts status='OFFLINE' and never touches
		// last_heartbeat_at). Treat that as "no live holder".
		return 0, false, nil
	}
	return now.Sub(last.Time.UTC()), true, nil
}

// ReleaseLeaseRow takes a released lease row OFFLINE at once, so the name it
// held is immediately free for the next instance — including its own restart.
//
// A farewell is the holder stating that it has stopped. Without this the row
// stays ONLINE for the whole OfflineAfter window and the allocator keeps
// counting the name as occupied, so a restarting instance is issued the NEXT
// suffix and the operator's saved addresses drift on every bounce (external
// review F11).
//
// proxy_ready is cleared with it, exactly as the liveness reconciler does for
// any OFFLINE transition: a node that is gone must not render into /sub.
func ReleaseLeaseRow(db *sql.DB, sid, nid string) error {
	if _, err := db.Exec(
		`UPDATE nodes SET status = ?, proxy_ready = 0 WHERE sid = ? AND nid = ?`,
		string(StateOffline), sid, nid); err != nil {
		return fmt.Errorf("node: release lease row %s/%s: %w", sid, nid, err)
	}
	return nil
}
