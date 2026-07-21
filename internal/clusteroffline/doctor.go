package clusteroffline

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/storage"
)

// doctor.go (B3 item 2) — a CHEAP, READ-ONLY preflight an operator runs BEFORE the irreversible
// `cluster init --from-existing` (which stops the daemon). It never mutates anything: no DB copy,
// no migration run, no conf swap. Each check is PASS / ADVISORY / FATAL; advisories are VISIBLE
// (the FDE advisory previously only went to stderr). Exit-on-FATAL is the caller's job.

// DoctorStatus is a single check's verdict.
type DoctorStatus string

const (
	DoctorPass     DoctorStatus = "PASS"
	DoctorAdvisory DoctorStatus = "ADVISORY"
	DoctorFatal    DoctorStatus = "FATAL"
)

// DoctorCheck is one preflight result.
type DoctorCheck struct {
	Name   string       `json:"name"`
	Status DoctorStatus `json:"status"`
	Detail string       `json:"detail"`
}

// DoctorOptions parameterizes the preflight. RaftAddr/NatsRoute are optional — when supplied (e.g.
// by `init --check`) their port bindability is probed with the migration-aware classification.
type DoctorOptions struct {
	SecretsDir string
	DBPath     string
	ConfPath   string
	RaftAddr   string // host:7400 (NEW port; in-use = conflict = FATAL)
	NatsRoute  string // nats://host:6222 (may be the running daemon = ADVISORY)
}

// Doctor runs every non-mutating check and returns the results in a stable order.
func Doctor(opts DoctorOptions) []DoctorCheck {
	var checks []DoctorCheck

	// 1. secrets present + safe perms; the FDE psk_at_rest advisory becomes a VISIBLE row.
	adv, fatal := SecretsPreflight(opts.SecretsDir)
	if fatal != nil {
		checks = append(checks, DoctorCheck{"secrets", DoctorFatal, fatal.Error()})
	} else {
		checks = append(checks, DoctorCheck{"secrets", DoctorPass, "all §15 secret files present with safe (0600) private-key perms"})
	}
	for _, a := range adv {
		checks = append(checks, DoctorCheck{"secrets-advisory", DoctorAdvisory, a})
	}

	// 2. tunnel cert is readable + fingerprintable (a bad cert un-pins every agent at cutover).
	if _, err := TunnelCertFingerprint(opts.SecretsDir); err != nil {
		checks = append(checks, DoctorCheck{"tunnel-cert-fp", DoctorFatal, err.Error()})
	} else {
		checks = append(checks, DoctorCheck{"tunnel-cert-fp", DoctorPass, "tunnel cert readable + fingerprintable"})
	}

	// 3/4. port bindability — raft-addr is a NEW port (in-use = real conflict = FATAL); the
	// nats-route :6222 may be held by the very daemon you're about to stop (in-use = ADVISORY).
	if opts.RaftAddr != "" {
		checks = append(checks, portBindCheck("raft-addr", opts.RaftAddr, true))
	}
	if opts.NatsRoute != "" {
		checks = append(checks, portBindCheck("nats-route", stripScheme(opts.NatsRoute), false))
	}

	// 5. nats.conf fail-closed gate (no include / unrecognized directive) would pass.
	if _, err := natsconf.Preflight(opts.ConfPath); err != nil {
		checks = append(checks, DoctorCheck{"nats.conf", DoctorFatal, err.Error()})
	} else {
		checks = append(checks, DoctorCheck{"nats.conf", DoctorPass, "no include / unrecognized directive (takeover's fail-closed gate would pass)"})
	}

	// 6. DB opens read-only AND is a real, intact tether database (the migration source must be
	// reachable). NO schema mutation / copy.
	if err := DBPreflight(opts.DBPath); err != nil {
		checks = append(checks, DoctorCheck{"db", DoctorFatal, err.Error()})
	} else {
		checks = append(checks, DoctorCheck{"db", DoctorPass, "tether.db opens read-only, passes quick_check, and carries a tether schema (migration source reachable)"})
	}

	return checks
}

// dbPreflightTimeout bounds every probe DBPreflight runs. The checks are local-file SQLite reads,
// so a second is generous; the bound exists so a pathological file (a FIFO, a hung network mount)
// can never wedge a preflight an operator runs under time pressure.
const dbPreflightTimeout = 10 * time.Second

// DBPreflight proves the DB at dbPath is a REACHABLE, INTACT tether database, read-only and without
// mutating a byte.
//
// R10 P5 (#50) — "the gatekeeper was lying". This check used to be `storage.OpenReadOnly(...)` and
// nothing else, but `OpenReadOnly` is a bare `sql.Open`, and `database/sql`'s Open is documented to
// be LAZY: it validates the DSN, allocates a pool, and NEVER contacts the database. So every
// pathological input below returned a nil error, `cluster doctor --offline --db <anything>` reported
// 0 FATAL, and exited 0 — a green light for a host with no database at all. Any drill or runbook step
// using doctor as its precondition gate was therefore gated on nothing.
//
// The states this must reject (all FATAL, all covered by TestDBPreflightRejectsAllFiveStates):
//
//	(1) the file does not exist          → os.Stat
//	(2) the path is a DIRECTORY          → os.Stat + IsDir
//	(3) the file is EMPTY (0 bytes)      → opens + pings + quick_check "ok"; ONLY the schema probe catches it
//	(4) the file is TRUNCATED / garbage  → Ping
//	(5) the file is PERMISSION-DENIED    → Ping (the lazy Open is what hid this one)
//	(6) a CORRUPT page mid-file          → opens + pings + has a schema; ONLY quick_check catches it
//
// On layering — verified by mutation, because the obvious reading is wrong. Any ONE of Ping /
// quick_check / the schema probe forces the real connection that `database/sql` skips, so deleting
// Ping alone (or Ping+quick_check) still rejects states 1-5 and is NOT detectable from those states.
// Each layer earns its place by the state only it catches, and by the DIAGNOSIS it gives:
//
//	Ping        — the only correct read of PERMISSION-DENIED. Without it that case falls through to
//	              quick_check and is reported as "not an intact SQLite database", i.e. the operator is
//	              told their DB is CORRUPT when it is merely unreadable. Wrong diagnosis, wrong remedy.
//	quick_check — the ONLY layer that sees a corrupt page (state 6). Remove it and a genuinely
//	              corrupt DB gets a clean bill of health: #50 again, one layer deeper.
//	schema      — the ONLY layer that sees an empty file (state 3), which opens, pings and
//	              quick_checks perfectly and is a valid, useless SQLite database.
//
// Exported so the same proof is available to any other offline surgery that must not proceed on an
// unreadable DB.
func DBPreflight(dbPath string) error {
	if dbPath == "" {
		return errors.New("no DB path given (--db is empty) — cannot verify the migration source")
	}
	st, err := os.Stat(dbPath) // Stat (not Lstat): a symlink to a real DB is fine; a dangling one errors here.
	if err != nil {
		return fmt.Errorf("stat %s: %v — the DB file is missing or unreadable", dbPath, err)
	}
	if st.IsDir() {
		return fmt.Errorf("%s is a DIRECTORY, not a SQLite database file", dbPath)
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file (mode %s) — refusing to treat it as a SQLite database", dbPath, st.Mode())
	}

	db, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		return fmt.Errorf("open %s read-only: %v", dbPath, err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), dbPreflightTimeout)
	defer cancel()

	// Force a real connection — this is what `database/sql`'s lazy Open never does, and skipping it
	// is what made the pre-R10 check a no-op. (The probes below would also connect; Ping is kept
	// because it is the layer that reports PERMISSION-DENIED as unreadable rather than as corrupt.)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("open %s read-only: %v — the file is not a readable SQLite database (missing? permission denied? not a database?)", dbPath, err)
	}
	// Integrity: catches a truncated / partially-written / corrupted page image.
	var quick string
	if err := db.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&quick); err != nil {
		return fmt.Errorf("PRAGMA quick_check on %s: %v — the file is not an intact SQLite database", dbPath, err)
	}
	if quick != "ok" {
		return fmt.Errorf("PRAGMA quick_check on %s reported %q (want \"ok\") — the database is CORRUPT", dbPath, quick)
	}
	// Schema: an EMPTY file is a perfectly valid, perfectly useless SQLite database — it opens, it
	// pings, and quick_check says "ok". Only "does it carry a tether schema?" separates it from a
	// real migration source.
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'`).Scan(&n); err != nil {
		return fmt.Errorf("read sqlite_master of %s: %v", dbPath, err)
	}
	if n == 0 {
		return fmt.Errorf("%s opens as SQLite but has no schema_migrations table — it is EMPTY or is not a tether database "+
			"(a fresh host has no DB until the broker has run once, or until `cluster recovery restore` installs one)", dbPath)
	}
	return nil
}

// DoctorSummary counts the verdicts.
func DoctorSummary(checks []DoctorCheck) (pass, advisory, fatal int) {
	for _, c := range checks {
		switch c.Status {
		case DoctorPass:
			pass++
		case DoctorAdvisory:
			advisory++
		case DoctorFatal:
			fatal++
		}
	}
	return
}

// portBindCheck probes whether addr is bindable. An in-use raft-addr is a real conflict (FATAL);
// an in-use nats-route is expected pre-cutover (the running daemon → ADVISORY). A non-in-use bind
// error (permission / malformed addr) is always FATAL.
func portBindCheck(name, addr string, conflictFatal bool) DoctorCheck {
	ln, err := net.Listen("tcp", addr)
	if err == nil {
		_ = ln.Close()
		return DoctorCheck{name, DoctorPass, addr + " is free / bindable"}
	}
	if strings.Contains(err.Error(), "address already in use") {
		if conflictFatal {
			return DoctorCheck{name, DoctorFatal, addr + " already bound — a process OTHER than the broker you're migrating holds it"}
		}
		return DoctorCheck{name, DoctorAdvisory, addr + " in use — likely the running broker you'll stop (expected pre-cutover)"}
	}
	return DoctorCheck{name, DoctorFatal, fmt.Sprintf("%s bind failed: %v", addr, err)}
}

// stripScheme turns "nats://host:6222" into "host:6222" (net.Listen wants no scheme).
func stripScheme(u string) string {
	if i := strings.Index(u, "://"); i >= 0 {
		return u[i+3:]
	}
	return u
}
