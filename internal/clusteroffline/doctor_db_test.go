package clusteroffline_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/storage"
)

// doctor_db_test.go (formerly r10_doctor_db_test.go) — R10 P5 (#50): "the gatekeeper was lying".
//
// `cluster doctor --offline --db <nonexistent>` reported 0 FATAL and exited 0, because the db check
// was `storage.OpenReadOnly(...)` and nothing more — and `database/sql`'s Open is LAZY: it never
// contacts the database. The same round's `--conf /nonexistent` DID report FATAL (natsconf.Preflight
// does a real read), which proves doctor's plumbing was fine and the defect was specific to that one
// cell. Every drill and every runbook step that used doctor as a precondition gate was therefore
// gated on nothing.
//
// The exit requirement is FIVE states, all FATAL, all exit-nonzero. They are enumerated here in one
// table so a future refactor cannot quietly drop one. A SIXTH row (corrupt page) was added during
// mutation verification: without it, deleting the quick_check layer was invisible to the suite, and a
// corrupt DB still passed doctor. Each row is now the state that ONE layer alone catches, so no layer
// of DBPreflight can be removed without turning this red.

// realDB writes a genuine, migrated tether DB (the only input that must PASS).
func realDB(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "tether.db")
	db, err := storage.Open("file:" + p)
	if err != nil {
		t.Fatalf("seed real db: %v", err)
	}
	_ = db.Close()
	return p
}

// origin: r10_doctor_db_test.go (renamed in B6)
func TestDBPreflightRejectsAllFiveStates(t *testing.T) {
	// (5) permission-denied is unconstructible as root — the CI/dev path runs unprivileged, but a
	// drill container may not. Detect and skip only THAT row, never the whole table.
	rootNow := os.Geteuid() == 0

	cases := []struct {
		name    string
		build   func(t *testing.T) string
		wantSub string // a substring the operator can act on
		skip    bool
	}{
		{
			name:    "1-missing",
			build:   func(t *testing.T) string { return filepath.Join(t.TempDir(), "absent.db") },
			wantSub: "missing or unreadable",
		},
		{
			name: "2-directory",
			build: func(t *testing.T) string {
				d := filepath.Join(t.TempDir(), "tether.db")
				if err := os.Mkdir(d, 0o700); err != nil {
					t.Fatal(err)
				}
				return d
			},
			wantSub: "is a DIRECTORY",
		},
		{
			name: "3-empty-file",
			build: func(t *testing.T) string {
				p := filepath.Join(t.TempDir(), "tether.db")
				if err := os.WriteFile(p, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				return p
			},
			// A 0-byte file is a VALID SQLite database: it opens, it pings, quick_check says "ok".
			// Only the schema probe separates it from a real migration source.
			wantSub: "no schema_migrations table",
		},
		{
			name: "4-truncated",
			build: func(t *testing.T) string {
				src := realDB(t)
				blob, err := os.ReadFile(src)
				if err != nil {
					t.Fatal(err)
				}
				if len(blob) < 4096 {
					t.Fatalf("seed db unexpectedly tiny (%d bytes)", len(blob))
				}
				p := filepath.Join(t.TempDir(), "tether.db")
				// Keep a valid 16-byte SQLite magic but truncate mid-file: the header parses, the
				// page images do not. This is the shape a killed `cp`/half-synced restore leaves.
				if err := os.WriteFile(p, blob[:2048], 0o600); err != nil {
					t.Fatal(err)
				}
				return p
			},
			wantSub: "not a readable SQLite database",
		},
		{
			name: "5-permission-denied",
			build: func(t *testing.T) string {
				src := realDB(t)
				blob, err := os.ReadFile(src)
				if err != nil {
					t.Fatal(err)
				}
				p := filepath.Join(t.TempDir(), "tether.db")
				if err := os.WriteFile(p, blob, 0o000); err != nil {
					t.Fatal(err)
				}
				return p
			},
			// Pin the DIAGNOSIS, not merely the rejection. An unreadable file is not a corrupt one,
			// and the two have different remedies (fix the mode / run as the right user, vs. restore
			// from a backup). Ping is the only layer that gets this right: delete it and the case
			// falls through to quick_check, which reports "not an intact SQLite database". Without
			// this substring, deleting Ping is invisible to the whole suite — verified by mutation.
			wantSub: "not a readable SQLite database",
			skip:    rootNow || runtime.GOOS == "windows",
		},
		{
			// (6) A CORRUPT PAGE mid-file. This state is why quick_check exists: the file opens, Ping
			// succeeds, sqlite_master reads back a tether schema — every other layer says "fine" —
			// and the database is nonetheless corrupt. Without quick_check, `cluster doctor` hands a
			// corrupt DB a clean bill of health, which is #50 one layer deeper.
			//
			// The corruption must land at/after page 2 (offset >= 4096): the pre-R10 fixture flipped
			// bytes in the header region, where Ping catches it first and quick_check is redundant.
			name: "6-corrupt-page",
			build: func(t *testing.T) string {
				d := t.TempDir()
				src := filepath.Join(d, "seed.db")
				db, err := storage.Open("file:" + src)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`CREATE TABLE r10_pad(a TEXT)`); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 500; i++ {
					if _, err := db.Exec(`INSERT INTO r10_pad VALUES(?)`, strings.Repeat("pad", 13)); err != nil {
						t.Fatal(err)
					}
				}
				_ = db.Close()
				blob, err := os.ReadFile(src)
				if err != nil {
					t.Fatal(err)
				}
				const at = 8192
				if len(blob) < at+64 {
					t.Fatalf("seed db too small to corrupt a non-header page (%d bytes)", len(blob))
				}
				for i := at; i < at+64; i++ {
					blob[i] ^= 0xFF
				}
				p := filepath.Join(d, "tether.db")
				if err := os.WriteFile(p, blob, 0o600); err != nil {
					t.Fatal(err)
				}
				return p
			},
			wantSub: "CORRUPT",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip("permission-denied is unconstructible for this uid/OS")
			}
			path := tc.build(t)

			// (a) the primitive rejects it.
			err := clusteroffline.DBPreflight(path)
			if err == nil {
				t.Fatalf("DBPreflight(%s) returned nil — this is the #50 false green light", path)
			}
			if tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error must name the actionable cause %q, got: %v", tc.wantSub, err)
			}

			// (b) THE GATE: it must count as a FATAL in the doctor report, not merely produce a
			// string somewhere. This is the assertion #50 actually needed — an error the summary
			// ignores is the same false green light in a different shape.
			checks := clusteroffline.Doctor(clusteroffline.DoctorOptions{
				SecretsDir: t.TempDir(), DBPath: path,
				ConfPath: filepath.Join(t.TempDir(), "absent.conf"),
			})
			var dbCheck *clusteroffline.DoctorCheck
			for i := range checks {
				if checks[i].Name == "db" {
					dbCheck = &checks[i]
				}
			}
			if dbCheck == nil {
				t.Fatal("Doctor() emitted no db check at all")
			}
			if dbCheck.Status != clusteroffline.DoctorFatal {
				t.Fatalf("db check status = %s, want FATAL (detail: %s)", dbCheck.Status, dbCheck.Detail)
			}
			if _, _, fatal := clusteroffline.DoctorSummary(checks); fatal == 0 {
				t.Fatal("DoctorSummary reported 0 fatal — renderDoctor keys the nonzero exit on this count, so exit would be 0")
			}
		})
	}
}

// TestDBPreflightAcceptsARealDB is the other half of the table: the check must not have been made
// unconditionally red (a gate that always fails is as useless as one that never does, and would be
// the lazy way to turn this row green).
func TestDBPreflightAcceptsARealDB(t *testing.T) {
	p := realDB(t)
	if err := clusteroffline.DBPreflight(p); err != nil {
		t.Fatalf("a genuine migrated tether DB must PASS, got: %v", err)
	}
	checks := clusteroffline.Doctor(clusteroffline.DoctorOptions{
		SecretsDir: t.TempDir(), DBPath: p, ConfPath: filepath.Join(t.TempDir(), "absent.conf"),
	})
	for _, c := range checks {
		if c.Name == "db" && c.Status != clusteroffline.DoctorPass {
			t.Fatalf("db check on a real DB = %s (%s), want PASS", c.Status, c.Detail)
		}
	}
}

// TestDBPreflightStillDoesNotMutate re-proves the B3 non-mutation invariant now that the check
// actually CONNECTS (Ping + quick_check + a sqlite_master read). A read-only handle that silently
// created a -wal/-shm sidecar next to a production DB would be a regression introduced BY this fix.
func TestDBPreflightStillDoesNotMutate(t *testing.T) {
	p := realDB(t)
	digest := func() [3]int64 {
		var out [3]int64
		for i, suffix := range []string{"", "-wal", "-shm"} {
			if st, err := os.Stat(p + suffix); err == nil {
				out[i] = st.Size()
			} else {
				out[i] = -1
			}
		}
		return out
	}
	before := digest()
	if err := clusteroffline.DBPreflight(p); err != nil {
		t.Fatal(err)
	}
	if after := digest(); after != before {
		t.Fatalf("DBPreflight mutated the DB tree: %v -> %v (it must stay strictly read-only)", before, after)
	}
}
