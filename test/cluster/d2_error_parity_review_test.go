package cluster_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/session"
)

func rejectPIN(pin, hash string) error { return errors.New("bad pin") }

func TestD2PlanErrorParity_Review(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)

	t.Run("tombstone_deleting", func(t *testing.T) {
		live := freshDB(t)
		planned := freshDB(t)
		for _, db := range []*testDB{{live}, {planned}} {
			if _, err := session.Create(db.DB, "lab", "lab", "o", "p", now); err != nil {
				t.Fatal(err)
			}
			if err := session.Tombstone(db.DB, "lab", now); err != nil {
				t.Fatal(err)
			}
		}
		if err := session.Tombstone(live, "lab", now); !errors.Is(err, session.ErrDeleting) {
			t.Fatalf("live: got %v, want ErrDeleting", err)
		}
		if _, err := session.PlanTombstone(planned, "lab", now); !errors.Is(err, session.ErrDeleting) {
			t.Fatalf("plan: got %v, want ErrDeleting", err)
		}
	})

	t.Run("join_pin_failures", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			prepare   func(*testing.T, *testDB)
			verify    func(string, string) error
			wantError error
		}{
			{
				name:      "missing_session",
				prepare:   func(*testing.T, *testDB) {},
				verify:    acceptPIN,
				wantError: session.ErrNotFound,
			},
			{
				name: "deleting_session",
				prepare: func(t *testing.T, db *testDB) {
					if _, err := session.Create(db.DB, "lab", "lab", "o", "p", now); err != nil {
						t.Fatal(err)
					}
					if err := session.Tombstone(db.DB, "lab", now); err != nil {
						t.Fatal(err)
					}
				},
				verify:    acceptPIN,
				wantError: session.ErrDeleting,
			},
			{
				name: "invalid_pin",
				prepare: func(t *testing.T, db *testDB) {
					if _, err := session.Create(db.DB, "lab", "lab", "o", "p", now); err != nil {
						t.Fatal(err)
					}
				},
				verify:    rejectPIN,
				wantError: session.ErrInvalidPIN,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				live := &testDB{freshDB(t)}
				planned := &testDB{freshDB(t)}
				tc.prepare(t, live)
				tc.prepare(t, planned)
				if err := session.JoinWithPIN(live.DB, "lab", "SHA256:member", "1234", tc.verify, now); !errors.Is(err, tc.wantError) {
					t.Fatalf("live: got %v, want %v", err, tc.wantError)
				}
				if _, err := session.PlanJoinWithPIN(planned.DB, "lab", "SHA256:member", "1234", tc.verify, now); !errors.Is(err, tc.wantError) {
					t.Fatalf("plan: got %v, want %v", err, tc.wantError)
				}
			})
		}
	})

	t.Run("agentprov_pin_failures", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			prepare   func(*testing.T, *testDB)
			verify    func(string, string) error
			wantError error
		}{
			{
				name:      "missing_session",
				prepare:   func(*testing.T, *testDB) {},
				verify:    acceptPIN,
				wantError: agentprov.ErrSessionMissing,
			},
			{
				name: "deleting_session",
				prepare: func(t *testing.T, db *testDB) {
					if _, err := session.Create(db.DB, "lab", "lab", "o", "p", now); err != nil {
						t.Fatal(err)
					}
					if err := session.Tombstone(db.DB, "lab", now); err != nil {
						t.Fatal(err)
					}
				},
				verify:    acceptPIN,
				wantError: agentprov.ErrSessionDeleting,
			},
			{
				name: "invalid_pin",
				prepare: func(t *testing.T, db *testDB) {
					if _, err := session.Create(db.DB, "lab", "lab", "o", "p", now); err != nil {
						t.Fatal(err)
					}
				},
				verify:    rejectPIN,
				wantError: agentprov.ErrInvalidPIN,
			},
			{
				name: "already_provisioned_different_fp",
				prepare: func(t *testing.T, db *testDB) {
					if _, err := session.Create(db.DB, "lab", "lab", "o", "p", now); err != nil {
						t.Fatal(err)
					}
					if err := agentprov.ProvisionWithPIN(db.DB, "lab", "lab-1", "SHA256:first", "1234", acceptPIN, now); err != nil {
						t.Fatal(err)
					}
				},
				verify:    acceptPIN,
				wantError: agentprov.ErrAlreadyProvisioned,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				live := &testDB{freshDB(t)}
				planned := &testDB{freshDB(t)}
				tc.prepare(t, live)
				tc.prepare(t, planned)
				if err := agentprov.ProvisionWithPIN(live.DB, "lab", "lab-1", "SHA256:second", "1234", tc.verify, now); !errors.Is(err, tc.wantError) {
					t.Fatalf("live: got %v, want %v", err, tc.wantError)
				}
				if _, err := agentprov.PlanProvisionWithPIN(planned.DB, "lab", "lab-1", "SHA256:second", "1234", tc.verify, now); !errors.Is(err, tc.wantError) {
					t.Fatalf("plan: got %v, want %v", err, tc.wantError)
				}
			})
		}
	})

	t.Run("port_exhausted", func(t *testing.T) {
		cfg := &port.Config{BandLow: 14000, BandHigh: 14000, Now: func() time.Time { return now }}
		live := freshDB(t)
		planned := freshDB(t)
		seedSessionNode(t, live, "lab", "lab-1", now)
		seedSessionNode(t, planned, "lab", "lab-1", now)
		if _, err := port.Allocate(live, "lab", "lab-1", "first", 1, 0, "fp", cfg); err != nil {
			t.Fatal(err)
		}
		_, cmd, err := port.PlanAllocate(planned, "lab", "lab-1", "first", 1, 0, "fp", cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.ExecCommand(planned, cmd); err != nil {
			t.Fatal(err)
		}
		if _, err := port.Allocate(live, "lab", "lab-1", "second", 2, 0, "fp", cfg); !errors.Is(err, port.ErrPortExhausted) {
			t.Fatalf("live: got %v, want ErrPortExhausted", err)
		}
		if _, _, err := port.PlanAllocate(planned, "lab", "lab-1", "second", 2, 0, "fp", cfg); !errors.Is(err, port.ErrPortExhausted) {
			t.Fatalf("plan: got %v, want ErrPortExhausted", err)
		}
	})
}

type testDB struct {
	DB *sql.DB
}
