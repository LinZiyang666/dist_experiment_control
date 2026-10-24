package port

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: hot-update S8 plan §6.2 / §11.2 P35
func TestPortMutationRejectsRecreatedSession(t *testing.T) {
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePortAuthority(t)
	}
	testharness.AssertNoGoroutineLeak(t, "port authority writers", beforeG)
	testharness.AssertNoFDLeak(t, "port authority writers", beforeFD, 0)
}

func exercisePortAuthority(t *testing.T) {
	t.Helper()
	// A real on-disk production database makes missing Close/rollback visible;
	// memory-only allocation fixtures cannot witness a retained SQLite FD.
	db, err := storage.Open(filepath.Join(t.TempDir(), "port-authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error("close port authority database:", err)
		}
	}()
	serial := 0
	for _, verb := range []string{"free", "revoke", "plan-free", "plan-revoke", "guarded-free", "inherited-free", "rehome", "rotate", "allocate"} {
		for _, state := range []string{"ACTIVE", "DELETING"} {
			for _, changed := range []string{"salt", "owner", "created-at"} {
				t.Run(verb+"/"+state+"/"+changed, func(t *testing.T) {
					serial++
					old, guard := seedPortAuthority(t, db, fmt.Sprintf("port-%d", serial), state, verb != "allocate")
					var stale *cluster.Command
					if verb != "free" && verb != "revoke" {
						stale, err = portAuthorityCommand(db, verb, old, guard)
						if err != nil {
							t.Fatal("plan with original creation:", err)
						}
						assertPortAuthorityLiteralCommand(t, stale)
					}
					fresh := recreatePortAuthority(t, db, old, state, changed, verb != "allocate")
					if verb == "free" || verb == "revoke" {
						assertPortIncarnationMismatch(t, mutatePortAuthority(db, verb, old))
					} else {
						_, err := portAuthorityCommand(db, verb, old, guard)
						assertPortIncarnationMismatch(t, err)
						// This is the actual apply boundary: a plan made before
						// recreation must affect zero rows, including both rotation
						// statements and an allocation with an absence expectation.
						assertPortAuthorityCommandRows(t, db, stale, 0)
					}
					assertPortAuthorityUnchanged(t, db, old, verb != "allocate")
					freshAllocation := old
					freshAllocation.Authority = fresh
					if verb == "free" || verb == "revoke" {
						if err := mutatePortAuthority(db, verb, freshAllocation); err != nil {
							t.Fatal("fresh creation could not settle:", err)
						}
					} else {
						cmd, err := portAuthorityCommand(db, verb, freshAllocation, fresh)
						if err != nil {
							t.Fatal("fresh creation could not plan settlement:", err)
						}
						want := int64(1)
						if verb == "rotate" {
							want = 2
						}
						assertPortAuthorityCommandRows(t, db, cmd, want)
					}
					assertPortAuthorityEffect(t, db, old, verb)
					// Keep the fixed band empty for the next case. Settlement
					// itself was exercised above; this is fixture teardown only.
					if _, err := db.Exec(`DELETE FROM sessions WHERE sid=?`, old.SID); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
	exercisePortOriginalGuards(t, db)
	t.Run("authority-scope", func(t *testing.T) { exercisePortAuthorityScope(t, db) })
}

func seedPortAuthority(t *testing.T, db *sql.DB, sid, state string, allocated bool) (Allocation, upgradeauthority.Guard) {
	t.Helper()
	created := time.Unix(1700000000, 123).UTC()
	salt := bytes.Repeat([]byte{17}, 16)
	owner := "owner?'quoted"
	insertPortAuthoritySession(t, db, sid, state, owner, created, salt)
	incarnation, err := upgradeauthority.Derive(sid, owner, created.UnixNano(), salt)
	if err != nil {
		t.Fatal(err)
	}
	guard := upgradeauthority.Guard{SID: sid, Incarnation: incarnation, InstanceID: "instance-a", ConfiguredNID: "node-a"}
	a := Allocation{Port: 14000, SID: sid, NID: "node-a", Name: "expose", LocalPort: 8080,
		TokenHash: HashToken("original-token"), State: StateAllocated, CreatedByFP: owner,
		CreatedAt: created, HomeBroker: "home-a", Epoch: 7, Authority: guard}
	if allocated {
		insertPortAuthorityAllocation(t, db, a)
	}
	return a, guard
}

func insertPortAuthoritySession(t *testing.T, db *sql.DB, sid, state, owner string, created time.Time, salt []byte) {
	t.Helper()
	phc := "$argon2id$v=19$m=65536,t=3,p=2$" + base64.RawStdEncoding.EncodeToString(salt) + "$" +
		base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{23}, 32))
	if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,created_at,state) VALUES (?,?,?,?,?,?)`,
		sid, sid, owner, phc, created, state); err != nil {
		t.Fatal("insert authority session:", err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(sid,nid,status,boot_id) VALUES (?,'node-a','ONLINE','instance-a')`, sid); err != nil {
		t.Fatal(err)
	}
}

func insertPortAuthorityAllocation(t *testing.T, db *sql.DB, a Allocation) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO port_allocations(port,sid,nid,name,local_port,token_hash,state,created_by_fp,created_at,home_broker,epoch)
		VALUES (?,?,?,?,?,?,'ALLOCATED',?,?,?,?)`, a.Port, a.SID, a.NID, a.Name, a.LocalPort,
		a.TokenHash, a.CreatedByFP, a.CreatedAt, a.HomeBroker, a.Epoch); err != nil {
		t.Fatal(err)
	}
}

func recreatePortAuthority(t *testing.T, db *sql.DB, a Allocation, state, changed string, allocated bool) upgradeauthority.Guard {
	t.Helper()
	if _, err := db.Exec(`DELETE FROM sessions WHERE sid=?`, a.SID); err != nil {
		t.Fatal(err)
	}
	owner, created, salt := a.CreatedByFP, a.CreatedAt, bytes.Repeat([]byte{17}, 16)
	switch changed {
	case "salt":
		salt = bytes.Repeat([]byte{29}, 16)
	case "owner":
		owner = "replacement?'owner"
	case "created-at":
		created = created.Add(time.Nanosecond)
	}
	insertPortAuthoritySession(t, db, a.SID, state, owner, created, salt)
	if allocated {
		// Deliberately reuse every original allocation field, especially token,
		// home and epoch. Only the session creation can reject the stale writer.
		insertPortAuthorityAllocation(t, db, a)
		assertPortAuthorityUnchanged(t, db, a, true)
	}
	incarnation, err := upgradeauthority.Derive(a.SID, owner, created.UnixNano(), salt)
	if err != nil || incarnation == a.Authority.Incarnation {
		t.Fatal("fixture did not recreate the session identity")
	}
	fresh := a.Authority
	fresh.Incarnation = incarnation
	return fresh
}

func portAuthorityCommand(db *sql.DB, verb string, a Allocation, authority upgradeauthority.Guard) (*cluster.Command, error) {
	guard := MutationGuard{Expected: &a, FromHome: a.HomeBroker, NowBootNS: int64(time.Second), Authority: authority,
		ExpectedMaintenance: map[string]string{"home-a": "", "home-b": ""}}
	intent := AllocationIntent{SID: a.SID, NID: a.NID, Name: a.Name, LocalPort: a.LocalPort, DesiredPort: a.Port,
		CreatedByFP: a.CreatedByFP, HomeBroker: "home-b", Config: tinyBand()}
	switch verb {
	case "plan-free":
		return PlanFreeAllocation(db, a, testRehomeTime)
	case "plan-revoke":
		return PlanRevokeAllocation(db, a, testRehomeTime)
	case "guarded-free":
		return PlanFreeAllocationGuarded(db, guard, testRehomeTime)
	case "inherited-free":
		guard.Authority = upgradeauthority.Guard{}
		return PlanFreeAllocationGuarded(db, guard, testRehomeTime)
	case "rehome":
		guard.ToHome = intent.HomeBroker
		_, cmd, err := PlanReassignHomeGuarded(db, guard, intent.HomeBroker, testRehomeTime)
		return cmd, err
	case "rotate":
		guard.ToHome = intent.HomeBroker
		_, cmd, err := PlanRotateAllocationGuarded(db, guard, intent, testRehomeTime)
		return cmd, err
	case "allocate":
		guard.Expected, guard.ExpectAbsent, guard.FromHome, guard.ToHome = nil, true, "", intent.HomeBroker
		_, cmd, err := PlanAllocateGuarded(db, guard, intent)
		return cmd, err
	default:
		panic("unknown port authority test verb")
	}
}

func mutatePortAuthority(db *sql.DB, verb string, a Allocation) error {
	if verb == "revoke" {
		return RevokeAllocation(db, a, testRehomeTime)
	}
	return FreeAllocation(db, a, testRehomeTime)
}

func assertPortIncarnationMismatch(t *testing.T, err error) {
	t.Helper()
	var typed *upgradetx.Error
	if !errors.As(err, &typed) || typed.Code != upgradetx.CodeSessionIncarnationMismatch {
		t.Fatalf("stale creation error = %v, want session_incarnation_mismatch", err)
	}
}

func assertPortAuthorityLiteralCommand(t *testing.T, cmd *cluster.Command) {
	t.Helper()
	if cmd == nil || len(cmd.Body) == 0 {
		t.Fatal("authority planner returned no mutation")
	}
	for _, statement := range cmd.Body {
		if len(statement.Args) != 0 {
			t.Fatal("authority planner added a replicated parameter format")
		}
	}
}

func assertPortAuthorityCommandRows(t *testing.T, db *sql.DB, cmd *cluster.Command, want int64) {
	t.Helper()
	assertPortAuthorityLiteralCommand(t, cmd)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var total int64
	for _, statement := range cmd.Body {
		result, err := tx.Exec(statement.SQL)
		if err != nil {
			t.Fatal("apply guarded port mutation:", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		total += changed
	}
	if total != want {
		t.Fatalf("guarded port writes = %d, want %d", total, want)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func assertPortAuthorityUnchanged(t *testing.T, db *sql.DB, old Allocation, allocated bool) {
	t.Helper()
	current, err := LookupByName(db, old.SID, old.Name)
	if !allocated {
		if !errors.Is(err, ErrNotFound) {
			t.Fatal("stale allocation created a port in the replacement session")
		}
		return
	}
	if err != nil || current.Port != old.Port || current.NID != old.NID || current.TokenHash != old.TokenHash ||
		current.HomeBroker != old.HomeBroker || current.Epoch != old.Epoch || current.RevokedAt != nil {
		t.Fatal("stale writer changed the recreated session's allocation")
	}
}

func assertPortAuthorityEffect(t *testing.T, db *sql.DB, old Allocation, verb string) {
	t.Helper()
	if verb == "free" || verb == "plan-free" || verb == "guarded-free" || verb == "inherited-free" || verb == "revoke" || verb == "plan-revoke" {
		var state string
		var ended sql.NullTime
		if err := db.QueryRow(`SELECT state,revoked_at FROM port_allocations WHERE sid=? AND token_hash=?`, old.SID, old.TokenHash).Scan(&state, &ended); err != nil {
			t.Fatal(err)
		}
		want := "FREED"
		if verb == "revoke" || verb == "plan-revoke" {
			want = "REVOKED"
		}
		if state != want || !ended.Valid || !ended.Time.Equal(testRehomeTime) {
			t.Fatal("fresh creation lost its state transition or settlement timestamp")
		}
		return
	}
	current, err := LookupByName(db, old.SID, old.Name)
	if err != nil || current.HomeBroker != "home-b" {
		t.Fatal("fresh creation did not obtain the requested home")
	}
	if verb == "rehome" {
		if current.Epoch != old.Epoch+1 || current.TokenHash != old.TokenHash {
			t.Fatal("rehome lost its original token or monotonic epoch")
		}
	} else if current.Epoch != 0 || current.TokenHash == old.TokenHash {
		t.Fatal("fresh allocation did not mint a new token and initial epoch")
	}
}

func exercisePortOriginalGuards(t *testing.T, db *sql.DB) {
	t.Helper()
	for index, change := range []string{"token_hash='other-token'", "name='other-name'", "home_broker='other-home'", "epoch=8", "maintenance"} {
		t.Run("original-guard/"+change, func(t *testing.T) {
			a, guard := seedPortAuthority(t, db, fmt.Sprintf("original-%d", index), "ACTIVE", true)
			cmd, err := portAuthorityCommand(db, "guarded-free", a, guard)
			if err != nil {
				t.Fatal(err)
			}
			query := `UPDATE port_allocations SET ` + change + ` WHERE state='ALLOCATED'`
			want := ErrNotFound
			if change == "maintenance" {
				query = `INSERT INTO cluster_meta(key,value) VALUES ('broker_image_maintenance.home-a','changed')`
				want = ErrMutationGuard
				t.Cleanup(func() {
					if _, err := db.Exec(`DELETE FROM cluster_meta WHERE key='broker_image_maintenance.home-a'`); err != nil {
						t.Error(err)
					}
				})
			}
			if _, err := db.Exec(query); err != nil {
				t.Fatal(err)
			}
			assertPortAuthorityCommandRows(t, db, cmd, 0)
			if _, err := portAuthorityCommand(db, "guarded-free", a, guard); !errors.Is(err, want) {
				t.Fatal("authority replaced the original allocation guard")
			}
			if _, err := db.Exec(`DELETE FROM sessions WHERE sid=?`, a.SID); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func exercisePortAuthorityScope(t *testing.T, db *sql.DB) {
	t.Helper()
	a, guard := seedPortAuthority(t, db, "scope", "DELETING", true)
	_, foreign := seedPortAuthority(t, db, "other-session", "ACTIVE", false)
	// Permission removal keeps the original five-field contract even when
	// irrelevant home/epoch metadata would be invalid for a home mutation.
	poisoned := a
	poisoned.HomeBroker, poisoned.Epoch = "\x00unused-home", -1
	if _, err := PlanFreeAllocation(db, poisoned, testRehomeTime); err != nil {
		t.Fatal("ordinary allocation planner interpreted fields outside its original guard")
	}
	for _, field := range []string{"sid", "nid", "incarnation"} {
		wrong := a
		switch field {
		case "sid":
			wrong.Authority = foreign
		case "nid":
			wrong.Authority.ConfiguredNID = "other-node"
		case "incarnation":
			wrong.Authority.Incarnation = ""
		}
		if err := FreeAllocation(db, wrong, testRehomeTime); !errors.Is(err, ErrMutationGuard) {
			t.Fatal("incomplete or foreign creation guard reached the single writer")
		}
		if _, err := PlanFreeAllocation(db, wrong, testRehomeTime); !errors.Is(err, ErrMutationGuard) {
			t.Fatal("incomplete or foreign creation guard reached the leader planner")
		}
	}
	conflicting := guard
	conflicting.InstanceID = "different-instance"
	if _, err := portAuthorityCommand(db, "guarded-free", a, conflicting); !errors.Is(err, ErrMutationGuard) {
		t.Fatal("explicit authority overwrote the inherited allocation guard")
	}
	// The comparison must use the supplied transaction, including its own
	// uncommitted creation writes, without opening the one-connection DB again.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`UPDATE sessions SET owner_pubkey_fp='uncommitted-owner' WHERE sid=?`, a.SID); err != nil {
		t.Fatal(err)
	}
	_, _, err = allocationAuthorityPredicate(tx, guard, a.SID, a.NID)
	assertPortIncarnationMismatch(t, err)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertPortAuthorityUnchanged(t, db, a, true)
}
