package session_test

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// A SID, lifecycle state and all child keys can be reused. Cleanup must reject
// the original creation both before planning and inside every delayed DELETE.
// File-backed DBs and real transfer/history stores make resource loss observable;
// a replacement that is already DELETING defeats the old state-only predicate.
// origin: hot-update S8 plan §6.2 / §11.2 P36
func TestSessionCleanupRejectsNewIncarnation(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "session cleanup", beforeG)
	defer testharness.AssertNoFDLeak(t, "session cleanup", beforeFD, 0)
	for round := 0; round < 5; round++ {
		exerciseSessionCleanup(t)
	}
}

func exerciseSessionCleanup(t *testing.T) {
	t.Helper()
	for _, changed := range []string{"salt", "owner", "created-at"} {
		t.Run(changed, func(t *testing.T) {
			db, err := storage.OpenWAL(filepath.Join(t.TempDir(), "cleanup.db"))
			if err != nil {
				t.Fatal(err)
			}
			testharness.CloseDBOnCleanup(t, db)
			now := time.Date(2026, 10, 13, 1, 2, 3, 456789000, time.FixedZone("CDT", -5*3600))
			old := cleanupCreation{owner: "owner'with?punctuation", at: now, salt: 1}
			oldGuard := createCleanupSession(t, db, old)
			// Zero expected is allowed only by the first-register contract. A
			// finalizer cannot learn whichever creation happens to occupy SID.
			for _, bad := range []upgradeauthority.Guard{
				{SID: "lab"}, {SID: "lab", Incarnation: "malformed"},
				{SID: "other", Incarnation: oldGuard.Incarnation},
			} {
				assertCleanupRefusal(t, db, bad)
			}
			if _, err := session.HardDeleteGuarded(db, oldGuard); !errors.Is(err, session.ErrDeleting) {
				t.Fatalf("active session cleanup: %v", err)
			}
			if _, cmd, err := session.PlanHardDeleteGuarded(db, oldGuard); !errors.Is(err, session.ErrDeleting) || cmd != nil {
				t.Fatalf("active session planned cleanup: command=%t err=%v", cmd != nil, err)
			}
			tombstoneCleanupSession(t, db, now)
			seedCleanupChildren(t, db, now)
			actual, oldPlan, err := session.PlanHardDeleteGuarded(db, oldGuard)
			if err != nil || actual != oldGuard.Incarnation || oldPlan == nil {
				t.Fatalf("original creation could not plan cleanup: %v", err)
			}
			// Apply must still require DELETING if lifecycle changed since planning.
			if _, err := db.Exec(`UPDATE sessions SET state='ACTIVE' WHERE sid='lab'`); err != nil {
				t.Fatal(err)
			}
			if err := cluster.ExecCommand(db, oldPlan); err != nil {
				t.Fatal(err)
			}
			assertCleanupRows(t, db, 1)
			tombstoneCleanupSession(t, db, now)
			assertCleanupPlan(t, oldPlan)
			legacyPlan, err := session.PlanHardDelete(db, "lab")
			if err != nil {
				t.Fatal(err)
			}
			actual, err = session.HardDeleteGuarded(db, oldGuard)
			if err != nil || actual != oldGuard.Incarnation {
				t.Fatalf("original creation could not clean up: %v", err)
			}
			assertCleanupRows(t, db, 0)
			// An absent-row retry is permanently inert, including after the SID
			// is recreated and tombstoned before that retry reaches Apply.
			absentPlan, err := session.PlanHardDelete(db, "lab")
			if err != nil || absentPlan == nil {
				t.Fatalf("absent cleanup plan: %v", err)
			}
			assertCleanupRefusal(t, db, oldGuard)

			replacement := old
			switch changed {
			case "salt":
				replacement.salt++
			case "owner":
				replacement.owner += "-replacement"
			case "created-at":
				replacement.at = now.Add(time.Nanosecond)
			}
			newGuard := createCleanupSession(t, db, replacement)
			if newGuard.Incarnation == oldGuard.Incarnation {
				t.Fatal("fixture did not create a distinct incarnation")
			}
			seedCleanupChildren(t, db, now)
			assertCleanupRefusal(t, db, oldGuard)
			assertCleanupRows(t, db, 1)
			tombstoneCleanupSession(t, db, now)

			// These are the replacement's real JetStream domain, not counters or
			// mock deletion callbacks. Stale SQL must preserve their authority as
			// well as the exact object version, bytes and physical history row.
			nc, err := nats.Connect(testharness.StartJSNATS(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(nc.Close)
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			if err := jsstream.EnsureHistoryStream(t.Context(), js, "lab", 1); err != nil {
				t.Fatal(err)
			}
			objects, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "transfer-lab"})
			if err != nil {
				t.Fatal(err)
			}
			payload := []byte("replacement transfer domain " + newGuard.Incarnation)
			object, err := objects.PutBytes(t.Context(), "replacement-transfer", payload)
			if err != nil {
				t.Fatal(err)
			}
			ack, err := js.Publish(t.Context(), proto.SubjAuditTransfer("lab"), payload)
			if err != nil {
				t.Fatal(err)
			}
			assertCleanupRefusal(t, db, oldGuard)
			for _, plan := range []*cluster.Command{oldPlan, legacyPlan, absentPlan} {
				// Apply the actual baked SQL, twice, after compare -> recreate.
				// Child-table checks catch a guard attached only to sessions.
				for replay := 0; replay < 2; replay++ {
					if err := cluster.ExecCommand(db, plan); err != nil {
						t.Fatalf("stale plan Apply: %v", err)
					}
					assertCleanupRows(t, db, 1)
					assertCleanupTransfer(t, objects, js, payload, object.NUID, ack.Sequence)
				}
			}
			// A later child-table error must roll back earlier DELETEs, and an
			// underlying driver error must not disclose private creation material.
			if _, err := db.Exec(`CREATE TRIGGER cleanup_failure BEFORE DELETE ON nodes BEGIN SELECT RAISE(ABORT, 'argon2id private creation material'); END`); err != nil {
				t.Fatal(err)
			}
			actual, err = session.HardDeleteGuarded(db, newGuard)
			if err == nil || actual != "" || strings.Contains(err.Error(), "argon2id") || strings.Contains(err.Error(), "private creation") {
				t.Fatalf("failed cleanup returned authority, success or private SQL error: %v", err)
			}
			if stats := db.Stats(); stats.InUse != 0 {
				t.Fatalf("failed cleanup retained a transaction: %+v", stats)
			}
			assertCleanupRows(t, db, 1)
			if _, err := db.Exec(`DROP TRIGGER cleanup_failure`); err != nil {
				t.Fatal(err)
			}
			// Rejection cannot poison the pool or prevent the replacement's own
			// finalizer from removing its subtree once settlement permits it.
			actual, currentPlan, err := session.PlanHardDeleteGuarded(db, newGuard)
			if err != nil || actual != newGuard.Incarnation || currentPlan == nil {
				t.Fatalf("replacement plan: %v", err)
			}
			assertCleanupPlan(t, currentPlan)
			if err := cluster.ExecCommand(db, currentPlan); err != nil {
				t.Fatal(err)
			}
			assertCleanupRows(t, db, 0)
			assertCleanupRefusal(t, db, newGuard)
			if stats := db.Stats(); stats.InUse != 0 {
				t.Fatalf("cleanup retained a transaction: %+v", stats)
			}
		})
	}
}

type cleanupCreation struct {
	owner string
	at    time.Time
	salt  byte
}

func createCleanupSession(t *testing.T, db *sql.DB, creation cleanupCreation) upgradeauthority.Guard {
	t.Helper()
	salt := bytes.Repeat([]byte{creation.salt}, 16)
	// A valid PHC is comparison material only; no PIN verification is needed.
	pinHash := "$argon2id$v=19$m=65536,t=3,p=2$" + base64.RawStdEncoding.EncodeToString(salt) + "$" + base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	if _, err := session.Create(db, "lab", "lab", creation.owner, pinHash, creation.at); err != nil {
		t.Fatal(err)
	}
	incarnation, err := upgradeauthority.Derive("lab", creation.owner, creation.at.UnixNano(), salt)
	if err != nil {
		t.Fatal(err)
	}
	return upgradeauthority.Guard{SID: "lab", Incarnation: incarnation, InstanceID: "instance", ConfiguredNID: "lab-1"}
}

func tombstoneCleanupSession(t *testing.T, db *sql.DB, now time.Time) {
	t.Helper()
	if err := session.Tombstone(db, "lab", now); err != nil {
		t.Fatal(err)
	}
}

func seedCleanupChildren(t *testing.T, db *sql.DB, now time.Time) {
	t.Helper()
	for _, query := range []string{
		`INSERT INTO nodes(sid,nid,last_heartbeat_at,status) VALUES('lab','lab-1',?, 'ONLINE')`,
		`INSERT INTO processes(pid,sid,nid,argv,started_at,status,started_by_fp) VALUES('replacement-pid','lab','lab-1','[]',?,'RUNNING','actor')`,
		`INSERT INTO port_allocations(port,sid,nid,name,local_port,token_hash,state,created_by_fp,created_at) VALUES(14000,'lab','lab-1','web',8080,'hash','ALLOCATED','actor',?)`,
		`INSERT INTO agent_provisioning(sid,nid,agent_fp,joined_at) VALUES('lab','lab-1','SHA256:agent',?)`,
		`INSERT INTO proxy_subscribers(sid,sub_id,name,token_hash,psk,cipher,created_by_fp,created_at) VALUES('lab','replacement-sub','alice','hash','psk','2022-blake3-aes-256-gcm','actor',?)`,
	} {
		if _, err := db.Exec(query, now); err != nil {
			t.Fatal(err)
		}
	}
}

func assertCleanupRefusal(t *testing.T, db *sql.DB, guard upgradeauthority.Guard) {
	t.Helper()
	actual, err := session.HardDeleteGuarded(db, guard)
	assertCleanupMismatch(t, err)
	if actual != "" {
		t.Fatal("rejected single cleanup returned an authority")
	}
	actual, cmd, err := session.PlanHardDeleteGuarded(db, guard)
	assertCleanupMismatch(t, err)
	if actual != "" || cmd != nil {
		t.Fatal("rejected planner returned an authority or mutation")
	}
}

func assertCleanupMismatch(t *testing.T, err error) {
	t.Helper()
	var typed *upgradetx.Error
	if !errors.As(err, &typed) || typed.Code != upgradetx.CodeSessionIncarnationMismatch || typed.Accepted {
		t.Fatalf("stale cleanup must refuse with incarnation mismatch: %v", err)
	}
	if strings.Contains(err.Error(), "argon2id") || strings.Contains(err.Error(), "SHA256:") {
		t.Fatal("cleanup error exposed creation material")
	}
}

func assertCleanupPlan(t *testing.T, cmd *cluster.Command) {
	t.Helper()
	if cmd.Op != cluster.OpSessionHardDelete || cmd.Version != cluster.CommandVersion() || len(cmd.Body) != 7 {
		t.Fatal("cleanup changed the existing replicated operation or subtree")
	}
	for _, stmt := range cmd.Body {
		if len(stmt.Args) != 0 || !strings.Contains(stmt.SQL, "state='DELETING'") || !strings.Contains(stmt.SQL, "EXISTS (SELECT 1 FROM sessions AS upgrade_authority") {
			t.Fatal("cleanup did not bake the creation and lifecycle guards into every DELETE")
		}
	}
}

func assertCleanupRows(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	for _, table := range []string{"sessions", "members", "nodes", "processes", "port_allocations", "agent_provisioning", "proxy_subscribers"} {
		var got int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table + ` WHERE sid='lab'`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("stale cleanup deleted replacement %s rows: got %d, want %d", table, got, want)
		}
	}
}

func assertCleanupTransfer(t *testing.T, objects jetstream.ObjectStore, js jetstream.JetStream, payload []byte, nuid string, sequence uint64) {
	t.Helper()
	info, err := objects.GetInfo(t.Context(), "replacement-transfer")
	if err != nil || info.NUID != nuid {
		t.Fatalf("stale cleanup removed or replaced the new transfer version: %v", err)
	}
	got, err := objects.GetBytes(t.Context(), "replacement-transfer")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("stale cleanup lost the new transfer bytes: %v", err)
	}
	stream, err := js.Stream(t.Context(), jsstream.HistoryStreamName("lab"))
	if err != nil {
		t.Fatal(err)
	}
	msg, err := stream.GetMsg(t.Context(), sequence)
	if err != nil || msg.Subject != proto.SubjAuditTransfer("lab") || !bytes.Equal(msg.Data, payload) {
		t.Fatalf("stale cleanup lost the new transfer history: %v", err)
	}
}
