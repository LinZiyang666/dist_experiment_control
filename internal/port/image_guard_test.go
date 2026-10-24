package port

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
)

// origin: hot-update S6 plan §7.2 / T26
func TestBrokerLateCloseDoesNotKillReplacement(t *testing.T) {
	for _, verb := range []string{"free", "revoke", "guarded-free"} {
		t.Run(verb, func(t *testing.T) {
			db, old, guard, _ := guardFixture(t, "expose")
			plan := func(a Allocation) (*cluster.Command, error) {
				switch verb {
				case "free":
					return PlanFreeAllocation(db, a, testRehomeTime)
				case "revoke":
					return PlanRevokeAllocation(db, a, testRehomeTime)
				default:
					guard.Expected = &a
					guard.FromHome = a.HomeBroker
					guard.ToHome = ""
					return PlanFreeAllocationGuarded(db, guard, testRehomeTime)
				}
			}
			cmd, err := plan(*old)
			if err != nil {
				t.Fatal(err)
			}
			if err := FreeAllocation(db, *old, testRehomeTime); err != nil {
				t.Fatal(err)
			}
			// Same port, home AND epoch; only the new token identifies B.
			replacement, _, err := PlanAllocate(db, old.SID, old.NID, old.Name, old.LocalPort, old.Port,
				old.CreatedByFP, old.HomeBroker, false, tinyBand())
			if err != nil {
				t.Fatal(err)
			}
			insert := `INSERT INTO port_allocations (port,sid,nid,name,local_port,token_hash,state,created_by_fp,created_at,home_broker,epoch) VALUES (?,?,?,?,?,?,'ALLOCATED',?,?,?,?)`
			if _, err := db.Exec(insert, replacement.Port, replacement.SID, replacement.NID, replacement.Name,
				replacement.LocalPort, replacement.TokenHash, replacement.CreatedByFP, replacement.CreatedAt, replacement.HomeBroker, old.Epoch); err != nil {
				t.Fatal(err)
			}
			if _, err := plan(*old); !errors.Is(err, ErrNotFound) {
				t.Fatalf("late A planning = %v, want ErrNotFound", err)
			}
			assertCommandRows(t, db, cmd, 0)
			current, err := LookupByName(db, old.SID, old.Name)
			if err != nil || current.TokenHash != replacement.TokenHash || current.Epoch != old.Epoch || current.HomeBroker != old.HomeBroker {
				t.Fatalf("late A close changed B: current=%+v err=%v", current, err)
			}
			fresh, err := plan(*current)
			if err != nil {
				t.Fatal(err)
			}
			assertCommandRows(t, db, fresh, 1)
		})
	}
}

// origin: hot-update S6 plan §2.1 / §7.1 / §7.2 / T42
func TestRehomeFullAllocationCAS(t *testing.T) {
	t.Run("every-identity-field", func(t *testing.T) {
		fields := []struct {
			name   string
			change func(*Allocation)
			set    string
		}{
			{"port", func(a *Allocation) { a.Port++ }, `port=14002`},
			{"sid", func(a *Allocation) { a.SID = "other" }, `sid='other'`},
			{"nid", func(a *Allocation) { a.NID = "other-node" }, `nid='other-node'`},
			{"name", func(a *Allocation) { a.Name = "other-name" }, `name='other-name'`},
			{"token", func(a *Allocation) { a.TokenHash = "other-hash" }, `token_hash='other-hash'`},
			{"home", func(a *Allocation) { a.HomeBroker = "home-c" }, `home_broker='home-c'`},
			{"epoch", func(a *Allocation) { a.Epoch++ }, `epoch=1`},
			{"state", func(a *Allocation) { a.State = StateFreed }, `state='FREED'`},
		}
		for _, verb := range []string{"rehome", "free", "rotate"} {
			for _, field := range fields {
				t.Run(verb+"/"+field.name, func(t *testing.T) {
					db, old, guard, intent := guardFixture(t, "expose")
					cmd, err := guardedCommand(db, verb, guard, intent)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := db.Exec(`UPDATE port_allocations SET ` + field.set + ` WHERE state='ALLOCATED'`); err != nil {
						t.Fatal(err)
					}
					// A delayed command must be an observable zero-row result,
					// including the insert half of rotation, not merely no error.
					assertCommandRows(t, db, cmd, 0)
					assertAllocationCount(t, db, 1)
					if _, err := guardedCommand(db, verb, guard, intent); !errors.Is(err, ErrNotFound) {
						t.Fatalf("stale writer planning = %v", err)
					}
					// Independently test the expected identity, not only stale SQL.
					if _, err := db.Exec(`UPDATE port_allocations SET ` + identityRestoreSQL(*old)); err != nil {
						t.Fatal(err)
					}
					wrong := *old
					field.change(&wrong)
					guard.Expected = &wrong
					if _, err := guardedCommand(db, verb, guard, intent); err == nil {
						t.Fatal("wrong expected identity accepted")
					}
				})
			}
		}
	})
	t.Run("from-and-to-maintenance", func(t *testing.T) {
		for _, verb := range []string{"rehome", "free", "rotate", "allocate"} {
			for _, home := range []string{"home-a", "home-b"} {
				if (verb == "free" && home == "home-b") || (verb == "allocate" && home == "home-a") {
					continue
				}
				t.Run(verb+"/"+home, func(t *testing.T) {
					db, _, guard, intent := guardFixture(t, "expose")
					if verb == "allocate" {
						guard.Expected = nil
						guard.ExpectAbsent = true
						guard.FromHome = ""
						intent.Name = "new-name"
					}
					if verb == "free" {
						guard.ToHome = ""
					}
					cmd, err := guardedCommand(db, verb, guard, intent)
					if err != nil {
						t.Fatal(err)
					}
					lease, raw := maintenanceFixture(home, brokermaint.MaintActive, guard.NowBootNS)
					putMaintenance(t, db, home, raw)
					assertCommandRows(t, db, cmd, 0)
					assertAllocationCount(t, db, 1)
					// The guard was created before the current writer callback.
					if _, err := guardedCommand(db, verb, guard, intent); !errors.Is(err, ErrMutationGuard) {
						t.Fatalf("stale maintenance expectation = %v", err)
					}
					guard.ExpectedMaintenance[home] = raw
					// An unobserved record is no authority to freeze ordinary
					// failure recovery forever. Planned cut admission separately
					// requires every observer's proof and ACK before activation.
					// origin: hot-update S6 plan §7.1–§7.2 / T42 / TESTFIX
					unobserved, err := guardedCommand(db, verb, guard, intent)
					if err != nil || unobserved == nil {
						t.Fatalf("unobserved maintenance blocked failure recovery: %v", err)
					}
					// Lack of protection never removes the record CAS: a newly
					// installed maintenance record must invalidate this command.
					_, nextRecord := maintenanceFixture(home, brokermaint.MaintPrepared, guard.NowBootNS)
					putMaintenance(t, db, home, nextRecord)
					assertCommandRows(t, db, unobserved, 0)
					putMaintenance(t, db, home, raw)
					guard.Proofs[home] = lease
					if _, err := guardedCommand(db, verb, guard, intent); !errors.Is(err, ErrMaintenanceProtected) {
						t.Fatalf("active maintenance = %v", err)
					}
					lease.ExpiresLocalBootNS = guard.NowBootNS
					guard.Proofs[home] = lease
					cmd, err = guardedCommand(db, verb, guard, intent)
					if err != nil {
						t.Fatalf("expired local proof blocked ordinary failure action: %v", err)
					}
					// Even an expired record remains an exact SQL CAS expectation.
					ended := lease.Record
					ended.Phase = brokermaint.MaintEnded
					body, err := json.Marshal(ended)
					if err != nil {
						t.Fatal(err)
					}
					putMaintenance(t, db, home, string(body))
					assertCommandRows(t, db, cmd, 0)
					guard.ExpectedMaintenance[home] = string(body)
					delete(guard.Proofs, home)
					cmd, err = guardedCommand(db, verb, guard, intent)
					if err != nil {
						t.Fatalf("ended maintenance blocked action: %v", err)
					}
					assertCommandRows(t, db, cmd, 1)
				})
			}
		}
	})
	t.Run("proof-qualification", func(t *testing.T) {
		cases := []struct {
			name   string
			change func(*brokermaint.LocalLease)
		}{
			{"record", func(l *brokermaint.LocalLease) { l.Record.Holder = "other" }},
			{"cut", func(l *brokermaint.LocalLease) { l.Proof.CutID = "other" }},
			{"boot", func(l *brokermaint.LocalLease) { l.Proof.IssuerBootID = "other" }},
			{"nonce", func(l *brokermaint.LocalLease) { l.Proof.Nonce = "" }},
			{"signature", func(l *brokermaint.LocalLease) { l.Proof.Sig = "" }},
			{"digest", func(l *brokermaint.LocalLease) { l.Proof.RecordDigest = strings.Repeat("0", 64) }},
			{"issuer-expiry", func(l *brokermaint.LocalLease) { l.Proof.ExpiresBootNS++ }},
			{"response-before-issue", func(l *brokermaint.LocalLease) { l.Proof.ResponseBootNS = l.Record.IssuedBootNS - 1 }},
			{"response-expired", func(l *brokermaint.LocalLease) {
				l.Proof.ResponseBootNS = l.Record.ExpiresBootNS
				l.ExpiresLocalBootNS = 1
			}},
			{"applied", func(l *brokermaint.LocalLease) { l.Proof.AppliedIndex = 0 }},
			{"local-expiry", func(l *brokermaint.LocalLease) { l.ExpiresLocalBootNS = 0 }},
			{"extended-expiry", func(l *brokermaint.LocalLease) { l.ExpiresLocalBootNS += int64(20 * time.Second) }},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				db, _, guard, intent := guardFixture(t, "expose")
				lease, raw := maintenanceFixture("home-a", brokermaint.MaintActive, guard.NowBootNS)
				putMaintenance(t, db, "home-a", raw)
				guard.ExpectedMaintenance["home-a"] = raw
				c.change(&lease)
				guard.Proofs["home-a"] = lease
				if _, err := guardedCommand(db, "rehome", guard, intent); !errors.Is(err, ErrMaintenanceUnknown) {
					t.Fatalf("invalid proof accepted: %v", err)
				}
			})
		}
	})
	t.Run("record-qualification", func(t *testing.T) {
		cases := []struct {
			name   string
			change func(*brokermaint.Record)
		}{
			{"home", func(r *brokermaint.Record) { r.Home = "other" }},
			{"cut", func(r *brokermaint.Record) { r.CutID = "" }},
			{"holder", func(r *brokermaint.Record) { r.Holder = "" }},
			{"instance", func(r *brokermaint.Record) { r.InstanceID = "" }},
			{"boot", func(r *brokermaint.Record) { r.IssuerBootID = "" }},
			{"generation", func(r *brokermaint.Record) { r.Generation = 0 }},
			{"capability", func(r *brokermaint.Record) { r.RequiredCapability = "unknown" }},
			{"issue", func(r *brokermaint.Record) { r.IssuedBootNS = 0; r.ExpiresBootNS = int64(8 * time.Second) }},
			{"expiry", func(r *brokermaint.Record) { r.ExpiresBootNS = r.IssuedBootNS }},
			{"ttl", func(r *brokermaint.Record) { r.ExpiresBootNS = r.IssuedBootNS + int64(9*time.Second) }},
			{"mode", func(r *brokermaint.Record) { r.Mode = "unknown" }},
			{"phase", func(r *brokermaint.Record) { r.Phase = 0 }},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				db, _, guard, intent := guardFixture(t, "expose")
				lease, _ := maintenanceFixture("home-a", brokermaint.MaintEnded, guard.NowBootNS)
				c.change(&lease.Record)
				body, err := json.Marshal(lease.Record)
				if err != nil {
					t.Fatal(err)
				}
				putMaintenance(t, db, "home-a", string(body))
				guard.ExpectedMaintenance["home-a"] = string(body)
				if _, err := guardedCommand(db, "rehome", guard, intent); !errors.Is(err, ErrMaintenanceUnknown) {
					t.Fatalf("invalid record accepted: %v", err)
				}
			})
		}
		for _, raw := range []string{"", "{", `{}`, `null`} {
			t.Run("malformed/"+raw, func(t *testing.T) {
				db, _, guard, intent := guardFixture(t, "expose")
				putMaintenance(t, db, "home-a", raw)
				guard.ExpectedMaintenance["home-a"] = raw
				if _, err := guardedCommand(db, "rehome", guard, intent); err == nil {
					t.Fatal("malformed maintenance accepted")
				}
			})
		}
	})
	t.Run("noncanonical-record-and-deleted-expectation", func(t *testing.T) {
		for _, defect := range []string{"whitespace", "unknown-field", "deleted"} {
			t.Run(defect, func(t *testing.T) {
				db, _, guard, intent := guardFixture(t, "expose")
				_, raw := maintenanceFixture("home-a", brokermaint.MaintEnded, guard.NowBootNS)
				switch defect {
				case "whitespace":
					raw += " "
				case "unknown-field":
					raw = raw[:len(raw)-1] + `,"unknown":1}`
				}
				guard.ExpectedMaintenance["home-a"] = raw
				if defect != "deleted" {
					putMaintenance(t, db, "home-a", raw)
				}
				if _, err := guardedCommand(db, "rehome", guard, intent); err == nil {
					t.Fatal("noncanonical or deleted expected record accepted")
				}
			})
		}
	})
	t.Run("proxy-name-scope", func(t *testing.T) {
		db, _, guard, intent := guardFixture(t, ProxyPortName)
		guard.Expected = nil
		guard.ExpectAbsent = true
		guard.FromHome = ""
		intent.NID = "other-node"
		allocation, cmd, err := PlanAllocateGuarded(db, guard, intent)
		if err != nil {
			t.Fatalf("another node proxy blocked by shared name: %v", err)
		}
		if cmd.Op != cluster.OpProxyAllocate {
			t.Fatalf("proxy allocation op = %s", cmd.Op)
		}
		assertCommandRows(t, db, cmd, 1)
		current, err := LookupProxyByNode(db, intent.SID, intent.NID)
		if err != nil || current.TokenHash != allocation.TokenHash {
			t.Fatal("proxy allocation not committed")
		}
		if _, _, err := PlanAllocateGuarded(db, guard, intent); !errors.Is(err, ErrNameTaken) {
			t.Fatalf("duplicate proxy = %v", err)
		}
	})
	t.Run("rotation-atomic-and-absence", func(t *testing.T) {
		for _, name := range []string{"expose", ProxyPortName} {
			t.Run(name, func(t *testing.T) {
				db, old, guard, intent := guardFixture(t, name)
				intent.DesiredPort = old.Port // Same-port rotation needs the free and insert in one transaction.
				intent.RebuildOff = true
				allocation, cmd, err := PlanRotateAllocationGuarded(db, guard, intent, testRehomeTime)
				if err != nil {
					t.Fatal(err)
				}
				if len(cmd.Body) != 2 {
					t.Fatalf("rotation split across commands: %+v", cmd)
				}
				if allocation.Token == "" || allocation.TokenHash != HashToken(allocation.Token) || allocation.HomeBroker != intent.HomeBroker || !allocation.RebuildOff {
					t.Fatal("invalid replacement projection")
				}
				for _, stmt := range cmd.Body {
					if len(stmt.Args) != 0 || strings.Contains(stmt.SQL, allocation.Token) {
						t.Fatal("raw token or unbaked args replicated")
					}
				}
				// Force an INSERT failure after a successful free; the entire command
				// must roll back, leaving the old allocation live.
				if _, err := db.Exec(`CREATE TRIGGER reject_rotation BEFORE INSERT ON port_allocations BEGIN SELECT RAISE(ABORT,'test rotation rollback'); END`); err != nil {
					t.Fatal(err)
				}
				if err := cluster.ExecCommand(db, cmd); err == nil {
					t.Fatal("insert failure not reported")
				}
				current, err := LookupByName(db, old.SID, old.Name)
				if err != nil || current.TokenHash != old.TokenHash {
					t.Fatal("free escaped failed rotation transaction")
				}
				if _, err := db.Exec(`DROP TRIGGER reject_rotation`); err != nil {
					t.Fatal(err)
				}
				assertCommandRows(t, db, cmd, 1)
				current, err = LookupByName(db, old.SID, old.Name)
				if err != nil || current.TokenHash != allocation.TokenHash || current.HomeBroker != intent.HomeBroker || current.Epoch != 0 {
					t.Fatalf("replacement not committed: %+v %v", current, err)
				}
				assertCommandRows(t, db, cmd, 0) // Exact replay must not insert a second replacement.
				assertAllocationCount(t, db, 2)
			})
		}
	})
	t.Run("prepared-maintenance-and-unrelated-home", func(t *testing.T) {
		db, _, guard, intent := guardFixture(t, "expose")
		lease, raw := maintenanceFixture("home-a", brokermaint.MaintPrepared, guard.NowBootNS)
		putMaintenance(t, db, "home-a", raw)
		guard.ExpectedMaintenance["home-a"] = raw
		guard.Proofs["home-a"] = lease
		if _, err := guardedCommand(db, "rehome", guard, intent); !errors.Is(err, ErrMaintenanceProtected) {
			t.Fatalf("prepared maintenance = %v", err)
		}
		if _, err := db.Exec(`DELETE FROM cluster_meta WHERE key=?`, maintenanceKeyPrefix+"home-a"); err != nil {
			t.Fatal(err)
		}
		guard.ExpectedMaintenance["home-a"] = ""
		putMaintenance(t, db, "home-c", raw)
		cmd, err := guardedCommand(db, "rehome", guard, intent)
		if err != nil {
			t.Fatalf("unrelated home maintenance blocked action: %v", err)
		}
		assertCommandRows(t, db, cmd, 1)
	})
	t.Run("all-planner-shapes", func(t *testing.T) {
		for _, verb := range []string{"allocate", "rotate", "free"} {
			for _, defect := range []string{"expected", "absent", "from", "to"} {
				t.Run(verb+"/"+defect, func(t *testing.T) {
					db, _, guard, intent := guardFixture(t, "expose")
					if verb == "allocate" {
						guard.Expected = nil
						guard.ExpectAbsent = true
						guard.FromHome = ""
						intent.Name = "new-name"
					}
					if verb == "free" {
						guard.ToHome = ""
					}
					switch defect {
					case "expected":
						if guard.Expected == nil {
							a := Allocation{}
							guard.Expected = &a
						} else {
							guard.Expected = nil
						}
					case "absent":
						guard.ExpectAbsent = !guard.ExpectAbsent
					case "from":
						guard.FromHome = "other"
					case "to":
						guard.ToHome = "other"
						guard.ExpectedMaintenance["other"] = ""
					}
					var err error
					if verb == "free" {
						_, err = PlanFreeAllocationGuarded(db, guard, testRehomeTime)
					} else {
						_, err = guardedCommand(db, verb, guard, intent)
					}
					if !errors.Is(err, ErrMutationGuard) {
						t.Fatalf("invalid %s shape = %v", verb, err)
					}
				})
			}
		}
	})
	t.Run("rotation-logical-identity", func(t *testing.T) {
		for _, field := range []string{"sid", "nid", "name"} {
			t.Run(field, func(t *testing.T) {
				db, _, guard, intent := guardFixture(t, "expose")
				switch field {
				case "sid":
					intent.SID = "other"
				case "nid":
					intent.NID = "other-node"
				case "name":
					intent.Name = "other-name"
				}
				if _, _, err := PlanRotateAllocationGuarded(db, guard, intent, testRehomeTime); !errors.Is(err, ErrMutationGuard) {
					t.Fatalf("rotation changed logical identity: %v", err)
				}
			})
		}
	})
	t.Run("rotation-stale-free", func(t *testing.T) {
		db, old, guard, intent := guardFixture(t, "expose")
		_, cmd, err := PlanRotateAllocationGuarded(db, guard, intent, testRehomeTime)
		if err != nil {
			t.Fatal(err)
		}
		// Same timestamp and identity as the rotation's free, but performed by
		// an earlier command. Only changes() proves this rotation won its CAS.
		if err := FreeAllocation(db, *old, testRehomeTime); err != nil {
			t.Fatal(err)
		}
		assertCommandRows(t, db, cmd, 0)
		assertAllocationCount(t, db, 1)
	})
	t.Run("rotation-target-conflict", func(t *testing.T) {
		db, old, guard, intent := guardFixture(t, "expose")
		allocation, cmd, err := PlanRotateAllocationGuarded(db, guard, intent, testRehomeTime)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Allocate(db, old.SID, old.NID, "other-name", 1, allocation.Port, "", false, tinyBand()); err != nil {
			t.Fatal(err)
		}
		assertCommandRows(t, db, cmd, 0)
		current, err := LookupByName(db, old.SID, old.Name)
		if err != nil || current.TokenHash != old.TokenHash {
			t.Fatal("target conflict freed the source allocation")
		}
	})
	t.Run("allocate-conflicts-after-planning", func(t *testing.T) {
		for _, conflict := range []string{"name", "port"} {
			t.Run(conflict, func(t *testing.T) {
				db, old, guard, intent := guardFixture(t, "expose")
				guard.Expected = nil
				guard.ExpectAbsent = true
				guard.FromHome = ""
				intent.Name = "new-name"
				intent.DesiredPort = old.Port + 1
				allocation, cmd, err := PlanAllocateGuarded(db, guard, intent)
				if err != nil {
					t.Fatal(err)
				}
				otherName, otherPort := intent.Name, old.Port+2
				if conflict == "port" {
					otherName = "other-name"
					otherPort = intent.DesiredPort
				}
				if _, err := Allocate(db, intent.SID, intent.NID, otherName, 1, otherPort, "", false, tinyBand()); err != nil {
					t.Fatal(err)
				}
				assertCommandRows(t, db, cmd, 0)
				var found int
				if err := db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE token_hash=?`, allocation.TokenHash).Scan(&found); err != nil || found != 0 {
					t.Fatal("losing allocation projection published")
				}
			})
		}
	})
	t.Run("guard-shape-and-epoch-overflow", func(t *testing.T) {
		cases := []struct {
			name   string
			change func(*MutationGuard)
		}{
			{"port", func(g *MutationGuard) { g.Expected.Port = 0 }},
			{"negative-epoch", func(g *MutationGuard) { g.Expected.Epoch = -1 }},
			{"state", func(g *MutationGuard) { g.Expected.State = StateFreed }},
			{"sid", func(g *MutationGuard) { g.Expected.SID = "" }},
			{"nid", func(g *MutationGuard) { g.Expected.NID = "" }},
			{"name", func(g *MutationGuard) { g.Expected.Name = "" }},
			{"token", func(g *MutationGuard) { g.Expected.TokenHash = "" }},
			{"no-clock", func(g *MutationGuard) { g.NowBootNS = 0 }},
			{"absent-with-expected", func(g *MutationGuard) { g.ExpectAbsent = true }},
			{"no-expected", func(g *MutationGuard) { g.Expected = nil }},
			{"from", func(g *MutationGuard) { g.FromHome = "other"; g.ExpectedMaintenance["other"] = "" }},
			{"to", func(g *MutationGuard) { g.ToHome = "other" }},
			{"unobserved-home", func(g *MutationGuard) { delete(g.ExpectedMaintenance, "home-b") }},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				db, _, guard, intent := guardFixture(t, "expose")
				c.change(&guard)
				if _, err := guardedCommand(db, "rehome", guard, intent); !errors.Is(err, ErrMutationGuard) {
					t.Fatalf("incomplete guard = %v", err)
				}
			})
		}
		db, old, guard, intent := guardFixture(t, "expose")
		old.Epoch = math.MaxInt64
		if _, err := db.Exec(`UPDATE port_allocations SET epoch=?`, old.Epoch); err != nil {
			t.Fatal(err)
		}
		if _, err := guardedCommand(db, "rehome", guard, intent); !errors.Is(err, ErrMutationGuard) {
			t.Fatalf("epoch overflow = %v", err)
		}
	})
	t.Run("permission-revoke-bypasses-maintenance", func(t *testing.T) {
		db, old, guard, _ := guardFixture(t, "expose")
		_, raw := maintenanceFixture("home-a", brokermaint.MaintActive, guard.NowBootNS)
		putMaintenance(t, db, "home-a", raw)
		cmd, err := PlanRevokeAllocation(db, *old, testRehomeTime)
		if err != nil {
			t.Fatal(err)
		}
		assertCommandRows(t, db, cmd, 1)
	})
}

var (
	_ PlanReassignHomeGuardedFunc     = PlanReassignHomeGuarded
	_ PlanAllocateGuardedFunc         = PlanAllocateGuarded
	_ PlanRotateAllocationGuardedFunc = PlanRotateAllocationGuarded
	_ PlanFreeAllocationGuardedFunc   = PlanFreeAllocationGuarded
)

func guardFixture(t *testing.T, name string) (*sql.DB, *Allocation, MutationGuard, AllocationIntent) {
	t.Helper()
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	seedSessionAndNode(t, db, "other", "lab-1")
	for _, sid := range []string{"lab", "other"} {
		if _, err := db.Exec(`INSERT INTO nodes(sid,nid,status) VALUES (?,'other-node','ONLINE')`, sid); err != nil {
			t.Fatal(err)
		}
	}
	allocation, cmd, err := PlanAllocate(db, "lab", "lab-1", name, 8080, 14000, "owner", "home-a", false, tinyBand())
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
	guard := MutationGuard{Expected: allocation, FromHome: "home-a", ToHome: "home-b", ExpectedMaintenance: map[string]string{"home-a": "", "home-b": ""}, Proofs: map[string]brokermaint.LocalLease{}, NowBootNS: int64(100 * time.Second)}
	intent := AllocationIntent{SID: allocation.SID, NID: allocation.NID, Name: name, LocalPort: allocation.LocalPort, HomeBroker: "home-b", Config: tinyBand()}
	return db, allocation, guard, intent
}

func guardedCommand(db *sql.DB, verb string, guard MutationGuard, intent AllocationIntent) (*cluster.Command, error) {
	switch verb {
	case "rehome":
		_, cmd, err := PlanReassignHomeGuarded(db, guard, intent.HomeBroker, testRehomeTime)
		return cmd, err
	case "free":
		guard.ToHome = ""
		return PlanFreeAllocationGuarded(db, guard, testRehomeTime)
	case "rotate":
		_, cmd, err := PlanRotateAllocationGuarded(db, guard, intent, testRehomeTime)
		return cmd, err
	case "allocate":
		_, cmd, err := PlanAllocateGuarded(db, guard, intent)
		return cmd, err
	default:
		panic("unknown test verb")
	}
}

// These are consumer fixtures, not claims of P15 challenge/signature verification.
// Deliberately separate issuer/local clock domains expose accidental BOOTTIME
// comparison; signature verification belongs to P15 before populating the cache.
func maintenanceFixture(home string, phase brokermaint.Phase, localNow int64) (brokermaint.LocalLease, string) {
	record := brokermaint.Record{CutID: "cut", Holder: "holder", Home: home, InstanceID: "instance", IssuerBootID: "issuer-boot", Mode: "quorum", Generation: 1, Phase: phase, IssuedBootNS: int64(10000 * time.Second), ExpiresBootNS: int64(10008 * time.Second), Term: 1, ConfigIndex: 1, VoterDigest: "voters", BaselineDigest: "baseline", RequiredCapability: brokermaint.Capability}
	body, err := json.Marshal(record)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(body)
	proof := brokermaint.Proof{CutID: record.CutID, Nonce: "nonce", IssuerBootID: record.IssuerBootID, ResponseBootNS: record.IssuedBootNS + int64(time.Second), ExpiresBootNS: record.ExpiresBootNS, RecordDigest: hex.EncodeToString(digest[:]), AppliedIndex: 1, Sig: "verified-cache-fixture"}
	return brokermaint.LocalLease{Record: record, ExpiresLocalBootNS: localNow + int64(5*time.Second), Proof: proof}, string(body)
}

func putMaintenance(t *testing.T, db *sql.DB, home, raw string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, maintenanceKeyPrefix+home, raw); err != nil {
		t.Fatal(err)
	}
}

func assertCommandRows(t *testing.T, db *sql.DB, cmd *cluster.Command, want int64) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for index, stmt := range cmd.Body {
		result, err := tx.Exec(stmt.SQL, stmt.Args...)
		if err != nil {
			t.Fatalf("statement %d failed: %v", index, err)
		}
		got, err := result.RowsAffected()
		if err != nil || got != want {
			t.Fatalf("statement %d rows=%d err=%v, want %d", index, got, err, want)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func assertAllocationCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM port_allocations`).Scan(&got); err != nil || got != want {
		t.Fatalf("allocation count=%d err=%v, want %d", got, err, want)
	}
}

func identityRestoreSQL(a Allocation) string {
	identity, _ := cluster.LitTextAll(a.SID, a.NID, a.Name, a.TokenHash, a.HomeBroker)
	return `port=` + cluster.LitInt(int64(a.Port)) + `,sid=` + identity[0] + `,nid=` + identity[1] + `,name=` + identity[2] + `,token_hash=` + identity[3] + `,home_broker=` + identity[4] + `,epoch=` + cluster.LitInt(a.Epoch) + `,state='ALLOCATED'`
}
