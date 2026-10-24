package brokerupgrade

// The rollback oracle uses real temporary SQLite databases and reads the latest
// ACK/absence set inside Activate, before any replay is possible. Image import,
// process exec and the journal remain explicit ports; P17/P23 own their seam.

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	_ "modernc.org/sqlite" // Register the real hermetic SQLite driver for the ACK oracle.
)

func reverseRequest(f *engineFixture) (upgradetx.Fence, upgradetx.ReverseRequest) {
	r := f.store.record
	return upgradetx.Fence{Current: handle(r), Owner: f.runtime.owner, RunningSHA: f.runtime.running, PreviousSHA: r.RecordedPreviousSHA},
		upgradetx.ReverseRequest{ClientNonce: "reverse", ActorFP: "actor", HistoricalTxID: r.Grant.Handle.TxID, TargetSHA: r.RecordedPreviousSHA, Scope: f.store.scope, ExpiresAt: f.store.grant.ExpiresAt}
}

func ackDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, statement := range []string{
		"CREATE TABLE ack (id TEXT PRIMARY KEY, value TEXT NOT NULL)",
		"INSERT INTO ack VALUES ('overwrite', 'first'), ('deleted', 'present'), ('revoked', 'allowed')",
	} {
		if _, err = db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// origin: hot-update S8 plan §8.1 / §8.3 M13 / §11.2 P10
func TestBrokerRollbackExportsCurrentDatabaseBeforeReplay(t *testing.T) {
	f := newEngineFixture()
	root := t.TempDir()
	currentPath, stalePath := filepath.Join(root, "current.db"), filepath.Join(root, "bootstrap.db")
	db := ackDatabase(t, currentPath)
	ackDatabase(t, stalePath)
	f.start(t)
	for _, statement := range []string{
		"UPDATE ack SET value='second' WHERE id='overwrite'",
		"UPDATE ack SET value='latest' WHERE id='overwrite'",
		"DELETE FROM ack WHERE id IN ('deleted', 'revoked')",
		"INSERT INTO ack VALUES ('new-request', 'acknowledged')",
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	f.runtime.currentDescriptor, f.runtime.frozenDescriptor = []byte(currentPath), []byte(stalePath)
	checked := false
	f.runtime.beforeActivate = func(c upgradeport.Cut) {
		// No callback is permitted to "repair" an old DB through Replay. Read
		// the complete set at the first consumer boundary, not just a prefix.
		observed, err := sql.Open("sqlite", "file:"+string(c.Descriptor)+"?mode=ro")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := observed.Close(); err != nil {
				t.Error(err)
			}
		}()
		rows, err := observed.Query("SELECT id, value FROM ack ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := rows.Close(); err != nil {
				t.Error(err)
			}
		}()
		actual := make(map[string]string)
		for rows.Next() {
			var id, value string
			if err = rows.Scan(&id, &value); err != nil {
				t.Fatal(err)
			}
			actual[id] = value
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, map[string]string{"overwrite": "latest", "new-request": "acknowledged"}) {
			t.Fatalf("stale database before first Replay: %v", actual)
		}
		checked = true
	}
	expected, req := reverseRequest(f)
	before := cloneRecord(f.store.record)
	v, err := f.endpoint.Rollback(t.Context(), expected, req)
	if err != nil {
		t.Fatal(err)
	}
	if !checked || f.runtime.exportCalls != 2 || active(v.Historical).Direction != upgradetx.DirectionReverse || active(v.Historical).Handle.Generation != expected.Current.Generation+1 {
		t.Fatal("rollback did not consume a new current cut")
	}
	if !reflect.DeepEqual(f.store.history[before.Grant.Handle.TxID], before) {
		t.Fatal("manual reverse rewrote completed forward history")
	}
	for _, receipt := range []upgradetx.Receipt{active(v.Historical).Control, active(v.Historical).Resources, active(v.Historical).Released} {
		if !sameAttempt(receipt.Stamp.Attempt, active(v.Historical).Handle) || receipt.RunningSHA != expected.PreviousSHA {
			t.Fatal("reverse reused forward receipt")
		}
	}
}

// origin: hot-update S8 plan §8.1 / §8.3 M07, M12
func TestBrokerRollbackRejectsStaleFenceAndForwardReceipts(t *testing.T) {
	for _, field := range []string{"transaction", "cut", "generation", "revision", "owner", "running", "previous", "history", "target"} {
		t.Run(field, func(t *testing.T) {
			f := newEngineFixture()
			f.start(t)
			expected, req := reverseRequest(f)
			switch field {
			case "transaction":
				expected.Current.TxID = strings.Repeat("e", 32)
			case "cut":
				expected.Current.CutID = strings.Repeat("e", 32)
			case "generation":
				expected.Current.Generation++
			case "revision":
				expected.Current.Revision--
			case "owner":
				expected.Owner.StartTicks++
			case "running":
				expected.RunningSHA = strings.Repeat("c", 64)
			case "previous":
				expected.PreviousSHA = strings.Repeat("c", 64)
			case "history":
				req.HistoricalTxID = strings.Repeat("e", 32)
			case "target":
				req.TargetSHA = strings.Repeat("c", 64)
			}
			before := cloneRecord(f.store.record)
			if _, err := f.endpoint.Rollback(t.Context(), expected, req); err == nil {
				t.Fatal("stale rollback was accepted")
			}
			if f.runtime.freezeCalls != 1 || f.runtime.activateCalls != 1 || !reflect.DeepEqual(f.store.record, before) {
				t.Fatal("stale rollback touched the current transaction")
			}
		})
	}
	for _, kind := range []upgradetx.ReceiptKind{upgradetx.ReceiptControlReady, upgradetx.ReceiptResourcesMigrated, upgradetx.ReceiptOldGenerationReleased} {
		t.Run(string(kind), func(t *testing.T) {
			f := newEngineFixture()
			completed := f.start(t)
			expected, req := reverseRequest(f)
			old := active(completed.Historical)
			f.runtime.receipt = func(r *upgradetx.Receipt) {
				if r.Kind != kind {
					return
				}
				if kind == upgradetx.ReceiptControlReady {
					*r = old.Control
				}
				if kind == upgradetx.ReceiptResourcesMigrated {
					*r = old.Resources
				}
				if kind == upgradetx.ReceiptOldGenerationReleased {
					*r = old.Released
				}
			}
			v, err := f.endpoint.Rollback(t.Context(), expected, req)
			requireAcceptedFailure(t, v, err)
		})
	}
}

// origin: hot-update S8 plan §8.1 / §8.3 M01
func TestBrokerRollbackRetiresRetainedOwnersBeforeNextGeneration(t *testing.T) {
	for _, remaining := range []bool{false, true} {
		t.Run(map[bool]string{false: "released", true: "retained"}[remaining], func(t *testing.T) {
			f := newEngineFixture()
			f.runtime.activateErr = errors.New("partial take retained a keeper")
			v, err := f.endpoint.Start(t.Context(), f.artifact.Grant, f.artifact)
			requireAcceptedFailure(t, v, err)
			if f.runtime.thawCalls != 0 || active(v.Historical).CutStage != upgradetx.CutPartialActive {
				t.Fatal("partial activation was treated as pristine")
			}
			f.runtime.activateErr = nil
			f.runtime.oldOwners = []upgradetx.Owner{f.artifact.Grant.Issuer}
			if remaining {
				f.runtime.receipt = func(r *upgradetx.Receipt) {
					if r.Kind == upgradetx.ReceiptOldGenerationReleased {
						r.Census.OldOwners = []upgradetx.Owner{f.artifact.Grant.Issuer}
					}
				}
			}
			f.runtime.beforeActivate = func(upgradeport.Cut) {
				if f.runtime.retireCalls == 0 {
					t.Fatal("third generation activated before retained owner retirement")
				}
			}
			expected, req := reverseRequest(f)
			v, err = f.endpoint.Rollback(t.Context(), expected, req)
			if remaining {
				requireAcceptedFailure(t, v, err)
				if active(v.Historical).Phase != upgradetx.PhaseRollbackBlocked || f.runtime.activateCalls != 1 {
					t.Fatal("retained owner failed to block reverse")
				}
			} else if err != nil || active(v.Historical).Phase != upgradetx.PhaseCompleted {
				t.Fatalf("current salvage failed: %v", err)
			}
		})
	}
}

// origin: hot-update S8 plan §4.1 / §8.1
func TestBrokerRollbackUnreadablePreviousPreservesCurrent(t *testing.T) {
	f := newEngineFixture()
	f.start(t)
	before := cloneRecord(f.store.record)
	f.runtime.preflightErr = refusal(upgradetx.CodeRejected, "previous cannot read the required tracker domain")
	expected, req := reverseRequest(f)
	_, err := f.endpoint.Rollback(t.Context(), expected, req)
	if err == nil || f.runtime.activateCalls != 1 || f.runtime.freezeCalls != 1 || f.runtime.running != before.Grant.TargetSHA || !reflect.DeepEqual(f.store.record, before) {
		t.Fatal("unreadable previous changed current business/history")
	}
}

// origin: hot-update S8 plan §4.1 / §8.1
func TestBrokerCancelRequiresPristineLiveOwnerAndSuccessfulThaw(t *testing.T) {
	for _, defect := range []string{"none", "thaw", "selector", "owner", "active"} {
		t.Run(defect, func(t *testing.T) {
			f := newEngineFixture()
			f.store.fail = func(ev upgradetx.Event) bool {
				return ev.Action.Kind == upgradetx.ActionActivate && ev.Action.State == upgradetx.ActionIntent
			}
			v, err := f.endpoint.Start(t.Context(), f.artifact.Grant, f.artifact)
			requireAcceptedFailure(t, v, err)
			f.store.fail = nil
			if defect == "thaw" {
				f.runtime.thawErr = errors.New("unflip failed")
			}
			if defect == "selector" {
				f.runtime.thawNoRestore = true
				f.runtime.effective = f.artifact.TargetSHA
			}
			if defect == "owner" {
				f.runtime.observe = func(o *upgradetx.Observation) { o.OwnerKnown = false }
			}
			if defect == "active" {
				f.store.record.Forward.CutStage = upgradetx.CutPartialActive
			}
			v, err = f.endpoint.Cancel(t.Context(), handle(f.store.record))
			if defect == "none" {
				if err != nil || active(v.Historical).Outcome != upgradetx.OutcomeCancelled || f.runtime.thawCalls != 1 {
					t.Fatalf("pristine cancellation failed: %v", err)
				}
			} else {
				requireAcceptedFailure(t, v, err)
				if active(v.Historical).Phase == upgradetx.PhaseCancelled {
					t.Fatal("unproven cancellation became terminal")
				}
				if defect != "thaw" && defect != "selector" && f.runtime.thawCalls != 0 {
					t.Fatal("unsafe thaw")
				}
			}
		})
	}
}

// origin: hot-update S8 plan §3.1 / §8.1
func TestBrokerArchiveCannotInventDeathProof(t *testing.T) {
	f := newEngineFixture()
	f.start(t)
	f.runtime.observe = func(o *upgradetx.Observation) { o.OwnerKnown = false; o.Census.Resources = nil }
	if _, err := f.endpoint.Archive(t.Context(), handle(f.store.record)); !hasCode(err, upgradetx.CodeUnsettled) || f.store.archives != 0 {
		t.Fatal("unknown owner was used as an archive proof")
	}
	v, err := f.endpoint.CloseWindow(t.Context(), handle(f.store.record))
	if err != nil || f.store.closes != 1 || v.Metadata.Window != upgradetx.WindowClosed {
		t.Fatal("close window did not use the sole store writer")
	}
}

// origin: hot-update S8 plan §11.2 P10 / §12.2
func TestBrokerRollbackEngineLeakCycles(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerRollbackEngine(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker rollback engine", beforeG)
	testharness.AssertNoFDLeak(t, "broker rollback engine", beforeFD, 0)
}

func exerciseBrokerRollbackEngine(t *testing.T) {
	t.Helper()
	f := newEngineFixture()
	f.start(t)
	expected, req := reverseRequest(f)
	if _, err := f.endpoint.Rollback(t.Context(), expected, req); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S8 plan §2.2 / §3.1 / §5.2
func TestBrokerUpgradeRejectsUnboundPreflight(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(*engineFixture)
	}{
		{"artifact_grant", func(f *engineFixture) { f.artifact.Grant.ActorFP = "another-actor" }},
		{"artifact_source", func(f *engineFixture) { f.artifact.SourceSHA = strings.Repeat("c", 64) }},
		{"artifact_target", func(f *engineFixture) { f.artifact.TargetSHA = strings.Repeat("c", 64) }},
		{"reader", func(f *engineFixture) {
			f.runtime.preflight = func(p *upgradeport.PreflightProof) { p.ReadersDigest = "" }
		}},
		{"config", func(f *engineFixture) {
			f.runtime.preflight = func(p *upgradeport.PreflightProof) { p.ConfigDigest = "" }
		}},
		{"unknown", func(f *engineFixture) {
			f.runtime.preflight = func(p *upgradeport.PreflightProof) { p.Census.Known = false }
		}},
		{"incomplete", func(f *engineFixture) {
			f.runtime.preflight = func(p *upgradeport.PreflightProof) { p.Census.Complete = false }
		}},
		{"admission", func(f *engineFixture) {
			f.runtime.preflight = func(p *upgradeport.PreflightProof) { p.Census.Admitting = 1 }
		}},
		{"issuer", func(f *engineFixture) {
			f.runtime.preflight = func(p *upgradeport.PreflightProof) { p.Census.Stamp.Producer.StartTicks++ }
		}},
		{"scope", func(f *engineFixture) {
			f.runtime.preflight = func(p *upgradeport.PreflightProof) { p.Census.Stamp.ScopeDigest = "other-scope" }
		}},
		{"running", func(f *engineFixture) { f.runtime.running = f.artifact.TargetSHA }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newEngineFixture()
			grant := f.artifact.Grant
			tc.corrupt(f)
			_, err := f.endpoint.Start(t.Context(), grant, f.artifact)
			if err == nil || f.store.starts != 0 || f.runtime.freezeCalls != 0 {
				t.Fatal("invalid preflight reached acceptance")
			}
		})
	}
	f := newEngineFixture()
	key := f.artifact.Grant.Handle.Key
	if _, err := f.endpoint.Prepare(t.Context(), key, upgradetx.PrepareRequest{}); err == nil {
		t.Fatal("empty mode authorized execution")
	}
	key.Component = upgradetx.ComponentAgent
	if _, err := f.endpoint.Query(t.Context(), key, "tx", "nonce"); !hasCode(err, upgradetx.CodeUnsupportedComponent) {
		t.Fatal("broker accepted another role")
	}
}

// origin: hot-update S8 plan §3.2 / §3.3
func TestBrokerUpgradeResumeSettlesOriginalActions(t *testing.T) {
	for _, kind := range []upgradetx.ActionKind{upgradetx.ActionControl, upgradetx.ActionMigrate, upgradetx.ActionRetire} {
		for _, state := range []upgradetx.ActionState{upgradetx.ActionObserved, upgradetx.ActionDurable} {
			t.Run(string(kind)+"/"+string(state), func(t *testing.T) {
				f := newEngineFixture()
				f.store.fail = func(ev upgradetx.Event) bool { return ev.Action.Kind == kind && ev.Action.State == state }
				v, err := f.endpoint.Start(t.Context(), f.artifact.Grant, f.artifact)
				requireAcceptedFailure(t, v, err)
				var original, recordedDigest string
				for _, action := range f.store.record.Actions {
					if action.Kind == kind {
						original = action.ID
						recordedDigest = action.ResultDigest
					}
				}
				f.store.fail = nil
				v, err = Resume(t.Context(), f.endpoint, handle(f.store.record))
				if err != nil || active(v.Historical).Phase != upgradetx.PhaseCompleted {
					t.Fatalf("readback failed to settle original action: %v", err)
				}
				settled := false
				for _, action := range v.Historical.Actions {
					if action.ID == original && action.State == upgradetx.ActionDurable {
						settled = true
						if recordedDigest != "" && action.ResultDigest != recordedDigest {
							t.Fatal("readback rewrote the original observed evidence")
						}
					}
				}
				if !settled || f.runtime.activateCalls != 1 || f.runtime.exportCalls != 1 {
					t.Fatal("readback abandoned its action or redispatched")
				}
			})
		}
	}
}

// origin: hot-update S8 plan §4.2 / §5.1 / §8.1
func TestBrokerUpgradeRejectsIncompleteCutCustody(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(*upgradeport.Cut)
	}{
		{"cut", func(c *upgradeport.Cut) { c.Attempt.CutID = "different" }},
		{"revision", func(c *upgradeport.Cut) { c.Attempt.Revision-- }},
		{"owner", func(c *upgradeport.Cut) { c.Owner.StartTicks++ }},
		{"descriptor", func(c *upgradeport.Cut) { c.Descriptor = nil }},
		{"scope", func(c *upgradeport.Cut) { c.Scope.Expected = c.Scope.Expected[1:] }},
		{"sequence", func(c *upgradeport.Cut) { c.Sequence-- }},
		{"nil_fd", func(c *upgradeport.Cut) { c.Auxiliary[0].File = nil }},
		{"duplicate_fd", func(c *upgradeport.Cut) { c.Auxiliary = append(c.Auxiliary, c.Auxiliary[0]) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newEngineFixture()
			file, err := os.CreateTemp(t.TempDir(), "retained-")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := file.Close(); err != nil {
					t.Error(err)
				}
			}()
			f.runtime.file, f.runtime.export = file, tc.corrupt
			v, err := f.endpoint.Start(t.Context(), f.artifact.Grant, f.artifact)
			requireAcceptedFailure(t, v, err)
			if f.runtime.activateCalls != 0 {
				t.Fatal("incomplete cut reached activation")
			}
			if _, err = file.WriteString("caller retains custody"); err != nil {
				t.Fatal("failure consumed caller custody", err)
			}
		})
	}
}

// origin: hot-update S8 plan §3.3 / §8.1 / §8.3 M12
func TestBrokerRollbackCannotClaimNextTransaction(t *testing.T) {
	f := newEngineFixture()
	f.start(t)
	oldFence, oldRequest := reverseRequest(f)
	g := f.store.grant
	g.Handle.TxID, g.Handle.CutID = strings.Repeat("5", 32), strings.Repeat("6", 32)
	g.Handle.Generation++
	g.Handle.Revision = 1
	g.SourceSHA, g.TargetSHA, g.Issuer = f.runtime.running, strings.Repeat("c", 64), f.runtime.owner
	f.store.grant = g
	f.artifact = upgradeartifact.Receipt{Grant: g, SourceSHA: g.SourceSHA, TargetSHA: g.TargetSHA}
	current := f.start(t)
	// The port deliberately leaves the old window open: the engine must also
	// enforce the live identity fence, rather than relying on metadata alone.
	if _, err := f.endpoint.Rollback(t.Context(), oldFence, oldRequest); err == nil {
		t.Fatal("historical A rollback claimed B")
	}
	if f.store.reverseAttempts != 0 || f.runtime.activateCalls != 2 || !reflect.DeepEqual(current.Historical, f.store.record) {
		t.Fatal("historical A reached B's reverse writer")
	}
}
