package brokerupgrade

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// A register's inherited expectation is compared with the session's actual
// creation before any lease, row or farewell effect; an ordinary first register
// learns it; a recreated SID never inherits the old image's authority; and an
// unreadable creation is distinguishable from a proven mismatch.
// origin: hot-update S8 plan §6.2 / §11.2 P15 (adjudicateLease / farewell call sites)
func TestRegisterAuthorityComparesCreationBeforeAnyWrite(t *testing.T) {
	db := testharness.OpenDB(t)
	hash, err := auth.HashPIN("first-pin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(db, "lab", "lab", "owner-fp", hash, time.Now()); err != nil {
		t.Fatal(err)
	}
	actual, err := RegisterAuthority(db, "lab", "")
	if err != nil || len(actual) != 64 {
		t.Fatalf("ordinary register could not learn the creation: %q %v", actual, err)
	}
	if again, err := RegisterAuthority(db, "lab", actual); err != nil || again != actual {
		t.Fatalf("matching image refused: %q %v", again, err)
	}
	stale := strings.Repeat("e", 64)
	if _, err := RegisterAuthority(db, "lab", stale); !IncarnationMismatch(err) {
		t.Fatalf("a different creation was accepted: %v", err)
	}
	// Recreate the SID with every other field equal: only the creation salt and
	// timestamp distinguish it, and no deletion event needs to have arrived.
	if _, err := db.Exec(`DELETE FROM sessions WHERE sid='lab'`); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterAuthority(db, "lab", actual); !IncarnationMismatch(err) {
		t.Fatalf("a deleted session kept lending its authority: %v", err)
	}
	hash, err = auth.HashPIN("first-pin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(db, "lab", "lab", "owner-fp", hash, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterAuthority(db, "lab", actual); !IncarnationMismatch(err) {
		t.Fatalf("the recreated session adopted the old image: %v", err)
	}
	if learned, err := RegisterAuthority(db, "lab", ""); err != nil || learned == actual || len(learned) != 64 {
		t.Fatalf("recreated session learned %q (old %q): %v", learned, actual, err)
	}
	// Legacy creation material (no PHC salt) keeps an ordinary reply
	// byte-identical, but cannot satisfy an inherited expectation.
	if _, err := session.Create(db, "legacy", "legacy", "owner-fp", "pin-hash", time.Now()); err != nil {
		t.Fatal(err)
	}
	if omitted, err := RegisterAuthority(db, "legacy", ""); omitted != "" || err != nil {
		t.Fatalf("unreadable creation changed an ordinary reply: %q %v", omitted, err)
	}
	if _, err := RegisterAuthority(db, "legacy", actual); err == nil || IncarnationMismatch(err) {
		t.Fatalf("unreadable creation must defer, not revoke or accept: %v", err)
	}
	if omitted, err := RegisterAuthority(nil, "lab", ""); omitted != "" || err != nil {
		t.Fatalf("no store must not invent an authority: %q %v", omitted, err)
	}
	var refused *upgradetx.Error
	if _, err := RegisterAuthority(nil, "lab", actual); !errors.As(err, &refused) || refused.Code != upgradetx.CodeDeferred {
		t.Fatalf("no store with an inherited image = %v", err)
	}
}

type summaryStore struct {
	upgradetx.Store
	view upgradetx.View
	err  error
	key  upgradetx.Key
	txID string
}

func (s *summaryStore) Load(key upgradetx.Key, txID string) (upgradetx.View, error) {
	s.key, s.txID = key, txID
	return s.view, s.err
}

// Public health projects only the Current record's active attempt from disk.
// It is never a running proof: RunningSHA stays empty without an observation,
// and an unreadable journal reports nothing rather than an idle broker.
// origin: hot-update S8 plan §2.2 / §7.1 / §11.2 P15
func TestBrokerHealthSummaryIsPublicDiskProjection(t *testing.T) {
	key := upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: "domain", ConfigPath: "/etc/tether/broker.yaml"}
	forward := upgradetx.Attempt{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 7},
		Phase: upgradetx.PhaseRollbackPending, Outcome: upgradetx.OutcomePending, TargetSHA: strings.Repeat("b", 64)}
	reverse := upgradetx.Attempt{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("3", 32), Generation: 3, Revision: 7},
		Phase: upgradetx.PhaseActivating, Outcome: upgradetx.OutcomePending, TargetSHA: strings.Repeat("a", 64)}
	store := &summaryStore{view: upgradetx.View{Historical: upgradetx.Record{Key: key, Forward: forward, Reverse: reverse,
		Grant: upgradetx.Grant{ActorFP: "private-actor"}}}}
	summary := HealthSummary(&Dependencies{Key: key, Store: store})
	if summary == nil || store.key != key || store.txID != "" {
		t.Fatalf("health did not read the Current record for its own key: %+v key=%+v tx=%q", summary, store.key, store.txID)
	}
	if summary.CutID != reverse.Handle.CutID || summary.Generation != 3 || summary.Phase != string(upgradetx.PhaseActivating) || summary.RunningSHA != "" {
		t.Fatalf("summary is not the active attempt's disk state: %+v", summary)
	}
	if HealthSummary(nil) != nil || HealthSummary(&Dependencies{Key: key}) != nil {
		t.Fatal("an unbound broker reported an upgrade")
	}
	store.err = &upgradetx.Error{Code: upgradetx.CodeStateCorrupt}
	if HealthSummary(&Dependencies{Key: key, Store: store}) != nil {
		t.Fatal("an unreadable journal was reported as a transaction state")
	}
}
