package upgradecli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradetx/contractstub"
)

// startEndpoint replaces only Start; every other method stays P00's
// fail-closed stub so an accidental Prepare/Rollback cannot succeed.
type startEndpoint struct {
	contractstub.AgentEndpoint
	start func(upgradetx.Grant, upgradeartifact.Receipt) (upgradetx.View, error)
	calls int
}

func (e *startEndpoint) Start(_ context.Context, g upgradetx.Grant, r upgradeartifact.Receipt) (upgradetx.View, error) {
	e.calls++
	return e.start(g, r)
}

// The ctl home records exactly one grant per target-issued ID. A different
// grant under the same ID, a missing entry or an unreadable one never yields a
// guessed Key for a later status/rollback.
// origin: hot-update S8 plan §3.1 / §7.1 / §11.2 P19
func TestGrantCacheRefusesConflictingOrUnknownTransactions(t *testing.T) {
	home := t.TempDir()
	g := remoteGrant()
	if _, err := RecallGrant(home, g.Handle.TxID); ExitCode(err) != 64 {
		t.Fatalf("unknown transaction must be a 64 operator error, got %v", err)
	}
	if err := RememberGrant(home, g); err != nil {
		t.Fatal(err)
	}
	if err := RememberGrant(home, g); err != nil {
		t.Fatalf("identical grant must be idempotent: %v", err)
	}
	other := g
	other.Handle.Key.ConfiguredNID = "other-node"
	if err := RememberGrant(home, other); ExitCode(err) != 64 {
		t.Fatalf("a second grant under one ID retargeted the transaction: %v", err)
	}
	got, err := RecallGrant(home, g.Handle.TxID)
	if err != nil || got != g {
		t.Fatalf("recalled grant changed: %+v %v", got, err)
	}
	path := filepath.Join(grantCacheDir(home), g.Handle.TxID+".json")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("grant cache must be private: %v %v", info, err)
	}
	if err := os.WriteFile(path, []byte(`{"handle":{"tx_id":"`+g.Handle.TxID+`"},"handle":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := RecallGrant(home, g.Handle.TxID); ExitCode(err) != 74 {
		t.Fatalf("a duplicate-field cache entry must be state_corrupt, got %v", err)
	}
	if err := RememberGrant(home, g); err == nil {
		t.Fatal("a corrupt cache entry was overwritten instead of refused")
	}
}

// Resume starts exactly the receipt's grant for the named ID. A receipt for
// another transaction, or one that disagrees with the recorded grant, never
// reaches the target; a reply about another grant is an unsettled accepted
// result, not a success.
// origin: hot-update S8 plan §3.1 / §5.2 / M16 / §11.2 P19
func TestResumeStartsOnlyTheOriginalGrant(t *testing.T) {
	ctx := context.Background() // ctx-root: synchronous CLI policy test.
	g := remoteGrant()
	ok := func(g upgradetx.Grant, _ upgradeartifact.Receipt) (upgradetx.View, error) {
		return upgradetx.View{Historical: upgradetx.Record{Key: g.Handle.Key, Grant: g}}, nil
	}
	t.Run("wrong_tx", func(t *testing.T) {
		e := &startEndpoint{start: ok}
		if _, err := Resume(ctx, e, t.TempDir(), strings.Repeat("9", 32), remoteReceipt(g)); ExitCode(err) != 64 || e.calls != 0 {
			t.Fatalf("receipt retargeted another transaction: calls=%d err=%v", e.calls, err)
		}
	})
	t.Run("sha_mismatch", func(t *testing.T) {
		e, r := &startEndpoint{start: ok}, remoteReceipt(g)
		r.TargetSHA = strings.Repeat("0", 64)
		if _, err := Resume(ctx, e, t.TempDir(), g.Handle.TxID, r); ExitCode(err) != 64 || e.calls != 0 {
			t.Fatalf("receipt with another target SHA was sent: %v", err)
		}
	})
	t.Run("recorded_grant_differs", func(t *testing.T) {
		home, e := t.TempDir(), &startEndpoint{start: ok}
		other := g
		other.ClientNonce = strings.Repeat("8", 32)
		if err := RememberGrant(home, other); err != nil {
			t.Fatal(err)
		}
		if _, err := Resume(ctx, e, home, g.Handle.TxID, remoteReceipt(g)); ExitCode(err) != 64 || e.calls != 0 {
			t.Fatalf("receipt grant replaced the recorded grant: %v", err)
		}
	})
	t.Run("reply_for_other_grant", func(t *testing.T) {
		e := &startEndpoint{start: func(g upgradetx.Grant, _ upgradeartifact.Receipt) (upgradetx.View, error) {
			g.ActorFP = "SHA256:other"
			return upgradetx.View{Historical: upgradetx.Record{Key: g.Handle.Key, Grant: g}}, nil
		}}
		_, err := Resume(ctx, e, t.TempDir(), g.Handle.TxID, remoteReceipt(g))
		var failure *upgradetx.Error
		if !errors.As(err, &failure) || failure.Code != upgradetx.CodeUnsettled || !failure.Accepted {
			t.Fatalf("reply for another grant signed success: %v", err)
		}
	})
	t.Run("accepted_then_recorded", func(t *testing.T) {
		home, e := t.TempDir(), &startEndpoint{start: ok}
		if _, err := Resume(ctx, e, home, g.Handle.TxID, remoteReceipt(g)); err != nil || e.calls != 1 {
			t.Fatalf("authorized resume failed: calls=%d err=%v", e.calls, err)
		}
		if got, err := RecallGrant(home, g.Handle.TxID); err != nil || got != g {
			t.Fatalf("accepted grant was not recorded for later verbs: %v", err)
		}
	})
}

func reverseView() upgradetx.View {
	g := remoteGrant()
	h := g.Handle
	attempt := upgradetx.Attempt{Handle: h, Direction: upgradetx.DirectionForward, Phase: upgradetx.PhaseCompleted, Outcome: upgradetx.OutcomeSuccess,
		Scope: acceptedReverseScope(h.Key)}
	producer := upgradetx.Owner{BootID: "boot", PID: 8, StartTicks: 10, InstanceID: strings.Repeat("e", 32)}
	return upgradetx.View{
		Historical: upgradetx.Record{Key: h.Key, Revision: h.Revision, Grant: g, Forward: attempt, RecordedPreviousSHA: g.SourceSHA},
		Observation: upgradetx.Observation{OwnerKnown: true, Current: attempt, RunningSHA: g.TargetSHA, Census: upgradetx.Census{
			Known: true, Complete: true, Stamp: upgradetx.Stamp{Producer: producer},
			Resources: []upgradetx.Resource{{Domain: upgradetx.DomainAuthority, ID: strings.Repeat("a", 64), Origin: "session:lab", Owner: producer, Generation: h.Generation, State: "active", Custody: "target"}, {Domain: upgradetx.DomainExec, ID: "exec-1", Origin: "request:exec-1", Owner: producer, Generation: h.Generation, State: "running", Custody: "target"}}}},
	}
}

// A manual reverse is built from a FRESH view of the original transaction:
// the current attempt, its observed producer and running SHA, and the
// record's recorded previous image. Unknown ownership or a missing authority
// incarnation refuses before any rollback request exists.
// origin: hot-update S8 plan §4.1 / §8.1 / M12 / §11.2 P19
func TestReverseUsesTheFreshCurrentAttempt(t *testing.T) {
	now := time.Unix(100, 0)
	view := reverseView()
	fence, reverse, err := ReverseFor(view, "SHA256:actor", now)
	if err != nil {
		t.Fatal(err)
	}
	g := view.Historical.Grant
	if fence.Current != g.Handle || fence.Owner != view.Observation.Census.Stamp.Producer || fence.RunningSHA != g.TargetSHA ||
		fence.PreviousSHA != g.SourceSHA || fence.Incarnation != strings.Repeat("a", 64) {
		t.Fatalf("fence does not name the current owner: %+v", fence)
	}
	if reverse.TargetSHA != g.SourceSHA || reverse.HistoricalTxID != g.Handle.TxID || reverse.ActorFP != "SHA256:actor" ||
		!hexValue(reverse.ClientNonce, 32) || reverse.ExpiresAt <= now.UnixNano() || len(reverse.Scope.Participants) != 1 {
		t.Fatalf("reverse request is not bound to the recorded previous image: %+v", reverse)
	}
	for name, mutate := range map[string]func(*upgradetx.View){
		"owner_unknown":    func(v *upgradetx.View) { v.Observation.OwnerKnown = false },
		"stale_revision":   func(v *upgradetx.View) { v.Observation.Current.Handle.Revision-- },
		"other_tx":         func(v *upgradetx.View) { v.Observation.Current.Handle.TxID = strings.Repeat("9", 32) },
		"no_recorded_prev": func(v *upgradetx.View) { v.Historical.RecordedPreviousSHA = "" },
		"no_incarnation":   func(v *upgradetx.View) { v.Observation.Census.Resources = v.Observation.Census.Resources[1:] },
		"two_incarnations": func(v *upgradetx.View) {
			v.Observation.Census.Resources = append(v.Observation.Census.Resources, v.Observation.Census.Resources[0])
		},
		"malformed_authority": func(v *upgradetx.View) { v.Observation.Census.Resources[0].ID = "not-a-digest" },
	} {
		v := reverseView()
		mutate(&v)
		if _, _, err := ReverseFor(v, "SHA256:actor", now); err == nil {
			t.Fatalf("%s: reverse built from a view without current proof", name)
		}
	}
	broker := reverseView()
	key := upgradetx.Key{DomainID: broker.Historical.Key.DomainID, Component: upgradetx.ComponentBroker, ConfigPath: "/etc/tether/broker.yaml"}
	broker.Historical.Key, broker.Historical.Grant.Handle.Key, broker.Observation.Current.Handle.Key = key, key, key
	broker.Observation.Census.Resources = nil
	if fence, _, err := ReverseFor(broker, "SHA256:admin", now); err != nil || fence.Incarnation != "" {
		t.Fatalf("broker reverse needs no session incarnation: %+v %v", fence, err)
	}
}

// --grant-out never replaces an input or another transaction's grant.
func TestWriteGrantNeverReplacesAnotherGrant(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "grant.json")
	g := remoteGrant()
	if err := WriteGrant(path, g); err != nil {
		t.Fatal(err)
	}
	if err := WriteGrant(path, g); err != nil {
		t.Fatalf("rewriting the same grant must be idempotent: %v", err)
	}
	other := g
	other.Handle.TxID = strings.Repeat("7", 32)
	if err := WriteGrant(path, other); ExitCode(err) != 64 {
		t.Fatalf("grant-out replaced another authorization: %v", err)
	}
	if err := WriteGrant(dir, g); ExitCode(err) != 64 {
		t.Fatalf("grant-out replaced a directory: %v", err)
	}
	if err := WriteGrant("", g); ExitCode(err) != 64 {
		t.Fatal("empty grant-out accepted")
	}
}
