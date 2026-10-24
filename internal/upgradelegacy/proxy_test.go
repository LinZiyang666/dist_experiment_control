package upgradelegacy

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// This is a durable test port, not a replacement for the P02 journal adapter.
// It exercises real file/dir sync and reopen while allowing each crash window
// to be selected independently. It never calls a P00 stub or an installed CLI.
type legacyDisk struct {
	Record upgradetx.Record
	Proof  upgradetx.LegacyUpgradeProof
}

type legacyJournal struct {
	mu              sync.Mutex
	path            string
	failIntent      bool
	failResult      bool
	failAfterIntent bool
	failAfterResult bool
	falseReceipt    bool
	loadErr         error
	alterReceipt    func(*upgradetx.Record)
	afterIntent     func()
}

func (journal *legacyJournal) read() (legacyDisk, error) {
	var disk legacyDisk
	data, err := os.ReadFile(journal.path)
	if err != nil {
		return disk, err
	}
	err = json.Unmarshal(data, &disk)
	return disk, err
}

func (journal *legacyJournal) write(disk legacyDisk) (err error) {
	data, err := json.Marshal(disk)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(journal.path), "legacy-journal-*")
	if err != nil {
		return err
	}
	defer func() {
		if removeErr := os.Remove(file.Name()); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	}()
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), journal.path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(journal.path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (journal *legacyJournal) Load(ctx context.Context, key upgradetx.Key, txID string) (upgradetx.Record, upgradetx.LegacyUpgradeProof, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return upgradetx.Record{}, upgradetx.LegacyUpgradeProof{}, err
	}
	if journal.loadErr != nil {
		return upgradetx.Record{}, upgradetx.LegacyUpgradeProof{}, journal.loadErr
	}
	disk, err := journal.read()
	if err != nil {
		return disk.Record, disk.Proof, err
	}
	if disk.Record.Key != key || disk.Record.Grant.Handle.TxID != txID {
		return upgradetx.Record{}, upgradetx.LegacyUpgradeProof{}, &upgradetx.Error{Code: upgradetx.CodeGrantUnknown}
	}
	return disk.Record, disk.Proof, nil
}

func (journal *legacyJournal) Commit(ctx context.Context, expected upgradetx.Handle, action upgradetx.Action,
	proof upgradetx.LegacyUpgradeProof) (upgradetx.Record, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return upgradetx.Record{}, err
	}
	disk, err := journal.read()
	if err != nil {
		return disk.Record, err
	}
	if disk.Record.Forward.Handle != expected {
		return disk.Record, errors.New("test journal CAS conflict")
	}
	intent := action.State == upgradetx.ActionIntent
	if (intent && journal.failIntent) || (!intent && journal.failResult) {
		return disk.Record, errors.New("test journal pre-sync failure")
	}
	if journal.falseReceipt {
		return disk.Record, nil
	}
	if intent {
		if len(disk.Record.Actions) != 0 {
			return disk.Record, errors.New("test journal intent already exists")
		}
		disk.Record.Actions = []upgradetx.Action{action}
	} else {
		if len(disk.Record.Actions) != 1 || disk.Record.Actions[0].ID != action.ID ||
			disk.Record.Actions[0].IntentDigest != action.IntentDigest {
			return disk.Record, errors.New("test journal observed identity changed")
		}
		disk.Record.Actions[0] = action
	}
	disk.Proof = proof
	disk.Record.Revision++
	disk.Record.Forward.Handle.Revision = disk.Record.Revision
	if err := journal.write(disk); err != nil {
		return disk.Record, err
	}
	if intent && journal.afterIntent != nil {
		journal.afterIntent()
	}
	if (intent && journal.failAfterIntent) || (!intent && journal.failAfterResult) {
		return disk.Record, errors.New("test journal lost sync receipt")
	}
	if journal.alterReceipt != nil {
		journal.alterReceipt(&disk.Record)
	}
	return disk.Record, nil
}

type legacyPolicy struct {
	mu            sync.Mutex
	authorization Authorization
	called        int
	err           error
}

func (policy *legacyPolicy) Authorize(ctx context.Context, _ upgradetx.Grant, _ string) (Authorization, error) {
	policy.mu.Lock()
	defer policy.mu.Unlock()
	policy.called++
	if err := ctx.Err(); err != nil {
		return Authorization{}, err
	}
	return policy.authorization, policy.err
}

type legacyTransport struct {
	mu        sync.Mutex
	sends     int
	attempts  int
	queries   int
	routes    []upgradetx.Owner
	lostReply bool
	queryErr  bool
	causal    bool
	alter     func(*upgradetx.LegacyUpgradeProof)
	before    func(upgradetx.LegacyUpgradeProof) error
}

func (transport *legacyTransport) call(ctx context.Context, proof upgradetx.LegacyUpgradeProof, nonce string, dispatch bool) (upgradetx.LegacyUpgradeProof, error) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if dispatch {
		transport.attempts++
	}
	if err := ctx.Err(); err != nil {
		return proof, err
	}
	if dispatch {
		if transport.before != nil {
			if err := transport.before(proof); err != nil {
				return proof, err
			}
		}
		transport.sends++
	} else {
		transport.queries++
	}
	transport.routes = append(transport.routes, proof.Issuer)
	if (dispatch && transport.lostReply) || (!dispatch && transport.queryErr) {
		return proof, errors.New("test transport reply lost")
	}
	proof.State, proof.ObservedSHA, proof.ObservedNonce = "observed", proof.CandidateSHA, nonce
	// The observer sees an old marker from an unrelated upgrade in the same
	// time window. Even its matching SHA and nonempty ID cannot claim our cut.
	proof.LegacyUpgradeID, proof.Causal = "unrelated-old-upgrade", transport.causal
	if transport.alter != nil {
		transport.alter(&proof)
	}
	return proof, nil
}

func (transport *legacyTransport) Dispatch(ctx context.Context, proof upgradetx.LegacyUpgradeProof, nonce string) (upgradetx.LegacyUpgradeProof, error) {
	return transport.call(ctx, proof, nonce, true)
}

func (transport *legacyTransport) Query(ctx context.Context, proof upgradetx.LegacyUpgradeProof, nonce string) (upgradetx.LegacyUpgradeProof, error) {
	return transport.call(ctx, proof, nonce, false)
}

func legacyFixture(t *testing.T) (*Proxy, *legacyJournal, *legacyPolicy, *legacyTransport, upgradetx.Grant) {
	t.Helper()
	key := upgradetx.Key{DomainID: "canonical-domain", Component: upgradetx.ComponentAgent,
		SID: "original-session", ConfiguredNID: "status", UID: 1000}
	grant := upgradetx.Grant{
		Handle:     upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 1, Revision: 1},
		ParentTxID: strings.Repeat("3", 32), ParentSlot: "host-agent", ClientNonce: strings.Repeat("4", 32),
		Issuer:  upgradetx.Owner{BootID: "issuer-boot", PID: 123, StartTicks: 456, InstanceID: "issuer-instance"},
		ActorFP: "original-actor", Mode: upgradetx.ModeLegacyInterrupting, SourceRelease: "v0.6.0", TargetRelease: "s8-candidate",
		SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64),
		ScopeDigest: strings.Repeat("c", 64), CapabilityDigest: strings.Repeat("d", 64), ExpiresAt: time.Now().Add(5 * time.Minute).UnixNano(),
	}
	journal := &legacyJournal{path: filepath.Join(t.TempDir(), "issuer-journal.json")}
	record := upgradetx.Record{Version: upgradetx.RecordVersion, Key: key, Revision: 1, Grant: grant,
		Forward: upgradetx.Attempt{Handle: grant.Handle, Direction: upgradetx.DirectionForward, SourceSHA: grant.SourceSHA,
			TargetSHA: grant.TargetSHA, Phase: upgradetx.PhasePrepared, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending}}
	if err := journal.write(legacyDisk{Record: record}); err != nil {
		t.Fatal(err)
	}
	policy := &legacyPolicy{authorization: Authorization{
		Grant: grant, TargetInstance: "legacy-instance",
		Consumers: []Consumer{{Owner: grant.Issuer, Capable: true}}, Complete: true, Reserved: true,
	}}
	transport := &legacyTransport{}
	transport.before = func(proof upgradetx.LegacyUpgradeProof) error {
		disk, err := journal.read()
		if err != nil {
			return err
		}
		if disk.Proof != proof || len(disk.Record.Actions) != 1 || disk.Record.Actions[0].State != upgradetx.ActionIntent {
			t.Error("send preceded durable intent")
			return errors.New("send preceded durable intent")
		}
		return nil
	}
	return New(journal, policy, transport), journal, policy, transport, grant
}

func requireLegacyError(t *testing.T, err error, code string, accepted bool) {
	t.Helper()
	var classified *upgradetx.Error
	if !errors.As(err, &classified) || classified.Code != code || classified.Accepted != accepted {
		t.Fatalf("error = %v; want %s accepted=%t", err, code, accepted)
	}
}

// origin: hot-update S8 plan §3.2 / §7.2 / P12
func TestLegacyDispatchNeverResendsAfterLostReply(t *testing.T) {
	proxy, journal, policy, transport, grant := legacyFixture(t)
	transport.lostReply = true
	proof, err := proxy.Dispatch(t.Context(), grant, "legacy-instance")
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	if proof.State != "unknown" || proof.Causal || transport.sends != 1 {
		t.Fatalf("lost reply = %+v; sends=%d", proof, transport.sends)
	}
	// Reconstruct both proxy and journal object, retaining only durable bytes.
	reopened := &legacyJournal{path: journal.path}
	proxy = New(reopened, policy, transport)
	proof, err = proxy.Dispatch(t.Context(), grant, "legacy-instance")
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	if transport.sends != 1 || transport.queries != 1 || proof.Causal || proof.State != "observed" {
		t.Fatalf("retry claimed/repeated dispatch: sends=%d queries=%d proof=%+v", transport.sends, transport.queries, proof)
	}
	if policy.called != 1 {
		t.Fatalf("retry recomputed its route: policy calls=%d", policy.called)
	}
}

// origin: hot-update S8 plan §3.2 four crash windows / P12
func TestLegacyDispatchCrashWindows(t *testing.T) {
	for _, window := range []string{"before_intent", "after_intent", "after_effect", "after_receipt"} {
		t.Run(window, func(t *testing.T) { exerciseLegacyCrashWindow(t, window) })
	}
}

func exerciseLegacyCrashWindow(t *testing.T, window string) {
	t.Helper()
	proxy, journal, policy, transport, grant := legacyFixture(t)
	switch window {
	case "before_intent":
		journal.failIntent = true
	case "after_intent":
		journal.failAfterIntent = true
	case "after_effect":
		transport.lostReply = true
	case "after_receipt":
		journal.failAfterResult = true
	}
	_, err := proxy.Dispatch(t.Context(), grant, "legacy-instance")
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	firstSends := transport.sends
	wantFirstSends := 1
	if window == "before_intent" || window == "after_intent" {
		wantFirstSends = 0
	}
	if firstSends != wantFirstSends {
		t.Fatalf("crash window %s did not hit its effect boundary: sends=%d want=%d", window, firstSends, wantFirstSends)
	}
	proxy = New(&legacyJournal{path: journal.path}, policy, transport)
	proof, err := proxy.Dispatch(t.Context(), grant, "legacy-instance")
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	if window == "before_intent" {
		if firstSends != 0 || transport.sends != 1 {
			t.Fatalf("unsent grant did not start once: before=%d after=%d", firstSends, transport.sends)
		}
	} else if transport.sends != firstSends {
		t.Fatalf("crash at %s repeated effect: before=%d after=%d", window, firstSends, transport.sends)
	}
	if proof.Causal {
		t.Fatal("a time-adjacent matching image claimed the unrelated old UpgradeID")
	}
}

// origin: hot-update S8 plan §7.2 / P12
func TestLegacyDispatchRequiresBoundFleetAuthorization(t *testing.T) {
	cases := []struct {
		name string
		edit func(*Authorization)
		code string
	}{
		{"issuer", func(a *Authorization) { a.Grant.Issuer.PID++ }, upgradetx.CodeRejected},
		{"target_key", func(a *Authorization) { a.Grant.Handle.Key.UID++ }, upgradetx.CodeRejected},
		{"target_instance", func(a *Authorization) { a.TargetInstance = "another-instance" }, upgradetx.CodeRejected},
		{"parent", func(a *Authorization) { a.Grant.ParentTxID = strings.Repeat("5", 32) }, upgradetx.CodeRejected},
		{"slot", func(a *Authorization) { a.Grant.ParentSlot = "another-slot" }, upgradetx.CodeRejected},
		{"actor", func(a *Authorization) { a.Grant.ActorFP = "another-actor" }, upgradetx.CodeRejected},
		{"nonce", func(a *Authorization) { a.Grant.ClientNonce = strings.Repeat("5", 32) }, upgradetx.CodeRejected},
		{"source", func(a *Authorization) { a.Grant.SourceSHA = strings.Repeat("e", 64) }, upgradetx.CodeRejected},
		{"candidate", func(a *Authorization) { a.Grant.TargetSHA = strings.Repeat("e", 64) }, upgradetx.CodeRejected},
		{"scope", func(a *Authorization) { a.Grant.ScopeDigest = strings.Repeat("e", 64) }, upgradetx.CodeRejected},
		{"caps", func(a *Authorization) { a.Grant.CapabilityDigest = strings.Repeat("e", 64) }, upgradetx.CodeRejected},
		{"expiry", func(a *Authorization) { a.Grant.ExpiresAt++ }, upgradetx.CodeRejected},
		{"unknown", func(a *Authorization) { a.Complete = false }, upgradetx.CodeRejected},
		{"empty", func(a *Authorization) { a.Consumers = nil }, upgradetx.CodeRejected},
		{"oversized", func(a *Authorization) { a.Consumers = make([]Consumer, 4097) }, upgradetx.CodeRejected},
		{"mixed_nonvoter", func(a *Authorization) {
			other := a.Grant.Issuer
			other.PID++
			a.Consumers = append(a.Consumers, Consumer{Owner: other})
		}, upgradetx.CodeRejected},
		{"unknown_identity", func(a *Authorization) { a.Consumers[0].Owner.StartTicks = 0 }, upgradetx.CodeRejected},
		{"duplicate", func(a *Authorization) { a.Consumers = append(a.Consumers, a.Consumers[0]) }, upgradetx.CodeRejected},
		{"missing_issuer", func(a *Authorization) { a.Consumers[0].Owner.PID++ }, upgradetx.CodeDeferred},
		{"unreserved", func(a *Authorization) { a.Reserved = false }, upgradetx.CodeDeferred},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			proxy, journal, policy, transport, grant := legacyFixture(t)
			test.edit(&policy.authorization)
			requireLegacyError(t, proxy.Preflight(t.Context(), grant, "legacy-instance"), test.code, false)
			_, err := proxy.Dispatch(t.Context(), grant, "legacy-instance")
			requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
			disk, err := journal.read()
			if err != nil || len(disk.Record.Actions) != 0 || transport.sends != 0 {
				t.Fatalf("policy refusal wrote/sent: sends=%d disk=%+v err=%v", transport.sends, disk, err)
			}
		})
	}
}

// origin: hot-update S8 plan §7.2 issuer reboot/route change / P12
func TestLegacyQueryUsesOriginalIssuerAfterRestart(t *testing.T) {
	proxy, journal, policy, transport, grant := legacyFixture(t)
	// Give the accepted fixture its original short deadline before dispatch;
	// recovery must not rewrite or extend that immutable grant.
	grant.ExpiresAt = time.Now().Add(100 * time.Millisecond).UnixNano()
	policy.authorization.Grant = grant
	disk, err := journal.read()
	if err != nil {
		t.Fatal(err)
	}
	disk.Record.Grant = grant
	if err := journal.write(disk); err != nil {
		t.Fatal(err)
	}
	transport.lostReply = true
	_, err = proxy.Dispatch(t.Context(), grant, "legacy-instance")
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	// A fresh leader would select another issuer. Existing proof still routes
	// to the original journal, even when the accepted grant has since expired.
	policy.authorization.Grant.Issuer.PID++
	policy.authorization.Grant.Issuer.InstanceID = "new-leader"
	// sleep-fixture: expire the original accepted grant, not a readiness barrier.
	time.Sleep(time.Until(time.Unix(0, grant.ExpiresAt)) + time.Millisecond)
	proxy = New(&legacyJournal{path: journal.path}, policy, transport)
	proof, err := proxy.Query(t.Context(), grant, "legacy-instance", strings.Repeat("6", 32))
	if err != nil || proof.Issuer != grant.Issuer || proof.ObservedNonce != strings.Repeat("6", 32) {
		t.Fatalf("restart query = %+v, %v", proof, err)
	}
	for _, route := range transport.routes {
		if route != grant.Issuer {
			t.Fatalf("query changed issuer: %+v", route)
		}
	}
	transport.queryErr = true
	_, err = proxy.Dispatch(t.Context(), grant, "legacy-instance")
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	if transport.sends != 1 || policy.called != 1 {
		t.Fatalf("unavailable old issuer triggered new execution: sends=%d policy=%d", transport.sends, policy.called)
	}
}

// origin: hot-update S8 plan §3.2 / §7.1 / P12
func TestLegacyDispatchFailsClosedOnJournalErrors(t *testing.T) {
	for _, mode := range []string{"false_receipt", "result_sync", "late_current", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			proxy, journal, _, transport, grant := legacyFixture(t)
			journal.falseReceipt = mode == "false_receipt"
			journal.failResult = mode == "result_sync"
			transport.causal = mode == "result_sync"
			if mode == "corrupt" {
				if err := os.WriteFile(journal.path, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "late_current" {
				transport.alter = func(_ *upgradetx.LegacyUpgradeProof) {
					disk, err := journal.read()
					if err != nil {
						t.Fatal(err)
					}
					disk.Record.Forward.Handle.TxID = strings.Repeat("7", 32)
					disk.Record.Grant.Handle.TxID = strings.Repeat("7", 32)
					if err := journal.write(disk); err != nil {
						t.Fatal(err)
					}
				}
			}
			proof, err := proxy.Dispatch(t.Context(), grant, "legacy-instance")
			if mode == "corrupt" {
				requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
				if transport.sends != 0 {
					t.Fatalf("corrupt journal sent: %v", err)
				}
				return
			}
			requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
			if proof.Causal || (mode == "false_receipt" && transport.sends != 0) {
				t.Fatalf("false durable success: %+v sends=%d", proof, transport.sends)
			}
			if mode == "result_sync" && proof.State != "unknown" {
				t.Fatalf("failed result sync returned a durable observation: %+v", proof)
			}
			if mode == "late_current" {
				disk, err := journal.read()
				if err != nil || disk.Record.Grant.Handle.TxID != strings.Repeat("7", 32) || disk.Proof.State != "attempted" {
					t.Fatalf("late result rewrote next Current: %+v %v", disk, err)
				}
			}
		})
	}
}

// origin: hot-update S8 plan §2 / §3.1 / §7.2 / P12
func TestLegacyProxyRejectsUnacceptedGrant(t *testing.T) {
	for _, mode := range []string{"unknown", "hot", "default", "expired", "long_deadline", "empty_parent", "changed_actor", "cancelled_transaction"} {
		t.Run(mode, func(t *testing.T) {
			proxy, journal, _, transport, grant := legacyFixture(t)
			switch mode {
			case "unknown":
				grant.Handle.TxID = strings.Repeat("8", 32)
			case "hot":
				grant.Mode = upgradetx.ModeHot
			case "default":
				grant.Mode = ""
			case "expired":
				grant.ExpiresAt = time.Now().Add(-time.Minute).UnixNano()
			case "long_deadline":
				grant.ExpiresAt = time.Now().Add(time.Hour).UnixNano()
			case "empty_parent":
				grant.ParentTxID = ""
			case "changed_actor":
				grant.ActorFP = "other-actor"
			case "cancelled_transaction":
				disk, err := journal.read()
				if err != nil {
					t.Fatal(err)
				}
				disk.Record.Forward.Phase, disk.Record.Forward.Outcome = upgradetx.PhaseCancelled, upgradetx.OutcomeCancelled
				if err := journal.write(disk); err != nil {
					t.Fatal(err)
				}
			}
			_, err := proxy.Dispatch(t.Context(), grant, "legacy-instance")
			if err == nil || transport.sends != 0 {
				t.Fatalf("invalid grant %s sent: %v", mode, err)
			}
			disk, readErr := journal.read()
			if readErr != nil || len(disk.Record.Actions) != 0 {
				t.Fatalf("invalid grant created intent: %+v %v", disk, readErr)
			}
		})
	}
	proxy := New(nil, nil, nil)
	requireLegacyError(t, proxy.Preflight(t.Context(), upgradetx.Grant{}, ""), upgradetx.CodeNotWired, false)
	proxy, _, _, transport, grant := legacyFixture(t)
	_, err := proxy.Query(t.Context(), grant, "legacy-instance", strings.Repeat("e", 32))
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	if transport.queries != 0 || transport.sends != 0 {
		t.Fatal("query created a legacy attempt")
	}
}

// origin: hot-update S8 plan §11.2 proxy five rounds / §12.2 / P12
func TestLegacyProxyLifecycleNoLeaks(t *testing.T) {
	beforeGoroutines, beforeFDs := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLegacyProxyLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "legacy proxy lifecycle", beforeGoroutines)
	testharness.AssertNoFDLeak(t, "legacy proxy lifecycle", beforeFDs, 0)
}

func exerciseLegacyProxyLifecycle(t *testing.T) {
	t.Helper()
	for _, window := range []string{"before_intent", "after_intent", "after_effect", "after_receipt"} {
		exerciseLegacyCrashWindow(t, window)
	}
	proxy, _, _, transport, grant := legacyFixture(t)
	// Competing request handlers load the same original revision. Only the
	// winning durable intent can issue a send; every goroutine is joined.
	var joined sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 8; worker++ {
		joined.Go(func() {
			<-start
			_, err := proxy.Dispatch(t.Context(), grant, "legacy-instance")
			var classified *upgradetx.Error
			if !errors.As(err, &classified) || classified.Code != upgradetx.CodeUnsettled || !classified.Accepted {
				t.Errorf("concurrent dispatch = %v", err)
			}
		})
	}
	close(start)
	joined.Wait()
	if transport.sends != 1 {
		t.Fatalf("concurrent send count=%d", transport.sends)
	}
	// Cancel between intent durability and the first possible transport effect.
	proxy, journal, _, transport, grant := legacyFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	journal.afterIntent = cancel
	_, err := proxy.Dispatch(ctx, grant, "legacy-instance")
	cancel()
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	journal.afterIntent = nil
	_, err = proxy.Dispatch(t.Context(), grant, "legacy-instance")
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	if transport.sends != 0 || transport.queries != 1 || transport.attempts != 0 {
		t.Fatalf("cancelled intent was resent: sends=%d queries=%d", transport.sends, transport.queries)
	}
}
