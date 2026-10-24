package upgradecli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradetx/contractstub"
)

// queryEndpoint replaces only Query. All mutation methods remain P00's
// fail-closed port, so accidental Start/Prepare cannot silently succeed.
type queryEndpoint struct {
	contractstub.AgentEndpoint
	query func(context.Context, upgradetx.Key, string, string) (upgradetx.View, error)
}

func (e queryEndpoint) Query(ctx context.Context, key upgradetx.Key, txID, nonce string) (upgradetx.View, error) {
	return e.query(ctx, key, txID, nonce)
}

func transactionView() upgradetx.View {
	key := upgradetx.Key{DomainID: "installation", Component: upgradetx.ComponentAgent, SID: "session", ConfiguredNID: "status"}
	handle := upgradetx.Handle{Key: key, TxID: strings.Repeat("a", 32), CutID: strings.Repeat("b", 32), Generation: 2, Revision: 3}
	attempt := upgradetx.Attempt{Handle: handle, Direction: upgradetx.DirectionForward, Phase: upgradetx.PhaseMigrating, Outcome: upgradetx.OutcomePending, CutStage: upgradetx.CutActive, TargetSHA: strings.Repeat("c", 64)}
	grant := upgradetx.Grant{Handle: handle, SourceSHA: strings.Repeat("d", 64), TargetSHA: attempt.TargetSHA, Mode: upgradetx.ModeHot}
	return upgradetx.View{
		Historical:  upgradetx.Record{Key: key, Grant: grant, Forward: attempt},
		Observation: upgradetx.Observation{At: 1, OwnerKnown: true, Current: attempt, RunningSHA: attempt.TargetSHA},
	}
}

func viewClient(view upgradetx.View) Client {
	return Client{Endpoint: queryEndpoint{query: func(_ context.Context, _ upgradetx.Key, _ string, nonce string) (upgradetx.View, error) {
		view.Observation.Nonce = nonce
		return view, nil
	}}, PollInterval: time.Millisecond}
}

func queryIdentity(view upgradetx.View) (upgradetx.Key, string) {
	return view.Historical.Key, view.Historical.Grant.Handle.TxID
}

// origin: hot-update S8 plan §7.1 / §11.2 P18 — queryunknown nonzero mutant.
func TestUpgradeQueryUnknownIsSuccessful(t *testing.T) {
	view := transactionView()
	view.Observation.OwnerKnown = false
	view.Observation.Current.Outcome = upgradetx.OutcomeUnknown
	view.Code = upgradetx.CodeUnsettled
	client := viewClient(view)
	key, txID := queryIdentity(view)
	ctx := context.Background() // ctx-root: synchronous CLI policy test.
	got, err := client.Query(ctx, key, txID)
	if err != nil || ExitCode(err) != 0 || got.Observation.OwnerKnown || got.Observation.Current.Outcome != upgradetx.OutcomeUnknown || got.Code != view.Code {
		t.Fatalf("unknown query lost its readable state: view=%+v err=%v rc=%d", got, err, ExitCode(err))
	}
}

// origin: hot-update S8 plan §7.1 / §11.2 P18 — waitunknown zero mutant.
func TestUpgradeWaitUnknownIsUnsettled(t *testing.T) {
	for _, defect := range []string{"owner", "outcome", "current_tx", "current_key", "phase", "historical_completed"} {
		t.Run(defect, func(t *testing.T) {
			view := transactionView()
			switch defect {
			case "owner":
				view.Observation.OwnerKnown = false
			case "outcome":
				view.Observation.Current.Outcome = upgradetx.OutcomeUnknown
			case "current_tx":
				view.Observation.Current.Handle.TxID = strings.Repeat("e", 32)
			case "current_key":
				view.Observation.Current.Handle.Key.ConfiguredNID = "another"
			case "phase":
				view.Observation.Current.Phase = "future_phase"
			case "historical_completed":
				view.Historical.Forward.Phase = upgradetx.PhaseCompleted
				view.Historical.Forward.Outcome = upgradetx.OutcomeSuccess
				view.Observation.OwnerKnown = false
			}
			key, txID := queryIdentity(view)
			ctx := context.Background() // ctx-root: synchronous unknown wait test.
			got, err := viewClient(view).Wait(ctx, key, txID, time.Second, false)
			if ExitCode(err) != 74 || !reflect.DeepEqual(got.Observation.Current, view.Observation.Current) {
				t.Fatalf("unknown wait must keep current facts and return 74: rc=%d err=%v", ExitCode(err), err)
			}
		})
	}
}

// origin: hot-update S8 plan §7.1 / M15 / P18.
func TestUpgradeQueryRejectsStaleOrMisroutedObservation(t *testing.T) {
	for _, defect := range []string{"nonce", "time", "key", "grant_key", "tx"} {
		t.Run(defect, func(t *testing.T) {
			view := transactionView()
			key, txID := queryIdentity(view)
			client := Client{Endpoint: queryEndpoint{query: func(_ context.Context, _ upgradetx.Key, _ string, nonce string) (upgradetx.View, error) {
				view.Observation.Nonce = nonce
				switch defect {
				case "nonce":
					view.Observation.Nonce = "cached"
				case "time":
					view.Observation.At = 0
				case "key":
					view.Historical.Key.ConfiguredNID = "elsewhere"
				case "tx":
					view.Historical.Grant.Handle.TxID = strings.Repeat("e", 32)
				case "grant_key":
					view.Historical.Grant.Handle.Key.ConfiguredNID = "another"
				}
				return view, nil
			}}}
			ctx := context.Background() // ctx-root: fresh observation boundary test.
			if _, err := client.Query(ctx, key, txID); ExitCode(err) != 74 {
				t.Fatalf("%s response accepted: %v", defect, err)
			}
		})
	}
}

// origin: hot-update S8 plan §7.1 / P18 — local failures cannot reach a
// transport or turn its classified refusal into successful empty output.
func TestUpgradeClientBoundaryRefusals(t *testing.T) {
	view := transactionView()
	key, txID := queryIdentity(view)
	ctx := context.Background() // ctx-root: synchronous CLI boundary fixture.
	if _, err := (Client{}).Query(ctx, key, txID); ExitCode(err) != 74 {
		t.Fatal("unwired query accepted")
	}
	client := Client{Endpoint: queryEndpoint{query: func(context.Context, upgradetx.Key, string, string) (upgradetx.View, error) {
		t.Fatal("invalid local request reached transport")
		return view, nil
	}}}
	if _, err := client.Query(ctx, key, "invalid"); ExitCode(err) != 64 {
		t.Fatal("invalid query transaction accepted")
	}
	for _, timeout := range []time.Duration{0, -time.Second} {
		if _, err := client.Wait(ctx, key, txID, timeout, false); ExitCode(err) != 64 {
			t.Fatal("invalid wait timeout accepted")
		}
	}
	client.PollInterval = -time.Second
	if _, err := client.Wait(ctx, key, txID, time.Second, false); ExitCode(err) != 64 {
		t.Fatal("negative poll interval accepted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := client.Query(cancelled, key, txID); !errors.Is(err, context.Canceled) {
		t.Fatalf("query ignored cancellation: %v", err)
	}
	sentinel := &upgradetx.Error{Code: "not_owner"}
	client.Endpoint = queryEndpoint{query: func(context.Context, upgradetx.Key, string, string) (upgradetx.View, error) { return view, sentinel }}
	if _, err := client.Query(ctx, key, txID); err != sentinel {
		t.Fatal("query lost endpoint refusal")
	}
}

// origin: hot-update S8 plan §11.2 P18 — cancellation owns the whole wait,
// including an in-flight endpoint call, without a background polling loop.
func TestUpgradeWaitCancellationStopsQueries(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseWaitCancellation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "upgrade wait cancellation", beforeG)
	testharness.AssertNoFDLeak(t, "upgrade wait cancellation", beforeFD, 0)
}

func exerciseWaitCancellation(t *testing.T) {
	t.Helper()
	view := transactionView()
	key, txID := queryIdentity(view)
	for _, inFlight := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond) // ctx-root: caller cancellation exercise.
		calls := 0
		client := Client{PollInterval: time.Hour, Endpoint: queryEndpoint{query: func(ctx context.Context, _ upgradetx.Key, _ string, nonce string) (upgradetx.View, error) {
			calls++
			if inFlight {
				cancel()
				<-ctx.Done()
				return view, ctx.Err()
			}
			view.Observation.Nonce = nonce
			return view, nil
		}}}
		_, err := client.Wait(ctx, key, txID, time.Second, false)
		cancel()
		if ExitCode(err) != 74 || calls != 1 {
			t.Fatalf("cancelled wait kept polling: calls=%d rc=%d err=%v", calls, ExitCode(err), err)
		}
	}
}

// origin: hot-update S8 plan §7.1 / P18 — query timeout cannot start a
// second timeout window or turn an accepted failure into retryable refusal.
func TestUpgradeWaitDeadlineIncludesTransportAndBackoff(t *testing.T) {
	view := transactionView()
	key, txID := queryIdentity(view)
	for _, blockTransport := range []bool{false, true} {
		calls := 0
		client := Client{PollInterval: time.Hour, Endpoint: queryEndpoint{query: func(ctx context.Context, _ upgradetx.Key, _ string, nonce string) (upgradetx.View, error) {
			calls++
			if blockTransport {
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("transport has no absolute wait deadline")
				}
				<-ctx.Done()
				return view, ctx.Err()
			}
			view.Observation.Nonce = nonce
			return view, nil
		}}}
		ctx := context.Background() // ctx-root: absolute deadline exercise.
		if _, err := client.Wait(ctx, key, txID, 10*time.Millisecond, false); ExitCode(err) != 74 || calls != 1 {
			t.Fatalf("timeout did not settle as unknown: calls=%d err=%v", calls, err)
		}
	}
}

// origin: hot-update S8 plan §4.2 / §7.1 / P18 — test only CLI delegation,
// preserving the exact proof for P01 rather than implementing a second validator.
func TestUpgradeWaitRequiresFreshCompletionProof(t *testing.T) {
	view := transactionView()
	view.Observation.Current.Phase = upgradetx.PhaseCompleted
	view.Observation.Current.Outcome = upgradetx.OutcomeSuccess
	key, txID := queryIdentity(view)
	ctx := context.Background() // ctx-root: completion validator delegation test.
	client := viewClient(view)
	if _, err := client.Wait(ctx, key, txID, time.Second, false); ExitCode(err) != 74 {
		t.Fatalf("unwired validator accepted completion: %v", err)
	}
	calls := 0
	client.Completed = func(attempt upgradetx.Attempt, observation upgradetx.Observation) error {
		calls++
		if !reflect.DeepEqual(attempt, view.Observation.Current) || observation.Nonce == "" || observation.RunningSHA != view.Observation.RunningSHA {
			t.Fatal("completion validator received stale or projected proof")
		}
		return errors.New("one domain remains unknown")
	}
	if _, err := client.Wait(ctx, key, txID, time.Second, false); ExitCode(err) != 74 || calls != 1 {
		t.Fatalf("validator refusal bypassed: calls=%d err=%v", calls, err)
	}
	client.Completed = func(attempt upgradetx.Attempt, observation upgradetx.Observation) error {
		calls++
		if !reflect.DeepEqual(attempt, observation.Current) || observation.Nonce == "" {
			t.Fatal("fresh attempt was not passed to validator")
		}
		return nil
	}
	if _, err := client.Wait(ctx, key, txID, time.Second, false); err != nil || calls != 2 {
		t.Fatalf("verified completion failed: calls=%d err=%v", calls, err)
	}
}

// origin: hot-update S8 plan §7.1 / P18.
func TestUpgradeWaitTerminalExitPolicy(t *testing.T) {
	for _, row := range []struct {
		phase upgradetx.Phase
		rc    int
	}{
		{upgradetx.PhaseDeferred, 75}, {upgradetx.PhaseRejected, 64}, {upgradetx.PhaseCancelled, 64},
		{upgradetx.PhaseRecoveryRequired, 74}, {upgradetx.PhaseRollbackBlocked, 74}, {upgradetx.PhaseAbandoned, 74},
	} {
		t.Run(string(row.phase), func(t *testing.T) {
			view := transactionView()
			view.Observation.Current.Phase = row.phase
			if row.phase == upgradetx.PhaseDeferred || row.phase == upgradetx.PhaseRejected {
				view.Observation.Current.CutStage = upgradetx.CutNone
			}
			key, txID := queryIdentity(view)
			ctx := context.Background() // ctx-root: terminal wait classification test.
			if _, err := viewClient(view).Wait(ctx, key, txID, time.Second, false); ExitCode(err) != row.rc {
				t.Fatalf("got rc%d want rc%d: %v", ExitCode(err), row.rc, err)
			}
		})
	}
	view := transactionView()
	view.Observation.Current.Phase = upgradetx.PhaseRolledBack
	view.Historical.RecordedPreviousSHA = strings.Repeat("d", 64)
	view.Historical.Reverse = view.Observation.Current
	view.Historical.Reverse.Phase = upgradetx.PhaseCompleted
	view.Historical.Reverse.Direction = upgradetx.DirectionReverse
	view.Historical.Reverse.TargetSHA = view.Historical.RecordedPreviousSHA
	client := viewClient(view)
	calls := 0
	client.Completed = func(attempt upgradetx.Attempt, _ upgradetx.Observation) error {
		calls++
		if !reflect.DeepEqual(attempt, view.Historical.Reverse) {
			t.Fatal("rollback reused forward proof")
		}
		return nil
	}
	key, txID := queryIdentity(view)
	ctx := context.Background() // ctx-root: automatic versus requested rollback policy.
	if _, err := client.Wait(ctx, key, txID, time.Second, false); ExitCode(err) != 64 || calls != 1 {
		t.Fatalf("automatic rollback falsely reached upgrade goal: %v calls=%d", err, calls)
	}
	if _, err := client.Wait(ctx, key, txID, time.Second, true); err != nil || calls != 2 {
		t.Fatalf("requested rollback did not reach its goal: %v calls=%d", err, calls)
	}
	view.Historical.Reverse.TargetSHA = strings.Repeat("e", 64)
	client = viewClient(view)
	client.Completed = func(upgradetx.Attempt, upgradetx.Observation) error {
		t.Fatal("invalid rollback reached proof validator")
		return nil
	}
	if _, err := client.Wait(ctx, key, txID, time.Second, true); ExitCode(err) != 74 {
		t.Fatalf("rollback to unrecorded previous SHA accepted: %v", err)
	}
}

// origin: hot-update S8 plan §7.1 / P18 — every poll observes the same
// transaction with a new nonce; legacy observations cannot become hot success.
func TestUpgradeWaitPollsFreshOriginalTransaction(t *testing.T) {
	view := transactionView()
	key, txID := queryIdentity(view)
	seen := make(map[string]bool)
	client := Client{PollInterval: time.Millisecond, Endpoint: queryEndpoint{query: func(_ context.Context, gotKey upgradetx.Key, gotTx, nonce string) (upgradetx.View, error) {
		if gotKey != key || gotTx != txID || !hexValue(nonce, 32) || seen[nonce] {
			t.Fatal("poll changed the transaction or reused an observation nonce")
		}
		seen[nonce] = true
		view.Observation.Nonce = nonce
		if len(seen) == 3 {
			view.Observation.Current.Phase = upgradetx.PhaseCompleted
		}
		return view, nil
	}}, Completed: func(upgradetx.Attempt, upgradetx.Observation) error { return nil }}
	ctx := context.Background() // ctx-root: fresh-poll delegation exercise.
	if _, err := client.Wait(ctx, key, txID, time.Second, false); err != nil || len(seen) != 3 {
		t.Fatalf("wait did not poll the original transaction: polls=%d err=%v", len(seen), err)
	}
	for _, defect := range []string{"legacy_mode", "legacy_outcome", "active_deferred", "active_rejected", "response_code"} {
		view := transactionView()
		switch defect {
		case "legacy_mode":
			view.Historical.Grant.Mode = upgradetx.ModeLegacyInterrupting
			view.Observation.Current.Phase = upgradetx.PhaseCompleted
		case "legacy_outcome":
			view.Observation.Current.Outcome = upgradetx.OutcomeLegacyInterrupting
		case "active_deferred":
			view.Observation.Current.Phase = upgradetx.PhaseDeferred
		case "active_rejected":
			view.Observation.Current.Phase = upgradetx.PhaseRejected
		case "response_code":
			view.Code = upgradetx.CodeDeferred
		}
		client := viewClient(view)
		client.Completed = func(upgradetx.Attempt, upgradetx.Observation) error {
			t.Fatal("legacy observation reached hot proof validator")
			return nil
		}
		if _, err := client.Wait(ctx, key, txID, time.Second, false); ExitCode(err) != 74 {
			t.Fatalf("%s reported success or retry: %v", defect, err)
		}
	}
}

// origin: hot-update S8 plan §5.2 / §7.1 / P18.
func TestUpgradeOptionsKeepResumeGrantImmutable(t *testing.T) {
	sha := strings.Repeat("a", 64)
	for _, opts := range []UpgradeOptions{
		{URL: "https://example.invalid/image", SHA256: sha},
		{URL: "https://example.invalid/image", SHA256: sha, PrepareOnly: true, GrantOut: "grant.json"},
		{URL: "https://example.invalid/image", SHA256: sha, Interrupting: true},
		{TxID: strings.Repeat("a", 32), ArtifactReceipt: "receipt.json"},
	} {
		if err := ValidateUpgrade(upgradetx.ComponentAgent, opts); err != nil {
			t.Fatal(err)
		}
	}
	if err := ValidateUpgrade(upgradetx.ComponentBroker, UpgradeOptions{ToVersion: "v1", ExpectSHA256: sha}); err != nil {
		t.Fatal(err)
	}
	for _, opts := range []UpgradeOptions{
		{}, {TxID: "caller-chosen"}, {TxID: strings.Repeat("a", 32), URL: "replacement"},
		{TxID: strings.Repeat("a", 32), SHA256: sha}, {TxID: strings.Repeat("a", 32), ToVersion: "changed"},
		{TxID: strings.Repeat("a", 32), ExpectSHA256: sha}, {TxID: strings.Repeat("a", 32), PrepareOnly: true},
		{TxID: strings.Repeat("a", 32), GrantOut: "new"}, {TxID: strings.Repeat("a", 32), Interrupting: true},
		{URL: "url", SHA256: sha, ArtifactReceipt: "receipt"}, {URL: "url", SHA256: sha, GrantOut: "grant"},
		{URL: "url", SHA256: sha, PrepareOnly: true}, {URL: "url", SHA256: sha, PrepareOnly: true, GrantOut: "grant", Interrupting: true},
		{URL: "url", SHA256: strings.Repeat("A", 64)},
	} {
		if err := ValidateUpgrade(upgradetx.ComponentAgent, opts); ExitCode(err) != 64 {
			t.Fatalf("invalid flags accepted: %+v err=%v", opts, err)
		}
	}
	if err := ValidateUpgrade(upgradetx.ComponentBroker, UpgradeOptions{ToVersion: "v1", ExpectSHA256: sha, Interrupting: true}); ExitCode(err) != 64 {
		t.Fatal("broker interrupting flag accepted")
	}
	if err := ValidateUpgrade(upgradetx.ComponentNATS, UpgradeOptions{}); ExitCode(err) != 64 {
		t.Fatal("reserved S9 component accepted")
	}
}

func stageFiles(t *testing.T) (StageOptions, upgradetx.Grant, upgradeartifact.Receipt) {
	t.Helper()
	dir := t.TempDir()
	opts := StageOptions{Grant: filepath.Join(dir, "grant.json"), Source: filepath.Join(dir, "source"), Candidate: filepath.Join(dir, "candidate"), ReceiptOut: filepath.Join(dir, "receipt.json")}
	grant := transactionView().Historical.Grant
	data, err := json.Marshal(grant)
	if err != nil {
		t.Fatal(err)
	}
	for path, contents := range map[string][]byte{opts.Grant: data, opts.Source: []byte("running source"), opts.Candidate: []byte("candidate")} {
		if err := os.WriteFile(path, contents, 0600); err != nil {
			t.Fatal(err)
		}
	}
	receipt := upgradeartifact.Receipt{Grant: grant, CanonicalPath: "/product/canonical", SourcePath: "/product/source", TargetPath: "/product/target", SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA, ManifestDigest: strings.Repeat("f", 64)}
	return opts, grant, receipt
}

// origin: hot-update S8 plan §5.2 / LQ1 / P18.
func TestUpgradeStageParserRequiresExplicitFiles(t *testing.T) {
	got, err := ParseStage([]string{"--grant", "g", "--source", "s", "--candidate", "c", "--receipt-out", "r"})
	if err != nil || got != (StageOptions{Grant: "g", Source: "s", Candidate: "c", ReceiptOut: "r"}) {
		t.Fatalf("stage parse: %+v %v", got, err)
	}
	for _, args := range [][]string{nil, {"--grant", "g"}, {"--unknown", "x"}, {"--grant", "g", "--source", "s", "--candidate", "c", "--receipt-out", "r", "extra"}} {
		if _, err := ParseStage(args); ExitCode(err) != 64 {
			t.Fatalf("invalid stage args accepted: %v", args)
		}
	}
}

// origin: hot-update S8 plan §5.2 / LQ1 / P18 — no endpoint or journal
// writer is available to this command. P05 alone supplies privileged proofs.
func TestUpgradeStagePublishesOnlyArtifactReceipt(t *testing.T) {
	opts, grant, receipt := stageFiles(t)
	ctx := context.Background() // ctx-root: synchronous stage leaf fixture.
	calls := 0
	publish := func(gotCtx context.Context, gotGrant upgradetx.Grant, source, target io.Reader) (upgradeartifact.Receipt, error) {
		calls++
		if gotCtx != ctx || !reflect.DeepEqual(gotGrant, grant) {
			t.Fatal("stage changed grant or caller context")
		}
		a, errA := io.ReadAll(source)
		b, errB := io.ReadAll(target)
		if errA != nil || errB != nil || string(a) != "running source" || string(b) != "candidate" {
			t.Fatal("stage did not pass both images")
		}
		return receipt, nil
	}
	for round := 0; round < 5; round++ {
		got, err := Stage(ctx, opts, publish)
		if err != nil || !reflect.DeepEqual(got, receipt) {
			t.Fatalf("publication failed: %+v %v", got, err)
		}
		var stored upgradeartifact.Receipt
		if err := readJSON(opts.ReceiptOut, &stored); err != nil || !reflect.DeepEqual(stored, receipt) {
			t.Fatalf("receipt not persisted: %+v %v", stored, err)
		}
		info, err := os.Stat(opts.ReceiptOut)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("receipt permissions: %v %v", info, err)
		}
		entries, err := os.ReadDir(filepath.Dir(opts.Grant))
		if err != nil || len(entries) != 4 {
			t.Fatalf("stage wrote files beyond its receipt: %v %v", entries, err)
		}
	}
	if calls != 5 {
		t.Fatalf("publisher calls=%d", calls)
	}
}

// origin: hot-update S8 plan §5.2 / M16 / P18.
func TestUpgradeStageRejectsAmbiguousFiles(t *testing.T) {
	decoder := json.NewDecoder(strings.NewReader(strings.Repeat("[", 66) + "0" + strings.Repeat("]", 66)))
	if err := uniqueJSON(decoder, 0); err == nil {
		t.Fatal("authorization nesting limit ignored")
	}
	for _, defect := range []string{"unknown_field", "duplicate", "case_alias", "nested_duplicate", "trailing", "oversize", "same_input", "hardlink", "symlink", "other_grant"} {
		t.Run(defect, func(t *testing.T) {
			opts, _, receipt := stageFiles(t)
			data, err := os.ReadFile(opts.Grant)
			if err != nil {
				t.Fatal(err)
			}
			switch defect {
			case "unknown_field":
				data = append([]byte(`{"unrecognized":true,`), data[1:]...)
			case "duplicate":
				data = append([]byte(`{"mode":"hot",`), data[1:]...)
			case "case_alias":
				data = append([]byte(`{"MODE":"hot",`), data[1:]...)
			case "nested_duplicate":
				data = []byte(strings.Replace(string(data), `"key":{`, `"key":{"domain_id":"wrong",`, 1))
			case "trailing":
				data = append(data, []byte(` {}`)...)
			case "oversize":
				data = append(data, []byte(strings.Repeat(" ", (1<<20)+1))...)
			case "same_input":
				opts.ReceiptOut = opts.Grant
			case "hardlink":
				if err := os.Link(opts.Grant, opts.ReceiptOut); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(opts.Grant, opts.ReceiptOut); err != nil {
					t.Fatal(err)
				}
			case "other_grant":
				receipt.Grant.Handle.TxID = strings.Repeat("e", 32)
				old, err := json.Marshal(receipt)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(opts.ReceiptOut, old, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(opts.Grant, data, 0600); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background() // ctx-root: grant/file ambiguity exercise.
			_, err = Stage(ctx, opts, func(context.Context, upgradetx.Grant, io.Reader, io.Reader) (upgradeartifact.Receipt, error) {
				t.Fatal("ambiguous files reached privileged publication")
				return receipt, nil
			})
			if ExitCode(err) != 64 {
				t.Fatalf("ambiguous stage accepted: %v", err)
			}
		})
	}
}

// origin: hot-update S8 plan §5.2 / P18 — neither failed publication nor
// a forged publisher result can destroy the original authorization evidence.
func TestUpgradeStageFailurePreservesEvidence(t *testing.T) {
	for _, defect := range []string{"refusal", "grant", "source", "target", "manifest", "canonical", "source_path", "target_path", "persistence", "unwired", "cancelled", "source_close", "target_close"} {
		t.Run(defect, func(t *testing.T) {
			opts, grant, receipt := stageFiles(t)
			ctx, cancel := context.WithCancel(context.Background()) // ctx-root: stage failure exercise.
			defer cancel()
			wantRC := 74
			publish := upgradeartifact.PublishFunc(func(_ context.Context, _ upgradetx.Grant, source, target io.Reader) (upgradeartifact.Receipt, error) {
				switch defect {
				// origin: hot-update S8 plan §5.2 / LINT-s8 — cleanup
				// failure must not persist an apparently successful receipt.
				case "source_close", "target_close":
					reader := source
					if defect == "target_close" {
						reader = target
					}
					closer, ok := reader.(io.Closer)
					if !ok {
						t.Fatal("publisher input has no owned close operation")
					}
					if err := closer.Close(); err != nil {
						t.Fatal(err)
					}
				case "refusal":
					return upgradeartifact.Receipt{}, &upgradetx.Error{Code: upgradetx.CodeDeferred}
				case "grant":
					receipt.Grant.ClientNonce = "another"
				case "source":
					receipt.SourceSHA = strings.Repeat("e", 64)
				case "target":
					receipt.TargetSHA = strings.Repeat("e", 64)
				case "manifest":
					receipt.ManifestDigest = ""
				case "canonical":
					receipt.CanonicalPath = ""
				case "source_path":
					receipt.SourcePath = ""
				case "target_path":
					receipt.TargetPath = ""
				case "persistence":
					// Create a directory after preflight, forcing Rename to fail
					// after publication without assuming the test runs non-root.
					if err := os.Mkdir(opts.ReceiptOut, 0700); err != nil {
						t.Fatal(err)
					}
				}
				return receipt, nil
			})
			if defect == "refusal" {
				wantRC = 75
			}
			if defect == "unwired" {
				publish = nil
			}
			if defect == "cancelled" {
				cancel()
			}
			if _, err := Stage(ctx, opts, publish); ExitCode(err) != wantRC {
				t.Fatalf("failure classified rc%d want%d: %v", ExitCode(err), wantRC, err)
			}
			var stillGrant upgradetx.Grant
			if err := readJSON(opts.Grant, &stillGrant); err != nil || !reflect.DeepEqual(stillGrant, grant) {
				t.Fatal("failed stage changed grant evidence")
			}
			// origin: hot-update S8 plan §5.2 / LINT-s8 — a failed
			// receipt rename must release its temporary publication evidence.
			temporary, err := filepath.Glob(filepath.Join(filepath.Dir(opts.ReceiptOut), ".upgrade-receipt-*"))
			if err != nil || len(temporary) != 0 {
				t.Fatalf("failed stage retained temporary receipts: %v, %v", temporary, err)
			}
			if defect != "persistence" {
				if _, err := os.Stat(opts.ReceiptOut); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("failed stage left a success receipt")
				}
			}
		})
	}
}

// origin: hot-update S8 plan §5.2 / §11.2 P18 — publication and refusal
// must close both image inputs as well as grant, receipt and directory handles.
func TestUpgradeStageLifecycleNoLeaks(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseStageLifetime(t)
	}
	testharness.AssertNoGoroutineLeak(t, "upgrade stage publication and refusal", beforeG)
	testharness.AssertNoFDLeak(t, "upgrade stage publication and refusal", beforeFD, 0)
}

func exerciseStageLifetime(t *testing.T) {
	t.Helper()
	opts, _, receipt := stageFiles(t)
	ctx := context.Background() // ctx-root: synchronous artifact file lifetime fixture.
	for _, refuse := range []bool{false, true} {
		_, err := Stage(ctx, opts, func(context.Context, upgradetx.Grant, io.Reader, io.Reader) (upgradeartifact.Receipt, error) {
			if refuse {
				return upgradeartifact.Receipt{}, &upgradetx.Error{Code: upgradetx.CodeDeferred}
			}
			return receipt, nil
		})
		want := 0
		if refuse {
			want = 75
		}
		if ExitCode(err) != want {
			t.Fatalf("stage lifetime rc%d want%d: %v", ExitCode(err), want, err)
		}
	}
}

// origin: hot-update S8 plan §5.2 / P18 — the running source is an open
// proc executable, not necessarily a still-linked canonical disk pathname.
func TestUpgradeStageAcceptsRunningProcSource(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("proc executable input is Linux-specific")
	}
	opts, _, receipt := stageFiles(t)
	opts.Source = "/proc/self/exe"
	ctx := context.Background() // ctx-root: running executable reader fixture.
	_, err := Stage(ctx, opts, func(_ context.Context, _ upgradetx.Grant, source, _ io.Reader) (upgradeartifact.Receipt, error) {
		magic := make([]byte, 4)
		if _, err := io.ReadFull(source, magic); err != nil || string(magic) != "\x7fELF" {
			t.Fatalf("running source not opened: %q %v", magic, err)
		}
		return receipt, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// An unlinked inode remains usable through a proc magic link, just as a
	// replaced running executable does. No child process is needed for this cut.
	opts, _, receipt = stageFiles(t)
	file, err := os.Open(opts.Source)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := os.Remove(opts.Source); err != nil {
		t.Fatal(err)
	}
	opts.Source = fmt.Sprintf("/proc/self/fd/%d", file.Fd())
	_, err = Stage(ctx, opts, func(_ context.Context, _ upgradetx.Grant, source, _ io.Reader) (upgradeartifact.Receipt, error) {
		data, err := io.ReadAll(source)
		if err != nil || string(data) != "running source" {
			t.Fatalf("unlinked source inode not opened: %q %v", data, err)
		}
		return receipt, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
