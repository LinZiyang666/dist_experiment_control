// wait_retry_test.go pins how Client.Wait / WaitRecorded treat a refusal that
// accepted nothing: asked again within the one absolute deadline, never ended
// as a retryable 75. (Moved out of commands_test.go so that file stays within
// the new-file line limit; the helpers it uses live there.)
package upgradecli

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: hot-update S8 plan §7.1 / §8.1; S8 FIX-DESCRIBE (real-cut finding).
// Right after an accepted Start the target is between its exec and its new
// subscriptions: the first queries find no responder or lose their reply.
// Such a pre-acceptance refusal changed nothing, so wait asks again with a
// fresh nonce until its one absolute deadline, which ends as unknown (74),
// never as a retryable 75. An accepted refusal or any other failure still
// ends the wait at once. The recorded wait reads its rollback goal from the
// record and shares the same retry for its very first query.
func TestUpgradeWaitRetriesTransientQueryRefusals(t *testing.T) {
	ctx := context.Background() // ctx-root: synchronous wait-loop policy test.
	completed := transactionView()
	completed.Observation.Current.Phase, completed.Observation.Current.Outcome = upgradetx.PhaseCompleted, upgradetx.OutcomeSuccess
	completed.Observation.Current.CutStage = upgradetx.CutReleased
	key, txID := queryIdentity(completed)
	lost := &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "agent transaction reply is unavailable"}
	unrouted := &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "agent transaction route is not subscribed"}
	// The source's own answer between its early Start reply and its exec
	// (agentupgradeport.ActivatingRefusal) is the same kind of refusal.
	activating := &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "target_activating: the target is replacing its image; retry"}
	scripted := func(view upgradetx.View, failures ...error) (Client, *int) {
		calls := 0
		return Client{PollInterval: time.Millisecond, Completed: func(upgradetx.Attempt, upgradetx.Observation) error { return nil },
			Endpoint: queryEndpoint{query: func(_ context.Context, _ upgradetx.Key, _ string, nonce string) (upgradetx.View, error) {
				calls++
				if calls <= len(failures) {
					return upgradetx.View{}, failures[calls-1]
				}
				view.Observation.Nonce = nonce
				return view, nil
			}}}, &calls
	}
	client, calls := scripted(completed, activating, lost, unrouted, lost)
	if _, err := client.Wait(ctx, key, txID, time.Second, false); err != nil || *calls != 5 {
		t.Fatalf("transient refusals ended the wait: calls=%d err=%v", *calls, err)
	}
	client, calls = scripted(completed, lost)
	if _, err := client.WaitRecorded(ctx, key, txID, time.Second); err != nil || *calls != 2 {
		t.Fatalf("recorded wait did not retry its first query: calls=%d err=%v", *calls, err)
	}
	for name, failure := range map[string]error{
		"accepted":   &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "accepted", Accepted: true},
		"not_owner":  &upgradetx.Error{Code: "not_owner", Detail: "upgrade actor or target is not authorized"},
		"unsettled":  &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "unknown"},
		"plain_text": errors.New("transport broke"),
	} {
		client, calls = scripted(completed, failure, failure)
		if _, err := client.Wait(ctx, key, txID, time.Second, false); !errors.Is(err, failure) || *calls != 1 {
			t.Fatalf("%s: a non-transient refusal was retried or rewritten: calls=%d err=%v", name, *calls, err)
		}
	}
	forever := Client{PollInterval: time.Millisecond, Endpoint: queryEndpoint{query: func(context.Context, upgradetx.Key, string, string) (upgradetx.View, error) {
		return upgradetx.View{}, lost
	}}}
	if _, err := forever.Wait(ctx, key, txID, 30*time.Millisecond, false); ExitCode(err) != 74 {
		t.Fatalf("a wait whose budget ran out on transient refusals did not end unknown: rc=%d err=%v", ExitCode(err), err)
	}
	// A standalone reverse record's goal is its reverse target, read from the
	// record: an upgrade goal would refuse the completed reverse (64).
	reverse := completed
	reverse.Historical.Forward = upgradetx.Attempt{}
	reverse.Historical.RecordedPreviousSHA = strings.Repeat("e", 64)
	reverse.Observation.Current.Direction, reverse.Observation.Current.TargetSHA = upgradetx.DirectionReverse, reverse.Historical.RecordedPreviousSHA
	reverse.Historical.Reverse = reverse.Observation.Current
	client, _ = scripted(reverse, unrouted)
	if _, err := client.WaitRecorded(ctx, key, txID, time.Second); err != nil {
		t.Fatalf("recorded wait missed the standalone reverse goal: %v", err)
	}
	if _, err := client.Wait(ctx, key, txID, time.Second, false); ExitCode(err) != 64 {
		t.Fatalf("an upgrade goal accepted a completed reverse: %v", err)
	}
}
