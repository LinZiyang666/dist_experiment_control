package clusterroll

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §3.1 / §7.1
func TestAgentQueryRetriesOnlyTheOriginalReadAfterHandoffLoss(t *testing.T) {
	key := upgradetx.Key{Component: upgradetx.ComponentAgent, DomainID: strings.Repeat("a", 32), SID: "session", ConfiguredNID: "agent"}
	tx, nonce := strings.Repeat("b", 32), strings.Repeat("c", 32)
	calls := 0
	c := &clusterClient{ports: ClusterPorts{Agent: func(_ context.Context, k upgradetx.Key, raw []byte) ([]byte, error) {
		calls++
		var r upgradewire.Request
		if err := json.Unmarshal(raw, &r); err != nil {
			t.Fatal(err)
		}
		if k != key || r.Key != key || r.Op != upgradewire.OpQuery || r.TxID != tx || r.QueryNonce != nonce {
			t.Fatal("retry issued another operation or child identity")
		}
		if calls == 1 {
			return nil, errors.New("reply lost across exec")
		}
		// This transport test returns unresolved observation, never completion.
		return upgradewire.EncodeResponse(upgradewire.Response{View: upgradetx.View{Code: upgradetx.CodeUnsettled, Detail: "fixture unresolved"}})
	}}}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	view, err := (clusterComponents{client: c}).Query(ctx, key, tx, nonce)
	if err != nil || calls != 2 || view.Code != upgradetx.CodeUnsettled {
		t.Fatalf("read loss did not re-query the original child: calls=%d view=%+v error=%v", calls, view, err)
	}
	for _, err := range []error{&upgradetx.Error{Code: upgradetx.CodeRejected}, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "start reply lost"}, errors.New("unknown mutation")} {
		if lostAgentQuery(err) {
			t.Fatalf("mutation/refusal classified as a retryable read: %v", err)
		}
	}
}
