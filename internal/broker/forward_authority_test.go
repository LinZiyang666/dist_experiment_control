package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/cluster"
	nodepkg "github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/hashicorp/raft"
)

// The optional envelope preserves ordinary N-1 inputs and carries every field
// of a captured guard. A key-only freeze cannot catch removal of omitempty or
// a projection that silently drops authority on a broker-to-broker hop.
// origin: S7 internal review BROKER-REDS F4; hot-update S8 plan §6.2 / §7
func TestForwardAuthorityIsAdditiveAndPreserved(t *testing.T) {
	guard := upgradeauthority.Guard{SID: "lab", Incarnation: strings.Repeat("a", 64), InstanceID: strings.Repeat("b", 26), ConfiguredNID: "node"}
	for name, specimen := range map[string]any{
		"allocation": PortFreeAllocationPayload{Port: 14000, SID: "lab", NID: "node", Name: "svc", TokenHash: "token"},
		"session":    SessionMutatePayload{SID: "lab"},
		"exit":       ProcMarkExitedPayload{Pid: "pid", Sid: "lab", ExitCode: 23},
		"register":   nodepkg.RegisterInput{SID: "lab", NID: "node", BootID: "boot", ProtoVersion: 2},
	} {
		t.Run(name, func(t *testing.T) {
			value := reflect.New(reflect.TypeOf(specimen))
			value.Elem().Set(reflect.ValueOf(specimen))
			field, ok := value.Elem().Type().FieldByName("Authority")
			if !ok || field.Type != reflect.TypeOf(&guard) || field.Tag.Get("json") != "authority,omitempty" {
				t.Fatal("authority must remain an optional, tagged guard pointer")
			}
			legacy, err := json.Marshal(specimen)
			if err != nil {
				t.Fatal(err)
			}
			var keys map[string]json.RawMessage
			if err := json.Unmarshal(legacy, &keys); err != nil {
				t.Fatal(err)
			}
			if _, present := keys["authority"]; present {
				t.Fatal("ordinary legacy input gained an authority key")
			}
			value.Elem().FieldByName("Authority").Set(reflect.ValueOf(&guard))
			body, err := json.Marshal(value.Interface())
			if err != nil {
				t.Fatal(err)
			}
			decoded := reflect.New(value.Elem().Type())
			if err := json.Unmarshal(body, decoded.Interface()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded.Interface(), value.Interface()) {
				t.Fatal("forwarding lost the captured authority or ordinary input")
			}
			// Decode an old sender over the populated value's type, as a new
			// leader does. Omission must not invent a creation identity.
			decoded = reflect.New(value.Elem().Type())
			if err := json.Unmarshal(legacy, decoded.Interface()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded.Elem().Interface(), specimen) {
				t.Fatal("ordinary legacy input no longer decodes unchanged")
			}
		})
	}
}

// Consolidating the two PIN seams must preserve both leader-local and
// forwarded transient errors while returning permanent business errors intact.
// origin: S7 internal review BROKER-REDS F3; hot-update S6 plan §8.1
func TestPINWriteErrorPreservesTransientAndBusinessIdentity(t *testing.T) {
	for _, err := range []error{raft.ErrNotLeader, raft.ErrLeadershipLost, raft.ErrLeadershipTransferInProgress, raft.ErrEnqueueTimeout, cluster.ErrForwardNotLeader} {
		if got := pinWriteError(fmt.Errorf("wrapped: %w", err)); !errors.Is(got, authcallout.ErrNotLeader) {
			t.Errorf("leadership loss was not transient: %v", got)
		}
	}
	permanent := errors.New("permanent business refusal")
	if pinWriteError(permanent) != permanent || pinWriteError(nil) != nil {
		t.Fatal("PIN error projection changed business refusal or success identity")
	}
}
