//go:build linux

package agentupgradeport

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
)

// noticeImage is a frozen cut whose final re-export succeeds or fails.
type noticeImage struct {
	parentToken string
	prepareErr  error
	events      *[]string
}

func (i noticeImage) Bundle() agentimage.Bundle { return agentimage.Bundle{} }
func (i noticeImage) Thaw() error               { *i.events = append(*i.events, "thaw"); return nil }
func (i noticeImage) Prepare(_ string, adjunct handoff.Resource) (*handoff.Prepared, error) {
	*i.events = append(*i.events, "prepare")
	// Parent authority must survive final re-export before any exec notice.
	// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.1
	state, err := upgradecut.Decode(adjunct.Data)
	if err != nil || state.ParentToken != i.parentToken {
		return nil, errors.New("parent reservation token disappeared from sealed cut")
	}
	if i.prepareErr != nil {
		return nil, i.prepareErr
	}
	return handoff.Prepare(handoff.State{Role: "notice-test", Instance: "notice", Generation: 1}, nil)
}

// fileLease is a domain reservation over a real descriptor (dupLease dups it).
type fileLease struct{ file *os.File }

func (l fileLease) FD() uintptr      { return l.file.Fd() }
func (l fileLease) DomainID() string { return strings.Repeat("d", 32) }
func (l fileLease) Close() error     { return l.file.Close() }

// noticeRuntime is a runtime that is not the production adapter.
type noticeRuntime struct {
	upgradeport.RuntimePort
	activate func()
}

func (r noticeRuntime) Activate(upgradeport.Cut) (upgradeport.Activation, error) {
	r.activate()
	return upgradeport.Activation{}, errors.New("not activated")
}

// origin: hot-update S8 plan §3.1 / §7.1; S8 FIX-DESCRIBE (real-cut finding).
// A Start loses its own reply to a successful exec; the broker then answered
// only after its whole 30 s forward timeout. The request therefore gets ONE
// early accepted-unknown reply (74, query the original), sent by the
// production adapter only after the final re-export and reader checks passed
// and right before exec — so a refusal there still returns its exact reason —
// and flushed before exec. The final reply is then suppressed; without an
// early reply it is the only one. Another runtime is notified before Activate.
// From the early reply until exec returns, every other request is refused at
// once (ActivatingRefusal) and never again afterwards.
func TestStartReplyPrecedesOnlyTheExec(t *testing.T) {
	nc, err := nats.Connect(testharness.StartNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node"}
	owner := upgradetx.Owner{BootID: "11111111-2222-3333-4444-555555555555", PID: os.Getpid(), StartTicks: 9, InstanceID: strings.Repeat("9", 32)}
	handle := upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 3}
	scope := upgradetx.Scope{ID: strings.Repeat("6", 32), Participants: []upgradetx.Key{key}, Domains: append([]upgradetx.Domain(nil), registryDomains[:]...),
		Expected: []upgradetx.Resource{}}
	final := []byte(`{"code":"upgrade_unsettled","error":"final"}`)
	for _, tc := range []struct {
		name        string
		prepareErr  error
		early       bool
		events      string
		parentToken string
	}{
		{"exec_after_reply", nil, true, "prepare,reply,exec,thaw", ""},
		{"parent_exec_after_reply", nil, true, "prepare,reply,exec,thaw", strings.Repeat("8", 32)},
		{"refused_final_reexport", errors.New("late reader refusal"), false, "prepare,thaw", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inbox := nats.NewInbox()
			sub, err := nc.SubscribeSync(inbox)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			var events []string
			lease, err := os.Open(filepath.Join(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			var port *runtimePort
			port = NewRuntime(Host{Key: key, Incarnation: func() string { return strings.Repeat("7", 64) },
				Exec: func(string, *handoff.Prepared) error {
					// The early reply is already on the wire when the exec starts.
					if _, err := sub.NextMsg(2 * time.Second); err != nil {
						t.Errorf("exec began before the early reply was flushed: %v", err)
					}
					// Any other request now is refused at once, accepting nothing.
					// origin: S8 FIX-DESCRIBE (real-cut finding: a query lost to the exec)
					refusal, err := upgradewire.DecodeResponse(ActivatingRefusal(port))
					if err != nil || refusal.Code != upgradetx.CodeDeferred || !strings.HasPrefix(refusal.Error, "target_activating") {
						t.Errorf("a request during the exec window was not refused at once: %+v %v", refusal, err)
					}
					events = append(events, "reply", "exec")
					return errors.New("exec returned")
				}}).(*runtimePort)
			if ActivatingRefusal(port) != nil {
				t.Fatal("an idle adapter refused a request as activating")
			}
			port.ready[handle.TxID] = preflighted{target: "/candidate", sourceSHA: strings.Repeat("a", 64), candidate: agentimage.Capabilities{SHA256: strings.Repeat("b", 64)}}
			port.held = &heldCut{parentToken: tc.parentToken, handle: handle, image: noticeImage{parentToken: tc.parentToken, prepareErr: tc.prepareErr, events: &events}, lease: fileLease{lease}, sequence: 1,
				timer: time.AfterFunc(time.Hour, func() {})}
			ctx, reply := ReplyOnce(t.Context(), nc, inbox)
			_, err = ActivateNotifying(ctx, port, upgradeport.Cut{Attempt: handle, Owner: owner, Scope: scope, Sequence: 1})
			var typed *upgradetx.Error
			if !errors.As(err, &typed) || typed.Code != upgradetx.CodeUnsettled || !typed.Accepted {
				t.Fatalf("activation outcome is not accepted-unknown: %v", err)
			}
			if !tc.early && typed.Detail != "abi_late" {
				t.Fatalf("a refused final re-export lost its exact reason: %q", typed.Detail)
			}
			reply(final)
			if ActivatingRefusal(port) != nil {
				t.Fatal("the source kept refusing requests after its exec returned")
			}
			if got := strings.Join(events, ","); got != tc.events {
				t.Fatalf("activation order = %s, want %s", got, tc.events)
			}
			if !tc.early {
				msg, err := sub.NextMsg(2 * time.Second)
				if err != nil || string(msg.Data) != string(final) {
					t.Fatalf("the final reply is not the only reply: %v", err)
				}
			}
			if extra, err := sub.NextMsg(100 * time.Millisecond); err == nil {
				t.Fatalf("a second reply reached the requester: %s", extra.Data)
			}
		})
	}
	// The early reply's content is the accepted-unknown classification.
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	ctx, reply := ReplyOnce(t.Context(), nc, inbox)
	accepted := upgradetx.Record{Key: key, Grant: upgradetx.Grant{Handle: handle}, Forward: upgradetx.Attempt{Handle: handle}}
	ctx = WithActivationRecord(ctx, accepted)
	notified := false
	if ActivatingRefusal(noticeRuntime{}) != nil {
		t.Fatal("a runtime that cannot report activation refused a request")
	}
	_, _ = ActivateNotifying(ctx, noticeRuntime{activate: func() {
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatal("another runtime was activated before its requester was notified")
		}
		response, err := upgradewire.DecodeResponse(msg.Data)
		if err != nil || response.Code != upgradetx.CodeUnsettled || !strings.Contains(response.Error, "query the original transaction") {
			t.Fatalf("early reply is not accepted-unknown: %+v %v", response, err)
		}
		// origin: S8 internal review upgrade-maketest F8
		if !reflect.DeepEqual(response.View.Historical, accepted) {
			t.Fatal("early wire reply lost the issued activation identity")
		}
		notified = true
	}}, upgradeport.Cut{Attempt: handle})
	reply(final)
	if !notified {
		t.Fatal("other runtime was not activated")
	}
	if extra, err := sub.NextMsg(100 * time.Millisecond); err == nil {
		t.Fatalf("the final reply followed the early one: %s", extra.Data)
	}
	// Without a request context there is no early reply, only the final one.
	var noRequest context.Context
	if ctx, reply := ReplyOnce(noRequest, nc, nats.NewInbox()); ctx != nil || reply == nil {
		t.Fatal("a nil request context gained an early reply")
	}
}
