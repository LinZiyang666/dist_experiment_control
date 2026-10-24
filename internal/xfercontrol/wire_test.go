package xfercontrol

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nkeys"
)

// origin: hot-update S7 plan §6.2 / T39
func TestTransferWirePreservesScopeAndOperationEvidence(t *testing.T) {
	f := newControlFixture(t)
	scope := f.id
	body := proto.TransferStatusReq{CallID: "call", Identity: proto.Identity(scope), Origin: proto.Origin(f.origin)}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := DecodeWire(x.OpStatus, raw, scope)
	if err != nil || req.Identity != scope || req.Origin != f.origin || req.CallID != "call" {
		t.Fatalf("lost wire identity: %+v %v", req, err)
	}
	bad := scope
	bad.ActorFP = "foreign"
	if _, err := DecodeWire(x.OpStatus, raw, bad); err == nil {
		t.Fatal("body replaced subject actor")
	}
	bad = scope
	bad.NID = "other"
	if _, err := DecodeWire(x.OpStatus, raw, bad); err == nil {
		t.Fatal("body replaced subject node")
	}
	if _, err := DecodeWire(x.OpStatus, append(raw[:len(raw)-1], []byte(`,"extra":true}`)...), scope); err == nil {
		t.Fatal("open wire union accepted")
	}
	seal := x.BudgetSeal{StartedAt: time.Now().UTC(), EffectiveBudgetNS: 100, RemainingNS: 53, Size: 43, SourceSealed: true}
	st := x.Status{State: x.StatusTerminal, Phase: x.PhaseHistoryConfirmed, Identity: scope, Origin: f.origin, Budget: seal, DurableOffset: 43, ChunkIndex: 3, LastConsumedStreamSeq: 9, Terminal: &f.receipt, CleanupPending: true}
	wire, err := WireResponse(x.OpStatus, x.Response{Status: &st})
	if err != nil {
		t.Fatal(err)
	}
	got := wire.(proto.TransferStatusResp)
	if x.Identity(got.Identity) != scope || x.Origin(got.Origin) != f.origin || x.BudgetSeal(got.Budget) != seal || got.DurableOffset != 43 || got.ChunkIndex != 3 || got.LastConsumedStreamSeq != 9 || !got.CleanupPending || got.Terminal == nil || x.TerminalReceipt(*got.Terminal) != f.receipt {
		t.Fatalf("lost status evidence: %+v", got)
	}
	for _, op := range []x.Operation{x.OpPermit, x.OpReserve, x.OpTerminal} {
		response := x.Response{Permit: &f.permit, Grant: &f.grant, Receipt: &f.receipt}
		wire, err := WireResponse(op, response)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		var decoded any
		switch op {
		case x.OpPermit:
			decoded = &proto.TransferPermitResp{}
		case x.OpReserve:
			decoded = &proto.TransferReserveResp{}
		case x.OpTerminal:
			decoded = &proto.TransferTerminalResp{}
		}
		if err := proto.DecodeTransferWire(encoded, decoded); err != nil {
			t.Fatalf("%s response: %v", op, err)
		}
		switch value := decoded.(type) {
		case *proto.TransferPermitResp:
			var p x.CreationPermit
			_ = projectWire(value.Permit, &p)
			if p != f.permit {
				t.Fatal("permit projection lost authority")
			}
		case *proto.TransferReserveResp:
			var g x.CommitGrant
			_ = projectWire(value.Grant, &g)
			if g != f.grant {
				t.Fatal("grant projection lost authority")
			}
		case *proto.TransferTerminalResp:
			if value.Receipt == nil || x.TerminalReceipt(*value.Receipt) != f.receipt {
				t.Fatal("receipt projection lost evidence")
			}
		}
	}
	probe := x.ProbeResult{Version: x.Version, HistoryGuardVersion: x.Version, BucketConfigVersion: x.Version, InstanceID: "agent", MaxPayload: 123, MaxMsgSize: 111}
	wire, err = WireResponse(x.OpProbe, x.Response{Probe: &probe})
	if err != nil || !reflect.DeepEqual(wire, proto.TransferProbeResp(probe)) {
		t.Fatalf("probe projection: %+v %v", wire, err)
	}
	// An authenticated fingerprint is base64 text, not a NATS subject component.
	f.id.ActorFP = "SHA256://////////////////////////////////////////8"
	f.principal.FP = f.id.ActorFP
	f.status.Identity = f.id
	f.live()
	if _, err = f.ctl.Handle(f.request(x.OpStatus), f.principal, testDeadline(t)); err != nil {
		t.Fatalf("canonical fingerprint rejected: %v", err)
	}
}

// origin: hot-update S7 plan §6.1 / T26
func TestTransferScopedPermitBindsAuthenticatedCaller(t *testing.T) {
	f := newControlFixture(t)
	issuer, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer issuer.Wipe()
	permits, err := xferauth.NewPermitAuthority(t.TempDir(), f.origin, issuer, func() time.Time { return f.now })
	if err != nil {
		t.Fatal(err)
	}
	cfg := f.config()
	cfg.Permits = permits
	controller, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := controller.Stop(testDeadline(t)); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	request := f.request(x.OpPermit)
	response, err := controller.Handle(request, f.principal, testDeadline(t))
	if err != nil || response.Permit == nil || response.Permit.Identity.SID != request.Identity.SID || response.Permit.Identity.ActorFP != f.principal.FP || response.Permit.Identity.TransferID != request.Identity.TransferID {
		t.Fatalf("scoped issuance: %+v %v", response, err)
	}
	again, err := controller.Handle(request, f.principal, testDeadline(t))
	if err != nil || again.Permit == nil || *again.Permit != *response.Permit {
		t.Fatalf("permit replay changed original authority: %v", err)
	}
}

// origin: hot-update S7 plan §6.3 / P16
func TestTransferRegistrationDescriptorKeepsOriginalOwnerBroadcast(t *testing.T) {
	rows := Registrations()
	if len(rows) != 19 {
		t.Fatalf("registration closure changed: %d", len(rows))
	}
	seen := make(map[string]bool)
	for _, row := range rows {
		if seen[row.Subject] || row.Subject == "" || !row.PlainBroadcast {
			t.Fatalf("duplicate, empty or queued subscription: %+v", row)
		}
		seen[row.Subject] = true
		if row.Operation != x.OpProbe && row.Operation != x.OpPermit && row.Operation != x.OpBucketConfig && !row.OriginalOwner {
			t.Fatalf("continuation lost original-owner route: %+v", row)
		}
		if row.CurrentHome != (row.Operation == x.OpProbe || row.Operation == x.OpPermit) {
			t.Fatalf("current-home route leaked into continuation: %+v", row)
		}
	}
	for _, op := range []string{"probe", "permit", "status", "source", "reserve", "terminal", "cancel"} {
		key := fmt.Sprintf("%s.ctrl.by.*.s.*.node.*.transfer.*.%s.req", proto.SubjectPrefix, op)
		if !seen[key] {
			t.Fatalf("missing concrete controller subject %s", key)
		}
	}
	rows[0].Subject = "corrupted-cold-table"
	if Registrations()[0].Subject == rows[0].Subject {
		t.Fatal("cold registration mutation corrupted hot subscriptions")
	}
}

// origin: hot-update S7 plan §4 / §6.2 / P16
func TestTransferControllerRoutesOnlyRetainedMutations(t *testing.T) {
	for _, op := range []x.Operation{x.OpSource, x.OpReserve, x.OpCommit, x.OpTerminal, x.OpFinalize, x.OpCancel} {
		t.Run(string(op), func(t *testing.T) {
			f := newControlFixture(t)
			req := f.request(op)
			r, err := f.ctl.Handle(req, f.principal, testDeadline(t))
			requireCode(t, r, err, x.CodeUnknown)
			if f.sources+f.reserves+f.finishes+f.cancels != 0 {
				t.Fatal("mutation created a missing transfer")
			}
			f.live()
			r, err = f.ctl.Handle(req, f.principal, testDeadline(t))
			if err != nil || f.sources+f.reserves+f.finishes+f.cancels != 1 {
				t.Fatalf("retained mutation was not dispatched once: %+v %v calls=%v", r, err, f.calls)
			}
			if f.admits != 0 || f.issues != 0 || f.buckets != 0 || f.forwards != 0 || len(f.requests) != 0 {
				t.Fatal("continuation reentered creation or data transport")
			}
		})
	}
	t.Run("receiver_binding", func(t *testing.T) {
		f := newControlFixture(t)
		f.live()
		req := f.request(x.OpTerminal)
		p := f.principal
		p.Role, p.InstanceID = x.RoleAgent, f.id.AgentInstance
		r, err := f.ctl.Handle(req, p, testDeadline(t))
		requireCode(t, r, err, x.CodeNotOwner)
		if f.finishes != 0 {
			t.Fatal("agent acknowledged a grant belonging to the ctl receiver")
		}
	})
	t.Run("management_never_returns_empty_success", func(t *testing.T) {
		f := newControlFixture(t)
		p := f.principal
		p.Role = x.RoleBroker
		for _, op := range []x.Operation{x.OpCensus, x.OpBucketConfig} {
			r, err := f.ctl.Handle(f.request(op), p, testDeadline(t))
			requireCode(t, r, err, x.CodePayloadUnsupported)
		}
	})
}
