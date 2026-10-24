package proto

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	x "github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
	"reflect"
	"testing"
	"time"
)

// origin: hot-update S7 plan §6.1–§6.4 / T32 / INTEG-D
func TestTransferCarriersPreserveAuthenticatedPrepare(t *testing.T) {
	for _, verb := range []string{"push", "pull"} {
		for _, tier := range []string{"a", "b"} {
			t.Run(verb+"_"+tier, func(t *testing.T) {
				data := []byte("retained original inline bytes")
				sum := sha256.Sum256(data)
				intent := x.Intent{Verb: verb, Tier: tier, Path: "/tmp/source", Force: true, RateLimitBPS: 4096}
				if verb == "push" {
					intent.Size = int64(len(data))
					intent.SHA256 = hex.EncodeToString(sum[:])
				}
				if tier == "b" {
					intent.ChunkSize = 32768
				}
				digest, err := TransferIntentDigest(Intent(intent))
				if err != nil {
					t.Fatal(err)
				}
				// This is the P00 ordered encoding used by the real client and controller.
				raw, _ := json.Marshal(intent)
				wantDigest := sha256.Sum256(raw)
				if digest != hex.EncodeToString(wantDigest[:]) {
					t.Fatal("wire and controller intent digests disagree")
				}
				id := x.Identity{SID: "lab", NID: "node", ActorFP: "SHA256:fp", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: digest}
				if tier == "b" {
					id.ObjectVersion = "originalNUID"
				}
				origin := x.Origin{BrokerID: "original", InstanceID: "broker", BootID: "boot", Generation: 3}
				permit := x.CreationPermit{Identity: id, Origin: origin, IssuedAt: time.Now().UTC(), CreateUntil: time.Now().UTC().Add(time.Minute), PermitNonce: "permit", Signature: "signature"}
				request := x.Request{Op: x.OpPrepare, CallID: "transfer-prepare", Identity: id, Origin: origin, Intent: &intent, Permit: &permit}
				if tier == "a" && verb == "push" {
					request.Inline = bytes.Clone(data)
				}
				subject, body, err := EncodeTransferRequest(fakeActor, request)
				if err != nil {
					t.Fatal(err)
				}
				if subject != SubjCmdBy(id.SID, fakeActor, id.NID, verb) {
					t.Fatal("prepare changed subject scope")
				}
				decoded, err := DecodeTransferRequest(id.ActorFP, id.SID, id.NID, id.TransferID, x.OpPrepare, verb, body)
				if err != nil || !reflect.DeepEqual(request, decoded) {
					t.Fatalf("prepare lost original authority: %v\nwant=%+v\ngot=%+v", err, request, decoded)
				}
				if _, err = DecodeTransferRequest("foreign", id.SID, id.NID, id.TransferID, x.OpPrepare, verb, body); err == nil {
					t.Fatal("payload actor replaced authenticated actor")
				}
				status := x.Status{State: x.StatusLive, Phase: x.PhasePrepared, Identity: id, Origin: origin, Budget: x.BudgetSeal{StartedAt: permit.IssuedAt, Size: int64(len(data)), RateLimitBPS: 4096, SourceSealed: true, EffectiveBudgetNS: int64(time.Minute), RemainingNS: int64(time.Minute)}}
				source := x.SourceSeal{Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), RateLimitBPS: 4096, SourceFingerprint: "pinned-stat"}
				if tier == "b" {
					source.ChunkSize = 32768
					source.Chunks = 1
				}
				response := x.Response{Status: &status, Source: &source, Settlement: &x.SettlementCapsule{Identity: id, Origin: origin, ReceiverNkey: fakeActor, Issuer: "issuer", Nonce: "nonce", Signature: "signed"}}
				if tier == "a" && verb == "pull" {
					response.Inline = bytes.Clone(data)
				}
				encoded, err := EncodeTransferResponse(request, response)
				if err != nil {
					t.Fatal(err)
				}
				got, err := DecodeTransferResponse(x.OpPrepare, verb, encoded)
				if err != nil || !reflect.DeepEqual(got, response) {
					t.Fatalf("prepare response lost source/capsule: %v\nwant=%+v\ngot=%+v", err, response, got)
				}
				// A status request is never a way to exfiltrate prepared bytes or capsule.
				if _, err := EncodeTransferResponse(x.Request{Op: x.OpStatus}, response); err == nil {
					t.Fatal("status encoded a prepare capsule")
				}
			})
		}
	}
}

// origin: hot-update S7 plan §6.1 / §6.4 / INTEG-D
func TestTransferStatusCarrierRejectsPrivateAndForeignFields(t *testing.T) {
	c := transferWireFixture()
	s := TransferStatusResp{State: StatusLive, Phase: PhasePrepared, Identity: c.Identity, Origin: c.Origin, Budget: c.Budget}
	b, _ := json.Marshal(s)
	for _, field := range []string{`"settlement":{}`, `"inline_data":"YQ=="`, `"source":{}`, `"State":"unknown"`} {
		bad := append(bytes.Clone(b[:len(b)-1]), []byte(","+field+"}")...)
		if _, err := DecodeTransferResponse(x.OpStatus, "", bad); err == nil {
			t.Fatalf("status accepted private/ambiguous field %s", field)
		}
	}
	id := x.Identity{SID: "lab", NID: "node", ActorFP: "SHA256:fp", TransferID: "transfer"}
	r := x.Request{Op: x.OpStatus, CallID: "lookup", Identity: id}
	_, body, err := EncodeTransferRequest(fakeActor, r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeTransferRequest(id.ActorFP, id.SID, id.NID, id.TransferID, x.OpStatus, "", body)
	if err != nil || got.Identity != id || got.Origin != (x.Origin{}) {
		t.Fatalf("read-only discovery failed: %+v %v", got, err)
	}
	// Public read-only scope may be partial; a retained mutation must be complete.
	r.Op = x.OpCommit
	if _, _, err = EncodeTransferRequest(fakeActor, r); err == nil {
		t.Fatal("partial status scope admitted commit")
	}
	status := contractStatus(&s)
	encoded, err := EncodeTransferResponse(x.Request{Op: x.OpStatus}, x.Response{Status: status, Code: x.CodeOwnerRecovering, Error: "owner not ready"})
	if err != nil {
		t.Fatal(err)
	}
	response, err := DecodeTransferResponse(x.OpStatus, "", encoded)
	if err != nil || response.Code != x.CodeOwnerRecovering || response.Error != "owner not ready" {
		t.Fatal("a retained status laundered a controller refusal")
	}
	commit := TransferCommitResp{OK: true, Continuation: &c}
	for _, private := range []string{"source", "settlement", "intent"} {
		copy := c
		copy.Source, copy.Settlement, copy.Intent = nil, nil, nil
		switch private {
		case "source":
			copy.Source = &SourceSeal{}
		case "settlement":
			copy.Settlement = &SettlementCapsule{Identity: copy.Identity, Origin: copy.Origin, ReceiverNkey: fakeActor, Issuer: "issuer", Nonce: "nonce", Signature: "signature"}
		case "intent":
			copy.Intent = &Intent{Verb: "push", Tier: "b", Path: "/tmp/dst"}
		}
		commit.Continuation = &copy
		encoded, _ = json.Marshal(commit)
		if _, err = DecodeTransferResponse(x.OpCommit, "push", encoded); err == nil {
			t.Fatalf("commit accepted prepare-only %s", private)
		}
	}
}
