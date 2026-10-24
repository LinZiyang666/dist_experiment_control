package xfercontrol

import (
	"encoding/json"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

type pullSelectionTransport struct {
	testTransport
	source *x.SourceSeal
}

func (p pullSelectionTransport) Request(r x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	response, attempt, err := p.testTransport.Request(r, d)
	response.Source = p.source
	return response, attempt, err
}

// origin: S7 internal review INTEG-O F4
func TestTransferPullSelectionProbeNeverCreatesAuthority(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePullSelectionProbe(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exercisePullSelectionProbe(t *testing.T) {
	t.Helper()
	for _, tc := range []struct {
		name   string
		source *x.SourceSeal
		code   string
		intent *x.Intent
	}{
		{"empty", &x.SourceSeal{}, "", &x.Intent{Verb: "pull", Path: "/source"}},
		{"small", &x.SourceSeal{Size: 4096}, "", &x.Intent{Verb: "pull", Path: "/source"}},
		{"missing", nil, x.CodePayloadUnsupported, &x.Intent{Verb: "pull", Path: "/source"}},
		{"negative", &x.SourceSeal{Size: -1}, x.CodePayloadUnsupported, &x.Intent{Verb: "pull", Path: "/source"}},
		{"oversize", &x.SourceSeal{Size: proto.XferMaxBytes + 1}, x.CodePayloadUnsupported, &x.Intent{Verb: "pull", Path: "/source"}},
		{"premature-seal", &x.SourceSeal{Size: 4096, SHA256: "unsealed"}, x.CodePayloadUnsupported, &x.Intent{Verb: "pull", Path: "/source"}},
		{"unsolicited-source", &x.SourceSeal{Size: 4096}, x.CodeResultUnknown, nil},
		{"capability-only", nil, "", nil},
		{"creation-intent", &x.SourceSeal{Size: 4096}, x.CodeIntentConflict, &x.Intent{Verb: "pull", Path: "/source", Tier: "b"}},
		{"hidden-rate", &x.SourceSeal{Size: 4096}, x.CodeIntentConflict, &x.Intent{Verb: "pull", Path: "/source", RateLimitBPS: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newControlFixture(t)
			cfg := f.config()
			cfg.Transport = pullSelectionTransport{testTransport: testTransport{f}, source: tc.source}
			controller, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := controller.Stop(testDeadline(t)); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			request := f.request(x.OpProbe)
			request.Origin, request.Intent = x.Origin{}, tc.intent
			request.Identity.EntryNonce, request.Identity.IntentDigest, request.Identity.ObjectVersion = "", "", ""
			response, err := controller.Handle(request, f.principal, testDeadline(t))
			if response.Code != tc.code || (err != nil) != (tc.code != "") {
				t.Fatalf("source inspection code=%q want=%q err=%v", response.Code, tc.code, err)
			}
			if f.issues != 0 || f.admits != 0 || f.buckets != 0 || f.forwards != 0 || f.sources != 0 {
				t.Fatal("read-only inspection created permit, tracker, bucket, worker or source seal")
			}
			if tc.code == "" && tc.intent != nil {
				wire, err := WireResponse(x.OpProbe, response)
				if err != nil {
					t.Fatal(err)
				}
				body, err := json.Marshal(wire)
				if err != nil {
					t.Fatal(err)
				}
				var decoded PullSelectionProbeResp
				if err := proto.DecodeTransferWire(body, &decoded); err != nil || decoded.SourceSize == nil || *decoded.SourceSize != tc.source.Size {
					t.Fatalf("source-size evidence lost on actual wire: %v", err)
				}
			}
		})
	}
	request := PullSelectionProbeReq{CallID: "select", SID: "session", NID: "node", SourcePath: "/source"}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeWire(x.OpProbe, body, x.Identity{SID: "session", NID: "node", TransferID: "original", ActorFP: "actor"})
	if err != nil || decoded.Intent == nil || *decoded.Intent != (x.Intent{Verb: "pull", Path: "/source"}) || decoded.CallID != request.CallID {
		t.Fatalf("source-path query lost on actual wire: %+v %v", decoded.Intent, err)
	}
	if _, _, err := EncodePullSelectionProbe("actor", x.Request{Op: x.OpProbe}); err == nil {
		t.Fatal("source query without its path intent was encoded")
	}
}
